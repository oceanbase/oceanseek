/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/ob_test_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/ob_block_gc_handler.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class TestPrivateGCBlockHandler : public ObBlockGCHandler
{
public:
  explicit TestPrivateGCBlockHandler(bool &is_stopped)
    : ObBlockGCHandler(ObTabletID(1), is_stopped)
  {}

  int get_blocks_for_tablet(
      const int64_t tablet_meta_version,
      const bool is_shared,
      ObIArray<MacroBlockId> &block_ids) override
  {
    UNUSEDx(tablet_meta_version, is_shared, block_ids);
    return OB_SUCCESS;
  }
  int delete_tablet_meta_version(int64_t tablet_meta_version) override
  {
    UNUSED(tablet_meta_version);
    return OB_SUCCESS;
  }
  int try_delete_tablet_meta_dir() override { return OB_SUCCESS; }
  int try_delete_tablet_data_dir() override { return OB_SUCCESS; }
  int get_block_ids_from_dir(ObIArray<MacroBlockId> &block_ids) override
  {
    UNUSED(block_ids);
    return OB_SUCCESS;
  }
  int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &collect_check_block_op) override
  {
    UNUSEDx(tablet_versions, min_retain_tablet_meta_version, collect_check_block_op);
    return OB_SUCCESS;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    UNUSEDx(buf, buf_len);
    return 0;
  }
};

class TestSSMacroCacheMgr : public ::testing::Test
{
public:
  TestSSMacroCacheMgr() {}
  virtual ~TestSSMacroCacheMgr() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void check_macro_cache_meta(const MacroBlockId &macro_id,
                              const ObSSMacroCacheInfo &cache_info);
  int write_shared_read_cache(const MacroBlockId &macro_id, const uint64_t tablet_id);
public:
  static const int64_t WRITE_IO_SIZE = 2 * 1024L * 1024L; // 2MB
  ObStorageObjectWriteInfo write_info_;
  char write_buf_[WRITE_IO_SIZE];
};

void TestSSMacroCacheMgr::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSSMacroCacheMgr::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMacroCacheMgr::SetUp()
{
  // construct write info
  write_buf_[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf_, 'a', mid_offset);
  memset(write_buf_ + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info_.io_desc_.set_wait_event(1);
  write_info_.buffer_ = write_buf_;
  write_info_.offset_ = 0;
  write_info_.size_ = WRITE_IO_SIZE;
  write_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info_.mtl_tenant_id_ = MTL_ID();
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
}

void TestSSMacroCacheMgr::TearDown()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto = false;
  }
  write_buf_[0] = '\0';
}

int TestSSMacroCacheMgr::write_shared_read_cache(
    const MacroBlockId &macro_id,
    const uint64_t tablet_id)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle write_object_handle;
  write_info_.set_is_write_cache(false);
  write_info_.set_effective_tablet_id(tablet_id);
  if (OB_FAIL(write_object_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set shared macro block id", KR(ret), K(macro_id), K(tablet_id));
  } else {
    ObSSLocalCacheWriter local_cache_writer;
    if (OB_FAIL(local_cache_writer.aio_write_with_create_parent_dir(
        write_info_, write_object_handle))) {
      LOG_WARN("fail to write shared macro into local cache", KR(ret), K(macro_id), K(tablet_id));
    } else if (OB_FAIL(write_object_handle.wait())) {
      LOG_WARN("fail to wait shared macro local cache write", KR(ret), K(macro_id), K(tablet_id));
    }
  }
  return ret;
}

void TestSSMacroCacheMgr::check_macro_cache_meta(
    const MacroBlockId &macro_id,
    const ObSSMacroCacheInfo &cache_info)
{
  static int64_t call_times = 0;
  call_times++;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr) << "call_times: " << call_times;
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle)) << "call_times: " << call_times;
  ASSERT_TRUE(meta_handle.is_valid()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.effective_tablet_id_, meta_handle()->get_effective_tablet_id()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.size_, meta_handle()->get_size()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.cache_type_, meta_handle()->get_cache_type()) << "call_times: " << call_times;
  ASSERT_EQ(cache_info.is_write_cache_, meta_handle()->get_is_write_cache()) << "call_times: " << call_times;
}

// 用例分组总览：
//   A. Macro cache manager 既有基础路径       (8 个用例)
//   B. Private GC shared cache 与 HOT GC 路径 (4 个用例)

// ============================================================================
// A. Macro cache manager 既有基础路径 (8 个用例)
// ============================================================================

TEST_F(TestSSMacroCacheMgr, put_update_erase)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200001;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // ObSSMacroCacheInfo
  uint64_t effective_tablet_id = 200001;
  uint32_t size = 1 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);

  // 1. put
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 2. update
  ObSSMacroCacheUpdateParam update_param(true/*is_update_lru*/,
                                         true/*is_update_last_access_time*/,
                                         true/*is_update_effective_tablet_id*/,
                                         true/*is_update_size*/,
                                         false/*is_update_marked*/,
                                         false/*is_update_write_cache*/);
  cache_info.effective_tablet_id_ = 200002;
  cache_info.size_ = 2 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update(macro_id, cache_info, update_param));
  check_macro_cache_meta(macro_id, cache_info);

  // 3. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
}

TEST_F(TestSSMacroCacheMgr, put_or_update_and_batch_update)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200001;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // ObSSMacroCacheInfo
  uint64_t effective_tablet_id = 200001;
  uint32_t size = 1 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);

  // 1. put_or_update trigger put
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 2. put_or_update trigger update
  cache_info.effective_tablet_id_ = 200002;
  cache_info.size_ = 2 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id, cache_info));
  check_macro_cache_meta(macro_id, cache_info);

  // 3. put_or_update another macro_id
  MacroBlockId macro_id_2;
  macro_id_2.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id_2.set_second_id(tablet_id);
  macro_id_2.set_reorganization_scn(0); // private_macro_third_id
  macro_id_2.set_tenant_seq(101); // tenant_seq
  ASSERT_TRUE(macro_id_2.is_valid());

  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put_or_update(macro_id_2, cache_info));
  check_macro_cache_meta(macro_id_2, cache_info);

  // 4. batch update
  cache_info.effective_tablet_id_ = 200003;

  ObArray<MacroBlockId> macro_id_arr;
  ASSERT_EQ(OB_SUCCESS, macro_id_arr.push_back(macro_id));
  ASSERT_EQ(OB_SUCCESS, macro_id_arr.push_back(macro_id_2));
  ObSSMacroCacheUpdateParam update_param(true/*is_update_lru*/,
                                         true/*is_update_last_access_time*/,
                                         true/*is_update_effective_tablet_id*/,
                                         true/*is_update_size*/,
                                         false/*is_update_marked*/,
                                         false/*is_update_write_cache*/);
  for (int64_t i = 0; i < macro_id_arr.count(); ++i) {
    const MacroBlockId &cur_macro_id = macro_id_arr.at(i);
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update(cur_macro_id, cache_info, update_param));
  }
  check_macro_cache_meta(macro_id, cache_info);
  check_macro_cache_meta(macro_id_2, cache_info);

  // 5. update_effective_tablet_id
  cache_info.effective_tablet_id_ = 200004;
  ObTabletID cur_tablet_id(cache_info.effective_tablet_id_);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->update_effective_tablet_id(macro_id_arr, cur_tablet_id));
  check_macro_cache_meta(macro_id, cache_info);
  check_macro_cache_meta(macro_id_2, cache_info);

  // 6. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id_2));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id_2, meta_handle));
}

TEST_F(TestSSMacroCacheMgr, update_effective_tablet_id_by_get)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200004;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // 1. write to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // check macro cache meta
  uint64_t effective_tablet_id = tablet_id;
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);
  check_macro_cache_meta(macro_id, cache_info);

  // 2. get with new valid effective_tablet_id,
  //    expect macro_cache_meta's effective_tablet_id to be updated to new_effective_tablet_id
  bool is_hit_cache = false;
  ObSSFdCacheHandle fd_handle;
  ObTabletID new_effective_tablet_id(effective_tablet_id + 1);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, new_effective_tablet_id, size, is_hit_cache, fd_handle));
  ASSERT_EQ(TRUE, is_hit_cache);
  ObSSMacroCacheInfo new_cache_info(new_effective_tablet_id.id(), size, cache_type, is_write_cache);
  check_macro_cache_meta(macro_id, new_cache_info);
  fd_handle.reset();

  // 3. get with invalid effective_tablet_id,
  //    expect macro_cache_meta's effective_tablet_id to remain unchanged
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID), size, is_hit_cache, fd_handle));
  ASSERT_EQ(TRUE, is_hit_cache);
  check_macro_cache_meta(macro_id, new_cache_info); // new_cache_info here is the same as above
  fd_handle.reset();

  // 4. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
}

// ----------------------------------------------------------------------------
// HOT 与 AUTO 类型转换保持 LRU、访问时间和磁盘空间统计一致
//
// 场景：构造一个本地 macro cache，依次执行 AUTO→HOT、HOT→AUTO，并再次发起
//       HOT→AUTO 请求模拟多个 fork tablet 连续触发 Private GC。
// 校验：首次真实转换刷新访问时间；当前类型已不是 ori 时不再 touch；
//       重复降级不删除缓存；两类空间统计守恒。
// ----------------------------------------------------------------------------
TEST_F(TestSSMacroCacheMgr, shift_lru_list)
{
  int ret = OB_SUCCESS;
  LOG_INFO("========== shift_lru_list begin ==========");
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);

  const uint64_t tablet_id = 200005;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // 1. write to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // check macro cache meta
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheInfo cache_info(tablet_id, size, ObSSMacroCacheType::MACRO_BLOCK, true/*is_write_cache*/);
  check_macro_cache_meta(macro_id, cache_info);

  // check disk space
  ObSSMacroCacheStat macro_block_stat;
  ObSSMacroCacheStat hot_tablet_macro_block_stat;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK,
                                                                 macro_block_stat));
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                                 hot_tablet_macro_block_stat));
  const int64_t ori_macro_block_used = macro_block_stat.used_;
  const int64_t ori_hot_tablet_macro_block_used = hot_tablet_macro_block_stat.used_;

  // 2. modify macro cache type from MACRO_BLOCK to HOT_TABLET_MACRO_BLOCK
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->modify_macro_cache_type(
                                         macro_id,
                                         ObSSMacroCacheType::MACRO_BLOCK,
                                         ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));
  // check macro cache meta
  ObSSMacroCacheInfo new_cache_info(tablet_id, size, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, true/*is_write_cache*/);
  check_macro_cache_meta(macro_id, new_cache_info);

  // check disk space
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK,
                                                                 macro_block_stat));
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                                 hot_tablet_macro_block_stat));
  ASSERT_EQ(macro_block_stat.used_, ori_macro_block_used - size);
  ASSERT_EQ(hot_tablet_macro_block_stat.used_, ori_hot_tablet_macro_block_used + size);

  // 3. 模拟 Private GC 将 HOT 降为 AUTO，并验证转换同时刷新访问时间。
  ObSSMacroCacheMetaHandle touch_meta_handle;
  OK(macro_cache_mgr->meta_map_.get_refactored(macro_id, touch_meta_handle));
  ASSERT_TRUE(touch_meta_handle.is_valid());
  touch_meta_handle()->set_last_access_time_us(1);
  OK(macro_cache_mgr->modify_macro_cache_type_and_touch(
      macro_id,
      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
      ObSSMacroCacheType::MACRO_BLOCK));
  // check macro cache meta
  check_macro_cache_meta(macro_id, cache_info);
  ASSERT_GT(touch_meta_handle()->get_last_access_time_us(), 1);

  // 4. 多个 fork tablet 连续 GC 时，已经是 AUTO 的缓存保留且不再 touch。
  touch_meta_handle()->set_last_access_time_us(1);
  OK(macro_cache_mgr->modify_macro_cache_type_and_touch(
      macro_id,
      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
      ObSSMacroCacheType::MACRO_BLOCK));
  check_macro_cache_meta(macro_id, cache_info);
  ASSERT_EQ(1, touch_meta_handle()->get_last_access_time_us());

  // check disk space
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK,
                                                                 macro_block_stat));
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                                 hot_tablet_macro_block_stat));
  ASSERT_EQ(macro_block_stat.used_, ori_macro_block_used);
  ASSERT_EQ(hot_tablet_macro_block_stat.used_, ori_hot_tablet_macro_block_used);

  // 5. erase
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->erase(macro_id));
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_HASH_NOT_EXIST, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  LOG_INFO("========== shift_lru_list end ==========");
}

TEST_F(TestSSMacroCacheMgr, update_lru_list)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);

  const uint64_t tablet_id = 200006;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  MacroBlockId macro_id_2;
  macro_id_2.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id_2.set_second_id(tablet_id);
  macro_id_2.set_reorganization_scn(0); // private_macro_third_id
  macro_id_2.set_tenant_seq(101); // tenant_seq
  ASSERT_TRUE(macro_id_2.is_valid());

  // 1. write to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id_2));
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // check macro cache meta
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheInfo cache_info(tablet_id, size, ObSSMacroCacheType::MACRO_BLOCK, true/*is_write_cache*/);
  check_macro_cache_meta(macro_id, cache_info);
  check_macro_cache_meta(macro_id_2, cache_info);
  ObSSMacroCacheMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_TRUE(meta_handle()->get_is_in_fifo_list());
  const int64_t last_access_time = meta_handle()->get_last_access_time_us(); // record last_access_time for compare below
  meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id_2, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_TRUE(meta_handle()->get_is_in_fifo_list());
  meta_handle.reset();

  // 2. get
  for (int64_t i = 1; i <= ObSSMacroCacheMeta::UPDATE_LRU_LIST_THRESHOLD; ++i) {
    bool is_hit_cache = false;
    ObSSFdCacheHandle fd_handle;
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id), size, is_hit_cache, fd_handle));
    ASSERT_TRUE(is_hit_cache);
    ObSSMacroCacheMetaHandle meta_handle;
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
    ASSERT_TRUE(meta_handle.is_valid());
    if (i == ObSSMacroCacheMeta::UPDATE_LRU_LIST_THRESHOLD) {
      ASSERT_EQ(0, meta_handle()->access_cnt_);
    } else {
      ASSERT_EQ(i, meta_handle()->access_cnt_);
    }
  }
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_FALSE(meta_handle()->get_is_in_fifo_list());
  ASSERT_GT(meta_handle()->get_last_access_time_us(), last_access_time);
  meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id_2, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  ASSERT_TRUE(meta_handle()->get_is_in_fifo_list());
  meta_handle.reset();
}

TEST_F(TestSSMacroCacheMgr, force_evict_by_macro_id_write_cache)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200007;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // 1. write to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // check macro cache meta
  uint64_t effective_tablet_id = tablet_id;
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = true;
  ObSSMacroCacheInfo cache_info(effective_tablet_id, size, cache_type, is_write_cache);
  check_macro_cache_meta(macro_id, cache_info);

  // 2. force evict the macro
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->force_evict_by_macro_id(macro_id));
  ASSERT_EQ(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
}

TEST_F(TestSSMacroCacheMgr, evict_by_macro_id_when_meta_not_exist)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200010;
  const uint64_t server_id = 1;
  bool is_write_cache = false;

  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0);
  macro_id.set_tenant_seq(100);
  ASSERT_TRUE(macro_id.is_valid());

  ASSERT_EQ(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->evict_by_macro_id(macro_id, is_write_cache));
  ASSERT_EQ(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
  ASSERT_FALSE(is_write_cache);
}

TEST_F(TestSSMacroCacheMgr, clear_macro_cache)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  const uint64_t tablet_id = 200008;
  const uint64_t hot_tablet_id = 200009;
  const uint64_t server_id = 1;

  // MacroBlockId
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  // 1.1 write non-HOT_TABLET_MACRO_BLOCK to macro cache
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info_.set_is_write_cache(false); // simulate read_cache
  write_info_.set_effective_tablet_id(tablet_id);
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 1.2 check macro cache meta
  uint32_t size = 2 * 1024 * 1024;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MACRO_BLOCK;
  bool is_write_cache = false;
  ObSSMacroCacheInfo cache_info(tablet_id, size, cache_type, is_write_cache);
  check_macro_cache_meta(macro_id, cache_info);

  
  // 2.1 write HOT_TABLET_MACRO_BLOCK to macro cache
  MacroBlockId hot_macro_id(macro_id);
  hot_macro_id.set_second_id(hot_tablet_id);
  ASSERT_TRUE(hot_macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(hot_macro_id));
  write_info_.set_is_write_cache(false); // simulate read_cache
  write_info_.set_effective_tablet_id(hot_tablet_id);
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 2.2 check macro cache meta
  ObSSMacroCacheInfo hot_cache_info(hot_tablet_id, size, ObSSMacroCacheType::MACRO_BLOCK, is_write_cache);
  check_macro_cache_meta(hot_macro_id, hot_cache_info);

  // 2.3 modify macro cache type from MACRO_BLOCK to HOT_TABLET_MACRO_BLOCK
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->modify_macro_cache_type(
                                         hot_macro_id,
                                         ObSSMacroCacheType::MACRO_BLOCK,
                                         ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));
  hot_cache_info.cache_type_ = ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK;
  check_macro_cache_meta(hot_macro_id, hot_cache_info);

  // 3. clear macro cache
  const int64_t abs_timeout_us = ObTimeUtility::current_time() + 10000000L; // 10s
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->clear_macro_cache(abs_timeout_us, false/*is_hot_tablet*/));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(hot_macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  // 4. clear hot tablet macro cache
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->clear_macro_cache(abs_timeout_us, true/*is_hot_tablet*/));
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(hot_macro_id, is_exist));
  ASSERT_FALSE(is_exist);
}

// ============================================================================
// B. Private GC shared cache 与 HOT cache GC 路径 (4 个用例)
// ============================================================================

// ----------------------------------------------------------------------------
// B1. Private GC 开关打开后保留 shared local cache，已降级的 AUTO cache 不再 touch
//
// 场景：构造一个 HOT shared macro，模拟多个 fork tablet 先后对同一 macro 执行 Private GC。
// 操作：打开软降级开关，连续两次调用 delete_macro_cache_。
// 校验：第一次 HOT→AUTO 并 touch；第二次保持访问时间不变；每次 GC 后本地文件均可命中。
// ----------------------------------------------------------------------------
TEST_F(TestSSMacroCacheMgr, private_gc_keeps_shared_cache_without_retouching_auto)
{
  LOG_INFO("========== private_gc_keeps_shared_cache_without_retouching_auto begin ==========");
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto = true;
  }

  // step 1：写入真实 shared local cache，并将其转成 HOT。
  constexpr uint64_t TABLET_ID = 210001;
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  macro_id.set_second_id(TABLET_ID);
  macro_id.set_seq_id(1001);
  ASSERT_TRUE(macro_id.is_valid());
  OK(write_shared_read_cache(macro_id, TABLET_ID));
  OK(macro_cache_mgr->modify_macro_cache_type(
      macro_id,
      ObSSMacroCacheType::MACRO_BLOCK,
      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));

  ObSSMacroCacheMetaHandle meta_handle;
  OK(macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  meta_handle()->set_last_access_time_us(1);

  // step 2：首次 Private GC 将 HOT 软降级为 AUTO，但不删除本地缓存。
  bool is_stopped = false;
  TestPrivateGCBlockHandler gc_handler(is_stopped);
  ObSEArray<MacroBlockId, 1> block_ids;
  block_ids.set_attr(ObMemAttr(MTL_ID(), "PriGCShared"));
  OK(block_ids.push_back(macro_id));
  OK(gc_handler.delete_macro_cache_(block_ids));
  const int64_t first_gc_touch_time_us = meta_handle()->get_last_access_time_us();
  LOG_INFO("step 2: verify first private gc result",
      K(macro_id), K(first_gc_touch_time_us), KPC(meta_handle()));
  ASSERT_EQ(ObSSMacroCacheType::MACRO_BLOCK, meta_handle()->get_cache_type());
  ASSERT_GT(first_gc_touch_time_us, 1);

  bool is_hit_cache = false;
  ObSSFdCacheHandle fd_handle;
  OK(macro_cache_mgr->get(
      macro_id, ObTabletID(TABLET_ID), WRITE_IO_SIZE, is_hit_cache, fd_handle));
  ASSERT_TRUE(is_hit_cache);
  fd_handle.reset();

  // step 3：以第一次 GC 后的最新访问时间为基准，验证第二次 GC 不再 touch AUTO macro。
  const int64_t access_time_before_second_gc_us = meta_handle()->get_last_access_time_us();
  OK(gc_handler.delete_macro_cache_(block_ids));
  const int64_t access_time_after_second_gc_us = meta_handle()->get_last_access_time_us();
  LOG_INFO("step 3: verify repeated private gc result",
      K(macro_id), K(access_time_before_second_gc_us), K(access_time_after_second_gc_us), KPC(meta_handle()));
  ASSERT_EQ(ObSSMacroCacheType::MACRO_BLOCK, meta_handle()->get_cache_type());
  ASSERT_EQ(access_time_before_second_gc_us, access_time_after_second_gc_us);

  is_hit_cache = false;
  OK(macro_cache_mgr->get(
      macro_id, ObTabletID(TABLET_ID), WRITE_IO_SIZE, is_hit_cache, fd_handle));
  ASSERT_TRUE(is_hit_cache);
  fd_handle.reset();
  meta_handle.reset();

  // step 4：清理用例产物，避免影响后续 case。
  OK(ObSSObjectAccessUtil::delete_local_file(macro_id));
  LOG_INFO("========== private_gc_keeps_shared_cache_without_retouching_auto end ==========");
}

// ----------------------------------------------------------------------------
// B2. Private GC 开关关闭时保持原有 shared local cache 删除行为
//
// 场景：构造一个 HOT shared macro，保持软降级开关关闭。
// 操作：调用 Private GC 的 delete_macro_cache_。
// 校验：cache meta 和本地文件均被删除，确保默认配置下兼容旧逻辑。
// ----------------------------------------------------------------------------
TEST_F(TestSSMacroCacheMgr, private_gc_deletes_shared_cache_when_switch_is_disabled)
{
  LOG_INFO("========== private_gc_deletes_shared_cache_when_switch_is_disabled begin ==========");
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto = false;
  }

  // step 1：写入 HOT shared local cache。
  constexpr uint64_t TABLET_ID = 210002;
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  macro_id.set_second_id(TABLET_ID);
  macro_id.set_seq_id(1002);
  ASSERT_TRUE(macro_id.is_valid());
  OK(write_shared_read_cache(macro_id, TABLET_ID));
  OK(macro_cache_mgr->modify_macro_cache_type(
      macro_id,
      ObSSMacroCacheType::MACRO_BLOCK,
      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));

  // step 2：执行默认 Private GC，并同时校验 meta 与物理本地缓存。
  bool is_stopped = false;
  TestPrivateGCBlockHandler gc_handler(is_stopped);
  ObSEArray<MacroBlockId, 1> block_ids;
  block_ids.set_attr(ObMemAttr(MTL_ID(), "PriGCShared"));
  OK(block_ids.push_back(macro_id));
  OK(gc_handler.delete_macro_cache_(block_ids));

  bool is_exist = true;
  OK(macro_cache_mgr->exist(macro_id, is_exist));
  LOG_INFO("step 2: verify disabled switch result", K(macro_id), K(is_exist));
  ASSERT_FALSE(is_exist);
  bool is_hit_cache = true;
  ObSSFdCacheHandle fd_handle;
  OK(macro_cache_mgr->get(
      macro_id, ObTabletID(TABLET_ID), WRITE_IO_SIZE, is_hit_cache, fd_handle));
  ASSERT_FALSE(is_hit_cache);
  fd_handle.reset();
  LOG_INFO("========== private_gc_deletes_shared_cache_when_switch_is_disabled end ==========");
}

// ----------------------------------------------------------------------------
// B3. HOT cache GC 按配置删除或降级无引用且已经超过 idle time 的本地宏块
//
// 场景：同一 HOT tablet 下构造“仍被引用”、“无引用但最近访问”、“无引用且长时间未访问”三个 macro。
// 操作：先关闭软降级执行删除，再打开软降级执行 HOT→AUTO。
// 校验：引用中或未空闲的 macro 保留 HOT；符合回收条件的 macro 按配置删除或降为 AUTO。
// ----------------------------------------------------------------------------
TEST_F(TestSSMacroCacheMgr, hot_cache_gc_respects_reference_and_idle_time)
{
  LOG_INFO("========== hot_cache_gc_respects_reference_and_idle_time begin ==========");
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  constexpr uint64_t TABLET_ID = 210003;
  constexpr int64_t IDLE_TIME_US = 60L * 60L * 1000L * 1000L;
  const int64_t current_time_us = ObTimeUtility::current_time_us();
  ObSEArray<MacroBlockId, 3> macro_ids;
  macro_ids.set_attr(ObMemAttr(MTL_ID(), "HotGCTest"));

  // step 1：写入三个 HOT shared local cache，并设置可控的访问时间。
  for (int64_t i = 0; i < 3; ++i) {
    MacroBlockId macro_id;
    macro_id.set_storage_object_type(
        static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
    macro_id.set_second_id(TABLET_ID);
    macro_id.set_seq_id(1100 + i);
    ASSERT_TRUE(macro_id.is_valid());
    OK(macro_ids.push_back(macro_id));
    OK(write_shared_read_cache(macro_id, TABLET_ID));
    OK(macro_cache_mgr->modify_macro_cache_type(
        macro_id,
        ObSSMacroCacheType::MACRO_BLOCK,
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));

    ObSSMacroCacheMetaHandle meta_handle;
    OK(macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
    ASSERT_TRUE(meta_handle.is_valid());
    const int64_t last_access_time_us =
        (1 == i) ? current_time_us : current_time_us - 2 * IDLE_TIME_US;
    meta_handle()->set_last_access_time_us(last_access_time_us);
  }

  // step 2：只将第一个 macro 放入引用集合，关闭软降级触发 HOT cache GC。
  ObHashSet<MacroBlockId> referenced_macro_set;
  const ObMemAttr mem_attr(MTL_ID(), "HotGCTest");
  OK(referenced_macro_set.create(16, mem_attr));
  OK(referenced_macro_set.set_refactored(macro_ids.at(0)));
  OK(macro_cache_mgr->gc_hot_tablet_macro_cache(
      referenced_macro_set, IDLE_TIME_US, false /*convert_to_auto*/));

  // step 3：断言三种条件的结果，避免只验证单一删除 happy path。
  bool referenced_exists = false;
  bool recent_exists = false;
  bool idle_exists = true;
  OK(macro_cache_mgr->exist(macro_ids.at(0), referenced_exists));
  OK(macro_cache_mgr->exist(macro_ids.at(1), recent_exists));
  OK(macro_cache_mgr->exist(macro_ids.at(2), idle_exists));
  LOG_INFO("step 3: verify hot cache gc result",
      K(referenced_exists), K(recent_exists), K(idle_exists), K(IDLE_TIME_US));
  ASSERT_TRUE(referenced_exists);
  ASSERT_TRUE(recent_exists);
  ASSERT_FALSE(idle_exists);

  // step 4：将第二个 macro 设为长时间未访问，打开软降级后应保留文件并转为 AUTO。
  ObSSMacroCacheMetaHandle referenced_meta_handle;
  ObSSMacroCacheMetaHandle unreferenced_meta_handle;
  OK(macro_cache_mgr->meta_map_.get_refactored(macro_ids.at(0), referenced_meta_handle));
  OK(macro_cache_mgr->meta_map_.get_refactored(macro_ids.at(1), unreferenced_meta_handle));
  ASSERT_TRUE(referenced_meta_handle.is_valid());
  ASSERT_TRUE(unreferenced_meta_handle.is_valid());
  const int64_t stale_access_time_us = current_time_us - 2 * IDLE_TIME_US;
  unreferenced_meta_handle()->set_last_access_time_us(stale_access_time_us);
  OK(macro_cache_mgr->gc_hot_tablet_macro_cache(
      referenced_macro_set, IDLE_TIME_US, true /*convert_to_auto*/));

  OK(macro_cache_mgr->exist(macro_ids.at(1), recent_exists));
  LOG_INFO("step 4: verify unreferenced hot cache is converted to auto",
      K(recent_exists), KPC(referenced_meta_handle()), KPC(unreferenced_meta_handle()));
  ASSERT_TRUE(recent_exists);
  ASSERT_EQ(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
      referenced_meta_handle()->get_cache_type());
  ASSERT_EQ(ObSSMacroCacheType::MACRO_BLOCK,
      unreferenced_meta_handle()->get_cache_type());
  ASSERT_EQ(stale_access_time_us, unreferenced_meta_handle()->get_last_access_time_us());
  referenced_meta_handle.reset();
  unreferenced_meta_handle.reset();

  // step 5：清理仍被保留的两个本地缓存。
  OK(ObSSObjectAccessUtil::delete_local_file(macro_ids.at(0)));
  OK(ObSSObjectAccessUtil::delete_local_file(macro_ids.at(1)));
  LOG_INFO("========== hot_cache_gc_respects_reference_and_idle_time end ==========");
}

// ----------------------------------------------------------------------------
// B4. Deleted owner 的 shared HOT cache 安全降为 AUTO，并在真实重新标热时更新 owner
//
// 场景：shared macro 的 effective owner A 已不在 active tablet 集合，B/C 代表后续 HOT 预热任务。
// 操作：deleted-tablet cleanup 先将 HOT(A)→AUTO(A)，随后以 B 执行 AUTO→HOT，再以 C 重复标热。
// 校验：cleanup 不删除本地文件，首次 HOT→AUTO 会 touch，重复 cleanup 不再 touch；
//       只有真实 AUTO→HOT 时 owner 从 A 更新为 B。
// ----------------------------------------------------------------------------
TEST_F(TestSSMacroCacheMgr, deleted_owner_hot_cache_is_demoted_and_reowned_on_promotion)
{
  LOG_INFO("========== deleted_owner_hot_cache_is_demoted_and_reowned_on_promotion begin ==========");
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  constexpr uint64_t DELETED_OWNER_A = 210004;
  constexpr uint64_t HOT_TABLET_B = 210005;
  constexpr uint64_t HOT_TABLET_C = 210006;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto = true;
  }

  // step 1：写入 owner=A 的真实 shared local cache，并将其转成 HOT。
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  macro_id.set_second_id(DELETED_OWNER_A);
  macro_id.set_seq_id(1200);
  ASSERT_TRUE(macro_id.is_valid());
  OK(write_shared_read_cache(macro_id, DELETED_OWNER_A));
  OK(macro_cache_mgr->modify_macro_cache_type(
      macro_id,
      ObSSMacroCacheType::MACRO_BLOCK,
      ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK));

  ObSSMacroCacheMetaHandle meta_handle;
  OK(macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  meta_handle()->set_last_access_time_us(1);

  // step 2：cleanup 确认 A 不 active 后做 HOT→AUTO 并 touch，保留 meta 和本地文件。
  OK(macro_cache_mgr->clean_deleted_tablet_());
  const int64_t demote_access_time_us = meta_handle()->get_last_access_time_us();
  LOG_INFO("step 2: verify deleted owner cache demotion",
      K(macro_id), K(DELETED_OWNER_A), K(demote_access_time_us), KPC(meta_handle()));
  ASSERT_EQ(ObSSMacroCacheType::MACRO_BLOCK, meta_handle()->get_cache_type());
  ASSERT_EQ(DELETED_OWNER_A, meta_handle()->get_effective_tablet_id());
  ASSERT_GT(demote_access_time_us, 1);

  // step 3：已是 AUTO 时重复 cleanup 不再 touch，保证无引用缓存后续能进入 expire。
  meta_handle()->set_last_access_time_us(1);
  OK(macro_cache_mgr->clean_deleted_tablet_());
  LOG_INFO("step 3: verify auto cache is retained without touch", K(macro_id), KPC(meta_handle()));
  ASSERT_EQ(ObSSMacroCacheType::MACRO_BLOCK, meta_handle()->get_cache_type());
  ASSERT_EQ(1, meta_handle()->get_last_access_time_us());

  bool is_hit_cache = false;
  ObSSFdCacheHandle fd_handle;
  OK(macro_cache_mgr->get(
      macro_id, ObTabletID(DELETED_OWNER_A), WRITE_IO_SIZE, is_hit_cache, fd_handle));
  ASSERT_TRUE(is_hit_cache);
  fd_handle.reset();

  // step 4：B 真实执行 AUTO→HOT 时接管 owner；已是 HOT 时 C 的重复请求不再改 owner。
  OK(macro_cache_mgr->promote_macro_cache_to_hot(
      macro_id, ObTabletID(HOT_TABLET_B)));
  ASSERT_EQ(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, meta_handle()->get_cache_type());
  ASSERT_EQ(HOT_TABLET_B, meta_handle()->get_effective_tablet_id());

  OK(macro_cache_mgr->promote_macro_cache_to_hot(
      macro_id, ObTabletID(HOT_TABLET_C)));
  LOG_INFO("step 4: verify owner only changes on actual promotion",
      K(macro_id), K(HOT_TABLET_B), K(HOT_TABLET_C), KPC(meta_handle()));
  ASSERT_EQ(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, meta_handle()->get_cache_type());
  ASSERT_EQ(HOT_TABLET_B, meta_handle()->get_effective_tablet_id());
  meta_handle.reset();

  // step 5：清理真实本地文件，避免影响后续 case。
  OK(ObSSObjectAccessUtil::delete_local_file(macro_id));
  LOG_INFO("========== deleted_owner_hot_cache_is_demoted_and_reowned_on_promotion end ==========");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_mgr.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
