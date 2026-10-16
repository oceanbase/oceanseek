/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "lib/utility/ob_test_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "sensitive_test/object_storage/test_object_storage.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{

class TestSSMacroCacheStat : public ::testing::Test
{
public:
  TestSSMacroCacheStat() {}
  virtual ~TestSSMacroCacheStat() {}

  virtual void SetUp() override
  {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    OK(TestSSMacroCacheMgrUtil::clean_macro_cache_mgr());
    ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
    ASSERT_TRUE(macro_cache_mgr->tablet_stat_map_.empty());
  }

  virtual void TearDown() override
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto = false;
    }
  }

  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    OK(MockTenantModuleEnv::get_instance().init());
    ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());

    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
    macro_cache_mgr->flush_task_.is_inited_ = false;
    macro_cache_mgr->evict_task_.is_inited_ = false;
    macro_cache_mgr->expire_task_.is_inited_ = false;
    macro_cache_mgr->ckpt_task_.is_inited_ = false;
  }

  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;
    MockTenantModuleEnv::get_instance().destroy();
    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
    }
  }
};

TEST_F(TestSSMacroCacheStat, test_cache_space_stat)
{
  ObSSCacheSpaceStat space_stat;
  ASSERT_TRUE(space_stat.is_valid());
  ASSERT_EQ(0, space_stat.get_block_count());
  ASSERT_EQ(0, space_stat.get_required_size());

  space_stat.add(2, 2048);
  ASSERT_TRUE(space_stat.is_valid());
  ASSERT_EQ(2, space_stat.get_block_count());
  ASSERT_EQ(2048, space_stat.get_required_size());

  space_stat.sub(1, 1024);
  ASSERT_EQ(1, space_stat.get_block_count());
  ASSERT_EQ(1024, space_stat.get_required_size());

  ObSSCacheSpaceStat other_space_stat;
  other_space_stat.add(3, 4096);
  space_stat.merge(other_space_stat);
  ASSERT_EQ(4, space_stat.get_block_count());
  ASSERT_EQ(5120, space_stat.get_required_size());
}

TEST_F(TestSSMacroCacheStat, test_macro_cache_queue_space_stat)
{
  ObSSMacroCacheQueueSpaceStat queue_space;
  queue_space.add(false/*is_hot*/, true/*is_in_fifo*/, 1/*block_count*/, 100/*required_size*/);
  queue_space.add(false/*is_hot*/, false/*is_in_fifo*/, 2/*block_count*/, 200/*required_size*/);
  queue_space.add(true/*is_hot*/, true/*is_in_fifo*/, 3/*block_count*/, 300/*required_size*/);
  queue_space.add(true/*is_hot*/, false/*is_in_fifo*/, 4/*block_count*/, 400/*required_size*/);

  ASSERT_TRUE(queue_space.is_valid());
  ASSERT_EQ(4, queue_space.get_fifo_block_count());
  ASSERT_EQ(400, queue_space.get_fifo_required_size());
  ASSERT_EQ(6, queue_space.get_lru_block_count());
  ASSERT_EQ(600, queue_space.get_lru_required_size());
  ASSERT_EQ(10, queue_space.get_valid_block_count());
  ASSERT_EQ(1000, queue_space.get_valid_required_size());
  ASSERT_EQ(7, queue_space.get_hot_block_count());
  ASSERT_EQ(700, queue_space.get_hot_required_size());
}

TEST_F(TestSSMacroCacheStat, test_macro_cache_ls_info_serialize)
{
  ObSSMacroCacheLSInfo ls_info;
  ls_info.set_ls_id(ObLSID(1001));
  ls_info.set_tablet_cnt(2);
  ls_info.add_queue_space(false/*is_hot*/, true/*is_in_fifo*/, 1/*block_count*/, 100/*required_size*/);
  ls_info.add_queue_space(true/*is_hot*/, false/*is_in_fifo*/, 2/*block_count*/, 200/*required_size*/);
  ASSERT_TRUE(ls_info.is_valid());
  ASSERT_EQ(3, ls_info.get_valid_block_count());
  ASSERT_EQ(300, ls_info.get_valid_size());
  ASSERT_EQ(2, ls_info.get_hot_block_count());
  ASSERT_EQ(200, ls_info.get_hot_required_size());

  const int64_t buf_size = ls_info.get_serialize_size();
  char buf[buf_size];
  int64_t pos = 0;
  OK(ls_info.serialize(buf, buf_size, pos));
  ASSERT_EQ(buf_size, pos);

  ObSSMacroCacheLSInfo new_ls_info;
  pos = 0;
  OK(new_ls_info.deserialize(buf, buf_size, pos));
  ASSERT_EQ(ls_info.get_ls_id(), new_ls_info.get_ls_id());
  ASSERT_EQ(ls_info.get_tablet_cnt(), new_ls_info.get_tablet_cnt());
  ASSERT_EQ(ls_info.get_valid_block_count(), new_ls_info.get_valid_block_count());
  ASSERT_EQ(ls_info.get_valid_size(), new_ls_info.get_valid_size());
  ASSERT_EQ(ls_info.get_hot_block_count(), new_ls_info.get_hot_block_count());
  ASSERT_EQ(ls_info.get_hot_required_size(), new_ls_info.get_hot_required_size());
}

TEST_F(TestSSMacroCacheStat, test_local_cache_ls_info_serialize_macro_space)
{
  ObSSLocalCacheLSInfo local_cache_ls_info;
  local_cache_ls_info.macro_cache_ls_info_.set_ls_id(ObLSID(1001));
  local_cache_ls_info.macro_cache_ls_info_.set_tablet_cnt(2);
  local_cache_ls_info.macro_cache_ls_info_.add_queue_space(
      false/*is_hot*/, true/*is_in_fifo*/, 1/*block_count*/, 100/*required_size*/);
  local_cache_ls_info.macro_cache_ls_info_.add_queue_space(
      true/*is_hot*/, false/*is_in_fifo*/, 2/*block_count*/, 200/*required_size*/);
  ASSERT_TRUE(local_cache_ls_info.is_valid());

  const int64_t buf_size = local_cache_ls_info.get_serialize_size();
  char buf[buf_size];
  int64_t pos = 0;
  OK(local_cache_ls_info.serialize(buf, buf_size, pos));
  ASSERT_EQ(buf_size, pos);

  ObSSLocalCacheLSInfo new_local_cache_ls_info;
  pos = 0;
  OK(new_local_cache_ls_info.deserialize(buf, buf_size, pos));
  ASSERT_TRUE(new_local_cache_ls_info.is_valid());
  ASSERT_EQ(local_cache_ls_info.get_valid_size(), new_local_cache_ls_info.get_valid_size());
  ASSERT_EQ(local_cache_ls_info.macro_cache_ls_info_.get_valid_block_count(),
      new_local_cache_ls_info.macro_cache_ls_info_.get_valid_block_count());
  ASSERT_EQ(local_cache_ls_info.macro_cache_ls_info_.get_valid_size(),
      new_local_cache_ls_info.macro_cache_ls_info_.get_valid_size());
  ASSERT_EQ(local_cache_ls_info.macro_cache_ls_info_.get_hot_block_count(),
      new_local_cache_ls_info.macro_cache_ls_info_.get_hot_block_count());
  ASSERT_EQ(local_cache_ls_info.macro_cache_ls_info_.get_hot_required_size(),
      new_local_cache_ls_info.macro_cache_ls_info_.get_hot_required_size());
}

TEST_F(TestSSMacroCacheStat, test_tablet_stat)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB

  // 1. put a macro into mgr
  const uint64_t tablet_id = 212345;
  const uint64_t server_id = 1;

  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  ObSSMacroCacheInfo macro_cache_info(
      tablet_id, WRITE_IO_SIZE, ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
  ASSERT_TRUE(macro_cache_info.is_valid());

  OK(macro_cache_mgr->put(macro_id, macro_cache_info));
  ASSERT_EQ(1, macro_cache_mgr->meta_map_.size());
  ASSERT_NE(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
  
  macro_cache_mgr->tablet_stat_task_.runTimerTask();
  ASSERT_EQ(1, macro_cache_mgr->tablet_stat_map_.size());
  const ObSSMacroCacheTabletStat *tablet_stat = macro_cache_mgr->tablet_stat_map_.get(tablet_id);
  ASSERT_NE(nullptr, tablet_stat);
  ASSERT_EQ(WRITE_IO_SIZE, tablet_stat->get_total_cached_size());
  ASSERT_EQ(1, tablet_stat->get_total_block_count());
  ASSERT_EQ(0, tablet_stat->cache_hit_cnt_);
  ASSERT_EQ(0, tablet_stat->cache_hit_bytes_);
  ASSERT_EQ(0, tablet_stat->cache_miss_cnt_);
  ASSERT_EQ(0, tablet_stat->cache_miss_bytes_);

  // access the macro
  const int64_t access_size = 11;
  bool is_hit_cache = false;
  ObSSFdCacheHandle fd_handle;
  ObTabletID effective_tablet_id(tablet_id);
  OK(macro_cache_mgr->get(macro_id, effective_tablet_id, access_size, is_hit_cache, fd_handle));
  ASSERT_FALSE(is_hit_cache);
  fd_handle.reset();
  
  ASSERT_EQ(WRITE_IO_SIZE, tablet_stat->get_total_cached_size());
  ASSERT_EQ(1, tablet_stat->get_total_block_count());
  ASSERT_EQ(0, tablet_stat->cache_hit_cnt_);
  ASSERT_EQ(0, tablet_stat->cache_hit_bytes_);
  ASSERT_EQ(1, tablet_stat->cache_miss_cnt_);
  ASSERT_EQ(access_size, tablet_stat->cache_miss_bytes_);

  // 3. write to disk
  ObStorageObjectWriteInfo write_info;
  char write_buf[WRITE_IO_SIZE];
  write_buf[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf, 'a', mid_offset);
  memset(write_buf + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  OK(macro_cache_mgr->get(macro_id, effective_tablet_id, access_size * 2, is_hit_cache, fd_handle));
  ASSERT_TRUE(is_hit_cache);
  fd_handle.reset();

  ASSERT_EQ(WRITE_IO_SIZE, tablet_stat->get_total_cached_size());
  ASSERT_EQ(1, tablet_stat->get_total_block_count());
  ASSERT_EQ(1, tablet_stat->cache_hit_cnt_);
  ASSERT_EQ(access_size * 2, tablet_stat->cache_hit_bytes_);
  ASSERT_EQ(1, tablet_stat->cache_miss_cnt_);
  ASSERT_EQ(access_size, tablet_stat->cache_miss_bytes_);

  // 4. clean tablet
  ObSSMacroCacheMetaHandle meta_handle;
  OK(macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  meta_handle()->set_last_access_time_us(ObTimeUtility::fast_current_time() - 3600LL * 1000LL * 1000LL);
  meta_handle.reset();
  macro_cache_mgr->clean_deleted_tablet_();
  ASSERT_TRUE(macro_cache_mgr->tablet_stat_map_.empty());
  ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
}

TEST_F(TestSSMacroCacheStat, test_tablet_stat_after_evict)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB

  // 1. put a macro into mgr
  const uint64_t tablet_id = 212345;
  const uint64_t server_id = 1;

  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0); // private_macro_third_id
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  ObSSMacroCacheInfo macro_cache_info(
      tablet_id, WRITE_IO_SIZE, ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
  ASSERT_TRUE(macro_cache_info.is_valid());

  OK(macro_cache_mgr->put(macro_id, macro_cache_info));
  ASSERT_EQ(1, macro_cache_mgr->meta_map_.size());
  ASSERT_NE(nullptr, macro_cache_mgr->meta_map_.get(macro_id));

  // 2. write to disk
  ObStorageObjectWriteInfo write_info;
  char write_buf[WRITE_IO_SIZE];
  write_buf[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf, 'a', mid_offset);
  memset(write_buf + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 3. evict the read macro cache
  macro_cache_mgr->tablet_stat_task_.runTimerTask();
  const ObSSMacroCacheTabletStat *tablet_stat = macro_cache_mgr->tablet_stat_map_.get(tablet_id);
  ASSERT_NE(nullptr, tablet_stat);
  ASSERT_EQ(WRITE_IO_SIZE, tablet_stat->get_total_cached_size());
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->force_evict_by_macro_id(macro_id));
  ASSERT_EQ(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
  macro_cache_mgr->tablet_stat_task_.runTimerTask();
  ASSERT_EQ(1, macro_cache_mgr->tablet_stat_map_.size());
  tablet_stat = macro_cache_mgr->tablet_stat_map_.get(tablet_id);
  ASSERT_EQ(0, tablet_stat->get_total_cached_size());
  ASSERT_EQ(0, tablet_stat->get_total_block_count());
}

// ----------------------------------------------------------------------------
// Deleted tablet cleanup 保留 shared macro cache meta 时，仍按 active tablet 集合清理 stat
//
// 场景：使用不存在的 tablet ID 构造 shared macro cache 和对应 stat。
// 操作：分别在开关打开和关闭时执行 cleanup。
// 校验：(a) 开关打开时 shared meta 保留，deleted tablet stat 清理；
//       (b) 开关关闭时 cleanup 按原逻辑同时清理 shared meta 和 stat。
// ----------------------------------------------------------------------------
TEST_F(TestSSMacroCacheStat, shared_cache_meta_survives_deleted_tablet_stat_cleanup)
{
  LOG_INFO("========== shared_cache_meta_survives_deleted_tablet_stat_cleanup begin ==========");
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  const int64_t CACHE_SIZE = DIO_READ_ALIGN_SIZE * 256;

  // step 1：构造 deleted tablet 的 shared cache meta 和 cached-size stat。
  constexpr uint64_t PROTECTED_TABLET_ID = 212346;
  MacroBlockId protected_macro_id;
  protected_macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  protected_macro_id.set_second_id(PROTECTED_TABLET_ID);
  protected_macro_id.set_seq_id(2001);
  ASSERT_TRUE(protected_macro_id.is_valid());
  ObSSMacroCacheInfo protected_cache_info(
      PROTECTED_TABLET_ID,
      CACHE_SIZE,
      ObSSMacroCacheType::MACRO_BLOCK,
      false /*is_write_cache*/);
  OK(macro_cache_mgr->put(protected_macro_id, protected_cache_info));
  OK(macro_cache_mgr->calibrate_tablet_stat_cached_size_());
  ASSERT_NE(nullptr, macro_cache_mgr->tablet_stat_map_.get(PROTECTED_TABLET_ID));

  ObSSMacroCacheMetaHandle meta_handle;
  OK(macro_cache_mgr->meta_map_.get_refactored(protected_macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  meta_handle()->set_last_access_time_us(1);
  meta_handle.reset();

  // step 2：开关打开时保留 shared cache meta，但 deleted tablet stat 仍按 active tablet 集合清理。
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto = true;
  }
  OK(macro_cache_mgr->clean_deleted_tablet_());
  bool is_exist = false;
  OK(macro_cache_mgr->exist(protected_macro_id, is_exist));
  const ObSSMacroCacheTabletStat *tablet_stat =
      macro_cache_mgr->tablet_stat_map_.get(PROTECTED_TABLET_ID);
  LOG_INFO("step 2: verify retained shared cache and cleaned stat",
      K(protected_macro_id), K(is_exist), KP(tablet_stat));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(nullptr, tablet_stat);

  // step 3：显式清理已保留的 shared cache meta，避免影响后续场景。
  OK(macro_cache_mgr->erase(protected_macro_id));
  tablet_stat = macro_cache_mgr->tablet_stat_map_.get(PROTECTED_TABLET_ID);
  LOG_INFO("step 3: verify deleted tablet stat remains cleaned", KP(tablet_stat), K(PROTECTED_TABLET_ID));
  ASSERT_EQ(nullptr, tablet_stat);

  // step 4：开关关闭时，shared cache 仍按 deleted effective_tablet_id 执行原有清理。
  constexpr uint64_t LEGACY_TABLET_ID = 212347;
  MacroBlockId legacy_macro_id;
  legacy_macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  legacy_macro_id.set_second_id(LEGACY_TABLET_ID);
  legacy_macro_id.set_seq_id(2002);
  ASSERT_TRUE(legacy_macro_id.is_valid());
  ObSSMacroCacheInfo legacy_cache_info(
      LEGACY_TABLET_ID,
      CACHE_SIZE,
      ObSSMacroCacheType::MACRO_BLOCK,
      false /*is_write_cache*/);
  OK(macro_cache_mgr->put(legacy_macro_id, legacy_cache_info));
  OK(macro_cache_mgr->calibrate_tablet_stat_cached_size_());
  OK(macro_cache_mgr->meta_map_.get_refactored(legacy_macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  meta_handle()->set_last_access_time_us(1);
  meta_handle.reset();

  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(tenant_config.is_valid());
    tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto = false;
  }
  OK(macro_cache_mgr->clean_deleted_tablet_());
  OK(macro_cache_mgr->exist(legacy_macro_id, is_exist));
  tablet_stat = macro_cache_mgr->tablet_stat_map_.get(LEGACY_TABLET_ID);
  LOG_INFO("step 4: verify legacy cleanup", K(legacy_macro_id), K(is_exist), KP(tablet_stat));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(nullptr, tablet_stat);
  LOG_INFO("========== shared_cache_meta_survives_deleted_tablet_stat_cleanup end ==========");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_stat.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_stat.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}