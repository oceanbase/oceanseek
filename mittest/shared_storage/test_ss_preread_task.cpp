/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#include <future>
#include <thread>
#define protected public
#define private public
#include "lib/alloc/alloc_func.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/stat/ob_diagnostic_info_summary.h"
#include "lib/utility/ob_defer.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "share/ob_thread_mgr.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
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

class TestSSPreReadTask : public ::testing::Test
{
public:
  TestSSPreReadTask() {}
  virtual ~TestSSPreReadTask() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  int prepare_completed_preread(ObPrereadCacheManager &preread_cache_mgr,
      const int64_t tmp_file_id, ObSSPreReadEntry *&preread_entry);
  int prepare_completed_preread(ObPrereadCacheManager &preread_cache_mgr,
      const ObPreReadFileMeta &file_meta, const ObStorageObjectWriteInfo &write_info,
      ObSSPreReadEntry *&preread_entry);
  void check_repeated_put_eagain(const bool stop_after_retries);

public:
  static const int64_t WRITE_IO_SIZE = 257 * 1024; // 257KB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
  int64_t original_tenant_memory_limit_ = 0;
  bool tenant_memory_limit_overridden_ = false;
};

void TestSSPreReadTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestSSPreReadTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSPreReadTask::SetUp()
{
  original_tenant_memory_limit_ = lib::get_tenant_memory_limit(MTL_ID());
  tenant_memory_limit_overridden_ = false;
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
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;

  // construct read info
  read_buf_[0] = '\0';
  read_info_.io_desc_.set_wait_event(1);
  read_info_.buf_ = read_buf_;
  read_info_.offset_ = 0;
  read_info_.size_ = WRITE_IO_SIZE;
  read_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info_.mtl_tenant_id_ = MTL_ID();

  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  tenant_kvcache_mgr->disable_cache();
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->erase_tenant_entries());

  // erase_tenant_entries() 不取消已提交的 L2 IO。先等待在途 IO 回收，再清理
  // L2，确保每个用例都从空的两级缓存开始，避免用基线偏移掩盖残留数据。
  const int64_t wait_start_us = ObTimeUtility::current_time();
  const int64_t wait_timeout_us = 10 * 1000 * 1000L;
  while ((tenant_kvcache_mgr->persist_worker_.ring_buffer_.size() > 0
      || tenant_kvcache_mgr->persist_worker_.write_task_.inflight_count() > 0)
      && ObTimeUtility::current_time() - wait_start_us < wait_timeout_us) {
    ob_usleep(10 * 1000L);
  }
  // 给已经从 Ring Buffer 取出、但尚未进入 WriteTask 的 entry 一个处理周期。
  ob_usleep(20 * 1000L);
  ASSERT_EQ(0, tenant_kvcache_mgr->persist_worker_.ring_buffer_.size());
  ASSERT_EQ(0,
      tenant_kvcache_mgr->persist_worker_.write_task_.inflight_count());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::clean_macro_cache_mgr());
  tenant_kvcache_mgr->enable_cache();
}

void TestSSPreReadTask::TearDown()
{
  if (tenant_memory_limit_overridden_) {
    lib::set_tenant_memory_limit(MTL_ID(), original_tenant_memory_limit_);
    tenant_memory_limit_overridden_ = false;
  }
  original_tenant_memory_limit_ = 0;
  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

int TestSSPreReadTask::prepare_completed_preread(ObPrereadCacheManager &preread_cache_mgr,
    const int64_t tmp_file_id, ObSSPreReadEntry *&preread_entry)
{
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id.set_second_id(tmp_file_id);
  macro_id.set_fourth_id(1);
  const ObPreReadFileMeta file_meta(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID), 0);
  return prepare_completed_preread(preread_cache_mgr, file_meta, write_info_, preread_entry);
}

int TestSSPreReadTask::prepare_completed_preread(ObPrereadCacheManager &preread_cache_mgr,
    const ObPreReadFileMeta &file_meta, const ObStorageObjectWriteInfo &write_info,
    ObSSPreReadEntry *&preread_entry)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &macro_id = file_meta.file_id_;
  ObSSObjectStorageWriter writer;
  ObStorageObjectHandle write_handle;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  const ObPrereadNode preread_node(ObPrereadNodeStatus::FAKE, 0);
  // 先写远端并建立占位节点，再完成一次真实预读；结果留在读队列，供各用例控制 put 时机。
  if (OB_FAIL(write_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set test preread macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(writer.aio_write(write_info, write_handle))) {
    LOG_WARN("fail to write test preread object", KR(ret), K(macro_id));
  } else if (OB_FAIL(write_handle.wait())) {
    LOG_WARN("fail to wait test preread object write", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_cache_mgr.preread_map_.set_refactored(macro_id, preread_node))) {
    LOG_WARN("fail to create test preread node", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_task.free_list_.pop(preread_entry))) {
    LOG_WARN("fail to acquire test preread entry", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_entry->init(file_meta))) {
    LOG_WARN("fail to init test preread entry", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_task.do_async_read_entry(*preread_entry))) {
    LOG_WARN("fail to submit test preread IO", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_entry->read_handle_.wait())) {
    LOG_WARN("fail to wait test preread IO", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_task.async_read_list_.push(preread_entry))) {
    LOG_WARN("fail to enqueue test preread result", KR(ret), K(macro_id));
  }
  return ret;
}

// 测试中不启动预读定时任务，由测试线程取出完成结果，再手动交给 preread 处理。
static int wait_preread_put_result(ObSSPreReadTask &preread_task, ObSSPreReadEntry *&completed_entry)
{
  int ret = OB_SUCCESS;
  const int64_t deadline_us = ObTimeUtility::current_time() + 5 * 1000 * 1000L;
  while (OB_ENTRY_NOT_EXIST == (ret = preread_task.completed_put_entries_.pop(completed_entry))
      && ObTimeUtility::current_time() < deadline_us) {
    ob_usleep(100);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to wait test preread put result", KR(ret),
        "pending_put_count", preread_task.pending_put_count_,
        "completed_count", preread_task.completed_put_entries_.get_curr_total(),
        "read_count", preread_task.async_read_list_.get_curr_total(),
        "free_count", preread_task.free_list_.get_curr_total());
  }
  return ret;
}

// 场景：临时文件批量预读完成后应命中 L1；等待条件同时覆盖异步 put，最后回收全部任务。
TEST_F(TestSSPreReadTask, basic_pre_read)
{
  SCOPED_TRACE("临时文件批量预读完成后应命中 L1；等待条件同时覆盖异步 put，最后回收全部任务。");
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  // 1. 写入远端临时文件，作为后续批量预读的数据源。
  const int64_t tmp_file_cnt = 100;
  MacroBlockId macro_ids[tmp_file_cnt];
  for (int64_t i = 0; i < tmp_file_cnt; ++i) {
    macro_ids[i].set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    macro_ids[i].set_second_id(i); // tmp_file_id
    macro_ids[i].set_fourth_id(1); // segment_id
    ASSERT_TRUE(macro_ids[i].is_valid());
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_ids[i]));
    ObSSObjectStorageWriter object_storage_writer;
    ASSERT_EQ(OB_SUCCESS, object_storage_writer.aio_write(write_info_, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
    // Note: TMP_FILE has no effective_tablet_id
    ASSERT_EQ(OB_SUCCESS, file_manager->push_to_preread_queue(macro_ids[i],
                                        ObTabletID(ObTabletID::INVALID_TABLET_ID)));
  }

  // 2. wait preread_task read tmp file to local cache.
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          KR(preread_cache_mgr.preread_queue_.size()), KR(preread_task.segment_files_.count()),
          KR(preread_task.async_read_list_.get_curr_total()), KR(preread_task.pending_put_count_));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3. 回读并逐字节比较写入内容，验证缓存命中后的数据完整性。
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  ObSSTenantMacroBlockKVCacheMgrStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get_cache_stat(cache_stat));
  const int64_t hit_cnt_before = cache_stat.hit_cnt_;
  const int64_t miss_cnt_before = cache_stat.miss_cnt_;
  for (int64_t i = 0; i < tmp_file_cnt; ++i) {
    read_info_.macro_block_id_ = macro_ids[i];
    ObStorageObjectHandle read_object_handle;
    ObSSTmpFileReader tmp_file_reader;
    ASSERT_EQ(OB_SUCCESS, tmp_file_reader.aio_read(read_info_, read_object_handle));
    ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
    ASSERT_NE(nullptr, read_object_handle.get_buffer());
    ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
    ASSERT_EQ(0, memcmp(write_buf_, read_object_handle.get_buffer(), WRITE_IO_SIZE));

    // Verify that data was read from KVCACHE by checking cache statistics.
    // When reading from KVCACHE, there is no actual IO operation;
    // data is directly copied from memory. The correct way to verify is to check
    // cache hit statistics instead of IO flags.
    ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get_cache_stat(cache_stat));
    ASSERT_EQ(hit_cnt_before + i + 1, cache_stat.hit_cnt_);
    ASSERT_EQ(miss_cnt_before, cache_stat.miss_cnt_);
  }

  // 4. 等待下一段文件的预读及异步 put 完成，再检查资源回收。
  start_us = ObTimeUtility::current_time();
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          KR(preread_cache_mgr.preread_queue_.size()), KR(preread_task.segment_files_.count()),
          KR(preread_task.async_read_list_.get_curr_total()), KR(preread_task.pending_put_count_));
      break;
    }
  }

  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t max_parallel_cnt = file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_;
  ASSERT_EQ(max_parallel_cnt, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(0, preread_cache_mgr.preread_queue_.size());
  ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  // delete all tmp file
  for (int64_t i = 0; i < tmp_file_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(macro_ids[i]));
  }
}

// 场景：没有 effective_tablet_id 的宏块首次读取触发预读，异步 put 完成后进入租户缓存。
TEST_F(TestSSPreReadTask, preread_without_effective_tablet_id)
{
  SCOPED_TRACE("没有 effective_tablet_id 的宏块首次读取触发预读，异步 put 完成后进入租户缓存。");
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);

  // 1. 准备一个宏块并写入远端，避免初始缓存影响预读结果。
  uint64_t tablet_id = 200001;
  uint64_t data_seq = 1;
  uint64_t reorganization_scn = 0;

  // 写入远端并等待完成，确保后续读取时文件已经存在。
  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_META_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_reorganization_scn(reorganization_scn);
  macro_id.set_seq_id(data_seq);
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // 2. 从空缓存读取，触发 cache miss 和预读。
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_effective_tablet_id(ObTabletID(ObTabletID::INVALID_TABLET_ID));
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // 3. 等待远端预读和异步 put 完成，检查宏块进入租户 KVCACHE。
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          K(preread_cache_mgr.preread_queue_.size()), K(preread_task.segment_files_.count()),
          K(preread_task.async_read_list_.get_curr_total()), K(preread_task.pending_put_count_));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  // 4. 删除远端测试文件，避免影响后续用例。
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_file(macro_id));
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

// 场景：不读取缓存的对象存储读取路径不应触发预读，等待后台任务结束后 L1/L2 均不新增该宏块。
TEST_F(TestSSPreReadTask, preread_only_when_need_read_cache)
{
  SCOPED_TRACE("不读取缓存的对象存储读取路径不应触发预读，等待后台任务结束后 L1/L2 均不新增该宏块。");
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // 1. 准备一个宏块并写入远端，避免初始缓存影响预读结果。
  uint64_t tablet_id = 200002;
  uint64_t data_seq = 1;
  uint64_t reorganization_scn = 0;

  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_META_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_reorganization_scn(reorganization_scn);
  macro_id.set_seq_id(data_seq);
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // 2. 使用不读取缓存的 reader，验证该入口不会因读取而触发预读。
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  read_info_.set_effective_tablet_id(ObTabletID(ObTabletID::INVALID_TABLET_ID));
  ObStorageObjectHandle read_object_handle;
  ObSSObjectStorageReader object_storage_reader;
  ASSERT_EQ(OB_SUCCESS, object_storage_reader.aio_read(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // 3. 等待预读任务结束，确认该宏块未进入 L1 或 L2 缓存。
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          K(preread_cache_mgr.preread_queue_.size()), K(preread_task.segment_files_.count()),
          K(preread_task.async_read_list_.get_curr_total()), K(preread_task.pending_put_count_));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);

  // 4. 删除远端测试文件，避免影响后续用例。
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_file(macro_id));
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

// 场景：异步 put 完成与 GC 删除节点交错时，已写入的缓存仍可读取，完成回收不能重复创建节点。
TEST_F(TestSSPreReadTask, preread_and_gc_parallel)
{
  SCOPED_TRACE("异步 put 完成与 GC 删除节点交错时，已写入的缓存仍可读取，完成回收不能重复创建节点。");
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, file_manager);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(file_manager));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ASSERT_EQ(OB_SUCCESS, preread_task.put_worker_.start());
  ASSERT_EQ(OB_SUCCESS, file_manager->calibrate_disk_space_task_.calibrate_disk_space());
  // tmp_file write_cache and read_cache size
  ObSSMacroCacheStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, cache_stat));
  const int64_t initial_cache_used = cache_stat.used_;
  ASSERT_EQ(0, initial_cache_used);
  // construct macro_id
  MacroBlockId file_id;
  const int64_t tmp_file_id = 100;
  const int64_t segment_id = 101;
  file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  file_id.set_second_id(tmp_file_id); // tmp_file_id
  file_id.set_fourth_id(segment_id); // segment_id
  ObStorageObjectHandle write_object_handle;
  // write file to object storage
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  ObSSObjectStorageWriter object_storage_writer;
  ASSERT_EQ(OB_SUCCESS, object_storage_writer.aio_write(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  // read file from object storage
  ObPreReadFileMeta file_meta(file_id, ObTabletID(ObTabletID::INVALID_TABLET_ID), 0);
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, preread_task.free_list_.pop(entry));
  ObSSPreReadEntry &preread_entry = *entry;
  ASSERT_EQ(OB_SUCCESS, preread_entry.init(file_meta));
  ASSERT_EQ(OB_SUCCESS, preread_task.do_async_read_entry(preread_entry));
  ASSERT_EQ(OB_SUCCESS, preread_entry.read_handle_.wait());
  // test1: preread_write,read_whole,GC,update_to_normal
  // push to preread map
  ObPrereadNode preread_node = ObPrereadNode(ObPrereadNodeStatus::FAKE, 0/*file_length*/);
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.set_refactored(file_id, preread_node));
  // create dir and get dir size
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  int64_t expected_disk_size = initial_cache_used;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  // write
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  ObSSTenantMacroBlockKVCacheMgrStat cache_stat_before;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get_cache_stat(cache_stat_before));
  ASSERT_EQ(OB_SUCCESS, preread_task.try_submit_kvcache_put_(preread_entry));
  ObSSPreReadEntry *completed_entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, wait_preread_put_result(preread_task, completed_entry));
  ASSERT_EQ(entry, completed_entry);
  ASSERT_EQ(OB_SUCCESS, completed_entry->kvcache_put_ret_);
  // put 已发布缓存，但 preread 尚未收尾，原始 buffer 和节点仍必须保留。
  ASSERT_NE(nullptr, completed_entry->buf_);
  ASSERT_EQ(1, preread_task.pending_put_count_);
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(file_id, preread_node));
  ASSERT_EQ(ObPrereadNodeStatus::FAKE, preread_node.node_status_);
  ObSSTenantMacroBlockKVCacheMgrStat cache_stat_after;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get_cache_stat(cache_stat_after));
  ASSERT_EQ(cache_stat_before.valid_macro_cnt_ + 1, cache_stat_after.valid_macro_cnt_);
  ASSERT_GT(cache_stat_after.memory_size_, cache_stat_before.memory_size_);

  // memory_size_ 包含 Key、Value 和节点元数据，单纯变大不能证明 Value 完整。
  // 从 KVCACHE 读回并逐字节比较，保留原测试对精确数据长度的验证强度。
  const int64_t cached_data_size = preread_entry.read_handle_.get_data_size();
  ASSERT_EQ(read_info_.size_, cached_data_size);
  ObIOInfo cache_io_info;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      cache_io_info, file_id, 0,
      static_cast<int32_t>(cached_data_size), read_buf_));
  ObStorageObjectHandle cache_object_handle;
  bool is_hit_cache = false;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get(
      file_id, cache_io_info, cache_object_handle, is_hit_cache));
  ASSERT_TRUE(is_hit_cache);
  ASSERT_EQ(cached_data_size, cache_object_handle.get_data_size());
  ASSERT_EQ(0, MEMCMP(preread_entry.read_handle_.get_buffer(),
      cache_object_handle.get_buffer(), cached_data_size));
  // 模拟 GC 与完成通知交错：节点消失也应完成回收，不重复插入节点。
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.remove_preread_node(file_id));
  ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entry));
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entry->buf_);
  ASSERT_EQ(OB_HASH_NOT_EXIST, preread_cache_mgr.preread_map_.get_refactored(file_id, preread_node));
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_tmp_file(file_id));
}

// 场景：小于原准入大小的 COLD 元数据仍写入 L1，entry 回收后缓存内容完整可读。
TEST_F(TestSSPreReadTask, async_put_caches_small_cold_metadata)
{
  SCOPED_TRACE("小于原准入大小的 COLD 元数据仍写入 L1，entry 回收后缓存内容完整可读。");
  const uint64_t tablet_id = 200101;
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ASSERT_EQ(OB_SUCCESS, policy_service->get_tablet_status_map().set_refactored(tablet_id, PolicyStatus::COLD));
  DEFER(policy_service->get_tablet_status_map().erase_refactored(tablet_id));
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ASSERT_EQ(OB_SUCCESS, preread_task.put_worker_.start());

  // 小于原准入大小的冷元数据块也应进入 L1，避免丢弃已读回的数据。
  const int64_t data_size = 4096;
  ObSSLocalCacheService *cache_service = MTL(ObSSLocalCacheService *);
  ASSERT_NE(nullptr, cache_service);
  ASSERT_LT(data_size, cache_service->get_macro_cache_min_admit_block_size());
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_META_MACRO));
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0);
  macro_id.set_seq_id(1);
  const ObPreReadFileMeta file_meta(macro_id, ObTabletID(tablet_id), 0);
  ObStorageObjectWriteInfo write_info = write_info_;
  write_info.size_ = data_size;
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, file_meta, write_info, entry));
  ASSERT_EQ(data_size, entry->read_handle_.get_data_size());
  ASSERT_EQ(OB_SUCCESS, preread_task.write_preread_entries());
  ASSERT_EQ(1, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
  ObSSPreReadEntry *completed_entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, wait_preread_put_result(preread_task, completed_entry));
  ASSERT_EQ(entry, completed_entry);
  ASSERT_EQ(OB_SUCCESS, completed_entry->kvcache_put_ret_);
  ASSERT_NE(nullptr, entry->buf_);
  ObPrereadNode preread_node;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(macro_id, preread_node));
  ASSERT_EQ(ObPrereadNodeStatus::FAKE, preread_node.node_status_);
  ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entry));
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entry->buf_);
  // 非临时文件完成预读后不保留节点，但必须记录已完成文件。
  ASSERT_EQ(OB_HASH_NOT_EXIST, preread_cache_mgr.preread_map_.get_refactored(macro_id, preread_node));
  ASSERT_EQ(1, preread_task.preread_files_.count());
  EXPECT_EQ(macro_id, preread_task.preread_files_.at(0).file_id_);

  // 原始 buffer 回收后，从 L1 读回并校验完整内容。
  ObIOInfo cache_io_info;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      cache_io_info, macro_id, 0, static_cast<int32_t>(data_size), read_buf_));
  ObStorageObjectHandle cache_handle;
  bool is_hit_cache = false;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSTenantMacroBlockKVCacheMgr *)->get(
      macro_id, cache_io_info, cache_handle, is_hit_cache));
  ASSERT_TRUE(is_hit_cache);
  ASSERT_EQ(data_size, cache_handle.get_data_size());
  ASSERT_EQ(0, MEMCMP(write_info.buffer_, cache_handle.get_buffer(), data_size));
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_file(macro_id));
}

// 场景：完整 2MiB 宏块异步写入 L1 后，释放预读 buffer 不影响缓存内容和后续读取。
TEST_F(TestSSPreReadTask, async_put_full_macro_remains_readable_after_entry_recycle)
{
  SCOPED_TRACE("完整 2MiB 宏块异步写入 L1 后，释放预读 buffer 不影响缓存内容和后续读取。");
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TestPreRead"));
  const int64_t data_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
  char *write_buffer = static_cast<char *>(allocator.alloc(data_size));
  char *read_buffer = static_cast<char *>(allocator.alloc(data_size));
  ASSERT_NE(nullptr, write_buffer);
  ASSERT_NE(nullptr, read_buffer);
  // 使用随位置变化的数据，完整比较 2MiB，避免只验证了前缀或重复的填充值。
  for (int64_t i = 0; i < data_size; ++i) {
    write_buffer[i] = static_cast<char>((i + i / 4096) % 251);
  }
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ASSERT_EQ(OB_SUCCESS, preread_task.put_worker_.start());
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id.set_second_id(9200);
  macro_id.set_fourth_id(1);
  const ObPreReadFileMeta file_meta(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID), 0);
  ObStorageObjectWriteInfo write_info = write_info_;
  write_info.buffer_ = write_buffer;
  write_info.size_ = data_size;
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, file_meta, write_info, entry));
  ASSERT_EQ(data_size, entry->read_handle_.get_data_size());
  ASSERT_EQ(OB_SUCCESS, preread_task.write_preread_entries());
  ObSSPreReadEntry *completed_entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, wait_preread_put_result(preread_task, completed_entry));
  ASSERT_EQ(entry, completed_entry);
  ASSERT_EQ(OB_SUCCESS, completed_entry->kvcache_put_ret_);
  ASSERT_NE(nullptr, entry->buf_);
  ASSERT_EQ(0, MEMCMP(write_buffer, entry->read_handle_.get_buffer(), data_size));

  ObSSTenantMacroBlockKVCacheMgr *cache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, cache_mgr);
  ObIOInfo cache_io_info;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      cache_io_info, macro_id, 0, static_cast<int32_t>(data_size), read_buffer));
  ObStorageObjectHandle cache_handle;
  bool is_hit_cache = false;
  ASSERT_EQ(OB_SUCCESS, cache_mgr->get(macro_id, cache_io_info, cache_handle, is_hit_cache));
  ASSERT_TRUE(is_hit_cache);
  ASSERT_EQ(data_size, cache_handle.get_data_size());
  ASSERT_EQ(0, MEMCMP(write_buffer, cache_handle.get_buffer(), data_size));
  cache_handle.reset();

  ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entry));
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entry->buf_);
  ASSERT_FALSE(entry->read_handle_.is_valid());
  // 释放原始 buffer 后重新获取缓存，确保缓存数据不依赖 preread entry 的生命周期。
  is_hit_cache = false;
  ASSERT_EQ(OB_SUCCESS, cache_mgr->get(macro_id, cache_io_info, cache_handle, is_hit_cache));
  ASSERT_TRUE(is_hit_cache);
  ASSERT_EQ(data_size, cache_handle.get_data_size());
  ASSERT_EQ(0, MEMCMP(write_buffer, cache_handle.get_buffer(), data_size));
  cache_handle.reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(macro_id));
}

// 场景：同轮混合处理失败、EAGAIN 和成功结果，分别清理、重试、完成，并允许后续新任务继续提交。
TEST_F(TestSSPreReadTask, async_put_mixed_results_do_not_block_following_preread)
{
  SCOPED_TRACE("同轮混合处理失败、EAGAIN 和成功结果，分别清理、重试、完成，并允许后续新任务继续提交。");
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ASSERT_EQ(OB_SUCCESS, preread_task.put_worker_.start());
  ObSSPreReadEntry *entries[3] = {nullptr, nullptr, nullptr};
  MacroBlockId macro_ids[3];
  const int put_results[] = {OB_IO_ERROR, OB_EAGAIN, OB_SUCCESS};
  for (int64_t i = 0; i < ARRAYSIZEOF(entries); ++i) {
    SCOPED_TRACE(::testing::Message() << "i=" << i);
    ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9300 + i, entries[i]));
    macro_ids[i] = entries[i]->file_meta_.file_id_;
    ObSSPreReadEntry *read_entry = nullptr;
    ASSERT_EQ(OB_SUCCESS, preread_task.async_read_list_.pop(read_entry));
    ASSERT_EQ(entries[i], read_entry);
    // 只注入完成结果，单独验证同一轮收尾中的失败、重试和成功分支。
    ++preread_task.pending_put_count_;
    preread_task.put_worker_.enqueue_completed_entry_(*read_entry, put_results[i]);
  }
  const char *retry_buffer = entries[1]->buf_;
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
  ASSERT_EQ(1, preread_task.async_read_list_.get_curr_total());
  ASSERT_EQ(preread_task.max_pre_read_parallelism_ - 1, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entries[0]->buf_);
  ASSERT_EQ(retry_buffer, entries[1]->buf_);
  ASSERT_EQ(nullptr, entries[2]->buf_);
  ObPrereadNode preread_node;
  ASSERT_EQ(OB_HASH_NOT_EXIST, preread_cache_mgr.preread_map_.get_refactored(macro_ids[0], preread_node));
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(macro_ids[1], preread_node));
  ASSERT_EQ(ObPrereadNodeStatus::FAKE, preread_node.node_status_);
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(macro_ids[2], preread_node));
  ASSERT_EQ(ObPrereadNodeStatus::NORMAL, preread_node.node_status_);
  ASSERT_EQ(static_cast<int64_t>(WRITE_IO_SIZE), preread_node.file_length_);

  // 完成队列已经为空，下一轮仍应继续提交重试 entry 和新读完的 entry。
  ObSSPreReadEntry *new_entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9303, new_entry));
  const MacroBlockId new_macro_id = new_entry->file_meta_.file_id_;
  ASSERT_EQ(OB_SUCCESS, preread_task.preread_files());
  ASSERT_EQ(2, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
  ObSSPreReadEntry *completed_entries[2] = {nullptr, nullptr};
  for (int64_t i = 0; i < ARRAYSIZEOF(completed_entries); ++i) {
    SCOPED_TRACE(::testing::Message() << "i=" << i);
    ASSERT_EQ(OB_SUCCESS, wait_preread_put_result(preread_task, completed_entries[i]));
    ASSERT_EQ(OB_SUCCESS, completed_entries[i]->kvcache_put_ret_);
    ASSERT_TRUE(completed_entries[i] == entries[1] || completed_entries[i] == new_entry);
  }
  ASSERT_NE(completed_entries[0], completed_entries[1]);
  for (int64_t i = 0; i < ARRAYSIZEOF(completed_entries); ++i) {
    SCOPED_TRACE(::testing::Message() << "i=" << i);
    ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entries[i]));
  }
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  for (int64_t i = 0; i < ARRAYSIZEOF(macro_ids); ++i) {
    SCOPED_TRACE(::testing::Message() << "i=" << i);
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(macro_ids[i]));
  }
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(new_macro_id));
}

// 场景：一次 EAGAIN 后保留 buffer 和 FAKE 节点；重试成功后节点转 NORMAL，entry 归还池。
TEST_F(TestSSPreReadTask, async_put_retry_preserves_buffer_and_node)
{
  SCOPED_TRACE("一次 EAGAIN 后保留 buffer 和 FAKE 节点；重试成功后节点转 NORMAL，entry 归还池。");
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ASSERT_EQ(OB_SUCCESS, preread_task.put_worker_.start());
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9001, entry));
  const MacroBlockId macro_id = entry->file_meta_.file_id_;
  const char *original_buffer = entry->buf_;

  // 注入一次后台可重试结果，验证跨线程交回后没有提前删除节点或释放原始数据。
  ASSERT_EQ(OB_SUCCESS, preread_task.async_read_list_.pop(entry));
  ++preread_task.pending_put_count_;
  preread_task.put_worker_.enqueue_completed_entry_(*entry, OB_EAGAIN);
  preread_task.adjust_pre_read_task_interval();
  ASSERT_EQ(100L, preread_task.interval_us_);
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(1, preread_task.async_read_list_.get_curr_total());
  ASSERT_EQ(original_buffer, entry->buf_);
  ObPrereadNode preread_node;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(macro_id, preread_node));
  ASSERT_EQ(ObPrereadNodeStatus::FAKE, preread_node.node_status_);

  ASSERT_EQ(OB_SUCCESS, preread_task.write_preread_entries());
  ASSERT_EQ(1, preread_task.pending_put_count_);
  // 已提交给 worker 的 entry 也计入池总数，容量检查不能将它误判为丢失。
  ASSERT_EQ(OB_SUCCESS, preread_task.batch_get_preread_entry_ids());
  ObSSPreReadEntry *completed_entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, wait_preread_put_result(preread_task, completed_entry));
  ASSERT_EQ(entry, completed_entry);
  ASSERT_EQ(OB_SUCCESS, completed_entry->kvcache_put_ret_);
  ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entry));
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entry->buf_);
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(macro_id, preread_node));
  ASSERT_EQ(ObPrereadNodeStatus::NORMAL, preread_node.node_status_);
  ASSERT_EQ(static_cast<int64_t>(WRITE_IO_SIZE), preread_node.file_length_);
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(macro_id));
}

// 使用真实 put 线程组，只为指定宏块的前几次 put 注入 EAGAIN；其他宏块和后续 put 正常执行。
// 析构前汇合线程并回收完成结果，避免线程组继续引用已经销毁的测试回调。
class ScopedPreReadPutEagainHandler final : public lib::TGTaskHandler
{
public:
  ScopedPreReadPutEagainHandler(ObSSPreReadTask &preread_task,
      const MacroBlockId &retry_macro_id, const int64_t eagain_count)
      : preread_task_(preread_task), retry_macro_id_(retry_macro_id), remaining_eagain_count_(eagain_count)
  {}

  ~ScopedPreReadPutEagainHandler()
  {
    preread_task_.stop();
    preread_task_.wait();
  }

  int start()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(TG_SET_HANDLER_AND_START(preread_task_.put_worker_.tg_id_, *this))) {
      LOG_WARN("fail to start test preread put handler", KR(ret));
    } else {
      ATOMIC_STORE(&preread_task_.put_worker_.is_stopped_, false);
    }
    return ret;
  }

  void handle(void *task) override
  {
    ObSSPreReadEntry *entry = static_cast<ObSSPreReadEntry *>(task);
    if (OB_NOT_NULL(entry) && !preread_task_.put_worker_.is_stopped()
        && entry->file_meta_.file_id_ == retry_macro_id_ && remaining_eagain_count_ > 0) {
      --remaining_eagain_count_;
      preread_task_.put_worker_.enqueue_completed_entry_(*entry, OB_EAGAIN);
    } else {
      preread_task_.put_worker_.handle(task);
    }
  }

  void handle_drop(void *task) override
  {
    preread_task_.put_worker_.handle_drop(task);
  }

private:
  ObSSPreReadTask &preread_task_;
  const MacroBlockId retry_macro_id_;
  int64_t remaining_eagain_count_; // 仅由 worker 线程修改。
  DISALLOW_COPY_AND_ASSIGN(ScopedPreReadPutEagainHandler);
};

void TestSSPreReadTask::check_repeated_put_eagain(const bool stop_after_retries)
{
  SCOPED_TRACE(::testing::Message() << "stop_after_retries=" << stop_after_retries);
  LOG_INFO("start repeated preread put retry scenario", K(stop_after_retries));
  const int64_t EAGAIN_ROUND_COUNT = 8;
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ObSSPreReadEntry *retry_entry = nullptr;
  ObSSPreReadEntry *following_entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9600, retry_entry));
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9601, following_entry));
  const MacroBlockId retry_macro_id = retry_entry->file_meta_.file_id_;
  const MacroBlockId following_macro_id = following_entry->file_meta_.file_id_;
  const char *original_buffer = retry_entry->buf_;
  ScopedPreReadPutEagainHandler put_handler(preread_task, retry_macro_id, EAGAIN_ROUND_COUNT);
  ASSERT_EQ(OB_SUCCESS, put_handler.start());

  for (int64_t round = 0; round < EAGAIN_ROUND_COUNT; ++round) {
    SCOPED_TRACE(::testing::Message() << "retry_round=" << round << ", stop_after_retries=" << stop_after_retries);
    const int64_t submitted_count = 0 == round ? 2 : 1;
    ASSERT_EQ(OB_SUCCESS, preread_task.write_preread_entries());
    ASSERT_EQ(submitted_count, preread_task.pending_put_count_);
    ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
    ASSERT_EQ(OB_SUCCESS, preread_task.batch_get_preread_entry_ids());
    ObSSPreReadEntry *completed_entries[2] = {nullptr, nullptr};
    for (int64_t i = 0; i < submitted_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, wait_preread_put_result(preread_task, completed_entries[i]));
      ASSERT_TRUE(completed_entries[i] == retry_entry || (0 == round && completed_entries[i] == following_entry));
      const int expected_ret = completed_entries[i] == retry_entry ? OB_EAGAIN : OB_SUCCESS;
      ASSERT_EQ(expected_ret, completed_entries[i]->kvcache_put_ret_);
    }
    if (2 == submitted_count) {
      ASSERT_NE(completed_entries[0], completed_entries[1]);
    }
    // 先全部取回再放回，避免把测试自身重新入队的结果当成下一个 worker 结果。
    for (int64_t i = 0; i < submitted_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entries[i]));
    }
    ASSERT_EQ(original_buffer, retry_entry->buf_);
    ASSERT_TRUE(retry_entry->is_read_handle_valid());
    ASSERT_EQ(0, MEMCMP(write_buf_, retry_entry->buf_, WRITE_IO_SIZE));

    if (stop_after_retries && round == EAGAIN_ROUND_COUNT - 1) {
      // 最后一轮仍是 EAGAIN，留在完成队列中；停止过程必须独立清理，不依赖下一轮预读。
      preread_cache_mgr.stop();
      preread_cache_mgr.wait();
    } else {
      ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
      ASSERT_EQ(1, preread_task.async_read_list_.get_curr_total());
      ASSERT_EQ(preread_task.max_pre_read_parallelism_ - 1, preread_task.free_list_.get_curr_total());
      ObPrereadNode retry_node;
      ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(retry_macro_id, retry_node));
      ASSERT_EQ(ObPrereadNodeStatus::FAKE, retry_node.node_status_);
      ASSERT_EQ(original_buffer, retry_entry->buf_);
    }
    ASSERT_EQ(0, preread_task.pending_put_count_);
    ASSERT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
    LOG_INFO("verify preread retry round cleanup", K(round), K(stop_after_retries),
        "pending_put_count", preread_task.pending_put_count_,
        "read_count", preread_task.async_read_list_.get_curr_total(),
        "free_count", preread_task.free_list_.get_curr_total());
    // 同轮的另一个宏块已经完成并释放 entry，不能被持续失败的宏块阻塞。
    ASSERT_EQ(nullptr, following_entry->buf_);
    ObPrereadNode following_node;
    ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(following_macro_id, following_node));
    ASSERT_EQ(ObPrereadNodeStatus::NORMAL, following_node.node_status_);
  }

  if (!stop_after_retries) {
    // 注入次数耗尽后由真实 worker 执行 put，原 buffer 仍可正常发布到缓存。
    ASSERT_EQ(OB_SUCCESS, preread_task.write_preread_entries());
    ObSSPreReadEntry *completed_entry = nullptr;
    ASSERT_EQ(OB_SUCCESS, wait_preread_put_result(preread_task, completed_entry));
    ASSERT_EQ(retry_entry, completed_entry);
    ASSERT_EQ(OB_SUCCESS, completed_entry->kvcache_put_ret_);
    ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entry));
    ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  }
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
  ASSERT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, retry_entry->buf_);
  ASSERT_FALSE(retry_entry->is_read_handle_valid());
  ObPrereadNode retry_node;
  if (stop_after_retries) {
    ASSERT_EQ(OB_HASH_NOT_EXIST, preread_cache_mgr.preread_map_.get_refactored(retry_macro_id, retry_node));
    bool is_cached = true;
    ASSERT_EQ(OB_SUCCESS, MTL(ObSSTenantMacroBlockKVCacheMgr *)->check_exist(retry_macro_id, is_cached));
    ASSERT_FALSE(is_cached);
  } else {
    ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(retry_macro_id, retry_node));
    ASSERT_EQ(ObPrereadNodeStatus::NORMAL, retry_node.node_status_);
    ObIOInfo cache_io_info;
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
        cache_io_info, retry_macro_id, 0, static_cast<int32_t>(WRITE_IO_SIZE), read_buf_));
    ObStorageObjectHandle cache_handle;
    bool is_hit_cache = false;
    ASSERT_EQ(OB_SUCCESS, MTL(ObSSTenantMacroBlockKVCacheMgr *)->get(
        retry_macro_id, cache_io_info, cache_handle, is_hit_cache));
    ASSERT_TRUE(is_hit_cache);
    ASSERT_EQ(static_cast<int64_t>(WRITE_IO_SIZE), cache_handle.get_data_size());
    ASSERT_EQ(0, MEMCMP(write_buf_, cache_handle.get_buffer(), WRITE_IO_SIZE));
  }
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(retry_macro_id));
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(following_macro_id));
}

// 场景：指定宏块连续八次 EAGAIN 后成功，其他宏块不被阻塞，重试期间数据和节点保持有效。
TEST_F(TestSSPreReadTask, repeated_put_eagain_eventually_succeeds)
{
  SCOPED_TRACE("指定宏块连续八次 EAGAIN 后成功，其他宏块不被阻塞，重试期间数据和节点保持有效。");
  // 共享辅助函数注入八轮 EAGAIN；本分支继续真实 put 并校验完整回读。
  check_repeated_put_eagain(false/*stop_after_retries*/);
}

// 场景：连续八次 EAGAIN 后停止，不能依赖下一轮定时任务，应独立清理节点并归还 entry。
TEST_F(TestSSPreReadTask, repeated_put_eagain_stop_reclaims_entry)
{
  SCOPED_TRACE("连续八次 EAGAIN 后停止，不能依赖下一轮定时任务，应独立清理节点并归还 entry。");
  // 共享辅助函数在第八轮完成队列保留 EAGAIN，然后 stop/wait 并检查池、节点和缓存。
  check_repeated_put_eagain(true/*stop_after_retries*/);
}

// 场景：提交前读 handle 无效时仅回收 entry，保留 FAKE 节点，与原同步路径的清理契约一致。
TEST_F(TestSSPreReadTask, invalid_read_handle_preserves_preread_node)
{
  SCOPED_TRACE("提交前读 handle 无效时仅回收 entry，保留 FAKE 节点，与原同步路径的清理契约一致。");
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id.set_second_id(9500);
  macro_id.set_fourth_id(1);
  const ObPreReadFileMeta file_meta(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID), 0);
  ObPrereadNode preread_node(ObPrereadNodeStatus::FAKE, 0);
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.set_refactored(macro_id, preread_node));
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, preread_task.free_list_.pop(entry));
  ASSERT_EQ(OB_SUCCESS, entry->init(file_meta));
  // 不发起 IO，让有效 entry 携带无效读 handle，确定性地进入提交前的检查失败分支。
  ASSERT_TRUE(entry->is_valid());
  ASSERT_FALSE(entry->is_read_handle_valid());
  ASSERT_EQ(OB_SUCCESS, preread_task.async_read_list_.push(entry));
  ASSERT_EQ(OB_SUCCESS, preread_task.write_preread_entries());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
  ASSERT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entry->buf_);
  // 与同步版本一致：回收 entry 后仍保留占位节点，不因这次失败重新开放预读。
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(macro_id, preread_node));
  ASSERT_EQ(ObPrereadNodeStatus::FAKE, preread_node.node_status_);
}

// 场景：worker 停止后拒绝提交，未被接收的 entry 被回收，预读节点仍保留。
TEST_F(TestSSPreReadTask, stopped_put_submission_preserves_preread_node)
{
  SCOPED_TRACE("worker 停止后拒绝提交，未被接收的 entry 被回收，预读节点仍保留。");
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9501, entry));
  const MacroBlockId macro_id = entry->file_meta_.file_id_;
  // 停止后 submit 直接拒绝；只回收未被 worker 接收的 entry，保留预读节点。
  preread_task.put_worker_.stop();
  ASSERT_EQ(OB_SUCCESS, preread_task.write_preread_entries());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
  ASSERT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entry->buf_);
  ASSERT_FALSE(entry->is_read_handle_valid());
  ObPrereadNode preread_node;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.preread_map_.get_refactored(macro_id, preread_node));
  ASSERT_EQ(ObPrereadNodeStatus::FAKE, preread_node.node_status_);
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(macro_id));
}

// 场景：禁用 KVCACHE 使后台 put 返回停止错误，完成回收后不残留 entry、预读节点或缓存。
TEST_F(TestSSPreReadTask, async_put_failure_reclaims_entry)
{
  SCOPED_TRACE("禁用 KVCACHE 使后台 put 返回停止错误，完成回收后不残留 entry、预读节点或缓存。");
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ASSERT_EQ(OB_SUCCESS, preread_task.put_worker_.start());
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9002, entry));
  const MacroBlockId macro_id = entry->file_meta_.file_id_;
  ObSSTenantMacroBlockKVCacheMgr *cache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *);
  // 仅在提交和等待 put 结果期间禁用缓存，断言前先恢复，避免错误路径污染环境。
  cache_mgr->disable_cache();
  const int submit_ret = preread_task.write_preread_entries();
  ObSSPreReadEntry *completed_entry = nullptr;
  const int wait_ret = wait_preread_put_result(preread_task, completed_entry);
  cache_mgr->enable_cache();
  ASSERT_EQ(OB_SUCCESS, submit_ret);
  ASSERT_EQ(OB_SUCCESS, wait_ret);
  ASSERT_EQ(OB_SERVICE_STOPPED, completed_entry->kvcache_put_ret_);
  ASSERT_NE(nullptr, completed_entry->buf_);
  ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entry));
  // 把结果交回预读主流程，确认 buffer、节点和 entry 都被清理，L1 未产生缓存。
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entry->buf_);
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.is_exist_in_preread_map(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, cache_mgr->check_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(macro_id));
}

// 仅代理局部 manager 的 put 线程组，在 submit 已通过停止检查、实际入队前暂停。
// 其他操作仍交给真实线程组；退出作用域时先结束定时任务和 worker，再恢复线程组指针。
class ScopedPreReadPutSubmitGate final : public lib::ITG
{
public:
  explicit ScopedPreReadPutSubmitGate(ObPrereadCacheManager &preread_cache_mgr)
    : preread_cache_mgr_(preread_cache_mgr),
      put_tg_id_(preread_cache_mgr.preread_task_.put_worker_.tg_id_),
      original_tg_(*TG_MGR.tgs_[put_tg_id_]), condition_(),
      submission_entered_(false), submission_released_(false), submit_ret_(OB_NOT_INIT)
  {}

  virtual ~ScopedPreReadPutSubmitGate() override
  {
    // 即使用例中途断言失败，也先放行入队，避免 manager 销毁时等待定时任务而卡住。
    if (condition_.is_inited()) {
      release_submission();
      preread_cache_mgr_.stop();
      preread_cache_mgr_.wait();
      TG_MGR.tgs_[put_tg_id_] = &original_tg_;
    }
  }

  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(condition_.init(ObWaitEventIds::COND_WAIT_FOR_BASE_CLASS))) {
      LOG_WARN("fail to init preread put submit gate", KR(ret));
    } else {
      // 条件变量就绪后才安装代理，初始化失败时不影响真实线程组。
      attr_ = original_tg_.attr_;
      tg_helper_ = original_tg_.tg_helper_;
      TG_MGR.tgs_[put_tg_id_] = this;
    }
    return ret;
  }

  virtual int thread_cnt() override { return original_tg_.thread_cnt(); }
  virtual int set_thread_cnt(int64_t thread_count) override { return original_tg_.set_thread_cnt(thread_count); }
  virtual int set_handler(lib::TGTaskHandler &handler) override { return original_tg_.set_handler(handler); }
  virtual int start() override { return original_tg_.start(); }
  virtual void stop() override
  {
    EXPECT_TRUE(preread_cache_mgr_.preread_task_.put_worker_.submit_lock_.self_locked());
    original_tg_.stop();
  }
  virtual void wait() override { original_tg_.wait(); }

  virtual int push_task(void *task) override
  {
    int ret = OB_SUCCESS;
    EXPECT_TRUE(preread_cache_mgr_.preread_task_.put_worker_.submit_lock_.self_locked());
    {
      ObThreadCondGuard guard(condition_);
      submission_entered_ = true;
      if (OB_FAIL(condition_.broadcast())) {
        LOG_WARN("fail to notify preread put submission", KR(ret));
      } else if (OB_FAIL(wait_for_flag_(submission_released_))) {
        LOG_WARN("fail to wait for preread put submission release", KR(ret));
      }
    }
    // 解锁后再转交真实线程组；超时则终止本次提交，由调用方回收 entry。
    if (OB_SUCC(ret)) {
      ret = original_tg_.push_task(task);
    }
    submit_ret_ = ret;
    return ret;
  }

  int wait_for_submission()
  {
    int ret = OB_SUCCESS;
    ObThreadCondGuard guard(condition_);
    if (OB_FAIL(wait_for_flag_(submission_entered_))) {
      LOG_WARN("fail to wait for preread put submission", KR(ret));
    }
    return ret;
  }

  void release_submission()
  {
    int ret = OB_SUCCESS;
    ObThreadCondGuard guard(condition_);
    submission_released_ = true;
    if (OB_FAIL(condition_.broadcast())) {
      LOG_WARN("fail to release preread put submission", KR(ret));
    }
  }

  // manager.wait() 已保证提交方退出，此后才能读取入队结果。
  int get_submit_ret() const { return submit_ret_; }

private:
  int wait_for_flag_(const bool &flag)
  {
    int ret = OB_SUCCESS;
    const int64_t deadline_us = ObTimeUtility::current_monotonic_time() + 5 * common::S_US;
    // 调用方持有 condition_ 的锁；唤醒后重新检查条件，虚假唤醒不延长总等待时间。
    while (OB_SUCC(ret) && !flag) {
      const int64_t remaining_us = deadline_us - ObTimeUtility::current_monotonic_time();
      if (OB_UNLIKELY(remaining_us <= 0)) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(condition_.wait_us(remaining_us))) {
        // 超时返回前已重新持锁；条件可能在此期间变为真，按条件实际状态判断。
        if (OB_TIMEOUT == ret && flag) {
          ret = OB_SUCCESS;
        }
      }
    }
    return ret;
  }

private:
  ObPrereadCacheManager &preread_cache_mgr_;
  const int put_tg_id_;
  lib::ITG &original_tg_;
  ObThreadCond condition_;
  bool submission_entered_;
  bool submission_released_;
  int submit_ret_;
  DISALLOW_COPY_AND_ASSIGN(ScopedPreReadPutSubmitGate);
};

// 场景：在提交通过停止检查但尚未入队时并发 stop，验证提交与停止串行化且 entry 不丢失。
TEST_F(TestSSPreReadTask, async_put_stop_during_submission_returns_entry)
{
  SCOPED_TRACE("在提交通过停止检查但尚未入队时并发 stop，验证提交与停止串行化且 entry 不丢失。");
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9400, entry));
  const MacroBlockId macro_id = entry->file_meta_.file_id_;
  ASSERT_NE(nullptr, TG_MGR.tgs_[preread_task.put_worker_.tg_id_]);
  ScopedPreReadPutSubmitGate submit_gate(preread_cache_mgr);
  ASSERT_EQ(OB_SUCCESS, submit_gate.init());
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.start());
  ASSERT_EQ(OB_SUCCESS, submit_gate.wait_for_submission());

  // 定时任务持有提交锁，已通过停止检查但尚未入队。
  // 另一线程发起停止，必须等本次入队结束才能执行 TG_STOP，不能阻断 entry 的交回。
  std::promise<void> stop_started;
  std::thread stop_thread([&]() {
    stop_started.set_value();
    preread_cache_mgr.stop();
  });
  stop_started.get_future().wait();
  submit_gate.release_submission();
  stop_thread.join();
  EXPECT_TRUE(preread_task.put_worker_.is_stopped());
  preread_cache_mgr.wait();

  ASSERT_EQ(OB_SUCCESS, submit_gate.get_submit_ret());
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.async_read_list_.get_curr_total());
  ASSERT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(nullptr, entry->buf_);
  ASSERT_FALSE(entry->read_handle_.is_valid());
  ASSERT_EQ(0, preread_cache_mgr.get_preread_map_size());
  // 本次提交先完成入队，put 可能在停止前执行；停止后再次提交必须被拒绝。
  ASSERT_EQ(OB_SERVICE_STOPPED, preread_task.put_worker_.submit(*entry));
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(macro_id));
}

// 场景：任务排队期间 tablet 转为 COLD，worker 仍写入 L1，避免丢弃已读回的数据。
TEST_F(TestSSPreReadTask, async_put_caches_data_after_tablet_becomes_cold)
{
  SCOPED_TRACE("任务排队期间 tablet 转为 COLD，worker 仍写入 L1，避免丢弃已读回的数据。");
  const uint64_t tablet_id = 200102;
  const int64_t data_size = 4096;
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ASSERT_LT(data_size, MTL(ObSSLocalCacheService *)->get_macro_cache_min_admit_block_size());
  ASSERT_EQ(OB_SUCCESS, policy_service->get_tablet_status_map().set_refactored(tablet_id, PolicyStatus::HOT));
  DEFER(policy_service->get_tablet_status_map().erase_refactored(tablet_id));

  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  MacroBlockId macro_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_META_MACRO));
  macro_id.set_second_id(tablet_id);
  macro_id.set_reorganization_scn(0);
  macro_id.set_seq_id(1);
  const ObPreReadFileMeta file_meta(macro_id, ObTabletID(tablet_id), 0);
  ObStorageObjectWriteInfo write_info = write_info_;
  write_info.size_ = data_size;
  ObSSPreReadEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, file_meta, write_info, entry));
  ScopedPreReadPutSubmitGate submit_gate(preread_cache_mgr);
  ASSERT_EQ(OB_SUCCESS, submit_gate.init());
  ASSERT_EQ(OB_SUCCESS, preread_task.put_worker_.start());

  // 暂停提交，在 worker 执行前从 HOT 改为 COLD，已读数据仍应进入 L1。
  ObTenantBase *tenant = ObTenantEnv::get_tenant();
  auto submission = std::async(std::launch::async, [&]() {
    ObTenantSwitchGuard tenant_guard(tenant);
    return preread_task.write_preread_entries();
  });
  EXPECT_EQ(OB_SUCCESS, submit_gate.wait_for_submission());
  EXPECT_EQ(OB_SUCCESS, policy_service->get_tablet_status_map().set_refactored(
      tablet_id, PolicyStatus::COLD, 1/*overwrite*/));
  submit_gate.release_submission();
  ASSERT_EQ(OB_SUCCESS, submission.get());
  ASSERT_EQ(1, preread_task.pending_put_count_);
  ObSSPreReadEntry *completed_entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, wait_preread_put_result(preread_task, completed_entry));
  ASSERT_EQ(entry, completed_entry);
  EXPECT_EQ(OB_SUCCESS, completed_entry->kvcache_put_ret_);
  ASSERT_EQ(OB_SUCCESS, preread_task.completed_put_entries_.push(completed_entry));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSTenantMacroBlockKVCacheMgr *)->check_exist(macro_id, is_exist));
  EXPECT_TRUE(is_exist);

  // put 成功后更新节点和预读统计，再回收原始 buffer。
  // mittest 默认关闭诊断统计；本段仅开启当前线程，并使用独立统计槽。
  bool &disable_diagnose_info = lib::ObPerfModeGuard::get_tl_instance();
  const bool original_disable_diagnose_info = disable_diagnose_info;
  disable_diagnose_info = false;
  DEFER(disable_diagnose_info = original_disable_diagnose_info);
  ObDiagnosticInfoSlot stat_slot;
  ObTenantDiagnosticInfoSummaryGuard stat_guard(&stat_slot);
  ASSERT_TRUE(lib::is_diagnose_info_enabled());
  const auto *prewarm_count = stat_slot.get_stats().get(ObStatEventIds::LOCAL_CACHE_PREREAD_TASK_PREWARM);
  const auto *prewarm_bytes = stat_slot.get_stats().get(ObStatEventIds::LOCAL_CACHE_PREREAD_TASK_PREWARM_BYTES);
  ASSERT_NE(nullptr, prewarm_count);
  ASSERT_NE(nullptr, prewarm_bytes);
  const int64_t original_count = prewarm_count->stat_value_;
  const int64_t original_bytes = prewarm_bytes->stat_value_;
  ASSERT_EQ(OB_SUCCESS, preread_task.recycle_completed_put_entries_());
  EXPECT_EQ(original_count + 1, prewarm_count->stat_value_);
  EXPECT_EQ(original_bytes + data_size, prewarm_bytes->stat_value_);
  EXPECT_EQ(0, preread_task.pending_put_count_);
  EXPECT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
  EXPECT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  EXPECT_EQ(nullptr, entry->buf_);
  EXPECT_FALSE(entry->is_read_handle_valid());
  ObPrereadNode preread_node;
  // 非临时文件完成预读后不保留节点，但必须记录已完成文件。
  ASSERT_EQ(OB_HASH_NOT_EXIST, preread_cache_mgr.preread_map_.get_refactored(macro_id, preread_node));
  ASSERT_EQ(1, preread_task.preread_files_.count());
  EXPECT_EQ(macro_id, preread_task.preread_files_.at(0).file_id_);
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_file(macro_id));
}

// 场景：entry 池全部提交后停止，完成队列无人主动消费时 stop/wait 仍应回收全部 entry。
TEST_F(TestSSPreReadTask, async_put_stop_returns_all_entries)
{
  SCOPED_TRACE("entry 池全部提交后停止，完成队列无人主动消费时 stop/wait 仍应回收全部 entry。");
  ObPrereadCacheManager preread_cache_mgr;
  ASSERT_EQ(OB_SUCCESS, preread_cache_mgr.init(MTL(ObTenantFileManager *)));
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ASSERT_EQ(OB_SUCCESS, preread_task.put_worker_.start());
  ObArray<MacroBlockId> macro_ids;
  ObSSPreReadEntry *entry = nullptr;
  for (int64_t i = 0; i < preread_task.max_pre_read_parallelism_; ++i) {
    ASSERT_EQ(OB_SUCCESS, prepare_completed_preread(preread_cache_mgr, 9100 + i, entry));
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(entry->file_meta_.file_id_));
  }
  ASSERT_EQ(OB_SUCCESS, preread_task.write_preread_entries());
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.pending_put_count_);
  ASSERT_EQ(0, preread_task.free_list_.get_curr_total());
  // 故意不消费完成队列；即使所有 entry 都在后台，stop/wait 也必须独立完成。
  preread_cache_mgr.stop();
  ASSERT_EQ(OB_SERVICE_STOPPED, preread_task.put_worker_.submit(*entry));
  preread_cache_mgr.wait();
  ASSERT_EQ(0, preread_task.pending_put_count_);
  ASSERT_EQ(preread_task.max_pre_read_parallelism_, preread_task.free_list_.get_curr_total());
  ASSERT_EQ(0, preread_task.completed_put_entries_.get_curr_total());
  ASSERT_EQ(0, preread_cache_mgr.get_preread_map_size());
  for (int64_t i = 0; i < macro_ids.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantFileManager *)->delete_tmp_file(macro_ids.at(i)));
  }
}

// 场景：共享 tablet 子元数据预读进入 L1 后，在低内存压力下仍能下沉到 L2 并正确回读。
TEST_F(TestSSPreReadTask, preread_shared_tablet_sub_meta)
{
  SCOPED_TRACE("共享 tablet 子元数据预读进入 L1 后，在低内存压力下仍能下沉到 L2 并正确回读。");
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // 把租户内存使用率降至低速区间，验证无需制造压力也能完成 L1 到 L2 的持久化。
  const int64_t initial_tenant_memory_hold =
      lib::get_tenant_memory_hold(MTL_ID());
  const int64_t safe_tenant_memory_limit = OB_MAX(
      original_tenant_memory_limit_,
      (initial_tenant_memory_hold * 100 + 49) / 50);
  tenant_memory_limit_overridden_ = true;
  lib::set_tenant_memory_limit(MTL_ID(), safe_tenant_memory_limit);
  LOG_INFO("stabilize tenant memory before preread persistence",
      K(initial_tenant_memory_hold), K(safe_tenant_memory_limit),
      K(original_tenant_memory_limit_));
  ASSERT_LT(lib::get_tenant_memory_hold(MTL_ID()) * 100,
      lib::get_tenant_memory_limit(MTL_ID()) * 75);

  // 1. 构造 256KB 的 shared_tablet_sub_meta 文件标识，覆盖非完整宏块预读。
  const int64_t SHARED_TABLET_SUB_META_SIZE = 256 * 1024; // 256KB
  uint64_t tablet_id = 200001;
  uint64_t op_id = 1;
  uint64_t data_seq = 1;

  MacroBlockId macro_id;
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_TABLET_SUB_META);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_reorganization_scn(0);
  macro_id.set_op_id(op_id);
  macro_id.set_seq_id(data_seq);
  ASSERT_TRUE(macro_id.is_valid());

  // 2. 准备 256KB 数据，后续用原始内容校验预读结果。
  ObMemAttr attr(MTL_ID(), "test_ss_preread");
  char *write_buf = static_cast<char *>(ob_malloc(SHARED_TABLET_SUB_META_SIZE, attr));
  ASSERT_NE(nullptr, write_buf);
  memset(write_buf, 'x', SHARED_TABLET_SUB_META_SIZE);
  
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = SHARED_TABLET_SUB_META_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  write_info.io_desc_.set_write_through(true);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  // 3. 写入对象存储并等待完成。
  ObSSShareMacroWriter share_macro_writer;
  ASSERT_EQ(OB_SUCCESS, share_macro_writer.aio_write(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // 4. 确认远端文件存在，区分数据准备失败与缓存链路失败。
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // 5. 清空两级缓存并记录刷盘计数基线，排除历史缓存和刷盘的干扰。
  tenant_kvcache_mgr->enable_cache();
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->erase_tenant_entries());
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->force_evict_by_macro_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
  ObSSTenantMacroBlockKVCacheMgrStat cache_stat_before;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get_cache_stat(cache_stat_before));

  // 6. 准备读取参数及有效 tablet 标识。
  char *read_buf = static_cast<char *>(ob_malloc(SHARED_TABLET_SUB_META_SIZE, attr));
  ASSERT_NE(nullptr, read_buf);
  memset(read_buf, 0, SHARED_TABLET_SUB_META_SIZE);

  ObStorageObjectReadInfo read_info;
  read_info.io_desc_.set_wait_event(1);
  read_info.buf_ = read_buf;
  read_info.macro_block_id_ = macro_id;
  read_info.offset_ = 1;
  read_info.size_ = SHARED_TABLET_SUB_META_SIZE / 2;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.set_effective_tablet_id(ObTabletID(tablet_id));

  // 7. 多次读取触发 cache miss 和预读，让数据经异步 put 进入缓存。
  const int READ_COUNT = 3;
  for (int i = 0; i < READ_COUNT; ++i) {
    ObStorageObjectHandle read_object_handle;
    ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info, read_object_handle));
    ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
    ASSERT_NE(nullptr, read_object_handle.get_buffer());
    ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
    // Verify read data correctness
    ASSERT_EQ(0, memcmp(write_buf + read_info.offset_, read_object_handle.get_buffer(), read_info.size_));
    read_object_handle.reset();
  }

  // 8. 等待读任务和异步 put 完成；超时时输出队列及进行中的任务数。
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          K(preread_cache_mgr.preread_queue_.size()), K(preread_task.segment_files_.count()),
          K(preread_task.async_read_list_.get_curr_total()), K(preread_task.pending_put_count_));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // 9. 验证预读结果进入租户 KVCACHE，并核对内容。
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  // 10. 保持低压力，等待后台持续低速刷盘；任务也可能已在前面的预读等待期间完成。
  ObSSMacroPersistWorker &persist_worker = tenant_kvcache_mgr->persist_worker_;
  ObSSMacroPersistPressureSnapshot snapshot;
  ObSSMacroPersistDrainPolicy policy;
  persist_worker.collect_pressure_snapshot_(snapshot);
  ASSERT_EQ(OB_SUCCESS, ObSSMacroPersistPolicyController::calculate_drain_policy(
      snapshot, persist_worker.write_task_.capacity(), policy));
  ASSERT_EQ(2, policy.scan_batch_size_);
  ASSERT_EQ(2, policy.target_inflight_count_);
  ASSERT_EQ(100000, policy.min_batch_interval_us_);

  // 等待目标macro到达L2，并确认Worker完成持久化IO。
  start_us = ObTimeUtility::current_time();
  const int64_t persist_timeout_us = 10 * 1000 * 1000L;
  bool is_persist_success = false;
  bool is_local_exist = false;
  ObSSTenantMacroBlockKVCacheMgrStat cache_stat_after;
  while (ObTimeUtility::current_time() - start_us < persist_timeout_us) {
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_id, is_local_exist));
    ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get_cache_stat(cache_stat_after));
    if (is_local_exist
        && cache_stat_after.persist_stat_.persist_success_cnt_
            > cache_stat_before.persist_stat_.persist_success_cnt_) {
      is_persist_success = true;
      break;
    }
    ob_usleep(50 * 1000L);
  }

  LOG_INFO("after KVCACHE persistence", K(cache_stat_before),
      K(cache_stat_after), K(is_persist_success), K(is_local_exist));
  ASSERT_TRUE(is_persist_success);
  ASSERT_GT(cache_stat_after.persist_stat_.persist_success_cnt_,
      cache_stat_before.persist_stat_.persist_success_cnt_);
  ASSERT_GT(cache_stat_after.persist_stat_.persist_success_bytes_,
      cache_stat_before.persist_stat_.persist_success_bytes_);
  ASSERT_EQ(0, cache_stat_after.persist_stat_.persist_inflight_cnt_);
  lib::set_tenant_memory_limit(MTL_ID(), original_tenant_memory_limit_);
  tenant_memory_limit_overridden_ = false;

  // 11. Verify data has been written to local disk cache.
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_local_exist));
  ASSERT_TRUE(is_local_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);
  LOG_INFO("macro block is now in local disk cache", K(macro_id));

  ObSSMacroCacheStat macro_cache_stat;
  macro_cache_stat.reset();
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_space_mgr);
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_cache_stat));
  // 13. delete file
  ASSERT_EQ(OB_SUCCESS, file_manager->delete_file(macro_id));
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_remote_file(macro_id, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, file_manager->is_exist_local_file(macro_id, 0/*ls_epoch_id*/, is_local_exist));
  ASSERT_FALSE(is_local_exist);
  int64_t expected_disk_size = macro_cache_stat.used_ - SHARED_TABLET_SUB_META_SIZE;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_cache_stat));
  ASSERT_EQ(expected_disk_size, macro_cache_stat.used_);

  // 13. Clean up resources
  ob_free(write_buf);
  ob_free(read_buf);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_preread_task.log*");
  OB_LOGGER.set_file_name("test_ss_preread_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
