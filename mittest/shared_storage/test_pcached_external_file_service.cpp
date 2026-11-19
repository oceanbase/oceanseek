/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "lib/utility/ob_test_util.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "sql/engine/table/ob_pcached_external_file_service.h"
#include "sensitive_test/object_storage/test_object_storage.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected
#include "test_pcached_external_file_service.h"

using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{

const bool is_shared_storage_mode_test = true;

class TestSSPCachedExternalFileService : public TestPCachedExternalFileService
{
public:
  TestSSPCachedExternalFileService() : TestPCachedExternalFileService() {}
  virtual ~TestSSPCachedExternalFileService() {}
  int check_macro_cache_hit_status(MacroBlockId &macro_id, const bool expect_hit_status);
};

int TestSSPCachedExternalFileService::check_macro_cache_hit_status(
    MacroBlockId &macro_id, const bool expect_hit_status)
{
  int ret = OB_SUCCESS;
  bool is_macro_cache_hit = false;
  ObSSFdCacheHandle fd_handle;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_UNLIKELY(!is_valid_external_macro_id(macro_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id), K(expect_hit_status));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), K(macro_id), K(expect_hit_status));
  } else if (OB_FAIL(macro_cache_mgr->get(macro_id,
      ObTabletID(ObTabletID::INVALID_TABLET_ID)/*effective_tablet_id*/,
      0/*size*/,
      is_macro_cache_hit, fd_handle))) {
    LOG_WARN("failed to get macro cache", KR(ret), K(macro_id), K(expect_hit_status));
  } else if (OB_UNLIKELY(is_macro_cache_hit != expect_hit_status)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected macro cache hit status", KR(ret),
        K(macro_id), K(expect_hit_status), K(is_macro_cache_hit));
  }
  LOG_INFO("check_macro_cache_hit_status", KR(ret),
      K(macro_id), K(expect_hit_status), K(is_macro_cache_hit));
  return ret;
}

TEST_F(TestSSPCachedExternalFileService, test_io_callback_allocator)
{
  ObPCachedIOCallbackAllocator allocator;
  ObMemAttr attr(MTL_ID(), "TestExtIOCB");
  ASSERT_EQ(OB_SUCCESS, allocator.init(OB_MALLOC_NORMAL_BLOCK_SIZE, attr, 1 * MB));
  ASSERT_EQ(0, allocator.get_alloc_cnt());

  void *ptr1 = allocator.alloc(128);
  ASSERT_NE(nullptr, ptr1);
  ASSERT_EQ(1, allocator.get_alloc_cnt());

  void *ptr2 = allocator.alloc(256, attr);
  ASSERT_NE(nullptr, ptr2);
  ASSERT_EQ(2, allocator.get_alloc_cnt());

  allocator.free(ptr1);
  ASSERT_EQ(1, allocator.get_alloc_cnt());
  allocator.free(ptr2);
  ASSERT_EQ(0, allocator.get_alloc_cnt());
  allocator.destroy();
}

TEST_F(TestSSPCachedExternalFileService, basic_test)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);
    
    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));
    
    // read external table file, no buffered read
    const int64_t read_buf_size = 100;
    char read_buf[read_buf_size] = { 0 };
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, external_file_name, read_buf, read_buf_size, 0/*offset*/,
        external_file_info, external_read_info));
    external_read_info.io_desc_.set_buffered_read(false);
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

    // buffered read
    external_read_info.io_desc_.set_buffered_read(true);
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

TEST_F(TestSSPCachedExternalFileService, test_prefetch_task_basic)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    // generate external table file
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));
    
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);

    // init
    const int64_t read_buf_size = 100;
    char read_buf[read_buf_size] = { 0 };
    const int64_t offset = 2 * MB + 7;
    ASSERT_TRUE(offset + read_buf_size < content_size);
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, external_file_name, read_buf, read_buf_size, offset,
        external_file_info, external_read_info));

    // check macro block not cached
    MacroBlockId macro_id;
    const int64_t modify_time = external_file_info.modify_time_;
    ObPCachedExtMacroKey macro_key(external_file_name, "", modify_time, offset);
    OK(accesser->path_map_.get_or_generate(macro_key, macro_id));
    OK(check_macro_cache_hit_status(macro_id, false/*expect_hit_status*/));
    
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));
    ASSERT_EQ(1, accesser->path_map_.path_id_map_.size());
    ASSERT_EQ(1, accesser->path_map_.macro_id_map_.size());

    // check read task
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(1, accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    const int64_t start_time_us = ObTimeUtility::current_time();
    const int64_t MAX_WAIT_TIME_US = 10 * S_US;  // 10s
    const int64_t READ_TIME_LIMIT_US = ObTimeUtility::current_time() + MAX_WAIT_TIME_US;
    ObStorageIOPipelineTask<ObSSExternalDataPrefetchTaskInfo> *task = nullptr;
    OK(accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_.head_unsafe(task));
    ASSERT_NE(nullptr, task);
    ASSERT_EQ(TaskState::TASK_READ_IN_PROGRESS, task->info_.get_state());
    ASSERT_EQ(ObExternalDataPrefetchTaskInfo::READ_THEN_WRITE, task->info_.operation_type_);
    LOG_INFO("check bounded full-macro prefetch", K(task->info_));
    ASSERT_EQ(content_size, task->info_.file_size_);
    ASSERT_EQ(2 * MB, task->info_.get_read_size());
    ASSERT_EQ(2 * MB, task->info_.get_buf_size());
    while (0 < accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_.get_curr_total()
        && ObTimeUtility::current_time() < READ_TIME_LIMIT_US) {
      ob_usleep(10 * MS_US); // 10ms
      accesser->timer_task_scheduler_.prefetch_timer_.runTimerTask();
    }
    LOG_INFO("check read task", K(accesser->timer_task_scheduler_));
    
    // check write task
    const int64_t WRITE_TIME_LIMIT_US = ObTimeUtility::current_time() + MAX_WAIT_TIME_US;
    while (0 < accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_write_list_.get_curr_total()
        && ObTimeUtility::current_time() < WRITE_TIME_LIMIT_US) {
      ob_usleep(10 * MS_US); // 10ms
      accesser->timer_task_scheduler_.prefetch_timer_.runTimerTask();
    }
    LOG_INFO("check write task", K(accesser->timer_task_scheduler_));
    ASSERT_EQ(0, accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_write_list_.get_curr_total());

    // check finish task
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

    // check macro block has cached
    OK(check_macro_cache_hit_status(macro_id, true/*expect_hit_status*/));
    
    // read again, read different range in same block
    external_read_info.offset_ = offset + 10;
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));

    // read task should not be generated
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(0, accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    LOG_INFO("read task should not be generated", K(accesser->timer_task_scheduler_),
        K(external_read_info), K(external_file_info));

    {
      // read tail block, cached size should be content_size % 2MB = 7;
      const int64_t tail_block_size = content_size % (2 * MB);
      ASSERT_TRUE(tail_block_size > 0);
      ASSERT_TRUE(tail_block_size < 2 * MB);
      const int64_t tail_block_offset = content_size - tail_block_size;
      const int64_t tail_block_read_buf_size = tail_block_size + 10;
      char tail_block_read_buf[tail_block_read_buf_size] = { 0 };
      
      external_read_info.offset_ = tail_block_offset;
      external_read_info.size_ = tail_block_read_buf_size;
      external_read_info.buffer_ = tail_block_read_buf;
      ObPCachedExtMacroKey macro_key(external_file_name, "", modify_time, tail_block_offset);
      LOG_INFO("read tail block", K(macro_key), K(tail_block_offset), K(tail_block_read_buf_size), K(macro_id));
      ASSERT_EQ(OB_HASH_NOT_EXIST, accesser->path_map_.get(macro_key, macro_id));
      print_map(accesser->path_map_.path_id_map_);
      print_map(accesser->path_map_.macro_id_map_);

      ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
      OK(read_and_check(external_file_info, external_read_info, write_buf, tail_block_size));
      ASSERT_EQ(1, accesser->path_map_.path_id_map_.size());
      ASSERT_EQ(2, accesser->path_map_.macro_id_map_.size());

      // finish prefetch task
      ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
      OK(accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_
          .head_unsafe(task));
      ASSERT_NE(nullptr, task);
      LOG_INFO("check bounded tail prefetch", K(task->info_));
      ASSERT_EQ(content_size, task->info_.file_size_);
      ASSERT_EQ(tail_block_size, task->info_.get_read_size());
      ASSERT_EQ(2 * MB, task->info_.get_buf_size());
      OK(wait_prefetch_task_finish());
      LOG_INFO("finish read task", K(accesser->timer_task_scheduler_),
          K(external_read_info), K(external_file_info));
      ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

      // check macro block has cached
      OK(accesser->path_map_.get(macro_key, macro_id));
      OK(check_macro_cache_hit_status(macro_id, true/*expect_hit_status*/));

      // read again
      // read must use read size
      external_read_info.size_ = tail_block_size;
      OK(read_and_check(external_file_info, external_read_info, write_buf, tail_block_size));

      // read task should not be generated
      ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
      ASSERT_EQ(0, accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
      LOG_INFO("read task should not be generated", K(accesser->timer_task_scheduler_),
          K(external_read_info), K(external_file_info));
    }

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

// ----------------------------------------------------------------------------
// 文件尾预读正确裁剪；非法范围报错，恰好EOF跳过，均不留下去重占位。
// 场景：两个完整宏块加7字节的对象，检查任务复制、负长度及EOF边界。
// 操作：初始化任务、深拷贝、重置，再分别提交非法范围和恰好EOF的预读。
// 校验：尾块请求7字节、buffer仍为宏块大小；返回码符合范围约定且不提交无效任务。
// ----------------------------------------------------------------------------
TEST_F(TestSSPCachedExternalFileService, test_prefetch_file_length_boundaries)
{
  LOG_INFO("========== test_prefetch_file_length_boundaries begin ==========");
  if (enable_test_) {
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);
    const int64_t macro_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    const int64_t full_macro_count = 2;
    const int64_t tail_size = 7;
    const int64_t content_size = full_macro_count * macro_size + tail_size;
    const int64_t tail_offset = full_macro_count * macro_size;
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TestExtRange"));
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    char *write_buf = nullptr;

    // 使用真实文件版本构造任务，但不提交它的GET，独立检查长度复制和重置。
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));
    char read_buf[1] = { 0 };
    ObExternalAccessFileInfo file_info;
    ObExternalReadInfo read_info;
    OK(init_external_info(allocator, external_file_name, read_buf, sizeof(read_buf),
        0/*offset*/, file_info, read_info));
    ObPCachedExtMacroKey macro_key(
        external_file_name, "", file_info.get_modify_time(), tail_offset);
    MacroBlockId macro_id;
    OK(accesser->path_map_.get_or_generate(macro_key, macro_id));
    ObSSExternalDataPrefetchTaskInfo task_info;
    ObSSExternalDataPrefetchTaskInfo copied_info;
    OK(task_info.init(external_file_name, &info_base_, tail_offset, macro_id, content_size));
    OK(copied_info.assign(task_info));
    LOG_INFO("check copied prefetch range", K(task_info), K(copied_info));
    ASSERT_EQ(tail_size, copied_info.get_read_size());
    ASSERT_EQ(content_size, copied_info.file_size_);
    ASSERT_EQ(macro_size, copied_info.get_buf_size());
    ASSERT_NE(task_info.storage_info_, copied_info.storage_info_);
    copied_info.reset();
    ASSERT_EQ(-1, copied_info.file_size_);
    ASSERT_FALSE(copied_info.is_valid());

    // 负文件长度和超出EOF返回参数错误，不产生预读任务。
    const int64_t running_count = accesser->running_prefetch_tasks_map_.size();
    const int64_t beyond_eof_offset_idx = full_macro_count + 1;
    LOG_INFO("check invalid prefetch ranges", K(content_size), K(beyond_eof_offset_idx));
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        accesser->ss_add_prefetch_task(external_file_name, &info_base_, 0/*offset_idx*/,
            macro_id, -1/*file_size*/));
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        accesser->ss_add_prefetch_task(external_file_name, &info_base_, beyond_eof_offset_idx,
            macro_id, content_size));

    // 用整块对齐的长度构造恰好EOF，与上面超过EOF的场景分开验证。
    LOG_INFO("check EOF prefetch ranges", K(full_macro_count), K(tail_offset));
    OK(accesser->ss_add_prefetch_task(external_file_name, &info_base_, 0/*offset_idx*/,
        macro_id, 0/*file_size*/));
    OK(accesser->ss_add_prefetch_task(external_file_name, &info_base_, full_macro_count,
        macro_id, tail_offset/*file_size*/));
    LOG_INFO("check prefetch range cleanup", K(running_count), K(content_size));
    ASSERT_EQ(running_count, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        copied_info.init(external_file_name, &info_base_, content_size/*read_offset*/,
            macro_id, content_size));
    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
  LOG_INFO("========== test_prefetch_file_length_boundaries end ==========");
}

// ----------------------------------------------------------------------------
// 过期文件长度导致预读短读时，只丢弃缓存填充，已经完成的前台读取仍返回正确数据。
// 场景：真实对象比交给缓存服务的文件长度少1字节，当前查询只读取有效的首字节。
// 操作：完成前台读取并推进预读pipeline，等待短读任务回收。
// 校验：前台数据正确、去重记录清空，异常预读没有把不完整对象写入宏块缓存。
// ----------------------------------------------------------------------------
TEST_F(TestSSPCachedExternalFileService, test_prefetch_short_read_does_not_fill_cache)
{
  LOG_INFO("========== test_prefetch_short_read_does_not_fill_cache begin ==========");
  if (enable_test_) {
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);
    const int64_t content_size = 7;
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TestExtShort"));
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    char *write_buf = nullptr;
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));
    char read_buf[1] = { 0 };
    ObExternalAccessFileInfo file_info;
    ObExternalReadInfo read_info;
    OK(init_external_info(allocator, external_file_name, read_buf, sizeof(read_buf),
        0/*offset*/, file_info, read_info));

    // 模拟取得元数据后文件被截短，前台需要的范围仍然有效。
    file_info.file_size_ = content_size + 1;
    LOG_INFO("read valid foreground range with stale file length", K(file_info), K(read_info));
    OK(read_and_check(file_info, read_info, write_buf, sizeof(read_buf)));
    OK(wait_prefetch_task_finish());
    ObPCachedExtMacroKey macro_key(
        external_file_name, "", file_info.get_modify_time(), 0/*offset*/);
    MacroBlockId macro_id;
    OK(accesser->path_map_.get(macro_key, macro_id));
    LOG_INFO("verify short prefetch was discarded", K(macro_id), K(file_info));
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    OK(check_macro_cache_hit_status(macro_id, false/*expect_hit_status*/));
    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
  LOG_INFO("========== test_prefetch_short_read_does_not_fill_cache end ==========");
}

TEST_F(TestSSPCachedExternalFileService, test_read_complete_macro)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));
    
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);

    const int64_t read_buf_size = 2 * MB;
    char read_buf[read_buf_size] = { 0 };
    const int64_t offset = 0;
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, external_file_name, read_buf, read_buf_size, offset,
        external_file_info, external_read_info));
    
    // check macro block not cached
    MacroBlockId macro_id;
    const int64_t modify_time = external_file_info.modify_time_;
    ObPCachedExtMacroKey macro_key(external_file_name, "", modify_time, offset);
    ASSERT_EQ(OB_HASH_NOT_EXIST, accesser->path_map_.get(macro_key, macro_id));

    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    OK(read_and_check(external_file_info, external_read_info, write_buf, read_buf_size));
    ASSERT_EQ(1, accesser->path_map_.path_id_map_.size());
    ASSERT_EQ(1, accesser->path_map_.macro_id_map_.size());

    // check read task
    ASSERT_EQ(1, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(1, accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    const int64_t start_time_us = ObTimeUtility::current_time();
    const int64_t MAX_WAIT_TIME_US = 10 * S_US;  // 10s
    ObStorageIOPipelineTask<ObSSExternalDataPrefetchTaskInfo> *task = nullptr;
    OK(accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_.head_unsafe(task));
    ASSERT_NE(nullptr, task);
    ASSERT_EQ(ObExternalDataPrefetchTaskInfo::DIRECT_WRITE, task->info_.operation_type_);

    ASSERT_EQ(TaskState::TASK_READ_IN_PROGRESS, task->info_.get_state());
    accesser->timer_task_scheduler_.prefetch_timer_.runTimerTask();
    ASSERT_EQ(TaskState::TASK_WRITE_IN_PROGRESS, task->info_.get_state());
    OK(wait_prefetch_task_finish());

    // check macro block has cached
    OK(accesser->path_map_.get(macro_key, macro_id));
    OK(check_macro_cache_hit_status(macro_id, true/*expect_hit_status*/));

    // read again
    MEMSET(read_buf, 0, read_buf_size);
    external_read_info.offset_ = 10;
    external_read_info.size_ = 100;
    external_read_info.buffer_ = read_buf;
    OK(read_and_check(external_file_info, external_read_info, write_buf, external_read_info.size_));

    // read task should not be generated
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(0, accesser->timer_task_scheduler_.ss_prefetch_timer_.pipeline_.async_read_list_.get_curr_total());
    LOG_INFO("read task should not be generated", K(accesser->timer_task_scheduler_),
        K(external_read_info), K(external_file_info));

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

TEST_F(TestSSPCachedExternalFileService, test_clean_path_map)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    {
      ObExternalFilePathMap path_map;
      OK(path_map.init(MTL_ID()));
      const ObString url_a("s3://bucket/path/a");
      const ObString url_b("s3://bucket/path/b");
      const ObString url_c("s3://bucket/path/c");
      const ObPCachedExtMacroKey macro_key_1(url_a, "", 100, 0);
      const ObPCachedExtMacroKey macro_key_2(url_a, "", 100, 2 * MB);
      const ObPCachedExtMacroKey macro_key_3(url_b, "", 100, 4 * MB);
      const ObPCachedExtMacroKey macro_key_4(url_c, "", 100, 0);
      
      MacroBlockId macro_id_1;
      OK(path_map.get_or_generate(macro_key_1, macro_id_1));
      MacroBlockId macro_id_2;
      OK(path_map.get_or_generate(macro_key_2, macro_id_2));
      MacroBlockId macro_id_3;
      OK(path_map.get_or_generate(macro_key_3, macro_id_3));
      ObStorageObjectHandle macro_handle;
      MacroBlockId macro_id_4;
      OK(path_map.alloc_macro(macro_key_4, macro_handle));
      macro_id_4 = macro_handle.get_macro_id();
      MacroBlockId macro_id_5;
      const ObExternalFilePathMap::MacroMapKey macro_map_key_5(UINT64_MAX - 1, 1);
      OK(path_map.get_or_generate_macro_id_(macro_map_key_5, macro_id_5));
      
      // check map size
      ASSERT_EQ(3, path_map.path_id_map_.size());
      ASSERT_EQ(4, path_map.macro_id_map_.size());

      // clean orphaned path
      ObExternalFileReversedPathMap reversed_path_map;
      OK(reversed_path_map.create(path_map.bucket_count(), ObMemAttr(MTL_ID(), "ExtRvsPathMap")));
      OK(path_map.build_reversed_path_mapping(reversed_path_map));

      MacroBlockId tmp_macro_id;
      const ObPCachedExtMacroKey *tmp_macro_key = nullptr;
      OK(path_map.get(macro_key_1, tmp_macro_id));
      ASSERT_EQ(macro_id_1, tmp_macro_id);
      tmp_macro_key = reversed_path_map.get(macro_id_1);
      ASSERT_NE(nullptr, tmp_macro_key);
      ASSERT_EQ(url_a, tmp_macro_key->url());
      
      OK(path_map.get(macro_key_2, tmp_macro_id));
      ASSERT_EQ(macro_id_2, tmp_macro_id);
      tmp_macro_key = reversed_path_map.get(macro_id_2);
      ASSERT_NE(nullptr, tmp_macro_key);
      ASSERT_EQ(url_a, tmp_macro_key->url());
      
      OK(path_map.get(macro_key_3, tmp_macro_id));
      ASSERT_EQ(macro_id_3, tmp_macro_id);
      tmp_macro_key = reversed_path_map.get(macro_id_3);
      ASSERT_NE(nullptr, tmp_macro_key);
      ASSERT_EQ(url_b, tmp_macro_key->url());
      
      ASSERT_EQ(3, reversed_path_map.size());
      ASSERT_EQ(2, path_map.path_id_map_.size());
      ASSERT_EQ(3, path_map.macro_id_map_.size());

      OK(path_map.erase_orphaned_macro_id(macro_key_1));
      OK(path_map.erase_orphaned_macro_id(macro_key_2));
      OK(path_map.erase_orphaned_macro_id(macro_key_3));
      OK(path_map.build_reversed_path_mapping(reversed_path_map));
      ASSERT_TRUE(reversed_path_map.empty());
    }

    // generate external table file
    ObArenaAllocator allocator;
    const int64_t content_size = 4 * MB + 7;
    char ext_file_a[OB_MAX_URI_LENGTH] = { 0 };
    char ext_file_b[OB_MAX_URI_LENGTH] = { 0 };
    char ext_file_c[OB_MAX_URI_LENGTH] = { 0 };
    char *write_buf_a = nullptr;
    char *write_buf_b = nullptr;
    char *write_buf_c = nullptr;
    OK(generate_external_file(allocator, content_size,
        ext_file_a, sizeof(ext_file_a), write_buf_a));
    OK(generate_external_file(allocator, content_size,
        ext_file_b, sizeof(ext_file_b), write_buf_b));
    OK(generate_external_file(allocator, content_size,
        ext_file_c, sizeof(ext_file_c), write_buf_c));
    
    // cache data
    // a: [0, 2MB), [2MB, 4MB), [4MB, 7)
    // b: [4MB, 7) only in macro id map
    // c: [2MB, 4MB), [4MB, 7); only in path map (means not cached)
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);
    const int64_t read_buf_size = 2 * MB;
    char read_buf[read_buf_size] = { 0 };
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, ext_file_a, read_buf, read_buf_size, 0/*offset*/,
        external_file_info, external_read_info));
    // cache a
    external_read_info.size_ = 100;
    OK(read_and_check(external_file_info, external_read_info, write_buf_a, external_read_info.size_));
    external_read_info.offset_ = 2 * MB;
    external_read_info.size_ = 2 * MB;
    OK(read_and_check(external_file_info, external_read_info, write_buf_a, external_read_info.size_));
    external_read_info.offset_ = 4 * MB;
    external_read_info.size_ = (content_size % (2 * MB));
    OK(read_and_check(external_file_info, external_read_info, write_buf_a, external_read_info.size_));
    // check prefetch task
    ASSERT_EQ(3, accesser->running_prefetch_tasks_map_.size());
    OK(wait_prefetch_task_finish());
    // cache b
    external_file_info.url_ = ext_file_b;
    external_read_info.offset_ = 4 * MB;
    external_read_info.size_ = (content_size % (2 * MB));
    OK(read_and_check(external_file_info, external_read_info, write_buf_b, external_read_info.size_));
    // cache c
    external_file_info.url_ = ext_file_c;
    external_read_info.offset_ = 2 * MB;
    external_read_info.size_ = 2 * MB;
    OK(read_and_check(external_file_info, external_read_info, write_buf_c, external_read_info.size_));
    external_file_info.url_ = ext_file_c;
    external_read_info.offset_ = 4 * MB;
    external_read_info.size_ = (content_size % (2 * MB));
    OK(read_and_check(external_file_info, external_read_info, write_buf_c, external_read_info.size_));
    // check prefetch task
    ASSERT_EQ(3, accesser->running_prefetch_tasks_map_.size());
    OK(wait_prefetch_task_finish());
    // check blocks in path map
    ObPCachedExtMacroKey tmp_macro_key(ext_file_a, "", external_file_info.modify_time_, 0/*offset*/);
    MacroBlockId tmp_macro_id;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.offset_ = 2 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.url_ = ext_file_b;
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.url_ = ext_file_c;
    tmp_macro_key.offset_ = 2 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    tmp_macro_key.url_ = ext_file_c;
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));

    // generate other blocks
    // b: [4MB, 7) only in macro id map
    tmp_macro_key.url_ = ext_file_b;
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.path_id_map_.erase_refactored(tmp_macro_key));
    LOG_INFO("print_map =============================== begin");
    print_map(accesser->path_map_.path_id_map_);
    print_map(accesser->path_map_.macro_id_map_);
    // c: [2MB, 4MB), [4MB, 7); only in path map (means not cached)
    // evict c [2MB, 4MB)
    tmp_macro_key.url_ = ext_file_c;
    tmp_macro_key.offset_ = 2 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    OK(check_macro_cache_hit_status(tmp_macro_id, true/*expect_hit_status*/));
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    OK(macro_cache_mgr->erase(tmp_macro_id));
    OK(check_macro_cache_hit_status(tmp_macro_id, false/*expect_hit_status*/));
    // evict c [4MB, 7)
    tmp_macro_key.url_ = ext_file_c;
    tmp_macro_key.offset_ = 4 * MB;
    OK(accesser->path_map_.get(tmp_macro_key, tmp_macro_id));
    OK(check_macro_cache_hit_status(tmp_macro_id, true/*expect_hit_status*/));
    OK(macro_cache_mgr->erase(tmp_macro_id));
    OK(check_macro_cache_hit_status(tmp_macro_id, false/*expect_hit_status*/));

    // clean path map
    accesser->timer_task_scheduler_.cleanup_task_.runTimerTask();
    // remained blocks:
    // a: [0, 2MB), [2MB, 4MB), [4MB, 7)
    // c: only in path id map
    ASSERT_EQ(2, accesser->path_map_.path_id_map_.size());
    ASSERT_EQ(3, accesser->path_map_.macro_id_map_.size());
    // clean again
    accesser->timer_task_scheduler_.cleanup_task_.runTimerTask();
    // a: [0, 2MB), [2MB, 4MB), [4MB, 7)
    ASSERT_EQ(1, accesser->path_map_.path_id_map_.size());
    ASSERT_EQ(3, accesser->path_map_.macro_id_map_.size());

    OK(ObBackupIoAdapter::del_file(ext_file_a, &info_base_));
    OK(ObBackupIoAdapter::del_file(ext_file_b, &info_base_));
    OK(ObBackupIoAdapter::del_file(ext_file_c, &info_base_));
  }
}

TEST_F(TestSSPCachedExternalFileService, test_external_file_access)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObPCachedExternalFileService *accesser = MTL(ObPCachedExternalFileService *);
    ASSERT_NE(nullptr, accesser);

    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));
    
    // read without disk cache
    ObExternalFileAccess file_access;
    char access_info[OB_MAX_URI_LENGTH] = { 0 };
    OK(info_base_.get_storage_info_str(access_info, sizeof(access_info)));
    const ObExternalFileUrlInfo file_url_info(external_file_name, access_info, external_file_name,
        ""/*content_digest*/, content_size, 1/*modify_time*/);
    ObExternalFileCacheOptions cache_options(false/*enable_page_cache*/, false/*enable_disk_cache*/);
    OK(file_access.open(file_url_info, cache_options));

    char read_buf[content_size] = { 0 };
    // 跨macro读
    int64_t offset = 3 * MB - 1;
    ObExternalReadInfo read_info(offset, read_buf, content_size - offset, 10000/*io_timeout_ms*/);
    int64_t read_size = -1;
    OK(file_access.pread(read_info, read_size));
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());
    ASSERT_EQ(read_size, read_info.size_);
    ASSERT_EQ(0, MEMCMP(read_buf, write_buf + read_info.offset_, read_size));

    // read with disk cache
    file_access.reset();
    cache_options.set_enable_disk_cache();
    read_info.offset_ = 0;
    read_info.size_ = content_size;
    OK(file_access.open(file_url_info, cache_options));
    OK(file_access.pread(read_info, read_size));
    ASSERT_EQ(read_size, read_info.size_);
    ASSERT_EQ(0, MEMCMP(read_buf, write_buf + read_info.offset_, read_size));

    // check prefetch task
    // [0, 2MB), [2MB, 4MB), [4MB, 7)
    ASSERT_EQ(3, accesser->running_prefetch_tasks_map_.size());
    OK(wait_prefetch_task_finish());

    // read with disk cache again, disable page cache
    file_access.reset();
    cache_options.enable_page_cache_ = false;
    OK(file_access.open(file_url_info, cache_options));
    OK(file_access.pread(read_info, read_size));
    ASSERT_EQ(read_size, read_info.size_);
    ASSERT_EQ(0, MEMCMP(read_buf, write_buf + read_info.offset_, read_size));
    ASSERT_EQ(0, accesser->running_prefetch_tasks_map_.size());

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

TEST_F(TestSSPCachedExternalFileService, test_external_table_write_alloc_file_size)
{
  int ret = OB_SUCCESS;
  if (enable_test_) {
    ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
    ASSERT_NE(nullptr, disk_space_mgr);
    ObSSMacroCacheStat macro_block_stat;
    OK(disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_block_stat));
    const int64_t used_before = macro_block_stat.used_;

    // generate external table file
    const int64_t content_size = 4 * MB + 7;
    ObArenaAllocator allocator;
    char *write_buf = nullptr;
    char external_file_name[OB_MAX_URI_LENGTH] = { 0 };
    OK(generate_external_file(allocator, content_size,
        external_file_name, sizeof(external_file_name), write_buf));

    // buffered read triggers prefetch write to local cache (EXTERNAL_TABLE_FILE)
    char read_buf[100] = { 0 };
    ObExternalAccessFileInfo external_file_info;
    ObExternalReadInfo external_read_info;
    OK(init_external_info(
        allocator, external_file_name, read_buf, sizeof(read_buf), 0/*offset*/,
        external_file_info, external_read_info));
    OK(read_and_check(external_file_info, external_read_info, write_buf, external_read_info.size_));
    external_read_info.offset_ = 2 * MB;
    OK(read_and_check(external_file_info, external_read_info, write_buf, external_read_info.size_));
    external_read_info.offset_ = 4 * MB;
    external_read_info.size_ = (content_size % (2 * MB));
    OK(read_and_check(external_file_info, external_read_info, write_buf, external_read_info.size_));
    OK(wait_prefetch_task_finish());

    OK(disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_block_stat));
    ASSERT_GE(macro_block_stat.used_, used_before + content_size);

    OK(ObBackupIoAdapter::del_file(external_file_name, &info_base_));
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_pcached_external_file_service.log*");
  oceanbase::ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  OB_LOGGER.set_file_name("test_pcached_external_file_service.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
