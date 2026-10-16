/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGETEST

#include <stdint.h>
#include <gtest/gtest.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "lib/allocator/page_arena.h"
#include "lib/checksum/ob_crc64.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

class TestSSLocalCacheService : public ::testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp() override;
};

void TestSSLocalCacheService::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSLocalCacheService::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSLocalCacheService::SetUp()
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  tenant_kvcache_mgr->enable_cache();
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->erase_tenant_entries());
  tenant_kvcache_mgr->reset_hit_stat_();
}

TEST_F(TestSSLocalCacheService, basic)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  ASSERT_TRUE(OB_GLOBAL_MACRO_BLOCK_KVCACHE.is_inited());
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->check_exist(
      TestSSCommonUtil::gen_macro_block_id(99), is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestSSLocalCacheService, reinit_after_destroy)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  tenant_kvcache_mgr->stop();
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->wait());
  tenant_kvcache_mgr->destroy();

  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->init(MTL_ID()));
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->start());
}

TEST_F(TestSSLocalCacheService, put_get_and_duplicate)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(100);
  const char macro_data[] = "0123456789abcdef";

  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_kvcache_mgr->put(
      macro_id, macro_id.second_id(), macro_data,
      static_cast<int64_t>(INT32_MAX) + 1));
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->put(
      macro_id, macro_id.second_id(), macro_data, sizeof(macro_data) - 1));
  // 重复 put 保留旧值，并按幂等成功返回。
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->put(
      macro_id, macro_id.second_id(), macro_data, sizeof(macro_data) - 1));

  char read_buf[8] = {};
  ObIOInfo io_info;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      io_info, macro_id, 12/*offset*/, sizeof(read_buf), read_buf));
  ObStorageObjectHandle object_handle;
  bool is_hit = false;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get(
      macro_id, io_info, object_handle, is_hit));
  ASSERT_TRUE(is_hit);
  const int64_t actual_read_size =
      static_cast<int64_t>(sizeof(macro_data) - 1) - io_info.offset_;
  ASSERT_EQ(0, MEMCMP(
      read_buf, macro_data + io_info.offset_, actual_read_size));

  bool is_exist = false;
  ASSERT_EQ(
      OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  const MacroBlockId missing_macro_id =
      TestSSCommonUtil::gen_macro_block_id(101);
  char missing_read_buf[8] = {};
  const int32_t missing_read_size =
      static_cast<int32_t>(sizeof(missing_read_buf));
  ObIOInfo missing_io_info;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      missing_io_info, missing_macro_id, 0/*offset*/,
      missing_read_size, missing_read_buf));
  ObStorageObjectHandle missing_object_handle;
  is_hit = true;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get(
      missing_macro_id, missing_io_info, missing_object_handle, is_hit));
  ASSERT_FALSE(is_hit);

  ObSSTenantMacroBlockKVCacheMgrStat cache_stat;
  ASSERT_EQ(
      OB_SUCCESS, tenant_kvcache_mgr->get_cache_stat(cache_stat));
  ASSERT_EQ(1, cache_stat.hit_cnt_);
  ASSERT_EQ(actual_read_size, cache_stat.hit_size_);
  ASSERT_EQ(1, cache_stat.miss_cnt_);
  ASSERT_EQ(missing_read_size, cache_stat.miss_size_);
  ASSERT_EQ(1, cache_stat.valid_macro_cnt_);
  ASSERT_GT(cache_stat.memory_size_, 0);
}

TEST_F(TestSSLocalCacheService, disabled_cache_still_allows_cleanup)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(101);
  const char macro_data[] = "macro block";
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->put(
      macro_id, macro_id.second_id(), macro_data, sizeof(macro_data)));

  bool is_exist = false;
  ASSERT_EQ(
      OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  tenant_kvcache_mgr->disable_cache();
  ASSERT_FALSE(tenant_kvcache_mgr->is_cache_enabled());
  ASSERT_EQ(
      OB_SUCCESS, tenant_kvcache_mgr->erase_macro_block(macro_id));

  tenant_kvcache_mgr->enable_cache();
  ASSERT_TRUE(tenant_kvcache_mgr->is_cache_enabled());

  is_exist = true;
  ASSERT_EQ(
      OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestSSLocalCacheService, oversized_read_returns_aligned_data)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(109);
  const char macro_data[] = "0123456789abcdef";
  const int64_t macro_size = sizeof(macro_data) - 1;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->put(
      macro_id, macro_id.second_id(), macro_data, macro_size));

  const int32_t read_size = 2 * 1024 * 1024;
  ObArenaAllocator allocator("TestKVRead");
  char *read_buf = static_cast<char *>(allocator.alloc(read_size));
  ASSERT_NE(nullptr, read_buf);
  ObIOInfo io_info;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      io_info, macro_id, 0/*offset*/, read_size, read_buf));
  ObStorageObjectHandle object_handle;
  bool is_hit = false;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get(
      macro_id, io_info, object_handle, is_hit));
  ASSERT_TRUE(is_hit);
  ASSERT_EQ(read_size, io_info.size_);
  ASSERT_EQ(upper_align(macro_size, DIO_ALIGN_SIZE),
      object_handle.get_data_size());
  ASSERT_NE(nullptr, object_handle.get_buffer());
  ASSERT_EQ(0, MEMCMP(object_handle.get_buffer(), macro_data, macro_size));
}

TEST_F(TestSSLocalCacheService, invalid_read_range_is_rejected)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  const MacroBlockId cached_macro_id =
      TestSSCommonUtil::gen_macro_block_id(110);
  const MacroBlockId missing_macro_id =
      TestSSCommonUtil::gen_macro_block_id(111);
  const char macro_data[] = "0123456789abcdef";
  const int64_t macro_size = sizeof(macro_data) - 1;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->put(
      cached_macro_id, cached_macro_id.second_id(), macro_data,
      macro_size));

  char read_buf[8] = {};
  ObIOInfo io_info;
  ObStorageObjectHandle object_handle;
  bool is_hit = false;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      io_info, cached_macro_id, -1/*offset*/, sizeof(read_buf), read_buf));
  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_kvcache_mgr->get(
      cached_macro_id, io_info, object_handle, is_hit));
  ASSERT_FALSE(is_hit);

  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      io_info, missing_macro_id, -1/*offset*/, sizeof(read_buf), read_buf));
  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_kvcache_mgr->get(
      missing_macro_id, io_info, object_handle, is_hit));
  ASSERT_FALSE(is_hit);

  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      io_info, cached_macro_id, 0/*offset*/, 0/*size*/, read_buf));
  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_kvcache_mgr->get(
      cached_macro_id, io_info, object_handle, is_hit));
  ASSERT_FALSE(is_hit);

  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      io_info, missing_macro_id, 0/*offset*/, 0/*size*/, read_buf));
  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_kvcache_mgr->get(
      missing_macro_id, io_info, object_handle, is_hit));
  ASSERT_FALSE(is_hit);

  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      io_info, cached_macro_id, macro_size/*offset*/, 1/*size*/, read_buf));
  ASSERT_EQ(OB_INVALID_ARGUMENT, tenant_kvcache_mgr->get(
      cached_macro_id, io_info, object_handle, is_hit));
  ASSERT_FALSE(is_hit);

  ObSSTenantMacroBlockKVCacheMgrStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->get_cache_stat(cache_stat));
  ASSERT_EQ(0, cache_stat.hit_cnt_);
  ASSERT_EQ(0, cache_stat.miss_cnt_);
}

TEST_F(TestSSLocalCacheService, value_crc_and_partial_range)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(102);
  const char macro_data[] = "0123456789abcdef";
  ObSSMacroBlockCacheValue value(
      macro_id.second_id(), macro_data, sizeof(macro_data) - 1,
      static_cast<uint32_t>(ob_crc64(macro_data, sizeof(macro_data) - 1)));
  ASSERT_EQ(OB_SUCCESS, value.check_crc());
  ++value.crc_;
  ASSERT_EQ(OB_CHECKSUM_ERROR, value.check_crc());

  char read_buf[8] = {};
  ObIOInfo io_info;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::init_io_info(
      io_info, macro_id, 12/*offset*/, sizeof(read_buf), read_buf));
  ObStorageObjectHandle object_handle;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->handle_hit_macro_block_(
      macro_id, macro_data + io_info.offset_, value.data_size_ - io_info.offset_,
      io_info, object_handle));
  ASSERT_EQ(0, MEMCMP(read_buf, macro_data + io_info.offset_, 4));
  ASSERT_EQ(macro_id, object_handle.get_macro_id());
}

TEST_F(TestSSLocalCacheService, unaligned_value_is_not_persisted)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  MacroBlockId macro_id;
  macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  macro_id.set_second_id(1);
  macro_id.set_third_id(1);
  ASSERT_TRUE(macro_id.is_valid());

  const char data[] = "unaligned";
  const ObSSMacroBlockCacheValue value(
      OB_INVALID_ID, data, sizeof(data) - 1,
      static_cast<uint32_t>(ob_crc64(data, sizeof(data) - 1)));
  bool can_persist = true;
  ASSERT_EQ(OB_SUCCESS,
      tenant_kvcache_mgr->persist_worker_.check_can_persist_(
          macro_id, value, can_persist));
  ASSERT_FALSE(can_persist);
}

TEST_F(TestSSLocalCacheService, ring_buffer_overwrites_oldest_and_reports_stat)
{
  static const int64_t CAPACITY = 2;
  ObSSMacroRingBuffer ring_buffer;
  const MacroBlockId first_macro_id =
      TestSSCommonUtil::gen_macro_block_id(120);
  const MacroBlockId second_macro_id =
      TestSSCommonUtil::gen_macro_block_id(121);
  const MacroBlockId third_macro_id =
      TestSSCommonUtil::gen_macro_block_id(122);
  const int64_t enqueue_start_ts = ObTimeUtility::current_time();

  ASSERT_EQ(OB_SUCCESS, ring_buffer.init(MTL_ID(), CAPACITY));
  ASSERT_EQ(OB_SUCCESS, ring_buffer.push_overwrite(first_macro_id));
  ASSERT_EQ(OB_SUCCESS, ring_buffer.push_overwrite(second_macro_id));
  ASSERT_EQ(OB_SUCCESS, ring_buffer.push_overwrite(third_macro_id));

  ObSSMacroRingBufferStat stat;
  ring_buffer.get_stat_snapshot(stat);
  ASSERT_EQ(CAPACITY, stat.capacity_);
  ASSERT_EQ(3, stat.enqueue_cnt_);
  ASSERT_EQ(1, stat.overwrite_oldest_cnt_);
  ASSERT_EQ(CAPACITY, stat.queue_current_);

  ObSSMacroPersistEntry entry;
  ASSERT_EQ(OB_SUCCESS, ring_buffer.try_pop(entry));
  ASSERT_EQ(second_macro_id, entry.macro_id_);
  ASSERT_GE(entry.enqueue_ts_, enqueue_start_ts);
  ASSERT_EQ(OB_SUCCESS, ring_buffer.try_pop(entry));
  ASSERT_EQ(third_macro_id, entry.macro_id_);
  ASSERT_GE(entry.enqueue_ts_, enqueue_start_ts);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ring_buffer.try_pop(entry));

  ring_buffer.get_stat_snapshot(stat);
  ASSERT_EQ(0, stat.queue_current_);
  ASSERT_EQ(CAPACITY, ring_buffer.free_size());
  ring_buffer.destroy();
}

TEST_F(TestSSLocalCacheService, recent_evicted_entry_is_skipped_and_counted)
{
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ObSSMacroCacheMgr *l2_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  ASSERT_NE(nullptr, l2_mgr);
  const MacroBlockId macro_id =
      TestSSCommonUtil::gen_macro_block_id(123);
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->erase_macro_block(macro_id));
  ASSERT_EQ(OB_SUCCESS, l2_mgr->force_evict_by_macro_id(macro_id));

  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_kvcache_mgr->check_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, l2_mgr->exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);

  ObSSMacroPersistStat stat_before;
  tenant_kvcache_mgr->persist_worker_.get_persist_stat_snapshot(stat_before);
  ObSSMacroPersistEntry entry;
  entry.macro_id_ = macro_id;
  entry.enqueue_ts_ = ObTimeUtility::current_time();
  const ObSSMacroBlockCacheValue *value = nullptr;
  ObKVCacheHandle kv_handle;
  bool need_submit = true;
  ASSERT_EQ(OB_SUCCESS,
      tenant_kvcache_mgr->persist_worker_.prepare_persist_entry_(
          entry, value, kv_handle, need_submit));
  ASSERT_FALSE(need_submit);
  ASSERT_EQ(nullptr, value);
  ASSERT_FALSE(kv_handle.is_valid());
  static const int64_t RECENT_ENTRY_INTERVAL_US = 5 * 60 * 1000 * 1000L;
  ASSERT_LT(ObTimeUtility::current_time() - entry.enqueue_ts_,
      RECENT_ENTRY_INTERVAL_US);

  ObSSMacroPersistStat stat_after;
  tenant_kvcache_mgr->persist_worker_.get_persist_stat_snapshot(stat_after);
  ASSERT_EQ(stat_before.persist_peek_miss_cnt_ + 1,
      stat_after.persist_peek_miss_cnt_);
}

TEST_F(TestSSLocalCacheService, write_lcache_task_rejects_invalid_arguments)
{
  ObSSWriteKV2LCacheTask task;
  ASSERT_EQ(OB_SUCCESS, task.init(MTL_ID(), 1/*capacity*/));
  ObSSMacroPersistEntry persist_entry;
  ObSSWriteKV2LCacheEntry write_entry;
  ObSSMacroBlockCacheValue invalid_value;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      task.async_persist_macro_block(persist_entry, invalid_value));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.do_write_into_macro_cache_(
      persist_entry, invalid_value, write_entry));

  persist_entry.macro_id_ = TestSSCommonUtil::gen_macro_block_id(124);
  const char data[] = "macro block";
  const ObSSMacroBlockCacheValue valid_value(
      OB_INVALID_ID, data, sizeof(data) - 1,
      static_cast<uint32_t>(ob_crc64(data, sizeof(data) - 1)));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      task.async_persist_macro_block(persist_entry, valid_value));

  persist_entry.enqueue_ts_ = ObTimeUtility::current_time();
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      task.async_persist_macro_block(persist_entry, invalid_value));
  task.destroy();
}

TEST_F(TestSSLocalCacheService, write_lcache_task_entry_pool_and_stat)
{
  static const int64_t CAPACITY = 4;
  ObSSWriteKV2LCacheTask task;
  ObSSWriteKV2LCacheEntry *entries[CAPACITY] = {};
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      task.init(OB_SERVER_TENANT_ID, CAPACITY));
  ASSERT_EQ(OB_SUCCESS, task.init(MTL_ID(), CAPACITY));

  ObSSWriteKV2LCacheStat stat;
  task.get_stat_snapshot(stat);
  ASSERT_EQ(0, stat.submit_fail_cnt_);
  ASSERT_EQ(0, stat.success_cnt_);
  ASSERT_EQ(0, stat.success_bytes_);
  ASSERT_EQ(0, stat.io_fail_cnt_);
  ASSERT_EQ(0, stat.inflight_cnt_);

  for (int64_t i = 0; i < CAPACITY; ++i) {
    ASSERT_EQ(OB_SUCCESS, task.acquire_entry_(entries[i]));
    ASSERT_NE(nullptr, entries[i]);
  }
  ObSSWriteKV2LCacheEntry *overflow_entry = nullptr;
  ASSERT_EQ(OB_EAGAIN, task.acquire_entry_(overflow_entry));
  ASSERT_EQ(nullptr, overflow_entry);
  task.get_stat_snapshot(stat);
  ASSERT_EQ(1, stat.submit_fail_cnt_);
  ASSERT_EQ(0, stat.success_cnt_);
  ASSERT_EQ(0, stat.success_bytes_);
  ASSERT_EQ(0, stat.io_fail_cnt_);
  ASSERT_EQ(0, stat.inflight_cnt_);

  for (int64_t i = 0; i < CAPACITY; ++i) {
    ASSERT_EQ(OB_SUCCESS, task.recycle_entry_(*entries[i]));
  }
  ASSERT_EQ(CAPACITY, task.free_entry_list_.get_curr_total());
  task.destroy();
}

TEST_F(TestSSLocalCacheService, write_lcache_task_handles_all_entries_after_recycle_failure)
{
  static const int64_t CAPACITY = 2;
  ObSSWriteKV2LCacheTask task;
  ObSSWriteKV2LCacheEntry *entries[CAPACITY] = {};
  ObSSWriteKV2LCacheEntry dummy_entries[CAPACITY];
  ASSERT_EQ(OB_SUCCESS, task.init(MTL_ID(), CAPACITY));

  for (int64_t i = 0; i < CAPACITY; ++i) {
    ASSERT_EQ(OB_SUCCESS, task.acquire_entry_(entries[i]));
    ASSERT_NE(nullptr, entries[i]);
    ASSERT_EQ(OB_SUCCESS, task.write_entry_list_.push(entries[i]));
    // Fill the free list to inject recycle failures for all inflight entries.
    ASSERT_EQ(OB_SUCCESS, task.free_entry_list_.push(&dummy_entries[i]));
  }
  ASSERT_EQ(CAPACITY, task.write_entry_list_.get_curr_total());
  ASSERT_EQ(CAPACITY, task.free_entry_list_.get_curr_total());

  ASSERT_EQ(OB_SUCCESS, task.handle_completed_entry());
  ASSERT_EQ(0, task.write_entry_list_.get_curr_total());
  ASSERT_EQ(CAPACITY, task.free_entry_list_.get_curr_total());
  task.destroy();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_ss_local_cache_service.log*");
  OB_LOGGER.set_file_name("test_ss_local_cache_service.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
