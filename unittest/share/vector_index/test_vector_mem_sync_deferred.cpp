/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#undef private

namespace oceanbase
{
namespace share
{
using namespace common;

class TestVectorMemSyncDeferred : public ::testing::Test
{
public:
  TestVectorMemSyncDeferred() : info_(OB_SYS_TENANT_ID), tablet_id_(200001) {}

  void SetUp() override
  {
    ObLSID ls_id(1001);
    ASSERT_EQ(OB_SUCCESS, info_.init(16, OB_SYS_TENANT_ID, ls_id));
  }

  void TearDown() override
  {
    destroy_task_contexts(info_.first_mem_sync_map_);
    destroy_task_contexts(info_.second_mem_sync_map_);
    info_.destroy();
  }

  void defer_until(const int64_t next_schedule_ts)
  {
    ASSERT_EQ(OB_SUCCESS, info_.add_deferred_task(tablet_id_, 500001, OB_REPLICA_NOT_READABLE));
    ObVectorIndexMemSyncDeferredInfo deferred;
    ASSERT_EQ(OB_SUCCESS, info_.deferred_map_.get_refactored(tablet_id_, deferred));
    // Control the deadline directly so the regression does not depend on sleeps.
    deferred.next_schedule_ts_ = next_schedule_ts;
    ASSERT_EQ(OB_SUCCESS, info_.deferred_map_.set_refactored(tablet_id_, deferred, 1));
  }

  static void destroy_task_contexts(VectorIndexMemSyncMap &map)
  {
    for (auto iter = map.begin(); iter != map.end(); ++iter) {
      if (nullptr != iter->second) {
        iter->second->~ObVecIndexAsyncTaskCtx();
      }
    }
  }

  ObVectorIndexMemSyncInfo info_;
  ObTabletID tablet_id_;
};

TEST_F(TestVectorMemSyncDeferred, due_task_is_enqueued_and_consumed)
{
  ASSERT_NO_FATAL_FAILURE(defer_until(0));
  ASSERT_EQ(0, info_.get_waiting_size());

  ASSERT_EQ(OB_SUCCESS, info_.promote_due_deferred_tasks());
  ASSERT_EQ(1, info_.get_waiting_size());
  EXPECT_EQ(0, info_.deferred_map_.size());
  ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, info_.get_waiting_map().get_refactored(tablet_id_, task_ctx));
  ASSERT_NE(nullptr, task_ctx);
  EXPECT_EQ(tablet_id_, task_ctx->task_status_.tablet_id_);
  EXPECT_EQ(500001, task_ctx->task_status_.table_id_);

  bool need_sync = false;
  bool all_finished = false;
  info_.check_and_switch_if_needed(need_sync, all_finished, false);
  EXPECT_TRUE(need_sync);
  EXPECT_EQ(0, info_.get_waiting_size());
  EXPECT_EQ(1, info_.get_processing_size());
  ASSERT_EQ(OB_SUCCESS, info_.promote_due_deferred_tasks());
  EXPECT_EQ(0, info_.get_waiting_size());
}

TEST_F(TestVectorMemSyncDeferred, future_task_stays_deferred)
{
  ASSERT_NO_FATAL_FAILURE(defer_until(INT64_MAX));
  ASSERT_EQ(OB_SUCCESS, info_.promote_due_deferred_tasks());
  EXPECT_EQ(0, info_.get_waiting_size());
  EXPECT_EQ(1, info_.deferred_map_.size());
}

TEST_F(TestVectorMemSyncDeferred, existing_waiting_task_is_preserved)
{
  ASSERT_NO_FATAL_FAILURE(defer_until(0));
  ASSERT_EQ(OB_SUCCESS, info_.add_task_to_waiting_map(tablet_id_, 500001));
  ObVecIndexAsyncTaskCtx *existing_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, info_.get_waiting_map().get_refactored(tablet_id_, existing_ctx));

  ASSERT_EQ(OB_SUCCESS, info_.promote_due_deferred_tasks());
  EXPECT_EQ(1, info_.get_waiting_size());
  EXPECT_EQ(0, info_.deferred_map_.size());
  ObVecIndexAsyncTaskCtx *task_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, info_.get_waiting_map().get_refactored(tablet_id_, task_ctx));
  EXPECT_EQ(existing_ctx, task_ctx);
}

TEST_F(TestVectorMemSyncDeferred, lookup_error_preserves_deferred_task)
{
  ASSERT_NO_FATAL_FAILURE(defer_until(0));
  VectorIndexMemSyncMap &waiting_map = info_.get_waiting_map();
  waiting_map.destroy();

  EXPECT_EQ(OB_NOT_INIT, info_.promote_due_deferred_tasks());
  EXPECT_EQ(1, info_.deferred_map_.size());
  ASSERT_EQ(OB_SUCCESS, waiting_map.create(16, "VecIdxTaskMap", "VecIdxTaskMap", OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, info_.promote_due_deferred_tasks());
  EXPECT_EQ(1, info_.get_waiting_size());
  EXPECT_EQ(0, info_.deferred_map_.size());
}

TEST_F(TestVectorMemSyncDeferred, allocation_failure_preserves_deferred_task)
{
  class FailAllocator : public ObIAllocator
  {
  public:
    void *alloc(const int64_t size) override { UNUSED(size); return nullptr; }
    void *alloc(const int64_t size, const ObMemAttr &attr) override
    {
      UNUSED(attr);
      return alloc(size);
    }
    void free(void *ptr) override { UNUSED(ptr); }
  } fail_allocator;
  ASSERT_NO_FATAL_FAILURE(defer_until(0));
  ObArenaAllocator &allocator = static_cast<ObArenaAllocator &>(info_.get_waiting_allocator());
  allocator.arena_.page_allocator_.set_allocator(&fail_allocator);
  const int ret = info_.promote_due_deferred_tasks();
  allocator.arena_.page_allocator_.set_allocator(nullptr);

  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);
  EXPECT_EQ(0, info_.get_waiting_size());
  EXPECT_EQ(1, info_.deferred_map_.size());
  ASSERT_EQ(OB_SUCCESS, info_.promote_due_deferred_tasks());
  EXPECT_EQ(1, info_.get_waiting_size());
  EXPECT_EQ(0, info_.deferred_map_.size());
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_file_name("test_vector_mem_sync_deferred.log", true, true);
  return RUN_ALL_TESTS();
}
