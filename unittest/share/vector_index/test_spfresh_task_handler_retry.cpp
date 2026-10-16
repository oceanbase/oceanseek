/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <thread>
#define private public
#include "share/vector_index/spfresh/ob_spfresh_task_executor.h"
#undef private
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

TEST(TestSpfreshTaskHandlerRetry, failed_publication_retains_task_and_retry_delay)
{
  ObSpfreshTaskExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.queue_map_.init());
  ObSpfreshTaskQueue *queue = nullptr;
  ASSERT_EQ(OB_SUCCESS, executor.queue_map_.get_queue(1001, queue));
  ObSpfreshTask task(ObSpfreshTaskType::REASSIGN, ObSpfreshTaskKey(2001, 1001, 7), 12345);
  bool pushed = false;
  ASSERT_EQ(OB_SUCCESS, queue->push_if_absent(task, pushed));
  ASSERT_TRUE(pushed);
  bool marked = false;
  ASSERT_EQ(OB_SUCCESS, queue->pop_and_mark_in_flight(task, marked));
  ASSERT_TRUE(marked);

  ObSpfreshTaskHandler handler;
  ASSERT_EQ(OB_SUCCESS, handler.init(1, &executor));
  using PendingTask = ObSpfreshTaskHandler::PendingTask;
  void *buf = ob_malloc(sizeof(PendingTask), ObMemAttr(1, "RetryTest"));
  ASSERT_NE(nullptr, buf);
  handler.pending_tasks_ = new (buf) PendingTask(task);
  // Make queue publication fail deterministically without losing the key.
  queue->is_inited_ = false;
  handler.retry_pending_tasks();
  EXPECT_NE(nullptr, handler.pending_tasks_);
  queue->is_inited_ = true;
  EXPECT_EQ(OB_HASH_EXIST, queue->in_flight_keys_.exist_refactored(task.task_key_));
  const int64_t before = ObTimeUtility::current_time();
  handler.retry_pending_tasks();
  EXPECT_EQ(nullptr, handler.pending_tasks_);
  ASSERT_EQ(1, queue->reassign_task_queue_.count());
  EXPECT_EQ(12345, queue->reassign_task_queue_.at(0).cutover_modify_ts_);
  EXPECT_GE(queue->reassign_task_queue_.at(0).retry_after_us_, before + 1000000);
  EXPECT_EQ(OB_HASH_NOT_EXIST, queue->in_flight_keys_.exist_refactored(task.task_key_));
  handler.retry_pending_tasks();
  EXPECT_EQ(1, queue->reassign_task_queue_.count());
  handler.destroy();
  executor.queue_map_.destroy();
}

TEST(TestSpfreshTaskHandlerRetry, destroy_releases_retained_key_and_late_retry_is_empty)
{
  ObSpfreshTaskExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.queue_map_.init());
  ObSpfreshTaskQueue *queue = nullptr;
  ASSERT_EQ(OB_SUCCESS, executor.queue_map_.get_queue(1001, queue));
  ObSpfreshTask task(ObSpfreshTaskType::REASSIGN, ObSpfreshTaskKey(2001, 1001, 7), 12345);
  bool pushed = false;
  ASSERT_EQ(OB_SUCCESS, queue->push_if_absent(task, pushed));
  bool marked = false;
  ASSERT_EQ(OB_SUCCESS, queue->pop_and_mark_in_flight(task, marked));
  ASSERT_TRUE(marked);
  ObSpfreshTaskHandler handler;
  ASSERT_EQ(OB_SUCCESS, handler.init(1, &executor));
  using PendingTask = ObSpfreshTaskHandler::PendingTask;
  void *buf = ob_malloc(sizeof(PendingTask), ObMemAttr(1, "RetryTest"));
  ASSERT_NE(nullptr, buf);
  handler.pending_tasks_ = new (buf) PendingTask(task);
  handler.destroy();
  EXPECT_EQ(nullptr, handler.pending_tasks_);
  EXPECT_EQ(nullptr, handler.executor_);
  EXPECT_EQ(OB_HASH_NOT_EXIST, queue->in_flight_keys_.exist_refactored(task.task_key_));
  handler.retry_pending_tasks();
  EXPECT_EQ(nullptr, handler.pending_tasks_);
  EXPECT_EQ(0, queue->reassign_task_queue_.count());
  executor.queue_map_.destroy();
}

TEST(TestSpfreshTaskHandlerRetry, stop_rejects_whole_round_before_service_scan)
{
  ObSpfreshTaskExecutor executor;
  // No service or cache exists: a closed gate must return without scanning.
  EXPECT_EQ(OB_SUCCESS, executor.process_queue());
  executor.is_dispatch_stopped_ = false;
  {
    ObSpfreshTaskExecutor::DispatchGuard first(executor);
    ObSpfreshTaskExecutor::DispatchGuard second(executor);
    EXPECT_TRUE(first.is_entered());
    EXPECT_TRUE(second.is_entered());
    EXPECT_EQ(2, executor.active_dispatch_count_);
    executor.stop();
    EXPECT_EQ(OB_SUCCESS, executor.process_queue());
    EXPECT_EQ(2, executor.active_dispatch_count_);
  }
  executor.wait_dispatches_();
  EXPECT_EQ(0, executor.active_dispatch_count_);
}

TEST(TestSpfreshTaskHandlerRetry, destroy_waits_for_admitted_scan_before_freeing_queue)
{
  class ExecutorForTest : public ObSpfreshTaskExecutor
  {
  public:
    void mark_inited() { is_inited_ = true; }
  } executor;
  ASSERT_EQ(OB_SUCCESS, executor.queue_map_.init());
  ASSERT_EQ(OB_SUCCESS, executor.handler_.init(1, &executor));
  executor.mark_inited();
  executor.is_dispatch_stopped_ = false;
  std::promise<void> destroy_entered;
  std::promise<void> destroy_done;
  auto entered = destroy_entered.get_future();
  auto done = destroy_done.get_future();
  std::thread destroyer;
  {
    // Hold the production admission guard at the scan phase, before dispatch.
    ObSpfreshTaskExecutor::DispatchGuard scan(executor);
    ASSERT_TRUE(scan.is_entered());
    destroyer = std::thread([&]() {
      executor.stop();
      destroy_entered.set_value();
      executor.destroy();
      destroy_done.set_value();
    });
    entered.wait();
    EXPECT_EQ(std::future_status::timeout, done.wait_for(std::chrono::milliseconds(20)));
    // The admitted scan can still use the queue while destroy is waiting.
    ObSpfreshTaskQueue *queue = nullptr;
    EXPECT_EQ(OB_SUCCESS, executor.queue_map_.get_queue(1001, queue));
    EXPECT_NE(nullptr, queue);
    EXPECT_EQ(OB_SUCCESS, executor.process_queue());
  }
  EXPECT_EQ(std::future_status::ready, done.wait_for(std::chrono::seconds(5)));
  destroyer.join();
  EXPECT_FALSE(executor.queue_map_.is_inited_);
  EXPECT_EQ(OB_SUCCESS, executor.process_queue());
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
