/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <memory>
#include <future>
#include <chrono>
#include <thread>
#include <atomic>
#define private public
#define protected public
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/ob_gts_rpc.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace transaction;
namespace unittest
{

class CountingTsTask : public ObTsCbTask
{
public:
  explicit CountingTsTask(uint64_t tenant_id) : tenant_id_(tenant_id) {}
  int gts_callback_interrupted(int, share::ObLSID) override { ++callbacks_; return OB_SUCCESS; }
  int get_gts_callback(MonotonicTs, const share::SCN &, MonotonicTs) override
  { ++callbacks_; return OB_SUCCESS; }
  int gts_elapse_callback(MonotonicTs, const share::SCN &) override
  { ++callbacks_; return OB_SUCCESS; }
  MonotonicTs get_stc() const override { return MonotonicTs(1); }
  uint64_t hash() const override { return 0; }
  uint64_t get_tenant_id() const override { return tenant_id_; }
  uint64_t tenant_id_;
  int callbacks_ = 0;
};

class TestGtsSourceLifecycle : public ::testing::TestWithParam<uint64_t>
{
public:
  void SetUp() override
  {
    mgr_.reset(new ObTsMgr());
    ASSERT_EQ(OB_SUCCESS, mgr_->ts_source_info_map_.init(ObMemAttr(OB_SERVER_TENANT_ID, "TestTsSource")));
    void *buf = ob_malloc(sizeof(ObTsSourceInfo), ObMemAttr(OB_SERVER_TENANT_ID, "TestTsSource"));
    ASSERT_NE(nullptr, buf);
    source_ = new (buf) ObTsSourceInfo();
    ASSERT_EQ(OB_SUCCESS, source_->init(GetParam()));
    ASSERT_EQ(OB_SUCCESS, source_->gts_source_.queue_[0].init(GET_GTS));
    ASSERT_EQ(OB_SUCCESS, source_->gts_source_.queue_[1].init(WAIT_GTS_ELAPSING));
    source_->gts_source_.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, mgr_->ts_source_info_map_.insert_and_get(ObTsSourceInfoKey(GetParam()), source_, nullptr));
    mgr_->ts_source_info_map_.revert(source_);
  }
  void TearDown() override
  {
    // Some tests exercise the public API without starting RPC/worker threads.
    mgr_->is_running_ = false;
    mgr_->is_inited_ = false;
    // Tests do not start the RPC service. Release their source explicitly.
    if (source_ != nullptr) {
      EXPECT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam()));
    }
    mgr_.reset();
  }
  void make_obsolete()
  {
    source_->last_access_ts_ = ObClockGenerator::getClock() - ObTsMgr::TS_SOURCE_INFO_OBSOLETE_TIME - 1;
  }
  void prepare_sync_request()
  {
    auto &pool = ObTsSyncGetTsCbTaskPool::get_instance();
    if (!pool.is_inited_) {
      ASSERT_EQ(OB_SUCCESS, pool.init());
    }
    // An uninitialized location adapter returns an error, leaving the request
    // queued until the test supplies a GTS response. No RPC service is needed.
    source_->gts_source_.tenant_id_ = GetParam();
    source_->gts_source_.location_adapter_ = &mgr_->location_adapter_def_;
    mgr_->is_inited_ = true;
    mgr_->is_running_ = true;
  }
  std::unique_ptr<ObTsMgr> mgr_;
  ObTsSourceInfo *source_ = nullptr;
};

TEST_P(TestGtsSourceLifecycle, returning_tenant_cancels_obsolete_deletion)
{
  make_obsolete(); // The refresh scan has already selected this tenant.
  {
    ObTsSourceInfoGuard guard;
    ASSERT_EQ(OB_SUCCESS, mgr_->get_ts_source_info_opt_(GetParam(), guard, false, true));
    ASSERT_EQ(source_, guard.get_ts_source_info());
  }
  EXPECT_EQ(OB_EAGAIN, mgr_->delete_tenant_(GetParam(), true));
}

TEST_P(TestGtsSourceLifecycle, queued_task_survives_source_expiration)
{
  CountingTsTask task(GetParam());
  {
    ObTsSourceInfoGuard guard;
    ASSERT_EQ(OB_SUCCESS, mgr_->get_ts_source_info_opt_(GetParam(), guard, false, true));
    ASSERT_EQ(OB_SUCCESS, guard.get_ts_source_info()->gts_source_.queue_[1].push(&task));
  }
  make_obsolete(); // Pending tasks protect the source even after it is idle.
  EXPECT_EQ(OB_EAGAIN, mgr_->delete_tenant_(GetParam()));
  ObLink *link = nullptr;
  EXPECT_EQ(OB_SUCCESS, source_->gts_source_.queue_[1].queue_.pop(link));
  EXPECT_EQ(static_cast<ObLink *>(&task), link);
  EXPECT_EQ(0, task.callbacks_);
  EXPECT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam()));
  source_ = nullptr;
  ObTsSourceInfoGuard guard;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, mgr_->get_ts_source_info_opt_(GetParam(), guard, false, false));
}

TEST_P(TestGtsSourceLifecycle, active_lookup_defers_deletion_without_blocking)
{
  make_obsolete();
  {
    ObTsSourceInfoGuard guard;
    ASSERT_EQ(OB_SUCCESS, mgr_->get_ts_source_info_opt_(GetParam(), guard, false, false));
    int delete_ret = OB_SUCCESS;
    std::thread deleter([&] { delete_ret = mgr_->delete_tenant_(GetParam()); });
    deleter.join(); // Must finish while the reader is still held.
    EXPECT_EQ(OB_EAGAIN, delete_ret);
    EXPECT_EQ(source_, guard.get_ts_source_info());
  }
  EXPECT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam()));
  source_ = nullptr;
}

TEST_P(TestGtsSourceLifecycle, extra_reference_defers_deletion)
{
  // Populate the cache when the tenant is in its range.
  {
    ObTsSourceInfoGuard guard;
    ASSERT_EQ(OB_SUCCESS, mgr_->get_ts_source_info_opt_(GetParam(), guard, false, false));
  }
  ObTsSourceInfo *held = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr_->ts_source_info_map_.get(ObTsSourceInfoKey(GetParam()), held));
  EXPECT_EQ(OB_EAGAIN, mgr_->delete_tenant_(GetParam()));
  EXPECT_EQ(1, mgr_->ts_source_info_map_.count());
  mgr_->ts_source_info_map_.revert(held);
  EXPECT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam()));
  source_ = nullptr;
  EXPECT_EQ(0, mgr_->ts_source_info_map_.count());
}

TEST_P(TestGtsSourceLifecycle, deleted_source_is_recreated_on_lookup)
{
  mgr_->server_.set_ip_addr("127.0.0.1", 2882);
  mgr_->location_adapter_ = &mgr_->location_adapter_def_;
  mgr_->gts_request_rpc_ = ObGtsRequestRpcFactory::alloc();
  ASSERT_NE(nullptr, mgr_->gts_request_rpc_);
  for (int i = 0; i < 8; ++i) {
    {
      ObTsSourceInfoGuard guard;
      ASSERT_EQ(OB_SUCCESS, mgr_->get_ts_source_info_opt_(GetParam(), guard, true, true));
      ASSERT_EQ(source_, guard.get_ts_source_info());
    }
    ASSERT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam()));
    source_ = nullptr;
    {
      ObTsSourceInfoGuard guard;
      EXPECT_EQ(OB_ENTRY_NOT_EXIST, mgr_->get_ts_source_info_opt_(GetParam(), guard, false, false));
    }
    {
      ObTsSourceInfoGuard guard;
      ASSERT_EQ(OB_SUCCESS, mgr_->get_ts_source_info_opt_(GetParam(), guard, true, true));
      source_ = guard.get_ts_source_info();
      ASSERT_NE(nullptr, source_);
      EXPECT_EQ(GetParam(), source_->get_tenant_id());
      EXPECT_EQ(0, source_->gts_source_.get_task_count());
    }
  }
}

TEST_P(TestGtsSourceLifecycle, concurrent_delete_and_create_lookup)
{
  mgr_->server_.set_ip_addr("127.0.0.1", 2882);
  mgr_->location_adapter_ = &mgr_->location_adapter_def_;
  mgr_->gts_request_rpc_ = ObGtsRequestRpcFactory::alloc();
  ASSERT_NE(nullptr, mgr_->gts_request_rpc_);
  // The deletion thread may free the fixture's initial source.
  source_ = nullptr;
  const uint64_t tenant_id = GetParam();
  std::atomic<bool> start(false);
  std::thread deleter([&] {
    while (!start.load()) { std::this_thread::yield(); }
    for (int i = 0; i < 128; ++i) {
      const int ret = mgr_->delete_tenant_(tenant_id);
      EXPECT_TRUE(OB_SUCCESS == ret || OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret);
      std::this_thread::yield();
    }
  });
  start.store(true);
  for (int i = 0; i < 128; ++i) {
    ObTsSourceInfoGuard guard;
    // Deletion must never surface a transient not-found/task-queued result.
    const int ret = mgr_->get_ts_source_info_opt_(tenant_id, guard, true, true);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (OB_SUCCESS == ret) {
      EXPECT_EQ(tenant_id, guard.get_ts_source_info()->get_tenant_id());
      EXPECT_TRUE(guard.get_ts_source_info()->is_inited_);
    }
    std::this_thread::yield();
  }
  deleter.join();
  ObTsSourceInfoGuard guard;
  ASSERT_EQ(OB_SUCCESS, mgr_->get_ts_source_info_opt_(tenant_id, guard, true, true));
  source_ = guard.get_ts_source_info();
}

TEST_P(TestGtsSourceLifecycle, traversal_reference_defers_deletion)
{
  auto visitor = [&](ObTsSourceInfo *info) {
    EXPECT_EQ(source_, info);
    EXPECT_EQ(OB_EAGAIN, mgr_->delete_tenant_(GetParam()));
    return true;
  };
  ASSERT_EQ(OB_SUCCESS, mgr_->ts_source_info_map_.for_each(visitor));
  EXPECT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam()));
  source_ = nullptr;
}

TEST_P(TestGtsSourceLifecycle, nonempty_source_refuses_deletion)
{
  CountingTsTask task(GetParam());
  ASSERT_EQ(OB_SUCCESS, source_->gts_source_.queue_[1].push(&task));
  EXPECT_TRUE(source_->is_inited_);
  EXPECT_TRUE(source_->gts_source_.is_inited_);
  EXPECT_TRUE(source_->gts_source_.queue_[1].is_inited_);
  EXPECT_EQ(1, source_->gts_source_.get_task_count());
  // Explicit deletion (also used at shutdown) must not bypass the task check.
  EXPECT_EQ(OB_EAGAIN, mgr_->delete_tenant_(GetParam()));
  EXPECT_EQ(0, task.callbacks_);
  ObLink *link = nullptr;
  EXPECT_EQ(OB_SUCCESS, source_->gts_source_.queue_[1].queue_.pop(link));
  EXPECT_EQ(static_cast<ObLink *>(&task), link);
}

TEST_P(TestGtsSourceLifecycle, destroy_defense_does_not_skip_cleanup)
{
  CountingTsTask task(GetParam());
  ASSERT_EQ(OB_SUCCESS, source_->gts_source_.queue_[1].push(&task));
  // Deliberately violate the pre-destroy invariant. Internal checks only log;
  // returning from void destroy() cannot prevent C++ member destruction.
  source_->destroy();
  EXPECT_FALSE(source_->is_inited_);
  EXPECT_FALSE(source_->gts_source_.is_inited_);
  EXPECT_FALSE(source_->gts_source_.queue_[0].is_inited_);
  EXPECT_FALSE(source_->gts_source_.queue_[1].is_inited_);
  EXPECT_EQ(0, task.callbacks_);
  // Do not leave the stack task linked when the fixture releases its source.
  ObLink *link = nullptr;
  EXPECT_EQ(OB_SUCCESS, source_->gts_source_.queue_[1].queue_.pop(link));
  EXPECT_EQ(static_cast<ObLink *>(&task), link);
}

TEST_P(TestGtsSourceLifecycle, popped_task_protects_source_until_callback_returns)
{
  class BlockingTask : public CountingTsTask
  {
  public:
    explicit BlockingTask(uint64_t tenant_id) : CountingTsTask(tenant_id) {}
    int gts_elapse_callback(MonotonicTs, const share::SCN &) override
    {
      entered_.set_value();
      resume_.get_future().wait();
      return OB_EAGAIN; // Still waiting: must return to the same source.
    }
    std::promise<void> entered_;
    std::promise<void> resume_;
  } task(GetParam());
  ASSERT_EQ(OB_SUCCESS, source_->gts_source_.queue_[1].push(&task));
  make_obsolete();
  auto entered = task.entered_.get_future();
  std::thread worker([&] {
    share::ObTenantBase tenant(GetParam());
    MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
    if (OB_SUCCESS == tenant_guard.switch_to(&tenant)) {
      ObTsSourceInfoGuard guard;
      if (OB_SUCCESS == mgr_->get_ts_source_info_opt_(GetParam(), guard, false, false)) {
        EXPECT_EQ(OB_EAGAIN, source_->gts_source_.queue_[1].foreach_task(
            MonotonicTs(100), 100, MonotonicTs(100)));
      }
    }
  });
  const bool callback_started = std::future_status::ready == entered.wait_for(std::chrono::seconds(10));
  EXPECT_TRUE(callback_started);
  if (callback_started) {
    EXPECT_EQ(0, source_->gts_source_.get_task_count());
    EXPECT_EQ(OB_EAGAIN, mgr_->delete_tenant_(GetParam()));
  }
  task.resume_.set_value();
  worker.join();
  EXPECT_EQ(1, source_->gts_source_.get_task_count());
  EXPECT_EQ(OB_EAGAIN, mgr_->delete_tenant_(GetParam()));
  ObLink *link = nullptr;
  EXPECT_EQ(OB_SUCCESS, source_->gts_source_.queue_[1].queue_.pop(link));
  EXPECT_EQ(static_cast<ObLink *>(&task), link);
}

// Cover both the cached path and the previously unprotected hashmap-only path.
INSTANTIATE_TEST_CASE_P(CachedAndUncached, TestGtsSourceLifecycle, ::testing::Values(1001UL, 5001UL));

TEST_P(TestGtsSourceLifecycle, sync_request_for_nonlocal_tenant_completes)
{
  prepare_sync_request();
  const uint64_t caller_tenant_id = 1004;
  share::ObTenantBase caller_tenant(caller_tenant_id);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(caller_guard);
  ASSERT_EQ(OB_SUCCESS, caller_guard.switch_to(&caller_tenant));
  share::SCN result;
  auto waiter = std::async(std::launch::async, [&] {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(waiter_guard);
    const int ret = waiter_guard.switch_to(&caller_tenant);
    if (OB_SUCCESS != ret) {
      return ret;
    }
    MonotonicTs received;
    return mgr_->get_gts_sync(GetParam(), MonotonicTs(100), 5 * 1000 * 1000, result, received);
  });
  auto &queue = source_->gts_source_.queue_[0];
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (queue.get_task_count() == 0 && std::chrono::steady_clock::now() < deadline
      && waiter.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_EQ(1, queue.get_task_count());
  // Only the caller's tenant is available in this test. Both the requested
  // tenant and sys must be rejected as callback contexts.
  ObLink *link = nullptr;
  (void)queue.queue_.pop(link);
  EXPECT_NE(nullptr, link);
  if (link != nullptr) {
    auto *task = static_cast<ObTsCbTask *>(link);
    EXPECT_EQ(caller_tenant_id, task->get_tenant_id());
    EXPECT_EQ(OB_SUCCESS, queue.push(task));
  }
  EXPECT_EQ(GetParam(), source_->get_tenant_id());
  EXPECT_EQ(OB_SUCCESS, queue.foreach_task(MonotonicTs(100), 12345, MonotonicTs(100)));
  EXPECT_EQ(OB_SUCCESS, waiter.get());
  EXPECT_EQ(12345, result.get_val_for_tx());
  EXPECT_EQ(0, queue.get_task_count());
  // Also clean up on the unfixed code, where the task stays queued until timeout.
  link = nullptr;
  (void)queue.queue_.pop(link);
  if (link != nullptr) {
    static_cast<ObTsCbTask *>(link)->get_gts_callback(MonotonicTs(100), result, MonotonicTs(100));
  }
}

TEST_P(TestGtsSourceLifecycle, nonlocal_sync_timeout_is_recycled_by_late_response)
{
  prepare_sync_request();
  const uint64_t caller_tenant_id = 1004;
  share::ObTenantBase caller_tenant(caller_tenant_id);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(caller_guard);
  ASSERT_EQ(OB_SUCCESS, caller_guard.switch_to(&caller_tenant));
  share::SCN result;
  MonotonicTs received;
  EXPECT_EQ(OB_TIMEOUT, mgr_->get_gts_sync(GetParam(), MonotonicTs(100), 1000, result, received));
  auto &queue = source_->gts_source_.queue_[0];
  ObLink *link = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.queue_.pop(link));
  ASSERT_NE(nullptr, link);
  auto *task = static_cast<ObTsSyncGetTsCbTask *>(link);
  EXPECT_EQ(caller_tenant_id, task->get_tenant_id());
  EXPECT_EQ(GetParam(), source_->get_tenant_id());
  EXPECT_TRUE(task->is_occupied_);
  EXPECT_TRUE(task->is_early_exit_);
  ASSERT_EQ(OB_SUCCESS, queue.push(task));
  EXPECT_EQ(OB_SUCCESS, queue.foreach_task(MonotonicTs(100), 12345, MonotonicTs(100)));
  EXPECT_FALSE(task->is_occupied_);
  EXPECT_EQ(0, queue.get_task_count());
  // Keep a failing run from contaminating the pool or fixture teardown.
  link = nullptr;
  (void)queue.queue_.pop(link);
  if (link != nullptr) {
    static_cast<ObTsCbTask *>(link)->get_gts_callback(MonotonicTs(100), result, MonotonicTs(100));
  }
  make_obsolete();
  EXPECT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam(), true));
  source_ = nullptr;
}

TEST_P(TestGtsSourceLifecycle, exited_caller_is_recycled_by_stale_response_or_ls_offline)
{
  prepare_sync_request();
  share::ObTenantBase caller_tenant(1004);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(caller_guard);
  ASSERT_EQ(OB_SUCCESS, caller_guard.switch_to(&caller_tenant));
  auto &queue = source_->gts_source_.queue_[0];
  for (bool interrupt : {false, true}) {
    SCOPED_TRACE(interrupt);
    share::SCN result;
    MonotonicTs received;
    EXPECT_EQ(OB_TIMEOUT, mgr_->get_gts_sync(GetParam(), MonotonicTs(100), 1000, result, received));
    ObLink *link = nullptr;
    ASSERT_EQ(OB_SUCCESS, queue.queue_.pop(link));
    ASSERT_NE(nullptr, link);
    auto *task = static_cast<ObTsSyncGetTsCbTask *>(link);
    EXPECT_EQ(1004, task->get_tenant_id());
    EXPECT_TRUE(task->is_early_exit_);
    EXPECT_TRUE(task->is_occupied_);
    ASSERT_EQ(OB_SUCCESS, queue.push(task));

    // The caller tenant remains available, so cleanup must happen inside the
    // normal callback even if the response is stale or the event is LS offline.
    const int ret = interrupt
        ? queue.gts_callback_interrupted(OB_LS_OFFLINE, share::ObLSID(1001))
        : queue.foreach_task(MonotonicTs(99), 12345, MonotonicTs(100));
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, queue.get_task_count());
    EXPECT_FALSE(task->is_occupied_);

    // Clean up on the unfixed code too, without contaminating the next case.
    link = nullptr;
    (void)queue.queue_.pop(link);
    if (link != nullptr) {
      result.convert_for_gts(12345);
      static_cast<ObTsCbTask *>(link)->get_gts_callback(MonotonicTs(100), result, MonotonicTs(100));
    }
  }
  make_obsolete();
  EXPECT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam(), true));
  source_ = nullptr;
}

TEST_P(TestGtsSourceLifecycle, departed_caller_timeout_is_recycled_without_tenant)
{
  prepare_sync_request();
  auto &pool = ObTsSyncGetTsCbTaskPool::get_instance();
  auto &queue = source_->gts_source_.queue_[0];
  // A current response, an older response, tenant stop and LS offline must all
  // be able to discard a request whose waiter has already exited.
  for (int event = 0; event < 4; ++event) {
    ObTsSyncGetTsCbTask *task = nullptr;
    {
      share::ObTenantBase caller_tenant(1004);
      MAKE_TENANT_SWITCH_SCOPE_GUARD(caller_guard);
      ASSERT_EQ(OB_SUCCESS, caller_guard.switch_to(&caller_tenant));
      share::SCN result;
      MonotonicTs received;
      EXPECT_EQ(OB_TIMEOUT, mgr_->get_gts_sync(GetParam(), MonotonicTs(100), 1000, result, received));
      ObLink *link = nullptr;
      ASSERT_EQ(OB_SUCCESS, queue.queue_.pop(link));
      ASSERT_NE(nullptr, link);
      task = static_cast<ObTsSyncGetTsCbTask *>(link);
      EXPECT_EQ(1004, task->get_tenant_id());
      EXPECT_TRUE(task->is_early_exit_);
      EXPECT_TRUE(task->is_occupied_);
      ASSERT_EQ(OB_SUCCESS, queue.push(task));
    } // Destroy the caller's local context before processing its queued task.
    {
      MAKE_TENANT_SWITCH_SCOPE_GUARD(absent_guard);
      ASSERT_NE(OB_SUCCESS, absent_guard.switch_to(1004));
    }
    auto process = [&] {
      return event < 2
          ? queue.foreach_task(MonotonicTs(event == 0 ? 100 : 99), 12345, MonotonicTs(100))
          : queue.gts_callback_interrupted(event == 2 ? OB_TENANT_NOT_EXIST : OB_LS_OFFLINE,
                                          share::ObLSID(1001));
    };
    EXPECT_EQ(OB_SUCCESS, process()) << "event=" << event;
    EXPECT_EQ(0, queue.get_task_count());
    EXPECT_FALSE(task->is_occupied_);
    // Clean up on the unfixed code too, so one failure does not contaminate later cases.
    ObLink *link = nullptr;
    (void)queue.queue_.pop(link);
    if (link != nullptr) {
      share::SCN result;
      result.convert_for_gts(12345);
      static_cast<ObTsCbTask *>(link)->get_gts_callback(MonotonicTs(100), result, MonotonicTs(100));
    }
    // Reuse the same pool slot. A repeated event must not recycle this new request.
    ObTsSyncGetTsCbTask *reused = nullptr;
    ASSERT_EQ(OB_SUCCESS, pool.get_task(MonotonicTs(200), 1004, reused));
    EXPECT_EQ(task, reused);
    EXPECT_EQ(OB_SUCCESS, process());
    EXPECT_TRUE(reused->is_occupied_);
    EXPECT_EQ(OB_SUCCESS, pool.recycle_task(reused));
  }
  make_obsolete();
  EXPECT_EQ(OB_SUCCESS, mgr_->delete_tenant_(GetParam(), true));
  source_ = nullptr;
}

TEST(TestGtsTaskQueue, timeout_during_queue_processing_is_recycled_after_switch_failure)
{
  auto &pool = ObTsSyncGetTsCbTaskPool::get_instance();
  if (!pool.is_inited_) {
    ASSERT_EQ(OB_SUCCESS, pool.init());
  }
  class TimeoutOnTenantLookupTask : public ObTsSyncGetTsCbTask
  {
  public:
    uint64_t get_tenant_id() const override
    {
      // Force wait() to exit after the task is dequeued, but before switching
      // to the now-unavailable caller. Exercise both queue entrances.
      share::SCN result;
      bool recycle = true;
      EXPECT_EQ(OB_TIMEOUT, const_cast<TimeoutOnTenantLookupTask *>(this)->wait(1, result, recycle));
      EXPECT_FALSE(recycle);
      return ObTsSyncGetTsCbTask::get_tenant_id();
    }
  };
  for (bool interrupt : {false, true}) {
    TimeoutOnTenantLookupTask task;
    ASSERT_EQ(OB_SUCCESS, task.init(0));
    ASSERT_EQ(OB_SUCCESS, task.config(MonotonicTs(100), 1004));
    task.is_occupied_ = true;
    ObGTSTaskQueue queue;
    ASSERT_EQ(OB_SUCCESS, queue.init(GET_GTS));
    ASSERT_EQ(OB_SUCCESS, queue.push(&task));
    const int ret = interrupt
        ? queue.gts_callback_interrupted(OB_TENANT_NOT_EXIST, share::ObLSID(1001))
        : queue.foreach_task(MonotonicTs(100), 12345, MonotonicTs(100));
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_TRUE(task.is_early_exit_);
    EXPECT_FALSE(task.is_occupied_);
    EXPECT_EQ(0, queue.get_task_count());
    ObLink *link = nullptr;
    (void)queue.queue_.pop(link);
  }
}

TEST(TestGtsTaskQueue, waiting_sync_task_still_requires_caller_and_fresh_response)
{
  auto &pool = ObTsSyncGetTsCbTaskPool::get_instance();
  if (!pool.is_inited_) {
    ASSERT_EQ(OB_SUCCESS, pool.init());
  }
  ObTsSyncGetTsCbTask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, pool.get_task(MonotonicTs(100), 1004, task));
  ObGTSTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, queue.init(GET_GTS));
  ASSERT_EQ(OB_SUCCESS, queue.push(task));
  EXPECT_NE(OB_SUCCESS, queue.foreach_task(MonotonicTs(100), 12345, MonotonicTs(100)));
  EXPECT_EQ(1, queue.get_task_count());
  EXPECT_TRUE(task->is_occupied_);
  EXPECT_FALSE(task->is_finished_);
  share::ObTenantBase caller_tenant(1004);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(caller_guard);
  ASSERT_EQ(OB_SUCCESS, caller_guard.switch_to(&caller_tenant));
  EXPECT_EQ(OB_EAGAIN, queue.foreach_task(MonotonicTs(99), 12345, MonotonicTs(100)));
  EXPECT_EQ(OB_SUCCESS, queue.gts_callback_interrupted(OB_LS_OFFLINE, share::ObLSID(1001)));
  EXPECT_EQ(1, queue.get_task_count());
  EXPECT_FALSE(task->is_finished_);
  EXPECT_EQ(OB_SUCCESS, queue.foreach_task(MonotonicTs(100), 12345, MonotonicTs(100)));
  EXPECT_EQ(0, queue.get_task_count());
  share::SCN result;
  bool recycle = false;
  EXPECT_EQ(OB_SUCCESS, task->wait(1000, result, recycle));
  EXPECT_TRUE(recycle);
  EXPECT_EQ(12345, result.get_val_for_tx());
  EXPECT_EQ(OB_SUCCESS, pool.recycle_task(task));
}

TEST(TestGtsTaskQueue, tenant_switch_failure_preserves_task_for_retry)
{
  for (auto type : {GET_GTS, WAIT_GTS_ELAPSING}) {
    for (bool interrupt : {false, true}) {
      ObGTSTaskQueue queue;
      ASSERT_EQ(OB_SUCCESS, queue.init(type));
      // Use an invalid ID distinct from foreach_task's initial last_tenant_id (0),
      // so both callback paths attempt switch_to and fail before running a callback.
      CountingTsTask task(OB_INVALID_ID);
      ASSERT_EQ(OB_SUCCESS, queue.push(&task));
      int ret = interrupt
          ? queue.gts_callback_interrupted(OB_LS_OFFLINE, share::ObLSID(1001))
          : queue.foreach_task(MonotonicTs(100), 100, MonotonicTs(100));
      EXPECT_NE(OB_SUCCESS, ret);
      EXPECT_EQ(1, queue.get_task_count());
      EXPECT_EQ(0, task.callbacks_);

      share::ObTenantBase tenant(1001);
      MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
      ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(&tenant));
      task.tenant_id_ = 1001;
      EXPECT_EQ(OB_SUCCESS, queue.gts_callback_interrupted(OB_LS_OFFLINE, share::ObLSID(1001)));
      EXPECT_EQ(0, queue.get_task_count());
      EXPECT_EQ(1, task.callbacks_);
      EXPECT_EQ(OB_SUCCESS, queue.gts_callback_interrupted(OB_LS_OFFLINE, share::ObLSID(1001)));
      EXPECT_EQ(1, task.callbacks_);
    }
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_gts_source_lifecycle.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
