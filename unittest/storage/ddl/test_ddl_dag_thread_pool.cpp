/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <array>
#include <chrono>
#include <condition_variable>
#include <dlfcn.h>
#include <mutex>
#include <time.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include "storage/ddl/ob_ddl_dag_thread_pool.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace
{
std::mutex gate_mutex;
std::condition_variable gate_cond;
lib::Threads *tested_pool = nullptr;
bool fail_second_worker = false;
bool gate_reached = false;
bool release_gate = false;
bool last_start_released = false;
bool gate_timed_out = false;
int start_calls = 0;
int sleep_calls = 0;
int sleep_calls_before_start_return = 0;
int joined_workers = 0;
int process_calls = 0;
int premature_process_calls = 0;
thread_local bool test_worker = false;
thread_local bool test_joiner = false;
std::array<pthread_t *, 2> native_worker_handles{{nullptr, nullptr}};

class TestDDLDag : public ObDDLIndependentDag
{
public:
  bool is_scan_finished() override { return false; }
  lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
};

class TestDDLDagThreadPool : public ::testing::Test
{
protected:
  void check_start(const bool fail)
  {
    TestDDLDag dag;
    sql::ObSQLSessionInfo session;
    ObDDLDagThreadPool pool;
    tested_pool = &pool;
    fail_second_worker = fail;
    gate_reached = release_gate = last_start_released = gate_timed_out = false;
    sleep_calls = sleep_calls_before_start_return = 0;
    start_calls = joined_workers = process_calls = premature_process_calls = 0;
    native_worker_handles.fill(nullptr);
    ASSERT_EQ(OB_SUCCESS, pool.init(2, &dag, &session));
    test_joiner = true;

    const auto begin = std::chrono::steady_clock::now();
    const int ret = pool.start();
    // Real rollback must already have joined worker 0 when start() fails.
    const int rollback_joins = joined_workers;
    if (!fail) {
      pool.wait();
    }
    EXPECT_EQ(fail ? OB_ALLOCATE_MEMORY_FAILED : OB_SUCCESS, ret);
    EXPECT_EQ(2, start_calls);
    EXPECT_TRUE(gate_reached);
    EXPECT_FALSE(gate_timed_out);
    if (!fail) {
      EXPECT_GE(sleep_calls_before_start_return, 2);
    }
    EXPECT_EQ(0, premature_process_calls);
    EXPECT_EQ(fail ? 0 : 2, process_calls);
    EXPECT_EQ(fail ? 1 : 0, rollback_joins);
    EXPECT_EQ(fail ? 1 : 2, joined_workers);
    EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(5));
    EXPECT_EQ(fail, dag.has_set_stop());
    EXPECT_EQ(fail ? OB_ALLOCATE_MEMORY_FAILED : OB_SUCCESS, dag.get_dag_ret());
    pool.stop();
    pool.wait();
    pool.destroy();
    if (fail) {
      dag.simply_set_stop();
      EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, dag.get_dag_ret());
    }
    test_joiner = false;
    tested_pool = nullptr;
  }
};
} // namespace

// Test-only native worker substitute. Threads::start/stop/wait and the DDL pool
// are compiled unchanged; use pthreads to control failure without allocating OB stacks.
int lib::Thread::start()
{
  const bool last_worker = threads_ == tested_pool && ++start_calls == 2;
  if (last_worker) {
    std::unique_lock<std::mutex> guard(gate_mutex);
    gate_timed_out = !gate_cond.wait_for(guard, std::chrono::seconds(3),
                                       [] { return gate_reached || process_calls > 0; });
    release_gate = true;
    gate_cond.notify_all();
    if (fail_second_worker) {
      return OB_ALLOCATE_MEMORY_FAILED;
    }
    // Only worker 0 exists here. Let its first sleep finish, but hold the
    // last start() until worker 0 checks the barrier again or enters process().
    gate_timed_out |= !gate_cond.wait_for(guard, std::chrono::seconds(3),
                                        [] { return sleep_calls >= 2 || process_calls > 0; });
    sleep_calls_before_start_return = sleep_calls;
  }
  const int ret = pthread_create(&pth_, nullptr, [](void *arg) -> void * {
    auto *thread = static_cast<lib::Thread *>(arg);
    current_thread_ = thread;
    test_worker = thread->threads_ == tested_pool;
    thread->run();
    return nullptr;
  }, this);
  if (ret == 0 && threads_ == tested_pool) {
    native_worker_handles[start_calls - 1] = &pth_;
  }
  if (last_worker) {
    std::lock_guard<std::mutex> guard(gate_mutex);
    last_start_released = true;
    gate_cond.notify_all();
  }
  return ret == 0 ? OB_SUCCESS : OB_ERR_SYS;
}

// Preserve join interception when ThreadSanitizer is linked statically.
extern "C" int __interceptor_pthread_join(pthread_t, void **) __attribute__((weak));

// Release builds may inline Thread::wait()/destroy(). Clear each native handle
// at the actual join so the real cleanup cannot try to join it a second time.
extern "C" int pthread_join(pthread_t thread, void **retval)
{
  static auto real_join = __interceptor_pthread_join != nullptr
      ? __interceptor_pthread_join
      : reinterpret_cast<int (*)(pthread_t, void **)>(dlsym(RTLD_NEXT, "pthread_join"));
  const int ret = real_join(thread, retval);
  if (ret == 0 && test_joiner) {
    for (auto &handle : native_worker_handles) {
      if (handle != nullptr && pthread_equal(*handle, thread)) {
        *handle = 0;
        handle = nullptr;
        ++joined_workers;
        break;
      }
    }
  }
  return ret;
}

extern "C" int usleep(useconds_t usec)
{
  if (test_worker && usec == 1000) {
    std::unique_lock<std::mutex> guard(gate_mutex);
    ++sleep_calls;
    gate_reached = true;
    gate_cond.notify_all();
    gate_timed_out |= !gate_cond.wait_for(guard, std::chrono::seconds(3),
                                        [] { return release_gate; });
  }
  const timespec duration = {static_cast<time_t>(usec / 1000000),
                             static_cast<long>(usec % 1000000) * 1000};
  return nanosleep(&duration, nullptr);
}

int share::ObIndependentDag::process()
{
  std::lock_guard<std::mutex> guard(gate_mutex);
  ++process_calls;
  if (!last_start_released) {
    ++premature_process_calls;
  }
  gate_cond.notify_all();
  return OB_SUCCESS;
}

TEST_F(TestDDLDagThreadPool, second_worker_start_failure)
{
  check_start(true);
}

TEST_F(TestDDLDagThreadPool, all_workers_start_successfully)
{
  check_start(false);
}

int main(int argc, char **argv)
{
  alarm(15);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
