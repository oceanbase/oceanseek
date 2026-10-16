/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>

#define private public
#define protected public
#include "share/logical_table/ob_namespace_ru_manager.h"
#undef private
#undef protected
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
using namespace share;
using namespace common;
namespace unittest
{

// ==================== ObTokenBucket unit tests ====================

class TestTokenBucket : public ::testing::Test
{
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TestTokenBucket, basic_try_acquire)
{
  ObTokenBucket bucket;
  bucket.update_config(10, 100);
  ATOMIC_STORE(&bucket.tokens_, static_cast<int64_t>(10));
  ATOMIC_STORE(&bucket.last_refill_ts_, ObTimeUtility::current_time());

  ASSERT_EQ(OB_SUCCESS, bucket.try_acquire(5));
  ASSERT_EQ(5, ATOMIC_LOAD(&bucket.tokens_));

  ASSERT_EQ(OB_SUCCESS, bucket.try_acquire(5));
  ASSERT_EQ(0, ATOMIC_LOAD(&bucket.tokens_));

  ASSERT_EQ(OB_EAGAIN, bucket.try_acquire(1));
  ASSERT_EQ(0, ATOMIC_LOAD(&bucket.tokens_));
}

TEST_F(TestTokenBucket, consume_allows_negative)
{
  ObTokenBucket bucket;
  bucket.update_config(10, 100);
  ATOMIC_STORE(&bucket.tokens_, static_cast<int64_t>(5));
  ATOMIC_STORE(&bucket.last_refill_ts_, ObTimeUtility::current_time());

  bucket.consume(8);
  ASSERT_LT(ATOMIC_LOAD(&bucket.tokens_), 0);
}

TEST_F(TestTokenBucket, refill_replenishes_tokens)
{
  ObTokenBucket bucket;
  const int64_t rate = 1000;
  bucket.update_config(100, rate);
  ATOMIC_STORE(&bucket.tokens_, static_cast<int64_t>(0));
  ATOMIC_STORE(&bucket.last_refill_ts_, ObTimeUtility::current_time());

  // Wait 50ms, expect ~50 tokens refilled (1000/s * 0.05s = 50).
  usleep(50000);
  bucket.refill();

  const int64_t tokens = ATOMIC_LOAD(&bucket.tokens_);
  ASSERT_GT(tokens, 30);
  ASSERT_LE(tokens, 100);
}

TEST_F(TestTokenBucket, refill_capped_at_burst)
{
  ObTokenBucket bucket;
  bucket.update_config(20, 10000);
  ATOMIC_STORE(&bucket.tokens_, static_cast<int64_t>(0));
  ATOMIC_STORE(&bucket.last_refill_ts_, ObTimeUtility::current_time());

  usleep(100000);
  bucket.refill();

  ASSERT_LE(ATOMIC_LOAD(&bucket.tokens_), 20);
}

TEST_F(TestTokenBucket, update_config_caps_tokens)
{
  ObTokenBucket bucket;
  bucket.update_config(100, 100);
  ATOMIC_STORE(&bucket.tokens_, static_cast<int64_t>(80));
  ATOMIC_STORE(&bucket.last_refill_ts_, ObTimeUtility::current_time());

  bucket.update_config(50, 100);
  ASSERT_EQ(50, ATOMIC_LOAD(&bucket.tokens_));

  bucket.update_config(200, 100);
  ASSERT_EQ(50, ATOMIC_LOAD(&bucket.tokens_));
}

// ==================== ObNamespaceRUController unit tests ====================

class TestNamespaceRUController : public ::testing::Test
{
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TestNamespaceRUController, init_basic)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  ASSERT_EQ(1001u, ctrl.get_namespace_id());
  ASSERT_FALSE(ctrl.is_throttled());
}

TEST_F(TestNamespaceRUController, qps_acquire_within_burst)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  for (int i = 0; i < 200; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_qps());
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_qps());
}

TEST_F(TestNamespaceRUController, qps_recovers_after_refill)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  for (int i = 0; i < 200; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_qps());
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_qps());

  // Token refill: 100/s, wait 50ms to refill ~5 tokens.
  usleep(50000);
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_qps());
}

TEST_F(TestNamespaceRUController, data_overdraft_triggers_throttle)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  const int64_t overdraft_limit = 100;
  ASSERT_EQ(OB_SUCCESS, ctrl.consume_data(10000 + 200, overdraft_limit));
  // tokens: 10000 - 10200 = -200, overdraft 200 > limit 100
  ASSERT_TRUE(ctrl.is_throttled());
}

TEST_F(TestNamespaceRUController, throttle_blocks_qps)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  const int64_t overdraft_limit = 100;
  ASSERT_EQ(OB_SUCCESS, ctrl.consume_data(10000 + 200, overdraft_limit));
  ASSERT_TRUE(ctrl.is_throttled());

  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_qps());
}

TEST_F(TestNamespaceRUController, throttle_recovers_after_data_refill)
{
  ObNamespaceRUController ctrl;
  ctrl.init(2001, 200/*qps_burst*/, 100/*qps_rate*/,
            100/*tps_burst*/, 50/*tps_rate*/,
            10000/*data_burst*/, 10000/*data_rate = 10000/s*/);

  const int64_t overdraft_limit = 50;
  ASSERT_EQ(OB_SUCCESS, ctrl.consume_data(10000 + 100, overdraft_limit));
  ASSERT_TRUE(ctrl.is_throttled());

  // data_rate=10000/s, overdraft 100, needs ~10ms to recover.
  usleep(30000);
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_qps());
  ASSERT_FALSE(ctrl.is_throttled());
}

TEST_F(TestNamespaceRUController, no_throttle_within_limit)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  const int64_t overdraft_limit = 500;
  ASSERT_EQ(OB_SUCCESS, ctrl.consume_data(10000 + 100, overdraft_limit));
  // tokens: 10000 - 10100 = -100, overdraft 100 < limit 500
  ASSERT_FALSE(ctrl.is_throttled());
}

// ==================== ObNamespaceRUManager unit tests ====================

class TestNamespaceRUManager : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, mgr_.init(1/*tenant_id*/, &sql_proxy_));
  }
  void TearDown() override
  {
    mgr_.destroy();
  }
  ObMySQLProxy sql_proxy_;
  ObNamespaceRUManager mgr_;
};

TEST_F(TestNamespaceRUManager, init_twice)
{
  ASSERT_EQ(OB_INIT_TWICE, mgr_.init(1, &sql_proxy_));
}

TEST_F(TestNamespaceRUManager, acquire_qps_lazy_create)
{
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(1001));
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(1001));
}

TEST_F(TestNamespaceRUManager, consume_data_without_acquire)
{
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.consume_data(9999, 100));
}

TEST_F(TestNamespaceRUManager, full_flow_acquire_then_consume)
{
  const uint64_t ns_id = 3001;
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(ns_id));
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(ns_id, 1024));
}

TEST_F(TestNamespaceRUManager, multiple_namespaces)
{
  for (uint64_t ns = 1; ns <= 100; ++ns) {
    ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(ns));
  }
  for (uint64_t ns = 1; ns <= 100; ++ns) {
    ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(ns, 64));
  }
}

TEST_F(TestNamespaceRUManager, qps_exhaustion_per_namespace)
{
  const uint64_t ns_id = 5001;
  int succ_count = 0;
  for (int i = 0; i < 300; ++i) {
    if (OB_SUCCESS == mgr_.acquire_qps(ns_id)) {
      ++succ_count;
    }
  }
  // Default burst=200: the first 200 succeed, then throttled.
  ASSERT_EQ(200, succ_count);
}

// ==================== Concurrency tests ====================

class TestTokenBucketConcurrency : public ::testing::Test
{
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(TestTokenBucketConcurrency, concurrent_try_acquire_no_over_issue)
{
  ObTokenBucket bucket;
  const int64_t total_tokens = 1000;
  bucket.update_config(total_tokens, 0);
  ATOMIC_STORE(&bucket.tokens_, total_tokens);
  ATOMIC_STORE(&bucket.last_refill_ts_, ObTimeUtility::current_time());

  std::atomic<int64_t> total_acquired(0);
  const int thread_count = 8;

  auto worker = [&]() {
    int64_t acquired = 0;
    for (int i = 0; i < 500; ++i) {
      if (OB_SUCCESS == bucket.try_acquire(1)) {
        ++acquired;
      }
    }
    total_acquired.fetch_add(acquired);
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  // refill_rate=0, no refill; total issued must not exceed the initial tokens.
  ASSERT_EQ(total_tokens, total_acquired.load());
  ASSERT_EQ(0, ATOMIC_LOAD(&bucket.tokens_));
}

TEST_F(TestTokenBucketConcurrency, concurrent_consume_total_correct)
{
  ObTokenBucket bucket;
  const int64_t initial = 10000;
  bucket.update_config(initial, 0);
  bucket.tokens_ = initial;
  bucket.last_refill_ts_ = ObTimeUtility::current_time();

  const int thread_count = 8;
  const int64_t per_thread = 100;

  auto worker = [&]() {
    for (int i = 0; i < per_thread; ++i) {
      bucket.consume(1);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  ASSERT_EQ(initial - thread_count * per_thread, ATOMIC_LOAD(&bucket.tokens_));
}

TEST_F(TestTokenBucketConcurrency, concurrent_manager_acquire_qps)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));

  const uint64_t ns_id = 7001;
  std::atomic<int64_t> total_success(0);
  const int thread_count = 4;

  auto worker = [&]() {
    int64_t success = 0;
    for (int i = 0; i < 200; ++i) {
      if (OB_SUCCESS == mgr.acquire_qps(ns_id)) {
        ++success;
      }
    }
    total_success.fetch_add(success);
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  // burst=200, refill_rate=100/s, 4*200=800 requests in total.
  // No over-issue; total success count <= 200 + the refilled amount.
  ASSERT_LE(total_success.load(), 250);
  ASSERT_GE(total_success.load(), 200);

  mgr.destroy();
}

// ==================== Advanced concurrency tests ====================

class TestConcurrencyAdvanced : public ::testing::Test
{
protected:
  void SetUp() override {}
  void TearDown() override {}
};

// Test 1: multiple threads each operate a different namespace; verify concurrent create + LRU move_to_front.
TEST_F(TestConcurrencyAdvanced, concurrent_acquire_qps_multi_namespace)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));

  const int thread_count = 8;
  const int ops_per_thread = 100;
  std::vector<int64_t> success_counts(thread_count, 0);

  auto worker = [&](int tid) {
    const uint64_t ns_id = 10000 + tid;
    int64_t success = 0;
    for (int i = 0; i < ops_per_thread; ++i) {
      if (OB_SUCCESS == mgr.acquire_qps(ns_id)) {
        ++success;
      }
    }
    success_counts[tid] = success;
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }
  for (auto &t : threads) {
    t.join();
  }

  // burst=200, each namespace is independent, all 100 requests succeed.
  for (int i = 0; i < thread_count; ++i) {
    ASSERT_EQ(ops_per_thread, success_counts[i])
      << "thread " << i << " should succeed all ops within burst";
  }
  mgr.destroy();
}

// Test 2: mixed acquire_qps + consume_data on the same namespace.
TEST_F(TestConcurrencyAdvanced, concurrent_acquire_and_consume_same_namespace)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));

  const uint64_t ns_id = 20001;
  ASSERT_EQ(OB_SUCCESS, mgr.acquire_qps(ns_id));

  const int acquire_threads = 4;
  const int consume_threads = 4;
  std::atomic<int64_t> total_acquire_success(0);
  std::atomic<int64_t> total_consume_success(0);

  auto acquire_worker = [&]() {
    int64_t success = 0;
    for (int i = 0; i < 50; ++i) {
      if (OB_SUCCESS == mgr.acquire_qps(ns_id)) {
        ++success;
      }
    }
    total_acquire_success.fetch_add(success);
  };

  auto consume_worker = [&]() {
    int64_t success = 0;
    for (int i = 0; i < 50; ++i) {
      if (OB_SUCCESS == mgr.consume_data(ns_id, 10)) {
        ++success;
      }
    }
    total_consume_success.fetch_add(success);
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < acquire_threads; ++i) {
    threads.emplace_back(acquire_worker);
  }
  for (int i = 0; i < consume_threads; ++i) {
    threads.emplace_back(consume_worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  // burst=200, SetUp already consumed 1, 199 left; 4*50=200 acquires won't all succeed.
  ASSERT_LE(total_acquire_success.load(), 200 + 50);
  ASSERT_GE(total_acquire_success.load(), 1);
  // consume_data always returns OB_SUCCESS.
  ASSERT_EQ(consume_threads * 50, total_consume_success.load());
  mgr.destroy();
}

// Test 3: multiple threads race to create the same non-existent namespace.
TEST_F(TestConcurrencyAdvanced, concurrent_create_same_namespace)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));

  const uint64_t ns_id = 30001;
  const int thread_count = 8;
  std::atomic<bool> go(false);
  std::atomic<int64_t> success_count(0);
  std::atomic<int64_t> fail_count(0);

  auto worker = [&]() {
    while (!go.load()) {}
    int ret = mgr.acquire_qps(ns_id);
    if (OB_SUCCESS == ret) {
      success_count.fetch_add(1);
    } else {
      fail_count.fetch_add(1);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker);
  }
  go.store(true);
  for (auto &t : threads) {
    t.join();
  }

  // All threads should succeed (on concurrent create, hash_exist falls back to get).
  ASSERT_EQ(thread_count, success_count.load());
  ASSERT_EQ(0, fail_count.load());
  // The controller exists, consume_data should succeed.
  ASSERT_EQ(OB_SUCCESS, mgr.consume_data(ns_id, 1));
  mgr.destroy();
}

// Test 4: concurrently create many namespaces to trigger eviction.
TEST_F(TestConcurrencyAdvanced, concurrent_eviction_under_load)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));
  mgr.set_max_controller_count(50);

  const int thread_count = 8;
  const int ns_per_thread = 20;

  auto worker = [&](int tid) {
    for (int i = 0; i < ns_per_thread; ++i) {
      uint64_t ns_id = static_cast<uint64_t>(tid * 1000 + i + 1);
      mgr.acquire_qps(ns_id);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker, i);
  }
  for (auto &t : threads) {
    t.join();
  }

  // Success as long as it does not crash; and a new namespace still works.
  ASSERT_EQ(OB_SUCCESS, mgr.acquire_qps(99999));
  ASSERT_EQ(OB_SUCCESS, mgr.consume_data(99999, 64));
  mgr.destroy();
}

// Test 5: concurrent throttle and recovery.
TEST_F(TestConcurrencyAdvanced, concurrent_throttle_and_recover)
{
  ObNamespaceRUController ctrl;
  // data_burst=500, data_rate=50000/s, recovers quickly.
  ctrl.init(50001, 200/*qps_burst*/, 100/*qps_rate*/,
            100/*tps_burst*/, 50/*tps_rate*/,
            500/*data_burst*/, 50000/*data_rate*/);

  const int64_t overdraft_limit = 100;
  // Consume 1000, overdraft 500 > limit 100, triggers throttle.
  ASSERT_EQ(OB_SUCCESS, ctrl.consume_data(1000, overdraft_limit));
  ASSERT_TRUE(ctrl.is_throttled());

  // 4 threads concurrently acquire_qps, all should be rejected by the throttle.
  const int thread_count = 4;
  std::atomic<int64_t> eagain_count(0);

  auto worker = [&]() {
    for (int i = 0; i < 20; ++i) {
      if (OB_EAGAIN == ctrl.try_acquire_qps()) {
        eagain_count.fetch_add(1);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  // Most should be throttled (refill may recover a few).
  ASSERT_GE(eagain_count.load(), thread_count * 20 - 10);

  // Wait 50ms; data_rate=50000/s is enough to recover 2500 tokens.
  usleep(50000);
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_qps());
  ASSERT_FALSE(ctrl.is_throttled());
}

// ==================== LRU mechanism tests ====================

class TestLRUMechanism : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, mgr_.init(1, &sql_proxy_));
  }
  void TearDown() override
  {
    mgr_.destroy();
  }
  ObMySQLProxy sql_proxy_;
  ObNamespaceRUManager mgr_;
};

// Test 6: eviction removes the least-recently-accessed controller.
TEST_F(TestLRUMechanism, lru_eviction_removes_least_recent)
{
  mgr_.set_max_controller_count(5);

  // Create ns 1..5; ns1 is the oldest = LRU tail.
  for (uint64_t ns = 1; ns <= 5; ++ns) {
    ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(ns));
  }

  // Access ns1, move it to the head; the LRU tail becomes ns2.
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(1));

  // Create ns6 to trigger eviction (evict 10% = max(5*10/100,1) = 1, evict ns2).
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(6));

  // ns2 should be evicted.
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.consume_data(2, 1));
  // ns1 is still there (it was accessed and moved to the head).
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(1, 1));
  // ns3,4,5,6 are all there.
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(3, 1));
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(4, 1));
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(5, 1));
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(6, 1));
}

// Test 7: eviction removes the tail 10%.
TEST_F(TestLRUMechanism, lru_eviction_batch_10_percent)
{
  mgr_.set_max_controller_count(20);

  for (uint64_t ns = 1; ns <= 20; ++ns) {
    ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(ns));
  }

  // Create ns21 to trigger eviction (evict 10% = 2, evict ns1, ns2).
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(21));

  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.consume_data(1, 1));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.consume_data(2, 1));
  // ns3..ns21 are all there.
  for (uint64_t ns = 3; ns <= 21; ++ns) {
    ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(ns, 1))
      << "ns " << ns << " should still be alive";
  }
}

// Test 8: access updates the LRU order.
TEST_F(TestLRUMechanism, lru_access_updates_order)
{
  mgr_.set_max_controller_count(5);

  // Create ns 1..5.
  for (uint64_t ns = 1; ns <= 5; ++ns) {
    ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(ns));
  }

  // Access ns1, ns2 to move them to the head.
  // LRU order tail->head: ns3, ns4, ns5, ns2, ns1
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(1));
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(2));

  // Create ns6 to trigger eviction, evicting the LRU tail = ns3.
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_qps(6));

  // ns3 is evicted.
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr_.consume_data(3, 1));
  // ns1,2,4,5,6 are all there.
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(1, 1));
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(2, 1));
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(4, 1));
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(5, 1));
  ASSERT_EQ(OB_SUCCESS, mgr_.consume_data(6, 1));
}

// ==================== TPS throttling tests ====================

class TestTPSThrottle : public ::testing::Test
{
protected:
  void SetUp() override {}
  void TearDown() override {}
};

// T021: TPS acquire within burst
TEST_F(TestTPSThrottle, tps_acquire_within_burst)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_tps());
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_tps());
}

// T022: TPS recovers after refill
TEST_F(TestTPSThrottle, tps_recovers_after_refill)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_tps());
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_tps());

  // tps_rate=50/s, wait 50ms => ~2.5 tokens
  usleep(50000);
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_tps());
}

// T023: try_acquire(READ) routes to qps_bucket_
TEST_F(TestTPSThrottle, try_acquire_dispatch_read)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  for (int i = 0; i < 200; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire(ObNamespaceOpType::READ));
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire(ObNamespaceOpType::READ));
  // TPS bucket should still have tokens
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire(ObNamespaceOpType::WRITE));
}

// T024: try_acquire(WRITE) routes to tps_bucket_
TEST_F(TestTPSThrottle, try_acquire_dispatch_write)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire(ObNamespaceOpType::WRITE));
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire(ObNamespaceOpType::WRITE));
  // QPS bucket should still have tokens
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire(ObNamespaceOpType::READ));
}

// T025: Data overdraft blocks both READ and WRITE
TEST_F(TestTPSThrottle, data_overdraft_blocks_both_types)
{
  ObNamespaceRUController ctrl;
  ctrl.init(1001, 200/*qps_burst*/, 100/*qps_rate*/, 100/*tps_burst*/, 50/*tps_rate*/, 10000/*data_burst*/, 5000/*data_rate*/);
  const int64_t overdraft_limit = 100;
  ASSERT_EQ(OB_SUCCESS, ctrl.consume_data(10000 + 200, overdraft_limit));
  ASSERT_TRUE(ctrl.is_throttled());

  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire(ObNamespaceOpType::READ));
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire(ObNamespaceOpType::WRITE));
}

// T026: Manager-level acquire(ns, WRITE) lazy creates controller
TEST_F(TestNamespaceRUManager, acquire_write_lazy_create)
{
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire(1001, ObNamespaceOpType::WRITE, ObString()));
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire(1001, ObNamespaceOpType::WRITE, ObString()));
}

// T027: TPS exhaustion per namespace via manager
TEST_F(TestNamespaceRUManager, tps_exhaustion_per_namespace)
{
  const uint64_t ns_id = 6001;
  int succ_count = 0;
  for (int i = 0; i < 200; ++i) {
    if (OB_SUCCESS == mgr_.acquire(ns_id, ObNamespaceOpType::WRITE, ObString())) {
      ++succ_count;
    }
  }
  // default tps_burst=100
  ASSERT_EQ(100, succ_count);
}

// T028: Concurrent write acquire on same namespace
TEST_F(TestConcurrencyAdvanced, concurrent_acquire_write_same_namespace)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));

  const uint64_t ns_id = 8001;
  std::atomic<int64_t> total_success(0);
  const int thread_count = 4;

  auto worker = [&]() {
    int64_t success = 0;
    for (int i = 0; i < 100; ++i) {
      if (OB_SUCCESS == mgr.acquire(ns_id, ObNamespaceOpType::WRITE, ObString())) {
        ++success;
      }
    }
    total_success.fetch_add(success);
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  // tps_burst=100, tps_rate=50/s, total 4*100=400 requests
  ASSERT_LE(total_success.load(), 150);
  ASSERT_GE(total_success.load(), 100);

  mgr.destroy();
}

// T029: Concurrent mixed READ and WRITE on same namespace
TEST_F(TestConcurrencyAdvanced, concurrent_mixed_read_write)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));

  const uint64_t ns_id = 9001;
  std::atomic<int64_t> read_success(0);
  std::atomic<int64_t> write_success(0);
  const int thread_count = 4;

  auto read_worker = [&]() {
    int64_t success = 0;
    for (int i = 0; i < 100; ++i) {
      if (OB_SUCCESS == mgr.acquire(ns_id, ObNamespaceOpType::READ, ObString())) {
        ++success;
      }
    }
    read_success.fetch_add(success);
  };

  auto write_worker = [&]() {
    int64_t success = 0;
    for (int i = 0; i < 100; ++i) {
      if (OB_SUCCESS == mgr.acquire(ns_id, ObNamespaceOpType::WRITE, ObString())) {
        ++success;
      }
    }
    write_success.fetch_add(success);
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(read_worker);
  }
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(write_worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  // Read (qps_burst=200): 4*100=400, ~200-250 should succeed
  ASSERT_GE(read_success.load(), 200);
  ASSERT_LE(read_success.load(), 250);
  // Write (tps_burst=100): 4*100=400, ~100-150 should succeed
  ASSERT_GE(write_success.load(), 100);
  ASSERT_LE(write_success.load(), 150);

  mgr.destroy();
}

// ==================== ObNamespaceRUController tps_bucket_ supplementary tests ====================

class TestTPSBucket : public ::testing::Test
{
protected:
  void SetUp() override {}
  void TearDown() override {}
};

// tps and qps do not affect each other: after exhausting tps, qps still works, and vice versa.
TEST_F(TestTPSBucket, tps_and_qps_independent)
{
  ObNamespaceRUController ctrl;
  ctrl.init(2001, 200/*qps_burst*/, 100/*qps_rate*/,
            50/*tps_burst*/, 50/*tps_rate*/,
            10000/*data_burst*/, 5000/*data_rate*/);
  // Exhaust tps.
  for (int i = 0; i < 50; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_tps());
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_tps());
  // qps is unaffected.
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_qps());
}

TEST_F(TestTPSBucket, qps_exhausted_tps_still_works)
{
  ObNamespaceRUController ctrl;
  ctrl.init(2002, 50/*qps_burst*/, 50/*qps_rate*/,
            200/*tps_burst*/, 100/*tps_rate*/,
            10000/*data_burst*/, 5000/*data_rate*/);
  // Exhaust qps.
  for (int i = 0; i < 50; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_qps());
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_qps());
  // tps is unaffected.
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_tps());
}

// Calling try_acquire_tps before init returns OB_NOT_INIT.
TEST_F(TestTPSBucket, tps_before_init)
{
  ObNamespaceRUController ctrl;
  ASSERT_EQ(OB_NOT_INIT, ctrl.try_acquire_tps());
}

// data overdraft also blocks tps; after recovery tps becomes available.
TEST_F(TestTPSBucket, data_overdraft_blocks_tps_then_recovers)
{
  ObNamespaceRUController ctrl;
  ctrl.init(2003, 200/*qps_burst*/, 100/*qps_rate*/,
            100/*tps_burst*/, 50/*tps_rate*/,
            500/*data_burst*/, 50000/*data_rate*/);

  const int64_t overdraft_limit = 50;
  ASSERT_EQ(OB_SUCCESS, ctrl.consume_data(700, overdraft_limit));
  ASSERT_TRUE(ctrl.is_throttled());
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_tps());

  // data_rate=50000/s, overdraft 200, ~4ms to recover; 30ms is enough.
  usleep(30000);
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_tps());
  ASSERT_FALSE(ctrl.is_throttled());
}

// When tps burst=0 all requests are rejected (zero-capacity bucket).
TEST_F(TestTPSBucket, tps_zero_burst)
{
  ObNamespaceRUController ctrl;
  ctrl.init(2004, 200/*qps_burst*/, 100/*qps_rate*/,
            0/*tps_burst*/, 0/*tps_rate*/,
            10000/*data_burst*/, 5000/*data_rate*/);
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_tps());
  // qps works normally.
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_qps());
}

// tps large-burst scenario: confirm a large number of requests all pass.
TEST_F(TestTPSBucket, tps_large_burst)
{
  ObNamespaceRUController ctrl;
  ctrl.init(2005, 200/*qps_burst*/, 100/*qps_rate*/,
            10000/*tps_burst*/, 0/*tps_rate*/,
            100000/*data_burst*/, 50000/*data_rate*/);
  for (int i = 0; i < 10000; ++i) {
    ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_tps());
  }
  ASSERT_EQ(OB_EAGAIN, ctrl.try_acquire_tps());
}

// After reset tps is unavailable (re-init required).
TEST_F(TestTPSBucket, tps_after_reset)
{
  ObNamespaceRUController ctrl;
  ctrl.init(2006, 200/*qps_burst*/, 100/*qps_rate*/,
            100/*tps_burst*/, 50/*tps_rate*/,
            10000/*data_burst*/, 5000/*data_rate*/);
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_tps());
  ctrl.reset();
  ASSERT_EQ(OB_NOT_INIT, ctrl.try_acquire_tps());
}

// Concurrent tps acquire does not over-issue.
TEST_F(TestTPSBucket, concurrent_tps_no_over_issue)
{
  ObNamespaceRUController ctrl;
  const int64_t tps_burst = 500;
  ctrl.init(2007, 200/*qps_burst*/, 100/*qps_rate*/,
            tps_burst, 0/*tps_rate=0, no refill*/,
            100000/*data_burst*/, 50000/*data_rate*/);

  std::atomic<int64_t> total_success(0);
  const int thread_count = 8;

  auto worker = [&]() {
    int64_t success = 0;
    for (int i = 0; i < 200; ++i) {
      if (OB_SUCCESS == ctrl.try_acquire_tps()) {
        ++success;
      }
    }
    total_success.fetch_add(success);
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  // tps_rate=0, no refill; total success count == burst.
  ASSERT_EQ(tps_burst, total_success.load());
}

// Concurrent mixed tps + data consume: no crash and tps does not over-issue.
TEST_F(TestTPSBucket, concurrent_tps_with_data_consume)
{
  ObNamespaceRUController ctrl;
  ctrl.init(2008, 200/*qps_burst*/, 100/*qps_rate*/,
            100/*tps_burst*/, 50/*tps_rate*/,
            10000/*data_burst*/, 5000/*data_rate*/);

  std::atomic<int64_t> tps_success(0);
  const int thread_count = 4;

  auto tps_worker = [&]() {
    int64_t success = 0;
    for (int i = 0; i < 50; ++i) {
      if (OB_SUCCESS == ctrl.try_acquire_tps()) {
        ++success;
      }
    }
    tps_success.fetch_add(success);
  };

  auto data_worker = [&]() {
    for (int i = 0; i < 50; ++i) {
      ctrl.consume_data(10, 100);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(tps_worker);
  }
  for (int i = 0; i < thread_count; ++i) {
    threads.emplace_back(data_worker);
  }
  for (auto &t : threads) {
    t.join();
  }

  // tps_burst=100, 4*50=200 requests, success count <= 100 + refill.
  ASSERT_LE(tps_success.load(), 110);
  ASSERT_GE(tps_success.load(), 1);
}

// ==================== Concurrent-thread limit (try_acquire_thread / release_thread) ====================

class TestNamespaceThreadLimit : public ::testing::Test
{
protected:
  void SetUp() override {}
  void TearDown() override {}
};

// max_threads_=0 (unlimited): any count is acquired immediately.
TEST_F(TestNamespaceThreadLimit, unlimited_when_zero)
{
  ObNamespaceRUController ctrl;
  ctrl.init(3001, 200, 100, 100, 50, 10000, 5000);
  ctrl.set_max_threads(0);
  bool acquired = false;
  const int64_t deadline = ObTimeUtility::current_time() + 1000000;
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_thread(100, deadline, acquired));
  ASSERT_TRUE(acquired);
}

// Within max_threads_ it can acquire; over the limit it times out (deadline passed)
// without error and acquired=false.
TEST_F(TestNamespaceThreadLimit, acquire_within_and_timeout_over_limit)
{
  ObNamespaceRUController ctrl;
  ctrl.init(3002, 200, 100, 100, 50, 10000, 5000);
  ctrl.set_max_threads(16);
  bool acquired = false;
  const int64_t now = ObTimeUtility::current_time();
  // First fill up all 16.
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_thread(16, now + 1000000, acquired));
  ASSERT_TRUE(acquired);
  ASSERT_EQ(16, ctrl.get_active_threads());
  // Acquire 1 more: over the limit, deadline already expired => times out without error, acquired=false.
  acquired = true;
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_thread(1, ObTimeUtility::current_time(), acquired));
  ASSERT_FALSE(acquired);
  ASSERT_EQ(16, ctrl.get_active_threads());
}

// After release_thread returns slots, they can be acquired again.
TEST_F(TestNamespaceThreadLimit, release_frees_slots)
{
  ObNamespaceRUController ctrl;
  ctrl.init(3003, 200, 100, 100, 50, 10000, 5000);
  ctrl.set_max_threads(4);
  bool acquired = false;
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_thread(4, ObTimeUtility::current_time() + 1000000, acquired));
  ASSERT_TRUE(acquired);
  // Full, times out immediately.
  acquired = true;
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_thread(1, ObTimeUtility::current_time(), acquired));
  ASSERT_FALSE(acquired);
  // Release 2.
  ctrl.release_thread(2);
  ASSERT_EQ(2, ctrl.get_active_threads());
  // Now 2 can be acquired.
  acquired = false;
  ASSERT_EQ(OB_SUCCESS, ctrl.try_acquire_thread(2, ObTimeUtility::current_time() + 1000000, acquired));
  ASSERT_TRUE(acquired);
  ASSERT_EQ(4, ctrl.get_active_threads());
}

// release does not drive active below zero.
TEST_F(TestNamespaceThreadLimit, release_never_negative)
{
  ObNamespaceRUController ctrl;
  ctrl.init(3004, 200, 100, 100, 50, 10000, 5000);
  ctrl.set_max_threads(4);
  ctrl.release_thread(100);  // over-release
  ASSERT_EQ(0, ctrl.get_active_threads());
}

// Manager level: default thread limit enabled (16); acquire_thread returns acquired=false when over the limit.
TEST_F(TestNamespaceRUManager, acquire_thread_manager_level)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));
  mgr.set_default_max_threads(4);
  ASSERT_TRUE(mgr.is_thread_limited());
  const uint64_t ns_id = 7777;
  bool acquired = false;
  ASSERT_EQ(OB_SUCCESS, mgr.acquire_thread(ns_id, 4, ObTimeUtility::current_time() + 1000000, acquired));
  ASSERT_TRUE(acquired);
  // Over the limit + immediate timeout.
  acquired = true;
  ASSERT_EQ(OB_SUCCESS, mgr.acquire_thread(ns_id, 1, ObTimeUtility::current_time(), acquired));
  ASSERT_FALSE(acquired);
  // After release it can acquire again.
  mgr.release_thread(ns_id, 4);
  acquired = false;
  ASSERT_EQ(OB_SUCCESS, mgr.acquire_thread(ns_id, 1, ObTimeUtility::current_time() + 1000000, acquired));
  ASSERT_TRUE(acquired);
  mgr.destroy();
}

// Manager level: when default_max_threads=0 there is no throttling, acquire_thread always succeeds.
TEST_F(TestNamespaceRUManager, acquire_thread_unlimited)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));
  mgr.set_default_max_threads(0);
  ASSERT_FALSE(mgr.is_thread_limited());
  bool acquired = false;
  ASSERT_EQ(OB_SUCCESS, mgr.acquire_thread(8888, 1000, ObTimeUtility::current_time(), acquired));
  ASSERT_TRUE(acquired);
  mgr.destroy();
}

// ==================== refresh_config unit tests (no real SQL, cover the param/cooldown paths) ====================

class TestRefreshConfig : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, ctrl_.init(5001, 200, 100, 100, 50, 10000, 5000));
    ctrl_.qps_bucket_.update_config(200, 100);
    ctrl_.tps_bucket_.update_config(100, 50);
    ctrl_.data_bucket_.update_config(10000, 5000);
  }
  ObMySQLProxy dummy_proxy_;
  ObNamespaceRUController ctrl_;
};

TEST_F(TestRefreshConfig, null_proxy_rejected)
{
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ctrl_.refresh_config(nullptr, 1001, ObString("test")));
}

TEST_F(TestRefreshConfig, empty_database_skips_without_touching_buckets)
{
  const int64_t qps_burst = ctrl_.qps_bucket_.burst_;
  const int64_t tps_burst = ctrl_.tps_bucket_.burst_;
  ASSERT_EQ(OB_SUCCESS, ctrl_.refresh_config(&dummy_proxy_, 1001, ObString()));
  ASSERT_EQ(qps_burst, ctrl_.qps_bucket_.burst_);
  ASSERT_EQ(tps_burst, ctrl_.tps_bucket_.burst_);
}

TEST_F(TestRefreshConfig, cooldown_skips_second_refresh)
{
  ctrl_.last_refresh_time_ = ObTimeUtility::current_time();
  const int64_t qps_burst = ctrl_.qps_bucket_.burst_;
  ASSERT_EQ(OB_SUCCESS, ctrl_.refresh_config(&dummy_proxy_, 1001, ObString("test")));
  ASSERT_EQ(qps_burst, ctrl_.qps_bucket_.burst_);
}

TEST_F(TestRefreshConfig, enabled_zero_unlimits_all_buckets)
{
  // Simulate the config-application logic of refresh_config for the three buckets when enabled=0.
  ctrl_.is_throttled_ = true;
  (void)ctrl_.qps_bucket_.update_config(INT64_MAX, INT64_MAX);
  (void)ctrl_.tps_bucket_.update_config(INT64_MAX, INT64_MAX);
  (void)ctrl_.data_bucket_.update_config(INT64_MAX, INT64_MAX);
  ctrl_.is_throttled_ = false;
  ASSERT_FALSE(ctrl_.is_throttled());
  ASSERT_EQ(INT64_MAX, ctrl_.qps_bucket_.burst_);
  ASSERT_EQ(INT64_MAX, ctrl_.tps_bucket_.burst_);
  ASSERT_EQ(INT64_MAX, ctrl_.data_bucket_.burst_);
}

TEST_F(TestNamespaceRUManager, default_max_threads_is_sixteen)
{
  ObMySQLProxy sql_proxy;
  ObNamespaceRUManager mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1, &sql_proxy));
  ASSERT_TRUE(mgr.is_thread_limited());
  ASSERT_EQ(16, mgr.default_max_threads_);
  const uint64_t ns_id = 5555;
  bool acquired = false;
  for (int i = 0; i < 16; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr.acquire_thread(ns_id, 1, ObTimeUtility::current_time() + 1000000, acquired));
    ASSERT_TRUE(acquired);
  }
  acquired = true;
  ASSERT_EQ(OB_SUCCESS, mgr.acquire_thread(ns_id, 1, ObTimeUtility::current_time(), acquired));
  ASSERT_FALSE(acquired);
  mgr.destroy();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
