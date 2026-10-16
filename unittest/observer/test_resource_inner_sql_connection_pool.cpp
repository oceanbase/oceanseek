/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <chrono>
#include <thread>
#include <gtest/gtest.h>
#define private public
#include "observer/ob_resource_inner_sql_connection_pool.h"
#undef private

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::sqlclient;

class TestResourceInnerSQLConnectionPool : public testing::Test
{
protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, pool_.id_conn_map_.create(128, "TestInnerGC"));
    ASSERT_EQ(OB_SUCCESS, pool_.inner_sql_conn_pool_.cond_.init(
        ObWaitEventIds::INNER_CONNECTION_POOL_COND_WAIT));
    pool_.inner_sql_conn_pool_.inited_ = true;
    pool_.is_inited_ = true;
  }

  void TearDown() override
  {
    pool_.recycle_timer_.destroy();
    while (pool_.id_conn_map_.size() > 0) {
      ObISQLConnection *conn = pool_.id_conn_map_.begin()->second;
      ASSERT_EQ(OB_SUCCESS, pool_.release(false, conn));
    }
    EXPECT_EQ(0, pool_.inner_sql_conn_pool_.used_conn_list_.get_size());
    EXPECT_EQ(pool_.inner_sql_conn_pool_.total_conn_cnt_,
              pool_.inner_sql_conn_pool_.free_conn_list_.get_size());
  }

  // Use real pool allocation/ref-counting/destruction, with no SQL session or running tenant.
  ObInnerSQLConnection *add_connection(const uint64_t id, const bool idle, const int64_t last_query)
  {
    ObInnerSQLConnection *conn = NULL;
    EXPECT_EQ(OB_SUCCESS, pool_.inner_sql_conn_pool_.alloc_conn(conn));
    if (NULL != conn) {
      conn->inited_ = true;
      conn->pool_ = &pool_.inner_sql_conn_pool_;
      conn->ref();
      conn->set_resource_conn_id(id);
      conn->set_is_idle(idle);
      conn->set_last_query_timestamp(last_query);
      EXPECT_EQ(OB_SUCCESS, pool_.inner_sql_conn_pool_.add_to_used_conn_list(conn));
      EXPECT_EQ(OB_SUCCESS, pool_.id_conn_map_.set_refactored(id, conn));
    }
    return conn;
  }

  bool contains(const uint64_t id)
  {
    ObLatchWGuard guard(pool_.lock_, ObLatchIds::INNER_CONN_POOL_LOCK);
    ObISQLConnection *conn = NULL;
    return OB_SUCCESS == pool_.id_conn_map_.get_refactored(id, conn);
  }

  int64_t expired_timestamp()
  {
    return ObTimeUtility::current_time() - ObResourceInnerSQLConnectionPool::IDLE_TIMEOUT_US - 1000000;
  }

  ObResourceInnerSQLConnectionPool pool_;
};

TEST_F(TestResourceInnerSQLConnectionPool, busy_head_does_not_hide_expired_connection)
{
  add_connection(1, false, expired_timestamp());
  add_connection(2, true, expired_timestamp());
  pool_.recycle_expired_connections();
  EXPECT_TRUE(contains(1));
  EXPECT_FALSE(contains(2));
  EXPECT_EQ(1, pool_.inner_sql_conn_pool_.free_conn_list_.get_size());
}

TEST_F(TestResourceInnerSQLConnectionPool, recently_used_head_does_not_hide_expired_connection)
{
  add_connection(1, true, ObTimeUtility::current_time());
  add_connection(2, true, expired_timestamp());
  pool_.recycle_expired_connections();
  EXPECT_TRUE(contains(1));
  EXPECT_FALSE(contains(2));
}

TEST_F(TestResourceInnerSQLConnectionPool, timer_recycles_without_any_new_rpc)
{
  add_connection(1, false, expired_timestamp());
  add_connection(2, true, ObTimeUtility::current_time());
  add_connection(3, true, expired_timestamp());
  ASSERT_EQ(OB_SUCCESS, pool_.recycle_timer_.init("TestInnerGC"));
  ASSERT_EQ(OB_SUCCESS, pool_.recycle_timer_.schedule(pool_.recycle_task_, 1000, true));
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (contains(3) && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  pool_.recycle_timer_.stop();
  EXPECT_TRUE(contains(1));
  EXPECT_TRUE(contains(2));
  EXPECT_FALSE(contains(3));
  EXPECT_EQ(1, pool_.inner_sql_conn_pool_.free_conn_list_.get_size());
}

TEST_F(TestResourceInnerSQLConnectionPool, scans_all_connections_in_same_and_different_buckets)
{
  const uint64_t bucket_count = pool_.id_conn_map_.bucket_count();
  // Integer keys hash to themselves: exercise consecutive erases in one bucket.
  for (uint64_t i = 0; i < 40; ++i) {
    add_connection(1 + i * bucket_count, true, expired_timestamp());
  }
  add_connection(2, true, expired_timestamp());
  add_connection(3, false, expired_timestamp());
  add_connection(4, true, ObTimeUtility::current_time());
  pool_.recycle_expired_connections();
  EXPECT_EQ(2, pool_.id_conn_map_.size());
  EXPECT_TRUE(contains(3));
  EXPECT_TRUE(contains(4));
  EXPECT_EQ(41, pool_.inner_sql_conn_pool_.free_conn_list_.get_size());
}

TEST_F(TestResourceInnerSQLConnectionPool, idle_timeout_boundaries)
{
  const int64_t now = ObTimeUtility::current_time();
  const int64_t ten_minutes_us = 600L * 1000 * 1000;
  ObInnerSQLConnection *conn = add_connection(1, true, now - ten_minutes_us + 1);
  ASSERT_NE(nullptr, conn);
  EXPECT_FALSE(pool_.is_expired(*conn, now));
  conn->set_last_query_timestamp(now - ten_minutes_us);
  EXPECT_TRUE(pool_.is_expired(*conn, now));
  conn->set_is_idle(false);
  EXPECT_FALSE(pool_.is_expired(*conn, now));
  conn->set_is_idle(true);
  conn->set_last_query_timestamp(0);
  EXPECT_FALSE(pool_.is_expired(*conn, now));
  conn->set_last_query_timestamp(now + 1);
  EXPECT_FALSE(pool_.is_expired(*conn, now));
}

TEST_F(TestResourceInnerSQLConnectionPool, acquired_and_refreshed_connections_are_not_recycled)
{
  add_connection(1, true, expired_timestamp());
  ObISQLConnection *request_conn = NULL;
  ASSERT_EQ(OB_SUCCESS, pool_.acquire(1, false, false, request_conn, NULL));
  pool_.recycle_expired_connections();
  EXPECT_TRUE(contains(1));
  ASSERT_EQ(OB_SUCCESS, pool_.release(true, request_conn));
  // Once the request completes, its refreshed idle timestamp must also protect it.
  pool_.recycle_expired_connections();
  EXPECT_TRUE(contains(1));
}

TEST_F(TestResourceInnerSQLConnectionPool, released_connection_is_not_recycled_again)
{
  ObISQLConnection *conn = add_connection(1, true, expired_timestamp());
  ASSERT_EQ(OB_SUCCESS, pool_.release(false, conn));
  pool_.recycle_expired_connections();
  EXPECT_FALSE(contains(1));
  EXPECT_EQ(1, pool_.inner_sql_conn_pool_.free_conn_list_.get_size());
}

TEST_F(TestResourceInnerSQLConnectionPool, request_cannot_acquire_recycled_connection)
{
  add_connection(1, true, expired_timestamp());
  pool_.recycle_expired_connections();
  ObISQLConnection *conn = NULL;
  EXPECT_EQ(OB_SESSION_NOT_FOUND, pool_.acquire(1, false, false, conn, NULL));
  EXPECT_EQ(nullptr, conn);
}

TEST_F(TestResourceInnerSQLConnectionPool, concurrent_request_and_recycling_have_one_owner)
{
  for (uint64_t id = 1; id <= 32; ++id) {
    add_connection(id, true, expired_timestamp());
    int acquire_ret = OB_SUCCESS;
    ObISQLConnection *conn = NULL;
    std::thread request([&]() { acquire_ret = pool_.acquire(id, false, false, conn, NULL); });
    pool_.recycle_expired_connections();
    request.join();
    if (OB_SUCCESS == acquire_ret) {
      ASSERT_NE(nullptr, conn);
      EXPECT_TRUE(contains(id));
      ASSERT_EQ(OB_SUCCESS, pool_.release(false, conn));
    } else {
      EXPECT_EQ(OB_SESSION_NOT_FOUND, acquire_ret);
      EXPECT_EQ(nullptr, conn);
    }
    EXPECT_FALSE(contains(id));
    EXPECT_EQ(0, pool_.inner_sql_conn_pool_.used_conn_list_.get_size());
    EXPECT_EQ(pool_.inner_sql_conn_pool_.total_conn_cnt_,
              pool_.inner_sql_conn_pool_.free_conn_list_.get_size());
  }
}

TEST_F(TestResourceInnerSQLConnectionPool, stop_cancels_timer_without_draining_connections)
{
  ObISQLConnection *idle = add_connection(1, true, ObTimeUtility::current_time());
  ObISQLConnection *busy = add_connection(2, false, expired_timestamp());
  ASSERT_EQ(OB_SUCCESS, pool_.recycle_timer_.init("TestInnerGC"));
  ASSERT_EQ(OB_SUCCESS, pool_.recycle_timer_.schedule(pool_.recycle_task_, 1000, true));
  pool_.stop();
  EXPECT_TRUE(contains(1));
  EXPECT_TRUE(contains(2));
  EXPECT_FALSE(pool_.recycle_timer_.task_exist(pool_.recycle_task_));
  ASSERT_EQ(OB_SUCCESS, pool_.release(true, busy));
  ASSERT_NE(nullptr, busy);
  EXPECT_TRUE(contains(2));
  EXPECT_TRUE(static_cast<ObInnerSQLConnection *>(busy)->is_idle());
  ASSERT_EQ(OB_SUCCESS, pool_.release(false, idle));
  ASSERT_EQ(OB_SUCCESS, pool_.release(false, busy));
  EXPECT_EQ(OB_SUCCESS, pool_.wait());
}

} // namespace observer
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_resource_inner_sql_connection_pool.log", true);
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  oceanbase::common::ObTimerService::get_instance().stop();
  oceanbase::common::ObTimerService::get_instance().wait();
  oceanbase::common::ObTimerService::get_instance().destroy();
  return ret;
}
