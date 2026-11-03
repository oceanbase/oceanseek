/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "env/ob_simple_cluster_test_base.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/vector_index/ob_ltable_bg_task_scheduler.h"

namespace oceanbase
{
namespace share
{
class ObLTableBGTaskTestPeer
{
public:
  static void init_scheduler(ObLTableBGTaskScheduler &scheduler,
                             uint64_t tenant_id,
                             uint64_t database_id,
                             common::ObMySQLProxy *sql_proxy)
  {
    scheduler.tenant_id_ = tenant_id;
    scheduler.sdk_database_id_ = database_id;
    scheduler.sql_proxy_ = sql_proxy;
  }

  static int check_failed(ObLTableBGTaskScheduler &scheduler,
                          const ObLTableBGTaskCtx &ctx,
                          int64_t recycle_time,
                          const char *operation,
                          bool &blocked)
  {
    return scheduler.check_drop_task_failed_(ctx.collection_id_, ctx.namespace_id_,
        ctx.ltable_id_, recycle_time, operation, blocked);
  }

  static int check_completed(ObLTableBGTaskScheduler &scheduler,
                             const ObLTableBGTaskCtx &ctx,
                             int64_t recycle_time,
                             const char *operation,
                             bool &completed)
  {
    return scheduler.check_drop_task_completed_(ctx.collection_id_, ctx.namespace_id_,
        ctx.ltable_id_, recycle_time, operation, completed);
  }

  static int record_start(ObLTableBGDeleteTask &task) { return task.record_task_start_(); }
  static int fail(ObLTableBGDeleteTask &task, int error) { return task.fail_drop_task_(error); }
  static int complete(ObLTableBGDeleteTask &task) { return task.complete_drop_task_(); }
};
} // namespace share

namespace unittest
{
using namespace common;
using namespace share;

class TestLtableCacheCleanupRetry : public ObSimpleClusterTestBase
{
public:
  TestLtableCacheCleanupRetry() : ObSimpleClusterTestBase("test_ltable_cache_cleanup_retry") {}

protected:
  void check_failure(const uint64_t tenant_id, const ObLTableBGTaskType type,
                     const int failure, const bool retryable)
  {
    ObLTableBGTaskCtx ctx;
    ctx.type_ = type;
    ctx.tenant_id_ = tenant_id;
    ctx.database_id_ = 500001;
    ctx.namespace_id_ = 1;
    ctx.ltable_id_ = LTABLE_BG_LTABLE_DELETE == type ? 1 : -1;
    ctx.tablet_id_ = ObTabletID(200001);
    STRCPY(ctx.collection_id_, "0123456789abcdef0123456789abcdef");
    STRCPY(ctx.sdk_database_name_, "test");
    STRCPY(ctx.original_name_, "cleanup_retry");
    ObLTableBGTaskScheduler scheduler;
    ObLTableBGTaskTestPeer::init_scheduler(
        scheduler, tenant_id, ctx.database_id_, GCTX.sql_proxy_);
    ObLTableBGDeleteTask task;
    ASSERT_EQ(OB_SUCCESS, task.init(ctx, GCTX.sql_proxy_));
    const char *operation = LTABLE_BG_LTABLE_DELETE == type ? "LTABLE_DELETE" : "NAMESPACE_DELETE";
    const int64_t recycle_time = ObTimeUtility::current_time() - 60L * 1000 * 1000;
    bool blocked = false;
    ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::check_failed(
        scheduler, ctx, recycle_time, operation, blocked));
    EXPECT_FALSE(blocked);
    ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::record_start(task));
    ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::fail(task, failure));
    ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::check_failed(
        scheduler, ctx, recycle_time, operation, blocked));
    EXPECT_TRUE(blocked); // Even transient failures must respect the cooldown.

    ObSqlString sql;
    int64_t rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "UPDATE %s SET gmt_modified = DATE_SUB(now(6), INTERVAL 31 SECOND) "
        "WHERE tenant_id=%lu AND collection_id='%s' AND ltable_id=%ld",
        OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_TNAME, tenant_id,
        ctx.collection_id_, ctx.ltable_id_));
    ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), rows));
    EXPECT_EQ(1, rows);
    ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::check_failed(
        scheduler, ctx, recycle_time, operation, blocked));
    EXPECT_EQ(!retryable, blocked);

    if (retryable) {
      // A restarted scheduler has no in-memory retry state but must allow retry.
      ObLTableBGTaskScheduler recovered;
      ObLTableBGTaskTestPeer::init_scheduler(
          recovered, tenant_id, ctx.database_id_, GCTX.sql_proxy_);
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::check_failed(
          recovered, ctx, recycle_time, operation, blocked));
      EXPECT_FALSE(blocked);
      // A second failure must refresh the history timestamp, otherwise every
      // scheduler tick after the first cooldown could retry immediately.
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::record_start(task));
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::fail(task, failure));
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::check_failed(
          recovered, ctx, recycle_time, operation, blocked));
      EXPECT_TRUE(blocked);
      ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), rows));
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::check_failed(
          recovered, ctx, recycle_time, operation, blocked));
      EXPECT_FALSE(blocked);
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::record_start(task));
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::complete(task));
      bool completed = false;
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::check_completed(
          recovered, ctx, recycle_time, operation, completed));
      EXPECT_TRUE(completed);
      // A stale failing DAG must not replace another DAG's completion record.
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::record_start(task));
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::fail(task, OB_ERR_UNEXPECTED));
      ASSERT_EQ(OB_SUCCESS, ObLTableBGTaskTestPeer::check_completed(
          recovered, ctx, recycle_time, operation, completed));
      EXPECT_TRUE(completed);
    }
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "DELETE FROM %s WHERE tenant_id=%lu AND collection_id='%s'",
        OB_ALL_AGENT_DB_BACKGROUND_TASK_HISTORY_TNAME, tenant_id, ctx.collection_id_));
    ASSERT_EQ(OB_SUCCESS, GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), rows));
  }
};

TEST_F(TestLtableCacheCleanupRetry, transient_failure_recovers_but_permanent_failure_stays_terminal)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    const int retryable_errors[] = {OB_TIMEOUT, OB_EAGAIN, OB_SCHEMA_EAGAIN,
        OB_TENANT_SCHEMA_NOT_FULL, OB_ERR_REMOTE_SCHEMA_NOT_FULL, OB_ENTRY_NOT_EXIST};
    for (const int error : retryable_errors) {
      ASSERT_NO_FATAL_FAILURE(check_failure(tenant_id, LTABLE_BG_LTABLE_DELETE, error, true));
      ASSERT_NO_FATAL_FAILURE(check_failure(tenant_id, LTABLE_BG_NAMESPACE_DELETE, error, true));
    }
    ASSERT_NO_FATAL_FAILURE(check_failure(tenant_id, LTABLE_BG_LTABLE_DELETE,
        OB_INVALID_ARGUMENT, false));
    ASSERT_NO_FATAL_FAILURE(check_failure(tenant_id, LTABLE_BG_NAMESPACE_DELETE,
        OB_ERR_UNEXPECTED, false));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
