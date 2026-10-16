/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <cstdio>
#include <cstring>
#include <gtest/gtest.h>
#include <unistd.h>
#define private public
#define protected public
#include "storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#undef protected
#undef private
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/time/ob_time_utility.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;

namespace storage
{

struct SSExecDagEventRow
{
  SSExecDagEventRow()
    : module_(),
      event_(),
      name1_(),
      value1_(),
      name2_(),
      value2_(),
      name3_(),
      value3_(),
      name4_(),
      value4_(),
      name5_(),
      value5_(),
      name6_(),
      value6_(),
      extra_info_()
  {}

  char module_[MAX_ROOTSERVICE_EVENT_DESC_LENGTH];
  char event_[MAX_ROOTSERVICE_EVENT_DESC_LENGTH];
  char name1_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value1_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name2_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value2_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name3_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value3_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name4_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value4_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name5_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value5_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name6_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value6_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char extra_info_[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH];
};

TEST(TestSSGCExecutorCostStat, reset_clears_macro_counts)
{
  ObSSGCExecutorCostStat stat;
  stat.succ_sstable_macro_cnt_ = 12;
  stat.succ_tablet_sub_meta_cnt_ = 7;
  stat.reset();
  ASSERT_EQ(0, stat.succ_sstable_macro_cnt_);
  ASSERT_EQ(0, stat.succ_tablet_sub_meta_cnt_);
  char buf[512] = {};
  stat.to_string(buf, sizeof(buf));
  ASSERT_NE(nullptr, strstr(buf, "succ_sstable_macro_cnt=0"));
  ASSERT_NE(nullptr, strstr(buf, "succ_tablet_sub_meta_cnt=0"));
}

class TestSSGCExecDagEvent : public unittest::ObSimpleClusterTestBase
{
public:
  TestSSGCExecDagEvent()
    : unittest::ObSimpleClusterTestBase("test_ss_gc_exec_dag_event")
  {}
  virtual void SetUp() override;
  virtual void TearDown() override;

protected:
  void report_dag_event(const ObSharedGCTaskType gc_type,
                        const int64_t allow_gc_round_id,
                        const int64_t plan_cost_us,
                        const int64_t execute_cost_us,
                        const int64_t succ_row_count,
                        const int64_t failed_row_count,
                        const int64_t succ_sstable_macro_cnt,
                        const int64_t succ_tablet_sub_meta_cnt,
                        const int dag_ret,
                        const ObIDag::ObDagStatus dag_status);
  void wait_and_get_event(const int64_t begin_ts,
                          const uint64_t tenant_id,
                          const char *gc_type,
                          const int64_t allow_gc_round_id,
                          SSExecDagEventRow &row);
  void assert_event_row(const SSExecDagEventRow &row,
                        const uint64_t tenant_id,
                        const char *gc_type,
                        const int64_t allow_gc_round_id,
                        const int dag_ret,
                        const ObIDag::ObDagStatus dag_status,
                        const int64_t plan_cost_us,
                        const int64_t execute_cost_us,
                        const int64_t succ_row_count,
                        const int64_t failed_row_count,
                        const int64_t succ_sstable_macro_cnt,
                        const int64_t succ_tablet_sub_meta_cnt);

};

void TestSSGCExecDagEvent::SetUp()
{
  ObSimpleClusterTestBase::SetUp();
}

void TestSSGCExecDagEvent::TearDown()
{
  ObSimpleClusterTestBase::TearDown();
}

void TestSSGCExecDagEvent::report_dag_event(
    const ObSharedGCTaskType gc_type,
    const int64_t allow_gc_round_id,
    const int64_t plan_cost_us,
    const int64_t execute_cost_us,
    const int64_t succ_row_count,
    const int64_t failed_row_count,
    const int64_t succ_sstable_macro_cnt,
    const int64_t succ_tablet_sub_meta_cnt,
    const int dag_ret,
    const ObIDag::ObDagStatus dag_status)
{
  ObSSGCExecDag dag;
  ObSSGCExecDagInitParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.gc_type_ = gc_type;
  param.allow_gc_round_id_ = allow_gc_round_id;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  // Synthesize a plan/execute window that matches the requested durations.
  // report_result() stamps finish_us_ via inc_update, so record_start_us()
  // supplies the matching dag-level start anchor.
  const int64_t now_us = ObTimeUtility::current_time_us();
  dag.record_start_us();
  dag.record_plan_us(now_us, now_us + plan_cost_us);
  dag.record_execute_us(now_us, now_us + execute_cost_us);
  dag.add_row_stat(succ_row_count, failed_row_count,
                   succ_sstable_macro_cnt, succ_tablet_sub_meta_cnt);
  dag.set_dag_ret(dag_ret);
  dag.set_dag_status(dag_status);
  ASSERT_EQ(OB_SUCCESS, dag.report_result());
}

void TestSSGCExecDagEvent::wait_and_get_event(
    const int64_t begin_ts,
    const uint64_t tenant_id,
    const char *gc_type,
    const int64_t allow_gc_round_id,
    SSExecDagEventRow &row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const int64_t deadline_us = ObTimeUtility::current_time() + 10 * 1000 * 1000LL;
  ObSqlString sql;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT module, event, name1, value1, name2, value2, name3, value3, "
        "name4, value4, name5, value5, name6, value6, extra_info "
        "FROM oceanbase.%s WHERE time_to_usec(gmt_create) >= %ld "
        "AND module = 'SHARED_GC' "
        "AND event = 'SS_GC_EXEC_DAG_FINISH' "
        "AND name1 = 'tenant_id' AND value1 = '%lu' "
        "AND name2 = 'gc_type' AND value2 = '%s' "
        "AND name3 = 'allow_gc_round_id' AND value3 = '%ld' "
        "ORDER BY gmt_create DESC LIMIT 1",
        OB_ALL_SERVER_EVENT_HISTORY_TNAME,
        begin_ts,
        tenant_id,
        gc_type,
        allow_gc_round_id));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(res, sql.ptr()))) {
        LOG_WARN("fail to read server event history", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mysql result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        int64_t tmp_real_str_len = 0;
        found = true;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "module", row.module_,
                                   MAX_ROOTSERVICE_EVENT_DESC_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "event", row.event_,
                                   MAX_ROOTSERVICE_EVENT_DESC_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name1", row.name1_,
                                   MAX_ROOTSERVICE_EVENT_NAME_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value1", row.value1_,
                                   MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name2", row.name2_,
                                   MAX_ROOTSERVICE_EVENT_NAME_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value2", row.value2_,
                                   MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name3", row.name3_,
                                   MAX_ROOTSERVICE_EVENT_NAME_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value3", row.value3_,
                                   MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name4", row.name4_,
                                   MAX_ROOTSERVICE_EVENT_NAME_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value4", row.value4_,
                                   MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name5", row.name5_,
                                   MAX_ROOTSERVICE_EVENT_NAME_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value5", row.value5_,
                                   MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name6", row.name6_,
                                   MAX_ROOTSERVICE_EVENT_NAME_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value6", row.value6_,
                                   MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, tmp_real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "extra_info", row.extra_info_,
                                   MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH, tmp_real_str_len);
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    if (!found) {
      usleep(100 * 1000);
    }
  } while (!found && ObTimeUtility::current_time() < deadline_us);
  ASSERT_TRUE(found) << "server event history row not found, sql=" << sql.ptr();
}

void TestSSGCExecDagEvent::assert_event_row(
    const SSExecDagEventRow &row,
    const uint64_t tenant_id,
    const char *gc_type,
    const int64_t allow_gc_round_id,
    const int dag_ret,
    const ObIDag::ObDagStatus dag_status,
    const int64_t plan_cost_us,
    const int64_t execute_cost_us,
    const int64_t succ_row_count,
    const int64_t failed_row_count,
    const int64_t succ_sstable_macro_cnt,
    const int64_t succ_tablet_sub_meta_cnt)
{
  char tenant_id_buf[32] = {0};
  char allow_gc_round_id_buf[32] = {0};
  char dag_ret_buf[32] = {0};
  char dag_status_buf[32] = {0};
  ASSERT_GT(snprintf(tenant_id_buf, sizeof(tenant_id_buf), "%lu", tenant_id), 0);
  ASSERT_GT(snprintf(allow_gc_round_id_buf, sizeof(allow_gc_round_id_buf),
                     "%ld", allow_gc_round_id), 0);
  ASSERT_GT(snprintf(dag_ret_buf, sizeof(dag_ret_buf), "%d", dag_ret), 0);
  ASSERT_GT(snprintf(dag_status_buf, sizeof(dag_status_buf), "%ld",
                     static_cast<int64_t>(dag_status)), 0);

  ASSERT_STREQ("SHARED_GC", row.module_);
  ASSERT_STREQ("SS_GC_EXEC_DAG_FINISH", row.event_);
  ASSERT_STREQ("tenant_id", row.name1_);
  ASSERT_STREQ(tenant_id_buf, row.value1_);
  ASSERT_STREQ("gc_type", row.name2_);
  ASSERT_STREQ(gc_type, row.value2_);
  ASSERT_STREQ("allow_gc_round_id", row.name3_);
  ASSERT_STREQ(allow_gc_round_id_buf, row.value3_);
  ASSERT_STREQ("dag_ret", row.name4_);
  ASSERT_STREQ(dag_ret_buf, row.value4_);
  ASSERT_STREQ("dag_status", row.name5_);
  ASSERT_STREQ(dag_status_buf, row.value5_);
  ASSERT_STREQ("dag_id", row.name6_);
  ASSERT_NE(nullptr, strstr(row.extra_info_, "total_cost_us="));

  char cost_buf[64] = {0};
  ASSERT_GT(snprintf(cost_buf, sizeof(cost_buf), "plan_cost_us=%ld", plan_cost_us), 0);
  ASSERT_NE(nullptr, strstr(row.extra_info_, cost_buf));
  ASSERT_GT(snprintf(cost_buf, sizeof(cost_buf), "execute_cost_us=%ld", execute_cost_us), 0);
  ASSERT_NE(nullptr, strstr(row.extra_info_, cost_buf));
  ASSERT_GT(snprintf(cost_buf, sizeof(cost_buf), "succ_row_count=%ld", succ_row_count), 0);
  ASSERT_NE(nullptr, strstr(row.extra_info_, cost_buf));
  ASSERT_GT(snprintf(cost_buf, sizeof(cost_buf), "failed_row_count=%ld", failed_row_count), 0);
  ASSERT_NE(nullptr, strstr(row.extra_info_, cost_buf));
  ASSERT_GT(snprintf(cost_buf, sizeof(cost_buf),
                     "succ_sstable_macro_cnt=%ld", succ_sstable_macro_cnt), 0);
  ASSERT_NE(nullptr, strstr(row.extra_info_, cost_buf));
  ASSERT_GT(snprintf(cost_buf, sizeof(cost_buf),
                     "succ_tablet_sub_meta_cnt=%ld", succ_tablet_sub_meta_cnt), 0);
  ASSERT_NE(nullptr, strstr(row.extra_info_, cost_buf));
}

// Verify the normal MARK_SWEEP_GC finish path:
// 1. Build an initialized DAG and inject plan/execute statistics.
// 2. Mark the DAG as successfully finished and call report_result().
// 3. Query __all_server_event_history and check event fields plus extra_info.
TEST_F(TestSSGCExecDagEvent, report_mark_sweep_finish_event)
{
  const int64_t begin_ts = ObTimeUtility::current_time();
  const int64_t allow_gc_round_id = 6001;
  report_dag_event(ObSharedGCTaskType::MARK_SWEEP_GC,
                   allow_gc_round_id,
                   11 /*plan_cost_us*/,
                   22 /*execute_cost_us*/,
                   3 /*succ_row_count*/,
                   1 /*failed_row_count*/,
                   12 /*succ_sstable_macro_cnt*/,
                   4 /*succ_tablet_sub_meta_cnt*/,
                   OB_SUCCESS,
                   ObIDag::DAG_STATUS_FINISH);

  SSExecDagEventRow row;
  wait_and_get_event(begin_ts, OB_SYS_TENANT_ID, "MARK_SWEEP_GC",
                     allow_gc_round_id, row);
  assert_event_row(row, OB_SYS_TENANT_ID, "MARK_SWEEP_GC",
                   allow_gc_round_id, OB_SUCCESS, ObIDag::DAG_STATUS_FINISH,
                   11, 22, 3, 1, 12, 4);
}

// Verify the normal FAIL_GC finish path:
// 1. Report a successful FAIL_GC DAG with its default allow_gc_round_id.
// 2. Query the generated server event by gc_type and allow_gc_round_id.
// 3. Ensure the event records FAIL_GC and preserves all accumulated costs.
TEST_F(TestSSGCExecDagEvent, report_fail_gc_finish_event)
{
  const int64_t begin_ts = ObTimeUtility::current_time();
  const int64_t allow_gc_round_id = 0;
  report_dag_event(ObSharedGCTaskType::FAIL_GC,
                   allow_gc_round_id,
                   7 /*plan_cost_us*/,
                   13 /*execute_cost_us*/,
                   5 /*succ_row_count*/,
                   0 /*failed_row_count*/,
                   20 /*succ_sstable_macro_cnt*/,
                   7 /*succ_tablet_sub_meta_cnt*/,
                   OB_SUCCESS,
                   ObIDag::DAG_STATUS_FINISH);

  SSExecDagEventRow row;
  wait_and_get_event(begin_ts, OB_SYS_TENANT_ID, "FAIL_GC",
                     allow_gc_round_id, row);
  assert_event_row(row, OB_SYS_TENANT_ID, "FAIL_GC",
                   allow_gc_round_id, OB_SUCCESS, ObIDag::DAG_STATUS_FINISH,
                   7, 13, 5, 0, 20, 7);
}

// Verify that failed DAGs are still reported:
// 1. Build a MARK_SWEEP_GC DAG with failed row statistics.
// 2. Set a non-success dag_ret and DAG_STATUS_NODE_FAILED before reporting.
// 3. Check server_event_history keeps the failure status and cost details.
TEST_F(TestSSGCExecDagEvent, report_failed_dag_event)
{
  const int64_t begin_ts = ObTimeUtility::current_time();
  const int64_t allow_gc_round_id = 7001;
  report_dag_event(ObSharedGCTaskType::MARK_SWEEP_GC,
                   allow_gc_round_id,
                   17 /*plan_cost_us*/,
                   23 /*execute_cost_us*/,
                   0 /*succ_row_count*/,
                   2 /*failed_row_count*/,
                   0 /*succ_sstable_macro_cnt*/,
                   0 /*succ_tablet_sub_meta_cnt*/,
                   OB_ERR_UNEXPECTED,
                   ObIDag::DAG_STATUS_NODE_FAILED);

  SSExecDagEventRow row;
  wait_and_get_event(begin_ts, OB_SYS_TENANT_ID, "MARK_SWEEP_GC",
                     allow_gc_round_id, row);
  assert_event_row(row, OB_SYS_TENANT_ID, "MARK_SWEEP_GC",
                   allow_gc_round_id, OB_ERR_UNEXPECTED,
                   ObIDag::DAG_STATUS_NODE_FAILED,
                   17, 23, 0, 2, 0, 0);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
