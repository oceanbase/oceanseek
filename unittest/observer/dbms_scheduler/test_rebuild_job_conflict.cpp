/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <string>
#include <set>
#include <sstream>
#include "lib/ob_define.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/mysqlclient/ob_isql_result_handler.h"
#include "lib/mysqlclient/ob_mysql_result_impl.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "storage/vector_index/ob_vector_index_sched_job_utils.h"
#include "storage/vector_index/ob_vector_index_refresh.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace common::sqlclient;
using namespace dbms_scheduler;
using rootserver::ObDDLTaskRecordOperator;

TEST(RebuildRetryAdmissionTest, reuse_clears_only_the_attempt_marker)
{
  storage::ObVectorRefreshIndexCtx ctx;
  EXPECT_FALSE(ctx.rebuild_admission_rejected_);
  ctx.rebuild_admission_rejected_ = true;
  ctx.reuse();
  EXPECT_FALSE(ctx.rebuild_admission_rejected_);
}

class ConflictResult : public ObMySQLResultImpl
{
public:
  ConflictResult(ObMySQLStatement &stmt, int next_ret) : ObMySQLResultImpl(stmt), next_ret_(next_ret) {}
  int next() override { return next_ret_; }
private:
  int next_ret_;
};

class ConflictResultHandler : public ObISQLResultHandler
{
public:
  explicit ConflictResultHandler(int next_ret) : result_(statement_, next_ret) {}
  ObMySQLResult *mysql_result() override { return &result_; }
private:
  ObMySQLStatement statement_;
  ConflictResult result_;
};

class ConflictProxy : public ObMySQLProxy
{
public:
  using ObMySQLProxy::read;
  int read(ReadResult &res, const uint64_t tenant_id, const char *sql) override
  {
    ++reads_;
    tenant_id_ = tenant_id;
    sql_ = sql;
    int ret = read_ret_;
    if (OB_SUCC(ret) && !null_result_) {
      ConflictResultHandler *handler = nullptr;
      ret = res.create_handler(handler, next_ret_);
    }
    return ret;
  }
  int read_ret_ = OB_SUCCESS;
  int next_ret_ = OB_ITER_END;
  bool null_result_ = false;
  int reads_ = 0;
  uint64_t tenant_id_ = 0;
  std::string sql_;
};

class RebuildConflictTest : public testing::Test
{
public:
  void SetUp() override
  {
    job_.tenant_id_ = 1004;
    job_.job_ = 101;
    job_.next_date_ = ObTimeUtility::current_time();
    job_.func_type_ = ObDBMSSchedFuncType::VECTOR_INDEX_REFRESH_JOB;
    job_.job_name_ = ObString::make_string("500001_rebuild");

  }
  int query(bool &conflict)
  {
    return ObDDLTaskRecordOperator::check_vec_index_ddl_conflict(
        proxy_, job_.tenant_id_, 500001, conflict);
  }
  ConflictProxy proxy_;
  ObDBMSSchedJobInfo job_;
};

TEST_F(RebuildConflictTest, row_and_no_row_are_distinguished)
{
  for (int next_ret : {OB_SUCCESS, OB_ITER_END}) {
    proxy_.next_ret_ = next_ret;
    bool conflict = true;
    EXPECT_EQ(OB_SUCCESS, query(conflict));
    EXPECT_EQ(next_ret == OB_SUCCESS, conflict);
  }
}

TEST_F(RebuildConflictTest, query_errors_are_propagated)
{
  for (int error : {OB_EAGAIN, OB_TIMEOUT, OB_ALLOCATE_MEMORY_FAILED, OB_ERR_UNEXPECTED}) {
    bool conflict = false;
    proxy_.read_ret_ = error;
    EXPECT_EQ(error, query(conflict));
    proxy_.read_ret_ = OB_SUCCESS;
    proxy_.next_ret_ = error;
    EXPECT_EQ(error, query(conflict));
  }
  proxy_.null_result_ = true;
  bool conflict = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED, query(conflict));
}

TEST_F(RebuildConflictTest, query_uses_explicit_tenant_and_base_table)
{
  for (uint64_t tenant : {1004UL, 1006UL}) {
    job_.tenant_id_ = tenant;
    bool conflict = true;
    ASSERT_EQ(OB_SUCCESS, query(conflict));
    EXPECT_EQ(tenant, proxy_.tenant_id_);
    EXPECT_NE(std::string::npos, proxy_.sql_.find("object_id=500001"));
    EXPECT_EQ(std::string::npos, proxy_.sql_.find("message"));
    EXPECT_EQ(std::string::npos, proxy_.sql_.find("snapshot"));
    EXPECT_EQ(std::string::npos, proxy_.sql_.find("status="));
  }
}

TEST_F(RebuildConflictTest, invalid_ids_do_not_query)
{
  bool conflict = false;
  for (uint64_t tenant : {OB_INVALID_TENANT_ID, OB_INVALID_ID}) {
    EXPECT_EQ(OB_INVALID_ARGUMENT,
        ObDDLTaskRecordOperator::check_vec_index_ddl_conflict(proxy_, tenant, 500001, conflict));
  }
  for (uint64_t table_id : {uint64_t(0), OB_INVALID_ID}) {
    EXPECT_EQ(OB_INVALID_ARGUMENT,
        ObDDLTaskRecordOperator::check_vec_index_ddl_conflict(proxy_, 1004, table_id, conflict));
  }
  EXPECT_EQ(0, proxy_.reads_);
}

TEST_F(RebuildConflictTest, vector_ddl_types_include_sparse_indexes)
{
  bool conflict = false;
  ASSERT_EQ(OB_SUCCESS, query(conflict));
  const std::string marker = "ddl_type in (";
  const auto begin = proxy_.sql_.find(marker);
  ASSERT_NE(std::string::npos, begin);
  const auto end = proxy_.sql_.find(')', begin);
  ASSERT_NE(std::string::npos, end);
  std::istringstream values(proxy_.sql_.substr(begin + marker.size(), end - begin - marker.size()));
  std::set<int> actual;
  std::string value;
  while (std::getline(values, value, ',')) {
    actual.insert(std::stoi(value));
  }
  using namespace share;
  const std::set<int> expected = {
      DDL_CREATE_VEC_INDEX, DDL_CREATE_VEC_IVFFLAT_INDEX, DDL_CREATE_VEC_IVFSQ8_INDEX,
      DDL_CREATE_VEC_IVFPQ_INDEX, DDL_DROP_VEC_INDEX, DDL_DROP_VEC_IVFFLAT_INDEX,
      DDL_DROP_VEC_IVFSQ8_INDEX, DDL_DROP_VEC_IVFPQ_INDEX, DDL_REBUILD_INDEX,
      DDL_CREATE_VEC_SPIV_INDEX, DDL_DROP_VEC_SPIV_INDEX};
  EXPECT_EQ(expected, actual);
}

class RebuildDeferralTest : public testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1004));
    ASSERT_EQ(OB_SUCCESS, lib::ObMallocAllocator::get_instance()->set_tenant_limit(1004, 1LL << 30));
  }
  static void TearDownTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(1004));
  }
  void SetUp() override
  {
    root_.tenant_id_ = 1004;
    root_.job_ = 101;
    root_.job_name_ = ObString::make_string("500001_rebuild");
    root_.func_type_ = ObDBMSSchedFuncType::VECTOR_INDEX_REFRESH_JOB;
    root_.repeat_interval_ = ObString::make_string("FREQ=DAILY; INTERVAL=1");
    root_.job_action_ = ObString::make_string("DBMS_VECTOR.rebuild_index_inner(500001,0.3,\"\",\"\",\"\",4)");
    root_.exec_env_ = ObString::make_string("saved execution environment");
    root_.enabled_ = 1;
    root_.this_date_ = now_;
    root_.next_date_ = now_ + 3600000000L;
    root_.end_date_ = now_ + 86400000000L;
    root_.max_run_duration_ = 7200;
  }
  int build(ObDBMSSchedJobInfo &job, int64_t now, ObDBMSSchedJobInfo &next,
            int64_t retry_cnt = 0)
  {
    return storage::ObVectorIndexSchedJobUtils::build_rebuild_defer_job(
        allocator_, 500001, job, root_, now, retry_cnt, next);
  }
  const int64_t now_ = 1000000000000L;
  ObArenaAllocator allocator_;
  ObDBMSSchedJobInfo root_;
};

TEST_F(RebuildDeferralTest, one_shot_preserves_action_environment_and_original_schedule)
{
  ObDBMSSchedJobInfo next;
  ASSERT_EQ(OB_SUCCESS, build(root_, now_, next));
  EXPECT_TRUE(next.is_once_job());
  EXPECT_TRUE(next.auto_drop_);
  EXPECT_TRUE(root_.job_type_.empty());
  EXPECT_EQ(ObString::make_string("PLSQL_BLOCK"), next.job_type_);
  EXPECT_EQ(now_ + 20000000, next.start_date_);
  EXPECT_EQ(root_.next_date_, next.end_date_);
  EXPECT_EQ(root_.job_action_, next.job_action_);
  EXPECT_EQ(root_.exec_env_, next.exec_env_);
  EXPECT_FALSE(root_.is_once_job());
  EXPECT_EQ(now_ + 3600000000L, root_.next_date_);
  EXPECT_EQ(0, next.this_date_);
}

TEST_F(RebuildDeferralTest, repeated_handoff_has_deterministic_name_and_fixed_deadline)
{
  ObDBMSSchedJobInfo first, duplicate, second;
  ASSERT_EQ(OB_SUCCESS, build(root_, now_, first));
  ASSERT_EQ(OB_SUCCESS, build(root_, now_ + 1000000, duplicate));
  EXPECT_EQ(first.job_name_, duplicate.job_name_);
  EXPECT_EQ(first.end_date_, duplicate.end_date_);
  first.job_ = 102;
  first.this_date_ = first.start_date_;
  ASSERT_EQ(OB_SUCCESS, build(first, first.start_date_, second));
  EXPECT_NE(first.job_name_, second.job_name_);
  EXPECT_EQ(first.end_date_, second.end_date_);
  EXPECT_EQ(root_.job_action_, second.job_action_);
  EXPECT_EQ(OB_TIMEOUT, build(first, first.end_date_, second));
}

TEST_F(RebuildDeferralTest, new_period_does_not_extend_previous_chain)
{
  ObDBMSSchedJobInfo child, next;
  ASSERT_EQ(OB_SUCCESS, build(root_, now_, child));
  child.job_ = 102;
  child.this_date_ = child.start_date_;
  root_.next_date_ += 3600000000L;
  ASSERT_EQ(OB_SUCCESS, build(child, child.this_date_, next));
  EXPECT_EQ(child.end_date_, next.end_date_);
  EXPECT_EQ(OB_TIMEOUT, build(child, child.end_date_, next));
}

TEST_F(RebuildDeferralTest, disabled_and_recreated_root_stop_the_chain)
{
  ObDBMSSchedJobInfo child, next;
  ASSERT_EQ(OB_SUCCESS, build(root_, now_, child));
  child.job_ = 102;
  child.this_date_ = child.start_date_;
  root_.enabled_ = 0;
  EXPECT_EQ(OB_CANCELED, build(child, child.this_date_, next));
  root_.enabled_ = 1;
  root_.state_ = ObString::make_string("BROKEN");
  EXPECT_EQ(OB_CANCELED, build(child, child.this_date_, next));
  root_.state_.reset();
  root_.job_ = 103;
  EXPECT_EQ(OB_CANCELED, build(child, child.this_date_, next));
}

TEST_F(RebuildDeferralTest, run_budget_and_end_date_bound_first_handoff)
{
  ObDBMSSchedJobInfo next;
  root_.max_run_duration_ = 60;
  ASSERT_EQ(OB_SUCCESS, build(root_, now_, next));
  EXPECT_EQ(now_ + 60000000, next.end_date_);
  root_.end_date_ = now_ + 30000000;
  ASSERT_EQ(OB_SUCCESS, build(root_, now_, next));
  EXPECT_EQ(root_.end_date_, next.end_date_);
}

TEST_F(RebuildDeferralTest, large_duration_does_not_overflow)
{
  ObDBMSSchedJobInfo next;
  root_.max_run_duration_ = INT64_MAX;
  ASSERT_EQ(OB_SUCCESS, build(root_, now_, next));
  EXPECT_EQ(root_.next_date_, next.end_date_);
}

TEST_F(RebuildDeferralTest, successors_keep_consumed_budget_and_other_config)
{
  root_.job_config_ = ObString::make_string(R"({"unrelated":{"value":"keep","ratio":1.25,"quote":"a\"b"}})");
  for (int64_t failures : {0L, 1L, 3L}) {
    ObDBMSSchedJobInfo child, successor;
    int64_t actual = -1;
    ASSERT_EQ(OB_SUCCESS, build(root_, now_, child, failures));
    ASSERT_EQ(OB_SUCCESS, storage::ObVectorIndexSchedJobUtils::get_rebuild_job_retry_count(child, actual));
    EXPECT_EQ(failures, actual);
    std::string config(child.job_config_.ptr(), child.job_config_.length());
    EXPECT_NE(std::string::npos, config.find("unrelated"));
    EXPECT_NE(std::string::npos, config.find("keep"));
    child.job_ = 102;
    child.this_date_ = child.start_date_;
    ASSERT_EQ(OB_SUCCESS, build(child, child.this_date_, successor, actual));
    ASSERT_EQ(OB_SUCCESS, storage::ObVectorIndexSchedJobUtils::get_rebuild_job_retry_count(successor, actual));
    EXPECT_EQ(failures, actual);
    EXPECT_EQ(child.end_date_, successor.end_date_);
  }
  EXPECT_EQ(ObString::make_string(R"({"unrelated":{"value":"keep","ratio":1.25,"quote":"a\"b"}})"), root_.job_config_);
}

TEST_F(RebuildDeferralTest, periodic_occurrences_start_with_a_fresh_budget)
{
  root_.job_config_ = ObString::make_string("{\"vector_rebuild_retry_count\":3}");
  int64_t failures = -1;
  ASSERT_EQ(OB_SUCCESS, storage::ObVectorIndexSchedJobUtils::get_rebuild_job_retry_count(root_, failures));
  EXPECT_EQ(0, failures);
}

TEST_F(RebuildDeferralTest, malformed_or_exhausted_successor_budget_is_rejected)
{
  ObDBMSSchedJobInfo child;
  ASSERT_EQ(OB_SUCCESS, build(root_, now_, child));
  const char *invalid[] = {"", "{}", "[]", "null", "not json",
      "{\"vector_rebuild_retry_count\":-1}",
      "{\"vector_rebuild_retry_count\":4}",
      "{\"vector_rebuild_retry_count\":18446744073709551617}",
      "{\"vector_rebuild_retry_count\":0.5}",
      "{\"vector_rebuild_retry_count\":\"1\"}",
      "{\"vector_rebuild_retry_count\":true}",
      "{\"vector_rebuild_retry_count\":1,\"vector_rebuild_retry_count\":2}"};
  for (const char *config : invalid) {
    child.job_config_ = ObString::make_string(config);
    int64_t failures = -1;
    EXPECT_NE(OB_SUCCESS,
        storage::ObVectorIndexSchedJobUtils::get_rebuild_job_retry_count(child, failures)) << config;
  }
  ObDBMSSchedJobInfo next;
  EXPECT_EQ(OB_INVALID_ARGUMENT, build(root_, now_, next, -1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, build(root_, now_, next, 4));
}

TEST_F(RebuildDeferralTest, unrelated_jobs_are_rejected)
{
  ObDBMSSchedJobInfo next;
  root_.func_type_ = ObDBMSSchedFuncType::USER_JOB;
  EXPECT_EQ(OB_INVALID_ARGUMENT, build(root_, now_, next));
  root_.func_type_ = ObDBMSSchedFuncType::VECTOR_INDEX_REFRESH_JOB;
  root_.job_name_ = ObString::make_string("500001_refresh");
  EXPECT_EQ(OB_INVALID_ARGUMENT, build(root_, now_, next));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
