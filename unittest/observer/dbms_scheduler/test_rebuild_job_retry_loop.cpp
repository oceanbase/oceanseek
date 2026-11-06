/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <vector>
#include "lib/alloc/ob_malloc_allocator.h"
#include "observer/ob_server_struct.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#define private public
#include "storage/vector_index/cmd/ob_vector_refresh_index_executor.h"
#undef private
#include "storage/vector_index/ob_vector_index_sched_job_utils.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace storage;
using namespace sql;
using namespace dbms_scheduler;

struct CheckStep { int ret_; bool deferred_; };
struct RefreshStep { int ret_; bool admission_rejected_; };
// These two dependencies are interposed only in this standalone test executable.
static std::vector<CheckStep> checks;
static std::vector<RefreshStep> refreshes;
static std::vector<int64_t> budgets;
static size_t refresh_calls = 0;

class RebuildJobRetryLoopTest : public testing::Test
{
public:
  RebuildJobRetryLoopTest() : ctx_(allocator_) {}
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
    checks.clear();
    refreshes.clear();
    budgets.clear();
    refresh_calls = 0;
    old_proxy_ = GCTX.sql_proxy_;
    GCTX.sql_proxy_ = &proxy_;
    ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, &allocator_));
    ASSERT_EQ(OB_SUCCESS, session_.set_session_state(QUERY_ACTIVE));
    ctx_.set_my_session(&session_);
    ctx_.set_sql_proxy(&proxy_);
    ASSERT_EQ(OB_SUCCESS, ctx_.create_physical_plan_ctx());
    ctx_.get_physical_plan_ctx()->set_timeout_timestamp(ObTimeUtility::current_time() + 60000000);
    job_.tenant_id_ = 1004;
    job_.func_type_ = ObDBMSSchedFuncType::VECTOR_INDEX_REFRESH_JOB;
    job_.job_config_ = ObString::make_string("{\"vector_rebuild_retry_count\":0}");
    session_.set_job_info(&job_);
    executor_.ctx_ = &ctx_;
    executor_.session_info_ = &session_;
    executor_.tenant_id_ = 1004;
    executor_.base_tb_id_ = 500001;
    executor_.domain_tb_id_ = 500002;
    executor_.index_id_tb_id_ = 500003;
    executor_.refresh_method_ = share::schema::ObVectorRefreshMethod::REBUILD_COMPLETE;
    executor_.idx_organization_ = share::schema::ObVectorIndexOrganization::IN_MEMORY_NEIGHBOR_GRAPH;
    executor_.idx_distance_metrics_ = share::schema::ObVetcorIndexDistanceMetric::EUCLIDEAN;
    executor_.idx_parallel_creation_ = 1;
    executor_.delta_rate_threshold_ = 0.2;
  }
  void TearDown() override
  {
    EXPECT_EQ(checks.size(), budgets.size());
    EXPECT_EQ(refreshes.size(), refresh_calls);
    session_.set_job_info(nullptr);
    GCTX.sql_proxy_ = old_proxy_;
  }
  int run() { return executor_.do_rebuild_with_retry(); }
  ObArenaAllocator allocator_;
  ObSQLSessionInfo session_;
  ObExecContext ctx_;
  ObMySQLProxy proxy_;
  ObMySQLProxy *old_proxy_ = nullptr;
  ObDBMSSchedJobInfo job_;
  ObVectorRefreshIndexExecutor executor_;
};

TEST_F(RebuildJobRetryLoopTest, entry_error_retries_before_refresh)
{
  checks = {{OB_EAGAIN, false}, {OB_SUCCESS, false}};
  refreshes = {{OB_SUCCESS, false}};
  EXPECT_EQ(OB_SUCCESS, run());
  EXPECT_EQ((std::vector<int64_t>{0, 1}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, entry_errors_exhaust_original_budget)
{
  checks = {{OB_EAGAIN, false}, {OB_EAGAIN, false}, {OB_EAGAIN, false}, {OB_EAGAIN, false}};
  EXPECT_EQ(OB_EAGAIN, run());
  EXPECT_EQ((std::vector<int64_t>{0, 1, 2, 3}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, inherited_budget_is_not_reloaded_before_handoff)
{
  job_.job_config_ = ObString::make_string("{\"vector_rebuild_retry_count\":2}");
  checks = {{OB_EAGAIN, false}, {OB_SUCCESS, true}};
  EXPECT_EQ(OB_SUCCESS, run());
  EXPECT_EQ((std::vector<int64_t>{2, 3}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, exhausted_child_does_not_retry_query_error)
{
  job_.job_config_ = ObString::make_string("{\"vector_rebuild_retry_count\":3}");
  checks = {{OB_EAGAIN, false}};
  EXPECT_EQ(OB_EAGAIN, run());
  EXPECT_EQ((std::vector<int64_t>{3}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, admission_recheck_error_retries_through_entry)
{
  checks = {{OB_SUCCESS, false}, {OB_EAGAIN, false}, {OB_SUCCESS, false}};
  refreshes = {{OB_ERR_PARALLEL_DDL_CONFLICT, true}, {OB_SUCCESS, false}};
  EXPECT_EQ(OB_SUCCESS, run());
  EXPECT_EQ((std::vector<int64_t>{0, 0, 1}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, last_attempt_can_defer_after_admission_race)
{
  job_.job_config_ = ObString::make_string("{\"vector_rebuild_retry_count\":3}");
  checks = {{OB_SUCCESS, false}, {OB_SUCCESS, true}};
  refreshes = {{OB_ERR_PARALLEL_DDL_CONFLICT, true}};
  EXPECT_EQ(OB_SUCCESS, run());
  EXPECT_EQ((std::vector<int64_t>{3, 3}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, terminal_recheck_errors_are_not_replaced_with_conflict)
{
  for (int error : {OB_TIMEOUT, OB_CANCELED, OB_ENTRY_NOT_EXIST, OB_INVALID_ARGUMENT,
                    OB_ALLOCATE_MEMORY_FAILED, OB_ERR_UNEXPECTED}) {
    checks = {{OB_SUCCESS, false}, {error, false}};
    refreshes = {{OB_ERR_PARALLEL_DDL_CONFLICT, true}};
    budgets.clear();
    refresh_calls = 0;
    EXPECT_EQ(error, run());
    EXPECT_EQ((std::vector<int64_t>{0, 0}), budgets);
    EXPECT_EQ(1, refresh_calls);
  }
}

TEST_F(RebuildJobRetryLoopTest, vanished_conflict_keeps_original_retry_error)
{
  checks = {{OB_SUCCESS, false}, {OB_SUCCESS, false}, {OB_SUCCESS, false}};
  refreshes = {{OB_ERR_PARALLEL_DDL_CONFLICT, true}, {OB_SUCCESS, false}};
  EXPECT_EQ(OB_SUCCESS, run());
  EXPECT_EQ((std::vector<int64_t>{0, 0, 1}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, persisted_ddl_failure_consumes_budget_before_deferral)
{
  checks = {{OB_SUCCESS, false}, {OB_SUCCESS, true}};
  refreshes = {{OB_ERR_PARALLEL_DDL_CONFLICT, false}};
  EXPECT_EQ(OB_SUCCESS, run());
  EXPECT_EQ((std::vector<int64_t>{0, 1}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, entry_timeout_does_not_retry_or_refresh)
{
  checks = {{OB_TIMEOUT, false}};
  EXPECT_EQ(OB_TIMEOUT, run());
  EXPECT_EQ((std::vector<int64_t>{0}), budgets);
}

TEST_F(RebuildJobRetryLoopTest, invalid_budget_stops_before_check)
{
  job_.job_config_ = ObString::make_string("{\"vector_rebuild_retry_count\":4}");
  EXPECT_EQ(OB_INVALID_ARGUMENT, run());
}

TEST_F(RebuildJobRetryLoopTest, expired_execution_stops_before_check)
{
  ctx_.get_physical_plan_ctx()->set_timeout_timestamp(ObTimeUtility::current_time() - 1);
  EXPECT_EQ(OB_TIMEOUT, run());
}

TEST_F(RebuildJobRetryLoopTest, manual_inner_call_retains_bounded_retries)
{
  session_.set_job_info(nullptr);
  refreshes = {{OB_EAGAIN, false}, {OB_EAGAIN, false}, {OB_EAGAIN, false}, {OB_EAGAIN, false}};
  EXPECT_EQ(OB_EAGAIN, run());
}
} // namespace unittest

namespace storage
{
int ObVectorIndexSchedJobUtils::check_and_defer_rebuild_job(
    common::ObISQLClient &, const uint64_t index_id, const uint64_t table_id,
    dbms_scheduler::ObDBMSSchedJobInfo &job, const int64_t retry_cnt, bool &deferred)
{
  using namespace unittest;
  EXPECT_EQ(1004, job.tenant_id_);
  EXPECT_EQ(500002, index_id);
  EXPECT_EQ(500001, table_id);
  const size_t pos = budgets.size();
  budgets.push_back(retry_cnt);
  deferred = false;
  if (pos >= checks.size()) {
    ADD_FAILURE() << "unexpected conflict check";
    return common::OB_ERR_UNEXPECTED;
  }
  deferred = checks[pos].deferred_;
  return checks[pos].ret_;
}

int ObVectorIndexRefresher::refresh()
{
  using namespace unittest;
  EXPECT_FALSE(refresh_ctx_->rebuild_admission_rejected_);
  const size_t pos = refresh_calls++;
  if (pos >= refreshes.size()) {
    ADD_FAILURE() << "unexpected rebuild execution";
    return common::OB_ERR_UNEXPECTED;
  }
  refresh_ctx_->rebuild_admission_rejected_ = refreshes[pos].admission_rejected_;
  return refreshes[pos].ret_;
}
} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
