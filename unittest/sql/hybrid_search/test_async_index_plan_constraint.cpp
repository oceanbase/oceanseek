/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <cstring>
#include <gtest/gtest.h>
#include "lib/worker.h"
#include "../test_sql_utils.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_init.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/hybrid_search/ob_hybrid_search_plan_cache.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/plan_cache/ob_pcv_set.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_plan_set.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_evolution_plan.h"
#endif
#undef protected
#undef private

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace test;

class TestAsyncIndexPlanConstraint : public TestSqlUtils, public ::testing::Test
{
protected:
  void SetUp() override
  {
    memset(schema_file_path_, '\0', sizeof(schema_file_path_));
    TestSqlUtils::init();
    ASSERT_NE(nullptr, exec_ctx_.get_physical_plan_ctx());
    exec_ctx_.get_physical_plan_ctx()->set_original_param_cnt(0);
    ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));
  }

  void TearDown() override
  {
    session_info_.get_tx_desc() = nullptr;
    TestSqlUtils::destroy();
  }

  void init_cache_table(ObTableSchema &table_schema, const char *table_name,
                        const bool has_async_index)
  {
    table_schema.set_tenant_id(OB_SYS_TENANT_ID);
    table_schema.set_database_id(OB_SYS_DATABASE_ID);
    table_schema.set_table_type(USER_TABLE);
    table_schema.set_index_type(INDEX_TYPE_IS_NOT);
    table_schema.set_rowkey_column_num(1);
    table_schema.set_max_used_column_id(OB_APP_MIN_COLUMN_ID);
    ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name(table_name));
    table_schema.set_table_id(get_next_table_id(OB_SYS_TENANT_ID));
    table_schema.set_tablet_id(get_next_tablet_id(OB_SYS_TENANT_ID));
    if (has_async_index) {
      table_schema.add_table_flag(HAS_ASYNC_INDEX_FLAG);
    }
    ObColumnSchemaV2 column;
    column.set_tenant_id(OB_SYS_TENANT_ID);
    column.set_table_id(table_schema.get_table_id());
    column.set_column_id(OB_APP_MIN_COLUMN_ID);
    column.set_data_type(ObIntType);
    column.set_data_length(static_cast<int64_t>(sizeof(int64_t)));
    column.set_rowkey_position(1);
    column.set_nullable(false);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name("id"));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
    ASSERT_EQ(OB_SUCCESS, add_table_schema(table_schema));
  }

  void check_plan_admission(ObPhysicalPlan &plan, ObPlanCacheCtx &pc_ctx,
                            const bool expected)
  {
    ObSql sql;
    ObResultSet result(session_info_, allocator_);
    result.set_exec_context(exec_ctx_);
    // The result only borrows this stack plan for the admission check.
    result.get_cache_obj_guard().cache_obj_ = &plan;
    bool need_add = true;
    EXPECT_EQ(OB_SUCCESS, sql.need_add_plan(pc_ctx, result, true, need_add));
    EXPECT_EQ(expected, need_add);
    result.get_cache_obj_guard().cache_obj_ = nullptr;
  }

  void set_current_constraints(ObSqlCtx &sql_ctx, ObQueryCtx &query_ctx)
  {
    sql_ctx.all_plan_const_param_constraints_ = &query_ctx.all_plan_const_param_constraints_;
    sql_ctx.all_equal_param_constraints_ = &query_ctx.all_equal_param_constraints_;
    sql_ctx.all_priv_constraints_ = &query_ctx.all_priv_constraints_;
    sql_ctx.all_pre_calc_constraints_ = &query_ctx.all_pre_calc_constraints_;
  }

  int init_cached_set(ObSqlPlanSet &plan_set, ObPlanCacheCtx &pc_ctx)
  {
    int ret = OB_SUCCESS;
    ObPlanCache plan_cache;
    lib::MemoryContext mem_context = CURRENT_CONTEXT;
    ObPCVSet pcv_set(&plan_cache, mem_context);
    ObPlanCacheValue cache_value(1);
    ObPhysicalPlan plan;
    plan_cache.set_tenant_id(OB_SYS_TENANT_ID);
    cache_value.pcv_set_ = &pcv_set;
    plan_set.plan_cache_value_ = &cache_value;
    // Exercise the owning base initializer without publishing a physical plan.
    if (OB_FAIL(plan_set.ObPlanSet::init_new_set(pc_ctx, plan, OB_INVALID_INDEX,
                                                &allocator_))) {
      LOG_WARN("failed to initialize cached constraint snapshot", K(ret));
    }
    plan_set.plan_cache_value_ = nullptr;
    return ret;
  }

  void check_lookup(ObSqlPlanSet &plan_set, ObPlanCacheCtx &pc_ctx,
                    bool expected, int expected_ret = OB_SUCCESS)
  {
    ParamStore params((ObWrapperAllocator(pc_ctx.allocator_)));
    bool matched = !expected;
    EXPECT_EQ(expected_ret,
              plan_set.match_params_info(&params, pc_ctx, OB_INVALID_INDEX, matched));
    EXPECT_EQ(expected, matched);
    matched = !expected;
    EXPECT_EQ(expected_ret,
              plan_set.match_params_info(nullptr, pc_ctx, OB_INVALID_INDEX, matched));
    EXPECT_EQ(expected, matched);
  }

  void check_session_state_constraints_for_sessions(
      ObSqlPlanSet &plan_set, ObPlanCacheCtx &pc_ctx, ObSQLSessionInfo &session)
  {
    EXPECT_EQ(OB_SUCCESS, session.set_autocommit(true));
    session.get_tx_desc() = nullptr;
    check_lookup(plan_set, pc_ctx, true);
    tx_desc_.flags_.EXPLICIT_ = true;
    session.get_tx_desc() = &tx_desc_;
    check_lookup(plan_set, pc_ctx, false);
    tx_desc_.flags_.EXPLICIT_ = false;
    check_lookup(plan_set, pc_ctx, true);
    session.get_tx_desc() = nullptr;
    EXPECT_EQ(OB_SUCCESS, session.set_autocommit(false));
    check_lookup(plan_set, pc_ctx, false);
    EXPECT_EQ(OB_SUCCESS, session.set_autocommit(true));
    check_lookup(plan_set, pc_ctx, true);
  }

  void check_second_lookup(ObPlanCacheValue &cache_value, ObSqlPlanSet &plan_set,
                           ObPlanCacheCtx &pc_ctx, bool expected)
  {
    ParamStore params((ObWrapperAllocator(pc_ctx.allocator_)));
    bool matched = !expected;
    pc_ctx.fp_result_.cache_params_ = &params;
    EXPECT_EQ(OB_SUCCESS, cache_value.match_all_params_info(
                             &plan_set, pc_ctx, OB_INVALID_INDEX, matched));
    EXPECT_EQ(expected, matched);
    pc_ctx.fp_result_.cache_params_ = nullptr;
    matched = !expected;
    EXPECT_EQ(OB_SUCCESS, cache_value.match_all_params_info(
                             &plan_set, pc_ctx, OB_INVALID_INDEX, matched));
    EXPECT_EQ(expected, matched);
  }

#ifdef OB_BUILD_SPM
  void check_spm_baseline_session_state_constraint_gate(
      const uint64_t source_mask, const uint64_t baseline_mask)
  {
    ObSqlPlanSet source_set(1);
    ObSqlPlanSet safe_set(2);
    ObPlanCacheValue cache_value(1);
    ObPhysicalPlan source_plan;
    ObPhysicalPlan baseline_plan;
    PreCalcExprHandler source_handler;
    PreCalcExprHandler baseline_handler;
    ObQueryCtx query_ctx(allocator_);
    ObPreCalcExprConstraint cached_cons(allocator_);
    ObPreCalcExprConstraint current_cons(allocator_);
    ObDList<ObPreCalcExprConstraint> cached_cons_list;
    ObPCPrivInfo cached_priv;
    ObPCPrivInfo current_priv;
    ObSEArray<ObPCPrivInfo, 1> cached_priv_list;
    ObSEArray<PCVSchemaObj, 1> schema_array;
    cached_cons.expect_result_ = PRE_CALC_RESULT_TRUE;
    current_cons.expect_result_ = PRE_CALC_RESULT_FALSE;
    ASSERT_TRUE(cached_cons_list.add_last(&cached_cons));
    ASSERT_EQ(OB_SUCCESS, source_set.set_pre_calc_constraint(cached_cons_list));
    ASSERT_TRUE(query_ctx.all_pre_calc_constraints_.add_last(&current_cons));
    cached_priv.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
    cached_priv.owner_user_id_ = 1;
    current_priv.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
    current_priv.owner_user_id_ = 2;
    ASSERT_EQ(OB_SUCCESS, cached_priv_list.push_back(cached_priv));
    ASSERT_EQ(OB_SUCCESS, source_set.set_priv_constraint(cached_priv_list));
    ASSERT_EQ(OB_SUCCESS, query_ctx.all_priv_constraints_.push_back(current_priv));
    ASSERT_EQ(OB_SUCCESS, source_set.local_evolution_plan_.init(&source_set));
    source_set.session_state_constraints_ = source_mask;
    sql_ctx_.session_state_constraints_ = baseline_mask;
    set_current_constraints(sql_ctx_, query_ctx);
    sql_ctx_.res_map_rule_version_ = 0;
    ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                          allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
    ObSpmCacheCtx &spm_ctx = sql_ctx_.spm_ctx_;
    spm_ctx.reset();
    spm_ctx.spm_stat_ = ObSpmCacheCtx::STAT_ADD_BASELINE_PLAN;
    spm_ctx.evolution_plan_type_ = OB_PHY_PLAN_LOCAL;
    spm_ctx.evo_plan_set_ = &source_set;
    spm_ctx.evolution_plan_ = &source_set.local_evolution_plan_;
    pc_ctx.need_evolution_ = true;
    source_plan.set_evolution(true);
    source_plan.plan_set_id_ = source_set.id_;
    baseline_plan.plan_set_id_ = 73;
    source_set.local_evolution_plan_.evolving_plan_ = &source_plan;
    source_handler.inc_ref_cnt();
    source_set.pre_cal_expr_handler_ = &source_handler;
    source_plan.stat_.pre_cal_expr_handler_ = &source_handler;
    baseline_plan.stat_.pre_cal_expr_handler_ = &baseline_handler;
    EXPECT_TRUE(cache_value.plan_sets_.add_last(&source_set));

    EXPECT_EQ(OB_SQL_PC_PLAN_DUPLICATE, cache_value.add_plan(baseline_plan, schema_array, pc_ctx));
    EXPECT_TRUE(spm_ctx.evolution_task_in_two_plan_set_);
    EXPECT_EQ(1, cache_value.plan_sets_.get_size());
    EXPECT_EQ(&source_set, cache_value.plan_sets_.get_first());
    EXPECT_EQ(&source_set, spm_ctx.evo_plan_set_);
    EXPECT_EQ(&source_set.local_evolution_plan_, spm_ctx.evolution_plan_);
    EXPECT_EQ(&source_set, source_set.local_evolution_plan_.plan_set_);
    EXPECT_EQ(&source_plan, source_set.local_evolution_plan_.evolving_plan_);
    EXPECT_TRUE(source_set.local_evolution_plan_.baseline_plans_.empty());
    EXPECT_FALSE(source_set.local_evolution_plan_.get_is_evolving_flag());
    EXPECT_FALSE(spm_ctx.finish_start_evolution_);
    EXPECT_EQ(nullptr, spm_ctx.evo_plan_guard_.get_cache_obj());
    EXPECT_EQ(source_mask, source_set.get_session_state_constraints());
    EXPECT_EQ(baseline_mask, sql_ctx_.session_state_constraints_);
    EXPECT_EQ(source_set.id_, source_plan.plan_set_id_);
    EXPECT_EQ(73UL, baseline_plan.plan_set_id_);
    EXPECT_EQ(1, source_set.all_pre_calc_constraints_.get_size());
    EXPECT_EQ(1, source_set.all_priv_constraints_.count());
    EXPECT_EQ(1, query_ctx.all_pre_calc_constraints_.get_size());
    EXPECT_EQ(1, query_ctx.all_priv_constraints_.count());
    EXPECT_EQ(PRE_CALC_RESULT_FALSE, current_cons.expect_result_);
    EXPECT_EQ(2UL, query_ctx.all_priv_constraints_.at(0).owner_user_id_);
    EXPECT_EQ(&source_handler, source_set.pre_cal_expr_handler_);
    EXPECT_EQ(&source_handler, source_plan.stat_.pre_cal_expr_handler_);
    EXPECT_EQ(&baseline_handler, baseline_plan.stat_.pre_cal_expr_handler_);
    EXPECT_EQ(2, source_plan.get_pre_expr_ref_count());
    EXPECT_EQ(1, baseline_plan.get_pre_expr_ref_count());
    EXPECT_EQ(0, source_plan.get_ref_count());
    EXPECT_EQ(0, baseline_plan.get_ref_count());
    EXPECT_TRUE(source_plan.get_evolution());
    EXPECT_FALSE(baseline_plan.get_evolution());

    // A subsequent ordinary request can publish safe B in a set without session state constraints.
    spm_ctx.reset();
    pc_ctx.need_evolution_ = false;
    sql_ctx_.session_state_constraints_ = NO_SESSION_STATE_CONSTRAINT;
    if (NO_SESSION_STATE_CONSTRAINT == baseline_mask) {
      EXPECT_EQ(OB_SUCCESS, safe_set.add_physical_plan(OB_PHY_PLAN_LOCAL, pc_ctx, baseline_plan));
      EXPECT_EQ(1, safe_set.local_plans_.count());
      if (1 == safe_set.local_plans_.count()) {
        EXPECT_EQ(&baseline_plan, safe_set.local_plans_.at(0));
      }
      EXPECT_EQ(&baseline_plan, safe_set.direct_local_plan_);
      EXPECT_EQ(NO_SESSION_STATE_CONSTRAINT, safe_set.get_session_state_constraints());
      EXPECT_EQ(&source_plan, source_set.local_evolution_plan_.evolving_plan_);
      EXPECT_EQ(&source_set, source_set.local_evolution_plan_.plan_set_);
      EXPECT_EQ(source_set.id_, source_plan.plan_set_id_);
    }
    safe_set.local_plans_.reset();
    safe_set.direct_local_plan_ = nullptr;
    // Neither the cache value nor a guard may take ownership of these stack objects.
    cache_value.plan_sets_.remove(&source_set);
    source_set.local_evolution_plan_.reset();
    source_set.pre_cal_expr_handler_ = nullptr;
    source_plan.stat_.pre_cal_expr_handler_ = nullptr;
    baseline_plan.stat_.pre_cal_expr_handler_ = nullptr;
    sql_ctx_.all_pre_calc_constraints_ = nullptr;
    sql_ctx_.all_priv_constraints_ = nullptr;
    sql_ctx_.all_plan_const_param_constraints_ = nullptr;
    sql_ctx_.all_equal_param_constraints_ = nullptr;
  }

  void check_spm_transfer_session_state_constraint_gate(
      const ObPhyPlanType plan_type, const uint64_t source_mask, const uint64_t destination_mask)
  {
    ObSqlPlanSet source_set(1);
    ObSqlPlanSet destination_set(2);
    ObPhysicalPlan source_plan;
    ObPhysicalPlan baseline_plan;
    ObPhysicalPlan ordinary_plan;
    ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                          allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
    ObSpmCacheCtx &spm_ctx = sql_ctx_.spm_ctx_;
    ObEvolutionPlan &source_evolution = OB_PHY_PLAN_LOCAL == plan_type
        ? source_set.local_evolution_plan_ : source_set.dist_evolution_plan_;
    ObEvolutionPlan &destination_evolution = OB_PHY_PLAN_LOCAL == plan_type
        ? destination_set.local_evolution_plan_ : destination_set.dist_evolution_plan_;
    ASSERT_EQ(OB_SUCCESS, source_evolution.init(&source_set));
    ASSERT_EQ(OB_SUCCESS, destination_evolution.init(&destination_set));
    ASSERT_EQ(OB_SUCCESS, destination_set.dist_plans_.init(&destination_set));
    ASSERT_EQ(OB_SUCCESS, destination_set.local_plans_.push_back(&ordinary_plan));
    ASSERT_EQ(OB_SUCCESS, destination_set.dist_plans_.dist_plans_.push_back(&ordinary_plan));
    destination_set.direct_local_plan_ = &ordinary_plan;
    ordinary_plan.stat_.execute_times_ = 17;
    source_set.session_state_constraints_ = source_mask;
    destination_set.session_state_constraints_ = destination_mask;
    source_plan.set_plan_type(plan_type);
    baseline_plan.set_plan_type(plan_type);
    source_plan.set_evolution(true);
    source_plan.plan_set_id_ = source_set.id_;
    source_evolution.evolving_plan_ = &source_plan;
    spm_ctx.reset();
    spm_ctx.spm_stat_ = ObSpmCacheCtx::STAT_ADD_BASELINE_PLAN;
    spm_ctx.evolution_plan_type_ = plan_type;
    spm_ctx.evolution_plan_ = &source_evolution;
    spm_ctx.evo_plan_set_ = &source_set;
    pc_ctx.need_evolution_ = true;

    EXPECT_EQ(OB_SQL_PC_PLAN_DUPLICATE,
              destination_set.add_physical_plan(plan_type, pc_ctx, baseline_plan));
    EXPECT_TRUE(spm_ctx.evolution_task_in_two_plan_set_);
    EXPECT_EQ(&source_plan, source_evolution.evolving_plan_);
    EXPECT_EQ(nullptr, destination_evolution.evolving_plan_);
    EXPECT_EQ(&source_set, source_evolution.plan_set_);
    EXPECT_EQ(&destination_set, destination_evolution.plan_set_);
    EXPECT_EQ(&source_evolution, spm_ctx.evolution_plan_);
    EXPECT_EQ(&source_set, spm_ctx.evo_plan_set_);
    EXPECT_EQ(source_mask, source_set.get_session_state_constraints());
    EXPECT_EQ(destination_mask, destination_set.get_session_state_constraints());
    EXPECT_TRUE(source_evolution.baseline_plans_.empty());
    EXPECT_TRUE(destination_evolution.baseline_plans_.empty());
    EXPECT_FALSE(source_evolution.get_is_evolving_flag());
    EXPECT_FALSE(destination_evolution.get_is_evolving_flag());
    EXPECT_FALSE(spm_ctx.finish_start_evolution_);
    EXPECT_EQ(nullptr, destination_evolution.evolution_records_);
    EXPECT_TRUE(source_plan.get_evolution());
    EXPECT_FALSE(baseline_plan.get_evolution());
    EXPECT_EQ(1, destination_set.local_plans_.count());
    if (1 == destination_set.local_plans_.count()) {
      EXPECT_EQ(&ordinary_plan, destination_set.local_plans_.at(0));
    }
    EXPECT_EQ(&ordinary_plan, destination_set.direct_local_plan_);
    EXPECT_EQ(1, destination_set.dist_plans_.count());
    if (1 == destination_set.dist_plans_.count()) {
      EXPECT_EQ(&ordinary_plan, destination_set.dist_plans_.dist_plans_.at(0));
    }
    EXPECT_EQ(17, ordinary_plan.stat_.execute_times_);
    EXPECT_EQ(ObILibCacheObject::ACTIVE, ordinary_plan.get_obj_status());
    EXPECT_EQ(0, ordinary_plan.get_ref_count());
    EXPECT_EQ(0, source_plan.get_ref_count());
    EXPECT_EQ(0, baseline_plan.get_ref_count());
    EXPECT_EQ(source_set.id_, source_plan.plan_set_id_);
    spm_ctx.reset();
    source_evolution.reset();
    destination_evolution.reset();
    destination_set.local_plans_.reset();
    destination_set.direct_local_plan_ = nullptr;
    destination_set.dist_plans_.reset();
  }

  void check_spm_same_session_state_constraint_transfer(
      const ObPhyPlanType plan_type, const uint64_t session_state_constraint_mask)
  {
    ObSqlPlanSet source_set(1);
    ObSqlPlanSet destination_set(2);
    ObPhysicalPlan source_plan;
    ObPhysicalPlan baseline_plan;
    ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                          allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
    ObSpmCacheCtx &spm_ctx = sql_ctx_.spm_ctx_;
    ObEvolutionPlan &source_evolution = OB_PHY_PLAN_LOCAL == plan_type
        ? source_set.local_evolution_plan_ : source_set.dist_evolution_plan_;
    ObEvolutionPlan &destination_evolution = OB_PHY_PLAN_LOCAL == plan_type
        ? destination_set.local_evolution_plan_ : destination_set.dist_evolution_plan_;
    ASSERT_EQ(OB_SUCCESS, source_evolution.init(&source_set));
    ASSERT_EQ(OB_SUCCESS, destination_evolution.init(&destination_set));
    source_set.session_state_constraints_ = session_state_constraint_mask;
    destination_set.session_state_constraints_ = session_state_constraint_mask;
    source_plan.set_plan_type(plan_type);
    baseline_plan.set_plan_type(plan_type);
    source_plan.set_evolution(true);
    source_evolution.evolving_plan_ = &source_plan;
    spm_ctx.reset();
    spm_ctx.spm_stat_ = ObSpmCacheCtx::STAT_ADD_BASELINE_PLAN;
    spm_ctx.evolution_plan_type_ = plan_type;
    spm_ctx.evolution_plan_ = &source_evolution;
    spm_ctx.evo_plan_set_ = &source_set;

    EXPECT_EQ(OB_SUCCESS, destination_evolution.swap_evolution_plan(pc_ctx, source_evolution));
    EXPECT_EQ(nullptr, source_evolution.evolving_plan_);
    EXPECT_EQ(&source_plan, destination_evolution.evolving_plan_);
    EXPECT_EQ(&source_set, source_evolution.plan_set_);
    EXPECT_EQ(&destination_set, destination_evolution.plan_set_);
    EXPECT_EQ(&destination_evolution, spm_ctx.evolution_plan_);
    EXPECT_EQ(session_state_constraint_mask, source_set.get_session_state_constraints());
    EXPECT_EQ(session_state_constraint_mask, destination_set.get_session_state_constraints());
    EXPECT_FALSE(spm_ctx.evolution_task_in_two_plan_set_);
    EXPECT_EQ(OB_SUCCESS, destination_evolution.add_baseline_plan(pc_ctx, baseline_plan));
    EXPECT_EQ(OB_SUCCESS, destination_evolution.start_evolution_plan(spm_ctx));
    EXPECT_EQ(&source_plan, destination_evolution.evolving_plan_);
    EXPECT_EQ(1, destination_evolution.baseline_plans_.count());
    if (1 == destination_evolution.baseline_plans_.count()) {
      EXPECT_EQ(&baseline_plan, destination_evolution.baseline_plans_.at(0));
    }
    EXPECT_TRUE(source_plan.get_evolution());
    EXPECT_TRUE(baseline_plan.get_evolution());
    EXPECT_TRUE(destination_evolution.get_is_evolving_flag());
    EXPECT_TRUE(spm_ctx.finish_start_evolution_);
    EXPECT_NE(nullptr, destination_evolution.evolution_records_);
    EXPECT_EQ(destination_evolution.evolution_records_, source_plan.stat_.evolution_stat_.records_);
    EXPECT_EQ(destination_evolution.evolution_records_,
              baseline_plan.stat_.evolution_stat_.records_);
    // Reset records while both stack plans and the allocating owner are still alive.
    destination_evolution.reset();
    source_evolution.reset();
    EXPECT_EQ(nullptr, source_plan.stat_.evolution_stat_.records_);
    EXPECT_EQ(nullptr, baseline_plan.stat_.evolution_stat_.records_);
    spm_ctx.reset();
  }
#endif

  transaction::ObTxDesc tx_desc_;
};

TEST_F(TestAsyncIndexPlanConstraint, hybrid_transaction_plans_on_async_tables_are_not_cached)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObTableSchema table_schema;
  ASSERT_NO_FATAL_FAILURE(init_cache_table(table_schema, "txn_cache_async", true));
  ASSERT_EQ(OB_SUCCESS, schema_service_->get_schema_guard(schema_guard_));
  ObPhysicalPlan plan;
  ASSERT_EQ(OB_SUCCESS, plan.init_dependency_table_store(1));
  ASSERT_EQ(OB_SUCCESS, plan.get_dependency_table().push_back(ObSchemaObjVersion(
      table_schema.get_table_id(), table_schema.get_schema_version(), DEPENDENCY_TABLE)));
  // A fallback plan has a base-table dependency, but no async scan or transaction mask.
  EXPECT_EQ(NO_SESSION_STATE_CONSTRAINT, sql_ctx_.session_state_constraints_);
  ObPlanCacheCtx pc_ctx(ObString::make_string("select id from hybrid_search(table txn_cache_async, '{}')"),
                        PC_TEXT_MODE, allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  ObHybridDslRequestInfo request(allocator_);
  request.set_sql_raw_param_count(0);
  request.set_params_bound(true);
  pc_ctx.fp_result_.hybrid_dsl_request_ = &request;
  stmt_factory_.get_query_ctx()->has_hybrid_search_ = true;
  check_plan_admission(plan, pc_ctx, true);
  tx_desc_.flags_.EXPLICIT_ = true;
  session_info_.get_tx_desc() = &tx_desc_;
  check_plan_admission(plan, pc_ctx, false);
  // An implicit autocommit transaction does not exclude async candidates.
  tx_desc_.flags_.EXPLICIT_ = false;
  check_plan_admission(plan, pc_ctx, true);
  session_info_.get_tx_desc() = nullptr;
  check_plan_admission(plan, pc_ctx, true);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(false));
  check_plan_admission(plan, pc_ctx, false);
  tx_desc_.flags_.EXPLICIT_ = true;
  session_info_.get_tx_desc() = &tx_desc_;
  check_plan_admission(plan, pc_ctx, false);
  // COMMIT/ROLLBACK does not remove the autocommit=0 restriction.
  session_info_.get_tx_desc() = nullptr;
  check_plan_admission(plan, pc_ctx, false);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));
  check_plan_admission(plan, pc_ctx, true);
}

TEST_F(TestAsyncIndexPlanConstraint, hybrid_plan_admission_checks_all_base_table_dependencies)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObTableSchema plain_table;
  ObTableSchema async_table;
  ASSERT_NO_FATAL_FAILURE(init_cache_table(plain_table, "cache_plain", false));
  ASSERT_NO_FATAL_FAILURE(init_cache_table(async_table, "cache_async", true));
  ASSERT_EQ(OB_SUCCESS, schema_service_->get_schema_guard(schema_guard_));
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(false));
  ObPlanCacheCtx pc_ctx(ObString::make_string("select id from hybrid_search(table cache_plain, '{}')"),
                        PC_TEXT_MODE, allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  ObHybridDslRequestInfo request(allocator_);
  request.set_sql_raw_param_count(0);
  request.set_params_bound(true);
  pc_ctx.fp_result_.hybrid_dsl_request_ = &request;
  ObQueryCtx *query_ctx = stmt_factory_.get_query_ctx();
  query_ctx->has_hybrid_search_ = true;
  ObPhysicalPlan plan;
  ASSERT_EQ(OB_SUCCESS, plan.init_dependency_table_store(3));
  check_plan_admission(plan, pc_ctx, true);
  ASSERT_EQ(OB_SUCCESS, plan.get_dependency_table().push_back(ObSchemaObjVersion(
      plain_table.get_table_id(), plain_table.get_schema_version(), DEPENDENCY_TABLE)));
  check_plan_admission(plan, pc_ctx, true);
  // Non-table dependencies must not be looked up as table schemas.
  ASSERT_EQ(OB_SUCCESS, plan.get_dependency_table().push_back(ObSchemaObjVersion(
      OB_INVALID_ID, 1, DEPENDENCY_FUNCTION)));
  check_plan_admission(plan, pc_ctx, true);
  // A later async-index table is enough to prevent caching the complete hybrid plan.
  ASSERT_EQ(OB_SUCCESS, plan.get_dependency_table().push_back(ObSchemaObjVersion(
      async_table.get_table_id(), async_table.get_schema_version(), DEPENDENCY_TABLE)));
  check_plan_admission(plan, pc_ctx, false);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));
  check_plan_admission(plan, pc_ctx, true);
  // Hybrid admission must not bypass other SQL admission conditions.
  query_ctx->set_has_dynamic_partition_tier(true);
  check_plan_admission(plan, pc_ctx, false);
  query_ctx->set_has_dynamic_partition_tier(false);
  query_ctx->set_has_odps_external_table(true);
  check_plan_admission(plan, pc_ctx, false);
  query_ctx->set_has_odps_external_table(false);
  check_plan_admission(plan, pc_ctx, true);
}

TEST_F(TestAsyncIndexPlanConstraint, ordinary_sql_is_unchanged_and_hybrid_still_requires_bound_dsl)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObTableSchema table_schema;
  ASSERT_NO_FATAL_FAILURE(init_cache_table(table_schema, "plain_sql_async", true));
  ASSERT_EQ(OB_SUCCESS, schema_service_->get_schema_guard(schema_guard_));
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(false));
  ObPhysicalPlan plan;
  ASSERT_EQ(OB_SUCCESS, plan.init_dependency_table_store(1));
  ASSERT_EQ(OB_SUCCESS, plan.get_dependency_table().push_back(ObSchemaObjVersion(
      table_schema.get_table_id(), table_schema.get_schema_version(), DEPENDENCY_TABLE)));
  for (int64_t i = 0; i < 2; ++i) {
    ObPlanCacheCtx pc_ctx(ObString::make_string("select id from plain_sql_async"),
        0 == i ? PC_TEXT_MODE : PC_PS_MODE, allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
    check_plan_admission(plan, pc_ctx, true);
  }
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));
  ObPlanCacheCtx pc_ctx(ObString::make_string("select id from hybrid_search(table plain_sql_async, '{}')"),
                        PC_TEXT_MODE, allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  stmt_factory_.get_query_ctx()->has_hybrid_search_ = true;
  check_plan_admission(plan, pc_ctx, false);
  ObHybridDslRequestInfo request(allocator_);
  pc_ctx.fp_result_.hybrid_dsl_request_ = &request;
  request.set_sql_raw_param_count(0);
  check_plan_admission(plan, pc_ctx, false);
  request.set_params_bound(true);
  check_plan_admission(plan, pc_ctx, true);
}

TEST_F(TestAsyncIndexPlanConstraint, async_index_registration_uses_current_request)
{
  ObStaticEngineCG cg(CLUSTER_CURRENT_VERSION);
  ObTscCgService tsc_cg(cg);
  ObAddr addr;
  ObGlobalHint global_hint(allocator_);
  ObSqlCtx other_sql_ctx;
  ObExecContext other_exec_ctx(allocator_);
  ObOptimizerContext opt_ctx(&session_info_, nullptr, nullptr, nullptr, allocator_,
                             nullptr, addr, nullptr, global_hint, expr_factory_, nullptr, false);
  EXPECT_EQ(OB_ERR_UNEXPECTED, tsc_cg.add_async_index_plan_constraint());
  cg.opt_ctx_ = &opt_ctx;
  EXPECT_EQ(OB_ERR_UNEXPECTED, tsc_cg.add_async_index_plan_constraint());
  opt_ctx.exec_ctx_ = &other_exec_ctx;
  EXPECT_EQ(OB_ERR_UNEXPECTED, tsc_cg.add_async_index_plan_constraint());
  other_exec_ctx.set_sql_ctx(&other_sql_ctx);
  ASSERT_EQ(nullptr, opt_ctx.get_query_ctx());
  const uint64_t other_constraint = 1ULL << 1;
  sql_ctx_.session_state_constraints_ = other_constraint;
  other_sql_ctx.session_state_constraints_ = other_constraint;
  EXPECT_EQ(OB_SUCCESS, tsc_cg.add_async_index_plan_constraint());
  EXPECT_EQ(other_constraint | SESSION_STATE_NO_MULTI_STMT_TRANSACTION,
            other_sql_ctx.session_state_constraints_);
  EXPECT_EQ(OB_SUCCESS, tsc_cg.add_async_index_plan_constraint());
  EXPECT_EQ(other_constraint | SESSION_STATE_NO_MULTI_STMT_TRANSACTION,
            other_sql_ctx.session_state_constraints_);
  EXPECT_EQ(other_constraint, sql_ctx_.session_state_constraints_);
}

TEST_F(TestAsyncIndexPlanConstraint, code_generate_invalid_arguments_reset_request_constraints)
{
  ObResultSet result(session_info_, allocator_);
  result.set_exec_context(exec_ctx_);
  ObStmtNeedPrivs stmt_need_privs(allocator_);
  ObStmtOraNeedPrivs stmt_ora_need_privs(allocator_);
  ObSEArray<ObAuditUnit, 1> audit_units;
  ObPhysicalPlan *phy_plan = nullptr;
  for (int64_t i = 0; i < 2; ++i) {
    sql_ctx_.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSql::code_generate(sql_ctx_, result, nullptr,
        stmt_need_privs, stmt_ora_need_privs, audit_units, nullptr, phy_plan));
    EXPECT_EQ(NO_SESSION_STATE_CONSTRAINT, sql_ctx_.session_state_constraints_);
  }
}

TEST_F(TestAsyncIndexPlanConstraint, current_request_session_state_constraints_and_null_params)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObSqlPlanSet plan_set(1);
  plan_set.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  EXPECT_FALSE(plan_set.can_skip_params_match());
  EXPECT_TRUE(plan_set.can_delay_init_datum_store());
  EXPECT_TRUE(plan_set.all_pre_calc_constraints_.is_empty());
  EXPECT_EQ(nullptr, plan_set.pre_cal_expr_handler_);
  ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                        allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  check_session_state_constraints_for_sessions(plan_set, pc_ctx, session_info_);
  exec_ctx_.set_my_session(nullptr);
  check_lookup(plan_set, pc_ctx, false, OB_ERR_UNEXPECTED);
  exec_ctx_.set_my_session(&session_info_);

  ObArenaAllocator other_allocator;
  ObSQLSessionInfo other_session;
  ObSqlCtx other_sql_ctx;
  ObExecContext other_exec_ctx(other_allocator);
  ASSERT_EQ(OB_SUCCESS, other_session.init_tenant(ObString::make_string("sql_test"),
                                                 OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, other_session.test_init(0, 0, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, other_session.load_default_sys_variable(true, true));
  other_sql_ctx.session_info_ = &other_session;
  other_sql_ctx.schema_guard_ = &schema_guard_;
  other_exec_ctx.set_sql_ctx(&other_sql_ctx);
  other_exec_ctx.set_my_session(&other_session);
  other_exec_ctx.get_task_executor_ctx()->set_min_cluster_version(CLUSTER_CURRENT_VERSION);
  ASSERT_EQ(OB_SUCCESS, other_exec_ctx.create_physical_plan_ctx());
  other_exec_ctx.get_physical_plan_ctx()->set_original_param_cnt(0);
  ObPlanCacheCtx other_pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                              other_allocator, other_sql_ctx, other_exec_ctx,
                              OB_SYS_TENANT_ID);
  // The original request session has a different state from this lookup request.
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(false));
  check_session_state_constraints_for_sessions(plan_set, other_pc_ctx, other_session);
  EXPECT_FALSE(session_info_.get_local_autocommit());
}

TEST_F(TestAsyncIndexPlanConstraint,
       session_state_constraint_match_does_not_require_parameter_context)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObSqlPlanSet plan_set(1);
  ObExecContext no_frame_ctx(allocator_);
  no_frame_ctx.set_sql_ctx(&sql_ctx_);
  ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                        allocator_, sql_ctx_, no_frame_ctx, OB_SYS_TENANT_ID);
  bool matched = false;
  ASSERT_EQ(nullptr, no_frame_ctx.get_physical_plan_ctx());
  EXPECT_EQ(OB_SUCCESS, plan_set.match_session_state_constraints(pc_ctx, matched));
  EXPECT_TRUE(matched);
  EXPECT_EQ(OB_SUCCESS, plan_set.match_params_info(nullptr, pc_ctx, OB_INVALID_INDEX, matched));
  EXPECT_TRUE(matched);
  plan_set.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  EXPECT_EQ(OB_ERR_UNEXPECTED, plan_set.match_session_state_constraints(pc_ctx, matched));
  EXPECT_FALSE(matched);
  no_frame_ctx.set_my_session(&session_info_);
  EXPECT_EQ(OB_SUCCESS, plan_set.match_session_state_constraints(pc_ctx, matched));
  EXPECT_TRUE(matched);
  EXPECT_EQ(OB_SUCCESS, plan_set.match_params_info(nullptr, pc_ctx, OB_INVALID_INDEX, matched));
  EXPECT_TRUE(matched);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(false));
  EXPECT_EQ(OB_SUCCESS, plan_set.match_params_info(nullptr, pc_ctx, OB_INVALID_INDEX, matched));
  EXPECT_FALSE(matched);
  plan_set.session_state_constraints_ |= 1ULL << 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, plan_set.match_session_state_constraints(pc_ctx, matched));
  EXPECT_FALSE(matched);
}

TEST_F(TestAsyncIndexPlanConstraint, context_snapshot_and_reset_lifetime)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObSqlPlanSet plan_set(1);
  EXPECT_EQ(NO_SESSION_STATE_CONSTRAINT, plan_set.session_state_constraints_);
  {
    ObArenaAllocator source_allocator;
    ObQueryCtx query_ctx(source_allocator);
    ObSqlCtx source_sql_ctx;
    EXPECT_EQ(NO_SESSION_STATE_CONSTRAINT, source_sql_ctx.session_state_constraints_);
    source_sql_ctx.session_state_constraints_ |= SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
    source_sql_ctx.session_state_constraints_ |= SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
    EXPECT_EQ(SESSION_STATE_NO_MULTI_STMT_TRANSACTION, source_sql_ctx.session_state_constraints_);
    EXPECT_TRUE(query_ctx.all_expr_constraints_.empty());
    set_current_constraints(source_sql_ctx, query_ctx);
    source_sql_ctx.session_info_ = &session_info_;
    source_sql_ctx.set_is_do_insert_batch_opt(2);
    ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                          source_allocator, source_sql_ctx, exec_ctx_, OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, init_cached_set(plan_set, pc_ctx));
    EXPECT_EQ(SESSION_STATE_NO_MULTI_STMT_TRANSACTION, plan_set.session_state_constraints_);
    EXPECT_FALSE(plan_set.get_can_skip_params_match());
    EXPECT_TRUE(plan_set.get_can_delay_init_datum_store());
    query_ctx.reset();
    source_sql_ctx.reset();
    EXPECT_EQ(NO_SESSION_STATE_CONSTRAINT, source_sql_ctx.session_state_constraints_);
    EXPECT_EQ(SESSION_STATE_NO_MULTI_STMT_TRANSACTION, plan_set.session_state_constraints_);
  }
  // The source contexts and their allocator no longer exist.
  ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                        allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  check_session_state_constraints_for_sessions(plan_set, pc_ctx, session_info_);
  plan_set.reset();
  EXPECT_EQ(NO_SESSION_STATE_CONSTRAINT, plan_set.session_state_constraints_);
  EXPECT_TRUE(plan_set.can_skip_params_match());
  EXPECT_TRUE(plan_set.can_delay_init_datum_store());
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(false));
  check_lookup(plan_set, pc_ctx, true);
}

TEST_F(TestAsyncIndexPlanConstraint, add_grouping_checks_complete_identity)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObSqlPlanSet async_set(1);
  ObSqlPlanSet safe_set(2);
  async_set.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  ObQueryCtx query_ctx(allocator_);
  set_current_constraints(sql_ctx_, query_ctx);
  ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                        allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  bool matched = true;
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    const bool need_match_cons = (0 == i);
    sql_ctx_.session_state_constraints_ = NO_SESSION_STATE_CONSTRAINT;
    ret = async_set.match_params_info(safe_set.params_info_, OB_INVALID_INDEX,
                                      pc_ctx, need_match_cons, matched);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_FALSE(matched);
    ret = safe_set.match_params_info(safe_set.params_info_, OB_INVALID_INDEX,
                                     pc_ctx, need_match_cons, matched);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_TRUE(matched);
    ASSERT_EQ(OB_SUCCESS, safe_set.match_and_merge_plan_cons(pc_ctx, matched));
    EXPECT_TRUE(matched);
    sql_ctx_.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
    ret = safe_set.match_params_info(async_set.params_info_, OB_INVALID_INDEX,
                                     pc_ctx, need_match_cons, matched);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_FALSE(matched);
    ret = async_set.match_params_info(async_set.params_info_, OB_INVALID_INDEX,
                                      pc_ctx, need_match_cons, matched);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_TRUE(matched);
    ASSERT_EQ(OB_SUCCESS, async_set.match_and_merge_plan_cons(pc_ctx, matched));
    EXPECT_TRUE(matched);
    sql_ctx_.session_state_constraints_ |= 1ULL << 1;
    ret = async_set.match_params_info(async_set.params_info_, OB_INVALID_INDEX,
                                      pc_ctx, need_match_cons, matched);
    ASSERT_EQ(OB_SUCCESS, ret);
    EXPECT_FALSE(matched);
  }
}

TEST_F(TestAsyncIndexPlanConstraint, spm_identity_precedes_unrelated_constraint_merge)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObSqlPlanSet plan_set(1);
  ObQueryCtx query_ctx(allocator_);
  ObPreCalcExprConstraint cached_cons(allocator_);
  ObPreCalcExprConstraint current_cons(allocator_);
  ObDList<ObPreCalcExprConstraint> cached_cons_list;
  cached_cons.expect_result_ = PRE_CALC_RESULT_TRUE;
  current_cons.expect_result_ = PRE_CALC_RESULT_FALSE;
  ASSERT_TRUE(cached_cons_list.add_last(&cached_cons));
  ASSERT_EQ(OB_SUCCESS, plan_set.set_pre_calc_constraint(cached_cons_list));
  ASSERT_TRUE(query_ctx.all_pre_calc_constraints_.add_last(&current_cons));
  ObPCPrivInfo cached_priv;
  ObPCPrivInfo current_priv;
  cached_priv.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  cached_priv.owner_user_id_ = 1;
  current_priv.priv_type_ = ObPCPrivInfo::TABLE_OWNER_PRIV;
  current_priv.owner_user_id_ = 2;
  ObSEArray<ObPCPrivInfo, 1> cached_priv_list;
  ASSERT_EQ(OB_SUCCESS, cached_priv_list.push_back(cached_priv));
  ASSERT_EQ(OB_SUCCESS, plan_set.set_priv_constraint(cached_priv_list));
  ASSERT_EQ(OB_SUCCESS, query_ctx.all_priv_constraints_.push_back(current_priv));
  set_current_constraints(sql_ctx_, query_ctx);
  ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                        allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  bool matched = true;
  plan_set.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  ASSERT_EQ(OB_SUCCESS, plan_set.match_and_merge_plan_cons(pc_ctx, matched));
  EXPECT_FALSE(matched);
  EXPECT_EQ(1, query_ctx.all_pre_calc_constraints_.get_size());
  EXPECT_EQ(1, query_ctx.all_priv_constraints_.count());
  plan_set.session_state_constraints_ = NO_SESSION_STATE_CONSTRAINT;
  sql_ctx_.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  ASSERT_EQ(OB_SUCCESS, plan_set.match_and_merge_plan_cons(pc_ctx, matched));
  EXPECT_FALSE(matched);
  EXPECT_EQ(1, query_ctx.all_pre_calc_constraints_.get_size());
  EXPECT_EQ(1, query_ctx.all_priv_constraints_.count());
  EXPECT_EQ(1, plan_set.all_pre_calc_constraints_.get_size());
  EXPECT_EQ(1, plan_set.all_priv_constraints_.count());
  EXPECT_EQ(2UL, query_ctx.all_priv_constraints_.at(0).owner_user_id_);
  // Equal session state constraints retain the existing expression/privilege union behavior.
  plan_set.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  ASSERT_EQ(OB_SUCCESS, plan_set.match_and_merge_plan_cons(pc_ctx, matched));
  EXPECT_FALSE(matched);
  EXPECT_EQ(2, query_ctx.all_pre_calc_constraints_.get_size());
  EXPECT_EQ(2, query_ctx.all_priv_constraints_.count());
  EXPECT_EQ(PRE_CALC_RESULT_TRUE,
            query_ctx.all_pre_calc_constraints_.get_last()->expect_result_);
  EXPECT_EQ(1UL, query_ctx.all_priv_constraints_.at(1).owner_user_id_);
  EXPECT_EQ(SESSION_STATE_NO_MULTI_STMT_TRANSACTION, plan_set.session_state_constraints_);
  EXPECT_EQ(SESSION_STATE_NO_MULTI_STMT_TRANSACTION, sql_ctx_.session_state_constraints_);
}

TEST_F(TestAsyncIndexPlanConstraint, same_session_state_constraints_preserve_expression_matching)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObSqlPlanSet plan_set(1);
  ObQueryCtx query_ctx(allocator_);
  ObPreCalcExprConstraint cached_cons(allocator_);
  ObPreCalcExprConstraint current_cons(allocator_);
  ObDList<ObPreCalcExprConstraint> cached_cons_list;
  cached_cons.expect_result_ = PRE_CALC_RESULT_TRUE;
  current_cons.expect_result_ = PRE_CALC_RESULT_FALSE;
  ASSERT_TRUE(cached_cons_list.add_last(&cached_cons));
  ASSERT_EQ(OB_SUCCESS, plan_set.set_pre_calc_constraint(cached_cons_list));
  ASSERT_TRUE(query_ctx.all_pre_calc_constraints_.add_last(&current_cons));
  sql_ctx_.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  plan_set.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  set_current_constraints(sql_ctx_, query_ctx);
  ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                        allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  bool matched = true;
  ASSERT_EQ(OB_SUCCESS, plan_set.match_params_info(plan_set.params_info_, OB_INVALID_INDEX,
                                                 pc_ctx, true, matched));
  EXPECT_FALSE(matched);
  ASSERT_EQ(OB_SUCCESS, plan_set.match_params_info(plan_set.params_info_, OB_INVALID_INDEX,
                                                 pc_ctx, false, matched));
  EXPECT_TRUE(matched);
  current_cons.expect_result_ = PRE_CALC_RESULT_TRUE;
  ASSERT_EQ(OB_SUCCESS, plan_set.match_and_merge_plan_cons(pc_ctx, matched));
  EXPECT_TRUE(matched);
  EXPECT_EQ(1, query_ctx.all_pre_calc_constraints_.get_size());
}

TEST_F(TestAsyncIndexPlanConstraint, retry_lookup_uses_current_request_constraints)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObSqlPlanSet async_set(1);
  ObSqlPlanSet safe_set(2);
  ObPlanCacheValue cache_value(1);
  async_set.session_state_constraints_ = SESSION_STATE_NO_MULTI_STMT_TRANSACTION;
  ObPlanCacheCtx pc_ctx(ObString::make_string("select 1"), PC_TEXT_MODE,
                        allocator_, sql_ctx_, exec_ctx_, OB_SYS_TENANT_ID);
  pc_ctx.try_get_plan_ = true;
  // Shared matcher coverage only; this does not exercise adaptive/inactive scheduling.
  check_second_lookup(cache_value, async_set, pc_ctx, true);
  tx_desc_.flags_.EXPLICIT_ = true;
  session_info_.get_tx_desc() = &tx_desc_;
  check_second_lookup(cache_value, async_set, pc_ctx, false);
  check_second_lookup(cache_value, safe_set, pc_ctx, true);
  session_info_.get_tx_desc() = nullptr;
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(false));
  check_second_lookup(cache_value, async_set, pc_ctx, false);
  check_second_lookup(cache_value, safe_set, pc_ctx, true);
  ASSERT_EQ(OB_SUCCESS, session_info_.set_autocommit(true));
  check_second_lookup(cache_value, async_set, pc_ctx, true);
}

#ifdef OB_BUILD_SPM
TEST_F(TestAsyncIndexPlanConstraint, spm_baseline_probe_preserves_session_state_constraint_owner)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  check_spm_baseline_session_state_constraint_gate(
      SESSION_STATE_NO_MULTI_STMT_TRANSACTION, NO_SESSION_STATE_CONSTRAINT);
  check_spm_baseline_session_state_constraint_gate(
      NO_SESSION_STATE_CONSTRAINT, SESSION_STATE_NO_MULTI_STMT_TRANSACTION);
  check_spm_baseline_session_state_constraint_gate(SESSION_STATE_NO_MULTI_STMT_TRANSACTION,
      SESSION_STATE_NO_MULTI_STMT_TRANSACTION | (1ULL << 32));
}

TEST_F(TestAsyncIndexPlanConstraint,
       spm_local_and_distributed_transfer_reject_session_state_constraint_mismatch)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  check_spm_transfer_session_state_constraint_gate(OB_PHY_PLAN_LOCAL,
      SESSION_STATE_NO_MULTI_STMT_TRANSACTION, NO_SESSION_STATE_CONSTRAINT);
  check_spm_transfer_session_state_constraint_gate(OB_PHY_PLAN_LOCAL,
      NO_SESSION_STATE_CONSTRAINT, SESSION_STATE_NO_MULTI_STMT_TRANSACTION);
  check_spm_transfer_session_state_constraint_gate(OB_PHY_PLAN_DISTRIBUTED,
      SESSION_STATE_NO_MULTI_STMT_TRANSACTION, NO_SESSION_STATE_CONSTRAINT);
  check_spm_transfer_session_state_constraint_gate(OB_PHY_PLAN_DISTRIBUTED,
      NO_SESSION_STATE_CONSTRAINT, SESSION_STATE_NO_MULTI_STMT_TRANSACTION);
}

TEST_F(TestAsyncIndexPlanConstraint, spm_same_session_state_constraint_transfer_starts_evolution)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  check_spm_same_session_state_constraint_transfer(OB_PHY_PLAN_LOCAL, NO_SESSION_STATE_CONSTRAINT);
  check_spm_same_session_state_constraint_transfer(
      OB_PHY_PLAN_LOCAL, SESSION_STATE_NO_MULTI_STMT_TRANSACTION);
  check_spm_same_session_state_constraint_transfer(
      OB_PHY_PLAN_DISTRIBUTED, NO_SESSION_STATE_CONSTRAINT);
  check_spm_same_session_state_constraint_transfer(
      OB_PHY_PLAN_DISTRIBUTED, SESSION_STATE_NO_MULTI_STMT_TRANSACTION);
}
#endif

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_file_name("test_async_index_plan_constraint.log", true);
  oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
