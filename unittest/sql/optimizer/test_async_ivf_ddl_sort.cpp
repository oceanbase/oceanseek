/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/mock_schema_service.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/async_index/ob_async_index_refresh_worker.h"
#undef protected
#undef private

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace share;
using namespace share::schema;

class TestDdlInsert : public ObLogInsert
{
public:
  explicit TestDdlInsert(ObDelUpdLogPlan &plan) : ObLogInsert(plan) {}
  using ObLogInsert::generate_ddl_slice_id_expr;
  using ObLogInsert::generate_pdml_partition_id_expr;
};

class TestAsyncIVFDdlSort : public ::testing::Test
{
public:
  TestAsyncIVFDdlSort()
    : allocator_("IVFDdlSortTest"), global_hint_(allocator_), expr_factory_(allocator_),
      optimizer_ctx_(&session_, nullptr, &sql_schema_guard_, nullptr, allocator_,
                     nullptr, server_, nullptr, global_hint_, expr_factory_, nullptr, false),
      plan_(optimizer_ctx_, nullptr), dml_info_(allocator_)
  {}

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, schema_service_.init());
    ASSERT_EQ(OB_SUCCESS, session_.init_tenant(ObString("sys"), OB_SYS_TENANT_ID));
    session_.get_ddl_info().set_is_ddl(true);
    optimizer_ctx_.set_is_online_ddl(true);
    sql_schema_guard_.set_schema_guard(&schema_guard_);
  }

  void add_target(const uint64_t table_id, const ObIndexType index_type,
                  const ObPartitionLevel part_level = PARTITION_LEVEL_ZERO)
  {
    ObTableSchema table;
    table.set_tenant_id(OB_SYS_TENANT_ID);
    table.set_table_id(table_id);
    table.set_database_id(OB_SYS_DATABASE_ID);
    table.set_schema_version(OB_CORE_SCHEMA_VERSION + 1);
    table.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    table.set_table_type(INDEX_TYPE_IS_NOT == index_type ? USER_TABLE : USER_INDEX);
    table.set_index_type(index_type);
    table.set_data_table_id(50000);
    table.set_part_level(part_level);
    if (PARTITION_LEVEL_ZERO != part_level) {
      // One implicit hash partition is reported as a nonpartitioned table.
      table.get_part_option().set_part_num(2);
    }
    ASSERT_EQ(part_level, table.get_part_level());
    char name[64];
    snprintf(name, sizeof(name), "__idx_50000_ddl_sort_target_%lu", table_id);
    ASSERT_EQ(OB_SUCCESS, table.set_table_name(name));
    table.set_rowkey_column_num(1);
    ObColumnSchemaV2 column;
    column.set_tenant_id(OB_SYS_TENANT_ID);
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID);
    column.set_rowkey_position(1);
    column.set_data_type(ObUInt64Type);
    column.set_data_length(static_cast<int32_t>(sizeof(uint64_t)));
    column.set_nullable(false);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name("vid"));
    ASSERT_EQ(OB_SUCCESS, table.add_column(column));
    ASSERT_EQ(OB_SUCCESS, schema_service_.add_table_schema(table, table.get_schema_version()));
    ASSERT_EQ(OB_SUCCESS, schema_service_.get_schema_guard(schema_guard_));
    dml_info_.ref_table_id_ = table_id;
  }

  ObArenaAllocator allocator_;
  ObGlobalHint global_hint_;
  ObRawExprFactory expr_factory_;
  ObAddr server_;
  ObSQLSessionInfo session_;
  MockSchemaService schema_service_;
  ObSchemaGetterGuard schema_guard_;
  ObSqlSchemaGuard sql_schema_guard_;
  ObOptimizerContext optimizer_ctx_;
  ObInsertLogPlan plan_;
  IndexDMLInfo dml_info_;
};

TEST_F(TestAsyncIVFDdlSort, sorted_ddl_keeps_slice_id_on_exchange)
{
  TestDdlInsert insert(plan_);
  ObLogSort sort(plan_);
  ObLogExchange consumer(plan_);
  ObLogExchange producer(plan_);
  sort.set_type(log_op_def::LOG_SORT);
  consumer.set_type(log_op_def::LOG_EXCHANGE);
  producer.set_type(log_op_def::LOG_EXCHANGE);
  producer.set_to_producer();
  insert.set_child(0, &sort);
  sort.set_child(0, &consumer);
  consumer.set_child(0, &producer);
  ASSERT_EQ(OB_SUCCESS, insert.generate_ddl_slice_id_expr());
  ASSERT_NE(nullptr, producer.get_ddl_slice_id_expr());
  EXPECT_EQ(T_PSEUDO_DDL_SLICE_ID, producer.get_ddl_slice_id_expr()->get_expr_type());
  EXPECT_EQ(nullptr, consumer.get_ddl_slice_id_expr());
}

TEST_F(TestAsyncIVFDdlSort, unsorted_input_selects_only_async_ivf_schema)
{
  const ObIndexType types[] = {INDEX_TYPE_NORMAL_LOCAL, INDEX_TYPE_UNIQUE_LOCAL,
      INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, INDEX_TYPE_IS_NOT};
  for (int64_t i = 0; i < ARRAYSIZEOF(types); ++i) {
    ASSERT_NO_FATAL_FAILURE(add_target(50201 + i, types[i]));
    bool enabled = true;
    ASSERT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
    EXPECT_FALSE(enabled);
  }
}

TEST_F(TestAsyncIVFDdlSort, unsorted_input_requires_ddl_execution)
{
  ASSERT_NO_FATAL_FAILURE(add_target(50301, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL));
  bool enabled = true;
  optimizer_ctx_.set_is_online_ddl(false);
  ASSERT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
  EXPECT_FALSE(enabled);
  optimizer_ctx_.set_is_online_ddl(true);
  session_.get_ddl_info().set_is_ddl(false);
  ASSERT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
  EXPECT_FALSE(enabled);
  session_.get_ddl_info().set_is_ddl(true);
  optimizer_ctx_.set_is_heap_table_ddl(true);
  EXPECT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
  EXPECT_FALSE(enabled);
}

TEST_F(TestAsyncIVFDdlSort, unsorted_input_rejects_missing_target)
{
  ASSERT_EQ(OB_SUCCESS, schema_service_.get_schema_guard(schema_guard_));
  dml_info_.ref_table_id_ = 59999;
  bool enabled = false;
  EXPECT_EQ(OB_TABLE_NOT_EXIST, plan_.check_async_ivf_build(dml_info_, enabled));
  EXPECT_FALSE(enabled);
}

TEST_F(TestAsyncIVFDdlSort, unsorted_input_accepts_partitioned_targets)
{
  const ObPartitionLevel levels[] = {PARTITION_LEVEL_ZERO, PARTITION_LEVEL_ONE, PARTITION_LEVEL_TWO};
  for (int64_t i = 0; i < ARRAYSIZEOF(levels); ++i) {
    ASSERT_NO_FATAL_FAILURE(add_target(50101 + i, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, levels[i]));
    bool enabled = false;
    ASSERT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
    EXPECT_TRUE(enabled);
  }
}

TEST_F(TestAsyncIVFDdlSort, unsorted_mode_supports_initial_build_and_ivf_refresh)
{
  ASSERT_NO_FATAL_FAILURE(add_target(50101, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL));
  bool enabled = false;
  ASSERT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
  EXPECT_TRUE(enabled);
  session_.get_ddl_info().set_is_async_index_refresh(true);
  ASSERT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
  EXPECT_TRUE(enabled);
  session_.get_ddl_info().set_is_async_index_refresh(false);
  session_.get_ddl_info().set_is_vec_tablet_rebuild(true);
  ASSERT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
  EXPECT_FALSE(enabled);
  session_.get_ddl_info().set_is_vec_tablet_rebuild(false);
  ASSERT_EQ(OB_SUCCESS, plan_.check_async_ivf_build(dml_info_, enabled));
  EXPECT_TRUE(enabled);
}

TEST_F(TestAsyncIVFDdlSort, ivf_refresh_retry_preserves_unsorted_target_kind)
{
  storage::ObAsyncIndexRefreshTaskParam param;
  EXPECT_FALSE(param.is_ivf_index_);
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.assign("SELECT 1"));
  param.sql_ = &sql;
  param.is_ivf_index_ = true;
  storage::ObAsyncIndexRefreshTask task;
  ASSERT_EQ(OB_SUCCESS, task.set_task(param));
  storage::ObRetryTaskNode retry;
  task.copy_stat(retry.stat_);
  ASSERT_EQ(OB_SUCCESS, retry.sql_.assign(sql));
  storage::ObAsyncIndexRefreshTaskParam retried;
  retry.build_task_param(nullptr, retried);
  EXPECT_TRUE(retried.is_ivf_index_);
  EXPECT_FALSE(retried.is_delete_index_);
  EXPECT_FALSE(retried.is_search_index_);
  retry.reset();
  retry.build_task_param(nullptr, retried);
  EXPECT_FALSE(retried.is_ivf_index_);
}

TEST_F(TestAsyncIVFDdlSort, ivf_input_keeps_range_exchange_slice_without_sort)
{
  TestDdlInsert insert(plan_);
  ObLogExchange consumer(plan_);
  ObLogExchange producer(plan_);
  consumer.set_type(log_op_def::LOG_EXCHANGE);
  producer.set_type(log_op_def::LOG_EXCHANGE);
  producer.set_to_producer();
  insert.set_child(0, &consumer);
  consumer.set_child(0, &producer);
  ASSERT_EQ(OB_SUCCESS, insert.generate_ddl_slice_id_expr());
  ASSERT_NE(nullptr, producer.get_ddl_slice_id_expr());
  EXPECT_EQ(T_PSEUDO_DDL_SLICE_ID, producer.get_ddl_slice_id_expr()->get_expr_type());
  EXPECT_EQ(nullptr, consumer.get_ddl_slice_id_expr());
}

TEST_F(TestAsyncIVFDdlSort, ivf_input_uses_exchange_tablet_metadata)
{
  TestDdlInsert insert(plan_);
  ObLogExchange consumer(plan_);
  ObLogExchange producer(plan_);
  ObLogTableScan scan(plan_);
  consumer.set_type(log_op_def::LOG_EXCHANGE);
  producer.set_type(log_op_def::LOG_EXCHANGE);
  producer.set_to_producer();
  scan.set_type(log_op_def::LOG_TABLE_SCAN);
  scan.set_table_id(60001);
  scan.set_ref_table_id(50000);
  insert.set_child(0, &consumer);
  consumer.set_child(0, &producer);
  producer.set_child(0, &scan);
  insert.set_is_pdml(true);
  ASSERT_EQ(OB_SUCCESS, insert.add_index_dml_info(&dml_info_));
  dml_info_.loc_table_id_ = 60002;
  dml_info_.ref_table_id_ = 50001;
  ASSERT_EQ(OB_SUCCESS, insert.generate_pdml_partition_id_expr());
  EXPECT_EQ(nullptr, scan.get_tablet_id_expr());
  EXPECT_NE(nullptr, producer.get_partition_id_expr());
}

TEST_F(TestAsyncIVFDdlSort, dag_reuse_clears_target_index_type)
{
  class InputDag final : public storage::ObDDLIndependentDag
  {
  public:
    bool is_scan_finished() override { return false; }
    void set_index_type(const ObIndexType index_type)
    {
      ddl_table_schema_.table_item_.index_type_ = index_type;
    }
  } dag;
  EXPECT_FALSE(is_async_ivf_index(dag.get_ddl_table_schema().table_item_.index_type_));
  dag.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  EXPECT_TRUE(is_async_ivf_index(dag.get_ddl_table_schema().table_item_.index_type_));
  dag.reuse();
  EXPECT_FALSE(is_async_ivf_index(dag.get_ddl_table_schema().table_item_.index_type_));
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_async_ivf_ddl_sort.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
