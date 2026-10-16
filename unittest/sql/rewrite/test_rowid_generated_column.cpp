/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_REWRITE

#include <gtest/gtest.h>
#include "lib/worker.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "../optimizer/test_optimizer_utils.h"
#define private public
#include "sql/rewrite/ob_transform_pre_process.h"
#include "sql/resolver/ob_schema_checker.h"
#include "share/schema/ob_table_schema.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace share::schema;
using namespace test;

namespace
{

bool contains_expr_type(const ObRawExpr *expr, const ObItemType expr_type)
{
  bool found = false;
  if (OB_NOT_NULL(expr)) {
    found = expr_type == expr->get_expr_type();
    for (int64_t i = 0; !found && i < expr->get_param_count(); ++i) {
      found = contains_expr_type(expr->get_param_expr(i), expr_type);
    }
  }
  return found;
}

class TestRowidGeneratedColumn : public TestOptimizerUtils
{
protected:
  void SetUp() override
  {
    // 跳过默认 schema 文件，测试中按需构造表结构。
    memset(schema_file_path_, '\0', sizeof(schema_file_path_));
    TestOptimizerUtils::SetUp();
  }

  void TearDown() override
  {
    destroy();
  }
};

TEST_F(TestRowidGeneratedColumn, rebuild_generated_partition_column_with_full_resolver)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  LinkExecCtxGuard exec_ctx_guard(session_info_, exec_ctx_);
  ObObj sql_mode;
  sql_mode.set_varchar(STR_PAD_CHAR_TO_FULL_LENGTH);
  ASSERT_EQ(OB_SUCCESS, session_info_.update_sys_variable_by_name(OB_SV_SQL_MODE, sql_mode));
  do_create_table(
      "create table t_rowid_gen_col(k char(4) primary key, "
      "g varchar(8) generated always as (concat(k, 'x')) virtual) "
      "organization index");

  sql_mode.set_varchar("");
  ASSERT_EQ(OB_SUCCESS, session_info_.update_sys_variable_by_name(OB_SV_SQL_MODE, sql_mode));
  ObStmt *basic_stmt = NULL;
  do_resolve("select k from t_rowid_gen_col", basic_stmt, JSON_FORMAT, OB_SUCCESS, false);
  ASSERT_NE(nullptr, basic_stmt);
  ASSERT_EQ(stmt::T_SELECT, basic_stmt->get_stmt_type());
  ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(basic_stmt);
  ASSERT_EQ(1, select_stmt->get_table_size());
  TableItem *table_item = select_stmt->get_table_item(0);
  ASSERT_NE(nullptr, table_item);

  ObSchemaChecker schema_checker;
  ASSERT_EQ(OB_SUCCESS, schema_checker.init(get_schema_guard()));
  const ObTableSchema *table_schema = NULL;
  ASSERT_EQ(OB_SUCCESS,
            schema_checker.get_table_schema(session_info_.get_effective_tenant_id(),
                                            table_item->ref_id_,
                                            table_schema,
                                            false));
  ASSERT_NE(nullptr, table_schema);
  const ObColumnSchemaV2 *column_schema =
      table_schema->get_column_schema(ObString::make_string("g"));
  ASSERT_NE(nullptr, column_schema);

  ASSERT_EQ(nullptr,
            select_stmt->get_column_expr_by_id(table_item->table_id_, column_schema->get_column_id()));

  ObTransformerCtx trans_ctx;
  trans_ctx.allocator_ = &allocator_;
  trans_ctx.schema_checker_ = &schema_checker;
  trans_ctx.session_info_ = &session_info_;
  trans_ctx.exec_ctx_ = &exec_ctx_;
  trans_ctx.expr_factory_ = &expr_factory_;
  trans_ctx.stmt_factory_ = &stmt_factory_;
  ObTransformPreProcess transform(&trans_ctx);
  ObColumnRefRawExpr *rebuilt_expr = NULL;
  ASSERT_EQ(OB_SUCCESS,
            transform.get_or_create_rowid_column_expr(*select_stmt,
                                                      *table_item,
                                                      *table_schema,
                                                      column_schema->get_column_id(),
                                                      rebuilt_expr));
  ASSERT_NE(nullptr, rebuilt_expr);
  ASSERT_NE(nullptr, rebuilt_expr->get_dependant_expr());
  EXPECT_TRUE(contains_expr_type(rebuilt_expr->get_dependant_expr(), T_FUN_PAD));
  EXPECT_TRUE(contains_expr_type(rebuilt_expr->get_dependant_expr(), T_FUN_COLUMN_CONV));
  EXPECT_NE(OB_INVALID_INDEX_INT64,
            rebuilt_expr->get_dependant_expr()->get_local_session_var_id());
}

TEST_F(TestRowidGeneratedColumn, allow_string_lob_generated_partition_column)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  LinkExecCtxGuard exec_ctx_guard(session_info_, exec_ctx_);
  do_create_table(
      "create table t_rowid_lob_gen_col(k varchar(8) primary key, "
      "g string generated always as (repeat(k, 20)) stored) "
      "organization index lob_inrow_threshold = 0 "
      "partition by key(g) partitions 2");

  ObStmt *basic_stmt = NULL;
  do_resolve("select k from t_rowid_lob_gen_col",
             basic_stmt,
             JSON_FORMAT,
             OB_SUCCESS,
             false);
  ASSERT_NE(nullptr, basic_stmt);
  ASSERT_EQ(stmt::T_SELECT, basic_stmt->get_stmt_type());
  ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(basic_stmt);
  ASSERT_EQ(1, select_stmt->get_table_size());
  TableItem *table_item = select_stmt->get_table_item(0);
  ASSERT_NE(nullptr, table_item);

  ObSchemaChecker schema_checker;
  ASSERT_EQ(OB_SUCCESS, schema_checker.init(get_schema_guard()));
  const ObTableSchema *table_schema = NULL;
  ASSERT_EQ(OB_SUCCESS,
            schema_checker.get_table_schema(session_info_.get_effective_tenant_id(),
                                            table_item->ref_id_,
                                            table_schema,
                                            false));
  ASSERT_NE(nullptr, table_schema);
  const ObColumnSchemaV2 *column_schema =
      table_schema->get_column_schema(ObString::make_string("g"));
  ASSERT_NE(nullptr, column_schema);
  ASSERT_TRUE(column_schema->is_string_lob());
  ObTransformerCtx trans_ctx;
  trans_ctx.allocator_ = &allocator_;
  trans_ctx.schema_checker_ = &schema_checker;
  trans_ctx.session_info_ = &session_info_;
  trans_ctx.exec_ctx_ = &exec_ctx_;
  trans_ctx.expr_factory_ = &expr_factory_;
  trans_ctx.stmt_factory_ = &stmt_factory_;
  ObTransformPreProcess transform(&trans_ctx);
  ObSysFunRawExpr *rowid_expr = NULL;
  ASSERT_EQ(OB_SUCCESS,
            transform.build_rowid_expr(select_stmt, table_item, rowid_expr));
  ASSERT_NE(nullptr, rowid_expr);
  ASSERT_EQ(4, rowid_expr->get_param_count());
  ObRawExpr *version_expr = rowid_expr->get_param_expr(0);
  ObRawExpr *real_pk_count_expr = rowid_expr->get_param_expr(1);
  ASSERT_NE(nullptr, version_expr);
  ASSERT_NE(nullptr, real_pk_count_expr);
  ASSERT_EQ(T_NUMBER, version_expr->get_expr_type());
  ASSERT_EQ(T_NUMBER, real_pk_count_expr->get_expr_type());
  ObConstRawExpr *version_param = static_cast<ObConstRawExpr *>(version_expr);
  ObConstRawExpr *real_pk_count_param =
      static_cast<ObConstRawExpr *>(real_pk_count_expr);
  int64_t version = ObURowIDData::INVALID_ROWID_VERSION;
  int64_t real_pk_count = 0;
  ASSERT_TRUE(version_param->get_value().is_number());
  ASSERT_TRUE(real_pk_count_param->get_value().is_number());
  ASSERT_TRUE(version_param->get_value().get_number().is_valid_int64(version));
  ASSERT_TRUE(real_pk_count_param->get_value().get_number().is_valid_int64(real_pk_count));
  EXPECT_EQ(ObURowIDData::PK_ROWID_VERSION, version);
  EXPECT_EQ(1, real_pk_count);
  ObRawExpr *lob_param = rowid_expr->get_param_expr(3);
  ASSERT_NE(nullptr, lob_param);
  EXPECT_EQ(select_stmt->get_column_expr_by_id(table_item->table_id_,
                                               column_schema->get_column_id()),
            lob_param);
  EXPECT_EQ(ObLongTextType, lob_param->get_data_type());
  EXPECT_EQ(OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER,
            lob_param->get_accuracy().get_length());
}

} // namespace

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_rowid_generated_column.log", true);
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
