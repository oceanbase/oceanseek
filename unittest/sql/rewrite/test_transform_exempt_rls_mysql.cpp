/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include "lib/worker.h"
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

TEST(TestTransformExemptRlsMysqlNullCtx, null_ctx_returns_error)
{
  ObTransformerCtx ctx;
  ObTransformPreProcess transform(&ctx);
  ObTableSchema table_schema;
  ObSEArray<ObPCPrivInfo, 4> priv_constraints;
  bool exempt = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED, transform.check_exempt_table_rls_policy_mysql(table_schema, priv_constraints, exempt));
}

class TestTransformExemptRlsMysql : public TestOptimizerUtils
{
protected:
  TestTransformExemptRlsMysql() {}

  void SetUp()
  {
    // Override ctor path "./test_optimizer_schema.sql": skip SQL file load so tests
    // run from build_release without parsing large schema batches.
    memset(schema_file_path_, '\0', sizeof(schema_file_path_));
    TestOptimizerUtils::SetUp();
  }

  void TearDown()
  {
    destroy();
  }
};

// Inner session bypass is handled by check_bypass_rls_mysql (not this function).
// This function only checks FORCE and table ownership; here we verify owner-based
// exemption still applies when the session happens to be an inner session.
TEST_F(TestTransformExemptRlsMysql, inner_session_owner_exempt)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObTransformerCtx trans_ctx;
  trans_ctx.session_info_ = &session_info_;
  ObSchemaChecker schema_checker;
  ASSERT_EQ(OB_SUCCESS, schema_checker.init(get_schema_guard()));
  trans_ctx.schema_checker_ = &schema_checker;
  ObTransformPreProcess transform(&trans_ctx);
  session_info_.set_inner_session();
  ObTableSchema table_schema;
  table_schema.set_define_user_id(session_info_.get_priv_user_id());
  table_schema.set_force_row_level_security(false);
  ObSEArray<ObPCPrivInfo, 4> priv_constraints;
  bool exempt = false;
  ASSERT_EQ(OB_SUCCESS, transform.check_exempt_table_rls_policy_mysql(table_schema, priv_constraints, exempt));
  EXPECT_TRUE(exempt);
}

TEST_F(TestTransformExemptRlsMysql, force_rls_not_exempt_non_owner)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObTransformerCtx trans_ctx;
  trans_ctx.session_info_ = &session_info_;
  ObSchemaChecker schema_checker;
  ASSERT_EQ(OB_SUCCESS, schema_checker.init(get_schema_guard()));
  trans_ctx.schema_checker_ = &schema_checker;
  ObTransformPreProcess transform(&trans_ctx);
  session_info_.set_user_session();
  ObTableSchema table_schema;
  table_schema.set_define_user_id(session_info_.get_priv_user_id() + 999999UL);
  table_schema.set_force_row_level_security(true);
  ObSEArray<ObPCPrivInfo, 4> priv_constraints;
  bool exempt = false;
  ASSERT_EQ(OB_SUCCESS, transform.check_exempt_table_rls_policy_mysql(table_schema, priv_constraints, exempt));
  EXPECT_FALSE(exempt);
}

TEST_F(TestTransformExemptRlsMysql, force_rls_not_exempt_even_when_owner)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObTransformerCtx trans_ctx;
  trans_ctx.session_info_ = &session_info_;
  ObSchemaChecker schema_checker;
  ASSERT_EQ(OB_SUCCESS, schema_checker.init(get_schema_guard()));
  trans_ctx.schema_checker_ = &schema_checker;
  ObTransformPreProcess transform(&trans_ctx);
  session_info_.set_user_session();
  ObTableSchema table_schema;
  table_schema.set_define_user_id(session_info_.get_priv_user_id());
  table_schema.set_force_row_level_security(true);
  ObSEArray<ObPCPrivInfo, 4> priv_constraints;
  bool exempt = false;
  ASSERT_EQ(OB_SUCCESS, transform.check_exempt_table_rls_policy_mysql(table_schema, priv_constraints, exempt));
  EXPECT_FALSE(exempt);
}

TEST_F(TestTransformExemptRlsMysql, table_owner_exempt_without_force)
{
  lib::CompatModeGuard compat_guard(lib::Worker::CompatMode::MYSQL);
  ObTransformerCtx trans_ctx;
  trans_ctx.session_info_ = &session_info_;
  ObSchemaChecker schema_checker;
  ASSERT_EQ(OB_SUCCESS, schema_checker.init(get_schema_guard()));
  trans_ctx.schema_checker_ = &schema_checker;
  ObTransformPreProcess transform(&trans_ctx);
  session_info_.set_user_session();
  ObTableSchema table_schema;
  table_schema.set_define_user_id(session_info_.get_priv_user_id());
  table_schema.set_force_row_level_security(false);
  ObSEArray<ObPCPrivInfo, 4> priv_constraints;
  bool exempt = false;
  ASSERT_EQ(OB_SUCCESS, transform.check_exempt_table_rls_policy_mysql(table_schema, priv_constraints, exempt));
  EXPECT_TRUE(exempt);
}

int main(int argc, char **argv)
{
  system("rm -rf test_transform_exempt_rls_mysql.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_transform_exempt_rls_mysql.log", true);
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
