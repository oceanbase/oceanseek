/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include "lib/allocator/ob_malloc.h"
#include "sql/parser/ob_parser.h"
#include "sql/ob_sql_mode_manager.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

class TestRlsParser : public ::testing::Test
{
public:
  TestRlsParser() : allocator_(ObModIds::TEST) {}
  virtual ~TestRlsParser() {}

protected:
  ParseNode *parse_ok(const char *sql)
  {
    ObString stmt(sql);
    ObParser parser(allocator_, SMO_DEFAULT);
    ParseResult result;
    memset(&result, 0, sizeof(result));
    int ret = parser.parse(stmt, result);
    EXPECT_EQ(OB_SUCCESS, ret) << "Failed to parse: " << sql;
    if (OB_SUCCESS != ret) return NULL;
    EXPECT_TRUE(NULL != result.result_tree_);
    EXPECT_EQ(T_STMT_LIST, result.result_tree_->type_);
    EXPECT_GE(result.result_tree_->num_child_, 1);
    return result.result_tree_->children_[0];
  }

  void parse_fail(const char *sql)
  {
    ObString stmt(sql);
    ObParser parser(allocator_, SMO_DEFAULT);
    ParseResult result;
    memset(&result, 0, sizeof(result));
    int ret = parser.parse(stmt, result);
    EXPECT_NE(OB_SUCCESS, ret) << "Should have failed to parse: " << sql;
  }

  ObArenaAllocator allocator_;
};

// =====================================================================
// CREATE POLICY
// =====================================================================

TEST_F(TestRlsParser, create_policy_minimal_using)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 USING (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_CREATE_RLS_POLICY, node->type_);
  EXPECT_EQ(9, node->num_child_);
  // children[0] = NULL (no hint)
  EXPECT_TRUE(NULL == node->children_[0]);
  // children[1] = NULL (no IF NOT EXISTS)
  EXPECT_TRUE(NULL == node->children_[1]);
  // children[2] = policy name
  ASSERT_TRUE(NULL != node->children_[2]);
  // children[3] = table
  ASSERT_TRUE(NULL != node->children_[3]);
  // children[4] = NULL (default permissive)
  EXPECT_TRUE(NULL == node->children_[4]);
  // children[5] = NULL (default ALL)
  EXPECT_TRUE(NULL == node->children_[5]);
  // children[6] = NULL (PUBLIC)
  EXPECT_TRUE(NULL == node->children_[6]);
  // children[7] = USING expr
  ASSERT_TRUE(NULL != node->children_[7]);
  // children[8] = NULL (no WITH CHECK)
  EXPECT_TRUE(NULL == node->children_[8]);
}

TEST_F(TestRlsParser, create_policy_full_options)
{
  ParseNode *node = parse_ok(
      "CREATE POLICY IF NOT EXISTS p1 ON db1.t1 "
      "AS RESTRICTIVE FOR SELECT TO role1 "
      "USING (col1 > 0) WITH CHECK (col2 < 100)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_CREATE_RLS_POLICY, node->type_);
  EXPECT_EQ(9, node->num_child_);
  // IF NOT EXISTS
  ASSERT_TRUE(NULL != node->children_[1]);
  // permissive: RESTRICTIVE = 0
  ASSERT_TRUE(NULL != node->children_[4]);
  EXPECT_EQ(0, node->children_[4]->value_);
  // cmd type: SELECT = 1
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_EQ(1, node->children_[5]->value_);
  // role list
  ASSERT_TRUE(NULL != node->children_[6]);
  // USING
  ASSERT_TRUE(NULL != node->children_[7]);
  // WITH CHECK
  ASSERT_TRUE(NULL != node->children_[8]);
}

TEST_F(TestRlsParser, create_policy_as_permissive)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 AS PERMISSIVE USING (1=1)");
  ASSERT_TRUE(NULL != node);
  ASSERT_TRUE(NULL != node->children_[4]);
  EXPECT_EQ(1, node->children_[4]->value_);
}

TEST_F(TestRlsParser, create_policy_for_all)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 FOR ALL USING (1=1)");
  ASSERT_TRUE(NULL != node);
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_EQ(0, node->children_[5]->value_);
}

TEST_F(TestRlsParser, create_policy_for_insert)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 FOR INSERT WITH CHECK (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_EQ(2, node->children_[5]->value_);
}

TEST_F(TestRlsParser, create_policy_for_update)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 FOR UPDATE USING (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_EQ(3, node->children_[5]->value_);
}

TEST_F(TestRlsParser, create_policy_for_delete)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 FOR DELETE USING (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_EQ(4, node->children_[5]->value_);
}

TEST_F(TestRlsParser, create_policy_no_using_no_check)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_CREATE_RLS_POLICY, node->type_);
  EXPECT_TRUE(NULL == node->children_[7]);
  EXPECT_TRUE(NULL == node->children_[8]);
}

TEST_F(TestRlsParser, create_policy_with_check_only)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 WITH CHECK (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  EXPECT_TRUE(NULL == node->children_[7]);
  ASSERT_TRUE(NULL != node->children_[8]);
}

TEST_F(TestRlsParser, create_policy_multiple_roles)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 TO role1, role2, role3 USING (1=1)");
  ASSERT_TRUE(NULL != node);
  ASSERT_TRUE(NULL != node->children_[6]);
  EXPECT_EQ(3, node->children_[6]->num_child_);
}

TEST_F(TestRlsParser, create_policy_db_qualified_table)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON mydb.mytable USING (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_CREATE_RLS_POLICY, node->type_);
}

// =====================================================================
// DROP POLICY
// =====================================================================

TEST_F(TestRlsParser, drop_policy_basic)
{
  ParseNode *node = parse_ok("DROP POLICY p1 ON t1");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_DROP_RLS_POLICY, node->type_);
  EXPECT_EQ(3, node->num_child_);
  // no IF EXISTS
  EXPECT_TRUE(NULL == node->children_[0]);
  // policy name
  ASSERT_TRUE(NULL != node->children_[1]);
  // table
  ASSERT_TRUE(NULL != node->children_[2]);
}

TEST_F(TestRlsParser, drop_policy_if_exists)
{
  ParseNode *node = parse_ok("DROP POLICY IF EXISTS p1 ON t1");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_DROP_RLS_POLICY, node->type_);
  ASSERT_TRUE(NULL != node->children_[0]);
}

TEST_F(TestRlsParser, drop_policy_db_qualified)
{
  ParseNode *node = parse_ok("DROP POLICY p1 ON mydb.t1");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_DROP_RLS_POLICY, node->type_);
}

// =====================================================================
// ALTER POLICY
// =====================================================================

TEST_F(TestRlsParser, alter_policy_rename)
{
  ParseNode *node = parse_ok("ALTER POLICY p1 ON t1 RENAME TO p2");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_ALTER_RLS_POLICY, node->type_);
  EXPECT_EQ(7, node->num_child_);
  // children[0] = NULL (no hint)
  EXPECT_TRUE(NULL == node->children_[0]);
  // children[6] = new name (RENAME)
  ASSERT_TRUE(NULL != node->children_[6]);
  // children[3-5] = NULL (not modify attrs)
  EXPECT_TRUE(NULL == node->children_[3]);
  EXPECT_TRUE(NULL == node->children_[4]);
  EXPECT_TRUE(NULL == node->children_[5]);
}

TEST_F(TestRlsParser, alter_policy_using)
{
  ParseNode *node = parse_ok("ALTER POLICY p1 ON t1 USING (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_ALTER_RLS_POLICY, node->type_);
  // children[4] = USING expr
  ASSERT_TRUE(NULL != node->children_[4]);
  // children[6] = NULL (no rename)
  EXPECT_TRUE(NULL == node->children_[6]);
}

TEST_F(TestRlsParser, alter_policy_with_check)
{
  ParseNode *node = parse_ok("ALTER POLICY p1 ON t1 WITH CHECK (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_ALTER_RLS_POLICY, node->type_);
  ASSERT_TRUE(NULL != node->children_[5]);
}

TEST_F(TestRlsParser, alter_policy_to_role)
{
  ParseNode *node = parse_ok("ALTER POLICY p1 ON t1 TO role1, role2");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_ALTER_RLS_POLICY, node->type_);
  ASSERT_TRUE(NULL != node->children_[3]);
  EXPECT_EQ(2, node->children_[3]->num_child_);
}

TEST_F(TestRlsParser, alter_policy_combined_attrs)
{
  ParseNode *node = parse_ok(
      "ALTER POLICY p1 ON t1 TO role1 USING (col1 > 0) WITH CHECK (col2 < 100)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_ALTER_RLS_POLICY, node->type_);
  ASSERT_TRUE(NULL != node->children_[3]);
  ASSERT_TRUE(NULL != node->children_[4]);
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_TRUE(NULL == node->children_[6]);
}

// =====================================================================
// ALTER TABLE ... ROW LEVEL SECURITY
// =====================================================================

TEST_F(TestRlsParser, alter_table_enable_rls)
{
  ParseNode *node = parse_ok("ALTER TABLE t1 ENABLE ROW LEVEL SECURITY");
  ASSERT_TRUE(NULL != node);
}

TEST_F(TestRlsParser, alter_table_disable_rls)
{
  ParseNode *node = parse_ok("ALTER TABLE t1 DISABLE ROW LEVEL SECURITY");
  ASSERT_TRUE(NULL != node);
}

TEST_F(TestRlsParser, alter_table_force_rls)
{
  ParseNode *node = parse_ok("ALTER TABLE t1 FORCE ROW LEVEL SECURITY");
  ASSERT_TRUE(NULL != node);
}

TEST_F(TestRlsParser, alter_table_no_force_rls)
{
  ParseNode *node = parse_ok("ALTER TABLE t1 NO FORCE ROW LEVEL SECURITY");
  ASSERT_TRUE(NULL != node);
}

// =====================================================================
// Syntax Error Cases
// =====================================================================

TEST_F(TestRlsParser, error_create_missing_on)
{
  parse_fail("CREATE POLICY p1 USING (col1 > 0)");
}

TEST_F(TestRlsParser, error_create_missing_name)
{
  parse_fail("CREATE POLICY ON t1 USING (col1 > 0)");
}

TEST_F(TestRlsParser, error_drop_missing_on)
{
  parse_fail("DROP POLICY p1");
}

TEST_F(TestRlsParser, error_alter_missing_on)
{
  parse_fail("ALTER POLICY p1 RENAME TO p2");
}

TEST_F(TestRlsParser, error_invalid_cmd_type)
{
  parse_fail("CREATE POLICY p1 ON t1 FOR MERGE USING (1=1)");
}

// =====================================================================
// Semantically invalid combinations (parser accepts, resolver rejects)
// =====================================================================
// The parser allows these combinations; the resolver enforces compatibility.
// These tests document the boundary between parser and resolver validation.

TEST_F(TestRlsParser, create_policy_insert_with_using_parses)
{
  ParseNode *node = parse_ok("CREATE POLICY p1 ON t1 FOR INSERT USING (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_CREATE_RLS_POLICY, node->type_);
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_EQ(2, node->children_[5]->value_);
  ASSERT_TRUE(NULL != node->children_[7]);
}

TEST_F(TestRlsParser, create_policy_select_with_check_parses)
{
  ParseNode *node = parse_ok(
      "CREATE POLICY p1 ON t1 FOR SELECT USING (1=1) WITH CHECK (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_CREATE_RLS_POLICY, node->type_);
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_EQ(1, node->children_[5]->value_);
  ASSERT_TRUE(NULL != node->children_[8]);
}

TEST_F(TestRlsParser, create_policy_delete_with_check_parses)
{
  ParseNode *node = parse_ok(
      "CREATE POLICY p1 ON t1 FOR DELETE USING (1=1) WITH CHECK (col1 > 0)");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_CREATE_RLS_POLICY, node->type_);
  ASSERT_TRUE(NULL != node->children_[5]);
  EXPECT_EQ(4, node->children_[5]->value_);
  ASSERT_TRUE(NULL != node->children_[8]);
}

// =====================================================================
// GRANT / REVOKE BYPASSRLS
// =====================================================================

TEST_F(TestRlsParser, grant_bypassrls)
{
  ParseNode *node = parse_ok("GRANT BYPASSRLS ON *.* TO user1");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_GRANT, node->type_);
}

TEST_F(TestRlsParser, revoke_bypassrls)
{
  ParseNode *node = parse_ok("REVOKE BYPASSRLS ON *.* FROM user1");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_REVOKE, node->type_);
}

TEST_F(TestRlsParser, create_user_no_bypassrls)
{
  ParseNode *node = parse_ok("CREATE USER 'u1' IDENTIFIED BY 'pass'");
  ASSERT_TRUE(NULL != node);
  EXPECT_EQ(T_CREATE_USER, node->type_);
  EXPECT_EQ(4, node->num_child_);
}

// =====================================================================
// BYPASSRLS / NOBYPASSRLS / PERMISSIVE / RESTRICTIVE as non-reserved keywords
// =====================================================================

TEST_F(TestRlsParser, permissive_as_identifier)
{
  ParseNode *node = parse_ok("SELECT permissive FROM t1");
  ASSERT_TRUE(NULL != node);
}

TEST_F(TestRlsParser, restrictive_as_identifier)
{
  ParseNode *node = parse_ok("SELECT restrictive FROM t1");
  ASSERT_TRUE(NULL != node);
}

TEST_F(TestRlsParser, bypassrls_as_identifier)
{
  ParseNode *node = parse_ok("SELECT bypassrls FROM t1");
  ASSERT_TRUE(NULL != node);
}

TEST_F(TestRlsParser, nobypassrls_as_identifier)
{
  ParseNode *node = parse_ok("SELECT nobypassrls FROM t1");
  ASSERT_TRUE(NULL != node);
}

// =====================================================================
// BYPASSRLS no longer valid in CREATE/ALTER USER
// =====================================================================

TEST_F(TestRlsParser, alter_user_bypassrls_no_longer_valid)
{
  parse_fail("ALTER USER user1 BYPASSRLS");
}

TEST_F(TestRlsParser, create_user_bypassrls_no_longer_valid)
{
  parse_fail("CREATE USER 'u1' IDENTIFIED BY 'pass' BYPASSRLS");
}

int main(int argc, char **argv)
{
  system("rm -rf test_rls_parser.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_rls_parser.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
