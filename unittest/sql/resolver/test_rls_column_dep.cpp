/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/worker.h"
#define private public
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/parser/ob_parser.h"
#include "share/schema/ob_schema_struct.h"
#include "rootserver/ob_rls_ddl_helper.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace lib;
using namespace share::schema;
using namespace rootserver;

// =====================================================================
// check_parse_node_references_column tests
// Column-name comparison follows compat-mode semantics:
//   MySQL mode  -> case-insensitive
//   Oracle mode -> case-sensitive
// driven by CompatModeGuard rather than the tenant's name_case_mode.
// =====================================================================

class TestCheckParseNodeRef : public ::testing::Test
{
public:
  TestCheckParseNodeRef() : allocator_(ObModIds::TEST) {}

protected:
  ParseNode *make_node(ObItemType type, int num_child)
  {
    ParseNode *node = static_cast<ParseNode *>(
        allocator_.alloc(sizeof(ParseNode)));
    EXPECT_TRUE(NULL != node);
    memset(node, 0, sizeof(ParseNode));
    node->type_ = type;
    node->num_child_ = num_child;
    if (num_child > 0) {
      node->children_ = static_cast<ParseNode **>(
          allocator_.alloc(sizeof(ParseNode *) * num_child));
      memset(node->children_, 0, sizeof(ParseNode *) * num_child);
    }
    return node;
  }

  ParseNode *make_column_ref(const char *col_name)
  {
    ParseNode *node = make_node(T_COLUMN_REF, 3);
    // children_[0] = db (NULL), children_[1] = table (NULL), children_[2] = column
    ParseNode *col_node = make_node(T_IDENT, 0);
    col_node->str_value_ = col_name;
    col_node->str_len_ = static_cast<int32_t>(strlen(col_name));
    node->children_[2] = col_node;
    return node;
  }

  ObArenaAllocator allocator_;
};

TEST_F(TestCheckParseNodeRef, null_node_not_referenced)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  bool referenced = true;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      NULL, ObString("col1"), false, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckParseNodeRef, non_column_leaf_not_referenced)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  ParseNode *node = make_node(T_INT, 0);
  bool referenced = true;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      node, ObString("col1"), false, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckParseNodeRef, single_column_ref_match)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  ParseNode *node = make_column_ref("dept_id");
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      node, ObString("dept_id"), false, referenced));
  EXPECT_TRUE(referenced);
}

TEST_F(TestCheckParseNodeRef, single_column_ref_no_match)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  ParseNode *node = make_column_ref("dept_id");
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      node, ObString("salary"), false, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckParseNodeRef, nested_tree_finds_column)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  // Simulate: col1 > 0 AND col2 < 100
  ParseNode *root = make_node(T_OP_AND, 2);
  ParseNode *left = make_node(T_OP_GT, 2);
  left->children_[0] = make_column_ref("col1");
  left->children_[1] = make_node(T_INT, 0);
  ParseNode *right = make_node(T_OP_LT, 2);
  right->children_[0] = make_column_ref("col2");
  right->children_[1] = make_node(T_INT, 0);
  root->children_[0] = left;
  root->children_[1] = right;

  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      root, ObString("col2"), false, referenced));
  EXPECT_TRUE(referenced);
}

TEST_F(TestCheckParseNodeRef, duplicate_column_still_found)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  // col1 > 0 AND col1 < 100
  ParseNode *root = make_node(T_OP_AND, 2);
  ParseNode *left = make_node(T_OP_GT, 2);
  left->children_[0] = make_column_ref("col1");
  left->children_[1] = make_node(T_INT, 0);
  ParseNode *right = make_node(T_OP_LT, 2);
  right->children_[0] = make_column_ref("col1");
  right->children_[1] = make_node(T_INT, 0);
  root->children_[0] = left;
  root->children_[1] = right;

  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      root, ObString("col1"), false, referenced));
  EXPECT_TRUE(referenced);
}

TEST_F(TestCheckParseNodeRef, column_ref_with_fewer_children_not_referenced)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  // T_COLUMN_REF with only 2 children — should not match
  ParseNode *node = make_node(T_COLUMN_REF, 2);
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      node, ObString("col1"), false, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckParseNodeRef, column_ref_null_col_child_not_referenced)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  // T_COLUMN_REF with 3 children but children_[2] = NULL
  ParseNode *node = make_node(T_COLUMN_REF, 3);
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      node, ObString("col1"), false, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckParseNodeRef, oracle_mode_case_sensitive_no_match)
{
  CompatModeGuard guard(Worker::CompatMode::ORACLE);
  ParseNode *node = make_column_ref("COL1");
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      node, ObString("col1"), true, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckParseNodeRef, mysql_mode_case_insensitive_match)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  ParseNode *node = make_column_ref("COL1");
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_parse_node_references_column(
      node, ObString("col1"), false, referenced));
  EXPECT_TRUE(referenced);
}

// =====================================================================
// check_rls_expr_references_column tests
// =====================================================================

class TestCheckRlsExprRef : public ::testing::Test
{
public:
  TestCheckRlsExprRef() : allocator_(ObModIds::TEST) {}

protected:
  ObArenaAllocator allocator_;
};

TEST_F(TestCheckRlsExprRef, empty_expr_not_referenced)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  bool referenced = true;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString(), allocator_, ObString("col1"), false, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckRlsExprRef, simple_expr_referenced_column)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString("col1 = 10"), allocator_, ObString("col1"), false, referenced));
  EXPECT_TRUE(referenced);
}

TEST_F(TestCheckRlsExprRef, simple_expr_unreferenced_column)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  bool referenced = true;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString("col1 = 10"), allocator_, ObString("col2"), false, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckRlsExprRef, compound_expr_both_columns_referenced)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  bool referenced = false;

  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString("col1 > 0 AND col2 < 100"), allocator_, ObString("col1"), false, referenced));
  EXPECT_TRUE(referenced);

  referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString("col1 > 0 AND col2 < 100"), allocator_, ObString("col2"), false, referenced));
  EXPECT_TRUE(referenced);
}

TEST_F(TestCheckRlsExprRef, compound_expr_unrelated_column_not_referenced)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  bool referenced = true;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString("col1 > 0 AND col2 < 100"), allocator_, ObString("salary"), false, referenced));
  EXPECT_FALSE(referenced);
}

TEST_F(TestCheckRlsExprRef, mysql_mode_case_insensitive_match)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString("COL1 = 10"), allocator_, ObString("col1"), false, referenced));
  EXPECT_TRUE(referenced);
}

TEST_F(TestCheckRlsExprRef, complex_nested_expr)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  bool referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString("(dept_id = 10 OR dept_id = 20) AND status = 1"),
      allocator_, ObString("dept_id"), false, referenced));
  EXPECT_TRUE(referenced);

  referenced = false;
  EXPECT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      ObString("(dept_id = 10 OR dept_id = 20) AND status = 1"),
      allocator_, ObString("status"), false, referenced));
  EXPECT_TRUE(referenced);
}

// =====================================================================
// Integration: simulate check_column_referenced_by_rls_policy logic (drop path)
// =====================================================================

TEST(TestDropColumnRlsCheck, column_in_using_expr_blocks_drop)
{
  ObRlsPolicySchema policy;
  policy.set_tenant_id(1);
  policy.set_rls_policy_id(100);
  policy.set_table_id(200);
  policy.set_policy_name(ObString("dept_policy"));
  policy.set_using_expr_str(ObString("dept_id = 10"));

  ObArenaAllocator allocator(ObModIds::TEST);
  CompatModeGuard guard(Worker::CompatMode::MYSQL);

  bool referenced = false;
  ASSERT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      policy.get_using_expr_str(), allocator, ObString("dept_id"), false, referenced));
  EXPECT_TRUE(referenced);
}

TEST(TestDropColumnRlsCheck, column_in_with_check_blocks_drop)
{
  ObRlsPolicySchema policy;
  policy.set_tenant_id(1);
  policy.set_rls_policy_id(100);
  policy.set_table_id(200);
  policy.set_policy_name(ObString("check_policy"));
  policy.set_with_check_str(ObString("status = 1"));

  ObArenaAllocator allocator(ObModIds::TEST);
  CompatModeGuard guard(Worker::CompatMode::MYSQL);

  bool ref_using = false;
  ASSERT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      policy.get_using_expr_str(), allocator, ObString("status"), false, ref_using));
  EXPECT_FALSE(ref_using);

  bool ref_check = false;
  ASSERT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      policy.get_with_check_str(), allocator, ObString("status"), false, ref_check));
  EXPECT_TRUE(ref_check);

  bool blocked = ref_using || ref_check;
  EXPECT_TRUE(blocked);
}

TEST(TestDropColumnRlsCheck, unreferenced_column_allows_drop)
{
  ObRlsPolicySchema policy;
  policy.set_tenant_id(1);
  policy.set_rls_policy_id(100);
  policy.set_table_id(200);
  policy.set_policy_name(ObString("dept_policy"));
  policy.set_using_expr_str(ObString("dept_id = 10"));
  policy.set_with_check_str(ObString("dept_id > 0"));

  ObArenaAllocator allocator(ObModIds::TEST);
  CompatModeGuard guard(Worker::CompatMode::MYSQL);

  bool ref_using = false;
  ASSERT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      policy.get_using_expr_str(), allocator, ObString("salary"), false, ref_using));
  bool ref_check = false;
  ASSERT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      policy.get_with_check_str(), allocator, ObString("salary"), false, ref_check));

  bool blocked = ref_using || ref_check;
  EXPECT_FALSE(blocked);
}

// =====================================================================
// Integration: simulate check_column_referenced_by_rls_policy logic (rename path)
// =====================================================================

TEST(TestRenameColumnRlsCheck, referenced_column_blocks_rename)
{
  ObRlsPolicySchema policy;
  policy.set_tenant_id(1);
  policy.set_rls_policy_id(100);
  policy.set_table_id(200);
  policy.set_policy_name(ObString("dept_policy"));
  policy.set_using_expr_str(ObString("dept_id = 10"));

  ObArenaAllocator allocator(ObModIds::TEST);
  CompatModeGuard guard(Worker::CompatMode::MYSQL);

  bool referenced = false;
  ASSERT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      policy.get_using_expr_str(), allocator, ObString("dept_id"), false, referenced));
  EXPECT_TRUE(referenced);
}

TEST(TestRenameColumnRlsCheck, unreferenced_column_allows_rename)
{
  ObRlsPolicySchema policy;
  policy.set_tenant_id(1);
  policy.set_rls_policy_id(100);
  policy.set_table_id(200);
  policy.set_policy_name(ObString("dept_policy"));
  policy.set_using_expr_str(ObString("dept_id = 10"));

  ObArenaAllocator allocator(ObModIds::TEST);
  CompatModeGuard guard(Worker::CompatMode::MYSQL);

  bool referenced = false;
  ASSERT_EQ(OB_SUCCESS, ObRlsDDLHelper::check_rls_expr_references_column(
      policy.get_using_expr_str(), allocator, ObString("name"), false, referenced));
  EXPECT_FALSE(referenced);
}

int main(int argc, char **argv)
{
  system("rm -rf test_rls_column_dep.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_rls_column_dep.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
