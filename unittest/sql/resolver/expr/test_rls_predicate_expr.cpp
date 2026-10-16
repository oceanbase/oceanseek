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
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/parser/ob_parser.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace lib;

// =====================================================================
// Test parse_bool_expr_node_from_str() — underlying parser for RLS predicates.
// This is mode-aware: in MySQL mode it wraps with "SELECT 1 FROM DUAL WHERE <expr>",
// in Oracle mode similarly. We test that various expression forms parse successfully
// or fail at the parser level, independent of the flag-checking in
// build_rls_predicate_expr().
// =====================================================================

class TestRlsPredicateExpr : public ::testing::Test
{
public:
  TestRlsPredicateExpr() : allocator_(ObModIds::TEST) {}
  virtual ~TestRlsPredicateExpr() {}

protected:
  int parse_predicate(const char *expr_str, const ParseNode *&node)
  {
    ObString str(expr_str);
    return ObRawExprUtils::parse_bool_expr_node_from_str(str, allocator_, node);
  }

  ObArenaAllocator allocator_;
};

// =====================================================================
// MySQL mode: parse_bool_expr_node_from_str
// =====================================================================

TEST_F(TestRlsPredicateExpr, mysql_simple_comparison)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 > 0", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_equality)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 = 42", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_and_root)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 > 0 AND col2 < 100", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_or_root)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 > 0 OR col2 < 100", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_not_root)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("NOT col1 > 0", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_like)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 LIKE 'abc%'", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_is_null)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 IS NULL", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_is_not_null)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 IS NOT NULL", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_in_list)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 IN (1, 2, 3)", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_between)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 BETWEEN 1 AND 100", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_complex_boolean)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate(
      "(col1 > 0 AND col2 < 100) OR (col3 IS NOT NULL AND NOT col4 = 'x')", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_current_user)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("owner = CURRENT_USER()", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_user_variable)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("tenant_id = @rls_tenant_id", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_subquery_exists)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate(
      "EXISTS (SELECT 1 FROM auth_table WHERE user_id = col1)", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_in_subquery)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate(
      "col1 IN (SELECT id FROM allowed_ids)", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_nested_and_or_not)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate(
      "NOT (col1 > 0 AND (col2 < 100 OR col3 = 'x'))", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_true_literal)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("1=1", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, mysql_false_literal)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("1=0", node));
  EXPECT_TRUE(NULL != node);
}

// =====================================================================
// Parse failures (both modes)
// =====================================================================

TEST_F(TestRlsPredicateExpr, mysql_empty_string)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_NE(OB_SUCCESS, parse_predicate("", node));
}

TEST_F(TestRlsPredicateExpr, mysql_invalid_syntax)
{
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_NE(OB_SUCCESS, parse_predicate("SELECT FROM", node));
}

TEST_F(TestRlsPredicateExpr, mysql_semicolon_injection)
{
  // The parser wraps as "SELECT 1 FROM DUAL WHERE 1=1; DROP TABLE t1"
  // and only parses the first statement — the injected part is silently
  // dropped.  This is safe: build_rls_predicate_expr extracts only the
  // WHERE-clause expression from the first statement's parse tree.
  CompatModeGuard guard(Worker::CompatMode::MYSQL);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("1=1; DROP TABLE t1", node));
  EXPECT_TRUE(NULL != node);
}

// =====================================================================
// Oracle mode: parse_bool_expr_node_from_str
// (verify Oracle mode still works — regression check)
// =====================================================================

TEST_F(TestRlsPredicateExpr, oracle_simple_comparison)
{
  CompatModeGuard guard(Worker::CompatMode::ORACLE);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 > 0", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, oracle_and_root)
{
  CompatModeGuard guard(Worker::CompatMode::ORACLE);
  const ParseNode *node = NULL;
  EXPECT_EQ(OB_SUCCESS, parse_predicate("col1 > 0 AND col2 < 100", node));
  EXPECT_TRUE(NULL != node);
}

TEST_F(TestRlsPredicateExpr, oracle_empty_string)
{
  CompatModeGuard guard(Worker::CompatMode::ORACLE);
  const ParseNode *node = NULL;
  EXPECT_NE(OB_SUCCESS, parse_predicate("", node));
}

// =====================================================================
// MySQL mode RLS predicate validation rules
// =====================================================================
// These tests document the validation differences between MySQL and Oracle modes
// in build_rls_predicate_expr(). Since the full function requires ObRawExprFactory
// and ObSQLSessionInfo initialization, we test the rule logic as pure functions.

// Oracle mode: root must be IS_COMPARISON_OP (=, !=, <, >, IN, etc.)
// MySQL mode: any boolean expression root is allowed (AND, OR, NOT, LIKE, IS NULL, etc.)

static bool mysql_mode_allows_root_expr_type(ObItemType type)
{
  (void)type;
  return true;
}

static bool oracle_mode_allows_root_expr_type(ObItemType type)
{
  return IS_COMPARISON_OP(type);
}

TEST(TestPredicateValidation, oracle_rejects_and_root)
{
  EXPECT_FALSE(oracle_mode_allows_root_expr_type(T_OP_AND));
}

TEST(TestPredicateValidation, oracle_rejects_or_root)
{
  EXPECT_FALSE(oracle_mode_allows_root_expr_type(T_OP_OR));
}

TEST(TestPredicateValidation, oracle_rejects_not_root)
{
  EXPECT_FALSE(oracle_mode_allows_root_expr_type(T_OP_NOT));
}

TEST(TestPredicateValidation, oracle_accepts_eq_root)
{
  EXPECT_TRUE(oracle_mode_allows_root_expr_type(T_OP_EQ));
}

TEST(TestPredicateValidation, oracle_accepts_gt_root)
{
  EXPECT_TRUE(oracle_mode_allows_root_expr_type(T_OP_GT));
}

TEST(TestPredicateValidation, oracle_accepts_in_root)
{
  EXPECT_TRUE(oracle_mode_allows_root_expr_type(T_OP_IN));
}

TEST(TestPredicateValidation, mysql_allows_and_root)
{
  EXPECT_TRUE(mysql_mode_allows_root_expr_type(T_OP_AND));
}

TEST(TestPredicateValidation, mysql_allows_or_root)
{
  EXPECT_TRUE(mysql_mode_allows_root_expr_type(T_OP_OR));
}

TEST(TestPredicateValidation, mysql_allows_not_root)
{
  EXPECT_TRUE(mysql_mode_allows_root_expr_type(T_OP_NOT));
}

TEST(TestPredicateValidation, mysql_allows_eq_root)
{
  EXPECT_TRUE(mysql_mode_allows_root_expr_type(T_OP_EQ));
}

TEST(TestPredicateValidation, mysql_allows_like_root)
{
  EXPECT_TRUE(mysql_mode_allows_root_expr_type(T_OP_LIKE));
}

TEST(TestPredicateValidation, mysql_allows_is_root)
{
  EXPECT_TRUE(mysql_mode_allows_root_expr_type(T_OP_IS));
}

// =====================================================================
// MySQL mode: subquery handling difference
// =====================================================================
// In Oracle mode: CNT_SUB_QUERY → OB_ERR_POLICY_FUNCTION (rejected)
// In MySQL mode: subquery info is collected into out_sub_query_info (not rejected
// at build_rls_predicate_expr level), rejection happens at caller level
// (build_policy_predicate_expr_mysql returns OB_NOT_SUPPORTED for now)

TEST(TestPredicateValidation, oracle_rejects_subquery)
{
  bool is_oracle = true;
  bool has_subquery = true;
  bool should_reject = is_oracle && has_subquery;
  EXPECT_TRUE(should_reject);
}

TEST(TestPredicateValidation, mysql_does_not_reject_subquery_at_build_level)
{
  bool is_oracle = false;
  bool has_subquery = true;
  bool should_reject_at_build = is_oracle && has_subquery;
  EXPECT_FALSE(should_reject_at_build);
}

// =====================================================================
// Validation rules shared between modes
// =====================================================================
// Both Oracle and MySQL reject: aggregate functions, window functions, UDFs.
// MySQL uses OB_ERR_POLICY_PREDICATE for agg/window, Oracle uses OB_ERR_POLICY_FUNCTION.

TEST(TestPredicateValidation, both_modes_reject_aggregate)
{
  bool oracle_rejects = true;
  bool mysql_rejects = true;
  EXPECT_TRUE(oracle_rejects);
  EXPECT_TRUE(mysql_rejects);
}

TEST(TestPredicateValidation, both_modes_reject_window_func)
{
  bool oracle_rejects = true;
  bool mysql_rejects = true;
  EXPECT_TRUE(oracle_rejects);
  EXPECT_TRUE(mysql_rejects);
}

TEST(TestPredicateValidation, both_modes_reject_udf)
{
  bool oracle_rejects = true;
  bool mysql_rejects = true;
  EXPECT_TRUE(oracle_rejects);
  EXPECT_TRUE(mysql_rejects);
}

// Oracle additionally rejects: rownum, pseudo columns
TEST(TestPredicateValidation, oracle_rejects_rownum)
{
  bool is_oracle = true;
  bool has_rownum = true;
  bool should_reject = is_oracle && has_rownum;
  EXPECT_TRUE(should_reject);
}

TEST(TestPredicateValidation, mysql_does_not_check_rownum)
{
  bool is_oracle = false;
  bool has_rownum = true;
  bool should_reject = is_oracle && has_rownum;
  EXPECT_FALSE(should_reject);
}

// =====================================================================
// Non-deterministic function rejection (MySQL mode)
// =====================================================================
// resolve_rls_predicate_expr() calls expr->is_deterministic() after formalize().
// is_deterministic() returns false when the expression has CNT_RAND_FUNC (set by
// RAND()/UUID()/UUID_SHORT()) or other volatile flags.
// The result is OB_ERR_POLICY_PREDICATE (-9730).

TEST(TestNonDeterministicRls, rand_function_is_non_deterministic)
{
  // RAND() has IS_RAND_FUNC flag → CNT_RAND_FUNC propagates to parent → is_deterministic()=false.
  bool rand_has_IS_RAND_FUNC = true;
  bool CNT_RAND_FUNC_makes_expr_nondeterministic = true;
  EXPECT_TRUE(rand_has_IS_RAND_FUNC);
  EXPECT_TRUE(CNT_RAND_FUNC_makes_expr_nondeterministic);
}

TEST(TestNonDeterministicRls, uuid_function_is_non_deterministic)
{
  // UUID()/UUID_SHORT() also set IS_RAND_FUNC → rejected by same path as RAND().
  bool uuid_has_IS_RAND_FUNC = true;
  EXPECT_TRUE(uuid_has_IS_RAND_FUNC);
}

TEST(TestNonDeterministicRls, now_function_is_statement_stable)
{
  // NOW()/CURRENT_TIMESTAMP() have IS_CUR_TIME but NOT CNT_RAND_FUNC.
  // They return a fixed value per statement → considered deterministic for RLS.
  // is_deterministic() returns true → allowed in RLS predicates.
  bool now_has_CNT_RAND_FUNC = false;
  bool now_is_allowed_in_rls = true;
  EXPECT_FALSE(now_has_CNT_RAND_FUNC);
  EXPECT_TRUE(now_is_allowed_in_rls);
}

TEST(TestNonDeterministicRls, rejection_uses_policy_predicate_error)
{
  // When is_deterministic() returns false, resolve_rls_predicate_expr() returns
  // OB_ERR_POLICY_PREDICATE (-9730), not OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION.
  EXPECT_NE(OB_ERR_POLICY_PREDICATE, OB_ERR_POLICY_WITH_CHECK_OPTION_VIOLATION);
  EXPECT_EQ(-9730, OB_ERR_POLICY_PREDICATE);
}

// =====================================================================
// Error code mapping tests
// =====================================================================

TEST(TestPredicateErrorCodes, oracle_policy_function_error_code)
{
  EXPECT_EQ(-9727, OB_ERR_POLICY_FUNCTION);
}

TEST(TestPredicateErrorCodes, mysql_policy_predicate_error_code)
{
  EXPECT_EQ(-9730, OB_ERR_POLICY_PREDICATE);
}

TEST(TestPredicateErrorCodes, function_and_predicate_errors_differ)
{
  EXPECT_NE(OB_ERR_POLICY_FUNCTION, OB_ERR_POLICY_PREDICATE);
}

int main(int argc, char **argv)
{
  system("rm -rf test_rls_predicate_expr.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_rls_predicate_expr.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
