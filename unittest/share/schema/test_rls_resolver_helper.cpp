/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <cstring>
#define private public
#define protected public
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ddl/ob_rls_policy_resolver_helper.h"
#include "sql/parser/parse_node.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/worker.h"
#include "common/ob_smart_var.h"

using namespace oceanbase;
using namespace common;
using namespace share::schema;
using namespace sql;
using namespace share;

static ParseNode make_int_node(int64_t value)
{
  ParseNode node;
  memset(&node, 0, sizeof(node));
  node.type_ = T_INT;
  node.value_ = value;
  return node;
}

static ParseNode make_str_node(const char *str)
{
  ParseNode node;
  memset(&node, 0, sizeof(node));
  node.type_ = T_VARCHAR;
  node.str_value_ = str;
  node.str_len_ = strlen(str);
  return node;
}

// 初始化并返回一个可用的 ObSQLSessionInfo，调用方负责其生命周期
// mode: MYSQL 模式下 lower_case_table_names=1 触发大小写归一化
static int init_session(ObSQLSessionInfo &session, ObIAllocator &alloc,
                        lib::Worker::CompatMode compat_mode,
                        int64_t lower_case_mode /* OB_ORIGIN_AND_SENSITIVE=0, OB_LOWERCASE_AND_INSENSITIVE=1 */)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPreProcessSysVars::init_sys_var())) {
    // 允许重复初始化
    if (OB_INIT_TWICE == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session.test_init(0, 0, 0, &alloc))) {
  } else if (OB_FAIL(session.load_default_sys_variable(false, false))) {
  } else {
    THIS_WORKER.set_compatibility_mode(compat_mode);
    ObObj val;
    val.set_int(lower_case_mode);
    ret = session.update_sys_variable(SYS_VAR_LOWER_CASE_TABLE_NAMES, val);
  }
  return ret;
}

// =====================================================================
// resolve_stmt_type Tests
// =====================================================================

TEST(TestResolverHelper, stmt_type_null_node_defaults_to_all)
{
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(NULL, schema));
  EXPECT_EQ(RLS_POLICY_SELECT_FLAG | RLS_POLICY_INSERT_FLAG
            | RLS_POLICY_UPDATE_FLAG | RLS_POLICY_DELETE_FLAG,
            schema.get_stmt_type());
}

TEST(TestResolverHelper, stmt_type_value_0_all)
{
  ParseNode node = make_int_node(0);
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_EQ(RLS_POLICY_SELECT_FLAG | RLS_POLICY_INSERT_FLAG
            | RLS_POLICY_UPDATE_FLAG | RLS_POLICY_DELETE_FLAG,
            schema.get_stmt_type());
}

TEST(TestResolverHelper, stmt_type_value_1_select)
{
  ParseNode node = make_int_node(1);
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_EQ(RLS_POLICY_SELECT_FLAG, schema.get_stmt_type());
}

TEST(TestResolverHelper, stmt_type_value_2_insert)
{
  ParseNode node = make_int_node(2);
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_EQ(RLS_POLICY_INSERT_FLAG, schema.get_stmt_type());
}

TEST(TestResolverHelper, stmt_type_value_3_update)
{
  ParseNode node = make_int_node(3);
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_EQ(RLS_POLICY_UPDATE_FLAG, schema.get_stmt_type());
}

TEST(TestResolverHelper, stmt_type_value_4_delete)
{
  ParseNode node = make_int_node(4);
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_EQ(RLS_POLICY_DELETE_FLAG, schema.get_stmt_type());
}

TEST(TestResolverHelper, stmt_type_invalid_value)
{
  ParseNode node = make_int_node(5);
  ObRlsPolicySchema schema;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
}

TEST(TestResolverHelper, stmt_type_negative_value)
{
  ParseNode node = make_int_node(-1);
  ObRlsPolicySchema schema;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
}

// =====================================================================
// Stmt Type Bitmask Semantics
// =====================================================================

TEST(TestResolverHelper, stmt_type_flags_are_disjoint)
{
  EXPECT_EQ(0, RLS_POLICY_SELECT_FLAG & RLS_POLICY_INSERT_FLAG);
  EXPECT_EQ(0, RLS_POLICY_SELECT_FLAG & RLS_POLICY_UPDATE_FLAG);
  EXPECT_EQ(0, RLS_POLICY_SELECT_FLAG & RLS_POLICY_DELETE_FLAG);
  EXPECT_EQ(0, RLS_POLICY_INSERT_FLAG & RLS_POLICY_UPDATE_FLAG);
  EXPECT_EQ(0, RLS_POLICY_INSERT_FLAG & RLS_POLICY_DELETE_FLAG);
  EXPECT_EQ(0, RLS_POLICY_UPDATE_FLAG & RLS_POLICY_DELETE_FLAG);
}

TEST(TestResolverHelper, stmt_type_all_covers_individual_flags)
{
  ParseNode node = make_int_node(0);
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));

  EXPECT_TRUE(schema.has_stmt_type_flag(RLS_POLICY_SELECT_FLAG));
  EXPECT_TRUE(schema.has_stmt_type_flag(RLS_POLICY_INSERT_FLAG));
  EXPECT_TRUE(schema.has_stmt_type_flag(RLS_POLICY_UPDATE_FLAG));
  EXPECT_TRUE(schema.has_stmt_type_flag(RLS_POLICY_DELETE_FLAG));
}

TEST(TestResolverHelper, stmt_type_select_only)
{
  ParseNode node = make_int_node(1);
  ObRlsPolicySchema schema;
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));

  EXPECT_TRUE(schema.has_stmt_type_flag(RLS_POLICY_SELECT_FLAG));
  EXPECT_FALSE(schema.has_stmt_type_flag(RLS_POLICY_INSERT_FLAG));
  EXPECT_FALSE(schema.has_stmt_type_flag(RLS_POLICY_UPDATE_FLAG));
  EXPECT_FALSE(schema.has_stmt_type_flag(RLS_POLICY_DELETE_FLAG));
}

// =====================================================================
// CREATE POLICY Semantic Rules: stmt_type / expression compatibility
// =====================================================================
// These tests encode the business rules enforced in
// ObCreateRlsPolicyResolver::resolve() and ObAlterRlsPolicyResolver::resolve():
//   - INSERT-only policy must NOT have USING expression
//   - SELECT-only or DELETE-only policy must NOT have WITH CHECK expression
//   - ALL and UPDATE policies allow both USING and WITH CHECK

static bool is_using_allowed(int64_t stmt_type)
{
  return stmt_type != RLS_POLICY_INSERT_FLAG;
}

static bool is_with_check_allowed(int64_t stmt_type)
{
  return stmt_type != RLS_POLICY_SELECT_FLAG
         && stmt_type != RLS_POLICY_DELETE_FLAG;
}

TEST(TestCreatePolicySemantics, insert_disallows_using)
{
  ObRlsPolicySchema schema;
  ParseNode node = make_int_node(2);
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_FALSE(is_using_allowed(schema.get_stmt_type()));
}

TEST(TestCreatePolicySemantics, select_disallows_with_check)
{
  ObRlsPolicySchema schema;
  ParseNode node = make_int_node(1);
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_FALSE(is_with_check_allowed(schema.get_stmt_type()));
}

TEST(TestCreatePolicySemantics, delete_disallows_with_check)
{
  ObRlsPolicySchema schema;
  ParseNode node = make_int_node(4);
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_FALSE(is_with_check_allowed(schema.get_stmt_type()));
}

TEST(TestCreatePolicySemantics, all_allows_both)
{
  ObRlsPolicySchema schema;
  ParseNode node = make_int_node(0);
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_TRUE(is_using_allowed(schema.get_stmt_type()));
  EXPECT_TRUE(is_with_check_allowed(schema.get_stmt_type()));
}

TEST(TestCreatePolicySemantics, select_allows_using)
{
  ObRlsPolicySchema schema;
  ParseNode node = make_int_node(1);
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_TRUE(is_using_allowed(schema.get_stmt_type()));
}

TEST(TestCreatePolicySemantics, insert_allows_with_check)
{
  ObRlsPolicySchema schema;
  ParseNode node = make_int_node(2);
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_TRUE(is_with_check_allowed(schema.get_stmt_type()));
}

TEST(TestCreatePolicySemantics, update_allows_both)
{
  ObRlsPolicySchema schema;
  ParseNode node = make_int_node(3);
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_TRUE(is_using_allowed(schema.get_stmt_type()));
  EXPECT_TRUE(is_with_check_allowed(schema.get_stmt_type()));
}

TEST(TestCreatePolicySemantics, delete_allows_using)
{
  ObRlsPolicySchema schema;
  ParseNode node = make_int_node(4);
  ASSERT_EQ(OB_SUCCESS, ObRlsPolicyResolverHelper::resolve_stmt_type(&node, schema));
  EXPECT_TRUE(is_using_allowed(schema.get_stmt_type()));
}

// =====================================================================
// resolve_policy_name Tests
// =====================================================================

TEST(TestResolverHelper, policy_name_null_node_returns_error)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc(ObModIds::OB_SQL_SESSION);
  SMART_VAR(ObSQLSessionInfo, session) {
    ASSERT_EQ(OB_SUCCESS, init_session(session, alloc, lib::Worker::CompatMode::MYSQL,
                                       OB_ORIGIN_AND_SENSITIVE));
    ObRlsPolicySchema schema;
    EXPECT_EQ(OB_ERR_UNEXPECTED,
              ObRlsPolicyResolverHelper::resolve_policy_name(NULL, session, alloc, schema));
  }
}

// Oracle 模式：名字保持原始大小写
TEST(TestResolverHelper, policy_name_oracle_mode_preserves_case)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc(ObModIds::OB_SQL_SESSION);
  SMART_VAR(ObSQLSessionInfo, session) {
    ASSERT_EQ(OB_SUCCESS, init_session(session, alloc, lib::Worker::CompatMode::ORACLE,
                                       OB_ORIGIN_AND_SENSITIVE));
    ParseNode node = make_str_node("My_Policy");
    ObRlsPolicySchema schema;
    ASSERT_EQ(OB_SUCCESS,
              ObRlsPolicyResolverHelper::resolve_policy_name(&node, session, alloc, schema));
    EXPECT_EQ(ObString("My_Policy"), schema.get_policy_name());
  }
}

// MySQL 模式 + OB_ORIGIN_AND_SENSITIVE (lower_case_table_names=0)：名字保持原始大小写
TEST(TestResolverHelper, policy_name_mysql_case_sensitive_preserves_case)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc(ObModIds::OB_SQL_SESSION);
  SMART_VAR(ObSQLSessionInfo, session) {
    ASSERT_EQ(OB_SUCCESS, init_session(session, alloc, lib::Worker::CompatMode::MYSQL,
                                       OB_ORIGIN_AND_SENSITIVE));
    ParseNode node = make_str_node("My_Policy");
    ObRlsPolicySchema schema;
    ASSERT_EQ(OB_SUCCESS,
              ObRlsPolicyResolverHelper::resolve_policy_name(&node, session, alloc, schema));
    EXPECT_EQ(ObString("My_Policy"), schema.get_policy_name());
  }
}

// MySQL 模式 + OB_LOWERCASE_AND_INSENSITIVE (lower_case_table_names=1)：大写转小写
TEST(TestResolverHelper, policy_name_mysql_lowercase_mode_converts_to_lower)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc(ObModIds::OB_SQL_SESSION);
  SMART_VAR(ObSQLSessionInfo, session) {
    ASSERT_EQ(OB_SUCCESS, init_session(session, alloc, lib::Worker::CompatMode::MYSQL,
                                       OB_LOWERCASE_AND_INSENSITIVE));
    ParseNode node = make_str_node("My_Policy");
    ObRlsPolicySchema schema;
    ASSERT_EQ(OB_SUCCESS,
              ObRlsPolicyResolverHelper::resolve_policy_name(&node, session, alloc, schema));
    EXPECT_EQ(ObString("my_policy"), schema.get_policy_name());
  }
}

// MySQL 模式 + OB_LOWERCASE_AND_INSENSITIVE：纯小写名字不变
TEST(TestResolverHelper, policy_name_mysql_lowercase_mode_already_lower)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc(ObModIds::OB_SQL_SESSION);
  SMART_VAR(ObSQLSessionInfo, session) {
    ASSERT_EQ(OB_SUCCESS, init_session(session, alloc, lib::Worker::CompatMode::MYSQL,
                                       OB_LOWERCASE_AND_INSENSITIVE));
    ParseNode node = make_str_node("my_policy");
    ObRlsPolicySchema schema;
    ASSERT_EQ(OB_SUCCESS,
              ObRlsPolicyResolverHelper::resolve_policy_name(&node, session, alloc, schema));
    EXPECT_EQ(ObString("my_policy"), schema.get_policy_name());
  }
}

int main(int argc, char **argv)
{
  system("rm -rf test_rls_resolver_helper.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_file_name("test_rls_resolver_helper.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
