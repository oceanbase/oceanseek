/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#define private public
#include "sql/resolver/ddl/ob_rls_policy_stmt.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase;
using namespace common;
using namespace share::schema;
using namespace sql;
using namespace obrpc;

// =====================================================================
// ObRlsPolicyDDLStmt — set/get table info
// =====================================================================

TEST(TestRlsPolicyDDLStmt, set_get_table_info_roundtrip)
{
  ObCreateRlsPolicyStmt stmt;
  EXPECT_TRUE(stmt.get_database_name().empty());
  EXPECT_TRUE(stmt.get_table_name().empty());
  stmt.set_database_name(ObString("test_db"));
  stmt.set_table_name(ObString("test_table"));
  stmt.set_is_sys_table(false);
  EXPECT_EQ(ObString("test_db"), stmt.get_database_name());
  EXPECT_EQ(ObString("test_table"), stmt.get_table_name());
  EXPECT_FALSE(stmt.is_sys_table());
}

TEST(TestRlsPolicyDDLStmt, set_get_sys_table)
{
  ObCreateRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("oceanbase"));
  stmt.set_table_name(ObString("__all_sys"));
  stmt.set_is_sys_table(true);
  EXPECT_EQ(ObString("oceanbase"), stmt.get_database_name());
  EXPECT_TRUE(stmt.is_sys_table());
}

// =====================================================================
// Stmt type correctness
// =====================================================================

TEST(TestRlsPolicyDDLStmt, create_stmt_type)
{
  ObCreateRlsPolicyStmt stmt;
  EXPECT_EQ(stmt::T_CREATE_RLS_POLICY, stmt.get_stmt_type());
}

TEST(TestRlsPolicyDDLStmt, drop_stmt_type)
{
  ObDropRlsPolicyStmt stmt;
  EXPECT_EQ(stmt::T_DROP_RLS_POLICY, stmt.get_stmt_type());
}

TEST(TestRlsPolicyDDLStmt, alter_stmt_type)
{
  ObAlterRlsPolicyStmt stmt;
  EXPECT_EQ(stmt::T_ALTER_RLS_POLICY, stmt.get_stmt_type());
}

// =====================================================================
// get_arg() and field access
// =====================================================================

TEST(TestRlsPolicyDDLStmt, create_arg_access)
{
  ObCreateRlsPolicyStmt stmt;
  ObRlsPolicyDDLArg &arg = stmt.get_arg();
  arg.ddl_type_ = OB_DDL_CREATE_RLS_POLICY;
  arg.exec_tenant_id_ = 1001;
  arg.if_not_exist_ = true;
  arg.schema_.set_tenant_id(1001);
  ASSERT_EQ(OB_SUCCESS, arg.schema_.set_policy_name(ObString("p1")));
  EXPECT_EQ(OB_DDL_CREATE_RLS_POLICY, arg.ddl_type_);
  EXPECT_EQ(1001UL, arg.exec_tenant_id_);
  EXPECT_TRUE(arg.if_not_exist_);
  EXPECT_EQ(ObString("p1"), arg.schema_.get_policy_name());
}

TEST(TestRlsPolicyDDLStmt, drop_arg_if_exist)
{
  ObDropRlsPolicyStmt stmt;
  ObRlsPolicyDDLArg &arg = stmt.get_arg();
  arg.ddl_type_ = OB_DDL_DROP_RLS_POLICY;
  arg.if_exist_ = true;
  EXPECT_EQ(OB_DDL_DROP_RLS_POLICY, arg.ddl_type_);
  EXPECT_TRUE(arg.if_exist_);
  EXPECT_FALSE(arg.if_not_exist_);
}

TEST(TestRlsPolicyDDLStmt, alter_arg_rename)
{
  ObAlterRlsPolicyStmt stmt;
  ObRlsPolicyDDLArg &arg = stmt.get_arg();
  arg.ddl_type_ = OB_DDL_ALTER_RLS_POLICY;
  arg.new_policy_name_ = ObString("new_p1");
  arg.option_bitset_.add_member(ObRlsPolicyDDLArg::RENAME_OPT);
  EXPECT_FALSE(arg.option_bitset_.is_empty());
  EXPECT_TRUE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::RENAME_OPT));
  EXPECT_EQ(ObString("new_p1"), arg.new_policy_name_);
}

// =====================================================================
// option_bitset combinations
// =====================================================================

TEST(TestRlsPolicyDDLStmt, option_bitset_using_and_check)
{
  ObRlsPolicyDDLArg arg;
  arg.option_bitset_.add_member(ObRlsPolicyDDLArg::USING_EXPR_OPT);
  arg.option_bitset_.add_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT);
  EXPECT_TRUE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::USING_EXPR_OPT));
  EXPECT_TRUE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::RENAME_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::ROLE_LIST_OPT));
}

TEST(TestRlsPolicyDDLStmt, option_bitset_all_modify_opts)
{
  ObRlsPolicyDDLArg arg;
  arg.option_bitset_.add_member(ObRlsPolicyDDLArg::USING_EXPR_OPT);
  arg.option_bitset_.add_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT);
  arg.option_bitset_.add_member(ObRlsPolicyDDLArg::ROLE_LIST_OPT);
  EXPECT_TRUE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::USING_EXPR_OPT));
  EXPECT_TRUE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT));
  EXPECT_TRUE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::ROLE_LIST_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::RENAME_OPT));
}

TEST(TestRlsPolicyDDLStmt, option_bitset_rename_exclusive)
{
  ObRlsPolicyDDLArg arg;
  arg.option_bitset_.add_member(ObRlsPolicyDDLArg::RENAME_OPT);
  EXPECT_TRUE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::RENAME_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::USING_EXPR_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT));
  EXPECT_FALSE(arg.option_bitset_.has_member(ObRlsPolicyDDLArg::ROLE_LIST_OPT));
}

// =====================================================================
// get_ddl_arg returns the same arg
// =====================================================================

TEST(TestRlsPolicyDDLStmt, get_ddl_arg_returns_same_arg)
{
  ObCreateRlsPolicyStmt stmt;
  ObRlsPolicyDDLArg &arg = stmt.get_arg();
  arg.if_not_exist_ = true;
  ObDDLArg &ddl_arg = stmt.get_ddl_arg();
  ObRlsPolicyDDLArg &cast_arg = static_cast<ObRlsPolicyDDLArg &>(ddl_arg);
  EXPECT_TRUE(cast_arg.if_not_exist_);
}

// =====================================================================
// Alter DDL arg enum values
// =====================================================================

TEST(TestRlsPolicyDDLStmt, alter_option_enum_values_distinct)
{
  EXPECT_NE(ObRlsPolicyDDLArg::ENABLE, ObRlsPolicyDDLArg::PERMISSIVE_OPT);
  EXPECT_NE(ObRlsPolicyDDLArg::PERMISSIVE_OPT, ObRlsPolicyDDLArg::CMD_TYPE_OPT);
  EXPECT_NE(ObRlsPolicyDDLArg::CMD_TYPE_OPT, ObRlsPolicyDDLArg::ROLE_LIST_OPT);
  EXPECT_NE(ObRlsPolicyDDLArg::ROLE_LIST_OPT, ObRlsPolicyDDLArg::USING_EXPR_OPT);
  EXPECT_NE(ObRlsPolicyDDLArg::USING_EXPR_OPT, ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT);
  EXPECT_NE(ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT, ObRlsPolicyDDLArg::RENAME_OPT);
}

TEST(TestRlsPolicyDDLStmt, alter_option_values)
{
  EXPECT_EQ(1, ObRlsPolicyDDLArg::ENABLE);
  EXPECT_EQ(4, ObRlsPolicyDDLArg::PERMISSIVE_OPT);
  EXPECT_EQ(5, ObRlsPolicyDDLArg::CMD_TYPE_OPT);
  EXPECT_EQ(6, ObRlsPolicyDDLArg::ROLE_LIST_OPT);
  EXPECT_EQ(7, ObRlsPolicyDDLArg::USING_EXPR_OPT);
  EXPECT_EQ(8, ObRlsPolicyDDLArg::WITH_CHECK_EXPR_OPT);
  EXPECT_EQ(9, ObRlsPolicyDDLArg::RENAME_OPT);
  EXPECT_EQ(100, ObRlsPolicyDDLArg::MAX_OPTION);
}

// =====================================================================
// Default init state
// =====================================================================

TEST(TestRlsPolicyDDLStmt, default_init_empty)
{
  ObCreateRlsPolicyStmt stmt;
  EXPECT_TRUE(stmt.get_database_name().empty());
  EXPECT_TRUE(stmt.get_table_name().empty());
  EXPECT_FALSE(stmt.is_sys_table());
  ObDropRlsPolicyStmt stmt2;
  EXPECT_TRUE(stmt2.get_database_name().empty());
  ObAlterRlsPolicyStmt stmt3;
  EXPECT_TRUE(stmt3.get_database_name().empty());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
