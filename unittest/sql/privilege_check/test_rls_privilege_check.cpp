/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#define private public
#include "sql/privilege_check/ob_privilege_check.h"
#include "sql/resolver/ddl/ob_rls_policy_stmt.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/worker.h"

using namespace oceanbase;
using namespace common;
using namespace share::schema;
using namespace sql;

// =====================================================================
// get_rls_policy_ddl_stmt_need_privs is a free function registered via
// OB_STMT_TYPE_DEF. We declare it here so we can call it directly.
// =====================================================================

namespace oceanbase {
namespace sql {
extern int get_rls_policy_ddl_stmt_need_privs(
    const share::schema::ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    common::ObIArray<share::schema::ObNeedPriv> &need_privs);
} // namespace sql
} // namespace oceanbase

// =====================================================================
// Null stmt
// =====================================================================

TEST(TestRlsPrivCheck, null_stmt_returns_error)
{
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  ObSessionPrivInfo session_priv;
  session_priv.tenant_id_ = 1;
  session_priv.user_id_ = 1;
  ObSEArray<ObNeedPriv, 4> need_privs;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            get_rls_policy_ddl_stmt_need_privs(session_priv, NULL, need_privs));
}

// =====================================================================
// Oracle mode is a no-op
// =====================================================================

TEST(TestRlsPrivCheck, oracle_mode_noop)
{
  lib::CompatModeGuard guard(lib::Worker::CompatMode::ORACLE);
  ObSessionPrivInfo session_priv;
  session_priv.tenant_id_ = 1;
  session_priv.user_id_ = 1;
  ObCreateRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("db1"));
  stmt.set_table_name(ObString("t1"));
  ObSEArray<ObNeedPriv, 4> need_privs;
  EXPECT_EQ(OB_SUCCESS,
            get_rls_policy_ddl_stmt_need_privs(session_priv, &stmt, need_privs));
  EXPECT_EQ(1, need_privs.count());
}

// =====================================================================
// Create stmt — normal case: need_priv added
// =====================================================================

TEST(TestRlsPrivCheck, create_stmt_adds_need_priv)
{
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  ObSessionPrivInfo session_priv;
  session_priv.tenant_id_ = 1;
  session_priv.user_id_ = 1;
  session_priv.user_name_ = ObString("test_user");
  session_priv.host_name_ = ObString("%");
  ObCreateRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("user_db"));
  stmt.set_table_name(ObString("user_tbl"));
  ObSEArray<ObNeedPriv, 4> need_privs;
  int ret = get_rls_policy_ddl_stmt_need_privs(session_priv, &stmt, need_privs);
  EXPECT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, need_privs.count());
  EXPECT_EQ(ObString("user_db"), need_privs.at(0).db_);
  EXPECT_EQ(ObString("user_tbl"), need_privs.at(0).table_);
  EXPECT_EQ(OB_PRIV_ALTER, need_privs.at(0).priv_set_);
}

// =====================================================================
// Drop stmt works the same way
// =====================================================================

TEST(TestRlsPrivCheck, drop_stmt_adds_need_priv)
{
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  ObSessionPrivInfo session_priv;
  session_priv.tenant_id_ = 1;
  session_priv.user_id_ = 1;
  session_priv.user_name_ = ObString("test_user");
  session_priv.host_name_ = ObString("%");
  ObDropRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("db2"));
  stmt.set_table_name(ObString("tbl2"));
  ObSEArray<ObNeedPriv, 4> need_privs;
  int ret = get_rls_policy_ddl_stmt_need_privs(session_priv, &stmt, need_privs);
  EXPECT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, need_privs.count());
  EXPECT_EQ(ObString("db2"), need_privs.at(0).db_);
}

// =====================================================================
// Alter stmt works the same way
// =====================================================================

TEST(TestRlsPrivCheck, alter_stmt_adds_need_priv)
{
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  ObSessionPrivInfo session_priv;
  session_priv.tenant_id_ = 1;
  session_priv.user_id_ = 1;
  session_priv.user_name_ = ObString("test_user");
  session_priv.host_name_ = ObString("%");
  ObAlterRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("db3"));
  stmt.set_table_name(ObString("tbl3"));
  ObSEArray<ObNeedPriv, 4> need_privs;
  int ret = get_rls_policy_ddl_stmt_need_privs(session_priv, &stmt, need_privs);
  EXPECT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, need_privs.count());
}

// =====================================================================
// Stmt without stored table info uses empty db/table names
// =====================================================================

TEST(TestRlsPrivCheck, default_stmt_uses_empty_names)
{
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  ObSessionPrivInfo session_priv;
  session_priv.tenant_id_ = 1;
  session_priv.user_id_ = 1;
  session_priv.user_name_ = ObString("test_user");
  session_priv.host_name_ = ObString("%");
  ObCreateRlsPolicyStmt stmt;
  EXPECT_TRUE(stmt.get_database_name().empty());
  ObSEArray<ObNeedPriv, 4> need_privs;
  int ret = get_rls_policy_ddl_stmt_need_privs(session_priv, &stmt, need_privs);
  EXPECT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, need_privs.count());
  EXPECT_TRUE(need_privs.at(0).db_.empty());
  EXPECT_TRUE(need_privs.at(0).table_.empty());
}

// =====================================================================
// information_schema DB is rejected (can_do_operation_on_db)
// =====================================================================

TEST(TestRlsPrivCheck, information_schema_rejected)
{
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  ObSessionPrivInfo session_priv;
  // Non-sys tenant: information_schema is rejected by can_do_operation_on_db
  session_priv.tenant_id_ = 1001;
  session_priv.user_id_ = 1;
  session_priv.user_name_ = ObString("test_user");
  session_priv.host_name_ = ObString("%");
  ObCreateRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("information_schema"));
  stmt.set_table_name(ObString("some_tbl"));
  ObSEArray<ObNeedPriv, 4> need_privs;
  int ret = get_rls_policy_ddl_stmt_need_privs(session_priv, &stmt, need_privs);
  EXPECT_NE(OB_SUCCESS, ret);
}

// =====================================================================
// mysql org table rejected for non-sys tenant
// =====================================================================

TEST(TestRlsPrivCheck, mysql_org_table_rejected)
{
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  ObSessionPrivInfo session_priv;
  session_priv.tenant_id_ = 1001;
  session_priv.user_id_ = 1;
  session_priv.user_name_ = ObString("user1");
  session_priv.host_name_ = ObString("localhost");
  ObCreateRlsPolicyStmt stmt;
  stmt.set_database_name(ObString("mysql"));
  stmt.set_table_name(ObString("user"));
  stmt.set_is_sys_table(true);
  ObSEArray<ObNeedPriv, 4> need_privs;
  int ret = get_rls_policy_ddl_stmt_need_privs(session_priv, &stmt, need_privs);
  EXPECT_EQ(OB_ERR_NO_TABLE_PRIVILEGE, ret);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
