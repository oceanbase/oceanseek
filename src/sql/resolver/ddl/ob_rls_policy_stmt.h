/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_RESOLVER_DDL_RLS_POLICY_STMT_H_
#define OCEANBASE_SQL_OB_RESOLVER_DDL_RLS_POLICY_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObRlsPolicyDDLStmt : public ObDDLStmt
{
public:
  explicit ObRlsPolicyDDLStmt(common::ObIAllocator *name_pool, stmt::StmtType type)
    : ObDDLStmt(name_pool, type), arg_(), db_name_(), table_name_(), is_sys_table_(false)
  {}
  ObRlsPolicyDDLStmt(stmt::StmtType type)
    : ObDDLStmt(type), arg_(), db_name_(), table_name_(), is_sys_table_(false)
  {}
  virtual ~ObRlsPolicyDDLStmt() = default;
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
  obrpc::ObRlsPolicyDDLArg &get_arg() { return arg_; }

  void set_database_name(const common::ObString &db_name) { db_name_ = db_name; }
  void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  void set_is_sys_table(bool is_sys_table) { is_sys_table_ = is_sys_table; }
  const common::ObString &get_database_name() const { return db_name_; }
  const common::ObString &get_table_name() const { return table_name_; }
  bool is_sys_table() const { return is_sys_table_; }

private:
  obrpc::ObRlsPolicyDDLArg arg_;
  common::ObString db_name_;
  common::ObString table_name_;
  bool is_sys_table_;
  DISALLOW_COPY_AND_ASSIGN(ObRlsPolicyDDLStmt);
};

class ObCreateRlsPolicyStmt : public ObRlsPolicyDDLStmt
{
public:
  explicit ObCreateRlsPolicyStmt(common::ObIAllocator *name_pool)
    : ObRlsPolicyDDLStmt(name_pool, stmt::T_CREATE_RLS_POLICY)
  {}
  ObCreateRlsPolicyStmt()
    : ObRlsPolicyDDLStmt(stmt::T_CREATE_RLS_POLICY)
  {}
  virtual ~ObCreateRlsPolicyStmt() = default;
};

class ObDropRlsPolicyStmt : public ObRlsPolicyDDLStmt
{
public:
  explicit ObDropRlsPolicyStmt(common::ObIAllocator *name_pool)
    : ObRlsPolicyDDLStmt(name_pool, stmt::T_DROP_RLS_POLICY)
  {}
  ObDropRlsPolicyStmt()
    : ObRlsPolicyDDLStmt(stmt::T_DROP_RLS_POLICY)
  {}
  virtual ~ObDropRlsPolicyStmt() = default;
};

class ObAlterRlsPolicyStmt : public ObRlsPolicyDDLStmt
{
public:
  explicit ObAlterRlsPolicyStmt(common::ObIAllocator *name_pool)
    : ObRlsPolicyDDLStmt(name_pool, stmt::T_ALTER_RLS_POLICY)
  {}
  ObAlterRlsPolicyStmt()
    : ObRlsPolicyDDLStmt(stmt::T_ALTER_RLS_POLICY)
  {}
  virtual ~ObAlterRlsPolicyStmt() = default;
};

} /* sql */
} /* oceanbase */
#endif // OCEANBASE_SQL_OB_RESOLVER_DDL_RLS_POLICY_STMT_H_
