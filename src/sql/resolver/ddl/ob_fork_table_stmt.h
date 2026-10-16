/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_FORK_TABLE_STMT_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_FORK_TABLE_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObForkTableStmt : public ObDDLStmt
{
public:
  explicit ObForkTableStmt(common::ObIAllocator *name_pool);
  ObForkTableStmt();
  virtual ~ObForkTableStmt();

  const obrpc::ObForkTableArg &get_fork_table_arg() const { return fork_table_arg_; }
  obrpc::ObForkTableArg &get_fork_table_arg() { return fork_table_arg_; }
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return fork_table_arg_; }

  TO_STRING_KV(K_(stmt_type), K_(fork_table_arg));
private:
  obrpc::ObForkTableArg fork_table_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObForkTableStmt);
};

class ObForkDatabaseStmt : public ObDDLStmt
{
public:
  explicit ObForkDatabaseStmt(common::ObIAllocator *name_pool);
  ObForkDatabaseStmt();
  virtual ~ObForkDatabaseStmt();

  const obrpc::ObForkDatabaseArg &get_fork_database_arg() const { return fork_database_arg_; }
  obrpc::ObForkDatabaseArg &get_fork_database_arg() { return fork_database_arg_; }
  virtual bool cause_implicit_commit() const { return true; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return fork_database_arg_; }

  TO_STRING_KV(K_(stmt_type), K_(fork_database_arg));
private:
  obrpc::ObForkDatabaseArg fork_database_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObForkDatabaseStmt);
};

}
}

#endif //OCEANBASE_SQL_RESOLVER_DDL_OB_FORK_TABLE_STMT_
