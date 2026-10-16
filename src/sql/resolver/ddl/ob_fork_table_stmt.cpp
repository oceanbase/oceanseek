/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_fork_table_stmt.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObForkTableStmt::ObForkTableStmt(ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_FORK_TABLE),
    fork_table_arg_()
{
}

ObForkTableStmt::ObForkTableStmt()
  : ObDDLStmt(stmt::T_FORK_TABLE),
    fork_table_arg_()
{
}

ObForkTableStmt::~ObForkTableStmt()
{
}

ObForkDatabaseStmt::ObForkDatabaseStmt(ObIAllocator *name_pool)
  : ObDDLStmt(name_pool, stmt::T_FORK_DATABASE),
    fork_database_arg_()
{
}

ObForkDatabaseStmt::ObForkDatabaseStmt()
  : ObDDLStmt(stmt::T_FORK_DATABASE),
    fork_database_arg_()
{
}

ObForkDatabaseStmt::~ObForkDatabaseStmt()
{
}

}
}
