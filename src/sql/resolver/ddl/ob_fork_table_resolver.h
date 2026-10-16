/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_FORK_TABLE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_FORK_TABLE_RESOLVER_H_ 1

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_fork_table_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObForkTableResolver : public ObDDLResolver
{
public:
  enum node_type {
    DST_TABLE_NODE = 0,
    SRC_TABLE_NODE,
    READ_LOCK_NODE,
    MAX_NODE
  };
public:
  explicit ObForkTableResolver(ObResolverParams &params);
  virtual ~ObForkTableResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObForkTableResolver);
};

class ObForkDatabaseResolver : public ObDDLResolver
{
public:
  enum node_type {
    DST_DATABASE_NODE = 0,
    SRC_DATABASE_NODE,
    MAX_NODE
  };
public:
  explicit ObForkDatabaseResolver(ObResolverParams &params);
  virtual ~ObForkDatabaseResolver();

  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObForkDatabaseResolver);
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_DDL_OB_FORK_TABLE_RESOLVER_H_ */
