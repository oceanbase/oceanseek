/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_SYNC_CATALOG_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_SYNC_CATALOG_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObSyncCatalogStmt;

class ObSyncCatalogResolver : public ObDDLResolver
{
public:
  explicit ObSyncCatalogResolver(ObResolverParams &params)
      : ObDDLResolver(params)
  {
  }
  virtual ~ObSyncCatalogResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  int resolve_sync_target(ObSyncCatalogStmt &stmt);
  int resolve_table(const ParseNode &database_node,
                    const ParseNode &table_node,
                    ObSyncCatalogStmt &stmt);
  int resolve_database(const ParseNode &database_node, ObSyncCatalogStmt &stmt);
  DISALLOW_COPY_AND_ASSIGN(ObSyncCatalogResolver);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_SYNC_CATALOG_RESOLVER_H_
