/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_SEARCH_INDEX_CONFIG_RESOLVER_H_
#define OCEANBASE_SQL_OB_SEARCH_INDEX_CONFIG_RESOLVER_H_

#include "share/search_index/ob_search_index_config.h"
#include "sql/parser/parse_node.h"

namespace oceanbase
{
namespace sql
{
class ObSchemaChecker;

class ObSearchIndexConfigResolver final
{
public:
  // Each block uses fixed defaults for its unspecified properties.
  // Returns a comment compatible with the tenant data version; FULLTEXT requires 4.6.3.1.
  static int resolve(const ParseNode *column_options,
                     const common::ObString &database_name,
                     uint64_t tenant_id,
                     uint64_t tenant_data_version,
                     ObSchemaChecker *schema_checker,
                     const common::ObObjMeta &source_meta,
                     common::ObCollationType connection_collation,
                     common::ObSqlString &comment);
};

} // namespace sql
} // namespace oceanbase
#endif
