/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_ID_RESOLVER_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_ID_RESOLVER_H_

#include "share/schema/ob_simple_schema_kv_cache.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}
namespace share
{
namespace schema
{

struct ObRefreshSchemaStatus;
class ObServerSchemaService;

class ObSchemaIDResolver
{
public:
  // Resolve [start, end] for table_id under snapshot_version.
  // Always fills a cacheable range for exist / not-exist / deleted.
  static int get_schema_version_range_by_id(common::ObISQLClient &sql_client,
                                            const ObRefreshSchemaStatus &schema_status,
                                            ObServerSchemaService *schema_service,
                                            const uint64_t tenant_id,
                                            const uint64_t table_id,
                                            const int64_t snapshot_version,
                                            ObSchemaResolveRange &version_range);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_SCHEMA_ID_RESOLVER_H_
