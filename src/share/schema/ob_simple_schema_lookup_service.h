/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_LOOKUP_SERVICE_H_
#define OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_LOOKUP_SERVICE_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObISQLClient;
class ObString;
}
namespace share
{
namespace schema
{

class ObSchemaGetterGuard;
class ObSchemaService;
class ObSimpleTableSchemaV2;
struct ObRefreshSchemaStatus;
struct ObSimpleForeignKeyInfo;
struct ObSimpleConstraintInfo;

// Fetch and post-process helpers for ON_DEMAND simple schema backsource.
// Schema version range lookup lives in ObSchemaIDResolver / ObSchemaNameResolver.
class ObSimpleSchemaLookupService
{
public:
  static int fetch_simple_table_schema(common::ObISQLClient &sql_client,
                                       ObSchemaService &schema_service,
                                       const ObRefreshSchemaStatus &schema_status,
                                       const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       const int64_t precise_schema_version,
                                       common::ObIAllocator &allocator,
                                       const ObSimpleTableSchemaV2 *&schema);

  static int get_foreign_key_info_by_name(common::ObISQLClient &sql_client,
                                          ObSchemaService &schema_service,
                                          const ObRefreshSchemaStatus &schema_status,
                                          const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          const common::ObString &foreign_key_name,
                                          const int64_t snapshot_version,
                                          ObSimpleForeignKeyInfo &info);

  static int get_constraint_info_by_name(common::ObISQLClient &sql_client,
                                         ObSchemaService &schema_service,
                                         const ObRefreshSchemaStatus &schema_status,
                                         const uint64_t tenant_id,
                                         const uint64_t database_id,
                                         const common::ObString &constraint_name,
                                         const int64_t snapshot_version,
                                         ObSimpleConstraintInfo &info);

  static int post_process_simple_table_schema(ObSchemaGetterGuard &guard,
                                              const uint64_t tenant_id,
                                              ObSimpleTableSchemaV2 &table_schema);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_SIMPLE_SCHEMA_LOOKUP_SERVICE_H_
