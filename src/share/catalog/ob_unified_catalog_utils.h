/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_UTILS_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_UTILS_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}

class ObUnifiedCatalogUtils
{
public:
  static int resolve_catalog_id(const uint64_t tenant_id,
                                const common::ObString &access_token,
                                const common::ObString &remote_catalog_name,
                                schema::ObSchemaGetterGuard &schema_guard,
                                common::ObIAllocator &allocator,
                                uint64_t &child_catalog_id);
  static int resolve_catalog_name(const uint64_t tenant_id,
                                  const common::ObString &access_token,
                                  const uint64_t child_catalog_id,
                                  schema::ObSchemaGetterGuard &schema_guard,
                                  common::ObIAllocator &allocator,
                                  common::ObString &remote_catalog_name);
  static int try_resolve_catalog(const uint64_t tenant_id,
                                 const common::ObString &access_token,
                                 const common::ObString &requested_catalog_name,
                                 const uint64_t current_catalog_id,
                                 const common::ObNameCaseMode case_mode,
                                 schema::ObSchemaGetterGuard &schema_guard,
                                 common::ObIAllocator &allocator,
                                 uint64_t &catalog_id,
                                 common::ObString &remote_catalog_name,
                                 bool &is_remote_catalog);
  static int validate_database(const uint64_t tenant_id,
                               const common::ObString &access_token,
                               const common::ObString &remote_catalog_name,
                               const common::ObString &database_name,
                               schema::ObSchemaGetterGuard &schema_guard,
                               common::ObIAllocator &allocator,
                               bool &need_local_db_access);

private:
  static int check_local_catalog_conflict(const uint64_t tenant_id,
                                          const common::ObString &remote_catalog_name,
                                          schema::ObSchemaGetterGuard &schema_guard);
  DISALLOW_COPY_AND_ASSIGN(ObUnifiedCatalogUtils);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_UTILS_H_
