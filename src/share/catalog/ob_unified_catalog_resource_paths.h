/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_RESOURCE_PATHS_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_RESOURCE_PATHS_H_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{

/** Builds Unified Catalog REST resource paths and percent-encodes URL components. */
class ObUnifiedCatalogResourcePaths
{
public:
  static int build_catalog_collection_path(common::ObSqlString &path);
  static int build_catalog_path(const common::ObString &catalog_name, common::ObSqlString &path);
  static int build_namespace_collection_path(const common::ObString &catalog_name,
                                             common::ObSqlString &path);
  static int build_namespace_collection_page_path(const common::ObString &catalog_name,
                                                  const common::ObString &page_token,
                                                  common::ObSqlString &path);
  static int build_namespace_path(const common::ObString &catalog_name,
                                  const common::ObString &database_name,
                                  common::ObSqlString &path);
  static int build_ob_table_collection_path(const common::ObString &catalog_name,
                                            const common::ObString &database_name,
                                            common::ObSqlString &path);
  static int build_ob_table_collection_page_path(const common::ObString &catalog_name,
                                                 const common::ObString &database_name,
                                                 const common::ObString &page_token,
                                                 common::ObSqlString &path);
  static int build_ob_table_path(const common::ObString &catalog_name,
                                 const common::ObString &database_name,
                                 const common::ObString &table_name,
                                 common::ObSqlString &path);
  static int build_ob_table_rename_path(const common::ObString &catalog_name,
                                        const common::ObString &database_name,
                                        const common::ObString &table_name,
                                        common::ObSqlString &path);
  static int build_ob_reservation_path(const common::ObString &catalog_name,
                                       const common::ObString &database_name,
                                       const common::ObString &table_name,
                                       common::ObSqlString &path);
  static int build_ob_cancel_path(const common::ObString &catalog_name,
                                  const common::ObString &database_name,
                                  const common::ObString &table_name,
                                  const common::ObString &operation_id,
                                  common::ObSqlString &path);
  static int build_iceberg_table_path(const common::ObString &catalog_name,
                                      const common::ObString &database_name,
                                      const common::ObString &table_name,
                                      common::ObSqlString &path);
  static int build_ob_complete_path(const common::ObString &catalog_name,
                                    const common::ObString &database_name,
                                    const common::ObString &table_name,
                                    common::ObSqlString &path);
  static int build_part_sync_path(const common::ObString &catalog_name,
                                  const common::ObString &database_name,
                                  const common::ObString &table_name,
                                  common::ObSqlString &path);
  static int build_part_sync_batch_path(const common::ObString &catalog_name,
                                        const common::ObString &database_name,
                                        const common::ObString &table_name,
                                        const int64_t sync_id,
                                        common::ObSqlString &path);
  static int build_part_sync_commit_path(const common::ObString &catalog_name,
                                         const common::ObString &database_name,
                                         const common::ObString &table_name,
                                         const int64_t sync_id,
                                         common::ObSqlString &path);
  static int build_part_values_path(const common::ObString &catalog_name,
                                    const common::ObString &database_name,
                                    const common::ObString &table_name,
                                    const common::ObString &page_token,
                                    common::ObSqlString &path);

private:
  static bool is_url_unreserved_byte(const unsigned char ch);
  static int encode_url_component(const common::ObString &component, common::ObSqlString &encoded);

private:
  static constexpr char CATALOG_COLLECTION_PATH[] = "/api/catalog/oceanbase/v1/catalogs";
  static constexpr char CATALOG_PATH_TEMPLATE[] = "/api/catalog/oceanbase/v1/%.*s/catalog";
  static constexpr char NAMESPACE_COLLECTION_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces";
  static constexpr char NAMESPACE_COLLECTION_PAGE_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces?pageSize=%ld";
  static constexpr char NAMESPACE_COLLECTION_PAGE_WITH_TOKEN_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces?pageSize=%ld&pageToken=%.*s";
  static constexpr char NAMESPACE_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s";
  static constexpr char OB_TABLE_COLLECTION_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/tables";
  static constexpr char OB_TABLE_COLLECTION_PAGE_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/tables?pageSize=%ld";
  static constexpr char OB_TABLE_COLLECTION_PAGE_WITH_TOKEN_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/"
        "tables?pageSize=%ld&pageToken=%.*s";
  static constexpr char OB_TABLE_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/tables/%.*s";
  static constexpr char OB_TABLE_RENAME_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/tables/%.*s/rename";
  static constexpr char OB_RESERVATION_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/tables/%.*s/reservations";
  static constexpr char OB_COMPLETE_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/tables/%.*s/complete";
  static constexpr char OB_TABLE_CANCEL_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/tables/%.*s?operationId=%.*s";
  static constexpr char ICEBERG_TABLE_PATH_TEMPLATE[]
      = "/api/catalog/iceberg/v1/%.*s/namespaces/%.*s/tables/%.*s";
  static constexpr char PART_SYNC_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/tables/%.*s/partition-syncs";
  static constexpr char PART_SYNC_BATCH_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/"
        "tables/%.*s/partition-syncs/%ld/batches";
  static constexpr char PART_SYNC_COMMIT_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/"
        "tables/%.*s/partition-syncs/%ld/commit";
  static constexpr char PART_VALUES_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/"
        "tables/%.*s/partitions?pageSize=%ld";
  static constexpr char PART_VALUES_WITH_TOKEN_PATH_TEMPLATE[]
      = "/api/catalog/oceanbase/v1/%.*s/namespaces/%.*s/"
        "tables/%.*s/partitions?pageSize=%ld&pageToken=%.*s";
  static constexpr int64_t PART_VALUE_PAGE_SIZE = 1000;
  static constexpr int64_t METADATA_LIST_PAGE_SIZE = 1000;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_RESOURCE_PATHS_H_
