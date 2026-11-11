/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_METADATA_PROVIDER_H_
#define OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_METADATA_PROVIDER_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
template <typename T> class ObIArray;
}
namespace share
{
class ObUnifiedCatalogClient;
class ObUnifiedCatalogReader;
class ObUnifiedCatalogConfig;
enum class ObUnifiedCatalogQueryTableKind;
namespace schema
{
class ObDatabaseSchema;
class ObTableSchema;
}
}
namespace sql
{
namespace iceberg
{
class ObIcebergTableMetadata;
}

/** Loads UC metadata and delegates schema construction to the format converter. */
class ObUnifiedCatalogMetadataProvider
{
public:
  static int list_namespace_names(const uint64_t tenant_id,
                                  const common::ObString &access_token,
                                  const common::ObString &remote_catalog_name,
                                  common::ObIAllocator &allocator,
                                  common::ObIArray<common::ObString> &namespace_names);
  static int list_table_names(const uint64_t tenant_id,
                              const common::ObString &access_token,
                              const common::ObString &remote_catalog_name,
                              const common::ObString &database_name,
                              const ObNameCaseMode case_mode,
                              common::ObIAllocator &allocator,
                              common::ObIArray<common::ObString> &table_names);
  static int fetch_database_schema(const uint64_t tenant_id,
                                   const common::ObString &access_token,
                                   const uint64_t child_catalog_id,
                                   const common::ObString &remote_catalog_name,
                                   const common::ObString &database_name,
                                   const uint64_t database_id,
                                   const ObNameCaseMode case_mode,
                                   common::ObIAllocator &allocator,
                                   share::schema::ObDatabaseSchema *&database_schema);
  static int fetch_table_schema(const uint64_t tenant_id,
                                const common::ObString &access_token,
                                const uint64_t child_catalog_id,
                                const common::ObString &remote_catalog_name,
                                const common::ObString &database_name,
                                const common::ObString &table_name,
                                const uint64_t database_id,
                                const uint64_t table_id,
                                const ObNameCaseMode case_mode,
                                common::ObIAllocator &allocator,
                                share::schema::ObTableSchema *&table_schema);
  static int fetch_query_table_schema(const uint64_t tenant_id,
                                      const common::ObString &access_token,
                                      const uint64_t child_catalog_id,
                                      const common::ObString &remote_catalog_name,
                                      const common::ObString &database_name,
                                      const common::ObString &table_name,
                                      const uint64_t database_id,
                                      const uint64_t table_id,
                                      const ObNameCaseMode case_mode,
                                      common::ObIAllocator &allocator,
                                      share::schema::ObTableSchema *&table_schema,
                                      iceberg::ObIcebergTableMetadata *&iceberg_metadata);
  static int check_table_read_access(const uint64_t tenant_id,
                                     const common::ObString &access_token,
                                     const common::ObString &remote_catalog_name,
                                     const common::ObString &database_name,
                                     const common::ObString &table_name,
                                     common::ObIAllocator &allocator);
  // Classify a DATA-authorized query table by format and native source identity,
  // without decoding columns.
  static int classify_query_table(const uint64_t tenant_id,
                                  const common::ObString &access_token,
                                  const common::ObString &remote_catalog_name,
                                  const common::ObString &database_name,
                                  const common::ObString &table_name,
                                  common::ObIAllocator &allocator,
                                  share::ObUnifiedCatalogQueryTableKind &query_table_kind);

private:
  static int build_ob_table_schema(
      const uint64_t tenant_id,
      const uint64_t child_catalog_id,
      const common::ObString &table_name,
      const uint64_t database_id,
      const uint64_t table_id,
      const common::ObString &response_body,
      common::ObIAllocator &allocator,
      share::schema::ObTableSchema *&table_schema,
      bool &is_ob_table);
  static int fetch_iceberg_table_schema(
      const uint64_t tenant_id,
      const uint64_t child_catalog_id,
      const common::ObString &database_name,
      const common::ObString &table_name,
      const uint64_t database_id,
      const uint64_t table_id,
      const ObNameCaseMode case_mode,
      common::ObIAllocator &allocator,
      share::ObUnifiedCatalogReader &reader,
      share::schema::ObTableSchema *&table_schema);
  static int fetch_ob_query_table_schema(
      const uint64_t child_catalog_id,
      const common::ObString &database_name,
      const common::ObString &table_name,
      common::ObIAllocator &allocator,
      share::ObUnifiedCatalogReader &reader,
      bool &is_ob_table);
  static int fetch_iceberg_query_table_schema(
      const uint64_t tenant_id,
      const uint64_t child_catalog_id,
      const common::ObString &database_name,
      const common::ObString &table_name,
      const uint64_t database_id,
      const uint64_t table_id,
      const ObNameCaseMode case_mode,
      common::ObIAllocator &allocator,
      share::ObUnifiedCatalogReader &reader,
      share::schema::ObTableSchema *&table_schema,
      iceberg::ObIcebergTableMetadata *&iceberg_metadata);
  static int init_client(const uint64_t tenant_id,
                         const common::ObString &access_token,
                         const common::ObString &remote_catalog_name,
                         common::ObIAllocator &allocator,
                         share::ObUnifiedCatalogConfig &config,
                         share::ObUnifiedCatalogClient &client);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_METADATA_PROVIDER_H_
