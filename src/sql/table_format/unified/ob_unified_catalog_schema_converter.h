/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_SCHEMA_CONVERTER_H_
#define OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_SCHEMA_CONVERTER_H_

#include "share/catalog/ob_unified_catalog_schema_codec.h"
#include "lib/ob_define.h"


namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
class ObRestLoadTableResponse;
namespace schema
{
class ObColumnSchemaV2;
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

/** Converts decoded UC table metadata into a request-local OceanBase schema. */
class ObUnifiedCatalogSchemaConverter
{
public:
  static int build_database_schema(common::ObIAllocator &allocator,
                                   const common::ObString &database_name,
                                   const uint64_t tenant_id,
                                   const uint64_t child_catalog_id,
                                   const uint64_t database_id,
                                   const ObNameCaseMode case_mode,
                                   share::schema::ObDatabaseSchema *&database_schema);
  static int build_ob_table_schema(common::ObIAllocator &allocator,
                                   const share::ObUnifiedCatalogObTableMetadata &metadata,
                                   const uint64_t tenant_id,
                                   const uint64_t child_catalog_id,
                                   const uint64_t database_id,
                                   const uint64_t table_id,
                                   share::schema::ObTableSchema *&table_schema);
  static int build_iceberg_table_schema(common::ObIAllocator &allocator,
                                        const share::ObRestLoadTableResponse &metadata,
                                        const common::ObString &database_name,
                                        const common::ObString &table_name,
                                        const uint64_t tenant_id,
                                        const uint64_t child_catalog_id,
                                        const uint64_t database_id,
                                        const uint64_t table_id,
                                        const ObNameCaseMode case_mode,
                                        share::schema::ObTableSchema *&table_schema);
  static int build_iceberg_query_table_schema(
      const common::ObString &metadata_location,
      const common::ObString &database_name,
      const common::ObString &table_name,
      const uint64_t tenant_id,
      const uint64_t child_catalog_id,
      const uint64_t database_id,
      const uint64_t table_id,
      const ObNameCaseMode case_mode,
      share::schema::ObTableSchema *&table_schema,
      iceberg::ObIcebergTableMetadata &iceberg_metadata);

private:
  static constexpr char CHARSET_TOKEN[] = " CHARSET ";
  static constexpr char COLLATE_TOKEN[] = " COLLATE ";
  static int build_ob_column(common::ObIAllocator &allocator,
                             const share::ObUnifiedCatalogObColumnMetadata &metadata,
                             const uint64_t table_id,
                             share::schema::ObColumnSchemaV2 &column);
  static int set_native_collation(const common::ObString &native_type,
                                  share::schema::ObColumnSchemaV2 &column);
  static int validate_rowkeys(const share::ObUnifiedCatalogObTableMetadata &metadata, int64_t &rowkey_count);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_TABLE_FORMAT_UNIFIED_OB_UNIFIED_CATALOG_SCHEMA_CONVERTER_H_
