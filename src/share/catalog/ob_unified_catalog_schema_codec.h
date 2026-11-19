/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_SCHEMA_CODEC_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_SCHEMA_CODEC_H_

#include "common/object/ob_obj_type.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObJsonArray;
class ObJsonNode;
class ObJsonObject;
}
namespace share
{
namespace schema
{
class ObColumnSchemaV2;
class ObTableSchema;
struct AlterColumnSchema;
}

struct ObUnifiedCatalogObColumnMetadata
{
  TO_STRING_KV(K_(name), K_(native_type), K_(column_id), K_(ob_type), K_(nullable),
               K_(rowkey_position), K_(generated), K_(stored_generated), K_(generated_expression));

  common::ObString name_;
  common::ObString native_type_;
  uint64_t column_id_ = 0;
  common::ObObjType ob_type_ = common::ObMaxType;
  bool nullable_ = true;
  int64_t rowkey_position_ = 0;
  bool generated_ = false;
  bool stored_generated_ = false;
  common::ObString generated_expression_;
};

struct ObUnifiedCatalogObTableMetadata
{
  common::ObString table_name_;
  uint64_t cluster_id_ = 0;
  uint64_t tenant_id_ = 0;
  uint64_t database_id_ = 0;
  uint64_t table_id_ = 0;
  int64_t schema_version_ = 0;
  common::ObSEArray<ObUnifiedCatalogObColumnMetadata, 8> columns_;
  // Owned by the allocator supplied to the table metadata decoder.
  const common::ObJsonNode *partition_spec_ = NULL;
};

enum class ObUnifiedCatalogQueryTableKind
{
  LOCAL_OB,
  NON_LOCAL_OB,
  ICEBERG
};

/**
 * Encodes OceanBase schemas for the OceanBase REST API and decodes its table responses.
 * The metadata field contains the complete versioned schema as a JSON string.
 * Unsupported local metadata fails before any HTTP request.
 */
class ObUnifiedCatalogSchemaCodec
{
public:
  // Shared by CREATE and table metadata update requests.
  static int encode_table_metadata(const uint64_t cluster_id,
                                   const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const schema::ObTableSchema &table_schema,
                                   common::ObIAllocator &allocator,
                                   common::ObString &metadata_json);

  static int encode_prepare_table_request(const common::ObString &operation_id,
                                          const uint64_t cluster_id,
                                          const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          common::ObIAllocator &allocator,
                                          common::ObString &request_body);
  static int encode_complete_table_request(const common::ObString &operation_id,
                                           const common::ObString &metadata,
                                           const int64_t sync_id,
                                           common::ObIAllocator &allocator,
                                           common::ObString &request_body);
  static int encode_update_table_request(const common::ObString &metadata,
                                         const uint64_t expected_source_table_id,
                                         common::ObIAllocator &allocator,
                                         common::ObString &request_body);
  static int encode_rename_table_request(const common::ObString &destination_database_name,
                                         const common::ObString &destination_table_name,
                                         const uint64_t expected_source_table_id,
                                         const common::ObString &metadata,
                                         common::ObIAllocator &allocator,
                                         common::ObString &request_body);
  static int decode_create_state(const common::ObString &response_body,
                                 const common::ObString &table_name,
                                 common::ObIAllocator &allocator,
                                 bool &preparing,
                                 common::ObString &operation_id);

  static int validate_load_table_response(const common::ObString &response_body,
                                          const uint64_t cluster_id,
                                          const uint64_t tenant_id,
                                          const uint64_t database_id,
                                          const schema::ObTableSchema &table_schema,
                                          common::ObIAllocator &allocator);

  static int validate_table_source(const common::ObString &response_body,
                                   const uint64_t cluster_id,
                                   const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &table_name,
                                   const uint64_t table_id,
                                   common::ObIAllocator &allocator,
                                   int64_t &schema_version);

  // String fields in metadata refer to the supplied allocator and response body.
  // Only a successful decode with is_ob_table=false permits an Iceberg API retry.
  static int try_decode_ob_table_metadata(const common::ObString &response_body,
                                          const common::ObString &table_name,
                                          common::ObIAllocator &allocator,
                                          ObUnifiedCatalogObTableMetadata &metadata,
                                          bool &is_ob_table);
  // Classify read routing by table format and native source identity.
  static int decode_query_table_kind(const common::ObString &response_body,
                                     const uint64_t cluster_id,
                                     const uint64_t tenant_id,
                                     const common::ObString &table_name,
                                     common::ObIAllocator &allocator,
                                     ObUnifiedCatalogQueryTableKind &query_table_kind);
  static int serialize_native_type(const schema::ObColumnSchemaV2 &column,
                                   common::ObIAllocator &allocator,
                                   common::ObString &native_type);

  // Run before any remote namespace or table write.
  static int validate_table_schema(const schema::ObTableSchema &table_schema);
  // CREATE resolver has template definitions before the RPC materializes child partitions.
  static int validate_create_table_schema(const schema::ObTableSchema &table_schema);
  // Run before local ADD COLUMN so unsupported metadata cannot diverge from Unified Catalog.
  static int validate_alter_add_column(const schema::AlterColumnSchema &column);

private:
  static int parse_table_response_header(const common::ObString &response_body,
                                         const common::ObString &table_name,
                                         common::ObIAllocator &allocator,
                                         const common::ObJsonObject *&table);
  static int decode_is_iceberg_table(const common::ObJsonObject &table,
                                     const common::ObString &table_name,
                                     bool &is_iceberg_table);
  static int parse_ob_table_payload(const common::ObJsonObject &table,
                                    const common::ObString &table_name,
                                    common::ObIAllocator &allocator,
                                    const common::ObJsonObject *&source,
                                    const common::ObJsonNode *&columns_node,
                                    const common::ObJsonNode *&partition_spec_node);
  static bool can_encode_primary_key(const schema::ObTableSchema &table_schema);
  static int validate_column_schema(const schema::ObColumnSchemaV2 &column);
  static int validate_table_schema_and_build_partition_spec(
      const schema::ObTableSchema &table_schema,
      common::ObIAllocator &allocator,
      common::ObJsonObject *&partition_spec,
      const bool allow_unmaterialized_template = false);

  static constexpr const char *TABLE_NAME = "name";
  static constexpr const char *METADATA_COLUMNS = "columns";
  static constexpr const char *PARTITION_SPEC = "partition_spec";
  static constexpr const char *TABLE_RESPONSE = "table";
  static constexpr const char *CREATE_PREPARING = "PREPARING";
  static constexpr const char *CREATE_ACTIVE = "ACTIVE";
  static constexpr const char *COMPLETE_OPERATION_ID = "operationId";
  static constexpr const char *COMPLETE_METADATA = "metadata";
  static constexpr const char *COMPLETE_SYNC_ID = "syncId";
  static constexpr const char *EXPECTED_SOURCE_TABLE_ID = "expectedSourceTableId";
  static constexpr const char *DESTINATION_NAMESPACE = "destinationNamespace";
  static constexpr const char *DESTINATION_TABLE = "destinationTable";
  static constexpr const char *TABLE_STATE = "state";
  static constexpr const char *TABLE_TYPE = "tableType";
  static constexpr const char *TABLE_TYPE_OCEANBASE = "OCEANBASE";
  static constexpr const char *TABLE_TYPE_ICEBERG = "ICEBERG";
  static constexpr const char *TABLE_SOURCE = "source";
  static constexpr const char *TABLE_SOURCE_CLUSTER_ID = "clusterId";
  static constexpr const char *TABLE_SOURCE_TENANT_ID = "tenantId";
  static constexpr const char *TABLE_SOURCE_DATABASE_ID = "databaseId";
  static constexpr const char *METADATA_SOURCE = "source";
  static constexpr const char *SOURCE_SCHEMA_VERSION = "schema-version";
  static constexpr const char *SOURCE_CLUSTER_ID = "cluster-id";
  static constexpr const char *SOURCE_TENANT_ID = "tenant-id";
  static constexpr const char *SOURCE_DATABASE_ID = "database-id";
  static constexpr const char *SOURCE_TABLE_ID = "table-id";
  static constexpr const char *COLUMN_ID = "column-id";
  static constexpr const char *COLUMN_NAME = "name";
  static constexpr const char *COLUMN_OB_TYPE = "ob-type";
  static constexpr const char *COLUMN_NATIVE_TYPE = "native-type";
  static constexpr const char *COLUMN_ROWKEY_POSITION = "rowkey-position";
  static constexpr const char *COLUMN_NULLABLE = "nullable";
  static constexpr const char *COLUMN_GENERATED_EXPRESSION = "generated-expression";
  static constexpr const char *COLUMN_GENERATED_STORAGE = "generated-storage";
  static constexpr const char *GENERATED_STORAGE_VIRTUAL = "VIRTUAL";
  static constexpr const char *GENERATED_STORAGE_STORED = "STORED";

  static int alloc_object(common::ObIAllocator &allocator,
                          common::ObJsonObject *&object);
  static int alloc_array(common::ObIAllocator &allocator,
                         common::ObJsonArray *&array);
  static int add_string(common::ObIAllocator &allocator,
                        common::ObJsonObject &object,
                        const char *key,
                        const common::ObString &value);
  static int add_owned_string(common::ObIAllocator &allocator,
                              common::ObJsonObject &object,
                              const char *key,
                              const common::ObString &value);
  static int add_uint_string(common::ObIAllocator &allocator,
                             common::ObJsonObject &object,
                             const char *key,
                             const uint64_t value);
  static int add_integer(common::ObIAllocator &allocator,
                         common::ObJsonObject &object,
                         const char *key,
                         const int64_t value);
  static int add_boolean(common::ObIAllocator &allocator,
                         common::ObJsonObject &object,
                         const char *key,
                         const bool value);
  static int validate_column_type(const schema::ObColumnSchemaV2 &column);
  static int decode_ob_column(const common::ObJsonObject &column_object,
                              ObUnifiedCatalogObColumnMetadata &column);
  static int parse_ob_table_metadata_response(
      const common::ObString &response_body,
      const common::ObString &table_name,
      common::ObIAllocator &allocator,
      ObUnifiedCatalogObTableMetadata &metadata,
      const common::ObJsonObject *&source,
      const common::ObJsonNode *&columns_node,
      const common::ObJsonNode *&partition_spec_node,
      bool &is_ob_table);
  static int decode_ob_table_metadata_impl(const common::ObString &response_body,
                                           const common::ObString &table_name,
                                           common::ObIAllocator &allocator,
                                           ObUnifiedCatalogObTableMetadata &metadata,
                                           bool &is_ob_table);
  static int decode_ob_table_columns(const common::ObJsonNode &columns_node,
                                     const common::ObJsonNode *partition_spec_node,
                                     ObUnifiedCatalogObTableMetadata &metadata);
  static int get_uint_string(const common::ObJsonObject &object,
                             const char *key,
                             uint64_t &value);
  static int add_column(common::ObIAllocator &allocator,
                        const schema::ObColumnSchemaV2 &column,
                        const bool encode_primary_key,
                        common::ObJsonArray &columns);
  static int get_string(const common::ObJsonObject &object,
                        const char *key,
                        common::ObString &value);
  static int expect_string(const common::ObJsonObject &object,
                           const char *key,
                           const common::ObString &expected);
  static int expect_uint_string(const common::ObJsonObject &object,
                                const char *key,
                                const uint64_t expected);
  static int expect_integer(const common::ObJsonObject &object,
                            const char *key,
                            const int64_t expected);
  static int validate_column(const common::ObJsonObject &column_object,
                             const schema::ObColumnSchemaV2 &column,
                             const bool validate_primary_key,
                             common::ObIAllocator &allocator);
  static int parse_source_schema_version(const common::ObJsonObject &source,
                                         int64_t &schema_version);
  static int parse_load_table_response(const common::ObString &response_body,
                                       const common::ObString &table_name,
                                       common::ObIAllocator &allocator,
                                       const common::ObJsonObject *&source,
                                       const common::ObJsonNode *&columns_node,
                                       const common::ObJsonNode *&partition_spec_node);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_SCHEMA_CODEC_H_
