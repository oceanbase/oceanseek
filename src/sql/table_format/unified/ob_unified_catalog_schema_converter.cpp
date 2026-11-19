/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "sql/table_format/unified/ob_unified_catalog_schema_converter.h"

#include <cstring>

#include "common/object/ob_object.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "share/schema/ob_column_schema.h"
#include "share/catalog/ob_unified_catalog_partition_spec_codec.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/catalog/rest/responses/ob_rest_http_response.h"
#include "sql/table_format/hive/ob_hive_table_metadata.h"
#include "sql/table_format/iceberg/ob_iceberg_table_metadata.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObUnifiedCatalogSchemaConverter::build_database_schema(ObIAllocator &allocator,
                                                           const ObString &database_name,
                                                           const uint64_t tenant_id,
                                                           const uint64_t child_catalog_id,
                                                           const uint64_t database_id,
                                                           const ObNameCaseMode case_mode,
                                                           ObDatabaseSchema *&database_schema)
{
  int ret = OB_SUCCESS;
  database_schema = NULL;
  if (database_name.empty() || !is_valid_tenant_id(tenant_id) || 0 == child_catalog_id
      || child_catalog_id > INT64_MAX || !is_external_object_id(database_id)
      || OB_NAME_CASE_INVALID == case_mode) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid UC namespace schema identity", K(ret));
  } else if (OB_ISNULL(database_schema = OB_NEWx(ObDatabaseSchema, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate UC database schema", K(ret));
  } else {
    database_schema->set_tenant_id(tenant_id);
    database_schema->set_catalog_id(child_catalog_id);
    database_schema->set_database_id(database_id);
    database_schema->set_name_case_mode(case_mode);
    database_schema->set_charset_type(CHARSET_UTF8MB4);
    database_schema->set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    if (OB_FAIL(database_schema->set_database_name(database_name))) {
      LOG_WARN("failed to set UC database name", K(ret));
      OB_DELETEx(ObDatabaseSchema, &allocator, database_schema);
      database_schema = NULL;
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaConverter::set_native_collation(const ObString &native_type,
                                                          ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  const int64_t charset_token_len = sizeof(CHARSET_TOKEN) - 1;
  const int64_t collate_token_len = sizeof(COLLATE_TOKEN) - 1;
  int64_t charset_pos = -1;
  int64_t collate_pos = -1;
  for (int64_t i = native_type.length() - charset_token_len; i >= 0 && charset_pos < 0; --i) {
    if (0 == memcmp(native_type.ptr() + i, CHARSET_TOKEN, charset_token_len)) {
      charset_pos = i;
    }
  }
  if (charset_pos >= 0) {
    const int64_t charset_begin = charset_pos + charset_token_len;
    for (int64_t i = native_type.length() - collate_token_len;
         i >= charset_begin && collate_pos < 0;
         --i) {
      if (0 == memcmp(native_type.ptr() + i, COLLATE_TOKEN, collate_token_len)) {
        collate_pos = i;
      }
    }
    const int64_t charset_end = collate_pos >= 0 ? collate_pos : native_type.length();
    const ObString charset_name(static_cast<int32_t>(charset_end - charset_begin),
                                native_type.ptr() + charset_begin);
    const ObCharsetType charset = ObCharset::charset_type(charset_name);
    const ObCollationType collation
        = collate_pos >= 0 ? ObCharset::collation_type(
              ObString(static_cast<int32_t>(native_type.length() - collate_pos - collate_token_len),
                       native_type.ptr() + collate_pos + collate_token_len))
                           : ObCharset::get_default_collation(charset);
    if (!ObCharset::is_valid_charset(charset)
        || !ObCharset::is_valid_collation(charset, collation)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid Unified Catalog OB native type collation", K(ret), K(native_type));
    } else {
      column.set_charset_type(charset);
      column.set_collation_type(collation);
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaConverter::build_ob_column(
    ObIAllocator &allocator,
    const share::ObUnifiedCatalogObColumnMetadata &metadata,
    const uint64_t table_id,
    ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  ObString encoded_native_type;
  column.set_table_id(table_id);
  column.set_column_id(metadata.column_id_);
  if (OB_FAIL(column.set_column_name(metadata.name_))) {
    LOG_WARN("failed to set UC column name", K(ret));
  } else if (OB_FAIL(hive::ObHiveTableMetadata::setup_ob_native_column_type(metadata.native_type_,
                                                                            false,
                                                                            allocator,
                                                                            column))) {
    LOG_WARN("failed to decode UC OB native type", K(ret), K(metadata.name_));
  } else {
    // The SQL formatter uses the same spelling for both date type families.
    if ((ObMySQLDateType == metadata.ob_type_ && ObDateType == column.get_data_type())
        || (ObMySQLDateTimeType == metadata.ob_type_ && ObDateTimeType == column.get_data_type())) {
      column.set_data_type(metadata.ob_type_);
    }
    if (metadata.ob_type_ != column.get_data_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("UC OB type discriminator does not match native type",
               K(ret),
               K(metadata.name_),
               K(metadata.ob_type_),
               K(column.get_data_type()));
    } else if (OB_FAIL(set_native_collation(metadata.native_type_, column))) {
      LOG_WARN("failed to restore UC OB column collation", K(ret), K(metadata.name_));
    } else if (OB_FAIL(share::ObUnifiedCatalogSchemaCodec::serialize_native_type(column,
                                                                                 allocator,
                                                                                 encoded_native_type))) {
      LOG_WARN("failed to format restored UC OB native type", K(ret), K(metadata.name_));
    } else if (encoded_native_type != metadata.native_type_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("UC OB native type cannot be restored without loss", K(ret), K(metadata.name_));
    } else {
      column.set_nullable(metadata.nullable_);
      if (metadata.generated_) {
        ObObj generated_expression;
        generated_expression.set_varchar(metadata.generated_expression_);
        generated_expression.set_collation_type(ObCharset::get_system_collation());
        generated_expression.set_collation_level(CS_LEVEL_COERCIBLE);
        if (OB_FAIL(column.set_cur_default_value(generated_expression, false))) {
          LOG_WARN("failed to restore UC generated column expression", K(ret), K(metadata.name_));
        } else if (OB_FAIL(column.set_orig_default_value(generated_expression))) {
          LOG_WARN("failed to restore UC original generated column expression",
                   K(ret),
                   K(metadata.name_));
        } else if (metadata.stored_generated_) {
          column.add_column_flag(STORED_GENERATED_COLUMN_FLAG);
        } else {
          column.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
        }
      }
      if (OB_SUCC(ret) && metadata.rowkey_position_ > 0) {
        column.set_rowkey_position(metadata.rowkey_position_);
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaConverter::validate_rowkeys(
    const share::ObUnifiedCatalogObTableMetadata &metadata,
    int64_t &rowkey_count)
{
  int ret = OB_SUCCESS;
  rowkey_count = 0;
  for (int64_t i = 0; i < metadata.columns_.count(); ++i) {
    if (metadata.columns_.at(i).rowkey_position_ > 0) {
      ++rowkey_count;
    }
  }
  for (int64_t position = 1; OB_SUCC(ret) && position <= rowkey_count; ++position) {
    int64_t matches = 0;
    for (int64_t i = 0; i < metadata.columns_.count(); ++i) {
      if (metadata.columns_.at(i).rowkey_position_ == position) {
        ++matches;
      }
    }
    if (1 != matches) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid UC OB rowkey positions", K(ret), K(position), K(matches));
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaConverter::build_ob_table_schema(
    ObIAllocator &allocator,
    const share::ObUnifiedCatalogObTableMetadata &metadata,
    const uint64_t tenant_id,
    const uint64_t child_catalog_id,
    const uint64_t database_id,
    const uint64_t table_id,
    ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_count = 0;
  ObTableSchema *schema = NULL;
  table_schema = NULL;
  if (metadata.table_name_.empty() || 0 == metadata.columns_.count()
      || !is_valid_tenant_id(tenant_id) || 0 == child_catalog_id || child_catalog_id > INT64_MAX
      || !is_external_object_id(database_id) || !is_external_object_id(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid UC OB schema identity", K(ret));
  } else if (OB_FAIL(validate_rowkeys(metadata, rowkey_count))) {
    LOG_WARN("invalid UC OB primary key metadata", K(ret));
  } else if (OB_ISNULL(schema = OB_NEWx(ObTableSchema, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate UC OB table schema", K(ret));
  } else {
    schema->set_tenant_id(tenant_id);
    schema->set_catalog_id(child_catalog_id);
    schema->set_database_id(database_id);
    schema->set_table_id(table_id);
    schema->set_schema_version(metadata.schema_version_);
    schema->set_table_type(USER_TABLE);
    schema->set_charset_type(CHARSET_UTF8MB4);
    schema->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    schema->set_table_pk_exists_mode(rowkey_count > 0 ? TOM_TABLE_WITH_PK : TOM_TABLE_WITHOUT_PK);
    if (OB_FAIL(schema->set_table_name(metadata.table_name_))) {
      LOG_WARN("failed to set UC OB table name", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < metadata.columns_.count(); ++i) {
      ObColumnSchemaV2 column;
      if (OB_FAIL(build_ob_column(allocator, metadata.columns_.at(i), table_id, column))) {
        LOG_WARN("failed to convert UC OB column", K(ret), K(i));
      } else if (OB_FAIL(schema->add_column(column))) {
        LOG_WARN("failed to add UC OB column", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(share::ObUnifiedCatalogPartitionSpecCodec::restore(allocator,
                                                                     metadata.partition_spec_,
                                                                     *schema))) {
        LOG_WARN("failed to restore UC OB partition schema", K(ret), K(metadata.table_name_));
      }
    }
    if (OB_SUCC(ret)) {
      table_schema = schema;
    } else {
      OB_DELETEx(ObTableSchema, &allocator, schema);
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaConverter::build_iceberg_table_schema(
    ObIAllocator &allocator,
    const share::ObRestLoadTableResponse &metadata,
    const ObString &database_name,
    const ObString &table_name,
    const uint64_t tenant_id,
    const uint64_t child_catalog_id,
    const uint64_t database_id,
    const uint64_t table_id,
    const ObNameCaseMode case_mode,
    ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  iceberg::ObIcebergTableMetadata iceberg_metadata(allocator);
  if (OB_FAIL(iceberg_metadata.init(tenant_id,
                                    child_catalog_id,
                                    database_id,
                                    table_id,
                                    database_name,
                                    table_name,
                                    case_mode))) {
    LOG_WARN("failed to initialize UC Iceberg metadata", K(ret));
  } else if (OB_FAIL(iceberg_metadata.table_metadata_.assign(metadata.table_metadata_))) {
    LOG_WARN("failed to copy UC Iceberg table metadata", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator,
                                     metadata.metadata_location_,
                                     iceberg_metadata.table_metadata_.metadata_file_location,
                                     true))) {
    LOG_WARN("failed to copy UC Iceberg metadata location", K(ret));
  } else {
    iceberg_metadata.lake_table_metadata_version_
        = iceberg_metadata.table_metadata_.current_snapshot_id;
    if (OB_FAIL(iceberg_metadata.build_table_schema(std::nullopt, std::nullopt, table_schema))) {
      LOG_WARN("failed to build UC Iceberg table schema", K(ret), K(table_name));
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaConverter::build_iceberg_query_table_schema(
    const ObString &metadata_location,
    const ObString &database_name,
    const ObString &table_name,
    const uint64_t tenant_id,
    const uint64_t child_catalog_id,
    const uint64_t database_id,
    const uint64_t table_id,
    const ObNameCaseMode case_mode,
    ObTableSchema *&table_schema,
    iceberg::ObIcebergTableMetadata &iceberg_metadata)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  if (OB_FAIL(iceberg_metadata.init(tenant_id,
                                    child_catalog_id,
                                    database_id,
                                    table_id,
                                    database_name,
                                    table_name,
                                    case_mode))) {
    LOG_WARN("failed to initialize UC Iceberg query metadata", K(ret));
  } else if (OB_FAIL(iceberg_metadata.load_by_metadata_location(metadata_location))) {
    LOG_WARN("failed to read UC Iceberg metadata file", K(ret), K(table_name));
  } else if (OB_FAIL(iceberg_metadata.build_table_schema(
                 std::nullopt, std::nullopt, table_schema))) {
    LOG_WARN("failed to build UC Iceberg query table schema", K(ret), K(table_name));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
