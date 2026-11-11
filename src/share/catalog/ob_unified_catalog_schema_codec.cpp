/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_schema_codec.h"

#include "common/object/ob_obj_type.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_sql_string.h"
#include <stdint.h>
#include "share/ob_get_compat_mode.h"
#include "share/catalog/ob_unified_catalog_partition_spec_codec.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace share
{
bool ObUnifiedCatalogSchemaCodec::can_encode_primary_key(const ObTableSchema &table_schema)
{
  return table_schema.is_table_with_pk() && table_schema.is_index_organized_table();
}

int ObUnifiedCatalogSchemaCodec::alloc_object(ObIAllocator &allocator, ObJsonObject *&object)
{
  int ret = OB_SUCCESS;
  object = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(object)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate json object", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::alloc_array(ObIAllocator &allocator, ObJsonArray *&array)
{
  int ret = OB_SUCCESS;
  array = OB_NEWx(ObJsonArray, &allocator, &allocator);
  if (OB_ISNULL(array)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate json array", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::add_string(ObIAllocator &allocator,
                                            ObJsonObject &object,
                                            const char *key,
                                            const ObString &value)
{
  int ret = OB_SUCCESS;
  ObJsonString *node = OB_NEWx(ObJsonString, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate json string", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add json string", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::add_owned_string(ObIAllocator &allocator,
                                                  ObJsonObject &object,
                                                  const char *key,
                                                  const ObString &value)
{
  int ret = OB_SUCCESS;
  ObString copied_value;
  if (OB_FAIL(ob_write_string(allocator, value, copied_value))) {
    LOG_WARN("failed to copy json string", K(ret), K(key));
  } else if (OB_FAIL(add_string(allocator, object, key, copied_value))) {
    LOG_WARN("failed to add copied json string", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::add_uint_string(ObIAllocator &allocator,
                                                 ObJsonObject &object,
                                                 const char *key,
                                                 const uint64_t value)
{
  int ret = OB_SUCCESS;
  ObSqlString value_string;
  if (OB_FAIL(value_string.append_fmt("%lu", value))) {
    LOG_WARN("failed to format uint json string", K(ret), K(key), K(value));
  } else if (OB_FAIL(add_owned_string(allocator, object, key, value_string.string()))) {
    LOG_WARN("failed to add uint json string", K(ret), K(key), K(value));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::add_integer(ObIAllocator &allocator,
                                             ObJsonObject &object,
                                             const char *key,
                                             const int64_t value)
{
  int ret = OB_SUCCESS;
  ObJsonInt *node = OB_NEWx(ObJsonInt, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate json integer", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add json integer", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::add_boolean(ObIAllocator &allocator,
                                             ObJsonObject &object,
                                             const char *key,
                                             const bool value)
{
  int ret = OB_SUCCESS;
  ObJsonBoolean *node = OB_NEWx(ObJsonBoolean, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate json boolean", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add json boolean", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::validate_column_type(const ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  const ObObjType data_type = column.get_data_type();
  switch (data_type) {
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType:
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type:
    case ObFloatType:
    case ObDoubleType:
    case ObUFloatType:
    case ObUDoubleType:
    case ObNumberType:
    case ObUNumberType:
    case ObDateTimeType:
    case ObTimestampType:
    case ObDateType:
    case ObTimeType:
    case ObYearType:
    case ObVarcharType:
    case ObCharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObBitType:
    case ObEnumType:
    case ObSetType:
    case ObJsonType:
    case ObGeometryType:
    case ObDecimalIntType:
    case ObCollectionSQLType:
    case ObMySQLDateType:
    case ObMySQLDateTimeType:
    case ObRoaringBitmapType:
      break;
    case ObNullType:
    case ObHexStringType:
    case ObExtendType:
    case ObUnknownType:
    case ObEnumInnerType:
    case ObSetInnerType:
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType:
    case ObRawType:
    case ObIntervalYMType:
    case ObIntervalDSType:
    case ObNumberFloatType:
    case ObNVarchar2Type:
    case ObNCharType:
    case ObURowIDType:
    case ObLobType:
    case ObUserDefinedSQLType:
    case ObMaxType:
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported Unified Catalog column type",
               K(ret), K(column.get_column_name_str()), K(data_type));
      break;
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::serialize_native_type(const ObColumnSchemaV2 &column,
                                                       ObIAllocator &allocator,
                                                       ObString &native_type)
{
  int ret = OB_SUCCESS;
  char stack_buf[OB_MAX_SYS_PARAM_NAME_LENGTH] = {0};
  char *buf = stack_buf;
  int64_t buf_len = sizeof(stack_buf);
  int64_t pos = 0;
  const uint64_t sub_type = column.is_xmltype()
      ? column.get_sub_data_type()
      : static_cast<uint64_t>(column.get_geo_type());
  native_type.reset();
  if (OB_FAIL(ob_sql_type_str_with_coll(buf,
                                        buf_len,
                                        pos,
                                        column.get_data_type(),
                                        column.get_data_length(),
                                        column.get_data_precision(),
                                        column.get_data_scale(),
                                        column.get_collation_type(),
                                        column.get_extended_type_info(),
                                        sub_type,
                                        column.is_string_lob()))) {
    if (OB_SIZE_OVERFLOW == ret) {
      buf_len = OB_MAX_EXTENDED_TYPE_INFO_LENGTH;
      pos = 0;
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate native type buffer", K(ret), K(buf_len));
      } else if (OB_FAIL(ob_sql_type_str_with_coll(buf,
                                                   buf_len,
                                                   pos,
                                                   column.get_data_type(),
                                                   column.get_data_length(),
                                                   column.get_data_precision(),
                                                   column.get_data_scale(),
                                                   column.get_collation_type(),
                                                   column.get_extended_type_info(),
                                                   sub_type,
                                                   column.is_string_lob()))) {
        LOG_WARN("failed to serialize native column type", K(ret));
      }
    } else {
      LOG_WARN("failed to serialize native column type",
               K(ret), K(column.get_column_name_str()), K(column.get_data_type()));
    }
  }
  if (OB_SUCC(ret)) {
    if (stack_buf == buf) {
      const ObString serialized_type(static_cast<int32_t>(pos), buf);
      if (OB_FAIL(ob_write_string(allocator, serialized_type, native_type))) {
        LOG_WARN("failed to copy native column type", K(ret));
      }
    } else {
      native_type.assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::add_column(ObIAllocator &allocator,
                                            const ObColumnSchemaV2 &column,
                                            const bool encode_primary_key,
                                            ObJsonArray &columns)
{
  int ret = OB_SUCCESS;
  ObJsonObject *column_object = NULL;
  ObString native_type;
  if (OB_FAIL(validate_column_type(column))) {
    LOG_WARN("failed to validate Unified Catalog column type", K(ret), K(column.get_column_name_str()));
  } else if (OB_FAIL(serialize_native_type(column, allocator, native_type))) {
    LOG_WARN("failed to serialize Unified Catalog column type", K(ret), K(column.get_column_name_str()));
  } else if (OB_FAIL(alloc_object(allocator, column_object))) {
    LOG_WARN("failed to allocate Unified Catalog column object", K(ret));
  } else if (OB_FAIL(add_uint_string(
                 allocator, *column_object, COLUMN_ID, column.get_column_id()))) {
    LOG_WARN("failed to add column id", K(ret));
  } else if (OB_FAIL(add_string(
                 allocator, *column_object, COLUMN_NAME, column.get_column_name_str()))) {
    LOG_WARN("failed to add column name", K(ret));
  } else if (OB_FAIL(add_integer(allocator,
                                 *column_object,
                                 COLUMN_OB_TYPE,
                                 static_cast<int64_t>(column.get_data_type())))) {
    LOG_WARN("failed to add OceanBase column type", K(ret));
  } else if (OB_FAIL(add_string(allocator, *column_object, COLUMN_NATIVE_TYPE, native_type))) {
    LOG_WARN("failed to add native column type", K(ret));
  } else if (OB_FAIL(
                 add_boolean(allocator, *column_object, COLUMN_NULLABLE, column.is_nullable()))) {
    LOG_WARN("failed to add nullable", K(ret));
  } else if (encode_primary_key && column.is_original_rowkey_column()
             && OB_FAIL(add_integer(allocator,
                                    *column_object,
                                    COLUMN_ROWKEY_POSITION,
                                    column.get_rowkey_position()))) {
    LOG_WARN("failed to add primary key position", K(ret));
  } else if (OB_FAIL(columns.append(column_object))) {
    LOG_WARN("failed to append Unified Catalog column", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::get_string(const ObJsonObject &object,
                                            const char *key,
                                            ObString &value)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = object.get_value(key);
  if (OB_ISNULL(node) || ObJsonNodeType::J_STRING != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("required json string is missing or invalid", K(ret), K(key));
  } else {
    value = static_cast<const ObJsonString *>(node)->value();
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::expect_string(const ObJsonObject &object,
                                               const char *key,
                                               const ObString &expected)
{
  int ret = OB_SUCCESS;
  ObString actual;
  if (OB_FAIL(get_string(object, key, actual))) {
    LOG_WARN("failed to read required json string", K(ret), K(key));
  } else if (actual != expected) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog metadata string mismatch", K(ret), K(key), K(expected), K(actual));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::expect_uint_string(const ObJsonObject &object,
                                                    const char *key,
                                                    const uint64_t expected)
{
  int ret = OB_SUCCESS;
  ObSqlString expected_string;
  if (OB_FAIL(expected_string.append_fmt("%lu", expected))) {
    LOG_WARN("failed to format expected uint", K(ret), K(key));
  } else if (OB_FAIL(expect_string(object, key, expected_string.string()))) {
    LOG_WARN("Unified Catalog source identity mismatch", K(ret), K(key), K(expected));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::expect_integer(const ObJsonObject &object,
                                                const char *key,
                                                const int64_t expected)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = object.get_value(key);
  if (OB_ISNULL(node) || ObJsonNodeType::J_INT != node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog metadata integer is missing or invalid", K(ret), K(key));
  } else if (static_cast<const ObJsonInt *>(node)->value() != expected) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog metadata integer mismatch", K(ret), K(key), K(expected));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::validate_column(const ObJsonObject &column_object,
                                                 const ObColumnSchemaV2 &column,
                                                 const bool validate_primary_key,
                                                 ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString native_type;
  const ObJsonNode *nullable_node = column_object.get_value(COLUMN_NULLABLE);
  const ObJsonNode *rowkey_position_node = column_object.get_value(COLUMN_ROWKEY_POSITION);
  const bool expect_primary_key_position =
      validate_primary_key && column.is_original_rowkey_column();
  if (OB_FAIL(validate_column_type(column))) {
    LOG_WARN("failed to validate expected Unified Catalog column type", K(ret));
  } else if (OB_FAIL(serialize_native_type(column, allocator, native_type))) {
    LOG_WARN("failed to serialize expected Unified Catalog column type", K(ret));
  } else if (OB_FAIL(expect_uint_string(column_object, COLUMN_ID, column.get_column_id()))) {
    LOG_WARN("column id mismatch", K(ret), K(column.get_column_name_str()));
  } else if (OB_FAIL(expect_string(column_object, COLUMN_NAME, column.get_column_name_str()))) {
    LOG_WARN("column name mismatch", K(ret), K(column.get_column_name_str()));
  } else if (OB_FAIL(expect_integer(column_object,
                                    COLUMN_OB_TYPE,
                                    static_cast<int64_t>(column.get_data_type())))) {
    LOG_WARN("OceanBase column type mismatch", K(ret), K(column.get_column_name_str()));
  } else if (OB_FAIL(expect_string(column_object, COLUMN_NATIVE_TYPE, native_type))) {
    LOG_WARN("native column type mismatch", K(ret), K(column.get_column_name_str()));
  } else if (OB_ISNULL(nullable_node)
             || ObJsonNodeType::J_BOOLEAN != nullable_node->json_type()
             || static_cast<const ObJsonBoolean *>(nullable_node)->value()
                    != column.is_nullable()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("column nullable mismatch", K(ret), K(column.get_column_name_str()));
  } else if (expect_primary_key_position
             && OB_FAIL(expect_integer(column_object,
                                       COLUMN_ROWKEY_POSITION,
                                       column.get_rowkey_position()))) {
    LOG_WARN("primary key position mismatch", K(ret), K(column.get_column_name_str()));
  } else if (!expect_primary_key_position && OB_NOT_NULL(rowkey_position_node)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unexpected primary key position", K(ret), K(column.get_column_name_str()));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::validate_table_schema(const ObTableSchema &table_schema)
{
  ObArenaAllocator allocator("UCPart");
  ObJsonObject *partition_spec = NULL;
  return validate_table_schema_and_build_partition_spec(table_schema, allocator, partition_spec);
}

int ObUnifiedCatalogSchemaCodec::validate_create_table_schema(const ObTableSchema &table_schema)
{
  ObArenaAllocator allocator("UCPart", OB_MALLOC_NORMAL_BLOCK_SIZE,
                             table_schema.get_tenant_id());
  ObJsonObject *partition_spec = NULL;
  return validate_table_schema_and_build_partition_spec(table_schema, allocator,
                                                        partition_spec, true);
}

int ObUnifiedCatalogSchemaCodec::validate_table_schema_and_build_partition_spec(
    const ObTableSchema &table_schema,
    ObIAllocator &allocator,
    ObJsonObject *&partition_spec,
    const bool allow_unmaterialized_template)
{
  int ret = OB_SUCCESS;
  partition_spec = NULL;
  bool is_oracle_mode = false;
  int64_t visible_column_count = 0;
  int64_t visible_primary_key_count = 0;
  const bool is_partitioned = table_schema.is_partitioned_table();
  const bool has_primary_key_metadata = can_encode_primary_key(table_schema);
  ObColumnIterByPrevNextID column_iter(table_schema);
  const ObColumnSchemaV2 *column = NULL;
  // CREATE has no table ID before the local RPC; use its tenant compatibility mode.
  if (allow_unmaterialized_template) {
    if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
            table_schema.get_tenant_id(), is_oracle_mode))) {
      LOG_WARN("failed to get table compatibility mode", K(ret));
    }
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("failed to get table compatibility mode", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (is_oracle_mode) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Oracle mode table is not supported by Unified Catalog synchronization", K(ret));
    } else if (!table_schema.is_user_table() || table_schema.is_index_table()
        || table_schema.is_aux_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("non-user table is not supported by Unified Catalog synchronization", K(ret));
    } else if (table_schema.get_schema_version() < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table schema version", K(ret), K(table_schema.get_schema_version()));
    } else if (is_partitioned && table_schema.is_table_with_pk()
               && !table_schema.is_index_organized_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("heap-organized primary key is not supported by Unified Catalog synchronization", K(ret));
    } else if (table_schema.get_constraint_count() > 0
               || table_schema.get_foreign_key_infos().count() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported Unified Catalog table constraint", K(ret),
               K(table_schema.get_constraint_count()),
               K(table_schema.get_foreign_key_infos().count()));
    }
  }
  while (OB_SUCC(ret) && OB_SUCC(column_iter.next(column))) {
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret));
    } else if (!column->is_hidden() && !column->is_shadow_column()) {
      ++visible_column_count;
      const bool supported_primary_key_column =
          has_primary_key_metadata && column->is_original_rowkey_column();
      const bool unsupported_primary_key_column =
          (is_partitioned && column->is_heap_table_primary_key_column())
          || (column->is_rowkey_column() && !supported_primary_key_column);
      if (supported_primary_key_column) {
        ++visible_primary_key_count;
      }
      if (column->is_invisible_column() || column->is_generated_column()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported Unified Catalog column",
                 K(ret), K(column->get_column_name_str()),
                 "invisible", column->is_invisible_column(),
                 "generated", column->is_generated_column());
      } else if (unsupported_primary_key_column) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported Unified Catalog primary key column",
                 K(ret), K(column->get_column_name_str()));
      } else if (column->is_autoincrement()
                 || column->is_identity_column()
                 || column->is_on_update_current_timestamp()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported Unified Catalog column attribute", K(ret),
                 K(column->get_column_name_str()),
                 "auto_increment", column->is_autoincrement(),
                 "identity", column->is_identity_column(),
                 "on_update", column->is_on_update_current_timestamp());
      } else if (!column->get_cur_default_value().is_null()
                 && !column->get_cur_default_value().is_nop_value()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported Unified Catalog column default",
                 K(ret), K(column->get_column_name_str()));
      } else if (OB_FAIL(validate_column_type(*column))) {
        LOG_WARN("failed to validate Unified Catalog column",
                 K(ret), K(column->get_column_name_str()));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && visible_column_count <= 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table without visible columns is not supported by Unified Catalog synchronization",
             K(ret));
  }
  if (OB_SUCC(ret) && has_primary_key_metadata) {
    if (visible_primary_key_count <= 0
        || visible_primary_key_count != table_schema.get_rowkey_info().get_size()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("primary key columns are missing from Unified Catalog metadata",
               K(ret), K(visible_primary_key_count), K(table_schema.get_rowkey_info().get_size()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < visible_primary_key_count; ++i) {
      const ObRowkeyColumn *rowkey = table_schema.get_rowkey_info().get_column(i);
      const ObColumnSchemaV2 *column = OB_ISNULL(rowkey)
          ? NULL : table_schema.get_column_schema(rowkey->column_id_);
      if (OB_ISNULL(column) || column->is_hidden() || column->is_shadow_column()
          || !column->is_original_rowkey_column() || column->get_rowkey_position() != i + 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported Unified Catalog primary key layout", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret) && is_partitioned) {
    if (OB_FAIL(ObUnifiedCatalogPartitionSpecCodec::build(allocator,
                                                          table_schema,
                                                          partition_spec,
                                                          allow_unmaterialized_template))) {
      LOG_WARN("partition spec is not supported by Unified Catalog synchronization", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::encode_table_metadata(const uint64_t cluster_id,
                                                       const uint64_t tenant_id,
                                                       const uint64_t database_id,
                                                       const ObTableSchema &table_schema,
                                                       ObIAllocator &allocator,
                                                       ObString &metadata_json)
{
  int ret = OB_SUCCESS;
  ObJsonObject *metadata = NULL;
  ObJsonObject *source = NULL;
  ObJsonArray *columns = NULL;
  ObJsonObject *partition_spec = NULL;
  const bool has_primary_key_metadata = can_encode_primary_key(table_schema);
  metadata_json.reset();
  if (OB_FAIL(validate_table_schema_and_build_partition_spec(table_schema,
                                                             allocator,
                                                             partition_spec))) {
    LOG_WARN("table schema is not supported by Unified Catalog synchronization", K(ret));
  } else if (OB_FAIL(alloc_object(allocator, metadata)) || OB_FAIL(alloc_object(allocator, source))
             || OB_FAIL(alloc_array(allocator, columns))) {
    LOG_WARN("failed to allocate Unified Catalog metadata json", K(ret));
  } else if (OB_FAIL(add_uint_string(allocator, *source, SOURCE_CLUSTER_ID, cluster_id))
             || OB_FAIL(add_uint_string(allocator, *source, SOURCE_TENANT_ID, tenant_id))
             || OB_FAIL(add_uint_string(allocator, *source, SOURCE_DATABASE_ID, database_id))
             || OB_FAIL(
                 add_uint_string(allocator, *source, SOURCE_TABLE_ID, table_schema.get_table_id()))
             || OB_FAIL(
                 add_uint_string(allocator,
                                 *source,
                                 SOURCE_SCHEMA_VERSION,
                                 static_cast<uint64_t>(table_schema.get_schema_version())))) {
    LOG_WARN("failed to build Unified Catalog table source", K(ret));
  } else {
    ObColumnIterByPrevNextID column_iter(table_schema);
    const ObColumnSchemaV2 *column = NULL;
    while (OB_SUCC(ret) && OB_SUCC(column_iter.next(column))) {
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret));
      } else if (!column->is_hidden() && !column->is_shadow_column()
                 && OB_FAIL(add_column(allocator,
                                       *column,
                                       has_primary_key_metadata,
                                       *columns))) {
        LOG_WARN("failed to append Unified Catalog table column", K(ret));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(metadata->object_add(
            ObString::make_string(METADATA_SOURCE), source))
        || OB_FAIL(metadata->object_add(ObString::make_string(METADATA_COLUMNS), columns))) {
      LOG_WARN("failed to assemble Unified Catalog metadata property", K(ret));
    } else if (table_schema.is_partitioned_table()) {
      if (OB_FAIL(metadata->object_add(
              ObString::make_string(PARTITION_SPEC), partition_spec))) {
        LOG_WARN("failed to add Unified Catalog partition spec", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObJsonBuffer buffer(&allocator);
      if (OB_FAIL(metadata->print(buffer, false))) {
        LOG_WARN("failed to serialize Unified Catalog metadata property", K(ret));
      } else {
        metadata_json = buffer.string();
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::encode_prepare_table_request(const ObString &operation_id,
                                                              const uint64_t cluster_id,
                                                              const uint64_t tenant_id,
                                                              const uint64_t database_id,
                                                              ObIAllocator &allocator,
                                                              ObString &request_body)
{
  int ret = OB_SUCCESS;
  ObJsonObject *root = NULL;
  ObJsonObject *source = NULL;
  request_body.reset();
  if (operation_id.empty() || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid OceanBase create reservation", K(ret));
  } else if (OB_FAIL(alloc_object(allocator, root)) || OB_FAIL(alloc_object(allocator, source))) {
    LOG_WARN("failed to allocate OceanBase create reservation", K(ret));
  } else if (OB_FAIL(add_string(allocator, *root, COMPLETE_OPERATION_ID, operation_id))
             || OB_FAIL(add_uint_string(allocator, *source, TABLE_SOURCE_CLUSTER_ID, cluster_id))
             || OB_FAIL(add_uint_string(allocator, *source, TABLE_SOURCE_TENANT_ID, tenant_id))
             || OB_FAIL(add_uint_string(allocator, *source, TABLE_SOURCE_DATABASE_ID, database_id))
             || OB_FAIL(root->object_add(ObString::make_string(TABLE_SOURCE), source))) {
    LOG_WARN("failed to build OceanBase create reservation", K(ret));
  } else {
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(root->print(buffer, false))) {
      LOG_WARN("failed to serialize OceanBase create reservation", K(ret));
    } else {
      request_body = buffer.string();
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::encode_complete_table_request(const ObString &operation_id,
                                                               const ObString &metadata,
                                                               const int64_t sync_id,
                                                               ObIAllocator &allocator,
                                                               ObString &request_body)
{
  int ret = OB_SUCCESS;
  ObJsonObject *root = NULL;
  request_body.reset();
  if (operation_id.empty() || metadata.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog create completion", K(ret));
  } else if (OB_FAIL(alloc_object(allocator, root))) {
    LOG_WARN("failed to allocate Unified Catalog create completion", K(ret));
  } else if (OB_FAIL(add_string(allocator, *root, COMPLETE_OPERATION_ID, operation_id))
             || OB_FAIL(add_string(allocator, *root, COMPLETE_METADATA, metadata))
             || (sync_id > 0 && OB_FAIL(add_integer(allocator, *root, COMPLETE_SYNC_ID, sync_id)))) {
    LOG_WARN("failed to build Unified Catalog create completion", K(ret));
  } else {
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(root->print(buffer, false))) {
      LOG_WARN("failed to serialize Unified Catalog create completion", K(ret));
    } else {
      request_body = buffer.string();
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::encode_update_table_request(const ObString &metadata,
                                                             ObIAllocator &allocator,
                                                             ObString &request_body)
{
  int ret = OB_SUCCESS;
  ObJsonObject *root = NULL;
  request_body.reset();
  if (metadata.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty OceanBase table metadata", K(ret));
  } else if (OB_FAIL(alloc_object(allocator, root))) {
    LOG_WARN("failed to allocate OceanBase table update", K(ret));
  } else if (OB_FAIL(add_string(allocator, *root, COMPLETE_METADATA, metadata))) {
    LOG_WARN("failed to build OceanBase table update", K(ret));
  } else {
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(root->print(buffer, false))) {
      LOG_WARN("failed to serialize OceanBase table update", K(ret));
    } else {
      request_body = buffer.string();
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::decode_create_state(const ObString &response_body,
                                                     const ObString &table_name,
                                                     ObIAllocator &allocator,
                                                     bool &preparing,
                                                     ObString &operation_id)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root_node = NULL;
  preparing = false;
  operation_id.reset();
  if (OB_FAIL(ObJsonParser::get_tree(&allocator, response_body, root_node))) {
    LOG_WARN("failed to parse OceanBase create state", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog create state response", K(ret));
  } else {
    const ObJsonNode *table_node = static_cast<const ObJsonObject *>(root_node)->get_value(TABLE_RESPONSE);
    if (OB_ISNULL(table_node) || ObJsonNodeType::J_OBJECT != table_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog create state table is missing", K(ret));
    } else {
      const ObJsonObject *table = static_cast<const ObJsonObject *>(table_node);
      ObString state;
      if (OB_FAIL(expect_string(*table, TABLE_NAME, table_name))
          || OB_FAIL(get_string(*table, TABLE_STATE, state))) {
        LOG_WARN("invalid OceanBase table state", K(ret), K(table_name));
      } else if (state == ObString::make_string(CREATE_PREPARING)) {
        preparing = true;
        if (OB_FAIL(get_string(*table, COMPLETE_OPERATION_ID, operation_id))) {
          LOG_WARN("failed to read OceanBase reservation operation ID", K(ret));
        } else if (operation_id.empty()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("OceanBase reservation operation ID is missing", K(ret));
        }
      } else if (state != ObString::make_string(CREATE_ACTIVE)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("unknown OceanBase table state", K(ret), K(state));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::parse_table_response_header(const ObString &response_body,
                                                             const ObString &table_name,
                                                             ObIAllocator &allocator,
                                                             const ObJsonObject *&table)
{
  int ret = OB_SUCCESS;
  ObJsonNode *root_node = NULL;
  table = NULL;
  if (response_body.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog load table response", K(ret));
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator, response_body, root_node))) {
    LOG_WARN("failed to parse OceanBase table response", K(ret));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog load table response", K(ret));
  } else {
    const ObJsonNode *table_node = static_cast<const ObJsonObject *>(root_node)->get_value(TABLE_RESPONSE);
    if (OB_ISNULL(table_node) || ObJsonNodeType::J_OBJECT != table_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog response table object is missing", K(ret));
    } else {
      const ObJsonObject *candidate = static_cast<const ObJsonObject *>(table_node);
      ObString state;
      if (OB_FAIL(expect_string(*candidate, TABLE_NAME, table_name))
          || OB_FAIL(get_string(*candidate, TABLE_STATE, state))) {
        LOG_WARN("invalid OceanBase table identity", K(ret), K(table_name));
      } else if (state == ObString::make_string(CREATE_PREPARING)) {
        ret = OB_EAGAIN;
        LOG_WARN("OceanBase table creation is still preparing", K(ret), K(table_name));
      } else if (state != ObString::make_string(CREATE_ACTIVE)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("unknown Unified Catalog create state", K(ret), K(table_name), K(state));
      } else {
        table = candidate;
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::decode_is_iceberg_table(const ObJsonObject &table,
                                                         const ObString &table_name,
                                                         bool &is_iceberg_table)
{
  int ret = OB_SUCCESS;
  ObString table_type_text;
  if (OB_FAIL(get_string(table, TABLE_TYPE, table_type_text))) {
    LOG_WARN("failed to decode Unified Catalog table type", K(ret), K(table_name));
  } else if (table_type_text != ObString::make_string(TABLE_TYPE_OCEANBASE)
             && table_type_text != ObString::make_string(TABLE_TYPE_ICEBERG)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("unknown Unified Catalog table type", K(ret), K(table_name), K(table_type_text));
  } else if (table_type_text == ObString::make_string(TABLE_TYPE_ICEBERG)
             && OB_NOT_NULL(table.get_value(COMPLETE_METADATA))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Iceberg table contains OceanBase metadata", K(ret), K(table_name));
  } else {
    is_iceberg_table = table_type_text == ObString::make_string(TABLE_TYPE_ICEBERG);
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::parse_ob_table_payload(const ObJsonObject &table,
                                                        const ObString &table_name,
                                                        ObIAllocator &allocator,
                                                        const ObJsonObject *&source,
                                                        const ObJsonNode *&columns_node,
                                                        const ObJsonNode *&partition_spec_node)
{
  int ret = OB_SUCCESS;
  ObString metadata_json;
  ObJsonNode *metadata_node = NULL;
  source = NULL;
  columns_node = NULL;
  partition_spec_node = NULL;
  if (OB_FAIL(get_string(table, COMPLETE_METADATA, metadata_json))) {
    LOG_WARN("OceanBase table metadata is missing", K(ret), K(table_name));
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator, metadata_json, metadata_node))) {
    LOG_WARN("failed to parse OceanBase table metadata", K(ret), K(table_name));
  } else if (OB_ISNULL(metadata_node) || ObJsonNodeType::J_OBJECT != metadata_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog table metadata is invalid", K(ret), K(table_name));
  } else {
    const ObJsonObject *metadata = static_cast<const ObJsonObject *>(metadata_node);
    const ObJsonNode *source_node = metadata->get_value(METADATA_SOURCE);
    columns_node = metadata->get_value(METADATA_COLUMNS);
    partition_spec_node = metadata->get_value(PARTITION_SPEC);
    if (OB_ISNULL(source_node) || ObJsonNodeType::J_OBJECT != source_node->json_type()
        || OB_ISNULL(columns_node) || ObJsonNodeType::J_ARRAY != columns_node->json_type()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid OceanBase table metadata", K(ret), K(table_name));
    } else {
      source = static_cast<const ObJsonObject *>(source_node);
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::parse_load_table_response(const ObString &response_body,
                                                           const ObString &table_name,
                                                           ObIAllocator &allocator,
                                                           const ObJsonObject *&source,
                                                           const ObJsonNode *&columns_node,
                                                           const ObJsonNode *&partition_spec_node)
{
  int ret = OB_SUCCESS;
  const ObJsonObject *table = NULL;
  source = NULL;
  columns_node = NULL;
  partition_spec_node = NULL;
  if (OB_FAIL(parse_table_response_header(response_body, table_name, allocator, table))) {
    LOG_WARN("failed to parse Unified Catalog table header", K(ret), K(table_name));
  } else if (OB_FAIL(parse_ob_table_payload(*table, table_name, allocator,
                                            source, columns_node, partition_spec_node))) {
    LOG_WARN("failed to parse Unified Catalog OB table payload", K(ret), K(table_name));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::validate_load_table_response(const ObString &response_body,
                                                              const uint64_t cluster_id,
                                                              const uint64_t tenant_id,
                                                              const uint64_t database_id,
                                                              const ObTableSchema &table_schema,
                                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObJsonObject *source = NULL;
  const ObJsonNode *columns_node = NULL;
  const ObJsonNode *partition_spec_node = NULL;
  const bool has_primary_key_metadata = can_encode_primary_key(table_schema);
  if (OB_FAIL(validate_table_schema(table_schema))) {
    LOG_WARN("table schema is not supported by Unified Catalog synchronization", K(ret));
  } else if (OB_FAIL(parse_load_table_response(response_body,
                                               table_schema.get_table_name_str(),
                                               allocator,
                                               source,
                                               columns_node,
                                               partition_spec_node))) {
    LOG_WARN("failed to parse Unified Catalog table metadata", K(ret));
  } else if (OB_FAIL(expect_uint_string(*source, SOURCE_CLUSTER_ID, cluster_id))
             || OB_FAIL(expect_uint_string(*source, SOURCE_TENANT_ID, tenant_id))
             || OB_FAIL(expect_uint_string(*source, SOURCE_DATABASE_ID, database_id))
             || OB_FAIL(expect_uint_string(*source, SOURCE_TABLE_ID, table_schema.get_table_id()))
             || OB_FAIL(expect_uint_string(*source,
                                           SOURCE_SCHEMA_VERSION,
                                           static_cast<uint64_t>(
                                               table_schema.get_schema_version())))) {
    LOG_WARN("Unified Catalog source identity mismatch", K(ret));
  } else {
    int64_t remote_column_index = 0;
    ObColumnIterByPrevNextID column_iter(table_schema);
    const ObColumnSchemaV2 *column = NULL;
    while (OB_SUCC(ret) && OB_SUCC(column_iter.next(column))) {
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret));
      } else if (!column->is_hidden() && !column->is_shadow_column()) {
        ObIJsonBase *column_node = NULL;
        if (remote_column_index >= columns_node->element_count()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("Unified Catalog column count mismatch", K(ret), K(remote_column_index));
        } else if (OB_FAIL(columns_node->get_array_element(remote_column_index, column_node))) {
          LOG_WARN("failed to read Unified Catalog column", K(ret), K(remote_column_index));
        } else if (OB_ISNULL(column_node)
                   || ObJsonNodeType::J_OBJECT != column_node->json_type()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("Unified Catalog column shape mismatch", K(ret), K(remote_column_index));
        } else {
          const ObJsonObject &remote_column = *static_cast<const ObJsonObject *>(column_node);
          if (OB_FAIL(validate_column(remote_column,
                                      *column,
                                      has_primary_key_metadata,
                                      allocator))) {
            LOG_WARN("Unified Catalog column metadata mismatch",
                     K(ret),
                     K(remote_column_index),
                     K(column->get_column_name_str()));
          } else {
            ++remote_column_index;
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)
        && remote_column_index != static_cast<int64_t>(columns_node->element_count())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("Unified Catalog column count mismatch",
               K(ret), K(remote_column_index), K(columns_node->element_count()));
    }
  }
  if (OB_SUCC(ret)) {
    if (table_schema.is_partitioned_table()) {
      if (OB_FAIL(ObUnifiedCatalogPartitionSpecCodec::validate(allocator,
                                                               table_schema,
                                                               partition_spec_node))) {
        LOG_WARN("Unified Catalog partition spec mismatch", K(ret));
      }
    } else if (OB_NOT_NULL(partition_spec_node)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("unexpected Unified Catalog partition spec for non-partitioned table", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::parse_source_schema_version(const ObJsonObject &source,
                                                             int64_t &schema_version)
{
  int ret = OB_SUCCESS;
  ObString version_text;
  schema_version = 0;
  if (OB_FAIL(get_string(source, SOURCE_SCHEMA_VERSION, version_text))) {
    LOG_WARN("failed to read Unified Catalog source schema version", K(ret));
  } else if (version_text.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog source schema version is missing", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < version_text.length(); ++i) {
      const char digit = version_text.ptr()[i];
      if (digit < '0' || digit > '9' || schema_version > (INT64_MAX - (digit - '0')) / 10) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("invalid Unified Catalog source schema version", K(ret));
      } else {
        schema_version = schema_version * 10 + digit - '0';
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::get_uint_string(const ObJsonObject &object,
                                                 const char *key,
                                                 uint64_t &value)
{
  int ret = OB_SUCCESS;
  ObString text;
  value = 0;
  if (OB_FAIL(get_string(object, key, text))) {
    LOG_WARN("failed to read Unified Catalog unsigned field", K(ret), K(key));
  } else if (text.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog unsigned field is missing", K(ret), K(key));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < text.length(); ++i) {
    const char digit = text.ptr()[i];
    if (digit < '0' || digit > '9' || value > (UINT64_MAX - (digit - '0')) / 10) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid Unified Catalog unsigned field", K(ret), K(key));
    } else {
      value = value * 10 + digit - '0';
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::decode_ob_column(const ObJsonObject &column_object,
                                                  ObUnifiedCatalogObColumnMetadata &column)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *type_node = column_object.get_value(COLUMN_OB_TYPE);
  const ObJsonNode *nullable_node = column_object.get_value(COLUMN_NULLABLE);
  const ObJsonNode *rowkey_node = column_object.get_value(COLUMN_ROWKEY_POSITION);
  if (OB_FAIL(get_uint_string(column_object, COLUMN_ID, column.column_id_))) {
    LOG_WARN("failed to decode Unified Catalog column id", K(ret));
  } else if (OB_FAIL(get_string(column_object, COLUMN_NAME, column.name_))
             || OB_FAIL(get_string(column_object, COLUMN_NATIVE_TYPE, column.native_type_))) {
    LOG_WARN("failed to decode Unified Catalog column name or native type", K(ret));
  } else if (0 == column.column_id_ || column.name_.empty() || column.native_type_.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog column identity or native type", K(ret));
  } else if (OB_ISNULL(type_node) || ObJsonNodeType::J_INT != type_node->json_type()
             || static_cast<const ObJsonInt *>(type_node)->value() <= ObNullType
             || static_cast<const ObJsonInt *>(type_node)->value() >= ObMaxType
             || OB_ISNULL(nullable_node)
             || ObJsonNodeType::J_BOOLEAN != nullable_node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid Unified Catalog column type or nullability", K(ret));
  } else {
    column.ob_type_ = static_cast<ObObjType>(static_cast<const ObJsonInt *>(type_node)->value());
    column.nullable_ = static_cast<const ObJsonBoolean *>(nullable_node)->value();
    if (OB_NOT_NULL(rowkey_node)) {
      if (ObJsonNodeType::J_INT != rowkey_node->json_type()
          || static_cast<const ObJsonInt *>(rowkey_node)->value() <= 0) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("invalid Unified Catalog rowkey position", K(ret));
      } else {
        column.rowkey_position_ = static_cast<const ObJsonInt *>(rowkey_node)->value();
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::parse_ob_table_metadata_response(
    const ObString &response_body,
    const ObString &table_name,
    ObIAllocator &allocator,
    ObUnifiedCatalogObTableMetadata &metadata,
    const ObJsonObject *&source,
    const ObJsonNode *&columns_node,
    const ObJsonNode *&partition_spec_node,
    bool &is_ob_table)
{
  int ret = OB_SUCCESS;
  const ObJsonObject *table = NULL;
  bool is_iceberg_table = false;
  source = NULL;
  columns_node = NULL;
  partition_spec_node = NULL;
  metadata.columns_.reset();
  metadata.table_name_ = table_name;
  metadata.cluster_id_ = 0;
  metadata.tenant_id_ = 0;
  metadata.database_id_ = 0;
  metadata.table_id_ = 0;
  metadata.schema_version_ = 0;
  metadata.partition_spec_ = NULL;
  if (table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog table identity", K(ret));
  } else if (OB_FAIL(parse_table_response_header(response_body, table_name, allocator, table))) {
    LOG_WARN("failed to parse Unified Catalog OceanBase Table response", K(ret), K(table_name));
  } else if (OB_FAIL(decode_is_iceberg_table(*table, table_name, is_iceberg_table))) {
    LOG_WARN("failed to identify Unified Catalog table format", K(ret), K(table_name));
  } else if (is_iceberg_table) {
    is_ob_table = false;
  } else {
    is_ob_table = true;
    if (OB_FAIL(parse_ob_table_payload(*table, table_name, allocator,
                                       source, columns_node, partition_spec_node))) {
      LOG_WARN("failed to parse Unified Catalog OB table payload", K(ret), K(table_name));
    } else if (OB_FAIL(parse_source_schema_version(*source, metadata.schema_version_))) {
      LOG_WARN("failed to decode Unified Catalog OB schema version", K(ret), K(table_name));
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::decode_ob_table_columns(const ObJsonNode &columns_node,
                                                         const ObJsonNode *partition_spec_node,
                                                         ObUnifiedCatalogObTableMetadata &metadata)
{
  int ret = OB_SUCCESS;
  if (0 == columns_node.element_count()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog OB table has no columns", K(ret));
  } else {
    metadata.partition_spec_ = partition_spec_node;
    for (int64_t i = 0; OB_SUCC(ret) && i < columns_node.element_count(); ++i) {
      ObIJsonBase *column_node = NULL;
      ObUnifiedCatalogObColumnMetadata column;
      if (OB_FAIL(columns_node.get_array_element(i, column_node))) {
        LOG_WARN("failed to read Unified Catalog OB column", K(ret), K(i));
      } else if (OB_ISNULL(column_node) || ObJsonNodeType::J_OBJECT != column_node->json_type()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("invalid Unified Catalog OB column", K(ret), K(i));
      } else if (OB_FAIL(decode_ob_column(*static_cast<const ObJsonObject *>(column_node), column))) {
        LOG_WARN("failed to decode Unified Catalog OB column", K(ret), K(i));
      } else if (OB_FAIL(metadata.columns_.push_back(column))) {
        LOG_WARN("failed to append Unified Catalog OB column", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::try_decode_ob_table_metadata(const ObString &response_body,
                                                              const ObString &table_name,
                                                              ObIAllocator &allocator,
                                                              ObUnifiedCatalogObTableMetadata &metadata,
                                                              bool &is_ob_table)
{
  return decode_ob_table_metadata_impl(response_body, table_name, allocator, metadata, is_ob_table);
}

int ObUnifiedCatalogSchemaCodec::decode_query_table_kind(
    const ObString &response_body,
    const uint64_t cluster_id,
    const uint64_t tenant_id,
    const ObString &table_name,
    ObIAllocator &allocator,
    ObUnifiedCatalogQueryTableKind &query_table_kind)
{
  int ret = OB_SUCCESS;
  const ObJsonObject *table = NULL;
  const ObJsonObject *source = NULL;
  const ObJsonNode *columns_node = NULL;
  const ObJsonNode *partition_spec_node = NULL;
  bool is_iceberg_table = false;
  uint64_t source_cluster_id = 0;
  uint64_t source_tenant_id = 0;
  if (table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Unified Catalog table identity", K(ret));
  } else if (OB_FAIL(parse_table_response_header(response_body, table_name, allocator, table))) {
    LOG_WARN("failed to decode Unified Catalog table header", K(ret), K(table_name));
  } else if (OB_FAIL(decode_is_iceberg_table(*table, table_name, is_iceberg_table))) {
    LOG_WARN("failed to decode Unified Catalog table type", K(ret), K(table_name));
  } else if (is_iceberg_table) {
    query_table_kind = ObUnifiedCatalogQueryTableKind::ICEBERG;
  } else if (OB_FAIL(parse_ob_table_payload(*table,
                                            table_name,
                                            allocator,
                                            source,
                                            columns_node,
                                            partition_spec_node))) {
    LOG_WARN("failed to decode Unified Catalog OB table payload", K(ret), K(table_name));
  } else if (OB_FAIL(get_uint_string(*source, SOURCE_CLUSTER_ID, source_cluster_id))
             || OB_FAIL(get_uint_string(*source, SOURCE_TENANT_ID, source_tenant_id))) {
    LOG_WARN("failed to decode Unified Catalog OB table source", K(ret), K(table_name));
  } else if (cluster_id == source_cluster_id && tenant_id == source_tenant_id) {
    query_table_kind = ObUnifiedCatalogQueryTableKind::LOCAL_OB;
  } else {
    query_table_kind = ObUnifiedCatalogQueryTableKind::NON_LOCAL_OB;
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::decode_ob_table_metadata_impl(const ObString &response_body,
                                                               const ObString &table_name,
                                                               ObIAllocator &allocator,
                                                               ObUnifiedCatalogObTableMetadata &metadata,
                                                               bool &is_ob_table)
{
  const ObJsonObject *source = NULL;
  const ObJsonNode *columns_node = NULL;
  const ObJsonNode *partition_spec_node = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_ob_table_metadata_response(response_body, table_name, allocator, metadata,
                                               source, columns_node, partition_spec_node,
                                               is_ob_table))) {
    LOG_WARN("failed to parse Unified Catalog OB table metadata response", K(ret), K(table_name));
  } else if (is_ob_table
             && OB_FAIL(decode_ob_table_columns(*columns_node, partition_spec_node, metadata))) {
    LOG_WARN("failed to decode Unified Catalog OB table columns", K(ret), K(table_name));
  }
  return ret;
}

int ObUnifiedCatalogSchemaCodec::validate_table_source(const ObString &response_body,
                                                       const uint64_t cluster_id,
                                                       const uint64_t tenant_id,
                                                       const uint64_t database_id,
                                                       const ObString &table_name,
                                                       const uint64_t table_id,
                                                       ObIAllocator &allocator,
                                                       int64_t *schema_version)
{
  int ret = OB_SUCCESS;
  const ObJsonObject *source = NULL;
  const ObJsonNode *columns_node = NULL;
  const ObJsonNode *partition_spec_node = NULL;
  if (table_name.empty() || OB_INVALID_ID == database_id || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected Unified Catalog table source identity",
             K(ret), K(database_id), K(table_name), K(table_id));
  } else if (OB_FAIL(parse_load_table_response(response_body,
                                               table_name,
                                               allocator,
                                               source,
                                               columns_node,
                                               partition_spec_node))) {
    LOG_WARN("failed to parse Unified Catalog table source", K(ret), K(table_name));
  } else if (OB_FAIL(expect_uint_string(*source, SOURCE_CLUSTER_ID, cluster_id))
             || OB_FAIL(expect_uint_string(*source, SOURCE_TENANT_ID, tenant_id))
             || OB_FAIL(expect_uint_string(*source, SOURCE_DATABASE_ID, database_id))
             || OB_FAIL(expect_uint_string(*source, SOURCE_TABLE_ID, table_id))) {
    LOG_WARN("Unified Catalog table source identity mismatch",
             K(ret),
             K(database_id),
             K(table_name),
             K(table_id));
  } else if (OB_NOT_NULL(schema_version)
             && OB_FAIL(parse_source_schema_version(*source, *schema_version))) {
    LOG_WARN("invalid Unified Catalog table source schema version", K(ret), K(table_name));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
