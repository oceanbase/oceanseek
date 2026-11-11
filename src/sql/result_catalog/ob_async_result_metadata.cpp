/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/result_catalog/ob_async_result_metadata.h"

#include <type_traits>

#include "observer/omt/ob_tenant_config_mgr.h"
#include "common/object/ob_obj_type.h"
#include "lib/allocator/page_arena.h"
#include "lib/charset/ob_charset.h"
#include "lib/enumset/ob_enum_set_meta.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/utility/ob_sort.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/basic/ob_select_into_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/result_catalog/ob_async_result_arrow_codec.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

namespace
{

static const char *const RESULT_METADATA_FORMAT = "oceanbase-result-table";
static const char *const RESULT_DATA_FORMAT = "PARQUET";
static const char *const RESULT_PATH_MODE = "RELATIVE_TO_RESULT_ROOT";
static const char *const RESULT_ORDERING_UNSPECIFIED = "UNSPECIFIED";
static const char *const RESULT_ORDERING_SORTED_WITHIN_FILE = "SORTED_WITHIN_FILE";
static const char *const RESULT_ORDERING_SORTED_WITHIN_WRITER = "SORTED_WITHIN_WRITER";
static const char *const RESULT_ORDER_ASC = "ASC";
static const char *const RESULT_ORDER_DESC = "DESC";
static const char *const RESULT_NULL_ORDER_FIRST = "FIRST";
static const char *const RESULT_NULL_ORDER_LAST = "LAST";
static const char *const RESULT_METADATA_FILE_NAME = "metadata.json";
static const char *const RESULT_DATA_FILE_PREFIX = "part";
static const char *const RESULT_RELATIVE_ROOT = "__ob_async_query_result/";
static const char *const RESULT_ROOT_MARKER_FILE_NAME = ".keep";

int build_result_job_path(const uint64_t tenant_id,
                          const uint64_t job_id,
                          const char *leaf_name,
                          ObSqlString &path)
{
  int ret = OB_SUCCESS;
  path.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == job_id)
      || OB_ISNULL(leaf_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result path identity", KR(ret), K(tenant_id), K(job_id), KP(leaf_name));
  } else if (OB_FAIL(path.append_fmt("tenant=%016lx/job=%016lx/",
                                     static_cast<unsigned long>(tenant_id),
                                     static_cast<unsigned long>(job_id)))) {
    LOG_WARN("failed to append async result job path", KR(ret), K(tenant_id), K(job_id));
  } else if ('\0' != leaf_name[0] && OB_FAIL(path.append(leaf_name))) {
    LOG_WARN("failed to append async result path leaf", KR(ret), K(tenant_id), K(job_id), K(leaf_name));
  }
  return ret;
}

int deep_copy_string(ObIAllocator &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  if (src.empty()) {
    dst.reset();
  } else if (OB_FAIL(ob_write_string(allocator, src, dst, true))) {
    LOG_WARN("failed to copy async result metadata string", K(ret));
  }
  return ret;
}

int alloc_json_object(ObIAllocator &allocator, ObJsonObject *&object)
{
  int ret = OB_SUCCESS;
  object = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(object)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate result metadata json object", K(ret));
  }
  return ret;
}

int alloc_json_array(ObIAllocator &allocator, ObJsonArray *&array)
{
  int ret = OB_SUCCESS;
  array = OB_NEWx(ObJsonArray, &allocator, &allocator);
  if (OB_ISNULL(array)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate result metadata json array", K(ret));
  }
  return ret;
}

int add_json_string(ObIAllocator &allocator,
                    ObJsonObject &object,
                    const char *key,
                    const ObString &value)
{
  int ret = OB_SUCCESS;
  ObJsonString *node = OB_NEWx(ObJsonString, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate result metadata json string", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add result metadata json string", K(ret), K(key));
  }
  return ret;
}

int add_json_string_array(ObIAllocator &allocator,
                          ObJsonObject &object,
                          const char *key,
                          const ObIArray<ObString> &values)
{
  int ret = OB_SUCCESS;
  ObJsonArray *array = NULL;
  if (OB_FAIL(alloc_json_array(allocator, array))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
    ObJsonString *node = OB_NEWx(ObJsonString, &allocator, values.at(i));
    if (OB_ISNULL(node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate result metadata json array string",
               K(ret), K(key), K(i));
    } else if (OB_FAIL(array->array_append(node))) {
      LOG_WARN("failed to append result metadata json array string",
               K(ret), K(key), K(i));
    }
  }
  if (OB_SUCC(ret)
      && OB_FAIL(object.object_add(ObString::make_string(key), array))) {
    LOG_WARN("failed to add result metadata json string array", K(ret), K(key));
  }
  return ret;
}

int add_json_int(ObIAllocator &allocator,
                 ObJsonObject &object,
                 const char *key,
                 const int64_t value)
{
  int ret = OB_SUCCESS;
  ObJsonInt *node = OB_NEWx(ObJsonInt, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate result metadata json integer", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add result metadata json integer", K(ret), K(key));
  }
  return ret;
}

int add_json_bool(ObIAllocator &allocator,
                  ObJsonObject &object,
                  const char *key,
                  const bool value)
{
  int ret = OB_SUCCESS;
  ObJsonBoolean *node = OB_NEWx(ObJsonBoolean, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate result metadata json boolean", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add result metadata json boolean", K(ret), K(key));
  }
  return ret;
}

int get_required_object(const ObJsonObject &parent,
                        const char *key,
                        const ObJsonObject *&object)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = parent.get_value(ObString::make_string(key));
  if (OB_ISNULL(node) || ObJsonNodeType::J_OBJECT != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid result metadata json object", K(ret), K(key));
  } else {
    object = down_cast<const ObJsonObject *>(node);
  }
  return ret;
}

int get_required_array(const ObJsonObject &parent,
                       const char *key,
                       const ObJsonArray *&array)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = parent.get_value(ObString::make_string(key));
  if (OB_ISNULL(node) || ObJsonNodeType::J_ARRAY != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid result metadata json array", K(ret), K(key));
  } else {
    array = down_cast<const ObJsonArray *>(node);
  }
  return ret;
}

template <typename T>
int get_required_primitive(const ObJsonObject &object, const char *key, T &value)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = object.get_value(ObString::make_string(key));
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("required result metadata field does not exist", K(ret), K(key));
  } else if constexpr (std::is_same_v<T, int64_t>) {
    if (ObJsonNodeType::J_INT == node->json_type()) {
      value = node->get_int();
    } else if (ObJsonNodeType::J_UINT == node->json_type()
               && node->get_uint() <= static_cast<uint64_t>(INT64_MAX)) {
      value = static_cast<int64_t>(node->get_uint());
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid result metadata integer", K(ret), K(key), K(node->json_type()));
    }
  } else if constexpr (std::is_same_v<T, bool>) {
    if (ObJsonNodeType::J_BOOLEAN == node->json_type()) {
      value = node->get_boolean();
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid result metadata boolean", K(ret), K(key), K(node->json_type()));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported result metadata primitive", K(ret), K(key));
  }
  return ret;
}

int get_optional_int(const ObJsonObject &object,
                     const char *key,
                     int64_t &value,
                     const int64_t default_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(object.get_value(ObString::make_string(key)))) {
    value = default_value;
  } else if (OB_FAIL(get_required_primitive(object, key, value))) {
    LOG_WARN("failed to read optional result metadata integer", K(ret), K(key));
  }
  return ret;
}

int get_optional_string(ObIAllocator &allocator,
                        const ObJsonObject &object,
                        const char *key,
                        ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(object.get_value(ObString::make_string(key)))) {
    value.reset();
  } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                         allocator, object, ObString::make_string(key), value))) {
    LOG_WARN("failed to read optional result metadata string", K(ret), K(key));
  }
  return ret;
}

int get_optional_string_array(ObIAllocator &allocator,
                              const ObJsonObject &object,
                              const char *key,
                              ObIArray<ObString> &values)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = object.get_value(ObString::make_string(key));
  values.reset();
  if (OB_ISNULL(node)) {
  } else if (ObJsonNodeType::J_ARRAY != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid result metadata json string array",
             K(ret), K(key), K(node->json_type()));
  } else {
    const ObJsonArray *array = down_cast<const ObJsonArray *>(node);
    for (int64_t i = 0;
         OB_SUCC(ret) && i < static_cast<int64_t>(array->element_count());
         ++i) {
      const ObJsonNode *item = array->get_value(i);
      ObString copied;
      if (OB_ISNULL(item) || ObJsonNodeType::J_STRING != item->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid result metadata json string array item",
                 K(ret), K(key), K(i));
      } else if (OB_FAIL(deep_copy_string(
                     allocator,
                     down_cast<const ObJsonString *>(item)->value(),
                     copied))) {
        LOG_WARN("failed to copy result metadata json string array item",
                 K(ret), K(key), K(i));
      } else if (OB_FAIL(values.push_back(copied))) {
        LOG_WARN("failed to retain result metadata json string array item",
                 K(ret), K(key), K(i));
      }
    }
  }
  return ret;
}

int add_column_json(ObIAllocator &allocator,
                    const ObResultColumnMetadata &column,
                    ObJsonArray &columns)
{
  int ret = OB_SUCCESS;
  ObJsonObject *column_json = NULL;
  ObJsonObject *ob_type_info = NULL;
  ObJsonObject *physical = NULL;
  ObJsonObject *codec = NULL;
  if (OB_FAIL(alloc_json_object(allocator, column_json))) {
  } else if (OB_FAIL(add_json_int(allocator, *column_json, "field_id", column.field_id_))) {
  } else if (OB_FAIL(add_json_int(allocator, *column_json, "ordinal", column.ordinal_))) {
  } else if (OB_FAIL(add_json_string(allocator, *column_json, "name", column.name_))) {
  } else if (OB_FAIL(add_json_bool(allocator, *column_json, "nullable", column.nullable_))) {
  } else if (OB_FAIL(alloc_json_object(allocator, ob_type_info))) {
  } else if (OB_FAIL(add_json_int(allocator, *ob_type_info, "type_id", column.ob_type_id_))) {
  } else if (OB_FAIL(add_json_string(allocator, *ob_type_info, "type_name", column.ob_type_name_))) {
  } else if (OB_FAIL(add_json_string(allocator, *ob_type_info, "sql_type", column.sql_type_))) {
  } else if (column.precision_ >= 0
             && OB_FAIL(add_json_int(allocator, *ob_type_info, "precision", column.precision_))) {
  } else if (column.scale_ >= 0
             && OB_FAIL(add_json_int(allocator, *ob_type_info, "scale", column.scale_))) {
  } else if (column.length_ >= 0
             && OB_FAIL(add_json_int(allocator, *ob_type_info, "length", column.length_))) {
  } else if (column.length_semantics_ >= 0
             && OB_FAIL(add_json_int(allocator, *ob_type_info,
                                     "length_semantics", column.length_semantics_))) {
  } else if (OB_FAIL(add_json_bool(allocator, *ob_type_info, "unsigned", column.is_unsigned_))) {
  } else if (OB_FAIL(add_json_int(allocator, *ob_type_info,
                                  "collation_id", column.collation_id_))) {
  } else if (OB_FAIL(add_json_int(allocator, *ob_type_info,
                                  "collation_level", column.collation_level_))) {
  } else if (!column.charset_name_.empty()
             && OB_FAIL(add_json_string(allocator, *ob_type_info,
                                        "charset", column.charset_name_))) {
  } else if (!column.collation_name_.empty()
             && OB_FAIL(add_json_string(allocator, *ob_type_info,
                                        "collation", column.collation_name_))) {
  } else if (column.subschema_id_ >= 0
             && OB_FAIL(add_json_int(allocator, *ob_type_info,
                                     "subschema_id", column.subschema_id_))) {
  } else if (!column.extended_type_info_.empty()
             && OB_FAIL(add_json_string_array(allocator, *ob_type_info,
                                              "extended_type_info",
                                              column.extended_type_info_))) {
  } else if (OB_FAIL(column_json->object_add(
                         ObString::make_string("ob_type_info"), ob_type_info))) {
  } else if (OB_FAIL(alloc_json_object(allocator, physical))) {
  } else if (OB_FAIL(add_json_string(allocator, *physical,
                                     "arrow_type", column.arrow_type_))) {
  } else if (OB_FAIL(add_json_string(allocator, *physical,
                                     "parquet_physical_type",
                                     column.parquet_physical_type_))) {
  } else if (!column.parquet_logical_type_.empty()
             && OB_FAIL(add_json_string(allocator, *physical,
                                        "parquet_logical_type",
                                        column.parquet_logical_type_))) {
  } else if (OB_FAIL(column_json->object_add(ObString::make_string("physical"), physical))) {
  } else if (OB_FAIL(alloc_json_object(allocator, codec))) {
  } else if (OB_FAIL(add_json_string(allocator, *codec, "name", column.codec_name_))) {
  } else if (OB_FAIL(add_json_int(allocator, *codec, "version", column.codec_version_))) {
  } else if (OB_FAIL(column_json->object_add(ObString::make_string("codec"), codec))) {
  } else if (OB_FAIL(columns.array_append(column_json))) {
    LOG_WARN("failed to append result metadata column", K(ret));
  }
  return ret;
}

int add_partition_json(ObIAllocator &allocator,
                       const ObResultPartitionMetadata &partition,
                       ObJsonArray &partitions)
{
  int ret = OB_SUCCESS;
  ObJsonObject *partition_json = NULL;
  ObJsonArray *files = NULL;
  if (OB_FAIL(alloc_json_object(allocator, partition_json))) {
  } else if (OB_FAIL(add_json_int(allocator, *partition_json,
                                  "partition_id", partition.partition_id_))) {
  } else if (OB_FAIL(add_json_int(allocator, *partition_json,
                                  "row_count", partition.row_count_))) {
  } else if (OB_FAIL(add_json_int(allocator, *partition_json,
                                  "compressed_size", partition.compressed_size_))) {
  } else if (OB_FAIL(alloc_json_array(allocator, files))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition.files_.count(); ++i) {
    const ObResultFileMetadata &file = partition.files_.at(i);
    ObJsonObject *file_json = NULL;
    ObJsonArray *row_groups = NULL;
    if (OB_FAIL(alloc_json_object(allocator, file_json))) {
    } else if (OB_FAIL(add_json_string(allocator, *file_json, "path", file.path_))) {
    } else if (OB_FAIL(add_json_int(allocator, *file_json, "row_count", file.row_count_))) {
    } else if (OB_FAIL(add_json_int(allocator, *file_json, "byte_count", file.byte_count_))) {
    } else if (file.writer_sqc_id_ >= 0
               && OB_FAIL(add_json_int(allocator, *file_json,
                                       "writer_sqc_id", file.writer_sqc_id_))) {
    } else if (file.writer_task_id_ >= 0
               && OB_FAIL(add_json_int(allocator, *file_json,
                                       "writer_task_id", file.writer_task_id_))) {
    } else if (file.file_sequence_ >= 0
               && OB_FAIL(add_json_int(allocator, *file_json,
                                       "file_sequence", file.file_sequence_))) {
    } else if (OB_FAIL(alloc_json_array(allocator, row_groups))) {
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < file.row_groups_.count(); ++j) {
      const ObResultFileMetadata::RowGroup &row_group = file.row_groups_.at(j);
      ObJsonObject *row_group_json = NULL;
      if (OB_FAIL(alloc_json_object(allocator, row_group_json))) {
      } else if (OB_FAIL(add_json_int(allocator, *row_group_json,
                                      "ordinal", row_group.ordinal_))) {
      } else if (OB_FAIL(add_json_int(allocator, *row_group_json,
                                      "row_count", row_group.row_count_))) {
      } else if (OB_FAIL(row_groups->array_append(row_group_json))) {
        LOG_WARN("failed to append result metadata row group", K(ret), K(i), K(j));
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(file_json->object_add(ObString::make_string("row_groups"), row_groups))) {
      LOG_WARN("failed to add result metadata row groups", K(ret), K(i));
    } else if (OB_SUCC(ret) && OB_FAIL(files->array_append(file_json))) {
      LOG_WARN("failed to append result metadata file", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)
      && OB_FAIL(partition_json->object_add(ObString::make_string("files"), files))) {
    LOG_WARN("failed to add result metadata files", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(partitions.array_append(partition_json))) {
    LOG_WARN("failed to append result metadata partition", K(ret));
  }
  return ret;
}

int add_ordering_field_json(ObIAllocator &allocator,
                            const ObResultOrderingFieldMetadata &field,
                            ObJsonArray &ordering_fields)
{
  int ret = OB_SUCCESS;
  ObJsonObject *field_json = NULL;
  if (OB_FAIL(alloc_json_object(allocator, field_json))) {
  } else if (OB_FAIL(add_json_int(allocator, *field_json, "field_id", field.field_id_))) {
  } else if (OB_FAIL(add_json_string(
                 allocator,
                 *field_json,
                 "direction",
                 ObString::make_string(field.ascending_ ? RESULT_ORDER_ASC : RESULT_ORDER_DESC)))) {
  } else if (OB_FAIL(add_json_string(
                 allocator,
                 *field_json,
                 "null_order",
                 ObString::make_string(
                     field.nulls_first_ ? RESULT_NULL_ORDER_FIRST : RESULT_NULL_ORDER_LAST)))) {
  } else if (OB_FAIL(ordering_fields.array_append(field_json))) {
    LOG_WARN("failed to append result metadata ordering field", K(ret), K(field));
  }
  return ret;
}

int build_column_metadata(ObIAllocator &allocator,
                          ObExecContext &exec_ctx,
                          const ObSelectIntoSpec &spec,
                          const int64_t column_idx,
                          ObResultColumnMetadata &column)
{
  int ret = OB_SUCCESS;
  ObExpr *expr = NULL;
  const char *type_name = NULL;
  const char *charset_name = NULL;
  const char *collation_name = NULL;
  char sql_type_buf[512] = {0};
  int64_t sql_type_pos = 0;
  ObSEArray<ObString, 4> type_info;
  const ObSqlCollectionInfo *collection_info = NULL;
  const ObEnumSetMeta *enum_set_meta = NULL;
  if (column_idx < 0 || column_idx >= spec.select_exprs_.count()
      || column_idx >= spec.alias_names_.strs_.count()
      || OB_ISNULL(expr = spec.select_exprs_.at(column_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result metadata column", K(ret), K(column_idx));
  } else {
    const ObObjType type = expr->datum_meta_.type_;
    ObCollationType collation = ObCollectionSQLType == type
                              ? CS_TYPE_BINARY
                              : expr->obj_meta_.get_collation_type();
    ObCollationLevel collation_level = ObCollectionSQLType == type
                                     ? CS_LEVEL_IMPLICIT
                                     : expr->obj_meta_.get_collation_level();
    column.field_id_ = spec.field_ids_.count() == spec.select_exprs_.count()
                     ? static_cast<int64_t>(spec.field_ids_.at(column_idx))
                     : column_idx + 1;
    column.ordinal_ = column_idx;
    // Async Parquet result fields are always written as OPTIONAL.  Do not
    // advertise a stronger NOT NULL constraint in RESULT_SCAN metadata.
    column.nullable_ = true;
    column.ob_type_id_ = static_cast<int64_t>(type);
    column.precision_ = ob_is_string_type(type) ? -1 : expr->datum_meta_.precision_;
    column.scale_ = expr->datum_meta_.scale_;
    column.length_ = expr->max_length_;
    column.length_semantics_ = ob_is_string_type(type)
                             ? expr->datum_meta_.length_semantics_
                             : -1;
    column.is_unsigned_ = ob_is_unsigned_type(type);
    column.subschema_id_ = (ObUserDefinedSQLType == type
                            || ObCollectionSQLType == type
                            || ObSetType == type)
                         ? expr->obj_meta_.get_subschema_id()
                         : -1;
    if (ObEnumType == type) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "ENUM in asynchronous query results");
      LOG_WARN("ENUM async result column is not supported", K(ret), K(column_idx));
    } else if (ObUserDefinedSQLType == type) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "user-defined type in asynchronous query results");
      LOG_WARN("user-defined async result column is not supported",
               K(ret), K(column_idx), K(column.subschema_id_));
    } else if (ObSetType == type) {
      if (OB_FAIL(exec_ctx.get_enumset_meta_by_subschema_id(
              expr->obj_meta_.get_subschema_id(), false /* is_in_pl */, enum_set_meta))) {
        LOG_WARN("failed to resolve async result SET descriptor",
                 K(ret), K(column_idx), K(column.subschema_id_));
      } else if (OB_ISNULL(enum_set_meta)
                 || !enum_set_meta->is_valid()
                 || ObSetType != enum_set_meta->get_type()
                 || OB_ISNULL(enum_set_meta->get_str_values())
                 || enum_set_meta->get_str_values()->empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async result SET descriptor",
                 K(ret), K(column_idx), K(column.subschema_id_), KP(enum_set_meta));
      } else {
        collation = enum_set_meta->get_collation_type();
        collation_level = enum_set_meta->get_collation_level();
        for (int64_t i = 0;
             OB_SUCC(ret) && i < enum_set_meta->get_str_values()->count();
             ++i) {
          const ObString &value = enum_set_meta->get_str_values()->at(i);
          ObString copied_value;
          if (OB_FAIL(type_info.push_back(value))) {
            LOG_WARN("failed to remember async result SET member",
                     K(ret), K(column_idx), K(i));
          } else if (OB_FAIL(deep_copy_string(allocator, value, copied_value))) {
            LOG_WARN("failed to copy async result SET member",
                     K(ret), K(column_idx), K(i));
          } else if (OB_FAIL(column.extended_type_info_.push_back(copied_value))) {
            LOG_WARN("failed to retain async result SET member",
                     K(ret), K(column_idx), K(i));
          }
        }
      }
    } else if (ObCollectionSQLType == type) {
      ObSubSchemaValue sub_meta;
      if (OB_FAIL(exec_ctx.get_sqludt_meta_by_subschema_id(
              expr->obj_meta_.get_subschema_id(), sub_meta))) {
        LOG_WARN("failed to resolve async result collection descriptor",
                 K(ret), K(column_idx),
                 "subschema_id", expr->obj_meta_.get_subschema_id());
      } else if (OB_UNLIKELY(OB_SUBSCHEMA_COLLECTION_TYPE != sub_meta.type_)
                 || OB_ISNULL(sub_meta.value_)
                 || OB_ISNULL(collection_info = reinterpret_cast<const ObSqlCollectionInfo *>(
                        sub_meta.value_))
                 || collection_info->get_def_string().empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid async result collection descriptor",
                 K(ret), K(column_idx), K(sub_meta));
      } else if (OB_FAIL(type_info.push_back(collection_info->get_def_string()))) {
        LOG_WARN("failed to remember async result collection descriptor",
                 K(ret), K(column_idx));
      } else {
        ObString copied_type_info;
        if (OB_FAIL(deep_copy_string(allocator,
                                    collection_info->get_def_string(),
                                    copied_type_info))) {
          LOG_WARN("failed to copy async result collection descriptor",
                   K(ret), K(column_idx));
        } else if (OB_FAIL(column.extended_type_info_.push_back(copied_type_info))) {
          LOG_WARN("failed to retain async result collection descriptor",
                   K(ret), K(column_idx));
        }
      }
    }
    if (OB_SUCC(ret)) {
      column.collation_id_ = static_cast<int64_t>(collation);
      column.collation_level_ = static_cast<int64_t>(collation_level);
      type_name = ob_obj_type_str(type);
      charset_name = ObCharset::charset_name(collation);
      collation_name = ObCharset::collation_name(collation);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(type_name)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get async result ob type name", K(ret), K(type));
    } else if (OB_FAIL(deep_copy_string(allocator,
                                        spec.alias_names_.strs_.at(column_idx),
                                        column.name_))) {
    } else if (OB_FAIL(deep_copy_string(allocator,
                                        ObString::make_string(type_name),
                                        column.ob_type_name_))) {
    } else if (OB_NOT_NULL(charset_name)
               && OB_FAIL(deep_copy_string(allocator,
                                           ObString::make_string(charset_name),
                                           column.charset_name_))) {
    } else if (OB_NOT_NULL(collation_name)
               && OB_FAIL(deep_copy_string(allocator,
                                           ObString::make_string(collation_name),
                                           column.collation_name_))) {
    } else if (ObCollectionSQLType == type) {
      if (OB_FAIL(deep_copy_string(allocator,
                                   collection_info->get_def_string(),
                                   column.sql_type_))) {
        LOG_WARN("failed to copy async result collection sql type",
                 K(ret), K(column_idx));
      }
    } else if (OB_FAIL(ob_sql_type_str(sql_type_buf,
                                      sizeof(sql_type_buf),
                                      sql_type_pos,
                                      type,
                                      expr->max_length_,
                                      column.precision_,
                                      column.scale_,
                                      collation,
                                      type_info))) {
      LOG_WARN("failed to format async result sql type", K(ret), K(type));
    } else if (OB_FAIL(deep_copy_string(allocator,
                                       ObString(sql_type_pos, sql_type_buf),
                                       column.sql_type_))) {
    }
    if (OB_SUCC(ret)
        && OB_FAIL(ObAsyncResultArrowCodec::fill_result_column_metadata(
               allocator, *expr, column))) {
      LOG_WARN("failed to build async result physical type metadata", K(ret), K(column_idx));
    }
  }
  return ret;
}

int resolve_result_paths(const ObSelectIntoSpec &spec,
                         ObIAllocator &allocator,
                         share::ObBackupDest &storage_dest,
                         ObString &full_result_root,
                         ObString &full_metadata_path)
{
  int ret = OB_SUCCESS;
  ObSqlString metadata_relative_path;
  if (OB_UNLIKELY(!spec.async_result_target_.enabled_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot resolve paths for a non-async-result sink", KR(ret));
  } else if (OB_FAIL(ObAsyncResultStorageUtils::build_result_metadata_relative_path(
                         spec.async_result_target_.tenant_id_,
                         spec.async_result_target_.job_id_,
                         metadata_relative_path))) {
    LOG_WARN("failed to build async result metadata path", KR(ret),
             K(spec.async_result_target_.tenant_id_), K(spec.async_result_target_.job_id_));
  } else if (OB_FAIL(ObAsyncResultStorageUtils::resolve_internal_path(
                         allocator,
                         metadata_relative_path.string(),
                         storage_dest,
                         full_metadata_path))) {
    LOG_WARN("failed to resolve async result metadata path", K(ret));
  } else if (!full_metadata_path.suffix_match(RESULT_METADATA_FILE_NAME)
             || full_metadata_path.length()
                    <= static_cast<int64_t>(STRLEN(RESULT_METADATA_FILE_NAME))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid full async result metadata path", K(ret), K(full_metadata_path));
  } else {
    const ObString result_root_view(
        full_metadata_path.length() - STRLEN(RESULT_METADATA_FILE_NAME),
        full_metadata_path.ptr());
    if (OB_FAIL(ob_write_string(allocator, result_root_view, full_result_root, true))) {
      LOG_WARN("failed to save async result root path", K(ret), K(result_root_view));
    }
  }
  return ret;
}

int make_relative_data_file_path(ObIAllocator &allocator,
                                 const ObString &full_result_root,
                                 const ObString &full_file_path,
                                 ObString &relative_file_path)
{
  int ret = OB_SUCCESS;
  if (full_result_root.empty()
      || full_file_path.length() <= full_result_root.length()
      || 0 != MEMCMP(full_file_path.ptr(),
                     full_result_root.ptr(),
                     full_result_root.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async result data file is outside result root",
             K(ret), K(full_result_root), K(full_file_path));
  } else {
    const ObString relative(full_file_path.length() - full_result_root.length(),
                            full_file_path.ptr() + full_result_root.length());
    if (OB_FAIL(deep_copy_string(allocator, relative, relative_file_path))) {
      LOG_WARN("failed to copy relative async result file path", K(ret));
    }
  }
  return ret;
}

// Publication happens after every SQC has completed its multipart writer, so
// the published Parquet objects are immutable here.  Read only the Parquet
// footer through the same random-access adapter used by the scan path; do not
// materialize result data merely to build metadata.json.
int load_result_parquet_row_groups(const ObString &access_info,
                                   const ObAsyncResultFileDesc &data_file,
                                   ObResultFileMetadata &result_file)
{
  int ret = OB_SUCCESS;
  arrow::Status status = arrow::Status::OK();
  ObExternalFileAccess file_access;
  ObArrowMemPool arrow_allocator;
  int64_t total_row_count = 0;
  if (!data_file.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result parquet descriptor for row-group metadata",
             KR(ret), K(data_file));
  } else {
    arrow_allocator.init(ObMemAttr(MTL_ID(), "ResultArrow"));
    BEGIN_CATCH_EXCEPTIONS
      std::shared_ptr<ObArrowFile> parquet_file = std::make_shared<ObArrowFile>(
          file_access, data_file.get_path().ptr(), &arrow_allocator);
      std::unique_ptr<parquet::ParquetFileReader> parquet_reader;
      ObExternalFileUrlInfo file_info(data_file.get_path(),
                                      access_info,
                                      data_file.get_path(),
                                      ObString::make_empty_string(),
                                      data_file.byte_count_,
                                      INT64_MAX);
      ObExternalFileCacheOptions cache_options(false /* page cache */, false /* disk cache */);
      parquet::ReaderProperties reader_properties;
      if (OB_FAIL(parquet_file->open(file_info, cache_options))) {
        LOG_WARN("failed to open async result parquet footer", KR(ret), K(data_file));
      } else if (FALSE_IT(parquet_reader = parquet::ParquetFileReader::Open(
                               parquet_file, reader_properties))) {
      } else if (OB_ISNULL(parquet_reader) || OB_ISNULL(parquet_reader->metadata())) {
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_WARN("async result parquet footer is null", KR(ret), K(data_file));
      } else {
        const std::shared_ptr<parquet::FileMetaData> &file_metadata = parquet_reader->metadata();
        if (file_metadata->num_row_groups() <= 0) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("async result parquet has no row groups", KR(ret), K(data_file));
        }
        for (int64_t row_group_idx = 0;
             OB_SUCC(ret) && row_group_idx < file_metadata->num_row_groups();
             ++row_group_idx) {
          const std::shared_ptr<parquet::RowGroupMetaData> row_group =
              file_metadata->RowGroup(static_cast<int>(row_group_idx));
          ObResultFileMetadata::RowGroup result_row_group;
          if (OB_ISNULL(row_group) || row_group->num_rows() <= 0
              || row_group->num_rows() > INT64_MAX - total_row_count) {
            ret = OB_INVALID_EXTERNAL_FILE;
            LOG_WARN("invalid async result parquet row group", KR(ret), K(data_file),
                     K(row_group_idx), KP(row_group.get()), K(total_row_count));
          } else {
            result_row_group.ordinal_ = row_group_idx;
            result_row_group.row_count_ = row_group->num_rows();
            total_row_count += result_row_group.row_count_;
            if (OB_FAIL(result_file.row_groups_.push_back(result_row_group))) {
              LOG_WARN("failed to retain async result parquet row-group metadata",
                       KR(ret), K(data_file), K(row_group_idx));
            }
          }
        }
        if (OB_SUCC(ret) && total_row_count != data_file.row_count_) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_WARN("async result parquet footer row count differs from written descriptor",
                   KR(ret), K(data_file), K(total_row_count));
        }
      }
    END_CATCH_EXCEPTIONS
  }
  return ret;
}

} // namespace

ObResultColumnMetadata::ObResultColumnMetadata()
  : field_id_(0),
    ordinal_(-1),
    name_(),
    nullable_(true),
    ob_type_id_(ObNullType),
    ob_type_name_(),
    sql_type_(),
    precision_(-1),
    scale_(-1),
    length_(-1),
    length_semantics_(-1),
    is_unsigned_(false),
    collation_id_(CS_TYPE_INVALID),
    collation_level_(CS_LEVEL_INVALID),
    charset_name_(),
    collation_name_(),
    subschema_id_(-1),
    extended_type_info_(),
    arrow_type_(),
    parquet_physical_type_(),
    parquet_logical_type_(),
    codec_name_(),
    codec_version_(0)
{
}

int ObResultColumnMetadata::assign(const ObResultColumnMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    field_id_ = other.field_id_;
    ordinal_ = other.ordinal_;
    name_ = other.name_;
    nullable_ = other.nullable_;
    ob_type_id_ = other.ob_type_id_;
    ob_type_name_ = other.ob_type_name_;
    sql_type_ = other.sql_type_;
    precision_ = other.precision_;
    scale_ = other.scale_;
    length_ = other.length_;
    length_semantics_ = other.length_semantics_;
    is_unsigned_ = other.is_unsigned_;
    collation_id_ = other.collation_id_;
    collation_level_ = other.collation_level_;
    charset_name_ = other.charset_name_;
    collation_name_ = other.collation_name_;
    subschema_id_ = other.subschema_id_;
    arrow_type_ = other.arrow_type_;
    parquet_physical_type_ = other.parquet_physical_type_;
    parquet_logical_type_ = other.parquet_logical_type_;
    codec_name_ = other.codec_name_;
    codec_version_ = other.codec_version_;
    if (OB_FAIL(extended_type_info_.assign(other.extended_type_info_))) {
      LOG_WARN("failed to assign result column extended type information", K(ret));
    }
  }
  return ret;
}

int ObResultColumnMetadata::deep_copy(ObIAllocator &allocator,
                                     const ObResultColumnMetadata &other)
{
  int ret = OB_SUCCESS;
  field_id_ = other.field_id_;
  ordinal_ = other.ordinal_;
  nullable_ = other.nullable_;
  ob_type_id_ = other.ob_type_id_;
  precision_ = other.precision_;
  scale_ = other.scale_;
  length_ = other.length_;
  length_semantics_ = other.length_semantics_;
  is_unsigned_ = other.is_unsigned_;
  collation_id_ = other.collation_id_;
  collation_level_ = other.collation_level_;
  subschema_id_ = other.subschema_id_;
  codec_version_ = other.codec_version_;
  OZ(deep_copy_string(allocator, other.name_, name_));
  OZ(deep_copy_string(allocator, other.ob_type_name_, ob_type_name_));
  OZ(deep_copy_string(allocator, other.sql_type_, sql_type_));
  OZ(deep_copy_string(allocator, other.charset_name_, charset_name_));
  OZ(deep_copy_string(allocator, other.collation_name_, collation_name_));
  extended_type_info_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < other.extended_type_info_.count(); ++i) {
    ObString copied;
    OZ(deep_copy_string(allocator, other.extended_type_info_.at(i), copied));
    OZ(extended_type_info_.push_back(copied));
  }
  OZ(deep_copy_string(allocator, other.arrow_type_, arrow_type_));
  OZ(deep_copy_string(allocator, other.parquet_physical_type_, parquet_physical_type_));
  OZ(deep_copy_string(allocator, other.parquet_logical_type_, parquet_logical_type_));
  OZ(deep_copy_string(allocator, other.codec_name_, codec_name_));
  return ret;
}

ObResultFileMetadata::ObResultFileMetadata()
  : path_(), row_count_(0), byte_count_(0),
    writer_sqc_id_(-1), writer_task_id_(-1), file_sequence_(-1),
    row_groups_()
{
}

int ObResultFileMetadata::assign(const ObResultFileMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    path_ = other.path_;
    row_count_ = other.row_count_;
    byte_count_ = other.byte_count_;
    writer_sqc_id_ = other.writer_sqc_id_;
    writer_task_id_ = other.writer_task_id_;
    file_sequence_ = other.file_sequence_;
    if (OB_FAIL(row_groups_.assign(other.row_groups_))) {
      LOG_WARN("failed to assign result file row groups", K(ret));
    }
  }
  return ret;
}

int ObResultFileMetadata::deep_copy(ObIAllocator &allocator,
                                   const ObResultFileMetadata &other)
{
  int ret = OB_SUCCESS;
  row_count_ = other.row_count_;
  byte_count_ = other.byte_count_;
  writer_sqc_id_ = other.writer_sqc_id_;
  writer_task_id_ = other.writer_task_id_;
  file_sequence_ = other.file_sequence_;
  OZ(deep_copy_string(allocator, other.path_, path_));
  row_groups_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < other.row_groups_.count(); ++i) {
    OZ(row_groups_.push_back(other.row_groups_.at(i)));
  }
  return ret;
}

ObResultPartitionMetadata::ObResultPartitionMetadata()
  : partition_id_(-1), row_count_(0), compressed_size_(0), files_()
{
}

int ObResultPartitionMetadata::assign(const ObResultPartitionMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    partition_id_ = other.partition_id_;
    row_count_ = other.row_count_;
    compressed_size_ = other.compressed_size_;
    if (OB_FAIL(files_.assign(other.files_))) {
      LOG_WARN("failed to assign result partition files", K(ret));
    }
  }
  return ret;
}

int ObResultPartitionMetadata::deep_copy(ObIAllocator &allocator,
                                        const ObResultPartitionMetadata &other)
{
  int ret = OB_SUCCESS;
  partition_id_ = other.partition_id_;
  row_count_ = other.row_count_;
  compressed_size_ = other.compressed_size_;
  files_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < other.files_.count(); ++i) {
    ObResultFileMetadata file;
    OZ(file.deep_copy(allocator, other.files_.at(i)));
    OZ(files_.push_back(file));
  }
  return ret;
}

ObResultTableMetadata::ObResultTableMetadata(ObIAllocator &allocator)
  : allocator_(allocator),
    format_version_(CURRENT_FORMAT_VERSION),
    min_reader_version_(1),
    expire_time_us_(0),
    columns_(),
    partitions_(),
    file_count_(0),
    row_count_(0),
    byte_count_(0),
    ordering_mode_(ObString::make_string(RESULT_ORDERING_UNSPECIFIED)),
    ordering_fields_()
{
}

void ObResultTableMetadata::reset()
{
  format_version_ = CURRENT_FORMAT_VERSION;
  min_reader_version_ = 1;
  expire_time_us_ = 0;
  columns_.reset();
  partitions_.reset();
  file_count_ = 0;
  row_count_ = 0;
  byte_count_ = 0;
  ordering_mode_ = ObString::make_string(RESULT_ORDERING_UNSPECIFIED);
  ordering_fields_.reset();
}

int ObResultTableMetadata::assign(const ObResultTableMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    format_version_ = other.format_version_;
    min_reader_version_ = other.min_reader_version_;
    expire_time_us_ = other.expire_time_us_;
    file_count_ = other.file_count_;
    row_count_ = other.row_count_;
    byte_count_ = other.byte_count_;
    if (OB_FAIL(deep_copy_string(allocator_, other.ordering_mode_, ordering_mode_))) {
      LOG_WARN("failed to copy async result ordering metadata", KR(ret));
    } else if (OB_FAIL(ordering_fields_.assign(other.ordering_fields_))) {
      LOG_WARN("failed to copy async result ordering fields", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.columns_.count(); ++i) {
      if (OB_FAIL(add_column(other.columns_.at(i)))) {
        LOG_WARN("failed to copy async result column metadata", KR(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other.partitions_.count(); ++i) {
      if (OB_FAIL(add_partition(other.partitions_.at(i)))) {
        LOG_WARN("failed to copy async result file-manifest entry", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObResultTableMetadata::add_column(const ObResultColumnMetadata &column)
{
  int ret = OB_SUCCESS;
  ObResultColumnMetadata *copied_column = columns_.alloc_place_holder();
  if (OB_ISNULL(copied_column)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate result column metadata", K(ret));
  } else if (OB_FAIL(copied_column->deep_copy(allocator_, column))) {
    columns_.pop_back();
    LOG_WARN("failed to copy result column metadata", K(ret));
  }
  return ret;
}

int ObResultTableMetadata::add_partition(const ObResultPartitionMetadata &partition)
{
  int ret = OB_SUCCESS;
  ObResultPartitionMetadata *copied_partition = partitions_.alloc_place_holder();
  if (OB_ISNULL(copied_partition)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate result partition metadata", K(ret));
  } else if (OB_FAIL(copied_partition->deep_copy(allocator_, partition))) {
    partitions_.pop_back();
    LOG_WARN("failed to copy result partition metadata", K(ret));
  }
  return ret;
}

int ObResultTableMetadata::set_file_ordering(
    const ObIArray<ObResultOrderingFieldMetadata> &ordering_fields)
{
  int ret = OB_SUCCESS;
  ordering_fields_.reset();
  ordering_mode_ = ObString::make_string(RESULT_ORDERING_UNSPECIFIED);
  min_reader_version_ = 1;
  if (!ordering_fields.empty()) {
    if (OB_FAIL(ordering_fields_.assign(ordering_fields))) {
      LOG_WARN("failed to assign async result ordering fields", K(ret));
    } else {
      ordering_mode_ = ObString::make_string(RESULT_ORDERING_SORTED_WITHIN_WRITER);
      min_reader_version_ = CURRENT_READER_VERSION;
    }
  }
  return ret;
}

bool ObResultTableMetadata::has_writer_file_ordering() const
{
  return ordering_mode_ == ObString::make_string(RESULT_ORDERING_SORTED_WITHIN_WRITER);
}

ObResultCatalogTableMetadata::ObResultCatalogTableMetadata(ObIAllocator &allocator)
  : share::ObILakeTableMetadata(allocator),
    job_id_(OB_INVALID_ID),
    result_root_(),
    access_info_(),
    result_table_metadata_(allocator),
    requested_chunk_no_(OB_INVALID_INDEX_INT64)
{
  tenant_id_ = OB_INVALID_TENANT_ID;
}

share::ObLakeTableFormat ObResultCatalogTableMetadata::get_format_type() const
{
  return share::ObLakeTableFormat::RESULT;
}

int64_t ObResultCatalogTableMetadata::get_convert_size() const
{
  return -1;
}

int ObResultCatalogTableMetadata::do_build_table_schema(
    std::optional<int32_t> schema_id,
    std::optional<int64_t> snapshot_id,
    share::schema::ObTableSchema *&table_schema)
{
  UNUSED(schema_id);
  UNUSED(snapshot_id);
  table_schema = nullptr;
  // RESULT_SCAN currently builds its statement-local mock schema before the
  // metadata pointer is attached to TableItem.  This override makes Result
  // Catalog metadata usable through the common lake metadata base without
  // changing that working scan path.
  return OB_NOT_SUPPORTED;
}

int ObResultCatalogTableMetadata::assign(const ObResultCatalogTableMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(ObILakeTableMetadata::assign(other))) {
      LOG_WARN("failed to copy Result Catalog lake metadata", KR(ret));
    }
    job_id_ = other.job_id_;
    requested_chunk_no_ = other.requested_chunk_no_;
    if (OB_SUCC(ret) && OB_FAIL(deep_copy_string(allocator_, other.result_root_, result_root_))) {
      LOG_WARN("failed to copy Result Catalog root", KR(ret));
    } else if (OB_SUCC(ret)
               && OB_FAIL(deep_copy_string(allocator_, other.access_info_, access_info_))) {
      LOG_WARN("failed to copy Result Catalog access information", KR(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(result_table_metadata_.assign(other.result_table_metadata_))) {
      LOG_WARN("failed to copy Result Catalog table metadata", KR(ret));
    }
  }
  return ret;
}

int ObResultTableMetadata::finalize_summary()
{
  int ret = OB_SUCCESS;
  file_count_ = 0;
  row_count_ = 0;
  byte_count_ = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < partitions_.count(); ++i) {
    const ObResultPartitionMetadata &partition = partitions_.at(i);
    if (partition.row_count_ > INT64_MAX - row_count_
        || partition.compressed_size_ > INT64_MAX - byte_count_
        || partition.files_.count() > INT64_MAX - file_count_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("async result metadata summary overflow", K(ret), K(i));
    } else {
      row_count_ += partition.row_count_;
      byte_count_ += partition.compressed_size_;
      file_count_ += partition.files_.count();
    }
  }
  return ret;
}

int ObResultTableMetadata::get_chunk_count(int64_t &chunk_count) const
{
  int ret = OB_SUCCESS;
  chunk_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < partitions_.count(); ++i) {
    const ObResultPartitionMetadata &partition = partitions_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < partition.files_.count(); ++j) {
      const ObResultFileMetadata &file = partition.files_.at(j);
      if (file.row_count_ > 0 && file.row_groups_.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("async result manifest has no row-group map", KR(ret), K(i), K(j), K(file));
      } else if (file.row_groups_.count() > INT64_MAX - chunk_count) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("async result chunk count overflow", KR(ret), K(i), K(j), K(chunk_count));
      } else {
        chunk_count += file.row_groups_.count();
      }
    }
  }
  return ret;
}

int ObResultTableMetadata::get_chunk_location(const int64_t chunk_no,
                                              ObResultChunkLocation &location) const
{
  int ret = OB_SUCCESS;
  int64_t next_chunk_no = 0;
  location = ObResultChunkLocation();
  if (chunk_no < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("negative async result chunk id", KR(ret), K(chunk_no));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partitions_.count(); ++i) {
    const ObResultPartitionMetadata &partition = partitions_.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < partition.files_.count(); ++j) {
      const ObResultFileMetadata &file = partition.files_.at(j);
      if (file.row_count_ > 0 && file.row_groups_.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("async result manifest has no row-group map", KR(ret), K(i), K(j), K(file));
      }
      for (int64_t k = 0; OB_SUCC(ret) && k < file.row_groups_.count(); ++k) {
        if (next_chunk_no == chunk_no) {
          const ObResultFileMetadata::RowGroup &row_group = file.row_groups_.at(k);
          location.partition_idx_ = i;
          location.file_idx_ = j;
          location.row_group_idx_ = row_group.ordinal_;
          location.row_count_ = row_group.row_count_;
          return OB_SUCCESS;
        } else if (next_chunk_no == INT64_MAX) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("async result chunk id overflow", KR(ret), K(i), K(j), K(k));
        } else {
          ++next_chunk_no;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("async result chunk id is out of range", KR(ret), K(chunk_no), K(next_chunk_no));
  }
  return ret;
}

int ObResultTableMetadata::validate() const
{
  int ret = OB_SUCCESS;
  int64_t file_count = 0;
  int64_t row_count = 0;
  int64_t byte_count = 0;
  const bool ordering_unspecified =
      ordering_mode_ == ObString::make_string(RESULT_ORDERING_UNSPECIFIED);
  const bool sorted_within_file =
      ordering_mode_ == ObString::make_string(RESULT_ORDERING_SORTED_WITHIN_FILE);
  const bool sorted_within_writer = has_writer_file_ordering();
  if (CURRENT_FORMAT_VERSION != format_version_
      || min_reader_version_ <= 0
      || min_reader_version_ > CURRENT_READER_VERSION
      || expire_time_us_ < 0
      || (!ordering_unspecified && !sorted_within_file && !sorted_within_writer)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async result metadata version or ordering",
             K(ret), K(format_version_), K(min_reader_version_), K(ordering_mode_));
  } else if ((ordering_unspecified && !ordering_fields_.empty())
             || ((sorted_within_file || sorted_within_writer) && ordering_fields_.empty())
             || (sorted_within_writer && min_reader_version_ < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inconsistent async result ordering mode and fields",
             K(ret), K(ordering_mode_), K(ordering_fields_.count()),
             K(min_reader_version_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
    const ObResultColumnMetadata &column = columns_.at(i);
    if (column.field_id_ <= 0
        || column.field_id_ > INT32_MAX
        || column.ordinal_ != i
        || column.ob_type_id_ < ObNullType
        || column.ob_type_id_ >= ObMaxType
        || column.name_.empty()
        || column.ob_type_name_.empty()
        || column.sql_type_.empty()
        || column.arrow_type_.empty()
        || column.parquet_physical_type_.empty()
        || column.codec_name_.empty()
        || column.codec_version_ <= 0
        || (ObCollectionSQLType == column.ob_type_id_
            && 1 != column.extended_type_info_.count())
        || (ObSetType == column.ob_type_id_
            && column.extended_type_info_.empty())
        || (ObCollectionSQLType != column.ob_type_id_
            && ObSetType != column.ob_type_id_
            && !column.extended_type_info_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result column metadata",
               K(ret),
               K(i),
               K(column.field_id_),
               K(column.ordinal_),
               K(column.ob_type_id_),
               K(column.codec_version_),
               "name_length",
               column.name_.length(),
               "ob_type_name_length",
               column.ob_type_name_.length(),
               "sql_type_length",
               column.sql_type_.length(),
               "arrow_type_length",
               column.arrow_type_.length(),
               "parquet_physical_type_length",
               column.parquet_physical_type_.length(),
               "codec_name_length",
               column.codec_name_.length(),
               "extended_type_info_count",
               column.extended_type_info_.count());
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
      if (columns_.at(j).field_id_ == column.field_id_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("duplicate async result field id", K(ret), K(i), K(j), K(column.field_id_));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ordering_fields_.count(); ++i) {
    const ObResultOrderingFieldMetadata &field = ordering_fields_.at(i);
    bool found_column = false;
    for (int64_t j = 0; !found_column && j < columns_.count(); ++j) {
      found_column = field.field_id_ == columns_.at(j).field_id_;
    }
    if (!found_column) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("async result ordering field is absent from schema", K(ret), K(i), K(field));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
      if (ordering_fields_.at(j).field_id_ == field.field_id_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("duplicate async result ordering field", K(ret), K(i), K(j), K(field));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partitions_.count(); ++i) {
    const ObResultPartitionMetadata &partition = partitions_.at(i);
    int64_t partition_rows = 0;
    int64_t partition_bytes = 0;
    if (partition.partition_id_ != i || partition.files_.empty()
        || partition.row_count_ < 0 || partition.compressed_size_ < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result partition metadata", K(ret), K(i));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < partition.files_.count(); ++j) {
      const ObResultFileMetadata &file = partition.files_.at(j);
      int64_t row_group_rows = 0;
      const bool has_writer_identity =
          file.writer_sqc_id_ >= 0 && file.writer_task_id_ >= 0
          && file.file_sequence_ >= 0;
      const bool legacy_file =
          file.writer_sqc_id_ == -1 && file.writer_task_id_ == -1
          && file.file_sequence_ == -1;
      if (file.path_.empty()
          || file.path_[0] == '/'
          || OB_NOT_NULL(file.path_.reverse_find('?'))
          || share::ObExternalTableUtils::is_abs_url(file.path_)
          || share::ObExternalTableUtils::is_sub_path_contain_parent_dir(file.path_)
          || file.row_count_ < 0
          || file.byte_count_ < 0
          || file.row_count_ > INT64_MAX - partition_rows
          || file.byte_count_ > INT64_MAX - partition_bytes) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid async result file metadata", K(ret), K(i), K(j), K(file.path_));
      } else {
        partition_rows += file.row_count_;
        partition_bytes += file.byte_count_;
      }
      if (OB_SUCC(ret) && (!has_writer_identity && !legacy_file)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("incomplete async result writer identity", K(ret), K(i), K(j), K(file));
      } else if (OB_SUCC(ret) && sorted_within_writer
                 && (!has_writer_identity || file.file_sequence_ != j
                     || (j > 0
                         && (file.writer_sqc_id_ != partition.files_.at(0).writer_sqc_id_
                             || file.writer_task_id_ != partition.files_.at(0).writer_task_id_)))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ordered async result writer stream", K(ret), K(i), K(j), K(file));
      }
      for (int64_t k = 0; OB_SUCC(ret) && k < file.row_groups_.count(); ++k) {
        const ObResultFileMetadata::RowGroup &row_group = file.row_groups_.at(k);
        if (row_group.ordinal_ != k
            || row_group.row_count_ <= 0
            || row_group.row_count_ > INT64_MAX - row_group_rows) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid async result row-group metadata", KR(ret), K(i), K(j), K(k),
                   K(row_group));
        } else {
          row_group_rows += row_group.row_count_;
        }
      }
      if (OB_SUCC(ret) && !file.row_groups_.empty() && row_group_rows != file.row_count_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("inconsistent async result row-group totals", KR(ret), K(i), K(j),
                 K(row_group_rows), K(file.row_count_));
      }
      for (int64_t pi = 0; OB_SUCC(ret) && pi <= i; ++pi) {
        const ObResultPartitionMetadata &previous_partition = partitions_.at(pi);
        const int64_t file_limit = pi == i ? j : previous_partition.files_.count();
        for (int64_t fi = 0; OB_SUCC(ret) && fi < file_limit; ++fi) {
          if (previous_partition.files_.at(fi).path_ == file.path_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicate async result file path", K(ret), K(file.path_));
          }
        }
      }
    }
    if (OB_SUCC(ret) && sorted_within_writer && i > 0) {
      const ObResultFileMetadata &writer = partition.files_.at(0);
      const ObResultFileMetadata &previous_writer = partitions_.at(i - 1).files_.at(0);
      if (writer.writer_sqc_id_ < previous_writer.writer_sqc_id_
          || (writer.writer_sqc_id_ == previous_writer.writer_sqc_id_
              && writer.writer_task_id_ <= previous_writer.writer_task_id_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ordered async result writers are duplicated or out of order",
                 K(ret), K(i), K(writer), K(previous_writer));
      }
    }
    if (OB_SUCC(ret)
        && (partition_rows != partition.row_count_
            || partition_bytes != partition.compressed_size_
            || partition.row_count_ > INT64_MAX - row_count
            || partition.compressed_size_ > INT64_MAX - byte_count
            || partition.files_.count() > INT64_MAX - file_count)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("inconsistent async result partition totals",
               K(ret), K(i), K(partition_rows), K(partition_bytes),
               K(partition.row_count_), K(partition.compressed_size_));
    } else if (OB_SUCC(ret)) {
      row_count += partition.row_count_;
      byte_count += partition.compressed_size_;
      file_count += partition.files_.count();
    }
  }
  if (OB_SUCC(ret)
      && (file_count != file_count_
          || row_count != row_count_
          || byte_count != byte_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inconsistent async result metadata summary",
             K(ret), K(file_count), K(row_count), K(byte_count),
             K(file_count_), K(row_count_), K(byte_count_));
  }
  return ret;
}

int ObResultTableMetadata::to_json(ObIAllocator &allocator, ObString &json) const
{
  int ret = OB_SUCCESS;
  ObJsonObject *root = NULL;
  ObJsonObject *schema = NULL;
  ObJsonArray *columns = NULL;
  ObJsonArray *partitions = NULL;
  ObJsonObject *summary = NULL;
  ObJsonObject *ordering = NULL;
  ObJsonArray *ordering_fields = NULL;
  if (OB_FAIL(validate())) {
    LOG_WARN("invalid async result metadata before serialization", K(ret));
  } else if (OB_FAIL(alloc_json_object(allocator, root))) {
  } else if (OB_FAIL(add_json_string(allocator, *root, "format",
                                      ObString::make_string(RESULT_METADATA_FORMAT)))) {
  } else if (OB_FAIL(add_json_int(allocator, *root, "format_version", format_version_))) {
  } else if (OB_FAIL(add_json_int(allocator, *root,
                                  "min_reader_version", min_reader_version_))) {
  } else if (OB_FAIL(add_json_string(allocator, *root, "data_format",
                                      ObString::make_string(RESULT_DATA_FORMAT)))) {
  } else if (OB_FAIL(add_json_string(allocator, *root, "path_mode",
                                      ObString::make_string(RESULT_PATH_MODE)))) {
  } else if (OB_FAIL(add_json_int(allocator, *root, "expire_time_us", expire_time_us_))) {
  } else if (OB_FAIL(alloc_json_object(allocator, schema))) {
  } else if (OB_FAIL(add_json_int(allocator, *schema, "column_count", columns_.count()))) {
  } else if (OB_FAIL(alloc_json_array(allocator, columns))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
    OZ(add_column_json(allocator, columns_.at(i), *columns));
  }
  if (OB_SUCC(ret) && OB_FAIL(schema->object_add(ObString::make_string("columns"), columns))) {
    LOG_WARN("failed to add async result columns", K(ret));
  } else if (OB_SUCC(ret)
             && OB_FAIL(root->object_add(ObString::make_string("schema"), schema))) {
    LOG_WARN("failed to add async result schema", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(alloc_json_array(allocator, partitions))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partitions_.count(); ++i) {
    OZ(add_partition_json(allocator, partitions_.at(i), *partitions));
  }
  if (OB_SUCC(ret)
      && OB_FAIL(root->object_add(ObString::make_string("partitions"), partitions))) {
    LOG_WARN("failed to add async result partitions", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(alloc_json_object(allocator, summary))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(add_json_int(allocator, *summary,
                                     "partition_count", partitions_.count()))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(add_json_int(allocator, *summary, "file_count", file_count_))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(add_json_int(allocator, *summary, "row_count", row_count_))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(add_json_int(allocator, *summary, "byte_count", byte_count_))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(root->object_add(ObString::make_string("summary"), summary))) {
    LOG_WARN("failed to add async result summary", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(alloc_json_object(allocator, ordering))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(add_json_string(allocator, *ordering, "mode", ordering_mode_))) {
  }
  if (OB_SUCC(ret) && !ordering_fields_.empty()) {
    if (OB_FAIL(alloc_json_array(allocator, ordering_fields))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ordering_fields_.count(); ++i) {
      OZ(add_ordering_field_json(allocator, ordering_fields_.at(i), *ordering_fields));
    }
    if (OB_SUCC(ret)
        && OB_FAIL(ordering->object_add(ObString::make_string("keys"), ordering_fields))) {
      LOG_WARN("failed to add async result ordering keys", K(ret));
    }
  }
  if (OB_SUCC(ret)
      && OB_FAIL(root->object_add(ObString::make_string("ordering"), ordering))) {
    LOG_WARN("failed to add async result ordering", K(ret));
  } else if (OB_SUCC(ret)) {
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(root->print(buffer, false))) {
      LOG_WARN("failed to print async result metadata json", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, buffer.string(), json, true))) {
      LOG_WARN("failed to save async result metadata json", K(ret));
    }
  }
  return ret;
}

int ObResultTableMetadata::from_json(const ObString &json)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator parser_allocator("ResultMeta");
  ObJsonNode *root = NULL;
  reset();
  if (json.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty async result metadata json", K(ret));
  } else if (OB_FAIL(ObJsonParser::get_tree(&parser_allocator, json, root))) {
    LOG_WARN("failed to parse async result metadata json", K(ret));
  } else if (OB_ISNULL(root) || ObJsonNodeType::J_OBJECT != root->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async result metadata root is not an object", K(ret));
  } else if (OB_FAIL(init_from_json_object(*down_cast<const ObJsonObject *>(root)))) {
    LOG_WARN("failed to initialize async result metadata from json", K(ret));
  } else if (OB_FAIL(validate())) {
    LOG_WARN("invalid async result metadata json", K(ret));
  }
  return ret;
}

int ObResultTableMetadata::init_from_json_object(const ObJsonObject &root)
{
  int ret = OB_SUCCESS;
  ObString format;
  ObString data_format;
  ObString path_mode;
  const ObJsonObject *schema = NULL;
  const ObJsonArray *columns = NULL;
  const ObJsonArray *partitions = NULL;
  const ObJsonObject *summary = NULL;
  const ObJsonObject *ordering = NULL;
  const ObJsonArray *ordering_fields = NULL;
  int64_t declared_column_count = 0;
  int64_t declared_partition_count = 0;
  if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
          allocator_, root, ObString::make_string("format"), format))) {
  } else if (format != ObString::make_string(RESULT_METADATA_FORMAT)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async result metadata format", K(ret), K(format));
  } else if (OB_FAIL(get_required_primitive(root, "format_version", format_version_))) {
  } else if (OB_FAIL(get_required_primitive(root,
                                             "min_reader_version",
                                             min_reader_version_))) {
  } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                         allocator_, root, ObString::make_string("data_format"),
                         data_format))) {
  } else if (data_format != ObString::make_string(RESULT_DATA_FORMAT)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async result data format", K(ret), K(data_format));
  } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                         allocator_, root, ObString::make_string("path_mode"),
                         path_mode))) {
  } else if (path_mode != ObString::make_string(RESULT_PATH_MODE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported async result path mode", K(ret), K(path_mode));
  } else if (OB_FAIL(get_optional_int(root, "expire_time_us", expire_time_us_, 0))) {
  } else if (OB_FAIL(get_required_object(root, "schema", schema))) {
  } else if (OB_FAIL(get_required_primitive(*schema,
                                             "column_count",
                                             declared_column_count))) {
  } else if (OB_FAIL(get_required_array(*schema, "columns", columns))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(columns->element_count()); ++i) {
    const ObJsonNode *column_node = columns->get_value(i);
    const ObJsonObject *column_json = NULL;
    const ObJsonObject *ob_type_info = NULL;
    const ObJsonObject *physical = NULL;
    const ObJsonObject *codec = NULL;
    ObResultColumnMetadata column;
    if (OB_ISNULL(column_node) || ObJsonNodeType::J_OBJECT != column_node->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result metadata column json", K(ret), K(i));
    } else if (FALSE_IT(column_json = down_cast<const ObJsonObject *>(column_node))) {
    } else if (OB_FAIL(get_required_primitive(*column_json, "field_id", column.field_id_))) {
    } else if (OB_FAIL(get_required_primitive(*column_json, "ordinal", column.ordinal_))) {
    } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                           allocator_, *column_json, ObString::make_string("name"), column.name_))) {
    } else if (OB_FAIL(get_required_primitive(*column_json, "nullable", column.nullable_))) {
    } else if (OB_FAIL(get_required_object(*column_json, "ob_type_info", ob_type_info))) {
    } else if (OB_FAIL(get_required_primitive(*ob_type_info, "type_id", column.ob_type_id_))) {
    } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                           allocator_, *ob_type_info, ObString::make_string("type_name"),
                           column.ob_type_name_))) {
    } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                           allocator_, *ob_type_info, ObString::make_string("sql_type"),
                           column.sql_type_))) {
    } else if (OB_FAIL(get_optional_int(*ob_type_info, "precision", column.precision_, -1))) {
    } else if (OB_FAIL(get_optional_int(*ob_type_info, "scale", column.scale_, -1))) {
    } else if (OB_FAIL(get_optional_int(*ob_type_info, "length", column.length_, -1))) {
    } else if (OB_FAIL(get_optional_int(*ob_type_info, "length_semantics",
                                        column.length_semantics_, -1))) {
    } else if (OB_FAIL(get_required_primitive(*ob_type_info,
                                               "unsigned",
                                               column.is_unsigned_))) {
    } else if (OB_FAIL(get_required_primitive(*ob_type_info,
                                               "collation_id",
                                               column.collation_id_))) {
    } else if (OB_FAIL(get_required_primitive(*ob_type_info,
                                               "collation_level",
                                               column.collation_level_))) {
    } else if (OB_FAIL(get_optional_string(allocator_, *ob_type_info,
                                           "charset", column.charset_name_))) {
    } else if (OB_FAIL(get_optional_string(allocator_, *ob_type_info,
                                           "collation", column.collation_name_))) {
    } else if (OB_FAIL(get_optional_int(*ob_type_info, "subschema_id",
                                        column.subschema_id_, -1))) {
    } else if (OB_FAIL(get_optional_string_array(allocator_, *ob_type_info,
                                                 "extended_type_info",
                                                 column.extended_type_info_))) {
    } else if (OB_FAIL(get_required_object(*column_json, "physical", physical))) {
    } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                           allocator_, *physical, ObString::make_string("arrow_type"),
                           column.arrow_type_))) {
    } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                           allocator_, *physical,
                           ObString::make_string("parquet_physical_type"),
                           column.parquet_physical_type_))) {
    } else if (OB_FAIL(get_optional_string(allocator_, *physical,
                                           "parquet_logical_type",
                                           column.parquet_logical_type_))) {
    } else if (OB_FAIL(get_required_object(*column_json, "codec", codec))) {
    } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                           allocator_, *codec, ObString::make_string("name"),
                           column.codec_name_))) {
    } else if (OB_FAIL(get_required_primitive(*codec, "version", column.codec_version_))) {
    } else if (OB_FAIL(columns_.push_back(column))) {
      LOG_WARN("failed to append parsed async result column", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && declared_column_count != columns_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inconsistent async result column count",
             K(ret), K(declared_column_count), K(columns_.count()));
  } else if (OB_SUCC(ret) && OB_FAIL(get_required_array(root, "partitions", partitions))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(partitions->element_count()); ++i) {
    const ObJsonNode *partition_node = partitions->get_value(i);
    const ObJsonObject *partition_json = NULL;
    const ObJsonArray *files = NULL;
    ObResultPartitionMetadata partition;
    if (OB_ISNULL(partition_node) || ObJsonNodeType::J_OBJECT != partition_node->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result metadata partition json", K(ret), K(i));
    } else if (FALSE_IT(partition_json = down_cast<const ObJsonObject *>(partition_node))) {
    } else if (OB_FAIL(get_required_primitive(*partition_json,
                                               "partition_id",
                                               partition.partition_id_))) {
    } else if (OB_FAIL(get_required_primitive(*partition_json,
                                               "row_count",
                                               partition.row_count_))) {
    } else if (OB_FAIL(get_required_primitive(*partition_json,
                                               "compressed_size",
                                               partition.compressed_size_))) {
    } else if (OB_FAIL(get_required_array(*partition_json, "files", files))) {
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < static_cast<int64_t>(files->element_count()); ++j) {
      const ObJsonNode *file_node = files->get_value(j);
      const ObJsonObject *file_json = NULL;
      const ObJsonArray *row_groups = NULL;
      ObResultFileMetadata file;
      if (OB_ISNULL(file_node) || ObJsonNodeType::J_OBJECT != file_node->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid async result metadata file json", K(ret), K(i), K(j));
      } else if (FALSE_IT(file_json = down_cast<const ObJsonObject *>(file_node))) {
      } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                             allocator_, *file_json, ObString::make_string("path"), file.path_))) {
      } else if (OB_FAIL(get_required_primitive(*file_json,
                                                 "row_count",
                                                 file.row_count_))) {
      } else if (OB_FAIL(get_required_primitive(*file_json,
                                                 "byte_count",
                                                 file.byte_count_))) {
      } else if (OB_FAIL(get_optional_int(*file_json,
                                          "writer_sqc_id",
                                          file.writer_sqc_id_, -1))) {
      } else if (OB_FAIL(get_optional_int(*file_json,
                                          "writer_task_id",
                                          file.writer_task_id_, -1))) {
      } else if (OB_FAIL(get_optional_int(*file_json,
                                          "file_sequence",
                                          file.file_sequence_, -1))) {
      } else if (OB_NOT_NULL(file_json->get_value(ObString::make_string("row_groups")))
                 && OB_FAIL(get_required_array(*file_json, "row_groups", row_groups))) {
      }
      for (int64_t k = 0; OB_SUCC(ret) && OB_NOT_NULL(row_groups)
                              && k < static_cast<int64_t>(row_groups->element_count()); ++k) {
        const ObJsonNode *row_group_node = row_groups->get_value(k);
        const ObJsonObject *row_group_json = NULL;
        ObResultFileMetadata::RowGroup row_group;
        if (OB_ISNULL(row_group_node)
            || ObJsonNodeType::J_OBJECT != row_group_node->json_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid async result row-group json", KR(ret), K(i), K(j), K(k));
        } else if (FALSE_IT(row_group_json = down_cast<const ObJsonObject *>(row_group_node))) {
        } else if (OB_FAIL(get_required_primitive(*row_group_json,
                                                   "ordinal",
                                                   row_group.ordinal_))) {
        } else if (OB_FAIL(get_required_primitive(*row_group_json,
                                                   "row_count",
                                                   row_group.row_count_))) {
        } else if (OB_FAIL(file.row_groups_.push_back(row_group))) {
          LOG_WARN("failed to append parsed async result row group", KR(ret), K(i), K(j), K(k));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition.files_.push_back(file))) {
          LOG_WARN("failed to append parsed async result file", K(ret), K(i), K(j));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(partitions_.push_back(partition))) {
      LOG_WARN("failed to append parsed async result partition", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(get_required_object(root, "summary", summary))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(get_required_primitive(*summary,
                                                "partition_count",
                                                declared_partition_count))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(get_required_primitive(*summary, "file_count", file_count_))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(get_required_primitive(*summary, "row_count", row_count_))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(get_required_primitive(*summary, "byte_count", byte_count_))) {
  } else if (OB_SUCC(ret) && declared_partition_count != partitions_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inconsistent async result partition count",
             K(ret), K(declared_partition_count), K(partitions_.count()));
  } else if (OB_SUCC(ret) && OB_FAIL(get_required_object(root, "ordering", ordering))) {
  } else if (OB_SUCC(ret)
             && OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                 allocator_, *ordering, ObString::make_string("mode"), ordering_mode_))) {
  }
  if (OB_SUCC(ret)
      && OB_NOT_NULL(ordering->get_value(ObString::make_string("keys")))
      && OB_FAIL(get_required_array(*ordering, "keys", ordering_fields))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(ordering_fields)
                          && i < static_cast<int64_t>(ordering_fields->element_count()); ++i) {
    const ObJsonNode *field_node = ordering_fields->get_value(i);
    const ObJsonObject *field_json = NULL;
    ObString direction;
    ObString null_order;
    ObResultOrderingFieldMetadata field;
    if (OB_ISNULL(field_node) || ObJsonNodeType::J_OBJECT != field_node->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result ordering field json", K(ret), K(i));
    } else if (FALSE_IT(field_json = down_cast<const ObJsonObject *>(field_node))) {
    } else if (OB_FAIL(get_required_primitive(*field_json, "field_id", field.field_id_))) {
    } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                           allocator_, *field_json, ObString::make_string("direction"),
                           direction))) {
    } else if (OB_FAIL(iceberg::ObCatalogJsonUtils::get_string(
                           allocator_, *field_json, ObString::make_string("null_order"),
                           null_order))) {
    } else if (direction != ObString::make_string(RESULT_ORDER_ASC)
               && direction != ObString::make_string(RESULT_ORDER_DESC)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result ordering direction", K(ret), K(i), K(direction));
    } else if (null_order != ObString::make_string(RESULT_NULL_ORDER_FIRST)
               && null_order != ObString::make_string(RESULT_NULL_ORDER_LAST)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result null ordering", K(ret), K(i), K(null_order));
    } else {
      field.ascending_ = direction == ObString::make_string(RESULT_ORDER_ASC);
      field.nulls_first_ = null_order == ObString::make_string(RESULT_NULL_ORDER_FIRST);
      if (OB_FAIL(ordering_fields_.push_back(field))) {
        LOG_WARN("failed to append parsed async result ordering field", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObAsyncResultStorageUtils::build_result_data_relative_path(
    const uint64_t tenant_id,
    const uint64_t job_id,
    ObSqlString &path)
{
  return build_result_job_path(
      tenant_id, job_id, RESULT_DATA_FILE_PREFIX, path);
}

int ObAsyncResultStorageUtils::build_result_metadata_relative_path(
    const uint64_t tenant_id,
    const uint64_t job_id,
    ObSqlString &path)
{
  return build_result_job_path(
      tenant_id, job_id, RESULT_METADATA_FILE_NAME, path);
}

int ObAsyncResultStorageUtils::build_result_job_relative_prefix(
    const uint64_t tenant_id,
    const uint64_t job_id,
    ObSqlString &path)
{
  return build_result_job_path(tenant_id, job_id, "", path);
}

int ObAsyncResultStorageUtils::resolve_internal_path(ObIAllocator &allocator,
                                                    const ObString &relative_path,
                                                    share::ObBackupDest &storage_dest,
                                                    ObString &full_path)
{
  int ret = OB_SUCCESS;
  share::ObDeviceConfig data_device_config;
  ObSqlString path;
  ObSqlString rooted_relative_path;
  if (relative_path.empty()
      || relative_path[0] == '/'
      || OB_NOT_NULL(relative_path.reverse_find('?'))
      || share::ObExternalTableUtils::is_abs_url(relative_path)
      || share::ObExternalTableUtils::is_sub_path_contain_parent_dir(relative_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid internal async result relative path", K(ret), K(relative_path));
  } else {
    if (!relative_path.prefix_match(RESULT_RELATIVE_ROOT)
        && OB_FAIL(rooted_relative_path.append(RESULT_RELATIVE_ROOT))) {
      LOG_WARN("failed to append async result storage root", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(rooted_relative_path.append(relative_path))) {
      LOG_WARN("failed to append async result relative path", K(ret), K(relative_path));
    }
  }
  if (OB_SUCC(ret)) {
    const int device_ret = share::ObDeviceConfigMgr::get_instance().get_device_config(
        share::ObStorageUsedType::TYPE::USED_TYPE_DATA, data_device_config);
    if (OB_SUCCESS == device_ret) {
      if (OB_FAIL(storage_dest.set(data_device_config.path_,
                                   data_device_config.endpoint_,
                                   data_device_config.access_info_,
                                   data_device_config.extension_))) {
        LOG_WARN("failed to build async result storage destination", K(ret));
      }
    } else if (OB_NOT_INIT == device_ret || OB_ENTRY_NOT_EXIST == device_ret) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("external async result store requires shared storage mode",
               KR(ret), K(device_ret));
    } else {
      ret = device_ret;
      LOG_WARN("failed to get shared storage data device config", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(storage_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result storage destination has no storage info", K(ret));
  } else if (OB_SUCC(ret) && OB_STORAGE_S3 != storage_dest.get_storage_type()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async result internal uri requires shared S3 storage",
             K(ret), "storage_type", storage_dest.get_storage_type());
  } else if (OB_SUCC(ret)
             && OB_FAIL(share::ObExternalTableUtils::concat_external_file_location(
                 storage_dest.get_root_path(), rooted_relative_path.string(), path))) {
    LOG_WARN("failed to build async result shared storage path",
             K(ret), K(relative_path), K(rooted_relative_path));
  } else if (OB_SUCC(ret)
             && OB_FAIL(ob_write_string(allocator, path.string(), full_path, true))) {
    LOG_WARN("failed to save async result shared storage path", K(ret));
  }
  return ret;
}

int ObAsyncResultStorageUtils::get_result_retention_us(const uint64_t tenant_id,
                                                       int64_t &retention_us)
{
  int ret = OB_SUCCESS;
  retention_us = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("async result tenant config is unavailable", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(
                 (retention_us = tenant_config->async_result_retention_time.get_value()) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async result retention time", KR(ret), K(tenant_id), K(retention_us));
  }
  return ret;
}

int ObAsyncResultStorageUtils::ensure_result_root_marker()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ResultRootMark");
  share::ObBackupDest storage_dest;
  ObSqlString marker_relative_path;
  ObString marker_path;
  ObBackupIoAdapter io_adapter;
  bool marker_exists = false;
  static const char marker_contents[] = "OceanBase async result root\n";
  if (OB_FAIL(marker_relative_path.append(RESULT_RELATIVE_ROOT))) {
    LOG_WARN("failed to build async result root marker path", KR(ret));
  } else if (OB_FAIL(marker_relative_path.append(RESULT_ROOT_MARKER_FILE_NAME))) {
    LOG_WARN("failed to append async result root marker filename", KR(ret));
  } else if (OB_FAIL(resolve_internal_path(allocator,
                                           marker_relative_path.string(),
                                           storage_dest,
                                           marker_path))) {
    if (OB_NOT_SUPPORTED == ret) {
      LOG_WARN("skip async result root marker without shared storage", KR(ret));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to resolve async result root marker path", KR(ret));
    }
  } else if (OB_ISNULL(storage_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result root marker has no storage information", KR(ret));
  } else if (OB_FAIL(io_adapter.is_exist(
                 marker_path, storage_dest.get_storage_info(), marker_exists))) {
    LOG_WARN("failed to check async result root marker", KR(ret), K(marker_path));
  } else if (!marker_exists
             && OB_FAIL(io_adapter.write_single_file(
                 marker_path,
                 storage_dest.get_storage_info(),
                 marker_contents,
                 STRLEN(marker_contents),
                 ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN("failed to create async result root marker", KR(ret), K(marker_path));
  } else if (marker_exists) {
    LOG_TRACE("async result root marker already exists", K(marker_path));
  } else {
    LOG_DEBUG("created async result root marker", K(marker_path));
  }
  return ret;
}

int ObAsyncResultStorageUtils::cleanup_result_job_prefix(const uint64_t tenant_id,
                                                          const uint64_t job_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ResultCleanup");
  share::ObBackupDest storage_dest;
  ObSqlString job_relative_path;
  ObString job_prefix;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == job_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async result cleanup identity", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(build_result_job_relative_prefix(
                 tenant_id, job_id, job_relative_path))) {
    LOG_WARN("failed to build async result job cleanup path", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(resolve_internal_path(allocator,
                                           job_relative_path.string(),
                                           storage_dest,
                                           job_prefix))) {
    if (OB_NOT_SUPPORTED == ret) {
      LOG_WARN("skip external async result cleanup without shared storage",
               KR(ret), K(tenant_id), K(job_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to resolve async result job cleanup path",
               KR(ret), K(tenant_id), K(job_id));
    }
  } else if (OB_ISNULL(storage_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result cleanup has no storage information", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(ObBackupIoAdapter::del_dir(
                 job_prefix, storage_dest.get_storage_info(), true /* recursive */))) {
    if (OB_FILE_NOT_EXIST == ret || OB_DIR_NOT_EXIST == ret
        || OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("async result job prefix was already absent", K(job_prefix));
    } else {
      LOG_WARN("failed to clean async result job prefix", KR(ret), K(job_prefix));
    }
  } else {
    LOG_DEBUG("cleaned async result job prefix", K(job_prefix));
  }
  return ret;
}

int ObAsyncResultMetadataPublisher::publish(
    ObExecContext &exec_ctx,
    const ObSelectIntoSpec &spec,
    const ObIArray<ObAsyncResultFileDesc> &data_files)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator metadata_allocator("ResultMeta");
  ObResultTableMetadata metadata(metadata_allocator);
  share::ObBackupDest storage_dest;
  ObString full_metadata_path;
  ObString full_result_root;
  ObString json;
  char access_info_buf[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
  ObString access_info;
  int64_t chunk_count = 0;
  int64_t retention_us = 0;
  const ObSQLSessionInfo *session = exec_ctx.get_my_session();
  if (!spec.async_result_target_.enabled_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot publish metadata for a non-async-result sink", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result publication session is null", KR(ret));
  } else if (OB_FAIL(ObAsyncResultStorageUtils::get_result_retention_us(
                 spec.async_result_target_.tenant_id_, retention_us))) {
    LOG_WARN("failed to get async result retention time", KR(ret),
             K(spec.async_result_target_.tenant_id_));
  } else {
    const int64_t publish_time_us = ObTimeUtility::current_time();
    if (OB_UNLIKELY(publish_time_us <= 0 || retention_us > INT64_MAX - publish_time_us)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("invalid async result expiration", KR(ret), K(publish_time_us), K(retention_us));
    } else {
      metadata.set_expire_time_us(publish_time_us + retention_us);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(resolve_result_paths(spec,
                                          metadata_allocator,
                                          storage_dest,
                                          full_result_root,
                                          full_metadata_path))) {
    LOG_WARN("failed to resolve async result publication paths", K(ret));
  } else if (OB_ISNULL(storage_dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result publication storage destination has no access information", KR(ret));
  } else if (OB_FAIL(storage_dest.get_storage_info()->get_storage_info_str(
                 access_info_buf, sizeof(access_info_buf)))) {
    LOG_WARN("failed to serialize async result publication storage access information", KR(ret));
  } else {
    access_info = ObString::make_string(access_info_buf);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < spec.select_exprs_.count(); ++i) {
    ObResultColumnMetadata column;
    OZ(build_column_metadata(metadata_allocator, exec_ctx, spec, i, column));
    OZ(metadata.add_column(column));
  }
  ObSEArray<ObResultOrderingFieldMetadata, 4> ordering_fields;
  ordering_fields.set_attr(ObMemAttr(MTL_ID(), "ResultMeta"));
  if (OB_SUCC(ret)
      && OB_UNLIKELY(spec.async_result_target_.input_ordering_column_idxs_.count()
                     != spec.async_result_target_.input_ordering_directions_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mismatched async result ordering in physical spec", K(ret),
             "column_count", spec.async_result_target_.input_ordering_column_idxs_.count(),
             "direction_count", spec.async_result_target_.input_ordering_directions_.count());
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && i < spec.async_result_target_.input_ordering_column_idxs_.count();
       ++i) {
    const int64_t column_idx = spec.async_result_target_.input_ordering_column_idxs_.at(i);
    const int64_t direction_value = spec.async_result_target_.input_ordering_directions_.at(i);
    if (OB_UNLIKELY(column_idx < 0 || column_idx >= spec.select_exprs_.count()
                    || direction_value < NULLS_FIRST_ASC || direction_value >= UNORDERED)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async result ordering in physical spec", K(ret), K(i),
               K(column_idx), K(direction_value),
               "column_count", spec.select_exprs_.count());
    } else {
      const ObOrderDirection direction = static_cast<ObOrderDirection>(direction_value);
      ObResultOrderingFieldMetadata field;
      field.field_id_ = spec.field_ids_.count() == spec.select_exprs_.count()
                        ? static_cast<int64_t>(spec.field_ids_.at(column_idx))
                        : column_idx + 1;
      field.ascending_ = is_ascending_direction(direction);
      field.nulls_first_ = is_null_first(direction);
      if (OB_FAIL(ordering_fields.push_back(field))) {
        LOG_WARN("failed to build async result ordering field", K(ret), K(i), K(field));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(metadata.set_file_ordering(ordering_fields))) {
    LOG_WARN("failed to set async result file ordering", K(ret));
  }
  const bool writer_ordered = !ordering_fields.empty();
  ObSEArray<const ObAsyncResultFileDesc *, 8> sorted_files;
  sorted_files.set_attr(ObMemAttr(MTL_ID(), "ResultFiles"));
  for (int64_t i = 0; OB_SUCC(ret) && i < data_files.count(); ++i) {
    if (!data_files.at(i).is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async result data file", K(ret), K(i));
    } else if (OB_FAIL(sorted_files.push_back(&data_files.at(i)))) {
      LOG_WARN("failed to collect async result data file", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (writer_ordered) {
      lib::ob_sort(sorted_files.begin(), sorted_files.end(),
                   [](const ObAsyncResultFileDesc *left,
                      const ObAsyncResultFileDesc *right) {
                     if (left->writer_sqc_id_ != right->writer_sqc_id_) {
                       return left->writer_sqc_id_ < right->writer_sqc_id_;
                     } else if (left->writer_task_id_ != right->writer_task_id_) {
                       return left->writer_task_id_ < right->writer_task_id_;
                     } else if (left->file_sequence_ != right->file_sequence_) {
                       return left->file_sequence_ < right->file_sequence_;
                     } else {
                       return left->get_path().compare(right->get_path()) < 0;
                     }
                   });
    } else {
      lib::ob_sort(sorted_files.begin(), sorted_files.end(),
                   [](const ObAsyncResultFileDesc *left,
                      const ObAsyncResultFileDesc *right) {
                     return left->get_path().compare(right->get_path()) < 0;
                   });
    }
  }
  ObResultPartitionMetadata partition;
  int64_t current_sqc_id = -1;
  int64_t current_task_id = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < sorted_files.count(); ++i) {
    const ObAsyncResultFileDesc &data_file = *sorted_files.at(i);
    const bool new_partition = !writer_ordered
        || data_file.writer_sqc_id_ != current_sqc_id
        || data_file.writer_task_id_ != current_task_id;
    if (new_partition) {
      if (!partition.files_.empty() && OB_FAIL(metadata.add_partition(partition))) {
        LOG_WARN("failed to retain async result writer partition", K(ret), K(partition));
      } else {
        partition.files_.reset();
        partition.partition_id_ = metadata.get_partitions().count();
        partition.row_count_ = 0;
        partition.compressed_size_ = 0;
        current_sqc_id = data_file.writer_sqc_id_;
        current_task_id = data_file.writer_task_id_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (writer_ordered && data_file.file_sequence_ != partition.files_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("async result writer file sequence has a gap or duplicate",
               K(ret), K(data_file), "expected_sequence", partition.files_.count());
    } else if (data_file.row_count_ > INT64_MAX - partition.row_count_
               || data_file.byte_count_ > INT64_MAX - partition.compressed_size_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("async result writer partition summary overflow", K(ret), K(data_file));
    }
    ObResultFileMetadata file;
    file.row_count_ = data_file.row_count_;
    file.byte_count_ = data_file.byte_count_;
    file.writer_sqc_id_ = data_file.writer_sqc_id_;
    file.writer_task_id_ = data_file.writer_task_id_;
    file.file_sequence_ = data_file.file_sequence_;
    OZ(make_relative_data_file_path(metadata_allocator,
                                    full_result_root,
                                    data_file.get_path(),
                                    file.path_));
    OZ(load_result_parquet_row_groups(access_info, data_file, file));
    OZ(partition.files_.push_back(file));
    if (OB_SUCC(ret)) {
      partition.row_count_ += file.row_count_;
      partition.compressed_size_ += file.byte_count_;
    }
  }
  if (OB_SUCC(ret) && !partition.files_.empty()
      && OB_FAIL(metadata.add_partition(partition))) {
    LOG_WARN("failed to retain final async result writer partition", K(ret), K(partition));
  }
  OZ(metadata.finalize_summary());
  OZ(metadata.get_chunk_count(chunk_count));
  OZ(metadata.to_json(metadata_allocator, json));
  if (OB_SUCC(ret)) {
    ObBackupIoAdapter io_adapter;
    if (OB_FAIL(io_adapter.write_single_file(
            full_metadata_path,
            storage_dest.get_storage_info(),
            json.ptr(),
            json.length(),
            ObStorageIdMod::get_default_id_mod()))) {
      LOG_WARN("failed to publish async result metadata", K(ret), K(full_metadata_path));
    } else {
      LOG_DEBUG("published async result metadata",
               K(full_metadata_path),
               "partition_count", metadata.get_partitions().count(),
               "file_count", metadata.get_file_count(),
               "chunk_count", chunk_count,
               "row_count", metadata.get_row_count(),
               "byte_count", metadata.get_byte_count(),
               "ordering_mode", metadata.get_ordering_mode(),
               "ordering_field_count", metadata.get_ordering_fields().count());
    }
  }
  return ret;
}

int ObAsyncResultMetadataPublisher::cleanup(const ObSelectIntoSpec &spec)
{
  int ret = OB_SUCCESS;
  if (!spec.async_result_target_.enabled_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cannot clean a non-async-result sink", K(ret));
  } else if (OB_FAIL(ObAsyncResultStorageUtils::cleanup_result_job_prefix(
                         spec.async_result_target_.tenant_id_,
                         spec.async_result_target_.job_id_))) {
    LOG_WARN("failed to clean unsuccessful async result", KR(ret),
             K(spec.async_result_target_.tenant_id_), K(spec.async_result_target_.job_id_));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
