/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_unified_catalog_partition_spec_codec.h"

#include <cstring>

#include "common/rowkey/ob_rowkey.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_se_array.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_sort.h"
#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_list_row_values.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace share
{

int ObUnifiedCatalogPartitionSpecCodec::check_table_partition_structure(
    const ObTableSchema &table_schema, const bool allow_unmaterialized_template)
{
  int ret = OB_SUCCESS;
  ObPartition **parts = table_schema.get_part_array();
  const int64_t part_count = table_schema.get_partition_num();
  if (part_count <= 0 || OB_ISNULL(parts)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partitioned table has no partitions", K(ret), K(part_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
    const ObPartition *part = parts[i];
    if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret), K(i));
    } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()
               && !(allow_unmaterialized_template
                    && table_schema.get_def_subpartition_num() > 0)) {
      ObSubPartition **subparts = part->get_subpart_array();
      const int64_t subpart_count = part->get_subpartition_num();
      if (subpart_count <= 0 || OB_ISNULL(subparts)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("two-level partition has no physical subpartitions",
                 K(ret),
                 K(i),
                 K(subpart_count));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < subpart_count; ++j) {
        if (OB_ISNULL(subparts[j])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpartition is null", K(ret), K(i), K(j));
        }
      }
    }
  }
  if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
    const int64_t template_count = table_schema.get_def_subpartition_num();
    ObSubPartition **templates = table_schema.get_def_subpart_array();
    if (template_count > 0 && OB_ISNULL(templates)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartition template is missing", K(ret), K(template_count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < template_count; ++i) {
      if (OB_ISNULL(templates[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition template is null", K(ret), K(i));
      }
    }
  }
  return ret;
}

bool ObUnifiedCatalogPartitionSpecCodec::partition_position_less(const ObPartition *lhs,
                                                                 const ObPartition *rhs)
{
  return lhs->get_part_idx() != rhs->get_part_idx()
             ? lhs->get_part_idx() < rhs->get_part_idx()
             : lhs->get_part_id() < rhs->get_part_id();
}

bool ObUnifiedCatalogPartitionSpecCodec::subpartition_position_less(const ObSubPartition *lhs,
                                                                    const ObSubPartition *rhs)
{
  return lhs->get_sub_part_idx() != rhs->get_sub_part_idx()
             ? lhs->get_sub_part_idx() < rhs->get_sub_part_idx()
             : lhs->get_sub_part_id() < rhs->get_sub_part_id();
}

bool ObUnifiedCatalogPartitionSpecCodec::is_generated_hash_name(const ObString &name,
                                                                const int64_t index,
                                                                const ObHashNameType type,
                                                                const bool is_oracle_mode,
                                                                const ObPartition *parent)
{
  bool is_generated = false;
  char generated[OB_MAX_PARTITION_NAME_LENGTH] = {0};
  // If the native generator cannot build a name in its fixed buffer, keep the stored name explicit.
  if (NULL == parent || parent->get_part_name().length() < OB_MAX_PARTITION_NAME_LENGTH) {
    const int name_ret = ObPartitionSchema::gen_hash_part_name(index,
                                                                type,
                                                                is_oracle_mode,
                                                                generated,
                                                                sizeof(generated),
                                                                NULL,
                                                                parent);
    if (OB_SUCCESS == name_ret) {
      is_generated = 0 == name.compare(ObString(static_cast<int32_t>(strlen(generated)),
                                                generated));
    }
  }
  return is_generated;
}

int ObUnifiedCatalogPartitionSpecCodec::alloc_object(ObIAllocator &allocator, ObJsonObject *&object)
{
  int ret = OB_SUCCESS;
  object = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(object)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Unified Catalog partition object", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::alloc_array(ObIAllocator &allocator, ObJsonArray *&array)
{
  int ret = OB_SUCCESS;
  array = OB_NEWx(ObJsonArray, &allocator, &allocator);
  if (OB_ISNULL(array)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Unified Catalog partition array", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::add_string(ObIAllocator &allocator,
                                                   ObJsonObject &object,
                                                   const char *key,
                                                   const ObString &value)
{
  int ret = OB_SUCCESS;
  ObJsonString *node = OB_NEWx(ObJsonString, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Unified Catalog partition string", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add Unified Catalog partition string", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::add_integer(ObIAllocator &allocator,
                                                    ObJsonObject &object,
                                                    const char *key,
                                                    const int64_t value)
{
  int ret = OB_SUCCESS;
  ObJsonInt *node = OB_NEWx(ObJsonInt, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Unified Catalog partition integer", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add Unified Catalog partition integer", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::add_empty_object(ObIAllocator &allocator,
                                                         ObJsonObject &object,
                                                         const char *key)
{
  int ret = OB_SUCCESS;
  ObJsonObject *empty = NULL;
  if (OB_FAIL(alloc_object(allocator, empty))) {
    LOG_WARN("failed to allocate empty Unified Catalog partition object", K(ret), K(key));
  } else if (OB_FAIL(object.object_add(ObString::make_string(key), empty))) {
    LOG_WARN("failed to add empty Unified Catalog partition object", K(ret), K(key));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::get_method(const ObPartitionFuncType part_type, ObString &method)
{
  int ret = OB_SUCCESS;
  if (PARTITION_FUNC_TYPE_HASH == part_type) {
    method = ObString::make_string(METHOD_HASH);
  } else if (PARTITION_FUNC_TYPE_KEY == part_type
             || PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type) {
    method = ObString::make_string(METHOD_KEY);
  } else if (PARTITION_FUNC_TYPE_RANGE == part_type
             || PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type) {
    method = ObString::make_string(METHOD_RANGE);
  } else if (PARTITION_FUNC_TYPE_LIST == part_type
             || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
    method = ObString::make_string(METHOD_LIST);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported Unified Catalog partition method", K(ret), K(part_type));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build_key(ObIAllocator &allocator,
                                                  const ObTableSchema &table_schema,
                                                  const ObPartitionOption &option,
                                                  const ObPartitionKeyInfo &key_info,
                                                  ObJsonObject *&key)
{
  int ret = OB_SUCCESS;
  const ObPartitionFuncType part_type = option.get_part_func_type();
  if (OB_FAIL(alloc_object(allocator, key))) {
    LOG_WARN("failed to allocate Unified Catalog partition key", K(ret));
  } else if (PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type) {
    if (OB_FAIL(add_empty_object(allocator, *key, KEY_IMPLICIT_PRIMARY_KEY))) {
      LOG_WARN("failed to add implicit partition key", K(ret));
    }
  } else if (PARTITION_FUNC_TYPE_KEY == part_type || PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type
             || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
    ObJsonArray *columns = NULL;
    if (option.get_part_func_expr_str().empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("partition columns lack their original expression", K(ret));
    } else if (OB_FAIL(alloc_array(allocator, columns))) {
      LOG_WARN("failed to allocate partition key columns", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < key_info.get_size(); ++i) {
      ObRowkeyColumn rowkey_column;
      const ObColumnSchemaV2 *column = NULL;
      if (OB_FAIL(key_info.get_column(i, rowkey_column))) {
        LOG_WARN("failed to get partition key column", K(ret), K(i));
      } else if (OB_ISNULL(column = table_schema.get_column_schema(rowkey_column.column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition key column is missing", K(ret), K(rowkey_column.column_id_));
      } else {
        ObJsonString *name = OB_NEWx(ObJsonString, &allocator, column->get_column_name_str());
        if (OB_ISNULL(name)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate partition key column", K(ret), K(i));
        } else if (OB_FAIL(columns->array_append(name))) {
          LOG_WARN("failed to append partition key column", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)
        && (OB_FAIL(key->object_add(ObString::make_string(KEY_COLUMNS), columns))
            || OB_FAIL(add_string(allocator, *key, KEY_OB_PART_EXPR,
                                  option.get_part_func_expr_str())))) {
      LOG_WARN("failed to add partition key columns", K(ret));
    }
  } else if (option.get_part_func_expr_str().empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("partition expression is empty", K(ret));
  } else if (OB_FAIL(
                 add_string(allocator, *key, KEY_EXPRESSION, option.get_part_func_expr_str()))) {
    LOG_WARN("failed to add partition key expression", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build_value(ObIAllocator &allocator,
                                                    const ObObj &value,
                                                    ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_object(allocator, json))) {
    LOG_WARN("failed to allocate partition bound value", K(ret));
  } else if (value.is_max_value()) {
    if (OB_FAIL(add_empty_object(allocator, *json, BOUND_MAX_VALUE))) {
      LOG_WARN("failed to add maximum partition bound", K(ret));
    }
  } else {
    char *buf = static_cast<char *>(allocator.alloc(OB_MAX_PARTITION_EXPR_LENGTH));
    int64_t pos = 0;
    ObObj copied_value = value;
    ObRowkey rowkey(&copied_value, 1);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate partition literal buffer", K(ret));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(false,
                                                                       rowkey,
                                                                       buf,
                                                                       OB_MAX_PARTITION_EXPR_LENGTH,
                                                                       pos,
                                                                       false,
                                                                       nullptr))) {
      LOG_WARN("failed to serialize partition bound literal", K(ret));
    } else if (OB_FAIL(add_string(allocator,
                                  *json,
                                  BOUND_SQL,
                                  ObString(static_cast<int32_t>(pos), buf)))) {
      LOG_WARN("failed to add partition bound literal", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build_values(ObIAllocator &allocator,
                                                     const ObRowkey &rowkey,
                                                     ObJsonArray *&values)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_array(allocator, values))) {
    LOG_WARN("failed to allocate partition bound values", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
    ObJsonObject *value = NULL;
    if (OB_FAIL(build_value(allocator, rowkey.get_obj_ptr()[i], value))) {
      LOG_WARN("failed to build partition bound value", K(ret), K(i));
    } else if (OB_FAIL(values->array_append(value))) {
      LOG_WARN("failed to append partition bound value", K(ret), K(i));
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build_list_tuples(ObIAllocator &allocator,
                                                          const ObBasePartition &partition,
                                                          ObJsonArray *&tuples)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObNewRow> &rows = partition.get_list_row_values();
  if (OB_FAIL(alloc_array(allocator, tuples))) {
    LOG_WARN("failed to allocate list partition tuples", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
    const ObNewRow &row = rows.at(i);
    ObJsonObject *tuple = NULL;
    ObJsonArray *tuple_values = NULL;
    if (OB_FAIL(alloc_object(allocator, tuple)) || OB_FAIL(alloc_array(allocator, tuple_values))) {
      LOG_WARN("failed to allocate list partition tuple", K(ret), K(i));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < row.get_count(); ++j) {
      ObJsonObject *value = NULL;
      if (OB_FAIL(build_value(allocator, row.get_cell(j), value))) {
        LOG_WARN("failed to build list partition value", K(ret), K(i), K(j));
      } else if (OB_FAIL(tuple_values->array_append(value))) {
        LOG_WARN("failed to append list partition value", K(ret), K(i), K(j));
      }
    }
    if (OB_SUCC(ret)
        && (OB_FAIL(tuple->object_add(ObString::make_string(VALUES), tuple_values))
            || OB_FAIL(tuples->array_append(tuple)))) {
      LOG_WARN("failed to append list partition tuple", K(ret), K(i));
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build_definition(ObIAllocator &allocator,
                                                         const ObBasePartition &partition,
                                                         const ObPartitionFuncType part_type,
                                                         ObJsonObject *&definition)
{
  int ret = OB_SUCCESS;
  ObJsonObject *values = NULL;
  ObJsonObject *bound = NULL;
  if (OB_FAIL(alloc_object(allocator, definition))) {
    LOG_WARN("failed to allocate logical partition definition", K(ret));
  } else if (partition.get_part_name().empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("failed to add logical partition name", K(ret));
  } else if (OB_FAIL(add_string(allocator, *definition, DEFINITION_NAME,
                                partition.get_part_name()))) {
    LOG_WARN("failed to add logical partition name", K(ret));
  } else if (is_range_part(part_type)) {
    ObJsonObject *less_than = NULL;
    ObJsonArray *bound_values = NULL;
    char *hex = static_cast<char *>(allocator.alloc(OB_MAX_B_HIGH_BOUND_VAL_LENGTH));
    int64_t hex_pos = 0;
    if (OB_ISNULL(hex)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate range partition bound", K(ret));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_hex(partition.get_high_bound_val(),
                                                                hex,
                                                                OB_MAX_B_HIGH_BOUND_VAL_LENGTH,
                                                                hex_pos))
               || OB_FAIL(add_string(allocator,
                                     *definition,
                                     BOUND_HIGH_HEX,
                                     ObString(static_cast<int32_t>(hex_pos), hex)))
               || OB_FAIL(alloc_object(allocator, values))
               || OB_FAIL(alloc_object(allocator, bound))
               || OB_FAIL(alloc_object(allocator, less_than))
               || OB_FAIL(build_values(allocator, partition.get_high_bound_val(), bound_values))) {
      LOG_WARN("failed to build range partition bound", K(ret));
    } else if (OB_FAIL(less_than->object_add(ObString::make_string(VALUES), bound_values))
               || OB_FAIL(bound->object_add(ObString::make_string(BOUND_LESS_THAN), less_than))
               || OB_FAIL(values->object_add(ObString::make_string(BOUND_RANGE), bound))
               || OB_FAIL(definition->object_add(ObString::make_string(VALUES), values))) {
      LOG_WARN("failed to assemble range partition bound", K(ret));
    }
  } else if (is_list_part(part_type)) {
    const ObIArray<ObNewRow> &rows = partition.get_list_row_values();
    const bool is_default
        = 1 == rows.count() && 1 == rows.at(0).get_count() && rows.at(0).get_cell(0).is_max_value();
    char *hex = static_cast<char *>(allocator.alloc(OB_MAX_B_PARTITION_EXPR_LENGTH));
    int64_t hex_pos = 0;
    if (OB_ISNULL(hex)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate list partition bound", K(ret));
    } else if (OB_FAIL(ObPartitionUtils::convert_rows_to_hex(rows,
                                                              hex,
                                                              OB_MAX_B_PARTITION_EXPR_LENGTH,
                                                              hex_pos))
               || OB_FAIL(add_string(allocator,
                                     *definition,
                                     BOUND_LIST_HEX,
                                     ObString(static_cast<int32_t>(hex_pos), hex)))
               || OB_FAIL(alloc_object(allocator, values))
               || OB_FAIL(alloc_object(allocator, bound))) {
      LOG_WARN("failed to allocate list partition bound", K(ret));
    } else if (is_default) {
      if (OB_FAIL(add_empty_object(allocator, *bound, BOUND_DEFAULT_PARTITION))) {
        LOG_WARN("failed to add default list partition", K(ret));
      }
    } else {
      ObJsonArray *tuples = NULL;
      if (OB_FAIL(build_list_tuples(allocator, partition, tuples))) {
        LOG_WARN("failed to build list partition tuples", K(ret));
      } else if (OB_FAIL(bound->object_add(ObString::make_string(BOUND_TUPLES), tuples))) {
        LOG_WARN("failed to add list partition tuples", K(ret));
      }
    }
    if (OB_SUCC(ret)
        && (OB_FAIL(values->object_add(ObString::make_string(BOUND_LIST), bound))
            || OB_FAIL(definition->object_add(ObString::make_string(VALUES), values)))) {
      LOG_WARN("failed to assemble list partition bound", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build_parent_subpartitions(ObIAllocator &allocator,
                                                                   const ObPartition &partition,
                                                                   const ObPartitionFuncType subpart_type,
                                                                   const bool is_oracle_mode,
                                                                   ObJsonObject *&subpartitions)
{
  int ret = OB_SUCCESS;
  ObSubPartition **parts = partition.get_subpart_array();
  const int64_t count = partition.get_subpartition_num();
  ObSEArray<const ObSubPartition *, 16> ordered_parts;
  bool has_explicit_hash_name = false;
  if (count <= 0 || OB_ISNULL(parts)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("individual subpartitions are missing", K(ret), K(count));
  } else if (OB_FAIL(alloc_object(allocator, subpartitions))) {
    LOG_WARN("failed to allocate individual subpartition rules", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_ISNULL(parts[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("individual subpartition is null", K(ret), K(i));
    } else if (OB_FAIL(ordered_parts.push_back(parts[i]))) {
      LOG_WARN("failed to collect individual subpartition", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && ordered_parts.count() > 1) {
    lib::ob_sort(ordered_parts.get_data(),
                 ordered_parts.get_data() + ordered_parts.count(),
                 subpartition_position_less);
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_hash_like_part(subpart_type) && i < count; ++i) {
    if (!is_generated_hash_name(ordered_parts.at(i)->get_part_name(),
                                i,
                                INDIVIDUAL_SUB_PART,
                                is_oracle_mode,
                                &partition)) {
      has_explicit_hash_name = true;
    }
  }
  if (OB_SUCC(ret) && is_hash_like_part(subpart_type)
      && OB_FAIL(add_integer(allocator, *subpartitions, COUNT, count))) {
    LOG_WARN("failed to add individual subpartition count", K(ret));
  }
  if (OB_SUCC(ret) && (!is_hash_like_part(subpart_type) || has_explicit_hash_name)) {
    ObJsonArray *definitions = NULL;
    if (OB_FAIL(alloc_array(allocator, definitions))) {
      LOG_WARN("failed to allocate individual subpartition bounds", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ordered_parts.count(); ++i) {
      ObJsonObject *definition = NULL;
      if (OB_FAIL(build_definition(allocator, *ordered_parts.at(i), subpart_type, definition))) {
        LOG_WARN("failed to build individual subpartition bound", K(ret), K(i));
      } else if (OB_FAIL(definitions->array_append(definition))) {
        LOG_WARN("failed to append individual subpartition bound", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(subpartitions->object_add(ObString::make_string(DEFINITIONS), definitions))) {
      LOG_WARN("failed to add individual subpartition bounds", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build_layout(ObIAllocator &allocator,
                                                     const ObTableSchema &table_schema,
                                                     ObJsonObject *&layout,
                                                     const bool allow_unmaterialized_template)
{
  int ret = OB_SUCCESS;
  const ObPartitionFuncType part_type = table_schema.get_part_option().get_part_func_type();
  const bool individual_subparts = PARTITION_LEVEL_TWO == table_schema.get_part_level()
                                   && 0 == table_schema.get_def_subpartition_num();
  const int64_t count = table_schema.get_partition_num();
  ObPartition **parts = table_schema.get_part_array();
  ObSEArray<const ObPartition *, 16> ordered_parts;
  bool is_oracle_mode = false;
  bool has_explicit_hash_name = false;
  if (count <= 0 || OB_ISNULL(parts)
      || (is_hash_like_part(part_type)
          && table_schema.get_part_option().get_part_num() != count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partitioned table has no partition definitions", K(ret), K(count));
  } else if (OB_FAIL(alloc_object(allocator, layout))) {
    LOG_WARN("failed to allocate Unified Catalog partition layout", K(ret));
  } else if (!allow_unmaterialized_template
             && (is_hash_like_part(part_type) || individual_subparts)
             && OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("failed to get partition name mode", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_ISNULL(parts[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret), K(i));
    } else if (OB_FAIL(ordered_parts.push_back(parts[i]))) {
      LOG_WARN("failed to collect logical partition", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && ordered_parts.count() > 1) {
    lib::ob_sort(ordered_parts.get_data(),
                 ordered_parts.get_data() + ordered_parts.count(),
                 partition_position_less);
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_hash_like_part(part_type) && i < count; ++i) {
    if (!is_generated_hash_name(ordered_parts.at(i)->get_part_name(),
                                i,
                                FIRST_PART,
                                is_oracle_mode)) {
      has_explicit_hash_name = true;
    }
  }
  if (OB_SUCC(ret) && is_hash_like_part(part_type)) {
    if (OB_FAIL(add_integer(allocator,
                            *layout,
                            LAYOUT_GENERATED_COUNT,
                            table_schema.get_part_option().get_part_num()))) {
      LOG_WARN("failed to add generated partition count", K(ret));
    }
  }
  if (OB_SUCC(ret)
      && (!is_hash_like_part(part_type) || individual_subparts || has_explicit_hash_name)) {
    ObJsonArray *definitions = NULL;
    if (OB_FAIL(alloc_array(allocator, definitions))) {
      LOG_WARN("failed to allocate logical partition definitions", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ordered_parts.count(); ++i) {
      ObJsonObject *definition = NULL;
      if (OB_FAIL(build_definition(allocator, *ordered_parts.at(i), part_type, definition))) {
        LOG_WARN("failed to build logical partition definition", K(ret), K(i));
      } else if (individual_subparts) {
        ObJsonObject *subpartitions = NULL;
        if (OB_FAIL(
                build_parent_subpartitions(allocator,
                                           *ordered_parts.at(i),
                                           table_schema.get_sub_part_option().get_part_func_type(),
                                           is_oracle_mode,
                                           subpartitions))) {
          LOG_WARN("failed to build individual subpartition rules", K(ret), K(i));
        } else if (OB_FAIL(definition->object_add(ObString::make_string(SUBPARTITIONS),
                                                  subpartitions))) {
          LOG_WARN("failed to add individual subpartition rules", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(definitions->array_append(definition))) {
        LOG_WARN("failed to append logical partition definition", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(layout->object_add(ObString::make_string(DEFINITIONS), definitions))) {
      LOG_WARN("failed to add logical partition definitions", K(ret));
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build_subpartitioning(ObIAllocator &allocator,
                                                              const ObTableSchema &table_schema,
                                                              ObJsonObject *&subpartitioning,
                                                              const bool allow_unmaterialized_template)
{
  int ret = OB_SUCCESS;
  const ObPartitionFuncType subpart_type = table_schema.get_sub_part_option().get_part_func_type();
  const int64_t template_count = table_schema.get_def_subpartition_num();
  ObString method;
  ObJsonObject *key = NULL;
  ObJsonObject *layout = NULL;
  bool is_generated_template = template_count > 0 && is_hash_like_part(subpart_type);
  if (is_generated_template) {
    bool is_oracle_mode = false;
    ObSubPartition **parts = table_schema.get_def_subpart_array();
    ObSEArray<const ObSubPartition *, 16> ordered_parts;
    if (OB_ISNULL(parts)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartition template is missing", K(ret));
    } else if (!allow_unmaterialized_template
               && OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("failed to get subpartition name mode", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < template_count; ++i) {
      if (OB_ISNULL(parts[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition template entry is null", K(ret), K(i));
      } else if (OB_FAIL(ordered_parts.push_back(parts[i]))) {
        LOG_WARN("failed to collect subpartition template", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && ordered_parts.count() > 1) {
      lib::ob_sort(ordered_parts.get_data(),
                   ordered_parts.get_data() + ordered_parts.count(),
                   subpartition_position_less);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < template_count; ++i) {
      if (!is_generated_hash_name(ordered_parts.at(i)->get_part_name(),
                                  i,
                                  TEMPLATE_SUB_PART,
                                  is_oracle_mode)) {
        is_generated_template = false;
      }
    }
  }
  if (OB_SUCC(ret)
      && (OB_FAIL(get_method(subpart_type, method))
          || OB_FAIL(alloc_object(allocator, subpartitioning))
          || OB_FAIL(build_key(allocator,
                               table_schema,
                               table_schema.get_sub_part_option(),
                               table_schema.get_subpartition_key_info(),
                               key))
          || OB_FAIL(alloc_object(allocator, layout)))) {
    LOG_WARN("failed to build Unified Catalog subpartitioning", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (is_generated_template) {
      ObJsonObject *count = NULL;
      if (OB_FAIL(alloc_object(allocator, count))
          || OB_FAIL(add_integer(allocator, *count, COUNT, template_count))
          || OB_FAIL(layout->object_add(ObString::make_string(LAYOUT_TEMPLATE_COUNT), count))) {
        LOG_WARN("failed to add subpartition template count", K(ret));
      }
    } else if (template_count > 0) {
      ObSubPartition **parts = table_schema.get_def_subpart_array();
      ObSEArray<const ObSubPartition *, 16> ordered_parts;
      ObJsonObject *container = NULL;
      ObJsonArray *definitions = NULL;
      if (OB_ISNULL(parts)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition template is missing", K(ret));
      } else if (OB_FAIL(alloc_object(allocator, container))
                 || OB_FAIL(alloc_array(allocator, definitions))) {
        LOG_WARN("failed to allocate subpartition template bounds", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < template_count; ++i) {
        if (OB_ISNULL(parts[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpartition template entry is null", K(ret), K(i));
        } else if (OB_FAIL(ordered_parts.push_back(parts[i]))) {
          LOG_WARN("failed to collect subpartition template", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && ordered_parts.count() > 1) {
        lib::ob_sort(ordered_parts.get_data(),
                     ordered_parts.get_data() + ordered_parts.count(),
                     subpartition_position_less);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ordered_parts.count(); ++i) {
        ObJsonObject *definition = NULL;
        if (OB_FAIL(build_definition(allocator, *ordered_parts.at(i), subpart_type, definition))) {
          LOG_WARN("failed to build subpartition template bound", K(ret), K(i));
        } else if (OB_FAIL(definitions->array_append(definition))) {
          LOG_WARN("failed to append subpartition template bound", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)
          && (OB_FAIL(container->object_add(ObString::make_string(DEFINITIONS), definitions))
              || OB_FAIL(layout->object_add(ObString::make_string(LAYOUT_TEMPLATE_DEFINITIONS),
                                            container)))) {
        LOG_WARN("failed to add subpartition template bounds", K(ret));
      }
    } else {
      int64_t default_count = table_schema.get_sub_part_option().get_part_num();
      if (default_count <= 0) {
        default_count = DEFAULT_GENERATED_SUBPARTITION_COUNT;
      }
      ObJsonObject *individual = NULL;
      if (OB_FAIL(alloc_object(allocator, individual))
          || OB_FAIL(add_integer(allocator, *individual, LAYOUT_DEFAULT_GENERATED_COUNT,
                                 default_count))
          || OB_FAIL(layout->object_add(ObString::make_string(LAYOUT_INDIVIDUAL), individual))) {
        LOG_WARN("failed to add individual subpartition layout", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)
      && (OB_FAIL(add_string(allocator, *subpartitioning, METHOD_FIELD, method))
          || OB_FAIL(subpartitioning->object_add(ObString::make_string(KEY_FIELD), key))
          || OB_FAIL(subpartitioning->object_add(ObString::make_string(LAYOUT_FIELD), layout)))) {
    LOG_WARN("failed to assemble Unified Catalog subpartitioning", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::build(ObIAllocator &allocator,
                                              const ObTableSchema &table_schema,
                                              ObJsonObject *&partition_spec,
                                              const bool allow_unmaterialized_template)
{
  int ret = OB_SUCCESS;
  partition_spec = NULL;
  const ObPartitionFuncType part_type = table_schema.get_part_option().get_part_func_type();
  ObString method;
  ObJsonObject *key = NULL;
  ObJsonObject *layout = NULL;
  ObJsonObject *subpartitioning = NULL;
  if (!table_schema.is_partitioned_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("non-partitioned table has no Unified Catalog partition spec", K(ret));
  } else if (table_schema.get_part_option().get_auto_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("automatic partitioning is not supported by Unified Catalog partition spec", K(ret));
  } else if (OB_FAIL(check_table_partition_structure(table_schema,
                                                     allow_unmaterialized_template))) {
    LOG_WARN("invalid partition structure for Unified Catalog sync", K(ret));
  } else if (OB_FAIL(get_method(part_type, method))) {
    LOG_WARN("unsupported Unified Catalog partition method", K(ret));
  } else if (OB_FAIL(alloc_object(allocator, partition_spec))
             || OB_FAIL(build_key(allocator,
                                  table_schema,
                                  table_schema.get_part_option(),
                                  table_schema.get_partition_key_info(),
                                  key))
             || OB_FAIL(build_layout(allocator, table_schema, layout,
                                     allow_unmaterialized_template))) {
    LOG_WARN("failed to build Unified Catalog partition spec", K(ret));
  } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()
             && OB_FAIL(build_subpartitioning(allocator, table_schema, subpartitioning,
                                               allow_unmaterialized_template))) {
    LOG_WARN("failed to build Unified Catalog subpartition spec", K(ret));
  } else if (OB_FAIL(add_integer(allocator,
                                 *partition_spec,
                                 SHOW_CREATE_VERSION_FIELD,
                                 SHOW_CREATE_VERSION))
             || OB_FAIL(add_integer(allocator,
                                 *partition_spec,
                                 LEVEL_FIELD,
                                 static_cast<int64_t>(table_schema.get_part_level())))
             || OB_FAIL(add_string(allocator, *partition_spec, METHOD_FIELD, method))
             || OB_FAIL(partition_spec->object_add(ObString::make_string(KEY_FIELD), key))
             || OB_FAIL(partition_spec->object_add(ObString::make_string(LAYOUT_FIELD), layout))) {
    LOG_WARN("failed to assemble Unified Catalog partition spec", K(ret));
  } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()
             && OB_FAIL(partition_spec->object_add(ObString::make_string(SUBPARTITIONING_FIELD),
                                                   subpartitioning))) {
    LOG_WARN("failed to add Unified Catalog subpartitioning", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::validate(ObIAllocator &allocator,
                                                 const ObTableSchema &table_schema,
                                                 const ObJsonNode *remote_partition_spec)
{
  int ret = OB_SUCCESS;
  ObJsonObject *expected = NULL;
  int comparison = 0;
  if (OB_ISNULL(remote_partition_spec)
      || ObJsonNodeType::J_OBJECT != remote_partition_spec->json_type()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog partition spec is missing or invalid", K(ret));
  } else if (OB_FAIL(build(allocator, table_schema, expected))) {
    LOG_WARN("failed to build expected Unified Catalog partition spec", K(ret));
  } else if (OB_FAIL(expected->compare(*remote_partition_spec, comparison))) {
    LOG_WARN("failed to compare Unified Catalog partition spec", K(ret));
  } else if (0 != comparison) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("Unified Catalog partition spec differs from OceanBase schema", K(ret));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::required_object(const ObJsonNode *node,
                                                        const ObJsonObject *&object)
{
  int ret = OB_SUCCESS;
  object = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_NOT_SUPPORTED;
  } else if (ObJsonNodeType::J_OBJECT != node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    object = static_cast<const ObJsonObject *>(node);
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::required_array(const ObJsonObject &object,
                                                       const char *key,
                                                       const ObJsonNode *&array)
{
  int ret = OB_SUCCESS;
  array = object.get_value(ObString::make_string(key));
  if (OB_ISNULL(array)) {
    ret = OB_NOT_SUPPORTED;
  } else if (ObJsonNodeType::J_ARRAY != array->json_type()) {
    ret = OB_STATE_NOT_MATCH;
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::required_string(const ObJsonObject &object,
                                                        const char *key,
                                                        ObString &value)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = object.get_value(ObString::make_string(key));
  if (OB_ISNULL(node)) {
    ret = OB_NOT_SUPPORTED;
  } else if (ObJsonNodeType::J_STRING != node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    value = static_cast<const ObJsonString *>(node)->value();
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::required_integer(const ObJsonObject &object,
                                                         const char *key,
                                                         int64_t &value)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *node = object.get_value(ObString::make_string(key));
  if (OB_ISNULL(node)) {
    ret = OB_NOT_SUPPORTED;
  } else if (ObJsonNodeType::J_INT != node->json_type()) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    value = static_cast<const ObJsonInt *>(node)->value();
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::array_object(const ObJsonNode &array,
                                                     const int64_t index,
                                                     const ObJsonObject *&object)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *node = NULL;
  if (ObJsonNodeType::J_ARRAY != array.json_type()
      || index < 0 || index >= static_cast<int64_t>(array.element_count())) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(array.get_array_element(index, node))) {
    LOG_WARN("failed to read partition definition", K(ret), K(index));
  } else if (OB_FAIL(required_object(static_cast<const ObJsonNode *>(node), object))) {
    LOG_WARN("partition definition is not an object", K(ret), K(index));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::decode_hex(ObIAllocator &allocator,
                                                   const ObString &hex,
                                                   const int64_t max_length,
                                                   char *&bytes,
                                                   int64_t &byte_length)
{
  int ret = OB_SUCCESS;
  bytes = NULL;
  byte_length = hex.length() / 2;
  if (hex.empty() || hex.length() > max_length || 0 != hex.length() % 2) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(bytes = static_cast<char *>(allocator.alloc(byte_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (hex.length() != str_to_hex(hex.ptr(), hex.length(), bytes, byte_length)) {
    ret = OB_STATE_NOT_MATCH;
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::set_generated_name(ObBasePartition &partition,
                                                           const int64_t index,
                                                           const ObHashNameType type,
                                                           const bool is_oracle_mode,
                                                           const ObPartition *parent)
{
  int ret = OB_SUCCESS;
  char generated[OB_MAX_PARTITION_NAME_LENGTH] = {0};
  if (NULL != parent && parent->get_part_name().length() >= OB_MAX_PARTITION_NAME_LENGTH) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("generated subpartition name exceeds native limit", K(ret), K(index));
  } else if (OB_FAIL(ObPartitionSchema::gen_hash_part_name(index,
                                                            type,
                                                            is_oracle_mode,
                                                            generated,
                                                            sizeof(generated),
                                                            NULL,
                                                            parent))) {
    LOG_WARN("failed to generate partition name", K(ret), K(index));
  } else if (OB_FAIL(partition.set_part_name(ObString::make_string(generated)))) {
    LOG_WARN("failed to set generated partition name", K(ret), K(index));
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::restore_key(const ObJsonObject &source,
                                                    const bool is_subpartition,
                                                    ObTableSchema &table_schema,
                                                    ObPartitionFuncType &part_type,
                                                    int64_t &bound_width)
{
  int ret = OB_SUCCESS;
  const ObJsonObject *key = NULL;
  ObString method;
  ObString expression;
  const ObJsonNode *columns = NULL;
  ObPartitionOption &option = is_subpartition ? table_schema.get_sub_part_option()
                                               : table_schema.get_part_option();
  bound_width = 1;
  if (OB_FAIL(required_string(source, METHOD_FIELD, method))
      || OB_FAIL(required_object(source.get_value(ObString::make_string(KEY_FIELD)), key))) {
    LOG_WARN("partition method or key is missing", K(ret));
  } else {
    const bool implicit = NULL != key->get_value(ObString::make_string(KEY_IMPLICIT_PRIMARY_KEY));
    const bool has_columns = NULL != key->get_value(ObString::make_string(KEY_COLUMNS));
    const bool has_expression = NULL != key->get_value(ObString::make_string(KEY_EXPRESSION));
    if (static_cast<int>(implicit) + static_cast<int>(has_columns)
            + static_cast<int>(has_expression) != 1) {
      ret = OB_STATE_NOT_MATCH;
    } else if (implicit) {
      const ObJsonObject *empty = NULL;
      if (method != ObString::make_string(METHOD_KEY)) {
        ret = OB_STATE_NOT_MATCH;
      } else if (OB_FAIL(required_object(
                     key->get_value(ObString::make_string(KEY_IMPLICIT_PRIMARY_KEY)), empty))) {
        LOG_WARN("implicit partition key is invalid", K(ret));
      } else if (0 != empty->element_count()) {
        ret = OB_STATE_NOT_MATCH;
      } else {
        part_type = PARTITION_FUNC_TYPE_KEY_IMPLICIT;
        bound_width = 0;
      }
    } else if (has_columns) {
      if (method == ObString::make_string(METHOD_KEY)) {
        part_type = PARTITION_FUNC_TYPE_KEY;
      } else if (method == ObString::make_string(METHOD_RANGE)) {
        part_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
      } else if (method == ObString::make_string(METHOD_LIST)) {
        part_type = PARTITION_FUNC_TYPE_LIST_COLUMNS;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(required_array(*key, KEY_COLUMNS, columns))
            || OB_FAIL(required_string(*key, KEY_OB_PART_EXPR, expression))) {
          LOG_WARN("partition key columns or original expression missing", K(ret));
        } else {
          bound_width = static_cast<int64_t>(columns->element_count());
          if (bound_width <= 0 || bound_width > OB_MAX_PARTITION_KEY_COLUMN_NUMBER
              || expression.empty()) {
            ret = OB_STATE_NOT_MATCH;
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < bound_width; ++i) {
        ObIJsonBase *column_node = NULL;
        if (OB_FAIL(columns->get_array_element(i, column_node))) {
          LOG_WARN("failed to read partition key column", K(ret), K(i));
        } else if (OB_ISNULL(column_node)
                   || ObJsonNodeType::J_STRING != column_node->json_type()) {
          ret = OB_STATE_NOT_MATCH;
        } else {
          const ObString name = static_cast<const ObJsonString *>(column_node)->value();
          if (name.empty()) {
            ret = OB_STATE_NOT_MATCH;
          } else if (is_subpartition) {
            if (OB_FAIL(table_schema.add_subpartition_key(name))) {
              LOG_WARN("failed to restore subpartition key", K(ret), K(name));
            }
          } else if (OB_FAIL(table_schema.add_partition_key(name))) {
            LOG_WARN("failed to restore partition key", K(ret), K(name));
          }
        }
      }
    } else {
      if (method == ObString::make_string(METHOD_HASH)) {
        part_type = PARTITION_FUNC_TYPE_HASH;
      } else if (method == ObString::make_string(METHOD_RANGE)) {
        part_type = PARTITION_FUNC_TYPE_RANGE;
      } else if (method == ObString::make_string(METHOD_LIST)) {
        part_type = PARTITION_FUNC_TYPE_LIST;
      } else {
        ret = OB_STATE_NOT_MATCH;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(required_string(*key, KEY_EXPRESSION, expression))) {
          LOG_WARN("partition expression is missing", K(ret));
        } else if (expression.empty()) {
          ret = OB_STATE_NOT_MATCH;
        }
      }
    }
    if (OB_SUCC(ret)) {
      option.set_part_func_type(part_type);
      if (OB_FAIL(option.set_part_expr(expression))) {
        LOG_WARN("failed to set partition expression", K(ret));
      }
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::restore_definition(ObIAllocator &allocator,
                                                           const ObJsonObject &source,
                                                           const ObPartitionFuncType part_type,
                                                           const int64_t bound_width,
                                                           const bool is_oracle_mode,
                                                           ObBasePartition &partition)
{
  int ret = OB_SUCCESS;
  ObString name;
  if (OB_FAIL(required_string(source, DEFINITION_NAME, name))) {
    LOG_WARN("partition definition lacks its name; re-SYNC is required", K(ret));
  } else if (name.empty()
             || (is_oracle_mode
                     ? name.length() > OB_MAX_PARTITION_NAME_LENGTH
                     : ObCharset::strlen_char(CS_TYPE_UTF8MB4_GENERAL_CI,
                                              name.ptr(),
                                              name.length()) > OB_MAX_PARTITION_NAME_LENGTH)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(partition.set_part_name(name))) {
    LOG_WARN("failed to restore partition name", K(ret));
  } else if (is_range_part(part_type)) {
    ObString hex;
    char *bytes = NULL;
    int64_t byte_length = 0;
    int64_t pos = 0;
    ObRowkey rowkey;
    if (OB_FAIL(required_string(source, BOUND_HIGH_HEX, hex))) {
      LOG_WARN("range partition lacks typed bound; re-SYNC is required", K(ret));
    } else if (OB_FAIL(decode_hex(allocator, hex, OB_MAX_B_HIGH_BOUND_VAL_LENGTH,
                                bytes, byte_length))) {
      LOG_WARN("invalid range partition bound hex", K(ret));
    } else if (OB_FAIL(rowkey.deserialize(allocator, bytes, byte_length, pos))) {
      LOG_WARN("failed to decode typed range bound", K(ret));
    } else if (pos != byte_length || rowkey.get_obj_cnt() != bound_width) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(partition.set_high_bound_val(rowkey))) {
      LOG_WARN("failed to restore typed range bound", K(ret));
    }
  } else if (is_list_part(part_type)) {
    ObString hex;
    char *bytes = NULL;
    int64_t byte_length = 0;
    int64_t pos = 0;
    int64_t row_count = 0;
    ObListRowValues rows(allocator);
    if (OB_FAIL(required_string(source, BOUND_LIST_HEX, hex))) {
      LOG_WARN("list partition lacks typed bound; re-SYNC is required", K(ret));
    } else if (OB_FAIL(decode_hex(allocator, hex, OB_MAX_B_PARTITION_EXPR_LENGTH,
                                bytes, byte_length))) {
      LOG_WARN("invalid list partition bound hex", K(ret));
    } else if (OB_FAIL(serialization::decode_vi64(bytes, byte_length, pos, &row_count))) {
      LOG_WARN("failed to read list partition row count", K(ret));
    } else if (row_count <= 0 || row_count > OB_MAX_PARTITION_NUM_MYSQL
               || row_count > byte_length) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      pos = 0;
      if (OB_FAIL(rows.deserialize(allocator, bytes, byte_length, pos))) {
        LOG_WARN("failed to decode typed list bounds", K(ret));
      } else if (pos != byte_length || rows.count() != row_count) {
        ret = OB_STATE_NOT_MATCH;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
        const ObNewRow &row = rows.at(i);
        const bool is_default = 1 == rows.count() && 1 == row.get_count()
                                && row.get_cell(0).is_max_value();
        if (!is_default && row.get_count() != bound_width) {
          ret = OB_STATE_NOT_MATCH;
        } else if (OB_FAIL(partition.add_list_row(row))) {
          LOG_WARN("failed to restore typed list row", K(ret), K(i));
        }
      }
    }
  } else if (!is_hash_like_part(part_type)) {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::restore_individual_subpartitions(
    ObIAllocator &allocator,
    const ObJsonObject &source,
    const ObPartitionFuncType part_type,
    const int64_t bound_width,
    const bool is_oracle_mode,
    ObPartition &parent)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *definitions = source.get_value(ObString::make_string(DEFINITIONS));
  int64_t count = 0;
  if (is_hash_like_part(part_type)) {
    if (OB_FAIL(required_integer(source, COUNT, count))) {
      LOG_WARN("individual HASH/KEY count is missing", K(ret));
    }
  } else if (OB_FAIL(required_array(source, DEFINITIONS, definitions))) {
    LOG_WARN("individual partition definitions are missing", K(ret));
  } else {
    count = definitions->element_count();
  }
  const int64_t max_count = is_oracle_mode ? OB_MAX_PARTITION_NUM_ORACLE
                                            : OB_MAX_PARTITION_NUM_MYSQL;
  if (OB_SUCC(ret)) {
    if (count <= 0 || count > max_count) {
      ret = OB_STATE_NOT_MATCH;
    } else if (NULL != definitions
               && (ObJsonNodeType::J_ARRAY != definitions->json_type()
                   || static_cast<int64_t>(definitions->element_count()) != count)) {
      ret = OB_STATE_NOT_MATCH;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObSubPartition subpart(&allocator);
    subpart.set_tenant_id(parent.get_tenant_id());
    subpart.set_table_id(parent.get_table_id());
    subpart.set_part_id(parent.get_part_id());
    subpart.set_sub_part_id(i);
    subpart.set_sub_part_idx(i);
    subpart.set_part_idx(i);
    if (NULL == definitions) {
      if (OB_FAIL(set_generated_name(subpart, i, INDIVIDUAL_SUB_PART,
                                     is_oracle_mode, &parent))) {
        LOG_WARN("failed to restore generated subpartition name", K(ret));
      }
    } else {
      const ObJsonObject *definition = NULL;
      if (OB_FAIL(array_object(*definitions, i, definition))) {
        LOG_WARN("failed to read individual subpartition", K(ret), K(i));
      } else if (OB_FAIL(restore_definition(allocator, *definition, part_type,
                                            bound_width, is_oracle_mode, subpart))) {
        LOG_WARN("failed to restore individual subpartition", K(ret), K(i));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < parent.get_subpartition_num(); ++j) {
      if (ObCharset::case_insensitive_equal(subpart.get_part_name(),
                                            parent.get_subpart_array()[j]->get_part_name())) {
        ret = OB_STATE_NOT_MATCH;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(parent.add_partition(subpart))) {
      LOG_WARN("failed to add individual subpartition", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    parent.set_sub_part_num(count);
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::restore_layout(ObIAllocator &allocator,
                                                       const ObJsonObject &layout,
                                                       const ObPartitionFuncType part_type,
                                                       const int64_t bound_width,
                                                       const bool is_oracle_mode,
                                                       const bool individual_subparts,
                                                       const ObPartitionFuncType subpart_type,
                                                       const int64_t subpart_bound_width,
                                                       ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *definitions = layout.get_value(ObString::make_string(DEFINITIONS));
  int64_t count = 0;
  if (is_hash_like_part(part_type)) {
    if (OB_FAIL(required_integer(layout, LAYOUT_GENERATED_COUNT, count))) {
      LOG_WARN("generated partition count is missing", K(ret));
    }
  } else if (OB_FAIL(required_array(layout, DEFINITIONS, definitions))) {
    LOG_WARN("partition definitions are missing", K(ret));
  } else {
    count = definitions->element_count();
  }
  const int64_t max_count = is_oracle_mode ? OB_MAX_PARTITION_NUM_ORACLE
                                            : OB_MAX_PARTITION_NUM_MYSQL;
  if (OB_SUCC(ret)) {
    if (count <= 0 || count > max_count || (individual_subparts && NULL == definitions)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (NULL != definitions
               && (ObJsonNodeType::J_ARRAY != definitions->json_type()
                   || static_cast<int64_t>(definitions->element_count()) != count)) {
      ret = OB_STATE_NOT_MATCH;
    }
  }
  int64_t physical_subpartitions = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObPartition partition(&allocator);
    partition.set_tenant_id(table_schema.get_tenant_id());
    partition.set_table_id(table_schema.get_table_id());
    partition.set_part_id(i);
    partition.set_part_idx(i);
    const ObJsonObject *definition = NULL;
    if (NULL == definitions) {
      if (OB_FAIL(set_generated_name(partition, i, FIRST_PART, is_oracle_mode))) {
        LOG_WARN("failed to restore generated partition name", K(ret), K(i));
      }
    } else if (OB_FAIL(array_object(*definitions, i, definition))) {
      LOG_WARN("failed to read partition definition", K(ret), K(i));
    } else if (OB_FAIL(restore_definition(allocator, *definition, part_type,
                                          bound_width, is_oracle_mode, partition))) {
      LOG_WARN("failed to restore partition definition", K(ret), K(i));
    }
    if (OB_SUCC(ret) && individual_subparts) {
      const ObJsonObject *subpartitions = NULL;
      if (OB_FAIL(required_object(definition->get_value(ObString::make_string(SUBPARTITIONS)),
                                  subpartitions))) {
        LOG_WARN("individual subpartition rules are missing", K(ret), K(i));
      } else if (OB_FAIL(restore_individual_subpartitions(allocator, *subpartitions,
                                                          subpart_type, subpart_bound_width,
                                                          is_oracle_mode, partition))) {
        LOG_WARN("failed to restore individual subpartitions", K(ret), K(i));
      } else if (partition.get_subpartition_num() > max_count - physical_subpartitions) {
        ret = OB_STATE_NOT_MATCH;
      } else {
        physical_subpartitions += partition.get_subpartition_num();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_schema.check_part_name(partition))) {
        LOG_WARN("duplicate partition name in Unified Catalog metadata", K(ret), K(i));
      } else if (OB_FAIL(table_schema.add_partition(partition))) {
        LOG_WARN("failed to add restored partition", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    table_schema.set_part_num(count);
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::restore_template_layout(ObIAllocator &allocator,
                                                                const ObJsonObject &layout,
                                                                const ObPartitionFuncType part_type,
                                                                const int64_t bound_width,
                                                                const bool is_oracle_mode,
                                                                ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *count_node = layout.get_value(ObString::make_string(LAYOUT_TEMPLATE_COUNT));
  const ObJsonNode *definitions_node
      = layout.get_value(ObString::make_string(LAYOUT_TEMPLATE_DEFINITIONS));
  const ObJsonNode *definitions = NULL;
  int64_t count = 0;
  if ((NULL == count_node) == (NULL == definitions_node)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (NULL != count_node) {
    const ObJsonObject *container = NULL;
    if (!is_hash_like_part(part_type)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(required_object(count_node, container))
               || OB_FAIL(required_integer(*container, COUNT, count))) {
      LOG_WARN("subpartition template count is invalid", K(ret));
    }
  } else {
    const ObJsonObject *container = NULL;
    if (OB_FAIL(required_object(definitions_node, container))
        || OB_FAIL(required_array(*container, DEFINITIONS, definitions))) {
      LOG_WARN("subpartition template definitions are missing", K(ret));
    } else {
      count = definitions->element_count();
    }
  }
  const int64_t max_count = is_oracle_mode ? OB_MAX_PARTITION_NUM_ORACLE
                                            : OB_MAX_PARTITION_NUM_MYSQL;
  if (OB_SUCC(ret) && (count <= 0 || count > max_count
                       || count > max_count / table_schema.get_partition_num())) {
    ret = OB_STATE_NOT_MATCH;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObSubPartition subpart(&allocator);
    subpart.set_tenant_id(table_schema.get_tenant_id());
    subpart.set_table_id(table_schema.get_table_id());
    subpart.set_part_id(ObSubPartition::TEMPLATE_PART_ID);
    subpart.set_sub_part_id(i);
    subpart.set_sub_part_idx(i);
    subpart.set_part_idx(i);
    if (NULL == definitions) {
      if (OB_FAIL(set_generated_name(subpart, i, TEMPLATE_SUB_PART, is_oracle_mode))) {
        LOG_WARN("failed to restore generated template name", K(ret), K(i));
      }
    } else {
      const ObJsonObject *definition = NULL;
      if (OB_FAIL(array_object(*definitions, i, definition))) {
        LOG_WARN("failed to read subpartition template", K(ret), K(i));
      } else if (OB_FAIL(restore_definition(allocator, *definition, part_type,
                                            bound_width, is_oracle_mode, subpart))) {
        LOG_WARN("failed to restore subpartition template", K(ret), K(i));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < table_schema.get_def_subpartition_num(); ++j) {
      if (ObCharset::case_insensitive_equal(
              subpart.get_part_name(), table_schema.get_def_subpart_array()[j]->get_part_name())) {
        ret = OB_STATE_NOT_MATCH;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(table_schema.add_def_subpartition(subpart))) {
      LOG_WARN("failed to add subpartition template", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    bool generated = false;
    table_schema.set_def_sub_part_num(count);
    table_schema.set_sub_part_template_def_valid();
    if (OB_FAIL(table_schema.try_generate_subpart_by_template(generated))) {
      LOG_WARN("failed to materialize subpartition template", K(ret));
    } else if (!generated) {
      ret = OB_STATE_NOT_MATCH;
    }
  }
  return ret;
}

int ObUnifiedCatalogPartitionSpecCodec::restore(ObIAllocator &allocator,
                                                const ObJsonNode *partition_spec,
                                                ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (NULL != partition_spec) {
    const ObJsonObject *root = NULL;
    const ObJsonObject *layout = NULL;
    const ObJsonObject *subpartitioning = NULL;
    const ObJsonObject *sublayout = NULL;
    ObPartitionFuncType part_type = PARTITION_FUNC_TYPE_MAX;
    ObPartitionFuncType subpart_type = PARTITION_FUNC_TYPE_MAX;
    int64_t part_bound_width = 0;
    int64_t subpart_bound_width = 0;
    int64_t version = 0;
    int64_t level = 0;
    bool is_oracle_mode = false;
    bool individual_subparts = false;
    if (OB_FAIL(required_object(partition_spec, root))
        || OB_FAIL(required_integer(*root, SHOW_CREATE_VERSION_FIELD, version))
        || OB_FAIL(required_integer(*root, LEVEL_FIELD, level))) {
      LOG_WARN("Unified Catalog partition spec is incomplete; re-SYNC is required", K(ret));
    } else if (version != SHOW_CREATE_VERSION
               || (level != PARTITION_LEVEL_ONE && level != PARTITION_LEVEL_TWO)) {
      ret = OB_NOT_SUPPORTED;
    } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("failed to read partition name mode", K(ret));
    } else if (OB_FAIL(restore_key(*root, false, table_schema,
                                    part_type, part_bound_width))
               || OB_FAIL(required_object(root->get_value(ObString::make_string(LAYOUT_FIELD)),
                                          layout))) {
      LOG_WARN("failed to restore partition rule", K(ret));
    } else if (PARTITION_LEVEL_TWO == level) {
      if (OB_FAIL(required_object(
              root->get_value(ObString::make_string(SUBPARTITIONING_FIELD)), subpartitioning))
          || OB_FAIL(restore_key(*subpartitioning, true, table_schema,
                                  subpart_type, subpart_bound_width))
          || OB_FAIL(required_object(
              subpartitioning->get_value(ObString::make_string(LAYOUT_FIELD)), sublayout))) {
        LOG_WARN("failed to restore subpartition rule", K(ret));
      } else {
        individual_subparts
            = NULL != sublayout->get_value(ObString::make_string(LAYOUT_INDIVIDUAL));
        const bool template_count
            = NULL != sublayout->get_value(ObString::make_string(LAYOUT_TEMPLATE_COUNT));
        const bool template_definitions
            = NULL != sublayout->get_value(ObString::make_string(LAYOUT_TEMPLATE_DEFINITIONS));
        if (static_cast<int>(individual_subparts) + static_cast<int>(template_count)
                + static_cast<int>(template_definitions) != 1) {
          ret = OB_STATE_NOT_MATCH;
        } else if (individual_subparts) {
          const ObJsonObject *individual = NULL;
          int64_t default_count = 0;
          if (OB_FAIL(required_object(
                  sublayout->get_value(ObString::make_string(LAYOUT_INDIVIDUAL)), individual))
              || OB_FAIL(required_integer(*individual,
                                          LAYOUT_DEFAULT_GENERATED_COUNT, default_count))) {
            LOG_WARN("individual subpartition default count is missing", K(ret));
          } else if (default_count <= 0 || default_count > OB_MAX_PARTITION_NUM_ORACLE) {
            ret = OB_STATE_NOT_MATCH;
          } else {
            table_schema.set_def_sub_part_num(default_count);
          }
        }
      }
    } else if (NULL != root->get_value(ObString::make_string(SUBPARTITIONING_FIELD))) {
      ret = OB_STATE_NOT_MATCH;
    }
    if (OB_SUCC(ret)) {
      table_schema.set_part_level(static_cast<ObPartitionLevel>(level));
      if (OB_FAIL(restore_layout(allocator, *layout, part_type, part_bound_width,
                                  is_oracle_mode, individual_subparts, subpart_type,
                                  subpart_bound_width, table_schema))) {
        LOG_WARN("failed to restore Unified Catalog partition layout", K(ret));
      } else if (PARTITION_LEVEL_TWO == level && !individual_subparts
                 && OB_FAIL(restore_template_layout(allocator, *sublayout, subpart_type,
                                                     subpart_bound_width, is_oracle_mode,
                                                     table_schema))) {
        LOG_WARN("failed to restore Unified Catalog subpartition template", K(ret));
      } else if (OB_FAIL(validate(allocator, table_schema, partition_spec))) {
        LOG_WARN("typed partition metadata differs from source SQL representation", K(ret));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
