/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/hive/ob_hms_partition_spec_builder.h"

#include "share/schema/ob_column_schema.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

#include <stdint.h>

namespace oceanbase
{
using namespace common;
using namespace share::schema;

namespace share
{

int ObHmsPartitionSpecBuilder::get_partition_method(const ObPartitionFuncType part_type,
                                                    ObString &method)
{
  int ret = OB_SUCCESS;
  if (PARTITION_FUNC_TYPE_HASH == part_type) {
    method = ObString::make_string(HASH_PARTITION_METHOD);
  } else if (PARTITION_FUNC_TYPE_KEY == part_type
             || PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type) {
    method = ObString::make_string(KEY_PARTITION_METHOD);
  } else if (PARTITION_FUNC_TYPE_RANGE == part_type
             || PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type) {
    method = ObString::make_string(RANGE_PARTITION_METHOD);
  } else if (PARTITION_FUNC_TYPE_LIST == part_type
             || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
    method = ObString::make_string(LIST_PARTITION_METHOD);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported partition function type", K(ret), K(part_type));
  }
  return ret;
}

bool ObHmsPartitionSpecBuilder::is_generated_partition_name(const ObString &partition_name,
                                                            const int64_t partition_index)
{
  bool is_generated = false;
  char generated_name[OB_MAX_PARTITION_NAME_LENGTH] = {0};
  int64_t pos = 0;
  if (OB_SUCCESS
      == databuff_printf(generated_name, sizeof(generated_name), pos, "p%ld", partition_index)) {
    is_generated
        = 0 == partition_name.case_compare(ObString(static_cast<int32_t>(pos), generated_name));
  }
  return is_generated;
}

bool ObHmsPartitionSpecBuilder::is_generated_individual_subpartition_name(const ObString &partition_name)
{
  bool is_generated = partition_name.length() > 1
                      && ('p' == partition_name.ptr()[0] || 'P' == partition_name.ptr()[0]);
  int64_t generated_id = 0;
  for (int64_t i = 1; is_generated && i < partition_name.length(); ++i) {
    const char ch = partition_name.ptr()[i];
    if (ch < '0' || ch > '9') {
      is_generated = false;
    } else {
      const int64_t digit = ch - '0';
      if (generated_id > (INT64_MAX - digit) / 10) {
        generated_id = INT64_MAX;
      } else {
        generated_id = generated_id * 10 + digit;
      }
    }
  }
  return is_generated && generated_id >= OB_MAX_PARTITION_NUM_MYSQL;
}

int ObHmsPartitionSpecBuilder::build_partition_value(ObIAllocator &allocator,
                                                     const ObObj &value,
                                                     ObHmsPartitionValue &out)
{
  int ret = OB_SUCCESS;
  if (value.is_max_value()) {
    out.max_value_.set();
  } else {
    char *buf = static_cast<char *>(allocator.alloc(OB_MAX_PARTITION_EXPR_LENGTH));
    int64_t pos = 0;
    ObObj copied_value = value;
    ObRowkey rowkey(&copied_value, 1);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate partition value buffer", K(ret));
    } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(false,
                                                                       rowkey,
                                                                       buf,
                                                                       OB_MAX_PARTITION_EXPR_LENGTH,
                                                                       pos,
                                                                       false,
                                                                       nullptr))) {
      LOG_WARN("failed to print partition value", K(ret), K(value));
    } else {
      out.sql_.set().assign_ptr(buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

int ObHmsPartitionSpecBuilder::build_partition_values(ObIAllocator &allocator,
                                                      const ObBasePartition &partition,
                                                      const ObPartitionFuncType part_type,
                                                      ObHmsPartitionDefinition &definition)
{
  int ret = OB_SUCCESS;
  if (is_range_part(part_type)) {
    const ObRowkey &rowkey = partition.get_high_bound_val();
    ObHmsRangeBound &range = definition.values_.set().range_.set();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); ++i) {
      ObHmsPartitionValue value;
      if (OB_FAIL(build_partition_value(allocator, rowkey.get_obj_ptr()[i], value))) {
        LOG_WARN("failed to build range value", K(ret), K(i));
      } else if (OB_FAIL(range.less_than_.values_.push_back(value))) {
        LOG_WARN("failed to append range value", K(ret), K(i));
      }
    }
  } else if (is_list_part(part_type)) {
    const ObIArray<ObNewRow> &rows = partition.get_list_row_values();
    const bool is_default
        = 1 == rows.count() && 1 == rows.at(0).get_count() && rows.at(0).get_cell(0).is_max_value();
    ObHmsListBound &list = definition.values_.set().list_.set();
    if (is_default) {
      list.default_partition_.set();
    } else {
      list.tuples_.set();
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_default && i < rows.count(); ++i) {
      const ObNewRow &row = rows.at(i);
      ObHmsListTuple tuple;
      for (int64_t j = 0; OB_SUCC(ret) && j < row.get_count(); ++j) {
        ObHmsPartitionValue value;
        if (OB_FAIL(build_partition_value(allocator, row.get_cell(j), value))) {
          LOG_WARN("failed to build list value", K(ret), K(i), K(j));
        } else if (OB_FAIL(tuple.values_.push_back(value))) {
          LOG_WARN("failed to append list value", K(ret), K(i), K(j));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(list.tuples_.value_.push_back(tuple))) {
        LOG_WARN("failed to append list tuple", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObHmsPartitionSpecBuilder::build_partition_definition(
    ObIAllocator &allocator,
    const ObBasePartition &partition,
    const ObPartitionFuncType part_type,
    ObHmsPartitionDefinition &definition)
{
  int ret = OB_SUCCESS;
  const storage::ObStorageCachePolicyType cache_policy
      = partition.get_part_storage_cache_policy_type();
  const char *cache_policy_str = nullptr;
  if (OB_FAIL(ob_write_string(allocator, partition.get_part_name(), definition.name_))) {
    LOG_WARN("failed to copy partition name", K(ret), K(partition.get_part_name()));
  } else if (OB_FAIL(build_partition_values(allocator, partition, part_type, definition))) {
    LOG_WARN("failed to build partition values", K(ret));
  } else if (storage::ObStorageCacheGlobalPolicy::is_valid(cache_policy)
             && OB_FAIL(storage::ObStorageCacheGlobalPolicy::safely_get_str(cache_policy,
                                                                            cache_policy_str))) {
    LOG_WARN("failed to get partition storage cache policy", K(ret), K(cache_policy));
  } else if (OB_NOT_NULL(cache_policy_str)
             && OB_FAIL(ob_write_string(allocator,
                                        ObString::make_string(cache_policy_str),
                                        definition.storage_cache_policy_.set()))) {
    LOG_WARN("failed to copy partition storage cache policy", K(ret), K(cache_policy));
  }
  return ret;
}

int ObHmsPartitionSpecBuilder::build_partition_with_subpartitions(
    ObIAllocator &allocator,
    const ObTableSchema &table_schema,
    const ObPartition &partition,
    const ObPartitionFuncType part_type,
    ObHmsPartitionDefinition &definition)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_partition_definition(allocator, partition, part_type, definition))) {
    LOG_WARN("failed to build partition definition", K(ret), K(partition.get_part_name()));
  } else {
    ObSubPartition **subpartitions = partition.get_subpart_array();
    const int64_t subpartition_num = partition.get_subpartition_num();
    for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num; ++i) {
      ObHmsPartitionDefinition *sub_definition = nullptr;
      if (OB_ISNULL(subpartitions) || OB_ISNULL(subpartitions[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("subpartition is null", K(ret), K(i));
      } else if (subpartitions[i]->is_empty_partition_name()
                 || is_generated_individual_subpartition_name(subpartitions[i]->get_part_name())) {
      } else if (OB_ISNULL(sub_definition = OB_NEWx(ObHmsPartitionDefinition, &allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate subpartition definition", K(ret), K(i));
      } else if (OB_FAIL(build_partition_definition(
                     allocator,
                     *subpartitions[i],
                     table_schema.get_sub_part_option().get_part_func_type(),
                     *sub_definition))) {
        LOG_WARN("failed to build subpartition definition", K(ret), K(i));
        sub_definition->~ObHmsPartitionDefinition();
      } else if (OB_FAIL(
                     definition.subpartitions_.set().definitions_.push_back(sub_definition))) {
        LOG_WARN("failed to append subpartition definition", K(ret), K(i));
        sub_definition->~ObHmsPartitionDefinition();
      }
    }
  }
  return ret;
}

int ObHmsPartitionSpecBuilder::build_partition_key_from_option(
    ObIAllocator &allocator,
    const ObTableSchema &table_schema,
    const ObPartitionOption &partition_option,
    const ObPartitionKeyInfo &partition_key_info,
    ObHmsPartitionKey &key)
{
  int ret = OB_SUCCESS;
  const ObPartitionFuncType part_type = partition_option.get_part_func_type();
  if (PARTITION_FUNC_TYPE_KEY_IMPLICIT == part_type) {
    key.implicit_primary_key_.set();
  } else if (PARTITION_FUNC_TYPE_KEY == part_type || PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type
             || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
    common::ObIArray<ObString> &columns = key.columns_.set();
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_key_info.get_size(); ++i) {
      ObRowkeyColumn rowkey_column;
      const ObColumnSchemaV2 *column = nullptr;
      ObString column_name;
      if (OB_FAIL(partition_key_info.get_column(i, rowkey_column))) {
        LOG_WARN("failed to get partition column", K(ret), K(i));
      } else if (OB_ISNULL(column = table_schema.get_column_schema(rowkey_column.column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition column is null", K(ret), K(rowkey_column.column_id_));
      } else if (OB_FAIL(ob_write_string(
                     allocator, column->get_column_name_str(), column_name))) {
        LOG_WARN("failed to copy partition column", K(ret), K(i));
      } else if (OB_FAIL(columns.push_back(column_name))) {
        LOG_WARN("failed to append partition column", K(ret), K(i));
      }
    }
  } else {
    if (OB_FAIL(ob_write_string(
            allocator, partition_option.get_part_func_expr_str(), key.expression_.set()))) {
      LOG_WARN("failed to copy partition expression", K(ret));
    }
  }
  return ret;
}

int ObHmsPartitionSpecBuilder::build_partition_key(ObIAllocator &allocator,
                                                   const ObTableSchema &table_schema,
                                                   ObHmsPartitionKey &key)
{
  return build_partition_key_from_option(allocator,
                                         table_schema,
                                         table_schema.get_part_option(),
                                         table_schema.get_partition_key_info(),
                                         key);
}

int ObHmsPartitionSpecBuilder::build_subpartition_key(ObIAllocator &allocator,
                                                      const ObTableSchema &table_schema,
                                                      ObHmsPartitionKey &key)
{
  return build_partition_key_from_option(allocator,
                                         table_schema,
                                         table_schema.get_sub_part_option(),
                                         table_schema.get_subpartition_key_info(),
                                         key);
}

int ObHmsPartitionSpecBuilder::build_partition_layout(ObIAllocator &allocator,
                                                      const ObTableSchema &table_schema,
                                                      ObHmsPartitionLayout &layout)
{
  int ret = OB_SUCCESS;
  const ObPartitionFuncType part_type = table_schema.get_part_option().get_part_func_type();
  ObPartition **partitions = table_schema.get_part_array();
  const int64_t partition_num = table_schema.get_partition_num();
  bool has_count = false;
  bool build_definitions = OB_NOT_NULL(partitions) && partition_num > 0;
  bool has_explicit_definition = false;
  if (is_hash_like_part(part_type)) {
    layout.generated_count_.set() = table_schema.get_part_option().get_part_num();
    has_count = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && has_count && build_definitions && i < partition_num; ++i) {
    if (OB_ISNULL(partitions[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret), K(i));
    } else if (!partitions[i]->is_empty_partition_name()
               && !is_generated_partition_name(partitions[i]->get_part_name(), i)) {
      has_explicit_definition = true;
    }
  }
  if (has_count) {
    build_definitions = build_definitions && has_explicit_definition;
  }
  const bool build_individual_subpartitions
      = PARTITION_LEVEL_TWO == table_schema.get_part_level()
        && 0 == table_schema.get_def_subpartition_num();
  for (int64_t i = 0; OB_SUCC(ret) && build_definitions && i < partition_num; ++i) {
    ObHmsPartitionDefinition *definition = nullptr;
    if (OB_ISNULL(partitions[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", K(ret), K(i));
    } else if (OB_ISNULL(definition = OB_NEWx(ObHmsPartitionDefinition, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate partition definition", K(ret), K(i));
    } else if (build_individual_subpartitions
               && OB_FAIL(build_partition_with_subpartitions(
                   allocator, table_schema, *partitions[i], part_type, *definition))) {
      LOG_WARN("failed to build partition with subpartitions", K(ret), K(i));
      definition->~ObHmsPartitionDefinition();
    } else if (!build_individual_subpartitions
               && OB_FAIL(build_partition_definition(
                   allocator, *partitions[i], part_type, *definition))) {
      LOG_WARN("failed to build partition definition", K(ret), K(i));
      definition->~ObHmsPartitionDefinition();
    } else if (OB_FAIL(layout.definitions_.set().push_back(definition))) {
      LOG_WARN("failed to append partition definition", K(ret), K(i));
      definition->~ObHmsPartitionDefinition();
    }
  }
  return ret;
}

int ObHmsPartitionSpecBuilder::build_subpartitioning(ObIAllocator &allocator,
                                                     const ObTableSchema &table_schema,
                                                     ObHmsSubpartitioning &subpartitioning)
{
  int ret = OB_SUCCESS;
  const ObPartitionFuncType part_type = table_schema.get_sub_part_option().get_part_func_type();
  const int64_t subpartition_num = table_schema.get_def_subpartition_num();
  ObSubPartition **subpartitions = table_schema.get_def_subpart_array();
  ObString method;
  if (OB_FAIL(get_partition_method(part_type, method))) {
    LOG_WARN("failed to get subpartition method", K(ret), K(part_type));
  } else if (OB_FAIL(ob_write_string(allocator, method, subpartitioning.method_))) {
    LOG_WARN("failed to copy subpartition method", K(ret));
  } else if (OB_FAIL(
                 build_subpartition_key(allocator, table_schema, subpartitioning.key_))) {
    LOG_WARN("failed to build subpartition key", K(ret));
  } else if (OB_NOT_NULL(subpartitions) && subpartition_num > 0) {
    bool is_count_template = is_hash_like_part(part_type);
    for (int64_t i = 0; is_count_template && i < subpartition_num; ++i) {
      is_count_template = OB_NOT_NULL(subpartitions[i])
                          && is_generated_partition_name(subpartitions[i]->get_part_name(), i);
    }
    if (is_count_template) {
      subpartitioning.layout_.template_count_.set().count_ = subpartition_num;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num; ++i) {
        ObHmsPartitionDefinition *definition = nullptr;
        if (OB_ISNULL(subpartitions[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("template subpartition is null", K(ret), K(i));
        } else if (OB_ISNULL(definition = OB_NEWx(ObHmsPartitionDefinition, &allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate template subpartition definition", K(ret), K(i));
        } else if (OB_FAIL(build_partition_definition(allocator,
                                                      *subpartitions[i],
                                                      part_type,
                                                      *definition))) {
          LOG_WARN("failed to build template subpartition", K(ret), K(i));
          definition->~ObHmsPartitionDefinition();
        } else if (OB_FAIL(subpartitioning.layout_.template_definitions_
                                      .set()
                                      .definitions_
                                      .push_back(definition))) {
          LOG_WARN("failed to append template subpartition", K(ret), K(i));
          definition->~ObHmsPartitionDefinition();
        }
      }
    }
  } else {
    int64_t default_count = table_schema.get_sub_part_option().get_part_num();
    if (default_count <= 0) {
      default_count = 1;
    }
    subpartitioning.layout_.individual_.set().default_generated_count_ = default_count;
  }
  return ret;
}

int ObHmsPartitionSpecBuilder::build(ObIAllocator &allocator,
                                     const ObTableSchema &table_schema,
                                     ObHmsPartitionSpec &spec)
{
  int ret = OB_SUCCESS;
  const ObPartitionFuncType part_type = table_schema.get_part_option().get_part_func_type();
  ObString method;
  if (OB_FAIL(get_partition_method(part_type, method))) {
    LOG_WARN("failed to get partition method", K(ret), K(part_type));
  } else if (OB_FAIL(ob_write_string(allocator, method, spec.method_))) {
    LOG_WARN("failed to copy partition method", K(ret));
  } else if (OB_FAIL(build_partition_key(allocator, table_schema, spec.key_))) {
    LOG_WARN("failed to build partition key", K(ret));
  } else if (OB_FAIL(build_partition_layout(allocator, table_schema, spec.layout_))) {
    LOG_WARN("failed to build partition layout", K(ret));
  } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
    if (OB_FAIL(build_subpartitioning(
            allocator, table_schema, spec.subpartitioning_.set()))) {
      LOG_WARN("failed to build subpartitioning", K(ret));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
