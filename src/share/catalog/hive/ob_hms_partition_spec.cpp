/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/hive/ob_hms_partition_spec.h"
#include "share/catalog/hive/ob_hms_partition_spec_json_serializer.h"

#include "lib/json_type/ob_json_tree.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
using namespace common;

namespace share
{

ObHmsPartitionDefinition::ObHmsPartitionDefinition()
    : name_(), values_(), storage_cache_policy_(), subpartitions_()
{
}

ObHmsPartitionDefinition::~ObHmsPartitionDefinition()
{
  if (subpartitions_.is_set_) {
    ObIArray<ObHmsPartitionDefinition *> &definitions
        = subpartitions_.value_.definitions_;
    for (int64_t i = 0; i < definitions.count(); ++i) {
      if (OB_NOT_NULL(definitions.at(i))) {
        definitions.at(i)->~ObHmsPartitionDefinition();
      }
    }
  }
}

ObHmsPartitionLayout::ObHmsPartitionLayout()
    : generated_count_(),
      definitions_(),
      template_count_(),
      template_definitions_(),
      individual_()
{
}

ObHmsPartitionLayout::~ObHmsPartitionLayout()
{
  if (definitions_.is_set_) {
    ObIArray<ObHmsPartitionDefinition *> &definitions = definitions_.value_;
    for (int64_t i = 0; i < definitions.count(); ++i) {
      if (OB_NOT_NULL(definitions.at(i))) {
        definitions.at(i)->~ObHmsPartitionDefinition();
      }
    }
  }
  if (template_definitions_.is_set_) {
    ObIArray<ObHmsPartitionDefinition *> &definitions
        = template_definitions_.value_.definitions_;
    for (int64_t i = 0; i < definitions.count(); ++i) {
      if (OB_NOT_NULL(definitions.at(i))) {
        definitions.at(i)->~ObHmsPartitionDefinition();
      }
    }
  }
}

ObHmsPartitionSpec::ObHmsPartitionSpec()
    : method_(), key_(), layout_(), subpartitioning_()
{
}

int ObHmsPartitionSpecJsonSerializer::alloc_object(
    ObIAllocator &allocator, ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  json = OB_NEWx(ObJsonObject, &allocator, &allocator);
  if (OB_ISNULL(json)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate partition spec json object", K(ret));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::alloc_array(
    ObIAllocator &allocator, ObJsonArray *&json)
{
  int ret = OB_SUCCESS;
  json = OB_NEWx(ObJsonArray, &allocator, &allocator);
  if (OB_ISNULL(json)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate partition spec json array", K(ret));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::add_string(
    ObIAllocator &allocator,
    ObJsonObject &json,
    const char *key,
    const ObString &value)
{
  int ret = OB_SUCCESS;
  ObJsonString *node = OB_NEWx(ObJsonString, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate partition spec json string", K(ret), K(key));
  } else if (OB_FAIL(json.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add partition spec json string", K(ret), K(key));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::add_integer(
    ObIAllocator &allocator,
    ObJsonObject &json,
    const char *key,
    const int64_t value)
{
  int ret = OB_SUCCESS;
  ObJsonInt *node = OB_NEWx(ObJsonInt, &allocator, value);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate partition spec json integer", K(ret), K(key));
  } else if (OB_FAIL(json.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add partition spec json integer", K(ret), K(key));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::add_empty_object(
    ObIAllocator &allocator,
    ObJsonObject &json,
    const char *key)
{
  int ret = OB_SUCCESS;
  ObJsonObject *node = nullptr;
  if (OB_FAIL(alloc_object(allocator, node))) {
    LOG_WARN("failed to allocate empty partition spec json object", K(ret), K(key));
  } else if (OB_FAIL(json.object_add(ObString::make_string(key), node))) {
    LOG_WARN("failed to add empty partition spec json object", K(ret), K(key));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_partition_value(
    ObIAllocator &allocator,
    const ObHmsPartitionValue &value,
    ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  if (value.sql_.is_set_ == value.max_value_.is_set_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition value must contain exactly one alternative", K(ret), K(value));
  } else if (OB_FAIL(alloc_object(allocator, json))) {
    LOG_WARN("failed to allocate partition value json", K(ret));
  } else if (value.sql_.is_set_) {
    if (OB_FAIL(add_string(allocator, *json, "sql", value.sql_.value_))) {
      LOG_WARN("failed to add partition sql", K(ret));
    }
  } else if (OB_FAIL(add_empty_object(allocator, *json, "max_value"))) {
    LOG_WARN("failed to add max value", K(ret));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_partition_value_list(
    ObIAllocator &allocator,
    const ObIArray<ObHmsPartitionValue> &value_list,
    ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  ObJsonArray *values = nullptr;
  if (OB_FAIL(alloc_object(allocator, json))) {
    LOG_WARN("failed to allocate partition value list json", K(ret));
  } else if (OB_FAIL(alloc_array(allocator, values))) {
    LOG_WARN("failed to allocate partition value array", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value_list.count(); ++i) {
    ObJsonObject *value = nullptr;
    if (OB_FAIL(build_partition_value(allocator, value_list.at(i), value))) {
      LOG_WARN("failed to serialize partition value", K(ret), K(i));
    } else if (OB_FAIL(values->array_append(value))) {
      LOG_WARN("failed to append partition value", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(json->object_add(ObString("values"), values))) {
    LOG_WARN("failed to add partition values", K(ret));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_list_tuple(
    ObIAllocator &allocator,
    const ObHmsListTuple &tuple,
    ObJsonObject *&json)
{
  int ret = build_partition_value_list(allocator, tuple.values_, json);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to serialize list tuple", K(ret));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_range_bound(
    ObIAllocator &allocator,
    const ObHmsRangeBound &range,
    ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  ObJsonObject *less_than = nullptr;
  if (OB_FAIL(alloc_object(allocator, json))) {
    LOG_WARN("failed to allocate range bound json", K(ret));
  } else if (OB_FAIL(build_partition_value_list(
                 allocator, range.less_than_.values_, less_than))) {
    LOG_WARN("failed to serialize less than values", K(ret));
  } else if (OB_FAIL(json->object_add(ObString("less_than"), less_than))) {
    LOG_WARN("failed to add less than values", K(ret));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_list_bound(
    ObIAllocator &allocator,
    const ObHmsListBound &list,
    ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  if (list.default_partition_.is_set_ == list.tuples_.is_set_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("list bound must contain exactly one alternative", K(ret));
  } else if (OB_FAIL(alloc_object(allocator, json))) {
    LOG_WARN("failed to allocate list bound json", K(ret));
  } else if (list.default_partition_.is_set_) {
    if (OB_FAIL(add_empty_object(allocator, *json, "default_partition"))) {
      LOG_WARN("failed to add default list partition", K(ret));
    }
  } else {
    ObJsonArray *tuples = nullptr;
    if (OB_FAIL(alloc_array(allocator, tuples))) {
      LOG_WARN("failed to allocate list tuple array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < list.tuples_.value_.count(); ++i) {
      ObJsonObject *tuple = nullptr;
      if (OB_FAIL(build_list_tuple(allocator, list.tuples_.value_.at(i), tuple))) {
        LOG_WARN("failed to serialize list tuple", K(ret), K(i));
      } else if (OB_FAIL(tuples->array_append(tuple))) {
        LOG_WARN("failed to append list tuple", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(json->object_add(ObString("tuples"), tuples))) {
      LOG_WARN("failed to add list tuples", K(ret));
    }
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_bound_values(
    ObIAllocator &allocator,
    const ObHmsPartitionBoundValues &values,
    ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  if (values.range_.is_set_ == values.list_.is_set_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition values must contain exactly one alternative", K(ret));
  } else if (OB_FAIL(alloc_object(allocator, json))) {
    LOG_WARN("failed to allocate partition values json", K(ret));
  } else {
    ObJsonObject *bound = nullptr;
    if (values.range_.is_set_) {
      if (OB_FAIL(build_range_bound(allocator, values.range_.value_, bound))) {
        LOG_WARN("failed to serialize range bound", K(ret));
      } else if (OB_FAIL(json->object_add(ObString("range"), bound))) {
        LOG_WARN("failed to add range bound", K(ret));
      }
    } else if (OB_FAIL(build_list_bound(allocator, values.list_.value_, bound))) {
      LOG_WARN("failed to serialize list bound", K(ret));
    } else if (OB_FAIL(json->object_add(ObString("list"), bound))) {
      LOG_WARN("failed to add list bound", K(ret));
    }
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_partition_key(
    ObIAllocator &allocator,
    const ObHmsPartitionKey &key,
    ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  const int64_t alternative_count = static_cast<int64_t>(key.implicit_primary_key_.is_set_)
                                    + static_cast<int64_t>(key.columns_.is_set_)
                                    + static_cast<int64_t>(key.expression_.is_set_);
  if (1 != alternative_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition key must contain exactly one alternative", K(ret));
  } else if (OB_FAIL(alloc_object(allocator, json))) {
    LOG_WARN("failed to allocate partition key json", K(ret));
  } else if (key.implicit_primary_key_.is_set_) {
    if (OB_FAIL(add_empty_object(allocator, *json, "implicit_primary_key"))) {
      LOG_WARN("failed to add implicit primary key", K(ret));
    }
  } else if (key.expression_.is_set_) {
    if (OB_FAIL(add_string(allocator, *json, "expression", key.expression_.value_))) {
      LOG_WARN("failed to add partition expression", K(ret));
    }
  } else {
    ObJsonArray *columns = nullptr;
    if (OB_FAIL(alloc_array(allocator, columns))) {
      LOG_WARN("failed to allocate partition column array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < key.columns_.value_.count(); ++i) {
      ObJsonString *column
          = OB_NEWx(ObJsonString, &allocator, key.columns_.value_.at(i));
      if (OB_ISNULL(column)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate partition column", K(ret), K(i));
      } else if (OB_FAIL(columns->array_append(column))) {
        LOG_WARN("failed to append partition column", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(json->object_add(ObString("columns"), columns))) {
      LOG_WARN("failed to add partition columns", K(ret));
    }
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_definition_array(
    ObIAllocator &allocator,
    const ObIArray<ObHmsPartitionDefinition *> &definitions,
    ObJsonArray *&json)
{
  int ret = alloc_array(allocator, json);
  for (int64_t i = 0; OB_SUCC(ret) && i < definitions.count(); ++i) {
    ObJsonObject *definition = nullptr;
    if (OB_ISNULL(definitions.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition definition is null", K(ret), K(i));
    } else if (OB_FAIL(build_partition_definition(allocator, *definitions.at(i), definition))) {
      LOG_WARN("failed to serialize partition definition", K(ret), K(i));
    } else if (OB_FAIL(json->array_append(definition))) {
      LOG_WARN("failed to append partition definition", K(ret), K(i));
    }
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_definition_container(
    ObIAllocator &allocator,
    const ObHmsPartitionDefinitions &definitions,
    ObJsonObject *&json)
{
  int ret = OB_SUCCESS;
  ObJsonArray *definition_array = nullptr;
  if (OB_FAIL(alloc_object(allocator, json))) {
    LOG_WARN("failed to allocate definition container", K(ret));
  } else if (OB_FAIL(build_definition_array(
                 allocator, definitions.definitions_, definition_array))) {
    LOG_WARN("failed to serialize definition array", K(ret));
  } else if (OB_FAIL(json->object_add(ObString("definitions"), definition_array))) {
    LOG_WARN("failed to add definition array", K(ret));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_partition_definition(
    ObIAllocator &allocator,
    const ObHmsPartitionDefinition &definition,
    ObJsonObject *&json)
{
  int ret = alloc_object(allocator, json);
  if (OB_SUCC(ret) && OB_FAIL(add_string(allocator, *json, "name", definition.name_))) {
    LOG_WARN("failed to add partition name", K(ret));
  }
  if (OB_SUCC(ret) && definition.values_.is_set_) {
    ObJsonObject *values = nullptr;
    if (OB_FAIL(build_bound_values(allocator, definition.values_.value_, values))) {
      LOG_WARN("failed to serialize partition values", K(ret));
    } else if (OB_FAIL(json->object_add(ObString("values"), values))) {
      LOG_WARN("failed to add partition values", K(ret));
    }
  }
  if (OB_SUCC(ret) && definition.storage_cache_policy_.is_set_) {
    if (OB_FAIL(add_string(allocator,
                           *json,
                           "storage_cache_policy",
                           definition.storage_cache_policy_.value_))) {
      LOG_WARN("failed to add storage cache policy", K(ret));
    }
  }
  if (OB_SUCC(ret) && definition.subpartitions_.is_set_) {
    ObJsonObject *subpartitions = nullptr;
    if (OB_FAIL(build_definition_container(
            allocator, definition.subpartitions_.value_, subpartitions))) {
      LOG_WARN("failed to serialize subpartitions", K(ret));
    } else if (OB_FAIL(json->object_add(ObString("subpartitions"), subpartitions))) {
      LOG_WARN("failed to add subpartitions", K(ret));
    }
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_partition_layout(
    ObIAllocator &allocator,
    const ObHmsPartitionLayout &layout,
    ObJsonObject *&json)
{
  int ret = alloc_object(allocator, json);
  if (OB_SUCC(ret) && layout.generated_count_.is_set_
      && OB_FAIL(add_integer(
          allocator, *json, "generated_count", layout.generated_count_.value_))) {
    LOG_WARN("failed to add generated partition count", K(ret));
  }
  if (OB_SUCC(ret) && layout.definitions_.is_set_) {
    ObJsonArray *definitions = nullptr;
    if (OB_FAIL(build_definition_array(allocator, layout.definitions_.value_, definitions))) {
      LOG_WARN("failed to serialize layout definitions", K(ret));
    } else if (OB_FAIL(json->object_add(ObString("definitions"), definitions))) {
      LOG_WARN("failed to add layout definitions", K(ret));
    }
  }
  if (OB_SUCC(ret) && layout.template_count_.is_set_) {
    ObJsonObject *template_count = nullptr;
    if (OB_FAIL(alloc_object(allocator, template_count))) {
      LOG_WARN("failed to allocate template count", K(ret));
    } else if (OB_FAIL(add_integer(allocator,
                                   *template_count,
                                   "count",
                                   layout.template_count_.value_.count_))) {
      LOG_WARN("failed to add template count", K(ret));
    } else if (OB_FAIL(json->object_add(ObString("template_count"), template_count))) {
      LOG_WARN("failed to add template count object", K(ret));
    }
  }
  if (OB_SUCC(ret) && layout.template_definitions_.is_set_) {
    ObJsonObject *definitions = nullptr;
    if (OB_FAIL(build_definition_container(
            allocator, layout.template_definitions_.value_, definitions))) {
      LOG_WARN("failed to serialize template definitions", K(ret));
    } else if (OB_FAIL(json->object_add(ObString("template_definitions"), definitions))) {
      LOG_WARN("failed to add template definitions", K(ret));
    }
  }
  if (OB_SUCC(ret) && layout.individual_.is_set_) {
    ObJsonObject *individual = nullptr;
    if (OB_FAIL(alloc_object(allocator, individual))) {
      LOG_WARN("failed to allocate individual layout", K(ret));
    } else if (OB_FAIL(add_integer(
                   allocator,
                   *individual,
                   "default_generated_count",
                   layout.individual_.value_.default_generated_count_))) {
      LOG_WARN("failed to add individual default generated count", K(ret));
    } else if (OB_FAIL(json->object_add(ObString("individual"), individual))) {
      LOG_WARN("failed to add individual layout", K(ret));
    }
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_subpartitioning(
    ObIAllocator &allocator,
    const ObHmsSubpartitioning &subpartitioning,
    ObJsonObject *&json)
{
  int ret = alloc_object(allocator, json);
  ObJsonObject *key = nullptr;
  ObJsonObject *layout = nullptr;
  if (OB_SUCC(ret)
      && OB_FAIL(add_string(allocator, *json, "method", subpartitioning.method_))) {
    LOG_WARN("failed to add subpartition method", K(ret));
  } else if (OB_SUCC(ret)
             && OB_FAIL(build_partition_key(allocator, subpartitioning.key_, key))) {
    LOG_WARN("failed to serialize subpartition key", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(json->object_add(ObString("key"), key))) {
    LOG_WARN("failed to add subpartition key", K(ret));
  } else if (OB_SUCC(ret)
             && OB_FAIL(build_partition_layout(allocator, subpartitioning.layout_, layout))) {
    LOG_WARN("failed to serialize subpartition layout", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(json->object_add(ObString("layout"), layout))) {
    LOG_WARN("failed to add subpartition layout", K(ret));
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::build_partition_spec(
    ObIAllocator &allocator,
    const ObHmsPartitionSpec &spec,
    ObJsonObject *&json)
{
  int ret = alloc_object(allocator, json);
  ObJsonObject *key = nullptr;
  ObJsonObject *layout = nullptr;
  if (OB_SUCC(ret) && OB_FAIL(add_string(allocator, *json, "method", spec.method_))) {
    LOG_WARN("failed to add partition method", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(build_partition_key(allocator, spec.key_, key))) {
    LOG_WARN("failed to serialize partition key", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(json->object_add(ObString("key"), key))) {
    LOG_WARN("failed to add partition key", K(ret));
  } else if (OB_SUCC(ret)
             && OB_FAIL(build_partition_layout(allocator, spec.layout_, layout))) {
    LOG_WARN("failed to serialize partition layout", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(json->object_add(ObString("layout"), layout))) {
    LOG_WARN("failed to add partition layout", K(ret));
  }
  if (OB_SUCC(ret) && spec.subpartitioning_.is_set_) {
    ObJsonObject *subpartitioning = nullptr;
    if (OB_FAIL(build_subpartitioning(
            allocator, spec.subpartitioning_.value_, subpartitioning))) {
      LOG_WARN("failed to serialize subpartitioning", K(ret));
    } else if (OB_FAIL(
                   json->object_add(ObString("subpartitioning"), subpartitioning))) {
      LOG_WARN("failed to add subpartitioning", K(ret));
    }
  }
  return ret;
}

int ObHmsPartitionSpecJsonSerializer::serialize(
    ObIAllocator &allocator,
    const ObHmsPartitionSpec &spec,
    ObString &out)
{
  int ret = OB_SUCCESS;
  ObJsonObject *root = nullptr;
  if (OB_FAIL(build_partition_spec(allocator, spec, root))) {
    LOG_WARN("failed to build partition spec json", K(ret));
  } else {
    ObJsonBuffer buffer(&allocator);
    if (OB_FAIL(root->print(buffer, false))) {
      LOG_WARN("failed to print partition spec json", K(ret));
    } else {
      out = buffer.string();
    }
  }
  return ret;
}

int ObHmsPartitionSpec::to_json(ObIAllocator &allocator, ObString &out) const
{
  return ObHmsPartitionSpecJsonSerializer::serialize(allocator, *this, out);
}

} // namespace share
} // namespace oceanbase
