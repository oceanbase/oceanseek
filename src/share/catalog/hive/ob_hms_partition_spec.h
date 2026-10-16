/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_H_
#define OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{

// This header is the readable contract for the JSON stored in the HMS
// "default-partition-spec" table parameter. The mapping is mechanical:
//   1. remove the trailing '_' from each field name to get the JSON key;
//   2. omit ObHmsJsonOptional fields when is_set_ is false;
//   3. serialize ObHmsJsonEmptyObject as {} and ObSEArray as a JSON array.
// Fields documented as alternatives must have exactly one alternative set.

template <typename T>
struct ObHmsJsonOptional
{
  ObHmsJsonOptional() : is_set_(false), value_() {}

  T &set()
  {
    is_set_ = true;
    return value_;
  }

  TO_STRING_KV(K_(is_set), K_(value));

  bool is_set_;
  T value_;
};

struct ObHmsJsonEmptyObject
{
  TO_STRING_EMPTY();
};

// JSON: {"sql":"..."} | {"max_value":{}}
struct ObHmsPartitionValue
{
  TO_STRING_KV(K_(sql), K_(max_value));

  ObHmsJsonOptional<common::ObString> sql_;
  ObHmsJsonOptional<ObHmsJsonEmptyObject> max_value_;
};

// JSON: {"values":[PartitionValue, ...]}
struct ObHmsPartitionValueList
{
  common::ObSEArray<ObHmsPartitionValue, 4> values_;
};

// JSON: {"less_than":{"values":[...]}}
struct ObHmsRangeBound
{
  ObHmsPartitionValueList less_than_;
};

// JSON: {"values":[PartitionValue, ...]}
struct ObHmsListTuple
{
  TO_STRING_KV(K_(values));

  common::ObSEArray<ObHmsPartitionValue, 4> values_;
};

// JSON: {"default_partition":{}} | {"tuples":[ListTuple, ...]}
struct ObHmsListBound
{
  ObHmsJsonOptional<ObHmsJsonEmptyObject> default_partition_;
  ObHmsJsonOptional<common::ObSEArray<ObHmsListTuple, 4>> tuples_;
};

// JSON: {"range":RangeBound} | {"list":ListBound}
struct ObHmsPartitionBoundValues
{
  ObHmsJsonOptional<ObHmsRangeBound> range_;
  ObHmsJsonOptional<ObHmsListBound> list_;
};

// JSON: {"implicit_primary_key":{}} | {"columns":[...]} | {"expression":"..."}
struct ObHmsPartitionKey
{
  ObHmsJsonOptional<ObHmsJsonEmptyObject> implicit_primary_key_;
  ObHmsJsonOptional<common::ObSEArray<common::ObString, 4>> columns_;
  ObHmsJsonOptional<common::ObString> expression_;
};

struct ObHmsPartitionDefinition;

// JSON: {"definitions":[PartitionDefinition, ...]}
struct ObHmsPartitionDefinitions
{
  common::ObSEArray<ObHmsPartitionDefinition *, 4> definitions_;
};

struct ObHmsPartitionDefinition
{
  ObHmsPartitionDefinition();
  ~ObHmsPartitionDefinition();

  // ObIArray<T *>::to_string dereferences T. Keep this deliberately non-recursive.
  TO_STRING_KV(K_(name));

  common::ObString name_;
  ObHmsJsonOptional<ObHmsPartitionBoundValues> values_;
  ObHmsJsonOptional<common::ObString> storage_cache_policy_;
  ObHmsJsonOptional<ObHmsPartitionDefinitions> subpartitions_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHmsPartitionDefinition);
};

// JSON: {"count":n}
struct ObHmsPartitionCount
{
  ObHmsPartitionCount() : count_(0) {}
  int64_t count_;
};

// JSON: {"default_generated_count":n}
struct ObHmsIndividualPartitionLayout
{
  ObHmsIndividualPartitionLayout() : default_generated_count_(0) {}
  int64_t default_generated_count_;
};

// Each optional field maps directly to the same-named JSON field. generated_count and
// definitions may coexist; template_count, template_definitions and individual are alternatives.
struct ObHmsPartitionLayout
{
  ObHmsPartitionLayout();
  ~ObHmsPartitionLayout();

  ObHmsJsonOptional<int64_t> generated_count_;
  ObHmsJsonOptional<common::ObSEArray<ObHmsPartitionDefinition *, 4>> definitions_;
  ObHmsJsonOptional<ObHmsPartitionCount> template_count_;
  ObHmsJsonOptional<ObHmsPartitionDefinitions> template_definitions_;
  ObHmsJsonOptional<ObHmsIndividualPartitionLayout> individual_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHmsPartitionLayout);
};

struct ObHmsSubpartitioning
{
  common::ObString method_;
  ObHmsPartitionKey key_;
  ObHmsPartitionLayout layout_;
};

struct ObHmsPartitionSpec
{
  ObHmsPartitionSpec();

  // The only public serialization entry point. The returned ObString is owned by allocator.
  int to_json(common::ObIAllocator &allocator, common::ObString &out) const;

  common::ObString method_;
  ObHmsPartitionKey key_;
  ObHmsPartitionLayout layout_;
  ObHmsJsonOptional<ObHmsSubpartitioning> subpartitioning_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_H_
