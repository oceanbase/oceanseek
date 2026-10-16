/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_JSON_SERIALIZER_H_
#define OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_JSON_SERIALIZER_H_

#include "share/catalog/hive/ob_hms_partition_spec.h"

namespace oceanbase
{
namespace common
{
class ObJsonArray;
class ObJsonObject;
}
namespace share
{

class ObHmsPartitionSpecJsonSerializer
{
public:
  static int serialize(common::ObIAllocator &allocator,
                       const ObHmsPartitionSpec &spec,
                       common::ObString &out);

private:
  static int alloc_object(common::ObIAllocator &allocator, common::ObJsonObject *&json);
  static int alloc_array(common::ObIAllocator &allocator, common::ObJsonArray *&json);
  static int add_string(common::ObIAllocator &allocator,
                        common::ObJsonObject &json,
                        const char *key,
                        const common::ObString &value);
  static int add_integer(common::ObIAllocator &allocator,
                         common::ObJsonObject &json,
                         const char *key,
                         const int64_t value);
  static int add_empty_object(common::ObIAllocator &allocator,
                              common::ObJsonObject &json,
                              const char *key);
  static int build_partition_value(common::ObIAllocator &allocator,
                                   const ObHmsPartitionValue &value,
                                   common::ObJsonObject *&json);
  static int build_partition_value_list(
      common::ObIAllocator &allocator,
      const common::ObIArray<ObHmsPartitionValue> &values,
      common::ObJsonObject *&json);
  static int build_list_tuple(common::ObIAllocator &allocator,
                              const ObHmsListTuple &tuple,
                              common::ObJsonObject *&json);
  static int build_range_bound(common::ObIAllocator &allocator,
                               const ObHmsRangeBound &range,
                               common::ObJsonObject *&json);
  static int build_list_bound(common::ObIAllocator &allocator,
                              const ObHmsListBound &list,
                              common::ObJsonObject *&json);
  static int build_bound_values(common::ObIAllocator &allocator,
                                const ObHmsPartitionBoundValues &values,
                                common::ObJsonObject *&json);
  static int build_partition_key(common::ObIAllocator &allocator,
                                 const ObHmsPartitionKey &key,
                                 common::ObJsonObject *&json);
  static int build_definition_array(
      common::ObIAllocator &allocator,
      const common::ObIArray<ObHmsPartitionDefinition *> &definitions,
      common::ObJsonArray *&json);
  static int build_definition_container(
      common::ObIAllocator &allocator,
      const ObHmsPartitionDefinitions &definitions,
      common::ObJsonObject *&json);
  static int build_partition_definition(common::ObIAllocator &allocator,
                                        const ObHmsPartitionDefinition &definition,
                                        common::ObJsonObject *&json);
  static int build_partition_layout(common::ObIAllocator &allocator,
                                    const ObHmsPartitionLayout &layout,
                                    common::ObJsonObject *&json);
  static int build_subpartitioning(common::ObIAllocator &allocator,
                                   const ObHmsSubpartitioning &subpartitioning,
                                   common::ObJsonObject *&json);
  static int build_partition_spec(common::ObIAllocator &allocator,
                                  const ObHmsPartitionSpec &spec,
                                  common::ObJsonObject *&json);

  ObHmsPartitionSpecJsonSerializer() = delete;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_JSON_SERIALIZER_H_
