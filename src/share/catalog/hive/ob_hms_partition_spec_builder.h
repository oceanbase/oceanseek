/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_BUILDER_H_
#define OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_BUILDER_H_

#include "common/object/ob_obj_type.h"
#include "share/catalog/hive/ob_hms_partition_spec.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace share
{

class ObHmsPartitionSpecBuilder
{
public:
  static int build(common::ObIAllocator &allocator,
                   const share::schema::ObTableSchema &table_schema,
                   ObHmsPartitionSpec &spec);

private:
  static constexpr const char *HASH_PARTITION_METHOD = "HASH";
  static constexpr const char *KEY_PARTITION_METHOD = "KEY";
  static constexpr const char *RANGE_PARTITION_METHOD = "RANGE";
  static constexpr const char *LIST_PARTITION_METHOD = "LIST";

  static int build_partition_key(common::ObIAllocator &allocator,
                                 const share::schema::ObTableSchema &table_schema,
                                 ObHmsPartitionKey &key);
  static int build_subpartition_key(common::ObIAllocator &allocator,
                                    const share::schema::ObTableSchema &table_schema,
                                    ObHmsPartitionKey &key);
  static int build_partition_key_from_option(
      common::ObIAllocator &allocator,
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObPartitionOption &partition_option,
      const common::ObPartitionKeyInfo &partition_key_info,
      ObHmsPartitionKey &key);
  static int build_partition_layout(common::ObIAllocator &allocator,
                                    const share::schema::ObTableSchema &table_schema,
                                    ObHmsPartitionLayout &layout);
  static int build_subpartitioning(common::ObIAllocator &allocator,
                                   const share::schema::ObTableSchema &table_schema,
                                   ObHmsSubpartitioning &subpartitioning);
  static int build_partition_definition(common::ObIAllocator &allocator,
                                        const share::schema::ObBasePartition &partition,
                                        const share::schema::ObPartitionFuncType part_type,
                                        ObHmsPartitionDefinition &definition);
  static int build_partition_with_subpartitions(
      common::ObIAllocator &allocator,
      const share::schema::ObTableSchema &table_schema,
      const share::schema::ObPartition &partition,
      const share::schema::ObPartitionFuncType part_type,
      ObHmsPartitionDefinition &definition);
  static int build_partition_values(common::ObIAllocator &allocator,
                                    const share::schema::ObBasePartition &partition,
                                    const share::schema::ObPartitionFuncType part_type,
                                    ObHmsPartitionDefinition &definition);
  static int build_partition_value(common::ObIAllocator &allocator,
                                   const common::ObObj &value,
                                   ObHmsPartitionValue &out);
  static int get_partition_method(const share::schema::ObPartitionFuncType part_type,
                                  common::ObString &method);
  static bool is_generated_partition_name(const common::ObString &partition_name,
                                          const int64_t partition_index);
  static bool is_generated_individual_subpartition_name(
      const common::ObString &partition_name);

  ObHmsPartitionSpecBuilder() = delete;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PARTITION_SPEC_BUILDER_H_
