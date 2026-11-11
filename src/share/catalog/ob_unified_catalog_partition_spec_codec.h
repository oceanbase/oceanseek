/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SPEC_CODEC_H_
#define OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SPEC_CODEC_H_

#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObJsonArray;
class ObJsonNode;
class ObJsonObject;
class ObObj;
class ObRowkey;
class ObString;
}
namespace share
{
namespace schema
{
class ObTableSchema;
}

// Encodes DDL partition rules, names and typed bounds. Physical partition IDs and
// partition resource rows are not stored in Generic Table properties.
class ObUnifiedCatalogPartitionSpecCodec
{
public:
  // CREATE preflight verifies MySQL tenant mode before allowing unresolved templates.
  static int build(common::ObIAllocator &allocator,
                   const schema::ObTableSchema &table_schema,
                   common::ObJsonObject *&partition_spec,
                   const bool allow_unmaterialized_template = false);
  static int validate(common::ObIAllocator &allocator,
                      const schema::ObTableSchema &table_schema,
                      const common::ObJsonNode *remote_partition_spec);
  static int restore(common::ObIAllocator &allocator,
                     const common::ObJsonNode *partition_spec,
                     schema::ObTableSchema &table_schema);

private:
  static constexpr int64_t DEFAULT_GENERATED_SUBPARTITION_COUNT = 1;
  static constexpr int64_t SHOW_CREATE_VERSION = 1;
  static constexpr const char *SHOW_CREATE_VERSION_FIELD = "ob_show_create_version";
  static constexpr const char *LEVEL_FIELD = "level";
  static constexpr const char *METHOD_FIELD = "method";
  static constexpr const char *KEY_FIELD = "key";
  static constexpr const char *LAYOUT_FIELD = "layout";
  static constexpr const char *SUBPARTITIONING_FIELD = "subpartitioning";
  static constexpr const char *KEY_IMPLICIT_PRIMARY_KEY = "implicit_primary_key";
  static constexpr const char *KEY_COLUMNS = "columns";
  static constexpr const char *KEY_EXPRESSION = "expression";
  static constexpr const char *KEY_OB_PART_EXPR = "ob_part_expr";
  static constexpr const char *DEFINITION_NAME = "name";
  static constexpr const char *BOUND_HIGH_HEX = "b_high_bound_val";
  static constexpr const char *BOUND_LIST_HEX = "b_list_val";
  static constexpr const char *BOUND_MAX_VALUE = "max_value";
  static constexpr const char *BOUND_SQL = "sql";
  static constexpr const char *VALUES = "values";
  static constexpr const char *BOUND_LESS_THAN = "less_than";
  static constexpr const char *BOUND_RANGE = "range";
  static constexpr const char *BOUND_DEFAULT_PARTITION = "default_partition";
  static constexpr const char *BOUND_TUPLES = "tuples";
  static constexpr const char *BOUND_LIST = "list";
  static constexpr const char *COUNT = "count";
  static constexpr const char *DEFINITIONS = "definitions";
  static constexpr const char *LAYOUT_GENERATED_COUNT = "generated_count";
  static constexpr const char *SUBPARTITIONS = "subpartitions";
  static constexpr const char *LAYOUT_TEMPLATE_COUNT = "template_count";
  static constexpr const char *LAYOUT_TEMPLATE_DEFINITIONS = "template_definitions";
  static constexpr const char *LAYOUT_DEFAULT_GENERATED_COUNT = "default_generated_count";
  static constexpr const char *LAYOUT_INDIVIDUAL = "individual";
  static constexpr const char *METHOD_HASH = "HASH";
  static constexpr const char *METHOD_KEY = "KEY";
  static constexpr const char *METHOD_RANGE = "RANGE";
  static constexpr const char *METHOD_LIST = "LIST";

  static int check_table_partition_structure(const schema::ObTableSchema &table_schema,
                                             const bool allow_unmaterialized_template);
  static bool partition_position_less(const schema::ObPartition *lhs,
                                      const schema::ObPartition *rhs);
  static bool subpartition_position_less(const schema::ObSubPartition *lhs,
                                         const schema::ObSubPartition *rhs);
  static bool is_generated_hash_name(const common::ObString &name,
                                     const int64_t index,
                                     const schema::ObHashNameType type,
                                     const bool is_oracle_mode,
                                     const schema::ObPartition *parent = NULL);
  static int alloc_object(common::ObIAllocator &allocator, common::ObJsonObject *&object);
  static int alloc_array(common::ObIAllocator &allocator, common::ObJsonArray *&array);
  static int add_string(common::ObIAllocator &allocator,
                        common::ObJsonObject &object,
                        const char *key,
                        const common::ObString &value);
  static int add_integer(common::ObIAllocator &allocator,
                         common::ObJsonObject &object,
                         const char *key,
                         const int64_t value);
  static int add_empty_object(common::ObIAllocator &allocator,
                              common::ObJsonObject &object,
                              const char *key);
  static int get_method(const schema::ObPartitionFuncType part_type,
                        common::ObString &method);
  static int build_key(common::ObIAllocator &allocator,
                       const schema::ObTableSchema &table_schema,
                       const schema::ObPartitionOption &option,
                       const common::ObPartitionKeyInfo &key_info,
                       common::ObJsonObject *&key);
  static int build_value(common::ObIAllocator &allocator,
                         const common::ObObj &value,
                         common::ObJsonObject *&json);
  static int build_values(common::ObIAllocator &allocator,
                          const common::ObRowkey &rowkey,
                          common::ObJsonArray *&values);
  static int build_list_tuples(common::ObIAllocator &allocator,
                               const schema::ObBasePartition &partition,
                               common::ObJsonArray *&tuples);
  static int build_definition(common::ObIAllocator &allocator,
                              const schema::ObBasePartition &partition,
                              const schema::ObPartitionFuncType part_type,
                              common::ObJsonObject *&definition);
  static int build_parent_subpartitions(common::ObIAllocator &allocator,
                                       const schema::ObPartition &partition,
                                       const schema::ObPartitionFuncType subpart_type,
                                       const bool is_oracle_mode,
                                       common::ObJsonObject *&subpartitions);
  static int build_layout(common::ObIAllocator &allocator,
                          const schema::ObTableSchema &table_schema,
                          common::ObJsonObject *&layout,
                          const bool allow_unmaterialized_template);
  static int build_subpartitioning(common::ObIAllocator &allocator,
                                   const schema::ObTableSchema &table_schema,
                                   common::ObJsonObject *&subpartitioning,
                                   const bool allow_unmaterialized_template);
  static int required_object(const common::ObJsonNode *node,
                             const common::ObJsonObject *&object);
  static int required_array(const common::ObJsonObject &object,
                            const char *key,
                            const common::ObJsonNode *&array);
  static int required_string(const common::ObJsonObject &object,
                             const char *key,
                             common::ObString &value);
  static int required_integer(const common::ObJsonObject &object,
                              const char *key,
                              int64_t &value);
  static int array_object(const common::ObJsonNode &array,
                          const int64_t index,
                          const common::ObJsonObject *&object);
  static int decode_hex(common::ObIAllocator &allocator,
                        const common::ObString &hex,
                        const int64_t max_length,
                        char *&bytes,
                        int64_t &byte_length);
  static int set_generated_name(schema::ObBasePartition &partition,
                                const int64_t index,
                                const schema::ObHashNameType type,
                                const bool is_oracle_mode,
                                const schema::ObPartition *parent = NULL);
  static int restore_key(const common::ObJsonObject &source,
                         const bool is_subpartition,
                         schema::ObTableSchema &table_schema,
                         schema::ObPartitionFuncType &part_type,
                         int64_t &bound_width);
  static int restore_definition(common::ObIAllocator &allocator,
                                const common::ObJsonObject &source,
                                const schema::ObPartitionFuncType part_type,
                                const int64_t bound_width,
                                const bool is_oracle_mode,
                                schema::ObBasePartition &partition);
  static int restore_individual_subpartitions(common::ObIAllocator &allocator,
                                              const common::ObJsonObject &source,
                                              const schema::ObPartitionFuncType part_type,
                                              const int64_t bound_width,
                                              const bool is_oracle_mode,
                                              schema::ObPartition &parent);
  static int restore_layout(common::ObIAllocator &allocator,
                            const common::ObJsonObject &layout,
                            const schema::ObPartitionFuncType part_type,
                            const int64_t bound_width,
                            const bool is_oracle_mode,
                            const bool individual_subparts,
                            const schema::ObPartitionFuncType subpart_type,
                            const int64_t subpart_bound_width,
                            schema::ObTableSchema &table_schema);
  static int restore_template_layout(common::ObIAllocator &allocator,
                                     const common::ObJsonObject &layout,
                                     const schema::ObPartitionFuncType part_type,
                                     const int64_t bound_width,
                                     const bool is_oracle_mode,
                                     schema::ObTableSchema &table_schema);

  ObUnifiedCatalogPartitionSpecCodec() = delete;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_OB_UNIFIED_CATALOG_PARTITION_SPEC_CODEC_H_
