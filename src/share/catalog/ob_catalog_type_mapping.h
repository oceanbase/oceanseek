/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef _SHARE_CATALOG_OB_CATALOG_TYPE_MAPPING_H
#define _SHARE_CATALOG_OB_CATALOG_TYPE_MAPPING_H

#include "common/object/ob_obj_type.h"
#include "share/schema/ob_table_schema.h"
#include "share/catalog/hive/thrift/gen_cpp/hive_metastore_types.h"

#include <string>

namespace oceanbase
{
namespace share
{
struct CatalogPartitionValues;
class ObCatalogTypeMapping
{
public:
  static int build_hms_placeholder_table(
      const share::schema::ObTableSchema &ob_table,
      const common::ObString &db_name,
      const uint64_t tenant_id,
      Apache::Hadoop::Hive::Table &hms_table,
      Apache::Hadoop::Hive::SQLAllTableConstraints &hms_constraints);
  static int apply_hms_table_constraints(
      const Apache::Hadoop::Hive::SQLAllTableConstraints &hms_constraints,
      share::schema::ObTableSchema &table_schema);
  static int build_all_partition_values(
      const share::schema::ObTableSchema &table_schema,
      common::ObIArray<CatalogPartitionValues> &partition_values);
  static int build_hms_partition_from_values(
      const share::schema::ObTableSchema &ob_table,
      const CatalogPartitionValues &partition_values,
      const Apache::Hadoop::Hive::Table &hms_table,
      Apache::Hadoop::Hive::Partition &hms_partition);

private:
  // Encode the real OB column type into the HMS FieldSchema type string via ob_sql_type_str,
  // e.g. "int(10) unsigned" / "varchar(64)" / "enum('a','b')". The read-back side rebuilds the
  // exact OB type from this string (see ObHiveTableMetadata::setup_ob_native_column_type).
  static int build_ob_column_type_str(const share::schema::ObColumnSchemaV2 &col, std::string &out);
  static int build_hms_default_value(const share::schema::ObColumnSchemaV2 &col,
                                     std::string &default_value);
  static int apply_hms_default_value(const std::string &default_value,
                                     share::schema::ObColumnSchemaV2 &column);
  // Reverse of the backslash escaping applied by ObObj::print_varchar_literal
  // (mysql mode) when the write path builds the quoted string default value.
  // Unescapes \\ \0 \' \" \n \r \t back to their raw bytes.
  static int unescape_hms_string_literal(common::ObIAllocator &allocator,
                                         const char *src,
                                         const int64_t src_len,
                                         common::ObString &out);

  static int build_hms_partition_info(const share::schema::ObTableSchema &table_schema,
                                      Apache::Hadoop::Hive::Table &hms_table);
  static int add_hms_partition_key(const char *name, Apache::Hadoop::Hive::Table &hms_table);
  static int build_hms_partition(const share::schema::ObTableSchema &ob_table,
                                 const Apache::Hadoop::Hive::Table &hms_table,
                                 const share::schema::ObPartition &ob_partition,
                                 const share::schema::ObSubPartition *ob_subpartition,
                                 Apache::Hadoop::Hive::Partition &hms_partition);
  static int append_partition_values(
      const share::schema::ObPartition &partition,
      const share::schema::ObSubPartition *subpartition,
      common::ObIArray<CatalogPartitionValues> &partition_values);
  static int expand_partition_values(const share::schema::ObPartitionLevel part_level,
                                     const share::schema::ObPartition &partition,
                                     common::ObIArray<CatalogPartitionValues> &partition_values);
  static int build_hms_partition_location(const Apache::Hadoop::Hive::Table &hms_table,
                                          const common::ObString &partition_name,
                                          const common::ObString *subpartition_name,
                                          std::string &location);
  static void append_hms_path_value(const common::ObString &value, std::string &path);
};

} // namespace share
} // namespace oceanbase

#endif /* _SHARE_CATALOG_OB_CATALOG_TYPE_MAPPING_H */
