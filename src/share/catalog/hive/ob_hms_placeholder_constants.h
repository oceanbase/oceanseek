/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PLACEHOLDER_CONSTANTS_H_
#define OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PLACEHOLDER_CONSTANTS_H_

#include <stdint.h>

namespace oceanbase
{
namespace share
{

// Shared contract between the HMS placeholder writers and readers.
struct ObHmsPlaceholderConstants
{
  static constexpr const char *DEFAULT_PARTITION_SPEC_PARAMETER = "default-partition-spec";
  static constexpr const char *OB_TYPE_PARAMETER = "ob.type";
  static constexpr const char *OB_TENANT_ID_PARAMETER = "ob.tenant_id";
  static constexpr const char *OB_TABLE_ID_PARAMETER = "ob.table_id";
  static constexpr const char *OB_PARTITION_ID_PARAMETER = "ob.partition_id";
  static constexpr const char *OB_SUBPARTITION_ID_PARAMETER = "ob.subpartition_id";
  static constexpr const char *OB_SCHEMA_VERSION_PARAMETER = "ob.schema_version";
  static constexpr const char *COMMENT_PARAMETER = "comment";

  static constexpr const char *PLACEHOLDER_TABLE_MARKER = "placeholder";
  static constexpr const char *PLACEHOLDER_PARTITION_MARKER = "placeholder_partition";
  static constexpr const char *EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";

  static constexpr const char *TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
  static constexpr const char *TEXT_OUTPUT_FORMAT
      = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
  static constexpr const char *LAZY_SIMPLE_SERDE
      = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

  static constexpr const char *PARTITION_KEY_NAME = "partition";
  static constexpr const char *SUBPARTITION_KEY_NAME = "subpartition";
  static constexpr const char *PARTITION_KEY_TYPE = "string";
  static constexpr const char *PARTITION_KEY_COMMENT = "OceanBase physical partition name";
  static constexpr int64_t ONE_LEVEL_PARTITION_KEY_COUNT = 1;
  static constexpr int64_t TWO_LEVEL_PARTITION_KEY_COUNT = 2;

  static constexpr char PARTITION_PATH_SEPARATOR = '/';
  static constexpr char PARTITION_KEY_VALUE_SEPARATOR = '=';
  static constexpr char PATH_ESCAPE_PREFIX = '%';
  static constexpr const char *PATH_HEX_DIGITS = "0123456789ABCDEF";
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_CATALOG_HIVE_OB_HMS_PLACEHOLDER_CONSTANTS_H_
