/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SCHEMA_EXTERNAL_TABLE_COLUMN_SCHEMA_HELPER_H_
#define OCEANBASE_SCHEMA_EXTERNAL_TABLE_COLUMN_SCHEMA_HELPER_H_
#include "ob_column_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObExternalTableColumnSchemaHelper
{
public:
  static void set_column_accuracy(ObCompatibilityMode mode, ObObjType accuracy_type, ObColumnSchemaV2 &column_schema);
  static int setup_bool(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_tinyint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_utinyint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_smallint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_usmallint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_mediumint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_umediumint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_int(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_uint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_bigint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_ubigint(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_float(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_ufloat(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_double(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_udouble(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_decimal(const bool &is_oracle_mode,
                           const int16_t &precision,
                           const int16_t &scale,
                           ObColumnSchemaV2 &column_schema);
  static int setup_udecimal(const bool &is_oracle_mode,
                            const int16_t &precision,
                            const int16_t &scale,
                            ObColumnSchemaV2 &column_schema);
  static int setup_varchar(const bool &is_oracle_mode,
                           const int64_t &length,
                           ObCharsetType cs_type,
                           ObCollationType collation,
                           ObColumnSchemaV2 &column_schema);
  static int setup_char(const bool &is_oracle_mode,
                        const int64_t &length,
                        ObCharsetType cs_type,
                        ObCollationType collation,
                        ObColumnSchemaV2 &column_schema);
  static int setup_string(const bool &is_oracle_mode,
                          ObCharsetType cs_type,
                          ObCollationType collation,
                          ObColumnSchemaV2 &column_schema);
  static int setup_text(const bool &is_oracle_mode,
                        const ObObjType text_type,
                        ObCharsetType cs_type,
                        ObCollationType collation,
                        ObColumnSchemaV2 &column_schema);
  static int setup_timestamp(const bool &is_oracle_mode,
                             ObColumnSchemaV2 &column_schema);
  static int setup_datetime(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_date(const bool &is_oracle_mode,
                        ObColumnSchemaV2 &column_schema);
  static int setup_time(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_year(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_bit(const bool &is_oracle_mode,
                       const int64_t &precision,
                       ObColumnSchemaV2 &column_schema);
  static int setup_json(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_geometry(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_roaringbitmap(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_mysql_date(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_mysql_datetime(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_enum(const bool &is_oracle_mode,
                        const common::ObIArray<common::ObString> &type_info,
                        ObColumnSchemaV2 &column_schema);
  static int setup_set(const bool &is_oracle_mode,
                       const common::ObIArray<common::ObString> &type_info,
                       ObColumnSchemaV2 &column_schema);
  static int setup_timestamp_ns(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
  static int setup_timestamp_tz_ns(const bool &is_oracle_mode, ObColumnSchemaV2 &column_schema);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SCHEMA_EXTERNAL_TABLE_COLUMN_SCHEMA_HELPER_H_