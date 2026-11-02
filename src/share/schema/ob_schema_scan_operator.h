/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_SCAN_OPERATOR_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_SCAN_OPERATOR_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
namespace schema
{

enum class ObSchemaScanKind
{
  BY_TABLE_TYPE = 0,           // A: idx_table_type
  BY_DUPLICATE_SCOPE = 1,      // A: idx_duplicate_scope
  BY_DYNAMIC_PARTITION = 2,    // A: idx_has_dynamic_partition_policy
  BY_INDEX_TYPE = 3,           // A: idx_index_type
  BY_TABLEGROUP = 4,           // B: idx_tablegroup_id
  BY_TABLESPACE = 5,           // B: idx_tablespace_id
  BY_DATABASE = 6,             // C: idx_db_tb_name
  BY_TENANT = 7,               // C: primary key prefix
  BY_ENCRYPTED_TABLESPACE = 8, // A: two-step via tablespace
};

// Align BY_TABLEGROUP with ObSchemaMgr::get_table_schemas_in_tablegroup:
// only USER_TABLE / SYSTEM_TABLE / MySQL TMP_TABLE. AUX LOB / index inherit
// tablegroup_id after ALTER TABLE ... tablegroup, but must not be listed.
inline bool is_tablegroup_listed_table_type(const ObTableType table_type)
{
  return ObTableType::USER_TABLE == table_type
      || ObTableType::SYSTEM_TABLE == table_type
      || is_mysql_tmp_table(table_type);
}

enum ObSchemaScanSelectFlag
{
  OB_SCHEMA_SCAN_SELECT_TABLE_ID = 1LL << 0,
  OB_SCHEMA_SCAN_SELECT_TABLE_TYPE = 1LL << 1,
  OB_SCHEMA_SCAN_SELECT_INDEX_TYPE = 1LL << 2,
  OB_SCHEMA_SCAN_SELECT_TABLEGROUP_ID = 1LL << 3,
  OB_SCHEMA_SCAN_SELECT_DATABASE_ID = 1LL << 4,
  OB_SCHEMA_SCAN_SELECT_DATA_TABLE_ID = 1LL << 5,
  OB_SCHEMA_SCAN_SELECT_TABLESPACE_ID = 1LL << 6,
  OB_SCHEMA_SCAN_SELECT_DUPLICATE_SCOPE = 1LL << 7,
  OB_SCHEMA_SCAN_SELECT_HAS_DYNAMIC_PARTITION_POLICY = 1LL << 8,
  // Extended for ObSchemaLiteTableIterator (附6.1); kept out of ALL_LITE for snapshot compatibility.
  OB_SCHEMA_SCAN_SELECT_PART_LEVEL = 1LL << 9,
  OB_SCHEMA_SCAN_SELECT_TABLET_ID = 1LL << 10,
  OB_SCHEMA_SCAN_SELECT_TABLE_NAME = 1LL << 11,
  OB_SCHEMA_SCAN_SELECT_SESSION_ID = 1LL << 12,
  OB_SCHEMA_SCAN_SELECT_ENCRYPTION = 1LL << 13,
  OB_SCHEMA_SCAN_SELECT_INDEX_STATUS = 1LL << 14,
  OB_SCHEMA_SCAN_SELECT_SCHEMA_VERSION = 1LL << 15,
  OB_SCHEMA_SCAN_SELECT_OBJECT_STATUS = 1LL << 16,
  OB_SCHEMA_SCAN_SELECT_TABLE_MODE = 1LL << 17,
  // Extended for 附6.5.2 A/C (dup tablets / do_build_ / SCP / TTL). Kept out of ALL_LITE.
  OB_SCHEMA_SCAN_SELECT_DUPLICATE_READ_CONSISTENCY = 1LL << 18,
  OB_SCHEMA_SCAN_SELECT_STORAGE_CACHE_POLICY = 1LL << 19,
  OB_SCHEMA_SCAN_SELECT_TTL_DEFINITION = 1LL << 20,
  OB_SCHEMA_SCAN_SELECT_TTL_FLAG = 1LL << 21,
  OB_SCHEMA_SCAN_SELECT_KV_ATTRIBUTES = 1LL << 22,
  OB_SCHEMA_SCAN_SELECT_NEED_STR_ALLOCATOR =
      OB_SCHEMA_SCAN_SELECT_TABLE_NAME
      | OB_SCHEMA_SCAN_SELECT_ENCRYPTION
      | OB_SCHEMA_SCAN_SELECT_STORAGE_CACHE_POLICY
      | OB_SCHEMA_SCAN_SELECT_TTL_DEFINITION
      | OB_SCHEMA_SCAN_SELECT_TTL_FLAG
      | OB_SCHEMA_SCAN_SELECT_KV_ATTRIBUTES,
  OB_SCHEMA_SCAN_SELECT_ALL_LITE =
      OB_SCHEMA_SCAN_SELECT_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
      | OB_SCHEMA_SCAN_SELECT_INDEX_TYPE
      | OB_SCHEMA_SCAN_SELECT_TABLEGROUP_ID
      | OB_SCHEMA_SCAN_SELECT_DATABASE_ID
      | OB_SCHEMA_SCAN_SELECT_DATA_TABLE_ID
      | OB_SCHEMA_SCAN_SELECT_TABLESPACE_ID
      | OB_SCHEMA_SCAN_SELECT_DUPLICATE_SCOPE
      | OB_SCHEMA_SCAN_SELECT_HAS_DYNAMIC_PARTITION_POLICY,
};

// Lite table fields read from inner-table metadata.
// String fields are deep-copied into caller-provided allocator when the
// corresponding select flags are set; otherwise empty.
struct ObSchemaTableLiteRow
{
  ObSchemaTableLiteRow()
    : table_id_(common::OB_INVALID_ID),
      table_type_(MAX_TABLE_TYPE),
      index_type_(INDEX_TYPE_IS_NOT),
      tablegroup_id_(common::OB_INVALID_ID),
      database_id_(common::OB_INVALID_ID),
      data_table_id_(common::OB_INVALID_ID),
      tablespace_id_(common::OB_INVALID_ID),
      duplicate_scope_(0),
      has_dynamic_partition_policy_(false),
      part_level_(PARTITION_LEVEL_ZERO),
      tablet_id_(),
      table_name_(),
      session_id_(0),
      encryption_(),
      index_status_(INDEX_STATUS_UNAVAILABLE),
      schema_version_(common::OB_INVALID_VERSION),
      object_status_(ObObjectStatus::INVALID),
      table_mode_(0),
      duplicate_read_consistency_(0),
      storage_cache_policy_(),
      ttl_definition_(),
      ttl_flag_(),
      kv_attributes_()
  {}
  TO_STRING_KV(K_(table_id), K_(table_type), K_(index_type), K_(tablegroup_id),
               K_(database_id), K_(data_table_id), K_(tablespace_id),
               K_(duplicate_scope), K_(has_dynamic_partition_policy),
               K_(part_level), K_(tablet_id), K_(table_name), K_(session_id),
               K_(encryption), K_(index_status), K_(schema_version),
               K_(object_status), K_(table_mode), K_(duplicate_read_consistency),
               K_(storage_cache_policy), K_(ttl_definition), K_(ttl_flag),
               K_(kv_attributes));

  uint64_t table_id_;
  ObTableType table_type_;
  ObIndexType index_type_;
  uint64_t tablegroup_id_;
  uint64_t database_id_;
  uint64_t data_table_id_;
  uint64_t tablespace_id_;
  int64_t duplicate_scope_;
  bool has_dynamic_partition_policy_;
  ObPartitionLevel part_level_;
  common::ObTabletID tablet_id_;
  common::ObString table_name_;
  uint64_t session_id_;
  common::ObString encryption_;
  ObIndexStatus index_status_;
  int64_t schema_version_;
  ObObjectStatus object_status_;
  int32_t table_mode_;  // ObTableMode.mode_
  int64_t duplicate_read_consistency_;  // ObDuplicateReadConsistency; default STRONG=0
  common::ObString storage_cache_policy_;
  common::ObString ttl_definition_;
  common::ObString ttl_flag_;  // serialized ObTTLFlag (binary varchar)
  common::ObString kv_attributes_;
};

struct ObSchemaScanSpec
{
  ObSchemaScanSpec()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      kind_(ObSchemaScanKind::BY_TENANT),
      filter_id_(common::OB_INVALID_ID),
      enum_values_(NULL),
      select_flags_(OB_SCHEMA_SCAN_SELECT_TABLE_ID),
      caller_tag_(NULL)
  {}

  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    kind_ = ObSchemaScanKind::BY_TENANT;
    filter_id_ = common::OB_INVALID_ID;
    enum_values_ = NULL;
    select_flags_ = OB_SCHEMA_SCAN_SELECT_TABLE_ID;
    caller_tag_ = NULL;
  }

  TO_STRING_KV(K_(tenant_id), "kind", static_cast<int64_t>(kind_), K_(filter_id),
               K_(select_flags), K_(caller_tag));

  uint64_t tenant_id_;
  ObSchemaScanKind kind_;
  // table_type / tablegroup_id / tablespace_id / database_id depending on kind_
  uint64_t filter_id_;
  // table_type IN (...) / index_type IN (...); owned by caller
  common::ObIArray<int64_t> *enum_values_;
  uint64_t select_flags_;
  const char *caller_tag_;
};

/**
 * Unified helper: scan __all_table for full-scan paths (A/B/C).
 * Guard / iterators call this; do not touch sql_proxy directly at call sites.
 * DatabaseSchema scan is out of scope for this phase.
 */
class ObSchemaScanOperator
{
public:
  static int scan_table_ids(common::ObISQLClient &sql_client,
                            const ObSchemaScanSpec &spec,
                            common::ObIArray<uint64_t> &table_ids);
  // Alive table_ids at schema_version from __all_table_history
  // (max(schema_version)<=V per table_id, is_deleted=0). Excludes OB_ALL_CORE_TABLE_TID.
  static int scan_alive_table_ids_from_history(common::ObISQLClient &sql_client,
                                               const uint64_t tenant_id,
                                               const int64_t schema_version,
                                               common::ObIArray<uint64_t> &table_ids);
  static int scan_table_rows(common::ObISQLClient &sql_client,
                             const ObSchemaScanSpec &spec,
                             common::ObIArray<ObSchemaTableLiteRow> &rows);

  // Helpers to fill enum_values_ for common A-class predicates.
  static int fill_tmp_table_types(common::ObIArray<int64_t> &table_types);
  static int fill_view_table_types(common::ObIArray<int64_t> &table_types);
  static int fill_external_table_types(common::ObIArray<int64_t> &table_types);
  static int fill_vec_index_types(common::ObIArray<int64_t> &index_types);
  static int fill_vec_ivf_index_types(common::ObIArray<int64_t> &index_types);
  static int fill_async_index_types(common::ObIArray<int64_t> &index_types);

  static const char *scan_kind_to_str(const ObSchemaScanKind kind);
  // Shared by encrypted tablespace enumeration and lite scans.
  static int scan_encrypted_tablespace_ids(common::ObISQLClient &sql_client,
                                           const uint64_t tenant_id,
                                           common::ObIArray<uint64_t> &tablespace_ids);

  // Public wrappers for ObSchemaLiteTableIterator SQL builders.
  static int check_spec(const ObSchemaScanSpec &spec) { return check_spec_(spec); }
  static int build_select_list(const uint64_t select_flags, common::ObSqlString &select_list)
  { return build_select_list_(select_flags, select_list); }
  static int build_where_clause(const ObSchemaScanSpec &spec, common::ObSqlString &where_sql)
  { return build_where_clause_(spec, where_sql); }
  static int build_uint_in_list(const common::ObIArray<uint64_t> &values, common::ObSqlString &in_list)
  { return build_uint_in_list_(values, in_list); }
  // str_allocator required when NEED_STR_ALLOCATOR flags are set.
  static int fill_lite_row(common::sqlclient::ObMySQLResult &result,
                           const uint64_t select_flags,
                           ObSchemaTableLiteRow &row,
                           common::ObIAllocator *str_allocator = NULL)
  { return fill_lite_row_(result, select_flags, row, str_allocator); }

  // L3 testability hooks (debug / unittest)
  static ObSchemaScanKind get_last_scan_kind() { return last_scan_kind_; }
  static int64_t get_last_scan_row_cnt() { return last_scan_row_cnt_; }
  static int64_t get_last_scan_elapsed_us() { return last_scan_elapsed_us_; }

private:
  static int check_spec_(const ObSchemaScanSpec &spec);
  static int build_select_list_(const uint64_t select_flags, common::ObSqlString &select_list);
  static int build_where_clause_(const ObSchemaScanSpec &spec, common::ObSqlString &where_sql);
  static int build_in_list_(const common::ObIArray<int64_t> &values, common::ObSqlString &in_list);
  static int build_uint_in_list_(const common::ObIArray<uint64_t> &values, common::ObSqlString &in_list);
  static int execute_table_sql_(common::ObISQLClient &sql_client,
                                const ObSchemaScanSpec &spec,
                                const common::ObSqlString &sql,
                                const bool only_ids,
                                common::ObIArray<uint64_t> *table_ids,
                                common::ObIArray<ObSchemaTableLiteRow> *rows);
  static int scan_encrypted_tablespace_ids_(common::ObISQLClient &sql_client,
                                            const uint64_t tenant_id,
                                            common::ObIArray<uint64_t> &tablespace_ids);
  static int fill_lite_row_(common::sqlclient::ObMySQLResult &result,
                            const uint64_t select_flags,
                            ObSchemaTableLiteRow &row,
                            common::ObIAllocator *str_allocator);
  static void record_scan_stat_(const ObSchemaScanSpec &spec,
                                const int ret_code,
                                const int64_t row_cnt,
                                const int64_t elapsed_us);

private:
  static ObSchemaScanKind last_scan_kind_;
  static int64_t last_scan_row_cnt_;
  static int64_t last_scan_elapsed_us_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_SCAN_OPERATOR_H_
