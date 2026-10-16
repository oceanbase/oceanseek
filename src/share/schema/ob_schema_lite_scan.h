/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_LITE_SCAN_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_LITE_SCAN_H_

#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_scan_operator.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSimpleTableSchemaV2;
class ObSchemaGetterGuard;

/**
 * Aggregation for count_lite_tables() (same Guard entry as scan_lite_tables).
 * - NONE: row iterator (scan_lite_tables / get_next).
 * - COUNT_TABLES: number of matching tables (COUNT(*) on __all_table / Original Array size).
 * - COUNT_PART_NUM: SUM(get_all_part_num()) — level0=1, level1=normal __all_part rows,
 *   level2=normal __all_sub_part rows (partition_type = NORMAL; hidden split/merge excluded).
 *   This is the DDL timeout "tablet_count" (get_tablet_count), not physical tablet cardinality:
 *   view/virtual/external still count 1 at level0 even when has_tablet() is false.
 *   Original: SUM(get_all_part_num()) on matching Simple (uses part_level_).
 *   OnDemand: infer leaf count from part/subpart row existence (no part_level WHERE;
 *   part_level is not indexed). Incomplete schema (part_level already 1/2 but rows not
 *   persisted) may diverge from Original; acceptable for DDL timeout.
 */
enum class ObSchemaLiteAggKind : int8_t
{
  NONE = 0,
  COUNT_TABLES = 1,
  COUNT_PART_NUM = 2,
};

enum ObSchemaLitePartFlag : uint64_t
{
  OB_SCHEMA_LITE_PART_PART_ID = 1ULL << 0,
  OB_SCHEMA_LITE_PART_PART_IDX = 1ULL << 1,
  OB_SCHEMA_LITE_PART_TABLET_ID = 1ULL << 2,
  OB_SCHEMA_LITE_PART_PART_NAME = 1ULL << 3,
  OB_SCHEMA_LITE_PART_SCHEMA_VERSION = 1ULL << 4,
  OB_SCHEMA_LITE_PART_STORAGE_CACHE_POLICY = 1ULL << 5,
  OB_SCHEMA_LITE_PART_ALL =
      OB_SCHEMA_LITE_PART_PART_ID
      | OB_SCHEMA_LITE_PART_PART_IDX
      | OB_SCHEMA_LITE_PART_TABLET_ID,
};

enum ObSchemaLiteSubPartFlag : uint64_t
{
  OB_SCHEMA_LITE_SUBPART_PART_ID = 1ULL << 0,       // parent one-level part_id (nest key)
  OB_SCHEMA_LITE_SUBPART_SUB_PART_ID = 1ULL << 1,
  OB_SCHEMA_LITE_SUBPART_SUB_PART_IDX = 1ULL << 2,
  OB_SCHEMA_LITE_SUBPART_TABLET_ID = 1ULL << 3,
  OB_SCHEMA_LITE_SUBPART_SCHEMA_VERSION = 1ULL << 4,
  OB_SCHEMA_LITE_SUBPART_STORAGE_CACHE_POLICY = 1ULL << 5,
  OB_SCHEMA_LITE_SUBPART_ALL =
      OB_SCHEMA_LITE_SUBPART_PART_ID
      | OB_SCHEMA_LITE_SUBPART_SUB_PART_ID
      | OB_SCHEMA_LITE_SUBPART_SUB_PART_IDX
      | OB_SCHEMA_LITE_SUBPART_TABLET_ID,
};

enum ObSchemaLiteColumnFlag : uint64_t
{
  OB_SCHEMA_LITE_COLUMN_NONE = 0,
  // Extended later for TTL / StorageCache TIME_POLICY (__all_column).
};

struct ObSchemaSubPartLiteRow
{
  ObSchemaSubPartLiteRow()
    : part_id_(common::OB_INVALID_ID),
      sub_part_id_(common::OB_INVALID_ID),
      sub_part_idx_(-1),
      tablet_id_()
  {}
  void reset()
  {
    part_id_ = common::OB_INVALID_ID;
    sub_part_id_ = common::OB_INVALID_ID;
    sub_part_idx_ = -1;
    tablet_id_.reset();
  }
  TO_STRING_KV(K_(part_id), K_(sub_part_id), K_(sub_part_idx), K_(tablet_id));

  int64_t part_id_;
  int64_t sub_part_id_;
  int64_t sub_part_idx_;
  common::ObTabletID tablet_id_;
};

struct ObSchemaPartLiteRow
{
  ObSchemaPartLiteRow()
    : part_id_(common::OB_INVALID_ID),
      part_idx_(-1),
      tablet_id_(),
      sub_parts_()
  {}
  void reset()
  {
    part_id_ = common::OB_INVALID_ID;
    part_idx_ = -1;
    tablet_id_.reset();
    sub_parts_.reuse();
  }
  TO_STRING_KV(K_(part_id), K_(part_idx), K_(tablet_id), "sub_part_cnt", sub_parts_.count());

  int64_t part_id_;
  int64_t part_idx_;
  common::ObTabletID tablet_id_;
  common::ObArray<ObSchemaSubPartLiteRow> sub_parts_;
};

struct ObSchemaLiteTableResult
{
  ObSchemaLiteTableResult() { reset(); }
  void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    table_type_ = MAX_TABLE_TYPE;
    index_type_ = INDEX_TYPE_IS_NOT;
    tablegroup_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    data_table_id_ = common::OB_INVALID_ID;
    tablespace_id_ = common::OB_INVALID_ID;
    duplicate_scope_ = 0;
    has_dynamic_partition_policy_ = false;
    part_level_ = PARTITION_LEVEL_ZERO;
    tablet_id_.reset();
    table_name_.reset();
    session_id_ = 0;
    encryption_.reset();
    index_status_ = INDEX_STATUS_UNAVAILABLE;
    schema_version_ = common::OB_INVALID_VERSION;
    object_status_ = ObObjectStatus::INVALID;
    table_mode_ = 0;
    duplicate_read_consistency_ = 0;
    storage_cache_policy_.reset();
    storage_cache_policy_type_ = storage::ObStorageCachePolicyType::MAX_POLICY;
    ttl_definition_.reset();
    ttl_flag_.reset();
    kv_attributes_.reset();
    parts_.reuse();
  }
  void reuse() { reset(); }

  // Mirror ObSimpleTableSchemaV2::has_partition() using table_type_ + index_type_.
  bool has_partition() const;
  // Mirror ObSimpleTableSchemaV2::has_tablet() (local index included; view/virtual/external excluded).
  bool has_tablet() const;
  bool need_encrypt() const
  {
    return 0 != encryption_.length() && 0 != encryption_.case_compare("none");
  }
  bool is_index_table() const { return share::schema::is_index_table(table_type_); }
  bool is_view_table() const { return share::schema::is_view_table(table_type_); }
  bool is_user_table() const { return USER_TABLE == table_type_; }
  bool is_sys_table() const { return SYSTEM_TABLE == table_type_; }
  bool is_user_view() const { return USER_VIEW == table_type_; }
  bool is_external_table() const { return EXTERNAL_TABLE == table_type_; }
  bool is_tmp_table() const
  {
    return share::schema::is_mysql_tmp_table(table_type_)
        || TMP_TABLE_ORA_SESS == table_type_
        || TMP_TABLE_ORA_TRX == table_type_
        || TMP_TABLE_ORA_SESS_V2 == table_type_
        || TMP_TABLE_ORA_TRX_V2 == table_type_;
  }
  bool is_oracle_tmp_table_v2() const
  {
    return TMP_TABLE_ORA_SESS_V2 == table_type_
        || TMP_TABLE_ORA_TRX_V2 == table_type_;
  }
  bool is_ctas_tmp_table() const { return 0 != session_id_ && !is_tmp_table(); }
  bool is_mlog_table() const { return MATERIALIZED_VIEW_LOG == table_type_; }
  bool is_available_mlog() const
  {
    return is_mlog_table() && INDEX_STATUS_AVAILABLE == index_status_;
  }
  bool is_materialized_view() const { return MATERIALIZED_VIEW == table_type_; }
  bool is_aux_table() const
  {
    return USER_INDEX == table_type_
        || AUX_VERTIAL_PARTITION_TABLE == table_type_
        || AUX_LOB_PIECE == table_type_
        || AUX_LOB_META == table_type_;
  }
  bool is_aux_lob_meta_table() const { return share::schema::is_aux_lob_meta_table(table_type_); }
  bool is_vec_index() const { return share::schema::is_vec_index(index_type_); }
  bool is_vec_ivf_centroid_index() const
  {
    return share::schema::is_local_vec_ivf_centroid_index(index_type_);
  }
  bool is_vec_ivfflat_cid_vector_index() const
  {
    return share::schema::is_vec_ivfflat_cid_vector_index(index_type_);
  }
  bool is_vec_ivfflat_rowkey_cid_index() const
  {
    return share::schema::is_vec_ivfflat_rowkey_cid_index(index_type_);
  }
  // Align ObSimpleTableSchemaV2: HEAP, or IOT without user PK.
  bool is_table_with_hidden_pk_column() const;
  bool is_in_recyclebin() const { return common::OB_RECYCLEBIN_SCHEMA_ID == database_id_; }
  bool has_rowid() const { return is_user_table() || is_tmp_table(); }
  bool is_user_hidden_table() const;
  bool is_normal_schema() const { return !is_user_hidden_table(); }
  bool is_extended_rowid_mode() const;
  // Enumeration path has no in_offline_ddl_white_list_; same as Simple default.
  bool check_can_do_ddl() const;
  bool mv_container_table() const;
  bool mv_available() const;
  // Align ObSimpleTableSchemaV2: CLUSTER + STRONG. Callers must project both
  // duplicate_scope and duplicate_read_consistency to exclude broadcast (WEAK).
  bool is_duplicate_table() const
  {
    return static_cast<ObDuplicateScope>(duplicate_scope_) == ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER
        && static_cast<ObDuplicateReadConsistency>(duplicate_read_consistency_)
               == ObDuplicateReadConsistency::STRONG;
  }
  bool is_broadcast_table() const
  {
    return static_cast<ObDuplicateScope>(duplicate_scope_) == ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER
        && static_cast<ObDuplicateReadConsistency>(duplicate_read_consistency_)
               == ObDuplicateReadConsistency::WEAK;
  }

  TO_STRING_KV(K_(table_id), K_(table_type), K_(index_type), K_(tablegroup_id),
               K_(database_id), K_(data_table_id), K_(part_level), K_(tablet_id),
               K_(table_name), K_(session_id), K_(encryption), K_(index_status),
               K_(schema_version), K_(object_status), K_(table_mode),
               K_(duplicate_scope), K_(duplicate_read_consistency),
               K_(storage_cache_policy), K_(storage_cache_policy_type),
               K_(ttl_definition), K_(ttl_flag),
               K_(kv_attributes), "part_cnt", parts_.count());

  // From __all_table (projected by table_spec_.select_flags_ / table_flags_).
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
  int64_t duplicate_read_consistency_;
  common::ObString storage_cache_policy_;
  // Parsed from storage_cache_policy_ (SQL) or Simple enum (Original). Empty string -> MAX_POLICY.
  storage::ObStorageCachePolicyType storage_cache_policy_type_;
  common::ObString ttl_definition_;
  common::ObString ttl_flag_;
  common::ObString kv_attributes_;

  // Reserved nested part tree; scan_lite_tables is table-only and leaves this empty.
  common::ObArray<ObSchemaPartLiteRow> parts_;
};

struct ObSchemaLiteScanSpec
{
  ObSchemaLiteScanSpec()
    : table_spec_(),
      part_flags_(0),
      subpart_flags_(0),
      column_flags_(0),
      agg_kind_(ObSchemaLiteAggKind::NONE),
      caller_tag_(NULL)
  {}

  void reset()
  {
    table_spec_.reset();
    part_flags_ = 0;
    subpart_flags_ = 0;
    column_flags_ = 0;
    agg_kind_ = ObSchemaLiteAggKind::NONE;
    caller_tag_ = NULL;
  }

  TO_STRING_KV(K_(table_spec), K_(part_flags), K_(subpart_flags), K_(column_flags),
               "agg_kind", static_cast<int>(agg_kind_), K_(caller_tag));

  // Table predicate + select_flags_ (use OB_SCHEMA_SCAN_SELECT_*).
  ObSchemaScanSpec table_spec_;
  uint64_t part_flags_;
  uint64_t subpart_flags_;
  uint64_t column_flags_;  // reserved; non-zero not implemented yet
  ObSchemaLiteAggKind agg_kind_;
  const char *caller_tag_;
};

/**
 * Lite full-scan iterator: one get_next() => one table view (scalars + nested parts).
 *
 * Unified call site (Original / OnDemand):
 *   ObSchemaLiteTableIterator iter;
 *   guard.scan_lite_tables(spec, iter);  // or iter.init(guard, spec)
 *   while (OB_SUCC(iter.get_next(row))) { ... }
 * COUNT (no rows):
 *   spec.agg_kind_ = COUNT_TABLES / COUNT_PART_NUM;
 *   guard.count_lite_tables(spec, count);
 *
 * - OnDemand: stream inner SQL (table-only). Part/subpart uses scan_lite_part_rows().
 *   COUNT uses COUNT(*) SQL. COUNT_PART_NUM: JOIN __all_table + NOT EXISTS
 *   on part/subpart (see count_from_inner_sql_).
 * - Original: collect matching Simple schemas, fill result via PartIter.
 *   COUNT sums Array size / get_all_part_num() (same membership as pre-lite ids + schema).
 *
 * Pointer from get_next() is valid only until next get_next()/reset().
 */
class ObSchemaLitePartJoinIterator;

class ObSchemaLiteTableIterator
{
  friend class ObSchemaLitePartJoinIterator;
public:
  ObSchemaLiteTableIterator();
  ~ObSchemaLiteTableIterator() { reset(); }

  // Unified entry: OnDemand -> SQL path (+ mgr core append); Original -> Simple schemas.
  int init(ObSchemaGetterGuard &guard, const ObSchemaLiteScanSpec &spec);
  // COUNT_TABLES / COUNT_PART_NUM. Same Original vs OnDemand split as init(guard).
  int count(ObSchemaGetterGuard &guard, const ObSchemaLiteScanSpec &spec, int64_t &count);
  // Original mock: schemas already filtered by caller; get_next fills result_ from Simple.
  int init_by_simple_schemas(const common::ObIArray<const ObSimpleTableSchemaV2 *> &schemas,
                             const ObSchemaLiteScanSpec &spec);
  // After OnDemand SQL init, union matching is_core_table from resident mgr (for mittest / dual-path).
  int append_core_schemas_from_mgr(ObSchemaGetterGuard &guard);
  void reset();
  bool is_inited() const { return is_inited_; }

  int get_next(const ObSchemaLiteTableResult *&result);

  bool is_by_simple_schemas() const { return by_simple_schemas_; }

  TO_STRING_KV(K_(is_inited), K_(by_simple_schemas), K_(spec), K_(simple_idx));

private:
  // Low-level SQL entry for focused tests. Production callers must use the
  // Guard entry above so the full-scan gate and STRONG snapshot are enforced.
  int init(common::ObISQLClient &sql_client, const ObSchemaLiteScanSpec &spec);

  enum class PathKind : int8_t
  {
    INVALID = 0,
    SQL = 1,
    SIMPLE_SCHEMA = 2,
  };

  int check_spec_(const bool for_count = false);
  int collect_original_schemas_(ObSchemaGetterGuard &guard,
                                common::ObIArray<const ObSimpleTableSchemaV2 *> &schemas);
  int count_from_simple_schemas_(const common::ObIArray<const ObSimpleTableSchemaV2 *> &schemas,
                                 int64_t &count) const;
  int count_from_inner_sql_(int64_t &count);
  int count_core_part_num_(ObSchemaGetterGuard &guard, int64_t &count);
  // table_alias only for COUNT_PART_NUM JOIN (qualify t.col); NULL = unprefixed generic where.
  int build_count_table_where_(common::ObSqlString &where_sql, const char *table_alias = NULL);
  int exec_count_sql_(const common::ObSqlString &sql, int64_t &count);
  static bool match_scan_pred_(const ObSimpleTableSchemaV2 &schema,
                               const ObSchemaLiteScanSpec &spec,
                               const common::ObIArray<uint64_t> *encrypted_ts_ids);
  int load_encrypted_tablespace_ids_(common::ObIArray<uint64_t> &encrypted_ts_ids) const;
  int open_sql_scan_();
  // __all_table omits is_core_table; append matching core Simple schemas after SQL EOF.
  int append_core_simple_schemas_from_mgr_(ObSchemaGetterGuard &guard);
  int build_table_sql_(common::ObSqlString &sql);

  int get_next_from_sql_();
  int get_next_from_simple_schemas_();

  int assign_table_fields_from_lite_row_(const ObSchemaTableLiteRow &row);
  int assign_table_fields_from_simple_(const ObSimpleTableSchemaV2 &schema);

private:
  bool is_inited_;
  bool by_simple_schemas_;
  PathKind path_;
  ObSchemaLiteScanSpec spec_;
  common::ObISQLClient *sql_client_;

  ObSchemaLiteTableResult result_;
  // Owns deep-copied string fields for the current get_next() row.
  common::ObArenaAllocator string_allocator_;

  // OnDemand SQL cursor (InnerSQL result; held until reset()).
  common::ObISQLClient::ReadResult *table_res_;
  common::sqlclient::ObMySQLResult *table_result_;
  bool table_res_eof_;

  // SIMPLE_SCHEMA path
  common::ObArray<const ObSimpleTableSchemaV2 *> simple_schemas_;
  int64_t simple_idx_;
};

/**
 * A-tier join side for scan_lite_part_rows().
 * INNER JOIN __all_table with __all_part (PART) or __all_sub_part (SUBPART).
 * Hidden split/merge rows (partition_type != NORMAL) are excluded, matching
 * part_array_ / ObPartitionSchemaIter(CHECK_PARTITION_MODE_NORMAL).
 */
enum class ObSchemaLiteJoinSide : int8_t
{
  PART = 0,
  SUBPART = 1,
};

/**
 * Flat JOIN row: t.* projected ints + p/s partition columns.
 * Pointer from get_next() is valid only until the next get_next()/reset().
 * table_tablet_id_ is t.tablet_id; tablet_id_ is p.tablet_id / s.tablet_id.
 */
struct ObSchemaLiteJoinRow
{
  ObSchemaLiteJoinRow() { reset(); }
  void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    table_type_ = MAX_TABLE_TYPE;
    index_type_ = INDEX_TYPE_IS_NOT;
    tablegroup_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    data_table_id_ = common::OB_INVALID_ID;
    tablespace_id_ = common::OB_INVALID_ID;
    duplicate_scope_ = 0;
    has_dynamic_partition_policy_ = false;
    part_level_ = PARTITION_LEVEL_ZERO;
    table_tablet_id_.reset();
    table_name_.reset();
    session_id_ = 0;
    encryption_.reset();
    index_status_ = INDEX_STATUS_UNAVAILABLE;
    schema_version_ = common::OB_INVALID_VERSION;
    object_status_ = ObObjectStatus::INVALID;
    table_mode_ = 0;
    duplicate_read_consistency_ = 0;
    storage_cache_policy_.reset();
    ttl_definition_.reset();
    ttl_flag_.reset();
    kv_attributes_.reset();
    part_id_ = common::OB_INVALID_ID;
    part_idx_ = -1;
    sub_part_id_ = common::OB_INVALID_ID;
    sub_part_idx_ = -1;
    tablet_id_.reset();
    part_name_.reset();
    part_schema_version_ = common::OB_INVALID_VERSION;
    part_storage_cache_policy_type_ = storage::ObStorageCachePolicyType::MAX_POLICY;
  }
  void reuse() { reset(); }

  bool has_partition() const;
  bool has_tablet() const;
  bool is_index_table() const { return share::schema::is_index_table(table_type_); }
  bool is_view_table() const { return share::schema::is_view_table(table_type_); }
  bool is_user_hidden_table() const;
  bool is_normal_schema() const { return !is_user_hidden_table(); }
  bool is_duplicate_table() const
  {
    return static_cast<ObDuplicateScope>(duplicate_scope_) == ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER
        && static_cast<ObDuplicateReadConsistency>(duplicate_read_consistency_)
               == ObDuplicateReadConsistency::STRONG;
  }
  bool is_broadcast_table() const
  {
    return static_cast<ObDuplicateScope>(duplicate_scope_) == ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER
        && static_cast<ObDuplicateReadConsistency>(duplicate_read_consistency_)
               == ObDuplicateReadConsistency::WEAK;
  }

  TO_STRING_KV(K_(table_id), K_(table_type), K_(index_type), K_(data_table_id),
               K_(part_level), K_(table_tablet_id), K_(table_mode),
               K_(duplicate_scope), K_(duplicate_read_consistency),
               K_(storage_cache_policy), K_(ttl_definition),
               K_(part_id), K_(part_idx), K_(sub_part_id), K_(sub_part_idx),
               K_(tablet_id), K_(part_name), K_(part_schema_version),
               K_(part_storage_cache_policy_type));

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
  common::ObTabletID table_tablet_id_;
  common::ObString table_name_;
  uint64_t session_id_;
  common::ObString encryption_;
  ObIndexStatus index_status_;
  int64_t schema_version_;
  ObObjectStatus object_status_;
  int32_t table_mode_;
  int64_t duplicate_read_consistency_;
  common::ObString storage_cache_policy_;
  common::ObString ttl_definition_;
  common::ObString ttl_flag_;
  common::ObString kv_attributes_;

  int64_t part_id_;       // PART: p.part_id; SUBPART: parent s.part_id
  int64_t part_idx_;      // PART: p.part_idx; SUBPART: parent p.part_idx if requested
  int64_t sub_part_id_;
  int64_t sub_part_idx_;
  common::ObTabletID tablet_id_;  // p/s tablet_id
  common::ObString part_name_;    // PART: p.part_name; SUBPART: parent p.part_name if requested
  // Aliased as part_schema_version / part_storage_cache_policy in SQL to avoid
  // colliding with t.schema_version / t.storage_cache_policy.
  int64_t part_schema_version_;
  storage::ObStorageCachePolicyType part_storage_cache_policy_type_;
};

/**
 * A-tier part/subpart JOIN iterator.
 *   ObSchemaLitePartJoinIterator iter;
 *   guard.scan_lite_part_rows(spec, ObSchemaLiteJoinSide::PART, iter);
 *   while (OB_SUCC(iter.get_next(row))) { ... }
 *
 * - OnDemand: INNER JOIN SQL streamed via InnerSQL; application does not probe a wanted
 *   dictionary against a standalone __all_part dump.
 * - Original: flatten matching Simple + PartIter into the same ObSchemaLiteJoinRow.
 * - Core tables omitted by __all_table are appended from resident mgr after SQL EOF.
 */
class ObSchemaLitePartJoinIterator
{
public:
  ObSchemaLitePartJoinIterator();
  ~ObSchemaLitePartJoinIterator() { reset(); }

  int init(ObSchemaGetterGuard &guard,
           const ObSchemaLiteScanSpec &spec,
           const ObSchemaLiteJoinSide join_side);
  void reset();
  bool is_inited() const { return is_inited_; }

  int get_next(const ObSchemaLiteJoinRow *&row);

  ObSchemaLiteJoinSide get_join_side() const { return join_side_; }
  bool is_by_simple_schemas() const { return by_simple_schemas_; }

  TO_STRING_KV(K_(is_inited), K_(by_simple_schemas),
               "join_side", static_cast<int>(join_side_), K_(spec),
               K_(schema_idx), K_(part_pos), K_(sub_pos));

private:
  enum class PathKind : int8_t
  {
    INVALID = 0,
    SQL = 1,
    SIMPLE_SCHEMA = 2,
  };

  int check_spec_();
  int open_sql_scan_();
  int append_core_schemas_(ObSchemaGetterGuard &guard);
  int build_join_sql_(common::ObSqlString &sql);
  int build_join_select_list_(common::ObSqlString &select_list);
  int build_join_table_where_(common::ObSqlString &where_sql);
  int fill_join_row_from_sql_(common::sqlclient::ObMySQLResult &result);
  int fill_join_row_from_simple_(const ObSimpleTableSchemaV2 &schema,
                                 const ObPartition *partition,
                                 const ObSubPartition *subpart);
  int assign_table_fields_from_lite_row_(const ObSchemaTableLiteRow &row);
  int get_next_from_sql_();
  int get_next_from_simple_schemas_();
  int next_simple_part_row_();
  int next_simple_subpart_row_();

private:
  bool is_inited_;
  bool by_simple_schemas_;
  PathKind path_;
  ObSchemaLiteJoinSide join_side_;
  ObSchemaLiteScanSpec spec_;
  common::ObISQLClient *sql_client_;

  ObSchemaLiteJoinRow row_;
  common::ObArenaAllocator string_allocator_;

  common::ObISQLClient::ReadResult *sql_res_;
  common::sqlclient::ObMySQLResult *sql_result_;
  bool sql_res_eof_;

  common::ObArray<const ObSimpleTableSchemaV2 *> simple_schemas_;
  int64_t schema_idx_;
  int64_t part_pos_;
  int64_t sub_pos_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_LITE_SCAN_H_
