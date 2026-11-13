/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_SQL_GEN_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_SQL_GEN_H_

#include "lib/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

// Parameters describing one incremental refresh SQL to generate.
//
// The async index incremental refresh reads the data table at a fixed snapshot
// SCN and selects only the rows that changed after the last refresh point (via
// the ora_rowscn pseudo column). The hidden OLD_NEW$$ pseudo column distinguishes
// newly inserted rows ('N') from deleted old rows ('O'):
//   - 'N' rows are inserted into the index table.
//   - 'O' rows have their rowkey (__pk_increment) written into the index delete
//     buffer table.
struct ObAsyncIndexSqlParam
{
  ObAsyncIndexSqlParam() { reset(); }

  void reset()
  {
    database_name_.reset();
    data_table_name_.reset();
    index_table_name_.reset();
    delete_table_name_.reset();
    data_partition_name_.reset();
    index_partition_name_.reset();
    select_columns_ = nullptr;
    source_columns_ = nullptr;
    partition_key_columns_ = nullptr;
    rowkey_columns_ = nullptr;
    order_columns_ = nullptr;
    index_table_id_ = OB_INVALID_ID;
    is_async_search_aux_index_ = false;
    snapshot_scn_.reset();
    last_refresh_scn_.reset();
    schema_version_ = 0;
    parallelism_ = 1;
    refresh_task_id_ = 0;
    execution_id_ = 1;
    enable_newsort_ = true;
    is_oracle_mode_ = false;
  }

  // Required fields for the INSERT-new-rows SQL.
  bool is_valid_for_insert() const;
  // Required fields for the DELETE-old-rows SQL.
  bool is_valid_for_delete() const;
  // Required fields for the COMPLETE (full snapshot) refresh SQL.
  bool is_valid_for_complete() const;

  TO_STRING_KV(K_(database_name),
               K_(data_table_name),
               K_(index_table_name),
               K_(delete_table_name),
               K_(data_partition_name),
               K_(index_partition_name),
               KP_(select_columns),
               KP_(source_columns),
               KP_(partition_key_columns),
               KP_(rowkey_columns),
               KP_(order_columns),
               K_(index_table_id),
               K_(is_async_search_aux_index),
               K_(snapshot_scn),
               K_(last_refresh_scn),
               K_(schema_version),
               K_(parallelism),
               K_(refresh_task_id),
               K_(execution_id),
               K_(enable_newsort),
               K_(is_oracle_mode));

  common::ObString database_name_;        // schema (database) name
  common::ObString data_table_name_;      // source data table name
  common::ObString index_table_name_;     // target index table name (INSERT-new target)
  common::ObString delete_table_name_;    // index delete buffer table name (DELETE-old target)
  common::ObString data_partition_name_;  // partition on the data table (empty => no PARTITION clause)
  common::ObString index_partition_name_; // partition on the index/delete table (empty => none)
  // Columns selected from the data table into the index table (INSERT-new path).
  const common::ObIArray<common::ObString> *select_columns_;
  // Async Search reads user columns from the main table, then INDEX_DATA_GEN
  // expands them into select_columns_ (the physical Data/Text target columns).
  const common::ObIArray<common::ObString> *source_columns_;
  // Partition keys plus generated-column dependencies required by DDL routing.
  const common::ObIArray<common::ObString> *partition_key_columns_;
  // 物理索引 rowkey；delete-old 路径同时将其投影为 delete buffer 的 docid。
  const common::ObIArray<common::ObString> *rowkey_columns_;
  // INSERT/COMPLETE refresh 的排序列，可能包含非 rowkey 的聚合输入列。
  const common::ObIArray<common::ObString> *order_columns_;
  uint64_t index_table_id_;               // INDEX_DATA_GEN destination schema id
  // Selects INDEX_DATA_GEN for DATA/MATCH/PHRASE.
  bool is_async_search_aux_index_;
  share::SCN snapshot_scn_;               // AS OF SNAPSHOT point; the refresh target SCN
  share::SCN last_refresh_scn_;           // ora_rowscn lower bound; the previous refresh point
  int64_t schema_version_;                // schema version for ob_ddl_schema_version hint
  int64_t parallelism_;                   // DOP for parallel(N) hint
  int64_t refresh_task_id_;               // unique task id for opt_param('ddl_task_id') hint
  int64_t execution_id_;                  // execution_id for opt_param('ddl_execution_id') hint
  bool enable_newsort_;                    // true when all ORDER BY rowkey columns support encoded sorting
  // Tenant compat mode; selects the identifier quoting character (` for mysql,
  // " for oracle) used when escaping schema names into the generated SQL.
  bool is_oracle_mode_;
};

// Generates the inner SQL statements that drive async index incremental refresh.
//
// The database / table / column / partition names all originate from raw,
// user-controlled schema names (a user may create objects whose names contain
// the quoting character or other metacharacters). Every identifier is therefore
// escaped via ObSQLUtils::generate_new_name_with_escape_character (doubling the
// quote char per the tenant compat mode) and then wrapped in quotes before being
// concatenated, mirroring the kernel DDL build-replica path
// (ObDDLUtil::generate_build_replica_sql). Do NOT emit a raw `%.*s` name.
class ObAsyncIndexSqlGenerator
{
public:
  // Build the INSERT statement that pulls newly inserted rows into the index table:
  //
  //   INSERT /*+ monitor enable_parallel_dml parallel(N)
  //          opt_param('enable_newsort', 'true') use_px */
  //   INTO `db`.`index_table`[ PARTITION(p)]
  //   SELECT /*+ index(`db`.`data_table` primary)
  //            ob_ddl_schema_version(`db`.`data_table`, ver)
  //            opt_param('hidden_column_visible', 'true') */
  //     `c1`, `c2`, ...
  //   FROM `db`.`data_table`[ PARTITION(p)] AS OF SNAPSHOT snapshot_scn
  //   WHERE `ora_rowscn` > last_refresh_scn AND `OLD_NEW$$` = 'N'
  //   ORDER BY `pk1`, `pk2`, ...;
  static int generate_insert_new_rows_sql(const ObAsyncIndexSqlParam &param,
                                          common::ObSqlString &sql);

  // Build the COMPLETE (first / no-baseline) refresh statement. The index table
  // is empty (or its baseline is being re-established), so there is no usable
  // incremental start point; we read the FULL data table as of the snapshot and
  // bulk-load every current row. This is a PLAIN snapshot scan -- it must NOT be
  // executed in mview-refresh mode and carries no ora_rowscn / OLD_NEW$$ predicate
  // (those force an incremental mview delta scan that requires a version range):
  //
  //   INSERT /*+ monitor enable_parallel_dml parallel(N)
  //          opt_param('enable_newsort', 'true') use_px */
  //   INTO `db`.`index_table`[ PARTITION(p)]
  //   SELECT /*+ index(`db`.`data_table` primary)
  //            ob_ddl_schema_version(`db`.`data_table`, ver)
  //            opt_param('hidden_column_visible', 'true') */
  //     `c1`, `c2`, ...
  //   FROM `db`.`data_table`[ PARTITION(p)] AS OF SNAPSHOT snapshot_scn
  //   [ORDER BY `pk1`, `pk2`, ...];
  static int generate_complete_refresh_sql(const ObAsyncIndexSqlParam &param,
                                           common::ObSqlString &sql);

  // Build the INSERT statement that records docids of deleted rows into the
  // index delete buffer table:
  //
  //   INSERT /*+ monitor enable_parallel_dml parallel(N)
  //          opt_param('enable_newsort', 'true') use_px */
  //   INTO `db`.`delete_table`[ PARTITION(p)] (`__pk_increment`[, `part_key`, ...])
  //   SELECT /*+ index(`db`.`data_table` primary)
  //            ob_ddl_schema_version(`db`.`data_table`, ver)
  //            opt_param('hidden_column_visible', 'true') */
  //     `__pk_increment`[, `part_key`, ...]
  //                         -- extra columns are used only for DDL tablet routing
  //   FROM `db`.`data_table`[ PARTITION(p)] AS OF SNAPSHOT snapshot_scn
  //   WHERE `ora_rowscn` > last_refresh_scn AND `OLD_NEW$$` = 'O'
  //   ORDER BY `__pk_increment`;   -- required: PDML insert needs DDL sort keys
  static int generate_delete_old_rows_sql(const ObAsyncIndexSqlParam &param,
                                          common::ObSqlString &sql);

private:
  // Append `/*+ monitor enable_parallel_dml parallel(N) opt_param('ddl_execution_id', E)
  //          opt_param('ddl_task_id', T) use_px */`.
  static int append_dml_hint_(const ObAsyncIndexSqlParam &param, common::ObSqlString &sql);
  // Append the SELECT-level hint block (index + ob_ddl_schema_version + opt_param).
  static int append_select_hint_(const ObAsyncIndexSqlParam &param, common::ObSqlString &sql);
  // Append `INTO `db`.`table`[ PARTITION(p)]`.
  static int append_into_target_(const common::ObString &database_name,
                                 const common::ObString &table_name,
                                 const common::ObString &partition_name,
                                 const bool is_oracle_mode,
                                 common::ObIAllocator &allocator,
                                 common::ObSqlString &sql);
  // Append `FROM `db`.`data_table`[ PARTITION(p)] AS OF SNAPSHOT scn`.
  static int append_from_source_(const ObAsyncIndexSqlParam &param,
                                 common::ObIAllocator &allocator,
                                 common::ObSqlString &sql);
  // Append a quoted, comma separated column list (`c1`, `c2`, ...), each name escaped.
  static int append_column_list_(const common::ObIArray<common::ObString> &columns,
                                 const bool is_oracle_mode,
                                 common::ObIAllocator &allocator,
                                 common::ObSqlString &sql);
  // Append an explicit INSERT column list ` (`c1`, `c2`, ...)`. This pins the
  // INSERT target columns by name so the SELECT projection binds correctly
  // regardless of the index table's storage (column-id) order; without it the
  // binding is positional and can misroute PDML partition calculation.
  static int append_insert_column_list_(const common::ObIArray<common::ObString> &columns,
                                        const bool is_oracle_mode,
                                        common::ObIAllocator &allocator,
                                        common::ObSqlString &sql);
  static int append_search_data_gen_select_(const ObAsyncIndexSqlParam &param,
                                            const bool incremental,
                                            common::ObIAllocator &allocator,
                                            common::ObSqlString &sql);
  // Escape `name` for the tenant compat mode (double the quote char) and append it
  // wrapped in the corresponding quote (` for mysql, " for oracle) to `sql`.
  static int append_quoted_ident_(const common::ObString &name,
                                  const bool is_oracle_mode,
                                  common::ObIAllocator &allocator,
                                  common::ObSqlString &sql);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_SQL_GEN_H_
