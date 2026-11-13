/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_async_index_sql_gen.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/allocator/page_arena.h"   // ObArenaAllocator
#include "sql/ob_sql_utils.h"           // ObSQLUtils::generate_new_name_with_escape_character

namespace oceanbase
{
namespace storage
{
using namespace common;

// ======================== ObAsyncIndexSqlParam ========================
bool ObAsyncIndexSqlParam::is_valid_for_insert() const
{
  return !database_name_.empty()
      && !data_table_name_.empty()
      && !index_table_name_.empty()
      && nullptr != select_columns_
      && select_columns_->count() > 0
      && nullptr != order_columns_
      && order_columns_->count() > 0
      && (!is_async_search_aux_index_
          || (OB_INVALID_ID != index_table_id_
              && nullptr != source_columns_
              && source_columns_->count() > 0))
      && snapshot_scn_.is_valid()
      && last_refresh_scn_.is_valid()
      && parallelism_ > 0
      && refresh_task_id_ > 0;
}

bool ObAsyncIndexSqlParam::is_valid_for_delete() const
{
  return !database_name_.empty()
      && !data_table_name_.empty()
      && !delete_table_name_.empty()
      && nullptr != rowkey_columns_
      && rowkey_columns_->count() > 0
      && snapshot_scn_.is_valid()
      && last_refresh_scn_.is_valid()
      && parallelism_ > 0
      && refresh_task_id_ > 0;
}

bool ObAsyncIndexSqlParam::is_valid_for_complete() const
{
  // No last_refresh_scn_ requirement: a complete refresh has no incremental
  // start point. Only the snapshot to read the full table as of is needed.
  return !database_name_.empty()
      && !data_table_name_.empty()
      && !index_table_name_.empty()
      && nullptr != select_columns_
      && select_columns_->count() > 0
      && nullptr != order_columns_
      && order_columns_->count() > 0
      && (!is_async_search_aux_index_
          || (OB_INVALID_ID != index_table_id_
              && nullptr != source_columns_
              && source_columns_->count() > 0))
      && snapshot_scn_.is_valid()
      && parallelism_ > 0
      && refresh_task_id_ > 0;
}

// ======================== ObAsyncIndexSqlGenerator ========================
int ObAsyncIndexSqlGenerator::append_dml_hint_(const ObAsyncIndexSqlParam &param, ObSqlString &sql)
{
  return sql.append_fmt(" /*+ monitor enable_parallel_dml parallel(%ld)"
                        " opt_param('ddl_execution_id', %ld)"
                        " opt_param('ddl_task_id', %ld)"
                        " opt_param('enable_newsort', '%s') use_px */",
                        param.parallelism_,
                        param.execution_id_,
                        param.refresh_task_id_,
                        param.enable_newsort_ ? "true" : "false");
}

int ObAsyncIndexSqlGenerator::append_quoted_ident_(const ObString &name,
                                                   const bool is_oracle_mode,
                                                   ObIAllocator &allocator,
                                                   ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const char quote = is_oracle_mode ? '"' : '`';
  ObString escaped;
  // Double every embedded quote char so a user-controlled schema name containing
  // the quote (or other metacharacters) cannot break out of the quoted identifier.
  if (OB_FAIL(sql::ObSQLUtils::generate_new_name_with_escape_character(
          allocator, name, escaped, is_oracle_mode))) {
    LOG_WARN("failed to escape identifier", K(ret), K(name), K(is_oracle_mode));
  } else if (OB_FAIL(sql.append_fmt("%c%.*s%c", quote,
                                    static_cast<int>(escaped.length()), escaped.ptr(),
                                    quote))) {
    LOG_WARN("failed to append quoted identifier", K(ret), K(escaped));
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::append_select_hint_(const ObAsyncIndexSqlParam &param,
                                                  ObSqlString &sql)
{
  // index(`db`.`data_table` primary) ob_ddl_schema_version(`db`.`data_table`, ver)
  // opt_param('hidden_column_visible', 'true'). db/table names are escaped+quoted.
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AsyncIdxSql");
  const bool is_oracle = param.is_oracle_mode_;
  if (OB_FAIL(sql.append(" /*+ index("))) {
    LOG_WARN("failed to append index hint open", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(param.database_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append db name in index hint", K(ret));
  } else if (OB_FAIL(sql.append("."))) {
    LOG_WARN("failed to append dot", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(param.data_table_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append table name in index hint", K(ret));
  } else if (OB_FAIL(sql.append(" primary) ob_ddl_schema_version("))) {
    LOG_WARN("failed to append schema version hint open", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(param.database_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append db name in schema version hint", K(ret));
  } else if (OB_FAIL(sql.append("."))) {
    LOG_WARN("failed to append dot", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(param.data_table_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append table name in schema version hint", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %ld)"
                                    " opt_param('hidden_column_visible', 'true') */",
                                    param.schema_version_))) {
    LOG_WARN("failed to append schema version hint close", K(ret));
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::append_into_target_(const ObString &database_name,
                                                  const ObString &table_name,
                                                  const ObString &partition_name,
                                                  const bool is_oracle_mode,
                                                  ObIAllocator &allocator,
                                                  ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append(" INTO "))) {
    LOG_WARN("failed to append into", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(database_name, is_oracle_mode, allocator, sql))) {
    LOG_WARN("failed to append into db name", K(ret), K(database_name));
  } else if (OB_FAIL(sql.append("."))) {
    LOG_WARN("failed to append dot", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(table_name, is_oracle_mode, allocator, sql))) {
    LOG_WARN("failed to append into table name", K(ret), K(table_name));
  } else if (!partition_name.empty()) {
    if (OB_FAIL(sql.append(" PARTITION("))) {
      LOG_WARN("failed to append into partition open", K(ret));
    } else if (OB_FAIL(append_quoted_ident_(partition_name, is_oracle_mode, allocator, sql))) {
      LOG_WARN("failed to append into partition name", K(ret), K(partition_name));
    } else if (OB_FAIL(sql.append(")"))) {
      LOG_WARN("failed to append into partition close", K(ret));
    }
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::append_from_source_(const ObAsyncIndexSqlParam &param,
                                                  ObIAllocator &allocator,
                                                  ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const bool is_oracle = param.is_oracle_mode_;
  if (OB_FAIL(sql.append(" FROM "))) {
    LOG_WARN("failed to append from", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(param.database_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append from db name", K(ret), K(param));
  } else if (OB_FAIL(sql.append("."))) {
    LOG_WARN("failed to append dot", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(param.data_table_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append from table name", K(ret), K(param));
  } else if (!param.data_partition_name_.empty()) {
    if (OB_FAIL(sql.append(" PARTITION("))) {
      LOG_WARN("failed to append source partition open", K(ret));
    } else if (OB_FAIL(append_quoted_ident_(param.data_partition_name_, is_oracle,
                                            allocator, sql))) {
      LOG_WARN("failed to append source partition name", K(ret), K(param));
    } else if (OB_FAIL(sql.append(")"))) {
      LOG_WARN("failed to append source partition close", K(ret));
    }
  }
  if (OB_SUCC(ret)
      && OB_FAIL(sql.append_fmt(" AS OF SNAPSHOT %lu", param.snapshot_scn_.get_val_for_sql()))) {
    LOG_WARN("failed to append snapshot", K(ret), K(param));
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::append_column_list_(const ObIArray<ObString> &columns,
                                                  const bool is_oracle_mode,
                                                  ObIAllocator &allocator,
                                                  ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObString &col = columns.at(i);
    if (0 != i && OB_FAIL(sql.append(", "))) {
      LOG_WARN("failed to append column separator", K(ret), K(i));
    } else if (OB_FAIL(append_quoted_ident_(col, is_oracle_mode, allocator, sql))) {
      LOG_WARN("failed to append column", K(ret), K(i), K(col));
    }
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::append_insert_column_list_(const ObIArray<ObString> &columns,
                                                         const bool is_oracle_mode,
                                                         ObIAllocator &allocator,
                                                         ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append(" ("))) {
    LOG_WARN("failed to append insert column list open paren", K(ret));
  } else if (OB_FAIL(append_column_list_(columns, is_oracle_mode, allocator, sql))) {
    LOG_WARN("failed to append insert columns", K(ret));
  } else if (OB_FAIL(sql.append(")"))) {
    LOG_WARN("failed to append insert column list close paren", K(ret));
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::append_search_data_gen_select_(
    const ObAsyncIndexSqlParam &param,
    const bool incremental,
    ObIAllocator &allocator,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const bool is_oracle = param.is_oracle_mode_;
  if (OB_FAIL(sql.append(" SELECT"))) {
    LOG_WARN("failed to append search select", K(ret));
  } else if (OB_FAIL(append_select_hint_(param, sql))) {
    LOG_WARN("failed to append search select hint", K(ret));
  } else if (OB_FAIL(sql.append(" DATA_GEN_TABLE.*"))) {
    LOG_WARN("failed to append search generated columns", K(ret));
  } else if (nullptr != param.partition_key_columns_
             && !param.partition_key_columns_->empty()
             && OB_FAIL(sql.append(", "))) {
    LOG_WARN("failed to append search partition column separator", K(ret));
  } else if (nullptr != param.partition_key_columns_
             && !param.partition_key_columns_->empty()
             && OB_FAIL(append_column_list_(*param.partition_key_columns_,
                                            is_oracle, allocator, sql))) {
    LOG_WARN("failed to append search partition columns", K(ret));
  } else if (OB_FAIL(append_from_source_(param, allocator, sql))) {
    LOG_WARN("failed to append search source", K(ret));
  } else if (OB_FAIL(sql.append(", INDEX_DATA_GEN(COLUMNS("))) {
    LOG_WARN("failed to append INDEX_DATA_GEN", K(ret));
  } else if (OB_FAIL(append_column_list_(*param.source_columns_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append search source columns", K(ret));
  } else if (OB_FAIL(sql.append(", "))) {
    LOG_WARN("failed to append hidden primary key separator", K(ret));
  } else if (OB_FAIL(append_quoted_ident_(
                 ObString(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME), is_oracle, allocator, sql))) {
    LOG_WARN("failed to append hidden primary key column", K(ret));
  } else if (OB_FAIL(sql.append(")"))) {
    LOG_WARN("failed to close INDEX_DATA_GEN columns", K(ret));
  } else if (nullptr != param.partition_key_columns_
             && !param.partition_key_columns_->empty()
             && OB_FAIL(sql.append(", PARTITION KEY("))) {
    LOG_WARN("failed to append INDEX_DATA_GEN partition key", K(ret));
  } else if (nullptr != param.partition_key_columns_
             && !param.partition_key_columns_->empty()
             && OB_FAIL(append_column_list_(*param.partition_key_columns_,
                                            is_oracle, allocator, sql))) {
    LOG_WARN("failed to append INDEX_DATA_GEN partition columns", K(ret));
  } else if (nullptr != param.partition_key_columns_
             && !param.partition_key_columns_->empty()
             && OB_FAIL(sql.append(")"))) {
    LOG_WARN("failed to close INDEX_DATA_GEN partition key", K(ret));
  } else if (OB_FAIL(sql.append_fmt(", %lu) DATA_GEN_TABLE", param.index_table_id_))) {
    LOG_WARN("failed to close INDEX_DATA_GEN", K(ret));
  } else if (incremental
             && OB_FAIL(sql.append_fmt(
                    " WHERE `ora_rowscn` > %lu AND `OLD_NEW$$` = 'N'",
                    param.last_refresh_scn_.get_val_for_sql()))) {
    LOG_WARN("failed to append search incremental predicate", K(ret));
  } else if (nullptr != param.order_columns_ && !param.order_columns_->empty()) {
    if (OB_FAIL(sql.append(" ORDER BY "))) {
      LOG_WARN("failed to append search order by", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param.order_columns_->count(); ++i) {
      if (OB_FAIL(sql.append_fmt("%s%ld", 0 == i ? "" : ", ", i + 1))) {
        LOG_WARN("failed to append search order ordinal", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::generate_insert_new_rows_sql(const ObAsyncIndexSqlParam &param,
                                                           ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  ObArenaAllocator allocator("AsyncIdxSql");
  const bool is_oracle = param.is_oracle_mode_;
  if (OB_UNLIKELY(!param.is_valid_for_insert())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param for insert sql", K(ret), K(param));
  } else if (OB_FAIL(sql.append("INSERT"))) {
    LOG_WARN("failed to append insert", K(ret));
  } else if (OB_FAIL(append_dml_hint_(param, sql))) {
    LOG_WARN("failed to append dml hint", K(ret));
  } else if (OB_FAIL(append_into_target_(param.database_name_, param.index_table_name_,
                                         param.index_partition_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append into target", K(ret));
  // Explicit INSERT column list. Without it, the SELECT columns bind to the
  // index table's columns POSITIONALLY in storage order (sorted by column id),
  // not in the order we SELECT them. The index rowkey order (word_segment, pk)
  // differs from the storage/col-id order (pk, word_segment, ...), so an
  // implicit binding routes the VARCHAR word_segment value into the BIGINT
  // __pk_increment slot; PDML then computes the partition over the mis-bound
  // column, hitting the integer-only fast calc-partition-id path with a VARCHAR
  // and failing plan generation (-4002). Emitting `INTO idx(cols) SELECT cols`
  // with the same column list pins the binding by name, exactly as the kernel
  // DDL build-replica SQL (ObDDLUtil::generate_build_replica_sql) does.
  } else if (OB_FAIL(append_insert_column_list_(*param.select_columns_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append insert column list", K(ret));
  } else if (param.is_async_search_aux_index_) {
    if (OB_FAIL(append_search_data_gen_select_(param, true, allocator, sql))) {
      LOG_WARN("failed to append async search insert select", K(ret));
    }
  } else if (OB_FAIL(sql.append(" SELECT"))) {
    LOG_WARN("failed to append select", K(ret));
  } else if (OB_FAIL(append_select_hint_(param, sql))) {
    LOG_WARN("failed to append select hint", K(ret));
  } else if (OB_FAIL(sql.append(" "))) {
    LOG_WARN("failed to append space", K(ret));
  } else if (OB_FAIL(append_column_list_(*param.select_columns_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append select columns", K(ret));
  } else if (OB_FAIL(append_from_source_(param, allocator, sql))) {
    LOG_WARN("failed to append from source", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" WHERE `ora_rowscn` > %lu AND `OLD_NEW$$` = 'N'",
                                    param.last_refresh_scn_.get_val_for_sql()))) {
    LOG_WARN("failed to append where", K(ret));
  } else if (nullptr != param.order_columns_ && param.order_columns_->count() > 0) {
    if (OB_FAIL(sql.append(" ORDER BY "))) {
      LOG_WARN("failed to append order by", K(ret));
    } else if (OB_FAIL(append_column_list_(*param.order_columns_, is_oracle, allocator, sql))) {
      LOG_WARN("failed to append order by columns", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("generated async index insert-new sql", K(param), K(sql));
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::generate_complete_refresh_sql(const ObAsyncIndexSqlParam &param,
                                                            ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  ObArenaAllocator allocator("AsyncIdxSql");
  const bool is_oracle = param.is_oracle_mode_;
  if (OB_UNLIKELY(!param.is_valid_for_complete())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param for complete refresh sql", K(ret), K(param));
  } else if (OB_FAIL(sql.append("INSERT"))) {
    LOG_WARN("failed to append insert", K(ret));
  } else if (OB_FAIL(append_dml_hint_(param, sql))) {
    LOG_WARN("failed to append dml hint", K(ret));
  } else if (OB_FAIL(append_into_target_(param.database_name_, param.index_table_name_,
                                         param.index_partition_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append into target", K(ret));
  // Explicit INSERT column list -- see generate_insert_new_rows_sql for why this
  // is required (positional binding would route word_segment into the pk slot
  // and break PDML partition routing with -4002).
  } else if (OB_FAIL(append_insert_column_list_(*param.select_columns_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append insert column list", K(ret));
  } else if (param.is_async_search_aux_index_) {
    if (OB_FAIL(append_search_data_gen_select_(param, false, allocator, sql))) {
      LOG_WARN("failed to append async search complete select", K(ret));
    }
  } else if (OB_FAIL(sql.append(" SELECT"))) {
    LOG_WARN("failed to append select", K(ret));
  } else if (OB_FAIL(append_select_hint_(param, sql))) {
    LOG_WARN("failed to append select hint", K(ret));
  } else if (OB_FAIL(sql.append(" "))) {
    LOG_WARN("failed to append space", K(ret));
  } else if (OB_FAIL(append_column_list_(*param.select_columns_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append select columns", K(ret));
  } else if (OB_FAIL(append_from_source_(param, allocator, sql))) {
    LOG_WARN("failed to append from source", K(ret));
  // Intentionally NO `WHERE ora_rowscn > ... AND OLD_NEW$$ = ...`: a complete
  // refresh is a plain snapshot full scan, not an mview incremental delta scan.
  } else if (nullptr != param.order_columns_ && param.order_columns_->count() > 0) {
    if (OB_FAIL(sql.append(" ORDER BY "))) {
      LOG_WARN("failed to append order by", K(ret));
    } else if (OB_FAIL(append_column_list_(*param.order_columns_, is_oracle, allocator, sql))) {
      LOG_WARN("failed to append order by columns", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("generated async index complete-refresh sql", K(param), K(sql));
  }
  return ret;
}

int ObAsyncIndexSqlGenerator::generate_delete_old_rows_sql(const ObAsyncIndexSqlParam &param,
                                                           ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  ObArenaAllocator allocator("AsyncIdxSql");
  ObSEArray<ObString, 8> delete_columns;
  const bool is_oracle = param.is_oracle_mode_;
  if (OB_UNLIKELY(!param.is_valid_for_delete())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param for delete sql", K(ret), K(param));
  } else if (OB_FAIL(delete_columns.assign(*param.rowkey_columns_))) {
    LOG_WARN("failed to assign delete rowkey columns", K(ret));
  } else if (nullptr != param.partition_key_columns_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.partition_key_columns_->count(); ++i) {
      const ObString &routing_column = param.partition_key_columns_->at(i);
      bool found = false;
      for (int64_t j = 0; !found && j < delete_columns.count(); ++j) {
        found = delete_columns.at(j) == routing_column;
      }
      if (!found && OB_FAIL(delete_columns.push_back(routing_column))) {
        LOG_WARN("failed to append delete routing column", K(ret), K(i), K(routing_column));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append("INSERT"))) {
    LOG_WARN("failed to append insert", K(ret));
  } else if (OB_FAIL(append_dml_hint_(param, sql))) {
    LOG_WARN("failed to append dml hint", K(ret));
  } else if (OB_FAIL(append_into_target_(param.database_name_, param.delete_table_name_,
                                         param.index_partition_name_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append into target", K(ret));
  } else if (OB_FAIL(append_insert_column_list_(delete_columns, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append insert column list", K(ret));
  } else if (OB_FAIL(sql.append(" SELECT"))) {
    LOG_WARN("failed to append select", K(ret));
  } else if (OB_FAIL(append_select_hint_(param, sql))) {
    LOG_WARN("failed to append select hint", K(ret));
  } else if (OB_FAIL(sql.append(" "))) {
    LOG_WARN("failed to append space", K(ret));
  // Routing columns let the normal PARTITION_RANGE path calculate the target
  // tablet. ORDER BY below remains the physical delete-index rowkey.
  } else if (OB_FAIL(append_column_list_(delete_columns, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append delete columns", K(ret));
  } else if (OB_FAIL(append_from_source_(param, allocator, sql))) {
    LOG_WARN("failed to append from source", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" WHERE `ora_rowscn` > %lu AND `OLD_NEW$$` = 'O'",
                                    param.last_refresh_scn_.get_val_for_sql()))) {
    LOG_WARN("failed to append where", K(ret));
  // ORDER BY the rowkey is REQUIRED, not just for locality: this INSERT runs as a
  // PDML insert on the index build path, where ObInsertStmt::get_ddl_sort_keys()
  // pulls the child SELECT's order items as the DDL sort keys and errors with
  // -4016 if there are none. is_valid_for_delete() guarantees rowkey_columns_ is
  // non-empty, so this clause is always emitted.
  } else if (OB_FAIL(sql.append(" ORDER BY "))) {
    LOG_WARN("failed to append order by", K(ret));
  } else if (OB_FAIL(append_column_list_(*param.rowkey_columns_, is_oracle, allocator, sql))) {
    LOG_WARN("failed to append order by columns", K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("generated async index delete-old sql", K(param), K(sql));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
