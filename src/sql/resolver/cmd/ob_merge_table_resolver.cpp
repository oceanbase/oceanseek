/*
 * Copyright (c) 2025 OceanBase.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_merge_table_resolver.h"
#include "sql/resolver/cmd/ob_merge_table_stmt.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/ob_name_def.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{
// Only ordinary user tables: rejects views, materialized views and their container
// tables, external, temporary, system and virtual tables, and DDL hidden tables.
static bool is_mergeable_table(const ObTableSchema &schema)
{
  return schema.is_user_table() && !schema.mv_container_table() && !schema.is_user_hidden_table();
}

static const char *const COL_DEF_MSG = "MERGE TABLE on tables with different column definitions";

// Appends `name`, doubling embedded backquotes.
static int append_quoted(ObSqlString &sql, const ObString &name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("`"))) {
    LOG_WARN("failed to append opening backquote", K(ret), K(name));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    if (OB_FAIL('`' == name.ptr()[i] ? sql.append("``") : sql.append(name.ptr() + i, 1))) {
      LOG_WARN("failed to append quoted name character", K(ret), K(name), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append("`"))) {
    LOG_WARN("failed to append closing backquote", K(ret), K(name));
  }
  return ret;
}

static int append_table_name(ObSqlString &sql, const ObString &db_name, const ObString &table_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_quoted(sql, db_name))
      || OB_FAIL(sql.append("."))
      || OB_FAIL(append_quoted(sql, table_name))) {
    LOG_WARN("failed to append table name", K(ret), K(db_name), K(table_name));
  }
  return ret;
}

// "<prefix>c1, <prefix>c2, ..."
static int append_cols(ObSqlString &sql, const ObIArray<ObString> &cols, const char *prefix)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); ++i) {
    const ObString &col = cols.at(i);
    if (OB_FAIL(sql.append_fmt("%s%s%.*s", 0 == i ? "" : ", ", prefix, col.length(), col.ptr()))) {
      LOG_WARN("failed to append column", K(ret), K(i));
    }
  }
  return ret;
}

// "<lhs_prefix>k1 = <rhs_open>k1<rhs_close> AND ..."
static int append_key_eq(ObSqlString &sql, const ObIArray<ObString> &keys,
                  const char *lhs_prefix, const char *rhs_open, const char *rhs_close)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    const ObString &key = keys.at(i);
    if (OB_FAIL(sql.append_fmt("%s%s%.*s = %s%.*s%s", 0 == i ? "" : " AND ",
                               lhs_prefix, key.length(), key.ptr(),
                               rhs_open, key.length(), key.ptr(), rhs_close))) {
      LOG_WARN("failed to append key equality", K(ret), K(i));
    }
  }
  return ret;
}

// "__ob_merge_value_equal(c.v1, i.v1) AND ...", the same value comparison for FAIL and THEIRS.
// Primary key matching keeps the normal equality / unique key semantics instead.
static int append_value_equal(ObSqlString &sql, const ObIArray<ObString> &cols)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); ++i) {
    const ObString &col = cols.at(i);
    if (OB_FAIL(sql.append_fmt("%s%s(c.%.*s, i.%.*s)", 0 == i ? "" : " AND ",
                               N_MERGE_VALUE_EQUAL, col.length(), col.ptr(), col.length(), col.ptr()))) {
      LOG_WARN("failed to append value comparison", K(ret), K(i));
    }
  }
  return ret;
}

// "`c1` = __ob_merge_unique_check(NOT (<cur>.`k` = VALUES(`k`) AND ...), VALUES(`c1`)),
//  `c2` = VALUES(`c2`), ..."
// The first assignment carries the primary key guard; target key columns are
// qualified with the table name since the SELECT exposes the same names.
static int append_dup_update(ObSqlString &sql, const ObSqlString &cur_table,
                      const ObIArray<ObString> &pk_cols, const ObIArray<ObString> &update_cols)
{
  int ret = OB_SUCCESS;
  ObSqlString key_prefix;
  ObSqlString key_eq;
  if (OB_UNLIKELY(update_cols.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update columns are empty", K(ret), K(update_cols.count()));
  } else {
    const ObString &first = update_cols.at(0);
    if (OB_FAIL(key_prefix.append_fmt("%s.", cur_table.ptr()))
        || OB_FAIL(append_key_eq(key_eq, pk_cols, key_prefix.ptr(), "VALUES(", ")"))
        || OB_FAIL(sql.append_fmt(" ON DUPLICATE KEY UPDATE %.*s = %s(NOT (%s), VALUES(%.*s))",
                                  first.length(), first.ptr(), N_MERGE_UNIQUE_CHECK, key_eq.ptr(),
                                  first.length(), first.ptr()))) {
      LOG_WARN("failed to append guarded assignment", K(ret));
    }
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < update_cols.count(); ++i) {
    const ObString &col = update_cols.at(i);
    if (OB_FAIL(sql.append_fmt(", %.*s = VALUES(%.*s)", col.length(), col.ptr(), col.length(), col.ptr()))) {
      LOG_WARN("failed to append assignment", K(ret), K(i));
    }
  }
  return ret;
}

// Check the column mapping and the type contract used by MERGE's comparisons.
// INSERT applies the target's defaults, generated expressions and write constraints;
// their definitions need not match the source.
static int check_merge_column_compatibility(const ObColumnSchemaV2 &lhs,
                                            const ObColumnSchemaV2 &rhs, bool &is_same)
{
  int ret = OB_SUCCESS;
  bool is_same_type = false;
  is_same = false;
  if (lhs.get_column_name_str() != rhs.get_column_name_str()) {
    // Columns are paired in logical column order and referenced by name in the inner SQL.
  } else if (lhs.get_rowkey_position() != rhs.get_rowkey_position()
             || lhs.is_heap_table_primary_key_column() != rhs.is_heap_table_primary_key_column()) {
    // Both tables must identify a row with the same key, including HEAP logical keys.
    LOG_WARN("column primary key mismatch", K(ret), K(lhs), K(rhs));
  } else if (OB_FAIL(ObTableSchema::check_is_exactly_same_type(lhs, rhs, is_same_type))) {
    // Value comparison does not cast its operands. Keep the existing exact type
    // contract (including accuracy and collation) through the shared schema helper.
    LOG_WARN("failed to compare exact column type", K(ret), K(lhs), K(rhs));
  } else if (!is_same_type) {
    LOG_WARN("column type mismatch", K(ret), K(lhs), K(rhs));
  } else {
    is_same = true;
    if (ob_is_enumset_tc(lhs.get_data_type())) {
      // ENUM/SET datums contain ordinals/bitmaps. Equal bytes must denote equal
      // labels; the shared helper only compares labels using their collation.
      const ObIArray<ObString> &lhs_labels = lhs.get_extended_type_info();
      const ObIArray<ObString> &rhs_labels = rhs.get_extended_type_info();
      is_same = lhs_labels.count() == rhs_labels.count();
      for (int64_t i = 0; is_same && i < lhs_labels.count(); ++i) {
        is_same = lhs_labels.at(i) == rhs_labels.at(i);
      }
    }
  }
  return ret;
}

// INVISIBLE columns are user data; only internal hidden columns are skipped.
static int get_user_columns(const ObTableSchema &table, ObIArray<const ObColumnSchemaV2 *> &cols)
{
  int ret = OB_SUCCESS;
  // DDL can change logical column order without rearranging the schema array.
  ObColumnIterByPrevNextID iter(table);
  const ObColumnSchemaV2 *column_schema = NULL;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.next(column_schema))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to iterate columns in logical order", K(ret));
      }
      break;
    } else if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret));
    } else if (!column_schema->is_hidden() && OB_FAIL(cols.push_back(column_schema))) {
      LOG_WARN("failed to push column", K(ret));
    }
  }
  return ret;
}

// Resolve the MERGE TABLE parse tree: extract table names and merge strategy,
// validate both table schemas, then build the internal SQL into ObMergeTableStmt.
int ObMergeTableResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObMergeTableStmt *stmt = NULL;
  const ObTableSchema *cur_schema = NULL;
  const ObTableSchema *inc_schema = NULL;
  MergeColumns cols;
  uint64_t min_data_version = 0;
  if (OB_ISNULL(session_info_) || OB_ISNULL(schema_checker_) || OB_ISNULL(params_.allocator_)
      || T_MERGE_TABLE != parse_tree.type_ || MERGE_TABLE_NODE_COUNT != parse_tree.num_child_
      || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree for MERGE TABLE", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), min_data_version))) {
    LOG_WARN("failed to get tenant min data version", K(ret));
  } else if (min_data_version < DATA_VERSION_4_6_3_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "MERGE TABLE during upgrade");
    LOG_WARN("tenant data version does not support MERGE TABLE", K(ret), K(min_data_version));
  } else if (OB_ISNULL(stmt = create_stmt<ObMergeTableStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate merge table stmt", K(ret));
  } else if (OB_FAIL(resolve_table_names_and_strategy_(parse_tree, *stmt))) {
    LOG_WARN("failed to resolve table names and strategy", K(ret));
  } else if (OB_FAIL(get_table_schemas_(*stmt, cur_schema, inc_schema))) {
    LOG_WARN("failed to get table schemas", K(ret));
  } else if (OB_FAIL(stmt->add_global_dependency_table(ObSchemaObjVersion(
                 cur_schema->get_table_id(), cur_schema->get_schema_version(), DEPENDENCY_TABLE)))
             || OB_FAIL(stmt->add_global_dependency_table(ObSchemaObjVersion(
                 inc_schema->get_table_id(), inc_schema->get_schema_version(), DEPENDENCY_TABLE)))) {
    LOG_WARN("failed to record merge table schema dependencies", K(ret));
  } else if (OB_FAIL(collect_columns_(*cur_schema, *inc_schema, cols))) {
    LOG_WARN("failed to collect merge columns", K(ret));
  } else if (OB_FAIL(build_merge_sql_(*stmt, cols, parse_tree.children_[HINT_NODE]))) {
    LOG_WARN("failed to build merge sql", K(ret));
  } else {
    LOG_TRACE("MERGE TABLE resolved", "strategy", stmt->get_strategy(),
              "merge_sql", stmt->get_merge_sql());
  }
  return ret;
}

int ObMergeTableResolver::resolve_table_names_and_strategy_(const ParseNode &parse_tree,
                                                            ObMergeTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  const ParseNode *strategy_node = parse_tree.children_[STRATEGY_NODE];
  const ParseNode *inc_table_node = parse_tree.children_[INCOMING_TABLE_NODE];
  const ParseNode *cur_table_node = parse_tree.children_[CURRENT_TABLE_NODE];
  ObString inc_table_name;
  ObString inc_db_name;
  ObString cur_table_name;
  ObString cur_db_name;
  bool inc_has_dblink = false;
  bool cur_has_dblink = false;
  // Check explicit links before resolving names, which may require a default database.
  if (OB_ISNULL(strategy_node) || OB_ISNULL(inc_table_node) || OB_ISNULL(cur_table_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge table node is NULL", K(ret), KP(strategy_node), KP(inc_table_node), KP(cur_table_node));
  } else if (OB_UNLIKELY(T_RELATION_FACTOR != inc_table_node->type_
                        || T_RELATION_FACTOR != cur_table_node->type_
                        || inc_table_node->num_child_ < 2 || cur_table_node->num_child_ < 2)
             || OB_ISNULL(inc_table_node->children_) || OB_ISNULL(cur_table_node->children_)
             || OB_ISNULL(inc_table_node->children_[1]) || OB_ISNULL(cur_table_node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge table relation node", K(ret), KP(inc_table_node), KP(cur_table_node));
  } else if (OB_FAIL(resolve_dblink_info(inc_table_node, NULL, NULL, &inc_has_dblink))
             || OB_FAIL(resolve_dblink_info(cur_table_node, NULL, NULL, &cur_has_dblink))) {
    LOG_WARN("failed to resolve merge table database links", K(ret));
  } else if (inc_has_dblink || cur_has_dblink) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "MERGE TABLE with database links");
    LOG_WARN("merge table does not support database links", K(ret), K(inc_has_dblink), K(cur_has_dblink));
  } else if (OB_FAIL(resolve_table_relation_node(inc_table_node, inc_table_name, inc_db_name))) {
    LOG_WARN("failed to resolve incoming table", K(ret));
  } else if (OB_FAIL(resolve_table_relation_node(cur_table_node, cur_table_name, cur_db_name))) {
    LOG_WARN("failed to resolve current table", K(ret));
  } else if (strategy_node->value_ < static_cast<int64_t>(ObMergeTableStrategy::FAIL)
             || strategy_node->value_ >= static_cast<int64_t>(ObMergeTableStrategy::MAX)) {
    // The parser produces the ObMergeTableStrategy values (see sql_parser_mysql_mode.y).
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown merge strategy", K(ret), K(strategy_node->value_));
  } else {
    stmt.set_cur_db_name(cur_db_name);
    stmt.set_cur_table_name(cur_table_name);
    stmt.set_inc_db_name(inc_db_name);
    stmt.set_inc_table_name(inc_table_name);
    stmt.set_strategy(static_cast<ObMergeTableStrategy>(strategy_node->value_));
  }
  return ret;
}

int ObMergeTableResolver::get_table_schemas_(const ObMergeTableStmt &stmt,
                                              const ObTableSchema *&cur_schema,
                                              const ObTableSchema *&inc_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = session_info_->get_effective_tenant_id();
  if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, stmt.get_cur_db_name(),
                                                stmt.get_cur_table_name(), false, cur_schema))) {
    LOG_WARN("failed to get current table schema", K(ret));
  } else if (OB_ISNULL(cur_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("current table not exist", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, stmt.get_inc_db_name(),
                                                       stmt.get_inc_table_name(), false, inc_schema))) {
    LOG_WARN("failed to get incoming table schema", K(ret));
  } else if (OB_ISNULL(inc_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("incoming table not exist", K(ret));
  } else if (!is_mergeable_table(*cur_schema) || !is_mergeable_table(*inc_schema)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "MERGE TABLE on non-user tables");
    LOG_WARN("merge table only supports user tables", K(ret),
             "cur_table_type", cur_schema->get_table_type(),
             "inc_table_type", inc_schema->get_table_type());
  } else if (cur_schema->get_table_id() == inc_schema->get_table_id()) {
    // Compare ids so differently qualified names of one table are rejected too.
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "MERGE TABLE with the same source and target table");
    LOG_WARN("merge source and target are the same table", K(ret));
  }
  return ret;
}

// Both tables must have the same user columns in the same order. Vector/collection
// columns are LOB-stored and compared by content; UDT columns are not supported.
int ObMergeTableResolver::collect_columns_(const ObTableSchema &cur_schema,
                                           const ObTableSchema &inc_schema,
                                           MergeColumns &cols)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObColumnSchemaV2 *, 16> cur_cols;
  ObSEArray<const ObColumnSchemaV2 *, 16> inc_cols;
  ObSqlString quoted;
  ObString name;
  if (OB_FAIL(get_user_columns(cur_schema, cur_cols))
      || OB_FAIL(get_user_columns(inc_schema, inc_cols))) {
    LOG_WARN("failed to get user columns", K(ret));
  } else if (cur_cols.count() != inc_cols.count()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("column count mismatch", K(ret), K(cur_cols.count()), K(inc_cols.count()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, COL_DEF_MSG);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_cols.count(); ++i) {
    quoted.reset();
    name.reset();
    const ObColumnSchemaV2 &col = *cur_cols.at(i);
    bool is_same = false;
    if (OB_FAIL(check_merge_column_compatibility(col, *inc_cols.at(i), is_same))) {
      LOG_WARN("failed to compare column definitions", K(ret), K(col));
    } else if (!is_same) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("column definition mismatch", K(ret), K(col), KPC(inc_cols.at(i)));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, COL_DEF_MSG);
    } else if (col.is_extend()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "MERGE TABLE with UDT columns");
      LOG_WARN("unsupported merge column type", K(ret), K(col));
    } else if (OB_FAIL(append_quoted(quoted, col.get_column_name_str()))
               || OB_FAIL(ob_write_string(*params_.allocator_, quoted.string(), name, true))) {
      LOG_WARN("failed to quote column name", K(ret), K(col));
    } else {
      const bool is_pk = col.is_rowkey_column() || col.is_heap_table_primary_key_column();
      // Compare every user column, but only write columns writable in the target.
      // Target generated expressions may differ and are evaluated by INSERT.
      const bool is_written = !col.is_generated_column();
      if (is_pk) {
        if (OB_FAIL(cols.pk_.push_back(name))) {
          LOG_WARN("failed to push primary key column", K(ret), K(name));
        }
      } else if (OB_FAIL(cols.val_.push_back(name))) {
        LOG_WARN("failed to push value column", K(ret), K(name));
      }
      if (OB_SUCC(ret) && is_written) {
        if (OB_FAIL(cols.insert_.push_back(name))
            || (!is_pk && OB_FAIL(cols.update_.push_back(name)))) {
          LOG_WARN("failed to push writable column", K(ret), K(name));
        }
      }
    }
  }
  if (OB_SUCC(ret) && cols.pk_.empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("table requires a primary key", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "MERGE TABLE on table without primary key");
  }
  return ret;
}

// The single INSERT statement executed for every strategy. Matching, value
// comparison, conflict detection and writing share one statement snapshot, so
// no row can change between the check and the write even under READ COMMITTED.
// Example (cur = `test`.`users`, inc = `test`.`users_inc`, pk = [id], val = [name, age]):
//   INSERT INTO `test`.`users` (`id`, `name`, `age`)
//   SELECT i.`id`, i.`name`, i.`age` FROM `test`.`users_inc` i
//   WHERE NOT EXISTS (SELECT 1 FROM `test`.`users` c WHERE <skip condition>)
//   [ON DUPLICATE KEY UPDATE ...]
// Rows are copied as they are. The executor temporarily enables
// NO_AUTO_VALUE_ON_ZERO so an explicit 0 in an auto-increment column is preserved.
// The strategy decides the rest:
//   OURS  : skip c.`id` = i.`id`, so only incoming-only rows are inserted.
//   FAIL  : skip c.`id` = i.`id` AND __ob_merge_conflict_check(c.`id` = i.`id` AND NOT (...))
//           Each matched pair is checked inside the anti join and the statement
//           aborts on the first conflict. The guard repeats the key equality to
//           stay correct regardless of join condition evaluation order.
//   THEIRS: skip c.`id` = i.`id` AND __ob_merge_value_equal(c.`name`, i.`name`) AND ...
//           so identical rows are left untouched, then
//           ON DUPLICATE KEY UPDATE
//             `name` = __ob_merge_unique_check(NOT (`test`.`users`.`id` = VALUES(`id`)),
//                                              VALUES(`name`)),
//             `age` = VALUES(`age`)
//           ON DUPLICATE KEY UPDATE fires on any unique key, the guard aborts
//           when the conflicting row has a different primary key and otherwise
//           passes VALUES(`name`) through with its own type.
int ObMergeTableResolver::build_merge_sql_(ObMergeTableStmt &stmt, const MergeColumns &cols,
                                          const ParseNode *hint_node)
{
  int ret = OB_SUCCESS;
  const ObMergeTableStrategy strategy = stmt.get_strategy();
  const bool has_val = !cols.val_.empty();
  const bool check_matched = ObMergeTableStrategy::FAIL == strategy && has_val;
  const bool update_matched = ObMergeTableStrategy::THEIRS == strategy && has_val && !cols.update_.empty();
  ObSqlString cur_table;
  ObSqlString inc_table;
  ObSqlString pk_eq;
  ObSqlString val_eq;
  ObSqlString sql;
  ObString merge_sql;
  ObString hint;
  // As in LOAD DATA URL, keep the original hint text on the generated INSERT.
  // The inner DML resolver handles PARALLEL, ENABLE_PARALLEL_DML and other hints.
  if (OB_NOT_NULL(hint_node)) {
    if (T_HINT_OPTION_LIST != hint_node->type_ || hint_node->str_len_ <= 0
        || OB_ISNULL(hint_node->str_value_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid merge table hint", K(ret), KP(hint_node));
    } else {
      const char *hint_start = strstr(hint_node->str_value_, "/*");
      if (OB_ISNULL(hint_start)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("merge table hint comment is missing", K(ret), KP(hint_node));
      } else {
        hint.assign_ptr(hint_start, hint_node->str_len_ - (hint_start - hint_node->str_value_));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_table_name(cur_table, stmt.get_cur_db_name(), stmt.get_cur_table_name()))
      || OB_FAIL(append_table_name(inc_table, stmt.get_inc_db_name(), stmt.get_inc_table_name()))
      || OB_FAIL(append_key_eq(pk_eq, cols.pk_, "c.", "i.", ""))
      || (has_val && OB_FAIL(append_value_equal(val_eq, cols.val_)))) {
    LOG_WARN("failed to build merge sql parts", K(ret));
  } else if (OB_FAIL(sql.append("INSERT"))
             || (!hint.empty() && OB_FAIL(sql.append_fmt(" %.*s", hint.length(), hint.ptr())))
             || OB_FAIL(sql.append_fmt(" INTO %s (", cur_table.ptr()))
             || OB_FAIL(append_cols(sql, cols.insert_, ""))
             || OB_FAIL(sql.append(") SELECT "))
             || OB_FAIL(append_cols(sql, cols.insert_, "i."))
             || OB_FAIL(sql.append_fmt(" FROM %s i WHERE NOT EXISTS (SELECT 1 FROM %s c WHERE %s",
                                       inc_table.ptr(), cur_table.ptr(), pk_eq.ptr()))) {
    LOG_WARN("failed to build merge insert", K(ret));
  } else if (check_matched && OB_FAIL(sql.append_fmt(" AND %s(%s AND NOT (%s))",
                                                     N_MERGE_CONFLICT_CHECK, pk_eq.ptr(), val_eq.ptr()))) {
    LOG_WARN("failed to append conflict check", K(ret));
  } else if (update_matched && OB_FAIL(sql.append_fmt(" AND %s", val_eq.ptr()))) {
    LOG_WARN("failed to append value comparison", K(ret));
  } else if (OB_FAIL(sql.append(")"))) {
    LOG_WARN("failed to close anti join", K(ret));
  } else if (update_matched && OB_FAIL(append_dup_update(sql, cur_table, cols.pk_, cols.update_))) {
    LOG_WARN("failed to append duplicate key update", K(ret));
  } else if (OB_FAIL(ob_write_string(*params_.allocator_, sql.string(), merge_sql))) {
    LOG_WARN("failed to copy merge sql", K(ret));
  } else {
    stmt.set_merge_sql(merge_sql);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
