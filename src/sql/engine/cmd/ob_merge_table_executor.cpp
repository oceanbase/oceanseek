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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_merge_table_executor.h"
#include "sql/resolver/cmd/ob_merge_table_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/ob_sql.h"
#include "sql/ob_result_set.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
namespace sql
{
namespace
{
// Validate against the very guard used to compile the inner INSERT, on every
// inner retry as well. A fresh guard obtained separately here would leave a gap
// between the validation and compilation. OB_SCHEMA_ERROR is returned to the
// outer query (not retried inside SPI), so MERGE rebuilds its entire column list.
class ObMergeTableSqlExecutor : public common::sqlclient::ObIExecutor
{
public:
  ObMergeTableSqlExecutor(const ObMergeTableStmt &stmt, int64_t &affected_rows)
    : stmt_(stmt), affected_rows_(affected_rows)
  {}

  int execute(ObSql &engine, ObSqlCtx &ctx, ObResultSet &res) override
  {
    int ret = OB_SUCCESS;
    bool table_version_equal = true;
    int64_t schema_version = 0;
    const ObIArray<share::schema::ObSchemaObjVersion> *dependencies = stmt_.get_global_dependency_table();
    const ObString &sql = stmt_.get_merge_sql();
    SQL_INFO_GUARD(sql, ObString(OB_MAX_SQL_ID_LENGTH, ctx.sql_id_));
    if (OB_ISNULL(ctx.schema_guard_) || OB_ISNULL(ctx.session_info_)
        || OB_ISNULL(dependencies) || dependencies->count() < 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge table schema dependencies are missing", K(ret), KP(dependencies));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && table_version_equal && i < dependencies->count(); ++i) {
        const share::schema::ObSchemaObjVersion &dep = dependencies->at(i);
        if (share::schema::DEPENDENCY_TABLE == dep.get_type()) {
          if (OB_FAIL(ctx.schema_guard_->get_schema_version(
                  share::schema::TABLE_SCHEMA, ctx.session_info_->get_effective_tenant_id(),
                  dep.get_object_id(), schema_version))) {
            LOG_WARN("failed to get merge table schema version", K(ret), K(dep));
          } else {
            table_version_equal = dep.get_version() == schema_version;
          }
        }
      }
      if (OB_SUCC(ret) && !table_version_equal) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("merge table schema changed since resolve", K(ret), KPC(dependencies));
      }
    }
    if (OB_SUCC(ret)) {
      ObString query;
      if (OB_FAIL(ob_write_string(res.get_mem_pool(), sql, query, true))) {
        LOG_WARN("failed to copy merge SQL", K(ret));
      } else {
        res.get_session().store_query_string(query);
        if (OB_FAIL(engine.stmt_query(query, ctx, res))) {
          LOG_WARN("failed to compile merge SQL", K(ret));
        } else {
          DEBUG_SYNC(AFTER_MERGE_TABLE_INNER_RESOLVE);
        }
      }
    }
    return ret;
  }

  int process_result(ObResultSet &res) override
  {
    affected_rows_ = res.get_affected_rows();
    return OB_SUCCESS;
  }

  TO_STRING_KV("merge_sql", stmt_.get_merge_sql());

private:
  const ObMergeTableStmt &stmt_;
  int64_t &affected_rows_;
};
} // namespace

// Run the single INSERT statement prebuilt by the resolver via an inner SQL
// connection sharing the caller's session and transaction. Conflict detection
// and writing happen in that one statement:
//   FAIL   : inserts incoming-only rows, aborts on the first conflict.
//   THEIRS : inserts incoming-only rows, overwrites conflicts (incoming side wins).
//   OURS   : inserts incoming-only rows only (current side keeps its values).
// Being a single DML, it needs no extra transaction control: on failure its own
// statement rollback undoes partial writes inside a caller transaction, and in
// autocommit mode the inner statement commits or rolls back by itself.
int ObMergeTableExecutor::execute(ObExecContext &ctx, ObMergeTableStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObInnerSQLConnectionPool *pool = NULL;
  ObInnerSQLConnection *conn = NULL;
  int64_t affected_rows = 0;
  ObObj saved_sql_mode;
  bool need_restore_sql_mode = false;

  if (OB_ISNULL(session) || OB_ISNULL(ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or session is null", K(ret), K(OB_ISNULL(session)), K(OB_ISNULL(ctx.get_sql_proxy())));
  } else if (OB_ISNULL(pool = static_cast<ObInnerSQLConnectionPool *>(ctx.get_sql_proxy()->get_pool()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection pool is null", K(ret));
  } else if (OB_FAIL(pool->acquire_spi_conn(session, conn))) {
    LOG_WARN("failed to acquire inner sql connection", K(ret));
  } else if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner sql connection is null", K(ret));
  } else {
    // RLS bypass must be checked against the caller's actual BYPASSRLS privilege.
    conn->set_check_priv(true);
  }

  // Keep copied zero values through the existing session variable protocol.
  // The SPI connection shares this session, so restore it on every exit path.
  if (OB_SUCC(ret) && !(session->get_sql_mode() & SMO_NO_AUTO_VALUE_ON_ZERO)) {
    if (OB_FAIL(session->get_sys_variable(share::SYS_VAR_SQL_MODE, saved_sql_mode))) {
      LOG_WARN("failed to save sql mode", K(ret));
    } else {
      ObObj merge_sql_mode;
      merge_sql_mode.set_uint64(session->get_sql_mode() | SMO_NO_AUTO_VALUE_ON_ZERO);
      need_restore_sql_mode = true;
      if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_SQL_MODE, merge_sql_mode))) {
        LOG_WARN("failed to set merge sql mode", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    DEBUG_SYNC(BEFORE_MERGE_TABLE_INNER_SQL);
    ObMergeTableSqlExecutor executor(stmt, affected_rows);
    if (OB_FAIL(conn->execute(session->get_effective_tenant_id(), executor))) {
      LOG_WARN("failed to execute merge sql", K(ret), K(stmt.get_merge_sql()));
    } else {
      ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
      if (OB_NOT_NULL(plan_ctx)) {
        plan_ctx->set_affected_rows(affected_rows);
      }
      LOG_TRACE("merge sql executed", K(affected_rows));
    }
  }

  if (OB_NOT_NULL(conn)) {
    ctx.get_sql_proxy()->close(conn, ret);
  }
  if (need_restore_sql_mode) {
    int tmp_ret = session->update_sys_variable(share::SYS_VAR_SQL_MODE, saved_sql_mode);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to restore sql mode", K(tmp_ret), K(ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
