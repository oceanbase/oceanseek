/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_gc_control_operator.h"

#include "lib/worker.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_timeout_ctx.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_rpc_struct.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

int ObSharedGCControlOperator::insert_row(
    ObISQLClient &sql_client,
    const ObSharedGCControlInfo &info,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert_row get invalid argument", KR(ret), K(info));
  } else if (OB_FAIL(get_real_tenant_id_(info.tenant_id_, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(info.tenant_id_));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(info.tenant_id_, timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "INSERT /*+ QUERY_TIMEOUT(%ld) */ INTO %s "
      "(tenant_id, executor_server_id, status, gc_round_id, allow_gc_round_id, gc_meta_scn, last_succ_gc_meta_scn) "
      "VALUES (%lu, %ld, '%s', %ld, %ld, %lu, %lu)",
      timeout_us,
      OB_ALL_SHARED_GC_CONTROL_TNAME,
      info.tenant_id_,
      info.executor_server_id_,
      shared_gc_control_status_to_str(info.status_),
      info.gc_round_id_,
      info.allow_gc_round_id_,
      info.gc_meta_scn_.get_val_for_inner_table_field(),
      info.last_succ_gc_meta_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(info));
  } else if (OB_FAIL(do_sql_write_(sql_client, real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to do sql write", KR(ret), K(sql));
  }
  return ret;
}

// Insert a default row (executor_server_id=0, status='IDLE', gc_round_id=0,
// allow_gc_round_id=0, gc_meta_scn=0, last_succ_gc_meta_scn=0) for the given business tenant_id.
// Used by the tenant-create flow to initialize the row alongside tenant creation.
// executor_server_id=0 is meaningless when status=IDLE; it is overwritten by
// update_executor_state when a server takes over and flips status to DOING.
int ObSharedGCControlOperator::insert_default_row(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert_default_row get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "INSERT /*+ QUERY_TIMEOUT(%ld) */ INTO %s "
      "(tenant_id, executor_server_id, status, gc_round_id, allow_gc_round_id, gc_meta_scn, last_succ_gc_meta_scn) "
      "VALUES (%lu, 0, '%s', 0, 0, 0, 0)",
      timeout_us,
      OB_ALL_SHARED_GC_CONTROL_TNAME,
      tenant_id,
      shared_gc_control_status_to_str(ObSharedGCControlStatus::IDLE)))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id));
  } else if (OB_FAIL(do_sql_write_(sql_client, real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to do sql write", KR(ret), K(sql));
  }
  LOG_INFO("insert default shared gc control info row", KR(ret), K(tenant_id), K(real_tenant_id), K(affected_rows));
  return ret;
}

int ObSharedGCControlOperator::delete_row(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete_row get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s WHERE tenant_id = %lu",
      timeout_us,
      OB_ALL_SHARED_GC_CONTROL_TNAME,
      tenant_id))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id));
  } else if (OB_FAIL(do_sql_write_(sql_client, real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to do sql write", KR(ret), K(sql));
  }
  LOG_INFO("delete shared gc control info row", KR(ret), K(tenant_id), K(affected_rows));
  return ret;
}

int ObSharedGCControlOperator::update_row(
    ObISQLClient &sql_client,
    const ObSharedGCControlInfo &info,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const uint64_t cur_server_id = GCTX.get_server_id();
  ObSqlString set_clause;

  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_row get invalid argument", KR(ret), K(info));
  } else if (info.executor_server_id_ != static_cast<int64_t>(cur_server_id)) {
    // Full-row overwrite must not be used to "demote" the local server. For
    // takeover use update_executor_state() inside a select-for-update trans.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_row's executor_server_id must equal cur_server_id",
        KR(ret), K(info), K(cur_server_id));
  } else if (OB_FAIL(set_clause.assign_fmt(
      "executor_server_id = %ld, status = '%s', allow_gc_round_id = %ld, "
      "gc_meta_scn = %lu, last_succ_gc_meta_scn = %lu",
      info.executor_server_id_,
      shared_gc_control_status_to_str(info.status_),
      info.allow_gc_round_id_,
      info.gc_meta_scn_.get_val_for_inner_table_field(),
      info.last_succ_gc_meta_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to build set clause", KR(ret), K(info));
  } else if (OB_FAIL(do_update_set_clause_(sql_client, info.tenant_id_, set_clause,
                                           true /* with_server_condition */,
                                           affected_rows))) {
    LOG_WARN("fail to update row", KR(ret), K(info));
  }
  return ret;
}

int ObSharedGCControlOperator::update_allow_gc_round_id_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t allow_gc_round_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObSqlString set_clause;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (allow_gc_round_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_allow_gc_round_id_ get invalid argument", KR(ret),
        K(tenant_id), K(allow_gc_round_id));
  } else if (OB_FAIL(set_clause.assign_fmt(
      "allow_gc_round_id = %ld", allow_gc_round_id))) {
    LOG_WARN("fail to build set clause", KR(ret), K(allow_gc_round_id));
  } else if (OB_FAIL(do_update_set_clause_(sql_client, tenant_id, set_clause,
                                           true /* with_server_condition */,
                                           affected_rows))) {
    LOG_WARN("fail to update allow_gc_round_id", KR(ret),
        K(tenant_id), K(allow_gc_round_id));
  }
  return ret;
}

int ObSharedGCControlOperator::update_allow_gc_round_id(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t round_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const uint64_t cur_server_id = GCTX.get_server_id();
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  int64_t stmt_timeout = 0;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;
  ObSharedGCControlInfo info;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (round_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_allow_gc_round_id get invalid argument", KR(ret),
        K(tenant_id), K(round_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, stmt_timeout))) {
    LOG_WARN("fail to get sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", KR(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("fail to set timeout", KR(ret), K(stmt_timeout));
  } else if (OB_FAIL(trans.start(&sql_client, real_tenant_id, false /*with_snapshot*/,
                                 OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id), K(real_tenant_id));
  } else {
    // Lock the row, then update allow_gc_round_id only when the current executor is
    // still self AND the table's gc_round_id matches this round_id. This guards against
    // a stale dag (left over from a previous round / before a switch-leader) advancing
    // the watermark after another server has taken over.
    if (OB_FAIL(select_tenant_id_for_update(trans, tenant_id, info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_WARN("control info row not exist", KR(ret), K(tenant_id), K(round_id));
      } else {
        LOG_WARN("fail to select for update", KR(ret), K(tenant_id));
      }
    } else if ((info.executor_server_id_ != static_cast<int64_t>(cur_server_id))
               || (info.gc_round_id_ != round_id)) {
      LOG_INFO("executor or round_id changed, skip update allow_gc_round_id",
          K(tenant_id), K(cur_server_id), K(round_id), K(info));
    } else if (OB_UNLIKELY(round_id <= info.allow_gc_round_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("allow_gc_round_id should not fall back", KR(ret),
          K(tenant_id), K(cur_server_id), K(round_id), K(info));
    } else if (OB_FAIL(update_allow_gc_round_id_(trans, tenant_id, round_id, affected_rows))) {
      LOG_WARN("fail to update allow_gc_round_id", KR(ret), K(tenant_id), K(round_id), K(info));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to update allow_gc_round_id", KR(ret), K(tenant_id), K(cur_server_id),
          K(round_id), K(affected_rows), K(info));
    } else {
      LOG_INFO("succ to update allow_gc_round_id", K(tenant_id), K(cur_server_id),
          K(round_id), K(affected_rows), K(info));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCCESS == ret /*commit*/))) {
      LOG_WARN("fail to end trans", KR(tmp_ret), K(tenant_id), K(round_id));
      ret = ((OB_SUCCESS == ret) ? tmp_ret : ret);
    }
  }
  return ret;
}

int ObSharedGCControlOperator::update_status(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObSharedGCControlStatus status,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObSqlString set_clause;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                || !is_valid_shared_gc_control_status(status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_status get invalid argument", KR(ret), K(tenant_id), K(status));
  } else if (OB_FAIL(set_clause.assign_fmt(
      "status = '%s'", shared_gc_control_status_to_str(status)))) {
    LOG_WARN("fail to build set clause", KR(ret), K(status));
  } else if (OB_FAIL(do_update_set_clause_(sql_client, tenant_id, set_clause,
                                           true /* with_server_condition */,
                                           affected_rows))) {
    LOG_WARN("fail to update status", KR(ret), K(tenant_id), K(status));
  }
  return ret;
}

int ObSharedGCControlOperator::update_gc_meta_scn_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const SCN gc_meta_scn,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObSqlString set_clause;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !gc_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_gc_meta_scn get invalid argument", KR(ret), K(tenant_id), K(gc_meta_scn));
  } else if (OB_FAIL(set_clause.assign_fmt(
      "gc_meta_scn = %lu", gc_meta_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to build set clause", KR(ret), K(gc_meta_scn));
  } else if (OB_FAIL(do_update_set_clause_(sql_client, tenant_id, set_clause,
                                           true /* with_server_condition */,
                                           affected_rows))) {
    LOG_WARN("fail to update gc_meta_scn", KR(ret), K(tenant_id), K(gc_meta_scn));
  }
  return ret;
}

int ObSharedGCControlOperator::update_last_succ_gc_meta_scn_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const SCN last_succ_gc_meta_scn,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObSqlString set_clause;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !last_succ_gc_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_last_succ_gc_meta_scn get invalid argument", KR(ret),
        K(tenant_id), K(last_succ_gc_meta_scn));
  } else if (OB_FAIL(set_clause.assign_fmt(
      "last_succ_gc_meta_scn = %lu",
      last_succ_gc_meta_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to build set clause", KR(ret), K(last_succ_gc_meta_scn));
  } else if (OB_FAIL(do_update_set_clause_(sql_client, tenant_id, set_clause,
                                           true /* with_server_condition */,
                                           affected_rows))) {
    LOG_WARN("fail to update last_succ_gc_meta_scn", KR(ret),
        K(tenant_id), K(last_succ_gc_meta_scn));
  }
  return ret;
}

int ObSharedGCControlOperator::update_executor_state(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t executor_server_id,
    const ObSharedGCControlStatus status,
    const int64_t gc_round_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObSqlString set_clause;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                || (executor_server_id < 0)
                || !is_valid_shared_gc_control_status(status)
                || (gc_round_id <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_executor_state get invalid argument", KR(ret),
        K(tenant_id), K(executor_server_id), K(status), K(gc_round_id));
  } else if (OB_FAIL(set_clause.assign_fmt(
      "executor_server_id = %ld, status = '%s', gc_round_id = %ld",
      executor_server_id,
      shared_gc_control_status_to_str(status),
      gc_round_id))) {
    LOG_WARN("fail to build set clause", KR(ret), K(executor_server_id), K(status), K(gc_round_id));
  } else if (OB_FAIL(do_update_set_clause_(sql_client, tenant_id, set_clause,
                                           false /* with_server_condition */,
                                           affected_rows))) {
    LOG_WARN("fail to update executor state", KR(ret),
        K(tenant_id), K(executor_server_id), K(status), K(gc_round_id));
  }
  LOG_TRACE("update shared gc control info executor state",
      KR(ret), K(tenant_id), K(executor_server_id), K(status), K(gc_round_id), K(affected_rows));
  return ret;
}

int ObSharedGCControlOperator::update_gc_meta_scn(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t round_id,
    const SCN &new_gc_meta_scn,
    bool &is_updated)
{
  return update_gc_meta_scn_(sql_client, tenant_id, round_id, new_gc_meta_scn,
                             GCMetaScnColumn::GC_META_SCN, is_updated);
}

int ObSharedGCControlOperator::update_gc_meta_scn_in_trans(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t round_id,
    const SCN &new_gc_meta_scn,
    bool &is_updated)
{
  return update_scn_in_trans_(trans, tenant_id, round_id, new_gc_meta_scn,
                              GCMetaScnColumn::GC_META_SCN, is_updated);
}

int ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t round_id,
    const SCN &new_last_succ_gc_meta_scn)
{
  bool is_updated = false;
  return update_gc_meta_scn_(sql_client, tenant_id, round_id, new_last_succ_gc_meta_scn,
                             GCMetaScnColumn::LAST_SUCC_GC_META_SCN, is_updated);
}

const char *ObSharedGCControlOperator::gc_meta_scn_column_to_str(const GCMetaScnColumn column)
{
  const char *str = nullptr;
  switch (column) {
    case GCMetaScnColumn::GC_META_SCN:
      str = "gc_meta_scn";
      break;
    case GCMetaScnColumn::LAST_SUCC_GC_META_SCN:
      str = "last_succ_gc_meta_scn";
      break;
    default:
      str = nullptr; // unknown column: caller must treat as an error
      break;
  }
  return str;
}

int ObSharedGCControlOperator::update_gc_meta_scn_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t round_id,
    const SCN &new_scn,
    const GCMetaScnColumn column,
    bool &is_updated)
{
  int ret = OB_SUCCESS;
  is_updated = false;
  const char *scn_name = gc_meta_scn_column_to_str(column);
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  int64_t stmt_timeout = 0;
  ObTimeoutCtx timeout_ctx;
  ObMySQLTransaction trans;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (round_id <= 0) || !new_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_gc_meta_scn_ get invalid argument", KR(ret),
        K(tenant_id), K(round_id), K(new_scn));
  } else if (OB_ISNULL(scn_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown gc meta scn column", KR(ret), K(tenant_id), "column",
        static_cast<int64_t>(column));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, stmt_timeout))) {
    LOG_WARN("fail to get sql timeout", KR(ret), K(tenant_id));
  } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout))) {
    LOG_WARN("fail to set trx timeout", KR(ret), K(stmt_timeout));
  } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
    LOG_WARN("fail to set timeout", KR(ret), K(stmt_timeout));
  } else if (OB_FAIL(trans.start(&sql_client, real_tenant_id, false /*with_snapshot*/,
                                 OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id), K(real_tenant_id));
  } else {
    if (OB_FAIL(update_scn_in_trans_(trans, tenant_id, round_id, new_scn, column, is_updated))) {
      LOG_WARN("fail to update scn in trans", KR(ret), K(tenant_id), K(round_id),
          K(new_scn), K(scn_name));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCCESS == ret /*commit*/))) {
      LOG_WARN("fail to end trans", KR(tmp_ret), K(tenant_id), K(scn_name));
      ret = ((OB_SUCCESS == ret) ? tmp_ret : ret);
    }
  }
  return ret;
}

int ObSharedGCControlOperator::update_scn_in_trans_(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const int64_t round_id,
    const SCN &new_scn,
    const GCMetaScnColumn column,
    bool &is_updated)
{
  int ret = OB_SUCCESS;
  is_updated = false;
  const uint64_t cur_server_id = GCTX.get_server_id();
  const char *scn_name = gc_meta_scn_column_to_str(column);
  ObSharedGCControlInfo info;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (round_id <= 0) || !new_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update_scn_in_trans_ get invalid argument", KR(ret),
        K(tenant_id), K(round_id), K(new_scn));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(scn_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown gc meta scn column", KR(ret), K(tenant_id), "column",
        static_cast<int64_t>(column));
  } else {
    // 1. Lock the row and read current value, then compare to guarantee monotonicity.
    // 2. Lock the row to serialize with insert __all_shared_meta_reserved_scn
    if (OB_FAIL(select_tenant_id_for_update(trans, tenant_id, info))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // Row not inserted yet by tenant-create flow
        LOG_WARN("control info row not exist", KR(ret), K(tenant_id), K(new_scn), K(scn_name));
      } else {
        LOG_WARN("fail to select for update", KR(ret), K(tenant_id));
      }
    } else if ((info.executor_server_id_ != static_cast<int64_t>(cur_server_id))
               || (info.gc_round_id_ != round_id)) {
      // executor or round_id changed (e.g. switch-leader / new round); skip, no update.
      LOG_INFO("executor or round_id changed, skip update gc meta scn",
          K(tenant_id), K(cur_server_id), K(round_id), K(scn_name), K(info));
    } else {
      const SCN &cur_scn = ((GCMetaScnColumn::LAST_SUCC_GC_META_SCN == column)
                            ? info.last_succ_gc_meta_scn_
                            : info.gc_meta_scn_);
      if (new_scn <= cur_scn) {
        // Already at a not-smaller value; keep it (no rollback), no UPDATE.
        LOG_INFO("new scn is not larger than current, skip update",
            K(tenant_id), K(new_scn), K(cur_scn), K(scn_name), K(info));
      } else { // new_scn > cur_scn
        int64_t affected_rows = 0;
        if (GCMetaScnColumn::LAST_SUCC_GC_META_SCN == column) {
          ret = update_last_succ_gc_meta_scn_(trans, tenant_id, new_scn, affected_rows);
        } else {
          ret = update_gc_meta_scn_(trans, tenant_id, new_scn, affected_rows);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to update gc meta scn", KR(ret), K(tenant_id), K(new_scn),
              K(scn_name), K(info));
        } else if (1 != affected_rows) {
          // Local server is not the executor any more
          ret = OB_EAGAIN;
          LOG_WARN("local server is not executor", KR(ret), K(tenant_id), K(new_scn),
              K(scn_name), K(info));
        } else {
          is_updated = true;
          LOG_INFO("succ to update gc meta scn", K(tenant_id), K(new_scn), K(cur_scn),
              K(scn_name), K(affected_rows));
        }
      }
    }
  }
  return ret;
}

int ObSharedGCControlOperator::read_row(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObSharedGCControlInfo &info)
{
  int ret = OB_SUCCESS;
  info.reset();
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read_row get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, executor_server_id, "
          "status, gc_round_id, allow_gc_round_id, gc_meta_scn, last_succ_gc_meta_scn "
          "FROM %s WHERE tenant_id = %lu",
          timeout_us,
          OB_ALL_SHARED_GC_CONTROL_TNAME,
          tenant_id))) {
        LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id));
      } else if (OB_FAIL(do_sql_read_(sql_client, real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("fail to do sql read", KR(ret), K(sql));
      } else if (OB_FAIL(parse_result_row_(result, info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to parse result row", KR(ret), K(tenant_id), K(sql));
        }
      } else if (OB_SUCC(result.get_result()->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("more than one row matched, unexpected", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ITER_END != ret) {
        LOG_WARN("fail to fetch next result", KR(ret), K(sql));
      } else { // OB_ITER_END == ret
        ret = OB_SUCCESS; // ignore ret
      }
    }
  }
  return ret;
}

int ObSharedGCControlOperator::select_tenant_id_for_update(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    ObSharedGCControlInfo &info)
{
  int ret = OB_SUCCESS;
  info.reset();
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, result) {
      if (OB_FAIL(sql.assign_fmt(
          "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, executor_server_id, "
          "status, gc_round_id, allow_gc_round_id, gc_meta_scn, last_succ_gc_meta_scn "
          "FROM %s WHERE tenant_id = %lu FOR UPDATE",
          timeout_us,
          OB_ALL_SHARED_GC_CONTROL_TNAME,
          tenant_id))) {
        LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id));
      } else if (OB_FAIL(do_sql_read_(sql_client, real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("fail to do sql read", KR(ret), K(sql));
      } else if (OB_FAIL(parse_result_row_(result, info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to parse result row", KR(ret), K(tenant_id), K(sql));
        }
      } else if (OB_SUCC(result.get_result()->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("more than one row matched, unexpected", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ITER_END != ret) {
        LOG_WARN("fail to fetch next result", KR(ret), K(sql));
      } else { // OB_ITER_END == ret
        ret = OB_SUCCESS; // ignore ret
      }
    }
  }
  return ret;
}

int ObSharedGCControlOperator::do_update_set_clause_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObSqlString &set_clause,
    const bool with_server_condition,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  ObSqlString sql;
  int64_t timeout_us = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || set_clause.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do_update_set_clause_ get invalid argument", KR(ret),
        K(tenant_id), K(set_clause));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("fail to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_sql_timeout_us(tenant_id, timeout_us))) {
    LOG_WARN("fail to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "UPDATE /*+ QUERY_TIMEOUT(%ld) */ %s SET %.*s WHERE tenant_id = %lu",
      timeout_us,
      OB_ALL_SHARED_GC_CONTROL_TNAME,
      static_cast<int>(set_clause.length()),
      set_clause.ptr(),
      tenant_id))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(set_clause));
  } else if (with_server_condition) {
    const uint64_t cur_server_id = GCTX.get_server_id();
    if (OB_FAIL(sql.append_fmt(" AND executor_server_id = %ld", static_cast<int64_t>(cur_server_id)))) {
      LOG_WARN("fail to append server condition", KR(ret), K(cur_server_id));
    }
  }
  if (FAILEDx(do_sql_write_(sql_client, real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to do sql write", KR(ret), K(sql));
  }
  return ret;
}

int ObSharedGCControlOperator::parse_result_row_(
    ObMySQLProxy::MySQLResult &result,
    ObSharedGCControlInfo &info)
{
  int ret = OB_SUCCESS;
  info.reset();
  int64_t tenant_id = 0;
  int64_t executor_server_id = 0;
  int64_t gc_round_id = 0;
  int64_t allow_gc_round_id = 0;
  uint64_t gc_meta_scn_val = 0;
  uint64_t last_succ_gc_meta_scn_val = 0;
  ObString status_str;

  if (OB_ISNULL(result.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is nullptr", KR(ret));
  } else if (OB_FAIL(result.get_result()->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("fail to get next result", KR(ret));
    }
  } else if (OB_FAIL(result.get_result()->get_int("tenant_id", tenant_id))) {
    LOG_WARN("fail to get tenant_id", KR(ret));
  } else if (OB_FAIL(result.get_result()->get_int("executor_server_id", executor_server_id))) {
    LOG_WARN("fail to get executor_server_id", KR(ret));
  } else if (OB_FAIL(result.get_result()->get_varchar("status", status_str))) {
    LOG_WARN("fail to get status", KR(ret));
  } else if (OB_FAIL(shared_gc_control_status_from_str(status_str, info.status_))) {
    LOG_WARN("fail to parse status string", KR(ret), K(status_str));
  } else if (OB_FAIL(result.get_result()->get_int("gc_round_id", gc_round_id))) {
    LOG_WARN("fail to get gc_round_id", KR(ret));
  } else if (OB_FAIL(result.get_result()->get_int("allow_gc_round_id", allow_gc_round_id))) {
    LOG_WARN("fail to get allow_gc_round_id", KR(ret));
  } else if (OB_FAIL(result.get_result()->get_uint("gc_meta_scn", gc_meta_scn_val))) {
    LOG_WARN("fail to get gc_meta_scn", KR(ret));
  } else if (OB_FAIL(result.get_result()->get_uint("last_succ_gc_meta_scn", last_succ_gc_meta_scn_val))) {
    LOG_WARN("fail to get last_succ_gc_meta_scn", KR(ret));
  } else if (OB_FAIL(info.gc_meta_scn_.convert_for_inner_table_field(gc_meta_scn_val))) {
    LOG_WARN("fail to convert gc_meta_scn", KR(ret), K(gc_meta_scn_val));
  } else if (OB_FAIL(info.last_succ_gc_meta_scn_.convert_for_inner_table_field(last_succ_gc_meta_scn_val))) {
    LOG_WARN("fail to convert last_succ_gc_meta_scn", KR(ret), K(last_succ_gc_meta_scn_val));
  } else {
    info.tenant_id_ = static_cast<uint64_t>(tenant_id);
    info.executor_server_id_ = executor_server_id;
    info.gc_round_id_ = gc_round_id;
    info.allow_gc_round_id_ = allow_gc_round_id;
  }
  return ret;
}

int ObSharedGCControlOperator::get_real_tenant_id_(
    const uint64_t tenant_id,
    uint64_t &real_tenant_id)
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (is_user_tenant(tenant_id)) {
    real_tenant_id = gen_meta_tenant_id(tenant_id);
  } else if (is_meta_tenant(tenant_id)) {
    real_tenant_id = OB_SYS_TENANT_ID;
  } else if (is_sys_tenant(tenant_id)) {
    real_tenant_id = tenant_id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown tenant type", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSharedGCControlOperator::do_sql_write_(
    ObISQLClient &sql_client,
    const uint64_t real_tenant_id,
    const char *sql,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const int32_t group_id = OBCG_STORAGE;

  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do_sql_write_ get invalid argument", KR(ret), KP(sql));
  } else if (OB_FAIL(sql_client.write(real_tenant_id, sql, group_id, affected_rows))) {
    LOG_WARN("fail to execute write sql", KR(ret), KP(sql));
  }
  return ret;
}

int ObSharedGCControlOperator::do_sql_read_(
    ObISQLClient &sql_client,
    const uint64_t real_tenant_id,
    const char *sql,
    ObMySQLProxy::MySQLResult &result)
{
  int ret = OB_SUCCESS;
  const int32_t group_id = OBCG_STORAGE;

  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("do_sql_read_ get invalid argument", KR(ret), KP(sql));
  } else if (OB_FAIL(sql_client.read(result, real_tenant_id, sql, group_id))) {
    LOG_WARN("fail to execute read sql", KR(ret), KP(sql));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
