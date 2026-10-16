/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_fork_table_reserved_scn_operator.h"

#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_define.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_define.h"

using namespace oceanbase;
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace storage;

ObForkTableReservedScnRow::ObForkTableReservedScnRow()
  : tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    ddl_task_id_(0),
    tablet_id_(),
    meta_scn_(),
    status_(ObForkTableReservedScnStatus::DOING)
{
}

void ObForkTableReservedScnRow::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  ddl_task_id_ = 0;
  tablet_id_.reset();
  meta_scn_.reset();
  status_ = ObForkTableReservedScnStatus::DOING;
}

bool ObForkTableReservedScnRow::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
      && ls_id_.is_valid()
      && ddl_task_id_ > 0
      && tablet_id_.is_valid()
      && meta_scn_.is_valid()
      && status_ >= ObForkTableReservedScnStatus::DOING
      && status_ <= ObForkTableReservedScnStatus::DONE;
}

ObForkTableReservedScnOperator::ObForkTableReservedScnOperator()
  : is_inited_(false),
    sql_client_(nullptr),
    sql_timeout_us_(0)
{
}

int ObForkTableReservedScnOperator::init(
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator init twice", KR(ret));
  } else if (OB_UNLIKELY(sql_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid sql timeout", KR(ret), K(sql_timeout_us));
  } else {
    sql_client_ = &sql_client;
    sql_timeout_us_ = sql_timeout_us;
    is_inited_ = true;
  }
  return ret;
}

void ObForkTableReservedScnOperator::reset()
{
  sql_client_ = nullptr;
  sql_timeout_us_ = 0;
  is_inited_ = false;
}

int ObForkTableReservedScnOperator::batch_insert_rows(
    const common::ObIArray<ObForkTableReservedScnRow> &rows)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  int64_t timeout_us = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_UNLIKELY(rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] batch insert get empty rows", KR(ret));
  } else if (OB_FAIL(get_real_tenant_id_(rows.at(0).tenant_id_, real_tenant_id))) {
    LOG_WARN("[FORKTABLE] failed to get real tenant id",
        KR(ret), "tenant_id", rows.at(0).tenant_id_);
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to get sql timeout", KR(ret));
  } else {
    for (int64_t start = 0; OB_SUCC(ret) && start < rows.count(); start += BATCH_INSERT_ROW_LIMIT) {
      const int64_t end = MIN(start + BATCH_INSERT_ROW_LIMIT, rows.count());
      ObDMLSqlSplicer dml_splicer;
      ObSqlString insert_header;
      ObArray<ObSqlString> sqls;
      int64_t batch_affected_rows = 0;
      for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
        const ObForkTableReservedScnRow &row = rows.at(i);
        if (OB_UNLIKELY(!row.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("[FORKTABLE] invalid fork table reserved scn row", KR(ret), K(row));
        } else if (OB_FAIL(dml_splicer.add_uint64_pk_column("tenant_id", row.tenant_id_))
            || OB_FAIL(dml_splicer.add_pk_column("ls_id", row.ls_id_.id()))
            || OB_FAIL(dml_splicer.add_pk_column("ddl_task_id", row.ddl_task_id_))
            || OB_FAIL(dml_splicer.add_uint64_pk_column("tablet_id", row.tablet_id_.id()))
            || OB_FAIL(dml_splicer.add_column("meta_scn", row.meta_scn_.get_val_for_tx()))
            || OB_FAIL(dml_splicer.add_column("status", static_cast<int64_t>(row.status_)))) {
          LOG_WARN("[FORKTABLE] failed to add batch insert columns", KR(ret), K(row));
        } else if (OB_FAIL(dml_splicer.finish_row())) {
          LOG_WARN("[FORKTABLE] failed to finish batch insert row", KR(ret), K(row));
        }
      }
      if (OB_SUCC(ret)
          && OB_FAIL(insert_header.assign_fmt("INSERT /*+ QUERY_TIMEOUT(%ld) */", timeout_us))) {
        LOG_WARN("[FORKTABLE] failed to assign batch insert header", KR(ret));
      } else if (OB_SUCC(ret)
          && OB_FAIL(dml_splicer.splice_batch_insert_in_array(
              share::OB_ALL_FORK_TABLE_RESERVED_SCN_TNAME, sqls, insert_header.ptr()))) {
        LOG_WARN("[FORKTABLE] failed to splice batch insert sql", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < sqls.count(); ++i) {
        int64_t affected_rows = 0;
        if (OB_FAIL(do_sql_write_(real_tenant_id, sqls.at(i).ptr(), affected_rows))) {
          LOG_WARN("[FORKTABLE] failed to execute batch insert",
              KR(ret), K(real_tenant_id), K(sqls.at(i)));
        } else {
          batch_affected_rows += affected_rows;
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(batch_affected_rows != end - start)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] unexpected affected rows on batch insert",
            KR(ret), K(batch_affected_rows), "expected", end - start);
      }
    }
    LOG_INFO("[FORKTABLE] batch insert fork table reserved scn rows",
        KR(ret), "row_cnt", rows.count(), "first_row", rows.at(0));
  }
  return ret;
}

int ObForkTableReservedScnOperator::update_tablet_done(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t ddl_task_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  int64_t timeout_us = 0;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid()
                         || ddl_task_id <= 0 || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid update done argument",
        KR(ret), K(tenant_id), K(ls_id), K(ddl_task_id), K(tablet_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("[FORKTABLE] failed to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "UPDATE /*+ QUERY_TIMEOUT(%ld) */ %s SET status = %ld "
      "WHERE tenant_id = %lu AND ls_id = %ld AND ddl_task_id = %ld "
      "AND tablet_id = %lu AND status = %ld",
      timeout_us,
      share::OB_ALL_FORK_TABLE_RESERVED_SCN_TNAME,
      static_cast<int64_t>(ObForkTableReservedScnStatus::DONE),
      tenant_id,
      ls_id.id(),
      ddl_task_id,
      tablet_id.id(),
      static_cast<int64_t>(ObForkTableReservedScnStatus::DOING)))) {
    LOG_WARN("[FORKTABLE] failed to assign update done sql", KR(ret));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("[FORKTABLE] failed to execute update done", KR(ret), K(real_tenant_id), K(sql));
  } else if (OB_UNLIKELY(0 != affected_rows && 1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] update done affected unexpected rows", KR(ret), K(affected_rows), K(sql));
  } else if (0 == affected_rows) {
    ObForkTableReservedScnRow row;
    bool is_exist = false;
    if (OB_FAIL(get_tablet_row_(tenant_id, ls_id, ddl_task_id, tablet_id, row, is_exist))) {
      LOG_WARN("[FORKTABLE] failed to check fork table reserved scn row after update",
          KR(ret), K(tenant_id), K(ls_id), K(ddl_task_id), K(tablet_id));
    } else if (!is_exist) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("[FORKTABLE] fork table reserved scn row does not exist",
          KR(ret), K(tenant_id), K(ls_id), K(ddl_task_id), K(tablet_id));
    } else if (ObForkTableReservedScnStatus::DONE != row.status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("[FORKTABLE] fork table reserved scn status does not match",
          KR(ret), K(row), "expected_status",
          static_cast<int64_t>(ObForkTableReservedScnStatus::DONE));
    } else {
      LOG_INFO("[FORKTABLE] fork table reserved scn row is already done", K(row));
    }
  }
  return ret;
}

int ObForkTableReservedScnOperator::delete_group(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t ddl_task_id)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  int64_t timeout_us = 0;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid() || ddl_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid delete group argument",
        KR(ret), K(tenant_id), K(ls_id), K(ddl_task_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("[FORKTABLE] failed to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE /*+ QUERY_TIMEOUT(%ld) */ FROM %s "
      "WHERE tenant_id = %lu AND ls_id = %ld AND ddl_task_id = %ld",
      timeout_us,
      share::OB_ALL_FORK_TABLE_RESERVED_SCN_TNAME,
      tenant_id,
      ls_id.id(),
      ddl_task_id))) {
    LOG_WARN("[FORKTABLE] failed to assign delete group sql", KR(ret));
  } else if (OB_FAIL(do_sql_write_(real_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("[FORKTABLE] failed to execute delete group", KR(ret), K(real_tenant_id), K(sql));
  } else {
    LOG_INFO("[FORKTABLE] delete fork table reserved scn group",
        K(tenant_id), K(ls_id), K(ddl_task_id), K(affected_rows));
  }
  return ret;
}

int ObForkTableReservedScnOperator::scan_ls_rows(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    common::ObIArray<ObForkTableReservedScnRow> &rows)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  int64_t timeout_us = 0;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid scan ls rows argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("[FORKTABLE] failed to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, ls_id, ddl_task_id, tablet_id, meta_scn, status "
      "FROM %s WHERE tenant_id = %lu AND ls_id = %ld",
      timeout_us,
      share::OB_ALL_FORK_TABLE_RESERVED_SCN_TNAME,
      tenant_id,
      ls_id.id()))) {
    LOG_WARN("[FORKTABLE] failed to assign scan ls sql", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, result) {
      if (OB_FAIL(do_sql_read_(real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("[FORKTABLE] failed to execute scan ls rows", KR(ret), K(real_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] scan result is null", KR(ret), K(sql));
      } else {
        ObForkTableReservedScnRow row;
        while (OB_SUCC(ret)) {
          row.reset();
          if (OB_FAIL(result.get_result()->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("[FORKTABLE] failed to iterate scan result", KR(ret), K(sql));
            }
          } else if (OB_FAIL(parse_row_(*result.get_result(), row))) {
            LOG_WARN("[FORKTABLE] failed to parse fork table reserved scn row", KR(ret));
          } else if (OB_FAIL(rows.push_back(row))) {
            LOG_WARN("[FORKTABLE] failed to push back fork table reserved scn row",
                KR(ret), K(row));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObForkTableReservedScnOperator::get_group_any_row(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t ddl_task_id,
    ObForkTableReservedScnRow &row,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  int64_t timeout_us = 0;
  ObSqlString sql;
  is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid() || ddl_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid get group any row argument",
        KR(ret), K(tenant_id), K(ls_id), K(ddl_task_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("[FORKTABLE] failed to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, ls_id, ddl_task_id, tablet_id, meta_scn, status "
      "FROM %s WHERE tenant_id = %lu AND ls_id = %ld AND ddl_task_id = %ld LIMIT 1",
      timeout_us,
      share::OB_ALL_FORK_TABLE_RESERVED_SCN_TNAME,
      tenant_id,
      ls_id.id(),
      ddl_task_id))) {
    LOG_WARN("[FORKTABLE] failed to assign get group any row sql", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, result) {
      if (OB_FAIL(do_sql_read_(real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("[FORKTABLE] failed to execute get group any row",
            KR(ret), K(real_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] get group any row result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[FORKTABLE] failed to iterate get group any row result", KR(ret), K(sql));
        }
      } else if (OB_FAIL(parse_row_(*result.get_result(), row))) {
        LOG_WARN("[FORKTABLE] failed to parse fork table reserved scn row", KR(ret));
      } else {
        is_exist = true;
      }
    }
  }
  return ret;
}

int ObForkTableReservedScnOperator::get_tablet_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t ddl_task_id,
    const common::ObTabletID &tablet_id,
    ObForkTableReservedScnRow &row,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  int64_t timeout_us = 0;
  ObSqlString sql;
  row.reset();
  is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid()
                         || ddl_task_id <= 0 || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid get tablet row argument",
        KR(ret), K(tenant_id), K(ls_id), K(ddl_task_id), K(tablet_id));
  } else if (OB_FAIL(get_real_tenant_id_(tenant_id, real_tenant_id))) {
    LOG_WARN("[FORKTABLE] failed to get real tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("[FORKTABLE] failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, ls_id, ddl_task_id, tablet_id, meta_scn, status "
      "FROM %s WHERE tenant_id = %lu AND ls_id = %ld AND ddl_task_id = %ld AND tablet_id = %lu",
      timeout_us,
      share::OB_ALL_FORK_TABLE_RESERVED_SCN_TNAME,
      tenant_id,
      ls_id.id(),
      ddl_task_id,
      tablet_id.id()))) {
    LOG_WARN("[FORKTABLE] failed to assign get tablet row sql", KR(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, result) {
      if (OB_FAIL(do_sql_read_(real_tenant_id, sql.ptr(), result))) {
        LOG_WARN("[FORKTABLE] failed to execute get tablet row",
            KR(ret), K(real_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] get tablet row result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result.get_result()->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[FORKTABLE] failed to iterate get tablet row result", KR(ret), K(sql));
        }
      } else if (OB_FAIL(parse_row_(*result.get_result(), row))) {
        LOG_WARN("[FORKTABLE] failed to parse fork table reserved scn row", KR(ret));
      } else {
        is_exist = true;
      }
    }
  }
  return ret;
}

int ObForkTableReservedScnOperator::parse_row_(
    common::sqlclient::ObMySQLResult &result,
    ObForkTableReservedScnRow &row)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id_int = 0;
  int64_t ls_id_int = 0;
  int64_t tablet_id_int = 0;
  int64_t meta_scn_val = 0;
  int64_t status_int = 0;
  row.reset();
  if (OB_FAIL(result.get_int("tenant_id", tenant_id_int))) {
    LOG_WARN("[FORKTABLE] failed to get tenant id", KR(ret));
  } else if (FALSE_IT(row.tenant_id_ = static_cast<uint64_t>(tenant_id_int))) {
  } else if (OB_FAIL(result.get_int("ls_id", ls_id_int))) {
    LOG_WARN("[FORKTABLE] failed to get ls id", KR(ret));
  } else if (FALSE_IT(row.ls_id_ = share::ObLSID(ls_id_int))) {
  } else if (OB_FAIL(result.get_int("ddl_task_id", row.ddl_task_id_))) {
    LOG_WARN("[FORKTABLE] failed to get ddl task id", KR(ret));
  } else if (OB_FAIL(result.get_int("tablet_id", tablet_id_int))) {
    LOG_WARN("[FORKTABLE] failed to get tablet id", KR(ret));
  } else if (FALSE_IT(row.tablet_id_ = common::ObTabletID(static_cast<uint64_t>(tablet_id_int)))) {
  } else if (OB_FAIL(result.get_int("meta_scn", meta_scn_val))) {
    LOG_WARN("[FORKTABLE] failed to get meta scn", KR(ret));
  } else if (OB_FAIL(row.meta_scn_.convert_for_tx(meta_scn_val))) {
    LOG_WARN("[FORKTABLE] failed to convert meta scn", KR(ret), K(meta_scn_val));
  } else if (OB_FAIL(result.get_int("status", status_int))) {
    LOG_WARN("[FORKTABLE] failed to get status", KR(ret));
  } else if (OB_UNLIKELY(
      status_int < static_cast<int64_t>(ObForkTableReservedScnStatus::DOING)
      || status_int > static_cast<int64_t>(ObForkTableReservedScnStatus::DONE))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] unexpected fork table reserved scn status", KR(ret), K(status_int));
  } else {
    row.status_ = static_cast<ObForkTableReservedScnStatus>(status_int);
  }
  return ret;
}

int ObForkTableReservedScnOperator::get_real_tenant_id_(
    const uint64_t tenant_id,
    uint64_t &real_tenant_id)
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_FAIL(ObSharedMetaReservedUtil::get_real_tenant_id(tenant_id, real_tenant_id))) {
    // 与 shared meta reserved 表使用同一租户路由，保证事务原子性。
    LOG_WARN("[FORKTABLE] failed to get real tenant id", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObForkTableReservedScnOperator::get_sql_timeout_us_(int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  timeout_us = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (THIS_WORKER.is_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("[FORKTABLE] already timeout", KR(ret));
  } else if (THIS_WORKER.is_timeout_ts_valid()) {
    const int64_t remain = THIS_WORKER.get_timeout_remain();
    if (remain > 0) {
      timeout_us = MIN(remain, sql_timeout_us_);
    } else {
      ret = OB_TIMEOUT;
      LOG_WARN("[FORKTABLE] timeout remain is not enough", KR(ret), K(remain));
    }
  } else {
    timeout_us = sql_timeout_us_;
  }
  return ret;
}

int ObForkTableReservedScnOperator::do_sql_write_(
    const uint64_t tenant_id,
    const char *sql,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  const int32_t group_id = share::OBCG_STORAGE;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(sql))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] do sql write get invalid argument", KR(ret), K(tenant_id), KP(sql));
  } else if (OB_FAIL(sql_client_->write(tenant_id, sql, group_id, affected_rows))) {
    LOG_WARN("[FORKTABLE] failed to write sql", KR(ret), K(tenant_id), KP(sql));
  }
  return ret;
}

int ObForkTableReservedScnOperator::do_sql_read_(
    const uint64_t tenant_id,
    const char *sql,
    common::ObMySQLProxy::MySQLResult &result)
{
  int ret = OB_SUCCESS;
  const int32_t group_id = share::OBCG_STORAGE;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table reserved scn operator not init", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(sql))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] do sql read get invalid argument", KR(ret), K(tenant_id), KP(sql));
  } else if (OB_FAIL(sql_client_->read(result, tenant_id, sql, group_id))) {
    LOG_WARN("[FORKTABLE] failed to read sql", KR(ret), K(tenant_id), KP(sql));
  }
  return ret;
}
