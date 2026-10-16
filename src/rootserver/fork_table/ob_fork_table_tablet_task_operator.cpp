/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX RS

#include "rootserver/fork_table/ob_fork_table_tablet_task_operator.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_tenant_info_proxy.h"
#include "share/schema/ob_schema_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase {
namespace rootserver {

using namespace common;
using namespace share;
using namespace share::schema;

void ObForkTableTabletTaskInfo::reset()
{
  task_id_ = OB_INVALID_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  src_tablet_id_.reset();
  dest_tablet_id_.reset();
  status_ = static_cast<int64_t>(ObForkTableTabletTaskStatus::INIT);
  ret_code_ = OB_SUCCESS;
}

int ObForkTableTabletTaskOperator::batch_insert(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<ObForkTableTabletTaskInfo> &infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), "count", infos.count());
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    const int64_t count = infos.count();
    for (int64_t start = 0; OB_SUCC(ret) && start < count; ) {
      const int64_t end = std::min(start + DEFAULT_BATCH_SIZE, count);
      dml_splicer.reset();
      sql.reset();
      for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
        const ObForkTableTabletTaskInfo &info = infos.at(i);
        if (OB_UNLIKELY(!info.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid info", KR(ret), K(info));
        } else if (OB_FAIL(dml_splicer.add_pk_column("task_id", info.task_id_))
            || OB_FAIL(dml_splicer.add_pk_column("dest_tablet_id", info.dest_tablet_id_.id()))
            || OB_FAIL(dml_splicer.add_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, info.tenant_id_)))
            || OB_FAIL(dml_splicer.add_column("src_tablet_id", info.src_tablet_id_.id()))
            || OB_FAIL(dml_splicer.add_column("status", info.status_))
            || OB_FAIL(dml_splicer.add_column("ret_code", info.ret_code_))) {
          LOG_WARN("fail to add column", KR(ret), K(info));
        } else if (OB_FAIL(dml_splicer.finish_row())) {
          LOG_WARN("fail to finish row", KR(ret));
        }
      }
      if (FAILEDx(dml_splicer.splice_batch_insert_sql(OB_ALL_FORK_TABLE_TABLET_TASK_TNAME, sql))) {
        LOG_WARN("fail to splice batch insert sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K(affected_rows));
      } else if (OB_UNLIKELY(affected_rows != end - start)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected rows on batch insert", KR(ret), K(affected_rows),
                 "expected", end - start, K(tenant_id), K(start), K(end));
      }
      start = end;
    }
  }
  return ret;
}

int ObForkTableTabletTaskOperator::get_all_by_task_id(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t task_id,
    ObIArray<ObForkTableTabletTaskInfo> &infos)
{
  int ret = OB_SUCCESS;
  infos.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT task_id, tenant_id, src_tablet_id, dest_tablet_id, status, ret_code "
              "FROM %s WHERE task_id = %ld", OB_ALL_FORK_TABLE_TABLET_TASK_TNAME, task_id))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else {
        ObForkTableTabletTaskInfo info;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          info.reset();
          if (OB_FAIL(fill_info_from_result_(*result, tenant_id, info))) {
            LOG_WARN("fail to fill info", KR(ret));
          } else if (OB_FAIL(infos.push_back(info))) {
            LOG_WARN("fail to push back", KR(ret), K(info));
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

int ObForkTableTabletTaskOperator::get_by_dest_tablet_id(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTabletID &dest_tablet_id,
    ObForkTableTabletTaskInfo &info)
{
  int ret = OB_SUCCESS;
  info.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !dest_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(dest_tablet_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT task_id, tenant_id, src_tablet_id, dest_tablet_id, status, ret_code "
              "FROM %s WHERE dest_tablet_id = %lu",
              OB_ALL_FORK_TABLE_TABLET_TASK_TNAME,
              dest_tablet_id.id()))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no row found", KR(ret), K(dest_tablet_id));
        } else {
          LOG_WARN("fail to get next", KR(ret));
        }
      } else if (OB_FAIL(fill_info_from_result_(*result, tenant_id, info))) {
        LOG_WARN("fail to fill info", KR(ret));
      }
    }
  }
  return ret;
}

int ObForkTableTabletTaskOperator::update_status(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t task_id,
    const ObTabletID &dest_tablet_id,
    const ObForkTableTabletTaskStatus expected_old_status,
    const ObForkTableTabletTaskStatus new_status,
    const int new_ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || task_id <= 0
      || !dest_tablet_id.is_valid()
      || expected_old_status < ObForkTableTabletTaskStatus::INIT
      || expected_old_status >= ObForkTableTabletTaskStatus::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id), K(dest_tablet_id),
             K(expected_old_status), K(new_status));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    if (OB_FAIL(dml_splicer.add_pk_column("task_id", task_id))
        || OB_FAIL(dml_splicer.add_pk_column("dest_tablet_id", dest_tablet_id.id()))
        || OB_FAIL(dml_splicer.add_column("status", static_cast<int64_t>(new_status)))
        || OB_FAIL(dml_splicer.add_column("ret_code", new_ret_code))) {
      LOG_WARN("fail to add column", KR(ret), K(task_id), K(dest_tablet_id));
    } else if (OB_FAIL(dml_splicer.splice_update_sql(OB_ALL_FORK_TABLE_TABLET_TASK_TNAME, sql))) {
      LOG_WARN("fail to splice update sql", KR(ret));
    } else if (OB_FAIL(sql.append_fmt(" AND status = %ld", static_cast<int64_t>(expected_old_status)))) {
      LOG_WARN("fail to append cas guard", KR(ret), K(expected_old_status));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql));
    } else if (OB_UNLIKELY(affected_rows > 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(task_id), K(dest_tablet_id));
    } else if (0 == affected_rows) {
      ret = OB_STATE_NOT_MATCH;
    }
  }
  return ret;
}

int ObForkTableTabletTaskOperator::get_status_summary(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t task_id,
    int64_t &total_cnt,
    int64_t &success_cnt,
    int64_t &fail_cnt)
{
  int ret = OB_SUCCESS;
  total_cnt = 0;
  success_cnt = 0;
  fail_cnt = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      // 空表时 SUM(...) 返回 NULL,EXTRACT_INT_FIELD_MYSQL 读 NULL 会报 OB_OBJ_TYPE_ERROR,
      // 故用 COALESCE(SUM(...), 0) 兜底。另外 SUM(...) 返回 NUMBER/decimal 类型,
      // EXTRACT_INT_FIELD_MYSQL 读 decimal 列同样报 OB_OBJ_TYPE_ERROR,故再 CAST AS SIGNED
      // 保证 success_cnt/fail_cnt 列是整型。
      if (OB_FAIL(sql.assign_fmt(
              "SELECT COUNT(*) AS total_cnt, "
              "CAST(COALESCE(SUM(CASE WHEN status = %ld THEN 1 ELSE 0 END), 0) AS SIGNED) AS success_cnt, "
              "CAST(COALESCE(SUM(CASE WHEN status = %ld THEN 1 ELSE 0 END), 0) AS SIGNED) AS fail_cnt "
              "FROM %s WHERE task_id = %ld",
              static_cast<int64_t>(ObForkTableTabletTaskStatus::SUCCESS),
              static_cast<int64_t>(ObForkTableTabletTaskStatus::FAILED),
              OB_ALL_FORK_TABLE_TABLET_TASK_TNAME,
              task_id))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "total_cnt", total_cnt, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "success_cnt", success_cnt, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "fail_cnt", fail_cnt, int64_t);
      }
    }
  }
  return ret;
}

int ObForkTableTabletTaskOperator::archive_and_remove_by_task_id(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(task_id));
  } else {
    ObSqlString archive_sql;
    ObSqlString delete_sql;
    int64_t archive_rows = 0;
    int64_t delete_rows = 0;
    if (OB_FAIL(archive_sql.assign_fmt(
            "INSERT INTO %s "
            "(task_id, dest_tablet_id, tenant_id, src_tablet_id, status, ret_code) "
            "SELECT task_id, dest_tablet_id, tenant_id, src_tablet_id, status, ret_code "
            "FROM %s WHERE task_id = %ld",
            OB_ALL_FORK_TABLE_TABLET_TASK_HISTORY_TNAME,
            OB_ALL_FORK_TABLE_TABLET_TASK_TNAME,
            task_id))) {
      LOG_WARN("fail to assign archive sql", KR(ret), K(task_id));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, archive_sql.ptr(), archive_rows))) {
      LOG_WARN("fail to archive fork tablet tasks", KR(ret), K(archive_sql));
    } else if (OB_FAIL(delete_sql.assign_fmt(
            "DELETE FROM %s WHERE task_id = %ld",
            OB_ALL_FORK_TABLE_TABLET_TASK_TNAME,
            task_id))) {
      LOG_WARN("fail to assign delete sql", KR(ret), K(task_id));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, delete_sql.ptr(), delete_rows))) {
      LOG_WARN("fail to remove fork tablet tasks", KR(ret), K(delete_sql));
    } else if (OB_UNLIKELY(archive_rows != delete_rows || archive_rows <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected archive/delete row count", KR(ret), K(tenant_id), K(task_id),
               K(archive_rows), K(delete_rows));
    } else {
      LOG_INFO("archive and remove fork tablet tasks", K(tenant_id), K(task_id),
               K(archive_rows), K(delete_rows));
    }
  }
  return ret;
}

int ObForkTableTabletTaskOperator::batch_delete_expired_history(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const int64_t expire_ts_us,
    const int64_t limit,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || expire_ts_us <= 0 || limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(expire_ts_us), K(limit));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "DELETE FROM %s WHERE gmt_create < usec_to_time(%ld) LIMIT %ld",
            OB_ALL_FORK_TABLE_TABLET_TASK_HISTORY_TNAME, expire_ts_us, limit))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to delete expired fork tablet task history", KR(ret), K(sql));
    } else if (OB_UNLIKELY(affected_rows > limit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows on batch delete expired history", KR(ret),
               K(affected_rows), K(limit), K(tenant_id), K(expire_ts_us));
    }
  }
  return ret;
}

int ObForkTableTabletTaskOperator::clean_expired_history(ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> primary_tenant_ids;
  if (OB_FAIL(ObAllTenantInfoProxy::get_primary_tenant_ids(&sql_proxy, primary_tenant_ids))) {
    LOG_WARN("get primary tenant ids failed", KR(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < primary_tenant_ids.count(); ++i) {
      int tmp_ret = OB_SUCCESS;
      const uint64_t tenant_id = primary_tenant_ids.at(i);
      int64_t retention_us = DEFAULT_HISTORY_EXPIRATION_US;
      {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
        if (tenant_config.is_valid()) {
          retention_us = tenant_config->_fork_history_expiration_time;
        }
      }
      const int64_t expire_ts_us = now - retention_us;
      int64_t total_deleted = 0;
      int64_t affected_rows = 0;
      do {
        affected_rows = 0;
        if (OB_TMP_FAIL(batch_delete_expired_history(
                sql_proxy, tenant_id, expire_ts_us,
                DEFAULT_DELETE_EXPIRED_HISTORY_LIMIT, affected_rows))) {
          LOG_WARN("batch delete expired fork table tablet task history failed",
                   KR(tmp_ret), K(tenant_id), K(expire_ts_us));
        } else {
          total_deleted += affected_rows;
        }
      } while (OB_SUCCESS == tmp_ret
               && affected_rows >= DEFAULT_DELETE_EXPIRED_HISTORY_LIMIT);
      if (total_deleted > 0) {
        LOG_INFO("clean fork table tablet task history", K(tenant_id), K(expire_ts_us), K(total_deleted));
      }
    }
  }
  return ret;
}

int ObForkTableTabletTaskOperator::fill_info_from_result_(
    sqlclient::ObMySQLResult &result,
    const uint64_t exec_tenant_id,
    ObForkTableTabletTaskInfo &info)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  int64_t src_tablet_id = 0;
  int64_t dest_tablet_id = 0;
  int64_t status = 0;
  int64_t ret_code = 0;

  EXTRACT_INT_FIELD_MYSQL(result, "task_id", task_id, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", tenant_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "src_tablet_id", src_tablet_id, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "dest_tablet_id", dest_tablet_id, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "status", status, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "ret_code", ret_code, int64_t);

  if (OB_SUCC(ret)) {
    info.task_id_ = task_id;
    info.tenant_id_ = tenant_id;
    if (OB_INVALID_TENANT_ID != exec_tenant_id && (OB_INVALID_TENANT_ID == info.tenant_id_ || 0 == info.tenant_id_)) {
      info.tenant_id_ = exec_tenant_id;
    }
    info.src_tablet_id_ = ObTabletID(src_tablet_id);
    info.dest_tablet_id_ = ObTabletID(dest_tablet_id);
    info.status_ = status;
    info.ret_code_ = static_cast<int>(ret_code);
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
