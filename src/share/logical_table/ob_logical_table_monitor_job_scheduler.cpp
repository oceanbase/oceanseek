/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/ob_logical_table_monitor_job_scheduler.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace share
{

using namespace oceanbase::common;
using namespace oceanbase::dbms_scheduler;

bool ObLogicalTableMonitorJobScheduler::is_namespace_stats_job(const ObString &job_name)
{
  return 0 == job_name.case_compare(LOGICAL_TABLE_NAMESPACE_STATS_JOB_NAME);
}

int ObLogicalTableMonitorJobScheduler::set_attribute(
    const sql::ObSQLSessionInfo *session,
    const ObString &job_name,
    const ObString &attr_name,
    const ObString &attr_val_str,
    const ObDBMSSchedJobInfo &job_info,
    bool &is_namespace_stats_attr,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  is_namespace_stats_attr = false;
  if (OB_ISNULL(session) || job_name.empty() || attr_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session), K(job_name), K(attr_name), K(attr_val_str));
  } else if (!is_namespace_stats_job(job_name)
             && ObDBMSSchedFuncType::LOGICAL_TABLE_NAMESPACE_STATS_JOB != job_info.get_func_type()) {
    // not our job
  } else if (!is_user_tenant(session->get_effective_tenant_id())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("not user tenant, can't set attribute for namespace stats job",
             KR(ret), "tenant_id", session->get_effective_tenant_id());
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set attribute for logic_table_namespace_stats_job non-user tenant");
  } else if (attr_val_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty attr val", KR(ret), K(job_name), K(attr_name));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "attribute value can not be empty");
  } else if (0 == attr_name.case_compare("repeat_interval")) {
    int64_t next_date = 0;
    ObDBMSSchedJobInfo calc_job_info;
    calc_job_info.start_date_ = job_info.start_date_;
    calc_job_info.repeat_interval_ = attr_val_str;
    calc_job_info.interval_ts_ = 0;
    if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_repeat_interval(
            session->get_effective_tenant_id(), attr_val_str))) {
      LOG_WARN("invalid repeat_interval for namespace stats job", KR(ret), K(attr_val_str));
    } else if (OB_FAIL(ObDBMSSchedJobUtils::calc_dbms_sched_repeat_expr(calc_job_info, next_date))) {
      LOG_WARN("failed to calc next_date for namespace stats job", KR(ret), K(attr_val_str));
    } else if (OB_FAIL(dml.add_column("repeat_interval", attr_val_str))) {
      LOG_WARN("failed to add repeat_interval column", KR(ret), K(attr_val_str));
    } else if (OB_FAIL(dml.add_column("`interval#`", "null"))) {
      LOG_WARN("failed to add interval# column", KR(ret));
    } else if (OB_FAIL(dml.add_raw_time_column("next_date", next_date))) {
      LOG_WARN("failed to add next_date column", KR(ret), K(next_date));
    } else if (OB_FAIL(dml.add_column("interval_ts", 0))) {
      LOG_WARN("failed to add interval_ts column", KR(ret));
    } else {
      is_namespace_stats_attr = true;
      LOG_INFO("set repeat_interval for namespace stats job", K(attr_val_str), K(next_date));
    }
  } else if (0 == attr_name.case_compare("start_date") || 0 == attr_name.case_compare("next_date")) {
    int64_t next_date_ts = OB_INVALID_TIMESTAMP;
    int32_t offset_sec = 0;
    if (OB_FAIL(ObDbmsStatsMaintenanceWindow::parse_next_date(
            session, attr_val_str, offset_sec, next_date_ts))) {
      LOG_WARN("parse start_date failed for namespace stats job", KR(ret), K(attr_val_str));
    } else if (ObTimeUtility::current_time() > next_date_ts) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid start_date for namespace stats job", KR(ret), K(attr_val_str), K(next_date_ts));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "set start_date, it is previous date");
    } else if (OB_FAIL(dml.add_time_column("next_date", next_date_ts))) {
      LOG_WARN("failed to add next_date column", KR(ret));
    } else if (OB_FAIL(dml.add_time_column("start_date", next_date_ts))) {
      LOG_WARN("failed to add start_date column", KR(ret));
    } else {
      is_namespace_stats_attr = true;
      LOG_INFO("set start_date for namespace stats job", K(attr_val_str), K(next_date_ts));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid attribute for logic_table_namespace_stats_job", KR(ret), K(attr_name));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "attribute for logic_table_namespace_stats_job");
  }
  return ret;
}

int ObLogicalTableMonitorJobScheduler::check_job_exists_in_trans_(
  ObMySQLTransaction &trans,
  const uint64_t tenant_id,
  const char *job_name,
  bool &job_exists)
{
  int ret = OB_SUCCESS;
  job_exists = false;
  int64_t row_count = 0;
  ObSqlString select_sql;
  if (OB_ISNULL(job_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job_name is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(select_sql.append_fmt(
      "SELECT count(*) FROM %s WHERE tenant_id = %ld AND job_name = '%s'",
      share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
      share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
      job_name))) {
    LOG_WARN("failed to build check job sql", KR(ret), K(tenant_id), K(job_name));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      if (OB_FAIL(trans.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to check scheduler job", KR(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get check job result", KR(ret), K(select_sql));
      } else if (OB_FAIL(client_result->next())) {
        LOG_WARN("failed to read check job row", KR(ret), K(select_sql));
      } else {
        ObObj obj;
        const int64_t col_idx = 0;
        if (OB_FAIL(client_result->get_obj(col_idx, obj))) {
          LOG_WARN("failed to get count obj", KR(ret));
        } else if (OB_FAIL(obj.get_int(row_count))) {
          LOG_WARN("failed to get count", KR(ret), K(obj));
        } else {
          job_exists = row_count > 0;
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", KR(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObLogicalTableMonitorJobScheduler::create_namespace_stats_job(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool job_exists = false;
  if (OB_FAIL(check_job_exists_in_trans_(
          trans, tenant_id, LOGICAL_TABLE_NAMESPACE_STATS_JOB_NAME, job_exists))) {
    LOG_WARN("fail to check namespace stats job exists", KR(ret), K(tenant_id));
  } else if (job_exists) {
    LOG_INFO("logic table namespace stats job already exists", K(tenant_id));
  } else if (OB_FAIL(create_job_(sys_variable, tenant_id, trans))) {
    LOG_WARN("fail to create logic table namespace stats job", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObLogicalTableMonitorJobScheduler::create_namespace_stats_job_for_upgrade(
  common::ObMySQLProxy *sql_proxy,
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool job_exists = false;
  UNUSED(sql_proxy);
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_job_exists_in_trans_(
                 trans, tenant_id, LOGICAL_TABLE_NAMESPACE_STATS_JOB_NAME, job_exists))) {
    LOG_WARN("fail to check namespace stats job exists", KR(ret), K(tenant_id));
  } else if (job_exists) {
    LOG_INFO("logic table namespace stats job already exists", K(tenant_id));
  } else if (OB_FAIL(create_job_(sys_variable, tenant_id, trans))) {
    LOG_WARN("fail to create logic table namespace stats job", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObLogicalTableMonitorJobScheduler::create_job_(
  const schema::ObSysVariableSchema &sys_variable,
  const uint64_t tenant_id,
  ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
  int64_t pos = 0;
  int64_t job_id = OB_INVALID_ID;
  const int64_t start_usec = ObTimeUtility::current_time();
  const int64_t end_date = 64060560000000000; // 4000-01-01
  const int64_t max_run_duration_sec = 3600;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", KR(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("generate_job_id failed", KR(ret), K(tenant_id));
  } else {
    ObString exec_env(pos, buf);
    ObString job_name(LOGICAL_TABLE_NAMESPACE_STATS_JOB_NAME);
    ObString repeat_interval("FREQ=SECONDLY; INTERVAL=60");
    ObString job_action("DBMS_LOGIC_TABLE.GATHER_NAMESPACE_STATS_INNER()");

    HEAP_VAR(ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString("oceanbase");
      job_info.job_style_ = ObString("REGULAR");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_usec;
      job_info.end_date_ = end_date;
      job_info.repeat_interval_ = repeat_interval;
      job_info.enabled_ = true;
      job_info.auto_drop_ = false;
      job_info.max_run_duration_ = max_run_duration_sec;
      job_info.exec_env_ = exec_env;
      job_info.comments_ = ObString("logic table namespace stats gather background job");
      job_info.func_type_ = ObDBMSSchedFuncType::LOGICAL_TABLE_NAMESPACE_STATS_JOB;

      if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("logic table namespace stats job already exists", K(job_info));
        } else {
          LOG_WARN("failed to create logic table namespace stats job", KR(ret), K(job_info));
        }
      } else {
        LOG_INFO("successfully created logic table namespace stats job", K(job_info));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
