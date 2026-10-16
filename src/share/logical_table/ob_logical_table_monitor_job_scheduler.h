/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_MONITOR_JOB_SCHEDULER_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_MONITOR_JOB_SCHEDULER_H_

#include "sql/engine/ob_exec_context.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace dbms_scheduler
{
class ObDBMSSchedJobInfo;
}
namespace share
{

static const char *LOGICAL_TABLE_NAMESPACE_STATS_JOB_NAME = "logic_table_namespace_stats_job";

class ObLogicalTableMonitorJobScheduler
{
public:
  static bool is_namespace_stats_job(const common::ObString &job_name);

  static int set_attribute(
    const sql::ObSQLSessionInfo *session,
    const common::ObString &job_name,
    const common::ObString &attr_name,
    const common::ObString &attr_val_str,
    const dbms_scheduler::ObDBMSSchedJobInfo &job_info,
    bool &is_namespace_stats_attr,
    ObDMLSqlSplicer &dml);

  static int create_namespace_stats_job(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

  static int create_namespace_stats_job_for_upgrade(
    common::ObMySQLProxy *sql_proxy,
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);

private:
  static int check_job_exists_in_trans_(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const char *job_name,
    bool &job_exists);

  static int create_job_(
    const schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    ObMySQLTransaction &trans);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_MONITOR_JOB_SCHEDULER_H_ */
