/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER


#include "ob_dbms_sched_job_utils.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "share/balance/ob_scheduled_trigger_partition_balance.h" // ObScheduledTriggerPartitionBalance
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h"
#include "storage/ob_common_id_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_rpc_proxy.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_time_utils.h"
#include "lib/allocator/page_arena.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/json/ob_json.h"
#include "sql/session/ob_basic_session_info.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace sqlclient;
using namespace sql;

namespace dbms_scheduler
{
ObDBMSSchedFuncSet ObDBMSSchedFuncSet::instance_;

namespace
{

static const int64_t ASYNC_QUERY_JOB_CONFIG_VERSION = 1;
static const char *const ASYNC_RESULT_CLEANUP_JOB_NAME = "__ob_async_result_cleanup";
static const char *const ASYNC_RESULT_CLEANUP_JOB_ACTION = "ASYNC_RESULT_CLEANUP";
static const char *const ASYNC_RESULT_CLEANUP_REPEAT_INTERVAL = "FREQ=DAILY;INTERVAL=1";
static const int64_t ASYNC_RESULT_CLEANUP_MAX_RUN_DURATION_SEC = 6 * 60 * 60;

bool is_safe_async_query_hint_line(const ObString &hint_line)
{
  bool is_safe = true;
  for (int64_t i = 0; is_safe && i < hint_line.length(); ++i) {
    is_safe = '\0' != hint_line[i]
        && !(i + 1 < hint_line.length()
             && (('/' == hint_line[i] && '*' == hint_line[i + 1])
                 || ('*' == hint_line[i] && '/' == hint_line[i + 1])));
  }
  return is_safe;
}

} // namespace

int ObAsyncQueryJobConfig::deserialize(
    const ObString &job_config,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator parser_allocator("AsyncJobCfg");
  reset();
  if (job_config.empty()) {
    // Empty is the backward-compatible default: native result store with no
    // result-store execution hint.
  } else {
    json::Parser parser;
    json::Value *root = NULL;
    int64_t version = -1;
    bool has_version = false;
    bool has_hint_line = false;
    bool has_result_store = false;
    ObString parsed_hint_line;
    if (OB_FAIL(parser.init(&parser_allocator))) {
      LOG_WARN("failed to initialize async query job config parser", K(ret));
    } else if (OB_FAIL(parser.parse(job_config.ptr(), job_config.length(), root))) {
      LOG_WARN("failed to parse async query job config", K(ret), K(job_config));
    } else if (OB_ISNULL(root) || json::JT_OBJECT != root->get_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("async query job config is not a json object", K(ret), K(job_config));
    } else {
      DLIST_FOREACH_X(it, root->get_object(), OB_SUCC(ret)) {
        if (0 == it->name_.case_compare("version")) {
          if (has_version || json::JT_NUMBER != it->value_->get_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid async query job config version", K(ret), K(has_version));
          } else {
            has_version = true;
            version = it->value_->get_number();
          }
        } else if (0 == it->name_.case_compare("hint_line")) {
          if (has_hint_line || json::JT_STRING != it->value_->get_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid async query job config hint_line", K(ret), K(has_hint_line));
          } else {
            has_hint_line = true;
            parsed_hint_line = it->value_->get_string();
          }
        } else if (0 == it->name_.case_compare("result_store")) {
          if (has_result_store || json::JT_STRING != it->value_->get_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid async query job config result_store",
                     K(ret), K(has_result_store));
          } else {
            const ObString store = it->value_->get_string();
            has_result_store = true;
            if (0 == store.case_compare("NATIVE")) {
              result_store_ = ObAsyncQueryResultStore::NATIVE;
            } else if (0 == store.case_compare("EXTERNAL")) {
              result_store_ = ObAsyncQueryResultStore::EXTERNAL;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid async query result store", K(ret), K(store));
            }
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unknown async query job config option", K(ret), K(it->name_));
        }
      }
      if (OB_SUCC(ret) && (!has_version || ASYNC_QUERY_JOB_CONFIG_VERSION != version)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported async query job config version", K(ret), K(has_version), K(version));
      } else if (OB_SUCC(ret) && !is_safe_async_query_hint_line(parsed_hint_line)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsafe async query job config hint_line", K(ret));
      } else if (OB_SUCC(ret) && !parsed_hint_line.empty()
                 && OB_FAIL(ob_write_string(allocator, parsed_hint_line, hint_line_))) {
        LOG_WARN("failed to copy async query hint line", K(ret));
      }
    }
  }
  return ret;
}

int ObAsyncQueryJobConfig::serialize(
    ObIAllocator &allocator,
    ObString &job_config) const
{
  int ret = OB_SUCCESS;
  const char *result_store_name = NULL;
  job_config.reset();
  if (ObAsyncQueryResultStore::NATIVE == result_store_) {
    result_store_name = "NATIVE";
  } else if (ObAsyncQueryResultStore::EXTERNAL == result_store_) {
    result_store_name = "EXTERNAL";
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async query result store", K(ret), K(result_store_));
  }

  if (OB_SUCC(ret)) {
    ObJsonObject *root = OB_NEWx(ObJsonObject, &allocator, &allocator);
    ObJsonInt *version = OB_NEWx(
        ObJsonInt, &allocator, ASYNC_QUERY_JOB_CONFIG_VERSION);
    ObJsonString *store = OB_NEWx(
        ObJsonString, &allocator, ObString::make_string(result_store_name));
    ObJsonString *hint = OB_NEWx(ObJsonString, &allocator, hint_line_);
    if (OB_ISNULL(root) || OB_ISNULL(version)
        || OB_ISNULL(store) || OB_ISNULL(hint)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async query job config json",
               K(ret), KP(root), KP(version), KP(store), KP(hint));
    } else if (OB_FAIL(root->object_add(ObString::make_string("version"), version))) {
      LOG_WARN("failed to add async query job config version", K(ret));
    } else if (OB_FAIL(root->object_add(ObString::make_string("result_store"), store))) {
      LOG_WARN("failed to add async query job result store", K(ret));
    } else if (OB_FAIL(root->object_add(ObString::make_string("hint_line"), hint))) {
      LOG_WARN("failed to add async query job hint line", K(ret));
    } else {
      ObJsonBuffer buffer(&allocator);
      if (OB_FAIL(root->print(buffer, false))) {
        LOG_WARN("failed to serialize async query job config", K(ret));
      } else if (OB_UNLIKELY(
                     buffer.string().length() > OB_MAX_JOB_INFO_JOB_CONFIG_LENGTH)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_USER_ERROR(OB_SIZE_OVERFLOW);
        LOG_WARN("async query job config is too long", K(ret),
                 "length", buffer.string().length(),
                 K(OB_MAX_JOB_INFO_JOB_CONFIG_LENGTH));
      } else if (OB_FAIL(ob_write_string(
                     allocator, buffer.string(), job_config, true))) {
        LOG_WARN("failed to save async query job config", K(ret));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobInfo::build_async_query_result_table_name(ObSqlString &table_name) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_async_query_job())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not an async query job", KR(ret), K(func_type_), K(job_));
  } else if (OB_FAIL(table_name.assign_fmt("__ob_aqr_t%016lx_u%016lx_j%016lx",
                                           static_cast<unsigned long>(tenant_id_),
                                           static_cast<unsigned long>(user_id_),
                                           static_cast<unsigned long>(job_)))) {
    LOG_WARN("failed to build async query result table name", KR(ret),
             K(tenant_id_), K(user_id_), K(job_));
  }
  return ret;
}

bool ObDBMSSchedJobInfo::is_async_query_result_target(const ObString &database_name,
                                                      const ObString &table_name) const
{
  ObSqlString expected_table_name;
  return is_async_query_job()
      && 0 == database_name.case_compare(get_async_query_result_database_name())
      && OB_SUCCESS == build_async_query_result_table_name(expected_table_name)
      && 0 == table_name.case_compare(expected_table_name.string());
}

int ObDBMSSchedJobUtils::check_is_valid_name(const ObString &name) 
{
  int ret = OB_SUCCESS;
  if (NULL != name.find('\'') || NULL != name.find('\"') || 
      NULL != name.find(';') || NULL != name.find('`') || NULL != name.find(' ')) {
      ret = OB_ERR_ILLEGAL_NAME;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_is_valid_job_style(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (0 != str.case_compare("REGULAR")) {
      ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_is_valid_argument_num(const int64_t num) 
{
  int ret = OB_SUCCESS;
  if (0 > num) {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_is_valid_job_type(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (0 != str.case_compare("PLSQL_BLOCK") && 0 != str.case_compare("STORED_PROCEDURE")) {
      ret = OB_NOT_SUPPORTED;
  }
  return ret;
}
/*
IMMEDIATE - Start date and repeat interval are NULL
ONCE - Repeat interval is NULL
PLSQL - PL/SQL expression used as schedule
CALENDAR - Oracle calendaring expression used as schedule
*/
int ObDBMSSchedJobUtils::check_is_valid_sched_type(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (0 != str.case_compare("IMMEDIATE") && 0 != str.case_compare("ONCE") && 0 != str.case_compare("PLSQL") && 0 != str.case_compare("CALENDAR")) {
      ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_is_valid_state(const ObString &str) 
{
  int ret = OB_SUCCESS;
  if (0 != str.case_compare("STARTED") && 0 != str.case_compare("SUCCEEDED") &&
      0 != str.case_compare("FAILED") && 0 != str.case_compare("BROKEN") &&
      0 != str.case_compare("COMPLETED") && 0 != str.case_compare("STOPPED") &&
      0 != str.case_compare("SCHEDULED") && 0 != str.case_compare("KILLED")) {
      ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_is_valid_end_date(const int64_t start_date, const int64_t end_date) 
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  if (end_date < now || start_date > end_date) {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_is_valid_repeat_interval(const int64_t tenant_id, const ObString &str, bool is_limit_interval_num) {
  int ret = OB_SUCCESS;
  if (!str.empty() && 0 != str.case_compare("null")) {
    ObDBMSSchedTimeUtil time_util;
    OZ(time_util.check_is_valid_repeat_interval(tenant_id, str, is_limit_interval_num));
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_is_valid_max_run_duration(const int64_t max_run_duration) 
{
  int ret = OB_SUCCESS;
  if (0 > max_run_duration) {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_not_buildin_job_class(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (str.empty() || 0 == str.case_compare("MYSQL_EVENT_JOB_CLASS") || 0 == str.case_compare("OLAP_ASYNC_JOB_CLASS")
      || 0 == str.case_compare("DATE_EXPRESSION_JOB_CLASS")) {
      ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_is_valid_log_history(const int64_t log_history)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_LOG_HISTORY = 1000000;
  if (log_history < 0 || log_history > MAX_LOG_HISTORY) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "log_history value is invalid");
  }
  return ret;
}

int ObDBMSSchedJobUtils::zone_check_impl(int64_t tenant_id, const ObString &zone)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTenantSchema *tenant_info = NULL;
  ObSchemaGetterGuard schema_guard;
  common::ObArray<common::ObZone> zone_list;
  bool found = false;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), KP(tenant_info));
  } else if (OB_FAIL(tenant_info->get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      if (0 == zone_list.at(i).str().case_compare(zone)) {
        found = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "OBE-23428: The job-specified zone does not exist.");
  }
  return ret;
}

int ObDBMSSchedJobUtils::job_class_check_impl(int64_t tenant_id, const ObString &job_class_name)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  int64_t rows = 0;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::check_is_valid_name(job_class_name))) {
    ret = OB_SP_RAISE_APPLICATION_ERROR;
    ObString err_info("job class is an invalid name for a database object.");
    LOG_ORACLE_USER_ERROR(OB_SP_RAISE_APPLICATION_ERROR, 27452L, err_info.length(), err_info.ptr());
  } else {
    CK (OB_NOT_NULL(sql_proxy));
    OZ (sql.append_fmt("select count(*) rows from %s where tenant_id = %ld and job_class_name = \'%.*s\'",
        OB_ALL_TENANT_SCHEDULER_JOB_CLASS_TNAME,
        ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), job_class_name.length(), job_class_name.ptr()));

    if (OB_SUCC(ret)) {
      rows = 0;
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        if (OB_FAIL(sql_proxy->read(result, tenant_id, sql.ptr()))) {
          LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id));
        } else if (OB_NOT_NULL(result.get_result())) {
          if (OB_SUCCESS == (ret = result.get_result()->next())) {
            EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*(result.get_result()), "rows", rows, uint64_t);
            if (OB_SUCC(ret) && (result.get_result()->next()) != OB_ITER_END) {
              LOG_ERROR("got more than one row for count!", K(ret), K(tenant_id));
              ret = OB_ERR_UNEXPECTED;
            }
          } else {
            LOG_WARN("failed to get timestamp", K(ret), K(tenant_id));
          }
        }
      }
      if (OB_SUCC(ret) && rows == 0) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "job class not exist");
        LOG_WARN("job class not exist", K(tenant_id), K(job_class_name));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::get_max_failures_value(int64_t tenant_id, const ObString &src_str, int64_t &value)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  const int64_t MAX_FAILURES_LIMIT = 1000000;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
  } else if (data_version < MOCK_DATA_VERSION_4_2_1_9 || (data_version >= DATA_VERSION_4_2_2_0 && data_version <= MOCK_DATA_VERSION_4_2_5_0)
            || (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_5_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                    "ORA-23428: not support set max_failures");
  } else {
    const int64_t MAX_SRC_STR_LEN = 16;
    char src_str_buf[MAX_SRC_STR_LEN];
    int64_t pos = src_str.to_string(src_str_buf, MAX_SRC_STR_LEN);
    value = atoll(src_str_buf);
    if (value < 0 || value > MAX_FAILURES_LIMIT) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("max_failures value overflow", K(ret), K(tenant_id), K(value));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                    "ORA-23428: job associated attr val is not supported");
    }
  }
  return ret;
}

int ObDBMSSchedJobInfo::deep_copy(ObIAllocator &allocator, const ObDBMSSchedJobInfo &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  user_id_ = other.user_id_;
  database_id_ = other.database_id_;
  job_ = other.job_;
  last_modify_ = other.last_modify_;
  last_date_ = other.last_date_;
  this_date_ = other.this_date_;
  next_date_ = other.next_date_;
  total_ = other.total_;
  failures_ = other.failures_;
  flag_ = other.flag_;
  scheduler_flags_ = other.scheduler_flags_;
  start_date_ = other.start_date_;
  end_date_ = other.end_date_;
  enabled_ = other.enabled_;
  auto_drop_ = other.auto_drop_;
  interval_ts_ = other.interval_ts_;
  is_oracle_tenant_ = other.is_oracle_tenant_;
  max_run_duration_ = other.max_run_duration_;
  max_failures_ = other.max_failures_;
  func_type_ = other.func_type_;
  this_exec_date_ = other.this_exec_date_;

  OZ (ob_write_string(allocator, other.lowner_, lowner_));
  OZ (ob_write_string(allocator, other.powner_, powner_));
  OZ (ob_write_string(allocator, other.cowner_, cowner_));

  OZ (ob_write_string(allocator, other.interval_, interval_));
  OZ (ob_write_string(allocator, other.repeat_interval_, repeat_interval_));

  OZ (ob_write_string(allocator, other.what_, what_));
  OZ (ob_write_string(allocator, other.nlsenv_, nlsenv_));
  OZ (ob_write_string(allocator, other.charenv_, charenv_));
  OZ (ob_write_string(allocator, other.field1_, field1_));
  OZ (ob_write_string(allocator, other.exec_env_, exec_env_));
  OZ (ob_write_string(allocator, other.job_name_, job_name_));
  OZ (ob_write_string(allocator, other.job_class_, job_class_));
  OZ (ob_write_string(allocator, other.program_name_, program_name_));
  OZ (ob_write_string(allocator, other.state_, state_));
  OZ (ob_write_string(allocator, other.job_action_, job_action_));
  OZ (ob_write_string(allocator, other.job_type_, job_type_));
  OZ (ob_write_string(allocator, other.this_exec_addr_, this_exec_addr_));
  OZ (ob_write_string(allocator, other.this_exec_trace_id_, this_exec_trace_id_));
  OZ (ob_write_string(allocator, other.job_config_, job_config_));
  //处理存在兼容性问题的列
  //job style 
  OZ (ob_write_string(allocator, "REGULAR", job_style_));

  return ret;
}

ObDBMSSchedFuncType ObDBMSSchedJobInfo::get_func_type() const
{
  int ret = OB_SUCCESS;
  ObDBMSSchedFuncType func_type = func_type_;
  if (func_type == ObDBMSSchedFuncType::USER_JOB) { // update old inner job func type
    if (ObDbmsStatsMaintenanceWindow::is_stats_job(job_name_)) {
      func_type = ObDBMSSchedFuncType::STAT_MAINTENANCE_JOB;
    } else if (ObScheduledTriggerPartitionBalance::is_trigger_job(job_name_)) {
      func_type = ObDBMSSchedFuncType::NODE_BALANCE_JOB;
    } else if (!!(scheduler_flags_ & JOB_SCHEDULER_FLAG_DATE_EXPRESSION_JOB_CLASS)) {
      func_type = ObDBMSSchedFuncType::MVIEW_JOB;
    } else if (0 == job_class_.case_compare("MYSQL_EVENT_JOB_CLASS")) {
      func_type = ObDBMSSchedFuncType::MYSQL_EVENT_JOB;
    } else if (0 == job_class_.case_compare("OLAP_ASYNC_JOB_CLASS")) {
      func_type = ObDBMSSchedFuncType::OLAP_ASYNC_JOB;
    }
    if (func_type != ObDBMSSchedFuncType::USER_JOB) {
      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
      } else if (DATA_VERSION_4_3_5_1 <= data_version) {
        ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
        ObDMLSqlSplicer dml;
        ObSqlString sql;
        int64_t affected_rows = 0;
        CK (OB_NOT_NULL(sql_proxy));
        OZ (dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_)));
        OZ (dml.add_pk_column("job", job_));
        OZ (dml.add_pk_column("job_name", job_name_));
        OZ (dml.add_column("func_type", static_cast<uint64_t>(func_type)));
        OZ (dml.splice_update_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
        OZ (sql_proxy->write(tenant_id_, sql.ptr(), affected_rows));
      }
    }
  }
  return func_type;
}

int ObDBMSSchedJobClassInfo::deep_copy(common::ObIAllocator &allocator, const ObDBMSSchedJobClassInfo &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  is_oracle_tenant_ = other.is_oracle_tenant_;
  OZ (log_history_.from(other.log_history_, allocator));
  OZ (ob_write_string(allocator, other.job_class_name_, job_class_name_));
  OZ (ob_write_string(allocator, other.service_, service_));
  OZ (ob_write_string(allocator, other.resource_consumer_group_, resource_consumer_group_));
  OZ (ob_write_string(allocator, other.logging_level_, logging_level_));
  OZ (ob_write_string(allocator, other.comments_, comments_));
  return ret;
}

int ObDBMSSchedJobUtils::generate_job_id(int64_t tenant_id, int64_t &max_job_id)
{
  int ret = OB_SUCCESS;
  ObCommonID raw_id;
  if (OB_FAIL(storage::ObCommonIDUtils::gen_unique_id_by_rpc(tenant_id, raw_id))) {
    LOG_WARN("gen unique id failed", K(ret), K(tenant_id));
  } else {
    max_job_id = raw_id.id() + ObDBMSSchedTableOperator::JOB_ID_OFFSET;
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_async_query_result_ready(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  const ObDatabaseSchema *database_schema = NULL;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  bool is_oracle_mode = false;
  bool upgrade_pending = false;
  ObArenaAllocator allocator("AsyncResReady");
  ObDBMSSchedJobInfo cleanup_job_info;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async query requires a user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get tenant data version for async query", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_2_1) {
    ret = OB_NOT_SUPPORTED;
    upgrade_pending = true;
    LOG_WARN("async query tenant data version is not ready", KR(ret), K(tenant_id), KDV(data_version));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id, ObString(OB_ASYNC_QUERY_RESULT_DATABASE_NAME), database_schema))) {
    LOG_WARN("failed to get async result database schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_NOT_SUPPORTED;
    upgrade_pending = true;
    LOG_WARN("async result database is not ready", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("failed to get tenant system variables", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant system variables are null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable_schema->get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get tenant compatibility mode", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_dbms_sched_job_info(sql_client,
                                              tenant_id,
                                              is_oracle_mode,
                                              ObString(ASYNC_RESULT_CLEANUP_JOB_NAME),
                                              allocator,
                                              cleanup_job_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_NOT_SUPPORTED;
      upgrade_pending = true;
      LOG_WARN("async result cleanup job is not ready", KR(ret), K(tenant_id));
    } else {
      LOG_WARN("failed to get async result cleanup job", KR(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(!cleanup_job_info.is_async_result_cleanup_job())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result cleanup job has unexpected type", KR(ret), K(tenant_id));
  }
  if (upgrade_pending) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "SUBMIT JOB SELECT before tenant async result upgrade completes");
  }
  return ret;
}

int ObDBMSSchedJobUtils::ensure_async_result_cleanup_job(
    const share::schema::ObSysVariableSchema &sys_variable,
    const uint64_t tenant_id,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  bool is_oracle_mode = false;
  bool job_exists = false;
  ObArenaAllocator allocator("AsyncResClean");
  ObDBMSSchedJobInfo existing_job_info;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async result cleanup must be a user-tenant job", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get tenant compatibility mode", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_dbms_sched_job_info(trans,
                                               tenant_id,
                                               is_oracle_mode,
                                               ObString(ASYNC_RESULT_CLEANUP_JOB_NAME),
                                               allocator,
                                               existing_job_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to look up async result cleanup job", KR(ret), K(tenant_id));
    }
  } else if (OB_UNLIKELY(!existing_job_info.is_async_result_cleanup_job())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async result cleanup job name conflicts with another scheduler job",
             KR(ret), K(tenant_id), K(existing_job_info));
  } else {
    job_exists = true;
    LOG_TRACE("async result cleanup job already exists", K(tenant_id), K(existing_job_info));
  }

  if (OB_SUCC(ret) && !job_exists) {
    char exec_env_buf[OB_MAX_PROC_ENV_LENGTH] = {0};
    int64_t exec_env_pos = 0;
    int64_t job_id = OB_INVALID_ID;
    if (OB_FAIL(sql::ObExecEnv::gen_exec_env(
            sys_variable, exec_env_buf, OB_MAX_PROC_ENV_LENGTH, exec_env_pos))) {
      LOG_WARN("failed to generate async result cleanup execution environment", KR(ret), K(tenant_id));
    } else if (OB_FAIL(generate_job_id(tenant_id, job_id))) {
      LOG_WARN("failed to generate async result cleanup job id", KR(ret), K(tenant_id));
    } else {
      HEAP_VAR(ObDBMSSchedJobInfo, job_info) {
        job_info.tenant_id_ = tenant_id;
        job_info.job_ = job_id;
        job_info.job_name_ = ObString(ASYNC_RESULT_CLEANUP_JOB_NAME);
        // The executor dispatches this shadow function type directly; the
        // non-empty action is only the scheduler's persisted job contract.
        job_info.job_action_ = ObString(ASYNC_RESULT_CLEANUP_JOB_ACTION);
        job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
        job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
        job_info.cowner_ = is_oracle_mode ? ObString("SYS") : ObString(OB_SYS_DATABASE_NAME);
        job_info.job_style_ = ObString("REGULAR");
        job_info.job_type_ = ObString("PLSQL_BLOCK");
        job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
        job_info.start_date_ = ObTimeUtility::current_time();
        job_info.end_date_ = ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE;
        job_info.repeat_interval_ = ObString(ASYNC_RESULT_CLEANUP_REPEAT_INTERVAL);
        job_info.enabled_ = true;
        job_info.auto_drop_ = false;
        job_info.max_run_duration_ = ASYNC_RESULT_CLEANUP_MAX_RUN_DURATION_SEC;
        job_info.exec_env_ = ObString(exec_env_pos, exec_env_buf);
        job_info.comments_ = ObString("daily cleanup for expired async query results");
        job_info.func_type_ = ObDBMSSchedFuncType::ASYNC_RESULT_CLEANUP_JOB;

        if (OB_FAIL(create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ret = OB_SUCCESS;
            LOG_TRACE("async result cleanup job already exists", K(job_info));
          } else {
            LOG_WARN("failed to create async result cleanup job", KR(ret), K(job_info));
          }
        } else {
          LOG_DEBUG("created async result cleanup job", K(job_info));
        }
      } else {
        LOG_WARN("failed to allocate async result cleanup job", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::stop_dbms_sched_job(
    common::ObISQLClient &sql_client,
    const ObDBMSSchedJobInfo &job_info,
    const bool is_delete_after_stop)
{
  int ret = OB_SUCCESS;
  obrpc::ObDBMSSchedJobRpcProxy *rpc_proxy = GCTX.dbms_sched_job_rpc_proxy_;
  uint64_t tenant_id = job_info.tenant_id_;
  bool is_oracle_tenant =  lib::is_oracle_mode();
  ObSqlString sql;
  CK (OB_NOT_NULL(rpc_proxy));
  if (OB_SUCC(ret)) {
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
    } else if (data_version < MOCK_DATA_VERSION_4_2_1_5
      || (data_version >= DATA_VERSION_4_2_2_0 && data_version < MOCK_DATA_VERSION_4_2_4_0)
      || (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_2_0)) {
      ret = OB_NOT_SUPPORTED;
    }
  }

  if (OB_SUCC(ret)) {
    if (is_delete_after_stop) {
      ObObj state_obj;
      state_obj.set_char("KILLED");
      if(OB_FAIL(update_dbms_sched_job_info(sql_client, job_info, ObString("state"), state_obj))) {
        LOG_WARN("update job info failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt("select svr_ip, svr_port, session_id from %s where tenant_id = %lu and job_name = \'%.*s\'",
        OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_TNAME, tenant_id, job_info.job_name_.length(),job_info.job_name_.ptr()))) {
        LOG_WARN("append sql failed", KR(ret)); 
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, result) {
          if (OB_FAIL(sql_client.read(result, sql.ptr()))) {
            LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id), K(job_info.job_name_));
          } else if (OB_ISNULL(result.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", K(ret), K(sql), K(tenant_id), K(job_info.job_name_));
          } else {
            bool result_empty = true;
            do {
              if (OB_FAIL(result.get_result()->next())) {
                if (ret == OB_ITER_END) {
                  //do nothing
                } else {
                  LOG_WARN("fail to get result", K(ret));
                }
              } else {
                result_empty = false;
                uint64_t session_id = OB_INVALID_ID;
                ObAddr svr;
                ObString svr_ip;
                int64_t svr_port = OB_INVALID_INDEX;
                EXTRACT_VARCHAR_FIELD_MYSQL(*(result.get_result()), "svr_ip", svr_ip);
                EXTRACT_UINT_FIELD_MYSQL(*(result.get_result()), "session_id", session_id, uint64_t);
                EXTRACT_INT_FIELD_MYSQL(*(result.get_result()), "svr_port", svr_port, int64_t);
                if (OB_SUCC(ret)) {
                  if (!svr.set_ip_addr(svr_ip, svr_port)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("set addr failed", K(svr_ip), K(svr_port));
                  } else {
                    LOG_INFO("send rpc", K(tenant_id), K(job_info.job_name_), K(svr), K(session_id));
                    ObString stop_job_name = ObString(job_info.job_name_);
                    OZ (rpc_proxy->stop_dbms_sched_job(tenant_id,
                      stop_job_name,
                      svr,
                      session_id));
                  }
                }
              }
            } while (OB_SUCC(ret));
            if (OB_ITER_END == ret) {
              if (result_empty) {
                ret = OB_ENTRY_NOT_EXIST;
                LOG_WARN("no running job", K(ret), K(tenant_id), K(job_info.job_name_));
              } else {
                ret = OB_SUCCESS;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::remove_dbms_sched_job(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObString &job_name,
    const bool if_exists)
{
  int ret = OB_SUCCESS;
  bool is_oracle_tenant =  lib::is_oracle_mode();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || job_name.empty() || OB_FAIL(check_is_valid_name(job_name)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(job_name));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(
        "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("job_name", job_name))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, dml, affected_rows))) {
        LOG_WARN("execute delete failed", KR(ret));
      } else if (is_zero_row(affected_rows) && !if_exists) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("execute delete failed", KR(ret), K(if_exists));
      } else if (!if_exists && !is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be two", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::create_dbms_sched_job(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t job_id,
    const dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  bool is_oracle_tenant =  lib::is_oracle_mode();
  int64_t next_date = job_info.start_date_;
  if ((job_info.func_type_ >= ObDBMSSchedFuncType::FUNCTION_TYPE_MAXNUM)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("func type has not register", K(ret), K(job_info), K(job_info.func_type_));
  //chcek job name
  } else if (job_info.job_name_.empty() || OB_FAIL(check_is_valid_name(job_info.job_name_)) || 0 == (job_info.job_name_.case_compare("__dummy_guard"))) {
    ret = OB_INVALID_ARGUMENT;
  //check job style
  } else if (job_info.job_style_.empty() || OB_FAIL(check_is_valid_job_style(job_info.job_style_))) {
    ret = OB_INVALID_ARGUMENT;
  //check job type
  } else if (job_info.job_type_.empty() || OB_FAIL(check_is_valid_job_type(job_info.job_type_))) {
    ret = OB_INVALID_ARGUMENT;
  //check job owner
  } else if (job_info.powner_.empty() || job_info.lowner_.empty() || job_info.cowner_.empty()) {
    ret = OB_INVALID_ARGUMENT;
  //check job class
  } else if (job_info.job_class_.empty() || OB_FAIL(check_is_valid_name(job_info.job_class_))) {
    ret = OB_INVALID_ARGUMENT;
  //check program job
  } else if (0 == job_info.job_type_.case_compare("STORED_PROCEDURE") && OB_FAIL(check_is_valid_name(job_info.program_name_))) {
    ret = OB_INVALID_ARGUMENT;
  // async query is an immediate, one-shot job
  } else if (job_info.is_async_query_job()
             && (!job_info.is_once_job()
                 || job_info.start_date_ > ObTimeUtility::current_time())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async query must be submitted as an immediate one-shot job", K(ret), K(job_info));
  //check repeat_interval
  } else if (!job_info.repeat_interval_.empty() && !job_info.is_mview_job() && OB_FAIL(check_is_valid_repeat_interval(tenant_id, job_info.repeat_interval_))) {
    ret = OB_INVALID_ARGUMENT;
  //check argument
  } else if (OB_FAIL(check_is_valid_argument_num(job_info.number_of_argument_))) {
    ret = OB_INVALID_ARGUMENT;
  
  //check database_id/user_id
  // } else if (OB_INVALID_ID == job_info.database_id_ || OB_INVALID_ID == job_info.user_id_) {
  //   ret = OB_INVALID_ARGUMENT;

  //check end_date
  } else if (OB_FAIL(check_is_valid_end_date(job_info.start_date_, job_info.end_date_))) {
    ret = OB_INVALID_ARGUMENT;
  //check max_run_duration
  } else if (OB_FAIL(check_is_valid_max_run_duration(job_info.max_run_duration_))) {
    ret = OB_INVALID_ARGUMENT;
  //check destination/credetial need null
  } else if (!job_info.destination_name_.empty() || !job_info.credential_name_.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (job_info.is_mview_job() || job_info.repeat_interval_.empty() || (0 == job_info.repeat_interval_.case_compare("null"))) {
    next_date = job_info.start_date_;
  } else if (OB_FAIL(calc_dbms_sched_repeat_expr(job_info, next_date, true/*is_first_time*/))) {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCC(ret)) {
    ObCommonID raw_id;
    if (OB_INVALID_TENANT_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
    } else {
      ObDMLSqlSplicer dml;
      ObSqlString sql;
      const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
      int64_t affected_rows = 0;
      const int64_t now = ObTimeUtility::current_time();
      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
        LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
      } else {
        for (int i = 0; OB_SUCC(ret) && i <= 1; ++i) {
          OZ (dml.add_gmt_create(now));
          OZ (dml.add_gmt_modified(now));
          OZ (dml.add_pk_column("tenant_id",
              ObSchemaUtils::get_extract_tenant_id(job_info.tenant_id_, job_info.tenant_id_)));
          if ((MOCK_DATA_VERSION_4_2_4_0 <= data_version && DATA_VERSION_4_3_0_0 > data_version) || 
              data_version > DATA_VERSION_4_3_2_0) {
            OZ (dml.add_column("user_id", job_info.user_id_));
            OZ (dml.add_column("database_id", job_info.database_id_));
          }
          OZ (dml.add_pk_column("job", 0 == i? 0 : job_id));
          OZ (dml.add_column("lowner", ObHexEscapeSqlStr(job_info.lowner_)));
          OZ (dml.add_column("powner", ObHexEscapeSqlStr(job_info.powner_)));
          OZ (dml.add_column("cowner", ObHexEscapeSqlStr(job_info.cowner_)));
          OZ (dml.add_raw_time_column("next_date", next_date));
          OZ (dml.add_column("total", 0));
          if (job_info.repeat_interval_.empty()) {
            OZ (dml.add_column("`interval#`", ObHexEscapeSqlStr(ObString("null"))));
          } else {
            OZ (dml.add_column("`interval#`", ObHexEscapeSqlStr(job_info.repeat_interval_)));
          } 
          OZ (dml.add_column("flag", 0));
          OZ (dml.add_column("job_name", ObHexEscapeSqlStr(job_info.job_name_)));
          OZ (dml.add_column("job_style", ObHexEscapeSqlStr(job_info.job_style_)));
          OZ (dml.add_column("job_type", ObHexEscapeSqlStr(job_info.job_type_)));
          OZ (dml.add_column("job_class", ObHexEscapeSqlStr(job_info.job_class_)));
          OZ (dml.add_column("job_action", ObHexEscapeSqlStr(job_info.job_action_)));
          OZ (dml.add_column("what", ObHexEscapeSqlStr(job_info.job_action_)));
          OZ (dml.add_raw_time_column("start_date", job_info.start_date_));
          OZ (dml.add_raw_time_column("end_date", job_info.end_date_));
          OZ (dml.add_column("repeat_interval", ObHexEscapeSqlStr(job_info.repeat_interval_)));
          OZ (dml.add_column("enabled", job_info.enabled_));
          OZ (dml.add_column("auto_drop", job_info.auto_drop_));
          OZ (dml.add_column("max_run_duration", job_info.max_run_duration_));
          OZ (dml.add_column("interval_ts", 0));
          OZ (dml.add_column("scheduler_flags", job_info.scheduler_flags_));
          OZ (dml.add_column("exec_env", job_info.exec_env_));
          OZ (dml.add_column("comments", ObHexEscapeSqlStr(job_info.comments_)));
          OZ (dml.add_column("program_name", ObHexEscapeSqlStr(job_info.program_name_)));
          OZ (dml.add_column("max_failures", job_info.max_failures_));
          if ( DATA_VERSION_4_3_5_1 <= data_version) {
            OZ (dml.add_column("func_type", static_cast<uint64_t>(job_info.func_type_)));
          }
          if (DATA_VERSION_SUPPORT_JOB_CONFIG(data_version)) {
            OZ (dml.add_column("job_config", ObHexEscapeSqlStr(job_info.job_config_)));
          }
          OZ (dml.finish_row());
        }
        OZ(dml.splice_batch_insert_sql(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, sql));
        OZ(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows));
        if (OB_SUCC(ret) && OB_UNLIKELY(!is_double_row(affected_rows))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("affected_rows unexpected to be two", KR(ret), K(affected_rows));
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::update_dbms_sched_job_info(common::ObISQLClient &sql_client,
                                                    const ObDBMSSchedJobInfo &job_info,
                                                    const ObString &job_attribute_name,
                                                    const ObObj &job_attribute_value,
                                                    const bool from_pl_set_attr)
{
  int ret = OB_SUCCESS;
  bool is_oracle_tenant =  lib::is_oracle_mode();
  const int64_t now = ObTimeUtility::current_time();
  ObDMLSqlSplicer dml;
  int64_t exec_tenant_id = 0;
  int64_t tenant_id = job_info.tenant_id_;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  //chcek job name
  } else if (job_info.job_name_.empty() || OB_FAIL(check_is_valid_name(job_info.job_name_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job name", KR(ret), K(job_info.job_name_));
  } else if (OB_FAIL(dml.add_pk_column(
        "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("job_name", job_info.job_name_)) || OB_FAIL(dml.add_gmt_modified(now))) {
      LOG_WARN("add column failed", KR(ret));
  } else if (0 == job_attribute_name.case_compare("job_name") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_column("job_name", job_attribute_value.get_string()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()));
    }
  } else if (0 == job_attribute_name.case_compare("powner") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_column("powner", job_attribute_value.get_string()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()));
    }
  } else if (0 == job_attribute_name.case_compare("user_id") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_column("user_id", job_attribute_value.get_int()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_int()));
    }
  } else if (0 == job_attribute_name.case_compare("state") && !from_pl_set_attr) {
    if (OB_FAIL(check_is_valid_state(job_attribute_value.get_string()))) {
      LOG_WARN("invalid state", KR(ret), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(dml.add_column("state", job_attribute_value.get_string()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()));
    }
  } else if (0 == job_attribute_name.case_compare("auto_drop") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_column("auto_drop", job_attribute_value.get_bool()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_bool()));
    }
  } else if (0 == job_attribute_name.case_compare("enabled") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_column("enabled", job_attribute_value.get_bool()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_bool()));
    } else if (job_attribute_value.get_bool() && (0 == job_info.state_.case_compare("BROKEN"))) {
      if (OB_FAIL(dml.add_column("state", "SCHEDULED"))) {
        LOG_WARN("add state column failed", KR(ret), K(job_info.state_));
      } else if (OB_FAIL(dml.add_column("failures", 0))) {
        LOG_WARN("add failures column failed", KR(ret), K(job_info.failures_));
      }
    }
  } else if (0 == job_attribute_name.case_compare("repeat_interval") && !from_pl_set_attr) {
    int64_t next_date = 0;
    ObDBMSSchedJobInfo calc_job_info;
    calc_job_info.start_date_ = job_info.start_date_;
    calc_job_info.repeat_interval_ = job_attribute_value.get_string();
    calc_job_info.interval_ts_ = 0;
    if (OB_FAIL(check_is_valid_repeat_interval(tenant_id, job_attribute_value.get_string()))) {
      LOG_WARN("invalid repeat_interval", KR(ret), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(calc_dbms_sched_repeat_expr(calc_job_info, next_date))) {
      LOG_WARN("invalid next_date", KR(ret), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(dml.add_column("repeat_interval", job_attribute_value.get_string()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(dml.add_column("`interval#`", "null"))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(dml.add_raw_time_column("next_date", next_date))) {
      LOG_WARN("add next_date column failed", KR(ret), K(next_date));
    } else if (OB_FAIL(dml.add_column("interval_ts", 0))) {
      LOG_WARN("add interval_ts column failed", KR(ret), K(job_info.interval_ts_));
    }
  } else if (0 == job_attribute_name.case_compare("job_action") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_column("job_action", ObHexEscapeSqlStr(job_attribute_value.get_string())))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(dml.add_column("what", ObHexEscapeSqlStr(job_attribute_value.get_string())))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()));
    }
  } else if (0 == job_attribute_name.case_compare("max_run_duration") || 0 == job_attribute_name.case_compare("duration")) {
    const int MAX_RUN_DURATION_LEN = 16;
    char max_run_duration_buf[MAX_RUN_DURATION_LEN];
    int64_t pos = job_attribute_value.get_string().to_string(max_run_duration_buf, MAX_RUN_DURATION_LEN);
    int64_t max_run_duration = atoll(max_run_duration_buf);
    max_run_duration = max_run_duration < 0 ? INT64_MAX : max_run_duration;
    if (OB_FAIL(dml.add_column("max_run_duration", max_run_duration))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()), K(max_run_duration));
    }
  } else if (0 == job_attribute_name.case_compare("start_date") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_raw_time_column("start_date", job_attribute_value.get_time()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_time()));
    } else if (OB_FAIL(dml.add_raw_time_column("next_date", job_attribute_value.get_time()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_time()));
    }
  } else if (0 == job_attribute_name.case_compare("end_date") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_raw_time_column("end_date", job_attribute_value.get_time()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_time()));
    }
  } else if (0 == job_attribute_name.case_compare("comments") && !from_pl_set_attr) {
    if (OB_FAIL(dml.add_column("comments", job_attribute_value.get_string()))) {
      LOG_WARN("add column failed", KR(ret), K(job_attribute_value.get_string()));
    }
  } else if (0 ==  job_attribute_name.case_compare("instance_id")) {
    if (0 != job_attribute_value.get_string().compare("RANDOM") && OB_FAIL(zone_check_impl(tenant_id, job_attribute_value.get_string()))) {
        LOG_WARN("failed to check zone", K(ret), K(job_info), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(dml.add_column("field1", job_attribute_value.get_string()))) {
      LOG_WARN("failed to set zone", K(ret), K(job_info), K(job_attribute_value.get_string()));
    }
  } else if (0 ==  job_attribute_name.case_compare("job_class")) {
    if (0 != job_attribute_value.get_string().case_compare("DEFAULT_JOB_CLASS") && OB_FAIL(job_class_check_impl(tenant_id, job_attribute_value.get_string()))) {
      LOG_WARN("failed to check job_class", K(ret), K(job_info), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(dml.add_column("job_class", job_attribute_value.get_string()))) {
      LOG_WARN("failed to set job_class", K(ret), K(job_info), K(job_attribute_value.get_string()));
    }
  } else if (0 == job_attribute_name.case_compare("max_failures")) {
    int64_t value = 0;
    if (OB_FAIL(get_max_failures_value(tenant_id, job_attribute_value.get_string(), value))) {
      LOG_WARN("failed to get_max_failure_value", K(ret), K(job_info), K(job_attribute_value.get_string()));
    } else if (OB_FAIL(dml.add_column("max_failures", value))) {
      LOG_WARN("failed to set job_class", K(ret), K(job_info), K(job_attribute_value.get_string()), K(value));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not support argument", KR(ret), K(tenant_id));
  }

  if (OB_SUCC(ret)) {
    ObDMLExecHelper exec(sql_client, tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_update(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret));
    } else if (is_zero_row(affected_rows)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("not change", KR(ret), K(affected_rows));
    } else if (!is_double_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be two", KR(ret), K(affected_rows));
    }
  }
  return ret;  
}

int ObDBMSSchedJobUtils::get_dbms_sched_job_info(common::ObISQLClient &sql_client,
                                                 const uint64_t tenant_id,
                                                 const bool is_oracle_tenant, 
                                                 const ObString &job_name,
                                                 common::ObIAllocator &allocator,
                                                 ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || job_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(job_name));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(sql.append_fmt("select * from %s where tenant_id = %ld and job_name = \'%.*s\' and job > 0",
                                                     OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
                                                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                                     job_name.length(), job_name.ptr()))) {
        LOG_WARN("failed to assign sql", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute query failed", K(ret), K(sql));
        } else {
          if (res.get_result() != NULL && OB_SUCCESS == (ret = res.get_result()->next())) {
            ObDBMSSchedTableOperator table_operator;
            OZ (table_operator.extract_info(*(res.get_result()), tenant_id, is_oracle_tenant, allocator, job_info));
          }
          if (OB_FAIL(ret)) {
            if (OB_ITER_END == ret) {
              ret = OB_ENTRY_NOT_EXIST;
            } else {
              LOG_WARN("next failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;  
}

int ObDBMSSchedJobUtils::check_dbms_sched_job_priv(const ObUserInfo *user_info,
                                                   const ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  bool is_oracle_tenant =  lib::is_oracle_mode();
  if (OB_ISNULL(user_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user info is NULL", KR(ret));
  } else if (is_ora_sys_user(user_info->get_user_id()) || is_root_user(user_info->get_user_id())) {
    // do nothing
  } else if (job_info.user_id_ != OB_INVALID_ID) { //如果 job 有 user_id 优先使用
    if (job_info.user_id_ != user_info->get_user_id()) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("job user id check failed", KR(ret), K(user_info), K(job_info.user_id_));
    }
  } else if (is_oracle_tenant) {
    if (0 != job_info.powner_.case_compare(user_info->get_user_name())) { // job 的 owner 和 输入的 user 不一致
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("oracle check job owner failed", KR(ret), K(user_info), K(job_info.user_id_));
    }
  } else {
    if (0 != job_info.powner_.case_compare(user_info->get_user_name())) { // job 保存的 owner 可能是 root@% or root (旧)
      const char *c = job_info.powner_.reverse_find('@');
      if (OB_ISNULL(c)) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("mysql check job owner failed", KR(ret), K(user_info), K(job_info.user_id_));
      } else {
        ObString user = job_info.powner_;
        ObString user_name;
        ObString host_name;
        user_name = user.split_on(c);
        host_name = user;
        if (0 != user_name.case_compare(user_info->get_user_name()) || 0 != host_name.case_compare(user_info->get_host_name())) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("job user id check failed", KR(ret), K(user_info), K(job_info.user_id_));
        }
      }
    }
  }

  return ret;
}

int ObDBMSSchedJobUtils::calc_dbms_sched_repeat_expr(const ObDBMSSchedJobInfo &job_info, int64_t &next_run_time, bool is_first_time)
{
  int ret = OB_SUCCESS;
  int64_t repeat_num = 0;
  int64_t freq_num = 0;
  const int64_t now = ObTimeUtility::current_time();

  //处理旧job
  if (job_info.interval_ts_ > 0) {
    int64_t N = (now - job_info.start_date_) / job_info.interval_ts_;
    next_run_time = job_info.start_date_ + (N + 1) * job_info.interval_ts_;
  } else if (job_info.repeat_interval_.empty() || 0 == job_info.repeat_interval_.case_compare("null")) {
    if (now < job_info.start_date_) { //job 未开始
      next_run_time = job_info.start_date_;
    } else {
      next_run_time = ObDBMSSchedJobInfo::DEFAULT_MAX_END_DATE;
    }
  } else {
    ObDBMSSchedTimeUtil calendar;
    OZ(calendar.calc_repeat_interval_next_date(job_info.repeat_interval_, job_info.start_date_, next_run_time, is_first_time ? job_info.start_date_ : job_info.next_date_, now, is_first_time));
  }
  return ret;
}

int ObDBMSSchedJobUtils::reserve_user_with_minimun_id(ObIArray<const ObUserInfo *> &user_infos)
{
  int ret = OB_SUCCESS;
  if (user_infos.count() > 1) {
    //bug:
    //resver the minimum user id to execute
    const ObUserInfo *minimum_user_info = user_infos.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < user_infos.count(); ++i) {
      if (OB_ISNULL(minimum_user_info) || OB_ISNULL(user_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(minimum_user_info), K(user_infos.at(i)));
      } else if (minimum_user_info->get_user_id() > user_infos.at(i)->get_user_id()) {
        minimum_user_info = user_infos.at(i);
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      user_infos.reset();
      if (OB_FAIL(user_infos.push_back(minimum_user_info))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

} // end for namespace dbms_scheduler
} // end for namespace oceanbase
