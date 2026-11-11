/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_olap_async_job_resolver.h"

#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace dbms_scheduler;
namespace sql
{
ObOLAPAsyncJobResolver::ObOLAPAsyncJobResolver(ObResolverParams &params)
    : ObSelectResolver(params)
{
}

int ObOLAPAsyncJobResolver::resolve(const ParseNode &parse_tree)
{

  int ret = OB_SUCCESS;
  ObItemType stmt_type = parse_tree.type_;
  switch (stmt_type) {
  case T_OLAP_ASYNC_JOB_SUBMIT: {
    ObOLAPAsyncSubmitJobStmt job_stmt;
    OZ (resolve_submit_job_stmt(parse_tree, job_stmt));
    OZ (execute_submit_job(job_stmt));
    OZ (init_select_stmt(job_stmt));
    break;
  }
  case T_OLAP_ASYNC_JOB_CANCEL: {
    ObOLAPAsyncCancelJobStmt *stmt = create_stmt<ObOLAPAsyncCancelJobStmt>();
    OV (OB_NOT_NULL(stmt), OB_ALLOCATE_MEMORY_FAILED);
    OZ (resolve_cancel_job_stmt(parse_tree, stmt));
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stmt type", K(ret), K(stmt_type));
  }
  return ret;
}

int ObOLAPAsyncJobResolver::resolve_submit_job_stmt(const ParseNode &parse_tree, ObOLAPAsyncSubmitJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  int64_t session_query_time_out_ts = 0;
  bool is_async_query = false;
  ObSchemaGetterGuard *schema_guard = NULL;
  const ParseNode *option_list_node = NULL;
  const ParseNode *sql_stmt_node = NULL;
  /* 解析的结构
  parse_tree->T_OLAP_ASYNC_JOB_SUBMIT
  |--[0] T_SUBMIT_JOB_OPTION_LIST (optional)
  |--[1] T_SQL_STMT
    |--[0] [T_SELECT/T_INSERT/T_CREATE_TABLE] user_sql
  */
  if (OB_ISNULL(parse_tree.children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid submit job parse tree", K(ret));
  } else if (1 == parse_tree.num_child_) {
    // Compatibility with parse trees produced before WITH (...) was added.
    sql_stmt_node = parse_tree.children_[0];
  } else if (2 == parse_tree.num_child_) {
    option_list_node = parse_tree.children_[0];
    sql_stmt_node = parse_tree.children_[1];
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid submit job child count", K(ret), K(parse_tree.num_child_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sql_stmt_node) || sql_stmt_node->type_ != T_SQL_STMT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid submit job SQL node", K(ret), KP(sql_stmt_node));
  } else if (sql_stmt_node->num_child_ != 1 || OB_ISNULL(sql_stmt_node->children_[0]) || (
     sql_stmt_node->children_[0]->type_ != T_INSERT &&
     sql_stmt_node->children_[0]->type_ != T_LOAD_DATA &&
     sql_stmt_node->children_[0]->type_ != T_CREATE_TABLE &&
     sql_stmt_node->children_[0]->type_ != T_SELECT)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sql type");
  } else if (FALSE_IT(is_async_query = (T_SELECT == sql_stmt_node->children_[0]->type_))) {
  } else if (OB_FAIL(resolve_submit_job_options(option_list_node, is_async_query, stmt))) {
    LOG_WARN("failed to resolve submit job options", K(ret), K(is_async_query));
  } else if (is_async_query && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async query job requires cluster version later than 4.6.3", KR(ret),
             "min_cluster_version", GET_MIN_CLUSTER_VERSION());
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "SUBMIT JOB SELECT requires a cluster version later than 4.6.3");
  } else if (is_async_query
             && !is_internal_catalog_id(session_info_->get_current_default_catalog())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async query job must be submitted from the internal catalog", KR(ret),
             "catalog_id", session_info_->get_current_default_catalog());
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "SUBMIT JOB SELECT from an external catalog");
  } else if (is_async_query
             && (OB_ISNULL(schema_checker_)
                 || OB_ISNULL(schema_guard = schema_checker_->get_schema_guard())
                 || OB_ISNULL(GCTX.sql_proxy_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async query readiness check is unavailable", KR(ret), KP(schema_checker_),
             KP(schema_guard), KP(GCTX.sql_proxy_));
  } else if (is_async_query
             && OB_FAIL(ObDBMSSchedJobUtils::check_async_query_result_ready(
                    session_info_->get_effective_tenant_id(), *schema_guard, *GCTX.sql_proxy_))) {
    LOG_WARN("async query tenant is not ready before job ID generation", KR(ret),
             "tenant_id", session_info_->get_effective_tenant_id());
  } else if (OB_JOB_SQL_MAX_LENGTH - 1 < parse_tree.str_len_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sql too long", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sql length");
  } else if (OB_INVALID_ID == session_info_->get_database_id()) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("not select database", KR(ret));
  } else if (OB_FAIL(session_info_->get_query_timeout(session_query_time_out_ts))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session query timeout failed", KR(ret));
  } else {
    const int definer_buf_size = OB_MAX_USER_NAME_LENGTH + OB_MAX_HOST_NAME_LENGTH + 2; // @ + \0
    char *definer_buf = static_cast<char*>(allocator_->alloc(definer_buf_size));
    if (OB_ISNULL(definer_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      memset(definer_buf, 0, definer_buf_size);
      snprintf(definer_buf, definer_buf_size, "%.*s@%.*s", session_info_->get_user_name().length(), session_info_->get_user_name().ptr(), 
                                                           session_info_->get_host_name().length(), session_info_->get_host_name().ptr());
      stmt.set_job_definer(definer_buf);
    } 

    if (OB_SUCC(ret)) {
      const uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
      stmt.set_tenant_id(tenant_id);
      stmt.set_user_id(session_info_->get_user_id());
      stmt.set_job_database(session_info_->get_database_name());
      stmt.set_database_id(session_info_->get_database_id());
      stmt.set_query_time_out_second(session_query_time_out_ts / 1000 / 1000);
      stmt.set_async_query(is_async_query);
    }

    if (OB_SUCC(ret)) {
      char *sql_buf = static_cast<char*>(allocator_->alloc(OB_JOB_SQL_MAX_LENGTH));
      if (OB_ISNULL(sql_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        memset(sql_buf, 0, OB_JOB_SQL_MAX_LENGTH);
        snprintf(sql_buf, OB_JOB_SQL_MAX_LENGTH, "%.*s;", (int)parse_tree.str_len_, parse_tree.str_value_);
        stmt.set_job_action(sql_buf);
      }    
    }

    if (OB_SUCC(ret)) {
      char *job_exec_buf = static_cast<char*>(allocator_->alloc(OB_MAX_PROC_ENV_LENGTH));
      if (OB_ISNULL(job_exec_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        memset(job_exec_buf, 0, OB_MAX_PROC_ENV_LENGTH);
        int64_t pos = 0;
        if (OB_FAIL(ObExecEnv::gen_exec_env(*session_info_, job_exec_buf, OB_MAX_PROC_ENV_LENGTH, pos))){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("generate exec env failed", K(ret), K(session_info_));
        } else {
          stmt.set_exec_env((ObString(pos, job_exec_buf))); 
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t job_id = OB_INVALID_ID;
      if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::generate_job_id(stmt.get_tenant_id(), job_id))) {
          LOG_WARN("generate_job_id failed", KR(ret), K(stmt.get_tenant_id()));
      } else {
        stmt.set_job_id(job_id);
        char *job_name_buf = static_cast<char*>(allocator_->alloc(OB_JOB_NAME_MAX_LENGTH));
        if (OB_ISNULL(job_name_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          memset(job_name_buf, 0, OB_JOB_NAME_MAX_LENGTH);
          snprintf(job_name_buf, OB_JOB_NAME_MAX_LENGTH, "%"PRIu64"%"PRIu64,stmt.get_database_id(), stmt.get_job_id());
          stmt.set_job_name(job_name_buf);
        }   
      }
    }
  }
  return ret;
}

int ObOLAPAsyncJobResolver::resolve_submit_job_options(
    const ParseNode *option_list,
    const bool is_async_query,
    ObOLAPAsyncSubmitJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  bool has_hint = false;
  bool has_result_store = false;
  ObAsyncQueryJobConfig async_query_config;
  async_query_config.set_result_store(ObAsyncQueryResultStore::EXTERNAL);
  if (OB_ISNULL(option_list)) {
    // New async queries persist their default below. Keep an empty config only
    // for non-query jobs and for backward compatibility when reading old jobs.
  } else if (OB_UNLIKELY(T_SUBMIT_JOB_OPTION_LIST != option_list->type_
                         || option_list->num_child_ <= 0
                         || OB_ISNULL(option_list->children_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "SUBMIT JOB WITH options");
    LOG_WARN("invalid submit job option list", K(ret), KP(option_list));
  }

  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(option_list)
                          && i < option_list->num_child_; ++i) {
    const ParseNode *option = option_list->children_[i];
    const ParseNode *name = NULL;
    const ParseNode *value = NULL;
    if (OB_ISNULL(option) || T_SUBMIT_JOB_OPTION != option->type_
        || 2 != option->num_child_ || OB_ISNULL(option->children_)
        || OB_ISNULL(name = option->children_[0])
        || OB_ISNULL(value = option->children_[1])
        || T_IDENT != name->type_ || T_VARCHAR != value->type_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "SUBMIT JOB WITH option");
      LOG_WARN("invalid submit job option", K(ret), K(i), KP(option));
    } else if (0 == ObString(name->str_len_, name->str_value_).case_compare("hint")) {
      if (has_hint) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate SUBMIT JOB hint");
        LOG_WARN("duplicate submit job hint", K(ret));
      } else if (!is_async_query) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "SUBMIT JOB hint for non-SELECT SQL");
        LOG_WARN("submit job hint requires SELECT", K(ret));
      } else {
        has_hint = true;
        async_query_config.set_hint_line(
            ObString(value->str_len_, value->str_value_));
        for (int64_t j = 0;
             OB_SUCC(ret) && j < async_query_config.get_hint_line().length(); ++j) {
          const bool is_comment_delimiter =
              j + 1 < async_query_config.get_hint_line().length()
              && (('/' == async_query_config.get_hint_line()[j]
                   && '*' == async_query_config.get_hint_line()[j + 1])
                  || ('*' == async_query_config.get_hint_line()[j]
                      && '/' == async_query_config.get_hint_line()[j + 1]));
          if ('\0' == async_query_config.get_hint_line()[j] || is_comment_delimiter) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "SUBMIT JOB hint");
            LOG_WARN("unsafe submit job hint", K(ret), K(j));
          }
        }
      }
    } else if (0 == ObString(name->str_len_, name->str_value_).case_compare("result_store")) {
      const ObString option_value(value->str_len_, value->str_value_);
      if (has_result_store) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate SUBMIT JOB result_store");
        LOG_WARN("duplicate submit job result_store", K(ret));
      } else if (!is_async_query) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "SUBMIT JOB result_store for non-SELECT SQL");
        LOG_WARN("submit job result_store requires SELECT", K(ret));
      } else if (0 != option_value.case_compare("NATIVE")
                 && 0 != option_value.case_compare("EXTERNAL")) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "SUBMIT JOB result_store must be NATIVE or EXTERNAL");
        LOG_WARN("invalid submit job result_store", K(ret), K(option_value));
      } else {
        has_result_store = true;
        async_query_config.set_result_store(
            0 == option_value.case_compare("EXTERNAL")
                ? ObAsyncQueryResultStore::EXTERNAL
                : ObAsyncQueryResultStore::NATIVE);
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "SUBMIT JOB option");
      LOG_WARN("unsupported submit job option", K(ret),
               "option_name", ObString(name->str_len_, name->str_value_));
    }
  }

  if (OB_SUCC(ret) && is_async_query) {
    ObString job_config;
    if (OB_FAIL(async_query_config.serialize(*allocator_, job_config))) {
      LOG_WARN("failed to build submit job config", K(ret));
    } else {
      stmt.set_job_config(job_config);
    }
  } else if (OB_SUCC(ret)) {
    stmt.set_job_config(ObString());
  }
  return ret;
}
int ObOLAPAsyncJobResolver::resolve_cancel_job_stmt(const ParseNode &parse_tree, ObOLAPAsyncCancelJobStmt *stmt)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
  stmt->set_tenant_id(tenant_id);
  stmt->set_user_id(session_info_->get_user_id());
  if (parse_tree.num_child_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job name", K(ret));
  } else {
    const ParseNode *job_name_node = parse_tree.children_[0];
    if (OB_ISNULL(job_name_node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid job name", K(ret));
    } else {
      char *job_name_buf = static_cast<char*>(allocator_->alloc(OB_JOB_NAME_MAX_LENGTH));
      if (OB_ISNULL(job_name_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        memset(job_name_buf, 0, OB_JOB_NAME_MAX_LENGTH);
        snprintf(job_name_buf, OB_JOB_NAME_MAX_LENGTH, "%.*s", (int)job_name_node->str_len_, job_name_node->str_value_);
        stmt->set_job_name(job_name_buf);
      }    
    }
  }
  return ret;
}

#ifdef ERRSIM
ERRSIM_POINT_DEF(ERRSIM_SUBMIT_ERR_JOB_NAME);
ERRSIM_POINT_DEF(ERRSIM_SUBMIT_ERR_JOB_START_TIME);
#endif

int ObOLAPAsyncJobResolver::execute_submit_job(ObOLAPAsyncSubmitJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == stmt.get_tenant_id()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(stmt.get_tenant_id()));
  } else {
    int64_t start_date_us = ObTimeUtility::current_time();
    int64_t end_date_us = 64060560000000000; // 4000-01-01
    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = stmt.get_tenant_id();
      job_info.user_id_ = stmt.get_user_id();
      job_info.database_id_ = stmt.get_database_id();
      job_info.job_ = stmt.get_job_id();
      job_info.job_name_ = stmt.get_job_name();
      job_info.job_action_ = stmt.get_job_action();
      job_info.lowner_ = stmt.get_job_definer();
      job_info.powner_ = stmt.get_job_definer();
      job_info.cowner_ = stmt.get_job_database();
      job_info.job_style_ = ObString("regular");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("OLAP_ASYNC_JOB_CLASS"); // for compat old version
      job_info.start_date_ = start_date_us;
      job_info.end_date_ = end_date_us;
      job_info.repeat_interval_ = job_info.repeat_interval_;
      job_info.enabled_ = true;
      // RESULT_SCAN resolves its owner and physical result target from this
      // scheduler record. A completed async SELECT is retained and is later
      // removed by result metadata cleanup.
      job_info.auto_drop_ = !stmt.is_async_query();
      job_info.max_run_duration_ = stmt.get_query_time_out_second() + 60;
      job_info.interval_ts_ = 0;
      job_info.exec_env_ = stmt.get_exec_env();
      job_info.job_config_ = stmt.get_job_config();
      job_info.comments_ = stmt.is_async_query() ? ObString("async query job") : ObString("olap async job");
      job_info.func_type_ = stmt.is_async_query()
          ? dbms_scheduler::ObDBMSSchedFuncType::ASYNC_QUERY_JOB
          : dbms_scheduler::ObDBMSSchedFuncType::OLAP_ASYNC_JOB;

      #ifdef ERRSIM
      if (OB_SUCCESS != ERRSIM_SUBMIT_ERR_JOB_NAME) { //注入一个错误的JOB NAME
        job_info.job_name_ = "ERRSIM_JOB";
      }
      if (OB_SUCCESS != ERRSIM_SUBMIT_ERR_JOB_START_TIME) { //注入一个错误的开始时间
        job_info.start_date_ = 64060560000000000;
      }
      #endif

      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(GCTX.sql_proxy_, stmt.get_tenant_id()))) {
        LOG_WARN("failed to start trans", KR(ret), K(stmt.get_tenant_id()));
      } else if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(
          trans, stmt.get_tenant_id(), stmt.get_job_id(), job_info))) {
        LOG_WARN("failed to create dbms scheduler job", KR(ret));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
      if (ret == OB_ERR_PRIMARY_KEY_DUPLICATE) {
        LOG_WARN("job is exist", KR(ret));
      }
    }
  }
  return ret;
}

int ObOLAPAsyncJobResolver::init_select_stmt(ObOLAPAsyncSubmitJobStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = create_stmt<ObSelectStmt>();
  OV (OB_NOT_NULL(select_stmt), OB_ALLOCATE_MEMORY_FAILED);
  if (OB_LIKELY(OB_SUCC(ret))) {
    ObSqlString select_sql;
    if (OB_FAIL(select_sql.assign_fmt(
            "SELECT '%.*s' as job_id", stmt.get_job_name().length(), stmt.get_job_name().ptr()))) {
      LOG_WARN("assign async job name sql string failed", KR(ret), K(stmt.get_job_name()));
    } else if (OB_FAIL(parse_and_resolve_select_sql(select_sql.string()))) {
      LOG_WARN("fail to parse and resolve select sql", K(ret), K(select_sql));
    } else if (OB_UNLIKELY(stmt::T_SELECT != stmt_->get_stmt_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(stmt_->get_stmt_type()));
    } else {
      select_stmt->set_select_type(NOT_AFFECT_FOUND_ROWS);
      if(OB_FAIL(select_stmt->formalize_stmt(session_info_))) {
        LOG_WARN("pull select stmt all expr relation ids failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOLAPAsyncJobResolver::parse_and_resolve_select_sql(const ObString &select_sql)
{
  int ret = OB_SUCCESS;
  // 1. parse and resolve view defination
  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data member is not init", K(ret), K(session_info_), K(params_.allocator_));
  } else {
    ParseResult select_result;
    ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
    if (OB_FAIL(parser.parse(select_sql, select_result))) {
      LOG_WARN("parse select sql failed", K(select_sql), K(ret));
    } else {
      // use alias to make all columns number continued
      if (OB_ISNULL(select_result.result_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is NULL", K(ret));
      } else if (OB_UNLIKELY(select_result.result_tree_->num_child_ != 1
                             || NULL == select_result.result_tree_->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is invalid",
                 K(ret), K(select_result.result_tree_->num_child_), K(select_result.result_tree_->children_));
      } else if (OB_UNLIKELY(NULL == select_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is invalid", K(ret), "child ptr", select_result.result_tree_->children_[0]);
      } else {
        ParseNode *select_stmt_node = select_result.result_tree_->children_[0];
        if (OB_FAIL(ObSelectResolver::resolve(*select_stmt_node))) {
          LOG_WARN("resolve select in view definition failed", K(ret), K(select_stmt_node));
        }
      }
    }
  }
  return ret;
}



} //namespace sql
} //namespace oceanbase
