/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/vector_index/ob_vector_index_sched_job_utils.h"
#include "storage/vector_index/cmd/ob_vector_refresh_index_executor.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/utility/utility.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_parse.h"

namespace oceanbase {
using namespace common;
using namespace dbms_scheduler;
using namespace share;
using namespace share::schema;
using namespace sql;

namespace storage {

int ObVectorIndexSchedJobUtils::add_scheduler_job(
    common::ObISQLClient &sql_client, const uint64_t tenant_id,
    const int64_t job_id, const common::ObString &job_name,
    const common::ObString &job_action, const common::ObObj &start_date,
    const int64_t repeat_interval_ts, const common::ObString &exec_env) {
  int ret = OB_SUCCESS;
  ObSqlString interval_str;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(interval_str.append_fmt("FREQ=SECONDLY; INTERVAL=%ld", repeat_interval_ts / 1000000L))) {
    LOG_WARN("fail to append interval string", K(ret));
  } else {
    int64_t start_date_us = start_date.is_null() ? ObTimeUtility::current_time() + repeat_interval_ts : start_date.get_timestamp();
    int64_t end_date_us = 64060560000000000; // 4000-01-01

    HEAP_VAR(ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = ObString("oceanbase");
      job_info.cowner_ = ObString("oceanbase");
      job_info.powner_ =
          lib::is_oracle_mode() ? ObString("SYS") : ObString("root@%");
      job_info.job_style_ = ObString("regular");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_date_us;
      job_info.end_date_ = end_date_us;
      job_info.repeat_interval_ = interval_str.string();
      job_info.enabled_ = 1;
      job_info.auto_drop_ = 0;
      job_info.max_run_duration_ = 24 * 60 * 60; // set to 1 day
      job_info.exec_env_ = exec_env;
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::VECTOR_INDEX_REFRESH_JOB;
      if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(
              sql_client, tenant_id, job_id, job_info))) {
        LOG_WARN("failed to create dbms scheduler job", KR(ret));
      }
    }
  }
  return ret;
}

// Only successors inherit the budget; each periodic occurrence starts at zero.
int ObVectorIndexSchedJobUtils::get_rebuild_job_retry_count(
    const ObDBMSSchedJobInfo &job, int64_t &retry_cnt)
{
  int ret = OB_SUCCESS;
  retry_cnt = 0;
  if (job.is_once_job()) {
    ObArenaAllocator allocator(ObMemAttr(job.tenant_id_, "VecRetryCnt"));
    ObJsonNode *root = nullptr;
    ObIJsonBase *value = nullptr;
    OZ(ObJsonParser::get_tree(&allocator, job.job_config_, root,
        ObJsonParser::JSN_STRICT_FLAG | ObJsonParser::JSN_UNIQUE_FLAG));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(root) || root->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      OZ(root->get_object_value(ObString::make_string("vector_rebuild_retry_count"), value));
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(value)) {
        ret = OB_INVALID_ARGUMENT;
      } else if (value->json_type() == ObJsonNodeType::J_INT
          && value->get_int() >= 0 && value->get_int() <= ObVectorRefreshIndexExecutor::MAX_REFRESH_RETRY_THRESHOLD) {
        retry_cnt = value->get_int();
      } else if (value->json_type() == ObJsonNodeType::J_UINT
          && value->get_uint() <= ObVectorRefreshIndexExecutor::MAX_REFRESH_RETRY_THRESHOLD) {
        retry_cnt = static_cast<int64_t>(value->get_uint());
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("invalid deferred rebuild retry budget", K(ret), K(job.job_), K(job.job_config_));
    }
  }
  return ret;
}

static int set_rebuild_job_retry_count(ObIAllocator &allocator,
    const int64_t retry_cnt, ObDBMSSchedJobInfo &job)
{
  int ret = OB_SUCCESS;
  ObSqlString patch;
  ObJsonNode *config = nullptr;
  ObJsonNode *update = nullptr;
  const ObString old_config = job.job_config_.empty() ? ObString::make_string("{}") : job.job_config_;
  if (retry_cnt < 0 || retry_cnt > ObVectorRefreshIndexExecutor::MAX_REFRESH_RETRY_THRESHOLD) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    OZ(patch.assign_fmt("{\"vector_rebuild_retry_count\":%ld}", retry_cnt));
    OZ(ObJsonParser::get_tree(&allocator, old_config, config,
        ObJsonParser::JSN_STRICT_FLAG | ObJsonParser::JSN_UNIQUE_FLAG));
    OZ(ObJsonParser::get_tree(&allocator, patch.string(), update));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(config) || OB_ISNULL(update)
        || config->json_type() != ObJsonNodeType::J_OBJECT
        || update->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObJsonObject *object = static_cast<ObJsonObject *>(config);
      ObJsonBuffer buffer(&allocator);
      OZ(object->merge_patch(&allocator, static_cast<ObJsonObject *>(update)));
      OZ(object->print(buffer, false));
      OZ(ob_write_string(allocator, ObString(buffer.length(), buffer.ptr()), job.job_config_));
    }
  }
  return ret;
}

// The deadline belongs to the original periodic occurrence, not to each successor.
static int get_rebuild_defer_deadline(
    const uint64_t index_id, ObDBMSSchedJobInfo &job,
    ObDBMSSchedJobInfo &root_job, const int64_t now, int64_t &deadline)
{
  int ret = OB_SUCCESS;
  ObSqlString root_name;
  ObSqlString prefix;
  deadline = 0;
  OZ(root_name.assign_fmt("%lu_rebuild", index_id));
  OZ(prefix.assign_fmt("%lu_rebuild_defer_%lu_", index_id, root_job.job_));
  if (OB_FAIL(ret)) {
  } else if (index_id == 0 || index_id == OB_INVALID_ID
      || job.tenant_id_ == OB_INVALID_TENANT_ID || job.tenant_id_ == OB_INVALID_ID
      || job.tenant_id_ != root_job.tenant_id_
      || job.get_func_type() != ObDBMSSchedFuncType::VECTOR_INDEX_REFRESH_JOB
      || root_job.get_func_type() != ObDBMSSchedFuncType::VECTOR_INDEX_REFRESH_JOB
      || root_job.job_name_ != root_name.string() || root_job.is_once_job()
      || job.job_ == OB_INVALID_ID || job.job_ == 0 || job.this_date_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid automatic rebuild job", K(ret), K(job), K(root_job));
  } else if (root_job.is_disabled() || root_job.is_broken() || root_job.is_killed()
      || root_job.is_completed()) {
    ret = OB_CANCELED;
  } else if (job.is_once_job()) {
    if (!job.job_name_.prefix_match(prefix.ptr())) {
      // A deleted and recreated periodic job must not inherit its predecessor's chain.
      ret = OB_CANCELED;
    } else {
      deadline = min(job.end_date_, min(root_job.end_date_, root_job.next_date_));
    }
  } else if (job.job_name_ != root_name.string() || job.job_ != root_job.job_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    deadline = min(job.end_date_, min(root_job.end_date_, root_job.next_date_));
    const int64_t duration = job.get_max_run_duration();
    if (duration < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (duration <= (INT64_MAX - job.this_date_) / 1000000L) {
      deadline = min(deadline, job.this_date_ + duration * 1000000L);
    }
  }
  if (OB_SUCC(ret) && now >= deadline) {
    ret = OB_TIMEOUT;
    LOG_WARN("automatic rebuild deferral window expired", K(ret), K(deadline), K(job));
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::build_rebuild_defer_job(
    ObIAllocator &allocator, const uint64_t index_id, ObDBMSSchedJobInfo &job,
    ObDBMSSchedJobInfo &root_job, const int64_t now, const int64_t retry_cnt,
    ObDBMSSchedJobInfo &next_job)
{
  int ret = OB_SUCCESS;
  int64_t deadline = 0;
  ObSqlString next_name;
  OZ(get_rebuild_defer_deadline(index_id, job, root_job, now, deadline));
  // Same occurrence always creates the same successor, including after an ambiguous write result.
  OZ(next_name.assign_fmt("%lu_rebuild_defer_%lu_%lu_%ld",
                         index_id, root_job.job_, job.job_, job.this_date_));
  OZ(next_job.deep_copy(allocator, job));
  OZ(set_rebuild_job_retry_count(allocator, retry_cnt, next_job));
  OZ(ob_write_string(allocator, next_name.string(), next_job.job_name_));
  if (OB_SUCC(ret)) {
    // extract_info does not restore job_type; all vector jobs execute a PLSQL block.
    next_job.job_type_ = ObString::make_string("PLSQL_BLOCK");
    next_job.job_ = OB_INVALID_ID;
    next_job.start_date_ = now + REBUILD_DEFER_INTERVAL_US;
    next_job.next_date_ = next_job.start_date_;
    next_job.end_date_ = deadline;
    next_job.repeat_interval_.reset();
    next_job.interval_.reset();
    next_job.interval_ts_ = 0;
    next_job.auto_drop_ = true;
    next_job.enabled_ = 1;
    next_job.this_date_ = 0;
    next_job.this_exec_date_ = 0;
    next_job.failures_ = 0;
    next_job.state_.reset();
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::check_and_defer_rebuild_job(
    ObISQLClient &sql_client, const uint64_t index_id, const uint64_t table_id,
    ObDBMSSchedJobInfo &job, const int64_t retry_cnt, bool &deferred)
{
  int ret = OB_SUCCESS;
  deferred = false;
  ObArenaAllocator allocator(ObMemAttr(job.tenant_id_, "VecDeferJob"));
  ObDBMSSchedJobInfo root_job;
  ObSqlString root_name;
  bool has_conflict = false;
  int64_t deadline = 0;
  OZ(root_name.assign_fmt("%lu_rebuild", index_id));
  OZ(ObDBMSSchedJobUtils::get_dbms_sched_job_info(
      sql_client, job.tenant_id_, lib::is_oracle_mode(), root_name.string(), allocator, root_job));
  OZ(get_rebuild_defer_deadline(index_id, job, root_job,
                               ObTimeUtility::current_time(), deadline));
  OZ(rootserver::ObDDLTaskRecordOperator::check_vec_index_ddl_conflict(
      sql_client, job.tenant_id_, table_id, has_conflict));
  if (OB_SUCC(ret) && ObTimeUtility::current_time() >= deadline) {
    ret = OB_TIMEOUT;
  }
  if (OB_SUCC(ret) && has_conflict) {
    ObDBMSSchedJobInfo next_job;
    int64_t next_job_id = OB_INVALID_ID;
    OZ(build_rebuild_defer_job(allocator, index_id, job, root_job,
                              ObTimeUtility::current_time(), retry_cnt, next_job));
    OZ(ObMViewSchedJobUtils::generate_job_id(job.tenant_id_, next_job_id));
    next_job.job_ = next_job_id;
    next_job.start_date_ = ObTimeUtility::current_time() + REBUILD_DEFER_INTERVAL_US;
    next_job.next_date_ = next_job.start_date_;
    if (OB_FAIL(ret)) {
      LOG_WARN("generate deferred rebuild job id failed", K(ret));
    } else if (next_job.start_date_ >= next_job.end_date_) {
      ret = OB_TIMEOUT;
      LOG_WARN("no time left for another rebuild deferral", K(ret), K(job));

    } else if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(
                   sql_client, job.tenant_id_, next_job_id, next_job))) {
      // A duplicate or timed-out INSERT may already have persisted the successor.
      const int create_ret = ret;
      ObDBMSSchedJobInfo existing_job;
      int lookup_ret = ObDBMSSchedJobUtils::get_dbms_sched_job_info(
          sql_client, job.tenant_id_, lib::is_oracle_mode(), next_job.job_name_, allocator, existing_job);
      if (OB_SUCCESS == lookup_ret && existing_job.job_ != OB_INVALID_ID
          && existing_job.get_func_type() == next_job.get_func_type()
          && existing_job.is_once_job() && existing_job.auto_drop_
          && !existing_job.is_disabled() && !existing_job.is_broken() && !existing_job.is_killed()
          && !existing_job.is_completed() && existing_job.end_date_ == next_job.end_date_
          && existing_job.job_action_ == next_job.job_action_
          && existing_job.exec_env_ == next_job.exec_env_
          && existing_job.job_config_ == next_job.job_config_) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("create deferred rebuild job failed", K(create_ret), K(lookup_ret), K(job));
      }
    }
    if (OB_SUCC(ret)) {
      deferred = true;
      LOG_INFO("automatic rebuild handed off after DDL conflict", K(job.job_),
               K(next_job.job_name_), K(next_job.start_date_), K(next_job.end_date_));
    }
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::add_vector_index_refresh_job(
    common::ObISQLClient &sql_client, const uint64_t tenant_id,
    const uint64_t vidx_table_id, const common::ObString &exec_env) {
  LOG_INFO("################## [add_vector_index_refresh_job]", K(tenant_id), K(vidx_table_id), K(exec_env));
  int ret = OB_SUCCESS;
  int64_t job_id = OB_INVALID_ID;
  common::ObObj start_date;
  start_date.set_null();
  if (OB_FAIL(ObMViewSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("failed to generate vector index refresh job id", K(ret));
  } else {
    ObSqlString job_action;
    ObSqlString refresh_job_name;
    if (OB_FAIL(job_action.assign_fmt(
            "DBMS_VECTOR.refresh_index_inner(%lu, %lu)",
            vidx_table_id,
            ObVectorIndexSchedJobUtils::DEFAULT_REFRESH_TRIGGER_THRESHOLD))) {
      LOG_WARN("failed to generate refresh index job id", K(ret));
    } else if (OB_FAIL(refresh_job_name.assign_fmt("%lu_refresh", vidx_table_id))) {
      LOG_WARN("failed to generate refresh job name", K(ret));
    } else if (OB_FAIL(ObVectorIndexSchedJobUtils::add_scheduler_job(
                   sql_client, tenant_id, job_id, refresh_job_name.string(),
                   job_action.string(), start_date,
                   ObVectorIndexSchedJobUtils::DEFAULT_REFRESH_INTERVAL_TS,
                   exec_env))) {
      LOG_WARN("failed to add refresh index job", K(ret), K(vidx_table_id),
               K(job_action), K(exec_env));
    } else {
      LOG_INFO("succeed to add refresh index job", K(ret), K(vidx_table_id),
               K(job_action), K(exec_env));
    }
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::remove_vector_index_refresh_job(
    common::ObISQLClient &sql_client, const uint64_t tenant_id,
    const uint64_t vidx_table_id) {
  LOG_INFO("################## [remove_vector_index_refresh_job]", K(vidx_table_id));
  int ret = OB_SUCCESS;
  ObSqlString refresh_job_name;
  if (OB_FAIL(refresh_job_name.assign_fmt("%lu_refresh", vidx_table_id))) {
    LOG_WARN("failed to generate refresh job name", K(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
        sql_client, tenant_id, refresh_job_name.string(), true))) {
    LOG_WARN("failed to remove vector index refresh job",
        KR(ret), K(tenant_id), K(vidx_table_id));
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::add_vector_index_rebuild_job(common::ObISQLClient &sql_client,
                                                             const uint64_t tenant_id,
                                                             const uint64_t vidx_table_id,
                                                             const common::ObString &exec_env,
                                                             const uint64_t paralellism)
{
  LOG_INFO("################## [add_vector_index_rebuild_job]", K(tenant_id), K(vidx_table_id), K(exec_env), K(paralellism));
  int ret = OB_SUCCESS;
  int64_t job_id = OB_INVALID_ID;
  common::ObObj start_date;
  start_date.set_null();
  if (OB_FAIL(ObMViewSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("failed to generate vector index refresh job id", K(ret));
  } else {
    ObSqlString job_action;
    ObSqlString rebuild_job_name;
    const uint64_t real_paralellism = paralellism == 0 ? 1 : paralellism; // make sure default 1
    if (OB_FAIL(job_action.assign_fmt(
            "DBMS_VECTOR.rebuild_index_inner(%lu, %lf, \"\", \"\", \"\", %lu)",
            vidx_table_id,
            ObVectorIndexSchedJobUtils::DEFAULT_REBUILD_TRIGGER_THRESHOLD,
            real_paralellism))) {
      LOG_WARN("failed to generate rebuild index job id", K(ret));
    } else if (OB_FAIL(rebuild_job_name.assign_fmt("%lu_rebuild", vidx_table_id))) {
      LOG_WARN("failed to generate rebuild job name", K(ret));
    } else if (OB_FAIL(ObVectorIndexSchedJobUtils::add_scheduler_job(
                   sql_client, tenant_id, job_id, rebuild_job_name.string(),
                   job_action.string(), start_date,
                   ObVectorIndexSchedJobUtils::DEFAULT_REBUILD_INTERVAL_TS,
                   exec_env))) {
      LOG_WARN("failed to add rebuild index job", K(ret), K(vidx_table_id),
               K(job_action), K(exec_env));
    } else {
      LOG_INFO("succeed to add rebuild index job", K(ret), K(vidx_table_id),
               K(job_action), K(exec_env));
    }
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::remove_vector_index_rebuild_job(common::ObISQLClient &sql_client,
                                                                const uint64_t tenant_id,
                                                                const uint64_t vidx_table_id)
{
  LOG_INFO("################## [remove_vector_index_rebuild_job]", K(vidx_table_id));
  int ret = OB_SUCCESS;
  ObSqlString rebuild_job_name;
  if (OB_FAIL(rebuild_job_name.assign_fmt("%lu_rebuild", vidx_table_id))) {
    LOG_WARN("failed to generate refresh job name", K(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
        sql_client, tenant_id, rebuild_job_name.string(), true))) {
    LOG_WARN("failed to remove vector index rebuild job",
        KR(ret), K(tenant_id), K(vidx_table_id));
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::get_vector_index_job_info(common::ObISQLClient &sql_client,
                                                          const uint64_t tenant_id,
                                                          const uint64_t vidx_table_id,
                                                          common::ObIAllocator &allocator,
                                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                                          dbms_scheduler::ObDBMSSchedJobInfo &job_info,
                                                          const bool is_get_rebuild)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  ObSqlString rebuild_job_name;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant schema is null", K(tenant_id)); // skip
  } else if (is_get_rebuild && OB_FAIL(rebuild_job_name.assign_fmt("%lu_rebuild", vidx_table_id))) {
    LOG_WARN("failed to generate refresh job name", K(ret));
  } else if (!is_get_rebuild && OB_FAIL(rebuild_job_name.assign_fmt("%lu_refresh", vidx_table_id))) {
    LOG_WARN("failed to generate refresh job name", K(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::get_dbms_sched_job_info(sql_client, tenant_id, 
                                                                  tenant_schema->is_oracle_tenant(),
                                                                  rebuild_job_name.string(), 
                                                                  allocator,
                                                                  job_info))) {
    LOG_WARN("fail to get dbms schedule info", K(ret), K(tenant_id), K(rebuild_job_name));
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase