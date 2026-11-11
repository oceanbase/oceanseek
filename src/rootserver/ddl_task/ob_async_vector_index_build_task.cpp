/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "rootserver/ddl_task/ob_async_vector_index_build_task.h"
#include "rootserver/ddl_task/ob_drop_async_vector_index_task.h"
#include "rootserver/ddl_task/ob_sys_ddl_util.h" // for ObSysDDLSchedulerUtil

#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_service_launcher.h" // for ObDDLServiceLauncher
#include "rootserver/ob_root_service.h"
#include "share/ob_async_index_refresh_util.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_index_builder_util.h"
#include "storage/async_index/ob_async_index_define.h"
#include "share/ob_ddl_sim_point.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/ddl/ob_ddl_lock.h"

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace rootserver
{

ObAsyncVectorIndexBuildTask::ObAsyncVectorIndexBuildTask()
  : ObDDLTask(DDL_CREATE_ASYNC_VEC_IVF_INDEX),
    create_index_arg_(),
    shared_async_delete_table_id_(OB_INVALID_ID),
    baseline_task_submitted_(false),
    baseline_task_id_(0),
    drop_task_submitted_(false),
    drop_task_id_(0),
    is_retryable_ddl_(true),
    existing_async_index_table_ids_(),
    snapshot_holder_schema_version_(0),
    async_index_publish_refresh_target_()
{
}

ObAsyncVectorIndexBuildTask::~ObAsyncVectorIndexBuildTask()
{
}

int ObAsyncVectorIndexBuildTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    const int64_t schema_version,
    const int64_t parallelism,
    const int64_t parent_task_id,
    const int64_t consumer_group_id,
    const int32_t sub_task_trace_id,
    const ObCreateIndexArg &create_index_arg,
    const uint64_t tenant_data_version,
    const int64_t snapshot_version,
    const bool is_retryable_ddl)
{
  int ret = OB_SUCCESS;
  const bool defer_snapshot =
      ObDDLUtil::should_defer_async_index_snapshot(create_index_arg, tenant_data_version);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task is initialized twice", K(ret));
  } else if (OB_ISNULL(data_table_schema)
             || OB_ISNULL(index_schema)
             || OB_INVALID_TENANT_ID == tenant_id
             || task_id <= 0
             || schema_version <= 0
             || parallelism <= 0
             || parent_task_id < 0
             || (create_index_arg.is_offline_rebuild_ && parent_task_id <= 0)
             || consumer_group_id < 0
             || tenant_data_version <= 0
             || snapshot_version < 0
             || (0 == snapshot_version && !defer_snapshot)
             || !create_index_arg.is_valid()
             || index_schema->get_schema_version() <= 0
             || !is_async_ivf_index(index_schema->get_index_type())
             || !is_async_ivf_index(create_index_arg.index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async vector build arguments", K(ret), K(tenant_id),
        K(task_id), K(schema_version), K(snapshot_version),
        K(tenant_data_version), KPC(data_table_schema), KPC(index_schema),
        K(create_index_arg));
  } else if (OB_FAIL(ObDDLTask::deep_copy_table_arg(
                 allocator_, create_index_arg, create_index_arg_))) {
    LOG_WARN("fail to copy create index arg", K(ret), K(create_index_arg));
  } else {
    task_type_ = DDL_CREATE_ASYNC_VEC_IVF_INDEX;
    task_version_ = OB_ASYNC_VECTOR_INDEX_BUILD_TASK_VERSION;
    task_status_ = ObDDLTaskStatus::PREPARE;
    tenant_id_ = tenant_id;
    object_id_ = data_table_schema->get_table_id();
    target_object_id_ = index_schema->get_table_id();
    schema_version_ = schema_version;
    snapshot_version_ = snapshot_version;
    snapshot_holder_schema_version_ = index_schema->get_schema_version();
    task_id_ = task_id;
    parent_task_id_ = parent_task_id;
    parallelism_ = parallelism;
    consumer_group_id_ = consumer_group_id;
    sub_task_trace_id_ = sub_task_trace_id;
    data_format_version_ = tenant_data_version;
    is_retryable_ddl_ = is_retryable_ddl;
    dst_tenant_id_ = tenant_id;
    dst_schema_version_ = schema_version;
    create_index_arg_.exec_tenant_id_ = tenant_id;
    create_index_arg_.parallelism_ = parallelism_;
    create_index_arg_.consumer_group_id_ = consumer_group_id_;
    start_time_ = ObTimeUtility::current_time();
    set_gmt_create(ObTimeUtility::current_time());
    if (OB_FAIL(init_ddl_task_monitor_info(target_object_id_))) {
      LOG_WARN("fail to initialize DDL monitor info", K(ret), K(target_object_id_));
    } else {
      is_inited_ = true;
      ddl_tracing_.open();
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task is initialized twice", K(ret));
  } else if (OB_UNLIKELY(!task_record.is_valid()
             || DDL_CREATE_ASYNC_VEC_IVF_INDEX != task_record.ddl_type_
             || !is_supported_task_version(task_record.task_version_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async vector build task record", K(ret), K(task_record));
  } else {
    task_type_ = task_record.ddl_type_;
    task_version_ = task_record.task_version_;
    tenant_id_ = task_record.tenant_id_;
    object_id_ = task_record.object_id_;
    target_object_id_ = task_record.target_object_id_;
    schema_version_ = task_record.schema_version_;
    snapshot_version_ = task_record.snapshot_version_;
    execution_id_ = task_record.execution_id_;
    task_status_ = static_cast<ObDDLTaskStatus>(task_record.task_status_);
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    ret_code_ = task_record.ret_code_;
    start_time_ = ObTimeUtility::current_time();
    consensus_schema_version_ = task_record.consensus_schema_version_;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    if (OB_ISNULL(task_record.message_.ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task record message is null", K(ret), K(task_record));
    } else if (OB_FAIL(deserialize_params_from_message(
                   tenant_id_, task_record.message_.ptr(),
                   task_record.message_.length(), pos))) {
      LOG_WARN("fail to deserialize async vector build task", K(ret), K(task_record));
    } else if (OB_FAIL(init_ddl_task_monitor_info(target_object_id_))) {
      LOG_WARN("fail to initialize DDL monitor info", K(ret), K(target_object_id_));
    } else {
      is_inited_ = true;
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not initialized", K(ret));
  } else if (!need_retry()) {
  } else if (OB_FAIL(check_health())) {
    LOG_WARN("fail to check async vector build health", K(ret), K(*this));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch (static_cast<ObDDLTaskStatus>(task_status_)) {
      case ObDDLTaskStatus::PREPARE:
        ret = prepare();
        break;
      case ObDDLTaskStatus::GENERATE_ASYNC_VECTOR_INDEX_BASELINE:
        ret = create_baseline_task();
        break;
      case ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE:
        ret = wait_baseline_task_finish();
        break;
      case ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH:
        ret = trigger_async_index_refresh();
        break;
      case ObDDLTaskStatus::TAKE_EFFECT:
        ret = wait_schema_refresh_and_trans_end();
        break;
      case ObDDLTaskStatus::FAIL:
        ret = clean_on_failed();
        break;
      case ObDDLTaskStatus::SUCCESS:
        ret = succ();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected async vector build status", K(ret), K(task_status_));
        break;
    }
    ddl_tracing_.release_span_hierarchy();
    if (OB_FAIL(ret)) {
      ret = handle_process_error(ret);
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::collect_longops_stat(ObLongopsValue &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
  const char *status_name = nullptr;
  switch (status) {
    case ObDDLTaskStatus::PREPARE:
    case ObDDLTaskStatus::GENERATE_ASYNC_VECTOR_INDEX_BASELINE:
    case ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE:
    case ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH:
    case ObDDLTaskStatus::TAKE_EFFECT:
    case ObDDLTaskStatus::FAIL:
    case ObDDLTaskStatus::SUCCESS:
      status_name = ddl_task_status_to_str(status);
      break;
    default:
      break;
  }
  if (OB_ISNULL(status_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected async vector build status", K(ret), K(task_status_));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, DDL_TASK_COLLECT_LONGOPS_STAT_FAILED))) {
    LOG_WARN("ddl sim failure", K(ret), K(tenant_id_), K(task_id_));
  } else if (OB_FAIL(copy_longops_stat(value))) {
    LOG_WARN("failed to copy async vector build longops stat", K(ret));
  } else if (OB_FAIL(databuff_printf(value.message_, MAX_LONG_OPS_MESSAGE_LENGTH, pos,
      "TENANT_ID: %lu, TASK_ID: %ld, STATUS: %s, BASELINE_TASK_ID: %ld",
      tenant_id_, task_id_, status_name, baseline_task_id_))) {
    LOG_WARN("failed to print async vector build message", K(ret));
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::handle_process_error(const int process_ret)
{
  int ret = process_ret;
  if (ObIDDLTask::in_ddl_retry_white_list(process_ret)) {
    LOG_WARN("async vector build step will retry", K(process_ret), K(task_status_));
    ret = OB_SUCCESS;
  } else if (ObDDLTaskStatus::FAIL == task_status_
             || ObDDLTaskStatus::SUCCESS == task_status_) {
  } else {
    const int switch_ret = switch_status(ObDDLTaskStatus::FAIL, false, process_ret);
    if (OB_SUCCESS != switch_ret) {
      ret = switch_ret;
      LOG_WARN("fail to switch async vector build to FAIL", K(ret), K(process_ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::check_health()
{
  int ret = OB_SUCCESS;
  if (!ObDDLServiceLauncher::is_ddl_service_started()) {
    ret = OB_STATE_NOT_MATCH;
    need_retry_ = false;
    LOG_WARN("DDL service is not started", K(ret));
  } else if (OB_FAIL(check_and_refresh_status_if_rs_epoch_changed())) {
    LOG_WARN("fail to refresh task status after RS epoch change", K(ret));
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("fail to refresh schema version", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = nullptr;
    bool data_table_exist = false;
    bool index_table_exist = false;
    const bool need_check_target =
        ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE != task_status_
        && ObDDLTaskStatus::FAIL != task_status_;
    // Persisted v1 tasks can carry a data-table version older than index creation.
    // Wait for the original index dependency without changing the task key or snapshot holder.
    if (need_check_target && OB_FAIL(ObDDLUtil::check_schema_version_refreshed(
                                tenant_id_, snapshot_holder_schema_version_))) {
      LOG_WARN("async vector index schema version is not refreshed", K(ret), K(tenant_id_),
          K(task_id_), K(snapshot_holder_schema_version_));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(
                   tenant_id_, object_id_, data_table_exist))) {
      LOG_WARN("fail to check data table", K(ret), K(object_id_));
    } else if (!data_table_exist) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table does not exist", K(ret), K(object_id_));
    } else if (need_check_target && OB_FAIL(schema_guard.check_table_exist(
                   tenant_id_, target_object_id_, index_table_exist))) {
      LOG_WARN("fail to check async vector index", K(ret), K(target_object_id_));
    } else if (need_check_target && !index_table_exist) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("async vector index does not exist", K(ret), K(target_object_id_));
    } else if (need_check_target && OB_FAIL(schema_guard.get_table_schema(
                   tenant_id_, target_object_id_, index_schema))) {
      LOG_WARN("fail to get async vector index schema", K(ret), K(target_object_id_));
    } else if (need_check_target && OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("async vector index schema is null", K(ret), K(target_object_id_));
    } else if (need_check_target
               && INDEX_STATUS_INDEX_ERROR == index_schema->get_index_status()) {
      ret = OB_SUCCESS == ret_code_ ? OB_ERR_ADD_INDEX : ret_code_;
      LOG_WARN("async vector index is in error status", K(ret), KPC(index_schema));
    }
  }
  if (OB_FAIL(ret) && !ObIDDLTask::in_ddl_retry_white_list(ret)
      && ObDDLTaskStatus::FAIL != task_status_
      && ObDDLTaskStatus::SUCCESS != task_status_) {
    const int switch_ret = switch_status(ObDDLTaskStatus::FAIL, false, ret);
    if (OB_SUCCESS != switch_ret) {
      ret = switch_ret;
      LOG_WARN("fail to switch unhealthy async vector build to FAIL", K(ret));
    }
  }
  if (ObDDLTaskStatus::FAIL == task_status_
      || ObDDLTaskStatus::SUCCESS == task_status_) {
    ret = OB_SUCCESS;
  }
  check_ddl_task_execute_too_long();
  return ret;
}

// Persist the index build snapshot and retain the historical data it needs in one transaction.
// Use the persisted holder key even if metadata DDL has advanced the index schema version.
// Lock the DDL task record and reuse any saved snapshot so retries and recovery read the same data version.
int ObAsyncVectorIndexBuildTask::persist_and_hold_async_index_snapshot(
    const ObTableSchema &data_table_schema,
    const ObTableSchema &index_table_schema,
    const int64_t new_fetched_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t task_status = 0;
  int64_t execution_id = -1;
  int64_t ret_code = OB_SUCCESS;
  int64_t persisted_snapshot = 0;
  ObMySQLTransaction trans;
  if (OB_UNLIKELY(new_fetched_snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index snapshot", K(ret), K(new_fetched_snapshot), K(*this));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(
      trans, tenant_id_, task_id_, task_status, execution_id, ret_code, persisted_snapshot))) {
    LOG_WARN("fail to lock async index task record", K(ret), K(tenant_id_), K(task_id_));
  } else if (ObDDLTaskStatus::PREPARE != static_cast<ObDDLTaskStatus>(task_status)) {
    ret = OB_EAGAIN;
    LOG_WARN("async index task is no longer in prepare", K(ret), K(task_id_), K(task_status));
  } else if (persisted_snapshot > 0) {
    // Reuse the saved and retained snapshot so retries keep reading the same data version.
    DEBUG_SYNC(ASYNC_INDEX_SNAPSHOT_REUSED);
  } else if (OB_FAIL(ObDDLUtil::hold_snapshot(
      trans, data_table_schema, index_table_schema, new_fetched_snapshot,
      snapshot_holder_schema_version_))) {
    LOG_WARN("fail to hold async index snapshot", K(ret), K(new_fetched_snapshot), K(*this));
  } else if (OB_FAIL(DDL_SIM(tenant_id_, task_id_, ASYNC_INDEX_SNAPSHOT_HOLD_DISCARDED))) {
    LOG_WARN("injected async index snapshot retention failure", K(ret), K(task_id_));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::update_snapshot_version(
      trans, tenant_id_, task_id_, new_fetched_snapshot))) {
    LOG_WARN("fail to persist async index snapshot", K(ret), K(new_fetched_snapshot), K(task_id_));
  }

  if (trans.is_started()) {
    const bool need_commit = OB_SUCC(ret);
    if (need_commit) {
      DEBUG_SYNC(ASYNC_INDEX_SNAPSHOT_BEFORE_COMMIT);
    }
    int tmp_ret = trans.end(need_commit);
    if (need_commit && OB_SUCCESS == tmp_ret) {
      // Simulate a lost response only after the snapshot transaction really committed.
      tmp_ret = DDL_SIM(tenant_id_, task_id_, ASYNC_INDEX_SNAPSHOT_COMMIT_RESPONSE_LOST);
    }
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to end transaction", K(ret), K(tmp_ret), K(need_commit));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      DEBUG_SYNC(ASYNC_INDEX_SNAPSHOT_COMMIT_ERROR);
    } else if (need_commit) {
      snapshot_version_ = persisted_snapshot > 0 ? persisted_snapshot : new_fetched_snapshot;
    }
  }
  return ret;
}

// After write transactions using the old table schema end, select, retain, and save the snapshot for index build.
// Each call checks once; success with snapshot_ready=false tells the caller to retry in a later scheduling round.
int ObAsyncVectorIndexBuildTask::prepare_async_index_snapshot(bool &snapshot_ready)
{
  int ret = OB_SUCCESS;
  snapshot_ready = snapshot_version_ > 0;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *index_table_schema = nullptr;
  bool index_attached_to_data_table = false;
  int64_t new_fetched_snapshot = 0;
  bool is_trans_end = false;
  if (snapshot_ready) {
    // The build snapshot and its data retention record already exist; no further waiting or selection is needed.
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service is null", K(ret));
  } else if (OB_FAIL(GCTX.root_service_->get_schema_service().get_tenant_schema_guard(
      tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      tenant_id_, target_object_id_, index_table_schema))) {
    LOG_WARN("fail to get index table schema", K(ret), K(tenant_id_), K(target_object_id_));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(index_table_schema)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("async index schema is not ready", K(ret), K(object_id_), K(target_object_id_));
  } else {
    const ObIArray<ObAuxTableMetaInfo> &index_infos = data_table_schema->get_simple_index_infos();
    for (int64_t i = 0; !index_attached_to_data_table && i < index_infos.count(); ++i) {
      index_attached_to_data_table = index_infos.at(i).table_id_ == target_object_id_;
    }
    if (!index_attached_to_data_table) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("data table schema does not contain async index yet", K(ret),
          K(object_id_), K(target_object_id_), "data_schema_version", data_table_schema->get_schema_version());
    } else if (!wait_trans_ctx_.is_inited()
               && OB_FAIL(wait_trans_ctx_.init(
                   tenant_id_,
                   task_id_,
                   task_status_,
                   object_id_,
                   ObDDLWaitTransEndCtx::WAIT_SCHEMA_TRANS,
                   data_table_schema->get_schema_version()))) {
      LOG_WARN("fail to init async index wait trans context", K(ret),
          K(object_id_), "data_schema_version", data_table_schema->get_schema_version());
    } else if (OB_FAIL(wait_trans_ctx_.try_wait(
        is_trans_end, new_fetched_snapshot, true /* need_wait_trans_end */))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to wait old schema transactions for async index", K(ret), K(object_id_));
      }
    } else if (!is_trans_end) {
      // Old-schema transactions are still active. Report not ready and check again on the next scheduling round.
      if (wait_trans_ctx_.get_pending_tx_id().is_valid()) {
        DEBUG_SYNC(ASYNC_INDEX_OLD_SCHEMA_TRANS_PENDING);
      }
    } else if (OB_UNLIKELY(new_fetched_snapshot <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid snapshot after waiting old schema transactions",
          K(ret), K(new_fetched_snapshot), K(object_id_));
    } else if (OB_FAIL(persist_and_hold_async_index_snapshot(
        *data_table_schema, *index_table_schema, new_fetched_snapshot))) {
      if (OB_SNAPSHOT_DISCARDED == ret) {
        wait_trans_ctx_.reset();
        ret = OB_SUCCESS;
        DEBUG_SYNC(ASYNC_INDEX_SNAPSHOT_WAIT_RESET);
      } else {
        LOG_WARN("fail to persist and hold async index snapshot",
            K(ret), K(new_fetched_snapshot), K(*this));
      }
    } else {
      snapshot_ready = true;
      wait_trans_ctx_.reset();
      LOG_INFO("async index build snapshot is ready",
          K(snapshot_version_), K(object_id_), K(target_object_id_));
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::prepare()
{
  int ret = OB_SUCCESS;
  if (ObDDLTaskStatus::PREPARE != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status does not match PREPARE", K(ret), K(task_status_));
  } else {
    bool snapshot_ready = snapshot_version_ > 0;
    if (uses_parent_publish_protocol()
        && OB_FAIL(prepare_async_index_snapshot(snapshot_ready))) {
      LOG_WARN("fail to prepare async vector build snapshot", K(ret), K(*this));
    } else if (!snapshot_ready) {
      if (!uses_parent_publish_protocol()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("baseline snapshot is invalid", K(ret), K(snapshot_version_));
      } else if (OB_FAIL(refresh_status())) {
        LOG_WARN("fail to refresh async vector task status while waiting for snapshot", K(ret), K(*this));
      }
    }
    if (OB_SUCC(ret) && snapshot_ready) {
      DEBUG_SYNC(ASYNC_INDEX_SNAPSHOT_READY);
      existing_async_index_table_ids_.reset();
      if (OB_FAIL(ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
              tenant_id_, object_id_, existing_async_index_table_ids_))) {
        LOG_WARN("fail to collect existing async index ids", K(ret), K(object_id_));
      } else if (OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("SQL proxy is null", K(ret));
      // Persist the membership before advancing the status. If RS restarts
      // between the two writes, PREPARE is recovered and recollects it.
      } else if (OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
        LOG_WARN("fail to persist existing async index ids", K(ret),
            K(existing_async_index_table_ids_));
      } else if (OB_FAIL(switch_status(
                     ObDDLTaskStatus::GENERATE_ASYNC_VECTOR_INDEX_BASELINE,
                     true, OB_SUCCESS))) {
        LOG_WARN("fail to switch to baseline generation", K(ret));
      }
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::construct_async_delete_arg(
    ObIAllocator &allocator,
    ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLTask::deep_copy_table_arg(allocator, create_index_arg_, arg))) {
    LOG_WARN("fail to copy create index arg", K(ret));
  } else if (OB_FAIL(ObIndexBuilderUtil::normalize_async_delete_index_arg(
                 arg, &allocator))) {
    LOG_WARN("fail to normalize async delete arg", K(ret));
  } else {
    // The offline-redefinition child carries the pre-built baseline schema in
    // create_index_arg_. The shared async-delete index is a different index,
    // so none of that target-specific rebuild state may reach create_aux_index.
    arg.index_table_id_ = OB_INVALID_ID;
    arg.index_schema_.reset();
    arg.is_rebuild_index_ = false;
    arg.parallelism_ = parallelism_;
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::prepare_shared_async_delete_table()
{
  int ret = OB_SUCCESS;
  int64_t ddl_rpc_timeout = 0;
  ObCommonRpcProxy *common_rpc = nullptr;
  if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("RS RPC proxy is null", K(ret));
  } else if (OB_FALSE_IT(common_rpc = GCTX.rs_rpc_proxy_)) {
  } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
                 tenant_id_, object_id_, ddl_rpc_timeout))) {
    LOG_WARN("fail to get DDL RPC timeout", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_INVALID_ID == shared_async_delete_table_id_) {
    ObArenaAllocator round_allocator("AsyncVecDel");
    SMART_VARS_3((ObCreateIndexArg, index_arg),
                 (ObCreateAuxIndexArg, arg),
                 (ObCreateAuxIndexRes, res)) {
      arg.tenant_id_ = tenant_id_;
      arg.exec_tenant_id_ = tenant_id_;
      arg.data_table_id_ = object_id_;
      arg.task_id_ = task_id_;
      arg.snapshot_version_ = snapshot_version_;
      if (OB_FAIL(construct_async_delete_arg(round_allocator, index_arg))) {
        LOG_WARN("fail to construct async delete arg", K(ret));
      } else if (OB_FAIL(arg.create_index_arg_.assign(index_arg))) {
        LOG_WARN("fail to assign async delete arg", K(ret));
      } else if (OB_FAIL(common_rpc->to(ObRpcProxy::myaddr_)
                         .timeout(ddl_rpc_timeout).create_aux_index(arg, res))) {
        LOG_WARN("fail to create or reuse shared async delete table", K(ret), K(arg));
      } else if (OB_UNLIKELY(!res.schema_generated_
                             || OB_INVALID_ID == res.aux_table_id_
                             || OB_INVALID_ID != res.ddl_task_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid shared async delete result", K(ret), K(res));
      } else {
        shared_async_delete_table_id_ = res.aux_table_id_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *async_delete_schema = nullptr;
    if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
            tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(
                   tenant_id_, shared_async_delete_table_id_, async_delete_schema))) {
      LOG_WARN("fail to get shared async delete schema", K(ret),
          K(shared_async_delete_table_id_));
    } else if (OB_ISNULL(async_delete_schema)
               || !async_delete_schema->is_async_delete_index()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("invalid shared async delete schema", K(ret),
          K(shared_async_delete_table_id_), KPC(async_delete_schema));
    } else if (INDEX_STATUS_UNAVAILABLE == async_delete_schema->get_index_status()
               && !uses_parent_publish_protocol()
               && OB_FAIL(update_index_status(
                      *async_delete_schema, INDEX_STATUS_AVAILABLE, schema_guard))) {
      LOG_WARN("fail to enable shared async delete table", K(ret),
          K(shared_async_delete_table_id_));
    } else if (INDEX_STATUS_AVAILABLE != async_delete_schema->get_index_status()
               && INDEX_STATUS_UNAVAILABLE != async_delete_schema->get_index_status()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected shared async delete status", K(ret), KPC(async_delete_schema));
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::create_baseline_task()
{
  int ret = OB_SUCCESS;
  if (ObDDLTaskStatus::GENERATE_ASYNC_VECTOR_INDEX_BASELINE != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status does not match baseline generation", K(ret), K(task_status_));
  } else if (OB_FAIL(prepare_shared_async_delete_table())) {
    LOG_WARN("fail to prepare shared async delete table", K(ret), K(*this));
  } else if (!baseline_task_submitted_) {
    int64_t ddl_rpc_timeout = 0;
    if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("RS RPC proxy is null", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
                   tenant_id_, object_id_, ddl_rpc_timeout))) {
      LOG_WARN("fail to get DDL RPC timeout", K(ret));
    } else {
      SMART_VARS_2((ObCreateAuxIndexArg, arg), (ObCreateAuxIndexRes, res)) {
        arg.tenant_id_ = tenant_id_;
        arg.exec_tenant_id_ = tenant_id_;
        arg.data_table_id_ = object_id_;
        arg.task_id_ = task_id_;
        arg.snapshot_version_ = snapshot_version_;
        if (OB_FAIL(arg.create_index_arg_.assign(create_index_arg_))) {
          LOG_WARN("fail to assign baseline create arg", K(ret));
        } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(ObRpcProxy::myaddr_)
                           .timeout(ddl_rpc_timeout).create_aux_index(arg, res))) {
          LOG_WARN("fail to create async vector baseline task", K(ret), K(arg));
        } else if (OB_UNLIKELY(!res.schema_generated_
                               || target_object_id_ != res.aux_table_id_
                               || (res.ddl_task_id_ <= 0
                                   && -1 != res.ddl_task_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid async vector baseline result", K(ret), K(res),
              K(target_object_id_));
        } else {
          baseline_task_submitted_ = true;
          baseline_task_id_ = res.ddl_task_id_;
        }
      }
    }
  }
  if (OB_SUCC(ret) && baseline_task_submitted_) {
    if (-1 == baseline_task_id_) {
      // create_aux_index() returns -1 after a committed child has already made
      // the target AVAILABLE. No child result remains to wait for.
      if (OB_FAIL(switch_status(
              is_offline_redefinition_child()
                  ? ObDDLTaskStatus::TAKE_EFFECT
                  : ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH,
              true, OB_SUCCESS))) {
        LOG_WARN("fail to advance completed async vector baseline", K(ret));
      }
    } else if (OB_UNLIKELY(baseline_task_id_ <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid baseline child task id", K(ret), K(baseline_task_id_));
    } else if (OB_FAIL(switch_status(
                   ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE,
                   true, OB_SUCCESS))) {
      LOG_WARN("fail to switch to baseline wait", K(ret));
    }
  }
  return ret;
}

bool ObAsyncVectorIndexBuildTask::is_baseline_failure_confirmed(const ObDDLTaskRecord &child_record)
{
  int ret = OB_SUCCESS;
  bool confirmed = false;
  if (can_report_terminal_failure_before_cleanup(child_record)) {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
            tenant_id_, schema_guard))) {
      LOG_WARN("fail to confirm baseline failure schema", K(ret), K(baseline_task_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, target_object_id_, index_schema))) {
      LOG_WARN("fail to get baseline failure schema", K(ret), K(target_object_id_));
    } else if (nullptr != index_schema) {
      // Async IVF build ERROR and AVAILABLE transitions cannot overwrite
      // each other. INDEX_ERROR confirms cleanup cannot recover as AVAILABLE.
      confirmed = INDEX_STATUS_INDEX_ERROR == index_schema->get_index_status();
    }
    // This is an optional early result. A failed lookup must not replace the
    // child's result or turn a potentially successful build into parent FAIL.
  }
  return confirmed;
}

int ObAsyncVectorIndexBuildTask::wait_baseline_task_finish()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (ObDDLTaskStatus::WAIT_ASYNC_VECTOR_INDEX_BASELINE != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status does not match baseline wait", K(ret), K(task_status_));
  } else if (!baseline_task_submitted_ || baseline_task_id_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("baseline child is not submitted", K(ret), K(baseline_task_submitted_),
        K(baseline_task_id_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SQL proxy is null", K(ret));
  } else {
    HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage, error_message) {
      ObAddr unused_addr;
      int64_t unused_user_msg_len = 0;
      ret = ObDDLErrorMessageTableOperator::get_ddl_error_message(
          tenant_id_, baseline_task_id_, -1 /* target_object_id */,
          unused_addr, false /* is_ddl_retry_task */, *GCTX.sql_proxy_,
          error_message, unused_user_msg_len);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        // A child's FAIL record can still finish successfully if its index
        // already took effect. Only report failure early after cleanup has
        // committed INDEX_ERROR; keep waiting for the final result otherwise.
        ObArenaAllocator allocator(ObMemAttr(MTL_CTX() ? MTL_ID() : ob_thread_tenant_id(), "AsyncDdlFail"));
        ObDDLTaskRecord child_record;
        const int record_ret = ObDDLTaskRecordOperator::get_ddl_task_record(
            tenant_id_, baseline_task_id_, *GCTX.sql_proxy_, allocator, child_record);
        if (OB_SUCCESS == record_ret && is_baseline_failure_confirmed(child_record)) {
          if (OB_FAIL(switch_status(ObDDLTaskStatus::FAIL, false, child_record.ret_code_))) {
            LOG_WARN("fail to persist failed baseline before cleanup", K(ret), K(baseline_task_id_));
          } else {
            LOG_INFO("baseline failed, retain child cleanup dependency", K(baseline_task_id_),
                K(child_record.ret_code_));
          }
        } else if (OB_SUCCESS != record_ret && OB_ENTRY_NOT_EXIST != record_ret) {
          LOG_WARN("fail to inspect pending baseline cleanup", K(record_ret), K(baseline_task_id_));
        }
      } else if (OB_FAIL(ret)) {
        LOG_WARN("fail to query baseline child result", K(ret), K(baseline_task_id_));
      } else if (OB_SUCCESS != error_message.ret_code_) {
        const int child_ret = error_message.ret_code_;
        FORWARD_USER_ERROR(child_ret, error_message.user_message_);
        LOG_WARN("async vector baseline child failed", K(child_ret), K(baseline_task_id_));
        // The child has finished. Retrying this wait cannot recover its output;
        // persist FAIL before the generic handler can swallow a whitelist error.
        if (OB_FAIL(switch_status(ObDDLTaskStatus::FAIL, false, child_ret))) {
          LOG_WARN("fail to persist baseline child failure", K(ret), K(child_ret),
              K(baseline_task_id_));
        }
      } else {
        state_finished = true;
      }
    }
  }
  if (OB_SUCC(ret) && state_finished
      && OB_FAIL(switch_status(
          is_offline_redefinition_child()
              ? ObDDLTaskStatus::TAKE_EFFECT
              : ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH,
          true, OB_SUCCESS))) {
    LOG_WARN("fail to advance completed async vector baseline", K(ret));
  }
  return ret;
}

void ObAsyncVectorIndexBuildTask::clear_old_status_context()
{
  ObDDLTask::clear_old_status_context();
  async_index_publish_refresh_target_.reset();
}

int ObAsyncVectorIndexBuildTask::trigger_async_index_refresh()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> refresh_index_ids;
  const int8_t refresh_type = static_cast<int8_t>(storage::ASYNC_REFRESH_TYPE_CREATE_INDEX);
  if (ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status does not match refresh", K(ret), K(task_status_));
  } else if (!ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("async index refresh is disabled", K(ret), K(tenant_id_));
  } else if (OB_FAIL(refresh_index_ids.assign(existing_async_index_table_ids_))) {
    LOG_WARN("fail to copy async vector publish cohort", K(ret));
  } else {
    const uint64_t new_ids[] = {target_object_id_, shared_async_delete_table_id_};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(new_ids); ++i) {
      bool found = false;
      if (OB_INVALID_ID == new_ids[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async vector publish target", K(ret), K(new_ids[i]));
      } else {
        for (int64_t j = 0; !found && j < refresh_index_ids.count(); ++j) {
          found = new_ids[i] == refresh_index_ids.at(j);
        }
        if (!found && OB_FAIL(refresh_index_ids.push_back(new_ids[i]))) {
          LOG_WARN("fail to add async vector publish target", K(ret), K(new_ids[i]));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObAsyncIndexDDLPublishOp publish_op(
          tenant_id_, object_id_, refresh_index_ids, refresh_type);
      if (OB_FAIL(async_index_publish_refresh_target_.refresh(publish_op, share::SCN::invalid_scn()))) {
        LOG_WARN("fail to publish async vector cohort", K(ret),
            K(async_index_publish_refresh_target_), K(refresh_index_ids));
      } else if (OB_FAIL(enable_published_indexes())) {
        LOG_WARN("fail to enable published async vector indexes", K(ret), K(*this));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(switch_status(ObDDLTaskStatus::TAKE_EFFECT, true, OB_SUCCESS))) {
    LOG_WARN("fail to advance published async vector build", K(ret));
  }
  if (OB_FAIL(ret) && ObDDLTaskStatus::TRIGGER_ASYNC_INDEX_REFRESH == task_status_
      && ObIDDLTask::in_ddl_retry_white_list(ret)) {
    const int refresh_ret = ret;
    if (OB_FAIL(retry_async_index_refresh(refresh_ret))) {
      LOG_WARN("fail to schedule async vector publish retry", K(ret), K(refresh_ret), K(task_id_));
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::enable_published_indexes()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
          tenant_id_, schema_guard))) {
    LOG_WARN("fail to get published async vector schema guard", K(ret));
  } else {
    // Both targets already have P. Enable delete first, then the user index;
    // every query that can select the new IVF also sees its published deletes.
    const uint64_t ids[] = {shared_async_delete_table_id_, target_object_id_};
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(ids); ++i) {
      const ObTableSchema *schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, ids[i], schema))) {
        LOG_WARN("fail to get async vector publish schema", K(ret), K(ids[i]));
      } else if (OB_ISNULL(schema)) {
        ret = OB_SCHEMA_EAGAIN;
        LOG_WARN("async vector publish schema is missing", K(ret), K(ids[i]));
      } else if (INDEX_STATUS_UNAVAILABLE == schema->get_index_status()) {
        if (OB_FAIL(update_index_status(*schema, INDEX_STATUS_AVAILABLE, schema_guard))) {
          LOG_WARN("fail to enable async vector publish target", K(ret), K(ids[i]));
        }
      } else if (INDEX_STATUS_AVAILABLE != schema->get_index_status()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("unexpected async vector publish status", K(ret), KPC(schema));
      }
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::wait_schema_refresh_and_trans_end()
{
  int ret = OB_SUCCESS;
  bool target_trans_end = false;
  bool async_delete_trans_end = false;
  ObSchemaGetterGuard schema_guard;
  if (ObDDLTaskStatus::TAKE_EFFECT != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status does not match TAKE_EFFECT", K(ret), K(task_status_));
  } else if (OB_INVALID_ID == shared_async_delete_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared async delete table is invalid", K(ret),
        K(shared_async_delete_table_id_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(check_schema_and_trans_end(
                 target_object_id_, schema_guard, target_trans_end))) {
    LOG_WARN("fail to wait async vector index schema transaction", K(ret),
        K(target_object_id_));
  } else if (OB_FAIL(check_schema_and_trans_end(
                 shared_async_delete_table_id_, schema_guard,
                 async_delete_trans_end))) {
    LOG_WARN("fail to wait shared async delete schema transaction", K(ret),
        K(shared_async_delete_table_id_));
  } else if (are_take_effect_schemas_ready(
                 target_trans_end, async_delete_trans_end)
             && OB_FAIL(switch_status(
                    ObDDLTaskStatus::SUCCESS, true, OB_SUCCESS))) {
    LOG_WARN("fail to switch async vector build to SUCCESS", K(ret));
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::check_schema_and_trans_end(
    const uint64_t index_table_id,
    ObSchemaGetterGuard &schema_guard,
    bool &is_trans_end)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = nullptr;
  int64_t unused_snapshot_version = 0;
  ObDDLWaitTransEndCtx wait_trans_end;
  is_trans_end = false;
  if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id_, index_table_id, index_schema))) {
    LOG_WARN("fail to get async index schema", K(ret), K(index_table_id));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index schema is null", K(ret), K(index_table_id));
  } else if (OB_UNLIKELY(!index_schema->can_read_index())) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("async index is not readable", K(ret), KPC(index_schema));
  } else if (OB_FAIL(wait_trans_end.init(
                 tenant_id_, task_id_, task_status_, index_table_id,
                 ObDDLWaitTransEndCtx::WAIT_SCHEMA_TRANS,
                 index_schema->get_schema_version()))) {
    LOG_WARN("fail to initialize async index schema transaction wait", K(ret),
        K(index_table_id));
  } else if (OB_FAIL(wait_trans_end.try_wait(
                 is_trans_end, unused_snapshot_version))) {
    LOG_WARN("fail to wait async index schema transaction", K(ret),
        K(index_table_id));
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::update_index_status(
    const ObTableSchema &index_schema,
    const ObIndexStatus status,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_FAIL(schema_guard.get_database_schema(
          tenant_id_, index_schema.get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(index_schema));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", K(ret), K(index_schema));
  } else {
    ObUpdateIndexStatusArg arg;
    arg.index_table_id_ = index_schema.get_table_id();
    arg.data_table_id_ = index_schema.get_data_table_id();
    arg.database_name_ = database_schema->get_database_name();
    arg.status_ = status;
    arg.exec_tenant_id_ = tenant_id_;
    arg.in_offline_ddl_white_list_ = true;
    // Enabling the shared table must not release the user index build lock.
    // Updating the user index to ERROR still uses the parent task as owner.
    arg.task_id_ = index_schema.is_async_delete_index()
        || (uses_parent_publish_protocol() && INDEX_STATUS_AVAILABLE == status) ? 0 : task_id_;
    int64_t ddl_rpc_timeout = 0;
    int64_t data_rpc_timeout = 0;
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
            index_schema.get_all_part_num(), ddl_rpc_timeout))) {
      LOG_WARN("fail to get index DDL timeout", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
                   tenant_id_, index_schema.get_data_table_id(), data_rpc_timeout))) {
      LOG_WARN("fail to get data-table DDL timeout", K(ret));
    } else if (OB_FALSE_IT(ddl_rpc_timeout += data_rpc_timeout)) {
    } else if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("RS RPC proxy is null", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(GCTX.self_addr())
                       .timeout(ddl_rpc_timeout).update_index_status(arg))) {
      LOG_WARN("fail to update index status", K(ret), K(arg));
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::wait_failure_drop_finish(bool &is_finish)
{
  int ret = OB_SUCCESS;
  bool need_resubmit = false;
  is_finish = false;
  if (!drop_task_submitted_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("failure DROP task is not submitted", K(ret));
  } else if (OB_FAIL(wait_drop_index_task_finish(
                 drop_task_id_, is_finish, need_resubmit))) {
    if (need_resubmit) {
      drop_task_submitted_ = false;
      drop_task_id_ = 0;
      const int tmp_ret = update_task_message(*GCTX.sql_proxy_);
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to persist failure DROP resubmission", K(ret));
      }
    }
    LOG_WARN("fail to wait failure DROP task", K(ret), K(need_resubmit));
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::finish_failure_metadata_cleanup(bool &is_finish)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  ObSEArray<const ObSimpleTableSchemaV2 *, OB_MAX_AUX_TABLE_PER_MAIN_TABLE>
      index_schemas;
  bool index_exist = false;
  bool async_delete_exist = false;
  bool has_user_async_index = false;
  is_finish = false;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global service is null", K(ret));
  } else if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
                 tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(object_id_));
  } else if (OB_ISNULL(data_table_schema)) {
    is_finish = true;
  } else if (OB_FAIL(schema_guard.check_table_exist(
                 tenant_id_, target_object_id_, index_exist))) {
    LOG_WARN("fail to check async vector index", K(ret), K(target_object_id_));
  } else if (OB_INVALID_ID != shared_async_delete_table_id_
             && OB_FAIL(schema_guard.check_table_exist(
                 tenant_id_, shared_async_delete_table_id_, async_delete_exist))) {
    LOG_WARN("fail to check shared async delete table", K(ret));
  } else if (index_exist) {
    LOG_INFO("wait failure DROP schema propagation before metadata cleanup",
        K(index_exist), K(async_delete_exist));
  } else if (is_offline_redefinition_child()) {
    // The parent owns the hidden table and discards it when this child reports
    // failure. Updating table options here is both unnecessary and forbidden
    // while the table is executing offline DDL; finish so cleanup can notify
    // the parent instead of retrying forever.
    is_finish = true;
    LOG_INFO("skip metadata cleanup for offline redefinition child", K(task_id_));
  } else if (OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(
                 tenant_id_, object_id_, index_schemas))) {
    LOG_WARN("fail to get index schemas", K(ret), K(tenant_id_), K(object_id_));
  } else if (OB_FAIL(ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
                 index_schemas, has_user_async_index))) {
    LOG_WARN("fail to check user async index membership", K(ret), K(object_id_));
  } else if (has_user_async_index) {
    is_finish = true;
  } else if (async_delete_exist) {
    LOG_INFO("wait shared async delete schema propagation before metadata cleanup",
        K(async_delete_exist), K(shared_async_delete_table_id_));
  } else if (OB_FAIL(ObAsyncVectorIndexTaskUtil::clear_async_index_table_options(
                 tenant_id_, object_id_, create_index_arg_.ddl_stmt_str_,
                 GCTX.root_service_, has_user_async_index))) {
    LOG_WARN("fail to clear failed async vector build metadata", K(ret));
  } else {
    is_finish = true;
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::submit_failure_drop(
    const ObTableSchema &data_table_schema,
    const ObTableSchema *index_schema,
    const ObTableSchema *async_delete_schema)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObTableSchema *drop_schema = nullptr != index_schema
      ? index_schema : async_delete_schema;
  ObSchemaGetterGuard schema_guard;
  ObDropIndexArg arg;
  ObDropIndexRes res;
  int64_t ddl_rpc_timeout = 0;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id_, data_table_schema.get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", K(ret));
  } else if (OB_ISNULL(drop_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failure DROP target schema is null", K(ret));
  } else if (OB_FAIL(construct_failure_drop_arg(
                 data_table_schema, *database_schema,
                 index_schema, async_delete_schema, arg))) {
    LOG_WARN("fail to construct async vector failure DROP argument", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
                 drop_schema->get_all_part_num()
                     + data_table_schema.get_all_part_num(),
                 ddl_rpc_timeout))) {
    LOG_WARN("fail to get failure DROP timeout", K(ret));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(ddl_rpc_timeout)
                     .drop_index_on_failed(arg, res))) {
    LOG_WARN("fail to submit async vector failure DROP", K(ret), K(arg));
  } else {
    drop_task_submitted_ = true;
    drop_task_id_ = res.task_id_;
    LOG_INFO("submitted async vector failure DROP", K(drop_task_id_), K(arg));
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::construct_failure_drop_arg(
    const ObTableSchema &data_table_schema,
    const ObDatabaseSchema &database_schema,
    const ObTableSchema *index_schema,
    const ObTableSchema *async_delete_schema,
    ObDropIndexArg &arg) const
{
  int ret = OB_SUCCESS;
  const bool is_composite_drop = nullptr != index_schema
      && nullptr != async_delete_schema;
  const ObTableSchema *drop_schema = nullptr != index_schema
      ? index_schema : async_delete_schema;
  ObString index_name;
  if (OB_ISNULL(drop_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failure DROP target schema is null", K(ret));
  } else if (OB_FAIL(drop_schema->get_index_name(index_name))) {
    LOG_WARN("fail to get failure DROP target name", K(ret), KPC(drop_schema));
  } else {
    arg.tenant_id_ = tenant_id_;
    arg.exec_tenant_id_ = tenant_id_;
    arg.index_table_id_ = drop_schema->get_table_id();
    arg.table_id_ = drop_schema->get_table_id();
    arg.session_id_ = create_index_arg_.session_id_;
    arg.index_name_ = index_name;
    arg.table_name_ = data_table_schema.get_table_name();
    arg.database_name_ = database_schema.get_database_name_str();
    arg.index_action_type_ = ObIndexArg::DROP_INDEX;
    arg.ddl_stmt_str_ = create_index_arg_.ddl_stmt_str_;
    arg.is_add_to_scheduler_ = true;
    arg.is_hidden_ = data_table_schema.is_user_hidden_table();
    arg.is_inner_ = true;
    arg.is_parent_task_dropping_user_async_index_ = is_composite_drop;
    arg.task_id_ = task_id_;
    if (OB_NOT_NULL(index_schema)
        && OB_FAIL(arg.index_ids_.push_back(index_schema->get_table_id()))) {
      LOG_WARN("fail to add async vector cleanup target", K(ret), KPC(index_schema));
    } else if (OB_NOT_NULL(async_delete_schema)
               && OB_FAIL(arg.index_ids_.push_back(
                   async_delete_schema->get_table_id()))) {
      LOG_WARN("fail to add shared async delete cleanup candidate", K(ret),
          KPC(async_delete_schema));
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::wait_baseline_task_stop(bool &is_finish)
{
  int ret = OB_SUCCESS;
  is_finish = !baseline_task_submitted_ || baseline_task_id_ <= 0;
  if (!is_finish) {
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("SQL proxy is null", K(ret));
    } else {
      HEAP_VAR(ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage,
          error_message) {
        ObAddr unused_addr;
        int64_t unused_user_msg_len = 0;
        ret = ObDDLErrorMessageTableOperator::get_ddl_error_message(
            tenant_id_, baseline_task_id_, -1 /* target_object_id */,
            unused_addr, false /* is_ddl_retry_task */, *GCTX.sql_proxy_,
            error_message, unused_user_msg_len);
        if (OB_ENTRY_NOT_EXIST == ret) {
          ObMySQLTransaction trans;
          int64_t child_status = 0;
          int64_t child_execution_id = 0;
          int64_t child_ret_code = OB_SUCCESS;
          int64_t child_snapshot_version = 0;
          ret = OB_SUCCESS;
          if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
            LOG_WARN("fail to start baseline cancellation transaction", K(ret));
          } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(
                         trans, tenant_id_, baseline_task_id_, child_status,
                         child_execution_id, child_ret_code, child_snapshot_version))) {
            LOG_WARN("fail to lock baseline child for cancellation", K(ret),
                K(baseline_task_id_));
          } else if (ObDDLTaskStatus::SUCCESS == child_status) {
            // Its durable success may precede reporting. Wait for that result
            // instead of turning a completed child into a cancelled task.
          } else if (OB_SUCCESS == child_ret_code
                     && OB_FAIL(ObDDLTaskRecordOperator::update_ret_code(
                         trans, tenant_id_, baseline_task_id_, OB_CANCELED))) {
            LOG_WARN("fail to save baseline cancellation result", K(ret),
                K(baseline_task_id_));
          } else if (OB_FAIL(ObDDLTaskRecordOperator::update_task_status(
                         trans, tenant_id_, baseline_task_id_,
                         ObDDLTaskStatus::FAIL))) {
            LOG_WARN("fail to cancel baseline child task", K(ret),
                K(baseline_task_id_));
          }
          if (trans.is_started()) {
            const int tmp_ret = trans.end(OB_SUCC(ret));
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("fail to end baseline cancellation transaction",
                  K(ret), K(tmp_ret));
              ret = OB_SUCC(ret) ? tmp_ret : ret;
            }
          }
        } else if (OB_FAIL(ret)) {
          LOG_WARN("fail to query baseline child result", K(ret),
              K(baseline_task_id_));
        } else {
          is_finish = true;
        }
      }
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::clean_on_failed()
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  bool baseline_stopped = false;
  bool task_exist = true;
  if (ObDDLTaskStatus::FAIL != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status does not match FAIL", K(ret), K(task_status_));
  } else if (uses_parent_publish_protocol() && OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SQL proxy is null", K(ret));
  } else if (uses_parent_publish_protocol()
             && OB_FAIL(ObDDLTaskRecordOperator::check_task_id_exist(
                 *GCTX.sql_proxy_, tenant_id_, task_id_, task_exist))) {
    LOG_WARN("fail to check async vector cleanup task existence", K(ret), K(task_id_));
  } else if (!task_exist) {
    // DELETE follows snapshot release and result reporting; its response may be lost.
    need_retry_ = false;
    LOG_INFO("async vector build task is already cleaned", K(task_id_));
  } else if (uses_parent_publish_protocol()
             && OB_FAIL(refresh_task_context(ObDDLTaskStatus::FAIL))) {
    // Do not DROP schemas until the durable snapshot owner is known.
    LOG_WARN("fail to reload async vector failure cleanup context", K(ret), K(task_id_));
  } else if (OB_FAIL(wait_baseline_task_stop(baseline_stopped))) {
    LOG_WARN("fail to stop async vector baseline child", K(ret));
  } else if (!baseline_stopped) {
    LOG_INFO("wait async vector baseline child to stop", K(baseline_task_id_));
  } else if (is_parent_snapshot_owner()) {
    // The target schema is still needed to enumerate every snapshot tablet.
    // Commit snapshot release before submitting the failure DROP.
    if (OB_FAIL(release_parent_snapshot_before_failure_drop())) {
      LOG_WARN("fail to release snapshot before async vector failure DROP",
          K(ret), K(snapshot_version_), K(snapshot_holder_schema_version_));
    }
  } else if (drop_task_submitted_) {
    if (OB_FAIL(wait_failure_drop_finish(state_finished))) {
      LOG_WARN("fail to wait async vector failure DROP", K(ret));
    }
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_table_schema = nullptr;
    const ObTableSchema *index_schema = nullptr;
    const ObTableSchema *async_delete_schema = nullptr;
    bool index_exist = false;
    bool async_delete_exist = false;
    bool should_drop_async_delete = false;
    if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
                 tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(
                   tenant_id_, object_id_, data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(object_id_));
    } else if (OB_ISNULL(data_table_schema)) {
      state_finished = true;
      LOG_INFO("data table no longer exists during async vector cleanup", K(object_id_));
    } else if (OB_FAIL(schema_guard.check_table_exist(
                   tenant_id_, target_object_id_, index_exist))) {
      LOG_WARN("fail to check async vector index", K(ret), K(target_object_id_));
    } else if (index_exist
               && OB_FAIL(schema_guard.get_table_schema(
                      tenant_id_, target_object_id_, index_schema))) {
      LOG_WARN("fail to get async vector index schema", K(ret));
    } else if (index_exist && OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("async vector index schema is null", K(ret));
    } else if (OB_INVALID_ID != shared_async_delete_table_id_
               && OB_FAIL(schema_guard.check_table_exist(
                   tenant_id_, shared_async_delete_table_id_, async_delete_exist))) {
      LOG_WARN("fail to check shared async delete table", K(ret));
    } else if (async_delete_exist
               && OB_FAIL(schema_guard.get_table_schema(
                      tenant_id_, shared_async_delete_table_id_, async_delete_schema))) {
      LOG_WARN("fail to get shared async delete schema", K(ret));
    } else if (async_delete_exist
               && (OB_ISNULL(async_delete_schema)
                   || !async_delete_schema->is_async_delete_index())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("invalid shared async delete schema", K(ret), KPC(async_delete_schema));
    } else {
      should_drop_async_delete = async_delete_exist;
      for (int64_t i = 0;
           should_drop_async_delete && i < existing_async_index_table_ids_.count();
           ++i) {
        should_drop_async_delete =
            existing_async_index_table_ids_.at(i) != shared_async_delete_table_id_;
      }
    }
    if (OB_SUCC(ret) && (index_exist || should_drop_async_delete)
               && OB_FAIL(submit_failure_drop(
                      *data_table_schema, index_schema,
                      should_drop_async_delete ? async_delete_schema : nullptr))) {
      LOG_WARN("fail to submit async vector failure DROP", K(ret));
    } else if (OB_SUCC(ret) && !index_exist && !should_drop_async_delete) {
      state_finished = true;
    }
  }
  if (OB_SUCC(ret) && state_finished) {
    bool metadata_finished = false;
    if (OB_FAIL(finish_failure_metadata_cleanup(metadata_finished))) {
      LOG_WARN("fail to finish async vector failure metadata cleanup", K(ret));
    } else if (!metadata_finished) {
      state_finished = false;
    }
  }
  if (OB_SUCC(ret) && state_finished) {
    if (OB_FAIL(cleanup())) {
      LOG_WARN("fail to clean async vector build task", K(ret));
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::release_parent_snapshot_before_failure_drop()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  bool snapshot_released = false;
  if (!is_parent_snapshot_owner()) {
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SQL proxy is null", K(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start snapshot release transaction", K(ret), K(tenant_id_));
  } else if (OB_FAIL(try_release_snapshot(trans, snapshot_released))) {
    LOG_WARN("fail to release parent-owned async vector snapshot", K(ret),
        K(snapshot_version_), K(snapshot_holder_schema_version_));
  }
  if (trans.is_started()) {
    const int tmp_ret = trans.end(OB_SUCC(ret));
    const bool trans_committed = OB_SUCC(ret) && OB_SUCCESS == tmp_ret;
    finish_snapshot_release(snapshot_released, trans_committed);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to end snapshot release transaction", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::release_build_lock()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  ObTableLockOwnerID owner_id;
  ObMySQLTransaction trans;
  bool snapshot_released = false;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, object_id_, data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start unlock transaction", K(ret));
  } else if (OB_ISNULL(data_table_schema)) {
    LOG_INFO("data table no longer exists, skip build lock release", K(object_id_));
  } else if (OB_FAIL(owner_id.convert_from_value(
                 ObLockOwnerType::DEFAULT_OWNER_TYPE, task_id_))) {
    LOG_WARN("fail to construct build lock owner", K(ret), K(task_id_));
  } else if (OB_FAIL(ObDDLLock::unlock_for_add_drop_index(
                 *data_table_schema, target_object_id_, false,
                 owner_id, trans))) {
    LOG_WARN("fail to unlock async vector build", K(ret), K(task_id_));
  }
  // Deferred cleanup reloads its durable snapshot even when the cached value is zero.
  if (OB_SUCC(ret)
      && (uses_parent_publish_protocol()
          || (ObDDLTaskStatus::FAIL == task_status_ && is_parent_snapshot_owner()))
      && OB_FAIL(try_release_snapshot(trans, snapshot_released))) {
    LOG_WARN("fail to release parent-owned async vector snapshot", K(ret),
        K(snapshot_version_), K(snapshot_holder_schema_version_));
  }
  if (trans.is_started()) {
    const int tmp_ret = trans.end(OB_SUCC(ret));
    const bool trans_committed = OB_SUCC(ret) && OB_SUCCESS == tmp_ret;
    finish_snapshot_release(snapshot_released, trans_committed);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to end unlock transaction", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::refresh_task_context(
    const ObDDLTaskStatus status)
{
  int ret = OB_SUCCESS;
  if (ObDDLTaskStatus::FAIL == status) {
    ObArenaAllocator round_allocator("AsyncVecReload");
    SMART_VAR(ObAsyncVectorIndexBuildTask, persisted_task) {
      if (OB_FAIL(ObDDLUtil::load_ddl_task(
              tenant_id_, task_id_, round_allocator, persisted_task))) {
        LOG_WARN("fail to load async vector build task", K(ret), K(task_id_));
      } else {
        snapshot_version_ = persisted_task.snapshot_version_;
        snapshot_holder_schema_version_ = persisted_task.snapshot_holder_schema_version_;
        wait_trans_ctx_.reset();
        shared_async_delete_table_id_ = persisted_task.shared_async_delete_table_id_;
        baseline_task_submitted_ = persisted_task.baseline_task_submitted_;
        baseline_task_id_ = persisted_task.baseline_task_id_;
        drop_task_submitted_ = persisted_task.drop_task_submitted_;
        drop_task_id_ = persisted_task.drop_task_id_;
      }
    }
  }
  return ret;
}

void ObAsyncVectorIndexBuildTask::finish_snapshot_release(
    const bool snapshot_released,
    const bool trans_committed)
{
  if (snapshot_released && trans_committed) {
    snapshot_version_ = 0;
  }
}

int ObAsyncVectorIndexBuildTask::try_release_snapshot(
    ObMySQLTransaction &trans,
    bool &snapshot_released)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 2> tablet_ids;
  SCN snapshot_scn;
  int64_t persisted_status = 0;
  int64_t persisted_execution = -1;
  int64_t persisted_ret = OB_SUCCESS;
  int64_t persisted_snapshot = snapshot_version_;
  snapshot_released = false;
  if (uses_parent_publish_protocol()) {
    if (ObDDLTaskStatus::FAIL != task_status_ && ObDDLTaskStatus::SUCCESS != task_status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("async vector task is not ready for snapshot cleanup", K(ret), K(task_id_), K(task_status_));
    } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(
            trans, tenant_id_, task_id_, persisted_status, persisted_execution,
            persisted_ret, persisted_snapshot))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // Task deletion follows snapshot release and durable result reporting.
        ret = OB_SUCCESS;
        snapshot_released = true;
      } else {
        LOG_WARN("fail to reload async vector snapshot for release", K(ret), K(task_id_));
      }
    } else if (ObDDLTaskStatus::FAIL != static_cast<ObDDLTaskStatus>(persisted_status)
               && ObDDLTaskStatus::SUCCESS != static_cast<ObDDLTaskStatus>(persisted_status)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("persisted async vector task is not ready for snapshot cleanup",
          K(ret), K(task_id_), K(persisted_status));
    } else if (persisted_snapshot < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid persisted async vector snapshot", K(ret), K(persisted_snapshot));
    } else {
      snapshot_version_ = persisted_snapshot;
    }
  }
  if (OB_FAIL(ret) || snapshot_released) {
  } else if (snapshot_version_ <= 0) {
    snapshot_released = uses_parent_publish_protocol();
  } else if (snapshot_holder_schema_version_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot holder schema version is invalid", K(ret),
        K(snapshot_holder_schema_version_));
  } else if (OB_FAIL(ObDDLUtil::get_tablet_ids(
                 tenant_id_, object_id_, target_object_id_, tablet_ids))) {
    LOG_WARN("fail to get async vector snapshot tablets", K(ret),
        K(object_id_), K(target_object_id_));
  } else if (tablet_ids.count() <= 0) {
    if (OB_FAIL(ObDDLTaskRecordOperator::update_snapshot_version(
            trans, tenant_id_, task_id_, 0))) {
      LOG_WARN("fail to clear async vector task snapshot", K(ret), K(task_id_));
    } else {
      snapshot_released = true;
    }
  } else if (OB_FAIL(snapshot_scn.convert_for_tx(snapshot_version_))) {
    LOG_WARN("fail to convert async vector snapshot SCN", K(ret),
        K(snapshot_version_));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service is null", K(ret));
  } else if (OB_FAIL(GCTX.root_service_->get_ddl_service()
                 .get_snapshot_mgr().batch_release_snapshot_in_trans(
                     trans, SNAPSHOT_FOR_DDL, tenant_id_,
                     snapshot_holder_schema_version_, snapshot_scn, tablet_ids))) {
    LOG_WARN("fail to release async vector snapshot", K(ret), K(tablet_ids));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::update_snapshot_version(
                 trans, tenant_id_, task_id_, 0))) {
    LOG_WARN("fail to clear async vector task snapshot", K(ret), K(task_id_));
  } else {
    snapshot_released = true;
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::succ()
{
  ret_code_ = OB_SUCCESS;
  return cleanup();
}

int ObAsyncVectorIndexBuildTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_message;
  if (OB_FAIL(release_build_lock())) {
    LOG_WARN("fail to release async vector build lock", K(ret), K(task_id_));
  } else if (OB_FAIL(report_error_code(unused_message))) {
    LOG_WARN("fail to report async vector build result", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SQL proxy is null", K(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::delete_record(
                 *GCTX.sql_proxy_, tenant_id_, task_id_))) {
    LOG_WARN("fail to delete async vector build task record", K(ret), K(task_id_));
  } else {
    need_retry_ = false;
  }
  if (OB_SUCC(ret) && parent_task_id_ > 0) {
    const ObDDLTaskID parent_task_id(tenant_id_, parent_task_id_);
    ObSysDDLSchedulerUtil::on_ddl_task_finish(
        parent_task_id, get_task_key(), ret_code_, trace_id_);
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::on_child_task_finish(
    const uint64_t child_task_key,
    const int ret_code)
{
  UNUSEDx(child_task_key, ret_code);
  // Completion is polled from the durable DDL error-message table so recovery
  // does not depend on an in-memory callback.
  return OB_SUCCESS;
}

int ObAsyncVectorIndexBuildTask::serialize_params_to_message(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int8_t baseline_submitted = static_cast<int8_t>(baseline_task_submitted_);
  const int8_t drop_submitted = static_cast<int8_t>(drop_task_submitted_);
  const int8_t retryable = static_cast<int8_t>(is_retryable_ddl_);
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid serialization buffer", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(
                 buf, buf_len, pos))) {
    LOG_WARN("fail to serialize base DDL task", K(ret));
  } else if (OB_FAIL(create_index_arg_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize create index arg", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        shared_async_delete_table_id_, baseline_submitted, baseline_task_id_,
        drop_submitted, drop_task_id_, retryable);
    if (OB_SUCC(ret)
        && OB_FAIL(serialization::encode_i64(
               buf, buf_len, pos, existing_async_index_table_ids_.count()))) {
      LOG_WARN("fail to serialize existing async index count", K(ret));
    }
    for (int64_t i = 0;
         OB_SUCC(ret) && i < existing_async_index_table_ids_.count(); ++i) {
      if (OB_FAIL(serialization::encode_i64(
              buf, buf_len, pos,
              static_cast<int64_t>(existing_async_index_table_ids_.at(i))))) {
        LOG_WARN("fail to serialize existing async index id", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(serialization::encode_i64(
               buf, buf_len, pos, snapshot_holder_schema_version_))) {
      LOG_WARN("fail to serialize snapshot holder schema version", K(ret),
          K(snapshot_holder_schema_version_));
    }
  }
  return ret;
}

int ObAsyncVectorIndexBuildTask::deserialize_params_from_message(
    const uint64_t tenant_id,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int8_t baseline_submitted = 0;
  int8_t drop_submitted = 0;
  int8_t retryable = 0;
  SMART_VAR(ObCreateIndexArg, tmp_arg) {
    if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                    || nullptr == buf || data_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid deserialization arguments", K(ret), K(tenant_id),
          KP(buf), K(data_len));
    } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(
                   tenant_id, buf, data_len, pos))) {
      LOG_WARN("fail to deserialize base DDL task", K(ret));
    } else if (OB_FAIL(tmp_arg.deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize create index arg", K(ret));
    } else if (OB_FAIL(ObDDLUtil::replace_user_tenant_id(tenant_id, tmp_arg))) {
      LOG_WARN("fail to replace tenant id", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObDDLTask::deep_copy_table_arg(
                   allocator_, tmp_arg, create_index_arg_))) {
      LOG_WARN("fail to copy deserialized create index arg", K(ret));
    } else {
      // Persisted ALTER tasks may have omitted the nested scheduling parameters.
      create_index_arg_.parallelism_ = parallelism_;
      create_index_arg_.consumer_group_id_ = consumer_group_id_;
      LST_DO_CODE(OB_UNIS_DECODE,
          shared_async_delete_table_id_, baseline_submitted, baseline_task_id_,
          drop_submitted, drop_task_id_, retryable);
      baseline_task_submitted_ = static_cast<bool>(baseline_submitted);
      drop_task_submitted_ = static_cast<bool>(drop_submitted);
      is_retryable_ddl_ = static_cast<bool>(retryable);
      int64_t existing_index_count = 0;
      if (OB_SUCC(ret)
          && OB_FAIL(serialization::decode_i64(
                 buf, data_len, pos, &existing_index_count))) {
        LOG_WARN("fail to deserialize existing async index count", K(ret));
      } else if (OB_SUCC(ret) && OB_UNLIKELY(existing_index_count < 0)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("invalid existing async index count", K(ret), K(existing_index_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < existing_index_count; ++i) {
        int64_t table_id = 0;
        if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &table_id))) {
          LOG_WARN("fail to deserialize existing async index id", K(ret), K(i));
        } else if (OB_UNLIKELY(table_id <= 0)) {
          ret = OB_DESERIALIZE_ERROR;
          LOG_WARN("invalid existing async index id", K(ret), K(table_id), K(i));
        } else if (OB_FAIL(existing_async_index_table_ids_.push_back(
                       static_cast<uint64_t>(table_id)))) {
          LOG_WARN("fail to save existing async index id", K(ret), K(table_id));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(serialization::decode_i64(
                 buf, data_len, pos, &snapshot_holder_schema_version_))) {
        LOG_WARN("fail to deserialize snapshot holder schema version", K(ret));
      } else if (OB_SUCC(ret) && snapshot_holder_schema_version_ <= 0) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("invalid snapshot holder schema version", K(ret),
            K(snapshot_holder_schema_version_));
      }
    }
  }
  return ret;
}

int64_t ObAsyncVectorIndexBuildTask::get_serialize_param_size() const
{
  int64_t len = ObDDLTask::get_serialize_param_size()
      + create_index_arg_.get_serialize_size();
  const int8_t baseline_submitted = static_cast<int8_t>(baseline_task_submitted_);
  const int8_t drop_submitted = static_cast<int8_t>(drop_task_submitted_);
  const int8_t retryable = static_cast<int8_t>(is_retryable_ddl_);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      shared_async_delete_table_id_, baseline_submitted, baseline_task_id_,
      drop_submitted, drop_task_id_, retryable);
  len += serialization::encoded_length_i64(existing_async_index_table_ids_.count())
      + existing_async_index_table_ids_.count()
          * serialization::encoded_length_i64(0);
  len += serialization::encoded_length_i64(snapshot_holder_schema_version_);
  return len;
}

int ObAsyncVectorIndexBuildTask::update_task_message(ObISQLClient &proxy)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AsyncVecBuild");
  const int64_t size = get_serialize_param_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate task message", K(ret), K(size));
  } else if (OB_FAIL(serialize_params_to_message(buf, size, pos))) {
    LOG_WARN("fail to serialize task message", K(ret));
  } else {
    ObString message;
    message.assign(buf, size);
    if (OB_FAIL(ObDDLTaskRecordOperator::update_message(
            proxy, tenant_id_, task_id_, message))) {
      LOG_WARN("fail to update task message", K(ret), K(task_id_));
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
