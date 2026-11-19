/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "rootserver/ddl_task/ob_drop_async_vector_index_task.h"
#include "rootserver/ddl_task/ob_async_index_ddl_util.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{

ObDropAsyncVectorIndexTask::ObDropAsyncVectorIndexTask()
  : ObDDLTask(DDL_DROP_ASYNC_VEC_IVF_INDEX),
    root_service_(nullptr),
    user_index_(),
    async_delete_(),
    drop_user_index_finish_(false),
    drop_async_delete_finish_(false),
    async_delete_info_needs_persist_(false)
{
}

ObDropAsyncVectorIndexTask::~ObDropAsyncVectorIndexTask()
{
}

int ObDropAsyncVectorIndexTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const uint64_t data_table_id,
    const ObDDLType ddl_type,
    const ObVecIndexDDLChildTaskInfo &user_index,
    const ObVecIndexDDLChildTaskInfo &async_delete,
    const ObString &ddl_stmt_str,
    const int64_t schema_version,
    const int64_t consumer_group_id,
    const int64_t target_object_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(DDL_DROP_ASYNC_VEC_IVF_INDEX != ddl_type
               || OB_INVALID_ID == tenant_id
               || task_id <= 0
               || OB_INVALID_ID == data_table_id
               || !user_index.is_valid()
               || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id),
        K(ddl_type), K(user_index), K(async_delete), K(schema_version));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root service is null", K(ret));
  } else if (OB_FAIL(user_index_.deep_copy_from_other(user_index, allocator_))) {
    LOG_WARN("fail to copy user async vector index child info", K(ret), K(user_index));
  } else if (OB_FAIL(async_delete_.deep_copy_from_other(async_delete, allocator_))) {
    LOG_WARN("fail to copy async delete child info", K(ret), K(async_delete));
  } else if (OB_FAIL(set_ddl_stmt_str(ddl_stmt_str))) {
    LOG_WARN("fail to copy ddl statement", K(ret));
  } else {
    task_type_ = ddl_type;
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = tenant_id;
    object_id_ = data_table_id;
    target_object_id_ = target_object_id;
    schema_version_ = schema_version;
    task_id_ = task_id;
    parent_task_id_ = 0;
    consumer_group_id_ = consumer_group_id;
    task_version_ = OB_DROP_ASYNC_VECTOR_INDEX_TASK_VERSION;
    dst_tenant_id_ = tenant_id;
    dst_schema_version_ = schema_version;
    is_inited_ = true;
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(!task_record.is_valid()
               || DDL_DROP_ASYNC_VEC_IVF_INDEX != task_record.ddl_type_
               || OB_DROP_ASYNC_VECTOR_INDEX_TASK_VERSION != task_record.task_version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async vector DROP task record", K(ret), K(task_record));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root service is null", K(ret));
  } else {
    task_type_ = task_record.ddl_type_;
    tenant_id_ = task_record.tenant_id_;
    object_id_ = task_record.object_id_;
    target_object_id_ = task_record.target_object_id_;
    schema_version_ = task_record.schema_version_;
    task_id_ = task_record.task_id_;
    parent_task_id_ = task_record.parent_task_id_;
    task_version_ = task_record.task_version_;
    ret_code_ = task_record.ret_code_;
    dst_tenant_id_ = tenant_id_;
    dst_schema_version_ = schema_version_;
    if (OB_ISNULL(task_record.message_.ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task record message is null", K(ret), K(task_record));
    } else if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_,
                   task_record.message_.ptr(), task_record.message_.length(), pos))) {
      LOG_WARN("fail to deserialize async vector DROP task", K(ret), K(task_record));
    } else {
      is_inited_ = true;
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not initialized", K(ret));
  } else if (!need_retry()) {
  } else if (OB_FAIL(check_switch_succ())) {
    LOG_WARN("fail to check async vector DROP completion", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    switch (static_cast<ObDDLTaskStatus>(task_status_)) {
      case ObDDLTaskStatus::PREPARE:
        ret = prepare(ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH);
        break;
      case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH:
        ret = check_and_wait_finish();
        break;
      case ObDDLTaskStatus::SUCCESS:
        ret = succ();
        break;
      case ObDDLTaskStatus::FAIL:
        ret = fail();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected task status", K(ret), K(task_status_));
        break;
    }
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::serialize_params_to_message(
    char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int8_t user_finish = static_cast<int8_t>(drop_user_index_finish_);
  const int8_t async_delete_finish = static_cast<int8_t>(drop_async_delete_finish_);
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize base task", K(ret));
  } else if (OB_FAIL(user_index_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize user index child", K(ret), K(user_index_));
  } else if (OB_FAIL(async_delete_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize async delete child", K(ret), K(async_delete_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_size, pos, user_index_.task_id_))) {
    LOG_WARN("fail to serialize user index child task id", K(ret), K(user_index_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_size, pos, async_delete_.task_id_))) {
    LOG_WARN("fail to serialize async delete child task id", K(ret), K(async_delete_));
  } else if (OB_FAIL(ddl_stmt_str_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize ddl statement", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_size, pos, user_finish))) {
    LOG_WARN("fail to serialize user index finish flag", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_size, pos, async_delete_finish))) {
    LOG_WARN("fail to serialize async delete finish flag", K(ret));
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::deserialize_params_from_message(
    const uint64_t tenant_id, const char *buf, const int64_t buf_size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObVecIndexDDLChildTaskInfo tmp_info;
  ObString tmp_ddl_stmt;
  int64_t user_task_id = 0;
  int64_t async_delete_task_id = 0;
  int8_t user_finish = 0;
  int8_t async_delete_finish = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize base task", K(ret));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize user index child", K(ret));
  } else if (OB_FAIL(user_index_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to copy user index child", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize async delete child", K(ret));
  } else if (OB_FAIL(async_delete_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to copy async delete child", K(ret), K(tmp_info));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_size, pos, &user_task_id))) {
    LOG_WARN("fail to deserialize user index child task id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_size, pos, &async_delete_task_id))) {
    LOG_WARN("fail to deserialize async delete child task id", K(ret));
  } else if (OB_FAIL(tmp_ddl_stmt.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize ddl statement", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, tmp_ddl_stmt, ddl_stmt_str_))) {
    LOG_WARN("fail to copy ddl statement", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, buf_size, pos, &user_finish))) {
    LOG_WARN("fail to deserialize user index finish flag", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, buf_size, pos, &async_delete_finish))) {
    LOG_WARN("fail to deserialize async delete finish flag", K(ret));
  } else {
    user_index_.task_id_ = user_task_id;
    async_delete_.task_id_ = async_delete_task_id;
    drop_user_index_finish_ = static_cast<bool>(user_finish);
    drop_async_delete_finish_ = static_cast<bool>(async_delete_finish);
  }
  return ret;
}

int64_t ObDropAsyncVectorIndexTask::get_serialize_param_size() const
{
  return ObDDLTask::get_serialize_param_size()
       + user_index_.get_serialize_size()
       + async_delete_.get_serialize_size()
       + serialization::encoded_length_i64(user_index_.task_id_)
       + serialization::encoded_length_i64(async_delete_.task_id_)
       + ddl_stmt_str_.get_serialize_size()
       + serialization::encoded_length_i8(static_cast<int8_t>(drop_user_index_finish_))
       + serialization::encoded_length_i8(static_cast<int8_t>(drop_async_delete_finish_));
}

int ObDropAsyncVectorIndexTask::update_task_message(common::ObISQLClient &proxy)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DropAsyncVec");
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
    if (OB_FAIL(ObDDLTaskRecordOperator::update_message(proxy, tenant_id_, task_id_, message))) {
      LOG_WARN("fail to update task message", K(ret), K(task_id_));
    }
  }
  return ret;
}

ObDropAsyncVectorIndexTask::ChildType ObDropAsyncVectorIndexTask::get_next_child_type() const
{
  return !drop_user_index_finish_ ? USER_INDEX
       : (!drop_async_delete_finish_ ? ASYNC_DELETE : NO_CHILD);
}

bool ObDropAsyncVectorIndexTask::can_switch_to_success(
    const bool user_index_exist,
    const bool async_delete_exist) const
{
  // A skipped (-1) shared-delete child intentionally remains visible while
  // another user async index owns it. For an actual leaf DROP (>0), its finish
  // flag is not enough: wait until the dropped schema is no longer visible.
  const bool async_delete_done = !async_delete_exist
      || (is_skipped_child_task_id(async_delete_.task_id_)
          && drop_async_delete_finish_);
  return !user_index_exist && async_delete_done;
}

bool ObDropAsyncVectorIndexTask::reconcile_skipped_async_delete(
    const bool user_index_exist,
    const bool async_delete_exist,
    const bool data_table_exist,
    const bool has_user_async_index,
    bool &need_persist)
{
  need_persist = false;
  if (!user_index_exist
      && async_delete_exist
      && data_table_exist
      && is_skipped_child_task_id(async_delete_.task_id_)
      && drop_async_delete_finish_
      && !has_user_async_index) {
    async_delete_.task_id_ = 0;
    drop_async_delete_finish_ = false;
    need_persist = true;
  }
  return can_switch_to_success(user_index_exist, async_delete_exist);
}

int ObDropAsyncVectorIndexTask::check_switch_succ()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool user_index_exist = true;
  bool async_delete_exist = true;
  bool data_table_exist = true;
  bool has_user_async_index = false;
  bool need_persist = false;
  ObSEArray<const ObSimpleTableSchemaV2 *, OB_MAX_AUX_TABLE_PER_MAIN_TABLE>
      index_schemas;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not initialized", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("global service is null", K(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObDDLUtil::check_tenant_status_normal(GCTX.sql_proxy_, tenant_id_))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret || OB_STANDBY_READ_ONLY == ret) {
      need_retry_ = false;
      LOG_INFO("tenant status is abnormal, stop task", K(ret), K(tenant_id_));
    } else {
      LOG_WARN("fail to check tenant status", K(ret), K(tenant_id_));
    }
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("fail to refresh schema version", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, user_index_.table_id_, user_index_exist))) {
    LOG_WARN("fail to check user async vector index", K(ret), K(user_index_));
  } else {
    if (!async_delete_.is_valid()) {
      async_delete_exist = !drop_async_delete_finish_;
    } else if (OB_FAIL(schema_guard.check_table_exist(
                   tenant_id_, async_delete_.table_id_, async_delete_exist))) {
      LOG_WARN("fail to check async delete index", K(ret), K(async_delete_));
    }
    if (OB_SUCC(ret)
        && !user_index_exist
        && async_delete_exist
        && is_skipped_child_task_id(async_delete_.task_id_)
        && drop_async_delete_finish_) {
      if (OB_FAIL(schema_guard.check_table_exist(
              tenant_id_, object_id_, data_table_exist))) {
        LOG_WARN("fail to check data table", K(ret), K(object_id_));
      } else if (data_table_exist
                 && OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(
                     tenant_id_, object_id_, index_schemas))) {
        LOG_WARN("fail to get current async index membership", K(ret), K(object_id_));
      } else if (data_table_exist
                 && OB_FAIL(ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
                     index_schemas, has_user_async_index))) {
        LOG_WARN("fail to check current user async index membership", K(ret),
            K(object_id_));
      }
    }
    if (OB_SUCC(ret)) {
      const bool can_switch = reconcile_skipped_async_delete(
          user_index_exist, async_delete_exist, data_table_exist,
          has_user_async_index, need_persist);
      if (need_persist && OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
        // The durable message still contains the skipped state. Keep memory
        // consistent so the next retry re-evaluates membership and persists.
        async_delete_.task_id_ = -1;
        drop_async_delete_finish_ = true;
        LOG_WARN("fail to persist shared async delete DROP resubmission",
            K(ret), K(async_delete_));
      } else if (can_switch) {
        task_status_ = ObDDLTaskStatus::SUCCESS;
      } else if (need_persist) {
        LOG_INFO("retry skipped shared async delete DROP after membership changed",
            K(async_delete_), K(object_id_));
      }
    }
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::prepare(const ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not initialized", K(ret));
  } else if (ObDDLTaskStatus::PREPARE != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status does not match PREPARE", K(ret), K(task_status_));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(switch_status(new_status, true, ret))) {
      LOG_WARN("fail to persist async vector DROP status", K(ret), K(new_status));
    }
  } else {
    const int process_ret = ret;
    const ObDDLTaskStatus next_status = ObIDDLTask::in_ddl_retry_white_list(ret)
        ? new_status : ObDDLTaskStatus::FAIL;
    const int tmp_ret = switch_status(next_status, true, process_ret);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to persist async vector DROP error status",
          K(tmp_ret), K(process_ret), K(next_status));
    }
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::check_and_wait_finish()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not initialized", K(ret));
  } else if (ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status does not match child wait", K(ret), K(task_status_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  } else {
    switch (get_next_child_type()) {
      case USER_INDEX:
        ret = send_and_wait_drop_index_task(user_index_, schema_guard, drop_user_index_finish_);
        break;
      case ASYNC_DELETE:
        if ((!async_delete_.is_valid() || async_delete_info_needs_persist_)
            && OB_FAIL(prepare_async_delete_index(schema_guard))) {
          LOG_WARN("fail to prepare shared async delete index cleanup", K(ret), K(object_id_));
        } else if (!drop_async_delete_finish_
                   && OB_FAIL(send_and_wait_drop_index_task(
                       async_delete_, schema_guard, drop_async_delete_finish_))) {
          LOG_WARN("fail to send and wait shared async delete index DROP", K(ret));
        }
        break;
      case NO_CHILD:
        // Child completion only means the leaf task has finished. Keep this
        // composite task in WAIT until check_switch_succ() refreshes schema and
        // confirms the dropped schemas are no longer visible.
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected async vector DROP child type", K(ret), K(task_status_),
                 K(drop_user_index_finish_), K(drop_async_delete_finish_));
        break;
    }
  }
  // Once child DROP starts, keep this durable composite task in WAIT for every
  // child error. send_and_wait_drop_index_task() resets a failed leaf to task
  // id 0, so the next round can resubmit instead of abandoning partial state.
  return ret;
}

int ObDropAsyncVectorIndexTask::prepare_async_delete_index(
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *async_delete_schema = nullptr;
  ObString async_delete_name;
  if (!async_delete_info_needs_persist_) {
    if (async_delete_.is_valid()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("shared async delete index cleanup state does not match", K(ret), K(async_delete_));
    } else if (OB_FAIL(ObAsyncIndexDDLUtil::get_shared_async_delete_index_schema(
                   tenant_id_, object_id_, schema_guard, async_delete_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        drop_async_delete_finish_ = true;
      } else {
        LOG_WARN("fail to find shared async delete index", K(ret), K(tenant_id_), K(object_id_));
      }
    } else if (OB_ISNULL(async_delete_schema)) {
      drop_async_delete_finish_ = true;
    } else if (OB_FAIL(async_delete_schema->get_index_name(async_delete_name))) {
      LOG_WARN("fail to get shared async delete index name", K(ret), KPC(async_delete_schema));
    } else {
      const ObVecIndexDDLChildTaskInfo async_delete(
          async_delete_name, async_delete_schema->get_table_id(), 0);
      if (OB_FAIL(async_delete_.deep_copy_from_other(async_delete, allocator_))) {
        LOG_WARN("fail to save shared async delete index info", K(ret), K(async_delete));
      }
    }
  }
  if (OB_SUCC(ret) && !async_delete_info_needs_persist_) {
    async_delete_info_needs_persist_ = true;
  }
  if (OB_SUCC(ret) && OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
    LOG_WARN("fail to persist shared async delete index cleanup state", K(ret), K(async_delete_));
  } else if (OB_SUCC(ret)) {
    async_delete_info_needs_persist_ = false;
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::send_and_wait_drop_index_task(
    ObVecIndexDDLChildTaskInfo &child_task_info,
    ObSchemaGetterGuard &schema_guard,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  if (0 == child_task_info.task_id_
      && child_task_info.is_valid()
      && OB_FAIL(create_drop_index_task(schema_guard, child_task_info.table_id_,
                                        child_task_info.index_name_, child_task_info.task_id_))) {
    LOG_WARN("fail to create child DROP task", K(ret), K(child_task_info));
  } else if (is_skipped_child_task_id(child_task_info.task_id_)) {
    has_finished = true;
    if (OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
      LOG_WARN("fail to persist skipped child DROP", K(ret), K(child_task_info));
    } else {
      LOG_INFO("child DROP does not need a leaf task", K(child_task_info));
    }
  } else {
    bool need_resubmit = false;
    if (OB_FAIL(wait_drop_index_task_finish(
            child_task_info.task_id_, has_finished, need_resubmit))) {
      const int child_ret = ret;
      if (need_resubmit) {
        child_task_info.task_id_ = 0;
        const int tmp_ret = update_task_message(*GCTX.sql_proxy_);
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to persist child DROP resubmission", K(ret), K(child_ret),
              K(child_task_info));
        }
      }
      LOG_WARN("fail to wait child DROP task", K(ret), K(child_ret),
          K(need_resubmit), K(child_task_info));
    } else if (has_finished && OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
      LOG_WARN("fail to persist child DROP completion", K(ret), K(child_task_info));
    }
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::create_drop_index_task(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t index_table_id,
    const ObString &index_name,
    int64_t &task_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  bool index_exist = false;
  if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("RS RPC proxy is null", K(ret), K(tenant_id_), K(task_id_),
             K(index_table_id));
  } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id_, index_table_id, index_exist))) {
    LOG_WARN("fail to check child index", K(ret), K(index_table_id));
  } else if (!index_exist) {
    task_id = -1;
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, index_table_id, index_schema))) {
    LOG_WARN("fail to get child index schema", K(ret), K(index_table_id));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child index schema is null", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id_, index_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_database_schema(
                 tenant_id_, index_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(index_table_id));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table or database schema is null", K(ret), K(index_table_id),
        KP(data_table_schema), KP(database_schema));
  } else {
    int64_t timeout_us = 0;
    obrpc::ObDropIndexArg arg;
    obrpc::ObDropIndexRes res;
    arg.is_inner_ = true;
    arg.tenant_id_ = tenant_id_;
    arg.exec_tenant_id_ = tenant_id_;
    arg.index_table_id_ = index_table_id;
    arg.session_id_ = data_table_schema->get_session_id();
    arg.index_name_ = index_name;
    arg.table_name_ = data_table_schema->get_table_name();
    arg.database_name_ = database_schema->get_database_name_str();
    arg.index_action_type_ = obrpc::ObIndexArg::DROP_INDEX;
    arg.ddl_stmt_str_ = get_child_ddl_stmt(*index_schema, ddl_stmt_str_);
    arg.is_add_to_scheduler_ = true;
    arg.task_id_ = task_id_;
    arg.is_hidden_ = data_table_schema->is_user_hidden_table();
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
            index_schema->get_all_part_num() + data_table_schema->get_all_part_num(), timeout_us))) {
      LOG_WARN("fail to get ddl rpc timeout", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(timeout_us).drop_index(arg, res))) {
      LOG_WARN("fail to submit child DROP task", K(ret), K(arg), K(res.task_id_));
    } else {
      task_id = res.task_id_;
    }
  }
  return ret;
}

ObString ObDropAsyncVectorIndexTask::get_child_ddl_stmt(
    const ObTableSchema &index_schema,
    const ObString &parent_ddl_stmt)
{
  return index_schema.is_async_delete_index()
      ? ObString::make_empty_string() : parent_ddl_stmt;
}

int ObDropAsyncVectorIndexTask::check_async_delete_index_exist(
    ObSchemaGetterGuard &schema_guard, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (async_delete_.is_valid() && OB_FAIL(schema_guard.check_table_exist(
                 tenant_id_, async_delete_.table_id_, is_exist))) {
    LOG_WARN("fail to check async delete index", K(ret), K(async_delete_));
  }
  return ret;
}

int ObAsyncVectorIndexTaskUtil::check_has_user_async_index(
    const ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas,
    bool &has_user_async_index)
{
  int ret = OB_SUCCESS;
  has_user_async_index = false;
  for (int64_t i = 0;
       OB_SUCC(ret) && !has_user_async_index && i < index_schemas.count(); ++i) {
    const ObSimpleTableSchemaV2 *index_schema = index_schemas.at(i);
    if (OB_ISNULL(index_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema is null", K(ret), K(i));
    } else if (!index_schema->is_user_defined_async_index()
               && !index_schema->is_async_search_def_index()) {
      continue;
    } else if (ObIndexStatus::INDEX_STATUS_INDEX_ERROR
               == index_schema->get_index_status()) {
      continue;
    } else {
      has_user_async_index = true;
    }
  }
  return ret;
}

int ObAsyncVectorIndexTaskUtil::clear_async_index_table_options(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const ObString &ddl_stmt_str,
    ObRootService *root_service,
    bool &has_user_async_index)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  ObSEArray<const ObSimpleTableSchemaV2 *, OB_MAX_AUX_TABLE_PER_MAIN_TABLE>
      index_schemas;
  int64_t refreshed_schema_version = OB_INVALID_VERSION;
  bool schema_updated = false;
  ObDDLSQLTransaction trans(GCTX.schema_service_);
  has_user_async_index = false;
  if (OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == data_table_id
      || OB_ISNULL(root_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index metadata cleanup arguments", K(ret),
        K(tenant_id), K(data_table_id), KP(root_service));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global service is null", K(ret), KP(GCTX.schema_service_),
        KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
                 tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
    LOG_WARN("fail to get schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_INFO("data table schema does not exist", K(ret), K(data_table_id));
  } else if (TABLE_STATE_HIDDEN_OFFLINE_DDL == data_table_schema->get_table_state_flag()) {
    // The offline DDL parent owns the hidden table and removes it on rollback.
    LOG_INFO("skip async vector index metadata cleanup for offline DDL hidden table", K(data_table_id));
  } else if (OB_FAIL(schema_guard.get_index_schemas_with_data_table_id(
                 tenant_id, data_table_id, index_schemas))) {
    LOG_WARN("fail to get index schemas", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(check_has_user_async_index(
                 index_schemas, has_user_async_index))) {
    LOG_WARN("fail to check user async index membership", K(ret), K(data_table_id));
  } else if (has_user_async_index) {
    LOG_INFO("preserve async index table options for existing member",
        K(data_table_id), K(index_schemas));
  } else if (!data_table_schema->has_async_index()
             && data_table_schema->get_async_index_params().empty()) {
    LOG_INFO("async index table options are already clear", K(data_table_id));
  } else {
    ObDDLOperator ddl_operator(*GCTX.schema_service_, *GCTX.sql_proxy_);
    HEAP_VAR(ObTableSchema, new_table_schema) {
      if (OB_FAIL(new_table_schema.assign(*data_table_schema))) {
        LOG_WARN("fail to copy data table schema", K(ret));
      } else {
        new_table_schema.del_table_flag(HAS_ASYNC_INDEX_FLAG);
        if (OB_FAIL(new_table_schema.set_async_index_params(ObString::make_empty_string()))) {
          LOG_WARN("fail to clear async index parameters", K(ret));
        } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id, refreshed_schema_version))) {
          LOG_WARN("fail to start DDL transaction", K(ret));
        } else if (OB_FAIL(ddl_operator.update_table_attribute(
                       new_table_schema, trans, OB_DDL_ALTER_TABLE, &ddl_stmt_str))) {
          LOG_WARN("fail to update data table attributes", K(ret));
        } else if (OB_FAIL(storage::ObTabletAsyncIndexRefreshInfoMdsHelper::register_membership(
                       new_table_schema, trans))) {
          LOG_WARN("fail to remove async index membership", K(ret), K(data_table_id));
        } else {
          schema_updated = true;
        }
      }
    }
  }
  if (trans.is_started()) {
    const int tmp_ret = trans.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to end DDL transaction", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  if (OB_SUCC(ret) && schema_updated) {
    const int64_t timeout_ts = THIS_WORKER.get_timeout_ts();
    ObAddrArray addrs;
    const int tmp_ret = root_service->get_ddl_service().publish_schema(tenant_id, addrs);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "fail to publish schema", K(tenant_id));
    }
    THIS_WORKER.set_timeout_ts(timeout_ts);
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::succ()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool async_delete_exist = false;
  bool has_user_async_index = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  } else if (OB_FAIL(check_async_delete_index_exist(schema_guard, async_delete_exist))) {
    LOG_WARN("fail to check async delete index", K(ret));
  } else if (!async_delete_exist
             && OB_FAIL(ObAsyncVectorIndexTaskUtil::clear_async_index_table_options(
                 tenant_id_, object_id_, ddl_stmt_str_, root_service_, has_user_async_index))) {
    LOG_WARN("fail to clear async index table metadata", K(ret), K(object_id_));
  }
  if (OB_TABLE_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && OB_FAIL(cleanup())) {
    LOG_WARN("fail to clean async vector DROP task", K(ret));
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::fail()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_WARN("fail to clean async vector DROP task", K(ret));
  }
  return ret;
}

int ObDropAsyncVectorIndexTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not initialized", K(ret), K(task_id_), K(task_status_));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("fail to report error code", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("global service is null", K(ret), KP(GCTX.sql_proxy_),
        KP(GCTX.schema_service_));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_table_schema = nullptr;
    ObTableLockOwnerID owner_id;
    ObMySQLTransaction trans;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(object_id_));
    } else if (OB_ISNULL(data_table_schema)) {
      // The enclosing table can be dropped after the composite DROP commits.
      // Its add/drop-index lock disappears with the table, so only remove the task record.
      LOG_INFO("data table schema does not exist, skip async vector DROP unlock", K(object_id_));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(
                   ObLockOwnerType::DEFAULT_OWNER_TYPE, task_id_))) {
      LOG_WARN("fail to construct lock owner", K(ret), K(task_id_));
    } else if (OB_FAIL(ObDDLLock::unlock_for_add_drop_index(
                   *data_table_schema, 0, false, owner_id, trans))) {
      LOG_WARN("fail to unlock async vector DROP", K(ret), K(task_id_));
    }
    if (trans.is_started()) {
      const int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("fail to end unlock transaction", K(ret), K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObDDLTaskRecordOperator::delete_record(
          *GCTX.sql_proxy_, tenant_id_, task_id_))) {
    LOG_WARN("fail to delete task record", K(ret), K(task_id_));
  } else if (OB_SUCC(ret)) {
    need_retry_ = false;
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
