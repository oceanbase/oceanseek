/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "rootserver/ddl_task/ob_drop_fts_index_task.h"
#include "rootserver/ddl_task/ob_async_index_ddl_util.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_operator.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "storage/ddl/ob_ddl_lock.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{

ObDropFTSIndexTask::ObDropFTSIndexTask()
  : ObDDLTask(DDL_DROP_FTS_INDEX),
    root_service_(nullptr),
    rowkey_doc_(),
    doc_rowkey_(),
    domain_index_(),
    fts_doc_word_(),
    async_delete_(),
    drop_domain_index_finish_(false),
    drop_doc_word_index_finish_(false),
    drop_async_delete_index_finish_(false),
    drop_doc_rowkey_index_finish_(false),
    drop_rowkey_doc_index_finish_(false)
{
}

ObDropFTSIndexTask::~ObDropFTSIndexTask()
{
}

int ObDropFTSIndexTask::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    const uint64_t data_table_id,
    const ObDDLType ddl_type,
    const ObFTSDDLChildTaskInfo &rowkey_doc,
    const ObFTSDDLChildTaskInfo &doc_rowkey,
    const ObFTSDDLChildTaskInfo &domain_index,
    const ObFTSDDLChildTaskInfo &fts_doc_word,
    const ObFTSDDLChildTaskInfo &async_delete,
    const ObString &ddl_stmt_str,
    const int64_t schema_version,
    const int64_t consumer_group_id,
    const int64_t target_object_id)
{
  int ret = OB_SUCCESS;
  const bool is_fts_task = ddl_type == DDL_DROP_FTS_INDEX;
  const bool is_async_fts_task = ddl_type == DDL_DROP_ASYNC_FTS_INDEX;

  // remove !domain_index.is_valid() check, as create fts index may not build all index table success
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id
               || task_id <= 0
               || OB_INVALID_ID == data_table_id
               || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(task_id), K(data_table_id),
        K(domain_index), K(schema_version));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service is null", K(ret));
  } else if (OB_FAIL(rowkey_doc_.deep_copy_from_other(rowkey_doc, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(rowkey_doc));
  } else if (OB_FAIL(doc_rowkey_.deep_copy_from_other(doc_rowkey, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(doc_rowkey));
  } else if (OB_FAIL(domain_index_.deep_copy_from_other(domain_index, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(domain_index));
  } else if (is_fts_task && OB_FAIL(fts_doc_word_.deep_copy_from_other(fts_doc_word, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(fts_doc_word));
  } else if (is_async_fts_task && OB_FAIL(async_delete_.deep_copy_from_other(async_delete, allocator_))) {
    LOG_WARN("fail to deep copy from other", K(ret), K(async_delete));
  } else if (OB_FAIL(set_ddl_stmt_str(ddl_stmt_str))) {
    LOG_WARN("fail to deep copy drop index arg", K(ret));
  } else {
    task_type_ = ddl_type;
    set_gmt_create(ObTimeUtility::current_time());
    tenant_id_ = tenant_id;
    object_id_ = data_table_id;
    target_object_id_ = target_object_id; // not use this id
    schema_version_ = schema_version;
    task_id_ = task_id;
    parent_task_id_ = 0; // no parent task
    consumer_group_id_ = consumer_group_id;
    task_version_ = OB_DROP_FTS_INDEX_TASK_VERSION;
    dst_tenant_id_ = tenant_id;
    dst_schema_version_ = schema_version;
    is_inited_ = true;
  }
  return ret;
}

int ObDropFTSIndexTask::init(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(!task_record.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_record));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, root service is nullptr", K(ret));
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
    pos = 0;
    if (OB_ISNULL(task_record.message_.ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, task record message is nullptr", K(ret), K(task_record));
    } else if (OB_FAIL(deserialize_params_from_message(task_record.tenant_id_, task_record.message_.ptr(),
            task_record.message_.length(), pos))) {
      LOG_WARN("deserialize params from message failed", K(ret));
    } else {
      is_inited_ = true;
      // set up span during recover task
      ddl_tracing_.open_for_recovery();
    }
  }
  return ret;
}

int ObDropFTSIndexTask::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("[ASYNC_DROP] process enter", K(task_id_), K(task_status_), K(is_inited_), K(need_retry()),
      K(domain_index_), K(async_delete_), K(drop_domain_index_finish_), K(drop_async_delete_index_finish_));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDropIndexTask has not been inited", K(ret));
  } else if (!need_retry()) {
    // task is done
  } else if (OB_FAIL(check_switch_succ())) {
    LOG_WARN("check need retry failed", K(ret));
  } else {
    ddl_tracing_.restore_span_hierarchy();
    const ObDDLTaskStatus status = static_cast<ObDDLTaskStatus>(task_status_);
    LOG_DEBUG("[ASYNC_DROP] after check_switch_succ", K(task_id_), K(task_status_));
    switch (status) {
      case ObDDLTaskStatus::PREPARE:
        if (OB_FAIL(prepare(WAIT_CHILD_TASK_FINISH))) {
          LOG_WARN("fail to prepare", K(ret));
        }
        break;
      case ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH:
        if (OB_FAIL(check_and_wait_finish(SUCCESS))) {
          LOG_WARN("fail to check and wait task", K(ret));
        }
        break;
      case ObDDLTaskStatus::SUCCESS:
        if (OB_FAIL(succ())) {
          LOG_WARN("do succ procedure failed", K(ret));
        }
        break;
      case ObDDLTaskStatus::FAIL:
        if (OB_FAIL(fail())) {
          LOG_WARN("do fail procedure failed", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, task status is not valid", K(ret), K(task_status_));
    }
    ddl_tracing_.release_span_hierarchy();
  }
  return ret;
}

int ObDropFTSIndexTask::serialize_params_to_message(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int8_t drop_domain_index_finish = static_cast<int8_t>(drop_domain_index_finish_);
  int8_t drop_doc_word_index_finish = static_cast<int8_t>(drop_doc_word_index_finish_);
  int8_t drop_async_delete_index_finish = static_cast<int8_t>(drop_async_delete_index_finish_);
  int8_t drop_doc_rowkey_index_finish = static_cast<int8_t>(drop_doc_rowkey_index_finish_);
  int8_t drop_rowkey_doc_index_finish = static_cast<int8_t>(drop_rowkey_doc_index_finish_);
  if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::serialize_params_to_message(buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::serialize", K(ret));
  } else if (OB_FAIL(rowkey_doc_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize aux rowkey doc table info", K(ret), K(rowkey_doc_));
  } else if (OB_FAIL(doc_rowkey_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize aux doc rowkey table info", K(ret), K(doc_rowkey_));
  } else if (OB_FAIL(domain_index_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize aux fts index table info", K(ret), K(domain_index_));
  } else if (OB_FAIL(fts_doc_word_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize aux doc word aux table info", K(ret), K(fts_doc_word_));
  } else if (task_version_ >= OB_DROP_FTS_INDEX_TASK_VERSION_V2 && OB_FAIL(async_delete_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize async delete table info", K(ret), K(async_delete_));
  } else if (OB_FAIL(ddl_stmt_str_.serialize(buf, buf_size, pos))) {
    LOG_WARN("fail to serialize ddl stmt string", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_size,
                                              pos,
                                              drop_domain_index_finish))) {
    LOG_WARN("fail to serialize drop domain index finish", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_size,
                                              pos,
                                              drop_doc_word_index_finish))) {
    LOG_WARN("fail to serialize drop doc word index finish", K(ret));
  } else if (task_version_ >= OB_DROP_FTS_INDEX_TASK_VERSION_V2
             && OB_FAIL(serialization::encode_i8(buf,
                                                 buf_size,
                                                 pos,
                                                 drop_async_delete_index_finish))) {
    LOG_WARN("fail to serialize drop async delete index finish", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_size,
                                              pos,
                                              drop_doc_rowkey_index_finish))) {
    LOG_WARN("fail to serialize drop doc rowkey index finish", K(ret));
  } else if (OB_FAIL(serialization::encode_i8(buf,
                                              buf_size,
                                              pos,
                                              drop_rowkey_doc_index_finish))) {
    LOG_WARN("fail to serialize drop rowkey doc index finish", K(ret));
  }
  return ret;
}

int ObDropFTSIndexTask::deserialize_params_from_message(
    const uint64_t tenant_id,
    const char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObFTSDDLChildTaskInfo tmp_info;
  ObString tmp_ddl_stmt_str;
  int8_t drop_domain_index_finish = 0;
  int8_t drop_doc_word_index_finish = 0;
  int8_t drop_async_delete_index_finish = 0;
  int8_t drop_doc_rowkey_index_finish = 0;
  int8_t drop_rowkey_doc_index_finish = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), KP(buf), K(buf_size));
  } else if (OB_FAIL(ObDDLTask::deserialize_params_from_message(tenant_id, buf, buf_size, pos))) {
    LOG_WARN("fail to ObDDLTask::deserialize", K(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize aux rowkey doc table info", K(ret));
  } else if (OB_FAIL(rowkey_doc_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize aux doc rowkey table info", K(ret));
  } else if (OB_FAIL(doc_rowkey_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize aux fts index table info", K(ret));
  } else if (OB_FAIL(domain_index_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize aux doc word table info", K(ret));
  } else if (OB_FAIL(fts_doc_word_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (task_version_ >= OB_DROP_FTS_INDEX_TASK_VERSION_V2 && OB_FAIL(tmp_info.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize async delete table info", K(ret));
  } else if (task_version_ >= OB_DROP_FTS_INDEX_TASK_VERSION_V2 && OB_FAIL(async_delete_.deep_copy_from_other(tmp_info, allocator_))) {
    LOG_WARN("fail to deep copy from tmp info", K(ret), K(tmp_info));
  } else if (OB_UNLIKELY(pos >= buf_size)) {
    // The end of the message has been reached. It is an old version message without drop index arg.
    // just skip.
  } else if (OB_FAIL(tmp_ddl_stmt_str.deserialize(buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize drop index arg", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, tmp_ddl_stmt_str, ddl_stmt_str_))) {
    LOG_WARN("fail to copy ddl stmt string", K(ret), K(tmp_ddl_stmt_str));
  } else if (OB_UNLIKELY(pos >= buf_size)) {
    // The end of the message has been reached. It is an old version message without drop index arg.
    // just skip.
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              buf_size,
                                              pos,
                                              &drop_domain_index_finish))) {
      LOG_WARN("fail to deserialize drop domain index finish", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              buf_size,
                                              pos,
                                              &drop_doc_word_index_finish))) {
      LOG_WARN("fail to deserialize drop doc word index finish", K(ret));
  } else if (task_version_ >= OB_DROP_FTS_INDEX_TASK_VERSION_V2
             && OB_FAIL(serialization::decode_i8(buf,
                                                 buf_size,
                                                 pos,
                                                 &drop_async_delete_index_finish))) {
      LOG_WARN("fail to deserialize drop async delete index finish", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              buf_size,
                                              pos,
                                              &drop_doc_rowkey_index_finish))) {
      LOG_WARN("fail to deserialize drop doc rowkey index finish", K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf,
                                              buf_size,
                                              pos,
                                              &drop_rowkey_doc_index_finish))) {
      LOG_WARN("fail to deserialize drop rowkey doc index finish", K(ret));
  } else {
    drop_domain_index_finish_ = static_cast<bool>(drop_domain_index_finish);
    drop_doc_word_index_finish_ = static_cast<bool>(drop_doc_word_index_finish);
    drop_async_delete_index_finish_ = static_cast<bool>(drop_async_delete_index_finish);
    drop_doc_rowkey_index_finish_ = static_cast<bool>(drop_doc_rowkey_index_finish);
    drop_rowkey_doc_index_finish_ = static_cast<bool>(drop_rowkey_doc_index_finish);
  }
  return ret;
}

int64_t ObDropFTSIndexTask::get_serialize_param_size() const
{
  int8_t drop_domain_index_finish = static_cast<int8_t>(drop_domain_index_finish_);
  int8_t drop_doc_word_index_finish = static_cast<int8_t>(drop_doc_word_index_finish_);
  int8_t drop_async_delete_index_finish = static_cast<int8_t>(drop_async_delete_index_finish_);
  int8_t drop_doc_rowkey_index_finish = static_cast<int8_t>(drop_doc_rowkey_index_finish_);
  int8_t drop_rowkey_doc_index_finish = static_cast<int8_t>(drop_rowkey_doc_index_finish_);
  return ObDDLTask::get_serialize_param_size()
       + rowkey_doc_.get_serialize_size()
       + doc_rowkey_.get_serialize_size()
       + domain_index_.get_serialize_size()
       + fts_doc_word_.get_serialize_size()
       + (task_version_ >= OB_DROP_FTS_INDEX_TASK_VERSION_V2 ? async_delete_.get_serialize_size() : 0)
       + ddl_stmt_str_.get_serialize_size()
       + serialization::encoded_length_i8(drop_domain_index_finish)
       + serialization::encoded_length_i8(drop_doc_word_index_finish)
       + (task_version_ >= OB_DROP_FTS_INDEX_TASK_VERSION_V2 ? serialization::encoded_length_i8(drop_async_delete_index_finish) : 0)
       + serialization::encoded_length_i8(drop_doc_rowkey_index_finish)
       + serialization::encoded_length_i8(drop_rowkey_doc_index_finish);
}

int ObDropFTSIndexTask::update_task_message(common::ObISQLClient &proxy)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  ObString msg;
  common::ObArenaAllocator allocator("ObDropFTSIndex");
  const int64_t serialize_param_size = get_serialize_param_size();

  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(serialize_param_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", KR(ret), K(serialize_param_size));
  } else if (OB_FAIL(serialize_params_to_message(buf, serialize_param_size, pos))) {
    LOG_WARN("failed to serialize params to message", KR(ret));
  } else {
    msg.assign(buf, serialize_param_size);
    if (OB_FAIL(ObDDLTaskRecordOperator::update_message(proxy, tenant_id_, task_id_, msg))) {
      LOG_WARN("failed to update message", KR(ret));
    }
  }
  return ret;
}

int ObDropFTSIndexTask::check_switch_succ()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_domain_index_exist = false;
  bool is_doc_word_exist = false;
  bool is_rowkey_doc_exist = false;
  bool is_doc_rowkey_exist = false;
  bool is_async_delete_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hasn't initialized", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", KR(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObDDLUtil::check_tenant_status_normal(GCTX.sql_proxy_, tenant_id_))) {
    if (OB_TENANT_HAS_BEEN_DROPPED == ret || OB_STANDBY_READ_ONLY == ret) {
      need_retry_ = false;
      LOG_INFO("tenant status is abnormal, exit anyway", K(ret), K(tenant_id_));
    } else {
      LOG_WARN("check tenant status failed", K(ret), K(tenant_id_));
    }
  } else if (OB_FAIL(refresh_schema_version())) {
    LOG_WARN("refresh schema version failed", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id_));
  } else if (domain_index_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, domain_index_.table_id_, is_domain_index_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(domain_index_));
  } else if (fts_doc_word_.is_valid() && is_fts_task()
          && OB_FAIL(schema_guard.check_table_exist(
              tenant_id_, fts_doc_word_.table_id_, is_doc_word_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(fts_doc_word_));
  } else if (doc_rowkey_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, doc_rowkey_.table_id_, is_doc_rowkey_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(doc_rowkey_));
  } else if (rowkey_doc_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, rowkey_doc_.table_id_, is_rowkey_doc_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(rowkey_doc_));
  } else if (is_async_fts_task() && !async_delete_.is_valid()) {
    // 共享表需要在当前 Def 删除后进行第二阶段判断，未持久化该判断前不能提前成功。
    is_async_delete_exist = true;
  } else if (async_delete_.is_valid()
          && OB_FAIL(schema_guard.check_table_exist(tenant_id_, async_delete_.table_id_, is_async_delete_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(async_delete_));
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("[ASYNC_DROP] check_switch_succ table exist check",
        K(task_id_), K(is_domain_index_exist), K(is_doc_word_exist),
        K(is_rowkey_doc_exist), K(is_doc_rowkey_exist), K(is_async_delete_exist),
        K(domain_index_.table_id_), K(async_delete_.table_id_));
  }
  if (OB_SUCC(ret) && !is_domain_index_exist && !is_doc_word_exist && !is_rowkey_doc_exist && !is_doc_rowkey_exist
          && !is_async_delete_exist) {
    uint64_t doc_id_col_id = OB_INVALID_ID;
    uint64_t ft_col_id = OB_INVALID_ID;
    const ObTableSchema *data_table_schema = nullptr;
    bool has_fts_index = false;
    bool has_multivalue_index = false;

    int tmp_ret = schema_guard.get_table_schema(tenant_id_, object_id_, data_table_schema);
    if (tmp_ret != OB_SUCCESS) {
    } else if (OB_ISNULL(data_table_schema)) {
    } else if ((tmp_ret = data_table_schema->check_has_fts_index_aux(schema_guard, has_fts_index))!= OB_SUCCESS) {
    } else if ((tmp_ret = data_table_schema->check_has_multivalue_index_aux(schema_guard, has_multivalue_index))!= OB_SUCCESS) {
    } else if (!has_fts_index && !has_multivalue_index
      && (tmp_ret = data_table_schema->get_fulltext_column_ids(doc_id_col_id, ft_col_id)) != OB_SUCCESS) {
    }
    // the main table exists, and the fts and multivalue indexes no longer exist.
    // the doc id column is removed from the main table,
    // otherwise there may be a small probability of dml scenarios where the schema is not refreshed,
    // resulting in an additional doc id column in the cg stage.
    LOG_DEBUG("[ASYNC_DROP] check_switch_succ all tables gone, checking doc_id",
        K(task_id_), K(tmp_ret), KP(data_table_schema), K(doc_id_col_id), K(has_fts_index), K(has_multivalue_index));
    if (tmp_ret == OB_SUCCESS
        && OB_NOT_NULL(data_table_schema)
        && doc_id_col_id != OB_INVALID_ID) {
      LOG_DEBUG("[ASYNC_DROP] check_switch_succ blocked by doc_id column", K(task_id_), K(doc_id_col_id));
    } else {
      LOG_INFO("[ASYNC_DROP] check_switch_succ -> SUCCESS", K(task_id_));
      task_status_ = ObDDLTaskStatus::SUCCESS;
    }
  }
  return ret;
}

int ObDropFTSIndexTask::prepare(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::PREPARE != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else {
    state_finished = true;
  }

  DEBUG_SYNC(DROP_FTS_INDEX_PREPARE_STATUS);

  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status = new_status;
    if (OB_FAIL(ret)) {
      next_status = ObIDDLTask::in_ddl_retry_white_list(ret) ? next_status : ObDDLTaskStatus::FAIL;
    }
    (void)switch_status(next_status, true, ret);
  }
  return ret;
}

int ObDropFTSIndexTask::check_and_wait_finish(const share::ObDDLTaskStatus &new_status)
{
  int ret = OB_SUCCESS;
  bool state_finished = false;
  ObSchemaGetterGuard schema_guard;
  LOG_DEBUG("[ASYNC_DROP] check_and_wait_finish enter", K(task_id_), K(task_status_),
      K(drop_domain_index_finish_), K(drop_async_delete_index_finish_),
      K(drop_doc_word_index_finish_), K(is_async_fts_task()),
      K(domain_index_), K(async_delete_));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ObDDLTaskStatus::WAIT_CHILD_TASK_FINISH != task_status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("task status not match", K(ret), K(task_status_));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
  }
  if (OB_SUCC(ret)) {
    if (!drop_domain_index_finish_) {
      LOG_DEBUG("[ASYNC_DROP] waiting for domain_index drop", K(task_id_), K(domain_index_));
      if (OB_FAIL(send_and_wait_drop_index_task(domain_index_, schema_guard, drop_domain_index_finish_))) {
          LOG_WARN("Failed to send and wait drop index task for domain_index", K(ret));
      }
    } else if (is_async_fts_task()) {
      if (!drop_async_delete_index_finish_) {
        LOG_DEBUG("[ASYNC_DROP] waiting for async_delete drop", K(task_id_), K(async_delete_));
        if (!async_delete_.is_valid()
            && OB_FAIL(prepare_async_delete_index(schema_guard))) {
          LOG_WARN("failed to prepare shared async delete index cleanup", K(ret), K(object_id_));
        } else if (!drop_async_delete_index_finish_
                   && OB_FAIL(send_and_wait_drop_index_task(
                       async_delete_, schema_guard, drop_async_delete_index_finish_))) {
          LOG_WARN("Failed to send and wait drop index task for async_delete", K(ret));
        }
      } else {
        LOG_INFO("[ASYNC_DROP] all child tasks finished, state_finished=true", K(task_id_));
        state_finished = true;
      }
    } else if (!drop_doc_word_index_finish_) {
      if (OB_FAIL(send_and_wait_drop_index_task(fts_doc_word_, schema_guard, drop_doc_word_index_finish_))) {
          LOG_WARN("Failed to send and wait drop index task for fts_doc_word", K(ret));
      }
    } else if (!drop_doc_rowkey_index_finish_) {
      if (OB_FAIL(send_and_wait_drop_index_task(doc_rowkey_, schema_guard, drop_doc_rowkey_index_finish_))) {
          LOG_WARN("Failed to send and wait drop index task for doc_rowkey", K(ret));
      }
    } else if (!drop_rowkey_doc_index_finish_) {
      if (OB_FAIL(send_and_wait_drop_index_task(rowkey_doc_, schema_guard, drop_rowkey_doc_index_finish_))) {
          LOG_WARN("Failed to send and wait drop index task for rowkey_doc", K(ret));
      }
    } else {
      state_finished = true;
    }
  }
  if (state_finished || OB_FAIL(ret)) {
    ObDDLTaskStatus next_status = new_status;
    if (OB_FAIL(ret)) {
      next_status = ObIDDLTask::in_ddl_retry_white_list(ret) ? next_status : ObDDLTaskStatus::FAIL;
    }
    (void)switch_status(next_status, true, ret);
  }
  return ret;
}

int ObDropFTSIndexTask::prepare_async_delete_index(ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *async_delete_schema = nullptr;
  ObString async_delete_name;
  if (!is_async_fts_task() || async_delete_.is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("shared async delete index cleanup state does not match", K(ret), K(task_type_), K(async_delete_));
  } else if (OB_FAIL(ObAsyncIndexDDLUtil::get_shared_async_delete_index_schema(
      tenant_id_, object_id_, schema_guard, async_delete_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      drop_async_delete_index_finish_ = true;
    } else {
      LOG_WARN("failed to find shared async delete index", K(ret), K(tenant_id_), K(object_id_));
    }
  } else if (OB_ISNULL(async_delete_schema)) {
    drop_async_delete_index_finish_ = true;
  } else if (OB_FAIL(async_delete_schema->get_index_name(async_delete_name))) {
    LOG_WARN("failed to get shared async delete index name", K(ret), KPC(async_delete_schema));
  } else {
    const ObFTSDDLChildTaskInfo async_delete(
        async_delete_name, async_delete_schema->get_table_id(), 0/*task_id*/);
    if (OB_FAIL(async_delete_.deep_copy_from_other(async_delete, allocator_))) {
      LOG_WARN("failed to save shared async delete index info", K(ret), K(async_delete));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
    if (async_delete_.is_valid()) {
      async_delete_.index_name_.reset();
      async_delete_.table_id_ = OB_INVALID_ID;
      async_delete_.task_id_ = 0;
    } else {
      drop_async_delete_index_finish_ = false;
    }
    LOG_WARN("failed to persist shared async delete index cleanup state", K(ret), K(async_delete_));
  }
  return ret;
}

int ObDropFTSIndexTask::check_drop_index_finish(
    const uint64_t tenant_id,
    const int64_t task_id,
    const int64_t table_id,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  const ObAddr unused_addr;
  int64_t unused_user_msg_len = 0;
  share::ObDDLErrorMessageTableOperator::ObBuildDDLErrorMessage error_message;
  has_finished = false;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || task_id <= 0 || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), K(tenant_id), K(task_id), K(table_id));
  } else if (OB_FAIL(share::ObDDLErrorMessageTableOperator::get_ddl_error_message(tenant_id,
                                                                                  task_id,
                                                                                  -1/*target_object_id*/,
                                                                                  table_id,
                                                                                  *GCTX.sql_proxy_,
                                                                                  error_message,
                                                                                  unused_user_msg_len))) {

    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("the drop index task not completed", K(ret), K(tenant_id), K(task_id), K(table_id), K(has_finished));
    } else {
      LOG_WARN("fail to get ddl error message", K(ret), K(tenant_id), K(task_id), K(table_id));
    }
  } else {
    ret = error_message.ret_code_;
    has_finished = true;
    if (OB_SUCCESS == ret && OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
      LOG_WARN("fail to update drop fulltext index task message", K(ret));
    }
    LOG_INFO("wait drop index finish", K(ret), K(tenant_id), K(task_id), K(table_id), K(has_finished));
  }
  return ret;
}

int ObDropFTSIndexTask::wait_drop_child_task_finish(
    const ObFTSDDLChildTaskInfo &child_task_info,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  has_finished = false;
  if (-1 == child_task_info.task_id_ || OB_INVALID_ID == child_task_info.table_id_) {
    has_finished = true;
  } else if (OB_FAIL(check_drop_index_finish(tenant_id_, child_task_info.task_id_, child_task_info.table_id_, has_finished))) {
    LOG_WARN("fail to check fts index child task finish", K(ret));
  } else if (!has_finished) {
    LOG_INFO("the child task hasn't been finished", K(ret), K(tenant_id_), K(child_task_info));
  }
  return ret;
}

int ObDropFTSIndexTask::send_and_wait_drop_index_task(
    ObFTSDDLChildTaskInfo &child_task_info,
    ObSchemaGetterGuard &schema_guard,
    bool &has_finished)
{
  int ret = OB_SUCCESS;
  if (0 == child_task_info.task_id_ &&
      child_task_info.is_valid() &&
      OB_FAIL(create_drop_index_task(schema_guard,
                                     child_task_info.table_id_,
                                     child_task_info.index_name_,
                                     child_task_info.task_id_))) {
      LOG_WARN("fail to create drop index task", K(ret), K(child_task_info));
  } else if (-1 == child_task_info.task_id_) {
    if (OB_FAIL(update_task_message(*GCTX.sql_proxy_))) {
      LOG_WARN("fail to update drop fulltext index task message", K(ret), K(child_task_info));
    } else {
      has_finished = true;
      LOG_INFO("the child index table do not need to be deleted", K(ret), K(child_task_info));
    }
  } else if (0 == child_task_info.task_id_) {
    // by pass
    has_finished = true;
    LOG_INFO("the rowkey doc/doc rowkey index table do not need to be deleted, \
        there are still other full-text indexes", K(ret), K(child_task_info));
  } else if (OB_FAIL(wait_drop_child_task_finish(child_task_info, has_finished))) {
    LOG_WARN("fail to wait drop domain child task finish", K(ret), K(child_task_info));
  }
  return ret;
}

int ObDropFTSIndexTask::create_drop_index_task(
    share::schema::ObSchemaGetterGuard &guard,
    const uint64_t index_tid,
    const common::ObString &index_name,
    int64_t &task_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObSqlString drop_index_sql;
  bool is_index_exist = false;
  if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.rs_rpc_proxy_));
  } else if (OB_INVALID_ID == index_tid) {
    // nothing to do, just by pass.
    task_id = -1;
  } else if (OB_UNLIKELY(index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(index_name));
  } else if (OB_FAIL(guard.check_table_exist(tenant_id_, index_tid, is_index_exist))) {
    LOG_WARN("fail to check table exist", K(ret), K(tenant_id_), K(index_tid));
  } else if (!is_index_exist) {
    // nothing to do, just by pass.
    task_id = -1;
  } else if (OB_FAIL(guard.get_table_schema(tenant_id_, index_tid, index_schema))) {
    LOG_WARN("fail to get index table schema", K(ret), K(tenant_id_), K(index_tid));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, index schema is nullptr", K(ret), KP(index_schema));
  } else if (index_schema->is_async_delete_index() && index_schema->is_in_deleting()) {
    ret = OB_EAGAIN;
    LOG_INFO("shared async delete index is being dropped by another task",
        K(ret), K(tenant_id_), K(index_tid), KPC(index_schema));
  } else if (OB_FAIL(guard.get_database_schema(tenant_id_, index_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", K(ret), K(index_schema->get_database_id()));
  } else if (OB_FAIL(guard.get_table_schema(tenant_id_, index_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("fail to get data table schema", K(ret), K(index_schema->get_data_table_id()));
  } else if (OB_UNLIKELY(nullptr == database_schema || nullptr == data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, schema is nullptr", K(ret), KP(database_schema), KP(data_table_schema));
  } else if ((index_schema->is_fts_index_aux() || index_schema->is_async_fts_index())
          && OB_FAIL(drop_index_sql.assign(ddl_stmt_str_))) {
    LOG_WARN("fail to assign drop index sql", K(ret));
  } else {
    int64_t ddl_rpc_timeout_us = 0;
    obrpc::ObDropIndexArg arg;
    obrpc::ObDropIndexRes res;
    arg.is_inner_            = true;
    arg.tenant_id_           = tenant_id_;
    arg.exec_tenant_id_      = tenant_id_;
    arg.index_table_id_      = index_tid;
    arg.session_id_          = data_table_schema->get_session_id();
    arg.index_name_          = index_name;
    arg.table_name_          = data_table_schema->get_table_name();
    arg.database_name_       = database_schema->get_database_name_str();
    arg.index_action_type_   = obrpc::ObIndexArg::DROP_INDEX;
    arg.ddl_stmt_str_        = nullptr;
    arg.is_add_to_scheduler_ = true;
    arg.task_id_             = task_id_;
    arg.is_hidden_           = data_table_schema->is_user_hidden_table();
    if (index_schema->is_fts_index_aux() || index_schema->is_async_fts_index()) {
      arg.ddl_stmt_str_ = drop_index_sql.string();
    }
    if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(
            index_schema->get_all_part_num() + data_table_schema->get_all_part_num(), ddl_rpc_timeout_us))) {
      LOG_WARN("fail to get ddl rpc timeout", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(ddl_rpc_timeout_us).drop_index(arg, res))) {
      LOG_WARN("fail to drop index", K(ret), K(ddl_rpc_timeout_us), K(arg), K(res.task_id_));
    } else {
      task_id = res.task_id_;
    }
    LOG_INFO("drop index", K(ret), K(index_tid), K(index_name), K(task_id),
        "data table name", data_table_schema->get_table_name_str(),
        "database name", database_schema->get_database_name_str(),
        K(drop_index_sql.ptr()));
  }

  DEBUG_SYNC(AFTER_DROP_FTS_SUBMIT_SUBTASK);

  return ret;
}

int ObDropFTSIndexTask::succ()
{
  int ret = OB_SUCCESS;
  LOG_INFO("[ASYNC_DROP] succ() enter", K(task_id_), K(is_async_fts_task()), K(object_id_));
  if (is_async_fts_task()) {
    ObSchemaGetterGuard schema_guard;
    bool can_clear = false;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
    } else if (OB_FAIL(ObAsyncIndexDDLUtil::check_can_clear_async_index_options(
        tenant_id_, object_id_, schema_guard, can_clear))) {
      LOG_WARN("failed to check async index schemas", K(ret), K(object_id_));
    } else if (can_clear && OB_FAIL(ObAsyncIndexDDLUtil::clear_async_index_table_options(
        root_service_, tenant_id_, object_id_, task_id_, ddl_stmt_str_))) {
      LOG_WARN("failed to clear async index table options", K(ret), K(object_id_));
    }
    if (OB_FAIL(ret) && OB_TABLE_NOT_EXIST == ret) {
      LOG_INFO("[ASYNC_DROP] succ() data table not exist, skip clear", K(ret), K(object_id_));
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret) && ObIDDLTask::in_ddl_retry_white_list(ret)) {
      LOG_WARN("[ASYNC_DROP] succ() async clear failed but retryable", K(ret), K(object_id_));
    }
    LOG_INFO("[ASYNC_DROP] succ() async clear result", K(ret), K(can_clear), K(object_id_));
  }
  if (OB_SUCC(ret) && OB_FAIL(cleanup())) {
    LOG_WARN("cleanup task failed", K(ret));
  }
  return ret;
}

int ObDropFTSIndexTask::fail()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup())) {
    LOG_WARN("cleanup task failed", K(ret));
  }
  return ret;
}

int ObDropFTSIndexTask::cleanup_impl()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("[ASYNC_DROP] cleanup_impl enter", K(task_id_), K(task_type_), K(object_id_));
  ObString unused_str;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(report_error_code(unused_str))) {
    LOG_WARN("report error code failed", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_table_schema = nullptr;
    ObTableLockOwnerID owner_id;
    ObMySQLTransaction trans;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_,
                                                              schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_,
                                                     object_id_,
                                                     data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(object_id_));
    } else if (OB_UNLIKELY(nullptr == data_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data table schema is nullptr", K(ret), KP(data_table_schema));
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(owner_id.convert_from_value(ObLockOwnerType::DEFAULT_OWNER_TYPE, task_id_))) {
      LOG_WARN("fail to get owner id", K(ret), K(task_id_));
    } else if (OB_FAIL(ObDDLLock::unlock_for_add_drop_index(*data_table_schema,
                                                            0 /* index_table_id */, // not support global fulltext index
                                                            false /* is_global_index = false */,
                                                            owner_id,
                                                            trans))) {
      LOG_WARN("failed to unlock online ddl lock", K(ret));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  if (FAILEDx(ObDDLTaskRecordOperator::delete_record(*GCTX.sql_proxy_, tenant_id_, task_id_))) {
    LOG_WARN("delete task record failed", K(ret), K(task_id_), K(schema_version_));
  } else {
    need_retry_ = false;      // clean succ, stop the task
  }
  LOG_INFO("clean task finished", K(ret), K(*this));
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
