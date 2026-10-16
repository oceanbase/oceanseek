/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/direct_load/ob_direct_load_stream_append_op.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_forward_rpc_proxy.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "share/ob_common_id.h"
#include "share/ob_server_struct.h"
#include "storage/ob_common_id_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObDirectLoadStreamAppendOpInput, ObTableInsertOpInput));

OB_DEF_SERIALIZE(ObDirectLoadStreamAppendSpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObDirectLoadStreamAppendSpec, ObTableInsertSpec));
  OB_UNIS_ENCODE(stream_id_);
  OB_UNIS_ENCODE(target_schema_version_);
  OB_UNIS_ENCODE(target_column_ids_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDirectLoadStreamAppendSpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObDirectLoadStreamAppendSpec, ObTableInsertSpec));
  OB_UNIS_DECODE(stream_id_);
  OB_UNIS_DECODE(target_schema_version_);
  OB_UNIS_DECODE(target_column_ids_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDirectLoadStreamAppendSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObDirectLoadStreamAppendSpec, ObTableInsertSpec));
  OB_UNIS_ADD_LEN(stream_id_);
  OB_UNIS_ADD_LEN(target_schema_version_);
  OB_UNIS_ADD_LEN(target_column_ids_);
  return len;
}

ObDirectLoadStreamAppendOp::ObDirectLoadStreamAppendOp(ObExecContext &ctx,
                                                       const ObOpSpec &spec,
                                                       ObOpInput *input)
  : ObTableInsertOp(ctx, spec, input),
    queue_desc_(),
    queue_handle_(),
    append_handle_(),
    chunk_writer_(),
    statement_id_(common::OB_INVALID_ID),
    absolute_deadline_(0),
    remote_append_started_(false),
    appended_row_count_(0),
    queue_touched_(false),
    stream_failed_reported_(false)
{
}

ObDirectLoadStreamAppendOp::~ObDirectLoadStreamAppendOp()
{
  if (is_append_registered_()) {
    const int end_ret = finish_append_(OB_CANCELED);
    if (OB_SUCCESS != end_ret) {
      LOG_ERROR_RET(end_ret, "failed to release direct load stream append registration on destroy",
                    K(end_ret), K_(queue_touched));
    }
  }
}

int ObDirectLoadStreamAppendOp::inner_open()
{
  int ret = OB_SUCCESS;
  const ObInsCtDef *primary_ins_ctdef = nullptr;
  NG_TRACE(insert_open);
  appended_row_count_ = 0;
  queue_touched_ = false;
  stream_failed_reported_ = false;
  if (OB_UNLIKELY(!observer::ObTableLoadService::is_direct_load_stream_enabled())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Stream Direct Load when disabled is");
    LOG_WARN("Stream Direct Load is disabled", KR(ret));
  } else if (OB_FAIL(ObTableModifyOp::inner_open())) {
    LOG_WARN("inner open ObTableModifyOp failed", K(ret));
  } else if (OB_FAIL(get_primary_insert_ctdef_(primary_ins_ctdef))) {
    LOG_WARN("failed to get direct load stream append primary insert ctdef", K(ret));
  } else if (OB_FAIL(check_append_shape_(*primary_ins_ctdef))) {
    LOG_WARN("invalid direct load stream append shape", K(ret), KPC(primary_ins_ctdef));
  } else if (OB_UNLIKELY(iter_end_)) {
    // do nothing
  } else if (OB_FAIL(open_table_for_each())) {
    LOG_WARN("open table for each failed", K(ret));
  } else if (OB_FAIL(init_append_context_(*primary_ins_ctdef))) {
    LOG_WARN("failed to init direct load stream append context", K(ret), KPC(primary_ins_ctdef));
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::rescan()
{
  const int ret = OB_NOT_SUPPORTED;
  if (queue_touched_) {
    const int mark_ret = mark_stream_failed_(ret, "direct load stream append rescan is not supported");
    if (OB_SUCCESS != mark_ret) {
      LOG_WARN("failed to mark direct load stream failed after rejected rescan", K(mark_ret), K(ret));
    }
  }
  if (is_append_registered_()) {
    const int end_ret = finish_append_(ret);
    if (OB_SUCCESS != end_ret) {
      LOG_WARN("failed to end direct load stream append after rejected rescan", K(end_ret), K(ret));
      const int mark_ret = mark_stream_failed_(
          end_ret, "end direct load stream append after rejected rescan failed", false);
      if (OB_SUCCESS != mark_ret) {
        LOG_WARN("failed to mark direct load stream failed after rejected rescan release error",
                 K(mark_ret), K(end_ret), K(ret));
      }
    }
  }
  LOG_WARN("direct load stream append does not support rescan", K(ret), K_(queue_touched),
           "append_registered", append_handle_.is_valid(), K_(appended_row_count));
  return ret;
}

int ObDirectLoadStreamAppendOp::inner_close()
{
  int ret = ObTableInsertOp::inner_close();
  if (OB_SUCCESS != ret && queue_touched_) {
    const int mark_ret = mark_stream_failed_(ret, "close direct load stream append failed");
    if (OB_SUCCESS != mark_ret) {
      LOG_WARN("failed to mark direct load stream failed after close error", K(mark_ret), K(ret));
    }
  }
  if (is_append_registered_()) {
    const int end_ret = finish_append_(OB_SUCCESS == ret ? OB_SUCCESS : ret);
    if (OB_SUCCESS != end_ret) {
      LOG_WARN("failed to end direct load stream append on close", K(end_ret));
      const int mark_ret = mark_stream_failed_(end_ret, "end direct load stream append on close failed", false);
      if (OB_SUCCESS != mark_ret) {
        LOG_WARN("failed to mark direct load stream failed after close release error",
                 K(mark_ret), K(end_ret), K(ret));
      }
      if (OB_SUCC(ret)) {
        ret = end_ret;
      }
    }
  }
  chunk_writer_.reset();
  if (!is_append_registered_()) {
    queue_handle_.reset();
    statement_id_ = common::OB_INVALID_ID;
    absolute_deadline_ = 0;
    appended_row_count_ = 0;
    queue_touched_ = false;
    stream_failed_reported_ = false;
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::get_primary_insert_ctdef_(const ObInsCtDef *&primary_ins_ctdef) const
{
  int ret = OB_SUCCESS;
  const ObDirectLoadStreamAppendSpec &spec = static_cast<const ObDirectLoadStreamAppendSpec &>(get_spec());
  const int64_t target_table_count = spec.ins_ctdefs_.count();
  primary_ins_ctdef = nullptr;
  if (OB_UNLIKELY(target_table_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream append target table ctdef is empty", K(ret), K(target_table_count));
  } else if (OB_UNLIKELY(target_table_count > 1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load stream does not support multiple target tables");
    LOG_WARN("direct load stream does not support multiple target tables", K(ret), K(target_table_count));
  } else {
    const int64_t table_ctdef_count = spec.ins_ctdefs_.at(0).count();
    if (OB_UNLIKELY(table_ctdef_count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("direct load stream append primary table ctdef is empty", K(ret), K(table_ctdef_count));
    } else if (OB_UNLIKELY(table_ctdef_count > 1)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "direct load stream append temporarily does not support target tables "
                     "with secondary indexes");
      LOG_WARN("direct load stream append temporarily does not support secondary indexes",
               K(ret), K(table_ctdef_count));
    } else if (OB_ISNULL(primary_ins_ctdef = spec.ins_ctdefs_.at(0).at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary insert ctdef is null", K(ret));
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::get_primary_insert_defs_(const ObInsCtDef *&primary_ins_ctdef,
                                                         ObInsRtDef *&primary_ins_rtdef)
{
  int ret = OB_SUCCESS;
  primary_ins_ctdef = nullptr;
  primary_ins_rtdef = nullptr;
  if (OB_FAIL(get_primary_insert_ctdef_(primary_ins_ctdef))) {
    LOG_WARN("failed to get primary insert ctdef", K(ret));
  } else if (OB_UNLIKELY(ins_rtdefs_.count() != 1 || ins_rtdefs_.at(0).count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary insert rtdef is invalid", K(ret), K(ins_rtdefs_.count()));
  } else {
    primary_ins_rtdef = &ins_rtdefs_.at(0).at(0);
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::check_append_shape_(const ObInsCtDef &primary_ins_ctdef) const
{
  int ret = OB_SUCCESS;
  const ObDirectLoadStreamAppendSpec &spec = static_cast<const ObDirectLoadStreamAppendSpec &>(get_spec());
  if (OB_UNLIKELY(!observer::is_valid_stream_id(spec.stream_id_) || spec.target_schema_version_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream append spec", K(ret), K(spec.stream_id_), K(spec.target_schema_version_));
  } else if (OB_UNLIKELY(!primary_ins_ctdef.is_primary_index_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary insert ctdef is not primary index", K(ret), K(primary_ins_ctdef));
  } else if (OB_UNLIKELY(primary_ins_ctdef.das_base_ctdef_.schema_version_ != spec.target_schema_version_)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("direct load stream append schema version mismatch", K(ret),
             "ctdef_schema_version", primary_ins_ctdef.das_base_ctdef_.schema_version_,
             K(spec.target_schema_version_));
  } else if (OB_UNLIKELY(MY_SPEC.need_trigger_fire_ || primary_ins_ctdef.has_instead_of_trigger_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load stream does not support target tables with triggers");
    LOG_WARN("direct load stream does not support target tables with triggers", K(ret),
             K(MY_SPEC.need_trigger_fire_), K(primary_ins_ctdef.has_instead_of_trigger_));
  } else if (OB_UNLIKELY(MY_SPEC.need_foreign_key_check_ || primary_ins_ctdef.fk_args_.count() > 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load stream does not support target tables with foreign keys");
    LOG_WARN("direct load stream does not support target tables with foreign keys", K(ret),
             K(MY_SPEC.need_foreign_key_check_), K(primary_ins_ctdef.fk_args_.count()));
  } else if (OB_UNLIKELY(primary_ins_ctdef.error_logging_ctdef_.is_error_logging_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct load stream append does not support error logging");
    LOG_WARN("direct load stream append does not support error logging", K(ret));
  } else if (OB_UNLIKELY(primary_ins_ctdef.is_table_without_pk_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "direct load stream append temporarily does not support hidden primary key tables");
    LOG_WARN("direct load stream append temporarily does not support hidden primary key tables", K(ret));
  } else if (OB_UNLIKELY(primary_ins_ctdef.is_table_with_clustering_key_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "direct load stream append temporarily does not support clustering key tables");
    LOG_WARN("direct load stream append temporarily does not support clustering key tables", K(ret));
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::check_stream_meta_(const observer::ObDirectLoadStreamMeta &meta,
                                                   const ObInsCtDef &primary_ins_ctdef,
                                                   bool &need_finish_stream) const
{
  int ret = OB_SUCCESS;
  need_finish_stream = false;
  const ObDirectLoadStreamAppendSpec &spec = static_cast<const ObDirectLoadStreamAppendSpec &>(get_spec());
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
  const uint64_t target_table_id = primary_ins_ctdef.das_base_ctdef_.index_tid_;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_UNLIKELY(meta.stream_id_ != spec.stream_id_
                         || observer::DIRECT_LOAD_STREAM_STATUS_OPEN != meta.status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("direct load stream is not appendable", K(ret), K(meta), K(spec.stream_id_));
  } else if (OB_UNLIKELY(meta.tenant_id_ != session->get_effective_tenant_id()
                         || meta.user_id_ != session->get_priv_user_id())) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("direct load stream append owner mismatch", K(ret), K(meta),
             "tenant_id", session->get_effective_tenant_id(),
             "user_id", session->get_priv_user_id());
  } else if (OB_UNLIKELY(meta.table_id_ != target_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    need_finish_stream = true;
    LOG_WARN("direct load stream target table mismatch", K(ret), K(meta), K(target_table_id));
  } else if (OB_UNLIKELY(meta.target_schema_version_ != spec.target_schema_version_)) {
    ret = OB_SCHEMA_ERROR;
    need_finish_stream = true;
    LOG_WARN("direct load stream target schema version mismatch", K(ret), K(meta),
             K(spec.target_schema_version_));
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::check_target_table_runtime_(const ObInsCtDef &primary_ins_ctdef) const
{
  int ret = OB_SUCCESS;
  const ObDirectLoadStreamAppendSpec &spec = static_cast<const ObDirectLoadStreamAppendSpec &>(get_spec());
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  const share::schema::ObDatabaseSchema *database_schema = nullptr;
  share::schema::ObSessionPrivInfo session_priv;
  share::schema::ObStmtNeedPrivs stmt_need_privs(ctx_.get_allocator());
  const uint64_t target_table_id = primary_ins_ctdef.das_base_ctdef_.index_tid_;
  if (OB_ISNULL(session) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null runtime context", K(ret), KP(session), KP(GCTX.schema_service_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == target_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream target table id", K(ret), K(target_table_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                         session->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("failed to get current tenant schema guard", K(ret),
             "tenant_id", session->get_effective_tenant_id());
  } else if (OB_FAIL(schema_guard.get_table_schema(session->get_effective_tenant_id(),
                                                   target_table_id,
                                                   table_schema))) {
    LOG_WARN("failed to get direct load stream target table schema", K(ret), K(target_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("direct load stream target table does not exist", K(ret), K(target_table_id));
  } else if (OB_UNLIKELY(table_schema->get_schema_version() != spec.target_schema_version_)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("direct load stream target schema version mismatch at runtime", K(ret), K(target_table_id),
             "current_schema_version", table_schema->get_schema_version(), K(spec.target_schema_version_));
  } else if (OB_FAIL(schema_guard.get_database_schema(session->get_effective_tenant_id(),
                                                      table_schema->get_database_id(),
                                                      database_schema))) {
    LOG_WARN("failed to get direct load stream target database schema", K(ret),
             K(table_schema->get_database_id()), K(target_table_id));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream target database schema is null", K(ret),
             K(table_schema->get_database_id()), K(target_table_id));
  } else if (OB_FAIL(schema_guard.get_session_priv_info(session->get_priv_tenant_id(),
                                                        session->get_priv_user_id(),
                                                        database_schema->get_database_name_str(),
                                                        session_priv))) {
    LOG_WARN("failed to get current direct load stream append session privilege", K(ret),
             "tenant_id", session->get_priv_tenant_id(),
             "user_id", session->get_priv_user_id());
  } else {
    const share::schema::ObColumnSchemaV2 *column_schema = nullptr;
    share::schema::ObNeedPriv need_priv(database_schema->get_database_name_str(),
                                        table_schema->get_table_name_str(),
                                        share::schema::OB_PRIV_TABLE_LEVEL,
                                        OB_PRIV_INSERT,
                                        table_schema->is_sys_table());
    for (int64_t i = 0; OB_SUCC(ret) && i < spec.target_column_ids_.count(); ++i) {
      const uint64_t column_id = spec.target_column_ids_.at(i);
      if (OB_UNLIKELY(column_id < OB_APP_MIN_COLUMN_ID)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid direct load stream append target column id", K(ret), K(column_id), K(i));
      } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("direct load stream append target column does not exist", K(ret),
                 K(target_table_id), K(column_id), K(i));
      } else if (OB_FAIL(need_priv.columns_.push_back(column_schema->get_column_name_str()))) {
        LOG_WARN("failed to add direct load stream append runtime privilege column", K(ret),
                 K(target_table_id), K(column_id), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt_need_privs.need_privs_.init(1))) {
      LOG_WARN("failed to init direct load stream append insert privilege array", K(ret));
    } else if (OB_FAIL(stmt_need_privs.need_privs_.push_back(need_priv))) {
      LOG_WARN("failed to add direct load stream append insert privilege", K(ret), K(need_priv));
    } else if (OB_FAIL(schema_guard.check_priv(session_priv,
                                              session->get_enable_role_array(),
                                              stmt_need_privs))) {
      LOG_WARN("direct load stream append target insert privilege check failed", K(ret),
               K(session_priv), K(target_table_id), K(stmt_need_privs));
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::init_append_context_(const ObInsCtDef &primary_ins_ctdef)
{
  int ret = OB_SUCCESS;
  const ObDirectLoadStreamAppendSpec &spec = static_cast<const ObDirectLoadStreamAppendSpec &>(get_spec());
  observer::ObDirectLoadStreamMeta meta;
  observer::ObTableLoadService *table_load_service = nullptr;
  bool need_finish_stream = false;
  queue_handle_.reset();
  queue_desc_ = table::ObDirectLoadStreamQueueDesc();
  statement_id_ = common::OB_INVALID_ID;
  absolute_deadline_ = 0;
  remote_append_started_ = false;
  if (OB_FAIL(observer::ObDirectLoadStreamService::get_direct_load_stream(spec.stream_id_, meta))) {
    LOG_WARN("failed to get direct load stream meta", K(ret), K(spec.stream_id_));
  } else if (OB_FAIL(check_stream_meta_(meta, primary_ins_ctdef, need_finish_stream))) {
    LOG_WARN("direct load stream meta check failed", K(ret), K(meta));
    if (need_finish_stream) {
      const int mark_ret = try_fail_stream_if_open_(ret);
      if (OB_SUCCESS != mark_ret) {
        LOG_WARN("failed to mark direct load stream failed after meta check error", K(mark_ret), K(ret));
      }
    }
  } else if (OB_FAIL(check_target_table_runtime_(primary_ins_ctdef))) {
    LOG_WARN("direct load stream target runtime check failed", K(ret), K(meta));
    if (OB_SCHEMA_ERROR == ret || OB_TABLE_NOT_EXIST == ret) {
      const int mark_ret = try_fail_stream_if_open_(ret);
      if (OB_SUCCESS != mark_ret) {
        LOG_WARN("failed to mark direct load stream failed after target table check error", K(mark_ret), K(ret));
      }
    }
  } else if (OB_ISNULL(table_load_service = MTL(observer::ObTableLoadService *))) {
    ret = OB_ERR_SYS;
    LOG_WARN("null table load service", K(ret));
  } else if (OB_UNLIKELY(table_load_service->is_stop())) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("table load service is stop", K(ret));
  } else if (OB_FAIL(table_load_service->get_direct_load_stream_service()
                         .select_append_queue(meta, queue_desc_))) {
    LOG_WARN("failed to select direct load stream append queue", K(ret), K(meta));
  } else if (queue_desc_.server_addr_ == GCTX.self_addr()
             && OB_FAIL(table_load_service->get_direct_load_stream_service()
                      .get_buffer_queue_manager()
                      .get_queue(observer::ObDirectLoadStreamBufferQueueKey(
                                     spec.stream_id_, queue_desc_.queue_id_),
                                 queue_handle_))) {
    LOG_WARN("failed to get selected direct load stream buffer queue",
             K(ret), K(spec.stream_id_), K(queue_desc_));
    const int mark_ret = try_fail_stream_if_open_(ret);
    if (OB_SUCCESS != mark_ret) {
      LOG_WARN("failed to mark direct load stream failed after local queue error", K(mark_ret), K(ret),
               K(spec.stream_id_));
    }
  } else if (!chunk_writer_.is_inited()
             && OB_FAIL(chunk_writer_.init(meta.tenant_id_))) {
    LOG_WARN("failed to init direct load stream chunk writer", K(ret));
  } else if (OB_FAIL(check_append_transaction_())) {
    LOG_WARN("direct load stream append transaction check failed", K(ret));
  } else if (queue_desc_.server_addr_ == GCTX.self_addr()
             ? OB_FAIL(begin_queue_append_())
             : OB_FAIL(begin_remote_append_(meta))) {
    LOG_WARN("failed to begin direct load stream append", K(ret));
    if (OB_STATE_NOT_MATCH != ret) {
      const int mark_ret = mark_stream_failed_(
          ret, "direct load stream queue registration failed", false);
      if (OB_SUCCESS != mark_ret) {
        LOG_WARN("failed to mark direct load stream failed after queue registration error",
                 K(mark_ret), K(ret));
      }
    }
  } else {
    chunk_writer_.reuse();
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::check_append_transaction_() const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
  bool autocommit = false;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(session->get_autocommit(autocommit))) {
    LOG_WARN("failed to get autocommit", K(ret));
  } else if (OB_UNLIKELY(!autocommit || session->has_explicit_start_trans())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "direct load stream append requires autocommit and does not support explicit transactions");
    LOG_WARN("direct load stream append requires autocommit and no explicit transaction", K(ret),
             K(autocommit), K(session->is_in_transaction()), K(session->has_explicit_start_trans()));
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::begin_queue_append_()
{
  int ret = OB_SUCCESS;
  observer::ObDirectLoadStreamBufferQueue *queue = nullptr;
  if (append_handle_.is_valid()) {
    // already registered
  } else if (OB_UNLIKELY(!queue_handle_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load stream append queue handle is invalid", K(ret));
  } else if (OB_ISNULL(queue = queue_handle_.get_queue())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream append queue is null", K(ret));
  } else if (OB_FAIL(queue->begin_append(append_handle_))) {
    LOG_WARN("failed to begin direct load stream queue append", K(ret));
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::end_queue_append_(const int result_code)
{
  int ret = OB_SUCCESS;
  observer::ObDirectLoadStreamBufferQueue *queue = nullptr;
  // 已触碰 queue 的失败 APPEND 必须先阻止正常 CLOSE/EOF，再释放登记。
  // 未产生 queue 副作用的请求仍只释放登记，保留修正后重试的边界。
  if (!append_handle_.is_valid()) {
    // not registered
  } else if (OB_UNLIKELY(!queue_handle_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load stream append queue handle is invalid", K(ret));
  } else if (OB_ISNULL(queue = queue_handle_.get_queue())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream append queue is null", K(ret));
  } else {
    const int fail_ret = queue_touched_ && OB_SUCCESS != result_code
        ? queue->fail(result_code) : OB_SUCCESS;
    // 并发 cleanup 已标记 REMOVED 时，正常 EOF 也已被阻止，仍须释放登记以完成清理。
    if (OB_SUCCESS != fail_ret && OB_ENTRY_NOT_EXIST != fail_ret) {
      ret = fail_ret;
      LOG_WARN("failed to mark local stream queue failed before append release",
               K(ret), K(result_code), K_(queue_desc));
    } else if (OB_FAIL(queue->end_append(append_handle_))) {
      if (OB_SUCCESS != result_code && result_code == ret && !append_handle_.is_valid()) {
        // FAILED queue 已释放登记但返回首错；调用方已保存同一错误，不视为释放失败。
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to end direct load stream queue append", K(ret), K(result_code));
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::begin_remote_append_(
    const observer::ObDirectLoadStreamMeta &meta)
{
  int ret = OB_SUCCESS;
  share::ObCommonID common_id;
  const int64_t now = common::ObTimeUtility::current_time();
  if (OB_UNLIKELY(remote_append_started_ || append_handle_.is_valid()
                  || queue_desc_.server_addr_ == GCTX.self_addr())) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null srv rpc proxy for remote stream append", K(ret), K(queue_desc_));
  } else if (OB_FAIL(storage::ObCommonIDUtils::gen_unique_id(MTL_ID(), common_id))) {
    LOG_WARN("fail to generate stream append statement id", K(ret), K(meta));
  } else if (OB_UNLIKELY(common_id.id() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("generated invalid stream append statement id", K(ret), K(common_id));
  } else {
    statement_id_ = static_cast<uint64_t>(common_id.id());
    absolute_deadline_ = THIS_WORKER.is_timeout_ts_valid()
        ? THIS_WORKER.get_timeout_ts()
        : now + observer::ObDirectLoadStreamForwardRpcProxy::DEFAULT_TIMEOUT_US;
    if (OB_UNLIKELY(absolute_deadline_ <= now)) {
      ret = OB_TIMEOUT;
    } else {
      observer::ObDirectLoadStreamForwardBeginArg arg;
      arg.tenant_id_ = meta.tenant_id_;
      arg.stream_id_ = meta.stream_id_;
      arg.queue_id_ = queue_desc_.queue_id_;
      arg.target_schema_version_ = meta.target_schema_version_;
      arg.statement_id_ = statement_id_;
      arg.absolute_deadline_ = absolute_deadline_;
      observer::ObDirectLoadStreamForwardRpcProxy proxy(*GCTX.srv_rpc_proxy_);
      if (OB_FAIL(proxy.to(queue_desc_.server_addr_)
                      .timeout(absolute_deadline_ - now)
                      .by(meta.tenant_id_)
                      .begin(arg))) {
        LOG_WARN("fail to begin remote stream append", K(ret), K(arg), K(queue_desc_));
      } else {
        remote_append_started_ = true;
      }
    }
  }
  if (OB_FAIL(ret) && !remote_append_started_) {
    statement_id_ = common::OB_INVALID_ID;
    absolute_deadline_ = 0;
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::append_remote_chunk_(const common::ObString &chunk)
{
  int ret = OB_SUCCESS;
  const int64_t now = common::ObTimeUtility::current_time();
  if (OB_UNLIKELY(!remote_append_started_ || common::OB_INVALID_ID == statement_id_
                  || chunk.empty())) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(absolute_deadline_ <= now)) {
    ret = OB_TIMEOUT;
  } else {
    observer::ObDirectLoadStreamForwardChunkArg arg;
    arg.statement_id_ = statement_id_;
    arg.chunk_ = chunk;
    observer::ObDirectLoadStreamForwardRpcProxy proxy(*GCTX.srv_rpc_proxy_);
    ret = proxy.to(queue_desc_.server_addr_)
              .timeout(absolute_deadline_ - now)
              .by(MTL_ID())
              .append_chunk(arg);
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::finish_remote_append_(const int result_code)
{
  int ret = OB_SUCCESS;
  const int64_t now = common::ObTimeUtility::current_time();
  const uint64_t statement_id = statement_id_;
  remote_append_started_ = false;
  statement_id_ = common::OB_INVALID_ID;
  if (OB_UNLIKELY(common::OB_INVALID_ID == statement_id)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(absolute_deadline_ <= now)) {
    ret = OB_TIMEOUT;
  } else {
    observer::ObDirectLoadStreamForwardRpcProxy proxy(*GCTX.srv_rpc_proxy_);
    proxy.to(queue_desc_.server_addr_).timeout(absolute_deadline_ - now).by(MTL_ID());
    // Errors before any CHUNK attempt only release admission for a later APPEND.
    if (OB_SUCCESS == result_code || !queue_touched_) {
      observer::ObDirectLoadStreamForwardEndArg arg;
      arg.statement_id_ = statement_id;
      ret = proxy.end(arg);
    } else {
      observer::ObDirectLoadStreamForwardAbortArg arg;
      arg.statement_id_ = statement_id;
      arg.result_code_ = result_code;
      ret = proxy.abort(arg);
    }
  }
  absolute_deadline_ = 0;
  return ret;
}

int ObDirectLoadStreamAppendOp::finish_append_(const int result_code)
{
  return remote_append_started_
      ? finish_remote_append_(result_code)
      : end_queue_append_(result_code);
}

bool ObDirectLoadStreamAppendOp::is_append_registered_() const
{
  return remote_append_started_ || append_handle_.is_valid();
}

int ObDirectLoadStreamAppendOp::append_current_row_(const ObInsCtDef &primary_ins_ctdef,
                                                    ObInsRtDef &primary_ins_rtdef)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  bool is_skipped = false;
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else {
    ++primary_ins_rtdef.cur_row_num_;
    if (OB_FAIL(ObDMLService::init_heap_table_pk_for_ins(primary_ins_ctdef, eval_ctx_))) {
      LOG_WARN("fail to init heap table pk to null", K(ret));
    } else if (OB_FAIL(ObDMLService::process_insert_row(primary_ins_ctdef,
                                                        primary_ins_rtdef,
                                                        *this,
                                                        is_skipped))) {
      LOG_WARN("process insert row failed", K(ret));
    } else if (OB_UNLIKELY(is_skipped)) {
      if (OB_FAIL(merge_implict_cursor(0, 0, 0, 0))) {
        LOG_WARN("merge implicit cursor failed", K(ret));
      }
    } else if (OB_FAIL(chunk_writer_.append_row(primary_ins_ctdef.new_row_, eval_ctx_))) {
      if (OB_SIZE_OVERFLOW == ret && chunk_writer_.get_row_count() > 0) {
        ret = OB_SUCCESS;
        if (OB_FAIL(flush_chunk_())) {
          LOG_WARN("failed to flush full direct load stream chunk", K(ret));
        } else if (OB_FAIL(chunk_writer_.append_row(primary_ins_ctdef.new_row_, eval_ctx_))) {
          LOG_WARN("failed to append row to empty direct load stream chunk", K(ret));
        } else {
          bool is_ins_val_opt = ctx_.get_sql_ctx()->is_do_insert_batch_opt();
          if (MY_SPEC.ab_stmt_id_ != nullptr && !is_ins_val_opt) {
            plan_ctx->record_last_insert_id_cur_stmt_for_batch();
          } else {
            plan_ctx->record_last_insert_id_cur_stmt();
          }
          if (OB_FAIL(merge_implict_cursor(1, 0, 0, 0))) {
            LOG_WARN("merge implicit cursor failed", K(ret));
          }
        }
      } else {
        LOG_WARN("failed to append row to direct load stream chunk writer", K(ret), K(primary_ins_ctdef.new_row_.count()));
      }
    } else {
      bool is_ins_val_opt = ctx_.get_sql_ctx()->is_do_insert_batch_opt();
      if (MY_SPEC.ab_stmt_id_ != nullptr && !is_ins_val_opt) {
        plan_ctx->record_last_insert_id_cur_stmt_for_batch();
      } else {
        plan_ctx->record_last_insert_id_cur_stmt();
      }
      if (OB_FAIL(merge_implict_cursor(1, 0, 0, 0))) {
        LOG_WARN("merge implicit cursor failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      record_err_for_load_data(ret, primary_ins_rtdef.cur_row_num_);
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::write_row_to_das_buffer()
{
  int ret = OB_SUCCESS;
  const ObInsCtDef *primary_ins_ctdef = nullptr;
  ObInsRtDef *primary_ins_rtdef = nullptr;
  if (OB_FAIL(get_primary_insert_defs_(primary_ins_ctdef, primary_ins_rtdef))) {
    LOG_WARN("failed to get primary insert defs", K(ret));
  } else if (OB_FAIL(append_current_row_(*primary_ins_ctdef, *primary_ins_rtdef))) {
    LOG_WARN("failed to process current row for direct load stream append", K(ret));
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::flush_chunk_()
{
  int ret = OB_SUCCESS;
  common::ObString chunk;
  observer::ObDirectLoadStreamBufferQueue *queue = nullptr;
  if (OB_UNLIKELY(!chunk_writer_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("direct load stream chunk writer is not inited", K(ret));
  } else if (chunk_writer_.get_row_count() <= 0) {
    // no buffered rows
  } else if (OB_FAIL(chunk_writer_.build_chunk(chunk))) {
    LOG_WARN("failed to build direct load stream chunk", K(ret), K(chunk_writer_));
  } else {
    const int64_t chunk_row_count = chunk_writer_.get_row_count();
    queue_touched_ = true;
    if (remote_append_started_) {
      if (OB_FAIL(append_remote_chunk_(chunk))) {
        LOG_WARN("failed to forward direct load stream chunk", K(ret), K(chunk_writer_),
                 K_(statement_id), K_(queue_desc));
      }
    } else if (OB_UNLIKELY(!queue_handle_.is_valid())) {
      ret = OB_NOT_INIT;
      LOG_WARN("direct load stream append queue handle is invalid", K(ret));
    } else if (OB_ISNULL(queue = queue_handle_.get_queue())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("direct load stream append queue is null", K(ret));
    } else if (OB_FAIL(queue->append_chunk(append_handle_, chunk))) {
      LOG_WARN("failed to append direct load stream chunk", K(ret), K(chunk_writer_));
    }
    if (OB_SUCC(ret)) {
      appended_row_count_ += chunk_row_count;
      LOG_DEBUG("direct load stream chunk appended", K(chunk_row_count),
                K(appended_row_count_));
      chunk_writer_.reuse();
    }
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::mark_stream_failed_(const int ret_code,
                                                   const char *error_msg,
                                                   const bool require_queue_touched)
{
  int ret = OB_SUCCESS;
  const ObDirectLoadStreamAppendSpec &spec = static_cast<const ObDirectLoadStreamAppendSpec &>(get_spec());
  if (stream_failed_reported_) {
    // 保留第一次失败原因。
  } else if (require_queue_touched && !queue_touched_) {
    // 尚未尝试写 tmp file 或发送远端 CHUNK 时，当前请求可以修正后重试，
    // 不改变 Stream 状态；无副作用的迟到 APPEND 同样不能覆盖已有终态。
  } else if (OB_UNLIKELY(OB_SUCCESS == ret_code || nullptr == error_msg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream failure report", K(ret), K(ret_code), KP(error_msg));
  } else {
    // 先释放当前 statement 和 queue 引用，避免同步 cleanup 等待调用方自身。
    const int release_ret = is_append_registered_() ? finish_append_(ret_code) : OB_SUCCESS;
    if (OB_SUCCESS != release_ret) {
      LOG_WARN("failed to release direct load stream append before terminal cleanup",
               K(release_ret), K(spec.stream_id_), K(ret_code), K_(queue_desc));
    }
    if (OB_UNLIKELY(is_append_registered_())) {
      ret = OB_SUCCESS != release_ret ? release_ret : OB_ERR_UNEXPECTED;
      LOG_WARN("direct load stream append remains registered before terminal cleanup",
               K(ret), K(spec.stream_id_), K(ret_code), K_(queue_desc));
    } else {
      queue_handle_.reset();
      const int finish_ret = observer::ObDirectLoadStreamService::
          finish_and_cleanup_direct_load_stream(spec.stream_id_, ret_code);
      if (OB_SUCCESS != finish_ret) {
        ret = finish_ret;
      }
    }
  }
  if (OB_SUCCESS != ret) {
    LOG_WARN("failed to handle direct load stream fatal append error", K(ret),
             K(spec.stream_id_), K(ret_code), K(error_msg));
  } else {
    LOG_WARN("completed fatal error handling for direct load stream append", K(spec.stream_id_),
             K(ret_code), K(error_msg), K(queue_touched_), K(appended_row_count_));
    stream_failed_reported_ = true;
  }
  return ret;
}

int ObDirectLoadStreamAppendOp::try_fail_stream_if_open_(const int error_code)
{
  const ObDirectLoadStreamAppendSpec &spec =
      static_cast<const ObDirectLoadStreamAppendSpec &>(get_spec());
  return observer::ObDirectLoadStreamService::fail_direct_load_stream_if_open(
      spec.stream_id_, error_code);
}

int ObDirectLoadStreamAppendOp::write_rows_post_proc(int last_errno)
{
  int ret = last_errno;
  const bool is_append_terminal = OB_SUCCESS != last_errno || iter_end_;
  if (OB_SUCC(ret) && iter_end_) {
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    if (OB_ISNULL(plan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical plan ctx is null", K(ret));
    } else if (OB_FAIL(flush_chunk_())) {
      LOG_WARN("failed to flush direct load stream append chunk", K(ret));
    } else {
      plan_ctx->add_row_matched_count(appended_row_count_);
      plan_ctx->add_affected_rows(appended_row_count_);
      if (OB_FAIL(plan_ctx->sync_last_value_local())) {
        LOG_WARN("failed to sync last value", K(ret));
      } else {
        int sync_ret = plan_ctx->sync_last_value_global();
        if (OB_SUCCESS != sync_ret) {
          LOG_WARN("failed to sync value globally", K(sync_ret));
        }
        if (OB_SUCC(ret)) {
          ret = sync_ret;
        }
      }
    }
  }
  if (is_append_terminal) {
    const int append_result = ret;
    if (OB_SUCCESS != append_result && queue_touched_) {
      const int mark_ret = mark_stream_failed_(
          append_result, "post process direct load stream append failed");
      if (OB_SUCCESS != mark_ret) {
        LOG_WARN("failed to mark direct load stream failed after append post process error",
                 K(mark_ret), K(append_result));
      }
    }
    if (is_append_registered_()) {
      const int end_ret = finish_append_(OB_SUCCESS == append_result ? OB_SUCCESS : append_result);
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to finish direct load stream append", K(end_ret), K(append_result));
        if (OB_SUCC(ret)) {
          ret = end_ret;
        }
        // Failed release or an unknown RPC result is fatal; keep the original SQL error.
        const int mark_ret = mark_stream_failed_(
            end_ret, "finish direct load stream append failed", false);
        if (OB_SUCCESS != mark_ret) {
          LOG_WARN("failed to mark stream after append terminal error",
                   K(mark_ret), K(end_ret));
        }
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
