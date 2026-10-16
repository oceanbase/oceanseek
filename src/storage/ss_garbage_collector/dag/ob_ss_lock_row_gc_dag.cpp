/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/dag/ob_ss_lock_row_gc_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSLockRowGCLSCtx::ObSSLockRowGCLSCtx()
  : ls_id_(),
    ls_lock_row_key_(),
    ls_lock_row_value_(),
    page_iter_(),
    iterator_inited_(false),
    scan_finished_(false),
    scan_failed_(false),
    all_tablet_lock_rows_deleted_(true),
    first_error_code_(OB_SUCCESS)
{
}

ObSSLockRowGCLSCtx::~ObSSLockRowGCLSCtx()
{
  reset();
}

void ObSSLockRowGCLSCtx::reset()
{
  ls_id_.reset();
  ls_lock_row_key_.reset();
  ls_lock_row_value_.reset();
  page_iter_.reset();
  iterator_inited_ = false;
  scan_finished_ = false;
  scan_failed_ = false;
  ATOMIC_STORE(&all_tablet_lock_rows_deleted_, true);
  ATOMIC_STORE(&first_error_code_, OB_SUCCESS);
}

bool ObSSLockRowGCLSCtx::is_valid() const
{
  return ls_id_.is_valid() && ls_lock_row_key_.is_valid();
}

bool ObSSLockRowGCLSCtx::record_first_error_code(const int error_code)
{
  return OB_SUCCESS != error_code
      && ATOMIC_BCAS(&first_error_code_, OB_SUCCESS, error_code);
}

int ObSSLockRowGCLSCtx::get_first_error_code() const
{
  return ATOMIC_LOAD(&first_error_code_);
}

void ObSSLockRowGCLSCtx::mark_tablet_lock_rows_not_all_deleted()
{
  ATOMIC_STORE(&all_tablet_lock_rows_deleted_, false);
}

bool ObSSLockRowGCLSCtx::are_all_tablet_lock_rows_deleted() const
{
  return ATOMIC_LOAD(&all_tablet_lock_rows_deleted_);
}

ObSSLockRowGCDagCtx::ObSSLockRowGCDagCtx()
  : tenant_id_(OB_INVALID_TENANT_ID),
    sql_timeout_us_(0),
    ls_ctxs_(),
    ls_count_(0),
    first_error_code_(OB_SUCCESS),
    failed_ls_count_(0),
    scanned_tablet_count_(0),
    deleted_tablet_lock_row_count_(0),
    deleted_ls_lock_row_count_(0)
{
}

ObSSLockRowGCDagCtx::~ObSSLockRowGCDagCtx()
{
  reset();
}

int ObSSLockRowGCDagCtx::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock row gc dag ctx init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    ls_ctxs_.set_attr(ObMemAttr(tenant_id, "LockGCLSCtxArr"));
  }
  return ret;
}

void ObSSLockRowGCDagCtx::reset()
{
  for (int64_t i = 0; i < ls_ctxs_.count(); ++i) {
    ObSSLockRowGCLSCtx *ls_ctx = ls_ctxs_.at(i);
    if (OB_NOT_NULL(ls_ctx)) {
      ls_ctx->~ObSSLockRowGCLSCtx();
      mtl_free(ls_ctx);
    }
  }
  ls_ctxs_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_timeout_us_ = 0;
  ATOMIC_STORE(&ls_count_, 0);
  ATOMIC_STORE(&first_error_code_, OB_SUCCESS);
  ATOMIC_STORE(&failed_ls_count_, 0);
  ATOMIC_STORE(&scanned_tablet_count_, 0);
  ATOMIC_STORE(&deleted_tablet_lock_row_count_, 0);
  ATOMIC_STORE(&deleted_ls_lock_row_count_, 0);
}

bool ObSSLockRowGCDagCtx::is_valid() const
{
  return is_valid_tenant_id(tenant_id_);
}

int ObSSLockRowGCDagCtx::alloc_ls_ctx(
    const ObLSID &ls_id,
    ObSSLockRowGCLSCtx *&ls_ctx)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ls_ctx = nullptr;
  if (OB_UNLIKELY(!is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(ls_id));
  } else if (OB_ISNULL(buf = mtl_malloc(
      sizeof(ObSSLockRowGCLSCtx), ObMemAttr(tenant_id_, "LockGCLSCtx")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate lock row gc ls ctx", KR(ret), K_(tenant_id),
        K(ls_id));
  } else {
    ls_ctx = new (buf) ObSSLockRowGCLSCtx();
    ls_ctx->ls_id_ = ls_id;
    ls_ctx->ls_lock_row_key_.type_ =
        blocksstable::ObStorageObjectType::SHARED_LS_LOCK;
    ls_ctx->ls_lock_row_key_.ls_key_.first_id_ = ls_id.id();
    ls_ctx->ls_lock_row_key_.ls_key_.second_id_ = 0;
    ls_ctx->ls_lock_row_key_.ls_key_.control_scn_.set_min();
    if (OB_UNLIKELY(!ls_ctx->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lock row gc ls ctx", KR(ret), K_(tenant_id),
          K(ls_id), KPC(ls_ctx));
    } else if (OB_FAIL(ls_ctxs_.push_back(ls_ctx))) {
      LOG_WARN("fail to append lock row gc ls ctx", KR(ret), K_(tenant_id),
          K(ls_id));
    }
    if (OB_FAIL(ret)) {
      ls_ctx->~ObSSLockRowGCLSCtx();
      mtl_free(ls_ctx);
      ls_ctx = nullptr;
    } else {
      (void)ATOMIC_AAF(&ls_count_, 1);
    }
  }
  return ret;
}

bool ObSSLockRowGCDagCtx::record_error_code(
    ObSSLockRowGCLSCtx &ls_ctx,
    const int error_code)
{
  bool is_first_ls_error = false;
  if (OB_SUCCESS == error_code) {
    // Nothing to record.
  } else if (OB_UNLIKELY(!is_valid() || !ls_ctx.is_valid()
      || !owns_ls_ctx_(ls_ctx))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT,
        "invalid or unowned lock row gc ls ctx", K_(tenant_id), K(ls_ctx),
        K(error_code));
  } else {
    is_first_ls_error = ls_ctx.record_first_error_code(error_code);
    if (is_first_ls_error) {
      (void)ATOMIC_BCAS(&first_error_code_, OB_SUCCESS, error_code);
      (void)ATOMIC_AAF(&failed_ls_count_, 1);
    }
  }
  return is_first_ls_error;
}

void ObSSLockRowGCDagCtx::add_scanned_tablet_count(const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&scanned_tablet_count_, count);
  }
}

void ObSSLockRowGCDagCtx::add_deleted_tablet_lock_row_count(
    const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&deleted_tablet_lock_row_count_, count);
  }
}

void ObSSLockRowGCDagCtx::add_deleted_ls_lock_row_count(
    const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&deleted_ls_lock_row_count_, count);
  }
}

int64_t ObSSLockRowGCDagCtx::get_ls_count() const
{
  return ATOMIC_LOAD(&ls_count_);
}

int ObSSLockRowGCDagCtx::get_first_error_code() const
{
  return ATOMIC_LOAD(&first_error_code_);
}

int64_t ObSSLockRowGCDagCtx::get_failed_ls_count() const
{
  return ATOMIC_LOAD(&failed_ls_count_);
}

int64_t ObSSLockRowGCDagCtx::get_scanned_tablet_count() const
{
  return ATOMIC_LOAD(&scanned_tablet_count_);
}

int64_t ObSSLockRowGCDagCtx::get_deleted_tablet_lock_row_count() const
{
  return ATOMIC_LOAD(&deleted_tablet_lock_row_count_);
}

int64_t ObSSLockRowGCDagCtx::get_deleted_ls_lock_row_count() const
{
  return ATOMIC_LOAD(&deleted_ls_lock_row_count_);
}

bool ObSSLockRowGCDagCtx::owns_ls_ctx_(
    const ObSSLockRowGCLSCtx &ls_ctx) const
{
  bool owns_ls_ctx = false;
  for (int64_t i = 0; !owns_ls_ctx && i < ls_ctxs_.count(); ++i) {
    owns_ls_ctx = &ls_ctx == ls_ctxs_.at(i);
  }
  return owns_ls_ctx;
}

ObSSLockRowGCDagParam::ObSSLockRowGCDagParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    schedule_ts_(0)
{
}

int ObSSLockRowGCDagParam::init(
    const uint64_t tenant_id,
    const int64_t schedule_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock row gc dag param init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || schedule_ts < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lock row gc dag param", KR(ret), K(tenant_id),
        K(schedule_ts));
  } else {
    tenant_id_ = tenant_id;
    schedule_ts_ = schedule_ts;
  }
  return ret;
}

bool ObSSLockRowGCDagParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && schedule_ts_ >= 0;
}

ObSSLockRowGCDag::ObSSLockRowGCDag()
  : ObSSGCDagBase(DAG_TYPE),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    schedule_ts_(0),
    start_event_reported_(0),
    ctx_(nullptr)
{
}

ObSSLockRowGCDag::~ObSSLockRowGCDag()
{
  free_ctx_();
  tenant_id_ = OB_INVALID_TENANT_ID;
  schedule_ts_ = 0;
  ATOMIC_STORE(&start_event_reported_, 0);
  is_inited_ = false;
}

int ObSSLockRowGCDag::alloc_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(OB_NOT_NULL(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock row gc dag ctx already allocated", KR(ret), KP_(ctx));
  } else if (OB_ISNULL(buf = mtl_malloc(
      sizeof(ObSSLockRowGCDagCtx), ObMemAttr(MTL_ID(), "LockRowGCDag")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate lock row gc dag ctx", KR(ret));
  } else {
    ctx_ = new (buf) ObSSLockRowGCDagCtx();
  }
  return ret;
}

void ObSSLockRowGCDag::free_ctx_()
{
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObSSLockRowGCDagCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

bool ObSSLockRowGCDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() == other.get_type()) {
    const ObSSLockRowGCDag &other_dag =
        static_cast<const ObSSLockRowGCDag &>(other);
    bret = tenant_id_ == other_dag.tenant_id_;
  }
  return bret;
}

uint64_t ObSSLockRowGCDag::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  return hash_val;
}

int ObSSLockRowGCDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  char stat_buf[128] = {0};
  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock row gc dag ctx is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(databuff_printf(stat_buf, sizeof(stat_buf),
      "scanned=%ld,tablet_lock_rows_deleted=%ld,ls_lock_rows_deleted=%ld",
      ctx_->get_scanned_tablet_count(),
      ctx_->get_deleted_tablet_lock_row_count(),
      ctx_->get_deleted_ls_lock_row_count()))) {
    LOG_WARN("fail to format lock row gc dag statistics", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(
      out_param, allocator, get_type(), static_cast<int64_t>(tenant_id_),
      static_cast<int64_t>(GCTX.get_server_id()), ctx_->get_ls_count(),
      static_cast<int64_t>(ctx_->get_first_error_code()),
      ctx_->get_failed_ls_count(), "server_addr", GCONF.self_addr_,
      "row_stat", stat_buf))) {
    LOG_WARN("fail to fill lock row gc dag info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSLockRowGCDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "tenant_id=%lu", tenant_id_))) {
    LOG_WARN("fail to fill lock row gc dag key", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSLockRowGCDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSLockRowGCDagParam *dag_param =
      static_cast<const ObSSLockRowGCDagParam *>(param);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock row gc dag init twice", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(dag_param) || OB_UNLIKELY(!dag_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lock row gc dag param", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(alloc_ctx_())) {
    LOG_WARN("fail to allocate lock row gc dag ctx", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(ctx_->init(dag_param->tenant_id_))) {
    LOG_WARN("fail to init lock row gc dag ctx", KR(ret), KPC(dag_param));
  } else {
    tenant_id_ = dag_param->tenant_id_;
    schedule_ts_ = dag_param->schedule_ts_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLockRowGCDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSLockRowGCPrepareTask *prepare_task = nullptr;
  ObSSLockRowGCScheduleTask *schedule_task = nullptr;
  ObSSLockRowGCDeleteLSLockTask *delete_ls_lock_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)
      || OB_ISNULL(ctx_)
      || OB_UNLIKELY(!ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock row gc dag is not initialized", KR(ret), K_(tenant_id),
        K_(is_inited), KPC_(ctx));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("fail to allocate lock row gc prepare task", KR(ret),
        K_(tenant_id));
  } else if (OB_ISNULL(prepare_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated lock row gc prepare task is null", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(alloc_task(schedule_task))) {
    LOG_WARN("fail to allocate lock row gc schedule task", KR(ret),
        K_(tenant_id));
  } else if (OB_ISNULL(schedule_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated lock row gc schedule task is null", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(alloc_task(delete_ls_lock_task))) {
    LOG_WARN("fail to allocate lock row gc delete ls lock task", KR(ret),
        K_(tenant_id));
  } else if (OB_ISNULL(delete_ls_lock_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated lock row gc delete ls lock task is null", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(prepare_task->init())) {
    LOG_WARN("fail to init lock row gc prepare task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schedule_task->init(0, true /* is_ls_head */))) {
    LOG_WARN("fail to init lock row gc bootstrap schedule task", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(delete_ls_lock_task->init())) {
    LOG_WARN("fail to init lock row gc delete ls lock task", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(prepare_task->add_child(*schedule_task))) {
    LOG_WARN("fail to add prepare to schedule edge", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schedule_task->add_child(*delete_ls_lock_task))) {
    LOG_WARN("fail to add schedule to delete ls lock edge", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    LOG_WARN("fail to add lock row gc prepare task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(add_task(*schedule_task))) {
    LOG_WARN("fail to add lock row gc schedule task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(add_task(*delete_ls_lock_task))) {
    LOG_WARN("fail to add lock row gc delete ls lock task", KR(ret),
        K_(tenant_id));
  }
  LOG_INFO("finish to create lock row gc dag first tasks", KR(ret),
      K_(tenant_id), KPC_(ctx));
  return ret;
}

void ObSSLockRowGCDag::report_start_event()
{
  if (ATOMIC_BCAS(&start_event_reported_, 0, 1)) {
    SERVER_EVENT_ADD("SHARED_GC", "START_LOCK_ROW_GC_DAG",
        "tenant_id", tenant_id_,
        "schedule_ts", schedule_ts_,
        "dag_id", get_dag_id(),
        "dag_start_time_us", get_start_time(),
        "trace_id", ObCurTraceId::get_trace_id_str());
  }
}

int ObSSLockRowGCDag::report_result()
{
  const int dag_ret = get_dag_ret();
  const ObIDag::ObDagStatus dag_status = get_dag_status();
  const int64_t finish_time_us = ObTimeUtility::fast_current_time_us();
  const int64_t start_time_us = get_start_time();
  const int64_t cost_us = start_time_us > 0 && finish_time_us >= start_time_us
      ? finish_time_us - start_time_us : 0;
  char stat_buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
  if (OB_ISNULL(ctx_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED,
        "lock row gc dag ctx is null when reporting", K_(tenant_id),
        K(dag_ret), K(dag_status));
  } else {
    (void)databuff_printf(stat_buf, sizeof(stat_buf),
        "failed_ls_count=%ld,ls_count=%ld,scanned_tablet_count=%ld,"
        "deleted_tablet_lock_row_count=%ld,"
        "deleted_ls_lock_row_count=%ld,cost_us=%ld",
        ctx_->get_failed_ls_count(), ctx_->get_ls_count(),
        ctx_->get_scanned_tablet_count(),
        ctx_->get_deleted_tablet_lock_row_count(),
        ctx_->get_deleted_ls_lock_row_count(), cost_us);
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_LOCK_ROW_GC_DAG",
        "tenant_id", tenant_id_,
        "schedule_ts", schedule_ts_,
        "dag_ret", dag_ret,
        "dag_status", static_cast<int64_t>(dag_status),
        "dag_id", get_dag_id(),
        "first_error_code", ctx_->get_first_error_code(),
        stat_buf /* extra_info */);
  }
  LOG_INFO("finish lock row gc dag", K_(tenant_id), K(dag_ret),
      K(dag_status), K(cost_us), KPC_(ctx));
  return OB_SUCCESS;
}

ObSSLockRowGCPrepareTask::ObSSLockRowGCPrepareTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_LOCK_ROW_GC),
    is_inited_(false)
{
}

int ObSSLockRowGCPrepareTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock row gc prepare task init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSLockRowGCPrepareTask::alloc_ls_ctxs_(
    ObSSLockRowGCDagCtx &ctx,
    const ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
    ObSSLockRowGCLSCtx *ls_ctx = nullptr;
    if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
      LOG_INFO("stop allocating lock row gc ls contexts", KR(ret), K(i),
          K(ctx));
    } else if (OB_FAIL(ctx.alloc_ls_ctx(ls_ids.at(i), ls_ctx))) {
      LOG_WARN("fail to allocate lock row gc ls ctx", KR(ret),
          K_(ctx.tenant_id), K(ls_ids.at(i)));
    }
  }
  return ret;
}

int ObSSLockRowGCPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObArray<ObLSID> ls_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock row gc prepare task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLockRowGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get lock row gc dag ctx", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop lock row gc prepare task", KR(ret), KPC(ctx));
  } else if (FALSE_IT(
      static_cast<ObSSLockRowGCDag *>(get_dag())->report_start_event())) {
  } else if (OB_FAIL(ObSSGCUtil::get_shared_gc_sql_timeout_us(
      ctx->tenant_id_, ctx->sql_timeout_us_))) {
    LOG_WARN("fail to get shared gc sql timeout", KR(ret),
        K_(ctx->tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ctx->tenant_id));
  } else {
    ls_ids.set_attr(ObMemAttr(ctx->tenant_id_, "LockRowGCLSIds"));
    if (OB_FAIL(ObSSGCDagUtil::get_all_ls_ids_from_meta_ctrl(
        ctx->tenant_id_, *GCTX.sql_proxy_, ctx->sql_timeout_us_, ls_ids,
        this))) {
      LOG_WARN("fail to get lock row gc ls ids", KR(ret),
          K_(ctx->tenant_id), K_(ctx->sql_timeout_us));
    } else if (OB_FAIL(alloc_ls_ctxs_(*ctx, ls_ids))) {
      LOG_WARN("fail to allocate lock row gc ls contexts", KR(ret),
          K_(ctx->tenant_id), "ls_count", ls_ids.count());
    }
  }
  LOG_INFO("finish lock row gc prepare task", KR(ret), KPC(ctx));
  return ret;
}

ObSSLockRowGCScheduleTask::ObSSLockRowGCScheduleTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_LOCK_ROW_GC),
    is_inited_(false),
    next_task_generated_(false),
    ls_idx_(-1),
    is_ls_head_(false),
    batch_idx_(-1),
    ls_ctx_(nullptr)
{
}

int ObSSLockRowGCScheduleTask::init(
    const int64_t ls_idx,
    const bool is_ls_head,
    const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock row gc schedule task init twice", KR(ret), K_(ls_idx));
  } else if (OB_UNLIKELY(ls_idx < 0 || batch_idx < 0
      || (is_ls_head && 0 != batch_idx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lock row gc schedule task input", KR(ret), K(ls_idx),
        K(is_ls_head), K(batch_idx));
  } else {
    ls_idx_ = ls_idx;
    is_ls_head_ = is_ls_head;
    batch_idx_ = batch_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLockRowGCScheduleTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSLockRowGCDag *dag = nullptr;
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCScheduleTask *next_head = nullptr;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock row gc schedule task is not initialized", KR(ret));
  } else if (next_task_generated_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLockRowGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get lock row gc dag ctx", KR(ret), K_(ls_idx));
  } else if (ctx->ls_ctxs_.empty() || !is_ls_head_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(bind_ls_ctx_(*ctx))) {
    LOG_WARN("fail to bind lock row gc ls ctx", KR(ret), K_(ls_idx));
  } else if (ls_idx_ + 1 >= ctx->ls_ctxs_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag = static_cast<ObSSLockRowGCDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock row gc dag is null", KR(ret), K_(ls_idx));
  } else if (OB_FAIL(dag->alloc_task(next_head))) {
    LOG_WARN("fail to allocate next lock row gc ls head", KR(ret),
        K_(ls_idx));
  } else if (OB_ISNULL(next_head)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated next lock row gc ls head is null", KR(ret),
        K_(ls_idx));
  } else if (OB_FAIL(next_head->init(
      ls_idx_ + 1, true /* is_ls_head */, 0 /* batch_idx */))) {
    LOG_WARN("fail to init next lock row gc ls head", KR(ret), K_(ls_idx));
  } else {
    next_task_generated_ = true;
    next_task = next_head;
  }
  return ret;
}

int ObSSLockRowGCScheduleTask::bind_ls_ctx_(ObSSLockRowGCDagCtx &ctx)
{
  int ret = OB_SUCCESS;
  ls_ctx_ = nullptr;
  if (OB_UNLIKELY(ls_idx_ < 0 || ls_idx_ >= ctx.ls_ctxs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock row gc ls index", KR(ret), K_(ls_idx),
        "ls_count", ctx.ls_ctxs_.count());
  } else if (OB_ISNULL(ls_ctx_ = ctx.ls_ctxs_.at(ls_idx_))
      || OB_UNLIKELY(!ls_ctx_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock row gc ls ctx", KR(ret), K_(ls_idx), KPC_(ls_ctx));
    ls_ctx_ = nullptr;
  }
  return ret;
}

void ObSSLockRowGCScheduleTask::fill_page_scan_param_(
    ObSSMetaCtrlPageScanParam &scan_param) const
{
  scan_param.reset();
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::SINGLE_LOCK_META_TYPE;
  scan_param.single_meta_type_ =
      blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  scan_param.page_size_ = CTRL_SCAN_PAGE_SIZE;
}

int ObSSLockRowGCScheduleTask::get_ls_lock_row_value_(
    const ObSSLockRowGCDagCtx &ctx,
    bool &ls_lock_row_exist)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "LockGCLSRow"));
  ObString serialized_meta_key;
  ObSSMetaCtrlLockValueIterator lock_row_iter;
  ls_lock_row_exist = false;
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ctx.is_valid()
      || !ls_ctx_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lock row gc ls scan context", KR(ret), K(ctx),
        K_(ls_idx), KPC_(ls_ctx));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ctx.tenant_id), K_(ls_idx));
  } else if (OB_FAIL(ObSSGCDagUtil::serialize_meta_key(
      ls_ctx_->ls_lock_row_key_, allocator, serialized_meta_key))) {
    LOG_WARN("fail to serialize ls lock row key", KR(ret), K_(ls_idx),
        KPC_(ls_ctx));
  } else if (OB_FAIL(lock_row_iter.init(
      *GCTX.sql_proxy_, ctx.sql_timeout_us_, ctx.tenant_id_,
      ls_ctx_->ls_id_, serialized_meta_key))) {
    LOG_WARN("fail to init ls lock row iterator", KR(ret), K_(ls_idx),
        KPC_(ls_ctx));
  } else {
    ls_ctx_->ls_lock_row_value_.reset();
    const int first_ret =
        lock_row_iter.get_next(ls_ctx_->ls_lock_row_value_);
    if (OB_ITER_END == first_ret) {
      // The LS lock row may have been deleted by another observer after
      // Prepare collected this LS. Treat the already-converged branch as a
      // successful no-op.
      LOG_INFO("ls lock row no longer exists", K_(ctx.tenant_id),
          K_(ls_idx), KPC_(ls_ctx));
    } else if (OB_SUCCESS != first_ret) {
      ret = first_ret;
      LOG_WARN("fail to get ls lock row value", KR(ret), K_(ctx.tenant_id),
          K_(ls_idx), KPC_(ls_ctx));
    } else {
      ObSSMetaCtrlLockRowValue extra_lock_row_value;
      const int next_ret = lock_row_iter.get_next(extra_lock_row_value);
      if (OB_ITER_END == next_ret) {
        ls_lock_row_exist = true;
      } else if (OB_SUCCESS == next_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("more than one ls lock row found", KR(ret),
            K_(ctx.tenant_id), K_(ls_idx), KPC_(ls_ctx));
      } else {
        ret = next_ret;
        LOG_WARN("fail to check duplicate ls lock row", KR(ret),
            K_(ctx.tenant_id), K_(ls_idx), KPC_(ls_ctx));
      }
    }
  }
  return ret;
}

int ObSSLockRowGCScheduleTask::init_ls_scan_(
    ObSSLockRowGCDagCtx &ctx,
    bool &need_scan)
{
  int ret = OB_SUCCESS;
  bool ls_lock_row_exist = false;
  ObSSMetaCtrlPageScanParam scan_param;
  need_scan = false;
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ctx.is_valid()
      || !ls_ctx_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock row gc ls context", KR(ret), K_(ls_idx),
        KPC_(ls_ctx));
  } else if (ls_ctx_->iterator_inited_) {
    if (OB_UNLIKELY(ls_ctx_->scan_finished_ || ls_ctx_->scan_failed_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid initialized lock row gc scan state", KR(ret),
          K_(ls_idx), KPC_(ls_ctx));
    } else {
      // A Continuation reuses the iterator owned by the LS context.
      need_scan = true;
    }
  } else if (OB_UNLIKELY(!is_ls_head_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock row gc continuation has no initialized iterator",
        KR(ret), K_(ls_idx), KPC_(ls_ctx));
  } else if (ls_ctx_->scan_finished_ || ls_ctx_->scan_failed_) {
    // A terminal HEAD branch has no iterator to reuse.
  } else if (OB_FAIL(get_ls_lock_row_value_(ctx, ls_lock_row_exist))) {
    LOG_WARN("fail to get ls lock row value", KR(ret), K_(ls_idx),
        KPC_(ls_ctx));
  } else if (!ls_lock_row_exist) {
    ls_ctx_->scan_finished_ = true;
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ctx.tenant_id), K_(ls_idx));
  } else {
    fill_page_scan_param_(scan_param);
    if (OB_FAIL(ls_ctx_->page_iter_.init(
        *GCTX.sql_proxy_, ctx.sql_timeout_us_, ctx.tenant_id_,
        ls_ctx_->ls_id_, scan_param))) {
      LOG_WARN("fail to init lock row gc page iterator", KR(ret),
          K_(ctx.tenant_id), K_(ls_idx), KPC_(ls_ctx), K(scan_param));
    } else {
      ls_ctx_->iterator_inited_ = true;
      need_scan = true;
    }
  }
  return ret;
}

int ObSSLockRowGCScheduleTask::fetch_tablet_infos_(
    ObSSLockRowGCDagCtx &ctx,
    ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
    bool &need_continuation)
{
  int ret = OB_SUCCESS;
  int64_t scanned_tablet_count = 0;
  tablet_infos.reset();
  need_continuation = false;
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ctx.is_valid()
      || !ls_ctx_->is_valid() || !ls_ctx_->iterator_inited_
      || ls_ctx_->scan_finished_ || ls_ctx_->scan_failed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock row gc scan state", KR(ret), K_(ls_idx),
        KPC_(ls_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < EXECUTOR_BATCH_SIZE; ++i) {
      if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
        tablet_infos.reset();
        LOG_INFO("stop fetching lock row gc tablets", KR(ret), K_(ls_idx));
      } else {
        ObTabletID tablet_id;
        SCN reorg_scn;
        const int iter_ret =
            ls_ctx_->page_iter_.get_next_tablet(tablet_id, reorg_scn);
        if (OB_ITER_END == iter_ret) {
          ls_ctx_->scan_finished_ = true;
          break;
        } else if (OB_SUCCESS != iter_ret) {
          ret = iter_ret;
          LOG_WARN("fail to get next lock row gc tablet", KR(ret),
              K_(ls_idx), KPC_(ls_ctx));
        } else {
          ++scanned_tablet_count;
          const ObSSGCTabletBaseInfo tablet_info(tablet_id, reorg_scn);
          if (OB_UNLIKELY(!tablet_info.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid lock row gc tablet info", KR(ret), K_(ls_idx),
                K(tablet_info));
          } else if (OB_FAIL(tablet_infos.push_back(tablet_info))) {
            LOG_WARN("fail to append lock row gc tablet info", KR(ret),
                K_(ls_idx), K(tablet_info));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      need_continuation =
          !ls_ctx_->scan_finished_ && !ls_ctx_->scan_failed_;
    }
  }
  ctx.add_scanned_tablet_count(scanned_tablet_count);
  return ret;
}

int ObSSLockRowGCScheduleTask::generate_tasks_(
    ObSSLockRowGCDag &dag,
    const ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
    const bool need_continuation)
{
  int ret = OB_SUCCESS;
  ObSSLockRowGCExecutorTask *executor_task = nullptr;
  ObSSLockRowGCScheduleTask *continuation_task = nullptr;
  ObSEArray<ObITask *, 2> new_tasks;
  if (OB_UNLIKELY(tablet_infos.count() > EXECUTOR_BATCH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lock row gc task generation input", KR(ret),
        "tablet_info_count", tablet_infos.count(), K(need_continuation));
  } else if (!tablet_infos.empty()) {
    if (OB_FAIL(dag.alloc_task(executor_task))) {
      LOG_WARN("fail to allocate lock row gc executor task", KR(ret),
          K_(ls_idx));
    } else if (OB_ISNULL(executor_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocated lock row gc executor task is null", KR(ret),
          K_(ls_idx));
    } else if (OB_FAIL(executor_task->init(
        ls_ctx_, tablet_infos, batch_idx_))) {
      LOG_WARN("fail to init lock row gc executor task", KR(ret),
          K_(ls_idx));
    }
  }

  if (OB_SUCC(ret)) {
    if (need_continuation) {
      // Executor and continuation are sibling tasks; the original barrier
      // waits for both.
      if (OB_FAIL(dag.alloc_task(continuation_task))) {
        LOG_WARN("fail to allocate lock row gc continuation task", KR(ret),
            K_(ls_idx));
      } else if (OB_ISNULL(continuation_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocated lock row gc continuation task is null", KR(ret),
            K_(ls_idx));
      } else if (OB_FAIL(continuation_task->init(
          ls_idx_, false /* is_ls_head */, batch_idx_ + 1))) {
        LOG_WARN("fail to init lock row gc continuation task", KR(ret),
            K_(ls_idx));
      } else if (OB_NOT_NULL(executor_task)
          && OB_FAIL(copy_children_to(*executor_task))) {
        LOG_WARN("fail to copy barrier to lock row gc executor", KR(ret),
            K_(ls_idx));
      } else if (OB_FAIL(copy_children_to(*continuation_task))) {
        LOG_WARN("fail to copy barrier to lock row gc continuation", KR(ret),
            K_(ls_idx));
      } else if (OB_NOT_NULL(executor_task)
          && OB_FAIL(add_child(*executor_task))) {
        LOG_WARN("fail to add lock row gc executor dependency", KR(ret),
            K_(ls_idx));
      } else if (OB_FAIL(add_child(*continuation_task))) {
        LOG_WARN("fail to add lock row gc continuation dependency", KR(ret),
            K_(ls_idx));
      } else if (OB_NOT_NULL(executor_task)
          && OB_FAIL(new_tasks.push_back(executor_task))) {
        LOG_WARN("fail to append lock row gc executor task", KR(ret),
            K_(ls_idx));
      } else if (OB_FAIL(new_tasks.push_back(continuation_task))) {
        LOG_WARN("fail to append lock row gc continuation task", KR(ret),
            K_(ls_idx));
      }
    } else {
      // A terminal batch only needs the executor. An empty terminal batch is
      // a successful no-op.
      if (OB_NOT_NULL(executor_task)) {
        if (OB_FAIL(copy_children_to(*executor_task))) {
          LOG_WARN("fail to copy barrier to lock row gc executor", KR(ret),
              K_(ls_idx));
        } else if (OB_FAIL(add_child(*executor_task))) {
          LOG_WARN("fail to add lock row gc executor dependency", KR(ret),
              K_(ls_idx));
        } else if (OB_FAIL(new_tasks.push_back(executor_task))) {
          LOG_WARN("fail to append lock row gc executor task", KR(ret),
              K_(ls_idx));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !new_tasks.empty()) {
    if (OB_FAIL(dag.batch_add_task(new_tasks))) {
      LOG_WARN("fail to add lock row gc dynamic tasks", KR(ret), K_(ls_idx),
          "task_count", new_tasks.count());
    }
  }
  return ret;
}

int ObSSLockRowGCScheduleTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_scan = false;
  bool need_continuation = false;
  ObSSLockRowGCDag *dag = nullptr;
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSEArray<ObSSGCTabletBaseInfo, EXECUTOR_BATCH_SIZE> tablet_infos;
  tablet_infos.set_attr(ObMemAttr(MTL_ID(), "LockGCTabInfos"));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock row gc schedule task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLockRowGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get lock row gc dag ctx", KR(ret), K_(ls_idx));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop lock row gc schedule task", KR(ret), K_(ls_idx),
        KPC(ctx));
  } else if (ctx->ls_ctxs_.empty()) {
    if (OB_UNLIKELY(0 != ls_idx_ || !is_ls_head_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid empty lock row gc bootstrap task", KR(ret),
          K_(ls_idx), K_(is_ls_head));
    }
  } else if (OB_FAIL(bind_ls_ctx_(*ctx))) {
    LOG_WARN("fail to bind lock row gc ls ctx", KR(ret), K_(ls_idx));
  } else if (OB_TMP_FAIL(init_ls_scan_(*ctx, need_scan))) {
    LOG_WARN("fail to init lock row gc ls scan", KR(tmp_ret), K_(ls_idx));
  } else if (!need_scan) {
    // The LS lock row was already removed by a concurrent GC attempt.
  } else if (OB_TMP_FAIL(fetch_tablet_infos_(
      *ctx, tablet_infos, need_continuation))) {
    LOG_WARN("fail to fetch lock row gc tablet infos", KR(tmp_ret),
        K_(ls_idx));
  } else if (OB_ISNULL(dag = static_cast<ObSSLockRowGCDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock row gc dag is null", KR(ret), K_(ls_idx));
  } else if (OB_FAIL(generate_tasks_(
      *dag, tablet_infos, need_continuation))) {
    LOG_WARN("fail to generate lock row gc tasks", KR(ret), K_(ls_idx),
        "tablet_info_count", tablet_infos.count(), K(need_continuation));
  }
  if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
    const int stop_ret = ObSSGCDagUtil::check_if_stopped(*this);
    if (OB_SUCCESS != stop_ret) {
      ret = stop_ret;
      LOG_INFO("stop lock row gc schedule task after business error", KR(ret),
          KR(tmp_ret), K_(ls_idx), KPC_(ls_ctx));
    } else {
      ls_ctx_->scan_failed_ = true;
      (void)ctx->record_error_code(*ls_ctx_, tmp_ret);
      LOG_WARN("defer lock row gc ls after business error", KR(tmp_ret),
          K_(ls_idx), KPC_(ls_ctx));
      tablet_infos.reset();
    }
  }
  LOG_INFO("finish lock row gc schedule task", KR(ret), K_(ls_idx),
      K_(is_ls_head), K_(batch_idx),
      "tablet_info_count", tablet_infos.count(), K(need_continuation),
      "dag_id", get_dag()->get_dag_id(), KPC(ctx), KPC_(ls_ctx));
  return ret;
}

ObSSLockRowGCExecutorTask::ObSSLockRowGCExecutorTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_LOCK_ROW_GC),
    is_inited_(false),
    batch_idx_(-1),
    ls_ctx_(nullptr),
    tablet_infos_()
{
}

ObSSLockRowGCExecutorTask::~ObSSLockRowGCExecutorTask()
{
  tablet_infos_.reset();
  batch_idx_ = -1;
  ls_ctx_ = nullptr;
  is_inited_ = false;
}

int ObSSLockRowGCExecutorTask::init(
    ObSSLockRowGCLSCtx *ls_ctx,
    const ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
    const int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock row gc executor task init twice", KR(ret));
  } else if (OB_ISNULL(ls_ctx) || OB_UNLIKELY(batch_idx < 0
      || !ls_ctx->is_valid()
      || tablet_infos.empty()
      || tablet_infos.count()
          > ObSSLockRowGCScheduleTask::EXECUTOR_BATCH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lock row gc executor task input", KR(ret), KP(ls_ctx),
        K(batch_idx), "tablet_info_count", tablet_infos.count());
  } else {
    tablet_infos_.set_attr(ObMemAttr(MTL_ID(), "LockGCExecInfo"));
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_infos.count(); ++i) {
      if (OB_UNLIKELY(!tablet_infos.at(i).is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lock row gc executor tablet info", KR(ret), K(i),
            K(tablet_infos.at(i)));
      } else if (OB_FAIL(tablet_infos_.push_back(tablet_infos.at(i)))) {
        LOG_WARN("fail to copy lock row gc executor tablet info", KR(ret),
            K(i));
      }
    }
    if (OB_SUCC(ret)) {
      batch_idx_ = batch_idx;
      ls_ctx_ = ls_ctx;
      is_inited_ = true;
    } else {
      tablet_infos_.reset();
    }
  }
  return ret;
}

int ObSSLockRowGCExecutorTask::do_tablet_lock_row_gc_work_(
    const ObSSLockRowGCDagCtx &ctx,
    const ObSSGCTabletBaseInfo &tablet_info,
    bool &delete_lock_row,
    int64_t &deleted_lock_row_count)
{
  int ret = OB_SUCCESS;
  bool can_delete_lock_row = false;
  ObSSMetaCtrlLockRowValue lock_row_value;
  ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "LockGCTablet"));
  ObString prefix_meta_key;
  const SCN min_meta_scn(SCN::min_scn());
  delete_lock_row = false;
  deleted_lock_row_count = 0;
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ctx.is_valid()
      || !ls_ctx_->is_valid()
      || !ls_ctx_->ls_lock_row_value_.is_valid()
      || !tablet_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lock row gc tablet input", KR(ret), K(ctx),
        KPC_(ls_ctx), K(tablet_info));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ctx.tenant_id),
        KPC_(ls_ctx));
  } else {
    const ObSSMetaCtrlKey tablet_lock_row_key(
        blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK,
        tablet_info.tablet_id_, tablet_info.reorg_scn_, min_meta_scn,
        ls_ctx_->ls_id_);
    ObSSMetaCtrlTabletLockValueIterator lock_row_iter;
    if (OB_FAIL(ObSSGCDagUtil::serialize_prefix_meta_key(
        tablet_lock_row_key, allocator, prefix_meta_key))) {
      LOG_WARN("fail to serialize tablet lock row prefix", KR(ret),
          K(tablet_lock_row_key), K(tablet_info));
    } else if (OB_FAIL(lock_row_iter.init(
        *GCTX.sql_proxy_, ctx.sql_timeout_us_, ctx.tenant_id_,
        ls_ctx_->ls_id_, prefix_meta_key))) {
      LOG_WARN("fail to init tablet lock row iterator", KR(ret), K(ctx),
          KPC_(ls_ctx), K(tablet_info));
    } else {
      const int iter_ret = lock_row_iter.get_next(lock_row_value);
      if (OB_ITER_END == iter_ret) {
        // Another observer may have deleted this row after Schedule scanned
        // it. Keep the LS lock conservatively and let a later round converge.
      } else if (OB_SUCCESS != iter_ret) {
        ret = iter_ret;
        LOG_WARN("fail to get tablet lock row value", KR(ret), K(ctx),
            KPC_(ls_ctx), K(tablet_info));
      } else if ((!lock_row_value.is_mark_deleted_
          && !tablet_info.tablet_id_.is_inner_tablet())
          || (!ls_ctx_->ls_lock_row_value_.is_mark_deleted_
          && tablet_info.tablet_id_.is_inner_tablet())) {
        // The lock row is not eligible for deletion yet.
      } else if (OB_FAIL(check_can_delete_tablet_lock_row_(
          ctx, tablet_lock_row_key, tablet_info, can_delete_lock_row))) {
        LOG_WARN("fail to check whether tablet lock rows can be deleted",
            KR(ret), K(ctx), KPC_(ls_ctx), K(tablet_info));
      } else if (!can_delete_lock_row) {
        // Tablet Meta or a non-lock control row still exists.
      } else if (OB_FAIL(delete_tablet_lock_rows_(
          ctx, tablet_lock_row_key, deleted_lock_row_count))) {
        LOG_WARN("fail to delete tablet lock rows", KR(ret), K(ctx),
            KPC_(ls_ctx), K(tablet_info));
      } else {
        delete_lock_row = true;
        FLOG_INFO("tablet lock rows are all deleted", K(ctx),
            KPC_(ls_ctx), K(tablet_info));
      }
    }
  }
  return ret;
}

int ObSSLockRowGCExecutorTask::check_can_delete_tablet_lock_row_(
    const ObSSLockRowGCDagCtx &ctx,
    const ObSSMetaCtrlKey &tablet_lock_row_key,
    const ObSSGCTabletBaseInfo &tablet_info,
    bool &can_delete)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = nullptr;
  ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "LockGCMeta"));
  ObTabletHandle tablet_handle;
  can_delete = false;
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ctx.is_valid()
      || !ls_ctx_->is_valid()
      || !tablet_lock_row_key.is_valid()
      || !tablet_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet lock row deletion check input", KR(ret),
        K(ctx), KPC_(ls_ctx), K(tablet_lock_row_key), K(tablet_info));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K(ctx),
        KPC_(ls_ctx), K(tablet_info));
  } else if (OB_FAIL(meta_service->get_tablet(
      ls_ctx_->ls_id_, tablet_info.tablet_id_, tablet_info.reorg_scn_,
      allocator, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      can_delete = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet from shared meta service", KR(ret),
          K(ctx), KPC_(ls_ctx), K(tablet_info));
    }
  }

  if (OB_SUCC(ret) && can_delete) {
    bool ctrl_value_exist = false;
    if (OB_FAIL(check_tablet_ctrl_value_exist_(
        ctx, tablet_lock_row_key, ctrl_value_exist))) {
      LOG_WARN("fail to check tablet control value", KR(ret), K(ctx),
          KPC_(ls_ctx), K(tablet_lock_row_key), K(tablet_info));
    } else {
      can_delete = !ctrl_value_exist;
      if (can_delete) {
        LOG_INFO("tablet lock rows can be deleted", K(ctx), KPC_(ls_ctx),
            K(tablet_info));
      } else {
        LOG_DEBUG("tablet still has a control row", K(ctx), KPC_(ls_ctx),
            K(tablet_info));
      }
    }
  }
  return ret;
}

int ObSSLockRowGCExecutorTask::check_tablet_ctrl_value_exist_(
    const ObSSLockRowGCDagCtx &ctx,
    const ObSSMetaCtrlKey &tablet_lock_row_key,
    bool &ctrl_value_exist)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "LockGCCtrl"));
  ObString prefix_meta_key;
  ObSSMetaCtrlValueIterator value_iter;
  uint64_t unused_tenant_id = OB_INVALID_ID;
  ObLSID unused_ls_id;
  ObSSMetaCtrlKey unused_key;
  ObSSMetaCtrlValue unused_value;
  ctrl_value_exist = false;
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ctx.is_valid()
      || !ls_ctx_->is_valid() || !tablet_lock_row_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet control value check input", KR(ret), K(ctx),
        KPC_(ls_ctx), K(tablet_lock_row_key));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ctx.tenant_id),
        KPC_(ls_ctx));
  } else if (OB_FAIL(ObSSGCDagUtil::serialize_prefix_meta_key(
      tablet_lock_row_key, allocator, prefix_meta_key))) {
    LOG_WARN("fail to serialize tablet control row prefix", KR(ret),
        K(tablet_lock_row_key));
  } else if (OB_FAIL(value_iter.init(
      *GCTX.sql_proxy_, ctx.sql_timeout_us_, ctx.tenant_id_,
      ls_ctx_->ls_id_, prefix_meta_key))) {
    LOG_WARN("fail to init tablet control value iterator", KR(ret), K(ctx),
        KPC_(ls_ctx), K(tablet_lock_row_key));
  } else if (OB_FAIL(value_iter.get_next(
      unused_tenant_id, unused_ls_id, unused_key, unused_value))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet control value", KR(ret), K(ctx),
          KPC_(ls_ctx), K(tablet_lock_row_key));
    }
  } else {
    ctrl_value_exist = true;
  }
  return ret;
}

int ObSSLockRowGCExecutorTask::delete_tablet_lock_rows_(
    const ObSSLockRowGCDagCtx &ctx,
    const ObSSMetaCtrlKey &tablet_lock_row_key,
    int64_t &deleted_lock_row_count)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlLockValueIterator lock_row_iter;
  ObString prefix_meta_key;
  ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "LockGCDelRow"));
  ObMySQLTransaction trans;
  const bool with_snapshot = false;
  const int32_t group_id = OBCG_STORAGE;
  ObTimeoutCtx timeout_ctx;
  ObSSMetaCtrlTableOperator table_operator;
  deleted_lock_row_count = 0;
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ctx.is_valid()
      || ctx.sql_timeout_us_ <= 0
      || !ls_ctx_->is_valid()
      || !ctx.owns_ls_ctx_(*ls_ctx_)
      || !tablet_lock_row_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet lock row deletion input", KR(ret), K(ctx),
        KPC_(ls_ctx), K(tablet_lock_row_key));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ctx.tenant_id),
        KPC_(ls_ctx));
  } else if (OB_FAIL(ObSSGCDagUtil::serialize_prefix_meta_key(
      tablet_lock_row_key, allocator, prefix_meta_key))) {
    LOG_WARN("fail to serialize tablet lock row prefix", KR(ret),
        K(tablet_lock_row_key));
  } else if (OB_FAIL(lock_row_iter.init(
      *GCTX.sql_proxy_, ctx.sql_timeout_us_, ctx.tenant_id_,
      ls_ctx_->ls_id_, prefix_meta_key))) {
    LOG_WARN("fail to init tablet lock row deletion iterator", KR(ret),
        K(ctx), KPC_(ls_ctx), K(tablet_lock_row_key));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
      ctx.tenant_id_, ctx.sql_timeout_us_, with_snapshot, group_id,
      timeout_ctx, trans))) {
    LOG_WARN("fail to start tablet lock row deletion transaction", KR(ret),
        K(ctx), KPC_(ls_ctx), K(group_id));
  } else {
    ObSSMetaCtrlLockRowValue unused_lock_row_value;
    uint64_t unused_tenant_id = OB_INVALID_ID;
    ObLSID unused_ls_id;
    ObSSMetaCtrlKey ctrl_key;
    ObString row_meta_key;
    int64_t affected_rows = 0;
    int64_t transaction_deleted_lock_row_count = 0;
    if (OB_FAIL(table_operator.init(trans, ctx.sql_timeout_us_))) {
      LOG_WARN("fail to init tablet lock row table operator", KR(ret),
          K(ctx), KPC_(ls_ctx));
    } else {
      while (OB_SUCC(ret)) {
        allocator.reset();
        unused_lock_row_value.reset();
        ctrl_key.reset();
        row_meta_key.reset();
        affected_rows = 0;
        if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
          LOG_INFO("stop deleting tablet lock rows", KR(ret), K(ctx),
              KPC_(ls_ctx), K(tablet_lock_row_key));
        } else if (OB_FAIL(lock_row_iter.get_next(
            unused_tenant_id, unused_ls_id, ctrl_key,
            unused_lock_row_value))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next tablet lock row", KR(ret), K(ctx),
                KPC_(ls_ctx), K(tablet_lock_row_key));
          }
        } else if (OB_FAIL(ObSSGCDagUtil::serialize_meta_key(
            ctrl_key, allocator, row_meta_key))) {
          LOG_WARN("fail to serialize tablet lock row key", KR(ret),
              K(ctrl_key));
        } else if (OB_FAIL(table_operator.delete_row(
            ctx.tenant_id_, ls_ctx_->ls_id_, ctrl_key.type_, row_meta_key,
            false /* prefix_delete */, affected_rows))) {
          LOG_WARN("fail to delete tablet lock row", KR(ret), K(ctx),
              KPC_(ls_ctx), K(ctrl_key));
        } else if (OB_UNLIKELY(0 != affected_rows && 1 != affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected deleted tablet lock row count", KR(ret),
              K(ctx), KPC_(ls_ctx), K(ctrl_key), K(affected_rows));
        } else {
          transaction_deleted_lock_row_count += affected_rows;
        }
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(ret, trans))) {
      LOG_WARN("fail to end tablet lock row deletion transaction",
          KR(tmp_ret), KR(ret), K(ctx), KPC_(ls_ctx));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      deleted_lock_row_count = transaction_deleted_lock_row_count;
    }
  }
  return ret;
}

int ObSSLockRowGCExecutorTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSLockRowGCDagCtx *ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(ls_ctx_)
      || !ls_ctx_->is_valid() || tablet_infos_.empty())) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock row gc executor task is not initialized", KR(ret),
        K_(is_inited), KP_(ls_ctx),
        "tablet_info_count", tablet_infos_.count());
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLockRowGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get lock row gc dag ctx", KR(ret), KPC_(ls_ctx));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop lock row gc executor task", KR(ret), KPC(ctx),
        KPC_(ls_ctx));
  } else if (OB_UNLIKELY(!ctx->owns_ls_ctx_(*ls_ctx_)
      || ctx->sql_timeout_us_ <= 0
      || !ls_ctx_->ls_lock_row_value_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock row gc executor context", KR(ret), KPC(ctx),
        KPC_(ls_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_SUCCESS == tmp_ret
        && i < tablet_infos_.count(); ++i) {
      if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
        LOG_INFO("stop processing tablet lock rows", KR(ret), K(i),
            KPC_(ls_ctx));
      } else {
        bool delete_lock_row = false;
        int64_t deleted_lock_row_count = 0;
        if (OB_TMP_FAIL(do_tablet_lock_row_gc_work_(
            *ctx, tablet_infos_.at(i), delete_lock_row,
            deleted_lock_row_count))) {
          ls_ctx_->mark_tablet_lock_rows_not_all_deleted();
          LOG_WARN("fail to process tablet lock row", KR(tmp_ret), K(i),
              K(tablet_infos_.at(i)), KPC_(ls_ctx));
        } else {
          if (!delete_lock_row) {
            ls_ctx_->mark_tablet_lock_rows_not_all_deleted();
          } else {
            ctx->add_deleted_tablet_lock_row_count(
                deleted_lock_row_count);
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
      const int stop_ret = ObSSGCDagUtil::check_if_stopped(*this);
      if (OB_SUCCESS != stop_ret) {
        ret = stop_ret;
        LOG_INFO("stop lock row gc executor task after business error",
            KR(ret), KR(tmp_ret), K_(batch_idx), KPC_(ls_ctx));
      } else {
        (void)ctx->record_error_code(*ls_ctx_, tmp_ret);
        LOG_WARN("defer lock row gc executor after business error",
            KR(tmp_ret), K_(batch_idx), KPC_(ls_ctx));
      }
    }
  }
  LOG_INFO("finish lock row gc executor task", KR(ret), K_(batch_idx),
      "dag_id", get_dag()->get_dag_id(), KPC(ctx), KPC_(ls_ctx),
      "tablet_info_count", tablet_infos_.count());
  return ret;
}

ObSSLockRowGCDeleteLSLockTask::ObSSLockRowGCDeleteLSLockTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_LOCK_ROW_GC),
    is_inited_(false)
{
}

int ObSSLockRowGCDeleteLSLockTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock row gc delete ls lock task init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSLockRowGCDeleteLSLockTask::check_ls_meta_exist_(
    const ObLSID &ls_id,
    bool &ls_meta_exist)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = nullptr;
  ObSSLSMeta ls_meta;
  ls_meta_exist = false;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta existence check input", KR(ret), K(ls_id));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K(ls_id));
  } else if (OB_FAIL(meta_service->get_ls_meta(ls_id, ls_meta))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get latest ls meta", KR(ret), K(ls_id));
    }
  } else {
    ls_meta_exist = true;
  }
  return ret;
}

int ObSSLockRowGCDeleteLSLockTask::delete_ls_lock_row_(
    const ObSSLockRowGCDagCtx &ctx,
    const ObSSLockRowGCLSCtx &ls_ctx,
    int64_t &deleted_lock_row_count)
{
  int ret = OB_SUCCESS;
  bool ls_meta_exist = false;
  ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "LockGCDelLS"));
  ObString row_meta_key;
  ObMySQLTransaction trans;
  const bool with_snapshot = false;
  const int32_t group_id = OBCG_STORAGE;
  ObTimeoutCtx timeout_ctx;
  ObSSMetaCtrlTableOperator table_operator;
  int64_t affected_rows = 0;
  deleted_lock_row_count = 0;
  if (OB_UNLIKELY(!ctx.is_valid() || ctx.sql_timeout_us_ <= 0
      || !ls_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls lock row deletion input", KR(ret), K(ctx),
        K(ls_ctx));
  } else if (OB_FAIL(check_ls_meta_exist_(
      ls_ctx.ls_id_, ls_meta_exist))) {
    LOG_WARN("fail to check latest ls meta", KR(ret), K(ctx), K(ls_ctx));
  } else if (ls_meta_exist) {
    LOG_INFO("latest ls meta still exists, keep ls lock row", K(ctx),
        K(ls_ctx));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ctx.tenant_id), K(ls_ctx));
  } else if (OB_FAIL(ObSSGCDagUtil::serialize_meta_key(
      ls_ctx.ls_lock_row_key_, allocator, row_meta_key))) {
    LOG_WARN("fail to serialize ls lock row key", KR(ret), K(ctx),
        K(ls_ctx));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
      ctx.tenant_id_, ctx.sql_timeout_us_, with_snapshot, group_id,
      timeout_ctx, trans))) {
    LOG_WARN("fail to start ls lock row deletion transaction", KR(ret),
        K(ctx), K(ls_ctx), K(group_id));
  } else {
    if (OB_FAIL(table_operator.init(trans, ctx.sql_timeout_us_))) {
      LOG_WARN("fail to init ls lock row table operator", KR(ret), K(ctx),
          K(ls_ctx));
    } else if (OB_FAIL(table_operator.delete_row(
        ctx.tenant_id_, ls_ctx.ls_id_, ls_ctx.ls_lock_row_key_.type_,
        row_meta_key, false /* prefix_delete */, affected_rows))) {
      LOG_WARN("fail to delete ls lock row", KR(ret), K(ctx), K(ls_ctx));
    } else if (OB_UNLIKELY(0 != affected_rows && 1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected deleted ls lock row count", KR(ret), K(ctx),
          K(ls_ctx), K(affected_rows));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(ret, trans))) {
      LOG_WARN("fail to end ls lock row deletion transaction", KR(tmp_ret),
          KR(ret), K(ctx), K(ls_ctx));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      deleted_lock_row_count = affected_rows;
    }
    FLOG_INFO("finish deleting ls lock row", KR(ret), K(ctx), K(ls_ctx),
        K(affected_rows));
  }
  return ret;
}

int ObSSLockRowGCDeleteLSLockTask::process()
{
  int ret = OB_SUCCESS;
  ObSSLockRowGCDagCtx *ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock row gc delete ls lock task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLockRowGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get lock row gc dag ctx", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop lock row gc delete ls lock task", KR(ret), KPC(ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx->ls_ctxs_.count(); ++i) {
      ObSSLockRowGCLSCtx *ls_ctx = ctx->ls_ctxs_.at(i);
      if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
        LOG_INFO("stop deleting lock row gc ls locks", KR(ret), K(i),
            KPC(ctx));
      } else if (OB_ISNULL(ls_ctx) || OB_UNLIKELY(!ls_ctx->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lock row gc ls context", KR(ret), K(i),
            KPC(ls_ctx), KPC(ctx));
      } else if (ls_ctx->scan_finished_
          && !ls_ctx->scan_failed_
          && ls_ctx->ls_lock_row_value_.is_mark_deleted_
          && ls_ctx->are_all_tablet_lock_rows_deleted()) {
        int64_t deleted_lock_row_count = 0;
        const int tmp_ret = delete_ls_lock_row_(
            *ctx, *ls_ctx, deleted_lock_row_count);
        if (OB_SUCCESS != tmp_ret) {
          const int stop_ret = ObSSGCDagUtil::check_if_stopped(*this);
          if (OB_SUCCESS != stop_ret) {
            ret = stop_ret;
            LOG_INFO("stop deleting ls lock rows after business error",
                KR(ret), KR(tmp_ret), K(i), KPC(ls_ctx));
          } else {
            (void)ctx->record_error_code(*ls_ctx, tmp_ret);
            LOG_WARN("defer ls lock row after business error", KR(tmp_ret),
                K(i), KPC(ls_ctx));
          }
        } else {
          ctx->add_deleted_ls_lock_row_count(deleted_lock_row_count);
        }
      }
    }
  }
  LOG_INFO("finish lock row gc delete ls lock task", KR(ret),
      "dag_id", get_dag()->get_dag_id(), KPC(ctx));
  return ret;
}

} // namespace storage
} // namespace oceanbase
