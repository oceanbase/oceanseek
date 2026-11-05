/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/dag/ob_ss_fail_gc_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_all_server_tracer.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_ctrl.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ss_garbage_collector/ob_ss_gc_rpc.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_FAIL_GC_DO_CTRL_ROW_FAIL);
ERRSIM_POINT_DEF(EN_SS_FAIL_GC_GENERATE_TASK_FAIL);
#endif

int ObSSFailGCDagUtil::get_fail_gc_scn(
    const uint64_t tenant_id,
    bool &need_gc,
    SCN &gc_scn)
{
  int ret = OB_SUCCESS;
  SCN cur_gts;
  int64_t shared_meta_operation_timeout_us = 0;
  int64_t shared_meta_operation_timeout_ns = 0;
  need_gc = false;
  gc_scn.reset();

  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
    } else {
      shared_meta_operation_timeout_us = tenant_config->_shared_meta_operation_timeout;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == shared_meta_operation_timeout_us) {
    LOG_INFO("fail gc is disabled by shared meta operation timeout",
        K(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_gts(tenant_id, cur_gts))) {
    LOG_WARN("fail to get gts", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!cur_gts.is_valid()
      || cur_gts.is_min()
      || cur_gts.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid current gts", KR(ret), K(tenant_id), K(cur_gts));
  } else {
    shared_meta_operation_timeout_ns =
        shared_meta_operation_timeout_us * 1000L;
    const int64_t cur_gts_val =
        static_cast<int64_t>(cur_gts.get_val_for_gts());
    if (OB_UNLIKELY(cur_gts_val <= shared_meta_operation_timeout_ns)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current gts is not larger than fail gc timeout", KR(ret),
          K(tenant_id), K(cur_gts), K(shared_meta_operation_timeout_us),
          K(shared_meta_operation_timeout_ns));
    } else {
      need_gc = true;
      gc_scn = SCN::minus(cur_gts, shared_meta_operation_timeout_ns);
    }
  }

  if (OB_SUCC(ret) && need_gc && OB_UNLIKELY(!gc_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fail gc scn", KR(ret), K(tenant_id), K(gc_scn));
  }
  return ret;
}

int ObSSFailGCDagUtil::get_palf_kv_checkpoint_meta_scn(
    SCN &checkpoint_meta_scn)
{
  int ret = OB_SUCCESS;
  checkpoint_meta_scn.reset();
  //TODO(muwei.ym): get palf kv checkpoint meta scn
  return ret;
}

ObSSFailGCCtrlRow::ObSSFailGCCtrlRow()
  : ctrl_key_(),
    ctrl_value_()
{
}

void ObSSFailGCCtrlRow::reset()
{
  ctrl_key_.reset();
  ctrl_value_.reset();
}

bool ObSSFailGCCtrlRow::is_valid() const
{
  return ctrl_key_.is_valid() && ctrl_value_.is_valid();
}

int ObSSFailGCCtrlRow::assign(const ObSSFailGCCtrlRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc ctrl row", KR(ret), K(row));
  } else if (this == &row) {
    // do nothing
  } else if (OB_FAIL(ctrl_key_.assign(row.ctrl_key_))) {
    LOG_WARN("fail to assign fail gc ctrl key", KR(ret), K(row));
  } else {
    ctrl_value_ = row.ctrl_value_;
  }
  return ret;
}

ObSSFailGCInfo::ObSSFailGCInfo()
  : key_info_(),
    ctrl_value_(),
    need_generate_task_(false)
{
}

void ObSSFailGCInfo::reset()
{
  key_info_.reset();
  ctrl_value_.reset();
  need_generate_task_ = false;
}

bool ObSSFailGCInfo::is_valid() const
{
  return key_info_.is_valid() && ctrl_value_.is_valid();
}

int ObSSFailGCInfo::assign(const ObSSFailGCInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss fail gc info", KR(ret), K(info));
  } else if (this == &info) {
    // do nothing
  } else if (OB_FAIL(key_info_.assign(info.key_info_))) {
    LOG_WARN("fail to assign key info", KR(ret), K(info));
  } else {
    ctrl_value_ = info.ctrl_value_;
    need_generate_task_ = info.need_generate_task_;
  }
  return ret;
}

ObSSFailGCLSCtx::ObSSFailGCLSCtx()
  : ls_id_(),
    checkpoint_meta_scn_(),
    page_iter_(),
    iterator_inited_(false),
    scan_finished_(false),
    scan_failed_(false),
    first_error_code_(OB_SUCCESS)
{
}

ObSSFailGCLSCtx::~ObSSFailGCLSCtx()
{
  reset();
}

void ObSSFailGCLSCtx::reset()
{
  ls_id_.reset();
  checkpoint_meta_scn_.reset();
  page_iter_.reset();
  iterator_inited_ = false;
  scan_finished_ = false;
  scan_failed_ = false;
  ATOMIC_STORE(&first_error_code_, OB_SUCCESS);
}

bool ObSSFailGCLSCtx::is_valid() const
{
  return ls_id_.is_valid();
}

bool ObSSFailGCLSCtx::record_first_error_code(const int error_code)
{
  return OB_SUCCESS != error_code
      && ATOMIC_BCAS(&first_error_code_, OB_SUCCESS, error_code);
}

int ObSSFailGCLSCtx::get_first_error_code() const
{
  return ATOMIC_LOAD(&first_error_code_);
}

ObSSFailGCDagCtx::ObSSFailGCDagCtx()
  : tenant_id_(OB_INVALID_TENANT_ID),
    sql_timeout_us_(0),
    gc_scn_(),
    need_gc_(false),
    ls_ctxs_(),
    ls_count_(0),
    first_error_code_(OB_SUCCESS),
    failed_ls_count_(0),
    scanned_ctrl_row_count_(0),
    abandoned_ctrl_row_count_(0),
    generated_task_count_(0)
{
}

ObSSFailGCDagCtx::~ObSSFailGCDagCtx()
{
  reset();
}

int ObSSFailGCDagCtx::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail gc dag ctx init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    ls_ctxs_.set_attr(ObMemAttr(tenant_id, "FailGCLSCtxArr"));
  }
  return ret;
}

void ObSSFailGCDagCtx::reset()
{
  for (int64_t i = 0; i < ls_ctxs_.count(); ++i) {
    ObSSFailGCLSCtx *ls_ctx = ls_ctxs_.at(i);
    if (OB_NOT_NULL(ls_ctx)) {
      ls_ctx->~ObSSFailGCLSCtx();
      mtl_free(ls_ctx);
    }
  }
  ls_ctxs_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_timeout_us_ = 0;
  gc_scn_.reset();
  need_gc_ = false;
  ATOMIC_STORE(&ls_count_, 0);
  ATOMIC_STORE(&first_error_code_, OB_SUCCESS);
  ATOMIC_STORE(&failed_ls_count_, 0);
  ATOMIC_STORE(&scanned_ctrl_row_count_, 0);
  ATOMIC_STORE(&abandoned_ctrl_row_count_, 0);
  ATOMIC_STORE(&generated_task_count_, 0);
}

bool ObSSFailGCDagCtx::is_valid() const
{
  return is_valid_tenant_id(tenant_id_);
}

int ObSSFailGCDagCtx::alloc_ls_ctx(
    const ObLSID &ls_id,
    ObSSFailGCLSCtx *&ls_ctx)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ls_ctx = nullptr;
  if (OB_UNLIKELY(!is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(ls_id));
  } else if (OB_ISNULL(buf = mtl_malloc(
      sizeof(ObSSFailGCLSCtx), ObMemAttr(tenant_id_, "FailGCLSCtx")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate fail gc ls ctx", KR(ret), K_(tenant_id), K(ls_id));
  } else {
    ls_ctx = new (buf) ObSSFailGCLSCtx();
    ls_ctx->ls_id_ = ls_id;
    if (OB_FAIL(ls_ctxs_.push_back(ls_ctx))) {
      LOG_WARN("fail to append fail gc ls ctx", KR(ret), K_(tenant_id), K(ls_id));
      ls_ctx->~ObSSFailGCLSCtx();
      mtl_free(ls_ctx);
      ls_ctx = nullptr;
    } else {
      (void)ATOMIC_AAF(&ls_count_, 1);
    }
  }
  return ret;
}

void ObSSFailGCDagCtx::add_scanned_ctrl_row_count(const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&scanned_ctrl_row_count_, count);
  }
}

void ObSSFailGCDagCtx::add_abandoned_ctrl_row_count(const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&abandoned_ctrl_row_count_, count);
  }
}

void ObSSFailGCDagCtx::add_generated_task_count(const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&generated_task_count_, count);
  }
}

int64_t ObSSFailGCDagCtx::get_ls_count() const
{
  return ATOMIC_LOAD(&ls_count_);
}

bool ObSSFailGCDagCtx::record_error_code(
    ObSSFailGCLSCtx &ls_ctx,
    const int error_code)
{
  bool is_first_ls_error = false;
  if (OB_SUCCESS == error_code) {
    // Nothing to record.
  } else if (OB_UNLIKELY(!is_valid() || !ls_ctx.is_valid()
      || !owns_ls_ctx_(ls_ctx))) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid or unowned fail gc ls ctx",
        K_(tenant_id), K(ls_ctx), K(error_code));
  } else {
    is_first_ls_error = ls_ctx.record_first_error_code(error_code);
    if (is_first_ls_error) {
      (void)ATOMIC_BCAS(&first_error_code_, OB_SUCCESS, error_code);
      (void)ATOMIC_AAF(&failed_ls_count_, 1);
    }
  }
  return is_first_ls_error;
}

bool ObSSFailGCDagCtx::owns_ls_ctx_(const ObSSFailGCLSCtx &ls_ctx) const
{
  bool owns_ls_ctx = false;
  for (int64_t i = 0; !owns_ls_ctx && i < ls_ctxs_.count(); ++i) {
    owns_ls_ctx = &ls_ctx == ls_ctxs_.at(i);
  }
  return owns_ls_ctx;
}

int ObSSFailGCDagCtx::get_first_error_code() const
{
  return ATOMIC_LOAD(&first_error_code_);
}

int64_t ObSSFailGCDagCtx::get_failed_ls_count() const
{
  return ATOMIC_LOAD(&failed_ls_count_);
}

int64_t ObSSFailGCDagCtx::get_scanned_ctrl_row_count() const
{
  return ATOMIC_LOAD(&scanned_ctrl_row_count_);
}

int64_t ObSSFailGCDagCtx::get_abandoned_ctrl_row_count() const
{
  return ATOMIC_LOAD(&abandoned_ctrl_row_count_);
}

int64_t ObSSFailGCDagCtx::get_generated_task_count() const
{
  return ATOMIC_LOAD(&generated_task_count_);
}

ObSSFailGCDagParam::ObSSFailGCDagParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    schedule_ts_(0)
{
}

int ObSSFailGCDagParam::init(
    const uint64_t tenant_id,
    const int64_t schedule_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail gc dag param init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || schedule_ts < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc dag param", KR(ret), K(tenant_id),
        K(schedule_ts));
  } else {
    tenant_id_ = tenant_id;
    schedule_ts_ = schedule_ts;
  }
  return ret;
}

bool ObSSFailGCDagParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && schedule_ts_ >= 0;
}

ObSSFailGCDag::ObSSFailGCDag()
  : ObSSGCDagBase(DAG_TYPE),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    schedule_ts_(0),
    start_event_reported_(0),
    ctx_(nullptr)
{
}

ObSSFailGCDag::~ObSSFailGCDag()
{
  free_ctx_();
  tenant_id_ = OB_INVALID_TENANT_ID;
  schedule_ts_ = 0;
  ATOMIC_STORE(&start_event_reported_, 0);
  is_inited_ = false;
}

int ObSSFailGCDag::alloc_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(OB_NOT_NULL(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail gc dag ctx already allocated", KR(ret), KP_(ctx));
  } else if (OB_ISNULL(buf = mtl_malloc(
      sizeof(ObSSFailGCDagCtx), ObMemAttr(MTL_ID(), "FailGCDagCtx")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate fail gc dag ctx", KR(ret));
  } else {
    ctx_ = new (buf) ObSSFailGCDagCtx();
  }
  return ret;
}

void ObSSFailGCDag::free_ctx_()
{
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObSSFailGCDagCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

bool ObSSFailGCDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() == other.get_type()) {
    const ObSSFailGCDag &other_dag = static_cast<const ObSSFailGCDag &>(other);
    bret = tenant_id_ == other_dag.tenant_id_;
  }
  return bret;
}

uint64_t ObSSFailGCDag::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  return hash_val;
}

int ObSSFailGCDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  char stat_buf[128] = {0};
  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail gc dag ctx is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(databuff_printf(stat_buf, sizeof(stat_buf),
      "scanned=%ld,abandoned=%ld,generated=%ld",
      ctx_->get_scanned_ctrl_row_count(),
      ctx_->get_abandoned_ctrl_row_count(),
      ctx_->get_generated_task_count()))) {
    LOG_WARN("fail to format fail gc dag statistics", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(
      out_param, allocator, get_type(), static_cast<int64_t>(tenant_id_),
      static_cast<int64_t>(GCTX.get_server_id()), ctx_->get_ls_count(),
      static_cast<int64_t>(ctx_->get_first_error_code()),
      ctx_->get_failed_ls_count(), "server_addr", GCONF.self_addr_,
      "row_stat", stat_buf))) {
    LOG_WARN("fail to fill fail gc dag info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSFailGCDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "tenant_id=%lu", tenant_id_))) {
    LOG_WARN("fail to fill fail gc dag key", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSFailGCDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSFailGCDagParam *dag_param =
      static_cast<const ObSSFailGCDagParam *>(param);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail gc dag init twice", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(dag_param) || OB_UNLIKELY(!dag_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc dag param", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(alloc_ctx_())) {
    LOG_WARN("fail to allocate fail gc dag ctx", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(ctx_->init(dag_param->tenant_id_))) {
    LOG_WARN("fail to init fail gc dag ctx", KR(ret), KPC(dag_param));
  } else {
    tenant_id_ = dag_param->tenant_id_;
    schedule_ts_ = dag_param->schedule_ts_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSFailGCDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSFailGCPrepareTask *prepare_task = nullptr;
  ObSSFailGCScheduleTask *schedule_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)
      || OB_ISNULL(ctx_)
      || OB_UNLIKELY(!ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail gc dag is not initialized", KR(ret), K_(tenant_id),
        K_(is_inited), KPC_(ctx));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("fail to allocate fail gc prepare task", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(prepare_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated fail gc prepare task is null", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(alloc_task(schedule_task))) {
    LOG_WARN("fail to allocate fail gc schedule task", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(schedule_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated fail gc schedule task is null", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(prepare_task->init())) {
    LOG_WARN("fail to init fail gc prepare task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schedule_task->init(0, true /* is_ls_head */))) {
    LOG_WARN("fail to init fail gc bootstrap schedule task", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(prepare_task->add_child(*schedule_task))) {
    LOG_WARN("fail to add prepare to schedule edge", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    LOG_WARN("fail to add fail gc prepare task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(add_task(*schedule_task))) {
    LOG_WARN("fail to add fail gc schedule task", KR(ret), K_(tenant_id));
  }
  LOG_INFO("finish to create fail gc dag first tasks", KR(ret), K_(tenant_id),
      KPC_(ctx));
  return ret;
}

void ObSSFailGCDag::report_start_event()
{
  if (ATOMIC_BCAS(&start_event_reported_, 0, 1)) {
    SERVER_EVENT_ADD("SHARED_GC", "START_FAIL_GC_DAG",
        "tenant_id", tenant_id_,
        "schedule_ts", schedule_ts_,
        "dag_id", get_dag_id(),
        "dag_start_time_us", get_start_time(),
        "trace_id", ObCurTraceId::get_trace_id_str());
  }
}

int ObSSFailGCDag::report_result()
{
  const int dag_ret = get_dag_ret();
  const ObIDag::ObDagStatus dag_status = get_dag_status();
  const int64_t finish_time_us = ObTimeUtility::fast_current_time_us();
  const int64_t start_time_us = get_start_time();
  const int64_t cost_us = start_time_us > 0 && finish_time_us >= start_time_us
      ? finish_time_us - start_time_us : 0;
  char stat_buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
  if (OB_ISNULL(ctx_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail gc dag ctx is null when reporting",
        K_(tenant_id), K(dag_ret), K(dag_status));
  } else {
    (void) databuff_printf(stat_buf, sizeof(stat_buf),
        "failed_ls_count=%ld,ls_count=%ld,scanned_ctrl_row_count=%ld,"
        "abandoned_ctrl_row_count=%ld,generated_task_count=%ld,cost_us=%ld",
        ctx_->get_failed_ls_count(), ctx_->get_ls_count(),
        ctx_->get_scanned_ctrl_row_count(),
        ctx_->get_abandoned_ctrl_row_count(),
        ctx_->get_generated_task_count(), cost_us);
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_FAIL_GC_DAG",
        "tenant_id", tenant_id_,
        "schedule_ts", schedule_ts_,
        "dag_ret", dag_ret,
        "dag_status", static_cast<int64_t>(dag_status),
        "dag_id", get_dag_id(),
        "first_error_code", ctx_->get_first_error_code(),
        stat_buf /* extra_info */);
  }
  LOG_INFO("finish fail gc dag", K_(tenant_id), K(dag_ret), K(dag_status),
      K(cost_us), K(stat_buf), KPC_(ctx));
  return OB_SUCCESS;
}

ObSSFailGCPrepareTask::ObSSFailGCPrepareTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_FAIL_GC),
    is_inited_(false)
{
}

int ObSSFailGCPrepareTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail gc prepare task init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSFailGCPrepareTask::process()
{
  int ret = OB_SUCCESS;
  bool need_gc = false;
  SCN gc_scn;
  ObSSFailGCDagCtx *ctx = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail gc prepare task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSFailGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get fail gc dag ctx", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop fail gc prepare task", KR(ret), KPC(ctx));
  } else if (FALSE_IT(
      static_cast<ObSSFailGCDag *>(get_dag())->report_start_event())) {
  } else if (OB_FAIL(ObSSGCUtil::get_shared_gc_sql_timeout_us(
      ctx->tenant_id_, ctx->sql_timeout_us_))) {
    LOG_WARN("fail to get shared gc sql timeout", KR(ret), K_(ctx->tenant_id));
  } else if (OB_FAIL(ObSSFailGCDagUtil::get_fail_gc_scn(
      ctx->tenant_id_, need_gc, gc_scn))) {
    LOG_WARN("fail to get fail gc scn", KR(ret), K_(ctx->tenant_id));
  } else {
    ObArray<ObLSID> ls_ids(OB_MALLOC_NORMAL_BLOCK_SIZE,
        ModulePageAllocator(ObMemAttr(ctx->tenant_id_, "FailGCLSIds")));
    ctx->need_gc_ = need_gc;
    ctx->gc_scn_ = gc_scn;
    if (!need_gc) {
      // Fail GC is disabled for this round.
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret), K_(ctx->tenant_id));
    } else if (OB_FAIL(ObSSGCDagUtil::get_all_ls_ids_from_meta_ctrl(
        ctx->tenant_id_, *GCTX.sql_proxy_, ctx->sql_timeout_us_, ls_ids,
        this))) {
      LOG_WARN("fail to get fail gc ls ids", KR(ret), K_(ctx->tenant_id),
          K_(ctx->sql_timeout_us));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
        ObSSFailGCLSCtx *ls_ctx = nullptr;
        if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
          LOG_INFO("stop allocating fail gc ls contexts", KR(ret), K(i),
              KPC(ctx));
        } else if (OB_FAIL(ctx->alloc_ls_ctx(ls_ids.at(i), ls_ctx))) {
          LOG_WARN("fail to allocate fail gc ls ctx", KR(ret),
              K_(ctx->tenant_id), K(ls_ids.at(i)));
        }
      }
    }
  }

  LOG_INFO("finish fail gc prepare task", KR(ret), K(need_gc), K(gc_scn),
      KPC(ctx));
  return ret;
}

ObSSFailGCScheduleTask::ObSSFailGCScheduleTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_FAIL_GC),
    is_inited_(false),
    next_task_generated_(false),
    ls_idx_(-1),
    is_ls_head_(false),
    ls_ctx_(nullptr)
{
}

int ObSSFailGCScheduleTask::init(
    const int64_t ls_idx,
    const bool is_ls_head)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail gc schedule task init twice", KR(ret), K_(ls_idx));
  } else if (OB_UNLIKELY(ls_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc ls index", KR(ret), K(ls_idx), K(is_ls_head));
  } else {
    ls_idx_ = ls_idx;
    is_ls_head_ = is_ls_head;
    is_inited_ = true;
  }
  return ret;
}

int ObSSFailGCScheduleTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSFailGCDag *dag = nullptr;
  ObSSFailGCDagCtx *ctx = nullptr;
  ObSSFailGCScheduleTask *next_head = nullptr;
  next_task = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail gc schedule task is not initialized", KR(ret));
  } else if (next_task_generated_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSFailGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get fail gc dag ctx", KR(ret), K_(ls_idx));
  } else if (!ctx->need_gc_ || ctx->ls_ctxs_.empty() || !is_ls_head_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(bind_ls_ctx_(*ctx))) {
    LOG_WARN("fail to bind fail gc ls ctx", KR(ret), K_(ls_idx));
  } else if (ls_idx_ + 1 >= ctx->ls_ctxs_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag = static_cast<ObSSFailGCDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail gc dag is null", KR(ret), K_(ls_idx));
  } else if (OB_FAIL(dag->alloc_task(next_head))) {
    LOG_WARN("fail to allocate next fail gc ls head", KR(ret), K_(ls_idx));
  } else if (OB_ISNULL(next_head)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated next fail gc ls head is null", KR(ret), K_(ls_idx));
  } else if (OB_FAIL(next_head->init(
      ls_idx_ + 1, true /* is_ls_head */))) {
    LOG_WARN("fail to init next fail gc ls head", KR(ret), K_(ls_idx));
  } else {
    next_task_generated_ = true;
    next_task = next_head;
  }
  return ret;
}

int ObSSFailGCScheduleTask::bind_ls_ctx_(ObSSFailGCDagCtx &ctx)
{
  int ret = OB_SUCCESS;
  ls_ctx_ = nullptr;
  if (OB_UNLIKELY(ls_idx_ < 0 || ls_idx_ >= ctx.ls_ctxs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fail gc ls index", KR(ret), K_(ls_idx),
        "ls_count", ctx.ls_ctxs_.count());
  } else if (OB_ISNULL(ls_ctx_ = ctx.ls_ctxs_.at(ls_idx_))
      || OB_UNLIKELY(!ls_ctx_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fail gc ls ctx", KR(ret), K_(ls_idx), KP_(ls_ctx));
  }
  return ret;
}

void ObSSFailGCScheduleTask::fill_page_scan_param_(
    ObSSMetaCtrlPageScanParam &scan_param) const
{
  scan_param.reset();
  scan_param.scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  scan_param.page_size_ = CTRL_SCAN_PAGE_SIZE;
}

int ObSSFailGCScheduleTask::init_ls_iterator_(ObSSFailGCDagCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlPageScanParam scan_param;
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ls_ctx_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fail gc ls ctx", KR(ret), K_(ls_idx), KP_(ls_ctx));
  } else if (ls_ctx_->iterator_inited_) {
    // The continuation reuses the iterator owned by this LS context.
  } else if (OB_UNLIKELY(!is_ls_head_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail gc continuation has no initialized iterator", KR(ret),
        K_(ls_idx), KPC_(ls_ctx));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(ctx.tenant_id_), K_(ls_idx));
  } else {
    if (is_sys_tenant(ctx.tenant_id_)) {
      if (OB_FAIL(ObSSFailGCDagUtil::get_palf_kv_checkpoint_meta_scn(
          ls_ctx_->checkpoint_meta_scn_))) {
        LOG_WARN("fail to get palf kv checkpoint meta scn", KR(ret),
            K(ctx.tenant_id_), K_(ls_idx));
      }
    }
    if (OB_SUCC(ret)) {
      fill_page_scan_param_(scan_param);
      if (OB_FAIL(ls_ctx_->page_iter_.init(
          *GCTX.sql_proxy_, ctx.sql_timeout_us_, ctx.tenant_id_,
          ls_ctx_->ls_id_, scan_param))) {
        LOG_WARN("fail to init fail gc page iterator", KR(ret),
            K(ctx.tenant_id_), K_(ls_idx), KPC_(ls_ctx), K(scan_param));
      } else {
        ls_ctx_->iterator_inited_ = true;
      }
    }
  }
  return ret;
}

int ObSSFailGCScheduleTask::fetch_ctrl_rows_(
    ObSSFailGCDagCtx &ctx,
    ObIArray<ObSSFailGCCtrlRow> &ctrl_rows,
    bool &need_continuation)
{
  int ret = OB_SUCCESS;
  int64_t scanned_ctrl_row_count = 0;
  need_continuation = false;
  ctrl_rows.reset();
  if (OB_ISNULL(ls_ctx_) || OB_UNLIKELY(!ls_ctx_->iterator_inited_
      || ls_ctx_->scan_finished_ || ls_ctx_->scan_failed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fail gc scan state", KR(ret), K_(ls_idx), KPC_(ls_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < EXECUTOR_BATCH_SIZE; ++i) {
      ObSSFailGCCtrlRow ctrl_row;
      if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
        ctrl_rows.reset();
        LOG_INFO("stop fetching fail gc ctrl rows", KR(ret), K_(ls_idx));
      } else {
        const int iter_ret = ls_ctx_->page_iter_.get_next(
            ctrl_row.ctrl_key_, ctrl_row.ctrl_value_);
        if (OB_ITER_END == iter_ret) {
          ret = OB_SUCCESS;
          ls_ctx_->scan_finished_ = true;
          break;
        } else if (OB_SUCCESS != iter_ret) {
          ret = iter_ret;
          LOG_WARN("fail to get next fail gc ctrl row", KR(ret),
              K_(ls_idx), KPC_(ls_ctx));
        } else {
          ++scanned_ctrl_row_count;
          if (OB_FAIL(ctrl_rows.push_back(ctrl_row))) {
            LOG_WARN("fail to append fail gc ctrl row", KR(ret),
                K_(ls_idx), K(ctrl_row));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      need_continuation =
          !ls_ctx_->scan_finished_ && !ls_ctx_->scan_failed_;
    }
  }
  ctx.add_scanned_ctrl_row_count(scanned_ctrl_row_count);
  return ret;
}

int ObSSFailGCScheduleTask::generate_tasks_(
    ObSSFailGCDag &dag,
    const ObIArray<ObSSFailGCCtrlRow> &ctrl_rows,
    const bool need_continuation)
{
  int ret = OB_SUCCESS;
  ObSSFailGCExecutorTask *executor_task = nullptr;
  ObSSFailGCScheduleTask *continuation_task = nullptr;
  ObSEArray<ObITask *, 2> new_tasks;

  if (OB_UNLIKELY(ctrl_rows.count() > EXECUTOR_BATCH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc task generation input", KR(ret),
        "ctrl_row_count", ctrl_rows.count(), K(need_continuation));
  } else if (!ctrl_rows.empty()) {
    if (OB_FAIL(dag.alloc_task(executor_task))) {
      LOG_WARN("fail to allocate fail gc executor task", KR(ret), K_(ls_idx));
    } else if (OB_ISNULL(executor_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocated fail gc executor task is null", KR(ret), K_(ls_idx));
    } else if (OB_FAIL(executor_task->init(ls_ctx_, ctrl_rows))) {
      LOG_WARN("fail to init fail gc executor task", KR(ret), K_(ls_idx));
    }
  }

  if (OB_SUCC(ret)) {
    if (need_continuation) {
      // Executor and continuation are sibling tasks.
      if (OB_FAIL(dag.alloc_task(continuation_task))) {
        LOG_WARN("fail to allocate fail gc continuation task", KR(ret),
            K_(ls_idx));
      } else if (OB_ISNULL(continuation_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocated fail gc continuation task is null", KR(ret),
            K_(ls_idx));
      } else if (OB_FAIL(continuation_task->init(
          ls_idx_, false /* is_ls_head */))) {
        LOG_WARN("fail to init fail gc continuation task", KR(ret),
            K_(ls_idx));
      } else if (OB_NOT_NULL(executor_task)
          && OB_FAIL(add_child(*executor_task))) {
        LOG_WARN("fail to add fail gc executor dependency", KR(ret),
            K_(ls_idx));
      } else if (OB_FAIL(add_child(*continuation_task))) {
        LOG_WARN("fail to add fail gc continuation dependency", KR(ret),
            K_(ls_idx));
      } else if (OB_NOT_NULL(executor_task)
          && OB_FAIL(new_tasks.push_back(executor_task))) {
        LOG_WARN("fail to append fail gc executor task", KR(ret), K_(ls_idx));
      } else if (OB_FAIL(new_tasks.push_back(continuation_task))) {
        LOG_WARN("fail to append fail gc continuation task", KR(ret),
            K_(ls_idx));
      }
    } else {
      // A terminal batch only needs the executor. An empty terminal batch is
      // a successful no-op.
      if (OB_NOT_NULL(executor_task)) {
        if (OB_FAIL(add_child(*executor_task))) {
          LOG_WARN("fail to add fail gc executor dependency", KR(ret),
              K_(ls_idx));
        } else if (OB_FAIL(new_tasks.push_back(executor_task))) {
          LOG_WARN("fail to append fail gc executor task", KR(ret),
              K_(ls_idx));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !new_tasks.empty()) {
    if (OB_FAIL(dag.batch_add_task(new_tasks))) {
      LOG_WARN("fail to add fail gc dynamic tasks", KR(ret), K_(ls_idx),
          "task_count", new_tasks.count());
    }
  }
  return ret;
}

int ObSSFailGCScheduleTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_continuation = false;
  ObSSFailGCDag *dag = nullptr;
  ObSSFailGCDagCtx *ctx = nullptr;
  ObSEArray<ObSSFailGCCtrlRow, EXECUTOR_BATCH_SIZE> ctrl_rows;
  ctrl_rows.set_attr(ObMemAttr(MTL_ID(), "FailGCCtrlRows"));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail gc schedule task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSFailGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get fail gc dag ctx", KR(ret), K_(ls_idx));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop fail gc schedule task", KR(ret), K_(ls_idx), KPC(ctx));
  } else if (!ctx->need_gc_ || ctx->ls_ctxs_.empty()) {
    // The bootstrap task is already in the static DAG before LS discovery.
    // It becomes a no-op when GC is unnecessary or no LS exists.
  } else if (OB_FAIL(bind_ls_ctx_(*ctx))) {
    LOG_WARN("fail to bind fail gc ls ctx", KR(ret), K_(ls_idx));
  } else if (OB_TMP_FAIL(init_ls_iterator_(*ctx))) {
    LOG_WARN("fail to init fail gc ls iterator", KR(tmp_ret), K_(ls_idx));
  } else if (OB_TMP_FAIL(fetch_ctrl_rows_(
      *ctx, ctrl_rows, need_continuation))) {
    LOG_WARN("fail to fetch fail gc ctrl rows", KR(tmp_ret), K_(ls_idx));
  } else if (OB_ISNULL(dag = static_cast<ObSSFailGCDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail gc dag is null", KR(ret), K_(ls_idx));
  } else if (OB_FAIL(generate_tasks_(
      *dag, ctrl_rows, need_continuation))) {
    LOG_WARN("fail to generate fail gc tasks", KR(ret), K_(ls_idx),
        "ctrl_row_count", ctrl_rows.count(), K(need_continuation));
  }

  if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
    const int stop_ret = ObSSGCDagUtil::check_if_stopped(*this);
    if (OB_SUCCESS != stop_ret) {
      ret = stop_ret;
      LOG_INFO("stop fail gc schedule task after business error", KR(ret),
          KR(tmp_ret), K_(ls_idx), KPC_(ls_ctx));
    } else {
      ls_ctx_->scan_failed_ = true;
      (void)ctx->record_error_code(*ls_ctx_, tmp_ret);
      LOG_WARN("defer fail gc ls after business error", KR(tmp_ret),
          K_(ls_idx), KPC_(ls_ctx));
      ctrl_rows.reset();
    }
  }

  LOG_INFO("finish fail gc schedule task", KR(ret), K_(ls_idx),
      K_(is_ls_head), "ctrl_row_count", ctrl_rows.count(),
      K(need_continuation), KPC_(ls_ctx));
  return ret;
}

ObSSFailGCExecutorTask::ObSSFailGCExecutorTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_FAIL_GC),
    is_inited_(false),
    ls_ctx_(nullptr),
    ctrl_rows_(),
    fail_gc_tablet_map_()
{
}

ObSSFailGCExecutorTask::~ObSSFailGCExecutorTask()
{
  if (fail_gc_tablet_map_.created()) {
    fail_gc_tablet_map_.destroy();
  }
  ctrl_rows_.reset();
  ls_ctx_ = nullptr;
  is_inited_ = false;
}

int ObSSFailGCExecutorTask::init(
    ObSSFailGCLSCtx *ls_ctx,
    const ObIArray<ObSSFailGCCtrlRow> &ctrl_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail gc executor task init twice", KR(ret));
  } else if (OB_ISNULL(ls_ctx)
      || OB_UNLIKELY(!ls_ctx->is_valid())
      || OB_UNLIKELY(ctrl_rows.empty()
          || ctrl_rows.count() > ObSSFailGCScheduleTask::EXECUTOR_BATCH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc executor task input", KR(ret), KP(ls_ctx),
        "ctrl_row_count", ctrl_rows.count());
  } else {
    ctrl_rows_.set_attr(ObMemAttr(MTL_ID(), "FailGCExecRows"));
    for (int64_t i = 0; OB_SUCC(ret) && i < ctrl_rows.count(); ++i) {
      if (OB_UNLIKELY(!ctrl_rows.at(i).is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid fail gc executor ctrl row", KR(ret), K(i),
            K(ctrl_rows.at(i)));
      } else if (OB_FAIL(ctrl_rows_.push_back(ctrl_rows.at(i)))) {
        LOG_WARN("fail to copy fail gc executor ctrl row", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(fail_gc_tablet_map_.create(
        ctrl_rows_.count(),
        ObMemAttr(MTL_ID(), "FailGCExecMap")))) {
      LOG_WARN("fail to create fail gc executor tablet map", KR(ret));
    }
    if (OB_SUCC(ret)) {
      ls_ctx_ = ls_ctx;
      is_inited_ = true;
    } else {
      if (fail_gc_tablet_map_.created()) {
        fail_gc_tablet_map_.destroy();
      }
      ctrl_rows_.reset();
    }
  }
  return ret;
}

int ObSSFailGCExecutorTask::process_ctrl_row_(
    ObSSFailGCDagCtx &ctx,
    const ObSSFailGCCtrlRow &ctrl_row,
    ObIArray<ObSSFailGCInfo> &abandoned_key_infos)
{
  int ret = OB_SUCCESS;
  SCN meta_scn;
  bool can_do_failed_gc = false;
  if (OB_UNLIKELY(!is_inited_
      || !ctx.is_valid()
      || !ctx.gc_scn_.is_valid()
      || OB_ISNULL(ls_ctx_)
      || !ls_ctx_->is_valid()
      || !ctrl_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc executor row input", KR(ret), K_(is_inited),
        K(ctx), KP_(ls_ctx), K(ctrl_row));
  } else if (OB_FAIL(ctrl_row.ctrl_key_.get_control_scn(meta_scn))) {
    LOG_WARN("fail to get fail gc ctrl row meta scn", KR(ret), K(ctrl_row));
  } else if (OB_FAIL(check_can_do_ctrl_row_failed_gc_(
      ctrl_row.ctrl_key_, ctrl_row.ctrl_value_, meta_scn, ctx.gc_scn_,
      ls_ctx_->checkpoint_meta_scn_, can_do_failed_gc))) {
    LOG_WARN("fail to check whether ctrl row can do fail gc", KR(ret),
        K(ctrl_row), K(meta_scn), K_(ctx.gc_scn),
        K_(ls_ctx_->checkpoint_meta_scn));
  } else if (can_do_failed_gc
      && OB_FAIL(do_ctrl_row_failed_gc_(
          ctx, ctrl_row, abandoned_key_infos))) {
    LOG_WARN("fail to process fail gc ctrl row", KR(ret), K(ctrl_row),
        KPC_(ls_ctx));
  }
  return ret;
}

int ObSSFailGCExecutorTask::do_ctrl_row_failed_gc_(
    ObSSFailGCDagCtx &ctx,
    const ObSSFailGCCtrlRow &ctrl_row,
    ObIArray<ObSSFailGCInfo> &abandoned_key_infos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSFailGCInfo fail_gc_info;
  ObSSMetaCtrlKeyInfo &key_info = fail_gc_info.key_info_;
  bool &need_generate_task = fail_gc_info.need_generate_task_;
  bool need_collect = false;
  bool need_abandon = false;
  bool can_set_abandon = false;

  if (OB_UNLIKELY(!ctx.is_valid()
      || OB_ISNULL(ls_ctx_)
      || !ls_ctx_->is_valid()
      || !ctrl_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc row argument", KR(ret), K(ctx), KP_(ls_ctx),
        K(ctrl_row));
  } else {
    key_info.tenant_id_ = ctx.tenant_id_;
    key_info.ls_id_ = ls_ctx_->ls_id_;
    fail_gc_info.ctrl_value_ = ctrl_row.ctrl_value_;
    if (OB_FAIL(key_info.key_.assign(ctrl_row.ctrl_key_))) {
      LOG_WARN("fail to assign fail gc ctrl key", KR(ret), K(ctx),
          KPC_(ls_ctx), K(ctrl_row));
    } else if (ctrl_row.ctrl_value_.ctrl_info_.status_.is_abandon_status()) {
      need_abandon = true;
    } else if (OB_FAIL(check_can_set_abandon_(
        ctx, ctrl_row.ctrl_value_, can_set_abandon))) {
      LOG_WARN("fail to check whether ctrl row can be abandoned", KR(ret),
          K(ctx), KPC_(ls_ctx), K(ctrl_row));
    } else if (!can_set_abandon) {
      // The owner may still finish the atomic protocol.
    } else {
      ObAtomicProtocolCtrl protocol_ctrl;
      if (OB_FAIL(protocol_ctrl.init(key_info))) {
        LOG_WARN("fail to init atomic protocol ctrl", KR(ret), K(key_info));
      } else if (OB_TMP_FAIL(cancel_atomic_protocol_(protocol_ctrl))) {
        LOG_WARN("fail to cancel atomic protocol", KR(tmp_ret), K(key_info),
            K_(ctx.gc_scn));
      } else {
        need_abandon = true;
      }
    }
  }

  if (OB_SUCC(ret) && need_abandon
      && OB_FAIL(check_need_collect_abandoned_row_(
          ctx, key_info, ctrl_row.ctrl_value_, need_collect))) {
    LOG_WARN("fail to check whether abandoned row should be collected",
        KR(ret), K(key_info), K(ctrl_row.ctrl_value_));
  }
  if (OB_SUCC(ret) && need_collect) {
    if (OB_FAIL(check_need_generate_task_(
        key_info, ctrl_row.ctrl_value_, need_generate_task))) {
      LOG_WARN("fail to check whether fail gc task is needed", KR(ret),
          K(key_info), K(ctrl_row.ctrl_value_));
    } else if (OB_FAIL(abandoned_key_infos.push_back(fail_gc_info))) {
      LOG_WARN("fail to append abandoned fail gc row", KR(ret),
          K(fail_gc_info));
    } else {
      ctx.add_abandoned_ctrl_row_count(1);
    }
  }
#ifdef ERRSIM
  INJECT_SS_GC_ERRSIM(EN_SS_FAIL_GC_DO_CTRL_ROW_FAIL);
#endif
  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("SHARED_GC", "FAIL_GC_DO_CTRL_ROW_FAIL",
        "tenant_id", ctx.tenant_id_,
        "ls_id", OB_ISNULL(ls_ctx_) ? OB_INVALID_ID : ls_ctx_->ls_id_.id(),
        "meta_type", static_cast<int64_t>(ctrl_row.ctrl_key_.type_),
        "tablet_id", ctrl_row.ctrl_key_.tablet_key_.tablet_id_.id(),
        "trace_id", ObCurTraceId::get_trace_id_str(),
        "ret", ret);
  }
  return ret;
}

int ObSSFailGCExecutorTask::cancel_atomic_protocol_(
    ObAtomicProtocolCtrl &protocol_ctrl)
{
  return protocol_ctrl.cancel_atomic_protocol();
}

int ObSSFailGCExecutorTask::check_can_do_ctrl_row_failed_gc_(
    const ObSSMetaCtrlKey &ctrl_key,
    const ObSSMetaCtrlValue &ctrl_value,
    const SCN &meta_scn,
    const SCN &gc_scn,
    const SCN &checkpoint_meta_scn,
    bool &can_do_failed_gc) const
{
  int ret = OB_SUCCESS;
  can_do_failed_gc = false;
  if (OB_UNLIKELY(!ctrl_key.is_valid()
      || !ctrl_value.is_valid()
      || !meta_scn.is_valid()
      || !gc_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc row filter argument", KR(ret), K(ctrl_key),
        K(ctrl_value), K(meta_scn), K(gc_scn), K(checkpoint_meta_scn));
  } else {
    const bool can_do_abandoned_failed_gc =
        ctrl_value.ctrl_info_.status_.is_abandon_status()
        && !ctrl_key.is_checkpoint_type()
        && !ctrl_key.is_lock_row()
        && (ctrl_key.is_ls_type()
            || ctrl_key.is_tablet_type()
            || ctrl_key.is_sstable_type());
    const bool checkpoint_row_cannot_gc = ctrl_key.is_checkpoint_type()
        && (!checkpoint_meta_scn.is_valid()
            || meta_scn >= checkpoint_meta_scn);
    const bool is_not_reach_gc_scn =
        meta_scn >= gc_scn || checkpoint_row_cannot_gc;
    can_do_failed_gc =
        can_do_abandoned_failed_gc || !is_not_reach_gc_scn;
  }
  return ret;
}

int ObSSFailGCExecutorTask::check_need_collect_abandoned_row_(
    const ObSSFailGCDagCtx &ctx,
    const ObSSMetaCtrlKeyInfo &key_info,
    const ObSSMetaCtrlValue &ctrl_value,
    bool &need_collect)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  need_collect = false;
  if (OB_UNLIKELY(!ctx.is_valid()
      || !key_info.is_valid()
      || !ctrl_value.is_valid()
      || !fail_gc_tablet_map_.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid abandoned fail gc row", KR(ret), K(ctx), K(key_info),
        K(ctrl_value), "map_created", fail_gc_tablet_map_.created());
  } else if (!key_info.key_.is_major_sstable_type()) {
    need_collect = true;
  } else {
    const ObSSGCTabletBaseInfo tablet_info(
        key_info.key_.tablet_key_.tablet_id_,
        key_info.key_.tablet_key_.reorg_scn_);
    ObSSFailGCTabletMapValue fail_gc_tablet_value;
    hash_ret = fail_gc_tablet_map_.get_refactored(
        tablet_info, fail_gc_tablet_value);
    if (OB_SUCCESS != hash_ret && OB_HASH_NOT_EXIST != hash_ret) {
      ret = hash_ret;
      LOG_WARN("fail to get fail gc tablet value", KR(ret), K(key_info));
    } else if (OB_HASH_NOT_EXIST == hash_ret
        && OB_FAIL(add_fail_gc_tablet_to_map_(
            ctx, key_info, ctrl_value, fail_gc_tablet_value))) {
      LOG_WARN("fail to add fail gc tablet to executor map", KR(ret),
          K(key_info), K(ctrl_value));
    } else if (fail_gc_tablet_value.is_mark_deleted_) {
      need_collect = true;
    } else {
      need_collect =
          fail_gc_tablet_value.last_major_snapshot_
              >= ctrl_value.object_info_.snapshot_version_;
    }
  }
  return ret;
}

int ObSSFailGCExecutorTask::check_need_generate_task_(
    const ObSSMetaCtrlKeyInfo &key_info,
    const ObSSMetaCtrlValue &ctrl_value,
    bool &need_generate_task)
{
  int ret = OB_SUCCESS;
  need_generate_task = false;
  ObSSFailGCTabletMapValue fail_gc_tablet_value;
  if (OB_UNLIKELY(!key_info.is_valid()
      || !ctrl_value.is_valid()
      || !fail_gc_tablet_map_.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc task generation input", KR(ret), K(key_info),
        K(ctrl_value), "map_created", fail_gc_tablet_map_.created());
  } else if (!key_info.key_.is_major_sstable_type()) {
    need_generate_task = !key_info.key_.is_ls_type();
  } else {
    const ObSSGCTabletBaseInfo tablet_info(
        key_info.key_.tablet_key_.tablet_id_,
        key_info.key_.tablet_key_.reorg_scn_);
    if (OB_FAIL(fail_gc_tablet_map_.get_refactored(
        tablet_info, fail_gc_tablet_value))) {
      LOG_WARN("fail to get fail gc tablet value", KR(ret), K(key_info));
    } else if (!fail_gc_tablet_value.is_mark_deleted_
        || fail_gc_tablet_value.last_major_snapshot_
            >= ctrl_value.object_info_.snapshot_version_) {
      need_generate_task = false;
    } else {
      need_generate_task = true;
    }
  }
  return ret;
}

int ObSSFailGCExecutorTask::add_fail_gc_tablet_to_map_(
    const ObSSFailGCDagCtx &ctx,
    const ObSSMetaCtrlKeyInfo &key_info,
    const ObSSMetaCtrlValue &ctrl_value,
    ObSSFailGCTabletMapValue &fail_gc_tablet_value)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = nullptr;
  ObArenaAllocator allocator(
      "SSFailGCTablet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle ss_tablet_handle;
  ObTablet *ss_tablet = nullptr;
  bool need_check_tablet_lock_row = false;
  const ObSSGCTabletBaseInfo tablet_info(
      key_info.key_.tablet_key_.tablet_id_,
      key_info.key_.tablet_key_.reorg_scn_);
  fail_gc_tablet_value.reset();

  if (OB_UNLIKELY(!ctx.is_valid()
      || !key_info.is_valid()
      || !ctrl_value.is_valid()
      || !fail_gc_tablet_map_.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc tablet input", KR(ret), K(ctx), K(key_info),
        K(ctrl_value), "map_created", fail_gc_tablet_map_.created());
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K(key_info));
  } else if (OB_FAIL(meta_service->get_tablet(
      key_info.ls_id_, key_info.key_.tablet_key_.tablet_id_,
      key_info.key_.tablet_key_.reorg_scn_, allocator, ss_tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      need_check_tablet_lock_row = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet from shared storage log", KR(ret),
          K(key_info));
    }
  } else if (OB_ISNULL(ss_tablet = ss_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared storage tablet is null", KR(ret), K(key_info),
        K(ss_tablet_handle));
  } else {
    fail_gc_tablet_value.is_mark_deleted_ = false;
    if (OB_FAIL(fail_gc_tablet_value.last_major_snapshot_.convert_for_tx(
        ss_tablet->get_last_major_snapshot_version()))) {
      LOG_WARN("fail to convert tablet last major snapshot", KR(ret),
          K(key_info), K(ctrl_value));
    }
  }

  if (OB_SUCC(ret) && need_check_tablet_lock_row) {
    ObSSMetaCtrlLockRowValue tablet_lock_value;
    if (OB_FAIL(get_tablet_lock_row_value_(
        ctx, key_info, ctrl_value, tablet_lock_value))) {
      LOG_WARN("fail to get tablet lock row value", KR(ret), K(key_info),
          K(ctrl_value));
    } else if (!tablet_lock_value.is_mark_deleted_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet lock row is not mark deleted", KR(ret),
          K(tablet_lock_value));
    } else {
      fail_gc_tablet_value.is_mark_deleted_ = true;
      if (OB_FAIL(fail_gc_tablet_value.last_major_snapshot_.convert_for_tx(
          tablet_lock_value.last_major_snapshot_))) {
        LOG_WARN("fail to convert lock row last major snapshot", KR(ret),
            K(key_info), K(ctrl_value));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(fail_gc_tablet_map_.set_refactored(
      tablet_info, fail_gc_tablet_value))) {
    LOG_WARN("fail to cache fail gc tablet value", KR(ret), K(tablet_info),
        K(fail_gc_tablet_value));
  }
  return ret;
}

int ObSSFailGCExecutorTask::get_tablet_lock_row_value_(
    const ObSSFailGCDagCtx &ctx,
    const ObSSMetaCtrlKeyInfo &key_info,
    const ObSSMetaCtrlValue &ctrl_value,
    ObSSMetaCtrlLockRowValue &lock_value)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(
      "SSFailGCLock", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObSSMetaCtrlLockValueIterator lock_value_iter;
  const ObSSMetaCtrlKey lock_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK,
      key_info.key_.sstable_key_.tablet_id_,
      key_info.key_.sstable_key_.reorg_scn_,
      SCN::min_scn(),
      key_info.ls_id_);
  ObString lock_key_str;
  lock_value.reset();

  if (OB_UNLIKELY(!ctx.is_valid()
      || ctx.sql_timeout_us_ <= 0
      || !key_info.is_valid()
      || !key_info.key_.is_major_sstable_type()
      || !ctrl_value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet lock row input", KR(ret), K(ctx), K(key_info),
        K(ctrl_value));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(ctx), K(key_info));
  } else if (OB_FAIL(ObSSGCDagUtil::serialize_meta_key(
      lock_key, allocator, lock_key_str))) {
    LOG_WARN("fail to serialize tablet lock key", KR(ret), K(lock_key));
  } else if (OB_FAIL(lock_value_iter.init(
      *GCTX.sql_proxy_, ctx.sql_timeout_us_, key_info.tenant_id_,
      key_info.ls_id_, lock_key_str))) {
    LOG_WARN("fail to init tablet lock value iterator", KR(ret), K(ctx),
        K(key_info));
  } else {
    ObSSMetaCtrlLockRowValue lock_row_value;
    if (OB_FAIL(lock_value_iter.get_next(lock_row_value))) {
      LOG_WARN("fail to get tablet lock row value", KR(ret), K(lock_key));
    } else {
      lock_value = lock_row_value;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(lock_value_iter.get_next(lock_row_value))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next tablet lock row value", KR(ret),
              K(lock_key));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("major tablet lock row is not unique", KR(ret), K(lock_key),
            K(lock_row_value));
      }
    }
  }
  return ret;
}

int ObSSFailGCExecutorTask::check_can_set_abandon_(
    const ObSSFailGCDagCtx &ctx,
    const ObSSMetaCtrlValue &ctrl_value,
    bool &can_abandon) const
{
  int ret = OB_SUCCESS;
  can_abandon = false;
  const ObCurTraceId::TraceId &trace_id = ctrl_value.ctrl_info_.trace_id_;
  ObAddr owner_addr;
  if (OB_UNLIKELY(!ctx.is_valid() || !ctrl_value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc owner check input", KR(ret), K(ctx),
        K(ctrl_value));
  } else if (trace_id.is_invalid()) {
    can_abandon = true;
    FLOG_INFO("ctrl row has no owner trace id, treat as abandonable",
        K(ctrl_value));
  } else if (OB_FAIL(trace_id.get_server_addr(owner_addr))) {
    LOG_WARN("fail to get owner address from trace id", KR(ret), K(trace_id));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool is_permanent_offline = false;
    tmp_ret = SVR_TRACER.check_server_permanent_offline(
        owner_addr, is_permanent_offline);
    if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      can_abandon = true;
      FLOG_INFO("owner server is absent, treat ctrl row as abandonable",
          K(owner_addr), K(trace_id));
    } else if (OB_TMP_FAIL(tmp_ret)) {
      LOG_WARN("fail to check whether owner server is offline", KR(tmp_ret),
          K(owner_addr), K(trace_id));
    } else if (is_permanent_offline) {
      can_abandon = true;
      FLOG_INFO("owner server is permanently offline", K(owner_addr),
          K(trace_id));
    } else if (OB_UNLIKELY(ctx.sql_timeout_us_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid fail gc rpc timeout", KR(ret), K(ctx), K(trace_id));
    } else {
      obrpc::ObSSGCProxy proxy;
      ObCheckAtomicProtocolArg arg;
      arg.tenant_id_ = ctx.tenant_id_;
      arg.trace_id_ = trace_id;
      obrpc::Bool rpc_res(false);
      if (OB_TMP_FAIL(proxy.to(owner_addr).by(ctx.tenant_id_)
          .timeout(ctx.sql_timeout_us_)
          .check_atomic_protocol_exist(arg, rpc_res))) {
        LOG_WARN("fail to check atomic protocol existence", KR(tmp_ret),
            K(owner_addr), K(trace_id));
      } else {
        can_abandon = !rpc_res;
      }
    }
  }
  return ret;
}

int ObSSFailGCExecutorTask::generate_fail_gc_task_(
    ObSSFailGCDagCtx &ctx,
    const ObIArray<ObSSFailGCInfo> &abandoned_key_infos)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t generated_task_count = 0;
  if (OB_UNLIKELY(!is_inited_
      || !ctx.is_valid()
      || ctx.sql_timeout_us_ <= 0
      || OB_ISNULL(ls_ctx_)
      || !ls_ctx_->is_valid()
      || !ctx.owns_ls_ctx_(*ls_ctx_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc transaction input", KR(ret), K_(is_inited),
        K(ctx), KP_(ls_ctx), "batch_size", abandoned_key_infos.count());
  } else if (abandoned_key_infos.empty()) {
    // No abandoned ctrl row in this Executor batch.
  } else {
    ObMySQLTransaction trans;
    ObSSMetaCtrlTableOperator table_operator;
    ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "SSFailGC"));
    const bool with_snapshot = false;
    const int32_t group_id = OBCG_STORAGE;
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(ObAtomicProtocolUtil::start_trans(
        ctx.tenant_id_, ctx.sql_timeout_us_, with_snapshot,
        group_id, timeout_ctx, trans))) {
      LOG_WARN("fail to start fail gc transaction", KR(ret), K(ctx));
    } else {
      if (OB_FAIL(table_operator.init(trans, ctx.sql_timeout_us_))) {
        LOG_WARN("fail to init fail gc table operator", KR(ret), K(ctx));
      } else {
        for (int64_t i = 0;
             OB_SUCC(ret) && i < abandoned_key_infos.count(); ++i) {
          const ObSSFailGCInfo &fail_gc_info = abandoned_key_infos.at(i);
          if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
            LOG_INFO("stop generating fail gc tasks", KR(ret), K(i),
                KPC_(ls_ctx));
          } else if (OB_UNLIKELY(!fail_gc_info.is_valid()
              || fail_gc_info.key_info_.tenant_id_ != ctx.tenant_id_
              || fail_gc_info.key_info_.ls_id_ != ls_ctx_->ls_id_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid fail gc info for executor transaction", KR(ret),
                K(i), K(fail_gc_info), K(ctx), KPC_(ls_ctx));
          } else if (OB_FAIL(delete_abandoned_key_info_(
              fail_gc_info, allocator, table_operator))) {
            LOG_WARN("fail to delete abandoned ctrl row", KR(ret), K(i),
                K(fail_gc_info));
          } else if (fail_gc_info.need_generate_task_
              && OB_FAIL(insert_fail_gc_task_(fail_gc_info, trans))) {
            LOG_WARN("fail to insert fail gc task", KR(ret), K(i),
                K(fail_gc_info));
          } else if (fail_gc_info.need_generate_task_) {
            ++generated_task_count;
          }
        }
      }
      if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(ret, trans))) {
        LOG_WARN("fail to end fail gc transaction", KR(tmp_ret), KR(ret),
            K(ctx), "batch_size", abandoned_key_infos.count());
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_SUCC(ret)) {
        ctx.add_generated_task_count(generated_task_count);
      }
    }

#ifdef ERRSIM
    INJECT_SS_GC_ERRSIM(EN_SS_FAIL_GC_GENERATE_TASK_FAIL);
#endif
    if (OB_FAIL(ret)) {
      SERVER_EVENT_ADD("SHARED_GC", "FAIL_GC_GENERATE_TASK_TRANS_FAIL",
          "tenant_id", ctx.tenant_id_,
          "batch_size", abandoned_key_infos.count(),
          "ret", ret,
          "trace_id", ObCurTraceId::get_trace_id_str(),
          "ls_id", ls_ctx_->ls_id_.id());
    }
  }
  return ret;
}

int ObSSFailGCExecutorTask::delete_abandoned_key_info_(
    const ObSSFailGCInfo &fail_gc_info,
    ObIAllocator &allocator,
    ObSSMetaCtrlTableOperator &table_operator)
{
  int ret = OB_SUCCESS;
  ObString row_meta_key;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!fail_gc_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc info", KR(ret), K(fail_gc_info));
  } else if (OB_FAIL(ObSSGCDagUtil::serialize_meta_key(
      fail_gc_info.key_info_.key_, allocator, row_meta_key))) {
    LOG_WARN("fail to serialize abandoned ctrl row key", KR(ret),
        K(fail_gc_info));
  } else if (OB_FAIL(table_operator.delete_row(
      fail_gc_info.key_info_.tenant_id_, fail_gc_info.key_info_.ls_id_,
      fail_gc_info.key_info_.key_.type_, row_meta_key,
      false /* prefix_delete */, affected_rows))) {
    LOG_WARN("fail to delete abandoned ctrl row", KR(ret), K(fail_gc_info));
  } else if (OB_UNLIKELY(0 != affected_rows && 1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows for abandoned ctrl row delete",
        KR(ret), K(affected_rows), K(fail_gc_info));
  }
  return ret;
}

int ObSSFailGCExecutorTask::insert_fail_gc_task_(
    const ObSSFailGCInfo &fail_gc_info,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const ObSSMetaCtrlKeyInfo &key_info = fail_gc_info.key_info_;
  if (OB_UNLIKELY(!fail_gc_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc info", KR(ret), K(fail_gc_info));
  } else if (OB_FAIL(ObSharedGCTaskUtil::generate_fail_gc_task(
      key_info.tenant_id_, key_info.ls_id_, key_info.key_,
      fail_gc_info.ctrl_value_, trans))) {
    LOG_WARN("fail to generate fail gc task", KR(ret), K(fail_gc_info));
  }
  return ret;
}

int ObSSFailGCExecutorTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSFailGCDagCtx *ctx = nullptr;
  ObSEArray<ObSSFailGCInfo,
      ObSSFailGCScheduleTask::EXECUTOR_BATCH_SIZE> abandoned_key_infos;
  abandoned_key_infos.set_attr(ObMemAttr(MTL_ID(), "FailGCInfos"));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail gc executor task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSFailGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get fail gc dag ctx", KR(ret), KPC_(ls_ctx));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop fail gc executor task", KR(ret), KPC_(ls_ctx));
  } else if (OB_ISNULL(ls_ctx_)
      || OB_UNLIKELY(!ls_ctx_->is_valid()
          || !ctx->owns_ls_ctx_(*ls_ctx_)
          || !ctx->need_gc_
          || !ctx->gc_scn_.is_valid()
          || ctx->sql_timeout_us_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fail gc executor context", KR(ret), KPC(ctx),
        KPC_(ls_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_SUCCESS == tmp_ret
        && i < ctrl_rows_.count(); ++i) {
      if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
        LOG_INFO("stop processing fail gc ctrl rows", KR(ret), K(i),
            KPC_(ls_ctx));
      } else if (OB_TMP_FAIL(process_ctrl_row_(
          *ctx, ctrl_rows_.at(i), abandoned_key_infos))) {
        LOG_WARN("fail to process fail gc ctrl row", KR(tmp_ret), K(i),
            K(ctrl_rows_.at(i)), KPC_(ls_ctx));
      }
    }
    if (OB_SUCC(ret) && OB_SUCCESS == tmp_ret
        && OB_TMP_FAIL(generate_fail_gc_task_(*ctx, abandoned_key_infos))) {
      LOG_WARN("fail to commit fail gc executor batch", KR(tmp_ret),
          KPC_(ls_ctx), "batch_size", abandoned_key_infos.count());
    }
    if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
      const int stop_ret = ObSSGCDagUtil::check_if_stopped(*this);
      if (OB_SUCCESS != stop_ret) {
        ret = stop_ret;
        LOG_INFO("stop fail gc executor task after business error", KR(ret),
            KR(tmp_ret), KPC_(ls_ctx));
      } else {
        (void)ctx->record_error_code(*ls_ctx_, tmp_ret);
        LOG_WARN("defer fail gc executor batch after business error",
            KR(tmp_ret), KPC_(ls_ctx), "ctrl_row_count", ctrl_rows_.count(),
            "abandoned_row_count", abandoned_key_infos.count());
      }
    }
  }
  LOG_INFO("finish fail gc executor task", KR(ret), KPC_(ls_ctx),
      "ctrl_row_count", ctrl_rows_.count(),
      "abandoned_row_count", abandoned_key_infos.count());
  return ret;
}

} // namespace storage
} // namespace oceanbase
