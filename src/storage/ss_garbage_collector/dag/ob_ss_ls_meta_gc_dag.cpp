/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/dag/ob_ss_ls_meta_gc_dag.h"
#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_sort.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace
{

struct ObSSLSMetaGCTaskCompare final
{
public:
  explicit ObSSLSMetaGCTaskCompare(int &sort_ret)
    : sort_ret_(sort_ret)
  {
  }

  bool operator()(const ObSharedGCTask &left, const ObSharedGCTask &right)
  {
    int ret = OB_SUCCESS;
    bool less = false;
    ObSharedGCLSMetaInfo left_info;
    ObSharedGCLSMetaInfo right_info;
    if (OB_SUCCESS != sort_ret_) {
      // Keep the first comparator error.
    } else if (OB_FAIL(left_info.from_value(left.value_))) {
      sort_ret_ = ret;
      LOG_WARN("fail to deserialize left ls meta gc task for sorting",
          KR(sort_ret_), K(left.key_));
    } else if (OB_FAIL(right_info.from_value(right.value_))) {
      sort_ret_ = ret;
      LOG_WARN("fail to deserialize right ls meta gc task for sorting",
          KR(sort_ret_), K(right.key_));
    } else if (OB_UNLIKELY(!left_info.is_valid()
        || !right_info.is_valid())) {
      sort_ret_ = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls meta gc task for sorting", KR(sort_ret_),
          K(left_info), K(right_info), K(left.key_), K(right.key_));
    } else if (left_info.meta_scn_ != right_info.meta_scn_) {
      less = left_info.meta_scn_ < right_info.meta_scn_;
    } else {
      less = left.key_.task_id_ < right.key_.task_id_;
    }
    return less;
  }

private:
  int &sort_ret_;
};

} // namespace

ObSSLSMetaGCTabletReplayedScnInfo::ObSSLSMetaGCTabletReplayedScnInfo()
  : max_replayed_scn_(),
    is_cold_(false)
{
}

void ObSSLSMetaGCTabletReplayedScnInfo::reset()
{
  max_replayed_scn_.reset();
  is_cold_ = false;
}

bool ObSSLSMetaGCTabletReplayedScnInfo::is_valid() const
{
  return max_replayed_scn_.is_valid();
}

ObSSLSMetaGCDagCtx::ObSSLSMetaGCDagCtx()
  : tenant_id_(OB_INVALID_TENANT_ID),
    cur_gts_(),
    allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "LSMetaGCCtx")),
    ls_meta_gc_tasks_map_(),
    map_iter_(),
    map_end_(),
    map_inited_(false),
    map_iter_inited_(false),
    ls_count_(0),
    first_error_code_(OB_SUCCESS),
    failed_ls_count_(0),
    gc_task_count_(0),
    deleted_ls_meta_version_count_(0),
    finished_gc_task_count_(0)
{
}

ObSSLSMetaGCDagCtx::~ObSSLSMetaGCDagCtx()
{
  reset();
}

int ObSSLSMetaGCDagCtx::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls meta gc dag ctx init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta gc dag tenant", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    allocator_.set_tenant_id(tenant_id);
  }
  return ret;
}

void ObSSLSMetaGCDagCtx::free_task_map_()
{
  map_iter_inited_ = false;
  map_iter_ = ObSSLSMetaGCTaskMap::const_iterator();
  map_end_ = ObSSLSMetaGCTaskMap::const_iterator();
  if (map_inited_) {
    for (ObSSLSMetaGCTaskMap::iterator iter = ls_meta_gc_tasks_map_.begin();
         iter != ls_meta_gc_tasks_map_.end(); ++iter) {
      ObArray<ObSharedGCTask> *gc_tasks = iter->second;
      if (OB_NOT_NULL(gc_tasks)) {
        gc_tasks->~ObArray<ObSharedGCTask>();
        allocator_.free(gc_tasks);
        iter->second = nullptr;
      }
    }
    ls_meta_gc_tasks_map_.destroy();
    map_inited_ = false;
  }
  allocator_.reset();
}

void ObSSLSMetaGCDagCtx::reset()
{
  free_task_map_();
  tenant_id_ = OB_INVALID_TENANT_ID;
  cur_gts_.reset();
  ATOMIC_STORE(&ls_count_, 0);
  ATOMIC_STORE(&first_error_code_, OB_SUCCESS);
  ATOMIC_STORE(&failed_ls_count_, 0);
  ATOMIC_STORE(&gc_task_count_, 0);
  ATOMIC_STORE(&deleted_ls_meta_version_count_, 0);
  ATOMIC_STORE(&finished_gc_task_count_, 0);
}

bool ObSSLSMetaGCDagCtx::is_valid() const
{
  // GTS and task-map state are published by PrepareTask, after the DAG and its
  // first tasks have already been initialized.
  return is_valid_tenant_id(tenant_id_);
}

void ObSSLSMetaGCDagCtx::add_ls_count(const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&ls_count_, count);
  }
}

void ObSSLSMetaGCDagCtx::add_gc_task_count(const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&gc_task_count_, count);
  }
}

void ObSSLSMetaGCDagCtx::add_deleted_ls_meta_version_count(
    const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&deleted_ls_meta_version_count_, count);
  }
}

void ObSSLSMetaGCDagCtx::add_finished_gc_task_count(const int64_t count)
{
  if (count > 0) {
    (void)ATOMIC_AAF(&finished_gc_task_count_, count);
  }
}

void ObSSLSMetaGCDagCtx::record_error_code(const int error_code)
{
  if (OB_SUCCESS != error_code) {
    (void)ATOMIC_BCAS(&first_error_code_, OB_SUCCESS, error_code);
    (void)ATOMIC_AAF(&failed_ls_count_, 1);
  }
}

int64_t ObSSLSMetaGCDagCtx::get_ls_count() const
{
  return ATOMIC_LOAD(&ls_count_);
}

int ObSSLSMetaGCDagCtx::get_first_error_code() const
{
  return ATOMIC_LOAD(&first_error_code_);
}

int64_t ObSSLSMetaGCDagCtx::get_failed_ls_count() const
{
  return ATOMIC_LOAD(&failed_ls_count_);
}

int64_t ObSSLSMetaGCDagCtx::get_gc_task_count() const
{
  return ATOMIC_LOAD(&gc_task_count_);
}

int64_t ObSSLSMetaGCDagCtx::get_deleted_ls_meta_version_count() const
{
  return ATOMIC_LOAD(&deleted_ls_meta_version_count_);
}

int64_t ObSSLSMetaGCDagCtx::get_finished_gc_task_count() const
{
  return ATOMIC_LOAD(&finished_gc_task_count_);
}

ObSSLSMetaGCDagParam::ObSSLSMetaGCDagParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    schedule_ts_(0)
{
}

int ObSSLSMetaGCDagParam::init(
    const uint64_t tenant_id,
    const int64_t schedule_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls meta gc dag param init twice", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || schedule_ts < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta gc dag param", KR(ret), K(tenant_id),
        K(schedule_ts));
  } else {
    tenant_id_ = tenant_id;
    schedule_ts_ = schedule_ts;
  }
  return ret;
}

bool ObSSLSMetaGCDagParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && schedule_ts_ >= 0;
}

ObSSLSMetaGCDag::ObSSLSMetaGCDag()
  : ObSSGCDagBase(DAG_TYPE),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    schedule_ts_(0),
    start_event_reported_(0),
    ctx_(nullptr)
{
}

ObSSLSMetaGCDag::~ObSSLSMetaGCDag()
{
  free_ctx_();
  tenant_id_ = OB_INVALID_TENANT_ID;
  schedule_ts_ = 0;
  ATOMIC_STORE(&start_event_reported_, 0);
  is_inited_ = false;
}

int ObSSLSMetaGCDag::alloc_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(OB_NOT_NULL(ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc dag ctx already allocated", KR(ret), KP_(ctx));
  } else if (OB_ISNULL(buf = mtl_malloc(
      sizeof(ObSSLSMetaGCDagCtx), ObMemAttr(MTL_ID(), "LSMetaGCDag")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate ls meta gc dag ctx", KR(ret));
  } else {
    ctx_ = new (buf) ObSSLSMetaGCDagCtx();
  }
  return ret;
}

void ObSSLSMetaGCDag::free_ctx_()
{
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObSSLSMetaGCDagCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

bool ObSSLSMetaGCDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() == other.get_type()) {
    const ObSSLSMetaGCDag &other_dag =
        static_cast<const ObSSLSMetaGCDag &>(other);
    bret = tenant_id_ == other_dag.tenant_id_;
  }
  return bret;
}

uint64_t ObSSLSMetaGCDag::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  return hash_val;
}

int ObSSLSMetaGCDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  char stat_buf[128] = {0};
  if (OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls meta gc dag ctx is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(databuff_printf(stat_buf, sizeof(stat_buf),
      "gc_tasks=%ld,ls_meta_versions_deleted=%ld,gc_tasks_finished=%ld",
      ctx_->get_gc_task_count(),
      ctx_->get_deleted_ls_meta_version_count(),
      ctx_->get_finished_gc_task_count()))) {
    LOG_WARN("fail to format ls meta gc dag statistics", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(
      out_param, allocator, get_type(), static_cast<int64_t>(tenant_id_),
      static_cast<int64_t>(GCTX.get_server_id()), ctx_->get_ls_count(),
      static_cast<int64_t>(ctx_->get_first_error_code()),
      ctx_->get_failed_ls_count(), "server_addr", GCONF.self_addr_,
      "row_stat", stat_buf))) {
    LOG_WARN("fail to fill ls meta gc dag info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSLSMetaGCDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "tenant_id=%lu", tenant_id_))) {
    LOG_WARN("fail to fill ls meta gc dag key", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSLSMetaGCDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSLSMetaGCDagParam *dag_param =
      static_cast<const ObSSLSMetaGCDagParam *>(param);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls meta gc dag init twice", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(dag_param) || OB_UNLIKELY(!dag_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta gc dag param", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(alloc_ctx_())) {
    LOG_WARN("fail to allocate ls meta gc dag ctx", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(ctx_->init(dag_param->tenant_id_))) {
    LOG_WARN("fail to init ls meta gc dag ctx", KR(ret), KPC(dag_param));
  } else {
    tenant_id_ = dag_param->tenant_id_;
    schedule_ts_ = dag_param->schedule_ts_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLSMetaGCDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSLSMetaGCPrepareTask *prepare_task = nullptr;
  ObSSLSMetaGCScheduleTask *schedule_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)
      || OB_ISNULL(ctx_)
      || OB_UNLIKELY(!ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls meta gc dag is not initialized", KR(ret), K_(tenant_id),
        K_(is_inited), KPC_(ctx));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("fail to allocate ls meta gc prepare task", KR(ret),
        K_(tenant_id));
  } else if (OB_ISNULL(prepare_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated ls meta gc prepare task is null", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(alloc_task(schedule_task))) {
    LOG_WARN("fail to allocate ls meta gc schedule task", KR(ret),
        K_(tenant_id));
  } else if (OB_ISNULL(schedule_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated ls meta gc schedule task is null", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(prepare_task->init())) {
    LOG_WARN("fail to init ls meta gc prepare task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(schedule_task->init())) {
    LOG_WARN("fail to init ls meta gc schedule task", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(prepare_task->add_child(*schedule_task))) {
    LOG_WARN("fail to add ls meta gc prepare to schedule edge", KR(ret),
        K_(tenant_id));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    LOG_WARN("fail to add ls meta gc prepare task", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(add_task(*schedule_task))) {
    LOG_WARN("fail to add ls meta gc schedule task", KR(ret), K_(tenant_id));
  }
  LOG_INFO("finish to create ls meta gc dag first tasks", KR(ret),
      K_(tenant_id), KPC_(ctx));
  return ret;
}

void ObSSLSMetaGCDag::report_start_event()
{
  if (ATOMIC_BCAS(&start_event_reported_, 0, 1)) {
    SERVER_EVENT_ADD("SHARED_GC", "START_LS_META_GC_DAG",
        "tenant_id", tenant_id_,
        "schedule_ts", schedule_ts_,
        "dag_id", get_dag_id(),
        "dag_start_time_us", get_start_time(),
        "trace_id", ObCurTraceId::get_trace_id_str());
  }
}

int ObSSLSMetaGCDag::report_result()
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
        "ls meta gc dag ctx is null when reporting", K_(tenant_id),
        K(dag_ret), K(dag_status));
  } else {
    (void)databuff_printf(stat_buf, sizeof(stat_buf),
        "failed_ls_count=%ld,ls_count=%ld,gc_task_count=%ld,"
        "deleted_ls_meta_version_count=%ld,finished_gc_task_count=%ld,"
        "cost_us=%ld",
        ctx_->get_failed_ls_count(), ctx_->get_ls_count(),
        ctx_->get_gc_task_count(), ctx_->get_deleted_ls_meta_version_count(),
        ctx_->get_finished_gc_task_count(), cost_us);
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_LS_META_GC_DAG",
        "tenant_id", tenant_id_,
        "schedule_ts", schedule_ts_,
        "dag_ret", dag_ret,
        "dag_status", static_cast<int64_t>(dag_status),
        "dag_id", get_dag_id(),
        "first_error_code", ctx_->get_first_error_code(),
        stat_buf /* extra_info */);
  }
  LOG_INFO("finish ls meta gc dag", K_(tenant_id), K(dag_ret),
      K(dag_status), K(cost_us), KPC_(ctx));
  return OB_SUCCESS;
}

ObSSLSMetaGCPrepareTask::ObSSLSMetaGCPrepareTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_LS_META_GC),
    is_inited_(false)
{
}

int ObSSLSMetaGCPrepareTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls meta gc prepare task init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSLSMetaGCPrepareTask::append_ls_meta_gc_task_(
    ObSSLSMetaGCDagCtx &ctx,
    const ObSharedGCTask &gc_task,
    const ObSharedGCLSMetaInfo &ls_meta_info,
    bool &new_ls)
{
  int ret = OB_SUCCESS;
  ObArray<ObSharedGCTask> *gc_tasks = nullptr;
  new_ls = false;
  if (OB_UNLIKELY(!ctx.is_valid()
      || !gc_task.is_valid()
      || !ls_meta_info.is_valid()
      || gc_task.key_.tenant_id_ != ctx.tenant_id_
      || !is_ls_meta_gc_type(gc_task.key_.type_)
      || 0 != gc_task.key_.round_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls meta gc task row", KR(ret), K(ctx), K(gc_task),
        K(ls_meta_info));
  } else if (OB_FAIL(ctx.ls_meta_gc_tasks_map_.get_refactored(
      ls_meta_info.ls_id_, gc_tasks))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(gc_tasks = OB_NEWx(
          ObArray<ObSharedGCTask>, &ctx.allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate ls meta gc task array", KR(ret),
            K(ctx.tenant_id_), K(ls_meta_info.ls_id_));
      } else {
        gc_tasks->set_attr(ObMemAttr(ctx.tenant_id_, "LSMetaGCTasks"));
        if (OB_FAIL(ctx.ls_meta_gc_tasks_map_.set_refactored(
            ls_meta_info.ls_id_, gc_tasks))) {
          LOG_WARN("fail to add ls meta gc task array to map", KR(ret),
              K(ctx.tenant_id_), K(ls_meta_info.ls_id_));
        } else {
          new_ls = true;
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(gc_tasks)) {
        gc_tasks->~ObArray<ObSharedGCTask>();
        ctx.allocator_.free(gc_tasks);
        gc_tasks = nullptr;
      }
    } else {
      LOG_WARN("fail to get ls meta gc task array", KR(ret),
          K(ctx.tenant_id_), K(ls_meta_info.ls_id_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(gc_tasks)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc task array is null", KR(ret),
        K(ctx.tenant_id_), K(ls_meta_info.ls_id_));
  } else if (OB_FAIL(gc_tasks->push_back(gc_task))) {
    LOG_WARN("fail to append ls meta gc task", KR(ret),
        K(ctx.tenant_id_), K(ls_meta_info.ls_id_), K(gc_task.key_));
  }
  return ret;
}

int ObSSLSMetaGCPrepareTask::collect_ls_meta_gc_tasks_(
    ObSSLSMetaGCDagCtx &ctx,
    int64_t &ls_count,
    int64_t &gc_task_count)
{
  int ret = OB_SUCCESS;
  ObSharedGCTaskLSMetaIterator task_iter;
  ObSharedGCTask gc_task;
  ObSharedGCLSMetaInfo ls_meta_info;
  bool new_ls = false;
  ls_count = 0;
  gc_task_count = 0;
  if (OB_UNLIKELY(!ctx.is_valid() || !ctx.map_inited_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta gc dag context", KR(ret), K(ctx));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(ctx.tenant_id_));
  } else if (OB_FAIL(task_iter.init(ctx.tenant_id_, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to init ls meta gc task iterator", KR(ret),
        K(ctx.tenant_id_));
  } else {
    while (OB_SUCC(ret)) {
      gc_task.reset();
      ls_meta_info.reset();
      new_ls = false;
      if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
        LOG_INFO("stop collecting ls meta gc tasks", KR(ret),
            K(ctx.tenant_id_), K(ls_count), K(gc_task_count));
      } else if (OB_FAIL(task_iter.get_next(ctx.allocator_, gc_task))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next ls meta gc task", KR(ret),
              K(ctx.tenant_id_), K(gc_task_count));
        }
      } else if (OB_FAIL(ls_meta_info.from_value(gc_task.value_))) {
        LOG_WARN("fail to deserialize ls meta gc task", KR(ret),
            K(ctx.tenant_id_), K(gc_task.key_));
      } else if (OB_UNLIKELY(!ls_meta_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls meta gc task value", KR(ret),
            K(ctx.tenant_id_), K(gc_task.key_), K(ls_meta_info));
      } else if (OB_FAIL(append_ls_meta_gc_task_(
          ctx, gc_task, ls_meta_info, new_ls))) {
        LOG_WARN("fail to aggregate ls meta gc task", KR(ret),
            K(ctx.tenant_id_), K(gc_task.key_), K(ls_meta_info));
      } else {
        ++gc_task_count;
        if (new_ls) {
          ++ls_count;
        }
      }
    }
  }
  return ret;
}

int ObSSLSMetaGCPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  SCN cur_gts;
  int64_t ls_count = 0;
  int64_t gc_task_count = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls meta gc prepare task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLSMetaGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get ls meta gc dag ctx", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop ls meta gc prepare task", KR(ret), KPC(ctx));
  } else if (OB_UNLIKELY(ctx->map_inited_
      || ctx->map_iter_inited_
      || ctx->cur_gts_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls meta gc dag context has already been prepared", KR(ret),
        KPC(ctx));
  } else {
    static_cast<ObSSLSMetaGCDag *>(get_dag())->report_start_event();
    if (OB_FAIL(ObAtomicProtocolUtil::get_gts(ctx->tenant_id_, cur_gts))) {
      LOG_WARN("fail to get gts for ls meta gc", KR(ret),
          K_(ctx->tenant_id));
    } else if (OB_UNLIKELY(!cur_gts.is_valid()
        || cur_gts.is_min()
        || cur_gts.is_max())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid gts for ls meta gc", KR(ret),
          K_(ctx->tenant_id), K(cur_gts));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", KR(ret), K_(ctx->tenant_id));
    } else if (OB_FAIL(ctx->ls_meta_gc_tasks_map_.create(
        DEFAULT_TASK_MAP_BUCKET_NUM,
        ObMemAttr(ctx->tenant_id_, "LSMetaGCMap")))) {
      LOG_WARN("fail to create ls meta gc task map", KR(ret),
          K_(ctx->tenant_id));
    } else {
      ctx->map_inited_ = true;
      if (OB_FAIL(collect_ls_meta_gc_tasks_(
          *ctx, ls_count, gc_task_count))) {
        LOG_WARN("fail to collect ls meta gc tasks", KR(ret),
            K_(ctx->tenant_id), K(ls_count), K(gc_task_count));
      } else {
        ctx->cur_gts_ = cur_gts;
        ctx->map_iter_ = ctx->ls_meta_gc_tasks_map_.begin();
        ctx->map_end_ = ctx->ls_meta_gc_tasks_map_.end();
        ctx->add_ls_count(ls_count);
        ctx->add_gc_task_count(gc_task_count);
        // Publish iterator readiness last. ScheduleTask owns the iterator after
        // PrepareTask completes and never mutates the task map itself.
        ctx->map_iter_inited_ = true;
      }
    }
  }
  LOG_INFO("finish ls meta gc prepare task", KR(ret), K(ls_count),
      K(gc_task_count), KPC(ctx));
  return ret;
}

ObSSLSMetaGCScheduleTask::ObSSLSMetaGCScheduleTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_LS_META_GC),
    is_inited_(false),
    next_task_generated_(false)
{
}

int ObSSLSMetaGCScheduleTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls meta gc schedule task init twice", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSLSMetaGCScheduleTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ObSSLSMetaGCDag *dag = nullptr;
  ObSSLSMetaGCExecutorTask *executor_task = nullptr;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls meta gc schedule task is not initialized", KR(ret));
  } else if (next_task_generated_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLSMetaGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get ls meta gc dag ctx", KR(ret));
  } else if (OB_UNLIKELY(!ctx->map_inited_
      || !ctx->map_iter_inited_
      || !ctx->cur_gts_.is_valid()
      || ctx->cur_gts_.is_min()
      || ctx->cur_gts_.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc task map is not ready", KR(ret), KPC(ctx));
  } else if (ctx->map_iter_ == ctx->map_end_) {
    next_task_generated_ = true;
    ret = OB_ITER_END;
  } else {
    const ObSSLSMetaGCTaskMap::const_iterator current_iter = ctx->map_iter_;
    ObSSLSMetaGCTaskMap::const_iterator next_iter = current_iter;
    const ObLSID ls_id = current_iter->first;
    const ObArray<ObSharedGCTask> *gc_tasks = current_iter->second;
    ++next_iter;
    const bool has_more = next_iter != ctx->map_end_;
    if (OB_UNLIKELY(!ls_id.is_valid()
        || OB_ISNULL(gc_tasks)
        || gc_tasks->empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls meta gc task map entry", KR(ret), K(ls_id),
          KP(gc_tasks), KPC(ctx));
    } else if (OB_ISNULL(dag =
        static_cast<ObSSLSMetaGCDag *>(get_dag()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls meta gc dag is null", KR(ret), K(ls_id));
    } else if (OB_FAIL(dag->alloc_task(executor_task))) {
      LOG_WARN("fail to allocate ls meta gc executor task", KR(ret),
          K(ls_id));
    } else if (OB_ISNULL(executor_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocated ls meta gc executor task is null", KR(ret),
          K(ls_id));
    } else if (OB_FAIL(executor_task->init(ls_id, gc_tasks, has_more))) {
      LOG_WARN("fail to init ls meta gc executor task", KR(ret), K(ls_id),
          K(has_more));
    } else {
      // Commit iterator progress only after the Executor is fully initialized.
      ctx->map_iter_ = next_iter;
      next_task_generated_ = true;
      next_task = executor_task;
    }
  }
  return ret;
}

int ObSSLSMetaGCScheduleTask::process()
{
  int ret = OB_SUCCESS;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls meta gc schedule task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLSMetaGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get ls meta gc dag ctx", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop ls meta gc schedule task", KR(ret), KPC(ctx));
  } else if (OB_UNLIKELY(!ctx->map_inited_
      || !ctx->map_iter_inited_
      || !ctx->cur_gts_.is_valid()
      || ctx->cur_gts_.is_min()
      || ctx->cur_gts_.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc task map is not ready", KR(ret), KPC(ctx));
  } else if (OB_UNLIKELY(!next_task_generated_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc schedule successor has not been generated", KR(ret),
        KPC(ctx));
  }
  return ret;
}

ObSSLSMetaGCExecutorTask::ObSSLSMetaGCExecutorTask()
  : ObSharedGCBaseTask(ObITask::TASK_TYPE_SS_LS_META_GC),
    is_inited_(false),
    next_task_generated_(false),
    ls_id_(),
    gc_tasks_(nullptr),
    has_more_(false)
{
}

int ObSSLSMetaGCExecutorTask::init(
    const ObLSID &ls_id,
    const ObArray<ObSharedGCTask> *gc_tasks,
    const bool has_more)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls meta gc executor task init twice", KR(ret), K_(ls_id));
  } else if (OB_UNLIKELY(!ls_id.is_valid()
      || OB_ISNULL(gc_tasks)
      || gc_tasks->empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta gc executor task input", KR(ret), K(ls_id),
        KP(gc_tasks), K(has_more));
  } else {
    ls_id_ = ls_id;
    gc_tasks_ = gc_tasks;
    has_more_ = has_more;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  ObSSLSMetaGCDag *dag = nullptr;
  ObSSLSMetaGCScheduleTask *schedule_task = nullptr;
  next_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls meta gc executor task is not initialized", KR(ret));
  } else if (next_task_generated_) {
    ret = OB_ITER_END;
  } else if (!has_more_) {
    next_task_generated_ = true;
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLSMetaGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get ls meta gc dag ctx", KR(ret), K_(ls_id));
  } else if (OB_UNLIKELY(!ctx->map_inited_
      || !ctx->map_iter_inited_
      || !ctx->cur_gts_.is_valid()
      || ctx->cur_gts_.is_min()
      || ctx->cur_gts_.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc task map is not ready", KR(ret), K_(ls_id),
        KPC(ctx));
  } else if (OB_ISNULL(dag =
      static_cast<ObSSLSMetaGCDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc dag is null", KR(ret), K_(ls_id));
  } else if (OB_FAIL(dag->alloc_task(schedule_task))) {
    LOG_WARN("fail to allocate next ls meta gc schedule task", KR(ret),
        K_(ls_id));
  } else if (OB_ISNULL(schedule_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocated ls meta gc schedule task is null", KR(ret),
        K_(ls_id));
  } else if (OB_FAIL(schedule_task->init())) {
    LOG_WARN("fail to init next ls meta gc schedule task", KR(ret),
        K_(ls_id));
  } else {
    next_task_generated_ = true;
    next_task = schedule_task;
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::get_ls_tablet_ids_(
    const ObSSLSMetaGCDagCtx &ctx,
    ObIArray<ObSSGCTabletBaseInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> tablet_iter_guard;
  ObSSMetaTabletKeyIterator *tablet_iter = nullptr;
  ObSSMetaService *meta_service = nullptr;
  tablet_infos.reset();
  if (OB_UNLIKELY(!ctx.is_valid()
      || !ls_id_.is_valid()
      || !ctx.cur_gts_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls tablet enumeration input", KR(ret), K(ctx),
        K_(ls_id));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K_(ls_id),
        K_(ctx.tenant_id));
  } else {
    read_param.set_ls_level_param(
        ObSSMetaReadParamType::LS_PREFIX,
        ObSSMetaReadResultType::READ_ONLY_KEY,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id_, SCN::min_scn(), true /* is_in_table */);
    read_param.set_read_mark_delete(true);
    if (OB_FAIL(meta_service->get_tablet_id_iter(
        read_param, ctx.cur_gts_, tablet_iter_guard))) {
      LOG_WARN("fail to get tablet id iterator", KR(ret), K_(ls_id),
          K_(ctx.tenant_id), K_(ctx.cur_gts), K(read_param));
    } else if (OB_FAIL(tablet_iter_guard.get_iter(tablet_iter))) {
      LOG_WARN("fail to get tablet id iterator from guard", KR(ret),
          K_(ls_id), K_(ctx.tenant_id), K_(ctx.cur_gts));
    } else if (OB_ISNULL(tablet_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet id iterator is null", KR(ret), K_(ls_id),
          K_(ctx.tenant_id));
    } else {
      while (OB_SUCC(ret)) {
        ObSSMetaTabletKey key;
        if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
          LOG_INFO("stop enumerating ls tablets", KR(ret), K_(ls_id),
              K_(ctx.tenant_id));
        } else if (OB_FAIL(tablet_iter->get_next(key))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next tablet key", KR(ret), K_(ls_id),
                K_(ctx.tenant_id), K_(ctx.cur_gts));
          }
        } else if (OB_FAIL(tablet_infos.push_back(
            ObSSGCTabletBaseInfo(
                key.tablet_id_, key.reorganization_scn_)))) {
          LOG_WARN("fail to append ls tablet info", KR(ret), K_(ls_id),
              K_(ctx.tenant_id), K(key));
        }
      }
    }
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::build_tablet_info_(
    const ObSSLSMetaGCDagCtx &ctx,
    const ObSSGCTabletBaseInfo &tablet_info,
    ObSSMetaService *meta_service,
    ObArenaAllocator &allocator,
    ObSSLSMetaGCTabletReplayedScnInfoMap &tablet_replayed_scn_info_map)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  ObSSMetaIterGuard<ObSSMetaTabletSCNIterator> scn_iter_guard;
  ObSSMetaTabletSCNIterator *scn_iter = nullptr;
  ObTabletHandle oldest_tablet_handle;
  ObTabletHandle latest_tablet_handle;
  ObTablet *oldest_tablet = nullptr;
  ObTablet *latest_tablet = nullptr;
  SCN oldest_meta_scn;
  ObSSLSMetaGCTabletReplayedScnInfo replayed_scn_info;
  bool tablet_exist = true;
  allocator.reset();
  if (OB_UNLIKELY(!ctx.is_valid()
      || !ls_id_.is_valid()
      || !tablet_info.is_valid()
      || OB_ISNULL(meta_service))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet replay info input", KR(ret), K(ctx),
        K_(ls_id), K(tablet_info), KP(meta_service));
  } else {
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id_, tablet_info.tablet_id_, SCN::min_scn(),
        tablet_info.reorg_scn_, true /* is_in_table */);
    read_param.set_read_mark_delete(true);
    // TODO(muwei.ym): 迭代 tablet 时会多次访问 meta table，需要进行性能优化。
    if (OB_FAIL(meta_service->get_tablet_scn_iter(
        read_param, SCN::max_scn(), scn_iter_guard))) {
      LOG_WARN("fail to get tablet scn iterator", KR(ret), K_(ls_id),
          K_(ctx.tenant_id), K(tablet_info));
    } else if (OB_FAIL(scn_iter_guard.get_iter(scn_iter))) {
      LOG_WARN("fail to get tablet scn iterator from guard", KR(ret),
          K_(ls_id), K_(ctx.tenant_id), K(tablet_info));
    } else if (OB_ISNULL(scn_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet scn iterator is null", KR(ret), K_(ls_id),
          K_(ctx.tenant_id), K(tablet_info));
    } else if (OB_FAIL(scn_iter->get_next(oldest_meta_scn))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        tablet_exist = false;
        LOG_INFO("tablet does not exist when building replay info",
            K_(ls_id), K_(ctx.tenant_id), K(tablet_info));
      } else {
        LOG_WARN("fail to get oldest tablet meta scn", KR(ret),
            K_(ls_id), K_(ctx.tenant_id), K(tablet_info));
      }
    }

    if (OB_FAIL(ret) || !tablet_exist) {
      // Do nothing.
    } else {
      if (OB_FAIL(meta_service->get_tablet(
          ls_id_, tablet_info.tablet_id_, tablet_info.reorg_scn_,
          oldest_meta_scn, allocator, oldest_tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("oldest tablet does not exist", K_(ls_id),
              K_(ctx.tenant_id), K(tablet_info), K(oldest_meta_scn));
        } else {
          LOG_WARN("fail to get oldest tablet", KR(ret), K_(ls_id),
              K_(ctx.tenant_id), K(tablet_info), K(oldest_meta_scn));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(meta_service->get_tablet(
          ls_id_, tablet_info.tablet_id_, tablet_info.reorg_scn_,
          allocator, latest_tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("latest tablet does not exist", K_(ls_id),
              K_(ctx.tenant_id), K(tablet_info));
        } else {
          LOG_WARN("fail to get latest tablet", KR(ret), K_(ls_id),
              K_(ctx.tenant_id), K(tablet_info));
        }
      }
      if (OB_SUCC(ret)) {
        oldest_tablet = oldest_tablet_handle.get_obj();
        latest_tablet = latest_tablet_handle.get_obj();
        if (OB_ISNULL(oldest_tablet) && OB_ISNULL(latest_tablet)) {
          LOG_INFO("oldest and latest tablet both do not exist",
              K_(ls_id), K_(ctx.tenant_id), K(tablet_info),
              K(oldest_meta_scn));
        } else {
          if (OB_NOT_NULL(oldest_tablet) && OB_NOT_NULL(latest_tablet)) {
            replayed_scn_info.is_cold_ =
                oldest_tablet->get_tablet_meta().get_max_replayed_scn()
                == latest_tablet->get_tablet_meta().get_max_replayed_scn();
          }
          replayed_scn_info.max_replayed_scn_ =
              OB_NOT_NULL(oldest_tablet)
              ? oldest_tablet->get_tablet_meta().get_max_replayed_scn()
              : latest_tablet->get_tablet_meta().get_max_replayed_scn();
          if (OB_FAIL(tablet_replayed_scn_info_map.set_refactored(
              tablet_info, replayed_scn_info))) {
            LOG_WARN("fail to add tablet replay info", KR(ret), K_(ls_id),
                K_(ctx.tenant_id), K(tablet_info), K(oldest_meta_scn),
                K(replayed_scn_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::build_tablet_replayed_scn_info_map_(
    const ObSSLSMetaGCDagCtx &ctx,
    const ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
    ObSSLSMetaGCTabletReplayedScnInfoMap
        &tablet_replayed_scn_info_map)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = nullptr;
  ObArenaAllocator allocator(ObMemAttr(ctx.tenant_id_, "SSLSMetaGC"));
  if (OB_UNLIKELY(!ctx.is_valid() || !ls_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replay info map input", KR(ret), K(ctx), K_(ls_id));
  } else if (tablet_infos.empty()) {
    // Do nothing.
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K_(ls_id),
        K_(ctx.tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_infos.count(); ++i) {
      if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
        LOG_INFO("stop building tablet replay info map", KR(ret),
            K_(ls_id), K_(ctx.tenant_id), K(i));
      } else if (OB_FAIL(build_tablet_info_(ctx, tablet_infos.at(i),
          meta_service, allocator, tablet_replayed_scn_info_map))) {
        LOG_WARN("fail to build tablet replay info", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(tablet_infos.at(i)));
      }
    }
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::get_ls_meta_checkpoint_scn_(
    const ObSSLSMetaGCDagCtx &ctx,
    const SCN &ls_meta_scn,
    SCN &ls_ss_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = nullptr;
  ObSSLSMeta ls_meta;
  ls_ss_checkpoint_scn.reset();
  if (OB_UNLIKELY(!ctx.is_valid()
      || !ls_id_.is_valid()
      || !ls_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta query input", KR(ret), K(ctx), K_(ls_id),
        K(ls_meta_scn));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K_(ls_id),
        K_(ctx.tenant_id), K(ls_meta_scn));
  } else if (OB_FAIL(meta_service->get_ls_meta(
      ls_id_, ls_meta_scn, ls_meta))) {
    if (OB_LS_NOT_EXIST != ret) {
      LOG_WARN("fail to get ls meta", KR(ret), K_(ls_id),
          K_(ctx.tenant_id), K(ls_meta_scn));
    }
  } else {
    ls_ss_checkpoint_scn = ls_meta.get_ss_checkpoint_scn();
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::remove_ls_meta_(
    const ObSSLSMetaGCDagCtx &ctx,
    const SCN &ls_meta_scn)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = nullptr;
  if (OB_UNLIKELY(!ctx.is_valid()
      || !ls_id_.is_valid()
      || !ls_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta removal input", KR(ret), K(ctx), K_(ls_id),
        K(ls_meta_scn));
  } else if (OB_ISNULL(meta_service = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K_(ls_id),
        K_(ctx.tenant_id), K(ls_meta_scn));
  } else if (OB_FAIL(meta_service->remove_ls(ls_id_, ls_meta_scn))) {
    LOG_WARN("fail to remove ls meta", KR(ret), K_(ls_id),
        K_(ctx.tenant_id), K(ls_meta_scn));
  } else {
    LOG_INFO("remove ls meta success", K_(ls_id), K_(ctx.tenant_id), K(ls_meta_scn));
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::finish_ls_meta_gc_task_(
    const ObSSLSMetaGCDagCtx &ctx,
    const ObSharedGCTask &gc_task,
    const SCN &ls_meta_scn,
    bool &is_gced)
{
  int ret = OB_SUCCESS;
  is_gced = false;
  if (OB_UNLIKELY(!ctx.is_valid()
      || !ls_id_.is_valid()
      || !ls_meta_scn.is_valid()
      || !gc_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta gc task finish input", KR(ret), K(ctx),
        K_(ls_id), K(ls_meta_scn), K(gc_task));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ls_id), K_(ctx.tenant_id),
        K(ls_meta_scn));
  } else if (OB_FAIL(
      ObSharedGCTaskTableOperator::delete_task_and_insert_history(
          gc_task, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to finish ls meta gc task", KR(ret), K_(ls_id),
        K_(ctx.tenant_id), K(ls_meta_scn), K(gc_task.key_));
  } else {
    is_gced = true;
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::do_ls_meta_version_gc_(
    ObSSLSMetaGCDagCtx &ctx,
    const ObSharedGCTask &gc_task,
    const ObIArray<ObSSGCTabletBaseInfo> &tablet_infos,
    const ObSSLSMetaGCTabletReplayedScnInfoMap
        &tablet_replayed_scn_info_map,
    bool &is_gced)
{
  int ret = OB_SUCCESS;
  ObSharedGCLSMetaInfo gc_info;
  SCN ls_ss_checkpoint_scn;
  bool can_gc_ls_meta_version = true;
  is_gced = false;
  if (OB_FAIL(gc_info.from_value(gc_task.value_))) {
    LOG_WARN("fail to deserialize ls meta gc task", KR(ret), K_(ls_id),
        K_(ctx.tenant_id), K(gc_task.key_));
  } else if (OB_UNLIKELY(!gc_info.is_valid()
      || gc_info.ls_id_ != ls_id_
      || !gc_info.meta_scn_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls meta gc task value", KR(ret), K_(ls_id),
        K_(ctx.tenant_id), K(gc_info), K(gc_task.key_));
  } else {
    const SCN &ls_meta_scn = gc_info.meta_scn_;
    if (OB_FAIL(get_ls_meta_checkpoint_scn_(
        ctx, ls_meta_scn, ls_ss_checkpoint_scn))) {
      if (OB_LS_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(finish_ls_meta_gc_task_(
            ctx, gc_task, ls_meta_scn, is_gced))) {
          LOG_WARN("fail to finish absent ls meta gc task", KR(ret),
              K_(ls_id), K_(ctx.tenant_id), K(ls_meta_scn), K(gc_task.key_));
        } else {
          ctx.add_finished_gc_task_count(1);
        }
      } else {
        LOG_WARN("fail to get ls meta checkpoint scn", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(ls_meta_scn));
      }
    } else if (OB_UNLIKELY(!ls_ss_checkpoint_scn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls ss checkpoint scn", KR(ret), K_(ls_id),
          K_(ctx.tenant_id), K(ls_meta_scn), K(ls_ss_checkpoint_scn));
    } else {
      for (int64_t i = 0; OB_SUCC(ret)
          && can_gc_ls_meta_version
          && i < tablet_infos.count(); ++i) {
        ObSSLSMetaGCTabletReplayedScnInfo replayed_scn_info;
        if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
          LOG_INFO("stop checking tablet replay info", KR(ret), K_(ls_id),
              K_(ctx.tenant_id), K(ls_meta_scn), K(i));
        } else if (OB_FAIL(tablet_replayed_scn_info_map.get_refactored(
            tablet_infos.at(i), replayed_scn_info))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("tablet replay info does not exist, skip it",
                K_(ls_id), K_(ctx.tenant_id), K(ls_meta_scn),
                K(tablet_infos.at(i)));
          } else {
            LOG_WARN("fail to get tablet replay info", KR(ret), K_(ls_id),
                K_(ctx.tenant_id), K(ls_meta_scn),
                K(tablet_infos.at(i)));
          }
        } else if (OB_UNLIKELY(!replayed_scn_info.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tablet replay info", KR(ret), K_(ls_id),
              K_(ctx.tenant_id), K(ls_meta_scn), K(tablet_infos.at(i)),
              K(replayed_scn_info));
        } else if (replayed_scn_info.max_replayed_scn_
            < ls_ss_checkpoint_scn
            && !replayed_scn_info.is_cold_) {
          can_gc_ls_meta_version = false;
          LOG_INFO("tablet has not replayed beyond ls checkpoint",
              K_(ls_id), K_(ctx.tenant_id), K(ls_meta_scn),
              K(tablet_infos.at(i)), K(replayed_scn_info),
              K(ls_ss_checkpoint_scn));
          char extra_info[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
          int64_t pos = 0;
          (void)databuff_printf(extra_info, sizeof(extra_info), pos, "ls_ss_checkpoint_scn=%lu",
               ls_ss_checkpoint_scn.get_val_for_inner_table_field());
          if (TC_REACH_TIME_INTERVAL(3600LL * 1000LL * 1000LL)) { // every 1h
            SERVER_EVENT_ADD("SHARED_GC", "LS_META_CAN_NOT_GC", "tenant_id", ctx.tenant_id_,
                "ls_id", ls_id_.id(), "ls_meta_scn", ls_meta_scn, "tablet_id",
                tablet_infos.at(i).tablet_id_.id(), "transfer_scn", tablet_infos.at(i).reorg_scn_,
                "max_replayed_scn", replayed_scn_info.max_replayed_scn_, extra_info);
          }
        }
      }

      if (OB_SUCC(ret) && can_gc_ls_meta_version) {
        if (OB_FAIL(remove_ls_meta_(ctx, ls_meta_scn))) {
          LOG_WARN("fail to remove ls meta version", KR(ret), K_(ls_id),
              K_(ctx.tenant_id), K(ls_meta_scn));
        } else {
          ctx.add_deleted_ls_meta_version_count(1);
          if (OB_FAIL(finish_ls_meta_gc_task_(
              ctx, gc_task, ls_meta_scn, is_gced))) {
            LOG_WARN("fail to finish ls meta gc task", KR(ret), K_(ls_id),
                K_(ctx.tenant_id), K(ls_meta_scn), K(gc_task.key_));
          } else {
            ctx.add_finished_gc_task_count(1);
            FLOG_INFO("succeed to gc ls meta version", K_(ls_id),
                K_(ctx.tenant_id), K(ls_meta_scn), K(gc_task.key_));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::do_ls_meta_gc_(ObSSLSMetaGCDagCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObSharedGCTask> sorted_gc_tasks;
  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  ObSSLSMetaGCTabletReplayedScnInfoMap tablet_replayed_scn_info_map;
  bool replay_map_inited = false;
  sorted_gc_tasks.set_attr(ObMemAttr(ctx.tenant_id_, "SSLSMetaGCTask"));
  tablet_infos.set_attr(ObMemAttr(ctx.tenant_id_, "SSLSMetaTablet"));
  if (OB_UNLIKELY(!ctx.is_valid()
      || !ctx.cur_gts_.is_valid()
      || !ls_id_.is_valid()
      || OB_ISNULL(gc_tasks_)
      || gc_tasks_->empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta gc executor input", KR(ret), K(ctx),
        K_(ls_id), KP_(gc_tasks));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < gc_tasks_->count(); ++i) {
      const ObSharedGCTask &gc_task = gc_tasks_->at(i);
      ObSharedGCLSMetaInfo gc_info;
      if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
        LOG_INFO("stop validating ls meta gc tasks", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(i));
      } else if (OB_UNLIKELY(!gc_task.is_valid()
          || gc_task.key_.tenant_id_ != ctx.tenant_id_
          || !is_ls_meta_gc_type(gc_task.key_.type_)
          || 0 != gc_task.key_.round_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls meta gc task", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(gc_task));
      } else if (OB_FAIL(gc_info.from_value(gc_task.value_))) {
        LOG_WARN("fail to deserialize ls meta gc task", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(gc_task.key_));
      } else if (OB_UNLIKELY(!gc_info.is_valid()
          || gc_info.ls_id_ != ls_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls meta gc task value", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(gc_info), K(gc_task.key_));
      } else if (OB_FAIL(sorted_gc_tasks.push_back(gc_task))) {
        LOG_WARN("fail to copy ls meta gc task", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(gc_task.key_));
      }
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(sorted_gc_tasks.get_data(),
          sorted_gc_tasks.get_data() + sorted_gc_tasks.count(),
          ObSSLSMetaGCTaskCompare(ret));
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to sort ls meta gc tasks", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(sorted_gc_tasks.count()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_ls_tablet_ids_(ctx, tablet_infos))) {
      LOG_WARN("fail to get ls tablet ids", KR(ret), K_(ls_id),
          K_(ctx.tenant_id));
    } else if (!tablet_infos.empty()) {
      if (OB_FAIL(tablet_replayed_scn_info_map.create(
          common::hash::cal_next_prime(tablet_infos.count() + 1),
          ObMemAttr(ctx.tenant_id_, "SSLSReplayScn")))) {
        LOG_WARN("fail to create tablet replay info map", KR(ret), K_(ls_id),
            K_(ctx.tenant_id), K(tablet_infos.count()));
      } else {
        replay_map_inited = true;
        if (OB_FAIL(build_tablet_replayed_scn_info_map_(
            ctx, tablet_infos, tablet_replayed_scn_info_map))) {
          LOG_WARN("fail to build tablet replay info map", KR(ret), K_(ls_id),
              K_(ctx.tenant_id), K(tablet_infos.count()));
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret)
      && i < sorted_gc_tasks.count(); ++i) {
    bool is_gced = false;
    if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
      LOG_INFO("stop processing ls meta gc versions", KR(ret), K_(ls_id),
          K_(ctx.tenant_id), K(i));
    } else if (OB_FAIL(do_ls_meta_version_gc_(ctx,
        sorted_gc_tasks.at(i), tablet_infos,
        tablet_replayed_scn_info_map, is_gced))) {
      LOG_WARN("fail to gc ls meta version", KR(ret), K_(ls_id),
          K_(ctx.tenant_id), K(sorted_gc_tasks.at(i).key_));
    } else if (!is_gced) {
      LOG_INFO("ls meta version cannot be gced, stop current ls",
          K_(ls_id), K_(ctx.tenant_id), K(sorted_gc_tasks.at(i).key_));
      break;
    }
  }
  if (replay_map_inited) {
    tablet_replayed_scn_info_map.destroy();
  }
  return ret;
}

int ObSSLSMetaGCExecutorTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSLSMetaGCDagCtx *ctx = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls meta gc executor task is not initialized", KR(ret));
  } else if (OB_FAIL(ObSSGCDagUtil::get_dag_ctx<ObSSLSMetaGCDag>(
      *this, ctx))) {
    LOG_WARN("fail to get ls meta gc dag ctx", KR(ret), K_(ls_id));
  } else if (OB_FAIL(ObSSGCDagUtil::check_if_stopped(*this))) {
    LOG_INFO("stop ls meta gc executor task", KR(ret), K_(ls_id), KPC(ctx));
  } else if (OB_UNLIKELY(!ctx->map_inited_
      || !ctx->map_iter_inited_
      || !ctx->cur_gts_.is_valid()
      || ctx->cur_gts_.is_min()
      || ctx->cur_gts_.is_max())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc task map is not ready", KR(ret), K_(ls_id),
        KPC(ctx));
  } else if (OB_UNLIKELY(!next_task_generated_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls meta gc executor successor has not been generated", KR(ret),
        K_(ls_id), KPC(ctx));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(ls_id), KPC(ctx));
  } else if (OB_TMP_FAIL(do_ls_meta_gc_(*ctx))) {
    LOG_WARN("fail to process ls meta gc", KR(tmp_ret), K_(ls_id), KPC(ctx));
  }
  if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
    const int stop_ret = ObSSGCDagUtil::check_if_stopped(*this);
    if (OB_SUCCESS != stop_ret) {
      ret = stop_ret;
      LOG_INFO("stop ls meta gc executor task after business error", KR(ret),
          KR(tmp_ret), K_(ls_id), KPC(ctx));
    } else {
      ctx->record_error_code(tmp_ret);
      LOG_WARN("defer ls meta gc after business error", KR(tmp_ret),
          K_(ls_id), KPC(ctx));
    }
  }
  LOG_INFO("finish ls meta gc executor task", KR(ret),
      K_(ls_id), K_(has_more), "gc_task_count",
      OB_ISNULL(gc_tasks_) ? 0 : gc_tasks_->count(), KPC(ctx));
  return ret;
}

} // namespace storage
} // namespace oceanbase
