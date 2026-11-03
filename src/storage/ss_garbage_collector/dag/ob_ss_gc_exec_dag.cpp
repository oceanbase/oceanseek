/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/dag/ob_ss_gc_exec_dag.h"
#include "lib/allocator/page_arena.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "storage/ss_garbage_collector/ob_ss_fail_gc_util.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_util.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_FAIL_GC_EXEC_PROCESS_ROW_ERROR);
ERRSIM_POINT_DEF(EN_SS_FAIL_GC_EXEC_PROCESS_SLICE_ERROR);
#endif

ObSSGCExecutorCostStat::ObSSGCExecutorCostStat()
{
  reset();
}

void ObSSGCExecutorCostStat::reset()
{
  start_us_ = INT64_MAX;
  finish_us_ = 0;
  plan_start_us_ = INT64_MAX;
  plan_finish_us_ = 0;
  execute_start_us_ = INT64_MAX;
  execute_finish_us_ = 0;
  succ_row_count_ = 0;
  failed_row_count_ = 0;
  succ_sstable_macro_cnt_ = 0;
  succ_tablet_sub_meta_cnt_ = 0;
}

int64_t ObSSGCExecutorCostStat::total_cost_us() const
{
  return ((INT64_MAX == start_us_) || (0 == finish_us_)) ? 0 : (finish_us_ - start_us_);
}

int64_t ObSSGCExecutorCostStat::plan_cost_us() const
{
  return ((INT64_MAX == plan_start_us_) || (0 == plan_finish_us_)) ? 0
      : (plan_finish_us_ - plan_start_us_);
}

int64_t ObSSGCExecutorCostStat::execute_cost_us() const
{
  return ((INT64_MAX == execute_start_us_) || (0 == execute_finish_us_)) ? 0
      : (execute_finish_us_ - execute_start_us_);
}

int64_t ObSSGCExecutorCostStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  (void)databuff_printf(buf, buf_len, pos,
                        "{total_cost_us=%ld,plan_cost_us=%ld,execute_cost_us=%ld,"
                        "succ_row_count=%ld,failed_row_count=%ld,"
                        "succ_sstable_macro_cnt=%ld,succ_tablet_sub_meta_cnt=%ld}",
                        total_cost_us(),
                        plan_cost_us(),
                        execute_cost_us(),
                        succ_row_count_,
                        failed_row_count_,
                        succ_sstable_macro_cnt_,
                        succ_tablet_sub_meta_cnt_);
  return pos;
}

bool ObSSGCExecDagInitParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
      && is_shared_data_gc_task_type(gc_type_)
      && (allow_gc_round_id_ >= 0);
}

ObSSGCExecDag::ObSSGCExecDag()
  : ObSSGCDagBase(ObDagType::DAG_TYPE_SS_GC_EXEC),
    tenant_id_(OB_INVALID_TENANT_ID),
    gc_type_(ObSharedGCTaskType::INVALID_TYPE),
    allow_gc_round_id_(-1),
    cost_stat_(),
    is_inited_(false)
{
}

bool ObSSGCExecDag::is_valid() const
{
  return is_inited_
      && is_valid_tenant_id(tenant_id_)
      && is_shared_data_gc_task_type(gc_type_)
      && (allow_gc_round_id_ >= 0);
}

ObSSGCExecDag::~ObSSGCExecDag()
{
  destroy();
}

void ObSSGCExecDag::destroy()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  gc_type_ = ObSharedGCTaskType::INVALID_TYPE;
  allow_gc_round_id_ = -1;
  cost_stat_.reset();
}

int ObSSGCExecDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss gc exec dag init twice", KR(ret));
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), KP(param));
  } else {
    const ObSSGCExecDagInitParam *p = static_cast<const ObSSGCExecDagInitParam *>(param);
    tenant_id_ = p->tenant_id_;
    gc_type_ = p->gc_type_;
    allow_gc_round_id_ = p->allow_gc_round_id_;
    cost_stat_.reset();
    is_inited_ = true;
  }
  return ret;
}

int ObSSGCExecDag::create_first_task()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag not inited", KR(ret));
  } else if (ObSharedGCTaskType::MARK_SWEEP_GC == gc_type_) {
    // MarkSweep uses the Plan + Execute pattern: a single plan task
    // queries the round/task_id ranges and chains execute tasks by slice.
    ObMarkSweepPlanTask *plan_task = nullptr;
    if (OB_FAIL(create_task(nullptr/*parent*/, plan_task, tenant_id_, allow_gc_round_id_))) {
      LOG_WARN("fail to create mark sweep plan task", KR(ret),
          K_(tenant_id), K_(allow_gc_round_id));
    }
  } else if (ObSharedGCTaskType::FAIL_GC == gc_type_) {
    ObFailGCPlanTask *plan_task = nullptr;
    if (OB_FAIL(create_task(nullptr/*parent*/, plan_task, tenant_id_))) {
      LOG_WARN("fail to create fail gc plan task", KR(ret), K_(tenant_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected gc type in create_first_task", KR(ret), K_(gc_type));
  }

  LOG_INFO("finish to create ss gc exec dag first task", KR(ret),
      K_(tenant_id), K_(gc_type), K_(allow_gc_round_id));
  return ret;
}

bool ObSSGCExecDag::operator==(const ObIDag &other) const
{
  bool bret = true;
  if (this != &other) {
    if (get_type() != other.get_type()) {
      bret = false;
    } else {
      const ObSSGCExecDag &o = static_cast<const ObSSGCExecDag &>(other);
      bret = (tenant_id_ == o.tenant_id_) && (gc_type_ == o.gc_type_);
    }
  }
  return bret;
}

uint64_t ObSSGCExecDag::hash() const
{
  uint64_t hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
  const int gc_type_int = static_cast<int>(gc_type_);
  hash_val = common::murmurhash(&gc_type_int, sizeof(gc_type_int), hash_val);
  return hash_val;
}

int ObSSGCExecDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                                   ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss gc exec dag not init", KR(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                        static_cast<int64_t>(tenant_id_), 
                                        allow_gc_round_id_))) {
    LOG_WARN("fail to add dag warning info param", KR(ret),
        K_(tenant_id), K_(allow_gc_round_id));
  }
  return ret;
}

int ObSSGCExecDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss gc exec dag not init", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
                                     "ObSSGCExecDag: tenant_id=%lu gc_type=%d "
                                     "allow_gc_round_id=%ld",
                                     tenant_id_,
                                     static_cast<int>(gc_type_),
                                     allow_gc_round_id_))) {
    LOG_WARN("failed to fill dag key", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSGCExecDag::report_result()
{
  int ret = OB_SUCCESS;
  const int32_t dag_ret = get_dag_ret();
  const ObIDag::ObDagStatus dag_status = get_dag_status();
  (void)inc_update(&cost_stat_.finish_us_, ObTimeUtility::current_time_us());
  if (OB_FAIL(SERVER_EVENT_ADD("SHARED_GC", "SS_GC_EXEC_DAG_FINISH",
                               "tenant_id", tenant_id_,
                               "gc_type", ob_shared_gc_task_type_to_string(gc_type_),
                               "allow_gc_round_id", allow_gc_round_id_,
                               "dag_ret", dag_ret,
                               "dag_status", static_cast<int64_t>(dag_status),
                               "dag_id", get_dag_id(),
                               cost_stat_ /*extra_info*/))) {
    LOG_WARN("fail to add ss gc exec dag finish event", KR(ret),
        K_(tenant_id), K_(gc_type), K_(allow_gc_round_id), K(dag_ret), K(dag_status));
  }
  LOG_INFO("report ss gc exec dag result", KR(ret), K_(tenant_id), K_(gc_type),
      K_(allow_gc_round_id), K(dag_ret), K(dag_status), K_(cost_stat));
  return ret;
}

void ObSSGCExecDag::record_start_us()
{
  (void)dec_update(&cost_stat_.start_us_, ObTimeUtility::current_time_us());
}

void ObSSGCExecDag::record_plan_us(const int64_t plan_start_us, const int64_t plan_finish_us)
{
  (void)dec_update(&cost_stat_.plan_start_us_, plan_start_us);
  (void)inc_update(&cost_stat_.plan_finish_us_, plan_finish_us);
}

void ObSSGCExecDag::record_execute_us(const int64_t execute_start_us, const int64_t execute_finish_us)
{
  (void)dec_update(&cost_stat_.execute_start_us_, execute_start_us);
  (void)inc_update(&cost_stat_.execute_finish_us_, execute_finish_us);
}

void ObSSGCExecDag::add_row_stat(
    const int64_t succ_row_count,
    const int64_t failed_row_count,
    const int64_t succ_sstable_macro_cnt,
    const int64_t succ_tablet_sub_meta_cnt)
{
  (void)ATOMIC_AAF(&cost_stat_.succ_row_count_, succ_row_count);
  (void)ATOMIC_AAF(&cost_stat_.failed_row_count_, failed_row_count);
  (void)ATOMIC_AAF(&cost_stat_.succ_sstable_macro_cnt_, succ_sstable_macro_cnt);
  (void)ATOMIC_AAF(&cost_stat_.succ_tablet_sub_meta_cnt_, succ_tablet_sub_meta_cnt);
}

// ============================ ObExecuteBaseTask ============================
ObExecuteBaseTask::ProcessSliceFn::ProcessSliceFn(ObExecuteBaseTask &task)
  : task_(task)
{}

int ObExecuteBaseTask::ProcessSliceFn::operator()()
{
  return task_.process_slice_();
}

ObExecuteBaseTask::OnRetryFn::OnRetryFn(ObExecuteBaseTask &task)
  : task_(task)
{}

int ObExecuteBaseTask::OnRetryFn::operator()()
{
  return task_.check_if_stopped_or_failed();
}

ObExecuteBaseTask::ObExecuteBaseTask(const ObITask::ObITaskType task_type)
  : ObSharedGCBaseTask(task_type),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    round_range_(),
    slice_range_()
{
}

int ObExecuteBaseTask::init_base_(
    const uint64_t tenant_id,
    const ObSSGCTaskIdRange &round_range,
    const ObSSGCTaskIdRange &slice_range)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss gc execute task init twice", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || round_range.start_task_id < 0
      || round_range.end_task_id < round_range.start_task_id
      || slice_range.start_task_id < round_range.start_task_id
      || slice_range.end_task_id < slice_range.start_task_id
      || round_range.end_task_id < slice_range.end_task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss gc execute task argument", KR(ret),
        K(tenant_id), K(round_range), K(slice_range));
  } else {
    tenant_id_ = tenant_id;
    round_range_ = round_range;
    slice_range_ = slice_range;
    is_inited_ = true;
  }
  return ret;
}

int ObExecuteBaseTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObSSGCExecDag *dag = static_cast<ObSSGCExecDag *>(get_dag());
  const int64_t next_start_task_id = slice_range_.end_task_id + 1;
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (next_start_task_id > round_range_.end_task_id) {
    ret = OB_ITER_END;
  } else {
    const int64_t slice_size = get_next_slice_size_();
    if (OB_UNLIKELY(slice_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ss gc slice size", KR(ret), K(slice_size), K_(tenant_id), K_(round_range));
    } else {
      const int64_t slice_end_task_id =
          std::min(next_start_task_id + slice_size - 1, round_range_.end_task_id);
      const ObSSGCTaskIdRange slice_range(next_start_task_id, slice_end_task_id);
      if (OB_FAIL(alloc_next_task_(*dag, slice_range, next_task))) {
        LOG_WARN("fail to alloc next ss gc execute task", KR(ret),
            K_(tenant_id), K_(round_range), K(slice_range), K(get_gc_type_()));
      }
    }
  }
  return ret;
}

int ObExecuteBaseTask::process()
{
  int ret = OB_SUCCESS;
  int64_t retry_cnt = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss gc execute task not inited", KR(ret), K(get_gc_type_()));
  } else {
    ProcessSliceFn work_fn(*this);
    OnRetryFn on_retry_fn(*this);
    if (OB_FAIL(execute_with_retry(work_fn, on_retry_fn, retry_cnt))) {
      LOG_WARN("fail to process ss gc execute task slice with retry", KR(ret),
          K_(tenant_id), K_(round_range), K_(slice_range), K(retry_cnt), K(get_gc_type_()));
    }
  }
  return ret;
}

int ObExecuteBaseTask::process_slice_()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  int64_t succ_row_count = 0;
  int64_t failed_row_count = 0;
  int64_t succ_sstable_macro_cnt = 0;
  int64_t succ_tablet_sub_meta_cnt = 0;
  char dag_id_buf[common::OB_MAX_TRACE_ID_BUFFER_SIZE] = {0};
  ObString trace_id_str;
  ObSSGCExecDag *dag = static_cast<ObSSGCExecDag *>(get_dag());
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObSharedGCTaskTableRangeIterator iter;
    ObArenaAllocator alloc(lib::ObMemAttr(MTL_ID(), get_page_allocator_label_()));
    const int64_t server_id = static_cast<int64_t>(GCTX.get_server_id());
    dag->get_dag_id().to_string(dag_id_buf, sizeof(dag_id_buf));
    trace_id_str.assign_ptr(dag_id_buf, static_cast<int32_t>(strlen(dag_id_buf)));

    if (OB_FAIL(iter.init(tenant_id_,
                          get_gc_type_(),
                          get_iter_round_id_(),
                          slice_range_,
                          *GCTX.sql_proxy_))) {
      LOG_WARN("fail to init ss gc iterator", KR(ret),
          K_(tenant_id), K_(slice_range), K(get_gc_type_()), K(get_iter_round_id_()));
    } else {
      // Successful rows are committed together at the end of the slice.
      ObSharedGCTaskArray succ_rows;
      succ_rows.set_attr(ObMemAttr(tenant_id_, get_succ_rows_label_()));
      int64_t pending_sstable_macro_cnt = 0;
      int64_t pending_tablet_sub_meta_cnt = 0;
      bool is_iter_end = false;
      while (OB_SUCC(ret) && !is_iter_end) {
        ObSharedGCTask row;
        if (OB_FAIL(check_if_stopped_or_failed())) {
          LOG_WARN("fail to check if stopped or failed", KR(ret));
        } else if (OB_FAIL(iter.get_next(alloc, row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_iter_end = true;
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          int64_t row_sstable_macro_cnt = 0;
          int64_t row_tablet_sub_meta_cnt = 0;
          if (OB_TMP_FAIL(process_row_(row, server_id, trace_id_str,
                  row_sstable_macro_cnt, row_tablet_sub_meta_cnt))) {
            if (OB_ENTRY_EXIST == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
              // 认领冲突或 peer 已完成属于预期跳过，process_row_ 内已记录日志。
            } else {
              ++failed_row_count;
              LOG_WARN("fail to process row, skip batch finish", KR(tmp_ret), "key", row.key_);
            }
          } else if (OB_TMP_FAIL(succ_rows.push_back(row))) {
            ++failed_row_count;
            LOG_WARN("fail to push back succ row, skip batch finish", KR(tmp_ret), "key", row.key_);
          } else {
            pending_sstable_macro_cnt += row_sstable_macro_cnt;
            pending_tablet_sub_meta_cnt += row_tablet_sub_meta_cnt;
          }
        }
      }
      if (OB_SUCC(ret) && !succ_rows.empty()) {
        if (OB_FAIL(ObSharedGCTaskTableOperator::batch_delete_tasks_and_insert_histories(
                tenant_id_, succ_rows))) {
          failed_row_count += succ_rows.count();
          LOG_WARN("fail to batch finish slice rows", KR(ret),
              K_(tenant_id), "succ_rows_count", succ_rows.count());
        } else {
          succ_row_count += succ_rows.count();
          // Count only committed rows
          succ_sstable_macro_cnt = pending_sstable_macro_cnt;
          succ_tablet_sub_meta_cnt = pending_tablet_sub_meta_cnt;
        }
      }
      alloc.reset();
    }
  }
  if (OB_NOT_NULL(dag)) {
    dag->record_execute_us(start_us, ObTimeUtility::current_time_us());
    dag->add_row_stat(succ_row_count, failed_row_count,
        succ_sstable_macro_cnt, succ_tablet_sub_meta_cnt);
  }

#ifdef ERRSIM
  if (OB_SUCC(ret) && ObSharedGCTaskType::FAIL_GC == get_gc_type_()) {
    INJECT_SS_GC_ERRSIM(EN_SS_FAIL_GC_EXEC_PROCESS_SLICE_ERROR, tenant_id_);
  }
#endif

  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("SHARED_GC", "FAIL_GC_PROCESS_SLICE_FAIL",
        "tenant_id", tenant_id_,
        "slice_start", slice_range_.start_task_id,
        "slice_end", slice_range_.end_task_id,
        "succ_rows", succ_row_count,
        "trace_id", trace_id_str,
        "ret", ret);
  }
  return ret;
}

int ObExecuteBaseTask::update_server_id_and_trace_id_(
    ObSharedGCTask &row,
    const int64_t server_id,
    const ObString &trace_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
          row.key_, server_id, trace_id, *GCTX.sql_proxy_))) {
    // OB_ENTRY_EXIST / OB_ENTRY_NOT_EXIST 是预期的 skip 信号，由调用方按行处理，
    // 这里只对非预期错误打 WARN，避免线上双日志误报。
    if (OB_ENTRY_EXIST != ret && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to update server id and trace id", KR(ret), K(row.key_), K(server_id), K(trace_id));
    }
  } else {
    row.server_id_ = server_id;
    row.trace_id_ = trace_id;
  }
  return ret;
}

// ============================ ObSSGCPlanBaseTask ============================
ObSSGCPlanBaseTask::ObSSGCPlanBaseTask(const ObITask::ObITaskType task_type)
  : ObSharedGCBaseTask(task_type),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

int ObSSGCPlanBaseTask::init_base_(const uint64_t tenant_id, const bool is_valid_arg)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss gc plan task init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(is_valid_arg));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObSSGCPlanBaseTask::record_plan_us_(const int64_t start_us)
{
  ObSSGCExecDag *dag = static_cast<ObSSGCExecDag *>(get_dag());
  if (OB_NOT_NULL(dag)) {
    dag->record_plan_us(start_us, ObTimeUtility::current_time_us());
  }
}

// ============================ ObMarkSweepPlanTask ============================
ObMarkSweepPlanTask::ObMarkSweepPlanTask()
  : ObSSGCPlanBaseTask(ObITask::TASK_TYPE_SS_MARK_SWEEP_GC),
    allow_gc_round_id_(-1)
{
}

int ObMarkSweepPlanTask::init(
    const uint64_t tenant_id,
    const int64_t allow_gc_round_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base_(tenant_id, allow_gc_round_id >= 0))) {
    LOG_WARN("fail to init mark sweep plan task", KR(ret), K(tenant_id), K(allow_gc_round_id));
  } else {
    allow_gc_round_id_ = allow_gc_round_id;
  }
  return ret;
}

int ObMarkSweepPlanTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  ObSSGCExecDag *dag = static_cast<ObSSGCExecDag *>(get_dag());
  bool is_empty = true;
  int64_t min_round_id = OB_INVALID_ID;
  bool need_execute_task = false;
  ObSSGCTaskIdRange round_range;
  ObSSGCTaskIdRange slice_range;
  ObMarkSweepExecuteTask *execute_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss gc plan task not inited", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (FALSE_IT(dag->record_start_us())) {
  // Step 1: find the smallest round_id and check the watermark. If there is
  // nothing to GC, the plan finishes immediately and the DAG is a no-op.
  // One timer cycle only drains rows under this min_round_id; the next
  // timer-triggered DAG will pick up the next smallest round_id.
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_min_round_id(
          tenant_id_,
          ObSharedGCTaskType::MARK_SWEEP_GC,
          OB_ALL_SHARED_GC_TASK_TNAME,
          *GCTX.sql_proxy_,
          is_empty,
          min_round_id))) {
    LOG_WARN("fail to get min_round_id", KR(ret),
        K_(tenant_id), K_(allow_gc_round_id));
  } else if (is_empty || (min_round_id > allow_gc_round_id_)) {
    LOG_INFO("no rows to gc in window, plan task exits",
        K_(tenant_id), K_(allow_gc_round_id));
  // Step 2: query task_id range and compute the first slice for min_round_id.
  } else if (OB_FAIL(slice_task_id_range_(min_round_id, need_execute_task, round_range, slice_range))) {
    LOG_WARN("fail to plan round", KR(ret), K_(tenant_id), K(min_round_id));
  } else if (!need_execute_task) {
    // nothing to do
  // Step 3: enqueue the first execute task; later slices chain via generate_next_task.
  } else if (OB_FAIL(dag->create_task(this/*parent*/, execute_task, tenant_id_, min_round_id,
                                      round_range, slice_range, allow_gc_round_id_))) {
    LOG_WARN("fail to create first execute task", KR(ret),
        K(min_round_id), K(round_range), K(slice_range));
  }
  record_plan_us_(start_us);
  return ret;
}

int ObMarkSweepPlanTask::slice_task_id_range_(
    const int64_t round_id,
    bool &need_execute_task,
    ObSSGCTaskIdRange &round_range,
    ObSSGCTaskIdRange &slice_range)
{
  int ret = OB_SUCCESS;
  bool is_empty = true;
  need_execute_task = false;
  round_range = ObSSGCTaskIdRange();
  slice_range = ObSSGCTaskIdRange();
  if (OB_FAIL(ObSharedGCTaskTableOperator::get_task_id_range(
          tenant_id_,
          ObSharedGCTaskType::MARK_SWEEP_GC,
          round_id,
          OB_ALL_SHARED_GC_TASK_TNAME,
          *GCTX.sql_proxy_,
          is_empty,
          round_range))) {
    LOG_WARN("fail to select task_id range", KR(ret), K_(tenant_id), K(round_id));
  } else if (is_empty) {
    // need_execute_task remains false. This happens when all rows for min_round_id have
    // already been deleted (e.g., another executor drained this round concurrently, or the
    // round produced zero GC tasks because every macro id in the diff result was reclaimed
    // before DiffTask wrote its first task row). The DAG exits as a no-op; the timer's
    // next tick will pick up the next eligible round_id.
    LOG_INFO("no rows to gc in round, need_execute_task=false", K_(tenant_id), K(round_id));
  } else if (OB_UNLIKELY(!round_range.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid task_id range", KR(ret), K_(tenant_id), K(round_id),
        K(round_range));
  } else {
    const int64_t slice_end_task_id =
        std::min(round_range.start_task_id + ObExecuteBaseTask::DEFAULT_SLICE_SIZE - 1,
                 round_range.end_task_id);
    slice_range = ObSSGCTaskIdRange(round_range.start_task_id, slice_end_task_id);
    need_execute_task = true;
  }
  return ret;
}

// ============================ ObMarkSweepExecuteTask ============================
ObMarkSweepExecuteTask::ObMarkSweepExecuteTask()
  : ObExecuteBaseTask(ObITask::TASK_TYPE_SS_MARK_SWEEP_GC),
    round_id_(0),
    allow_gc_round_id_(-1)
{
}

int ObMarkSweepExecuteTask::init(
    const uint64_t tenant_id,
    const int64_t round_id,
    const ObSSGCTaskIdRange &round_range,
    const ObSSGCTaskIdRange &slice_range,
    const int64_t allow_gc_round_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(round_id < 0 || allow_gc_round_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mark sweep execute task argument", KR(ret),
        K(tenant_id), K(round_id), K(allow_gc_round_id), K(round_range), K(slice_range));
  } else if (OB_FAIL(init_base_(tenant_id, round_range, slice_range))) {
    LOG_WARN("fail to init mark sweep execute task base", KR(ret),
        K(tenant_id), K(round_id), K(round_range), K(slice_range));
  } else {
    round_id_ = round_id;
    allow_gc_round_id_ = allow_gc_round_id;
  }
  return ret;
}

int ObMarkSweepExecuteTask::alloc_next_task_(
    ObSSGCExecDag &dag,
    const ObSSGCTaskIdRange &slice_range,
    ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObMarkSweepExecuteTask *task = nullptr;
  next_task = nullptr;
  if (OB_FAIL(dag.alloc_task(task))) {
    LOG_WARN("fail to alloc next mark sweep execute task", KR(ret));
  } else if (OB_FAIL(task->init(tenant_id_, round_id_, round_range_, slice_range, allow_gc_round_id_))) {
    LOG_WARN("fail to init next mark sweep execute task", KR(ret),
        K_(tenant_id), K_(round_id), K_(round_range), K(slice_range));
  } else {
    next_task = task;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
    task->reset_node();
    dag.remove_task(*task);
    task = nullptr;
  }
  return ret;
}

int ObMarkSweepExecuteTask::process_row_(
    ObSharedGCTask &row,
    const int64_t server_id,
    const ObString &trace_id,
    int64_t &sstable_macro_cnt,
    int64_t &tablet_sub_meta_cnt)
{
  int ret = OB_SUCCESS;
  sstable_macro_cnt = 0;
  tablet_sub_meta_cnt = 0;
  const uint64_t tenant_id = row.key_.tenant_id_;
  ObSharedGCMacroIdList macro_list;

  // Step A：反序列化 value -> MacroBlockId 列表。value 损坏属于单行失败，
  // 该行留在 __all_shared_gc_task，等待下一轮 DAG 重试。
  if (OB_FAIL(macro_list.from_value(row.value_))) {
    LOG_WARN("failed to deserialize task value",
        KR(ret), "key", row.key_, "value_len", row.value_.length());
  }

  // Step B：用 (server_id, trace_id) 认领行。未认领的行、同 server 重试，
  // 以及 owner 已超时的行可以被认领；live owner 冲突或已完成的行不能继续
  // 进入 Step C。
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_server_id_and_trace_id_(row, server_id, trace_id))) {
      if (OB_ENTRY_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("skip row: update rejected or peer already completed",
            KR(ret), "key", row.key_);
      } else {
        LOG_WARN("fail to update server id and trace id", KR(ret), "key", row.key_);
      }
    }
  }

  // Step C：调用统一删除接口batch_delete_blocks
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObSSGCUtil::batch_delete_blocks(tenant_id, macro_list.macro_id_array_,
            sstable_macro_cnt, tablet_sub_meta_cnt))) {
      LOG_WARN("fail to batch delete blocks", KR(ret), "key", row.key_,
          "macro_count", macro_list.count());
    }
  }

  return ret;
}

// ============================ ObFailGCPlanTask ============================
ObFailGCPlanTask::ObFailGCPlanTask()
  : ObSSGCPlanBaseTask(ObITask::TASK_TYPE_SS_FAIL_GC)
{
}

int ObFailGCPlanTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base_(tenant_id, true/*is_valid_arg*/))) {
    LOG_WARN("fail to init fail gc plan task", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObFailGCPlanTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  ObSSGCExecDag *dag = static_cast<ObSSGCExecDag *>(get_dag());
  bool is_empty = true;
  ObSSGCTaskIdRange full_range;
  ObFailGCExecuteTask *execute_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss gc plan task not inited", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (FALSE_IT(dag->record_start_us())) {
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_task_id_range(
          tenant_id_,
          ObSharedGCTaskType::FAIL_GC,
          0 /*round_id*/,
          OB_ALL_SHARED_GC_TASK_TNAME,
          *GCTX.sql_proxy_,
          is_empty,
          full_range))) {
    LOG_WARN("fail to get task_id range", KR(ret), K_(tenant_id));
  } else if (is_empty) {
    LOG_INFO("no fail gc rows, plan task exits", K_(tenant_id));
  } else {
    // Cap this DAG's window to scan_batch_size so each invocation is bounded.
    const int64_t batch_size = ObSSGCUtil::get_scan_batch_size(tenant_id_);
    const int64_t round_end_task_id =
        std::min(full_range.start_task_id + batch_size - 1, full_range.end_task_id);
    const ObSSGCTaskIdRange round_range(full_range.start_task_id, round_end_task_id);
    const int64_t slice_end_task_id =
        std::min(round_range.start_task_id + ObExecuteBaseTask::DEFAULT_SLICE_SIZE - 1,
                 round_range.end_task_id);
    const ObSSGCTaskIdRange slice_range(round_range.start_task_id, slice_end_task_id);
    if (OB_FAIL(dag->create_task(this/*parent*/, execute_task,
                                 tenant_id_, round_range, slice_range))) {
      LOG_WARN("fail to create first fail gc execute task", KR(ret),
          K_(tenant_id), K(round_range), K(slice_range));
    } else {
      LOG_INFO("fail gc plan task done", K_(tenant_id), K(round_range), K(slice_range));
    }
  }
  record_plan_us_(start_us);
  return ret;
}

// ============================ ObFailGCExecuteTask ============================
ObFailGCExecuteTask::ObFailGCExecuteTask()
  : ObExecuteBaseTask(ObITask::TASK_TYPE_SS_FAIL_GC)
{
}

int ObFailGCExecuteTask::init(
    const uint64_t tenant_id,
    const ObSSGCTaskIdRange &round_range,
    const ObSSGCTaskIdRange &slice_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base_(tenant_id, round_range, slice_range))) {
    LOG_WARN("fail to init fail gc execute task base", KR(ret),
        K(tenant_id), K(round_range), K(slice_range));
  }
  return ret;
}

int ObFailGCExecuteTask::alloc_next_task_(
    ObSSGCExecDag &dag,
    const ObSSGCTaskIdRange &slice_range,
    ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObFailGCExecuteTask *task = nullptr;
  next_task = nullptr;
  if (OB_FAIL(dag.alloc_task(task))) {
    LOG_WARN("fail to alloc next fail gc execute task", KR(ret));
  } else if (OB_FAIL(task->init(tenant_id_, round_range_, slice_range))) {
    LOG_WARN("fail to init next fail gc execute task", KR(ret),
        K_(tenant_id), K_(round_range), K(slice_range));
  } else {
    next_task = task;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(task)) {
    task->reset_node();
    dag.remove_task(*task);
    task = nullptr;
  }
  return ret;
}

int ObFailGCExecuteTask::process_row_(
    ObSharedGCTask &row,
    const int64_t server_id,
    const ObString &trace_id,
    int64_t &sstable_macro_cnt,
    int64_t &tablet_sub_meta_cnt)
{
  int ret = OB_SUCCESS;
  sstable_macro_cnt = 0;
  tablet_sub_meta_cnt = 0;
  ObSharedGCFailInfo info;

  // Step A：反序列化 value -> ObSharedGCFailInfo。
  if (OB_FAIL(info.from_value(row.value_))) {
    LOG_WARN("fail to deserialize fail gc row value", KR(ret), "key", row.key_);
  }

  // Step B：用 (server_id, trace_id) 认领行。未认领的行、同 server 重试，
  // 以及 owner 已超时的行可以被认领；live owner 冲突或已完成的行不能继续
  // 进入 Step C。
  if (OB_SUCC(ret)) {
    if (OB_FAIL(update_server_id_and_trace_id_(row, server_id, trace_id))) {
      if (OB_ENTRY_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("skip row: update rejected or peer already completed",
            KR(ret), "key", row.key_);
      } else {
        LOG_WARN("fail to update server id and trace id", KR(ret), "key", row.key_);
      }
    }
  }

  // Step C：按 meta 类型删除对应对象。
  if (OB_SUCC(ret)) {
    ObSSMetaCtrlType ctrl_type;
    ctrl_type.type_ = info.meta_type_;
    if (ctrl_type.is_sstable_type() && !ctrl_type.is_lock_row()) {
      const uint64_t tenant_id = row.key_.tenant_id_;
      ObSEArray<MacroBlockId, OB_DEFAULT_SE_ARRAY_COUNT> macro_id_array;
      macro_id_array.set_attr(ObMemAttr(tenant_id, "FailGCSST"));
      if (OB_FAIL(ObSSFailGCUtil::detect_sstable_block_ids(
              info.meta_type_,
              info.ls_id_,
              info.tablet_id_,
              info.transfer_scn_,
              info.ctrl_object_info_,
              info.op_id_,
              macro_id_array))) {
        LOG_WARN("fail to detect sstable block ids", KR(ret), K(info));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObSSGCUtil::batch_delete_blocks(tenant_id, macro_id_array,
                sstable_macro_cnt, tablet_sub_meta_cnt))) {
          LOG_WARN("fail to batch delete sstable blocks", KR(ret), K(info),
              "macro_count", macro_id_array.count());
        } else if (OB_FAIL(record_fail_gc_macro_ids_(row, macro_id_array))) {
          LOG_WARN("fail to record fail gc macro ids", KR(ret), K(info),
              "macro_count", macro_id_array.count());
        }
      }
    } else if (blocksstable::ObStorageObjectType::SHARED_TABLET_META == info.meta_type_) {
      if (OB_FAIL(process_tablet_meta_row_(info, row,
              sstable_macro_cnt, tablet_sub_meta_cnt))) {
        LOG_WARN("fail to process tablet meta row", KR(ret), K(info));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected meta type in fail gc row", KR(ret), K(info));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret) && ObSSGCUtil::is_errsim_target(row.key_.tenant_id_, info.tablet_id_.id())) {
    INJECT_SS_GC_ERRSIM(EN_SS_FAIL_GC_EXEC_PROCESS_ROW_ERROR, row.key_.tenant_id_);
  }
#endif

  if (OB_FAIL(ret)) {
    SERVER_EVENT_ADD("SHARED_GC", "FAIL_GC_PROCESS_ROW_FAIL",
        "tenant_id", row.key_.tenant_id_,
        "meta_type", static_cast<int64_t>(info.meta_type_),
        "tablet_id", info.tablet_id_.id(),
        "trace_id", row.trace_id_,
        "ret", ret);
  }

  return ret;
}

int ObFailGCExecuteTask::process_tablet_meta_row_(
    const ObSharedGCFailInfo &info,
    const ObSharedGCTask &row,
    int64_t &sstable_macro_cnt,
    int64_t &tablet_sub_meta_cnt)
{
  int ret = OB_SUCCESS;
  sstable_macro_cnt = 0;
  tablet_sub_meta_cnt = 0;
  const uint64_t tenant_id = row.key_.tenant_id_;
  ObSEArray<MacroBlockId, OB_DEFAULT_SE_ARRAY_COUNT> macro_id_array;
  macro_id_array.set_attr(ObMemAttr(tenant_id, "FailGCTabMeta"));
  if (OB_FAIL(ObSSFailGCUtil::detect_tablet_meta_block_ids(
          info.ls_id_,
          info.tablet_id_,
          info.transfer_scn_,
          info.op_id_,
          macro_id_array))) {
    LOG_WARN("fail to detect tablet meta block ids", KR(ret), K(info));
  } else if (OB_FAIL(ObSSGCUtil::batch_delete_blocks(tenant_id, macro_id_array,
          sstable_macro_cnt, tablet_sub_meta_cnt))) {
    LOG_WARN("fail to batch delete tablet meta blocks", KR(ret), K(info),
        "macro_count", macro_id_array.count());
  } else if (OB_FAIL(record_fail_gc_macro_ids_(row, macro_id_array))) {
    LOG_WARN("fail to record fail gc macro ids", KR(ret), K(info),
        "macro_count", macro_id_array.count());
  }
  return ret;
}

int ObFailGCExecuteTask::record_fail_gc_macro_ids_(
    const ObSharedGCTask &src_row,
    const ObIArray<MacroBlockId> &macro_id_array)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = src_row.key_.tenant_id_;
  const int64_t fail_gc_task_id = src_row.key_.task_id_;
  ObSharedGCMacroIdList cur_list;
  int64_t cur_serialize_size = cur_list.get_serialize_size();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), K(tenant_id));
  }
  // Split the macro id list into chunks, each serialize_size below MAX_VALUE_LENGTH.
  for (int64_t i = 0; OB_SUCC(ret) && (i < macro_id_array.count()); ++i) {
    const MacroBlockId &macro_id = macro_id_array.at(i);
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid macro id", KR(ret), K(macro_id), K(src_row.key_));
    } else {
      int64_t next_serialize_size = 0;
      if (OB_FAIL(cur_list.get_serialize_size_after_add(
              macro_id, cur_serialize_size, next_serialize_size))) {
        LOG_WARN("fail to get serialize size after add", KR(ret), K(macro_id), K(src_row.key_));
      } else if (next_serialize_size > ObSharedGCTask::MAX_VALUE_LENGTH) {
        if (OB_UNLIKELY(cur_list.empty())) { // defence: single macro id too large
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("single macro id serialize size overflow, unexpected", KR(ret),
              K(next_serialize_size), K(macro_id), K(src_row.key_));
        } else if (OB_FAIL(ObSharedGCTaskUtil::generate_fail_gc_macro_id_history(
            tenant_id, fail_gc_task_id, cur_list, *GCTX.sql_proxy_))) {
          LOG_WARN("fail to generate fail gc macro id history", KR(ret), K(src_row.key_),
              "cur_list_count", cur_list.count(), K(cur_serialize_size));
        } else {
          cur_list.reset();
          cur_serialize_size = cur_list.get_serialize_size();
          if (OB_FAIL(cur_list.get_serialize_size_after_add(
                  macro_id, cur_serialize_size, next_serialize_size))) {
            LOG_WARN("fail to get serialize size after reset and add", KR(ret),
                K(macro_id), K(src_row.key_));
          } else if (next_serialize_size > ObSharedGCTask::MAX_VALUE_LENGTH) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("single macro id serialize size overflow, unexpected", KR(ret),
                K(next_serialize_size), K(macro_id), K(src_row.key_));
          }
        }
      }
      if (FAILEDx(cur_list.add(macro_id))) {
        LOG_WARN("fail to add macro id to list", KR(ret), K(macro_id), K(src_row.key_));
      } else {
        cur_serialize_size = next_serialize_size;
      }
    }
  }
  if (OB_SUCC(ret) && !cur_list.empty()) {
    if (OB_FAIL(ObSharedGCTaskUtil::generate_fail_gc_macro_id_history(
        tenant_id, fail_gc_task_id, cur_list, *GCTX.sql_proxy_))) {
      LOG_WARN("fail to generate fail gc macro id history", KR(ret), K(src_row.key_),
          "cur_list_count", cur_list.count(), K(cur_serialize_size));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
