/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include "ob_vector_index_merge_task.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_aux_table_handler.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_vector_index_aux_table_handler.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/allocator/ob_tenant_vector_allocator.h"
#include "share/ob_ddl_common.h"
#include "storage/ls/ob_ls.h"
#include "storage/lob/common/ob_lob_util.h"

namespace oceanbase
{
namespace share
{
ERRSIM_POINT_DEF(ERRSIM_VEC_MERGE_BEFORE_EXCHANGE,
    "Inject an error before vector index merge exchange.");

ERRSIM_POINT_DEF(ERRSIM_VEC_MERGE_SCN_EQUAL_FREEZE,
                 "Use the registered freeze SCN for merge; match selects the incremental tablet.");
ERRSIM_POINT_DEF(ERRSIM_VEC_MERGE_SCN_BEFORE_FREEZE,
                 "Use a merge SCN older than the registered freeze SCN; match selects the incremental tablet.");

int ObVecIdxMergeTaskExecutor::do_check_task_result(
    ObVecIndexAsyncTaskCtx *task_ctx,
    ObVecTaskResultCheckAction &action)
{
  return do_check_normal_running_task_result(task_ctx, action);
}

bool ObVecIdxMergeTaskExecutor::check_operation_allow()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool bret = true;
  const bool is_not_support = ! ObPluginVectorIndexHelper::enable_persist_vector_index_incremental(tenant_id_);
  if (is_not_support) {
    bret = false;
    LOG_DEBUG("skip this round, not support async task.");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_6_3_1) {
    bret = false;
    LOG_DEBUG("vector index merge task requires data version 4_6_3_1", K(tenant_data_version));
  } else if (OB_FAIL(ObPluginVectorIndexHelper::get_merge_base_percentage(tenant_id_, merge_base_percentage_))) {
    LOG_WARN("failed to get merge base percentage", K(ret), K(tenant_id_));
  }
  return bret;
}

int ObVecIdxMergeTaskExecutor::load_triggered_task(const ObVecIndexTaskStatus &task_row)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector index load task not inited", KR(ret));
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) {
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_handle_));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null index_ls_mgr", K(ret));
  } else {
    storage::ObLS *ls = ls_handle_.get_ls();
    ObVecIndexTaskStatus task_row_copy = task_row;
    // estimate memory for alter param merge task
    if (task_row_copy.trigger_type_ ==
        ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_ALTER_INDEX_PARAM) {
      ObTabletID inc_tablet_id;
      ObPluginVectorIndexAdapterGuard adpt_guard;
      ObVecIndexAsyncTaskCtx estimate_ctx;
      int64_t estimate_mem = 0;
      if (OB_FAIL(index_ls_mgr->get_inc_tablet_id_by_vbitmap(
              task_row_copy.table_id_, task_row_copy.tablet_id_,
              inc_tablet_id))) {
        LOG_WARN("fail to get inc tablet id for triggered merge task", K(ret),
                 K(task_row_copy));
      } else if (OB_FAIL(index_ls_mgr->get_adapter_inst_guard(inc_tablet_id,
                                                              adpt_guard))) {
        LOG_WARN("fail to get adapter for triggered merge task", K(ret),
                 K(inc_tablet_id), K(task_row_copy));
      } else {
        task_row_copy.inc_tablet_id_ = inc_tablet_id;
        estimate_ctx.task_status_ = task_row_copy;
        // for estimate memory
        estimate_ctx.task_status_.tenant_id_ = tenant_id_;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::estimate_task_memory(
                &estimate_ctx, estimate_mem, adpt_guard.get_adatper()))) {
          LOG_WARN("fail to estimate triggered merge task memory", K(tmp_ret),
                   K(task_row_copy));
        } else {
          task_row_copy.task_info_.task_estimate_memory_ = estimate_mem;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::try_add_task_ctx_from_inner_row(
                   tenant_id_, task_row_copy, ls, ls_handle_, *index_ls_mgr,
                   true /*verify_tablet_on_ls*/))) {
      LOG_WARN("fail to add triggered merge task ctx", K(ret),
               K(task_row_copy));
    }
  }
  return ret;
}

int ObVecIdxMergeTaskExecutor::load_task(
    uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObVecIndexSchemaChecker schema_checker(tenant_id_, schema_guard);
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", KR(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) { // skip
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_handle_));
  } else if (OB_NOT_NULL(index_ls_mgr) && index_ls_mgr->get_async_task_opt().is_stop()) {
    // LS is being destroyed; skip creating new tasks.  Cancellation of existing
    // tasks is handled by check_and_schedule_* later in the same tick.
    LOG_INFO("[VEC_ASYNC_TASK] ls is stopping, skip load task", K(tenant_id_),
             K(ls_handle_.get_ls()->get_ls_id()));
  } else {
    storage::ObLS *ls = ls_handle_.get_ls();
    common::hash::ObHashSet<uint64_t> dropping_index_ids;
    if (OB_FAIL(dropping_index_ids.create(16))) {
      LOG_WARN("fail to create dropping index set", KR(ret), K_(tenant_id), K_(ls_handle));
    } else if (OB_FAIL(get_dropping_index_ids(tenant_id_, dropping_index_ids))) {
    }
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    const int64_t current_task_cnt = ObVecIndexAsyncTaskUtil::get_processing_task_cnt(task_opt);

    ObSEArray<ObTabletID, 64> adapter_tablet_ids;
    if (OB_SUCC(ret) && OB_FAIL(index_ls_mgr->collect_adaptor_tablet_ids(adapter_tablet_ids))) {
      LOG_WARN("fail to collect task adapter tablets", KR(ret), K_(tenant_id), K_(ls_handle));
    }
    for (int64_t adapter_idx = 0; OB_SUCC(ret) && adapter_idx < adapter_tablet_ids.count()
         && task_ctx_array.count() + current_task_cnt <= MAX_ASYNC_TASK_PROCESSING_COUNT; ++adapter_idx) {
      const ObTabletID adapter_tablet_id = adapter_tablet_ids.at(adapter_idx);
      ObPluginVectorIndexAdapterGuard adapter_guard;
      if (OB_FAIL(index_ls_mgr->get_adapter_inst_guard(adapter_tablet_id, adapter_guard))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get task adapter", KR(ret), K_(tenant_id), K(adapter_tablet_id));
        }
        continue;
      }
      ObPluginVectorIndexAdaptor *adapter = adapter_guard.get_adatper();
      ObTabletID tablet_id = OB_ISNULL(adapter) ? ObTabletID() : adapter->get_vbitmap_tablet_id();
      if (OB_ISNULL(adapter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else {
        int64_t new_task_id = OB_INVALID_ID;
        int64_t index_table_id = OB_INVALID_ID;
        bool inc_new_task = false;
        bool task_ctx_in_map = false;
        common::ObCurTraceId::TraceId new_trace_id;

        char *task_ctx_buf = nullptr;
        ObVecIndexAsyncTaskCtx* task_ctx = nullptr;
        bool need_merge = false;
        ObVecMergePolicyInput estimate_input;
        ObVecMergePlan estimate_plan;
        bool schema_active = false;
        if (! adapter->is_complete()) {
          LOG_TRACE("adapter not complete, no need merge", K(ret), KPC(adapter));
        } else if (!adapter->is_ready_complete()) {
          LOG_TRACE("adapter is not complete, no need merge", K(ret), KPC(adapter));
        } else if (OB_HASH_EXIST == dropping_index_ids.exist_refactored(adapter->get_inc_table_id())) {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_WARN("[VEC_DDL_SKIP] skip merge generation for dropping index",
                     K_(tenant_id), K_(ls_handle), K(tablet_id), K(adapter->get_inc_table_id()));
          } else {
            LOG_DEBUG("[VEC_DDL_SKIP] skip merge generation for dropping index",
                      K_(tenant_id), K_(ls_handle), K(tablet_id), K(adapter->get_inc_table_id()));
          }
        } else if (adapter->is_skip_merge_sched()) {
          LOG_TRACE("incr bitmap incomplete and delta table not empty, skip merge sched until adaptor replaced", K(ret), KPC(adapter));
        } else if (OB_FAIL(adapter->check_need_merge(ls, merge_base_percentage_, need_merge, estimate_input, estimate_plan))) {
          LOG_WARN("failed to check need merge", K(ret), KPC(adapter));
        } else if (!need_merge) {
          LOG_TRACE("vector index is not need to merge", K(ret), KPC(adapter));
        } else if (OB_FAIL(schema_checker.check_table(adapter->get_vbitmap_table_id(), schema_active))) {
          LOG_WARN("fail to check merge task schema", KR(ret), K(tablet_id));
        } else if (!schema_active) {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            LOG_WARN("[VEC_SCHEMA_SKIP] skip merge task for inactive schema",
                     K_(tenant_id), K_(ls_handle), K(tablet_id), K(adapter->get_vbitmap_table_id()));
          } else {
            LOG_DEBUG("[VEC_SCHEMA_SKIP] skip merge task for inactive schema",
                      K_(tenant_id), K_(ls_handle), K(tablet_id), K(adapter->get_vbitmap_table_id()));
          }
        } else if (OB_NOT_NULL(scheduler_) && scheduler_->is_task_disabled(
                       ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_MERGE, tablet_id.id())) {
          LOG_TRACE("[VEC_IDX_TASK_DISABLED] skip generating disabled task",
                    K_(tenant_id), K(tablet_id), "task_type", "MERGE");
        } else if (OB_ISNULL(task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObVecIndexAsyncTaskCtx))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("async task ctx is null", K(ret));
        } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObVecIndexAsyncTaskCtx())) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
          LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
        } else if (! adapter->is_vbitmap_tablet_valid() || tablet_id != adapter->get_vbitmap_tablet_id()) {
          LOG_DEBUG("index table id is invalid, skip", K(ret)); // skip to next
        } else if (OB_FALSE_IT(index_table_id = adapter->get_vbitmap_table_id())) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++task_trace_base_num, allocator, new_trace_id))) {
          LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
        } else {
          LOG_INFO("[VEC_ASYNC_TASK] task loaded with PREPARE status",
                   K(ret), K(tablet_id), K(tenant_id_), K(new_trace_id), K(new_task_id),
                   K(task_trace_base_num), K(ls->get_ls_id()));
          // 1. update task_ctx to async task map
          task_ctx->tenant_id_ = tenant_id_;
          task_ctx->ls_handle_ = ls_handle_;
          task_ctx->task_status_.tablet_id_ = tablet_id.id();
          task_ctx->task_status_.tenant_id_ = tenant_id_;
          task_ctx->task_status_.table_id_ = index_table_id;
          task_ctx->task_status_.task_id_ = new_task_id;
          task_ctx->task_status_.task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_INDEX_MERGE;
          task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
          task_ctx->task_status_.exec_addr_ = GCTX.self_addr();
          task_ctx->task_status_.trace_id_ = new_trace_id;
          task_ctx->allocator_.set_tenant_id(tenant_id_);
          task_ctx->task_status_.inc_tablet_id_ = adapter->get_inc_tablet_id();
          // Coarse admission here; the worker reserves its actual selected sources.
          int64_t estimate_mem = 0;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::estimate_merge_memory(task_ctx,
                  estimate_input, estimate_plan, estimate_mem, adapter))) {
            LOG_WARN("estimate merge plan memory failed", K(tmp_ret),
                K(tablet_id), K(estimate_plan));
          } else {
            task_ctx->task_status_.task_info_.task_estimate_memory_ = estimate_mem;
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(try_add_task_ctx_for_adapter(
                       *index_ls_mgr, adapter_tablet_id, adapter, task_ctx, inc_new_task))) {
            LOG_WARN("fail to add task ctx", K(ret));
          } else if (FALSE_IT(task_ctx_in_map = inc_new_task)) {
          } else if (inc_new_task && OB_FAIL(task_ctx_array.push_back(task_ctx))) {
            LOG_WARN("fail to push back task status", K(ret), K(task_ctx));
          } else if (!inc_new_task && OB_SUCCESS == tmp_ret) {
            // Reuse this round's cached estimate for an existing waiting AUTO merge.
            ObVecIndexAsyncTaskCtx *waiting_ctx = nullptr;
            ObVecIndexAsyncTaskKey key(tablet_id, OB_VECTOR_ASYNC_INDEX_MERGE);
            common::ObSpinLockGuard map_guard(index_ls_mgr->task_ctx_lock_);
            if (OB_TMP_FAIL(task_opt.get_async_task_map().get_refactored(key, waiting_ctx))) {
              LOG_WARN("get waiting merge task failed", K(tmp_ret), K(key));
            } else if (OB_ISNULL(waiting_ctx)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("waiting merge task is null", K(ret), K(key));
            } else {
              common::ObSpinLockGuard ctx_guard(waiting_ctx->lock_);
              if (!waiting_ctx->in_thread_pool_
                  && waiting_ctx->task_status_.status_ == OB_VECTOR_ASYNC_TASK_PREPARE
                  && waiting_ctx->task_status_.trigger_type_ == OB_VEC_TRIGGER_AUTO) {
                waiting_ctx->task_status_.task_info_.task_estimate_memory_ = estimate_mem;
              }
            }
          }
        }
        if (OB_NOT_NULL(task_ctx) && (!OB_SUCC(ret) || !inc_new_task)) { // release memory when fail
          if (task_ctx_in_map) {
            int tmp_ret = ObVecIndexAsyncTaskUtil::clear_task_ctx(index_ls_mgr->get_async_task_opt(), task_ctx);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("fail to clear task ctx after load task failed", K(tmp_ret), K(ret), K(task_ctx));
            }
          } else {
            task_ctx->~ObVecIndexAsyncTaskCtx();
            allocator->free(task_ctx); // arena need free
          }
          task_ctx = nullptr;
        }
      }
    }
    if (task_ctx_array.count() > 0) {
      LOG_INFO("finish load async task", K(ret), K(ls->get_ls_id()), K(task_ctx_array.count()), K(current_task_cnt));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(insert_new_task(task_ctx_array))) {
    LOG_WARN("fail to insert new tasks", K(ret), K(tenant_id_), K(ls_handle_));
  }
  // clear on fail
  if (OB_FAIL(ret) && !task_ctx_array.empty()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::clear_task_ctxs(index_ls_mgr->get_async_task_opt(), task_ctx_array))) {
      LOG_WARN("fail to clear task ctx", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObVecIdxMergeTask::do_work()
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_VECTOR_MID);
  ObTraceIdGuard trace_guard(ctx_->task_status_.trace_id_);
  LOG_INFO("[VEC_ASYNC_TASK] start merge do_work", K(ret), K(ctx_->task_status_), K(ls_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIdxMergeTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->get_ls())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected nullptr", K(ret), KP(ctx_));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (ctx_->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_MERGE) {
    // Manual and resumed tasks may bypass executor admission checks.
    uint64_t tenant_data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
      LOG_WARN("get tenant data version before merge failed", K(ret), K(tenant_id_));
    } else if (tenant_data_version < DATA_VERSION_4_6_3_1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("vector index merge requires data version 4_6_3_1",
          K(ret), K(tenant_id_), K(tenant_data_version), KPC(ctx_));
    } else if (OB_FAIL(process_merge())) {
      LOG_WARN("fail to process merge", K(ret), KPC(ctx_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task type", K(ret), KPC(ctx_));
  }

  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    if (ctx_->task_status_.status_ == ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL) {
      ret = OB_CANCELED;
    }
    ctx_->task_status_.ret_code_ = ret;
  }
  LOG_INFO("[VEC_ASYNC_TASK] end merge do_work", K(ret), K(ctx_->task_status_));
  DEBUG_SYNC(AFTER_VECTOR_INDEX_MERGE_DO_WORK);
  return ret;
}

bool ObVecIdxMergeTask::is_alter_param_child() const
{
  return OB_NOT_NULL(ctx_)
      && ctx_->task_status_.task_type_ == OB_VECTOR_ASYNC_INDEX_MERGE
      && ctx_->task_status_.trigger_type_ == OB_VEC_TRIGGER_ALTER_INDEX_PARAM
      && ctx_->task_status_.task_info_.alter_param_info_.is_child_valid();
}

int extract_segment_scn(const ObVectorIndexSegmentMeta &seg_meta, ObString &scn_str)
{
  int ret = OB_SUCCESS;
  ObString key = seg_meta.start_key_;
  const ObString tablet_id_str = key.split_on('_');
  scn_str = key.split_on('_');
  if (scn_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scn is empty", K(ret), K(seg_meta));
  }
  return ret;
}

int ObVecIdxMergeTask::prepare_merge_segment()
{
  int ret = OB_SUCCESS;
  const int64_t base_count = merge_source_meta_.bases_.count();
  const bool reserve_memory = !is_alter_param_child();
  int64_t merge_mem = 0;
  merge_segments_.reuse();
  min_vid_ = INT64_MAX;
  max_vid_ = 0;
  filter_sql_str_.reset();
  if (merge_plan_.begin_ < 0 || merge_plan_.end_ <= merge_plan_.begin_
      || merge_plan_.end_ > merge_source_meta_.segment_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid checked merge plan", K(ret), K(merge_plan_), K(merge_source_meta_));
  } else {
    merge_from_base_ = merge_plan_.output_base_;
    for (int64_t i = merge_plan_.begin_; OB_SUCC(ret) && i < merge_plan_.end_; ++i) {
      const ObVectorIndexSegmentMeta &meta = i < base_count ? merge_source_meta_.bases_.at(i)
          : merge_source_meta_.incrs_.at(i - base_count);
      if (!meta.segment_handle_.is_valid() || !meta.segment_handle_->is_inited()
          || meta.scn_ > ctx_->task_status_.target_scn_.get_val_for_inner_table_field()) {
        ret = OB_EAGAIN;
        LOG_INFO("merge source is not ready for target snapshot", K(ret), K(i), K(meta),
            "target_scn", ctx_->task_status_.target_scn_);
      } else if (OB_FAIL(merge_segments_.push_back(&meta))) {
        LOG_WARN("save merge source failed", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && reserve_memory
        && OB_FAIL(ObVecIndexAsyncTaskUtil::estimate_merge_memory(ctx_,
            merge_policy_input_, merge_plan_, merge_mem, old_adapter_))) {
      LOG_WARN("estimate execution merge plan memory failed", K(ret), K(merge_plan_));
    }
    if (OB_SUCC(ret) && reserve_memory) {
      // Same model as load-time admission, using the actual execution window.
      // ALTER retains its existing estimate and scheduler admission.
      {
        common::ObSpinLockGuard guard(ctx_->lock_);
        ctx_->task_status_.task_info_.task_estimate_memory_ = merge_mem;
      }
      if (OB_FAIL(MTL(ObSharedMemAllocMgr*)->vector_allocator().pre_alloc(merge_mem))) {
        LOG_WARN("reserve selected merge memory failed", K(ret), K(merge_mem), K(merge_plan_));
      } else {
        pre_alloc_size_ = merge_mem;
      }
    }
  }
  LOG_INFO("merge plan prepared", K(ret), K(merge_plan_), K(merge_mem));
  return ret;
}

int ObVecIdxMergeTask::update_task_merge_segments_info() {
  int ret = OB_SUCCESS;
  ctx_->task_status_.task_info_.merge_segs_.reset();
  ctx_->task_status_.task_info_.res_segs_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
    ObVecIndexTaskSegInfo seg_info;
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::seg_meta_to_task_seg_info(*merge_segments_.at(i), ctx_->allocator_, seg_info))) {
      LOG_WARN("seg_meta_to_task_seg_info fail", K(ret), K(i), KPC(merge_segments_.at(i)));
    } else if (OB_FAIL(ctx_->task_status_.task_info_.merge_segs_.push_back(seg_info))) {
      LOG_WARN("push_back merge_segments fail", K(ret), K(seg_info));
    }
  }
  if (OB_SUCC(ret)) {
    // Publish selected sources before the long-running merge. Do not overwrite
    // a concurrent cancellation or fail the merge if this observability write fails.
    ObSEArray<int64_t, 1> expected_running;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(expected_running.push_back(ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_RUNNING))) {
      LOG_WARN("fail to prepare merge task info update", K(tmp_ret));
    } else if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_status_and_ret_code_if_match(
                   ctx_, expected_running))) {
      LOG_WARN("fail to publish merge task segment info", K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObVecIdxMergeTask::update_task_result_segments_info()
{
  int ret = OB_SUCCESS;
  const ObVecIdxSnapshotDataHandle &snap = new_adapter_->get_snap_data();
  ctx_->task_status_.task_info_.res_segs_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < snap->meta_.bases_.count(); ++i) {
    ObVecIndexTaskSegInfo info;
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::seg_meta_to_task_seg_info(
        snap->meta_.bases_.at(i), ctx_->allocator_, info))) {
      LOG_WARN("get result segment info failed", K(ret), K(i));
    } else {
      if (info.vector_cnt_ < 0 && snap->meta_.bases_.count() == 1) {
        info.vector_cnt_ = snap->last_res_seg_info_.vector_cnt_;
        info.mem_used_ = snap->last_res_seg_info_.mem_used_;
        info.min_vid_ = snap->last_res_seg_info_.min_vid_;
        info.max_vid_ = snap->last_res_seg_info_.max_vid_;
      }
      if (OB_FAIL(ctx_->task_status_.task_info_.res_segs_.push_back(info))) {
        LOG_WARN("save result segment info failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObVecIdxMergeTask::build_filter_clause(const ObTableSchema &data_table_schema, const ObTableSchema &snapshot_table_schema)
{
  int ret = OB_SUCCESS;
  ObString vid_col_name;
  int64_t pos = 0;
  ObSEArray<uint64_t, 4> all_column_ids;
  bool all_segments_empty = true;
  min_vid_ = INT64_MAX;
  max_vid_ = 0;
  if (merge_segments_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_segments is empty", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
    const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
    int64_t vector_cnt = 0;
    int64_t seg_min_vid = INT64_MAX;
    int64_t seg_max_vid = 0;
    if (OB_ISNULL(seg_meta) || ! seg_meta->segment_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge segment is invalid", K(ret), K(i), KP(seg_meta));
    } else if (OB_FAIL(seg_meta->segment_handle_->get_index_number(vector_cnt))) {
      LOG_WARN("get index number fail", K(ret), K(i), KPC(seg_meta));
    } else if (OB_UNLIKELY(vector_cnt < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector count is invalid", K(ret), K(i), K(vector_cnt), KPC(seg_meta));
    } else if (0 == vector_cnt) {
      LOG_INFO("skip empty merge segment when building filter clause", K(i), KPC(seg_meta));
    } else if (OB_FAIL(seg_meta->segment_handle_->get_vid_bound(seg_min_vid, seg_max_vid))) {
      LOG_WARN("get_vid_bound fail", K(ret), K(i), K(seg_meta));
    } else if (OB_UNLIKELY(seg_min_vid > seg_max_vid)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("non-empty segment vid bound is invalid", K(ret), K(i), K(vector_cnt),
               K(seg_min_vid), K(seg_max_vid), KPC(seg_meta));
    } else {
      all_segments_empty = false;
      min_vid_ = OB_MIN(min_vid_, seg_min_vid);
      max_vid_ = OB_MAX(max_vid_, seg_max_vid);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (all_segments_empty) {
    min_vid_ = 0;
    max_vid_ = 0;
    if (OB_FAIL(filter_sql_str_.assign_fmt("where 1 = 0 "))) {
      LOG_WARN("assign empty merge filter fail", K(ret));
    } else {
      LOG_INFO("build empty merge filter success", K(min_vid_), K(max_vid_), K(filter_sql_str_));
    }
  } else if (min_vid_ > max_vid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vid bound is invalid", K(ret), K(min_vid_), K(max_vid_));
  } else if (OB_FAIL(snapshot_table_schema.get_all_column_ids(all_column_ids))) {
    LOG_WARN("fail to get all column ids", K(ret), K(data_table_schema));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < all_column_ids.count(); i++) {
    const ObColumnSchemaV2 *column_schema;
    if (OB_ISNULL(column_schema = data_table_schema.get_column_schema(all_column_ids.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get column schema", K(ret), K(all_column_ids.at(i)));
    } else if (column_schema->is_vec_hnsw_vid_column()) {
      vid_col_name = column_schema->get_column_name_str();
      LOG_INFO("[VECTOR INDEX MERGE] vid column name", K(vid_col_name));
      break;
    } else if (column_schema->is_hidden_pk_column_id(all_column_ids.at(i))) {
      vid_col_name = column_schema->get_column_name_str();
      LOG_INFO("[VECTOR INDEX MERGE] vid column name", K(vid_col_name));
      break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (all_segments_empty) {
  } else if (merge_from_base_) {
    int64_t real_min_vid = min_vid_;
    min_vid_ = 0;
    if (OB_FAIL(filter_sql_str_.assign_fmt("where %.*s <= %ld ",
      static_cast<int>(vid_col_name.length()), vid_col_name.ptr(), max_vid_))) {
      LOG_WARN("append fmt fail", K(ret), K(vid_col_name));
    } else {
      LOG_INFO("build_filter_clause from base success", K(vid_col_name), K(min_vid_), K(max_vid_), K(real_min_vid), K(filter_sql_str_));
    }
  } else if (OB_FAIL(filter_sql_str_.assign_fmt("where %.*s >= %ld and %.*s <= %ld ",
      static_cast<int>(vid_col_name.length()), vid_col_name.ptr(), min_vid_,
      static_cast<int>(vid_col_name.length()), vid_col_name.ptr(), max_vid_))) {
    LOG_WARN("append fmt fail", K(ret), K(vid_col_name));
  } else {
    LOG_INFO("build_filter_clause success", K(vid_col_name), K(min_vid_), K(max_vid_), K(filter_sql_str_));
  }
  
  if (OB_SUCC(ret) && ! all_segments_empty && ! data_table_schema.is_heap_organized_table()) {
    filter_sql_str_.reset();
    LOG_INFO("table is not heap_organized_table, so reset filter", K(data_table_schema));
  }
  return ret;
}

int ObVecIdxMergeTask::collect_merge_deleted_vids()
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
  merge_deleted_vids_valid_ = false;
  merge_deleted_vids_.reset_bitmap();
  ObVectorIndexRoaringBitMap deleted(&allocator_, tenant_id_);
  roaring::api::roaring64_iterator_t *iter = nullptr;
  if (OB_FAIL(deleted.init(false, true)) || OB_FAIL(merge_deleted_vids_.init(false, true))) {
    LOG_WARN("allocate merge deletion candidates failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_source_meta_.segment_count(); ++i) {
    const auto &meta = merge_source_meta_.flat_seg_meta_at(i);
    const ObVectorIndexRoaringBitMap *bitmap = meta.vbitmap_;
    if (!meta.segment_handle_.is_valid() || !meta.segment_handle_->is_inited()) {
      ret = OB_NOT_INIT;
    } else if (OB_NOT_NULL(bitmap) && OB_NOT_NULL(bitmap->delete_bitmap_)) {
      ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(deleted.delete_bitmap_, bitmap->delete_bitmap_));
      if (OB_FAIL(ret)) {
        LOG_WARN("collect persistent deletion candidates failed", K(ret), K(meta));
      }
    }
  }
  ObVecIdxVBitmapDataHandle bitmaps[2];
  if (OB_SUCC(ret)) {
    const auto &frozen = old_adapter_->get_frozen_data();
    if (frozen.is_valid()) {
      TCRLockGuard guard(frozen->mem_data_rwlock_);
      bitmaps[0] = old_adapter_->get_vbitmap_data();
      if (frozen->has_frozen()) { bitmaps[1] = frozen->vbitmap_; }
    } else {
      bitmaps[0] = old_adapter_->get_vbitmap_data();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    if (bitmaps[i].is_valid()) {
      TCRLockGuard guard(bitmaps[i]->bitmap_rwlock_);
      if (OB_NOT_NULL(bitmaps[i]->bitmap_) && OB_NOT_NULL(bitmaps[i]->bitmap_->delete_bitmap_)) {
        ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(deleted.delete_bitmap_, bitmaps[i]->bitmap_->delete_bitmap_));
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("collect in-memory deletion candidates failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ROARING_TRY_CATCH(iter = roaring64_iterator_create(deleted.delete_bitmap_));
    if (OB_SUCC(ret) && OB_ISNULL(iter)) { ret = OB_ALLOCATE_MEMORY_FAILED; }
  }
  while (OB_SUCC(ret) && roaring64_iterator_has_value(iter)) {
    CHECK_TASK_CANCELLED(ret, ctx_);
    const uint64_t vid = roaring64_iterator_value(iter);
    bool selected = false;
    bool retained = false;
    for (int64_t i = merge_plan_.begin_; OB_SUCC(ret) && !selected && i < merge_plan_.end_; ++i) {
      ret = merge_source_meta_.flat_seg_meta_at(i).segment_handle_->check_id_exist(vid, selected);
    }
    for (int64_t i = 0; OB_SUCC(ret) && selected && !retained
        && i < merge_source_meta_.segment_count(); ++i) {
      if (i < merge_plan_.begin_ || i >= merge_plan_.end_) {
        ret = merge_source_meta_.flat_seg_meta_at(i).segment_handle_->check_id_exist(vid, retained);
      }
    }
    if (OB_SUCC(ret) && selected && !retained) {
      ROARING_TRY_CATCH(roaring64_bitmap_add(merge_deleted_vids_.delete_bitmap_, vid));
    }
    roaring64_iterator_advance(iter);
  }
  if (OB_NOT_NULL(iter)) { roaring64_iterator_free(iter); }
  deleted.reset_bitmap();
  if (OB_SUCC(ret)) {
    merge_deleted_vids_valid_ = true;
    LOG_INFO("collected merge deleted VIDs", K(merge_plan_),
        "candidate_count", roaring64_bitmap_get_cardinality(merge_deleted_vids_.delete_bitmap_));
  } else {
    merge_deleted_vids_.reset_bitmap();
    if (ret == OB_ALLOCATE_MEMORY_FAILED || ret == OB_NOT_INIT || ret == OB_EAGAIN) {
      LOG_INFO("skip optional merge bitmap pruning", K(ret));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObVecIdxMergeTask::filter_reclaimed_vids()
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
  const auto &outputs = new_adapter_->get_snap_data()->meta_;
  for (int64_t i = 0; OB_SUCC(ret) && merge_deleted_vids_valid_ && i < outputs.segment_count(); ++i) {
    const auto &handle = outputs.flat_seg_meta_at(i).segment_handle_;
    if (!handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(handle->ibitmap_) || OB_ISNULL(handle->ibitmap_->insert_bitmap_)) {
      merge_deleted_vids_valid_ = false;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && merge_deleted_vids_valid_ && i < outputs.segment_count(); ++i) {
    CHECK_TASK_CANCELLED(ret, ctx_);
    if (OB_SUCC(ret)) {
      ROARING_TRY_CATCH(roaring64_bitmap_andnot_inplace(merge_deleted_vids_.delete_bitmap_,
          outputs.flat_seg_meta_at(i).segment_handle_->ibitmap_->insert_bitmap_));
    }
  }
  if (OB_SUCC(ret) && merge_deleted_vids_valid_) {
    LOG_INFO("filtered reclaimed merge VIDs",
        "reclaimed_count", roaring64_bitmap_get_cardinality(merge_deleted_vids_.delete_bitmap_));
  }
  return ret;
}

int ObVecIdxMergeTask::merge_bitmap(ObPluginVectorIndexAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  ObVectorIndexSegmentBuilder* segment_builder = nullptr;
  ObVectorIndexRoaringBitMap *ibitmap = nullptr;
  ObVectorIndexRoaringBitMap *vbitmap = nullptr;
  bool need_bitmap = merge_plan_.strategy_ != ObVecMergePlan::FULL_MERGE;
  if (OB_ISNULL(adaptor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor is null", K(ret));
  } else if (merge_segments_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge_segments is empty", K(ret));
  } else if (OB_FAIL(adaptor->init_snap_data_for_build(need_bitmap))) {
    LOG_WARN("init segment builder fail", K(ret), KPC(adaptor));
  } else if (OB_ISNULL(segment_builder = adaptor->get_snap_data()->builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("segment_builder is null", K(ret));
  } else if (min_vid_ > max_vid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vid bound is invalid", K(ret), K(min_vid_), K(max_vid_));
  } else if (OB_FALSE_IT(segment_builder->vid_bound_.set_vid_bound(max_vid_, min_vid_))) {
  } else if (OB_FALSE_IT(segment_builder->need_vid_check_ = true)) {
  } else {
    segment_builder->merge_source_seg_handles_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
      const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
      if (OB_ISNULL(seg_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("merge source seg meta is null", K(ret), K(i));
      } else if (OB_FAIL(segment_builder->merge_source_seg_handles_.push_back(seg_meta->segment_handle_))) {
        LOG_WARN("push merge source seg handle fail", K(ret), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(segment_builder->seg_type_ = merge_plan_.output_type_)) {
  } else if (!need_bitmap) {
    LOG_INFO("full BASE merge does not need output bitmaps", K(merge_plan_));
  } else if (OB_ISNULL(ibitmap = segment_builder->ibitmap_) || OB_ISNULL(vbitmap = segment_builder->vbitmap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ibitmap or vbitmap is null", K(ret), KP(ibitmap), KP(vbitmap));
  } else if (OB_FAIL(ibitmap->init(true, false))) {  
    LOG_WARN("init ibitmap fail", K(ret));
  } else if (OB_FAIL(vbitmap->init(true, true))) {  
    LOG_WARN("init vbitmap fail", K(ret));
  } else {
    int64_t merge_vbitmap_insert = 0;
    int64_t merge_vbitmap_delete = 0;
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
      const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
      const ObVectorIndexRoaringBitMap *bitmap = seg_meta->vbitmap_;
      if (!seg_meta->segment_handle_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("segment handle is invalid", K(ret), K(i), KPC(seg_meta));
      } else if (OB_SUCC(ret) && OB_NOT_NULL(bitmap)) {
        if (OB_NOT_NULL(bitmap->insert_bitmap_)) {
          ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(vbitmap->insert_bitmap_, bitmap->insert_bitmap_));
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(bitmap->delete_bitmap_)) {
          ROARING_TRY_CATCH(roaring64_bitmap_or_inplace(vbitmap->delete_bitmap_, bitmap->delete_bitmap_));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(vbitmap->insert_bitmap_)) {
      if (OB_NOT_NULL(vbitmap->delete_bitmap_)) {
        ROARING_TRY_CATCH(roaring64_bitmap_andnot_inplace(vbitmap->insert_bitmap_, vbitmap->delete_bitmap_));
      }
      if (OB_SUCC(ret)) {
        merge_vbitmap_insert = roaring64_bitmap_get_cardinality(vbitmap->insert_bitmap_);
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(vbitmap->delete_bitmap_)) {
      merge_vbitmap_delete = roaring64_bitmap_get_cardinality(vbitmap->delete_bitmap_);
    }
    LOG_INFO("init segment builder sucess", K(ret),KPC(segment_builder), K(merge_vbitmap_insert), K(merge_vbitmap_delete));
  }
  return ret;
}

int ObVecIdxMergeTask::process_merge()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexAdapterGuard adpt_guard;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  LOG_TRACE("[VECTOR INDEX MERGE] start process_merge", K(ret), K(ctx_->task_status_), K(ls_id_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObVecIdxMergeTask is not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret), K(tenant_id_), K(ls_id_));
  } else if (OB_INVALID_ID == ctx_->task_status_.inc_tablet_id_.id()
             && OB_FAIL(vec_idx_mgr_->get_inc_tablet_id_by_vbitmap(
                 ctx_->task_status_.table_id_,
                 ObTabletID(ctx_->task_status_.tablet_id_),
                 ctx_->task_status_.inc_tablet_id_))) {
    // __all_vector_index_task only stores index_id (vbitmap) table_id/tablet_id; inc_tablet_id_
    // is kept in memory by load_task but lost after resume (incl. dbms_vector.compact_index).
    // Look up delta-buffer tablet on vec_adaptor_map before get_adapter_inst_guard.
    ret = OB_EAGAIN;
    LOG_INFO("can not get inc tablet id for merge task, need wait", K(ret), KPC(ctx_));
  } else if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard(ctx_->task_status_.inc_tablet_id_, adpt_guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("can not get adapter, need wait", K(ret), KPC(ctx_));
    } else {
      LOG_WARN("fail to get adapter instance", KR(ret), KPC(ctx_));
    }
  } else if (OB_ISNULL(adpt_guard.get_adatper())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get vector index adapter", KR(ret), KPC(ctx_));
  } else if (! adpt_guard.get_adatper()->is_complete()) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter not complete, need wait", K(ret), KP(adpt_guard.get_adatper()));
  } else if (!adpt_guard.get_adatper()->is_ready_complete()) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter is not complete, no need merge", K(ret), KPC(adpt_guard.get_adatper()));
  } else if (!check_snapshot_table_available(*adpt_guard.get_adatper())) {
    ret = OB_EAGAIN; // will retry
    LOG_INFO("skip to do async task due to snapshot table not available", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(check_and_merge(adpt_guard))) {
    LOG_WARN("[VECTOR INDEX MERGE] fail to check and merge", K(ret), KP(adpt_guard.get_adatper()));
  } else {
    LOG_INFO("[VECTOR INDEX MERGE] process merge success", K(ret), KP(adpt_guard.get_adatper()));
  }

  // Segment allocators belong to the source adaptor protected by adpt_guard.
  merge_segments_.reuse();
  merge_source_meta_.release();
  merge_policy_input_.segments_.reuse();
  return ret;
}

int ObVecIdxMergeTask::check_active_segment_param(
    ObPluginVectorIndexAdaptor &adaptor,
    const ObVectorIndexParam &target_param,
    bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  const ObVecIdxActiveDataHandle &incr_data = adaptor.get_incr_data();
  if (!incr_data.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("active segment is invalid", K(ret), KPC(&adaptor));
  } else {
    TCRLockGuard lock_guard(incr_data->mem_data_rwlock_);
    if (!incr_data->is_inited()) {
      ret = OB_EAGAIN;
      LOG_INFO("active segment is not initialized, wait for alter param freeze",
               KR(ret), KPC(&adaptor));
    } else if (!incr_data->segment_handle_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("active segment is invalid", K(ret), KPC(&adaptor));
    } else if (OB_FAIL(ObVectorIndexUtil::check_build_params_equal(
        incr_data->segment_handle_->get_index_param(), target_param, equal))) {
      LOG_WARN("failed to compare active segment param", K(ret), KPC(&adaptor));
    }
  }
  return ret;
}

int ObVecIdxMergeTask::prepare_alter_param_merge(
    ObPluginVectorIndexAdaptor &current_adaptor,
    ObVectorIndexParam &target_param,
    bool &need_alter_param_merge)
{
  int ret = OB_SUCCESS;
  const ObString target_index_param =
      ctx_->task_status_.task_info_.alter_param_info_.index_params_.str();
  bool active_equal = false;
  need_alter_param_merge = false;
  if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
      target_index_param, ObVectorIndexType::VIT_HNSW_INDEX, target_param))) {
    LOG_WARN("failed to parse target index param", K(ret), K(target_index_param));
  } else if (OB_FAIL(check_active_segment_param(
          current_adaptor, target_param, active_equal))) {
    LOG_WARN("failed to check active segment param", K(ret), KPC(ctx_));
  } else if (!active_equal) {
    ret = OB_EAGAIN;
    LOG_INFO("active segment has not switched to target params", K(ret), KPC(ctx_));
  } else if (current_adaptor.has_frozen()) {
    ret = OB_EAGAIN;
    LOG_INFO("frozen segment has not been persisted by alter param freeze",
             K(ret), KPC(ctx_));
  } else if (OB_FAIL(current_adaptor.check_need_alter_param_merge(
          target_param, need_alter_param_merge))) {
    LOG_WARN("failed to check alter param merge", K(ret), KPC(&current_adaptor));
  } else if (!need_alter_param_merge) {
    LOG_INFO("alter param merge is already complete", KPC(ctx_));
  }
  return ret;
}

int ObVecIdxMergeTask::check_and_merge(ObPluginVectorIndexAdapterGuard &adpt_guard)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexAdapterGuard new_adpt_guard;
  ObPluginVectorIndexAdaptor *new_adapter = nullptr;
  const bool is_alter_param = is_alter_param_child();
  const ObString target_index_param = is_alter_param
      ? ctx_->task_status_.task_info_.alter_param_info_.index_params_.str()
      : ObString();
  ObVectorIndexParam target_param;
  bool need_alter_param_merge = false;
  bool need_merge = false;
  SCN max_commit_scn;

  // prepare adaptor
  if (is_alter_param && OB_FAIL(prepare_alter_param_merge(
      *adpt_guard.get_adatper(), target_param, need_alter_param_merge))) {
    LOG_WARN("failed to prepare alter param merge", K(ret), KPC(ctx_));
  } else if (is_alter_param && !need_alter_param_merge) {
  } else if (OB_FALSE_IT(set_old_adapter(adpt_guard.get_adatper()))) {
  } else if (OB_FAIL(create_new_adapter(vector_index_service, adpt_guard, new_adapter))) {
    LOG_WARN("fail to get or create new adapter", K(ret), K(ctx_));
  } else if (OB_FAIL(new_adpt_guard.set_adapter(new_adapter))) { // inc_ref + 1
    LOG_WARN("fail to set new adpater to guard", K(ret));
  } else if (OB_FALSE_IT(new_adapter->set_created_by_segment_merge(true))) {
  } else if (is_alter_param && OB_FAIL(new_adapter->update_index_param(
      target_param, target_index_param))) {
    LOG_WARN("failed to assign target build param", K(ret), K(target_index_param));
  } else if (!ctx_->task_status_.target_scn_.is_valid()
             && OB_FAIL(get_current_scn(ctx_->task_status_.target_scn_))) {
    LOG_WARN("fail to get scn", KR(ret));
  } else if (OB_FAIL(check_and_wait_write(max_commit_scn))) {
    LOG_WARN("fail to check and wait write", KR(ret));
  }
  CHECK_TASK_CANCELLED(ret, ctx_);
  if (OB_FAIL(ret)) {
  } else if (is_alter_param && !need_alter_param_merge) {
  } else {
    const ObVecIndexAsyncTaskTriggerType trigger = static_cast<ObVecIndexAsyncTaskTriggerType>(
        ctx_->task_status_.trigger_type_);
    const bool is_full_merge = trigger == OB_VEC_TRIGGER_ALTER_INDEX_PARAM;
    // Retain one source snapshot and plan for both full and ordinary merges.
    int tmp_ret = OB_SUCCESS;
    if (!is_full_merge && OB_TMP_FAIL(old_adapter_->refresh_segment_delete_counts(ctx_))) {
      LOG_WARN("refresh segment delete counts failed", K(tmp_ret));
    }
    CHECK_TASK_CANCELLED(ret, ctx_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(old_adapter_->prepare_merge_policy_input(
        merge_source_meta_, merge_policy_input_, merge_config_, is_full_merge))) {
      LOG_WARN("prepare merge policy input failed", K(ret));
    } else if (!is_alter_param && OB_FAIL(old_adapter_->prepare_async_merge_input(ctx_->get_ls(), merge_policy_input_))) {
      LOG_WARN("prepare async merge visibility failed", K(ret));
    } else if (OB_FAIL(ObVectorIndexMergePolicy(merge_config_).find_merge_segments(merge_policy_input_,
        trigger, merge_plan_))) {
      if (ret == OB_NO_NEED_MERGE) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("check merge plan failed", K(ret));
      }
    } else {
      need_merge = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!need_merge) {
    LOG_INFO("merge recheck has no plan, finish task", KPC(ctx_));
  } else if (OB_FAIL(prepare_merge_scn(max_commit_scn))) {
    LOG_WARN("failed to prepare merge SCN", K(ret), K(max_commit_scn), KPC(ctx_));
  } else if (OB_FAIL(execute_merge())) {
    LOG_WARN("merge incr fail", K(ret), KPC(ctx_));
  } else {
    CHECK_TASK_CANCELLED(ret, ctx_);
    if (OB_FAIL(ret)) {
      LOG_INFO("merge task canceled before execute exchange", K(ret), KPC(ctx_));
    } else if (OB_UNLIKELY(
                   OB_SUCCESS !=
                   (ret = EVENT_CALL(ERRSIM_VEC_MERGE_BEFORE_EXCHANGE,
                                     new_adapter->get_data_table_id())))) {
      LOG_WARN("injected error before vector index merge exchange", KR(ret),
               K(new_adapter->get_data_table_id()));
    } else if (OB_FAIL(execute_exchange())) {
      LOG_WARN("execute exchange fail", K(ret), KPC(ctx_));
    } else {
      ObPluginVectorIndexAdapterGuard current;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(vec_idx_mgr_->get_adapter_inst_guard(ctx_->task_status_.inc_tablet_id_, current))) {
        LOG_WARN("get current adapter failed", K(tmp_ret));
      } else if (OB_TMP_FAIL(current.get_adatper()->refresh_segment_delete_counts(ctx_))) {
        LOG_WARN("refresh segment delete counts failed", K(tmp_ret));
      }
    }
  }

  // clear memory_hold_adaptor_ before releasing tmp_info to avoid use-after-free
  if (OB_NOT_NULL(ctx_)) {
    ctx_->reset_adaptor();
  }

  // clean tmp info
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(vector_index_service->release_vector_index_tmp_info(ctx_->task_status_.task_id_))) {
      LOG_WARN("fail to release vector index tmp info", K(tmp_ret));
    }
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }
  // release pre alloc size
  if (pre_alloc_size_ > 0) {
    ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
    ObTenantVectorAllocator& vector_allocator = shared_mem_mgr->vector_allocator();
    vector_allocator.pre_free(pre_alloc_size_);
    pre_alloc_size_ = 0;
  }

  LOG_TRACE("[VECTOR INDEX MERGE] end process_merge", K(ret), K(ctx_->task_status_));
  return ret;
}

int ObVecIdxMergeTask::execute_merge()
{
  int ret = OB_SUCCESS;
  const int64_t data_table_id = new_adapter_->get_data_table_id();
  const int64_t snapshot_table_id = new_adapter_->get_snapshot_table_id();
  uint64_t data_schema_version = 0;
  bool is_user_hidden_table = false;
  bool is_need_padding = false;
  ObString partition_names;
  if (OB_FAIL(prepare_merge_segment())) {
    LOG_WARN("prepare merge segment fail", K(ret));
  } else if (OB_FAIL(update_task_merge_segments_info())) {
    LOG_WARN("prepare task segment info fail", K(ret));
  } else if (data_table_id == OB_INVALID_ID || snapshot_table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_table_id), K(snapshot_table_id));
  } else {
    ObSchemaGetterGuard schema_guard;
    ObSEArray<uint64_t, 4> all_column_ids;
    ObSEArray<uint64_t, 4> dml_column_ids;
    ObSEArray<uint64_t, 1> upd_column_ids;
    ObSEArray<uint64_t, 4> extra_column_idxs;
    common::ObCollationType cs_type = CS_TYPE_INVALID;
    const ObTableSchema *data_schema = nullptr;
    const ObTableSchema *snapshot_schema = nullptr;
    const ObTabletID dest_tablet_id = new_adapter_->get_snap_tablet_id();
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, snapshot_table_id, snapshot_schema))) {
      LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(snapshot_table_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, data_table_id, data_schema))) {
      LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id_), K(data_table_id));
    } else if (OB_ISNULL(snapshot_schema) || OB_ISNULL(data_schema) ||
        snapshot_schema->is_in_recyclebin() || data_schema->is_in_recyclebin()) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table schema not exist", K(ret), K(data_table_id), K(snapshot_table_id), KP(snapshot_schema), KP(data_schema));
    } else if (OB_FAIL(get_snap_index_column_info(*data_schema, *snapshot_schema, all_column_ids, dml_column_ids, extra_column_idxs, cs_type))) {
      LOG_WARN("fail to get snap index column info", K(ret));
    } else if (OB_FAIL(build_filter_clause(*data_schema, *snapshot_schema))) {
      LOG_WARN("fail to build_filter_clause", K(ret), KPC(ctx_));
    } else if (OB_FAIL(data_schema->is_need_padding_for_generated_column(is_need_padding))) {
      LOG_WARN("fail to check need padding", K(ret));
    } else {
      data_schema_version = data_schema->get_schema_version();
      is_user_hidden_table = data_schema->is_user_hidden_table();
      if (OB_FAIL(get_partition_name(*data_schema, data_table_id, snapshot_table_id, dest_tablet_id, allocator_, partition_names))) {
        LOG_WARN("fail to get partition name", K(ret), K(ctx_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(collect_merge_deleted_vids())) {
      LOG_WARN("collect merge deleted VIDs failed", K(ret));
    } else if (OB_FAIL(merge_bitmap(new_adapter_))) {
      LOG_WARN("merge bitmap fail", K(ret));
    } else if (merge_plan_.output_type_ == ObVectorIndexSegmentType::BASE
        && (merge_plan_.strategy_ == ObVecMergePlan::MANUAL_MERGE
            || merge_plan_.strategy_ == ObVecMergePlan::FULL_MERGE)
        && OB_FAIL(new_adapter_->enable_merge_multi_segment(merge_config_.max_vectors_))) {
      LOG_WARN("enable merge multi segment failed", K(ret));
    } else if (OB_FAIL(execute_insert(data_schema_version, is_user_hidden_table, is_need_padding, partition_names))) {
      LOG_WARN("fail to execute insert", K(ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObVecIdxMergeTask::serialize_and_insert_snap_index(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObVecIdxSnapshotDataWriteCtx write_ctx;
  ObAccessService *oas = MTL(ObAccessService *);
  int64_t snapshot_version = 0;
  if (OB_ISNULL(ctx_) || OB_ISNULL(tx_desc) || OB_ISNULL(new_adapter_) || OB_ISNULL(oas)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(ctx_), KP(tx_desc), KP(new_adapter_), KP(oas));
  } else if (!new_adapter_->is_snap_inited()) {
    LOG_INFO("[VECTOR INDEX MERGE] snap index not inited, skip serialize and insert", KPC(new_adapter_));
  } else {
    snapshot_version = ctx_->task_status_.target_scn_.get_val_for_inner_table_field();
    write_ctx.ls_id_ = ls_id_;
    write_ctx.data_tablet_id_ = new_adapter_->get_data_tablet_id();
    ObHNSWSerializeCallback::CbParam param;
    param.vctx_ = &write_ctx;
    param.allocator_ = &allocator_;
    param.tmp_allocator_ = &allocator_;
    param.lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
    param.timeout_ = timeout_us;
    param.snapshot_ = &snapshot;
    param.tx_desc_ = tx_desc;
    param.tablet_id_ = new_adapter_->get_snap_tablet_id();
    param.snapshot_version_ = snapshot_version;
    param.is_vec_tablet_rebuild_ = true;
    param.task_ctx_ = ctx_;
    ObVectorIndexAlgorithmType index_type = VIAT_MAX;
    ObVecIdxSnapTableSegMergeOp snap_table_handler(new_adapter_->get_tenant_id());
    if (OB_FAIL(snap_table_handler.init(ls_id_, new_adapter_->get_data_table_id(), new_adapter_->get_snapshot_table_id(), new_adapter_->get_snap_tablet_id()))) {
      LOG_WARN("init snap table handler fail", K(ret));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::get_lob_tablet_id(ls_id_, param.tablet_id_, write_ctx.lob_meta_tablet_id_, write_ctx.lob_piece_tablet_id_))) {
      LOG_WARN("get_lob_tablet_id fail", K(ret), K(ls_id_), K(param.tablet_id_));
    } else if (OB_FAIL(snap_table_handler.get_lob_store_column_idx(write_ctx.lob_col_idx_))) {
      LOG_WARN("get lob store column idx fail", K(ret));
    } else if (OB_FAIL(new_adapter_->serialize_snapshot(param, false/*skip_for_merge*/))) {
      LOG_WARN("serialize snapshot fail", K(ret));
    } else if (new_adapter_->get_snap_data()->meta_.bases_.empty()
        || (!new_adapter_->get_snap_data()->enable_multi_segment_
            && new_adapter_->get_snap_data()->meta_.bases_.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid serialized merge result count", K(ret),
          "multi_segment", new_adapter_->get_snap_data()->enable_multi_segment_,
          "base_count", new_adapter_->get_snap_data()->meta_.bases_.count());
    } else if (OB_FALSE_IT(new_adapter_->get_snap_data()->meta_.bases_.at(0).seg_type_ =
        merge_plan_.output_type_)) {
    } else if (OB_FALSE_IT(index_type = new_adapter_->get_snap_index_type())) {
    } else if (OB_FAIL(snap_table_handler.insert_segment_data(
        write_ctx.vals_, tx_desc, snapshot, snapshot_version, index_type, timeout_us, ctx_))) {
      LOG_WARN("insert_segment_data fail", K(ret));
    } else {
      LOG_INFO("insert_segment_data success", K(ret), KP(new_adapter_), K(index_type),
        "count", write_ctx.vals_.count(), "snapshot_version", snapshot_version, "tx_id", tx_desc->get_tx_id());
    }
  }
  return ret;
}

int ObVecIdxMergeTask::execute_exchange()
{
  int ret = OB_SUCCESS;
  transaction::ObTxDesc *tx_desc = nullptr;
  transaction::ObTxReadSnapshot snapshot;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;

  if (OB_FAIL(ObVectorIndexTableHelper::start_trans(ls_id_, false/*is_for_read*/, timeout_us, new_adapter_->get_data_tablet_id(), tx_desc))) {
    LOG_WARN("fail to get tx_desc", K(ret));
  } else if (OB_ISNULL(tx_desc) || OB_ISNULL(txs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tx desc or ob access service, get nullptr", K(ret));
  } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_us, snapshot))) { // get new snapshot version to exchange table 5 rows
    LOG_WARN("fail to get snapshot", K(ret));
  } else {
    ObVecIdxSnapshotDataHandle& new_snap = new_adapter_->get_snap_data();
    LOG_INFO("new snap info", K(new_snap));
    CHECK_TASK_CANCELLED(ret, ctx_);
    if (OB_FAIL(ret)) {
      LOG_INFO("merge task canceled before serialize snapshot", K(ret), KPC(ctx_));
    } else if (OB_FAIL(serialize_and_insert_snap_index(tx_desc, snapshot, timeout_us))) {
      LOG_WARN("fail to serialize and insert snap index", K(ret), K(new_snap));
    } else if ((!new_snap->enable_multi_segment_ && new_snap->meta_.bases_.count() > 1)
        || new_snap->meta_.incrs_.count() != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new snap is incorrect", K(ret), K(new_snap));
    } else if (merge_deleted_vids_valid_ && OB_FAIL(filter_reclaimed_vids())) {
      LOG_WARN("filter reclaimed VIDs failed", K(ret));
    } else if (OB_FAIL(clean_snap_index_rows(tx_desc, snapshot, timeout_us))) {
      LOG_WARN("fail to delete inc index rows", K(ret), K(ctx_));
    }
    // Keep failures from the mandatory exchange path separate from optional bridge construction.
    const int exchange_ret = ret;
    int bridge_ret = OB_SUCCESS;
    // Bridge is written later by update_meta.
    ObArenaAllocator snap_meta_allocator("VIMrgBridge", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObVectorIndexSegmentMeta bridge_meta;
    ObVectorIndexSegmentHandle bridge_segment;
    bool has_bridge = false;
    if (OB_FAIL(ret)) {
    } else if (is_alter_param_child() && merge_from_base_
        && new_snap->meta_.bases_.count() == 1
        && old_adapter_->get_snap_data()->meta_.bases_.count() == 1
        && ObVectorIndexSegmentMeta::is_snap_meta_supported(tenant_id_)) {
      bool has_base_seg = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
        const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
        if (OB_NOT_NULL(seg_meta) && seg_meta->seg_type_ == ObVectorIndexSegmentType::BASE) {
          has_base_seg = true;
          break;
        }
      }
      if (!has_base_seg) {
        LOG_INFO("[VECTOR INDEX MERGE] initial base built from incr, skip snap_meta bridge",
            K(merge_segments_.count()), K(merge_from_base_));
      } else if (new_snap->meta_.bases_.count() <= 0) {
        LOG_INFO("[VECTOR INDEX MERGE] empty new snap meta, skip snap_meta bridge",
            K(new_snap), "merge_segment_count", merge_segments_.count());
      } else {
        // Keep bridge bitmap allocation and later free on the VIBitmap path.
        lib::ObMallocHookAttrGuard bridge_vib_guard(lib::ObMemAttr(tenant_id_, "VIBitmap"));
        bool has_bridge_bitmap = false;
        if (OB_FAIL(ObVectorIndexSegment::create_bitmap_only(
            bridge_segment, tenant_id_, snap_meta_allocator))) {
          LOG_WARN("create snap_meta bridge segment fail", K(ret));
        } else if (OB_ISNULL(bridge_meta.vbitmap_ = OB_NEWx(
            ObVectorIndexRoaringBitMap, &snap_meta_allocator, &snap_meta_allocator, tenant_id_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc snap_meta bridge vbitmap fail", K(ret));
        } else if (OB_FAIL(bridge_meta.vbitmap_->init(true/*insert*/, true/*delete*/))) {
          LOG_WARN("init snap_meta bridge vbitmap fail", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
          const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
          if (OB_ISNULL(seg_meta)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null seg_meta in merge_segments", K(ret), K(i));
          } else if (seg_meta->seg_type_ == ObVectorIndexSegmentType::BASE) {
            if (bridge_meta.seg_type_ != ObVectorIndexSegmentType::SNAP_META_BRIDGE
                || seg_meta->scn_ > bridge_meta.scn_) {
              bridge_meta.seg_type_ = ObVectorIndexSegmentType::SNAP_META_BRIDGE;
              bridge_meta.index_type_ = ObVectorIndexAlgorithmType::VIAT_MAX;
              bridge_meta.scn_ = seg_meta->scn_;
              bridge_meta.has_segment_meta_row_ = true;
              bridge_meta.blocks_cnt_ = 1;
            }
          } else if (seg_meta->segment_handle_.is_valid()) {
            const ObVectorIndexRoaringBitMap *bitmap = seg_meta->vbitmap_;
            if (OB_NOT_NULL(bitmap)) {
              has_bridge_bitmap = true;
              if (OB_NOT_NULL(bitmap->insert_bitmap_)) {
                ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_or_inplace(
                    bridge_meta.vbitmap_->insert_bitmap_, bitmap->insert_bitmap_));
              }
              if (OB_SUCC(ret) && OB_NOT_NULL(bitmap->delete_bitmap_)) {
                ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_or_inplace(
                    bridge_meta.vbitmap_->delete_bitmap_, bitmap->delete_bitmap_));
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (bridge_meta.seg_type_ != ObVectorIndexSegmentType::SNAP_META_BRIDGE
            || bridge_meta.scn_ <= 0
            || !bridge_segment.is_valid()
            || OB_ISNULL(bridge_meta.vbitmap_)
            || !has_bridge_bitmap) {
          // Bridge is optional; merge can still advance without it.
          LOG_INFO("[VECTOR INDEX MERGE] bridge not buildable, skip snap_meta bridge",
              K(bridge_meta), K(has_bridge_bitmap));
        } else {
          has_bridge = true;
          bridge_meta.bitmap_scn_ = new_snap->meta_.scn();
          LOG_INFO("[VECTOR INDEX MERGE] built snap_meta bridge for child row",
              K(bridge_meta), K(bridge_segment));
        }
      }
    }
    if (OB_SUCCESS == exchange_ret) {
      bridge_ret = ret;
      ret = exchange_ret;
    }
    if (OB_SUCCESS != bridge_ret) {
      // The bridge is an optimization. Only bridge construction errors are downgraded;
      // mandatory segment serialization/cleanup errors must reach end_trans and roll back.
      LOG_WARN("build snap_meta bridge failed, continue merge without bridge", K(bridge_ret));
      has_bridge = false;
      bridge_segment.reset();
      bridge_meta.release();
    }
    if (OB_SUCC(ret)) {
      CHECK_TASK_CANCELLED(ret, ctx_);
    }
    if (OB_FAIL(ret)) {
      LOG_INFO("merge task canceled or failed before update meta", K(ret), KPC(ctx_));
    } else if (OB_FAIL(update_meta(new_snap, tx_desc, snapshot, timeout_us,
        has_bridge ? &bridge_meta : nullptr,
        has_bridge ? &bridge_segment : nullptr))) {
      LOG_WARN("update meta fail", K(ret), K(new_snap));
    }
  }
  int tmp_ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_VECTOR_INDEX_MERGE_COMMIT);
  CHECK_TASK_CANCELLED(ret, ctx_);
  if (nullptr != tx_desc && OB_SUCCESS != (tmp_ret = ObVectorIndexTableHelper::end_trans(tx_desc, OB_SUCCESS != ret, timeout_us))) {
    ret = tmp_ret; 
    LOG_WARN("fail to end trans", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(refresh_adaptor())) {
    LOG_WARN("refresh adaptor fail", K(ret));
  }
  return ret;
}

int ObVecIdxMergeTask::execute_insert(
    const uint64_t data_schema_version,
    const bool is_user_hidden_table,
    const bool is_need_padding,
    const ObString &partition_names)
{
  int ret = OB_SUCCESS;
  int64_t parallelism = 0;
  int64_t data_table_id;
  int64_t dest_table_id;
  int64_t alter_param_parallelism = 0;

  if (OB_ISNULL(ctx_) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(ctx_), K(new_adapter_));
  } else if (OB_FALSE_IT(data_table_id = new_adapter_->get_data_table_id())) {
  } else if (OB_FALSE_IT(dest_table_id = new_adapter_->get_snapshot_table_id())) {
  } else if (OB_FALSE_IT(alter_param_parallelism = ctx_->task_status_.task_info_.alter_param_info_.idx_parallel_build_)) {
  } else if (is_alter_param_child() && alter_param_parallelism > 0 &&
             OB_FALSE_IT(parallelism = alter_param_parallelism)) {
  } else if (parallelism <= 0 && OB_FAIL(get_task_paralellism(parallelism))) {
    LOG_WARN("fail to get task parallelism", K(ret), K(ctx_));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::set_inner_sql_snapshot_version(ctx_->task_status_.task_id_, ctx_->task_status_.target_scn_.get_val_for_sql()))) {
    LOG_WARN("fail to set inner sql snapshot", K(ret), K(ctx_));
  } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::set_inner_sql_schema_version(ctx_->task_status_.task_id_, data_schema_version))) {
    LOG_WARN("fail to set inner sql snapshot", K(ret), K(ctx_));
  } else {
    CHECK_TASK_CANCELLED(ret, ctx_);
    DEBUG_SYNC(BEFORE_VECTOR_INDEX_MERGE_INNER_SQL);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(execute_inner_sql(data_schema_version, is_user_hidden_table, is_need_padding,
      data_table_id, dest_table_id, ctx_->task_status_.task_id_,
      parallelism, partition_names, ctx_->task_status_.target_scn_))) {
    LOG_WARN("fail to execute inner sql", K(ret), K(partition_names), K(ctx_));
  }
  return ret;
}

int ObVecIdxMergeTask::clean_snap_index_rows(
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot,
    const uint64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  ObTableScanIterator *table_scan_iter = nullptr;
  storage::ObTableScanParam snap_scan_param;
  schema::ObTableParam snap_table_param(allocator_);
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObVecIdxSnapTableSegMergeOp snap_table_handler(new_adapter_->get_tenant_id());
  int64_t loop_cnt = 0;
  if (OB_ISNULL(ctx_) || OB_ISNULL(tx_desc) || OB_ISNULL(new_adapter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(tx_desc), KP(ctx_));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
      new_adapter_,
      snapshot.version(),
      INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
      allocator_,
      allocator_,
      snap_scan_param,
      snap_table_param,
      snap_data_iter))) {
    LOG_WARN("fail to read snapshot table local tablet.", K(ret));
  } else if (OB_FAIL(snap_table_handler.init(ls_id_, new_adapter_->get_data_table_id(),
      new_adapter_->get_snapshot_table_id(), new_adapter_->get_snap_tablet_id()))) {
    LOG_WARN("init snap table handler fail", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_segments_.count(); ++i) {
      const ObVectorIndexSegmentMeta *seg_meta = merge_segments_.at(i);
      LOG_INFO("delete segment info", K(i), KPC(seg_meta));
      if (OB_FAIL(rescan(*seg_meta, snap_scan_param, timeout_us, table_scan_iter))) {
        LOG_WARN("rescan fail", K(ret), K(i), KPC(seg_meta));
      } else if (OB_FAIL(snap_table_handler.delete_segment_data(
          new_adapter_, table_scan_iter, tx_desc, snapshot, timeout_us, ctx_))) {
        LOG_WARN("delete segment row fail", K(ret), K(i), KPC(seg_meta));
      }
      CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
    }
  }
  if (OB_NOT_NULL(oas)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snap_data_iter)) {
      tmp_ret = oas->revert_scan_iter(snap_data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snap_data_iter = nullptr;
  }
  return ret;
}

int ObVecIdxMergeTask::rescan(
    const ObVectorIndexSegmentMeta& seg_meta, storage::ObTableScanParam &scan_param,
    const uint64_t timeout, ObTableScanIterator *table_scan_iter)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  ObNewRange range;
  scan_param.key_ranges_.reuse();
  // update timeout
  scan_param.timeout_ = timeout;
  scan_param.for_update_wait_timeout_ = scan_param.timeout_;
  int64_t rowkey_cnt = scan_param.table_param_->get_read_info().get_schema_rowkey_count();
  if (rowkey_objs_.count() < rowkey_cnt && OB_FAIL(rowkey_objs_.prepare_allocate(rowkey_cnt*2))) {
    LOG_WARN("prepare rowkey array fail", K(ret), K(rowkey_cnt));
  } else if (OB_FAIL(build_rowkey_range(seg_meta, range))) {
    LOG_WARN("build_range fail", K(ret));
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
    LOG_WARN("push key range fail", K(ret), K(scan_param), K(range));
  } else if (OB_FAIL(oas->reuse_scan_iter(false/*tablet id same*/, table_scan_iter))) {
    LOG_WARN("reuse scan iter fail", K(ret));
  } else if (OB_FAIL(oas->table_rescan(scan_param, table_scan_iter))) {
    LOG_WARN("do table rescan fail", K(ret));
  }
  return ret;
}

int ObVecIdxMergeTask::build_rowkey_range(const ObVectorIndexSegmentMeta& seg_meta, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = rowkey_objs_.count() / 2;
  for (int64_t i = 0; i < rowkey_cnt; ++i) {
    if (vector_key_col_idx_ == i) {
      rowkey_objs_[i].reset();
      rowkey_objs_[i].set_varchar(seg_meta.start_key_);
      rowkey_objs_[i].set_collation_type(key_col_cs_type_);
    } else {
      rowkey_objs_[i] = ObObj::make_min_obj();
    }
  }
  ObRowkey min_row_key(rowkey_objs_.get_data(), rowkey_cnt);
  for (int64_t i = rowkey_cnt; i < rowkey_objs_.count(); ++i) {
    if (i - rowkey_cnt == vector_key_col_idx_) {
      rowkey_objs_[i].reset();
      rowkey_objs_[i].set_varchar(seg_meta.end_key_);
      rowkey_objs_[i].set_collation_type(key_col_cs_type_);
    } else {
      rowkey_objs_[i] = ObObj::make_max_obj();
    }
  }
  ObRowkey max_row_key(rowkey_objs_.get_data() + rowkey_cnt, rowkey_cnt);

  range.table_id_ = 0; // TODO make sure this is correct
  range.start_key_ = min_row_key;
  range.end_key_ = max_row_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  return ret;
}

int ObVecIdxMergeTask::update_meta(
    const ObVecIdxSnapshotDataHandle& new_snap, transaction::ObTxDesc *tx_desc,
    transaction::ObTxReadSnapshot &snapshot, const uint64_t timeout_us,
    const ObVectorIndexSegmentMeta *bridge_meta,
    const ObVectorIndexSegmentHandle *bridge_segment)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObVecIdxSnapTableSegMergeOp snap_table_handler(new_adapter_->get_tenant_id());
  if (OB_FAIL(snap_table_handler.init(ls_id_, new_adapter_->get_data_table_id(), new_adapter_->get_snapshot_table_id(), new_adapter_->get_snap_tablet_id()))) {
    LOG_WARN("init snap table handler fail", K(ret));
  } else if (OB_FAIL(snap_table_handler.preprea_meta(new_snap->meta_.bases_, merge_segments_))) {
    LOG_WARN("prepare meta fail", K(ret));
  } else {
    // Let insertup_meta_row write the child row and root reference in one transaction.
    snap_table_handler.set_meta_bridge(bridge_meta, bridge_segment);
    snap_table_handler.set_insert_bitmap_pruned(new_snap->insert_bitmap_pruned_);
    if (merge_deleted_vids_valid_) {
      snap_table_handler.set_bitmap_prune_plan(&merge_source_meta_, &merge_deleted_vids_);
    }
    if (OB_FAIL(snap_table_handler.insertup_meta_row(new_adapter_, tx_desc, timeout_us))) {
      LOG_WARN("insertup_meta_row fail", K(ret));
    } else {
      new_snap->meta_.is_persistent_ = true;
    }
  }

  if (OB_SUCC(ret) && OB_TMP_FAIL(update_task_result_segments_info())) {
    // The observability should not alter the execution flow. So update task info here
    LOG_WARN("upadte task result segments info fail", K(tmp_ret));
  }
  return ret;
}

int ObVecIdxMergeTask::prepare_new_meta(
    ObVectorIndexMeta &new_meta,
    ObVectorIndexSegmentMeta &new_seg_meta,
    const ObVectorIndexMeta &old_meta)
{
  int ret = OB_SUCCESS;
  if (merge_from_base_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.bases_.count(); ++i) {
      const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.bases_.at(i);
      bool is_merge_segment = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < merge_segments_.count() && ! is_merge_segment; ++j) {
        const ObVectorIndexSegmentMeta *merge_seg_meta = merge_segments_.at(j);
        if (0 == merge_seg_meta->start_key_.compare(old_seg_meta.start_key_)) {
          is_merge_segment = true;
        }
      }
      if (! is_merge_segment && OB_FAIL(new_meta.bases_.push_back(old_seg_meta))) {
        LOG_WARN("push back seg meta to bases fail", K(ret), K(old_seg_meta), K(new_meta));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(new_meta.bases_.push_back(new_seg_meta))) {
      LOG_WARN("push back seg meta to bases fail", K(ret), K(new_seg_meta), K(new_meta));
    }
  } else if (OB_FAIL(new_meta.bases_.assign(old_meta.bases_))) {
    LOG_WARN("assign base fail", K(ret));
  } else if (OB_FAIL(new_meta.incrs_.push_back(new_seg_meta))) {
    LOG_WARN("push back seg meta to incrs fail", K(ret), K(new_meta));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.incrs_.count(); ++i) {
    const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.incrs_.at(i);
    bool is_merge_segment = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < merge_segments_.count() && ! is_merge_segment; ++j) {
      const ObVectorIndexSegmentMeta *merge_seg_meta = merge_segments_.at(j);
      if (0 == merge_seg_meta->start_key_.compare(old_seg_meta.start_key_)) {
        is_merge_segment = true;
      }
    }
    if (! is_merge_segment && OB_FAIL(new_meta.incrs_.push_back(old_seg_meta))) {
      LOG_WARN("push back seg meta to incrs fail", K(ret), K(old_seg_meta), K(new_meta));
    }
  }
  LOG_INFO("prepare new meta finished", K(ret), K(new_meta), K(old_meta));
  return ret;
}

int ObVecIdxMergeTask::refresh_adaptor()
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  share::SCN current_scn;
  schema::ObTableParam snap_table_param(allocator_);
  storage::ObTableScanParam snap_scan_param;
  common::ObNewRowIterator *snap_data_iter = nullptr;
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *datum_row = nullptr;
  if (OB_FAIL(get_current_scn(current_scn))) {
    LOG_WARN("fail to get scn", KR(ret));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                                new_adapter_,
                                current_scn,
                                INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                allocator_,
                                allocator_,
                                snap_scan_param,
                                snap_table_param,
                                snap_data_iter))) {
    LOG_WARN("read_local_tablet fail", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snap_data_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table scan iter is null", K(ret), KP(snap_data_iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed.", K(ret));
    } else {
      LOG_INFO("there is no vector index meta row");
    }
  } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row invalid.", K(ret));
  } else if (datum_row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
  } else if (! datum_row->storage_datums_[0].get_string().suffix_match("_meta_data")) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first row of vector index is not meta row", K(ret), KPC(datum_row));
  } else {
    ObString meta_data = datum_row->storage_datums_[1].get_string();
    int64_t meta_scn = 0;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator_, ObLongTextType, meta_data, nullptr))) {
      LOG_WARN("read real string data fail", K(ret));
    } else if (OB_FAIL(ObVectorIndexMeta::get_meta_scn(meta_data, meta_scn))) {
      LOG_WARN("get meta scn fail", K(ret));
    } else if (meta_scn <= old_adapter_->get_snap_data()->meta_.header_.scn_) {
      no_need_replace_adaptor_ = true;
      LOG_WARN("no need refresh adaptor", K(meta_scn), KPC(old_adapter_));
      ObPluginVectorIndexAdapterGuard latest_adpt_guard;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(vec_idx_mgr_->get_adapter_inst_guard(ctx_->task_status_.inc_tablet_id_, latest_adpt_guard))) {
        LOG_WARN("fail to get adapter instance", K(tmp_ret), KPC(ctx_));
      } else if (latest_adpt_guard.get_adatper() != old_adapter_) {
        LOG_INFO("old adaptor is changeed", KPC(old_adapter_), KPC(latest_adpt_guard.get_adatper()), KPC(new_adapter_));
      } else {
        int64_t pos = 0;
        ObVectorIndexMeta tmp_meta;
        if (OB_TMP_FAIL(tmp_meta.deserialize(meta_data.ptr(), meta_data.length(), pos))) {
          LOG_WARN("deserialize fail", K(ret), K(meta_data.length()));
        }
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("old adaptor is not changeed, but scn is small", K(tmp_meta), KPC(old_adapter_), KPC(new_adapter_));
      }
    } else {
      CHECK_TASK_CANCELLED(ret, ctx_);
      DEBUG_SYNC(BEFORE_VECTOR_INDEX_MERGE_DESERIALIZE);

      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(new_adapter_->get_snap_data()->free_memdata_resource_with_lock(new_adapter_->get_allocator(), new_adapter_->get_tenant_id()))) {
      } else if (OB_FAIL(new_adapter_->deserialize_snap_meta(meta_data))) {
        LOG_WARN("deserialize snap meta fail", K(ret), K(meta_scn), K(meta_data.length()));
      } else if (OB_FAIL(try_reuse_segments_from_old_adapter(current_scn))) {
        LOG_WARN("try reuse segments from old adapter fail", K(ret));
      } else if (OB_FAIL(new_adapter_->get_snap_data()->load_persist_segments(new_adapter_, allocator_, ls_id_,
          current_scn, ctx_))) {
        LOG_WARN("load persist segments fail", K(ret));
      } else if (OB_FALSE_IT(new_adapter_->get_snap_data()->set_inited())) {
      } else if (OB_FALSE_IT(new_adapter_->set_snap_data_has_complete())) {
      } else {
        LOG_INFO("build new adaptor success", KPC(new_adapter_), KPC(old_adapter_));
        ObPluginVectorIndexAdapterGuard latest_adpt_guard;
        RWLock::WLockGuard lock_guard(vec_idx_mgr_->get_adapter_map_lock());
        if (OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard_in_lock(ctx_->task_status_.inc_tablet_id_, latest_adpt_guard))) {
          LOG_WARN("fail to get adapter instance", K(ret), KPC(ctx_));
        } else if (latest_adpt_guard.get_adatper()->get_snapshot_scn() > new_adapter_->get_snapshot_scn()) {
          LOG_INFO("snap data is load by other, so no need to refresh", KPC(new_adapter_), KPC(latest_adpt_guard.get_adatper()));
        } else if (OB_FAIL(new_adapter_->merge_incr_data(latest_adpt_guard.get_adatper()))){
          LOG_WARN("partial vector index adapter not valid", K(ret), KP(latest_adpt_guard.get_adatper()), KPC(this));
        } else if ((merge_from_base_ || merge_plan_.output_type_ == ObVectorIndexSegmentType::INCR_BASE)
                   && OB_FAIL(ObPluginVectorIndexUtils::refresh_async_query_scn(
                          ls_id_, new_adapter_))) {
          LOG_WARN("failed to set async query barrier for merged segment", K(ret));
        } else if (OB_FAIL(new_adapter_->set_replace_scn(current_scn))) {
          LOG_WARN("failed to set replace scn", K(ret), K(current_scn));
        } else if (OB_FAIL(vec_idx_mgr_->replace_old_adapter(new_adapter_))) {
          LOG_WARN("failed to replace old adapter", K(ret));
        } else {
          has_replace_old_adapter_ = true;
          LOG_INFO("relpace old adaptor success", KP(new_adapter_), KP(old_adapter_), KP(latest_adpt_guard.get_adatper()));
        }
      }
    }
  }

  if (OB_NOT_NULL(snap_data_iter)) {
    int tmp_ret = MTL(ObAccessService*)->revert_scan_iter(snap_data_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snap_data_iter failed", K(ret));
    }
    snap_data_iter = nullptr;
  }

  return ret;
}

int ObVecIdxMergeTask::prepare_merge_scn(const SCN &max_commit_scn)
{
  int ret = OB_SUCCESS;
  const ObVecIdxActiveDataHandle &incr_data = old_adapter_->get_incr_data();
  if (!incr_data.is_valid() || !max_commit_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge SCN state", K(ret), K(max_commit_scn), K(incr_data));
  } else {
    TCWLockGuard guard(incr_data->mem_data_rwlock_);
    SCN merge_scn = max_commit_scn;
    const SCN &freeze_task_scn = incr_data->freeze_task_scn_;
#ifdef ERRSIM
    if (freeze_task_scn.is_valid()) {
      if (OB_SUCCESS != ERRSIM_VEC_MERGE_SCN_EQUAL_FREEZE.test(old_adapter_->get_inc_tablet_id().id())) {
        merge_scn = freeze_task_scn;
      } else if (OB_SUCCESS != ERRSIM_VEC_MERGE_SCN_BEFORE_FREEZE.test(old_adapter_->get_inc_tablet_id().id())) {
        merge_scn = SCN::scn_dec(freeze_task_scn);
      }
    }
#endif
    if (freeze_task_scn.is_valid() && merge_scn <= freeze_task_scn) {
      ret = OB_EAGAIN;
      LOG_INFO("merge SCN overlaps freeze registration, retry later",
               K(ret), K(merge_scn), K(freeze_task_scn), KPC(ctx_));
    } else {
      if (!incr_data->merge_task_scn_.is_valid() || incr_data->merge_task_scn_ < merge_scn) {
        incr_data->merge_task_scn_ = merge_scn;
      }
      ctx_->task_status_.target_scn_ = merge_scn;
    }
  }
  return ret;
}

int ObVecIdxMergeTask::check_and_wait_write(SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  const static int64_t WAIT_US = 100 * 1000; // 100ms
  const static int64_t MAX_WAIT_CNT = 100;
  int64_t check_cnt = 0;
  const ObTabletID data_tablet_id = new_adapter_->get_data_tablet_id();
  const int64_t wait_threshold_us = ctx_->task_status_.target_scn_.convert_to_ts();
  const ObLSID &ls_id = ls_id_;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  ObLSService *ls_service = MTL(ObLSService *);
  transaction::ObTransID tx_id;
  ObLSHandle ls_handle;
  int64_t loop_cnt = 0;
  while(OB_SUCC(ret) && check_cnt < MAX_WAIT_CNT && ! snapshot_version.is_valid()) {
    if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls failed", K(ls_id));
    } else if (OB_FAIL(ls_handle.get_ls()->check_modify_time_elapsed(
                   data_tablet_id, wait_threshold_us, tx_id))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("check modify time elapsed failed", K(ret), K(wait_threshold_us), K(tx_id),
                 K(data_tablet_id), K(ls_id));
      } else {
        ret = OB_SUCCESS;
        ob_usleep(WAIT_US);
        LOG_INFO("there are some write trans donot commit, so need wait", K(ls_id),
                 K(data_tablet_id), K(wait_threshold_us));
      }
    } else if (OB_FAIL(txs->get_max_commit_version(snapshot_version))) {
      LOG_WARN("fail to get max commit version", K(ret));
    } else {
      LOG_INFO("check success", K(snapshot_version), K(data_tablet_id), K(wait_threshold_us), K(tx_id));
    }
    ++check_cnt;
    CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
  }
  if (OB_SUCC(ret) && check_cnt >= MAX_WAIT_CNT && ! snapshot_version.is_valid()) {
    ret = OB_EAGAIN;
    LOG_WARN("check and wait write timeout, need retry", K(ret), K(data_tablet_id),
             K(wait_threshold_us), K(tx_id));
  }
  return ret;
}


}
}
