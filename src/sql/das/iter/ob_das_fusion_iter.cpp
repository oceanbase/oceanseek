/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_sort.h"
#include "share/vector/type_traits.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/omt/ob_multi_tenant.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/ob_physical_plan.h"
#include <float.h>
#include <cmath>
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_fusion_iter.h"
#include "sql/das/iter/ob_das_profile_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASFusionIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param.type_ != ObDASIterType::DAS_ITER_FUSION)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    const ObDASFusionIterParam &fusion_param = static_cast<const ObDASFusionIterParam &>(param);
    // Extract necessary fields from param (avoid copying entire struct)
    fusion_ctdef_ = fusion_param.fusion_ctdef_;
    fusion_method_ = fusion_param.fusion_method_;
    rank_window_size_ = fusion_param.rank_window_size_;
    rank_constant_ = fusion_param.rank_constant_;
    has_hybrid_fusion_op_ = fusion_param.has_hybrid_fusion_op_;
    is_single_partition_ = fusion_param.is_single_partition_;
    fusion_iter_exec_mode_ = fusion_param.fusion_iter_exec_mode_;
    offset_ = fusion_param.offset_;
    size_ = fusion_param.size_;
    min_score_ = fusion_param.min_score_;
    search_ctx_ = fusion_param.search_ctx_;
    fusion_rtdef_ = fusion_param.fusion_rtdef_;
    if (OB_NOT_NULL(fusion_ctdef_->rerank_window_size_expr_)) {
      rerank_window_size_ = fusion_param.rerank_window_size_;
    }
    
    // if it is distrubuted scenario, the rank_window_size_ is the max of all path top k limits.
    for (int64_t i = 0; OB_SUCC(ret) && rank_window_size_ > 0 && i < fusion_ctdef_->children_cnt_; ++i) {
      int64_t path_top_k_limit = fusion_param.path_top_k_limits_.at(i);
      if (OB_FAIL(path_top_k_limits_.push_back(path_top_k_limit))) {
        LOG_WARN("failed to push back path top k limit", K(ret), K(i));
      } else if (!is_single_partition() && path_top_k_limit > rank_window_size_) {
        rank_window_size_ = path_top_k_limit;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(weights_.assign(fusion_param.weights_))) {
      LOG_WARN("failed to assign weights", K(ret));
    } else if (OB_ISNULL(fusion_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion_ctdef is null", K(ret));
    } else {
      const int64_t children_cnt_for_estimate = OB_MAX(1L, static_cast<int64_t>(fusion_ctdef_->children_cnt_));
      const int64_t base_estimate = (rank_window_size_ > 0 && rank_window_size_ > INT64_MAX / children_cnt_for_estimate)
                                     ? INT64_MAX : rank_window_size_ * children_cnt_for_estimate;
      expected_doc_count_ = OB_MAX(64L, base_estimate);
      int64_t estimated_doc_count = 0;
      for (int64_t i = 0; i < path_top_k_limits_.count(); ++i) {
        const int64_t path_top_k_limit = path_top_k_limits_.at(i);
        if (path_top_k_limit > 0) {
          if (estimated_doc_count > INT64_MAX - path_top_k_limit) {
            estimated_doc_count = INT64_MAX;
          } else {
            estimated_doc_count += path_top_k_limit;
          }
        }
      }
      if (estimated_doc_count > 0) {
        expected_doc_count_ = OB_MAX(expected_doc_count_, estimated_doc_count);
      }
      score_exprs_ = &fusion_ctdef_->get_score_exprs();
      lib::ContextParam context_param;
      context_param.set_mem_attr(MTL_ID(), "DASFusionIter", ObCtxIds::DEFAULT_CTX_ID)
          .set_properties(lib::USE_TL_PAGE_OPTIONAL);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(fusion_memctx_, context_param))) {
        LOG_WARN("failed to create fusion iter memory context", K(ret));
      } else if (OB_FAIL(set_rowkey_is_uint64_flag())) {
        LOG_WARN("failed to check rowkey type", K(ret));
      } else {
        const int64_t children_cnt = fusion_ctdef_->children_cnt_;
        const int64_t default_capacity = OB_MAX(rank_window_size_ * children_cnt, 64);
        const int64_t fusion_docs_capacity = OB_MAX(expected_doc_count_, default_capacity);
        const int64_t sorted_indices_capacity = fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS
                                                  ? OB_MAX(rank_window_size_, 64L)
                                                  : fusion_docs_capacity;
        if (OB_FAIL(fusion_docs_.reserve(fusion_docs_capacity))) {
          LOG_WARN("failed to reserve fusion_docs_", K(ret), K(fusion_docs_capacity));
        } else if (OB_FAIL(sorted_doc_indices_.reserve(sorted_indices_capacity))) {
          LOG_WARN("failed to reserve sorted_doc_indices_", K(ret), K(sorted_indices_capacity));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_fusion_row())) {
      LOG_WARN("failed to init fusion row", K(ret));
    } else if (OB_ISNULL(search_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("search_ctx_ is null", K(ret));
    } else {
      // 并行规划推迟到 do_table_scan()，此时所有子迭代器及其 SearchOp 树均已构造完成。
    }
  }
  return ret;
}

int ObDASFusionIter::inner_reuse()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_NOT_NULL(children_[i])) {
      if (OB_FAIL(children_[i]->reuse())) {
        LOG_WARN("failed to reuse child iter", K(ret), K(i));
      }
    }
  }

  for (int64_t i = 0; i < fusion_docs_.count(); ++i) {
    fusion_docs_.at(i).reset();
  }
  
  fusion_docs_.reset();
  sorted_doc_indices_.reset();
  destroy_rowid_map();
  free_fusion_score_heap();
  release_parallel_runtime();

  if (OB_NOT_NULL(fusion_memctx_)) {
    fusion_memctx_->reset_remain_one_page();
  }

  // Reset state
  fusion_row_.reset();
  fusion_row_inited_ = false;
  fusion_finished_ = false;
  is_sorted_ = false;
  rowid_map_inited_ = false;
  uint64_rowid_map_inited_ = false;
  output_idx_ = 0;
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  fusion_profile_ = nullptr;
  fake_skip_ = nullptr;
  parallel_execution_used_ = false;

  return ret;
}

int ObDASFusionIter::inner_release()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < fusion_docs_.count(); ++i) {
    fusion_docs_.at(i).reset();
  }

  fusion_docs_.reset();
  sorted_doc_indices_.reset();
  destroy_rowid_map();
  free_fusion_score_heap();
  release_parallel_runtime();

  if (OB_NOT_NULL(fusion_memctx_)) {
    fusion_memctx_->reset_remain_one_page();
    DESTROY_CONTEXT(fusion_memctx_);
    fusion_memctx_ = nullptr;
  }

  fusion_row_.reset();
  weights_.reset();
  path_top_k_limits_.reset();
  heap_cmp_ = ObDASFusionScoreMinHeapCmp();

  fusion_row_inited_ = false;
  fusion_finished_ = false;
  is_sorted_ = false;
  rowid_map_inited_ = false;
  uint64_rowid_map_inited_ = false;
  output_idx_ = 0;
  input_row_cnt_ = 0;
  output_row_cnt_ = 0;
  fusion_profile_ = nullptr;
  fake_skip_ = nullptr;
  score_exprs_ = nullptr;
  parallel_execution_used_ = false;
  serial_children_started_ = false;
  return ret;
}

int ObDASFusionIter::record_auto_dop_profile()
{
  int ret = OB_SUCCESS;
  const ObDASFusionPathAdapter *path_adapter = nullptr;
  const ObDASFusionDopDecision *decision = nullptr;
  double tenant_cpu_pressure_percent = -1.0;
  int64_t load_dop_cap = 1;
  int64_t target_path_dop = 1;
  int64_t actual_path_slice_count = 1;
  if (OB_ISNULL(fusion_ctdef_)
      || ObDASFusionDopPolicy::AUTO_QUERY_DOP
             != fusion_ctdef_->query_dop_
      || !fusion_ctdef_->has_search_subquery()) {
    // 显式 DOP 不发布 AUTO DOP 决策指标。
  } else {
    tenant_cpu_pressure_percent =
        parallel_ctx_.get_tenant_cpu_pressure_percent();
    load_dop_cap = ObDASFusionDopPolicy::get_load_dop_cap(
        tenant_cpu_pressure_percent);
    path_adapter = parallel_ctx_.get_path_adapter(
        fusion_ctdef_->get_search_idx());
    if (OB_NOT_NULL(path_adapter)) {
      decision = &path_adapter->get_dop_decision();
      load_dop_cap = OB_MAX(1L, decision->load_dop_cap_);
      target_path_dop = OB_MAX(1L, decision->target_path_dop_);
      actual_path_slice_count = OB_MAX(
          1L, decision->actual_path_slice_count_);
    } else {
      // 提前负载反压及其他串行计划不会构造 PathAdapter。
    }
    if (std::isfinite(tenant_cpu_pressure_percent)
        && tenant_cpu_pressure_percent >= 0.0) {
      const int64_t pressure_percent = static_cast<int64_t>(
          tenant_cpu_pressure_percent + 0.5);
      SET_METRIC_VAL(
          common::ObMetricId::HS_AUTO_DOP_CPU_PRESSURE,
          pressure_percent);
    } else {
      // 尽力采样不可用时不发布对应指标。
    }
    SET_METRIC_VAL(
        common::ObMetricId::HS_AUTO_DOP_LOAD_CAP,
        load_dop_cap);
    SET_METRIC_VAL(
        common::ObMetricId::HS_AUTO_DOP_TARGET,
        target_path_dop);
    SET_METRIC_VAL(
        common::ObMetricId::HS_AUTO_DOP_ACTUAL,
        actual_path_slice_count);
  }
  return ret;
}

int ObDASFusionIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  bool use_parallel = false;
  common::ObOpProfile<common::ObMetric> *fusion_profile = nullptr;

  if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
          common::ObProfileId::HYBRID_SEARCH_FUSION_ITER, fusion_profile))) {
    LOG_WARN("failed to init runtime profile", KR(ret));
  } else {
    fusion_profile_ = fusion_profile;
    common::ObProfileSwitcher switcher(fusion_profile);
    SET_METRIC_VAL(common::ObMetricId::HS_FUSION_OFFSET, offset_);
    SET_METRIC_VAL(common::ObMetricId::HS_FUSION_SIZE, size_);
    SET_METRIC_VAL(common::ObMetricId::HS_RANK_WINDOW_SIZE, rank_window_size_);
    SET_METRIC_VAL(common::ObMetricId::HS_FUSION_METHOD, static_cast<uint64_t>(fusion_method_));
    SET_METRIC_VAL(common::ObMetricId::HS_FUSION_EXEC_MODE, static_cast<uint64_t>(fusion_iter_exec_mode_));
    SET_METRIC_VAL(common::ObMetricId::HS_FUSION_INPUT_ROW_COUNT, 0);
    {
      common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
      if (OB_FAIL(plan_parallel_execution(use_parallel))) {
        LOG_WARN("failed to plan fusion table scan execution", KR(ret));
      } else if (use_parallel && OB_FAIL(do_parallel_table_scan())) {
        LOG_WARN("failed to do parallel fusion table scan", KR(ret));
      } else if (!use_parallel
                 && OB_FAIL(do_serial_table_scan(
                        false /* use_rescan */))) {
        LOG_WARN("failed to do serial fusion table scan", KR(ret));
      }
    }
  }

  return ret;
}

int ObDASFusionIter::rescan()
{
  int ret = OB_SUCCESS;
  bool use_parallel = false;
  common::ObOpProfile<common::ObMetric> *fusion_profile = nullptr;

  if (OB_FAIL(inner_reuse())) {
    LOG_WARN("failed to reuse fusion iter", K(ret));
  } else if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
          common::ObProfileId::HYBRID_SEARCH_FUSION_ITER, fusion_profile))) {
    LOG_WARN("failed to init runtime profile", KR(ret));
  } else {
    fusion_profile_ = fusion_profile;
    common::ObProfileSwitcher switcher(fusion_profile);
    SET_METRIC_VAL(common::ObMetricId::HS_FUSION_INPUT_ROW_COUNT, 0);
    {
      common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
      if (OB_FAIL(plan_parallel_execution(use_parallel))) {
        LOG_WARN("failed to plan fusion rescan execution", KR(ret));
      } else if (use_parallel && OB_FAIL(do_parallel_table_scan())) {
        LOG_WARN("failed to do parallel fusion rescan", KR(ret));
      } else if (!use_parallel
                 && OB_FAIL(do_serial_table_scan(
                        true /* use_rescan */))) {
        LOG_WARN("failed to do serial fusion rescan", KR(ret));
      }
    }
  }

  return ret;
}

int ObDASFusionIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(fusion_profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  if (limit_param_.limit_ >= 0 && output_row_cnt_ >= limit_param_.limit_) {
    ret = OB_ITER_END;
  } else if (!fusion_finished_ && OB_FAIL(do_fusion(false /* is_vectorized */))) {
    LOG_WARN("failed to do fusion", K(ret));
  } else {
    if (OB_FAIL(get_next_row_from_store(fusion_row_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row from store", K(ret));
      }
    } else {
      ++input_row_cnt_;
      ++output_row_cnt_;
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 1);
    }
  }
  return ret;
}

int ObDASFusionIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  common::ObProfileSwitcher switcher(fusion_profile_);
  common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
  count = 0;
  if (fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS && rank_window_size_ == 0) {
    ret = OB_ITER_END;
  } else if (!fusion_finished_ && OB_FAIL(do_fusion(true /* is_vectorized */))) {
    LOG_WARN("failed to do fusion", K(ret));
  } else {
    int64_t max_capacity = capacity;
    if (limit_param_.limit_ > 0) {
      max_capacity = OB_MIN(capacity, limit_param_.limit_ - output_row_cnt_);
    }
    if (max_capacity <= 0) {
      ret = OB_ITER_END;
    } else {
      if (OB_FAIL(get_next_batch_from_store(fusion_row_, max_capacity, count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next batch from store", K(ret));
        } else if (count > 0) {
          ret = OB_SUCCESS;
          input_row_cnt_ += count;
          output_row_cnt_ += count;
        }
      } else {
        input_row_cnt_ += count;
        output_row_cnt_ += count;
      }
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
    }
  }

  return ret;
}

template<typename RowkeyType>
int ObDASFusionIter::extract_rowkey(RowkeyType &rowkey)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

template<>
int ObDASFusionIter::extract_rowkey<uint64_t>(uint64_t &rowkey)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &rowkey_exprs = fusion_ctdef_->rowid_exprs_;
  if (OB_UNLIKELY(rowkey_exprs.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uint64 rowkey should have exactly one expr", K(ret), K(rowkey_exprs.count()));
  } else {
    ObExpr *rowkey_expr = rowkey_exprs.at(0);
    ObDatum &rowkey_datum = rowkey_expr->locate_expr_datum(*eval_ctx_);
    rowkey = rowkey_datum.get_uint();
  }
  return ret;
}

// to do: implement this
template<typename RowkeyType>
int ObDASFusionIter::find_or_create_doc(const RowkeyType &rowkey,
                                        const int64_t path_idx,
                                        double score)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

template<>
int ObDASFusionIter::find_or_create_doc<uint64_t>(const uint64_t &rowkey_val,
                                                  const int64_t path_idx,
                                                  double score)
{
  int ret = OB_SUCCESS;
  int64_t existing_idx = -1;
  int64_t doc_idx = -1;
  if (!uint64_rowid_map_inited_ && OB_FAIL(init_rowid_map())) {
    LOG_WARN("failed to init uint64_rowid_doc_map", K(ret));
  } else if (OB_FAIL(uint64_rowid_doc_map_.get_refactored(rowkey_val, existing_idx))){
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      existing_idx = -1;
    } else {
      LOG_WARN("failed to get from uint64_rowid_doc_map", K(ret), K(rowkey_val));
    }
  }

  if (OB_SUCC(ret)) {
    if (-1 == existing_idx) {
      ObDASFusionDocInfo doc_info;
      doc_info.fusion_score_ = 0.0;
      ObArenaAllocator &allocator = fusion_memctx_->get_arena_allocator();
      const int64_t path_count = fusion_ctdef_->children_cnt_;
      if (OB_FAIL(doc_info.init_raw_scores(path_count, allocator, fusion_method_))) {
        LOG_WARN("failed to init raw_scores_", K(ret), K(path_count));
      } else if (OB_FAIL(fusion_docs_.push_back(doc_info))) {
        LOG_WARN("failed to push back doc_info", K(ret));
      } else {
        doc_idx = fusion_docs_.count() - 1;
        fusion_docs_.at(doc_idx).set_uint64_rowkey(rowkey_val);
        if (OB_FAIL(uint64_rowid_doc_map_.set_refactored(rowkey_val, doc_idx))) {
          LOG_WARN("failed to insert into uint64_rowid_doc_map", K(ret), K(rowkey_val), K(doc_idx));
        } else if (OB_FAIL(set_score_to_doc(doc_idx, score, path_idx))) {
          LOG_WARN("failed to set score to doc", K(ret), K(doc_idx), K(path_idx));
        }
      }
    } else {
      doc_idx = existing_idx;
      if (OB_FAIL(set_score_to_doc(doc_idx, score, path_idx))) {
        LOG_WARN("failed to set score to doc", K(ret), K(doc_idx), K(path_idx));
      }
    }
  }
  return ret;
}

int ObDASFusionIter::plan_parallel_execution(bool &use_parallel)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  const ObPhysicalPlan *phy_plan = nullptr;
  use_parallel = false;
  parallel_execution_used_ = false;
  if (OB_ISNULL(fusion_ctdef_)
      || OB_ISNULL(fusion_rtdef_)
      || OB_ISNULL(search_ctx_)
      || OB_ISNULL(fusion_memctx_)
      || OB_ISNULL(exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion iter misses state for parallel planning",
             KR(ret), KP(fusion_ctdef_), KP(fusion_rtdef_),
             KP(search_ctx_), KP(exec_ctx_));
  } else if (OB_ISNULL(
                 plan_ctx = exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion exec ctx misses physical plan ctx", KR(ret));
  } else {
    phy_plan = plan_ctx->get_phy_plan();
    if (0 == exec_ctx_->get_phy_op_size()
        || OB_ISNULL(phy_plan)) {
      // Remote DAS reconstructs a partial ExecContext without a complete
      // physical plan. The current Fusion worker runtime cannot clone it.
      use_parallel = false;
      LOG_INFO("[FUSION_TRACE] fallback fusion to serial for partial exec ctx",
               "phy_op_size", exec_ctx_->get_phy_op_size(),
               KP(phy_plan));
    } else {
      ObIAllocator &alloc = fusion_memctx_->get_arena_allocator();
      if (OB_FAIL(parallel_ctx_.plan_paths(
              alloc, *fusion_ctdef_, *fusion_rtdef_, *search_ctx_,
              children_, children_cnt_, path_top_k_limits_))) {
        LOG_WARN("failed to plan fusion paths", KR(ret), K(children_cnt_));
      } else {
        use_parallel = !parallel_ctx_.should_fallback_to_serial();
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(record_auto_dop_profile())) {
    LOG_WARN("failed to record auto dop profile", KR(ret));
  } else {
    LOG_TRACE("[FUSION_TRACE] planned fusion execution mode",
              K(use_parallel), K(children_cnt_));
  }
  return ret;
}

int ObDASFusionIter::do_serial_table_scan(const bool use_rescan)
{
  int ret = OB_SUCCESS;
  const bool should_rescan = use_rescan && serial_children_started_;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child iter is null", KR(ret), K(i));
    } else if (should_rescan && OB_FAIL(children_[i]->rescan())) {
      LOG_WARN("failed to rescan fusion child iter", KR(ret), K(i));
    } else if (!should_rescan
               && OB_FAIL(children_[i]->do_table_scan())) {
      LOG_WARN("failed to do table scan on fusion child iter", KR(ret), K(i));
    } else {
      // Continue opening or rescanning the remaining children.
    }
  }
  if (OB_SUCC(ret) && !should_rescan) {
    serial_children_started_ = true;
  }
  return ret;
}

int ObDASFusionIter::submit_ready_work_units(
    const int64_t timeout_ts,
    const int32_t group_id,
    int64_t &submitted_task_cnt,
    bool &submitted_ready_unit)
{
  int ret = OB_SUCCESS;
  omt::ObMultiTenant *omt = GCTX.omt_;
  const int64_t work_unit_count = parallel_ctx_.get_work_unit_count();
  submitted_ready_unit = false;
  // 关闭 SQL 内存泄漏检查：ObDASFusionWorkUnitTask 通过 OB_NEW 在此处分配，
  // 执行完成后由 OMT 框架在 worker 线程释放。跨线程转移所有权会造成误报。
  DISABLE_SQL_MEMLEAK_GUARD;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("OMT is null when submitting fusion work units", KR(ret));
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && i < work_unit_count;
       ++i) {
    bool is_ready = false;
    ObDASFusionWorkUnit *work_unit = parallel_ctx_.get_work_unit(i);
    ObDASFusionWorkUnitTask *task = nullptr;
    if (OB_ISNULL(work_unit)
        || OB_ISNULL(work_unit->work_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null fusion work unit or work",
               KR(ret), K(i), KP(work_unit),
               "work", OB_NOT_NULL(work_unit)
                   ? work_unit->work_ : nullptr);
    } else if (ATOMIC_LOAD(&work_unit->submitted_)
               || ATOMIC_LOAD(&work_unit->finished_)) {
      // 已提交或已进入终态。
    } else if (OB_FAIL(
        parallel_ctx_.check_work_unit_ready(*work_unit, is_ready))) {
      LOG_WARN("failed to check fusion work unit dependency",
               KR(ret), K(i), KPC(work_unit));
    } else if (!is_ready) {
      // 它依赖的 PREPARE WorkUnit 仍在执行。
    } else if (OB_SUCCESS != parallel_coordinator_.get_first_error()) {
      ret = parallel_coordinator_.get_first_error();
      LOG_WARN("stop submitting ready work unit after task failure",
               KR(ret), K(i), KPC(work_unit));
    } else if (OB_ISNULL(task = OB_NEW(
        ObDASFusionWorkUnitTask,
        common::ObMemAttr(MTL_ID(), "DASFusTask")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc fusion work unit task",
               KR(ret), K(i), KPC(work_unit));
    } else if (OB_FAIL(task->init(
        work_unit, &parallel_coordinator_, timeout_ts, group_id))) {
      LOG_WARN("failed to init fusion work unit task",
               KR(ret), K(i), KPC(work_unit));
    } else {
      ATOMIC_STORE(&work_unit->submitted_, true);
      if (OB_FAIL(omt->recv_request(MTL_ID(), *task))) {
        ATOMIC_STORE(&work_unit->submitted_, false);
        LOG_WARN("failed to submit fusion work unit task",
                 KR(ret), K(i), KPC(work_unit));
      } else {
        task = nullptr;
        ++submitted_task_cnt;
        submitted_ready_unit = true;
      }
    }
    if (OB_SUCCESS != ret && OB_NOT_NULL(task)) {
      OB_DELETE(ObDASFusionWorkUnitTask, "DASFusTask", task);
      task = nullptr;
    }
  }
  return ret;
}

int ObDASFusionIter::execute_parallel_work_units(
    const int64_t timeout_ts,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  int wait_ret = OB_SUCCESS;
  int64_t submitted_task_cnt = 0;
  int64_t observed_finished_cnt = 0;
  const int64_t work_unit_count = parallel_ctx_.get_work_unit_count();
  while (OB_SUCC(ret) && submitted_task_cnt < work_unit_count) {
    bool submitted_ready_unit = false;
    if (OB_SUCCESS != parallel_coordinator_.get_first_error()) {
      ret = parallel_coordinator_.get_first_error();
      LOG_WARN("stop submitting fusion work units after task failure",
               KR(ret), K(submitted_task_cnt), K(work_unit_count));
    } else if (OB_FAIL(submit_ready_work_units(
                   timeout_ts, group_id,
                   submitted_task_cnt, submitted_ready_unit))) {
      LOG_WARN("failed to submit ready fusion work units",
               KR(ret), K(submitted_task_cnt), K(work_unit_count));
    } else if (submitted_task_cnt < work_unit_count
               && !submitted_ready_unit) {
      if (0 == submitted_task_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no ready fusion work unit can make progress",
                 KR(ret), K(work_unit_count));
      } else {
        int64_t new_finished_cnt = observed_finished_cnt;
        if (OB_FAIL(parallel_coordinator_.wait_for_progress(
                observed_finished_cnt, new_finished_cnt, timeout_ts))) {
          LOG_WARN("failed to wait fusion work unit dependency",
                   KR(ret), K(observed_finished_cnt),
                   K(submitted_task_cnt), K(work_unit_count));
        } else {
          observed_finished_cnt = new_finished_cnt;
        }
      }
    }
  }
  if (OB_SUCCESS != ret && parallel_coordinator_.is_inited()) {
    parallel_coordinator_.set_first_error(ret);
    parallel_ctx_.cancel_unsubmitted_work_units(parallel_coordinator_, ret);
  }
  if (parallel_coordinator_.is_inited()) {
    wait_ret = parallel_coordinator_.wait_all_complete(timeout_ts);
    if (OB_SUCCESS != wait_ret) {
      LOG_WARN("failed to wait fusion work unit tasks",
               KR(wait_ret), KR(ret), K(timeout_ts), K(children_cnt_),
               K(submitted_task_cnt),
               "work_unit_count", parallel_ctx_.get_work_unit_count());
      parallel_coordinator_.set_first_error(wait_ret);
      parallel_ctx_.cancel_unsubmitted_work_units(
          parallel_coordinator_, wait_ret);
      const int barrier_ret =
          parallel_coordinator_.wait_all_complete_barrier();
      if (OB_SUCCESS != barrier_ret) {
        LOG_WARN("failed to wait fusion work unit ownership barrier",
                 KR(barrier_ret), KR(wait_ret), KR(ret),
                 K(submitted_task_cnt),
                 "work_unit_count",
                 parallel_ctx_.get_work_unit_count());
      } else {
        // 所有 worker 都已停止访问查询持有的 Work 和子任务状态。
      }
      if (OB_SUCC(ret)) {
        ret = wait_ret;
      }
    }
  }
  return ret;
}

int ObDASFusionIter::record_parallel_path_makespan(
    const int64_t parallel_stage_begin_ts_us)
{
  int ret = OB_SUCCESS;
  int64_t query_path_complete_ts_us = 0;
  int64_t knn_path_complete_ts_us = 0;
  const int64_t work_unit_count = parallel_ctx_.get_work_unit_count();
  for (int64_t i = 0; i < work_unit_count; ++i) {
    const ObDASFusionWorkUnit *work_unit =
        parallel_ctx_.get_work_unit(i);
    const ObDASFusionPathAdapter *path_adapter =
        OB_ISNULL(work_unit)
            ? nullptr
            : parallel_ctx_.get_path_adapter(work_unit->path_id_);
    const int64_t complete_ts_us =
        OB_ISNULL(work_unit)
            ? 0
            : ATOMIC_LOAD(&work_unit->complete_ts_us_);
    if (OB_ISNULL(path_adapter) || complete_ts_us <= 0) {
      LOG_WARN("invalid work unit runtime metric",
               K(i), KP(work_unit), KP(path_adapter),
               K(complete_ts_us));
    } else if (ObDASFusionPathType::QUERY
               == path_adapter->get_logical_path().type_) {
      query_path_complete_ts_us =
          OB_MAX(query_path_complete_ts_us, complete_ts_us);
    } else if (ObDASFusionPathType::KNN
               == path_adapter->get_logical_path().type_) {
      knn_path_complete_ts_us =
          OB_MAX(knn_path_complete_ts_us, complete_ts_us);
    }
  }
  if (query_path_complete_ts_us > 0) {
    SET_METRIC_VAL(
        common::ObMetricId::HS_QUERY_PATH_MAKESPAN,
        OB_MAX(0, query_path_complete_ts_us
                  - parallel_stage_begin_ts_us) * 1000);
  }
  if (knn_path_complete_ts_us > 0) {
    SET_METRIC_VAL(
        common::ObMetricId::HS_KNN_PATH_MAKESPAN,
        OB_MAX(0, knn_path_complete_ts_us
                  - parallel_stage_begin_ts_us) * 1000);
  }
  return ret;
}

int ObDASFusionIter::do_parallel_table_scan()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  const int64_t timeout_ts = THIS_WORKER.get_timeout_ts();
  const int32_t group_id =
      THIS_WORKER.get_group_id() | das::OB_DAS_PARALLEL_POOL_MARK;
  if (OB_ISNULL(exec_ctx_)
      || OB_ISNULL(eval_ctx_)
      || OB_ISNULL(search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr for parallel fusion scan",
             KR(ret), KP(exec_ctx_), KP(eval_ctx_),
             KP(search_ctx_));
  } else if (OB_ISNULL(plan_ctx = exec_ctx_->get_physical_plan_ctx())
             || OB_ISNULL(fusion_ctdef_)
             || OB_ISNULL(fusion_rtdef_)
             || OB_ISNULL(fusion_memctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion iter misses state for parallel table scan",
             KR(ret), KP(plan_ctx), KP(fusion_ctdef_),
             KP(fusion_rtdef_));
  } else {
    ObIAllocator &alloc = fusion_memctx_->get_arena_allocator();
    if (OB_FAIL(parallel_ctx_.prepare_works(
                   alloc, *fusion_ctdef_, *search_ctx_,
                   *exec_ctx_, *eval_ctx_))) {
      LOG_WARN("failed to prepare fusion parallel works",
               KR(ret), K(children_cnt_));
    } else if (OB_FAIL(parallel_coordinator_.init(
        parallel_ctx_.get_work_unit_count()))) {
      LOG_WARN("failed to init fusion parallel coordinator", KR(ret),
               "work_unit_count", parallel_ctx_.get_work_unit_count());
    } else if (OB_FAIL(create_parallel_task_profiles())) {
      parallel_coordinator_.set_first_error(ret);
      parallel_ctx_.cancel_unsubmitted_work_units(
          parallel_coordinator_, ret);
      LOG_WARN("failed to create parallel task profiles", KR(ret));
    } else {
      const int64_t parallel_stage_begin_ts_us =
          common::ObTimeUtility::current_time();
      if (OB_FAIL(execute_parallel_work_units(
              timeout_ts, group_id))) {
        LOG_WARN("failed to execute fusion parallel work units", KR(ret));
      } else if (OB_FAIL(record_parallel_path_makespan(
                     parallel_stage_begin_ts_us))) {
        LOG_WARN("failed to record fusion path makespan", KR(ret));
      } else {
        parallel_execution_used_ = true;
      }
    }
  }
  return ret;
}

namespace
{
struct ObDASFusionMaterializedRowCmp
{
  bool operator()(const ObDASFusionMaterializedRow &lhs,
                  const ObDASFusionMaterializedRow &rhs) const
  {
    bool bret = false;
    if (lhs.score_ > rhs.score_) {
      bret = true;
    } else if (lhs.score_ < rhs.score_) {
      bret = false;
    } else {
      bret = lhs.rowkey_ < rhs.rowkey_;
    }
    return bret;
  }
};
} // namespace

int ObDASFusionIter::merge_parallel_work_rows(
    const ObDASFusionPathSliceWork &work)
{
  int ret = OB_SUCCESS;
  const common::ObSEArray<ObDASFusionMaterializedRow, 256> &rows =
      work.get_rows();
  for (int64_t row_idx = 0;
       OB_SUCC(ret) && row_idx < rows.count();
       ++row_idx) {
    const ObDASFusionMaterializedRow &row = rows.at(row_idx);
    if (OB_FAIL(find_or_create_doc(
            row.rowkey_, work.get_path_id(), row.score_))) {
      LOG_WARN("failed to merge fusion path work row",
               KR(ret), K(row_idx),
               "path_idx", work.get_path_id(), K(row));
    }
  }
  return ret;
}

int ObDASFusionIter::merge_partitioned_path_rows(
    const int64_t path_idx,
    const int64_t top_k_limit,
    common::ObIArray<ObDASFusionMaterializedRow> &path_rows)
{
  int ret = OB_SUCCESS;
  // top_k_limit < 0 means TopK was not pushed down into the search iter (e.g. upper SORT
  // is on a non-score column). Match serial semantics: emit all rows without sort and let
  // the upper operator handle ordering / limiting.
  const bool no_limit = (top_k_limit < 0);
  if (!no_limit && path_rows.count() > 1) {
    lib::ob_sort(path_rows.get_data(), path_rows.get_data() + path_rows.count(),
                 ObDASFusionMaterializedRowCmp());
  }
  const int64_t emit_cnt = no_limit ? path_rows.count() : OB_MIN(top_k_limit, path_rows.count());
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < emit_cnt; ++row_idx) {
    const ObDASFusionMaterializedRow &row = path_rows.at(row_idx);
    if (OB_FAIL(find_or_create_doc(row.rowkey_, path_idx, row.score_))) {
      LOG_WARN("failed to merge partitioned path row",
               KR(ret), K(path_idx), K(top_k_limit), K(row_idx), K(row));
    }
  }
  return ret;
}

// 将所有并行执行 WorkUnit 的结果合并到 FusionIter 主数据结构中。
//
// 合并策略如下：
//   - 只有一个 Work 的路径直接合并该 Work 的结果行。
//   - 包含多个物理 Slice 的路径先将结果收集到路径级容器，统一排序后输出路径 TopK，
//     从而保持物理切分与 RRF 逻辑路径语义相互独立。
int ObDASFusionIter::merge_parallel_results()
{
  int ret = OB_SUCCESS;
  const int64_t work_count =
      parallel_ctx_.get_path_slice_work_count();

  // 调用本方法前已经处理完调度完成状态和错误，本阶段只消费成功的路径 Slice 结果。
  if (OB_ISNULL(fusion_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion ctdef is null", KR(ret), KP(fusion_ctdef_));
  } else {
    const int64_t path_count = fusion_ctdef_->children_cnt_;
    common::ObSEArray<int64_t, 8> path_work_counts;
    common::ObSEArray<int64_t, 8> path_top_k_limits;
    common::ObSEArray<bool, 8> path_top_k_limit_initialized;
    common::ObSEArray<common::ObSEArray<ObDASFusionMaterializedRow, 16>, 8> path_rows_buckets;
    if (OB_FAIL(path_work_counts.prepare_allocate(path_count)) ||
        OB_FAIL(path_top_k_limits.prepare_allocate(path_count)) ||
        OB_FAIL(path_top_k_limit_initialized.prepare_allocate(path_count)) ||
        OB_FAIL(path_rows_buckets.prepare_allocate(path_count))) {
      LOG_WARN("failed to init path merge structures", KR(ret), K(path_count));
    } else {
      for (int64_t path_idx = 0; path_idx < path_count; ++path_idx) {
        path_work_counts.at(path_idx) = 0;
        path_top_k_limits.at(path_idx) = -1;
        path_top_k_limit_initialized.at(path_idx) = false;
      }
    }
    // 先按逻辑路径统计物理 Work 数量；合并行为取决于路径是否被切分，
    // 而不是某种具体的 Slice 类型。
    for (int64_t i = 0; OB_SUCC(ret) && i < work_count; ++i) {
      ObDASFusionPathSliceWork *work =
          parallel_ctx_.get_path_slice_work(i);
      if (OB_ISNULL(work)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fusion path slice work is null", KR(ret), K(i));
      } else if (work->get_path_id() < 0
                 || work->get_path_id() >= path_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fusion path slice work path idx is invalid",
                 KR(ret), K(i),
                 "path_idx", work->get_path_id(), K(path_count));
      } else {
        ++path_work_counts.at(work->get_path_id());
      }
    }
    // 收集被切分路径的结果，并直接合并未切分路径。
    for (int64_t i = 0; OB_SUCC(ret) && i < work_count; ++i) {
      ObDASFusionPathSliceWork *work =
          parallel_ctx_.get_path_slice_work(i);
      if (OB_ISNULL(work)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fusion path slice work is null", KR(ret), K(i));
      } else if (path_work_counts.at(work->get_path_id()) > 1) {
        const ObDASFusionWorkSlice &work_slice =
            work->get_work_slice();
        const int64_t path_top_k_limit =
            work_slice.path_top_k_limit_;
        if (path_top_k_limit_initialized.at(work->get_path_id())
            && path_top_k_limits.at(work->get_path_id())
                   != path_top_k_limit) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partitioned path top k limit is inconsistent",
                   KR(ret), K(i),
                   "path_idx", work->get_path_id(),
                   K(path_top_k_limit),
                   "expected_top_k_limit",
                   path_top_k_limits.at(work->get_path_id()));
        } else {
          path_top_k_limits.at(work->get_path_id()) =
              path_top_k_limit;
          path_top_k_limit_initialized.at(work->get_path_id()) = true;
        }
        common::ObSEArray<ObDASFusionMaterializedRow, 16> &bucket =
            path_rows_buckets.at(work->get_path_id());
        const common::ObSEArray<ObDASFusionMaterializedRow, 256> &rows =
            work->get_rows();
        for (int64_t row_idx = 0;
             OB_SUCC(ret) && row_idx < rows.count();
             ++row_idx) {
          if (OB_FAIL(bucket.push_back(rows.at(row_idx)))) {
            LOG_WARN("failed to collect partitioned path row",
                     KR(ret), K(i), K(row_idx),
                     "path_idx", work->get_path_id());
          }
        }
      } else if (OB_FAIL(merge_parallel_work_rows(*work))) {
        LOG_WARN("failed to merge parallel work rows",
                 KR(ret), K(i),
                 "path_idx", work->get_path_id());
      }
    }
    // 每条被切分的逻辑路径只输出一次。
    for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
      if (path_work_counts.at(path_idx) > 1) {
        if (OB_FAIL(merge_partitioned_path_rows(
                path_idx, path_top_k_limits.at(path_idx),
                path_rows_buckets.at(path_idx)))) {
          LOG_WARN("failed to merge partitioned path rows",
                   KR(ret), K(path_idx));
        }
      }
    }
  }
  return ret;
}

void ObDASFusionIter::release_parallel_runtime()
{
  if (parallel_coordinator_.is_inited()) {
    int wait_ret = parallel_coordinator_.wait_all_complete_barrier();
    if (OB_SUCCESS != wait_ret) {
      LOG_WARN_RET(wait_ret, "failed to wait all fusion work unit tasks during release");
    }
    parallel_coordinator_.reset();
  }

  parallel_ctx_.release();
}

int ObDASFusionIter::create_parallel_task_profiles()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *fusion_profile =
      common::get_current_profile<common::ObMetric>();
  if (OB_ISNULL(fusion_profile)) {
    // Profile disabled, nothing to do.
  } else {
    // Create per-task child profiles on the ExecContext's lazily-created
    // thread-safe arena (ObSafeArenaAllocator wrapping mem_context_'s arena).
    // Worker-thread allocations (register_child / register_metric) are
    // serialized by the spinlock; the arena lives until ~ObExecContext, so
    // child profiles stay valid through submit_op_monitor_node.
    common::ObSafeArenaAllocator *safe_alloc = nullptr;
    if (OB_FAIL(exec_ctx_->get_safe_profile_allocator(safe_alloc))) {
      LOG_WARN("failed to get safe profile allocator", KR(ret));
    } else if (OB_ISNULL(safe_alloc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("safe profile allocator is null", KR(ret));
    } else {
      const int64_t work_unit_count = parallel_ctx_.get_work_unit_count();
      SET_METRIC_VAL(
          common::ObMetricId::HS_PARALLEL_TASK_COUNT,
          work_unit_count);
      for (int64_t i = 0; OB_SUCC(ret) && i < work_unit_count; ++i) {
        ObDASFusionWorkUnit *work_unit = parallel_ctx_.get_work_unit(i);
        if (OB_NOT_NULL(work_unit)
            && OB_NOT_NULL(work_unit->work_)) {
          void *buf = safe_alloc->alloc(sizeof(common::ObOpProfile<common::ObMetric>));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc per-task child profile", KR(ret), K(i));
          } else {
            common::ObOpProfile<common::ObMetric> *child_profile =
                new (buf) common::ObOpProfile<common::ObMetric>(
                    common::ObProfileId::HYBRID_SEARCH_PARALLEL_TASK, safe_alloc);
            if (OB_FAIL(fusion_profile->adopt_child(child_profile))) {
              LOG_WARN("failed to adopt per-task child profile", KR(ret), K(i));
            } else {
              work_unit->profile_ = child_profile;
              const ObDASFusionPathAdapter *path_adapter =
                  parallel_ctx_.get_path_adapter(work_unit->path_id_);
              if (OB_ISNULL(path_adapter)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fusion path adapter is null when creating profile",
                         KR(ret), K(i), KPC(work_unit));
              } else {
                common::ObProfileSwitcher switcher(child_profile);
                SET_METRIC_VAL(
                    common::ObMetricId::HS_PARALLEL_WORK_UNIT_ID,
                    work_unit->unit_id_);
                SET_METRIC_VAL(
                    common::ObMetricId::HS_PARALLEL_PATH_ID,
                    work_unit->path_id_);
                SET_METRIC_VAL(
                    common::ObMetricId::HS_PARALLEL_PATH_TYPE,
                    static_cast<int64_t>(
                        path_adapter->get_logical_path().type_));
                SET_METRIC_VAL(
                    common::ObMetricId::HS_PARALLEL_WORK_KIND,
                    static_cast<int64_t>(work_unit->kind_));
                SET_METRIC_VAL(
                    common::ObMetricId::HS_OMT_QUEUE_TIME,
                    0);
                SET_METRIC_VAL(
                    common::ObMetricId::HS_TOTAL_TIME,
                    0);
              }
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fusion work unit or work is null when creating profile",
                   KR(ret), K(i), KP(work_unit));
        }
      }
    }
  }
  return ret;
}

int ObDASFusionIter::do_fusion(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fusion_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion_ctdef is null", K(ret));
  } else if (parallel_execution_used_) {
    {
      common::ScopedTimer merge_timer(
          common::ObMetricId::HS_PARALLEL_RESULT_MERGE_TIME);
      if (OB_FAIL(merge_parallel_results())) {
        LOG_WARN("failed to merge parallel fusion results", KR(ret));
      }
    }
  } else if (OB_LIKELY(is_vectorized)) {
    for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < children_cnt_; ++path_idx) {
      ObDASIter *child = children_[path_idx];
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret), K(path_idx));
      } else {
        while (OB_SUCC(ret)) {
          child->clear_evaluated_flag();
          int64_t read_count = 0;
          if (OB_FAIL(child->get_next_rows(read_count, max_size_))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next rows from child", K(ret), K(path_idx));
            } else if (read_count > 0) {
              if (OB_FAIL(add_batch(path_idx, fusion_row_, read_count))) {
                LOG_WARN("failed to add batch", K(ret), K(path_idx), K(read_count));
              } else {
                ret = OB_SUCCESS;
                break;
              }
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(add_batch(path_idx, fusion_row_, read_count))) {
            LOG_WARN("failed to add batch", K(ret), K(path_idx), K(read_count));
          }
        }
      }
    }
  } else {
    for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < children_cnt_; ++path_idx) {
      ObDASIter *child = children_[path_idx];
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret), K(path_idx));
      } else {
        while (OB_SUCC(ret)) {
          child->clear_evaluated_flag();
          if (OB_FAIL(child->get_next_row())) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("failed to get next row from child", K(ret), K(path_idx));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(add_row(path_idx, fusion_row_))) {
            LOG_WARN("failed to add row", K(ret), K(path_idx));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    {
      common::ScopedTimer finalize_timer(
          common::ObMetricId::HS_FUSION_FINALIZE_TIME);
      if (OB_FAIL(finish_fusion())) {
        LOG_WARN("failed to finish fusion", K(ret));
      } else {
        fusion_finished_ = true;
      }
    }
  }

  return ret;
}

void ObDASFusionIter::free_fusion_score_heap()
{
  if (OB_NOT_NULL(fusion_score_heap_)) {
    fusion_score_heap_->reset();
    fusion_score_heap_->~ObDASFusionScoreHeap();
    fusion_score_heap_ = nullptr;
  }
}

int ObDASFusionIter::init_rowid_map()
{
  int ret = OB_SUCCESS;
  if (rowid_map_inited_ || uint64_rowid_map_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowid_map already initialized, should not call init_rowid_map again", K(ret));
  } else {
    const int64_t default_bucket_cnt = rank_window_size_ * children_cnt_ > 0 ? rank_window_size_ * children_cnt_ : 1;
    const int64_t estimated_bucket_cnt = expected_doc_count_ > INT64_MAX / MAP_BUCKET_RATIO
                                           ? INT64_MAX
                                           : expected_doc_count_ * MAP_BUCKET_RATIO;
    const int64_t bucket_cnt = OB_MIN(OB_MAX(estimated_bucket_cnt, default_bucket_cnt),
                                       MAX_ROWID_MAP_BUCKET_CNT);
    if (rowkey_is_uint64_) {
      if (OB_FAIL(uint64_rowid_doc_map_.create(bucket_cnt,
                                                lib::ObLabel("DASFusU64Map"),
                                                lib::ObLabel("DASFusU64Map"),
                                                MTL_ID()))) {
        LOG_WARN("failed to create uint64_rowid_doc_map", K(ret), K(bucket_cnt));
      } else {
        uint64_rowid_map_inited_ = true;
      }
    } else {
      // General path: use ObRowkey hash map
      if (OB_FAIL(rowid_doc_map_.create(bucket_cnt,
                                        lib::ObLabel("DASFusionMap"),
                                        lib::ObLabel("DASFusionMap"),
                                        MTL_ID()))) {
        LOG_WARN("failed to create rowid_doc_map", K(ret), K(bucket_cnt));
      } else {
        rowid_map_inited_ = true;
      }
    }
  }
  return ret;
}

int ObDASFusionIter::init_fusion_row()
{
  int ret = OB_SUCCESS;

  if (fusion_row_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion_row_ is already initialized", K(ret));
  } else if (OB_ISNULL(fusion_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion_ctdef_ is null", K(ret));
  } else if (OB_FAIL(fusion_row_.assign(fusion_ctdef_->result_output_))) {
    LOG_WARN("failed to assign fusion result output to fusion_row", K(ret));
  } else {
    fusion_row_inited_ = true;
  }

  return ret;
}

void ObDASFusionIter::destroy_rowid_map()
{
  if (rowid_map_inited_) {
    rowid_doc_map_.destroy();
    rowid_map_inited_ = false;
  }
  if (uint64_rowid_map_inited_) {
    uint64_rowid_doc_map_.destroy();
    uint64_rowid_map_inited_ = false;
  }
}

int ObDASFusionIter::extract_score(const int64_t path_idx, double &score)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  if (OB_ISNULL(score_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score_exprs_ is null", K(ret));
  } else if (OB_UNLIKELY(path_idx < 0 || path_idx >= score_exprs_->count() - 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid path_idx", K(ret), K(path_idx), K(score_exprs_->count()));
  } else if (OB_FAIL(score_exprs_->at(path_idx)->eval(*eval_ctx_, datum))) {
    LOG_WARN("failed to eval score expr", K(ret), K(path_idx));
  } else if (OB_ISNULL(datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum is null", K(ret));
  } else if (datum->is_null()) {
    score = 0.0;
  } else {
    score = datum->get_double();
  }
  return ret;
}

int ObDASFusionIter::add_row(const int64_t path_idx, 
                             const common::ObIArray<ObExpr *> &exprs)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_sorted_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot add row after sorting", K(ret));
  } else if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval_ctx_ is null", K(ret));
  } else if (rowkey_is_uint64_) {
    uint64_t rowkey_val = 0;
    double score = 0.0;
    if (OB_FAIL(extract_rowkey(rowkey_val))) {
      LOG_WARN("failed to extract rowkey", K(ret));
    } else if (OB_FAIL(extract_score(path_idx, score))) {
        LOG_WARN("failed to extract score", K(ret), K(path_idx));
    } else if (OB_FAIL(find_or_create_doc(rowkey_val, path_idx, score))) {
      LOG_WARN("failed to find or create doc", K(ret), K(rowkey_val));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("it is not supported to add row with non-uint64 rowkey", K(ret));
  }
  return ret;
}

int ObDASFusionIter::add_batch(const int64_t path_idx,
                               const common::ObIArray<ObExpr *> &exprs, 
                               const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &rowid_exprs = fusion_ctdef_->rowid_exprs_;
  ObExpr *score_expr = score_exprs_->at(path_idx);
  if (OB_UNLIKELY(is_sorted_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot add batch after sorting", K(ret));
  } else if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval_ctx_ is null", K(ret));
  } else if (score_expr->enable_rich_format() &&
             is_valid_format(score_expr->get_format(*eval_ctx_))) {
    ObIVector *rowid_vec = rowid_exprs.at(0)->get_vector(*eval_ctx_);
    ObIVector *score_vec = score_expr->get_vector(*eval_ctx_);
    if (rowkey_is_uint64_) {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_size; ++row_idx) {
        double score = score_vec->get_double(row_idx);
        uint64_t rowkey_val = rowid_vec->get_uint(row_idx);
        int64_t doc_idx = -1;
        if (OB_FAIL(find_or_create_doc(rowkey_val, path_idx, score))) {
          LOG_WARN("failed to find or create doc (uint64)", K(ret), K(rowkey_val), K(row_idx));
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("it is not supported to add batch with non-uint64 rowkey now.", K(ret));
    }
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_size(batch_size);
    ObDatum *rowkey_datum = rowid_exprs.at(0)->locate_batch_datums(*eval_ctx_);
    ObDatum *score_datum = score_expr->locate_batch_datums(*eval_ctx_);
    if (rowkey_is_uint64_) {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < batch_size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        double score = score_datum[row_idx].get_double();
        uint64_t rowkey_val = rowkey_datum[row_idx].get_uint64();
        if (OB_FAIL(find_or_create_doc(rowkey_val, path_idx, score))) {
          LOG_WARN("failed to find or create doc (uint64)", K(ret), K(rowkey_val), K(row_idx));
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("it is not supported to add batch with non-uint64 rowkey now.", K(ret));
    }
  }
  return ret;
}

int ObDASFusionIter::set_score_to_doc(int64_t doc_idx, double score, int64_t path_idx)
{
  int ret = OB_SUCCESS;
  if (doc_idx < 0 || doc_idx >= fusion_docs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid doc_idx", K(ret), K(doc_idx), K(fusion_docs_.count()));
  } else {
    ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
    if (OB_FAIL(doc.set_raw_score(path_idx, score))) {
      LOG_WARN("failed to set raw score", K(ret), K(path_idx), K(score));
    } else {
      doc.set_path(path_idx);
    }
  }
  return ret;
}

int ObDASFusionIter::finish_fusion()
{
  int ret = OB_SUCCESS;
  const bool need_sort_topk = fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS
                              && need_sorted();
  if (OB_UNLIKELY(is_sorted_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already sorted", K(ret));
  } else if (need_sort_topk) {
    if (OB_FAIL(calculate_fusion_score())) {
      LOG_WARN("failed to calculate fusion score", K(ret));
    } else if (OB_FAIL(build_topk_heap())) {
      LOG_WARN("failed to build topk heap", K(ret));
    } else if (OB_FAIL(sort_topk_results())) {
      LOG_WARN("failed to sort topk results", K(ret));
    }
  } else if (fusion_iter_exec_mode_ == ObFusionIterExecMode::ROWKEY_SCORE_FULL_RECALL) {
    if (OB_FAIL(calculate_fusion_score())) {
      LOG_WARN("failed to calculate fusion score", K(ret));
    } else if (OB_FAIL(build_all_path_topk_doc_indices())) {
      LOG_WARN("failed to build all path topk doc indices", K(ret));
    }
  } else {
    if (OB_FAIL(build_all_path_topk_doc_indices())) {
      LOG_WARN("failed to build all path topk doc indices", K(ret));
    }
  }
  
  if (OB_SUCC(ret)) {
    if (!need_sort_topk) {
      SET_METRIC_VAL(common::ObMetricId::HS_FUSION_INPUT_ROW_COUNT,
                     static_cast<uint64_t>(sorted_doc_indices_.count()));
    }
    is_sorted_ = true;
    output_idx_ = 0;
  }
  return ret;
}

int ObDASFusionIter::calculate_fusion_score()
{
  int ret = OB_SUCCESS;
  ObFusionMethod fusion_method = fusion_method_;
  if (fusion_method == ObFusionMethod::WEIGHT_SUM) {
    if (OB_FAIL(calculate_weight_sum_score())) {
      LOG_WARN("failed to calculate weight sum score", K(ret));
    }
  } else if (fusion_method == ObFusionMethod::RRF) {
    if (OB_FAIL(calculate_rrf_score())) {
      LOG_WARN("failed to calculate rrf score", K(ret));
    }
  } else if (fusion_method == ObFusionMethod::MINMAX_NORMALIZER) {
    if (OB_FAIL(calculate_minmax_normalizer_score())) {
      LOG_WARN("failed to calculate minmax normalizer score", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("calculate_fusion_score for unsupported fusion method", K(ret), K(fusion_method));
  }
  return ret;
}

int ObDASFusionIter::calculate_weight_sum_score()
{
  int ret = OB_SUCCESS;
  const int64_t path_count = fusion_ctdef_->children_cnt_;
  const int64_t doc_count = fusion_docs_.count();
  const common::ObSEArray<double, 2> &weights = weights_;
  for (int64_t doc_idx = 0; OB_SUCC(ret) && doc_idx < doc_count; ++doc_idx) {
    ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
    doc.fusion_score_ = 0.0;
    for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
      if (!doc.has_path(path_idx)) {
        continue;
      }
      double score = 0.0;
      if (OB_FAIL(doc.get_raw_score(path_idx, score))) {
        LOG_WARN("failed to get raw score", K(ret), K(path_idx));
      } else {
        double weight = weights.at(path_idx);
        double weighted_score = score * weight;
        doc.fusion_score_ += weighted_score;
      }
    }
  }
  return ret;
}

int ObDASFusionIter::calculate_rrf_score()
{
  int ret = OB_SUCCESS;
  const int64_t path_count = fusion_ctdef_->children_cnt_;
  const int64_t doc_count = fusion_docs_.count();
  const int64_t rank_constant = rank_constant_;

  // Step 1: For each path, sort documents by score and assign ranks
  // Use a temporary array of (doc_idx, score) pairs for sorting
  common::ObSEArray<ObDASFusionScoreEntry, 64> doc_score_pairs;
  if (OB_UNLIKELY(weights_.count() != path_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("weights count not equal to path_count", K(ret), K(weights_.count()), K(path_count));
  } else if (OB_FAIL(doc_score_pairs.reserve(doc_count))) {
    LOG_WARN("failed to reserve doc_score_pairs", K(ret), K(doc_count));
  }
  for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
    doc_score_pairs.reset();
    for (int64_t doc_idx = 0; OB_SUCC(ret) && doc_idx < doc_count; ++doc_idx) {
      ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
      double score = 0.0;
      if (doc.has_path(path_idx)) {
        if (OB_FAIL(doc.get_raw_score(path_idx, score))) {
          LOG_WARN("failed to get raw score", K(ret), K(path_idx), K(doc_idx));
        } else if (OB_FAIL(doc_score_pairs.push_back(ObDASFusionScoreEntry(doc_idx, score)))) {
          LOG_WARN("failed to push back doc_score_pair", K(ret));
        }
      }
    }
    
    // Sort by score (descending)
    if (OB_SUCC(ret) && doc_score_pairs.count() > 0) {
      lib::ob_sort(doc_score_pairs.get_data(), 
                   doc_score_pairs.get_data() + doc_score_pairs.count());
      // Assign ranks (rank starts from 1)
      // Same score should have the same rank
      double last_score = 0.0;
      int64_t current_rank = 0;
      bool is_first = true;
      for (int64_t i = 0; i < doc_score_pairs.count(); ++i) {
        const ObDASFusionScoreEntry &entry = doc_score_pairs.at(i);
        if (is_first || entry.score_ != last_score) {
          current_rank = i + 1;
          last_score = entry.score_;
          is_first = false;
        }
        int64_t doc_idx = entry.doc_idx_;
        ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
        doc.rank_scores.at(path_idx) = current_rank;
      }
    }
  }
  
  // Step 2: Calculate RRF score for each document
  // RRF score = sum(weight_i * 1 / (k + rank_i)) for all paths where document exists
  for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
    double weight = weights_.at(path_idx);
    for (int64_t doc_idx = 0; OB_SUCC(ret) && doc_idx < doc_count; ++doc_idx) {
      ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
      if (doc.has_path(path_idx)) {
        int64_t rank = doc.rank_scores.at(path_idx);
        if (rank <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rank is less than 0", K(ret), K(rank));
        } else {
          double rrf_contribution = 1.0 / (rank_constant + static_cast<double>(rank));
          doc.fusion_score_ += weight * rrf_contribution;
        }
      }
    }
  }
  
  return ret;
}

int ObDASFusionIter::build_topk_heap()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_fusion_score_heap())) {
    LOG_WARN("failed to init fusion_score_heap", K(ret));
  } else {
    for (int64_t doc_idx = 0; OB_SUCC(ret) && doc_idx < fusion_docs_.count(); ++doc_idx) {
      const ObDASFusionDocInfo &doc_info = fusion_docs_.at(doc_idx);
      double fusion_score = doc_info.fusion_score_;
      ObDASFusionScoreEntry new_entry(doc_idx, fusion_score);

      if (fusion_score_heap_->count() < rank_window_size_) {
        if (OB_FAIL(fusion_score_heap_->push(new_entry))) {
          LOG_WARN("failed to push to heap", K(ret), K(doc_idx), K(fusion_score));
        }
      } else {
        ObDASFusionScoreEntry top_entry = fusion_score_heap_->top();
        if (heap_cmp_(new_entry, top_entry)) {
          if (OB_FAIL(fusion_score_heap_->replace_top(new_entry))) {
            LOG_WARN("failed to replace top in heap", K(ret), K(doc_idx), K(fusion_score));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASFusionIter::init_fusion_score_heap()
{
  int ret = OB_SUCCESS;
  heap_cmp_ = ObDASFusionScoreMinHeapCmp(&fusion_docs_, rowkey_is_uint64_);

  if (OB_NOT_NULL(fusion_score_heap_)) {
    fusion_score_heap_->reset();
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(fusion_memctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fusion_memctx is null", K(ret));
    } else if (OB_ISNULL(buf = fusion_memctx_->get_arena_allocator().alloc(sizeof(ObDASFusionScoreHeap)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for fusion_score_heap", K(ret));
    } else {
      fusion_score_heap_ = new (buf) ObDASFusionScoreHeap(heap_cmp_,
                                                          &fusion_memctx_->get_arena_allocator());
    }
  }

  return ret;
}

int ObDASFusionIter::sort_topk_results()
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(fusion_score_heap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion_score_heap is null", K(ret));
  } else {
    // Reserve capacity after reset (reset() clears capacity)
    const int64_t heap_size = fusion_score_heap_->count();
    // Pop from heap to get sorted indices (ascending order)
    while (OB_SUCC(ret) && fusion_score_heap_->count() > 0) {
      ObDASFusionScoreEntry entry = fusion_score_heap_->top();
      if (OB_FAIL(fusion_score_heap_->pop())) {
        LOG_WARN("failed to pop from heap", K(ret));
      } else if (entry.doc_idx_ < 0 || entry.doc_idx_ >= fusion_docs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid doc_idx", K(ret), K(entry.doc_idx_), K(fusion_docs_.count()));
      } else if (!has_rerank() && entry.score_ < min_score_) {
        // do nothing
      } else if (OB_FAIL(sorted_doc_indices_.push_back(entry.doc_idx_))) {
        LOG_WARN("failed to push doc index", K(ret), K(entry.doc_idx_));
      }
    }
  }
  // sorted_doc_indices_ is in ascending score order; convert to descending and apply offset/limit
  if (OB_SUCC(ret)) {
    SET_METRIC_VAL(common::ObMetricId::HS_FUSION_INPUT_ROW_COUNT,
                   static_cast<uint64_t>(sorted_doc_indices_.count()));
    bool need_trim = !has_rerank() && is_single_partition_ && fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS;
    // 1) Skip first offset_ in final order: drop the last offset_ elements (highest in ascending)
    if (need_trim) {
      const int64_t to_trim = OB_MIN(offset_, sorted_doc_indices_.count());
      for (int64_t i = 0; i < to_trim; ++i) {
        sorted_doc_indices_.pop_back();
      }
    }
    // 2) Reverse to descending order (highest score first)
    const int64_t count = sorted_doc_indices_.count();
    for (int64_t i = 0; i < count / 2; ++i) {
      const int64_t j = count - 1 - i;
      int64_t tmp = sorted_doc_indices_[i];
      sorted_doc_indices_[i] = sorted_doc_indices_[j];
      sorted_doc_indices_[j] = tmp;
    }
    // 3) Cap result size: keep only top N (size_ for normal; rerank_window_size_ for rerank)
    if (is_single_partition_ && fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS) {
      int64_t cap = has_rerank() ? rerank_window_size_ : size_;
      while (sorted_doc_indices_.count() > cap) {
        sorted_doc_indices_.pop_back();
      }
    }
  }
  
  return ret;
}

template<typename RowkeyType>
int ObDASFusionIter::set_rowkey_to_exprs(const RowkeyType &rowkey, const ObIArray<ObExpr *> &rowkey_exprs)
{
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

template<>
int ObDASFusionIter::set_rowkey_to_exprs<uint64_t>(const uint64_t &rowkey, const ObIArray<ObExpr *> &rowkey_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey_exprs.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uint64 rowkey should have exactly one expr", K(ret), K(rowkey_exprs.count()));
  } else {
    ObExpr *rowkey_expr = rowkey_exprs.at(0);
    ObDatum &rowkey_datum = rowkey_expr->locate_datum_for_write(*eval_ctx_);
    rowkey_datum.set_uint(rowkey);
  }
  return ret;
}

template<typename RowkeyType>
int ObDASFusionIter::set_rowkeys_batch_non_rich_format(int64_t start_idx, 
                                                        int64_t batch_size)
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("set_rowkeys_batch_non_rich_format not implemented for this rowkey type", K(ret));
  return ret;
}

template<>
int ObDASFusionIter::set_rowkeys_batch_non_rich_format<uint64_t>(int64_t start_idx, 
                                                                 int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &rowkey_exprs = fusion_ctdef_->rowid_exprs_;
  
  if (OB_UNLIKELY(rowkey_exprs.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("uint64 rowkey should have exactly one expr", K(ret), K(rowkey_exprs.count()));
  } else {
    ObExpr *rowkey_expr = rowkey_exprs.at(0);
    if (OB_ISNULL(rowkey_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey_expr is null", K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(batch_size);
      ObDatum *rowkey_datums = rowkey_expr->locate_datums_for_update(*eval_ctx_, batch_size);
      
      if (OB_ISNULL(rowkey_datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey datums is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
          int64_t doc_idx = sorted_doc_indices_.at(start_idx + i);
          if (doc_idx < 0 || doc_idx >= fusion_docs_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid doc_idx", K(ret), K(doc_idx), K(fusion_docs_.count()));
          } else {
            const ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
            if (OB_UNLIKELY(!doc.is_uint64())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("doc rowkey is not uint64", K(ret), K(doc_idx));
            } else {
              uint64_t rowkey_val = doc.get_uint64_rowkey();
              rowkey_datums[i].set_uint(rowkey_val);
              // SQL pagination uses this matching row's physical key after main-table lookup.
              if (search_ctx_ != nullptr && search_ctx_->has_shard_doc()) {
                if (OB_FAIL(search_ctx_->project_shard_doc(rowkey_val, i, batch_size))) {
                  LOG_WARN("failed to project hybrid search __shard_doc", K(ret));
                }
              }
            }
          }
        }
        
        if (OB_SUCC(ret)) {
          rowkey_expr->set_evaluated_projected(*eval_ctx_);
        }
      }
    }
  }
  
  return ret;
}

int ObDASFusionIter::set_rowkey_and_score_to_exprs_batch(int64_t start_idx, 
                                                         int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &score_expr = fusion_ctdef_->get_score_exprs();
  bool use_rich_format = score_expr.at(0)->enable_rich_format();
  
  if (use_rich_format) {
    if (rowkey_is_uint64_) {
      if (OB_FAIL(set_rowkeys_batch_rich_format<uint64_t>(start_idx, batch_size))) {
        LOG_WARN("failed to set rowkeys batch rich format uint64", K(ret));
      }
    } else {
      if (OB_FAIL(set_rowkeys_batch_rich_format<common::ObRowkey>(start_idx, batch_size))) {
        LOG_WARN("failed to set rowkeys batch rich format", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_scores_batch_rich_format(start_idx, batch_size))) {
        LOG_WARN("failed to set scores batch rich format", K(ret));
      }
    }
  } else {
    if (rowkey_is_uint64_) {
      if (OB_FAIL(set_rowkeys_batch_non_rich_format<uint64_t>(start_idx, batch_size))) {
        LOG_WARN("failed to set rowkeys batch rich format uint64", K(ret));
      }
    } else {
      if (OB_FAIL(set_rowkeys_batch_non_rich_format<common::ObRowkey>(start_idx, batch_size))) {
        LOG_WARN("failed to set rowkeys batch non-rich format", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_scores_batch_non_rich_format(start_idx, batch_size))) {
        LOG_WARN("failed to set scores batch non-rich format", K(ret));
      }
    }
  }
  
  return ret;
}

template<typename RowkeyType>
int ObDASFusionIter::set_rowkeys_batch_rich_format(int64_t start_idx, 
                                                   int64_t batch_size)
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("set_rowkeys_batch_rich_format not implemented for this rowkey type", K(ret));
  return ret;
}

template<>
int ObDASFusionIter::set_rowkeys_batch_rich_format<uint64_t>(int64_t start_idx, 
                                                             int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &rowkey_exprs = fusion_ctdef_->rowid_exprs_;
  
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); ++i) {
    if (OB_FAIL(rowkey_exprs.at(i)->init_vector_for_write(*eval_ctx_, rowkey_exprs.at(i)->get_default_res_format(), batch_size))) {
      LOG_WARN("failed to init vector for rowkey expr", K(ret), K(i));
    }
  }
  
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(rowkey_exprs.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("uint64 rowkey should have exactly one expr", K(ret), K(rowkey_exprs.count()));
    } else {
      ObExpr *rowkey_expr = rowkey_exprs.at(0);
      ObIVector *rowkey_vec = rowkey_expr->get_vector(*eval_ctx_);
      if (OB_ISNULL(rowkey_vec)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rowkey vector is null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
          int64_t doc_idx = sorted_doc_indices_.at(start_idx + i);
          if (doc_idx < 0 || doc_idx >= fusion_docs_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid doc_idx", K(ret), K(doc_idx), K(fusion_docs_.count()));
          } else {
            const ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
            if (OB_UNLIKELY(!doc.is_uint64())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("doc rowkey is not uint64", K(ret), K(doc_idx));
            } else {
              uint64_t rowkey_val = doc.get_uint64_rowkey();
              rowkey_vec->set_uint(i, rowkey_val);
              // SQL pagination uses this matching row's physical key after main-table lookup.
              if (search_ctx_ != nullptr && search_ctx_->has_shard_doc()) {
                if (OB_FAIL(search_ctx_->project_shard_doc(rowkey_val, i, batch_size))) {
                  LOG_WARN("failed to project hybrid search __shard_doc", K(ret));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          rowkey_expr->set_evaluated_projected(*eval_ctx_);
        }
      }
    }
  }
  
  return ret;
}

int ObDASFusionIter::set_scores_batch_rich_format(int64_t start_idx, 
                                                  int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &score_exprs = fusion_ctdef_->get_score_exprs();
  
  for (int64_t i = 0; OB_SUCC(ret) && i < score_exprs.count(); ++i) {
    if (OB_FAIL(score_exprs.at(i)->init_vector_for_write(*eval_ctx_, score_exprs.at(i)->get_default_res_format(), batch_size))) {
      LOG_WARN("failed to init vector for score expr", K(ret), K(i));
    }
  }
  
  if (OB_SUCC(ret)) {
    ObExpr *fusion_score_expr = score_exprs.at(score_exprs.count() - 1);
    ObIVector *fusion_score_vec = fusion_score_expr->get_vector(*eval_ctx_);
    
    if (OB_ISNULL(score_exprs_) || OB_ISNULL(fusion_score_expr) || OB_ISNULL(fusion_score_vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("score_exprs_ or fusion_score_expr or fusion_score_vec is null", K(ret));
    } else {
      int64_t path_count = score_exprs_->count() - 1;
      ObSEArray<ObIVector*, 4> path_score_vecs;
      for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
        ObExpr *score_expr = score_exprs_->at(path_idx);
        ObIVector *score_vec = score_expr->get_vector(*eval_ctx_);
        if (OB_ISNULL(score_vec)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("score vector is null", K(ret), K(path_idx));
        } else if (OB_FAIL(path_score_vecs.push_back(score_vec))) {
          LOG_WARN("failed to push back score vec", K(ret));
        }
      }
      
      // Set scores for each row in batch
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        int64_t doc_idx = sorted_doc_indices_.at(start_idx + i);
        if (doc_idx < 0 || doc_idx >= fusion_docs_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid doc_idx", K(ret), K(doc_idx), K(fusion_docs_.count()));
        } else {
          const ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
          // Set fusion score
          fusion_score_vec->set_double(i, doc.fusion_score_);
          // Set path scores
          for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
            if (!doc.has_path(path_idx)) {
              path_score_vecs.at(path_idx)->set_null(i);
            } else {
              double score = 0.0;
              if (OB_FAIL(doc.get_raw_score(path_idx, score))) {
                LOG_WARN("failed to get raw score", K(ret), K(path_idx));
              } else {
                path_score_vecs.at(path_idx)->set_double(i, score);
              }
            }
          }
        }
      }
      
      if (OB_SUCC(ret)) {
        fusion_score_expr->set_evaluated_projected(*eval_ctx_);
        for (int64_t path_idx = 0; path_idx < path_count; ++path_idx) {
          score_exprs_->at(path_idx)->set_evaluated_projected(*eval_ctx_);
        }
      }
    }
  }
  
  return ret;
}

int ObDASFusionIter::set_scores_batch_non_rich_format(int64_t start_idx, 
                                                       int64_t batch_size)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &score_exprs = fusion_ctdef_->get_score_exprs();
  ObExpr *fusion_score_expr = score_exprs.at(score_exprs.count() - 1);
  ObDatum *fusion_datums = nullptr;

  if (OB_ISNULL(score_exprs_) || OB_ISNULL(fusion_score_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score_exprs_ or fusion_score_expr is null", K(ret));
  } else if (OB_ISNULL(fusion_datums = fusion_score_expr->locate_datums_for_update(*eval_ctx_, batch_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to locate datums for fusion_score_expr", K(ret), K(fusion_score_expr));
  } else {
    int64_t path_count = score_exprs_->count() - 1;
    ObSEArray<ObDatum*, 4> path_datums;
    ObSEArray<ObExpr*, 4> path_score_exprs;
    for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
      ObExpr *score_expr = score_exprs_->at(path_idx);
      ObDatum *datums = score_expr->locate_datums_for_update(*eval_ctx_, batch_size);
      if (OB_ISNULL(datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to locate datums for score expr", K(ret), K(path_idx));
      } else if (OB_FAIL(path_datums.push_back(datums))) {
        LOG_WARN("failed to push back datums", K(ret));
      } else if (OB_FAIL(path_score_exprs.push_back(score_expr))) {
        LOG_WARN("failed to push back score expr", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      int64_t doc_idx = sorted_doc_indices_.at(start_idx + i);
      if (doc_idx < 0 || doc_idx >= fusion_docs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid doc_idx", K(ret), K(doc_idx), K(fusion_docs_.count()));
      } else {
        const ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
        fusion_datums[i].set_double(doc.fusion_score_);
        for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
          if (!doc.has_path(path_idx)) {
            path_datums.at(path_idx)[i].set_null();
          } else {
            double score = 0.0;
            if (OB_FAIL(doc.get_raw_score(path_idx, score))) {
              LOG_WARN("failed to get raw score", K(ret), K(path_idx));
            } else {
              path_datums.at(path_idx)[i].set_double(score);
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      fusion_score_expr->set_evaluated_projected(*eval_ctx_);
      for (int64_t path_idx = 0; path_idx < path_count; ++path_idx) {
        path_score_exprs.at(path_idx)->set_evaluated_projected(*eval_ctx_);
      }
    }
  }
  
  return ret;
}

int ObDASFusionIter::get_next_row_from_store(const ObIArray<ObExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  const int64_t total_count = sorted_doc_indices_.count();
  
  if (OB_UNLIKELY(!is_sorted_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must call finish_fusion before get_next_row_from_store", K(ret));
  } else if (output_idx_ >= total_count) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval_ctx is null", K(ret));
  } else {
    if (OB_FAIL(init_output_vectors(exprs, *eval_ctx_, 1))) {
      LOG_WARN("failed to init output vectors", K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(1);
      batch_info_guard.set_batch_idx(0);
      
      if (OB_FAIL(set_rowkey_and_score_to_exprs(sorted_doc_indices_.at(output_idx_)))) {
        LOG_WARN("failed to set rowkey and score to exprs", K(ret));
      } else {
        ++output_idx_;
      }
    }
  }

  return ret;
}

int ObDASFusionIter::get_next_batch_from_store(const ObIArray<ObExpr *> &exprs, int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  const int64_t total_count = sorted_doc_indices_.count();
  const int64_t batch_size = OB_MIN(capacity, total_count - output_idx_);

  if (OB_UNLIKELY(!is_sorted_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("must call finish_fusion before get_next_batch_from_store", K(ret));
  } else if (output_idx_ >= total_count) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval_ctx is null", K(ret));
  } else if (OB_FAIL(set_rowkey_and_score_to_exprs_batch(output_idx_, batch_size))) {
      LOG_WARN("failed to set rowkey and score to exprs batch", K(ret));
  } else {
    count = batch_size;
    output_idx_ += batch_size;
  }
    
  return ret;
}

int ObDASFusionIter::set_rowkey_and_score_to_exprs(int64_t doc_idx)
{
  int ret = OB_SUCCESS;
  if (doc_idx < 0 || doc_idx >= fusion_docs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid doc_idx", K(ret), K(doc_idx), K(fusion_docs_.count()));
  } else {
    const ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
    const ObIArray<ObExpr *> &rowkey_exprs = fusion_ctdef_->rowid_exprs_;
    if (doc.is_uint64()) {
      if (OB_FAIL(set_rowkey_to_exprs(doc.get_uint64_rowkey(), rowkey_exprs))) {
        LOG_WARN("failed to set rowkey to exprs", K(ret));
      } else if (OB_FAIL(set_score_to_expr(doc))) {
        LOG_WARN("failed to set score to expr", K(ret));
      } else if (search_ctx_ != nullptr && search_ctx_->has_shard_doc()
                 && OB_FAIL(search_ctx_->project_shard_doc(doc.get_uint64_rowkey(), 0, 1))) {
        LOG_WARN("failed to project hybrid search __shard_doc", K(ret));
      }
    } else {
      if (OB_FAIL(set_rowkey_to_exprs(doc.rowkey_union_.rowkey_, rowkey_exprs))) {
        LOG_WARN("failed to set rowkey to exprs", K(ret));
      } else if (OB_FAIL(set_score_to_expr(doc))) {
        LOG_WARN("failed to set score to expr", K(ret));
      }
    }
  }
  
  return ret;
}

int ObDASFusionIter::set_score_to_expr(const ObDASFusionDocInfo &doc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(score_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score_exprs_ is null", K(ret));
  } else {
    const ExprFixedArray &score_exprs = fusion_ctdef_->get_score_exprs();
    ObExpr *fusion_score_expr = score_exprs.at(score_exprs.count() - 1);
    ObDatum &fusion_datum = fusion_score_expr->locate_datum_for_write(*eval_ctx_);
    fusion_datum.set_double(doc.fusion_score_);
    int64_t path_count = children_cnt_;
    for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
      ObExpr *score_expr = score_exprs_->at(path_idx);
      ObDatum &datum = score_expr->locate_datum_for_write(*eval_ctx_);
      if (!doc.has_path(path_idx)) {
        datum.set_null();
      } else {
        double score = 0.0;
        if (OB_FAIL(doc.get_raw_score(path_idx, score))) {
          LOG_WARN("failed to get raw score", K(ret), K(path_idx));
        } else {
          datum.set_double(score);
        }
      }
    }
    
    if (OB_SUCC(ret)) {
      fusion_score_expr->set_evaluated_projected(*eval_ctx_);
      for (int64_t path_idx = 0; path_idx < path_count; ++path_idx) {
        score_exprs_->at(path_idx)->set_evaluated_projected(*eval_ctx_);
      }
    }
  }
  return ret;
}

int ObDASFusionIter::calculate_minmax_normalizer_score()
{
  int ret = OB_SUCCESS;
  const int64_t path_count = fusion_ctdef_->children_cnt_;
  const int64_t doc_count = fusion_docs_.count();
  // Step 1: For each path, find min and max scores
  common::ObSEArray<double, 4> path_min_scores;
  common::ObSEArray<double, 4> path_max_scores;
  if (OB_FAIL(path_min_scores.prepare_allocate(path_count))) {
    LOG_WARN("failed to prepare allocate path_min_scores", K(ret), K(path_count));
  } else if (OB_FAIL(path_max_scores.prepare_allocate(path_count))) {
    LOG_WARN("failed to prepare allocate path_max_scores", K(ret), K(path_count));
  } else {
    // Initialize min to max double, max to min double
    for (int64_t path_idx = 0; path_idx < path_count; ++path_idx) {
      path_min_scores.at(path_idx) = DBL_MAX;
      path_max_scores.at(path_idx) = -DBL_MAX;
    }
    
    // Find min and max for each path
    for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
      for (int64_t doc_idx = 0; OB_SUCC(ret) && doc_idx < doc_count; ++doc_idx) {
        const ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
        if (doc.has_path(path_idx)) {
          double score = 0.0;
          if (OB_FAIL(doc.get_raw_score(path_idx, score))) {
            LOG_WARN("failed to get raw score", K(ret), K(path_idx), K(doc_idx));
          } else {
            if (score < path_min_scores.at(path_idx)) {
              path_min_scores.at(path_idx) = score;
            }
            if (score > path_max_scores.at(path_idx)) {
              path_max_scores.at(path_idx) = score;
            }
          }
        }
      }
    }
    static const double EPSILON = 1e-6;
    
    // Step 2: Normalize scores and calculate fusion score
    // Process each path, then accumulate weighted normalized scores for all documents
    for (int64_t path_idx = 0; OB_SUCC(ret) && path_idx < path_count; ++path_idx) {
      double min_score = path_min_scores.at(path_idx);
      double max_score = path_max_scores.at(path_idx);
      bool min_equal_max = (max_score - min_score) < EPSILON;
      double weight = weights_.at(path_idx);
      for (int64_t doc_idx = 0; OB_SUCC(ret) && doc_idx < doc_count; ++doc_idx) {
        ObDASFusionDocInfo &doc = fusion_docs_.at(doc_idx);
        if (doc.has_path(path_idx)) {
          double raw_score = 0.0;
          if (OB_FAIL(doc.get_raw_score(path_idx, raw_score))) {
            LOG_WARN("failed to get raw score", K(ret), K(path_idx), K(doc_idx));
          } else {
            double normalized_score = 0.0;
            if (min_equal_max) {
              normalized_score = 1.0;
            } else {
              normalized_score = (raw_score - min_score) / (max_score - min_score);
            }
            double weighted_score = normalized_score * weight;
            doc.fusion_score_ += weighted_score;
          }
        }
      }
    }
  }
  
  return ret;
}

void ObDASFusionIter::clear_evaluated_flag()
{
  for (int64_t i = 0; i < children_cnt_; ++i) {
    if (OB_NOT_NULL(children_[i])) {
      children_[i]->clear_evaluated_flag();
    }
  }
}

int ObDASFusionDocInfo::init_raw_scores(int64_t path_count, common::ObArenaAllocator &allocator, ObFusionMethod fusion_method)
{
  int ret = OB_SUCCESS;
  raw_scores_.set_allocator(&allocator);
  if (OB_FAIL(raw_scores_.prepare_allocate(path_count))) {
    LOG_WARN("failed to prepare allocate raw_scores_", K(ret), K(path_count));
  } else {
    for (int64_t i = 0; i < path_count; ++i) {
      raw_scores_.at(i) = 0.0;
    }
    if (fusion_method == ObFusionMethod::RRF) {
      rank_scores.set_allocator(&allocator);
      if (OB_FAIL(rank_scores.prepare_allocate(path_count))) {
        LOG_WARN("failed to prepare allocate rank_scores", K(ret), K(path_count));
      } else {
        for (int64_t i = 0; i < path_count; ++i) {
          rank_scores.at(i) = 0;
        }
      }
    }
  }
  return ret;
}

int ObDASFusionDocInfo::get_raw_score(int64_t path_idx, double &score) const
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (OB_UNLIKELY(path_idx < 0 || path_idx >= raw_scores_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path_idx out of range", K(ret), K(path_idx), K(raw_scores_.count()));
  } else {
    score = raw_scores_.at(path_idx);
  }
  return ret;
}

int ObDASFusionDocInfo::set_raw_score(int64_t path_idx, double score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(path_idx < 0 || path_idx >= raw_scores_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path_idx out of range", K(ret), K(path_idx), K(raw_scores_.count()));
  } else {
    raw_scores_.at(path_idx) = score;
  }
  return ret;
}

int ObDASFusionIter::init_output_vectors(
    const common::ObIArray<ObExpr *> &exprs,
    ObEvalCtx &ctx,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObExpr *e = exprs.at(i);
    if (OB_ISNULL(e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i));
    } else if (e->is_const_expr()) {
      continue;
    } else {
      VectorFormat format = e->get_format(ctx);
      if (VEC_INVALID == format) {
        if (OB_FAIL(e->init_vector_default(ctx, size))) {
          LOG_WARN("init output vector failed", K(ret), K(i), K(size));
        }
      }
    }
  }
  return ret;
}

int ObDASFusionIter::set_rowkey_is_uint64_flag()
{
  int ret = OB_SUCCESS;
  rowkey_is_uint64_ = false;
  if (OB_ISNULL(fusion_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fusion_ctdef_ is null", K(ret));
  } else {
    const ObIArray<ObExpr*> &rowid_exprs = fusion_ctdef_->rowid_exprs_;
    if (OB_UNLIKELY(rowid_exprs.count() != 1)) {
      // do nothing
    } else {
      const ObExpr *expr = rowid_exprs.at(0);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_LIKELY(expr->obj_meta_.is_unsigned_integer())) {
        rowkey_is_uint64_ = true;
      }
    }
  }
  return ret;
}

int ObDASFusionIter::build_all_path_topk_doc_indices()
{
  int ret = OB_SUCCESS;
  bool need_filter_min_score = !(fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS && !need_sorted());
  if (need_sorted() && fusion_iter_exec_mode_ == ObFusionIterExecMode::SCORE_TOP_K_QUERY_HITS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is_distributed_ is false", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fusion_docs_.count(); ++i) {
      if (need_filter_min_score && fusion_docs_.at(i).fusion_score_ < min_score_) {
        continue;
      }
      if (OB_FAIL(sorted_doc_indices_.push_back(i))) {
        LOG_WARN("failed to push back sorted doc index", K(ret), K(i));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
