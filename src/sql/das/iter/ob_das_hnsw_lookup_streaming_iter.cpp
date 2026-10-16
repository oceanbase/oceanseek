/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_hnsw_lookup_streaming_iter.h"
#include "sql/das/iter/ob_das_hnsw_scan_iter.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObDASHNSWLookupStreamingIter::ObDASHNSWLookupStreamingIter()
  : ObDASLocalLookupIter(ObDASIterType::DAS_ITER_LOCAL_LOOKUP),
    streaming_ctx_(nullptr),
    hnsw_scan_iter_(nullptr),
    enable_simplified_scan_(false),
    scan_mode_(ScanMode::UNRESOLVED),
    skip_(nullptr)
{
}

int ObDASHNSWLookupStreamingIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObDASHNSWLookupStreamingIterParam &streaming_param =
      static_cast<ObDASHNSWLookupStreamingIterParam &>(param);
  if (OB_FAIL(ObDASLocalLookupIter::inner_init(param))) {
    LOG_WARN("failed to init local lookup iter", K(ret));
  } else if (OB_ISNULL(streaming_param.streaming_ctx_)
             || OB_ISNULL(streaming_param.hnsw_scan_iter_)
             || OB_ISNULL(exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector lookup streaming context", K(ret), K(max_size_),
             KP(streaming_param.streaming_ctx_), KP(streaming_param.hnsw_scan_iter_),
             KP(exec_ctx_));
  } else {
    streaming_ctx_ = streaming_param.streaming_ctx_;
    hnsw_scan_iter_ = streaming_param.hnsw_scan_iter_;
    enable_simplified_scan_ = streaming_param.enable_simplified_scan_;
  }
  return ret;
}

int ObDASHNSWLookupStreamingIter::prepare_scan_mode(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  if (ScanMode::UNRESOLVED != scan_mode_) {
  } else if (OB_ISNULL(streaming_ctx_) || OB_ISNULL(hnsw_scan_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid vector lookup streaming state", K(ret), KP(streaming_ctx_),
             KP(hnsw_scan_iter_));
  } else if (OB_FAIL(hnsw_scan_iter_->prepare_query_result(is_vectorized))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to prepare hnsw query result", K(ret), K(is_vectorized));
    }
  } else {
    scan_mode_ = enable_simplified_scan_ && !streaming_ctx_->need_recompute_distance_
                   ? ScanMode::SIMPLIFIED
                   : ScanMode::LOOKUP;
    LOG_TRACE("resolve vector lookup streaming scan mode", K(enable_simplified_scan_),
              K(streaming_ctx_->need_recompute_distance_), K(static_cast<int64_t>(scan_mode_)));
  }
  return ret;
}

void ObDASHNSWLookupStreamingIter::clear_distance_evaluated_flags(
    bool is_vectorized, int64_t row_count)
{
  if (OB_NOT_NULL(streaming_ctx_) && OB_NOT_NULL(streaming_ctx_->distance_expr_)
      && OB_NOT_NULL(eval_ctx_)) {
    ObExpr *distance_expr = streaming_ctx_->distance_expr_;
    distance_expr->get_eval_info(*eval_ctx_).clear_evaluated_flag();
    if (is_vectorized && distance_expr->is_batch_result()) {
      distance_expr->get_evaluated_flags(*eval_ctx_).reset(row_count);
    }
  }
}

void ObDASHNSWLookupStreamingIter::clear_distance_input_evaluated_flags(
    bool is_vectorized, int64_t batch_size)
{
  if (OB_NOT_NULL(streaming_ctx_) && OB_NOT_NULL(streaming_ctx_->distance_expr_)
      && OB_NOT_NULL(eval_ctx_)) {
    ObExpr *distance_expr = streaming_ctx_->distance_expr_;
    if (distance_expr->arg_cnt_ > 0 && OB_NOT_NULL(distance_expr->args_[0])) {
      ObExpr *vector_expr = distance_expr->args_[0];
      vector_expr->get_eval_info(*eval_ctx_).clear_evaluated_flag();
      if (is_vectorized && vector_expr->is_batch_result()) {
        vector_expr->get_evaluated_flags(*eval_ctx_).reset(batch_size);
      }
    }
  }
}

int ObDASHNSWLookupStreamingIter::recompute_distance_batch(int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(streaming_ctx_) || OB_ISNULL(streaming_ctx_->distance_expr_)
      || OB_ISNULL(exec_ctx_) || OB_UNLIKELY(max_size_ <= 0 || row_count > max_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid context for lookup distance recompute", K(ret), K(row_count), K(max_size_),
             KP(streaming_ctx_), KP(exec_ctx_));
  } else if (OB_ISNULL(skip_)) {
    if (OB_ISNULL(skip_ = to_bit_vector(
            exec_ctx_->get_allocator().alloc(ObBitVector::memory_size(max_size_))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate vector lookup streaming skip", K(ret), K(max_size_));
    } else {
      skip_->init(max_size_);
    }
  }
  if (OB_SUCC(ret)) {
    clear_distance_evaluated_flags(true, row_count);
    skip_->reset(row_count);
  }
  if (OB_SUCC(ret) && OB_FAIL(streaming_ctx_->distance_expr_->eval_batch(
                  *eval_ctx_, *skip_, row_count))) {
    LOG_WARN("failed to recompute lookup distance batch", K(ret), K(row_count));
  }
  return ret;
}

int ObDASHNSWLookupStreamingIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASLocalLookupIter::inner_reuse())) {
    LOG_WARN("failed to reuse local lookup iter", K(ret));
  }
  if (OB_NOT_NULL(streaming_ctx_)) {
    streaming_ctx_->reset_runtime();
  }
  scan_mode_ = ScanMode::UNRESOLVED;
  return ret;
}

int ObDASHNSWLookupStreamingIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASLocalLookupIter::rescan())) {
    LOG_WARN("failed to rescan local lookup iter", K(ret));
  }
  if (OB_NOT_NULL(streaming_ctx_)) {
    streaming_ctx_->reset_runtime();
  }
  scan_mode_ = ScanMode::UNRESOLVED;
  return ret;
}

int ObDASHNSWLookupStreamingIter::inner_release()
{
  int ret = OB_SUCCESS;
  streaming_ctx_ = nullptr;
  hnsw_scan_iter_ = nullptr;
  enable_simplified_scan_ = false;
  scan_mode_ = ScanMode::UNRESOLVED;
  skip_ = nullptr;
  if (OB_FAIL(ObDASLocalLookupIter::inner_release())) {
    LOG_WARN("failed to release local lookup iter", K(ret));
  }
  return ret;
}

int ObDASHNSWLookupStreamingIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_scan_mode(false))) {
  } else if (ScanMode::SIMPLIFIED == scan_mode_) {
    index_table_iter_->clear_evaluated_flag();
    if (OB_FAIL(index_table_iter_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get simplified vector scan row", K(ret));
      }
    }
  } else {
    if (streaming_ctx_->need_recompute_distance_) {
      clear_distance_input_evaluated_flags(false, 1);
    }
    if (OB_FAIL(ObDASLookupIter::inner_get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get lookup row for vector distance calculation", K(ret));
      }
    } else if (streaming_ctx_->need_recompute_distance_) {
      ObDatum *distance = nullptr;
      if (OB_ISNULL(streaming_ctx_->distance_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("distance expression is null for lookup distance recompute", K(ret));
      } else {
        clear_distance_evaluated_flags(false, 1);
      }
      if (OB_SUCC(ret) && OB_FAIL(streaming_ctx_->distance_expr_->eval(*eval_ctx_, distance))) {
        LOG_WARN("failed to evaluate lookup distance", K(ret));
      }
    }
  }
  return ret;
}

int ObDASHNSWLookupStreamingIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_FAIL(prepare_scan_mode(true))) {
  } else if (ScanMode::SIMPLIFIED == scan_mode_) {
    index_table_iter_->clear_evaluated_flag();
    ret = index_table_iter_->get_next_rows(count, capacity);
    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("failed to get simplified vector scan rows", K(ret), K(count));
    }
  } else {
    if (streaming_ctx_->need_recompute_distance_) {
      clear_distance_input_evaluated_flags(true, capacity);
    }
    ret = ObDASLookupIter::inner_get_next_rows(count, capacity);
    if ((OB_SUCCESS == ret || OB_ITER_END == ret) && count > 0) {
      if (streaming_ctx_->need_recompute_distance_) {
        const int child_ret = ret;
        if (OB_FAIL(recompute_distance_batch(count))) {
          LOG_WARN("failed to calculate lookup distance batch", K(ret), K(count));
        } else {
          ret = child_ret;
        }
      }
    } else if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("failed to get lookup rows for vector distance calculation", K(ret));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
