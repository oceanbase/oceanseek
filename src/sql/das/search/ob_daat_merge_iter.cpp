/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_daat_merge_iter.h"
#include "sql/das/search/ob_das_async_token_op.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"

namespace oceanbase
{
namespace sql
{

ObDAATMergeIter::ObDAATMergeIter()
  : op_(nullptr),
    curr_id_(),
    curr_score_(0.0),
    curr_score_materialized_(false),
    curr_block_max_info_(nullptr),
    max_score_(0.0),
    iter_idx_(-1),
    max_score_calculated_(false),
    in_shallow_status_(false),
    iter_end_(false),
    is_inited_(false)
{
}

int ObDAATMergeIter::init(const int64_t iter_idx, ObIDASSearchOp &op)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("iter already inited", K(ret));
  } else {
    iter_idx_ = iter_idx;
    op_ = &op;
    max_score_calculated_ = false;
    curr_score_materialized_ = false;
    in_shallow_status_ = false;
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

void ObDAATMergeIter::reuse()
{
  curr_id_.reset();
  curr_score_ = 0.0;
  curr_score_materialized_ = false;
  curr_block_max_info_ = nullptr;
  max_score_calculated_ = false;
  max_score_ = 0.0;
  in_shallow_status_ = false;
  iter_end_ = false;
}

int ObDAATMergeIter::next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not get next row after shallow advance",
        K(ret), K_(in_shallow_status));
  } else if (FALSE_IT(curr_score_materialized_ = false)) {
  } else if (OB_FAIL(op_->next_rowid(curr_id_, curr_score_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to next rowid", K(ret));
    } else {
      iter_end_ = true;
    }
  } else {
    curr_score_materialized_ = true;
  }
  return ret;
}

int ObDAATMergeIter::advance_to(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(curr_score_materialized_ = false)) {
  } else if (OB_FAIL(op_->advance_to(target, curr_id_, curr_score_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to", K(ret));
    } else {
      iter_end_ = true;
    }
  } else {
    in_shallow_status_ = false;
    curr_score_materialized_ = true;
  }
  return ret;
}

int ObDAATMergeIter::advance_to_id(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(curr_score_materialized_ = false)) {
  } else if (op_->can_advance_to_id()) {
    if (OB_FAIL(op_->advance_to_id(target, curr_id_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to id", K(ret), K(target));
      } else {
        iter_end_ = true;
      }
    } else {
      in_shallow_status_ = false;
    }
  } else if (OB_FAIL(op_->advance_to(target, curr_id_, curr_score_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to for doc id only", K(ret), K(target));
    } else {
      iter_end_ = true;
    }
  } else {
    in_shallow_status_ = false;
    curr_score_materialized_ = true;
  }
  return ret;
}

int ObDAATMergeIter::advance_shallow(const ObDASRowID &target, const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(curr_score_materialized_ = false)) {
  } else if (OB_FAIL(op_->advance_shallow(target, inclusive, curr_block_max_info_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance shallow", K(ret));
    } else {
      iter_end_ = true;
    }
  } else {
    in_shallow_status_ = true;
  }
  return ret;
}

int ObDAATMergeIter::to_shallow()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (in_shallow_status_) {
    // skip
  } else if (FALSE_IT(curr_score_materialized_ = false)) {
  } else if (OB_FAIL(op_->advance_shallow(curr_id_, true, curr_block_max_info_))) {
    LOG_WARN("failed to advance shallow", K(ret));
  } else {
    in_shallow_status_ = true;
  }
  return ret;
}

int ObDAATMergeIter::get_curr_id(ObDASRowID &curr_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    curr_id = curr_id_;
  }
  return ret;
}

int ObDAATMergeIter::get_curr_score(double &score) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not get curr score after shallow advance",
        K(ret), K_(in_shallow_status));
  } else if (OB_UNLIKELY(!curr_score_materialized_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("curr score is not materialized", K(ret));
  } else {
    score = curr_score_;
  }
  return ret;
}

int ObDAATMergeIter::load_curr_score(double &score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(iter_end_ || in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not load curr score",
        K(ret), K_(iter_end), K_(in_shallow_status));
  } else if (curr_score_materialized_) {
    score = curr_score_;
  } else if (OB_FAIL(op_->load_curr_score(curr_score_))) {
    LOG_WARN("failed to load curr score", K(ret));
  } else {
    curr_score_materialized_ = true;
    score = curr_score_;
  }
  return ret;
}

int ObDAATMergeIter::get_curr_block_max_info(const MaxScoreTuple *&max_score_tuple) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected  status", K(ret), K_(in_shallow_status));
  } else {
    max_score_tuple = curr_block_max_info_;
  }
  return ret;
}

int ObDAATMergeIter::get_max_score(double &max_score)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (max_score_calculated_) {
    max_score = max_score_;
  } else if (OB_FAIL(op_->calc_max_score(max_score_))) {
    LOG_WARN("failed to calc max score", K(ret));
  } else {
    max_score = max_score_;
    max_score_calculated_ = true;
  }
  return ret;
}

// Delegate the conservative range score bound without advancing the merge iterator.
int ObDAATMergeIter::get_range_max_score(
    const ObDASRowID &start_id,
    const bool start_inclusive,
    const ObDASRowID &end_id,
    double &max_score,
    bool &can_calc)
{
  int ret = OB_SUCCESS;
  can_calc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(op_->calc_range_max_score(
      start_id, start_inclusive, end_id, max_score, can_calc))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to calc range max score", K(ret),
        K(start_id), K(start_inclusive), K(end_id));
  }
  return ret;
}

int ObDAATMergeIter::drain_until(
    const ObDASRowID &boundary_id, DocScoreAccumulator &doc_score_acc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    // nothing to drain
  } else if (OB_UNLIKELY(in_shallow_status_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter status, can not drain batch after shallow advance",
        K(ret), K_(in_shallow_status));
  } else if (OB_UNLIKELY(!curr_score_materialized_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("curr score is not materialized", K(ret));
  } else if (!doc_score_acc.has_filter_mask()
      && DAS_SEARCH_OP_ASYNC_TOKEN == op_->get_op_type()) {
    if (OB_UNLIKELY(!curr_id_.is_normal() || !boundary_id.is_normal())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid accumulated drain boundary", K(ret), K_(curr_id), K(boundary_id));
    } else if (curr_id_.get_uint64() <= boundary_id.get_uint64()) {
      ObDASRowID next_id;
      double next_score = 0.0;
      if (OB_FAIL(doc_score_acc.add_dim(curr_id_.get_uint64(), curr_score_))) {
        LOG_WARN("failed to add current accumulated dim", K(ret), K_(curr_id));
      } else if (OB_FAIL(static_cast<ObDASAsyncTokenOp *>(op_)->drain_until_accumulated(
          boundary_id, doc_score_acc, next_id, next_score))) {
        LOG_WARN("failed to drain search op into accumulator", K(ret), K(boundary_id));
      } else {
        iter_end_ = next_id.is_max();
        curr_id_ = next_id;
        curr_score_ = next_score;
        curr_score_materialized_ = !iter_end_;
      }
    }
  } else if (OB_FAIL(drain_search_op_until(
      *op_, curr_id_, curr_score_, iter_end_, boundary_id, doc_score_acc))) {
    LOG_WARN("failed to drain search op until boundary", K(ret));
  } else if (iter_end_) {
    curr_score_materialized_ = false;
  } else {
    curr_score_materialized_ = true;
  }
  return ret;
}

int ObDAATMergeIter::drain_until_active(
    const ObDASRowID &boundary_id, DocScoreAccumulator &doc_score_acc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    // nothing to drain
  } else if (OB_FAIL(op_->drain_until_active(boundary_id, doc_score_acc))) {
    LOG_WARN("failed to drain active rows until boundary", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
