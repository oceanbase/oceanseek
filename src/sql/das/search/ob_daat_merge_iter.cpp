/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include <algorithm>
#include "ob_daat_merge_iter.h"
#include "sql/das/search/ob_das_async_token_op.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"
#include "sql/das/search/ob_das_sparse_candidate_buffer.h"

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
    async_token_op_(nullptr),
    is_inited_(false)
{
  curr_id_.set_min();
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
    async_token_op_ = DAS_SEARCH_OP_ASYNC_TOKEN == op.get_op_type()
        ? static_cast<ObDASAsyncTokenOp *>(&op)
        : nullptr;
    is_inited_ = true;
  }
  return ret;
}

void ObDAATMergeIter::reuse()
{
  curr_id_.reset();
  curr_id_.set_min();
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

int ObDAATMergeIter::advance_to_uint64(
    const uint64_t target,
    uint64_t &curr_id,
    bool &iter_end)
{
  int ret = OB_SUCCESS;
  iter_end = iter_end_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    // keep successful EOF in the typed hot-path contract
  } else if (OB_NOT_NULL(async_token_op_)) {
    curr_score_materialized_ = false;
    if (OB_FAIL(async_token_op_->advance_to_id_uint64(target, curr_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance to uint64 id", K(ret), K(target));
      } else {
        ret = OB_SUCCESS;
        iter_end_ = true;
      }
    } else {
      curr_id_.set_uint64(curr_id);
      in_shallow_status_ = false;
    }
  } else {
    ObDASRowID target_id;
    target_id.set_uint64(target);
    if (OB_FAIL(advance_to_id(target_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance fallback to uint64 id", K(ret), K(target));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      curr_id = curr_id_.get_uint64();
    }
  }
  if (OB_SUCC(ret)) {
    iter_end = iter_end_;
  }
  return ret;
}

int ObDAATMergeIter::probe_sorted_candidates(
    const uint64_t *ids, const int64_t count, double *scores, bool *matched)
{
  int ret = OB_SUCCESS;
  uint64_t curr = 0;
  bool iter_end = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ids) || OB_ISNULL(scores) || OB_ISNULL(matched)
      || OB_UNLIKELY(count <= 0 || count > blocksstable::ObInvSSTablePostingIter::MAX_SORTED_PROBE_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sorted candidate tile", K(ret), K(count));
  } else if (!iter_end_ && (in_shallow_status_ || !curr_id_.is_normal())) {
    if (OB_FAIL(advance_to_uint64(ids[0], curr, iter_end))) {
      LOG_WARN("failed to position sorted candidate probe", K(ret), K(ids[0]));
    } else if (!iter_end && OB_UNLIKELY(curr < ids[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sorted candidate head behind target", K(ret), K(curr), K(ids[0]));
    }
  }
  if (OB_FAIL(ret) || iter_end_) {
  } else if (count >= 8 && nullptr != async_token_op_
      && async_token_op_->can_probe_sorted_ids()) {
    int64_t first = curr_id_.get_uint64() <= ids[0] ? 0
        : std::lower_bound(ids, ids + count, curr_id_.get_uint64()) - ids;
    if (first < count && ids[first] == curr_id_.get_uint64()) {
      double score = 0.0;
      if (OB_FAIL(load_curr_score(score))) {
        LOG_WARN("failed to load sorted tile cached head", K(ret));
      } else {
        scores[first] += score;
        matched[first] = true;
        ++first;
      }
    }
    if (OB_SUCC(ret) && first < count
        && OB_FAIL(async_token_op_->probe_sorted_ids(
            ids + first, count - first, scores + first, matched + first,
            curr_id_, curr_score_, curr_score_materialized_, iter_end_))) {
      LOG_WARN("failed to probe sorted candidate tile", K(ret));
    }
  } else {
    curr = curr_id_.get_uint64();
    int64_t c = 0;
    while (OB_SUCC(ret) && !iter_end_ && c < count) {
      if (curr < ids[c]) {
        if (OB_FAIL(advance_to_uint64(ids[c], curr, iter_end))) {
          LOG_WARN("failed to seek sorted candidate", K(ret), K(c), K(ids[c]));
        } else if (!iter_end && OB_UNLIKELY(curr < ids[c])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sorted candidate head behind target", K(ret), K(curr), K(ids[c]));
        }
      }
      if (OB_FAIL(ret) || iter_end_) {
      } else if (curr > ids[c]) {
        ++c;
        if (c < count && ids[c] < curr) {
          c = curr > ids[count - 1]
              ? count
              : std::lower_bound(ids + c + 1, ids + count, curr) - ids;
        }
      } else {
        double score = 0.0;
        if (OB_FAIL(load_curr_score(score))) {
          LOG_WARN("failed to load sorted candidate score", K(ret), K(c));
        } else {
          scores[c] += score;
          matched[c] = true;
          ++c;
        }
      }
    }
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

int ObDAATMergeIter::drain_into_accumulator(
    const ObDASRowID &boundary_id, DocScoreAccumulator &doc_score_acc)
{
  int ret = OB_SUCCESS;
  if (!doc_score_acc.has_filter_mask()
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
      }
    }
  } else if (OB_FAIL(drain_search_op_until(
      *op_, curr_id_, curr_score_, iter_end_, boundary_id, doc_score_acc))) {
    LOG_WARN("failed to drain search op until boundary", K(ret));
  }
  return ret;
}

int ObDAATMergeIter::score_candidates(SparseCandidateBuffer &candidates)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge iterator not initialized", K(ret));
  } else if (iter_end_ || candidates.is_empty()) {
  } else if (OB_NOT_NULL(async_token_op_)) {
    if (OB_FAIL(async_token_op_->score_candidates(
        candidates, curr_id_, curr_score_))) {
      LOG_WARN("failed to drain token into candidates", K(ret));
    } else {
      iter_end_ = curr_id_.is_max();
      in_shallow_status_ = false;
      curr_score_materialized_ = !iter_end_;
    }
  } else {
    // Other score operators use the existing scored batch protocol.
    struct CandidateScoreSink
    {
      SparseCandidateBuffer &candidates_;
      int64_t slot_;
      int add_dim(const uint64_t id, const double score)
      {
        return add_dim_batch(&id, &score, 1);
      }
      int add_dim_batch(const uint64_t *ids, const double *scores, const int64_t count)
      {
        int64_t scored_count = 0;
        struct ArrayScoreAt
        {
          const double *scores_;
          OB_INLINE double operator()(const int64_t i) const
          {
            return scores_[i];
          }
        };
        const ArrayScoreAt scorer{scores};
        return candidates_.add_scored_dim_batch(ids, count, scorer, slot_, scored_count);
      }
    };
    const uint64_t *candidate_ids = candidates.get_doc_ids();
    ObDASRowID start_id;
    ObDASRowID end_id;
    start_id.set_uint64(candidate_ids[0]);
    end_id.set_uint64(candidate_ids[candidates.get_count() - 1]);
    CandidateScoreSink sink{candidates, 0};
    if (OB_FAIL(advance_to(start_id))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to position candidate drain", K(ret), K(start_id));
      }
    } else if (OB_FAIL(drain_until(end_id, sink))) {
      LOG_WARN("failed to drain scores into candidates", K(ret), K(end_id));
    }
  }
  return ret;
}

int ObDAATMergeIter::drain_until_active(
    const uint64_t range_start,
    const uint64_t range_end, DocScoreAccumulator &doc_score_acc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    // nothing to drain
  } else if (OB_FAIL(op_->drain_until_active(range_start, range_end, doc_score_acc))) {
    LOG_WARN("failed to drain active rows until boundary", K(ret));
  } else {
    curr_id_.set_min();
    curr_score_materialized_ = false;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
