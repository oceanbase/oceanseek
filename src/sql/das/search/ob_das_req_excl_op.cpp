/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_req_excl_op.h"
#include "common/ob_smart_call.h"
#include "sql/das/search/ob_das_async_index_deletion_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASReqExclOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(children.push_back(required_))) {
    LOG_WARN("failed to push required op", KR(ret), KPC(required_));
  } else if (OB_FAIL(children.push_back(excluded_))) {
    LOG_WARN("failed to push excluded op", KR(ret), KPC(excluded_));
  }
  return ret;
}

int ObDASReqExclOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASReqExclOpParam &req_excl_op_param = static_cast<const ObDASReqExclOpParam &>(op_param);
  if (OB_ISNULL(req_excl_op_param.get_required()) || OB_ISNULL(req_excl_op_param.get_excluded())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid required/excluded op", K(ret), K(req_excl_op_param.get_required()), K(req_excl_op_param.get_excluded()));
  } else {
    required_ = req_excl_op_param.get_required();
    excluded_ = req_excl_op_param.get_excluded();
  }
  return ret;
}

int ObDASReqExclOp::try_apply_sort_bottom(const common::ObDatum &sort_bottom, bool &applied)
{
  int ret = OB_SUCCESS;
  applied = false;
  if (OB_ISNULL(required_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort bottom required input is null", K(ret));
  } else if (OB_FAIL(SMART_CALL(required_->try_apply_sort_bottom(sort_bottom, applied)))) {
    LOG_WARN("failed to prune required input ranges", K(ret));
  }
  return ret;
}

int ObDASReqExclOp::do_open()
{
  int ret = OB_SUCCESS;
  // RowID type and child op type are finalized by init(). Keep one exclusion
  // mode for the whole operator lifetime, as the other batch merge ops do.
  use_deletion_bitmap_ = is_valid()
      && DAS_SCOPE_BASE == scope_
      && DAS_ROWID_TYPE_UINT64 == get_rowid_type()
      && DAS_SEARCH_OP_DELETION == excluded_->get_op_type()
      && DAS_ROWID_TYPE_UINT64 == excluded_->get_rowid_type();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->open())) {
    LOG_WARN("failed to open required op", KR(ret));
  } else if (OB_FAIL(excluded_->open())) {
    LOG_WARN("failed to open excluded op", KR(ret));
  } else if (use_deletion_bitmap_ && OB_FAIL(ensure_batch_buffer())) {
    LOG_WARN("failed to initialize required/excluded batch buffer", K(ret));
  } else {
    is_first_nk_ = true;
  }
  return ret;
}

int ObDASReqExclOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->rescan())) {
    LOG_WARN("failed to rescan required op", KR(ret));
  } else if (OB_FAIL(excluded_->rescan())) {
    LOG_WARN("failed to rescan excluded op", KR(ret));
  } else {
    is_first_nk_ = true;
  }
  return ret;
}

int ObDASReqExclOp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
  } else {
    required_ = nullptr;
    excluded_ = nullptr;
    batch_allocator_.reset();
    batch_ids_ = nullptr;
    batch_scores_ = nullptr;
    use_deletion_bitmap_ = false;
    is_first_nk_ = true;
  }

  return ret;
}

bool ObDASReqExclOp::is_deleted(const uint64_t doc_id) const
{
  return static_cast<const ObDASAsyncIndexDeletionOp *>(excluded_)->contains_doc_id(doc_id);
}

int ObDASReqExclOp::ensure_batch_buffer()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(batch_ids_) || (is_scoring_ && OB_ISNULL(batch_scores_))) {
    batch_allocator_.reset();
    batch_ids_ = nullptr;
    batch_scores_ = nullptr;
    batch_ids_ = static_cast<uint64_t *>(batch_allocator_.alloc(
        BATCH_SIZE * static_cast<int64_t>(sizeof(uint64_t))));
    if (OB_ISNULL(batch_ids_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate required/excluded RowID batch buffer", K(ret));
    } else if (is_scoring_
               && OB_ISNULL(batch_scores_ = static_cast<double *>(
                   batch_allocator_.alloc(
                       BATCH_SIZE * static_cast<int64_t>(sizeof(double)))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate required/excluded score batch buffer", K(ret));
    }
    if (OB_FAIL(ret)) {
      batch_allocator_.reset();
      batch_ids_ = nullptr;
      batch_scores_ = nullptr;
    }
  }
  return ret;
}

int ObDASReqExclOp::init_nested_key_cursor()
{
  int ret = OB_SUCCESS;
  ObDASRowID excluded_doc;
  double score = 0.0;
  int cmp = 0;
  nested_excluded_matches_doc_ = false;
  if (OB_FAIL(excluded_->advance_to(required_doc_id_, excluded_doc, score))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to position excluded document", K(ret));
    }
  } else if (OB_FAIL(compare_rowid(required_doc_id_, excluded_doc, cmp))) {
    LOG_WARN("failed to compare excluded document", K(ret));
  } else {
    nested_excluded_matches_doc_ = 0 == cmp;
  }
  if (OB_SUCC(ret)) {
    is_first_nk_ = false;
  }
  return ret;
}

int ObDASReqExclOp::check_excluded(const ObDASRowID &target_id, int &cmp)
{
  int ret = OB_SUCCESS;
  bool need_advance_excluded = true;
  ObDASRowID excluded_id;
  double excluded_score = 0.0;
  cmp = 0;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(excluded_->advance_to(target_id, excluded_id, excluded_score))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      cmp = -1;
    } else {
      LOG_WARN("failed to advance to excluded op", K(ret), K(target_id));
    }
  } else if (OB_FAIL(compare_rowid(target_id, excluded_id, cmp))) {
    LOG_WARN("failed to compare rowid", K(ret), K(target_id), K(excluded_id));
  } else if (OB_UNLIKELY(cmp > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("excluded rowid is smaller than target rowid", K(ret), K(target_id), K(excluded_id));
  }
  return ret;
}

int ObDASReqExclOp::filter_nested_key(ObDASNestedKey &key, double &score)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  while (OB_SUCC(ret) && nested_excluded_matches_doc_ && 0 == cmp) {
    ObDASNestedKey excluded;
    double excluded_score = 0.0;
    if (OB_FAIL(excluded_->advance_to(key, excluded, excluded_score))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        nested_excluded_matches_doc_ = false;
      } else {
        LOG_WARN("failed to advance excluded nested key", KR(ret), K(key));
      }
    } else if (OB_FAIL(compare_rowid(key, excluded, cmp))) {
      LOG_WARN("failed to compare excluded nested key", KR(ret), K(key), K(excluded));
    } else if (OB_UNLIKELY(cmp > 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("excluded nested key is smaller than target", KR(ret), K(key), K(excluded));
    } else if (0 == cmp && OB_FAIL(required_->next_rowid(key, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to next required nested key", KR(ret));
    }
  }
  return ret;
}

int ObDASReqExclOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  is_first_nk_ = true;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->advance_to(target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to required op", K(ret), K(target));
  } else if (is_async_nested_scope(scope_)) {
    required_doc_id_ = curr_id;
  } else if (use_deletion_bitmap_) {
    while (OB_SUCC(ret) && is_deleted(curr_id.get_uint64())) {
      if (OB_FAIL(required_->next_rowid(curr_id, score))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to skip deleted required row", K(ret));
      }
    }
  } else {
    while (OB_SUCC(ret) && cmp == 0) {
      if (OB_FAIL(check_excluded(curr_id, cmp))) {
        LOG_WARN("failed to check excluded", K(ret), K(curr_id));
      } else if (cmp == 0) {
        if (OB_FAIL(required_->next_rowid(curr_id, score))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid required op", K(ret));
        }
      } else {
        LOG_TRACE("do_advance_to hit", K(target), K(curr_id), K(cmp));
      }
    }
  }
  return ret;
}

int ObDASReqExclOp::do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (!is_async_nested_scope(scope_)) {
    ret = OB_NOT_IMPLEMENT;
  } else if (is_first_nk_ && OB_FAIL(init_nested_key_cursor())) {
    LOG_WARN("failed to initialize nested key cursor", KR(ret));
  } else if (OB_FAIL(required_->advance_to(target, curr_nk, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance required nested key", KR(ret));
  } else if (OB_FAIL(filter_nested_key(curr_nk, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to filter nested key", KR(ret));
  }
  return ret;
}

int ObDASReqExclOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (is_async_nested_scope(scope_)) {
    ret = ObIDASSearchOp::do_advance_shallow(target, inclusive, max_score_tuple);
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->advance_shallow(target, inclusive, max_score_tuple))) {
    LOG_WARN("failed to advance shallow required op", KR(ret), K(target));
  }
  return ret;
}

int ObDASReqExclOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  is_first_nk_ = true;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (is_async_nested_scope(scope_)) {
    if (OB_FAIL(required_->next_rowid(next_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to next required document", KR(ret));
    } else {
      required_doc_id_ = next_id;
    }
  } else if (use_deletion_bitmap_) {
    do {
      if (OB_FAIL(required_->next_rowid(next_id, score))) {
        LOG_WARN_IGNORE_ITER_END(
            ret, "failed to get required row for deletion filtering", K(ret));
      }
    } while (OB_SUCC(ret) && is_deleted(next_id.get_uint64()));
  } else {
    while (OB_SUCC(ret) && cmp == 0) {
      if (OB_FAIL(required_->next_rowid(next_id, score))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid required op", K(ret));
      } else if (OB_FAIL(check_excluded(next_id, cmp))) {
        LOG_WARN("failed to check excluded", K(ret), K(next_id));
      }
    }
  }
  return ret;
}

int ObDASReqExclOp::do_next_rowid(ObDASNestedKey &next_nk, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (!is_async_nested_scope(scope_)) {
    ret = OB_NOT_IMPLEMENT;
  } else if (is_first_nk_ && OB_FAIL(init_nested_key_cursor())) {
    LOG_WARN("failed to initialize nested key cursor", KR(ret));
  } else if (OB_FAIL(required_->next_rowid(next_nk, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to next required nested key", KR(ret));
  } else if (OB_FAIL(filter_nested_key(next_nk, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to filter nested key", KR(ret));
  }
  return ret;
}

int ObDASReqExclOp::do_drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  is_first_nk_ = true;
  int ret = OB_SUCCESS;
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (use_deletion_bitmap_) {
    if (OB_FAIL(drain_deletion_bitmap_until(
        boundary, capacity, out_ids, out_scores, count,
        next_id_after, next_score_after))) {
      LOG_WARN("failed to drain deletion bitmap filtered rows", K(ret));
    }
  } else {
    if (OB_FAIL(ObIDASSearchOp::do_drain_until(
        boundary, capacity, out_ids, out_scores, count,
        next_id_after, next_score_after))) {
      LOG_WARN("failed to drain required/excluded op", K(ret));
    }
  }
  return ret;
}

int ObDASReqExclOp::drain_deletion_bitmap_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  bool done = false;
  const ObDASAsyncIndexDeletionOp *deletion_op =
      static_cast<const ObDASAsyncIndexDeletionOp *>(excluded_);
  while (OB_SUCC(ret) && count < capacity && !done) {
    const int64_t raw_capacity = MIN(BATCH_SIZE, capacity - count);
    int64_t raw_count = 0;
    ObDASRowID required_next_id;
    double required_next_score = 0.0;
    if (OB_FAIL(required_->drain_until(
        boundary, raw_capacity, batch_ids_, batch_scores_, raw_count,
        required_next_id, required_next_score))) {
      LOG_WARN("failed to batch drain required op", K(ret),
          K(raw_capacity), K(count), K(capacity));
    } else {
      if (raw_count > 0
          && !deletion_op->contains_any_doc_id(
              batch_ids_[0], batch_ids_[raw_count - 1])) {
        MEMCPY(out_ids + count, batch_ids_,
            raw_count * static_cast<int64_t>(sizeof(uint64_t)));
        if (nullptr != out_scores) {
          if (is_scoring_) {
            MEMCPY(out_scores + count, batch_scores_,
                raw_count * static_cast<int64_t>(sizeof(double)));
          } else {
            MEMSET(out_scores + count, 0,
                raw_count * static_cast<int64_t>(sizeof(double)));
          }
        }
        count += raw_count;
      } else {
        for (int64_t i = 0; i < raw_count; ++i) {
          if (!deletion_op->contains_doc_id(batch_ids_[i])) {
            out_ids[count] = batch_ids_[i];
            if (nullptr != out_scores) {
              out_scores[count] = is_scoring_ ? batch_scores_[i] : 0.0;
            }
            ++count;
          }
        }
      }
      if (raw_count == raw_capacity) {
        // A full required batch has no lookahead contract. Continue until the
        // caller's output is full or the required op reaches the boundary.
      } else if (required_next_id.is_max()) {
        next_id_after.set_max();
        done = true;
      } else if (OB_UNLIKELY(!required_next_id.is_normal())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid required batch lookahead", K(ret),
            K(required_next_id), K(raw_count), K(raw_capacity));
      } else if (!deletion_op->contains_doc_id(required_next_id.get_uint64())) {
        next_id_after = required_next_id;
        next_score_after = required_next_score;
        done = true;
      }
      // A deleted lookahead is consumed here. Continue draining to find the
      // first visible row beyond the boundary.
    }
  }
  return ret;
}

int ObDASReqExclOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is not valid", KR(ret));
  } else if (OB_FAIL(required_->set_min_competitive_score(threshold))) {
    LOG_WARN("failed to set min competitive score required op", KR(ret), K(threshold));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
