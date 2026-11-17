/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_conjunction_op.h"
#include "common/ob_smart_call.h"
#include <cmath>
#include "sql/das/ob_das_def_reg.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/das/search/ob_das_search_context.h"

namespace oceanbase
{
namespace sql
{

int ObDASConjunctionOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(required_ops_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("required ops is null", KR(ret));
  } else if (OB_FAIL(children.assign(*required_ops_))) {
    LOG_WARN("failed to assign required ops", KR(ret));
  }
  return ret;
}

int ObDASConjunctionOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASConjunctionOpParam &conjunction_op_param = static_cast<const ObDASConjunctionOpParam &>(op_param);
  const ObIArray<ObIDASSearchOp *> *required_ops = conjunction_op_param.get_required_ops();
  if (OB_ISNULL(required_ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr required ops", KR(ret));
  } else if (required_ops->empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty required ops", KR(ret));
  } else {
    last_idx_ = 0;
  }
  return ret;
}

int ObDASConjunctionOp::classify_children()
{
  int ret = OB_SUCCESS;
  probe_ops_.reset();
  driver_ops_.reset();
  has_probe_child_ = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObIDASSearchOp *op = children_[i];
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret), K(i));
    } else if (op->is_probe_mode()) {
      if (OB_FAIL(probe_ops_.push_back(op))) {
        LOG_WARN("failed to push back probe op", KR(ret), K(i));
      }
    } else if (OB_FAIL(driver_ops_.push_back(op))) {
      LOG_WARN("failed to push back driver op", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (driver_ops_.count() + probe_ops_.count() != children_cnt_ || driver_ops_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected children count", KR(ret), K(driver_ops_.count()), K(probe_ops_.count()), K(children_cnt_));
    } else if (probe_ops_.count() > 0) {
      has_probe_child_ = true;
    } else {
      has_probe_child_ = false;
    }
    last_idx_ = 0;
  }
  if (OB_SUCC(ret) && OB_FAIL(init_fixed_score_bound())) {
    LOG_WARN("failed to initialize conjunction fixed score bound", KR(ret));
  }
  LOG_TRACE("classify children", KR(ret), K(driver_ops_.count()), K(probe_ops_.count()),
    K(children_cnt_), K(has_probe_child_));
  return ret;
}

int ObDASConjunctionOp::init_fixed_score_bound()
{
  int ret = OB_SUCCESS;
  const double MAX_EXACT_INTEGER_SCORE = 9007199254740992.0; // 2^53
  double max_score = 0.0;
  bool known = is_scoring_ && driver_ops_.count() > 1;
  is_fixed_score_ = false;
  fixed_score_bound_ = INVALID_FIXED_SCORE_BOUND;
  // The builder only permits non-scoring probes; only drivers contribute scores.
  for (int64_t i = 0; OB_SUCC(ret) && known && i < driver_ops_.count(); ++i) {
    ObIDASSearchOp *op = driver_ops_.at(i);
    double child_score = 0.0;
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null conjunction child", KR(ret), K(i));
    } else if (ScoreCapability::ARRAY == op->get_score_capability()) {
      known = false;
    } else {
      child_score = op->get_constant_score();
      // Nonnegative integer partial sums up to 2^53 are exact in every driver order.
      // shortcut: fractional boosts are not optimized; revisit with a linear-time safe bound.
      if (!std::isfinite(child_score) || child_score < 0.0
          || std::floor(child_score) != child_score
          || child_score > MAX_EXACT_INTEGER_SCORE - max_score) {
        known = false;
      } else {
        max_score += child_score;
      }
    }
  }
  if (OB_SUCC(ret) && known) {
    fixed_score_bound_ = max_score;
    is_fixed_score_ = true;
  }
  return ret;
}

int ObDASConjunctionOp::try_apply_sort_bottom(const common::ObDatum &sort_bottom, bool &applied)
{
  int ret = OB_SUCCESS;
  applied = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    bool child_applied = false;
    if (OB_ISNULL(children_) || OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sort bottom conjunction child is null", K(ret), K(i));
    } else if (OB_FAIL(SMART_CALL(children_[i]->try_apply_sort_bottom(sort_bottom, child_applied)))) {
      LOG_WARN("failed to apply sort bottom to conjunction child", K(ret), K(i));
    }
    applied = applied || child_applied;
  }
  return ret;
}

int ObDASConjunctionOp::do_open()
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObIDASSearchOp *op = children_[i];
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret));
    } else if (OB_FAIL(op->open())) {
      LOG_WARN("failed to open", KR(ret), K(*op));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(classify_children())) {
      LOG_WARN("failed to classify children", KR(ret));
    } else {
      is_first_nk_ = true;
    }
  }
  return ret;
}

int ObDASConjunctionOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  is_fixed_score_ = false;
  fixed_score_bound_ = INVALID_FIXED_SCORE_BOUND;
  for (int i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_ISNULL(children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret));
    } else if (OB_FAIL(children_[i]->rescan_segment(range))) {
      LOG_WARN("failed to rescan_segment child", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    last_idx_ = 0;
    is_first_nk_ = true;
    if (OB_FAIL(init_fixed_score_bound())) {
      LOG_WARN("failed to refresh conjunction fixed score bound", KR(ret));
    }
  }
  return ret;
}

int ObDASConjunctionOp::do_rescan()
{
  int ret = OB_SUCCESS;
  is_fixed_score_ = false;
  fixed_score_bound_ = INVALID_FIXED_SCORE_BOUND;
  for (int i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    ObIDASSearchOp *op = children_[i];
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null op", KR(ret));
    } else if (OB_FAIL(op->rescan())) {
      LOG_WARN("failed to rescan", KR(ret), K(*op));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(classify_children())) {
      LOG_WARN("failed to classify children", KR(ret));
    } else {
      is_first_nk_ = true;
    }
  }
  return ret;
}

int ObDASConjunctionOp::do_close()
{
  int ret = OB_SUCCESS;
  last_idx_ = 0;
  has_probe_child_ = false;
  is_fixed_score_ = false;
  fixed_score_bound_ = INVALID_FIXED_SCORE_BOUND;
  probe_ops_.reset();
  driver_ops_.reset();
  is_first_nk_ = true;
  return ret;
}

int ObDASConjunctionOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  is_first_nk_ = true;
  return do_advance_to_impl<ObDASRowID>(target, curr_id, score);
}

int ObDASConjunctionOp::do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score)
{
  INIT_SUCC(ret);
  if (!is_async_nested_scope(scope_)) {
    ret = OB_NOT_IMPLEMENT;
  } else {
    if (is_first_nk_) {
      nested_last_idx_ = 0;
      is_first_nk_ = false;
    }
    ret = do_advance_to_impl<ObDASNestedKey>(target, curr_nk, score);
  }
  return ret;
}

template <typename RowType>
int ObDASConjunctionOp::do_advance_to_impl(const RowType &target, RowType &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t &last_idx = std::is_same_v<RowType, ObDASNestedKey> ? nested_last_idx_ : last_idx_;
  RowType init_target;
  ObIDASSearchOp *op = nullptr;
  score = 0.0;
  if (OB_UNLIKELY(last_idx >= driver_ops_.count() || OB_ISNULL(op = driver_ops_.at(last_idx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", KR(ret), K(last_idx), K(driver_ops_.count()));
  } else if (has_fixed_score_bound() && cannot_compete_with_score(fixed_score_bound_)) {
    curr_id.set_max();
    ret = OB_ITER_END;
  } else if (OB_FAIL(op->advance_to(target, init_target, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", KR(ret));
  } else if (OB_FAIL(inner_advance_to<RowType>(init_target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance to", KR(ret));
  }
  return ret;
}

template <typename RowType>
int ObDASConjunctionOp::inner_advance_to(
    const RowType &target,
    RowType &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if constexpr (std::is_same_v<RowType, ObDASNestedKey>) {
    ret = advance_driver_ops_to<RowType>(target, curr_id, score);
  } else {
    if (!has_probe_child_) {
      ret = advance_driver_ops_to<RowType>(target, curr_id, score);
    } else {
      ret = inner_advance_to_with_probe(target, curr_id, score);
    }
  }
  return ret;
}

int ObDASConjunctionOp::inner_advance_to_with_probe(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  // Probe-coordination path: separate the work into two phases per iteration.
  //   Phase 1: advance all non-probe children (the driver + any zig-zag siblings) to
  //            converge on a stable candidate_target via the standard zig-zag.
  //   Phase 2: probe every probe-capable child against the stable candidate. If all
  //            hit, candidate is the result. If any miss, advance the driver past the
  //            candidate and retry both phases at the new candidate.
  // A primary probe only answers membership directly. Its scalar score is a
  // per-clause constant, so collect it through calc_max_score() after a hit.
  bool got_result = false;
  ObDASRowID candidate_target = target;
  ObDASRowID candidate_curr_id = curr_id;
  double candidate_score = score;
  while (OB_SUCC(ret) && !got_result) {
    if (OB_FAIL(advance_driver_ops_to<ObDASRowID>(candidate_target, candidate_curr_id, candidate_score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance driver ops to", KR(ret), K(candidate_target));
    } else {
      bool all_hit = true;
      double probe_score = 0.0;
      for (int i = 0; OB_SUCC(ret) && i < probe_ops_.count() && all_hit; ++i) {
        ObIDASSearchOp *op = probe_ops_.at(i);
        bool hit = false;
        double child_score = 0.0;
        if (OB_ISNULL(op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null op", KR(ret), K(i));
        } else if (OB_FAIL(op->probe(candidate_curr_id, hit))) {
          LOG_WARN("failed to probe", KR(ret), K(i), K(target));
        } else if (!hit) {
          all_hit = false;
        } else if (is_scoring_ && OB_FAIL(op->calc_max_score(child_score))) {
          LOG_WARN("failed to get probe child score", KR(ret), K(i), K(candidate_curr_id));
        } else {
          probe_score += child_score;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (all_hit) {
        candidate_score += probe_score;
        got_result = true;
      } else {
        // continue to the next iteration
        if (OB_UNLIKELY(last_idx_ < 0 || last_idx_ >= driver_ops_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected last index", KR(ret), K(last_idx_), K(driver_ops_.count()));
        } else {
          ObIDASSearchOp *driver_op = driver_ops_.at(last_idx_);
          if (OB_ISNULL(driver_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null driver op", KR(ret), K(last_idx_));
          } else if (OB_FAIL(driver_op->next_rowid(candidate_target, candidate_score))) {
            LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid", KR(ret), K(last_idx_));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    curr_id = candidate_curr_id;
    score = candidate_score;
  }
  return ret;
}

template <typename RowType>
int ObDASConjunctionOp::advance_driver_ops_to(
    const RowType &target,
    RowType &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  int64_t &last_idx = std::is_same_v<RowType, ObDASNestedKey> ? nested_last_idx_ : last_idx_;
  RowType candidate_target = target;
  double candidate_score = score;
  bool all_advanced = false;
  while (OB_SUCC(ret) && !all_advanced) {
    bool target_changed = false;
    for (int i = 0; OB_SUCC(ret) && i < driver_ops_.count() && !target_changed; ++i) {
      RowType tmp_id;
      double tmp_score = 0.0;
      int cmp = 0;
      ObIDASSearchOp *op = nullptr;
      if (last_idx == i) {
        // already advanced to the target
      } else if (OB_ISNULL(op = driver_ops_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null op", KR(ret), K(i));
      } else if (OB_FAIL(op->advance_to(candidate_target, tmp_id, tmp_score))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to", KR(ret), K(i), K(candidate_target));
      } else if (OB_FAIL(compare_rowid(candidate_target, tmp_id, cmp))) {
        LOG_WARN("failed to compare rowids", KR(ret), K(candidate_target), K(tmp_id));
      } else if (cmp != 0) {
        if (cmp > 0) {
          // result should not be less than the target
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected candidate target > tmp id", KR(ret), K(i), K(candidate_target), K(tmp_id));
        } else {
          // this child advanced to a greater target, we should start a new round of iteration
          candidate_target = tmp_id;
          candidate_score = tmp_score;
          last_idx = i;
          target_changed = true;
        }
      } else {
        candidate_score += tmp_score;
      }
    }
    if (OB_SUCC(ret) && !target_changed) {
      // all children advanced to the candidate_target
      all_advanced = true;
    }
  }
  if (OB_SUCC(ret)) {
    curr_id = candidate_target;
    score = candidate_score;
  }
  return ret;
}

int ObDASConjunctionOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  is_first_nk_ = true;
  return next_impl<ObDASRowID>(next_id, score);
}

int ObDASConjunctionOp::do_next_rowid(ObDASNestedKey &next_nk, double &score)
{
  INIT_SUCC(ret);
  if (!is_async_nested_scope(scope_)) {
    ret = OB_NOT_IMPLEMENT;
  } else {
    if (is_first_nk_) {
      nested_last_idx_ = 0;
      is_first_nk_ = false;
    }
    ret = next_impl<ObDASNestedKey>(next_nk, score);
  }
  return ret;
}

template <typename RowType>
int ObDASConjunctionOp::next_impl(RowType &next_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t &last_idx = std::is_same_v<RowType, ObDASNestedKey> ? nested_last_idx_ : last_idx_;
  RowType init_target;
  ObIDASSearchOp *op = nullptr;
  score = 0.0;
  next_id.reset();
  if (OB_UNLIKELY(last_idx >= driver_ops_.count() || OB_ISNULL(op = driver_ops_.at(last_idx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", KR(ret), K(last_idx));
  } else if (has_fixed_score_bound() && cannot_compete_with_score(fixed_score_bound_)) {
    next_id.set_max();
    ret = OB_ITER_END;
  } else if (OB_FAIL(op->next_rowid(init_target, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to next rowid", KR(ret), K(last_idx));
  } else if (OB_FAIL(inner_advance_to<RowType>(init_target, next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance to", KR(ret), K(init_target));
  }
  return ret;
}

int ObDASConjunctionOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  // Probe-capable children degenerate to a point range in advance_shallow, so they can be
  // summed/intersected together with driver children without special handling.
  ObDASRowID maximum_min_id;
  ObDASRowID minimal_max_id;
  ObDASRowID candidate_target = target;
  double max_score = 0.0;
  int cmp = 0;
  if (has_fixed_score_bound() && cannot_compete_with_score(fixed_score_bound_)) {
    max_score_tuple = nullptr;
    ret = OB_ITER_END;
  }
  while (OB_SUCC(ret)) {
    max_score = 0.0;
    maximum_min_id.set_min();
    minimal_max_id.set_max();
    const MaxScoreTuple *tmp_max_score_tuple = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      ObIDASSearchOp *op = children_[i];
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null op", KR(ret), K(i));
      } else if (OB_FAIL(op->advance_shallow(candidate_target, inclusive, tmp_max_score_tuple))) {
        LOG_WARN("failed to advance shallow", KR(ret), K(i), K(candidate_target), K(inclusive));
      } else if (OB_ISNULL(tmp_max_score_tuple)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null max score tuple", KR(ret), K(i), K(candidate_target));
      } else {
        max_score += tmp_max_score_tuple->get_max_score();
        if (OB_FAIL(compare_rowid(maximum_min_id, tmp_max_score_tuple->get_min_id(), cmp))) {
          LOG_WARN("failed to compare rowids", KR(ret), K(i), K(maximum_min_id), K(tmp_max_score_tuple->get_min_id()));
        } else if (cmp < 0) {
          maximum_min_id = tmp_max_score_tuple->get_min_id();
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(compare_rowid(minimal_max_id, tmp_max_score_tuple->get_max_id(), cmp))) {
          LOG_WARN("failed to compare rowids", KR(ret), K(i), K(minimal_max_id), K(tmp_max_score_tuple->get_max_id()));
        } else if (cmp > 0) {
          minimal_max_id = tmp_max_score_tuple->get_max_id();
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(compare_rowid(maximum_min_id, minimal_max_id, cmp))) {
      LOG_WARN("failed to compare rowids", KR(ret), K(maximum_min_id), K(minimal_max_id));
    } else if (cmp > 0) {
      // all children's ranges should intersect, if not, we should try a new target
      candidate_target = maximum_min_id;
    } else {
      // all children's ranges intersect, we can return the result
      break;
    }
  }
  if (OB_SUCC(ret)) {
    max_score_tuple_.set(maximum_min_id, minimal_max_id, max_score);
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObDASConjunctionOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  if (has_fixed_score_bound()) {
    threshold = fixed_score_bound_;
  } else if (OB_FAIL(ObIDASSearchOp::do_calc_max_score(threshold))) {
    LOG_WARN("failed to calculate conjunction max score", KR(ret));
  }
  return ret;
}

int ObDASConjunctionOp::do_set_min_competitive_score(const double &threshold)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIDASSearchOp::do_set_min_competitive_score(threshold))) {
    LOG_WARN("failed to update conjunction threshold", KR(ret), K(threshold));
  } else if (is_scoring_ && 1 == driver_ops_.count() && !probe_ops_.empty()) {
    // Every probe is required and contributes at most its max score. With a single
    // enumerable driver, a row can beat the conjunction threshold only when the
    // driver score beats threshold - sum(probe max score). Do not apply this split
    // to multiple drivers because no single child owns the remaining score budget.
    double probe_max_score = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < probe_ops_.count(); ++i) {
      ObIDASSearchOp *probe_op = probe_ops_.at(i);
      double child_max_score = 0.0;
      if (OB_ISNULL(probe_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null probe op", KR(ret), K(i));
      } else if (OB_FAIL(probe_op->calc_max_score(child_max_score))) {
        LOG_WARN("failed to calculate probe max score", KR(ret), K(i));
      } else {
        probe_max_score += child_max_score;
      }
    }
    if (OB_SUCC(ret)) {
      ObIDASSearchOp *driver_op = driver_ops_.at(0);
      const double driver_threshold = threshold - probe_max_score;
      if (OB_ISNULL(driver_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null driver op", KR(ret));
      } else if (OB_FAIL(driver_op->set_min_competitive_score(driver_threshold))) {
        LOG_WARN("failed to forward threshold to conjunction driver",
                 KR(ret), K(threshold), K(probe_max_score), K(driver_threshold));
      } else {
        LOG_TRACE("forward threshold to conjunction driver",
                  K(threshold), K(probe_max_score), K(driver_threshold),
                  K(probe_ops_.count()));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
