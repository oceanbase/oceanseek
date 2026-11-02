/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS

#include <algorithm>
#include "sql/das/search/ob_das_async_scalar_ror_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{

namespace
{
// An asynchronous search index uses a four-column physical range and builds the posting
// term key from the three columns preceding DOC_ID.
static constexpr int64_t ASYNC_SEARCH_TERM_COLUMN_CNT = SEARCH_INDEX_DOC_ID;
static constexpr int64_t ASYNC_SEARCH_RANGE_COLUMN_CNT = SEARCH_INDEX_KEY_CNT;
}

int ObDASAsyncScalarRorUnionOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASAsyncScalarRorUnionOpParam &param =
      static_cast<const ObDASAsyncScalarRorUnionOpParam &>(op_param);
  if (OB_FAIL(ObDASDisjunctionFilterOp::do_init(op_param))) {
    LOG_WARN("failed to initialize async scalar ror union", K(ret));
  } else if (OB_ISNULL(scalar_rtdef_ = param.get_scalar_rtdef())
      || OB_UNLIKELY(common::OB_INVALID_ID == param.get_index_table_id()
                     || param.get_group_slot() < -1
                     || (param.get_direct_bind() && param.get_group_slot() < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar ror union binding", K(ret), KP_(scalar_rtdef),
        "index_table_id", param.get_index_table_id(),
        "group_slot", param.get_group_slot());
  } else if (FALSE_IT(index_table_id_ = param.get_index_table_id())) {
  } else if (FALSE_IT(group_slot_ = param.get_group_slot())) {
  } else if (FALSE_IT(direct_bind_ = param.get_direct_bind())) {
  } else if (FALSE_IT(direct_drain_enabled_ = param.is_direct_drain_enabled())) {
  } else if (FALSE_IT(independent_group_filter_ = param.is_independent_group_filter())) {
  } else if (independent_group_filter_
             && (!direct_bind_ || group_slot_ < 0 || direct_drain_enabled_
                 || op_param.get_is_scoring())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid independent Group filter binding", K(ret));
  } else {
    const ObDASScalarScanCtDef *ctdef = scalar_rtdef_->get_ctdef();
    if (OB_ISNULL(ctdef)
        || OB_UNLIKELY(!ctdef->is_async_ror_scan()
                       || (!ctdef->enable_new_false_range_
                           && !direct_drain_enabled_)
                       || (direct_drain_enabled_
                           && (OB_ISNULL(param.get_optional_ops())
                               || param.get_optional_ops()->empty()
                               || ctdef->is_scoring()
                               || op_param.get_is_scoring()
                               || DAS_SCOPE_BASE != scope_
                               || DAS_ROWID_TYPE_UINT64 != get_rowid_type())))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async scalar ror union definition", K(ret), KP(ctdef),
          K_(direct_drain_enabled), K_(scope), "rowid_type", get_rowid_type());
    }
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::ensure_range_children()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_rtdef_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar ror union is not initialized", K(ret));
  } else {
    const int64_t range_count = scalar_rtdef_->key_ranges_.count();
    const int64_t target_children_cnt = range_count > 0 ? range_count : 1;
    if (target_children_cnt > children_cnt_) {
      common::ObSEArray<ObIDASSearchOp *, 4> appended_ops;
      ObDASScopeId previous_scope = DAS_SCOPE_BASE;
      search_ctx_.enter_scope_for_op_creating(scope_, previous_scope);
      for (int64_t range_idx = children_cnt_;
           OB_SUCC(ret) && range_idx < target_children_cnt;
           ++range_idx) {
        ObIDASSearchOp *ror_op = nullptr;
        if (OB_FAIL(scalar_rtdef_->create_async_ror_op(
            search_ctx_, range_idx, index_table_id_, group_slot_,
            direct_bind_, ror_op))) {
          LOG_WARN("failed to create an appended async scalar ror", K(ret),
              K(range_idx), K(target_children_cnt));
        } else if (OB_ISNULL(ror_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null appended async scalar ror", K(ret), K(range_idx));
        } else if (OB_FAIL(appended_ops.push_back(ror_op))) {
          LOG_WARN("failed to collect an appended async scalar ror", K(ret), K(range_idx));
        }
      }
      search_ctx_.exit_scope_after_op_created(previous_scope);
      if (OB_SUCC(ret) && OB_FAIL(append_range_children(appended_ops))) {
        LOG_WARN("failed to append async scalar ror children", K(ret),
            K(target_children_cnt));
      }
    }
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::append_range_children(
    const common::ObIArray<ObIDASSearchOp *> &children)
{
  int ret = OB_SUCCESS;
  ObIDASSearchOp **expanded_children = nullptr;
  const int64_t append_count = children.count();
  int64_t expanded_count = 0;
  if (append_count <= 0) {
    // Nothing to append.
  } else if (OB_UNLIKELY(children_cnt_ < 0
      || append_count > INT64_MAX - children_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async scalar ror child count", K(ret),
        K_(children_cnt), K(append_count));
  } else if (FALSE_IT(expanded_count = children_cnt_ + append_count)) {
  } else if (OB_ISNULL(expanded_children = OB_NEW_ARRAY(
      ObIDASSearchOp *, &ctx_allocator(), expanded_count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to expand async scalar ror children", K(ret), K(expanded_count));
  } else {
    for (int64_t i = 0; i < children_cnt_; ++i) {
      expanded_children[i] = children_[i];
    }
    for (int64_t i = 0; i < append_count; ++i) {
      expanded_children[children_cnt_ + i] = children.at(i);
    }
    ctx_allocator().free(children_);
    children_ = expanded_children;
    children_cnt_ = expanded_count;
  }
  return ret;
}

void ObDASAsyncScalarRorUnionOp::reset_direct_cursors()
{
  ready_child_heap_.reset();
  heap_initialized_ = false;
  direct_single_cursor_ = DirectDrainCursor();
}

int ObDASAsyncScalarRorUnionOp::drain_active_child(
    const int64_t child_idx,
    ObDASRowID &current_rowid,
    const ObDASRowID &target,
    const ObDASRowID &boundary,
    ObDASRowIDBatchConsumer &consumer)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  uint64_t rowids[DRAIN_BATCH_SIZE];
  uint64_t current_id = 0;
  double score = 0.0;
  double next_score_after = 0.0;
  bool child_done = false;
  ObDASRowID next_id_after;
  if (OB_UNLIKELY(child_idx < 0 || child_idx >= children_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid active async scalar ror child index", K(ret),
        K(child_idx), K_(children_cnt));
  } else if (OB_ISNULL(children_[child_idx])
      || OB_UNLIKELY(!current_rowid.is_min()
                     && !current_rowid.is_normal())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid active async scalar ror child", K(ret), K(child_idx),
        KP(children_[child_idx]), K(current_rowid));
  } else if (children_[child_idx]->supports_direct_rowid_drain()) {
    if (OB_FAIL(children_[child_idx]->direct_drain_rowids(
            target.get_uint64(), boundary.get_uint64(), consumer, current_rowid))) {
      LOG_WARN("failed to directly drain async scalar ror child", K(ret),
          K(child_idx), K(target), K(boundary));
    } else if (OB_UNLIKELY(!current_rowid.is_max()
                           && (!current_rowid.is_normal()
                               || current_rowid.get_uint64() <= boundary.get_uint64()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async scalar ror direct-drain lookahead", K(ret), K(child_idx),
          K(boundary), K(current_rowid));
    }
  } else {
    // Compatibility fallback for ordered children. Production async posting
    // leaves implement direct drain and do not enter this materializing path.
    const uint64_t range_start = target.get_uint64();
    const uint64_t range_end = boundary.get_uint64();
    const bool need_advance = current_rowid.is_min()
        || (current_rowid.is_normal()
            && current_rowid.get_uint64() < range_start);
    if (!current_rowid.is_max()
        && need_advance
        && OB_FAIL(children_[child_idx]->advance_to(
            target, current_rowid, score))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        current_rowid.set_max();
      } else {
        LOG_WARN("failed to advance async scalar ror direct child", K(ret),
            K(child_idx), K(range_start));
      }
    }

    if (OB_SUCC(ret) && current_rowid.is_normal()
        && current_rowid.get_uint64() <= range_end) {
      current_id = current_rowid.get_uint64();
      if (OB_FAIL(consumer.consume(&current_id, 1))) {
        LOG_WARN("failed to consume async scalar ror current row", K(ret),
            K(child_idx));
      } else {
        while (OB_SUCC(ret) && !child_done) {
          count = 0;
          next_score_after = 0.0;
          if (OB_FAIL(children_[child_idx]->drain_until(
              boundary, DRAIN_BATCH_SIZE, rowids, nullptr, count,
              next_id_after, next_score_after))) {
            LOG_WARN("failed to drain async scalar ror direct child", K(ret),
                K(child_idx), K(range_start), K(range_end));
          } else if (count > 0 && OB_FAIL(consumer.consume(rowids, count))) {
            LOG_WARN("failed to consume async scalar ror direct batch", K(ret),
                K(child_idx), K(count));
          } else if (DRAIN_BATCH_SIZE == count) {
            // Continue until the child exposes a lookahead or reaches EOF.
          } else if (next_id_after.is_max()) {
            current_rowid.set_max();
            child_done = true;
          } else {
            current_rowid = next_id_after;
            child_done = true;
          }
        }
      }
    }

    if (OB_SUCC(ret) && !current_rowid.is_max()
        && OB_UNLIKELY(!current_rowid.is_normal()
                       || current_rowid.get_uint64() <= range_end)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async scalar ror direct lookahead", K(ret), K(child_idx),
          K(range_end), K(current_rowid));
    }
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_direct_drain_rowids(
    const uint64_t range_start,
    const uint64_t range_end,
    ObDASRowIDBatchConsumer &consumer,
    ObDASRowID &next_id_after)
{
  int ret = OB_SUCCESS;
  next_id_after.set_max();
  if (OB_UNLIKELY(!direct_drain_enabled_)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != get_rowid_type()
                         || range_start > range_end
                         || ready_child_heap_.count() > children_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar ror direct drain", K(ret), K(range_start),
        K(range_end), K_(children_cnt), K(ready_child_heap_.count()));
  } else {
    ObDASRowID target;
    ObDASRowID boundary;
    target.set_uint64(range_start);
    boundary.set_uint64(range_end);
    if (!heap_initialized_) {
      for (int64_t child_idx = 0;
           OB_SUCC(ret) && child_idx < children_cnt_;
           ++child_idx) {
        ObDASRowID current_rowid;
        current_rowid.set_min();
        if (OB_FAIL(drain_active_child(
            child_idx, current_rowid, target, boundary, consumer))) {
          LOG_WARN("failed to initialize async scalar ror direct child", K(ret),
              K(child_idx), K(range_start), K(range_end));
        } else if (current_rowid.is_normal()) {
          const DirectDrainCursor cursor(
              current_rowid.get_uint64(), child_idx);
          if (OB_FAIL(ready_child_heap_.push(cursor))) {
            LOG_WARN("failed to initialize async scalar ror direct heap", K(ret),
                K(child_idx));
          }
        }
      }
      if (OB_SUCC(ret)) {
        heap_initialized_ = true;
      }
    } else if (direct_single_cursor_.child_idx_ >= 0) {
      ObDASRowID current_rowid;
      current_rowid.set_uint64(direct_single_cursor_.rowid_);
      if (current_rowid.get_uint64() <= range_end
          && OB_FAIL(drain_active_child(
              direct_single_cursor_.child_idx_, current_rowid,
              target, boundary, consumer))) {
        LOG_WARN("failed to drain sole active async scalar ror child", K(ret),
            K_(direct_single_cursor), K(range_start), K(range_end));
      } else if (OB_SUCC(ret) && current_rowid.is_max()) {
        direct_single_cursor_ = DirectDrainCursor();
      } else if (OB_SUCC(ret)) {
        direct_single_cursor_.rowid_ = current_rowid.get_uint64();
      }
    } else {
      while (OB_SUCC(ret) && !ready_child_heap_.empty()
          && ready_child_heap_.top().rowid_ <= range_end) {
        DirectDrainCursor cursor = ready_child_heap_.top();
        ObDASRowID current_rowid;
        current_rowid.set_uint64(cursor.rowid_);
        if (OB_FAIL(ready_child_heap_.pop())) {
          LOG_WARN("failed to pop async scalar ror direct child", K(ret));
        } else if (OB_FAIL(drain_active_child(
            cursor.child_idx_, current_rowid, target, boundary, consumer))) {
          LOG_WARN("failed to drain ready async scalar ror direct child", K(ret),
              K(cursor.child_idx_), K(range_start), K(range_end));
        } else if (current_rowid.is_normal()) {
          cursor.rowid_ = current_rowid.get_uint64();
          if (OB_FAIL(ready_child_heap_.push(cursor))) {
            LOG_WARN("failed to requeue async scalar ror direct child", K(ret),
                K(cursor.child_idx_));
          }
        }
      }
    }
    if (OB_SUCC(ret) && direct_single_cursor_.child_idx_ < 0
        && 1 == ready_child_heap_.count()) {
      direct_single_cursor_ = ready_child_heap_.top();
      if (OB_FAIL(ready_child_heap_.pop())) {
        LOG_WARN("failed to detach sole async scalar ror child", K(ret));
        direct_single_cursor_ = DirectDrainCursor();
      }
    }
    if (OB_SUCC(ret) && direct_single_cursor_.child_idx_ >= 0) {
      if (OB_UNLIKELY(direct_single_cursor_.rowid_ <= range_end)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sole async scalar ror lookahead", K(ret),
            K(range_end), K_(direct_single_cursor));
      } else {
        next_id_after.set_uint64(direct_single_cursor_.rowid_);
      }
    } else if (OB_SUCC(ret) && !ready_child_heap_.empty()) {
      const DirectDrainCursor &next_cursor = ready_child_heap_.top();
      if (OB_UNLIKELY(next_cursor.child_idx_ < 0
                      || next_cursor.child_idx_ >= children_cnt_
                      || next_cursor.rowid_ <= range_end)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid async scalar ror direct heap root", K(ret),
            K(range_end), K(next_cursor.child_idx_), K(next_cursor.rowid_));
      } else {
        next_id_after.set_uint64(next_cursor.rowid_);
      }
    }
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_open()
{
  int ret = OB_SUCCESS;
  bool can_batch_merge = true;
  if (OB_FAIL(ensure_range_children())) {
    LOG_WARN("failed to prepare async scalar ror children on open", K(ret));
  } else if (direct_drain_enabled_
      && OB_FAIL(open_children(can_batch_merge))) {
    LOG_WARN("failed to open async scalar ror direct children", K(ret));
  } else if (!direct_drain_enabled_
      && OB_FAIL(ObDASDisjunctionFilterOp::do_open())) {
    LOG_WARN("failed to open async scalar ror union", K(ret));
  } else {
    if (direct_drain_enabled_) {
      reset_direct_cursors();
    }
    refresh_single_child_fast_path();
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_rescan()
{
  int ret = OB_SUCCESS;
  const int64_t opened_children_cnt = children_cnt_;
  if (OB_FAIL(ensure_range_children())) {
    LOG_WARN("failed to prepare async scalar ror children on rescan", K(ret));
  } else if (OB_FAIL(rescan_children(opened_children_cnt))) {
    LOG_WARN("failed to rescan async scalar ror children", K(ret));
  } else if (!direct_drain_enabled_ && OB_FAIL(reset_merge_cursor())) {
    LOG_WARN("failed to reset async scalar ror union cursor", K(ret));
  } else {
    if (direct_drain_enabled_) {
      reset_direct_cursors();
    }
    refresh_single_child_fast_path();
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_rescan_segment(
    const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  if (independent_group_filter_) {
    // The parent BMM is being bound to an FTS Segment. Reopen against our own
    // Search index/Group snapshot instead of forwarding that foreign version.
    if (OB_FAIL(do_rescan())) {
      LOG_WARN("failed to reset independent Group filter", K(ret));
    }
  } else if (direct_drain_enabled_) {
    if (OB_FAIL(ObIDASSearchOp::do_rescan_segment(range))) {
      LOG_WARN("failed to bind async scalar ror direct children", K(ret), K(range));
    } else {
      reset_direct_cursors();
    }
  } else if (OB_FAIL(ObDASDisjunctionFilterOp::do_rescan_segment(range))) {
    LOG_WARN("failed to rescan async scalar ror union segment", K(ret), K(range));
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_close()
{
  int ret = ObDASDisjunctionFilterOp::do_close();
  scalar_rtdef_ = nullptr;
  index_table_id_ = common::OB_INVALID_ID;
  group_slot_ = -1;
  direct_bind_ = false;
  direct_drain_enabled_ = false;
  independent_group_filter_ = false;
  use_single_child_fast_path_ = false;
  ready_child_heap_.reset();
  heap_initialized_ = false;
  direct_single_cursor_ = DirectDrainCursor();
  return ret;
}

void ObDASAsyncScalarRorUnionOp::refresh_single_child_fast_path()
{
  // enable_new_false_range_ keeps one placeholder leaf for an empty runtime
  // domain. Historical leaves remain allocated after a range-count shrink, so
  // use the current runtime range count rather than children_cnt_ to decide.
  const int64_t active_range_count = OB_ISNULL(scalar_rtdef_)
      ? 0 : scalar_rtdef_->key_ranges_.count();
  use_single_child_fast_path_ = active_range_count <= 1
      && children_cnt_ > 0
      && OB_NOT_NULL(children_[0])
      && DAS_ROWID_TYPE_UINT64 == get_rowid_type();
}

int ObDASAsyncScalarRorUnionOp::do_advance_to(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if (direct_drain_enabled_) {
    ret = OB_NOT_SUPPORTED;
  } else if (use_single_child_fast_path_) {
    if (iter_end_ || target.is_max()) {
      iter_end_ = true;
      ret = OB_ITER_END;
    } else {
      ObDASRowID min_target;
      if (OB_FAIL(children_[0]->advance_to(
          target.is_min() ? min_target : target, curr_id, score))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to advance single async scalar ror child", K(ret), K(target));
        }
      } else {
        score = is_scoring_ ? get_match_score() : 0.0;
      }
    }
  } else if (OB_FAIL(ObDASDisjunctionFilterOp::do_advance_to(
      target, curr_id, score))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to advance async scalar ror union", K(ret), K(target));
    }
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_next_rowid(
    ObDASRowID &next_id,
    double &score)
{
  int ret = OB_SUCCESS;
  if (direct_drain_enabled_) {
    ret = OB_NOT_SUPPORTED;
  } else if (use_single_child_fast_path_) {
    if (iter_end_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(children_[0]->next_rowid(next_id, score))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next single async scalar ror row", K(ret));
      }
    } else {
      score = is_scoring_ ? get_match_score() : 0.0;
    }
  } else if (OB_FAIL(ObDASDisjunctionFilterOp::do_next_rowid(next_id, score))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next async scalar ror union row", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  if (direct_drain_enabled_) {
    ret = OB_NOT_SUPPORTED;
  } else if (use_single_child_fast_path_) {
    if (iter_end_) {
      next_id_after.set_max();
    } else if (OB_FAIL(children_[0]->drain_until(
        boundary, capacity, out_ids, out_scores, count,
        next_id_after, next_score_after))) {
      LOG_WARN("failed to drain single async scalar ror child", K(ret),
          K(boundary), K(capacity));
    } else {
      const double match_score = is_scoring_ ? get_match_score() : 0.0;
      if (OB_NOT_NULL(out_scores)) {
        std::fill_n(out_scores, count, match_score);
      }
      next_score_after = next_id_after.is_normal() ? match_score : 0.0;
    }
  } else if (OB_FAIL(ObDASDisjunctionFilterOp::do_drain_until(
      boundary, capacity, out_ids, out_scores, count,
      next_id_after, next_score_after))) {
    LOG_WARN("failed to drain async scalar ror union", K(ret),
        K(boundary), K(capacity));
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (direct_drain_enabled_) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(ObDASDisjunctionFilterOp::do_advance_shallow(
      target, inclusive, max_score_tuple))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to advance shallow for async scalar ror union", K(ret),
          K(target), K(inclusive));
    }
  }
  return ret;
}

int ObDASAsyncScalarRorUnionOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  if (direct_drain_enabled_) {
    threshold = 0.0;
  } else if (OB_FAIL(ObDASDisjunctionFilterOp::do_calc_max_score(threshold))) {
    LOG_WARN("failed to calculate async scalar ror union max score", K(ret));
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASAsyncScalarRorOpParam &param = static_cast<const ObDASAsyncScalarRorOpParam &>(op_param);
  if (OB_ISNULL(param.get_scan_ctdef()) || OB_ISNULL(param.get_scan_rtdef())
      || OB_UNLIKELY(param.get_range_idx() < 0
                     || common::OB_INVALID_ID == param.get_index_table_id()
                     || param.get_group_slot() < -1
                     || (param.get_direct_bind() && param.get_group_slot() < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar op argument", K(ret), KP(param.get_scan_ctdef()),
        KP(param.get_scan_rtdef()), "range_idx", param.get_range_idx(),
        "index_table_id", param.get_index_table_id(),
        "group_slot", param.get_group_slot(),
        "direct_bind", param.get_direct_bind());
  } else if (OB_UNLIKELY(
      (DAS_SCOPE_BASE == scope_ && DAS_ROWID_TYPE_UINT64 != get_rowid_type())
      || (DAS_SCOPE_NESTED == scope_ && DAS_ROWID_TYPE_COMPACT != get_rowid_type()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async scalar op has unexpected rowid type for scope", K(ret),
        K_(scope), "rowid_type", get_rowid_type());
  } else {
    scalar_ctdef_ = param.get_scan_ctdef();
    scalar_rtdef_ = param.get_scan_rtdef();
    range_idx_ = param.get_range_idx();
    if (OB_UNLIKELY(scalar_rtdef_->get_ctdef() != scalar_ctdef_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("async scalar ctdef and rtdef mismatch", K(ret),
          KP_(scalar_ctdef), KP(scalar_rtdef_->get_ctdef()));
      scalar_ctdef_ = nullptr;
      scalar_rtdef_ = nullptr;
      range_idx_ = -1;
    } else {
      index_table_id_ = param.get_index_table_id();
      group_slot_ = param.get_group_slot();
      direct_bind_ = param.get_direct_bind();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::init_inv_idx_scan_param()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async scalar ctdef", K(ret), KP_(scalar_ctdef));
  } else if (OB_FAIL(search_ctx_.get_related_tablet_id(scalar_ctdef_, inv_idx_tablet_id_))) {
    LOG_WARN("failed to get async scalar index tablet id", K(ret), KPC_(scalar_ctdef));
  } else if (!inv_idx_scan_param_inited_) {
    // The posting reader selects an immutable index segment with version_range_.
    // Keep the SQL snapshot on the ordinary scan param for read-allowed checks.
    if (OB_FAIL(search_ctx_.init_scan_param(
        inv_idx_tablet_id_, scalar_ctdef_, scalar_rtdef_, inv_idx_scan_param_,
        false /* use_async_read_snapshot */))) {
      LOG_WARN("failed to init async scalar scan param", K(ret), K_(inv_idx_tablet_id));
    } else {
      inv_idx_scan_param_inited_ = true;
    }
  } else {
    ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), inv_idx_tablet_id_, inv_idx_scan_param_);
  }
  return ret;
}

int ObDASAsyncScalarRorOp::init_term_key()
{
  int ret = OB_SUCCESS;
  const common::ObNewRange *range = nullptr;
  const common::ObObj *start_objs = nullptr;
  const common::ObObj *end_objs = nullptr;
  void *datum_buf = nullptr;
  term_key_.reset();
  term_datums_ = nullptr;
  arena_allocator_.reuse();
  is_empty_range_ = false;
  if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async scalar ctdef/rtdef", K(ret),
        KP_(scalar_ctdef), KP_(scalar_rtdef));
  } else if (OB_UNLIKELY(range_idx_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar key range index", K(ret), K_(range_idx),
        "range_count", scalar_rtdef_->key_ranges_.count());
  } else if (range_idx_ >= scalar_rtdef_->key_ranges_.count()) {
    if (scalar_ctdef_->enable_new_false_range_) {
      // False ranges are compacted at runtime; a missing historical slot is empty.
      is_empty_range_ = true;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async scalar key range index", K(ret), K_(range_idx),
          "range_count", scalar_rtdef_->key_ranges_.count());
    }
  } else if (FALSE_IT(range = &scalar_rtdef_->key_ranges_.at(range_idx_))) {
  } else if (OB_UNLIKELY(
      ASYNC_SEARCH_RANGE_COLUMN_CNT != range->start_key_.get_obj_cnt()
      || range->start_key_.get_obj_cnt() != range->end_key_.get_obj_cnt())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unexpected async search range shape", K(ret), K_(scope), KPC(range));
  } else if (OB_ISNULL(start_objs = range->start_key_.get_obj_ptr())
      || OB_ISNULL(end_objs = range->end_key_.get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null search range rowkey", K(ret), KPC(range));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ASYNC_SEARCH_TERM_COLUMN_CNT; ++i) {
      if (start_objs[i].is_min_value() || start_objs[i].is_max_value()
          || end_objs[i].is_min_value() || end_objs[i].is_max_value()
          || start_objs[i] != end_objs[i]) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("async search index currently supports exact-value predicates only", K(ret), K(i),
            K(start_objs[i]), K(end_objs[i]));
      }
    }
  }

  if (OB_SUCC(ret) && !is_empty_range_) {
    const int64_t buf_size = sizeof(blocksstable::ObStorageDatum)
                           * ASYNC_SEARCH_TERM_COLUMN_CNT;
    if (OB_ISNULL(datum_buf = arena_allocator_.alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async scalar term datums", K(ret), K(buf_size));
    } else {
      term_datums_ = static_cast<blocksstable::ObStorageDatum *>(datum_buf);
      for (int64_t i = 0; OB_SUCC(ret) && i < ASYNC_SEARCH_TERM_COLUMN_CNT; ++i) {
        new (term_datums_ + i) blocksstable::ObStorageDatum();
        blocksstable::ObStorageDatum tmp_datum;
        if (OB_FAIL(tmp_datum.from_obj_enhance(start_objs[i]))) {
          LOG_WARN("failed to convert async scalar term object", K(ret), K(i), K(start_objs[i]));
        } else if (OB_FAIL(term_datums_[i].deep_copy(tmp_datum, arena_allocator_))) {
          LOG_WARN("failed to deep copy async scalar term datum", K(ret), K(i), K(tmp_datum));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(term_key_.assign(
          term_datums_, ASYNC_SEARCH_TERM_COLUMN_CNT))) {
        LOG_WARN("failed to assign async scalar term key", K(ret));
      }
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::init_posting_scan_param()
{
  int ret = OB_SUCCESS;
  posting_scan_param_.reset();
  query_flag_.set_use_block_cache();
  posting_scan_param_.search_key_ = &term_key_;
  posting_scan_param_.tablet_id_ = inv_idx_tablet_id_;
  posting_scan_param_.query_flag_ = query_flag_;
  posting_scan_param_.allocator_ = &posting_iter_allocator_;
  if (DAS_SCOPE_BASE == scope_ && search_ctx_.has_docid_range()) {
    if (!search_ctx_.get_docid_range_lo().is_min_value()) {
      posting_scan_param_.min_doc_id_ = search_ctx_.get_docid_range_lo().get_uint64();
    }
    if (!search_ctx_.get_docid_range_hi().is_max_value()) {
      posting_scan_param_.max_doc_id_ = search_ctx_.get_docid_range_hi().get_uint64();
    }
  }
  // Nested scope reads the posting-value block paired with the current doc_id and expands its
  // nested_key_list.
  posting_scan_param_.need_posting_value_ = (DAS_SCOPE_NESTED == scope_);
  // Scalar postings do not use BM25, but the shared posting reader validates
  // corpus statistics. A neutral average length keeps that contract satisfied.
  posting_scan_param_.avg_doc_length_ = 1.0;
  return ret;
}

int ObDASAsyncScalarRorOp::open_posting_iter_from_tablet()
{
  int ret = OB_SUCCESS;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  const storage::ObTabletHandle *pinned_tablet_handle = nullptr;
  if (OB_ISNULL(tsc_service_) && OB_ISNULL(tsc_service_ = MTL(storage::ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null access service", K(ret));
  } else if (OB_NOT_NULL(partition) && partition->ls_id_ != search_ctx_.get_ls_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async scalar index partition ls mismatch", K(ret), K(partition->ls_id_),
             "search_ls_id", search_ctx_.get_ls_id());
  } else if (OB_NOT_NULL(partition)
             && OB_FAIL(partition->get_pinned_index_tablet_handle(
                    inv_idx_tablet_id_, pinned_tablet_handle))) {
    LOG_WARN("failed to get pinned async scalar index tablet handle", K(ret),
             K_(inv_idx_tablet_id));
  } else if (OB_FAIL(tsc_service_->async_index_posting_scan(
      search_ctx_.get_ls_id(), pinned_tablet_handle, version_range_,
      inv_idx_scan_param_, posting_scan_param_, posting_iter_))) {
    LOG_WARN("failed to open async scalar posting scan", K(ret), K_(inv_idx_tablet_id), K_(version_range));
  }
  return ret;
}

void ObDASAsyncScalarRorOp::close_posting_iter()
{
  nested_key_list_reader_.reset();
  curr_nested_key_.reset();
  if (nullptr != posting_iter_) {
    posting_iter_->~ObInvSSTablePostingIter();
    posting_iter_allocator_.free(posting_iter_);
    posting_iter_ = nullptr;
  }
  posting_iter_allocator_.reuse();
}

void ObDASAsyncScalarRorOp::reset_leaf_runtime_state()
{
  close_posting_iter();
  posting_scan_param_.reset();
  version_range_.reset();
  term_key_.reset();
  term_datums_ = nullptr;
  arena_allocator_.reuse();
  rowid_allocators_[0].reuse();
  rowid_allocators_[1].reuse();
  rowid_allocator_idx_ = 0;
  is_empty_range_ = false;
  direct_cursor_pending_ = false;
  direct_pending_rowid_ = 0;
}

int ObDASAsyncScalarRorOp::position_nested_key(const ObDatum *target_nested_key)
{
  int ret = OB_SUCCESS;
  const ObDatum *nested_key_list = nullptr;
  nested_key_list_reader_.reset();
  curr_nested_key_.reset();
  if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar posting iter is not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->get_cur_nested_key_list(nested_key_list))) {
    LOG_WARN("failed to get nested key list from async scalar posting iter", K(ret));
  } else if (OB_ISNULL(nested_key_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null nested key list datum", K(ret));
  } else if (nested_key_list->is_null()) {
    // Top-level fields in a nested-enabled index use a NULL list and do not belong to nested scope.
    ret = OB_ITER_END;
  } else if (OB_FAIL(nested_key_list_reader_.init(*nested_key_list))) {
    LOG_WARN("failed to init nested key list reader", K(ret), KPC(nested_key_list));
  } else if (OB_ISNULL(target_nested_key)) {
    if (OB_FAIL(nested_key_list_reader_.first(curr_nested_key_))) {
      LOG_WARN("failed to get first nested key", K(ret));
    }
  } else if (OB_FAIL(nested_key_list_reader_.lower_bound(
      *target_nested_key, curr_nested_key_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to seek nested key list", K(ret), KPC(target_nested_key));
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::advance_to_next_nested_posting()
{
  int ret = OB_SUCCESS;
  bool positioned = false;
  nested_key_list_reader_.reset();
  curr_nested_key_.reset();
  if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar posting iter is not opened", K(ret));
  }
  while (OB_SUCC(ret) && !positioned) {
    if (OB_FAIL(posting_iter_->next())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance async scalar posting iter", K(ret));
      }
    } else if (OB_FAIL(position_nested_key(nullptr))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to position nested key", K(ret));
      }
    } else {
      positioned = true;
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::advance_nested_to(
    const ObDatum &target_doc_id,
    const ObDatum &target_nested_key)
{
  int ret = OB_SUCCESS;
  const ObDatum *current_doc_id = nullptr;
  uint64_t target_doc_id_value = 0;
  bool positioned = false;
  if (OB_UNLIKELY(target_doc_id.is_null() || target_doc_id.is_nop()
      || sizeof(uint64_t) != target_doc_id.len_
      || target_nested_key.is_null() || target_nested_key.is_nop()
      || 0 == target_nested_key.len_ || OB_ISNULL(target_nested_key.ptr_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nested async scalar target", K(ret), K(target_doc_id), K(target_nested_key));
  } else if (FALSE_IT(target_doc_id_value = target_doc_id.get_uint64())) {
  } else if (nested_key_list_reader_.is_inited()) {
    if (OB_FAIL(posting_iter_->get_cur_id(current_doc_id))) {
      LOG_WARN("failed to get current nested posting doc id", K(ret));
    } else if (OB_ISNULL(current_doc_id)
        || OB_UNLIKELY(sizeof(uint64_t) != current_doc_id->len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid current nested posting doc id", K(ret), KP(current_doc_id));
    } else {
      const uint64_t current_doc_id_value = current_doc_id->get_uint64();
      positioned = current_doc_id_value > target_doc_id_value
          || (current_doc_id_value == target_doc_id_value
              && curr_nested_key_.get_string().compare(
                  target_nested_key.get_string()) >= 0);
    }
  }
  if (OB_SUCC(ret) && !positioned) {
    if (OB_FAIL(posting_iter_->advance_to(target_doc_id))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance nested posting to target doc", K(ret), K(target_doc_id));
      }
    } else if (OB_FAIL(posting_iter_->get_cur_id(current_doc_id))) {
      LOG_WARN("failed to get advanced nested posting doc id", K(ret));
    } else if (OB_ISNULL(current_doc_id)
        || OB_UNLIKELY(sizeof(uint64_t) != current_doc_id->len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid advanced nested posting doc id", K(ret), KP(current_doc_id));
    } else {
      const ObDatum *nested_target = current_doc_id->get_uint64() == target_doc_id_value
          ? &target_nested_key : nullptr;
      if (OB_FAIL(position_nested_key(nested_target))) {
        if (OB_ITER_END == ret) {
          ret = advance_to_next_nested_posting();
        } else {
          LOG_WARN("failed to position nested posting", K(ret), K(target_doc_id), K(target_nested_key));
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::fetch_curr(ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  const common::ObDatum *id_datum = nullptr;
  score = 0.0;
  if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar posting iter is not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->get_cur_id(id_datum))) {
    LOG_WARN("failed to get current id from async scalar posting iter", K(ret));
  } else if (OB_ISNULL(id_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null async scalar posting id", K(ret));
  } else if (DAS_SCOPE_BASE == scope_) {
    rowid_allocators_[0].reuse();
    if (OB_FAIL(write_datum_to_rowid(*id_datum, curr_id, rowid_allocators_[0]))) {
      LOG_WARN("failed to build base async scalar rowid", K(ret));
    }
  } else if (!nested_key_list_reader_.is_inited()
      || curr_nested_key_.is_null()
      || curr_nested_key_.is_nop()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nested key list is not positioned", K(ret),
        K_(curr_nested_key), "reader_inited", nested_key_list_reader_.is_inited());
  } else {
    common::ObSEArray<common::ObDatum, 2> datums;
    ObArenaAllocator &rowid_allocator = rowid_allocators_[rowid_allocator_idx_];
    rowid_allocator.reuse();
    if (OB_FAIL(datums.push_back(*id_datum))) {
      LOG_WARN("failed to append async scalar doc id datum", K(ret));
    } else if (OB_FAIL(datums.push_back(curr_nested_key_))) {
      LOG_WARN("failed to append async scalar nested key datum", K(ret));
    } else if (OB_FAIL(search_ctx_.write_datums_to_rowid(
        scope_, datums, curr_id, rowid_allocator))) {
      LOG_WARN("failed to build nested async scalar rowid", K(ret), K_(scope));
    } else {
      rowid_allocator_idx_ = 1 - rowid_allocator_idx_;
    }
  }
  if (OB_SUCC(ret)) {
    score = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
  }
  return ret;
}

int ObDASAsyncScalarRorOp::get_bitmap_input_range(
    bool &available, uint64_t &lo, uint64_t &hi, int64_t &estimated_count)
{
  int ret = OB_SUCCESS;
  available = false;
  lo = UINT64_MAX;
  hi = 0;
  estimated_count = 0;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  if (DAS_SCOPE_BASE != scope_ || DAS_ROWID_TYPE_UINT64 != get_rowid_type()
      || search_ctx_.is_logical_table() || !version_range_.is_valid()
      || OB_ISNULL(partition)) {
    // No proven physical input domain: keep the existing chunked layout.
  } else if (OB_FAIL(partition->get_segment_rowid_range(
                 index_table_id_, version_range_, available, lo, hi))) {
    LOG_WARN("failed to get bitmap input domain", K(ret), K_(index_table_id));
  } else if (available && OB_NOT_NULL(scalar_rtdef_)) {
    ObDASSearchCost cost;
    if (OB_FAIL(scalar_rtdef_->get_cost(search_ctx_, cost))) {
      LOG_WARN("failed to estimate bitmap density", K(ret));
    } else if (cost.is_valid()) {
      estimated_count = cost.cost();
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_open()
{
  int ret = OB_SUCCESS;
  reset_leaf_runtime_state();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar op is not initialized", K(ret));
  } else if (OB_FAIL(init_inv_idx_scan_param())) {
    LOG_WARN("failed to init async scalar scan param", K(ret));
  } else if (OB_FAIL(init_term_key())) {
    LOG_WARN("failed to init async scalar term key", K(ret));
  } else if (is_empty_range_) {
    // The placeholder stays unopened until a later rescan supplies a range.
  } else if (OB_FAIL(init_posting_scan_param())) {
    LOG_WARN("failed to init async scalar posting scan param", K(ret));
  } else if (direct_bind_ && OB_FAIL(search_ctx_.resolve_direct_async_index_segment(
                 index_table_id_, inv_idx_tablet_id_, group_slot_,
                 version_range_, is_empty_range_))) {
    LOG_WARN("failed to resolve direct async scalar segment", K(ret),
             K_(index_table_id), K_(group_slot));
  } else if (is_empty_range_) {
    // No physical input in this partition and Group.
  } else if (!direct_bind_) {
    // The owning SegmentMerge actively calls rescan_segment to bind a physical Segment
    // for a non-direct Scalar.
    version_range_.reset();
    LOG_INFO("[ASYNC_INDEX_QUERY] scalar op open(lazy)", K_(inv_idx_tablet_id));
  } else if (OB_FAIL(open_posting_iter_from_tablet())) {
    LOG_WARN("failed to open async scalar posting iter", K(ret));
  }
  if (OB_FAIL(ret)) {
    reset_leaf_runtime_state();
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (is_empty_range_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar posting iter is not opened", K(ret));
  } else if (DAS_SCOPE_BASE == scope_) {
    if (OB_FAIL(posting_iter_->next())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to advance async scalar posting iter", K(ret));
      }
    }
  } else if (nested_key_list_reader_.is_inited()) {
    if (OB_FAIL(nested_key_list_reader_.next(curr_nested_key_))) {
      if (OB_ITER_END == ret) {
        ret = advance_to_next_nested_posting();
      } else {
        LOG_WARN("failed to get next nested key", K(ret));
      }
    }
  } else if (OB_FAIL(advance_to_next_nested_posting())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to position first nested posting", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(fetch_curr(next_id, score))) {
    LOG_WARN("failed to fetch async scalar posting row", K(ret));
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  if (is_empty_range_) {
    next_id_after.set_max();
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar posting iter is not opened", K(ret));
  } else if (OB_ISNULL(out_ids) || OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drain_until arguments", K(ret), KP(out_ids), K(capacity));
  } else {
    const uint64_t boundary_u64 = boundary.is_max() ? UINT64_MAX : boundary.get_uint64();
    const double match_score = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
    bool done = false;
    while (OB_SUCC(ret) && count < capacity && !done) {
      const uint64_t *doc_ids = nullptr;
      const uint64_t *token_freqs = nullptr;
      const uint64_t *doc_lengths = nullptr;
      int64_t batch_count = 0;
      if (OB_FAIL(posting_iter_->drain_until(
          boundary_u64, capacity - count, doc_ids,
          token_freqs, doc_lengths, batch_count))) {
        LOG_WARN("failed to drain async scalar posting iter", K(ret),
            K(boundary_u64), K(capacity), K(count));
      } else if (batch_count > 0) {
        MEMCPY(out_ids + count, doc_ids, batch_count * sizeof(uint64_t));
        if (nullptr != out_scores) {
          for (int64_t i = 0; i < batch_count; ++i) {
            out_scores[count + i] = match_score;
          }
        }
        count += batch_count;
      } else if (posting_iter_->is_end()) {
        next_id_after.set_max();
        done = true;
      } else if (OB_FAIL(fetch_curr(next_id_after, next_score_after))) {
        LOG_WARN("failed to fetch async scalar boundary lookahead", K(ret));
      } else {
        done = true;
      }
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::fetch_direct_cursor()
{
  int ret = OB_SUCCESS;
  const common::ObDatum *current_datum = nullptr;
  if (OB_FAIL(posting_iter_->get_cur_id(current_datum))) {
    LOG_WARN("failed to read async scalar direct cursor", K(ret));
  } else if (OB_ISNULL(current_datum)
             || OB_UNLIKELY(current_datum->is_null()
                            || current_datum->is_nop()
                            || sizeof(uint64_t) != current_datum->len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async scalar direct cursor", K(ret), KP(current_datum));
  } else {
    direct_pending_rowid_ = current_datum->get_uint64();
    direct_cursor_pending_ = true;
  }
  return ret;
}

int ObDASAsyncScalarRorOp::position_direct_cursor(
    const uint64_t range_start,
    bool &iter_end,
    bool &include_current)
{
  int ret = OB_SUCCESS;
  iter_end = false;
  include_current = false;
  if (direct_cursor_pending_ && direct_pending_rowid_ >= range_start) {
    // This row was returned as lookahead and has not been consumed yet.
    include_current = true;
  } else {
    uint64_t target_rowid = range_start;
    common::ObDatum target_datum;
    target_datum.ptr_ = reinterpret_cast<const char *>(&target_rowid);
    target_datum.set_uint(range_start);
    direct_cursor_pending_ = false;
    if (OB_FAIL(posting_iter_->advance_to_id(target_datum))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        iter_end = true;
      } else {
        LOG_WARN("failed to position async scalar direct drain", K(ret), K(range_start));
      }
    } else if (OB_FAIL(fetch_direct_cursor())) {
      LOG_WARN("failed to fetch positioned async scalar direct cursor", K(ret));
    } else {
      // advance_to_id() positions on the first row >= range_start. It is not
      // part of a previous direct batch and must be included exactly once.
      include_current = true;
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_direct_drain_rowids(
    const uint64_t range_start,
    const uint64_t range_end,
    ObDASRowIDBatchConsumer &consumer,
    ObDASRowID &next_id_after)
{
  int ret = OB_SUCCESS;
  bool done = false;
  bool include_current = false;
  next_id_after.set_max();
  if (OB_UNLIKELY(range_start > range_end)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar direct drain range", K(ret), K(range_start), K(range_end));
  } else if (OB_UNLIKELY(!supports_direct_rowid_drain())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async scalar posting does not support direct drain", K(ret),
        K_(scope), "rowid_type", get_rowid_type(), K_(is_scoring));
  } else if (is_empty_range_ || (OB_NOT_NULL(posting_iter_) && posting_iter_->is_end())) {
    direct_cursor_pending_ = false;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar posting iter is not opened", K(ret));
  } else if (OB_FAIL(position_direct_cursor(range_start, done, include_current))) {
    LOG_WARN("failed to prepare async scalar direct cursor", K(ret), K(range_start));
  } else {
    while (OB_SUCC(ret) && !done) {
      if (direct_cursor_pending_ && direct_pending_rowid_ > range_end) {
        next_id_after.set_uint64(direct_pending_rowid_);
        done = true;
      } else {
        const uint64_t *doc_ids = nullptr;
        const uint64_t *token_freqs = nullptr;
        const uint64_t *doc_lengths = nullptr;
        int64_t count = 0;
        const int drain_ret = include_current
            ? posting_iter_->drain_until_from_current(
                range_end, DAS_SEARCH_ROWID_DRAIN_BATCH_SIZE,
                doc_ids, token_freqs, doc_lengths, count)
            : posting_iter_->drain_until(
                range_end, DAS_SEARCH_ROWID_DRAIN_BATCH_SIZE,
                doc_ids, token_freqs, doc_lengths, count);
        include_current = false;
        direct_cursor_pending_ = false;
        if (OB_SUCCESS != drain_ret) {
          ret = drain_ret;
          LOG_WARN("failed to project async scalar posting batch", K(ret), K(range_end));
        } else if (count > 0) {
          // doc_ids points into the decoded posting block and is valid only
          // until the next iterator call, so the callback must finish here.
          if (OB_FAIL(consumer.consume(doc_ids, count))) {
            LOG_WARN("failed to synchronously consume async scalar posting batch",
                K(ret), K(count));
          }
        } else if (posting_iter_->is_end()) {
          done = true;
        } else if (OB_FAIL(fetch_direct_cursor())) {
          LOG_WARN("failed to fetch async scalar direct lookahead", K(ret));
        } else {
          next_id_after.set_uint64(direct_pending_rowid_);
          done = true;
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_advance_to(
    const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  common::ObDatum target_datum;
  common::ObDatum target_nested_key;
  score = 0.0;
  if (is_empty_range_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar posting iter is not opened", K(ret));
  } else if (OB_FAIL(get_datum_from_rowid(target, target_datum, 0))) {
    LOG_WARN("failed to convert async scalar target rowid", K(ret), K(target));
  } else if (DAS_SCOPE_NESTED == scope_
      && OB_FAIL(get_datum_from_rowid(target, target_nested_key, 1))) {
    LOG_WARN("failed to get async scalar target nested key", K(ret), K(target));
  } else if (DAS_SCOPE_BASE == scope_ && OB_FAIL(posting_iter_->advance_to(target_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance async scalar posting iter to target", K(ret), K(target));
    }
  } else if (DAS_SCOPE_NESTED == scope_
      && OB_FAIL(advance_nested_to(target_datum, target_nested_key))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance nested async scalar posting", K(ret), K(target));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(fetch_curr(curr_id, score))) {
    LOG_WARN("failed to fetch current async scalar posting row", K(ret));
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  const double boost_weight = scalar_rtdef_->get_boost_weight();
  if (is_empty_range_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret), K(target));
  } else if (inclusive) {
    max_score_tuple_.set(target, target, is_scoring_ ? boost_weight : 0.0);
    max_score_tuple = &max_score_tuple_;
  } else if (OB_FAIL(ObIDASSearchOp::do_advance_shallow(
      target, inclusive, max_score_tuple))) {
    LOG_WARN("failed to advance shallow", K(ret), K(target));
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  threshold = !is_empty_range_ && is_scoring_
      ? scalar_rtdef_->get_boost_weight()
      : 0.0;
  return ret;
}

int ObDASAsyncScalarRorOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar op is not initialized", K(ret));
  } else {
    reset_leaf_runtime_state();
    if (OB_FAIL(init_inv_idx_scan_param())) {
      LOG_WARN("failed to init async scalar scan param on rescan", K(ret));
    } else if (OB_FAIL(init_term_key())) {
      LOG_WARN("failed to init async scalar term key on rescan", K(ret));
    } else if (is_empty_range_) {
      // A historical leaf can become active again on a later rescan.
    } else if (OB_FAIL(init_posting_scan_param())) {
      LOG_WARN("failed to init async scalar posting scan param on rescan", K(ret));
    } else if (direct_bind_ && OB_FAIL(search_ctx_.resolve_direct_async_index_segment(
                   index_table_id_, inv_idx_tablet_id_, group_slot_,
                   version_range_, is_empty_range_))) {
      LOG_WARN("failed to resolve direct async scalar segment on rescan", K(ret),
               K_(index_table_id), K_(group_slot));
    } else if (is_empty_range_) {
      // No physical input in this partition and Group.
    } else if (!direct_bind_) {
      LOG_INFO("[ASYNC_INDEX_QUERY] scalar op rescan(lazy)", K_(inv_idx_tablet_id));
    } else if (OB_FAIL(open_posting_iter_from_tablet())) {
      LOG_WARN("failed to open async scalar posting iter on rescan", K(ret));
    }
    if (OB_FAIL(ret)) {
      reset_leaf_runtime_state();
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  direct_cursor_pending_ = false;
  direct_pending_rowid_ = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("async scalar op is not initialized", K(ret));
  } else if (OB_UNLIKELY(direct_bind_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("direct async scalar rejects segment rescan", K(ret),
             K_(index_table_id), K_(group_slot), K(range));
  } else {
    close_posting_iter();
    version_range_ = range;
    if (is_empty_range_) {
      // An empty slot does not bind a physical segment.
    } else if (OB_FAIL(init_posting_scan_param())) {
      LOG_WARN("failed to init posting param on async scalar segment switch", K(ret));
    } else if (OB_FAIL(open_posting_iter_from_tablet())) {
      LOG_WARN("failed to open posting iter on async scalar segment switch", K(ret));
    }
    if (OB_FAIL(ret)) {
      reset_leaf_runtime_state();
    }
  }
  return ret;
}

int ObDASAsyncScalarRorOp::do_close()
{
  int ret = OB_SUCCESS;
  reset_leaf_runtime_state();
  inv_idx_scan_param_.destroy_schema_guard();
  inv_idx_scan_param_.snapshot_.reset();
  inv_idx_scan_param_.destroy();
  inv_idx_scan_param_inited_ = false;
  tsc_service_ = nullptr;
  index_table_id_ = common::OB_INVALID_ID;
  group_slot_ = -1;
  direct_bind_ = false;
  arena_allocator_.reset();
  rowid_allocators_[0].reset();
  rowid_allocators_[1].reset();
  rowid_allocator_idx_ = 0;
  scalar_ctdef_ = nullptr;
  scalar_rtdef_ = nullptr;
  range_idx_ = -1;
  is_empty_range_ = false;
  is_inited_ = false;
  return ret;
}

} // namespace sql
} // namespace oceanbase
