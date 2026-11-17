/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_sort_topn_iter.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "sql/das/iter/ob_das_profile_iter.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
bool ObDASSortTopNHeap::Compare::operator()(const ObDASSortTopNEntry *left,
                                            const ObDASSortTopNEntry *right)
{
  bool before = false;
  int &ret = ret_;
  int cmp = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sort_ctdef_) || OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid hybrid sort heap comparison", K(ret), KP(sort_ctdef_), KP(left), KP(right));
  } else if (OB_FAIL(ObDASSearchUtils::compare_hybrid_sort_values(
        *sort_ctdef_, left->sort_values_, right->sort_values_, cmp))) {
    LOG_WARN("failed to compare hybrid sort heap entries", K(ret));
  } else {
    before = cmp < 0;
  }
  return before;
}

int ObDASSortTopNHeap::init(const ObDASHybridSearchSortCtDef &sort_ctdef, int64_t capacity, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (sort_ctdef_ != nullptr || capacity <= 0 || capacity > INT64_MAX / 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hybrid sort heap initialization", K(ret), KP_(sort_ctdef), K(capacity));
  } else if (OB_FAIL(sort_ctdef.validate())) {
    LOG_WARN("failed to validate hybrid sort definition", K(ret));
  } else if (!sort_ctdef.has_unique_shard_doc()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hybrid sort heap requires a unique shard document key", K(ret),
             "sort_count", sort_ctdef.sort_exprs_.count());
  } else if (OB_FAIL(shard_doc_entries_.create(MAX(capacity * 2, 16), ObMemAttr(MTL_ID(), "HSSortTopNHeap")))) {
    LOG_WARN("failed to create hybrid sort __shard_doc map", K(ret));
  } else {
    sort_ctdef_ = &sort_ctdef;
    capacity_ = capacity;
    allocator_ = &allocator;
    cmp_.reset();
    cmp_.sort_ctdef_ = &sort_ctdef;
  }
  return ret;
}

void ObDASSortTopNHeap::free_entry(ObDASSortTopNEntry *entry)
{
  if (entry != nullptr && allocator_ != nullptr) {
    for (int64_t i = 0; i < entry->sort_values_.count(); ++i) {
      if (!entry->sort_values_.at(i).is_null()) {
        allocator_->free(const_cast<char *>(entry->sort_values_.at(i).ptr_));
      }
    }
    entry->~ObDASSortTopNEntry();
    allocator_->free(entry);
  }
}

void ObDASSortTopNHeap::reset()
{
  int tmp_ret = OB_SUCCESS;
  if (shard_doc_entries_.created() && OB_TMP_FAIL(shard_doc_entries_.destroy())) {
    LOG_WARN_RET(tmp_ret, "failed to destroy hybrid sort __shard_doc map", K(tmp_ret));
  }
  // Cleanup must not compare entries: the comparator may already hold an execution error.
  for (int64_t i = 0; i < heap_.count(); ++i) {
    free_entry(heap_.at(i));
  }
  heap_.reset();
  cmp_.reset();
  sort_ctdef_ = nullptr;
  allocator_ = nullptr;
  capacity_ = 0;
}

int ObDASSortTopNHeap::offer(const ObDASSortTopNEntry &entry)
{
  int ret = OB_SUCCESS;
  ObDASSortTopNEntry *copy = nullptr;
  bool in_heap = false;
  if (sort_ctdef_ == nullptr || entry.sort_values_.count() != sort_ctdef_->sort_exprs_.count()) {
    ret = OB_NOT_INIT;
    LOG_WARN("hybrid sort heap is uninitialized or has a mismatched tuple", K(ret), KP_(sort_ctdef),
             "value_count", entry.sort_values_.count(),
             "sort_count", sort_ctdef_ == nullptr ? 0 : sort_ctdef_->sort_exprs_.count());
  } else if (OB_FAIL(cmp_.get_error_code())) {
    LOG_WARN("hybrid sort heap comparison previously failed", K(ret));
  } else {
    const int64_t shard_doc_index = sort_ctdef_->unique_shard_doc_index();
    const ObDatum &shard_doc_value = entry.sort_values_.at(shard_doc_index);
    ObDASSortTopNEntry *existing = nullptr;
    int cmp = 0;
    bool need_insert = true;
    if (shard_doc_value.is_null() || shard_doc_value.len_ != 32) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid hybrid sort shard document key", K(ret), K(shard_doc_index),
               "is_null", shard_doc_value.is_null(), "length", static_cast<int64_t>(shard_doc_value.len_));
    } else if (OB_FAIL(shard_doc_entries_.get_refactored(shard_doc_value.get_string(), existing))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to find hybrid sort __shard_doc", K(ret));
      }
    } else if (OB_ISNULL(existing)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hybrid sort __shard_doc has no retained entry", K(ret));
    } else {
      need_insert = false;
      // Repeated complete documents must agree on both their sort values and raw query scores.
      if (OB_FAIL(ObDASSearchUtils::compare_hybrid_sort_values(
            *sort_ctdef_, entry.sort_values_, existing->sort_values_, cmp))) {
        LOG_WARN("failed to compare duplicate hybrid sort entries", K(ret), K(shard_doc_index));
      } else if (cmp != 0 || entry.raw_score_ != existing->raw_score_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("duplicate hybrid sort document has conflicting keys or score", K(ret),
                 "keys_differ", cmp != 0, "raw_score_differs", entry.raw_score_ != existing->raw_score_);
      }
    }
    if (OB_SUCC(ret) && need_insert && full()) {
      if (OB_FAIL(ObDASSearchUtils::compare_hybrid_sort_values(
            *sort_ctdef_, entry.sort_values_, top().sort_values_, cmp))) {
        LOG_WARN("failed to compare hybrid sort candidate with heap threshold", K(ret), K_(capacity));
      } else if (cmp >= 0) {
        need_insert = false;
      }
    }
    if (OB_SUCC(ret) && need_insert) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObDASSortTopNEntry)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate hybrid sort entry", K(ret));
      } else {
        copy = new (buf) ObDASSortTopNEntry();
        if (OB_FAIL(ObDASSearchUtils::deep_copy_hybrid_sort_entry(entry, *copy, *allocator_))) {
          LOG_WARN("failed to copy hybrid sort entry", K(ret));
        }
      }
      if (OB_SUCC(ret) && full()) {
        ObDASSortTopNEntry *old = heap_.top();
        if (OB_FAIL(shard_doc_entries_.erase_refactored(old->sort_values_.at(shard_doc_index).get_string()))) {
          LOG_WARN("failed to remove hybrid sort __shard_doc entry", K(ret));
        } else {
          // The heap is nonempty: replace_top installs copy before sifting and may then return
          // a comparator error. Ownership transfers even on that error; old is no longer retained.
          in_heap = true;
          if (OB_FAIL(heap_.replace_top(copy))) {
            LOG_WARN("failed to replace hybrid sort heap top", K(ret));
          }
          free_entry(old); // free the old entry even if the replace_top fails.
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(cmp_.get_error_code())) {
            LOG_WARN("failed to compare entries while replacing hybrid sort heap top", K(ret));
          } else if (OB_FAIL(shard_doc_entries_.set_refactored(copy->sort_values_.at(shard_doc_index).get_string(), copy))) {
            LOG_WARN("failed to record hybrid sort __shard_doc entry", K(ret));
          }
        }
      } else if (OB_SUCC(ret)) {
        if (OB_FAIL(heap_.push(copy, in_heap))) {
          LOG_WARN("failed to push hybrid sort heap entry", K(ret));
        } else if (OB_FAIL(cmp_.get_error_code())) {
          LOG_WARN("failed to compare entries while pushing hybrid sort heap entry", K(ret));
        } else if (OB_FAIL(shard_doc_entries_.set_refactored(copy->sort_values_.at(shard_doc_index).get_string(), copy))) {
          LOG_WARN("failed to record hybrid sort __shard_doc entry", K(ret));
        }
      }
    }
  }
  // failed before pushed into heap or failed to push into heap, free the copy.
  if (OB_NOT_NULL(copy) && !in_heap) {
    free_entry(copy);
  }
  return ret;
}

int ObDASSortTopNHeap::drain_sorted(ObIArray<ObDASSortTopNEntry> &rows, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  rows.reset();
  if (OB_FAIL(cmp_.get_error_code())) {
    LOG_WARN("hybrid sort heap comparison previously failed", K(ret));
  } else if (OB_FAIL(rows.prepare_allocate(heap_.count()))) {
    LOG_WARN("failed to allocate hybrid sort heap output", K(ret), "row_count", heap_.count());
  }
  int64_t output = heap_.count();
  // Pop the worst entry into the output backwards, producing final ORDER BY order.
  while (OB_SUCC(ret) && !heap_.empty()) {
    ObDASSortTopNEntry *worst = heap_.top();
    if (OB_FAIL(ObDASSearchUtils::deep_copy_hybrid_sort_entry(*worst, rows.at(--output), allocator))) {
      LOG_WARN("failed to copy hybrid sort entry", K(ret));
    } else if (OB_FAIL(shard_doc_entries_.erase_refactored(worst->sort_values_.at(sort_ctdef_->unique_shard_doc_index()).get_string()))) {
      LOG_WARN("failed to remove hybrid sort __shard_doc entry", K(ret));
    } else {
      // A nonempty pop removes worst before sifting the remaining entries. Free the removed
      // entry even when that sift returns a comparator error; reset() only owns the survivors.
      if (OB_FAIL(heap_.pop())) {
        LOG_WARN("failed to pop hybrid sort heap entry", K(ret));
      }
      free_entry(worst);
      if (OB_SUCC(ret) && OB_FAIL(cmp_.get_error_code())) {
        LOG_WARN("failed to compare entries while popping hybrid sort heap entry", K(ret));
      }
    }
  }
  return ret;
}

namespace
{
int init_sort_projection(ObDASSearchCtx &ctx, ObEvalCtx &eval, const int64_t count,
                         const bool include_identity)
{
  int ret = OB_SUCCESS;
  const ObDASSortRuntimeInfo &info = ctx.get_sort_info();
  if (include_identity && OB_FAIL(ctx.get_rowid_exprs().at(0)->init_vector_for_write(eval, VEC_UNIFORM, count))) {
    LOG_WARN("failed to initialize sort rowid projection", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < info.fusion_ctdef_->score_exprs_.count(); ++i) {
    if (OB_FAIL(info.fusion_ctdef_->score_exprs_.at(i)->init_vector_for_write(eval, VEC_UNIFORM, count))) {
      LOG_WARN("failed to initialize sort score projection", K(ret), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < info.sort_ctdef_->sort_exprs_.count(); ++i) {
    if ((include_identity && info.sort_ctdef_->unique_shard_doc_flags_.at(i))
        || info.sort_ctdef_->score_sort_flags_.at(i)) {
      if (OB_FAIL(info.sort_ctdef_->sort_exprs_.at(i)->init_vector_for_write(eval, VEC_UNIFORM, count))) {
        LOG_WARN("failed to initialize produced sort key", K(ret), K(i));
      }
    }
  }
  return ret;
}

void project_sort_score(ObDASSearchCtx &ctx, ObEvalCtx &eval, int64_t index, double raw_score)
{
  const ObDASSortRuntimeInfo &info = ctx.get_sort_info();
  const double final_score = raw_score * info.weight_;
  for (int64_t i = 0; i < info.fusion_ctdef_->score_exprs_.count(); ++i) {
    ObExpr *expr = info.fusion_ctdef_->score_exprs_.at(i);
    expr->get_vector(eval)->set_double(index,
        i + 1 == info.fusion_ctdef_->score_exprs_.count() ? final_score : raw_score);
    expr->set_evaluated_projected(eval);
  }
  for (int64_t i = 0; i < info.sort_ctdef_->sort_exprs_.count(); ++i) {
    if (info.sort_ctdef_->score_sort_flags_.at(i)) {
      ObExpr *expr = info.sort_ctdef_->sort_exprs_.at(i);
      expr->get_vector(eval)->set_double(index, final_score);
      expr->set_evaluated_projected(eval);
    }
  }
}

} // namespace

int ObDASSortCandidateIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (nullptr == candidates_ || nullptr == rowkey_ || capacity <= 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid sort candidate read state", K(ret), KP_(candidates), KP_(rowkey), K(capacity));
  } else if (pos_ == candidates_->count()) {
    ret = OB_ITER_END;
  } else {
    count = MIN(MIN(capacity, max_size_), candidates_->count() - pos_);
    if (OB_FAIL(rowkey_->init_vector_for_write(*eval_ctx_, VEC_UNIFORM, count))) {
      LOG_WARN("failed to project finite sort candidate input", K(ret), K(count));
    } else {
      for (int64_t i = 0; i < count; ++i) {
        rowkey_->get_vector(*eval_ctx_)->set_uint(i, candidates_->at(pos_ + i).rowid_);
      }
      pos_ += count;
      rowkey_->set_evaluated_projected(*eval_ctx_);
    }
  }
  return ret;
}

ObDASSortValueLookupIter::ObDASSortValueLookupIter()
  : ObDASLocalLookupIter(),
    search_ctx_(nullptr),
    sort_ctdef_(nullptr),
    allocator_(ObMemAttr(MTL_ID(), "SortValueLookup")),
    candidate_allocator_(ObMemAttr(MTL_ID(), "SortCandidates")),
    private_eval_ctx_(nullptr),
    lookup_children_{nullptr, nullptr},
    candidates_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    returned_indices_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    batch_finished_(true)
{}

ObDASSortValueLookupIter::~ObDASSortValueLookupIter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(destroy_lookup())) {
    LOG_WARN("failed to destroy sort lookup", K(ret));
  }
  if (nullptr != private_eval_ctx_) {
    private_eval_ctx_->~ObEvalCtx();
    private_eval_ctx_ = nullptr;
  }
}

int ObDASSortValueLookupIter::refresh_parameter_frames()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &source = *search_ctx_->get_eval_ctx();
  const ObExprFrameInfo *layout = search_ctx_->get_sort_remote_frame_info();
  const ObPhysicalPlanCtx *plan_ctx = source.exec_ctx_.get_physical_plan_ctx();
  const ObPhysicalPlan *plan = nullptr;
  if (nullptr == layout && nullptr != plan_ctx && nullptr != (plan = plan_ctx->get_phy_plan())) {
    layout = &plan->get_expr_frame_info();
  }
  if (nullptr == layout || nullptr == private_eval_ctx_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("missing sort lookup parameter frame state", K(ret), KP(layout), KP_(private_eval_ctx));
  } else {
    for (int64_t i = 0; i < layout->param_frame_.count(); ++i) {
      const ObFrameInfo &frame = layout->param_frame_.at(i);
      if (nullptr != source.frames_[frame.frame_idx_]) {
        MEMCPY(private_eval_ctx_->frames_[frame.frame_idx_], source.frames_[frame.frame_idx_], frame.frame_size_);
      }
    }
  }
  return ret;
}

int ObDASSortValueLookupIter::init_lookup(ObDASSearchCtx &ctx)
{
  int ret = OB_SUCCESS;
  search_ctx_ = &ctx;
  sort_ctdef_ = ctx.get_sort_info().sort_ctdef_;
  const ObDASScanRtDef *source = ctx.get_sort_info().scan_template_;
  if (nullptr == sort_ctdef_ || nullptr == source || !sort_ctdef_->has_sort_value_lookup_
      || sort_ctdef_->sort_value_scan_ctdef_.rowkey_exprs_.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sort lookup initialization", K(ret), KP_(sort_ctdef), KP(source));
  } else if (OB_FAIL(init_private_frames())) {
    LOG_WARN("failed to initialize sort lookup expression frames", K(ret));
  } else {
    int64_t size = source->get_serialize_size();
    char *buf = static_cast<char *>(allocator_.alloc(size));
    int64_t pos = 0;
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate sort lookup runtime buffer", K(ret), K(size));
    } else if (OB_FAIL(source->serialize(buf, size, pos))) {
      LOG_WARN("failed to copy sort lookup runtime", K(ret));
    } else {
      pos = 0;
      if (OB_FAIL(rtdef_.deserialize(buf, size, pos))) {
        LOG_WARN("failed to restore sort lookup runtime", K(ret));
      } else {
        rtdef_.timeout_ts_ = source->timeout_ts_; // Local clone: do not adjust the RPC clock twice.
        rtdef_.ctdef_ = &sort_ctdef_->sort_value_scan_ctdef_;
        rtdef_.eval_ctx_ = private_eval_ctx_;
        rtdef_.stmt_allocator_.set_alloc(&allocator_);
        rtdef_.scan_allocator_.set_alloc(&allocator_);
        rtdef_.limit_param_.limit_ = -1;
        rtdef_.limit_param_.offset_ = 0;
        rtdef_.scan_flag_.scan_order_ = ObQueryFlag::KeepOrder;
        if (OB_FAIL(rtdef_.init_pd_op(private_eval_ctx_->exec_ctx_, sort_ctdef_->sort_value_scan_ctdef_))) {
          LOG_WARN("failed to initialize sort lookup projector", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObDASScanCtDef &scan = sort_ctdef_->sort_value_scan_ctdef_;
    ObDASIterParam input_param;
    input_param.eval_ctx_ = private_eval_ctx_;
    input_param.exec_ctx_ = &private_eval_ctx_->exec_ctx_;
    input_param.max_size_ = private_eval_ctx_->max_batch_size_;
    input_param.output_ = &scan.rowkey_exprs_;
    ObDASScanIterParam scan_param;
    scan_param.eval_ctx_ = private_eval_ctx_;
    scan_param.exec_ctx_ = &private_eval_ctx_->exec_ctx_;
    scan_param.max_size_ = private_eval_ctx_->max_batch_size_;
    scan_param.output_ = &scan.result_output_;
    scan_param.scan_ctdef_ = &scan;
    if (OB_FAIL(candidate_iter_.init(input_param))) {
      LOG_WARN("failed to initialize finite sort input", K(ret));
    } else if (OB_FAIL(scan_iter_.init(scan_param))) {
      LOG_WARN("failed to initialize sort main-table ScanIter", K(ret));
    } else {
      ObDASLocalLookupIterParam param;
      param.eval_ctx_ = private_eval_ctx_;
      param.exec_ctx_ = &private_eval_ctx_->exec_ctx_;
      param.max_size_ = private_eval_ctx_->max_batch_size_;
      param.output_ = &scan.result_output_;
      param.index_ctdef_ = &scan;
      param.index_rtdef_ = &rtdef_;
      param.lookup_ctdef_ = &scan;
      param.lookup_rtdef_ = &rtdef_;
      param.index_table_iter_ = &candidate_iter_;
      param.data_table_iter_ = &scan_iter_;
      param.rowkey_exprs_ = &scan.rowkey_exprs_;
      param.trans_desc_ = ctx.get_sort_tx_desc();
      // LocalLookup only reads the caller's snapshot and copies it into lookup_param_.
      param.snapshot_ = const_cast<transaction::ObTxReadSnapshot *>(ctx.get_snapshot());
      param.default_batch_row_count_ = ObDASLookupIterParam::LOCAL_LOOKUP_ITER_DEFAULT_BATCH_ROW_COUNT;
      if (OB_FAIL(ObDASIter::init(param))) {
        LOG_WARN("failed to initialize inherited sort lookup", K(ret));
      } else {
        lookup_children_[0] = &candidate_iter_;
        lookup_children_[1] = &scan_iter_;
        children_ = lookup_children_;
        children_cnt_ = 2;
        scan_iter_.set_scan_param(get_lookup_param());
        set_async_search_ctx(ctx.is_async_index_query() ? &ctx : nullptr);
        if (OB_FAIL(candidate_indices_.create(MAX(max_size_ * 2, 16), ObMemAttr(MTL_ID(), "HSortRowMap")))) {
          LOG_WARN("failed to initialize sort candidate hash", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASSortValueLookupIter::prepare_execution()
{
  int ret = OB_SUCCESS;
  // A new execution can have a different transaction/snapshot even on the same
  // tablet. Ordinary storage rescan only reuses ranges, so refresh its context.
  lookup_param_.need_switch_param_ = !is_first_lookup_;
  snapshot_ = const_cast<transaction::ObTxReadSnapshot *>(search_ctx_->get_snapshot());
  trans_desc_ = search_ctx_->get_sort_tx_desc();
  if (OB_FAIL(ObDASIter::reuse())) {
    LOG_WARN("failed to reuse sort lookup execution", K(ret));
  } else if (OB_FAIL(refresh_parameter_frames())) {
    LOG_WARN("failed to refresh private sort parameters", K(ret));
  } else if (OB_ISNULL(snapshot_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort lookup snapshot is null", K(ret));
  } else if (OB_FAIL(lookup_param_.snapshot_.assign(*snapshot_))) {
    LOG_WARN("failed to refresh sort lookup snapshot", K(ret));
  } else {
    rtdef_.timeout_ts_ = search_ctx_->get_sort_info().scan_template_->timeout_ts_;
    rtdef_.row_scan_cnt_ = 0;
    lookup_param_.row_scan_cnt_ = &rtdef_.row_scan_cnt_;
    lookup_param_.timeout_ = rtdef_.timeout_ts_;
    lookup_param_.trans_desc_ = rtdef_.is_for_foreign_check_ ? trans_desc_ : nullptr;
    if (nullptr != trans_desc_) {
      lookup_param_.tx_id_ = trans_desc_->get_tx_id();
    } else {
      lookup_param_.tx_id_.reset();
    }
    batch_finished_ = true;
    returned_indices_.reuse();
  }
  return ret;
}

int ObDASSortValueLookupIter::prepare_batch_binding()
{
  int ret = OB_SUCCESS;
  // Bind before parent reuse: LocalLookup compares the old and new tablet IDs.
  set_tablet_id(search_ctx_->get_main_tablet_id());
  set_ls_id(search_ctx_->get_ls_id());
  if (is_first_lookup_) {
    // A new storage iterator must initialize, not switch an existing context.
    lookup_param_.need_switch_param_ = false;
  } else {
    lookup_param_.need_switch_param_ |= lookup_param_.ls_id_ != lookup_ls_id_;
  }
  if (search_ctx_->is_async_index_query()) {
    const ObAsyncIndexPartitionSegCtx *partition = search_ctx_->get_current_async_partition();
    if (OB_ISNULL(partition) || !partition->get_read_snapshot().is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sort lookup async partition snapshot", K(ret), KP(partition));
    } else if (!is_first_lookup_) {
      lookup_param_.need_switch_param_ |= lookup_param_.snapshot_.version() != partition->get_read_snapshot();
    }
  }
  return ret;
}

int ObDASSortValueLookupIter::begin_candidate_batch(ObDASSearchDriverIter &driver, int64_t count)
{
  int ret = OB_SUCCESS;
  common::ScopedTimer timer(ObMetricId::HS_SORT_LOOKUP_MATCH_TIME);
  if (!batch_finished_ || count <= 0 || count > max_size_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid sort candidate submission", K(ret), K(count), K_(batch_finished), K_(max_size));
  } else if (OB_FAIL(prepare_batch_binding())) {
    LOG_WARN("failed to bind sort lookup partition", K(ret));
  } else if (OB_FAIL(ObDASIter::reuse())) {
    LOG_WARN("failed to reuse lookup for candidate batch", K(ret));
  } else if (OB_FAIL(candidate_indices_.reuse())) {
    LOG_WARN("failed to clear previous candidate associations", K(ret));
  } else {
    candidates_.reuse();
    returned_indices_.reuse();
    candidate_allocator_.reuse();
    ObEvalCtx &source = *search_ctx_->get_eval_ctx();
    ObEvalCtx::BatchInfoScopeGuard guard(source);
    guard.set_batch_size(count);
    ObExpr *identity = sort_ctdef_->sort_exprs_.at(sort_ctdef_->unique_shard_doc_index());
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      guard.set_batch_idx(i);
      ObDatum *rowid = nullptr;
      ObDatum *shard_doc = nullptr;
      ObDASSortCandidate candidate;
      if (OB_FAIL(search_ctx_->get_rowid_exprs().at(0)->eval(source, rowid))) {
        LOG_WARN("failed to read sort candidate rowid", K(ret), K(i));
      } else if (OB_FAIL(identity->eval(source, shard_doc))) {
        LOG_WARN("failed to read sort candidate shard document key", K(ret), K(i));
      } else if (rowid->is_null() || shard_doc->is_null() || shard_doc->len_ != 32) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sort candidate identity", K(ret), K(i));
      } else if (OB_FAIL(candidate.shard_doc_value_.deep_copy(*shard_doc, candidate_allocator_))) {
        LOG_WARN("failed to copy sort candidate shard document key", K(ret), K(i));
      } else if (OB_FAIL(driver.get_current_candidate_raw_score(i, candidate.raw_score_))) {
        LOG_WARN("failed to read sort candidate raw score", K(ret), K(i));
      } else {
        candidate.rowid_ = rowid->get_uint();
        if (OB_FAIL(candidate_indices_.set_refactored(candidate.rowid_, i))) {
          LOG_WARN("duplicate or invalid sort candidate rowid", K(ret), K(i), K(candidate.rowid_));
        } else if (OB_FAIL(candidates_.push_back(candidate))) {
          LOG_WARN("failed to save sort candidate", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      candidate_iter_.bind(candidates_, *sort_ctdef_->sort_value_scan_ctdef_.rowkey_exprs_.at(0));
      batch_finished_ = false;
    }
  }
  return ret;
}

int ObDASSortValueLookupIter::do_index_lookup()
{
  int ret = OB_SUCCESS;
  INC_METRIC_VAL(ObMetricId::HS_SORT_LOOKUP_TIMES, 1);
  INC_METRIC_VAL(ObMetricId::HS_SORT_LOOKUP_ROWS, lookup_rowkey_cnt_);
  if (OB_SUCC(ret) && OB_FAIL(ObDASLocalLookupIter::do_index_lookup())) {
    LOG_WARN("failed to execute sort-value local lookup", K(ret), K_(lookup_rowkey_cnt));
  }
  return ret;
}

int ObDASSortValueLookupIter::check_index_lookup()
{
  int ret = OB_SUCCESS;
  if (lookup_row_cnt_ > lookup_rowkey_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort lookup returned too many rows", K(ret), K_(lookup_row_cnt), K_(lookup_rowkey_cnt));
  } else if (!search_ctx_->is_async_index_query()) {
    // A synchronous index and the sort-value lookup share the SQL snapshot.
    // Preserve ordinary index-back defensive checks for missing rows.
    ret = ObDASLocalLookupIter::check_index_lookup();
  }
  // Async candidates may be invisible at the partition's read snapshot.
  // Every returned identity is still validated against the candidate batch.
  return ret;
}

int ObDASSortValueLookupIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  returned_indices_.reuse();
  if (batch_finished_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(search_ctx_->get_sort_first_error())) {
    LOG_WARN("sort lookup stopped after task failure", K(ret));
  } else {
    const uint64_t scanned_before = rtdef_.row_scan_cnt_;
    {
      common::ScopedTimer timer(ObMetricId::HS_SORT_LOOKUP_TIME);
      if (OB_FAIL(ObDASLookupIter::inner_get_next_rows(count, capacity))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to read sort lookup rows", K(ret), K(count), K(capacity));
        }
      }
    }
    // Record storage's scan counter even for an empty/terminal lookup batch.
    // It differs from requested rowkeys and visible output; preserve read errors.
    const int lookup_ret = ret;
    ret = OB_SUCCESS;
    INC_METRIC_VAL(ObMetricId::HS_SORT_LOOKUP_SCAN_ROWS, rtdef_.row_scan_cnt_ - scanned_before);
    if (OB_SUCC(ret) || (lookup_ret != OB_SUCCESS && lookup_ret != OB_ITER_END)) {
      ret = lookup_ret;
    }
    if ((OB_SUCCESS == ret || OB_ITER_END == ret) && count > 0) {
      const int end_ret = ret;
      ret = OB_SUCCESS;
      INC_METRIC_VAL(ObMetricId::HS_SORT_LOOKUP_OUTPUT_ROWS, count);
      common::ScopedTimer timer(ObMetricId::HS_SORT_LOOKUP_MATCH_TIME);
      ObEvalCtx::BatchInfoScopeGuard guard(*private_eval_ctx_);
      guard.set_batch_size(count);
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        guard.set_batch_idx(i);
        ObDatum *rowid = nullptr;
        int64_t index = -1;
        if (OB_FAIL(sort_ctdef_->sort_value_scan_ctdef_.rowkey_exprs_.at(0)->eval(*private_eval_ctx_, rowid))) {
          LOG_WARN("failed to read sort lookup rowid", K(ret), K(i));
        } else if (rowid->is_null()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sort lookup returned a null rowid", K(ret), K(i));
        } else if (OB_FAIL(candidate_indices_.get_refactored(rowid->get_uint(), index))) {
          LOG_WARN("sort lookup returned an unknown or repeated rowid", K(ret), K(i));
        } else if (index < 0 || index >= candidates_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sort lookup association is out of bounds", K(ret), K(i), K(index),
                   "candidate_count", candidates_.count());
        } else if (OB_FAIL(returned_indices_.push_back(index))) {
          LOG_WARN("failed to retain sort lookup association", K(ret));
        } else if (OB_FAIL(candidate_indices_.erase_refactored(rowid->get_uint()))) {
          LOG_WARN("failed to consume sort lookup association", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ret = end_ret;
      }
    }
    if (OB_ITER_END == ret) {
      batch_finished_ = true;
    }
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    count = 0;
  }
  return ret;
}

int ObDASSortValueLookupIter::restore_candidate_batch(int64_t count)
{
  int ret = OB_SUCCESS;
  if (count != returned_indices_.count()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("sort lookup association count does not match output", K(ret), K(count),
             "association_count", returned_indices_.count());
  } else if (OB_FAIL(init_sort_projection(*search_ctx_, *private_eval_ctx_, count, true))) {
    LOG_WARN("failed to initialize lookup identity projection", K(ret));
  } else {
    ObExpr *rowid = search_ctx_->get_rowid_exprs().at(0);
    ObExpr *identity = sort_ctdef_->sort_exprs_.at(sort_ctdef_->unique_shard_doc_index());
    for (int64_t i = 0; i < count; ++i) {
      const ObDASSortCandidate &candidate = candidates_.at(returned_indices_.at(i));
      rowid->get_vector(*private_eval_ctx_)->set_uint(i, candidate.rowid_);
      identity->get_vector(*private_eval_ctx_)->set_payload_shallow(
          i, candidate.shard_doc_value_.ptr_, candidate.shard_doc_value_.len_);
      project_sort_score(*search_ctx_, *private_eval_ctx_, i, candidate.raw_score_);
    }
    rowid->set_evaluated_projected(*private_eval_ctx_);
    identity->set_evaluated_projected(*private_eval_ctx_);
  }
  return ret;
}

int ObDASSortValueLookupIter::destroy_lookup()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // Registered children release before lookup_param_/lookup_memctx_.
  if (is_inited() && OB_FAIL(ObDASIter::release())) {
    LOG_WARN("failed to release inherited sort lookup", K(ret));
  }
  // Partial initialization may not have registered the children yet.
  if (scan_iter_.is_inited() && OB_TMP_FAIL(scan_iter_.release())) {
    LOG_WARN("failed to release sort lookup scan iterator", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  if (candidate_iter_.is_inited() && OB_TMP_FAIL(candidate_iter_.release())) {
    LOG_WARN("failed to release sort candidate iterator", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  if (candidate_indices_.created() && OB_TMP_FAIL(candidate_indices_.destroy())) {
    LOG_WARN("failed to destroy sort candidate associations", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  candidates_.reset();
  returned_indices_.reset();
  candidate_allocator_.reset();
  batch_finished_ = true;
  return ret;
}

ObDASHybridTopNImpl::ObDASHybridTopNImpl()
  : ObSortOpImpl(), after_row_(nullptr)
{}

int ObDASHybridTopNImpl::prepare_heap_sort(
    const ObDASHybridSearchSortCtDef &ctdef, int64_t capacity, ObEvalCtx &eval)
{
  int ret = OB_SUCCESS;
  if (!is_inited() || !use_heap_sort_) {
    if (is_inited()) {
      // A spill discarded the heap. Reset only the base sorter to keep the search_after cursor.
      ObSortOpImpl::reset();
    }
    if (OB_FAIL(ObSortOpImpl::init(MTL_ID(),
                                   &ctdef.sort_collations_, &ctdef.sort_cmp_funcs_,
                                   &eval,
                                   &eval.exec_ctx_,
                                   false,
                                   false,
                                   true /* retain rows until outer output is consumed */,
                                   0,
                                   capacity,
                                   false))) {
      LOG_WARN("failed to initialize hybrid TopN heap sort", K(ret), K(capacity));
    }
  }
  return ret;
}

int ObDASHybridTopNImpl::init_hybrid_sort(
    const ObDASHybridSearchSortCtDef &ctdef, ObDASSearchCtx &ctx, ObEvalCtx &eval)
{
  int ret = OB_SUCCESS;
  const ObDASSortRuntimeInfo &info = ctx.get_sort_info();
  if (OB_UNLIKELY(is_inited())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("hybrid TopN sorter already initialized", K(ret));
  } else if (OB_FAIL(prepare_heap_sort(ctdef, info.capacity_, eval))) {
    LOG_WARN("failed to prepare hybrid TopN heap sort", K(ret));
  } else if (OB_FAIL(after_cmp_.init(&ctdef.sort_collations_, &ctdef.sort_cmp_funcs_, &eval.exec_ctx_, false))) {
    LOG_WARN("failed to initialize cursor comparison", K(ret));
  } else if (!info.after_values_.empty()) {
    const int64_t cells = info.after_values_.count();
    const int64_t bytes = ObChunkDatumStore::Block::row_store_size(info.after_values_.get_data(), cells);
    void *buf = nullptr;
    if (cells != ctdef.sort_collations_.count() || bytes > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid fixed sort cursor layout", K(ret), K(cells), K(bytes));
    } else if (OB_ISNULL(buf = mem_context_->get_malloc_allocator().alloc(bytes))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate fixed sort cursor", K(ret), K(bytes));
    } else {
      after_row_ = new (buf) ObChunkDatumStore::StoredRow();
      if (OB_FAIL(after_row_->copy_shadow_datums(info.after_values_.get_data(), cells,
          reinterpret_cast<char *>(after_row_->cells()), bytes - sizeof(*after_row_), bytes, 0))) {
        LOG_WARN("failed to retain fixed cursor tuple", K(ret));
      }
    }
  }
  return ret;
}

int ObDASHybridTopNImpl::is_after(const ObIArray<ObExpr *> &exprs, ObEvalCtx &eval, bool &passes)
{
  int ret = OB_SUCCESS;
  passes = nullptr == after_row_;
  if (OB_NOT_NULL(after_row_)) {
    // Existing three-way comparison: negative means left follows right.
    const int cmp = after_cmp_.with_ties_cmp(&exprs, after_row_, eval);
    if (OB_FAIL(after_cmp_.get_error_code())) {
      LOG_WARN("failed to compare candidate with cursor", K(ret));
    } else {
      passes = cmp < 0; // Equal full tuples, including __shard_doc, never pass.
    }
  }
  return ret;
}

const ObDatum *ObDASHybridTopNImpl::get_bottom() const
{
  const ObDatum *bottom = nullptr;
  if (inited_ && use_heap_sort_ && !sorted_ && outputted_rows_cnt_ == 0
      && OB_SUCCESS == comp_.ret_ && nullptr != topn_heap_
      && topn_cnt_ > 0 && topn_heap_->heap_.count() == topn_cnt_) {
    const ObChunkDatumStore::StoredRow *row = topn_heap_->heap_.top();
    if (nullptr != row && nullptr != sort_collations_ && !sort_collations_->empty()) {
      bottom = &row->cells()[sort_collations_->at(0).field_idx_];
    }
  }
  return bottom;
}

void ObDASHybridTopNImpl::reuse()
{
  ObSortOpImpl::reuse();
  outputted_rows_cnt_ = 0;
  got_first_row_ = false;
  comp_.ret_ = OB_SUCCESS;
  comp_.cmp_count_ = 0;
  after_cmp_.ret_ = OB_SUCCESS;
  after_cmp_.cmp_count_ = 0;
}

void ObDASHybridTopNImpl::reset()
{
  after_cmp_.reset();
  if (nullptr != after_row_ && nullptr != mem_context_) {
    mem_context_->get_malloc_allocator().free(after_row_);
  }
  after_row_ = nullptr;
  ObSortOpImpl::reset();
}

int ObDASSortValueLookupIter::init_private_frames()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &source = *search_ctx_->get_eval_ctx();
  const ObExprFrameInfo *layout = search_ctx_->get_sort_remote_frame_info();
  const ObPhysicalPlanCtx *plan_ctx = source.exec_ctx_.get_physical_plan_ctx();
  const ObPhysicalPlan *phy_plan = nullptr;
  if (OB_ISNULL(layout) && OB_NOT_NULL(plan_ctx) && OB_NOT_NULL(phy_plan = plan_ctx->get_phy_plan())) {
    layout = &phy_plan->get_expr_frame_info();
  }
  const uint64_t frame_count = source.exec_ctx_.get_frame_cnt();
  char **frames = nullptr;
  if (OB_ISNULL(layout) || frame_count == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hybrid sort expression frame layout is missing", K(ret), KP(layout), K(frame_count));
  } else if (OB_ISNULL(frames = static_cast<char **>(allocator_.alloc(frame_count * sizeof(char *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hybrid sort frame pointers", K(ret), K(frame_count));
  } else {
    MEMCPY(frames, source.frames_, frame_count * sizeof(char *));
    // Lookup expressions share immutable frames with the driver, but their mutable frames must be private:
    // evaluating a sort field must not replace the __shard_doc values or scores of the pending candidate batch.
    const ObIArray<ObFrameInfo> *mutable_frames[] = {&layout->param_frame_, &layout->dynamic_frame_, &layout->datum_frame_};
    for (int64_t kind = 0; OB_SUCC(ret) && kind < ARRAYSIZEOF(mutable_frames); ++kind) {
      for (int64_t i = 0; OB_SUCC(ret) && i < mutable_frames[kind]->count(); ++i) {
        const ObFrameInfo &frame = mutable_frames[kind]->at(i);
        if (frame.frame_idx_ >= frame_count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid hybrid sort private frame index", K(ret), K(kind), K(i),
                   K(frame.frame_idx_), K(frame_count));
        } else if (OB_ISNULL(frames[frame.frame_idx_] = static_cast<char *>(allocator_.alloc(frame.frame_size_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate hybrid sort private frame", K(ret), K(kind), K(i),
                   K(frame.frame_idx_), K(frame.frame_size_));
        } else if (kind == 0 && OB_NOT_NULL(source.frames_[frame.frame_idx_])) {
          MEMCPY(frames[frame.frame_idx_], source.frames_[frame.frame_idx_], frame.frame_size_);
        } else {
          MEMSET(frames[frame.frame_idx_], 0, frame.frame_size_);
        }
      }
    }
    if (OB_SUCC(ret)) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObEvalCtx)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate hybrid sort evaluation context", K(ret), "size", sizeof(ObEvalCtx));
      } else {
        private_eval_ctx_ = new (buf) ObEvalCtx(source.exec_ctx_, &allocator_);
        private_eval_ctx_->frames_ = frames;
        private_eval_ctx_->max_batch_size_ = source.max_batch_size_;
      }
    }
  }
  return ret;
}


ObDASSortTopNIter::ObDASSortTopNIter()
  : ObDASIter(DAS_ITER_SORT_TOPN),
    driver_(nullptr),
    sort_ctdef_(nullptr),
    ctx_(nullptr),
    sort_eval_ctx_(nullptr),
    allocator_(ObMemAttr(MTL_ID(), "DASSortTopNIter")),
    sort_exprs_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    reset_exprs_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    visited_exprs_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    output_rows_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    skip_(nullptr),
    selector_(nullptr),
    emitted_rows_(0),
    started_(false),
    lookup_inited_(false),
    consumed_(false),
    saved_error_(OB_SUCCESS),
    profile_(nullptr)
{}

ObDASSortTopNIter::~ObDASSortTopNIter()
{
  topn_impl_.destroy();
  // Clear StoredRows now. Member order then destroys output views before the sorter context,
  // and destroys lookup's EvalCtx only after the sorter has finished its destruction.
}

int ObDASSortTopNIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  // A uint16_t selector stores row indexes from 0 through UINT16_MAX.
  constexpr int64_t MAX_SELECTOR_BATCH_SIZE = static_cast<int64_t>(UINT16_MAX) + 1;
  if (param.type_ != DAS_ITER_SORT_TOPN || !param.is_valid()
      || param.max_size_ <= 0 || param.max_size_ > MAX_SELECTOR_BATCH_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hybrid TopN initialization", K(ret), K(param.type_), K(param.max_size_));
  } else {
    ObDASSortTopNIterParam &sort_param = static_cast<ObDASSortTopNIterParam &>(param);
    driver_ = sort_param.driver_;
    sort_ctdef_ = &sort_param.sort_ctdef_;
    ctx_ = &sort_param.ctx_;
    skip_ = to_bit_vector(allocator_.alloc(ObBitVector::memory_size(max_size_)));
    selector_ = static_cast<uint16_t *>(allocator_.alloc(max_size_ * sizeof(uint16_t)));
    if (OB_ISNULL(skip_) || OB_ISNULL(selector_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate hybrid sort batch selectors", K(ret), K_(max_size), KP_(skip), KP_(selector));
    } else {
      skip_->init(max_size_);
      if (OB_FAIL(ctx_->get_sort_info().evaluate(*eval_ctx_))) {
        LOG_WARN("failed to evaluate hybrid sort runtime", K(ret));
      } else if (ctx_->get_sort_info().page_size_ > 0) {
        sort_eval_ctx_ = eval_ctx_;
        if (sort_ctdef_->has_sort_value_lookup_) {
          if (OB_FAIL(lookup_.init_lookup(*ctx_))) {
            LOG_WARN("failed to initialize sort-value local lookup", K(ret));
          } else {
            lookup_inited_ = true;
            sort_eval_ctx_ = &lookup_.get_value_eval_ctx();
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(prepare_sort_exprs())) {
          LOG_WARN("failed to prepare hybrid sort expressions", K(ret));
        } else if (OB_FAIL(topn_impl_.init_hybrid_sort(*sort_ctdef_, *ctx_, *sort_eval_ctx_))) {
          LOG_WARN("failed to initialize hybrid TopN sorter", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASSortTopNIter::collect_reset_exprs(ObExpr *expr)
{
  int ret = OB_SUCCESS;
  if (nullptr == expr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort expression dependency is null", K(ret));
  } else {
    bool produced = expr == ctx_->get_rowid_exprs().at(0);
    const ObDASSortRuntimeInfo &info = ctx_->get_sort_info();
    for (int64_t i = 0; !produced && i < info.fusion_ctdef_->score_exprs_.count(); ++i) {
      produced = expr == info.fusion_ctdef_->score_exprs_.at(i);
    }
    for (int64_t i = 0; !produced && i < sort_ctdef_->sort_exprs_.count(); ++i) {
      produced = expr == sort_ctdef_->sort_exprs_.at(i)
          && (sort_ctdef_->score_sort_flags_.at(i) || sort_ctdef_->unique_shard_doc_flags_.at(i));
    }
    const ExprFixedArray &access = sort_ctdef_->sort_value_scan_ctdef_.pd_expr_spec_.access_exprs_;
    for (int64_t i = 0; !produced && i < access.count(); ++i) {
      produced = expr == access.at(i);
    }
    bool visited = false;
    for (int64_t i = 0; !visited && i < visited_exprs_.count(); ++i) {
      visited = visited_exprs_.at(i) == expr;
    }
    if (produced || visited) {
      // Producer values, even function-shaped __shard_doc, must not be reevaluated.
    } else if (OB_FAIL(visited_exprs_.push_back(expr))) {
      LOG_WARN("failed to retain sort expression dependency", K(ret));
    } else {
      if (OB_NOT_NULL(expr->eval_func_) && !expr->is_const_expr()
          && OB_FAIL(reset_exprs_.push_back(expr))) {
        LOG_WARN("failed to retain per-batch sort invalidation", K(ret));
      }
      for (uint32_t i = 0; OB_SUCC(ret) && i < expr->arg_cnt_; ++i) {
        if (OB_FAIL(SMART_CALL(collect_reset_exprs(expr->args_[i])))) {
          LOG_WARN("failed to collect sort expression dependencies", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObDASSortTopNIter::prepare_sort_exprs()
{
  int ret = OB_SUCCESS;
  if (sort_exprs_.empty()) {
    if (OB_FAIL(sort_exprs_.assign(sort_ctdef_->sort_exprs_))) {
      LOG_WARN("failed to prepare sort expression inputs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_ctdef_->sort_collations_.count(); ++i) {
      if (sort_ctdef_->sort_collations_.at(i).field_idx_ != i) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hybrid sort key layout does not match stored row", K(ret), K(i));
      } else if (OB_FAIL(collect_reset_exprs(sort_ctdef_->sort_exprs_.at(i)))) {
        LOG_WARN("failed to prepare sort invalidation list", K(ret), K(i));
      }
    }
    const ObDASSortRuntimeInfo &info = ctx_->get_sort_info();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sort_exprs_.push_back(ctx_->get_rowid_exprs().at(0)))) {
      LOG_WARN("failed to append sort result rowid", K(ret));
    } else if (OB_FAIL(sort_exprs_.push_back(info.fusion_ctdef_->get_search_score_expr()))) {
      LOG_WARN("failed to append sort result raw score", K(ret));
    } else if (OB_FAIL(sort_exprs_.push_back(info.fusion_ctdef_->get_fusion_score_expr()))) {
      LOG_WARN("failed to append sort result final score", K(ret));
    }
    if (OB_FAIL(ret)) {
      // Do not reuse a partially built key/payload layout after initialization failure.
      sort_exprs_.reuse();
      reset_exprs_.reuse();
      visited_exprs_.reuse();
    }
  }
  return ret;
}

void ObDASSortTopNIter::clear_const_sort_expr_flags()
{
  // Reevaluate computed constants after refreshing parameters. process_batch() clears row-dependent expressions.
  for (int64_t i = 0; i < visited_exprs_.count(); ++i) {
    ObExpr *expr = visited_exprs_.at(i);
    if (expr->is_const_expr() && !IS_CONST_LITERAL(expr->type_) && OB_NOT_NULL(expr->eval_func_)) {
      expr->get_eval_info(*sort_eval_ctx_).clear_evaluated_flag();
    }
  }
}

void ObDASSortTopNIter::invalidate_sort_exprs()
{
  for (int64_t i = 0; i < reset_exprs_.count(); ++i) {
    // ObExpr::clear_evaluated_flag only clears the current row for batch exprs.
    // Clear EvalInfo instead; the first lazy eval initializes the new batch.
    reset_exprs_.at(i)->get_eval_info(*sort_eval_ctx_).clear_evaluated_flag();
  }
}

int ObDASSortTopNIter::start(bool rescan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(sort_ctdef_) || OB_ISNULL(driver_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid hybrid sort start state", K(ret), KP_(ctx), KP_(sort_ctdef), KP_(driver), KP_(eval_ctx));
  } else {
    if (OB_FAIL(ObDASProfileIter::init_runtime_profile(
        ObProfileId::HYBRID_SEARCH_SORT_TOPN_ITER, profile_))) {
      LOG_WARN("failed to initialize hybrid sort profile", K(ret));
    }
    common::ObProfileSwitcher switcher(profile_);
    const ObMetricId metrics[] = {
      ObMetricId::HS_TOTAL_TIME, ObMetricId::HS_OUTPUT_ROW_COUNT,
      ObMetricId::HS_NEXT_ROWIDS_TIME, ObMetricId::HS_NEXT_ROWIDS_COUNT,
      ObMetricId::HS_SORT_CANDIDATE_ROWS,
      ObMetricId::HS_SORT_LOOKUP_TIME, ObMetricId::HS_SORT_LOOKUP_TIMES,
      ObMetricId::HS_SORT_LOOKUP_ROWS,
      ObMetricId::HS_SORT_LOOKUP_SCAN_ROWS,
      ObMetricId::HS_SORT_LOOKUP_OUTPUT_ROWS, ObMetricId::HS_SORT_LOOKUP_MATCH_TIME,
      ObMetricId::HS_SORT_AFTER_FILTER_TIME, ObMetricId::HS_SORT_ACCEPTED_ROWS,
      ObMetricId::HS_SORT_TOPN_TIME
    };
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(metrics); ++i) {
      SET_METRIC_VAL(metrics[i], 0);
    }
    common::ScopedTimer total_timer(ObMetricId::HS_TOTAL_TIME);
    reuse_sort_result();
    if (OB_FAIL(ret)) {
    } else if (ctx_->get_sort_info().page_size_ == 0) {
      consumed_ = true;
    } else if (lookup_inited_ && OB_FAIL(lookup_.prepare_execution())) {
      LOG_WARN("failed to prepare sort lookup execution", K(ret));
    } else {
      clear_const_sort_expr_flags();
      if (rescan && started_) {
        if (OB_FAIL(topn_impl_.prepare_heap_sort(*sort_ctdef_, ctx_->get_sort_info().capacity_, *sort_eval_ctx_))) {
          LOG_WARN("failed to prepare hybrid TopN heap for rescan", K(ret));
        } else if (OB_FAIL(driver_->rescan())) {
          LOG_WARN("failed to rescan hybrid sort driver", K(ret));
        }
      } else if (OB_FAIL(driver_->do_table_scan())) {
        LOG_WARN("failed to start hybrid sort driver scan", K(ret));
      } else {
        started_ = true;
      }
    }
    if (OB_FAIL(ret)) {
      saved_error_ = ret;
    }
  }
  return ret;
}

int ObDASSortTopNIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start(false))) {
    LOG_WARN("failed to start hybrid TopN scan", K(ret));
  }
  return ret;
}

int ObDASSortTopNIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start(true))) {
    LOG_WARN("failed to rescan hybrid TopN", K(ret));
  }
  return ret;
}

int ObDASSortTopNIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  reuse_sort_result();
  profile_ = nullptr;
  if (lookup_inited_ && OB_FAIL(lookup_.reuse())) {
    LOG_WARN("failed to reuse sort lookup", K(ret));
  } else if (OB_NOT_NULL(driver_) && OB_FAIL(driver_->reuse())) {
    LOG_WARN("failed to reuse sort driver", K(ret));
  }
  if (OB_FAIL(ret)) {
    saved_error_ = ret;
  }
  return ret;
}

void ObDASSortTopNIter::reuse_sort_result()
{
  // Output datums borrow StoredRow payloads owned by the sorter.
  output_rows_.reuse();
  topn_impl_.reuse();
  emitted_rows_ = 0;
  consumed_ = false;
  saved_error_ = OB_SUCCESS;
}

int ObDASSortTopNIter::inner_release()
{
  int ret = OB_SUCCESS;
  // Discard output views before releasing the StoredRows they reference.
  output_rows_.reset();
  // Close storage while retaining the private EvalCtx needed by the sorter.
  if (OB_FAIL(lookup_.destroy_lookup())) {
    LOG_WARN("failed to release sort lookup", K(ret));
  }
  topn_impl_.~ObDASHybridTopNImpl();
  new (&topn_impl_) ObDASHybridTopNImpl();
  // No sorter references remain; the lookup destructor can now free its private frames.
  lookup_.~ObDASSortValueLookupIter();
  new (&lookup_) ObDASSortValueLookupIter();
  lookup_inited_ = false;
  sort_exprs_.reset();
  reset_exprs_.reset();
  visited_exprs_.reset();
  allocator_.reset();
  skip_ = nullptr;
  selector_ = nullptr;
  sort_eval_ctx_ = nullptr;
  profile_ = nullptr;
  driver_ = nullptr;
  sort_ctdef_ = nullptr;
  ctx_ = nullptr;
  started_ = false;
  consumed_ = false;
  emitted_rows_ = 0;
  saved_error_ = OB_SUCCESS;
  return ret;
}

int ObDASSortTopNIter::restore_driver_batch(int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_sort_projection(*ctx_, *sort_eval_ctx_, count, false))) {
    LOG_WARN("failed to initialize direct sort score projection", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    double raw_score = 0;
    if (OB_FAIL(driver_->get_current_candidate_raw_score(i, raw_score))) {
      LOG_WARN("failed to read candidate score", K(ret), K(i));
    } else {
      project_sort_score(*ctx_, *sort_eval_ctx_, i, raw_score);
    }
  }
  return ret;
}

int ObDASSortTopNIter::process_batch(int64_t count)
{
  int ret = OB_SUCCESS;
  int64_t selected_count = 0;
  const ObDASSortRuntimeInfo &info = ctx_->get_sort_info();
  ObEvalCtx::BatchInfoScopeGuard guard(*sort_eval_ctx_);
  guard.set_batch_size(count);
  invalidate_sort_exprs();
  if (sort_ctdef_->has_sort_value_lookup_) {
    if (OB_FAIL(lookup_.restore_candidate_batch(count))) {
      LOG_WARN("failed to restore sort lookup candidate batch", K(ret), K(count));
    }
  } else if (OB_FAIL(restore_driver_batch(count))) {
    LOG_WARN("failed to restore sort driver batch", K(ret), K(count));
  }
  if (OB_FAIL(ret)) {
  } else if (!info.apply_min_score_ && info.after_values_.empty()) {
    // Every returned row is eligible: do not build an identity selector or
    // reread projected scores for the ordinary first-page batch overload.
    skip_->reset(count);
    selected_count = count;
  } else {
    skip_->set_all(count);
    common::ScopedTimer timer(ObMetricId::HS_SORT_AFTER_FILTER_TIME);
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      guard.set_batch_idx(i);
      ObDatum *score = nullptr;
      bool passes = false;
      if (info.apply_min_score_
          && OB_FAIL(info.fusion_ctdef_->get_fusion_score_expr()->eval(*sort_eval_ctx_, score))) {
        LOG_WARN("failed to read final candidate score", K(ret), K(i));
      } else if (info.apply_min_score_ && score->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sort candidate final score is null", K(ret), K(i));
      } else if (info.apply_min_score_ && score->get_double() < info.min_score_) {
        // Do not evaluate sort keys for a candidate rejected by min_score.
      } else if (OB_FAIL(topn_impl_.is_after(sort_exprs_, *sort_eval_ctx_, passes))) {
        LOG_WARN("failed to evaluate hybrid search_after", K(ret), K(i));
      } else if (passes) {
        selector_[selected_count++] = static_cast<uint16_t>(i);
        skip_->unset(i);
      }
    }
  }
  INC_METRIC_VAL(ObMetricId::HS_SORT_ACCEPTED_ROWS, selected_count);
  if (OB_SUCC(ret) && selected_count > 0) {
    common::ScopedTimer timer(ObMetricId::HS_SORT_TOPN_TIME);
    // These are the existing parent overloads: no custom batch/heap/dump logic.
    ObSortOpImpl &sorter = topn_impl_;
    if (selected_count == count) {
      if (OB_FAIL(sorter.add_batch(sort_exprs_, *skip_, count, 0, nullptr))) {
        LOG_WARN("failed to add hybrid candidate batch to sorter", K(ret), K(count));
      }
    } else if (OB_FAIL(sorter.add_batch(sort_exprs_, *skip_, count, selector_, selected_count))) {
      LOG_WARN("failed to add selected hybrid candidates to sorter", K(ret), K(count), K(selected_count));
    }
  }
  return ret;
}

int ObDASSortTopNIter::consume()
{
  int ret = OB_SUCCESS;
  bool end = false;
  while (OB_SUCC(ret) && !end) {
    if (THIS_WORKER.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("hybrid TopN candidate consumption timed out", K(ret));
    } else if (OB_FAIL(ctx_->get_sort_first_error())) {
      LOG_WARN("sort stopped after task failure", K(ret));
    } else {
      ObDASGroupBatchInfo batch_info;
      int64_t count = 0;
      // Range leaves copy the borrowed bound before this synchronous call returns.
      if (OB_FAIL(driver_->read_candidate_batch(max_size_, topn_impl_.get_bottom(), count, batch_info))) {
        LOG_WARN("failed to read hybrid candidate batch", K(ret));
      } else if (count > 0) {
        INC_METRIC_VAL(ObMetricId::HS_SORT_CANDIDATE_ROWS, count);
        if (OB_FAIL(ret)) {
        } else if (!sort_ctdef_->has_sort_value_lookup_) {
          if (OB_FAIL(process_batch(count))) {
            LOG_WARN("failed to process hybrid sort driver batch", K(ret), K(count));
          }
        } else if (OB_FAIL(lookup_.begin_candidate_batch(*driver_, count))) {
          LOG_WARN("failed to submit candidate batch to lookup", K(ret));
        } else {
          bool lookup_end = false;
          while (OB_SUCC(ret) && !lookup_end) {
            if (THIS_WORKER.is_timeout()) {
              ret = OB_TIMEOUT;
              LOG_WARN("hybrid TopN sort lookup timed out", K(ret));
            } else if (OB_FAIL(ctx_->get_sort_first_error())) {
              LOG_WARN("sort lookup stopped after task failure", K(ret));
            } else {
              int64_t returned = 0;
              if (OB_FAIL(lookup_.get_next_rows(returned, max_size_))) {
                if (OB_ITER_END == ret) {
                  lookup_end = true;
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("failed to fetch hybrid sort lookup batch", K(ret), K(returned), K_(max_size));
                }
              }
              if (OB_FAIL(ret)) {
              } else if (returned > 0) {
                if (OB_FAIL(process_batch(returned))) {
                  LOG_WARN("failed to process hybrid sort lookup batch", K(ret), K(returned));
                }
              } else if (!lookup_end) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("sort lookup returned an empty unfinished batch", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        end = batch_info.all_end_;
        if (count == 0 && !batch_info.group_end_ && !end) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("hybrid candidate reader made no progress", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    common::ScopedTimer timer(ObMetricId::HS_SORT_TOPN_TIME);
    if (OB_FAIL(topn_impl_.sort())) {
      LOG_WARN("failed to finish existing hybrid sorter", K(ret));
    }
  }
  return ret;
}

int ObDASSortTopNIter::project_output(int64_t count)
{
  int ret = OB_SUCCESS;
  output_rows_.reuse();
  ObEvalCtx::BatchInfoScopeGuard guard(*sort_eval_ctx_);
  guard.set_batch_size(count);
  const int64_t keys = sort_ctdef_->sort_exprs_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    guard.set_batch_idx(i);
    ObDASSortTopNEntry entry;
    // get_next_batch() restored the StoredRow datums. Do not reevaluate expressions here:
    // rich-format conversion could otherwise replace them with the last candidate batch.
    const ObDatum &rowid = sort_exprs_.at(keys)->locate_expr_datum(*sort_eval_ctx_);
    const ObDatum &raw_score = sort_exprs_.at(keys + 1)->locate_expr_datum(*sort_eval_ctx_);
    const ObDatum &final_score = sort_exprs_.at(keys + 2)->locate_expr_datum(*sort_eval_ctx_);
    entry.rowid_.set_uint64(rowid.get_uint());
    entry.raw_score_ = raw_score.get_double();
    entry.final_score_ = final_score.get_double();
    for (int64_t k = 0; OB_SUCC(ret) && k < keys; ++k) {
      if (OB_FAIL(entry.sort_values_.push_back(sort_exprs_.at(k)->locate_expr_datum(*sort_eval_ctx_)))) {
        LOG_WARN("failed to append sorted key view", K(ret), K(k));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(output_rows_.push_back(entry))) {
      LOG_WARN("failed to append sorted output view", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObDASSearchUtils::project_hybrid_sort_entries(output_rows_, 0, count, *ctx_))) {
    LOG_WARN("failed to project sorted rows into the external frame", K(ret));
  }
  return ret;
}

int ObDASSortTopNIter::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObDASSortTopNIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  common::ObProfileSwitcher switcher(profile_);
  common::ScopedTimer total_timer(ObMetricId::HS_TOTAL_TIME);
  common::ScopedTimer read_timer(ObMetricId::HS_NEXT_ROWIDS_TIME);
  INC_METRIC_VAL(ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
  if (OB_FAIL(ret)) {
  } else if (saved_error_ != OB_SUCCESS) {
    ret = saved_error_;
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(sort_ctdef_) || OB_ISNULL(driver_)
      || capacity <= 0 || (!started_ && !consumed_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid hybrid sort output state", K(ret), KP_(ctx), KP_(sort_ctdef), KP_(driver),
             K(capacity), K_(started), K_(consumed));
  } else if (ctx_->get_sort_info().page_size_ == 0) {
    ret = OB_ITER_END;
  } else {
    if (!consumed_ && OB_FAIL(consume())) {
      LOG_WARN("failed to consume hybrid TopN candidates", K(ret));
    } else {
      consumed_ = true;
      {
        common::ScopedTimer timer(ObMetricId::HS_SORT_TOPN_TIME);
        if (OB_FAIL(topn_impl_.get_next_batch(sort_exprs_, MIN(capacity, max_size_), count))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to read hybrid TopN output batch", K(ret), K(count), K(capacity));
          }
        }
      }
      if ((ret == OB_SUCCESS || ret == OB_ITER_END) && count > 0) {
        const int end_ret = ret;
        if (OB_FAIL(project_output(count))) {
          LOG_WARN("failed to project hybrid TopN output batch", K(ret), K(count));
        } else {
          emitted_rows_ += count;
          INC_METRIC_VAL(ObMetricId::HS_OUTPUT_ROW_COUNT, count);
          if (OB_SUCC(ret)) {
            ret = emitted_rows_ >= ctx_->get_sort_info().capacity_ ? OB_ITER_END : end_ret;
          }
        }
      }
    }
  }
  if (ret != OB_SUCCESS && ret != OB_ITER_END) {
    saved_error_ = ret;
    count = 0;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
