/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_sort_op.h"
#include "share/search_index/ob_search_index_encoder.h"

namespace oceanbase
{
namespace sql
{



int ObDASSortOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(children.push_back(child_))) {
    LOG_WARN("failed to push child op", KR(ret), KPC(child_));
  }
  return ret;
}

int ObDASSortOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASSortOpParam &sort_op_param = static_cast<const ObDASSortOpParam &>(op_param);
  const ObDASScalarScanCtDef *scalar_ctdef = sort_op_param.get_scalar_ctdef();
  ObDASScalarScanRtDef *scalar_rtdef = sort_op_param.get_scalar_rtdef();
  unique_sort_ = sort_op_param.is_unique_sort();
  const int64_t column_count = get_rowid_exprs().count();
  if (OB_ISNULL(sort_op_param.get_child())
             || OB_ISNULL(sort_op_param.get_scalar_ctdef())
             || OB_ISNULL(sort_op_param.get_scalar_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sort input or metadata", K(ret),
        KP(sort_op_param.get_child()), KP(sort_op_param.get_scalar_ctdef()),
        KP(sort_op_param.get_scalar_rtdef()));
  } else if (FALSE_IT(input_op_ = sort_op_param.get_child())) {
  } else if (OB_ISNULL(input_rowid_exprs_ = input_op_->get_rowid_output_exprs())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sort input has no RowID output expressions", K(ret),
        K(input_op_->get_op_type()));
  } else if (FALSE_IT(nk_truncate_depth_ = unique_sort_
                          ? scalar_ctdef->nesting_depth_for_full_subtree_ : 0)) {
  } else {
    boost_weight_ = scalar_rtdef->get_boost_weight();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(get_sort_rowid_exprs().count() < 1
                         || get_sort_rowid_exprs().count() != get_rowid_exprs().count()
                         || (is_async_nested_scope(scope_)
                             && (get_sort_rowid_exprs().count() != 2
                                 || OB_ISNULL(get_sort_rowid_exprs().at(1)))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sort RowID output expression count", K(ret),
        "input_expr_count", get_sort_rowid_exprs().count(),
        "scope_expr_count", get_rowid_exprs().count());
  } else if (is_async_nested_scope(scope_)) {
    if (OB_ISNULL(rowid_store_ = OB_NEWx(CompactRowIDStore, &ctx_allocator(), search_ctx_, scope_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate sorted pair store", K(ret));
    } else if (OB_FAIL(rowid_store_->init(max_batch_size()))) {
      LOG_WARN("failed to initialize sorted pair store", K(ret));
    }
  } else {
    if (OB_FAIL(create_rowid_store(max_batch_size(), rowid_store_))) {
      LOG_WARN("failed to create rowid store", K(ret), K(max_batch_size()));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(rowid_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store", K(ret));
  } else if (FALSE_IT(rowid_store_iter_.init(rowid_store_))) {
  } else if (OB_FAIL(sort_collations_.init(column_count))) {
    LOG_WARN("failed to init sort collations", K(ret));
  } else if (OB_FAIL(sort_cmp_funcs_.init(column_count))) {
    LOG_WARN("failed to init sort cmp funcs", K(ret));
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < column_count; ++i) {
      ObExpr *expr = get_sort_rowid_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", K(ret), K(expr));
      } else {
        ObSortFieldCollation field_collation(i, expr->datum_meta_.cs_type_, true, NULL_FIRST);
        ObSortCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
                                                                 expr->datum_meta_.type_,
                                                                 field_collation.null_pos_,
                                                                 field_collation.cs_type_,
                                                                 expr->datum_meta_.scale_,
                                                                 lib::is_oracle_mode(),
                                                                 expr->datum_meta_.precision_,
                                                                 expr->datum_meta_.precision_);
        if (OB_ISNULL(cmp_func.cmp_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected cmp func", K(ret));
        } else if (OB_FAIL(sort_collations_.push_back(field_collation))) {
          LOG_WARN("failed to push back sort collation", K(ret));
        } else if (OB_FAIL(sort_cmp_funcs_.push_back(cmp_func))) {
          LOG_WARN("failed to push back sort cmp func", K(ret));
        }
      }
    }
  }
	return ret;
}

int ObDASSortOp::do_open()
{
  int ret = OB_SUCCESS;
  reset_rowid_cursor();
  if (OB_ISNULL(input_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(input_op_->open())) {
    LOG_WARN("failed to open sort input op", K(ret));
  } else if (is_async_nested_scope(scope_) && nested_compare_bytes() > sizeof(uint64_t)
      && OB_ISNULL(curr_nk_buf_ = static_cast<char *>(
          ctx_allocator().alloc(nested_compare_bytes())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sorted nested key buffer", K(ret), K_(nesting_depth));
  } else {
    lib::ContextParam context_param;
    context_param.set_mem_attr(MTL_ID(), "DASSortOp", ObCtxIds::DEFAULT_CTX_ID).set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(sort_memctx_, context_param))) {
      LOG_WARN("create sort memctx failed", K(ret));
    } else if (OB_ISNULL(sort_memctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null sort memctx", K(ret));
    } else {
      ObIAllocator &allocator = sort_memctx_->get_arena_allocator();
      void *buf = nullptr;
      // For unique_sort_ (NESTED child-domain must_not required branch), use
      // ObUniqueSortImpl so that adjacent rows with equal sort key (= full rowid
      // [doc_id, nested_key]) are collapsed inside the sort's get_next_batch.
      const int64_t impl_size = unique_sort_ ? sizeof(ObUniqueSortImpl) : sizeof(ObSortOpImpl);
      if (OB_ISNULL(buf = allocator.alloc(impl_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate sort impl memory", K(ret), KP(buf), K(unique_sort_));
      } else if (FALSE_IT(sort_impl_ = unique_sort_
                              ? static_cast<ObSortOpImpl *>(new (buf) ObUniqueSortImpl())
                              : new (buf) ObSortOpImpl())) {
      } else if (OB_FAIL(sort_impl_->init(MTL_ID(),
                                          &sort_collations_,
                                          &sort_cmp_funcs_,
                                          &eval_ctx(),
                                          &eval_ctx().exec_ctx_,
                                          false,     // enable encode sort key
                                          false,     // is local order
                                          false))) { // need rewind
        LOG_WARN("failed to init sort impl", K(ret));
      } else {
        is_first_nk_ = true;
      }
    }
  }
  return ret;
}

int ObDASSortOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan op", K(ret));
  } else if (OB_FAIL(input_op_->rescan())) {
    LOG_WARN("failed to rescan sort input op", K(ret));
  } else {
    is_first_nk_ = true;
    reset_rowid_cursor();
    sort_finished_ = false;
    sort_impl_->reuse();
    if (OB_NOT_NULL(rowid_store_)) {
      rowid_store_->reuse();
      rowid_store_iter_.reset();
      rowid_store_iter_.init(rowid_store_);
    }
  }
  return ret;
}

int ObDASSortOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input_op_) || OB_ISNULL(sort_impl_) || OB_ISNULL(rowid_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sort state on segment rescan", K(ret),
        KP_(input_op), KP_(sort_impl), KP_(rowid_store));
  } else if (OB_FAIL(input_op_->rescan_segment(range))) {
    LOG_WARN("failed to rescan sort child segment", K(ret), K(range));
  } else {
    is_first_nk_ = true;
    reset_rowid_cursor();
    sort_finished_ = false;
    sort_impl_->reuse();
    rowid_store_->reuse();
    rowid_store_iter_.reset();
    rowid_store_iter_.init(rowid_store_);
  }
  return ret;
}

int ObDASSortOp::do_close()
{
  int ret = OB_SUCCESS;
  reset_rowid_cursor();
  if (OB_NOT_NULL(curr_nk_buf_)) {
    ctx_allocator().free(curr_nk_buf_);
    curr_nk_buf_ = nullptr;
  }
  if (OB_NOT_NULL(rowid_store_)) {
    rowid_store_->reset();
    rowid_store_iter_.reset();
    rowid_store_ = nullptr;
  }
  if (OB_NOT_NULL(sort_impl_))  {
    sort_impl_->destroy();
    sort_impl_->~ObSortOpImpl();
    sort_impl_ = nullptr;
  }
  if (OB_NOT_NULL(sort_memctx_))  {
    sort_memctx_->reset_remain_one_page();
    DESTROY_CONTEXT(sort_memctx_);
    sort_memctx_ = nullptr;
  }
  is_first_nk_ = true;
  return ret;
}

int ObDASSortOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (!is_async_nested_scope(scope_)) {
    ret = advance_sorted_rowid(target, curr_id, score);
  } else {
    if (target.is_max() || curr_doc_id_.is_max()) {
      ret = OB_ITER_END;
    } else if (curr_doc_id_.is_normal()
        && (target.is_min() || curr_doc_id_.get_uint64() >= target.get_uint64())) {
      curr_id = curr_doc_id_;
      score = is_scoring_ ? boost_weight_ : 0.0;
    } else if (nested_input_end_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(advance_sorted_rowid(target, curr_id, score))) {
    } else {
      curr_doc_id_ = curr_id;
      curr_nk_.set_min();
      is_first_nk_ = true;
    }
    if (OB_ITER_END == ret) {
      curr_doc_id_.set_max();
      curr_nk_.set_max();
    }
  }
  return ret;
}

int ObDASSortOp::do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &curr_nk, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (!is_async_nested_scope(scope_)) {
    ret = OB_NOT_IMPLEMENT;
  } else if (!curr_doc_id_.is_normal()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (curr_nk_.is_max() || target.is_max()) {
    ret = OB_ITER_END;
  } else {
    int cmp = 0;
    if (OB_FAIL(compare_rowid(target, curr_nk_, cmp))) {
      LOG_WARN("failed to compare nested keys", K(ret), K(target), K_(curr_nk));
    } else if (curr_nk_.is_normal() && cmp <= 0) {
      curr_nk = curr_nk_;
    } else {
      ObDASNestedKey candidate;
      cmp = 1;
      while (OB_SUCC(ret) && cmp > 0) {
        if (OB_FAIL(next_impl(candidate, score))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to read sorted nested key", K(ret));
        } else if (OB_FAIL(compare_rowid(target, candidate, cmp))) {
          LOG_WARN("failed to compare nested keys", K(ret), K(target), K(candidate));
        }
      }
      if (OB_SUCC(ret)) {
        // Keep target valid until comparisons finish; it may borrow the previous output.
        curr_nk = candidate;
        if (candidate.length() > sizeof(uint64_t) && OB_FAIL(copy_nested_key(curr_nk))) {
          LOG_WARN("failed to preserve sorted nested key", K(ret), K(candidate));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    curr_nk_ = curr_nk;
  } else if (OB_ITER_END == ret) {
    curr_nk_.set_max();
  }
  return ret;
}

int ObDASSortOp::advance_sorted_rowid(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  score = 0.0;
  if (OB_ISNULL(rowid_store_) || OB_ISNULL(sort_impl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store or sort impl", K(ret));
  } else if (!sort_finished_ && OB_FAIL(do_sort())) {
    LOG_WARN("failed to do sort", K(ret));
  } else {
    ret = rowid_store_iter_.lower_bound(target);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowid_store_iter_.get_cur_rowid(curr_id))) {
        LOG_WARN("failed to get rowid", K(ret));
      } else {
        score = is_scoring_ ? boost_weight_ : 0.0;
      }
    } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("falied to find lower bound in store", K(ret));
    } else {
      ret = OB_SUCCESS;
      bool reached = false;
      int64_t storage_count = 0;
      // sort_impl_ use ObSortOpImpl/ObUniqueSortImpl, which directly write batch datum
      // while the expr may init as VEC_FIXED/VEC_CONTINUOUS/VEC_DISCRETE
      for (int64_t i = 0; i < get_sort_rowid_exprs().count() && OB_SUCC(ret); ++i) {
        ObExpr *e = get_sort_rowid_exprs().at(i);
        if (e->get_format(eval_ctx()) == VEC_UNIFORM) {
        } else if (OB_FAIL(e->init_vector(eval_ctx(), VEC_UNIFORM, max_batch_size(), false))) {
          LOG_WARN("failed to init vector", K(ret));
        }
      }
      while (OB_SUCC(ret) && !reached) {
        if (OB_FAIL(sort_impl_->get_next_batch(get_sort_rowid_exprs(), max_batch_size(), storage_count))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("failed to get next batch from sort impl", K(ret));
          } else if (storage_count > 0) {
            ret = OB_SUCCESS;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(search_ctx_.lower_bound_in_frame(scope_, target, &get_sort_rowid_exprs(), storage_count, idx))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("falied to find lower bound", K(ret));
          } else {
            // Current batch has all rows < target. sort_impl_ output is globally sorted
            // across batches, so a later batch may still contain rows >= target.
            // Reset ret so the outer while loop pulls the next batch instead of
            // reporting ITER_END prematurely.
            ret = OB_SUCCESS;
          }
        } else {
          rowid_store_->reuse();

          if (OB_SUCC(ret)) {
            if (OB_FAIL(rowid_store_->fill(idx, storage_count, get_sort_rowid_exprs()))) {
              LOG_WARN("failed to fill rowid store", K(ret));
            } else if (rowid_store_->count() == 0) {
              // no row satisfy the condition, continue to get next batch
            } else if (FALSE_IT(rowid_store_iter_.reuse())) {
            } else if (OB_FAIL(rowid_store_iter_.get_cur_rowid(curr_id))) {
              LOG_WARN("failed to get rowid", K(ret));
            } else {
              reached = true;
              score = is_scoring_ ? boost_weight_ : 0.0;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDASSortOp::next_impl(ObDASNestedKey &key, double &score)
{
  int ret = OB_SUCCESS;
  bool found = false;
  while (OB_SUCC(ret) && !found) {
    ObDASRowID doc;
    ObDatum datum;
    if (is_first_nk_) {
      is_first_nk_ = false;
      if (OB_FAIL(rowid_store_iter_.get_cur_rowid(doc))) {
        LOG_WARN("failed to get current rowid", K(ret));
      }
    } else if (OB_FAIL(next_sorted_rowid(doc, score))) {
      if (OB_ITER_END == ret) {
        nested_input_end_ = true;
      } else {
        LOG_WARN("failed to get next sorted rowid", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (doc.get_uint64() != curr_doc_id_.get_uint64()) {
      // Leave the next document's first pair for the DocID interface.
      ret = OB_ITER_END;
    } else if (OB_FAIL(static_cast<CompactRowIDStore *>(rowid_store_)->get_datum(
            rowid_store_iter_.cur_idx(), 1, datum))) {
      LOG_WARN("failed to read sorted nested key", K(ret));
    } else if (!datum.is_null() && datum.len_ >= nested_compare_bytes()) {
      key.assign(datum.ptr_, datum.len_);
      found = true;
    }
  }
  score = 0.0;
  return ret;
}

int ObDASSortOp::do_next_rowid(ObDASNestedKey &next_nk, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (!is_async_nested_scope(scope_)) {
    ret = OB_NOT_IMPLEMENT;
  } else if (!curr_doc_id_.is_normal()) {
    ret = OB_STATE_NOT_MATCH;
  } else if (curr_nk_.is_max()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(next_impl(next_nk, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to read sorted nested key", K(ret));
  } else {
    if (next_nk.length() > sizeof(uint64_t) && OB_FAIL(copy_nested_key(next_nk))) {
      LOG_WARN("failed to preserve sorted nested key", K(ret), K(next_nk));
    } else {
      curr_nk_ = next_nk;
    }
  }
  if (OB_ITER_END == ret) {
    curr_nk_.set_max();
  }
  return ret;
}

int ObDASSortOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  if (!is_async_nested_scope(scope_)) {
    if (OB_FAIL(next_sorted_rowid(next_id, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get next sorted rowid", K(ret));
    }
  } else {
    if (curr_doc_id_.is_max() || nested_input_end_) {
      ret = OB_ITER_END;
    } else {
      if (curr_doc_id_.is_normal()) {
        if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
          LOG_WARN("failed to get current rowid", K(ret));
        }
      } else if (OB_FAIL(next_sorted_rowid(next_id, score))) {
        LOG_WARN_IGNORE_ITER_END(ret, "failed to get next sorted rowid", K(ret));
      }
      // NK may have already stopped at the next document's first pair.
      while (OB_SUCC(ret) && curr_doc_id_.is_normal()
          && next_id.get_uint64() == curr_doc_id_.get_uint64()) {
        if (OB_FAIL(next_sorted_rowid(next_id, score))) {
          LOG_WARN_IGNORE_ITER_END(ret, "failed to get next sorted rowid", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        curr_doc_id_ = next_id;
        curr_nk_.set_min();
        is_first_nk_ = true;
        score = is_scoring_ ? boost_weight_ : 0.0;
      }
    }
    if (OB_ITER_END == ret) {
      curr_doc_id_.set_max();
      curr_nk_.set_max();
    }
  }
  return ret;
}

int ObDASSortOp::next_sorted_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (OB_ISNULL(rowid_store_) || OB_ISNULL(sort_impl_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store or sort impl", K(ret));
  } else if (!sort_finished_ && OB_FAIL(do_sort())) {
    LOG_WARN("failed to do sort", K(ret));
  } else {
    rowid_store_iter_.next_idx();
    if (!rowid_store_iter_.is_empty()) {
      if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
        LOG_WARN("failed to get next rowid", K(ret));
      } else {
        score = is_scoring_ ? boost_weight_ : 0.0;
      }
    } else {
      int64_t storage_count = 0;
      rowid_store_->reuse();
      // sort_impl_ use ObSortOpImpl/ObUniqueSortImpl, which directly write batch datum
      // while the expr may init as VEC_FIXED/VEC_CONTINUOUS/VEC_DISCRETE
      for (int64_t i = 0; i < get_sort_rowid_exprs().count() && OB_SUCC(ret); ++i) {
        ObExpr *e = get_sort_rowid_exprs().at(i);
        if (e->get_format(eval_ctx()) == VEC_UNIFORM) {
        } else if (OB_FAIL(e->init_vector(eval_ctx(), VEC_UNIFORM, max_batch_size(), false))) {
          LOG_WARN("failed to init vector", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sort_impl_->get_next_batch(get_sort_rowid_exprs(), max_batch_size(), storage_count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next rows", K(ret));
        } else if (storage_count > 0) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(rowid_store_->fill(0, storage_count, get_sort_rowid_exprs()))) {
        LOG_WARN("failed to fill rowid store", K(ret));
      } else if (rowid_store_->count() == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty rowid store", K(ret));
      } else if (FALSE_IT(rowid_store_iter_.reuse())) {
      } else if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
        LOG_WARN("failed to get next rowid", K(ret));
      } else {
        score = is_scoring_ ? boost_weight_ : 0.0;
      }
    }
  }
  return ret;
}

int ObDASSortOp::do_advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret), K(target));
  } else if (inclusive) {
    max_score_tuple_.set(target, target, is_scoring_ ? boost_weight_ : 0.0);
    max_score_tuple = &max_score_tuple_;
  } else if (OB_FAIL(ObIDASSearchOp::do_advance_shallow(target, inclusive, max_score_tuple))) {
    LOG_WARN("failed to advance shallow", K(ret), K(target));
  }
  return ret;
}

int ObDASSortOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  threshold = is_scoring_ ? boost_weight_ : 0.0;
  return ret;
}

int ObDASSortOp::do_sort()
{
  int ret = OB_SUCCESS;
  ObBitVector *fake_skip = to_bit_vector(sort_memctx_->get_arena_allocator().alloc(ObBitVector::memory_size(max_batch_size())));
  fake_skip->init(max_batch_size());
  if (OB_ISNULL(input_op_) || OB_ISNULL(sort_impl_) || OB_ISNULL(input_rowid_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    int64_t count = 0;
    while (OB_SUCC(ret)) {
      count = 0;
      if (OB_FAIL(input_op_->next_rowids(max_batch_size(), count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed ro get next rows from child iter", K(ret));
        } else if (count > 0) {
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCCESS == ret || (OB_ITER_END == ret && count > 0)) {
        fake_skip->reset(max_batch_size());
        if (is_nested_scope(scope_) && search_ctx_.has_docid_range()) {
          ObIVector *doc_id_vec = nullptr;
          ObExpr *doc_id_expr = get_sort_rowid_exprs().empty()
              ? nullptr : get_sort_rowid_exprs().at(0);
          if (OB_ISNULL(doc_id_expr)
              || OB_ISNULL(doc_id_vec = doc_id_expr->get_vector(eval_ctx()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("nested sort DOCID_RANGE requires a doc id vector", K(ret),
                KP(doc_id_expr), K(count));
          } else {
            const bool has_lo = !search_ctx_.get_docid_range_lo().is_min_value();
            const bool has_hi = !search_ctx_.get_docid_range_hi().is_max_value();
            const uint64_t lo = has_lo
                ? search_ctx_.get_docid_range_lo().get_uint64() : 0;
            const uint64_t hi = has_hi
                ? search_ctx_.get_docid_range_hi().get_uint64() : UINT64_MAX;
            for (int64_t i = 0; i < count; ++i) {
              const uint64_t doc_id = doc_id_vec->get_uint64(i);
              if ((has_lo && doc_id < lo) || (has_hi && doc_id > hi)) {
                fake_skip->set(i);
              }
            }
          }
        }
        // When nk_truncate_depth_ > 0, truncate nested_key to depth*BYTES_PER_LEVEL bytes so that
        // deeper-level rows (e.g. BYTES_PER_LEVEL bytes nk from $.comments[].replies[]) are collapsed to
        // the current nested element identity (4B for depth=1). ObUniqueSortImpl then
        // correctly deduplicates by (doc_id, truncated_nk).
        bool truncation_ok = true;
        if (OB_SUCC(ret) && nk_truncate_depth_ > 0 && get_sort_rowid_exprs().count() > 1) {
          const int64_t truncate_len = nk_truncate_depth_ * ObSearchIndexNestedKeyEncoder::BYTES_PER_LEVEL;
          int64_t row_id_col_count = get_sort_rowid_exprs().count();
          ObExpr *nk_expr = get_sort_rowid_exprs().at(row_id_col_count - 1);
          ObIVector *vec = nullptr;
          if (OB_ISNULL(nk_expr) || OB_ISNULL(vec = nk_expr->get_vector(eval_ctx()))) {
            // In the unique_sort NESTED-scope branch, a missing nk column violates the
            // CG-side contract. Silently skipping truncation would let ObUniqueSortImpl
            // dedup on the full nested_key and leak deeper-level rows into the result —
            // fail loud instead of returning wrong results.
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unique_sort with nk_truncate_depth expects a valid nk vector",
                     K(ret), KP(nk_expr), K(count), K(nk_truncate_depth_));
            truncation_ok = false;
          } else {
            for (int64_t i = 0; i < count; i++) {
              if (!fake_skip->at(i)
                  && vec->get_length(i) > static_cast<ObLength>(truncate_len)) {
                vec->set_length(i, static_cast<ObLength>(truncate_len));
              }
            }
          }
        }
        if (OB_SUCC(ret) && truncation_ok) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(sort_impl_->add_batch(get_sort_rowid_exprs(), *fake_skip, count, 0, nullptr))) {
            ret = tmp_ret;
            LOG_WARN("failed to add batch to sort impl", K(tmp_ret));
          }
        }
      }
    }

    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
      if (OB_FAIL(sort_impl_->sort())) {
        LOG_WARN("failed to do sort", K(ret));
      } else {
        sort_finished_ = true;
      }
    }
  }

  return ret;
}

void ObDASSortOp::reset_rowid_cursor()
{
  nested_input_end_ = false;
  curr_doc_id_.set_min();
  curr_nk_.set_min();
}

int ObDASSortOp::copy_nested_key(ObDASNestedKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key.length() > nested_compare_bytes())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nested key exceeds sort buffer length", K(ret), K(key), K_(nesting_depth));
  } else {
    MEMCPY(curr_nk_buf_, key.ptr(), key.length());
    key.assign(curr_nk_buf_, key.length());
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
