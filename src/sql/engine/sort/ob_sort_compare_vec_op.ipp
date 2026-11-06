/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

namespace oceanbase
{
namespace sql
{
/*********************************** start compare **********************************/
template <typename Store_Row, bool has_addon>
bool GeneralCompare<Store_Row, has_addon>::operator()(const Store_Row *l, const Store_Row *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_FAIL(fast_check_status())) {
    SQL_ENG_LOG(WARN, "fast check failed", K(ret));
  } else {
    if (CompareBase::ENABLE == encode_sk_state_) {
      ObLength l_len = 0;
      ObLength r_len = 0;
      const char *l_data = nullptr;
      const char *r_data = nullptr;
      l->get_cell_payload(*sk_row_meta_, 0, l_data, l_len);
      r->get_cell_payload(*sk_row_meta_, 0, r_data, r_len);
      int cmp = 0;
      cmp = MEMCMP(l_data, r_data, min(l_len, r_len));
      less = cmp != 0 ? (cmp < 0) : (l_len - r_len) < 0;
    } else if (CompareBase::FALLBACK_TO_DISABLE == encode_sk_state_ && has_addon) {
      const Store_Row *l_real_cmp_row = l->get_addon_ptr(*sk_row_meta_);
      const Store_Row *r_real_cmp_row = r->get_addon_ptr(*sk_row_meta_);
      less = (compare(l_real_cmp_row, r_real_cmp_row, addon_row_meta_) > 0);
    } else {
      __builtin_prefetch(l, 0 /* read */, 2 /*high temp locality*/);
      __builtin_prefetch(r, 0 /* read */, 2 /*high temp locality*/);
      less = (compare(l, r, sk_row_meta_) > 0);
    }
  }
  return less;
}

template <typename Store_Row, bool has_addon>
bool GeneralCompare<Store_Row, has_addon>::operator()(Store_Row **l, Store_Row **r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(*r, *l);
  }
  return less;
}

template <typename Store_Row, bool has_addon>
bool GeneralCompare<Store_Row, has_addon>::operator()(const SortVecOpChunk *l,
                                                      const SortVecOpChunk *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(r->sk_row_, l->sk_row_);
  }
  return less;
}

template <typename Store_Row, bool has_addon>
bool GeneralCompare<Store_Row, has_addon>::operator()(const Store_Row *r, ObEvalCtx &eval_ctx)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    less = (compare(r, eval_ctx, sk_row_meta_) > 0);
  }
  return less;
}

template <typename Store_Row, bool has_addon>
int GeneralCompare<Store_Row, has_addon>::with_ties_cmp(const Store_Row *r, ObEvalCtx &eval_ctx)
{
  int &ret = ret_;
  int cmp = 0;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    cmp = compare(r, eval_ctx, sk_row_meta_);
  }
  return cmp;
}

template <typename Store_Row, bool has_addon>
int GeneralCompare<Store_Row, has_addon>::with_ties_cmp(const Store_Row *l, const Store_Row *r)
{
  int cmp = 0;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    cmp = compare(l, r, sk_row_meta_);
  }
  return cmp;
}

template <typename Store_Row, bool has_addon>
int GeneralCompare<Store_Row, has_addon>::compare(const Store_Row *l, const Store_Row *r,
                                                  const RowMeta *row_meta)
{
  int &ret = ret_;
  int cmp = 0;
  ObLength l_len = 0;
  ObLength r_len = 0;
  bool l_null = false;
  bool r_null = false;
  const char *l_data = nullptr;
  const char *r_data = nullptr;
  for (int64_t i = cmp_start_; 0 == cmp && i < cmp_end_ && OB_SUCC(ret); i++) {
    const ObSortFieldCollation &sort_collation = cmp_sort_collations_->at(i);
    const ObExpr *e = cmp_sk_exprs_->at(sort_collation.field_idx_);
    l_null = l->is_null(sort_collation.field_idx_);
    l->get_cell_payload(*row_meta, sort_collation.field_idx_, l_data, l_len);
    r_null = r->is_null(sort_collation.field_idx_);
    r->get_cell_payload(*row_meta, sort_collation.field_idx_, r_data, r_len);
    if (OB_FAIL(cmp_funcs_.at(i)(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data, r_len,
                                 r_null, cmp))) {
      SQL_ENG_LOG(WARN, "failed to compare", K(ret));
    } else {
      cmp = sort_collation.is_ascending_ ? -cmp : cmp;
    }
  }
  return cmp;
}

template <typename Store_Row, bool has_addon>
int GeneralCompare<Store_Row, has_addon>::compare(const Store_Row *r, ObEvalCtx &eval_ctx,
                                                  const RowMeta *row_meta)
{
  int &ret = ret_;
  int cmp = 0;
  ObLength l_len = 0;
  ObLength r_len = 0;
  bool l_null = false;
  bool r_null = false;
  const char *l_data = nullptr;
  const char *r_data = nullptr;
  const int64_t batch_idx = eval_ctx.get_batch_idx();
  for (int64_t i = cmp_start_; 0 == cmp && i < cmp_end_ && OB_SUCC(ret); i++) {
    const ObSortFieldCollation &sort_collation = cmp_sort_collations_->at(i);
    const ObExpr *e = cmp_sk_exprs_->at(sort_collation.field_idx_);
    l_null = sk_col_result_list_[i].is_null(batch_idx);
    r_null = r->is_null(sort_collation.field_idx_);
    l_data = sk_col_result_list_[i].get_payload(batch_idx);
    l_len = sk_col_result_list_[i].get_length(batch_idx);
    r->get_cell_payload(*row_meta, sort_collation.field_idx_, r_data, r_len);
    if (OB_FAIL(cmp_funcs_.at(i)(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data, r_len,
                                 r_null, cmp))) {
      SQL_ENG_LOG(WARN, "failed to compare", K(ret));
    } else {
      cmp = sort_collation.is_ascending_ ? -cmp : cmp;
    }
  }
  return cmp;
}

template <typename Compare, typename Store_Row>
int PartitionSortComparer<Compare, Store_Row>::init(
    const ObIArray<ObExpr *> *sk_exprs,
    const ObIArray<ObSortFieldCollation> *sk_collations, const int64_t part_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sk_exprs) || OB_ISNULL(sk_collations)
      || OB_ISNULL(compare_.sk_row_meta_) || OB_ISNULL(compare_.addon_row_meta_)
      || OB_ISNULL(compare_.cmp_sk_exprs_) || OB_ISNULL(compare_.cmp_sort_collations_)
      || OB_UNLIKELY(part_cnt <= 0)
      || OB_UNLIKELY(sk_collations->count() < part_cnt + 1)
      || OB_UNLIKELY(compare_.cmp_funcs_.count() != compare_.cmp_sort_collations_->count())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid partition sort compare arguments", K(ret), KP(sk_exprs),
                KP(sk_collations), K(part_cnt));
  } else {
    sk_exprs_ = sk_exprs;
    sk_collations_ = sk_collations;
    part_cnt_ = part_cnt;
  }
  return ret;
}

template <typename Compare, typename Store_Row>
bool PartitionSortComparer<Compare, Store_Row>::operator()(const Store_Row *l,
                                                           const Store_Row *r)
{
  bool less = false;
  int &ret = compare_.ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_FAIL(compare_.fast_check_status())) {
    SQL_ENG_LOG(WARN, "fast check failed", K(ret));
  } else {
    less = compare_partition_sort_rows(l, r) > 0;
  }
  return less;
}

template <typename Compare, typename Store_Row>
bool PartitionSortComparer<Compare, Store_Row>::operator()(Store_Row **l, Store_Row **r)
{
  bool less = false;
  int &ret = compare_.ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Reverse the row comparison for the merge heap.
    less = (*this)(*r, *l);
  }
  return less;
}

template <typename Compare, typename Store_Row>
bool PartitionSortComparer<Compare, Store_Row>::operator()(const SortVecOpChunk *l,
                                                           const SortVecOpChunk *r)
{
  bool less = false;
  int &ret = compare_.ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Reverse the row comparison for the merge heap, matching GeneralCompare.
    less = (*this)(r->sk_row_, l->sk_row_);
  }
  return less;
}

template <typename Compare, typename Store_Row>
int PartitionSortComparer<Compare, Store_Row>::compare_partition_sort_rows(
    const Store_Row *l, const Store_Row *r)
{
  int &ret = compare_.ret_;
  int cmp = 0;
  if (OB_ISNULL(l) || OB_ISNULL(r) || OB_ISNULL(sk_exprs_)
      || OB_ISNULL(sk_collations_) || OB_ISNULL(compare_.sk_row_meta_)
      || OB_UNLIKELY(part_cnt_ <= 0)
      || OB_UNLIKELY(sk_collations_->count() < part_cnt_ + 1)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid partition sort compare arguments", K(ret), KP(l), KP(r),
                KP(sk_exprs_), KP(sk_collations_), KP(compare_.sk_row_meta_),
                K(part_cnt_));
  } else {
    ObLength l_len = 0;
    ObLength r_len = 0;
    bool l_null = false;
    bool r_null = false;
    const char *l_data = nullptr;
    const char *r_data = nullptr;
    for (int64_t i = 0; 0 == cmp && OB_SUCC(ret) && i <= part_cnt_; ++i) {
      const ObSortFieldCollation &sort_collation = sk_collations_->at(i);
      const uint32_t field_idx = sort_collation.field_idx_;
      if (OB_UNLIKELY(field_idx >= sk_exprs_->count())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "partition sort field index is invalid", K(ret), K(field_idx),
                    K(sk_exprs_->count()), K(i));
      } else {
        const ObExpr *e = sk_exprs_->at(field_idx);
        if (OB_ISNULL(e) || OB_ISNULL(e->basic_funcs_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "partition sort expression is invalid", K(ret), KP(e), K(field_idx));
        } else {
          l_null = l->is_null(field_idx);
          r_null = r->is_null(field_idx);
          l->get_cell_payload(*compare_.sk_row_meta_, field_idx, l_data, l_len);
          r->get_cell_payload(*compare_.sk_row_meta_, field_idx, r_data, r_len);
          const NullSafeRowCmpFunc cmp_func = NULL_FIRST == sort_collation.null_pos_
                                                  ? e->basic_funcs_->row_null_first_cmp_
                                                  : e->basic_funcs_->row_null_last_cmp_;
          if (OB_UNLIKELY(nullptr == cmp_func)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "partition sort compare function is null", K(ret), K(field_idx));
          } else if (OB_FAIL(cmp_func(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null,
                                      r_data, r_len, r_null, cmp))) {
            SQL_ENG_LOG(WARN, "failed to compare partition sort prefix", K(ret), K(field_idx));
          } else {
            // A positive result means the left row precedes the right row.
            // Hash order is always ascending, matching HashNodeComparer.
            cmp = (i == 0 || sort_collation.is_ascending_) ? -cmp : cmp;
          }
        }
      }
    }

    if (OB_SUCC(ret) && 0 == cmp) {
      const Store_Row *l_addon = nullptr;
      const Store_Row *r_addon = nullptr;
      if (OB_ISNULL(compare_.cmp_sort_collations_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "addon compare collations are null", K(ret));
      } else if (0 == compare_.cmp_sort_collations_->count()) {
        // No per-partition order-by columns remain in the addon row.
      } else if (OB_ISNULL(compare_.addon_row_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "addon row metadata is null", K(ret));
      } else if (OB_ISNULL(l_addon = l->get_addon_ptr(*compare_.sk_row_meta_))
                 || OB_ISNULL(r_addon = r->get_addon_ptr(*compare_.sk_row_meta_))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "addon row is null in partition sort compare", K(ret), KP(l_addon), KP(r_addon));
      } else if (OB_ISNULL(compare_.cmp_sk_exprs_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "addon compare expressions are null", K(ret));
      } else {
        for (int64_t i = 0; 0 == cmp && OB_SUCC(ret) && i < compare_.cmp_sort_collations_->count(); ++i) {
          const ObSortFieldCollation &sort_collation = compare_.cmp_sort_collations_->at(i);
          const uint32_t field_idx = sort_collation.field_idx_;
          if (OB_UNLIKELY(field_idx >= compare_.cmp_sk_exprs_->count())
              || OB_UNLIKELY(i >= compare_.cmp_funcs_.count())) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "addon sort field index is invalid", K(ret), K(field_idx),
                        K(compare_.cmp_sk_exprs_->count()), K(i), K(compare_.cmp_funcs_.count()));
          } else {
            const ObExpr *e = compare_.cmp_sk_exprs_->at(field_idx);
            if (OB_ISNULL(e)) {
              ret = OB_ERR_UNEXPECTED;
              SQL_ENG_LOG(WARN, "addon sort expression is null", K(ret), K(field_idx));
            } else {
              l_null = l_addon->is_null(field_idx);
              r_null = r_addon->is_null(field_idx);
              l_addon->get_cell_payload(*compare_.addon_row_meta_, field_idx, l_data, l_len);
              r_addon->get_cell_payload(*compare_.addon_row_meta_, field_idx, r_data, r_len);
              const NullSafeRowCmpFunc cmp_func = compare_.cmp_funcs_.at(i);
              if (OB_UNLIKELY(nullptr == cmp_func)) {
                ret = OB_ERR_UNEXPECTED;
                SQL_ENG_LOG(WARN, "addon sort compare function is null", K(ret), K(i));
              } else if (OB_FAIL(cmp_func(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null,
                                          r_data, r_len, r_null, cmp))) {
                SQL_ENG_LOG(WARN, "failed to compare partition sort addon", K(ret), K(i));
              } else {
                cmp = sort_collation.is_ascending_ ? -cmp : cmp;
              }
            }
          }
        }
      }
    }
  }
  return cmp;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::init_cmp_func(const ObIArray<ObExpr *> &cmp_sk_exprs)
{
  int ret = OB_SUCCESS;
  if (is_basic_cmp) {
#define BASIC_SORT_CMP_FUNC_SWITCH(type_class, null_first)                                         \
  case type_class: {                                                                               \
    basic_cmp_func_ = FixedCmpFunc<type_class, null_first>::cmp;                                   \
    basic_not_null_cmp_func_ = FixedCmpFunc<type_class, null_first>::cmp_not_null;                 \
    break;                                                                                         \
  }

#define CMP_FUNC_SWITCH(null_first)                                                               \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_INTEGER, null_first)                                          \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_UINTEGER, null_first)                                         \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DATE, null_first)                                             \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_TIME, null_first)                                             \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DATETIME, null_first)                                         \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_YEAR, null_first)                                             \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_BIT, null_first)                                              \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_ENUM_SET, null_first)                                         \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_INTERVAL_YM, null_first)                                      \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT32, null_first)                                        \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT64, null_first)                                        \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT128, null_first)                                       \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT256, null_first)                                       \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT512, null_first)                                       \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_MYSQL_DATE, null_first)                                       \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_MYSQL_DATETIME, null_first)

    const ObSortFieldCollation &sort_collation = cmp_sort_collations_->at(0);
    const ObExpr *expr = cmp_sk_exprs.at(0);
    VecValueTypeClass vec_tc = expr->get_vec_value_tc();
    bool null_first = (NULL_FIRST == sort_collation.null_pos_);
    if (null_first) {
      switch (vec_tc) {
        CMP_FUNC_SWITCH(true)
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid vector value type class", K(vec_tc), K(ret));
      }
    } else {
      switch (vec_tc) {
        CMP_FUNC_SWITCH(false)
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid vector value type class", K(vec_tc), K(ret));
      }
    }
#undef CMP_FUNC_SWITCH
#undef BASIC_SORT_CMP_FUNC_SWITCH
  } else {
    const ObSortFieldCollation &sort_collation = cmp_sort_collations_->at(0);
    const ObExpr *e = cmp_sk_exprs_->at(0);
    str_cmp_func_ = NULL_FIRST == sort_collation.null_pos_ ?
                          e->basic_funcs_->row_null_first_cmp_ :
                          e->basic_funcs_->row_null_last_cmp_;
  }
  if (cmp_sort_collations_->at(0).is_ascending_) {
    cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::not_null_compare<true>;
    topn_cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::not_null_topn_compare<true>;
  } else {
    cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::not_null_compare<false>;
    topn_cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::not_null_topn_compare<false>;
  }
  return ret;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::check_sort_key_has_null(ObEvalCtx &eval_ctx,
                                                                                     const ObBatchRows &input_brs)
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < cmp_sk_exprs_->count() && !has_null; i++) {
    ObExpr *expr = cmp_sk_exprs_->at(i);
    if (OB_FAIL(expr->eval_vector(eval_ctx, input_brs))) {
      SQL_ENG_LOG(WARN, "failed to eval vector", K(ret));
    } else {
      ObIVector *vector = expr->get_vector(eval_ctx);
      if (vector->has_null()) {
        VectorFormat format = vector->get_format();
        if (common::VEC_UNIFORM == format) {
          const ObDatum *datums = static_cast<ObUniformBase *>(vector)->get_datums();
          for (int64_t i = 0; i < input_brs.size_; i++) {
            if (!input_brs.skip_->at(i) && datums[i].is_null()) {
              has_null = true;
              break;
            }
          }
        } else {
          has_null = true;
        }
      }
    }
  }
  if (has_null) {
    if (cmp_sort_collations_->at(0).is_ascending_) {
      cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_compare<true>;
      topn_cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_topn_compare<true>;
    } else {
      cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_compare<false>;
      topn_cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_topn_compare<false>;
    }
  }
  return ret;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::check_sort_key_has_null(
                                                    const ObCompactRow **sk_stored_rows, const int64_t row_size)
{
  int ret = OB_SUCCESS;
  bool has_null = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_size; i++) {
    const ObCompactRow *sk_stored_row = sk_stored_rows[i];
    for (int64_t j = 0; j < cmp_sort_collations_->count(); j++) {
      const ObSortFieldCollation &sort_collation = cmp_sort_collations_->at(j);
      if (sk_stored_row->is_null(sort_collation.field_idx_)) {
        has_null = true;
        break;
      }
    }
  }
  if (has_null) {
    if (cmp_sort_collations_->at(0).is_ascending_) {
      cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_compare<true>;
      topn_cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_topn_compare<true>;
    } else {
      cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_compare<false>;
      topn_cmp_func_ = &SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_topn_compare<false>;
    }
  }
  return ret;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::init(
  const ObIArray<ObExpr *> *cmp_sk_exprs, const RowMeta *sk_row_meta, const RowMeta *addon_row_meta,
  const ObIArray<ObSortFieldCollation> *cmp_sort_collations, ObExecContext *exec_ctx,
  bool enable_encode_sortkey)
{
  int ret = OB_SUCCESS;
  if (nullptr == cmp_sk_exprs || nullptr == cmp_sort_collations || nullptr == exec_ctx) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(cmp_sort_collations));
  } else {
    cmp_sk_exprs_ = cmp_sk_exprs;
    sk_row_meta_ = sk_row_meta;
    addon_row_meta_ = addon_row_meta;
    cmp_sort_collations_ = cmp_sort_collations;
    exec_ctx_ = exec_ctx;
    cnt_ = cmp_sort_collations_->count();
    cmp_start_ = 0;
    cmp_end_ = cmp_sort_collations_->count();
    cmp_obj_meta_ = cmp_sk_exprs->at(0)->obj_meta_;
    cs_ = ObCharset::get_charset(cmp_obj_meta_.get_collation_type());
    end_with_space_ =
      is_calc_with_end_space(cmp_obj_meta_.get_type(), cmp_obj_meta_.get_type(), lib::is_oracle_mode(),
                             cmp_obj_meta_.get_collation_type(), cmp_obj_meta_.get_collation_type());
    const ObDatumMeta &datum_data = cmp_sk_exprs->at(0)->datum_meta_;
  }
  if (OB_SUCC(ret) && OB_FAIL(init_cmp_func(*cmp_sk_exprs))) {
    SQL_ENG_LOG(WARN, "failed to init compare sort key", K(ret));
  }
  return ret;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
bool SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::operator()(const Store_Row *l,
                                                                                    const Store_Row *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_FAIL(fast_check_status())) {
    SQL_ENG_LOG(WARN, "fast check failed", K(ret));
  } else {
    __builtin_prefetch(l, 0 /* read */, 2 /*high temp locality*/);
    __builtin_prefetch(r, 0 /* read */, 2 /*high temp locality*/);
    less = (compare(l, r, sk_row_meta_) > 0);
  }
  return less;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
bool SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::operator()(Store_Row **l, Store_Row **r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(*r, *l);
  }
  return less;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
bool SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::operator()(const SortVecOpChunk *l,
                                                                                    const SortVecOpChunk *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(r->sk_row_, l->sk_row_);
  }
  return less;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
bool SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::operator()(const Store_Row *r,
                                                                                    ObEvalCtx &eval_ctx)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    less = (compare(r, eval_ctx, sk_row_meta_) > 0);
  }
  return less;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::with_ties_cmp(const Store_Row *r,
                                                                                      ObEvalCtx &eval_ctx)
{
  int &ret = ret_;
  int cmp = 0;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    cmp = compare(r, eval_ctx, sk_row_meta_);
  }
  return cmp;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::with_ties_cmp(const Store_Row *l,
                                                                                      const Store_Row *r)
{
  int cmp = 0;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    cmp = compare(l, r, sk_row_meta_);
  }
  return cmp;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
template <bool is_ascending>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_compare(const Store_Row *l,
                                                                                        const Store_Row *r,
                                                                                        const RowMeta *row_meta)
{
  int &ret = ret_;
  int cmp = 0;
  if (OB_SUCC(ret)) {
    ObLength l_len = 0;
    ObLength r_len = 0;
    bool l_null = false;
    bool r_null = false;
    const char *l_data = nullptr;
    const char *r_data = nullptr;
    const ObExpr *e = cmp_sk_exprs_->at(0);
    l_null = l->is_null(0);
    l->get_cell_payload(*row_meta, 0, l_data, l_len);
    r_null = r->is_null(0);
    r->get_cell_payload(*row_meta, 0, r_data, r_len);
    if (is_basic_cmp) {
      cmp = is_ascending ? -basic_cmp_func_(l_data, l_null, r_data, r_null)
                         : basic_cmp_func_(l_data, l_null, r_data, r_null);
    } else if (OB_FAIL(str_cmp_func_(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data, r_len, r_null, cmp))) {
      SQL_ENG_LOG(WARN, "failed to compare", K(ret));
    } else {
      cmp = is_ascending ? -cmp : cmp;
    }
  }
  return cmp;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
template <bool is_ascending>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::not_null_compare(const Store_Row *l,
                                                                                         const Store_Row *r,
                                                                                         const RowMeta *row_meta)
{
  int &ret = ret_;
  int cmp = 0;
  if (!is_basic_cmp) {
    ObLength l_len = *(reinterpret_cast<const ObLength *>(
        reinterpret_cast<const char *>(l) + LEN_OFFSET));
    ObLength r_len = *(reinterpret_cast<const ObLength *>(
        reinterpret_cast<const char *>(r) + LEN_OFFSET));
    const unsigned char *l_data = reinterpret_cast<const unsigned char *>(
        reinterpret_cast<const char *>(l) + DATA_OFFSET);
    const unsigned char *r_data = reinterpret_cast<const unsigned char *>(
        reinterpret_cast<const char *>(r) + DATA_OFFSET);
    int cmp_ret = cs_->coll->strnncollsp(cs_, l_data, l_len, r_data, r_len, end_with_space_);
    cmp = (cmp_ret > 0 ? is_ascending ? -1 : 1 : (cmp_ret < 0 ? is_ascending ? 1 : -1 : 0));
  } else {
    const unsigned char *l_data = reinterpret_cast<const unsigned char *>(
        reinterpret_cast<const char *>(l) + FIXED_DATA_OFFSET);
    const unsigned char *r_data = reinterpret_cast<const unsigned char *>(
        reinterpret_cast<const char *>(r) + FIXED_DATA_OFFSET);
    cmp = is_ascending ? -basic_not_null_cmp_func_(l_data, r_data)
                       : basic_not_null_cmp_func_(l_data, r_data);
  }
  return cmp;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::compare(const Store_Row *l,
                                                                                const Store_Row *r,
                                                                                const RowMeta *row_meta)
{
  return (this->*cmp_func_)(l, r, row_meta);
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
template <bool is_ascending>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::default_topn_compare(const Store_Row *r,
                                                                                              ObEvalCtx &eval_ctx,
                                                                                              const RowMeta *row_meta)
{
  int &ret = ret_;
  int cmp = 0;
  if (OB_SUCC(ret)) {
    ObLength l_len = 0;
    ObLength r_len = 0;
    bool l_null = false;
    bool r_null = false;
    const char *l_data = nullptr;
    const char *r_data = nullptr;
    const int64_t batch_idx = eval_ctx.get_batch_idx();
    const ObSortFieldCollation &sort_collation = cmp_sort_collations_->at(0);
    const ObExpr *e = cmp_sk_exprs_->at(sort_collation.field_idx_);
    l_null = sk_col_result_list_[0].is_null(batch_idx);
    r_null = r->is_null(sort_collation.field_idx_);
    l_data = sk_col_result_list_[0].get_payload(batch_idx);
    l_len = sk_col_result_list_[0].get_length(batch_idx);
    r->get_cell_payload(*row_meta, sort_collation.field_idx_, r_data, r_len);
    if (is_basic_cmp) {
      cmp = is_ascending ? -basic_cmp_func_(l_data, l_null, r_data, r_null)
                          : basic_cmp_func_(l_data, l_null, r_data, r_null);
    } else if (OB_FAIL(str_cmp_func_(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data, r_len, r_null, cmp))) {
      SQL_ENG_LOG(WARN, "failed to compare", K(ret));
    } else {
      cmp = is_ascending ? -cmp : cmp;
    }
  }
  return cmp;
}


template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
template <bool is_ascending>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::not_null_topn_compare(const Store_Row *r,
                                                                                              ObEvalCtx &eval_ctx,
                                                                                              const RowMeta *row_meta)
{
  int &ret = ret_;
  int cmp = 0;
  ObLength l_len = 0;
  const unsigned char *l_data = nullptr;
  const int64_t batch_idx = eval_ctx.get_batch_idx();
  l_data = reinterpret_cast<const unsigned char *>(
      sk_col_result_list_[0].get_payload(batch_idx));
  l_len = sk_col_result_list_[0].get_length(batch_idx);
  ObLength r_len = *(reinterpret_cast<const ObLength *>(
      reinterpret_cast<const char *>(r) + LEN_OFFSET));
  if (!is_basic_cmp) {
    const unsigned char *r_data = reinterpret_cast<const unsigned char *>(
      reinterpret_cast<const char *>(r) + DATA_OFFSET);
    int cmp_ret = cs_->coll->strnncollsp(cs_, l_data, l_len, r_data, r_len, end_with_space_);
    cmp = (cmp_ret > 0 ? is_ascending ? -1 : 1 : (cmp_ret < 0 ? is_ascending ? 1 : -1 : 0));
  } else {
    const unsigned char *r_data = reinterpret_cast<const unsigned char *>(
      reinterpret_cast<const char *>(r) + FIXED_DATA_OFFSET);
    cmp = is_ascending ? -basic_not_null_cmp_func_(l_data, r_data)
                       : basic_not_null_cmp_func_(l_data, r_data);
  }
  return cmp;
}

template <typename Store_Row, bool has_addon, bool is_basic_cmp, bool is_topn_sort>
int SingleColCompare<Store_Row, has_addon, is_basic_cmp, is_topn_sort>::compare(const Store_Row *r,
                                                                                ObEvalCtx &eval_ctx,
                                                                                const RowMeta *row_meta)
{
  return (this->*topn_cmp_func_)(r, eval_ctx, row_meta);
}

template <typename Store_Row, bool has_addon>
int FixedCompare<Store_Row, has_addon>::init_basic_cmp_func(
  const ObIArray<ObExpr *> &cmp_sk_exprs, const ObIArray<ObSortFieldCollation> &cmp_sort_collations)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(basic_cmp_funcs_.init(cmp_sort_collations.count()))) {
    SQL_ENG_LOG(WARN, "failed to init sort collations", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cmp_sort_collations.count(); i++) {
#define BASIC_SORT_CMP_FUNC_SWITCH(type_class, null_first)                                         \
  case type_class: {                                                                               \
    auto basic_cmp_func = FixedCmpFunc<type_class, null_first>::cmp;                               \
    if (OB_FAIL(basic_cmp_funcs_.push_back(basic_cmp_func))) {                                     \
      SQL_ENG_LOG(WARN, "failed to add basic compare func", K(ret));                               \
    }                                                                                              \
    break;                                                                                         \
  }

#define CMP_FUNC_SWITCH(type_class, null_first)                                                    \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_INTEGER, null_first);                                          \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_UINTEGER, null_first);                                         \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DATE, null_first);                                             \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_TIME, null_first);                                             \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DATETIME, null_first);                                         \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_YEAR, null_first);                                             \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_BIT, null_first);                                              \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_ENUM_SET, null_first);                                         \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_INTERVAL_YM, null_first);                                      \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT32, null_first);                                        \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT64, null_first);                                        \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT128, null_first);                                       \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT256, null_first);                                       \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_DEC_INT512, null_first);                                       \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_MYSQL_DATE, null_first);                                       \
  BASIC_SORT_CMP_FUNC_SWITCH(VEC_TC_MYSQL_DATETIME, null_first);

    const ObSortFieldCollation &sort_collation = cmp_sort_collations.at(i);
    const ObExpr *expr = cmp_sk_exprs.at(sort_collation.field_idx_);
    VecValueTypeClass vec_tc = expr->get_vec_value_tc();
    bool null_first = (NULL_FIRST == sort_collation.null_pos_);
    if (null_first) {
      switch (vec_tc) {
        CMP_FUNC_SWITCH(type_class, true);
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid vector value type class", K(vec_tc), K(ret));
      }
    } else {
      switch (vec_tc) {
        CMP_FUNC_SWITCH(type_class, false);
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_LOG(WARN, "invalid vector value type class", K(vec_tc), K(ret));
      }
    }
#undef CMP_FUNC_SWITCH
#undef BASIC_SORT_CMP_FUNC_SWITCH
  }
  return ret;
}

template <typename Store_Row, bool has_addon>
int FixedCompare<Store_Row, has_addon>::init(
  const ObIArray<ObExpr *> *cmp_sk_exprs, const RowMeta *sk_row_meta, const RowMeta *addon_row_meta,
  const ObIArray<ObSortFieldCollation> *cmp_sort_collations, ObExecContext *exec_ctx,
  bool enable_encode_sortkey)
{
  int ret = OB_SUCCESS;
  if (enable_encode_sortkey) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "encode sort does not support basic compare", K(ret));
  } else if (nullptr == cmp_sk_exprs || nullptr == cmp_sort_collations || nullptr == exec_ctx) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(cmp_sort_collations));
  } else if (OB_FAIL(init_basic_cmp_func(*cmp_sk_exprs, *cmp_sort_collations))) {
    SQL_ENG_LOG(WARN, "failed to init compare sort key", K(ret));
  } else {
    cmp_sk_exprs_ = cmp_sk_exprs;
    sk_row_meta_ = sk_row_meta;
    addon_row_meta_ = addon_row_meta;
    cmp_sort_collations_ = cmp_sort_collations;
    exec_ctx_ = exec_ctx;
    cnt_ = cmp_sort_collations_->count();
    cmp_start_ = 0;
    cmp_end_ = cmp_sort_collations_->count();
  }
  return ret;
}

template <typename Store_Row, bool has_addon>
bool FixedCompare<Store_Row, has_addon>::operator()(const Store_Row *l, const Store_Row *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_FAIL(fast_check_status())) {
    SQL_ENG_LOG(WARN, "fast check failed", K(ret));
  } else {
    __builtin_prefetch(l, 0 /* read */, 2 /*high temp locality*/);
    __builtin_prefetch(r, 0 /* read */, 2 /*high temp locality*/);
    less = (compare(l, r, sk_row_meta_) > 0);
  }
  return less;
}

template <typename Store_Row, bool has_addon>
bool FixedCompare<Store_Row, has_addon>::operator()(Store_Row **l, Store_Row **r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(*r, *l);
  }
  return less;
}

template <typename Store_Row, bool has_addon>
bool FixedCompare<Store_Row, has_addon>::operator()(const SortVecOpChunk *l,
                                                    const SortVecOpChunk *r)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), KP(l), KP(r));
  } else {
    // Return the reverse order since the heap top is the maximum element.
    // NOTE: can not return !(*this)(l->row_, r->row_)
    //       because we should always return false if l == r.
    less = (*this)(r->sk_row_, l->sk_row_);
  }
  return less;
}

template <typename Store_Row, bool has_addon>
bool FixedCompare<Store_Row, has_addon>::operator()(const Store_Row *r, ObEvalCtx &eval_ctx)
{
  bool less = false;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    __builtin_prefetch(r, 0 /* read */, 2 /*high temp locality*/);
    less = (compare(r, eval_ctx, sk_row_meta_) > 0);
  }
  return less;
}

template <typename Store_Row, bool has_addon>
int FixedCompare<Store_Row, has_addon>::with_ties_cmp(const Store_Row *r, ObEvalCtx &eval_ctx)
{
  int &ret = ret_;
  int cmp = 0;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    __builtin_prefetch(r, 0 /* read */, 2 /*high temp locality*/);
    cmp = compare(r, eval_ctx, sk_row_meta_);
  }
  return cmp;
}

template <typename Store_Row, bool has_addon>
int FixedCompare<Store_Row, has_addon>::with_ties_cmp(const Store_Row *l, const Store_Row *r)
{
  int cmp = 0;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret)) {
  } else {
    __builtin_prefetch(l, 0 /* read */, 2 /*high temp locality*/);
    __builtin_prefetch(r, 0 /* read */, 2 /*high temp locality*/);
    cmp = compare(l, r, sk_row_meta_);
  }
  return cmp;
}

template <typename Store_Row, bool has_addon>
int FixedCompare<Store_Row, has_addon>::compare(const Store_Row *l, const Store_Row *r,
                                                const RowMeta *row_meta)
{
  int &ret = ret_;
  int cmp = 0;
  bool l_null = false;
  bool r_null = false;
  const char *l_data = nullptr;
  const char *r_data = nullptr;
  for (int64_t i = cmp_start_; 0 == cmp && i < cmp_end_ && OB_SUCC(ret); i++) {
    const ObSortFieldCollation &sort_collation = cmp_sort_collations_->at(i);
    l_null = l->is_null(sort_collation.field_idx_);
    l_data = l->get_cell_payload(*row_meta, sort_collation.field_idx_);
    r_null = r->is_null(sort_collation.field_idx_);
    r_data = r->get_cell_payload(*row_meta, sort_collation.field_idx_);
    cmp = basic_cmp_funcs_.at(i)(l_data, l_null, r_data, r_null);
    cmp = sort_collation.is_ascending_ ? -cmp : cmp;
  }
  return cmp;
}

template <typename Store_Row, bool has_addon>
int FixedCompare<Store_Row, has_addon>::compare(const Store_Row *r, ObEvalCtx &eval_ctx,
                                                const RowMeta *row_meta)
{
  int &ret = ret_;
  int cmp = 0;
  bool l_null = false;
  bool r_null = false;
  const char *l_data = nullptr;
  const char *r_data = nullptr;
  const int64_t batch_idx = eval_ctx.get_batch_idx();
  for (int64_t i = cmp_start_; 0 == cmp && i < cmp_end_ && OB_SUCC(ret); i++) {
    const ObSortFieldCollation &sort_collation = cmp_sort_collations_->at(i);
    const ObExpr *e = cmp_sk_exprs_->at(sort_collation.field_idx_);
    l_null = sk_col_result_list_[i].is_null(batch_idx);
    l_data = sk_col_result_list_[i].get_payload(batch_idx);
    r_null = r->is_null(sort_collation.field_idx_);
    r_data = r->get_cell_payload(*row_meta, sort_collation.field_idx_);
    cmp = basic_cmp_funcs_.at(i)(l_data, l_null, r_data, r_null);
    cmp = sort_collation.is_ascending_ ? -cmp : cmp;
  }
  return cmp;
}
/*********************************** end Compare **********************************/

} // namespace sql
} // namespace oceanbase
