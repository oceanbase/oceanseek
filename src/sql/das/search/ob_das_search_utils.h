/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef DEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_UTILS_H_
#define DEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_UTILS_H_

#include "lib/container/ob_iarray.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "sql/das/search/ob_das_search_define.h"

namespace oceanbase
{
namespace storage
{
struct ObBlockMaxScoreIterParam;
}
namespace sql
{

enum class ObDASAsyncSegmentMergeMode
{
  SORT_MERGE = 0,
  DIRECT_MERGE
};

class ObDASSearchCtx;
class ObDASAsyncBindingGuard;
class ObIDASSearchOp;
struct ObIDASSearchCtDef;
struct ObIDASSearchRtDef;
struct ObDASIRScanCtDef;
struct ObDASIRScanRtDef;
struct ObDASScalarScanCtDef;

class ObDASSearchUtils
{
public:
  // Owns the storage scan range and its rowkey objects for an async scalar SQL range.
  // range_ references the arrays in this object, so this object must outlive every use of range_.
  struct AsyncScalarTermRange
  {
    AsyncScalarTermRange() : range_() {}
    common::ObNewRange range_;
    common::ObObj start_objs_[share::SEARCH_INDEX_KEY_CNT];
    common::ObObj end_objs_[share::SEARCH_INDEX_KEY_CNT];
  private:
    DISALLOW_COPY_AND_ASSIGN(AsyncScalarTermRange);
  };

  static int eval_and_tokenize_query_text(
      common::ObIAllocator &allocator,
      ObEvalCtx &eval_ctx,
      const ObExpr &query_text_expr,
      const ObDASScalarScanCtDef &inv_idx_scan_ctdef,
      common::ObIArray<common::ObString> &query_tokens);
  // Deduplicated variant: each unique token appears once in query_tokens, and
  // token_counts records its occurrence count in the query text.
  static int eval_and_tokenize_query_text(
      common::ObIAllocator &allocator,
      ObEvalCtx &eval_ctx,
      const ObExpr &query_text_expr,
      const ObDASScalarScanCtDef &inv_idx_scan_ctdef,
      common::ObIArray<common::ObString> &query_tokens,
      common::ObIArray<int64_t> &token_counts);
  static int project_async_scalar_term_range(
      const common::ObNewRange &range,
      AsyncScalarTermRange &term_range);
  typedef int (*AsyncCompoundOpGenerator)(
      ObIDASSearchRtDef &rtdef,
      const ObIDASSearchCtDef &ctdef,
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op);

  static int resolve_async_segment_domain(
      const ObDASIRScanCtDef &ir_ctdef,
      ObDASSearchCtx &search_ctx,
      uint64_t &index_table_id,
      int64_t &max_segment_cnt);
  static int resolve_async_field_segment_domain(
      const ObDASIRScanCtDef &ir_ctdef,
      ObDASSearchCtx &search_ctx,
      ObDASAsyncBindingGuard &binding_guard,
      uint64_t &index_table_id,
      int64_t &segment_op_cnt);
  // rtdef, ctdef, is_async_index, and generator must describe the same concrete query type.
  // The generator may be called repeatedly per Segment under a temporary async-index binding,
  // so validate the concrete type before casting.
  static int generate_async_compound_query_op(
      ObIDASSearchRtDef &rtdef,
      const ObIDASSearchCtDef &ctdef,
      const bool is_async_index,
      ObIDASSearchOp *pushdown_filter_op,
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      AsyncCompoundOpGenerator generator,
      ObIDASSearchOp *&op);
  static int create_async_segment_merge_op(
      const common::ObIArray<ObIDASSearchOp *> &segment_ops,
      const uint64_t index_table_id,
      const bool is_scoring,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&segment_merge_op,
      const bool force_merge = false,
      const ObDASAsyncSegmentMergeMode mode = ObDASAsyncSegmentMergeMode::SORT_MERGE);
  static int create_async_segment_group_merge_op(
      const common::ObIArray<ObIDASSearchOp *> &group_ops,
      const bool is_scoring,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&group_merge_op);
  static int create_token_search_op(
      const ObDASIRScanCtDef &ir_ctdef,
      ObDASIRScanRtDef &ir_rtdef,
      storage::ObBlockMaxScoreIterParam *block_max_param,
      const common::ObString &query_token,
      const double token_boost,
      const bool is_scoring,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&token_op);

  struct FixedFormatAccessor
  {
    FixedFormatAccessor(const char *payload, int64_t len) : payload_(payload), len_(len) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      return ObDatum(payload_ + idx * len_, len_, is_null);
    }
    const char *payload_;
    int64_t len_;
  };

  struct ContinuousFormatAccessor
  {
    ContinuousFormatAccessor(const char *payload, const uint32_t *offsets)
        : payload_(payload), offsets_(offsets) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      return ObDatum(payload_ + offsets_[idx], offsets_[idx + 1] - offsets_[idx], is_null);
    }
    const char *payload_;
    const uint32_t *offsets_;
  };

  struct DiscreteFormatAccessor
  {
    DiscreteFormatAccessor(char **ptrs, int32_t *lens) : ptrs_(ptrs), lens_(lens) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      return ObDatum(ptrs_[idx], lens_[idx], is_null);
    }
    char **ptrs_;
    int32_t *lens_;
  };

  struct UniformFormatAccessor
  {
    UniformFormatAccessor(const ObDatum *datums) : datums_(datums) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      UNUSED(is_null);
      return datums_[idx];
    }
    const ObDatum *datums_;
  };

  struct UniformConstFormatAccessor
  {
    UniformConstFormatAccessor(const ObDatum *datums) : datums_(datums) {}
    ObDatum operator()(int64_t idx, bool is_null) const
    {
      UNUSED(idx);
      UNUSED(is_null);
      return datums_[0];
    }
    const ObDatum *datums_;
  };

  static int binary_search_lower_bound(ObEvalCtx &eval_ctx,
                                       int64_t count,
                                       const ObExpr &expr,
                                       const ObDatum &target_datum,
                                       int64_t &idx)
  {
    int ret = OB_SUCCESS;
    int64_t left = idx;
    int64_t right = count;
    while (OB_SUCC(ret) && left < right) {
      int64_t mid = (left + right) >> 1;
      int cmp = 0;
      ObDatum datum;
      if (OB_FAIL(get_datum(expr, eval_ctx, mid, datum))) {
        SQL_DAS_LOG(WARN, "failed to get datum", K(ret));
      } else if (OB_FAIL(expr.basic_funcs_->null_first_cmp_(datum, target_datum, cmp))) {
        SQL_DAS_LOG(WARN,"failed to compare datums", K(ret));
      } else if (cmp < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    if (OB_SUCC(ret)) {
      idx = left;
    }
    return ret;
  }

  // Find first position in [left_idx, right_bound) where vec[pos] > target_datum.
  static int binary_search_upper_bound(ObEvalCtx &eval_ctx,
                                       int64_t right_bound,
                                       const ObExpr &expr,
                                       const ObDatum &target_datum,
                                       int64_t left_idx,
                                       int64_t &result)
  {
    int ret = OB_SUCCESS;
    int64_t left = left_idx;
    int64_t right = right_bound;
    while (OB_SUCC(ret) && left < right) {
      int64_t mid = (left + right) >> 1;
      int cmp = 0;
      ObDatum datum;
      if (OB_FAIL(get_datum(expr, eval_ctx, mid, datum))) {
        SQL_DAS_LOG(WARN, "failed to get datum", K(ret));
      } else if (OB_FAIL(expr.basic_funcs_->null_first_cmp_(datum, target_datum, cmp))) {
        SQL_DAS_LOG(WARN, "failed to compare datums", K(ret));
      } else if (cmp <= 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    if (OB_SUCC(ret)) {
      result = left;
    }
    return ret;
  }

  static int get_datum(const ObExpr &expr,
                       ObEvalCtx &eval_ctx,
                       int64_t idx,
                       ObDatum &datum)
  {
    int ret = OB_SUCCESS;
    ObIVector *vec = expr.get_vector(eval_ctx);
    if (OB_ISNULL(vec)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "unexpected nullptr vector", K(ret));
    } else {
      const VectorFormat format = vec->get_format();
      switch (format) {
        case VEC_FIXED: {
          if (OB_ISNULL(expr.get_rev_buf(eval_ctx)) || OB_UNLIKELY(expr.res_buf_len_ <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr rev buf or res buf len is 0", K(ret), K(expr.res_buf_len_));
          } else {
            FixedFormatAccessor accessor(expr.get_rev_buf(eval_ctx), expr.res_buf_len_);
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        case VEC_CONTINUOUS: {
          if (OB_ISNULL(expr.get_continuous_vector_data(eval_ctx)) ||
              OB_ISNULL(expr.get_continuous_vector_offsets(eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr continuous vector data or continuous vector offsets", K(ret));
          } else {
            ContinuousFormatAccessor accessor(expr.get_continuous_vector_data(eval_ctx),
                                              expr.get_continuous_vector_offsets(eval_ctx));
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        case VEC_DISCRETE: {
          if (OB_ISNULL(expr.get_discrete_vector_ptrs(eval_ctx)) || OB_ISNULL(expr.get_discrete_vector_lens(eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr discrete vector ptrs or discrete vector lens", K(ret));
          } else {
            DiscreteFormatAccessor accessor(expr.get_discrete_vector_ptrs(eval_ctx),
                                            expr.get_discrete_vector_lens(eval_ctx));
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        case VEC_UNIFORM: {
          if (OB_ISNULL(expr.locate_batch_datums(eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr batch datums", K(ret));
          } else {
            UniformFormatAccessor accessor(expr.locate_batch_datums(eval_ctx));
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        case VEC_UNIFORM_CONST: {
          if (OB_ISNULL(expr.locate_batch_datums(eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_DAS_LOG(WARN, "unexpected nullptr batch datums", K(ret));
          } else {
            UniformConstFormatAccessor accessor(expr.locate_batch_datums(eval_ctx));
            datum = accessor(idx, vec->is_null(idx));
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SQL_DAS_LOG(WARN, "unexpected vector format", KR(ret), K(format));
          break;
      }
    }
    return ret;
  }

  static int get_uint64_at(const ObExpr &expr, ObEvalCtx &eval_ctx, const int64_t idx, uint64_t &val)
  {
    int ret = OB_SUCCESS;
    ObDatum datum;
    if (OB_FAIL(get_datum(expr, eval_ctx, idx, datum))) {
      SQL_DAS_LOG(WARN, "failed to get datum for uint64", K(ret), K(idx));
    } else if (datum.is_null()) {
      ret = OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "unexpected null datum for uint64", K(ret), K(idx));
    } else {
      val = datum.get_uint64();
    }
    return ret;
  }
};

} // namespace sql
} // namespace oceanbase

#endif // DEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_UTILS_H_
