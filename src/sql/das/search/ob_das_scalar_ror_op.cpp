/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_scalar_ror_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{

int ObDASScalarROROp::fetch_and_process_storage_batch_(int64_t &storage_count)
{
  int ret = OB_SUCCESS;
  bool done = false;
  storage_count = 0;
  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null scan result", K(ret));
  }
  while (OB_SUCC(ret) && !done) {
    storage_count = 0;
    if (OB_FAIL(result_->get_next_rows(storage_count, max_batch_size()))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rows", K(ret));
      } else if (storage_count > 0) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      if (storage_count <= 0) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(ObDASScalarScanOp::process_logical_table_storage_batch(
                     search_ctx_, scalar_ctdef_, scalar_rtdef_,
                     storage_count))) {
        LOG_WARN("[LT hybrid scalar ROR] process storage batch failed", K(ret), K(storage_count));
      } else if (storage_count > 0) {
        // Got a non-empty batch after logical-table post-processing.
        done = true;
      }
      // Current batch was fully filtered; continue fetching the next storage batch.
    }
  }
  return ret;
}

int ObDASScalarROROp::do_open()
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObExpr *> *producer_exprs = nullptr;
  tsc_service_ = MTL(storage::ObAccessService *);
  if (OB_ISNULL(tsc_service_) || OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(scalar_ctdef_), K(scalar_rtdef_), K(tsc_service_));
  } else if (FALSE_IT(producer_exprs = scalar_ctdef_->is_logical_table()
      ? &get_rowid_exprs() : &scalar_ctdef_->rowkey_exprs_)) {
  } else if (OB_FAIL(rowid_output_exprs_.init(
                 *producer_exprs, get_rowid_exprs().count()))) {
    LOG_WARN("failed to initialize scalar ROR RowID output exprs", K(ret),
        K(producer_exprs->count()), "scope_expr_count", get_rowid_exprs().count(),
        K_(scope), "rowid_type", get_rowid_type());
  } else if (OB_FAIL(ObDASScalarScanOp::resolve_scalar_tablet_id(
                 *this, search_ctx_, scalar_ctdef_, tablet_id_))) {
    LOG_WARN("failed to resolve tablet id", K(ret));
  } else if (is_probe_mode()) {
    // Keep common initialization, but leave storage access to the probe iterator.
  } else if (OB_FAIL(search_ctx_.init_scan_param(tablet_id_, scalar_ctdef_, scalar_rtdef_, scan_param_,
                                                true /* use_async_read_snapshot */))) {
    LOG_WARN("failed to init scan param", K(ret), K(tablet_id_));
  } else if (FALSE_IT(scan_param_.is_get_ = false)) {
  } else if (OB_FAIL(prepare_scan_ranges(scalar_rtdef_))) {
    LOG_WARN("failed to prepare scan ranges", K(ret));
  } else if (OB_FAIL(narrow_scan_ranges_by_docid_range())) {
    LOG_WARN("failed to narrow scan ranges by docid range", K(ret));
  } else if (OB_FAIL(tsc_service_->table_scan(scan_param_, result_))) {
    LOG_WARN("failed to do table scan", K(scan_param_));
  } else if (OB_FAIL(create_rowid_store(max_batch_size(), rowid_store_))) {
    LOG_WARN("failed to create rowid store", K(ret), K(max_batch_size()));
  } else if (OB_ISNULL(rowid_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store", K(ret));
  } else {
    rowid_store_iter_.init(rowid_store_);
    SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
    LOG_TRACE("[hybrid search] open scalar ror op", K(ret), K(tablet_id_), K(scan_param_));
  }
  // Sync ns/lt into scalar_rtdef_->lt_rt_ so R2.6 pre-RPC gate and R3 new-signature
  // project_logical_columns can consume it directly from the hybrid-search ctx.
  if (OB_SUCC(ret) && OB_NOT_NULL(scalar_ctdef_) && scalar_ctdef_->is_logical_table()
      && OB_NOT_NULL(scalar_rtdef_)) {
    const ObLogicalTableSearchCtx &hs_lt_ctx = search_ctx_.get_logical_table_ctx();
    if (hs_lt_ctx.is_enabled_) {
      scalar_rtdef_->lt_rt_.namespace_id_     = hs_lt_ctx.namespace_id_;
      scalar_rtdef_->lt_rt_.logical_table_id_ = hs_lt_ctx.logical_table_id_;
      scalar_rtdef_->lt_rt_.enabled_          = true;
      // Keep the logical scan state alive until rtdef destruction, across DAS reuse.
      if (OB_ISNULL(scalar_rtdef_->eval_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("logical scalar ROR eval context is null", K(ret));
      } else if (OB_FAIL(ObDASUtils::ensure_lt_scan_rt_allocated(
              scalar_rtdef_->eval_ctx_->exec_ctx_.get_allocator(), scalar_rtdef_->lt_scan_rt_))) {
        LOG_WARN("[LT hybrid scalar ROR] allocate lt_scan_rt failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDASScalarROROp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_) || OB_ISNULL(tsc_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(scalar_ctdef_), K(scalar_rtdef_), K(tsc_service_));
  } else if (OB_FAIL(get_related_tablet_id(scalar_ctdef_, tablet_id_))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  } else if (FALSE_IT(ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), tablet_id_, scan_param_))) {
  } else if (search_ctx_.is_async_index_query()
             && OB_FAIL(search_ctx_.apply_async_index_read_snapshot(scan_param_))) {
    LOG_WARN("failed to apply async index read snapshot", K(ret), K(tablet_id_));
  } else if (OB_FAIL(tsc_service_->reuse_scan_iter(scan_param_.need_switch_param_, result_))) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  } else if (OB_FAIL(prepare_scan_ranges(scalar_rtdef_))) {
    LOG_WARN("failed to prepare scan ranges", K(ret));
  } else if (OB_FAIL(narrow_scan_ranges_by_docid_range())) {
    LOG_WARN("failed to narrow scan ranges by docid range", K(ret));
  } else if (OB_FAIL(tsc_service_->table_rescan(scan_param_, result_))) {
    LOG_WARN("failed to rescan table", K(ret), K(scan_param_));
  } else {
    if (OB_NOT_NULL(rowid_store_)) {
      rowid_store_->reuse();
      rowid_store_iter_.reset();
      rowid_store_iter_.init(rowid_store_);
    }
    scan_param_.need_switch_param_ = false;
    SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
  }
  return ret;
}

int ObDASScalarROROp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_) && OB_NOT_NULL(tsc_service_)) {
    if (OB_FAIL(tsc_service_->revert_scan_iter(result_))) {
      LOG_WARN("failed to revert scan iter", K(ret));
    }
    result_ = nullptr;
  }
  scan_param_.destroy_schema_guard();
  scan_param_.snapshot_.reset();
  scan_param_.destroy();
  if (OB_NOT_NULL(rowid_store_)) {
    rowid_store_->reset();
    rowid_store_iter_.reset();
    rowid_store_ = nullptr;
  }
  return ret;
}

int ObDASScalarROROp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t idx = -1;
  score = 0.0;

  if(OB_ISNULL(rowid_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store", K(ret));
  } else {
    ret = rowid_store_iter_.lower_bound(target);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rowid_store_iter_.get_cur_rowid(curr_id))) {
      LOG_WARN("failed to get rowid", K(ret));
    } else {
      score = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
    }
  } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("falied to find lower bound in store", K(ret));
  }
  // else if (OB_FAIL(advance_skip_scan(target))) {
  //   LOG_WARN("failed to advance skip scan", K(ret));
  // }
  else {
    bool reached = false;
    int64_t storage_count = 0;
    const common::ObIArray<ObExpr *> &rowid_exprs = *get_rowid_output_exprs();
    ret = OB_SUCCESS;
    while (!reached && OB_SUCC(ret)) {
      if (OB_FAIL(fetch_and_process_storage_batch_(storage_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fetch storage batch", K(ret));
        }
        break;
      } else if (OB_UNLIKELY(storage_count <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fetch_and_process_storage_batch returned success with empty batch", K(ret));
        break;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(search_ctx_.lower_bound_in_frame(scope_, target, &rowid_exprs, storage_count, idx))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("falied to find lower bound", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        rowid_store_->reuse();
        if (OB_FAIL(rowid_store_->fill(idx, storage_count, rowid_exprs))) {
          LOG_WARN("failed to fill rowid store", K(ret));
        } else if (FALSE_IT(rowid_store_iter_.reuse())) {
        } else if (OB_FAIL(rowid_store_iter_.get_cur_rowid(curr_id))) {
          LOG_WARN("failed to get rowid", K(ret));
        } else {
          reached = true;
          score = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
        }
      }
    }
  }

  return ret;
}

int ObDASScalarROROp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (OB_ISNULL(rowid_store_) || OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store or result", K(ret));
  } else {
    rowid_store_iter_.next_idx();
  }

  if (OB_FAIL(ret)) {
  } else if (!rowid_store_iter_.is_empty()) {
    if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
      LOG_WARN("failed to get next rowid", K(ret));
    } else {
      score = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
    }
  } else {
    bool got_rowid = false;
    const common::ObIArray<ObExpr *> &rowid_exprs = *get_rowid_output_exprs();
    while (OB_SUCC(ret) && !got_rowid) {
      rowid_store_->reuse();
      int64_t storage_count = 0;
      if (OB_FAIL(fetch_and_process_storage_batch_(storage_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to fetch storage batch", K(ret));
        }
        break;
      } else if (OB_UNLIKELY(storage_count <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fetch_and_process_storage_batch returned success with empty batch", K(ret));
        break;
      } else if (OB_FAIL(rowid_store_->fill(0, storage_count, rowid_exprs))) {
        LOG_WARN("failed to fill rowid store", K(ret));
      } else if (FALSE_IT(rowid_store_iter_.reuse())) {
      } else if (OB_FAIL(rowid_store_iter_.get_cur_rowid(next_id))) {
        LOG_WARN("failed to get next rowid", K(ret));
      } else {
        got_rowid = true;
        score = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
      }
    }
    if (OB_SUCC(ret) && !got_rowid) {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObDASScalarROROp::do_advance_shallow(const ObDASRowID &target, const bool inclusive, const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  const double boost_weight = scalar_rtdef_->get_boost_weight();
  if (OB_UNLIKELY(!target.is_normal())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target rowid", K(ret), K(target));
  } else if (inclusive) {
    max_score_tuple_.set(target, target, is_scoring_ ? boost_weight : 0.0);
    max_score_tuple = &max_score_tuple_;
  } else if (OB_FAIL(ObIDASSearchOp::do_advance_shallow(target, inclusive, max_score_tuple))) {
    LOG_WARN("failed to advance shallow", K(ret), K(target));
  }
  return ret;
}

int ObDASScalarROROp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  threshold = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
  return ret;
}

int ObDASScalarROROp::prepare_scan_ranges(const ObDASScalarScanRtDef *rtdef)
{
  int ret = OB_SUCCESS;
  const ObLogicalTableSearchCtx &lt_ctx = search_ctx_.get_logical_table_ctx();
  if (OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan rtdef", K(ret));
  } else if (lt_ctx.is_enabled_ && OB_NOT_NULL(scalar_ctdef_) && scalar_ctdef_->is_logical_table()) {
    if (OB_FAIL(ObDASScalarScanOp::prepare_logical_table_kv_scan_ranges(
                    lt_ctx, scalar_ctdef_, rtdef->key_ranges_,
                    *scan_param_.allocator_, scan_param_.key_ranges_))) {
      LOG_WARN("convert ranges for logical table failed", K(ret));
    } else {
      scan_param_.index_id_ = lt_ctx.physical_data_table_id_;
      scan_param_.tablet_id_ = lt_ctx.physical_tablet_id_;
    }
  } else if (OB_FAIL(scan_param_.key_ranges_.assign(rtdef->key_ranges_))) {
    LOG_WARN("failed to assign key ranges", K(ret));
  }
  return ret;
}

int ObDASScalarROROp::narrow_scan_ranges_by_docid_range()
{
  int ret = OB_SUCCESS;
  if (!search_ctx_.has_docid_range() || OB_ISNULL(scalar_ctdef_)) {
    // no narrowing needed
  } else {
    const common::ObObj &lo = search_ctx_.get_docid_range_lo();
    const common::ObObj &hi = search_ctx_.get_docid_range_hi();
    // The logical output is one uint64 doc_id. A nested physical rowkey may
    // carry trailing nested_key, so locate doc_id from the full producer shape
    // instead of assuming it is the last physical key column.
    const int64_t output_expr_cnt = get_rowid_output_exprs()->count();
    const int64_t producer_rowid_cnt = scalar_ctdef_->is_logical_table()
        ? output_expr_cnt : scalar_ctdef_->rowkey_exprs_.count();
    if (OB_UNLIKELY(get_rowid_type() != DAS_ROWID_TYPE_UINT64)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("docid range parallel requires uint64 rowid type",
          KR(ret), "rowid_type", get_rowid_type(), K_(scope));
    } else if (OB_UNLIKELY(output_expr_cnt != 1 || producer_rowid_cnt < output_expr_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("docid range narrowing has invalid RowID output projection",
          KR(ret), K(output_expr_cnt), K(producer_rowid_cnt));
    }
    ObRangeArray &key_ranges = scan_param_.key_ranges_;

    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
      ObNewRange &range = key_ranges.at(i);
      const int64_t obj_cnt = range.start_key_.get_obj_cnt();
      const int64_t doc_id_idx = obj_cnt - producer_rowid_cnt;
      if (OB_UNLIKELY(obj_cnt < 1 || doc_id_idx < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected key range obj count for docid range narrowing",
            KR(ret), K(i), K(obj_cnt), K(producer_rowid_cnt), K(range));
      } else {
        ObObj *start_objs = range.start_key_.get_obj_ptr();
        ObObj *end_objs = range.end_key_.get_obj_ptr();
        if (OB_ISNULL(start_objs) || OB_ISNULL(end_objs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null key range obj ptr for docid range narrowing",
              KR(ret), K(i), KP(start_objs), KP(end_objs), K(range));
        } else {
          // Narrow start key: tighten docid column to docid_range_lo
          if (!lo.is_min_value()) {
            ObObj &start_pk = start_objs[doc_id_idx];
            if (start_pk.is_min_value()
                || (!start_pk.is_max_value()
                    && start_pk.get_uint64() < lo.get_uint64())) {
              start_pk = lo;
              range.border_flag_.set_inclusive_start();
            }
          }

          // Narrow end key: tighten docid column to docid_range_hi
          if (!hi.is_max_value()) {
            ObObj &end_pk = end_objs[doc_id_idx];
            if (end_pk.is_max_value()
                || (!end_pk.is_min_value()
                    && end_pk.get_uint64() > hi.get_uint64())) {
              end_pk = hi;
              range.border_flag_.set_inclusive_end();
            }
          }
        }
      }
    }

    LOG_TRACE("[FUSION_TRACE] scalar ror narrow scan ranges by docid range",
        K(lo), K(hi), K(key_ranges));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
