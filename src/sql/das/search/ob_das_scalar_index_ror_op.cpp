/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_scalar_index_ror_op.h"
#include "sql/das/ob_das_def_reg.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/das/search/ob_das_search_context.h"

namespace oceanbase
{
namespace sql
{

int ObDASScalarIndexROROpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  // leaf node, no children
  return OB_SUCCESS;
}

int ObDASScalarIndexROROp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASScalarIndexROROpParam &scalar_op_param = static_cast<const ObDASScalarIndexROROpParam &>(op_param);
  if (OB_ISNULL(scalar_op_param.get_scan_ctdef()) || OB_ISNULL(scalar_op_param.get_scan_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    scalar_ctdef_ = scalar_op_param.get_scan_ctdef();
    scalar_rtdef_ = scalar_op_param.get_scan_rtdef();
  }
  return ret;
}

int ObDASScalarIndexROROp::advance_skip_scan(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  ObRangeArray &key_ranges = scan_param_.key_ranges_;
  const ObIArray<ObExpr *> &producer_rowkeys = scalar_ctdef_->rowkey_exprs_;
  const ObIArray<ObExpr *> &rowid_output_exprs = *get_rowid_output_exprs();
  if (OB_UNLIKELY(key_ranges.count() != 1)) {
    // do nothing
  } else if (OB_UNLIKELY(
      key_ranges.at(0).start_key_.get_obj_cnt() <= producer_rowkeys.count()
      || rowid_output_exprs.count() > producer_rowkeys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected RowID projection for scalar index range", K(ret),
        K(producer_rowkeys), K(rowid_output_exprs), K(key_ranges));
  } else {
    ObObj *start_rowkey_objs = key_ranges.at(0).start_key_.get_obj_ptr();
    const int64_t rowkey_offset =
        key_ranges.at(0).start_key_.get_obj_cnt() - producer_rowkeys.count();
    for (int64_t i = 0 ; i < rowid_output_exprs.count() ; i++) {
      const ObExpr *rowkey_expr = rowid_output_exprs.at(i);
      if (OB_ISNULL(rowkey_expr) || OB_ISNULL(start_rowkey_objs + rowkey_offset + i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr rowkey expr", K(ret));
      } else {
        ObDatum target_datum;
        if (OB_FAIL(get_datum_from_rowid(target, target_datum, i))) {
          LOG_WARN("failed to get datum from target rowid", K(ret), K(i));
        } else if (OB_FAIL(target_datum.to_obj(start_rowkey_objs[rowkey_offset + i], rowkey_expr->obj_meta_))) {
          LOG_WARN("failed to convert target datum to obj", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      key_ranges.at(0).border_flag_.set_inclusive_start();

      if (OB_ISNULL(tsc_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr tsc service", K(ret));
      } else if (OB_FAIL(tsc_service_->table_advance_scan(scan_param_, result_))) {
        LOG_WARN("failed to advance scan", K(ret));
      } else {
        LOG_TRACE("advance skip scan", K(key_ranges.at(0)));
      }
    }
  }
  return ret;
}

int ObDASScalarIndexROROp::do_drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  bool drained = false;
  count = 0;
  next_score_after = 0.0;
  const bool is_supported_boundary = boundary.is_normal() || boundary.is_max();
  if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != get_rowid_type()
                  || !is_supported_boundary)) {
    ret = ObIDASSearchOp::do_drain_until(
        boundary, capacity, out_ids, out_scores, count, next_id_after, next_score_after);
  } else if (OB_ISNULL(rowid_store_) || OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null rowid store or result", K(ret));
  } else {
    const common::ObIArray<ObExpr *> &rowid_exprs = *get_rowid_output_exprs();
    // drain_until() starts after the row cached by the upper merge iterator.
    rowid_store_iter_.next_idx();
    while (OB_SUCC(ret) && count < capacity && !drained) {
      if (rowid_store_iter_.is_empty()) {
        rowid_store_->reuse();
        int64_t storage_count = 0;
        if (OB_FAIL(fetch_and_process_storage_batch_(storage_count))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            next_id_after.set_max();
            drained = true;
          } else {
            LOG_WARN("failed to fetch storage batch", K(ret));
          }
        } else if (OB_UNLIKELY(storage_count <= 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fetch_and_process_storage_batch returned success with empty batch", K(ret));
        } else if (OB_FAIL(rowid_store_->fill(0, storage_count, rowid_exprs))) {
          LOG_WARN("failed to fill rowid store", K(ret));
        } else {
          rowid_store_iter_.reuse();
        }
      }

      if (OB_SUCC(ret) && !drained && !rowid_store_iter_.is_empty()
          && OB_FAIL(rowid_store_iter_.drain_until(
              boundary, capacity, out_ids, out_scores,
              count, next_id_after, next_score_after, drained))) {
        LOG_WARN("failed to drain rowid store", K(ret), K(boundary), K(capacity), K(count));
      }
    }
    if (OB_SUCC(ret)) {
      // RowIDStore carries no scores; restore the scalar contribution for this batch.
      const double score = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
      if (OB_NOT_NULL(out_scores)) {
        for (int64_t i = 0; i < count; ++i) {
          out_scores[i] = score;
        }
      }
      if (count < capacity && next_id_after.is_normal()) {
        next_score_after = score;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
