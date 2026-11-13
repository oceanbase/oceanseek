/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_utils.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"
#include "share/logical_table/ob_logical_table_range_builder.h"
#include "share/logical_table/ob_logical_table_decoder.h"
#include "share/vector/type_traits.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{

int ObDASRowIDOutputExprs::init(
    const common::ObIArray<ObExpr *> &producer_exprs,
    const int64_t output_expr_count)
{
  int ret = OB_SUCCESS;
  const int64_t producer_expr_count = producer_exprs.count();
  exprs_.reset();
  if (OB_UNLIKELY(output_expr_count <= 0
                  || producer_expr_count < output_expr_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid RowID output expression projection", K(ret),
        K(producer_expr_count), K(output_expr_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_expr_count; ++i) {
    if (OB_ISNULL(producer_exprs.at(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("RowID output expression is null", K(ret), K(i),
          K(producer_expr_count), K(output_expr_count));
    } else if (OB_FAIL(exprs_.push_back(producer_exprs.at(i)))) {
      LOG_WARN("failed to append RowID output expression", K(ret), K(i),
          K(producer_expr_count), K(output_expr_count));
    }
  }
  return ret;
}

int ObDASScalarScanOpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  // leaf node, no children
  return OB_SUCCESS;
}

int ObDASScalarScanOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanOpParam &scalar_op_param = static_cast<const ObDASScalarScanOpParam &>(op_param);
  if (OB_ISNULL(scalar_op_param.get_scan_ctdef()) || OB_ISNULL(scalar_op_param.get_scan_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    scalar_ctdef_ = scalar_op_param.get_scan_ctdef();
    scalar_rtdef_ = scalar_op_param.get_scan_rtdef();
    const ObDASRowIDScope &rowid_scope = search_ctx_.get_scope(scope_);
    const common::ObIArray<ObExpr *> *producer_exprs = &scalar_ctdef_->rowkey_exprs_;
    if (scalar_ctdef_->is_logical_table()) {
      producer_exprs = rowid_scope.rowid_exprs_;
    }
    if (OB_ISNULL(producer_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scalar scan RowID producer exprs are null", K(ret), K_(scope));
    } else if (OB_FAIL(init_rowid_output_exprs(*producer_exprs))) {
      LOG_WARN("failed to initialize scalar scan RowID output exprs", K(ret),
          "producer_expr_count", producer_exprs->count(),
          "scope_expr_count", OB_ISNULL(rowid_scope.rowid_exprs_)
              ? -1 : rowid_scope.rowid_exprs_->count(),
          K_(scope), "rowid_type", get_rowid_type());
    }
  }
  return ret;
}

int ObDASScalarScanOp::init_rowid_output_exprs(
    const common::ObIArray<ObExpr *> &producer_exprs)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObExpr *> *scope_exprs =
      search_ctx_.get_scope(scope_).rowid_exprs_;
  if (OB_ISNULL(scope_exprs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar RowID scope exprs are null", K(ret), K_(scope),
        "rowid_type", get_rowid_type());
  } else {
    const int64_t output_expr_count = scope_exprs->count();
    if (OB_UNLIKELY((DAS_ROWID_TYPE_UINT64 == get_rowid_type()
                     && (is_async_nested_scope(scope_) ? 2 : 1) != output_expr_count)
                    || output_expr_count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected scalar RowID output shape", K(ret), K_(scope),
          "rowid_type", get_rowid_type(), K(output_expr_count));
    } else if (OB_FAIL(rowid_output_exprs_.init(producer_exprs, output_expr_count))) {
      LOG_WARN("failed to project scalar RowID output exprs", K(ret),
          K(producer_exprs.count()), K(output_expr_count), K_(scope));
    }
  }
  return ret;
}

int ObDASScalarScanOp::do_open()
{
  int ret = OB_SUCCESS;
  tsc_service_ = MTL(storage::ObAccessService *);
  if (OB_ISNULL(tsc_service_) || OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(scalar_ctdef_), K(scalar_rtdef_), K(tsc_service_));
  } else if (OB_FAIL(resolve_scalar_tablet_id(*this, search_ctx_, scalar_ctdef_, tablet_id_))) {
    LOG_WARN("failed to resolve tablet id", K(ret));
  }
  if (OB_FAIL(ret)) {
    // tablet resolution failed above, do not proceed
  } else if (OB_FAIL(search_ctx_.init_scan_param(tablet_id_, scalar_ctdef_, scalar_rtdef_, scan_param_,
                                                true /* use_async_read_snapshot */))) {
    LOG_WARN("failed to init scan param", K(ret), K(tablet_id_));
  } else if (OB_FAIL(prepare_scan_ranges(scalar_rtdef_))) {
    LOG_WARN("failed to prepare scan ranges", K(ret));
  } else if (OB_FAIL(tsc_service_->table_scan(scan_param_, result_))) {
    LOG_WARN("failed to do table scan", K(scan_param_));
  }
  SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
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
        LOG_WARN("logical scalar scan eval context is null", K(ret));
      } else if (OB_FAIL(ObDASUtils::ensure_lt_scan_rt_allocated(
              scalar_rtdef_->eval_ctx_->exec_ctx_.get_allocator(), scalar_rtdef_->lt_scan_rt_))) {
        LOG_WARN("[LT hybrid scalar] allocate lt_scan_rt failed", K(ret));
      }
    }
  }
  LOG_TRACE("[hybrid search] open scalar scan op", K(ret), K(tablet_id_), K(scan_param_));
  return ret;
}

int ObDASScalarScanOp::do_rescan()
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
  } else if (OB_FAIL(tsc_service_->table_rescan(scan_param_, result_))) {
    LOG_WARN("failed to rescan table", K(ret), K(scan_param_));
  } else {
    scan_param_.need_switch_param_ = false;
    SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
  }
  return ret;
}

int ObDASScalarScanOp::do_close()
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
  return ret;
}

int ObDASScalarScanOp::prepare_scan_ranges(const ObDASScalarScanRtDef *rtdef)
{
  int ret = OB_SUCCESS;
  const ObLogicalTableSearchCtx &lt_ctx = search_ctx_.get_logical_table_ctx();
  if (OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan rtdef", K(ret));
  } else if (lt_ctx.is_enabled_ && OB_NOT_NULL(scalar_ctdef_) && scalar_ctdef_->is_logical_table()) {
    // logical table path: range rewrite
    if (OB_FAIL(prepare_logical_table_kv_scan_ranges(lt_ctx, scalar_ctdef_,
                                                    rtdef->key_ranges_,
                                                    *scan_param_.allocator_,
                                                    scan_param_.key_ranges_))) {
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

int ObDASScalarScanOp::prepare_logical_table_kv_scan_ranges(
    const ObLogicalTableSearchCtx &lt_ctx,
    const ObDASScalarScanCtDef *scalar_ctdef,
    const common::ObIArray<common::ObNewRange> &logical_ranges,
    common::ObIAllocator &alloc,
    common::ObIArray<common::ObNewRange> &physical_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (common::OB_INVALID_ID == scalar_ctdef->logical_index_seq_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical_index_seq_ is invalid for logical table kv range rewrite",
             K(ret), K(scalar_ctdef->ref_table_id_));
  } else {
    share::ObLogicalTableKeyPrefix prefix(
        lt_ctx.namespace_id_,
        lt_ctx.logical_table_id_,
        scalar_ctdef->logical_index_seq_,
        scalar_ctdef->logical_table_aux_type_);
    // Reset output so convert_ranges does not append onto leftover physical ranges
    // from a previous prepare/rescan. Stale logical ranges mismatch KV schema.
    physical_ranges.reset();
    if (OB_FAIL(share::ObLogicalTableRangeBuilder::convert_ranges(
            lt_ctx.physical_data_table_id_, prefix, logical_ranges, alloc, physical_ranges))) {
      LOG_WARN("convert ranges failed", K(ret), K(prefix));
    }
  }
  return ret;
}

int ObDASScalarScanOp::do_next_rowids(const int64_t max_batch_size, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("result is not initialized", K(ret));
  } else if (OB_FAIL(result_->get_next_rows(count, max_batch_size))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next rows", K(ret));
    } else if (count > 0) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && count > 0) {
    if (OB_FAIL(process_logical_table_storage_batch(
            search_ctx_, scalar_ctdef_, scalar_rtdef_, count))) {
      LOG_WARN("[LT hybrid scalar] process logical table storage batch failed", K(ret), K(count));
    } else if (count <= 0) {
      // logical_residual_filters compacted the whole batch; pull the next storage
      // batch(es) instead of signaling no match, so later batches are not lost.
      ret = repull_logical_table_batches_(count, max_batch_size);
    }
  }
  INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
  return ret;
}

// logical_residual_filters may compact an entire storage batch to zero rows;
// keep pulling the next storage batch (mirrors ObDASScalarROROp) so callers
// never observe (0 rows, OB_SUCCESS) and later batches are not silently lost.
int ObDASScalarScanOp::repull_logical_table_batches_(int64_t &count, const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  const bool may_compact_to_empty = OB_NOT_NULL(scalar_ctdef_)
      && scalar_ctdef_->is_logical_table()
      && !scalar_ctdef_->logical_residual_filters_.empty();
  bool pull_next = may_compact_to_empty;
  while (pull_next && OB_SUCC(ret)) {
    pull_next = false;
    count = 0;
    if (OB_FAIL(result_->get_next_rows(count, max_batch_size))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next rows", K(ret));
      } else if (count > 0) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret) && count > 0) {
      if (OB_FAIL(process_logical_table_storage_batch(
              search_ctx_, scalar_ctdef_, scalar_rtdef_, count))) {
        LOG_WARN("[LT hybrid scalar] process logical table storage batch failed", K(ret), K(count));
      } else if (count <= 0) {
        pull_next = true;  // batch compacted again; keep pulling
      }
    }
  }
  if (OB_SUCC(ret) && count <= 0) {
    ret = OB_ITER_END;  // storage exhausted (or non-LT no-match); signal end
  }
  return ret;
}

int ObDASScalarScanOp::fill_logical_rowid_exprs_(int64_t count)
{
  return fill_logical_rowid_exprs(search_ctx_, scalar_ctdef_, count);
}

int ObDASScalarScanOp::resolve_scalar_tablet_id(ObIDASSearchOp &search_op,
                                              ObDASSearchCtx &search_ctx,
                                              const ObDASScalarScanCtDef *scalar_ctdef,
                                              common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (scalar_ctdef->is_logical_table() && search_ctx.get_logical_table_ctx().is_enabled_) {
    tablet_id = search_ctx.get_logical_table_ctx().physical_tablet_id_;
    if (!tablet_id.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid scalar] physical_tablet_id_ invalid", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(search_op.get_related_tablet_id(scalar_ctdef, tablet_id))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  }
  return ret;
}

int ObDASScalarScanOp::process_logical_table_storage_batch(
    ObDASSearchCtx &search_ctx,
    const ObDASScalarScanCtDef *scalar_ctdef,
    ObDASScalarScanRtDef *scalar_rtdef,
    int64_t &storage_count)
{
  int ret = OB_SUCCESS;
  if (storage_count <= 0 || OB_ISNULL(scalar_ctdef)) {
  } else if (!scalar_ctdef->is_logical_table()) {
    // Physical table: scan batch already populated rowkey_exprs_; no post-processing needed.
  } else if (scalar_ctdef->logical_residual_filters_.empty()
             || OB_ISNULL(search_ctx.get_eval_ctx())) {
    if (OB_FAIL(fill_logical_rowid_exprs(search_ctx, scalar_ctdef, storage_count))) {
      LOG_WARN("fill logical rowid exprs failed", K(ret), K(storage_count));
    }
  } else {
    ObEvalCtx &eval_ctx = *search_ctx.get_eval_ctx();
    // 1) Decode logical columns (may set __pk_increment null via mapping).
    if (OB_SUCC(ret)
        && !scalar_ctdef->logical_output_exprs_.empty()
        && OB_NOT_NULL(scalar_rtdef)
        && scalar_rtdef->lt_rt_.enabled_
        && OB_NOT_NULL(scalar_rtdef->lt_scan_rt_)) {
      ExprFixedArray &outs =
          const_cast<ExprFixedArray &>(scalar_ctdef->logical_output_exprs_);
      // Build a per-call lt_ctdef by copying scalar_ctdef->lt_ctx_; kv exprs come directly
      // from ctdef's access_exprs_ (CG guarantees [kv_key, kv_value]; no override needed).
      // ivf_vec_project_dim_ already present in scalar_ctdef->lt_ctx_.
      ObLogicalTableScanCtDef lt_ctdef_for_proj = scalar_ctdef->lt_ctx_;
      const ExprFixedArray &acs = scalar_ctdef->pd_expr_spec_.access_exprs_;
      if (acs.count() >= 2) {
        if (OB_NOT_NULL(acs.at(0))) {
          lt_ctdef_for_proj.kv_key_expr_ = acs.at(0);
        }
        if (OB_NOT_NULL(acs.at(1))) {
          lt_ctdef_for_proj.kv_value_expr_ = acs.at(1);
        }
      }
      if (OB_FAIL(ObLogicalTableHelper::project_logical_columns(
              lt_ctdef_for_proj,
              scalar_rtdef->lt_rt_,
              scalar_ctdef->logical_column_mapping_,
              ObTSCIRScanType::OB_NOT_A_SPEC_SCAN,
              scalar_ctdef->logical_table_aux_type_,
              outs, storage_count, eval_ctx,
              scalar_rtdef->lt_scan_rt_))) {
        LOG_WARN("[LT hybrid scalar] project logical columns failed", K(ret), K(storage_count));
      }
    }
    // 2) Filter + compact access/output datums only (not rowid_exprs: compact copies ObDatum
    //    but fill_logical_rowid writes ObIVector; rowid_exprs[0] may alias __pk_increment).
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObLogicalTableHelper::eval_logical_residual_filters(
              *scalar_ctdef, storage_count, eval_ctx, nullptr))) {
        LOG_WARN("[LT hybrid scalar] eval logical residual filters failed", K(ret), K(storage_count));
      }
    }
    // 3) Fill rowid from compacted kv_key batch [0, storage_count).
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fill_logical_rowid_exprs(search_ctx, scalar_ctdef, storage_count))) {
        LOG_WARN("fill logical rowid exprs failed", K(ret), K(storage_count));
      }
    }
  }
  return ret;
}

int ObDASScalarScanOp::fill_logical_rowid_exprs(ObDASSearchCtx &search_ctx,
                                                const ObDASScalarScanCtDef *scalar_ctdef,
                                                int64_t count)
{
  int ret = OB_SUCCESS;
  // After CG refactor, access_exprs_[0/1] is invariantly [phys_key, phys_value] for
  // any logical-table scalar/scan ctdef. Pull phys_key directly from the leaf
  // ctdef's access_exprs_ rather than caching it on the runtime ctx.
  // After eval+compact: decode rowid from compacted phys_key in access_exprs_[0].
  // Do not pass rowid_exprs into compact_logical_batch_exprs (datum copy vs IVector::set_uint).
  ObExpr *key_expr = nullptr;
  if (OB_NOT_NULL(scalar_ctdef) && scalar_ctdef->is_logical_table()
      && scalar_ctdef->pd_expr_spec_.access_exprs_.count() >= 1) {
    key_expr = scalar_ctdef->pd_expr_spec_.access_exprs_.at(0);
  }
  if (OB_ISNULL(key_expr)) {
    // non-logical table or no key expr, skip
  } else {
    const common::ObIArray<ObExpr *> &rowid_exprs = search_ctx.get_rowid_exprs();
    if (OB_UNLIKELY(rowid_exprs.count() < 1) || OB_ISNULL(rowid_exprs.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowid exprs empty or null", K(ret), K(rowid_exprs.count()));
    } else {
      ObExpr *rowid_expr = rowid_exprs.at(0);
      ObEvalCtx *eval_ctx = search_ctx.get_eval_ctx();
      if (OB_ISNULL(eval_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("eval_ctx is null", K(ret));
      } else if (OB_FAIL(rowid_expr->init_vector_default(*eval_ctx, count))) {
        LOG_WARN("init rowid vector failed", K(ret), K(count));
      } else {
        ObIVector *rowid_vec = rowid_expr->get_vector(*eval_ctx);
        if (OB_ISNULL(rowid_vec)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rowid vector is null", K(ret));
        } else {
          ObIAllocator &alloc = eval_ctx->exec_ctx_.get_allocator();
          // Hoist per-expr format dispatch out of the row loop: kv_key is VARBINARY
          // (no lob header), so we can read the raw encoded key directly (zero-copy)
          // and decode the rowid via the aux_type overload, skipping prefix.decode()
          // and ObTextStringHelper::read_real_string_data() per row.
          const share::ObLogicalTableAuxType aux_type = scalar_ctdef->logical_table_aux_type_;
          const bool has_lob = key_expr->obj_meta_.is_lob_storage();
          const bool use_rich = key_expr->enable_rich_format()
                                && is_valid_format(key_expr->get_format(*eval_ctx));
          ObIVector *key_vec = use_rich ? key_expr->get_vector(*eval_ctx) : nullptr;
          for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
            bool key_is_null = false;
            ObString key_str;
            if (OB_UNLIKELY(has_lob)) {
              // Defensive fallback: kv_key should never carry a lob header, but if it
              // does, route through the generic reader to stay correct.
              if (OB_FAIL(ObLogicalTableHelper::read_kv_string_at_row(
                      *key_expr, *eval_ctx, i, alloc, key_is_null, key_str))) {
                LOG_WARN("[LT hybrid scalar] read key string failed", K(ret), K(i));
              }
            } else if (use_rich) {
              if (OB_ISNULL(key_vec)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("[LT hybrid scalar] kv_key vector is null", K(ret), K(i));
              } else {
                const char *payload = nullptr;
                ObLength len = 0;
                key_vec->get_payload(i, key_is_null, payload, len);
                if (!key_is_null) {
                  key_str.assign_ptr(payload, static_cast<int32_t>(len));
                }
              }
            } else {
              ObDatum &d = key_expr->locate_expr_datum(*eval_ctx, i);
              if (d.is_null()) {
                key_is_null = true;
              } else {
                key_str = d.get_string();
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_UNLIKELY(key_is_null)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("encoded_key is null at row", K(ret), K(i));
            } else {
              uint64_t rowid = 0;
              if (OB_FAIL(share::ObLogicalTableDecoder::decode_rowid(key_str, aux_type, rowid))) {
                LOG_WARN("decode rowid failed", K(ret), K(i), K(aux_type), "key_len", key_str.length());
              } else {
                rowid_vec->set_uint(i, rowid);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
