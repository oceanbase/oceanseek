/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_scalar_primary_ror_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "share/logical_table/ob_logical_table_range_builder.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"

namespace oceanbase
{
namespace sql
{

int ObDASScalarPrimaryROROpParam::get_children_ops(ObIArray<ObIDASSearchOp *> &children) const
{
  // leaf node, no children
  return OB_SUCCESS;
}

int ObDASScalarPrimaryROROp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASScalarPrimaryROROpParam &scalar_op_param = static_cast<const ObDASScalarPrimaryROROpParam &>(op_param);
  if (OB_ISNULL(scalar_op_param.get_scan_ctdef()) || OB_ISNULL(scalar_op_param.get_scan_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    scalar_ctdef_ = scalar_op_param.get_scan_ctdef();
    scalar_rtdef_ = scalar_op_param.get_scan_rtdef();
    is_probe_mode_ = scalar_op_param.get_is_probe_mode();
  }
  LOG_TRACE("do init", K(ret), K(is_probe_mode_));
  return ret;
}

int ObDASScalarPrimaryROROp::do_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASScalarROROp::do_open())) {
    LOG_WARN("failed to open", K(ret));
  } else if (is_probe_mode_) {
    // The parent initializes shared metadata without opening an ordinary scan in probe mode.
    // Open only the get iterator consumed by do_probe / point_get.
    if (OB_ISNULL(tsc_service_) || OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), K(tsc_service_), K(scalar_ctdef_), K(scalar_rtdef_));
    } else if (OB_FAIL(ObDASScalarScanOp::resolve_scalar_tablet_id(*this, search_ctx_, scalar_ctdef_, tablet_id_))) {
      LOG_WARN("failed to resolve scalar tablet id", K(ret), K(tablet_id_));
    } else if (OB_FAIL(search_ctx_.init_scan_param(tablet_id_, scalar_ctdef_, scalar_rtdef_, get_param_,
                                                   true /* use_async_read_snapshot */))) {
      LOG_WARN("failed to init get param", K(ret), K(tablet_id_));
    } else {
      get_param_.op_ = nullptr;
      get_param_.is_get_ = true;
      get_param_.key_ranges_.reset();

      common::ObIAllocator &allocator = ctx_allocator();
      if (OB_FAIL(prepare_probe_ranges(allocator))) {
        LOG_WARN("failed to prepare probe ranges", K(ret));
      } else if (OB_FAIL(init_get_pd_op())) {
        LOG_WARN("failed to init get pd op", K(ret));
      } else if (OB_FAIL(tsc_service_->table_scan(get_param_, get_result_))) {
        LOG_WARN("failed to do table scan", K(ret));
      } else if (OB_ISNULL(get_result_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr get result", K(ret));
      } else {
        SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
      }
    }
  }
  return ret;
}

int ObDASScalarPrimaryROROp::init_get_pd_op()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_rtdef_) || OB_ISNULL(scalar_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    ObEvalCtx *eval_ctx = scalar_rtdef_->eval_ctx_;
    if (OB_ISNULL(eval_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr eval ctx", K(ret));
    } else {
      new(&get_pd_expr_op_) ObPushdownOperator(*eval_ctx,
                                               scalar_ctdef_->pd_expr_spec_,
                                               scalar_rtdef_->scan_flag_.enable_rich_format_);
      if (OB_FAIL(get_pd_expr_op_.init_pushdown_storage_filter())) {
        LOG_WARN("failed to init pushdown storage filter for get param", K(ret));
      } else {
        get_param_.op_ = &get_pd_expr_op_;
      }
    }
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASScalarROROp::do_close())) {
    LOG_WARN("failed to close", K(ret));
  }
  if (OB_NOT_NULL(get_result_) && OB_NOT_NULL(tsc_service_)) {
    int tmp_ret = OB_SUCCESS;
    common::ObIAllocator &allocator = ctx_allocator();
    if (OB_TMP_FAIL(tsc_service_->revert_scan_iter(get_result_))) {
      LOG_WARN("failed to revert scan iter", K(ret));
    }
    ret = ret == OB_SUCCESS ? tmp_ret : ret;
    get_result_ = nullptr;
    get_pd_expr_op_.~ObPushdownOperator();

    release_probe_ranges(allocator);

    get_param_.destroy_schema_guard();
    get_param_.snapshot_.reset();
    get_param_.destroy();
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (!is_probe_mode_) {
    if (OB_FAIL(ObDASScalarROROp::do_rescan())) {
      LOG_WARN("failed to do rescan from parent", K(ret));
    }
  } else {
    if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_) || OB_ISNULL(tsc_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), K(scalar_ctdef_), K(scalar_rtdef_), K(tsc_service_));
    } else if (OB_FAIL(ObDASScalarScanOp::resolve_scalar_tablet_id(*this, search_ctx_, scalar_ctdef_, tablet_id_))) {
      LOG_WARN("failed to resolve scalar tablet id", K(ret));
    } else if (FALSE_IT(ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), tablet_id_, get_param_))) {
    } else if (search_ctx_.is_async_index_query()
               && OB_FAIL(search_ctx_.apply_async_index_read_snapshot(get_param_))) {
      LOG_WARN("failed to apply async index read snapshot", K(ret), K(tablet_id_));
    } else if (OB_FAIL(tsc_service_->reuse_scan_iter(get_param_.need_switch_param_, get_result_))) {
      LOG_WARN("failed to reuse scan iter", K(ret));
    } else {
      // reset key ranges to ensure correctness
      ObRangeArray &key_ranges = get_param_.key_ranges_;
      for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
        ObNewRange &range = key_ranges.at(i);
        ObObj *start_objs = range.start_key_.get_obj_ptr();
        ObObj *end_objs = range.end_key_.get_obj_ptr();
        int64_t obj_cnt = range.start_key_.get_obj_cnt();
        if (OB_ISNULL(start_objs) || OB_ISNULL(end_objs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr objs", K(ret), K(range));
        } else {
          for (int64_t j = 0; j < obj_cnt; ++j) {
            start_objs[j].set_min_value();
            end_objs[j].set_max_value();
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tsc_service_->table_rescan(get_param_, get_result_))) {
        LOG_WARN("failed to rescan table", K(ret), K(get_param_));
      } else {
        get_param_.need_switch_param_ = false;
        SET_METRIC_VAL(common::ObMetricId::HS_TABLET_ID, tablet_id_.id());
      }
    }
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  if (is_probe_mode_) {
    // Probe mode: this op is a non-driver follower coordinated by the parent conjunction,
    // which must only call probe() against it. Iterating the scan iterator is forbidden.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("advance_to not supported in primary probe mode", K(ret), K(target));
  } else if (OB_FAIL(ObDASScalarROROp::do_advance_to(target, curr_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance to", K(ret), K(target));
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (is_probe_mode_) {
    // Probe mode: this op is a non-driver follower coordinated by the parent conjunction,
    // which must only call probe() against it. Iterating the scan iterator is forbidden.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("next_rowid not supported in primary probe mode", K(ret));
  } else if (OB_FAIL(ObDASScalarROROp::do_next_rowid(next_id, score))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do next rowid", K(ret));
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_advance_shallow(const ObDASRowID &target,
                                                const bool inclusive,
                                                const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  if (is_probe_mode_) {
    const double max_score = is_scoring_ ? scalar_rtdef_->get_boost_weight() : 0.0;
    if (inclusive) {
      max_score_tuple_.set(target, target, max_score);
    } else {
      ObDASRowID next_id;
      next_id.set_uint64(target.get_uint64() + 1);
      max_score_tuple_.set(next_id, next_id, max_score);
    }
    max_score_tuple = &max_score_tuple_;
  } else if (OB_FAIL(ObDASScalarROROp::do_advance_shallow(target, inclusive, max_score_tuple))) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to do advance shallow", K(ret), K(target));
  }
  return ret;
}

int ObDASScalarPrimaryROROp::do_probe(const ObDASRowID &target, bool &hit)
{
  int ret = OB_SUCCESS;
  hit = false;
  if (!is_probe_mode_) {
    // Only the probe-mode supports probe.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("probe not supported in non-probe primary ROR mode", K(ret), K(target));
  } else if (OB_FAIL(point_get(target, hit))) {
    LOG_WARN("failed to do point get for probe", K(ret), K(target));
  } else {
    LOG_TRACE("primary probe", K(target), K(hit));
  }
  return ret;
}

int ObDASScalarPrimaryROROp::point_get(const ObDASRowID &target, bool &found)
{
  int ret = OB_SUCCESS;
  found = false;

  if (OB_ISNULL(tsc_service_) || OB_ISNULL(scalar_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(tsc_service_), K(scalar_ctdef_));
  } else if (OB_FAIL(locate_probe_range(target))) {
    LOG_WARN("failed to locate probe range", K(ret), K(target));
  } else if (OB_FAIL(tsc_service_->reuse_scan_iter(false, get_result_))) {
    LOG_WARN("failed to reuse scan iter", K(ret));
  } else if (OB_FAIL(tsc_service_->table_rescan(get_param_, get_result_))) {
    LOG_WARN("failed to rescan table", K(ret));
  } else if (OB_FAIL(fetch_probe_row(found))) {
    LOG_WARN("failed to fetch probe row", K(ret), K(target));
  }

  return ret;
}

int ObDASScalarPrimaryROROp::prepare_probe_ranges(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scalar rtdef", K(ret));
  } else {
    const ObIArray<ObNewRange> &rt_ranges = scalar_rtdef_->key_ranges_;
    if (OB_UNLIKELY(rt_ranges.count() != 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid probe range cnt", K(ret), K(rt_ranges.count()));
    } else {
      const ObNewRange &src_range = rt_ranges.at(0);
      ObNewRange range;
      if (OB_FAIL(deep_copy_range(allocator, src_range, range))) {
        LOG_WARN("failed to deep copy range", K(ret));
      } else if (OB_FAIL(get_param_.key_ranges_.push_back(range))) {
        LOG_WARN("failed to push back key range", K(ret));
      }
    }
  }
  return ret;
}

int ObDASScalarPrimaryROROp::locate_probe_range(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &rowkeys = get_rowid_exprs();
  ObRangeArray &key_ranges = get_param_.key_ranges_;

  if (OB_UNLIKELY(key_ranges.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("primary probe key range invariant was broken after open",
             K(ret), "range_count", key_ranges.count());
  } else if (OB_UNLIKELY(key_ranges.at(0).start_key_.get_obj_cnt() != rowkeys.count() ||
                         key_ranges.at(0).end_key_.get_obj_cnt() != rowkeys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey count", K(ret), K(rowkeys), K(key_ranges));
  } else {
    ObObj *start_rowkey_objs = key_ranges.at(0).start_key_.get_obj_ptr();
    ObObj *end_rowkey_objs = key_ranges.at(0).end_key_.get_obj_ptr();

    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
      const ObExpr *rowkey_expr = rowkeys.at(i);
      if (OB_ISNULL(rowkey_expr) || OB_ISNULL(start_rowkey_objs + i) || OB_ISNULL(end_rowkey_objs + i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr rowkey expr", K(ret));
      } else {
        ObDatum target_datum;
        if (OB_FAIL(get_datum_from_rowid(target, target_datum, i))) {
          LOG_WARN("failed to get datum from target rowid", K(ret), K(i));
        } else if (OB_FAIL(target_datum.to_obj(start_rowkey_objs[i], rowkey_expr->obj_meta_))) {
          LOG_WARN("failed to convert datum to obj", K(ret));
        } else if (OB_FAIL(target_datum.to_obj(end_rowkey_objs[i], rowkey_expr->obj_meta_))) {
          LOG_WARN("failed to convert datum to obj for end key", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      key_ranges.at(0).border_flag_.set_inclusive_start();
      key_ranges.at(0).border_flag_.set_inclusive_end();
      LOG_TRACE("point get", K(key_ranges.at(0)));
    }
  }
  return ret;
}

int ObDASScalarPrimaryROROp::fetch_probe_row(bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (OB_ISNULL(get_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr get result", K(ret));
  } else if (get_pd_expr_op_.is_vectorized()) {
    int64_t count = 0;
    // The T_SINGLE_GET batch path resets shared vectors to uniform before projecting the probe row.
    ret = get_result_->get_next_rows(count, 1 /* capacity */);
    if (OB_SUCCESS == ret || OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      found = count > 0;
    } else {
      LOG_WARN("failed to get next rows from point get result", K(ret));
    }
  } else if (OB_FAIL(get_result_->get_next_row())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      found = false;
    } else {
      LOG_WARN("failed to get next row from point get result", K(ret));
    }
  } else {
    found = true;
  }
  return ret;
}

void ObDASScalarPrimaryROROp::release_probe_ranges(common::ObIAllocator &allocator)
{
  for (int64_t i = 0; i < get_param_.key_ranges_.count(); ++i) {
    ObNewRange &range = get_param_.key_ranges_.at(i);
    if (nullptr != range.start_key_.get_obj_ptr()) {
      allocator.free(range.start_key_.get_obj_ptr());
      range.start_key_.assign(nullptr, 0);
    }
    if (nullptr != range.end_key_.get_obj_ptr()) {
      allocator.free(range.end_key_.get_obj_ptr());
      range.end_key_.assign(nullptr, 0);
    }
  }
}

int ObDASScalarLogicPrimaryROROp::prepare_probe_ranges(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObLogicalTableSearchCtx &lt_ctx = search_ctx_.get_logical_table_ctx();
  // Logical table: the probe physically hits the KV data table. Convert the logical
  // rowkey ranges into KV-encoded ranges and redirect the get to the physical KV
  // table/tablet, mirroring ObDASScalarScanOp::prepare_scan_ranges. Otherwise the
  // probe scans the logical table id with un-encoded ranges and never hits, so every
  // probe misses and the conjunction yields empty results.
  if (OB_ISNULL(scalar_ctdef_) || OB_ISNULL(scalar_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), K(scalar_ctdef_), K(scalar_rtdef_));
  } else if (OB_FAIL(ObDASScalarScanOp::prepare_logical_table_kv_scan_ranges(
                 lt_ctx, scalar_ctdef_, scalar_rtdef_->key_ranges_, allocator, get_param_.key_ranges_))) {
    LOG_WARN("failed to convert logical table kv scan ranges", K(ret));
  } else {
    get_param_.index_id_ = lt_ctx.physical_data_table_id_;
    get_param_.tablet_id_ = lt_ctx.physical_tablet_id_;
  }
  return ret;
}

int ObDASScalarLogicPrimaryROROp::locate_probe_range(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  const ObLogicalTableSearchCtx &lt_ctx = search_ctx_.get_logical_table_ctx();
  // Logical table: build a single-point KV lookup range from the target rowid to
  // match the physical KV data table rowkey (ns_id, kv_key). The raw rowkey-obj
  // overwrite used for physical tables does not carry the KV prefix and never hits.
  if (OB_ISNULL(scalar_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scalar ctdef", K(ret));
  } else if (OB_UNLIKELY(get_rowid_type() != DAS_ROWID_TYPE_UINT64)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("logical table primary probe only supports uint64 rowid", K(ret), K(get_rowid_type()));
  } else {
    const uint64_t rowid_val = target.get_uint64();
    common::ObSEArray<uint64_t, 1> rowids;
    rowids.set_attr(ObMemAttr(MTL_ID(), "LTProbeRowid"));
    share::ObLogicalTableKeyPrefix prefix(
        lt_ctx.namespace_id_, lt_ctx.logical_table_id_,
        scalar_ctdef_->logical_index_seq_, scalar_ctdef_->logical_table_aux_type_);
    get_param_.key_ranges_.reset();
    // Recycle the previous probe's KV range memory so it stays O(1) across probes
    // instead of accumulating on the op/statement allocator.
    probe_range_alloc_.reuse();
    if (OB_FAIL(rowids.push_back(rowid_val))) {
      LOG_WARN("failed to push back probe rowid", K(ret), K(rowid_val));
    } else if (OB_FAIL(share::ObLogicalTableRangeBuilder::build_lookup_ranges_from_rowids(
                   lt_ctx.physical_data_table_id_, prefix, rowids, probe_range_alloc_, get_param_.key_ranges_))) {
      LOG_WARN("failed to build logical kv lookup range for probe", K(ret), K(prefix), K(rowid_val));
    } else {
      LOG_TRACE("logical table point get", K(prefix), K(rowid_val), K(get_param_.key_ranges_));
    }
  }
  return ret;
}

int ObDASScalarLogicPrimaryROROp::fetch_probe_row(bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  // Logical table: fetch via the batch API so the kv_key/kv_value access exprs are
  // populated in the batch/vector format that process_logical_table_storage_batch
  // expects (rich format reads the vector by row_idx). Then run KV decode
  // (project_logical_columns) + logical_residual_filters_ -- the JSON-field DSL
  // conditions (e.g. data_content.metadata.xxx) that cannot be pushed to storage.
  // A row that exists but fails the residual filter must NOT hit; otherwise the
  // scalar filter silently has no effect in the primary-get probe path.
  if (OB_ISNULL(get_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr get result", K(ret));
  } else {
    int64_t cnt = 0;
    ret = get_result_->get_next_rows(cnt, 1 /*capacity*/);
    if (OB_ITER_END == ret && cnt > 0) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      if (cnt > 0 && OB_FAIL(ObDASScalarScanOp::process_logical_table_storage_batch(
                         search_ctx_, scalar_ctdef_, scalar_rtdef_,
                         cnt))) {
        LOG_WARN("failed to process logical table storage batch for probe", K(ret), K(cnt));
      } else {
        // process_logical_table_storage_batch compacts cnt to 0 when the residual
        // filters reject the row.
        found = (cnt > 0);
      }
    } else if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      found = false;
    } else {
      LOG_WARN("failed to get next rows from point get result", K(ret));
    }
  }
  return ret;
}

void ObDASScalarLogicPrimaryROROp::release_probe_ranges(common::ObIAllocator &allocator)
{
  UNUSED(allocator);
  // Logical table probe ranges are built on probe_range_alloc_ (see locate_probe_range),
  // so they are reclaimed by that arena's destructor; freeing them via ctx_allocator()
  // would be an allocator mismatch.
}

int ObDASScalarPrimaryROROp::advance_skip_scan(const ObDASRowID &target)
{
  int ret = OB_SUCCESS;
  if (is_probe_mode_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("advance_skip_scan not supported in primary probe mode", K(ret), K(target));
  } else {
    ObRangeArray &key_ranges = scan_param_.key_ranges_;
    const ObIArray<ObExpr *> &rowkeys = get_rowid_exprs();
    if (OB_UNLIKELY(key_ranges.count() != 1)) {
      // do nothing
    } else if (OB_UNLIKELY(key_ranges.at(0).start_key_.get_obj_cnt() != rowkeys.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey count", K(ret), K(rowkeys), K(key_ranges));
    } else {
      ObObj *start_rowkey_objs = key_ranges.at(0).start_key_.get_obj_ptr();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); i++) {
        const ObExpr *rowkey_expr = rowkeys.at(i);
        if (OB_ISNULL(rowkey_expr) || OB_ISNULL(start_rowkey_objs + i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr rowkey expr", K(ret));
        } else {
          ObDatum target_datum;
          if (OB_FAIL(get_datum_from_rowid(target, target_datum, i))) {
            LOG_WARN("failed to get datum from target", K(ret), K(i));
          } else if (OB_FAIL(target_datum.to_obj(start_rowkey_objs[i], rowkey_expr->obj_meta_))) {
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
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
