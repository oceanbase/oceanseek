/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_scalar_primary_ror_op.h"
#include "sql/das/search/ob_das_scalar_index_ror_op.h"
#include "sql/das/search/ob_das_sort_op.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_scalar_ror_op.h"
#include "sql/das/search/ob_das_async_scalar_range_op.h"
#include "sql/das/search/ob_das_disjunction_filter_op.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER((ObDASScalarCtDef, ObIDASSearchCtDef), has_main_scan_, has_index_scan_, boost_, primary_get_ratio_);

OB_SERIALIZE_MEMBER((ObDASScalarRtDef, ObIDASSearchRtDef));

OB_SERIALIZE_MEMBER((ObDASScalarScanCtDef, ObIDASSearchCtDef),
                    ref_table_id_,
                    access_column_ids_,
                    schema_version_,
                    table_param_,
                    pd_expr_spec_,
                    result_output_,
                    rowkey_exprs_,
                    table_scan_opt_,
                    pre_query_range_,
                    pre_range_graph_,
                    flags_,
                    logical_table_aux_type_,
                    physical_data_table_id_,
                    logical_column_mapping_,
                    logical_output_exprs_,
                    logical_index_seq_,
                    lt_ctx_,
                    logical_residual_filters_,
                    nested_path_for_full_subtree_,
                    column_idx_for_full_subtree_,
                    nesting_depth_for_full_subtree_);

OB_DEF_SERIALIZE(ObDASScalarScanRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_schema_version_,
              timeout_ts_,
              tx_lock_timeout_,
              sql_mode_,
              scan_flag_,
              key_ranges_,
              really_need_rowkey_order_,
              boost_weight_);
  LST_DO_CODE(OB_UNIS_ENCODE, lt_rt_.namespace_id_, lt_rt_.logical_table_id_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASScalarScanRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_schema_version_,
              timeout_ts_,
              tx_lock_timeout_,
              sql_mode_,
              scan_flag_,
              key_ranges_,
              really_need_rowkey_order_,
              boost_weight_);
  LST_DO_CODE(OB_UNIS_DECODE, lt_rt_.namespace_id_, lt_rt_.logical_table_id_);
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  if (OB_SUCC(ret) && lt_rt_.has_resolved_ns_lt()) {
    lt_rt_.enabled_ = true;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASScalarScanRtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              tenant_schema_version_,
              timeout_ts_,
              tx_lock_timeout_,
              sql_mode_,
              scan_flag_,
              key_ranges_,
              really_need_rowkey_order_,
              boost_weight_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, lt_rt_.namespace_id_, lt_rt_.logical_table_id_);
  return len;
}

const ObDASScalarScanCtDef* ObDASScalarCtDef::get_index_scan_ctdef() const
{
  const ObDASScalarScanCtDef* index_scan_ctdef = nullptr;
  if (has_index_scan_ && children_cnt_ > 0 && nullptr != children_[0]) {
    index_scan_ctdef = static_cast<const ObDASScalarScanCtDef *>(children_[0]);
    if (index_scan_ctdef->is_primary_table_scan_) {
      index_scan_ctdef = nullptr;
    }
  }
  return index_scan_ctdef;
}

const ObDASScalarScanCtDef* ObDASScalarCtDef::get_main_scan_ctdef() const
{
  const ObDASScalarScanCtDef* main_scan_ctdef = nullptr;
  if (has_main_scan_) {
    if (children_cnt_ == 1 && nullptr != children_[0]) {
      main_scan_ctdef = static_cast<const ObDASScalarScanCtDef *>(children_[0]);
    } else if (children_cnt_ == 2 && nullptr != children_[1]) {
      main_scan_ctdef = static_cast<const ObDASScalarScanCtDef *>(children_[1]);
    }

    if (nullptr != main_scan_ctdef && !main_scan_ctdef->is_primary_table_scan_) {
      main_scan_ctdef = nullptr;
    }
  }
  return main_scan_ctdef;
}

int ObDASScalarCtDef::eval_boost(ObEvalCtx *eval_ctx, double &boost_weight) const
{
  int ret = OB_SUCCESS;
  boost_weight = 0.0;
  if (!is_scoring() || OB_ISNULL(boost_)) {
    // do nothing
  } else {
    ObDatum *datum = nullptr;
    if (OB_FAIL(boost_->eval(*eval_ctx, datum))) {
      LOG_WARN("failed to eval scalar boost", K(ret));
    } else if (OB_ISNULL(datum) || datum->is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null scalar boost datum", K(ret));
    } else {
      boost_weight = datum->get_double();
    }
  }
  return ret;
}

ObDASScalarScanRtDef* ObDASScalarRtDef::get_index_scan_rtdef() const
{ 
  ObDASScalarScanRtDef* index_scan_rtdef = nullptr;
  if (children_cnt_ > 0 && nullptr != children_[0]) {
    index_scan_rtdef = static_cast<ObDASScalarScanRtDef *>(children_[0]);
  }
  return index_scan_rtdef;
}

ObDASScalarScanRtDef* ObDASScalarRtDef::get_main_scan_rtdef() const
{
  ObDASScalarScanRtDef* main_scan_rtdef = nullptr;
  if (children_cnt_ == 1 && nullptr != children_[0]) {
    main_scan_rtdef = static_cast<ObDASScalarScanRtDef *>(children_[0]);
  } else if (children_cnt_ == 2 && nullptr != children_[1]) {
    main_scan_rtdef = static_cast<ObDASScalarScanRtDef *>(children_[1]);
  }
  return main_scan_rtdef;
}

int ObDASScalarRtDef::compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  const ObDASScalarCtDef* scalar_ctdef = get_ctdef();
  if (OB_ISNULL(scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_UNLIKELY(children_cnt_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children count is not expected", K(ret), K(children_cnt_));
  } else if (scalar_ctdef->has_index_scan()) {
    ObDASScalarScanRtDef* index_scan_rtdef = get_index_scan_rtdef();
    if (OB_ISNULL(index_scan_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan rtdef is null", K(ret));
    } else if (OB_FAIL(index_scan_rtdef->get_cost(search_ctx, cost))) {
      LOG_WARN("failed to get index scan cost", K(ret));
    }
  } else {
    // primary table can not estimate row count
    cost = search_ctx.get_row_count();
  }

  return ret;
}

int ObDASScalarRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  return generate_op(lead_cost, search_ctx, false, false, op);
}

int ObDASScalarRtDef::generate_selected_primary_probe_op(
    ObDASSearchCost lead_cost,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *&op)
{
  return generate_op(lead_cost, search_ctx, false, true, op);
}

bool ObDASScalarRtDef::can_build_direct_rowid_drain(const ObDASSearchCtx &search_ctx) const
{
  const ObDASScalarScanRtDef *scan_rtdef = get_index_scan_rtdef();
  const bool bool_ret = OB_NOT_NULL(get_ctdef()) && get_ctdef()->has_index_scan()
      && OB_NOT_NULL(scan_rtdef) && scan_rtdef->can_build_direct_rowid_drain(search_ctx);
  return bool_ret;
}

int ObDASScalarRtDef::generate_op(
    ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
    bool allow_direct_drain, ObIDASSearchOp *&op)
{
  return generate_op(lead_cost, search_ctx, allow_direct_drain, false, op);
}

int ObDASScalarRtDef::generate_op(
    ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
    bool allow_direct_drain, bool force_primary_probe, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  ObDASSearchCost cost;
  const ObDASScalarCtDef* scalar_ctdef = get_ctdef();
  if (OB_ISNULL(scalar_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_FAIL(get_cost(search_ctx, cost))) {
    LOG_WARN("failed to compute cost", K(ret));
  } else {
    double boost_weight = 0.0;
    ObDASScalarScanRtDef *index_scan_rtdef = get_index_scan_rtdef();
    const ObDASScalarScanCtDef *index_scan_ctdef = scalar_ctdef->get_index_scan_ctdef();
    ObDASScalarScanRtDef *primary_scan_rtdef = get_main_scan_rtdef();
    const ObDASScalarScanCtDef *primary_scan_ctdef = scalar_ctdef->get_main_scan_ctdef();
    if (OB_NOT_NULL(index_scan_rtdef)) {
      index_scan_rtdef->set_estimated_work_row_count(cost.cost());
    }
    if (OB_NOT_NULL(primary_scan_rtdef)) {
      primary_scan_rtdef->set_estimated_work_row_count(cost.cost());
    }
    if (OB_FAIL(scalar_ctdef->eval_boost(eval_ctx_, boost_weight))) {
      LOG_WARN("failed to eval scalar boost weight", K(ret));
    } else {
      if (OB_NOT_NULL(index_scan_rtdef)) {
        index_scan_rtdef->set_boost_weight(boost_weight);
      }
      if (OB_NOT_NULL(primary_scan_rtdef)) {
        primary_scan_rtdef->set_boost_weight(boost_weight);
      }
    }
    // Decide the scan path:
    //   1. probe when allow probe and should probe;
    //   2. otherwise index scan when available;
    //   3. otherwise primary scan without index.
    bool prefer_probe = force_primary_probe;
    if (OB_FAIL(ret)) {
    } else if (!force_primary_probe && get_allow_probe()
               && OB_FAIL(prefer_primary_probe(lead_cost, search_ctx, prefer_probe))) {
      LOG_WARN("failed to decide probe selection", K(ret));
    } else if (prefer_probe && scalar_ctdef->has_main_scan()) {
      // scan primary table as a probe
      LOG_TRACE("scan primary table as a probe", K(ret), K(get_allow_probe()), K(cost), K(lead_cost));
      if (OB_ISNULL(primary_scan_rtdef) || OB_ISNULL(primary_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (FALSE_IT(primary_scan_rtdef->set_allow_probe(true))) {
      } else if (OB_FAIL(primary_scan_rtdef->generate_op(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate primary scan op", K(ret));
      }
    } else if (scalar_ctdef->has_index_scan()) {
      // scan index table
      LOG_TRACE("scan index table", K(ret), K(get_allow_probe()), K(cost), K(lead_cost));
      if (OB_ISNULL(index_scan_rtdef) || OB_ISNULL(index_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(allow_direct_drain
          ? index_scan_rtdef->generate_op(lead_cost, search_ctx, true, op)
          : index_scan_rtdef->generate_op(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate index scan op", K(ret));
      }
    } else if (scalar_ctdef->has_main_scan()) {
      // scan primary table without index
      LOG_TRACE("scan primary table without index", K(ret), K(get_allow_probe()), K(cost), K(lead_cost));
      if (OB_ISNULL(primary_scan_rtdef) || OB_ISNULL(primary_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (FALSE_IT(primary_scan_rtdef->set_allow_probe(get_allow_probe()))) {
      } else if (OB_FAIL(primary_scan_rtdef->generate_op(lead_cost, search_ctx, op))) {
        LOG_WARN("failed to generate primary scan op", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid scalar scan config", K(ret));
    }
  }
  return ret;
}

int ObDASScalarScanRtDef::init_pd_op(ObExecContext &exec_ctx, const ObDASScalarScanCtDef &scalar_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p_row2exprs_projector_)) {
    p_row2exprs_projector_ = new(&row2exprs_projector_) ObRow2ExprsProjector(exec_ctx.get_allocator());
  }
  if (nullptr == p_pd_expr_op_) {
    if (OB_ISNULL(eval_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(eval_ctx_));
    } else if (FALSE_IT(p_pd_expr_op_ = new(&pd_expr_op_) ObPushdownOperator(*eval_ctx_,
                                                                             scalar_ctdef.pd_expr_spec_,
                                                                             scan_flag_.enable_rich_format_))) {
    } else if (OB_FAIL(pd_expr_op_.init_pushdown_storage_filter())) {
      LOG_WARN("init pushdown storage filter failed", K(ret));
    }
  }
  return ret;
}

int ObDASScalarScanRtDef::compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  const ObDASScalarScanCtDef* ctdef = get_ctdef();
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (OB_FAIL(search_ctx.estimate_row_count(ctdef, this, row_count))) {
    LOG_WARN("failed to estimate row count", K(ret));
  } else {
    cost = ObDASSearchCost(row_count);
  }
  return ret;
}

bool ObDASScalarScanRtDef::can_build_streaming_group_filter(
    const ObDASSearchCtx &search_ctx) const
{
  const ObDASScalarScanCtDef *ctdef = get_ctdef();
  const ObDASAsyncIndexQueryCtx *query_ctx = search_ctx.get_async_query_ctx();
  const int64_t group_slot = search_ctx.get_async_group_slot();
  return OB_NOT_NULL(ctdef) && !ctdef->is_scoring()
      && ctdef->is_async_index_ && ctdef->is_search_index_
      && ctdef->is_index_term_point_scan_ && ctdef->enable_new_false_range_
      && !ctdef->is_logical_table() && !ctdef->is_nested_scope_
      && !ctdef->is_full_subtree_scan_for_nested_ && 1 == key_ranges_.count()
      && DAS_SCOPE_BASE == search_ctx.get_op_creating_scope()
      && DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type()
      && !search_ctx.is_logical_table() && !search_ctx.has_docid_range()
      && search_ctx.is_serial_fusion_plan()
      && group_slot >= 0 && OB_NOT_NULL(query_ctx) && query_ctx->is_inited()
      && 1 == query_ctx->get_max_segment_cnt(group_slot, ctdef->ref_table_id_);
}

int ObDASScalarScanRtDef::generate_streaming_group_filter_op(
    ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  op = nullptr;
  if (!can_build_streaming_group_filter(search_ctx)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported streaming Group filter domain", K(ret));
  } else {
    // Capture the Search index's Group binding, never the enclosing FTS index.
    // Each invocation allocates fresh leaves/cursors; the CtDef/RtDef is a recipe.
    const uint64_t index_table_id = get_ctdef()->ref_table_id_;
    const int64_t group_slot = search_ctx.get_async_group_slot();
    ObIDASSearchOp *leaf = nullptr;
    ObSEArray<ObIDASSearchOp *, 1> leaves;
    if (OB_FAIL(create_async_ror_op(search_ctx, 0, index_table_id, group_slot, true, leaf))) {
      LOG_WARN("failed to create independent Group ROR leaf", K(ret));
    } else if (OB_FAIL(leaves.push_back(leaf))) {
      LOG_WARN("failed to collect independent Group ROR leaf", K(ret));
    } else {
      // Retain the runtime-range union for false ranges and range growth on rescan.
      ObDASAsyncScalarRorUnionOpParam param(
          leaves, lead_cost, 0.0, this, index_table_id, group_slot, true, false, true);
      param.set_is_scoring(false);
      ObDASAsyncScalarRorUnionOp *filter = nullptr;
      if (OB_FAIL(search_ctx.create_op(param, filter))) {
        LOG_WARN("failed to create independent streaming Group filter", K(ret));
      } else {
        op = filter;
      }
    }
  }
  return ret;
}

int ObDASScalarScanRtDef::create_async_ror_op(
    ObDASSearchCtx &search_ctx,
    const int64_t range_idx,
    const uint64_t index_table_id,
    const int64_t group_slot,
    const bool direct_bind,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanCtDef *ctdef = get_ctdef();
  op = nullptr;
  if (OB_ISNULL(ctdef)
      || OB_UNLIKELY(!ctdef->is_async_ror_scan() || range_idx < 0
                     || common::OB_INVALID_ID == index_table_id
                     || group_slot < -1
                     || (direct_bind && group_slot < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar ror argument", K(ret), KP(ctdef), K(range_idx));
  } else {
    ObDASAsyncScalarRorOpParam ror_param(
        ctdef, this, range_idx, index_table_id, group_slot, direct_bind);
    ObDASAsyncScalarRorOp *ror_op = nullptr;
    ror_param.set_is_scoring(ctdef->is_scoring());
    if (OB_FAIL(search_ctx.create_op(ror_param, ror_op))) {
      LOG_WARN("failed to create async scalar ror op", K(ret), K(range_idx));
    } else {
      op = ror_op;
    }
  }
  return ret;
}

static int create_async_range_op(
    ObDASScalarScanRtDef &scalar_rtdef,
    ObDASSearchCtx &search_ctx,
    const uint64_t index_table_id,
    const int64_t group_slot,
    const bool direct_bind,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanCtDef *ctdef = scalar_rtdef.get_ctdef();
  ObDASAsyncScalarRangeOp *range_op = nullptr;
  op = nullptr;
  if (OB_ISNULL(ctdef)
      || OB_UNLIKELY(!ctdef->is_async_index_ || ctdef->is_async_ror_scan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar range definition", K(ret), KP(ctdef));
  } else {
    ObDASAsyncScalarRangeOpParam range_param(
        ctdef, &scalar_rtdef, index_table_id, group_slot, direct_bind);
    range_param.set_is_scoring(ctdef->is_scoring());
    if (OB_FAIL(search_ctx.create_op(range_param, range_op))) {
      LOG_WARN("failed to create async scalar range op", K(ret));
    } else {
      op = range_op;
    }
  }
  return ret;
}

static int create_async_range_materializer(
    ObDASScalarScanRtDef &scalar_rtdef,
    ObDASSearchCtx &search_ctx,
    ObIDASSearchOp *input_op,
    ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanCtDef *ctdef = scalar_rtdef.get_ctdef();
  op = nullptr;
  if (OB_ISNULL(ctdef) || OB_ISNULL(input_op)
      || OB_UNLIKELY(!ctdef->is_async_index_ || ctdef->is_async_ror_scan())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async scalar range materializer argument", K(ret),
        KP(ctdef), KP(input_op));
  } else if (DAS_SCOPE_NESTED_SYNC == search_ctx.get_op_creating_scope()) {
    // The same nested RowID may match multiple terms or overlapping ranges.
    ObDASSortOpParam sort_param(
        input_op, ctdef, &scalar_rtdef, true /* unique_sort */);
    ObDASSortOp *sort_op = nullptr;
    sort_param.set_is_scoring(ctdef->is_scoring());
    if (OB_FAIL(search_ctx.create_op(sort_param, sort_op))) {
      LOG_WARN("failed to create async scalar range sort op", K(ret));
    } else {
      op = sort_op;
    }
  } else {
    // A term range is not globally ordered by doc_id. Bitmap provides both
    // ordering and deduplication for the complete input domain.
    ObDASBitmapOpParam bitmap_param(input_op, ctdef, &scalar_rtdef);
    ObDASBitmapOp *bitmap_op = nullptr;
    bitmap_param.set_is_scoring(ctdef->is_scoring());
    if (OB_FAIL(search_ctx.create_op(bitmap_param, bitmap_op))) {
      LOG_WARN("failed to create async scalar range bitmap op", K(ret));
    } else {
      op = bitmap_op;
    }
  }
  return ret;
}

int ObDASScalarScanRtDef::generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op)
{
  return generate_op(lead_cost, search_ctx, false, op);
}

bool ObDASScalarScanRtDef::can_build_direct_rowid_drain(const ObDASSearchCtx &search_ctx) const
{
  const ObDASScalarScanCtDef *ctdef = get_ctdef();
  const bool bool_ret = OB_NOT_NULL(ctdef) && ctdef->is_async_ror_scan()
      && ctdef->is_index_term_point_scan_
      && !ctdef->is_scoring()
      && !key_ranges_.empty()
      && DAS_SCOPE_BASE == search_ctx.get_op_creating_scope()
      && DAS_ROWID_TYPE_UINT64 == search_ctx.get_rowid_type()
      && !search_ctx.is_logical_table();
  return bool_ret;
}

int ObDASScalarScanRtDef::generate_op(
    ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx,
    bool allow_direct_drain, ObIDASSearchOp *&op)
{
  int ret = OB_SUCCESS;
  // is_rowkey_order_scan_
  // ├── YES: need_rowkey_order_
  // |       ├── NO:  Scan Op
  // |       └── YES: ROR (Primary/Index ROR)
  // └── NO:  need_rowkey_order_
  //          ├── NO:  Scan Op
  //          └── YES: Scan Op + Bitmap/Sort Op
  const ObDASScalarScanCtDef *ctdef = get_ctdef();
  if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scalar ctdef is null", K(ret));
  } else if (ctdef->is_async_index_ && ctdef->is_full_subtree_scan_for_nested_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested only-must-not query on async search index is");
    LOG_WARN("nested full-subtree scan is not supported on async search index", K(ret), KPC(ctdef));
  } else if (ctdef->is_async_index_ && key_ranges_.empty()
      && !ctdef->enable_new_false_range_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty key ranges for async scalar scan", K(ret), KPC(ctdef));
  } else if (ctdef->is_async_index_) {
    ObDASAsyncIndexQueryCtx *query_ctx = search_ctx.get_async_query_ctx();
    const uint64_t index_table_id = ctdef->ref_table_id_;
    const uint64_t claimed_index_table_id = search_ctx.get_async_index_table_id();
    const int64_t group_slot = search_ctx.get_async_group_slot();
    const bool use_ror = ctdef->is_async_ror_scan();
    // New false ranges describe the empty-domain representation, not whether
    // Terms are static. The ROR union also owns range growth/shrink on rescan.
    const bool use_terms_direct_drain = allow_direct_drain
        && can_build_direct_rowid_drain(search_ctx);
    // Runtime point ranges need one streaming ROR each. Keep one placeholder
    // for an initially empty dynamic domain so later rescans can grow the union.
    const int64_t initial_ror_count = use_ror
        ? (ctdef->enable_new_false_range_ && key_ranges_.empty()
            ? 1 : key_ranges_.count())
        : 0;
    int64_t max_segment_cnt = 0;
    ObSEArray<ObIDASSearchOp *, 4> segment_ops;
    ObDASAsyncBindingGuard binding_guard(search_ctx);
    if (OB_ISNULL(query_ctx) || OB_UNLIKELY(!query_ctx->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async index query context is not initialized", K(ret), KP(query_ctx));
    } else if (OB_UNLIKELY(common::OB_INVALID_ID == index_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async scalar construction domain", K(ret),
               K(index_table_id), K(group_slot));
    } else if (use_ror && OB_UNLIKELY(initial_ror_count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid initial async scalar ror count", K(ret),
          K(initial_ror_count), "range_count", key_ranges_.count(), KPC(ctdef));
    } else if (common::OB_INVALID_ID != claimed_index_table_id) {
      if (OB_UNLIKELY(claimed_index_table_id != index_table_id)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("async scalar crosses claimed index domain", K(ret),
                 K(claimed_index_table_id), K(index_table_id), K(group_slot));
      } else {
        // The outer same-index compound subtree has already been cloned per Segment;
        // generate only one fixed-binding instance for this leaf.
        max_segment_cnt = 1;
      }
    } else {
      max_segment_cnt = group_slot >= 0
          ? query_ctx->get_max_segment_cnt(group_slot, index_table_id)
          : query_ctx->get_max_segment_cnt(index_table_id);
      binding_guard.set(index_table_id, group_slot, group_slot >= 0 && 1 >= max_segment_cnt);
    }
    for (int64_t segment_idx = 0;
         OB_SUCC(ret) && segment_idx < MAX(1L, max_segment_cnt); ++segment_idx) {
      ObIDASSearchOp *segment_op = nullptr;
      if (use_ror) {
        ObSEArray<ObIDASSearchOp *, 4> ror_ops;
        for (int64_t range_idx = 0;
             OB_SUCC(ret) && range_idx < initial_ror_count;
             ++range_idx) {
          ObIDASSearchOp *ror_op = nullptr;
          if (OB_FAIL(create_async_ror_op(
              search_ctx, range_idx, index_table_id, group_slot,
              search_ctx.get_async_direct_bind(), ror_op))) {
            LOG_WARN("failed to create initial async scalar ror", K(ret),
                K(segment_idx), K(range_idx));
          } else if (OB_ISNULL(ror_op)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null async scalar ror", K(ret), K(segment_idx), K(range_idx));
          } else if (OB_FAIL(ror_ops.push_back(ror_op))) {
            LOG_WARN("failed to collect async scalar ror", K(ret), K(segment_idx), K(range_idx));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(ror_ops.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("async scalar segment has no ror", K(ret), K(segment_idx));
        } else if ((use_terms_direct_drain && ror_ops.count() > 1)
                   || ctdef->enable_new_false_range_) {
          ObDASAsyncScalarRorUnionOpParam union_param(
              ror_ops, lead_cost, get_boost_weight(), this,
              index_table_id, group_slot, search_ctx.get_async_direct_bind(),
              use_terms_direct_drain);
          ObDASAsyncScalarRorUnionOp *union_op = nullptr;
          union_param.set_is_scoring(ctdef->is_scoring());
          if (OB_FAIL(search_ctx.create_op(union_param, union_op))) {
            LOG_WARN("failed to create async scalar ror union", K(ret),
                K(segment_idx), K(use_terms_direct_drain));
          } else {
            segment_op = union_op;
          }
        } else if (1 == ror_ops.count()) {
          segment_op = ror_ops.at(0);
        } else {
          ObDASDisjunctionFilterOpParam union_param(
              ror_ops, 1, lead_cost, get_boost_weight());
          ObDASDisjunctionFilterOp *union_op = nullptr;
          union_param.set_is_scoring(ctdef->is_scoring());
          if (OB_FAIL(search_ctx.create_op(union_param, union_op))) {
            LOG_WARN("failed to create async scalar ror union", K(ret), K(segment_idx));
          } else {
            segment_op = union_op;
          }
        }
      } else {
        // One leaf consumes every runtime range in this segment. Materialization
        // is added once after all physical Segment leaves are assembled.
        if (OB_FAIL(create_async_range_op(
                *this, search_ctx, index_table_id, group_slot,
                search_ctx.get_async_direct_bind(), segment_op))) {
          LOG_WARN("failed to create async scalar range leaf", K(ret), K(segment_idx));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(segment_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null async scalar segment op", K(ret), K(segment_idx));
      } else if (OB_FAIL(segment_ops.push_back(segment_op))) {
        LOG_WARN("failed to append async scalar segment op", K(ret), K(segment_idx));
      }
    }
    if (OB_SUCC(ret) && use_terms_direct_drain) {
      if (common::OB_INVALID_ID != claimed_index_table_id
          || search_ctx.get_async_direct_bind()) {
        if (OB_UNLIKELY(1 != segment_ops.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("direct/claimed async Terms filter has invalid leaf count", K(ret),
              K(index_table_id), K(group_slot), K(segment_ops.count()),
              "direct_bind", search_ctx.get_async_direct_bind());
        } else {
          op = segment_ops.at(0);
        }
      } else if (OB_FAIL(ObDASSearchUtils::create_async_segment_merge_op(
                     segment_ops, index_table_id, ctdef->is_scoring(), search_ctx,
                     op, true /* force_merge */,
                     ObDASAsyncSegmentMergeMode::DIRECT_MERGE))) {
        LOG_WARN("failed to create direct async Terms segment merge", K(ret),
            K(index_table_id), K(group_slot));
      }
      if (OB_SUCC(ret)) {
        LOG_TRACE("built async Terms BMM direct-drain path", K(ret),
            K(index_table_id), K(group_slot), K(segment_ops.count()),
            "direct_bind", search_ctx.get_async_direct_bind());
      }
    } else if (OB_SUCC(ret) && !use_ror) {
      ObIDASSearchOp *materializer_input = nullptr;
      if (common::OB_INVALID_ID != claimed_index_table_id
          || search_ctx.get_async_direct_bind()) {
        if (OB_UNLIKELY(1 != segment_ops.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("direct/claimed async scalar range has invalid leaf count", K(ret),
                   K(index_table_id), K(group_slot), K(segment_ops.count()),
                   "direct_bind", search_ctx.get_async_direct_bind());
        } else {
          // A claimed subtree is bound by its outer SORT owner. A direct-bound
          // one-Segment Group resolves the physical Segment in the leaf itself.
          materializer_input = segment_ops.at(0);
        }
      } else if (OB_FAIL(ObDASSearchUtils::create_async_segment_merge_op(
                     segment_ops, index_table_id, ctdef->is_scoring(), search_ctx,
                     materializer_input, true /* force_merge */,
                     ObDASAsyncSegmentMergeMode::DIRECT_MERGE))) {
        LOG_WARN("failed to create direct async scalar segment merge", K(ret),
                 K(index_table_id), K(group_slot));
      }
      if (OB_SUCC(ret) && OB_FAIL(create_async_range_materializer(
              *this, search_ctx, materializer_input, op))) {
        LOG_WARN("failed to create global async scalar range materializer", K(ret),
                 K(index_table_id), K(group_slot));
      }
    } else if (OB_SUCC(ret) && use_ror
               && OB_FAIL(ObDASSearchUtils::create_async_segment_merge_op(
                   segment_ops, index_table_id, ctdef->is_scoring(), search_ctx, op))) {
      LOG_WARN("failed to create async segment merge op for scalar ROR", K(ret),
               K(index_table_id));
    }
    if (OB_FAIL(ret)) {
      op = nullptr;
    }
  } else if (ctdef->is_rowkey_order_scan_) {
    // ROR scan (logical residual filters: ObDASScalarROROp::process_logical_table_storage_batch_)
    if (!really_need_rowkey_order_ && !ctdef->is_search_index_) {
      ObDASScalarScanOpParam op_param(ctdef, this);
      ObDASScalarScanOp *non_ror_scan_op = nullptr;
      op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(op_param, non_ror_scan_op))) {
        LOG_WARN("failed to create scan op", K(ret));
      } else if (OB_ISNULL(non_ror_scan_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else {
        op = static_cast<ObIDASSearchOp *>(non_ror_scan_op);
      }
    } else if (ctdef->is_primary_table_scan_) {
      ObDASScalarPrimaryROROpParam op_param(ctdef, this);
      ObDASScalarPrimaryROROp *primary_ror_op = nullptr;
      op_param.set_is_scoring(ctdef->is_scoring());
      op_param.set_is_probe_mode(get_allow_probe());
      // Logical tables probe the physical KV data table, so dispatch to the dedicated
      // ObDASScalarLogicPrimaryROROp which isolates the logical-specific probe handling.
      const bool is_logical_table = ctdef->is_logical_table()
          && search_ctx.get_logical_table_ctx().is_enabled_;
      if (is_logical_table) {
        ObDASScalarLogicPrimaryROROp *logic_ror_op = nullptr;
        if (OB_FAIL(search_ctx.create_op(op_param, logic_ror_op))) {
          LOG_WARN("failed to create logic primary ror op", K(ret));
        } else if (OB_ISNULL(logic_ror_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret));
        } else {
          primary_ror_op = logic_ror_op;
        }
      } else if (OB_FAIL(search_ctx.create_op(op_param, primary_ror_op))) {
        LOG_WARN("failed to create primary ror op", K(ret));
      } else if (OB_ISNULL(primary_ror_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      }
      if (OB_SUCC(ret)) {
        op = static_cast<ObIDASSearchOp *>(primary_ror_op);
      }
    } else {
      ObDASScalarIndexROROpParam op_param(ctdef, this);
      ObDASScalarIndexROROp *index_ror_op = nullptr;
      op_param.set_is_scoring(ctdef->is_scoring());
      if (OB_FAIL(search_ctx.create_op(op_param, index_ror_op))) {
        LOG_WARN("failed to create index ror op", K(ret));
      } else if (OB_ISNULL(index_ror_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else {
        op = static_cast<ObIDASSearchOp *>(index_ror_op);
      }
    }
  } else {
    // non-ROR scan
    ObDASScalarScanOpParam op_param(ctdef, this);
    ObDASScalarScanOp *non_ror_scan_op = nullptr;
    op_param.set_is_scoring(ctdef->is_scoring());
    if (OB_FAIL(search_ctx.create_op(op_param, non_ror_scan_op))) {
      LOG_WARN("failed to create scan op", K(ret));
    } else if (OB_ISNULL(non_ror_scan_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret));
    } else if (!really_need_rowkey_order_ && !ctdef->is_search_index_) {
      op = static_cast<ObIDASSearchOp *>(non_ror_scan_op);
    } else {
      // Route by the physical RowID contract of the created scan, not by the
      // scalar's logical origin. A direct nested scalar flattened by the nested
      // boundary is a normal BASE/uint64 scan and can reuse Bitmap. A subtree
      // that still needs nested-element identity is created in NESTED/COMPACT
      // scope and therefore keeps the Sort path.
      const bool route_to_bitmap =
          non_ror_scan_op->get_rowid_type() == DAS_ROWID_TYPE_UINT64;
      if (route_to_bitmap) {
        ObDASBitmapOpParam bitmap_op_param(non_ror_scan_op, ctdef, this);
        ObDASBitmapOp *bitmap_op = nullptr;
        bitmap_op_param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(search_ctx.create_op(bitmap_op_param, bitmap_op))) {
          LOG_WARN("failed to create bitmap op", K(ret));
        } else if (OB_ISNULL(bitmap_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret));
        } else {
          op = static_cast<ObIDASSearchOp *>(bitmap_op);
        }
      } else {
        const bool use_unique_sort = ctdef->is_wrap_with_unique_sort_;
        ObDASSortOpParam sort_op_param(non_ror_scan_op, ctdef, this, use_unique_sort);
        ObDASSortOp *sort_op = nullptr;
        sort_op_param.set_is_scoring(ctdef->is_scoring());
        if (OB_FAIL(search_ctx.create_op(sort_op_param, sort_op))) {
          LOG_WARN("failed to create sort op", K(ret));
        } else if (OB_ISNULL(sort_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret));
        } else {
          op = static_cast<ObIDASSearchOp *>(sort_op);
        }
      }
    }
  }
  return ret;
}

    
} // namespace sql
} // namespace oceanbase
