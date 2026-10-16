/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_utils.h"
#include "share/logical_table/ob_logical_table_rewrite_ctx.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"
#include "share/vector/ob_continuous_base.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

// Gate predicate for runtime logical-table key-range rewrite on scan/rescan/advance_scan.
// True iff: ctdef is a logical-table ctdef AND op-level lt_ctx is enabled AND
// ObDASScanOp::init_scan_param did NOT already produce the physical KV ranges
// (direct_phys_ranges_built_ == false) AND either CG didn't bake the KV range builder on the
// ctdef (lt_ctx_.is_enabled_ == false, e.g. FTS aux or lookup ctdef) or lt_rt already has
// expr-evaluated ns/lt (hybrid aux / plan-cache rescan).
//
// direct_phys_ranges_built_ marks the direct-SQL primary scan: build_primary_ranges() already
// turned the filter-derived ns/lt/rowid into a physical 2-column (ns_id, kv_key) range. Lookup
// paths never run build_primary_ranges(), so they leave the flag false and their logical
// 1-column rowid ranges still get wrapped here. Using a compile-time ns_id_expr_ != nullptr test
// instead is wrong: the lookup main-table ctdef of an FTS/vec query with explicit
// namespace_id/ltable_id predicates ALSO carries ns_id_expr_, yet its ranges are still logical.
static inline bool need_session_lt_range_rewrite(const ObDASScanCtDef *ctdef,
                                                 const ObLogicalTableRtDef &lt_rt,
                                                 const ObLogicalTableScanRtDef &lt_scan_rt)
{
  return OB_NOT_NULL(ctdef) && ctdef->is_logical_table()
      && lt_rt.enabled_
      && !lt_scan_rt.direct_phys_ranges_built_
      && (!ctdef->lt_ctx_.analyzed_ || lt_rt.has_resolved_ns_lt());
}

static inline bool need_session_lt_range_rewrite(const ObDASScalarScanCtDef *ctdef,
                                                 const ObLogicalTableRtDef &lt_rt,
                                                 const ObLogicalTableScanRtDef &lt_scan_rt)
{
  return OB_NOT_NULL(ctdef) && ctdef->is_logical_table()
      && lt_rt.enabled_
      && !lt_scan_rt.direct_phys_ranges_built_
      && (!ctdef->lt_ctx_.analyzed_ || lt_rt.has_resolved_ns_lt());
}

static inline bool is_das_remote_executor()
{
  bool is_remote = false;
  ObDiagnosticInfo *di = ObLocalDiagnosticInfo::get();
  if (OB_NOT_NULL(di)) {
    is_remote = di->get_ash_stat().in_das_remote_exec_;
  }
  return is_remote;
}

static inline bool need_eval_lt_residual_filters(const ObDASScanCtDef *scan_ctdef)
{
  bool need_eval = false;
  if (OB_NOT_NULL(scan_ctdef)
      && scan_ctdef->is_logical_table()
      && !scan_ctdef->logical_residual_filters_.empty()) {
    // FTS inv/fwd aux: CG mirrors pushdown filters into logical_residual_filters_, but
    // logical_output_exprs_ only covers FTS columns (token/rowid), not namespace_id/ltable_id.
    // Evaluating residual here reads unprojected datums and SIGSEGVs. ns/lt scope is already
    // enforced by KV key-prefix range rewrite in try_session_lt_range_rewrite_.
    if (share::LT_AUX_FTS_INVERTED == scan_ctdef->logical_table_aux_type_
        || share::LT_AUX_FTS_FORWARD == scan_ctdef->logical_table_aux_type_) {
      need_eval = false;
    // Plain primary logical table: residual mirrors TableScan filters. Always eval here:
    // gating on is_das_remote_executor() left a gap where a distributed plan executed
    // locally (QC skips op filters via skip_plain_logical_table_filter_on_qc, and the
    // task never enters the remote DAS executor) evaluated the filter nowhere, so
    // range/IN predicates silently matched the whole logical table. The QC op side now
    // skips whenever logical_residual_filters_ is non-empty, so this is the single
    // evaluation point. logical_output_exprs_ for primary projects all filter-referenced
    // columns (unlike FTS aux above), so local evaluation is safe.
    } else if (share::LT_AUX_PRIMARY == scan_ctdef->logical_table_aux_type_) {
      need_eval = true;
    } else {
      need_eval = true;
    }
  }
  return need_eval;
}

static int ensure_owned_lt_scan_rt_(ObLogicalTableScanRtDef *&owned_lt_scan_rt,
                                    common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(owned_lt_scan_rt)) {
  } else if (OB_ISNULL(owned_lt_scan_rt = OB_NEWx(ObLogicalTableScanRtDef, (&alloc), &alloc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate owned_lt_scan_rt_ failed", K(ret));
  }
  return ret;
}

// Single ctdef-agnostic logical-table range rewrite step.
// Templated to cover ObDASScanCtDef / ObDASScalarScanCtDef (mutually exclusive
// at runtime, dispatched by ObDASScanIter::try_session_lt_range_rewrite_).
template <typename CtDefT>
static int try_rewrite_one_lt_range_(ObTableScanParam &scan_param,
                                     const CtDefT &ctdef,
                                     ObEvalCtx &eval_ctx,
                                     ObLogicalTableRtDef &lt_rt_for_proj,
                                     ObLogicalTableScanCtDef &owned_lt_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObLogicalTableRewriteCtx rewrite_ctx = {};
  rewrite_ctx.namespace_id_     = lt_rt_for_proj.namespace_id_;
  rewrite_ctx.logical_table_id_ = lt_rt_for_proj.logical_table_id_;
  if (OB_FAIL(ObLogicalTableHelper::rewrite_table_scan_param_key_ranges(
          scan_param, ctdef, eval_ctx, eval_ctx.exec_ctx_.get_allocator(),
          rewrite_ctx))) {
    LOG_WARN("[LT hybrid DAS] logical range rewrite failed", K(ret));
  } else {
    lt_rt_for_proj.namespace_id_       = rewrite_ctx.namespace_id_;
    lt_rt_for_proj.logical_table_id_   = rewrite_ctx.logical_table_id_;
    lt_rt_for_proj.enabled_            = rewrite_ctx.is_enabled_;
    owned_lt_scan_ctdef.kv_key_expr_   = rewrite_ctx.kv_key_expr_;
    owned_lt_scan_ctdef.kv_value_expr_ = rewrite_ctx.kv_value_expr_;
  }
  return ret;
}

const ExprFixedArray &ObDASScanIterParam::get_result_output() const
{
  return use_scalar_ctdef_ ? scalar_scan_ctdef_->result_output_ : scan_ctdef_->result_output_;
}

common::ObTableID ObDASScanIterParam::get_ref_table_id() const
{
  return use_scalar_ctdef_ ? scalar_scan_ctdef_->ref_table_id_ : scan_ctdef_->ref_table_id_;
}

bool ObDASScanIterParam::is_external_table() const
{
  return use_scalar_ctdef_ ? false : scan_ctdef_->is_external_table_;
}

void ObDASScanIter::init_lt_from_defs(const ObLogicalTableRtDef &lt_rt,
                                       const ObDASScanCtDef *scan_ctdef_src,
                                       const ObDASScalarScanCtDef *scalar_ctdef_src,
                                       const ObLogicalTableScanRtDef *lt_scan_rt_src,
                                       ObExpr *kv_key_override,
                                       ObExpr *kv_value_override)
{
  // Copy compile-time descriptor base from ctdef.
  if (OB_NOT_NULL(scan_ctdef_src)) {
    owned_lt_scan_ctdef_ = scan_ctdef_src->lt_ctx_;
  } else if (OB_NOT_NULL(scalar_ctdef_src)) {
    owned_lt_scan_ctdef_ = scalar_ctdef_src->lt_ctx_;
  }
  // Override kv exprs if provided (e.g., from lookup ctdef access_exprs).
  if (OB_NOT_NULL(kv_key_override)) {
    owned_lt_scan_ctdef_.kv_key_expr_ = kv_key_override;
  }
  if (OB_NOT_NULL(kv_value_override)) {
    owned_lt_scan_ctdef_.kv_value_expr_ = kv_value_override;
  }
  // Populate runtime identity from lt_rt.
  lt_rt_for_proj_.namespace_id_     = lt_rt.namespace_id_;
  lt_rt_for_proj_.logical_table_id_ = lt_rt.logical_table_id_;
  lt_rt_for_proj_.enabled_          = lt_rt.enabled_;
  // Propagate scan-rt state.
  if (OB_NOT_NULL(lt_scan_rt_src) && OB_NOT_NULL(eval_ctx_)) {
    if (OB_SUCCESS == ensure_owned_lt_scan_rt_(
            owned_lt_scan_rt_, eval_ctx_->exec_ctx_.get_allocator())
        && OB_NOT_NULL(owned_lt_scan_rt_)) {
      owned_lt_scan_rt_->direct_phys_ranges_built_ = lt_scan_rt_src->direct_phys_ranges_built_;
    }
  }
}

int ObDASScanIter::mark_direct_phys_ranges_built(const bool built)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval ctx is null", K(ret));
  } else if (OB_FAIL(ensure_owned_lt_scan_rt_(
      owned_lt_scan_rt_,
      eval_ctx_->exec_ctx_.get_allocator()))) {
    LOG_WARN("fail to ensure owned logical table scan rtdef", K(ret));
  } else if (OB_ISNULL(owned_lt_scan_rt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("owned logical table scan rtdef is null", K(ret));
  } else {
    owned_lt_scan_rt_->direct_phys_ranges_built_ = built;
  }
  return ret;
}

int ObDASScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObDASScanIterParam &scan_iter_param = static_cast<ObDASScanIterParam&>(param);
  if (param.type_ != ObDASIterType::DAS_ITER_SCAN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param), K(ret));
  } else {
    output_ = &scan_iter_param.get_result_output();
    scan_ctdef_ = scan_iter_param.use_scalar_ctdef_ ? nullptr : scan_iter_param.scan_ctdef_;
    scalar_scan_ctdef_ = scan_iter_param.use_scalar_ctdef_ ? scan_iter_param.scalar_scan_ctdef_ : nullptr;
    tsc_service_ = is_virtual_table(scan_iter_param.get_ref_table_id()) ? GCTX.vt_par_ser_
                              : scan_iter_param.is_external_table() ? GCTX.et_access_service_
                                                               : MTL(ObAccessService *);
  }

  // Allocate owned_lt_scan_rt_ with a real allocator so path_cache_ is usable.
  // Only for logical-table ctdefs; plain table scans keep nullptr (§1.5 non-LT path).
  // init_lt_from_defs() may also lazy-allocate if this path was skipped.
  if (OB_SUCC(ret) && OB_NOT_NULL(eval_ctx_)) {
    const bool need_owned_lt_scan_rt =
        (OB_NOT_NULL(scan_ctdef_) && scan_ctdef_->is_logical_table())
        || (OB_NOT_NULL(scalar_scan_ctdef_) && scalar_scan_ctdef_->is_logical_table());
    if (need_owned_lt_scan_rt
        && OB_FAIL(ensure_owned_lt_scan_rt_(
                owned_lt_scan_rt_, eval_ctx_->exec_ctx_.get_allocator()))) {
      LOG_WARN("allocate owned_lt_scan_rt_ in inner_init failed", K(ret));
    }
  }

  // Populate owned_lt_scan_ctdef_ / lt_rt_for_proj_ for new-signature project_logical_columns.
  // For self-managed logical aux (IVF/FTS on ObDASScanIter), bypass the op-level lt_ctx_
  // and read identity directly from scan_iter_param + scan_ctdef_/scalar_scan_ctdef_.
  // For op-driven path (init_lt_from_defs), this block is skipped because the param flag
  // self_managed_logical_lt_ctx_ is false; init_lt_from_defs has already populated fields.
  if (OB_SUCC(ret) && OB_NOT_NULL(eval_ctx_)
      && scan_iter_param.self_managed_logical_lt_ctx_
      && (OB_NOT_NULL(scan_ctdef_) || OB_NOT_NULL(scalar_scan_ctdef_))) {
    if (OB_NOT_NULL(scan_ctdef_)) {
      owned_lt_scan_ctdef_ = scan_ctdef_->lt_ctx_;
    } else if (OB_NOT_NULL(scalar_scan_ctdef_)) {
      owned_lt_scan_ctdef_ = scalar_scan_ctdef_->lt_ctx_;
    }
    // kv exprs directly from access_exprs_[0/1] (CG guarantees [kv_key, kv_value]).
    // Same values the old self-managed block used to copy onto lt_ctx_->kv_*_expr_.
    const ExprFixedArray &acs = (nullptr != scan_ctdef_)
        ? scan_ctdef_->pd_expr_spec_.access_exprs_
        : scalar_scan_ctdef_->pd_expr_spec_.access_exprs_;
    if (acs.count() >= 2) {
      owned_lt_scan_ctdef_.kv_key_expr_   = acs.at(0);
      owned_lt_scan_ctdef_.kv_value_expr_ = acs.at(1);
    }
    if (scan_iter_param.logical_ivf_vec_project_dim_ > 0) {
      owned_lt_scan_ctdef_.ivf_vec_project_dim_ =
          scan_iter_param.logical_ivf_vec_project_dim_;
    }
    // ns/lt directly from param (bypass lt_ctx_).
    uint64_t ns_id = scan_iter_param.namespace_id_;
    uint64_t lt_id = scan_iter_param.ltable_id_;
    if (share::OB_INVALID_NAMESPACE_ID == ns_id
        || share::OB_INVALID_LOGICAL_TABLE_ID == lt_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid DAS] logical table self-managed scan iter requires valid "
               "namespace_id and ltable_id",
               K(ret), K(ns_id), K(lt_id),
               K(scan_iter_param.namespace_id_),
               K(scan_iter_param.ltable_id_));
    } else {
      lt_rt_for_proj_.namespace_id_     = ns_id;
      lt_rt_for_proj_.logical_table_id_ = lt_id;
      lt_rt_for_proj_.enabled_          = true;
      LOG_TRACE("[LT hybrid DAS] ObDASScanIter self-managed logical lt_ctx inited",
               KP(scan_ctdef_),
               "ir_scan_type",
               scan_ctdef_ != nullptr ? static_cast<int32_t>(scan_ctdef_->ir_scan_type_) : -1,
               K(scan_iter_param.self_managed_logical_lt_ctx_),
               "ivf_vec_project_dim", owned_lt_scan_ctdef_.ivf_vec_project_dim_,
               "access_expr_cnt", acs.count(),
               KP(owned_lt_scan_ctdef_.kv_key_expr_),
               KP(owned_lt_scan_ctdef_.kv_value_expr_));
    }
  }

  return ret;
}

int ObDASScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  // NOTE: need_switch_param_ should have been set before call reuse().
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else if (OB_FAIL(tsc_service_->reuse_scan_iter(scan_param_->need_switch_param_, result_))) {
    LOG_WARN("failed to reuse storage scan iter", K(ret));
  } else {
    scan_param_->key_ranges_.reuse();
    scan_param_->ss_key_ranges_.reuse();
    scan_param_->mbr_filters_.reuse();
    scan_param_->scan_tasks_.reuse();
  }
  return ret;
}

int ObDASScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_)) {
    if (OB_FAIL(tsc_service_->revert_scan_iter(result_))) {
      LOG_WARN("failed to revert storage scan iter", K(ret));
    }
    result_ = nullptr;
  }
  return ret;
}

int ObDASScanIter::try_session_lt_range_rewrite_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(eval_ctx_)
      || scan_param_->key_ranges_.empty()) {
    // no rewrite needed
  } else if (OB_NOT_NULL(owned_lt_scan_rt_)
             && need_session_lt_range_rewrite(scan_ctdef_, lt_rt_for_proj_, *owned_lt_scan_rt_)) {
    ret = try_rewrite_one_lt_range_(*scan_param_, *scan_ctdef_, *eval_ctx_,
                                    lt_rt_for_proj_, owned_lt_scan_ctdef_);
  } else if (OB_NOT_NULL(owned_lt_scan_rt_)
             && need_session_lt_range_rewrite(scalar_scan_ctdef_, lt_rt_for_proj_, *owned_lt_scan_rt_)) {
    ret = try_rewrite_one_lt_range_(*scan_param_, *scalar_scan_ctdef_, *eval_ctx_,
                                    lt_rt_for_proj_, owned_lt_scan_ctdef_);
  }
  return ret;
}

int ObDASScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else if (OB_UNLIKELY(nullptr != result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not null result iter ptr before do table scan", K(ret), KP_(result));
  } else {
    if (is_vec_pre_filtering_timeout_set()) {
      vec_start_scan_ts_us_ = common::ObClockGenerator::getClock();
    }
    // 1) logical table session-level range rewrite (independent step;
    //    must NOT short-circuit the storage table_scan below).
    if (OB_FAIL(try_session_lt_range_rewrite_())) {
      // already logged inside helper
    } else if (OB_FAIL(tsc_service_->table_scan(*scan_param_, result_))) {
      // 2) always issue the storage table_scan so that result_ gets initialized.
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param_->fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", KPC_(scan_param), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(scan_ctdef_) && scan_ctdef_->is_logical_table()
      && lt_rt_for_proj_.enabled_) {
    LOG_TRACE("[LT hybrid DAS] ObDASScanIter::do_table_scan after logical rewrite + table_scan ok",
             "ir_scan_type", static_cast<int32_t>(scan_ctdef_->ir_scan_type_),
             K(scan_param_->tablet_id_),
             "phys_key_range_cnt", scan_param_->key_ranges_.count(),
             K(scan_ctdef_->physical_data_table_id_),
             K(scan_ctdef_->ref_table_id_),
             "logical_mapping_cnt", scan_ctdef_->logical_column_mapping_.count(),
             "logical_output_cnt", scan_ctdef_->logical_output_exprs_.count());
  }
  LOG_DEBUG("[DAS ITER] scan iter do table scan", KPC_(scan_param), K(ret));

  return ret;
}

int ObDASScanIter::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else {
    if (is_vec_pre_filtering_timeout_set()) {
      vec_start_scan_ts_us_ = common::ObClockGenerator::getClock();
    }
    // 1) logical table session-level range rewrite (independent step;
    //    must NOT short-circuit the storage table_rescan below).
    if (OB_FAIL(try_session_lt_range_rewrite_())) {
      // already logged inside helper
    } else if (OB_FAIL(tsc_service_->table_rescan(*scan_param_, result_))) {
      // 2) always issue the storage table_rescan.
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param_->fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      }
      LOG_WARN("failed to rescan tablet", K(scan_param_->tablet_id_), K(ret));
    } else {
      // reset need_switch_param_ after real rescan.
      scan_param_->need_switch_param_ = false;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(scan_ctdef_) && scan_ctdef_->is_logical_table()
      && lt_rt_for_proj_.enabled_) {
    LOG_TRACE("[LT hybrid DAS] ObDASScanIter::rescan after logical rewrite + table_rescan ok",
             "ir_scan_type", static_cast<int32_t>(scan_ctdef_->ir_scan_type_),
             K(scan_param_->tablet_id_),
             "phys_key_range_cnt", scan_param_->key_ranges_.count(),
             K(scan_ctdef_->physical_data_table_id_),
             K(scan_ctdef_->ref_table_id_));
  }
  LOG_DEBUG("[DAS ITER] das scan iter rescan", KPC_(scan_param), K(ret));

  return ret;
}

int ObDASScanIter::advance_scan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan param", K(ret));
  } else {
    // 1) logical table session-level range rewrite (independent step;
    //    must NOT short-circuit the storage table_advance_scan below).
    if (OB_FAIL(try_session_lt_range_rewrite_())) {
      // already logged inside helper
    } else if (OB_FAIL(tsc_service_->table_advance_scan(*scan_param_, result_))) {
      // 2) always issue the storage table_advance_scan.
      LOG_WARN("failed to advance scan tablet", K(scan_param_->tablet_id_), K(ret));
    }
  }
  return ret;
}

int ObDASScanIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(scan_param_ != nullptr? scan_param_->index_id_ : 0);

  LOG_DEBUG("ObDASScanIter::inner_get_next_row entry",
           "lt_enabled", lt_rt_for_proj_.enabled_);
  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else if (OB_FAIL(result_->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (is_vec_pre_filtering_timeout_set() && OB_FAIL(try_check_vec_pre_filter_status())) {
    if (ret != OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY) {
      LOG_WARN("failed to try check vector pre-filter timeout", K(ret));
    }
  }
  // single-row variant must also project logical columns
  if (OB_SUCC(ret) && lt_rt_for_proj_.enabled_) {
    if (OB_FAIL(lt_project_filter_row_with_repull_(ret))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to project logical columns", K(ret));
      }
    }
  }
  return ret;
}

// A multi-ltable primary range crosses the index and auxiliary key spaces between
// logical-table IDs. Remove those physical rows before logical-column projection.
bool ObDASScanIter::need_filter_mixed_primary_rows_() const
{
  return lt_rt_for_proj_.enabled_
      && OB_NOT_NULL(scan_ctdef_)
      && share::LT_AUX_PRIMARY == scan_ctdef_->logical_table_aux_type_
      && owned_lt_scan_ctdef_.lt_id_start_expr_ != owned_lt_scan_ctdef_.lt_id_end_expr_;
}

int ObDASScanIter::append_unique_physical_expr_(
    ObExpr *expr,
    common::ObIArray<ObExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical compact expr is null", K(ret));
  } else {
    for (int64_t i = 0; !found && i < exprs.count(); ++i) {
      found = (exprs.at(i) == expr);
    }
    if (!found && OB_FAIL(exprs.push_back(expr))) {
      LOG_WARN("append physical compact expr failed", K(ret));
    }
  }
  return ret;
}

int ObDASScanIter::compact_continuous_vector_(
    common::ObIVector &vector,
    const ObBitVector &skip,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  int64_t write_idx = 0;
  if (OB_UNLIKELY(VEC_CONTINUOUS != vector.get_format())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compact vector is not continuous", K(ret), K(vector.get_format()));
  } else {
    ObContinuousBase &continuous = static_cast<ObContinuousBase &>(vector);
    uint32_t *offsets = continuous.get_offsets();
    char *data = continuous.get_data();
    uint32_t write_offset = offsets[0];
    for (int64_t read_idx = 0; read_idx < row_count; ++read_idx) {
      const bool is_null = vector.is_null(read_idx);
      const uint32_t read_offset = offsets[read_idx];
      const uint32_t read_end = offsets[read_idx + 1];
      if (!skip.at(read_idx)) {
        if (is_null) {
          vector.set_null(write_idx);
        } else {
          const uint32_t length = read_end - read_offset;
          if (write_offset != read_offset && length > 0) {
            MEMMOVE(data + write_offset, data + read_offset, length);
          }
          vector.unset_null(write_idx);
          write_offset += length;
        }
        offsets[write_idx + 1] = write_offset;
        ++write_idx;
      }
    }
  }
  return ret;
}

int ObDASScanIter::compact_physical_expr_(
    ObExpr &expr,
    const ObBitVector &skip,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  int64_t write_idx = 0;
  if (!expr.is_batch_result() || row_count <= 0) {
  } else if (expr.enable_rich_format() && is_valid_format(expr.get_format(*eval_ctx_))) {
    ObIVector *vector = expr.get_vector(*eval_ctx_);
    if (OB_ISNULL(vector)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical compact vector is null", K(ret), K(row_count));
    } else if (VEC_CONTINUOUS == vector->get_format()) {
      if (OB_FAIL(compact_continuous_vector_(*vector, skip, row_count))) {
        LOG_WARN("compact continuous physical vector failed", K(ret), K(row_count));
      }
    } else {
      for (int64_t read_idx = 0; read_idx < row_count; ++read_idx) {
        if (!skip.at(read_idx)) {
          if (write_idx != read_idx) {
            if (vector->is_null(read_idx)) {
              vector->set_null(write_idx);
            } else {
              const char *payload = nullptr;
              ObLength length = 0;
              vector->get_payload(read_idx, payload, length);
              vector->set_payload_shallow(write_idx, payload, length);
            }
          }
          ++write_idx;
        }
      }
    }
  } else {
    ObDatum *datums = expr.locate_batch_datums(*eval_ctx_);
    for (int64_t read_idx = 0; read_idx < row_count; ++read_idx) {
      if (!skip.at(read_idx)) {
        if (write_idx != read_idx) {
          datums[write_idx] = datums[read_idx];
        }
        ++write_idx;
      }
    }
  }
  return ret;
}

int ObDASScanIter::compact_physical_exprs_(
    const ObBitVector &skip,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  int64_t kept_count = 0;
  common::ObSEArray<ObExpr *, 8> compact_exprs;
  const ExprFixedArray &access_exprs = scan_ctdef_->pd_expr_spec_.access_exprs_;
  const ExprFixedArray &result_output = scan_ctdef_->result_output_;
  for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
    if (OB_FAIL(append_unique_physical_expr_(access_exprs.at(i), compact_exprs))) {
      LOG_WARN("append physical access expr failed", K(ret), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < result_output.count(); ++i) {
    if (OB_FAIL(append_unique_physical_expr_(result_output.at(i), compact_exprs))) {
      LOG_WARN("append physical result expr failed", K(ret), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < compact_exprs.count(); ++i) {
    if (OB_FAIL(compact_physical_expr_(*compact_exprs.at(i), skip, row_count))) {
      LOG_WARN("compact physical expr failed", K(ret), K(i), K(row_count));
    }
  }
  for (int64_t i = 0; i < row_count; ++i) {
    kept_count += skip.at(i) ? 0 : 1;
  }
  if (OB_SUCC(ret)) {
    row_count = kept_count;
  }
  return ret;
}

int ObDASScanIter::filter_mixed_primary_rows_(int64_t &row_count)
{
  int ret = OB_SUCCESS;
  ObExpr *key_expr = owned_lt_scan_ctdef_.kv_key_expr_;
  if (row_count <= 0 || !need_filter_mixed_primary_rows_()) {
  } else if (OB_ISNULL(eval_ctx_) || OB_ISNULL(key_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mixed primary row filter state", K(ret), KP(eval_ctx_), KP(key_expr));
  } else if (scan_ctdef_->pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot filter mixed primary rows after storage aggregation", K(ret));
  } else {
    common::ObArenaAllocator filter_alloc(
        "LTPhysFilter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    common::ObArenaAllocator key_scratch_alloc(
        "LTPhysKey", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    void *skip_buf = filter_alloc.alloc(ObBitVector::memory_size(row_count));
    bool has_filtered = false;
    const uint64_t expected_index_seq = common::OB_INVALID_ID != scan_ctdef_->logical_index_seq_
        ? scan_ctdef_->logical_index_seq_ : scan_ctdef_->ref_table_id_;
    bool key_is_null = false;
    ObString key;
    share::ObLogicalTableKeyPrefix prefix;
    int64_t key_pos = 0;
    if (OB_ISNULL(skip_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate mixed primary skip vector failed", K(ret), K(row_count));
    } else {
      ObBitVector &skip = *to_bit_vector(skip_buf);
      skip.init(static_cast<uint32_t>(row_count));
      skip.reset(row_count);
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
        key_is_null = false;
        key.reset();
        prefix.reset();
        key_pos = 0;
        if (OB_FAIL(ObLogicalTableHelper::read_kv_string_at_row(
                *key_expr, *eval_ctx_, row_idx, key_scratch_alloc, key_is_null, key))) {
          LOG_WARN("read key for mixed primary row filter failed", K(ret), K(row_idx));
        } else if (OB_UNLIKELY(key_is_null || key.empty())) {
          ret = OB_INVALID_DATA;
          LOG_WARN("key for mixed primary row filter is empty",
                   K(ret), K(row_idx), K(key_is_null));
        } else if (OB_FAIL(prefix.decode(key.ptr(), key.length(), key_pos))) {
          LOG_WARN("decode key prefix for mixed primary row filter failed",
                   K(ret), K(row_idx), K(key.length()));
        } else if (prefix.index_seq_ != expected_index_seq
                   || share::LT_AUX_PRIMARY != prefix.aux_type_) {
          skip.set(row_idx);
          has_filtered = true;
        }
        key_scratch_alloc.reuse();
      }
      if (OB_SUCC(ret) && has_filtered
          && OB_FAIL(compact_physical_exprs_(skip, row_count))) {
        LOG_WARN("compact mixed primary physical rows failed", K(ret), K(row_count));
      }
    }
  }
  return ret;
}

// Decode logical-table columns (kv_value JSON projection + residual filters) for
// the current batch of count rows (count == 1 for the single-row variant).
// Prefix/residual filters may drop rows in place; all_filtered is set when every
// row was dropped (count reset to 0), letting callers decide whether to re-pull.
int ObDASScanIter::lt_project_filter_current_(int64_t &count, int in_ret, bool &all_filtered)
{
  int ret = in_ret;
  all_filtered = false;
  // OB_ITER_END with count>0 is the vectorized "last batch + EOF" pattern;
  // we still need to project the rows in this batch and preserve OB_ITER_END.
  if (lt_rt_for_proj_.enabled_
      && OB_NOT_NULL(scan_ctdef_) && count > 0) {
    const int saved_ret = ret;
    ExprFixedArray &logical_output_exprs =
        const_cast<ExprFixedArray&>(scan_ctdef_->logical_output_exprs_);
    LOG_TRACE("[LT hybrid DAS] logical table scan got rows from storage",
            K(count), K(ret), K(lt_rt_for_proj_.enabled_),
            K(scan_ctdef_->logical_output_exprs_.count()),
            K(scan_ctdef_->logical_column_mapping_.count()),
            KP(owned_lt_scan_ctdef_.kv_key_expr_), KP(owned_lt_scan_ctdef_.kv_value_expr_),
            K(scan_ctdef_->ref_table_id_), K(scan_ctdef_->physical_data_table_id_));
    if (OB_FAIL(filter_mixed_primary_rows_(count))) {
      LOG_WARN("filter mixed primary rows failed", K(ret), K(count));
    } else if (0 == count) {
      ret = saved_ret;
    } else if (!logical_output_exprs.empty()) {
      LOG_TRACE("[LT hybrid DAS] lt_project_filter_current_ invoking project_logical_columns",
              K(count), K(ret), K(logical_output_exprs.count()),
              KP(owned_lt_scan_ctdef_.kv_key_expr_), KP(owned_lt_scan_ctdef_.kv_value_expr_));
      int64_t log_cnt = logical_output_exprs.count();
      int64_t res_cnt = scan_ctdef_->result_output_.count();
      LOG_TRACE("[LT hybrid DAS] M5 logical vs result output ptrs",
              "logical_0", log_cnt > 0 ? static_cast<void *>(logical_output_exprs.at(0)) : nullptr,
              "logical_1", log_cnt > 1 ? static_cast<void *>(logical_output_exprs.at(1)) : nullptr,
              "result_0", res_cnt > 0 ? static_cast<void *>(scan_ctdef_->result_output_.at(0)) : nullptr,
              "result_1", res_cnt > 1 ? static_cast<void *>(scan_ctdef_->result_output_.at(1)) : nullptr,
              K(log_cnt), K(res_cnt),
              "lt_aux_type", static_cast<int>(scan_ctdef_->logical_table_aux_type_));
      const int proj_ret = ObLogicalTableHelper::project_logical_columns(
          owned_lt_scan_ctdef_, lt_rt_for_proj_,
          *scan_ctdef_, logical_output_exprs, count, *eval_ctx_,
          owned_lt_scan_rt_);
      if (OB_SUCCESS != proj_ret) {
        ret = proj_ret;
        LOG_WARN("[LT hybrid DAS] project logical columns failed", K(proj_ret), K(count));
      } else if (need_eval_lt_residual_filters(scan_ctdef_)
                && OB_FAIL(ObLogicalTableHelper::eval_logical_residual_filters(
                    *scan_ctdef_, count, *eval_ctx_))) {
        LOG_WARN("[LT hybrid DAS] eval logical residual filters failed", K(ret), K(count));
      } else {
        ret = saved_ret;  // preserve OB_ITER_END for upper layer
      }
    } else if (lt_rt_for_proj_.enabled_) {
      LOG_WARN("[LT hybrid DAS] lt_ctx enabled but logical_output_exprs_ empty, "
              "skip projection (may cause wrong/null output datums)",
              K(count), K(ret), K(scan_ctdef_->ref_table_id_));
      ret = saved_ret;
    }
    all_filtered = (0 == count);
  } else if (lt_rt_for_proj_.enabled_
            && OB_NOT_NULL(scalar_scan_ctdef_) && scalar_scan_ctdef_->is_logical_table()
            && count > 0) {
    ExprFixedArray &logical_output_exprs =
        const_cast<ExprFixedArray&>(scalar_scan_ctdef_->logical_output_exprs_);
    if (!logical_output_exprs.empty()) {
      LOG_TRACE("[LT hybrid DAS] lt_project_filter_current_ (scalar) project_logical_columns",
              K(count), K(ret), K(logical_output_exprs.count()),
              KP(owned_lt_scan_ctdef_.kv_key_expr_), KP(owned_lt_scan_ctdef_.kv_value_expr_),
              K(scalar_scan_ctdef_->ref_table_id_));
      const int saved_ret = ret;
      const int proj_ret = ObLogicalTableHelper::project_logical_columns(
          owned_lt_scan_ctdef_, lt_rt_for_proj_,
          scalar_scan_ctdef_->logical_column_mapping_,
          ObTSCIRScanType::OB_NOT_A_SPEC_SCAN,
          scalar_scan_ctdef_->logical_table_aux_type_,
          logical_output_exprs, count, *eval_ctx_,
          owned_lt_scan_rt_,
          nullptr,
          scalar_scan_ctdef_->pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown());
      if (OB_SUCCESS != proj_ret) {
        ret = proj_ret;
        LOG_WARN("[LT hybrid DAS] scalar project logical columns failed",
                K(proj_ret), K(count));
      } else {
        ret = saved_ret;
      }
    }
  }
  return ret;
}

// LT post-processing for the row fetched by inner_get_next_row. LT prefix or
// residual filters may drop the fetched row; only then keep pulling the next
// row so a filtered row does not prematurely end the whole scan. All other
// paths (non-logical tables, or LT without prefix/residual filters) run exactly one
// pass with the original semantics.
int ObDASScanIter::lt_project_filter_row_with_repull_(int in_ret)
{
  int ret = in_ret;
  const bool may_drop_row = lt_rt_for_proj_.enabled_
      && OB_NOT_NULL(scan_ctdef_)
      && (need_filter_mixed_primary_rows_()
          || (!scan_ctdef_->logical_output_exprs_.empty()
              && need_eval_lt_residual_filters(scan_ctdef_)));
  bool pull_next = true;
  while (pull_next && OB_SUCC(ret)) {
    pull_next = false;
    int64_t row_count = 1;
    bool all_filtered = false;
    ret = lt_project_filter_current_(row_count, ret, all_filtered);
    if (OB_SUCC(ret) && all_filtered) {
      pull_next = may_drop_row;  // row dropped by prefix/residual filters; pull the next one
    }
    if (pull_next) {
      clear_evaluated_flag();
      if (OB_FAIL(result_->get_next_row())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (is_vec_pre_filtering_timeout_set() && OB_FAIL(try_check_vec_pre_filter_status())) {
        if (ret != OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY) {
          LOG_WARN("failed to try check vector pre-filter timeout", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDASScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(scan_param_ != nullptr ? scan_param_->index_id_ : 0);

  LOG_DEBUG("ObDASScanIter::inner_get_next_rows entry",
           K(capacity),
           "lt_enabled", lt_rt_for_proj_.enabled_);
  if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan iter", K(ret));
  } else {
    ret = result_->get_next_rows(count, capacity);
  }
  if (OB_FAIL(ret)) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (is_vec_pre_filtering_timeout_set() && OB_FAIL(try_check_vec_pre_filter_status(count))) {
    if (ret != OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY) {
      LOG_WARN("failed to try check vector pre-filter timeout", K(ret), K(count));
    }
  }
  // decode logical table columns from kv_value JSON
  if ((ret == OB_SUCCESS || ret == OB_ITER_END) && lt_rt_for_proj_.enabled_) {
    if (OB_FAIL(lt_project_filter_rows_with_repull_(count, capacity, ret))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to project logical columns", K(ret));
      }
    }
  }
  LOG_TRACE("[DAS ITER] scan iter get next rows", K(count), K(capacity), KPC_(scan_param), K(ret));
  if (OB_SUCC(ret) && !is_skip_vectorized_print()) {
    const ObBitVector *skip = nullptr;
    PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_, count, skip);
  }
  return ret;
}

// LT post-processing for the batch fetched by inner_get_next_rows. LT prefix or
// residual filters may compact an entire storage batch to zero rows; only then
// keep pulling the next storage batch until a non-empty batch, ITER_END, or
// error, so DAS consumers never observe (0 rows, OB_SUCCESS). All other paths
// (non-logical tables, or LT without prefix/residual filters) run exactly one pass
// with the original semantics.
int ObDASScanIter::lt_project_filter_rows_with_repull_(int64_t &count, int64_t capacity, int in_ret)
{
  int ret = in_ret;
  const bool may_compact_to_empty = lt_rt_for_proj_.enabled_
      && OB_NOT_NULL(scan_ctdef_)
      && (need_filter_mixed_primary_rows_()
          || (!scan_ctdef_->logical_output_exprs_.empty()
              && need_eval_lt_residual_filters(scan_ctdef_)));
  bool pull_next = true;
  while (pull_next && (OB_SUCCESS == ret || OB_ITER_END == ret)) {
    pull_next = false;
    bool all_filtered = false;
    ret = lt_project_filter_current_(count, ret, all_filtered);
    if (may_compact_to_empty && OB_SUCCESS == ret && all_filtered) {
      // The whole batch was compacted by prefix/residual filters; pull the next batch.
      ret = result_->get_next_rows(count, capacity);
      if (OB_FAIL(ret)) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (is_vec_pre_filtering_timeout_set() && OB_FAIL(try_check_vec_pre_filter_status(count))) {
        if (ret != OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY) {
          LOG_WARN("failed to try check vector pre-filter timeout", K(ret), K(count));
        }
      }
      pull_next = true;
    }
  }
  return ret;
}

void ObDASScanIter::clear_evaluated_flag()
{
  OB_ASSERT(nullptr != scan_param_);
  if (OB_NOT_NULL(scan_param_->op_)) {
    scan_param_->op_->clear_evaluated_flag();
  }
}

int ObDASScanIter::set_scan_rowkey(ObEvalCtx *eval_ctx,
                                   const ObIArray<ObExpr *> &rowkey_exprs,
                                   const ObDASScanCtDef *lookup_ctdef,
                                   ObIAllocator *alloc,
                                   int64_t group_id)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  if (OB_ISNULL(eval_ctx) || OB_UNLIKELY(rowkey_exprs.empty()) || OB_ISNULL(lookup_ctdef) || OB_ISNULL(alloc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid eval ctx, rowkey exprs, lookup ctdef, or allocator",
             K(eval_ctx), K(rowkey_exprs), K(lookup_ctdef), K(alloc), K(ret));
  } else {
    // For logical table lookup, build a simple uint64 rowid range here.
    // The physical KV key encoding is handled by rewrite_table_scan_param_key_ranges in rescan.
    ObObj *obj_ptr = nullptr;
    void *buf = nullptr;
    int64_t rowkey_cnt = rowkey_exprs.count();
    if (OB_ISNULL(buf = alloc->alloc(sizeof(ObObj) * rowkey_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(rowkey_cnt), K(ret));
    } else {
      obj_ptr = new (buf) ObObj[rowkey_cnt];
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
      ObObj tmp_obj;
      const ObExpr *expr = rowkey_exprs.at(i);
      ObDatum col_datum;
      if (expr->enable_rich_format() && is_valid_format(expr->get_format(*eval_ctx))) {
        const int64_t batch_idx = eval_ctx->get_batch_idx();
        if (OB_FAIL(ObDASSearchUtils::get_datum(*expr, *eval_ctx, batch_idx, col_datum))) {
          LOG_WARN("failed to get datum", K(ret));
        }
      } else {
        col_datum = expr->locate_expr_datum(*eval_ctx);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(T_PSEUDO_GROUP_ID == expr->type_ || T_PSEUDO_ROW_TRANS_INFO_COLUMN == expr->type_)) {
        // skip.
      } else if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("failed to convert datum to obj", K(ret));
      } else if (OB_FAIL(ob_write_obj(*alloc, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("failed to deep copy rowkey", K(ret), K(tmp_obj));
      }
    }

    if (OB_SUCC(ret)) {
      ObRowkey row_key(obj_ptr, rowkey_cnt);
      if (OB_FAIL(range.build_range(lookup_ctdef->ref_table_id_, row_key))) {
        LOG_WARN("failed to build lookup range", K(ret), K(lookup_ctdef->ref_table_id_), K(row_key));
      } else if (FALSE_IT(range.group_idx_ = ObNewRange::get_group_idx(group_id))) {
      } else if (OB_FAIL(scan_param_->key_ranges_.push_back(range))) {
        LOG_WARN("failed to push back lookup range", K(ret));
      } else {
        scan_param_->is_get_ = true;
      }
    }
  }
  LOG_DEBUG("set scan iter scan rowkey", K(range), K(ret));
  return ret;
}

int ObDASScanIter::try_check_vec_pre_filter_status(const int64_t row_count /* default 1 */) 
{
  int ret = OB_SUCCESS;
  uint64_t tmp_try_check_tick_rows = 0;
  try_check_tick_rows_ += row_count;
  ++try_check_tick_;
  bool need_check = false;
  if (try_check_tick_rows_ >= CHECK_STATUS_ROWS) {
    tmp_try_check_tick_rows = try_check_tick_rows_;
    try_check_tick_rows_ = 0;
    need_check = true;
  }
  if (!need_check && (try_check_tick_ % CHECK_STATUS_TRY_TIMES) == 0) {
    need_check = true;
  }
  if (need_check && OB_FAIL(check_vec_pre_filter_status())) {
    LOG_INFO("vector pre-filter search timeout", K(ret), K(tmp_try_check_tick_rows), K(try_check_tick_), K(row_count));
  }
  
  return ret;
}

int ObDASScanIter::check_vec_pre_filter_status()
{
  int ret = OB_SUCCESS;
  if (is_vec_pre_filtering_timeout_set() && OB_UNLIKELY(vec_start_scan_ts_us_ <= 0)) {
    // do nothing, no timeout
    LOG_WARN("vector start scan timestamp is unexpected unset", K(vec_pre_filtering_timeout_us_), K(vec_start_scan_ts_us_));
  } else {
    int64_t cur_ts = common::ObClockGenerator::getClock();
    int64_t timeout_ts = vec_start_scan_ts_us_ + vec_pre_filtering_timeout_us_;
    int64_t timeout_remain = timeout_ts - cur_ts;
    if (timeout_remain <= 0) {
      ret = OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY; // to trigger pre-filter -> post_iteration transition
      LOG_INFO("vector pre-filter scan timeout", K(ret), K(timeout_remain), K(cur_ts), K(vec_start_scan_ts_us_), K(vec_pre_filtering_timeout_us_), K(try_check_tick_));
    }
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase
