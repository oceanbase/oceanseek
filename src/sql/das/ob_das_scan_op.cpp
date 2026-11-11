/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_scan_op.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/das/ob_das_extra_data.h"
#include "storage/concurrency_control/ob_data_validation_service.h"
#include "sql/das/iter/ob_das_iter_utils.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/das/iter/ob_das_index_merge_fts_and_iter.h"
#include "sql/optimizer/ob_storage_estimator.h"
#include "sql/engine/expr/ob_expr_local_dynamic_filter.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_path.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/logical_table/ob_logical_table_range_builder.h"
#include "share/logical_table/ob_logical_table_decoder.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"
#include "share/logical_table/ob_logical_table_codec_utils.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector/type_traits.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_local_lookup_iter.h"
#include "sql/das/iter/ob_das_profile_iter.h"
#include "sql/das/iter/ob_das_func_data_iter.h"
#include "sql/das/iter/ob_das_domain_id_merge_iter.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/ob_operator.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_sqc_ctx.h"

ERRSIM_POINT_DEF(ERRSIM_DAS_ITER_PUBLISH_FAILED,
                "Fail after constructing a DAS tree, before publishing its root.");

namespace oceanbase
{
namespace common
{
namespace serialization
{
template <>
struct EnumEncoder<false, const sql::ObDASScanCtDef *> : sql::DASCtEncoder<sql::ObDASScanCtDef>
{
};

template <>
struct EnumEncoder<false, sql::ObDASScanRtDef *> : sql::DASRtEncoder<sql::ObDASScanRtDef>
{
};
} // end namespace serialization
} // end namespace common

using namespace storage;
using namespace transaction;
namespace sql
{
static ObDASScanIter *das_hybrid_resolve_lookup_data_scan_iter(ObDASIter *data_iter)
{
  ObDASScanIter *scan_iter = nullptr;
  if (OB_NOT_NULL(data_iter)) {
    if (ObDASIterType::DAS_ITER_SCAN == data_iter->get_type()) {
      scan_iter = static_cast<ObDASScanIter *>(data_iter);
    } else if (ObDASIterType::DAS_ITER_DOMAIN_ID_MERGE == data_iter->get_type()) {
      scan_iter = static_cast<ObDASDomainIdMergeIter *>(data_iter)->get_data_table_iter();
    }
  }
  return scan_iter;
}

static int eval_expr_uint64(ObExpr *expr, ObEvalCtx &eval_ctx, uint64_t &out_val)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null expr", K(ret));
  } else if (OB_FAIL(expr->eval(eval_ctx, datum))) {
    LOG_WARN("eval expr failed", K(ret));
  } else if (OB_ISNULL(datum) || datum->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr evaluated to null", K(ret));
  } else {
    out_val = datum->get_uint();
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDASTCBMemProfileKey, fake_unique_id_, timestamp_);

OB_SERIALIZE_MEMBER(ObDASScanCtDef,
                    ref_table_id_,
                    access_column_ids_,
                    schema_version_,
                    table_param_,
                    pd_expr_spec_,
                    aggregate_column_ids_,
                    group_id_expr_,
                    result_output_,
                    is_get_,
                    is_external_table_,
                    external_file_location_,
                    external_file_access_info_,
                    external_files_,
                    external_file_format_str_,
                    trans_info_expr_,
                    group_by_column_ids_,
                    ir_scan_type_,
                    rowkey_exprs_,
                    table_scan_opt_,
                    doc_id_idx_,
                    vec_vid_idx_,
                    multivalue_idx_,
                    multivalue_type_,
                    index_merge_idx_,
                    flags_,
                    partition_infos_,
                    domain_id_idxs_,
                    domain_types_,
                    domain_tids_,
                    pre_query_range_,
                    pre_range_graph_,
                    external_file_pattern_,
                    external_object_ctx_,
                    external_pushdown_filters_,
                    aggregate_param_props_,
                    lake_table_format_,
                    push_down_topn_,
                    semantic_index_info_,
                    external_file_pattern_type_,
                    logical_table_aux_type_,
                    physical_data_table_id_,
                    logical_column_mapping_,
                    logical_output_exprs_,
                    logical_index_seq_,
                    lt_ctx_,
                    logical_residual_filters_);

OB_DEF_SERIALIZE(ObDASScanRtDef)
{
  int ret = OB_SUCCESS;
    LST_DO_CODE(OB_UNIS_ENCODE,
    tenant_schema_version_,
    limit_param_,
    need_scn_,
    force_refresh_lc_,
    frozen_version_,
    fb_snapshot_,
    timeout_ts_,
    tx_lock_timeout_,
    sql_mode_,
    scan_flag_,
    pd_storage_flag_,
    need_check_output_datum_,
    is_for_foreign_check_,
    fb_read_tx_uncommitted_,
    key_ranges_,
    ss_key_ranges_,
    task_count_,
    scan_op_id_,
    scan_rows_size_,
    row_width_,
    das_tasks_key_,
    row_scan_cnt_,
    task_type_,
    local_dynamic_filter_params_,
    topn_param_,
    do_local_dynamic_filter_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASScanRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
    tenant_schema_version_,
    limit_param_,
    need_scn_,
    force_refresh_lc_,
    frozen_version_,
    fb_snapshot_,
    timeout_ts_,
    tx_lock_timeout_,
    sql_mode_,
    scan_flag_,
    pd_storage_flag_,
    need_check_output_datum_,
    is_for_foreign_check_,
    fb_read_tx_uncommitted_,
    key_ranges_,
    ss_key_ranges_,
    task_count_,
    scan_op_id_,
    scan_rows_size_,
    row_width_,
    das_tasks_key_,
    row_scan_cnt_,
    task_type_,
    local_dynamic_filter_params_,
    topn_param_,
    do_local_dynamic_filter_);
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASScanRtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    tenant_schema_version_,
    limit_param_,
    need_scn_,
    force_refresh_lc_,
    frozen_version_,
    fb_snapshot_,
    timeout_ts_,
    tx_lock_timeout_,
    sql_mode_,
    scan_flag_,
    pd_storage_flag_,
    need_check_output_datum_,
    is_for_foreign_check_,
    fb_read_tx_uncommitted_,
    key_ranges_,
    ss_key_ranges_,
    task_count_,
    scan_op_id_,
    scan_rows_size_,
    row_width_,
    das_tasks_key_,
    row_scan_cnt_,
    task_type_,
    local_dynamic_filter_params_,
    topn_param_,
    do_local_dynamic_filter_);
  return len;
}

ObDASScanRtDef::~ObDASScanRtDef()
{
  if (p_row2exprs_projector_ != nullptr) {
    p_row2exprs_projector_->~ObRow2ExprsProjector();
    p_row2exprs_projector_ = nullptr;
  }
  if (p_pd_expr_op_ != nullptr) {
    p_pd_expr_op_->~ObPushdownOperator();
    p_pd_expr_op_ = nullptr;
  }
  if (lt_scan_rt_ != nullptr) {
    lt_scan_rt_->~ObLogicalTableScanRtDef();
    lt_scan_rt_ = nullptr;
  }
}

int ObDASScanRtDef::init_pd_op(ObExecContext &exec_ctx,
                               const ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p_row2exprs_projector_)) {
    p_row2exprs_projector_ =
        new(&row2exprs_projector_) ObRow2ExprsProjector(exec_ctx.get_allocator());
  }
  if (nullptr == p_pd_expr_op_) {
    if (OB_ISNULL(eval_ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(eval_ctx_));
    } else if (FALSE_IT(p_pd_expr_op_ = new(&pd_expr_op_) ObPushdownOperator(*eval_ctx_,
                                                                             scan_ctdef.pd_expr_spec_,
                                                                             enable_rich_format()))) {
    } else if (OB_FAIL(pd_expr_op_.init_pushdown_storage_filter())) {
      LOG_WARN("init pushdown storage filter failed", K(ret));
    } else if (OB_NOT_NULL(scan_ctdef.trans_info_expr_)) {
      //do nothing
    }
  }
  return ret;
}

ObDASScanOp::ObDASScanOp(ObIAllocator &op_alloc)
  : ObIDASTaskOp(op_alloc),
    scan_param_(),
    scan_ctdef_(nullptr),
    scan_rtdef_(nullptr),
    result_(nullptr),
    remain_row_cnt_(0),
    tablet_ids_(op_alloc),
    retry_alloc_(nullptr),
    ir_param_(),
    search_ctx_(nullptr),
    async_query_ctx_(nullptr),
    own_async_query_ctx_(false)
{
}

ObDASScanOp::~ObDASScanOp()
{
  scan_param_.destroy();
  trans_info_array_.destroy();

  if (retry_alloc_ != nullptr) {
    retry_alloc_->reset();
    retry_alloc_ = nullptr;
  }
}

bool ObDASScanOp::is_logical_table_task() const
{
  return ObDASUtils::scan_task_hosts_logical_table_aux(scan_ctdef_, attach_ctdef_);
}

int ObDASScanOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIDASTaskOp::swizzling_remote_task(remote_info))) {
    LOG_WARN("fail to swizzling remote task", K(ret));
  } else if (remote_info != nullptr) {
    //DAS scan is executed remotely
    trans_desc_ = remote_info->trans_desc_;
    // Logical table ns/lt is NOT carried by rtdef: the runner resolves it in
    // init_scan_param by evaluating scan_ctdef_->lt_ctx_ exprs, so nothing has to
    // be checked or propagated here.
    scan_rtdef_->stmt_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
    scan_rtdef_->scan_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
    scan_rtdef_->tsc_monitor_info_ = remote_info->tsc_monitor_info_;
    if (OB_FAIL(scan_rtdef_->init_pd_op(*remote_info->exec_ctx_, *scan_ctdef_))) {
      LOG_WARN("init scan pushdown operator failed", K(ret));
    } else {
      scan_rtdef_->p_pd_expr_op_->get_eval_ctx()
            .set_max_batch_size(scan_ctdef_->pd_expr_spec_.max_batch_size_);
      bool is_vectorized = scan_rtdef_->p_pd_expr_op_->is_vectorized();
      if (!scan_rtdef_->p_pd_expr_op_->is_vectorized()) {
        scan_rtdef_->p_pd_expr_op_->get_eval_ctx().set_batch_size(1);
        scan_rtdef_->p_pd_expr_op_->get_eval_ctx().set_batch_idx(0);
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < related_rtdefs_.count(); ++i) {
      if (OB_NOT_NULL(related_rtdefs_.at(i)) &&
        (DAS_OP_TABLE_SCAN == related_rtdefs_.at(i)->op_type_ ||
          DAS_OP_TABLE_BATCH_SCAN == related_rtdefs_.at(i)->op_type_)) {
        const ObDASScanCtDef *related_ctdef = static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(i));
        ObDASScanRtDef *related_rtdef = static_cast<ObDASScanRtDef*>(related_rtdefs_.at(i));
        related_rtdef->stmt_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
        related_rtdef->scan_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
        related_rtdef->tsc_monitor_info_ = remote_info->tsc_monitor_info_;
        if (OB_FAIL(related_rtdef->init_pd_op(*remote_info->exec_ctx_, *related_ctdef))) {
          LOG_WARN("init related rtdef pushdown operator failed", K(ret));
        } else {
          related_rtdef->p_pd_expr_op_->get_eval_ctx()
              .set_max_batch_size(related_ctdef->pd_expr_spec_.max_batch_size_);
          if (!related_rtdef->p_pd_expr_op_->is_vectorized()) {
            related_rtdef->p_pd_expr_op_->get_eval_ctx().set_batch_size(1);
            related_rtdef->p_pd_expr_op_->get_eval_ctx().set_batch_idx(0);
          }
        }
      } else if (DAS_OP_SCALAR_SCAN_QUERY == related_rtdefs_.at(i)->op_type_) {
        const ObDASScalarScanCtDef *related_ctdef = static_cast<const ObDASScalarScanCtDef*>(related_ctdefs_.at(i));
        ObDASScalarScanRtDef *related_rtdef = static_cast<ObDASScalarScanRtDef*>(related_rtdefs_.at(i));
        related_rtdef->stmt_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
        related_rtdef->scan_allocator_.set_alloc(&CURRENT_CONTEXT->get_arena_allocator());
        if (OB_FAIL(related_rtdef->init_pd_op(*remote_info->exec_ctx_, *related_ctdef))) {
          LOG_WARN("init related rtdef pushdown operator failed", K(ret));
        } else {
          related_rtdef->p_pd_expr_op_->get_eval_ctx()
              .set_max_batch_size(related_ctdef->pd_expr_spec_.max_batch_size_);
          if (!related_rtdef->p_pd_expr_op_->is_vectorized()) {
            related_rtdef->p_pd_expr_op_->get_eval_ctx().set_batch_size(1);
            related_rtdef->p_pd_expr_op_->get_eval_ctx().set_batch_idx(0);
          }
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::init_scan_param()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  scan_param_.tenant_id_ = tenant_id;
  scan_param_.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
  scan_param_.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
  scan_param_.tx_lock_timeout_ = scan_rtdef_->tx_lock_timeout_;
  scan_param_.index_id_ = scan_ctdef_->ref_table_id_;
  scan_param_.is_get_ = scan_ctdef_->is_get_;
  scan_param_.is_for_foreign_check_ = scan_rtdef_->is_for_foreign_check_;
  scan_param_.timeout_ = scan_rtdef_->timeout_ts_;
  scan_param_.scan_flag_ = scan_rtdef_->scan_flag_;
  scan_param_.reserved_cell_count_ = scan_ctdef_->access_column_ids_.count();
  if (in_part_retry_ && retry_alloc_ != nullptr) {
    scan_param_.allocator_      = retry_alloc_;
    scan_param_.scan_allocator_ = retry_alloc_;
  } else {
    scan_param_.allocator_ = &scan_rtdef_->stmt_allocator_;
    scan_param_.scan_allocator_ = &scan_rtdef_->scan_allocator_;
  }
  scan_param_.sql_mode_ = scan_rtdef_->sql_mode_;
  scan_param_.frozen_version_ = scan_rtdef_->frozen_version_;
  scan_param_.force_refresh_lc_ = scan_rtdef_->force_refresh_lc_;
  scan_param_.output_exprs_ = &(scan_ctdef_->pd_expr_spec_.access_exprs_);
  scan_param_.ext_file_column_exprs_ = &(scan_ctdef_->pd_expr_spec_.ext_file_column_exprs_);
  scan_param_.ext_mapping_column_exprs_ = &(scan_ctdef_->pd_expr_spec_.ext_mapping_column_exprs_);
  scan_param_.ext_mapping_column_ids_ = &(scan_ctdef_->pd_expr_spec_.ext_mapping_column_ids_);
  scan_param_.ext_column_dependent_exprs_ = &(scan_ctdef_->pd_expr_spec_.ext_column_convert_exprs_);
  scan_param_.ext_enable_late_materialization_ = scan_ctdef_->pd_expr_spec_.ext_enable_late_materialization_;
  scan_param_.calc_exprs_ = &(scan_ctdef_->pd_expr_spec_.calc_exprs_);
  scan_param_.aggregate_exprs_ = &(scan_ctdef_->pd_expr_spec_.pd_storage_aggregate_output_);
  scan_param_.table_param_ = &(scan_ctdef_->table_param_);
  scan_param_.op_ = scan_rtdef_->p_pd_expr_op_;
  scan_param_.row2exprs_projector_ = scan_rtdef_->p_row2exprs_projector_;
  scan_param_.schema_version_ = scan_ctdef_->schema_version_;
  scan_param_.tenant_schema_version_ = scan_rtdef_->tenant_schema_version_;
  scan_param_.limit_param_ = scan_rtdef_->limit_param_;
  scan_param_.need_scn_ = scan_rtdef_->need_scn_;
  scan_param_.pd_storage_flag_ = scan_ctdef_->pd_expr_spec_.pd_storage_flag_.pd_flag_;
  scan_param_.table_scan_opt_ = scan_ctdef_->table_scan_opt_;
  scan_param_.fb_snapshot_ = scan_rtdef_->fb_snapshot_;
  scan_param_.fb_read_tx_uncommitted_ = scan_rtdef_->fb_read_tx_uncommitted_;
  scan_param_.auto_split_filter_type_ = scan_ctdef_->pd_expr_spec_.auto_split_filter_type_;
  scan_param_.auto_split_filter_ = scan_ctdef_->pd_expr_spec_.auto_split_expr_;
  scan_param_.auto_split_params_ = const_cast<ExprFixedArray *>(&(scan_ctdef_->pd_expr_spec_.auto_split_params_));
  
  scan_param_.main_table_scan_stat_.tsc_monitor_info_ = scan_rtdef_->tsc_monitor_info_;
  scan_param_.in_bf_cache_threshold_ = scan_rtdef_->in_bf_cache_threshold_;
  scan_param_.in_fuse_row_cache_threshold_ = scan_rtdef_->in_fuse_row_cache_threshold_;
  scan_param_.in_row_cache_threshold_ = scan_rtdef_->in_row_cache_threshold_;
  scan_param_.external_object_ctx_ = &scan_ctdef_->external_object_ctx_;
  scan_param_.row_scan_cnt_ = &scan_rtdef_->row_scan_cnt_;
  scan_param_.scan_resume_point_ = scan_rtdef_->scan_resume_point_;
  if (scan_rtdef_->is_for_foreign_check_) {
    scan_param_.trans_desc_ = trans_desc_;
  }
  scan_param_.ls_id_ = ls_id_;
  scan_param_.tablet_id_ = tablet_id_;
  if (scan_rtdef_->sample_info_ != nullptr) {
    scan_param_.sample_info_ = *scan_rtdef_->sample_info_;
  }
  scan_param_.enable_new_false_range_ = scan_ctdef_->enable_new_false_range_;
  if (OB_NOT_NULL(snapshot_)) {
    if (OB_FAIL(scan_param_.snapshot_.assign(*snapshot_))) {
      LOG_WARN("assign snapshot fail", K(ret));
    } else if (snapshot_->read_elr() && !scan_param_.trans_desc_) {
      scan_param_.trans_desc_ = trans_desc_;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("snapshot is null", K(ret), KPC(this));
  }
  // set tx_id for read-latest,
  // TODO: add if(query_flag.is_read_latest) ..
  if (OB_NOT_NULL(trans_desc_)) {
    scan_param_.tx_id_ = trans_desc_->get_tx_id();
    scan_param_.tx_seq_base_ = trans_desc_->get_seq_base();
  } else {
    scan_param_.tx_id_.reset();
  }
  if (!scan_ctdef_->pd_expr_spec_.pushdown_filters_.empty()) {
    scan_param_.op_filters_ = &scan_ctdef_->pd_expr_spec_.pushdown_filters_;
  }
  scan_param_.pd_storage_filters_ = scan_rtdef_->p_pd_expr_op_->pd_storage_filters_;
  if (FAILEDx(scan_param_.column_ids_.assign(scan_ctdef_->access_column_ids_))) {
    LOG_WARN("init column ids failed", K(ret));
  }
  if (OB_SUCC(ret) && !scan_ctdef_->pd_expr_spec_.vt_pd_col_ids_.empty()
    && OB_FAIL(scan_param_.vt_pd_colums_ids_.assign(scan_ctdef_->pd_expr_spec_.vt_pd_col_ids_))) {
    LOG_WARN("init column ids failed", K(ret));
  }
  //external table scan params
  if (OB_SUCC(ret) && scan_ctdef_->is_external_table_) {
    scan_param_.partition_infos_ = &(scan_ctdef_->partition_infos_);
    scan_param_.external_file_access_info_ = scan_ctdef_->external_file_access_info_.str_;
    scan_param_.external_file_location_ = scan_ctdef_->external_file_location_.str_;
    scan_param_.lake_table_format_ = scan_ctdef_->lake_table_format_;
    scan_param_.external_pushdown_filters_ = &scan_ctdef_->external_pushdown_filters_.strs_;
    if (OB_FAIL(scan_param_.external_file_format_.load_from_string(scan_ctdef_->external_file_format_str_.str_, *scan_param_.allocator_))) {
      LOG_WARN("fail to load from string", K(ret));
    } else {
      uint64_t max_idx = 0;
      for (int i = 0; i < scan_param_.ext_file_column_exprs_->count(); i++) {
        if (scan_param_.ext_file_column_exprs_->at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
          max_idx = std::max(max_idx, scan_param_.ext_file_column_exprs_->at(i)->extra_);
        }
      }
      scan_param_.external_file_format_.csv_format_.file_column_nums_ = static_cast<int64_t>(max_idx);
      scan_param_.external_file_format_.csv_format_.ignore_extra_fields_ = true;
      scan_param_.ext_tbl_filter_pd_level_ = scan_ctdef_->pd_expr_spec_.ext_tbl_filter_pd_level_;
    }
  }
  // R9: 本端/远端统一——求值主 ctdef expr 填主 lt_rt + propagate 到子树。
  // 远端 init_scan_param 在 ObIDASTaskOp::process() 路径跑，expr frame + ParamStore 已就绪。
  if (OB_SUCC(ret) && ObDASUtils::scan_task_hosts_logical_table_aux(
          scan_ctdef_, attach_ctdef_)
      && OB_NOT_NULL(scan_rtdef_) && OB_NOT_NULL(scan_rtdef_->eval_ctx_)) {
    ObLogicalTableRtDef &lt_rt = scan_rtdef_->lt_rt_;
    if (OB_FAIL(ObDASUtils::resolve_lt_rt_from_exprs(
            scan_ctdef_->lt_ctx_.ns_id_expr_,
            scan_ctdef_->lt_ctx_.lt_id_start_expr_,
            *scan_rtdef_->eval_ctx_,
            lt_rt,
            OB_NOT_SUPPORTED,
            "logical table query must carry namespace_id and ltable_id predicates"))) {
      LOG_WARN("resolve lt_rt from scan predicate exprs failed", K(ret));
    } else if (lt_rt.enabled_) {
      // lt_scan_rt_ must outlive DAS arena reuse during close/rescan.
      // Allocate it and its path cache from the execution context instead.
      if (OB_FAIL(ObDASUtils::propagate_lt_rt_for_scan_task(
              lt_rt, attach_ctdef_, attach_rtdef_,
              get_lookup_ctdef(), get_lookup_rtdef(),
              &related_ctdefs_, &related_rtdefs_))) {
        LOG_WARN("propagate lt_rt to attach/related failed", K(ret));
      } else if (OB_FAIL(ObLogicalTableRtDef::require_resolved_ns_lt(lt_rt, "init_scan_param after propagate"))) {
        LOG_WARN("lt_rt not resolved after propagate", K(ret), K(lt_rt));
      } else if (OB_FAIL(ObDASUtils::ensure_lt_scan_rt_allocated(
              scan_rtdef_->eval_ctx_->exec_ctx_.get_allocator(), scan_rtdef_->lt_scan_rt_))) {
        LOG_WARN("allocate lt_scan_rt failed", K(ret));
      }
    }
  }
  // Build binary KV scan ranges directly from filter-derived ns/lt/rowid values.
  // Does NOT depend on multi-column PK ranges — CG analyses WHERE filter exprs
  // and stores ObExpr* references in lt_ctx_. DAS evaluates them here to get
  // concrete uint64 values, constructs an ObLogicalTableKeyPrefix, and calls
  // build_primary_ranges() to produce single-column varbinary ranges.
  if (OB_SUCC(ret) && scan_ctdef_->is_logical_table()) {
    ObLogicalTableRtDef &lt_rt = scan_rtdef_->lt_rt_;
    const ObLogicalTableScanCtDef &lt_ct = scan_ctdef_->lt_ctx_;
    if (OB_NOT_NULL(scan_rtdef_->lt_scan_rt_)) {
      scan_rtdef_->lt_scan_rt_->reset_scan_state();
    }
    if (scan_param_.key_ranges_.empty()) {
      // empty range: nothing to do, leave lt_ctx disabled
    } else if (!lt_ct.analyzed_) {
      // ns/lt exprs not found in filter analysis; scan proceeds on the
      // logical table (HEAP, empty) and returns 0 rows
    } else {
      if (lt_rt.has_resolved_ns_lt()) {
        common::ObIAllocator &alloc = scan_rtdef_->stmt_allocator_;
        ObEvalCtx &eval_ctx = *scan_rtdef_->eval_ctx_;
        const uint64_t ns_id = lt_rt.namespace_id_;
        // ltable_id range: [lt_id_start, lt_id_end] with inclusive flags from CG.
        //   - equality (=)         -> start = end = expr value, both inclusive (CG sets both exprs)
        //   - lower bound only     -> start expr set, end_expr null -> lt_id_end = UINT64_MAX
        //   - upper bound only     -> end expr set, start_expr null -> lt_id_start = 0
        //   - lower + upper bound  -> both exprs set
        uint64_t lt_id_start = 0;
        uint64_t lt_id_end = UINT64_MAX;
        bool lt_start_inclusive = true;
        bool lt_end_inclusive = true;
        if (OB_SUCC(ret) && OB_NOT_NULL(lt_ct.lt_id_start_expr_)) {
          if (OB_FAIL(eval_expr_uint64(lt_ct.lt_id_start_expr_, eval_ctx, lt_id_start))) {
            LOG_WARN("eval lt_id_start expr failed", K(ret));
          } else {
            lt_start_inclusive = lt_ct.lt_start_inclusive_;
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(lt_ct.lt_id_end_expr_)) {
          if (OB_FAIL(eval_expr_uint64(lt_ct.lt_id_end_expr_, eval_ctx, lt_id_end))) {
            LOG_WARN("eval lt_id_end expr failed", K(ret));
          } else {
            lt_end_inclusive = lt_ct.lt_end_inclusive_;
          }
        }
        bool has_rowid_lower_bound = false;
        bool has_rowid_upper_bound = false;
        uint64_t rowid_start = 0;
        uint64_t rowid_end = UINT64_MAX;
        bool start_inclusive = true;
        bool end_inclusive = true;
        if (OB_SUCC(ret) && OB_NOT_NULL(lt_ct.rowid_start_expr_)) {
          has_rowid_lower_bound = true;
          if (OB_FAIL(eval_expr_uint64(lt_ct.rowid_start_expr_, eval_ctx, rowid_start))) {
            LOG_WARN("eval rowid_start expr failed", K(ret));
          } else {
            start_inclusive = lt_ct.start_inclusive_;
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(lt_ct.rowid_end_expr_)) {
          has_rowid_upper_bound = true;
          if (OB_FAIL(eval_expr_uint64(lt_ct.rowid_end_expr_, eval_ctx, rowid_end))) {
            LOG_WARN("eval rowid_end expr failed", K(ret));
          } else {
            end_inclusive = lt_ct.end_inclusive_;
          }
        }
        // prefix anchor: expr-evaluated ns/lt (D1); filter exprs supply lt_id/rowid bounds only
        share::ObLogicalTableKeyPrefix prefix(ns_id, lt_rt.logical_table_id_,
            scan_ctdef_->logical_index_seq_,
            scan_ctdef_->logical_table_aux_type_);
        common::ObSEArray<common::ObNewRange, 1> physical_ranges;
        physical_ranges.set_attr(ObMemAttr(MTL_ID(), "LTPhysRng"));
        if (OB_SUCC(ret) && OB_FAIL(share::ObLogicalTableRangeBuilder::build_primary_ranges(
            scan_ctdef_->physical_data_table_id_,
            prefix,
            lt_id_end, lt_start_inclusive, lt_end_inclusive,
            has_rowid_lower_bound, rowid_start, start_inclusive,
            has_rowid_upper_bound, rowid_end, end_inclusive,
            alloc, physical_ranges))) {
          LOG_WARN("build primary ranges for logical table failed", K(ret), K(prefix),
              K(lt_id_start), K(lt_id_end),
              K(lt_start_inclusive), K(lt_end_inclusive),
              K(rowid_start), K(rowid_end));
        } else if (physical_ranges.empty()) {
          // empty effective range (e.g. lt_start > lt_end): scan nothing
          scan_param_.key_ranges_.reuse();
          lt_rt.enabled_ = false;
        } else {
          scan_param_.key_ranges_.reuse();
          if (OB_FAIL(scan_param_.key_ranges_.assign(physical_ranges))) {
            LOG_WARN("assign physical ranges failed", K(ret));
          } else {
            lt_rt.enabled_ = true;
            if (OB_NOT_NULL(scan_rtdef_->lt_scan_rt_)) {
              scan_rtdef_->lt_scan_rt_->direct_phys_ranges_built_ = true;
            }
            // Redirect scan_param_ to the KV table's tablet on the same partition.
            // Tablegroup-aligned partition layout guarantees the logical table and
            // the KV table share part_idx for the same partition key value, but
            // their tablet_ids are independent. Use logical schema to derive
            // part_idx from the current logical tablet, then map to the KV table's
            // tablet via the same part_idx on the KV schema.
            {
              const uint64_t logical_ref_table_id = scan_ctdef_->ref_table_id_;
              const uint64_t kv_ref_table_id = scan_ctdef_->physical_data_table_id_;
              ObExecContext &exec_ctx = scan_rtdef_->eval_ctx_->exec_ctx_;
              ObSQLSessionInfo *session = exec_ctx.get_my_session();
              uint64_t tenant_id = OB_INVALID_TENANT_ID;
              share::schema::ObSchemaGetterGuard schema_guard;
              const share::schema::ObTableSchema *logical_schema = nullptr;
              const share::schema::ObTableSchema *kv_schema = nullptr;
              int64_t part_idx = 0;
              int64_t subpart_idx = 0;
              ObObjectID kv_object_id = OB_INVALID_ID;
              ObObjectID kv_first_level_part_id = OB_INVALID_ID;
              ObTabletID kv_tablet_id;
              if (OB_UNLIKELY(common::OB_INVALID_ID == logical_ref_table_id)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("logical table ref_table_id is invalid", K(ret), K(logical_ref_table_id));
              } else if (OB_ISNULL(session)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("session is null for logical table tablet redirect", K(ret));
              } else if (FALSE_IT(tenant_id = session->get_effective_tenant_id())) {
              } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                      tenant_id, schema_guard))) {
                LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
              } else if (OB_FAIL(schema_guard.get_table_schema(
                      tenant_id, logical_ref_table_id, logical_schema))) {
                LOG_WARN("get logical schema failed", K(ret), K(logical_ref_table_id));
              } else if (OB_ISNULL(logical_schema)) {
                ret = OB_TABLE_NOT_EXIST;
                LOG_WARN("logical schema not found", K(ret), K(logical_ref_table_id));
              } else if (OB_FAIL(schema_guard.get_table_schema(
                      tenant_id, kv_ref_table_id, kv_schema))) {
                LOG_WARN("get kv schema failed", K(ret), K(kv_ref_table_id));
              } else if (OB_ISNULL(kv_schema)) {
                ret = OB_TABLE_NOT_EXIST;
                LOG_WARN("kv schema not found", K(ret), K(kv_ref_table_id));
              } else if (OB_FAIL(logical_schema->get_part_idx_by_tablet(
                      scan_param_.tablet_id_, part_idx, subpart_idx))) {
                LOG_WARN("get part_idx from logical tablet failed", K(ret),
                    K(scan_param_.tablet_id_), K(logical_ref_table_id));
              } else if (OB_FAIL(kv_schema->get_part_id_and_tablet_id_by_idx(
                      part_idx, subpart_idx, kv_object_id,
                      kv_first_level_part_id, kv_tablet_id))) {
                LOG_WARN("get kv tablet by part_idx failed", K(ret),
                    K(part_idx), K(subpart_idx), K(kv_ref_table_id));
              } else {
                // Use the KV tablet resolved from the logical tablet to keep
                // scan_param_ aligned with the physical data table.
                scan_param_.tablet_id_ = kv_tablet_id;
                LOG_TRACE("[LT hybrid DAS] init_scan_param logical-table KV tablet redirect ok",
                         K(logical_ref_table_id), K(kv_tablet_id));
              }
            }
          }
        }
      } else if (ObDASUtils::is_hybrid_search(attach_ctdef_)) {
        ObLogicalTableRewriteCtx rewrite_ctx = {};
        rewrite_ctx.namespace_id_     = scan_rtdef_->lt_rt_.namespace_id_;
        rewrite_ctx.logical_table_id_ = scan_rtdef_->lt_rt_.logical_table_id_;
        if (OB_FAIL(ObLogicalTableHelper::rewrite_table_scan_param_key_ranges(
                scan_param_, *scan_ctdef_, *scan_rtdef_->eval_ctx_, scan_rtdef_->stmt_allocator_,
                rewrite_ctx))) {
          if (OB_NOT_SUPPORTED == ret) {
            LOG_WARN("[LT hybrid DAS] logical table range rewrite not supported, keep original scan range",
                     K(ret));
            ret = OB_SUCCESS;
          } else if (OB_FAIL(ret)) {
            LOG_WARN("[LT hybrid DAS] init_scan_param logical table range rewrite failed", K(ret));
          }
        }
      }
    }
  }
  LOG_DEBUG("init scan param", K(ret), K(scan_param_));
  return ret;
}

ObITabletScan &ObDASScanOp::get_tsc_service()
{
  return is_virtual_table(scan_ctdef_->ref_table_id_) ? *GCTX.vt_par_ser_
                                                      : scan_ctdef_->is_external_table_ ? *GCTX.et_access_service_
                                                                                        : *(MTL(ObAccessService *));
}

ObDASIterTreeType ObDASScanOp::get_iter_tree_type() const
{
  ObDASIterTreeType tree_type = ObDASIterTreeType::ITER_TREE_INVALID;
  bool is_fts_index = scan_param_.table_param_->is_fts_index() && attach_ctdef_ != nullptr;
  bool is_vector_index = ObDASUtils::is_vec_idx_scan(attach_ctdef_);
  bool is_spatial_index = scan_param_.table_param_->is_spatial_index() && !is_vector_index;
  bool is_multivalue_index = scan_param_.table_param_->is_multivalue_index();
  bool is_es_match = ObDASUtils::is_es_match_scan(attach_ctdef_);
  bool is_hybrid_search = ObDASUtils::is_hybrid_search(attach_ctdef_);

  if (is_hybrid_search) {
    tree_type = ObDASIterTreeType::ITER_TREE_HYBRID_SEARCH;
  } else if (is_vector_index) {
    tree_type = ObDASIterTreeType::ITER_TREE_VEC_LOOKUP;
  } else if (ObDASUtils::is_func_lookup(attach_ctdef_)) {
    tree_type = ObDASIterTreeType::ITER_TREE_FUNC_LOOKUP;
  } else if (is_es_match) {
    tree_type = ObDASIterTreeType::ITER_TREE_MATCH;
  } else if (is_fts_index) {
    tree_type = ObDASIterTreeType::ITER_TREE_TEXT_RETRIEVAL;
  } else if (is_spatial_index) {
    tree_type = ObDASIterTreeType::ITER_TREE_GIS_LOOKUP;
  } else if (is_multivalue_index) {
    tree_type = ObDASIterTreeType::ITER_TREE_MVI_LOOKUP;
  } else if (OB_UNLIKELY(ObDASUtils::is_index_merge(attach_ctdef_))) {
    tree_type = ObDASIterTreeType::ITER_TREE_INDEX_MERGE;
  } else {
    tree_type = OB_ISNULL(get_lookup_ctdef()) ? ObDASIterTreeType::ITER_TREE_PARTITION_SCAN
                                              : ObDASIterTreeType::ITER_TREE_LOCAL_LOOKUP;
  }
  LOG_TRACE("get iter tree type", K(tree_type), K(scan_param_.tablet_id_), KPC(attach_ctdef_));
  return tree_type;
}

storage::ObTableScanParam* ObDASScanOp::get_local_lookup_param()
{
  storage::ObTableScanParam* lookup_param = nullptr;
  if (is_local_task() && ITER_TREE_LOCAL_LOOKUP == get_iter_tree_type() && OB_NOT_NULL(result_)) {
    ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter*>(result_);
    lookup_param = &lookup_iter->get_lookup_param();
  }
  return lookup_param;
}

int ObDASScanOp::init_related_tablet_ids(ObDASRelatedTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  related_tablet_ids.reset();

  bool is_vec_index_query = ObDASUtils::is_vec_idx_scan(attach_ctdef_);
  if (OB_FAIL(get_table_lookup_tablet_id(related_tablet_ids.lookup_tablet_id_))) {
    LOG_WARN("failed to get table lookup tablet id", K(ret));
  } else if (OB_ISNULL(attach_ctdef_) || OB_ISNULL(attach_rtdef_)) { // no attached task.
  } else if (OB_UNLIKELY(related_ctdefs_.count() != related_tablet_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected related scan array not match", K(ret), K_(related_ctdefs), K_(related_tablet_ids));
  } else if (OB_FAIL(get_rowkey_domain_tablet_id(related_tablet_ids))) {
    LOG_WARN("fail to get rowkey doc tablet id", K(ret));
  } else if (OB_FAIL(get_doc_rowkey_tablet_id(related_tablet_ids))) {
    LOG_WARN("failed to get doc rowkey tablet id", K(ret));
  } else if (OB_FAIL(get_index_merge_tablet_ids(related_tablet_ids.index_merge_tablet_ids_))) {
    LOG_WARN("failed to get index merge tablet ids", K(ret));
  } else if (OB_FAIL(get_fts_tablet_ids(related_tablet_ids.fts_tablet_ids_, attach_rtdef_))) {
    LOG_WARN("failed to get fts tablet ids", K(ret));
  } else if (is_vec_index_query && OB_FAIL(get_vec_ir_tablet_ids(related_tablet_ids))) {
    LOG_WARN("failed to get vec aux tablet id", K(ret), K_(related_tablet_ids));
  }
  return ret;
}

int ObDASScanOp::check_merge_range_opt()
{
  int ret = OB_SUCCESS;
  int64_t range_cnt = scan_param_.key_ranges_.count();
  if (OB_ISNULL(scan_ctdef_) || OB_ISNULL(scan_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan ctdef is null", K(ret));
  } else if (scan_rtdef_->do_local_dynamic_filter_) {
    // use the row estimation interface to get number of logical rows after merging ranges,
    // enable the optimization if the number of logical rows does not increase significantly after merging.
    int64_t merge_opt_threshold = - EVENT_CALL(EventTable::EN_MERGE_RANGE_OPT_THRESHOLD);
    merge_opt_threshold = merge_opt_threshold > 0 ? merge_opt_threshold : 10;
    ObNewRange merge_range;
    ObObj *start_obj = nullptr;
    ObObj *end_obj = nullptr;
    uint64_t min_rowkey = UINT64_MAX;
    uint64_t max_rowkey = 0;
    if (OB_ISNULL(start_obj = OB_NEWx(ObObj, &op_alloc_))
        || OB_ISNULL(end_obj = OB_NEWx(ObObj, &op_alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate enough memory", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < range_cnt; i++) {
        min_rowkey = std::min(min_rowkey, scan_param_.key_ranges_.at(i).start_key_.get_obj_ptr()[0].get_uint64());
        max_rowkey = std::max(max_rowkey, scan_param_.key_ranges_.at(i).start_key_.get_obj_ptr()[0].get_uint64());
      }
      start_obj[0].set_uint64(min_rowkey);
      end_obj[0].set_uint64(max_rowkey);
      merge_range.table_id_ = scan_ctdef_->ref_table_id_;
      merge_range.start_key_.assign(start_obj, 1);
      merge_range.end_key_.assign(end_obj, 1);
      merge_range.border_flag_.set_inclusive_start();
      merge_range.border_flag_.set_inclusive_end();
      ObSimpleBatch batch;
      batch.type_ = common::ObSimpleBatch::T_SCAN;
      batch.range_ = &merge_range;
      ObArray<ObEstRowCountRecord> records; // unused
      double logical_row_count = 0;
      double physical_row_count = 0;
      share::SCN max_readable_scn;
      if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), nullptr, max_readable_scn))) {
        LOG_WARN("failed to get gts", K(ret));
      } else if (FALSE_IT(scan_param_.frozen_version_ = static_cast<int64_t>(max_readable_scn.get_val_for_sql()))) {
      } else if (OB_FAIL(ObStorageEstimator::storage_estimate_partition_batch_rowcount(MTL_ID(), batch, scan_param_, records, logical_row_count, physical_row_count))) {
        LOG_WARN("failed to estimate row count", K(ret));
      } else if (FALSE_IT(scan_param_.frozen_version_ = -1)) {  // restore frozen version
      } else if (logical_row_count <= static_cast<double>(range_cnt * merge_opt_threshold)) {
        // no significant increase, enable range merge
        scan_param_.key_ranges_.reuse();
        if (OB_FAIL(scan_param_.key_ranges_.push_back(merge_range))) {
          LOG_WARN("failed to push back key ranges", K(merge_range), K(ret));
        }
      }

      // whether performs merge range opt or not, filter params should be prepared
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(scan_rtdef_->p_pd_expr_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pd expr op is null", K(ret));
      } else if (OB_FAIL(prepare_local_dynamic_filter_context())) {
        LOG_WARN("failed to prepare local dynamic filter context", K(ret));
      } else {
        scan_rtdef_->p_pd_expr_op_->set_local_dynamic_filter_params(scan_rtdef_->local_dynamic_filter_params_);
      }
      LOG_TRACE("check merge range opt", K(ret), K(scan_rtdef_->local_dynamic_filter_params_.count()), K(merge_opt_threshold), K(merge_range), K(scan_param_), KPC(scan_rtdef_), K(logical_row_count));
    }
  }
  return ret;
}

int ObDASScanOp::prepare_local_dynamic_filter_context()
{
  int ret = OB_SUCCESS;
  ObExpr *local_dynamic_filter = nullptr;
  if (OB_UNLIKELY(scan_ctdef_->pd_expr_spec_.pushdown_filters_.count() != 1)
      || OB_ISNULL(local_dynamic_filter = scan_ctdef_->pd_expr_spec_.pushdown_filters_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pushdown filter", K(ret), K(scan_ctdef_->pd_expr_spec_.pushdown_filters_));
  } else {
    uint64_t op_id = local_dynamic_filter->expr_ctx_id_;
    ObExecContext &exec_ctx = scan_rtdef_->eval_ctx_->exec_ctx_;
    ObExprLocalDynamicFilterContext *local_dynamic_filter_ctx = nullptr;
    if (OB_ISNULL(local_dynamic_filter_ctx = static_cast<ObExprLocalDynamicFilterContext *>(
              exec_ctx.get_expr_op_ctx(op_id)))) {
      // create local dynamic filter context
      if (OB_FAIL(exec_ctx.create_expr_op_ctx(op_id, local_dynamic_filter_ctx))) {
        LOG_WARN("failed to create local dynamic filter context", K(ret), K(op_id));
      } else if (OB_ISNULL(local_dynamic_filter_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("local dynamic filter context is null", K(ret));
      }
    }
    if (OB_FAIL(ret) || OB_ISNULL(local_dynamic_filter_ctx)) {
    } else if (OB_FAIL(local_dynamic_filter_ctx->init_params(scan_rtdef_->local_dynamic_filter_params_))) {
      LOG_WARN("failed to init local dynamic filter context", K(ret));
    } else {
      local_dynamic_filter_ctx->set_do_local_dynamic_filter(true);
    }
  }
  return ret;
}

int ObDASScanOp::create_das_search_context()
{
  int ret = OB_SUCCESS;
  ObDASSearchCtx *search_ctx = nullptr;
  const ObDASBaseCtDef *ctdef = nullptr;
  const ObDASFusionCtDef *fusion_ctdef = nullptr;
  if (OB_UNLIKELY(nullptr != search_ctx_) ||
      OB_ISNULL(scan_rtdef_) ||
      !scan_rtdef_->enable_rich_format()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hybrid search argument", K(search_ctx_), KPC(scan_rtdef_), K(ret));
  } else if (OB_FAIL(ObDASUtils::find_child_das_ctdef(attach_ctdef_, DAS_OP_FUSION_QUERY, ctdef))) {
    LOG_WARN("failed to find fusion ctdef", K(ret));
  } else if (OB_ISNULL(ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctdef", K(ret));
  } else if (FALSE_IT(fusion_ctdef = static_cast<const ObDASFusionCtDef *>(ctdef))) {
  } else if (OB_ISNULL(search_ctx = OB_NEWx(ObDASSearchCtx, &op_alloc_, op_alloc_, *this))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate search ctx", K(ret));
  } else if (OB_FAIL(init_logical_table_ctx_for_search_(search_ctx, fusion_ctdef))) {
    LOG_WARN("failed to init logical table ctx for search", K(ret));
  } else if (OB_FAIL(search_ctx->init(
    fusion_ctdef->rowid_exprs_,
    fusion_ctdef->result_output_,
    true /* use dynamic pruning */))) {
    LOG_WARN("failed to init search ctx", K(ret));
  } else if (FALSE_IT(search_ctx->set_fusion_ctdef(fusion_ctdef))) {
  } else if (fusion_ctdef->has_nested_paths() &&
             OB_FAIL(search_ctx->register_nested_scope(&fusion_ctdef->nested_rowid_exprs_))) {
    LOG_WARN("failed to register nested scope", K(ret));
  } else {
    search_ctx_ = search_ctx;
  }
  return ret;
}

// Recursively detect any async fulltext wrapper or async scalar-search leaf.
// The latter may be used without MATCH, so it cannot rely on the fulltext boolean wrapper.
static bool das_ctdef_tree_has_async_index(const ObDASBaseCtDef *ctdef)
{
  bool found = false;
  if (OB_NOT_NULL(ctdef)) {
    if (DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN == ctdef->op_type_) {
      found = true;
    } else if (DAS_OP_BOOLEAN_QUERY == ctdef->op_type_
        && static_cast<const ObDASBooleanQueryCtDef *>(ctdef)->is_async()) {
      found = true;
    } else if (DAS_OP_SCALAR_SCAN_QUERY == ctdef->op_type_
        && static_cast<const ObDASScalarScanCtDef *>(ctdef)->is_async_index_) {
      found = true;
    } else {
      for (int64_t i = 0; !found && i < ctdef->children_cnt_; ++i) {
        found = das_ctdef_tree_has_async_index(ctdef->children_[i]);
      }
    }
  }
  return found;
}

// Collect the physical scan ctdefs that participate in async segment alignment.
// An IR node owns the async/sync decision for MATCH, so do not infer it from an
// arbitrary scalar child. This keeps synchronous MATCH tablets out of a segment
// group that was created by another async leaf in the same hybrid-search tree.
static int collect_async_index_scan_ctdefs(
    const ObDASBaseCtDef *ctdef,
    ObIArray<const ObDASBaseCtDef *> &scan_ctdefs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef)) {
    // Empty child is tolerated here; the ordinary DAS tree validation reports malformed trees.
  } else if (DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN == ctdef->op_type_) {
    const auto *scan = static_cast<const ObDASVecIndexAsyncIVFScanCtDef *>(ctdef)->get_index_scan_ctdef();
    if (nullptr == scan) { ret = OB_ERR_UNEXPECTED; }
    else { ret = scan_ctdefs.push_back(scan); }
  } else if (DAS_OP_IR_SCAN == ctdef->op_type_) {
    const ObDASIRScanCtDef *ir_ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef);
    const ObDASScalarScanCtDef *inv_scan_ctdef = nullptr;
    if (ir_ctdef->is_async_index_) {
      inv_scan_ctdef = ir_ctdef->get_inv_idx_scan_scalar_ctdef();
      if (OB_ISNULL(inv_scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async ir scan has no inverted scan ctdef", K(ret), KPC(ir_ctdef));
      } else if (OB_FAIL(scan_ctdefs.push_back(inv_scan_ctdef))) {
        LOG_WARN("failed to collect async fts inverted scan ctdef", K(ret));
      }
    }
  } else if (DAS_OP_SCALAR_SCAN_QUERY == ctdef->op_type_
      && static_cast<const ObDASScalarScanCtDef *>(ctdef)->is_async_index_) {
    if (OB_FAIL(scan_ctdefs.push_back(static_cast<const ObDASScalarScanCtDef *>(ctdef)))) {
      LOG_WARN("failed to collect async scalar scan ctdef", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctdef->children_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(collect_async_index_scan_ctdefs(
          ctdef->children_[i], scan_ctdefs)))) {
        LOG_WARN("failed to collect child async index scan ctdef", K(ret), K(i));
      }
    }
  }
  return ret;
}

bool ObDASScanOp::is_async_index_search() const
{
  return das_ctdef_tree_has_async_index(attach_ctdef_);
}

int ObDASScanOp::set_shared_async_index_query_ctx(ObDASAsyncIndexQueryCtx *query_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx) || OB_UNLIKELY(!query_ctx->is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared async index query catalog", K(ret), KP(query_ctx));
  } else if (own_async_query_ctx_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot replace owned async index query catalog", K(ret), KP(async_query_ctx_));
  } else {
    async_query_ctx_ = query_ctx;
    own_async_query_ctx_ = false;
  }
  return ret;
}

int ObDASScanOp::collect_async_delete_tablet_id_from_ctdef_(
    const ObDASBaseCtDef *ctdef,
    ObTabletID &delete_tablet_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef)) {
    // Empty child is allowed in a partially generated search tree.
  } else if (DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN == ctdef->op_type_) {
    const auto *scan = static_cast<const ObDASVecIndexAsyncIVFScanCtDef *>(ctdef)->get_delete_scan_ctdef();
    ObTabletID current;
    if (nullptr == scan) { ret = OB_ERR_UNEXPECTED; }
    else if (OB_FAIL(get_related_tablet_id(scan, current))) {
      LOG_WARN("failed to resolve async IVF deletion tablet", K(ret));
    } else if (!current.is_valid() || (delete_tablet_id.is_valid() && current != delete_tablet_id)) {
      ret = OB_ERR_UNEXPECTED;
    } else { delete_tablet_id = current; }
  } else if (DAS_OP_BOOLEAN_QUERY == ctdef->op_type_
      && static_cast<const ObDASBooleanQueryCtDef *>(ctdef)->is_async()) {
    const ObDASBooleanQueryCtDef *boolean_ctdef =
        static_cast<const ObDASBooleanQueryCtDef *>(ctdef);
    ObBooleanSubClause<ObIDASSearchCtDef> must_not_clause;
    ObIDASSearchCtDef *delete_ctdef = nullptr;
    ObTabletID current_delete_tablet_id;
    if (OB_FAIL(boolean_ctdef->must_not(must_not_clause))) {
      LOG_WARN("failed to get async delete clause", K(ret));
    } else if (OB_UNLIKELY(1 != must_not_clause.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected async delete clause count", K(ret), K(must_not_clause.count()));
    } else if (OB_FAIL(must_not_clause.get(0, delete_ctdef))) {
      LOG_WARN("failed to get async delete ctdef", K(ret));
    } else if (OB_ISNULL(delete_ctdef)
        || OB_UNLIKELY(DAS_OP_SCALAR_SCAN_QUERY != delete_ctdef->op_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected async delete ctdef", K(ret), KP(delete_ctdef));
    } else if (OB_FAIL(get_related_tablet_id(delete_ctdef, current_delete_tablet_id))) {
      LOG_WARN("failed to get async delete tablet id", K(ret), KPC(delete_ctdef));
    } else if (OB_UNLIKELY(!current_delete_tablet_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async delete tablet id", K(ret), K(current_delete_tablet_id));
    } else if (!delete_tablet_id.is_valid()) {
      delete_tablet_id = current_delete_tablet_id;
    } else if (OB_UNLIKELY(delete_tablet_id != current_delete_tablet_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("multiple async delete tablets found", K(ret), K(delete_tablet_id),
          K(current_delete_tablet_id));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(ctdef)
      && i < ctdef->children_cnt_; ++i) {
    if (OB_FAIL(SMART_CALL(collect_async_delete_tablet_id_from_ctdef_(
        ctdef->children_[i], delete_tablet_id)))) {
      LOG_WARN("failed to collect async delete tablet id from child", K(ret), K(i));
    }
  }
  return ret;
}

int ObDASScanOp::build_async_index_query_ctx(ObDASAsyncIndexQueryCtx &query_ctx) const
{
  int ret = OB_SUCCESS;
  bool prepared_from_candidates = false;
  ObSEArray<uint64_t, 2> index_table_ids;
  ObSEArray<ObTabletID, 2> index_tablet_ids;
  ObTabletID delete_tablet_id;
  share::SCN sql_read_snapshot;
  if (OB_ISNULL(snapshot_) || OB_ISNULL(scan_rtdef_) || OB_ISNULL(scan_ctdef_)
      || OB_ISNULL(scan_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null definition or runtime state for async index query", K(ret));
  } else if (OB_FAIL(ObDASAsyncIndexQueryCtx::check_tx_isolation(
                 scan_rtdef_->eval_ctx_->exec_ctx_.get_my_session()))) {
    LOG_WARN("failed to check async index query isolation", K(ret));
  } else if (FALSE_IT(sql_read_snapshot = snapshot_->version())) {
  } else if (OB_FAIL(ObDASAsyncIndexQueryCtx::collect_async_index_table_ids(
                 attach_ctdef_, index_table_ids))) {
    LOG_WARN("unsupported async index query shape", K(ret));
  } else if (OB_NOT_NULL(scan_rtdef_->table_loc_)) {
    ObSEArray<const ObDASTabletLoc *, 8> candidate_locs;
    const DASTabletLocList &tablet_locs = scan_rtdef_->table_loc_->get_tablet_locs();
    for (ConstDASTabletLocListIter iter = tablet_locs.begin();
         OB_SUCC(ret) && iter != tablet_locs.end(); ++iter) {
      const ObDASTabletLoc *anchor = *iter;
      if (OB_ISNULL(anchor) || anchor->server_ != GCTX.self_addr()) {
        // Only candidates routed to this observer can become GI tasks of this SQC.
      } else if (OB_FAIL(candidate_locs.push_back(anchor))) {
        LOG_WARN("failed to add remote async index candidate", K(ret), KPC(anchor));
      }
    }
    if (OB_SUCC(ret) && !candidate_locs.empty()
        && OB_FAIL(query_ctx.prepare(
               attach_ctdef_, candidate_locs, sql_read_snapshot,
               scan_ctdef_->use_async_index_group_alignment_
                   ? ASYNC_INDEX_EXEC_GROUP : ASYNC_INDEX_EXEC_LEGACY))) {
      LOG_WARN("failed to prepare remote async index catalog from candidates", K(ret));
    } else if (OB_SUCC(ret) && !candidate_locs.empty()) {
      prepared_from_candidates = true;
    }
  }
  if (OB_SUCC(ret) && !prepared_from_candidates) {
    ObSEArray<const ObDASBaseCtDef *, 4> scan_ctdefs;
    if (OB_FAIL(collect_async_index_scan_ctdefs(attach_ctdef_, scan_ctdefs))) {
      LOG_WARN("failed to collect current async index scan ctdefs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_table_ids.count(); ++i) {
      ObTabletID index_tablet_id;
      for (int64_t j = 0; OB_SUCC(ret) && j < scan_ctdefs.count(); ++j) {
        const ObDASBaseCtDef *scan = scan_ctdefs.at(j);
        const uint64_t ref_id = DAS_OP_TABLE_SCAN == scan->op_type_
            ? static_cast<const ObDASScanCtDef *>(scan)->ref_table_id_
            : static_cast<const ObDASScalarScanCtDef *>(scan)->ref_table_id_;
        if (index_table_ids.at(i) == ref_id) {
          ObTabletID current_index_tablet_id;
          if (OB_FAIL(get_related_tablet_id(
                  scan_ctdefs.at(j), current_index_tablet_id))) {
            LOG_WARN("failed to resolve zipped async index tablet", K(ret), K(i), K(j));
          } else if (OB_UNLIKELY(!current_index_tablet_id.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid zipped async index tablet", K(ret), K(i), K(j));
          } else if (!index_tablet_id.is_valid()) {
            index_tablet_id = current_index_tablet_id;
          } else if (OB_UNLIKELY(index_tablet_id != current_index_tablet_id)) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("conflicting zipped async index tablet", K(ret), K(i),
                     K(index_tablet_id), K(current_index_tablet_id));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(!index_tablet_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zipped async index tablet not found", K(ret),
                 "index_table_id", index_table_ids.at(i));
      } else if (OB_FAIL(index_tablet_ids.push_back(index_tablet_id))) {
        LOG_WARN("failed to append zipped async index tablet", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(collect_async_delete_tablet_id_from_ctdef_(
            attach_ctdef_, delete_tablet_id))) {
      LOG_WARN("failed to collect current async delete tablet", K(ret));
    } else if (OB_SUCC(ret) && OB_UNLIKELY(index_tablet_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no async fulltext tablet collected", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(query_ctx.prepare_resolved_partition(
                   attach_ctdef_, get_ls_id(), tablet_id_, index_table_ids,
                   index_tablet_ids, delete_tablet_id, sql_read_snapshot,
                   scan_ctdef_->use_async_index_group_alignment_
                       ? ASYNC_INDEX_EXEC_GROUP : ASYNC_INDEX_EXEC_LEGACY))) {
      LOG_WARN("failed to prepare current remote async index partition", K(ret), K_(tablet_id));
    }
  }
  return ret;
}

// Resolve KV physical tablet for Agent DB logical-table paths by the same rule as
// init_scan_param logical-table redirect (logical tablet -> part_idx on logical schema
// -> KV tablet on KV schema). KV LEVEL_ZERO uses get_tablet_and_object_id only.
int ObLogicalTableHelper::resolve_kv_tablet_via_logical_anchor(
    const uint64_t tenant_id,
    const uint64_t logical_ref_table_id,
    const uint64_t kv_table_id,
    const common::ObTabletID &logical_anchor_tablet,
    common::ObTabletID &kv_tablet_id,
    ObObjectID &kv_object_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *logical_schema = nullptr;
  const share::schema::ObTableSchema *kv_schema = nullptr;
  LOG_TRACE("[LT hybrid DAS] resolve_agent_db_kv_tablet_via_logical_tablet_ entry",
           K(tenant_id),
           K(logical_ref_table_id),
           K(kv_table_id),
           K(logical_anchor_tablet));
  if (OB_UNLIKELY(common::OB_INVALID_ID == logical_ref_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical_ref_table_id not set for kv tablet resolve via logical tablet",
             K(ret), K(logical_ref_table_id), K(kv_table_id));
  } else if (!logical_anchor_tablet.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("logical anchor tablet invalid for kv tablet resolve", K(ret),
             K(logical_anchor_tablet), K(kv_table_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, logical_ref_table_id, logical_schema))) {
    LOG_WARN("get logical table schema failed", K(ret), K(logical_ref_table_id));
  } else if (OB_ISNULL(logical_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("logical table schema not found", K(ret), K(logical_ref_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, kv_table_id, kv_schema))) {
    LOG_WARN("get kv data table schema failed", K(ret), K(kv_table_id));
  } else if (OB_ISNULL(kv_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("kv data table schema not found", K(ret), K(kv_table_id));
  } else {
    LOG_TRACE("[LT hybrid DAS] resolve_agent_db_kv_tablet_via_logical_tablet_ schemas loaded",
             K(logical_ref_table_id),
             K(kv_table_id),
             "logical_part_level",
             logical_schema->get_part_level(),
             "kv_part_level",
             kv_schema->get_part_level());
    if (PARTITION_LEVEL_ZERO == kv_schema->get_part_level()) {
      if (OB_FAIL(kv_schema->get_tablet_and_object_id(kv_tablet_id, kv_object_id))) {
        LOG_WARN("get_tablet_and_object_id failed for LEVEL_ZERO kv table",
                 K(ret), K(kv_table_id));
      } else {
        LOG_TRACE("[LT hybrid DAS] resolve_agent_db_kv_tablet_via_logical_tablet_ LEVEL_ZERO ok",
                 K(kv_table_id),
                 K(kv_tablet_id),
                 K(kv_object_id));
      }
    } else {
      int64_t part_idx = 0;
      int64_t subpart_idx = 0;
      ObObjectID kv_first_level_part_id = OB_INVALID_ID;
      if (OB_FAIL(logical_schema->get_part_idx_by_tablet(
              logical_anchor_tablet, part_idx, subpart_idx))) {
        LOG_WARN("get part_idx from logical tablet failed", K(ret),
                 K(logical_anchor_tablet),
                 K(logical_ref_table_id),
                 K(kv_table_id),
                 "logical_part_level",
                 logical_schema->get_part_level());
      } else if (OB_FAIL(kv_schema->get_part_id_and_tablet_id_by_idx(
                     part_idx, subpart_idx, kv_object_id, kv_first_level_part_id, kv_tablet_id))) {
        LOG_WARN("get kv tablet by part_idx failed", K(ret), K(part_idx), K(subpart_idx),
                 K(kv_table_id));
      } else {
        LOG_TRACE("[LT hybrid DAS] resolve_agent_db_kv_tablet_via_logical_tablet_ partitioned ok",
                 K(logical_anchor_tablet),
                 K(part_idx),
                 K(subpart_idx),
                 K(kv_object_id),
                 K(kv_first_level_part_id),
                 K(kv_tablet_id));
      }
    }
  }
  return ret;
}

int ObDASScanOp::create_async_index_query_ctx()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObSqcCtx *sqc_ctx = nullptr;
  ObSEArray<uint64_t, 2> async_table_ids;
  if (OB_ISNULL(snapshot_) || OB_ISNULL(scan_rtdef_) || OB_ISNULL(scan_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid state for async index query", K(ret));
  } else if (OB_FAIL(ObDASAsyncIndexQueryCtx::collect_async_index_table_ids(
                 attach_ctdef_, async_table_ids))) {
    LOG_WARN("unsupported async index query shape", K(ret));
  } else {
    ObExecContext &exec_ctx = scan_rtdef_->eval_ctx_->exec_ctx_;
    if (OB_NOT_NULL(async_query_ctx_)) {
      // The immutable catalog was injected by TableScanOp, or retained across a task reopen.
    } else if (OB_NOT_NULL(exec_ctx.get_sqc_handler())) {
      sqc_ctx = &exec_ctx.get_sqc_handler()->get_sqc_ctx();
      if (OB_UNLIKELY(OB_INVALID_ID == scan_rtdef_->scan_op_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid PX scan op id for async index query", K(ret));
      } else if (OB_FAIL(sqc_ctx->get_async_index_query_ctx(
                     scan_rtdef_->scan_op_id_, async_query_ctx_))) {
        LOG_WARN("async index query catalog was not prepared by SQC", K(ret),
                 K(scan_rtdef_->scan_op_id_));
      }
      own_async_query_ctx_ = false;
    } else if (OB_NOT_NULL(scan_rtdef_->table_loc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local async index query catalog was not prepared by table scan", K(ret),
               K(scan_rtdef_->scan_op_id_), K_(tablet_id));
    } else if (OB_ISNULL(buf = op_alloc_.alloc(sizeof(ObDASAsyncIndexQueryCtx)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate remote async index query ctx", K(ret));
    } else {
      async_query_ctx_ = new (buf) ObDASAsyncIndexQueryCtx();
      own_async_query_ctx_ = true;
      if (OB_FAIL(build_async_index_query_ctx(*async_query_ctx_))) {
        LOG_WARN("failed to build remote async index query catalog", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && (OB_ISNULL(async_query_ctx_) || !async_query_ctx_->is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid prepared async index query catalog", K(ret),
             K(scan_rtdef_->scan_op_id_), KP(async_query_ctx_));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(search_ctx_)) {
    const ObAsyncIndexPartitionSegCtx *partition = nullptr;
    if (OB_FAIL(async_query_ctx_->find_partition(tablet_id_, partition))) {
      LOG_WARN("current partition missing from async index query catalog", K(ret), K_(tablet_id));
    } else {
      search_ctx_->set_async_query_ctx(async_query_ctx_);
      search_ctx_->set_current_async_partition(partition);
    }
  }
  if (OB_FAIL(ret) && own_async_query_ctx_ && OB_NOT_NULL(async_query_ctx_)) {
    async_query_ctx_->~ObDASAsyncIndexQueryCtx();
    op_alloc_.free(async_query_ctx_);
    async_query_ctx_ = nullptr;
    own_async_query_ctx_ = false;
  }
  return ret;
}

int ObDASScanOp::switch_async_index_partition_on_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(async_query_ctx_)) {
    // no async ctx, nothing to switch
  } else if (OB_ISNULL(search_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null search ctx on async partition switch", K(ret));
  } else {
    const ObAsyncIndexPartitionSegCtx *partition = nullptr;
    if (OB_FAIL(async_query_ctx_->find_partition(tablet_id_, partition))) {
      LOG_WARN("partition missing from immutable async index query catalog", K(ret), K_(tablet_id));
    } else {
      search_ctx_->set_current_async_partition(partition);
    }
  }
  return ret;
}

int ObDASScanOp::init_logical_table_ctx_for_search_(
    ObDASSearchCtx *search_ctx,
    const ObDASFusionCtDef *fusion_ctdef)
{
  int ret = OB_SUCCESS;
  const ObDASBaseCtDef *scalar_ctdef = nullptr;
  if (OB_ISNULL(search_ctx) || OB_ISNULL(fusion_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(search_ctx), KP(fusion_ctdef));
  } else if (OB_FAIL(ObDASUtils::find_child_das_ctdef(
                 attach_ctdef_, DAS_OP_SCALAR_SCAN_QUERY, scalar_ctdef))) {
    LOG_WARN("failed to find scalar scan ctdef", K(ret));
  } else {
    ObLogicalTableSearchCtx pending_lt_ctx;
    bool inited_from_scalar = false;
    if (OB_NOT_NULL(scalar_ctdef)) {
      const ObDASScalarScanCtDef *scalar = static_cast<const ObDASScalarScanCtDef *>(scalar_ctdef);
      LOG_TRACE("[LT hybrid DAS] init_logical_table_ctx_for_search_ found scalar_ctdef",
               "ref_table_id", scalar->ref_table_id_,
               "is_logical_table", scalar->is_logical_table(),
               "aux_type", static_cast<int>(scalar->logical_table_aux_type_));
      if (scalar->is_logical_table()) {
        pending_lt_ctx.physical_data_table_id_ = scalar->physical_data_table_id_;
        pending_lt_ctx.logical_table_aux_type_ = scalar->logical_table_aux_type_;
        pending_lt_ctx.is_enabled_ = true;
        inited_from_scalar = true;
      }
    }
    if (OB_SUCC(ret) && !inited_from_scalar) {
      const ObDASScanCtDef *lt_kv_scan = nullptr;
      if (OB_FAIL(ObDASUtils::find_first_logical_table_das_scan_ctdef(attach_ctdef_, lt_kv_scan))) {
        LOG_WARN("find logical table das scan ctdef failed", K(ret));
      } else if (OB_NOT_NULL(lt_kv_scan)) {
        LOG_TRACE("[LT hybrid DAS] init_logical_table_ctx_for_search_ using DAS_TABLE_SCAN logical KV",
                 K(lt_kv_scan->physical_data_table_id_),
                 "ref_table_id", lt_kv_scan->ref_table_id_);
        pending_lt_ctx.physical_data_table_id_ = lt_kv_scan->physical_data_table_id_;
        pending_lt_ctx.logical_table_aux_type_ = lt_kv_scan->logical_table_aux_type_;
        pending_lt_ctx.is_enabled_ = true;
      } else {
        pending_lt_ctx.is_enabled_ = false;
        pending_lt_ctx.logical_table_aux_type_ = share::LT_AUX_MAX_TYPE;
      }
    }
    if (OB_SUCC(ret) && pending_lt_ctx.is_enabled_) {
      if (OB_ISNULL(scan_rtdef_) || OB_ISNULL(scan_rtdef_->eval_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scan_rtdef or eval_ctx is null", K(ret));
      } else {
        ObSQLSessionInfo *session = scan_rtdef_->eval_ctx_->exec_ctx_.get_my_session();
        uint64_t namespace_id = 0;
        uint64_t ltable_id = 0;
        // Read runtime ns/lt from scan_rtdef_->lt_rt_ (populated by init_scan_param expr eval + propagate).
        const ObLogicalTableRtDef &lt_rt = scan_rtdef_->lt_rt_;
        if (!lt_rt.get_resolved_identity(namespace_id, ltable_id)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED,
                         "logical table query must carry namespace_id and ltable_id predicates");
          LOG_WARN("op-level lt_ctx_ has no runtime ns/lt for hybrid search", K(ret),
                   KP(scan_ctdef_), K(lt_rt));
        }
        if (OB_FAIL(ret)) {
        } else if (share::OB_INVALID_NAMESPACE_ID == namespace_id
          || share::OB_INVALID_LOGICAL_TABLE_ID == ltable_id) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid logical table namespace_id or ltable_id");
          LOG_WARN("invalid runtime ns/lt for hybrid search", K(ret), K(namespace_id), K(ltable_id));
        } else {
          uint64_t tenant_id = OB_INVALID_ID;
          ObObjectID kv_object_id = OB_INVALID_OBJECT_ID;
          const uint64_t logical_ref_table_id = scan_ctdef_->ref_table_id_;
          ObTabletID physical_tablet_id;
          if (OB_ISNULL(session)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session is null for kv tablet resolve", K(ret));
          } else if (FALSE_IT(tenant_id = session->get_effective_tenant_id())) {
          } else if (OB_FAIL(ObLogicalTableHelper::resolve_kv_tablet_via_logical_anchor(
                         tenant_id, logical_ref_table_id, pending_lt_ctx.physical_data_table_id_, tablet_id_,
                         physical_tablet_id, kv_object_id))) {
            LOG_WARN("resolve kv tablet via logical tablet failed", K(ret),
                     K(pending_lt_ctx.physical_data_table_id_), K(logical_ref_table_id), K(tablet_id_));
          } else {
            pending_lt_ctx.namespace_id_ = namespace_id;
            pending_lt_ctx.logical_table_id_ = ltable_id;
            pending_lt_ctx.physical_tablet_id_ = physical_tablet_id;
          }
          if (OB_SUCC(ret)) {
            LOG_TRACE("[LT hybrid DAS] init_logical_table_ctx_for_search_ enabled", K(pending_lt_ctx.namespace_id_),
                     K(pending_lt_ctx.logical_table_id_), K(pending_lt_ctx.physical_data_table_id_),
                     K(pending_lt_ctx.physical_tablet_id_),
                     "scan_ctdef_baked_ns_lt",
                     (OB_NOT_NULL(scan_ctdef_)));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      search_ctx->get_logical_table_ctx() = pending_lt_ctx;
    }
    if (OB_SUCC(ret) && !pending_lt_ctx.is_enabled_) {
      LOG_TRACE("[LT hybrid DAS] init_logical_table_ctx_for_search_ NOT enabled",
               K(pending_lt_ctx.is_enabled_), K(pending_lt_ctx.physical_data_table_id_));
    }
  }
  return ret;
}

int ObDASScanOp::create_iter_tree_and_init_tablet_ids(const ObDASIterTreeType &tree_type, ObDASIter *&result)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(scan_rtdef_)
      && ObDASUtils::scan_task_hosts_logical_table_aux(scan_ctdef_, attach_ctdef_)
      && scan_rtdef_->lt_rt_.has_resolved_ns_lt()) {
    if (OB_FAIL(ObDASUtils::propagate_lt_rt_for_scan_task(
            scan_rtdef_->lt_rt_,
            attach_ctdef_,
            attach_rtdef_,
            get_lookup_ctdef(),
            get_lookup_rtdef(),
            &related_ctdefs_,
            &related_rtdefs_))) {
      LOG_WARN("propagate lt_rt to logical attach subtree failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ITER_TREE_HYBRID_SEARCH == tree_type) {
    if (OB_FAIL(create_das_search_context())) {
      LOG_WARN("failed to create das search context", K(ret));
    } else if (is_async_index_search() && OB_FAIL(create_async_index_query_ctx())) {
      // async fulltext: build segment ctx (once, all partitions x async tablets) before tree build
      LOG_WARN("failed to create async index query ctx", K(ret));
    } else if (OB_FAIL(create_das_iter_tree_internal(tree_type, result))) {
      LOG_WARN("failed to create das scan iter tree", K(tree_type), K(ret));
    } else if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr result", K(ret));
    } else if (OB_FAIL(set_hybrid_search_tablet_ids(result))) {
      LOG_WARN("failed to set hybrid search related ids", K(ret));
    }
  } else if (OB_FAIL(init_related_tablet_ids(tablet_ids_))) {
    LOG_WARN("failed to init related tablet ids", K(ret));
  } else if (OB_FAIL(create_das_iter_tree_internal(tree_type, result))) {
    LOG_WARN("failed to create das scan iter tree", K(tree_type), K(ret));
  }
  return ret;
}

int ObDASScanOp::create_das_iter_tree_internal(const ObDASIterTreeType &tree_type, ObDASIter *&result)
{
  return ObDASIterUtils::create_das_scan_iter_tree(tree_type, scan_param_, scan_ctdef_, scan_rtdef_, 
    get_lookup_ctdef(), get_lookup_rtdef(), attach_ctdef_, attach_rtdef_, tablet_ids_, trans_desc_, 
    snapshot_, op_alloc_, search_ctx_, result);
}

int ObDASScanOp::set_hybrid_search_tablet_ids(ObDASIter *result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr result", K(ret));
  } else {
    switch (result->get_type()) {
      case DAS_ITER_SCAN:
      case DAS_ITER_SEARCH_DRIVER:
      case DAS_ITER_FUSION:
      case DAS_ITER_PROFILE: {
        break;
      }
      case DAS_ITER_LOCAL_LOOKUP: {
        ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter *>(result);
        common::ObTabletID tablet_id;
        if (OB_FAIL(get_related_tablet_id(lookup_iter->get_lookup_ctdef(), tablet_id))) {
          LOG_WARN("failed to get lookup tablet id", K(ret));
        } else {
          lookup_iter->set_tablet_id(tablet_id);
          lookup_iter->set_ls_id(ls_id_);
        }
        break;
      }
      case DAS_ITER_VEC_INDEX_DRIVER: {
        ObDASVecIndexDriverIter *vec_index_driver_iter = static_cast<ObDASVecIndexDriverIter *>(result);
        ObDASVecIndexScanIter *vec_index_scan_iter = vec_index_driver_iter->get_vec_index_scan_iter();
        const ObDASVecIndexDriverCtDef *driver_ctdef = vec_index_driver_iter->get_driver_ctdef();
        ObDASRelatedTabletID related_tablet_ids(op_alloc_);
        if (OB_ISNULL(vec_index_scan_iter) || OB_ISNULL(driver_ctdef)
            || driver_ctdef->children_cnt_ < 1 || OB_ISNULL(driver_ctdef->children_)
            || OB_ISNULL(driver_ctdef->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr in vec index driver iter", K(ret), KP(vec_index_scan_iter), KP(driver_ctdef));
        } else if (driver_ctdef->children_[0]->op_type_ == DAS_OP_VEC_INDEX_BRUTE_SCAN) {
          const ObDASVecIndexBruteScanCtDef *brute_ctdef =
              static_cast<const ObDASVecIndexBruteScanCtDef *>(driver_ctdef->children_[0]);
          const ObDASScanCtDef *main_table_ctdef = nullptr;
          if (OB_ISNULL(brute_ctdef)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr brute scan ctdef", K(ret));
          } else if (OB_ISNULL(main_table_ctdef = brute_ctdef->get_main_table_ctdef())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr brute force main table ctdef", K(ret));
          } else if (OB_FAIL(get_related_tablet_id(main_table_ctdef,
                                                   related_tablet_ids.lookup_tablet_id_))) {
            LOG_WARN("failed to get brute force main table tablet id", K(ret));
          } else {
            vec_index_driver_iter->set_related_tablet_ids(related_tablet_ids);
            vec_index_scan_iter->set_related_tablet_ids(related_tablet_ids);
            vec_index_driver_iter->set_ls_id(ls_id_);
            vec_index_scan_iter->set_ls_id(ls_id_);
          }
        } else if (driver_ctdef->children_[0]->op_type_ == DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN) {
          const auto *scan_ctdef =
              static_cast<const ObDASVecIndexAsyncIVFScanCtDef *>(driver_ctdef->children_[0]);
          common::ObTabletID index_tablet_id;
          common::ObTabletID delete_tablet_id;
          if (OB_ISNULL(scan_ctdef->get_index_scan_ctdef())
              || OB_ISNULL(scan_ctdef->get_delete_scan_ctdef())) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(get_related_tablet_id(scan_ctdef->get_index_scan_ctdef(), index_tablet_id))) {
            LOG_WARN("failed to resolve async IVF index tablet", K(ret));
          } else if (OB_FAIL(get_related_tablet_id(scan_ctdef->get_delete_scan_ctdef(), delete_tablet_id))) {
            LOG_WARN("failed to resolve async IVF deletion tablet", K(ret));
          } else {
            // The adapter binds pinned index ranges through the shared query catalog.
            // Scalar post-filter lookup uses the current data partition.
            related_tablet_ids.lookup_tablet_id_ = scan_param_.tablet_id_;
            vec_index_driver_iter->set_related_tablet_ids(related_tablet_ids);
            vec_index_driver_iter->set_ls_id(ls_id_);
            vec_index_scan_iter->set_ls_id(ls_id_);
          }
        } else if (driver_ctdef->children_[0]->op_type_ == DAS_OP_VEC_INDEX_HNSW_SCAN) {
          const ObDASVecIndexHNSWScanCtDef *scan_ctdef =
              static_cast<const ObDASVecIndexHNSWScanCtDef *>(driver_ctdef->children_[0]);
          if (OB_ISNULL(scan_ctdef)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr hnsw scan ctdef", K(ret));
          } else if (OB_NOT_NULL(scan_ctdef->get_delta_buf_table_ctdef()) &&
                     OB_FAIL(get_related_tablet_id(scan_ctdef->get_delta_buf_table_ctdef(),
                                                   related_tablet_ids.delta_buf_tablet_id_))) {
            LOG_WARN("failed to get delta buf tablet id", K(ret));
          } else if (OB_NOT_NULL(scan_ctdef->get_index_id_table_ctdef()) &&
                     OB_FAIL(get_related_tablet_id(scan_ctdef->get_index_id_table_ctdef(),
                                                   related_tablet_ids.index_id_tablet_id_))) {
            LOG_WARN("failed to get index id tablet id", K(ret));
          } else if (OB_NOT_NULL(scan_ctdef->get_snapshot_table_ctdef()) &&
                     OB_FAIL(get_related_tablet_id(scan_ctdef->get_snapshot_table_ctdef(),
                                                   related_tablet_ids.snapshot_tablet_id_))) {
            LOG_WARN("failed to get snapshot tablet id", K(ret));
          } else if (OB_NOT_NULL(scan_ctdef->get_com_aux_vec_table_ctdef()) &&
                     OB_FAIL(get_related_tablet_id(scan_ctdef->get_com_aux_vec_table_ctdef(),
                                                   related_tablet_ids.lookup_tablet_id_))) {
            LOG_WARN("failed to get com aux vec tablet id", K(ret));
          } else {
            vec_index_driver_iter->set_related_tablet_ids(related_tablet_ids);
            vec_index_scan_iter->set_related_tablet_ids(related_tablet_ids);
            vec_index_driver_iter->set_ls_id(ls_id_);
            vec_index_scan_iter->set_ls_id(ls_id_);
          }
        } else if (OB_FAIL(get_vec_ir_tablet_ids(related_tablet_ids))) {
          LOG_WARN("failed to get vec index related tablet id", K(ret));
        } else {
          vec_index_driver_iter->set_related_tablet_ids(related_tablet_ids);
          vec_index_scan_iter->set_related_tablet_ids(related_tablet_ids);
          vec_index_driver_iter->set_ls_id(ls_id_);
          vec_index_scan_iter->set_ls_id(ls_id_);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected iter type", K(ret), K(result->get_type()));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < result->get_children_cnt(); i++) {
      OB_ASSERT(OB_NOT_NULL(result->get_children()[i]));
      if (OB_FAIL(SMART_CALL(set_hybrid_search_tablet_ids(result->get_children()[i])))) {
        LOG_WARN("failed to set hybrid search tablet ids", K(ret));
      }
    }
  }
  return ret;
}

int ObDASScanOp::do_inject_kv_lt_ctx_(ObDASScanIter *scan_iter,
                                      const ObDASScanCtDef *scan_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(scan_ctdef->pd_expr_spec_.access_exprs_.count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[LT DAS] inject kv lt_ctx: access_exprs count < 2", K(ret),
             "access_cnt", scan_ctdef->pd_expr_spec_.access_exprs_.count(),
             KP(scan_ctdef));
  } else {
    ObExpr *kv_key = scan_ctdef->pd_expr_spec_.access_exprs_.at(0);
    ObExpr *kv_value = scan_ctdef->pd_expr_spec_.access_exprs_.at(1);
    scan_iter->init_lt_from_defs(
        scan_rtdef_->lt_rt_, scan_ctdef, nullptr, scan_rtdef_->lt_scan_rt_,
        kv_key, kv_value);
  }
  return ret;
}

int ObDASScanOp::inject_logical_lookup_lt_ctx_(ObDASIter *iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    // skip
  } else if (iter->get_type() == DAS_ITER_LOCAL_LOOKUP
          || iter->get_type() == DAS_ITER_GLOBAL_LOOKUP) {
    ObDASLookupIter *lookup_iter = static_cast<ObDASLookupIter *>(iter);
    const ObDASScanCtDef *lookup_ctdef = lookup_iter->get_lookup_ctdef();
    ObDASIter *index_iter = lookup_iter->get_index_table_iter();
    const ObDASBaseCtDef *index_ctdef = lookup_iter->get_index_ctdef();
    // Func lookup / index-proj lookup: index_table_iter_ is the driver rowkey scan.
    // It must decode logical_output_exprs_ (e.g. __pk_increment) before CacheLookup
    // reads lookup_domain_id_expr_ from eval_ctx.
    if (OB_NOT_NULL(index_iter) && DAS_ITER_SCAN == index_iter->get_type()
        && OB_NOT_NULL(index_ctdef)
        && ObDASOpType::DAS_OP_TABLE_SCAN == index_ctdef->op_type_) {
      const ObDASScanCtDef *index_scan_ctdef = static_cast<const ObDASScanCtDef *>(index_ctdef);
      if (index_scan_ctdef->is_logical_table()
          && index_scan_ctdef->pd_expr_spec_.access_exprs_.count() >= 2) {
        if (OB_FAIL(do_inject_kv_lt_ctx_(static_cast<ObDASScanIter *>(index_iter),
                                         index_scan_ctdef))) {
          LOG_WARN("[LT DAS] inject index (rowkey) scan lt_ctx failed", K(ret));
        } else {
          LOG_TRACE("[LT DAS] inject index (rowkey) scan lt_ctx ok",
                    KP(index_iter), K(index_scan_ctdef->physical_data_table_id_));
        }
      }
    }
    ObDASIter *data_iter = lookup_iter->get_data_table_iter();
    ObDASScanIter *scan_iter = das_hybrid_resolve_lookup_data_scan_iter(data_iter);
    if (OB_ISNULL(scan_iter)) {
      // data_table_iter is not ObDASScanIter-backed (e.g. FTS / domain-index FUNC_LOOKUP
      // whose data_iter is DAS_ITER_FUNC_DATA). Such lookups are not logical-table
      // KV-lookups: neither lt_ctx inject nor logical_table_redirect applies. Skip
      // silently; only raise OB_ERR_UNEXPECTED when data_iter is expected to resolve
      // (SCAN/DOMAIN_ID_MERGE) but doesn't, to keep a defensive guard.
      if (OB_NOT_NULL(data_iter)
          && (data_iter->get_type() == DAS_ITER_SCAN
              || data_iter->get_type() == DAS_ITER_DOMAIN_ID_MERGE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT DAS] inject: cannot resolve ObDASScanIter from data_table_iter",
                 KP(data_iter), "data_iter_type", data_iter->get_type());
      } else {
        LOG_TRACE("[LT DAS] inject: skip non-KV-lookup path",
                  KP(data_iter),
                  "data_iter_type",
                  OB_NOT_NULL(data_iter) ? data_iter->get_type()
                                          : ObDASIterType::DAS_ITER_INVALID);
      }
    } else {
      // Only the KV-lookup path (data_iter is ObDASScanIter) needs redirect + inject.
      // set_logical_table_redirect must NOT be called for non-KV paths, otherwise
      // lt_enabled_=true would rewrite lookup_param_.index_id_ to the KV physical
      // table in do_index_lookup and break FTS/domain-index lookup.
      if (OB_ISNULL(lookup_ctdef)
          || lookup_ctdef->pd_expr_spec_.access_exprs_.count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT DAS] inject: lookup_ctdef missing or kv access_exprs count < 2",
                 KP(lookup_ctdef),
                 "access_cnt",
                 (OB_NOT_NULL(lookup_ctdef) ? lookup_ctdef->pd_expr_spec_.access_exprs_.count() : -1));
      } else {
        if (iter->get_type() == DAS_ITER_LOCAL_LOOKUP) {
          static_cast<ObDASLocalLookupIter *>(iter)->set_logical_table_redirect(
              scan_rtdef_->lt_rt_.enabled_, lookup_ctdef->physical_data_table_id_);
        }
        if (OB_FAIL(do_inject_kv_lt_ctx_(scan_iter, lookup_ctdef))) {
          LOG_WARN("[LT DAS] inject lookup lt_ctx failed", K(ret));
        } else {
          LOG_TRACE("[LT DAS] inject lookup lt_ctx ok",
                    KP(scan_iter), K(lookup_ctdef->physical_data_table_id_));
        }
      }
    }
  } else if (iter->get_type() == DAS_ITER_FUNC_DATA) {
    // Hybrid-search / FTS FUNC_DATA keeps main-table lookup in an internal ObDASScanIter
    // (not under LOCAL_LOOKUP.data_table_iter). Inject lt_ctx there for logical tables.
    ObDASFuncDataIter *func_data_iter = static_cast<ObDASFuncDataIter *>(iter);
    const ObDASScanCtDef *main_lookup_ctdef = func_data_iter->get_main_lookup_ctdef();
    ObDASIter *main_lookup_iter = func_data_iter->get_main_lookup_iter();
    if (OB_NOT_NULL(main_lookup_iter) && OB_NOT_NULL(main_lookup_ctdef)
        && main_lookup_ctdef->is_logical_table()) {
      if (OB_UNLIKELY(DAS_ITER_SCAN != main_lookup_iter->get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT DAS] inject: func_data main_lookup is not SCAN",
                 K(ret), "main_lookup_type", main_lookup_iter->get_type());
      } else if (main_lookup_ctdef->pd_expr_spec_.access_exprs_.count() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT DAS] inject: func_data main_lookup ctdef kv access_exprs count < 2",
                 K(ret), KP(main_lookup_ctdef),
                 "access_cnt", main_lookup_ctdef->pd_expr_spec_.access_exprs_.count());
      } else if (OB_FAIL(do_inject_kv_lt_ctx_(static_cast<ObDASScanIter *>(main_lookup_iter),
                                               main_lookup_ctdef))) {
        LOG_WARN("[LT DAS] inject func_data main_lookup lt_ctx failed", K(ret));
      } else {
        LOG_TRACE("[LT DAS] inject func_data main_lookup lt_ctx ok",
                  KP(main_lookup_iter), K(main_lookup_ctdef->physical_data_table_id_));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(iter) && i < iter->get_children_cnt(); ++i) {
    if (OB_NOT_NULL(iter->get_children()[i])) {
      if (OB_FAIL(SMART_CALL(inject_logical_lookup_lt_ctx_(iter->get_children()[i])))) {
        LOG_WARN("failed to inject logical lookup lt_ctx on child", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObDASScanOp::open_op()
{
  int ret = OB_SUCCESS;
  //Retry may be called many times.
  //Only for DASScanOp now, we add a retry alloc to avoid
  //memory expansion.
  if (in_part_retry_) {
    init_retry_alloc();
  }
  reset_access_datums_ptr();
  ObDASIterTreeType tree_type = ITER_TREE_INVALID;
  if (OB_FAIL(init_scan_param())) {
    LOG_WARN("init scan param failed", K(ret));
  } else if (OB_FAIL(check_merge_range_opt())) {
    LOG_WARN("failed to check merge range opt");
  } else if (FALSE_IT(tree_type = get_iter_tree_type())) {
  } else if (SUPPORTED_DAS_ITER_TREE(tree_type)) {
    ObDASIter *result = nullptr;
    if (OB_FAIL(create_iter_tree_and_init_tablet_ids(tree_type, result))) {
      LOG_WARN("failed to create das scan iter tree", K(tree_type), K(ret));
    } else if (ITER_TREE_HYBRID_SEARCH == tree_type && OB_FAIL(ERRSIM_DAS_ITER_PUBLISH_FAILED)) {
      LOG_WARN("simulate failure before publishing das iter tree", K(ret));
    } else if (scan_rtdef_->topn_param_.is_valid() && tree_type != ITER_TREE_INDEX_MERGE &&
               OB_FAIL(result->prepare_limit_pushdown_param(scan_ctdef_->push_down_topn_, scan_rtdef_->topn_param_))) {
      LOG_WARN("failed to prepare limit pushdown param", K(ret));
    } else {
      result_ = result;
      // A. main-table direct scan: inject lt projection fields into the root SCAN iter.
      // For a logical-table scan with domain-id merge attached, the root iter is the
      // merge iter; the data-table child (children_[0]) is the ObDASScanIter that
      // decodes logical columns (namespace_id/ltable_id/...) from kv_value. Without
      // this injection, lt_rt_for_proj_.enabled_ stays false on that child, so
      // project_logical_columns is skipped and the logical columns are left as
      // un-decoded datums, which makes upper-layer filters dereference garbage and
      // crash (SIGSEGV). Range rewrite is unaffected: init_scan_param already built
      // physical KV primary ranges at the op level via scan_rtdef_->lt_rt_.
      if (scan_ctdef_->is_logical_table() && scan_rtdef_->lt_rt_.enabled_) {
        // kv_key/value_expr_ are NOT stored in ctdef->lt_ctx_ (CG doesn't set them);
        // they come from pd_expr_spec_.access_exprs_ at runtime.
        ObExpr *kv_key = (scan_ctdef_->pd_expr_spec_.access_exprs_.count() >= 2)
            ? scan_ctdef_->pd_expr_spec_.access_exprs_.at(0) : nullptr;
        ObExpr *kv_value = (scan_ctdef_->pd_expr_spec_.access_exprs_.count() >= 2)
            ? scan_ctdef_->pd_expr_spec_.access_exprs_.at(1) : nullptr;
        ObDASScanIter *lt_scan_iter = nullptr;
        if (DAS_ITER_SCAN == result->get_type()) {
          lt_scan_iter = static_cast<ObDASScanIter *>(result);
        } else if (DAS_ITER_DOMAIN_ID_MERGE == result->get_type()
                   && 0 < result->get_children_cnt()
                   && OB_NOT_NULL(result->get_children()[0])
                   && DAS_ITER_SCAN == result->get_children()[0]->get_type()) {
          // data-table child of the domain-id merge iter.
          lt_scan_iter = static_cast<ObDASScanIter *>(result->get_children()[0]);
        }
        if (OB_NOT_NULL(lt_scan_iter)) {
          lt_scan_iter->init_lt_from_defs(
              scan_rtdef_->lt_rt_, scan_ctdef_, nullptr, scan_rtdef_->lt_scan_rt_,
              kv_key, kv_value);
        }
      }
      const ObDASScanCtDef *lookup_ctdef_for_inject = get_lookup_ctdef();
      const bool need_lookup_inject = scan_rtdef_->lt_rt_.enabled_
          && ((scan_ctdef_->is_logical_table())
              || (OB_NOT_NULL(lookup_ctdef_for_inject)
                  && lookup_ctdef_for_inject->is_logical_table()));
      if (need_lookup_inject) {
        if (OB_FAIL(inject_logical_lookup_lt_ctx_(result))) {
          LOG_WARN("inject logical lookup lt_ctx failed", K(ret));
        }
      } else if (scan_ctdef_->is_logical_table()
                 || (OB_NOT_NULL(lookup_ctdef_for_inject)
                     && lookup_ctdef_for_inject->is_logical_table())) {
        // legal stable state: session lacks @namespace_id/@ltable_id or PX
        // worker capture missed and deserialized ns/lt are INVALID. Keep the
        // empty-result fallback semantic rather than raising OB_ERR_UNEXPECTED.
        LOG_WARN("[LT DAS] scan/lookup ctdef is logical but lt_rt not enabled, "
                 "skip inject and fall back to empty-result path",
                 K(scan_rtdef_->lt_rt_.namespace_id_),
                 K(scan_rtdef_->lt_rt_.logical_table_id_),
                 K(scan_rtdef_->lt_rt_.enabled_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(result->do_table_scan())) {
        LOG_WARN("iter tree failed to do table scan", K(ret));
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(result) && result_ != result) {
      // Construction succeeded, but tablet IDs or limit initialization failed
      // before release_op() acquired the root.
      const int release_ret = result->release();
      if (OB_SUCCESS != release_ret) {
        LOG_WARN("failed to release unpublished das iter tree", K(release_ret));
      }
      result = nullptr;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter tree type", K(ret), K(tree_type));
  }

  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_LOOKUPOP_INIT_ERROR);
  if (OB_UNLIKELY(OB_SUCCESS != simulate_error)) {
    ret = simulate_error;
  }
  return ret;
}

int ObDASScanOp::release_op()
{
  int ret = OB_SUCCESS;
  ObDASIterTreeType tree_type = get_iter_tree_type();
  if (SUPPORTED_DAS_ITER_TREE(tree_type)) {
    if (OB_NOT_NULL(result_)) {
      ObDASIter *result = static_cast<ObDASIter*>(result_);
      if (OB_FAIL(result->release())) {
        LOG_WARN("failed to release das iter tree", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter tree type", K(ret), K(tree_type));
  }
  //need to clear the flag:need_switch_param_
  //otherwise table_rescan will jump to the switch iterator path in retry
  scan_param_.need_switch_param_ = false;
  scan_param_.destroy_schema_guard();

  if (retry_alloc_ != nullptr) {
    retry_alloc_->reset();
    retry_alloc_ = nullptr;
  }
  tablet_ids_.reset();
  if (OB_NOT_NULL(search_ctx_)) {
    search_ctx_->reset();
    op_alloc_.free(search_ctx_);
    search_ctx_ = nullptr;
  }
  if (own_async_query_ctx_ && OB_NOT_NULL(async_query_ctx_)) {
    async_query_ctx_->~ObDASAsyncIndexQueryCtx();
    op_alloc_.free(async_query_ctx_);
    async_query_ctx_ = nullptr;
    own_async_query_ctx_ = false;
  }
  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_LOOKUPOP_INIT_ERROR);
  if (OB_UNLIKELY(OB_SUCCESS != simulate_error)) {
    if (result_ != nullptr) {
      if (ObNewRowIterator::IterType::ObLocalIndexLookupIterator == result_->get_type() ||
          ObNewRowIterator::IterType::ObGroupLookupOp == result_->get_type()) {
        ObLocalIndexLookupOp *lookup_op = static_cast<ObLocalIndexLookupOp*>(result_);
        if (lookup_op->get_rowkey_iter() != nullptr && lookup_op->get_rowkey_iter()->get_type() == ObNewRowIterator::ObTableScanIterator) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("after executing release_op(), the rowkey_iter_ is still not null", K(ret));
        }
      }
    }
  }
  return ret;
}

//output row from remote DAS task
//maybe change the expr datum ptr to its RPC datum store
//if we need to fetch next row from the next das task,
//must reset the datum ptr to expr preallocate frame buffer
//otherwise, get_next_row in the local das task maybe has a wrong status
void ObDASScanOp::reset_access_datums_ptr(int64_t capacity)
{
  if (scan_rtdef_->p_pd_expr_op_->is_vectorized() && OB_NOT_NULL(scan_rtdef_->eval_ctx_)) {
    int64_t reset_batch_size = capacity > 0 ? capacity : scan_rtdef_->eval_ctx_->max_batch_size_;
    reset_batch_size = min(reset_batch_size, scan_rtdef_->eval_ctx_->max_batch_size_);
    if (attach_ctdef_ != nullptr) {
      reset_access_datums_ptr(attach_ctdef_, *scan_rtdef_->eval_ctx_, reset_batch_size);
    } else {
      reset_access_datums_ptr(scan_ctdef_, *scan_rtdef_->eval_ctx_, reset_batch_size);
      if (get_lookup_ctdef() != nullptr) {
        reset_access_datums_ptr(get_lookup_ctdef(), *scan_rtdef_->eval_ctx_, reset_batch_size);
      }
    }
  }
}

void ObDASScanOp::reset_access_datums_ptr(const ObDASBaseCtDef *ctdef, ObEvalCtx &eval_ctx, int64_t capacity)
{
  if (ctdef != nullptr) {
    if (ctdef->op_type_== DAS_OP_TABLE_SCAN) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(ctdef);
      FOREACH_CNT(e, scan_ctdef->pd_expr_spec_.access_exprs_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.set_point_to_frame(true);
      }
      FOREACH_CNT(e, scan_ctdef->pd_expr_spec_.pd_storage_aggregate_output_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.set_point_to_frame(true);
      }
      FOREACH_CNT(e, scan_ctdef->pd_expr_spec_.ext_file_column_exprs_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.set_point_to_frame(true);
      }
      if (scan_ctdef->trans_info_expr_ != nullptr) {
        ObExpr *trans_expr = scan_ctdef->trans_info_expr_;
        trans_expr->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = trans_expr->get_eval_info(eval_ctx);
        info.set_point_to_frame(true);
      }
    } else if (ctdef->op_type_ == DAS_OP_SCALAR_SCAN_QUERY) {
      const ObDASScalarScanCtDef *scalar_scan_ctdef = static_cast<const ObDASScalarScanCtDef*>(ctdef);
      FOREACH_CNT(e, scalar_scan_ctdef->pd_expr_spec_.access_exprs_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.set_point_to_frame(true);
      }
      FOREACH_CNT(e, scalar_scan_ctdef->pd_expr_spec_.pd_storage_aggregate_output_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.set_point_to_frame(true);
      }
      FOREACH_CNT(e, scalar_scan_ctdef->pd_expr_spec_.ext_file_column_exprs_) {
        (*e)->locate_datums_for_update(eval_ctx, capacity);
        ObEvalInfo &info = (*e)->get_eval_info(eval_ctx);
        info.set_point_to_frame(true);
      }
    } else {
      for (int64_t i = 0; i < ctdef->children_cnt_; i++) {
        reset_access_datums_ptr(ctdef->children_[i], eval_ctx, capacity);
      }
    }
  }
}

//对于远程执行返回的TSC result，DASScanOp解析ObDASTaskResult,
//并将result iterator link到DASScan Task上，输出给对应的TableScan Operator
int ObDASScanOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASScanResult));
  CK(task_id_ == task_result->get_task_id());
  CK(OB_NOT_NULL(scan_rtdef_));
  CK(OB_NOT_NULL(scan_rtdef_->tsc_monitor_info_));
#endif
  if (need_check_output_datum()) {
    reset_access_datums_ptr();
  }
  ObDASScanResult *scan_result = static_cast<ObDASScanResult*>(task_result);
  if (OB_FAIL(scan_result->init_result_iter(&get_result_outputs(),
                                            &(scan_rtdef_->p_pd_expr_op_->get_eval_ctx())))) {
    LOG_WARN("init scan result iterator failed", K(ret));
  } else {
    result_ = scan_result;
    LOG_DEBUG("decode task result", K(*scan_result));
    // Aggregate the remote TSC statistical information into the local monitor node.
    if (OB_NOT_NULL(scan_rtdef_->tsc_monitor_info_)) {
      ObTSCMonitorInfo &tsc_monitor_info = *scan_rtdef_->tsc_monitor_info_;
      tsc_monitor_info.add_io_read_bytes(scan_result->get_io_read_bytes());
      tsc_monitor_info.add_ssstore_read_bytes(scan_result->get_ssstore_read_bytes());
      tsc_monitor_info.add_base_read_row_cnt(scan_result->get_base_read_row_cnt());
      tsc_monitor_info.add_delta_read_row_cnt(scan_result->get_delta_read_row_cnt());
    }
  }
  return ret;
}

//远程执行返回的TSC result,通过RPC回包带回给DAS Scheduler，
//如果结果集超过一个RPC，标记RPC包为has_more，剩余结果集通过DTL传输回DAS Scheduler
int ObDASScanOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
  bool added = false;
  int64_t simulate_row_cnt = - EVENT_CALL(EventTable::EN_DAS_SCAN_RESULT_OVERFLOW);
  has_more = false;
  ObDASScanResult &scan_result = static_cast<ObDASScanResult&>(task_result);
  ObChunkDatumStore &datum_store = scan_result.get_datum_store();
  ObTempRowStore &vec_row_store = scan_result.get_vec_row_store();
  bool iter_end = false;
  int64_t loop_times = 0;
  while (OB_SUCC(ret) && !has_more) {
    const ExprFixedArray &result_output = get_result_outputs();
    ObEvalCtx &eval_ctx = scan_rtdef_->p_pd_expr_op_->get_eval_ctx();
    if (loop_times % 16 == 0 && OB_UNLIKELY(IS_INTERRUPTED())) {
      ObInterruptCode code = GET_INTERRUPT_CODE();
      ret = code.code_;
      LOG_WARN("received a interrupt", K(code), K(ret));
    } else {
      if (!scan_rtdef_->p_pd_expr_op_->is_vectorized()) {
        scan_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
        if (OB_FAIL(get_output_result_iter()->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row from result failed", K(ret));
          }
        } else if (OB_UNLIKELY(simulate_row_cnt > 0
                  && datum_store.get_row_cnt() >= simulate_row_cnt)) {
          // simulate a datum store overflow error, send the remaining result through RPC
          has_more = true;
          remain_row_cnt_ = 1;
        } else if (OB_FAIL(datum_store.try_add_row(result_output,
                                                   &eval_ctx,
                                                   das::OB_DAS_MAX_PACKET_SIZE,
                                                   added))) {
          LOG_WARN("try add row to datum store failed", K(ret));
        } else if (!added) {
          has_more = true;
          remain_row_cnt_ = 1;
        }
        if (OB_SUCC(ret) && has_more) {
          LOG_DEBUG("try fill task result", K(simulate_row_cnt),
                    K(datum_store.get_row_cnt()), K(has_more),
                    "output_row", ROWEXPR2STR(eval_ctx, result_output));
        }
      } else {
        int64_t max_batch_size = scan_ctdef_->pd_expr_spec_.max_batch_size_;
        scan_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
        remain_row_cnt_ = 0;
        if (iter_end) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(get_output_result_iter()->get_next_rows(remain_row_cnt_, max_batch_size))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next batch from result failed", K(ret));
          } else {
            iter_end = true;
            ret = OB_SUCCESS;
          }
        }
        if (enable_rich_format()) {
          LOG_DEBUG("rich format fill task result", K(remain_row_cnt_), K(memory_limit), K(ret), K(scan_result));
          if (OB_FAIL(ret) || 0 == remain_row_cnt_) {
          } else if (OB_UNLIKELY(simulate_row_cnt > 0
                    && vec_row_store.get_row_cnt() >= simulate_row_cnt)) {
            // simulate a datum store overflow error, send the remaining result through RPC
            has_more = true;
          } else if (OB_UNLIKELY(OB_FAIL(vec_row_store.try_add_batch(result_output, &eval_ctx,
                                                          remain_row_cnt_, memory_limit,
                                                          added)))) {
            LOG_WARN("try add row to datum store failed", K(ret));
          } else if (!added) {
            has_more = true;
          }
        } else {
          if (OB_FAIL(ret) || 0 == remain_row_cnt_) {
          } else if (OB_UNLIKELY(simulate_row_cnt > 0
                    && datum_store.get_row_cnt() >= simulate_row_cnt)) {
            // simulate a datum store overflow error, send the remaining result through RPC
            has_more = true;
          } else if (OB_UNLIKELY(OB_FAIL(datum_store.try_add_batch(result_output, &eval_ctx,
                                                          remain_row_cnt_, memory_limit,
                                                          added)))) {
            LOG_WARN("try add row to datum store failed", K(ret));
          } else if (!added) {
            has_more = true;
          }
        }
        if (OB_SUCC(ret) && has_more) {
          const ObBitVector *skip = NULL;
          PRINT_VECTORIZED_ROWS(SQL, DEBUG, eval_ctx, result_output, remain_row_cnt_, skip,
                                K(simulate_row_cnt), K(datum_store.get_row_cnt()),
                                K(has_more));
        }
      }
    }
    ++ loop_times;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (enable_rich_format()) {
    memory_limit -= vec_row_store.get_mem_used();
  } else {
    memory_limit -= datum_store.get_mem_used();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(scan_rtdef_->tsc_monitor_info_)) {
    scan_result.add_io_read_bytes(*scan_rtdef_->tsc_monitor_info_->io_read_bytes_);
    scan_result.add_ssstore_read_bytes(*scan_rtdef_->tsc_monitor_info_->ssstore_read_bytes_);
    scan_result.add_base_read_row_cnt(*scan_rtdef_->tsc_monitor_info_->base_read_row_cnt_);
    scan_result.add_delta_read_row_cnt(*scan_rtdef_->tsc_monitor_info_->delta_read_row_cnt_);
    scan_rtdef_->tsc_monitor_info_->reset_stat();
  }
  return ret;
}

int ObDASScanOp::fill_extra_result(const ObDASTCBInterruptInfo &interrupt_info)
{
  int ret = OB_SUCCESS;
  ObDASTaskResultMgr &result_mgr = MTL(ObDataAccessService *)->get_task_res_mgr();
  const ExprFixedArray &result_output = get_result_outputs();
  ObEvalCtx &eval_ctx = scan_rtdef_->p_pd_expr_op_->get_eval_ctx();
  ObNewRowIterator *output_result_iter = get_output_result_iter();
  if (OB_ISNULL(output_result_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("output result iter is null", K(ret));
  } else if (OB_FAIL(result_mgr.save_task_result(task_id_,
                                                 interrupt_info,
                                                 &result_output,
                                                 &eval_ctx,
                                                 *output_result_iter,
                                                 remain_row_cnt_,
                                                 scan_ctdef_,
                                                 scan_rtdef_,
                                                 *this))) {
    LOG_WARN("save task result failed", KR(ret), K(task_id_), K(interrupt_info));
  }
  return ret;
}

int ObDASScanOp::init_task_info(uint32_t row_extend_size)
{
  UNUSED(row_extend_size);
  return OB_SUCCESS;
}

int ObDASScanOp::rescan()
{
  int &ret = errcode_;
  reset_access_datums_ptr();
  scan_param_.tablet_id_ = tablet_id_;
  scan_param_.ls_id_ = ls_id_;
  if (OB_NOT_NULL(search_ctx_)) {
    search_ctx_->set_ls_id(ls_id_);
  }

  LOG_DEBUG("begin to das table rescan",
            "ls_id", scan_param_.ls_id_,
            "tablet_id", scan_param_.tablet_id_,
            "scan_range", scan_param_.key_ranges_,
            "range_pos", scan_param_.range_array_pos_);
  ObDASIterTreeType tree_type = get_iter_tree_type();
  if (SUPPORTED_DAS_ITER_TREE(tree_type)) {
    if (OB_ISNULL(result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr das iter tree", K(ret));
    } else if (OB_NOT_NULL(async_query_ctx_) && OB_FAIL(switch_async_index_partition_on_rescan())) {
      // partition switch: move to the new partition's cached snapshot/segments. The segment-group
      // ctx interface is NOT called again for already-cached partitions (decision: open-time once,
      // fixed for iter lifetime).
      LOG_WARN("failed to switch async index partition on rescan", K(ret), K_(tablet_id));
    } else {
      ObDASIter *result = static_cast<ObDASIter*>(result_);
      if (OB_FAIL(result->rescan())) {
        LOG_WARN("failed to rescan das iter", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter tree type", K(ret), K(tree_type));
  }

  return ret;
}

int ObDASScanOp::reuse_iter()
{
  int &ret = errcode_;
  //May be retry change to retry alloc.
  //Change back.
  scan_param_.scan_allocator_ = &scan_rtdef_->scan_allocator_;
  const ObTabletID &storage_tablet_id = scan_param_.tablet_id_;
  scan_param_.need_switch_param_ = (storage_tablet_id.is_valid() && storage_tablet_id != tablet_id_ ? true : false);
  scan_param_.key_ranges_.reuse();
  scan_param_.ss_key_ranges_.reuse();
  scan_param_.mbr_filters_.reuse();
  ObDASIterTreeType tree_type = get_iter_tree_type();
  if (SUPPORTED_DAS_ITER_TREE(tree_type)) {
    if (OB_ISNULL(result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr das iter tree", K(ret));
    } else {
      ObDASIter *result = static_cast<ObDASIter*>(result_);
      ObDASDomainIdMergeIter *domain_id_merge_iter = nullptr;
      if (ITER_TREE_HYBRID_SEARCH == tree_type) {
        if (OB_FAIL(set_hybrid_search_tablet_ids(result))) {
          LOG_WARN("failed to set hybrid search tablet ids", K(ret));
        } else if (OB_NOT_NULL(search_ctx_)) {
          // GI rescan switches partition: re-resolve KV physical tablet from
          // the freshly-set logical anchor (tablet_id_) and refresh search_ctx_.
          // lookup_iter holds a pointer into search_ctx_->logical_table_ctx_,
          // so updating it here propagates automatically.
          ObLogicalTableSearchCtx &hs_lt_ctx = search_ctx_->get_logical_table_ctx();
          if (hs_lt_ctx.is_enabled_ && OB_NOT_NULL(scan_ctdef_)) {
            const uint64_t tenant_id = MTL_ID();
            const uint64_t logical_ref_table_id = scan_ctdef_->ref_table_id_;
            const ObTabletID old_kv_tablet = hs_lt_ctx.physical_tablet_id_;
            ObTabletID new_kv_tablet;
            ObObjectID new_kv_object = OB_INVALID_OBJECT_ID;
            if (OB_FAIL(ObLogicalTableHelper::resolve_kv_tablet_via_logical_anchor(
                    tenant_id, logical_ref_table_id, hs_lt_ctx.physical_data_table_id_,
                    tablet_id_, new_kv_tablet, new_kv_object))) {
              LOG_WARN("[LT hybrid DAS] failed to refresh kv tablet on reuse_iter",
                       K(ret), K(tenant_id), K(logical_ref_table_id),
                       K(hs_lt_ctx.physical_data_table_id_), K_(tablet_id));
            } else {
              hs_lt_ctx.physical_tablet_id_ = new_kv_tablet;
              LOG_TRACE("[LT hybrid DAS] refreshed kv tablet on reuse_iter",
                       K_(tablet_id), K(old_kv_tablet), K(new_kv_tablet));
            }
          }
        }
      } else if (OB_FAIL(init_related_tablet_ids(tablet_ids_))) {
        LOG_WARN("fail to init related tablet ids", K(ret));
      } else {
        switch (tree_type) {
          case ITER_TREE_PARTITION_SCAN: {
            break;
          }
          case ITER_TREE_LOCAL_LOOKUP: {
            ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter*>(result_);
            lookup_iter->set_tablet_id(tablet_ids_.lookup_tablet_id_);
            lookup_iter->set_ls_id(ls_id_);
            break;
          }
          case ITER_TREE_MATCH: 
          case ITER_TREE_TEXT_RETRIEVAL: {
            ObDASIter *result_iter = static_cast<ObDASIter *>(result_);
            if (OB_FAIL(ObDASIterUtils::set_text_retrieval_related_ids(
                attach_ctdef_, attach_rtdef_, tablet_ids_, ls_id_, result_iter))) {
              LOG_WARN("failed to set text retrieval related ids", K(ret));
            }
            break;
          }
          case ITER_TREE_INDEX_MERGE: {
            ObDASIter *result_iter = static_cast<ObDASIter *>(result_);
            if (OB_FAIL(ObDASIterUtils::set_index_merge_related_ids(
                attach_ctdef_, attach_rtdef_, tablet_ids_, ls_id_, result_iter))) {
              LOG_WARN("failed to set index merge related ids", K(ret));
            } else if (OB_FAIL(try_set_pushdown_topk_related_ids(result_iter))) {
              LOG_WARN("failed to set pushdown topk related ids", K(ret));
            }
            break;
          }
          case ITER_TREE_FUNC_LOOKUP: {
            ObDASIter *result_iter = static_cast<ObDASIter *>(result_);
            if (OB_FAIL(ObDASIterUtils::set_func_lookup_iter_related_ids(
                attach_ctdef_, attach_rtdef_, tablet_ids_, ls_id_, result_iter))) {
              LOG_WARN("failed to set text retrieval related ids", K(ret));
            }
            break;
          }
          case ITER_TREE_MVI_LOOKUP: {
            if (OB_NOT_NULL(get_lookup_ctdef())) {
              ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter*>(result_);
              lookup_iter->set_tablet_id(tablet_ids_.lookup_tablet_id_);
              lookup_iter->set_ls_id(ls_id_);
            }
            break;
          }
          case ITER_TREE_GIS_LOOKUP: {
            ObDASLocalLookupIter *lookup_iter = static_cast<ObDASLocalLookupIter*>(result_);
            lookup_iter->set_tablet_id(tablet_ids_.lookup_tablet_id_);
            lookup_iter->set_ls_id(ls_id_);
            break;
          }
          case ITER_TREE_VEC_LOOKUP: {
            ObDASIter *result_iter = static_cast<ObDASIter *>(result_);
            if (result_iter->get_type() == ObDASIterType::DAS_ITER_PROFILE) {
              ObDASProfileIter *profile_iter = static_cast<ObDASProfileIter *>(result_iter);
              if (result_iter->get_children_cnt() != 1 || OB_ISNULL(result_iter->get_children())
                  || OB_ISNULL(result_iter->get_children()[0])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected vector profile iter children", K(ret), KPC(result_iter));
              } else {
                profile_iter->set_tablet_id(tablet_ids_.lookup_tablet_id_);
                result_iter = result_iter->get_children()[0];
              }
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(ObDASIterUtils::set_vec_lookup_related_ids(
                attach_ctdef_, attach_rtdef_, tablet_ids_, ls_id_, result_iter))) {
              LOG_WARN("failed to set vector index related ids", K(ret));
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected iter tree type", K(ret), K(tree_type));
          }
        }
      }
      if (FAILEDx(result->get_domain_id_merge_iter(domain_id_merge_iter))) {
        LOG_WARN("fail to get domain id merge iter", K(ret));
      } else if (OB_NOT_NULL(domain_id_merge_iter) && OB_FAIL(domain_id_merge_iter->set_domain_id_merge_related_ids(tablet_ids_, ls_id_))) {
        LOG_WARN("fail to set domain id merge related ids", K(ret));
      } else if (OB_FAIL(result->reuse())) {
        LOG_WARN("failed to reuse das iter tree", K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iter tree type", K(ret), K(tree_type));
  }

  return ret;
}

int ObDASScanOp::try_set_pushdown_topk_related_ids(ObDASIter *result_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr result_iter", K(ret));
  } else if (OB_ISNULL(attach_ctdef_) || OB_ISNULL(attach_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr attach_ctdef or attach_rtdef", K(ret));
  } else {
    const bool need_lookup = (attach_ctdef_->op_type_ == ObDASOpType::DAS_OP_TABLE_LOOKUP) ||
                             (attach_ctdef_->op_type_ == ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP);
    const ObDASBaseCtDef *index_merge_ctdef = need_lookup ? attach_ctdef_->children_[0] : attach_ctdef_;
    ObDASBaseRtDef *index_merge_rtdef = need_lookup ? attach_rtdef_->children_[0] : attach_rtdef_;
    ObDASIndexMergeFTSAndIter *two_phase_index_merge_iter = nullptr;

    if (need_lookup) {
      if (ObDASIterType::DAS_ITER_TWO_PHASE_INDEX_MERGE == result_iter->get_children()[0]->get_type()) {
        two_phase_index_merge_iter = static_cast<ObDASIndexMergeFTSAndIter *>(result_iter->get_children()[0]);
      } else if (ObDASIterType::DAS_ITER_INDEX_MERGE == result_iter->get_type()) {
        two_phase_index_merge_iter = static_cast<ObDASIndexMergeFTSAndIter *>(result_iter);
      }
    }

    if (OB_ISNULL(two_phase_index_merge_iter)) {
      // do nothing
    } else {
      int64_t pushdown_topk_iter_idx = two_phase_index_merge_iter->get_pushdown_topk_iter_idx();
      ObDASIter *pushdown_topk_iter_tree = two_phase_index_merge_iter->get_pushdown_topk_iter_tree();

      const ObDASBaseCtDef *child_ctdef = nullptr;
      ObDASBaseRtDef *child_rtdef = nullptr;
      if (need_lookup) {
        child_ctdef = attach_ctdef_->children_[0]->children_[pushdown_topk_iter_idx];
        child_rtdef = attach_rtdef_->children_[0]->children_[pushdown_topk_iter_idx];
      } else {
        child_ctdef = attach_ctdef_->children_[pushdown_topk_iter_idx];
        child_rtdef = attach_rtdef_->children_[pushdown_topk_iter_idx];
      }

      if (OB_ISNULL(child_ctdef) || OB_ISNULL(child_rtdef) || OB_ISNULL(pushdown_topk_iter_tree)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (ObDASOpType::DAS_OP_SORT == child_ctdef->op_type_ && ObDASIterType::DAS_ITER_SORT != pushdown_topk_iter_tree->get_type()) {
        child_ctdef = child_ctdef->children_[0];
        child_rtdef = child_rtdef->children_[0];
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDASIterUtils::set_index_merge_related_ids(child_ctdef,
                                                                     child_rtdef,
                                                                     tablet_ids_,
                                                                     ls_id_,
                                                                     pushdown_topk_iter_tree))) {
        LOG_WARN("failed to set index merge related ids", K(ret));
      }
    }
  }
  return ret;
}

const ExprFixedArray &ObDASScanOp::get_result_outputs() const
{
  const ExprFixedArray *result_output = nullptr;
  if (attach_ctdef_ != nullptr) {
    OB_ASSERT(ObDASTaskFactory::is_attached(attach_ctdef_->op_type_));
    result_output = &(static_cast<const ObDASAttachCtDef*>(attach_ctdef_)->result_output_);
  } else if (get_lookup_ctdef() != nullptr) {
    result_output = &get_lookup_ctdef()->result_output_;
  } else {
    result_output = &scan_ctdef_->result_output_;
  }
  return *result_output;
}

// get main table lookup ctdef
const ObDASScanCtDef *ObDASScanOp::get_lookup_ctdef() const
{
  const ObDASScanCtDef *lookup_ctdef = nullptr;
  const ObDASBaseCtDef *attach_ctdef = attach_ctdef_;
  if (OB_NOT_NULL(attach_ctdef) && DAS_OP_DOC_ID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    attach_ctdef = attach_ctdef->children_[0];
  } else if (OB_NOT_NULL(attach_ctdef) && DAS_OP_VID_MERGE == attach_ctdef->op_type_) {
    OB_ASSERT(2 == attach_ctdef->children_cnt_ && attach_ctdef->children_ != nullptr);
    attach_ctdef = attach_ctdef->children_[0];
  }
  if (nullptr == attach_ctdef) {
    if (!related_ctdefs_.empty()) {
      OB_ASSERT(related_ctdefs_.count() == 1);
      OB_ASSERT(related_ctdefs_.at(0)->op_type_ == DAS_OP_TABLE_SCAN);
      lookup_ctdef = static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(0));
    }
  } else {
    if (DAS_OP_TABLE_LOOKUP == attach_ctdef->op_type_) {
      const ObDASTableLookupCtDef *table_lookup_ctdef = static_cast<const ObDASTableLookupCtDef*>(attach_ctdef);
      lookup_ctdef = table_lookup_ctdef->get_lookup_scan_ctdef();
    } else if (DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef->op_type_) {
      // func lookup may involve main table lookup
      OB_ASSERT(2 == attach_ctdef->children_cnt_  && attach_ctdef->children_ != nullptr);
      if (DAS_OP_FUNC_LOOKUP == attach_ctdef->children_[1]->op_type_) {
        const ObDASFuncLookupCtDef *func_ctdef = static_cast<const ObDASFuncLookupCtDef*>(attach_ctdef->children_[1]);
        lookup_ctdef = static_cast<const ObDASScanCtDef*>(func_ctdef->get_main_lookup_scan_ctdef());
      } else {
        const ObDASIndexProjLookupCtDef *table_lookup_ctdef = static_cast<const ObDASIndexProjLookupCtDef*>(attach_ctdef);
        lookup_ctdef = table_lookup_ctdef->get_lookup_scan_ctdef();
      }
    }
  }
  return lookup_ctdef;
}

ObDASScanRtDef *ObDASScanOp::get_lookup_rtdef()
{
  ObDASScanRtDef *lookup_rtdef = nullptr;
  ObDASBaseRtDef *attach_rtdef = attach_rtdef_;
  if (OB_NOT_NULL(attach_rtdef) && DAS_OP_DOC_ID_MERGE == attach_rtdef->op_type_) {
    attach_rtdef = attach_rtdef->children_[0];
  } else if (OB_NOT_NULL(attach_rtdef) && DAS_OP_VID_MERGE == attach_rtdef->op_type_) {
    OB_ASSERT(2 == attach_rtdef->children_cnt_ && attach_rtdef->children_ != nullptr);
    attach_rtdef = attach_rtdef->children_[0];
  }
  if (nullptr == attach_rtdef) {
    if (!related_rtdefs_.empty()) {
      OB_ASSERT(related_rtdefs_.count() == 1);
      OB_ASSERT(related_rtdefs_.at(0)->op_type_ == DAS_OP_TABLE_SCAN);
      lookup_rtdef = static_cast<ObDASScanRtDef*>(related_rtdefs_.at(0));
    }
  } else {
    if (DAS_OP_TABLE_LOOKUP == attach_rtdef->op_type_) {
      ObDASTableLookupRtDef *table_lookup_rtdef = static_cast<ObDASTableLookupRtDef*>(attach_rtdef);
      lookup_rtdef = table_lookup_rtdef->get_lookup_scan_rtdef();
    } else if (DAS_OP_INDEX_PROJ_LOOKUP == attach_rtdef->op_type_) {
      OB_ASSERT(2 == attach_rtdef->children_cnt_  && attach_rtdef->children_ != nullptr);
      if (DAS_OP_FUNC_LOOKUP == attach_rtdef->children_[1]->op_type_) {
        const ObDASFuncLookupRtDef *func_rtdef = static_cast<const ObDASFuncLookupRtDef*>(attach_rtdef->children_[1]);
        lookup_rtdef = static_cast<ObDASScanRtDef*>(func_rtdef->get_main_lookup_scan_rtdef());
      } else {
        ObDASIndexProjLookupRtDef *table_lookup_rtdef = static_cast<ObDASIndexProjLookupRtDef*>(attach_rtdef);
        lookup_rtdef = table_lookup_rtdef->get_lookup_scan_rtdef();
      }
    }
  }
  return lookup_rtdef;
}

int ObDASScanOp::reserve_related_buffer(const int64_t related_scan_cnt)
{
  int ret = OB_SUCCESS;
  related_ctdefs_.clear();
  related_rtdefs_.clear();
  related_tablet_ids_.clear();
  OZ(related_ctdefs_.reserve(related_scan_cnt));
  OZ(related_rtdefs_.reserve(related_scan_cnt));
  OZ(related_tablet_ids_.reserve(related_scan_cnt));
  return ret;
}

int ObDASScanOp::set_related_task_info(const ObDASBaseCtDef *lookup_ctdef,
                                       ObDASBaseRtDef *lookup_rtdef,
                                       const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  OZ(related_ctdefs_.push_back(lookup_ctdef));
  OZ(related_rtdefs_.push_back(lookup_rtdef));
  OZ(related_tablet_ids_.push_back(tablet_id));
  return ret;
}

int ObDASScanOp::get_related_tablet_id(const ObDASBaseCtDef *scan_ctdef, common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(related_ctdefs_.count() == related_tablet_ids_.count());
  if (OB_ISNULL(scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan ctdef");
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < related_ctdefs_.count(); ++i) {
      if (scan_ctdef == related_ctdefs_.at(i)) {
        bool is_logical_related = false;
        if (DAS_OP_SCALAR_SCAN_QUERY == scan_ctdef->op_type_) {
          is_logical_related = static_cast<const ObDASScalarScanCtDef *>(scan_ctdef)->is_logical_table();
        } else if (DAS_OP_TABLE_SCAN == scan_ctdef->op_type_) {
          is_logical_related = static_cast<const ObDASScanCtDef *>(scan_ctdef)->is_logical_table();
        }
        if (is_logical_related) {
          const ObDASTabletLoc *anchor = get_tablet_loc();
          ObDASBaseRtDef *rtdef = related_rtdefs_.at(i);
          ObDASTableLoc *table_loc = OB_NOT_NULL(rtdef) ? rtdef->table_loc_ : nullptr;
          ObDASTabletLoc *tablet_loc = nullptr;
          if (OB_NOT_NULL(anchor) && OB_NOT_NULL(table_loc)
              && OB_NOT_NULL(tablet_loc = ObDASUtils::get_related_tablet_loc(
                                 *anchor, table_loc->loc_meta_->ref_table_id_))) {
            tablet_id = tablet_loc->tablet_id_;
          } else {
            tablet_id = related_tablet_ids_.at(i);
          }
        } else {
          tablet_id = related_tablet_ids_.at(i);
        }
        found = true;
      }
    }
    if (OB_UNLIKELY(!found)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get tablet id", KPC(scan_ctdef), K(related_ctdefs_), K(related_tablet_ids_));
    }
  }
  return ret;
}

int ObDASScanOp::get_rowkey_domain_tablet_id(ObDASRelatedTabletID &related_tablet_ids) const
{
  int ret = OB_SUCCESS;
  if (nullptr != attach_ctdef_) {
    const ObDASBaseCtDef *ctdef = nullptr;
    bool is_functional_lookup = ObDASUtils::is_func_lookup(attach_ctdef_);
    if (ObDASOpType::DAS_OP_DOMAIN_ID_MERGE == attach_ctdef_->op_type_ ||
        ObDASOpType::DAS_OP_DOC_ID_MERGE == attach_ctdef_->op_type_ ||
        ObDASOpType::DAS_OP_VID_MERGE == attach_ctdef_->op_type_) {
      ctdef = attach_ctdef_;
    } else if (ObDASOpType::DAS_OP_TABLE_LOOKUP == attach_ctdef_->op_type_ || ObDASOpType::DAS_OP_INDEX_PROJ_LOOKUP == attach_ctdef_->op_type_) {
      if (OB_UNLIKELY(2 != attach_ctdef_->children_cnt_) || OB_ISNULL(attach_ctdef_->children_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected error, children of doc id merge ctdef is invalid", K(ret), KPC(attach_ctdef_));
      } else if (ObDASOpType::DAS_OP_DOMAIN_ID_MERGE == attach_ctdef_->children_[1]->op_type_ ||
                 ObDASOpType::DAS_OP_DOC_ID_MERGE == attach_ctdef_->children_[1]->op_type_ ||
                 ObDASOpType::DAS_OP_VID_MERGE == attach_ctdef_->children_[1]->op_type_) {
        ctdef = attach_ctdef_->children_[1];
      } else if (is_functional_lookup) {
        // func lookup may involve rowkey doc lookup
        const ObDASBaseCtDef *func_ctdef = nullptr;
        if (OB_FAIL(ObDASUtils::find_child_das_ctdef(attach_ctdef_, DAS_OP_FUNC_LOOKUP, func_ctdef))) {
          SQL_DAS_LOG(WARN, "find chld das def failed", K(ret));
        } else if (OB_ISNULL(func_ctdef)) {
          ret = common::OB_ERR_UNEXPECTED;
          SQL_DAS_LOG(WARN, "can not find the target op def", K(ret));
        } else {
          ctdef = static_cast<const ObDASScanCtDef *>(static_cast<const ObDASFuncLookupCtDef *>(func_ctdef)->get_doc_id_lookup_scan_ctdef());
          related_tablet_ids.rowkey_doc_tablet_id_.reset();
          for (int64_t i = 0; OB_NOT_NULL(ctdef) && !related_tablet_ids.rowkey_doc_tablet_id_.is_valid() && i < related_ctdefs_.count(); ++i) {
            if (ctdef == related_ctdefs_.at(i)) {
              related_tablet_ids.rowkey_doc_tablet_id_ = related_tablet_ids_.at(i);
            }
          }
          if (OB_NOT_NULL(ctdef)) {
            if (ObDASOpType::DAS_OP_DOMAIN_ID_MERGE != ctdef->op_type_
                && ObDASOpType::DAS_OP_DOC_ID_MERGE != ctdef->op_type_
                && ObDASOpType::DAS_OP_VID_MERGE    != ctdef->op_type_) {
              ctdef = nullptr;
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(ctdef)) {
      if (OB_UNLIKELY(2 > ctdef->children_cnt_) || OB_ISNULL(ctdef->children_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected error, children of domain id merge ctdef is invalid", K(ret), KPC(ctdef));
      } else if (OB_FAIL(related_tablet_ids.domain_tablet_ids_.prepare_allocate(ctdef->children_cnt_ - 1))) {
        LOG_WARN("failed to prepare allocate array", K(ret), K(ctdef->children_cnt_));
      } else {
        for (int64_t i = 1; OB_SUCC(ret) && i < ctdef->children_cnt_; i++) {
          const ObDASScanCtDef *rowkey_domain_ctdef = static_cast<const ObDASScanCtDef *>(ctdef->children_[i]);
          common::ObTabletID tablet_id;
          tablet_id.reset();
          for (int64_t j = 0; !tablet_id.is_valid() && j < related_ctdefs_.count(); ++j) {
            if (rowkey_domain_ctdef == related_ctdefs_.at(j)) {
              tablet_id = related_tablet_ids_.at(j);
            }
          }
          if (tablet_id.is_valid()) {
            related_tablet_ids.domain_tablet_ids_.at(i - 1) = tablet_id;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to find valid tablet id", K(ret), K(i), KPC(rowkey_domain_ctdef));
          }
        }
      }
    }
  }
  LOG_DEBUG("get rowkey doc tablet id", K(ret), K(related_tablet_ids.domain_tablet_ids_), K(related_ctdefs_), KPC(attach_ctdef_));
  return ret;
}

int ObDASScanOp::get_domain_rowkey_tablet_id(const ObDASBaseCtDef *ctdef,
                                            common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  if (OB_ISNULL(ctdef)) {
    // do nothing
  } else if (ctdef->op_type_ == DAS_OP_IR_AUX_LOOKUP) {
    const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = static_cast<const ObDASIRAuxLookupCtDef*>(ctdef);
    for (int64_t i = 0; OB_SUCC(ret) && !tablet_id.is_valid() && i < related_ctdefs_.count(); ++i) {
      if (aux_lookup_ctdef->get_lookup_scan_ctdef() == related_ctdefs_.at(i)) {
        tablet_id = related_tablet_ids_.at(i);
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_doc_rowkey_tablet_id(ObDASRelatedTabletID &related_tablet_ids) const
{
  int ret = OB_SUCCESS;
  const ObDASBaseCtDef *ctdef = nullptr;
  ObDASBaseRtDef *rtdef = nullptr;
  if (nullptr != attach_ctdef_) {
    if (OB_FAIL(ObDASUtils::find_child_das_def(attach_ctdef_,
                                               attach_rtdef_,
                                               DAS_OP_IR_AUX_LOOKUP,
                                               ctdef,
                                               rtdef))) {
      LOG_WARN("failed to find aux lookup ctdef", K(ret));
    } else if (nullptr != ctdef) {
      const ObDASIRAuxLookupCtDef *aux_lookup_ctdef = static_cast<const ObDASIRAuxLookupCtDef*>(ctdef);
      if (ObDASIterUtils::is_vec_hnsw_scan(ctdef, rtdef)) {
        if (OB_FAIL(get_domain_rowkey_tablet_id(ctdef, related_tablet_ids.vid_rowkey_tablet_id_))) {
          LOG_WARN("fail to get vid rowkey tablet id", K(ret));
        } else if (OB_FAIL(ObDASUtils::find_child_das_ctdef(aux_lookup_ctdef->get_doc_id_scan_ctdef(), // fts as pre-filter
                                                            DAS_OP_IR_AUX_LOOKUP,
                                                            ctdef))) {
          LOG_WARN("failed to find aux lookup ctdef", K(ret));
        } else if (OB_FAIL(get_domain_rowkey_tablet_id(ctdef, related_tablet_ids.doc_rowkey_tablet_id_))) {
          LOG_WARN("fail to get rowkey doc tablet id", K(ret));
        }
      } else if (OB_FAIL(get_domain_rowkey_tablet_id(ctdef, related_tablet_ids.doc_rowkey_tablet_id_))) {
        LOG_WARN("fail to get rowkey doc tablet id", K(ret));
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_table_lookup_tablet_id(common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  if (get_lookup_ctdef() != nullptr) {
    for (int i = 0; !tablet_id.is_valid() && i < related_ctdefs_.count(); ++i) {
      if (get_lookup_ctdef() == related_ctdefs_.at(i)) {
        tablet_id = related_tablet_ids_.at(i);
      }
    }
  } else if (attach_ctdef_ != nullptr && ObDASOpType::DAS_OP_FUNC_LOOKUP == attach_ctdef_->op_type_) {
    // func lookup may involve main table lookup
    const ObDASBaseCtDef *main_ctdef = static_cast<const ObDASFuncLookupCtDef*>(attach_ctdef_)->get_main_lookup_scan_ctdef();
    if (main_ctdef != nullptr) {
      for (int i = 0; !tablet_id.is_valid() && i < related_ctdefs_.count(); ++i) {
        if (main_ctdef == related_ctdefs_.at(i)) {
          tablet_id = related_tablet_ids_.at(i);
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_vec_ir_tablet_ids(ObDASRelatedTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  bool is_async_ivf = ObDASIterUtils::is_async_ivf_scan(attach_ctdef_, attach_rtdef_);
  bool is_ivf = ObDASIterUtils::is_vec_ivf_scan(attach_ctdef_, attach_rtdef_);
  bool is_spiv = ObDASIterUtils::is_vec_spiv_scan(attach_ctdef_, attach_rtdef_);
  if (is_async_ivf) {
    if (OB_FAIL(get_async_ivf_tablet_ids(related_tablet_ids))) {
      LOG_WARN("failed to get async ivf related tablet ids", K(ret));
    }
  } else if (is_spiv) {
    if (OB_FAIL(get_spiv_ir_tablet_ids(related_tablet_ids.inv_idx_tablet_id_,
                                       related_tablet_ids.dim_docid_value_tablet_id_,
                                       related_tablet_ids.rowkey_doc_tablet_id_))) {
      LOG_WARN("failed to get spiv ir tablet ids", K(ret));
    }
  } else if (is_ivf) {
    if (OB_FAIL(get_ivf_ir_tablet_ids(related_tablet_ids.inv_idx_tablet_id_,
                                      related_tablet_ids.centroid_tablet_id_,
                                      related_tablet_ids.cid_vec_tablet_id_,
                                      related_tablet_ids.rowkey_cid_tablet_id_,
                                      related_tablet_ids.special_aux_tablet_id_,
                                      related_tablet_ids.lookup_tablet_id_))) {
      LOG_WARN("failed to get ivf ir tablet ids", K(ret));
    }
  } else {
    if (OB_FAIL(get_hnsw_ir_tablet_ids(related_tablet_ids.inv_idx_tablet_id_,
                                       related_tablet_ids.delta_buf_tablet_id_,
                                       related_tablet_ids.index_id_tablet_id_,
                                       related_tablet_ids.snapshot_tablet_id_,
                                       related_tablet_ids.embedded_tablet_id_,
                                       related_tablet_ids.lookup_tablet_id_,
                                       related_tablet_ids.rowkey_vid_tablet_id_,
                                       related_tablet_ids.vid_rowkey_tablet_id_))) {
      LOG_WARN("failed to get hnsw ir tablet ids", K(ret));
    }
  }

  return ret;
}

int ObDASScanOp::get_async_ivf_tablet_ids(ObDASRelatedTabletID &related_tablet_ids) const
{
  int ret = OB_SUCCESS;
  const ObDASBaseCtDef *base_ctdef = nullptr;
  const ObDASVecAuxScanCtDef *vec_aux_ctdef = nullptr;
  related_tablet_ids.async_ivf_index_tablet_id_.reset();
  related_tablet_ids.async_delete_tablet_id_.reset();
  if (OB_FAIL(ObDASUtils::find_child_das_ctdef(
          attach_ctdef_, DAS_OP_VEC_SCAN, base_ctdef))) {
    LOG_WARN("failed to find async ivf vector scan ctdef", K(ret));
  } else if (OB_ISNULL(base_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async ivf vector scan ctdef is null", K(ret));
  } else if (FALSE_IT(vec_aux_ctdef = static_cast<const ObDASVecAuxScanCtDef *>(base_ctdef))) {
  } else if (OB_UNLIKELY(!vec_aux_ctdef->is_async_index_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector scan ctdef is not async index", K(ret), KPC(vec_aux_ctdef));
  } else if (OB_ISNULL(vec_aux_ctdef->get_async_ivf_index_scan_ctdef())
             || OB_ISNULL(vec_aux_ctdef->get_async_delete_scan_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async ivf related scan ctdef is null", K(ret), KPC(vec_aux_ctdef));
  } else if (OB_FAIL(get_related_tablet_id(
                 vec_aux_ctdef->get_async_ivf_index_scan_ctdef(),
                 related_tablet_ids.async_ivf_index_tablet_id_))) {
    LOG_WARN("failed to get async ivf index tablet id", K(ret));
  } else if (OB_FAIL(get_related_tablet_id(
                 vec_aux_ctdef->get_async_delete_scan_ctdef(),
                 related_tablet_ids.async_delete_tablet_id_))) {
    LOG_WARN("failed to get async delete tablet id", K(ret));
  } else if (OB_UNLIKELY(!related_tablet_ids.async_ivf_index_tablet_id_.is_valid()
                         || !related_tablet_ids.async_delete_tablet_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid async ivf related tablet id", K(ret), K_(related_tablet_ids));
  }
  return ret;
}

int ObDASScanOp::get_ivf_ir_tablet_ids(
    common::ObTabletID &vec_row_tid,
    common::ObTabletID &centroid_tid_,
    common::ObTabletID &cid_vec_tid,
    common::ObTabletID &rowkey_cid_tid,
    common::ObTabletID &special_aux_tid,
    common::ObTabletID &com_aux_vec_tid)
{
  int ret = OB_SUCCESS;
  centroid_tid_.reset();
  cid_vec_tid.reset();
  rowkey_cid_tid.reset();
  special_aux_tid.reset();
  if (OB_UNLIKELY(related_ctdefs_.count() != related_tablet_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected related scan array not match", K(ret), K_(related_ctdefs), K_(related_tablet_ids));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
    if (OB_ISNULL(related_ctdefs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ctdef", K(ret), K(i));
    } else if (DAS_OP_TABLE_SCAN != related_ctdefs_.at(i)->op_type_) {
      // do nothing
    } else {
      const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef *>(related_ctdefs_.at(i));
      switch (ctdef->ir_scan_type_) {
        case ObTSCIRScanType::OB_NOT_A_SPEC_SCAN: {
          break;
        }
        case ObTSCIRScanType::OB_IR_INV_IDX_SCAN: {
          vec_row_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN: {
          centroid_tid_ = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN: {
          cid_vec_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN: {
          rowkey_cid_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN: {
          special_aux_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_COM_AUX_SCAN: {
          com_aux_vec_tid = related_tablet_ids_.at(i);
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_hnsw_ir_tablet_ids(
    common::ObTabletID &vec_row_tid,
    common::ObTabletID &delta_buf_tid,
    common::ObTabletID &index_id_tid,
    common::ObTabletID &snapshot_tid,
    common::ObTabletID &embedded_tid,
    common::ObTabletID &com_aux_vec_tid,
    common::ObTabletID &rowkey_vid_tid,
    common::ObTabletID &vid_rowkey_tid)
{
  int ret = OB_SUCCESS;
  vec_row_tid.reset();
  delta_buf_tid.reset();
  index_id_tid.reset();
  snapshot_tid.reset();
  embedded_tid.reset();
  com_aux_vec_tid.reset();
  rowkey_vid_tid.reset();
  if (OB_UNLIKELY(related_ctdefs_.count() != related_tablet_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected related scan array not match", K(ret), K_(related_ctdefs), K_(related_tablet_ids));
  }
  for (int64_t i= 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
    if (OB_ISNULL(related_ctdefs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ctdef", K(ret), K(i));
    } else if (DAS_OP_TABLE_SCAN != related_ctdefs_.at(i)->op_type_) {
      // do nothing
    } else {
      const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef *>(related_ctdefs_.at(i));
      switch (ctdef->ir_scan_type_) {
        case ObTSCIRScanType::OB_NOT_A_SPEC_SCAN: {
          break;
        }
        case ObTSCIRScanType::OB_IR_INV_IDX_SCAN: 
        case ObTSCIRScanType::OB_VEC_FILTER_SCAN: {
          vec_row_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN: {
          delta_buf_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_IDX_ID_SCAN: {
          index_id_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN: {
          snapshot_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_EMBEDDED_SCAN: {
          embedded_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_COM_AUX_SCAN: {
          com_aux_vec_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN: {
          rowkey_vid_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_VID_ROWKEY_SCAN: {
          vid_rowkey_tid = related_tablet_ids_.at(i);
          break;
        }
        default: {
          break;
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_spiv_ir_tablet_ids(
    common::ObTabletID &vec_row_tid,
    common::ObTabletID &dim_docid_value_tid,
    common::ObTabletID &rowkey_doc_tid)
{
  int ret = OB_SUCCESS;
  vec_row_tid.reset();
  dim_docid_value_tid.reset();
  rowkey_doc_tid.reset();

  if (OB_UNLIKELY(related_ctdefs_.count() != related_tablet_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected related scan array not match", K(ret), K_(related_ctdefs), K_(related_tablet_ids));
  }
  for (int64_t i= 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
    if (OB_ISNULL(related_ctdefs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ctdef", K(ret), K(i));
    } else if (DAS_OP_TABLE_SCAN != related_ctdefs_.at(i)->op_type_) {
      // do nothing
    } else {
      const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef *>(related_ctdefs_.at(i));
      switch (ctdef->ir_scan_type_) {
        case ObTSCIRScanType::OB_NOT_A_SPEC_SCAN: {
          break;
        }
        case ObTSCIRScanType::OB_IR_INV_IDX_SCAN: 
        case ObTSCIRScanType::OB_VEC_FILTER_SCAN: {
          vec_row_tid = related_tablet_ids_.at(i);
          break;
        }
        case ObTSCIRScanType::OB_VEC_SPIV_INDEX_SCAN: {
          dim_docid_value_tid = related_tablet_ids_.at(i);
          break;
        }  
        case ObTSCIRScanType::OB_VEC_ROWKEY_VID_SCAN: {
          rowkey_doc_tid = related_tablet_ids_.at(i);
          break;
        }      
        default: {
          break;
        }
      }
    }
  }
  return ret;
}

int ObDASScanOp::get_fts_tablet_ids(common::ObIArray<ObDASFTSTabletID> &fts_tablet_ids,
                                    ObDASBaseRtDef *rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr rtdef", K(ret));
  } else if (DAS_OP_IR_SCAN == rtdef->op_type_) {
    ObDASIRScanRtDef *ir_rtdef = static_cast<ObDASIRScanRtDef *>(rtdef);
    ObDASFTSTabletID fts_tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < related_rtdefs_.count(); ++i) {
      if (ir_rtdef->get_inv_idx_agg_rtdef() == related_rtdefs_.at(i) ||
          ir_rtdef->get_inv_idx_scan_rtdef() == related_rtdefs_.at(i)) {
        fts_tablet_id.inv_idx_tablet_id_ = related_tablet_ids_.at(i);
      } else if (ir_rtdef->get_doc_agg_rtdef() == related_rtdefs_.at(i)) {
        fts_tablet_id.domain_id_idx_tablet_id_ = related_tablet_ids_.at(i);
      } else if (ir_rtdef->get_fwd_idx_agg_rtdef() == related_rtdefs_.at(i)) {
        fts_tablet_id.fwd_idx_tablet_id_ = related_tablet_ids_.at(i);
      } else if (OB_NOT_NULL(ir_rtdef->get_stats_primary_scan_rtdef())
                 && ir_rtdef->get_stats_primary_scan_rtdef() == related_rtdefs_.at(i)) {
        // [LT plain MATCH] doc_agg stripped; domain slot carries logic-main partition
        // tablet for stats_primary (ref_table_id = logic data table id).
        fts_tablet_id.domain_id_idx_tablet_id_ = related_tablet_ids_.at(i);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fts_tablet_ids.push_back(fts_tablet_id))) {
        LOG_WARN("failed to push back fts tablet id", K(ret));
      } else {
        ir_rtdef->fts_idx_ = fts_tablet_ids.count() - 1;
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rtdef->children_cnt_; ++i) {
      if (OB_FAIL(SMART_CALL(get_fts_tablet_ids(fts_tablet_ids, rtdef->children_[i])))) {
        LOG_WARN("failed to get fts tablet ids", K(ret));
      }
    }
  }

  return ret;
}

int ObDASScanOp::get_index_merge_tablet_ids(common::ObIArray<common::ObTabletID> &index_merge_tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t N = related_tablet_ids_.count();
  int64_t index_merge_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    if (OB_ISNULL(related_ctdefs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ctdef", K(ret), K(i));
    } else if (DAS_OP_TABLE_SCAN != related_ctdefs_.at(i)->op_type_) {
      // do nothing
    } else {
      const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(i));
      if (ctdef->is_index_merge_) {
        index_merge_cnt += 1;
      }
    }
  }
  if (OB_SUCC(ret) && index_merge_cnt > 0) {
    if (OB_FAIL(index_merge_tablet_ids.prepare_allocate(index_merge_cnt))) {
      LOG_WARN("failed to reserve index merge tablet ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_ISNULL(related_ctdefs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ctdef", K(ret), K(i));
      } else if (DAS_OP_TABLE_SCAN != related_ctdefs_.at(i)->op_type_) {
        // do nothing
      } else {
        const ObDASScanCtDef *ctdef = static_cast<const ObDASScanCtDef*>(related_ctdefs_.at(i));
        if (ctdef->is_index_merge_) {
          if (OB_UNLIKELY(ctdef->index_merge_idx_ < 0 || ctdef->index_merge_idx_ >= index_merge_cnt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected index merge idx", K(ret), K(ctdef->index_merge_idx_), K(index_merge_cnt));
          } else {
            index_merge_tablet_ids.at(ctdef->index_merge_idx_) = related_tablet_ids_.at(i);
          }
        }
      }
    }
  }
  return ret;
}

// lt_ctx_ carries QC-runtime captured @namespace_id / @ltable_id to PX workers.
// CG no longer bakes session ns/lt into ctdef.lt_ctx_ (plan-cache safety); QC's init_scan_param
// captures session vars into lt_ctx_ and this serialize line ships them to PX workers
// whose shadow session lacks @namespace_id / @ltable_id.
OB_SERIALIZE_MEMBER((ObDASScanOp, ObIDASTaskOp),
                    scan_param_.key_ranges_,
                    scan_ctdef_,
                    scan_rtdef_,
                    scan_param_.ss_key_ranges_,
                    ir_param_);

OB_SERIALIZE_MEMBER(ObDASObsoletedObj, flag_);

ObDASGroupScanOp::ObDASGroupScanOp(ObIAllocator &op_alloc)
  : ObDASScanOp(op_alloc),
    iter_(),
    cur_group_idx_(0),
    group_size_(0)
{

}

void ObDASGroupScanOp::init_group_range(int64_t cur_group_idx, int64_t group_size)
{
  cur_group_idx_ = cur_group_idx;
  group_size_ = group_size;
}

ObDASGroupScanOp::~ObDASGroupScanOp()
{
}

OB_SERIALIZE_MEMBER((ObDASGroupScanOp, ObDASScanOp), iter_, cur_group_idx_, group_size_);

ObDASScanResult::ObDASScanResult()
  : ObIDASTaskResult(),
    ObNewRowIterator(),
    datum_store_("DASScanResult"),
    result_iter_(),
    output_exprs_(nullptr),
    eval_ctx_(nullptr),
    extra_result_(nullptr),
    need_check_output_datum_(false),
    enable_rich_format_(false),
    io_read_bytes_(0),
    ssstore_read_bytes_(0),
    base_read_row_cnt_(0),
    delta_read_row_cnt_(0),
    das_execute_remote_info_()
{
}

ObDASScanResult::~ObDASScanResult()
{
}

int ObDASScanResult::get_next_row(ObNewRow *&row)
{
  UNUSED(row);
  return OB_NOT_IMPLEMENT;
}

int ObDASScanResult::get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result_iter_.get_next_row<false>(*eval_ctx_, *output_exprs_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from result iterator failed", K(ret));
    } else if (extra_result_ != nullptr) {
      if (OB_FAIL(extra_result_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from extra result failed", K(ret));
        }
      }
    }
  } else {
    LOG_DEBUG("get next row from result iter", K(ret),
              "output", ROWEXPR2STR(*eval_ctx_, *output_exprs_));
  }
  return ret;
}

int ObDASScanResult::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (enable_rich_format_) {
    ret = vec_result_iter_.get_next_batch(*output_exprs_, *eval_ctx_, capacity, count);
  } else if (OB_UNLIKELY(need_check_output_datum_)) {
    ret = result_iter_.get_next_batch<true>(*output_exprs_, *eval_ctx_,
                                                       capacity, count);
  } else {
    ret = result_iter_.get_next_batch<false>(*output_exprs_, *eval_ctx_,
                                                       capacity, count);
  }
  if (OB_FAIL(ret)) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from result iterator failed", K(ret));
    } else if (extra_result_ != nullptr) {
      if (OB_FAIL(extra_result_->get_next_rows(count, capacity))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next batch from extra result failed", K(ret));
        }
      }
    }
  } else {
    const ObBitVector *skip = NULL;
    PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_exprs_, count, skip);
  }
  LOG_DEBUG("das result next rows", K(enable_rich_format_), K(count), K(capacity), K(ret));
  return ret;
}

void ObDASScanResult::reset()
{
  result_iter_.reset();
  datum_store_.reset();
  vec_result_iter_.reset();
  vec_row_store_.reset();
  output_exprs_ = nullptr;
  eval_ctx_ = nullptr;
  enable_rich_format_ = false;
}

int ObDASScanResult::init_result_iter(const ExprFixedArray *output_exprs, ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  output_exprs_ = output_exprs;
  eval_ctx_ = eval_ctx;
  LOG_DEBUG("init result iter", K(enable_rich_format_), K(vec_row_store_), K(datum_store_));
  if (enable_rich_format_) {
    if (OB_FAIL(vec_row_store_.begin(vec_result_iter_))) {
      LOG_WARN("begin vec row result iterator failed", K(ret));
    }
  } else {
    if (OB_FAIL(datum_store_.begin(result_iter_))) {
      LOG_WARN("begin datum result iterator failed", K(ret));
    }
  }

  return ret;
}

int ObDASScanResult::init(const ObIDASTaskOp &op, common::ObIAllocator &alloc)
{
  UNUSED(alloc);
  int ret = OB_SUCCESS;
  const ObDASScanOp &scan_op = static_cast<const ObDASScanOp&>(op);
  uint64_t tenant_id = MTL_ID();
  need_check_output_datum_ = scan_op.need_check_output_datum();
  enable_rich_format_ = scan_op.enable_rich_format();
  LOG_DEBUG("das scan result init", K(enable_rich_format_));
  //if (!enable_rich_format_) {
  if (OB_FAIL(datum_store_.init(UINT64_MAX,
                               tenant_id,
                               ObCtxIds::DEFAULT_CTX_ID,
                               "DASScanResult",
                               false/*enable_dump*/))) {
    LOG_WARN("init datum store failed", K(ret));
  } else {
    ObMemAttr mem_attr(tenant_id, "DASScanResult", ObCtxIds::DEFAULT_CTX_ID);
    const ExprFixedArray &result_output = scan_op.get_result_outputs();
    int64_t max_batch_size = static_cast<const ObDASScanCtDef *>(scan_op.get_ctdef())
                             ->pd_expr_spec_.max_batch_size_;
    if (OB_FAIL(vec_row_store_.init(result_output,
                                    max_batch_size,
                                    mem_attr,
                                    UINT64_MAX, /*mem_limit*/
                                    false, /*enable_dump*/
                                    0, /*row_extra_size*/
                                    NONE_COMPRESSOR,
                                    false /*reorder_fixed_expr*/))) {
      LOG_WARN("init vec row store failed", K(ret));
    }
  }

  return ret;
}

int ObDASScanResult::reuse()
{
  int ret = OB_SUCCESS;
  result_iter_.reset();
  datum_store_.reset();
  vec_result_iter_.reset();
  vec_row_store_.reset();
  return ret;
}

int ObDASScanResult::link_extra_result(ObDASExtraData &extra_result, ObIDASTaskOp *task_op)
{
  extra_result.set_output_info(output_exprs_, eval_ctx_);
  extra_result.set_need_check_output_datum(need_check_output_datum_);
  extra_result_ = &extra_result;
  ObTSCMonitorInfo *tsc_monitor_info =
    static_cast<ObDASScanRtDef *>(task_op->get_rtdef())->tsc_monitor_info_;
  extra_result.set_tsc_monitor_info(tsc_monitor_info);
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER((ObDASScanResult, ObIDASTaskResult),
                    datum_store_,
                    enable_rich_format_,
                    vec_row_store_,
                    io_read_bytes_,
                    ssstore_read_bytes_,
                    base_read_row_cnt_,  // FARM COMPAT WHITELIST
                    delta_read_row_cnt_, // FARM COMPAT WHITELIST
                    das_execute_remote_info_);

ObLocalIndexLookupOp::~ObLocalIndexLookupOp()
{

}

int ObLocalIndexLookupOp::init(const ObDASScanCtDef *lookup_ctdef,
                               ObDASScanRtDef *lookup_rtdef,
                               const ObDASScanCtDef *index_ctdef,
                               ObDASScanRtDef *index_rtdef,
                               ObTxDesc *tx_desc,
                               ObTxReadSnapshot *snapshot)
{
  int ret = OB_SUCCESS;
  lookup_ctdef_ = lookup_ctdef;
  lookup_rtdef_ = lookup_rtdef;
  index_ctdef_ = index_ctdef;
  index_rtdef_ = index_rtdef;
  tx_desc_ = tx_desc;
  snapshot_ = snapshot;
  state_ = INDEX_SCAN;
  if (OB_ISNULL(lookup_memctx_)) {
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), ObModIds::OB_SQL_TABLE_LOOKUP, ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(lookup_memctx_, param))) {
      LOG_WARN("create lookup mem context entity failed", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_row(ObNewRow *&row)
{
  UNUSED(row);
  return OB_NOT_IMPLEMENT;
}

void ObLocalIndexLookupOp::print_trans_info_and_key_range_()
{
  int ret = OB_ERR_DEFENSIVE_CHECK;
  if (trans_info_array_.count() == scan_param_.key_ranges_.count()) {
    for (int64_t i = 0; i < trans_info_array_.count(); i++) {
      LOG_ERROR("dump TableLookup DAS Task trans_info and key_ranges", K(i),
                KPC(trans_info_array_.at(i)), K(scan_param_.key_ranges_.at(i)));
    }
  } else {
    for (int64_t i = 0; i < scan_param_.key_ranges_.count(); i++) {
      LOG_ERROR("dump TableLookup DAS Task key_ranges",
                K(i), K(scan_param_.key_ranges_.at(i)));
    }
  }
}

int ObLocalIndexLookupOp::get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIndexLookupOpImpl::get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from index table lookup failed", K(ret));
      if (OB_ERR_DEFENSIVE_CHECK == ret) {
        (void)print_trans_info_and_key_range_();
      }
    } else {
      LOG_DEBUG("get next row from index table lookup", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIndexLookupOpImpl::get_next_rows(count, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next rows from index table lookup failed", K(ret));
      if (OB_ERR_DEFENSIVE_CHECK == ret) {
        (void)print_trans_info_and_key_range_();
      }
    } else {
      LOG_DEBUG("get next rows from index table lookup ", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_row_from_index_table()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowkey_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from index scan failed", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_rows_from_index_table(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rowkey_iter_->get_next_rows(count, capacity))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from index scan failed", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::process_data_table_rowkey()
{
  int ret = OB_SUCCESS;
  int64_t rowkey_cnt = index_ctdef_->result_output_.count();
  if (nullptr != index_ctdef_->group_id_expr_) {
    rowkey_cnt -= 1;
  }
  if (nullptr != index_ctdef_->trans_info_expr_) {
    rowkey_cnt -= 1;
  }
  int64_t group_idx = 0;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;
  common::ObArenaAllocator& lookup_alloc = lookup_memctx_->get_arena_allocator();
  ObNewRange lookup_range;
  if (OB_ISNULL(buf = lookup_alloc.alloc(sizeof(ObObj) * rowkey_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer failed", K(ret), K(rowkey_cnt));
  } else {
    obj_ptr = new (buf) ObObj[rowkey_cnt];
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_ctdef_->result_output_.count(); ++i) {
    ObObj tmp_obj;
    ObExpr *expr = index_ctdef_->result_output_.at(i);
    if (T_PSEUDO_GROUP_ID == expr->type_) {
      group_idx = ObNewRange::get_group_idx(expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_).get_int());
    } else if (T_PSEUDO_ROW_TRANS_INFO_COLUMN == expr->type_) {
      // do nothing
    } else {
      ObDatum &col_datum = expr->locate_expr_datum(*lookup_rtdef_->eval_ctx_);
      if (OB_FAIL(col_datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (OB_FAIL(ob_write_obj(lookup_alloc, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    }
  }

  if (OB_SUCC(ret) && nullptr != index_ctdef_->trans_info_expr_) {
    void *buf = nullptr;
    ObDatum *datum_ptr = nullptr;
    if (OB_FAIL(build_trans_datum(index_ctdef_->trans_info_expr_,
                                  lookup_rtdef_->eval_ctx_,
                                  lookup_memctx_->get_arena_allocator(),
                                  datum_ptr))) {

    } else if (OB_ISNULL(datum_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(trans_info_array_.push_back(datum_ptr))) {
      LOG_WARN("fail to push back trans info array", K(ret), KPC(datum_ptr));
    }
  }

  if (OB_SUCC(ret)) {
    ObRowkey table_rowkey(obj_ptr, rowkey_cnt);
    uint64_t ref_table_id = lookup_ctdef_->ref_table_id_;
    if (OB_FAIL(lookup_range.build_range(ref_table_id, table_rowkey))) {
      LOG_WARN("build lookup range failed", K(ret), K(ref_table_id), K(table_rowkey));
    } else if (FALSE_IT(lookup_range.group_idx_ = group_idx)) {
    } else if (OB_FAIL(scan_param_.key_ranges_.push_back(lookup_range))) {
      LOG_WARN("store lookup key range failed", K(ret), K(scan_param_));
    }
    LOG_DEBUG("build data table range", K(ret), K(table_rowkey), K(lookup_range), K(scan_param_.key_ranges_.count()));
  }
  return ret;
}

int ObLocalIndexLookupOp::process_data_table_rowkeys(const int64_t size, const ObBitVector *skip)
{
  int ret = OB_SUCCESS;
  UNUSED(skip);
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*index_rtdef_->eval_ctx_);
  batch_info_guard.set_batch_size(size);
  for (auto i = 0; OB_SUCC(ret) && i < size; i++) {
    batch_info_guard.set_batch_idx(i);
    if (OB_FAIL(process_data_table_rowkey())) {
      LOG_WARN("Failed to process_data_table_rowkey", K(ret), K(i));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::do_index_lookup()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  ObNewRowIterator *&storage_iter = get_lookup_storage_iter();
  if (scan_param_.key_ranges_.empty()) {
    //do nothing
  } else if (storage_iter == nullptr) {
    //first index lookup, init scan param and do table scan
    if (OB_FAIL(init_scan_param())) {
      LOG_WARN("init scan param failed", K(ret));
    } else if (OB_FAIL(tsc_service.table_scan(scan_param_,
                       storage_iter))) {
      if (OB_SNAPSHOT_DISCARDED == ret && scan_param_.fb_snapshot_.is_valid()) {
        ret = OB_INVALID_QUERY_TIMESTAMP;
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("fail to scan table", K(scan_param_), K(ret));
      }
    }
  } else {
    const ObTabletID &storage_tablet_id = scan_param_.tablet_id_;
    scan_param_.need_switch_param_ = (storage_tablet_id.is_valid() && storage_tablet_id != tablet_id_ ? true : false);
    scan_param_.tablet_id_ = tablet_id_;
    scan_param_.ls_id_ = ls_id_;
    if (OB_FAIL(reuse_iter())) {
      LOG_WARN("failed to reuse iter", K(ret));
    } else if (OB_FAIL(tsc_service.table_rescan(scan_param_, storage_iter))) {
      LOG_WARN("table_rescan scan iter failed", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_row_from_data_table()
{
  int ret = OB_SUCCESS;
  do_clear_evaluated_flag();
  if (scan_param_.key_ranges_.empty()) {
    ret= OB_ITER_END;
    state_ = FINISHED;
  } else if (OB_FAIL(lookup_iter_->get_next_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from data table failed", K(ret));
    } else {
      LOG_DEBUG("get next row from data table ", K(ret));
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::get_next_rows_from_data_table(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  lookup_rtdef_->p_pd_expr_op_->clear_evaluated_flag();
  if (scan_param_.key_ranges_.empty()) {
    ret = OB_ITER_END;
    state_ = FINISHED;
  } else {
    ret = lookup_iter_->get_next_rows(count, capacity);
    if (OB_ITER_END == ret && count > 0) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLocalIndexLookupOp::check_lookup_row_cnt()
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_defensive_check()
      && !is_group_scan_
      && lookup_ctdef_->pd_expr_spec_.pushdown_filters_.empty()
      && lookup_ctdef_->logical_residual_filters_.empty()) {
    if (OB_UNLIKELY(lookup_rowkey_cnt_ != lookup_row_cnt_)) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      ObString func_name = ObString::make_string("check_local_lookup_row_cnt");
      LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
      LOG_ERROR("Fatal Error!!! Catch a defensive error!",
                      K(ret), K_(lookup_rowkey_cnt), K_(lookup_row_cnt),
                      "index_table_id", index_ctdef_->ref_table_id_ ,
                      "data_table_tablet_id", tablet_id_ ,
                      KPC_(snapshot),
                      KPC_(tx_desc));
      concurrency_control::ObDataValidationService::set_delay_resource_recycle(ls_id_);
      (void)print_trans_info_and_key_range_();
    }
  }

  int simulate_error = EVENT_CALL(EventTable::EN_DAS_SIMULATE_DUMP_WRITE_BUFFER);
  if (0 != simulate_error) {
    for (int64_t i = 0; i < trans_info_array_.count(); i++) {
      LOG_INFO("dump TableLookup DAS Task trans info", K(i), KPC(trans_info_array_.at(i)));
    }
  }

  return ret;
}

OB_INLINE ObITabletScan &ObLocalIndexLookupOp::get_tsc_service()
{
  return is_virtual_table(lookup_ctdef_->ref_table_id_) ?
      *GCTX.vt_par_ser_ : *(MTL(ObAccessService *));
}

OB_INLINE int ObLocalIndexLookupOp::init_scan_param()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  scan_param_.tenant_id_ = tenant_id;
  scan_param_.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
  scan_param_.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
  scan_param_.tx_lock_timeout_ = lookup_rtdef_->tx_lock_timeout_;
  scan_param_.index_id_ = lookup_ctdef_->ref_table_id_;
  scan_param_.is_get_ = lookup_ctdef_->is_get_;
  scan_param_.is_for_foreign_check_ = lookup_rtdef_->is_for_foreign_check_;
  scan_param_.timeout_ = lookup_rtdef_->timeout_ts_;
  scan_param_.scan_flag_ = lookup_rtdef_->scan_flag_;
  scan_param_.reserved_cell_count_ = lookup_ctdef_->access_column_ids_.count();
  scan_param_.allocator_ = &lookup_rtdef_->stmt_allocator_;
  scan_param_.sql_mode_ = lookup_rtdef_->sql_mode_;
  scan_param_.scan_allocator_ = &lookup_memctx_->get_arena_allocator();
  scan_param_.frozen_version_ = lookup_rtdef_->frozen_version_;
  scan_param_.force_refresh_lc_ = lookup_rtdef_->force_refresh_lc_;
  scan_param_.output_exprs_ = &(lookup_ctdef_->pd_expr_spec_.access_exprs_);
  scan_param_.aggregate_exprs_ = &(lookup_ctdef_->pd_expr_spec_.pd_storage_aggregate_output_);
  scan_param_.table_param_ = &(lookup_ctdef_->table_param_);
  scan_param_.op_ = lookup_rtdef_->p_pd_expr_op_;
  scan_param_.row2exprs_projector_ = lookup_rtdef_->p_row2exprs_projector_;
  scan_param_.schema_version_ = lookup_ctdef_->schema_version_;
  scan_param_.tenant_schema_version_ = lookup_rtdef_->tenant_schema_version_;
  scan_param_.limit_param_ = lookup_rtdef_->limit_param_;
  scan_param_.need_scn_ = lookup_rtdef_->need_scn_;
  scan_param_.pd_storage_flag_ = lookup_ctdef_->pd_expr_spec_.pd_storage_flag_.pd_flag_;
  scan_param_.table_scan_opt_ = lookup_ctdef_->table_scan_opt_;
  scan_param_.fb_snapshot_ = lookup_rtdef_->fb_snapshot_;
  scan_param_.fb_read_tx_uncommitted_ = lookup_rtdef_->fb_read_tx_uncommitted_;
  scan_param_.ls_id_ = ls_id_;
  scan_param_.tablet_id_ = tablet_id_;
  scan_param_.main_table_scan_stat_.tsc_monitor_info_ = lookup_rtdef_->tsc_monitor_info_;
  if (lookup_rtdef_->is_for_foreign_check_) {
    scan_param_.trans_desc_ = tx_desc_;
  }
  // lookup to main table should invoke multi get
  scan_param_.is_get_ = true;
  scan_param_.is_for_foreign_check_ = lookup_rtdef_->is_for_foreign_check_;
  if (lookup_rtdef_->is_for_foreign_check_) {
    scan_param_.trans_desc_ = tx_desc_;
  }
  if (OB_NOT_NULL(snapshot_)) {
    if (OB_FAIL(scan_param_.snapshot_.assign(*snapshot_))) {
      LOG_WARN("assign snapshot fail", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("snapshot is null", K(ret), KPC(this));
  }
  // set tx_id for read-latest,
  // TODO: add if(query_flag.is_read_latest) ..
  if (OB_NOT_NULL(tx_desc_)) {
    scan_param_.tx_id_ = tx_desc_->get_tx_id();
    scan_param_.tx_seq_base_ = tx_desc_->get_seq_base();
  } else {
    scan_param_.tx_id_.reset();
  }
  if (!lookup_ctdef_->pd_expr_spec_.pushdown_filters_.empty()) {
    scan_param_.op_filters_ = &lookup_ctdef_->pd_expr_spec_.pushdown_filters_;
  }
  scan_param_.pd_storage_filters_ = lookup_rtdef_->p_pd_expr_op_->pd_storage_filters_;
  if (FAILEDx(scan_param_.column_ids_.assign(lookup_ctdef_->access_column_ids_))) {
    LOG_WARN("init column ids failed", K(ret));
  }
  LOG_DEBUG("init local index lookup scan_param", K(scan_param_));
  return ret;
}
int ObLocalIndexLookupOp::reuse_iter()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  if (OB_FAIL(tsc_service.reuse_scan_iter(scan_param_.need_switch_param_, get_lookup_storage_iter()))) {
    LOG_WARN("reuse scan iterator failed", K(ret));
  }
  return ret;
}
int ObLocalIndexLookupOp::reset_lookup_state()
{
  int ret = OB_SUCCESS;
  state_ = INDEX_SCAN;
  index_end_ = false;
  lookup_row_cnt_ = 0;
  lookup_rowkey_cnt_ = 0;
  trans_info_array_.reuse();
  lookup_rtdef_->stmt_allocator_.set_alloc(index_rtdef_->stmt_allocator_.get_alloc());
  // Keep lookup_rtdef_->stmt_allocator_.alloc_ consistent with index_rtdef_->stmt_allocator_.alloc_
  // to avoid memory expansion
  if (lookup_iter_ != nullptr) {
    scan_param_.key_ranges_.reuse();
    scan_param_.ss_key_ranges_.reuse();
  }
  if (OB_SUCC(ret) && lookup_memctx_ != nullptr) {
    lookup_memctx_->reset_remain_one_page();
  }

  return ret;
}

int ObLocalIndexLookupOp::revert_iter()
{
  int ret = OB_SUCCESS;
  ObITabletScan &tsc_service = get_tsc_service();
  if (OB_FAIL(tsc_service.revert_scan_iter(get_lookup_storage_iter()))) {
    LOG_WARN("revert scan iterator failed", K(ret));
  }
  //release the memory hold by local index lookup op

  rowkey_iter_ = NULL;
  lookup_iter_ = NULL;
  scan_param_.destroy_schema_guard();
  scan_param_.~ObTableScanParam();
  trans_info_array_.destroy();
  if (lookup_memctx_ != nullptr) {
    lookup_memctx_->reset_remain_one_page();
    DESTROY_CONTEXT(lookup_memctx_);
    lookup_memctx_ = nullptr;
  }
  return ret;
}

int ObLogicalTableHelper::validate_json_vec_raw_bytes(const common::ObString &vec_str,
                                                      int64_t dim_hint)
{
  // Encoder side (ob_logical_table_encoder.cpp ObCollectionSQLType branch) writes
  // the raw float[dim] bytes (after stripping the LOB header via read_real_string_data)
  // straight into an ObJsonString payload. So vec_str here is exactly dim*sizeof(float)
  // raw bytes — NOT an ASCII "[f1,f2,...]" textual JSON.
  int ret = OB_SUCCESS;
  const int64_t raw_len = vec_str.length();
  if (OB_UNLIKELY(raw_len <= 0) ||
      OB_UNLIKELY(raw_len % static_cast<int64_t>(sizeof(float)) != 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("[LT hybrid DAS] vec raw bytes length invalid",
             K(ret), K(raw_len), K(dim_hint));
  } else {
    const int64_t dim = raw_len / static_cast<int64_t>(sizeof(float));
    if (dim_hint > 0 && OB_UNLIKELY(dim != dim_hint)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[LT hybrid DAS] vec dim mismatch with hint",
               K(ret), K(dim), K(dim_hint), K(raw_len));
    }
  }
  return ret;
}

int ObLogicalTableHelper::decode_json_vec_str_to_lob(const common::ObString &vec_str,
                                                     int64_t dim_hint,
                                                     common::ObIAllocator &alloc,
                                                     common::ObString &out_lob)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(validate_json_vec_raw_bytes(vec_str, dim_hint))) {
  } else {
    const int64_t raw_len = vec_str.length();
    const int64_t dim = raw_len / static_cast<int64_t>(sizeof(float));
    // Allocate inrow LOB buffer: ObLobCommon header + float data
    const int64_t lob_buf_len = static_cast<int64_t>(sizeof(ObLobCommon)) + raw_len;
    char *lob_buf = static_cast<char *>(alloc.alloc(lob_buf_len));
    if (OB_ISNULL(lob_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[LT hybrid DAS] alloc vec lob buf failed",
               K(ret), K(dim), K(lob_buf_len));
    } else {
      // Initialize ObLobCommon inrow header (version=1, in_row=1)
      new (lob_buf) ObLobCommon();
      MEMCPY(lob_buf + sizeof(ObLobCommon), vec_str.ptr(), raw_len);
      out_lob.assign_ptr(lob_buf, static_cast<int32_t>(lob_buf_len));
    }
  }
  return ret;
}

// After compact_logical_batch_exprs moves base-column datums to their new batch
// slots, any computed expr inside the residual filter trees (e.g. json_extract on
// data_content) still carries evaluated_flags/datums for the OLD slot layout. Those
// exprs are frequently shared with the upper operator's output projection, which
// would then reuse stale datums from pre-compaction indices. Clear evaluated flags
// for every expr in the filter trees EXCEPT the compacted leaf exprs (whose datums
// were moved and remain valid), so consumers re-evaluate on the compacted batch.
static bool is_expr_in_list_(const ObExpr *expr, const common::ObIArray<ObExpr *> &exprs)
{
  bool found = false;
  for (int64_t i = 0; !found && i < exprs.count(); ++i) {
    found = (exprs.at(i) == expr);
  }
  return found;
}

static void clear_uncompacted_expr_flags_for_batch_(
    ObExpr &expr,
    ObEvalCtx &eval_ctx,
    const int64_t count,
    const common::ObIArray<ObExpr *> &compacted_exprs)
{
  if (!is_expr_in_list_(&expr, compacted_exprs)) {
    for (int64_t i = 0; i < expr.arg_cnt_; ++i) {
      ObExpr *arg = expr.args_[i];
      if (OB_NOT_NULL(arg)) {
        clear_uncompacted_expr_flags_for_batch_(*arg, eval_ctx, count, compacted_exprs);
      }
    }
    if (expr.is_batch_result() && count > 0) {
      ObBitVector &eval_flags = expr.get_evaluated_flags(eval_ctx);
      for (int64_t r = 0; r < count; ++r) {
        eval_flags.unset(r);
      }
    }
    expr.get_eval_info(eval_ctx).clear_evaluated_flag();
  }
}

int ObLogicalTableHelper::eval_logical_residual_filters_with_extra_compact_(
    const sql::ExprFixedArray &residual_filters,
    const sql::ExprFixedArray &access_exprs,
    const sql::ExprFixedArray &logical_output_exprs,
    int64_t &row_count,
    ObEvalCtx &eval_ctx,
    const common::ObIArray<ObExpr *> *extra_compact_exprs)
{
  int ret = OB_SUCCESS;
  if (residual_filters.empty() || row_count <= 0) {
  } else if (1 == row_count) {
    bool filtered = false;
    // project_logical_columns writes logical outputs to batch slots [0, row_count).
    // filter_row -> ObExpr::eval uses eval_ctx.batch_idx_ as the active row index.
    // After a vectorized storage read, batch_idx_ may stay non-zero even when only
    // one row is returned (e.g. batch_idx=1, row_count=1), while projection only
    // filled slot 0. Align batch context before filtering to avoid reading empty slots.
    ObEvalCtx::BatchInfoScopeGuard batch_guard(eval_ctx);
    batch_guard.set_batch_size(row_count);
    batch_guard.set_batch_idx(0);
    if (OB_FAIL(ObOperator::filter_row(eval_ctx, residual_filters, filtered))) {
      LOG_WARN("eval logical residual filter for single row failed", K(ret));
    } else if (filtered) {
      row_count = 0;
    }
  } else {
    ObIAllocator &alloc = eval_ctx.exec_ctx_.get_allocator();
    const int64_t batch_cap = eval_ctx.max_batch_size_;
    void *skip_buf = alloc.alloc(ObBitVector::memory_size(batch_cap));
    if (OB_ISNULL(skip_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc skip vector for logical residual filter failed", K(ret), K(batch_cap));
    } else {
      ObBitVector &skip = *to_bit_vector(skip_buf);
      skip.init(static_cast<uint32_t>(batch_cap));
      skip.reset(row_count);
      bool all_filtered = false;
      bool all_active = true;
      if (OB_FAIL(ObOperator::filter_batch_rows(eval_ctx,
                                                residual_filters,
                                                skip,
                                                row_count,
                                                all_filtered,
                                                all_active))) {
        LOG_WARN("eval logical residual filter batch failed", K(ret), K(row_count));
      }
      if (OB_FAIL(ret)) {
      } else if (all_filtered) {
        row_count = 0;
      } else {
        common::ObSEArray<ObExpr *, 32> compact_exprs;
        for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
          append_unique_expr(access_exprs.at(i), compact_exprs);
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < logical_output_exprs.count(); ++i) {
          append_unique_expr(logical_output_exprs.at(i), compact_exprs);
        }
        if (OB_NOT_NULL(extra_compact_exprs)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < extra_compact_exprs->count(); ++i) {
            append_unique_expr(extra_compact_exprs->at(i), compact_exprs);
          }
        }
        if (OB_FAIL(compact_logical_batch_exprs(eval_ctx, compact_exprs, skip, row_count))) {
          LOG_WARN("compact batch after logical residual filter failed", K(ret));
        } else {
          // Filter sub-exprs (e.g. json_extract over data_content) evaluated above hold
          // datums indexed by the pre-compaction layout; they may be shared with upper
          // operators' output projection. Reset their evaluated flags (keeping compacted
          // leaf exprs intact) so they are re-evaluated against the compacted batch.
          for (int64_t i = 0; i < residual_filters.count(); ++i) {
            ObExpr *filter = residual_filters.at(i);
            if (OB_NOT_NULL(filter)) {
              clear_uncompacted_expr_flags_for_batch_(*filter, eval_ctx,
                                                      eval_ctx.max_batch_size_,
                                                      compact_exprs);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogicalTableHelper::eval_logical_residual_filters(
    const sql::ExprFixedArray &residual_filters,
    const sql::ExprFixedArray &access_exprs,
    const sql::ExprFixedArray &logical_output_exprs,
    int64_t &row_count,
    ObEvalCtx &eval_ctx)
{
  return eval_logical_residual_filters_with_extra_compact_(
      residual_filters, access_exprs, logical_output_exprs, row_count, eval_ctx, nullptr);
}

int ObLogicalTableHelper::eval_logical_residual_filters(
    const ObDASScanCtDef &scan_ctdef,
    int64_t &row_count,
    ObEvalCtx &eval_ctx)
{
  return eval_logical_residual_filters(scan_ctdef.logical_residual_filters_,
                                     scan_ctdef.pd_expr_spec_.access_exprs_,
                                     scan_ctdef.logical_output_exprs_,
                                     row_count,
                                     eval_ctx);
}

int ObLogicalTableHelper::eval_logical_residual_filters(
    const ObDASScalarScanCtDef &scalar_ctdef,
    int64_t &row_count,
    ObEvalCtx &eval_ctx,
    const common::ObIArray<ObExpr *> *rowid_exprs_for_compact)
{
  return eval_logical_residual_filters_with_extra_compact_(
      scalar_ctdef.logical_residual_filters_,
      scalar_ctdef.pd_expr_spec_.access_exprs_,
      scalar_ctdef.logical_output_exprs_,
      row_count,
      eval_ctx,
      rowid_exprs_for_compact);
}

int ObLogicalTableHelper::read_kv_string_at_row(ObExpr &kv_expr,
                                                ObEvalCtx &eval_ctx,
                                                int64_t row_idx,
                                                common::ObIAllocator &alloc,
                                                bool &out_is_null,
                                                common::ObString &out_str)
{
  int ret = OB_SUCCESS;
  out_is_null = true;
  out_str.reset();
  if (kv_expr.enable_rich_format() && is_valid_format(kv_expr.get_format(eval_ctx))) {
    ObIVector *vec = kv_expr.get_vector(eval_ctx);
    if (OB_ISNULL(vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid DAS] kv expr vector is null", K(ret), K(row_idx));
    } else if (vec->is_null(row_idx)) {
      out_is_null = true;
    } else {
      out_is_null = false;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              alloc, vec, kv_expr.datum_meta_, out_str, row_idx))) {
        LOG_WARN("[LT hybrid DAS] read kv from vector failed", K(ret), K(row_idx));
      }
    }
  } else {
    ObDatum &d = kv_expr.locate_expr_datum(eval_ctx, row_idx);
    if (d.is_null()) {
      out_is_null = true;
    } else {
      out_is_null = false;
      out_str = d.get_string();
      // Preserve existing "only-unwrap-when-valid-ObLobCommon" defensive check
      // (hybrid lookup may have written payload-only into datum).
      bool is_real_lob = false;
      if (OB_NOT_NULL(out_str.ptr())
          && out_str.length() >= static_cast<int32_t>(sizeof(ObLobCommon))) {
        const ObLobCommon *lob_hdr = reinterpret_cast<const ObLobCommon *>(out_str.ptr());
        is_real_lob = lob_hdr->is_valid();
      }
      if (is_real_lob
          && OB_FAIL(ObTextStringHelper::read_real_string_data(
              alloc, d, kv_expr.datum_meta_, out_str))) {
        LOG_WARN("[LT hybrid DAS] read kv from datum failed", K(ret), K(row_idx));
      }
    }
  }
  return ret;
}

int ObLogicalTableHelper::project_ivf_cid_rowid_posting_(
    const ObLogicalTableScanCtDef &lt_ctdef,
    const ObLogicalTableColumnMapping &mapping,
    common::ObIArray<ObExpr *> &logical_output_exprs,
    const int64_t row_count,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObExpr *vec_out_expr = nullptr;
  ObExpr *rowid_out_expr = nullptr;
  ObExpr *cid_out_expr = nullptr;
  if (row_count <= 0 || mapping.count() != logical_output_exprs.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf cid rowid posting projection argument",
             K(ret), K(row_count), K(mapping.count()), K(logical_output_exprs.count()));
  } else if (OB_ISNULL(lt_ctdef.kv_key_expr_) || OB_ISNULL(lt_ctdef.kv_value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ivf cid rowid posting kv expr is null", K(ret),
             KP(lt_ctdef.kv_key_expr_), KP(lt_ctdef.kv_value_expr_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mapping.count(); ++i) {
      ObExpr *out_expr = logical_output_exprs.at(i);
      const ObLogicalColumnMappingEntry &entry = mapping.entries_.at(i);
      if (OB_NOT_NULL(out_expr)) {
        if (FROM_VALUE_PATH == entry.source_
            && (entry.is_vector_col_
                || ob_is_collection_sql_type(entry.obj_meta_.get_type()))) {
          vec_out_expr = out_expr;
        } else if (FROM_ROWID == entry.source_ || entry.is_rowid_) {
          rowid_out_expr = out_expr;
        } else if (FROM_KEY_CID == entry.source_) {
          cid_out_expr = out_expr;
        }
        if (out_expr->is_batch_result() && out_expr->enable_rich_format()
            && OB_FAIL(out_expr->init_vector_for_write(eval_ctx, VEC_UNIFORM, row_count))) {
          LOG_WARN("init ivf cid rowid posting output vector failed",
                   K(ret), K(i), K(row_count));
        } else if (!out_expr->is_batch_result() || !out_expr->enable_rich_format()) {
          out_expr->locate_datums_for_update(eval_ctx, row_count);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(vec_out_expr) || OB_ISNULL(rowid_out_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ivf cid rowid posting output expr is null",
                 K(ret), KP(vec_out_expr), KP(rowid_out_expr));
      } else if (OB_FAIL(project_ivf_cid_rowid_posting_rows_(
                     lt_ctdef, *vec_out_expr, *rowid_out_expr,
                     cid_out_expr, row_count, eval_ctx))) {
        LOG_WARN("project ivf cid rowid posting rows failed", K(ret), K(row_count));
      }
    }
  }
  return ret;
}

int ObLogicalTableHelper::decode_ivf_cid_rowid_key_(
    const common::ObString &key,
    uint64_t &rowid,
    common::ObString &cid)
{
  int ret = OB_SUCCESS;
  const int64_t min_key_len = share::OB_LOGICAL_TABLE_KEY_PREFIX_LEN
      + common::OB_DOC_ID_COLUMN_BYTE_LENGTH + static_cast<int64_t>(sizeof(rowid));
  rowid = 0;
  cid.reset();
  if (OB_ISNULL(key.ptr()) || key.length() < min_key_len) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ivf cid rowid key is invalid", K(ret), K(key.length()), K(min_key_len));
  } else {
    const unsigned char *src = reinterpret_cast<const unsigned char *>(key.ptr());
    int64_t rowid_pos = key.length() - static_cast<int64_t>(sizeof(rowid));
    if (OB_FAIL(share::ObLogicalTableCodecUtils::decode_uint64(
            src, key.length(), rowid_pos, rowid))) {
      LOG_WARN("decode ivf cid rowid key failed", K(ret), K(key.length()));
    } else {
      cid.assign_ptr(key.ptr() + share::OB_LOGICAL_TABLE_KEY_PREFIX_LEN,
                     common::OB_DOC_ID_COLUMN_BYTE_LENGTH);
    }
  }
  return ret;
}

int ObLogicalTableHelper::set_ivf_raw_vector_output_(
    ObExpr &out_expr,
    const common::ObString &raw_vec,
    const int64_t row_idx,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (out_expr.is_batch_result() && out_expr.enable_rich_format()) {
    if (OB_FAIL(ObTextStringHelper::string_to_templob_result(
            out_expr, eval_ctx, raw_vec, row_idx))) {
      LOG_WARN("wrap ivf cid rowid raw vector as templob failed",
               K(ret), K(row_idx), K(raw_vec.length()));
    }
  } else {
    ObDatum &out_datum = out_expr.locate_expr_datum(eval_ctx, row_idx);
    if (OB_FAIL(ObTextStringHelper::string_to_templob_result(
            out_expr, eval_ctx, out_datum, raw_vec))) {
      LOG_WARN("wrap ivf cid rowid raw vector datum as templob failed",
               K(ret), K(row_idx), K(raw_vec.length()));
    }
  }
  return ret;
}

int ObLogicalTableHelper::project_ivf_cid_rowid_posting_rows_(
    const ObLogicalTableScanCtDef &lt_ctdef,
    ObExpr &vec_out_expr,
    ObExpr &rowid_out_expr,
    ObExpr *cid_out_expr,
    const int64_t row_count,
    ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  ObExpr &key_expr = *lt_ctdef.kv_key_expr_;
  ObExpr &val_expr = *lt_ctdef.kv_value_expr_;
  const int64_t dim_hint = lt_ctdef.ivf_vec_project_dim_ > 0 ? lt_ctdef.ivf_vec_project_dim_ : 0;
  const bool use_rich_input = key_expr.enable_rich_format()
      && val_expr.enable_rich_format()
      && is_valid_format(key_expr.get_format(eval_ctx))
      && is_valid_format(val_expr.get_format(eval_ctx));
  ObIVector *key_vec = use_rich_input ? key_expr.get_vector(eval_ctx) : nullptr;
  ObIVector *val_vec = use_rich_input ? val_expr.get_vector(eval_ctx) : nullptr;
  ObIVector *vec_out_vec = get_logical_output_vector_(vec_out_expr, eval_ctx);
  ObIVector *rowid_out_vec = get_logical_output_vector_(rowid_out_expr, eval_ctx);
  if (use_rich_input && (OB_ISNULL(key_vec) || OB_ISNULL(val_vec))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ivf cid rowid posting projection kv vector is null",
             K(ret), KP(key_vec), KP(val_vec));
  } else {
    // Use row-scoped scratch because out-row LOB reads materialize into this allocator.
    common::ObArenaAllocator row_scratch_alloc(
        "LTIVFPost", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    ObString key_bin;
    ObString raw_vec;
    ObString cid;
    batch_info_guard.set_batch_size(row_count);
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
      bool key_is_null = false;
      bool val_is_null = false;
      key_bin.reset();
      raw_vec.reset();
      cid.reset();
      batch_info_guard.set_batch_idx(row_idx);
      if (use_rich_input) {
        key_is_null = key_vec->is_null(row_idx);
        val_is_null = val_vec->is_null(row_idx);
        if (!key_is_null) {
          key_bin.assign_ptr(key_vec->get_payload(row_idx),
                             key_vec->get_length(row_idx));
        }
        if (!val_is_null
            && OB_FAIL(ObTextStringHelper::read_real_string_data(
                row_scratch_alloc, val_vec, val_expr.datum_meta_, raw_vec, row_idx))) {
          LOG_WARN("ivf cid rowid posting projection read rich kv value failed",
                   K(ret), K(row_idx));
        }
      } else if (OB_FAIL(read_kv_string_at_row(
                     key_expr, eval_ctx, row_idx, row_scratch_alloc,
                     key_is_null, key_bin))) {
        LOG_WARN("ivf cid rowid posting projection read kv key failed", K(ret), K(row_idx));
      } else if (OB_FAIL(read_kv_string_at_row(
                     val_expr, eval_ctx, row_idx, row_scratch_alloc,
                     val_is_null, raw_vec))) {
        LOG_WARN("ivf cid rowid posting projection read kv value failed", K(ret), K(row_idx));
      }
      if (OB_SUCC(ret) && (key_is_null || val_is_null)) {
        if (OB_NOT_NULL(vec_out_vec)) {
          vec_out_vec->set_null(row_idx);
        } else {
          vec_out_expr.locate_expr_datum(eval_ctx, row_idx).set_null();
        }
        if (OB_NOT_NULL(rowid_out_vec)) {
          rowid_out_vec->set_null(row_idx);
        } else {
          rowid_out_expr.locate_expr_datum(eval_ctx, row_idx).set_null();
        }
        if (OB_NOT_NULL(cid_out_expr)) {
          set_logical_output_null(*cid_out_expr, row_idx, eval_ctx);
        }
      } else if (OB_SUCC(ret) && !raw_vec.empty()
                 && OB_FAIL(validate_json_vec_raw_bytes(raw_vec, dim_hint))) {
        LOG_WARN("ivf cid rowid raw vector validation failed",
                 K(ret), K(row_idx), K(raw_vec.length()), K(dim_hint));
      } else if (OB_SUCC(ret)) {
        uint64_t rowid = 0;
        if (OB_FAIL(decode_ivf_cid_rowid_key_(key_bin, rowid, cid))) {
          LOG_WARN("decode ivf cid rowid posting key failed", K(ret), K(row_idx));
        } else if (OB_NOT_NULL(rowid_out_vec)) {
          rowid_out_vec->set_uint(row_idx, rowid);
        } else {
          rowid_out_expr.locate_expr_datum(eval_ctx, row_idx).set_uint(rowid);
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(cid_out_expr)) {
          if (OB_FAIL(set_logical_output_string(
                  *cid_out_expr, row_idx, eval_ctx, cid))) {
            LOG_WARN("set ivf cid rowid posting cid failed", K(ret), K(row_idx));
          }
        }
        if (OB_SUCC(ret) && raw_vec.empty()) {
          // The aux5 writer reserves an empty raw payload for a NULL vector.
          set_logical_output_null(vec_out_expr, row_idx, eval_ctx);
        } else if (OB_SUCC(ret)) {
          // A temporary LOB cell avoids unaligned raw-float header probes.
          if (OB_FAIL(set_ivf_raw_vector_output_(
                  vec_out_expr, raw_vec, row_idx, eval_ctx))) {
            LOG_WARN("set ivf cid rowid raw vector output failed",
                     K(ret), K(row_idx), K(raw_vec.length()));
          }
        }
      }
      row_scratch_alloc.reuse();
    }
  }
  if (OB_NOT_NULL(vec_out_vec)) {
    vec_out_expr.set_evaluated_flag(eval_ctx);
  }
  if (OB_NOT_NULL(rowid_out_vec)) {
    rowid_out_expr.set_evaluated_flag(eval_ctx);
  }
  return ret;
}

int ObLogicalTableHelper::project_logical_columns(
    const ObLogicalTableScanCtDef &lt_ctdef,
    const ObLogicalTableRtDef &lt_rt,
    const ObDASScanCtDef &scan_ctdef,
    common::ObIArray<ObExpr *> &logical_output_exprs,
    int64_t row_count,
    ObEvalCtx &eval_ctx,
    ObLogicalTableScanRtDef *lt_scan_rt)
{
  return project_logical_columns(lt_ctdef,
                                 lt_rt,
                                 scan_ctdef.logical_column_mapping_,
                                 scan_ctdef.ir_scan_type_,
                                 scan_ctdef.logical_table_aux_type_,
                                 logical_output_exprs,
                                 row_count,
                                 eval_ctx,
                                 lt_scan_rt,
                                 &scan_ctdef.result_output_,
                                 scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown());
}

int ObLogicalTableHelper::project_logical_columns(
    const ObLogicalTableScanCtDef &lt_ctdef,
    const ObLogicalTableRtDef &lt_rt,
    const ObLogicalTableColumnMapping &mapping,
    const ObTSCIRScanType ir_scan_type,
    const share::ObLogicalTableAuxType aux_type,
    common::ObIArray<ObExpr *> &logical_output_exprs,
    int64_t row_count,
    ObEvalCtx &eval_ctx,
    ObLogicalTableScanRtDef *lt_scan_rt,
    const sql::ExprFixedArray *result_output_ptr,
    const bool storage_agg_pushdown)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lt_scan_rt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lt_scan_rt is null for ctdef+lt_rt project_logical_columns", K(ret));
  } else {
    ret = project_logical_columns_with_path_cache(lt_ctdef,
                                                  lt_rt,
                                                  lt_scan_rt->path_cache_,
                                                  mapping,
                                                  ir_scan_type,
                                                  aux_type,
                                                  logical_output_exprs,
                                                  row_count,
                                                  eval_ctx,
                                                  result_output_ptr,
                                                  storage_agg_pushdown);
  }
  return ret;
}

int ObLogicalTableHelper::project_logical_columns_with_path_cache(
    const ObLogicalTableScanCtDef &lt_ctdef,
    const ObLogicalTableRtDef &lt_rt,
    common::ObJsonPathCache &path_cache,
    const ObLogicalTableColumnMapping &mapping,
    const ObTSCIRScanType ir_scan_type,
    const share::ObLogicalTableAuxType aux_type,
    common::ObIArray<ObExpr *> &logical_output_exprs,
    int64_t row_count,
    ObEvalCtx &eval_ctx,
    const sql::ExprFixedArray *result_output_ptr,
    const bool storage_agg_pushdown)
{
  int ret = OB_SUCCESS;
  // Storage aggregate rows are not aux5 posting rows. Every normal aux5 row must use
  // the raw-vector projection below and must never enter the generic JSON projection.
  const bool is_ivf_cid_rowid_posting = share::LT_AUX_IVF_CID_ROWID == aux_type
      && !storage_agg_pushdown && row_count > 0;
  // A multi-ltable physical range needs the row's real ns/lt values after the scan
  // iterator has removed rows belonging to other index/aux prefixes.
  const bool decode_physical_prefix = !storage_agg_pushdown
      && share::LT_AUX_IVF_CID_ROWID != aux_type
      && lt_ctdef.lt_id_start_expr_ != lt_ctdef.lt_id_end_expr_;
  if (mapping.count() != logical_output_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mapping count mismatch with logical_output_exprs count", K(ret),
             K(mapping.count()), K(logical_output_exprs.count()));
  } else if (is_ivf_cid_rowid_posting) {
    if (OB_FAIL(project_ivf_cid_rowid_posting_(
            lt_ctdef, mapping, logical_output_exprs, row_count, eval_ctx))) {
      LOG_WARN("project ivf cid rowid posting failed", K(ret), K(row_count));
    }
  } else if (OB_ISNULL(lt_ctdef.kv_value_expr_)
             || (decode_physical_prefix && OB_ISNULL(lt_ctdef.kv_key_expr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("kv expr is null in lt_ctdef", K(ret), KP(lt_ctdef.kv_key_expr_),
             KP(lt_ctdef.kv_value_expr_), K(decode_physical_prefix));
  } else {
    ObIAllocator &alloc = eval_ctx.exec_ctx_.get_allocator();
    const int64_t entry_cnt = mapping.count();
    // Per expr: reset datum buffers; rich format also init batch vector for direct write
    // (ObDASVecIndexDriverIter pattern).
    for (int64_t i = 0; OB_SUCC(ret) && i < entry_cnt; ++i) {
      ObExpr *out_expr = logical_output_exprs.at(i);
      if (OB_NOT_NULL(out_expr)) {
        if (FROM_DOMAIN_FILL == mapping.entries_.at(i).source_) {
          // cid is produced outside this projection: filled by the domain-id
          // merge iterator after projection (local), or already materialized
          // from the remote scan result_output. Resetting its buffer here would
          // wipe the remotely materialized value before the keep path runs.
          continue;
        }
        if (out_expr->is_batch_result() && out_expr->enable_rich_format()) {
          // Align with storage SINGLE_GET batch path (ObMultipleMerge::get_next_rows
          // calls init_exprs_uniform_header for rich-format ops). VEC_UNIFORM aliases
          // the datum array, so writes via set_vector and reads via locate_batch_datums
          // share one buffer — required because downstream FTS token iter reads by datum.
          // use_reserve_buf=true also resets datum.ptr_ to frame.res_buf, eliminating
          // stale pointers carried over from prior plan execution.
          if (OB_FAIL(out_expr->init_vector_for_write(eval_ctx,
                                                      VEC_UNIFORM,
                                                      row_count))) {
            LOG_WARN("[LT hybrid DAS] init logical output vector for write failed",
                    K(ret), K(i), K(row_count));
          }
        } else {
          out_expr->locate_datums_for_update(eval_ctx, row_count);
        }
      }
    }
    // scope batch_idx to row_idx so per-row LOB/string buffers allocated via
    // ObExpr::get_str_res_mem(ctx, len) land in the correct per-row slot.
    // Without this guard, the helper falls back to ctx.get_batch_idx() which
    // stays fixed across the row loop and the last row's bytes overwrite all
    // earlier rows' LOB outputs (observed as cross-row data_content mixing).
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
    batch_info_guard.set_batch_size(row_count);
    // JSON path seek over binary JSON deep-clones hit nodes via
    // ObJsonBin::clone_new_node into the ObJsonBin's allocator. Previously this
    // was the request-lifetime exec_ctx allocator, so per-row/per-column clones
    // accumulated for the whole statement (observed 19G under concurrent hybrid
    // search). Use a per-row scratch arena that holds only the transient clones;
    // all materialized outputs are copied into the expr frame within the row,
    // so reuse()-ing this arena at row end is safe and bounds peak usage.
    common::ObArenaAllocator json_scratch_alloc(
        "LTJsonSeek", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
      batch_info_guard.set_batch_idx(row_idx);
      bool kv_value_is_null = false;
      ObString kv_value_bin;
      ObString physical_key;
      share::ObLogicalTableKeyPrefix physical_prefix;
      uint64_t row_namespace_id = lt_rt.namespace_id_;
      uint64_t row_logical_table_id = lt_rt.logical_table_id_;
      if (decode_physical_prefix) {
        bool key_is_null = false;
        if (OB_FAIL(read_kv_string_at_row(*lt_ctdef.kv_key_expr_, eval_ctx, row_idx,
                                          json_scratch_alloc, key_is_null, physical_key))) {
          LOG_WARN("read kv key for mixed aux projection failed", K(ret), K(row_idx));
        } else if (OB_UNLIKELY(key_is_null || physical_key.empty())) {
          ret = OB_INVALID_DATA;
          LOG_WARN("kv key for mixed aux projection is empty", K(ret), K(row_idx),
                   K(key_is_null));
        } else {
          int64_t key_pos = 0;
          if (OB_FAIL(physical_prefix.decode(
                  physical_key.ptr(), physical_key.length(), key_pos))) {
            LOG_WARN("decode mixed aux physical key prefix failed", K(ret), K(row_idx),
                     K(physical_key.length()));
          } else if (OB_UNLIKELY(aux_type != physical_prefix.aux_type_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected physical aux prefix after mixed row filter",
                     K(ret), K(row_idx), K(aux_type), K(physical_prefix));
          } else {
            row_namespace_id = physical_prefix.namespace_id_;
            row_logical_table_id = physical_prefix.logical_table_id_;
          }
        }
      }
      // kv_value is longblob: unwrap into per-row scratch so the materialized
      // JSON byte string does not accumulate in the request-lifetime alloc.
      // Safe because kv_value_bin is only consumed by ObJsonBin::seek within
      // this row; all final outputs copy out of hit_node into persistent
      // alloc/expr frame.
      if (OB_SUCC(ret)
          && OB_FAIL(read_kv_string_at_row(*lt_ctdef.kv_value_expr_, eval_ctx,
                                           row_idx, json_scratch_alloc,
                                           kv_value_is_null, kv_value_bin))) {
        LOG_WARN("[LT hybrid DAS] read kv_value at row failed", K(ret), K(row_idx));
      }
      if (OB_SUCC(ret) && (storage_agg_pushdown || kv_value_is_null)) {
        // kv_value is null: set most logical columns to null,
        // except FROM_QUERY_RANGE (ns_id/lt_id) and FROM_KEY_SUFFIX (decoded from kv_key)
        LOG_TRACE("KV value is null, filling FROM_QUERY_RANGE and FROM_KEY_SUFFIX columns",
            K(row_namespace_id), K(row_logical_table_id));
        for (int64_t i = 0; OB_SUCC(ret) && i < entry_cnt; ++i) {
          const ObLogicalColumnMappingEntry &entry = mapping.entries_.at(i);
          ObExpr *out_expr = logical_output_exprs.at(i);
          if (OB_ISNULL(out_expr)) {
            continue;
          }
          if (FROM_DOMAIN_FILL == entry.source_) {
            // cid is written by domain-merge fill (or bound from the remote scan
            // result_output) independently of kv_value; keep it as-is.
          } else if (FROM_QUERY_RANGE == entry.source_) {
            const uint64_t range_val = entry.is_query_range_ns_
                ? row_namespace_id
                : row_logical_table_id;
            ObLogicalTableHelper::set_logical_output_uint(*out_expr, row_idx, eval_ctx, range_val);
          } else if (FROM_KEY_CID == entry.source_) {
            // IVF ROWID_CID is a key-only auxiliary table.  Its value datum is
            // legitimately null, while CID is still encoded in the key suffix.
            // Do not fall through to the generic null fill in this path.
            if (OB_ISNULL(lt_ctdef.kv_key_expr_)) {
              ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
            } else {
              bool k_is_null = false;
              ObString key_bin;
              if (OB_FAIL(read_kv_string_at_row(*lt_ctdef.kv_key_expr_, eval_ctx,
                                                row_idx, alloc, k_is_null, key_bin))) {
                LOG_WARN("[LT hybrid DAS] read kv_key in null-value FROM_KEY_CID failed",
                         K(ret), K(row_idx));
              } else if (k_is_null || key_bin.empty()) {
                ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
              } else {
                ObString cid_bytes;
                if (entry.key_suffix_ordinal_ < 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("FROM_KEY_CID without ordinal in null-value path",
                           K(ret), K(entry));
                } else if (OB_FAIL(share::ObLogicalTableDecoder::locate_key_cid_bytes_at_ordinal(
                               key_bin, aux_type, entry.key_suffix_ordinal_, cid_bytes))) {
                  LOG_WARN("locate_key_cid_bytes_at_ordinal failed in null-value path",
                           K(ret), K(aux_type), K(entry));
                } else {
                  ObLogicalTableHelper::set_logical_output_string(
                      *out_expr, row_idx, eval_ctx, cid_bytes);
                }
              }
            }
          } else if (FROM_KEY_SUFFIX == entry.source_) {
            // Even when kv_value is null, key suffix may still contain data (e.g. FTS token)
            if (OB_ISNULL(lt_ctdef.kv_key_expr_)) {
              ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
            } else {
              bool k_is_null = false;
              ObString key_bin;
              if (OB_FAIL(read_kv_string_at_row(*lt_ctdef.kv_key_expr_, eval_ctx,
                                                row_idx, json_scratch_alloc, k_is_null, key_bin))) {
                LOG_WARN("[LT hybrid DAS] read kv_key in null-value FROM_KEY_SUFFIX failed",
                         K(ret), K(row_idx));
              } else if (k_is_null || key_bin.empty()) {
                ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
              } else if (entry.obj_meta_.is_varchar_or_char()
                         || entry.obj_meta_.get_type() == ObVarcharType) {
                ObString tok;
                if (OB_FAIL(share::ObLogicalTableDecoder::decode_key_varchar_at_ordinal(
                        key_bin, aux_type, entry.key_suffix_ordinal_, json_scratch_alloc, tok))) {
                  LOG_WARN("decode_key_varchar_at_ordinal failed in null-value path",
                           K(ret), K(entry));
                  ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
                } else {
                  if (OB_FAIL(ObLogicalTableHelper::set_logical_output_string(
                          *out_expr, row_idx, eval_ctx, tok))) {
                    LOG_WARN("copy logical key suffix output failed", K(ret), K(row_idx), K(i));
                  }
                }
              } else {
                ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
              }
            }
          } else if (FROM_ROWID == entry.source_) {
            // FROM_ROWID can also be decoded from kv_key when value is null
            if (OB_ISNULL(lt_ctdef.kv_key_expr_)) {
              ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
            } else {
              bool k_is_null = false;
              ObString key_bin;
              if (OB_FAIL(read_kv_string_at_row(*lt_ctdef.kv_key_expr_, eval_ctx,
                                                row_idx, json_scratch_alloc, k_is_null, key_bin))) {
                LOG_WARN("[LT hybrid DAS] read kv_key in null-value FROM_ROWID failed",
                         K(ret), K(row_idx));
              } else if (k_is_null || key_bin.empty()) {
                ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
              } else {
                uint64_t rowid_val = 0;
                if (entry.key_suffix_ordinal_ < 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("FROM_ROWID without ordinal in null-value path",
                           K(ret), K(entry));
                  ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
                } else if (OB_FAIL(share::ObLogicalTableDecoder::decode_key_uint64_at_ordinal(
                               key_bin, aux_type, entry.key_suffix_ordinal_, rowid_val))) {
                  LOG_WARN("decode_key_uint64_at_ordinal failed in null-value path",
                           K(ret), K(aux_type), K(entry));
                  ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
                }
                if (OB_SUCC(ret)) {
                  ObLogicalTableHelper::set_logical_output_uint(*out_expr, row_idx, eval_ctx, rowid_val);
                }
              }
            }
          } else {
            ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
          }
        }
      } else if (OB_SUCC(ret)) {
        // kv_value_bin already unwrapped by read_kv_string_at_row helper.
        ObJsonBin j_bin(kv_value_bin.ptr(), kv_value_bin.length(), &json_scratch_alloc);
        ObIJsonBase *j_base = &j_bin;
        if (OB_FAIL(j_bin.reset_iter())) {
          LOG_WARN("failed to reset json binary iterator", K(ret), K(row_idx));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < entry_cnt; ++i) {
            const ObLogicalColumnMappingEntry &entry = mapping.entries_.at(i);
            ObExpr *out_expr = logical_output_exprs.at(i);
            if (OB_ISNULL(out_expr)) {
              continue;
            }
            if (FROM_DOMAIN_FILL == entry.source_) {
              // cid was already written into this same output expr by domain-merge
              // fill (local) or bound from the remote scan result_output; do not
              // overwrite it with a kv_value decode result.
            } else if (FROM_QUERY_RANGE == entry.source_) {
              // FROM_QUERY_RANGE: fill ns_id or lt_id from query range prefix
              const uint64_t range_val = entry.is_query_range_ns_
                  ? row_namespace_id
                  : row_logical_table_id;
              ObLogicalTableHelper::set_logical_output_uint(*out_expr, row_idx, eval_ctx, range_val);
            } else if (entry.is_rowid_) {
              // FROM_ROWID: decode 8B uint64 column at ordinal in kv_key.
              if (OB_ISNULL(lt_ctdef.kv_key_expr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("kv_key_expr is null in lt_ctx", K(ret));
              } else {
                bool k_is_null = false;
                ObString kv_key_bin;
                if (OB_FAIL(read_kv_string_at_row(*lt_ctdef.kv_key_expr_, eval_ctx,
                                                  row_idx, json_scratch_alloc, k_is_null, kv_key_bin))) {
                  LOG_WARN("[LT hybrid DAS] read kv_key for FROM_ROWID failed",
                           K(ret), K(row_idx));
                }
                uint64_t rowid_val = 0;
                if (OB_SUCC(ret)) {
                  if (entry.key_suffix_ordinal_ < 0) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("[LT hybrid DAS] FROM_ROWID without ordinal", K(ret), K(entry));
                  } else if (OB_FAIL(share::ObLogicalTableDecoder::decode_key_uint64_at_ordinal(
                                  kv_key_bin, aux_type, entry.key_suffix_ordinal_, rowid_val))) {
                    LOG_WARN("[LT hybrid DAS] decode_key_uint64_at_ordinal failed",
                             K(ret), K(row_idx), K(aux_type),
                             "key_len", kv_key_bin.length(),
                             "ordinal", entry.key_suffix_ordinal_);
                  }
                  if (OB_SUCC(ret)) {
                    ObLogicalTableHelper::set_logical_output_uint(*out_expr, row_idx, eval_ctx, rowid_val);
                  }
                }
              }
            } else if (FROM_KEY_CID == entry.source_) {
              // FROM_KEY_CID: 16B order-preserving cid bytes from kv_key (IVF aux).
              if (OB_ISNULL(lt_ctdef.kv_key_expr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("kv_key_expr is null for FROM_KEY_CID", K(ret));
              } else {
                bool k_is_null = false;
                ObString key_bin;
                if (OB_FAIL(read_kv_string_at_row(*lt_ctdef.kv_key_expr_, eval_ctx,
                                                  row_idx, json_scratch_alloc, k_is_null, key_bin))) {
                  LOG_WARN("[LT hybrid DAS] read kv_key for FROM_KEY_CID failed",
                           K(ret), K(row_idx));
                } else {
                  ObString cid_bytes;
                  if (entry.key_suffix_ordinal_ < 0) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("[LT hybrid DAS] FROM_KEY_CID without ordinal", K(ret), K(entry));
                  } else if (OB_FAIL(share::ObLogicalTableDecoder::locate_key_cid_bytes_at_ordinal(
                                  key_bin, aux_type, entry.key_suffix_ordinal_, cid_bytes))) {
                    LOG_WARN("[LT hybrid DAS] locate_key_cid_bytes_at_ordinal failed",
                             K(ret), K(row_idx), K(aux_type),
                             "ordinal", entry.key_suffix_ordinal_);
                  } else {
                    if (OB_FAIL(ObLogicalTableHelper::set_logical_output_string(
                            *out_expr, row_idx, eval_ctx, cid_bytes))) {
                      LOG_WARN("copy logical cid output failed", K(ret), K(row_idx), K(i));
                    }
                  }
                }
              }
            } else if (FROM_KEY_SUFFIX == entry.source_) {
              // FROM_KEY_SUFFIX: decode varchar column from kv_key at given ordinal.
              if (OB_ISNULL(lt_ctdef.kv_key_expr_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("kv_key_expr is null for FROM_KEY_SUFFIX", K(ret));
              } else {
                bool k_is_null = false;
                ObString key_bin;
                if (OB_FAIL(read_kv_string_at_row(*lt_ctdef.kv_key_expr_, eval_ctx,
                                                  row_idx, json_scratch_alloc, k_is_null, key_bin))) {
                  LOG_WARN("[LT hybrid DAS] read kv_key for FROM_KEY_SUFFIX failed",
                           K(ret), K(row_idx));
                } else if (entry.obj_meta_.is_varchar_or_char()
                           || entry.obj_meta_.is_varbinary()
                           || entry.obj_meta_.get_type() == ObVarcharType) {
                  ObString tok;
                  if (OB_FAIL(share::ObLogicalTableDecoder::decode_key_varchar_at_ordinal(
                          key_bin,
                          aux_type,
                          entry.key_suffix_ordinal_,
                          json_scratch_alloc, tok))) {
                    LOG_WARN("decode_key_varchar_at_ordinal failed", K(ret), K(entry));
                  } else {
                    if (OB_FAIL(ObLogicalTableHelper::set_logical_output_string(
                            *out_expr, row_idx, eval_ctx, tok))) {
                      LOG_WARN("copy logical key suffix output failed", K(ret), K(row_idx), K(i));
                    }
                  }
                } else {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("FROM_KEY_SUFFIX with non-varchar type not supported yet",
                           K(ret), K(entry));
                }
              }
            } else if (FROM_VALUE_PATH == entry.source_) {
              ObString path_str = entry.json_path_;
              ObJsonPath *j_path = nullptr;
              ObJsonSeekResult hits;
              // M5 debug: print kv_value first few bytes to see JSON structure
              if (!kv_value_bin.empty()) {
                LOG_TRACE("[LT hybrid DAS] M5 kv_value peek",
                         K(row_idx), K(i), K(path_str),
                         "val_len", kv_value_bin.length(),
                         "val_hex8", static_cast<int64_t>(kv_value_bin.length()) >= 8
                             ? *(reinterpret_cast<const int64_t *>(kv_value_bin.ptr())) : 0);
              }
              if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(
                  alloc, &path_cache, j_path, path_str, static_cast<int>(i), true, true))) {
                LOG_WARN("failed to parse json path", K(ret), K(path_str));
              } else if (OB_ISNULL(j_path)) {
                LOG_TRACE("[LT hybrid DAS] M5 FROM_VALUE_PATH j_path is null, set_null",
                         K(row_idx), K(i), K(path_str));
                ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
              } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, false, hits))) {
                LOG_WARN("json seek failed", K(ret), K(path_str));
              } else if (0 == hits.size()) {
                LOG_TRACE("[LT hybrid DAS] M5 FROM_VALUE_PATH seek 0 hits, set_null",
                         K(row_idx), K(i), K(path_str));
                ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
              } else if (entry.is_vector_col_
                         || entry.obj_meta_.get_type() == ObCollectionSQLType
                         || (OB_NOT_NULL(out_expr)
                             && out_expr->obj_meta_.get_type() == ObCollectionSQLType)) {
                // Symmetric to ob_logical_table_encoder.cpp ObCollectionSQLType:
                // encoder strips LOB header then stores raw float[dim] bytes in ObJsonString;
                // decoder wraps raw bytes once via string_to_templob_result (single LOB header).
                ObIJsonBase *hit_node = hits[0];
                const common::ObJsonNodeType jtype = hit_node->json_type();
                if (common::ObJsonNodeType::J_NULL == jtype) {
                  ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
                } else if (common::ObJsonNodeType::J_STRING == jtype) {
                  const ObString vec_str(static_cast<int32_t>(hit_node->get_data_length()),
                                         const_cast<char *>(hit_node->get_data()));
                  const int64_t dim_hint = lt_ctdef.ivf_vec_project_dim_ > 0
                      ? lt_ctdef.ivf_vec_project_dim_ : 0;
                  if (OB_FAIL(ObLogicalTableHelper::validate_json_vec_raw_bytes(vec_str, dim_hint))) {
                    LOG_WARN("[LT hybrid DAS] vector column decode failed",
                             K(ret), K(row_idx), K(i), K(vec_str.length()), K(dim_hint));
                  } else if (out_expr->is_batch_result()
                             && out_expr->enable_rich_format()) {
                    if (OB_FAIL(sql::ObTextStringHelper::string_to_templob_result(
                            *out_expr, eval_ctx, vec_str, row_idx))) {
                      LOG_WARN("[LT hybrid DAS] wrap decoded vec raw bytes as templob to vector failed",
                               K(ret), K(row_idx), K(i));
                    } else {
                      out_expr->set_evaluated_flag(eval_ctx);
                    }
                  } else {
                    ObDatum &out_datum = out_expr->locate_expr_datum(eval_ctx, row_idx);
                    if (OB_FAIL(sql::ObTextStringHelper::string_to_templob_result(
                            *out_expr, eval_ctx, out_datum, vec_str))) {
                      LOG_WARN("[LT hybrid DAS] wrap decoded vec raw bytes as templob failed",
                               K(ret), K(row_idx), K(i));
                    }
                  }
                } else {
                  ret = OB_INVALID_DATA;
                  LOG_WARN("[LT hybrid DAS] unexpected json type for vector column, "
                             "encoder expects JsonString raw float[]",
                           K(ret), K(row_idx), K(i), "jtype", static_cast<int>(jtype));
                }
              } else {
                // Unified jtype decode aligned with ob_logical_table_encoder.cpp:
                //   J_INT/J_UINT/J_DOUBLE/J_STRING + get_raw_binary fallback for J_OBJECT etc.
                ObIJsonBase *hit_node = hits[0];
                const common::ObJsonNodeType jtype = hit_node->json_type();
                if (common::ObJsonNodeType::J_NULL == jtype) {
                  ObLogicalTableHelper::set_logical_output_null(*out_expr, row_idx, eval_ctx);
                } else if (common::ObJsonNodeType::J_INT == jtype) {
                  ObLogicalTableHelper::set_logical_output_int(*out_expr, row_idx, eval_ctx, hit_node->get_int());
                } else if (common::ObJsonNodeType::J_UINT == jtype) {
                  ObLogicalTableHelper::set_logical_output_uint(*out_expr, row_idx, eval_ctx, hit_node->get_uint());
                } else if (common::ObJsonNodeType::J_DOUBLE == jtype) {
                  if (OB_NOT_NULL(out_expr)
                      && ObFloatType == out_expr->obj_meta_.get_type()) {
                    ObLogicalTableHelper::set_logical_output_float(*out_expr, row_idx, eval_ctx,
                        static_cast<float>(hit_node->get_double()));
                  } else {
                    ObLogicalTableHelper::set_logical_output_double(*out_expr, row_idx, eval_ctx, hit_node->get_double());
                  }
                } else if (common::ObJsonNodeType::J_STRING == jtype) {
                  const char *src = hit_node->get_data();
                  const uint64_t slen = hit_node->get_data_length();
                  ObString str_val(static_cast<int32_t>(slen), src);
                  if (OB_NOT_NULL(out_expr) && out_expr->obj_meta_.is_lob_storage()) {
                    if (out_expr->is_batch_result()
                        && out_expr->enable_rich_format()) {
                      if (OB_FAIL(sql::ObTextStringHelper::string_to_templob_result(
                              *out_expr, eval_ctx, str_val, row_idx))) {
                        LOG_WARN("[LT hybrid DAS] wrap json string as templob to vector failed",
                                 K(ret), K(row_idx), K(i));
                      } else {
                        out_expr->set_evaluated_flag(eval_ctx);
                      }
                    } else {
                      ObDatum &out_datum = out_expr->locate_expr_datum(eval_ctx, row_idx);
                      if (OB_FAIL(sql::ObTextStringHelper::string_to_templob_result(
                              *out_expr, eval_ctx, out_datum, str_val))) {
                        LOG_WARN("[LT hybrid DAS] wrap json string as templob failed",
                                 K(ret), K(row_idx), K(i));
                      }
                    }
                  } else {
                    if (OB_FAIL(ObLogicalTableHelper::set_logical_output_string(
                            *out_expr, row_idx, eval_ctx, str_val))) {
                      LOG_WARN("[LT hybrid DAS] copy json string output failed",
                               K(ret), K(row_idx), K(i), K(slen));
                    }
                  }
                } else {
                  ObString raw_bin;
                  if (OB_FAIL(hit_node->get_raw_binary(raw_bin, &json_scratch_alloc))) {
                    LOG_WARN("[LT hybrid DAS] failed to get json raw binary for logical column",
                             K(ret), K(i), K(jtype));
                  } else if (OB_NOT_NULL(out_expr) && out_expr->obj_meta_.is_lob_storage()) {
                    if (out_expr->is_batch_result()
                        && out_expr->enable_rich_format()) {
                      if (OB_FAIL(sql::ObTextStringHelper::string_to_templob_result(
                              *out_expr, eval_ctx, raw_bin, row_idx))) {
                        LOG_WARN("[LT hybrid DAS] wrap raw json binary as templob to vector failed",
                                 K(ret), K(row_idx), K(i));
                      } else {
                        out_expr->set_evaluated_flag(eval_ctx);
                      }
                    } else {
                      ObDatum &out_datum = out_expr->locate_expr_datum(eval_ctx, row_idx);
                      if (OB_FAIL(sql::ObTextStringHelper::string_to_templob_result(
                              *out_expr, eval_ctx, out_datum, raw_bin))) {
                        LOG_WARN("[LT hybrid DAS] wrap raw json binary as templob failed",
                                 K(ret), K(row_idx), K(i));
                      }
                    }
                  } else {
                    if (OB_FAIL(ObLogicalTableHelper::set_logical_output_string(
                            *out_expr, row_idx, eval_ctx, raw_bin))) {
                      LOG_WARN("[LT hybrid DAS] copy raw json output failed",
                               K(ret), K(row_idx), K(i), K(raw_bin.length()));
                    }
                  }
                }
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("unsupported logical column source in project_logical_columns", K(ret),
                  K(entry.source_), K(entry.logical_column_id_));
            }
          }
        }
      }
      // Reclaim the per-row JSON seek clones; reuse() keeps pages for the next
      // row to avoid malloc churn while zeroing logical occupancy.
      json_scratch_alloc.reuse();
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("[LT hybrid DAS] project_logical_columns exit ok",
             K(row_count),
             K(mapping.count()));
  }
  return ret;
}

int ObLogicalTableHelper::rewrite_hybrid_logical_table_scan_param_key_ranges_(
    storage::ObTableScanParam &scan_param,
    uint64_t physical_data_table_id,
    share::ObLogicalTableAuxType aux_type,
    uint64_t index_seq,
    uint64_t ref_table_id,
    const ObPushdownExprSpec &pd_expr_spec,
    ObEvalCtx &eval_ctx,
    common::ObIAllocator &alloc,
    ObLogicalTableRewriteCtx &rewrite_ctx)
{
  int ret = OB_SUCCESS;
  // rewrite_ctx is expected to be pre-populated by the caller with expr-evaluated
  // namespace_id / logical_table_id from lt_rt.
  const uint64_t runtime_ns_id = rewrite_ctx.namespace_id_;
  const uint64_t runtime_lt_id = rewrite_ctx.logical_table_id_;
  // physical_data_table_id is a parameter; no reset() needed on POD.
  share::ObLogicalTableKeyPrefix prefix;
  common::ObSEArray<common::ObNewRange, 4> stripped_ranges;
  common::ObSEArray<common::ObNewRange, 4> physical_ranges;

  uint64_t ns_id = 0;
  uint64_t lt_id = common::OB_INVALID_ID;
  if (share::OB_INVALID_NAMESPACE_ID == runtime_ns_id
      || share::OB_INVALID_LOGICAL_TABLE_ID == runtime_lt_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "logical table query must carry namespace_id and ltable_id predicates");
    LOG_WARN("no runtime ns/lt available for hybrid logical table range rewrite",
             K(ret), K(ref_table_id), "aux_type", static_cast<int>(aux_type),
             K(runtime_ns_id), K(runtime_lt_id));
  } else {
    ns_id = runtime_ns_id;
    lt_id = runtime_lt_id;
  }
  if (OB_SUCC(ret)) {
    if (0 == ns_id || common::OB_INVALID_ID == lt_id || 0 == lt_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid logical table namespace_id or ltable_id");
    } else {
      prefix.namespace_id_ = ns_id;
      prefix.logical_table_id_ = lt_id;
      prefix.aux_type_ = aux_type;
      prefix.index_seq_ = index_seq;
      LOG_TRACE("[LT hybrid DAS] rewrite_table_scan_param_key_ranges hybrid prefix",
               K(prefix.namespace_id_), K(prefix.logical_table_id_),
               "aux_type", static_cast<int>(prefix.aux_type_),
               K(prefix.index_seq_), "range_cnt", scan_param.key_ranges_.count());
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("[LT hybrid DAS] rewrite_table_scan_param_key_ranges failed", K(ret));
  } else if (OB_FAIL(stripped_ranges.assign(scan_param.key_ranges_))) {
    LOG_WARN("[LT hybrid DAS] copy logical ranges for session-prefix path failed", K(ret));
  } else if (OB_FAIL(share::ObLogicalTableRangeBuilder::convert_ranges(
                 physical_data_table_id, prefix, stripped_ranges, alloc, physical_ranges))) {
    LOG_WARN("[LT hybrid DAS] convert logical ranges to physical failed", K(ret), K(prefix));
  } else if (physical_ranges.empty()) {
    // no-op
  } else {
    scan_param.key_ranges_.reuse();
    if (OB_FAIL(scan_param.key_ranges_.assign(physical_ranges))) {
      LOG_WARN("[LT hybrid DAS] assign physical ranges failed", K(ret));
    } else {
      rewrite_ctx.namespace_id_ = prefix.namespace_id_;
      rewrite_ctx.logical_table_id_ = prefix.logical_table_id_;
      if (pd_expr_spec.access_exprs_.count() >= 2) {
        rewrite_ctx.kv_key_expr_ = pd_expr_spec.access_exprs_.at(0);
        rewrite_ctx.kv_value_expr_ = pd_expr_spec.access_exprs_.at(1);
      }
      rewrite_ctx.is_enabled_ = true;
      ObExecContext &exec_ctx = eval_ctx.exec_ctx_;
      // On the remote DAS executor the shadow session may be null (session fallback is
      // forbidden there); guard before dereferencing to avoid SIGSEGV.
      ObSQLSessionInfo *session = exec_ctx.get_my_session();
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      const common::ObTabletID logical_anchor_tablet = scan_param.tablet_id_;
      common::ObTabletID kv_tablet_id;
      ObObjectID kv_object_id = OB_INVALID_OBJECT_ID;
      // PRIMARY and IVF logical aux scans carry a logical-tablet anchor; map it to the KV
      // physical tablet. FTS inverted/forward paths pre-set scan_param.tablet_id_ to the
      // KV tablet via search_ctx.physical_tablet_id_ and must not re-resolve here.
      const bool need_resolve_kv_tablet =
          (share::LT_AUX_PRIMARY == aux_type)
          || (share::LT_AUX_IVF_CID_VECTOR == aux_type)
          || (share::LT_AUX_IVF_CID_ROWID == aux_type)
          || (share::LT_AUX_IVF_ROWID_CID == aux_type);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT hybrid DAS] session is null for hybrid logical table range rewrite",
                 K(ret), K(ref_table_id), "aux_type", static_cast<int>(aux_type));
      } else if (FALSE_IT(tenant_id = session->get_effective_tenant_id())) {
      } else if (need_resolve_kv_tablet) {
        if (OB_FAIL(ObLogicalTableHelper::resolve_kv_tablet_via_logical_anchor(
                tenant_id, ref_table_id, physical_data_table_id,
                logical_anchor_tablet, kv_tablet_id, kv_object_id))) {
          LOG_WARN("[LT hybrid DAS] resolve kv tablet via logical tablet failed", K(ret),
                   K(physical_data_table_id), K(logical_anchor_tablet), K(ref_table_id),
                   "aux_type", static_cast<int>(aux_type));
        } else {
          scan_param.tablet_id_ = kv_tablet_id;
          LOG_TRACE("[LT hybrid DAS] rewrite_table_scan_param_key_ranges tablet redirect ok", K(kv_tablet_id),
                   K(kv_object_id), K(physical_data_table_id), K(aux_type),
                   "key_range_cnt", scan_param.key_ranges_.count());
        }
      } else if (!scan_param.tablet_id_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT hybrid DAS] non-primary logical aux rewrite expects pre-set kv tablet",
                 K(ret), K(aux_type), K(ref_table_id), K(physical_data_table_id));
      } else {
        LOG_TRACE("[LT hybrid DAS] rewrite_table_scan_param_key_ranges non-primary skip resolve",
                 K(aux_type), K(ref_table_id), K(scan_param.tablet_id_),
                 K(physical_data_table_id),
                 "key_range_cnt", scan_param.key_ranges_.count());
      }
    }
  }
  return ret;
}

int ObLogicalTableHelper::rewrite_table_scan_param_key_ranges(storage::ObTableScanParam &scan_param,
                                                              const ObDASScanCtDef &scan_ctdef, ObEvalCtx &eval_ctx,
                                                              common::ObIAllocator &alloc,
                                                              ObLogicalTableRewriteCtx &rewrite_ctx)
{
  int ret = OB_SUCCESS;
  if (!scan_ctdef.is_logical_table() || scan_param.key_ranges_.empty()) {
    // POD: no reset() needed; callers re-fill input fields before each call.
  } else {
    const uint64_t index_seq = (common::OB_INVALID_ID != scan_ctdef.logical_index_seq_)
        ? scan_ctdef.logical_index_seq_
        : scan_ctdef.ref_table_id_;
    ret = rewrite_hybrid_logical_table_scan_param_key_ranges_(
        scan_param, scan_ctdef.physical_data_table_id_,
        scan_ctdef.logical_table_aux_type_, index_seq, scan_ctdef.ref_table_id_,
        scan_ctdef.pd_expr_spec_, eval_ctx, alloc, rewrite_ctx);
  }
  return ret;
}

int ObLogicalTableHelper::rewrite_table_scan_param_key_ranges(
    storage::ObTableScanParam &scan_param,
    const ObDASScalarScanCtDef &scalar_ctdef,
    ObEvalCtx &eval_ctx,
    common::ObIAllocator &alloc,
    ObLogicalTableRewriteCtx &rewrite_ctx)
{
  int ret = OB_SUCCESS;
  if (!scalar_ctdef.is_logical_table() || scan_param.key_ranges_.empty()) {
    // POD: no reset() needed; callers re-fill input fields before each call.
  } else {
    const uint64_t index_seq = (common::OB_INVALID_ID != scalar_ctdef.logical_index_seq_)
        ? scalar_ctdef.logical_index_seq_
        : scalar_ctdef.ref_table_id_;
    ret = rewrite_hybrid_logical_table_scan_param_key_ranges_(
        scan_param, scalar_ctdef.physical_data_table_id_,
        scalar_ctdef.logical_table_aux_type_, index_seq, scalar_ctdef.ref_table_id_,
        scalar_ctdef.pd_expr_spec_, eval_ctx, alloc, rewrite_ctx);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
