/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/iter/ob_das_vec_scan_utils.h"

namespace oceanbase
{
namespace sql
{
int ObDasVecScanUtils::set_lookup_key(ObRowkey &rowkey, ObTableScanParam &scan_param, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObNewRange look_range;
  if (OB_FAIL(look_range.build_range(table_id, rowkey))) {
    LOG_WARN("build lookup range failed", K(ret));
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(look_range))) {
    LOG_WARN("store lookup key range failed", K(ret));
  }
  return ret;
}

int ObDasVecScanUtils::set_lookup_range(const ObNewRange &look_range, ObTableScanParam &scan_param, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(scan_param.key_ranges_.push_back(look_range))) {
    LOG_WARN("store lookup key range failed", K(ret));
  }
  return ret;
}

void ObDasVecScanUtils::release_scan_param(ObTableScanParam &scan_param)
{
  scan_param.destroy_schema_guard();
  scan_param.snapshot_.reset();
  scan_param.destroy();
}

void ObDasVecScanUtils::set_whole_range(ObNewRange &scan_range, common::ObTableID table_id)
{
  scan_range.table_id_ = table_id;
  scan_range.set_whole_range();
}

int ObDasVecScanUtils::get_distance_expr_type(ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObExprVectorDistance::ObVecDisType &dis_type)
{
  int ret = OB_SUCCESS;

  switch (expr.type_) {
    case T_FUN_SYS_L2_DISTANCE: 
      dis_type = ObExprVectorDistance::ObVecDisType::EUCLIDEAN;
      break;
    case T_FUN_SYS_L2_SQUARED:
      dis_type = ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED;
      break;
    case T_FUN_SYS_INNER_PRODUCT: 
      dis_type = ObExprVectorDistance::ObVecDisType::DOT;
      break;
    case T_FUN_SYS_NEGATIVE_INNER_PRODUCT: 
      dis_type = ObExprVectorDistance::ObVecDisType::DOT;
      break;
    case T_FUN_SYS_COSINE_DISTANCE: 
      dis_type = ObExprVectorDistance::ObVecDisType::COSINE;
      break;
    case T_FUN_SYS_L1_DISTANCE:
      dis_type = ObExprVectorDistance::ObVecDisType::MANHATTAN;
      break;
    default: 
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support vector sort expr", K(ret), K(expr.type_));
      break;
  }

  if (OB_SUCC(ret) && ObExprVectorDistance::DisFunc<float>::distance_funcs[dis_type] == nullptr) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support dis_type", K(ret), K(dis_type));
  }

  return ret;
}

int ObDasVecScanUtils::get_distance_threshold_hnsw(ObVectorIndexDistAlgorithm dist_algorithm,
                                                   float &similarity_threshold, 
                                                   float &distance_threshold)
{
  int ret = OB_SUCCESS;
  switch (dist_algorithm) {
    case ObVectorIndexDistAlgorithm::VIDA_L2:
      // l2_similarity = 1 / (1 + l2_square_distance), vsag l2_distance is l2_square_distance
      distance_threshold = 1 / similarity_threshold - 1;
      break;
    case ObVectorIndexDistAlgorithm::VIDA_COS:
      // cosine_similarity = (1 + cosine) / 2, vsag cosine_distance = 1 - cosine
      distance_threshold = 2 - 2 * similarity_threshold;
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support similarity for vector index distance algorithm", K(ret), K(dist_algorithm));
      break;
  }
  return ret;
}

int ObDasVecScanUtils::check_IP_distance(const share::ObVecIdxQueryResult &dist_result,
                                         const int64_t vid,
                                         const common::ObString &query_vector,
                                         double &distance,
                                         int64_t &segment_idx,
                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  DEBUG_SYNC(BEFORE_IP_BRUTE_FORCE_CHECK_ID);
  for (int64_t i = 0; OB_SUCC(ret) && i < dist_result.segments_.count(); ++i) {
    bool exists = false;
    share::ObVectorIndexSegment *segment =
        const_cast<share::ObVectorIndexSegment *>(dist_result.segments_.at(i).get());
    if (OB_ISNULL(segment)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null vector index segment", K(ret), K(vid), K(i));
    } else if (!segment->is_base() && segment->get_index_type() != share::VIAT_HNSW) {
    } else if (OB_FAIL(segment->check_id_exist(vid, exists))) {
      LOG_WARN("failed to check vector id for ambiguous IP distance", K(ret), K(vid), K(i));
    } else if (exists) {
      const float *recomputed_distances = nullptr;
      if (OB_ISNULL(query_vector.ptr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("null query vector for IP distance recomputation", K(ret), K(vid), K(i));
      } else if (OB_FAIL(segment->cal_distance_by_id(
          reinterpret_cast<const float *>(query_vector.ptr()), &vid, 1, recomputed_distances))) {
        LOG_WARN("failed to recompute ambiguous IP distance", K(ret), K(vid), K(i));
      } else if (OB_ISNULL(recomputed_distances)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null recomputed IP distances", K(ret), K(vid), K(i));
      } else {
        distance = recomputed_distances[0];
        segment_idx = i;
        is_valid = true;
      }
      if (OB_NOT_NULL(recomputed_distances)) {
        segment->mem_ctx()->Deallocate(const_cast<float *>(recomputed_distances));
      }
      break;
    }
  }
  return ret;
}

int ObDasVecScanUtils::check_ivf_support_similarity_threshold(ObExpr &expr)
{
  int ret = OB_SUCCESS;
  
  switch (expr.type_) {
    case T_FUN_SYS_L2_DISTANCE: 
      break;
    // currently we don't support ip similarity
    // case T_FUN_SYS_INNER_PRODUCT:
    // case T_FUN_SYS_NEGATIVE_INNER_PRODUCT: 
    case T_FUN_SYS_COSINE_DISTANCE:
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support vector sort expr", K(ret), K(expr.type_));
      break;
  }
  return ret;
}

int ObDasVecScanUtils::get_real_search_vec(common::ObIAllocator &allocator,
                                           ObEvalCtx *eval_ctx,
                                           ObExpr *origin_vec,
                                           ObString &real_search_vec)
{
  int ret = OB_SUCCESS;

  ObDatum *search_vec_datum = NULL;
  if (OB_ISNULL(eval_ctx) || OB_ISNULL(origin_vec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), K(eval_ctx), K(origin_vec));
  } else if (OB_FAIL(origin_vec->eval(*eval_ctx, search_vec_datum))) {
    LOG_WARN("eval vec arg failed", K(ret));
  } else if (search_vec_datum->is_null()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("search vector is null", K(ret));
  } else if (OB_FALSE_IT(real_search_vec = search_vec_datum->get_string())) {
  } else if (0 == real_search_vec.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("search vector is empty string", K(ret));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator,
                                                             origin_vec->datum_meta_.type_,
                                                             origin_vec->datum_meta_.cs_type_,
                                                             real_search_vec))){
    LOG_WARN("failed to get real data.", K(ret), K(origin_vec->datum_meta_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(real_search_vec.ptr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null pointer", K(ret), KP(real_search_vec.ptr()));
  }

  return ret;
}

int ObDasVecScanUtils::init_limit(const ObDASVecAuxScanCtDef *ir_ctdef,
                                  ObDASVecAuxScanRtDef *ir_rtdef,
                                  const ObDASSortCtDef *sort_ctdef,
                                  ObDASSortRtDef *sort_rtdef,
                                  common::ObLimitParam &limit_param)
{
  int ret = OB_SUCCESS;
  ObDASScanRtDef *base_rtdef = nullptr;
  if (OB_ISNULL(ir_ctdef) || OB_ISNULL(ir_rtdef) || OB_ISNULL(sort_ctdef) || OB_ISNULL(sort_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", K(ret), K(ir_ctdef), K(ir_rtdef), K(sort_ctdef), K(sort_rtdef));
  } else if (ObDASOpType::DAS_OP_TABLE_SCAN == ir_rtdef->get_inv_idx_scan_rtdef()->op_type_) {
    base_rtdef = static_cast<ObDASScanRtDef *>(ir_rtdef->get_inv_idx_scan_rtdef());
  } else if (ObDASOpType::DAS_OP_SORT == ir_rtdef->get_inv_idx_scan_rtdef()->op_type_) {
    ObDASSortRtDef *sort_rtdef = static_cast<ObDASSortRtDef *>(ir_rtdef->get_inv_idx_scan_rtdef());
    if (ObDASOpType::DAS_OP_TABLE_SCAN == sort_rtdef->children_[0]->op_type_) {
      base_rtdef = static_cast<ObDASScanRtDef *>(sort_rtdef->children_[0]);
    } else if (sort_rtdef->children_[0]->children_cnt_ > 0
      && ObDASOpType::DAS_OP_TABLE_SCAN == sort_rtdef->children_[0]->children_[0]->op_type_) {
      base_rtdef = static_cast<ObDASScanRtDef *>(sort_rtdef->children_[0]->children_[0]);
    }
  } else if (ObDASOpType::DAS_OP_INDEX_MERGE == ir_rtdef->get_inv_idx_scan_rtdef()->op_type_ && OB_ISNULL(sort_ctdef->limit_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null base limit expr", K(ret));
  }

  if (OB_ISNULL(base_rtdef) && (OB_ISNULL(sort_ctdef->limit_expr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null base rtdef", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
  } else if (OB_NOT_NULL(sort_ctdef->limit_expr_)) {
    // try init top-k limits
    bool is_null = false;
    if (OB_UNLIKELY((nullptr == sort_ctdef->limit_expr_ && nullptr == sort_ctdef->offset_expr_)
                    && (OB_ISNULL(base_rtdef) || base_rtdef->limit_param_.is_valid()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected top k limit with table scan limit pushdown", K(ret), KPC(ir_ctdef), KPC(ir_rtdef));
    } else if (nullptr != sort_ctdef->limit_expr_) {
      ObDatum *limit_datum = nullptr;
      if (OB_FAIL(sort_ctdef->limit_expr_->eval(*sort_rtdef->eval_ctx_, limit_datum))) {
        LOG_WARN("failed to eval limit expr", K(ret));
      } else if (limit_datum->is_null()) {
        is_null = true;
        limit_param.limit_ = 0;
      } else {
        limit_param.limit_ = limit_datum->get_int() < 0 ? 0 : limit_datum->get_int();
      }
    }

    if (OB_SUCC(ret) && !is_null && nullptr != sort_ctdef->offset_expr_) {
      ObDatum *offset_datum = nullptr;
      if (OB_FAIL(sort_ctdef->offset_expr_->eval(*sort_rtdef->eval_ctx_, offset_datum))) {
        LOG_WARN("failed to eval offset expr", K(ret));
      } else if (offset_datum->is_null()) {
        limit_param.offset_ = 0;
      } else {
        limit_param.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
      }
    }
  } else {
    // init with table scan pushdown limit
    limit_param = base_rtdef->limit_param_;
  }

  if (OB_SUCC(ret) && false ) {
    const ObDASBaseRtDef *index_rtdef = ir_rtdef->get_inv_idx_scan_rtdef();
    int64_t limit = limit_param.limit_;

    if (limit > 0 && OB_NOT_NULL(index_rtdef->table_loc_)) {
      ObDASTableLoc *table_loc = index_rtdef->table_loc_;
      uint64_t partiton = table_loc->get_tablet_locs().size();
      if (partiton > 1) {
        limit_param.limit_ = 2 * ((limit / partiton) + 1 );
      }
    }
  }

  return ret;
}

int ObDasVecScanUtils::init_sort_of_hybrid_index(ObIAllocator &allocator,
                                                 const ObDASVecAuxScanCtDef *ir_ctdef,
                                                 const ObDASSortCtDef *sort_ctdef,
                                                 ObDASSortRtDef *sort_rtdef,
                                                 ObString &hybrid_search_vec,
                                                 ObExpr *&distance_calc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_ctdef) || OB_ISNULL(sort_rtdef) || OB_ISNULL(ir_ctdef) || OB_ISNULL(sort_rtdef->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sort def", K(ret), KPC(sort_rtdef), KPC(sort_ctdef), KPC(ir_ctdef));
  } else {
    for (int i = 0; i < sort_ctdef->sort_exprs_.count() && OB_SUCC(ret) && hybrid_search_vec.empty(); ++i) {
      ObExpr *expr = sort_ctdef->sort_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr ptr", K(ret));
      } else if (expr->is_semantic_distance_expr()) {
        distance_calc = expr;
        ObString query_str;
        ObExpr *query_str_expr = nullptr;
        const bool arg_cnt_invalid = expr->is_semantic_vector_distance_expr()
                                     ? (expr->arg_cnt_ != 2)
                                     : (expr->arg_cnt_ != 2 && expr->arg_cnt_ != 3);
        if (arg_cnt_invalid) {
          ret = OB_ERR_PARAM_SIZE;
          LOG_WARN("unexpected arg num", K(ret), K(expr->arg_cnt_));
        } else if (!expr->args_[ARGS_IDX_ZERO]->is_const_expr() && !expr->args_[ARGS_IDX_ONE]->is_const_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("semantic_distance expr args are not string type", K(ret), KPC(expr->args_[ARGS_IDX_ZERO]), KPC(expr->args_[ARGS_IDX_ONE]));
        } else if (FALSE_IT(query_str_expr = expr->args_[ARGS_IDX_ZERO]->is_const_expr() ? expr->args_[ARGS_IDX_ZERO] : expr->args_[ARGS_IDX_ONE])) {
        } else if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(allocator, sort_rtdef->eval_ctx_, query_str_expr, query_str))) {
          LOG_WARN("failed to get real string from expr", K(ret));
        } else {
          if (expr->is_semantic_vector_distance_expr()) {
            if (OB_FAIL(ObVectorIndexUtil::get_vector_from_vector_array_string(allocator, query_str, ir_ctdef->vec_index_param_, hybrid_search_vec))) {
              LOG_WARN("failed to get vector from query text", K(ret), KPC(expr));
            }
          } else {
            ObVectorIndexContentType input_type = VICT_TEXT;
            ObString input_type_str;
            if (expr->arg_cnt_ == 3) {
              if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(allocator, sort_rtdef->eval_ctx_, expr->args_[2], input_type_str))) {
                LOG_WARN("failed to get semantic input type string", K(ret));
              } else if (OB_FAIL(ObVectorIndexUtil::parse_content_type_str(input_type_str, input_type))) {
                LOG_WARN("failed to parse semantic input type", K(ret), K(input_type_str));
              }
            }

            if (OB_SUCC(ret)
                && OB_FAIL(ObVectorIndexUtil::get_vector_from_content_by_embedding(allocator, query_str, input_type, ir_ctdef->vec_index_param_, hybrid_search_vec))) {
              LOG_WARN("failed to get vector from content by embedding", K(ret), KPC(expr));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDasVecScanUtils::init_sort(const ObDASVecAuxScanCtDef *ir_ctdef,
                                 ObDASVecAuxScanRtDef *ir_rtdef,
                                 const ObDASSortCtDef *sort_ctdef,
                                 ObDASSortRtDef *sort_rtdef,
                                 const common::ObLimitParam &limit_param,
                                 ObExpr *&search_vec,
                                 ObExpr *&distance_calc)
{
  int ret = OB_SUCCESS;
  const int64_t top_k_cnt = limit_param.is_valid() ? (limit_param.limit_ + limit_param.offset_) : INT64_MAX;
  if (OB_ISNULL(sort_ctdef) || OB_ISNULL(sort_rtdef) || OB_ISNULL(ir_ctdef) || OB_ISNULL(sort_rtdef->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sort def", K(ret), KPC(sort_rtdef), KPC(sort_ctdef), KPC(ir_ctdef));
  } else {
    for (int i = 0; i < sort_ctdef->sort_exprs_.count() && OB_SUCC(ret) && OB_ISNULL(search_vec); ++i) {
      ObExpr *expr = sort_ctdef->sort_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr ptr", K(ret));
      } else if (expr->is_vector_sort_expr()) {
        distance_calc = expr;
        if (expr->arg_cnt_ != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected arg num", K(ret), K(expr->arg_cnt_));
        } else if (expr->args_[0]->is_const_expr()) {
          search_vec = expr->args_[0];
        } else if (expr->args_[1]->is_const_expr()) {
          search_vec = expr->args_[1];
        }
      }
    }
  }
  return ret;
}

int ObDasVecScanUtils::reuse_iter(const share::ObLSID &ls_id,
                                  ObDASScanIter *iter,
                                  ObTableScanParam &scan_param,
                                  const ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;

  const ObTabletID &scan_tablet_id = scan_param.tablet_id_;
  scan_param.need_switch_param_ =
      scan_param.need_switch_param_ || (scan_tablet_id.is_valid() && (tablet_id != scan_tablet_id));
  scan_param.tablet_id_ = tablet_id;
  scan_param.ls_id_ = ls_id;

  if (OB_NOT_NULL(iter) && OB_FAIL(iter->reuse())) {
    LOG_WARN("reuse iter failed", K(ret));
  }

  return ret;
}

int ObDasVecScanUtils::init_scan_param(const share::ObLSID &ls_id,
                                       const common::ObTabletID &tablet_id,
                                       const ObDASScanCtDef *ctdef,
                                       ObDASScanRtDef *rtdef,
                                       transaction::ObTxDesc *tx_desc,
                                       transaction::ObTxReadSnapshot *snapshot,
                                       ObTableScanParam &scan_param,
                                       bool is_get,
                                       ObIAllocator *scan_allocator /*=nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctdef), K(rtdef));
  } else {
    scan_param.tenant_id_ = MTL_ID();
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = is_get;
    scan_param.is_for_foreign_check_ = rtdef->is_for_foreign_check_;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = scan_allocator == nullptr ? &rtdef->scan_allocator_ : scan_allocator;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.ext_file_column_exprs_ = &(ctdef->pd_expr_spec_.ext_file_column_exprs_);
    scan_param.ext_mapping_column_exprs_ = &(ctdef->pd_expr_spec_.ext_mapping_column_exprs_);
    scan_param.ext_mapping_column_ids_ = &(ctdef->pd_expr_spec_.ext_mapping_column_ids_);
    scan_param.ext_column_dependent_exprs_ = &(ctdef->pd_expr_spec_.ext_column_convert_exprs_);
    scan_param.ext_enable_late_materialization_ = ctdef->pd_expr_spec_.ext_enable_late_materialization_;
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    if (rtdef->is_for_foreign_check_) {
      scan_param.trans_desc_ = tx_desc;
    }
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    if (rtdef->sample_info_ != nullptr) {
      scan_param.sample_info_ = *rtdef->sample_info_;
    }
    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("snapshot is null", K(ret));
    }
    if (OB_NOT_NULL(tx_desc)) {
      scan_param.tx_id_ = tx_desc->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("init column ids failed", K(ret));
    }
    // external table scan params
    if (OB_SUCC(ret) && ctdef->is_external_table_) {
      scan_param.external_file_access_info_ = ctdef->external_file_access_info_.str_;
      scan_param.external_file_location_ = ctdef->external_file_location_.str_;
      if (OB_FAIL(scan_param.external_file_format_.load_from_string(ctdef->external_file_format_str_.str_,
                                                                    *scan_param.allocator_))) {
        LOG_WARN("fail to load from string", K(ret));
      } else {
        uint64_t max_idx = 0;
        for (int i = 0; i < scan_param.ext_file_column_exprs_->count(); i++) {
          max_idx = std::max(max_idx, scan_param.ext_file_column_exprs_->at(i)->extra_);
        }
        scan_param.external_file_format_.csv_format_.file_column_nums_ = static_cast<int64_t>(max_idx);
        scan_param.ext_tbl_filter_pd_level_ = ctdef->pd_expr_spec_.ext_tbl_filter_pd_level_;
      }
    }
  }
  return ret;
}

int ObDasVecScanUtils::init_vec_aux_scan_param(const share::ObLSID &ls_id,
                                               const common::ObTabletID &tablet_id,
                                               const sql::ObDASScanCtDef *ctdef,
                                               sql::ObDASScanRtDef *rtdef,
                                               transaction::ObTxDesc *tx_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               ObTableScanParam &scan_param,
                                               bool is_get,
                                               ObIAllocator *scan_allocator /*=nullptr*/)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(
          ObDasVecScanUtils::init_scan_param(ls_id, tablet_id, ctdef, rtdef, tx_desc, snapshot, scan_param, is_get, scan_allocator))) {
    LOG_WARN("failed to generate init vec aux scan param", K(ret));
  } else {
    scan_param.is_for_foreign_check_ = false;
    scan_param.op_ = nullptr;
    scan_param.output_exprs_ = nullptr;
    scan_param.aggregate_exprs_ = nullptr;
    scan_param.row2exprs_projector_ = nullptr;
  }

  return ret;
}

int ObDasVecScanUtils::get_rowkey(ObIAllocator &allocator, const ObDASScanCtDef * ctdef, ObDASScanRtDef *rtdef, ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;

  int64_t rowkey_cnt = 0;
  ObObj *obj_ptr = nullptr;
  void *buf = nullptr;

  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret), KP(ctdef), KP(rtdef));
  } else if (OB_FALSE_IT(rowkey_cnt = ctdef->rowkey_exprs_.count())) {
  } else if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rowkey cnt", K(ret));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * rowkey_cnt))) { 
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(rowkey_cnt));
  } else if (OB_FALSE_IT(obj_ptr = new (buf) ObObj[rowkey_cnt])) {
  } else if (OB_ISNULL(rowkey = static_cast<ObRowkey *>(allocator.alloc(sizeof(ObRowkey))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      ObObj tmp_obj;
      ObExpr *expr = ctdef->rowkey_exprs_.at(i);
      ObDatum &datum = expr->locate_expr_datum(*rtdef->eval_ctx_);
      if (OB_ISNULL(datum.ptr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get col datum null", K(ret));
      } else if (OB_FAIL(datum.to_obj(tmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("convert datum to obj failed", K(ret));
      } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, obj_ptr[i]))) {
        LOG_WARN("deep copy rowkey value failed", K(ret), K(tmp_obj));
      }
    }

    if (OB_SUCC(ret)) {
      rowkey->assign(obj_ptr, rowkey_cnt);
    }
  }

  return ret;
}

int ObDasVecScanUtils::get_rowkey_pre_filter(
    ObIAllocator &allocator, 
    bool is_vectorized, 
    ObDASIter *iter, 
    const ObDASScanCtDef *ctdef, 
    ObDASScanRtDef *rtdef, 
    ObIArray<ObRowkey *> &rowkeys)
{
  int ret = OB_SUCCESS;

  uint64_t rowkey_count = 0;
  while (OB_SUCC(ret) && rowkey_count < MAX_BRUTE_FORCE_SIZE) {
    iter->clear_evaluated_flag();
    if (!is_vectorized) {
      ObRowkey *rowkey;
      if (OB_FAIL(iter->get_next_row())) {
      } else if (OB_FALSE_IT(rowkey_count++)) {
      } else if (OB_FAIL(get_rowkey(allocator, ctdef, rtdef, rowkey))) {
        LOG_WARN("failed to get rowkey", K(ret));
      } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
        LOG_WARN("failed to push rowkey", K(ret));
      }
    } else {
      int64_t batch_row_count = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;

      int64_t scan_row_cnt = 0;
      if (OB_FAIL(iter->get_next_rows(scan_row_cnt, batch_row_count))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next rowkey", K(ret));
        }
      }

      rowkey_count += scan_row_cnt;
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        LOG_WARN("fail to get next row from inv_idx_scan_iter_", K(ret));
      } else if (scan_row_cnt > 0) {
        ret = OB_SUCCESS;
      }

      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*rtdef->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        for (int i = 0; OB_SUCC(ret) && i < scan_row_cnt; i++) {
          guard.set_batch_idx(i);
          ObRowkey *rowkey;
          if (OB_FAIL(get_rowkey(allocator, ctdef, rtdef, rowkey))) {
            LOG_WARN("failed to add rowkey", K(ret), K(i));
          } else if (OB_FAIL(rowkeys.push_back(rowkey))) {
            LOG_WARN("store push rowkey", K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_ITER_END != ret) {
  } else if (OB_ITER_END == ret && rowkey_count > 0) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int64_t ObDasVecScanUtils::calc_heap_size(int64_t limit_k, double select_ratio)
{
  // Heap-size policy versus selectivity:
  //   - high selectivity (>= 0.5): 2x limit_k is plenty (matches ceil(limit_k/0.5)).
  //   - low selectivity (<= 0.1): cap at 10x limit_k to avoid blowing up the
  //     heap when select_ratio is very small (otherwise ceil(limit_k/ratio)
  //     would explode).
  //   - middle range: heap_size = ceil(limit_k / select_ratio).
  static constexpr double HEAP_SIZE_HIGH_RATIO_THRESHOLD = 0.5;
  static constexpr double HEAP_SIZE_LOW_RATIO_THRESHOLD = 0.1;
  static constexpr int64_t HEAP_SIZE_HIGH_RATIO_MULTIPLIER = 2;
  static constexpr int64_t HEAP_SIZE_LOW_RATIO_MULTIPLIER = 10;

  int64_t heap_size = limit_k;
  if (select_ratio > 0.0 && select_ratio < 1.0) {
    if (select_ratio >= HEAP_SIZE_HIGH_RATIO_THRESHOLD) {
      heap_size = HEAP_SIZE_HIGH_RATIO_MULTIPLIER * limit_k;
    } else if (select_ratio <= HEAP_SIZE_LOW_RATIO_THRESHOLD) {
      heap_size = HEAP_SIZE_LOW_RATIO_MULTIPLIER * limit_k;
    } else {
      heap_size = std::ceil(limit_k / select_ratio);
    }
  }
  return heap_size;
}

// NOTICE: shadow copy from expr memory
int ObDasVecScanUtils::get_main_rowkey(const ObDASScanCtDef *ctdef, ObEvalCtx *eval_ctx, ObRowkey &main_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or eval_ctx is null", K(ret), KP(ctdef), KP(eval_ctx));
  } else {
    const int64_t rowkey_cnt = main_rowkey.get_obj_cnt();
    const ExprFixedArray &rk_exprs = ctdef->rowkey_exprs_;
    if (rk_exprs.count() != rowkey_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey expr count mismatch", K(ret), K(rowkey_cnt), K(rk_exprs.count()));
    } else {
      ObObj *obj_ptr = main_rowkey.get_obj_ptr();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
        ObExpr *expr = rk_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rowkey expr is null", K(ret), K(i));
        } else if (OB_FAIL(expr->locate_expr_datum(*eval_ctx)
                               .to_obj(obj_ptr[i], expr->obj_meta_, expr->obj_datum_map_))) {
          LOG_WARN("datum to obj failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int64_t ObDasVecScanUtils::calc_effective_batch(int64_t needed, double selectivity, int64_t max_batch)
{
  int64_t effective_batch = max_batch;
  if (OB_UNLIKELY(needed <= 0 || max_batch <= 0)) {
    effective_batch = std::max(max_batch, static_cast<int64_t>(1));
  } else if (selectivity > 0 && selectivity < 1.0) {
    double raw = std::ceil(needed / selectivity);
    effective_batch = (raw > static_cast<double>(max_batch))
        ? max_batch
        : static_cast<int64_t>(raw);
  } else {
    effective_batch = std::min(needed, max_batch / 2) * 2;
  }
  effective_batch = std::max(std::min(effective_batch, max_batch), static_cast<int64_t>(1));
  return effective_batch;
}

ObVecIdxAdaTryPath ObDasVecScanUtils::get_initial_vec_idx_path(
    ObVecIndexType vec_index_type,
    ObVecIdxAdaTryPath adaptive_try_path)
{
  ObVecIdxAdaTryPath initial_path = ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN;
  switch (vec_index_type) {
    case ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER: {
      initial_path = ObVecIdxAdaTryPath::VEC_INDEX_WITHOUT_FILTER;
      break;
    }
    case ObVecIndexType::VEC_INDEX_PRE: {
      initial_path = ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER;
      break;
    }
    case ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER: {
      initial_path = ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
      break;
    }
    case ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN: {
      initial_path = adaptive_try_path;
      break;
    }
    default: {
      break;
    }
  }
  return initial_path;
}

}  // namespace sql
}  // namespace oceanbase
