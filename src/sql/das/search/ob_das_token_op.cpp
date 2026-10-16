/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_token_op.h"
#include "sql/das/search/ob_das_scalar_define.h"

namespace oceanbase
{
namespace sql
{

ObDASTokenOpParam::ObDASTokenOpParam()
  : ObIDASSearchOpParam(DAS_SEARCH_OP_TOKEN),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    block_max_param_(nullptr),
    token_boost_(0.0),
    query_token_()
{}

bool ObDASTokenOpParam::is_valid() const
{
  const bool block_max_ok = ir_ctdef_->need_block_max_scan()
                            ? nullptr != block_max_param_
                            : true;
  return nullptr != ir_ctdef_
      && nullptr != ir_rtdef_
      && block_max_ok
      && token_boost_ > 0.0
      && !query_token_.empty();
}

ObDASTokenOp::ObDASTokenOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    arena_allocator_(common::ObMemAttr(MTL_ID(), "DASTokenOp")),
    inv_idx_tablet_id_(),
    inv_idx_scan_param_(),
    inv_idx_agg_param_(),
    inv_idx_scan_iter_(),
    inv_idx_agg_iter_(),
    block_max_param_(nullptr),
    text_retrieval_param_(),
    text_retrieval_iter_(),
    bm25_param_estimator_(),
    inv_idx_scan_range_(),
    inv_idx_agg_range_(),
    token_boost_(0.0),
    eval_ctx_(nullptr),
    obj_buf_(),
    agg_obj_buf_(),
    curr_id_(nullptr),
    min_id_(nullptr),
    max_id_(nullptr),
    is_simple_doc_id_(false),
    use_rich_format_(false),
    bm25_param_estimated_(false),
    block_max_inited_(false),
    estimated_work_row_count_(-1),
    is_inited_(false)
{}

int ObDASTokenOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASTokenOpParam &param = static_cast<const ObDASTokenOpParam &>(op_param);
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (FALSE_IT(is_simple_doc_id_ = search_ctx_.get_rowid_type() == DAS_ROWID_TYPE_UINT64)) {
  } else if (OB_FAIL(init_scan_param(param))) {
    LOG_WARN("failed to init text retrieval param", K(ret));
  } else if (OB_FAIL(bm25_param_estimator_.init(
      *param.ir_ctdef_,
      inv_idx_tablet_id_,
      *param.ir_rtdef_,
      inv_idx_scan_param_,
      search_ctx_.get_bm25_param_cache()))) {
    LOG_WARN("failed to init bm25 param estimator", K(ret));
  } else {
    ir_ctdef_ = param.ir_ctdef_;
    ir_rtdef_ = param.ir_rtdef_;
    block_max_param_ = param.block_max_param_;
    token_boost_ = param.token_boost_;
    query_token_ = param.query_token_;
    eval_ctx_ = ir_rtdef_->eval_ctx_;
    use_rich_format_ = param.use_rich_format_;
    bm25_param_estimated_ = false;
    block_max_inited_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObDASTokenOp::do_prepare_parallel_work_estimate()
{
  int ret = OB_SUCCESS;
  if (estimated_work_row_count_ >= 0) {
    // 复用之前执行或 rescan 已经准备好的估算值。
  } else if (OB_FAIL(estimate_work_row_count())) {
    estimated_work_row_count_ = -1;
    LOG_TRACE("[FUSION_TRACE] failed to estimate token work rows for auto dop",
              KR(ret), K(query_token_));
  } else {
    // 估算结果已缓存在 estimated_work_row_count_ 中。
  }
  return ret;
}

int ObDASTokenOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(bind_scan_tablet_on_open())) {
    LOG_WARN("failed to bind token scan tablet before open", K(ret));
  } else {
    ObDASScanIterParam inv_idx_scan_iter_param;
    init_scan_iter_param(
        *ir_ctdef_->get_inv_idx_scan_scalar_ctdef(),
        *ir_rtdef_->get_inv_idx_scan_scalar_rtdef(),
        inv_idx_scan_iter_param);
    if (OB_FAIL(inv_idx_scan_iter_.init(inv_idx_scan_iter_param))) {
      LOG_WARN("failed to init scan iter", K(ret));
    } else if (FALSE_IT(inv_idx_scan_iter_.set_scan_param(inv_idx_scan_param_))) {
      LOG_WARN("failed to set scan param", K(ret));
    } else if (OB_FAIL(inv_idx_scan_iter_.do_table_scan())) {
      LOG_WARN("failed to open scan iter", K(ret));
    } else if (ir_ctdef_->need_inv_idx_agg()) {
      ObDASScanIterParam inv_idx_agg_iter_param;
      init_scan_iter_param(
          *ir_ctdef_->get_inv_idx_agg_scalar_ctdef(),
          *ir_rtdef_->get_inv_idx_agg_scalar_rtdef(),
          inv_idx_agg_iter_param);
      if (OB_FAIL(inv_idx_agg_iter_.init(inv_idx_agg_iter_param))) {
        LOG_WARN("failed to init agg iter", K(ret));
      } else if (FALSE_IT(inv_idx_agg_iter_.set_scan_param(inv_idx_agg_param_))) {
        LOG_WARN("failed to set agg param", K(ret));
      } else if (OB_FAIL(inv_idx_agg_iter_.do_table_scan())) {
        LOG_WARN("failed to open agg iter", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_text_retrieval_param())) {
      LOG_WARN("failed to init text retrieval param", K(ret));
    } else if (OB_FAIL(bm25_param_estimator_.set_logical_stats_provider(text_retrieval_param_.logical_stats_provider_))) {
      LOG_WARN("failed to set logical stats provider for bm25 param estimator", K(ret));
    } else if (OB_FAIL(text_retrieval_iter_.init(
                   text_retrieval_param_, block_max_param_, &inv_idx_scan_param_))) {
      LOG_WARN("failed to init text retrieval iter", K(ret));
    } else if (is_simple_doc_id_) {
      // skip
    } else if (OB_FAIL(init_compact_id_rows())) {
      LOG_WARN("failed to init compact id rows", K(ret));
    }
  }
  return ret;
}

int ObDASTokenOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (ir_ctdef_->get_inv_idx_scan_scalar_ctdef()->is_logical_table()
             && search_ctx_.get_logical_table_ctx().is_enabled_) {
    inv_idx_tablet_id_ = search_ctx_.get_logical_table_ctx().physical_tablet_id_;
    if (!inv_idx_tablet_id_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[LT hybrid token] physical_tablet_id_ invalid for logical table inv idx rescan",
               K(ret), K(inv_idx_tablet_id_), K(search_ctx_.get_logical_table_ctx()));
    }
  } else if (OB_FAIL(get_related_tablet_id(ir_ctdef_->get_inv_idx_scan_scalar_ctdef(), inv_idx_tablet_id_))) {
    LOG_WARN("failed to get related tablet id", K(ret));
  } else if (OB_FAIL(reset_inv_scan_range())) {
    LOG_WARN("failed to reset inv scan range", K(ret));
  } else {
    const bool need_inv_idx_agg = ir_ctdef_->need_inv_idx_agg();
    ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), inv_idx_tablet_id_, inv_idx_scan_param_);
    ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), inv_idx_tablet_id_, inv_idx_agg_param_);
    bm25_param_estimator_.switch_tablet_id(inv_idx_tablet_id_);
    if (OB_FAIL(inv_idx_scan_iter_.reuse())) {
      LOG_WARN("failed to reuse inv idx scan iter", K(ret));
    } else if (need_inv_idx_agg && OB_FAIL(inv_idx_agg_iter_.reuse())) {
      LOG_WARN("failed to reuse inv idx agg iter", K(ret));
    }
    text_retrieval_iter_.reuse();
    block_max_inited_ = false;
    bm25_param_estimated_ = false;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!inv_idx_scan_param_.key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-empty scan range", K(ret), K(inv_idx_scan_param_.key_ranges_));
    } else if (OB_FAIL(inv_idx_scan_param_.key_ranges_.push_back(inv_idx_scan_range_))) {
      LOG_WARN("failed to push back scan range", K(ret));
    } else if (OB_FAIL(inv_idx_scan_iter_.rescan())) {
      LOG_WARN("failed to rescan inv idx scan iter", K(ret));
    } else if (!need_inv_idx_agg) {
      // skip
    } else if (OB_UNLIKELY(!inv_idx_agg_param_.key_ranges_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non-empty scan range", K(ret), K(inv_idx_agg_param_.key_ranges_));
    } else if (OB_FAIL(inv_idx_agg_param_.key_ranges_.push_back(inv_idx_agg_range_))) {
      LOG_WARN("failed to push back agg range", K(ret));
    } else if (OB_FAIL(inv_idx_agg_iter_.rescan())) {
      LOG_WARN("failed to rescan inv idx agg iter", K(ret));
    }
  }
  return ret;
}

int ObDASTokenOp::bind_scan_tablet_on_open()
{
  int ret = OB_SUCCESS;
  common::ObTabletID current_tablet_id;
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = OB_ISNULL(ir_ctdef_)
      ? nullptr : ir_ctdef_->get_inv_idx_scan_scalar_ctdef();
  if (OB_ISNULL(inv_idx_scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inverted index scan ctdef", K(ret), KP(ir_ctdef_));
  } else {
    const ObLogicalTableSearchCtx &lt_ctx = search_ctx_.get_logical_table_ctx();
    if (inv_idx_scan_ctdef->is_logical_table() && lt_ctx.is_enabled_) {
      current_tablet_id = lt_ctx.physical_tablet_id_;
      if (!current_tablet_id.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[LT hybrid token] physical tablet id is invalid when binding token scan",
                 K(ret), K(current_tablet_id), K(lt_ctx));
      }
    } else if (OB_FAIL(get_related_tablet_id(inv_idx_scan_ctdef, current_tablet_id))) {
      LOG_WARN("failed to get related tablet id", K(ret), KPC(inv_idx_scan_ctdef));
    }

    if (OB_SUCC(ret)) {
      inv_idx_tablet_id_ = current_tablet_id;
      // The original serial runtime can be opened after a preceding parallel
      // partition used only cloned runtimes. No storage iterator exists on the
      // original runtime yet, so bind the current tablet without switch semantics.
      inv_idx_scan_param_.ls_id_ = search_ctx_.get_ls_id();
      inv_idx_scan_param_.tablet_id_ = inv_idx_tablet_id_;
      inv_idx_scan_param_.need_switch_param_ = false;
      if (ir_ctdef_->need_inv_idx_agg()) {
        inv_idx_agg_param_.ls_id_ = search_ctx_.get_ls_id();
        inv_idx_agg_param_.tablet_id_ = inv_idx_tablet_id_;
        inv_idx_agg_param_.need_switch_param_ = false;
      }
    }
  }
  return ret;
}

int ObDASTokenOp::do_close()
{
  int ret = OB_SUCCESS;
  inv_idx_scan_iter_.release();
  inv_idx_agg_iter_.release();
  inv_idx_scan_range_.reset();
  inv_idx_agg_range_.reset();
  text_retrieval_iter_.reset();
  bm25_param_estimator_.reset();
  inv_idx_scan_param_.destroy_schema_guard();
  inv_idx_scan_param_.snapshot_.reset();
  inv_idx_scan_param_.destroy();
  inv_idx_agg_param_.destroy_schema_guard();
  inv_idx_agg_param_.snapshot_.reset();
  inv_idx_agg_param_.destroy();
  arena_allocator_.reset();
  estimated_work_row_count_ = -1;
  is_inited_ = false;
  return ret;
}

int ObDASTokenOp::estimate_work_row_count()
{
  int ret = OB_SUCCESS;
  int64_t logical_row_count = 0;
  int64_t physical_row_count = 0;
  ObAccessService *access_service = nullptr;
  ObSimpleBatch batch;
  ObTableScanParam estimate_param;
  ObSEArray<ObEstRowCountRecord, 1> estimate_records;
  storage::ObTableScanRange table_scan_range;
  ObArenaAllocator allocator;
  const int64_t timeout_us = THIS_WORKER.get_timeout_remain();
  if (OB_ISNULL(access_service = MTL(ObAccessService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_TRACE("access service is null when estimating token work rows", K(ret));
  } else if (inv_idx_scan_param_.key_ranges_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_TRACE("inverted index range is empty when estimating token work rows",
              K(ret), K(query_token_));
  } else {
    batch.type_ = ObSimpleBatch::T_SCAN;
    batch.range_ = &inv_idx_scan_param_.key_ranges_.at(0);
    estimate_param.index_id_ = inv_idx_scan_param_.index_id_;
    estimate_param.scan_flag_ = inv_idx_scan_param_.scan_flag_;
    estimate_param.tablet_id_ = inv_idx_scan_param_.tablet_id_;
    estimate_param.ls_id_ = inv_idx_scan_param_.ls_id_;
    estimate_param.tx_id_ = inv_idx_scan_param_.tx_id_;
    estimate_param.schema_version_ = inv_idx_scan_param_.schema_version_;
    estimate_param.frozen_version_ = GET_BATCH_ROWS_READ_SNAPSHOT_VERSION;
    if (OB_FAIL(table_scan_range.init(inv_idx_scan_param_, batch, allocator))) {
      LOG_TRACE("failed to init token work estimate range", K(ret), K(query_token_));
    } else if (OB_FAIL(access_service->estimate_row_count(
                   estimate_param,
                   table_scan_range,
                   timeout_us,
                   estimate_records,
                   logical_row_count,
                   physical_row_count))) {
      LOG_TRACE("failed to estimate token work rows", K(ret), K(query_token_));
    } else {
      estimated_work_row_count_ = std::max(0L, logical_row_count);
      LOG_TRACE("[FUSION_TRACE] estimated token work rows for auto dop",
                K(query_token_), K(estimated_work_row_count_),
                K(logical_row_count), K(physical_row_count));
    }
  }
  return ret;
}

int ObDASTokenOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  const ObDatum *id_datum = nullptr;
  if (OB_FAIL(do_inv_idx_bm25_param_estimation_on_demand())) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.get_next_row())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to get next row from text retrieval iter", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_id(id_datum))) {
    LOG_WARN("failed to get curr id from text retrieval iter", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_score(score))) {
    LOG_WARN("failed to get curr score from text retrieval iter", K(ret));
  } else if (FALSE_IT(score *= token_boost_)) {
  } else if (is_simple_doc_id_) {
    const uint64_t docid = id_datum->get_uint64();
    if (search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_hi().is_max_value()
        && docid > search_ctx_.get_docid_range_hi().get_uint64()) {
      ret = OB_ITER_END;
    } else {
      next_id.set_uint64(docid);
    }
  } else {
    datum_to_compact_row(*id_datum, *curr_id_);
    next_id.set_compact_row(curr_id_);
  }
  return ret;
}

int ObDASTokenOp::do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  const ObDatum *id_datum = nullptr;
  ObDatum target_datum;
  if (OB_FAIL(search_ctx_.get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from rowid", K(ret), K(target));
  } else {
    // Clamp target to docid range lower bound to prevent
    // ObTextRetrievalTokenIter::advance_to from corrupting the inverted index
    // scan range start_key (it overwrites obj_ptr[1] with target_docid,
    // expanding the scan range below docid_range_lo).
    if (is_simple_doc_id_ && search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_lo().is_min_value()
        && target_datum.get_uint64() < search_ctx_.get_docid_range_lo().get_uint64()) {
      target_datum.set_uint(search_ctx_.get_docid_range_lo().get_uint64());
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_inv_idx_bm25_param_estimation_on_demand())) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.advance_to(target_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance to target row", K(ret), K(target));
    }
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_id(id_datum))) {
    LOG_WARN("failed to get curr id from text retrieval iter", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_score(score))) {
    LOG_WARN("failed to get curr score from text retrieval iter", K(ret));
  } else if (FALSE_IT(score *= token_boost_)) {
  } else if (is_simple_doc_id_) {
    const uint64_t docid = id_datum->get_uint64();
    if (search_ctx_.has_docid_range()
        && !search_ctx_.get_docid_range_hi().is_max_value()
        && docid > search_ctx_.get_docid_range_hi().get_uint64()) {
      ret = OB_ITER_END;
    } else {
      curr_id.set_uint64(docid);
    }
  } else {
    datum_to_compact_row(*id_datum, *curr_id_);
    curr_id.set_compact_row(curr_id_);
  }
  return ret;
}

int ObDASTokenOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  const storage::ObMaxScoreTuple *storage_max_score_tuple = nullptr;
  ObDatum target_datum;
  ObDASRowID min_id;
  ObDASRowID max_id;
  if (OB_FAIL(search_ctx_.get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from rowid", K(ret), K(target));
  } else if (is_simple_doc_id_ && search_ctx_.has_docid_range()
             && !search_ctx_.get_docid_range_lo().is_min_value()
             && target_datum.get_uint64() < search_ctx_.get_docid_range_lo().get_uint64()) {
    target_datum.set_uint(search_ctx_.get_docid_range_lo().get_uint64());
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_block_max_iter_on_demand())) {
    LOG_WARN("failed to init block max iter on demand", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.advance_shallow(target_datum, inclusive))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance shallow", K(ret), K(target));
    }
  } else if (OB_FAIL(text_retrieval_iter_.get_curr_block_max_info(storage_max_score_tuple))) {
    LOG_WARN("failed to get curr block max info", K(ret));
  } else {
    if (storage_max_score_tuple->min_domain_id_->is_min()) {
      min_id.set_min();
    } else if (storage_max_score_tuple->min_domain_id_->is_max()) {
      min_id.set_max();
    } else if (is_simple_doc_id_) {
      min_id.set_uint64(storage_max_score_tuple->min_domain_id_->get_uint64());
    } else {
      datum_to_compact_row(*storage_max_score_tuple->min_domain_id_, *min_id_);
    }

    if (storage_max_score_tuple->max_domain_id_->is_max()) {
      max_id.set_max();
    } else if (storage_max_score_tuple->max_domain_id_->is_min()) {
      max_id.set_min();
    } else if (is_simple_doc_id_) {
      max_id.set_uint64(storage_max_score_tuple->max_domain_id_->get_uint64());
    } else {
      datum_to_compact_row(*storage_max_score_tuple->max_domain_id_, *max_id_);
    }

    max_score_tuple_.set(min_id, max_id, storage_max_score_tuple->max_score_ * token_boost_);
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObDASTokenOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  double token_max_score = 0.0;
  if (OB_FAIL(init_block_max_iter_on_demand())) {
    LOG_WARN("failed to init block max iter on demand", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.get_dim_max_score(token_max_score))) {
    LOG_WARN("failed to get dim max score", K(ret));
  } else {
    threshold += token_max_score * token_boost_;
  }
  return ret;
}

int ObDASTokenOp::init_scan_param(const ObDASTokenOpParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_scan_range(*param.ir_ctdef_, param.query_token_, inv_idx_scan_range_))) {
    LOG_WARN("failed to init scan range", K(ret), K(param.query_token_));
  } else {
    const bool need_inv_idx_agg = param.ir_ctdef_->need_inv_idx_agg();
    const ObDASScalarScanCtDef *inv_idx_scan_ctdef = param.ir_ctdef_->get_inv_idx_scan_scalar_ctdef();
    ObDASScalarScanRtDef *inv_idx_scan_rtdef = param.ir_rtdef_->get_inv_idx_scan_scalar_rtdef();
    const ObDASScalarScanCtDef *inv_idx_agg_ctdef = param.ir_ctdef_->get_inv_idx_agg_scalar_ctdef();
    ObDASScalarScanRtDef *inv_idx_agg_rtdef = param.ir_rtdef_->get_inv_idx_agg_scalar_rtdef();
    if (OB_ISNULL(inv_idx_scan_ctdef) || OB_ISNULL(inv_idx_scan_rtdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KP(inv_idx_scan_ctdef), KP(inv_idx_scan_rtdef), K(param));
    } else if (OB_UNLIKELY(need_inv_idx_agg && (nullptr == inv_idx_agg_ctdef || nullptr == inv_idx_agg_rtdef))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr to inv idx agg def", K(ret), K(param));
    } else {
      const ObLogicalTableSearchCtx &lt_ctx = search_ctx_.get_logical_table_ctx();
      // Phase 1: resolve tablet (must not be else-if sibling of init_scan_param below).
      if (inv_idx_scan_ctdef->is_logical_table() && lt_ctx.is_enabled_) {
        inv_idx_tablet_id_ = lt_ctx.physical_tablet_id_;
        if (!inv_idx_tablet_id_.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[LT hybrid token] physical_tablet_id_ invalid for logical table inv idx scan",
                   K(ret), K(inv_idx_tablet_id_), K(lt_ctx));
        }
      } else if (OB_FAIL(search_ctx_.get_related_tablet_id(inv_idx_scan_ctdef, inv_idx_tablet_id_))) {
        LOG_WARN("failed to get related tablet id", K(ret), K(inv_idx_scan_ctdef));
      }
      // Phase 2: init scan_param; for logical table override index_id to physical KV table.
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(!inv_idx_scan_rtdef->key_ranges_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected non-empty key ranges", K(ret), K(inv_idx_scan_rtdef->key_ranges_));
      } else if (OB_FAIL(search_ctx_.init_scan_param(
                     inv_idx_tablet_id_, inv_idx_scan_ctdef, inv_idx_scan_rtdef,
                     inv_idx_scan_param_, false /* use_async_read_snapshot */))) {
        LOG_WARN("failed to init scan param", K(ret), K(inv_idx_scan_ctdef), K(inv_idx_scan_rtdef));
      } else {
        if (inv_idx_scan_ctdef->is_logical_table() && lt_ctx.is_enabled_) {
          inv_idx_scan_param_.index_id_ = lt_ctx.physical_data_table_id_;
        }
        if (OB_FAIL(inv_idx_scan_param_.key_ranges_.push_back(inv_idx_scan_range_))) {
          LOG_WARN("failed to push back scan range", K(ret));
        } else if (!need_inv_idx_agg) {
          // skip
        } else if (OB_UNLIKELY(!inv_idx_agg_rtdef->key_ranges_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected non-empty key ranges", K(ret), K(inv_idx_agg_rtdef->key_ranges_));
        } else if (OB_FAIL(search_ctx_.init_scan_param(
                       inv_idx_tablet_id_, inv_idx_agg_ctdef, inv_idx_agg_rtdef,
                       inv_idx_agg_param_, false /* use_async_read_snapshot */))) {
          LOG_WARN("failed to init agg param", K(ret), K(inv_idx_agg_ctdef), K(inv_idx_agg_rtdef));
        } else {
          if (inv_idx_agg_ctdef->is_logical_table() && lt_ctx.is_enabled_) {
            inv_idx_agg_param_.index_id_ = lt_ctx.physical_data_table_id_;
          }
          if (OB_FAIL(inv_idx_agg_param_.key_ranges_.push_back(inv_idx_agg_range_))) {
            LOG_WARN("failed to push back agg range", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASTokenOp::init_scan_range(
    const ObDASIRScanCtDef &ir_ctdef,
    const ObString &query_token,
    ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  obj_buf_[0].set_common_value(query_token);
  obj_buf_[0].set_meta_type(ir_ctdef.search_text_->obj_meta_);
  obj_buf_[2].set_common_value(query_token);
  obj_buf_[2].set_meta_type(ir_ctdef.search_text_->obj_meta_);

  if (search_ctx_.has_docid_range() && is_simple_doc_id_
      && OB_NOT_NULL(ir_ctdef.inv_scan_domain_id_col_)) {
    const common::ObObj &lo = search_ctx_.get_docid_range_lo();
    const common::ObObj &hi = search_ctx_.get_docid_range_hi();
    if (lo.is_min_value()) {
      obj_buf_[1].set_min_value();
    } else {
      obj_buf_[1] = lo;
      obj_buf_[1].set_meta_type(ir_ctdef.inv_scan_domain_id_col_->obj_meta_);
    }
    if (hi.is_max_value()) {
      obj_buf_[3].set_max_value();
    } else {
      obj_buf_[3] = hi;
      obj_buf_[3].set_meta_type(ir_ctdef.inv_scan_domain_id_col_->obj_meta_);
    }
  } else {
    obj_buf_[1].set_min_value();
    obj_buf_[3].set_max_value();
  }
  scan_range.table_id_ = ir_ctdef.get_inv_idx_scan_scalar_ctdef()->ref_table_id_;
  scan_range.start_key_.assign(obj_buf_, 2);
  scan_range.end_key_.assign(obj_buf_ + 2, 2);
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();

  // Initialize agg range WITHOUT docid restriction so that token_doc_cnt estimation
  // in ObTextRetrievalTokenIter::estimate_token_doc_cnt() uses global statistics.
  // This ensures BM25 IDF is computed correctly for range-parallel tasks.
  agg_obj_buf_[0].set_common_value(query_token);
  agg_obj_buf_[0].set_meta_type(ir_ctdef.search_text_->obj_meta_);
  agg_obj_buf_[2].set_common_value(query_token);
  agg_obj_buf_[2].set_meta_type(ir_ctdef.search_text_->obj_meta_);
  agg_obj_buf_[1].set_min_value();
  agg_obj_buf_[3].set_max_value();
  inv_idx_agg_range_.table_id_ = ir_ctdef.get_inv_idx_scan_scalar_ctdef()->ref_table_id_;
  inv_idx_agg_range_.start_key_.assign(agg_obj_buf_, 2);
  inv_idx_agg_range_.end_key_.assign(agg_obj_buf_ + 2, 2);
  inv_idx_agg_range_.border_flag_.set_inclusive_start();
  inv_idx_agg_range_.border_flag_.set_inclusive_end();
  return ret;
}

void ObDASTokenOp::init_scan_iter_param(
    const ObDASScalarScanCtDef &scan_ctdef,
    ObDASScalarScanRtDef &scan_rtdef,
    ObDASScanIterParam &iter_param)
{
  iter_param.scan_ctdef_ = nullptr;
  iter_param.scalar_scan_ctdef_ = &scan_ctdef;
  iter_param.use_scalar_ctdef_ = true;
  iter_param.max_size_ = scan_rtdef.eval_ctx_->is_vectorized() ? scan_rtdef.eval_ctx_->max_batch_size_ : 1;
  iter_param.eval_ctx_ = scan_rtdef.eval_ctx_;
  iter_param.exec_ctx_ = &scan_rtdef.eval_ctx_->exec_ctx_;
  iter_param.output_ = &scan_ctdef.result_output_;
  init_logical_table_scan_iter_param(scan_ctdef, iter_param);
}

void ObDASTokenOp::init_logical_table_scan_iter_param(
    const ObDASScalarScanCtDef &scan_ctdef,
    ObDASScanIterParam &iter_param)
{
  // Logical-table FTS: ScanIter self-manages runtime lt_ctx for kv expr binding
  // and projection; ns/lt for range rewrite come from search_ctx_.logical_table_ctx_
  // (op-level lt_ctx_, captured from session at runtime — NOT from ctdef).
  if (scan_ctdef.is_logical_table() && search_ctx_.get_logical_table_ctx().is_enabled_) {
    iter_param.self_managed_logical_lt_ctx_ = true;
    const ObLogicalTableSearchCtx &hs = search_ctx_.get_logical_table_ctx();
    iter_param.namespace_id_ = hs.namespace_id_;
    iter_param.ltable_id_ = hs.logical_table_id_;
  }
}

int ObDASTokenOp::init_text_retrieval_param()
{
  int ret = OB_SUCCESS;
  text_retrieval_param_.allocator_ = &arena_allocator_;
  text_retrieval_param_.inv_idx_scan_param_ = &inv_idx_scan_param_;
  text_retrieval_param_.inv_idx_scan_iter_ = &inv_idx_scan_iter_;
  if (ir_ctdef_->need_inv_idx_agg()) {
    text_retrieval_param_.inv_idx_agg_param_ = &inv_idx_agg_param_;
    text_retrieval_param_.inv_idx_agg_iter_ = &inv_idx_agg_iter_;
    text_retrieval_param_.inv_idx_agg_expr_ = ir_ctdef_->get_inv_idx_agg_scalar_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
  } else {
    text_retrieval_param_.inv_idx_agg_param_ = nullptr;
    text_retrieval_param_.inv_idx_agg_iter_ = nullptr;
    text_retrieval_param_.inv_idx_agg_expr_ = nullptr;
  }
  text_retrieval_param_.fwd_idx_scan_param_ = nullptr;
  text_retrieval_param_.fwd_idx_agg_iter_ = nullptr;
  text_retrieval_param_.fwd_idx_agg_expr_ = nullptr;
  text_retrieval_param_.eval_ctx_ = eval_ctx_;
  text_retrieval_param_.relevance_expr_ = ir_ctdef_->relevance_expr_;
  text_retrieval_param_.inv_scan_doc_length_col_ = ir_ctdef_->inv_scan_doc_length_col_;
  text_retrieval_param_.inv_scan_domain_id_col_ = ir_ctdef_->inv_scan_domain_id_col_;
  text_retrieval_param_.reuse_inv_idx_agg_res_ = ir_ctdef_->need_inv_idx_agg();
  text_retrieval_param_.use_rich_format_ = use_rich_format_;
  if (OB_FAIL(init_logical_table_text_retrieval_param())) {
    LOG_WARN("failed to init logical table text retrieval param", K(ret));
  }
  return ret;
}

int ObDASTokenOp::init_logical_table_text_retrieval_param()
{
  int ret = OB_SUCCESS;
  const ObLogicalTableSearchCtx &lt_ctx = search_ctx_.get_logical_table_ctx();
  const ObDASScalarScanCtDef *inv_scan_ctdef = ir_ctdef_->get_inv_idx_scan_scalar_ctdef();
  // Logical-table FTS BM25: when this scan is logical and
  // scoring was preserved (need_calc_relevance() && !need_inv_idx_agg()), look
  // up / lazily create the query-scope filtered stats provider so the token
  // iter can source total_doc_cnt / avg_doc_token_cnt / token_doc_cnt from it.
  text_retrieval_param_.logical_stats_provider_ = nullptr;
  text_retrieval_param_.is_logical_fts_ = false;
  text_retrieval_param_.logical_index_seq_ = OB_INVALID_ID;
  text_retrieval_param_.query_token_.reset();
  text_retrieval_param_.query_token_meta_.reset();
  if (lt_ctx.is_enabled_ && nullptr != inv_scan_ctdef && inv_scan_ctdef->is_logical_table()) {
    text_retrieval_param_.is_logical_fts_ = true;
    text_retrieval_param_.logical_index_seq_ = inv_scan_ctdef->logical_index_seq_;
    text_retrieval_param_.query_token_ = query_token_;
    text_retrieval_param_.query_token_meta_ = ir_ctdef_->search_text_->obj_meta_;
  }
  if (ir_ctdef_->need_calc_relevance() && !ir_ctdef_->need_inv_idx_agg()) {
    if (lt_ctx.is_enabled_) {
      ObLogicalFilteredBM25StatsKey key;
      key.namespace_id_ = lt_ctx.namespace_id_;
      key.logical_table_id_ = lt_ctx.logical_table_id_;
      key.logical_index_seq_ = (nullptr != inv_scan_ctdef)
          ? inv_scan_ctdef->logical_index_seq_ : 0;
      key.physical_data_table_id_ = lt_ctx.physical_data_table_id_;
      key.physical_tablet_id_ = lt_ctx.physical_tablet_id_;
      key.ir_ctdef_ = ir_ctdef_;
      key.filter_exprs_ = &ir_ctdef_->logical_stats_filter_exprs_;
      ObLogicalFilteredBM25StatsProvider *provider = nullptr;
      if (OB_FAIL(search_ctx_.get_or_create_logical_bm25_stats_provider(
                  key,
                  ir_ctdef_->get_stats_primary_scan_scalar_ctdef(),
                  ir_rtdef_->get_stats_primary_scan_scalar_rtdef(),
                  inv_scan_ctdef,
                  ir_rtdef_->get_inv_idx_scan_scalar_rtdef(),
                  &ir_ctdef_->logical_stats_filter_exprs_,
                  provider))) {
        LOG_WARN("failed to get or create logical filtered bm25 stats provider", K(ret), K(key));
      } else {
        text_retrieval_param_.logical_stats_provider_ = provider;
      }
    }
  }
  return ret;
}

int ObDASTokenOp::init_compact_id_rows()
{
  int ret = OB_SUCCESS;
  const int64_t single_id_row_size = ObCompactRow::calc_max_row_size(get_rowid_exprs(), 0);
  void *id_row_buf = nullptr;
  if (OB_ISNULL(id_row_buf = arena_allocator_.alloc(3 * single_id_row_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc id row buf", K(ret), K(single_id_row_size));
  } else {
    ObCompactRow *row_arr = new (id_row_buf) ObCompactRow[3];
    curr_id_ = &row_arr[0];
    min_id_ = &row_arr[1];
    max_id_ = &row_arr[2];
    curr_id_->init(get_rowid_meta());
    min_id_->init(get_rowid_meta());
    max_id_->init(get_rowid_meta());
  }
  return ret;
}

int ObDASTokenOp::do_inv_idx_bm25_param_estimation_on_demand()
{
  int ret = OB_SUCCESS;
  uint64_t total_doc_cnt = 0;
  double avg_doc_token_cnt = 0.0;
  if (bm25_param_estimated_) {
    // skip
  } else if (OB_FAIL(bm25_param_estimator_.do_estimation(search_ctx_))) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(bm25_param_estimator_.get_bm25_param(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to get bm25 param", K(ret));
  } else {
    // project bm25 param to relevance exprs
    ObExpr *total_doc_cnt_expr = ir_ctdef_->relevance_expr_->args_[ObExprBM25::TOTAL_DOC_CNT_PARAM_IDX];
    ObExpr *avg_doc_token_cnt_expr = ir_ctdef_->avg_doc_token_cnt_expr_;
    if (use_rich_format_) {
      if (OB_FAIL(total_doc_cnt_expr->init_vector_for_write(
          *eval_ctx_, total_doc_cnt_expr->get_default_res_format(), 1))) {
        LOG_WARN("failed to init vector for write", K(ret));
      } else if (OB_FAIL(avg_doc_token_cnt_expr->init_vector_for_write(
          *eval_ctx_, avg_doc_token_cnt_expr->get_default_res_format(), 1))) {
        LOG_WARN("failed to init vector for write", K(ret));
      } else {
        total_doc_cnt_expr->get_vector(*eval_ctx_)->set_int(0, total_doc_cnt);
        avg_doc_token_cnt_expr->get_vector(*eval_ctx_)->set_double(0, avg_doc_token_cnt);
      }
    } else {
      total_doc_cnt_expr->locate_datum_for_write(*eval_ctx_).set_int(total_doc_cnt);
      avg_doc_token_cnt_expr->locate_datum_for_write(*eval_ctx_).set_double(avg_doc_token_cnt);
    }
    bm25_param_estimated_ = true;
  }
  return ret;
}

void ObDASTokenOp::datum_to_compact_row(const ObDatum &id_datum, ObCompactRow &row)
{
  row.set_cell_payload(get_rowid_meta(), 0, id_datum.ptr_, id_datum.len_);
}

int ObDASTokenOp::init_block_max_iter_on_demand()
{
  int ret = OB_SUCCESS;
  uint64_t total_doc_cnt = 0;
  double avg_doc_token_cnt = 0.0;
  if (block_max_inited_) {
    // skip
  } else if (OB_FAIL(do_inv_idx_bm25_param_estimation_on_demand())) {
    LOG_WARN("failed to do inv idx bm25 param estimation on demand", K(ret));
  } else if (OB_FAIL(bm25_param_estimator_.get_bm25_param(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to get bm25 param", K(ret));
  } else if (OB_FAIL(text_retrieval_iter_.init_block_max_iter(total_doc_cnt, avg_doc_token_cnt))) {
    LOG_WARN("failed to init block max iter", K(ret));
  } else {
    block_max_inited_ = true;
  }
  return ret;
}

int ObDASTokenOp::reset_inv_scan_range()
{
  int ret = OB_SUCCESS;
  if (search_ctx_.has_docid_range() && is_simple_doc_id_
      && OB_NOT_NULL(ir_ctdef_) && OB_NOT_NULL(ir_ctdef_->inv_scan_domain_id_col_)) {
    const common::ObObj &lo = search_ctx_.get_docid_range_lo();
    const common::ObObj &hi = search_ctx_.get_docid_range_hi();
    if (lo.is_min_value()) {
      obj_buf_[1].set_min_value();
    } else {
      obj_buf_[1] = lo;
      obj_buf_[1].set_meta_type(ir_ctdef_->inv_scan_domain_id_col_->obj_meta_);
    }
    if (hi.is_max_value()) {
      obj_buf_[3].set_max_value();
    } else {
      obj_buf_[3] = hi;
      obj_buf_[3].set_meta_type(ir_ctdef_->inv_scan_domain_id_col_->obj_meta_);
    }
  } else {
    obj_buf_[1].set_min_value();
    obj_buf_[3].set_max_value();
  }
  inv_idx_scan_range_.start_key_.assign(obj_buf_, 2);
  inv_idx_scan_range_.end_key_.assign(obj_buf_ + 2, 2);
  inv_idx_scan_range_.border_flag_.set_inclusive_start();
  inv_idx_scan_range_.border_flag_.set_inclusive_end();
  // Agg range always uses full range (no docid restriction) for global BM25 statistics.
  agg_obj_buf_[1].set_min_value();
  agg_obj_buf_[3].set_max_value();
  inv_idx_agg_range_.start_key_.assign(agg_obj_buf_, 2);
  inv_idx_agg_range_.end_key_.assign(agg_obj_buf_ + 2, 2);
  inv_idx_agg_range_.border_flag_.set_inclusive_start();
  inv_idx_agg_range_.border_flag_.set_inclusive_end();
  return ret;
}

}
}
