/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_async_token_helper.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_segment_merge_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{

ObDASAsyncTokenHelper::ObDASAsyncTokenHelper(ObDASSearchCtx &search_ctx)
  : search_ctx_(search_ctx),
    arena_allocator_(common::ObMemAttr(MTL_ID(), "DASAsyncTokHlp")),
    posting_iter_allocator_(common::ObMemAttr(MTL_ID(), "DASATokHIter")),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    inv_idx_tablet_id_(),
    inv_idx_scan_param_(),
    version_range_(),
    index_table_id_(common::OB_INVALID_ID),
    group_slot_(-1),
    direct_bind_(false),
    is_empty_segment_(false),
    tsc_service_(nullptr),
    query_flag_(),
    posting_iter_(nullptr),
    posting_scan_param_(),
    term_datums_(nullptr),
    term_key_(),
    query_token_(),
    param_estimator_(),
    dim_max_score_(0.0),
    token_boost_(1.0),
    phrase_block_max_tuple_(),
    inv_idx_scan_param_inited_(false),
    is_inited_(false)
{}

ObDASAsyncTokenHelper::~ObDASAsyncTokenHelper()
{
  // Always release: init/open may fail halfway before is_inited_ is set.
  (void)close();
}

int ObDASAsyncTokenHelper::init(const ObDASAsyncTokenHelperParam &param)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || param.group_slot_ < -1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async token helper param", K(ret), KP(param.ir_ctdef_),
             KP(param.ir_rtdef_), "token_len", param.query_token_.length(),
             K(param.index_table_id_), K(param.group_slot_), K(param.direct_bind_),
             K(param.token_boost_));
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != search_ctx_.get_rowid_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async token helper only supports uint64 rowid", K(ret),
             "rowid_type", search_ctx_.get_rowid_type());
  } else if (OB_FAIL(init_term_key(param.query_token_))) {
    LOG_WARN("failed to init term key", K(ret), "token_len", param.query_token_.length());
    arena_allocator_.reset();
    term_datums_ = nullptr;
    term_key_.reset();
    query_token_.reset();
  } else {
    ir_ctdef_ = param.ir_ctdef_;
    ir_rtdef_ = param.ir_rtdef_;
    index_table_id_ = param.index_table_id_;
    group_slot_ = param.group_slot_;
    direct_bind_ = param.direct_bind_;
    token_boost_ = param.token_boost_;
    is_inited_ = true;
  }
  return ret;
}

int ObDASAsyncTokenHelper::init_term_key(const common::ObString &query_token)
{
  int ret = OB_SUCCESS;
  const int64_t token_len = query_token.length();
  void *datum_buf = nullptr;
  char *token_buf = nullptr;
  if (OB_UNLIKELY(token_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty query token", K(ret), K(token_len));
  } else if (OB_ISNULL(datum_buf = arena_allocator_.alloc(sizeof(blocksstable::ObStorageDatum)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc term datum", K(ret));
  } else if (OB_ISNULL(token_buf = static_cast<char *>(arena_allocator_.alloc(token_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc term token buffer", K(ret), K(token_len));
  } else {
    term_datums_ = new (datum_buf) blocksstable::ObStorageDatum();
    MEMCPY(token_buf, query_token.ptr(), token_len);
    term_datums_->set_string(token_buf, static_cast<uint32_t>(token_len));
    query_token_.assign_ptr(token_buf, static_cast<int32_t>(token_len));
    if (OB_FAIL(term_key_.assign(term_datums_, 1))) {
      LOG_WARN("failed to assign term key", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncTokenHelper::init_inv_idx_scan_param()
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = nullptr;
  ObDASScalarScanRtDef *inv_idx_scan_rtdef = nullptr;
  if (inv_idx_tablet_id_.is_valid() 
      && (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_ctdef_->get_inv_idx_scan_scalar_ctdef()))) {
    // Tests inject the tablet id directly and do not construct DAS scalar scan defs.
  } else if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ir ctdef/rtdef", K(ret));
  } else if (OB_ISNULL(inv_idx_scan_ctdef = ir_ctdef_->get_inv_idx_scan_scalar_ctdef()) ||
             OB_ISNULL(inv_idx_scan_rtdef = ir_rtdef_->get_inv_idx_scan_scalar_rtdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null inv idx scan def", K(ret), KP(inv_idx_scan_ctdef), KP(inv_idx_scan_rtdef));
  } else if (OB_UNLIKELY(inv_idx_scan_rtdef->get_ctdef() != inv_idx_scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inv idx scan ctdef and rtdef mismatch", K(ret),
             KP(inv_idx_scan_ctdef), KP(inv_idx_scan_rtdef->get_ctdef()));
  } else if (OB_FAIL(search_ctx_.get_related_tablet_id(inv_idx_scan_ctdef, inv_idx_tablet_id_))) {
    LOG_WARN("failed to get related tablet id", K(ret), K(inv_idx_scan_ctdef));
  } else if (!inv_idx_scan_param_inited_) {
    // Keep the SQL snapshot for IncMajor refresh transaction visibility.
    if (OB_FAIL(search_ctx_.init_scan_param(
        inv_idx_tablet_id_, inv_idx_scan_ctdef, inv_idx_scan_rtdef, inv_idx_scan_param_,
        false /*use_async_read_snapshot*/))) {
      LOG_WARN("failed to init inv idx scan param", K(ret), K(inv_idx_tablet_id_));
    } else {
      inv_idx_scan_param_inited_ = true;
    }
  } else {
    ObIDASSearchOp::switch_tablet_id(search_ctx_.get_ls_id(), inv_idx_tablet_id_, inv_idx_scan_param_);
  }
  return ret;
}

int ObDASAsyncTokenHelper::init_posting_scan_param()
{
  posting_scan_param_.reset();
  query_flag_.set_use_block_cache();
  posting_scan_param_.search_key_ = &term_key_;
  posting_scan_param_.tablet_id_ = inv_idx_tablet_id_;
  posting_scan_param_.query_flag_ = query_flag_;
  posting_scan_param_.allocator_ = &posting_iter_allocator_;
  posting_scan_param_.need_posting_value_ = true;
  if (search_ctx_.has_docid_range()) {
    if (!search_ctx_.get_docid_range_lo().is_min_value()) {
      posting_scan_param_.min_doc_id_ = search_ctx_.get_docid_range_lo().get_uint64();
    }
    if (!search_ctx_.get_docid_range_hi().is_max_value()) {
      posting_scan_param_.max_doc_id_ = search_ctx_.get_docid_range_hi().get_uint64();
    }
  }
  return OB_SUCCESS;
}

int ObDASAsyncTokenHelper::init_bm25_param_estimator()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ir ctdef/rtdef", K(ret));
  } else if (OB_FAIL(param_estimator_.init(search_ctx_, *ir_ctdef_, *ir_rtdef_,
                                           inv_idx_tablet_id_, query_token_))) {
    LOG_WARN("failed to init bm25 param estimator", K(ret));
  }
  return ret;
}

int ObDASAsyncTokenHelper::init_bm25_param_estimator(storage::ObAsyncBM25ParamEstimator &estimator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ir ctdef/rtdef", K(ret));
  } else if (OB_FAIL(init_inv_idx_scan_param())) {
    LOG_WARN("failed to init inv idx scan param for bm25 estimator", K(ret));
  } else if (OB_FAIL(estimator.init(search_ctx_, *ir_ctdef_, *ir_rtdef_,
                                    inv_idx_tablet_id_, query_token_))) {
    LOG_WARN("failed to init shared bm25 param estimator", K(ret));
  }
  return ret;
}

int ObDASAsyncTokenHelper::open_posting_iter_from_tablet()
{
  int ret = OB_SUCCESS;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  const storage::ObTabletHandle *pinned_tablet_handle = nullptr;
  storage::ObAccessService *mtl_tsc_service = MTL(storage::ObAccessService *);
  const bool use_injected_tsc_service =
      OB_NOT_NULL(tsc_service_) && tsc_service_ != mtl_tsc_service;
  // Prefer test-injected service; otherwise take MTL.
  if (OB_ISNULL(tsc_service_) && OB_ISNULL(tsc_service_ = mtl_tsc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null access service", K(ret));
  } else if (OB_NOT_NULL(partition) && partition->ls_id_ != search_ctx_.get_ls_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index partition ls mismatch", K(ret), K(partition->ls_id_),
             "search_ls_id", search_ctx_.get_ls_id());
  } else if (!use_injected_tsc_service && OB_NOT_NULL(partition)
             && OB_FAIL(partition->get_pinned_index_tablet_handle(
                    inv_idx_tablet_id_, pinned_tablet_handle))) {
    LOG_WARN("failed to get pinned async index tablet handle", K(ret),
             K_(inv_idx_tablet_id));
  } else if (OB_FAIL(tsc_service_->async_index_posting_scan(
      search_ctx_.get_ls_id(),
      pinned_tablet_handle,
      version_range_,
      inv_idx_scan_param_,
      posting_scan_param_,
      posting_iter_))) {
    LOG_WARN("failed to async index posting scan", K(ret), K_(inv_idx_tablet_id), K_(version_range));
  }
  return ret;
}

void ObDASAsyncTokenHelper::close_posting_iter()
{
  if (nullptr != posting_iter_) {
    posting_iter_->~ObInvSSTablePostingIter();
    posting_iter_allocator_.free(posting_iter_);
    posting_iter_ = nullptr;
  }
  // free() on arena is a no-op; reset() returns pages so SQL mem-leak check is clean.
  posting_iter_allocator_.reset();
}

int ObDASAsyncTokenHelper::open()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(init_inv_idx_scan_param())) {
    LOG_WARN("failed to init inv idx scan param", K(ret));
  } else if (direct_bind_ && OB_FAIL(search_ctx_.resolve_direct_async_index_segment(
                 index_table_id_, inv_idx_tablet_id_, group_slot_,
                 version_range_, is_empty_segment_))) {
    LOG_WARN("failed to resolve direct async token helper segment", K(ret),
             K_(index_table_id), K_(group_slot));
  } else if (is_empty_segment_) {
    // No physical input in this partition and Group.
    dim_max_score_ = 0.0;
  } else if (OB_FAIL(init_posting_scan_param())) {
    LOG_WARN("failed to init posting scan param", K(ret));
  } else if (OB_FAIL(init_bm25_param_estimator())) {
    LOG_WARN("failed to init bm25 param estimator", K(ret));
  } else if (OB_FAIL(param_estimator_.get_bm25_params(
             posting_scan_param_.total_doc_cnt_,
             posting_scan_param_.avg_doc_length_,
             posting_scan_param_.doc_freq_))) {
    LOG_WARN("failed to get bm25 params", K(ret));
  } else if (!direct_bind_) {
    version_range_.reset();
    // The owning SegmentMerge calls rescan_segment() to bind a physical Segment for a
    // non-direct Helper.
    LOG_INFO("[ASYNC_INDEX_QUERY] token_helper open(lazy)", K_(inv_idx_tablet_id));
  } else if (OB_FAIL(open_posting_iter_from_tablet())) {
    LOG_WARN("failed to open posting iter", K(ret));
  } else {
    dim_max_score_ = ObExprBM25::max_term_bm25_score(
                     posting_scan_param_.doc_freq_, posting_scan_param_.total_doc_cnt_);
  }
  if (OB_FAIL(ret)) {
    close_posting_iter();
  }
  return ret;
}

int ObDASAsyncTokenHelper::rescan()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    close_posting_iter();
    version_range_.reset();
    posting_scan_param_.reset();
    param_estimator_.reset();
    if (OB_FAIL(init_inv_idx_scan_param())) {
      LOG_WARN("failed to init inv idx scan param on rescan", K(ret));
    } else if (direct_bind_ && OB_FAIL(search_ctx_.resolve_direct_async_index_segment(
                   index_table_id_, inv_idx_tablet_id_, group_slot_,
                   version_range_, is_empty_segment_))) {
      LOG_WARN("failed to resolve direct async token helper segment on rescan", K(ret),
               K_(index_table_id), K_(group_slot));
    } else if (is_empty_segment_) {
      // No physical input in this partition and Group.
      dim_max_score_ = 0.0;
    } else if (OB_FAIL(init_posting_scan_param())) {
      LOG_WARN("failed to init posting scan param on rescan", K(ret));
    } else if (OB_FAIL(init_bm25_param_estimator())) {
      LOG_WARN("failed to init bm25 param estimator on rescan", K(ret));
    } else if (OB_FAIL(param_estimator_.get_bm25_params(
               posting_scan_param_.total_doc_cnt_,
               posting_scan_param_.avg_doc_length_,
               posting_scan_param_.doc_freq_))) {
      LOG_WARN("failed to get bm25 params on rescan", K(ret));
    } else if (!direct_bind_) {
      LOG_INFO("[ASYNC_INDEX_QUERY] token_helper rescan(lazy)", K_(inv_idx_tablet_id));
    } else if (OB_FAIL(open_posting_iter_from_tablet())) {
      LOG_WARN("failed to open posting iter on rescan", K(ret));
    } else {
      dim_max_score_ = ObExprBM25::max_term_bm25_score(
                       posting_scan_param_.doc_freq_, posting_scan_param_.total_doc_cnt_);
    }
    if (OB_FAIL(ret)) {
      close_posting_iter();
    }
  }
  return ret;
}

int ObDASAsyncTokenHelper::rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(direct_bind_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("direct async token helper rejects segment rescan", K(ret),
             K_(index_table_id), K_(group_slot), K(range));
  } else {
    close_posting_iter();
    version_range_ = range;
    if (OB_FAIL(init_posting_scan_param())) {
      LOG_WARN("failed to init posting scan param on segment switch", K(ret));
    } else if (OB_FAIL(param_estimator_.get_bm25_params(
               posting_scan_param_.total_doc_cnt_,
               posting_scan_param_.avg_doc_length_,
               posting_scan_param_.doc_freq_))) {
      LOG_WARN("failed to get bm25 params on segment switch", K(ret));
    } else if (OB_FAIL(open_posting_iter_from_tablet())) {
      LOG_WARN("failed to open posting iter on segment switch", K(ret));
    } else {
      dim_max_score_ = ObExprBM25::max_term_bm25_score(
                       posting_scan_param_.doc_freq_, posting_scan_param_.total_doc_cnt_);
    }
    if (OB_FAIL(ret)) {
      close_posting_iter();
    }
  }
  return ret;
}

int ObDASAsyncTokenHelper::close()
{
  int ret = OB_SUCCESS;
  close_posting_iter();
  param_estimator_.reset();
  posting_scan_param_.reset();
  inv_idx_scan_param_.destroy_schema_guard();
  inv_idx_scan_param_.snapshot_.reset();
  inv_idx_scan_param_.destroy();
  inv_idx_scan_param_inited_ = false;
  tsc_service_ = nullptr;
  version_range_.reset();
  index_table_id_ = common::OB_INVALID_ID;
  group_slot_ = -1;
  direct_bind_ = false;
  is_empty_segment_ = false;
  term_key_.reset();
  term_datums_ = nullptr;
  query_token_.reset();
  arena_allocator_.reset();
  is_inited_ = false;
  return ret;
}

void ObDASAsyncTokenHelper::reuse()
{
  close_posting_iter();
}

int ObDASAsyncTokenHelper::get_next_row()
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_UNLIKELY(posting_iter_->in_shallow_status())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("next_row after advance_shallow is not supported", K(ret));
  } else if (OB_FAIL(posting_iter_->next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to next posting iter", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncTokenHelper::advance_to(const common::ObDatum &id_datum)
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->advance_to(id_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance posting iter", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncTokenHelper::advance_shallow(const common::ObDatum &id_datum, const bool inclusive)
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->advance_shallow(id_datum, inclusive))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to shallow advance posting iter", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncTokenHelper::get_curr_phrase_block_max_info(
    const storage::ObMaxScoreTuple *&tuple) const
{
  int ret = OB_SUCCESS;
  tuple = nullptr;
  double max_ranking = 0.0;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->get_curr_phrase_block_max_ranking(max_ranking))) {
    if (OB_NOT_SUPPORTED != ret) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get phrase block max ranking", K(ret));
    }
  } else {
    phrase_block_max_tuple_.max_score_ = max_ranking;
    phrase_block_max_tuple_.min_domain_id_ = nullptr;
    phrase_block_max_tuple_.max_domain_id_ = nullptr;
    tuple = &phrase_block_max_tuple_;
  }
  return ret;
}

int ObDASAsyncTokenHelper::get_block_id_range(uint64_t &min_id, uint64_t &max_id) const
{
  int ret = OB_SUCCESS;
  double max_score = 0.0;
  min_id = 0;
  max_id = 0;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->get_block_max_tuple(min_id, max_id, max_score))) {
    // Iter fills min/max even when block max score is absent; id-range callers ignore score.
    if (OB_NOT_SUPPORTED == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get block id range", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncTokenHelper::get_curr_bm25_param(
    int64_t &token_freq,
    int64_t &doc_length) const
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->get_curr_bm25_param(token_freq, doc_length))) {
    LOG_WARN("failed to get curr bm25 param from posting iter", K(ret));
  }
  return ret;
}

int ObDASAsyncTokenHelper::get_curr_id(const common::ObDatum *&id_datum) const
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->get_cur_id(id_datum))) {
    LOG_WARN("failed to get cur id from posting iter", K(ret));
  }
  return ret;
}

int ObDASAsyncTokenHelper::get_curr_pos_list(common::ObString &pos_list) const
{
  int ret = OB_SUCCESS;
  const common::ObDatum *value_datum = nullptr;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->get_cur_pos_list(value_datum))) {
    LOG_WARN("failed to get position list value from posting iter", K(ret),
             "need_posting_value", posting_scan_param_.need_posting_value_,
             "token_len", query_token_.length());
  } else {
    pos_list = value_datum->get_string();
  }
  return ret;
}

int ObDASAsyncTokenHelper::get_max_token_relevance(double &max_token_relevance) const
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    max_token_relevance = 0.0;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else {
    max_token_relevance = dim_max_score_ * token_boost_;
  }
  return ret;
}

int ObDASAsyncTokenHelper::get_bm25_param(uint64_t &total_doc_cnt, double &avg_doc_length) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    total_doc_cnt = posting_scan_param_.total_doc_cnt_;
    avg_doc_length = posting_scan_param_.avg_doc_length_;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
