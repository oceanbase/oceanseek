/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_DAS
#include <algorithm>
#include "ob_das_async_token_op.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"
#include "sql/das/search/ob_das_bm25_batch_kernel.h"
#include "sql/das/search/ob_das_sparse_candidate_buffer.h"
#include "sql/das/search/ob_das_async_segment_merge_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "storage/tx_storage/ob_access_service.h"

namespace oceanbase
{
namespace sql
{

ObDASAsyncTokenOpParam::ObDASAsyncTokenOpParam()
  : ObIDASSearchOpParam(DAS_SEARCH_OP_ASYNC_TOKEN),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    version_range_(),
    index_table_id_(common::OB_INVALID_ID),
    group_slot_(-1),
    direct_bind_(false),
    token_boost_(0.0),
    query_token_()
{}

bool ObDASAsyncTokenOpParam::is_valid() const
{
  return nullptr != ir_ctdef_
      && nullptr != ir_rtdef_
      && common::OB_INVALID_ID != index_table_id_
      && group_slot_ >= -1
      && (!direct_bind_ || group_slot_ >= 0)
      && token_boost_ > 0.0
      && !query_token_.empty();
}

ObDASAsyncTokenOp::ObDASAsyncTokenOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    arena_allocator_(common::ObMemAttr(MTL_ID(), "DASAsyncTokenOp")),
    posting_iter_allocator_(common::ObMemAttr(MTL_ID(), "DASATokIter")),
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
    posting_entry_(nullptr),
    posting_scan_param_(),
    term_datums_(nullptr),
    term_key_(),
    query_token_(),
    param_estimator_(),
    query_token_weight_(0.0),
    dim_max_score_(0.0),
    token_boost_(0.0),
    bm25_calc_params_(),
    bm25_score_kernels_(),
    bm25_expansion_kernels_(),
    bm25_existing_kernels_(),
    estimated_work_row_count_(-1),
    inv_idx_scan_param_inited_(false),
    is_inited_(false)
{}

int ObDASAsyncTokenOp::do_init(const ObIDASSearchOpParam &op_param)
{
  int ret = OB_SUCCESS;
  const ObDASAsyncTokenOpParam &param = static_cast<const ObDASAsyncTokenOpParam &>(op_param);
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async token argument", K(ret), KP(param.ir_ctdef_),
        KP(param.ir_rtdef_), K(param.version_range_), K(param.index_table_id_),
        K(param.group_slot_), K(param.direct_bind_), K(param.token_boost_),
        "token_len", param.query_token_.length());
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != search_ctx_.get_rowid_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("async token op only supports uint64 rowid", K(ret),
        "rowid_type", search_ctx_.get_rowid_type());
  } else if (OB_FAIL(init_term_key(param.query_token_))) {
    LOG_WARN("failed to init term key", K(ret), "token_len", param.query_token_.length());
  } else {
    ir_ctdef_ = param.ir_ctdef_;
    ir_rtdef_ = param.ir_rtdef_;
    version_range_ = param.version_range_;
    index_table_id_ = param.index_table_id_;
    group_slot_ = param.group_slot_;
    direct_bind_ = param.direct_bind_;
    token_boost_ = param.token_boost_;
    is_inited_ = true;
  }
  return ret;
}

int ObDASAsyncTokenOp::init_term_key(const common::ObString &query_token)
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
    // share the same deep-copied buffer for the estimator's token key
    query_token_.assign_ptr(token_buf, static_cast<int32_t>(token_len));
    if (OB_FAIL(term_key_.assign(term_datums_, 1))) {
      LOG_WARN("failed to assign term key", K(ret));
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_open()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_FAIL(init_inv_idx_scan_param())) {
    LOG_WARN("failed to init inv idx scan param", K(ret));
  } else if (direct_bind_ && OB_FAIL(search_ctx_.resolve_direct_async_index_segment(
                 index_table_id_, inv_idx_tablet_id_, group_slot_,
                 version_range_, is_empty_segment_))) {
    LOG_WARN("failed to resolve direct async token segment", K(ret),
             K_(index_table_id), K_(group_slot));
  } else if (is_empty_segment_) {
    // No physical input in this partition and Group.
  } else if (OB_FAIL(init_posting_scan_param())) {
    LOG_WARN("failed to init posting scan param", K(ret));
  } else if (OB_FAIL(init_bm25_param_estimator())) {
    LOG_WARN("failed to init bm25 param estimator", K(ret));
  } else if (OB_FAIL(fill_async_bm25_param(posting_scan_param_))) {
    LOG_WARN("failed to fill async bm25 param", K(ret));
  } else if (!direct_bind_) {
    // The owning SegmentMerge actively calls rescan_segment to bind a physical Segment
    // for a non-direct Token.
    version_range_.reset();
    LOG_INFO("[ASYNC_INDEX_QUERY] token_op open(lazy)", K_(inv_idx_tablet_id));
  } else if (OB_FAIL(open_posting_iter_from_tablet())) {
    LOG_WARN("failed to open posting iter", K(ret));
  } else if (OB_FAIL(init_dim_max_score(dim_max_score_))) {
    LOG_WARN("failed to init dim max score", K(ret));
  }
  if (OB_FAIL(ret)) {
    close_posting_iter();
  }
  return ret;
}

int ObDASAsyncTokenOp::do_prepare_parallel_work_estimate()
{
  int ret = OB_SUCCESS;
  if (estimated_work_row_count_ >= 0) {
    // 复用之前执行或 rescan 已经准备好的估算值。
  } else if (OB_FAIL(estimate_work_row_count())) {
    estimated_work_row_count_ = -1;
    LOG_TRACE("[FUSION_TRACE] failed to estimate async token work rows for auto dop",
              KR(ret), "token_len", query_token_.length());
  } else {
    // 估算结果已缓存在 estimated_work_row_count_ 中。
  }
  return ret;
}

int ObDASAsyncTokenOp::estimate_work_row_count()
{
  int ret = OB_SUCCESS;
  const ObDASScalarScanCtDef *inv_idx_scan_ctdef = nullptr;
  ObTabletID estimate_tablet_id;
  storage::ObAsyncBM25ParamEstimator estimator;
  uint64_t total_doc_count = 0;
  uint64_t token_doc_count = 0;
  double avg_doc_token_count = 0.0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_TRACE("async token op is not initialized when estimating work rows",
              KR(ret), "token_len", query_token_.length());
  } else if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_)
             || OB_ISNULL(inv_idx_scan_ctdef =
                    ir_ctdef_->get_inv_idx_scan_scalar_ctdef())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_TRACE("unexpected null async token scan def when estimating work rows",
              KR(ret), KP(ir_ctdef_), KP(ir_rtdef_),
              KP(inv_idx_scan_ctdef), "token_len", query_token_.length());
  } else if (OB_FAIL(get_related_tablet_id(
                 inv_idx_scan_ctdef, estimate_tablet_id))) {
    LOG_TRACE("failed to get async token tablet when estimating work rows",
              KR(ret), "token_len", query_token_.length());
  } else if (OB_FAIL(estimator.init(
                 search_ctx_, *ir_ctdef_, *ir_rtdef_, estimate_tablet_id,
                 query_token_))) {
    LOG_TRACE("failed to init async token work estimator",
              KR(ret), K(estimate_tablet_id), "token_len", query_token_.length());
  } else if (OB_FAIL(estimator.get_bm25_params(
                 total_doc_count, avg_doc_token_count,
                 token_doc_count))) {
    LOG_TRACE("failed to estimate async token posting rows",
              KR(ret), K(estimate_tablet_id), "token_len", query_token_.length());
  } else {
    estimated_work_row_count_ =
        token_doc_count > static_cast<uint64_t>(INT64_MAX)
        ? INT64_MAX
        : static_cast<int64_t>(token_doc_count);
    LOG_TRACE("[FUSION_TRACE] estimated async token work rows for auto dop",
              "token_len", query_token_.length(), K(estimate_tablet_id),
              K(estimated_work_row_count_), K(total_doc_count),
              K(avg_doc_token_count));
  }
  return ret;
}

int ObDASAsyncTokenOp::init_inv_idx_scan_param()
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
    // Logical ranges select segments; SQL snapshot checks refresh transaction
    // visibility when locating their IncMajor SSTables.
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

int ObDASAsyncTokenOp::init_posting_scan_param()
{
  int ret = OB_SUCCESS;
  posting_scan_param_.reset();
  query_flag_.set_use_block_cache();
  posting_scan_param_.search_key_ = &term_key_;
  posting_scan_param_.tablet_id_ = inv_idx_tablet_id_;
  posting_scan_param_.query_flag_ = query_flag_;
  posting_scan_param_.allocator_ = &posting_iter_allocator_;
  if (search_ctx_.has_docid_range()) {
    if (!search_ctx_.get_docid_range_lo().is_min_value()) {
      posting_scan_param_.min_doc_id_ = search_ctx_.get_docid_range_lo().get_uint64();
    }
    if (!search_ctx_.get_docid_range_hi().is_max_value()) {
      posting_scan_param_.max_doc_id_ = search_ctx_.get_docid_range_hi().get_uint64();
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::fill_async_bm25_param(blocksstable::ObInvPostingScanParam &param)
{
  int ret = OB_SUCCESS;
  param.has_relevance_params_ = true;
  if (OB_FAIL(param_estimator_.get_bm25_params(param.total_doc_cnt_, param.avg_doc_length_, param.doc_freq_))) {
    LOG_WARN("failed to get bm25 params", K(ret));
  }
  return ret;
}

int ObDASAsyncTokenOp::init_bm25_param_estimator()
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

int ObDASAsyncTokenOp::init_dim_max_score(double &max_score)
{
  int ret = OB_SUCCESS;
  // The query token weight (IDF) doubles as the conservative term-dimension max score.
  query_token_weight_ = ObExprBM25::query_token_weight(
      posting_scan_param_.doc_freq_, posting_scan_param_.total_doc_cnt_);
  max_score = query_token_weight_;
  // Precompute the boosted per-document scoring constants for fetch_curr()/do_drain_until().
  if (OB_FAIL(ObExprBM25::init_precomputed_params(
      query_token_weight_, posting_scan_param_.avg_doc_length_, token_boost_, bm25_calc_params_))) {
    LOG_WARN("failed to init bm25 precomputed params", K(ret),
        K_(query_token_weight), K(posting_scan_param_.avg_doc_length_), K_(token_boost));
  } else {
    bm25_score_kernels_ = get_bm25_score_kernels();
    bm25_expansion_kernels_ = get_bm25_accumulation_kernels<false>();
    bm25_existing_kernels_ = get_bm25_accumulation_kernels<true>();
  }
  return ret;
}

int ObDASAsyncTokenOp::open_posting_iter_from_tablet()
{
  int ret = OB_SUCCESS;
  const ObAsyncIndexPartitionSegCtx *partition = search_ctx_.get_current_async_partition();
  const storage::ObTabletHandle *pinned_tablet_handle = nullptr;
  storage::ObAccessService *mtl_tsc_service = MTL(storage::ObAccessService *);
  const bool use_injected_tsc_service =
      OB_NOT_NULL(tsc_service_) && tsc_service_ != mtl_tsc_service;
  // tsc_service_ may be pre-injected by tests; otherwise fetch it from MTL.
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
  } else if (OB_FAIL(search_ctx_.prepare_posting_scan(
      !use_injected_tsc_service, posting_iter_allocator_,
      posting_scan_param_, posting_entry_))) {
    LOG_WARN("failed to acquire token posting entry", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tsc_service_->async_index_posting_scan(
        search_ctx_.get_ls_id(), pinned_tablet_handle, version_range_,
        inv_idx_scan_param_, posting_scan_param_, posting_iter_))) {
      LOG_WARN("failed to async index posting scan", K(ret),
          K_(inv_idx_tablet_id), K_(version_range));
    }
  }
  if (OB_FAIL(ret)) {
    close_posting_iter();
  }
  return ret;
}

void ObDASAsyncTokenOp::close_posting_iter()
{
  search_ctx_.close_posting_scan(
      posting_scan_param_, posting_iter_allocator_, posting_entry_, posting_iter_);
  // free() on arena is a no-op; reset() returns pages so SQL mem-leak check is clean.
  posting_iter_allocator_.reset();
}

int ObDASAsyncTokenOp::fetch_curr_id(ObDASRowID &curr_id)
{
  int ret = OB_SUCCESS;
  const common::ObDatum *id_datum = nullptr;
  if (OB_FAIL(posting_iter_->get_cur_id(id_datum))) {
    LOG_WARN("failed to get cur id from posting iter", K(ret));
  } else {
    // 只读取 posting 当前 doc-id，不访问 BM25 参数列。
    curr_id.set_uint64(id_datum->get_uint64());
  }
  return ret;
}

int ObDASAsyncTokenOp::fetch_curr(ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  int64_t token_freq = 0;
  int64_t doc_length = 0;
  // 复用只取 doc-id 的路径，再读取当前行 BM25 参数并计算分数。
  if (OB_FAIL(fetch_curr_id(curr_id))) {
    LOG_WARN("failed to get current posting id", K(ret));
  } else if (OB_FAIL(posting_iter_->get_curr_bm25_param(token_freq, doc_length))) {
    LOG_WARN("failed to get curr bm25 param from posting iter", K(ret));
  } else {
    score = ObExprBM25::eval_with_precomputed_params(
        static_cast<uint64_t>(token_freq), static_cast<uint64_t>(doc_length), bm25_calc_params_);
  }
  return ret;
}

int ObDASAsyncTokenOp::fetch_curr_on_demand(ObDASRowID &curr_id, double &score)
{
  int ret = OB_SUCCESS;
  const common::ObDatum *id_datum = nullptr;
  int64_t token_freq = 0;
  int64_t doc_length = 0;
  if (OB_FAIL(posting_iter_->get_cur_id(id_datum))) {
    LOG_WARN("failed to get cur id from posting iter", K(ret));
  } else if (OB_FAIL(posting_iter_->load_curr_bm25_param(token_freq, doc_length))) {
    LOG_WARN("failed to load curr bm25 param from posting iter", K(ret));
  } else {
    score = ObExprBM25::eval_with_precomputed_params(
        static_cast<uint64_t>(token_freq), static_cast<uint64_t>(doc_length), bm25_calc_params_);
    curr_id.set_uint64(id_datum->get_uint64());
  }
  return ret;
}

int ObDASAsyncTokenOp::do_next_rowid(ObDASRowID &next_id, double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_UNLIKELY(posting_iter_->in_shallow_status())) {
    // next() right after advance_shallow() is forbidden; callers must advance_to() first.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("next_rowid after advance_shallow is not supported", K(ret));
  } else if (OB_FAIL(posting_iter_->next())) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to next posting iter", K(ret));
    }
  } else if (OB_FAIL(fetch_curr(next_id, score))) {
    LOG_WARN("failed to fetch curr row", K(ret));
  }
  return ret;
}

int ObDASAsyncTokenOp::do_drain_until(
    const ObDASRowID &boundary,
    int64_t capacity,
    uint64_t *out_ids,
    double *out_scores,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  if (is_empty_segment_) {
    next_id_after.set_max();
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_ISNULL(out_ids) || OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drain_until arguments", K(ret), KP(out_ids), K(capacity));
  } else {
    // is_max boundary means "no upper bound": drain the whole posting list.
    const uint64_t boundary_u64 = boundary.is_max() ? UINT64_MAX : boundary.get_uint64();
    const BM25ScoreKernels &score_kernels = bm25_score_kernels_;
    bool done = false;
    while (OB_SUCC(ret) && count < capacity && !done) {
      blocksstable::ObPostingDocIdView doc_ids;
      blocksstable::ObPostingRelevanceView relevance;
      int64_t n = 0;
      if (OB_FAIL(posting_iter_->drain_until_typed(
          boundary_u64, capacity - count, doc_ids, relevance, n))) {
        LOG_WARN("failed to drain posting iter", K(ret), K(boundary_u64), K(capacity), K(count));
      } else if (n > 0) {
        // Copy the doc ids for the downstream sink; score the run in place from the
        // projected token_freq/doc_length columns via the shared BM25 batch kernel.
        if (doc_ids.is_uint32()) {
          const uint32_t *ids = doc_ids.get_uint32();
          for (int64_t i = 0; i < n; ++i) { out_ids[count + i] = ids[i]; }
        } else {
          MEMCPY(out_ids + count, doc_ids.get_uint64(), n * sizeof(uint64_t));
        }
        if (nullptr != out_scores) {
          score_kernels.score(relevance, n, bm25_calc_params_, out_scores + count);
        }
        count += n;
      } else if (posting_iter_->is_end()) {
        next_id_after.set_max();
        done = true;
      } else {
        // Boundary reached: the cursor is on the first row > boundary. Report it (its
        // score is needed even when out_scores is null, as the caller caches it).
        if (OB_FAIL(fetch_curr_on_demand(next_id_after, next_score_after))) {
          LOG_WARN("failed to fetch prefetched boundary row", K(ret));
        } else {
          done = true;
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::drain_until_accumulated(
    const ObDASRowID &boundary,
    DocScoreAccumulator &accumulator,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  if (OB_UNLIKELY(!accumulator.is_valid() || accumulator.has_filter_mask() || boundary.is_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid accumulated drain arguments", K(ret), K(accumulator), K(boundary));
  } else if (OB_UNLIKELY(DAS_ROWID_TYPE_UINT64 != get_rowid_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("accumulated drain only supports uint64 rowid type", K(ret), K(get_rowid_type()));
  } else if (OB_LIKELY(nullptr == profile_)) {
    if (OB_FAIL(do_drain_until_accumulated(
        boundary, accumulator, count, next_id_after, next_score_after))) {
      LOG_WARN("failed to accumulate drained rows", KR(ret));
    }
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
    if (OB_FAIL(do_drain_until_accumulated(
        boundary, accumulator, count, next_id_after, next_score_after))) {
      LOG_WARN("failed to accumulate drained rows", KR(ret));
    } else {
      int64_t output_count = count;
      if (next_id_after.is_normal()) {
        output_count += is_new_profile_output_row(next_id_after) ? 1 : 0;
      }
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, output_count);
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_drain_until_accumulated(
    const ObDASRowID &boundary,
    DocScoreAccumulator &accumulator,
    int64_t &count,
    ObDASRowID &next_id_after,
    double &next_score_after)
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    count = 0;
    next_score_after = 0.0;
    next_id_after.set_max();
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else {
    static constexpr int64_t DRAIN_BATCH_SIZE = 1024;
    const uint64_t boundary_u64 = boundary.is_max() ? UINT64_MAX : boundary.get_uint64();
    const BM25AccumulationKernels<false> &kernels = bm25_expansion_kernels_;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      blocksstable::ObPostingDocIdView doc_ids;
      blocksstable::ObPostingRelevanceView relevance;
      int64_t n = 0;
      bool finished = false;
      if (OB_FAIL(posting_iter_->drain_until_typed_run(
          boundary_u64, DRAIN_BATCH_SIZE, doc_ids, relevance, n, finished))) {
        LOG_WARN("failed to drain posting iter into accumulator", K(ret), K(boundary_u64));
      } else if (n > 0) {
        int64_t matched = 0;
        if (OB_FAIL(accumulator.add_scored_dim_batch_fused(
                doc_ids, relevance, n, bm25_calc_params_, kernels, matched))) {
          LOG_WARN("failed to accumulate scored posting batch", K(ret), K(n));
        } else if (OB_LIKELY(nullptr == profile_)) {
          count += matched;
        } else {
          for (int64_t i = 0; i < n; ++i) {
            if (is_new_profile_output_row(doc_ids.at(i))) {
              ++count;
            }
          }
        }
      }
      // A positive run may already have a same-pin lookahead. Consume its views
      // above before materializing that lookahead's score or making another call.
      if (OB_SUCC(ret) && finished) {
        if (posting_iter_->is_end()) {
          next_id_after.set_max();
          done = true;
        } else if (OB_FAIL(fetch_curr_on_demand(next_id_after, next_score_after))) {
          LOG_WARN("failed to fetch row after accumulated drain boundary", K(ret));
        } else {
          if (!is_scoring_) {
            next_score_after = 0.0;
          }
          done = true;
        }
      }
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::score_candidates(
    SparseCandidateBuffer &candidates, ObDASRowID &next_id_after, double &next_score_after)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_LIKELY(nullptr == profile_)) {
    if (OB_FAIL(do_drain_into_candidates(candidates, count, next_id_after, next_score_after))) {
      LOG_WARN("failed to drain into compact candidates", K(ret));
    }
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_NEXT_ROWIDS_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_NEXT_ROWIDS_COUNT, 1);
    if (OB_FAIL(do_drain_into_candidates(candidates, count, next_id_after, next_score_after))) {
      LOG_WARN("failed to drain into compact candidates", K(ret));
    } else {
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, count);
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_drain_into_candidates(
    SparseCandidateBuffer &candidates, int64_t &count,
    ObDASRowID &next_id_after, double &next_score_after)
{
  int ret = OB_SUCCESS;
  count = 0;
  next_id_after.set_max();
  next_score_after = 0.0;
  if (is_empty_segment_) {
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iterator not opened", K(ret));
  } else if (OB_UNLIKELY(candidates.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty candidate drain", K(ret));
  } else {
    const uint64_t *candidate_ids = candidates.get_doc_ids();
    const int64_t candidate_count = candidates.get_count();
    const uint64_t boundary = candidate_ids[candidate_count - 1];
    const uint64_t start_id = candidate_ids[0];
    uint64_t curr_id = 0;
    bool done = false;
    bool first_run = true;
    int64_t slot = 0;
    int64_t profile_count = 0;
    if (OB_FAIL(posting_iter_->advance_to_id(start_id, curr_id))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        done = true;
      } else {
        LOG_WARN("failed to position posting candidate drain", K(ret), K(start_id));
      }
    }
    while (OB_SUCC(ret) && !done) {
      blocksstable::ObPostingDocIdView doc_ids;
      blocksstable::ObPostingRelevanceView relevance;
      int64_t n = 0;
      bool finished = false;
      if (OB_FAIL(posting_iter_->drain_until_typed_run(
          boundary, DAS_SEARCH_ROWID_DRAIN_BATCH_SIZE, doc_ids, relevance, n, finished, first_run))) {
        LOG_WARN("failed to drain typed posting run into candidates", K(ret), K(boundary));
      } else if (n > 0) {
        struct BM25ScoreAt
        {
          const blocksstable::ObPostingRelevanceView &relevance_;
          const ObBM25PrecomputedParams &params_;
          const bool is_scoring_;
          const blocksstable::ObPostingDocIdView &doc_ids_;
          ObDASAsyncTokenOp *profile_op_;
          int64_t &profile_count_;
          OB_INLINE double operator()(const int64_t i) const
          {
            double score = 0.0;
            if (is_scoring_) {
              score = relevance_.is_uint32()
                  ? ObExprBM25::eval_with_precomputed_params(
                      relevance_.get_token_freqs32()[i], relevance_.get_doc_lengths32()[i], params_)
                  : ObExprBM25::eval_with_precomputed_params(
                      relevance_.get_token_freqs64()[i], relevance_.get_doc_lengths64()[i], params_);
            }
            if (nullptr != profile_op_ && profile_op_->is_new_profile_output_row(doc_ids_.at(i))) {
              ++profile_count_;
            }
            return score;
          }
        };
        const BM25ScoreAt scorer{relevance, bm25_calc_params_, is_scoring_, doc_ids,
            nullptr == profile_ ? nullptr : this, profile_count};
        if (OB_FAIL(doc_ids.is_uint32()
            ? candidates.add_scored_dim_batch(doc_ids.get_uint32(), n, scorer, slot, count)
            : candidates.add_scored_dim_batch(doc_ids.get_uint64(), n, scorer, slot, count))) {
          LOG_WARN("failed to supplement compact candidate batch", K(ret), K(n));
        }
      }
      // Consume the borrowed views before materializing a same-block lookahead.
      if (OB_SUCC(ret) && finished) {
        done = true;
        if (!posting_iter_->is_end()
            && OB_FAIL(fetch_curr_on_demand(next_id_after, next_score_after))) {
          LOG_WARN("failed to fetch candidate drain lookahead", K(ret));
        }
      }
      first_run = false;
    }
    if (!is_scoring_) {
      next_score_after = 0.0;
    }
    if (nullptr != profile_) {
      count = profile_count;
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::get_active_posting_prefix_count(
    const blocksstable::ObPostingDocIdView &doc_ids, const int64_t posting_count,
    const DocScoreAccumulator &accumulator, int64_t &scoring_count) const
{
  int ret = OB_SUCCESS;
  scoring_count = 0;
  const uint64_t window_start_id = accumulator.get_start_id();
  const int64_t window_capacity = accumulator.get_capacity();
  if (posting_count <= 0) {
  } else if (OB_UNLIKELY(!doc_ids.is_valid() || !accumulator.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid active posting run", K(ret), K(posting_count));
  } else if (OB_UNLIKELY(doc_ids.at(0) < window_start_id
      || doc_ids.at(posting_count - 1) - window_start_id >= static_cast<uint64_t>(window_capacity))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("active posting run is outside accumulator window", K(ret), K(posting_count),
        K(doc_ids.at(0)), K(doc_ids.at(posting_count - 1)), K(window_start_id), K(window_capacity));
  } else {
    const uint64_t *words = accumulator.get_bitset_words();
    const int64_t first_offset = static_cast<int64_t>(doc_ids.at(0) - window_start_id);
    const int64_t end_offset = static_cast<int64_t>(doc_ids.at(posting_count - 1) - window_start_id);
    const int64_t first_word = das_bitset_helper::word_index(first_offset);
    int64_t last_word = das_bitset_helper::word_index(end_offset);
    uint64_t word = words[last_word] & das_bitset_helper::low_bits_mask(
        das_bitset_helper::bit_index(end_offset) + 1);
    while (0 == word && last_word > first_word) {
      word = words[--last_word];
    }
    if (last_word == first_word) {
      word = das_bitset_helper::clear_bits_below(word, das_bitset_helper::bit_index(first_offset));
    }
    if (0 != word) {
      const int64_t last_active_offset = last_word * das_bitset_helper::WORD_BITS
          + das_bitset_helper::highest_set_bit(word);
      const uint64_t last_active_id = window_start_id + last_active_offset;
      if (last_active_offset == end_offset) {
        scoring_count = posting_count;
      } else if (doc_ids.is_uint32()) {
        const uint32_t *ids = doc_ids.get_uint32();
        scoring_count = std::upper_bound(ids, ids + posting_count, last_active_id) - ids;
      } else {
        const uint64_t *ids = doc_ids.get_uint64();
        scoring_count = std::upper_bound(ids, ids + posting_count, last_active_id) - ids;
      }
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_drain_until_active(
    const uint64_t range_start,
    const uint64_t range_end,
    DocScoreAccumulator &accumulator,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (is_empty_segment_) {
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else {
    const int64_t active_capacity = accumulator.get_capacity();
    const BM25AccumulationKernels<true> &kernels = bm25_existing_kernels_;
    bool done = false;
    bool first_run = true;
    ObDASRowID start_id;
    common::ObDatum start_datum;
    // Keep the accumulator base for bitmap offsets; only narrow this iterator's drain.
    start_id.set_uint64(range_start);
    // Position by doc id only. The first BM25 score is computed together with the
    // projected run instead of being materialized by the scalar advance path.
    if (OB_FAIL(search_ctx_.get_datum_from_rowid(start_id, start_datum))) {
      LOG_WARN("failed to get datum from active start id", K(ret), K(range_start));
    } else if (OB_FAIL(posting_iter_->advance_to_id(start_datum))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        done = true;
      } else {
        LOG_WARN("failed to position posting iter for active drain", K(ret), K(range_start));
      }
    }
    while (OB_SUCC(ret) && !done) {
      blocksstable::ObPostingDocIdView doc_ids;
      blocksstable::ObPostingRelevanceView relevance;
      int64_t posting_count = 0;
      int64_t scoring_count = 0;
      int64_t matched = 0;
      bool finished = false;
      if (OB_FAIL(posting_iter_->drain_until_typed_run(
          range_end, active_capacity, doc_ids, relevance, posting_count, finished, first_run))) {
        LOG_WARN("failed to drain posting iter with active mask", K(ret),
            K(range_end), K(active_capacity), K(count));
      } else if (posting_count <= 0) {
        done = true;
      } else if (OB_FAIL(get_active_posting_prefix_count(doc_ids, posting_count, accumulator, scoring_count))) {
        LOG_WARN("failed to clip active posting run", K(ret), K(posting_count));
      } else if (scoring_count > 0 && OB_FAIL(accumulator.add_scored_dim_batch_fused(
          doc_ids, relevance, scoring_count, bm25_calc_params_, kernels, matched))) {
        LOG_WARN("failed to accumulate active posting batch", K(ret), K(scoring_count));
      } else if (OB_LIKELY(nullptr == profile_)) {
        count += matched;
      } else {
        const uint64_t start_id = accumulator.get_start_id();
        const uint64_t *active_words = accumulator.get_bitset_words();
        for (int64_t i = 0; i < scoring_count; ++i) {
          const uint64_t doc_id = doc_ids.at(i);
          const uint64_t offset = doc_id - start_id;
          if (0 != (active_words[offset >> 6] & (1ULL << (offset & 63)))
              && is_new_profile_output_row(doc_id)) {
            ++count;
          }
        }
      }
      if (OB_SUCC(ret) && finished) {
        done = true;
      }
      first_run = false;
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_advance_to(
    const ObDASRowID &target,
    ObDASRowID &curr_id,
    double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  common::ObDatum target_datum;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from rowid", K(ret), K(target));
  } else if (OB_FAIL(posting_iter_->advance_to(target_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance posting iter", K(ret), K(target));
    }
  } else if (OB_FAIL(fetch_curr(curr_id, score))) {
    LOG_WARN("failed to fetch curr row", K(ret));
  }
  return ret;
}

int ObDASAsyncTokenOp::do_advance_to_id(
    const ObDASRowID &target,
    ObDASRowID &curr_id)
{
  int ret = OB_SUCCESS;
  common::ObDatum target_datum;
  const common::ObDatum *id_datum = nullptr;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from rowid", K(ret), K(target));
  } else if (OB_FAIL(posting_iter_->advance_to_id(target_datum))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance posting iter to id", K(ret), K(target));
    }
  } else if (OB_FAIL(posting_iter_->get_cur_id(id_datum))) {
    LOG_WARN("failed to get cur id from posting iter", K(ret));
  } else {
    curr_id.set_uint64(id_datum->get_uint64());
  }
  return ret;
}

int ObDASAsyncTokenOp::advance_to_id_uint64(
    const uint64_t target,
    uint64_t &curr_id)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr == profile_)) {
    ret = do_advance_to_id_uint64(target, curr_id);
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_ADVANCE_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_ADVANCE_COUNT, 1);
    ret = do_advance_to_id_uint64(target, curr_id);
    if (OB_SUCC(ret) && is_new_profile_output_row(curr_id)) {
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, 1);
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN_IGNORE_ITER_END(ret, "failed to advance to uint64 id impl", KR(ret), K(target));
  }
  return ret;
}

int ObDASAsyncTokenOp::do_advance_to_id_uint64(
    const uint64_t target,
    uint64_t &curr_id)
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->advance_to_id(target, curr_id))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to advance posting iter to uint64 id", K(ret), K(target));
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::probe_sorted_ids(
    const uint64_t *ids, const int64_t count, double *scores, bool *matched,
    ObDASRowID &last_id, double &last_score, bool &score_materialized, bool &iter_end)
{
  int ret = OB_SUCCESS;
  int64_t matched_count = 0;
  if (OB_LIKELY(nullptr == profile_)) {
    if (OB_FAIL(do_probe_sorted_ids(ids, count, scores, matched,
        last_id, last_score, score_materialized, iter_end, matched_count))) {
      LOG_WARN("failed to probe sorted token ids", K(ret), K(count));
    }
  } else {
    common::ObProfileSwitcher switcher(profile_);
    common::ScopedTimer total_timer(common::ObMetricId::HS_TOTAL_TIME);
    common::ScopedTimer timer(common::ObMetricId::HS_ADVANCE_TIME);
    INC_METRIC_VAL(common::ObMetricId::HS_ADVANCE_COUNT, 1);
    if (OB_FAIL(do_probe_sorted_ids(ids, count, scores, matched,
        last_id, last_score, score_materialized, iter_end, matched_count))) {
      LOG_WARN("failed to probe sorted token ids", K(ret), K(count));
    } else {
      INC_METRIC_VAL(common::ObMetricId::HS_OUTPUT_ROW_COUNT, matched_count);
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_probe_sorted_ids(
    const uint64_t *ids, const int64_t count, double *scores, bool *matched,
    ObDASRowID &last_id, double &last_score, bool &score_materialized, bool &iter_end,
    int64_t &matched_count)
{
  int ret = OB_SUCCESS;
  matched_count = 0;
  if (OB_UNLIKELY(!can_probe_sorted_ids())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async token cannot probe sorted ids", K(ret));
  } else if (OB_ISNULL(ids) || OB_ISNULL(scores) || OB_ISNULL(matched)
      || OB_UNLIKELY(count <= 0 || count > blocksstable::ObInvSSTablePostingIter::MAX_SORTED_PROBE_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sorted token probe arguments", K(ret), K(count));
  } else {
    uint64_t token_freqs[blocksstable::ObInvSSTablePostingIter::MAX_SORTED_PROBE_COUNT];
    uint64_t doc_lengths[blocksstable::ObInvSSTablePostingIter::MAX_SORTED_PROBE_COUNT];
    uint64_t match_bits = 0;
    uint64_t final_id = 0;
    if (OB_FAIL(posting_iter_->probe_sorted_ids(
        ids, count, token_freqs, doc_lengths, match_bits, final_id))) {
      LOG_WARN("failed to probe sorted posting ids", K(ret));
    } else {
      int64_t last_match = -1;
      while (match_bits != 0) {
        const int64_t c = __builtin_ctzll(match_bits);
        match_bits &= match_bits - 1;
        const double score = ObExprBM25::eval_with_precomputed_params(
            token_freqs[c], doc_lengths[c], bm25_calc_params_);
        scores[c] += score;
        matched[c] = true; // A zero score still denotes membership.
        if (OB_LIKELY(nullptr == profile_) || is_new_profile_output_row(ids[c])) {
          ++matched_count;
        }
        last_score = score;
        last_match = c;
      }
      last_id.set_uint64(final_id);
      iter_end = posting_iter_->is_end();
      score_materialized = !iter_end && last_match >= 0 && ids[last_match] == final_id;
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_load_curr_score(double &score)
{
  int ret = OB_SUCCESS;
  int64_t token_freq = 0;
  int64_t doc_length = 0;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(posting_iter_->load_curr_bm25_param(token_freq, doc_length))) {
    LOG_WARN("failed to load curr bm25 param", K(ret));
  } else {
    score = ObExprBM25::eval_with_precomputed_params(
        static_cast<uint64_t>(token_freq), static_cast<uint64_t>(doc_length), bm25_calc_params_);
  }
  return ret;
}

int ObDASAsyncTokenOp::do_advance_shallow(
    const ObDASRowID &target,
    const bool inclusive,
    const MaxScoreTuple *&max_score_tuple)
{
  int ret = OB_SUCCESS;
  max_score_tuple = nullptr;
  common::ObDatum target_datum;
  uint64_t min_id = 0;
  uint64_t max_id = 0;
  double block_max_score = 0.0;
  if (is_empty_segment_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(target, target_datum))) {
    LOG_WARN("failed to get datum from rowid", K(ret), K(target));
  } else if (OB_FAIL(posting_iter_->advance_shallow(target_datum, inclusive))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("failed to shallow advance posting iter", K(ret), K(target));
    }
  } else if (OB_FAIL(posting_iter_->get_block_max_tuple(min_id, max_id, block_max_score))) {
    LOG_WARN("failed to get block max tuple", K(ret));
  } else {
    ObDASRowID min_rowid;
    ObDASRowID max_rowid;
    min_rowid.set_uint64(min_id);
    max_rowid.set_uint64(max_id);
    max_score_tuple_.set(min_rowid, max_rowid, block_max_score * token_boost_);
    max_score_tuple = &max_score_tuple_;
  }
  return ret;
}

int ObDASAsyncTokenOp::do_calc_max_score(double &threshold)
{
  int ret = OB_SUCCESS;
  if (is_empty_segment_) {
    // Empty input contributes no score.
  } else if (OB_ISNULL(posting_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else {
    // Round the product before the sum to avoid FMA under Clang's default contraction.
    const double boosted_max_score = dim_max_score_ * token_boost_;
    threshold += boosted_max_score;
  }
  return ret;
}

// 查询指定 posting doc_id 区间的原始分数上界，并在成功路径应用当前 token boost。
int ObDASAsyncTokenOp::do_calc_range_max_score(
    const ObDASRowID &start_id,
    const bool start_inclusive,
    const ObDASRowID &end_id,
    double &max_score,
    bool &can_calc)
{
  int ret = OB_SUCCESS;
  can_calc = false;
  if (is_empty_segment_) {
    max_score = 0.0;
    can_calc = true;
  } else if (OB_ISNULL(posting_iter_)) {
    // posting iterator 尚未打开时返回未初始化错误。
    ret = OB_NOT_INIT;
    LOG_WARN("posting iter not opened", K(ret));
  } else if (OB_UNLIKELY(!start_id.is_normal()
      || (!end_id.is_normal() && !end_id.is_max()))) {
    // 起点必须为普通 RowID，终点仅允许普通 RowID 或 MAX 哨兵。
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range max boundary", K(ret), K(start_id), K(end_id));
  } else {
    const uint64_t end = end_id.is_max() ? UINT64_MAX : end_id.get_uint64();
    if (OB_FAIL(posting_iter_->get_range_max_score(
        start_id.get_uint64(), start_inclusive, end, max_score, can_calc))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to get posting range max score", K(ret),
          K(start_id), K(start_inclusive), K(end_id));
    } else if (can_calc) {
      max_score *= token_boost_;
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    close_posting_iter();
    version_range_.reset();
    posting_scan_param_.reset();
    param_estimator_.reset();
    // Drop the previous segment's scoring constants; re-inited by init_dim_max_score()
    // below (non-lazy) or by a later do_rescan_segment() (lazy).
    bm25_calc_params_.reset();
    bm25_score_kernels_ = {};
    bm25_expansion_kernels_ = {};
    bm25_existing_kernels_ = {};
    if (OB_FAIL(init_inv_idx_scan_param())) {
      LOG_WARN("failed to init inv idx scan param on rescan", K(ret));
    } else if (direct_bind_ && OB_FAIL(search_ctx_.resolve_direct_async_index_segment(
                   index_table_id_, inv_idx_tablet_id_, group_slot_,
                   version_range_, is_empty_segment_))) {
      LOG_WARN("failed to resolve direct async token segment on rescan", K(ret),
               K_(index_table_id), K_(group_slot));
    } else if (is_empty_segment_) {
      // No physical input in this partition and Group.
    } else if (OB_FAIL(init_posting_scan_param())) {
      LOG_WARN("failed to init posting scan param on rescan", K(ret));
    } else if (OB_FAIL(init_bm25_param_estimator())) {
      LOG_WARN("failed to init bm25 param estimator on rescan", K(ret));
    } else if (OB_FAIL(fill_async_bm25_param(posting_scan_param_))) {
      LOG_WARN("failed to fill async bm25 param on rescan", K(ret));
    } else if (!direct_bind_) {
      // Keep a normal rescan of a non-direct Token lazy; SegmentMerge binds it precisely
      // afterward.
      LOG_INFO("[ASYNC_INDEX_QUERY] token_op rescan(lazy)", K_(inv_idx_tablet_id));
    } else if (OB_FAIL(open_posting_iter_from_tablet())) {
      LOG_WARN("failed to open posting iter on rescan", K(ret));
    } else if (OB_FAIL(init_dim_max_score(dim_max_score_))) {
      LOG_WARN("failed to init dim max score on rescan", K(ret));
    }
    if (OB_FAIL(ret)) {
      close_posting_iter();
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_rescan_segment(const common::ObNewVersionRange &range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(direct_bind_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("direct async token rejects segment rescan", K(ret),
             K_(index_table_id), K_(group_slot), K(range));
  } else {
    close_posting_iter();
    version_range_ = range;
    bm25_calc_params_.reset();
    bm25_score_kernels_ = {};
    bm25_expansion_kernels_ = {};
    bm25_existing_kernels_ = {};
    if (OB_FAIL(init_posting_scan_param())) {
      LOG_WARN("failed to init posting scan param on segment switch", K(ret));
    } else if (OB_FAIL(fill_async_bm25_param(posting_scan_param_))) {
      LOG_WARN("failed to fill bm25 param on segment switch", K(ret));
    } else if (OB_FAIL(open_posting_iter_from_tablet())) {
      LOG_WARN("failed to open posting iter on segment switch", K(ret));
    } else if (OB_FAIL(init_dim_max_score(dim_max_score_))) {
      LOG_WARN("failed to init dim max score on segment switch", K(ret));
    }
    if (OB_FAIL(ret)) {
      close_posting_iter();
    }
  }
  return ret;
}

int ObDASAsyncTokenOp::do_close()
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
  query_token_weight_ = 0.0;
  bm25_calc_params_.reset();
  bm25_score_kernels_ = {};
  bm25_expansion_kernels_ = {};
  bm25_existing_kernels_ = {};
  estimated_work_row_count_ = -1;
  arena_allocator_.reset();
  is_inited_ = false;
  return ret;
}

} // namespace sql
} // namespace oceanbase
