/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <vector>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "mittest/mtlenv/storage/blocksstable/test_inv_idx_sstable_prepare.h"
#include "storage/access/ob_table_read_info.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_async_token_op.h"
#include "sql/das/search/ob_das_bmm_op.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"
#include "sql/das/search/ob_daat_merge_iter.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/expr/ob_expr_bm25.h"
#include "storage/tx_storage/ob_access_service.h"
#include "mittest/mtlenv/storage/async_index/test_das_async_index_binding.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{

namespace
{
void format_term(const int64_t seq, char *buf, const int64_t buf_len)
{
  snprintf(buf, buf_len, "term%010ld", static_cast<long>(seq));
}

void build_rowkey_read_info(
    const ObTableSchema &table_schema,
    common::ObIAllocator &allocator,
    ObTableReadInfo &read_info)
{
  common::ObSEArray<ObColDesc, 8> schema_cols;
  OK(table_schema.get_column_ids(schema_cols));
  OK(read_info.init(
      allocator,
      table_schema.get_column_count(),
      table_schema.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      schema_cols,
      nullptr /*storage_cols_index*/));
  ASSERT_TRUE(read_info.is_valid());
}

// Mock access service: its async_index_posting_scan delegates to an in-process sstable's
// scan_posting (mirroring ObLSTabletService::async_index_posting_scan) so the op can be
// driven through its real path without a real tablet. It also stands in for the tablet by
// supplying the rowkey read_info to scan_posting. set_table_handles is unneeded because the
// test owns the sstable lifetime.
class MockAsyncIndexAccessService : public storage::ObAccessService
{
public:
  int async_index_posting_scan(
      const share::ObLSID &/*ls_id*/,
      const storage::ObTabletHandle */*pinned_tablet_handle*/,
      const common::ObNewVersionRange &/*version_range*/,
      storage::ObTableScanParam &/*scan_param*/,
      blocksstable::ObInvPostingScanParam &posting_param,
      blocksstable::ObInvSSTablePostingIter *&iter) override
  {
    int ret = OB_SUCCESS;
    iter = nullptr;
    if (OB_ISNULL(inv_sstable_) || OB_ISNULL(read_info_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = inv_sstable_->scan_posting(posting_param, *read_info_, iter);
    }
    return ret;
  }
  ObInvSSTable *inv_sstable_ = nullptr;
  const storage::ObITableReadInfo *read_info_ = nullptr;
};

constexpr uint64_t OP_START = 1;
constexpr uint64_t OP_STRIDE = 2;
constexpr int64_t OP_DOC_CNT = 1000;
constexpr int64_t OP_TERM_SEQ = 1;
constexpr uint64_t ASYNC_INDEX_TABLE_ID = 300001;

struct RealBMMPostingRow
{
  RealBMMPostingRow(
      const int64_t term_seq,
      const uint64_t doc_id,
      const uint64_t token_freq = 1,
      const uint64_t doc_length = 100)
    : term_seq_(term_seq),
      doc_id_(doc_id),
      token_freq_(token_freq),
      doc_length_(doc_length)
  {}

  int64_t term_seq_;
  uint64_t doc_id_;
  uint64_t token_freq_;
  uint64_t doc_length_;
};

inline uint64_t expect_doc(const uint64_t start, const int64_t i)
{ return start + static_cast<uint64_t>(i) * OP_STRIDE; }

// Reference score via the exact single-division kernel the op / storage now use. This is
// the bit-for-bit oracle for op fast-path outputs (ASSERT_DOUBLE_EQ against it is exact).
inline double fast_ref_score(
    const uint64_t token_freq, const uint64_t doc_length,
    const int64_t doc_freq, const int64_t doc_cnt,
    const double avg_doc_length, const double boost)
{
  sql::ObBM25PrecomputedParams params;
  const double query_weight = sql::ObExprBM25::query_token_weight(doc_freq, doc_cnt);
  EXPECT_EQ(OB_SUCCESS,
      sql::ObExprBM25::init_precomputed_params(query_weight, avg_doc_length, boost, params));
  return sql::ObExprBM25::eval_with_precomputed_params(token_freq, doc_length, params);
}

// Dual-tolerance closeness between the fast single-division score and the canonical
// two-division ObExprBM25::eval() score. The two are algebraically equal but differ by a
// few ULP due to float reassociation, so we assert bounded absolute OR relative error.
// Thresholds are the plan's initial values, confirmed by the random differential test
// (drain_bm25_random_differential) which reports the observed max errors.
constexpr double kBM25AbsTol = 1e-15;
constexpr double kBM25RelTol = 1e-13;
inline ::testing::AssertionResult bm25_close(const double fast, const double canonical)
{
  const double abs_err = std::fabs(fast - canonical);
  const double rel_bound = kBM25RelTol * std::max(std::fabs(fast), std::fabs(canonical));
  if (abs_err <= kBM25AbsTol || abs_err <= rel_bound) {
    return ::testing::AssertionSuccess();
  }
  return ::testing::AssertionFailure()
      << "fast=" << fast << " canonical=" << canonical
      << " abs_err=" << abs_err << " abs_tol=" << kBM25AbsTol
      << " rel_bound=" << rel_bound;
}

// Minimal ObDASSearchCtx for the async token op. The op only needs rowid_type_ ==
// UINT64, get_datum_from_rowid() (static dispatch, no init() required) and
// get_row_count() (default cost -> mocked total_doc_cnt fallback). We deliberately do NOT
// call ObDASSearchCtx::init(), which would invoke access_service->estimate_row_count.
struct AsyncTokCtx
{
  explicit AsyncTokCtx(common::ObIAllocator &alloc)
    : binding_(),
      scan_alloc_("AsyncTokScanOp"),
      mock_scan_op_(scan_alloc_),
      search_ctx_(alloc, mock_scan_op_)
  {
    search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    EXPECT_EQ(OB_SUCCESS, binding_.init(
        search_ctx_,
        share::ObLSID(TestInvIdxSSTablePrepare::LS_ID),
        ASYNC_INDEX_TABLE_ID,
        common::ObTabletID(TestInvIdxSSTablePrepare::TABLET_ID),
        0,
        TestInvIdxSSTablePrepare::SNAPSHOT_VERSION));
  }
  MockDASAsyncIndexBinding binding_;
  ObArenaAllocator scan_alloc_;
  sql::ObDASScanOp mock_scan_op_;
  sql::ObDASSearchCtx search_ctx_;
};

class BMMVectorizedConfigGuard
{
public:
  BMMVectorizedConfigGuard()
    : tenant_config_(TENANT_CONF(MTL_ID())),
      old_value_(false),
      is_valid_(tenant_config_.is_valid())
  {
    if (is_valid_) {
      old_value_ = tenant_config_->_enable_bmm_vectorized_merge;
      tenant_config_->_enable_bmm_vectorized_merge = true;
    }
  }

  ~BMMVectorizedConfigGuard()
  {
    if (is_valid_) {
      tenant_config_->_enable_bmm_vectorized_merge = old_value_;
    }
  }

  bool is_valid() const { return is_valid_; }

private:
  omt::ObTenantConfigGuard tenant_config_;
  bool old_value_;
  bool is_valid_;
};
} // namespace

class TestDASAsyncTokenOp : public TestInvIdxSSTablePrepare
{
public:
  // Build an ObInvSSTable holding exactly one term whose posting list is
  // doc_id(i) = start + i*OP_STRIDE for i in [0, doc_cnt). token_freq=1, doc_length=100.
  void build_single_term_sstable(
      const uint64_t start,
      const int64_t doc_cnt,
      const int64_t term_seq,
      ObInvSSTable &inv_sstable)
  {
    ASSERT_GT(doc_cnt, 0);
    ASSERT_GT(OP_STRIDE, 0u);
    ASSERT_GT(start, 0u);
    const bool has_pos_list = true;

    prepare_inv_idx_schema(table_schema_, has_pos_list);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    prepare_desc();

    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());

    char term_buf[32] = {0};
    snprintf(term_buf, sizeof(term_buf), "term%010ld", static_cast<long>(term_seq));
    ObString term_str(static_cast<int32_t>(strlen(term_buf)), term_buf);
    char pos_list_buf[8];
    MEMSET(pos_list_buf, 'p', sizeof(pos_list_buf));
    ObString pos_list(static_cast<int32_t>(sizeof(pos_list_buf)), pos_list_buf);
    const uint64_t token_freq = 1;
    const uint64_t doc_length = 100;

    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    uint64_t min_doc_id = 0;
    uint64_t max_doc_id = 0;
    uint64_t total_token_freq = 0;
    for (int64_t i = 0; i < doc_cnt; ++i) {
      const uint64_t doc_id = start + static_cast<uint64_t>(i) * OP_STRIDE;
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
          token_freq, doc_length, pos_list));
      OK(writer.append_row(row));
      if (0 == i) {
        min_doc_id = doc_id;
      }
      max_doc_id = doc_id;
      total_token_freq += token_freq;
    }
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(doc_cnt, res.row_count_);

    ObInvIdxStatsMeta inv_meta_stats;
    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, inv_meta_stats);
    inv_meta_stats.min_doc_id_ = min_doc_id;
    inv_meta_stats.max_doc_id_ = max_doc_id;
    inv_meta_stats.total_doc_length_ = total_token_freq;
    ASSERT_EQ(OB_SUCCESS, res.inv_meta_.init(inv_meta_stats));
    ASSERT_TRUE(res.inv_meta_.is_valid());

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
  }

  // Same as build_single_term_sstable but with per-doc (token_freq, doc_length)
  // arrays. doc_id(i) = start + i*OP_STRIDE; the term string is fixed by term_seq.
  // Used to build sstables whose posting blocks carry heterogeneous BM25 params so
  // the block-max frontier (and thus advance_shallow's tight upper bound) is
  // exercised instead of the global dim-max fallback.
  void build_single_term_sstable_per_doc(
      const uint64_t start,
      const int64_t doc_cnt,
      const int64_t term_seq,
      const common::ObIArray<uint64_t> &token_freqs,
      const common::ObIArray<uint64_t> &doc_lengths,
      ObInvSSTable &inv_sstable)
  {
    ASSERT_GT(doc_cnt, 0);
    ASSERT_EQ(doc_cnt, token_freqs.count());
    ASSERT_EQ(doc_cnt, doc_lengths.count());
    ASSERT_GT(OP_STRIDE, 0u);
    ASSERT_GT(start, 0u);
    const bool has_pos_list = true;

    prepare_inv_idx_schema(table_schema_, has_pos_list);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    prepare_desc();

    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());

    char term_buf[32] = {0};
    snprintf(term_buf, sizeof(term_buf), "term%010ld", static_cast<long>(term_seq));
    ObString term_str(static_cast<int32_t>(strlen(term_buf)), term_buf);
    char pos_list_buf[8];
    MEMSET(pos_list_buf, 'p', sizeof(pos_list_buf));
    ObString pos_list(static_cast<int32_t>(sizeof(pos_list_buf)), pos_list_buf);

    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    uint64_t min_doc_id = 0;
    uint64_t max_doc_id = 0;
    uint64_t total_token_freq = 0;
    for (int64_t i = 0; i < doc_cnt; ++i) {
      const uint64_t doc_id = start + static_cast<uint64_t>(i) * OP_STRIDE;
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
          token_freqs.at(i), doc_lengths.at(i), pos_list));
      OK(writer.append_row(row));
      if (0 == i) {
        min_doc_id = doc_id;
      }
      max_doc_id = doc_id;
      total_token_freq += token_freqs.at(i);
    }
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(doc_cnt, res.row_count_);

    ObInvIdxStatsMeta inv_meta_stats;
    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, inv_meta_stats);
    inv_meta_stats.min_doc_id_ = min_doc_id;
    inv_meta_stats.max_doc_id_ = max_doc_id;
    inv_meta_stats.total_doc_length_ = total_token_freq;
    ASSERT_EQ(OB_SUCCESS, res.inv_meta_.init(inv_meta_stats));
    ASSERT_TRUE(res.inv_meta_.is_valid());

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
  }

  void build_multi_term_sstable(
      const std::vector<RealBMMPostingRow> &rows,
      ObInvSSTable &inv_sstable)
  {
    ASSERT_FALSE(rows.empty());
    const bool has_pos_list = true;
    prepare_inv_idx_schema(table_schema_, has_pos_list);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    prepare_desc();

    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));

    char term_buf[32] = {0};
    char pos_list_buf[8];
    MEMSET(pos_list_buf, 'p', sizeof(pos_list_buf));
    const ObString pos_list(static_cast<int32_t>(sizeof(pos_list_buf)), pos_list_buf);
    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    uint64_t min_doc_id = UINT64_MAX;
    uint64_t max_doc_id = 0;
    uint64_t total_token_freq = 0;
    for (const RealBMMPostingRow &posting : rows) {
      format_term(posting.term_seq_, term_buf, sizeof(term_buf));
      const ObString term_str(static_cast<int32_t>(strlen(term_buf)), term_buf);
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, posting.doc_id_,
          posting.token_freq_, posting.doc_length_, pos_list));
      OK(writer.append_row(row));
      min_doc_id = std::min(min_doc_id, posting.doc_id_);
      max_doc_id = std::max(max_doc_id, posting.doc_id_);
      total_token_freq += posting.token_freq_;
    }
    OK(writer.close());

    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(static_cast<int64_t>(rows.size()), res.row_count_);
    ObInvIdxStatsMeta inv_meta_stats;
    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, inv_meta_stats);
    inv_meta_stats.min_doc_id_ = min_doc_id;
    inv_meta_stats.max_doc_id_ = max_doc_id;
    inv_meta_stats.total_doc_length_ = total_token_freq;
    ASSERT_EQ(OB_SUCCESS, res.inv_meta_.init(inv_meta_stats));
    ASSERT_TRUE(res.inv_meta_.is_valid());

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
  }

  // Create + init the op, inject one sstable / read_info / tablet id, and PREFILL the bm25
  // param cache so the op's lazy estimator returns from cache without touching the access
  // service or a real tablet (the access-service path is covered by the estimate_row_count
  // direct tests + the ObTableEstimator branch). cached_doc_freq is the token doc frequency
  // the op should read back; avg is clamped to DEFAULT by the op.
  void create_and_inject_single(
      AsyncTokCtx &ctx,
      sql::ObDASIRScanCtDef &ir_ctdef,
      sql::ObDASIRScanRtDef &ir_rtdef,
      const common::ObString &query_token,
      const double token_boost,
      const int64_t cached_total_doc_cnt,
      const int64_t cached_doc_freq,
      const ObTableReadInfo &read_info,
      ObInvSSTable &inv_sstable,
      sql::ObDASAsyncTokenOp *&op,
      const double cached_avg = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT,
      const bool direct_bind = true,
      const bool prefill_cache = true,
      const bool is_scoring = true)
  {
    // The injected test path has no scalar child, but the scalar getter requires this mode bit.
    ir_ctdef.use_scalar_scan_def_ = true;
    sql::ObDASAsyncTokenOpParam param;
    param.ir_ctdef_ = &ir_ctdef;
    param.ir_rtdef_ = &ir_rtdef;
    param.version_range_.base_version_ = 0;
    param.version_range_.snapshot_version_ = SNAPSHOT_VERSION;
    param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
    param.group_slot_ = 0;
    param.direct_bind_ = direct_bind;
    param.token_boost_ = token_boost;
    param.query_token_ = query_token;
    param.set_is_scoring(is_scoring);
    op = nullptr;
    OK(ctx.search_ctx_.create_op(param, op));
    ASSERT_NE(nullptr, op);
    mock_access_.inv_sstable_ = &inv_sstable;
    mock_access_.read_info_ = &read_info;
    op->tsc_service_ = &mock_access_;
    op->inv_idx_tablet_id_ = common::ObTabletID(TABLET_ID);

    // prefill cache: token_doc_cnt (doc_freq), total_doc_cnt (by tablet), avg_doc_token_cnt (by tablet)
    if (prefill_cache) {
      storage::ObInvIdxParamCache &cache = ctx.search_ctx_.get_bm25_param_cache();
      OK(cache.set_token_doc_cnt(common::ObTabletID(TABLET_ID), query_token, cached_doc_freq));
      OK(cache.set_total_doc_cnt(
          common::ObTabletID(TABLET_ID), static_cast<uint64_t>(cached_total_doc_cnt)));
      OK(cache.set_avg_doc_token_cnt(common::ObTabletID(TABLET_ID), cached_avg));
    }
  }

  // Drive one op through a full public drain_until() loop (as ObDAATMergeIter would),
  // collecting every projected (id, score) up to `boundary_u64`, plus the terminal
  // signal (hit_end / next_id_after / next_score_after). `pre_next` rows are consumed
  // via next_rowid() first (to exercise scalar->batch interleaving). out_scores is
  // passed as null when with_scores is false.
  void run_drain(
      ObInvSSTable &inv_sstable, ObTableReadInfo &read_info,
      const common::ObString &query_token, const double token_boost,
      const int64_t total_doc_cnt, const int64_t doc_freq,
      const int64_t capacity, const uint64_t boundary_u64,
      const bool with_scores, const int64_t pre_next, const double cached_avg,
      std::vector<uint64_t> &ids, std::vector<double> &scores,
      bool &hit_end, uint64_t &next_id, double &next_score)
  {
    ids.clear();
    scores.clear();
    hit_end = false;
    next_id = 0;
    next_score = 0.0;
    ObArenaAllocator a("DrainCtx");
    AsyncTokCtx ctx(a);
    sql::ObDASIRScanCtDef ctdef(a);
    sql::ObDASIRScanRtDef rtdef;
    sql::ObDASAsyncTokenOp *op = nullptr;
    create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost, total_doc_cnt,
        doc_freq, read_info, inv_sstable, op, cached_avg);
    OK(op->open());
    for (int64_t k = 0; k < pre_next; ++k) {
      ObDASRowID rid;
      double s = 0.0;
      OK(op->next_rowid(rid, s));
    }
    ObDASRowID boundary;
    boundary.set_uint64(boundary_u64);
    std::vector<uint64_t> bi(capacity);
    std::vector<double> bs(capacity);
    bool done = false;
    while (!done) {
      int64_t count = 0;
      ObDASRowID nid;
      double nscore = 0.0;
      OK(op->drain_until(boundary, capacity, bi.data(), with_scores ? bs.data() : nullptr,
          count, nid, nscore));
      ASSERT_GE(count, 0);
      ASSERT_LE(count, capacity);
      for (int64_t i = 0; i < count; ++i) {
        ids.push_back(bi[i]);
        if (with_scores) {
          scores.push_back(bs[i]);
        }
      }
      if (count == capacity) {
        continue;  // buffer full; keep draining with the same boundary
      }
      if (nid.is_max()) {
        hit_end = true;
      } else {
        hit_end = false;
        next_id = nid.get_uint64();
        next_score = nscore;
      }
      done = true;
    }
    OK(op->close());
  }

  // Scalar oracle over next_rowid(): the exact reference sequence the base row-by-row
  // do_drain_until() would produce for the same (boundary, pre_next).
  void run_oracle(
      ObInvSSTable &inv_sstable, ObTableReadInfo &read_info,
      const common::ObString &query_token, const double token_boost,
      const int64_t total_doc_cnt, const int64_t doc_freq,
      const uint64_t boundary_u64, const int64_t pre_next, const double cached_avg,
      std::vector<uint64_t> &ids, std::vector<double> &scores,
      bool &hit_end, uint64_t &next_id, double &next_score)
  {
    ids.clear();
    scores.clear();
    hit_end = false;
    next_id = 0;
    next_score = 0.0;
    ObArenaAllocator a("OracleCtx");
    AsyncTokCtx ctx(a);
    sql::ObDASIRScanCtDef ctdef(a);
    sql::ObDASIRScanRtDef rtdef;
    sql::ObDASAsyncTokenOp *op = nullptr;
    create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost, total_doc_cnt,
        doc_freq, read_info, inv_sstable, op, cached_avg);
    OK(op->open());
    for (int64_t k = 0; k < pre_next; ++k) {
      ObDASRowID rid;
      double s = 0.0;
      OK(op->next_rowid(rid, s));
    }
    int ret = OB_SUCCESS;
    bool stopped = false;
    while (!stopped) {
      ObDASRowID rid;
      double s = 0.0;
      ret = op->next_rowid(rid, s);
      if (OB_ITER_END == ret) {
        hit_end = true;
        stopped = true;
      } else {
        OK(ret);
        const uint64_t id = rid.get_uint64();
        if (id > boundary_u64) {
          hit_end = false;
          next_id = id;
          next_score = s;
          stopped = true;
        } else {
          ids.push_back(id);
          scores.push_back(s);
        }
      }
    }
    OK(op->close());
  }

  // Run both paths and assert the batch drain matches the scalar oracle exactly.
  void expect_drain_matches_oracle(
      ObInvSSTable &inv_sstable, ObTableReadInfo &read_info,
      const common::ObString &query_token, const double token_boost,
      const int64_t total_doc_cnt, const int64_t doc_freq,
      const int64_t capacity, const uint64_t boundary_u64,
      const bool with_scores = true, const int64_t pre_next = 0,
      const double cached_avg = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT)
  {
    std::vector<uint64_t> exp_ids, got_ids;
    std::vector<double> exp_scores, got_scores;
    bool exp_end = false, got_end = false;
    uint64_t exp_next = 0, got_next = 0;
    double exp_next_score = 0.0, got_next_score = 0.0;
    run_oracle(inv_sstable, read_info, query_token, token_boost, total_doc_cnt, doc_freq,
        boundary_u64, pre_next, cached_avg, exp_ids, exp_scores, exp_end, exp_next, exp_next_score);
    run_drain(inv_sstable, read_info, query_token, token_boost, total_doc_cnt, doc_freq,
        capacity, boundary_u64, with_scores, pre_next, cached_avg,
        got_ids, got_scores, got_end, got_next, got_next_score);
    ASSERT_EQ(exp_end, got_end) << "cap=" << capacity << " boundary=" << boundary_u64;
    ASSERT_EQ(exp_ids.size(), got_ids.size()) << "cap=" << capacity << " boundary=" << boundary_u64;
    for (size_t i = 0; i < exp_ids.size(); ++i) {
      ASSERT_EQ(exp_ids[i], got_ids[i]) << "id mismatch at " << i << " cap=" << capacity;
      if (with_scores) {
        ASSERT_DOUBLE_EQ(exp_scores[i], got_scores[i]) << "score mismatch at " << i << " cap=" << capacity;
      }
    }
    if (!exp_end) {
      ASSERT_EQ(exp_next, got_next) << "next_id mismatch, cap=" << capacity;
      // next_score is computed even when out_scores is null.
      ASSERT_DOUBLE_EQ(exp_next_score, got_next_score) << "next_score mismatch, cap=" << capacity;
    }
  }

  void run_real_multi_term_filter_bmm(
      const sql::ObDASBMMOp::FilterMergeMode expected_mode, const bool optional = false)
  {
    static constexpr int64_t SCORE_A_TERM_SEQ = 10;
    static constexpr int64_t SCORE_B_TERM_SEQ = 11;
    static constexpr int64_t SCORE_C_TERM_SEQ = 12;
    static constexpr int64_t FILTER_TERM_SEQ = 13;
    static constexpr int64_t FILLER_COUNT = 300;
    static constexpr int64_t TOTAL_DOC_CNT = 100000;
    static constexpr uint64_t HIT_LOW = 1;
    static constexpr uint64_t HIT_MULTI = 3;
    static constexpr uint64_t HIT_NON_ESSENTIAL = 5;
    static constexpr uint64_t SCORE_ONLY = 7;
    const uint64_t FILTER_ONLY = optional ? 0 : 9;
    static constexpr uint64_t LATE_HIT = 20000;
    static constexpr int64_t SCORE_A_DOC_FREQ = FILLER_COUNT + 4;
    static constexpr int64_t SCORE_B_DOC_FREQ = FILLER_COUNT + 3;
    static constexpr int64_t SCORE_C_DOC_FREQ = FILLER_COUNT + 4;
    static constexpr int64_t FILTER_DOC_FREQ = 5;

    std::vector<RealBMMPostingRow> rows;
    rows.reserve(3 * FILLER_COUNT + 16);
    rows.push_back(RealBMMPostingRow(SCORE_A_TERM_SEQ, HIT_MULTI));
    rows.push_back(RealBMMPostingRow(SCORE_A_TERM_SEQ, HIT_NON_ESSENTIAL));
    rows.push_back(RealBMMPostingRow(SCORE_A_TERM_SEQ, SCORE_ONLY));
    for (int64_t i = 0; i < FILLER_COUNT; ++i) {
      rows.push_back(RealBMMPostingRow(
          SCORE_A_TERM_SEQ, 10 + static_cast<uint64_t>(i) * 2));
    }
    rows.push_back(RealBMMPostingRow(SCORE_A_TERM_SEQ, LATE_HIT, optional ? 4 : 1));

    rows.push_back(RealBMMPostingRow(SCORE_B_TERM_SEQ, HIT_LOW));
    rows.push_back(RealBMMPostingRow(SCORE_B_TERM_SEQ, HIT_MULTI));
    rows.push_back(RealBMMPostingRow(SCORE_B_TERM_SEQ, SCORE_ONLY));
    for (int64_t i = 0; i < FILLER_COUNT; ++i) {
      rows.push_back(RealBMMPostingRow(
          SCORE_B_TERM_SEQ, 11 + static_cast<uint64_t>(i) * 3));
    }

    rows.push_back(RealBMMPostingRow(SCORE_C_TERM_SEQ, HIT_MULTI));
    rows.push_back(RealBMMPostingRow(SCORE_C_TERM_SEQ, HIT_NON_ESSENTIAL));
    rows.push_back(RealBMMPostingRow(SCORE_C_TERM_SEQ, SCORE_ONLY));
    for (int64_t i = 0; i < FILLER_COUNT; ++i) {
      rows.push_back(RealBMMPostingRow(
          SCORE_C_TERM_SEQ, 12 + static_cast<uint64_t>(i) * 4));
    }
    rows.push_back(RealBMMPostingRow(SCORE_C_TERM_SEQ, LATE_HIT, optional ? 4 : 1));

    const uint64_t filter_ids[] = {
        FILTER_ONLY, HIT_LOW, HIT_MULTI, HIT_NON_ESSENTIAL, LATE_HIT};
    for (uint64_t doc_id : filter_ids) {
      rows.push_back(RealBMMPostingRow(FILTER_TERM_SEQ, doc_id));
    }

    std::sort(rows.begin(), rows.end(), [](const RealBMMPostingRow &a, const RealBMMPostingRow &b) {
      return a.term_seq_ < b.term_seq_ || (a.term_seq_ == b.term_seq_ && a.doc_id_ < b.doc_id_);
    });
    ObInvSSTable inv_sstable;
    build_multi_term_sstable(rows, inv_sstable);
    ObTableReadInfo read_info;
    build_rowkey_read_info(table_schema_, allocator_, read_info);

    ObArenaAllocator ctx_alloc("RealFilterBMM");
    AsyncTokCtx ctx(ctx_alloc);
    sql::ObExecContext exec_ctx(ctx_alloc);
    sql::ObEvalCtx eval_ctx(exec_ctx);
    eval_ctx.max_batch_size_ = 256;
    sql::ObExpr rowid_expr;
    sql::ExprFixedArray rowid_exprs(ctx_alloc);
    sql::ExprFixedArray output(ctx_alloc);
    OK(rowid_exprs.init(1));
    OK(rowid_exprs.push_back(&rowid_expr));
    ctx.search_ctx_.eval_ctx_ = &eval_ctx;
    ctx.search_ctx_.rowid_exprs_ = &rowid_exprs;
    ctx.search_ctx_.scopes_[sql::DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs;
    ctx.search_ctx_.output_ = &output;
    ctx.search_ctx_.table_row_count_ = sql::ObDASSearchCost(TOTAL_DOC_CNT);

    sql::ObDASIRScanCtDef ctdef_a(ctx_alloc);
    sql::ObDASIRScanCtDef ctdef_b(ctx_alloc);
    sql::ObDASIRScanCtDef ctdef_c(ctx_alloc);
    sql::ObDASIRScanCtDef filter_ctdef(ctx_alloc);
    sql::ObDASIRScanRtDef rtdef_a;
    sql::ObDASIRScanRtDef rtdef_b;
    sql::ObDASIRScanRtDef rtdef_c;
    sql::ObDASIRScanRtDef filter_rtdef;
    char term_a_buf[32] = {0};
    char term_b_buf[32] = {0};
    char term_c_buf[32] = {0};
    char filter_term_buf[32] = {0};
    format_term(SCORE_A_TERM_SEQ, term_a_buf, sizeof(term_a_buf));
    format_term(SCORE_B_TERM_SEQ, term_b_buf, sizeof(term_b_buf));
    format_term(SCORE_C_TERM_SEQ, term_c_buf, sizeof(term_c_buf));
    format_term(FILTER_TERM_SEQ, filter_term_buf, sizeof(filter_term_buf));
    const ObString term_a(static_cast<int32_t>(strlen(term_a_buf)), term_a_buf);
    const ObString term_b(static_cast<int32_t>(strlen(term_b_buf)), term_b_buf);
    const ObString term_c(static_cast<int32_t>(strlen(term_c_buf)), term_c_buf);
    const ObString filter_term(
        static_cast<int32_t>(strlen(filter_term_buf)), filter_term_buf);
    const double boosts[] = {1.0, 2.0, 4.0};
    sql::ObDASAsyncTokenOp *op_a = nullptr;
    sql::ObDASAsyncTokenOp *op_b = nullptr;
    sql::ObDASAsyncTokenOp *op_c = nullptr;
    sql::ObDASAsyncTokenOp *filter_op = nullptr;
    create_and_inject_single(ctx, ctdef_a, rtdef_a, term_a, boosts[0], TOTAL_DOC_CNT,
        SCORE_A_DOC_FREQ, read_info, inv_sstable, op_a);
    create_and_inject_single(ctx, ctdef_b, rtdef_b, term_b, boosts[1], TOTAL_DOC_CNT,
        SCORE_B_DOC_FREQ, read_info, inv_sstable, op_b,
        sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, true, false);
    create_and_inject_single(ctx, ctdef_c, rtdef_c, term_c, boosts[2], TOTAL_DOC_CNT,
        SCORE_C_DOC_FREQ, read_info, inv_sstable, op_c,
        sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, true, false);
    create_and_inject_single(ctx, filter_ctdef, filter_rtdef, filter_term, 1.0,
        TOTAL_DOC_CNT, FILTER_DOC_FREQ, read_info, inv_sstable, filter_op,
        sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, true, false, false);
    storage::ObInvIdxParamCache &cache = ctx.search_ctx_.get_bm25_param_cache();
    OK(cache.set_token_doc_cnt(common::ObTabletID(TABLET_ID), term_b, SCORE_B_DOC_FREQ));
    OK(cache.set_token_doc_cnt(common::ObTabletID(TABLET_ID), term_c, SCORE_C_DOC_FREQ));
    OK(cache.set_token_doc_cnt(common::ObTabletID(TABLET_ID), filter_term, FILTER_DOC_FREQ));

    common::ObSEArray<sql::ObIDASSearchOp *, 3> score_ops;
    common::ObSEArray<sql::ObDASSearchCost, 3> score_op_costs;
    OK(score_ops.push_back(op_a));
    OK(score_ops.push_back(op_b));
    OK(score_ops.push_back(op_c));
    OK(score_op_costs.push_back(sql::ObDASSearchCost(SCORE_A_DOC_FREQ)));
    OK(score_op_costs.push_back(sql::ObDASSearchCost(SCORE_B_DOC_FREQ)));
    OK(score_op_costs.push_back(sql::ObDASSearchCost(SCORE_C_DOC_FREQ)));
    const bool sparse = sql::ObDASBMMOp::FilterMergeMode::SPARSE_SEEK == expected_mode;
    sql::ObDASBMMBuildOptions bmm_param_options;
    bmm_param_options.query_optional_ = optional;
    bmm_param_options.lead_cost_ = sql::ObDASSearchCost(sparse ? 1 : 1001);
    bmm_param_options.filter_cost_ = sql::ObDASSearchCost(FILTER_DOC_FREQ);
    sql::ObDASBMMOpParam bmm_param(
        score_ops, filter_op, ctx_alloc, bmm_param_options, &score_op_costs);
    bmm_param.set_is_scoring(true);
    sql::ObDASBMMOp bmm(ctx.search_ctx_);
    OK(bmm.init(bmm_param));
    ASSERT_TRUE(bmm.enable_vec_merge_);
    ASSERT_EQ(expected_mode, bmm.filter_merge_mode_);
    if (sparse && !optional) {
      ASSERT_FALSE(bmm.doc_score_accumulator_.is_valid());
      ASSERT_EQ(sql::ObDAATMergeIter::DRAIN_BATCH_SIZE,
          bmm.sparse_candidate_buffer_.get_capacity());
    } else {
      ASSERT_TRUE(bmm.doc_score_accumulator_.is_valid());
      ASSERT_NE(nullptr, bmm.filter_bitset_);
      ASSERT_EQ(sparse ? sql::ObDAATMergeIter::DRAIN_BATCH_SIZE : 0,
          bmm.sparse_candidate_buffer_.get_capacity());
    }
    OK(bmm.open());

    const double avg = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT;
    const double score_a = fast_ref_score(
        1, 100, SCORE_A_DOC_FREQ, TOTAL_DOC_CNT, avg, boosts[0]);
    const double score_b = fast_ref_score(
        1, 100, SCORE_B_DOC_FREQ, TOTAL_DOC_CNT, avg, boosts[1]);
    const double score_c = fast_ref_score(
        1, 100, SCORE_C_DOC_FREQ, TOTAL_DOC_CNT, avg, boosts[2]);
    ASSERT_LT(score_b, score_a + score_c);
    const double threshold = (score_b + score_a + score_c) / 2.0;
    OK(bmm.set_min_competitive_score(threshold));

    const double late_score = fast_ref_score(optional ? 4 : 1, 100,
        SCORE_A_DOC_FREQ, TOTAL_DOC_CNT, avg, boosts[0])
        + fast_ref_score(optional ? 4 : 1, 100, SCORE_C_DOC_FREQ, TOTAL_DOC_CNT, avg, boosts[2]);
    const uint64_t expected_ids[] = {HIT_MULTI, HIT_NON_ESSENTIAL, LATE_HIT};
    const double expected_scores[] = {
        score_a + score_b + score_c,
        score_a + score_c,
        late_score};
    ObDASRowID rowid;
    double score = 0.0;
    int ret = OB_SUCCESS;
    int64_t result_idx = 0;
    bool saw_local_partition = false;
    while (OB_SUCCESS == (ret = bmm.next_rowid(rowid, score))) {
      ASSERT_LT(result_idx, ARRAYSIZEOF(expected_ids));
      EXPECT_EQ(expected_ids[result_idx], rowid.get_uint64());
      EXPECT_TRUE(bm25_close(score, expected_scores[result_idx]));
      saw_local_partition = saw_local_partition
          || (bmm.vec_non_essential_dim_count_ > 0
              && bmm.vec_non_essential_dim_count_ < score_ops.count());
      if (0 == result_idx) {
        ASSERT_EQ(score_ops.count(), bmm.vec_range_max_scores_.count());
        for (int64_t i = 0; i < bmm.vec_range_max_scores_.count(); ++i) {
          EXPECT_GT(bmm.vec_range_max_scores_.at(i), 0.0);
        }
      }
      ++result_idx;
    }
    EXPECT_EQ(OB_ITER_END, ret);
    EXPECT_EQ(ARRAYSIZEOF(expected_ids), result_idx);
    EXPECT_TRUE(saw_local_partition);
    if (optional) {
      // The zero-score filter-only row belongs to the optional result domain.
      OK(bmm.rescan());
      ASSERT_TRUE(bmm.is_filter_driven_optional());
      OK(bmm.next_rowid(rowid, score));
      EXPECT_EQ(FILTER_ONLY, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(0.0, score);
      OK(bmm.rescan());
      sql::ObDASTopKCollectOpParam topk_param;
      topk_param.child_ = &bmm;
      topk_param.limit_ = 2;
      topk_param.set_is_scoring(true);
      topk_param.dynamic_pruning_enabled_ = true;
      sql::ObDASTopKCollectOp topk(ctx.search_ctx_);
      OK(topk.init(topk_param));
      ASSERT_TRUE(topk.dynamic_pruning_enabled_);
      // BMM is already open; rescan resets both collector and its child.
      OK(topk.rescan());
      std::vector<std::pair<uint64_t, double>> actual;
      while (OB_SUCCESS == (ret = topk.next_rowid(rowid, score))) {
        actual.emplace_back(rowid.get_uint64(), score);
      }
      ASSERT_EQ(OB_ITER_END, ret);
      std::sort(actual.begin(), actual.end());
      ASSERT_EQ(2, actual.size());
      EXPECT_EQ(HIT_MULTI, actual[0].first);
      EXPECT_TRUE(bm25_close(score_a + score_b + score_c, actual[0].second));
      EXPECT_EQ(LATE_HIT, actual[1].first);
      EXPECT_TRUE(bm25_close(late_score, actual[1].second));
      EXPECT_GT(bmm.min_competitive_score_, 0.0);
      EXPECT_FALSE(bmm.is_filter_driven_optional());
      OK(topk.close());
    }
    OK(bmm.close());
  }

  // Mock access service injected into the op so open_posting_iter_from_tablet() goes
  // through the real async_index_posting_scan path (delegating to scan_posting).
  MockAsyncIndexAccessService mock_access_;
};

// ObDASAsyncTokenOpParam::is_valid rejects each invalid combination.
TEST_F(TestDASAsyncTokenOp, param_is_valid)
{
  sql::ObDASIRScanCtDef ctdef(allocator_);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASAsyncTokenOpParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.token_boost_ = 1.0;
  param.query_token_ = ObString::make_string("term0000000001");
  ASSERT_TRUE(param.is_valid());

  sql::ObDASAsyncTokenOpParam bad = param;
  bad.ir_ctdef_ = nullptr;
  ASSERT_FALSE(bad.is_valid());
  bad = param;
  bad.ir_rtdef_ = nullptr;
  ASSERT_FALSE(bad.is_valid());
  bad = param;
  bad.index_table_id_ = common::OB_INVALID_ID;
  ASSERT_FALSE(bad.is_valid());
  bad = param;
  bad.group_slot_ = -2;
  ASSERT_FALSE(bad.is_valid());
  bad = param;
  bad.direct_bind_ = true;
  bad.group_slot_ = -1;
  ASSERT_FALSE(bad.is_valid());
  bad = param;
  bad.token_boost_ = 0.0;
  ASSERT_FALSE(bad.is_valid());
  bad = param;
  bad.query_token_.reset();
  ASSERT_FALSE(bad.is_valid());
}

// do_init rejects a non-uint64 rowid type.
TEST_F(TestDASAsyncTokenOp, do_init_rejects_non_uint64_rowid)
{
  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  ctx.search_ctx_.rowid_type_ = DAS_ROWID_TYPE_COMPACT;

  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASAsyncTokenOpParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.token_boost_ = 1.0;
  param.query_token_ = ObString::make_string("term0000000001");

  sql::ObDASAsyncTokenOp *op = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, ctx.search_ctx_.create_op(param, op));
  ASSERT_EQ(nullptr, op);
}

// Without either injected sstables or scalar scan defs for the real tablet path, do_open fails
// while initializing the async inv-index scan param.
TEST_F(TestDASAsyncTokenOp, open_without_sstables_or_scan_defs_returns_unexpected)
{
  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  ctdef.use_scalar_scan_def_ = true;

  sql::ObDASAsyncTokenOpParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.version_range_.base_version_ = 0;
  param.version_range_.snapshot_version_ = SNAPSHOT_VERSION;
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.token_boost_ = 1.0;
  param.query_token_ = ObString::make_string("term0000000001");

  sql::ObDASAsyncTokenOp *op = nullptr;
  OK(ctx.search_ctx_.create_op(param, op));
  ASSERT_NE(nullptr, op);
  ASSERT_EQ(OB_ERR_UNEXPECTED, op->open());
  ASSERT_EQ(OB_SUCCESS, op->close());
}

// next_rowid returns strictly increasing doc ids; the BM25 score (doc_freq / total /
// avg_doc_length from the prefilled bm25 param cache) is bounded by dim_max_score*boost.
TEST_F(TestDASAsyncTokenOp, next_rowid_increasing_with_real_score)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  const double token_boost = 3.0;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost,
      OP_DOC_CNT /*cached_total_doc_cnt*/, OP_DOC_CNT /*cached_doc_freq*/, read_info,
      inv_sstable, op);
  OK(op->open());

  // doc_freq / total come from the prefilled cache.
  ASSERT_EQ(OP_DOC_CNT, op->posting_scan_param_.doc_freq_);
  ASSERT_EQ(OP_DOC_CNT, op->posting_scan_param_.total_doc_cnt_);

  // Exact oracle for the op's fast path (single-division kernel).
  const double expected_score = fast_ref_score(
      1 /*token_freq*/, 100 /*doc_length*/,
      op->posting_scan_param_.doc_freq_,
      op->posting_scan_param_.total_doc_cnt_,
      op->posting_scan_param_.avg_doc_length_, token_boost);
  // Canonical two-division formula must agree within tolerance.
  const double canonical_score = sql::ObExprBM25::eval(
      1 /*token_freq*/, 100 /*doc_length*/,
      op->posting_scan_param_.doc_freq_,
      op->posting_scan_param_.total_doc_cnt_,
      op->posting_scan_param_.avg_doc_length_) * token_boost;
  ASSERT_TRUE(bm25_close(expected_score, canonical_score));
  const double dim_max = op->dim_max_score_ * token_boost;
  ASSERT_GT(expected_score, 0.0);
  ASSERT_GT(dim_max, expected_score) << "dim max must strictly bound the row score";

  ObDASRowID rid;
  double score = 0.0;
  int64_t i = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = op->next_rowid(rid, score))) {
    ASSERT_EQ(expect_doc(OP_START, i), rid.get_uint64()) << "doc id mismatch at " << i;
    ASSERT_DOUBLE_EQ(expected_score, score) << "score mismatch at " << i;
    ASSERT_LE(score, dim_max);
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OP_DOC_CNT, i);
  OK(op->close());
}

// A non-default avg_doc_token_cnt cached by tablet flows through the lazy estimator
// (get_bm25_params -> fill_async_bm25_param) into posting_scan_param_.avg_doc_length_.
// This exercises the avg plumbing without a real tablet / read-tables aggregation.
TEST_F(TestDASAsyncTokenOp, avg_doc_length_from_cache_flows_to_posting_param)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  // Non-default avg (DEFAULT is 100.0) so we can tell it came from the cache.
  const double cached_avg = 250.0;
  ASSERT_NE(cached_avg, sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT);

  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.0 /*token_boost*/,
      OP_DOC_CNT /*cached_total_doc_cnt*/, OP_DOC_CNT /*cached_doc_freq*/, read_info,
      inv_sstable, op, cached_avg);
  OK(op->open());

  ASSERT_DOUBLE_EQ(cached_avg, op->posting_scan_param_.avg_doc_length_);
  OK(op->close());
}

// next_rowid on an absent term returns OB_ITER_END immediately.
TEST_F(TestDASAsyncTokenOp, next_rowid_absent_term_iter_end)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(999999999, term_buf, sizeof(term_buf));  // absent term
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.0, OP_DOC_CNT,
      0 /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());
  ASSERT_EQ(0, op->posting_scan_param_.doc_freq_) << "absent term -> doc_freq 0";

  ObDASRowID rid;
  double score = 0.0;
  ASSERT_EQ(OB_ITER_END, op->next_rowid(rid, score));
  OK(op->close());
}

// advance_to returns the first doc_id >= target; beyond the term returns OB_ITER_END;
// next_rowid continues correctly after advance_to.
TEST_F(TestDASAsyncTokenOp, advance_to_semantics)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.0, OP_DOC_CNT,
      OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());

  ObDASRowID target;
  ObDASRowID res;
  double score = 0.0;
  target.set_uint64(expect_doc(OP_START, 5) - 1);  // absent, rounds up
  OK(op->advance_to(target, res, score));
  ASSERT_EQ(expect_doc(OP_START, 5), res.get_uint64());
  OK(op->advance_to(res, res, score));
  ASSERT_EQ(expect_doc(OP_START, 5), res.get_uint64());
  OK(op->next_rowid(res, score));
  ASSERT_EQ(expect_doc(OP_START, 6), res.get_uint64());

  target.set_uint64(expect_doc(OP_START, 10));
  OK(op->advance_to(target, res, score));
  ASSERT_EQ(expect_doc(OP_START, 10), res.get_uint64());

  target.set_uint64(expect_doc(OP_START, OP_DOC_CNT - 1) + 1);  // beyond last
  ASSERT_EQ(OB_ITER_END, op->advance_to(target, res, score));
  OK(op->close());
}

// advance_shallow supports both inclusive=false/true and returns a conservative tuple
// whose max_score == dim_max_score * token_boost. next_rowid right after shallow is
// rejected; advance_to after shallow materializes correctly.
TEST_F(TestDASAsyncTokenOp, advance_shallow_semantics)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  const double token_boost = 2.0;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost, OP_DOC_CNT,
      OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());
  const double dim_max_boosted = op->dim_max_score_ * token_boost;
  // Uniform sstable (tf=1, dl=100 for every doc): every posting block's frontier
  // collapses to a single (1,100) pair, so the real per-block block-max equals the
  // per-row score and is strictly below the global dim-max. After the storage fix
  // the op reads the real frontier; before the fix it fell back to dim_max here.
  const double expected_block_max = fast_ref_score(
      1 /*token_freq*/, 100 /*doc_length*/,
      op->posting_scan_param_.doc_freq_,
      op->posting_scan_param_.total_doc_cnt_,
      op->posting_scan_param_.avg_doc_length_, token_boost);
  ASSERT_TRUE(bm25_close(expected_block_max, sql::ObExprBM25::eval(
      1, 100, op->posting_scan_param_.doc_freq_,
      op->posting_scan_param_.total_doc_cnt_,
      op->posting_scan_param_.avg_doc_length_) * token_boost));
  ASSERT_GT(expected_block_max, 0.0);
  ASSERT_GT(dim_max_boosted, expected_block_max)
      << "real block-max must be strictly tighter than the global dim-max";

  ObDASRowID target;
  const MaxScoreTuple *tuple = nullptr;
  // Inclusive first: target is part of the range, so reported min_id <= target.
  // (Order matters: the scan frontier is monotone, so a later exclusive advance to
  // the same id raises it and never regresses -- see below and advance_frontier_monotonic.)
  target.set_uint64(expect_doc(OP_START, 10));
  OK(op->advance_shallow(target, true /*inclusive*/, tuple));
  ASSERT_NE(nullptr, tuple);
  ASSERT_DOUBLE_EQ(expected_block_max, tuple->get_max_score());
  ASSERT_LE(tuple->get_min_id().get_uint64(), expect_doc(OP_START, 10));
  ASSERT_GE(tuple->get_max_id().get_uint64(), expect_doc(OP_START, 10));

  // Exclusive to the same target: target is excluded, so the frontier moves strictly
  // past it and reported min_id > target. max_id (block extent) is unaffected.
  OK(op->advance_shallow(target, false /*inclusive*/, tuple));
  ASSERT_NE(nullptr, tuple);
  ASSERT_DOUBLE_EQ(expected_block_max, tuple->get_max_score());
  ASSERT_GT(tuple->get_min_id().get_uint64(), expect_doc(OP_START, 10));
  ASSERT_GT(tuple->get_max_id().get_uint64(), expect_doc(OP_START, 10));

  ObDASRowID rid;
  double score = 0.0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, op->next_rowid(rid, score));  // shallow -> next rejected
  OK(op->advance_to(target, rid, score));  // materializes
  ASSERT_EQ(expect_doc(OP_START, 10), rid.get_uint64());
  // For uniform data the materialized row score equals the block-max upper bound.
  ASSERT_DOUBLE_EQ(expected_block_max, score);
  ASSERT_LE(score, dim_max_boosted);
  OK(op->close());
}

// advance_to (materialized id) and advance_shallow (block-max min_id) feed a single
// monotonically non-decreasing frontier. Once a doc is materialized, or a shallow
// target is clamped past it, a later advance_shallow can never report a min_id
// behind it -- even if that later shallow target points backwards. This upholds the
// WAND-style contract that ids from advance_to and min_ids from advance_shallow
// interleave into one ordered stream. All targets stay within block 0 so every
// advance_shallow takes the covered_by_curr_block fast path (no meta reload).
TEST_F(TestDASAsyncTokenOp, advance_frontier_monotonic)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.0 /*token_boost*/,
      OP_DOC_CNT, OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());

  ObDASRowID target;
  ObDASRowID rid;
  double score = 0.0;
  const MaxScoreTuple *tuple = nullptr;
  uint64_t last = 0;  // running max over the merged emitted sequence

  // 1. shallow to doc(5): frontier rises to doc(5).
  target.set_uint64(expect_doc(OP_START, 5));
  OK(op->advance_shallow(target, true /*inclusive*/, tuple));
  ASSERT_NE(nullptr, tuple);
  ASSERT_EQ(expect_doc(OP_START, 5), tuple->get_min_id().get_uint64());
  ASSERT_GE(tuple->get_min_id().get_uint64(), last);
  last = tuple->get_min_id().get_uint64();

  // 2. materialize doc(20): frontier rises to doc(20).
  target.set_uint64(expect_doc(OP_START, 20));
  OK(op->advance_to(target, rid, score));
  ASSERT_EQ(expect_doc(OP_START, 20), rid.get_uint64());
  ASSERT_GE(rid.get_uint64(), last);
  last = rid.get_uint64();

  // 3. shallow to doc(10) -- a BACKWARD target in the same block. The frontier must
  // not regress: min_id stays at the already-materialized doc(20), not doc(10).
  target.set_uint64(expect_doc(OP_START, 10));
  OK(op->advance_shallow(target, true /*inclusive*/, tuple));
  ASSERT_NE(nullptr, tuple);
  ASSERT_EQ(expect_doc(OP_START, 20), tuple->get_min_id().get_uint64());
  ASSERT_GE(tuple->get_min_id().get_uint64(), last);
  last = tuple->get_min_id().get_uint64();

  // 4. forward shallow to doc(30): frontier advances normally.
  target.set_uint64(expect_doc(OP_START, 30));
  OK(op->advance_shallow(target, true /*inclusive*/, tuple));
  ASSERT_NE(nullptr, tuple);
  ASSERT_EQ(expect_doc(OP_START, 30), tuple->get_min_id().get_uint64());
  ASSERT_GE(tuple->get_min_id().get_uint64(), last);
  last = tuple->get_min_id().get_uint64();

  // 5. materialize doc(30): still monotone, no regression.
  target.set_uint64(expect_doc(OP_START, 30));
  OK(op->advance_to(target, rid, score));
  ASSERT_EQ(expect_doc(OP_START, 30), rid.get_uint64());
  ASSERT_GE(rid.get_uint64(), last);

  OK(op->close());
}

// advance_shallow lands on the posting block that may contain the target and reports
// THAT block's tight block-max, not a global bound. Here tf is constant within each
// posting block but increases across blocks (block k holds tf = k+1), so the four
// blocks have four distinct, strictly increasing block-max scores. This distinguishes
// "real per-block frontier" from "dim-max fallback": the fallback would report the
// same dim_max_boosted for every block.
TEST_F(TestDASAsyncTokenOp, advance_shallow_per_block_max)
{
  ObInvSSTable inv_sstable;
  common::ObSEArray<uint64_t, 1024> token_freqs;
  common::ObSEArray<uint64_t, 1024> doc_lengths;
  for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
    // 4 posting blocks of 256/256/256/232 docs; tf is constant per block, distinct across.
    OK(token_freqs.push_back(static_cast<uint64_t>(i / 256) + 1));
    OK(doc_lengths.push_back(100));
  }
  build_single_term_sstable_per_doc(OP_START, OP_DOC_CNT, OP_TERM_SEQ,
      token_freqs, doc_lengths, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  const double token_boost = 2.0;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost, OP_DOC_CNT,
      OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());
  const double avg = op->posting_scan_param_.avg_doc_length_;
  const int64_t doc_freq = op->posting_scan_param_.doc_freq_;
  const int64_t total = op->posting_scan_param_.total_doc_cnt_;
  const double dim_max_boosted = op->dim_max_score_ * token_boost;

  // doc count per posting block: 256, 256, 256, 232 (DEFAULT_POSTING_BLOCK_CAPACITY=256).
  const int64_t block_sizes[4] = {256, 256, 256, 232};
  double prev_max_score = 0.0;
  for (int64_t k = 0; k < 4; ++k) {
    const int64_t target_i = k * 256 + 128;  // comfortably inside block k
    ObDASRowID target;
    target.set_uint64(expect_doc(OP_START, target_i));
    const MaxScoreTuple *tuple = nullptr;
    OK(op->advance_shallow(target, true /*inclusive*/, tuple));
    ASSERT_NE(nullptr, tuple);
    // max_id comes straight from the positioned block's agg entry (untouched), but
    // min_id is clamped up to the advance target when the target lies past the
    // block's true start: the frontier describes the score bound for
    // [current position, max_id], not the block's raw [min_id, max_id] extent.
    ASSERT_EQ(expect_doc(OP_START, target_i), tuple->get_min_id().get_uint64());
    ASSERT_EQ(expect_doc(OP_START, k * 256 + block_sizes[k] - 1), tuple->get_max_id().get_uint64());
    ASSERT_LE(tuple->get_min_id().get_uint64(), expect_doc(OP_START, target_i));
    ASSERT_GE(tuple->get_max_id().get_uint64(), expect_doc(OP_START, target_i));
    // block-max = BM25(tf=k+1, dl=100); strictly increases across blocks, strictly below dim-max.
    const double exp_block_max = fast_ref_score(
        static_cast<uint64_t>(k + 1) /*tf*/, 100 /*dl*/, doc_freq, total, avg, token_boost);
    ASSERT_DOUBLE_EQ(exp_block_max, tuple->get_max_score());
    ASSERT_TRUE(bm25_close(exp_block_max,
        sql::ObExprBM25::eval(static_cast<int64_t>(k + 1), 100, doc_freq, total, avg) * token_boost));
    ASSERT_LT(prev_max_score, tuple->get_max_score());  // distinct & increasing per block
    ASSERT_LT(tuple->get_max_score(), dim_max_boosted);  // tighter than global dim-max
    prev_max_score = tuple->get_max_score();
  }
  OK(op->close());
}

// Exercise calc_range_max_score() on a real SSTable across posting blocks and the term tail,
// checking BM25 bounds, boost and inclusivity semantics, non-scoring/unavailable cases, and
// preservation of both materialized and shallow posting cursors.
TEST_F(TestDASAsyncTokenOp, range_max_real_sstable_contract)
{
  ObInvSSTable inv_sstable;
  common::ObSEArray<uint64_t, 1024> token_freqs;
  common::ObSEArray<uint64_t, 1024> doc_lengths;
  for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
    OK(token_freqs.push_back(static_cast<uint64_t>(i / 256) + 1));
    OK(doc_lengths.push_back(100));
  }
  build_single_term_sstable_per_doc(OP_START, OP_DOC_CNT, OP_TERM_SEQ,
      token_freqs, doc_lengths, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("RangeMaxToken");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);
  sql::ObDASAsyncTokenOp *op = nullptr;
  const double token_boost = 2.5;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost, OP_DOC_CNT,
      OP_DOC_CNT, read_info, inv_sstable, op);
  OK(op->open());

  ObDASRowID current_id;
  double current_score = 0.0;
  OK(op->next_rowid(current_id, current_score));
  const uint64_t saved_current_id = current_id.get_uint64();
  const int64_t saved_row_idx = op->posting_iter_->curr_row_idx_;
  const int64_t saved_read_idx = op->posting_iter_->scanner_.data_read_idx_;
  const int64_t saved_meta_idx = op->posting_iter_->scanner_.meta_row_scanner_.cur_idx_;
  const int64_t saved_block_idx = op->posting_iter_->curr_meta_.idx_;
  const uint64_t saved_frontier = op->posting_iter_->frontier_min_id_;
  const double avg = op->posting_scan_param_.avg_doc_length_;
  const int64_t doc_freq = op->posting_scan_param_.doc_freq_;
  const int64_t total = op->posting_scan_param_.total_doc_cnt_;

  ObDASRowID start;
  ObDASRowID end;
  double max_score = -1.0;
  bool can_calc = false;
  start.set_uint64(expect_doc(OP_START, 0));
  end.set_uint64(expect_doc(OP_START, 255));
  OK(op->calc_range_max_score(start, true /*start_inclusive*/, end, max_score, can_calc));
  ASSERT_TRUE(can_calc);
  ASSERT_DOUBLE_EQ(fast_ref_score(1, 100, doc_freq, total, avg, token_boost), max_score);

  // An exclusive boundary at block 0's maximum starts the range in block 1.
  start.set_uint64(expect_doc(OP_START, 255));
  end.set_uint64(expect_doc(OP_START, 511));
  max_score = -1.0;
  can_calc = false;
  OK(op->calc_range_max_score(start, false /*start_inclusive*/, end, max_score, can_calc));
  ASSERT_TRUE(can_calc);
  ASSERT_DOUBLE_EQ(fast_ref_score(2, 100, doc_freq, total, avg, token_boost), max_score);

  start.set_uint64(expect_doc(OP_START, 0));
  end.set_uint64(expect_doc(OP_START, 767));
  max_score = -1.0;
  can_calc = false;
  OK(op->calc_range_max_score(start, true /*start_inclusive*/, end, max_score, can_calc));
  ASSERT_TRUE(can_calc);
  ASSERT_DOUBLE_EQ(fast_ref_score(3, 100, doc_freq, total, avg, token_boost), max_score);

  end.set_max();
  max_score = -1.0;
  can_calc = false;
  OK(op->calc_range_max_score(start, true /*start_inclusive*/, end, max_score, can_calc));
  ASSERT_TRUE(can_calc);
  ASSERT_DOUBLE_EQ(fast_ref_score(4, 100, doc_freq, total, avg, token_boost), max_score);

  // Metadata lookahead is allowed to grow, but the materialized row and logical cursor stay put.
  ASSERT_EQ(saved_current_id, op->posting_iter_->curr_id_storage_);
  ASSERT_EQ(saved_row_idx, op->posting_iter_->curr_row_idx_);
  ASSERT_EQ(saved_read_idx, op->posting_iter_->scanner_.data_read_idx_);
  ASSERT_EQ(saved_meta_idx, op->posting_iter_->scanner_.meta_row_scanner_.cur_idx_);
  ASSERT_EQ(saved_block_idx, op->posting_iter_->curr_meta_.idx_);
  ASSERT_EQ(saved_frontier, op->posting_iter_->frontier_min_id_);

  ObDASRowID shallow_target;
  shallow_target.set_uint64(expect_doc(OP_START, 300));
  const MaxScoreTuple *tuple = nullptr;
  OK(op->advance_shallow(shallow_target, true /*inclusive*/, tuple));
  ASSERT_NE(nullptr, tuple);
  const int64_t shallow_read_idx = op->posting_iter_->scanner_.data_read_idx_;
  const int64_t shallow_meta_idx = op->posting_iter_->scanner_.meta_row_scanner_.cur_idx_;
  const int64_t shallow_block_idx = op->posting_iter_->curr_meta_.idx_;
  const int64_t shallow_row_idx = op->posting_iter_->curr_row_idx_;
  const uint64_t shallow_frontier = op->posting_iter_->frontier_min_id_;
  const uint64_t shallow_current_id = op->posting_iter_->curr_id_storage_;
  start = shallow_target;
  end.set_uint64(expect_doc(OP_START, 767));
  max_score = -1.0;
  can_calc = false;
  OK(op->calc_range_max_score(start, true /*start_inclusive*/, end, max_score, can_calc));
  ASSERT_TRUE(can_calc);
  ASSERT_DOUBLE_EQ(fast_ref_score(3, 100, doc_freq, total, avg, token_boost), max_score);
  ASSERT_TRUE(op->posting_iter_->in_shallow_status_);
  ASSERT_EQ(shallow_read_idx, op->posting_iter_->scanner_.data_read_idx_);
  ASSERT_EQ(shallow_meta_idx, op->posting_iter_->scanner_.meta_row_scanner_.cur_idx_);
  ASSERT_EQ(shallow_block_idx, op->posting_iter_->curr_meta_.idx_);
  ASSERT_EQ(shallow_row_idx, op->posting_iter_->curr_row_idx_);
  ASSERT_EQ(shallow_frontier, op->posting_iter_->frontier_min_id_);
  ASSERT_EQ(shallow_current_id, op->posting_iter_->curr_id_storage_);

  // The public wrapper zeros a calculable bound for a non-scoring token.
  op->is_scoring_ = false;
  end.set_uint64(op->posting_iter_->curr_meta_.max_id_);
  max_score = -1.0;
  can_calc = false;
  OK(op->calc_range_max_score(start, true /*start_inclusive*/, end, max_score, can_calc));
  ASSERT_TRUE(can_calc);
  ASSERT_DOUBLE_EQ(0.0, max_score);
  op->is_scoring_ = true;

  // An unavailable storage bound must not publish or transform a partial score.
  op->posting_iter_->has_relevance_params_ = false;
  max_score = -123.0;
  can_calc = true;
  OK(op->calc_range_max_score(start, true /*start_inclusive*/, end, max_score, can_calc));
  ASSERT_FALSE(can_calc);
  ASSERT_DOUBLE_EQ(-123.0, max_score);
  op->posting_iter_->has_relevance_params_ = true;
  OK(op->close());
}

// Real posting bounds tighten the global partition in the first 16K range.
// Both that range and the following high-score range must retain every competitive row.
TEST_F(TestDASAsyncTokenOp, real_async_tokens_drive_vectorized_bmm_range_max)
{
  BMMVectorizedConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  ObInvSSTable inv_sstable;
  std::vector<RealBMMPostingRow> rows;
  for (uint64_t i = 0; i < 256; ++i) {
    rows.emplace_back(OP_TERM_SEQ, 1 + i, 1, 100);
  }
  for (uint64_t i = 0; i < 256; ++i) {
    rows.emplace_back(OP_TERM_SEQ, 20001 + i, 8, 100);
  }
  build_multi_term_sstable(rows, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncBMMCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObExecContext exec_ctx(ctx_alloc);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  eval_ctx.max_batch_size_ = 256;
  sql::ObExpr rowid_expr;
  sql::ExprFixedArray rowid_exprs(ctx_alloc);
  sql::ExprFixedArray output(ctx_alloc);
  OK(rowid_exprs.init(1));
  OK(rowid_exprs.push_back(&rowid_expr));
  ctx.search_ctx_.eval_ctx_ = &eval_ctx;
  ctx.search_ctx_.rowid_exprs_ = &rowid_exprs;
  ctx.search_ctx_.output_ = &output;
  ctx.search_ctx_.table_row_count_ = sql::ObDASSearchCost(512);

  sql::ObDASIRScanCtDef ctdef_a(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef_b(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef_c(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef_a;
  sql::ObDASIRScanRtDef rtdef_b;
  sql::ObDASIRScanRtDef rtdef_c;
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);
  const double boosts[] = {1.0, 2.0, 3.0};
  sql::ObDASAsyncTokenOp *op_a = nullptr;
  sql::ObDASAsyncTokenOp *op_b = nullptr;
  sql::ObDASAsyncTokenOp *op_c = nullptr;
  create_and_inject_single(ctx, ctdef_a, rtdef_a, query_token, boosts[0], 512,
      512, read_info, inv_sstable, op_a);
  create_and_inject_single(ctx, ctdef_b, rtdef_b, query_token, boosts[1], 512,
      512, read_info, inv_sstable, op_b,
      sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, true, false);
  create_and_inject_single(ctx, ctdef_c, rtdef_c, query_token, boosts[2], 512,
      512, read_info, inv_sstable, op_c,
      sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, true, false);
  common::ObSEArray<sql::ObIDASSearchOp *, 3> score_ops;
  OK(score_ops.push_back(op_a));
  OK(score_ops.push_back(op_b));
  OK(score_ops.push_back(op_c));
  sql::ObDASBMMOpParam bmm_param(score_ops, nullptr, ctx_alloc, sql::ObDASBMMBuildOptions());
  bmm_param.set_is_scoring(true);
  sql::ObDASBMMOp bmm(ctx.search_ctx_);
  OK(bmm.init(bmm_param));
  ASSERT_TRUE(bmm.enable_vec_merge_);
  OK(bmm.open());

  const double base_score = fast_ref_score(1, 100, 512, 512,
      sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, 1.0);
  const double high_score = fast_ref_score(8, 100, 512, 512,
      sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, 1.0);
  const double threshold = 3.5 * base_score;
  ASSERT_GT(high_score, base_score);
  OK(bmm.set_min_competitive_score(threshold));
  ObDASRowID rowid;
  double score = 0.0;
  OK(bmm.next_rowid(rowid, score));
  ASSERT_EQ(1, rowid.get_uint64());
  ASSERT_NEAR(base_score * 6.0, score, 1e-15);
  // Observe actual range-local promotion, not merely a nonzero range-max call.
  EXPECT_GT(bmm.vec_non_essential_dim_count_, bmm.non_essential_dim_count_);
  ASSERT_EQ(3, bmm.vec_range_max_scores_.count());
  for (int64_t i = 0; i < bmm.vec_range_max_scores_.count(); ++i) {
    EXPECT_NEAR(base_score * boosts[i], bmm.vec_range_max_scores_.at(i), 1e-15);
    double global_max = 0.0;
    OK(bmm.merge_iters_.at(i)->get_max_score(global_max));
    EXPECT_LT(bmm.vec_range_max_scores_.at(i), global_max);
  }

  int64_t count = 1;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = bmm.next_rowid(rowid, score))) {
    const bool late = count >= 256;
    ASSERT_EQ(late ? 20001 + count - 256 : 1 + count, rowid.get_uint64());
    ASSERT_NEAR((late ? high_score : base_score) * 6.0, score, 1e-15);
    ++count;
  }
  EXPECT_EQ(OB_ITER_END, ret);
  EXPECT_EQ(512, count);
  OK(bmm.close());
}

// Drive real multi-term AsyncToken postings and a real filter through dense-window BMM,
// checking mode-specific buffers, BM25 score sums, threshold pruning, range bounds, and
// exactly the expected score/filter intersections.
TEST_F(TestDASAsyncTokenOp, real_multi_term_tokens_drive_dense_filter_bmm)
{
  BMMVectorizedConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  run_real_multi_term_filter_bmm(sql::ObDASBMMOp::FilterMergeMode::DENSE_WINDOW);
}

// Drive real multi-term AsyncToken postings and a real filter through sparse-seek BMM,
// checking mode-specific buffers, BM25 score sums, threshold pruning, range bounds, and
// exactly the expected score/filter intersections.
TEST_F(TestDASAsyncTokenOp, real_multi_term_tokens_drive_sparse_filter_bmm)
{
  BMMVectorizedConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  run_real_multi_term_filter_bmm(sql::ObDASBMMOp::FilterMergeMode::SPARSE_SEEK);
}

TEST_F(TestDASAsyncTokenOp, real_optional_filter_topk_feedback_crosses_windows)
{
  BMMVectorizedConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  run_real_multi_term_filter_bmm(sql::ObDASBMMOp::FilterMergeMode::SPARSE_SEEK, true);
}

// The block-max frontier is a Pareto set of (token_freq, doc_length) pairs. With
// alternating per-doc params — even i -> (5,200), odd i -> (3,100) — both pairs are
// Pareto-optimal (5>3 but 200>100), so the frontier keeps both and the block-max is
// max(BM25(5,200), BM25(3,100)). The winner depends on avg_doc_length; whichever pair
// wins, the other pair's rows score strictly below the block-max. This validates the
// multi-point frontier read path end-to-end.
TEST_F(TestDASAsyncTokenOp, advance_shallow_pareto_frontier)
{
  ObInvSSTable inv_sstable;
  common::ObSEArray<uint64_t, 1024> token_freqs;
  common::ObSEArray<uint64_t, 1024> doc_lengths;
  for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
    if (0 == i % 2) {
      OK(token_freqs.push_back(5));
      OK(doc_lengths.push_back(200));
    } else {
      OK(token_freqs.push_back(3));
      OK(doc_lengths.push_back(100));
    }
  }
  build_single_term_sstable_per_doc(OP_START, OP_DOC_CNT, OP_TERM_SEQ,
      token_freqs, doc_lengths, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  const double token_boost = 2.0;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost, OP_DOC_CNT,
      OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());
  const double avg = op->posting_scan_param_.avg_doc_length_;
  const int64_t doc_freq = op->posting_scan_param_.doc_freq_;
  const int64_t total = op->posting_scan_param_.total_doc_cnt_;
  const double score_5_200 = fast_ref_score(5, 200, doc_freq, total, avg, token_boost);
  const double score_3_100 = fast_ref_score(3, 100, doc_freq, total, avg, token_boost);
  ASSERT_TRUE(bm25_close(score_5_200, sql::ObExprBM25::eval(5, 200, doc_freq, total, avg) * token_boost));
  ASSERT_TRUE(bm25_close(score_3_100, sql::ObExprBM25::eval(3, 100, doc_freq, total, avg) * token_boost));
  const double exp_block_max = std::max(score_5_200, score_3_100);
  ASSERT_GT(exp_block_max, 0.0);

  // Position on block 0 (covers doc ids [1, 511]).
  ObDASRowID target;
  target.set_uint64(expect_doc(OP_START, 128));
  const MaxScoreTuple *tuple = nullptr;
  OK(op->advance_shallow(target, true /*inclusive*/, tuple));
  ASSERT_NE(nullptr, tuple);
  // Both (5,200) and (3,100) are Pareto-optimal (5>3 but 200>100), so the frontier
  // keeps both pairs — not a single point.
  ASSERT_GE(op->posting_iter_->curr_meta_.block_max_.pair_count_, 2);
  ASSERT_DOUBLE_EQ(exp_block_max, tuple->get_max_score());

  // Materialize one row of each pair (forward order: doc1 = i=0 even -> (5,200),
  // then doc3 = i=1 odd -> (3,100)). The block-max must upper-bound both, with the
  // losing pair strictly below — the whole point of a multi-point frontier.
  ObDASRowID rid;
  double score = 0.0;
  target.set_uint64(expect_doc(OP_START, 0));  // i=0 even -> (5,200)
  OK(op->advance_to(target, rid, score));
  ASSERT_DOUBLE_EQ(score_5_200, score);
  ASSERT_LE(score, tuple->get_max_score());

  target.set_uint64(expect_doc(OP_START, 1));  // i=1 odd -> (3,100)
  OK(op->advance_to(target, rid, score));
  ASSERT_DOUBLE_EQ(score_3_100, score);
  ASSERT_LE(score, tuple->get_max_score());

  // The losing pair is strictly below the block-max (the two pairs score differently,
  // so the frontier's max beats at least one of them).
  ASSERT_LT(std::min(score_5_200, score_3_100), tuple->get_max_score());
  OK(op->close());
}

// advance_shallow to a target beyond the last doc returns OB_ITER_END and leaves the
// tuple unset (no block to position on).
TEST_F(TestDASAsyncTokenOp, advance_shallow_beyond_last_iter_end)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.0, OP_DOC_CNT,
      OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());

  ObDASRowID target;
  target.set_uint64(expect_doc(OP_START, OP_DOC_CNT - 1) + 1);  // one past the last doc
  const MaxScoreTuple *tuple = reinterpret_cast<const MaxScoreTuple *>(0xdeadbeef);
  ASSERT_EQ(OB_ITER_END, op->advance_shallow(target, true /*inclusive*/, tuple));
  ASSERT_EQ(nullptr, tuple);
  OK(op->close());
}

// calc_max_score returns dim_max_score * token_boost and bounds every real row score.
TEST_F(TestDASAsyncTokenOp, calc_max_score_conservative)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  const double token_boost = 2.5;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost, OP_DOC_CNT,
      OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());

  double threshold = 1.0;
  OK(op->calc_max_score(threshold));
  ASSERT_DOUBLE_EQ(1.0 + op->dim_max_score_ * token_boost, threshold);

  ObDASRowID rid;
  double score = 0.0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = op->next_rowid(rid, score))) {
    ASSERT_LE(score, op->dim_max_score_ * token_boost);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  OK(op->close());
}

// After close, next_rowid returns OB_NOT_INIT.
TEST_F(TestDASAsyncTokenOp, close_then_next_not_init)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.0, OP_DOC_CNT,
      OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());
  OK(op->close());

  ObDASRowID rid;
  double score = 0.0;
  ASSERT_EQ(OB_NOT_INIT, op->next_rowid(rid, score));
}

// =============================================================================
// Batch drain (do_drain_until override) tests. Every case cross-checks the batch
// output against the row-by-row next_rowid() oracle (the exact reference the base
// do_drain_until would produce): same doc ids, same BM25 scores, same terminal
// state (EOF vs first row > boundary).
// =============================================================================

// Batch drain matches the scalar oracle across capacities and boundary positions
// (below-first, mid, gap, block-edge, block-start, last, and beyond -> EOF).
TEST_F(TestDASAsyncTokenOp, drain_matches_scalar_oracle)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  const int64_t caps[] = {1, 7, 255, 256, 257, 1024};
  const uint64_t boundaries[] = {
      0,                                          // below first -> count 0, next = first
      expect_doc(OP_START, 500),                  // mid, present
      expect_doc(OP_START, 500) + 1,              // gap (absent even id)
      expect_doc(OP_START, 255),                  // posting-block end
      expect_doc(OP_START, 256),                  // posting-block start
      expect_doc(OP_START, OP_DOC_CNT - 1),       // last present
      expect_doc(OP_START, OP_DOC_CNT - 1) + 100  // beyond last -> EOF
  };
  for (size_t c = 0; c < sizeof(caps) / sizeof(caps[0]); ++c) {
    for (size_t b = 0; b < sizeof(boundaries) / sizeof(boundaries[0]); ++b) {
      expect_drain_matches_oracle(inv_sstable, read_info, query_token, 3.0 /*token_boost*/,
          OP_DOC_CNT, OP_DOC_CNT, caps[c], boundaries[b]);
    }
  }
}

// out_scores == nullptr: doc ids still match; the prefetched next_score is still computed.
TEST_F(TestDASAsyncTokenOp, drain_out_scores_null)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 3.0,
      OP_DOC_CNT, OP_DOC_CNT, 256, expect_doc(OP_START, 500), false /*with_scores*/);
  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 3.0,
      OP_DOC_CNT, OP_DOC_CNT, 100, expect_doc(OP_START, OP_DOC_CNT - 1) + 100, false);
}

// Heterogeneous (token_freq, doc_length) per doc: the projected columns are scored
// per row and must match the oracle exactly across posting blocks.
TEST_F(TestDASAsyncTokenOp, drain_heterogeneous_params)
{
  common::ObArray<uint64_t> token_freqs;
  common::ObArray<uint64_t> doc_lengths;
  for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
    OK(token_freqs.push_back(1 + static_cast<uint64_t>(i % 7)));
    OK(doc_lengths.push_back(50 + static_cast<uint64_t>(i % 13)));
  }
  ObInvSSTable inv_sstable;
  build_single_term_sstable_per_doc(OP_START, OP_DOC_CNT, OP_TERM_SEQ,
      token_freqs, doc_lengths, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 1.7,
      OP_DOC_CNT, OP_DOC_CNT, 256, expect_doc(OP_START, OP_DOC_CNT - 1) + 100);
  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 1.7,
      OP_DOC_CNT, OP_DOC_CNT, 64, expect_doc(OP_START, 500));
}

// Non-unit token_boost and non-default avg_doc_length both flow into the scores.
TEST_F(TestDASAsyncTokenOp, drain_token_boost_and_nondefault_avg)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 2.5 /*token_boost*/,
      OP_DOC_CNT, OP_DOC_CNT, 100, expect_doc(OP_START, 400), true /*with_scores*/,
      0 /*pre_next*/, 250.0 /*cached_avg*/);
}

// Edge corpus stats still keep scalar/batch scores identical: doc_freq == 0 and
// doc_freq > total_doc_cnt (both allowed by the estimator).
TEST_F(TestDASAsyncTokenOp, drain_doc_freq_edges)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);
  const uint64_t eof_boundary = expect_doc(OP_START, OP_DOC_CNT - 1) + 100;

  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 1.0,
      OP_DOC_CNT, 0 /*doc_freq*/, 256, eof_boundary);
  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 1.0,
      OP_DOC_CNT, 2 * OP_DOC_CNT /*doc_freq > total*/, 256, eof_boundary);
}

// Scalar -> batch interleaving: consuming rows via next_rowid() first, then draining,
// must not repeat the consumed rows (drain starts after the current cursor).
TEST_F(TestDASAsyncTokenOp, drain_after_next_rowid)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 3.0,
      OP_DOC_CNT, OP_DOC_CNT, 256, expect_doc(OP_START, 300), true, 1 /*pre_next*/);
  expect_drain_matches_oracle(inv_sstable, read_info, query_token, 3.0,
      OP_DOC_CNT, OP_DOC_CNT, 64, expect_doc(OP_START, 500), true, 5 /*pre_next*/);
}

// Batch -> scalar interleaving: after a boundary-terminated drain, next_rowid() resumes
// from the row following the prefetched boundary row.
TEST_F(TestDASAsyncTokenOp, drain_then_next_rowid_continues)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 3.0, OP_DOC_CNT,
      OP_DOC_CNT, read_info, inv_sstable, op);
  OK(op->open());

  ObDASRowID boundary;
  boundary.set_uint64(expect_doc(OP_START, 100));  // rows 0..100 are <= boundary
  uint64_t bi[256];
  double bs[256];
  int64_t count = 0;
  ObDASRowID next_id;
  double next_score = 0.0;
  OK(op->drain_until(boundary, 256, bi, bs, count, next_id, next_score));
  ASSERT_EQ(101, count);
  ASSERT_EQ(expect_doc(OP_START, 100), bi[100]);
  ASSERT_FALSE(next_id.is_max());
  ASSERT_EQ(expect_doc(OP_START, 101), next_id.get_uint64());

  // The cursor sits on the prefetched boundary row (101); next_rowid advances past it.
  ObDASRowID rid;
  double s = 0.0;
  OK(op->next_rowid(rid, s));
  ASSERT_EQ(expect_doc(OP_START, 102), rid.get_uint64());
  OK(op->close());
}

// Absent term: the first drain returns 0 rows and next_id_after == max (EOF).
TEST_F(TestDASAsyncTokenOp, drain_absent_term)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  char term_buf[32] = {0};
  format_term(999999999, term_buf, sizeof(term_buf));  // absent term
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.0, OP_DOC_CNT,
      0 /*doc_freq*/, read_info, inv_sstable, op);
  OK(op->open());

  ObDASRowID boundary;
  boundary.set_uint64(expect_doc(OP_START, OP_DOC_CNT - 1) + 100);
  uint64_t bi[256];
  double bs[256];
  int64_t count = 0;
  ObDASRowID next_id;
  double next_score = 0.0;
  OK(op->drain_until(boundary, 256, bi, bs, count, next_id, next_score));
  ASSERT_EQ(0, count);
  ASSERT_TRUE(next_id.is_max());
  OK(op->close());
}

// Exercise the concrete AsyncToken fast path and the original masked path through
// the same merge iterator, including the cached current row and boundary lookahead.
TEST_F(TestDASAsyncTokenOp, accumulated_merge_drain_matches_bm25_across_boundaries)
{
  common::ObArray<uint64_t> token_freqs;
  common::ObArray<uint64_t> doc_lengths;
  for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
    OK(token_freqs.push_back(1 + static_cast<uint64_t>(i % 7)));
    OK(doc_lengths.push_back(50 + static_cast<uint64_t>(i % 13)));
  }
  ObInvSSTable inv_sstable;
  build_single_term_sstable_per_doc(OP_START, OP_DOC_CNT, OP_TERM_SEQ,
      token_freqs, doc_lengths, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);
  ObArenaAllocator ctx_alloc("FusedDrain");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.7, OP_DOC_CNT,
      OP_DOC_CNT, read_info, inv_sstable, op);
  OK(op->open());
  sql::ObDAATMergeIter iter;
  OK(iter.init(0, *op));
  const int64_t capacity = expect_doc(OP_START, OP_DOC_CNT - 1) - OP_START + 1;
  sql::DocScoreAccumulator accumulator;
  OK(accumulator.init(capacity, ctx_alloc));
  void *mask_buf = ctx_alloc.alloc(sql::ObBitVector::memory_size(capacity));
  ASSERT_NE(nullptr, mask_buf);
  sql::ObBitVector *mask = sql::to_bit_vector(mask_buf);
  mask->init(capacity);
  mask->reset(capacity);
  for (int64_t i = 0; i < OP_DOC_CNT; i += 2) {
    mask->set(expect_doc(OP_START, i) - OP_START);
  }
  for (const bool filtered : {false, true}) {
    for (const int64_t first_count : {0, 1, 255, 256, 257, 513, 1000}) {
      SCOPED_TRACE(::testing::Message() << "filtered=" << filtered
          << " first_count=" << first_count);
      OK(op->rescan());
      iter.reuse();
      OK(iter.next());
      uint64_t max_id = 0;
      OK(accumulator.reuse_with_start_id(OP_START, max_id));
      if (filtered) {
        accumulator.set_filter_mask(mask);
      } else {
        accumulator.clear_filter_mask();
      }
      ObDASRowID boundary;
      boundary.set_uint64(first_count == 0 ? OP_START - 1 : expect_doc(OP_START, first_count - 1));
      OK(iter.drain_until(boundary, accumulator));
      // Repeating the same boundary must not accumulate the cached lookahead twice.
      OK(iter.drain_until(boundary, accumulator));
      if (first_count < OP_DOC_CNT) {
        ObDASRowID next_id;
        double next_score = 0.0;
        OK(iter.get_curr_id(next_id));
        OK(iter.get_curr_score(next_score));
        EXPECT_EQ(expect_doc(OP_START, first_count), next_id.get_uint64());
        const double expected = fast_ref_score(
            token_freqs.at(first_count), doc_lengths.at(first_count), OP_DOC_CNT,
            OP_DOC_CNT, sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, 1.7);
        EXPECT_EQ(0, MEMCMP(&expected, &next_score, sizeof(double)));
      }
      boundary.set_uint64(max_id);
      OK(iter.drain_until(boundary, accumulator));
      EXPECT_TRUE(iter.iter_end());
      EXPECT_FALSE(iter.curr_score_materialized_);
      for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
        const uint64_t id = expect_doc(OP_START, i);
        const bool present = !filtered || i % 2 == 0;
        EXPECT_EQ(present, accumulator.bitset_->exist(id - OP_START));
        if (present) {
          double score = 0.0;
          OK(accumulator.get_score(id, score));
          const double expected = fast_ref_score(token_freqs.at(i),
              doc_lengths.at(i), OP_DOC_CNT, OP_DOC_CNT,
              sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, 1.7);
          EXPECT_EQ(0, MEMCMP(&expected, &score, sizeof(double)));
        }
      }
    }
  }
  OK(op->close());
}

TEST_F(TestDASAsyncTokenOp, active_drain_bitmap_boundaries)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);
  const uint64_t active_start = OP_START;
  const int64_t active_capacity = static_cast<int64_t>(
      expect_doc(OP_START, OP_DOC_CNT - 1) - active_start + 1);
  constexpr double INITIAL_SCORE = 1.0;

  auto run_case = [&](const std::vector<int64_t> &active_indexes,
                      const bool full_mask,
                      const int64_t boundary_index) {
    ObArenaAllocator ctx_alloc("ActiveDrain");
    AsyncTokCtx ctx(ctx_alloc);
    sql::ObDASIRScanCtDef ctdef(ctx_alloc);
    sql::ObDASIRScanRtDef rtdef;
    sql::ObDASAsyncTokenOp *op = nullptr;
    create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.7, OP_DOC_CNT,
        OP_DOC_CNT, read_info, inv_sstable, op);
    OK(op->open());

    sql::DocScoreAccumulator accumulator;
    uint64_t max_id = 0;
    OK(accumulator.init(active_capacity, ctx_alloc));
    OK(accumulator.reuse_with_start_id(active_start, max_id));
    ASSERT_EQ(expect_doc(OP_START, OP_DOC_CNT - 1), max_id);

    // Cover empty, single-bit, cross-word, tail, and full active candidate masks.
    std::vector<uint8_t> active_flags(OP_DOC_CNT, 0);
    if (full_mask) {
      for (int64_t i = 0; i <= boundary_index; ++i) {
        active_flags[i] = 1;
      }
    }
    for (size_t i = 0; i < active_indexes.size(); ++i) {
      ASSERT_GE(active_indexes[i], 0);
      ASSERT_LT(active_indexes[i], OP_DOC_CNT);
      active_flags[active_indexes[i]] = 1;
    }
    for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
      if (active_flags[i]) {
        OK(accumulator.add_dim(expect_doc(OP_START, i), INITIAL_SCORE));
      }
    }

    ObDASRowID boundary;
    boundary.set_uint64(expect_doc(OP_START, boundary_index));
    OK(op->drain_until_active(boundary, accumulator));

    const double bm25_score = fast_ref_score(1, 100, OP_DOC_CNT, OP_DOC_CNT,
        sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, 1.7);
    for (int64_t i = 0; i <= boundary_index; ++i) {
      double score = 0.0;
      OK(accumulator.get_score(expect_doc(OP_START, i), score));
      EXPECT_DOUBLE_EQ(active_flags[i] ? INITIAL_SCORE + bm25_score : 0.0, score);
    }
    if (boundary_index + 1 < OP_DOC_CNT) {
      ObDASRowID next_target;
      ObDASRowID next_id;
      double next_score = 0.0;
      next_target.set_uint64(expect_doc(OP_START, boundary_index + 1));
      OK(op->advance_to(next_target, next_id, next_score));
      EXPECT_EQ(next_target.get_uint64(), next_id.get_uint64());
      EXPECT_DOUBLE_EQ(bm25_score, next_score);
    } else {
      ObDASRowID end_id;
      double end_score = 0.0;
      EXPECT_EQ(OB_ITER_END, op->advance_to(boundary, end_id, end_score));
    }
    OK(op->close());
  };

  run_case({}, false, 130);
  run_case({0}, false, 130);
  run_case({31, 32, 63, 64, 129}, false, 130);
  run_case({}, true, 130);
  run_case({}, true, OP_DOC_CNT - 1);
}

// An absent active start is rounded up to the next posting row, which is then included.
TEST_F(TestDASAsyncTokenOp, active_drain_starts_at_absent_id)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);
  const int64_t first_index = 11;
  const uint64_t active_start = expect_doc(OP_START, first_index - 1) + 1;
  const uint64_t last_id = expect_doc(OP_START, OP_DOC_CNT - 1);
  const int64_t active_capacity = static_cast<int64_t>(last_id - active_start + 1);
  constexpr double INITIAL_SCORE = 1.0;

  ObArenaAllocator ctx_alloc("AbsentActive");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.7, OP_DOC_CNT,
      OP_DOC_CNT, read_info, inv_sstable, op);
  OK(op->open());

  sql::DocScoreAccumulator accumulator;
  uint64_t max_id = 0;
  OK(accumulator.init(active_capacity, ctx_alloc));
  OK(accumulator.reuse_with_start_id(active_start, max_id));
  ASSERT_EQ(last_id, max_id);
  OK(accumulator.add_dim(expect_doc(OP_START, first_index), INITIAL_SCORE));
  OK(accumulator.add_dim(expect_doc(OP_START, first_index + 2), INITIAL_SCORE));

  ObDASRowID boundary;
  boundary.set_uint64(expect_doc(OP_START, 130));
  OK(op->drain_until_active(boundary, accumulator));

  const double bm25_score = fast_ref_score(1, 100, OP_DOC_CNT, OP_DOC_CNT,
      sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT, 1.7);
  double score = 0.0;
  OK(accumulator.get_score(expect_doc(OP_START, first_index), score));
  EXPECT_DOUBLE_EQ(INITIAL_SCORE + bm25_score, score);
  OK(accumulator.get_score(expect_doc(OP_START, first_index + 1), score));
  EXPECT_DOUBLE_EQ(0.0, score);
  OK(accumulator.get_score(expect_doc(OP_START, first_index + 2), score));
  EXPECT_DOUBLE_EQ(INITIAL_SCORE + bm25_score, score);

  ObDASRowID next_target;
  ObDASRowID next_id;
  double next_score = 0.0;
  next_target.set_uint64(expect_doc(OP_START, 131));
  OK(op->advance_to(next_target, next_id, next_score));
  EXPECT_EQ(next_target.get_uint64(), next_id.get_uint64());
  EXPECT_DOUBLE_EQ(bm25_score, next_score);
  OK(op->close());

  ObArenaAllocator end_ctx_alloc("AbsentActiveEnd");
  AsyncTokCtx end_ctx(end_ctx_alloc);
  sql::ObDASIRScanCtDef end_ctdef(end_ctx_alloc);
  sql::ObDASIRScanRtDef end_rtdef;
  sql::ObDASAsyncTokenOp *end_op = nullptr;
  create_and_inject_single(end_ctx, end_ctdef, end_rtdef, query_token, 1.7, OP_DOC_CNT,
      OP_DOC_CNT, read_info, inv_sstable, end_op);
  OK(end_op->open());

  const uint64_t beyond_start = last_id + 10;
  sql::DocScoreAccumulator end_accumulator;
  OK(end_accumulator.init(16, end_ctx_alloc));
  OK(end_accumulator.reuse_with_start_id(beyond_start, max_id));
  boundary.set_uint64(beyond_start);
  OK(end_op->drain_until_active(boundary, end_accumulator));
  EXPECT_TRUE(end_accumulator.is_empty());
  ObDASRowID end_id;
  double end_score = 0.0;
  EXPECT_EQ(OB_ITER_END, end_op->advance_to(boundary, end_id, end_score));
  OK(end_op->close());
}

// Compare AsyncToken's batch active drain with the scalar fallback across bitset edges, ensuring
// both score only live candidates, leave pruned candidates inactive, and stop at the same row
// immediately after the boundary.
TEST_F(TestDASAsyncTokenOp, active_drain_matches_fallback_without_reviving_pruned_candidates)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);
  const uint64_t active_start = OP_START;
  const int64_t active_capacity = 401;
  const int64_t kept_indexes[] = {0, 31, 63, 129, 130, 150};
  const int64_t pruned_indexes[] = {32, 64};
  const int64_t boundary_index = 130;

  auto run_path = [&](const bool use_async_batch,
                      std::vector<double> &scores,
                      int64_t &drained_count,
                      uint64_t &next_id) {
    ObArenaAllocator ctx_alloc(use_async_batch ? "ActiveBatch" : "ActiveFallback");
    AsyncTokCtx ctx(ctx_alloc);
    ctx.search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    sql::ObDASIRScanCtDef ctdef(ctx_alloc);
    sql::ObDASIRScanRtDef rtdef;
    sql::ObDASAsyncTokenOp *op = nullptr;
    create_and_inject_single(ctx, ctdef, rtdef, query_token, 1.7, OP_DOC_CNT,
        OP_DOC_CNT, read_info, inv_sstable, op);
    OK(op->open());

    sql::DocScoreAccumulator accumulator;
    uint64_t max_id = 0;
    OK(accumulator.init(active_capacity, ctx_alloc));
    OK(accumulator.reuse_with_start_id(active_start, max_id));
    for (int64_t idx : kept_indexes) {
      OK(accumulator.add_dim(expect_doc(OP_START, idx), 2.0));
    }
    for (int64_t idx : pruned_indexes) {
      OK(accumulator.add_dim(expect_doc(OP_START, idx), 0.5));
    }
    OK(accumulator.filter_below_score(1.0));

    ObDASRowID boundary;
    boundary.set_uint64(expect_doc(OP_START, boundary_index));
    if (use_async_batch) {
      OK(op->do_drain_until_active(boundary, accumulator, drained_count));
    } else {
      OK(op->ObIDASSearchOp::do_drain_until_active(boundary, accumulator, drained_count));
    }
    scores.clear();
    for (int64_t idx : kept_indexes) {
      double score = 0.0;
      OK(accumulator.get_score(expect_doc(OP_START, idx), score));
      scores.push_back(score);
    }
    for (int64_t idx : pruned_indexes) {
      double score = -1.0;
      OK(accumulator.get_score(expect_doc(OP_START, idx), score));
      ASSERT_DOUBLE_EQ(0.0, score) << "pruned index " << idx;
    }
    ObDASRowID next_target;
    ObDASRowID advanced_id;
    double advanced_score = 0.0;
    next_target.set_uint64(expect_doc(OP_START, boundary_index + 1));
    OK(op->advance_to(next_target, advanced_id, advanced_score));
    next_id = advanced_id.get_uint64();
    OK(op->close());
  };

  std::vector<double> batch_scores;
  std::vector<double> fallback_scores;
  int64_t batch_count = 0;
  int64_t fallback_count = 0;
  uint64_t batch_next_id = 0;
  uint64_t fallback_next_id = 0;
  run_path(true, batch_scores, batch_count, batch_next_id);
  run_path(false, fallback_scores, fallback_count, fallback_next_id);
  ASSERT_EQ(fallback_count, batch_count);
  ASSERT_EQ(5, batch_count);
  ASSERT_EQ(fallback_scores.size(), batch_scores.size());
  for (size_t i = 0; i < batch_scores.size(); ++i) {
    ASSERT_DOUBLE_EQ(fallback_scores[i], batch_scores[i]) << "candidate " << i;
  }
  ASSERT_EQ(fallback_next_id, batch_next_id);
  ASSERT_EQ(expect_doc(OP_START, boundary_index + 1), batch_next_id);
}

// Batch drain scores match the canonical ObExprBM25::eval() formula exactly (independent
// oracle, heterogeneous per-doc params), validating the raw batch kernel and the
// query_token_weight_ / avg_doc_length wiring.
TEST_F(TestDASAsyncTokenOp, drain_scores_match_canonical_bm25)
{
  common::ObArray<uint64_t> token_freqs;
  common::ObArray<uint64_t> doc_lengths;
  for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
    OK(token_freqs.push_back(1 + static_cast<uint64_t>(i % 5)));
    OK(doc_lengths.push_back(80 + static_cast<uint64_t>(i % 17)));
  }
  ObInvSSTable inv_sstable;
  build_single_term_sstable_per_doc(OP_START, OP_DOC_CNT, OP_TERM_SEQ,
      token_freqs, doc_lengths, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObArenaAllocator ctx_alloc("AsyncTokCtx");
  AsyncTokCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  const double token_boost = 2.0;
  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, token_boost, OP_DOC_CNT,
      OP_DOC_CNT, read_info, inv_sstable, op);
  OK(op->open());
  const int64_t df = op->posting_scan_param_.doc_freq_;
  const int64_t total = op->posting_scan_param_.total_doc_cnt_;
  const double avg = op->posting_scan_param_.avg_doc_length_;

  ObDASRowID boundary;
  boundary.set_uint64(expect_doc(OP_START, OP_DOC_CNT - 1) + 100);  // beyond last -> full drain
  uint64_t bi[256];
  double bs[256];
  int64_t seen = 0;
  bool done = false;
  while (!done) {
    int64_t count = 0;
    ObDASRowID nid;
    double nscore = 0.0;
    OK(op->drain_until(boundary, 256, bi, bs, count, nid, nscore));
    for (int64_t i = 0; i < count; ++i) {
      const int64_t j = seen + i;
      // Exact: batch drain output equals the shared single-division kernel.
      const double expected_fast = fast_ref_score(
          token_freqs.at(j), doc_lengths.at(j), df, total, avg, token_boost);
      // Approximate: the canonical two-division formula agrees within tolerance.
      const double canonical = sql::ObExprBM25::eval(
          token_freqs.at(j), doc_lengths.at(j), df, total, avg) * token_boost;
      ASSERT_EQ(expect_doc(OP_START, j), bi[i]) << "doc id mismatch at " << j;
      ASSERT_DOUBLE_EQ(expected_fast, bs[i]) << "fast BM25 score mismatch at " << j;
      ASSERT_TRUE(bm25_close(bs[i], canonical)) << "canonical BM25 mismatch at " << j;
    }
    seen += count;
    if (count < 256) {
      ASSERT_TRUE(nid.is_max());
      done = true;
    }
  }
  ASSERT_EQ(OP_DOC_CNT, seen);
  OK(op->close());
}

// ============================================================================
// BM25 precomputed-params unit / differential / block-max-safety tests.
// ============================================================================

// init_precomputed_params fills the constants exactly once; rejects illegal inputs;
// reset and repeated init behave correctly; scalar kernel handles boundary inputs.
TEST_F(TestDASAsyncTokenOp, bm25_precompute_params_unit)
{
  using sql::ObExprBM25;
  using sql::ObBM25PrecomputedParams;
  const double nan_v = std::numeric_limits<double>::quiet_NaN();
  const double inf_v = std::numeric_limits<double>::infinity();

  ObBM25PrecomputedParams p;
  ASSERT_FALSE(p.is_inited_);  // constructed via reset()

  // Typical params (k1=1.2, b=0.75 are internal): only avg feeds doc_len_scale.
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(2.0, 10.0, 1.5, p));
  ASSERT_TRUE(p.is_inited_);
  ASSERT_DOUBLE_EQ(2.0, p.query_weight_);
  ASSERT_DOUBLE_EQ(1.5, p.score_multiplier_);
  ASSERT_DOUBLE_EQ(1.2 * (1.0 - 0.75), p.denom_bias_);
  ASSERT_DOUBLE_EQ(1.2 * 0.75 / 10.0, p.doc_len_scale_);

  p.reset();
  ASSERT_FALSE(p.is_inited_);
  ASSERT_DOUBLE_EQ(0.0, p.query_weight_);

  // Illegal avg_doc_length.
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObExprBM25::init_precomputed_params(2.0, 0.0, 1.0, p));
  ASSERT_FALSE(p.is_inited_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObExprBM25::init_precomputed_params(2.0, -5.0, 1.0, p));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObExprBM25::init_precomputed_params(2.0, nan_v, 1.0, p));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObExprBM25::init_precomputed_params(2.0, inf_v, 1.0, p));
  // Illegal weight / multiplier.
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObExprBM25::init_precomputed_params(nan_v, 10.0, 1.0, p));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObExprBM25::init_precomputed_params(2.0, 10.0, inf_v, p));
  ASSERT_FALSE(p.is_inited_);  // failed init must not leave a half-inited struct

  // Repeated init overwrites cleanly.
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(3.0, 20.0, 2.0, p));
  ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(4.0, 40.0, 1.0, p));
  ASSERT_DOUBLE_EQ(4.0, p.query_weight_);
  ASSERT_DOUBLE_EQ(1.2 * 0.75 / 40.0, p.doc_len_scale_);

  // Boundary scoring: tf=0 -> exactly 0; tf/dl extremes stay finite and non-negative.
  ASSERT_DOUBLE_EQ(0.0, ObExprBM25::eval_with_precomputed_params(0, 100, p));
  ASSERT_DOUBLE_EQ(0.0, ObExprBM25::eval_with_precomputed_params(0, 0, p));
  const double s_dl0 = ObExprBM25::eval_with_precomputed_params(3, 0, p);
  ASSERT_GE(s_dl0, 0.0);
  ASSERT_TRUE(std::isfinite(s_dl0));
  const double s_big = ObExprBM25::eval_with_precomputed_params(1000000, 1, p);
  ASSERT_GE(s_big, 0.0);
  ASSERT_TRUE(std::isfinite(s_big));
  // tf == avg boundary and dl far above avg both finite.
  ASSERT_TRUE(std::isfinite(ObExprBM25::eval_with_precomputed_params(40, 40, p)));
  ASSERT_TRUE(std::isfinite(ObExprBM25::eval_with_precomputed_params(1, 1000000, p)));
}

// Fixed-seed differential test: the fast single-division kernel matches the canonical
// two-division ObExprBM25::eval() within tolerance, and the batch kernel is bit-for-bit
// identical to the scalar kernel. Reports the observed max errors so the tolerance
// constants (kBM25AbsTol / kBM25RelTol) can be justified.
TEST_F(TestDASAsyncTokenOp, bm25_random_differential)
{
  using sql::ObExprBM25;
  using sql::ObBM25PrecomputedParams;
  std::mt19937_64 rng(20260720ULL);  // fixed seed: failures reproduce exactly
  std::uniform_int_distribution<uint64_t> tf_dist(0, 100000);
  std::uniform_int_distribution<uint64_t> dl_dist(0, 100000);
  std::uniform_int_distribution<int64_t> total_dist(1, 1000000);
  std::uniform_real_distribution<double> avg_dist(0.5, 5000.0);
  std::uniform_real_distribution<double> boost_dist(0.1, 10.0);

  double max_abs = 0.0;
  double max_rel = 0.0;

  // Part 1: fast vs canonical over thousands of independent samples.
  const int64_t kSamples = 20000;
  for (int64_t s = 0; s < kSamples; ++s) {
    const int64_t total = total_dist(rng);
    std::uniform_int_distribution<int64_t> df_dist(0, total);
    const int64_t df = df_dist(rng);
    const double avg = avg_dist(rng);
    const double boost = boost_dist(rng);
    const uint64_t tf = tf_dist(rng);
    const uint64_t dl = dl_dist(rng);

    ObBM25PrecomputedParams p;
    ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(
        ObExprBM25::query_token_weight(df, total), avg, boost, p));
    const double fast = ObExprBM25::eval_with_precomputed_params(tf, dl, p);
    const double canonical = ObExprBM25::eval(
        static_cast<int64_t>(tf), static_cast<int64_t>(dl), df, total, avg) * boost;

    ASSERT_TRUE(std::isfinite(fast)) << "s=" << s;
    ASSERT_GE(fast, 0.0) << "s=" << s;
    const double abs_err = std::fabs(fast - canonical);
    const double denom = std::max(std::fabs(fast), std::fabs(canonical));
    max_abs = std::max(max_abs, abs_err);
    max_rel = std::max(max_rel, denom > 0.0 ? abs_err / denom : 0.0);
    EXPECT_TRUE(bm25_close(fast, canonical))
        << "s=" << s << " tf=" << tf << " dl=" << dl << " df=" << df
        << " total=" << total << " avg=" << avg << " boost=" << boost;
  }

  // Part 2: scalar vs batch, bit-for-bit (shared params per batch).
  for (int64_t round = 0; round < 500; ++round) {
    const int64_t total = total_dist(rng);
    std::uniform_int_distribution<int64_t> df_dist(0, total);
    const int64_t df = df_dist(rng);
    const double avg = avg_dist(rng);
    const double boost = boost_dist(rng);
    ObBM25PrecomputedParams p;
    ASSERT_EQ(OB_SUCCESS, ObExprBM25::init_precomputed_params(
        ObExprBM25::query_token_weight(df, total), avg, boost, p));

    const int64_t cnt = 1 + (round % 128);
    std::vector<uint64_t> tfv(cnt);
    std::vector<uint64_t> dlv(cnt);
    for (int64_t i = 0; i < cnt; ++i) {
      tfv[i] = tf_dist(rng);
      dlv[i] = dl_dist(rng);
    }
    std::vector<double> bscores(cnt, -1.0);
    ObExprBM25::eval_batch_with_precomputed_params(tfv.data(), dlv.data(), cnt, p, bscores.data());
    for (int64_t i = 0; i < cnt; ++i) {
      const double scalar = ObExprBM25::eval_with_precomputed_params(tfv[i], dlv[i], p);
      ASSERT_EQ(scalar, bscores[i])  // exact bit equality required
          << "scalar/batch mismatch round=" << round << " i=" << i;
    }
  }

  RecordProperty("bm25_max_abs_err", std::to_string(max_abs));
  RecordProperty("bm25_max_rel_err", std::to_string(max_rel));
}

// Block-max upper bound holds for EVERY row in the block, at boost < 1, == 1 and > 1.
// A Pareto frontier (alternating (5,200)/(3,100)) keeps two points per block. For each
// block we compare the storage (un-boosted) bound and the SQL (boosted) bound against
// each row's fast score via direct inequality — the bound must never be exceeded.
TEST_F(TestDASAsyncTokenOp, block_max_upper_bounds_rows_boost_sweep)
{
  ObInvSSTable inv_sstable;
  common::ObSEArray<uint64_t, 1024> token_freqs;
  common::ObSEArray<uint64_t, 1024> doc_lengths;
  for (int64_t i = 0; i < OP_DOC_CNT; ++i) {
    if (0 == i % 2) {
      OK(token_freqs.push_back(5));
      OK(doc_lengths.push_back(200));
    } else {
      OK(token_freqs.push_back(3));
      OK(doc_lengths.push_back(100));
    }
  }
  build_single_term_sstable_per_doc(OP_START, OP_DOC_CNT, OP_TERM_SEQ,
      token_freqs, doc_lengths, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  const int64_t block_sizes[4] = {256, 256, 256, 232};
  const double boosts[3] = {0.5, 1.0, 2.0};
  for (int64_t bi = 0; bi < 3; ++bi) {
    const double boost = boosts[bi];
    ObArenaAllocator ca("BMSweep");
    AsyncTokCtx ctx(ca);
    sql::ObDASIRScanCtDef ctdef(ca);
    sql::ObDASIRScanRtDef rtdef;
    sql::ObDASAsyncTokenOp *op = nullptr;
    create_and_inject_single(ctx, ctdef, rtdef, query_token, boost, OP_DOC_CNT,
        OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op);
    OK(op->open());
    const double avg = op->posting_scan_param_.avg_doc_length_;
    const int64_t df = op->posting_scan_param_.doc_freq_;
    const int64_t total = op->posting_scan_param_.total_doc_cnt_;

    for (int64_t k = 0; k < 4; ++k) {
      ObDASRowID target;
      target.set_uint64(expect_doc(OP_START, k * 256));  // block start
      const MaxScoreTuple *tuple = nullptr;
      OK(op->advance_shallow(target, true /*inclusive*/, tuple));
      ASSERT_NE(nullptr, tuple);
      const double boosted_bound = tuple->get_max_score();
      uint64_t bmin = 0;
      uint64_t bmax = 0;
      double unboosted_bound = 0.0;
      OK(op->posting_iter_->get_block_max_tuple(bmin, bmax, unboosted_bound));
      for (int64_t i = k * 256; i < k * 256 + block_sizes[k]; ++i) {
        const uint64_t tf = token_freqs.at(i);
        const uint64_t dl = doc_lengths.at(i);
        const double unboosted = fast_ref_score(tf, dl, df, total, avg, 1.0);
        const double boosted = fast_ref_score(tf, dl, df, total, avg, boost);
        ASSERT_LE(unboosted, unboosted_bound)
            << "storage bound exceeded k=" << k << " i=" << i << " boost=" << boost;
        ASSERT_LE(boosted, boosted_bound)
            << "sql bound exceeded k=" << k << " i=" << i << " boost=" << boost;
      }
    }
    OK(op->close());
  }
}

// do_rescan_segment recomputes the BM25 scoring constants from the current segment's
// stats rather than reusing whatever was cached from the previous segment. We corrupt
// the constants after open, rescan the same segment, and confirm the row score is the
// fresh (correct) value again.
TEST_F(TestDASAsyncTokenOp, bm25_params_reinit_on_rescan_segment)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(OP_START, OP_DOC_CNT, OP_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObArenaAllocator ca("Rescan");
  AsyncTokCtx ctx(ca);
  sql::ObDASIRScanCtDef ctdef(ca);
  sql::ObDASIRScanRtDef rtdef;
  char term_buf[32] = {0};
  format_term(OP_TERM_SEQ, term_buf, sizeof(term_buf));
  const common::ObString query_token(static_cast<int32_t>(strlen(term_buf)), term_buf);

  const double boost = 2.0;
  const double cached_avg = 33.0;
  sql::ObDASAsyncTokenOp *op = nullptr;
  create_and_inject_single(ctx, ctdef, rtdef, query_token, boost, OP_DOC_CNT,
      OP_DOC_CNT /*cached_doc_freq*/, read_info, inv_sstable, op, cached_avg,
      false /*direct_bind*/);
  OK(op->open());
  const common::ObNewVersionRange &range = ctx.binding_.get_version_range();
  OK(op->rescan_segment(range));
  ASSERT_TRUE(op->bm25_calc_params_.is_inited_);
  const int64_t df = op->posting_scan_param_.doc_freq_;
  const int64_t total = op->posting_scan_param_.total_doc_cnt_;
  const double avg = op->posting_scan_param_.avg_doc_length_;
  ASSERT_DOUBLE_EQ(cached_avg, avg);

  ObDASRowID rid;
  double s0 = 0.0;
  OK(op->next_rowid(rid, s0));
  ASSERT_DOUBLE_EQ(fast_ref_score(1, 100, df, total, avg, boost), s0);

  // Corrupt the cached constants to emulate stale per-segment state.
  op->bm25_calc_params_.doc_len_scale_ = 999999.0;
  op->bm25_calc_params_.query_weight_ = -1.0;

  OK(op->rescan_segment(range));
  ASSERT_TRUE(op->bm25_calc_params_.is_inited_);
  const double avg2 = op->posting_scan_param_.avg_doc_length_;

  ObDASRowID rid2;
  double s1 = 0.0;
  OK(op->next_rowid(rid2, s1));
  ASSERT_EQ(rid.get_uint64(), rid2.get_uint64());
  ASSERT_DOUBLE_EQ(fast_ref_score(1, 100,
      op->posting_scan_param_.doc_freq_, op->posting_scan_param_.total_doc_cnt_, avg2, boost), s1);
  ASSERT_DOUBLE_EQ(s0, s1) << "rescan must reproduce the fresh score, not a stale one";
  OK(op->close());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_das_async_token_op.log*");
  OB_LOGGER.set_file_name("test_das_async_token_op.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
