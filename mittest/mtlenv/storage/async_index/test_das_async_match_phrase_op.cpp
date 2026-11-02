// owner: zhuwei.wzw

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "mittest/mtlenv/storage/blocksstable/test_inv_idx_sstable_prepare.h"
#include "share/ob_fts_pos_list_codec.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/retrieval/ob_phrase_match_counter.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_match_phrase_op.h"
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

constexpr uint64_t ASYNC_INDEX_TABLE_ID = 300001;

int encode_pos_list(
    common::ObIAllocator &alloc,
    const common::ObIArray<int64_t> &positions,
    common::ObString &out)
{
  int ret = OB_SUCCESS;
  share::ObFTSPositionListStore store;
  char *buf = nullptr;
  int64_t buf_len = 0;
  if (OB_FAIL(store.encode_and_serialize(alloc, positions, buf, buf_len))) {
    STORAGE_LOG(WARN, "failed to encode pos list", K(ret));
  } else {
    out.assign_ptr(buf, static_cast<int32_t>(buf_len));
  }
  return ret;
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
    ++posting_scan_count_;
    if (OB_ISNULL(inv_sstable_) || OB_ISNULL(read_info_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = inv_sstable_->scan_posting(posting_param, *read_info_, iter);
    }
    return ret;
  }
  ObInvSSTable *inv_sstable_ = nullptr;
  const storage::ObITableReadInfo *read_info_ = nullptr;
  int64_t posting_scan_count_ = 0;
};

struct PhraseOpCtx
{
  explicit PhraseOpCtx(common::ObIAllocator &alloc)
    : binding_(),
      scan_alloc_("PhraseOpCtx"),
      mock_scan_op_(scan_alloc_),
      search_ctx_(alloc, mock_scan_op_),
      exec_ctx_(alloc),
      eval_ctx_(exec_ctx_),
      rowid_exprs_(scan_alloc_)
  {
    search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    EXPECT_EQ(OB_SUCCESS, binding_.init(
        search_ctx_,
        share::ObLSID(TestInvIdxSSTablePrepare::LS_ID),
        ASYNC_INDEX_TABLE_ID,
        common::ObTabletID(TestInvIdxSSTablePrepare::TABLET_ID),
        0,
        TestInvIdxSSTablePrepare::SNAPSHOT_VERSION));
    setup_rowid_exprs();
    setup_bm25_exprs();
  }

  void setup_rowid_exprs()
  {
    rowid_expr_.reset();
    OK(rowid_exprs_.init(1));
    OK(rowid_exprs_.push_back(&rowid_expr_));
    search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.rowid_exprs_ = &rowid_exprs_;
    // write_datum_to_rowid / get_datum_from_rowid dispatch via scopes_[scope_],
    // not the legacy rowid_exprs_ alone (MatchPhraseOp uses those helpers).
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs_;
  }

  void setup_bm25_exprs()
  {
    static const int64_t kDatumCnt = 2;
    const int64_t frame_bytes = sizeof(sql::ObDatum) * kDatumCnt
        + sizeof(int64_t) + sizeof(double);
    char *frame0 = static_cast<char *>(alloc_.alloc(frame_bytes));
    ASSERT_NE(nullptr, frame0);
    char **frames = static_cast<char **>(alloc_.alloc(sizeof(char *)));
    ASSERT_NE(nullptr, frames);
    frames[0] = frame0;
    eval_ctx_.frames_ = frames;
    eval_ctx_.batch_idx_ = 0;

    total_doc_cnt_expr_.reset();
    total_doc_cnt_expr_.type_ = T_INT;
    total_doc_cnt_expr_.frame_idx_ = 0;
    total_doc_cnt_expr_.datum_off_ = 0;
    total_doc_cnt_expr_.res_buf_off_ = sizeof(sql::ObDatum) * kDatumCnt;
    total_doc_cnt_expr_.res_buf_len_ = sizeof(int64_t);

    avg_doc_expr_.reset();
    avg_doc_expr_.type_ = T_DOUBLE;
    avg_doc_expr_.frame_idx_ = 0;
    avg_doc_expr_.datum_off_ = sizeof(sql::ObDatum);
    avg_doc_expr_.res_buf_off_ = sizeof(sql::ObDatum) * kDatumCnt + sizeof(int64_t);
    avg_doc_expr_.res_buf_len_ = sizeof(double);

    for (int64_t i = 0; i < 6; ++i) {
      bm25_args_[i] = nullptr;
    }
    bm25_args_[sql::ObExprBM25::TOTAL_DOC_CNT_PARAM_IDX] = &total_doc_cnt_expr_;
    relevance_expr_.reset();
    relevance_expr_.arg_cnt_ = 6;
    relevance_expr_.args_ = bm25_args_;
  }

  void bind_ir_defs(sql::ObDASIRScanCtDef &ctdef, sql::ObDASIRScanRtDef &rtdef)
  {
    ctdef.use_scalar_scan_def_ = true;
    ctdef.relevance_expr_ = &relevance_expr_;
    ctdef.avg_doc_token_cnt_expr_ = &avg_doc_expr_;
    rtdef.eval_ctx_ = &eval_ctx_;
  }

  MockDASAsyncIndexBinding binding_;
  ObArenaAllocator scan_alloc_;
  sql::ObDASScanOp mock_scan_op_;
  sql::ObDASSearchCtx search_ctx_;
  sql::ObExecContext exec_ctx_;
  sql::ObEvalCtx eval_ctx_;
  sql::ObExpr relevance_expr_;
  sql::ObExpr total_doc_cnt_expr_;
  sql::ObExpr avg_doc_expr_;
  sql::ObExpr rowid_expr_;
  sql::ExprFixedArray rowid_exprs_;
  sql::ObExpr *bm25_args_[6];
  common::ObIAllocator &alloc_ = scan_alloc_;
};

void prefill_bm25_cache_base(
    sql::ObDASSearchCtx &search_ctx,
    const int64_t total_doc_cnt)
{
  storage::ObInvIdxParamCache &cache = search_ctx.get_bm25_param_cache();
  const common::ObTabletID tablet_id(TestInvIdxSSTablePrepare::TABLET_ID);
  uint64_t existing = 0;
  if (OB_ENTRY_NOT_EXIST == cache.get_total_doc_cnt(tablet_id, existing)) {
    OK(cache.set_total_doc_cnt(tablet_id, static_cast<uint64_t>(total_doc_cnt)));
    OK(cache.set_avg_doc_token_cnt(
        tablet_id, sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT));
  }
}

void prefill_token_doc_cnt(
    sql::ObDASSearchCtx &search_ctx,
    const common::ObString &query_token,
    const int64_t doc_freq)
{
  storage::ObInvIdxParamCache &cache = search_ctx.get_bm25_param_cache();
  OK(cache.set_token_doc_cnt(
      common::ObTabletID(TestInvIdxSSTablePrepare::TABLET_ID), query_token, doc_freq));
}

class CountingPhraseMatchCounter final : public storage::ObPhraseMatchCounter
{
public:
  CountingPhraseMatchCounter() : call_count_(0) {}
  int count_matches(
      const common::ObIArray<common::ObArray<int64_t>> &/*abs_pos_lists*/,
      double &total_match_count) override
  {
    ++call_count_;
    total_match_count = 0.0;
    return OB_SUCCESS;
  }
  int64_t call_count_;
protected:
  int inner_init(common::ObIAllocator &/*alloc*/) override { return OB_SUCCESS; }
  void inner_reset() override {}
};

} // namespace

class TestDASAsyncMatchPhraseOp : public TestInvIdxSSTablePrepare
{
public:
  struct PhraseRowSpec
  {
    const char *term_;
    uint64_t doc_id_;
    int64_t pos_;
    bool valid_pos_list_;
  };

  void build_phrase_sstable(
      const PhraseRowSpec *rows,
      const int64_t row_count,
      ObInvSSTable &inv_sstable)
  {
    ASSERT_NE(nullptr, rows);
    ASSERT_GT(row_count, 0);
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

    uint64_t min_doc_id = UINT64_MAX;
    uint64_t max_doc_id = 0;
    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    for (int64_t i = 0; i < row_count; ++i) {
      const PhraseRowSpec &spec = rows[i];
      common::ObString pos_list;
      if (spec.valid_pos_list_) {
        common::ObSEArray<int64_t, 4> positions;
        OK(positions.push_back(spec.pos_));
        OK(encode_pos_list(allocator_, positions, pos_list));
      } else {
        pos_list = common::ObString::make_string("corrupt");
      }
      common::ObString term_str(static_cast<int32_t>(strlen(spec.term_)), spec.term_);
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, spec.doc_id_,
          1 /*token_freq*/, 100 /*doc_length*/, pos_list));
      OK(writer.append_row(row));
      min_doc_id = MIN(min_doc_id, spec.doc_id_);
      max_doc_id = MAX(max_doc_id, spec.doc_id_);
    }
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(row_count, res.row_count_);

    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, res.inv_meta_);
    res.inv_meta_.min_doc_id_ = min_doc_id;
    res.inv_meta_.max_doc_id_ = max_doc_id;
    res.inv_meta_.total_doc_length_ = row_count;
    ASSERT_TRUE(res.inv_meta_.is_valid());

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
  }

  // Two-term inverted sstable for phrase matching:
  //   alpha @ doc 1 pos [0], alpha @ doc 2 pos [0], beta @ doc 1 pos [1]
  // Only doc 1 satisfies exact phrase "alpha beta".
  void build_two_term_phrase_sstable(ObInvSSTable &inv_sstable)
  {
    const PhraseRowSpec rows[] = {
      {"alpha", 1, 0, true},
      {"alpha", 2, 0, true},
      {"beta", 1, 1, true},
    };
    build_phrase_sstable(rows, ARRAYSIZEOF(rows), inv_sstable);
  }

  void inject_async_helpers(
      sql::ObDASMatchPhraseOp *op,
      const ObTableReadInfo &read_info,
      ObInvSSTable &inv_sstable)
  {
    mock_access_.inv_sstable_ = &inv_sstable;
    mock_access_.read_info_ = &read_info;
    for (int64_t i = 0; i < op->async_helpers_.count(); ++i) {
      op->async_helpers_[i]->set_tsc_service(&mock_access_);
      op->async_helpers_[i]->set_tablet_id(common::ObTabletID(TABLET_ID));
    }
  }

  MockAsyncIndexAccessService mock_access_;

  void open_phrase_op(
      sql::ObDASIRScanCtDef &ctdef,
      sql::ObDASIRScanRtDef &rtdef,
      PhraseOpCtx &ctx,
      sql::ObDASMatchPhraseOp *&op,
      ObInvSSTable &inv_sstable,
      ObTableReadInfo &read_info,
      const int64_t total_doc_cnt,
      const int64_t alpha_doc_freq,
      const int64_t beta_doc_freq)
  {
    build_rowkey_read_info(table_schema_, allocator_, read_info);

    ctdef.is_async_index_ = true;
    ctx.bind_ir_defs(ctdef, rtdef);

    storage::ObPhraseMatchCounter *counter = nullptr;
    common::ObSEArray<int64_t, 4> ids;
    common::ObSEArray<int64_t, 4> offsets;
    OK(ids.push_back(0));
    OK(ids.push_back(1));
    OK(offsets.push_back(0));
    OK(offsets.push_back(1));
    OK(storage::ObPhraseMatchCounter::create(allocator_, ids, offsets, 0 /*slop*/,
        false /*has_duplicate_tokens*/, counter));

    const common::ObString alpha = common::ObString::make_string("alpha");
    const common::ObString beta = common::ObString::make_string("beta");
    prefill_bm25_cache_base(ctx.search_ctx_, total_doc_cnt);
    prefill_token_doc_cnt(ctx.search_ctx_, alpha, alpha_doc_freq);
    prefill_token_doc_cnt(ctx.search_ctx_, beta, beta_doc_freq);

    sql::ObDASMatchPhraseOpParam param;
    param.ir_ctdef_ = &ctdef;
    param.ir_rtdef_ = &rtdef;
    param.block_max_param_ = nullptr;
    param.counter_ = counter;
    param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
    param.group_slot_ = 0;
    param.direct_bind_ = false;
    param.boost_ = 1.0;
    param.use_rich_format_ = false;
    OK(param.token_ids_.push_back(0));
    OK(param.token_ids_.push_back(1));
    OK(param.query_tokens_.push_back(alpha));
    OK(param.query_tokens_.push_back(beta));

    OK(ctx.search_ctx_.create_op(param, op));
    ASSERT_NE(nullptr, op);
    inject_async_helpers(op, read_info, inv_sstable);
    OK(op->open());
    common::ObNewVersionRange range;
    range.base_version_ = 0;
    range.snapshot_version_ = SNAPSHOT_VERSION;
    OK(op->rescan_segment(range));
  }

  void open_two_term_phrase_op(
      sql::ObDASIRScanCtDef &ctdef,
      sql::ObDASIRScanRtDef &rtdef,
      PhraseOpCtx &ctx,
      sql::ObDASMatchPhraseOp *&op,
      ObInvSSTable &inv_sstable,
      ObTableReadInfo &read_info)
  {
    build_two_term_phrase_sstable(inv_sstable);
    open_phrase_op(ctdef, rtdef, ctx, op, inv_sstable, read_info,
        2 /*total_doc_cnt*/, 2 /*alpha_doc_freq*/, 1 /*beta_doc_freq*/);
  }
};

TEST_F(TestDASAsyncMatchPhraseOp, param_is_valid_async_without_block_max)
{
  sql::ObDASIRScanCtDef ctdef(allocator_);
  sql::ObDASIRScanRtDef rtdef;
  ctdef.is_async_index_ = true;

  storage::ObPhraseMatchCounter *counter = nullptr;
  common::ObSEArray<int64_t, 4> ids;
  common::ObSEArray<int64_t, 4> offsets;
  OK(ids.push_back(0));
  OK(ids.push_back(1));
  OK(offsets.push_back(0));
  OK(offsets.push_back(1));
  OK(storage::ObPhraseMatchCounter::create(allocator_, ids, offsets, 0 /*slop*/,
      false /*has_duplicate_tokens*/, counter));
  ASSERT_NE(nullptr, counter);

  sql::ObDASMatchPhraseOpParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.block_max_param_ = nullptr;
  param.counter_ = counter;
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.boost_ = 1.0;
  OK(param.query_tokens_.push_back(common::ObString::make_string("alpha")));
  OK(param.query_tokens_.push_back(common::ObString::make_string("beta")));
  ASSERT_TRUE(param.is_valid());

  sql::ObDASMatchPhraseOpParam invalid_binding = param;
  invalid_binding.index_table_id_ = common::OB_INVALID_ID;
  ASSERT_FALSE(invalid_binding.is_valid());
  invalid_binding = param;
  invalid_binding.direct_bind_ = true;
  invalid_binding.group_slot_ = -1;
  ASSERT_FALSE(invalid_binding.is_valid());

  sql::ObDASIRScanCtDef sync_ctdef(allocator_);
  sync_ctdef.is_async_index_ = false;
  sql::ObDASMatchPhraseOpParam bad = param;
  bad.ir_ctdef_ = &sync_ctdef;
  bad.block_max_param_ = nullptr;
  ASSERT_FALSE(bad.is_valid());
}

TEST_F(TestDASAsyncMatchPhraseOp, do_init_async_creates_two_helpers)
{
  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  ctdef.is_async_index_ = true;
  ctx.bind_ir_defs(ctdef, rtdef);

  storage::ObPhraseMatchCounter *counter = nullptr;
  common::ObSEArray<int64_t, 4> ids;
  common::ObSEArray<int64_t, 4> offsets;
  OK(ids.push_back(0));
  OK(ids.push_back(1));
  OK(offsets.push_back(0));
  OK(offsets.push_back(1));
  OK(storage::ObPhraseMatchCounter::create(allocator_, ids, offsets, 0 /*slop*/,
      false /*has_duplicate_tokens*/, counter));

  sql::ObDASMatchPhraseOpParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.block_max_param_ = nullptr;
  param.counter_ = counter;
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.group_slot_ = 0;
  param.direct_bind_ = false;
  param.boost_ = 2.0;
  OK(param.token_ids_.push_back(0));
  OK(param.token_ids_.push_back(1));
  OK(param.query_tokens_.push_back(common::ObString::make_string("alpha")));
  OK(param.query_tokens_.push_back(common::ObString::make_string("beta")));

  sql::ObDASMatchPhraseOp *op = nullptr;
  OK(ctx.search_ctx_.create_op(param, op));
  ASSERT_NE(nullptr, op);
  ASSERT_TRUE(op->use_async_index_);
  ASSERT_EQ(2, op->async_helpers_.count());
  ASSERT_EQ(0, op->token_helpers_.count());
  ASSERT_EQ(2, op->abs_pos_lists_.count());
  OK(op->close());
}

TEST_F(TestDASAsyncMatchPhraseOp, abs_pos_scratch_reuses_small_and_releases_large)
{
  constexpr int64_t kModulePageBytes = 7936;
  constexpr int64_t kLargePositionCnt = kModulePageBytes / sizeof(int64_t) + 1;
  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASMatchPhraseOp op(ctx.search_ctx_);
  OK(op.init_abs_pos_lists(2));

  common::ObArray<int64_t> &positions = op.abs_pos_lists_.at(0);
  OK(positions.prepare_allocate(4));
  const int64_t small_data_size = positions.get_data_size();
  ASSERT_LE(small_data_size, kModulePageBytes);
  positions.reuse();
  ASSERT_EQ(0, positions.count());
  ASSERT_EQ(small_data_size, positions.get_data_size());
  OK(positions.prepare_allocate(2));
  ASSERT_EQ(small_data_size, positions.get_data_size());

  OK(positions.prepare_allocate(kLargePositionCnt));
  ASSERT_GT(positions.get_data_size(), kModulePageBytes);
  positions.reuse();
  ASSERT_EQ(0, positions.count());
  ASSERT_EQ(0, positions.get_data_size());
  OK(positions.prepare_allocate(1));
  ASSERT_EQ(1, positions.count());
  ASSERT_GT(positions.get_data_size(), 0);
  ASSERT_LE(positions.get_data_size(), kModulePageBytes);

  op.abs_pos_lists_.reset();
  ASSERT_EQ(0, op.abs_pos_lists_.count());
  op.allocator_.reset();
}

TEST_F(TestDASAsyncMatchPhraseOp, miss_hit_rescan_and_segment_clear_scratch)
{
  const PhraseRowSpec rows[] = {
    {"alpha", 1, 0, true},
    {"alpha", 2, 0, true},
    {"beta", 1, 2, true},
    {"beta", 2, 1, true},
  };
  ObInvSSTable inv_sstable;
  build_phrase_sstable(rows, ARRAYSIZEOF(rows), inv_sstable);
  ObTableReadInfo read_info;
  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASMatchPhraseOp *op = nullptr;
  open_phrase_op(ctdef, rtdef, ctx, op, inv_sstable, read_info,
      2 /*total_doc_cnt*/, 2 /*alpha_doc_freq*/, 2 /*beta_doc_freq*/);

  sql::ObDASRowID rid;
  double score = 0.0;
  OK(op->next_rowid(rid, score));
  ASSERT_EQ(2u, rid.get_uint64());
  ASSERT_EQ(1, op->abs_pos_lists_.at(0).count());
  ASSERT_EQ(0, op->abs_pos_lists_.at(0).at(0));
  ASSERT_EQ(1, op->abs_pos_lists_.at(1).count());
  ASSERT_EQ(1, op->abs_pos_lists_.at(1).at(0));

  const common::ObNewVersionRange &range = ctx.binding_.get_version_range();
  OK(op->rescan());
  OK(op->rescan_segment(range));
  OK(op->abs_pos_lists_.at(0).push_back(99));
  OK(op->abs_pos_lists_.at(1).push_back(99));
  OK(op->next_rowid(rid, score));
  ASSERT_EQ(2u, rid.get_uint64());
  ASSERT_EQ(1, op->abs_pos_lists_.at(0).count());
  ASSERT_EQ(0, op->abs_pos_lists_.at(0).at(0));
  ASSERT_EQ(1, op->abs_pos_lists_.at(1).count());
  ASSERT_EQ(1, op->abs_pos_lists_.at(1).at(0));

  OK(op->rescan_segment(range));
  OK(op->abs_pos_lists_.at(0).push_back(99));
  OK(op->abs_pos_lists_.at(1).push_back(99));
  OK(op->next_rowid(rid, score));
  ASSERT_EQ(2u, rid.get_uint64());
  ASSERT_EQ(1, op->abs_pos_lists_.at(0).count());
  ASSERT_EQ(0, op->abs_pos_lists_.at(0).at(0));
  ASSERT_EQ(1, op->abs_pos_lists_.at(1).count());
  ASSERT_EQ(1, op->abs_pos_lists_.at(1).at(0));

  OK(op->close());
  ASSERT_EQ(0, op->abs_pos_lists_.count());
  ASSERT_EQ(0, op->allocator_.used());
  OK(op->close());
}

TEST_F(TestDASAsyncMatchPhraseOp, partial_decode_error_skips_counter)
{
  const PhraseRowSpec rows[] = {
    {"alpha", 1, 0, true},
    {"beta", 1, 0, false},
  };
  ObInvSSTable inv_sstable;
  build_phrase_sstable(rows, ARRAYSIZEOF(rows), inv_sstable);
  ObTableReadInfo read_info;
  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  CountingPhraseMatchCounter counting_counter;
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASMatchPhraseOp *op = nullptr;
  open_phrase_op(ctdef, rtdef, ctx, op, inv_sstable, read_info,
      1 /*total_doc_cnt*/, 1 /*alpha_doc_freq*/, 1 /*beta_doc_freq*/);

  storage::ObPhraseMatchCounter *original_counter = op->counter_;
  op->counter_ = &counting_counter;
  OK(op->abs_pos_lists_.at(0).push_back(99));
  OK(op->abs_pos_lists_.at(1).push_back(99));

  common::ObString corrupt = common::ObString::make_string("corrupt");
  share::ObFTSPositionListStore decoder;
  common::ObArray<int64_t> positions;
  const int expected_ret = decoder.deserialize_and_decode(
      corrupt.ptr(), corrupt.length(), positions);
  ASSERT_NE(OB_SUCCESS, expected_ret);

  sql::ObDASRowID rid;
  double score = 0.0;
  ASSERT_EQ(expected_ret, op->next_rowid(rid, score));
  ASSERT_EQ(0, counting_counter.call_count_);
  ASSERT_EQ(1, op->abs_pos_lists_.at(0).count());
  ASSERT_EQ(0, op->abs_pos_lists_.at(0).at(0));
  ASSERT_EQ(0, op->abs_pos_lists_.at(1).count());

  op->counter_ = original_counter;
  OK(op->close());
}

TEST_F(TestDASAsyncMatchPhraseOp, phrase_match_next_rowid)
{
  ObInvSSTable inv_sstable;
  build_two_term_phrase_sstable(inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  ctdef.is_async_index_ = true;
  ctx.bind_ir_defs(ctdef, rtdef);

  storage::ObPhraseMatchCounter *counter = nullptr;
  common::ObSEArray<int64_t, 4> ids;
  common::ObSEArray<int64_t, 4> offsets;
  OK(ids.push_back(0));
  OK(ids.push_back(1));
  OK(offsets.push_back(0));
  OK(offsets.push_back(1));
  OK(storage::ObPhraseMatchCounter::create(allocator_, ids, offsets, 0 /*slop*/,
      false /*has_duplicate_tokens*/, counter));

  const common::ObString alpha = common::ObString::make_string("alpha");
  const common::ObString beta = common::ObString::make_string("beta");
  constexpr int64_t kTotalDocCnt = 2;
  prefill_bm25_cache_base(ctx.search_ctx_, kTotalDocCnt);
  prefill_token_doc_cnt(ctx.search_ctx_, alpha, 2 /*doc_freq*/);
  prefill_token_doc_cnt(ctx.search_ctx_, beta, 1 /*doc_freq*/);

  sql::ObDASMatchPhraseOpParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.block_max_param_ = nullptr;
  param.counter_ = counter;
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.group_slot_ = 0;
  param.direct_bind_ = false;
  param.boost_ = 1.0;
  param.use_rich_format_ = false;
  OK(param.token_ids_.push_back(0));
  OK(param.token_ids_.push_back(1));
  OK(param.query_tokens_.push_back(alpha));
  OK(param.query_tokens_.push_back(beta));

  sql::ObDASMatchPhraseOp *op = nullptr;
  OK(ctx.search_ctx_.create_op(param, op));
  ASSERT_NE(nullptr, op);
  inject_async_helpers(op, read_info, inv_sstable);

  OK(op->open());
  const common::ObNewVersionRange &range = ctx.binding_.get_version_range();
  OK(op->rescan_segment(range));

  sql::ObDASRowID rid;
  double score = 0.0;
  OK(op->next_rowid(rid, score));
  ASSERT_EQ(1u, rid.get_uint64());
  ASSERT_GT(score, 0.0);

  int ret = op->next_rowid(rid, score);
  ASSERT_EQ(OB_ITER_END, ret);
  OK(op->close());
}

TEST_F(TestDASAsyncMatchPhraseOp, direct_phrase_rescans_empty_nonempty_empty_nonempty)
{
  ObInvSSTable inv_sstable;
  build_two_term_phrase_sstable(inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  ctdef.is_async_index_ = true;
  ctx.bind_ir_defs(ctdef, rtdef);

  storage::ObPhraseMatchCounter *counter = nullptr;
  common::ObSEArray<int64_t, 4> ids;
  common::ObSEArray<int64_t, 4> offsets;
  OK(ids.push_back(0));
  OK(ids.push_back(1));
  OK(offsets.push_back(0));
  OK(offsets.push_back(1));
  OK(storage::ObPhraseMatchCounter::create(allocator_, ids, offsets, 0 /*slop*/,
      false /*has_duplicate_tokens*/, counter));

  const common::ObString alpha = common::ObString::make_string("alpha");
  const common::ObString beta = common::ObString::make_string("beta");
  sql::ObDASMatchPhraseOpParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.block_max_param_ = nullptr;
  param.counter_ = counter;
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.group_slot_ = 0;
  param.direct_bind_ = true;
  param.boost_ = 1.0;
  param.use_rich_format_ = false;
  OK(param.token_ids_.push_back(0));
  OK(param.token_ids_.push_back(1));
  OK(param.query_tokens_.push_back(alpha));
  OK(param.query_tokens_.push_back(beta));

  sql::ObDASMatchPhraseOp *op = nullptr;
  OK(ctx.search_ctx_.create_op(param, op));
  ASSERT_NE(nullptr, op);
  ASSERT_EQ(2, op->async_helpers_.count());
  inject_async_helpers(op, read_info, inv_sstable);
  storage::ObAsyncIndexSegmentGroup *group =
      ctx.binding_.partition_.storage_group_ctx_.groups_.at(0);
  ASSERT_NE(nullptr, group);
  sql::ObDASRowID rid;
  sql::ObDASRowID target;
  target.set_uint64(1);
  double score = 0.0;
  const sql::MaxScoreTuple *tuple = nullptr;

  for (int64_t phase = 0; phase < 4; ++phase) {
    SCOPED_TRACE(phase);
    // Keep a valid Group/tablet binding while changing its physical Segment count.
    OK(group->init(1));
    if (1 == phase) {
      // The first empty open must work without any cached BM25 statistics.
      prefill_bm25_cache_base(ctx.search_ctx_, 2 /*total_doc_cnt*/);
      prefill_token_doc_cnt(ctx.search_ctx_, alpha, 2 /*doc_freq*/);
      prefill_token_doc_cnt(ctx.search_ctx_, beta, 1 /*doc_freq*/);
    }
    if (1 == phase % 2) {
      OK(group->add_version_range(0, ctx.binding_.get_version_range()));
    }
    OK(0 == phase ? op->open() : op->rescan());
    OK(op->calc_max_score(score));
    if (0 == phase % 2) {
      ASSERT_DOUBLE_EQ(0.0, score);
      ASSERT_EQ(OB_ITER_END, op->next_rowid(rid, score));
      ASSERT_EQ(OB_ITER_END, op->advance_to(target, rid, score));
      ASSERT_EQ(OB_ITER_END, op->advance_shallow(target, true, tuple));
      ASSERT_EQ(nullptr, tuple);
    } else {
      ASSERT_GT(score, 0.0);
      OK(op->next_rowid(rid, score));
      ASSERT_EQ(1u, rid.get_uint64());
      ASSERT_GT(score, 0.0);
      ASSERT_EQ(OB_ITER_END, op->next_rowid(rid, score));
    }
    ASSERT_EQ(2 * ((phase + 1) / 2), mock_access_.posting_scan_count_);
  }
  OK(op->close());
}

// FTS postings with BM25 relevance params carry block-max frontier pairs; a very high
// min_competitive_score_ should prune the current doc in pre_evaluate.
TEST_F(TestDASAsyncMatchPhraseOp, pre_evaluate_high_threshold_prunes_with_block_max)
{
  ObInvSSTable inv_sstable;
  ObTableReadInfo read_info;
  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASMatchPhraseOp *op = nullptr;
  open_two_term_phrase_op(ctdef, rtdef, ctx, op, inv_sstable, read_info);

  OK(op->token_get_next_row(op->lead_idx_));
  op->dynamic_pruning_enabled_ = true;
  op->min_competitive_score_ = 1e12;
  bool is_candidate = false;
  OK(op->pre_evaluate(is_candidate));
  ASSERT_FALSE(is_candidate);

  OK(op->close());
}

// Dynamic pruning enabled but threshold=0 behaves like disabled: still returns the phrase hit.
TEST_F(TestDASAsyncMatchPhraseOp, dynamic_pruning_zero_threshold_finds_phrase_hit)
{
  ObInvSSTable inv_sstable;
  ObTableReadInfo read_info;
  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASMatchPhraseOp *op = nullptr;
  open_two_term_phrase_op(ctdef, rtdef, ctx, op, inv_sstable, read_info);

  op->dynamic_pruning_enabled_ = true;
  OK(op->set_min_competitive_score(0.0));

  sql::ObDASRowID rid;
  double score = 0.0;
  OK(op->next_rowid(rid, score));
  ASSERT_EQ(1u, rid.get_uint64());
  ASSERT_GT(score, 0.0);

  OK(op->close());
}

// When pruning is disabled, min_competitive_score_ must not suppress candidates.
TEST_F(TestDASAsyncMatchPhraseOp, dynamic_pruning_disabled_ignores_threshold)
{
  ObInvSSTable inv_sstable;
  ObTableReadInfo read_info;
  ObArenaAllocator ctx_alloc("PhraseOpCtx");
  PhraseOpCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASMatchPhraseOp *op = nullptr;
  open_two_term_phrase_op(ctdef, rtdef, ctx, op, inv_sstable, read_info);

  op->dynamic_pruning_enabled_ = false;
  OK(op->set_min_competitive_score(1e12));

  sql::ObDASRowID rid;
  double score = 0.0;
  OK(op->next_rowid(rid, score));
  ASSERT_EQ(1u, rid.get_uint64());
  ASSERT_GT(score, 0.0);

  OK(op->close());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_das_async_match_phrase_op.log*");
  OB_LOGGER.set_file_name("test_das_async_match_phrase_op.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
