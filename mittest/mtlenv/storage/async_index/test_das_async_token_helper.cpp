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
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_async_token_helper.h"
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

struct AsyncHelperCtx
{
  explicit AsyncHelperCtx(common::ObIAllocator &alloc)
    : binding_(),
      scan_alloc_("AsyncHelperCtx"),
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

void prefill_bm25_cache_base(
    sql::ObDASSearchCtx &search_ctx,
    const int64_t total_doc_cnt,
    const double avg_doc_token_cnt = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT)
{
  storage::ObInvIdxParamCache &cache = search_ctx.get_bm25_param_cache();
  const common::ObTabletID tablet_id(TestInvIdxSSTablePrepare::TABLET_ID);
  uint64_t existing = 0;
  if (OB_ENTRY_NOT_EXIST == cache.get_total_doc_cnt(tablet_id, existing)) {
    OK(cache.set_total_doc_cnt(tablet_id, static_cast<uint64_t>(total_doc_cnt)));
    OK(cache.set_avg_doc_token_cnt(tablet_id, avg_doc_token_cnt));
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

void prefill_bm25_cache(
    sql::ObDASSearchCtx &search_ctx,
    const common::ObString &query_token,
    const int64_t total_doc_cnt,
    const int64_t doc_freq,
    const double avg_doc_token_cnt = sql::ObExprBM25::DEFAULT_AVG_DOC_TOKEN_CNT)
{
  prefill_bm25_cache_base(search_ctx, total_doc_cnt, avg_doc_token_cnt);
  prefill_token_doc_cnt(search_ctx, query_token, doc_freq);
}

} // namespace

class TestDASAsyncTokenHelper : public TestInvIdxSSTablePrepare
{
public:
  void build_single_term_sstable_with_positions(
      const common::ObString &term_str,
      const uint64_t start_doc_id,
      const common::ObIArray<int64_t> &positions,
      ObInvSSTable &inv_sstable)
  {
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

    common::ObString pos_list;
    OK(encode_pos_list(allocator_, positions, pos_list));

    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, start_doc_id,
        1 /*token_freq*/, 100 /*doc_length*/, pos_list));
    OK(writer.append_row(row));
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(1, res.row_count_);

    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, res.inv_meta_);
    res.inv_meta_.min_doc_id_ = start_doc_id;
    res.inv_meta_.max_doc_id_ = start_doc_id;
    res.inv_meta_.total_doc_length_ = 1;
    ASSERT_TRUE(res.inv_meta_.is_valid());

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
  }

  void build_stride_term_sstable(
      const common::ObString &term_str,
      const uint64_t start,
      const int64_t doc_cnt,
      const uint64_t stride,
      ObInvSSTable &inv_sstable)
  {
    ASSERT_GT(doc_cnt, 0);
    ASSERT_GT(stride, 0u);
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

    common::ObSEArray<int64_t, 4> positions;
    OK(positions.push_back(0));
    common::ObString pos_list;
    OK(encode_pos_list(allocator_, positions, pos_list));

    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    uint64_t min_doc_id = 0;
    uint64_t max_doc_id = 0;
    for (int64_t i = 0; i < doc_cnt; ++i) {
      const uint64_t doc_id = start + static_cast<uint64_t>(i) * stride;
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
          1 /*token_freq*/, 100 /*doc_length*/, pos_list));
      OK(writer.append_row(row));
      if (0 == i) {
        min_doc_id = doc_id;
      }
      max_doc_id = doc_id;
    }
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(doc_cnt, res.row_count_);

    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, res.inv_meta_);
    res.inv_meta_.min_doc_id_ = min_doc_id;
    res.inv_meta_.max_doc_id_ = max_doc_id;
    res.inv_meta_.total_doc_length_ = doc_cnt;
    ASSERT_TRUE(res.inv_meta_.is_valid());

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
  }

  void build_single_term_search_sstable(
      const common::ObString &term_str,
      const uint64_t doc_id,
      ObInvSSTable &inv_sstable)
  {
    prepare_inv_idx_schema(table_schema_, false /*has_nested_key*/);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    apply_search_index_row_desc(inv_idx_row_desc_, false /*has_nested_key*/);
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    prepare_desc();

    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());

    common::ObString empty_pos_list;
    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
        0 /*token_freq ignored*/, 0 /*doc_length ignored*/, empty_pos_list));
    OK(writer.append_row(row));
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(1, res.row_count_);

    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, res.inv_meta_);
    res.inv_meta_.index_type_ = INV_IDX_TYPE_SEARCH;
    res.inv_meta_.min_doc_id_ = doc_id;
    res.inv_meta_.max_doc_id_ = doc_id;
    res.inv_meta_.total_doc_length_ = 0;
    ASSERT_TRUE(res.inv_meta_.is_valid());
    ASSERT_FALSE(res.inv_meta_.has_relevance_params_);

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
    ASSERT_FALSE(inv_sstable.get_inv_meta().has_relevance_params_);
  }

  void create_helper(
      AsyncHelperCtx &ctx,
      sql::ObDASIRScanCtDef &ir_ctdef,
      sql::ObDASIRScanRtDef &ir_rtdef,
      const common::ObString &query_token,
      const ObTableReadInfo &read_info,
      ObInvSSTable &inv_sstable,
      sql::ObDASAsyncTokenHelper *&helper,
      const int64_t cached_total_doc_cnt = 1,
      const int64_t cached_doc_freq = 1)
  {
    sql::ObDASAsyncTokenHelperParam param;
    param.ir_ctdef_ = &ir_ctdef;
    param.ir_rtdef_ = &ir_rtdef;
    param.query_token_ = query_token;
    param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
    param.group_slot_ = 0;
    param.direct_bind_ = false;
    param.token_boost_ = 1.0;

    helper = OB_NEWx(sql::ObDASAsyncTokenHelper, &allocator_, ctx.search_ctx_);
    ASSERT_NE(nullptr, helper);
    OK(helper->init(param));
    mock_access_.inv_sstable_ = &inv_sstable;
    mock_access_.read_info_ = &read_info;
    helper->set_tsc_service(&mock_access_);
    helper->set_tablet_id(common::ObTabletID(TABLET_ID));
    prefill_bm25_cache(ctx.search_ctx_, query_token, cached_total_doc_cnt, cached_doc_freq);
  }

  MockAsyncIndexAccessService mock_access_;
};

TEST_F(TestDASAsyncTokenHelper, init_invalid_param)
{
  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelperParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.query_token_ = common::ObString::make_string("alpha");
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.token_boost_ = 1.0;

  {
    sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
    OK(helper.init(param));
    ASSERT_EQ(OB_INIT_TWICE, helper.init(param));
  }

  sql::ObDASAsyncTokenHelperParam bad = param;
  bad.ir_ctdef_ = nullptr;
  {
    sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper.init(bad));
  }
  bad = param;
  bad.ir_rtdef_ = nullptr;
  {
    sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper.init(bad));
  }
  bad = param;
  bad.index_table_id_ = common::OB_INVALID_ID;
  {
    sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper.init(bad));
  }
  bad = param;
  bad.group_slot_ = -2;
  {
    sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper.init(bad));
  }
  bad = param;
  bad.direct_bind_ = true;
  bad.group_slot_ = -1;
  {
    sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper.init(bad));
  }
  bad = param;
  bad.query_token_.reset();
  {
    sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper.init(bad));
  }
  bad = param;
  bad.token_boost_ = 0.0;
  {
    sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper.init(bad));
  }
}

TEST_F(TestDASAsyncTokenHelper, init_rejects_non_uint64_rowid)
{
  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  ctx.search_ctx_.rowid_type_ = DAS_ROWID_TYPE_COMPACT;

  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  sql::ObDASAsyncTokenHelperParam param;
  param.ir_ctdef_ = &ctdef;
  param.ir_rtdef_ = &rtdef;
  param.query_token_ = common::ObString::make_string("alpha");
  param.index_table_id_ = ASYNC_INDEX_TABLE_ID;
  param.token_boost_ = 1.0;

  sql::ObDASAsyncTokenHelper helper(ctx.search_ctx_);
  ASSERT_EQ(OB_NOT_SUPPORTED, helper.init(param));
}

TEST_F(TestDASAsyncTokenHelper, need_posting_value_true_after_open)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  common::ObSEArray<int64_t, 4> positions;
  OK(positions.push_back(0));
  build_single_term_sstable_with_positions(term, 1 /*start_doc_id*/, positions, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper);
  OK(helper->open());
  ASSERT_TRUE(helper->posting_scan_param_.need_posting_value_);

  const common::ObNewVersionRange &range = ctx.binding_.get_version_range();
  OK(helper->rescan_segment(range));
  ASSERT_TRUE(helper->posting_scan_param_.need_posting_value_);
  OK(helper->close());
}

TEST_F(TestDASAsyncTokenHelper, next_row_and_get_curr_pos_list)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  common::ObSEArray<int64_t, 4> positions;
  OK(positions.push_back(3));
  OK(positions.push_back(7));
  build_single_term_sstable_with_positions(term, 42 /*start_doc_id*/, positions, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper);
  OK(helper->open());

  const common::ObNewVersionRange &range = ctx.binding_.get_version_range();
  OK(helper->rescan_segment(range));
  OK(helper->get_next_row());

  const common::ObDatum *id_datum = nullptr;
  OK(helper->get_curr_id(id_datum));
  ASSERT_EQ(42u, id_datum->get_uint64());

  common::ObString pos_blob;
  OK(helper->get_curr_pos_list(pos_blob));
  ASSERT_GT(pos_blob.length(), 0);

  share::ObFTSPositionListStore decoder;
  common::ObSEArray<int64_t, 4> decoded;
  OK(decoder.deserialize_and_decode(pos_blob.ptr(), pos_blob.length(), decoded));
  ASSERT_EQ(2, decoded.count());
  ASSERT_EQ(3, decoded.at(0));
  ASSERT_EQ(7, decoded.at(1));
  OK(helper->close());
}

TEST_F(TestDASAsyncTokenHelper, advance_to_skips_docs)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  build_stride_term_sstable(term, 10 /*start*/, 5 /*doc_cnt*/, 2 /*stride*/, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper, 5, 5);
  OK(helper->open());

  const common::ObNewVersionRange &range = ctx.binding_.get_version_range();
  OK(helper->rescan_segment(range));
  OK(helper->get_next_row());

  const common::ObDatum *id_datum = nullptr;
  OK(helper->get_curr_id(id_datum));
  ASSERT_EQ(10u, id_datum->get_uint64());

  // Match production/MatchPhraseOp path: rowid -> get_datum_from_rowid -> advance_to.
  sql::ObDASRowID target_rowid;
  target_rowid.set_uint64(18);
  common::ObDatum target_datum;
  OK(ctx.search_ctx_.get_datum_from_rowid(target_rowid, target_datum));
  OK(helper->advance_to(target_datum));
  OK(helper->get_curr_id(id_datum));
  ASSERT_EQ(18u, id_datum->get_uint64());
  OK(helper->close());
}

TEST_F(TestDASAsyncTokenHelper, task_range_bounds_next_seek_and_rescan)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  build_stride_term_sstable(term, 10 /*start*/, 5 /*doc_cnt*/, 2 /*stride*/, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;
  ObObj lo;
  ObObj hi;
  lo.set_uint64(12);
  hi.set_uint64(16);
  ctx.search_ctx_.set_docid_range(lo, hi);

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper, 5, 5);
  OK(helper->open());
  const common::ObNewVersionRange &range = ctx.binding_.get_version_range();
  OK(helper->rescan_segment(range));
  const common::ObDatum *id_datum = nullptr;
  for (uint64_t id = 12; id <= 16; id += 2) {
    OK(helper->get_next_row());
    OK(helper->get_curr_id(id_datum));
    ASSERT_EQ(id, id_datum->get_uint64());
  }
  ASSERT_EQ(OB_ITER_END, helper->get_next_row());

  // Reopening the segment must also refresh the worker's range.
  lo.set_uint64(17);
  hi.set_uint64(18);
  ctx.search_ctx_.set_docid_range(lo, hi);
  OK(helper->rescan_segment(range));
  sql::ObDASRowID target_rowid;
  target_rowid.set_uint64(10);
  common::ObDatum target_datum;
  OK(ctx.search_ctx_.get_datum_from_rowid(target_rowid, target_datum));
  OK(helper->advance_to(target_datum));
  OK(helper->get_curr_id(id_datum));
  ASSERT_EQ(18u, id_datum->get_uint64());
  ASSERT_EQ(OB_ITER_END, helper->get_next_row());

  lo.set_uint64(11);
  hi.set_uint64(11);
  ctx.search_ctx_.set_docid_range(lo, hi);
  OK(helper->rescan_segment(range));
  ASSERT_EQ(OB_ITER_END, helper->get_next_row());
  OK(helper->close());
}

TEST_F(TestDASAsyncTokenHelper, rescan_segment_reopens_iter)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  common::ObSEArray<int64_t, 4> positions;
  OK(positions.push_back(0));
  build_single_term_sstable_with_positions(term, 99 /*start_doc_id*/, positions, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper);
  OK(helper->open());

  const common::ObNewVersionRange &range = ctx.binding_.get_version_range();
  OK(helper->rescan_segment(range));
  OK(helper->get_next_row());

  OK(helper->rescan_segment(range));
  OK(helper->get_next_row());

  const common::ObDatum *id_datum = nullptr;
  OK(helper->get_curr_id(id_datum));
  ASSERT_EQ(99u, id_datum->get_uint64());
  OK(helper->close());
}

TEST_F(TestDASAsyncTokenHelper, get_curr_bm25_param_after_next_row)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  common::ObSEArray<int64_t, 4> positions;
  OK(positions.push_back(0));
  build_single_term_sstable_with_positions(term, 7 /*start_doc_id*/, positions, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper);
  OK(helper->open());

  common::ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = SNAPSHOT_VERSION;
  OK(helper->rescan_segment(range));
  OK(helper->get_next_row());

  int64_t token_freq = 0;
  int64_t doc_length = 0;
  OK(helper->get_curr_bm25_param(token_freq, doc_length));
  ASSERT_EQ(1, token_freq);
  ASSERT_EQ(100, doc_length);
  OK(helper->close());
}

TEST_F(TestDASAsyncTokenHelper, get_curr_phrase_block_max_with_relevance_params)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  common::ObSEArray<int64_t, 4> positions;
  OK(positions.push_back(0));
  build_single_term_sstable_with_positions(term, 1 /*start_doc_id*/, positions, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper);
  OK(helper->open());
  common::ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = SNAPSHOT_VERSION;
  OK(helper->rescan_segment(range));
  OK(helper->get_next_row());

  const storage::ObMaxScoreTuple *tuple = nullptr;
  OK(helper->get_curr_phrase_block_max_info(tuple));
  ASSERT_NE(nullptr, tuple);
  ASSERT_GT(tuple->max_score_, 0.0);
  OK(helper->close());
}

// Search-index postings without relevance params have no block-max frontier.
TEST_F(TestDASAsyncTokenHelper, get_curr_phrase_block_max_without_metadata_not_supported)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  build_single_term_search_sstable(term, 1 /*doc_id*/, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper);
  OK(helper->open());
  common::ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = SNAPSHOT_VERSION;
  OK(helper->rescan_segment(range));
  OK(helper->get_next_row());

  const storage::ObMaxScoreTuple *tuple = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, helper->get_curr_phrase_block_max_info(tuple));
  OK(helper->close());
}

TEST_F(TestDASAsyncTokenHelper, next_row_after_advance_shallow_is_rejected)
{
  ObInvSSTable inv_sstable;
  const common::ObString term = common::ObString::make_string("alpha");
  build_stride_term_sstable(term, 10 /*start*/, 5 /*doc_cnt*/, 2 /*stride*/, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObArenaAllocator ctx_alloc("AsyncHelperCtx");
  AsyncHelperCtx ctx(ctx_alloc);
  sql::ObDASIRScanCtDef ctdef(ctx_alloc);
  sql::ObDASIRScanRtDef rtdef;

  sql::ObDASAsyncTokenHelper *helper = nullptr;
  create_helper(ctx, ctdef, rtdef, term, read_info, inv_sstable, helper, 5, 5);
  OK(helper->open());

  common::ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = SNAPSHOT_VERSION;
  OK(helper->rescan_segment(range));
  OK(helper->get_next_row());

  sql::ObDASRowID target_rowid;
  target_rowid.set_uint64(14);
  common::ObDatum target_datum;
  OK(ctx.search_ctx_.get_datum_from_rowid(target_rowid, target_datum));
  OK(helper->advance_shallow(target_datum, true /*inclusive*/));
  ASSERT_EQ(OB_ERR_UNEXPECTED, helper->get_next_row());
  OK(helper->close());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_das_async_token_helper.log*");
  OB_LOGGER.set_file_name("test_das_async_token_helper.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
