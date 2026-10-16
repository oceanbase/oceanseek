/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <vector>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "test_inv_idx_sstable_prepare.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "storage/blocksstable/async_index/ob_posting_block_decoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "sql/engine/expr/ob_expr_bm25.h"

namespace oceanbase
{
namespace unittest
{

namespace
{
// Format a feeder term string: "term" + 10-digit zero-padded seq (matches ObInvIdxRowFeeder).
void format_term(const int64_t seq, char *buf, const int64_t buf_len)
{
  snprintf(buf, buf_len, "term%010ld", static_cast<long>(seq));
}

// Build the rowkey read info from the FTS schema built by the prepare harness.
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

// Build a 1-datum rowkey holding the term string (the inverted-index query key).
// build_term_rowkey() lives in test_inv_idx_sstable_prepare.h.

// Build a valid ObInvPostingScanParam pointing at the supplied (caller-owned) members.
// The caller MUST keep query_key (and its term buffer) alive for the whole scan.
// read_info is no longer part of the param; it is passed directly to scan_posting().
void build_scan_param(
    const ObDatumRowkey &query_key,
    const ObTabletID &tablet_id,
    const ObQueryFlag &query_flag,
    common::ObIAllocator &allocator,
    ObInvPostingScanParam &param)
{
  param.reset();
  param.search_key_ = &query_key;
  param.tablet_id_ = tablet_id;
  param.query_flag_ = query_flag;
  param.allocator_ = &allocator;
  param.doc_freq_ = 1000;
  param.total_doc_cnt_ = 1000000;
  param.avg_doc_length_ = 100.0;
}

// Build a scan param for indexes without BM25 relevance params (search index / doc_id-only FTS).
// has_relevance_params_ = false, so avg_doc_length_ is not validated.
void build_scan_param_no_bm25(
    const ObDatumRowkey &query_key,
    const ObTabletID &tablet_id,
    const ObQueryFlag &query_flag,
    common::ObIAllocator &allocator,
    ObInvPostingScanParam &param)
{
  build_scan_param(query_key, tablet_id, query_flag, allocator, param);
  param.has_relevance_params_ = false;
}

// Make a uint64 doc_id datum (backing storage held by the caller via `storage`).
void make_doc_id_datum(uint64_t &storage, const uint64_t doc_id, ObDatum &datum)
{
  storage = doc_id;
  datum.ptr_ = reinterpret_cast<const char *>(&storage);
  datum.set_uint(doc_id);
}

// =============================================================================
// Single-term dataset definition for the large-posting-list tests.
//
// One term carries ST_DOC_CNT doc_ids on a GAPPED arithmetic sequence so the
// inclusive lower-bound (first doc_id >= target) of advance_to()/advance_shallow()
// is exercised at interior positions, not only on exact hits:
//   doc_id(i) = ST_START + i * ST_STRIDE,  i in [0, ST_DOC_CNT)
//   present doc_ids are odd (1,3,5,...); even doc_ids are absent.
// ST_DOC_CNT = 1,000,000 spans ~3900 posting blocks (capacity 256) across many
// micro / macro / leaf-index boundaries.
// =============================================================================
constexpr uint64_t ST_START = 1;
constexpr uint64_t ST_STRIDE = 2;
constexpr int64_t ST_DOC_CNT = 1000000;
constexpr int64_t ST_TERM_SEQ = 1;

// doc_id materialized at posting position i.
inline uint64_t expect_doc(const int64_t i) { return ST_START + static_cast<uint64_t>(i) * ST_STRIDE; }
// Heterogeneous relevance params per position (used to prove per-column batch copy is
// exact and not off-by-one / column-swapped). Coprime periods so tf and dl vary independently.
inline uint64_t hetero_tf(const int64_t i) { return 1 + static_cast<uint64_t>(i % 7); }
inline uint64_t hetero_dl(const int64_t i) { return 50 + static_cast<uint64_t>(i % 13); }
// max doc_id present in the term.
inline uint64_t st_max_doc() { return expect_doc(ST_DOC_CNT - 1); }
constexpr int64_t DRAIN_DOC_CNT = 2048;  // spans 8 posting blocks (capacity 256)
inline uint64_t drain_max_doc() { return expect_doc(DRAIN_DOC_CNT - 1); }
// Smallest position i with expect_doc(i) >= target. May be == ST_DOC_CNT meaning
// "target beyond the term" (advance_to/advance_shallow then reach iter-end).
inline int64_t idx_ge(const uint64_t target)
{
  return (target <= ST_START)
      ? 0
      : static_cast<int64_t>((target - ST_START + ST_STRIDE - 1) / ST_STRIDE);
}
} // namespace

class TestInvSSTablePostingIter : public TestInvIdxSSTablePrepare
{
public:
  // Build an ObInvSSTable holding exactly one term ("term%010ld" % term_seq) whose
  // posting list is doc_id(i) = start_doc_id + i*stride for i in [0, doc_cnt).
  // By default token_freq is 1 and doc_length is 100 for every doc (matches the
  // relevance-param assertions used by the iterator tests). Optional score patterns support
  // heterogeneous row checks and monotonically increasing block-max range tests. Mirrors
  // TestInvIdxSSTablePrepare::build_inv_sstable but uses one controlled posting run.
  void build_single_term_sstable(
      const bool has_pos_list,
      const int64_t doc_cnt,
      const uint64_t start_doc_id,
      const uint64_t stride,
      const int64_t term_seq,
      ObInvSSTable &inv_sstable,
      const bool hetero = false,
      const bool ascending_block_score = false)
  {
    ASSERT_GT(doc_cnt, 0);
    ASSERT_GT(stride, 0u);
    ASSERT_GT(start_doc_id, 0u);  // min_doc_id must be > 0 for valid inv meta

    // 1. schema + row desc + data store desc + index builder
    prepare_inv_idx_schema(table_schema_, has_pos_list);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    ASSERT_EQ(has_pos_list, inv_idx_row_desc_.pos_list_col_idx_ >= 0);
    prepare_desc();

    // 2. open macro block writer
    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());

    // 3. feed the single term in strictly increasing doc_id order
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
      const uint64_t doc_id = start_doc_id + static_cast<uint64_t>(i) * stride;
      // range-max 专用数据按 posting block 递增 tf，使后继 block 的上界严格更高。
      const uint64_t token_freq = ascending_block_score
          ? 1 + static_cast<uint64_t>(i / DEFAULT_POSTING_BLOCK_CAPACITY)
          : (hetero ? hetero_tf(i) : 1);
      const uint64_t doc_length = hetero ? hetero_dl(i) : 100;
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
          token_freq, doc_length, pos_list));
      OK(writer.append_row(row));
      if (0 == i) {
        min_doc_id = doc_id;
      }
      max_doc_id = doc_id;
      total_token_freq += token_freq;
    }
    STORAGE_LOG(INFO, "fed single term inverted index rows", K(doc_cnt), K(has_pos_list),
        K(term_str), K(min_doc_id), K(max_doc_id));

    // 4. close writer -> flushes the index tree into the shared index builder
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    // 5. close index builder -> sstable merge result
    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(doc_cnt, res.row_count_);

    // 6. fill inv meta (schema-driven + data-driven) and build the ObInvSSTable
    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, res.inv_meta_);
    res.inv_meta_.min_doc_id_ = min_doc_id;
    res.inv_meta_.max_doc_id_ = max_doc_id;
    res.inv_meta_.total_doc_length_ = total_token_freq;
    ASSERT_TRUE(res.inv_meta_.is_valid());

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
    ASSERT_EQ(min_doc_id, inv_sstable.get_inv_meta().min_doc_id_);
    ASSERT_EQ(max_doc_id, inv_sstable.get_inv_meta().max_doc_id_);
    STORAGE_LOG(INFO, "built single term inv sstable", K(inv_sstable));
  }

  // Open a posting iterator on `inv_sstable` for the single-term dataset's term.
  // The caller-owned term_buf/term_datum/query_key/read_info/param MUST outlive the
  // returned iterator (open() shallow-copies the query key into the scan range).
  void open_single_term_iter(
      ObInvSSTable &inv_sstable,
      ObTableReadInfo &read_info,
      char *term_buf,
      const int64_t term_buf_len,
      ObStorageDatum &term_datum,
      ObDatumRowkey &query_key,
      ObQueryFlag &query_flag,
      ObTabletID &tablet_id,
      ObInvPostingScanParam &param,
      ObInvSSTablePostingIter *&iter,
      const bool need_posting_value = false,
      const uint64_t min_doc_id = 0,
      const uint64_t max_doc_id = UINT64_MAX)
  {
    format_term(ST_TERM_SEQ, term_buf, term_buf_len);
    build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
    query_flag.set_use_block_cache();
    build_scan_param(query_key, tablet_id, query_flag, allocator_, param);
    param.need_posting_value_ = need_posting_value;
    param.min_doc_id_ = min_doc_id;
    param.max_doc_id_ = max_doc_id;
    iter = nullptr;
    OK(inv_sstable.scan_posting(param, read_info, iter));
    ASSERT_NE(nullptr, iter);
  }

  void free_iter(ObInvSSTablePostingIter *&iter)
  {
    if (nullptr != iter) {
      iter->~ObInvSSTablePostingIter();
      allocator_.free(iter);
      iter = nullptr;
    }
  }

  // Repeatedly drain_until(boundary, max_count) from the iterator's current position,
  // reading the projected zero-copy pointers and concatenating all rows until a call
  // returns count == 0. Then hit_end reflects the terminal condition:
  //   hit_end == true  -> posting exhausted (iter->is_end())
  //   hit_end == false -> boundary reached; the cursor holds the first row > boundary,
  //                       read out into next_* via get_cur_id()/load_curr_bm25_param().
  void drain_collect(
      ObInvSSTablePostingIter *iter,
      const uint64_t boundary,
      const int64_t max_count,
      std::vector<uint64_t> &ids,
      std::vector<uint64_t> &tfs,
      std::vector<uint64_t> &dls,
      bool &hit_end,
      uint64_t &next_id_after,
      uint64_t &next_tf_after,
      uint64_t &next_dl_after)
  {
    ids.clear();
    tfs.clear();
    dls.clear();
    hit_end = false;
    next_id_after = 0;
    next_tf_after = 0;
    next_dl_after = 0;
    bool done = false;
    while (!done) {
      const uint64_t *pid = nullptr;
      const uint64_t *ptf = nullptr;
      const uint64_t *pdl = nullptr;
      int64_t n = 0;
      ASSERT_EQ(OB_SUCCESS, iter->drain_until(boundary, max_count, pid, ptf, pdl, n));
      ASSERT_GE(n, 0);
      ASSERT_LE(n, max_count);
      if (n > 0) {
        for (int64_t k = 0; k < n; ++k) {
          ids.push_back(pid[k]);
          tfs.push_back(ptf[k]);
          dls.push_back(pdl[k]);
        }
      } else {
        hit_end = iter->is_end();
        if (!hit_end) {
          const ObDatum *bid = nullptr;
          ASSERT_EQ(OB_SUCCESS, iter->get_cur_id(bid));
          int64_t tf = 0;
          int64_t dl = 0;
          ASSERT_EQ(OB_SUCCESS, iter->load_curr_bm25_param(tf, dl));
          next_id_after = bid->get_uint64();
          next_tf_after = static_cast<uint64_t>(tf);
          next_dl_after = static_cast<uint64_t>(dl);
        }
        done = true;
      }
    }
  }

  // Build a single-term ObInvSSTable whose posting blocks are doc_id-only (no token_freq /
  // doc_length / pos_list / nested_key). The row_desc is manually constructed to skip FTS
  // value columns so the posting block encoder produces doc_id-only blocks, and the inv_meta
  // carries has_relevance_params_ = false, index_type_ = SEARCH.
  void build_single_term_sstable_no_bm25(
      const int64_t doc_cnt,
      const uint64_t start_doc_id,
      const uint64_t stride,
      const int64_t term_seq,
      ObInvSSTable &inv_sstable,
      const bool has_nested_key = false)
  {
    ASSERT_GT(doc_cnt, 0);
    ASSERT_GT(stride, 0u);
    ASSERT_GT(start_doc_id, 0u);

    prepare_inv_idx_schema(table_schema_, has_nested_key /*reuse value column as nested_key*/);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    apply_search_index_row_desc(inv_idx_row_desc_, has_nested_key);
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    ASSERT_EQ(has_nested_key, inv_idx_row_desc_.has_posting_value());
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
    ObString empty_pos_list;
    char nested_key_buf[32] = {0};
    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    uint64_t min_doc_id = 0;
    uint64_t max_doc_id = 0;
    for (int64_t i = 0; i < doc_cnt; ++i) {
      const uint64_t doc_id = start_doc_id + static_cast<uint64_t>(i) * stride;
      snprintf(nested_key_buf, sizeof(nested_key_buf), "nk%010ld", static_cast<long>(i));
      ObString nested_key(static_cast<int32_t>(strlen(nested_key_buf)), nested_key_buf);
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
          0 /*token_freq ignored*/, 0 /*doc_length ignored*/, empty_pos_list, nested_key));
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
    res.inv_meta_.index_type_ = INV_IDX_TYPE_SEARCH;
    res.inv_meta_.has_nested_key_ = has_nested_key;
    res.inv_meta_.min_doc_id_ = min_doc_id;
    res.inv_meta_.max_doc_id_ = max_doc_id;
    res.inv_meta_.total_doc_length_ = 0;
    ASSERT_TRUE(res.inv_meta_.is_valid());
    ASSERT_FALSE(res.inv_meta_.has_relevance_params_);
    ASSERT_EQ(has_nested_key, res.inv_meta_.has_nested_key_);
    ASSERT_FALSE(res.inv_meta_.is_fts_index());

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
    ASSERT_FALSE(inv_sstable.get_inv_meta().has_relevance_params_);
    ASSERT_EQ(has_nested_key, inv_sstable.get_inv_meta().has_nested_key_);
    ASSERT_FALSE(inv_sstable.get_inv_meta().is_fts_index());
    ASSERT_EQ(min_doc_id, inv_sstable.get_inv_meta().min_doc_id_);
    ASSERT_EQ(max_doc_id, inv_sstable.get_inv_meta().max_doc_id_);
    STORAGE_LOG(INFO, "built no-bm25 single term inv sstable", K(inv_sstable));
  }
};

// open() on an absent term leaves the iterator valid with is_end() == true and next()
// returns OB_ITER_END.
TEST_F(TestInvSSTablePostingIter, scan_posting_open_absent_term)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  // A term far beyond any generated term (terms are "term%010d").
  char term_buf[32] = {0};
  format_term(999999999, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_TRUE(iter->is_end());
  ASSERT_EQ(OB_ITER_END, iter->next());
  ASSERT_EQ(OB_ITER_END, iter->next());

  iter->~ObInvSSTablePostingIter();
  allocator_.free(iter);
}

// Iterate all docs of one term via next() and verify contiguous, increasing doc_ids whose
// count matches the underlying meta. token_freq/doc_length match the fed values.
TEST_F(TestInvSSTablePostingIter, scan_posting_next_all_docs)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);

  uint64_t prev_doc_id = 0;
  int64_t doc_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter->next())) {
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_NE(nullptr, id_datum);
    const uint64_t doc_id = id_datum->get_uint64();
    if (0 == doc_cnt) {
      ASSERT_GT(doc_id, 0);
    } else {
      ASSERT_EQ(prev_doc_id + 1, doc_id) << "term doc_ids must be contiguous and increasing";
    }
    prev_doc_id = doc_id;
    // Relevance params for the prepared data: token_freq == 1, doc_length == 100.
    int64_t token_freq = 0;
    int64_t doc_length = 0;
    OK(iter->get_curr_bm25_param(token_freq, doc_length));
    ASSERT_EQ(1, token_freq);
    ASSERT_EQ(100, doc_length);
    ++doc_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GT(doc_cnt, 0);
  ASSERT_TRUE(iter->is_end());
  STORAGE_LOG(INFO, "iterated single term posting docs", K(doc_cnt), K(prev_doc_id));

  iter->~ObInvSSTablePostingIter();
  allocator_.free(iter);
}

// advance_to() to a target inside the very first posting block lands on the first doc_id >= target.
TEST_F(TestInvSSTablePostingIter, advance_to_inside_current_block)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);

  // Position on the first doc, then advance a couple of ids forward inside the same block.
  OK(iter->next());
  const ObDatum *id_datum = nullptr;
  OK(iter->get_cur_id(id_datum));
  const uint64_t first_doc_id = id_datum->get_uint64();

  const uint64_t target = first_doc_id + 3;
  uint64_t target_storage = 0;
  ObDatum target_datum;
  make_doc_id_datum(target_storage, target, target_datum);
  OK(iter->advance_to(target_datum));
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(target, id_datum->get_uint64()) << "advance_to lands on first doc >= target";

  iter->~ObInvSSTablePostingIter();
  allocator_.free(iter);
}

// advance_to() across many posting blocks lands on the correct row, and continues via next().
TEST_F(TestInvSSTablePostingIter, advance_to_cross_block)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param(query_key, tablet_id, query_flag, allocator_, param);

  // Find the term's first and last doc_id by a full scan first.
  ObInvSSTablePostingIter *probe = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, probe));
  uint64_t first_doc_id = 0;
  uint64_t last_doc_id = 0;
  int64_t doc_cnt = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = probe->next())) {
    const ObDatum *id_datum = nullptr;
    OK(probe->get_cur_id(id_datum));
    if (0 == doc_cnt) {
      first_doc_id = id_datum->get_uint64();
    }
    last_doc_id = id_datum->get_uint64();
    ++doc_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  probe->~ObInvSSTablePostingIter();
  allocator_.free(probe);
  // Require enough docs to span multiple posting blocks (default block size 256).
  ASSERT_GT(doc_cnt, 300);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));

  // Target a doc deep inside the run, well beyond the first block.
  const uint64_t target = first_doc_id + (last_doc_id - first_doc_id) / 2;
  uint64_t target_storage = 0;
  ObDatum target_datum;
  make_doc_id_datum(target_storage, target, target_datum);
  OK(iter->advance_to(target_datum));
  const ObDatum *id_datum = nullptr;
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(target, id_datum->get_uint64());
  // next() continues from there.
  OK(iter->next());
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(target + 1, id_datum->get_uint64());

  iter->~ObInvSSTablePostingIter();
  allocator_.free(iter);
}

// advance_to() beyond the term's max doc_id ends the iterator; repeated next() stays OB_ITER_END.
TEST_F(TestInvSSTablePostingIter, advance_to_beyond_end)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));

  // term1 is the first term; any later term starts at a larger doc_id, so UINT64_MAX-1 is
  // certainly beyond this term's run.
  const uint64_t target = UINT64_MAX - 1;
  uint64_t target_storage = 0;
  ObDatum target_datum;
  make_doc_id_datum(target_storage, target, target_datum);
  ASSERT_EQ(OB_ITER_END, iter->advance_to(target_datum));
  ASSERT_TRUE(iter->is_end());
  ASSERT_EQ(OB_ITER_END, iter->next());

  iter->~ObInvSSTablePostingIter();
  allocator_.free(iter);
}

// advance_shallow() positions at block granularity without materializing rows; block max
// metadata is absent in the current build so get_block_max_tuple returns OB_NOT_SUPPORTED.
// next() is rejected in shallow status; advance_to() materializes and clears shallow status.
TEST_F(TestInvSSTablePostingIter, advance_shallow_then_advance_to_materializes_target)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(1, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param(query_key, tablet_id, query_flag, allocator_, param);

  // Find the first doc_id.
  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  OK(iter->next());
  const ObDatum *id_datum = nullptr;
  OK(iter->get_cur_id(id_datum));
  const uint64_t first_doc_id = id_datum->get_uint64();

  const uint64_t target = first_doc_id + 5;
  uint64_t target_storage = 0;
  ObDatum target_datum;
  make_doc_id_datum(target_storage, target, target_datum);
  OK(iter->advance_shallow(target_datum, true /*inclusive*/));
  ASSERT_TRUE(iter->in_shallow_status());
  // No row is materialized while shallow: get_cur_id and get_curr_bm25_param are rejected.
  const ObDatum *shallow_id = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_cur_id(shallow_id));
  int64_t tf = 0;
  int64_t dl = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_curr_bm25_param(tf, dl));
  uint64_t min_id = 0, max_id = 0;
  double max_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, iter->get_block_max_tuple(min_id, max_id, max_score));
  // A covered shallow (target within the block next() already decoded) issues no new
  // posting-data IO and retains the decoded block for a subsequent advance_to().
  ASSERT_TRUE(iter->curr_block_loaded_);

  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->next());
  ASSERT_TRUE(iter->in_shallow_status());
  OK(iter->advance_to(target_datum));
  ASSERT_FALSE(iter->in_shallow_status());
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(target, id_datum->get_uint64());

  iter->~ObInvSSTablePostingIter();
  allocator_.free(iter);
}

// get_cur_pos_list() returns OB_NOT_SUPPORTED when need_posting_value_ is false.
TEST_F(TestInvSSTablePostingIter, get_cur_pos_list_disabled)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, 1000, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter, false /*need_posting_value*/);
  OK(iter->next());
  ASSERT_FALSE(iter->curr_value_block_loaded_);
  const ObDatum *value_datum = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, iter->get_cur_pos_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
  ASSERT_FALSE(iter->curr_value_block_loaded_);
  free_iter(iter);
}

// get_cur_pos_list() returns the position_list blob aligned with curr_row_idx_.
TEST_F(TestInvSSTablePostingIter, get_cur_pos_list_returns_position)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, 1000, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter, true /*need_posting_value*/);
  OK(iter->next());
  const ObDatum *id_datum = nullptr;
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(ST_START, id_datum->get_uint64());

  const ObDatum *value_datum = nullptr;
  ASSERT_FALSE(iter->curr_value_block_loaded_);
  OK(iter->get_cur_pos_list(value_datum));
  ASSERT_NE(nullptr, value_datum);
  ASSERT_EQ(TEST_POS_LIST_LEN, value_datum->len_);
  for (int64_t i = 0; i < value_datum->len_; ++i) {
    ASSERT_EQ('p', value_datum->ptr_[i]);
  }
  ASSERT_TRUE(iter->curr_value_block_loaded_);
  OK(iter->get_cur_pos_list(value_datum));
  free_iter(iter);
}

// get_cur_pos_list() in shallow status returns OB_ERR_UNEXPECTED.
TEST_F(TestInvSSTablePostingIter, get_cur_pos_list_in_shallow_status)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, 1000, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter, true /*need_posting_value*/);

  uint64_t target_doc_id = ST_START + ST_STRIDE;
  uint64_t target_storage = 0;
  ObDatum target_datum;
  make_doc_id_datum(target_storage, target_doc_id, target_datum);
  OK(iter->advance_shallow(target_datum, true /*inclusive*/));
  ASSERT_TRUE(iter->in_shallow_status());

  const ObDatum *value_datum = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_cur_pos_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
  free_iter(iter);
}

// get_cur_pos_list() on a segment without posting value column returns OB_ERR_UNEXPECTED.
TEST_F(TestInvSSTablePostingIter, get_cur_pos_list_no_value_column)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(false /*has_pos_list*/, 1000, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter, true /*need_posting_value*/);
  OK(iter->next());
  const ObDatum *value_datum = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_cur_pos_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
  free_iter(iter);
}

// get_cur_nested_key_list() returns OB_NOT_SUPPORTED when need_posting_value_ is false.
TEST_F(TestInvSSTablePostingIter, get_cur_nested_key_list_disabled)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(
      512, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable, true /*has_nested_key*/);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = false;

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);
  OK(iter->next());
  ASSERT_FALSE(iter->curr_value_block_loaded_);
  const ObDatum *nested_datum = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, iter->get_cur_nested_key_list(nested_datum));
  ASSERT_EQ(nullptr, nested_datum);
  ASSERT_FALSE(iter->curr_value_block_loaded_);
  free_iter(iter);
}

// get_cur_nested_key_list() returns the nested_key blob aligned with curr_row_idx_.
TEST_F(TestInvSSTablePostingIter, get_cur_nested_key_list_returns_blob)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(
      512, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable, true /*has_nested_key*/);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_FALSE(iter->has_relevance_params_);
  ASSERT_TRUE(inv_sstable.get_inv_meta().has_nested_key_);

  OK(iter->next());
  const ObDatum *id_datum = nullptr;
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(ST_START, id_datum->get_uint64());

  const ObDatum *nested_datum = nullptr;
  ASSERT_FALSE(iter->curr_value_block_loaded_);
  OK(iter->get_cur_nested_key_list(nested_datum));
  ASSERT_NE(nullptr, nested_datum);
  ASSERT_EQ(12, nested_datum->len_);
  ASSERT_EQ(0, memcmp("nk0000000000", nested_datum->ptr_, nested_datum->len_));
  ASSERT_TRUE(iter->curr_value_block_loaded_);

  const ObDatum *value_datum = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, iter->get_cur_pos_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);

  OK(iter->next());
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(ST_START + ST_STRIDE, id_datum->get_uint64());
  OK(iter->get_cur_nested_key_list(nested_datum));
  ASSERT_NE(nullptr, nested_datum);
  ASSERT_EQ(12, nested_datum->len_);
  ASSERT_EQ(0, memcmp("nk0000000001", nested_datum->ptr_, nested_datum->len_));
  free_iter(iter);
}

// get_cur_nested_key_list() in shallow status returns OB_ERR_UNEXPECTED.
TEST_F(TestInvSSTablePostingIter, get_cur_nested_key_list_in_shallow_status)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(
      512, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable, true /*has_nested_key*/);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);

  uint64_t target_storage = 0;
  ObDatum target_datum;
  make_doc_id_datum(target_storage, ST_START + ST_STRIDE, target_datum);
  OK(iter->advance_shallow(target_datum, true /*inclusive*/));
  ASSERT_TRUE(iter->in_shallow_status());

  const ObDatum *nested_datum = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_cur_nested_key_list(nested_datum));
  ASSERT_EQ(nullptr, nested_datum);
  free_iter(iter);
}

TEST_F(TestInvSSTablePostingIter, get_cur_nested_key_list_no_nested_column)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(
      512, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable, false /*has_nested_key*/);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);
  OK(iter->next());
  const ObDatum *nested_datum = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_cur_nested_key_list(nested_datum));
  ASSERT_EQ(nullptr, nested_datum);
  free_iter(iter);
}

// Regression guard for T6. The phrase position API and the search nested API share the
// same value-block elevator, but each must reject the other payload kind.
TEST_F(TestInvSSTablePostingIter, phrase_position_and_search_nested_apis_are_independent)
{
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;

  {
    ObInvSSTable phrase_sstable;
    build_single_term_sstable(
        true /*has_pos_list*/, 16, ST_START, ST_STRIDE, ST_TERM_SEQ, phrase_sstable);
    ObTableReadInfo read_info;
    build_rowkey_read_info(table_schema_, allocator_, read_info);
    open_single_term_iter(phrase_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter, true /*need_posting_value*/);
    OK(iter->next());

    const ObDatum *pos_datum = nullptr;
    OK(iter->get_cur_pos_list(pos_datum));
    ASSERT_NE(nullptr, pos_datum);
    ASSERT_EQ(8, pos_datum->len_);

    const ObDatum *nested_datum = nullptr;
    ASSERT_EQ(OB_NOT_SUPPORTED, iter->get_cur_nested_key_list(nested_datum));
    ASSERT_EQ(nullptr, nested_datum);
    free_iter(iter);
  }

  {
    ObInvSSTable search_sstable;
    build_single_term_sstable_no_bm25(
        16, ST_START, ST_STRIDE, ST_TERM_SEQ, search_sstable, true /*has_nested_key*/);
    ObTableReadInfo read_info;
    build_rowkey_read_info(table_schema_, allocator_, read_info);
    format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
    build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
    query_flag.reset();
    query_flag.set_use_block_cache();
    build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);
    param.need_posting_value_ = true;
    iter = nullptr;
    OK(search_sstable.scan_posting(param, read_info, iter));
    ASSERT_NE(nullptr, iter);
    OK(iter->next());

    const ObDatum *nested_datum = nullptr;
    OK(iter->get_cur_nested_key_list(nested_datum));
    ASSERT_NE(nullptr, nested_datum);
    ASSERT_EQ(12, nested_datum->len_);
    ASSERT_EQ(0, memcmp("nk0000000000", nested_datum->ptr_, nested_datum->len_));

    const ObDatum *pos_datum = nullptr;
    ASSERT_EQ(OB_NOT_SUPPORTED, iter->get_cur_pos_list(pos_datum));
    ASSERT_EQ(nullptr, pos_datum);
    free_iter(iter);
  }
}

// =============================================================================
// Large single-term posting list tests: one term carrying 1,000,000 doc_ids on a
// gapped (odd-only) sequence spanning ~3900 posting blocks across many micro/macro/
// leaf-index boundaries. These verify next()/advance_to()/advance_shallow() and their
// combinations under boundary and edge-case timings on a genuinely large posting list.
// =============================================================================

// next() over the entire 1M-doc term: contiguous-by-stride doc_ids, exact count, first/last
// match the meta, and relevance params hold for every materialized row.
TEST_F(TestInvSSTablePostingIter, single_term_full_next_scan)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  int64_t i = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter->next())) {
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_NE(nullptr, id_datum);
    const uint64_t doc_id = id_datum->get_uint64();
    // Manual compare in the hot loop; only assert (and stop) on the first divergence
    // to avoid a million macro expansions.
    if (doc_id != expect_doc(i)) {
      ASSERT_EQ(expect_doc(i), doc_id) << "doc_id mismatch at position " << i;
      break;
    }
    int64_t tf = 0;
    int64_t dl = 0;
    OK(iter->get_curr_bm25_param(tf, dl));
    if (1 != tf || 100 != dl) {
      ASSERT_EQ(1, tf) << "token_freq mismatch at position " << i;
      ASSERT_EQ(100, dl) << "doc_length mismatch at position " << i;
      break;
    }
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(ST_DOC_CNT, i) << "must iterate exactly the whole posting list";
  ASSERT_TRUE(iter->is_end());
  ASSERT_EQ(OB_ITER_END, iter->next());
  ASSERT_EQ(OB_ITER_END, iter->next());
  ASSERT_EQ(st_max_doc(), inv_sstable.get_inv_meta().max_doc_id_);
  STORAGE_LOG(INFO, "single term full next scan done", K(i));

  free_iter(iter);
}

// advance_to() across the large term: present targets at posting-block boundaries (in
// monotonic order), absent (even) targets rounding up across block edges, equal-target
// no-op, and a large cross-macro jump followed by a tail scan.
TEST_F(TestInvSSTablePostingIter, single_term_advance_to_scenarios)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;

  const int64_t CAP = DEFAULT_POSTING_BLOCK_CAPACITY;  // 256

  // Case 1: present targets at and around posting-block boundaries, advanced monotonically.
  // Positions chosen to hit block starts/ends and crossings; each is an exact present doc_id.
  {
    const int64_t positions[] = {
        0, 1, CAP - 1, CAP, CAP + 1, 2 * CAP - 1, 2 * CAP, 5 * CAP + 7,
        ST_DOC_CNT / 2, ST_DOC_CNT - CAP, ST_DOC_CNT - 2, ST_DOC_CNT - 1};
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    for (size_t k = 0; k < sizeof(positions) / sizeof(positions[0]); ++k) {
      const int64_t pos = positions[k];
      const uint64_t target = expect_doc(pos);
      uint64_t target_storage = 0;
      ObDatum target_datum;
      make_doc_id_datum(target_storage, target, target_datum);
      OK(iter->advance_to(target_datum));
      const ObDatum *id_datum = nullptr;
      OK(iter->get_cur_id(id_datum));
      ASSERT_EQ(target, id_datum->get_uint64()) << "advance_to exact present target, pos " << pos;
      // A following next() steps to the immediate successor (unless we are at the last doc).
      if (pos + 1 < ST_DOC_CNT) {
        OK(iter->next());
        OK(iter->get_cur_id(id_datum));
        ASSERT_EQ(expect_doc(pos + 1), id_datum->get_uint64()) << "next() after advance_to, pos " << pos;
      }
    }
    free_iter(iter);
  }

  // Case 2: absent (even) targets must round up to the next present (odd) doc, including
  // targets that straddle a posting-block boundary (e.g. 2*CAP -> doc at index CAP).
  {
    // even_target just below the doc at index `pos` rounds up to expect_doc(pos).
    const int64_t round_up_positions[] = {1, CAP, CAP + 1, 2 * CAP, 3 * CAP, ST_DOC_CNT - 1};
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    for (size_t k = 0; k < sizeof(round_up_positions) / sizeof(round_up_positions[0]); ++k) {
      const int64_t pos = round_up_positions[k];
      const uint64_t present = expect_doc(pos);
      const uint64_t even_target = present - 1;  // absent (even), one below a present odd doc
      ASSERT_EQ(static_cast<int64_t>(pos), idx_ge(even_target));
      uint64_t target_storage = 0;
      ObDatum target_datum;
      make_doc_id_datum(target_storage, even_target, target_datum);
      OK(iter->advance_to(target_datum));
      const ObDatum *id_datum = nullptr;
      OK(iter->get_cur_id(id_datum));
      ASSERT_EQ(present, id_datum->get_uint64())
          << "advance_to(absent) rounds up to next present doc, pos " << pos;
    }
    free_iter(iter);
  }

  // Case 3: advance_to(START) lands on the first doc; advance_to(current) is a no-op.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, ST_START, datum);
    OK(iter->advance_to(datum));
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(ST_START, id_datum->get_uint64());
    // advance_to(current doc) stays put (inclusive lower bound on the same id).
    uint64_t storage2 = 0;
    ObDatum datum2;
    make_doc_id_datum(storage2, ST_START, datum2);
    OK(iter->advance_to(datum2));
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(ST_START, id_datum->get_uint64()) << "advance_to(current) must not move";
    free_iter(iter);
  }

  // Case 4: one large cross-macro jump deep into the term, then scan the tail with next()
  // and verify every doc matches the expected sequence to the end.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const int64_t jump_pos = ST_DOC_CNT * 9 / 10;  // ~900k-th doc
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, expect_doc(jump_pos), datum);
    OK(iter->advance_to(datum));
    int64_t i = jump_pos;
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(expect_doc(i), id_datum->get_uint64());
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret = iter->next())) {
      OK(iter->get_cur_id(id_datum));
      ++i;
      if (id_datum->get_uint64() != expect_doc(i)) {
        ASSERT_EQ(expect_doc(i), id_datum->get_uint64()) << "tail scan mismatch at pos " << i;
        break;
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(ST_DOC_CNT - 1, i) << "tail scan must reach the last doc exactly once";
    free_iter(iter);
  }
}

// advance_shallow() on the large term: shallow positioning issues no posting-data IO and
// rejects row getters; next() fails in shallow status while advance_to() materializes;
// consecutive shallow advances compose; shallow beyond the term reaches iter-end.
TEST_F(TestInvSSTablePostingIter, single_term_advance_shallow_scenarios)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;

  const int64_t CAP = DEFAULT_POSTING_BLOCK_CAPACITY;

  // Case 1: shallow to a far cross-block target; next() fails, then advance_to() materializes.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const int64_t pos = 1234 * CAP + 13;  // deep, mid-block position
    const uint64_t target = expect_doc(pos);
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, target, datum);
    OK(iter->advance_shallow(datum, true /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());
    ASSERT_FALSE(iter->curr_block_loaded_);  // no posting-data IO issued by shallow advance
    const ObDatum *shallow_id = nullptr;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_cur_id(shallow_id));
    int64_t tf = 0;
    int64_t dl = 0;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_curr_bm25_param(tf, dl));
    uint64_t min_id = 0, max_id = 0;
    double max_score = 0.0;
    ASSERT_EQ(OB_SUCCESS, iter->get_block_max_tuple(min_id, max_id, max_score));  // no block-max metadata
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->next());
    ASSERT_TRUE(iter->in_shallow_status());
    OK(iter->advance_to(datum));
    ASSERT_FALSE(iter->in_shallow_status());
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(target, id_datum->get_uint64());
    free_iter(iter);
  }

  // Case 2: shallow to an absent (even) target; next() fails, then advance_to() rounds up.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const int64_t pos = 7 * CAP;          // block boundary position
    const uint64_t even_target = expect_doc(pos) - 1;  // absent
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, even_target, datum);
    OK(iter->advance_shallow(datum, true /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->next());
    ASSERT_TRUE(iter->in_shallow_status());
    OK(iter->advance_to(datum));
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(expect_doc(pos), id_datum->get_uint64()) << "advance_to() after shallow(absent) rounds up";
    free_iter(iter);
  }

  // Case 3: advance_shallow(T1) then advance_to(T2 > T1) without next() — advance_to materializes
  // directly and clears shallow.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const int64_t p1 = 100 * CAP + 5;
    const int64_t p2 = 300 * CAP + 9;  // farther forward
    uint64_t s1 = 0;
    ObDatum d1;
    make_doc_id_datum(s1, expect_doc(p1), d1);
    OK(iter->advance_shallow(d1, true /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());
    uint64_t s2 = 0;
    ObDatum d2;
    make_doc_id_datum(s2, expect_doc(p2), d2);
    OK(iter->advance_to(d2));
    ASSERT_FALSE(iter->in_shallow_status());
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(expect_doc(p2), id_datum->get_uint64());
    free_iter(iter);
  }

  // Case 4: consecutive shallow advances compose; advance_to(last_target) materializes correctly.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const int64_t p1 = 50 * CAP + 1;
    const int64_t p2 = 800 * CAP + 200;
    uint64_t s1 = 0;
    ObDatum d1;
    make_doc_id_datum(s1, expect_doc(p1), d1);
    OK(iter->advance_shallow(d1, true /*inclusive*/));
    uint64_t s2 = 0;
    ObDatum d2;
    make_doc_id_datum(s2, expect_doc(p2), d2);
    OK(iter->advance_shallow(d2, true /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->next());
    ASSERT_TRUE(iter->in_shallow_status());
    OK(iter->advance_to(d2));
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(expect_doc(p2), id_datum->get_uint64()) << "advance_to() after two shallow advances";
    free_iter(iter);
  }

  // Case 5: exclusive shallow seek lands on a block whose max_id is strictly greater than target.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const int64_t p = 16 * CAP + 7;
    const uint64_t target = expect_doc(p);
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, target, datum);
    OK(iter->advance_shallow(datum, false /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());
    uint64_t min_id = 0;
    uint64_t max_id = 0;
    double block_max_score = 0.0;
    const int tuple_ret = iter->get_block_max_tuple(min_id, max_id, block_max_score);
    ASSERT_TRUE(OB_SUCCESS == tuple_ret || OB_NOT_SUPPORTED == tuple_ret);
    ASSERT_GT(max_id, target);
    free_iter(iter);
  }

  // Case 6: advance_shallow beyond the term's max doc id reaches iter-end.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, st_max_doc() + 1, datum);
    ASSERT_EQ(OB_ITER_END, iter->advance_shallow(datum, true /*inclusive*/));
    ASSERT_TRUE(iter->is_end());
    ASSERT_EQ(OB_ITER_END, iter->next());
    free_iter(iter);
  }
}

// advance_shallow() decoded-block reuse: when the target stays inside the already decoded
// current block, the shallow advance retains the decoded view (no re-decode) and a following
// advance_to() reuses the same block buffer; a shallow that crosses posting blocks invalidates
// the stale decoded view.
TEST_F(TestInvSSTablePostingIter, advance_shallow_decoded_block_reuse)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, DRAIN_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;

  const int64_t CAP = DEFAULT_POSTING_BLOCK_CAPACITY;  // 256

  // Case 1: same-block shallow hit retains the decoded view; advance_to reuses it.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    // Load block 0 by materializing an early row.
    uint64_t s0 = 0;
    ObDatum d0;
    make_doc_id_datum(s0, expect_doc(5), d0);
    OK(iter->advance_to(d0));
    ASSERT_TRUE(iter->curr_block_loaded_);
    const uint64_t *decoded_ids = iter->posting_block_data_.get_doc_ids();
    ASSERT_NE(nullptr, decoded_ids);

    // Shallow to a larger target still inside block 0 (covered hit → no scanner move).
    const uint64_t target = expect_doc(100);
    uint64_t st = 0;
    ObDatum td;
    make_doc_id_datum(st, target, td);
    OK(iter->advance_shallow(td, true /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());
    ASSERT_TRUE(iter->curr_block_loaded_) << "covered shallow hit must retain the decoded block";
    ASSERT_EQ(-1, iter->curr_row_idx_) << "logical row cursor cleared while shallow";
    ASSERT_EQ(decoded_ids, iter->posting_block_data_.get_doc_ids())
        << "decoded block pointer must be unchanged after covered shallow";
    const ObDatum *shallow_id = nullptr;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_cur_id(shallow_id));

    // advance_to lands on the target via lower-bound, still reusing the decoded block.
    OK(iter->advance_to(td));
    ASSERT_FALSE(iter->in_shallow_status());
    ASSERT_EQ(decoded_ids, iter->posting_block_data_.get_doc_ids())
        << "advance_to after covered shallow must reuse the decoded block";
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(target, id_datum->get_uint64());
    free_iter(iter);
  }

  // Case 2: gap (absent even) target inside the decoded block: shallow retains, advance_to
  // rounds up to the first present (odd) doc >= target with inclusive semantics.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    uint64_t s0 = 0;
    ObDatum d0;
    make_doc_id_datum(s0, expect_doc(3), d0);
    OK(iter->advance_to(d0));
    ASSERT_TRUE(iter->curr_block_loaded_);
    const uint64_t *decoded_ids = iter->posting_block_data_.get_doc_ids();

    const uint64_t even_target = expect_doc(80) - 1;  // absent (even), inside block 0
    uint64_t st = 0;
    ObDatum td;
    make_doc_id_datum(st, even_target, td);
    OK(iter->advance_shallow(td, true /*inclusive*/));
    ASSERT_TRUE(iter->curr_block_loaded_);
    ASSERT_EQ(decoded_ids, iter->posting_block_data_.get_doc_ids());

    OK(iter->advance_to(td));
    ASSERT_EQ(decoded_ids, iter->posting_block_data_.get_doc_ids());
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(expect_doc(80), id_datum->get_uint64()) << "advance_to(absent) rounds up in-block";
    free_iter(iter);
  }

  // Case 3: cross-block shallow invalidates the stale decoded view; advance_to loads the
  // new block correctly (doc_id, tf, doc_length all consistent).
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    uint64_t s0 = 0;
    ObDatum d0;
    make_doc_id_datum(s0, expect_doc(5), d0);
    OK(iter->advance_to(d0));
    ASSERT_TRUE(iter->curr_block_loaded_);

    // Shallow far ahead, several blocks away (not covered by block 0).
    const int64_t far_pos = 5 * CAP + 42;
    const uint64_t far_target = expect_doc(far_pos);
    uint64_t st = 0;
    ObDatum td;
    make_doc_id_datum(st, far_target, td);
    OK(iter->advance_shallow(td, true /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());
    ASSERT_FALSE(iter->curr_block_loaded_) << "cross-block shallow must invalidate the decoded view";
    ASSERT_EQ(-1, iter->curr_row_idx_);
    const ObDatum *shallow_id = nullptr;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_cur_id(shallow_id));

    OK(iter->advance_to(td));
    ASSERT_FALSE(iter->in_shallow_status());
    ASSERT_TRUE(iter->curr_block_loaded_);
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(far_target, id_datum->get_uint64());
    int64_t tf = 0;
    int64_t dl = 0;
    OK(iter->get_curr_bm25_param(tf, dl));
    ASSERT_EQ(1, tf);
    ASSERT_EQ(100, dl);
    free_iter(iter);
  }
}

// Centerpiece: a long, deterministic, reproducible op sequence that interleaves next(),
// forward advance_to() (small in-block and large cross-block jumps), and advance_shallow()+advance_to()
// over the large term, validating every materialized doc against the expected sequence. A logical
// cursor `pos` tracks the iterator's current posting position to keep the oracle exact.
TEST_F(TestInvSSTablePostingIter, single_term_interleave_and_stress)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  const int64_t CAP = DEFAULT_POSTING_BLOCK_CAPACITY;
  int64_t pos = -1;          // logical posting position of the current row (-1 == before-first)
  int64_t ops = 0;
  bool done = false;
  // A fixed, varied jump schedule (no RNG/time) that crosses in-block and cross-block edges.
  const int64_t jump_table[] = {1, 3, CAP - 2, CAP, 2 * CAP + 5, 1, 7 * CAP, 13, 999, 4 * CAP + 1};
  const int64_t jump_n = sizeof(jump_table) / sizeof(jump_table[0]);

  for (int64_t round = 0; OB_LIKELY(!done) && round < 100000; ++round) {
    const int mode = static_cast<int>(round % 3);
    const ObDatum *id_datum = nullptr;
    if (0 == mode) {
      // plain next()
      const int ret = iter->next();
      if (OB_ITER_END == ret) {
        ASSERT_EQ(ST_DOC_CNT - 1, pos) << "next() hit end before the last doc";
        done = true;
      } else {
        OK(ret);
        ++pos;
        OK(iter->get_cur_id(id_datum));
        if (id_datum->get_uint64() != expect_doc(pos)) {
          ASSERT_EQ(expect_doc(pos), id_datum->get_uint64()) << "next() oracle mismatch at pos " << pos;
          break;
        }
      }
    } else if (1 == mode) {
      // forward advance_to(): target = current doc + a jump (an absent even id to also test round-up).
      const int64_t base = (pos < 0) ? 0 : pos;
      const int64_t jump = jump_table[round % jump_n];
      const int64_t tpos = base + jump;
      if (tpos >= ST_DOC_CNT) {
        // target beyond the term -> iter-end.
        uint64_t storage = 0;
        ObDatum datum;
        make_doc_id_datum(storage, st_max_doc() + 2, datum);
        ASSERT_EQ(OB_ITER_END, iter->advance_to(datum));
        ASSERT_TRUE(iter->is_end());
        done = true;
      } else {
        // even (absent) target one below the present doc at tpos -> rounds up to tpos.
        const uint64_t target = expect_doc(tpos) - 1;
        ASSERT_EQ(tpos, idx_ge(target));
        uint64_t storage = 0;
        ObDatum datum;
        make_doc_id_datum(storage, target, datum);
        OK(iter->advance_to(datum));
        pos = tpos;
        OK(iter->get_cur_id(id_datum));
        if (id_datum->get_uint64() != expect_doc(pos)) {
          ASSERT_EQ(expect_doc(pos), id_datum->get_uint64()) << "advance_to oracle mismatch at pos " << pos;
          break;
        }
      }
    } else {
      // advance_shallow() forward, next() fails in shallow, then advance_to() materializes.
      const int64_t base = (pos < 0) ? 0 : pos;
      const int64_t jump = jump_table[(round + 4) % jump_n];
      const int64_t tpos = base + jump;
      if (tpos >= ST_DOC_CNT) {
        uint64_t storage = 0;
        ObDatum datum;
        make_doc_id_datum(storage, st_max_doc() + 3, datum);
        ASSERT_EQ(OB_ITER_END, iter->advance_shallow(datum, true /*inclusive*/));
        ASSERT_TRUE(iter->is_end());
        done = true;
      } else {
        const uint64_t target = expect_doc(tpos);  // present target
        uint64_t storage = 0;
        ObDatum datum;
        make_doc_id_datum(storage, target, datum);
        OK(iter->advance_shallow(datum, true /*inclusive*/));
        ASSERT_TRUE(iter->in_shallow_status());
        ASSERT_EQ(OB_ERR_UNEXPECTED, iter->next());
        ASSERT_TRUE(iter->in_shallow_status());
        OK(iter->advance_to(datum));
        ASSERT_FALSE(iter->in_shallow_status());
        pos = tpos;
        OK(iter->get_cur_id(id_datum));
        if (id_datum->get_uint64() != expect_doc(pos)) {
          ASSERT_EQ(expect_doc(pos), id_datum->get_uint64()) << "shallow+advance_to oracle mismatch at pos " << pos;
          break;
        }
      }
    }
    ++ops;
  }
  ASSERT_TRUE(done) << "stress sequence should terminate at iter-end";
  ASSERT_GT(ops, 0);
  STORAGE_LOG(INFO, "interleave stress done", K(ops), K(pos));

  free_iter(iter);
}

// End / error boundaries on the large term: advance past the term, exhaustion stability, and
// invalid target datum handling.
TEST_F(TestInvSSTablePostingIter, single_term_end_and_error_boundaries)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;

  // Case 1: advance_to beyond the term's max -> iter-end, stable on repeated next().
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, st_max_doc() + 1, datum);
    ASSERT_EQ(OB_ITER_END, iter->advance_to(datum));
    ASSERT_TRUE(iter->is_end());
    ASSERT_EQ(OB_ITER_END, iter->next());
    ASSERT_EQ(OB_ITER_END, iter->next());
    free_iter(iter);
  }

  // Case 2: next() to exhaustion via a near-end jump, then advance_to/advance_shallow stay iter-end.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, expect_doc(ST_DOC_CNT - 1), datum);
    OK(iter->advance_to(datum));  // land on the last doc
    ASSERT_EQ(OB_ITER_END, iter->next());  // step off the end
    ASSERT_TRUE(iter->is_end());
    uint64_t s2 = 0;
    ObDatum d2;
    make_doc_id_datum(s2, expect_doc(ST_DOC_CNT / 2), d2);  // backward target ignored once ended
    ASSERT_EQ(OB_ITER_END, iter->advance_to(d2));
    ASSERT_EQ(OB_ITER_END, iter->advance_shallow(d2, true /*inclusive*/));
    free_iter(iter);
  }

  // Case 3: advance_to with a non-uint (string) datum -> OB_INVALID_ARGUMENT.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    ObDatum bad_datum;
    bad_datum.set_string("not-a-uint", 10);  // len_ != sizeof(uint64_t)
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter->advance_to(bad_datum));
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter->advance_shallow(bad_datum, true /*inclusive*/));
    free_iter(iter);
  }
}

// Calls on a freshly constructed (never opened) iterator must return OB_NOT_INIT.
TEST_F(TestInvSSTablePostingIter, not_inited_calls_return_not_init)
{
  ObInvSSTablePostingIter iter;
  ASSERT_EQ(OB_NOT_INIT, iter.next());
  uint64_t storage = 0;
  ObDatum datum;
  make_doc_id_datum(storage, 1, datum);
  ASSERT_EQ(OB_NOT_INIT, iter.advance_to(datum));
  ASSERT_EQ(OB_NOT_INIT, iter.advance_shallow(datum, true /*inclusive*/));
}

// =============================================================================
// Storage-layer batch drain (drain_until) tests. The large single-term dataset
// (odd doc_ids 1,3,5,...; even doc_ids absent) exercises inclusive-upper-bound
// boundaries on present ids, on gap ids, on block edges and across blocks. Every
// case cross-checks the batch output against the analytic expect_doc()/hetero_*()
// oracle and the three terminal states (BUFFER_FULL / BOUNDARY_REACHED / ITER_END).
// =============================================================================

// Cases 1-4: fresh iterator, boundary landing inside the first block — on a present
// id, exactly on an id, on a gap id, and below the first id (count == 0).
TEST_F(TestInvSSTablePostingIter, drain_boundary_inside_first_block)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;

  std::vector<uint64_t> ids, tfs, dls;
  bool hit_end = false;
  uint64_t next_id = 0, next_tf = 0, next_dl = 0;

  // Case 1: boundary on a present id inside the first block.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    drain_collect(iter, expect_doc(100), DEFAULT_POSTING_BLOCK_CAPACITY,
        ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
    ASSERT_FALSE(hit_end);
    ASSERT_EQ(101u, ids.size());
    for (int64_t i = 0; i < static_cast<int64_t>(ids.size()); ++i) {
      ASSERT_EQ(expect_doc(i), ids[i]);
      ASSERT_LE(ids[i], expect_doc(100));
      ASSERT_EQ(1u, tfs[i]);
      ASSERT_EQ(100u, dls[i]);
    }
    ASSERT_EQ(expect_doc(101), next_id);
    ASSERT_GT(next_id, expect_doc(100));
    free_iter(iter);
  }

  // Case 2: boundary exactly equal to a present id.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    drain_collect(iter, expect_doc(50), DEFAULT_POSTING_BLOCK_CAPACITY,
        ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
    ASSERT_FALSE(hit_end);
    ASSERT_EQ(51u, ids.size());
    ASSERT_EQ(expect_doc(50), ids.back());
    ASSERT_EQ(expect_doc(51), next_id);
    free_iter(iter);
  }

  // Case 3: boundary on a gap (absent even id) between two present ids.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const uint64_t gap = expect_doc(50) + 1;  // even, absent
    drain_collect(iter, gap, DEFAULT_POSTING_BLOCK_CAPACITY,
        ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
    ASSERT_FALSE(hit_end);
    ASSERT_EQ(51u, ids.size());
    ASSERT_EQ(expect_doc(50), ids.back());
    ASSERT_LE(ids.back(), gap);
    ASSERT_EQ(expect_doc(51), next_id);
    ASSERT_GT(next_id, gap);
    free_iter(iter);
  }

  // Case 4: boundary below the first id -> empty output, prefetch first id.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    drain_collect(iter, 0, DEFAULT_POSTING_BLOCK_CAPACITY,
        ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
    ASSERT_FALSE(hit_end);
    ASSERT_EQ(0u, ids.size());
    ASSERT_EQ(expect_doc(0), next_id);
    free_iter(iter);
  }
}

// Cases 5-6: capacity smaller than the in-boundary row count (repeat with same
// boundary must not duplicate/drop), and capacity exactly equal to the remaining
// in-boundary rows (fills as BUFFER_FULL, next call reports the boundary).
TEST_F(TestInvSSTablePostingIter, drain_capacity_boundaries)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;

  std::vector<uint64_t> ids, tfs, dls;
  bool hit_end = false;
  uint64_t next_id = 0, next_tf = 0, next_dl = 0;

  // Case 5: 200 rows within boundary, capacity 50 -> collected across 4 full batches
  // plus a final boundary batch, with no duplicates and exact ordering.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    drain_collect(iter, expect_doc(199), 50, ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
    ASSERT_FALSE(hit_end);
    ASSERT_EQ(200u, ids.size());
    for (int64_t i = 0; i < 200; ++i) {
      ASSERT_EQ(expect_doc(i), ids[i]);
    }
    ASSERT_EQ(expect_doc(200), next_id);
    free_iter(iter);
  }

  // Case 6a: max_count smaller than the block run -> a single call projects exactly
  // max_count rows (count > 0); more rows <= boundary remain, iterator not ended.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const uint64_t *pid = nullptr, *ptf = nullptr, *pdl = nullptr;
    int64_t n = 0;
    OK(iter->drain_until(drain_max_doc(), 50, pid, ptf, pdl, n));
    ASSERT_EQ(50, n);
    ASSERT_FALSE(iter->is_end());
    ASSERT_EQ(expect_doc(0), pid[0]);
    ASSERT_EQ(expect_doc(49), pid[49]);
    free_iter(iter);
  }

  // Case 6b: the in-boundary rows are projected (count > 0), then the following call
  // returns count == 0 with the cursor on the first row > boundary (not ended).
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    const uint64_t *pid = nullptr, *ptf = nullptr, *pdl = nullptr;
    int64_t n = 0;
    OK(iter->drain_until(expect_doc(99), 100, pid, ptf, pdl, n));
    ASSERT_EQ(100, n);
    ASSERT_EQ(expect_doc(99), pid[99]);
    OK(iter->drain_until(expect_doc(99), 100, pid, ptf, pdl, n));
    ASSERT_EQ(0, n);
    ASSERT_FALSE(iter->is_end());
    const ObDatum *bid = nullptr;
    OK(iter->get_cur_id(bid));
    ASSERT_EQ(expect_doc(100), bid->get_uint64());
    free_iter(iter);
  }
}

// Case 7: a single drain_until call never spans a posting-block boundary (one block
// per call). Starting mid-block (via advance_to) near block-0's tail, the first call
// projects only block-0's remaining tail (MORE), and the second call projects block-1
// from its head; the concatenation is contiguous across the block edge.
TEST_F(TestInvSSTablePostingIter, drain_one_block_per_call_contiguous)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  const int64_t CAP = DEFAULT_POSTING_BLOCK_CAPACITY;  // 256, == posting block size
  // Position on posting-block 0 near its tail so only [201, 256) remains in block 0.
  uint64_t st = 0;
  ObDatum td;
  make_doc_id_datum(st, expect_doc(200), td);
  OK(iter->advance_to(td));

  const uint64_t *pid = nullptr, *ptf = nullptr, *pdl = nullptr;
  int64_t n = 0;
  // Call 1: only block-0's tail (pos 201..255 = 55 rows).
  OK(iter->drain_until(drain_max_doc(), CAP, pid, ptf, pdl, n));
  ASSERT_EQ(CAP - 201, n);
  ASSERT_EQ(expect_doc(201), pid[0]);
  ASSERT_EQ(expect_doc(CAP - 1), pid[n - 1]);
  // Call 2: block-1 from its head (pos 256..), contiguous with call 1.
  OK(iter->drain_until(drain_max_doc(), CAP, pid, ptf, pdl, n));
  ASSERT_EQ(CAP, n);
  ASSERT_EQ(expect_doc(CAP), pid[0]) << "second call continues contiguously past the block edge";
  ASSERT_EQ(expect_doc(2 * CAP - 1), pid[n - 1]);
  free_iter(iter);
}

// Case 8: draining the whole term with a boundary past the last id reaches ITER_END
// and yields exactly the full posting list, in order, with correct relevance params.
TEST_F(TestInvSSTablePostingIter, drain_full_term_iter_end)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  std::vector<uint64_t> ids, tfs, dls;
  bool hit_end = false;
  uint64_t next_id = 0, next_tf = 0, next_dl = 0;
  drain_collect(iter, drain_max_doc() + 10, DEFAULT_POSTING_BLOCK_CAPACITY,
      ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
  ASSERT_TRUE(hit_end);
  ASSERT_EQ(static_cast<size_t>(DRAIN_DOC_CNT), ids.size());
  // Manual compare; assert only on the first divergence to avoid per-row macro expansions.
  for (int64_t i = 0; i < DRAIN_DOC_CNT; ++i) {
    if (ids[i] != expect_doc(i) || tfs[i] != 1u || dls[i] != 100u) {
      ASSERT_EQ(expect_doc(i), ids[i]) << "doc mismatch at " << i;
      ASSERT_EQ(1u, tfs[i]) << "tf mismatch at " << i;
      ASSERT_EQ(100u, dls[i]) << "dl mismatch at " << i;
      break;
    }
  }
  ASSERT_TRUE(iter->is_end());
  free_iter(iter);
}

// Case 9: an absent term drains immediately to ITER_END with no rows.
TEST_F(TestInvSSTablePostingIter, drain_absent_term_iter_end)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(999999999, term_buf, sizeof(term_buf));  // absent
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_TRUE(iter->is_end());
  const uint64_t *pid = nullptr, *ptf = nullptr, *pdl = nullptr;
  int64_t n = 0;
  OK(iter->drain_until(drain_max_doc(), DEFAULT_POSTING_BLOCK_CAPACITY, pid, ptf, pdl, n));
  ASSERT_EQ(0, n);
  ASSERT_TRUE(iter->is_end());
  free_iter(iter);
}

// Cases 10-13: scalar / batch interleaving. drain never re-emits the cached current
// row; drain begins after an advanced cursor; and scalar next()/advance_to() resume
// correctly from the final batch cursor.
TEST_F(TestInvSSTablePostingIter, drain_interleaves_with_scalar)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  const ObDatum *id_datum = nullptr;
  const uint64_t *pid = nullptr, *ptf = nullptr, *pdl = nullptr;
  int64_t n = 0;

  std::vector<uint64_t> ids, tfs, dls;
  bool hit_end = false;
  uint64_t next_id = 0, next_tf = 0, next_dl = 0;

  // Case 10: next() then drain -> drain starts at the row AFTER the current (no repeat).
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    OK(iter->next());  // current = pos 0
    drain_collect(iter, expect_doc(300), DEFAULT_POSTING_BLOCK_CAPACITY,
        ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
    ASSERT_FALSE(hit_end);
    ASSERT_EQ(300u, ids.size());
    ASSERT_EQ(expect_doc(1), ids.front()) << "current row must not be repeated";
    ASSERT_EQ(expect_doc(300), ids.back());
    ASSERT_EQ(expect_doc(301), next_id);
    free_iter(iter);
  }

  // Case 11: advance_to() then drain -> drain starts after the advanced row.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    uint64_t st = 0;
    ObDatum td;
    make_doc_id_datum(st, expect_doc(500), td);
    OK(iter->advance_to(td));  // current = pos 500
    drain_collect(iter, expect_doc(510), DEFAULT_POSTING_BLOCK_CAPACITY,
        ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
    ASSERT_FALSE(hit_end);
    ASSERT_EQ(10u, ids.size());
    ASSERT_EQ(expect_doc(501), ids.front());
    ASSERT_EQ(expect_doc(510), ids.back());
    ASSERT_EQ(expect_doc(511), next_id);
    free_iter(iter);
  }

  // Case 12: drain (capped, count > 0) then next() continues from the final batch cursor.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    OK(iter->drain_until(drain_max_doc(), 100, pid, ptf, pdl, n));
    ASSERT_EQ(100, n);
    ASSERT_EQ(expect_doc(99), pid[99]);
    OK(iter->next());
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(expect_doc(100), id_datum->get_uint64());
    free_iter(iter);
  }

  // Case 13: drain (capped, count > 0) then advance_to() keeps lower-bound semantics.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    OK(iter->drain_until(drain_max_doc(), 100, pid, ptf, pdl, n));
    ASSERT_EQ(100, n);
    uint64_t st = 0;
    ObDatum td;
    make_doc_id_datum(st, expect_doc(150), td);
    OK(iter->advance_to(td));
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(expect_doc(150), id_datum->get_uint64());
    free_iter(iter);
  }
}

// Case 14: drain_until in shallow status is rejected (mirrors next()'s constraint).
TEST_F(TestInvSSTablePostingIter, drain_in_shallow_status_rejected)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  uint64_t st = 0;
  ObDatum td;
  make_doc_id_datum(st, expect_doc(1000), td);
  OK(iter->advance_shallow(td, true /*inclusive*/));
  ASSERT_TRUE(iter->in_shallow_status());
  const uint64_t *pid = nullptr, *ptf = nullptr, *pdl = nullptr;
  int64_t n = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      iter->drain_until(drain_max_doc(), DEFAULT_POSTING_BLOCK_CAPACITY, pid, ptf, pdl, n));
  free_iter(iter);
}

// Case 15: heterogeneous relevance params — batch output tf/doc_length must match the
// per-position oracle exactly (proves per-column copy is not off-by-one/swapped), across
// several posting blocks.
TEST_F(TestInvSSTablePostingIter, drain_heterogeneous_relevance_params)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ,
      inv_sstable, true /*hetero*/);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  std::vector<uint64_t> ids, tfs, dls;
  bool hit_end = false;
  uint64_t next_id = 0, next_tf = 0, next_dl = 0;
  // 1001 rows spans ~4 posting blocks (capacity 256).
  drain_collect(iter, expect_doc(1000), DEFAULT_POSTING_BLOCK_CAPACITY,
      ids, tfs, dls, hit_end, next_id, next_tf, next_dl);
  ASSERT_FALSE(hit_end);
  ASSERT_EQ(1001u, ids.size());
  for (int64_t i = 0; i < static_cast<int64_t>(ids.size()); ++i) {
    ASSERT_EQ(expect_doc(i), ids[i]);
    ASSERT_EQ(hetero_tf(i), tfs[i]) << "tf mismatch at " << i;
    ASSERT_EQ(hetero_dl(i), dls[i]) << "dl mismatch at " << i;
  }
  ASSERT_EQ(expect_doc(1001), next_id);
  ASSERT_EQ(hetero_tf(1001), next_tf);
  ASSERT_EQ(hetero_dl(1001), next_dl);
  free_iter(iter);
}

// Batch drain positions only the row cursor; relevance is materialized by the explicit
// lazy loader and ordinary scalar advance keeps its eager-materialization contract.
TEST_F(TestInvSSTablePostingIter, drain_materializes_relevance_on_demand)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ,
      inv_sstable, true /*hetero*/);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  const uint64_t boundary = expect_doc(100);
  const uint64_t *pid = nullptr;
  const uint64_t *ptf = nullptr;
  const uint64_t *pdl = nullptr;
  int64_t n = 0;

  // 第一批投影 0..63，cursor 只定位到第 63 行且尚未读取相关性缓存。
  OK(iter->drain_until(boundary, 64, pid, ptf, pdl, n));
  ASSERT_EQ(64, n);
  ASSERT_EQ(expect_doc(0), pid[0]);
  ASSERT_EQ(expect_doc(63), pid[n - 1]);
  ASSERT_EQ(63, iter->curr_row_idx_);
  ASSERT_FALSE(iter->curr_relevance_materialized_);

  const ObDatum *id_datum = nullptr;
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(expect_doc(63), id_datum->get_uint64());
  ASSERT_FALSE(iter->curr_relevance_materialized_);

  // 首次 loader 读取第 63 行的异构 tf/dl，第二次 getter 复用缓存且 cursor 不移动。
  int64_t tf = 0;
  int64_t dl = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_curr_bm25_param(tf, dl));
  OK(iter->load_curr_bm25_param(tf, dl));
  ASSERT_EQ(hetero_tf(63), static_cast<uint64_t>(tf));
  ASSERT_EQ(hetero_dl(63), static_cast<uint64_t>(dl));
  ASSERT_TRUE(iter->curr_relevance_materialized_);
  int64_t cached_tf = 0;
  int64_t cached_dl = 0;
  OK(iter->get_curr_bm25_param(cached_tf, cached_dl));
  ASSERT_EQ(tf, cached_tf);
  ASSERT_EQ(dl, cached_dl);
  ASSERT_EQ(63, iter->curr_row_idx_);

  // 第二批投影 64..100 后重新进入未物化状态。
  OK(iter->drain_until(boundary, 64, pid, ptf, pdl, n));
  ASSERT_EQ(37, n);
  ASSERT_EQ(expect_doc(64), pid[0]);
  ASSERT_EQ(expect_doc(100), pid[n - 1]);
  ASSERT_EQ(100, iter->curr_row_idx_);
  ASSERT_FALSE(iter->curr_relevance_materialized_);

  // 第三次调用只定位首个越界 lookahead 行 101，loader 再按需物化该行相关性。
  OK(iter->drain_until(boundary, 64, pid, ptf, pdl, n));
  ASSERT_EQ(0, n);
  ASSERT_FALSE(iter->is_end());
  ASSERT_EQ(101, iter->curr_row_idx_);
  ASSERT_FALSE(iter->curr_relevance_materialized_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_curr_bm25_param(tf, dl));
  OK(iter->load_curr_bm25_param(tf, dl));
  ASSERT_EQ(hetero_tf(101), static_cast<uint64_t>(tf));
  ASSERT_EQ(hetero_dl(101), static_cast<uint64_t>(dl));
  ASSERT_TRUE(iter->curr_relevance_materialized_);

  // 连续 drain 跨过 posting block 边界，block 切换后仍只定位 cursor。
  OK(iter->drain_until(drain_max_doc(), DEFAULT_POSTING_BLOCK_CAPACITY, pid, ptf, pdl, n));
  ASSERT_EQ(DEFAULT_POSTING_BLOCK_CAPACITY - 102, n);
  ASSERT_EQ(expect_doc(102), pid[0]);
  ASSERT_EQ(expect_doc(DEFAULT_POSTING_BLOCK_CAPACITY - 1), pid[n - 1]);
  ASSERT_FALSE(iter->curr_relevance_materialized_);
  OK(iter->drain_until(drain_max_doc(), DEFAULT_POSTING_BLOCK_CAPACITY, pid, ptf, pdl, n));
  ASSERT_EQ(DEFAULT_POSTING_BLOCK_CAPACITY, n);
  ASSERT_EQ(expect_doc(DEFAULT_POSTING_BLOCK_CAPACITY), pid[0]);
  ASSERT_EQ(expect_doc(2 * DEFAULT_POSTING_BLOCK_CAPACITY - 1), pid[n - 1]);
  ASSERT_FALSE(iter->curr_relevance_materialized_);

  // 标量 advance_to 定位下一行时继续 eager 读取准确的相关性参数。
  uint64_t target_storage = 0;
  ObDatum target;
  make_doc_id_datum(target_storage, expect_doc(2 * DEFAULT_POSTING_BLOCK_CAPACITY), target);
  OK(iter->advance_to(target));
  ASSERT_TRUE(iter->curr_relevance_materialized_);
  OK(iter->get_curr_bm25_param(tf, dl));
  ASSERT_EQ(hetero_tf(2 * DEFAULT_POSTING_BLOCK_CAPACITY), static_cast<uint64_t>(tf));
  ASSERT_EQ(hetero_dl(2 * DEFAULT_POSTING_BLOCK_CAPACITY), static_cast<uint64_t>(dl));
  free_iter(iter);
}

// drain_until_from_current() accepts an id-only cursor and projects that row directly.
TEST_F(TestInvSSTablePostingIter, drain_from_current_after_advance_to_id)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ,
      inv_sstable, true /*hetero*/);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  uint64_t target_storage = 0;
  ObDatum target;
  make_doc_id_datum(target_storage, expect_doc(10), target);
  OK(iter->advance_to_id(target));
  ASSERT_EQ(10, iter->curr_row_idx_);
  ASSERT_FALSE(iter->curr_relevance_materialized_);

  const uint64_t *pid = nullptr;
  const uint64_t *ptf = nullptr;
  const uint64_t *pdl = nullptr;
  int64_t n = 0;
  OK(iter->drain_until_from_current(expect_doc(13), 4, pid, ptf, pdl, n));
  ASSERT_EQ(4, n);
  ASSERT_EQ(expect_doc(10), pid[0]);
  ASSERT_EQ(expect_doc(13), pid[n - 1]);
  ASSERT_EQ(hetero_tf(10), ptf[0]);
  ASSERT_EQ(hetero_dl(10), pdl[0]);
  ASSERT_EQ(hetero_tf(13), ptf[n - 1]);
  ASSERT_EQ(hetero_dl(13), pdl[n - 1]);
  ASSERT_EQ(13, iter->curr_row_idx_);
  ASSERT_FALSE(iter->curr_relevance_materialized_);
  free_iter(iter);
}

// advance_to_id() does not decode relevance for a newly positioned row.
TEST_F(TestInvSSTablePostingIter, advance_to_id_positions_without_materializing_relevance)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ,
      inv_sstable, true /*hetero*/);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  uint64_t target_storage = 0;
  ObDatum absent_target;
  make_doc_id_datum(target_storage, expect_doc(10) + 1, absent_target);
  OK(iter->advance_to_id(absent_target));
  ASSERT_EQ(11, iter->curr_row_idx_);
  ASSERT_FALSE(iter->curr_relevance_materialized_);

  const ObDatum *id_datum = nullptr;
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(expect_doc(11), id_datum->get_uint64());
  int64_t tf = 0;
  int64_t dl = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_curr_bm25_param(tf, dl));
  OK(iter->load_curr_bm25_param(tf, dl));
  ASSERT_EQ(hetero_tf(11), static_cast<uint64_t>(tf));
  ASSERT_EQ(hetero_dl(11), static_cast<uint64_t>(dl));
  ASSERT_TRUE(iter->curr_relevance_materialized_);

  ObDatum backward_target;
  make_doc_id_datum(target_storage, expect_doc(5), backward_target);
  OK(iter->advance_to_id(backward_target));
  ASSERT_EQ(11, iter->curr_row_idx_);
  ASSERT_TRUE(iter->curr_relevance_materialized_);

  OK(iter->next());
  ASSERT_EQ(12, iter->curr_row_idx_);
  ASSERT_TRUE(iter->curr_relevance_materialized_);
  OK(iter->get_curr_bm25_param(tf, dl));
  ASSERT_EQ(hetero_tf(12), static_cast<uint64_t>(tf));
  ASSERT_EQ(hetero_dl(12), static_cast<uint64_t>(dl));

  ObDatum scalar_target;
  make_doc_id_datum(target_storage, expect_doc(13), scalar_target);
  OK(iter->advance_to(scalar_target));
  ASSERT_EQ(13, iter->curr_row_idx_);
  ASSERT_TRUE(iter->curr_relevance_materialized_);
  OK(iter->get_curr_bm25_param(tf, dl));
  ASSERT_EQ(hetero_tf(13), static_cast<uint64_t>(tf));
  ASSERT_EQ(hetero_dl(13), static_cast<uint64_t>(dl));
  free_iter(iter);

  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);
  ObDatum block_target;
  make_doc_id_datum(target_storage, expect_doc(DEFAULT_POSTING_BLOCK_CAPACITY), block_target);
  OK(iter->advance_to_id(block_target));
  ASSERT_EQ(0, iter->curr_row_idx_);
  ASSERT_FALSE(iter->curr_relevance_materialized_);
  OK(iter->load_curr_bm25_param(tf, dl));
  ASSERT_EQ(hetero_tf(DEFAULT_POSTING_BLOCK_CAPACITY), static_cast<uint64_t>(tf));
  ASSERT_EQ(hetero_dl(DEFAULT_POSTING_BLOCK_CAPACITY), static_cast<uint64_t>(dl));

  ObDatum end_target;
  make_doc_id_datum(target_storage, drain_max_doc() + 1, end_target);
  ASSERT_EQ(OB_ITER_END, iter->advance_to_id(end_target));
  ASSERT_TRUE(iter->is_end());
  free_iter(iter);
}

// Reject inconsistent metadata before exposing direct decoded-column ranges.
TEST_F(TestInvSSTablePostingIter, load_rejects_meta_row_count_mismatch)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, 1, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  ++iter->curr_meta_.doc_count_;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->load_current_block());
  ASSERT_FALSE(iter->curr_block_loaded_);
  free_iter(iter);
}

// Case 16: argument validation — non-positive max_count and not-opened iterator.
TEST_F(TestInvSSTablePostingIter, drain_invalid_arguments)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true, DRAIN_DOC_CNT, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  const uint64_t *pid = nullptr, *ptf = nullptr, *pdl = nullptr;
  int64_t n = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      iter->drain_until(drain_max_doc(), 0, pid, ptf, pdl, n));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      iter->drain_until(drain_max_doc(), -1, pid, ptf, pdl, n));
  free_iter(iter);

  ObInvSSTablePostingIter fresh;
  ASSERT_EQ(OB_NOT_INIT,
      fresh.drain_until(drain_max_doc(), 8, pid, ptf, pdl, n));
}

// =============================================================================
// get_curr_bm25_param() scoring / get_block_max_tuple(min, max, score) API tests.
// =============================================================================

// get_curr_bm25_param() on a materialized row yields (tf=1, dl=100); feeding those
// into the canonical BM25 eval with the scan-param corpus stats matches the expected
// score and is stable across rows since every fed row shares those params. This is
// exactly the computation the DASAsyncTokenOp layer now performs on top of the iter.
TEST_F(TestInvSSTablePostingIter, get_curr_bm25_param_score_matches_bm25)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  // Expected score uses the exact mock corpus stats from build_scan_param.
  const double expected = sql::ObExprBM25::eval(
      1 /*token_freq*/, 100 /*doc_length*/,
      param.doc_freq_, param.total_doc_cnt_, param.avg_doc_length_);
  ASSERT_GT(expected, 0.0);

  double prev_score = -1.0;
  int64_t checked = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter->next()) && checked < 1000) {
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    int64_t token_freq = 0;
    int64_t doc_length = 0;
    OK(iter->get_curr_bm25_param(token_freq, doc_length));
    ASSERT_EQ(1, token_freq);
    ASSERT_EQ(100, doc_length);
    const double score = sql::ObExprBM25::eval(
        token_freq, doc_length, param.doc_freq_, param.total_doc_cnt_, param.avg_doc_length_);
    ASSERT_DOUBLE_EQ(expected, score) << "score must match canonical BM25 at row " << checked;
    if (0 != checked) {
      ASSERT_DOUBLE_EQ(prev_score, score) << "score must be stable across rows";
    }
    prev_score = score;
    ++checked;
  }
  ASSERT_GT(checked, 0);
  free_iter(iter);
}

// get_curr_bm25_param() state/error boundaries: not-opened, pre-materialization,
// shallow status, and end-of-iteration.
TEST_F(TestInvSSTablePostingIter, get_curr_bm25_param_state_boundaries)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;

  // Case 1: never opened -> OB_NOT_INIT.
  {
    ObInvSSTablePostingIter iter;
    int64_t tf = 0, dl = 0;
    ASSERT_EQ(OB_NOT_INIT, iter.get_curr_bm25_param(tf, dl));
  }

  // Case 2: opened but no row materialized yet -> OB_ERR_UNEXPECTED.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    int64_t tf = 0, dl = 0;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_curr_bm25_param(tf, dl));
    free_iter(iter);
  }

  // Case 3: shallow status rejects get_curr_bm25_param, then advance_to materializes.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    OK(iter->next());
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    const uint64_t target = id_datum->get_uint64() + 5;
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, target, datum);
    OK(iter->advance_shallow(datum, true /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());
    int64_t tf = 0, dl = 0;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_curr_bm25_param(tf, dl));
    OK(iter->advance_to(datum));
    ASSERT_FALSE(iter->in_shallow_status());
    OK(iter->get_curr_bm25_param(tf, dl));
    ASSERT_GT(sql::ObExprBM25::eval(
        tf, dl, param.doc_freq_, param.total_doc_cnt_, param.avg_doc_length_), 0.0);
    free_iter(iter);
  }

  // Case 4: exhausted term -> OB_ITER_END.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, st_max_doc() + 1, datum);
    ASSERT_EQ(OB_ITER_END, iter->advance_to(datum));
    ASSERT_TRUE(iter->is_end());
    int64_t tf = 0, dl = 0;
    ASSERT_EQ(OB_ITER_END, iter->get_curr_bm25_param(tf, dl));
    free_iter(iter);
  }
}

// On an absent term the iterator opens valid with is_end()==true; get_curr_bm25_param()
// reports OB_ITER_END (not an error), and get_block_max_tuple also reports OB_ITER_END.
TEST_F(TestInvSSTablePostingIter, get_curr_bm25_param_absent_term)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  // A term far beyond any generated term.
  char term_buf[32] = {0};
  format_term(999999999, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_TRUE(iter->is_end());
  int64_t tf = 0, dl = 0;
  ASSERT_EQ(OB_ITER_END, iter->get_curr_bm25_param(tf, dl));
  uint64_t min_id = 1, max_id = 1;
  double max_score = -1.0;
  ASSERT_EQ(OB_ITER_END, iter->get_block_max_tuple(min_id, max_id, max_score));

  iter->~ObInvSSTablePostingIter();
  allocator_.free(iter);
}

// get_block_max_tuple(min, max, score) reports max_id from curr_meta_ and min_id as
// max(frontier_min_id_, curr_meta_.min_id_); the current writer path carries no block-max
// frontier (so the score part is OB_NOT_SUPPORTED). Valid in both materialized and shallow states.
TEST_F(TestInvSSTablePostingIter, get_block_max_tuple_min_max_and_not_supported)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;

  // Materialized state: min/max cover the current doc; score is OB_NOT_SUPPORTED.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    OK(iter->next());
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    const uint64_t doc_id = id_datum->get_uint64();

    uint64_t min_id = 0, max_id = 0;
    double max_score = -1.0;
    ASSERT_EQ(OB_SUCCESS, iter->get_block_max_tuple(min_id, max_id, max_score));
    // min/max are still populated from curr_meta_ and must bracket the current doc.
    ASSERT_EQ(iter->curr_meta_.min_id_, min_id);
    ASSERT_EQ(iter->curr_meta_.max_id_, max_id);
    ASSERT_LE(min_id, doc_id);
    ASSERT_GE(max_id, doc_id);
    free_iter(iter);
  }

  // Shallow state: min_id is clamped to the scan frontier (frontier_min_id_ raised by
  // advance_shallow to target), not the raw block min; max_id still comes from curr_meta_.
  {
    ObInvSSTablePostingIter *iter = nullptr;
    open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
        query_key, query_flag, tablet_id, param, iter);
    OK(iter->next());
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    const uint64_t target = id_datum->get_uint64() + 5;
    uint64_t storage = 0;
    ObDatum datum;
    make_doc_id_datum(storage, target, datum);
    OK(iter->advance_shallow(datum, true /*inclusive*/));
    ASSERT_TRUE(iter->in_shallow_status());

    uint64_t min_id = 0, max_id = 0;
    double max_score = -1.0;
    ASSERT_EQ(OB_SUCCESS, iter->get_block_max_tuple(min_id, max_id, max_score));
    // frontier (raised to target) now exceeds curr_meta_.min_id_, so min_id reports the frontier.
    ASSERT_EQ(iter->frontier_min_id_, min_id);
    ASSERT_EQ(target, min_id);
    ASSERT_EQ(iter->curr_meta_.max_id_, max_id);
    ASSERT_LE(min_id, target);
    ASSERT_GE(max_id, target);
    free_iter(iter);
  }
}

// Range max starts at the current metadata cursor, covers later prefetched rows without
// moving that cursor, and only publishes a score after the complete range is proven.
TEST_F(TestInvSSTablePostingIter, get_range_max_score_complete_coverage)
{
  static constexpr int64_t RANGE_DOC_CNT = 200000;
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, RANGE_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable, false /*hetero*/, true /*ascending_block_score*/);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(inv_sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter);

  // 读取当前 row 的结构信息，选择一个同时拥有前驱和后继的中间 block。
  const char *agg_buf = nullptr;
  int64_t agg_size = 0;
  uint64_t row_max_doc_id = 0;
  OK(iter->scanner_.get_prefetched_meta_row(0, agg_buf, agg_size, row_max_doc_id));
  ObPostingMetaRowScanner meta_scanner;
  OK(meta_scanner.init(true /*has_max_score*/));
  OK(meta_scanner.open(agg_buf, agg_size));
  const int64_t current_row_block_count = meta_scanner.header_->logical_block_count_;
  ASSERT_GE(current_row_block_count, 3);
  const int64_t middle_idx = MIN(current_row_block_count / 2, current_row_block_count - 2);
  ObPostingBlockMeta middle_meta;
  ObPostingBlockMeta next_meta;
  OK(meta_scanner.get_block_meta_at(middle_idx, middle_meta));
  OK(meta_scanner.get_block_meta_at(middle_idx + 1, next_meta));

  // 将主 scanner 定位到当前 metadata row 的中间 block，作为 range 左侧锚点。
  uint64_t target_storage = 0;
  ObDatum target_datum;
  make_doc_id_datum(target_storage, middle_meta.min_id_, target_datum);
  OK(iter->advance_shallow(target_datum, true /*inclusive*/));
  ASSERT_EQ(middle_idx, iter->curr_meta_.idx_);
  ASSERT_EQ(middle_meta.min_id_, iter->curr_meta_.min_id_);
  ASSERT_EQ(middle_meta.max_id_, iter->curr_meta_.max_id_);

  const int64_t saved_read_idx = iter->scanner_.data_read_idx_;
  const int64_t saved_prefetch_idx = iter->scanner_.data_prefetch_idx_;
  const int64_t saved_meta_idx = iter->scanner_.meta_row_scanner_.cur_idx_;
  const uint64_t saved_frontier_min_id = iter->frontier_min_id_;
  EXPECT_EQ(1, iter->scanner_.get_prefetched_meta_row_count());
  const auto expected_block_score = [iter](const uint64_t block_max_id) {
    // 单 term 数据按固定 stride 反解 posting 位置，再还原该 block 使用的递增 tf。
    const uint64_t posting_idx = (block_max_id - ST_START) / ST_STRIDE;
    const uint64_t token_freq = 1 + posting_idx / DEFAULT_POSTING_BLOCK_CAPACITY;
    return iter->calc_bm25_score(token_freq, 100 /*doc_length*/);
  };

  // inclusive 区间在当前 block 的 max_id 精确闭合，只返回当前 block 上界。
  double range_max_score = -1.0;
  bool can_calc = false;
  OK(iter->get_range_max_score(
      middle_meta.min_id_, true /*start_inclusive*/, middle_meta.max_id_, range_max_score, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_DOUBLE_EQ(expected_block_score(middle_meta.max_id_), range_max_score);

  // exclusive 起点跳过当前 block，并在同 row 的后继 block 边界精确闭合。
  range_max_score = -1.0;
  can_calc = false;
  OK(iter->get_range_max_score(
      middle_meta.max_id_, false /*start_inclusive*/, next_meta.max_id_, range_max_score, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_DOUBLE_EQ(expected_block_score(next_meta.max_id_), range_max_score);

  // A far boundary grows metadata lookahead on demand. The fixed-depth window
  // cannot prove UINT64_MAX for this long term, so no partial score is published.
  range_max_score = -123.0;
  can_calc = true;
  OK(iter->get_range_max_score(middle_meta.min_id_,
      true /*start_inclusive*/, UINT64_MAX, range_max_score, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_DOUBLE_EQ(-123.0, range_max_score);

  // Use the now-pinned third metadata row to verify complete cross-row coverage.
  const int64_t prefetched_row_count = iter->scanner_.get_prefetched_meta_row_count();
  ASSERT_GE(prefetched_row_count, 3);
  ASSERT_FALSE(iter->scanner_.is_prefetched_to_end());
  const char *third_agg_buf = nullptr;
  int64_t third_agg_size = 0;
  uint64_t third_row_max_doc_id = 0;
  OK(iter->scanner_.get_prefetched_meta_row(
      2, third_agg_buf, third_agg_size, third_row_max_doc_id));
  meta_scanner.reuse();
  OK(meta_scanner.open(third_agg_buf, third_agg_size));
  ObPostingBlockMeta third_row_first_meta;
  OK(meta_scanner.get_block_meta_at(0, third_row_first_meta));
  range_max_score = -1.0;
  can_calc = false;
  OK(iter->get_range_max_score(middle_meta.min_id_, true /*start_inclusive*/,
      third_row_first_meta.max_id_, range_max_score, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_DOUBLE_EQ(expected_block_score(third_row_first_meta.max_id_), range_max_score);

  // 右端超过最后一条预取 row 且 term 尚未结束时，partial max 不得写给调用方。
  const char *last_agg_buf = nullptr;
  int64_t last_agg_size = 0;
  uint64_t last_row_max_doc_id = 0;
  OK(iter->scanner_.get_prefetched_meta_row(prefetched_row_count - 1,
      last_agg_buf, last_agg_size, last_row_max_doc_id));
  range_max_score = -123.0;
  can_calc = true;
  OK(iter->get_range_max_score(middle_meta.min_id_,
      true /*start_inclusive*/, last_row_max_doc_id + 1, range_max_score, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_DOUBLE_EQ(-123.0, range_max_score);

  // Range queries may grow the metadata prefetch cursor, but never move the
  // logical scanner or posting iterator cursor.
  EXPECT_EQ(saved_read_idx, iter->scanner_.data_read_idx_);
  EXPECT_GT(iter->scanner_.data_prefetch_idx_, saved_prefetch_idx);
  EXPECT_EQ(saved_meta_idx, iter->scanner_.meta_row_scanner_.cur_idx_);
  EXPECT_EQ(saved_frontier_min_id, iter->frontier_min_id_);
  EXPECT_EQ(middle_idx, iter->curr_meta_.idx_);

  // Missing block max conservatively rejects the fast path without publishing the
  // max accumulated before the rejection. Invalid metadata remains a real error.
  const int64_t current_ring_idx =
      iter->scanner_.data_read_idx_ % ObSSTablePostingScanner::DATA_RING_CAP;
  ObSSTablePostingScanner::DataPrefetchItem &current_item =
      iter->scanner_.data_ring_[current_ring_idx];
  const char *saved_current_agg = current_item.index_info_.agg_row_buf_;
  const int64_t saved_current_agg_size = current_item.index_info_.agg_buf_size_;
  const auto expect_conservative_without_cursor_move = [&](const uint64_t range_end) {
    const int64_t check_read_idx = iter->scanner_.data_read_idx_;
    const int64_t check_prefetch_idx = iter->scanner_.data_prefetch_idx_;
    const int64_t check_meta_idx = iter->scanner_.meta_row_scanner_.cur_idx_;
    const int64_t check_block_idx = iter->curr_meta_.idx_;
    const uint64_t check_frontier = iter->frontier_min_id_;
    double score = -123.0;
    bool calculable = true;
    OK(iter->get_range_max_score(
        middle_meta.min_id_, true /*start_inclusive*/, range_end, score, calculable));
    EXPECT_FALSE(calculable);
    EXPECT_DOUBLE_EQ(-123.0, score);
    EXPECT_EQ(check_read_idx, iter->scanner_.data_read_idx_);
    EXPECT_EQ(check_prefetch_idx, iter->scanner_.data_prefetch_idx_);
    EXPECT_EQ(check_meta_idx, iter->scanner_.meta_row_scanner_.cur_idx_);
    EXPECT_EQ(check_block_idx, iter->curr_meta_.idx_);
    EXPECT_EQ(check_frontier, iter->frontier_min_id_);
  };
  const auto expect_error_without_cursor_move = [&](const uint64_t range_end) {
    const int64_t check_read_idx = iter->scanner_.data_read_idx_;
    const int64_t check_prefetch_idx = iter->scanner_.data_prefetch_idx_;
    const int64_t check_meta_idx = iter->scanner_.meta_row_scanner_.cur_idx_;
    const int64_t check_block_idx = iter->curr_meta_.idx_;
    const uint64_t check_frontier = iter->frontier_min_id_;
    double score = -123.0;
    bool calculable = true;
    ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_range_max_score(
        middle_meta.min_id_, true /*start_inclusive*/, range_end, score, calculable));
    EXPECT_FALSE(calculable);
    EXPECT_DOUBLE_EQ(-123.0, score);
    EXPECT_EQ(check_read_idx, iter->scanner_.data_read_idx_);
    EXPECT_EQ(check_prefetch_idx, iter->scanner_.data_prefetch_idx_);
    EXPECT_EQ(check_meta_idx, iter->scanner_.meta_row_scanner_.cur_idx_);
    EXPECT_EQ(check_block_idx, iter->curr_meta_.idx_);
    EXPECT_EQ(check_frontier, iter->frontier_min_id_);
  };

  std::vector<char> missing_max_agg(agg_buf, agg_buf + agg_size);
  ObInvIdxAggDataEntry *missing_max_entries = reinterpret_cast<ObInvIdxAggDataEntry *>(
      missing_max_agg.data() + sizeof(ObInvIdxAggDataHeader));
  missing_max_entries[middle_idx + 1].frontier_count_ = 0;
  current_item.index_info_.agg_row_buf_ = missing_max_agg.data();
  current_item.index_info_.agg_buf_size_ = missing_max_agg.size();
  expect_conservative_without_cursor_move(next_meta.max_id_);

  // Corrupting the real serialized header exercises the parser error path, which must
  // propagate instead of being downgraded to a conservative success.
  std::vector<char> invalid_header_agg(agg_buf, agg_buf + agg_size);
  reinterpret_cast<ObInvIdxAggDataHeader *>(invalid_header_agg.data())->version_ += 1;
  current_item.index_info_.agg_row_buf_ = invalid_header_agg.data();
  current_item.index_info_.agg_buf_size_ = invalid_header_agg.size();
  const int64_t parse_read_idx = iter->scanner_.data_read_idx_;
  const int64_t parse_prefetch_idx = iter->scanner_.data_prefetch_idx_;
  const int64_t parse_meta_idx = iter->scanner_.meta_row_scanner_.cur_idx_;
  const int64_t parse_block_idx = iter->curr_meta_.idx_;
  const uint64_t parse_frontier = iter->frontier_min_id_;
  range_max_score = -123.0;
  can_calc = true;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter->get_range_max_score(middle_meta.min_id_,
      true /*start_inclusive*/, middle_meta.max_id_, range_max_score, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_DOUBLE_EQ(-123.0, range_max_score);
  EXPECT_EQ(parse_read_idx, iter->scanner_.data_read_idx_);
  EXPECT_EQ(parse_prefetch_idx, iter->scanner_.data_prefetch_idx_);
  EXPECT_EQ(parse_meta_idx, iter->scanner_.meta_row_scanner_.cur_idx_);
  EXPECT_EQ(parse_block_idx, iter->curr_meta_.idx_);
  EXPECT_EQ(parse_frontier, iter->frontier_min_id_);
  current_item.index_info_.agg_row_buf_ = saved_current_agg;
  current_item.index_info_.agg_buf_size_ = saved_current_agg_size;

  // Make the first block in the next metadata row overlap the previous row's last
  // block. The parsed metadata remains structurally valid, but cross-row order is not strict.
  const char *second_agg_buf = nullptr;
  int64_t second_agg_size = 0;
  uint64_t second_row_max_doc_id = 0;
  OK(iter->scanner_.get_prefetched_meta_row(
      1, second_agg_buf, second_agg_size, second_row_max_doc_id));
  meta_scanner.reuse();
  OK(meta_scanner.open(agg_buf, agg_size));
  ObPostingBlockMeta first_row_last_meta;
  OK(meta_scanner.get_block_meta_at(current_row_block_count - 1, first_row_last_meta));
  std::vector<char> non_strict_agg(second_agg_buf, second_agg_buf + second_agg_size);
  ObInvIdxAggDataEntry *non_strict_entries = reinterpret_cast<ObInvIdxAggDataEntry *>(
      non_strict_agg.data() + sizeof(ObInvIdxAggDataHeader));
  non_strict_entries[0].min_doc_id_ = first_row_last_meta.max_id_;
  const int64_t second_ring_idx =
      (iter->scanner_.data_read_idx_ + 1) % ObSSTablePostingScanner::DATA_RING_CAP;
  ObSSTablePostingScanner::DataPrefetchItem &second_item =
      iter->scanner_.data_ring_[second_ring_idx];
  const char *saved_second_agg = second_item.index_info_.agg_row_buf_;
  const int64_t saved_second_agg_size = second_item.index_info_.agg_buf_size_;
  second_item.index_info_.agg_row_buf_ = non_strict_agg.data();
  second_item.index_info_.agg_buf_size_ = non_strict_agg.size();
  expect_error_without_cursor_move(second_row_max_doc_id);
  second_item.index_info_.agg_row_buf_ = saved_second_agg;
  second_item.index_info_.agg_buf_size_ = saved_second_agg_size;

  // 定位到 term 尾部后，boundary 超过最后 doc 仍可由明确的 term 末尾闭合。
  const uint64_t near_term_end = expect_doc(RANGE_DOC_CNT - 100);
  make_doc_id_datum(target_storage, near_term_end, target_datum);
  OK(iter->advance_shallow(target_datum, true /*inclusive*/));
  const int64_t terminal_read_idx = iter->scanner_.data_read_idx_;
  const int64_t terminal_prefetch_idx = iter->scanner_.data_prefetch_idx_;
  const int64_t terminal_meta_idx = iter->scanner_.meta_row_scanner_.cur_idx_;
  const int64_t terminal_block_idx = iter->curr_meta_.idx_;
  const uint64_t term_max_doc_id = expect_doc(RANGE_DOC_CNT - 1);
  range_max_score = -1.0;
  can_calc = false;
  OK(iter->get_range_max_score(near_term_end, true /*start_inclusive*/,
      term_max_doc_id + 100, range_max_score, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_DOUBLE_EQ(expected_block_score(term_max_doc_id), range_max_score);
  EXPECT_TRUE(iter->scanner_.is_prefetched_to_end());
  EXPECT_EQ(terminal_read_idx, iter->scanner_.data_read_idx_);
  EXPECT_GE(iter->scanner_.data_prefetch_idx_, terminal_prefetch_idx);
  EXPECT_EQ(terminal_meta_idx, iter->scanner_.meta_row_scanner_.cur_idx_);
  EXPECT_EQ(terminal_block_idx, iter->curr_meta_.idx_);
  free_iter(iter);
}

// For a doc-ID-only posting list without BM25 relevance parameters, get_range_max_score() must
// return success with can_calc=false while preserving max_score, every scanner cursor, the current
// block, and the frontier.
TEST_F(TestInvSSTablePostingIter, get_range_max_score_without_relevance_is_conservative)
{
  const int64_t doc_cnt = 512;
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(doc_cnt, ST_START, ST_STRIDE, ST_TERM_SEQ, inv_sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);
  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_FALSE(iter->has_relevance_params_);

  const int64_t saved_read_idx = iter->scanner_.data_read_idx_;
  const int64_t saved_prefetch_idx = iter->scanner_.data_prefetch_idx_;
  const int64_t saved_meta_idx = iter->scanner_.meta_row_scanner_.cur_idx_;
  const int64_t saved_block_idx = iter->curr_meta_.idx_;
  const uint64_t saved_frontier = iter->frontier_min_id_;
  double max_score = -123.0;
  bool can_calc = true;
  OK(iter->get_range_max_score(
      ST_START, true /*start_inclusive*/, expect_doc(255), max_score, can_calc));
  EXPECT_FALSE(can_calc);
  EXPECT_DOUBLE_EQ(-123.0, max_score);
  EXPECT_EQ(saved_read_idx, iter->scanner_.data_read_idx_);
  EXPECT_EQ(saved_prefetch_idx, iter->scanner_.data_prefetch_idx_);
  EXPECT_EQ(saved_meta_idx, iter->scanner_.meta_row_scanner_.cur_idx_);
  EXPECT_EQ(saved_block_idx, iter->curr_meta_.idx_);
  EXPECT_EQ(saved_frontier, iter->frontier_min_id_);
  free_iter(iter);
}

// =============================================================================
// Inv sstable row-count estimation tests (ObIndexBlockScanEstimator::estimate_inv_sstable_row_count,
// index-only traversal, no posting data IO).
// =============================================================================

namespace
{
// Build a [<term>, doc_lo] .. [<term>, doc_hi] inclusive 2-column datum range. The 4 backing
// datums are owned by the caller (kept alive for the estimate call).
void build_term_range(
    const char *term_str,
    const int32_t term_len,
    const uint64_t doc_lo,
    const uint64_t doc_hi,
    uint64_t &lo_store,
    uint64_t &hi_store,
    ObStorageDatum (&start_datums)[2],
    ObStorageDatum (&end_datums)[2],
    ObDatumRange &range)
{
  start_datums[0].set_string(term_str, term_len);
  lo_store = doc_lo;
  start_datums[1].set_uint(doc_lo);
  end_datums[0].set_string(term_str, term_len);
  hi_store = doc_hi;
  end_datums[1].set_uint(doc_hi);
  range.start_key_.assign(start_datums, 2);
  range.end_key_.assign(end_datums, 2);
  range.set_left_closed();
  range.set_right_closed();
}

// Build a term-only [<term>, MIN] .. [<term>, MAX] inclusive 2-column datum range: the doc_id
// tail column carries the MIN/MAX sentinels that ObIndexBlockScanEstimator's term-only-range
// guard requires (a real FTS term scan passes exactly this shape). The 2 backing datums are
// owned by the caller (kept alive for the estimate call).
void build_term_only_range(
    const char *term_str,
    const int32_t term_len,
    ObStorageDatum (&start_datums)[2],
    ObStorageDatum (&end_datums)[2],
    ObDatumRange &range)
{
  start_datums[0].set_string(term_str, term_len);
  start_datums[1].set_min();
  end_datums[0].set_string(term_str, term_len);
  end_datums[1].set_max();
  range.start_key_.assign(start_datums, 2);
  range.end_key_.assign(end_datums, 2);
  range.set_left_closed();
  range.set_right_closed();
}
} // namespace

// estimate_row_count over the full term range equals the fed doc count (whole posting blocks
// are summed via the index tree; no data micro block is opened).
TEST_F(TestInvSSTablePostingIter, estimate_row_count_full_term)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));

  ObStorageDatum start_datums[2];
  ObStorageDatum end_datums[2];
  ObDatumRange range;
  build_term_only_range(term_buf, static_cast<int32_t>(strlen(term_buf)),
      start_datums, end_datums, range);

  int64_t row_count = -1;
  OK(ObIndexBlockScanEstimator::estimate_inv_sstable_row_count(
      inv_sstable, read_info, range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(ST_DOC_CNT, row_count) << "full-term estimate must equal the fed doc count";
}

TEST_F(TestInvSSTablePostingIter, estimate_row_count_rejects_doc_sub_range)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));

  const uint64_t doc_lo = expect_doc(ST_DOC_CNT / 4);
  const uint64_t doc_hi = expect_doc(ST_DOC_CNT / 2);

  uint64_t lo_store = 0;
  uint64_t hi_store = 0;
  ObStorageDatum start_datums[2];
  ObStorageDatum end_datums[2];
  ObDatumRange range;
  build_term_range(term_buf, static_cast<int32_t>(strlen(term_buf)), doc_lo, doc_hi,
      lo_store, hi_store, start_datums, end_datums, range);

  int64_t row_count = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObIndexBlockScanEstimator::estimate_inv_sstable_row_count(
      inv_sstable, read_info, range, nullptr /*tablet*/, row_count));
}

// estimate_row_count for an absent term is 0 (no posting blocks intersect the range).
TEST_F(TestInvSSTablePostingIter, estimate_row_count_absent_term)
{
  ObInvSSTable inv_sstable;
  build_single_term_sstable(true /*has_pos_list*/, ST_DOC_CNT, ST_START, ST_STRIDE,
      ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  format_term(999999999, term_buf, sizeof(term_buf));  // absent term

  ObStorageDatum start_datums[2];
  ObStorageDatum end_datums[2];
  ObDatumRange range;
  build_term_only_range(term_buf, static_cast<int32_t>(strlen(term_buf)),
      start_datums, end_datums, range);

  int64_t row_count = -1;
  OK(ObIndexBlockScanEstimator::estimate_inv_sstable_row_count(
      inv_sstable, read_info, range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(0, row_count);
}

// =============================================================================
// Search-index / no-BM25 tests
//
// These tests exercise the read path with an sstable whose posting blocks are
// doc_id-only (has_relevance_params_ = false). They cover the BM25-decoupling
// adaptation points:
//   - param.is_valid() passes without avg_doc_length_ > 0 (point 1)
//   - open() skips init_precomputed_params (point 2)
//   - has_max_score derived from inv_meta, frontier not parsed (point 3)
//   - materialize_row skips tf/dl decoding (point 5)
//   - get_curr_bm25_param returns OB_NOT_SUPPORTED (point 6)
//   - drain_until projects doc_ids with nullptr token_freqs/doc_lengths (point 7)
// =============================================================================

// Iterate all docs via next(); verify doc_ids are correct and BM25 interfaces
// return OB_NOT_SUPPORTED.
TEST_F(TestInvSSTablePostingIter, scan_posting_no_bm25_next_and_bm25_unsupported)
{
  const int64_t doc_cnt = 512;
  const uint64_t start = 1;
  const uint64_t stride = 2;
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(doc_cnt, start, stride, ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_FALSE(iter->has_relevance_params_);

  int64_t count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter->next())) {
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_NE(nullptr, id_datum);
    const uint64_t doc_id = id_datum->get_uint64();
    ASSERT_EQ(start + static_cast<uint64_t>(count) * stride, doc_id);

    int64_t tf = 0;
    int64_t dl = 0;
    ASSERT_EQ(OB_NOT_SUPPORTED, iter->get_curr_bm25_param(tf, dl));

    uint64_t min_id = 0, max_id = 0;
    double max_score = 0.0;
    ASSERT_EQ(OB_NOT_SUPPORTED, iter->get_block_max_tuple(min_id, max_id, max_score));
    ASSERT_LE(min_id, max_id);
    ++count;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(doc_cnt, count);
  ASSERT_TRUE(iter->is_end());
  ASSERT_FALSE(iter->curr_value_block_loaded_);

  free_iter(iter);
}

// Regression for z-6 §6.4: Search no-BM25 scan with default need_posting_value_=false must not
// touch posting value micro blocks while iterating doc_ids only.
TEST_F(TestInvSSTablePostingIter, scan_posting_no_bm25_next_zero_value_io)
{
  const int64_t doc_cnt = 64;
  const uint64_t start = 1;
  const uint64_t stride = 2;
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(
      doc_cnt, start, stride, ST_TERM_SEQ, inv_sstable, true /*has_nested_key*/);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);
  ASSERT_FALSE(param.need_posting_value_);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);

  int64_t count = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter->next())) {
    ASSERT_FALSE(iter->curr_value_block_loaded_);
    const ObDatum *id_datum = nullptr;
    OK(iter->get_cur_id(id_datum));
    ASSERT_EQ(start + static_cast<uint64_t>(count) * stride, id_datum->get_uint64());
    ++count;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(doc_cnt, count);
  ASSERT_FALSE(iter->curr_value_block_loaded_);
  free_iter(iter);
}

// drain_until() on a no-BM25 sstable: doc_ids projected, token_freqs / doc_lengths
// are nullptr.
TEST_F(TestInvSSTablePostingIter, scan_posting_no_bm25_drain_until_null_ptrs)
{
  const int64_t doc_cnt = 512;
  const uint64_t start = 1;
  const uint64_t stride = 2;
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(doc_cnt, start, stride, ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);

  const uint64_t max_doc = start + static_cast<uint64_t>(doc_cnt - 1) * stride;
  std::vector<uint64_t> ids;
  bool done = false;
  while (!done) {
    const uint64_t *pid = nullptr;
    const uint64_t *ptf = nullptr;
    const uint64_t *pdl = nullptr;
    int64_t n = 0;
    OK(iter->drain_until(max_doc + 10, DEFAULT_POSTING_BLOCK_CAPACITY, pid, ptf, pdl, n));
    ASSERT_GE(n, 0);
    if (n > 0) {
      ASSERT_NE(nullptr, pid);
      ASSERT_EQ(nullptr, ptf);
      ASSERT_EQ(nullptr, pdl);
      for (int64_t k = 0; k < n; ++k) {
        ids.push_back(pid[k]);
      }
    } else {
      done = true;
    }
  }
  ASSERT_TRUE(iter->is_end());
  ASSERT_EQ(static_cast<size_t>(doc_cnt), ids.size());
  for (int64_t i = 0; i < doc_cnt; ++i) {
    ASSERT_EQ(start + static_cast<uint64_t>(i) * stride, ids[i]) << "at " << i;
  }

  free_iter(iter);
}

// advance_to() on a no-BM25 sstable lands on the first doc_id >= target.
TEST_F(TestInvSSTablePostingIter, scan_posting_no_bm25_advance_to)
{
  const int64_t doc_cnt = 512;
  const uint64_t start = 1;
  const uint64_t stride = 2;
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(doc_cnt, start, stride, ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(ST_TERM_SEQ, term_buf, sizeof(term_buf));
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);

  const int64_t target_pos = 100;
  const uint64_t target_doc = start + static_cast<uint64_t>(target_pos) * stride;
  uint64_t target_storage = 0;
  ObDatum target_datum;
  make_doc_id_datum(target_storage, target_doc, target_datum);

  OK(iter->advance_to(target_datum));
  const ObDatum *id_datum = nullptr;
  OK(iter->get_cur_id(id_datum));
  ASSERT_EQ(target_doc, id_datum->get_uint64());

  free_iter(iter);
}

// An absent term on a no-BM25 sstable leaves the iterator valid with is_end().
TEST_F(TestInvSSTablePostingIter, scan_posting_no_bm25_absent_term)
{
  const int64_t doc_cnt = 512;
  const uint64_t start = 1;
  const uint64_t stride = 2;
  ObInvSSTable inv_sstable;
  build_single_term_sstable_no_bm25(doc_cnt, start, stride, ST_TERM_SEQ, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  char term_buf[32] = {0};
  format_term(999999999, term_buf, sizeof(term_buf));  // absent
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  build_term_rowkey(term_buf, static_cast<int32_t>(strlen(term_buf)), term_datum, query_key);
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  build_scan_param_no_bm25(query_key, tablet_id, query_flag, allocator_, param);

  ObInvSSTablePostingIter *iter = nullptr;
  OK(inv_sstable.scan_posting(param, read_info, iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_TRUE(iter->is_end());
  ASSERT_EQ(OB_ITER_END, iter->next());

  free_iter(iter);
}

TEST_F(TestInvSSTablePostingIter, docid_range_next_crosses_blocks_and_keeps_closed_bounds)
{
  ObInvSSTable sstable;
  build_single_term_sstable(true, 4096, ST_START, ST_STRIDE, ST_TERM_SEQ, sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter, false, 1024, 2061);
  uint64_t min_id = 0;
  uint64_t max_id = 0;
  double max_score = 0;
  OK(iter->get_block_max_tuple(min_id, max_id, max_score));
  EXPECT_GE(min_id, 1024u);
  EXPECT_LE(max_id, 2061u);
  for (uint64_t expected = 1025; expected <= 2061; expected += 2) {
    OK(iter->next());
    const ObDatum *id = nullptr;
    OK(iter->get_cur_id(id));
    ASSERT_EQ(expected, id->get_uint64());
  }
  EXPECT_EQ(OB_ITER_END, iter->next());
  EXPECT_TRUE(iter->is_end());
  free_iter(iter);
}

TEST_F(TestInvSSTablePostingIter, docid_range_drain_preserves_first_row_and_lookahead)
{
  ObInvSSTable sstable;
  build_single_term_sstable(true, 4096, ST_START, ST_STRIDE, ST_TERM_SEQ, sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter, false, 1024, 2061);
  const uint64_t *ids = nullptr;
  const uint64_t *freqs = nullptr;
  const uint64_t *lengths = nullptr;
  int64_t count = 0;
  OK(iter->drain_until(1100, 128, ids, freqs, lengths, count));
  ASSERT_EQ(38, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(1025 + 2 * i, ids[i]);
  }
  OK(iter->drain_until(1100, 128, ids, freqs, lengths, count));
  EXPECT_EQ(0, count);
  EXPECT_FALSE(iter->is_end());
  const ObDatum *lookahead = nullptr;
  OK(iter->get_cur_id(lookahead));
  EXPECT_EQ(1101u, lookahead->get_uint64());
  uint64_t expected = 1101;
  bool include_current = true;
  while (!iter->is_end()) {
    if (include_current) {
      OK(iter->drain_until_from_current(UINT64_MAX, 17, ids, freqs, lengths, count));
      include_current = false;
    } else {
      OK(iter->drain_until(UINT64_MAX, 17, ids, freqs, lengths, count));
    }
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_LE(ids[i], 2061u);
      EXPECT_EQ(expected, ids[i]);
      expected += 2;
    }
  }
  EXPECT_EQ(2063u, expected);
  free_iter(iter);
}

TEST_F(TestInvSSTablePostingIter, docid_range_seek_shallow_empty_and_reset)
{
  ObInvSSTable sstable;
  build_single_term_sstable(true, 4096, ST_START, ST_STRIDE, ST_TERM_SEQ, sstable);
  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char term_buf[32] = {0};
  ObStorageDatum term_datum;
  ObDatumRowkey query_key;
  ObQueryFlag query_flag;
  ObTabletID tablet_id(TABLET_ID);
  ObInvPostingScanParam param;
  ObInvSSTablePostingIter *iter = nullptr;
  open_single_term_iter(sstable, read_info, term_buf, sizeof(term_buf), term_datum,
      query_key, query_flag, tablet_id, param, iter, false, 1025, 1535);
  uint64_t target_storage = 0;
  ObDatum target;
  make_doc_id_datum(target_storage, 1000, target);
  OK(iter->advance_shallow(target, false));
  OK(iter->advance_to_id(target));
  const ObDatum *id = nullptr;
  OK(iter->get_cur_id(id));
  EXPECT_EQ(1025u, id->get_uint64());
  int64_t frequency = 0;
  int64_t length = 0;
  OK(iter->load_curr_bm25_param(frequency, length));
  EXPECT_EQ(1, frequency);
  EXPECT_EQ(100, length);
  make_doc_id_datum(target_storage, 1534, target);
  OK(iter->advance_to(target));
  OK(iter->get_cur_id(id));
  EXPECT_EQ(1535u, id->get_uint64());
  EXPECT_EQ(OB_ITER_END, iter->next());

  iter->reset();
  param.min_doc_id_ = 1024;
  param.max_doc_id_ = 1024;
  OK(iter->open(param, read_info, sstable));
  EXPECT_EQ(OB_ITER_END, iter->next());
  iter->reset();
  param.min_doc_id_ = 0;
  param.max_doc_id_ = UINT64_MAX;
  OK(iter->open(param, read_info, sstable));
  OK(iter->next());
  OK(iter->get_cur_id(id));
  EXPECT_EQ(1u, id->get_uint64());
  free_iter(iter);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_sstable_posting_iter.log*");
  OB_LOGGER.set_file_name("test_inv_sstable_posting_iter.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  const int ret = RUN_ALL_TESTS();
  std::_Exit(ret);
}
