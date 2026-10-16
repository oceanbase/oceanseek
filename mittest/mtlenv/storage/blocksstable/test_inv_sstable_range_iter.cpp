/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <vector>
#include <utility>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "test_inv_idx_sstable_prepare.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_range_iter.h"

namespace oceanbase
{
namespace unittest
{

namespace
{
// Format a term string: "term" + 10-digit zero-padded seq. Zero padding keeps the
// byte order monotonic in the term sequence, matching ObInvIdxRowFeeder.
void format_term(const int64_t seq, char *buf, const int64_t buf_len)
{
  snprintf(buf, buf_len, "term%010ld", static_cast<long>(seq));
}

void format_nested_key(const int64_t seq, char *buf, const int64_t buf_len)
{
  snprintf(buf, buf_len, "nk%010ld", static_cast<long>(seq));
}

// Build the rowkey read info from the schema built by the prepare harness.
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

// Build a valid ObInvRangeScanParam pointing at the supplied (caller-owned) range.
// The caller MUST keep scan_range (and its term key buffers) alive for the whole scan.
void build_range_scan_param(
    const ObDatumRange &scan_range,
    const ObTabletID &tablet_id,
    const ObQueryFlag &query_flag,
    common::ObIAllocator &allocator,
    ObInvRangeScanParam &param)
{
  param.reset();
  param.scan_range_ = &scan_range;
  param.tablet_id_ = tablet_id;
  param.query_flag_ = query_flag;
  param.allocator_ = &allocator;
}
} // namespace

// =============================================================================
// TestInvSSTableRangeIter — range scan over a search-index inverted sstable.
//
// All datasets are SEARCH indexes without BM25 relevance params: doc-id-only
// posting blocks, index_type_ == INV_IDX_TYPE_SEARCH, has_relevance_params_ ==
// false. The range iterator iterates (term, doc_id) pairs in dictionary order:
// terms ascending, doc_ids ascending within a term (doc_id may drop at a term
// boundary).
// =============================================================================
class TestInvSSTableRangeIter : public TestInvIdxSSTablePrepare
{
public:
  // Ground-truth (term_seq, doc_id) pairs of the last-built dataset, in the exact
  // dictionary order the writer fed them. Range assertions slice this oracle.
  std::vector<std::pair<int64_t, uint64_t>> oracle_;

  // Prepare a SEARCH-index schema + row desc: no relevance params, no pos_list,
  // no nested_key; doc-id-only posting blocks. Mirrors the no-bm25 setup used by
  // the posting-iter tests.
  void prepare_search_index_desc(const bool has_nested_key = false)
  {
    prepare_inv_idx_schema(table_schema_, has_nested_key /*reuse pos_list column slot*/);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    apply_search_index_row_desc(inv_idx_row_desc_, has_nested_key);
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    ASSERT_EQ(has_nested_key, inv_idx_row_desc_.has_posting_value());
    prepare_desc();
  }

  // Finalize a search-index ObInvSSTable from the fed rows: close the writer /
  // index builder, stamp SEARCH inv-meta (no relevance params), and build the
  // sstable. min/max doc_id come from the observed feed.
  void finalize_search_sstable(
      ObInvIdxMacroBlockWriter &writer,
      const int64_t row_cnt,
      const uint64_t min_doc_id,
      const uint64_t max_doc_id,
      ObInvSSTable &inv_sstable,
      const bool has_nested_key = false)
  {
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
    ASSERT_EQ(row_cnt, res.row_count_);

    fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, res.inv_meta_);
    res.inv_meta_.index_type_ = INV_IDX_TYPE_SEARCH;
    res.inv_meta_.has_nested_key_ = has_nested_key;
    res.inv_meta_.min_doc_id_ = min_doc_id;
    res.inv_meta_.max_doc_id_ = max_doc_id;
    res.inv_meta_.total_doc_length_ = 0;
    ASSERT_TRUE(res.inv_meta_.is_valid());
    ASSERT_FALSE(res.inv_meta_.has_relevance_params_);
    ASSERT_FALSE(res.inv_meta_.is_fts_index());
    ASSERT_EQ(has_nested_key, res.inv_meta_.has_nested_key_);

    finalize_inv_sstable_from_res(res, inv_sstable);
    ASSERT_TRUE(inv_sstable.is_inv_sstable());
    ASSERT_TRUE(inv_sstable.is_valid());
    ASSERT_FALSE(inv_sstable.get_inv_meta().has_relevance_params_);
    ASSERT_FALSE(inv_sstable.get_inv_meta().is_fts_index());
    ASSERT_EQ(has_nested_key, inv_sstable.get_inv_meta().has_nested_key_);
    ASSERT_EQ(min_doc_id, inv_sstable.get_inv_meta().min_doc_id_);
    ASSERT_EQ(max_doc_id, inv_sstable.get_inv_meta().max_doc_id_);
  }

  // Build a single-term SEARCH sstable: term "term%010ld" % term_seq with posting
  // list doc_id(i) = start_doc_id + i*stride, i in [0, doc_cnt). Records oracle_.
  void build_single_term_search_sstable(
      const int64_t doc_cnt,
      const uint64_t start_doc_id,
      const uint64_t stride,
      const int64_t term_seq,
      ObInvSSTable &inv_sstable,
      const bool has_nested_key = false)
  {
    ASSERT_GT(doc_cnt, 0);
    ASSERT_GT(stride, 0u);
    ASSERT_GT(start_doc_id, 0u);  // min_doc_id must be > 0 for valid inv meta
    oracle_.clear();

    prepare_search_index_desc(has_nested_key);

    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());

    char term_buf[32] = {0};
    format_term(term_seq, term_buf, sizeof(term_buf));
    ObString term_str(static_cast<int32_t>(strlen(term_buf)), term_buf);
    ObString empty_pos_list;
    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    uint64_t min_doc_id = 0;
    uint64_t max_doc_id = 0;
    for (int64_t i = 0; i < doc_cnt; ++i) {
      const uint64_t doc_id = start_doc_id + static_cast<uint64_t>(i) * stride;
      char nk_buf[32] = {0};
      ObString nested_key;
      if (has_nested_key) {
        format_nested_key(i, nk_buf, sizeof(nk_buf));
        nested_key = ObString(static_cast<int32_t>(strlen(nk_buf)), nk_buf);
      }
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
          0 /*token_freq ignored*/, 0 /*doc_length ignored*/, empty_pos_list, nested_key));
      OK(writer.append_row(row));
      if (0 == i) {
        min_doc_id = doc_id;
      }
      max_doc_id = doc_id;
      oracle_.push_back(std::make_pair(term_seq, doc_id));
    }
    finalize_search_sstable(writer, doc_cnt, min_doc_id, max_doc_id, inv_sstable,
        has_nested_key);
    STORAGE_LOG(INFO, "built single term search sstable", K(doc_cnt), K(term_seq),
        K(min_doc_id), K(max_doc_id));
  }

  // Build a multi-term SEARCH sstable. Terms are term_seq in [1, term_cnt]; each
  // term carries docs_per_term doc_ids. Within a term doc_ids strictly increase.
  //
  // doc_id_drop controls cross-term doc_id ordering:
  //   false: doc_id keeps rising across terms on one global counter (no drop) —
  //          the simplest valid layout.
  //   true:  each term's doc_ids restart at a base BELOW the previous term's max,
  //          so doc_id drops at every term boundary. Only (term, doc_id) ordering
  //          is required by the format; global doc_id monotonicity is not. Bases
  //          alternate on two disjoint bands so consecutive terms overlap in
  //          doc_id space yet each term stays strictly increasing internally.
  // Rows are fed in strict (term, doc_id) dictionary order. Records oracle_.
  void build_multi_term_search_sstable(
      const int64_t term_cnt,
      const int64_t docs_per_term,
      ObInvSSTable &inv_sstable,
      const bool doc_id_drop = false,
      const bool has_nested_key = false)
  {
    ASSERT_GT(term_cnt, 0);
    ASSERT_GT(docs_per_term, 0);
    oracle_.clear();

    prepare_search_index_desc(has_nested_key);

    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());

    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    ObString empty_pos_list;
    uint64_t doc_id = 0;
    uint64_t min_doc_id = UINT64_MAX;
    uint64_t max_doc_id = 0;
    int64_t total_rows = 0;
    // Two disjoint doc_id bands used by odd/even terms in the drop layout. band_hi
    // sits above band_lo, so an odd term (band_lo) followed by an even term
    // (band_hi) rises, and the following odd term drops back into band_lo — a drop
    // at that boundary. Both bases are > 0 so min_doc_id stays valid.
    const uint64_t band_lo_base = 1;
    const uint64_t band_hi_base = band_lo_base + static_cast<uint64_t>(docs_per_term) + 1;
    for (int64_t t = 1; t <= term_cnt; ++t) {
      char term_buf[32] = {0};
      format_term(t, term_buf, sizeof(term_buf));
      ObString term_str(static_cast<int32_t>(strlen(term_buf)), term_buf);
      // Per-term starting doc_id.
      uint64_t term_doc_id = 0;
      if (!doc_id_drop) {
        term_doc_id = doc_id;  // continue the global counter
      } else {
        term_doc_id = (0 == (t % 2)) ? band_hi_base - 1 : band_lo_base - 1;
      }
      for (int64_t j = 0; j < docs_per_term; ++j) {
        ++term_doc_id;
        doc_id = term_doc_id;
        char nk_buf[32] = {0};
        ObString nested_key;
        if (has_nested_key) {
          format_nested_key(total_rows, nk_buf, sizeof(nk_buf));
          nested_key = ObString(static_cast<int32_t>(strlen(nk_buf)), nk_buf);
        }
        OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
            0 /*token_freq ignored*/, 0 /*doc_length ignored*/, empty_pos_list, nested_key));
        OK(writer.append_row(row));
        if (doc_id < min_doc_id) {
          min_doc_id = doc_id;
        }
        if (doc_id > max_doc_id) {
          max_doc_id = doc_id;
        }
        oracle_.push_back(std::make_pair(t, doc_id));
        ++total_rows;
      }
    }
    finalize_search_sstable(writer, total_rows, min_doc_id, max_doc_id, inv_sstable,
        has_nested_key);
    STORAGE_LOG(INFO, "built multi term search sstable", K(term_cnt), K(docs_per_term),
        K(doc_id_drop), K(total_rows), K(min_doc_id), K(max_doc_id));
  }

  // Build a single-term SEARCH sstable with forced micro/macro block boundaries.
  // Every `docs_per_logical_block` rows are flushed as one logical block then
  // immediately flushed as their own micro block pair; every
  // `micro_block_pairs_per_macro` pairs the macro block is flushed. doc_cnt must
  // be a multiple of docs_per_logical_block, which must not exceed
  // DEFAULT_POSTING_BLOCK_CAPACITY (256). Records oracle_. The test exposes private
  // members, so ObInvIdxMacroBlockWriter's private flush helpers are reachable.
  void build_single_term_forced_boundaries_sstable(
      const int64_t doc_cnt,
      const uint64_t start_doc_id,
      const uint64_t stride,
      const int64_t term_seq,
      ObInvSSTable &inv_sstable,
      const bool has_nested_key,
      const int64_t docs_per_logical_block,
      const int64_t micro_block_pairs_per_macro)
  {
    const int64_t POSTING_BLOCK_CAPACITY = 256;
    ASSERT_GT(doc_cnt, 0);
    ASSERT_GT(stride, 0u);
    ASSERT_GT(start_doc_id, 0u);
    ASSERT_GT(docs_per_logical_block, 0);
    ASSERT_LE(docs_per_logical_block, POSTING_BLOCK_CAPACITY);
    ASSERT_GT(micro_block_pairs_per_macro, 0);
    ASSERT_EQ(0, doc_cnt % docs_per_logical_block)
        << "doc_cnt must be a multiple of docs_per_logical_block for clean boundaries";
    oracle_.clear();

    prepare_search_index_desc(has_nested_key);

    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());

    char term_buf[32] = {0};
    format_term(term_seq, term_buf, sizeof(term_buf));
    ObString term_str(static_cast<int32_t>(strlen(term_buf)), term_buf);
    ObString empty_pos_list;
    ObDatumRow row;
    OK(row.init(mv_col_cnt_));
    uint64_t min_doc_id = 0;
    uint64_t max_doc_id = 0;
    int64_t micro_block_pair_cnt = 0;
    for (int64_t i = 0; i < doc_cnt; ++i) {
      const uint64_t doc_id = start_doc_id + static_cast<uint64_t>(i) * stride;
      char nk_buf[32] = {0};
      ObString nested_key;
      if (has_nested_key) {
        format_nested_key(i, nk_buf, sizeof(nk_buf));
        nested_key = ObString(static_cast<int32_t>(strlen(nk_buf)), nk_buf);
      }
      OK(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str, doc_id,
          0 /*token_freq ignored*/, 0 /*doc_length ignored*/, empty_pos_list, nested_key));
      OK(writer.append_row(row));
      if (0 == i) { min_doc_id = doc_id; }
      max_doc_id = doc_id;
      oracle_.push_back(std::make_pair(term_seq, doc_id));

      if (0 == ((i + 1) % docs_per_logical_block)) {
        OK(writer.flush_logical_block());
        OK(writer.flush_micro_block_pair());
        ++micro_block_pair_cnt;
        if (0 == (micro_block_pair_cnt % micro_block_pairs_per_macro)) {
          OK(writer.flush_macro_block(false /*is_close_flush*/));
        }
      }
    }
    finalize_search_sstable(writer, doc_cnt, min_doc_id, max_doc_id, inv_sstable,
        has_nested_key);
    STORAGE_LOG(INFO, "built forced-boundaries single term search sstable",
        K(doc_cnt), K(term_seq), K(docs_per_logical_block), K(micro_block_pairs_per_macro),
        K(min_doc_id), K(max_doc_id));
  }

  // Read the current row's (term_seq, doc_id) out of the iterator.
  void read_curr(ObInvSSTableRangeIter &iter, int64_t &term_seq, uint64_t &doc_id)
  {
    const ObDatum *id_datum = nullptr;
    OK(iter.get_cur_id(id_datum));
    ASSERT_NE(nullptr, id_datum);
    doc_id = id_datum->get_uint64();

    const ObDatumRowkey *term_rowkey = nullptr;
    OK(iter.get_cur_term(term_rowkey));
    ASSERT_NE(nullptr, term_rowkey);
    ASSERT_EQ(1, term_rowkey->get_datum_cnt());
    const ObString term_str = term_rowkey->get_datum(0).get_string();
    // Parse the trailing seq of "term%010ld".
    ASSERT_GE(term_str.length(), 4);
    char buf[32] = {0};
    const int32_t len = term_str.length() < 31 ? term_str.length() : 31;
    MEMCPY(buf, term_str.ptr(), len);
    term_seq = atol(buf + 4);
  }

  // Build a term-level DatumRange from two term seqs with the given border flags.
  // Caller owns lo_buf / hi_buf and they must outlive the scan.
  void build_term_range_from_seq(
      const int64_t lo_term,
      const int64_t hi_term,
      const bool left_closed,
      const bool right_closed,
      char *lo_buf,
      const int64_t lo_buf_len,
      char *hi_buf,
      const int64_t hi_buf_len,
      ObStorageDatum &lo_datum,
      ObStorageDatum &hi_datum,
      ObDatumRowkey &lo_key,
      ObDatumRowkey &hi_key,
      ObDatumRange &scan_range)
  {
    format_term(lo_term, lo_buf, lo_buf_len);
    format_term(hi_term, hi_buf, hi_buf_len);
    build_term_rowkey(lo_buf, static_cast<int32_t>(strlen(lo_buf)), lo_datum, lo_key);
    build_term_rowkey(hi_buf, static_cast<int32_t>(strlen(hi_buf)), hi_datum, hi_key);
    scan_range.set_start_key(lo_key);
    scan_range.set_end_key(hi_key);
    if (left_closed) {
      scan_range.set_left_closed();
    } else {
      scan_range.set_left_open();
    }
    if (right_closed) {
      scan_range.set_right_closed();
    } else {
      scan_range.set_right_open();
    }
  }

  // Open a range scan over inv_sstable and collect (term_seq, doc_id) pairs.
  // Returns the set of distinct term seqs seen plus the total row count.
  void run_range_scan_collect(
      ObInvSSTable &inv_sstable,
      const ObTableReadInfo &read_info,
      const ObDatumRange &scan_range,
      std::vector<std::pair<int64_t, uint64_t>> &results,
      int64_t &min_term,
      int64_t &max_term)
  {
    results.clear();
    min_term = INT64_MAX;
    max_term = 0;
    ObQueryFlag query_flag;
    query_flag.set_use_block_cache();
    ObTabletID tablet_id(TABLET_ID);
    ObInvRangeScanParam param;
    build_range_scan_param(scan_range, tablet_id, query_flag, allocator_, param);
    ObInvSSTableRangeIter iter;
    OK(iter.open(param, read_info, inv_sstable));
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret = iter.next())) {
      int64_t term_seq = 0;
      uint64_t doc_id = 0;
      read_curr(iter, term_seq, doc_id);
      results.push_back(std::make_pair(term_seq, doc_id));
      if (term_seq < min_term) { min_term = term_seq; }
      if (term_seq > max_term) { max_term = term_seq; }
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
};

// open() on an empty range (a term far beyond any generated term) leaves the
// iterator valid with is_end() == true; next() returns OB_ITER_END repeatedly.
TEST_F(TestInvSSTableRangeIter, open_empty_range)
{
  ObInvSSTable inv_sstable;
  build_multi_term_search_sstable(8 /*term_cnt*/, 32 /*docs_per_term*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  // Range [term999999998, term999999999): beyond all generated terms -> empty.
  char lo_buf[32] = {0};
  char hi_buf[32] = {0};
  format_term(999999998, lo_buf, sizeof(lo_buf));
  format_term(999999999, hi_buf, sizeof(hi_buf));
  ObStorageDatum lo_datum;
  ObStorageDatum hi_datum;
  ObDatumRowkey lo_key;
  ObDatumRowkey hi_key;
  build_term_rowkey(lo_buf, static_cast<int32_t>(strlen(lo_buf)), lo_datum, lo_key);
  build_term_rowkey(hi_buf, static_cast<int32_t>(strlen(hi_buf)), hi_datum, hi_key);
  ObDatumRange scan_range;
  scan_range.set_start_key(lo_key);
  scan_range.set_end_key(hi_key);
  scan_range.set_left_closed();
  scan_range.set_right_open();

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(scan_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  ASSERT_TRUE(iter.is_end());
  ASSERT_EQ(OB_ITER_END, iter.next());
  ASSERT_EQ(OB_ITER_END, iter.next());
}

// open() rejects invalid arguments and double-open.
TEST_F(TestInvSSTableRangeIter, open_invalid_and_double_open)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(64 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);

  // tablet id 无效。
  {
    ObInvRangeScanParam bad;
    build_range_scan_param(whole_range, ObTabletID(), query_flag, allocator_, bad);
    ASSERT_FALSE(bad.is_valid());
    ObInvSSTableRangeIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.open(bad, read_info, inv_sstable));
    ASSERT_FALSE(iter.is_opened_);
  }

  // Invalid: null scan_range_.
  {
    ObInvRangeScanParam bad;
    bad.reset();
    bad.tablet_id_ = tablet_id;
    bad.query_flag_ = query_flag;
    bad.allocator_ = &allocator_;
    ASSERT_FALSE(bad.is_valid());
    ObInvSSTableRangeIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.open(bad, read_info, inv_sstable));
  }

  // Invalid: null allocator_.
  {
    ObInvRangeScanParam bad;
    bad.reset();
    bad.scan_range_ = &whole_range;
    bad.tablet_id_ = tablet_id;
    bad.query_flag_ = query_flag;
    bad.allocator_ = nullptr;
    ASSERT_FALSE(bad.is_valid());
    ObInvSSTableRangeIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.open(bad, read_info, inv_sstable));
  }

  // A full physical key is supported only when doc_id is padded with MIN/MAX.
  {
    ObStorageDatum start_datums[2];
    ObStorageDatum end_datums[2];
    start_datums[0].set_string("term0000000001", 14);
    start_datums[1].set_min();
    end_datums[0].set_string("term0000000001", 14);
    end_datums[1].set_max();
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    OK(lo_key.assign(start_datums, 2));
    OK(hi_key.assign(end_datums, 2));
    ObDatumRange physical_range;
    physical_range.set_start_key(lo_key);
    physical_range.set_end_key(hi_key);
    physical_range.set_left_closed();
    physical_range.set_right_closed();
    ObInvRangeScanParam param;
    build_range_scan_param(physical_range, tablet_id, query_flag, allocator_, param);
    ASSERT_TRUE(param.is_valid());
    ObInvSSTableRangeIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.open(param, read_info, inv_sstable));
  }

  // A full physical key carrying a real doc_id is rejected because posting
  // blocks are not trimmed at row granularity by this iterator.
  {
    ObStorageDatum start_datums[2];
    ObStorageDatum end_datums[2];
    start_datums[0].set_string("term0000000001", 14);
    start_datums[1].set_uint(1);
    end_datums[0].set_string("term0000000009", 14);
    end_datums[1].set_uint(100);
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    OK(lo_key.assign(start_datums, 2));
    OK(hi_key.assign(end_datums, 2));
    ObDatumRange over_range;
    over_range.set_start_key(lo_key);
    over_range.set_end_key(hi_key);
    over_range.set_left_closed();
    over_range.set_right_open();
    ObInvRangeScanParam param;
    build_range_scan_param(over_range, tablet_id, query_flag, allocator_, param);
    ASSERT_TRUE(param.is_valid());
    ObInvSSTableRangeIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.open(param, read_info, inv_sstable));
  }

  // read info 无效。
  {
    ObInvRangeScanParam param;
    build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
    ObTableReadInfo invalid_read_info;
    ASSERT_FALSE(invalid_read_info.is_valid());
    ObInvSSTableRangeIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.open(param, invalid_read_info, inv_sstable));
    ASSERT_FALSE(iter.is_opened_);
    OK(iter.open(param, read_info, inv_sstable));
  }

  // SSTable 无效。
  {
    ObInvRangeScanParam param;
    build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
    ObInvSSTable invalid_sstable;
    ASSERT_FALSE(invalid_sstable.is_valid());
    ObInvSSTableRangeIter iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.open(param, read_info, invalid_sstable));
    ASSERT_FALSE(iter.is_opened_);
  }

  // Valid open, then a second open() must be rejected with OB_INIT_TWICE.
  {
    ObInvRangeScanParam param;
    build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
    ObInvSSTableRangeIter iter;
    OK(iter.open(param, read_info, inv_sstable));
    ASSERT_EQ(OB_INIT_TWICE, iter.open(param, read_info, inv_sstable));
  }
}

// set_table_handles() before open() must be rejected.
TEST_F(TestInvSSTableRangeIter, set_table_handles_before_open)
{
  ObInvSSTableRangeIter iter;
  ObTabletHandle tablet_handle;
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_NOT_INIT, iter.set_table_handles(tablet_handle, table_handle));
}

// 未打开时，所有依赖迭代器状态的公开接口都返回 OB_NOT_INIT。
TEST_F(TestInvSSTableRangeIter, public_calls_before_open)
{
  ObInvSSTableRangeIter iter;
  const ObDatum *datum = nullptr;
  const ObDatumRowkey *term_rowkey = nullptr;
  const uint64_t *doc_ids = nullptr;
  int64_t count = 0;

  ASSERT_FALSE(iter.is_end());
  ASSERT_EQ(OB_NOT_INIT, iter.next());
  ASSERT_EQ(OB_NOT_INIT, iter.get_cur_id(datum));
  ASSERT_EQ(nullptr, datum);
  ASSERT_EQ(OB_NOT_INIT, iter.get_cur_term(term_rowkey));
  ASSERT_EQ(nullptr, term_rowkey);
  ASSERT_EQ(OB_NOT_INIT, iter.get_cur_pos_list(datum));
  ASSERT_EQ(nullptr, datum);
  ASSERT_EQ(OB_NOT_INIT, iter.get_cur_nested_key_list(datum));
  ASSERT_EQ(nullptr, datum);
  ASSERT_EQ(OB_NOT_INIT,
      iter.get_next_doc_ids(1, doc_ids, count));
  ASSERT_EQ(nullptr, doc_ids);
  ASSERT_EQ(0, count);
}

TEST_F(TestInvSSTableRangeIter, doc_id_drain_requires_explicit_route)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(16 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObInvRangeScanParam param;
  build_range_scan_param(
      whole_range, ObTabletID(TABLET_ID), query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  uint64_t sentinel = 1;
  const uint64_t *doc_ids = &sentinel;
  int64_t count = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      iter.get_next_doc_ids(0, doc_ids, count));
  EXPECT_EQ(nullptr, doc_ids);
  EXPECT_EQ(0, count);

  doc_ids = &sentinel;
  count = -1;
  EXPECT_EQ(OB_NOT_SUPPORTED,
      iter.get_next_doc_ids(1, doc_ids, count));
  EXPECT_EQ(nullptr, doc_ids);
  EXPECT_EQ(0, count);

  param.enable_doc_id_drain_ = true;
  param.need_posting_value_ = true;
  ObInvSSTableRangeIter posting_value_iter;
  OK(posting_value_iter.open(param, read_info, inv_sstable));
  EXPECT_EQ(OB_NOT_SUPPORTED,
      posting_value_iter.get_next_doc_ids(1, doc_ids, count));
  EXPECT_EQ(nullptr, doc_ids);
  EXPECT_EQ(0, count);
}

TEST_F(TestInvSSTableRangeIter, doc_id_drain_rejects_row_mode_apis)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(16 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObInvRangeScanParam param;
  build_range_scan_param(
      whole_range, ObTabletID(TABLET_ID), query_flag, allocator_, param);
  param.enable_doc_id_drain_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  EXPECT_FALSE(iter.term_buf_.is_valid());
  EXPECT_FALSE(iter.curr_term_.is_valid());
  EXPECT_FALSE(iter.endkey_buf_.is_valid());
  EXPECT_FALSE(iter.materialized_endkey_.is_valid());

  const ObDatum *datum = nullptr;
  const ObDatumRowkey *term_rowkey = nullptr;
  EXPECT_EQ(OB_NOT_SUPPORTED, iter.next());
  EXPECT_EQ(OB_NOT_SUPPORTED, iter.get_cur_id(datum));
  EXPECT_EQ(nullptr, datum);
  EXPECT_EQ(OB_NOT_SUPPORTED, iter.get_cur_term(term_rowkey));
  EXPECT_EQ(nullptr, term_rowkey);
  EXPECT_EQ(OB_NOT_SUPPORTED, iter.get_cur_pos_list(datum));
  EXPECT_EQ(nullptr, datum);
  EXPECT_EQ(OB_NOT_SUPPORTED, iter.get_cur_nested_key_list(datum));
  EXPECT_EQ(nullptr, datum);
}

// 打开后传入无效 tablet/table handle，返回 OB_INVALID_ARGUMENT。
TEST_F(TestInvSSTableRangeIter, set_table_handles_invalid_after_open)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(16 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObInvRangeScanParam param;
  build_range_scan_param(
      whole_range, ObTabletID(TABLET_ID), query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  ObTabletHandle tablet_handle;
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.set_table_handles(tablet_handle, table_handle));
}

// Whole-range scan over a single-term sstable: iterate the entire posting list and
// verify contiguous-by-stride doc_ids, a constant term, and the exact count.
TEST_F(TestInvSSTableRangeIter, single_term_whole_range)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(1000 /*doc_cnt*/, 1 /*start*/, 2 /*stride*/,
      1 /*term_seq*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t i = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    ASSERT_LT(i, static_cast<int64_t>(oracle_.size()));
    if (term_seq != oracle_[i].first || doc_id != oracle_[i].second) {
      ASSERT_EQ(oracle_[i].first, term_seq) << "term mismatch at pos " << i;
      ASSERT_EQ(oracle_[i].second, doc_id) << "doc_id mismatch at pos " << i;
      break;
    }
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(static_cast<int64_t>(oracle_.size()), i);
  ASSERT_TRUE(iter.is_end());
  ASSERT_EQ(OB_ITER_END, iter.next());
}

// One term's posting list spans several logical blocks that live in the same posting
// data micro block. Advancing across those logical blocks must reuse the fetched micro
// block and reader (need_load_micro_block_ stays false, the micro block buffer pointer
// is unchanged), only relocating the payload — no redundant cache-get / reader re-init.
TEST_F(TestInvSSTableRangeIter, single_term_reuse_micro_block_across_logical_blocks)
{
  // 800 docs / 256-cap logical blocks => ~4 logical blocks in one micro block.
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(800 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  const char *micro_buf = nullptr;
  int64_t logical_block_switches = 0;  // times the logical-block cursor advanced within the micro block
  int64_t micro_block_refetches = 0;   // times the backing micro block was (re)fetched
  int64_t prev_block_idx = -1;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    // After a materialized row the reader is loaded; need_load_micro_block_ is false.
    ASSERT_FALSE(iter.need_load_micro_block());
    ASSERT_TRUE(iter.curr_posting_block_loaded_);
    const char *curr_buf = iter.curr_posting_micro_block_.get_buf();
    ASSERT_NE(nullptr, curr_buf);
    if (nullptr == micro_buf) {
      micro_buf = curr_buf;
      ++micro_block_refetches;
    } else if (curr_buf != micro_buf) {
      // Backing micro block changed (crossed a data micro block for this term).
      micro_buf = curr_buf;
      ++micro_block_refetches;
    }
    const int64_t curr_block_idx = iter.curr_meta_.locator_.block_idx_;
    if (curr_block_idx != prev_block_idx) {
      ++logical_block_switches;
    }
    prev_block_idx = curr_block_idx;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  // Multiple logical blocks were traversed for the single term.
  ASSERT_GT(logical_block_switches, 1);
  // But all of them shared one posting data micro block, fetched exactly once — proving
  // logical-block advances within an index row do not re-fetch the micro block.
  ASSERT_EQ(1, micro_block_refetches)
      << "logical-block advance within one micro block must not re-fetch";
}

// Many small terms whose logical blocks all live in one posting data micro block: each
// term is a separate index row, but they share the same micro-block locator. Crossing
// from one index row (term) to the next must reuse the fetched micro block and reader
// (derived block_changed == false), so the micro block is fetched exactly once for the
// whole multi-term scan — not once per term.
TEST_F(TestInvSSTableRangeIter, multi_term_reuse_micro_block_across_index_rows)
{
  // 40 terms x 4 docs = 160 docs (~1.3KB of doc_ids) fit in one 16KB micro block.
  const int64_t term_cnt = 40;
  const int64_t docs_per_term = 4;
  ObInvSSTable inv_sstable;
  build_multi_term_search_sstable(term_cnt, docs_per_term, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  const char *micro_buf = nullptr;
  int64_t micro_block_refetches = 0;
  int64_t distinct_terms = 0;
  int64_t prev_term = 0;
  int64_t rows = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    ASSERT_FALSE(iter.need_load_micro_block());
    const char *curr_buf = iter.curr_posting_micro_block_.get_buf();
    ASSERT_NE(nullptr, curr_buf);
    if (curr_buf != micro_buf) {
      micro_buf = curr_buf;
      ++micro_block_refetches;
    }
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    if (term_seq != prev_term) {
      ++distinct_terms;
      prev_term = term_seq;
    }
    ++rows;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(term_cnt * docs_per_term, rows);
  // Every term produced a distinct index row...
  ASSERT_EQ(term_cnt, distinct_terms);
  // ...yet the small dataset packs all logical blocks into very few posting micro
  // blocks. The precise (locator-based) block_changed derivation reuses the micro block
  // across sibling index rows, so the fetch count is far below the term count (a naive
  // "re-fetch on every index row" would equal term_cnt). Not pinned to exactly 1 to
  // stay robust against the writer's micro-block split boundaries.
  ASSERT_GE(micro_block_refetches, 1);
  ASSERT_LT(micro_block_refetches, distinct_terms)
      << "sibling index rows sharing a micro block must not re-fetch it per term";
}

// Whole-range scan over a multi-term sstable spanning many posting blocks / micro /
// macro / leaf-index boundaries: verify the full (term, doc_id) sequence matches the
// oracle, terms are non-decreasing, and doc_ids increase within each term.
TEST_F(TestInvSSTableRangeIter, multi_term_whole_range_full_scan)
{
  ObInvSSTable inv_sstable;
  // 200 terms x 600 docs = 120k rows: crosses posting-block (cap 256), micro, and
  // likely macro/leaf-index boundaries while keeping the oracle cheap.
  build_multi_term_search_sstable(200 /*term_cnt*/, 600 /*docs_per_term*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t i = 0;
  int64_t prev_term = 0;
  uint64_t prev_doc = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    ASSERT_LT(i, static_cast<int64_t>(oracle_.size()));
    if (term_seq != oracle_[i].first || doc_id != oracle_[i].second) {
      ASSERT_EQ(oracle_[i].first, term_seq) << "term mismatch at pos " << i;
      ASSERT_EQ(oracle_[i].second, doc_id) << "doc_id mismatch at pos " << i;
      break;
    }
    // Ordering invariants: term non-decreasing; doc_id strictly increasing within a term.
    if (i > 0) {
      if (term_seq == prev_term) {
        ASSERT_GT(doc_id, prev_doc) << "doc_id must increase within a term at pos " << i;
      } else {
        ASSERT_GT(term_seq, prev_term) << "term must be non-decreasing at pos " << i;
      }
    }
    prev_term = term_seq;
    prev_doc = doc_id;
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(static_cast<int64_t>(oracle_.size()), i);
  ASSERT_TRUE(iter.is_end());
}

// Whole-range scan over a dataset where doc_id DROPS at every term boundary: within a
// term doc_ids strictly increase, but a new term restarts on a lower band so the first
// doc of a term can be < the last doc of the previous term. Verify the iterator crosses
// term boundaries correctly (does not rely on global doc_id monotonicity), the full
// (term, doc_id) sequence matches the oracle, and at least one real drop is observed.
TEST_F(TestInvSSTableRangeIter, multi_term_doc_id_drop_at_boundaries)
{
  ObInvSSTable inv_sstable;
  const int64_t term_cnt = 60;
  const int64_t docs_per_term = 400;  // crosses posting-block (cap 256) within a term
  build_multi_term_search_sstable(term_cnt, docs_per_term, inv_sstable, true /*doc_id_drop*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t i = 0;
  int64_t prev_term = 0;
  uint64_t prev_doc = 0;
  int64_t boundary_cnt = 0;
  int64_t doc_drop_cnt = 0;  // boundaries where the new term's first doc < previous term's last doc
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    ASSERT_LT(i, static_cast<int64_t>(oracle_.size()));
    if (term_seq != oracle_[i].first || doc_id != oracle_[i].second) {
      ASSERT_EQ(oracle_[i].first, term_seq) << "term mismatch at pos " << i;
      ASSERT_EQ(oracle_[i].second, doc_id) << "doc_id mismatch at pos " << i;
      break;
    }
    if (i > 0) {
      if (term_seq == prev_term) {
        ASSERT_GT(doc_id, prev_doc) << "doc_id must strictly increase within a term at pos " << i;
      } else {
        ASSERT_GT(term_seq, prev_term) << "term must be non-decreasing at pos " << i;
        ++boundary_cnt;
        if (doc_id < prev_doc) {
          ++doc_drop_cnt;
        }
      }
    }
    prev_term = term_seq;
    prev_doc = doc_id;
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(static_cast<int64_t>(oracle_.size()), i);
  ASSERT_TRUE(iter.is_end());
  ASSERT_EQ(term_cnt - 1, boundary_cnt) << "every term boundary must be crossed exactly once";
  // Odd terms live in band_lo (docs 1..docs_per_term); even terms in band_hi
  // (docs_per_term+1..2*docs_per_term). Odd->even boundaries rise; even->odd
  // boundaries drop (band_hi -> band_lo). So drops occur at boundaries 2->3, 4->5, ...
  ASSERT_GT(doc_drop_cnt, 0) << "expected doc_id to drop at term boundaries";
}

// Sub-range [term_a, term_c) with a closed lower and open upper bound: only terms in
// [a, c) appear; the upper boundary term c is excluded, and terms below a are skipped.
TEST_F(TestInvSSTableRangeIter, multi_term_sub_range_term_boundaries)
{
  ObInvSSTable inv_sstable;
  const int64_t term_cnt = 20;
  const int64_t docs_per_term = 100;
  build_multi_term_search_sstable(term_cnt, docs_per_term, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  // [term5, term15): expect terms 5..14 inclusive.
  const int64_t lo_term = 5;
  const int64_t hi_term = 15;
  char lo_buf[32] = {0};
  char hi_buf[32] = {0};
  format_term(lo_term, lo_buf, sizeof(lo_buf));
  format_term(hi_term, hi_buf, sizeof(hi_buf));
  ObStorageDatum lo_datum;
  ObStorageDatum hi_datum;
  ObDatumRowkey lo_key;
  ObDatumRowkey hi_key;
  build_term_rowkey(lo_buf, static_cast<int32_t>(strlen(lo_buf)), lo_datum, lo_key);
  build_term_rowkey(hi_buf, static_cast<int32_t>(strlen(hi_buf)), hi_datum, hi_key);
  ObDatumRange scan_range;
  scan_range.set_start_key(lo_key);
  scan_range.set_end_key(hi_key);
  scan_range.set_left_closed();
  scan_range.set_right_open();

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(scan_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t count = 0;
  int64_t min_term_seen = term_cnt + 1;
  int64_t max_term_seen = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    ASSERT_GE(term_seq, lo_term) << "term below the closed lower bound leaked in";
    ASSERT_LT(term_seq, hi_term) << "term at/above the open upper bound leaked in";
    if (term_seq < min_term_seen) {
      min_term_seen = term_seq;
    }
    if (term_seq > max_term_seen) {
      max_term_seen = term_seq;
    }
    ++count;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  // Terms lo_term .. hi_term-1, each with docs_per_term docs.
  ASSERT_EQ((hi_term - lo_term) * docs_per_term, count);
  ASSERT_EQ(lo_term, min_term_seen);
  ASSERT_EQ(hi_term - 1, max_term_seen);
}

// A start key that falls between two terms (an absent term string) positions the scan
// on the next present term's first doc; an open lower bound on a present term excludes it.
TEST_F(TestInvSSTableRangeIter, multi_term_start_between_terms)
{
  ObInvSSTable inv_sstable;
  const int64_t term_cnt = 12;
  const int64_t docs_per_term = 50;
  build_multi_term_search_sstable(term_cnt, docs_per_term, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  // Start key "term0000000006x" sorts strictly after term6 and before term7, so the
  // first row must belong to term7. End = whole tail.
  char lo_buf[32] = {0};
  snprintf(lo_buf, sizeof(lo_buf), "term%010ldx", static_cast<long>(6));
  ObStorageDatum lo_datum;
  ObDatumRowkey lo_key;
  build_term_rowkey(lo_buf, static_cast<int32_t>(strlen(lo_buf)), lo_datum, lo_key);
  ObDatumRowkey hi_key;
  hi_key.set_max_rowkey();
  ObDatumRange scan_range;
  scan_range.set_start_key(lo_key);
  scan_range.set_end_key(hi_key);
  scan_range.set_left_closed();
  scan_range.set_right_open();

  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(scan_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  OK(iter.next());
  int64_t first_term = 0;
  uint64_t first_doc = 0;
  read_curr(iter, first_term, first_doc);
  ASSERT_EQ(7, first_term) << "start between term6 and term7 must land on term7";

  int64_t count = 1;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    ASSERT_GE(term_seq, 7);
    ++count;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  // Terms 7..12 inclusive.
  ASSERT_EQ((term_cnt - 7 + 1) * docs_per_term, count);
}

// get_cur_id() / get_cur_term() before any next() and after end are errors, not crashes.
TEST_F(TestInvSSTableRangeIter, cursor_access_out_of_bounds)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(16 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  // Before the first next(): no row materialized.
  const ObDatum *id_datum = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.get_cur_id(id_datum));
  ASSERT_EQ(nullptr, id_datum);
  const ObDatumRowkey *term_rowkey = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.get_cur_term(term_rowkey));
  ASSERT_EQ(nullptr, term_rowkey);

  // Drain to end.
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {}
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_TRUE(iter.is_end());

  // After end: cursor getters report iter-end, not OB_SUCCESS.
  ASSERT_EQ(OB_ITER_END, iter.get_cur_id(id_datum));
  ASSERT_EQ(nullptr, id_datum);
  ASSERT_EQ(OB_ITER_END, iter.get_cur_term(term_rowkey));
  ASSERT_EQ(nullptr, term_rowkey);
}

// reset() after open returns the iterator to a pre-open state so it can be re-opened
// without OB_INIT_TWICE and re-scanned to the same result.
TEST_F(TestInvSSTableRangeIter, reset_then_reopen)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(300 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  // Partially consume, then reset.
  OK(iter.next());
  OK(iter.next());
  iter.reset();
  ASSERT_FALSE(iter.is_opened_);

  // Re-open on the same range and scan the full posting list.
  OK(iter.open(param, read_info, inv_sstable));
  int64_t i = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    ASSERT_LT(i, static_cast<int64_t>(oracle_.size()));
    ASSERT_EQ(oracle_[i].first, term_seq);
    ASSERT_EQ(oracle_[i].second, doc_id);
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(static_cast<int64_t>(oracle_.size()), i);
}

// ===========================================================================
// Posting Value Tests (nested_key_list) — grouped, separately numbered.
//
// These tests exercise the posting value micro block read path: lazy loading,
// cross-logical-block / cross-index-row micro block reuse, error handling, and
// correctness of nested_key_list blobs returned by get_cur_nested_key_list().
// All datasets are SEARCH indexes with has_nested_key=true; the nested_key blob
// for oracle row k is "nk%010ld" % k.
// ===========================================================================

// PV-1: basic correctness — single term with nested_key, verify every row's
// nested_key blob matches the oracle index.
TEST_F(TestInvSSTableRangeIter, pv1_nested_key_basic)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(100 /*doc_cnt*/, 1 /*start*/, 2 /*stride*/,
      1 /*term_seq*/, inv_sstable, true /*has_nested_key*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t i = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatum *nk_datum = nullptr;
    OK(iter.get_cur_nested_key_list(nk_datum));
    ASSERT_NE(nullptr, nk_datum);
    const ObString nk_str = nk_datum->get_string();
    char exp_buf[32] = {0};
    format_nested_key(i, exp_buf, sizeof(exp_buf));
    ASSERT_EQ(ObString(static_cast<int32_t>(strlen(exp_buf)), exp_buf), nk_str)
        << "nested_key mismatch at row " << i;
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(100, i);
}

// PV-2: lazy loading — posting value micro block is not fetched until
// get_cur_nested_key_list() is first called.
TEST_F(TestInvSSTableRangeIter, pv2_nested_key_lazy_load)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(16 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable, true /*has_nested_key*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  OK(iter.next());

  ASSERT_FALSE(iter.curr_value_block_loaded_);
  const ObDatum *nk_datum = nullptr;
  OK(iter.get_cur_nested_key_list(nk_datum));
  ASSERT_NE(nullptr, nk_datum);
  ASSERT_TRUE(iter.curr_value_block_loaded_);
}

// PV-3: reuse posting value micro block across logical blocks within one
// index row — the value micro block is fetched exactly once.
TEST_F(TestInvSSTableRangeIter, pv3_nested_key_reuse_across_logical_blocks)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(800 /*doc_cnt*/, 1 /*start*/, 1 /*stride*/,
      1 /*term_seq*/, inv_sstable, true /*has_nested_key*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  const char *value_buf = nullptr;
  int64_t value_refetches = 0;
  int64_t i = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatum *nk_datum = nullptr;
    OK(iter.get_cur_nested_key_list(nk_datum));
    ASSERT_NE(nullptr, nk_datum);
    const char *curr_buf = iter.curr_value_micro_block_.get_buf();
    ASSERT_NE(nullptr, curr_buf);
    if (curr_buf != value_buf) {
      value_buf = curr_buf;
      ++value_refetches;
    }
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(800, i);
  ASSERT_GT(value_refetches, 0);
  ASSERT_LE(value_refetches, 2);
}

// PV-4: reuse posting value micro block across multiple index rows (terms)
// that share the same physical value micro block.
TEST_F(TestInvSSTableRangeIter, pv4_nested_key_reuse_across_index_rows)
{
  const int64_t term_cnt = 40;
  const int64_t docs_per_term = 4;
  ObInvSSTable inv_sstable;
  build_multi_term_search_sstable(term_cnt, docs_per_term, inv_sstable,
      false /*doc_id_drop*/, true /*has_nested_key*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  const char *value_buf = nullptr;
  int64_t value_refetches = 0;
  int64_t i = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatum *nk_datum = nullptr;
    OK(iter.get_cur_nested_key_list(nk_datum));
    ASSERT_NE(nullptr, nk_datum);
    const char *curr_buf = iter.curr_value_micro_block_.get_buf();
    if (curr_buf != value_buf) {
      value_buf = curr_buf;
      ++value_refetches;
    }
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(term_cnt * docs_per_term, i);
  ASSERT_GE(value_refetches, 1);
  ASSERT_LT(value_refetches, term_cnt)
      << "sibling index rows sharing a value micro block must not re-fetch per term";
}

// PV-5：need_posting_value_ 为 false 时，两个 posting value 接口均返回
// OB_NOT_SUPPORTED。
TEST_F(TestInvSSTableRangeIter, pv5_nested_key_disabled)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(16, 1, 1, 1, inv_sstable, true /*has_nested_key*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  // need_posting_value_ left false

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  OK(iter.next());

  const ObDatum *value_datum = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_cur_pos_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_cur_nested_key_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
}

// PV-6：next() 前和迭代结束后，两个 posting value 接口均返回准确的状态错误码。
TEST_F(TestInvSSTableRangeIter, pv6_nested_key_access_out_of_bounds)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(16, 1, 1, 1, inv_sstable, true /*has_nested_key*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  const ObDatum *value_datum = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.get_cur_pos_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
  ASSERT_EQ(OB_ERR_UNEXPECTED, iter.get_cur_nested_key_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);

  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {}
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_TRUE(iter.is_end());

  ASSERT_EQ(OB_ITER_END, iter.get_cur_pos_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
  ASSERT_EQ(OB_ITER_END, iter.get_cur_nested_key_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
}

// PV-7: large multi-term dataset spanning multiple value micro blocks / macro
// blocks — verify full nested_key correctness across physical boundaries.
TEST_F(TestInvSSTableRangeIter, pv7_nested_key_cross_micro_block)
{
  ObInvSSTable inv_sstable;
  build_multi_term_search_sstable(200 /*term_cnt*/, 600 /*docs_per_term*/,
      inv_sstable, false /*doc_id_drop*/, true /*has_nested_key*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t i = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatum *nk_datum = nullptr;
    OK(iter.get_cur_nested_key_list(nk_datum));
    ASSERT_NE(nullptr, nk_datum);
    const ObString nk_str = nk_datum->get_string();
    char exp_buf[32] = {0};
    format_nested_key(i, exp_buf, sizeof(exp_buf));
    ASSERT_EQ(ObString(static_cast<int32_t>(strlen(exp_buf)), exp_buf), nk_str)
        << "nested_key mismatch at row " << i;
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(static_cast<int64_t>(oracle_.size()), i);
}

// PV-8：SSTable 不含 position list 和 nested key，且 need_posting_value_=true 时，
// 两个接口均返回 OB_NOT_SUPPORTED，且不会加载 value block。
TEST_F(TestInvSSTableRangeIter, pv8_nested_key_capability_not_supported)
{
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(16, 1, 1, 1, inv_sstable, false /*has_nested_key*/);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_FALSE(inv_sstable.get_inv_meta().has_nested_key_);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  OK(iter.next());

  ASSERT_FALSE(iter.curr_value_block_loaded_);
  const ObDatum *value_datum = nullptr;
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_cur_pos_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
  ASSERT_FALSE(iter.curr_value_block_loaded_);
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_cur_nested_key_list(value_datum));
  ASSERT_EQ(nullptr, value_datum);
  ASSERT_FALSE(iter.curr_value_block_loaded_);
}

// 同一个 term 跨多个 posting 微块（同一宏块内）：每 256 个文档强制切一次
// logical block + micro block，但不切宏块。验证遍历结果与 oracle 完全一致，
// posting 微块 locator 至少变化一次，且宏块 id 全程不变。
TEST_F(TestInvSSTableRangeIter, single_term_cross_forced_posting_micro_blocks)
{
  const int64_t docs_per_logical_block = 256;
  const int64_t logical_block_cnt = 4;
  const int64_t doc_cnt = docs_per_logical_block * logical_block_cnt;
  ObInvSSTable inv_sstable;
  build_single_term_forced_boundaries_sstable(doc_cnt, 1 /*start_doc_id*/,
      1 /*stride*/, 1 /*term_seq*/, inv_sstable, false /*has_nested_key*/,
      docs_per_logical_block, logical_block_cnt /*micro block pairs per macro: 不切宏块*/);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t i = 0;
  int64_t posting_locator_changes = 0;
  MacroBlockId prev_macro_id;
  int64_t prev_posting_offset = -1;
  int64_t prev_posting_size = -1;
  bool first_row = true;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    ASSERT_LT(i, static_cast<int64_t>(oracle_.size()));
    ASSERT_EQ(oracle_[i].first, term_seq) << "term mismatch at pos " << i;
    ASSERT_EQ(oracle_[i].second, doc_id) << "doc_id mismatch at pos " << i;

    const MacroBlockId &curr_macro_id = iter.curr_index_info_.get_macro_id();
    const int64_t curr_posting_offset = iter.curr_posting_micro_locator_.block_offset_;
    const int64_t curr_posting_size = iter.curr_posting_micro_locator_.block_size_;
    if (first_row) {
      prev_macro_id = curr_macro_id;
      prev_posting_offset = curr_posting_offset;
      prev_posting_size = curr_posting_size;
      first_row = false;
    } else {
      ASSERT_EQ(prev_macro_id, curr_macro_id)
          << "macro id must stay the same within one macro block at pos " << i;
      if (curr_posting_offset != prev_posting_offset
          || curr_posting_size != prev_posting_size) {
        ++posting_locator_changes;
        prev_posting_offset = curr_posting_offset;
        prev_posting_size = curr_posting_size;
      }
    }
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(static_cast<int64_t>(oracle_.size()), i);
  ASSERT_TRUE(iter.is_end());
  ASSERT_GT(posting_locator_changes, 0)
      << "posting micro block locator must change at least once across forced micro block boundaries";
}

// 同一个 term 跨多个宏块：每 256 个文档强制切一次 logical block + micro block，
// 且每次都切宏块。验证 sstable 含多个数据宏块，遍历期间宏块 id 发生变化，
// 且结果与 oracle 完全一致、无丢失或重复。
TEST_F(TestInvSSTableRangeIter, single_term_cross_forced_macro_blocks)
{
  const int64_t docs_per_logical_block = 256;
  const int64_t logical_block_cnt = 4;
  const int64_t doc_cnt = docs_per_logical_block * logical_block_cnt;
  ObInvSSTable inv_sstable;
  build_single_term_forced_boundaries_sstable(doc_cnt, 1 /*start_doc_id*/,
      1 /*stride*/, 1 /*term_seq*/, inv_sstable, false /*has_nested_key*/,
      docs_per_logical_block, 1 /*micro block pair per macro*/);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_GT(inv_sstable.get_data_macro_block_count(), 1)
      << "forced flush must produce multiple data macro blocks";

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t i = 0;
  int64_t macro_id_changes = 0;
  MacroBlockId prev_macro_id;
  bool first_row = true;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    int64_t term_seq = 0;
    uint64_t doc_id = 0;
    read_curr(iter, term_seq, doc_id);
    ASSERT_LT(i, static_cast<int64_t>(oracle_.size()));
    ASSERT_EQ(oracle_[i].first, term_seq) << "term mismatch at pos " << i;
    ASSERT_EQ(oracle_[i].second, doc_id) << "doc_id mismatch at pos " << i;

    const MacroBlockId &curr_macro_id = iter.curr_index_info_.get_macro_id();
    if (first_row) {
      prev_macro_id = curr_macro_id;
      first_row = false;
    } else if (!(curr_macro_id == prev_macro_id)) {
      ++macro_id_changes;
      prev_macro_id = curr_macro_id;
    }
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(static_cast<int64_t>(oracle_.size()), i);
  ASSERT_TRUE(iter.is_end());
  ASSERT_GT(macro_id_changes, 0)
      << "macro id must change at least once across forced macro block boundaries";
}

// 带 nested key 的同一个 term 跨多个 posting 微块和宏块：每 256 个文档切一次
// logical block + micro block，每两个微块切一次宏块。验证 posting 微块 locator
// 和 posting value 微块 locator 均发生变化，宏块 id 发生变化，且每行 nested key
// 与 oracle 完全一致。
TEST_F(TestInvSSTableRangeIter, pv9_nested_key_cross_forced_micro_and_macro_blocks)
{
  const int64_t docs_per_logical_block = 256;
  const int64_t logical_block_cnt = 4;
  const int64_t doc_cnt = docs_per_logical_block * logical_block_cnt;
  ObInvSSTable inv_sstable;
  build_single_term_forced_boundaries_sstable(doc_cnt, 1 /*start_doc_id*/,
      1 /*stride*/, 1 /*term_seq*/, inv_sstable, true /*has_nested_key*/,
      docs_per_logical_block, 2 /*micro_block_pairs_per_macro*/);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_GT(inv_sstable.get_data_macro_block_count(), 1)
      << "forced flush must produce multiple data macro blocks";

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObTabletID tablet_id(TABLET_ID);
  ObInvRangeScanParam param;
  build_range_scan_param(whole_range, tablet_id, query_flag, allocator_, param);
  param.need_posting_value_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));

  int64_t i = 0;
  int64_t posting_locator_changes = 0;
  int64_t value_locator_changes = 0;
  int64_t macro_id_changes = 0;
  MacroBlockId prev_macro_id;
  int64_t prev_posting_offset = -1;
  int64_t prev_posting_size = -1;
  int64_t prev_value_offset = -1;
  int64_t prev_value_size = -1;
  bool first_row = true;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.next())) {
    const ObDatum *nk_datum = nullptr;
    OK(iter.get_cur_nested_key_list(nk_datum));
    ASSERT_NE(nullptr, nk_datum);
    const ObString nk_str = nk_datum->get_string();
    char exp_buf[32] = {0};
    format_nested_key(i, exp_buf, sizeof(exp_buf));
    ASSERT_EQ(ObString(static_cast<int32_t>(strlen(exp_buf)), exp_buf), nk_str)
        << "nested_key mismatch at row " << i;

    const MacroBlockId &curr_macro_id = iter.curr_index_info_.get_macro_id();
    const int64_t curr_posting_offset = iter.curr_posting_micro_locator_.block_offset_;
    const int64_t curr_posting_size = iter.curr_posting_micro_locator_.block_size_;
    const int64_t curr_value_offset = iter.curr_value_micro_locator_.block_offset_;
    const int64_t curr_value_size = iter.curr_value_micro_locator_.block_size_;
    if (first_row) {
      prev_macro_id = curr_macro_id;
      prev_posting_offset = curr_posting_offset;
      prev_posting_size = curr_posting_size;
      prev_value_offset = curr_value_offset;
      prev_value_size = curr_value_size;
      first_row = false;
    } else {
      if (!(curr_macro_id == prev_macro_id)) {
        ++macro_id_changes;
        prev_macro_id = curr_macro_id;
      }
      if (curr_posting_offset != prev_posting_offset
          || curr_posting_size != prev_posting_size) {
        ++posting_locator_changes;
        prev_posting_offset = curr_posting_offset;
        prev_posting_size = curr_posting_size;
      }
      if (curr_value_offset != prev_value_offset
          || curr_value_size != prev_value_size) {
        ++value_locator_changes;
        prev_value_offset = curr_value_offset;
        prev_value_size = curr_value_size;
      }
    }
    ++i;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(static_cast<int64_t>(oracle_.size()), i);
  ASSERT_TRUE(iter.is_end());
  ASSERT_GT(posting_locator_changes, 0)
      << "posting micro block locator must change across forced micro block boundaries";
  ASSERT_GT(value_locator_changes, 0)
      << "posting value micro block locator must change across forced micro block boundaries";
  ASSERT_GT(macro_id_changes, 0)
      << "macro id must change across forced macro block boundaries";
}

// 四种边界组合在同一 sstable 上逐一验证：[a,b)、(a,b)、[a,b]、(a,b]。
// sstable 含 term1..term20，每 term 100 doc。
//   [5,15)  → term 5..14   (已有用例覆盖，此处作为基线)
//   (5,15)  → term 6..14   左开排除 term5
//   [5,15]  → term 5..15   右闭包含 term15
//   (5,15]  → term 6..15   左开排除 term5，右闭包含 term15
TEST_F(TestInvSSTableRangeIter, range_boundary_combinations)
{
  ObInvSSTable inv_sstable;
  const int64_t term_cnt = 20;
  const int64_t docs_per_term = 100;
  build_multi_term_search_sstable(term_cnt, docs_per_term, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  const int64_t lo_term = 5;
  const int64_t hi_term = 15;

  // [5,15) → term 5..14
  {
    char lo_buf[32] = {0};
    char hi_buf[32] = {0};
    ObStorageDatum lo_datum;
    ObStorageDatum hi_datum;
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    ObDatumRange scan_range;
    build_term_range_from_seq(lo_term, hi_term, true /*left_closed*/, false /*right_closed*/,
        lo_buf, sizeof(lo_buf), hi_buf, sizeof(hi_buf),
        lo_datum, hi_datum, lo_key, hi_key, scan_range);
    std::vector<std::pair<int64_t, uint64_t>> results;
    int64_t min_term = 0;
    int64_t max_term = 0;
    run_range_scan_collect(inv_sstable, read_info, scan_range, results, min_term, max_term);
    ASSERT_EQ((hi_term - lo_term) * docs_per_term, static_cast<int64_t>(results.size()));
    ASSERT_EQ(lo_term, min_term);
    ASSERT_EQ(hi_term - 1, max_term);
  }

  // (5,15) → term 6..14
  {
    char lo_buf[32] = {0};
    char hi_buf[32] = {0};
    ObStorageDatum lo_datum;
    ObStorageDatum hi_datum;
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    ObDatumRange scan_range;
    build_term_range_from_seq(lo_term, hi_term, false /*left_closed*/, false /*right_closed*/,
        lo_buf, sizeof(lo_buf), hi_buf, sizeof(hi_buf),
        lo_datum, hi_datum, lo_key, hi_key, scan_range);
    std::vector<std::pair<int64_t, uint64_t>> results;
    int64_t min_term = 0;
    int64_t max_term = 0;
    run_range_scan_collect(inv_sstable, read_info, scan_range, results, min_term, max_term);
    ASSERT_EQ((hi_term - lo_term - 1) * docs_per_term, static_cast<int64_t>(results.size()));
    ASSERT_EQ(lo_term + 1, min_term) << "left-open must exclude the lower bound term";
    ASSERT_EQ(hi_term - 1, max_term);
  }

  // [5,15] → term 5..15
  {
    char lo_buf[32] = {0};
    char hi_buf[32] = {0};
    ObStorageDatum lo_datum;
    ObStorageDatum hi_datum;
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    ObDatumRange scan_range;
    build_term_range_from_seq(lo_term, hi_term, true /*left_closed*/, true /*right_closed*/,
        lo_buf, sizeof(lo_buf), hi_buf, sizeof(hi_buf),
        lo_datum, hi_datum, lo_key, hi_key, scan_range);
    std::vector<std::pair<int64_t, uint64_t>> results;
    int64_t min_term = 0;
    int64_t max_term = 0;
    run_range_scan_collect(inv_sstable, read_info, scan_range, results, min_term, max_term);
    ASSERT_EQ((hi_term - lo_term + 1) * docs_per_term, static_cast<int64_t>(results.size()));
    ASSERT_EQ(lo_term, min_term);
    ASSERT_EQ(hi_term, max_term) << "right-closed must include the upper bound term";
  }

  // (5,15] → term 6..15
  {
    char lo_buf[32] = {0};
    char hi_buf[32] = {0};
    ObStorageDatum lo_datum;
    ObStorageDatum hi_datum;
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    ObDatumRange scan_range;
    build_term_range_from_seq(lo_term, hi_term, false /*left_closed*/, true /*right_closed*/,
        lo_buf, sizeof(lo_buf), hi_buf, sizeof(hi_buf),
        lo_datum, hi_datum, lo_key, hi_key, scan_range);
    std::vector<std::pair<int64_t, uint64_t>> results;
    int64_t min_term = 0;
    int64_t max_term = 0;
    run_range_scan_collect(inv_sstable, read_info, scan_range, results, min_term, max_term);
    ASSERT_EQ((hi_term - lo_term) * docs_per_term, static_cast<int64_t>(results.size()));
    ASSERT_EQ(lo_term + 1, min_term) << "left-open must exclude the lower bound term";
    ASSERT_EQ(hi_term, max_term) << "right-closed must include the upper bound term";
  }
}

// 退化范围：lo_term == hi_term，四种边界组合。
//   [5,5]  → 仅 term5，100 doc
//   [5,5)  → 空
//   (5,5]  → 空
//   (5,5)  → 空
TEST_F(TestInvSSTableRangeIter, range_degenerate_boundaries)
{
  ObInvSSTable inv_sstable;
  const int64_t term_cnt = 20;
  const int64_t docs_per_term = 100;
  build_multi_term_search_sstable(term_cnt, docs_per_term, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  const int64_t target_term = 5;

  // [5,5] → 仅 term5
  {
    char lo_buf[32] = {0};
    char hi_buf[32] = {0};
    ObStorageDatum lo_datum;
    ObStorageDatum hi_datum;
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    ObDatumRange scan_range;
    build_term_range_from_seq(target_term, target_term, true /*left_closed*/, true /*right_closed*/,
        lo_buf, sizeof(lo_buf), hi_buf, sizeof(hi_buf),
        lo_datum, hi_datum, lo_key, hi_key, scan_range);
    std::vector<std::pair<int64_t, uint64_t>> results;
    int64_t min_term = 0;
    int64_t max_term = 0;
    run_range_scan_collect(inv_sstable, read_info, scan_range, results, min_term, max_term);
    ASSERT_EQ(docs_per_term, static_cast<int64_t>(results.size()));
    ASSERT_EQ(target_term, min_term);
    ASSERT_EQ(target_term, max_term);
  }

  // [5,5) → 空
  {
    char lo_buf[32] = {0};
    char hi_buf[32] = {0};
    ObStorageDatum lo_datum;
    ObStorageDatum hi_datum;
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    ObDatumRange scan_range;
    build_term_range_from_seq(target_term, target_term, true /*left_closed*/, false /*right_closed*/,
        lo_buf, sizeof(lo_buf), hi_buf, sizeof(hi_buf),
        lo_datum, hi_datum, lo_key, hi_key, scan_range);
    ObQueryFlag query_flag;
    query_flag.set_use_block_cache();
    ObTabletID tablet_id(TABLET_ID);
    ObInvRangeScanParam param;
    build_range_scan_param(scan_range, tablet_id, query_flag, allocator_, param);
    ObInvSSTableRangeIter iter;
    OK(iter.open(param, read_info, inv_sstable));
    ASSERT_TRUE(iter.is_end());
    ASSERT_EQ(OB_ITER_END, iter.next());
  }

  // (5,5] → 空
  {
    char lo_buf[32] = {0};
    char hi_buf[32] = {0};
    ObStorageDatum lo_datum;
    ObStorageDatum hi_datum;
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    ObDatumRange scan_range;
    build_term_range_from_seq(target_term, target_term, false /*left_closed*/, true /*right_closed*/,
        lo_buf, sizeof(lo_buf), hi_buf, sizeof(hi_buf),
        lo_datum, hi_datum, lo_key, hi_key, scan_range);
    ObQueryFlag query_flag;
    query_flag.set_use_block_cache();
    ObTabletID tablet_id(TABLET_ID);
    ObInvRangeScanParam param;
    build_range_scan_param(scan_range, tablet_id, query_flag, allocator_, param);
    ObInvSSTableRangeIter iter;
    OK(iter.open(param, read_info, inv_sstable));
    ASSERT_TRUE(iter.is_end());
    ASSERT_EQ(OB_ITER_END, iter.next());
  }

  // (5,5) → 空
  {
    char lo_buf[32] = {0};
    char hi_buf[32] = {0};
    ObStorageDatum lo_datum;
    ObStorageDatum hi_datum;
    ObDatumRowkey lo_key;
    ObDatumRowkey hi_key;
    ObDatumRange scan_range;
    build_term_range_from_seq(target_term, target_term, false /*left_closed*/, false /*right_closed*/,
        lo_buf, sizeof(lo_buf), hi_buf, sizeof(hi_buf),
        lo_datum, hi_datum, lo_key, hi_key, scan_range);
    ObQueryFlag query_flag;
    query_flag.set_use_block_cache();
    ObTabletID tablet_id(TABLET_ID);
    ObInvRangeScanParam param;
    build_range_scan_param(scan_range, tablet_id, query_flag, allocator_, param);
    ObInvSSTableRangeIter iter;
    OK(iter.open(param, read_info, inv_sstable));
    ASSERT_TRUE(iter.is_end());
    ASSERT_EQ(OB_ITER_END, iter.next());
  }
}

// end key 位于两个实际 term 之间（"term0000000014x" 严格大于 term14、严格小于 term15）：
// 右开时 term14 包含、term15 排除；右闭时同样 term14 包含、term15 排除（因为
// term15 的 endkey > "term14x"）。验证 end key 不必命中实际 term。
TEST_F(TestInvSSTableRangeIter, range_end_key_between_terms)
{
  ObInvSSTable inv_sstable;
  const int64_t term_cnt = 20;
  const int64_t docs_per_term = 100;
  build_multi_term_search_sstable(term_cnt, docs_per_term, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);

  // start = term5 (closed), end = "term0000000014x" (between term14 and term15).
  char lo_buf[32] = {0};
  char hi_buf[32] = {0};
  format_term(5, lo_buf, sizeof(lo_buf));
  snprintf(hi_buf, sizeof(hi_buf), "term%010ldx", static_cast<long>(14));
  ObStorageDatum lo_datum;
  ObStorageDatum hi_datum;
  ObDatumRowkey lo_key;
  ObDatumRowkey hi_key;
  build_term_rowkey(lo_buf, static_cast<int32_t>(strlen(lo_buf)), lo_datum, lo_key);
  build_term_rowkey(hi_buf, static_cast<int32_t>(strlen(hi_buf)), hi_datum, hi_key);

  // 右开
  {
    ObDatumRange scan_range;
    scan_range.set_start_key(lo_key);
    scan_range.set_end_key(hi_key);
    scan_range.set_left_closed();
    scan_range.set_right_open();
    std::vector<std::pair<int64_t, uint64_t>> results;
    int64_t min_term = 0;
    int64_t max_term = 0;
    run_range_scan_collect(inv_sstable, read_info, scan_range, results, min_term, max_term);
    // term5..term14 inclusive: 10 terms × 100 docs
    ASSERT_EQ(10 * docs_per_term, static_cast<int64_t>(results.size()));
    ASSERT_EQ(5, min_term);
    ASSERT_EQ(14, max_term);
  }

  // 右闭
  {
    ObDatumRange scan_range;
    scan_range.set_start_key(lo_key);
    scan_range.set_end_key(hi_key);
    scan_range.set_left_closed();
    scan_range.set_right_closed();
    std::vector<std::pair<int64_t, uint64_t>> results;
    int64_t min_term = 0;
    int64_t max_term = 0;
    run_range_scan_collect(inv_sstable, read_info, scan_range, results, min_term, max_term);
    // term15 endkey "term15..." > "term14x", so term15 excluded; same as right-open.
    ASSERT_EQ(10 * docs_per_term, static_cast<int64_t>(results.size()));
    ASSERT_EQ(5, min_term);
    ASSERT_EQ(14, max_term);
  }
}

TEST_F(TestInvSSTableRangeIter, doc_id_drain_covers_flat_cache_groups)
{
  ObInvSSTable inv_sstable;
  build_multi_term_search_sstable(3 /*term_cnt*/, 257 /*docs_per_term*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObInvRangeScanParam param;
  build_range_scan_param(
      whole_range, ObTabletID(TABLET_ID), query_flag, allocator_, param);
  param.enable_doc_id_drain_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  std::vector<uint64_t> actual;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == ret) {
    const uint64_t *doc_ids = nullptr;
    int64_t count = 0;
    ret = iter.get_next_doc_ids(17 /*capacity*/, doc_ids, count);
    if (OB_SUCCESS == ret) {
      ASSERT_NE(nullptr, doc_ids);
      ASSERT_GT(count, 0);
      ASSERT_LE(count, 17);
      actual.insert(actual.end(), doc_ids, doc_ids + count);
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_TRUE(iter.is_end());

  std::vector<uint64_t> expected;
  expected.reserve(oracle_.size());
  for (int64_t i = 0; i < static_cast<int64_t>(oracle_.size()); ++i) {
    expected.push_back(oracle_[i].second);
  }
  std::sort(actual.begin(), actual.end());
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(expected, actual);

  uint64_t sentinel = 1;
  const uint64_t *doc_ids = &sentinel;
  int64_t count = -1;
  EXPECT_EQ(OB_ITER_END,
      iter.get_next_doc_ids(17, doc_ids, count));
  EXPECT_EQ(nullptr, doc_ids);
  EXPECT_EQ(0, count);
}

TEST_F(TestInvSSTableRangeIter, doc_id_drain_obeys_capacity_beyond_256)
{
  static const int64_t DOC_COUNT = 600;
  static const int64_t CAPACITY = 512;
  ObInvSSTable inv_sstable;
  build_single_term_search_sstable(
      DOC_COUNT, 1 /*start*/, 1 /*stride*/, 1 /*term_seq*/, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObInvRangeScanParam param;
  build_range_scan_param(
      whole_range, ObTabletID(TABLET_ID), query_flag, allocator_, param);
  param.enable_doc_id_drain_ = true;

  ObInvSSTableRangeIter iter;
  OK(iter.open(param, read_info, inv_sstable));
  const uint64_t *doc_ids = nullptr;
  int64_t count = 0;
  OK(iter.get_next_doc_ids(CAPACITY, doc_ids, count));
  ASSERT_NE(nullptr, doc_ids);
  ASSERT_EQ(CAPACITY, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(oracle_[i].second, doc_ids[i]);
  }

  OK(iter.get_next_doc_ids(CAPACITY, doc_ids, count));
  ASSERT_NE(nullptr, doc_ids);
  ASSERT_EQ(DOC_COUNT - CAPACITY, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(oracle_[CAPACITY + i].second, doc_ids[i]);
  }
  EXPECT_EQ(OB_ITER_END, iter.get_next_doc_ids(CAPACITY, doc_ids, count));
}

TEST_F(TestInvSSTableRangeIter, index_row_group_matches_physical_block_runs)
{
  static const int64_t TERM_COUNT = 2000;
  ObInvSSTable inv_sstable;
  build_multi_term_search_sstable(
      TERM_COUNT, 1 /*docs_per_term*/, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObSSTableIndexScanParam scan_param;
  OK(scan_param.init(read_info, query_flag, ObTabletID(TABLET_ID),
      ObSSTableIndexScanParam::ScanLevel::LEAF));

  struct GroupShape
  {
    MacroBlockId macro_id_;
    uint64_t block_offset_;
    uint64_t block_size_;
    int64_t row_cnt_;
  };
  std::vector<GroupShape> expected;
  bool has_multi_row_group = false;
  {
    ObArenaAllocator scan_allocator;
    ObSSTableIndexBaseScanner scanner;
    OK(scanner.init(whole_range, scan_param, inv_sstable, scan_allocator));
    ObMicroIndexInfo row;
    GroupShape current;
    bool has_current = false;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = scanner.get_next(row))) {
      const bool same_block = has_current
          && current.macro_id_ == row.get_macro_id()
          && current.block_offset_ == row.get_block_offset()
          && current.block_size_ == row.get_block_size();
      if (same_block) {
        ++current.row_cnt_;
      } else {
        if (has_current) {
          has_multi_row_group = has_multi_row_group || current.row_cnt_ > 1;
          expected.push_back(current);
        }
        current.macro_id_ = row.get_macro_id();
        current.block_offset_ = row.get_block_offset();
        current.block_size_ = row.get_block_size();
        current.row_cnt_ = 1;
        has_current = true;
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    if (has_current) {
      has_multi_row_group = has_multi_row_group || current.row_cnt_ > 1;
      expected.push_back(current);
    }
  }
  ASSERT_TRUE(has_multi_row_group);
  ObArenaAllocator group_allocator;
  ObSSTableIndexBaseScanner group_scanner;
  OK(group_scanner.init(whole_range, scan_param, inv_sstable, group_allocator));
  ObIndexRowGroup group;
  for (int64_t i = 0; i < static_cast<int64_t>(expected.size()); ++i) {
    OK(group_scanner.get_next_block_group(group));
    ASSERT_TRUE(group.is_valid());
    EXPECT_FALSE(group.need_agg_range_);
    EXPECT_EQ(expected[i].row_cnt_, group.row_cnt_);
    EXPECT_EQ(expected[i].macro_id_, group.first_row_.get_macro_id());
    EXPECT_EQ(expected[i].block_offset_, group.first_row_.get_block_offset());
    EXPECT_EQ(expected[i].block_size_, group.first_row_.get_block_size());
    EXPECT_EQ(group.first_row_.get_macro_id(), group.last_row_.get_macro_id());
    EXPECT_EQ(group.first_row_.get_block_offset(), group.last_row_.get_block_offset());
    EXPECT_EQ(group.first_row_.get_block_size(), group.last_row_.get_block_size());
    if (group.row_cnt_ > 1) {
      int cmp_ret = 0;
      OK(group.first_row_.endkey_.compare(group.last_row_.endkey_, read_info.get_datum_utils(), cmp_ret));
      EXPECT_LT(cmp_ret, 0);
    }
  }
  EXPECT_EQ(OB_ITER_END, group_scanner.get_next_block_group(group));
}

TEST_F(TestInvSSTableRangeIter, block_group_preserves_exclusive_upper_bound_for_transformed_and_raw)
{
  static const int64_t TERM_COUNT = 8;
  static const int64_t DOCS_PER_TERM = 64;
  static const int64_t FIRST_TERM = 3;
  static const int64_t LAST_TERM = 5;
  ObInvSSTable inv_sstable;
  build_multi_term_search_sstable(TERM_COUNT, DOCS_PER_TERM, inv_sstable);

  ObTableReadInfo read_info;
  build_rowkey_read_info(table_schema_, allocator_, read_info);
  char lo_buf[32] = {0};
  char hi_buf[32] = {0};
  ObStorageDatum lo_datum;
  ObStorageDatum hi_datum;
  ObDatumRowkey lo_key;
  ObDatumRowkey hi_key;
  ObDatumRange scan_range;
  build_term_range_from_seq(FIRST_TERM, LAST_TERM,
      true /*left_closed*/, false /*right_closed*/, lo_buf, sizeof(lo_buf),
      hi_buf, sizeof(hi_buf), lo_datum, hi_datum, lo_key, hi_key, scan_range);
  std::vector<uint64_t> expected;
  for (int64_t i = 0; i < static_cast<int64_t>(oracle_.size()); ++i) {
    if (oracle_[i].first >= FIRST_TERM && oracle_[i].first < LAST_TERM) {
      expected.push_back(oracle_[i].second);
    }
  }
  std::sort(expected.begin(), expected.end());
  const bool use_block_cache[] = {false, true, false};
  for (int64_t cache_mode = 0; cache_mode < ARRAYSIZEOF(use_block_cache); ++cache_mode) {
    ObQueryFlag query_flag;
    if (use_block_cache[cache_mode]) {
      query_flag.set_use_block_cache();
    } else {
      query_flag.set_not_use_block_cache();
    }
    if (!use_block_cache[cache_mode]) {
      ObSSTableIndexScanParam raw_scan_param;
      OK(raw_scan_param.init(
          read_info, query_flag, ObTabletID(TABLET_ID), ObSSTableIndexScanParam::ScanLevel::LEAF));
      ObArenaAllocator raw_group_allocator;
      ObSSTableIndexBaseScanner raw_group_scanner;
      OK(raw_group_scanner.init(scan_range, raw_scan_param, inv_sstable, raw_group_allocator));
      ObIndexRowGroup first_raw_group;
      OK(raw_group_scanner.get_next_block_group(first_raw_group));
      ASSERT_TRUE(first_raw_group.need_agg_range_);
    }
    ObInvRangeScanParam param;
    build_range_scan_param(
        scan_range, ObTabletID(TABLET_ID), query_flag, allocator_, param);
    param.enable_doc_id_drain_ = true;

    ObInvSSTableRangeIter iter;
    OK(iter.open(param, read_info, inv_sstable));
    std::vector<uint64_t> actual;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == ret) {
      const uint64_t *doc_ids = nullptr;
      int64_t count = 0;
      ret = iter.get_next_doc_ids(17 /*capacity*/, doc_ids, count);
      if (OB_SUCCESS == ret) {
        actual.insert(actual.end(), doc_ids, doc_ids + count);
      }
    }
    ASSERT_EQ(OB_ITER_END, ret);
    std::sort(actual.begin(), actual.end());
    EXPECT_EQ(expected, actual) << "use_block_cache=" << use_block_cache[cache_mode];
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_sstable_range_iter.log*");
  OB_LOGGER.set_file_name("test_inv_sstable_range_iter.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  const int ret = RUN_ALL_TESTS();
  std::_Exit(ret);
}
