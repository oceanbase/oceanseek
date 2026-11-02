/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "test_inv_idx_sstable_prepare.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_tree_cursor.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/access/ob_table_read_info.h"

namespace oceanbase
{
namespace unittest
{

class MacroIteratorGuard
{
public:
  MacroIteratorGuard(common::ObIAllocator &allocator, blocksstable::ObIMacroBlockIterator *&iter)
    : allocator_(allocator), iter_(iter)
  {}
  ~MacroIteratorGuard()
  {
    if (nullptr != iter_) {
      iter_->~ObIMacroBlockIterator();
      allocator_.free(iter_);
      iter_ = nullptr;
    }
  }
private:
  common::ObIAllocator &allocator_;
  blocksstable::ObIMacroBlockIterator *&iter_;
};

// Build an inverted-index sstable that carries a position list.
TEST_F(TestInvIdxSSTablePrepare, build_inv_sstable_with_position_list)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_TRUE(inv_sstable.get_inv_meta().get_stats().has_position_list_);
}

// Build an inverted-index sstable without a position list (phrase queries disabled).
// Exercises the writer's posting-value-skipping path end to end.
TEST_F(TestInvIdxSSTablePrepare, build_inv_sstable_without_position_list)
{
  const bool has_pos_list = false;
  const int64_t row_cnt = 100000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_FALSE(inv_sstable.get_inv_meta().get_stats().has_position_list_);
}

// Row count is configurable: a smaller sstable still builds and validates.
TEST_F(TestInvIdxSSTablePrepare, build_inv_sstable_with_configurable_row_count)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 5000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_TRUE(inv_sstable.get_inv_meta().get_stats().has_position_list_);
}

// After building an inverted-index sstable, use the secondary meta iterator to scan
// all data macro block metas. Verifies that the scan completes (OB_ITER_END) and
// returns at least one valid ObDataMacroBlockMeta entry.
TEST_F(TestInvIdxSSTablePrepare, scan_inv_sstable_with_sec_meta_iterator)
{
  // --- build the sstable ---
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  // --- build ObTableReadInfo from the schema ---
  // ObTableReadInfo::init() derives multi-version extra rowkey columns (trans_version,
  // sql_seq) internally from schema_rowkey_cnt, so passing the schema cols is correct.
  common::ObSEArray<ObColDesc, 8> schema_cols;
  OK(table_schema_.get_column_ids(schema_cols));

  ObTableReadInfo rowkey_read_info;
  OK(rowkey_read_info.init(
      allocator_,
      table_schema_.get_column_count(),
      table_schema_.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      schema_cols,
      nullptr /*storage_cols_index*/));
  ASSERT_TRUE(rowkey_read_info.is_valid());

  // --- open the secondary meta iterator over the whole key range ---
  ObDatumRange whole_range;
  whole_range.set_whole_range();

  ObSSTableSecMetaIterator meta_iter;
  OK(meta_iter.open(
      whole_range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      inv_sstable,
      rowkey_read_info,
      allocator_));

  // --- drain the iterator, count results ---
  int64_t meta_cnt = 0;
  ObDataMacroBlockMeta macro_meta;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = meta_iter.get_next(macro_meta))) {
    ASSERT_TRUE(macro_meta.is_valid());
    ++meta_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GT(meta_cnt, 0);
  STORAGE_LOG(INFO, "scanned inv sstable macro block metas",
      K(meta_cnt), K(inv_sstable.get_data_macro_block_count()));
}

// Scan the real, non-empty INV SSTable through the public macro-only path. This
// proves the newly supported combination does not merely pass an empty-table
// short circuit: it traverses the standard macro index tree and returns every
// data macro with the writer's document-row count semantics.
TEST_F(TestInvIdxSSTablePrepare, scan_inv_sstable_macro_only)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 100000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  common::ObSEArray<ObColDesc, 8> schema_cols;
  OK(table_schema_.get_column_ids(schema_cols));
  ObTableReadInfo rowkey_read_info;
  OK(rowkey_read_info.init(
      allocator_, table_schema_.get_column_count(),
      table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(),
      schema_cols, nullptr /* storage_cols_index */));

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObIMacroBlockIterator *iter = nullptr;
  MacroIteratorGuard iter_guard(allocator_, iter);
  OK(inv_sstable.scan_macro_block(
      whole_range, rowkey_read_info, allocator_, iter,
      false /* is_reverse_scan */, false /* need_record_micro_info */,
      false /* need_scan_sec_meta */));
  ASSERT_NE(nullptr, iter);

  int ret = OB_SUCCESS;
  int64_t macro_cnt = 0;
  int64_t total_row_cnt = 0;
  ObMacroBlockDesc macro_desc;
  while (OB_SUCC(ret = iter->get_next_macro_block(macro_desc))) {
    ASSERT_TRUE(macro_desc.is_valid());
    ASSERT_TRUE(macro_desc.macro_block_id_.is_valid());
    ASSERT_GT(macro_desc.row_count_, 0);
    total_row_cnt += macro_desc.row_count_;
    ++macro_cnt;
    macro_desc.reuse();
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(inv_sstable.get_data_macro_block_count(), macro_cnt);
  ASSERT_EQ(row_cnt, total_row_cnt);
}

// Exercise the exact macro_info path: forward scan, no standard micro info,
// and DATA_BLOCK_META pairing. Besides macro/meta identity, the empty arrays
// lock down that ObDualMacroMetaIterator honors record=false for INV SSTables.
TEST_F(TestInvIdxSSTablePrepare, scan_inv_sstable_macro_with_data_meta)
{
  const bool has_pos_list = true;
  const int64_t row_cnt = 100000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  common::ObSEArray<ObColDesc, 8> schema_cols;
  OK(table_schema_.get_column_ids(schema_cols));
  ObTableReadInfo rowkey_read_info;
  OK(rowkey_read_info.init(
      allocator_, table_schema_.get_column_count(),
      table_schema_.get_rowkey_column_num(), lib::is_oracle_mode(),
      schema_cols, nullptr /* storage_cols_index */));

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObIMacroBlockIterator *iter = nullptr;
  MacroIteratorGuard iter_guard(allocator_, iter);
  OK(inv_sstable.scan_macro_block(
      whole_range, rowkey_read_info, allocator_, iter,
      false /* is_reverse_scan */, false /* need_record_micro_info */,
      true /* need_scan_sec_meta */));
  ASSERT_NE(nullptr, iter);

  int ret = OB_SUCCESS;
  int64_t macro_cnt = 0;
  ObDataMacroBlockMeta macro_meta;
  ObMacroBlockDesc macro_desc;
  macro_desc.macro_meta_ = &macro_meta;
  while (OB_SUCC(ret = iter->get_next_macro_block(macro_desc))) {
    ASSERT_TRUE(macro_desc.is_valid());
    ASSERT_TRUE(macro_desc.is_valid_with_macro_meta());
    ASSERT_EQ(macro_desc.macro_block_id_, macro_meta.get_macro_id());
    ASSERT_EQ(INV_IDX_ROW_STORE, macro_meta.val_.row_store_type_);
    ASSERT_EQ(index_builder_->index_store_desc_.get_desc().get_row_store_type(),
        macro_desc.row_store_type_);
    ++macro_cnt;
    macro_desc.reuse();
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(inv_sstable.get_data_macro_block_count(), macro_cnt);
}

// Build a source inverted-index sstable, scan all its data macro block metas via
// ObSSTableSecMetaIterator, then use ObIndexBlockRebuilder to reindex those metas
// into a brand-new ObInvSSTable. Validates:
//   - every source macro meta is accepted without error,
//   - the rebuilt sstable is valid with the same macro block count as the source,
//   - the inverted-index extended metadata is preserved.
TEST_F(TestInvIdxSSTablePrepare, rebuild_inv_sstable_with_index_block_rebuilder)
{
  // ---- 1. build source sstable ----
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable src_sstable;
  build_inv_sstable(has_pos_list, row_cnt, src_sstable);
  ASSERT_TRUE(src_sstable.is_valid());
  const int64_t src_macro_cnt = src_sstable.get_data_macro_block_count();
  ASSERT_GT(src_macro_cnt, 0);

  // ---- 2. build ObTableReadInfo for secondary meta scan ----
  common::ObSEArray<ObColDesc, 8> schema_cols;
  OK(table_schema_.get_column_ids(schema_cols));
  ObTableReadInfo rowkey_read_info;
  OK(rowkey_read_info.init(
      allocator_,
      table_schema_.get_column_count(),
      table_schema_.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      schema_cols,
      nullptr /*storage_cols_index*/));
  ASSERT_TRUE(rowkey_read_info.is_valid());

  // ---- 3. create a fresh ObSSTableIndexBuilder for the rebuild ----
  // whole_desc_ retains the schema/desc configuration from build_inv_sstable().
  void *rebuild_builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  ASSERT_NE(nullptr, rebuild_builder_buf);
  ObSSTableIndexBuilder *rebuild_index_builder =
      new (rebuild_builder_buf) ObSSTableIndexBuilder(false /*not need writer buffer*/);
  ObDataStoreDesc &rebuild_desc = whole_desc_.get_desc();
  OK(rebuild_index_builder->init(rebuild_desc));
  rebuild_index_builder->set_is_async_index(true);
  rebuild_desc.sstable_index_builder_ = rebuild_index_builder;

  // ---- 4. init the index block rebuilder ----
  ObMacroSeqParam rebuild_seq_param;
  rebuild_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  rebuild_seq_param.start_ = 0;
  ObIndexBlockRebuilder rebuilder;
  OK(rebuilder.init(*rebuild_index_builder, rebuild_seq_param,
      nullptr /*task_idx*/, ObITable::TableKey()));

  // ---- 5. scan the source sstable and feed each macro meta into the rebuilder ----
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObSSTableSecMetaIterator meta_iter;
  OK(meta_iter.open(
      whole_range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      src_sstable,
      rowkey_read_info,
      allocator_));

  int64_t appended_cnt = 0;
  ObDataMacroBlockMeta macro_meta;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = meta_iter.get_next(macro_meta))) {
    ASSERT_TRUE(macro_meta.is_valid());
    OK(rebuilder.append_macro_row(macro_meta));
    ++appended_cnt;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(src_macro_cnt, appended_cnt);

  // ---- 6. close rebuilder, then finalize the index builder ----
  OK(rebuilder.close());
  ObSSTableMergeRes rebuild_res;
  OK(rebuild_index_builder->close(rebuild_res));
  ASSERT_TRUE(rebuild_res.root_desc_.is_valid());
  ASSERT_EQ(row_cnt, rebuild_res.row_count_);

  // ---- 7. verify the rebuilder aggregated inverted metadata, then build the new sstable ----
  const ObInvIdxStatsMeta &src_inv_meta = src_sstable.get_inv_meta().get_stats();
  ASSERT_TRUE(rebuild_res.inv_meta_.is_valid());
  ASSERT_EQ(src_inv_meta.index_type_, rebuild_res.inv_meta_.get_stats().index_type_);
  ASSERT_EQ(src_inv_meta.has_relevance_params_, rebuild_res.inv_meta_.get_stats().has_relevance_params_);
  ASSERT_EQ(src_inv_meta.has_position_list_, rebuild_res.inv_meta_.get_stats().has_position_list_);
  ASSERT_EQ(src_inv_meta.has_nested_key_, rebuild_res.inv_meta_.get_stats().has_nested_key_);
  ASSERT_EQ(src_inv_meta.doc_id_obj_meta_, rebuild_res.inv_meta_.get_stats().doc_id_obj_meta_);
  ASSERT_EQ(src_inv_meta.min_doc_id_, rebuild_res.inv_meta_.get_stats().min_doc_id_);
  ASSERT_EQ(src_inv_meta.max_doc_id_, rebuild_res.inv_meta_.get_stats().max_doc_id_);
  ASSERT_EQ(src_inv_meta.total_doc_length_,
      rebuild_res.inv_meta_.get_stats().total_doc_length_);
  ObInvSSTable rebuilt_sstable;
  finalize_inv_sstable_from_res(rebuild_res, rebuilt_sstable);

  // ---- 8. validate the rebuilt sstable ----
  ASSERT_TRUE(rebuilt_sstable.is_inv_sstable());
  ASSERT_TRUE(rebuilt_sstable.is_valid());
  ASSERT_EQ(src_macro_cnt, rebuilt_sstable.get_data_macro_block_count());
  ASSERT_TRUE(rebuilt_sstable.get_inv_meta().get_stats().is_valid());
  ASSERT_TRUE(rebuilt_sstable.get_inv_meta().get_stats().is_fts_index());
  ASSERT_EQ(has_pos_list, rebuilt_sstable.get_inv_meta().get_stats().has_position_list_);
  ASSERT_EQ(src_sstable.get_inv_meta().get_stats().min_doc_id_, rebuilt_sstable.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(src_sstable.get_inv_meta().get_stats().max_doc_id_, rebuilt_sstable.get_inv_meta().get_stats().max_doc_id_);
  STORAGE_LOG(INFO, "rebuilt inv sstable via index block rebuilder",
      K(src_macro_cnt), K(appended_cnt), K(row_cnt),
      "rebuilt_macro_cnt", rebuilt_sstable.get_data_macro_block_count());

  // cleanup — must follow finalize_inv_sstable_from_res so rebuild_res stays valid
  rebuild_index_builder->~ObSSTableIndexBuilder();
  rebuild_index_builder = nullptr;
}

// After building an inverted-index sstable, walk its index block tree down to the leaf
// layer with ObIndexBlockTreeCursor and verify the posting index block (the leaf index
// rows, one per (data micro block M, term T) pair) carries correct data:
//   - each leaf row's index row header and inverted-index agg payload are self-consistent;
//   - the writer's logical-block-grouping invariant holds: for consecutive leaf rows,
//     either the term differs, or they belong to different data micro blocks (a same-term
//     row in the same micro block would have been merged into one logical-block group).
TEST_F(TestInvIdxSSTablePrepare, scan_inv_sstable_leaf_rows_with_tree_cursor)
{
  // --- build the sstable ---
  const bool has_pos_list = true;
  const int64_t row_cnt = 1000000;
  ObInvSSTable inv_sstable;
  build_inv_sstable(has_pos_list, row_cnt, inv_sstable);
  ASSERT_TRUE(inv_sstable.is_valid());

  // --- build ObTableReadInfo from the schema (same recipe as scan_inv_sstable_*) ---
  common::ObSEArray<ObColDesc, 8> schema_cols;
  OK(table_schema_.get_column_ids(schema_cols));
  ObTableReadInfo rowkey_read_info;
  OK(rowkey_read_info.init(
      allocator_,
      table_schema_.get_column_count(),
      table_schema_.get_rowkey_column_num(),
      lib::is_oracle_mode(),
      schema_cols,
      nullptr /*storage_cols_index*/));
  ASSERT_TRUE(rowkey_read_info.is_valid());

  // --- open the index block tree cursor over the whole key range ---
  ObIndexBlockTreeCursor tree_cursor;
  OK(tree_cursor.init(inv_sstable, allocator_, &rowkey_read_info));

  bool is_beyond_range = false;
  OK(tree_cursor.drill_down(
      ObDatumRowkey::MIN_ROWKEY, ObIndexBlockTreeCursor::LEAF, is_beyond_range));
  ASSERT_FALSE(is_beyond_range);

  // --- iterate the leaf layer row by row ---
  // The leaf rows are the inverted-index "posting index" rows: each is a data-block row
  // (is_data_block_=true) carrying an inverted-index agg payload (is_inverted_index_).
  // For each row we record a snapshot used for cross-row invariant checks.
  struct LeafRowInfo
  {
    bool valid_;
    ObString term_;            // deep-copied into allocator_
    uint64_t max_doc_id_;
    MacroBlockId macro_id_;
    int64_t block_offset_;
    int64_t block_size_;
    uint64_t first_entry_min_doc_id_;
    uint64_t last_entry_max_doc_id_;
    int64_t entry_count_;
    LeafRowInfo() : valid_(false), term_(), max_doc_id_(0), macro_id_(),
        block_offset_(0), block_size_(0),
        first_entry_min_doc_id_(0), last_entry_max_doc_id_(0), entry_count_(0) {}
  } prev;

  int64_t leaf_row_cnt = 0;
  int64_t total_entry_doc_count = 0;
  uint64_t total_entry_doc_length = 0;
  uint64_t global_min_doc_id = UINT64_MAX;
  uint64_t global_max_doc_id = 0;
  int ret = OB_SUCCESS;
  const ObIndexBlockRowParser *parser = nullptr;
  const ObIndexBlockRowHeader *header = nullptr;
  ObDatumRowkey endkey;

  while (OB_SUCC(ret)) {
    // --- read the current leaf row's components ---
    OK(tree_cursor.get_idx_row_header(header));
    ASSERT_NE(nullptr, header);
    OK(tree_cursor.get_idx_parser(parser));
    OK(tree_cursor.get_current_endkey(endkey, /*get_schema_rowkey=*/true));
    // schema rowkey is (term, doc_id): datum[0]=term, datum[1]=max_doc_id.
    ASSERT_EQ(2, endkey.get_datum_cnt());
    const ObString term_src = endkey.datums_[0].get_string();
    const uint64_t max_doc_id = endkey.datums_[1].get_uint();

    MacroBlockId macro_id;
    OK(tree_cursor.get_macro_block_id(macro_id));
    const int64_t block_offset = header->get_block_offset();
    const int64_t block_size = header->get_block_size();

    // --- per-row structural checks ---
    // 1. leaf-row header flags + non-empty posting-data locator.
    ASSERT_TRUE(header->is_data_block()) << "leaf row must be a data-block row";
    ASSERT_TRUE(header->is_inverted_index()) << "leaf row must carry inv-index agg";
    ASSERT_EQ(1, header->get_micro_block_count());
    ASSERT_GT(block_offset, 0);
    ASSERT_GT(block_size, 0);
    ASSERT_TRUE(macro_id.is_valid());

    // 2/3. parse the inverted-index agg payload and validate its layout, as written by
    // ObInvIdxAggEncoder::write_payload (header + entries + frontier-pair pool).
    const ObIndexBlockRowMinorMetaInfo *minor_meta = nullptr;
    const char *agg_buf = nullptr;
    int64_t agg_size = 0;
    OK(parser->parse_minor_meta_and_agg_row(minor_meta, agg_buf, agg_size));
    ASSERT_NE(nullptr, agg_buf);
    ASSERT_GT(agg_size, 0);

    const ObInvIdxAggDataHeader *agg_header =
        reinterpret_cast<const ObInvIdxAggDataHeader *>(agg_buf);
    // Read VERSION by value (a static const integral class member has no out-of-line
    // definition, so taking its address — as ASSERT_EQ's const& args would — fails to link).
    const uint8_t agg_version = ObInvIdxAggDataHeader::VERSION;
    ASSERT_EQ(agg_version, agg_header->version_);
    const int64_t entry_cnt = agg_header->logical_block_count_;
    ASSERT_GT(entry_cnt, 0);
    ASSERT_EQ(agg_size, agg_header->total_size_)
        << "agg payload total_size must equal the parsed buffer size";

    const char *entries_buf = agg_buf + sizeof(ObInvIdxAggDataHeader);
    const char *frontier_pool = entries_buf + entry_cnt * sizeof(ObInvIdxAggDataEntry);
    const int64_t frontier_pool_size = agg_size - sizeof(ObInvIdxAggDataHeader)
        - entry_cnt * sizeof(ObInvIdxAggDataEntry);

    // 5. posting-value locator recorded in the agg header: with a position list the
    // posting-value micro block immediately follows the posting-data micro block, so its
    // offset is block_offset + block_size; without a position list it is absent (0/0).
    if (has_pos_list) {
      ASSERT_EQ(block_offset + block_size, agg_header->posting_value_offset_);
      ASSERT_GT(agg_header->posting_value_size_, 0);
    } else {
      ASSERT_EQ(0, agg_header->posting_value_offset_);
      ASSERT_EQ(0, agg_header->posting_value_size_);
    }

    // walk entries: validate each, accumulate frontier-pool offsets, enforce doc-id order.
    int64_t expect_frontier_offset = 0;
    uint64_t row_first_min_doc_id = 0;
    uint64_t row_last_max_doc_id = 0;
    int64_t row_doc_count_sum = 0;
    for (int64_t i = 0; i < entry_cnt; ++i) {
      const ObInvIdxAggDataEntry *e =
          reinterpret_cast<const ObInvIdxAggDataEntry *>(entries_buf + i * sizeof(ObInvIdxAggDataEntry));
      // 3. per-entry sanity.
      ASSERT_GT(e->doc_count_, 0);
      ASSERT_LE(e->min_doc_id_, e->max_doc_id_);
      ASSERT_GT(e->total_doc_length_, 0);
      ASSERT_EQ(expect_frontier_offset, e->frontier_offset_)
          << "frontier_offset must be contiguous across entries";
      ASSERT_GE(static_cast<int64_t>(e->frontier_offset_)
                + static_cast<int64_t>(e->frontier_count_) * static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair)),
          static_cast<int64_t>(e->frontier_offset_));  // no overflow
      // frontier region stays inside the pool.
      ASSERT_LE(static_cast<int64_t>(e->frontier_offset_)
                + static_cast<int64_t>(e->frontier_count_) * static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair)),
          frontier_pool_size);
      expect_frontier_offset += e->frontier_count_ * sizeof(ObInvIdxBM25ParamPair);

      // 4. entries are doc-id-ordered and non-overlapping within the row.
      if (0 == i) {
        row_first_min_doc_id = e->min_doc_id_;
      } else {
        ASSERT_LT(row_last_max_doc_id, e->min_doc_id_)
            << "entries within one row must have strictly increasing doc-id ranges";
      }
      row_last_max_doc_id = e->max_doc_id_;
      row_doc_count_sum += e->doc_count_;
      total_entry_doc_length += e->total_doc_length_;
      total_entry_doc_count += e->doc_count_;
      if (e->min_doc_id_ < global_min_doc_id) { global_min_doc_id = e->min_doc_id_; }
      if (e->max_doc_id_ > global_max_doc_id) { global_max_doc_id = e->max_doc_id_; }
    }
    // frontier pool is exactly filled by the entries.
    ASSERT_EQ(frontier_pool_size, expect_frontier_offset);
    // the row's rowkey max_doc_id equals its last entry's max_doc_id.
    ASSERT_EQ(row_last_max_doc_id, max_doc_id);
    // row_count_ carries the (term, M) doc count: the sum of this row's logical-block doc counts.
    ASSERT_EQ(row_doc_count_sum, header->get_row_count());

    // --- cross-row invariants ---
    // deep-copy term so it survives the next move_forward (which overwrites row_).
    char *term_buf = static_cast<char *>(allocator_.alloc(term_src.length()));
    ASSERT_NE(nullptr, term_buf);
    MEMCPY(term_buf, term_src.ptr(), term_src.length());
    ObString term(term_src.length(), term_buf);

    if (prev.valid_) {
      // term is monotonically non-decreasing. The feeder emits zero-padded ASCII terms,
      // so byte order matches the datum_utils collation order used by the tree.
      ASSERT_LE(prev.term_.compare(term), 0)
          << "leaf row terms must be non-decreasing across the leaf layer";

      const bool same_term = (0 == prev.term_.compare(term));
      if (same_term) {
        // The required grouping invariant: consecutive rows sharing a term MUST belong to
        // different data micro blocks — otherwise the writer would have merged them into a
        // single logical-block group (one leaf row).
        const bool same_micro = (prev.macro_id_ == macro_id)
            && (prev.block_offset_ == block_offset);
        ASSERT_FALSE(same_micro)
            << "consecutive same-term leaf rows must be in different data micro blocks";
        // within one term, max_doc_id strictly increases.
        ASSERT_LT(prev.max_doc_id_, max_doc_id);
      } else {
        // across terms, doc-id ranges are disjoint and ordered (doc ids are globally
        // increasing in the feeder, so a later term owns strictly larger doc ids).
        ASSERT_LT(prev.last_entry_max_doc_id_, row_first_min_doc_id)
            << "later term must own strictly larger doc ids than the previous term";
      }
    }

    prev.valid_ = true;
    prev.term_ = term;
    prev.max_doc_id_ = max_doc_id;
    prev.macro_id_ = macro_id;
    prev.block_offset_ = block_offset;
    prev.block_size_ = block_size;
    prev.first_entry_min_doc_id_ = row_first_min_doc_id;
    prev.last_entry_max_doc_id_ = row_last_max_doc_id;
    prev.entry_count_ = entry_cnt;

    ++leaf_row_cnt;
    ret = tree_cursor.move_forward(false /*sequential*/);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_GT(leaf_row_cnt, 0);

  // --- end-to-end accounting ---
  // Every input (term, doc_id) row lands in exactly one logical block, so the sum of all
  // agg-entry doc counts equals the input row count.
  ASSERT_EQ(row_cnt, total_entry_doc_count);
  // The leaf-layer doc-id extremes match the sstable's inv extended meta.
  ASSERT_EQ(inv_sstable.get_inv_meta().get_stats().min_doc_id_, global_min_doc_id);
  ASSERT_EQ(inv_sstable.get_inv_meta().get_stats().max_doc_id_, global_max_doc_id);
  ASSERT_EQ(inv_sstable.get_inv_meta().get_stats().total_doc_length_, total_entry_doc_length);
  // A multi-macro sstable must carry strictly more leaf rows than macro blocks (each macro
  // block contributes its own inverted-index micro block holding >=1 leaf row).
  ASSERT_GT(leaf_row_cnt, inv_sstable.get_data_macro_block_count());
  STORAGE_LOG(INFO, "scanned inv sstable leaf rows with tree cursor",
      K(leaf_row_cnt), K(total_entry_doc_count),
      K(global_min_doc_id), K(global_max_doc_id),
      "macro_cnt", inv_sstable.get_data_macro_block_count());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_idx_sstable_basic.log*");
  OB_LOGGER.set_file_name("test_inv_idx_sstable_basic.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
