/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#define UNITTEST

#include "mtlenv/storage/blocksstable/test_inv_idx_sstable_prepare.h"
#include "storage/blocksstable/async_index/ob_inv_idx_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_clustered_index_block_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace unittest;

namespace storage
{

class ObInvIdxClusteredIndexTest : public TestInvIdxSSTablePrepare
{
protected:
  ObSSTablePrivateObjectCleaner cleaner_;

  void assert_clustered_block_ids_published(
      ObInvIdxIndexBlockBuilder &builder,
      const ObSSTableMergeRes &res)
  {
    ObMacroBlocksWriteCtx *ctx = builder.index_tree_root_ctx_->clustered_index_write_ctx_;
    ASSERT_NE(nullptr, ctx);
    const ObIArray<MacroBlockId> &clustered_block_ids = ctx->get_macro_block_list();
    ASSERT_GT(clustered_block_ids.count(), 0);
    ASSERT_EQ(clustered_block_ids.count(), res.other_block_ids_.count());
    for (int64_t i = 0; i < clustered_block_ids.count(); ++i) {
      EXPECT_EQ(clustered_block_ids.at(i), res.other_block_ids_.at(i))
          << "clustered block id must be transferred in writer order, index=" << i;
    }
  }

  void open_writer(const bool micro_index_clustered, ObInvIdxMacroBlockWriter &writer)
  {
    open_writer(micro_index_clustered, writer, ObRowStoreType::MAX_ROW_STORE, true /*has_pos_list*/);
  }

  // force_row_store_type overrides the data store desc row store type (MAX_ROW_STORE = keep default),
  // so the test can exercise each actual/trial index micro writer format. has_pos_list controls
  // posting size: a no-position-list index has tiny per-doc postings, letting the index rows drive
  // macro splitting.
  void open_writer(const bool micro_index_clustered, ObInvIdxMacroBlockWriter &writer,
                   const ObRowStoreType force_row_store_type, const bool has_pos_list)
  {
    prepare_inv_idx_schema(table_schema_, has_pos_list);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    table_schema_.set_micro_index_clustered(micro_index_clustered);
    prepare_desc(force_row_store_type);

    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner_, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());
  }

  // Feed strictly increasing unique-term rows (one distinct term per doc) into the writer, up to
  // max_rows. Every doc is its own term, so each committed micro block pair emits one n-1 index
  // row per doc — the fastest way to exercise the actual/trial index admission path. Stops at the
  // first failing append_row and returns its ret; fed_rows is the number of successfully appended
  // rows.
  int feed_unique_terms(ObInvIdxMacroBlockWriter &writer, const int64_t max_rows, int64_t &fed_rows)
  {
    int ret = OB_SUCCESS;
    ObDatumRow row;
    char term_buf[32];
    fed_rows = 0;
    if (OB_FAIL(row.init(mv_col_cnt_))) {
      STORAGE_LOG(WARN, "fail to init row", K(ret), K_(mv_col_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < max_rows; ++i) {
      const int n = snprintf(term_buf, sizeof(term_buf), "term%010ld", i);
      ObString term_str(n, term_buf);
      ObString empty_pos_list;
      if (OB_FAIL(make_inv_idx_row(row, inv_idx_row_desc_, mv_col_cnt_, term_str,
          static_cast<uint64_t>(i + 1) /*doc_id*/, 1 /*token_freq*/, 100 /*doc_length*/,
          empty_pos_list))) {
        STORAGE_LOG(WARN, "fail to make inv idx row", K(ret), K(i));
      } else if (OB_FAIL(writer.append_row(row))) {
        // Do not overwrite ret: this is the failure we want to surface to the caller.
      } else {
        ++fed_rows;
      }
    }
    return ret;
  }

  void feed_and_close(ObInvIdxMacroBlockWriter &writer, const int64_t row_cnt,
                       ObSSTableMergeRes &res)
  {
    ObInvIdxRowFeeder feeder(inv_idx_row_desc_, mv_col_cnt_);
    OK(feeder.feed_n(writer, row_cnt));
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());
    OK(index_builder_->close(res));
    ASSERT_TRUE(res.root_desc_.is_valid());
  }
};

// 1. open with micro_index_clustered=true and verify clustered writer is created.
TEST_F(ObInvIdxClusteredIndexTest, init_with_micro_index_clustered)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(true /*micro_index_clustered*/, writer);

  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;
  EXPECT_TRUE(builder.micro_index_clustered_);
  ASSERT_NE(nullptr, builder.clustered_index_writer_);

  OK(writer.close());

  ObSSTableMergeRes res;
  OK(index_builder_->close(res));
}

// 2. full cycle with micro_index_clustered=true: feed rows, verify clustered
//    blocks are produced and published into ObSSTableMergeRes::other_block_ids_
//    so their refcount gets registered like any other sstable's clustered blocks.
TEST_F(ObInvIdxClusteredIndexTest, full_cycle_with_clustered)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(true /*micro_index_clustered*/, writer);
  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;

  ObSSTableMergeRes res;
  feed_and_close(writer, 2000 /*row_cnt*/, res);

  EXPECT_GT(res.data_block_ids_.count(), 0)
      << "inverted data macro blocks should be produced";
  assert_clustered_block_ids_published(builder, res);
}

// 3. micro_index_clustered=false: no clustered writer created, other_block_ids_
//    empty, full cycle works unchanged.
TEST_F(ObInvIdxClusteredIndexTest, regression_no_clustered)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(false /*micro_index_clustered*/, writer);

  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;
  EXPECT_FALSE(builder.micro_index_clustered_);
  EXPECT_EQ(nullptr, builder.clustered_index_writer_);

  ObSSTableMergeRes res;
  feed_and_close(writer, 2000 /*row_cnt*/, res);

  EXPECT_EQ(0, res.other_block_ids_.count())
      << "micro_index_clustered=false should not produce clustered blocks";
}

// 4. After feed then close, the clustered write ctx in root is populated,
//    proving term-group-to-clustered-row append and close-transfer work.
TEST_F(ObInvIdxClusteredIndexTest, clustered_write_ctx_populated_after_close)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(true, writer);
  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;
  ASSERT_NE(nullptr, builder.clustered_index_writer_);

  ObInvIdxRowFeeder feeder(inv_idx_row_desc_, mv_col_cnt_);
  OK(feeder.feed_n(writer, 2000));
  OK(writer.close());
  ObSSTableMergeRes res;
  OK(index_builder_->close(res));

  ObMacroBlocksWriteCtx *ctx = builder.index_tree_root_ctx_->clustered_index_write_ctx_;
  ASSERT_NE(nullptr, ctx);
  EXPECT_GT(ctx->get_macro_block_count(), 0);
}

// 5. Higher row count triggers multiple data macro flushes. After
//    writer.close(), the clustered write ctx in root has been populated.
TEST_F(ObInvIdxClusteredIndexTest, clustered_write_ctx_after_multi_data_macro)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(true, writer);
  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;

  ObSSTableMergeRes res;
  feed_and_close(writer, 8000 /*row_cnt*/, res);

  EXPECT_GT(res.data_block_ids_.count(), 0)
      << "multiple inverted data macro blocks should be produced";
  assert_clustered_block_ids_published(builder, res);
}

// 6. A committed inverted n-1 row is paired with exactly one clustered index row. Trial rows
//    must not reach the clustered writer; after candidate commit, actual, trial and clustered
//    row counts stay synchronized. Sample before close so the final macro flush does not drain
//    the clustered micro writer.
TEST_F(ObInvIdxClusteredIndexTest, clustered_row_count_matches_n1_rows)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(true /*micro_index_clustered*/, writer);
  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;
  ASSERT_NE(nullptr, builder.actual_index_micro_writer_);
  ASSERT_NE(nullptr, builder.trial_index_micro_writer_);
  ASSERT_NE(nullptr, builder.clustered_index_writer_);

  ObInvIdxRowFeeder feeder(inv_idx_row_desc_, mv_col_cnt_);
  const int64_t feed_chunk = 5000;
  const int64_t max_feed_rows = 100000;
  for (int64_t fed = 0;
       fed < max_feed_rows && builder.actual_index_micro_writer_->get_row_count() == 0;
       fed += feed_chunk) {
    OK(feeder.feed_n(writer, feed_chunk));
  }
  const int64_t actual_row_count = builder.actual_index_micro_writer_->get_row_count();
  const int64_t trial_row_count = builder.trial_index_micro_writer_->get_row_count();
  const int64_t clustered_row_count =
      builder.clustered_index_writer_->micro_writer_->get_row_count();
  EXPECT_GT(actual_row_count, 0)
      << "inverted n-1 index rows should be pending after a mid-stream micro flush";
  EXPECT_EQ(actual_row_count, trial_row_count)
      << "actual and trial index writers must stay synchronized after commit";
  EXPECT_EQ(actual_row_count, builder.index_row_count_);
  EXPECT_EQ(actual_row_count, clustered_row_count)
      << "clustered row count must match committed inverted n-1 row count";

  OK(writer.close());
  ObSSTableMergeRes res;
  OK(index_builder_->close(res));
}

// 7. After close, the clustered micro writer has no residual rows: every clustered
//    row was flushed with its data macro. The actual/trial writers retain the final
//    macro state and remain synchronized until the builder is reset.
TEST_F(ObInvIdxClusteredIndexTest, clustered_writer_drained_after_close)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(true /*micro_index_clustered*/, writer);
  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;
  ASSERT_NE(nullptr, builder.clustered_index_writer_);

  ObSSTableMergeRes res;
  feed_and_close(writer, 2000 /*row_cnt*/, res);

  ASSERT_NE(nullptr, builder.actual_index_micro_writer_);
  ASSERT_NE(nullptr, builder.trial_index_micro_writer_);
  EXPECT_TRUE(builder.is_closed_);
  EXPECT_EQ(0, builder.clustered_index_writer_->micro_writer_->get_row_count())
      << "clustered micro writer must be drained after close";
  const int64_t actual_row_count = builder.actual_index_micro_writer_->get_row_count();
  const int64_t trial_row_count = builder.trial_index_micro_writer_->get_row_count();
  EXPECT_GT(actual_row_count, 0);
  EXPECT_EQ(actual_row_count, trial_row_count)
      << "actual and trial index writers must retain the same final macro state";
  EXPECT_EQ(actual_row_count, builder.index_row_count_);
}

// 8. Empty partial sstable (0 fed rows) produces neither data macro blocks nor
//    clustered macro blocks; close takes the empty-partial branch.
TEST_F(ObInvIdxClusteredIndexTest, empty_sstable_produces_no_clustered)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(true /*micro_index_clustered*/, writer);

  OK(writer.close());
  ObSSTableMergeRes res;
  OK(index_builder_->close(res));

  EXPECT_EQ(0, res.data_block_ids_.count())
      << "empty sstable should produce no data macro blocks";
  EXPECT_EQ(0, res.other_block_ids_.count())
      << "empty sstable should produce no clustered macro blocks";
}

// 9. An ENCODING-format trial writer may reject a candidate at the current macro boundary.
//    The writer must switch macros, retry the candidate and finish without leaking
//    OB_BUF_NOT_ENOUGH or losing rows.
TEST_F(ObInvIdxClusteredIndexTest, append_index_row_encoding_splits_macro)
{
  ObInvIdxMacroBlockWriter writer;
  // micro_index_clustered=false isolates the n-1 index writer path.
  open_writer(false /*micro_index_clustered*/, writer, ENCODING_ROW_STORE, false /*has_pos_list*/);

  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;
  ASSERT_NE(nullptr, builder.leaf_store_desc_);
  ASSERT_TRUE(builder.leaf_store_desc_->encoding_enabled())
      << "index micro writer must be encoding format for this regression";

  int64_t fed_rows = 0;
  const int64_t max_rows = 200000;
  const int ret = feed_unique_terms(writer, max_rows, fed_rows);

  ASSERT_EQ(OB_SUCCESS, ret)
      << "encoding trial/commit admission must retry without losing rows; fed_rows=" << fed_rows;
  EXPECT_EQ(max_rows, fed_rows);

  OK(writer.close());
  ObSSTableMergeRes res;
  OK(index_builder_->close(res));
  ASSERT_TRUE(res.root_desc_.is_valid());
  ASSERT_EQ(max_rows, res.row_count_);
  ASSERT_NE(nullptr, builder.actual_index_micro_writer_);
  ASSERT_NE(nullptr, builder.trial_index_micro_writer_);
  EXPECT_EQ(builder.actual_index_micro_writer_->get_row_count(),
      builder.trial_index_micro_writer_->get_row_count());
  EXPECT_EQ(builder.index_row_count_, builder.actual_index_micro_writer_->get_row_count());
  EXPECT_GT(res.data_block_ids_.count(), 1)
      << "index admission should have forced multiple macro splits";
}

// 10. Run the same trial/commit macro-retry regression with a CS-ENCODING index writer.
TEST_F(ObInvIdxClusteredIndexTest, append_index_row_cs_encoding_splits_macro)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(false /*micro_index_clustered*/, writer, CS_ENCODING_ROW_STORE, false /*has_pos_list*/);

  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;
  ASSERT_NE(nullptr, builder.leaf_store_desc_);
  ASSERT_TRUE(builder.leaf_store_desc_->encoding_enabled())
      << "index micro writer must be CS-encoding format for this regression";
  ASSERT_EQ(CS_ENCODING_ROW_STORE, builder.leaf_store_desc_->get_row_store_type());

  int64_t fed_rows = 0;
  const int64_t max_rows = 200000;
  const int ret = feed_unique_terms(writer, max_rows, fed_rows);

  ASSERT_EQ(OB_SUCCESS, ret)
      << "cs-encoding trial/commit admission must retry without losing rows; fed_rows=" << fed_rows;
  EXPECT_EQ(max_rows, fed_rows);

  OK(writer.close());
  ObSSTableMergeRes res;
  OK(index_builder_->close(res));
  ASSERT_TRUE(res.root_desc_.is_valid());
  ASSERT_EQ(max_rows, res.row_count_);
  ASSERT_NE(nullptr, builder.actual_index_micro_writer_);
  ASSERT_NE(nullptr, builder.trial_index_micro_writer_);
  EXPECT_EQ(builder.actual_index_micro_writer_->get_row_count(),
      builder.trial_index_micro_writer_->get_row_count());
  EXPECT_EQ(builder.index_row_count_, builder.actual_index_micro_writer_->get_row_count());
  EXPECT_GT(res.data_block_ids_.count(), 1)
      << "index admission should have forced multiple macro splits";
}

// 11. A FLAT-format writer uses the same trial/commit admission contract. Complete the full
//     cycle and verify macro splitting and row preservation as the control format.
TEST_F(ObInvIdxClusteredIndexTest, append_index_row_flat_no_buf_not_enough)
{
  ObInvIdxMacroBlockWriter writer;
  open_writer(false /*micro_index_clustered*/, writer, FLAT_ROW_STORE, false /*has_pos_list*/);

  ObInvIdxIndexBlockBuilder &builder = writer.index_block_builder_;
  ASSERT_NE(nullptr, builder.leaf_store_desc_);
  ASSERT_FALSE(builder.leaf_store_desc_->encoding_enabled())
      << "control must use flat-format index micro writer";

  int64_t fed_rows = 0;
  const int64_t max_rows = 200000;
  const int ret = feed_unique_terms(writer, max_rows, fed_rows);

  ASSERT_EQ(OB_SUCCESS, ret)
      << "flat trial/commit admission must retry without losing rows; fed_rows=" << fed_rows;
  EXPECT_EQ(max_rows, fed_rows);
  OK(writer.close());

  ObSSTableMergeRes res;
  OK(index_builder_->close(res));
  ASSERT_TRUE(res.root_desc_.is_valid());
  ASSERT_EQ(max_rows, res.row_count_);
  ASSERT_NE(nullptr, builder.actual_index_micro_writer_);
  ASSERT_NE(nullptr, builder.trial_index_micro_writer_);
  EXPECT_EQ(builder.actual_index_micro_writer_->get_row_count(),
      builder.trial_index_micro_writer_->get_row_count());
  EXPECT_EQ(builder.index_row_count_, builder.actual_index_micro_writer_->get_row_count());
  EXPECT_GT(res.data_block_ids_.count(), 1)
      << "index admission should have forced multiple macro splits";
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_idx_clustered_index.log*");
  OB_LOGGER.set_file_name("test_inv_idx_clustered_index.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
