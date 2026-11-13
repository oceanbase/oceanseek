// owner: yilan.zyn
// owner group: storage

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <signal.h>
#include "test_inv_idx_sstable_prepare.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_helper.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_iter.h"
#include "storage/blocksstable/async_index/ob_partition_inv_sstable_merger.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/compaction/ob_partition_rows_merger.h"

namespace oceanbase
{
namespace unittest
{

TEST_F(TestInvIdxSSTablePrepare, inc_major_wrap_preserves_full_merge)
{
  ObInvSSTable input;
  build_inv_sstable(true, 10, input);
  ObSSTableMetaHandle meta;
  OK(input.get_meta(meta));
  for (const int16_t seq : {int16_t(0), int16_t(ObMacroDataSeq::MAX_SSTABLE_SEQ - 2),
                           int16_t(ObMacroDataSeq::MAX_SSTABLE_SEQ - 1)}) {
    SCOPED_TRACE(seq);
    const_cast<ObSSTableBasicMeta &>(meta.get_sstable_meta().get_basic_meta()).sstable_logic_seq_ = seq;
    ObTabletMergeDagParam dag_param;
    dag_param.merge_type_ = INC_MAJOR_MERGE;
    dag_param.exec_mode_ = EXEC_MODE_OUTPUT;
    ObAsyncIndexMergeCtx ctx(dag_param, allocator_);
    table_schema_.set_progressive_merge_num(0);
    void *schema_buf = allocator_.alloc(sizeof(ObStorageSchema));
    ASSERT_NE(nullptr, schema_buf);
    ObStorageSchema *storage_schema = new (schema_buf) ObStorageSchema();
    OK(storage_schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    ctx.static_param_.schema_ = storage_schema;
    OK(ctx.static_param_.tables_handle_.add_sstable(&input, ObStorageMetaHandle()));
    OK(ctx.static_param_.init_sstable_logic_seq());
    const bool wrapped = seq + 1 == ObMacroDataSeq::MAX_SSTABLE_SEQ;
    ASSERT_EQ(wrapped ? 0 : seq + 1, ctx.static_param_.sstable_logic_seq_);
    ASSERT_EQ(wrapped, ctx.static_param_.is_full_merge_);
    OK(ctx.cal_merge_param());
    EXPECT_EQ(wrapped, ctx.static_param_.is_full_merge_);

    ObTabletMergeDagParam minor_dag_param;
    minor_dag_param.merge_type_ = MINOR_MERGE;
    ObStaticMergeParam minor_param(minor_dag_param);
    OK(minor_param.tables_handle_.add_sstable(&input, ObStorageMetaHandle()));
    OK(minor_param.init_sstable_logic_seq());
    OK(minor_param.cal_minor_merge_param(false /*has_compaction_filter*/));
    EXPECT_EQ(minor_param.sstable_logic_seq_, ctx.static_param_.sstable_logic_seq_);
    EXPECT_EQ(minor_param.is_full_merge_, ctx.static_param_.is_full_merge_);
    EXPECT_EQ(!wrapped, !minor_param.is_full_merge_
        && minor_param.sstable_logic_seq_ < ObMacroDataSeq::MAX_SSTABLE_SEQ);
    EXPECT_EQ(!wrapped, !ctx.static_param_.is_full_merge_
        && ctx.static_param_.sstable_logic_seq_ < ObMacroDataSeq::MAX_SSTABLE_SEQ);
  }
}

TEST_F(TestInvIdxSSTablePrepare, inc_major_wrap_rewrites_old_generation_zero_macros)
{
  const int64_t row_count = 100000;
  ObInvSSTable input;
  build_inv_sstable(true, row_count, input, true /*vary_posting_payload*/);
  ASSERT_GT(input.get_data_macro_block_count(), 1);
  ObSSTableMetaHandle meta;
  OK(input.get_meta(meta));
  ObSEArray<ObColDesc, 8> columns;
  OK(table_schema_.get_multi_version_column_descs(columns));
  ObRowkeyReadInfo read_info;
  OK(read_info.init(allocator_, table_schema_.get_column_count(),
                   table_schema_.get_rowkey_column_num(), false /*is_oracle_mode*/, columns));
  ObDatumRange whole;
  whole.set_whole_range();

  // The input contains generation-zero macros. Changing only its table generation
  // models old macros surviving reuse across merges. Use one input so this test
  // works independently of scanning all inputs for MAX + 1.
  struct MergeCase {
    ObMergeType merge_type_;
    int16_t input_seq_;
    bool force_full_merge_;
    bool expect_reuse_;
  };
  const MergeCase cases[] = {
    {INC_MAJOR_MERGE, 0, false, true},
    {INC_MAJOR_MERGE, ObMacroDataSeq::MAX_SSTABLE_SEQ - 2, false, true},
    {INC_MAJOR_MERGE, ObMacroDataSeq::MAX_SSTABLE_SEQ - 1, false, false},
    {INC_MAJOR_MERGE, 0, true, true},
    {MAJOR_MERGE, 0, false, true},
    {MEDIUM_MERGE, 0, false, true},
  };
  for (const MergeCase &merge_case : cases) {
    const int16_t input_seq = merge_case.input_seq_;
    SCOPED_TRACE(input_seq);
    SCOPED_TRACE(merge_case.merge_type_);
    SCOPED_TRACE(merge_case.force_full_merge_);
    const_cast<ObSSTableBasicMeta &>(meta.get_sstable_meta().get_basic_meta()).sstable_logic_seq_ = input_seq;
    ObTabletMergeDagParam dag_param;
    dag_param.merge_type_ = merge_case.merge_type_;
    dag_param.exec_mode_ = EXEC_MODE_OUTPUT;
    ObAsyncIndexMergeCtx ctx(dag_param, allocator_);
    table_schema_.set_progressive_merge_num(0);
    void *schema_buf = allocator_.alloc(sizeof(ObStorageSchema));
    ASSERT_NE(nullptr, schema_buf);
    ObStorageSchema *storage_schema = new (schema_buf) ObStorageSchema();
    OK(storage_schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    ctx.static_param_.schema_ = storage_schema;
    OK(ctx.static_param_.tables_handle_.add_sstable(&input, ObStorageMetaHandle()));
    OK(ctx.static_param_.init_sstable_logic_seq());
    if (merge_case.force_full_merge_) {
      ctx.static_param_.set_full_merge_and_level(true);
    }
    OK(ctx.cal_merge_param());
    ASSERT_EQ(!merge_case.expect_reuse_, ctx.static_param_.is_full_merge_);

    // Compare generation-wrap behavior with the ordinary minor factory.
    // Minor preserves an explicitly forced full merge; INC_MAJOR clears it unless wrapping.
    if (is_inc_major_merge(merge_case.merge_type_) && !merge_case.force_full_merge_) {
      ObTabletMergeDagParam minor_dag_param;
      minor_dag_param.merge_type_ = MINOR_MERGE;
      ObStaticMergeParam minor_param(minor_dag_param);
      OK(minor_param.tables_handle_.add_sstable(&input, ObStorageMetaHandle()));
      OK(minor_param.init_sstable_logic_seq());
      OK(minor_param.cal_minor_merge_param(false /*has_compaction_filter*/));
      ObMergeParameter merge_param(minor_param);
      ObCompactionFilterHandle filter_handle;
      ObPartitionMinorMergeHelper minor_helper(read_info, allocator_);
      ObPartitionMergeIter *minor_iter = minor_helper.alloc_merge_iter(merge_param, 0, &input, filter_handle);
      ASSERT_NE(nullptr, minor_iter);
      EXPECT_EQ(merge_case.expect_reuse_, minor_iter->is_macro_merge_iter());
      minor_iter->~ObPartitionMergeIter();
    }

    index_builder_->~ObSSTableIndexBuilder();
    index_builder_ = nullptr;
    whole_desc_.reset();
    prepare_desc();
    // Generation zero is already represented by the existing raw sequence zero.
    // Keep the fixture's writer setup so this regression does not depend on the
    // separate OUTPUT descriptor/generation-encoding fix.
    ObInvIdxMacroBlockWriter writer;
    ObSSTablePrivateObjectCleaner cleaner;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0, seq_param_,
                   pre_warm_param, cleaner, nullptr /*callback*/));
    ObDeletionBitmapHandle deletion_bitmap;
    ObInvSSTableMergeHelper helper(read_info, inv_idx_row_desc_, allocator_,
                                  deletion_bitmap, &whole_desc_.get_desc());
    ObInvSSTableMergeIter *iter = OB_NEWx(ObInvSSTableMergeIter, &allocator_);
    ASSERT_NE(nullptr, iter);
    OK(iter->init(input, read_info, inv_idx_row_desc_, whole, allocator_));
    OK(helper.iters_.push_back(iter));
    OK(helper.prepare_rows_merger_(ctx.static_param_.is_full_merge_));
    ASSERT_EQ(ctx.static_param_.is_full_merge_, nullptr != iter->get_curr_row());
    helper.is_inited_ = true;
    ObLocalArena merger_arena("InvSeqWrap");
    ObPartitionInvSSTableMerger merger(merger_arena, ctx.static_param_);
    merger.merge_helper_ = &helper;
    merger.macro_writer_ = &writer;
    const int merge_ret = merger.do_merge_();
    // The test owns these stack objects, including on assertion failure.
    merger.merge_helper_ = nullptr;
    merger.macro_writer_ = nullptr;
    ASSERT_EQ(OB_SUCCESS, merge_ret);
    ASSERT_TRUE(helper.is_iter_end());
    ASSERT_EQ(merge_case.expect_reuse_ ? input.get_data_macro_block_count() : 0,
              writer.get_merge_block_info().multiplexed_macro_block_count_);
    // Wrapped merges must rewrite all rows, including after crossing block boundaries.
    ASSERT_EQ(0, writer.get_merge_block_info().multiplexed_logical_block_count_);
    ASSERT_EQ(0, merger.logical_reuse_stats_.check_count_);
    OK(writer.close());
    OK(index_builder_->merge_inv_meta(writer.get_seg_inv_meta()));
    ObSSTableMergeRes res;
    OK(index_builder_->close(res));
    ObInvSSTable output;
    finalize_inv_sstable_from_res(res, output);
    ASSERT_GT(output.get_data_macro_block_count(), 1);

    ObInvSSTableMergeIter macro_iter;
    OK(macro_iter.init(output, read_info, inv_idx_row_desc_, whole, allocator_));
    ObSEArray<ObLogicMacroBlockId, 8> ids;
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret = macro_iter.next_macro_block())) {
      ObMacroBlockDesc desc;
      OK(macro_iter.get_macro_block_desc(desc));
      const ObLogicMacroBlockId &id = desc.macro_meta_->val_.logic_id_;
      EXPECT_EQ(0, static_cast<int64_t>(id.data_seq_.sstable_logic_seq_));
      for (int64_t i = 0; i < ids.count(); ++i) {
        EXPECT_NE(ids.at(i), id);
      }
      OK(ids.push_back(id));
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(output.get_data_macro_block_count(), ids.count());

    ObInvSSTableMergeIter before;
    ObInvSSTableMergeIter after;
    OK(before.init(input, read_info, inv_idx_row_desc_, whole, allocator_));
    OK(after.init(output, read_info, inv_idx_row_desc_, whole, allocator_));
    int64_t count = 0;
    while (OB_SUCC(ret = before.next())) {
      OK(after.next());
      const ObDatumRow *expected = before.get_curr_row();
      const ObDatumRow *actual = after.get_curr_row();
      ASSERT_NE(nullptr, expected);
      ASSERT_NE(nullptr, actual);
      for (const int16_t col : {int16_t(0), inv_idx_row_desc_.pos_list_col_idx_}) {
        const ObStorageDatum &a = expected->storage_datums_[col];
        const ObStorageDatum &b = actual->storage_datums_[col];
        ASSERT_EQ(ObString(a.len_, a.ptr_), ObString(b.len_, b.ptr_));
      }
      for (const int16_t col : {int16_t(1), inv_idx_row_desc_.token_freq_col_idx_,
                               inv_idx_row_desc_.doc_length_col_idx_}) {
        ASSERT_EQ(expected->storage_datums_[col].get_uint(), actual->storage_datums_[col].get_uint());
      }
      ++count;
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(OB_ITER_END, after.next());
    ASSERT_EQ(row_count, count);
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_inv_sstable_generation_wrap.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
