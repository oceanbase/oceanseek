/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <gmock/gmock.h>
#include <string>
#define private public
#include "storage/ddl/ob_ddl_seq_generator.h"
#include "storage/ddl/ob_lob_macro_block_writer.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/ob_storage_schema.h"

#define ASSERT_SUCC(expr) ASSERT_EQ(common::OB_SUCCESS, (expr))
#define ASSERT_FAIL(expr) ASSERT_NE(common::OB_SUCCESS, (expr))
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;

TEST(ddl_seq_generator, init)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_FALSE(seq_generator.is_inited_);
  ASSERT_FAIL(seq_generator.init(-1, 5, 100));
  ASSERT_FAIL(seq_generator.init(0, 0, 100));
  ASSERT_FAIL(seq_generator.init(0, 5, -100));
  ASSERT_FAIL(seq_generator.init(0, 100, 5));
  ASSERT_SUCC(seq_generator.init(0, 5, 100));
  ASSERT_FAIL(seq_generator.init(0, 5, 100));
  ASSERT_TRUE(seq_generator.is_inited_);
  seq_generator.reset();
  ASSERT_FALSE(seq_generator.is_inited_);
}

TEST(ddl_seq_generator, get_next)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_SUCC(seq_generator.init(10, 5, 100));
  int64_t seq_val = -1;
  int64_t preview_next_val = -1;
  bool is_step_over = false;
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(10, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(11, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(12, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(13, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(14, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(110, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(111, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(112, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(113, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(114, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(210, seq_val); ASSERT_FALSE(is_step_over);

  ASSERT_SUCC(seq_generator.preview_next(seq_val, preview_next_val));
  ASSERT_EQ(211, preview_next_val);
  ASSERT_EQ(seq_val, seq_generator.get_current());
}

TEST(ddl_seq_generator, corner)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_SUCC(seq_generator.init(1, 1, 1));
  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(10, 1, 100));
  int64_t seq_val = -1;
  bool is_step_over = false;
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(10, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(110, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(210, seq_val); ASSERT_TRUE(is_step_over);

  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(0, 1, 1));
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(0, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(1, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(2, seq_val); ASSERT_TRUE(is_step_over);

  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(10, 1, 11));
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(10, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(21, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(32, seq_val); ASSERT_TRUE(is_step_over);
}

TEST(ddl_seq_generator, get_next_interval)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_SUCC(seq_generator.init(10, 10, 100));
  int64_t interval_start = -1;
  int64_t interval_end = -1;
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(10, interval_start); ASSERT_EQ(19, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(110, interval_start); ASSERT_EQ(119, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(210, interval_start); ASSERT_EQ(219, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(310, interval_start); ASSERT_EQ(319, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));

  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(11, 8, 123));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(11, interval_start); ASSERT_EQ(18, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(134, interval_start); ASSERT_EQ(141, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(257, interval_start); ASSERT_EQ(264, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));

}

TEST(ddl_seq_generator, big_start)
{
  ObDDLSeqGenerator seq_generator;
  ASSERT_SUCC(seq_generator.init(320, 10, 100));
  int64_t seq_val = -1;
  bool is_step_over = false;
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(320, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(321, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(322, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(323, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(324, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(325, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(326, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(327, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(328, seq_val); ASSERT_FALSE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(329, seq_val); ASSERT_TRUE(is_step_over);
  ASSERT_SUCC(seq_generator.get_next(seq_val, is_step_over)); ASSERT_EQ(420, seq_val); ASSERT_FALSE(is_step_over);

  seq_generator.reset();
  ASSERT_SUCC(seq_generator.init(320, 10, 100));
  int64_t interval_start = -1;
  int64_t interval_end = -1;
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(320, interval_start); ASSERT_EQ(329, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));
  ASSERT_SUCC(seq_generator.get_next_interval(interval_start, interval_end)); ASSERT_EQ(420, interval_start); ASSERT_EQ(429, interval_end); ASSERT_TRUE(seq_generator.is_step_over(seq_generator.get_current()));

}

TEST(ddl_seq_generator, lob_aux_macro_seq_generator_shared_allocation)
{
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 100;
  seq_param.end_ = 110;
  ObMacroIncSeqGenerator shared_generator;
  ASSERT_SUCC(shared_generator.init(seq_param));

  ObLobAuxMacroSeqGenerator col1_generator;
  ObLobAuxMacroSeqGenerator col2_generator;
  ASSERT_SUCC(col1_generator.bind(shared_generator));
  ASSERT_SUCC(col2_generator.bind(shared_generator));

  int64_t col1_current = -1;
  int64_t col1_preview = -1;
  int64_t col1_repeat_preview = -1;
  int64_t col2_current = -1;
  ASSERT_SUCC(col1_generator.get_next(col1_current));
  ASSERT_EQ(100, col1_current);
  ASSERT_SUCC(col1_generator.preview_next(col1_current, col1_preview));
  ASSERT_EQ(101, col1_preview);

  // col1 已经预留 101，col2 必须从共享 generator 取得下一个未使用序号。
  ASSERT_SUCC(col2_generator.get_next(col2_current));
  ASSERT_EQ(102, col2_current);
  ASSERT_SUCC(col1_generator.preview_next(col1_current, col1_repeat_preview));
  ASSERT_EQ(col1_preview, col1_repeat_preview);
  ASSERT_SUCC(col1_generator.get_next(col1_current));
  ASSERT_EQ(101, col1_current);
  ASSERT_EQ(102, col2_generator.get_current());
  ASSERT_EQ(102, shared_generator.get_current());
}

TEST(ddl_seq_generator, lob_aux_macro_seq_generator_reserved_sequence_survives_exhaustion)
{
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 10;
  seq_param.end_ = 12;
  ObMacroIncSeqGenerator shared_generator;
  ASSERT_SUCC(shared_generator.init(seq_param));

  ObLobAuxMacroSeqGenerator col1_generator;
  ObLobAuxMacroSeqGenerator col2_generator;
  ASSERT_SUCC(col1_generator.bind(shared_generator));
  ASSERT_SUCC(col2_generator.bind(shared_generator));

  int64_t current = -1;
  int64_t preview = -1;
  ASSERT_SUCC(col1_generator.get_next(current));
  ASSERT_EQ(10, current);
  ASSERT_SUCC(col1_generator.preview_next(current, preview));
  ASSERT_EQ(11, preview);
  ASSERT_EQ(OB_SIZE_OVERFLOW, col2_generator.get_next(current));

  // 共享窗口耗尽不影响已经为 col1 预留的序号。
  ASSERT_SUCC(col1_generator.get_next(current));
  ASSERT_EQ(11, current);
  ASSERT_EQ(OB_SIZE_OVERFLOW, col1_generator.get_next(current));
}

TEST(ddl_seq_generator, lob_aux_macro_seq_generator_guard)
{
  ObMacroSeqParam seq_param;
  ObLobAuxMacroSeqGenerator generator;
  int64_t seq_val = -1;
  ASSERT_EQ(OB_NOT_INIT, generator.get_next(seq_val));
  ASSERT_EQ(OB_NOT_INIT, generator.preview_next(-1, seq_val));
  ASSERT_EQ(OB_NOT_SUPPORTED, generator.init(seq_param));

  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = 20;
  seq_param.end_ = 30;
  ObMacroIncSeqGenerator shared_generator;
  ASSERT_SUCC(shared_generator.init(seq_param));
  ASSERT_SUCC(generator.bind(shared_generator));
  ASSERT_EQ(OB_INIT_TWICE, generator.bind(shared_generator));
  ASSERT_SUCC(generator.get_next(seq_val));
  ASSERT_EQ(20, seq_val);
  ASSERT_EQ(OB_STATE_NOT_MATCH, generator.preview_next(seq_val - 1, seq_val));
  generator.reset();
  ASSERT_EQ(OB_NOT_INIT, generator.get_next(seq_val));
}

TEST(ddl_seq_generator, lob_aux_run_state_range_and_order)
{
  const int64_t lob_col_idx = 5;
  ObLobMacroBlockWriter::ObLobAuxRunState run_state;
  run_state.reset(100, 102);

  ObLobId lob_id;
  lob_id.assign(lob_col_idx, 200001, 100);
  ASSERT_SUCC(run_state.check_and_record(lob_id, lob_col_idx));
  lob_id.assign(lob_col_idx, 200001, 100);
  ASSERT_SUCC(run_state.check_and_record(lob_id, lob_col_idx));
  lob_id.assign(lob_col_idx, 200001, 102);
  ASSERT_SUCC(run_state.check_and_record(lob_id, lob_col_idx));

  lob_id.assign(lob_col_idx, 200001, 101);
  ASSERT_EQ(OB_ERR_UNEXPECTED, run_state.check_and_record(lob_id, lob_col_idx));
  lob_id.assign(lob_col_idx, 200001, 103);
  ASSERT_EQ(OB_ERR_UNEXPECTED, run_state.check_and_record(lob_id, lob_col_idx));
  lob_id.assign(lob_col_idx + 1, 200001, 102);
  ASSERT_EQ(OB_ERR_UNEXPECTED, run_state.check_and_record(lob_id, lob_col_idx));

  run_state.reset(102, 100);
  lob_id.assign(lob_col_idx, 200001, 101);
  ASSERT_EQ(OB_ERR_UNEXPECTED, run_state.check_and_record(lob_id, lob_col_idx));
}

class TestExternalLobSequence : public ::testing::Test
{
protected:
  void init_writer(ObLobMacroBlockWriter &writer, const int64_t slice_idx,
                   const int64_t slice_count)
  {
    ObWriteMacroParam param;
    param.ls_id_ = share::ObLSID(1);
    param.tablet_id_ = ObTabletID(2002);
    param.lob_meta_tablet_id_ = ObTabletID(3003);
    param.lob_piece_tablet_id_ = ObTabletID(4004);
    param.tenant_data_version_ = DATA_CURRENT_VERSION;
    param.schema_version_ = 1;
    param.snapshot_version_ = 10;
    param.task_id_ = 1;
    param.slice_idx_ = slice_idx;
    param.slice_count_ = slice_count;
    param.direct_load_type_ = ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DDL;
    param.ddl_table_schema_.lob_meta_storage_schema_ = &lob_schema_;
    param.ddl_table_schema_.lob_piece_storage_schema_ = &lob_schema_;
    ASSERT_SUCC(param.ddl_table_schema_.column_items_.prepare_allocate(6));
    ASSERT_SUCC(param.ddl_table_schema_.lob_column_idxs_.push_back(2));
    ASSERT_SUCC(param.ddl_table_schema_.lob_column_idxs_.push_back(5));
    param.ddl_table_schema_.column_items_.at(2).col_type_.set_type(ObExtendBLobType);
    param.ddl_table_schema_.column_items_.at(5).col_type_.set_type(ObExtendBLobType);
    ASSERT_SUCC(writer.init(param, param.tablet_id_));
  }

  ObStorageSchema lob_schema_;
};

TEST_F(TestExternalLobSequence, replay_same_slice)
{
  ObLobMacroBlockWriter writer;
  ObLobMacroBlockWriter retry_writer;
  ASSERT_NO_FATAL_FAILURE(init_writer(writer, 1, 3));
  ASSERT_NO_FATAL_FAILURE(init_writer(retry_writer, 1, 3));
  ASSERT_EQ(0U, writer.lob_id_cache_.count());
  ASSERT_SUCC(writer.switch_lob_id_cache());
  ASSERT_SUCC(retry_writer.switch_lob_id_cache());
  ASSERT_EQ(writer.lob_meta_tablet_id_, writer.lob_id_cache_.tablet_id_);
  for (int64_t i = 0; i < 16; ++i) {
    uint64_t lob_id = 0;
    uint64_t retry_lob_id = 0;
    uint64_t seq_id = 0;
    uint64_t retry_seq_id = 0;
    ASSERT_SUCC(writer.lob_id_cache_.next_value(lob_id));
    ASSERT_SUCC(retry_writer.lob_id_cache_.next_value(retry_lob_id));
    ASSERT_SUCC(writer.lob_id_cache_.next_value(seq_id));
    ASSERT_SUCC(retry_writer.lob_id_cache_.next_value(retry_seq_id));
    ASSERT_EQ(lob_id, retry_lob_id);
    ASSERT_NE(lob_id, seq_id);
    ASSERT_EQ(100001U + 2 * i, seq_id);
    ASSERT_EQ(seq_id, retry_seq_id);
    uint64_t cache_value = 0;
    uint64_t retry_cache_value = 0;
    ASSERT_SUCC(writer.lob_id_cache_.get_value(cache_value));
    ASSERT_SUCC(retry_writer.lob_id_cache_.get_value(retry_cache_value));
    ASSERT_EQ(100003U + 2 * i, cache_value + 1);
    ASSERT_EQ(cache_value + 1, retry_cache_value + 1);
  }
  ASSERT_GT(writer.lob_id_cache_.remain_count(), 0U);
  ASSERT_EQ(writer.lob_id_generator_.get_current(), retry_writer.lob_id_generator_.get_current());
}

TEST_F(TestExternalLobSequence, slice_intervals_and_watermark_at_boundary)
{
  ObLobMacroBlockWriter first_slice;
  ObLobMacroBlockWriter second_slice;
  ASSERT_NO_FATAL_FAILURE(init_writer(first_slice, 0, 2));
  ASSERT_NO_FATAL_FAILURE(init_writer(second_slice, 1, 2));
  ASSERT_SUCC(first_slice.switch_lob_id_cache());
  ASSERT_SUCC(second_slice.switch_lob_id_cache());
  uint64_t first_seq = 0;
  uint64_t second_seq = 0;
  ASSERT_EQ(99999U, first_slice.lob_id_cache_.count());
  ASSERT_EQ(100000U, second_slice.lob_id_cache_.count());
  for (uint64_t i = 1; i < 100000; ++i) {
    ASSERT_SUCC(first_slice.lob_id_cache_.next_value(first_seq));
    ASSERT_EQ(i, first_seq);
  }
  for (uint64_t i = 100000; i < 200000; ++i) {
    ASSERT_SUCC(second_slice.lob_id_cache_.next_value(second_seq));
    ASSERT_EQ(i, second_seq);
    ASSERT_LT(first_seq, second_seq);
  }
  // get_value() clamps an exhausted cache to its end; the watermark must advance past it.
  ASSERT_EQ(0U, first_slice.lob_id_cache_.remain_count());
  ASSERT_EQ(0U, second_slice.lob_id_cache_.remain_count());
  uint64_t first_cache_value = 0;
  uint64_t second_cache_value = 0;
  ASSERT_SUCC(first_slice.lob_id_cache_.get_value(first_cache_value));
  ASSERT_SUCC(second_slice.lob_id_cache_.get_value(second_cache_value));
  ASSERT_EQ(100000U, first_cache_value + 1);
  ASSERT_EQ(200000U, second_cache_value + 1);
  ASSERT_SUCC(first_slice.switch_lob_id_cache());
  ASSERT_SUCC(second_slice.switch_lob_id_cache());
  ASSERT_SUCC(first_slice.lob_id_cache_.next_value(first_seq));
  ASSERT_SUCC(second_slice.lob_id_cache_.next_value(second_seq));
  ASSERT_EQ(200000U, first_seq);
  ASSERT_EQ(300000U, second_seq);
  ASSERT_SUCC(first_slice.lob_id_cache_.get_value(first_cache_value));
  ASSERT_SUCC(second_slice.lob_id_cache_.get_value(second_cache_value));
  const uint64_t first_watermark = first_cache_value + 1;
  const uint64_t second_watermark = second_cache_value + 1;
  ASSERT_EQ(200002U, first_watermark);
  ASSERT_EQ(300002U, second_watermark);
  ASSERT_GT(first_watermark, first_seq + 1);
  ASSERT_GT(second_watermark, second_seq + 1);
}

TEST_F(TestExternalLobSequence, replay_preserves_external_summary_row_bytes)
{
  ObArenaAllocator source_allocator;
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.etag_ = ObString::make_string("object-etag");
  info.offset_ = 3;
  info.length_ = 17;
  info.has_length_ = true;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder builder;
  oceanbase::common::ObLobLocatorV2 source_locator;
  ASSERT_SUCC(builder.init(source_allocator));
  ASSERT_SUCC(builder.set_external_lob_info(info));
  ASSERT_SUCC(builder.build(source_locator));

  ObLobManager lob_manager(OB_SYS_TENANT_ID);
  ObLobReadContext read_ctx;
  ASSERT_SUCC(read_ctx.build(source_locator, source_allocator, 0, UINT64_MAX));
  read_ctx.coll_type_ = CS_TYPE_BINARY;
  read_ctx.byte_size_ = info.length_;
  ObLobSourceReadCtx source_ctx;
  source_ctx.read_ctx_ = &read_ctx;
  source_ctx.lob_manager_ = &lob_manager;
  source_ctx.source_byte_len_ = info.length_;
  std::string summary_rows[3];
  for (int64_t attempt = 0; attempt < 3; ++attempt) {
    ObLobMacroBlockWriter writer;
    ASSERT_NO_FATAL_FAILURE(init_writer(writer, 2 == attempt ? 1 : 0, 2));
    ObArenaAllocator locator_allocator;
    ObArenaAllocator result_allocator;
    ObLobTargetWriteCtx target_ctx;
    target_ctx.locator_allocator_ = &locator_allocator;
    target_ctx.result_allocator_ = &result_allocator;
    target_ctx.coll_type_ = CS_TYPE_BINARY;
    target_ctx.lob_col_idx_ = 2;
    uint64_t auto_inc_id = 0;
    ASSERT_SUCC(writer.switch_lob_id_cache());
    ASSERT_SUCC(writer.lob_id_cache_.next_value(auto_inc_id));
    target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, writer.lob_meta_tablet_id_.id(), auto_inc_id);
    target_ctx.lob_meta_tablet_id_ = writer.lob_meta_tablet_id_;
    ASSERT_SUCC(writer.lob_id_cache_.next_value(target_ctx.external_lob_seq_id_));
    ASSERT_NE(auto_inc_id, target_ctx.external_lob_seq_id_);

    ObLobWriteIter write_iter;
    ObLobWriteResult result;
    ASSERT_SUCC(write_iter.open(source_ctx, target_ctx));
    ASSERT_SUCC(write_iter.get_next_row(result));
    ASSERT_TRUE(result.is_meta_summary());
    ASSERT_EQ(target_ctx.lob_id_, result.meta_info_.lob_id_);
    ASSERT_EQ(info.length_, result.meta_info_.byte_len_);
    uint64_t tablet_id = 0;
    uint64_t seq_id = 0;
    ASSERT_SUCC(ObLobMetaUtil::deserialize_external_lob_block_id(
        result.meta_info_.lob_block_id_, tablet_id, seq_id));
    ASSERT_EQ(writer.lob_meta_tablet_id_.id(), tablet_id);
    ASSERT_NE(writer.tablet_id_.id(), tablet_id);
    ASSERT_EQ(target_ctx.external_lob_seq_id_, seq_id);

    blocksstable::ObDatumRow row;
    ASSERT_SUCC(row.init(ObLobMetaInfo::COLUMN_CNT));
    ASSERT_SUCC(ObLobMetaUtil::transform_from_info_to_row(result.meta_info_, &row, false));
    const int64_t serialized_size = row.get_serialize_size();
    ASSERT_GT(serialized_size, 0);
    summary_rows[attempt].resize(serialized_size);
    int64_t pos = 0;
    ASSERT_SUCC(row.serialize(&summary_rows[attempt][0], serialized_size, pos));
    ASSERT_EQ(serialized_size, pos);
    ASSERT_EQ(OB_ITER_END, write_iter.get_next_row(result));
    ASSERT_TRUE(write_iter.is_finished());
  }
  ASSERT_EQ(summary_rows[0], summary_rows[1]);
  ASSERT_NE(summary_rows[0], summary_rows[2]);
}

TEST(ddl_seq_generator, lob_macro_meta_store_sort_and_overlap_guard)
{
  const ObTabletID tablet_id(200001);
  const int64_t cg_idx = 0;
  ObMacroMetaStoreManager store_manager;
  ObMacroMetaStoreManager::StoreItem col8_item;
  ObMacroMetaStoreManager::StoreItem col5_second_item;
  ObMacroMetaStoreManager::StoreItem col5_first_item;

  col8_item.macro_meta_store_ = OB_NEWx(
      ObMacroMetaTempStore, &store_manager.allocator_);
  col8_item.tablet_id_ = tablet_id;
  col8_item.cg_idx_ = cg_idx;
  col8_item.parallel_idx_ = 0;
  col8_item.is_lob_store_ = true;
  col8_item.lob_col_idx_ = 8;
  col8_item.lob_id_interval_start_ = 100;
  col8_item.lob_id_interval_end_ = 199;

  col5_second_item.macro_meta_store_ = OB_NEWx(
      ObMacroMetaTempStore, &store_manager.allocator_);
  col5_second_item.tablet_id_ = tablet_id;
  col5_second_item.cg_idx_ = cg_idx;
  col5_second_item.parallel_idx_ = 1;
  col5_second_item.is_lob_store_ = true;
  col5_second_item.lob_col_idx_ = 5;
  col5_second_item.lob_id_interval_start_ = 200;
  col5_second_item.lob_id_interval_end_ = 299;

  col5_first_item.macro_meta_store_ = OB_NEWx(
      ObMacroMetaTempStore, &store_manager.allocator_);
  col5_first_item.tablet_id_ = tablet_id;
  col5_first_item.cg_idx_ = cg_idx;
  col5_first_item.parallel_idx_ = 2;
  col5_first_item.is_lob_store_ = true;
  col5_first_item.lob_col_idx_ = 5;
  col5_first_item.lob_id_interval_start_ = 100;
  col5_first_item.lob_id_interval_end_ = 199;

  ASSERT_NE(nullptr, col8_item.macro_meta_store_);
  ASSERT_NE(nullptr, col5_second_item.macro_meta_store_);
  ASSERT_NE(nullptr, col5_first_item.macro_meta_store_);
  ASSERT_SUCC(store_manager.store_items_.push_back(col8_item));
  ASSERT_SUCC(store_manager.store_items_.push_back(col5_second_item));
  ASSERT_SUCC(store_manager.store_items_.push_back(col5_first_item));

  ObArray<ObMacroMetaStoreManager::StoreItem> sorted_items;
  ASSERT_SUCC(store_manager.get_sorted_lob_macro_meta_stores(
      tablet_id, cg_idx, sorted_items));
  ASSERT_EQ(3, sorted_items.count());
  ASSERT_EQ(5, sorted_items.at(0).lob_col_idx_);
  ASSERT_EQ(100u, sorted_items.at(0).lob_id_interval_start_);
  ASSERT_EQ(5, sorted_items.at(1).lob_col_idx_);
  ASSERT_EQ(200u, sorted_items.at(1).lob_id_interval_start_);
  ASSERT_EQ(8, sorted_items.at(2).lob_col_idx_);

  // 注入相邻区间端点重叠，merge 排序入口必须拒绝该组 run。
  store_manager.store_items_.at(1).lob_id_interval_start_ = 199;
  ASSERT_EQ(OB_ERR_UNEXPECTED, store_manager.get_sorted_lob_macro_meta_stores(
      tablet_id, cg_idx, sorted_items));
}


#define LOG_FILE_PATH "./test_ddl_seq_generator.log"

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
