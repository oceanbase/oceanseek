/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <gmock/gmock.h>
#define private public
#include "storage/ddl/ob_ddl_seq_generator.h"
#include "storage/ddl/ob_lob_macro_block_writer.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"

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
