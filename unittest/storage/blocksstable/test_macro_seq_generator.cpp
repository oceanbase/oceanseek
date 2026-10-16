/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <gmock/gmock.h>
#define private public
#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/incremental/ob_inc_ss_macro_seq_define.h"

#define ASSERT_SUCC(expr) ASSERT_EQ(common::OB_SUCCESS, (expr))
#define ASSERT_FAIL(expr) ASSERT_NE(common::OB_SUCCESS, (expr))
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

TEST(macro_seq_generator, param)
{
  ObMacroSeqParam seq_param;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  ASSERT_TRUE(seq_param.is_valid());
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_SKIP;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.interval_ = 1;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.step_ = 1;
  ASSERT_TRUE(seq_param.is_valid());
}

TEST(macro_seq_generator, inc_generator)
{
  ObMacroIncSeqGenerator inc_generator;
  ASSERT_FALSE(inc_generator.is_inited_);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_SKIP;
  seq_param.start_ = 11;
  seq_param.interval_ = 10;
  seq_param.step_ = 100;
  ASSERT_FAIL(inc_generator.init(seq_param));
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  ASSERT_SUCC(inc_generator.init(seq_param));
  ASSERT_TRUE(inc_generator.is_inited_);
  int64_t seq_val = -1;
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 11);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 12);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 13);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 14);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 15);
  ASSERT_SUCC(inc_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 16);
  ASSERT_EQ(seq_val, inc_generator.get_current());
  int64_t preview_next_val = -1;
  ASSERT_SUCC(inc_generator.preview_next(seq_val, preview_next_val));
  ASSERT_EQ(seq_val + 1, preview_next_val);
  ASSERT_EQ(seq_val, inc_generator.get_current());
}

TEST(macro_seq_generator, version_local_window)
{
  ObMacroSeqWindow window;
  ObMacroIncSeqGenerator generator;
  ObMacroSeqParam seq_param;
  int64_t raw_seq = -1;
  ASSERT_SUCC(ObVersionLocalMacroSeq::build_window(3, window));
  const int64_t step_size = window.end_ - window.start_;
  ASSERT_EQ(3 * step_size, window.start_);
  ASSERT_EQ(4 * step_size, window.end_);
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = window.start_;
  seq_param.end_ = window.start_;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.end_ = window.start_ - 1;
  ASSERT_FALSE(seq_param.is_valid());
  seq_param.end_ = window.end_;
  ASSERT_EQ(window.end_, seq_param.end_);
  ASSERT_SUCC(generator.init(seq_param));
  ASSERT_SUCC(generator.get_next(raw_seq));
  ASSERT_EQ(window.start_, raw_seq);
  ASSERT_SUCC(generator.get_next(raw_seq));
  ASSERT_EQ(window.start_ + 1, raw_seq);
  generator.current_ = window.end_ - 2;
  ASSERT_SUCC(generator.get_next(raw_seq));
  ASSERT_EQ(window.end_ - 1, raw_seq);
  ASSERT_EQ(OB_SIZE_OVERFLOW, generator.get_next(raw_seq));
  ASSERT_EQ(window.end_ - 1, generator.get_current());

  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObVersionLocalMacroSeq::build_window(-1, window));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObVersionLocalMacroSeq::get_raw_seq(-1, raw_seq));
}

static void check_copy_ss_parallel_windows(const int64_t parallel_cnt)
{
  const int64_t step_size = ObIncSSMacroSeq::get_step_size();
  int64_t previous_start_seq = -step_size;
  for (int64_t task_idx = 0; task_idx < parallel_cnt; ++task_idx) {
    int64_t start_seq = -1;
    ASSERT_SUCC(ObIncSSMacroSeq::build_parallel_start_seq(task_idx, start_seq));
    ASSERT_EQ(task_idx * step_size, start_seq);
    ASSERT_EQ(previous_start_seq + step_size, start_seq);
    previous_start_seq = start_seq;
  }
}

// 验证 CopySS 的常用并发以及 43-bit sequence 边界均按 slot 分配独立窗口。
TEST(macro_seq_generator, copy_ss_supported_parallel_windows)
{
  check_copy_ss_parallel_windows(1);
  check_copy_ss_parallel_windows(128);

  const int64_t max_slot =
      static_cast<int64_t>(ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT)
      / ObIncSSMacroSeq::get_step_size() - 1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObIncSSMacroSeq::check_parallel_slot_count(0));
  ASSERT_SUCC(ObIncSSMacroSeq::check_parallel_slot_count(1));
  ASSERT_SUCC(ObIncSSMacroSeq::check_parallel_slot_count(max_slot + 1));
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObIncSSMacroSeq::check_parallel_slot_count(max_slot + 2));
  int64_t start_seq = -1;
  ASSERT_SUCC(ObIncSSMacroSeq::build_parallel_start_seq(max_slot, start_seq));
  ASSERT_EQ(max_slot * ObIncSSMacroSeq::get_step_size(), start_seq);

  start_seq = -1;
  ASSERT_EQ(OB_SIZE_OVERFLOW,
      ObIncSSMacroSeq::build_parallel_start_seq(max_slot + 1, start_seq));
  ASSERT_EQ(-1, start_seq);
}

TEST(macro_seq_generator, version_local_resume_keeps_window_end)
{
  ObMacroSeqWindow window;
  ObMacroSeqParam seq_param;
  ObMacroIncSeqGenerator generator;
  int64_t seq_val = -1;
  ASSERT_SUCC(ObVersionLocalMacroSeq::build_window(11, window));
  const int64_t resume_start = window.start_ + 123;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = resume_start;
  ASSERT_SUCC(ObVersionLocalMacroSeq::calc_inc_major_data_seq_end(
      11, seq_param.start_, seq_param.end_));
  ASSERT_EQ(resume_start, seq_param.start_);
  ASSERT_EQ(window.end_, seq_param.end_);
  ASSERT_SUCC(generator.init(seq_param));
  ASSERT_SUCC(generator.get_next(seq_val));
  ASSERT_EQ(resume_start, seq_val);
  generator.current_ = window.end_ - 2;
  ASSERT_SUCC(generator.get_next(seq_val));
  ASSERT_EQ(window.end_ - 1, seq_val);
  ASSERT_EQ(OB_SIZE_OVERFLOW, generator.get_next(seq_val));

  int64_t invalid_end = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObVersionLocalMacroSeq::calc_inc_major_data_seq_end(
                12, resume_start, invalid_end));
  ASSERT_EQ(-1, invalid_end);
}

TEST(macro_seq_generator, version_local_last_window)
{
  ObMacroSeqWindow first_window;
  ObMacroSeqWindow last_window;
  ObMacroSeqWindow last_data_window;
  ObMacroSeqWindow index_window;
  ObMacroSeqWindow persister_window;
  ObMacroSeqWindow invalid_window;
  ASSERT_SUCC(ObVersionLocalMacroSeq::build_window(0, first_window));
  const int64_t step_size = first_window.end_ - first_window.start_;
  const int64_t sequence_limit =
      static_cast<int64_t>(ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT);
  const int64_t last_slot = sequence_limit / step_size - 1;
  ASSERT_EQ(1LL << ObMacroDataSeq::BIT_DATA_SEQ, step_size);
  ASSERT_EQ(1LL << ObMacroDataSeq::BIT_PARALLEL_IDX,
            sequence_limit / step_size);
  ASSERT_EQ(2045, ObVersionLocalMacroSeq::get_inc_major_index_slot());
  ASSERT_EQ(2046, ObVersionLocalMacroSeq::get_inc_major_persister_slot());
  ASSERT_EQ(2047, last_slot);
  ASSERT_SUCC(ObVersionLocalMacroSeq::build_window(last_slot, last_window));
  ASSERT_EQ(sequence_limit - step_size, last_window.start_);
  ASSERT_EQ(sequence_limit, last_window.end_);
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObVersionLocalMacroSeq::build_window(last_slot + 1, invalid_window));

  ASSERT_SUCC(ObVersionLocalMacroSeq::build_inc_major_data_window(
      ObVersionLocalMacroSeq::get_inc_major_index_slot() - 1,
      last_data_window));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObVersionLocalMacroSeq::build_inc_major_data_window(
                ObVersionLocalMacroSeq::get_inc_major_index_slot(),
                index_window));
  ASSERT_SUCC(ObVersionLocalMacroSeq::build_inc_major_index_window(index_window));
  ASSERT_SUCC(ObVersionLocalMacroSeq::build_inc_major_persister_window(
      persister_window));
  ASSERT_EQ(last_data_window.end_, index_window.start_);
  ASSERT_EQ(index_window.end_, persister_window.start_);
  ASSERT_EQ(persister_window.end_, last_window.start_);
  ASSERT_EQ(sequence_limit, last_window.end_);
  int64_t data_end = 0;
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            ObVersionLocalMacroSeq::calc_inc_major_data_seq_end(
                ObVersionLocalMacroSeq::get_inc_major_index_slot(),
                index_window.start_,
                data_end));
  ASSERT_EQ(-1, data_end);

}

TEST(macro_seq_generator, version_local_packed_sequence)
{
  ObMacroSeqWindow window;
  ASSERT_SUCC(ObVersionLocalMacroSeq::build_window(7, window));
  ObMacroDataSeq packed_seq(window.start_ + 11);
  packed_seq.set_index_block();
  ASSERT_EQ(7, packed_seq.get_parallel_idx());
  ASSERT_EQ(11, packed_seq.data_seq_);
  int64_t raw_seq = -1;
  ObMacroSeqParam raw_seq_param;
  ObMacroSeqParam seq_param;
  ASSERT_SUCC(ObVersionLocalMacroSeq::get_raw_seq(
      packed_seq.macro_data_seq_, raw_seq));
  ASSERT_EQ(window.start_ + 11, raw_seq);
  ObMacroDataSeq packed_end(window.end_);
  packed_end.set_index_block();
  raw_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  raw_seq_param.start_ = raw_seq;
  raw_seq_param.end_ = window.end_;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param.start_ = packed_seq.macro_data_seq_;
  seq_param.end_ = packed_end.macro_data_seq_;
  ASSERT_NE(raw_seq_param.start_, seq_param.start_);
  ASSERT_NE(raw_seq_param.end_, seq_param.end_);
  ASSERT_EQ(packed_seq.macro_data_seq_, seq_param.start_);
  ASSERT_EQ(packed_seq.macro_data_seq_ + window.end_ - raw_seq,
            seq_param.end_);
  ObMacroIncSeqGenerator generator;
  int64_t seq_val = -1;
  ASSERT_SUCC(generator.init(seq_param));
  generator.current_ = seq_param.end_ - 2;
  ASSERT_SUCC(generator.get_next(seq_val));
  ASSERT_EQ(seq_param.end_ - 1, seq_val);
  ASSERT_EQ(OB_SIZE_OVERFLOW, generator.get_next(seq_val));
}

TEST(macro_seq_generator, skip_generator)
{
  ObMacroSkipSeqGenerator skip_generator;
  ASSERT_FALSE(skip_generator.ddl_seq_generator_.is_inited_);
  ObMacroSeqParam seq_param;
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  ASSERT_FAIL(skip_generator.init(seq_param));
  seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_SKIP;
  seq_param.start_ = 22;
  seq_param.interval_ = 3;
  seq_param.step_ = 100;
  ASSERT_SUCC(skip_generator.init(seq_param));
  ASSERT_TRUE(skip_generator.ddl_seq_generator_.is_inited_);
  int64_t seq_val = -1;
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 22);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 23);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 24);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 122);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 123);
  ASSERT_SUCC(skip_generator.get_next(seq_val)); ASSERT_EQ(seq_val, 124);
  ASSERT_EQ(seq_val, skip_generator.get_current());
  int64_t preview_next_val = -1;
  ASSERT_SUCC(skip_generator.preview_next(seq_val, preview_next_val));
  ASSERT_GT(preview_next_val, seq_val);
  ASSERT_EQ(seq_val, skip_generator.get_current());
}



#define LOG_FILE_PATH "./test_macro_seq_generator.log"

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
