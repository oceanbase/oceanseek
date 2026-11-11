/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "sql/engine/ob_exec_context.h"

#include <gtest/gtest.h>

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/utils/expr_maker.h"
#include "sql/engine/ob_exec_context.h"
#define protected public
#define private public
#include "sql/engine/table/ob_parquet_table_row_iter.h"
#undef private
#undef protected

namespace oceanbase
{
namespace sql
{
namespace unittest
{

class TestFilterExecutor : public ObPushdownFilterExecutor
{
public:
  TestFilterExecutor(common::ObIAllocator &allocator, ObPushdownOperator &op)
      : ObPushdownFilterExecutor(allocator, op), col_ids_()
  {
  }

  common::ObIArray<uint64_t> &get_col_ids() override
  {
    return col_ids_;
  }

private:
  common::ObSEArray<uint64_t, 1> col_ids_;
};

TEST(TestParquetLazyRanges, FlushReadRangeBeforePositionDeleteGap)
{
  common::ObArenaAllocator allocator;
  ObExecContext exec_ctx(allocator);
  ObEvalCtx eval_ctx(exec_ctx);
  ObPushdownExprSpec expr_spec(allocator);
  ObPushdownOperator op(eval_ctx, expr_spec);
  TestFilterExecutor filter(allocator, op);
  common::ObBitmap *filter_result = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter.init_bitmap(2, filter_result));
  ASSERT_NE(nullptr, filter_result);
  ASSERT_EQ(OB_SUCCESS, filter_result->set(0));
  ASSERT_EQ(OB_SUCCESS, filter_result->set(1));

  ObParquetTableRowIterator iter;
  ObParquetTableRowIterator::BatchReadState batch(allocator);
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(0));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(1));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(1));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(1));

  ASSERT_EQ(OB_SUCCESS, iter.fill_selected_source_ranges(batch, *filter_result));
  ASSERT_EQ(2, batch.lazy_plan_.skip_ranges().count());
  ASSERT_EQ(2, batch.lazy_plan_.read_ranges().count());
  EXPECT_EQ(0, batch.lazy_plan_.skip_ranges().at(0));
  EXPECT_EQ(1, batch.lazy_plan_.read_ranges().at(0));
  EXPECT_EQ(1, batch.lazy_plan_.skip_ranges().at(1));
  EXPECT_EQ(1, batch.lazy_plan_.read_ranges().at(1));
}

TEST(TestParquetLazyRanges, PreserveFilterSkipsAcrossDeleteGaps)
{
  common::ObArenaAllocator allocator;
  ObExecContext exec_ctx(allocator);
  ObEvalCtx eval_ctx(exec_ctx);
  ObPushdownExprSpec expr_spec(allocator);
  ObPushdownOperator op(eval_ctx, expr_spec);
  TestFilterExecutor filter(allocator, op);
  common::ObBitmap *filter_result = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter.init_bitmap(5, filter_result));
  ASSERT_NE(nullptr, filter_result);
  ASSERT_EQ(OB_SUCCESS, filter_result->set(0));
  ASSERT_EQ(OB_SUCCESS, filter_result->set(1));
  ASSERT_EQ(OB_SUCCESS, filter_result->set(2));
  ASSERT_EQ(OB_SUCCESS, filter_result->set(4));

  ObParquetTableRowIterator iter;
  ObParquetTableRowIterator::BatchReadState batch(allocator);
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(0));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(2));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(2));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(2));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_skip_ranges().push_back(1));
  ASSERT_EQ(OB_SUCCESS, batch.source_plan_.mutable_read_ranges().push_back(1));

  ASSERT_EQ(OB_SUCCESS, iter.fill_selected_source_ranges(batch, *filter_result));
  ASSERT_EQ(3, batch.lazy_plan_.skip_ranges().count());
  ASSERT_EQ(3, batch.lazy_plan_.read_ranges().count());
  EXPECT_EQ(0, batch.lazy_plan_.skip_ranges().at(0));
  EXPECT_EQ(2, batch.lazy_plan_.read_ranges().at(0));
  EXPECT_EQ(2, batch.lazy_plan_.skip_ranges().at(1));
  EXPECT_EQ(1, batch.lazy_plan_.read_ranges().at(1));
  EXPECT_EQ(2, batch.lazy_plan_.skip_ranges().at(2));
  EXPECT_EQ(1, batch.lazy_plan_.read_ranges().at(2));
}

TEST(TestParquetLazyRanges, DynamicModeFollowsCumulativeSelectivity)
{
  ObParquetTableRowIterator iter;
  iter.mode_ = FilterCalcMode::DYNAMIC_EAGER_CALC;
  iter.stat_.projected_eager_cnt_ = 100;
  iter.stat_.projected_lazy_cnt_ = 67;

  iter.dynamic_switch_calc_mode();
  EXPECT_EQ(FilterCalcMode::DYNAMIC_LAZY_CALC, iter.mode_);

  // Model another row group whose low pass rate lowers the cumulative ratio to 50%.
  iter.stat_.projected_eager_cnt_ = 200;
  iter.stat_.projected_lazy_cnt_ = 100;
  iter.dynamic_switch_calc_mode();
  EXPECT_EQ(FilterCalcMode::DYNAMIC_EAGER_CALC, iter.mode_);

  iter.mode_ = FilterCalcMode::FORCE_LAZY_CALC;
  iter.stat_.projected_eager_cnt_ = 300;
  iter.stat_.projected_lazy_cnt_ = 10;
  iter.dynamic_switch_calc_mode();
  EXPECT_EQ(FilterCalcMode::FORCE_LAZY_CALC, iter.mode_);
}

TEST(TestParquetLazyRanges, MissingPhysicalOutputKeepsStrictHelperError)
{
  ObParquetTableRowIterator iter;
  common::ObSEArray<std::pair<uint64_t, uint64_t>, 1> mapping;
  common::ObSEArray<ObExpr *, 1> outputs;
  common::ObSEArray<bool, 1> mask;
  ObExpr output;
  output.type_ = T_REF_COLUMN;
  ASSERT_EQ(OB_SUCCESS, outputs.push_back(&output));
  ASSERT_EQ(OB_SUCCESS, mask.push_back(true));
  EXPECT_EQ(OB_SEARCH_NOT_FOUND, iter.generate_lazy_exprs(mapping, outputs, mask));
  ASSERT_EQ(OB_SUCCESS, mapping.push_back(std::make_pair(16, 0)));
  EXPECT_EQ(OB_SUCCESS, iter.generate_lazy_exprs(mapping, outputs, mask));
  ASSERT_EQ(1, iter.lazy_columns_.count());
  EXPECT_EQ(0, iter.lazy_columns_.at(0));
}

TEST(TestParquetLazyRanges, MetadataEagerConversionAndCompactionRefreshEveryBatch)
{
  common::ObArenaAllocator arena;
  ObExecContext exec(arena);
  ObEvalCtx eval(exec);
  const int64_t capacity = 8;
  SeqIntGenExprMaker maker(capacity, capacity, VEC_FIXED);
  ExprMaker &frames = maker;
  eval.max_batch_size_ = capacity;
  eval.frames_ = static_cast<char **>(arena.alloc(2 * sizeof(char *)));
  ASSERT_NE(nullptr, eval.frames_);
  for (int64_t i = 0; i < 2; ++i) {
    eval.frames_[i] = static_cast<char *>(arena.alloc(frames.frame_mem_size()));
    ASSERT_NE(nullptr, eval.frames_[i]);
    MEMSET(eval.frames_[i], 0, frames.frame_mem_size());
  }
  exec.set_frames(eval.frames_);
  exec.set_frame_cnt(2);
  ObExpr source;
  ObExpr output;
  frames.set_frame_idx(0);
  frames.make(source, eval);
  source.type_ = T_PSEUDO_PARTITION_LIST_COL;
  frames.set_frame_idx(1);
  frames.make(output, eval);
  output.type_ = T_REF_COLUMN;
  ExprFixedArray dependencies(arena);
  ASSERT_EQ(OB_SUCCESS, dependencies.init(1));
  ASSERT_EQ(OB_SUCCESS, dependencies.push_back(&source));
  storage::ObTableScanParam param;
  param.ext_column_dependent_exprs_ = &dependencies;
  ObParquetTableRowIterator iter;
  iter.scan_param_ = &param;
  ASSERT_EQ(OB_SUCCESS, iter.column_need_conv_.init(1));
  ASSERT_EQ(OB_SUCCESS, iter.column_exprs_.push_back(&output));
  ASSERT_EQ(OB_SUCCESS, iter.column_need_conv_.push_back(true));
  ASSERT_EQ(OB_SUCCESS, iter.eager_output_indices_.push_back(0));
  ObPushdownExprSpec expr_spec(arena);
  ObPushdownOperator op(eval, expr_spec);
  TestFilterExecutor filter(arena, op);
  common::ObBitmap *selected = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter.init_bitmap(capacity, selected));
  for (int round = 0; round < 3; ++round) {
    int64_t count = capacity;
    ASSERT_EQ(OB_SUCCESS, source.init_vector(eval, VEC_UNIFORM_CONST, count));
    ObDatum &value = source.locate_expr_datum(eval);
    if (round == 1) { value.set_null(); } else { value.set_int(17 + round); }
    source.set_evaluated_projected(eval);
    // Deliberately retain the output's old evaluated flag across batches.
    ASSERT_EQ(OB_SUCCESS, iter.calc_column_convert(count, eval,
        ObParquetTableRowIterator::ColumnConvertScope::EAGER_COLUMNS));
    ASSERT_EQ(VEC_UNIFORM, output.get_vector(eval)->get_format());
    selected->reuse();
    ASSERT_EQ(OB_SUCCESS, selected->set(2));
    ASSERT_EQ(OB_SUCCESS, selected->set(6));
    ASSERT_EQ(OB_SUCCESS, iter.reorder_output(*selected, eval, count, true));
    ASSERT_EQ(2, count);
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(round == 1, output.get_vector(eval)->is_null(i));
      if (round != 1) { EXPECT_EQ(17 + round, output.get_vector(eval)->get_int(i)); }
    }
  }
  // Exercise the framework's scalar evaluator fallback through the same conversion API.
  iter.bit_vector_cache_ = to_bit_vector(arena.alloc(ObBitVector::memory_size(capacity)));
  ASSERT_NE(nullptr, iter.bit_vector_cache_);
  iter.bit_vector_cache_->reset(capacity);
  source.get_eval_info(eval).clear_evaluated_flag();
  source.get_evaluated_flags(eval).reset(capacity);
  source.eval_vector_func_ = expr_default_eval_vector_func;
  source.eval_batch_func_ = expr_default_eval_batch_func;
  source.eval_func_ = [](const ObExpr &, ObEvalCtx &ctx, ObDatum &datum) {
    datum.set_int(100 + ctx.get_batch_idx());
    return OB_SUCCESS;
  };
  int64_t count = capacity;
  ASSERT_EQ(OB_SUCCESS, iter.calc_column_convert(count, eval,
      ObParquetTableRowIterator::ColumnConvertScope::EAGER_COLUMNS));
  ASSERT_EQ(OB_SUCCESS, iter.reorder_output(*selected, eval, count, true));
  ASSERT_EQ(2, count);
  EXPECT_EQ(102, output.get_vector(eval)->get_int(0));
  EXPECT_EQ(106, output.get_vector(eval)->get_int(1));
  ASSERT_EQ(OB_SUCCESS, iter.eager_columns_.push_back(99));
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.calc_column_convert(1, eval,
      ObParquetTableRowIterator::ColumnConvertScope::EAGER_COLUMNS));
}

} // namespace unittest
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
