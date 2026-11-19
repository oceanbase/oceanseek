/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include <gtest/gtest.h>
#include <map>
#include <memory>
#include <optional>
#include <vector>
#include "sql/engine/basic/utils/test_op_base.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/system_variable/ob_system_variable.h"
#include "sql/ob_sql_init.h"
#define private public
#include "sql/engine/basic/ob_hybrid_fusion_op.h"
#undef private

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
constexpr int64_t BATCH_SIZE = 8;

struct InputRow
{
  int64_t id_;
  std::optional<double> query_score_;
  std::optional<double> knn_score_;
  std::optional<double> second_knn_score_;
  double local_score_;
  bool skip_ = false;
};

void set_numeric_meta(ObExpr &expr, bool is_integer)
{
  expr.datum_meta_.type_ = is_integer ? ObIntType : ObDoubleType;
  expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
  expr.vec_value_tc_ = is_integer ? VEC_TC_INTEGER : VEC_TC_DOUBLE;
  expr.res_buf_len_ = sizeof(double);
  expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
      expr.datum_meta_.type_, CS_TYPE_BINARY, 0, false, false, 0);
  ASSERT_NE(nullptr, expr.basic_funcs_);
}

class InputExprMaker : public GenExprMaker
{
public:
  InputExprMaker(const std::vector<std::vector<InputRow>> &batches,
                int64_t column, bool rich_format, bool end_on_last_batch)
    : GenExprMaker(BATCH_SIZE, rich_format ? VEC_FIXED : VEC_UNIFORM),
      batches_(batches), column_(column), batch_idx_(0), end_on_last_batch_(end_on_last_batch)
  {}

  void reset() { batch_idx_ = 0; }

  void gen(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &ctx) override
  {
    if (batch_idx_ == batches_.size()) {
      brs.size_ = 0;
      brs.end_ = true;
    } else {
      const auto &rows = batches_[batch_idx_++];
      brs.size_ = rows.size();
      brs.end_ = end_on_last_batch_ && batch_idx_ == batches_.size();
      brs.all_rows_active_ = true;
      ASSERT_EQ(OB_SUCCESS, expr.init_vector(ctx, vec_format_, brs.size_));
      ObIVector *vec = expr.get_vector(ctx);
      ObDatum *datums = expr.locate_batch_datums(ctx);
      for (int64_t i = 0; i < brs.size_; ++i) {
        // Stored output can point datums into row-store memory. A fresh child
        // batch must restore its own writable storage, including after rescan.
        datums[i].ptr_ = ctx.frames_[expr.frame_idx_] + expr.res_buf_off_ + i * sizeof(double);
        const InputRow &row = rows[i];
        if (row.skip_) {
          brs.set_skip(i);
        }
        if (column_ == 0) {
          vec->set_int(i, row.id_);
        } else {
          std::optional<double> value;
          if (column_ == 1) {
            value = row.query_score_;
          } else if (column_ == 2) {
            value = row.knn_score_;
          } else if (column_ == 3) {
            value = row.second_knn_score_;
          } else {
            value = row.local_score_;
          }
          if (value.has_value()) {
            vec->set_double(i, *value);
          } else {
            vec->set_null(i);
          }
        }
      }
      expr.get_evaluated_flags(ctx).set_all(static_cast<int64_t>(0), brs.size_);
      expr.set_evaluated_projected(ctx);
    }
  }

protected:
  void setup_expr_meta(ObExpr &expr) override { set_numeric_meta(expr, column_ == 0); }

private:
  std::vector<std::vector<InputRow>> batches_;
  int64_t column_;
  size_t batch_idx_;
  bool end_on_last_batch_;
};

class ConstNumberExprMaker : public ExprMakerImpl
{
public:
  ConstNumberExprMaker(double value, bool is_integer)
    : value_(value), is_integer_(is_integer) {}

  int64_t frame_mem_size() const override
  {
    return compute_frame_mem_size(1, BATCH_SIZE, VEC_UNIFORM_CONST, sizeof(double));
  }

  void make(ObExpr &expr, ObEvalCtx &ctx) override
  {
    layout_frame(expr, ctx, frame_idx_, 1, BATCH_SIZE, VEC_UNIFORM_CONST, sizeof(double));
    set_numeric_meta(expr, is_integer_);
    expr.batch_result_ = false;
    expr.batch_idx_mask_ = 0;
    ObDatum *datum = expr.locate_batch_datums(ctx);
    datum->ptr_ = ctx.frames_[frame_idx_] + expr.res_buf_off_;
    if (is_integer_) {
      datum->set_int(static_cast<int64_t>(value_));
    } else {
      datum->set_double(value_);
    }
  }

private:
  double value_;
  bool is_integer_;
};

// Unit tests have no observer work-area manager. Only replace work-area setup;
// collection, global heaps, scoring, output and rescan use the production code.
class UnitHybridFusionOp : public ObHybridFusionOp
{
public:
  UnitHybridFusionOp(ObExecContext &ctx, const ObOpSpec &spec)
    : ObHybridFusionOp(ctx, spec, nullptr) {}

  int inner_open() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(row_store_.init(child_->get_spec().output_, BATCH_SIZE,
                               ObMemAttr(OB_SYS_TENANT_ID, "UTHybridFusion"),
                               INT64_MAX, false, 0, NONE_COMPRESSOR))) {
    } else if (OB_FAIL(row_store_reader_.init(&row_store_))) {
    } else if (OB_FAIL(init_constant_params())) {
    } else if (OB_FAIL(init_path_heaps())) {
    } else if (OB_FAIL(stored_rows_buffer_.prepare_allocate(BATCH_SIZE))) {
    }
    return ret;
  }
};
} // namespace

class TestHybridFusionOp : public TestOpBase, public testing::WithParamInterface<bool>
{
public:
  TestHybridFusionOp()
    : TestOpBase("TestHybridFusionOp"), spec_(allocator_, PHY_HYBRID_FUSION), op_(nullptr) {}

protected:
  void TearDown() override
  {
    if (op_ != nullptr) {
      op_->destroy();
      op_->~UnitHybridFusionOp();
      op_ = nullptr;
    }
  }

  void init_case(const std::vector<std::vector<InputRow>> &batches,
                 double min_score = 0.0,
                 double query_weight = 1.0,
                 double knn_weight = 1.0,
                 bool end_on_last_batch = true,
                 bool has_query = true,
                 ObFusionMethod method = ObFusionMethod::WEIGHT_SUM)
  {
    std::vector<GenExprMaker *> input_makers;
    for (int64_t col = 0; col < 5; ++col) {
      input_makers_.emplace_back(new InputExprMaker(batches, col, GetParam(), end_on_last_batch));
      input_makers.push_back(input_makers_.back().get());
    }
    // A tiny hits window and nonzero offset must not truncate FULL_RECALL output.
    ConstNumberExprMaker size(1, true), window(1, true), offset(7, true), threshold(min_score, false);
    ConstNumberExprMaker query_boost(query_weight, false), knn_boost(knn_weight, false), one(1, false);
    ConstNumberExprMaker query_k(1, true), knn_k(1, true), rank_constant(60, true);
    std::vector<ExprMaker *> constants = {&size, &window, &offset, &threshold,
        &query_boost, &knn_boost, &one, &query_k, &knn_k, &rank_constant};
    init_exec_env(input_makers, constants, spec_, BATCH_SIZE);
    for (int64_t i = 0; i < constants.size(); ++i) {
      constants[i]->make(constants_[i], eval_ctx_);
    }
    spec_.max_batch_size_ = BATCH_SIZE;
    spec_.use_rich_format_ = GetParam();
    child_utils_.spec()->use_rich_format_ = GetParam();
    spec_.fusion_method_ = method;
    spec_.fusion_iter_exec_mode_ = ObFusionIterExecMode::ROWKEY_SCORE_FULL_RECALL;
    spec_.search_index_ = has_query ? 0 : -1;
    spec_.size_expr_ = &constants_[0];
    spec_.rank_window_size_expr_ = &constants_[1];
    spec_.offset_expr_ = &constants_[2];
    spec_.min_score_expr_ = &constants_[3];
    spec_.rank_constant_expr_ = &constants_[9];
    ASSERT_EQ(OB_SUCCESS, spec_.weights_exprs_.init(3));
    ASSERT_EQ(OB_SUCCESS, spec_.path_top_k_limit_exprs_.init(3));
    ASSERT_EQ(OB_SUCCESS, spec_.score_expr_output_indices_.init(4));
    ASSERT_EQ(OB_SUCCESS, spec_.output_.init(5));
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, spec_.weights_exprs_.push_back(&constants_[4 + i]));
      ASSERT_EQ(OB_SUCCESS, spec_.path_top_k_limit_exprs_.push_back(&constants_[i == 0 ? 7 : 8]));
    }
    for (int64_t i = 0; i < 5; ++i) {
      ASSERT_EQ(OB_SUCCESS, spec_.output_.push_back(child_utils_.spec()->output_.at(i)));
      if (i > 0) {
        ASSERT_EQ(OB_SUCCESS, spec_.score_expr_output_indices_.push_back(i));
      }
    }
    op_ = new (allocator_.alloc(sizeof(UnitHybridFusionOp))) UnitHybridFusionOp(exec_ctx_, spec_);
    attach_mock_child(spec_, *op_);
    ASSERT_EQ(OB_SUCCESS, op_->open());
  }

  void verify_output(const std::map<int64_t, double> &expected, int64_t output_batch_size = BATCH_SIZE)
  {
    std::map<int64_t, double> actual;
    bool reached_end = false;
    for (int64_t batch_idx = 0; !reached_end && batch_idx < 32; ++batch_idx) {
      const ObBatchRows *brs = nullptr;
      ASSERT_EQ(OB_SUCCESS, op_->get_next_batch(output_batch_size, brs));
      ASSERT_NE(nullptr, brs);
      for (int64_t i = 0; i < brs->size_; ++i) {
        if (!brs->skip_->at(i)) {
          const int64_t id = GetParam() ? spec_.output_.at(0)->get_vector(eval_ctx_)->get_int(i)
              : spec_.output_.at(0)->locate_batch_datums(eval_ctx_)[i].get_int();
          const double score = GetParam() ? spec_.output_.at(4)->get_vector(eval_ctx_)->get_double(i)
              : spec_.output_.at(4)->locate_batch_datums(eval_ctx_)[i].get_double();
          ASSERT_TRUE(actual.emplace(id, score).second) << "duplicate id " << id;
        }
      }
      reached_end = brs->end_;
    }
    ASSERT_TRUE(reached_end);
    ASSERT_EQ(expected.size(), actual.size());
    for (const auto &entry : expected) {
      const auto found = actual.find(entry.first);
      ASSERT_NE(actual.end(), found) << "missing id " << entry.first;
      EXPECT_DOUBLE_EQ(entry.second, found->second) << "id " << entry.first;
    }
  }

  ObHybridFusionSpec spec_;
  UnitHybridFusionOp *op_;
  ObExpr constants_[10];
  std::vector<std::unique_ptr<InputExprMaker>> input_makers_;
};

TEST_P(TestHybridFusionOp, global_knn_loser_is_filtered_by_query_score)
{
  init_case({{{11, 0.350318471, 0.5, {}, 0.850318471}},
             {{1, {}, 1.0, {}, 1.0}}}, 0.5);
  verify_output({{1, 1.0}});
}

TEST_P(TestHybridFusionOp, global_knn_loser_keeps_query_match)
{
  init_case({{{11, 0.350318471, 0.5, {}, 0.850318471}},
             {{1, {}, 1.0, {}, 1.0}}});
  verify_output({{1, 1.0}, {11, 0.350318471}});
}

TEST_P(TestHybridFusionOp, global_knn_winner_keeps_query_contribution)
{
  init_case({{{1, 0.4, 0.8, {}, 1.2}}, {{2, {}, 0.5, {}, 0.5}}}, 1.0);
  verify_output({{1, 0.4 + 0.8}});
}

TEST_P(TestHybridFusionOp, zero_score_query_match_survives_knn_rejection)
{
  init_case({{{11, 0.0, 0.5, {}, 0.5}}, {{1, {}, 1.0, {}, 1.0}}});
  verify_output({{1, 1.0}, {11, 0.0}});
}

TEST_P(TestHybridFusionOp, weighted_query_boundary_is_inclusive)
{
  init_case({{{11, 0.25, 0.5, {}, 2.0}}, {{1, {}, 1.0, {}, 3.0}}}, 0.5, 2.0, 3.0);
  verify_output({{1, 3.0}, {11, 0.5}});
}

TEST_P(TestHybridFusionOp, each_knn_path_selects_its_own_global_winner)
{
  init_case({{{11, 0.25, 0.5, 0.9, 1.65}},
             {{1, {}, 1.0, 0.2, 1.2}}});
  verify_output({{1, 1.0}, {11, 0.25 + 0.9}});
}

TEST_P(TestHybridFusionOp, query_passthrough_and_buffered_output_are_disjoint)
{
  init_case({{{11, 0.25, 0.5, {}, 0.75}, {12, 0.4, {}, {}, 0.4}},
             {{1, {}, 1.0, {}, 1.0}, {13, 0.6, {}, {}, 0.6}}});
  verify_output({{1, 1.0}, {11, 0.25}, {12, 0.4}, {13, 0.6}});
}

TEST_P(TestHybridFusionOp, empty_nonterminal_and_skipped_batches_do_not_end_recall)
{
  init_case({{}, {{99, 10.0, 10.0, {}, 20.0, true}},
             {{11, 0.25, 0.5, {}, 0.75}}, {}, {{1, {}, 1.0, {}, 1.0}}});
  verify_output({{1, 1.0}, {11, 0.25}});
}

TEST_P(TestHybridFusionOp, separate_end_batch_finalizes_buffered_rows)
{
  init_case({{{11, 0.25, 0.5, {}, 0.75}}, {{1, {}, 1.0, {}, 1.0}}},
            0.0, 1.0, 1.0, false);
  verify_output({{1, 1.0}, {11, 0.25}});
}

TEST_P(TestHybridFusionOp, all_rows_below_final_min_score)
{
  init_case({{{11, 0.25, 0.5, {}, 0.75}}, {{1, {}, 1.0, {}, 1.0}}}, 2.0);
  verify_output({});
}

TEST_P(TestHybridFusionOp, only_query_hits_leave_no_buffered_output)
{
  init_case({{{11, 0.25, {}, {}, 0.25}, {12, 0.5, {}, {}, 0.5}}}, 0.5);
  verify_output({{12, 0.5}});
}

TEST_P(TestHybridFusionOp, empty_input)
{
  init_case({});
  verify_output({});
}

TEST_P(TestHybridFusionOp, knn_only_global_loser_is_not_an_output_candidate)
{
  init_case({{{11, {}, 0.5, 0.5, 1.0}}, {{1, {}, 1.0, {}, 1.0}},
             {{2, {}, {}, 1.0, 1.0}}}, 0.0, 1.0, 1.0, true, false);
  verify_output({{1, 1.0}, {2, 1.0}});
}

TEST_P(TestHybridFusionOp, stricter_min_score_does_not_promote_global_knn_losers)
{
  init_case({{{11, {}, 2.0 / 3, 2.0 / 3, 4.0 / 3}},
             {{1, {}, 1.0, {}, 1.0}}, {{2, {}, {}, 1.0, 1.0}}},
            1.25, 1.0, 1.0, true, false);
  verify_output({});
}

TEST_P(TestHybridFusionOp, zero_score_knn_winner_is_still_a_match)
{
  init_case({{{1, {}, 0.0, {}, 0.0}}}, 0.0, 1.0, 1.0, true, false);
  verify_output({{1, 0.0}});
}

TEST_P(TestHybridFusionOp, buffered_output_spans_multiple_batches)
{
  init_case({{{11, 0.25, 0.5, {}, 0.75}}, {{12, 0.4, 0.8, {}, 1.2}},
             {{1, {}, 1.0, {}, 1.0}}});
  verify_output({{1, 1.0}, {11, 0.25}, {12, 0.4}}, 1);
}

TEST_P(TestHybridFusionOp, rrf_membership_is_collected_before_heap_consumption)
{
  init_case({{{11, {}, 0.5, 0.5, 1.0}}, {{1, {}, 1.0, {}, 1.0}},
             {{2, {}, {}, 1.0, 1.0}}}, 0.0, 1.0, 1.0, true, false, ObFusionMethod::RRF);
  verify_output({{1, 1.0 / 61}, {2, 1.0 / 61}});
}

TEST_P(TestHybridFusionOp, rescan_rebuilds_global_candidates_and_scores)
{
  init_case({{{11, 0.25, 0.5, {}, 0.75}}, {{1, {}, 1.0, {}, 1.0}}});
  verify_output({{1, 1.0}, {11, 0.25}});
  for (const auto &maker : input_makers_) {
    maker->reset();
  }
  ASSERT_EQ(OB_SUCCESS, op_->rescan());
  verify_output({{1, 1.0}, {11, 0.25}});
}

INSTANTIATE_TEST_CASE_P(ScoreFormats, TestHybridFusionOp, testing::Bool());

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  (void)oceanbase::ObPreProcessSysVars::init_sys_var();
  (void)oceanbase::sql::init_sql_factories();
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}
