/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/ob_server_struct.h"
#include "share/system_variable/ob_system_variable.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_ai/ob_expr_ai_decide.h"
#include "sql/ob_sql_init.h"
#include "sql/session/ob_sql_session_info.h"
#include "unittest/sql/engine/basic/utils/expr_maker.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

class TestAIDecideVector : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, init_sql_factories());
  }

protected:
  static constexpr int64_t SIZE = 8;
  using StringMaker = FixedGenExprMaker<std::string>;

  TestAIDecideVector()
      : arena_("UTDecideVec"), exec_ctx_(arena_), ctx_(exec_ctx_), skip_(nullptr)
  {}

  void SetUp() override
  {
    // A mistaken eager model lookup cannot succeed against a test schema service.
    ASSERT_EQ(nullptr, GCTX.schema_service_);
    arena_.set_attr(ObMemAttr(OB_SYS_TENANT_ID, "UTDecideVec"));
    lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
    ASSERT_NE(nullptr, allocator);
    (void)allocator->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, &arena_));
    ASSERT_EQ(OB_SUCCESS, session_.load_default_sys_variable(false, true));
    ASSERT_EQ(OB_SUCCESS, session_.init_tenant(ObString("test"), OB_SYS_TENANT_ID));
    exec_ctx_.set_my_session(&session_);
    ctx_.max_batch_size_ = SIZE;
    ctx_.reuse(SIZE);
    ctx_.frames_ = static_cast<char **>(arena_.alloc(4 * sizeof(char *)));
    ASSERT_NE(nullptr, ctx_.frames_);
    skip_ = to_bit_vector(arena_.alloc(ObBitVector::memory_size(SIZE)));
    ASSERT_NE(nullptr, skip_);
    skip_->reset(SIZE);
    const char *values[] = {"missing/model", "state", "not-json", "seed"};
    for (int64_t i = 0; i < 4; ++i) {
      std::vector<std::optional<std::string>> rows(SIZE, std::string(values[i]));
      makers_[i].reset(new StringMaker(SIZE, {rows}, VEC_DISCRETE));
      // ExprMaker's base API exposes frame assignment; derived helpers keep it protected.
      ExprMaker *maker = makers_[i].get();
      maker->set_frame_idx(i);
      ctx_.frames_[i] = static_cast<char *>(arena_.alloc(maker->frame_mem_size()));
      ASSERT_NE(nullptr, ctx_.frames_[i]);
      MEMSET(ctx_.frames_[i], 0, maker->frame_mem_size());
      maker->make(exprs_[i], ctx_);
      ObBatchRows rows_to_generate;
      rows_to_generate.size_ = SIZE;
      rows_to_generate.skip_ = skip_;
      makers_[i]->gen(exprs_[i], rows_to_generate, ctx_);
    }
    exec_ctx_.set_frames(ctx_.frames_);
    exec_ctx_.set_frame_cnt(4);
    for (int64_t i = 0; i < 3; ++i) { args_[i] = &exprs_[i]; }
    ObExpr &out = output();
    out.args_ = args_;
    out.arg_cnt_ = 3;
    out.type_ = T_FUN_SYS_AI_DECIDE;
    out.datum_meta_.type_ = ObJsonType;
    out.datum_meta_.cs_type_ = CS_TYPE_UTF8MB4_BIN;
    out.vec_value_tc_ = VEC_TC_JSON;
    out.basic_funcs_ = ObDatumFuncs::get_basic_func(ObJsonType, CS_TYPE_UTF8MB4_BIN,
        out.datum_meta_.scale_, false, false, out.datum_meta_.precision_);
    ASSERT_NE(nullptr, out.basic_funcs_);
    ASSERT_EQ(OB_SUCCESS, out.init_vector(ctx_, VEC_DISCRETE, SIZE));
    for (int64_t i = 0; i < SIZE; ++i) {
      ObJsonInt marker(1000 + i);
      ASSERT_EQ(OB_SUCCESS, marker.get_raw_binary(sentinels_[i], &arena_));
      out.get_vector(ctx_)->set_string(i, sentinels_[i]);
    }
    out.get_evaluated_flags(ctx_).reset(SIZE);
  }

  ObExpr &output() { return exprs_[3]; }

  void make_uniform_constant(int64_t argument, const char *value)
  {
    ObExpr &expr = exprs_[argument];
    expr.batch_result_ = false;
    expr.batch_idx_mask_ = 0;
    ASSERT_EQ(OB_SUCCESS, expr.init_vector(ctx_, VEC_UNIFORM_CONST, 1));
    expr.get_vector(ctx_)->set_string(0, ObString::make_string(value));
    expr.get_eval_info(ctx_).set_evaluated(true);
    expr.is_static_const_ = true;
  }

  void expect_preserved(int64_t row)
  {
    ASSERT_FALSE(output().get_vector(ctx_)->is_null(row));
    ASSERT_EQ(sentinels_[row], output().get_vector(ctx_)->get_string(row));
  }

  static int fail_argument(const ObExpr &, ObEvalCtx &, const ObBitVector &, const EvalBound &)
  {
    return OB_ERR_DIVISOR_IS_ZERO;
  }

  ObArenaAllocator arena_;
  ObSQLSessionInfo session_;
  ObExecContext exec_ctx_;
  ObEvalCtx ctx_;
  ObExpr exprs_[4];
  ObExpr *args_[3];
  // StringMaker vectors borrow strings in these makers. Retain them for the whole test.
  std::unique_ptr<StringMaker> makers_[4];
  ObBitVector *skip_;
  ObString sentinels_[SIZE];
};

TEST_F(TestAIDecideVector, argument_count_errors_are_reported_before_evaluation)
{
  output().arg_cnt_ = 2;
  ObDatum scalar_result;
  EXPECT_EQ(OB_ERR_PARAM_SIZE, ObExprAIDecide::eval_ai_decide(output(), ctx_, scalar_result));
  EXPECT_EQ(OB_ERR_PARAM_SIZE,
      ObExprAIDecide::eval_ai_decide_vector(output(), ctx_, *skip_, EvalBound(SIZE, true)));
  for (int64_t i = 0; i < SIZE; ++i) {
    EXPECT_FALSE(output().get_evaluated_flags(ctx_).at(i));
    expect_preserved(i);
  }
}

TEST_F(TestAIDecideVector, every_row_has_a_null_argument_before_model_or_json_resolution)
{
  for (int64_t i = 0; i < SIZE; ++i) { exprs_[i % 3].get_vector(ctx_)->set_null(i); }
  ASSERT_EQ(OB_SUCCESS,
            ObExprAIDecide::eval_ai_decide_vector(
                output(), ctx_, *skip_, EvalBound(SIZE, true)));
  for (int64_t i = 0; i < SIZE; ++i) {
    ASSERT_TRUE(output().get_vector(ctx_)->is_null(i));
    ASSERT_TRUE(output().get_evaluated_flags(ctx_).at(i));
  }
  ASSERT_EQ(nullptr, GCTX.schema_service_);
}

TEST_F(TestAIDecideVector, all_null_state_does_not_resolve_uniform_constant_model_or_questions)
{
  make_uniform_constant(0, "missing/model");
  make_uniform_constant(2, "not-json");
  for (int64_t i = 0; i < SIZE; ++i) { exprs_[1].get_vector(ctx_)->set_null(i); }
  ASSERT_EQ(OB_SUCCESS,
            ObExprAIDecide::eval_ai_decide_vector(
                output(), ctx_, *skip_, EvalBound(SIZE, true)));
  for (int64_t i = 0; i < SIZE; ++i) {
    ASSERT_TRUE(output().get_vector(ctx_)->is_null(i));
    ASSERT_TRUE(output().get_evaluated_flags(ctx_).at(i));
  }
}

TEST_F(TestAIDecideVector, sparse_bound_preserves_skipped_and_evaluated_slots_across_reentry)
{
  skip_->set(2);
  skip_->set(5);
  output().get_evaluated_flags(ctx_).set(3);
  output().get_evaluated_flags(ctx_).set(6);
  exprs_[1].get_vector(ctx_)->set_null(1);
  exprs_[1].get_vector(ctx_)->set_null(4);
  const EvalBound bound(SIZE, 1, 7, false);
  ASSERT_EQ(OB_SUCCESS, ObExprAIDecide::eval_ai_decide_vector(output(), ctx_, *skip_, bound));
  for (int64_t i : {1, 4}) {
    ASSERT_TRUE(output().get_vector(ctx_)->is_null(i));
    ASSERT_TRUE(output().get_evaluated_flags(ctx_).at(i));
  }
  for (int64_t i : {0, 2, 3, 5, 6, 7}) { expect_preserved(i); }
  for (int64_t i : {0, 2, 5, 7}) { ASSERT_FALSE(output().get_evaluated_flags(ctx_).at(i)); }
  for (int64_t i : {3, 6}) { ASSERT_TRUE(output().get_evaluated_flags(ctx_).at(i)); }

  // Already evaluated rows now contain non-NULL arguments that would fail resolution.
  for (int64_t i : {1, 4}) {
    exprs_[1].get_vector(ctx_)->set_string(i, ObString::make_string("state"));
  }
  ASSERT_EQ(OB_SUCCESS, ObExprAIDecide::eval_ai_decide_vector(output(), ctx_, *skip_, bound));
  for (int64_t i : {1, 4}) { ASSERT_TRUE(output().get_vector(ctx_)->is_null(i)); }
  for (int64_t i : {0, 2, 3, 5, 6, 7}) { expect_preserved(i); }
}

TEST_F(TestAIDecideVector, null_argument_does_not_swallow_another_argument_evaluation_error)
{
  for (int64_t i = 0; i < SIZE; ++i) { exprs_[0].get_vector(ctx_)->set_null(i); }
  exprs_[2].eval_vector_func_ = fail_argument;
  exprs_[2].get_eval_info(ctx_).set_evaluated(false);
  exprs_[2].get_evaluated_flags(ctx_).reset(SIZE);
  ASSERT_EQ(OB_ERR_DIVISOR_IS_ZERO,
      ObExprAIDecide::eval_ai_decide_vector(output(), ctx_, *skip_, EvalBound(SIZE, true)));
}

} // namespace sql
} // namespace oceanbase
