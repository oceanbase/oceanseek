/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstddef>
#include <vector>
#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "sql/engine/expr/ob_expr_doc_id.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_batch_selector.h"
#include "storage/ddl/ob_ddl_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
namespace sql
{

class DocIdInsertOp : public ObPxMultiPartSSTableInsertOp
{
public:
  DocIdInsertOp(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxMultiPartSSTableInsertOp(ctx, spec, nullptr) {}
  using ObPxMultiPartSSTableInsertOp::eval_idempotent_doc_id_batch;
};

class TestSSTableInsertDocId : public ::testing::Test
{
protected:
  enum : int64_t { BATCH_SIZE = 8 };
  struct ExprFrame
  {
    ObDatum datums_[BATCH_SIZE];
    ObEvalInfo info_{};
    uint64_t flags_ = 0;
    VectorHeader vector_;
    uint64_t result_[BATCH_SIZE][2] = {};
  };
  struct DocIdEvalState
  {
    int64_t call_count_ = 0;
    int64_t fail_at_ = -1;
  };

  static int generate_doc_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum)
  {
    DocIdEvalState &state = *reinterpret_cast<DocIdEvalState *>(expr.extra_);
    ++state.call_count_;
    return ctx.get_batch_idx() == state.fail_at_
        ? OB_ALLOCATE_MEMORY_FAILED : ObExprDocID::generate_doc_id(expr, ctx, datum);
  }

  TestSSTableInsertDocId()
    : ctx_(allocator_), spec_(allocator_, PHY_VEC_PX_MULTI_PART_SSTABLE_INSERT), op_(nullptr)
  {}

  void SetUp() override
  {
    ctx_.set_my_session(&session_);
    spec_.max_batch_size_ = BATCH_SIZE;
    spec_.use_rich_format_ = true;
    frames_[0] = reinterpret_cast<char *>(&part_frame_);
    frames_[1] = reinterpret_cast<char *>(&doc_frame_);
    ctx_.set_frames(frames_);
    init_expr(part_expr_, 0);
    init_expr(doc_expr_, 1);
    part_expr_.datum_meta_.type_ = ObCharType;
    part_expr_.obj_meta_.set_binary();
    doc_expr_.type_ = T_FUN_SYS_DOC_ID;
    doc_expr_.eval_func_ = generate_doc_id;
    doc_expr_.extra_ = reinterpret_cast<uint64_t>(&eval_state_);
    doc_expr_.eval_batch_func_ = expr_default_eval_batch_func;
    doc_expr_.eval_vector_func_ = expr_default_eval_vector_func;
    args_[0] = &part_expr_;
    doc_expr_.args_ = args_;
    doc_expr_.arg_cnt_ = 1;
    ASSERT_EQ(OB_SUCCESS, spec_.ins_ctdef_.new_row_.init(2));
    ASSERT_EQ(OB_SUCCESS, spec_.ins_ctdef_.new_row_.push_back(&doc_expr_));
    op_ = new DocIdInsertOp(ctx_, spec_);
    brs_.size_ = BATCH_SIZE;
    brs_.skip_ = reinterpret_cast<ObBitVector *>(&skip_bits_);
    brs_.all_rows_active_ = false;
    autoinc_.need_autoinc_ = true;
    autoinc_.slice_count_ = 2;
    autoinc_.slice_idx_ = 0;
    // Use a small interval to cover allocation-range boundaries in one batch.
    autoinc_.autoinc_range_interval_ = 4;
    reset_batch(1001);
  }

  void TearDown() override { delete op_; }

  void init_expr(ObExpr &expr, const int64_t frame_idx)
  {
    expr.frame_idx_ = frame_idx;
    expr.datum_off_ = offsetof(ExprFrame, datums_);
    expr.eval_info_off_ = offsetof(ExprFrame, info_);
    expr.eval_flags_off_ = offsetof(ExprFrame, flags_);
    expr.vector_header_off_ = offsetof(ExprFrame, vector_);
    expr.res_buf_off_ = offsetof(ExprFrame, result_);
    expr.res_buf_len_ = sizeof(ObDocId);
    expr.batch_result_ = true;
    expr.batch_idx_mask_ = UINT64_MAX;
    expr.datum_meta_.type_ = ObVarcharType;
    expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
    expr.obj_meta_.set_varbinary();
    expr.vec_value_tc_ = VEC_TC_STRING;
    expr.max_length_ = sizeof(ObDocId);
  }

  void reset_batch(const uint64_t tablet_id)
  {
    skip_bits_ = 0;
    doc_frame_.info_.clear_evaluated_flag();
    part_frame_.info_.set_projected(true);
    part_frame_.vector_.format_ = VEC_INVALID;
    for (int64_t i = 0; i < BATCH_SIZE; ++i) {
      part_frame_.result_[i][0] = 1;
      part_frame_.result_[i][1] = tablet_id;
      part_frame_.datums_[i].set_string(
          reinterpret_cast<char *>(part_frame_.result_[i]), sizeof(part_frame_.result_[i]));
    }
  }

  void evaluate(const int64_t offset, const int64_t count, const int64_t written,
                std::vector<ObDocId> &ids)
  {
    ObArray<ObIVector *> vectors;
    ASSERT_EQ(OB_SUCCESS, op_->eval_idempotent_doc_id_batch(
        vectors, brs_, ObBatchSelector(offset, count), autoinc_, written));
    ASSERT_EQ(1, vectors.count());
    for (int64_t i = offset; i < offset + count; ++i) {
      const char *payload = nullptr;
      int32_t length = 0;
      vectors.at(0)->get_payload(i, payload, length);
      ObDocId id;
      ASSERT_EQ(OB_SUCCESS, id.from_string(ObString(length, payload)));
      ids.push_back(id);
    }
  }

  ObArenaAllocator allocator_;
  ObSQLSessionInfo session_;
  ObExecContext ctx_;
  ObPxMultiPartSSTableInsertVecSpec spec_;
  ExprFrame part_frame_;
  ExprFrame doc_frame_;
  char *frames_[2];
  ObExpr part_expr_;
  ObExpr doc_expr_;
  ObExpr *args_[1];
  DocIdInsertOp *op_;
  uint64_t skip_bits_ = 0;
  ObBatchRows brs_;
  ObDDLAutoincParam autoinc_;
  DocIdEvalState eval_state_;
};

TEST_F(TestSSTableInsertDocId, retry_with_different_batch_boundaries)
{
  std::vector<ObDocId> first;
  brs_.skip_->set(0);
  brs_.skip_->set(3);
  evaluate(1, 2, 0, first);
  evaluate(4, 3, 2, first);
  reset_batch(1001);
  evaluate(0, 3, 5, first);

  // Replaying the same slice with different batch boundaries must produce
  // byte-identical doc IDs, including across the autoincrement range boundary.
  std::vector<ObDocId> retry;
  reset_batch(1001);
  evaluate(0, 8, 0, retry);
  ASSERT_EQ(first.size(), retry.size());
  const uint64_t expected[] = {0, 1, 2, 3, 8, 9, 10, 11};
  ASSERT_EQ(8, first.size());
  for (int64_t i = 0; i < 8; ++i) {
    EXPECT_TRUE(first[i] == retry[i]);
    EXPECT_EQ(1001, first[i].tablet_id_);
    EXPECT_EQ(expected[i], first[i].seq_id_);
  }
  EXPECT_EQ(16, eval_state_.call_count_);
}

TEST_F(TestSSTableInsertDocId, slice_and_tablet_changes_in_one_batch)
{
  std::vector<ObDocId> ids;
  evaluate(0, 2, 0, ids);
  autoinc_.slice_idx_ = 1;
  evaluate(2, 2, 0, ids);
  autoinc_.slice_idx_ = 0;
  autoinc_.slice_count_ = 3;
  for (int64_t i = 4; i < 8; ++i) {
    part_frame_.result_[i][1] = 1002;
  }
  evaluate(4, 4, 0, ids);
  ASSERT_EQ(8, ids.size());
  const uint64_t expected[] = {0, 1, 4, 5, 0, 1, 2, 3};
  for (int64_t i = 0; i < 8; ++i) {
    EXPECT_EQ(i < 4 ? 1001 : 1002, ids[i].tablet_id_);
    EXPECT_EQ(expected[i], ids[i].seq_id_);
  }
  EXPECT_EQ(8, eval_state_.call_count_);
}

TEST_F(TestSSTableInsertDocId, scalar_columns_keep_constant_vector_format)
{
  part_expr_.batch_result_ = false;
  part_expr_.batch_idx_mask_ = 0;
  ASSERT_EQ(OB_SUCCESS, spec_.ins_ctdef_.new_row_.push_back(&part_expr_));
  ObArray<ObIVector *> vectors;
  ASSERT_EQ(OB_SUCCESS, op_->eval_idempotent_doc_id_batch(
      vectors, brs_, ObBatchSelector(2, 3), autoinc_, 0));
  ASSERT_EQ(2, vectors.count());
  EXPECT_EQ(VEC_UNIFORM, vectors.at(0)->get_format());
  EXPECT_EQ(VEC_UNIFORM_CONST, vectors.at(1)->get_format());
  EXPECT_EQ(3, eval_state_.call_count_);
  for (int64_t i = 2; i < 5; ++i) {
    EXPECT_EQ(part_frame_.datums_[0].ptr_, vectors.at(1)->get_payload(i));
    ObDocId doc_id;
    ASSERT_EQ(OB_SUCCESS, doc_id.from_string(
        ObString(vectors.at(0)->get_length(i), vectors.at(0)->get_payload(i))));
    EXPECT_EQ(1001, doc_id.tablet_id_);
    EXPECT_EQ(i - 2, doc_id.seq_id_);
  }
}

TEST_F(TestSSTableInsertDocId, evaluation_failure_preserves_error_and_batch_context)
{
  ObEvalCtx &eval_ctx = op_->get_eval_ctx();
  ObEvalCtx::BatchInfoScopeGuard batch_guard(eval_ctx);
  batch_guard.set_batch_size(3);
  batch_guard.set_batch_idx(2);
  eval_state_.fail_at_ = 4;
  ObArray<ObIVector *> vectors;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, op_->eval_idempotent_doc_id_batch(
      vectors, brs_, ObBatchSelector(2, 4), autoinc_, 0));
  EXPECT_EQ(3, eval_state_.call_count_);
  EXPECT_EQ(0, vectors.count());
  EXPECT_EQ(3, eval_ctx.get_batch_size());
  EXPECT_EQ(2, eval_ctx.get_batch_idx());
  EXPECT_FALSE(doc_expr_.get_evaluated_flags(eval_ctx).at(4));
  EXPECT_FALSE(doc_expr_.get_evaluated_flags(eval_ctx).at(5));
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
