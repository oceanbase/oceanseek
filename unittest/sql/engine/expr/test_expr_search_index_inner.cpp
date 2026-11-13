/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstddef>
#include <string>
#include "sql/engine/expr/ob_expr_search_index_inner.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/rewrite/ob_query_range.h"
#include "share/search_index/ob_search_index_encoder.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using share::ObSearchIndexValueEncoder;

class TestSearchIndexInnerValue : public ::testing::Test
{
protected:
  struct Frame
  {
    ObDatum datum_;
    ObEvalInfo info_{};
    alignas(uint64_t) char buffer_[16];
  };

  TestSearchIndexInnerValue() : exec_ctx_(allocator_), eval_ctx_(exec_ctx_, &allocator_)
  {
    for (int i = 0; i < 3; ++i) {
      frames_[i] = reinterpret_cast<char *>(&storage_[i]);
      exprs_[i].frame_idx_ = i;
      exprs_[i].datum_off_ = offsetof(Frame, datum_);
      exprs_[i].eval_info_off_ = offsetof(Frame, info_);
      exprs_[i].res_buf_off_ = offsetof(Frame, buffer_);
      exprs_[i].res_buf_len_ = sizeof(storage_[i].buffer_);
      storage_[i].datum_.ptr_ = storage_[i].buffer_;
    }
    eval_ctx_.frames_ = frames_;
    exprs_[0].obj_meta_.set_int();
    exprs_[1].obj_meta_.set_int();
    exprs_[2].obj_meta_.set_varbinary();
    exprs_[2].datum_meta_.type_ = ObVarcharType;
    exprs_[2].datum_meta_.cs_type_ = CS_TYPE_BINARY;
    args_[0] = &exprs_[0];
    args_[1] = &exprs_[1];
    exprs_[2].args_ = args_;
    exprs_[2].arg_cnt_ = 2;
    exprs_[2].eval_func_ = ObExprSearchIndexInnerValue::calc_search_index_inner_value;
  }

  int evaluate(std::string &key, bool &is_null)
  {
    // Simulate successive executions of the same generated expression: only
    // parameter datums and evaluated flags change, not expression metadata.
    exprs_[2].get_eval_info(eval_ctx_).clear_evaluated_flag();
    ObDatum *result = nullptr;
    int ret = exprs_[2].eval(eval_ctx_, result);
    if (OB_SUCCESS == ret) {
      is_null = result->is_null();
      key = is_null ? std::string() : std::string(result->ptr_, result->len_);
    }
    return ret;
  }

  void check_signed(const ObObjType target_type, const int64_t input, const int expected_bound)
  {
    exprs_[0].obj_meta_.set_int();
    storage_[0].datum_.set_int(input);
    check_value(target_type, expected_bound);
  }

  void check_unsigned(const ObObjType target_type, const uint64_t input, const int expected_bound)
  {
    exprs_[0].obj_meta_.set_uint64();
    storage_[0].datum_.set_uint(input);
    check_value(target_type, expected_bound);
  }

  void check_value(const ObObjType target_type, const int expected_bound)
  {
    SCOPED_TRACE(static_cast<int>(target_type));
    storage_[1].datum_.set_int(target_type);
    std::string actual;
    bool is_null = false;
    ASSERT_EQ(OB_SUCCESS, evaluate(actual, is_null));
    ASSERT_FALSE(is_null);
    ObObjMeta target_meta;
    target_meta.set_type(target_type);
    ObDatum column_value;
    uint64_t column_storage = 0;
    column_value.ptr_ = reinterpret_cast<char *>(&column_storage);
    // Compare against the real data-row encoder, including the extremes of
    // the column domain. Bounds must never equal any legal endpoint.
    if (ob_is_int_tc(target_type)) {
      column_value.set_int(expected_bound < 0 ? INT_MIN_VAL[target_type]
          : expected_bound > 0 ? INT_MAX_VAL[target_type]
          : ob_is_int_tc(exprs_[0].obj_meta_.get_type()) ? storage_[0].datum_.get_int()
          : static_cast<int64_t>(storage_[0].datum_.get_uint64()));
    } else {
      column_value.set_uint(expected_bound < 0 ? 0
          : expected_bound > 0 ? UINT_MAX_VAL[target_type]
          : ob_is_int_tc(exprs_[0].obj_meta_.get_type())
              ? static_cast<uint64_t>(storage_[0].datum_.get_int())
              : storage_[0].datum_.get_uint64());
    }
    ObString encoded;
    ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(
        allocator_, column_value, target_meta, encoded));
    const int cmp = ObString(actual.size(), actual.data()).compare(encoded);
    if (expected_bound < 0) {
      EXPECT_LT(cmp, 0);
    } else if (expected_bound > 0) {
      EXPECT_GT(cmp, 0);
    } else {
      EXPECT_EQ(0, cmp);
    }
  }

  ObArenaAllocator allocator_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  Frame storage_[3];
  char *frames_[3];
  ObExpr exprs_[3];
  ObExpr *args_[2];
};

TEST_F(TestSearchIndexInnerValue, signed_column_domains)
{
  const ObObjType targets[] = {ObTinyIntType, ObSmallIntType, ObMediumIntType, ObInt32Type, ObIntType};
  for (const auto target : targets) {
    check_signed(target, INT_MIN_VAL[target], 0);
    check_signed(target, INT_MAX_VAL[target], 0);
    check_signed(target, -3, 0);
    check_unsigned(target, 3, 0);
    check_unsigned(target, INT_MAX_VAL[target], 0);
    check_unsigned(target, static_cast<uint64_t>(INT_MAX_VAL[target]) + 1, 1);
    check_unsigned(target, UINT64_MAX, 1);
    if (target != ObIntType) {
      check_signed(target, INT_MIN_VAL[target] - 1, -1);
      check_signed(target, INT_MAX_VAL[target] + 1, 1);
    }
  }
}

TEST_F(TestSearchIndexInnerValue, unsigned_column_domains)
{
  const ObObjType targets[] = {ObUTinyIntType, ObUSmallIntType, ObUMediumIntType, ObUInt32Type, ObUInt64Type};
  for (const auto target : targets) {
    check_signed(target, -1, -1);
    check_signed(target, INT64_MIN, -1);
    check_signed(target, 0, 0);
    check_signed(target, 3, 0);
    check_unsigned(target, UINT_MAX_VAL[target], 0);
    if (target != ObUInt64Type) {
      check_unsigned(target, UINT_MAX_VAL[target] + 1, 1);
      check_signed(target, static_cast<int64_t>(UINT_MAX_VAL[target]) + 1, 1);
    }
  }
}

TEST_F(TestSearchIndexInnerValue, reused_expression_crosses_domain_in_both_directions)
{
  const int64_t inputs[] = {3, -3, 0, -3, 3};
  for (const auto input : inputs) {
    check_signed(ObUInt64Type, input, input < 0 ? -1 : 0);
  }
  check_signed(ObTinyIntType, 128, 1);
  check_signed(ObTinyIntType, 127, 0);
  check_signed(ObTinyIntType, -129, -1);
  check_signed(ObTinyIntType, -128, 0);
  check_signed(ObTinyIntType, 128, 1);
}

TEST_F(TestSearchIndexInnerValue, null_and_legacy_encoding)
{
  storage_[1].datum_.set_int(ObUInt64Type);
  storage_[0].datum_.set_null();
  std::string actual;
  bool is_null = false;
  ASSERT_EQ(OB_SUCCESS, evaluate(actual, is_null));
  EXPECT_TRUE(is_null);
  storage_[0].datum_.set_int(-3);
  storage_[1].datum_.set_null();
  ASSERT_EQ(OB_SUCCESS, evaluate(actual, is_null));
  EXPECT_FALSE(is_null);
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, ObSearchIndexValueEncoder::encode_value(
      allocator_, storage_[0].datum_, exprs_[0].obj_meta_, encoded));
  EXPECT_EQ(0, ObString(actual.size(), actual.data()).compare(encoded));
}

TEST_F(TestSearchIndexInnerValue, range_extraction_only_relaxes_integer_pairs)
{
  ObRawExprResType column_type;
  column_type.set_uint64();
  ObObjMeta calc_type;
  calc_type.set_uint64();
  bool always_true = false;
  const ObItemType comparisons[] = {T_OP_EQ, T_OP_NSEQ, T_OP_LT, T_OP_LE, T_OP_GT, T_OP_GE};
  for (const auto cmp : comparisons) {
    EXPECT_TRUE(ObQueryRange::can_be_extract_range(
        cmp, column_type, calc_type, ObIntType, always_true, true));
  }
  EXPECT_FALSE(ObQueryRange::can_be_extract_range(
      T_OP_EQ, column_type, calc_type, ObDoubleType, always_true, true));
  column_type.set_type(ObCollectionSQLType);
  calc_type.set_type(ObCollectionSQLType);
  EXPECT_FALSE(ObQueryRange::can_be_extract_range(
      T_OP_EQ, column_type, calc_type, ObCollectionSQLType, always_true, true));
  // The physical encoder compatibility relation must remain strict. Only the
  // scalar range path knows how to convert or bound an integer parameter.
  EXPECT_FALSE(ObSearchIndexValueEncoder::is_search_index_encode_compatible(
      ObUInt64Type, ObIntType));
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
