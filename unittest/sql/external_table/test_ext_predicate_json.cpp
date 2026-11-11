/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/file_prune/ob_ext_predicate_json.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace sql
{
namespace unittest
{

using namespace common;

class TestExtPredicateJson : public ::testing::Test
{
protected:
  TestExtPredicateJson() : allocator_("ExtPredJsonUT"), factory_(allocator_) {}

  ObColumnRefRawExpr *column(const char *name, const uint64_t id)
  {
    ObColumnRefRawExpr *expr = nullptr;
    EXPECT_EQ(OB_SUCCESS, factory_.create_raw_expr(T_REF_COLUMN, expr));
    if (expr != nullptr) {
      expr->set_column_id(id);
      expr->set_column_name(ObString::make_string(name));
      expr->set_data_type(ObIntType);
    }
    return expr;
  }

  ObOpRawExpr *op(const ObItemType type, ObRawExpr *left, ObRawExpr *right = nullptr)
  {
    ObOpRawExpr *expr = nullptr;
    EXPECT_EQ(OB_SUCCESS, factory_.create_raw_expr(type, expr));
    if (expr != nullptr) {
      EXPECT_EQ(OB_SUCCESS, expr->init_param_exprs(right != nullptr ? 2 : 1));
      EXPECT_EQ(OB_SUCCESS, expr->add_param_expr(left));
      if (right != nullptr) {
        EXPECT_EQ(OB_SUCCESS, expr->add_param_expr(right));
      }
      expr->set_data_type(type == T_OP_ABS ? ObIntType : ObTinyIntType);
    }
    return expr;
  }

  std::string emit(ObRawExpr *root, const ObIArray<uint64_t> &partition_ids,
                   std::string &partition_json)
  {
    ObSEArray<ObRawExpr *, 1> filters;
    ObSEArray<ObRawExpr *, 1> partition_exprs;
    ObString predicate;
    ObString partition;
    EXPECT_EQ(OB_SUCCESS, filters.push_back(root));
    EXPECT_EQ(OB_SUCCESS, ext_predicate::build_predicate_json_from_raw_expr(
        allocator_, nullptr, filters, partition_ids, predicate, partition,
        partition_exprs));
    partition_json.assign(partition.ptr() == nullptr ? "" : partition.ptr(),
                          partition.length());
    return std::string(predicate.ptr() == nullptr ? "" : predicate.ptr(),
                       predicate.length());
  }

  ObArenaAllocator allocator_;
  ObRawExprFactory factory_;
};

TEST_F(TestExtPredicateJson, AndPreservesUnknownChildren)
{
  auto *a = column("a", OB_APP_MIN_COLUMN_ID);
  auto *b = column("b", OB_APP_MIN_COLUMN_ID + 1);
  auto *c = column("c", OB_APP_MIN_COLUMN_ID + 2);
  ASSERT_NE(nullptr, a);
  ASSERT_NE(nullptr, b);
  ASSERT_NE(nullptr, c);
  auto *root = op(T_OP_AND, op(T_OP_EQ, a, b), op(T_OP_EQ, op(T_OP_ABS, c), a));
  ObSEArray<uint64_t, 1> partition_ids;
  std::string partition_json;
  const std::string json = emit(root, partition_ids, partition_json);
  EXPECT_NE(std::string::npos, json.find("\"kind\":\"and\""));
  EXPECT_NE(std::string::npos, json.find("\"name\":\"a\""));
  EXPECT_NE(std::string::npos, json.find("\"name\":\"b\""));
  EXPECT_NE(std::string::npos, json.find("\"name\":\"c\""));
  EXPECT_NE(std::string::npos, json.find("T_OP_ABS"));
  EXPECT_TRUE(partition_json.empty());
}

TEST_F(TestExtPredicateJson, NestedNotAndOrKeepOriginalShape)
{
  auto *a = column("a", OB_APP_MIN_COLUMN_ID);
  auto *b = column("b", OB_APP_MIN_COLUMN_ID + 1);
  ASSERT_NE(nullptr, a);
  ASSERT_NE(nullptr, b);
  auto *root = op(T_OP_NOT,
                  op(T_OP_OR, op(T_OP_EQ, a, b),
                     op(T_OP_NOT, op(T_OP_EQ, op(T_OP_ABS, a), b))));
  ObSEArray<uint64_t, 1> partition_ids;
  std::string partition_json;
  const std::string json = emit(root, partition_ids, partition_json);
  EXPECT_EQ(0, json.find("{\"kind\":\"not\""));
  EXPECT_NE(std::string::npos, json.find("\"kind\":\"or\""));
  EXPECT_NE(std::string::npos, json.find("T_OP_ABS"));
  const size_t nested_not = json.find("\"kind\":\"not\"", 1);
  EXPECT_NE(std::string::npos, nested_not);
}

TEST_F(TestExtPredicateJson, DynamicParameterCannotProvePartitionFilter)
{
  auto *a = column("a", OB_APP_MIN_COLUMN_ID);
  auto *b = column("b", OB_APP_MIN_COLUMN_ID + 1);
  ASSERT_NE(nullptr, a);
  ASSERT_NE(nullptr, b);
  auto *root = op(T_OP_EQ, a, b);
  ASSERT_EQ(OB_SUCCESS, root->add_flag(CNT_DYNAMIC_PARAM));
  ObSEArray<uint64_t, 2> partition_ids;
  ASSERT_EQ(OB_SUCCESS, partition_ids.push_back(OB_APP_MIN_COLUMN_ID));
  ASSERT_EQ(OB_SUCCESS, partition_ids.push_back(OB_APP_MIN_COLUMN_ID + 1));
  std::string partition_json;
  EXPECT_FALSE(emit(root, partition_ids, partition_json).empty());
  EXPECT_TRUE(partition_json.empty());
}

} // namespace unittest
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
