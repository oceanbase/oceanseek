/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "gtest/gtest.h"
#include "sql/engine/px/exchange/ob_row_heap.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class TestRowCompare
{
public:
  TestRowCompare() : ret_(OB_SUCCESS), rows_(NULL) {}

  int init(const ObIArray<ObSortFieldCollation> *,
           const ObIArray<ObSortCmpFunc> *,
           const ObIArray<const int *> &rows)
  {
    rows_ = &rows;
    return OB_SUCCESS;
  }

  bool operator()(const int64_t left_idx, const int64_t right_idx)
  {
    bool less = false;
    if (OB_SUCCESS != ret_) {
      // already failed
    } else if (fail_compare_) {
      ret_ = OB_ERR_UNEXPECTED;
    } else {
      less = *rows_->at(left_idx) < *rows_->at(right_idx);
    }
    return less;
  }

  int get_ret() const { return ret_; }
  static void set_fail_compare(const bool fail_compare) { fail_compare_ = fail_compare; }

private:
  int ret_;
  const ObIArray<const int *> *rows_;
  static bool fail_compare_;
};

bool TestRowCompare::fail_compare_ = false;

TEST(TestRowHeapErrorPropagation, push_heap)
{
  ObRowHeap<TestRowCompare, int> heap;
  int first = 1;
  int second = 2;
  ASSERT_EQ(OB_SUCCESS, heap.init(2, NULL, NULL));
  ASSERT_EQ(OB_SUCCESS, heap.push(&first));
  TestRowCompare::set_fail_compare(true);
  ASSERT_EQ(OB_ERR_UNEXPECTED, heap.push(&second));
  TestRowCompare::set_fail_compare(false);
}

TEST(TestRowHeapErrorPropagation, pop_heap)
{
  ObRowHeap<TestRowCompare, int> heap;
  int first = 1;
  int second = 2;
  int third = 3;
  const int *row = NULL;
  ASSERT_EQ(OB_SUCCESS, heap.init(3, NULL, NULL));
  ASSERT_EQ(OB_SUCCESS, heap.push(&first));
  ASSERT_EQ(OB_SUCCESS, heap.push(&second));
  ASSERT_EQ(OB_SUCCESS, heap.push(&third));
  TestRowCompare::set_fail_compare(true);
  ASSERT_EQ(OB_ERR_UNEXPECTED, heap.pop(row));
  ASSERT_EQ(NULL, row);
  TestRowCompare::set_fail_compare(false);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
