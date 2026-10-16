/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include <gtest/gtest.h>
#define private public
#include "sql/engine/table/ob_csv_table_row_iter.h"
#undef private
#include "lib/oblog/ob_log.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

void set_sentinel(ObDatum &datum, char *sentinel)
{
  datum.set_string(sentinel, 1);
  datum.set_lob();
}

void expect_sentinel(const ObDatum &datum, const char *sentinel)
{
  EXPECT_EQ(sentinel, datum.ptr_);
  EXPECT_EQ(1U, datum.len_);
  EXPECT_EQ(ObDatumDesc::LOB, datum.flag_);
  EXPECT_FALSE(datum.is_null());
}

} // namespace

TEST(TestCSVTableRowIterator, set_datum_field_value)
{
  char sentinel = 'S';
  char input = 'I';
  ObDatum datum;
  ObCSVGeneralParser::FieldValue field;
  field.ptr_ = &input;

  set_sentinel(datum, &sentinel);
  field.len_ = OB_MAX_LONGTEXT_LENGTH + 1LL;
  EXPECT_EQ(OB_SIZE_OVERFLOW,
            ObCSVTableRowIterator::set_field_value(datum, field, false));
  expect_sentinel(datum, &sentinel);

  field.len_ = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            ObCSVTableRowIterator::set_field_value(datum, field, false));
  expect_sentinel(datum, &sentinel);

  field.len_ = OB_MAX_LONGTEXT_LENGTH - 1LL;
  EXPECT_EQ(OB_SUCCESS,
            ObCSVTableRowIterator::set_field_value(datum, field, false));
  EXPECT_EQ(&input, datum.ptr_);
  EXPECT_EQ(static_cast<uint32_t>(OB_MAX_LONGTEXT_LENGTH - 1LL), datum.len_);
  EXPECT_EQ(ObDatumDesc::NONE, datum.flag_);
  EXPECT_FALSE(datum.is_null());

  field.len_ = OB_MAX_LONGTEXT_LENGTH;
  EXPECT_EQ(OB_SUCCESS,
            ObCSVTableRowIterator::set_field_value(datum, field, false));
  EXPECT_EQ(&input, datum.ptr_);
  EXPECT_EQ(static_cast<uint32_t>(OB_MAX_LONGTEXT_LENGTH), datum.len_);
  EXPECT_EQ(ObDatumDesc::NONE, datum.flag_);
  EXPECT_FALSE(datum.is_null());

  field.is_null_ = true;
  field.len_ = OB_MAX_LONGTEXT_LENGTH + 1LL;
  EXPECT_EQ(OB_SUCCESS,
            ObCSVTableRowIterator::set_field_value(datum, field, false));
  EXPECT_TRUE(datum.is_null());
}

TEST(TestCSVTableRowIterator, set_discrete_field_value)
{
  int32_t lens[2] = {1, 1};
  char sentinel0 = '0';
  char sentinel1 = '1';
  char input = 'I';
  char *ptrs[2] = {&sentinel0, &sentinel1};
  uint64_t null_buf[1] = {0};
  ObBitVector *nulls = reinterpret_cast<ObBitVector *>(null_buf);
  StrDiscVec vector(lens, ptrs, nulls);
  ObCSVGeneralParser::FieldValue field;
  field.ptr_ = &input;

  field.len_ = OB_MAX_LONGTEXT_LENGTH + 1LL;
  EXPECT_EQ(OB_SIZE_OVERFLOW,
            ObCSVTableRowIterator::set_field_value(vector, 0, field, false));
  EXPECT_EQ(&sentinel0, ptrs[0]);
  EXPECT_EQ(1, lens[0]);
  EXPECT_FALSE(nulls->at(0));

  nulls->set(1);
  field.len_ = 1;
  EXPECT_EQ(OB_SUCCESS,
            ObCSVTableRowIterator::set_field_value(vector, 1, field, false));
  EXPECT_EQ(&input, ptrs[1]);
  EXPECT_EQ(1, lens[1]);
  EXPECT_FALSE(nulls->at(1));

  field.is_null_ = true;
  field.len_ = OB_MAX_LONGTEXT_LENGTH + 1LL;
  EXPECT_EQ(OB_SUCCESS,
            ObCSVTableRowIterator::set_field_value(vector, 0, field, false));
  EXPECT_TRUE(nulls->at(0));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
