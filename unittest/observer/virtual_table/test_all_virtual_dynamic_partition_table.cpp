/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "share/ob_get_compat_mode.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"

#define private public
#include "observer/virtual_table/ob_all_virtual_dynamic_partition_table.h"
#undef private

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace share::schema;

class TestableDynamicPartitionTable : public ObAllVirtualDynamicPartitionTable
{
public:
  void set_row_cells(ObObj *cells, const int64_t count)
  {
    cur_row_.cells_ = cells;
    cur_row_.count_ = count;
  }
};

class TestAllVirtualDynamicPartitionTable : public testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().init(NULL));
  }

  static void TearDownTestCase()
  {
    ObCompatModeGetter::instance().destroy();
  }

protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().reset_compat_getter_map());
    lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().set_tenant_compat_mode(TENANT_ID, mode));
    ASSERT_EQ(OB_SUCCESS, vtable_.init(&ObMultiVersionSchemaService::get_instance()));
    vtable_.set_allocator(&allocator_);
    ASSERT_EQ(OB_SUCCESS, vtable_.tenant_ids_.push_back(TENANT_ID));
    vtable_.tenant_idx_ = 0;
    ObSEArray<uint64_t, 4> column_ids;
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(ObAllVirtualDynamicPartitionTable::TABLE_NAME));
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(ObAllVirtualDynamicPartitionTable::MAX_HIGH_BOUND_VAL));
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(ObAllVirtualDynamicPartitionTable::ENABLE));
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(ObAllVirtualDynamicPartitionTable::TIME_UNIT));
    ASSERT_EQ(OB_SUCCESS, vtable_.set_output_column_ids(column_ids));
    vtable_.set_row_cells(cells_, ARRAYSIZEOF(cells_));
    table_schema_.set_tenant_id(TENANT_ID);
    table_schema_.set_table_type(USER_TABLE);
    table_schema_.set_part_level(PARTITION_LEVEL_ONE);
    table_schema_.set_part_num(1);
    ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("t_dynamic_string_time"));
    ASSERT_EQ(OB_SUCCESS, table_schema_.set_dynamic_partition_policy(ObString::make_string(
        "enable=FALSE,time_unit=DAY,precreate_time=3DAY,expire_time=-1,time_zone=DEFAULT,bigint_precision=NONE")));
  }

  int add_list_partition(const char *list_value = "20260101")
  {
    ObObj value;
    value.set_varchar(list_value);
    value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    return add_list_partition(value);
  }

  int add_list_partition(ObObj &value)
  {
    table_schema_.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_LIST_COLUMNS);
    ObNewRow list_row;
    list_row.cells_ = &value;
    list_row.count_ = 1;
    ObPartition partition;
    int ret = partition.add_list_row(list_row);
    if (OB_SUCCESS == ret) {
      ret = table_schema_.add_partition(partition);
    }
    return ret;
  }

  int add_range_partition(ObObj &high_bound)
  {
    table_schema_.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANGE_COLUMNS);
    ObRowkey rowkey(&high_bound, 1);
    ObPartition partition;
    int ret = partition.set_high_bound_val(rowkey);
    if (OB_SUCCESS == ret) {
      ret = table_schema_.add_partition(partition);
    }
    return ret;
  }

  void check_row(const char *expected_high_bound)
  {
    ObNewRow *row = NULL;
    ASSERT_EQ(OB_SUCCESS, vtable_.build_new_row_(1, table_schema_, row));
    ASSERT_NE(nullptr, row);
    ASSERT_EQ(ARRAYSIZEOF(cells_), row->count_);
    EXPECT_EQ(ObString::make_string("t_dynamic_string_time"), row->cells_[0].get_string());
    ASSERT_TRUE(row->cells_[1].is_varchar());
    EXPECT_EQ(ObString::make_string(expected_high_bound), row->cells_[1].get_string());
    EXPECT_EQ(ObCharset::get_default_collation(ObCharset::get_default_charset()),
              row->cells_[1].get_collation_type());
    EXPECT_EQ(ObString::make_string("FALSE"), row->cells_[2].get_string());
    EXPECT_EQ(ObString::make_string("DAY"), row->cells_[3].get_string());
  }

  static const uint64_t TENANT_ID = 1001;
  ObArenaAllocator allocator_;
  TestableDynamicPartitionTable vtable_;
  ObTableSchema table_schema_;
  ObObj cells_[4];
};

const uint64_t TestAllVirtualDynamicPartitionTable::TENANT_ID;

TEST_F(TestAllVirtualDynamicPartitionTable, list_columns_literal)
{
  ASSERT_EQ(OB_SUCCESS, add_list_partition());
  ASSERT_FALSE(table_schema_.get_part_array()[0]->get_high_bound_val().is_valid());
  check_row("'20260101'");
}

TEST_F(TestAllVirtualDynamicPartitionTable, list_columns_in_oracle_mode)
{
  lib::Worker::CompatMode mode = lib::Worker::CompatMode::ORACLE;
  ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().set_tenant_compat_mode(TENANT_ID, mode));
  ASSERT_EQ(OB_SUCCESS, add_list_partition());
  table_schema_.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_LIST);
  check_row("'20260101'");
}

TEST_F(TestAllVirtualDynamicPartitionTable, list_uses_last_partition_literal)
{
  ASSERT_EQ(OB_SUCCESS, add_list_partition("20260101"));
  ASSERT_EQ(OB_SUCCESS, add_list_partition("20260103"));
  table_schema_.set_part_num(2);
  check_row("'20260103'");
}

TEST_F(TestAllVirtualDynamicPartitionTable, list_default_partition_literal)
{
  ObObj value;
  value.set_max_value();
  ASSERT_EQ(OB_SUCCESS, add_list_partition(value));
  check_row("DEFAULT");
}

TEST_F(TestAllVirtualDynamicPartitionTable, range_high_bound)
{
  ObObj high_bound;
  high_bound.set_int(20260102);
  ASSERT_EQ(OB_SUCCESS, add_range_partition(high_bound));
  check_row("20260102");
}

TEST_F(TestAllVirtualDynamicPartitionTable, range_maxvalue_high_bound)
{
  ObObj high_bound;
  high_bound.set_max_value();
  ASSERT_EQ(OB_SUCCESS, add_range_partition(high_bound));
  check_row("MAXVALUE");
}

TEST_F(TestAllVirtualDynamicPartitionTable, range_invalid_high_bound_still_fails)
{
  table_schema_.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANGE_COLUMNS);
  ObPartition partition;
  ASSERT_EQ(OB_SUCCESS, table_schema_.add_partition(partition));
  ObNewRow *row = NULL;
  EXPECT_EQ(OB_INVALID_ARGUMENT, vtable_.build_new_row_(1, table_schema_, row));
}

TEST_F(TestAllVirtualDynamicPartitionTable, list_overwrites_previous_range_high_bound)
{
  ObObj high_bound;
  high_bound.set_int(20260102);
  ASSERT_EQ(OB_SUCCESS, add_range_partition(high_bound));
  check_row("20260102");
  table_schema_.reset_partition_array();
  ASSERT_EQ(OB_SUCCESS, add_list_partition());
  check_row("'20260101'");
}

} // namespace observer
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_all_virtual_dynamic_partition_table.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
