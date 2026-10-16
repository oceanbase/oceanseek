/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "share/schema/ob_schema_service_sql_impl.h"
#undef private

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace schema;

namespace
{

const uint64_t TEST_TENANT_ID = 1001;
const uint64_t TEST_TABLE_ID = OB_MIN_USER_OBJECT_ID + 1;

int add_subpartitions(ObPartition &partition, const int64_t subpartition_num)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num; ++i) {
    ObSubPartition subpartition;
    subpartition.set_tenant_id(TEST_TENANT_ID);
    subpartition.set_table_id(TEST_TABLE_ID);
    subpartition.set_part_id(0);
    subpartition.set_part_idx(0);
    subpartition.set_sub_part_id(i);
    subpartition.set_sub_part_idx(i);
    if (OB_FAIL(partition.add_partition(subpartition))) {
      LOG_WARN("failed to add subpartition", K(ret), K(i));
    }
  }
  return ret;
}

int add_def_subpartitions(ObSimpleTableSchemaV2 &schema, const int64_t subpartition_num)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < subpartition_num; ++i) {
    ObSubPartition subpartition;
    subpartition.set_tenant_id(TEST_TENANT_ID);
    subpartition.set_table_id(TEST_TABLE_ID);
    subpartition.set_part_id(ObSubPartition::TEMPLATE_PART_ID);
    subpartition.set_part_idx(0);
    subpartition.set_sub_part_id(i);
    subpartition.set_sub_part_idx(i);
    if (OB_FAIL(schema.add_def_subpartition(subpartition))) {
      LOG_WARN("failed to add default subpartition", K(ret), K(i));
    }
  }
  return ret;
}

int add_partitions(ObSimpleTableSchemaV2 &schema, const int64_t partition_num)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
    ObPartition partition;
    partition.set_tenant_id(TEST_TENANT_ID);
    partition.set_table_id(TEST_TABLE_ID);
    partition.set_part_id(i);
    partition.set_part_idx(i);
    if (OB_FAIL(schema.add_partition(partition))) {
      LOG_WARN("failed to add partition", K(ret), K(i));
    }
  }
  return ret;
}

int init_partition_schema(
    ObSimpleTableSchemaV2 &schema,
    const int64_t expected_partition_num,
    const int64_t actual_partition_num)
{
  int ret = OB_SUCCESS;
  schema.set_tenant_id(TEST_TENANT_ID);
  schema.set_table_id(TEST_TABLE_ID);
  schema.set_schema_version(1);
  schema.set_database_id(1);
  schema.set_table_type(USER_TABLE);
  schema.set_part_level(PARTITION_LEVEL_ONE);
  schema.set_part_num(expected_partition_num);
  schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  if (OB_FAIL(schema.set_table_name(ObString::make_string("test_partition_history_sort")))) {
    LOG_WARN("failed to set table name", K(ret));
  } else if (OB_FAIL(add_partitions(schema, actual_partition_num))) {
    LOG_WARN("failed to add partitions", K(ret));
  }
  return ret;
}

int init_schema(
    ObSimpleTableSchemaV2 &schema,
    const int64_t expected_subpartition_num,
    const int64_t actual_subpartition_num,
    const bool is_template)
{
  int ret = OB_SUCCESS;
  schema.set_tenant_id(TEST_TENANT_ID);
  schema.set_table_id(TEST_TABLE_ID);
  schema.set_schema_version(1);
  schema.set_database_id(1);
  schema.set_table_type(USER_TABLE);
  schema.set_part_level(PARTITION_LEVEL_TWO);
  schema.set_part_num(1);
  schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  schema.get_sub_part_option().set_part_func_type(PARTITION_FUNC_TYPE_HASH);
  if (is_template) {
    schema.set_sub_part_template_def_valid();
    schema.set_def_sub_part_num(expected_subpartition_num);
  }

  if (OB_FAIL(schema.set_table_name(ObString::make_string("test_schema_history_sort")))) {
    LOG_WARN("failed to set table name", K(ret));
  } else {
    ObPartition partition;
    const int64_t partition_subpart_num = is_template
        ? expected_subpartition_num
        : actual_subpartition_num;
    partition.set_tenant_id(TEST_TENANT_ID);
    partition.set_table_id(TEST_TABLE_ID);
    partition.set_part_id(0);
    partition.set_part_idx(0);
    partition.set_sub_part_num(expected_subpartition_num);
    if (OB_FAIL(add_subpartitions(partition, partition_subpart_num))) {
      LOG_WARN("failed to add subpartitions", K(ret));
    } else if (OB_FAIL(schema.add_partition(partition))) {
      LOG_WARN("failed to add partition", K(ret));
    } else if (is_template && OB_FAIL(add_def_subpartitions(schema, actual_subpartition_num))) {
      LOG_WARN("failed to add default subpartitions", K(ret));
    }
  }
  return ret;
}

void expect_sort_result(
    const int64_t expected_subpartition_num,
    const int64_t actual_subpartition_num,
    const bool is_template,
    const int expected_ret)
{
  ObSimpleTableSchemaV2 schema;
  ASSERT_EQ(OB_SUCCESS,
      init_schema(schema, expected_subpartition_num, actual_subpartition_num, is_template));
  ASSERT_TRUE(schema.is_valid());
  ASSERT_TRUE(schema.is_user_partition_table());
  EXPECT_EQ(expected_ret, ObSchemaServiceSQLImpl::sort_subpartition_array(schema));
}

void expect_partition_sort_result(
    const int64_t expected_partition_num,
    const int64_t actual_partition_num,
    const int expected_ret)
{
  ObSimpleTableSchemaV2 schema;
  ASSERT_EQ(OB_SUCCESS,
      init_partition_schema(schema, expected_partition_num, actual_partition_num));
  ASSERT_TRUE(schema.is_valid());
  ASSERT_TRUE(schema.is_user_partition_table());
  EXPECT_EQ(expected_ret, ObSchemaServiceSQLImpl::sort_partition_array(schema));
}

TEST(TestSchemaHistorySort, partition_history_is_incomplete)
{
  expect_partition_sort_result(2, 1, OB_ERR_SCHEMA_HISTORY_EMPTY);
}

TEST(TestSchemaHistorySort, partition_history_has_extra_rows)
{
  expect_partition_sort_result(2, 3, OB_ERR_UNEXPECTED);
}

TEST(TestSchemaHistorySort, complete_partition_history_sorts_successfully)
{
  expect_partition_sort_result(2, 2, OB_SUCCESS);
}

TEST(TestSchemaHistorySort, non_template_subpartition_history_is_incomplete)
{
  expect_sort_result(2, 1, false, OB_ERR_SCHEMA_HISTORY_EMPTY);
}

TEST(TestSchemaHistorySort, non_template_subpartition_history_has_extra_rows)
{
  expect_sort_result(1, 2, false, OB_ERR_UNEXPECTED);
}

TEST(TestSchemaHistorySort, template_subpartition_history_is_incomplete)
{
  expect_sort_result(2, 1, true, OB_ERR_SCHEMA_HISTORY_EMPTY);
}

TEST(TestSchemaHistorySort, template_subpartition_history_has_extra_rows)
{
  expect_sort_result(1, 2, true, OB_ERR_UNEXPECTED);
}

TEST(TestSchemaHistorySort, complete_subpartition_history_sorts_successfully)
{
  expect_sort_result(2, 2, false, OB_SUCCESS);
  expect_sort_result(2, 2, true, OB_SUCCESS);
}

} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
