/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#define protected public
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/compaction/ob_partition_parallel_merge_ctx.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/test_tablet_helper.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;
using namespace compaction;
namespace storage
{

class TestPartitionParallelMergeCtx : public ObMultiVersionSSTableTest
{
public:
  TestPartitionParallelMergeCtx() : ObMultiVersionSSTableTest("test_fork_parallel_minor") {}

  static void SetUpTestCase();

  void prepare_minor_sstable(ObTableHandleV2 &handle)
  {
    const char *macro_data[6];
    macro_data[0] =
        "bigint var bigint bigint flag multi_version_row_flag\n"
        "0 var1 -9 0 EXIST CLF\n"
        "1 var1 -9 0 EXIST CLF\n";
    macro_data[1] =
        "bigint var bigint bigint flag multi_version_row_flag\n"
        "2 var1 -9 0 EXIST CLF\n"
        "3 var1 -9 0 EXIST CLF\n";
    macro_data[2] =
        "bigint var bigint bigint flag multi_version_row_flag\n"
        "4 var1 -9 0 EXIST CLF\n"
        "5 var1 -9 0 EXIST CLF\n";
    macro_data[3] =
        "bigint var bigint bigint flag multi_version_row_flag\n"
        "6 var1 -9 0 EXIST CLF\n"
        "7 var1 -9 0 EXIST CLF\n";
    macro_data[4] =
        "bigint var bigint bigint flag multi_version_row_flag\n"
        "8 var1 -9 0 EXIST CLF\n"
        "9 var1 -9 0 EXIST CLF\n";
    macro_data[5] =
        "bigint var bigint bigint flag multi_version_row_flag\n"
        "10 var1 -9 0 EXIST CLF\n"
        "11 var1 -9 0 EXIST CLF\n";
    ObScnRange scn_range;
    scn_range.start_scn_.set_min();
    ASSERT_EQ(OB_SUCCESS, scn_range.end_scn_.convert_for_tx(10));
    prepare_table_schema(macro_data, 2, scn_range, 10);
    reset_writer(10);
    for (int64_t i = 0; i < 6; ++i) {
      prepare_one_macro(&macro_data[i], 1);
    }
    prepare_data_end(handle, ObITable::MINOR_SSTABLE);
  }
};

void TestPartitionParallelMergeCtx::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();

  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::STORAGE_MOD));

  obrpc::ObBatchCreateTabletArg create_tablet_arg;
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(tenant_id_, ObLSID(ls_id_), ObTabletID(tablet_id_), create_tablet_arg,
      1, &table_schema));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, ObTabletID(tablet_id_), table_schema, allocator_));
}

TEST_F(TestPartitionParallelMergeCtx, serial_and_init_twice)
{
  ObTableHandleV2 handle;
  prepare_minor_sstable(handle);
  ObArray<ObITable *> tables;
  ASSERT_EQ(OB_SUCCESS, tables.push_back(handle.get_table()));
  ObArenaAllocator allocator("ForkPlanTest");
  ObParallelMergeCtx ctx(allocator);
  ASSERT_EQ(OB_SUCCESS, ctx.init_parallel_fork_minor_rewrite(full_read_info_, tables, 2L << 20,
      ObDagPrio::DAG_PRIO_DDL, false));
  ASSERT_TRUE(ctx.is_valid());
  ASSERT_EQ(1, ctx.get_concurrent_cnt());
  ObDatumRange serial_range;
  ASSERT_EQ(OB_SUCCESS, ctx.get_merge_range(0, serial_range));
  ASSERT_TRUE(serial_range.is_valid());
  ASSERT_TRUE(serial_range.is_whole_range());
  ASSERT_TRUE(serial_range.is_left_open());
  ASSERT_TRUE(serial_range.is_right_open());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.get_merge_range(1, serial_range));
  ASSERT_EQ(OB_INIT_TWICE, ctx.init_parallel_fork_minor_rewrite(full_read_info_, tables, 2L << 20,
      ObDagPrio::DAG_PRIO_DDL, true));
  ASSERT_EQ(1, ctx.get_concurrent_cnt());
  ASSERT_EQ(OB_SUCCESS, ctx.get_merge_range(0, serial_range));
  ASSERT_TRUE(serial_range.is_whole_range());
}

TEST_F(TestPartitionParallelMergeCtx, validate_public_arguments)
{
  ObTableHandleV2 handle;
  prepare_minor_sstable(handle);
  ObArray<ObITable *> tables;
  ObArenaAllocator allocator("ForkPlanArg");
  ObParallelMergeCtx ctx(allocator);
  MockObTableReadInfo invalid_read_info;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init_parallel_fork_minor_rewrite(
      full_read_info_, tables, 2L << 20, ObDagPrio::DAG_PRIO_DDL, true));
  ASSERT_EQ(OB_SUCCESS, tables.push_back(nullptr));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init_parallel_fork_minor_rewrite(
      full_read_info_, tables, 2L << 20, ObDagPrio::DAG_PRIO_DDL, true));
  tables.reuse();
  ASSERT_EQ(OB_SUCCESS, tables.push_back(handle.get_table()));
  handle.get_table()->set_table_type(ObITable::MAJOR_SSTABLE);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init_parallel_fork_minor_rewrite(
      full_read_info_, tables, 2L << 20, ObDagPrio::DAG_PRIO_DDL, true));
  ASSERT_FALSE(ctx.is_valid());
  handle.get_table()->set_table_type(ObITable::MINOR_SSTABLE);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init_parallel_fork_minor_rewrite(
      invalid_read_info, tables, 2L << 20, ObDagPrio::DAG_PRIO_DDL, true));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init_parallel_fork_minor_rewrite(full_read_info_, tables, 2L << 20, -1, true));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init_parallel_fork_minor_rewrite(full_read_info_, tables, 2L << 20,
      ObDagPrio::DAG_PRIO_MAX, true));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init_parallel_fork_minor_rewrite(full_read_info_, tables, 2L << 20,
      ObDagPrio::DAG_PRIO_MAX + 1, true));
  ASSERT_FALSE(ctx.is_valid());
  ASSERT_EQ(OB_SUCCESS, ctx.init_parallel_fork_minor_rewrite(full_read_info_, tables, -1,
      ObDagPrio::DAG_PRIO_DDL, true));
  ASSERT_EQ(1, ctx.get_concurrent_cnt());
}

TEST_F(TestPartitionParallelMergeCtx, dop_three)
{
  ObTableHandleV2 handle;
  prepare_minor_sstable(handle);
  ObArray<ObITable *> tables;
  ASSERT_EQ(OB_SUCCESS, tables.push_back(handle.get_table()));
  ObTenantTabletScheduler *tablet_scheduler = MTL(ObTenantTabletScheduler *);
  ASSERT_NE(nullptr, tablet_scheduler);
  const bool old_enable = tablet_scheduler->tenant_status_.enable_adaptive_merge_schedule_;
  tablet_scheduler->tenant_status_.enable_adaptive_merge_schedule_ = false;
  ObArenaAllocator allocator("ForkPlanDop");
  ObParallelMergeCtx ctx(allocator);
  const int ret = ctx.init_parallel_fork_minor_rewrite(full_read_info_, tables, 4L << 20,
      ObDagPrio::DAG_PRIO_DDL, true);
  const int64_t concurrent_cnt = ctx.get_concurrent_cnt();
  tablet_scheduler->tenant_status_.enable_adaptive_merge_schedule_ = old_enable;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, concurrent_cnt);
  ASSERT_TRUE(ctx.is_valid());
  ObArray<ObDatumRange> ranges;
  for (int64_t i = 0; i < concurrent_cnt; ++i) {
    ObDatumRange range;
    ASSERT_EQ(OB_SUCCESS, ctx.get_merge_range(i, range));
    ASSERT_TRUE(range.is_valid());
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
  }
  ASSERT_TRUE(ranges.at(0).get_start_key().is_min_rowkey());
  ASSERT_TRUE(ranges.at(0).is_left_open());
  ASSERT_TRUE(ranges.at(concurrent_cnt - 1).get_end_key().is_max_rowkey());
  ASSERT_TRUE(ranges.at(concurrent_cnt - 1).is_right_open());
  for (int64_t i = 1; i < concurrent_cnt; ++i) {
    ASSERT_TRUE(ranges.at(i - 1).is_right_closed());
    ASSERT_TRUE(ranges.at(i).is_left_open());
    ASSERT_TRUE(ranges.at(i - 1).get_end_key() == ranges.at(i).get_start_key());
  }
  ObDatumRange invalid_range;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.get_merge_range(concurrent_cnt, invalid_range));
  ASSERT_EQ(OB_INIT_TWICE, ctx.init_parallel_fork_minor_rewrite(full_read_info_, tables, 4L << 20,
      ObDagPrio::DAG_PRIO_DDL, true));
  ASSERT_EQ(concurrent_cnt, ctx.get_concurrent_cnt());
  for (int64_t i = 0; i < concurrent_cnt; ++i) {
    ObDatumRange preserved_range;
    ASSERT_EQ(OB_SUCCESS, ctx.get_merge_range(i, preserved_range));
    ASSERT_TRUE(ranges.at(i).get_start_key() == preserved_range.get_start_key());
    ASSERT_TRUE(ranges.at(i).get_end_key() == preserved_range.get_end_key());
    ASSERT_EQ(ranges.at(i).is_left_open(), preserved_range.is_left_open());
    ASSERT_EQ(ranges.at(i).is_right_open(), preserved_range.is_right_open());
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
