/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#define protected public

#include "storage/compaction/ob_partition_merger.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "storage/test_tablet_helper.h"
#include "storage/ob_partition_range_spliter.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace memtable;
using namespace observer;
using namespace unittest;
using namespace rpc::frame;
using namespace memtable;
namespace storage
{

static ObArenaAllocator global_allocator_;

class TestRangeSpliter: public ObMultiVersionSSTableTest
{
public:
  static const int64_t MAX_MACRO_CNT = 20;

  TestRangeSpliter() : ObMultiVersionSSTableTest("test_rangespliter") {}

  virtual ~TestRangeSpliter() = default;

  void SetUp() { ObMultiVersionSSTableTest::SetUp(); }

  void TearDown()
  {
    ObMultiVersionSSTableTest::TearDown();
    TRANS_LOG(INFO, "teardown success");
  }

  static void SetUpTestCase();
  static void TearDownTestCase();

  void build_query_range(ObIArray<ObStoreRange> &ranges, const char *rowkey_data);

  void transform_to_datum_range(ObIArray<ObPairStoreAndDatumRange> &datum_ranges, const ObIArray<ObStoreRange> &ranges);

  bool equal_ranges(ObStoreRange &range1, ObStoreRange &range2);

  bool loop_equal_ranges(ObIArray<ObStoreRange> &ranges1, ObIArray<ObStoreRange> &ranges2);

  void prepare_sstable_handle(ObTableHandleV2 &handle,
                              const int64_t macro_idx_start,
                              const int64_t macro_cnt,
                              const int64_t snapshot_version,
                              const int64_t micro_cnt = 1);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObMockIterator range_iter_;
};

void TestRangeSpliter::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  // create tablet
  obrpc::ObBatchCreateTabletArg create_tablet_arg;
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(tenant_id_, ls_id, tablet_id, create_tablet_arg, 1, &table_schema));

  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, ObMultiVersionSSTableTest::allocator_));
}

void TestRangeSpliter::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

bool TestRangeSpliter::equal_ranges(ObStoreRange &range1, ObStoreRange &range2)
{
  bool bret = false;
  if (!range1.get_start_key().simple_equal(range2.get_start_key())) {
  } else if (!range1.get_end_key().simple_equal(range2.get_end_key())) {
  } else {
    bret = range1.get_border_flag().get_data() == range2.get_border_flag().get_data();
  }
  if (!bret) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges are not equal", K(range1), K(range2));
  }

  return bret;
}

bool TestRangeSpliter::loop_equal_ranges(ObIArray<ObStoreRange> &ranges1, ObIArray<ObStoreRange> &ranges2)
{
  bool bret = false;
  if (ranges1.count() != ranges2.count()) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ranges are not equal", K(ranges1), K(ranges2));
  } else {
    bret = true;
    for (int64_t i = 0; bret && i < ranges1.count(); i++) {
      bret = equal_ranges(ranges1.at(i), ranges2.at(i));
    }
  }

  return bret;
}

void TestRangeSpliter::transform_to_datum_range(ObIArray<ObPairStoreAndDatumRange> &datum_ranges,
                                                const ObIArray<ObStoreRange> &ranges)
{
  for (int i = 0; i < ranges.count(); i++) {
    ObPairStoreAndDatumRange range;
    range.origin_store_range_ = &ranges.at(i);
    ASSERT_EQ(OB_SUCCESS, range.datum_range_.from_range(ranges.at(i), allocator_));
    ASSERT_EQ(OB_SUCCESS, datum_ranges.push_back(range));
  }
}

void TestRangeSpliter::build_query_range(ObIArray<ObStoreRange> &ranges, const char *rowkey_data)
{
  const ObStoreRow *row = nullptr;
  const int64_t rowkey_col_cnt = TEST_ROWKEY_COLUMN_CNT;
  ObStoreRange range, dst_range;
  ASSERT_TRUE(nullptr != rowkey_data);
  ranges.reset();
  range_iter_.reset();
  OK(range_iter_.from(rowkey_data));
  ASSERT_TRUE(range_iter_.count() > 0 && range_iter_.count() % 2 == 0);

  for (int64_t i = 0; i < range_iter_.count(); i+=2) {
    OK(range_iter_.get_row(i, row));
    ASSERT_TRUE(nullptr != row);
    range.get_start_key().assign(row->row_val_.cells_, rowkey_col_cnt);
    OK(range_iter_.get_row(i + 1, row));
    ASSERT_TRUE(nullptr != row);
    range.get_end_key().assign(row->row_val_.cells_, rowkey_col_cnt);
    if (range.get_start_key().compare(range.get_end_key()) == 0) {
      range.set_left_closed();
    } else {
      range.set_left_open();
    }
    range.set_right_closed();
    ASSERT_EQ(OB_SUCCESS, range.deep_copy(allocator_, dst_range));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(dst_range));
    range.reset();
    dst_range.reset();
  }
}

void TestRangeSpliter::prepare_sstable_handle(ObTableHandleV2 &handle,
                                              const int64_t macro_idx_start,
                                              const int64_t macro_cnt,
                                              const int64_t snapshot_version,
                                              const int64_t micro_cnt)
{
  const int64_t rowkey_cnt = TEST_ROWKEY_COLUMN_CNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ASSERT_EQ(true, macro_idx_start + macro_cnt <= MAX_MACRO_CNT);

  const char *macro_data[20];
  macro_data[0] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "0        var1    -9    0     EXIST   CLF\n"
      "1        var1    -9    MIN   EXIST   SCF\n"
      "1        var1    -9    0     EXIST   C\n";

  macro_data[1] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "1        var1    -8     0      EXIST   L\n"
      "2        var1    -9     0      EXIST   CLF\n";

  macro_data[2] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "3        var1    -8     0      EXIST   CLF\n"
      "4        var1   -9    MIN      EXIST   SCF\n"
      "4        var1    -9     0      EXIST   C\n";

  macro_data[3] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "4        var1    -8     0      EXIST   L\n"
      "5        var1    -9     0      EXIST   CLF\n";

  macro_data[4] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "6        var1    -9     0      EXIST   CLF\n"
      "7        var1   -9    MIN      EXIST   SCF\n"
      "7        var1    -9     0      EXIST   C\n";

  macro_data[5] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "7        var1    -8     0      EXIST   L\n"
      "8        var1    -9    0     EXIST   CLF\n";

  macro_data[6] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "9        var1    -8     0      EXIST   CLF\n"
      "10       var1  -9    MIN      EXIST   SCF\n"
      "10       var1    -9    0     EXIST    C\n";

  macro_data[7] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "10       var1    -8     0      EXIST   L\n"
      "11       var1    -9    0     EXIST   CLF\n";

  macro_data[8] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "12       var1     -9    0     EXIST   CLF\n"
      "13       var1   -9    MIN      EXIST   SCF\n"
      "13       var1     -9    0     EXIST   C\n";

  macro_data[9] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "13       var1   -8    MIN      EXIST   L\n"
      "14       var1   -9    MIN      EXIST   CLF\n";

  macro_data[10] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "15       var1     -8     0      EXIST   CLF\n"
      "16       var1   -9    MIN      EXIST   SCF\n"
      "16       var1     -9    0     EXIST     C\n";

  macro_data[11] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "16       var1     -8     0      EXIST   L\n"
      "17       var1     -9    0     EXIST   CLF\n";

  macro_data[12] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "18       var1     -9    0     EXIST   CLF\n"
      "19       var1   -9    MIN      EXIST   SCF\n"
      "19       var1     -9    0     EXIST   C\n";

  macro_data[13] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "19       var1     -8     0      EXIST   L\n"
      "20       var1     -9    0     EXIST   CLF\n";

  macro_data[14] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "21       var1     -8     0      EXIST   CLF\n"
      "22       var1   -9    MIN      EXIST   SCF\n"
      "22       var1     -9    0     EXIST   C\n";

  macro_data[15] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "22       var1    -8     0      EXIST   L\n"
      "23       var1    -9    0     EXIST   CLF\n";

  macro_data[16] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "24       var1    -9    0     EXIST   CLF\n"
      "25       var1    -9    MIN      EXIST  SCF\n"
      "25       var1    -9    0     EXIST   CL\n";

  macro_data[17] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "26       var1    -8     0      EXIST   L\n"
      "27       var1    -9    0     EXIST   CLF\n";

  macro_data[18] =
      "bigint   var   bigint bigint  flag    multi_version_row_flag\n"
      "28       var1    -8     0      EXIST   CLF\n"
      "29       var1   -9    MIN      EXIST   SCF\n"
      "29       var1    -9    0     EXIST   C\n";

  macro_data[19] =
      "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
      "29       var1    -8     0      EXIST   L\n"
      "30       var1    -9    0     EXIST   CLF\n";
  
  int schema_rowkey_cnt = 2;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  if (!table_schema_.is_valid()) {
    prepare_table_schema(macro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  }
  reset_writer(snapshot_version);
  for (int64_t i = macro_idx_start; i < macro_idx_start + macro_cnt * micro_cnt; i += micro_cnt) {
    prepare_one_macro(&macro_data[i], micro_cnt);
  }
  prepare_data_end(handle);
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_sstable(sstable));
  sstable->meta_->basic_meta_.occupy_size_ = (2<<20) * macro_cnt;
  sstable->key_.table_type_ = ObITable::MINOR_SSTABLE;
}

TEST_F(TestRangeSpliter, test_multi_range_estimate)
{
  allocator_.reuse();

  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 20, 10);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  {
    // single whole range
    ObArray<ObPairStoreAndDatumRange> ranges;
    ObPairStoreAndDatumRange range;
    range.datum_range_.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObMultiRangeRowEstimateContext context(false /* is_never_estimate */);
    ObIndexBlockTreeTraverser traverse;

    ASSERT_EQ(OB_SUCCESS, context.init(ranges, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.init(*sstable, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(context));
    ASSERT_EQ(50, context.ranges_.at(0).row_count_);
  }

  {
    ObArray<ObStoreRange> ranges;
    ObArray<ObPairStoreAndDatumRange> datum_ranges;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "7        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObMultiRangeRowEstimateContext context(false /* is_never_estimate */);
    ObIndexBlockTreeTraverser traverse;

    transform_to_datum_range(datum_ranges, ranges);
    ASSERT_EQ(OB_SUCCESS, context.init(datum_ranges, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.init(*sstable, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(context));
    ASSERT_LE(7, context.ranges_.at(0).row_count_);
    ASSERT_LE(context.ranges_.at(0).row_count_, 10);
    ASSERT_LE(12, context.ranges_.at(1).row_count_);
    ASSERT_LE(context.ranges_.at(1).row_count_, 15);
    ASSERT_EQ(2, context.ranges_.at(2).row_count_);
  }
}

TEST_F(TestRangeSpliter, test_multi_range_split)
{
  allocator_.reuse();

  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 20, 10);
  ObSSTable *sstable = static_cast<ObSSTable *>(handle.table_);

  {
    // single whole range
    ObArray<ObPairStoreAndDatumRange> ranges;
    ObPairStoreAndDatumRange range;
    ObArray<ObSplitRangeInfo> split_ranges;
    range.datum_range_.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));

    ObMultiRangeRowEstimateContext row_context(false /* is_never_estimate */);
    ObMultiRangeSplitContext context;
    ObIndexBlockTreeTraverser traverse;

    ASSERT_EQ(OB_SUCCESS, row_context.init(ranges, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.init(*sstable, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(row_context));
    traverse.reuse();
    ASSERT_EQ(OB_SUCCESS, context.init(ranges, *index_read_info, 10, allocator_, row_context.get_ranges(), split_ranges));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(context));
    ASSERT_EQ(6, split_ranges.count());
  }

  {
    ObArray<ObStoreRange> ranges;
    ObArray<ObPairStoreAndDatumRange> datum_ranges;
    ObArray<ObSplitRangeInfo> split_ranges;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "7        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObMultiRangeRowEstimateContext row_context(false /* is_never_estimate */);
    ObMultiRangeSplitContext context;
    ObIndexBlockTreeTraverser traverse;

    transform_to_datum_range(datum_ranges, ranges);
    ASSERT_EQ(OB_SUCCESS, row_context.init(datum_ranges, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.init(*sstable, *index_read_info));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(row_context));
    traverse.reuse();
    ASSERT_EQ(OB_SUCCESS, context.init(datum_ranges, *index_read_info, 4, allocator_, row_context.get_ranges(), split_ranges));
    ASSERT_EQ(OB_SUCCESS, traverse.traverse(context));
    ASSERT_EQ(6, split_ranges.count());
  }
}

TEST_F(TestRangeSpliter, test_partition_multi_range_split)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));

  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 2, *index_read_info, sstables, allocator_, multi_range_split_array));

    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));
  }
}

TEST_F(TestRangeSpliter, test_multi_sstable_split)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle, handle1, handle2;
  prepare_sstable_handle(handle, 0, 5, 10);
  prepare_sstable_handle(handle1, 5, 10, 10);
  prepare_sstable_handle(handle2, 15, 5, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle1.table_));
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle2.table_));

  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 2, *index_read_info, sstables, allocator_, multi_range_split_array));

    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "13       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "13       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));
  }

  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 3, *index_read_info, sstables, allocator_, multi_range_split_array));

    LOG_INFO("multi_range_split_array", K(multi_range_split_array));

    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "7        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "7        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "14       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));

    const char *rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "14       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data3);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(2)));
  }
}

TEST_F(TestRangeSpliter, test_micro_level_split)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 2, 10, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));

  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;

    ObStoreRange whole_range;
    whole_range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(whole_range));

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 4, *index_read_info, sstables, allocator_, multi_range_split_array));

    LOG_INFO("multi_range_split_array", K(multi_range_split_array));

    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "MIN      MIN    -9    MIN      EXIST   CLF\n"
                               "8       var1    -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ranges.at(0).get_start_key().set_min();
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "8        var1   -9    MIN      EXIST   CLF\n"
                               "13       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));

    const char *rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "13       var1   -9    MIN      EXIST   CLF\n"
                               "22       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data3);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(2)));

    const char *rowkey_data4 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "22       var1   -9    MIN      EXIST   CLF\n"
                               "MAX      MAX    -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data4);
    ranges.at(0).get_end_key().set_max();
    ranges.at(0).set_right_open();
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(3)));
  }
}

// 场景：分别构造普通与精确估算 context；覆盖精确路径禁止节点估算快捷方式的配置；
// 预期：普通 context 允许估算，而 precise context 强制继续下钻到数据微块。
TEST(TestMultiRangeInfoEstimateContext, precise_size_context_never_estimates_nodes)
{
  ObMultiRangeInfoEstimateContext legacy_context;
  ObMultiRangePreciseInfoEstimateContext precise_context;

  ASSERT_EQ(OB_SUCCESS, precise_context.init(INT64_MAX, 1024));
  ASSERT_FALSE(legacy_context.is_never_estimate());
  ASSERT_TRUE(precise_context.is_never_estimate());
}

// 场景：构造已经到期的 precise context 并进入索引树节点回调；覆盖单表精确估算 deadline；
// 预期：节点遍历立即返回 OB_TIMEOUT，交由上层切换到非精确估算，且不继续访问节点内容。
TEST(TestMultiRangeInfoEstimateContext, precise_size_context_honors_deadline)
{
  ObMultiRangePreciseInfoEstimateContext precise_context;
  ObMicroIndexInfo index_row;
  ObIIndexTreeTraverserContext::ObTraverserOperationType operation
      = ObIIndexTreeTraverserContext::NOTHING;

  ASSERT_EQ(OB_SUCCESS, precise_context.init(ObTimeUtility::current_time() - 1, 1024));
  ASSERT_EQ(OB_TIMEOUT, precise_context.on_inner_node(index_row, false, operation));
}

// 场景：两个数据微块位于同一宏块，第三个微块位于另一宏块；
// 预期：宏块数按 MacroBlockId 去重，依次为 1 和 2。
TEST(TestMultiRangeInfoEstimateContext, precise_size_context_deduplicates_macro_blocks)
{
  ObMultiRangePreciseInfoEstimateContext precise_context;
  ObIndexBlockRowHeader header1;
  ObIndexBlockRowHeader header2;
  ObIndexBlockRowHeader header3;
  ObMicroIndexInfo index_info1;
  ObMicroIndexInfo index_info2;
  ObMicroIndexInfo index_info3;
  ObPartitionEst est;
  const MacroBlockId macro_id1(0, 1, 0);
  const MacroBlockId macro_id2(0, 2, 0);

  ASSERT_EQ(OB_SUCCESS, precise_context.init(INT64_MAX, 2));
  ASSERT_EQ(OB_SUCCESS, header1.set_macro_id(macro_id1));
  ASSERT_EQ(OB_SUCCESS, header2.set_macro_id(macro_id1));
  ASSERT_EQ(OB_SUCCESS, header3.set_macro_id(macro_id2));
  header1.row_count_ = header2.row_count_ = header3.row_count_ = 1;
  header1.block_size_ = header2.block_size_ = header3.block_size_ = 100;
  header1.block_offset_ = 100;
  header2.block_offset_ = 200;
  header3.block_offset_ = 100;
  index_info1.row_header_ = &header1;
  index_info2.row_header_ = &header2;
  index_info3.row_header_ = &header3;
  index_info1.parent_macro_id_ = macro_id1;
  index_info2.parent_macro_id_ = macro_id1;
  index_info3.parent_macro_id_ = macro_id2;
  est.physical_row_count_ = 1;

  ASSERT_EQ(OB_SUCCESS, precise_context.on_leaf_node(index_info1, est));
  ASSERT_EQ(OB_SUCCESS, precise_context.on_leaf_node(index_info2, est));
  ASSERT_EQ(1, precise_context.get_total_macro_block_count());
  ASSERT_EQ(OB_SUCCESS, precise_context.on_leaf_node(index_info3, est));
  ASSERT_EQ(2, precise_context.get_total_macro_block_count());
}

// 场景：查询只命中数据微块中的部分物理行；覆盖边界微块按命中比例折算大小；
// 预期：大小按命中行数与微块总行数的比例计算，而不是整块计入。
TEST(TestMultiRangeInfoEstimateContext, precise_size_context_scales_boundary_micro_block)
{
  ObMultiRangePreciseInfoEstimateContext precise_context;
  ObIndexBlockRowHeader header;
  ObMicroIndexInfo index_info;
  ObPartitionEst est;
  const MacroBlockId macro_id(0, 1, 0);

  ASSERT_EQ(OB_SUCCESS, precise_context.init(INT64_MAX, 1));
  ASSERT_EQ(OB_SUCCESS, header.set_macro_id(macro_id));
  header.row_count_ = 10;
  header.block_size_ = 1000;
  index_info.row_header_ = &header;
  index_info.parent_macro_id_ = macro_id;
  est.physical_row_count_ = 3;

  ASSERT_EQ(OB_SUCCESS, precise_context.on_leaf_node(index_info, est));
  ASSERT_EQ(300, precise_context.get_total_size());
  ASSERT_EQ(3, precise_context.get_total_row_count());
  ASSERT_EQ(1, precise_context.get_total_macro_block_count());
}

// 场景：一万个不同宏块各命中多个微块，并按逆序再次重访；覆盖大规模宏块去重；
// 预期：宏块计数正确，整体处理耗时保持在线性量级。
TEST(TestMultiRangeInfoEstimateContext, precise_size_context_deduplicates_macro_blocks_linearly)
{
  constexpr int64_t MACRO_BLOCK_COUNT = 10000;
  ObMultiRangePreciseInfoEstimateContext precise_context;
  ObIndexBlockRowHeader header;
  ObMicroIndexInfo index_info;
  ObPartitionEst est;
  header.row_count_ = 1;
  header.block_size_ = 100;
  index_info.row_header_ = &header;
  est.physical_row_count_ = 1;
  const int64_t start_us = ObTimeUtility::current_time();

  ASSERT_EQ(OB_SUCCESS, precise_context.init(INT64_MAX, MACRO_BLOCK_COUNT));
  for (int64_t i = 0; i < MACRO_BLOCK_COUNT; ++i) {
    const MacroBlockId macro_id(0, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, header.set_macro_id(macro_id));
    index_info.parent_macro_id_ = macro_id;
    ASSERT_EQ(OB_SUCCESS, precise_context.on_leaf_node(index_info, est));
    ASSERT_EQ(OB_SUCCESS, precise_context.on_leaf_node(index_info, est));
  }
  for (int64_t i = MACRO_BLOCK_COUNT - 1; i >= 0; --i) {
    const MacroBlockId macro_id(0, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, header.set_macro_id(macro_id));
    index_info.parent_macro_id_ = macro_id;
    ASSERT_EQ(OB_SUCCESS, precise_context.on_leaf_node(index_info, est));
  }

  ASSERT_EQ(MACRO_BLOCK_COUNT, precise_context.get_total_macro_block_count());
  ASSERT_LT(ObTimeUtility::current_time() - start_us, 5L * 1000L * 1000L);
}

// 场景：构造包含两个宏块的小表并查询局部范围；覆盖边界微块下钻和真实数据微块读取；
// 预期：精确路径结果非零且小于旧接口的宏块粒度估算，行数保持一致。
TEST_F(TestRangeSpliter, test_precise_size_estimate_reaches_data_micro_blocks)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  constexpr int64_t DATA_MACRO_BLOCK_COUNT = 2;
  constexpr int64_t MICRO_BLOCK_COUNT_PER_MACRO = 10;
  prepare_sstable_handle(handle,
                         0 /* macro_idx_start */,
                         DATA_MACRO_BLOCK_COUNT,
                         10 /* snapshot_version */,
                         MICRO_BLOCK_COUNT_PER_MACRO);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));

  ObArray<ObStoreRange> ranges;
  const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                            "1        var1   -9    MIN      EXIST   CLF\n"
                            "7        var1   -9    MIN      EXIST   CLF\n";
  build_query_range(ranges, rowkey_data);

  ObPartitionMultiRangeSpliter spliter;
  ObRangePrecision range_precision;
  int64_t legacy_total_size = 0;
  int64_t legacy_row_count = 0;
  int64_t legacy_macro_block_count = 0;
  ASSERT_EQ(OB_SUCCESS,
            spliter.estimate_ranges_info(ranges,
                                         *index_read_info,
                                         sstables,
                                         legacy_total_size,
                                         legacy_row_count,
                                         legacy_macro_block_count,
                                         range_precision));

  int64_t precise_total_size = 0;
  int64_t precise_row_count = 0;
  int64_t precise_macro_block_count = 0;
  ASSERT_EQ(OB_SUCCESS,
            spliter.estimate_ranges_info_precise(ranges,
                                                 *index_read_info,
                                                 sstables,
                                                 precise_total_size,
                                                 precise_row_count,
                                                 precise_macro_block_count,
                                                 INT64_MAX));

  ASSERT_EQ(OB_DEFAULT_MACRO_BLOCK_SIZE, legacy_total_size);
  ASSERT_GT(precise_total_size, 0);
  ASSERT_LT(precise_total_size, legacy_total_size);
  ASSERT_EQ(12, legacy_row_count);
  ASSERT_EQ(9, precise_row_count);
  ASSERT_EQ(1, precise_macro_block_count);
}

TEST_F(TestRangeSpliter, test_mulit_basic)
{
  allocator_.reuse();

  ObArray<ObITable *> sstables;
  const ObTableReadInfo *index_read_info = &full_read_info_;
  ObTableHandleV2 handle;
  prepare_sstable_handle(handle, 0, 20, 10);
  ASSERT_EQ(OB_SUCCESS, sstables.push_back(handle.table_));

  // single task case
  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "7        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "19       var1   -9    MIN      EXIST   CLF\n"
                              "23       var1   -9    MIN      EXIST   CLF\n"
                              "26       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 1, *index_read_info, sstables, allocator_, multi_range_split_array));

    ASSERT_EQ(1, multi_range_split_array.count());
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));
  }

  // seperate range case, 5 ranges
  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "1        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "10       var1   -9    MIN      EXIST   CLF\n"
                              "12       var1   -9    MIN      EXIST   CLF\n"
                              "13       var1   -9    MIN      EXIST   CLF\n"
                              "15       var1   -9    MIN      EXIST   CLF\n"
                              "16       var1   -9    MIN      EXIST   CLF\n"
                              "18       var1   -9    MIN      EXIST   CLF\n"
                              "22       var1   -9    MIN      EXIST   CLF\n"
                              "28       var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 3, *index_read_info, sstables, allocator_, multi_range_split_array));

    LOG_INFO("multi_range_split_array", K(multi_range_split_array));

    ASSERT_EQ(3, multi_range_split_array.count());
    const char *rowkey_data1 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "1        var1   -9    MIN      EXIST   CLF\n"
                               "7        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data1);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(0)));

    const char *rowkey_data2 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "7        var1   -9    MIN      EXIST   CLF\n"
                               "8        var1   -9    MIN      EXIST   CLF\n"
                               "10       var1   -9    MIN      EXIST   CLF\n"
                               "12       var1   -9    MIN      EXIST   CLF\n"
                               "13        var1   -9    MIN      EXIST   CLF\n"
                               "15        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data2);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(1)));

    const char *rowkey_data3 = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                               "16        var1   -9    MIN      EXIST   CLF\n"
                               "18        var1   -9    MIN      EXIST   CLF\n"
                               "22        var1   -9    MIN      EXIST   CLF\n"
                               "28        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data3);
    ASSERT_EQ(true, loop_equal_ranges(ranges, multi_range_split_array.at(2)));
  }

  // single task case
  {
    ObArray<ObStoreRange> ranges;
    ObArrayArray<ObStoreRange> multi_range_split_array;
    const char *rowkey_data = "bigint   var   bigint  bigint flag    multi_version_row_flag\n"
                              "8        var1   -9    MIN      EXIST   CLF\n"
                              "8        var1   -9    MIN      EXIST   CLF\n";
    build_query_range(ranges, rowkey_data);

    ObPartitionMultiRangeSpliter spliter;
    ASSERT_EQ(OB_SUCCESS,
              spliter.build_range_array(
                  ranges, 3, *index_read_info, sstables, allocator_, multi_range_split_array));

    ASSERT_EQ(1, multi_range_split_array.count());
  }
}

}
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_partition_range_spliter.log*");
  OB_LOGGER.set_file_name("test_partition_range_spliter.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
