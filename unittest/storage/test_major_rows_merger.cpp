/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/compaction/ob_partition_rows_merger.h"
#include "storage/blocksstable/ob_multi_version_sstable_test.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/access/ob_rows_info.h"
#include "storage/ob_relative_table.h"
#include "storage/test_tablet_helper.h"
#include "mtlenv/storage/access/test_merge_basic.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace unittest;
namespace storage
{
template<typename T>
int prepare_partition_merge_iter(ObMergeParameter &merge_param,
                                 common::ObIAllocator &allocator,
                                 const ObITableReadInfo *read_info,
                                 const int iter_idx,
                                 ObPartitionMergeIter *&merge_iter)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
  } else {
    merge_iter = new (buf) T(allocator);
    if (OB_FAIL(merge_iter->init(merge_param, iter_idx, read_info))) {
      STORAGE_LOG(WARN, "failed to init merge iter", K(ret));
    }
  }
  return ret;
}

class ObMajorRowsMergerTest: public TestMergeBasic
{
public:
  ObMajorRowsMergerTest() : TestMergeBasic("test_multi_version_merge") {}
  virtual ~ObMajorRowsMergerTest() {}

  void SetUp();
  void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             const bool is_schema_changed_in_medium_info,
                             ObTabletMergeCtx &merge_context);
  int prepare_partition_minor_merge_iter(ObMergeParameter &merge_param,
                                 common::ObIAllocator &allocator,
                                 const ObITableReadInfo *read_info,
                                 const int iter_idx,
                                 ObPartitionMergeIter *&merge_iter);
  ObTabletMergeExecuteDag merge_dag_;
  ObCompactionFilterHandle filter_handle_;
};

void ObMajorRowsMergerTest::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  // mock sequence no
  ObClockGenerator::init();

  TestMergeBasic::create_tablet();
}

void ObMajorRowsMergerTest::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  // reset sequence no
  ObClockGenerator::destroy();
}

void ObMajorRowsMergerTest::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void ObMajorRowsMergerTest::TearDown()
{

  ObMultiVersionSSTableTest::TearDown();
  TRANS_LOG(INFO, "teardown success");
}

void ObMajorRowsMergerTest::prepare_merge_context(const ObMergeType &merge_type,
                                                  const bool is_full_merge,
                                                  const ObVersionRange &trans_version_range,
                                                  const bool is_schema_changed_in_medium_info,
                                                  ObTabletMergeCtx &merge_context)
{
  TestMergeBasic::prepare_merge_context(
      merge_type, is_full_merge, trans_version_range, &merge_dag_, merge_context, false/*is_delete_insert_merge*/);
  if (is_schema_changed_in_medium_info && merge_context.static_param_.major_merge_sstable_status_array_.count() > 0) {
    merge_context.static_param_.major_merge_sstable_status_array_.at(0).is_schema_changed_ = true;
  }
}

int ObMajorRowsMergerTest::prepare_partition_minor_merge_iter(ObMergeParameter &merge_param,
                                 common::ObIAllocator &allocator,
                                 const ObITableReadInfo *read_info,
                                 const int iter_idx,
                                 ObPartitionMergeIter *&merge_iter)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObPartitionMinorMacroMergeIter)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
  } else {
    merge_iter = new (buf) ObPartitionMinorMacroMergeIter(allocator, filter_handle_);
    if (OB_FAIL(merge_iter->init(merge_param, iter_idx, read_info))) {
      STORAGE_LOG(WARN, "failed to init merge iter", K(ret));
    }
  }
  return ret;
}

TEST_F(ObMajorRowsMergerTest, test_compare_func)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[3];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "2        var1  -8       MIN      3       2     T_DML_UPDATE EXIST   SCF\n"
      "2        var1  -8       0        3       NOP   T_DML_UPDATE EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint   bigint   bigint  bigint dml        flag    multi_version_row_flag\n"
      "2        var1  -6       0        2       2     T_DML_INSERT EXIST   CL\n"
      "3        var1  -8       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "4        var1  -8       0        NOP      1    T_DML_UPDATE  EXIST   LF\n";
  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_one_macro(&micro_data[1], 1);
  prepare_one_macro(&micro_data[2], 1);
  prepare_data_end(handle1);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -10       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n";
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MINOR_MERGE, false, trans_version_range, false, merge_context);
  ObMergeParameter merge_param(merge_context.static_param_);
  OK(merge_param.init(merge_context, 0, &allocator_));
  ObPartitionMergeIter *iter_0 = nullptr;
  ObPartitionMergeIter *iter_1 = nullptr;
  const ObITableReadInfo &read_info = merge_context.tablet_handle_.get_obj()->get_rowkey_read_info();
  prepare_partition_minor_merge_iter(merge_param, allocator_, &read_info, 0, iter_0);
  prepare_partition_minor_merge_iter(merge_param, allocator_, &read_info, 1, iter_1);
  OK(iter_0->next());
  OK(iter_1->next());

  int64_t cmp_ret;
  ObPartitionMergeLoserTreeItem item_0, item_1;
  item_0.iter_ = iter_0;
  item_0.iter_idx_ = 0;
  item_1.iter_ = iter_1;
  item_0.iter_idx_ = 1;

  ObPartitionMergeLoserTreeCmp cmp(read_info.get_datum_utils(), read_info.get_schema_rowkey_count());

  //compare both range
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::ALL_RANGE_NEED_OPEN, cmp_ret);

  OK(item_0.iter_->next());
  OK(item_0.iter_->next());
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ(1, cmp_ret);
  OK(cmp.compare(item_1, item_0, cmp_ret));
  ASSERT_EQ(-1, cmp_ret);

  OK(item_1.iter_->next());
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::LEFT_RANGE_NEED_OPEN, cmp_ret);
  OK(cmp.compare(item_1, item_0, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::RIGHT_RANGE_NEED_OPEN, cmp_ret);

  //compare rowkey and range
  OK(item_0.iter_->open_curr_range(false));
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::RIGHT_RANGE_NEED_OPEN, cmp_ret);
  OK(cmp.compare(item_1, item_0, cmp_ret));
  ASSERT_EQ((long)ObPartitionMergeLoserTreeCmp::LEFT_RANGE_NEED_OPEN, cmp_ret);

  //compare both rowkey
  OK(item_1.iter_->open_curr_range(false));
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ(-1, cmp_ret);

  OK(item_0.iter_->next());
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ(0, cmp_ret);

  OK(item_0.iter_->next());
  OK(cmp.compare(item_0, item_1, cmp_ret));
  ASSERT_EQ(1, cmp_ret);
  if (OB_NOT_NULL(iter_0)) {
    iter_0->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_1)) {
    iter_1->~ObPartitionMergeIter();
  }
}

TEST_F(ObMajorRowsMergerTest, single)
{
  int ret = OB_SUCCESS;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint flag    multi_version_row_flag \n"
      "0        var0  -5      0        2       2     EXIST   N    \n"
      "1        var1  -8     -0       1       15    EXIST   N    \n"
      "2        var2  -8     -0       2       15    EXIST   N    \n";
  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);

  reset_writer(snapshot_version, MAJOR_MERGE);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1, ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "1        var1  -10       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var1  -10       0        NOP     10   T_DML_UPDATE  EXIST   LF\n";
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;

  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, true, merge_context);
  ObMergeParameter merge_param(merge_context.static_param_);
  OK(merge_param.init(merge_context, 0, &allocator_));
  ObPartitionMergeIter *iter_0 = nullptr;
  ObPartitionMergeIter *iter_1 = nullptr;
  const ObITableReadInfo &read_info = merge_context.tablet_handle_.get_obj()->get_rowkey_read_info();
  OK(prepare_partition_merge_iter<ObPartitionMacroMergeIter>(merge_param, allocator_, &read_info, 0, iter_0));
  OK(prepare_partition_merge_iter<ObPartitionRowMergeIter>(merge_param, allocator_, &read_info, 1, iter_1));
  iter_0->next();
  iter_1->next();
  ObPartitionMergeLoserTreeItem item_0, item_1;
  item_0.iter_ = iter_0; item_0.iter_idx_ = 0;
  item_1.iter_ = iter_1; item_1.iter_idx_ = 1;
  ObPartitionMergeLoserTreeCmp cmp(read_info.get_datum_utils(), read_info.get_schema_rowkey_count());

  ObPartitionMajorRowsMerger merger(cmp);

  // not init
  ASSERT_EQ(ObPartitionMajorRowsMerger::NOT_INIT, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());

  // 0 player
  ret = merger.init(0, 0, allocator_);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);

  // init twice
  OK(merger.init(1, 1, allocator_));
  ASSERT_TRUE(merger.is_unique_champion());
  ret = merger.init(1, 1, allocator_);
  ASSERT_EQ(ret, OB_INIT_TWICE);
  ASSERT_EQ(0, merger.count());

  const ObPartitionMergeLoserTreeItem *top;
  //push base_iter
  ASSERT_EQ(OB_SUCCESS, merger.push(item_0));
  ASSERT_EQ(OB_SIZE_OVERFLOW, merger.push(item_0));
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  ASSERT_EQ(OB_SUCCESS, merger.top(top));
  ASSERT_EQ(item_0.iter_idx_, top->iter_idx_);
  ASSERT_EQ(OB_SUCCESS, merger.pop());

  //push other iter
  ASSERT_EQ(OB_SUCCESS, merger.push(item_1));
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::LOSER_TREE_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  ASSERT_EQ(OB_SUCCESS, merger.top(top));
  ASSERT_EQ(item_1.iter_idx_, top->iter_idx_);
  ASSERT_EQ(OB_SUCCESS, merger.pop());
  if (OB_NOT_NULL(iter_0)) {
    iter_0->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_1)) {
    iter_1->~ObPartitionMergeIter();
  }
}

TEST_F(ObMajorRowsMergerTest, two_iters)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle1;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag\n"
      "1        var1  -8       0        1        1      EXIST\n"
      "8        var1  -8       0        2        2      EXIST\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version, MAJOR_MERGE);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle1, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);


  STORAGE_LOG(INFO, "finish prepare sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -30       0        NOP     10   T_DML_UPDATE  DELETE   LF\n"
      "1        var1  -30       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  micro_data2[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "3        var1  -30       0        NOP     10   T_DML_UPDATE  EXIST   LF\n";
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data2, 1);
  //prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare sstable2");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, true, merge_context);
  ObMergeParameter merge_param(merge_context.static_param_);
  OK(merge_param.init(merge_context, 0, &allocator_));
  ObPartitionMergeIter *iter_0 = nullptr;
  ObPartitionMergeIter *iter_1 = nullptr;
  const ObITableReadInfo &read_info = merge_context.tablet_handle_.get_obj()->get_rowkey_read_info();
  OK(prepare_partition_merge_iter<ObPartitionMacroMergeIter>(merge_param, allocator_, &read_info, 0, iter_0));
  OK(prepare_partition_merge_iter<ObPartitionRowMergeIter>(merge_param, allocator_, &read_info, 1, iter_1));
  OK(iter_0->next());
  OK(iter_1->next());
  const ObPartitionMergeLoserTreeItem *top;
  ObPartitionMergeLoserTreeItem item_0, item_1;
  item_0.iter_ = iter_0;
  item_0.iter_idx_ = 0;
  item_1.iter_ = iter_1;
  item_1.iter_idx_ = 1;
  ObPartitionMergeLoserTreeCmp cmp(read_info.get_datum_utils(), read_info.get_schema_rowkey_count());
  ObPartitionMajorRowsMerger merger(cmp);

  //need purge
  OK(merger.init(2, 2, allocator_));
  OK(merger.push(item_0));
  OK(merger.push(item_1));
  ASSERT_EQ(2, merger.count());
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_REBUILD, merger.merger_state_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, merger.pop());
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_SKIP, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());

  //get purge iter
  OK(merger.top(top));
  ASSERT_EQ(1, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_SKIP_REBUILD, merger.merger_state_);
  OK(item_1.iter_->next());
  OK(merger.push_top(item_1));
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_FALSE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(1, top->iter_idx_);
  OK(merger.pop());
  OK(merger.top(top));
  ASSERT_TRUE(merger.is_unique_champion());
  ASSERT_EQ(0, top->iter_idx_);
  OK(merger.pop());
  ASSERT_TRUE(merger.empty());
  if (OB_NOT_NULL(iter_0)) {
    iter_0->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_1)) {
    iter_1->~ObPartitionMergeIter();
  }
}

TEST_F(ObMajorRowsMergerTest, multi_majors)
{
  int ret = OB_SUCCESS;
  merge_type_ = MAJOR_MERGE;
  ObTabletMergeDagParam param;
  ObTabletMajorMergeCtx merge_context(param, allocator_);

  ObTableHandleV2 handle;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint   flag\n"
      "1        var1  -5       0        1        1      EXIST\n"
      "6        var1  -5       0        2        2      EXIST\n";

  int schema_rowkey_cnt = 2;

  int64_t snapshot_version = 10;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version, MAJOR_MERGE);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(handle, storage::ObITable::MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle);
  STORAGE_LOG(INFO, "finish prepare major sstable");

  ObTableHandleV2 handle1;
  const char *micro_data1[1];
  micro_data1[0] = 
      "bigint   var   bigint   bigint   bigint bigint   flag\n"
      "2        var1  -7       0        1        1      EXIST\n"
      "5        var1  -7       0        2        2      EXIST\n";
  
  snapshot_version = 10;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data1, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version, MAJOR_MERGE);
  prepare_one_macro(micro_data1, 1);
  prepare_data_end(handle1, storage::ObITable::INC_MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle1);
  STORAGE_LOG(INFO, "finish prepare inc major sstable1");

  ObTableHandleV2 handle2;
  const char *micro_data2[2];
  micro_data2[0] = 
      "bigint   var   bigint   bigint   bigint bigint   flag\n"
      "7        var1  -8       0        1        1      EXIST\n";
  micro_data2[1] = 
      "bigint   var   bigint   bigint   bigint bigint   flag\n"
      "10       var1  -8       0        1        1      EXIST\n"
      "12       var1  -8       0        2        2      EXIST\n";
  
  snapshot_version = 10;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);
  prepare_table_schema(micro_data2, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version, MAJOR_MERGE);
  prepare_one_macro(micro_data2, 1);
  prepare_one_macro(&micro_data2[1], 1);
  prepare_data_end(handle2, storage::ObITable::INC_MAJOR_SSTABLE);
  merge_context.static_param_.tables_handle_.add_table(handle2);
  STORAGE_LOG(INFO, "finish prepare inc major sstable2");

  ObTableHandleV2 handle3;
  const char *micro_data3[2];
  micro_data3[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "1        var1  -30       0        NOP     10   T_DML_UPDATE  EXIST   LF\n"
      "3        var1  -30       0        NOP     12   T_DML_UPDATE  EXIST   LF\n";

  micro_data3[1] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "4        var1  -30       0        NOP   10   T_DML_UPDATE    EXIST   LF\n"
      "11        var1  -30       0        2     10   T_DML_UPDATE    DELETE   LF\n";
  snapshot_version = 20;
  scn_range.start_scn_.convert_for_tx(10);
  scn_range.end_scn_.convert_for_tx(20);
  table_key_.scn_range_ = scn_range;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data3, 2);
  prepare_data_end(handle3);
  merge_context.static_param_.tables_handle_.add_table(handle3);
  STORAGE_LOG(INFO, "finish prepare mini sstable");

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  
  prepare_merge_context(MAJOR_MERGE, false, trans_version_range, true, merge_context);
  ObMergeParameter merge_param(merge_context.static_param_);
  OK(merge_param.init(merge_context, 0, &allocator_));

  ObPartitionMergeIter *iter_major = nullptr;
  ObPartitionMergeIter *iter_inc_major_1 = nullptr;
  ObPartitionMergeIter *iter_inc_major_2 = nullptr;
  ObPartitionMergeIter *iter_inc = nullptr;
  const ObITableReadInfo &read_info = merge_context.tablet_handle_.get_obj()->get_rowkey_read_info();
  OK(prepare_partition_merge_iter<ObPartitionMacroMergeIter>(merge_param, allocator_, &read_info, 0, iter_major));
  OK(prepare_partition_merge_iter<ObPartitionMacroMergeIter>(merge_param, allocator_, &read_info, 1, iter_inc_major_1));
  OK(prepare_partition_merge_iter<ObPartitionMacroMergeIter>(merge_param, allocator_, &read_info, 2, iter_inc_major_2));
  OK(prepare_partition_merge_iter<ObPartitionRowMergeIter>(merge_param, allocator_, &read_info, 3, iter_inc));
  OK(iter_major->next());
  OK(iter_inc_major_1->next());
  OK(iter_inc_major_2->next());
  OK(iter_inc->next());
  const ObPartitionMergeLoserTreeItem *top;
  ObPartitionMergeLoserTreeItem item_major, item_inc_major_1, item_inc_major_2, item_inc;
  item_major.iter_ = iter_major;
  item_major.iter_idx_ = 0;
  item_inc_major_1.iter_ = iter_inc_major_1;
  item_inc_major_1.iter_idx_ = 1;
  item_inc_major_2.iter_ = iter_inc_major_2;
  item_inc_major_2.iter_idx_ = 2;
  item_inc.iter_ = iter_inc;
  item_inc.iter_idx_ = 3;
  ObPartitionMergeLoserTreeCmp cmp(read_info.get_datum_utils(), read_info.get_schema_rowkey_count());
  ObPartitionMajorRowsMerger merger(cmp, 3);

  OK(merger.init(4, 4, allocator_));
  OK(merger.push(item_major));
  OK(merger.push(item_inc_major_1));
  OK(merger.push(item_inc_major_2));
  OK(merger.push(item_inc));
  ASSERT_EQ(4, merger.count());
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_REBUILD, merger.merger_state_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, merger.pop());
  OK(merger.rebuild());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_FALSE(merger.is_unique_champion());

  OK(merger.top(top));
  ASSERT_EQ(3, top->iter_idx_);
  OK(merger.pop());
  ASSERT_TRUE(merger.is_unique_champion());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  OK(merger.top(top));
  ASSERT_EQ(0, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  OK(item_inc.iter_->next());
  OK(item_major.iter_->next());
  OK(merger.push(item_inc));
  OK(merger.push(item_major));
  OK(merger.rebuild());

  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(1, top->iter_idx_);
  OK(merger.pop());
  OK(item_inc_major_1.iter_->next());
  OK(merger.push_top(item_inc_major_1));
  ASSERT_EQ(ObPartitionMajorRowsMerger::LOSER_TREE_WIN, merger.merger_state_);
  OK(merger.rebuild()); // do nothing

  ASSERT_EQ(ObPartitionMajorRowsMerger::LOSER_TREE_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(3, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  OK(item_inc.iter_->next());
  OK(merger.push_top(item_inc)); // do compare_base_iter again
  ASSERT_EQ(ObPartitionMajorRowsMerger::LOSER_TREE_WIN, merger.merger_state_);
  OK(merger.rebuild()); // do nothing

  ASSERT_EQ(ObPartitionMajorRowsMerger::LOSER_TREE_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(3, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  OK(item_inc.iter_->next());
  OK(merger.push_top(item_inc));
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  OK(merger.rebuild()); // do nothing

  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(1, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_EQ(OB_ITER_END, item_inc_major_1.iter_->next());
  OK(merger.rebuild()); // do nothing

  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(0, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_EQ(OB_ITER_END, item_major.iter_->next());
  OK(merger.rebuild()); // do nothing

  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(2, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::LOSER_TREE_WIN, merger.merger_state_);
  OK(item_inc_major_2.iter_->next());
  OK(merger.push_top(item_inc_major_2));
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_SKIP, merger.merger_state_);
  OK(merger.rebuild()); // do nothing

  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_SKIP, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(3, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::NEED_SKIP_REBUILD, merger.merger_state_);
  ASSERT_EQ(OB_ITER_END, item_inc.iter_->next());
  OK(merger.rebuild()); // do nothing

  ASSERT_EQ(ObPartitionMajorRowsMerger::BASE_ITER_WIN, merger.merger_state_);
  ASSERT_TRUE(merger.is_unique_champion());
  OK(merger.top(top));
  ASSERT_EQ(2, top->iter_idx_);
  OK(merger.pop());
  ASSERT_EQ(ObPartitionMajorRowsMerger::LOSER_TREE_WIN, merger.merger_state_);
  ASSERT_EQ(OB_ITER_END, item_inc_major_2.iter_->next());

  ASSERT_TRUE(merger.empty());
  if (OB_NOT_NULL(iter_major)) {
    iter_major->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_inc_major_1)) {
    iter_inc_major_1->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_inc_major_2)) {
    iter_inc_major_2->~ObPartitionMergeIter();
  }
  if (OB_NOT_NULL(iter_inc)) {
    iter_inc->~ObPartitionMergeIter();
  }
}

TEST_F(ObMajorRowsMergerTest, inc_major_row_lock_check_with_exist)
{
  const int64_t build_snapshot_version = 100;
  const int64_t read_snapshot_version = 200;
  const int64_t commit_version = 300;
  const int64_t schema_rowkey_cnt = 1;
  const int64_t row_count = 2;
  const char *micro_data[1];
  micro_data[0] =
      "bigint bigint bigint bigint flag\n"
      "1      -100   0      11     EXIST\n"
      "2      -100   0      22     EXIST\n";

  merge_type_ = MAJOR_MERGE;
  share::ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  ASSERT_EQ(OB_SUCCESS, scn_range.end_scn_.convert_for_tx(build_snapshot_version));
  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, build_snapshot_version);
  reset_writer(build_snapshot_version, MAJOR_MERGE);
  prepare_one_macro(micro_data, 1);

  ObTableHandleV2 table_handle;
  prepare_data_end(table_handle, ObITable::INC_MAJOR_SSTABLE);
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(sstable));
  ASSERT_NE(nullptr, sstable);
  ASSERT_TRUE(sstable->is_inc_major_related_sstable());
  ASSERT_FALSE(sstable->contain_uncommitted_row());
  ASSERT_EQ(build_snapshot_version, sstable->get_max_merged_trans_version());
  ASSERT_EQ(OB_SUCCESS, sstable->set_upper_trans_version(allocator_, commit_version));
  ASSERT_EQ(commit_version, sstable->get_upper_trans_version());
  ASSERT_LE(build_snapshot_version, read_snapshot_version);
  ASSERT_GT(commit_version, read_snapshot_version);

  ObLSService *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS,
            ls_service->get_ls(ObLSID(ls_id_), ls_handle, ObLSGetMod::STORAGE_MOD));
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS,
            ls_handle.get_ls()->get_tablet(ObTabletID(tablet_id_), tablet_handle));
  ASSERT_NE(nullptr, tablet_handle.get_obj());

  share::SCN snapshot_scn;
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(read_snapshot_version));
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS,
            store_ctx.init_for_read(ObLSID(ls_id_), ObTabletID(tablet_id_),
                                    INT64_MAX, -1, snapshot_scn));
  ObVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = 0;
  version_range.snapshot_version_ = read_snapshot_version;
  ObQueryFlag query_flag;
  ObTableAccessContext access_ctx;
  ASSERT_EQ(OB_SUCCESS,
            access_ctx.init(query_flag, store_ctx, allocator_, allocator_, version_range));
  access_ctx.ls_id_ = ObLSID(ls_id_);

  ObSEArray<uint64_t, 4> column_ids;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(column_ids));
  ObTableDMLParam table_dml_param(allocator_);
  ASSERT_EQ(OB_SUCCESS,
            table_dml_param.convert(&table_schema_, SCHEMA_VERSION, column_ids));
  ObRelativeTable relative_table;
  ASSERT_EQ(OB_SUCCESS,
            relative_table.init(&table_dml_param.get_data_table(), ObTabletID(tablet_id_)));
  ASSERT_EQ(OB_SUCCESS, relative_table.tablet_iter_.set_tablet_handle(tablet_handle));

  const ObITableReadInfo &rowkey_read_info = tablet_handle.get_obj()->get_rowkey_read_info();
  ObRowsInfo rows_info;
  ASSERT_EQ(OB_SUCCESS,
            rows_info.init(table_dml_param.get_col_descs(), relative_table,
                           store_ctx, rowkey_read_info));
  ObDatumRow rows[row_count];
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, rows[i].init(allocator_, column_ids.count()));
    rows[i].storage_datums_[0].set_int(i + 1);
    rows[i].storage_datums_[1].set_int((i + 1) * 11);
    rows[i].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
  }
  ASSERT_EQ(OB_SUCCESS, rows_info.assign_rows(row_count, rows));
  ObStoreRowLockState batch_lock_states[row_count];
  for (int64_t i = 0; i < row_count; ++i) {
    rows_info.set_row_lock_state(i, &batch_lock_states[i]);
  }

  // 单行存在性检查在 M <= R < C 时需使用真实提交版本，并优先返回事务集合冲突。
  ObStoreRowLockState single_lock_state;
  ASSERT_EQ(OB_SUCCESS,
            sstable->check_row_locked(rows_info.exist_helper_.table_iter_param_,
                                      rows_info.get_rowkey(0), access_ctx,
                                      single_lock_state, true));
  EXPECT_EQ(commit_version, single_lock_state.trans_version_.get_val_for_tx());
  EXPECT_GT(single_lock_state.trans_version_.get_val_for_tx(),
            read_snapshot_version);
  EXPECT_EQ(ObDmlFlag::DF_INSERT, single_lock_state.lock_dml_flag_);

  // 批量存在性检查同样需使用真实提交版本，事务集合冲突优先于主键重复。
  share::SCN max_trans_version = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS,
            sstable->check_rows_locked(true, access_ctx, max_trans_version, rows_info));
  ASSERT_TRUE(rows_info.have_conflict());
  EXPECT_EQ(OB_TRANSACTION_SET_VIOLATION, rows_info.get_error_code());
  const int64_t conflict_idx = rows_info.get_conflict_idx();
  ASSERT_GE(conflict_idx, 0);
  ASSERT_LT(conflict_idx, row_count);
  EXPECT_EQ(commit_version,
            rows_info.get_row_lock_state(conflict_idx).trans_version_.get_val_for_tx());
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_major_rows_merger.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_major_rows_merger.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
