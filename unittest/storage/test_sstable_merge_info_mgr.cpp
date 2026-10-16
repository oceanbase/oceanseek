/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #include <gtest/gtest.h>
 #include <iostream>
 #define protected public
 #define private public
#include "storage/compaction/ob_sstable_merge_info_mgr.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace lib;
using namespace share;
using namespace omt;
using namespace compaction;

namespace unittest
{
static const int64_t MERGE_INFO_PAGE_SIZE = 1LL << 13; // 8KB
class TestSSTableMergeInfoMgr : public ::testing::Test
{
public:
  TestSSTableMergeInfoMgr()
    : tenant_id_(1),
      merge_info_mgr_(nullptr),
      tenant_base_(tenant_id_)
  { }
  ~TestSSTableMergeInfoMgr() {}
  void SetUp()
  {
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id_);
    ObUnitInfoGetter::ObTenantConfig unit_config;
    unit_config.mode_ = lib::Worker::CompatMode::MYSQL;
    unit_config.tenant_id_ = tenant_id_;
    TenantUnits units;
    ASSERT_EQ(OB_SUCCESS, units.push_back(unit_config));

    merge_info_mgr_ = OB_NEW(ObTenantSSTableMergeInfoMgr, ObModIds::TEST);
    tenant_base_.set(merge_info_mgr_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

    ObMallocAllocator *ma = ObMallocAllocator::get_instance();
    ASSERT_EQ(OB_SUCCESS, ma->set_tenant_limit(tenant_id_, 1LL << 30));
  }
  void TearDown()
  {
    merge_info_mgr_->~ObTenantSSTableMergeInfoMgr();
    merge_info_mgr_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id_);
  }
private:
  const uint64_t tenant_id_;
  ObTenantSSTableMergeInfoMgr *merge_info_mgr_;
  ObTenantBase tenant_base_;
  DISALLOW_COPY_AND_ASSIGN(TestSSTableMergeInfoMgr);
};

void init_merge_history(ObSSTableMergeHistory &merge_info)
{
  merge_info.static_info_.ls_id_ = 1;
  merge_info.static_info_.tablet_id_ = 3;
  merge_info.static_info_.compaction_scn_ = 100;
  merge_info.static_info_.merge_type_ = ObMergeType::MINOR_MERGE;
  merge_info.static_info_.exec_mode_ = EXEC_MODE_LOCAL;
  merge_info.running_info_.merge_start_time_ = ObTimeUtility::fast_current_time();
  merge_info.running_info_.merge_finish_time_ = ObTimeUtility::fast_current_time();
  merge_info.running_info_.dag_id_.init(GCTX.self_addr());
}

TEST_F(TestSSTableMergeInfoMgr, update_merge_task_info)
{
  ObSSTableMergeHistory merge_info;
  ObMergeBlockInfo first_task;
  ObMergeBlockInfo second_task;
  merge_info.static_info_.concurrent_cnt_ = 2;
  first_task.macro_block_count_ = 1;
  first_task.total_row_count_ = 10;
  first_task.incremental_row_count_ = 4;
  first_task.filter_row_count_ = 1;
  first_task.new_micro_count_in_new_macro_ = 2;
  first_task.original_size_ = 100;
  first_task.compressed_size_ = 40;
  first_task.new_micro_info_.add_data_micro_size(30);
  first_task.block_io_us_ = 7;
  second_task.macro_block_count_ = 2;
  second_task.total_row_count_ = 20;
  second_task.incremental_row_count_ = 6;
  second_task.filter_row_count_ = 2;
  second_task.new_micro_count_in_new_macro_ = 3;
  second_task.original_size_ = 200;
  second_task.compressed_size_ = 80;
  second_task.new_micro_info_.add_data_micro_size(50);
  second_task.block_io_us_ = 11;

  merge_info.update_merge_task_info(first_task, false /*without_row_cnt*/, 100, 30);
  merge_info.update_merge_task_info(second_task, false /*without_row_cnt*/, 200, 50);

  EXPECT_EQ(3, merge_info.block_info_.macro_block_count_);
  EXPECT_EQ(30, merge_info.block_info_.total_row_count_);
  EXPECT_EQ(10, merge_info.block_info_.incremental_row_count_);
  EXPECT_EQ(3, merge_info.block_info_.filter_row_count_);
  EXPECT_EQ(5, merge_info.block_info_.new_micro_count_in_new_macro_);
  EXPECT_EQ(300, merge_info.block_info_.original_size_);
  EXPECT_EQ(120, merge_info.block_info_.compressed_size_);
  EXPECT_EQ(80, merge_info.block_info_.new_micro_info_.get_data_micro_size());
  EXPECT_EQ(18, merge_info.block_info_.block_io_us_);
  EXPECT_EQ(300, merge_info.running_info_.execute_time_);
  const ObParalleMergeInfo::BasicInfo &scan_info =
      merge_info.running_info_.parallel_merge_info_.info_[ObParalleMergeInfo::SCAN_UNITS];
  EXPECT_EQ(30, scan_info.min_value_);
  EXPECT_EQ(50, scan_info.max_value_);
  EXPECT_EQ(80, scan_info.sum_value_);
  EXPECT_EQ(2, scan_info.count_);
}

TEST_F(TestSSTableMergeInfoMgr, normal)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeHistory merge_info;
  compaction::ObDiagnoseInfoParam<2, 0> info_param;
  info_param.type_.suspect_type_ = ObSuspectInfoType::SUSPECT_MEMTABLE_CANT_MINOR_MERGE;
  info_param.struct_type_ = compaction::ObInfoParamStructType::SUSPECT_INFO_PARAM;
  merge_info.info_param_ = &info_param;

  ObTenantSSTableMergeInfoMgr *merge_info_mgr = MTL(ObTenantSSTableMergeInfoMgr*);
  ASSERT_TRUE(nullptr != merge_info_mgr);
  MTL(ObTenantSSTableMergeInfoMgr*)->destroy();
  //not init
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(MERGE_INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_NE(OB_SUCCESS, ret);

  init_merge_history(merge_info);
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.static_info_.tablet_id_ = 3;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

}

TEST_F(TestSSTableMergeInfoMgr, iterator)
{
  int ret = OB_SUCCESS;
  ObTenantSSTableMergeInfoMgr *merge_info_mgr = MTL(ObTenantSSTableMergeInfoMgr*);
  ASSERT_TRUE(nullptr != merge_info_mgr);

  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(MERGE_INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTableMergeHistory merge_info;
  compaction::ObDiagnoseInfoParam<2, 0> info_param;
  info_param.type_.suspect_type_ = ObSuspectInfoType::SUSPECT_MEMTABLE_CANT_MINOR_MERGE;
  info_param.struct_type_ = compaction::ObInfoParamStructType::SUSPECT_INFO_PARAM;
  merge_info.info_param_ = &info_param;

  init_merge_history(merge_info);
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.static_info_.tablet_id_ = 4;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.static_info_.tablet_id_ = 1;
  merge_info.static_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.static_info_.tablet_id_ = 2;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(4, MTL(ObTenantSSTableMergeInfoMgr*)->size());

  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator, minor_iterator));

  ObSSTableMergeHistory read_info;
  char comment[common::OB_COMPACTION_EVENT_STR_LENGTH];
  int i = 1;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(ObTabletID(i), read_info.static_info_.tablet_id_);
      ++i;
    }
  }
  ASSERT_EQ(OB_ITER_END, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)));

  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator1;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator1;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator1, minor_iterator1));
  i = 1;
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator1, minor_iterator1, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.static_info_.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.static_info_.merge_type_ == MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator1, minor_iterator1, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.static_info_.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.static_info_.merge_type_ == MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator1, minor_iterator1, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.static_info_.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.static_info_.merge_type_ == MINOR_MERGE);

  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator2;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator2;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator2, minor_iterator2));
  i = 1;
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator2, minor_iterator2, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.static_info_.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.static_info_.merge_type_ == MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator2, minor_iterator2, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.static_info_.tablet_id_ == ObTabletID(i++));
  ASSERT_EQ(TRUE, read_info.static_info_.merge_type_ == MAJOR_MERGE);
}

void add_merge_info(
  const int64_t tablet_id,
  const ObMergeType merge_type)
{
  ObSSTableMergeHistory merge_info;
  init_merge_history(merge_info);
  merge_info.static_info_.tablet_id_ = ObTabletID(tablet_id);
  merge_info.static_info_.compaction_scn_ = 100;
  merge_info.static_info_.merge_type_ = merge_type;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_info));
}

TEST_F(TestSSTableMergeInfoMgr, resize)
{
  int ret = OB_SUCCESS;
  ObTenantSSTableMergeInfoMgr *merge_info_mgr = MTL(ObTenantSSTableMergeInfoMgr*);
  ASSERT_TRUE(nullptr != merge_info_mgr);

  ret = MTL(ObTenantSSTableMergeInfoMgr*)->init(MERGE_INFO_PAGE_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTableMergeHistory merge_info;
  compaction::ObDiagnoseInfoParam<2, 0> info_param;
  info_param.type_.suspect_type_ = ObSuspectInfoType::SUSPECT_MEMTABLE_CANT_MINOR_MERGE;
  info_param.struct_type_ = compaction::ObInfoParamStructType::SUSPECT_INFO_PARAM;
  merge_info.info_param_ = &info_param;
#define ADD_MAJOR_MERGE_INFO(tablet_id) \
  add_merge_info(tablet_id, ObMergeType::MAJOR_MERGE)
#define ADD_MINOR_MERGE_INFO(tablet_id) \
  add_merge_info(tablet_id, ObMergeType::MINOR_MERGE)
  
  const int64_t max_cnt = 20;
  int i = 0;
  for (i = 0; i < max_cnt; ++i) {
    ADD_MAJOR_MERGE_INFO(i + 1);
  }
  ASSERT_EQ(max_cnt, MTL(ObTenantSSTableMergeInfoMgr*)->size());

  for (i = 0; i < max_cnt; ++i) {
    ADD_MINOR_MERGE_INFO(i + 1);
  }
  ASSERT_EQ(2 * max_cnt, MTL(ObTenantSSTableMergeInfoMgr*)->size());
  
  compaction::ObIDiagnoseInfoMgr::Iterator major_iterator;
  compaction::ObIDiagnoseInfoMgr::Iterator minor_iterator;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_iterator, minor_iterator));

  ObSSTableMergeHistory read_info;
  char comment[common::OB_COMPACTION_EVENT_STR_LENGTH];
  ASSERT_EQ(OB_SUCCESS, ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)));
  ASSERT_EQ(TRUE, read_info.static_info_.tablet_id_ == ObTabletID(1));
  ASSERT_EQ(TRUE, read_info.static_info_.merge_type_ == ObMergeType::MAJOR_MERGE);

  const int64_t MEMORY_SIZE = 4 * MERGE_INFO_PAGE_SIZE;
  ret = MTL(ObTenantSSTableMergeInfoMgr*)->set_max(MEMORY_SIZE);
  // after set max size, major pool need purge under GC_LOW_PERCENTAGE, minor pool should not purge

  const int64_t minor_pool_size = upper_align(MEMORY_SIZE * ObTenantSSTableMergeInfoMgr::MINOR_MEMORY_PERCENTAGE / 100, MERGE_INFO_PAGE_SIZE);
  const int64_t major_pool_size = upper_align(MEMORY_SIZE * ObTenantSSTableMergeInfoMgr::MAJOR_MEMORY_PERCENTAGE / 100, MERGE_INFO_PAGE_SIZE);
  const int64_t after_purge_minor_pool_size = ObIDiagnoseInfoMgr::GC_LOW_PERCENTAGE / 100.0 * minor_pool_size;
  const int64_t after_purge_major_pool_size = ObIDiagnoseInfoMgr::GC_LOW_PERCENTAGE / 100.0 * major_pool_size;
  int64_t minor_item_cnt_after_purge = MIN(after_purge_minor_pool_size / sizeof(ObSSTableMergeHistory), max_cnt);
  int64_t major_item_cnt_after_purge = MIN(after_purge_major_pool_size / sizeof(ObSSTableMergeHistory), max_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "print item cnt", K(minor_item_cnt_after_purge), K(major_item_cnt_after_purge));
  ASSERT_EQ(minor_item_cnt_after_purge + major_item_cnt_after_purge, MTL(ObTenantSSTableMergeInfoMgr*)->size());
  int64_t read_idx = 0;
  // read major merge info from {max_cnt-major_item_cnt_after_purge..max_cnt}
  int64_t tablet_start_idx = max_cnt - major_item_cnt_after_purge + 1;
  while (read_idx < major_item_cnt_after_purge && OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.static_info_.tablet_id_ == ObTabletID(tablet_start_idx + read_idx));
      ASSERT_EQ(TRUE, read_info.static_info_.merge_type_ == ObMergeType::MAJOR_MERGE);
      ++read_idx;
    }
  }
  // read minor merge info from {1..max_cnt}
  read_idx = 0;
  tablet_start_idx = max_cnt - minor_item_cnt_after_purge + 1;
  while (read_idx < minor_item_cnt_after_purge && OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_iterator, minor_iterator, read_info, comment, sizeof(comment)))) {
      ASSERT_EQ(OB_ITER_END, ret);
    } else {
      ASSERT_EQ(TRUE, read_info.static_info_.tablet_id_ == ObTabletID(tablet_start_idx + read_idx));
      ASSERT_EQ(TRUE, read_info.static_info_.merge_type_ == ObMergeType::MINOR_MERGE);
      ++read_idx;
    }
  }
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_merge_info_mgr.log*");
  OB_LOGGER.set_file_name("test_sstable_merge_info_mgr.log", true);
  //OB_LOGGER.set_log_level("DEBUG");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
