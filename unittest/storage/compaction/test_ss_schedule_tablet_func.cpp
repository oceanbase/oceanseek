/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include <gtest/gtest.h>
#define protected public
#define private public
#include "share/scheduler/test_dag_common.h"
#include "storage/compaction_v2/ob_ss_schedule_tablet_func.h"
#include "storage/compaction_v2/ob_ss_skip_major_merge_dag.h"
#include "storage/tablet/ob_tablet.h"
#undef private
#undef protected

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace share;
namespace unittest
{

class TestSSScheduleTabletFunc : public TestDagScheduler
{};

TEST_F(TestSSScheduleTabletFunc, async_index_skip_does_not_require_storage_schema)
{
  // The skip decision needs only the cached tablet flags and medium-info reason.
  // Deliberately leave storage_schema_addr_ unset: scheduling must not load it.
  storage::ObTablet tablet;
  const ObTabletID tablet_id(200001);
  tablet.tablet_meta_.tablet_id_ = tablet_id;
  tablet.table_store_cache_.is_async_index_refresh_target_ = true;
  tablet.table_store_cache_.is_async_delete_index_ = false;
  ASSERT_TRUE(tablet.is_async_index_storage_tablet());

  const int64_t merge_version = 100;
  for (const auto reason : {ObAdaptiveMergePolicy::NO_INC_DATA, ObAdaptiveMergePolicy::DURING_DDL}) {
    ObSSScheduleTabletFunc func(merge_version, reason, true);
    EXPECT_EQ(OB_SUCCESS, func.schedule_merge_dag(ObLSID(1), tablet, MEDIUM_MERGE,
        merge_version, 0, ObCOMajorMergeStrategy(), reason));
    ASSERT_EQ(1, func.skip_merge_tablet_ids_.count());
    EXPECT_EQ(tablet_id, func.skip_merge_tablet_ids_.at(0));
    EXPECT_EQ(merge_version, func.skip_major_merge_scn_);
  }
}

TEST_F(TestSSScheduleTabletFunc, duplicate_skip_major_dag_clears_transient_state)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), 1000));
  scheduler->prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_LOW].set_adaptive_limit(0);

  const ObLSID ls_id(1);
  const int64_t merge_version = 100;
  const ObTabletID tablet_id(200001);
  ObUpdateSkipMajorParam param(ls_id, merge_version);
  ASSERT_EQ(OB_SUCCESS, param.tablet_info_array_.push_back(tablet_id));
  ObSSSkipMajorMergeDag *dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->create_dag<ObSSSkipMajorMergeDag>(&param, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  ASSERT_EQ(1, scheduler->get_cur_dag_cnt());

  ObSSScheduleTabletFunc func(merge_version);
  func.ls_status_.ls_id_ = ls_id;
  func.skip_major_merge_scn_ = merge_version;
  ASSERT_EQ(OB_SUCCESS, func.skip_merge_tablet_ids_.push_back(tablet_id));

  EXPECT_EQ(OB_SUCCESS, func.post_process_ls());
  EXPECT_EQ(1, scheduler->get_cur_dag_cnt());
  EXPECT_TRUE(func.skip_merge_tablet_ids_.empty());
  EXPECT_EQ(0, func.skip_major_merge_scn_);

  func.ls_status_.ls_id_ = ObLSID(2);
  EXPECT_EQ(OB_SUCCESS, func.post_process_ls());
  EXPECT_TRUE(func.skip_merge_tablet_ids_.empty());
  EXPECT_EQ(0, func.skip_major_merge_scn_);

  ASSERT_EQ(OB_SUCCESS, scheduler->cancel_dag(dag));
  EXPECT_EQ(0, scheduler->get_cur_dag_cnt());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
