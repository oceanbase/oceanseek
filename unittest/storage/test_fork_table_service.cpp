/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>

#define private public
#include "storage/high_availability/ob_fork_table_service.h"
#undef private

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace share;
using namespace storage;

class TestForkTableService : public ::testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, service_.thread_cond_.init(ObWaitEventIds::HA_SERVICE_COND_WAIT));
    ASSERT_EQ(OB_SUCCESS, service_.reported_group_set_.create(16, "ForkRptUTBkt", "ForkRptUTNode"));
    ASSERT_EQ(OB_SUCCESS, service_.finish_refresh_set_.create(16, "ForkFinUTBkt", "ForkFinUTNode"));
    ASSERT_EQ(OB_SUCCESS, service_.sched_state_map_.create(16, ObMemAttr(OB_SERVER_TENANT_ID, "ForkSchedUT")));
    service_.is_inited_ = true;
  }

  void TearDown() override
  {
    service_.is_inited_ = false;
    service_.reported_group_set_.destroy();
    service_.finish_refresh_set_.destroy();
    service_.sched_state_map_.destroy();
    service_.thread_cond_.destroy();
  }

  ObForkTableService service_;
};

TEST_F(TestForkTableService, finish_refresh_submission_deduplicates_wakeup)
{
  const ObLSID ls_id(1001);
  const ObTabletID tablet_id(200001);
  SCN reorg_scn;
  ASSERT_EQ(OB_SUCCESS, reorg_scn.convert_for_tx(1000));
  ObForkTabletInfo fork_info;
  fork_info.set_fork_snapshot_version(1000);
  fork_info.set_src_tablet_id(ObTabletID(200002));
  fork_info.set_data_complete(true);
  const ObForkTableService::ObForkFinishRefreshKey key(ls_id, tablet_id, reorg_scn, fork_info);

  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, tablet_id, reorg_scn, fork_info));
  ASSERT_EQ(1, service_.finish_refresh_set_.size());
  ASSERT_EQ(1, service_.wakeup_cnt_);
  for (int64_t i = 0; i < 100; ++i) {
    ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, tablet_id, reorg_scn, fork_info));
  }
  ASSERT_EQ(1, service_.finish_refresh_set_.size());
  ASSERT_EQ(1, service_.wakeup_cnt_);

  service_.wakeup_cnt_ = 0;
  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, tablet_id, reorg_scn, fork_info));
  ASSERT_EQ(1, service_.finish_refresh_set_.size());
  ASSERT_EQ(0, service_.wakeup_cnt_);

  service_.erase_finish_meta_refresh_task_(key);
  ASSERT_TRUE(service_.finish_refresh_set_.empty());
  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, tablet_id, reorg_scn, fork_info));
  ASSERT_EQ(1, service_.finish_refresh_set_.size());
  ASSERT_EQ(1, service_.wakeup_cnt_);
}

TEST_F(TestForkTableService, finish_refresh_submission_preserves_fork_identity)
{
  const ObLSID ls_id(1001);
  const ObTabletID tablet_id(200001);
  SCN reorg_scn;
  ASSERT_EQ(OB_SUCCESS, reorg_scn.convert_for_tx(1000));
  ObForkTabletInfo fork_info;
  fork_info.set_fork_snapshot_version(1000);
  fork_info.set_src_tablet_id(ObTabletID(200002));
  fork_info.set_data_complete(true);

  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, tablet_id, reorg_scn, fork_info));
  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ObLSID(1002), tablet_id, reorg_scn, fork_info));
  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, ObTabletID(200003), reorg_scn, fork_info));
  ASSERT_EQ(OB_SUCCESS, reorg_scn.convert_for_tx(2000));
  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, tablet_id, reorg_scn, fork_info));
  ASSERT_EQ(OB_SUCCESS, reorg_scn.convert_for_tx(1000));
  fork_info.set_src_tablet_id(ObTabletID(200004));
  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, tablet_id, reorg_scn, fork_info));
  fork_info.set_src_tablet_id(ObTabletID(200002));
  fork_info.set_fork_snapshot_version(2000);
  ASSERT_EQ(OB_SUCCESS, service_.submit_finish_meta_refresh(ls_id, tablet_id, reorg_scn, fork_info));
  ASSERT_EQ(6, service_.finish_refresh_set_.size());
  ASSERT_EQ(6, service_.wakeup_cnt_);
}

TEST_F(TestForkTableService, report_group_key)
{
  const ObForkTableService::ObForkReportGroupKey key(ObLSID(1001), 2001);
  ASSERT_TRUE(key.is_valid());
  ASSERT_TRUE(key == ObForkTableService::ObForkReportGroupKey(ObLSID(1001), 2001));
  ASSERT_FALSE(key == ObForkTableService::ObForkReportGroupKey(ObLSID(1002), 2001));
  ASSERT_FALSE(key == ObForkTableService::ObForkReportGroupKey(ObLSID(1001), 2002));
}

TEST_F(TestForkTableService, reported_group_set_lifecycle)
{
  const ObForkTableService::ObForkReportGroupKey key(ObLSID(1001), 2001);
  const ObForkTableService::ObForkReportGroupKey other_ls(ObLSID(1002), 2001);
  const ObForkTableService::ObForkReportGroupKey other_task(ObLSID(1001), 2002);
  ASSERT_FALSE(service_.is_reported_group_(key));

  service_.mark_reported_group_(key);
  ASSERT_TRUE(service_.is_reported_group_(key));
  ASSERT_FALSE(service_.is_reported_group_(other_ls));
  ASSERT_FALSE(service_.is_reported_group_(other_task));

  service_.erase_reported_group_(key);
  ASSERT_FALSE(service_.is_reported_group_(key));
}

TEST_F(TestForkTableService, erase_reported_groups_for_ls)
{
  const ObLSID ls_id(1001);
  const ObForkTableService::ObForkReportGroupKey first_group(ls_id, 2001);
  const ObForkTableService::ObForkReportGroupKey second_group(ls_id, 2002);
  const ObForkTableService::ObForkReportGroupKey other_ls_group(ObLSID(1002), 2001);
  service_.mark_reported_group_(first_group);
  service_.mark_reported_group_(second_group);
  service_.mark_reported_group_(other_ls_group);

  ASSERT_EQ(OB_INVALID_ARGUMENT, service_.erase_reported_groups_for_ls(ObLSID()));
  ASSERT_EQ(OB_SUCCESS, service_.erase_reported_groups_for_ls(ls_id));
  ASSERT_FALSE(service_.is_reported_group_(first_group));
  ASSERT_FALSE(service_.is_reported_group_(second_group));
  ASSERT_TRUE(service_.is_reported_group_(other_ls_group));
  ASSERT_EQ(OB_SUCCESS, service_.erase_reported_groups_for_ls(ls_id));
}

TEST_F(TestForkTableService, reported_group_cache_branches)
{
  const ObForkTableService::ObForkReportGroupKey key(ObLSID(1001), 2001);
  ObForkTableService::ObForkTabletItem item1;
  ObForkTableService::ObForkTabletItem item2;
  ObForkTableService::ObForkTabletSchedState sched_state;
  ObSEArray<ObForkTableService::ObForkTabletItem, 2> items;
  ObSEArray<int64_t, 2> item_idx;
  item1.tablet_id_ = ObTabletID(200001);
  item2.tablet_id_ = ObTabletID(200002);
  ASSERT_EQ(OB_SUCCESS, items.push_back(item1));
  ASSERT_EQ(OB_SUCCESS, items.push_back(item2));
  ASSERT_EQ(OB_SUCCESS, item_idx.push_back(0));
  ASSERT_EQ(OB_SUCCESS, item_idx.push_back(1));

  ASSERT_FALSE(service_.skip_group_by_report_cache_(key, items, item_idx));
  service_.mark_reported_group_(key);
  ASSERT_TRUE(service_.skip_group_by_report_cache_(key, items, item_idx));
  ASSERT_TRUE(service_.is_reported_group_(key));

  items.at(0).committed_inactive_ = true;
  ASSERT_EQ(OB_SUCCESS, service_.sched_state_map_.set_refactored(item1.tablet_id_, sched_state));
  ASSERT_TRUE(service_.skip_group_by_report_cache_(key, items, item_idx));
  ASSERT_TRUE(service_.is_reported_group_(key));
  ASSERT_EQ(OB_HASH_NOT_EXIST, service_.sched_state_map_.get_refactored(item1.tablet_id_, sched_state));

  items.at(1).committed_inactive_ = true;
  ASSERT_TRUE(service_.skip_group_by_report_cache_(key, items, item_idx));
  ASSERT_FALSE(service_.is_reported_group_(key));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
