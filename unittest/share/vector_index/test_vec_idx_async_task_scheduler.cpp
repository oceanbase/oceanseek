/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/vector_index/ob_vec_idx_async_task_scheduler.h"
#undef private
#undef protected

namespace oceanbase
{
namespace share
{

class TestVecIdxAsyncTaskScheduler : public ::testing::Test
{
public:
  void SetUp() override
  {
    scheduler_.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, scheduler_.ls_leader_executor_map_.create(
        17, common::ObMemAttr(OB_SERVER_TENANT_ID, "VecSchedUTL")));
    ASSERT_EQ(OB_SUCCESS, scheduler_.ls_follower_executor_map_.create(
        17, common::ObMemAttr(OB_SERVER_TENANT_ID, "VecSchedUTF")));
  }

  void TearDown() override
  {
    scheduler_.ls_leader_executor_map_.destroy();
    scheduler_.ls_follower_executor_map_.destroy();
    scheduler_.is_inited_ = false;
  }

protected:
  ObVecIdxAsyncTaskScheduler scheduler_;
};

TEST_F(TestVecIdxAsyncTaskScheduler, stopped_aux_maintenance_skips_tenant_services)
{
  // No schema service or tenant LS service is available in this fixture. After
  // stop(), neither collection phase may enter those services or start a round.
  common::ObArray<common::ObTabletID> tablet_ids;
  common::ObArray<ObVecIdxAsyncTaskScheduler::ObAuxTabletCandidate> freeze_candidates;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(common::ObTabletID(200001)));
  scheduler_.stop();

  ASSERT_EQ(OB_SUCCESS, scheduler_.collect_aux_tablet_ids_(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(OB_SUCCESS, scheduler_.collect_aux_tablet_candidates_(
      tablet_ids, freeze_candidates));
  ASSERT_TRUE(freeze_candidates.empty());
  ASSERT_EQ(OB_SUCCESS, scheduler_.do_aux_storage_maintenance_());
  ASSERT_EQ(0, scheduler_.aux_maintenance_round_seq_);
}

TEST_F(TestVecIdxAsyncTaskScheduler, sys_ls_skips_periodic_ivf_rewarm)
{
  ObVecIdxLeaderExecutors exec;
  exec.set_need_resume(true);
  uint64_t trace_id = 0;
  const ObLSID sys_ls(ObLSID::SYS_LS_ID);

  // The IVF executor is deliberately uninitialized, matching SYS LS production setup.
  // The periodic loader must still succeed and consume the existing recovery marker.
  ASSERT_EQ(OB_SUCCESS,
            scheduler_.load_leader_ls_tasks(sys_ls, exec, false, nullptr, trace_id));
  ASSERT_FALSE(exec.get_need_resume());
  ASSERT_EQ(0, exec.ivf_cache_rewarm_exec_.requested_epoch_);

  // A non-empty parent list must still enter the SYS LS alter-param path. Its executor is
  // deliberately uninitialized, so OB_NOT_INIT is observable only if that path was reached.
  common::ObSEArray<ObVecAlterParamParentInfo, 1> alter_param_parents;
  ASSERT_EQ(OB_SUCCESS, alter_param_parents.push_back(ObVecAlterParamParentInfo()));
  ASSERT_EQ(OB_NOT_INIT,
            scheduler_.load_leader_ls_tasks(
                sys_ls, exec, false, &alter_param_parents, trace_id));
}

TEST_F(TestVecIdxAsyncTaskScheduler, periodic_ivf_rewarm_preserved_for_data_ls)
{
  ObVecIdxLeaderExecutors sys_exec;
  ObVecIdxLeaderExecutors data_exec;
  uint64_t trace_id = 0;

  // The focused seam is also used by load_leader_ls_tasks. With uninitialized executors,
  // success for SYS LS and OB_NOT_INIT for data LS prove that only SYS LS is filtered.
  ASSERT_EQ(OB_SUCCESS, scheduler_.load_ivf_cache_rewarm_tasks_(
      ObLSID(ObLSID::SYS_LS_ID), sys_exec, true, trace_id));
  ASSERT_EQ(OB_NOT_INIT, scheduler_.load_ivf_cache_rewarm_tasks_(
      ObLSID(1001), data_exec, true, trace_id));
}

TEST_F(TestVecIdxAsyncTaskScheduler, new_executor_compensation_filters_only_sys_ls)
{
  const ObLSID sys_ls(ObLSID::SYS_LS_ID);
  const ObLSID data_ls(1001);
  ObVecIdxLeaderExecutors sys_leader;
  ObVecIdxLeaderExecutors data_leader;
  ObVecIdxFollowerExecutors sys_follower;
  ObVecIdxFollowerExecutors data_follower;
  // Initialize both SYS and data executors as request sentinels. If the SYS LS guard
  // regresses, request_* will increment its epoch instead of merely returning OB_NOT_INIT.
  sys_leader.ivf_cache_rewarm_exec_.is_inited_ = true;
  sys_follower.ivf_cache_cleanup_exec_.is_inited_ = true;
  data_leader.ivf_cache_rewarm_exec_.is_inited_ = true;
  data_follower.ivf_cache_cleanup_exec_.is_inited_ = true;

  scheduler_.compensate_new_ls_executor_(sys_ls, true, sys_leader, sys_follower);
  scheduler_.compensate_new_ls_executor_(sys_ls, false, sys_leader, sys_follower);
  scheduler_.compensate_new_ls_executor_(data_ls, true, data_leader, data_follower);
  scheduler_.compensate_new_ls_executor_(data_ls, false, data_leader, data_follower);

  ASSERT_TRUE(sys_leader.get_need_resume());
  ASSERT_TRUE(sys_follower.get_need_resume());
  ASSERT_EQ(0, sys_leader.ivf_cache_rewarm_exec_.requested_epoch_);
  ASSERT_EQ(0, sys_follower.ivf_cache_cleanup_exec_.requested_epoch_);
  ASSERT_TRUE(data_leader.get_need_resume());
  ASSERT_TRUE(data_follower.get_need_resume());
  ASSERT_EQ(1, data_leader.ivf_cache_rewarm_exec_.requested_epoch_);
  ASSERT_EQ(1, data_follower.ivf_cache_cleanup_exec_.requested_epoch_);
}

TEST_F(TestVecIdxAsyncTaskScheduler, request_entries_filter_only_sys_ls)
{
  const ObLSID sys_ls(ObLSID::SYS_LS_ID);
  const ObLSID data_ls(1001);
  ObVecIdxLeaderExecutors sys_leader;
  ObVecIdxLeaderExecutors data_leader;
  ObVecIdxFollowerExecutors sys_follower;
  ObVecIdxFollowerExecutors data_follower;

  // request_rewarm/request_cleanup only need the base initialization marker; no task is run.
  // SYS executors are initialized as sentinels so a missing guard observably increments epoch.
  sys_leader.ivf_cache_rewarm_exec_.is_inited_ = true;
  sys_follower.ivf_cache_cleanup_exec_.is_inited_ = true;
  data_leader.ivf_cache_rewarm_exec_.is_inited_ = true;
  data_follower.ivf_cache_cleanup_exec_.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, scheduler_.ls_leader_executor_map_.set_refactored(sys_ls, &sys_leader));
  ASSERT_EQ(OB_SUCCESS, scheduler_.ls_leader_executor_map_.set_refactored(data_ls, &data_leader));
  ASSERT_EQ(OB_SUCCESS, scheduler_.ls_follower_executor_map_.set_refactored(sys_ls, &sys_follower));
  ASSERT_EQ(OB_SUCCESS, scheduler_.ls_follower_executor_map_.set_refactored(data_ls, &data_follower));

  scheduler_.mark_ls_ivf_rewarm_requested(sys_ls);
  scheduler_.mark_ls_ivf_rewarm_requested(data_ls);
  scheduler_.mark_ls_need_resume_for_follower(sys_ls);
  scheduler_.mark_ls_need_resume_for_follower(data_ls);

  ASSERT_EQ(0, sys_leader.ivf_cache_rewarm_exec_.requested_epoch_);
  ASSERT_EQ(1, data_leader.ivf_cache_rewarm_exec_.requested_epoch_);
  ASSERT_EQ(0, sys_follower.ivf_cache_cleanup_exec_.requested_epoch_);
  ASSERT_EQ(1, data_follower.ivf_cache_cleanup_exec_.requested_epoch_);
  // Recovery markers remain independent of the IVF request filter.
  ASSERT_TRUE(sys_follower.get_need_resume());
  ASSERT_TRUE(data_follower.get_need_resume());
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
