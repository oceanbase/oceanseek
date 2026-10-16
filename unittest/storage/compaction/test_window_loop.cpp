/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gmock/gmock.h>
#include <atomic>
#include <memory>
#include <thread>

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/compaction/ob_window_loop.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "rootserver/freeze/window/ob_window_compaction_helper.h"
#include "storage/compaction_v2/ob_ss_window_loop.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace storage;
using namespace compaction;

namespace unittest
{
class TestBasicMergeScheduler : public ObBasicMergeScheduler
{
public:
  virtual int schedule_merge(const int64_t broadcast_version) override
  {
    UNUSED(broadcast_version);
    return OB_SUCCESS;
  }
};

class TestWindowLoop : public ::testing::Test
{
public:
  TestWindowLoop();
  virtual ~TestWindowLoop() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static ObTabletCompactionScoreDecisionInfo get_valid_decision_info(const int64_t cg_merge_batch_cnt = 3);
private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObTenantTabletStatMgr *stat_mgr_;
  ObTimerService *timer_service_;
};

TestWindowLoop::TestWindowLoop()
  : tenant_id_(1),
    tenant_base_(tenant_id_),
    stat_mgr_(nullptr),
    timer_service_(nullptr)
{
}

void TestWindowLoop::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestWindowLoop::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestWindowLoop::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;

  timer_service_ = OB_NEW(ObTimerService, ObModIds::TEST, tenant_id_);
  ASSERT_NE(nullptr, timer_service_);
  ASSERT_EQ(OB_SUCCESS, timer_service_->start());
  tenant_base_.set(timer_service_);
  stat_mgr_ = OB_NEW(ObTenantTabletStatMgr, ObModIds::TEST);
  ret = stat_mgr_->init(tenant_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  tenant_base_.set(stat_mgr_);

  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  ASSERT_EQ(tenant_id_, MTL_ID());
  ASSERT_EQ(stat_mgr_, MTL(ObTenantTabletStatMgr *));
  ASSERT_EQ(timer_service_, MTL(ObTimerService *));
}

void TestWindowLoop::TearDown()
{
  stat_mgr_->destroy();
  if (nullptr != timer_service_) {
    timer_service_->stop();
    timer_service_->wait();
    timer_service_->destroy();
    OB_DELETE(ObTimerService, ObModIds::TEST, timer_service_);
  }
  ObTenantEnv::set_tenant(nullptr);
}

ObTabletCompactionScoreDecisionInfo TestWindowLoop::get_valid_decision_info(const int64_t cg_merge_batch_cnt)
{
  ObTabletCompactionScoreDecisionInfo decision_info;
  decision_info.dynamic_info_.need_recycle_mds_ = false;
  decision_info.dynamic_info_.cg_merge_batch_cnt_ = cg_merge_batch_cnt;
  decision_info.base_inc_row_cnt_ = 1;
  decision_info.tablet_snapshot_version_ = 1;
  decision_info.major_snapshot_version_ = 1;
  return decision_info;
}

TEST_F(TestWindowLoop, test_window_compaction_params)
{
  rootserver::ObWindowParameters params;

  bool need_do_window_compaction = false;
  // params without window durations, not valid
  ASSERT_EQ(OB_NOT_INIT, params.check_need_do_window_compaction(need_do_window_compaction));
  params.tenant_id_ = 1002;
  params.is_inited_ = true;
  params.merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_TENANT;
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_IDLE;
  ASSERT_TRUE(params.is_valid());

  // 1. enable_window_compaction_ is false, do not need do window compaction
  params.enable_window_compaction_ = false;
  ASSERT_TRUE(params.is_window_compaction_stopped());
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);

  // 2. enable_window_compaction_ is true, but window_duration_us_ is 0, do not need do window compaction
  params.enable_window_compaction_ = true;
  params.window_duration_us_ = 0;
  ASSERT_TRUE(params.is_window_compaction_stopped());
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);

  // Both enable_window_compaction_ and window_duration_us_ are set
  params.window_duration_us_ = 5 * 1000 * 1000L; // 5s
  ASSERT_FALSE(params.is_window_compaction_stopped());
  // 3 merge_status_ is idle
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_IDLE;
  ASSERT_FALSE(params.is_window_compaction_forever());
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);
  params.window_duration_us_ = rootserver::ObWindowParameters::FULL_DAY_US;
  ASSERT_TRUE(params.is_window_compaction_forever());
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_TRUE(need_do_window_compaction);

  // 4. merge_status_ is not idle
  params.merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_TENANT;
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_MERGING;
  need_do_window_compaction = false;
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_VERIFYING;
  need_do_window_compaction = false;
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);

  params.merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_WINDOW;
  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_VERIFYING; // window compaction
  need_do_window_compaction = false;
  ASSERT_EQ(OB_ERR_UNEXPECTED, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);

  params.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_MERGING;
  const int64_t current_time_us = ObTimeUtility::current_time_us();
  params.window_start_time_us_ = current_time_us;
  params.window_duration_us_ = 1 * 1000 * 1000L;
  need_do_window_compaction = false;
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_TRUE(need_do_window_compaction);
  sleep(2);
  ASSERT_EQ(OB_SUCCESS, params.check_need_do_window_compaction(need_do_window_compaction));
  ASSERT_FALSE(need_do_window_compaction);
}

TEST(TestMergeInfo, test_update_merge_info)
{
  TestBasicMergeScheduler scheduler;
  const int64_t merge_start_time = ObTimeUtility::current_time();
  const int64_t global_broadcast_scn = 100;

  ASSERT_EQ(OB_SUCCESS, scheduler.update_merge_info(
      ObGlobalMergeInfo::MERGE_MODE_WINDOW, ObZoneMergeInfo::MERGE_STATUS_MERGING,
      merge_start_time, global_broadcast_scn));
  ASSERT_TRUE(scheduler.is_global_during_window_compaction());

  ASSERT_EQ(OB_INVALID_ARGUMENT, scheduler.update_merge_info(
      ObGlobalMergeInfo::MERGE_MODE_WINDOW, ObZoneMergeInfo::MERGE_STATUS_MERGING, merge_start_time, -1));
  ASSERT_TRUE(scheduler.is_global_during_window_compaction());
  ASSERT_EQ(OB_SUCCESS, scheduler.update_merge_info(
      ObGlobalMergeInfo::MERGE_MODE_TENANT, ObZoneMergeInfo::MERGE_STATUS_MERGING,
      merge_start_time, global_broadcast_scn + 1));
  ASSERT_FALSE(scheduler.is_global_during_window_compaction());
}

TEST(TestMergeInfo, test_merge_info_snapshot)
{
  TestBasicMergeScheduler scheduler;
  ASSERT_EQ(OB_SUCCESS, scheduler.update_merge_info(
      ObGlobalMergeInfo::MERGE_MODE_WINDOW, ObZoneMergeInfo::MERGE_STATUS_MERGING, 10, 100));
  ObMergeInfo snapshot;
  int64_t frozen_version = 0;
  {
    obsys::ObRLockGuard<> guard(scheduler.frozen_version_lock_);
    snapshot = scheduler.merge_info_;
    frozen_version = scheduler.frozen_version_;
  }
  EXPECT_EQ(scheduler.get_frozen_version(), frozen_version);

  // 模拟取得快照后刷新状态；本轮仍须使用原窗口的开始时间和目标。
  ASSERT_EQ(OB_SUCCESS, scheduler.update_merge_info(
      ObGlobalMergeInfo::MERGE_MODE_TENANT, ObZoneMergeInfo::MERGE_STATUS_IDLE, 20, 200));
  ASSERT_TRUE(snapshot.is_global_during_window_compaction());
  ObSSWindowLoop window_loop;
  window_loop.update_window_schedule_state_(snapshot);
  EXPECT_EQ(10, window_loop.window_start_time_us_);
  EXPECT_EQ(100, window_loop.target_scn_);

  std::atomic<bool> start(false);
  std::atomic<bool> write_failed(false);
  std::thread writer([&]() {
    while (!start.load()) {
      std::this_thread::yield();
    }
    for (int64_t i = 0; i < 10000; ++i) {
      const bool window = (i % 2 == 0);
      if (OB_SUCCESS != scheduler.update_merge_info(
          window ? ObGlobalMergeInfo::MERGE_MODE_WINDOW : ObGlobalMergeInfo::MERGE_MODE_TENANT,
          window ? ObZoneMergeInfo::MERGE_STATUS_MERGING : ObZoneMergeInfo::MERGE_STATUS_IDLE,
          window ? 10 : 20, window ? 100 : 200)) {
        write_failed.store(true);
      }
    }
  });
  start.store(true);
  bool mixed_snapshot = false;
  for (int64_t i = 0; i < 10000; ++i) {
    {
      obsys::ObRLockGuard<> guard(scheduler.frozen_version_lock_);
      snapshot = scheduler.merge_info_;
      frozen_version = scheduler.frozen_version_;
    }
    const bool window = snapshot.merge_mode_ == ObGlobalMergeInfo::MERGE_MODE_WINDOW;
    mixed_snapshot |= snapshot.merge_status_ != (window ? ObZoneMergeInfo::MERGE_STATUS_MERGING
                                                       : ObZoneMergeInfo::MERGE_STATUS_IDLE)
        || snapshot.merge_start_time_ != (window ? 10 : 20)
        || snapshot.global_broadcast_scn_ != (window ? 100 : 200);
  }
  writer.join();
  EXPECT_FALSE(write_failed.load());
  EXPECT_FALSE(mixed_snapshot);
}

TEST(TestMergeInfo, test_prepare_window_merge_invalid_target)
{
  TestBasicMergeScheduler scheduler;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      scheduler.prepare_window_merge(ObBasicMergeScheduler::INIT_COMPACTION_SCN));
}

TEST(TestSSWindowLoopState, test_target_lifecycle)
{
  const int64_t first_start_time = 10;
  const int64_t first_target_scn = 100;
  ObSSWindowLoop window_loop;
  ObMergeInfo merge_info;
  merge_info.merge_mode_ = share::ObGlobalMergeInfo::MERGE_MODE_WINDOW;
  merge_info.merge_status_ = share::ObZoneMergeInfo::MERGE_STATUS_MERGING;

  merge_info.merge_start_time_ = first_start_time;
  merge_info.global_broadcast_scn_ = first_target_scn;
  window_loop.update_window_schedule_state_(merge_info);
  ASSERT_EQ(first_start_time, window_loop.window_start_time_us_);
  ASSERT_EQ(first_target_scn, window_loop.target_scn_);

  window_loop.loop_cnt_ = 3;
  window_loop.ls_tablet_iter_.scan_finish_ = true;
  window_loop.ls_tablet_iter_.merge_finish_ = true;
  window_loop.ls_tablet_iter_.ls_idx_ = 2;
  window_loop.ls_tablet_iter_.schedule_tablet_cnt_ = 7;
  window_loop.ls_tablet_iter_.tablet_ids_.tablet_idx_ = 5;
  window_loop.ls_tablet_iter_.tablet_ids_.is_inited_ = true;
  window_loop.ls_tablet_iter_.report_scn_flag_ = true;
  merge_info.merge_start_time_ = first_start_time + 1;
  window_loop.update_window_schedule_state_(merge_info);
  ASSERT_EQ(3, window_loop.loop_cnt_);
  ASSERT_EQ(first_start_time + 1, window_loop.window_start_time_us_);
  ASSERT_TRUE(window_loop.ls_tablet_iter_.scan_finish_);
  ASSERT_TRUE(window_loop.ls_tablet_iter_.merge_finish_);
  ASSERT_EQ(2, window_loop.ls_tablet_iter_.ls_idx_);
  ASSERT_EQ(7, window_loop.ls_tablet_iter_.schedule_tablet_cnt_);
  ASSERT_EQ(5, window_loop.ls_tablet_iter_.tablet_ids_.tablet_idx_);
  ASSERT_TRUE(window_loop.ls_tablet_iter_.tablet_ids_.is_inited_);
  ASSERT_TRUE(window_loop.ls_tablet_iter_.report_scn_flag_);

  merge_info.merge_start_time_ = first_start_time + 2;
  merge_info.global_broadcast_scn_ = first_target_scn + 1;
  window_loop.update_window_schedule_state_(merge_info);
  ASSERT_EQ(0, window_loop.loop_cnt_);
  ASSERT_EQ(first_target_scn + 1, window_loop.target_scn_);
  ASSERT_FALSE(window_loop.ls_tablet_iter_.scan_finish_);
  ASSERT_FALSE(window_loop.ls_tablet_iter_.merge_finish_);
  ASSERT_EQ(0, window_loop.ls_tablet_iter_.ls_idx_);
  ASSERT_EQ(0, window_loop.ls_tablet_iter_.schedule_tablet_cnt_);
  ASSERT_EQ(0, window_loop.ls_tablet_iter_.tablet_ids_.tablet_idx_);
  ASSERT_FALSE(window_loop.ls_tablet_iter_.tablet_ids_.is_inited_);
  ASSERT_FALSE(window_loop.ls_tablet_iter_.report_scn_flag_);

  merge_info.merge_start_time_ = first_start_time + 3;
  merge_info.global_broadcast_scn_ = first_target_scn + 2;
  ASSERT_EQ(OB_SUCCESS, window_loop.try_schedule_window_compaction(merge_info, first_target_scn + 1));
  ASSERT_EQ(first_target_scn + 1, window_loop.target_scn_);

  merge_info.reset();
  ASSERT_EQ(OB_SUCCESS, window_loop.try_schedule_window_compaction(merge_info, first_target_scn + 1));
  ASSERT_EQ(first_start_time + 2, window_loop.window_start_time_us_);
  ASSERT_EQ(first_target_scn + 1, window_loop.target_scn_);

  window_loop.reset();
  ASSERT_EQ(0, window_loop.window_start_time_us_);
  ASSERT_EQ(0, window_loop.loop_cnt_);
  ASSERT_EQ(static_cast<int64_t>(ObBasicMergeScheduler::INIT_COMPACTION_SCN), window_loop.target_scn_);
}

TEST(TestSSWindowLoopState, test_schedule_boundary)
{
  ObSSWindowLoop window_loop;

  ASSERT_FALSE(window_loop.can_continue_schedule_(0, INT64_MAX));
  ASSERT_FALSE(window_loop.can_continue_schedule_(-1, INT64_MAX));
  ASSERT_FALSE(window_loop.can_continue_schedule_(1, 0));
  ASSERT_TRUE(window_loop.can_continue_schedule_(1, INT64_MAX));
}

TEST(TestSSWindowLoopState, test_skip_schedule_preserves_cursor_and_releases_ls)
{
  std::unique_ptr<ObLS> ls(new ObLS());
  ObLSMap ls_map;
  // 测试自身保留一个引用，避免迭代器释放引用时销毁测试创建的 LS。
  ASSERT_EQ(OB_SUCCESS, ls->get_ref_mgr().inc(ObLSGetMod::STORAGE_MOD));
  DEFER(ls->get_ref_mgr().dec(ObLSGetMod::STORAGE_MOD));
  ObTenantTabletScheduler scheduler;
  ObTenantBase tenant_context(1);
  ObTenantBase *original_tenant = ObTenantEnv::get_tenant();
  tenant_context.set(&scheduler);
  ObTenantEnv::set_tenant(&tenant_context);
  DEFER(ObTenantEnv::set_tenant(original_tenant));
  ASSERT_FALSE(scheduler.get_tenant_status().enable_window_compaction());

  // 配置关闭、TENANT 模式、窗口已结束及本地冻结点未就绪时，均释放引用并保留游标。
  for (int64_t scenario = 0; scenario < 4; ++scenario) {
    ObSSWindowLoop window_loop;
    ObMergeInfo merge_info;
    merge_info.merge_mode_ = scenario == 1
        ? ObGlobalMergeInfo::MERGE_MODE_TENANT : ObGlobalMergeInfo::MERGE_MODE_WINDOW;
    merge_info.merge_status_ = scenario == 2
        ? ObZoneMergeInfo::MERGE_STATUS_IDLE : ObZoneMergeInfo::MERGE_STATUS_MERGING;
    merge_info.global_broadcast_scn_ = 100;
    window_loop.target_scn_ = 100;
    auto &iter = window_loop.ls_tablet_iter_;
    ASSERT_EQ(OB_SUCCESS, iter.ls_ids_.push_back(ObLSID(1001)));
    iter.ls_idx_ = 0;
    ASSERT_EQ(OB_SUCCESS, iter.tablet_ids_.array_.push_back(ObTabletID(2001)));
    ASSERT_EQ(OB_SUCCESS, iter.tablet_ids_.array_.push_back(ObTabletID(2002)));
    iter.tablet_ids_.mark_inited();
    iter.tablet_ids_.tablet_idx_ = 1;
    ASSERT_EQ(OB_SUCCESS, iter.cur_ls_handle_.set_ls(ls_map, *ls, ObLSGetMod::COMPACT_MODE));
    ASSERT_EQ(2, ls->get_ref_mgr().get_total_ref_cnt());
    ASSERT_EQ(OB_SUCCESS, window_loop.try_schedule_window_compaction(
        merge_info, scenario == 3 ? 99 : 100));
    ASSERT_EQ(1, ls->get_ref_mgr().get_total_ref_cnt());
    ASSERT_FALSE(iter.cur_ls_handle_.is_valid());
    ASSERT_FALSE(iter.is_valid());
    ASSERT_EQ(1, iter.ls_ids_.count());
    ASSERT_EQ(ObLSID(1001), iter.ls_ids_.at(0));
    ASSERT_EQ(0, iter.ls_idx_);
    ASSERT_TRUE(iter.tablet_ids_.is_inited_);
    ASSERT_EQ(2, iter.tablet_ids_.count());
    ASSERT_EQ(1, iter.tablet_ids_.tablet_idx_);
    // 同一 target 下保留扫描位置；新 target 的重置由 test_target_lifecycle 覆盖。
    ASSERT_EQ(100, window_loop.target_scn_);
  }
}

TEST_F(TestWindowLoop, test_basic_init)
{
  ObWindowLoop window_loop;
  const int64_t merge_start_time = ObTimeUtility::current_time();
  ASSERT_FALSE(window_loop.is_active());
  ASSERT_FALSE(window_loop.is_inited_);
  ASSERT_EQ(OB_SUCCESS, window_loop.stop_window_compaction());
  ASSERT_FALSE(window_loop.is_active());
  ASSERT_FALSE(window_loop.is_inited_);

  ASSERT_EQ(OB_SUCCESS, window_loop.start_window_compaction(merge_start_time));
  ASSERT_TRUE(window_loop.is_active());
  ASSERT_TRUE(window_loop.is_inited_);
  ASSERT_EQ(OB_SUCCESS, window_loop.start_window_compaction(merge_start_time));
}

TEST_F(TestWindowLoop, test_window_loop_memory_size)
{
  ObWindowLoop window_loop;
  ObWindowCompactionMemoryContext &mem_ctx = window_loop.mem_ctx_;
  ObIAllocator &allocator = mem_ctx.allocator_;
  ObWindowCompactionPriorityQueue &priority_queue = window_loop.score_prio_queue_;
  LOG_INFO("start test_window_loop_memory_size");
  const int64_t merge_start_time = ObTimeUtility::current_time();
  const int64_t tablet_cnts[5] = {100, 1000, 10000, 10 * 10000, 100 * 10000};
  for (int64_t i = 0; i < 5; i++) {
    const int64_t tablet_cnt = tablet_cnts[i];
    LOG_INFO("start test_window_controller_memory_size", K(tablet_cnt));
    ASSERT_EQ(OB_SUCCESS, window_loop.start_window_compaction(merge_start_time));
    for (int64_t i = 0; i < tablet_cnt; i++) {
      ObTabletCompactionScore *score = nullptr;
      ASSERT_EQ(OB_SUCCESS, mem_ctx.alloc_score(score));
      ASSERT_NE(nullptr, score);
      score->key_.ls_id_ = 1001;
      score->key_.tablet_id_ = 200001 + i;
      score->score_ = rand() % 1000000 + 1;
      score->decision_info_ = get_valid_decision_info();
      ASSERT_EQ(OB_SUCCESS, window_loop.score_prio_queue_.push(score));
      if (i % 100 == 0) {
        LOG_INFO("finish push scores", K(tablet_cnt), K(i));
      }
    }
    LOG_INFO("finish push all scores", K(i), K(tablet_cnt), "total", allocator.total(), "used", allocator.used());
    window_loop.stop_window_compaction();
    ASSERT_EQ(0, allocator.total());
    ASSERT_EQ(0, allocator.used()); // check if all scores are destroyed
    LOG_INFO("finish stop window compaction", K(i), K(tablet_cnt), "total", allocator.total(), "used", allocator.used());
  }
  LOG_INFO("finish test_window_loop_memory_size");
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_window_loop.log*");
  OB_LOGGER.set_file_name("test_window_loop.log", true);
  OB_LOGGER.set_log_level("TRACE");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
