/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include <gtest/gtest.h>
#define protected public
#define private public
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/compaction/ob_compaction_resource_manager.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction_v2/ob_ss_schedule_tablet_func.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

namespace storage
{
static bool mock_is_sswriter = true;
static int64_t mock_lease_epoch = 1;

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  UNUSED(key);
  is_sswriter = mock_is_sswriter;
  epoch = mock_lease_epoch;
  return OB_SUCCESS;
}

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    const int64_t timeout_us)
{
  UNUSED(key);
  UNUSED(timeout_us);
  sswriter_addr.addr_ = GCTX.self_addr();
  return OB_SUCCESS;
}
} // namespace storage
} // namespace oceanbase

const char *get_per_file_test_name()
{
  return "test_ss_window_compaction_observer_";
}

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace share;
using namespace storage;
namespace unittest
{

class WriterQuotaPressureTask final : public ObITask
{
public:
  WriterQuotaPressureTask() : ObITask(ObITask::TASK_TYPE_UT) {}
private:
  int process() override
  {
    ob_usleep(2L * 1000L * 1000L);
    return OB_SUCCESS;
  }
};

class WriterQuotaPressureDag final : public ObIDag
{
public:
  WriterQuotaPressureDag()
    : ObIDag(ObDagType::DAG_TYPE_VERIFY_CKM), test_id_(0)
  {}

  int init(const int64_t test_id)
  {
    test_id_ = test_id;
    return OB_SUCCESS;
  }
  uint64_t hash() const override { return murmurhash(&test_id_, sizeof(test_id_), 0); }
  bool operator ==(const ObIDag &other) const override
  {
    return get_type() == other.get_type() && hash() == other.hash();
  }
  int fill_info_param(
      ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override
  {
    return ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), test_id_, 1L);
  }
  int fill_dag_key(char *buf, const int64_t buf_len) const override
  {
    return databuff_printf(buf, buf_len, "writer_quota_pressure_%ld", test_id_);
  }
  lib::Worker::CompatMode get_compat_mode() const override
  {
    return lib::Worker::CompatMode::MYSQL;
  }
  uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
private:
  int64_t test_id_;
};

struct WriterQuotaState
{
  WriterQuotaState()
    : compaction_low_adaptive_limit_(0), low_priority_dag_count_(0), writer_quota_(0)
  {}
  int64_t compaction_low_adaptive_limit_;
  int64_t low_priority_dag_count_;
  int64_t writer_quota_;
};

static int get_stable_writer_quota_state(
    ObTenantDagScheduler &dag_scheduler,
    const ObSSWindowLoop &window_loop,
    WriterQuotaState &state)
{
  int ret = OB_SUCCESS;
  bool got_stable_state = false;
  for (int64_t retry = 0; OB_SUCC(ret) && !got_stable_state && retry < 100; ++retry) {
    int64_t compaction_low_adaptive_limit_after = 0;
    int64_t dag_count_after = 0;
    if (OB_FAIL(dag_scheduler.get_adaptive_limit(
        ObDagPrio::DAG_PRIO_COMPACTION_LOW, state.compaction_low_adaptive_limit_))) {
    } else if (OB_FAIL(dag_scheduler.get_priority_dag_count(
        ObDagPrio::DAG_PRIO_COMPACTION_LOW, state.low_priority_dag_count_))) {
    } else if (OB_FAIL(window_loop.calc_writer_schedule_quota_(state.writer_quota_))) {
    } else if (OB_FAIL(dag_scheduler.get_adaptive_limit(
        ObDagPrio::DAG_PRIO_COMPACTION_LOW, compaction_low_adaptive_limit_after))) {
    } else if (OB_FAIL(dag_scheduler.get_priority_dag_count(
        ObDagPrio::DAG_PRIO_COMPACTION_LOW, dag_count_after))) {
    } else if (state.compaction_low_adaptive_limit_ == compaction_low_adaptive_limit_after
        && state.low_priority_dag_count_ == dag_count_after) {
      got_stable_state = true;
    } else {
      ob_usleep(1000);
    }
  }
  if (OB_SUCC(ret) && !got_stable_state) {
    ret = OB_EAGAIN;
  }
  return ret;
}

static int add_writer_quota_pressure_dag(
    ObTenantDagScheduler &dag_scheduler,
    const int64_t test_id)
{
  int ret = OB_SUCCESS;
  WriterQuotaPressureDag *dag = nullptr;
  WriterQuotaPressureTask *task = nullptr;
  bool scheduler_owned = false;
  if (OB_FAIL(dag_scheduler.alloc_dag(dag))) {
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(dag->init(test_id))) {
  } else if (OB_FAIL(dag->alloc_task(task))) {
  } else if (OB_FAIL(dag->add_task(*task))) {
  } else if (OB_FAIL(dag_scheduler.add_dag(dag))) {
    scheduler_owned = OB_NOT_NULL(dag->get_prev()) || OB_NOT_NULL(dag->get_next());
  } else {
    scheduler_owned = true;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag) && !scheduler_owned) {
    dag_scheduler.free_dag(*dag);
  }
  return ret;
}

static bool wait_for_dag_count(
    ObTenantDagScheduler &dag_scheduler,
    const ObDagType::ObDagTypeEnum dag_type,
    const int64_t expected_count)
{
  const int64_t wait_deadline = ObTimeUtility::current_time() + 10L * 1000L * 1000L;
  while (ObTimeUtility::current_time() < wait_deadline
      && expected_count != dag_scheduler.get_dag_count(dag_type)) {
    ob_usleep(10L * 1000L);
  }
  return expected_count == dag_scheduler.get_dag_count(dag_type);
}

class TestSSWindowCompactionObserver : public ObStorageCachePolicyPrewarmerTest
{
protected:
  void create_tablet(
      const char *table_name,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle)
  {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table %s (c1 bigint primary key, c2 bigint)", table_name));
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
    set_ls_and_tablet_id_for_run_ctx(table_name);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
        run_ctx_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls_handle.get_ls());
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(run_ctx_.tablet_id_, tablet_handle));
    ASSERT_TRUE(tablet_handle.is_valid());
  }

  int scan_tablets(
      ObCompactionScheduleIterator &iter,
      const ObTabletID &target,
      bool &found)
  {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    found = false;
    while (OB_SUCC(ret) && OB_SUCC(ret = iter.get_next_ls(ls_handle))) {
      while (OB_SUCC(ret = iter.get_next_tablet(tablet_handle))) {
        found |= tablet_handle.get_obj()->get_tablet_id() == target;
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    return ret;
  }
};

// SSWC-OBS-MIT-001：refresh 只准备窗口 target，不应额外启动 MediumLoop。
TEST_F(TestSSWindowCompactionObserver, prepare_window_merge_only_updates_local_target)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObTenantTabletScheduler scheduler;
  ObTenantCompactionProgressMgr progress_mgr;
  ASSERT_EQ(OB_SUCCESS, progress_mgr.init());
  DEFER(progress_mgr.destroy());
  ObTenantBase *original_tenant = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, original_tenant);
  ObTenantCompactionProgressMgr *shared_progress_mgr = MTL(ObTenantCompactionProgressMgr *);
  {
    // prepare_window_merge 会写入 MTL 进度；仅替换当前测试线程的依赖，避免结束共享租户的已有进度。
    ObTenantBase tenant_context(run_ctx_.tenant_id_);
    tenant_context = *original_tenant;
    tenant_context.set(&scheduler);
    tenant_context.set(&progress_mgr);
    ObTenantEnv::set_tenant(&tenant_context);
    DEFER(ObTenantEnv::set_tenant(original_tenant));
    ASSERT_EQ(&progress_mgr, MTL(ObTenantCompactionProgressMgr *));
    ASSERT_NE(shared_progress_mgr, MTL(ObTenantCompactionProgressMgr *));
    const int64_t medium_target_before = scheduler.medium_loop_.merge_version_;
    const int64_t window_target = medium_target_before + 100;

    ASSERT_EQ(OB_SUCCESS, scheduler.prepare_window_merge(window_target));
    EXPECT_EQ(window_target, scheduler.get_frozen_version());
    EXPECT_EQ(medium_target_before, scheduler.medium_loop_.merge_version_);
    int64_t progress_pos = -1;
    ASSERT_EQ(OB_SUCCESS, progress_mgr.get_pos_(window_target, progress_pos));
  }
  EXPECT_EQ(original_tenant, ObTenantEnv::get_tenant());
  EXPECT_EQ(shared_progress_mgr, MTL(ObTenantCompactionProgressMgr *));
}

// SSWC-OBS-MIT-002：同一 target 下，SS window 保留 WINDOW_COMPACTION，普通调度仍识别为 TENANT_MAJOR。
TEST_F(TestSSWindowCompactionObserver, ss_window_schedule_preserves_window_reason)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  create_tablet("t_ss_window_reason", ls_handle, tablet_handle);

  ObArenaAllocator allocator("SSWindowReason");
  const ObMediumCompactionInfoList *medium_info_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_info_list(allocator, medium_info_list));
  ASSERT_NE(nullptr, medium_info_list);
  int64_t max_sync_medium_scn = 0;
  ASSERT_EQ(OB_SUCCESS, ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
      *tablet_handle.get_obj(), *medium_info_list, max_sync_medium_scn));
  const int64_t target_scn = max_sync_medium_scn + 1;
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(ObTimeUtility::current_time()));

  ObMediumCompactionScheduleFunc window_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_info_list, nullptr,
      ObAdaptiveMergePolicy::WINDOW_COMPACTION);
  ASSERT_EQ(OB_SUCCESS, window_func.init_tablet_handle(tablet_handle));
  ASSERT_EQ(OB_SUCCESS, window_func.get_adaptive_reason(target_scn));
  EXPECT_EQ(ObAdaptiveMergePolicy::WINDOW_COMPACTION, window_func.merge_reason_);
  // 控制下游在检查 handle 时退出，仅验证目标调度入口不会覆盖窗口原因。
  window_func.tablet_handle_.reset();
  EXPECT_EQ(OB_ERR_UNEXPECTED, window_func.schedule_async_index_with_major_snapshot_(target_scn));
  EXPECT_EQ(ObAdaptiveMergePolicy::WINDOW_COMPACTION, window_func.merge_reason_);

  ObMediumCompactionScheduleFunc medium_func(
      *ls_handle.get_ls(), weak_read_scn, *medium_info_list, nullptr,
      ObAdaptiveMergePolicy::NONE);
  ASSERT_EQ(OB_SUCCESS, medium_func.init_tablet_handle(tablet_handle));
  ASSERT_EQ(OB_SUCCESS, medium_func.get_adaptive_reason(target_scn));
  EXPECT_EQ(ObAdaptiveMergePolicy::TENANT_MAJOR, medium_func.merge_reason_);
}

// SSWC-FALLBACK-001: exercise the real fallback, then route its output through both consumers.
TEST_F(TestSSWindowCompactionObserver, schema_fallback_preserves_window_routing)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  create_tablet("t_ss_window_fallback", ls_handle, tablet_handle);
  ASSERT_TRUE(tablet_handle.is_valid());
  const int64_t weak_read_ts = MAX(ObTimeUtility::current_time(),
      tablet_handle.get_obj()->get_last_major_snapshot_version() + 2);
  SCN weak_read_scn;
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(weak_read_ts));
  int64_t expected_schema_version = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_newest_schema_version(expected_schema_version));

  for (auto source : {ObAdaptiveMergePolicy::WINDOW_COMPACTION, ObAdaptiveMergePolicy::TENANT_MAJOR}) {
    SCOPED_TRACE(static_cast<int>(source));
    ObMediumCompactionInfoList medium_list;
    ObMediumCompactionScheduleFunc producer(
        *ls_handle.get_ls(), weak_read_scn, medium_list, nullptr, source);
    ASSERT_EQ(OB_SUCCESS, producer.init_tablet_handle(tablet_handle));
    ObMediumCompactionInfo info;
    int64_t schema_version = 0;
    // A weak read at the freeze version is not sufficient; retry must not emit a snapshot.
    EXPECT_EQ(OB_EAGAIN, producer.switch_to_choose_medium_snapshot(weak_read_ts, info, schema_version));
    EXPECT_EQ(0, info.medium_snapshot_);
    ASSERT_EQ(OB_SUCCESS, producer.switch_to_choose_medium_snapshot(weak_read_ts - 1, info, schema_version));
    EXPECT_TRUE(info.is_medium_compaction());
    EXPECT_EQ(weak_read_ts, info.medium_snapshot_);
    EXPECT_EQ(expected_schema_version, schema_version);
    const auto expected_reason = source == ObAdaptiveMergePolicy::WINDOW_COMPACTION
        ? ObAdaptiveMergePolicy::WINDOW_COMPACTION : ObAdaptiveMergePolicy::NONE;
    EXPECT_EQ(expected_reason, info.medium_merge_reason_);
    ASSERT_TRUE(medium_list.medium_info_list_.add_last(&info));
    DEFER(medium_list.medium_info_list_.remove(&info));

    ObSSScheduleTabletFunc normal_func(ObBasicMergeScheduler::INIT_COMPACTION_SCN);
    int64_t quota = 0;
    ObSSScheduleTabletFunc window_func(weak_read_ts, quota, 0);
    for (auto *func : {&normal_func, &window_func}) {
      func->ls_status_.ls_id_ = ls_handle.get_ls()->get_ls_id();
      func->tablet_status_.medium_list_ = &medium_list;
    }
    DEFER(normal_func.tablet_status_.medium_list_ = nullptr;
          window_func.tablet_status_.medium_list_ = nullptr);
    ObCOMajorMergeStrategy strategy;
    int64_t schedule_scn = 0;
    int64_t base_version = 0;
    ObMediumCompactionInfo::ObCompactionType type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;
    ObAdaptiveMergePolicy::AdaptiveMergeReason reason = ObAdaptiveMergePolicy::NONE;
    EXPECT_EQ(source == ObAdaptiveMergePolicy::WINDOW_COMPACTION ? OB_NO_NEED_MERGE : OB_SUCCESS,
        normal_func.get_schedule_execute_info(*tablet_handle.get_obj(),
            schedule_scn, base_version, strategy, reason, type));
    EXPECT_EQ(OB_SUCCESS, window_func.get_schedule_execute_info(*tablet_handle.get_obj(),
        schedule_scn, base_version, strategy, reason, type));
    EXPECT_EQ(expected_reason, reason);
    EXPECT_EQ(weak_read_ts, schedule_scn);
    EXPECT_EQ(OB_SIZE_OVERFLOW, window_func.schedule_merge_dag(ls_handle.get_ls()->get_ls_id(),
        *tablet_handle.get_obj(), MEDIUM_MERGE, schedule_scn, base_version, strategy, reason));
    EXPECT_EQ(0, quota);
    EXPECT_EQ(0, window_func.get_schedule_tablet_cnt().schedule_dag_cnt_);
  }
}

// 普通扫描跳过窗口任务，窗口扫描正常选出任务并沿用 quota。
TEST_F(TestSSWindowCompactionObserver, window_task_is_only_selected_by_window_loop)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  create_tablet("t_ss_window_task_route", ls_handle, tablet_handle);
  const int64_t scn = tablet_handle.get_obj()->get_last_major_snapshot_version() + 1;
  ObMediumCompactionInfo info;
  ObMediumCompactionInfoList medium_list;
  ASSERT_TRUE(medium_list.medium_info_list_.add_last(&info));
  DEFER(medium_list.medium_info_list_.remove(&info));
  ObSSScheduleTabletFunc medium_func(ObBasicMergeScheduler::INIT_COMPACTION_SCN);
  int64_t quota = 0;
  ObSSScheduleTabletFunc window_func(scn, quota, 0);
  for (auto *func : {&medium_func, &window_func}) {
    func->ls_status_.ls_id_ = ls_handle.get_ls()->get_ls_id();
    func->tablet_status_.medium_list_ = &medium_list;
  }
  DEFER(medium_func.tablet_status_.medium_list_ = nullptr;
        window_func.tablet_status_.medium_list_ = nullptr);
  ObCOMajorMergeStrategy strategy;
  int64_t schedule_scn = 0;
  int64_t base_version = 0;
  ObMediumCompactionInfo::ObCompactionType type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;
  ObAdaptiveMergePolicy::AdaptiveMergeReason reason = ObAdaptiveMergePolicy::NONE;
  for (auto source : {ObAdaptiveMergePolicy::WINDOW_COMPACTION,
                     ObAdaptiveMergePolicy::TENANT_MAJOR, ObAdaptiveMergePolicy::FREQUENT_WRITE}) {
    info.set_basic_info(ObMediumCompactionInfo::MEDIUM_COMPACTION, source, scn);
    const int expected = source == ObAdaptiveMergePolicy::WINDOW_COMPACTION ? OB_NO_NEED_MERGE : OB_SUCCESS;
    EXPECT_EQ(expected, medium_func.get_schedule_execute_info(*tablet_handle.get_obj(),
        schedule_scn, base_version, strategy, reason, type));
    EXPECT_EQ(OB_SUCCESS, window_func.get_schedule_execute_info(*tablet_handle.get_obj(),
        schedule_scn, base_version, strategy, reason, type));
    EXPECT_EQ(source, reason);
    EXPECT_EQ(scn, schedule_scn);
  }
  EXPECT_EQ(OB_SIZE_OVERFLOW, window_func.schedule_merge_dag(ls_handle.get_ls()->get_ls_id(),
      *tablet_handle.get_obj(), MEDIUM_MERGE, scn, 0, strategy, ObAdaptiveMergePolicy::WINDOW_COMPACTION));
  EXPECT_EQ(0, quota);
  EXPECT_EQ(0, window_func.get_schedule_tablet_cnt().schedule_dag_cnt_);
}

// 备租户按 frozen 消费重放的 WINDOW 记录，不依赖本地窗口开关或 MediumLoop target。
TEST_F(TestSSWindowCompactionObserver, standby_consumes_window_info_with_window_disabled)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  create_tablet("t_ss_window_standby_route", ls_handle, tablet_handle);
  const int64_t last_major = tablet_handle.get_obj()->get_last_major_snapshot_version();
  const int64_t target = last_major + 100;

  // 仅替换本测试线程的 MTL 上下文，不修改共用租户的角色、frozen 或配置。
  ObTenantTabletScheduler scheduler;
  ObTenantBase *original_tenant = ObTenantEnv::get_tenant();
  ObTenantBase tenant_context(run_ctx_.tenant_id_);
  tenant_context = *original_tenant;
  tenant_context.set(&scheduler);
  tenant_context.set_tenant_role(ObTenantRole::STANDBY_TENANT);
  ObTenantEnv::set_tenant(&tenant_context);
  DEFER(ObTenantEnv::set_tenant(original_tenant));
  scheduler.frozen_version_ = target;

  ObMediumCompactionInfo window_info;
  ObMediumCompactionInfo next_info;
  ObMediumCompactionInfoList medium_list;
  ASSERT_TRUE(medium_list.medium_info_list_.add_last(&window_info));
  DEFER(medium_list.medium_info_list_.remove(&window_info));
  ASSERT_TRUE(medium_list.medium_info_list_.add_last(&next_info));
  DEFER(medium_list.medium_info_list_.remove(&next_info));
  next_info.set_basic_info(ObMediumCompactionInfo::MAJOR_COMPACTION,
      ObAdaptiveMergePolicy::TENANT_MAJOR, target + 1);
  ObSSScheduleTabletFunc medium_func(ObBasicMergeScheduler::INIT_COMPACTION_SCN);
  medium_func.ls_status_.ls_id_ = ls_handle.get_ls()->get_ls_id();
  medium_func.tablet_status_.medium_list_ = &medium_list;
  DEFER(medium_func.tablet_status_.medium_list_ = nullptr);
  ObCOMajorMergeStrategy strategy;
  int64_t schedule_scn = 0;
  int64_t base_version = 0;
  ObMediumCompactionInfo::ObCompactionType type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;
  ObAdaptiveMergePolicy::AdaptiveMergeReason reason = ObAdaptiveMergePolicy::NONE;
  for (bool enable_window : {false, true}) {
    scheduler.tenant_status_.enable_window_compaction_ = enable_window;
    for (auto window_type : {ObMediumCompactionInfo::MAJOR_COMPACTION,
                             ObMediumCompactionInfo::MEDIUM_COMPACTION}) {
      window_info.set_basic_info(window_type, ObAdaptiveMergePolicy::WINDOW_COMPACTION, target);
      EXPECT_EQ(OB_SUCCESS, medium_func.get_schedule_execute_info(*tablet_handle.get_obj(),
          schedule_scn, base_version, strategy, reason, type));
      EXPECT_EQ(target, schedule_scn);
      EXPECT_EQ(window_type, type);
      EXPECT_EQ(ObAdaptiveMergePolicy::WINDOW_COMPACTION, reason);
    }
  }

  // MAJOR 目标未准备好时继续等待，不能绕过队首去消费下一条记录。
  window_info.set_basic_info(ObMediumCompactionInfo::MAJOR_COMPACTION,
      ObAdaptiveMergePolicy::WINDOW_COMPACTION, target);
  scheduler.frozen_version_ = target - 1;
  EXPECT_EQ(OB_NO_NEED_MERGE, medium_func.get_schedule_execute_info(*tablet_handle.get_obj(),
      schedule_scn, base_version, strategy, reason, type));
  EXPECT_EQ(0, schedule_scn);

  // 模拟队首已完成，普通租户合并记录可继续推进。
  window_info.medium_snapshot_ = last_major;
  scheduler.frozen_version_ = target + 1;
  EXPECT_EQ(OB_SUCCESS, medium_func.get_schedule_execute_info(*tablet_handle.get_obj(),
      schedule_scn, base_version, strategy, reason, type));
  EXPECT_EQ(target + 1, schedule_scn);
  EXPECT_EQ(ObAdaptiveMergePolicy::TENANT_MAJOR, reason);

  // 切回主租户后，普通路径仍不能消费 WINDOW，即使目标已经准备好。
  tenant_context.set_tenant_role(ObTenantRole::PRIMARY_TENANT);
  medium_func.merge_version_ = target;
  window_info.medium_snapshot_ = target;
  EXPECT_EQ(OB_NO_NEED_MERGE, medium_func.get_schedule_execute_info(*tablet_handle.get_obj(),
      schedule_scn, base_version, strategy, reason, type));
}

// 主表已达到窗口目标时，adaptive 开启也不能再生成一轮窗口合并。
TEST_F(TestSSWindowCompactionObserver, finished_window_tablet_does_not_schedule_adaptive)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  create_tablet("t_ss_window_finished", ls_handle, tablet_handle);
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  scheduler->stop_major_merge();
  DEFER(scheduler->resume_major_merge());
  const bool old_adaptive = scheduler->tenant_status_.enable_adaptive_compaction_;
  scheduler->tenant_status_.enable_adaptive_compaction_ = true;
  DEFER(scheduler->tenant_status_.enable_adaptive_compaction_ = old_adaptive);
  ASSERT_TRUE(scheduler->tenant_status_.enable_adaptive_compaction_with_cpu_load());

  // 安装一个真正达到目标 T 的 major，避免新建表的初始位点影响选点路径。
  const int64_t target = ObTimeUtility::current_time_ns();
  ObArenaAllocator allocator("WindowFinished");
  ObStorageSchema *schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator, schema));
  DEFER(ObTabletObjLoadHelper::free(allocator, schema));
  ObTabletCreateSSTableParam create_param;
  ASSERT_EQ(OB_SUCCESS, create_param.init_for_empty_major_sstable(
      tablet_handle.get_obj()->get_tablet_id(), *schema, target, -1, false, false));
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
      create_param, allocator, table_handle));
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(sstable));
  ObUpdateTableStoreParam update_param;
  update_param.snapshot_version_ = target;
  update_param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
  update_param.storage_schema_ = schema;
  update_param.rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq();
  update_param.sstable_ = sstable;
  ASSERT_EQ(OB_SUCCESS, update_param.init_with_ha_info(ObHATableStoreParam(
      tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_, true)));
  ASSERT_EQ(OB_SUCCESS, update_param.init_with_compaction_info(ObCompactionTableStoreParam(
      MAJOR_MERGE, sstable->get_end_scn(), false, tablet_handle.get_obj()->has_merged_with_mds_info())));
  ObTabletHandle new_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->update_tablet_table_store(
      tablet_handle.get_obj()->get_tablet_id(), update_param, new_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.assign(new_handle));
  ASSERT_EQ(target, tablet_handle.get_obj()->get_last_major_snapshot_version());
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_info_list(allocator, medium_list));
  ASSERT_NE(nullptr, medium_list);
  int64_t quota = 1;
  ObSSScheduleTabletFunc func(target, quota, 0);
  func.ls_status_.ls_handle_ = ls_handle;
  func.ls_status_.ls_id_ = ls_handle.get_ls()->get_ls_id();
  ASSERT_EQ(OB_SUCCESS, func.ls_status_.weak_read_ts_.convert_for_tx(target + 1000000000L));
  func.tablet_status_.medium_list_ = medium_list;
  func.tablet_status_.tablet_merge_finish_ = true;
  ASSERT_EQ(OB_SUCCESS, func.schedule_tablet_new_round(tablet_handle, false));
  EXPECT_EQ(0, func.tablet_cnt_.submit_clog_cnt_);
}

// Keep the real SQL/configuration path, but stop the private DAG scheduler's timer
// so that only the window entry under test can publish a new concurrency limit.
class WindowThreadCountTestContext
{
public:
  WindowThreadCountTestContext()
    : original_tenant_(ObTenantEnv::get_tenant()), tenant_context_(MTL_ID()),
      saved_job_(false), saved_config_(false), old_enable_window_(false)
  {}

  ~WindowThreadCountTestContext()
  {
    ObTenantEnv::set_tenant(original_tenant_);
    dag_scheduler_.destroy();
    if (saved_job_) {
      int ret = OB_SUCCESS;
      {
        ObMySQLTransaction trans;
        ObResourceManagerProxy::TransGuard guard(trans, MTL_ID(), ret);
        if (guard.ready()) {
          ret = ObScheduleDailyMaintenanceWindow::set_job_config(
              trans, allocator_, MTL_ID(), old_job_.job_config_);
        }
      }
      EXPECT_EQ(OB_SUCCESS, ret);
      ObSqlString sql;
      EXPECT_EQ(OB_SUCCESS, sql.assign_fmt(
          "CALL DBMS_SCHEDULER.SET_ATTRIBUTE('DAILY_MAINTENANCE_WINDOW', 'duration', '%ld')",
          old_job_.get_max_run_duration()));
      EXPECT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
      EXPECT_EQ(OB_SUCCESS, exe_sql(old_job_.is_disabled()
          ? "CALL DBMS_SCHEDULER.DISABLE('DAILY_MAINTENANCE_WINDOW')"
          : "CALL DBMS_SCHEDULER.ENABLE('DAILY_MAINTENANCE_WINDOW')"));
    }
    if (saved_config_) {
      omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
      if (config.is_valid()) {
        config->enable_window_compaction = old_enable_window_;
      }
    }
  }

  void init()
  {
    ASSERT_NE(nullptr, original_tenant_);
    {
      omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
      ASSERT_TRUE(config.is_valid());
      old_enable_window_ = config->enable_window_compaction;
      saved_config_ = true;
      config->enable_window_compaction = true;
    }
    ASSERT_EQ(OB_SUCCESS, ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(
        *GCTX.sql_proxy_, MTL_ID(), allocator_, old_job_));
    saved_job_ = true;
    ASSERT_EQ(OB_SUCCESS, exe_sql(
        "CALL DBMS_SCHEDULER.SET_ATTRIBUTE('DAILY_MAINTENANCE_WINDOW', 'duration', '3600')"));
    ASSERT_EQ(OB_SUCCESS, exe_sql("CALL DBMS_SCHEDULER.ENABLE('DAILY_MAINTENANCE_WINDOW')"));

    ASSERT_EQ(OB_SUCCESS, dag_scheduler_.init(MTL_ID()));
    dag_scheduler_.stop();
    dag_scheduler_.wait();
    tenant_context_ = *original_tenant_;
    tenant_context_.set(&tablet_scheduler_);
    tenant_context_.set(&dag_scheduler_);
    ObTenantEnv::set_tenant(&tenant_context_);
    ASSERT_EQ(OB_SUCCESS, tablet_scheduler_.tenant_status_.init_or_refresh());
    ASSERT_EQ(OB_SUCCESS, tablet_scheduler_.tenant_status_.refresh_tenant_config(false, false, true));
    tablet_scheduler_.is_stop_ = false;
    tablet_scheduler_.resume_major_merge();
    tablet_scheduler_.batch_size_mgr_.set_tablet_batch_size(1);
    ASSERT_FALSE(tablet_scheduler_.tenant_status_.is_skip_window_compaction_tenant());
    ASSERT_EQ(OB_SUCCESS, dag_scheduler_.set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_LOW, 8));

    merge_info_.merge_mode_ = ObGlobalMergeInfo::MERGE_MODE_WINDOW;
    merge_info_.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_MERGING;
    merge_info_.merge_start_time_ = ObTimeUtility::current_time();
    // Already-merged target: exercise the scan without starting unrelated data merges.
    merge_info_.global_broadcast_scn_ = ObBasicMergeScheduler::INIT_COMPACTION_SCN;
  }

  int scan()
  {
    return tablet_scheduler_.ss_window_loop_.try_schedule_window_compaction(
        merge_info_, merge_info_.global_broadcast_scn_);
  }

  void expect_limit(const int64_t expected)
  {
    int64_t actual = 0;
    ASSERT_EQ(OB_SUCCESS, dag_scheduler_.get_adaptive_limit(ObDagPrio::DAG_PRIO_COMPACTION_LOW, actual));
    EXPECT_EQ(expected, actual);
  }

  ObTenantBase *original_tenant_;
  ObTenantTabletScheduler tablet_scheduler_;
  ObTenantDagScheduler dag_scheduler_;
  ObTenantBase tenant_context_;
  ObMergeInfo merge_info_;
  ObArenaAllocator allocator_;
  dbms_scheduler::ObDBMSSchedJobInfo old_job_;
  bool saved_job_;
  bool saved_config_;
  bool old_enable_window_;
};

// SSWC-THREAD-001/002/003: production entry, live changes, expiry and disable/re-entry.
TEST_F(TestSSWindowCompactionObserver, window_thread_count_through_schedule_entry)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  WindowThreadCountTestContext ctx;
  ASSERT_NO_FATAL_FAILURE(ctx.init());
  const int64_t start_time = ctx.merge_info_.merge_start_time_;
  for (const int64_t count : {10, 1, 8, 100}) {
    SCOPED_TRACE(count);
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("CALL DBMS_DAILY_MAINTENANCE.SET_THREAD_COUNT(%ld)", count));
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
    // Do not call the cache refresh or DAG adaptation helpers from this test.
    ASSERT_EQ(OB_SUCCESS, ctx.scan());
    ASSERT_NO_FATAL_FAILURE(ctx.expect_limit(count));
  }

  ctx.merge_info_.merge_start_time_ = start_time - 2L * 60 * 60 * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS, ctx.scan());
  ASSERT_NO_FATAL_FAILURE(ctx.expect_limit(8));
  ctx.merge_info_.merge_start_time_ = start_time;
  ASSERT_EQ(OB_SUCCESS, ctx.scan());
  ASSERT_NO_FATAL_FAILURE(ctx.expect_limit(100));

  ASSERT_EQ(OB_SUCCESS, exe_sql("CALL DBMS_SCHEDULER.DISABLE('DAILY_MAINTENANCE_WINDOW')"));
  ASSERT_EQ(OB_SUCCESS, ctx.scan());
  ASSERT_NO_FATAL_FAILURE(ctx.expect_limit(8));
  ASSERT_EQ(OB_SUCCESS, exe_sql("CALL DBMS_SCHEDULER.ENABLE('DAILY_MAINTENANCE_WINDOW')"));
  ASSERT_EQ(OB_SUCCESS, ctx.scan());
  ASSERT_NO_FATAL_FAILURE(ctx.expect_limit(100));
}

// SSWC-THREAD-004: requires a dedicated writable cgroup v1 CPU hierarchy.
// Opt in with --gtest_also_run_disabled_tests; never silently pass on fallback.
TEST_F(TestSSWindowCompactionObserver, DISABLED_window_automatic_thread_count_from_resource_plan)
{
  ASSERT_NE(nullptr, GCTX.cgroup_ctrl_);
  bool linked_cgroup = false;
  const bool old_enable_cgroup = GCONF.enable_cgroup;
  DEFER(if (linked_cgroup) {
    GCONF.enable_cgroup = false;
    (void) GCTX.cgroup_ctrl_->check_cgroup_status();
    GCONF.enable_cgroup = old_enable_cgroup;
    EXPECT_EQ(0, unlink("cgroup"));
  });
  if (!GCTX.cgroup_ctrl_->is_valid()) {
    // The observer fixture recreates its working directory during startup, so
    // link the test-owned hierarchy here, after startup, through the normal API.
    const char *cgroup_dir = std::getenv("OB_MITTEST_CGROUP_DIR");
    ASSERT_NE(nullptr, cgroup_dir) << "Set OB_MITTEST_CGROUP_DIR to a dedicated cgroup v1 CPU directory";
    ASSERT_EQ(0, symlink(cgroup_dir, "cgroup"));
    linked_cgroup = true;
    GCONF.enable_cgroup = true;
    (void) GCTX.cgroup_ctrl_->check_cgroup_status();
  }
  ASSERT_TRUE(GCTX.cgroup_ctrl_->is_valid());
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  WindowThreadCountTestContext ctx;
  ASSERT_NO_FATAL_FAILURE(ctx.init());

  ASSERT_EQ(OB_SUCCESS, exe_sql("CALL DBMS_RESOURCE_MANAGER.CREATE_PLAN('INNER_DAILY_WINDOW_PLAN')"));
  DEFER(EXPECT_EQ(OB_SUCCESS, exe_sql("CALL DBMS_RESOURCE_MANAGER.DELETE_PLAN('INNER_DAILY_WINDOW_PLAN')")));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "CALL DBMS_RESOURCE_MANAGER.CREATE_CONSUMER_GROUP('INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP')"));
  DEFER(EXPECT_EQ(OB_SUCCESS, exe_sql(
      "CALL DBMS_RESOURCE_MANAGER.DELETE_CONSUMER_GROUP('INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP')")));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "CALL DBMS_RESOURCE_MANAGER.CREATE_PLAN_DIRECTIVE(PLAN => 'INNER_DAILY_WINDOW_PLAN', "
      "GROUP_OR_SUBPLAN => 'INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP', UTILIZATION_LIMIT => 100)"));
  DEFER(EXPECT_EQ(OB_SUCCESS, exe_sql(
      "CALL DBMS_RESOURCE_MANAGER.DELETE_PLAN_DIRECTIVE('INNER_DAILY_WINDOW_PLAN', "
      "'INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP')")));

  ObCompactionResourceManager resource_mgr;
  rootserver::ObWindowResourceCache resource_cache;
  bool switched = false;
  DEFER(EXPECT_EQ(OB_SUCCESS, resource_mgr.check_and_switch(
      false, ctx.merge_info_.merge_start_time_, resource_cache, switched)));
  ASSERT_EQ(OB_SUCCESS, resource_mgr.check_and_switch(
      true, ctx.merge_info_.merge_start_time_, resource_cache, switched));
  ASSERT_TRUE(switched);
  ObDailyWindowJobConfig old_config(ctx.allocator_);
  ASSERT_EQ(OB_SUCCESS, old_config.parse_from_string(ctx.old_job_.job_config_));
  if (old_config.thread_cnt_ != 0) {
    ASSERT_EQ(OB_SUCCESS, exe_sql("CALL DBMS_DAILY_MAINTENANCE.SET_THREAD_COUNT(0)"));
  }

  int64_t expected_count = 0;
  bool from_job_config = true;
  ASSERT_EQ(OB_SUCCESS, resource_mgr.get_window_compaction_thread_cnt(expected_count, from_job_config));
  ASSERT_FALSE(from_job_config);
  ASSERT_GT(expected_count, 0);
  // Exercise both sides of max(computed_count, base), using the actual resource calculation.
  for (const int64_t base : {1L, expected_count + 1}) {
    SCOPED_TRACE(base);
    ASSERT_EQ(OB_SUCCESS, ctx.dag_scheduler_.set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_LOW, base));
    ASSERT_EQ(OB_SUCCESS, ctx.scan());
    ASSERT_NO_FATAL_FAILURE(ctx.expect_limit(std::max(expected_count, base)));
    ctx.merge_info_.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_IDLE;
    ASSERT_EQ(OB_SUCCESS, ctx.scan());
    ASSERT_NO_FATAL_FAILURE(ctx.expect_limit(base));
    ctx.merge_info_.merge_status_ = ObZoneMergeInfo::MERGE_STATUS_MERGING;
  }
}

// Use isolated scheduler objects so background tenant threads cannot overwrite the assertions.
TEST_F(TestSSWindowCompactionObserver, window_thread_count_reaches_ss_dag_scheduler)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObTenantTabletScheduler tablet_scheduler;
  ObTenantDagScheduler dag_scheduler;
  ObTenantBase *original_tenant = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, original_tenant);
  ObTenantBase tenant_context(run_ctx_.tenant_id_);
  tenant_context = *original_tenant;
  tenant_context.set(&tablet_scheduler);
  tenant_context.set(&dag_scheduler);
  ObTenantEnv::set_tenant(&tenant_context);
  DEFER(ObTenantEnv::set_tenant(original_tenant));
  ObSSWindowLoop &window_loop = tablet_scheduler.ss_window_loop_;
  ObDagPrioScheduler &prio_scheduler = dag_scheduler.prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_LOW];
  prio_scheduler.priority_ = ObDagPrio::DAG_PRIO_COMPACTION_LOW;
  int64_t old_score = 0;
  int64_t new_score = 0;
  ASSERT_EQ(OB_SUCCESS, prio_scheduler.set_thread_score(8, old_score, new_score));
  EXPECT_EQ(8, prio_scheduler.get_adaptive_limit());

  // A fresh SS window must load the persisted value, including values below the base score.
  DEFER((void)exe_sql("CALL DBMS_DAILY_MAINTENANCE.SET_THREAD_COUNT(0)"));
  const int64_t thread_counts[] = {10, 1, 8, 100};
  for (const int64_t thread_count : thread_counts) {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "CALL DBMS_DAILY_MAINTENANCE.SET_THREAD_COUNT(%ld)", thread_count));
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
    window_loop.update_adaptive_compaction_thread_cnt_();
    dag_scheduler.adapt_window_thread_cnt();
    EXPECT_EQ(thread_count, prio_scheduler.get_adaptive_limit());
    // Config reload restores the base score; the next DAG adaptation reapplies the window limit.
    ASSERT_EQ(OB_SUCCESS, prio_scheduler.set_thread_score(8, old_score, new_score));
    EXPECT_EQ(8, prio_scheduler.get_adaptive_limit());
    dag_scheduler.adapt_window_thread_cnt();
    EXPECT_EQ(thread_count, prio_scheduler.get_adaptive_limit());
  }

  // An idle/non-window merge clears the override; the persisted setting remains for next time.
  ObMergeInfo merge_info;
  ASSERT_EQ(OB_SUCCESS, window_loop.try_schedule_window_compaction(merge_info, 0));
  EXPECT_EQ(8, prio_scheduler.get_adaptive_limit());
  window_loop.update_adaptive_compaction_thread_cnt_();
  dag_scheduler.adapt_window_thread_cnt();
  EXPECT_EQ(100, prio_scheduler.get_adaptive_limit());

  // Zero means automatic, never zero runnable tasks; no window resource plan is installed here.
  ASSERT_EQ(OB_SUCCESS, exe_sql("CALL DBMS_DAILY_MAINTENANCE.SET_THREAD_COUNT(0)"));
  window_loop.update_adaptive_compaction_thread_cnt_();
  dag_scheduler.adapt_window_thread_cnt();
  EXPECT_EQ(8, prio_scheduler.get_adaptive_limit());
}

TEST_F(TestSSWindowCompactionObserver, window_automatic_thread_count_respects_base_score)
{
  ObSSWindowLoop window_loop;
  window_loop.thread_cnt_assigned_ = false;
  int64_t thread_count = 0;
  window_loop.adaptive_thread_cnt_ = 2;
  ASSERT_EQ(OB_SUCCESS, window_loop.get_adaptive_compaction_thread_cnt(8, thread_count));
  EXPECT_EQ(8, thread_count);
  window_loop.adaptive_thread_cnt_ = 10;
  ASSERT_EQ(OB_SUCCESS, window_loop.get_adaptive_compaction_thread_cnt(8, thread_count));
  EXPECT_EQ(10, thread_count);
  window_loop.reset();
  ASSERT_EQ(OB_SUCCESS, window_loop.get_adaptive_compaction_thread_cnt(8, thread_count));
  EXPECT_EQ(8, thread_count);
}

TEST_F(TestSSWindowCompactionObserver, window_scan_refreshes_ids_after_complete_pass)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle created_ls;
  ObTabletHandle created_tablet;
  create_tablet("t_ss_window_scan_before", created_ls, created_tablet);
  ObCompactionScheduleIterator iter(MEDIUM_MERGE);
  bool found = false;
  ASSERT_EQ(OB_SUCCESS, iter.resume_or_build_iter(100000));
  ASSERT_EQ(OB_ITER_END, scan_tablets(iter, created_tablet.get_obj()->get_tablet_id(), found));
  ASSERT_TRUE(found);
  ASSERT_TRUE(iter.is_scan_finish());
  iter.release_ls_handle();

  create_tablet("t_ss_window_scan_after", created_ls, created_tablet);
  ASSERT_EQ(OB_SUCCESS, iter.resume_or_build_iter(100000));
  ASSERT_EQ(OB_ITER_END, scan_tablets(iter, created_tablet.get_obj()->get_tablet_id(), found));
  EXPECT_TRUE(found);
  EXPECT_TRUE(iter.is_scan_finish());
  iter.release_ls_handle();
}

// SSWC-OBS-MIT-003：验证低优先级 DAG 进入和离开队列时，每轮 Writer quota 随队列水位减少并恢复。
TEST_F(TestSSWindowCompactionObserver, writer_quota_tracks_compaction_low_dag_count)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, dag_scheduler);
  ObSSWindowLoop window_loop;
  const ObDagType::ObDagTypeEnum pressure_dag_type = ObDagType::DAG_TYPE_VERIFY_CKM;
  const int64_t pressure_dag_count_before = dag_scheduler->get_dag_count(pressure_dag_type);
  WriterQuotaState initial_state;
  WriterQuotaState pressure_state;
  WriterQuotaState recovered_state;

  // STEP 1：无测试 DAG 压力时，记录第一轮 quota。
  ASSERT_EQ(OB_SUCCESS, get_stable_writer_quota_state(
      *dag_scheduler, window_loop, initial_state));
  ASSERT_GT(initial_state.writer_quota_, 0);
  EXPECT_EQ(std::max(0L, initial_state.compaction_low_adaptive_limit_
      * ObSSWindowLoop::WRITER_DAG_LIMIT_FACTOR - initial_state.low_priority_dag_count_),
      initial_state.writer_quota_);

  // STEP 2：插入一个短暂驻留的 COMPACTION_LOW DAG，下一轮 quota 应减少 1。
  ASSERT_EQ(OB_SUCCESS, add_writer_quota_pressure_dag(
      *dag_scheduler, ObTimeUtility::current_time_ns()));
  ASSERT_TRUE(wait_for_dag_count(
      *dag_scheduler, pressure_dag_type, pressure_dag_count_before + 1));
  ASSERT_EQ(OB_SUCCESS, get_stable_writer_quota_state(
      *dag_scheduler, window_loop, pressure_state));
  ASSERT_EQ(initial_state.compaction_low_adaptive_limit_,
      pressure_state.compaction_low_adaptive_limit_);
  ASSERT_EQ(initial_state.low_priority_dag_count_ + 1,
      pressure_state.low_priority_dag_count_);
  EXPECT_EQ(initial_state.writer_quota_ - 1, pressure_state.writer_quota_);

  // STEP 3：测试 DAG 执行完成后，队列水位回落，新一轮 quota 应恢复。
  ASSERT_TRUE(wait_for_dag_count(
      *dag_scheduler, pressure_dag_type, pressure_dag_count_before));
  ASSERT_EQ(OB_SUCCESS, get_stable_writer_quota_state(
      *dag_scheduler, window_loop, recovered_state));
  EXPECT_EQ(initial_state.compaction_low_adaptive_limit_,
      recovered_state.compaction_low_adaptive_limit_);
  EXPECT_EQ(initial_state.low_priority_dag_count_, recovered_state.low_priority_dag_count_);
  EXPECT_EQ(initial_state.writer_quota_, recovered_state.writer_quota_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  const char *s3_bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (OB_ISNULL(s3_bucket) || '\0' == s3_bucket[0]) {
    s3_bucket = oceanbase::unittest::S3_BUCKET;
  }
  const char *s3_endpoint_override = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_test_endpoint = OB_NOT_NULL(s3_endpoint_override)
      && '\0' != s3_endpoint_override[0];
  const char *s3_endpoint = use_test_endpoint
      ? s3_endpoint_override : oceanbase::unittest::S3_ENDPOINT;
  const char *s3_ak = use_test_endpoint
      ? "minioadmin" : oceanbase::unittest::S3_AK;
  const char *s3_sk = use_test_endpoint
      ? "minioadmin" : oceanbase::unittest::S3_SK;
  const char *s3_region = std::getenv("OB_MITTEST_S3_REGION");
  if (OB_ISNULL(s3_region) || '\0' == s3_region[0]) {
    s3_region = oceanbase::unittest::S3_REGION;
  }
  char storage_info[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  const int ret = databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      s3_bucket, cur_time_ns, s3_endpoint, s3_ak, s3_sk, s3_region);
  if (OB_SUCCESS != ret) {
    return ret;
  }
  oceanbase::shared_storage_info = storage_info;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("20G");
  GCONF.memory_limit.set_value("10G");
  GCONF.system_memory.set_value("3G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
