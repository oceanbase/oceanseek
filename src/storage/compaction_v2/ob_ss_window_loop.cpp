// Copyright (c) 2026 OceanBase
// SPDX-License-Identifier: Apache-2.0

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_ss_window_loop.h"
#include "ob_ss_schedule_tablet_func.h"
#include "lib/literals/ob_literals.h"
#include "share/compaction/ob_schedule_daily_maintenance_window.h"
#include "share/compaction/ob_compaction_resource_manager.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace storage;
namespace compaction
{

ObSSWindowLoop::ObSSWindowLoop()
  : window_start_time_us_(0),
    target_scn_(ObBasicMergeScheduler::INIT_COMPACTION_SCN),
    loop_cnt_(0),
    ls_tablet_iter_(MEDIUM_MERGE),
    thread_cnt_lock_(ObLatchIds::WINDOW_COMPACTION_BASE_CONTAINER_LOCK),
    adaptive_thread_cnt_(0),
    thread_cnt_assigned_(false)
{
}

void ObSSWindowLoop::reset()
{
  window_start_time_us_ = 0;
  target_scn_ = ObBasicMergeScheduler::INIT_COMPACTION_SCN;
  loop_cnt_ = 0;
  ls_tablet_iter_.reset();
  reset_adaptive_compaction_thread_cnt_();
}

void ObSSWindowLoop::adapt_window_thread_cnt_(const bool can_schedule)
{
  if (can_schedule) {
    update_adaptive_compaction_thread_cnt_();
  } else {
    reset_adaptive_compaction_thread_cnt_();
  }
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  if (OB_NOT_NULL(dag_scheduler)) {
    // Apply the refreshed limit before calculating this scan's writer quota.
    dag_scheduler->adapt_window_thread_cnt();
  }
}

void ObSSWindowLoop::reset_adaptive_compaction_thread_cnt_()
{
  common::SpinWLockGuard guard(thread_cnt_lock_);
  if (OB_SUCCESS == guard.get_ret()) {
    adaptive_thread_cnt_ = 0;
    thread_cnt_assigned_ = false;
  }
}

void ObSSWindowLoop::update_adaptive_compaction_thread_cnt_()
{
  int tmp_ret = OB_SUCCESS;
  int64_t thread_cnt = 0;
  bool from_job_config = false;
  share::ObCompactionResourceManager resource_mgr;
  // Refresh every window scan, including the first scan of a new target/window.
  if (OB_TMP_FAIL(resource_mgr.get_window_compaction_thread_cnt(thread_cnt, from_job_config))) {
    reset_adaptive_compaction_thread_cnt_();
    if (OB_NOT_SUPPORTED == tmp_ret) {
      // Detailed unexpected failures are still logged by the resource manager.
      if (REACH_THREAD_TIME_INTERVAL(1_min)) {
        LOG_INFO("ss window thread count adaptation is not supported, use compaction_low_thread_score", K(tmp_ret));
      }
    } else {
      LOG_WARN_RET(tmp_ret, "failed to get ss window thread count, use compaction_low_thread_score", K(tmp_ret));
    }
  } else {
    common::SpinWLockGuard guard(thread_cnt_lock_);
    if (OB_SUCCESS == guard.get_ret()) {
      adaptive_thread_cnt_ = thread_cnt;
      thread_cnt_assigned_ = from_job_config;
    }
  }
}

int ObSSWindowLoop::get_adaptive_compaction_thread_cnt(
    const int64_t compaction_low_thread_score,
    int64_t &thread_cnt) const
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(thread_cnt_lock_);
  thread_cnt = compaction_low_thread_score;
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("failed to lock ss window thread count", K(ret));
  } else {
    thread_cnt = thread_cnt_assigned_
        ? adaptive_thread_cnt_ : std::max(adaptive_thread_cnt_, compaction_low_thread_score);
  }
  return ret;
}

void ObSSWindowLoop::update_window_schedule_state_(const ObMergeInfo &merge_info)
{
  const int64_t window_start_time_us = merge_info.merge_start_time_;
  const int64_t window_target_scn = merge_info.global_broadcast_scn_;
  if (window_target_scn != target_scn_) {
    // target 发生变化，进入新一轮窗口合并，切换新 target 并重新开始 Tablet 扫描
    window_start_time_us_ = window_start_time_us;
    target_scn_ = window_target_scn;
    loop_cnt_ = 0;
    ls_tablet_iter_.reset();
    LOG_INFO("start ss window loop state", K_(window_start_time_us), K_(target_scn));
  } else if (window_start_time_us != window_start_time_us_) {
    // 同一轮窗口合并，进入新的每日窗口，仅更新时间，继续复用 Tablet 扫描进度。
    const int64_t old_window_start_time_us = window_start_time_us_;
    window_start_time_us_ = window_start_time_us;
    LOG_INFO("update ss window start time", K(old_window_start_time_us), K_(window_start_time_us), K_(target_scn));
  }
}

int ObSSWindowLoop::check_can_schedule_(
    bool &can_schedule,
    int64_t &window_end_time_us) const
{
  int ret = OB_SUCCESS;
  can_schedule = false;
  window_end_time_us = 0;
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant tablet scheduler is null", K(ret));
  } else if (!scheduler->get_tenant_status().enable_window_compaction()
      || scheduler->get_tenant_status().is_skip_window_compaction_tenant()
      || !scheduler->could_major_merge_start()) {
    // 本地配置或合并状态不允许提交新的窗口合并 DAG。
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    ObArenaAllocator job_info_allocator("SSWindowCheck", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    dbms_scheduler::ObDBMSSchedJobInfo window_job_info;
    if (OB_FAIL(share::ObScheduleDailyMaintenanceWindow::get_daily_maintenance_window_job_info(
        *GCTX.sql_proxy_, MTL_ID(), job_info_allocator, window_job_info))) {
      LOG_WARN("failed to get daily maintenance window job info", K(ret));
    } else if (window_job_info.is_disabled()) {
      // 每日维护窗口禁用时不允许提交新的合并 DAG。
    } else {
      const int64_t window_duration_us = window_job_info.get_max_run_duration() * 1000 * 1000L;
      if (24_hour == window_duration_us) {
        can_schedule = true;
        window_end_time_us = INT64_MAX;
      } else {
        const int64_t now_us = ObTimeUtility::current_time_us();
        window_end_time_us = window_start_time_us_ + window_duration_us;
        can_schedule = now_us >= window_start_time_us_ && now_us < window_end_time_us;
      }
    }
  }
  return ret;
}

int ObSSWindowLoop::calc_writer_schedule_quota_(int64_t &writer_schedule_quota) const
{
  int ret = OB_SUCCESS;
  int64_t compaction_low_adaptive_limit = 0;
  int64_t low_priority_dag_count = 0;
  writer_schedule_quota = 0;
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null", K(ret));
  } else if (OB_FAIL(dag_scheduler->get_adaptive_limit(
      share::ObDagPrio::DAG_PRIO_COMPACTION_LOW, compaction_low_adaptive_limit))) {
    LOG_WARN("failed to get compaction low adaptive limit", K(ret));
  } else if (OB_FAIL(dag_scheduler->get_priority_dag_count(
      share::ObDagPrio::DAG_PRIO_COMPACTION_LOW, low_priority_dag_count))) {
    LOG_WARN("failed to get compaction low dag count", K(ret));
  } else {
    // TODO(xunyuan):WRITER_DAG_LIMIT_FACTOR 考虑下做成自适应的，目前是写死的
    const int64_t writer_dag_limit = compaction_low_adaptive_limit * WRITER_DAG_LIMIT_FACTOR;
    writer_schedule_quota = std::max(0L, writer_dag_limit - low_priority_dag_count);
    LOG_TRACE("get ss window writer schedule quota", K(compaction_low_adaptive_limit),
        K(writer_dag_limit), K(low_priority_dag_count), K(writer_schedule_quota));
  }
  return ret;
}

bool ObSSWindowLoop::can_continue_schedule_(
    const int64_t writer_schedule_quota,
    const int64_t window_end_time_us) const
{
  return writer_schedule_quota > 0
    && ObTimeUtility::current_time_us() < window_end_time_us;
}

int ObSSWindowLoop::loop_in_ls_(
    ObLSHandle &ls_handle,
    ObSSScheduleTabletFunc &schedule_func,
    int64_t &writer_schedule_quota,
    const int64_t window_end_time_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObLSID ls_id = ls_handle.get_ls()->get_ls_id();
  if (OB_FAIL(schedule_func.switch_ls(ls_handle))) {
    if (OB_STATE_NOT_MATCH != ret) {
      LOG_WARN("failed to switch ls for ss window compaction", K(ret), K(ls_id));
    }
  } else {
    ObTabletHandle tablet_handle;
    bool tablet_merge_finish = false;
    while (OB_SUCC(ret)) {
      // TODO(xunyuan):迭代器现在会在内存里保存单个LS的全量tablet_id，之后元信息改造去掉
      if (!can_continue_schedule_(writer_schedule_quota, window_end_time_us)) {
        break;
      } else if (OB_FAIL(ls_tablet_iter_.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet for ss window compaction", K(ret), K(ls_id));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid()
          || OB_ISNULL(tablet_handle.get_obj()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet for ss window compaction", K(ret), K(ls_id), K(tablet_handle));
      } else if (OB_TMP_FAIL(schedule_func.switch_and_schedule_tablet(tablet_handle, tablet_merge_finish))) {
        ls_tablet_iter_.update_merge_finish(false);
        if (OB_STATE_NOT_MATCH != tmp_ret) {
          LOG_WARN("failed to schedule tablet for ss window compaction", K(tmp_ret), K(ls_id),
              "tablet_id", tablet_handle.get_obj()->get_tablet_id());
        }
      } else {
        ls_tablet_iter_.update_merge_finish(tablet_merge_finish);
      }
    }
    if (OB_TMP_FAIL(schedule_func.post_process_ls())) {
      LOG_WARN("failed to post process ls for ss window compaction", K(tmp_ret), K(ls_id));
    }
  }
  return ret;
}

int ObSSWindowLoop::loop_(const int64_t window_end_time_us)
{
  int ret = OB_SUCCESS;
  int64_t writer_schedule_quota = 0;
  ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler *);
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant tablet scheduler is null", K(ret));
  } else if (OB_FAIL(calc_writer_schedule_quota_(writer_schedule_quota))) {
    LOG_WARN("failed to get ss window writer schedule quota", K(ret));
  } else if (!can_continue_schedule_(writer_schedule_quota, window_end_time_us)) {
    LOG_TRACE("skip ss window compaction for schedule limit", K(writer_schedule_quota), K(window_end_time_us));
  } else {
    const int64_t max_scan_tablet_count = scheduler->get_schedule_batch_size();
    if (OB_FAIL(ls_tablet_iter_.resume_or_build_iter(max_scan_tablet_count))) {
      LOG_WARN("failed to build ss window tablet iterator", K(ret), K(max_scan_tablet_count));
    } else {
      int tmp_ret = OB_SUCCESS;
      ObLSHandle ls_handle;
      ObSSScheduleTabletFunc schedule_func(target_scn_, writer_schedule_quota, loop_cnt_);
      while (OB_SUCC(ret)) {
        // 先推进 LS 游标，再检查 quota 和窗口时间，确保上一 LS 扫描完成时，能正确切换到下一 LS 或标记全租户扫描完成。
        if (OB_FAIL(ls_tablet_iter_.get_next_ls(ls_handle))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get ls for ss window compaction", K(ret), K_(ls_tablet_iter));
          }
        } else if (!can_continue_schedule_(writer_schedule_quota, window_end_time_us)) {
          break;
        } else if (OB_TMP_FAIL(loop_in_ls_(ls_handle, schedule_func, writer_schedule_quota, window_end_time_us))) {
          ls_tablet_iter_.skip_cur_ls();
          ls_tablet_iter_.update_merge_finish(false);
          if (OB_ITER_END != tmp_ret && OB_STATE_NOT_MATCH != tmp_ret && OB_LS_NOT_EXIST != tmp_ret) {
            LOG_WARN("failed to schedule ss window compaction in ls", K(tmp_ret),
                "ls_id", ls_handle.get_ls()->get_ls_id());
          }
        }
      }
      if (OB_SUCC(ret) && ls_tablet_iter_.is_scan_finish()) {
        ++loop_cnt_;
      }
      LOG_TRACE("finish ss window compaction schedule", K(ret), K_(target_scn), K_(loop_cnt),
          K(max_scan_tablet_count), K(window_end_time_us), K(writer_schedule_quota), K_(ls_tablet_iter),
          "schedule_tablet_count", schedule_func.get_schedule_tablet_cnt());
    }
  }
  return ret;
}

int ObSSWindowLoop::try_schedule_window_compaction(
    const ObMergeInfo &merge_info,
    const int64_t local_frozen_scn)
{
  int ret = OB_SUCCESS;
  bool can_schedule = false;
  int64_t window_end_time_us = 0;
  if (merge_info.is_global_during_window_compaction()
      && merge_info.global_broadcast_scn_ <= local_frozen_scn) {
    (void) update_window_schedule_state_(merge_info);
    if (OB_FAIL(check_can_schedule_(can_schedule, window_end_time_us))) {
      LOG_WARN("failed to check whether ss window compaction can be scheduled", K(ret), K(merge_info));
    } else if (!can_schedule) {
      LOG_TRACE("skip ss window compaction because scheduling is not allowed", K(merge_info));
    }
  }

  if (OB_SUCC(ret) && can_schedule) {
    if (OB_FALSE_IT((void)adapt_window_thread_cnt_(can_schedule))) {
    } else if (OB_FAIL(loop_(window_end_time_us))) {
      LOG_WARN("failed to loop ss window tablets", K(ret), K(merge_info));
    }
  } else {
    adapt_window_thread_cnt_(false /* can_schedule */);
  }

  // Resume this target at the next tablet, even if the next call is in another
  // window. Release the LS reference so migration cleanup can make progress.
  ls_tablet_iter_.release_ls_handle();
  return ret;
}

} // namespace compaction
} // namespace oceanbase
