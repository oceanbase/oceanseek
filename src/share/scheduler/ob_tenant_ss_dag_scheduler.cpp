/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/ob_perf_stat.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace share
{
static constexpr int64_t MAX_SS_DAG_PRIO_TYPE_NUM = 4;
static int64_t dag_prio_type_for_ss[MAX_SS_DAG_PRIO_TYPE_NUM] = {
    ObDagPrio::DAG_PRIO_COMPACTION_HIGH,
    ObDagPrio::DAG_PRIO_COMPACTION_MID,
    ObDagPrio::DAG_PRIO_INC_SSTABLE_UPLOAD,
    ObDagPrio::DAG_PRIO_LINK_SHARED_SSTABLE,
};
// only sum the thread used by shared storage: mini, upload, minor, attach
int64_t ObTenantDagScheduler::get_ss_total_thread_num_() const
{
#define GET_THREAD_NUM(DAG_PRIO, CONFIG) \
  CONFIG != 0 ? CONFIG : OB_DAG_PRIOS[DAG_PRIO].score_

  int64_t total_thread_num = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    total_thread_num += GET_THREAD_NUM(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, tenant_config->compaction_high_thread_score);
    total_thread_num += GET_THREAD_NUM(ObDagPrio::DAG_PRIO_COMPACTION_MID, tenant_config->compaction_mid_thread_score);
    // use compaction_high_thread_score as default upload thread count
    if (0 == tenant_config->inc_sstable_upload_thread_score) {
      total_thread_num += GET_THREAD_NUM(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, tenant_config->compaction_high_thread_score);
    } else {
      total_thread_num += tenant_config->inc_sstable_upload_thread_score;
    }
    total_thread_num += GET_THREAD_NUM(ObDagPrio::DAG_PRIO_LINK_SHARED_SSTABLE, tenant_config->link_shared_sstable_thread_score);
  } else {
    for (int64_t i = 0; i < MAX_SS_DAG_PRIO_TYPE_NUM; ++i) {
      total_thread_num += OB_DAG_PRIOS[dag_prio_type_for_ss[i]].score_;
    }
  }
  return total_thread_num;
#undef GET_THREAD_NUM
}

void ObTenantDagScheduler::ss_reload_config(const bool force_reload)
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    if (!tenant_config->_ob_enable_background_thread_auto_adapt
        || force_reload) {
      inner_reload_config();
    } else {
      set_thread_score(ObDagPrio::DAG_PRIO_LS_CKPT,
          GCONF.sys_ckpt_thread_score);
      // 这些共享存储维护优先级不受 adapt_ss_work_thread() 管理，需要刷新配置值，
      // 同时保留 compaction、upload 和 attach 优先级的自适应上限。
      set_thread_score(ObDagPrio::DAG_PRIO_MARK_SWEEP,
          tenant_config->shared_gc_generator_thread_score);
      set_thread_score(ObDagPrio::DAG_PRIO_SS_GC_GEN,
          tenant_config->shared_gc_generator_thread_score);
      set_thread_score(ObDagPrio::DAG_PRIO_SS_GC_EXEC,
          tenant_config->shared_gc_executor_thread_score);
      set_thread_score(ObDagPrio::DAG_PRIO_SS_DATA_CHECK,
          tenant_config->shared_storage_data_check_thread_score);
    }
  }
  if (is_sys_tenant(MTL_ID())) {
    set_thread_score(ObDagPrio::DAG_PRIO_FORK_DATABASE, GCONF._fork_database_prepare_thread_score);
  }
}

// only adapt the thread num used by shared storage: mini, upload, minor, attach
void ObTenantDagScheduler::adapt_ss_work_thread()
{
  int tmp_ret = OB_SUCCESS;
  int ret = OB_SUCCESS;
  RLOCAL_INIT(int64_t, last_update_timestamp, 0);
  int64_t current_timestamp = ObClockGenerator::getClock();
  int64_t time_gap = current_timestamp - last_update_timestamp;
  if (last_update_timestamp == 0) {
    last_update_timestamp = ObClockGenerator::getClock();
  } else if (time_gap >= ADAPT_WORK_THREAD_INTERVAL) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (!tenant_config.is_valid()) {
      // do nothing
      COMMON_LOG(INFO, "tenant config is invalid");
    } else if (!tenant_config->_ob_enable_background_thread_auto_adapt) {
      // do nothing, if the auto adapt is disabled
      COMMON_LOG(DEBUG, "auto adapt is disabled");
    } else {
      last_update_timestamp = current_timestamp;
      ObTaskPerfStat task_perf_stat[ObDagPrio::DAG_PRIO_MAX];
      MEMSET(task_perf_stat, 0, sizeof(task_perf_stat));

      // A DAG can override its type's initial priority, e.g. replace upload uses
      // MERGE_EXECUTE in the UPLOAD queue. Account both counts in the actual queue.
      for (int64_t i = 0; i < MAX_SS_DAG_PRIO_TYPE_NUM; ++i) {
        const int64_t dag_prio = dag_prio_type_for_ss[i];
        task_perf_stat[dag_prio].total_cnt_ = prio_sche_[dag_prio].get_dag_count();
        task_perf_stat[dag_prio].last_executed_cnt_ = get_scheduled_dag_cnts_by_prio(dag_prio);
      }
      // get last_ave_execute_time_ and total_estimate_execute_time_;
      int64_t total_estimate_execute_time = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX_SS_DAG_PRIO_TYPE_NUM; ++i) {
        const int64_t dag_prio = dag_prio_type_for_ss[i];
        if (task_perf_stat[dag_prio].total_cnt_ != 0
            && task_perf_stat[dag_prio].last_executed_cnt_ == 0) {
          task_perf_stat[dag_prio].last_ave_execute_time_ = time_gap;
        } else {
          task_perf_stat[dag_prio].last_ave_execute_time_ = (task_perf_stat[dag_prio].last_executed_cnt_ > 0
                                                     ? time_gap / task_perf_stat[dag_prio].last_executed_cnt_
                                                     : 0);
        }
        // ave time per thread, just use the adaptive num as the thread num.
        int64_t adaptive_limit = 0;
        if (OB_FAIL(get_adaptive_limit(dag_prio, adaptive_limit))) {
          COMMON_LOG(WARN, "get adaptive limit failed", K(ret), K(dag_prio));
        } else if (adaptive_limit <= 0) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "adaptive limit should not be 0", K(ret), K(dag_prio));
        } else {
          task_perf_stat[dag_prio].last_ave_execute_time_ /= adaptive_limit;
          task_perf_stat[dag_prio].total_estimate_execute_time_ = task_perf_stat[dag_prio].last_ave_execute_time_ * task_perf_stat[dag_prio].total_cnt_;
          total_estimate_execute_time += task_perf_stat[dag_prio].total_estimate_execute_time_;
        }
        COMMON_LOG(INFO, "auto adapt the thread", K(i), K(dag_prio), K(adaptive_limit), K(time_gap), K(task_perf_stat[dag_prio].last_executed_cnt_),
                   K(task_perf_stat[dag_prio].last_ave_execute_time_), K(task_perf_stat[dag_prio].total_estimate_execute_time_),
                   K(task_perf_stat[dag_prio].total_cnt_), K(total_estimate_execute_time));
      }
      // get the expected thread cnt, and set the thread cnt
      if (OB_FAIL(ret)) {
      } else if (total_estimate_execute_time == 0) {
        // adapt the thread num to origin.
        ss_reload_config(true /* force reload config */);
        COMMON_LOG(INFO, "reload config while there is no dag");
      } else {
        // adapt the thread num, at least has one thread. all the left thread will be set to ObDagPrio::DAG_PRIO_COMPACTION_HIGH.
        int64_t total_thread_num = get_ss_total_thread_num_();
        int64_t left_thread_num = total_thread_num;
        for (int64_t i = 0; i < MAX_SS_DAG_PRIO_TYPE_NUM; ++i) {
          const int64_t dag_prio = dag_prio_type_for_ss[i];
          int64_t expected_thread_num = (total_thread_num - MAX_SS_DAG_PRIO_TYPE_NUM) * task_perf_stat[dag_prio].total_estimate_execute_time_ / total_estimate_execute_time;
          if (OB_TMP_FAIL(set_adaptive_limit(dag_prio, expected_thread_num + 1))) {
            COMMON_LOG(WARN, "set adaptive limit failed", K(tmp_ret), K(dag_prio), K(expected_thread_num + 1));
          }
          left_thread_num -= (expected_thread_num + 1);
          COMMON_LOG(DEBUG, "expected thread num", K(i), K(dag_prio), K(expected_thread_num + 1), K(task_perf_stat[dag_prio].total_estimate_execute_time_), K(total_estimate_execute_time));
        }
        left_thread_num += (total_thread_num - MAX_SS_DAG_PRIO_TYPE_NUM) * task_perf_stat[ObDagPrio::DAG_PRIO_COMPACTION_HIGH].total_estimate_execute_time_ / total_estimate_execute_time;
        if (OB_TMP_FAIL(set_adaptive_limit(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, left_thread_num + 1))) {
          COMMON_LOG(WARN, "set adaptive limit failed", K(tmp_ret), K(ObDagPrio::DAG_PRIO_COMPACTION_HIGH), K(left_thread_num + 1));
        }
        COMMON_LOG(INFO, "total_thread_num", K(total_thread_num));
      }
    }
  }
}

} //namespace share
} //namespace oceanbase
#endif
