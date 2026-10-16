/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_RUNNER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_RUNNER_H_

#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_task_ctx.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_persist_micro_data_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_persist_micro_meta_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_release_cache_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_do_blk_checkpoint_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_monitor_task.h"
#include "storage/shared_storage/micro_cache/task/ob_ss_micro_cache_calibration_task.h"

namespace oceanbase
{
namespace storage
{
class ObTenantFileManager;
class ObSSPhysicalBlockManager;
class ObSSMemBlockManager;
class ObSSMicroMetaManager;
class ObSSMicroRangeManager;
class ObSSLAMicroKeyManager;

class ObSSMicroCacheTaskRunner
{
public:
  ObSSMicroCacheTaskRunner();
  virtual ~ObSSMicroCacheTaskRunner() {}

  int init(const uint64_t tenant_id, common::ObIAllocator &allocator, ObSSMemBlockManager &mem_blk_mgr,
           ObSSMicroMetaManager &micro_meta_mgr, ObSSMicroRangeManager &micro_range_mgr,
           ObSSPhysicalBlockManager &phy_blk_mgr, ObTenantFileManager &tnt_file_mgr,
           ObSSMicroCacheStat &cache_stat, ObSSLAMicroKeyManager &la_micro_mgr);
  int start();
  void stop();
  void wait();
  void destroy();

  int schedule_persist_data_task(const int64_t interval_us);
  int schedule_persist_meta_task(const int64_t interval_us);
  int schedule_do_blk_ckpt_task(const int64_t interval_us);
  int schedule_release_cache_task(const int64_t interval_us);
  int schedule_monitor_task(const int64_t interval_us);
  int schedule_calibration_task(const int64_t interval_us);

  void enable_replay_ckpt() { persist_meta_task_.enable_replay_ckpt(); }
  // For replay ckpt: disable common bg tasks + calibration (meta_ckpt runs replay)
  void disable_extra_task_for_ckpt()
  {
    disable_common_bg_tasks();
    disable_calibration();
  }
  void enable_extra_task_for_ckpt()
  {
    enable_common_bg_tasks();
    enable_calibration();
  }
  // For calibration: disable common bg tasks + meta_ckpt (calibration is executor)
  void disable_tasks_for_calibration()
  {
    disable_common_bg_tasks();
    disable_meta_ckpt();
  }
  void enable_tasks_for_calibration()
  {
    enable_common_bg_tasks();
    enable_meta_ckpt();
  }

  // Wait for persist_data, blk_ckpt, monitor to close. Don't wait for meta_ckpt and release_cache
  // because they share the same thread group with calibration task - they won't be scheduled when
  // calibration runs, so waiting would deadlock.
  int wait_tasks_close_for_calibration(const int64_t exp_timeout_us = SS_CALIBRATION_WAIT_TIMEOUT_US);
  // For operation/maintenance or test
  void enable_task()
  {
    enable_meta_ckpt();
    enable_calibration();
    enable_common_bg_tasks();
  }
  void disable_task()
  {
    disable_meta_ckpt();
    disable_calibration();
    disable_common_bg_tasks();
  }
  bool is_task_closed() const
  {
    return (is_meta_ckpt_closed() && is_blk_ckpt_closed() && is_persist_data_closed()
        && is_release_cache_closed() && is_monitor_closed() && is_calibration_closed());
  }
  // Used during replay failure path. Replay is executed by meta_ckpt_task on low_prio_tg,
  // so tasks sharing low_prio_tg (meta_ckpt, release_cache, calibration) cannot run while
  // this thread is blocked — no need to wait for them to close.
  // Only tasks on other threads (persist_data on high_prio_tg, blk_ckpt and monitor on
  // mid_prio_tg) need to be checked.
  bool is_task_closed_for_replay() const
  {
    return (is_blk_ckpt_closed() && is_persist_data_closed() && is_monitor_closed());
  }
  bool is_task_open() const
  {
    return ((!is_meta_ckpt_closed()) && (!is_blk_ckpt_closed()) && (!is_persist_data_closed())
        && (!is_release_cache_closed()) && (!is_monitor_closed()) && (!is_calibration_closed()));
  }

  TO_STRING_KV(K(is_meta_ckpt_closed()), K(is_blk_ckpt_closed()), K(is_persist_data_closed()),
      K(is_release_cache_closed()), K(is_monitor_closed()), K(is_calibration_closed()));

private:
  int init_task(const uint64_t tenant_id);
  int init_timer_thread();
  int start_timer_thread();
  void disable_common_bg_tasks();
  void enable_common_bg_tasks();

  OB_INLINE void enable_meta_ckpt() { persist_meta_task_.enable_meta_ckpt_op(); }
  OB_INLINE void disable_meta_ckpt() { persist_meta_task_.disable_meta_ckpt_op(); }
  OB_INLINE bool is_meta_ckpt_closed() const { return persist_meta_task_.is_meta_ckpt_op_closed(); }
  OB_INLINE void enable_blk_ckpt() { blk_ckpt_task_.enable_blk_ckpt_op(); }
  OB_INLINE void disable_blk_ckpt() { blk_ckpt_task_.disable_blk_ckpt_op(); }
  OB_INLINE bool is_blk_ckpt_closed() const { return blk_ckpt_task_.is_blk_ckpt_op_closed(); }
  OB_INLINE void enable_persist_data() { persist_data_task_.enable_persist_data_op(); }
  OB_INLINE void disable_persist_data() { persist_data_task_.disable_persist_data_op(); }
  OB_INLINE bool is_persist_data_closed() const { return persist_data_task_.is_persist_data_op_closed(); }
  OB_INLINE void enable_release_cache() 
  { 
    release_cache_task_.enable_evict_op(); 
    release_cache_task_.enable_reorganize_op(); 
  }
  OB_INLINE void disable_release_cache() 
  { 
    release_cache_task_.disable_evict_op(); 
    release_cache_task_.disable_reorganize_op(); 
  }
  OB_INLINE bool is_release_cache_closed() const 
  { 
    return release_cache_task_.is_evict_op_closed() && release_cache_task_.is_reorganize_op_closed(); 
  }
  OB_INLINE void enable_monitor() { monitor_task_.enable_monitor_op(); }
  OB_INLINE void disable_monitor() { monitor_task_.disable_monitor_op(); }
  OB_INLINE bool is_monitor_closed() const { return monitor_task_.is_monitor_op_closed(); }
  OB_INLINE void enable_calibration() { calibration_task_.enable_calibration_op(); }
  OB_INLINE void disable_calibration() { calibration_task_.disable_calibration_op(); }
  OB_INLINE bool is_calibration_closed() const { return calibration_task_.is_calibration_op_closed(); }

private:
  const static int64_t INVALID_TG_ID = -1;

private:
  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  int high_prio_tg_id_; // high priority timer thread, execute persist micro data/meta task
  int mid_prio_tg_id_;  // mid priority timer thread, execute blk_ckpt_task
  int low_prio_tg_id_;  // low priority timer thread, execute release_cache_task
  ObSSMicroCacheTaskCtx task_ctx_;
  ObSSPersistMicroDataTask persist_data_task_;
  ObSSPersistMicroMetaTask persist_meta_task_;
  ObSSReleaseCacheTask release_cache_task_;
  ObSSDoBlkCheckpointTask blk_ckpt_task_;
  ObSSMicroCacheMonitorTask monitor_task_;
  ObSSMicroCacheCalibrationTask calibration_task_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_TASK_RUNNER_H_ */