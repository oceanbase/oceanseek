/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_MANAGER_H_

#include "common/storage/ob_io_device.h"
#include "share/config/ob_server_config.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_ss_common_header.h"
#include "storage/shared_storage/common/ob_ss_common_info.h"
#include "storage/shared_storage/ob_disk_space_meta.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "share/ob_lease_struct.h"
#include "storage/shared_storage/ob_disk_space_limiter.h"
#include "storage/shared_storage/task/ob_check_expand_disk_size_task.h"
#include "storage/shared_storage/task/ob_persist_disk_space_task.h"
#include "share/ob_unit_getter.h"
#include "lib/time/ob_time_utility.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

namespace oceanbase
{
namespace storage
{
class ObMigrationOpArg;
class ObDiskSpaceManager final : public ObShareTenantDiskSpaceLimiterManager
{
public:
  static const int64_t DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH = 1024L * 1024L * 1024L; // tenant data_disk_size expand step length is 1GB
  int init(const int64_t total_disk_size);
  virtual void destroy() override;
  static ObDiskSpaceManager &get_instance();
  
  int resize(const int64_t new_total_disk_size);
  int alloc(const int64_t alloc_size);
  int free(const int64_t free_size);
  
  int reload_hidden_sys_data_disk_config(const ObServerConfig &server_config);
  int update_hidden_sys_data_disk_config_size(const int64_t new_hidden_sys_data_disk_config_size);
  int update_sys_tenant_data_disk_size(const int64_t new_data_disk_size, const bool is_hidden);
  
  OB_INLINE int64_t get_total_disk_size() const { return total_disk_size_; }
  OB_INLINE int64_t get_reserved_disk_size() const { return reserved_disk_size_; }
  // __all_virtual_server's DATA_DISK_CAPACITY
  OB_INLINE int64_t get_disk_size_capacity() const
  {
    return MAX(0, get_ss_cache_max_size_() - reserved_disk_size_ - get_hidden_sys_data_disk_size());
  }
  OB_INLINE int64_t get_free_disk_size() const
  {
    return MAX(0, total_disk_size_ - alloc_disk_size_ - reserved_disk_size_);
  }
  OB_INLINE int64_t get_hidden_sys_data_disk_config_size() const { return hidden_sys_data_disk_config_size_; }
  OB_INLINE int64_t get_hidden_sys_data_disk_size() const
  {
    return is_hidden_sys_ ? MAX(0, sys_tenant_data_disk_size_) : 0;
  }
  OB_INLINE int64_t get_real_sys_data_disk_size() const
  {
    return is_hidden_sys_ ? 0 : MAX(0, sys_tenant_data_disk_size_); 
  }
  static int get_used_disk_size(int64_t &used_disk_size);  // __all_virtual_server's DATA_DISK_IN_USE
  OB_INLINE int64_t get_data_disk_suggested_size() const { return data_disk_suggested_size_; }
  OB_INLINE share::DataDiskSuggestedOperationType::TYPE get_data_disk_suggested_operation() const { return data_disk_suggested_operation_; }
  
  static int get_tenant_pair_unallocated_disk_size(
      const uint64_t tenant_id, int64_t &tenant_pair_unallocated_disk_size);
  static int check_ls_migration_space_full(const ObMigrationOpArg &arg);
  static int cal_min_shrinkable_disk_size(
      const uint64_t tenant_id, int64_t &min_shrinkable_disk_size);

  TO_STRING_KV(K(is_inited_), K(total_disk_size_), K(reserved_disk_size_), K(alloc_disk_size_),
      K(hidden_sys_data_disk_config_size_), K(sys_tenant_data_disk_size_), K(is_hidden_sys_),
      K(data_disk_suggested_size_), K(data_disk_suggested_operation_),
      "suggested_operation",
      share::DataDiskSuggestedOperationType::get_str(data_disk_suggested_operation_));

private:
  enum DiskSizeOpType : uint8_t
  {
    ALLOC = 0,
    FREE = 1,
    RESIZE = 2,
  };

  void update_data_disk_suggested_info_(
      const int64_t new_data_disk_suggested_size,
      const int64_t old_total_disk_size,
      const int64_t old_alloc_disk_size);

  int64_t cal_data_disk_suggested_size_(const int64_t delta_alloc_disk_size = 0) const;
  int64_t get_ss_cache_max_size_() const;
  static int get_free_cpu_count_(double &free_cpu_count);
  static int cal_tenant_min_shrinkable_disk_size_(
      const uint64_t tenant_id, int64_t &min_shrinkable_disk_size);
  static int do_check_ls_migration_space_with_src_(
      const share::ObLSID &ls_id,
      const common::ObAddr &src_server,
      const int64_t tenant_pair_unallocated_disk_size,
      const bool allow_discard_hot_data_during_migration);

private:
  static const int64_t DEFAULT_SERVER_TENANT_ID_DISK_SIZE = 100L * 1024L * 1024L; // reserved 100MB disk size for server tenant id, used for writing slog file of server tenant
  static const int64_t MEMORY_TO_DATA_DISK_FACTOR = 2;
  static constexpr const double DEFAULT_DISK_SIZE_EXPAND_THRESHOLD_PERCENT = 0.9;
  static constexpr const double DEFAULT_DISK_SIZE_RESERVED_PERCPU = 2L * 1024L * 1024L * 1024L;  // 2GB
  
  ObDiskSpaceManager();
  ~ObDiskSpaceManager();

private:
  bool is_inited_;
  lib::ObMutex disk_size_lock_;
  int64_t total_disk_size_;
  int64_t reserved_disk_size_;
  int64_t alloc_disk_size_;
  int64_t hidden_sys_data_disk_config_size_;  // hidden sys's initial default value
  int64_t sys_tenant_data_disk_size_;
  bool is_hidden_sys_;
  int64_t data_disk_suggested_size_;  // 0 means cloud platform auto expands data disk; none-zero means cloud platform expands more than this value.
  share::DataDiskSuggestedOperationType::TYPE data_disk_suggested_operation_; // none, expand, shrink
};

enum class ObDiskSpaceType : uint8_t
{
  DIR = 0,
  FILE = 1
};

class ObTenantDiskSpaceManager final
{
public:
  static const int64_t PRINT_LOG_INTERVAL = 60L * 1000L * 1000L; // 1min
  ObTenantDiskSpaceManager();
  ~ObTenantDiskSpaceManager();
  
  // lifecycle
  int init(const uint64_t tenant_id, const int64_t data_disk_size);
  int start();
  void stop();
  void wait();
  void destroy();
  static int mtl_init(ObTenantDiskSpaceManager *&tenant_disk_space_manager);
  
  // disk space operations
  int alloc_file_size(const int64_t alloc_size,
                      const ObSSMacroCacheType cache_type,
                      const ObDiskSpaceType disk_space_type);
  int free_file_size(const int64_t free_size,
                     const ObSSMacroCacheType cache_type,
                     const ObDiskSpaceType disk_space_type);
  void set_first_calibration_done(const bool is_first_calibration_done);
  bool is_first_calibration_done() const { return ATOMIC_LOAD(&is_first_calibration_done_); }
  int shift_file_size(const ObSSMacroCacheType ori_cache_type,
                      const ObSSMacroCacheType new_cache_type,
                      const int64_t file_size);
  int calibrate_alloc_size(const int64_t alloc_size, const ObSSMacroCacheType cache_type);
  // Why this method exists:
  //
  // The non-atomic calibrate_alloc_size(target) path computes
  //   target = base + (cur_outside - initial)
  // where cur_outside is read in one lock acquisition, and target is then written
  // to used_ in a *second* lock acquisition. Any alloc/free that happens between
  // the two acquisitions has already updated stat.used_ and the limiter via
  // alloc_file_size / free_file_size, but it is NOT reflected in target. When
  // calibrate_alloc_size eventually runs:
  //   delta_used = target - cur_inside   // cur_inside includes the racing change
  //   force_update_used(delta_used)       // limiter is pushed below real usage
  //   used_ = target                      // racing change is overwritten / lost
  //
  // Concrete failure chain when a Y-byte alloc races in:
  //   1. alloc:        used_ += Y, limiter += Y                 (real & tracked)
  //   2. calibrate:    limiter -= Y (over-correction), used_ loses Y
  //                    -> limiter now Y below real on-disk usage
  //                    -> stat.used_ no longer accounts for the Y-byte file
  //   3. file deleted: free_file_size(Y) does limiter.release(Y) and tries
  //                    used_ -= Y, but used_ has no record of it, so it clamps
  //                    to 0 and adds Y to free_size_exceeded_sum_.
  //   4. check_variables_consistency sees limiter vs sum(used_) diverge > 10MB
  //      and fires FLOG_ERROR every second until the next calibration cycle.
  //
  // This method closes the window by reading cur and writing used_ under the
  // same disk_size_lock_, so any racing alloc/free is folded into observed_used_size
  // and shows up in final_target_size instead of being silently dropped:
  //   final_target_size = base_target_size + (cur_used_under_lock - initial_used_size)
  int calibrate_alloc_size_with_initial(
      const int64_t initial_used_size,
      const int64_t base_target_size,
      const ObSSMacroCacheType cache_type,
      int64_t &final_target_size,
      int64_t &observed_used_size);
  int calibrate_dir_info(const int64_t dir_num, const ObSSMacroCacheType cache_type);
  // Atomically apply dir calibration: final = scanned_dir_num + current_dir_num - initial_dir_num
  // under disk_size_lock_, so racing dir alloc/free between snapshot and apply is preserved.
  int calibrate_dir_info_with_initial(const int64_t initial_dir_num,
                                      const int64_t scanned_dir_num,
                                      const ObSSMacroCacheType cache_type);
  // Synchronize the disk space limiter's used size to match the ground-truth usage
  // derived from macro cache statistics.
  // This function corrects any drift between accounting (limiter) and actual usage (stats).
  int sync_used_size_to_macro_cache_stats();
  int need_expand_disk_size(bool &is_need_expand);
  int expand_data_disk_size(const int64_t expand_size);
  int need_shrink_disk_size(bool &is_need_shrink);
  int shrink_data_disk_size(const int64_t shrink_size);
  int cal_min_shrinkable_disk_size(int64_t &min_shrinkable_disk_size);
  int cal_cur_available_shrink_size(
      const int64_t macro_cache_cur_min_shrinkable_size, int64_t &cur_available_shrink_size);
  int set_config_data_disk_size(const int64_t new_config_data_disk_size);
  int sync_tenant_pair_unallocated_disk_size();
  void check_disk_space_status();
  
  // macro cache
  // Note on "config" vs. "allocated" getters:
  //
  // - get_unit_config_* APIs compute cache sizes and free space based on the *configured*
  //   tenant data disk size (unit config), even if the underlying cloud / disk layer
  //   has not yet fully resized the physical disk. Internally, they start from the
  //   currently allocated capacity and adjust it by the delta between
  //   unit_config_data_disk_size_ and total_disk_size_ to reflect the target configuration.
  //
  // - get_allocated_* APIs compute sizes and free space purely from the *actually
  //   allocated* tenant disk capacity and current usage, as reported by
  //   OB_SERVER_DISK_SPACE_MGR and the tenant_disk_space_limiter_. These reflect
  //   the real, currently available resources.
  //
  // In short:
  //   * get_unit_config_*   → "what we plan / expect to have" (config-based view)
  //   * get_allocated_* → "what we actually have right now" (runtime/physical view)
  int64_t get_unit_config_shared_macro_cache_size();
  int64_t get_allocated_shared_macro_cache_size();
  /**
   * Returns the tenant's macro cache capacity upper bound (in bytes),
   * computed from the tenant's current macro cache usage plus its proportional share
   * of the *configured/allocated* shared macro cache free space.
   *
   * Definition:
   *   tenant_capacity =
   *     get_tenant_macro_cache_used_size()
   *     + usage_ratio(tenant_id) * get_config/allocated_shared_macro_cache_free_size();
   *
   * where usage_ratio(tenant_id) splits the shared free space between tenants:
   *   - meta tenant: META_TENANT_DATA_DISK_SIZE_PERCENTAGE (e.g. 10%)
   *   - user tenant: 1 - META_TENANT_DATA_DISK_SIZE_PERCENTAGE (e.g. 90%)
   *   - sys tenant : 1.0 (sees the full shared pool)
   */
  int64_t get_unit_config_tnt_macro_cache_size_by_usage_ratio();
  int64_t get_allocated_tnt_macro_cache_size_by_usage_ratio();
  int64_t get_shared_macro_cache_used_size();
  int64_t get_tenant_macro_cache_used_size();
  int64_t get_unit_config_shared_macro_cache_free_size();
  int update_macro_cache_desc(const ObSSMacroCacheType cache_type, const ObSSMacroCacheDesc &cache_desc);
  int update_macro_cache_stat_info(const ObSSMacroCacheType cache_type, const ObSSMacroCacheStat &cache_stat, const int64_t dir_num, const int64_t dir_size);
  int get_macro_cache_stat(const ObSSMacroCacheType cache_type, ObSSMacroCacheStat &cache_stat);
  int get_dir_info(const ObSSMacroCacheType cache_type, int64_t &dir_size, int64_t &dir_num);
  int get_macro_cache_stats(common::ObIArray<ObFileStatInfo> &cache_stat_info);
  
  // metrics/getters
  int64_t get_total_disk_size();
  int get_theoretical_free_disk_size(
      const ObSSMacroCacheType cache_type,
      int64_t &theoretical_free_size);
  int get_used_disk_size(int64_t &used_disk_size);
  // the tenant's reserved_disk_size include dir_size(3%) and slog_size(100MB)
  OB_INLINE int64_t get_reserved_disk_size() const
  {
    return (total_disk_size_ * DEFAULT_RESERVED_DISK_SIZE_PERCENT) + RESERVED_SLOG_DISK_SIZE;
  }
  static int get_tenant_unit_data_disk_size(
    const uint64_t tenant_id,
    int64_t &config_data_disk_size,
    int64_t &actual_data_disk_size);

  TO_STRING_KV(K(is_inited_), K(tenant_id_), K(total_disk_size_),
      K(tg_id_), "macro_cache_stats",
      ObArrayWrap<ObSSMacroCacheStat>(macro_cache_stats_, SS_MACRO_CACHE_MAX_TYPE_VAL),
      "dir_sizes", ObArrayWrap<int64_t>(dir_sizes_, SS_MACRO_CACHE_MAX_TYPE_VAL),
      K(tenant_pair_unallocated_disk_size_), K(tenant_pair_unallocated_size_last_sync_us_),
      K(unit_config_data_disk_size_),
      "used_size_delta", get_used_size_delta_nolock_(),
      "free_size_exceeded_sum", ObArrayWrap<int64_t>(free_size_exceeded_sum_, SS_MACRO_CACHE_MAX_TYPE_VAL),
      "shift_size_exceeded_sum", ObArrayWrap<int64_t>(shift_size_exceeded_sum_, SS_MACRO_CACHE_MAX_TYPE_VAL),
      "free_dir_size_exceeded_sum", ObArrayWrap<int64_t>(free_dir_size_exceeded_sum_, SS_MACRO_CACHE_MAX_TYPE_VAL),
      "free_dir_num_exceeded_sum", ObArrayWrap<int64_t>(free_dir_num_exceeded_sum_, SS_MACRO_CACHE_MAX_TYPE_VAL),
      K(tenant_disk_space_limiter_), K(expand_diag_), K(shrink_diag_));

private:
  // only for test or maintenance
  int resize_total_disk_size_(const int64_t new_data_disk_size, bool &succ_resize);

  OB_INLINE int64_t get_disk_size_capacity_nolock_() const
  {
    return total_disk_size_ - get_reserved_disk_size();
  }

  static int64_t cal_delta_capacity_(const int64_t delta_disk_size)
  {
    return delta_disk_size * (1 - DEFAULT_RESERVED_DISK_SIZE_PERCENT);
  }
  int64_t get_tenant_pair_unallocated_disk_capacity_nolock_();
  int sync_tenant_pair_unallocated_disk_size_nolock_();
  void set_macro_cache_used_nolock_(const ObSSMacroCacheType cache_type, const int64_t new_used_size);
  // Calculate the delta between the total used disk space recorded by the limiter
  // and the sum of all macro cache used sizes from stats.
  // Theoretically, these two values should be equal:
  //   limiter.get_used() == sum(macro_cache_stats_[i].used_)
  // Returns: limiter_used - sum(macro_cache_stats_.used_)
  int64_t get_used_size_delta_nolock_() const;

  int calibrate_alloc_size_nolock_(const int64_t alloc_size, const ObSSMacroCacheType cache_type);
  
  int64_t get_tenant_macro_cache_used_size_nolock_() const;
  int64_t get_allocated_shared_macro_cache_free_size_nolock_() const;
  int64_t get_allocated_shared_macro_cache_size_nolock_() const;
  double get_tenant_macro_cache_usage_ratio_() const;
  int64_t get_free_exceeded_threshold_() const;

  struct DiskScalingDiagCommonInfo
  {
    DiskScalingDiagCommonInfo()
    {
      MEMSET(this, 0, sizeof(*this));
    }

    int64_t config_data_disk_size_;
    int64_t actual_data_disk_size_;
    int64_t disk_size_capacity_;
    int64_t allocated_macro_cache_size_;
    int64_t allocated_macro_cache_free_size_;
    int64_t allocated_macro_cache_used_size_;
    double allocated_macro_cache_usage_pct_;

    char unit_config_disk_str_[32];
    char unit_actual_disk_str_[32];
    char disk_cap_str_[32];
    char alloc_macro_size_str_[32];
    char alloc_macro_free_str_[32];
    char alloc_macro_used_str_[32];
  };

  // Diagnose info for disk scaling actions (expand/shrink).
  // - first_need_time_us_: the start time of the current "need_* == true" streak; reset to 0 when need_* turns false
  // - last_attempt_time_us_: last attempt time (set when calling expand/shrink)
  // - last_succ_time_us_: last successful time
  // - last_ret_: return code of the last attempt
  struct DiskScalingDiag
  {
    DiskScalingDiag()
      : first_need_time_us_(0),
        last_attempt_time_us_(0),
        last_succ_time_us_(0),
        last_ret_(OB_SUCCESS)
    {}

    void reset()
    {
      first_need_time_us_ = 0;
      last_attempt_time_us_ = 0;
      last_succ_time_us_ = 0;
      last_ret_ = OB_SUCCESS;
    }

    void mark_attempt()
    {
      last_attempt_time_us_ = common::ObTimeUtility::fast_current_time();
    }

    void mark_success()
    {
      first_need_time_us_ = 0;
      last_succ_time_us_ = common::ObTimeUtility::fast_current_time();
    }

    void mark_ret(const int ret)
    {
      last_ret_ = ret;
    }

    // Update the "need_* == true" start timestamp (first_need_time).
    // - When is_need becomes true and first_need_time is not set, set it to current time.
    // - When is_need is false, clear first_need_time.
    void refresh_first_need_time(const bool is_need)
    {
      if (is_need) {
        if (first_need_time_us_ <= 0) {
          first_need_time_us_ = common::ObTimeUtility::fast_current_time();
        }
      } else {
        first_need_time_us_ = 0;
      }
    }

    void get_stuck_info(
        const int64_t stuck_warn_interval_us,
        bool &is_stuck,
        int64_t &stuck_us) const
    {
      is_stuck = false;
      stuck_us = 0;
      if (first_need_time_us_ > 0) {
        stuck_us = common::ObTimeUtility::fast_current_time() - first_need_time_us_;
        is_stuck = (stuck_us >= stuck_warn_interval_us);
      }
    }

    TO_STRING_KV(K(first_need_time_us_), K(last_attempt_time_us_), K(last_succ_time_us_), KR(last_ret_));

    int64_t first_need_time_us_;
    int64_t last_attempt_time_us_;
    int64_t last_succ_time_us_;
    int last_ret_;
  };
  void check_disk_scaling_status_nolock_();
  void check_expand_status_nolock_(const DiskScalingDiagCommonInfo &common);
  void check_shrink_status_nolock_(const DiskScalingDiagCommonInfo &common);
  void check_variables_consistency_nolock_();

private:
  // reserved 3%. for meta_file and private_macro, the dir size (especially tablet_data dir and
  // tablet_meta dir) increases as more files written. to prevent current tenant's disk_size from
  // occupying other tenants' disk_size, reserve 3% for dir size.
  static constexpr const double DEFAULT_RESERVED_DISK_SIZE_PERCENT = 0.03;
  static constexpr const double DEFAULT_TENANT_DISK_SIZE_EXPAND_THRESHOLD_PCT = 0.9;
  static const int64_t RESERVED_SLOG_DISK_SIZE = 100L * 1024L * 1024L; // 100MB, used for writing slog file, because slog is written in appending
                                                                       // user tenant's ckpt use meta_file disk size, do not use reserved size
  static const int INVALID_TG_ID = -1;
  static constexpr int64_t TENANT_DISK_SIZE_MAX_SHRINK_STEP_PCT = 45; // 45%
  // if the time since last sync of tenant_pair_unallocated_disk_size exceeds this threshold,
  // trigger a forced sync when querying size
  static constexpr int64_t TENANT_PAIR_UNALLOCATED_SIZE_FORCE_SYNC_THRESHOLD_US = 3LL * common::S_US; // 3s
  // if the time since last sync of tenant_pair_unallocated_disk_size exceeds this threshold,
  // print error log when querying size
  static constexpr int64_t TENANT_PAIR_UNALLOCATED_SIZE_ERROR_LOG_THRESHOLD_US = common::MIN_US; // 1min

  static constexpr char DIAG_MOD_NAME[] = "SS_DISK_SPACE_MANAGER";
  static constexpr char DIAG_SUBMOD_EXPAND[] = "EXPAND";
  static constexpr char DIAG_SUBMOD_SHRINK[] = "SHRINK";
  static constexpr char DIAG_SUBMOD_CONSISTENCY[] = "CONSISTENCY";
  static constexpr int64_t NEED_EXPAND_STUCK_WARN_INTERVAL_US = 30LL * common::S_US; // 30s
  static constexpr int64_t NEED_SHRINK_STUCK_WARN_INTERVAL_US = 30LL * common::S_US; // 30s
  // used_size_delta = limiter_used - sum(macro_cache_stats_.used_)
  // Report ERROR when abs(delta) exceeds this threshold.
  static constexpr int64_t USED_SIZE_DELTA_ERROR_THRESHOLD = 100LL * common::MB; // 100MB

private:
  bool is_inited_;
  uint64_t tenant_id_;
  bool is_first_calibration_done_;
  lib::ObMutex disk_size_lock_;
  int64_t total_disk_size_;
  ObSSMacroCacheStat macro_cache_stats_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  int64_t dir_sizes_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  int64_t dir_nums_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  // Sum of delta when free_size exceeds macro_cache_stats_[idx].used_ during free_file_size
  int64_t free_size_exceeded_sum_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  // Sum of delta when file_size exceeds macro_cache_stats_[ori_idx].used_ during shift_file_size
  int64_t shift_size_exceeded_sum_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  int64_t free_dir_size_exceeded_sum_[SS_MACRO_CACHE_MAX_TYPE_VAL];
  int64_t free_dir_num_exceeded_sum_[SS_MACRO_CACHE_MAX_TYPE_VAL];

  int64_t unit_config_data_disk_size_;
  // tenant pair's config data disk size - tenant pair's actual data disk size
  int64_t tenant_pair_unallocated_disk_size_;
  int64_t tenant_pair_unallocated_size_last_sync_us_;
  ObDiskSpaceLimiter tenant_disk_space_limiter_;
  // Note: `ObPersistDiskSpaceTask` and `ObCheckExpandDiskTask` were moved here from the
  // file manager. Previously, the file manager hosted multiple periodic tasks on a single
  // thread, which could cause these two tasks to be blocked for long periods. Managing them
  // here avoids that contention and ensures they are triggered strictly at their configured
  // intervals, so disk-space persistence and auto-expansion happen in time; otherwise
  // capacity expansion may be delayed.
  int tg_id_;
  ObPersistDiskSpaceTask persist_disk_space_task_;
  ObCheckExpandDiskTask check_expand_disk_task_;

  DiskScalingDiag expand_diag_;
  DiskScalingDiag shrink_diag_;
};

} // namespace storage
} // namespace oceanbase

#define OB_SERVER_DISK_SPACE_MGR (oceanbase::storage::ObDiskSpaceManager::get_instance())

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_MANAGER_H_ */
