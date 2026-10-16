/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_SERVICE_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_SERVICE_H_

#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"
#include "storage/ss_garbage_collector/ob_ss_gc_task_runner.h"

namespace oceanbase
{
namespace storage
{

class ObSSGCService
{
public:
  ObSSGCService();
  virtual ~ObSSGCService();
  static int mtl_init(ObSSGCService *&gc_service);
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
  bool is_stopped() const { return ATOMIC_LOAD(&is_stopped_); }
  bool need_retry_mark_sweep_dag() const;
  void set_need_retry_mark_sweep_dag(const bool need_retry);
  void report_data_check_dag_result(
      const bool need_retry,
      const int64_t finish_ts);

  // 仅供同步 Study 调用；调用方从扫描 tablet 到本函数返回
  // 必须持续持有对应 LS handle，并传入扫描开始时的 LS epoch。
  // 持 study 锁按 LS epoch 和完整 StudyKey，
  // 分别更新 minor、major 的 finite 高水位。
  // tablet 生命周期变化产生的陈旧位点由后台任务清理。
  int check_and_update_local_tablet_study_info(
      const ObSSGCTablet &gc_tablet,
      const ObSSGCTabletStudyInfo &study_info,
      const int64_t ls_epoch);
  // 根据本地 LS 和 tablet 生命周期清理已经失效的 StudyKey。
  int cleanup_stale_local_tablet_study_info();
  // Track a deleted LS that still has tablet(s) not mark-deleted. Records the FIRST time such an
  // inconsistency is observed for the LS (keeps the original timestamp on later observations).
  // Logs ERROR once the duration reaches DEL_LS_WITH_UNDEL_TABLET_ERROR_THRESHOLD_US.
  int mark_del_ls_with_undel_tablet(const share::ObLSID &ls_id);
  // Erase the record once all tablets of the LS are mark-deleted (inconsistency resolved).
  int erase_del_ls_with_undel_tablet(const share::ObLSID &ls_id);
  // Fallback cleanup: every DEL_LS_MAP_CLEANUP_INTERVAL_US, drop records whose first_observed_us
  // is older than DEL_LS_MAP_CLEANUP_INTERVAL_US (e.g. LS already fully removed, no longer studied).
  int cleanup_stale_del_ls_with_undel_tablet();

private:
  struct TabletStudyKey final
  {
    TabletStudyKey() : tablet_id_(), transfer_scn_() {}
    TabletStudyKey(const common::ObTabletID &tablet_id, const share::SCN &transfer_scn)
      : tablet_id_(tablet_id), transfer_scn_(transfer_scn)
    {}
    ~TabletStudyKey() = default;
    bool is_valid() const;
    bool operator==(const TabletStudyKey &other) const;
    int hash(uint64_t &hash_val) const;
    TO_STRING_KV(K_(tablet_id), K_(transfer_scn));
    common::ObTabletID tablet_id_;
    share::SCN transfer_scn_;
  };

  typedef common::hash::ObHashMap<
      TabletStudyKey,
      ObSSGCTabletStudyInfo,
      common::hash::NoPthreadDefendMode> TabletStudyInfoMap;

  // 一个 context 只保存同一 LS 实例的 StudyKey。
  class LSStudyInfoCtx final
  {
  public:
    LSStudyInfoCtx();
    ~LSStudyInfoCtx();
    int init(const uint64_t tenant_id, const int64_t ls_epoch);
    void destroy();
    int64_t get_ls_epoch() const { return ls_epoch_; }
    TabletStudyInfoMap &get_tablet_study_info_map() { return tablet_study_info_map_; }
    const TabletStudyInfoMap &get_tablet_study_info_map() const
    {
      return tablet_study_info_map_;
    }

  private:
    bool is_inited_;
    int64_t ls_epoch_;
    TabletStudyInfoMap tablet_study_info_map_;
    DISALLOW_COPY_AND_ASSIGN(LSStudyInfoCtx);
  };

  struct TabletStudySnapshot final
  {
    TabletStudySnapshot() : gc_tablet_(), ls_epoch_(-1) {}
    TabletStudySnapshot(const ObSSGCTablet &gc_tablet, const int64_t ls_epoch)
      : gc_tablet_(gc_tablet), ls_epoch_(ls_epoch)
    {}
    ObSSGCTablet gc_tablet_;
    int64_t ls_epoch_;
    TO_STRING_KV(K_(gc_tablet), K_(ls_epoch));
  };

  typedef common::hash::ObHashMap<
      share::ObLSID,
      LSStudyInfoCtx *,
      common::hash::NoPthreadDefendMode> LSStudyInfoCtxMap;

  // 持锁检查 LS epoch，同 epoch 时更新已有 context。
  int update_ls_study_info_ctx_with_lock_(
      const ObSSGCTablet &gc_tablet,
      const ObSSGCTabletStudyInfo &study_info,
      const int64_t ls_epoch,
      LSStudyInfoCtx *&retired_study_ctx,
      bool &need_prepare_study_ctx);
  // 持锁复查 LS epoch，插入或替换锁外准备的 context。
  int insert_ls_study_info_ctx_with_lock_(
      const ObSSGCTablet &gc_tablet,
      const ObSSGCTabletStudyInfo &study_info,
      const int64_t ls_epoch,
      LSStudyInfoCtx *&pending_study_ctx,
      LSStudyInfoCtx *&retired_study_ctx);
  int create_ls_study_info_ctx_(const int64_t ls_epoch, LSStudyInfoCtx *&study_ctx);
  // 首次创建或切换 epoch 时，在锁外准备带首个 StudyKey 的 ctx。
  int prepare_ls_study_info_ctx_(
      const ObSSGCTablet &gc_tablet,
      const ObSSGCTabletStudyInfo &study_info,
      const int64_t ls_epoch,
      LSStudyInfoCtx *&pending_study_ctx);
  // 以下 without_lock 接口的调用方必须持有 tablet_study_info_lock_。
  int check_and_update_tablet_study_info_without_lock_(
      const ObSSGCTablet &gc_tablet,
      const ObSSGCTabletStudyInfo &study_info,
      TabletStudyInfoMap &tablet_study_info_map);
  int erase_local_tablet_study_info_without_lock_(
      const ObSSGCTablet &gc_tablet,
      const int64_t ls_epoch,
      LSStudyInfoCtx *&retired_study_ctx);
  int erase_ls_study_info_ctx_without_lock_(
      const share::ObLSID &ls_id,
      LSStudyInfoCtx *&study_ctx,
      LSStudyInfoCtx *&retired_study_ctx);
  int erase_empty_ls_study_info_ctx_without_lock_(
      const share::ObLSID &ls_id,
      LSStudyInfoCtx *&study_ctx,
      LSStudyInfoCtx *&retired_study_ctx);
  int check_local_tablet_study_info_stale_(
      const ObSSGCTablet &gc_tablet,
      const int64_t ls_epoch,
      bool &is_stale) const;
  int get_tablet_study_snapshots_without_lock_(
      common::ObIArray<TabletStudySnapshot> &study_snapshots) const;
  // 锁外 stale 仅用于初筛；位点写入与候选删除共用同一把锁，
  // 候选 key 必须在锁内重新核对后才能删除。
  int check_and_erase_stale_local_tablet_study_info_(
      const TabletStudySnapshot &study_snapshot,
      bool &is_erased);
  // 运行期 Context 先在锁内从外层 map 摘除，再在锁外销毁；
  // service destroy 在静默期批量释放。
  void destroy_ls_study_info_ctx_(LSStudyInfoCtx *&study_ctx);
  void destroy_ls_study_info_ctxs_();
  // 初始覆盖单 observer 每租户支持的最大 LS 数，外层 map 不自动扩容。
  static const int64_t LS_STUDY_INFO_CTX_MAP_BUCKET_NUM = 1024;
  // 与 ObLSTabletService 使用相同的固定 bucket 数，内层 map 不自动扩容。
  static const int64_t TABLET_STUDY_INFO_MAP_BUCKET_NUM = 10243L;
  static const int64_t DEL_LS_WITH_UNDEL_TABLET_ERROR_THRESHOLD_US = 24LL * 3600LL * 1000LL * 1000LL; // 1 day
  static const int64_t DEL_LS_MAP_CLEANUP_INTERVAL_US = 2LL * 24LL * 3600LL * 1000LL * 1000LL; // 2 days
  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  // runner of all kinds of gc tasks, e.g., MarkSweep GC, Fail GC...
  ObSSGCTaskRunner task_runner_;
  // record gc info (including min_meta_scn, reserved_meta_scns, ...) of each tablet.
  // can leverage this map for 'virtual table display' and 'min_meta_scn revert defence'.
  common::hash::ObHashMap<ObSSGCTablet, ObSSGCTabletInfo> tablet_info_map_;
  // LS epoch 与该 LS 的 StudyKey 在同一 context 内原子切换。
  // 外层 map 拥有 context，所有访问由 tablet_study_info_lock_ 串行；
  // 空 context 在锁内摘除，并在锁外销毁。
  LSStudyInfoCtxMap ls_study_info_ctx_map_;
  // 串行位点的比较更新与删除。
  lib::ObMutex tablet_study_info_lock_;
  // deleted LS -> first time observed it still has tablet(s) not mark-deleted.
  // used to ERROR-log after DEL_LS_WITH_UNDEL_TABLET_ERROR_THRESHOLD_US.
  common::hash::ObHashMap<share::ObLSID, int64_t> del_ls_with_undel_tablet_map_;
  int64_t last_del_ls_map_cleanup_time_us_;

};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_SERVICE_H_
