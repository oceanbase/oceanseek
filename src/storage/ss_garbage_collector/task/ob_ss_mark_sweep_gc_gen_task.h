/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_GC_GEN_TASK_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_GC_GEN_TASK_H_

#include "lib/hash/ob_hashmap.h"
#include "storage/ss_garbage_collector/task/ob_ss_gc_task_base.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
class ObMySQLTransaction;
}
namespace blocksstable
{
class MacroBlockId;
}
namespace storage
{

class ObSSLSMeta;

class ObSSMarkSweepGCGenTask : public ObSSGCTaskBase
{
public:
  explicit ObSSMarkSweepGCGenTask(volatile bool &is_stopped);
  virtual ~ObSSMarkSweepGCGenTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;
  bool need_retry_mark_sweep_dag() const;
  void set_need_retry_mark_sweep_dag(const bool need_retry);

protected:
  virtual int check_if_need_do_gc_work(bool &is_need) override;
  // 隔离 checkpoint payload 读取，便于测试完整合并路径及发布并发场景。
  virtual int read_ls_ckpt_data_(
      const share::SCN &ckpt_meta_scn,
      ObSSLSMeta &ls_meta,
      common::ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys);
  virtual int check_dag_exist_(bool &is_exist);
  virtual int cancel_dag_();

private:
  int cancel_dag_if_exist_();
  int check_tenant_server_normal_(const int64_t server_id, bool &is_normal);
  int end_trans_(common::ObMySQLTransaction &trans, const bool want_commit, int &upstream_ret);
  int try_release_executor_();
  int try_acquire_executor_(bool &is_acquired,
                            bool &need_check_old_executor,
                            int64_t &old_executor_server_id);
  int try_takeover_executor_(const int64_t old_executor_server_id, bool &is_takeovered);
  int try_acquire_or_takeover_executor_(bool &is_acquired_or_takeovered);
  int fetch_new_round_id_(int64_t &round_id);
  int update_gc_meta_scn_(const int64_t round_id, const share::SCN &safe_meta_scn);
  int check_executor_and_cancel_dag_if_not_self_();
  int check_executor_and_round_id_(const int64_t round_id, bool &is_match);
  int get_all_ls_id_(const share::SCN &snapshot_meta_scn,
                     common::ObIArray<share::ObLSID> &ls_id_arr);
  int get_one_ls_all_meta_scn_(const share::ObLSID &ls_id,
                               const share::SCN &snapshot_meta_scn,
                               common::ObIArray<share::SCN> &ls_meta_scn_arr);
  int get_all_ls_ss_checkpoint_scn_(
      const share::SCN &snapshot_meta_scn,
      const share::SCN &safe_meta_scn,
      const common::ObIArray<share::SCN> &reserved_meta_scns,
      const share::SCN &ckpt_ss_checkpoint_scn,
      common::hash::ObHashMap<share::ObLSID, share::SCN> &ls_ss_checkpoint_scn_map);
  int study_tablet_meta_scn_(
      const share::SCN &snapshot_meta_scn,
      const common::hash::ObHashMap<share::ObLSID, share::SCN> &ls_ss_checkpoint_scn_map,
      ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr,
      common::ObIArray<ObSSGCMarkDeletedTablet> &mark_deleted_tablets);
  int study_tablet_meta_scn_for_ls_(
      const share::ObLSID &ls_id,
      const share::SCN &snapshot_meta_scn,
      const share::SCN &ls_ss_checkpoint_scn,
      ObSSGCTabletWithDelScnArray &gc_tablets,
      ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr,
      common::ObIArray<ObSSGCMarkDeletedTablet> &mark_deleted_tablets);
  int calc_min_meta_scn_(
      const share::SCN &snapshot_meta_scn,
      const ObSSGCTabletStudyResultArray &study_result_arr,
      ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr);
  int get_max_major_meta_scn_(
      const share::SCN &snapshot_meta_scn,
      const ObSSGCTablet &gc_tablet,
      const int64_t min_major_snapshot,
      share::SCN &max_major_meta_scn);
  int filter_by_ls_ss_checkpoint_scn_(
      const share::ObLSID &ls_id,
      const share::SCN &snapshot_meta_scn,
      const share::SCN &ls_ss_checkpoint_scn,
      const ObSSGCTabletWithDelScnArray &gc_tablets,
      ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr);
  // 防御异常：tablet 引用数量不得超过租户 tablet 规模上限。
  int check_ls_ckpt_tablet_meta_keys_count_(
      const common::ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys);
  // 从 PALF KV 稳定读取一代 LS checkpoint；尚未发布时返回成功且 has_ls_ckpt 为 false。
  // ckpt_meta_scn 用于 LS_CKPT_GC 生成；ss_checkpoint_scn 和 tablet_meta_keys 用于 MarkSweep。
  int read_ls_ckpt_with_retry_(
      share::SCN &ss_checkpoint_scn,
      common::ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys,
      share::SCN &ckpt_meta_scn,
      bool &has_ls_ckpt);
  // 将 checkpoint 中每个 Tablet 的精确引用位点合并到 study 结果。
  int merge_tablet_meta_scns_from_ckpt_(
      const share::SCN &snapshot_meta_scn,
      const common::ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys,
      ObSSGCTabletWithMetaScnArray &tablet_with_meta_scn_arr);
  int handle_ckpt_tablet_not_in_study_(
      const share::SCN &snapshot_meta_scn,
      const ObSSGCTablet &ckpt_tablet,
      const share::SCN &ckpt_tablet_meta_scn);
  // 在 snapshot_meta_scn 快照下遍历所有 LS，为每个 LS 的每个过期 ls_meta 版本生成一个
  // LS_META_GC 任务。回收边界由 ObSSGCUtil::calc_min_meta_scn(reserved_meta_scns,
  // all_meta_scns, safe_meta_scn) 计算。LS checkpoint 已内嵌完整 LS_META，不依赖
  // PALF 中的 checkpoint 对象定位位点保护原始 LS_META 行。所有严格早于最终边界的版本
  // 都会通过 ObSharedGCTaskUtil::generate_ls_meta_gc_task 落成任务。
  int generate_ls_meta_gc_task_(
      const share::SCN &snapshot_meta_scn,
      const share::SCN &safe_meta_scn,
      const common::ObIArray<share::SCN> &reserved_meta_scns);
  // 通过 ObSharedGCTaskLSMetaIterator 遍历本租户 __all_shared_gc_task 中所有 LS_META_GC
  // 任务，并在 ls_meta_max_scn_map 中为每个 ls_id 记录其最大的 meta_scn（即该 LS 待回收
  // ls_meta 版本的上界）。
  int calc_gc_ls_meta_max_scn_(
      common::hash::ObHashMap<share::ObLSID, share::SCN> &ls_meta_max_scn_map);
  // 为严格小于 min_meta_scn 的 SYS_LS checkpoint 生成持久化 GC 任务。
  // 复用本轮从 PALF KV 读取的有效 checkpoint 版本；未发布时由调用方跳过。
  int generate_ls_ckpt_gc_task_(
      const share::SCN &snapshot_meta_scn,
      const share::SCN &safe_meta_scn,
      const share::SCN &ckpt_meta_scn,
      const common::ObIArray<share::SCN> &reserved_meta_scns);
  // 记录 gc_task 表中已生成 checkpoint GC 任务的最大 meta_scn。
  int calc_gc_ls_ckpt_max_scn_(share::SCN &max_scn);

private:
  static const int64_t SCHEDULE_INTERVAL_US = 10 * 1000 * 1000L; // 10s
  static const int64_t CHECK_EXECUTOR_INTERVAL_US = 5LL * 60LL * 1000LL * 1000LL; // 5min
  static const int64_t DEFAULT_STUDY_TIMEOUT_US = 5LL * 60LL * 1000LL * 1000LL; // 5min
  static const int64_t DEFAULT_LS_META_MAX_SCN_MAP_BUCKET_NUM = 1024; // ls count per tenant is small
  bool need_retry_mark_sweep_dag_;
  int64_t last_check_executor_time_us_;
  // round_id fetched at the moment this server acquires/takes-over the executor role; carried
  // from check_if_need_do_gc_work into runTimerTask and stored into ObSSMarkSweepDagParam.
  int64_t round_id_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_MARK_SWEEP_GC_GEN_TASK_H_
