/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_FORK_TABLE_SERVICE_H_
#define OCEANBASE_STORAGE_OB_FORK_TABLE_SERVICE_H_

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_pool.h"
#include "share/ob_fork_table_util.h"
#include "storage/high_availability/ob_fork_table_reserved_scn_operator.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace palf
{
class LogConfigVersion;
}
namespace obrpc
{
struct ObCheckForkTabletLandArg;
struct ObDDLBuildSingleReplicaRequestArg;
}
namespace storage
{
enum class ObForkDataPhase : int64_t;
struct ObTabletReorgInfoData;

class ObForkTableService final : public lib::ThreadPool
{
public:
  ObForkTableService();
  virtual ~ObForkTableService();
  static int mtl_init(ObForkTableService *&fork_table_service);

  int init(ObLSService *ls_service);
  void destroy();
  void run1() final;
  void wakeup();
  int submit_finish_meta_refresh(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
      const share::SCN &reorg_scn, const share::ObForkTabletInfo &fork_info);
  void stop();
  void wait();
  int start();
  int erase_reported_groups_for_ls(const share::ObLSID &ls_id);

private:
  struct ObForkReportGroupKey final
  {
    ObForkReportGroupKey() : ls_id_(), task_id_(0) {}
    ObForkReportGroupKey(const share::ObLSID &ls_id, const int64_t task_id) : ls_id_(ls_id), task_id_(task_id) {}
    bool is_valid() const { return ls_id_.is_valid() && task_id_ > 0; }
    uint64_t hash() const
    {
      uint64_t hash_val = ls_id_.hash();
      hash_val ^= static_cast<uint64_t>(task_id_) + 0x9e3779b97f4a7c15ULL + (hash_val << 6) + (hash_val >> 2);
      return hash_val;
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    bool operator==(const ObForkReportGroupKey &other) const
    {
      return ls_id_ == other.ls_id_ && task_id_ == other.task_id_;
    }
    TO_STRING_KV(K_(ls_id), K_(task_id));

    share::ObLSID ls_id_;
    int64_t task_id_;
  };

  struct ObForkTabletItem final
  {
    ObForkTabletItem() { reset(); }
    ~ObForkTabletItem() = default;
    void reset()
    {
      task_id_ = 0;
      tablet_id_.reset();
      reorg_scn_.reset();
      src_table_id_ = OB_INVALID_ID;
      dst_table_id_ = OB_INVALID_ID;
      src_schema_version_ = 0;
      dst_schema_version_ = 0;
      data_format_version_ = 0;
      fork_info_.reset();
      has_fork_info_ = false;
      tablet_exist_ = false;
      committed_fork_dst_ = false;
      committed_inactive_ = false;
      snapshot_match_ = false;
      local_done_ = false;
    }
    TO_STRING_KV(K_(task_id), K_(tablet_id), K_(reorg_scn), K_(src_table_id), K_(dst_table_id),
        K_(src_schema_version), K_(dst_schema_version), K_(data_format_version), K_(has_fork_info),
        K_(fork_info), K_(tablet_exist), K_(committed_fork_dst), K_(committed_inactive),
        K_(snapshot_match), K_(local_done));
    int64_t task_id_;
    common::ObTabletID tablet_id_;
    share::SCN reorg_scn_;
    uint64_t src_table_id_;
    uint64_t dst_table_id_;
    int64_t src_schema_version_;
    int64_t dst_schema_version_;
    uint64_t data_format_version_;
    share::ObForkTabletInfo fork_info_;
    bool has_fork_info_;
    bool tablet_exist_;
    bool committed_fork_dst_;
    bool committed_inactive_;
    bool snapshot_match_;
    bool local_done_;
  };

  struct ObForkFinishRefreshKey final
  {
    ObForkFinishRefreshKey() : ls_id_(), tablet_id_(), reorg_scn_(), fork_info_() {}
    ObForkFinishRefreshKey(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
        const share::SCN &reorg_scn, const share::ObForkTabletInfo &fork_info)
      : ls_id_(ls_id), tablet_id_(tablet_id), reorg_scn_(reorg_scn), fork_info_(fork_info) {}
    bool is_valid() const
    {
      return ls_id_.is_valid() && tablet_id_.is_valid() && reorg_scn_.is_valid() && fork_info_.is_valid()
          && fork_info_.is_data_complete() && !fork_info_.is_all_replicas_land_complete();
    }
    int hash(uint64_t &hash_val) const
    {
      const common::ObTabletID &src_tablet_id = fork_info_.get_src_tablet_id();
      const int64_t fork_snapshot_version = fork_info_.get_fork_snapshot_version();
      hash_val = common::murmurhash(&ls_id_, sizeof(ls_id_), 0);
      hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
      hash_val = common::murmurhash(&reorg_scn_, sizeof(reorg_scn_), hash_val);
      hash_val = common::murmurhash(&src_tablet_id, sizeof(src_tablet_id), hash_val);
      hash_val = common::murmurhash(&fork_snapshot_version, sizeof(fork_snapshot_version), hash_val);
      return OB_SUCCESS;
    }
    bool operator==(const ObForkFinishRefreshKey &other) const
    {
      return ls_id_ == other.ls_id_ && tablet_id_ == other.tablet_id_ && reorg_scn_ == other.reorg_scn_
          && fork_info_.get_src_tablet_id() == other.fork_info_.get_src_tablet_id()
          && fork_info_.get_fork_snapshot_version() == other.fork_info_.get_fork_snapshot_version();
    }
    TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(reorg_scn), K_(fork_info));

    share::ObLSID ls_id_;
    common::ObTabletID tablet_id_;
    share::SCN reorg_scn_;
    share::ObForkTabletInfo fork_info_;
  };

  struct ObForkTabletSchedState final
  {
    ObForkTabletSchedState() { reset(); }
    ~ObForkTabletSchedState() = default;
    void reset()
    {
      publish_sched_ts_ = 0;
      land_sched_ts_ = 0;
      ss_publish_done_ = false;
      done_fork_snapshot_.reset();
    }
    TO_STRING_KV(K_(publish_sched_ts), K_(land_sched_ts), K_(ss_publish_done), K_(done_fork_snapshot));
    int64_t publish_sched_ts_;
    int64_t land_sched_ts_;
    bool ss_publish_done_;
    share::SCN done_fork_snapshot_;
  };

  int get_ls_id_array_();
  int schedule_fork_table_();
  int process_ls_(
      const share::ObLSID &ls_id,
      const bool ss_probe_window,
      const bool scan_orphan);
  int scan_fork_reorg_infos_(
      ObLS *ls,
      const bool ss_probe_window,
      const bool scan_orphan);
  int collect_fork_tablet_item_(
      ObLS *ls,
      const ObTabletReorgInfoData &data,
      ObForkTabletItem &item);
  int reserve_gc_for_group_(
      const share::ObLSID &ls_id,
      const int64_t task_id,
      const common::ObIArray<ObForkTabletItem> &all_items,
      const common::ObIArray<int64_t> &item_idx,
      const int64_t expected_sswriter_epoch);
  static int probe_ss_fork_data_(const share::ObLSID &ls_id, const ObForkTabletItem &item, bool &ss_done,
      bool &fill_done_cache);
  int process_fork_group_(
      ObLS *ls,
      const int64_t task_id,
      const common::ObIArray<ObForkTabletItem> &all_items,
      const bool ss_probe_window);
  int release_fork_group_(
      const share::ObLSID &ls_id,
      const int64_t task_id,
      const int64_t expected_sswriter_epoch);
  int check_fork_group_reservation_exist_(const share::ObLSID &ls_id, const int64_t task_id,
      bool &reservation_exist) const;
  int check_orphan_reserved_scn_rows_(ObLS *ls, const common::ObIArray<int64_t> &active_task_ids);
  int check_ls_sswriter_lease_(
      const share::ObLSID &ls_id,
      bool &is_sswriter,
      int64_t &sswriter_epoch) const;
  int check_ls_sswriter_epoch_(
      const share::ObLSID &ls_id,
      const int64_t expected_sswriter_epoch,
      bool &is_same_epoch) const;
  int try_reserve_and_publish_fork_group_(
      const share::ObLSID &ls_id,
      const int64_t task_id,
      const common::ObIArray<ObForkTabletItem> &all_items,
      const common::ObIArray<int64_t> &item_idx,
      const common::ObIArray<int64_t> &publish_idx,
      const int64_t expected_sswriter_epoch);
  int try_release_and_report_fork_group_(
      ObLS *ls,
      const int64_t task_id,
      const common::ObIArray<ObForkTabletItem> &all_items,
      const common::ObIArray<int64_t> &report_idx,
      const int64_t expected_sswriter_epoch);
  int build_fork_land_check_idx_(
      const int64_t task_id,
      const common::ObIArray<ObForkTabletItem> &all_items,
      common::ObIArray<int64_t> &land_check_idx,
      bool &all_local_land_complete) const;
  int mark_all_replicas_fork_land_complete_(
      const share::ObLSID &ls_id,
      const common::ObIArray<ObForkTabletItem> &all_items,
      const common::ObIArray<int64_t> &land_check_idx,
      const int64_t expected_sswriter_epoch);
  int process_finish_meta_refresh_tasks_();
  int refresh_fork_all_complete_to_local_(ObLS *ls, const ObForkTabletItem &item);
  void erase_finish_meta_refresh_task_(const ObForkFinishRefreshKey &key);
  int refresh_fork_finish_group_to_local_(ObLS *ls, const common::ObIArray<ObForkTabletItem> &all_items,
      const common::ObIArray<int64_t> &item_idx);
  int build_fork_land_check_arg_(
      const share::ObLSID &ls_id,
      const common::ObIArray<ObForkTabletItem> &all_items,
      const common::ObIArray<int64_t> &report_idx,
      obrpc::ObCheckForkTabletLandArg &arg) const;
  int build_fork_land_check_addr_list_(
      const common::ObMemberList &member_list,
      const common::GlobalLearnerList &learner_list,
      common::ObIArray<common::ObAddr> &addr_list) const;
  int send_fork_land_check_rpc_(
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObAddr> &addr_list,
      const common::ObIArray<ObForkTabletItem> &all_items,
      const common::ObIArray<int64_t> &report_idx,
      bool &all_land_complete) const;
  int check_fork_tablet_land_complete_(
      const obrpc::ObForkTabletLandInfo &tablet_info,
      const share::ObForkTabletInfo &fork_info,
      bool &land_complete) const;
  int check_fork_land_membership_unchanged_(
      ObLS *ls,
      const palf::LogConfigVersion &config_version,
      const common::ObMemberList &member_list,
      const int64_t paxos_replica_num,
      const common::GlobalLearnerList &learner_list,
      bool &membership_unchanged) const;
  int check_all_replicas_fork_land_complete_(
      ObLS *ls,
      const common::ObIArray<ObForkTabletItem> &all_items,
      const common::ObIArray<int64_t> &report_idx,
      bool &all_land_complete) const;
  int try_schedule_fork_tablet_(const share::ObLSID &ls_id, const ObForkTabletItem &item,
      const ObForkDataPhase fork_phase, const int64_t expected_sswriter_epoch);
  void prepare_sched_round_();
  bool is_in_sched_cooldown_(const common::ObTabletID &tablet_id, const ObForkDataPhase fork_phase,
      const int64_t now_us);
  void stamp_sched_attempt_(const common::ObTabletID &tablet_id, const ObForkDataPhase fork_phase,
      const int64_t now_us);
  bool lookup_ss_done_cache_(
      const common::ObTabletID &tablet_id,
      const share::SCN &fork_snapshot);
  void fill_ss_done_cache_(
      const common::ObTabletID &tablet_id,
      const share::SCN &fork_snapshot);
  void erase_sched_state_(const common::ObTabletID &tablet_id);
  bool skip_group_by_report_cache_(const ObForkReportGroupKey &group_key,
      const common::ObIArray<ObForkTabletItem> &all_items, const common::ObIArray<int64_t> &item_idx);
  bool is_reported_group_(const ObForkReportGroupKey &group_key);
  void mark_reported_group_(const ObForkReportGroupKey &group_key);
  void erase_reported_group_(const ObForkReportGroupKey &group_key);
  int try_report_fork_complete_(
      const share::ObLSID &ls_id,
      const ObForkTabletItem &item,
      const int64_t expected_sswriter_epoch);
  int build_fork_table_dag_arg_(
      const share::ObLSID &ls_id,
      const ObForkTabletItem &item,
      obrpc::ObDDLBuildSingleReplicaRequestArg &arg);

private:
  static const int64_t SCHEDULER_WAIT_TIME_MS = 5 * 1000L;
  static const int64_t FORK_GC_RESERVE_TRANS_TIMEOUT_US = 5 * 1000 * 1000L;
  static const int64_t SS_PROBE_INTERVAL_US = 10 * 1000 * 1000L;
  static const int64_t FORK_DAG_SCHED_COOLDOWN_US = 10 * 1000 * 1000L;
  static const int64_t SCHED_STATE_MAP_BUCKET_NUM = 4096;
  static const int64_t SCHED_STATE_MAP_MAX_CNT = 100000;
  static const int64_t REPORTED_GROUP_SET_BUCKET_NUM = 1024;
  static const int64_t FINISH_REFRESH_SET_BUCKET_NUM = 4096;
  static const int64_t ORPHAN_SCAN_INTERVAL_US = 6 * 60 * 60 * 1000 * 1000L;
  bool is_inited_;
  common::ObThreadCond thread_cond_;
  int64_t wakeup_cnt_;
  ObLSService *ls_service_;
  common::ObArray<share::ObLSID> ls_id_array_;
  int64_t last_ss_probe_ts_;
  int64_t last_orphan_scan_ts_;
  common::hash::ObHashMap<common::ObTabletID, ObForkTabletSchedState> sched_state_map_;
  // Protected by thread_cond_ because LS destruction may erase entries outside the worker thread.
  common::hash::ObHashSet<ObForkReportGroupKey, common::hash::NoPthreadDefendMode> reported_group_set_;
  // Protected by thread_cond_ because replay threads submit tasks to the Fork worker.
  common::hash::ObHashSet<ObForkFinishRefreshKey, common::hash::NoPthreadDefendMode> finish_refresh_set_;
  DISALLOW_COPY_AND_ASSIGN(ObForkTableService);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_FORK_TABLE_SERVICE_H_
