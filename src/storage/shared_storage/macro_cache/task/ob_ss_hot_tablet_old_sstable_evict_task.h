/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_HOT_TABLET_OLD_SSTABLE_EVICT_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_HOT_TABLET_OLD_SSTABLE_EVICT_TASK_H_

#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace blocksstable
{
  class MacroBlockId;
  class ObSSTable;
}
namespace storage
{
class ObSSMacroCacheMgr;
class ObSSTableArray;

struct ObSSHotTabletOldSSTableEvictInfo
{
public:
  ObSSHotTabletOldSSTableEvictInfo()
    : ls_id_(share::ObLSID::INVALID_LS_ID), transfer_seq_(-1), timestamp_us_(0), trace_id_()
  {}
  ObSSHotTabletOldSSTableEvictInfo(const share::ObLSID &ls_id, const int64_t transfer_seq)
    : ls_id_(ls_id), transfer_seq_(transfer_seq), timestamp_us_(common::ObTimeUtility::current_time_us())
  {
    trace_id_ = *ObCurTraceId::get_trace_id();
  }
  virtual ~ObSSHotTabletOldSSTableEvictInfo() {}
  bool is_valid() const { return ls_id_.is_valid() && (transfer_seq_ >= 0) && (timestamp_us_ > 0); }
  TO_STRING_KV(K_(ls_id), K_(transfer_seq), K_(timestamp_us), K_(trace_id));

public:
  share::ObLSID ls_id_;
  int64_t transfer_seq_;
  int64_t timestamp_us_;
  common::ObCurTraceId::TraceId trace_id_;
};


class ObSSHotTabletOldSSTableEvictCheckFn
{
public:
  explicit ObSSHotTabletOldSSTableEvictCheckFn(
      common::hash::ObHashMap<common::ObTabletID, ObSSHotTabletOldSSTableEvictInfo> &allow_evict_tablet_map,
      common::hash::ObHashSet<common::ObTabletID> &skip_tablet_set)
    : allow_evict_tablet_map_(allow_evict_tablet_map), skip_tablet_set_(skip_tablet_set)
  {}
  virtual ~ObSSHotTabletOldSSTableEvictCheckFn() {}
  int operator()(const common::hash::HashMapPair<common::ObTabletID, ObSSHotTabletOldSSTableEvictInfo> &entry);

private:
  common::hash::ObHashMap<common::ObTabletID, ObSSHotTabletOldSSTableEvictInfo> &allow_evict_tablet_map_;
  common::hash::ObHashSet<common::ObTabletID> &skip_tablet_set_;
};


class ObSSHotTabletOldSSTableEvictTask : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSHotTabletOldSSTableEvictTask(volatile bool &is_stopped);
  virtual ~ObSSHotTabletOldSSTableEvictTask();
  int init(ObSSMacroCacheMgr *macro_cache_mgr);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;
  int push_old_sstable_evict_task(const common::ObTabletID &tablet_id,
                                  const ObSSHotTabletOldSSTableEvictInfo &evict_info);

private:
  int do_work();
  int evict_old_sstable(const common::ObTabletID &tablet_id,
                        const ObSSTableArray &major_sstables,
                        common::hash::ObHashSet<blocksstable::MacroBlockId> &last_major_block_set,
                        int64_t &unused_blk_cnt,
                        bool &need_skip);
  void erase_skipped_tablet();
  int build_last_major_block_set(const ObSSTableArray &major_sstables,
                                 int64_t &last_major_snapshot_version,
                                 common::hash::ObHashSet<blocksstable::MacroBlockId> &last_major_block_set);
  int find_unused_data_blocks(const blocksstable::ObSSTable &sstable,
                              const common::hash::ObHashSet<blocksstable::MacroBlockId> &last_major_block_set,
                              common::ObIArray<blocksstable::MacroBlockId> &unused_data_blocks);
  void try_clear_expired_tablet();

private:
  class ObFindExpiredTabletFn
  {
  public:
    explicit ObFindExpiredTabletFn(common::hash::ObHashSet<common::ObTabletID> &expired_tablet_set)
      : expired_tablet_set_(expired_tablet_set)
    {}
    virtual ~ObFindExpiredTabletFn() {}
    int operator()(const common::hash::HashMapPair<common::ObTabletID, ObSSHotTabletOldSSTableEvictInfo> &entry);
  
  private:
    common::hash::ObHashSet<common::ObTabletID> &expired_tablet_set_;
  };

private:
  static const int64_t SCHEDULE_INTERVAL_US = 5 * 1000L * 1000L; // 5s
  static const int64_t EXPIRED_TIME_US = 3600L * 1000L * 1000L; // 1h
  ObSSMacroCacheMgr *macro_cache_mgr_;
  common::hash::ObHashMap<common::ObTabletID, ObSSHotTabletOldSSTableEvictInfo> tablet_info_map_;
  // all previous tablet versions' ref_cnt are reduced to 0
  common::hash::ObHashMap<common::ObTabletID, ObSSHotTabletOldSSTableEvictInfo> allow_evict_tablet_map_;
  // ls_id/transfer_seq is not match, or tablet is not exist, or major_sstables_cnt < 2
  common::hash::ObHashSet<common::ObTabletID> skip_tablet_set_;
  int64_t last_clear_timestamp_us_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_HOT_TABLET_OLD_SSTABLE_EVICT_TASK_H_ */
