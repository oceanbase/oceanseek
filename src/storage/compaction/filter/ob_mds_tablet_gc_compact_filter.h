/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_STORAGE_COMPACTION_FILTER_OB_MDS_TABLET_GC_COMPACT_FILTER_H_
#define OB_STORAGE_COMPACTION_FILTER_OB_MDS_TABLET_GC_COMPACT_FILTER_H_

#include "storage/compaction/ob_i_compaction_filter.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "share/ob_light_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_spin_rwlock.h"

namespace oceanbase
{
namespace blocksstable
{
class ObDatumRow;
} // namespace blocksstable
} // namespace oceanbase

namespace oceanbase
{
namespace compaction
{

// Row key for sharing GC decision among parallel major-merge threads:
// all MDS rows under the same user tablet incarnation share (tablet_id, transfer_scn).
struct ObMdsGcGroupKey final
{
public:
  ObMdsGcGroupKey()
    : tablet_id_(0),
      transfer_scn_(0)
  {}
  ObMdsGcGroupKey(const common::ObTabletID &tablet_id, const share::SCN &transfer_scn);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool is_valid() const;
  bool operator==(const ObMdsGcGroupKey &other) const;
  TO_STRING_KV(K_(tablet_id), K_(transfer_scn));

public:
  int64_t tablet_id_;
  uint64_t transfer_scn_;
};

struct ObMdsGcGroupNode final : public share::ObLightHashLink<ObMdsGcGroupNode>
{
  ObMdsGcGroupNode();
  bool contain(const ObMdsGcGroupKey &k) const;
  ObMdsGcGroupKey key_;
  int8_t decision_;
};

struct ObMdsGcGroupNodeAlloc final
{
  ObMdsGcGroupNode *alloc_value() const;
  void free_value(ObMdsGcGroupNode *p) const;
};

typedef share::ObLightHashMap<ObMdsGcGroupKey,
    ObMdsGcGroupNode,
    ObMdsGcGroupNodeAlloc,
    common::SpinRWLock,
    2048 /* BUCKETS_CNT */,
    2048 /* LOCKS_CNT */>
    ObMdsGcGroupDecisionMap;

struct ObMdsGcForkSnapshotNode final : public share::ObLightHashLink<ObMdsGcForkSnapshotNode>
{
  ObMdsGcForkSnapshotNode();
  bool contain(const ObMdsGcGroupKey &k) const;
  ObMdsGcGroupKey key_;
  int64_t fork_snapshot_;
};

struct ObMdsGcForkSnapshotNodeAlloc final
{
  ObMdsGcForkSnapshotNode *alloc_value() const;
  void free_value(ObMdsGcForkSnapshotNode *p) const;
};

typedef share::ObLightHashMap<ObMdsGcGroupKey,
    ObMdsGcForkSnapshotNode,
    ObMdsGcForkSnapshotNodeAlloc,
    common::SpinRWLock,
    2048 /* BUCKETS_CNT */,
    2048 /* LOCKS_CNT */>
    ObMdsGcForkSnapshotMap;

// Major / meta-major compaction row filter for LS_MDS_TABLET: drops rows whose
// (tablet_id, transfer_scn) group is reclaimable after tablet-status and local
// tablet metadata checks.
//
// Tablet-status (ObTabletCreateDeleteMdsUserData) rows: deserialize first; only when
// tablet_status is deleted-for-GC and delete_commit_version_ is older than the
// _mds_multi_row_retention-capped waterline and this replica's max-decided SCN
// do we call t3m peek.
// Other MDS types: read shared map; on HASH_NOT_EXIST they point-read the latest
// committed tablet-status row and publish that exact group decision if found. If the
// tablet-status row has already been recycled (OB_EMPTY_RESULT), they reclaim only the
// current row when its own commit_version is older than the max-decided-capped GC waterline.
// Final KEEP/REMOVE decisions are stored in ObLightHashMap (per-bucket SpinRWLock);
// the first successful insert wins and decision_ is not updated again.
//
// Multi-row MDS types (MediumCompactionInfo / TruncateInfo / TTLFilterInfo) also carry
// historical rows under the same live tablet incarnation. Their history is reclaimed by a
// tenant-level major waterline initialized once per filter, capped by _mds_multi_row_retention
// relative to tenant GTS.
// TTLFilterInfo 扫描并缓存源 Tablet 上全部
// 已提交 fork marker 的最大 snapshot；存在有效 marker 时，保留其 snapshot 之前的 TTL 历史。
// marker 扫描失败时保守保留当前 TTL 行。无效 fork marker 在提交版本越过 GC 水位后按 key 回收。
class ObMdsTabletGcCompactFilter final : public ObICompactionFilter
{
public:
  ObMdsTabletGcCompactFilter();
  virtual ~ObMdsTabletGcCompactFilter() override { reset(); }
  void reset();
  int init(const share::ObLSID &ls_id);
  virtual int filter(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const override;
  virtual CompactionFilterType get_filter_type() const override
  {
    return LS_MDS_TABLET_GC_FILTER;
  }

private:
  share::ObLSID ls_id_;
  mutable ObMdsGcGroupDecisionMap group_decision_map_;
  ObMdsGcGroupNodeAlloc group_node_alloc_;
  bool group_map_inited_;
  mutable ObMdsGcForkSnapshotMap fork_snapshot_map_;
  ObMdsGcForkSnapshotNodeAlloc fork_snapshot_node_alloc_;
  bool fork_snapshot_map_inited_;
  bool is_inited_;
  int64_t medium_filter_snapshot_;
  int64_t mds_filter_snapshot_;
  int64_t max_decided_scn_;
  int init_history_filter_snapshot_();
  int filter_multi_row_history_(
      uint16_t mds_type,
      const ObMdsGcGroupKey &key,
      const share::SCN &transfer_scn,
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const;
  int filter_medium_compaction_history_(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const;
  int filter_truncate_info_history_(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const;
  int filter_fork_src_history_(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const;
  int filter_ttl_filter_info_history_(
      const ObMdsGcGroupKey &key,
      const share::SCN &transfer_scn,
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const;
  int get_max_committed_fork_snapshot_(
      const ObMdsGcGroupKey &key,
      const share::SCN &transfer_scn,
      bool &read_succeeded,
      int64_t &fork_snapshot) const;
  int decide_create_del_row_(
      const ObMdsGcGroupKey &key,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const blocksstable::ObDatumRow &row,
      int8_t &effective_dec) const;
  int decide_other_row_(
      const ObMdsGcGroupKey &key,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const blocksstable::ObDatumRow &row,
      int8_t &effective_dec) const;
  int decide_from_tablet_status_lookup_(
      const ObMdsGcGroupKey &key,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      bool &status_found,
      bool &status_empty,
      int8_t &effective_dec) const;
  // Initialized once in init() (single-threaded). If any hashset fails
  // to allocate, init() propagates the error and the filter is rejected.
  // After init() succeeds, all four sets are usable for the lifetime of
  // the filter, so record_*() does not need to revalidate state.
  int init_recycle_summary_();
  void destroy_recycle_summary_();
  void log_recycle_summary_() const;
  void record_multi_row_recycled_(const uint16_t mds_type, const int64_t tablet_id) const;
  void record_group_recycled_(const ObMdsGcGroupKey &key) const;
  mutable common::ObSpinLock recycle_summary_lock_;
  mutable common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode> medium_recycled_tablets_;
  mutable common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode> truncate_recycled_tablets_;
  mutable common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode> ttl_recycled_tablets_;
  mutable common::hash::ObHashSet<ObMdsGcGroupKey, common::hash::NoPthreadDefendMode> group_recycled_keys_;
};

OB_INLINE ObMdsGcGroupKey::ObMdsGcGroupKey(
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn)
  : tablet_id_(tablet_id.id()),
    transfer_scn_(transfer_scn.get_val_for_inner_table_field())
{}

OB_INLINE uint64_t ObMdsGcGroupKey::hash() const
{
  uint64_t h = common::ObTabletID(tablet_id_).hash();
  h ^= transfer_scn_ + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
  return h;
}

OB_INLINE int ObMdsGcGroupKey::hash(uint64_t &hash_val) const
{
  hash_val = ObMdsGcGroupKey::hash();
  return OB_SUCCESS;
}

OB_INLINE bool ObMdsGcGroupKey::is_valid() const
{
  return common::ObTabletID(tablet_id_).is_valid();
}

OB_INLINE bool ObMdsGcGroupKey::operator==(const ObMdsGcGroupKey &other) const
{
  return tablet_id_ == other.tablet_id_ && transfer_scn_ == other.transfer_scn_;
}

OB_INLINE ObMdsGcGroupNode::ObMdsGcGroupNode()
  : share::ObLightHashLink<ObMdsGcGroupNode>(),
    key_(),
    decision_(0)
{}

OB_INLINE bool ObMdsGcGroupNode::contain(const ObMdsGcGroupKey &k) const
{
  return key_ == k;
}

OB_INLINE ObMdsGcForkSnapshotNode::ObMdsGcForkSnapshotNode()
  : share::ObLightHashLink<ObMdsGcForkSnapshotNode>(),
    key_(),
    fork_snapshot_(0)
{}

OB_INLINE bool ObMdsGcForkSnapshotNode::contain(const ObMdsGcGroupKey &k) const
{
  return key_ == k;
}

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_FILTER_OB_MDS_TABLET_GC_COMPACT_FILTER_H_
