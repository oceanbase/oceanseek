/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "ob_mds_tablet_gc_compact_filter.h"

#include "lib/allocator/ob_malloc.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_errno.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/ls/ob_ls.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/meta_mem/ob_tablet_attr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_fork_src_mds_data.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_ts_mgr.h"

namespace oceanbase
{
namespace compaction
{
using namespace storage;
namespace
{

enum ObMdsGcDecision : int8_t
{
  DEC_KEEP = 0,
  DEC_REMOVE = 1,
};

constexpr int64_t NS_PER_SEC = 1000L * 1000L * 1000L;

constexpr uint16_t tablet_create_delete_mds_type()
{
  return static_cast<uint16_t>(
      mds_type_id_detail::TypeIdOf<mds::DummyKey, ObTabletCreateDeleteMdsUserData>::value);
}

constexpr uint16_t medium_compaction_info_mds_type()
{
  return static_cast<uint16_t>(
      mds_type_id_detail::TypeIdOf<compaction::ObMediumCompactionInfoKey,
          compaction::ObMediumCompactionInfo>::value);
}

constexpr uint16_t truncate_info_mds_type()
{
  return static_cast<uint16_t>(
      mds_type_id_detail::TypeIdOf<ObTruncateInfoKey, ObTruncateInfo>::value);
}

constexpr uint16_t ttl_filter_info_mds_type()
{
  return static_cast<uint16_t>(
      mds_type_id_detail::TypeIdOf<ObTTLFilterInfoKey, ObTTLFilterInfo>::value);
}

constexpr uint16_t fork_src_mds_type()
{
  return static_cast<uint16_t>(
      mds_type_id_detail::TypeIdOf<ObTabletForkSrcMdsKey, ObTabletForkSrcMdsData>::value);
}

constexpr int64_t MDS_TABLET_GC_RECYCLE_SUMMARY_BUCKET_CNT = 256;
constexpr int64_t MDS_TABLET_GC_RECYCLE_LOG_BATCH_CNT = 100;
constexpr int64_t MDS_TABLET_GC_POINT_READ_TIMEOUT_US = 10L * 1000L * 1000L;
// Sized to hold one full batch with margin: 100 * (20 char tablet_id + 1
// char "%lu" upper bound for transfer_scn + ":,") < 4 KB. 8 KB leaves
// headroom even if an entry serializes oddly.
constexpr int64_t MDS_TABLET_GC_RECYCLE_LOG_BUF_SIZE = 8192;

void log_recycled_tablet_ids_in_batches_(
    const share::ObLSID &ls_id,
    const char *recycle_kind,
    const uint16_t mds_type,
    const int64_t filter_snapshot,
    const int64_t total_cnt,
    const common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode> &tablet_ids)
{
  if (total_cnt > 0) {
    char buf[MDS_TABLET_GC_RECYCLE_LOG_BUF_SIZE];
    int64_t batch_idx = 0;
    int64_t batch_cnt = 0;
    int64_t pos = 0;
    typedef common::hash::ObHashSet<int64_t,
        common::hash::NoPthreadDefendMode>::const_iterator ConstIter;
    for (ConstIter iter = tablet_ids.begin(); iter != tablet_ids.end(); ++iter) {
      const int64_t tablet_id = iter->first;
      if (0 == batch_cnt) {
        pos = 0;
      }
      const int tmp_ret = databuff_printf(buf, sizeof(buf), pos,
          0 == batch_cnt ? "%ld" : ",%ld", tablet_id);
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN_RET(tmp_ret, "failed to format recycled tablet id",
            K(ls_id), "recycle_kind", recycle_kind, K(tablet_id));
        continue;
      }
      ++batch_cnt;
      if (batch_cnt >= MDS_TABLET_GC_RECYCLE_LOG_BATCH_CNT) {
        LOG_INFO("[DIAG_RECYCLE] ls mds tablet gc multi-row recycle detail",
            K(ls_id),
            "recycle_kind", recycle_kind,
            K(mds_type),
            K(filter_snapshot),
            K(total_cnt),
            K(batch_idx),
            K(batch_cnt),
            "tablet_ids", ObString(pos, buf));
        ++batch_idx;
        batch_cnt = 0;
      }
    }
    if (batch_cnt > 0) {
      LOG_INFO("[DIAG_RECYCLE] ls mds tablet gc multi-row recycle detail",
          K(ls_id),
          "recycle_kind", recycle_kind,
          K(mds_type),
          K(filter_snapshot),
          K(total_cnt),
          K(batch_idx),
          K(batch_cnt),
          "tablet_ids", ObString(pos, buf));
    }
  }
}

void log_recycled_group_keys_in_batches_(
    const share::ObLSID &ls_id,
    const int64_t medium_filter_snapshot,
    const int64_t mds_filter_snapshot,
    const int64_t total_cnt,
    const common::hash::ObHashSet<ObMdsGcGroupKey, common::hash::NoPthreadDefendMode> &group_keys)
{
  if (total_cnt > 0) {
    char buf[MDS_TABLET_GC_RECYCLE_LOG_BUF_SIZE];
    int64_t batch_idx = 0;
    int64_t batch_cnt = 0;
    int64_t pos = 0;
    typedef common::hash::ObHashSet<ObMdsGcGroupKey,
        common::hash::NoPthreadDefendMode>::const_iterator ConstIter;
    for (ConstIter iter = group_keys.begin(); iter != group_keys.end(); ++iter) {
      const ObMdsGcGroupKey &key = iter->first;
      if (0 == batch_cnt) {
        pos = 0;
      }
      const int tmp_ret = databuff_printf(buf, sizeof(buf), pos,
          0 == batch_cnt ? "%ld:%lu" : ",%ld:%lu", key.tablet_id_, key.transfer_scn_);
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN_RET(tmp_ret, "failed to format recycled group key", K(ls_id), K(key));
        continue;
      }
      ++batch_cnt;
      if (batch_cnt >= MDS_TABLET_GC_RECYCLE_LOG_BATCH_CNT) {
        LOG_INFO("[DIAG_RECYCLE] ls mds tablet gc group recycle detail",
            K(ls_id),
            K(medium_filter_snapshot),
            K(mds_filter_snapshot),
            K(total_cnt),
            K(batch_idx),
            K(batch_cnt),
            "group_keys", ObString(pos, buf));
        ++batch_idx;
        batch_cnt = 0;
      }
    }
    if (batch_cnt > 0) {
      LOG_INFO("[DIAG_RECYCLE] ls mds tablet gc group recycle detail",
          K(ls_id),
          K(medium_filter_snapshot),
          K(mds_filter_snapshot),
          K(total_cnt),
          K(batch_idx),
          K(batch_cnt),
          "group_keys", ObString(pos, buf));
    }
  }
}

int8_t read_group_decision_from_map_(
    ObMdsGcGroupDecisionMap &map,
    const ObMdsGcGroupKey &key)
{
  int8_t dec = DEC_KEEP;
  ObMdsGcGroupNode *n = nullptr;
  const int gr = map.get(key, n);
  if (OB_SUCCESS == gr) {
    dec = ATOMIC_LOAD_RLX(&(n->decision_));
    map.revert(n);
  }
  return dec;
}

int publish_group_decision_light_(
    ObMdsGcGroupDecisionMap &map,
    const ObMdsGcGroupNodeAlloc &alloc,
    const ObMdsGcGroupKey &key,
    const int8_t decision,
    int8_t &effective_dec)
{
  int ret = OB_SUCCESS;
  ObMdsGcGroupNode *node = nullptr;
  if (OB_SUCCESS == map.get(key, node)) {
    effective_dec = ATOMIC_LOAD_RLX(&(node->decision_));
    map.revert(node);
  } else {
    ObMdsGcGroupNode *new_node = alloc.alloc_value();
    if (OB_ISNULL(new_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mds gc group node failed", KR(ret), K(key));
    } else {
      new_node->key_ = key;
      ATOMIC_STORE_RLX(&(new_node->decision_), decision);
      if (OB_SUCCESS == map.insert(key, new_node)) {
        effective_dec = decision;
      } else {
        alloc.free_value(new_node);
        effective_dec = read_group_decision_from_map_(map, key);
      }
    }
  }
  return ret;
}

bool read_fork_snapshot_from_map_(
    ObMdsGcForkSnapshotMap &map,
    const ObMdsGcGroupKey &key,
    int64_t &fork_snapshot)
{
  bool found = false;
  ObMdsGcForkSnapshotNode *node = nullptr;
  if (OB_SUCCESS == map.get(key, node)) {
    fork_snapshot = ATOMIC_LOAD_RLX(&(node->fork_snapshot_));
    found = true;
    map.revert(node);
  }
  return found;
}

int publish_fork_snapshot_light_(
    ObMdsGcForkSnapshotMap &map,
    const ObMdsGcForkSnapshotNodeAlloc &alloc,
    const ObMdsGcGroupKey &key,
    const int64_t fork_snapshot,
    int64_t &effective_snapshot)
{
  int ret = OB_SUCCESS;
  effective_snapshot = fork_snapshot;
  ObMdsGcForkSnapshotNode *node = nullptr;
  if (OB_SUCCESS == map.get(key, node)) {
    effective_snapshot = ATOMIC_LOAD_RLX(&(node->fork_snapshot_));
    map.revert(node);
  } else {
    ObMdsGcForkSnapshotNode *new_node = alloc.alloc_value();
    if (OB_ISNULL(new_node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mds gc fork snapshot node failed", KR(ret), K(key));
    } else {
      new_node->key_ = key;
      ATOMIC_STORE_RLX(&(new_node->fork_snapshot_), fork_snapshot);
      const int insert_ret = map.insert(key, new_node);
      if (OB_SUCCESS == insert_ret) {
        effective_snapshot = fork_snapshot;
      } else {
        alloc.free_value(new_node);
        if (!read_fork_snapshot_from_map_(map, key, effective_snapshot)) {
          LOG_WARN_RET(insert_ret, "failed to cache fork snapshot", K(key), K(fork_snapshot));
        }
      }
    }
  }
  return ret;
}

int check_local_tablet_reclaimable_after_deleted_status_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &group_transfer_scn,
    bool &reclaimable)
{
  int ret = OB_SUCCESS;
  reclaimable = false;

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  if (OB_UNLIKELY(!ls_id.is_valid()
               || !tablet_id.is_valid()
               || !group_transfer_scn.is_valid()
               || tablet_id.is_ls_inner_tablet())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(group_transfer_scn));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("t3m is null", K(ret));
  } else {
    const ObTabletMapKey key(ls_id, tablet_id);
    ObTabletFastIterAttr iter_attr;
    share::SCN pointer_reorg_scn;
    const int peek_ret = t3m->get_tablet_attr_for_compaction_filter(
        key, iter_attr, pointer_reorg_scn);
    if (OB_ENTRY_NOT_EXIST == peek_ret) {
      // Caller must have already proven from LS_MDS_TABLET that this status
      // is deleted-for-GC and older than the retention waterline. Under that
      // precondition, no local pointer means no local tablet can read it.
      reclaimable = true;
    } else if (OB_SUCCESS != peek_ret) {
      ret = peek_ret;
      LOG_WARN("failed to peek tablet pointer attr; keep group",
          K(ret), K(ls_id), K(tablet_id));
    } else if (!iter_attr.valid_) {
      // A CAS is mid-flight and the attr was observed in its reset state.
      reclaimable = false;
      LOG_INFO("tablet pointer attr is mid-CAS, keep group for next round",
          K(ls_id), K(tablet_id), K(group_transfer_scn));
    } else if (!pointer_reorg_scn.is_valid()
               || pointer_reorg_scn != group_transfer_scn) {
      // The pointer belongs to a different tablet lifetime, so this group is
      // from a retired lifetime on this LS.
      reclaimable = true;
    } else if (iter_attr.is_empty_shell_) {
      // The live tablet is already empty-shell on this replica. The delete
      // commit scn is cached in tablet_meta, so the MDS group is no longer
      // needed by local empty-shell handling.
      reclaimable = true;
    } else {
      // Transient DELETED-but-not-empty-shell. ObEmptyShellTask on this
      // replica may still need the DELETED user_data row.
      reclaimable = false;
    }
  }
  return ret;
}

int8_t peek_t3m_gc_decision_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn)
{
  int8_t dec = DEC_KEEP;
  bool reclaimable = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_local_tablet_reclaimable_after_deleted_status_(
          ls_id, tablet_id, transfer_scn, reclaimable))) {
    LOG_WARN("ls mds tablet gc peek failed, keep group conservatively",
        KR(ret),
        K(ls_id),
        K(tablet_id),
        K(transfer_scn));
  } else {
    dec = reclaimable ? DEC_REMOVE : DEC_KEEP;
  }
  return dec;
}

int get_normalized_commit_version_(
    const blocksstable::ObDatumRow &row,
    int64_t &commit_version)
{
  int ret = OB_SUCCESS;
  using MDS_COL = ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX;
  const int64_t raw_commit_version =
      row.storage_datums_[static_cast<int64_t>(MDS_COL::COMMIT_VERSION)].get_int();
  commit_version = ObMdsTabletSchemaDef::normalize_commit_version_for_tx(raw_commit_version);
  if (OB_UNLIKELY(commit_version <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls mds tablet row commit version must be positive",
        KR(ret), K(raw_commit_version), K(commit_version), K(row));
  }
  return ret;
}

bool is_deleted_for_gc_before_snapshot_(
    const ObTabletCreateDeleteMdsUserData &user_data,
    const int64_t mds_filter_snapshot)
{
  return user_data.tablet_status_.is_deleted_for_gc()
      && mds_filter_snapshot > 0
      && user_data.delete_commit_version_ > 0
      && user_data.delete_commit_version_ < mds_filter_snapshot;
}

} // namespace

ObMdsGcGroupNode *ObMdsGcGroupNodeAlloc::alloc_value() const
{
  void *p = ob_malloc(sizeof(ObMdsGcGroupNode),
      ObMemAttr(MTL_ID(), "MdsTGCGrp", common::ObCtxIds::DEFAULT_CTX_ID));
  ObMdsGcGroupNode *n = nullptr;
  if (OB_NOT_NULL(p)) {
    n = new (p) ObMdsGcGroupNode();
  }
  return n;
}

void ObMdsGcGroupNodeAlloc::free_value(ObMdsGcGroupNode *p) const
{
  if (OB_NOT_NULL(p)) {
    p->~ObMdsGcGroupNode();
    ob_free(p);
  }
}

ObMdsGcForkSnapshotNode *ObMdsGcForkSnapshotNodeAlloc::alloc_value() const
{
  void *p = ob_malloc(sizeof(ObMdsGcForkSnapshotNode),
      ObMemAttr(MTL_ID(), "MdsTGCFork", common::ObCtxIds::DEFAULT_CTX_ID));
  ObMdsGcForkSnapshotNode *node = nullptr;
  if (OB_NOT_NULL(p)) {
    node = new (p) ObMdsGcForkSnapshotNode();
  }
  return node;
}

void ObMdsGcForkSnapshotNodeAlloc::free_value(ObMdsGcForkSnapshotNode *p) const
{
  if (OB_NOT_NULL(p)) {
    p->~ObMdsGcForkSnapshotNode();
    ob_free(p);
  }
}

ObMdsTabletGcCompactFilter::ObMdsTabletGcCompactFilter()
  : ObICompactionFilter(),
    ls_id_(),
    group_decision_map_(),
    group_node_alloc_(),
    group_map_inited_(false),
    fork_snapshot_map_(),
    fork_snapshot_node_alloc_(),
    fork_snapshot_map_inited_(false),
    is_inited_(false),
    medium_filter_snapshot_(0),
    mds_filter_snapshot_(0),
    max_decided_scn_(0),
    recycle_summary_lock_(common::ObLatchIds::OB_MERGE_FILTER_CTX_LOCK),
    medium_recycled_tablets_(),
    truncate_recycled_tablets_(),
    ttl_recycled_tablets_(),
    group_recycled_keys_()
{}

int ObMdsTabletGcCompactFilter::init_recycle_summary_()
{
  // Called from init() before any filter() invocation, so single-threaded
  // and lock-free. The four hashsets are required: if any allocation fails,
  // the caller (init()) aborts and the filter is not used.
  int ret = OB_SUCCESS;
  const ObMemAttr attr(MTL_ID(), "MdsTGCRcy");
  if (OB_FAIL(medium_recycled_tablets_.create(MDS_TABLET_GC_RECYCLE_SUMMARY_BUCKET_CNT, attr))) {
    LOG_WARN("failed to create medium_recycled_tablets set", KR(ret), K_(ls_id));
  } else if (OB_FAIL(truncate_recycled_tablets_.create(MDS_TABLET_GC_RECYCLE_SUMMARY_BUCKET_CNT, attr))) {
    LOG_WARN("failed to create truncate_recycled_tablets set", KR(ret), K_(ls_id));
  } else if (OB_FAIL(ttl_recycled_tablets_.create(MDS_TABLET_GC_RECYCLE_SUMMARY_BUCKET_CNT, attr))) {
    LOG_WARN("failed to create ttl_recycled_tablets set", KR(ret), K_(ls_id));
  } else if (OB_FAIL(group_recycled_keys_.create(MDS_TABLET_GC_RECYCLE_SUMMARY_BUCKET_CNT, attr))) {
    LOG_WARN("failed to create group_recycled_keys set", KR(ret), K_(ls_id));
  }
  if (OB_FAIL(ret)) {
    // Roll back any partial allocations so destroy_recycle_summary_() does
    // not double-free. ObHashSet::destroy() is idempotent on uninitialized
    // sets, so calling on all four is safe.
    (void)medium_recycled_tablets_.destroy();
    (void)truncate_recycled_tablets_.destroy();
    (void)ttl_recycled_tablets_.destroy();
    (void)group_recycled_keys_.destroy();
  }
  return ret;
}

void ObMdsTabletGcCompactFilter::log_recycle_summary_() const
{
  // Single-threaded (called from destroy_recycle_summary_() in reset()/dtor),
  // so iteration is lock-free. Empty/uninitialized sets report size 0 and
  // are silently skipped.
  const int64_t group_cnt = group_recycled_keys_.size();
  const int64_t medium_cnt = medium_recycled_tablets_.size();
  const int64_t truncate_cnt = truncate_recycled_tablets_.size();
  const int64_t ttl_cnt = ttl_recycled_tablets_.size();
  if (group_cnt > 0 || medium_cnt > 0 || truncate_cnt > 0 || ttl_cnt > 0) {
    LOG_INFO("[DIAG_RECYCLE] ls mds tablet gc compaction filter recycle summary",
        K_(ls_id),
        K_(medium_filter_snapshot),
        K_(mds_filter_snapshot),
        K_(max_decided_scn),
        K(group_cnt),
        K(medium_cnt),
        K(truncate_cnt),
        K(ttl_cnt));
    if (group_cnt > 0) {
      log_recycled_group_keys_in_batches_(
          ls_id_, medium_filter_snapshot_, mds_filter_snapshot_, group_cnt, group_recycled_keys_);
    }
    if (medium_cnt > 0) {
      log_recycled_tablet_ids_in_batches_(
          ls_id_,
          "medium_compaction_info",
          medium_compaction_info_mds_type(),
          medium_filter_snapshot_,
          medium_cnt,
          medium_recycled_tablets_);
    }
    if (truncate_cnt > 0) {
      log_recycled_tablet_ids_in_batches_(
          ls_id_,
          "truncate_info",
          truncate_info_mds_type(),
          mds_filter_snapshot_,
          truncate_cnt,
          truncate_recycled_tablets_);
    }
    if (ttl_cnt > 0) {
      log_recycled_tablet_ids_in_batches_(
          ls_id_,
          "ttl_filter_info",
          ttl_filter_info_mds_type(),
          mds_filter_snapshot_,
          ttl_cnt,
          ttl_recycled_tablets_);
    }
  }
}

void ObMdsTabletGcCompactFilter::destroy_recycle_summary_()
{
  // Single-threaded: reset() runs only when filter() callers are done.
  // ObHashSet::destroy() and size() are both idempotent on uninitialized
  // sets, so this is safe to call regardless of init success.
  log_recycle_summary_();
  (void)medium_recycled_tablets_.destroy();
  (void)truncate_recycled_tablets_.destroy();
  (void)ttl_recycled_tablets_.destroy();
  (void)group_recycled_keys_.destroy();
}

void ObMdsTabletGcCompactFilter::record_multi_row_recycled_(
    const uint16_t mds_type,
    const int64_t tablet_id) const
{
  // Reaching here means filter() is running, so init() succeeded and all
  // four hashsets are alive. Concurrent inserts are serialized by the lock.
  if (tablet_id > 0) {
    common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode> *tablet_ids = nullptr;
    if (mds_type == medium_compaction_info_mds_type()) {
      tablet_ids = &medium_recycled_tablets_;
    } else if (mds_type == truncate_info_mds_type()) {
      tablet_ids = &truncate_recycled_tablets_;
    } else if (mds_type == ttl_filter_info_mds_type()) {
      tablet_ids = &ttl_recycled_tablets_;
    }
    if (OB_NOT_NULL(tablet_ids)) {
      common::ObSpinLockGuard guard(recycle_summary_lock_);
      const int tmp_ret = tablet_ids->set_refactored(tablet_id, 1/*overwrite*/);
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret && OB_HASH_EXIST != tmp_ret)) {
        LOG_WARN_RET(tmp_ret, "failed to record multi-row recycled tablet",
            K_(ls_id), K(mds_type), K(tablet_id));
      }
    }
  }
}

void ObMdsTabletGcCompactFilter::record_group_recycled_(const ObMdsGcGroupKey &key) const
{
  if (key.is_valid()) {
    common::ObSpinLockGuard guard(recycle_summary_lock_);
    const int tmp_ret = group_recycled_keys_.set_refactored(key, 1/*overwrite*/);
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret && OB_HASH_EXIST != tmp_ret)) {
      LOG_WARN_RET(tmp_ret, "failed to record recycled group key", K_(ls_id), K(key));
    }
  }
}

void ObMdsTabletGcCompactFilter::reset()
{
  destroy_recycle_summary_();
  if (fork_snapshot_map_inited_) {
    fork_snapshot_map_.destroy();
    fork_snapshot_map_inited_ = false;
  }
  if (group_map_inited_) {
    group_decision_map_.destroy();
    group_map_inited_ = false;
  }
  is_inited_ = false;
  ls_id_.reset();
  medium_filter_snapshot_ = 0;
  mds_filter_snapshot_ = 0;
  max_decided_scn_ = 0;
}

int ObMdsTabletGcCompactFilter::init(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls mds tablet gc compaction filter init twice", KR(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_FAIL(group_decision_map_.init(
                 ObMemAttr(MTL_ID(), "MdsTGCLHM", common::ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("failed to init ls mds gc group decision light hashmap", KR(ret));
  } else if (FALSE_IT(ls_id_ = ls_id)) {
  } else if (FALSE_IT(group_map_inited_ = true)) {
  } else if (OB_FAIL(fork_snapshot_map_.init(
                 ObMemAttr(MTL_ID(), "MdsTGCFork", common::ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("failed to init ls mds gc fork snapshot light hashmap", KR(ret));
  } else if (FALSE_IT(fork_snapshot_map_inited_ = true)) {
  } else if (OB_FAIL(init_recycle_summary_())) {
    LOG_WARN("failed to init ls mds gc recycle summary", KR(ret), K(ls_id));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(init_history_filter_snapshot_())) {
      // Best-effort: history filter snapshot depends on tenant components
      // that may not be ready yet; row decisions still work without it.
      LOG_WARN("failed to init ls mds tablet history gc snapshot", K(tmp_ret), K(ls_id));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

// Tracepoint helper: drop min_dependent_freeze_scn from the waterline.
// Enable from SQL:
//   alter system set_tp tp_name = "EN_MDS_TABLET_GC_SKIP_MIN_DEPENDENT_FREEZE",
//                       error_code = 4001, frequency = 1;
// Without this, the waterline stays pinned to the init freeze info entry
// (frozen_scn=1) until MIN_DEPENDENT_FREEZE_INFO_GAP+1 tenant majors have
// accumulated, so recycle tests would otherwise need a warm-up tenant major.
ERRSIM_POINT_DEF(EN_MDS_TABLET_GC_SKIP_MIN_DEPENDENT_FREEZE);
int ObMdsTabletGcCompactFilter::init_history_filter_snapshot_()
{
  int ret = OB_SUCCESS;
  share::ObFreezeInfo freeze_info;
  ObBasicMergeScheduler *merge_scheduler = MERGE_SCHEDULER_PTR;
  ObTenantFreezeInfoMgr *freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *);
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  share::SCN current_gts = share::SCN::base_scn();
  share::SCN max_decided_scn;
  medium_filter_snapshot_ = 0;
  mds_filter_snapshot_ = 0;
  max_decided_scn_ = 0;
  if (OB_ISNULL(merge_scheduler) || OB_ISNULL(freeze_info_mgr) || OB_ISNULL(ls_service)) {
    ret = OB_EAGAIN;
    LOG_WARN("missing tenant components", K(ret), K(ls_id_), KP(merge_scheduler), KP(freeze_info_mgr), KP(ls_service));
  } else if (OB_FAIL(freeze_info_mgr->get_min_dependent_freeze_info(freeze_info))) {
    LOG_WARN("missing min dependent freeze info", KR(ret), K(ls_id_));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls for mds tablet gc max decided scn", KR(ret), K_(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K_(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("failed to get max decided scn for mds tablet gc", KR(ret), K_(ls_id));
  } else if (OB_UNLIKELY(!max_decided_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid max decided scn for mds tablet gc", KR(ret), K_(ls_id), K(max_decided_scn));
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_EAGAIN;
    LOG_WARN("invalid tenant config", K(ret), K(ls_id_));
  } else if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), nullptr, current_gts))) {
    LOG_WARN("gts not ready", K(ret), K(ls_id_));
  } else {
    const int64_t retention_usec = tenant_config->_mds_multi_row_retention;
    const int64_t inner_table_merged_scn = merge_scheduler->get_inner_table_merged_scn();
    const int64_t min_dependent_freeze_scn = freeze_info.frozen_scn_.get_val_for_tx();
    const int64_t snapshot_gc_scn = freeze_info_mgr->get_snapshot_gc_scn().get_val_for_tx();
    const int64_t retention_scn = MAX(0, current_gts.get_val_for_tx() - retention_usec * 1000L);
    max_decided_scn_ = max_decided_scn.get_val_for_tx();
    mds_filter_snapshot_ = MIN3(inner_table_merged_scn, snapshot_gc_scn, retention_scn);
    if (OB_UNLIKELY(EN_MDS_TABLET_GC_SKIP_MIN_DEPENDENT_FREEZE)) {
      LOG_INFO("tracepoint EN_MDS_TABLET_GC_SKIP_MIN_DEPENDENT_FREEZE: dropping min_dependent_freeze_scn from waterline", K_(ls_id));
    } else {
      mds_filter_snapshot_ = MIN(mds_filter_snapshot_, min_dependent_freeze_scn);
    }
    mds_filter_snapshot_ = MIN(mds_filter_snapshot_, max_decided_scn_);
    medium_filter_snapshot_ = mds_filter_snapshot_;
    LOG_INFO("[DIAG_RECYCLE] init ls mds tablet history gc snapshot",
        K_(ls_id), K(inner_table_merged_scn), K(min_dependent_freeze_scn),
        K(snapshot_gc_scn), K(retention_scn), K(current_gts), K(retention_usec), K_(max_decided_scn),
        K_(medium_filter_snapshot), K_(mds_filter_snapshot));
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::filter_multi_row_history_(
    const uint16_t mds_type,
    const ObMdsGcGroupKey &key,
    const share::SCN &transfer_scn,
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret) const
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_KEEP;
  if (mds_type == medium_compaction_info_mds_type()) {
    if (OB_FAIL(filter_medium_compaction_history_(row, filter_ret))) {
      LOG_WARN("failed to filter medium compaction history", KR(ret), K_(medium_filter_snapshot));
    }
  } else if (mds_type == truncate_info_mds_type()) {
    if (OB_FAIL(filter_truncate_info_history_(row, filter_ret))) {
      LOG_WARN("failed to filter truncate info history", KR(ret), K_(mds_filter_snapshot));
    }
  } else if (mds_type == fork_src_mds_type()) {
    if (OB_FAIL(filter_fork_src_history_(row, filter_ret))) {
      LOG_WARN("failed to filter fork source history", KR(ret), K_(mds_filter_snapshot));
    }
  } else if (mds_type == ttl_filter_info_mds_type()) {
    // TODO(miaoyixuan.myx): ttl mds gc 更优雅地感知 fork 点位
    if (OB_FAIL(filter_ttl_filter_info_history_(key, transfer_scn, row, filter_ret))) {
      LOG_WARN("failed to filter ttl filter info history", KR(ret), K_(mds_filter_snapshot));
    }
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::filter_medium_compaction_history_(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret) const
{
  int ret = OB_SUCCESS;
  using MDS_COL = ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX;
  const blocksstable::ObStorageDatum &key_datum =
      row.storage_datums_[static_cast<int64_t>(MDS_COL::MDS_KEY)];
  common::ObString key_str;
  int64_t pos = 0;
  ObMediumCompactionInfoKey medium_info_key;
  if (OB_UNLIKELY(key_datum.is_nop())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium compaction info key must not be nop", KR(ret), K(row));
  } else if (FALSE_IT(key_str = key_datum.get_string())) {
  } else if (OB_UNLIKELY(key_str.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium compaction info key must not be empty", KR(ret), K(row));
  } else if (medium_filter_snapshot_ <= 0) {
    // skip
  } else if (OB_FAIL(medium_info_key.mds_deserialize(key_str.ptr(), key_str.length(), pos))) {
    LOG_WARN("failed to deserialize medium compaction info key",
        KR(ret), "mds_type", medium_compaction_info_mds_type(), K_(medium_filter_snapshot));
  } else if (OB_UNLIKELY(pos != key_str.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialize medium compaction info key pos mismatch",
        KR(ret), K(pos), "len", key_str.length());
  } else if (medium_info_key.get_medium_snapshot() <= medium_filter_snapshot_) {
    filter_ret = FILTER_RET_REMOVE;
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::filter_truncate_info_history_(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret) const
{
  int ret = OB_SUCCESS;
  using MDS_COL = ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX;
  const blocksstable::ObStorageDatum &val_datum =
      row.storage_datums_[static_cast<int64_t>(MDS_COL::MDS_VALUE)];
  common::ObString value_str;
  int64_t raw_commit_version = 0;
  int64_t commit_version = 0;
  if (OB_UNLIKELY(val_datum.is_nop())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("truncate info value must not be nop", KR(ret), K(row));
  } else if (FALSE_IT(value_str = val_datum.get_string())) {
  } else if (OB_UNLIKELY(value_str.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("truncate info value must not be empty", KR(ret), K(row));
  } else if (FALSE_IT(raw_commit_version = row.storage_datums_[
                 static_cast<int64_t>(MDS_COL::COMMIT_VERSION)].get_int())) {
  } else if (FALSE_IT(commit_version =
                 ObMdsTabletSchemaDef::normalize_commit_version_for_tx(raw_commit_version))) {
  } else if (OB_UNLIKELY(commit_version <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("truncate info commit version must be positive",
        KR(ret), K(raw_commit_version), K(commit_version), K(row));
  } else if (mds_filter_snapshot_ <= 0) {
    // skip
  } else if (commit_version < mds_filter_snapshot_) {
    filter_ret = FILTER_RET_REMOVE;
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::filter_fork_src_history_(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret) const
{
  int ret = OB_SUCCESS;
  using MDS_COL = ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX;
  const blocksstable::ObStorageDatum &val_datum =
      row.storage_datums_[static_cast<int64_t>(MDS_COL::MDS_VALUE)];
  common::ObString value_str;
  int64_t pos = 0;
  int64_t commit_version = 0;
  ObTabletForkSrcMdsData fork_src_data;
  if (OB_UNLIKELY(val_datum.is_nop())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fork source marker value must not be nop", KR(ret), K(row));
  } else if (FALSE_IT(value_str = val_datum.get_string())) {
  } else if (OB_UNLIKELY(value_str.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fork source marker value must not be empty", KR(ret), K(row));
  } else if (OB_FAIL(fork_src_data.deserialize(value_str.ptr(), value_str.length(), pos))) {
    LOG_WARN("failed to deserialize fork source marker", KR(ret), K(row));
  } else if (OB_UNLIKELY(pos != value_str.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fork source marker deserialize pos mismatch", KR(ret), K(pos), K(value_str));
  } else if (fork_src_data.is_valid() || mds_filter_snapshot_ <= 0) {
    // 有效 marker 仍承担源端数据保护，不能按历史水位回收。
  } else if (OB_FAIL(get_normalized_commit_version_(row, commit_version))) {
    LOG_WARN("failed to get fork source marker commit version", KR(ret), K(row));
  } else if (commit_version < mds_filter_snapshot_) {
    // FINISH 写入的无效 marker 越过安全水位后，按目标 Tablet key 回收。
    filter_ret = FILTER_RET_REMOVE;
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::get_max_committed_fork_snapshot_(
    const ObMdsGcGroupKey &key,
    const share::SCN &transfer_scn,
    bool &read_succeeded,
    int64_t &fork_snapshot) const
{
  int ret = OB_SUCCESS;
  read_succeeded = false;
  fork_snapshot = 0;
  if (read_fork_snapshot_from_map_(fork_snapshot_map_, key, fork_snapshot)) {
    read_succeeded = true;
  } else {
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSHandle ls_handle;
    ObMdsTable *mds_table = nullptr;
    const share::SCN read_snapshot = share::SCN::max_scn();
    common::ObArenaAllocator allocator("MdsTGCFork", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObMdsTableScanIterator scan_iter;
    ObTabletForkSrcMdsKey fork_key;
    ObTabletForkSrcMdsData fork_src_data;
    ObMdsValueMeta meta;
    bool iter_end = false;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null when reading fork source marker", KR(ret), K_(ls_id), K(key));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls for fork source marker lookup", KR(ret), K_(ls_id), K(key));
    } else if (OB_ISNULL(ls_handle.get_ls())
               || OB_ISNULL(mds_table = ls_handle.get_ls()->get_mds_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls or mds table is null when reading fork source marker",
          KR(ret), K_(ls_id), K(key), KP(ls_handle.get_ls()));
    } else if (OB_FAIL((mds_table->table_scan<ObTabletForkSrcMdsKey, ObTabletForkSrcMdsData>(
          allocator,
          common::ObTabletID(key.tablet_id_),
          transfer_scn,
          read_snapshot,
          scan_iter)))) {
      LOG_WARN("failed to scan committed fork source markers", KR(ret), K_(ls_id), K(key));
    }
    while (OB_SUCC(ret) && !iter_end) {
      fork_key.reset();
      fork_src_data.reset();
      meta.reset();
      if (OB_FAIL(scan_iter.get_next_mds_kv(allocator, fork_key, fork_src_data, meta))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          iter_end = true;
        } else {
          LOG_WARN("failed to get next committed fork source marker", KR(ret), K_(ls_id), K(key));
        }
      } else if (fork_src_data.is_valid()) {
        fork_snapshot = MAX(fork_snapshot, fork_src_data.fork_snapshot_version_);
      }
    }
    if (OB_SUCC(ret) && iter_end) {
      read_succeeded = true;
    }
    if (!read_succeeded) {
      // 扫描失败不足以证明 TTL 历史可回收，保守保留当前行且不缓存失败结果。
      ret = OB_SUCCESS;
    } else {
      int64_t effective_snapshot = 0;
      if (OB_FAIL(publish_fork_snapshot_light_(
              fork_snapshot_map_, fork_snapshot_node_alloc_, key, fork_snapshot, effective_snapshot))) {
        LOG_WARN("failed to cache fork source marker snapshot", KR(ret), K(key), K(fork_snapshot));
      } else {
        fork_snapshot = effective_snapshot;
      }
    }
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::filter_ttl_filter_info_history_(
    const ObMdsGcGroupKey &key,
    const share::SCN &transfer_scn,
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret) const
{
  int ret = OB_SUCCESS;
  using MDS_COL = ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX;
  const blocksstable::ObStorageDatum &val_datum =
      row.storage_datums_[static_cast<int64_t>(MDS_COL::MDS_VALUE)];
  common::ObString value_str;
  int64_t raw_commit_version = 0;
  int64_t commit_version = 0;
  int64_t fork_snapshot = 0;
  bool marker_read_succeeded = false;
  if (OB_UNLIKELY(val_datum.is_nop())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ttl filter info value must not be nop", KR(ret), K(row));
  } else if (FALSE_IT(value_str = val_datum.get_string())) {
  } else if (OB_UNLIKELY(value_str.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ttl filter info value must not be empty", KR(ret), K(row));
  } else if (FALSE_IT(raw_commit_version = row.storage_datums_[
                 static_cast<int64_t>(MDS_COL::COMMIT_VERSION)].get_int())) {
  } else if (FALSE_IT(commit_version =
                 ObMdsTabletSchemaDef::normalize_commit_version_for_tx(raw_commit_version))) {
  } else if (OB_UNLIKELY(commit_version <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ttl filter info commit version must be positive",
        KR(ret), K(raw_commit_version), K(commit_version), K(row));
  } else if (mds_filter_snapshot_ <= 0) {
    // skip
  } else if (commit_version >= mds_filter_snapshot_) {
    // keep
  } else if (OB_FAIL(get_max_committed_fork_snapshot_(
                 key, transfer_scn, marker_read_succeeded, fork_snapshot))) {
    LOG_WARN("failed to get fork snapshot for ttl filter info history", KR(ret), K(key));
  } else if (!marker_read_succeeded) {
    // marker 扫描失败时保留当前行，且不缓存失败结果。
  } else if (fork_snapshot > 0 && commit_version <= fork_snapshot) {
    // An active fork still needs the source TTL history visible at its snapshot.
  } else {
    filter_ret = FILTER_RET_REMOVE;
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::decide_create_del_row_(
    const ObMdsGcGroupKey &key,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const blocksstable::ObDatumRow &row,
    int8_t &effective_dec) const
{
  int ret = OB_SUCCESS;
  int8_t decision = DEC_KEEP;
  using MDS_COL = ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX;
  const blocksstable::ObStorageDatum &val_datum =
      row.storage_datums_[static_cast<int64_t>(MDS_COL::MDS_VALUE)];
  common::ObString ud;
  ObTabletCreateDeleteMdsUserData user_data;
  int64_t pos = 0;
  if (OB_UNLIKELY(val_datum.is_nop())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet create/delete value must not be nop", KR(ret), K(key), K(row));
  } else if (FALSE_IT(ud = val_datum.get_string())) {
  } else if (OB_UNLIKELY(ud.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet create/delete value must not be empty", KR(ret), K(key), K(row));
  } else if (OB_FAIL(user_data.deserialize(ud.ptr(), ud.length(), pos))) {
    LOG_WARN("deserialize tablet create/delete user_data failed", KR(ret), K(key), "len", ud.length());
  } else if (OB_UNLIKELY(pos != ud.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialize pos mismatch", KR(ret), K(key), K(pos), "len", ud.length());
  } else if (is_deleted_for_gc_before_snapshot_(user_data, mds_filter_snapshot_)) {
    decision = peek_t3m_gc_decision_(ls_id_, tablet_id, transfer_scn);
  }
  if (OB_SUCC(ret)) {
    ret = publish_group_decision_light_(
        group_decision_map_, group_node_alloc_, key, decision, effective_dec);
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::decide_from_tablet_status_lookup_(
    const ObMdsGcGroupKey &key,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &status_found,
    bool &status_empty,
    int8_t &effective_dec) const
{
  int ret = OB_SUCCESS;
  status_found = false;
  status_empty = false;
  effective_dec = DEC_KEEP;
  if (mds_filter_snapshot_ <= 0) {
    // skip
  } else {
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSHandle ls_handle;
    ObMdsTable *mds_table = nullptr;
    const share::SCN read_snapshot = share::SCN::max_scn();
    common::ObArenaAllocator allocator("MdsTGCRead", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTabletCreateDeleteMdsUserData user_data;
    ObMdsValueMeta meta;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret), K_(ls_id), K(key));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls for ls mds tablet gc status lookup", KR(ret), K_(ls_id), K(key));
    } else if (OB_ISNULL(ls_handle.get_ls())
               || OB_ISNULL(mds_table = ls_handle.get_ls()->get_mds_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls or mds table is null", KR(ret), K_(ls_id), K(key), KP(ls_handle.get_ls()));
    } else {
      ret = mds_table->get<ObTabletCreateDeleteMdsUserData>(
          &allocator,
          tablet_id,
          transfer_scn,
          read_snapshot,
          MDS_TABLET_GC_POINT_READ_TIMEOUT_US,
          false/*read_uncommitted*/,
          user_data,
          meta);
      if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
        status_found = false;
        status_empty = true;
        LOG_INFO("[DIAG_RECYCLE] ls mds tablet gc status lookup empty, fallback to row snapshot",
            K_(ls_id), K(key), K_(mds_filter_snapshot));
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to lookup ls mds tablet gc status, keep group conservatively",
            KR(ret), K_(ls_id), K(key), K_(mds_filter_snapshot));
      } else {
        status_found = true;
        const int8_t decision = is_deleted_for_gc_before_snapshot_(user_data, mds_filter_snapshot_)
            ? peek_t3m_gc_decision_(ls_id_, tablet_id, transfer_scn)
            : DEC_KEEP;
        ret = publish_group_decision_light_(
            group_decision_map_, group_node_alloc_, key, decision, effective_dec);
      }
    }
    if (OB_FAIL(ret) && !status_found) {
      // A status-row point-read failure is not enough evidence to recycle.
      // Keep this path best-effort so compaction can continue; only a
      // successful lookup may publish a group decision.
      ret = OB_SUCCESS;
      effective_dec = DEC_KEEP;
    }
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::decide_other_row_(
    const ObMdsGcGroupKey &key,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const blocksstable::ObDatumRow &row,
    int8_t &effective_dec) const
{
  int ret = OB_SUCCESS;
  int64_t commit_version = 0;
  bool status_found = false;
  bool status_empty = false;
  ObMdsGcGroupNode *node = nullptr;
  if (OB_SUCCESS == group_decision_map_.get(key, node)) {
    effective_dec = ATOMIC_LOAD_RLX(&(node->decision_));
    group_decision_map_.revert(node);
  } else if (OB_FAIL(decide_from_tablet_status_lookup_(
                 key, tablet_id, transfer_scn, status_found, status_empty, effective_dec))) {
    LOG_WARN("failed to decide from tablet status lookup", KR(ret), K(key));
  } else if (status_found) {
    // decision has been published from the tablet-status row.
  } else if (!status_empty) {
    effective_dec = DEC_KEEP;
  } else if (OB_FAIL(get_normalized_commit_version_(row, commit_version))) {
    LOG_WARN("failed to get ls mds row commit version", KR(ret), K(key));
  } else if (mds_filter_snapshot_ <= 0 || commit_version >= mds_filter_snapshot_) {
    effective_dec = DEC_KEEP;
  } else {
    effective_dec = peek_t3m_gc_decision_(ls_id_, tablet_id, transfer_scn);
  }
  return ret;
}

int ObMdsTabletGcCompactFilter::filter(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret) const
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_KEEP;
  using MDS_COL = ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX;
  const int64_t expected_col_cnt = static_cast<int64_t>(MDS_COL::MAX_COLUMN_IDX);
  if (IS_NOT_INIT || !group_map_inited_ || !fork_snapshot_map_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls mds tablet gc compaction filter not inited", KR(ret));
  } else if (row.row_flag_.is_delete()) {
    // tombstones handled by merge path
  } else if (OB_UNLIKELY(row.get_column_count() != expected_col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected LS_MDS_TABLET row column count in gc compaction filter",
        KR(ret), "column_count", row.get_column_count(), K(expected_col_cnt));
  } else {
    const common::ObTabletID tablet_id(row.storage_datums_[static_cast<int64_t>(MDS_COL::TABLET_ID)].get_int());
    share::SCN transfer_scn;
    const uint16_t mds_type = static_cast<uint16_t>(row.storage_datums_[static_cast<int64_t>(MDS_COL::MDS_TYPE)].get_int());
    if (!tablet_id.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet_id on LS_MDS_TABLET row", KR(ret), K(row));
      // not a user tablet row, skip
    } else if (OB_FAIL(transfer_scn.convert_for_tx(row.storage_datums_[static_cast<int64_t>(MDS_COL::TRANSFER_SCN)].get_int()))) {
      LOG_WARN("invalid transfer_scn on LS_MDS_TABLET row", KR(ret), K(row));
    } else if (!transfer_scn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer_scn invalid after convert", KR(ret), K(row));
    } else {
      const ObMdsGcGroupKey key(tablet_id, transfer_scn);
      int8_t decision = DEC_KEEP;
      if (OB_FAIL(filter_multi_row_history_(mds_type, key, transfer_scn, row, filter_ret))) {
        LOG_WARN("failed to filter ls mds tablet multi-row history", KR(ret), K(mds_type), K(key));
      } else if (FILTER_RET_REMOVE == filter_ret) {
        record_multi_row_recycled_(mds_type, tablet_id.id());
      } else if (mds_type == tablet_create_delete_mds_type()) {
        ret = decide_create_del_row_(key, tablet_id, transfer_scn, row, decision);
      } else {
        ret = decide_other_row_(key, tablet_id, transfer_scn, row, decision);
      }
      if (OB_SUCC(ret) && DEC_REMOVE == decision) {
        filter_ret = FILTER_RET_REMOVE;
        record_group_recycled_(key);
      }
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
