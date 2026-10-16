/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DEPS_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DEPS_H_

#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashset.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/async_index/ob_async_index_define.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}
namespace storage
{

class ObLS;  // forward declaration; callers pass ObLS* without needing the full definition

// Injectable abstraction over the external modules the trigger / scheduler depend on.
//
// These are owned by other developers in the async-index project (schema, MDS,
// storage estimation). Modeling them as a pure virtual interface lets the
// scheduling logic be unit-tested deterministically against a fake provider,
// and lets the production provider be swapped in without touching call sites.
//
// IMPORTANT: every method here is expressed purely in terms of value types
// (tablet ids, SCNs, row counts) so that neither the interface nor its callers
// need to include heavy storage headers (ObTablet, ObTabletHandle, schema guard).
class ObIAsyncIndexDepProvider
{
public:
  ObIAsyncIndexDepProvider() {}
  virtual ~ObIAsyncIndexDepProvider() {}

  // Get the publish SCNs for the given index tablets.
  // The publish SCN is the SCN up to which an index tablet has been refreshed.
  // publish_scns[i] corresponds to index_tablet_ids[i].
  // The caller passes the ObLS* that owns the tablets so the implementation can
  // access each index tablet directly without re-looking up the LS by id.
  virtual int get_publish_scns(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObIArray<common::ObTabletID> &index_tablet_ids,
      common::ObIArray<share::SCN> &publish_scns) = 0;

  // Read the Async Refresh Info maintained on the data tablet's MDS (the ordered
  // snapshot array of pending refresh targets). The caller passes the ObLS* that
  // owns the tablet so the implementation can read it directly. A tablet that has
  // never been written returns an empty (valid) info.
  //
  // Non-pure with an empty default so existing fake providers keep compiling.
  virtual int get_async_index_refresh_info(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObTabletID &data_tablet_id,
      ObTabletAsyncIndexRefreshInfo &info)
  {
    UNUSEDx(tenant_id, ls, data_tablet_id);
    info.reset();
    return common::OB_SUCCESS;
  }

  // Overwrite the Async Refresh Info on the data tablet's MDS. Only the LS leader
  // may call this (the scheduler guarantees leadership); it commits the info
  // through the multi-data-source register path so followers pick it up via
  // replay.
  //
  // Non-pure with an empty default so existing fake providers keep compiling.
  virtual int set_async_index_refresh_info(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObTabletID &data_tablet_id,
      const ObTabletAsyncIndexRefreshInfo &info)
  {
    UNUSEDx(tenant_id, ls, data_tablet_id, info);
    return common::OB_SUCCESS;
  }

  // Check whether any incremental DML exists in (begin_scn, end_scn] on the
  // data tablet. Lightweight boolean probe — returns as soon as any committed
  // DML is detected, without computing exact row counts.
  virtual int has_incr_dml(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObTabletID &data_tablet_id,
      const share::SCN &begin_scn,
      const share::SCN &end_scn,
      bool &has_data) = 0;

  // Get the current readable snapshot version for the tenant.
  virtual int get_read_snapshot_version(
      const uint64_t tenant_id,
      share::SCN &snapshot_scn) = 0;

  // Fetch a fresh read snapshot SCN and cache it for this timer round.
  // Called once per round before trigger evaluation. The default is a no-op;
  // the production provider overrides it to call ObTransService.
  virtual int refresh_snapshot_scn() { return common::OB_SUCCESS; }

  // Get the count of minor sstables on a given index tablet.
  // The caller passes the ObLS* that owns the tablet.
  virtual int get_index_minor_sstable_count(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObTabletID &index_tablet_id,
      int64_t &count) = 0;

  // Get the tablet id of the delete-buffer table associated with an index
  // tablet. The delete-old refresh task targets this tablet. Returns an invalid
  // tablet id when the index has no delete table (caller then skips delete-old).
  virtual int get_index_delete_tablet_id(
      const uint64_t tenant_id,
      const common::ObTabletID &index_tablet_id,
      common::ObTabletID &delete_tablet_id) = 0;

  // Enumerate the data tables in this tenant that carry async indexes, expanded
  // to (data_table_id, data_tablet_id) pairs: data_table_ids[i] corresponds to
  // data_tablet_ids[i], and a data table with N tablets contributes N entries.
  // Backed by get_table_schemas_with_async_index_in_tenant (Original: mgr
  // derived set; OnDemand: inner-table index_type scan), so an idle tenant
  // returns an empty list cheaply.
  //
  // Non-pure with an empty default so existing fake providers keep compiling;
  // only the production provider overrides it to reach the schema guard.
  virtual int get_async_index_data_tablets(
      const uint64_t tenant_id,
      common::ObIArray<uint64_t> &data_table_ids,
      common::ObIArray<common::ObTabletID> &data_tablet_ids)
  {
    UNUSEDx(tenant_id);
    data_table_ids.reset();
    data_tablet_ids.reset();
    return common::OB_SUCCESS;
  }

  // Collect lightweight per-data-table scheduling metadata. The production
  // implementation reads only table ids and ASYNC_REFRESH_INTERVAL values, so
  // the timer can filter tables before building deep-copied refresh deps.
  //
  // Non-pure with an empty default so providers unrelated to timer scheduling
  // do not need to implement schema enumeration.
  virtual int get_async_table_refresh_intervals(
      const uint64_t tenant_id,
      common::ObIArray<ObAsyncTableRefreshInterval> &table_refresh_intervals)
  {
    UNUSEDx(tenant_id);
    table_refresh_intervals.reset();
    return common::OB_SUCCESS;
  }

  // Build complete refresh deps only for the supplied data tables. The returned
  // object owns the arena used by all deep-copied schema strings.
  virtual int get_async_index_refresh_deps(
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &data_table_ids,
      ObAsyncIndexRefreshDeps &refresh_deps)
  {
    UNUSEDx(tenant_id, data_table_ids);
    refresh_deps.reset();
    return common::OB_SUCCESS;
  }

  // Collect the ids of the LSs this node currently LEADS (strong leader) in the
  // tenant. The refresher calls this once per round to (a) detect leadership
  // changes since the previous round (to fence residual tasks) and (b) drive the
  // per-leader-LS refresh scope. An empty result means this node leads no LS.
  //
  // Non-pure with an empty default so existing fake providers keep compiling;
  // only the production provider overrides it to reach the LS service.
  virtual int collect_leader_ls_ids(common::ObIArray<share::ObLSID> &leader_ls_ids)
  {
    leader_ls_ids.reset();
    return common::OB_SUCCESS;
  }
};

// Production implementation backed by the real schema guard / MDS / storage.
//
// NOTE: the bodies in ob_async_index_deps.cpp are currently stubs that return
// OB_SUCCESS with empty/default outputs. They will be replaced by the schema /
// MDS / storage teams once their interfaces land (see specs/spec.md needs
// boundary). Stubbing here keeps the scheduler compilable and link-clean today.
class ObAsyncIndexDepProvider : public ObIAsyncIndexDepProvider
{
public:
  ObAsyncIndexDepProvider() : cached_snapshot_scn_() {}
  virtual ~ObAsyncIndexDepProvider() {}

  // Fetch a fresh read snapshot SCN from ObTransService and cache it.
  // Should be called ONCE per timer round before any trigger evaluation.
  int refresh_snapshot_scn();

  virtual int get_publish_scns(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObIArray<common::ObTabletID> &index_tablet_ids,
      common::ObIArray<share::SCN> &publish_scns) override;

  virtual int get_async_index_refresh_info(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObTabletID &data_tablet_id,
      ObTabletAsyncIndexRefreshInfo &info) override;

  virtual int set_async_index_refresh_info(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObTabletID &data_tablet_id,
      const ObTabletAsyncIndexRefreshInfo &info) override;

  virtual int has_incr_dml(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObTabletID &data_tablet_id,
      const share::SCN &begin_scn,
      const share::SCN &end_scn,
      bool &has_data) override;

  virtual int get_read_snapshot_version(
      const uint64_t tenant_id,
      share::SCN &snapshot_scn) override;

  virtual int get_index_minor_sstable_count(
      const uint64_t tenant_id,
      ObLS *ls,
      const common::ObTabletID &index_tablet_id,
      int64_t &count) override;

  virtual int get_index_delete_tablet_id(
      const uint64_t tenant_id,
      const common::ObTabletID &index_tablet_id,
      common::ObTabletID &delete_tablet_id) override;

  virtual int get_async_index_data_tablets(
      const uint64_t tenant_id,
      common::ObIArray<uint64_t> &data_table_ids,
      common::ObIArray<common::ObTabletID> &data_tablet_ids) override;

  virtual int get_async_table_refresh_intervals(
      const uint64_t tenant_id,
      common::ObIArray<ObAsyncTableRefreshInterval> &table_refresh_intervals) override;

  virtual int get_async_index_refresh_deps(
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &data_table_ids,
      ObAsyncIndexRefreshDeps &refresh_deps) override;

  // Build the refresh dependency info for a single data table by table_id.
  // Equivalent to one iteration of get_async_index_refresh_deps's selected table
  // loop. Useful when the caller already knows which data_table_id it targets
  // (e.g. direct-trigger path) and does not need to enumerate all tenant tables.
  // All string members are deep-copied into the supplied allocator.
  int get_single_table_refresh_info(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      common::ObIAllocator &allocator,
      ObAsyncIndexDataTableInfo &data_table_info);

  // Return the minimum published SCN of every async refresh target that belongs
  // to one data-table partition. The target set includes the shared async delete
  // index. has_async_index is false when the data table has no async target.
  int get_partition_min_async_published_scn(
      const uint64_t tenant_id,
      ObLS *ls,
      const uint64_t data_table_id,
      const common::ObTabletID &data_tablet_id,
      bool &has_async_index,
      share::SCN &min_published_scn);

  virtual int collect_leader_ls_ids(
      common::ObIArray<share::ObLSID> &leader_ls_ids) override;

private:
  // Collect the ids of the log streams this node currently LEADS, into leader_ls_ids.
  int collect_leader_ls_ids_(
      common::ObIArray<share::ObLSID> &leader_ls_ids);

private:
  // Cached read snapshot SCN, refreshed once per timer round.
  share::SCN cached_snapshot_scn_;

  DISALLOW_COPY_AND_ASSIGN(ObAsyncIndexDepProvider);
};

// Minimum of physical publish SCNs returned by get_publish_scns. Shared by the
// timer trigger and the CREATE_INDEX refresh path.
int calc_min_publish_scn(
    const common::ObIArray<share::SCN> &publish_scns,
    share::SCN &min_publish_scn);

// Compute the minimum physical publish SCN across all given index tablets via
// get_publish_scns + calc_min_publish_scn (same as ObAsyncIndexTrigger). Used by
// the CREATE_INDEX refresh path to supply a GC floor to
// maintain_async_index_refresh_info when the data tablet's refresh_snapshots
// array is still empty (Step 6.5 anchor). Returns base_scn() when the target
// set is empty.
int get_min_index_published_scn(
    const uint64_t tenant_id,
    ObLS *ls,
    ObIAsyncIndexDepProvider *dep_provider,
    const common::ObIArray<common::ObTabletID> &index_tablet_ids,
    share::SCN &min_publish_scn);

// Maintain the async index refresh info on a data tablet with lock protection.
// This is the unified interface for all paths (TIMER / manual trigger / post-build)
// that need to update the data tablet's refresh_info MDS. It acquires an EXCLUSIVE
// object lock (OBJ_TYPE_ASYNC_INDEX_REFRESH) on the data_tablet_id within a
// transaction, performs the read-modify-write atomically, and releases the lock
// when the transaction commits.
//
// Returns OB_EAGAIN on lock conflict (caller should skip this round).
// Returns OB_SIZE_OVERFLOW when the checkpoint array is full, regardless of
// whether checkpoint_capacity_full is supplied. This optional output identifies
// checkpoint capacity errors only; other OB_SIZE_OVERFLOW sources do not set it.
// A failed call does not provide committed out_refresh_info.
// 手动刷新在锁内检查现有 checkpoint 数量，超过流控阈值时返回 OB_OP_NOT_ALLOW。
// is_manual_refresh 默认为 false，DDL 和后台刷新不受该流控影响。
int maintain_async_index_refresh_info(
    const uint64_t tenant_id,
    ObLS *ls,
    const common::ObTabletID &data_tablet_id,
    const share::SCN &new_snapshot,
    const share::SCN &min_publish_scn,
    ObIAsyncIndexDepProvider *dep_provider,
    ObTabletAsyncIndexRefreshInfo &out_refresh_info,
    const int64_t lock_timeout_us = 0,
    const bool skip_incr_check = false,
    bool *checkpoint_capacity_full = nullptr,
    const bool is_manual_refresh = false);

// Fence async-index refresh tasks before DROP TABLE enters the normal table-lock path.
// The ids must be the index tablet ids used by ObAsyncIndexRefreshTask.
int lock_async_index_refresh_for_drop(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &index_tablet_ids,
    const int64_t lock_timeout_us,
    common::ObMySQLTransaction &trans);

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DEPS_H_
