/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_SERVICE_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_SERVICE_H_

#include "storage/async_index/ob_tenant_async_index_refresher.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/allocator/ob_fifo_allocator.h"

namespace oceanbase
{
namespace storage
{

class ObLS;  // forward declaration for ObAsyncIndexDirectRefreshParam

// Op type for a single direct-trigger refresh SQL.
// Determines which SQL variant (INSERT-new, DELETE-old, or full COMPLETE scan)
// is generated and executed. The caller picks exactly one op per call.
enum ObAsyncIndexDirectRefreshOp
{
  DIRECT_REFRESH_OP_INSERT_NEW = 0,  // incremental: pull new rows into the index table
  DIRECT_REFRESH_OP_DELETE_OLD = 1,  // incremental: write deleted docids to delete-buffer table
  DIRECT_REFRESH_OP_COMPLETE   = 2,  // full snapshot: bulk-load index from scratch (no baseline)
};

// Parameters for triggering a single index-tablet refresh directly and
// synchronously. The caller must guarantee:
//   - ls_ points to the leader LS that owns both data_tablet_id_ and
//     index_tablet_id_ (leader validity is NOT checked inside the interface).
//   - data_table_id_ / index_table_id_ are the table IDs whose schemas are
//     used to build the SQL.  data_tablet_id_ / index_tablet_id_ must be valid
//     tablets of those tables respectively.
//   - op_ is set to the appropriate SQL variant for this call (INSERT_NEW for
//     a regular physical async inverted index, DELETE_OLD for the delete-buffer
//     index, or COMPLETE for the first/no-baseline build).
//
// start_refresh_scn (the WHERE ora_rowscn lower bound) is read internally from
// the index tablet's publish scn, and DOP is selected adaptively inside
// trigger_incr_refresh(), so the caller does not need to supply either value.
struct ObAsyncIndexDirectRefreshParam
{
  ObAsyncIndexDirectRefreshParam()
    : tenant_id_(OB_INVALID_TENANT_ID),
      ls_(nullptr),
      data_table_id_(OB_INVALID_ID),
      index_table_id_(OB_INVALID_ID),
      data_tablet_id_(),
      index_tablet_id_(),
      target_refresh_scn_(),
      op_(DIRECT_REFRESH_OP_INSERT_NEW),
      refresh_type_(0)
  {}

  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
        && OB_NOT_NULL(ls_)
        && OB_INVALID_ID != data_table_id_
        && OB_INVALID_ID != index_table_id_
        && data_tablet_id_.is_valid()
        && index_tablet_id_.is_valid()
        && target_refresh_scn_.is_valid();
  }

  TO_STRING_KV(K_(tenant_id), KP_(ls),
               K_(data_table_id), K_(index_table_id),
               K_(data_tablet_id), K_(index_tablet_id),
               K_(target_refresh_scn), K_(op), K_(refresh_type));

  uint64_t tenant_id_;
  ObLS *ls_;                           // leader LS; caller guarantees leadership
  uint64_t data_table_id_;             // source data table (schema + lock)
  uint64_t index_table_id_;            // target index table (schema)
  common::ObTabletID data_tablet_id_;  // source partition tablet
  common::ObTabletID index_tablet_id_; // target index partition tablet
  share::SCN target_refresh_scn_;      // snapshot to refresh up to (AS OF SNAPSHOT)
  ObAsyncIndexDirectRefreshOp op_;     // which SQL variant to generate
  int8_t refresh_type_;
};

// Tenant-level MTL service for async index incremental refresh.
//
// It is NOT a logservice role-change handler and is not bound to any single LS.
// Instead it owns the ObTenantAsyncIndexRefresher (TIMER-based manager) and runs
// it unconditionally on every node once the tenant MTL starts. Each LS registers
// its ObLSAsyncIndexScheduler with the refresher during LS creation, and the
// refresher iterates all registered LS schedulers each timer round, processing
// only the leader ones.
//
// Lifecycle is driven by the MTL framework: mtl_init -> init -> start (starts the
// refresher) -> stop -> wait -> destroy.
class ObAsyncIndexService
{
public:
  ObAsyncIndexService()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      rpc_refresh_lock_(common::ObLatchIds::OB_ASYNC_INDEX_REFRESH_POOL_LOCK)
  {}
  virtual ~ObAsyncIndexService() {}

  static int mtl_init(ObAsyncIndexService *&service);
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();

  // Expose the refresher for LS scheduler registration/unregistration.
  ObTenantAsyncIndexRefresher &get_refresher() { return refresher_; }

  // --- execution context registry for the inner refresh SQL ---
  // Register (before executing the inner SQL) the refresh window, schema version,
  // and build ID keyed by the synthetic refresh_task_id; ObPxSubCoord::start_ddl reads them back
  // via the getters during execution (the task_id has no __all_ddl_task_status record).
  // Creates the entry on first use. Always pair register with release.
  int register_refresh_versions(const int64_t refresh_task_id,
                                const share::SCN &start_refresh_scn,
                                const int64_t snapshot_version,
                                const int64_t schema_version,
                                const int64_t ddl_build_id);
  // Read a registered value. Returns OB_EAGAIN if the entry is absent (retryable).
  int get_inner_sql_start_refresh_scn(const int64_t refresh_task_id,
                                      share::SCN &start_refresh_scn);
  int get_inner_sql_snapshot_version(const int64_t refresh_task_id, int64_t &snapshot_version);
  int get_inner_sql_schema_version(const int64_t refresh_task_id, int64_t &schema_version);
  int get_inner_sql_ddl_build_id(const int64_t refresh_task_id, int64_t &ddl_build_id);
  // Erase the entry. OB_HASH_NOT_EXIST is treated as success (idempotent).
  int release_refresh_versions(const int64_t refresh_task_id);

  // --- ddl slice info stash for the inner refresh SQL ---
  // Stash (deep-copy) the PX sample range cuts produced during the refresh SQL's
  // sample scan into the existing node-local entry, keyed by refresh_task_id.
  // Mirrors ObVecIndexAsyncTaskUtil::set_inner_sql_slice_info: the entry must
  // already exist (created by register_refresh_versions before the SQL runs). A
  // future ObColumnClusteredDag for async index reads it back via the getter.
  int set_inner_sql_slice_info(const int64_t refresh_task_id,
                               rootserver::ObDDLSliceInfo &ddl_slice_info);
  // Read back the stashed slice info (deep-copied into the caller's allocator).
  // Returns OB_EAGAIN if the entry is absent (retryable), matching the version getters.
  int get_inner_sql_slice_info(const int64_t refresh_task_id,
                               common::ObIAllocator &allocator,
                               rootserver::ObDDLSliceInfo &ddl_slice_info);

  // --- skipped rows 计数（PX 执行中回传）---
  int add_skipped_rows(const int64_t refresh_task_id, const int64_t cnt);
  int get_skipped_rows(const int64_t refresh_task_id, int64_t &skipped_rows);

  // --- RPC refresh skip-set for timer path coordination ---
  // When the RPC refresh path (ObAsyncIndexRefreshP::process) is performing
  // step-by-step refresh on a data tablet, it registers the tablet here so
  // that the periodic timer path skips the same tablet to avoid lock contention.
  int register_rpc_refresh_tablet(const common::ObTabletID &data_tablet_id);
  int unregister_rpc_refresh_tablet(const common::ObTabletID &data_tablet_id);
  bool is_tablet_rpc_refreshing(const common::ObTabletID &data_tablet_id) const;

  // --- direct incremental refresh ---
  // Trigger one refresh SQL synchronously for a single index tablet.
  // Internally: reads the index tablet's publish_scn (start_refresh_scn),
  // looks up schema by table_id, builds the refresh SQL, and executes it in
  // a DDL transaction (same path as the timer-driven flow). On success the
  // finished-task stat is recorded into the shared observability ring.
  //
  // The caller must guarantee leadership on param.ls_.
  // Returns OB_SUCCESS on success.
  int trigger_incr_refresh(const ObAsyncIndexDirectRefreshParam &param);

  // 为租户 major 合并准备异步索引刷新检查点。若存在尚未发布到 tenant_major_snapshot 的索引，
  // 则将该目标写入主表 Tablet 的 refresh_info。返回值仅表示检查点准备是否成功，
  // 刷新进度由租户合并进度检查流程判断。
  int prepare_tenant_major_refresh(
      ObLS &ls,
      const common::ObTabletID &data_tablet_id,
      const common::ObIArray<common::ObTabletID> &index_tablet_ids,
      const int64_t tenant_major_snapshot);

private:
  typedef common::hash::ObHashMap<int64_t, ObAsyncIndexRefreshTmpInfo *> ObAsyncIndexTmpInfoMap;

  static const int64_t RPC_REFRESH_TABLET_BUCKET_COUNT = 64;

  bool is_inited_;
  uint64_t tenant_id_;
  ObTenantAsyncIndexRefresher refresher_;
  common::ObFIFOAllocator tmp_info_allocator_;  // owns ObAsyncIndexRefreshTmpInfo nodes
  ObAsyncIndexTmpInfoMap tmp_info_map_;          // refresh_task_id -> versions (node-local)
  mutable common::SpinRWLock rpc_refresh_lock_;
  common::hash::ObHashSet<common::ObTabletID> rpc_refreshing_tablets_;

  DISALLOW_COPY_AND_ASSIGN(ObAsyncIndexService);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_SERVICE_H_
