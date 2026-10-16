/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_REASSIGN_TASK_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_REASSIGN_TASK_H_

#include <utility>
#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "share/vector_index/spfresh/ob_spfresh_task_queue.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace share
{

// Forward declarations
class ObSpfreshTaskQueue;

// Reassign task implementation
// Implements the batched reassignment process:
// Phase 1: Acquire lock with reentry support (SPLIT_DONE/MERGED/REASSIGNING -> REASSIGNING)
// Phase 2: Loop batch large transaction (BATCH_SIZE=1000)
//   - Each batch: scan -> DELETE/INSERT/UPDATE rowkey_cid -> COMMIT
//   - Middle batches: only migrate vectors, update posting_list_length
//   - Final batch: delete centroid + set del_scn
//   - Empty batch: single transaction delete centroid + del_scn
class ObSpfreshReassignTask
{
public:
  static const int64_t BATCH_SIZE = 1000;

  ObSpfreshReassignTask();
  ~ObSpfreshReassignTask();

  int init(uint64_t tenant_id,
           common::ObMySQLProxy *sql_proxy,
           uint64_t index_table_id,
           const ObSpfreshKvTableInfo &kv_info,
           uint64_t queue_tablet_id,
           int64_t center_id,
           int64_t ns_id,
           uint64_t ltable_id,
           ObSpfreshTaskQueue *task_queue,
           ObIvfMapCentCache *cache,
           ObVectorIndexDistAlgorithm dist_algo = ObVectorIndexDistAlgorithm::VIDA_L2,
           const share::ObLSID &physical_ls_id = share::ObLSID(),
           const common::ObTabletID &physical_tablet_id = common::ObTabletID(),
           int64_t cutover_modify_ts = 0);
  void destroy();

  // Execute the reassign task
  // @return OB_SUCCESS on success, error code on failure
  int execute();

  // Get the center_id being processed
  int64_t get_center_id() const { return center_id_; }

  // Getter for task execution output (valid after execute() returns)
  int64_t get_reassigned_vector_count() const { return total_processed_; }

private:
  class CollectCacheInfoFn
  {
  public:
    CollectCacheInfoFn(
        common::ObIArray<std::pair<int64_t, ObCentroidInfo>> &infos,
        const common::ObIArray<int64_t> &cids);
    int operator()(int64_t cid, const ObCentroidInfo &info);

  private:
    common::ObIArray<std::pair<int64_t, ObCentroidInfo>> &infos_;
    const common::ObIArray<int64_t> &cids_;
    DISALLOW_COPY_AND_ASSIGN(CollectCacheInfoFn);
  };

  // Phase 1: Acquire lock with CAS (cache filter → KV CAS → cache CAS)
  // lock_success=true → lock acquired, proceed; false → state changed, skip
  int acquire_lock_(SCN &cur_scn, bool &lock_success);

  // Wait until every transaction that could have observed the old centroid
  // layout before it was made non-writable has finished.
  int wait_pre_cutover_writers_ended_();

  // Phase 2: Process vectors in batches
  int process_batches_();

  // Publish batch progress and copy the pagination cursor before releasing all
  // batch-local allocations. The allocator is reused on both success and
  // failure so no vector/rowkey/group storage can accumulate across batches.
  static int finish_batch_iteration_(int batch_ret,
                                     const char *next_rowkey,
                                     int64_t next_rowkey_len,
                                     bool is_last_batch,
                                     int64_t processed_count,
                                     char *last_rowkey_buf,
                                     int64_t last_rowkey_buf_len,
                                     const char *&last_rowkey,
                                     int64_t &last_rowkey_len,
                                     int64_t &total_processed,
                                     common::ObIAllocator &allocator);

  // Process a single batch
  // @param [in] last_rowkey Last rowkey for pagination
  // @param [in] last_rowkey_len Length of last rowkey
  // @param [out] is_last_batch Whether this is the last batch
  // @param [out] processed_count Number of vectors processed in this batch
  // @return OB_SUCCESS on success
  int process_one_batch_(const char *last_rowkey,
                         int64_t last_rowkey_len,
                         char *next_rowkey_buf,
                         int64_t &next_rowkey_len,
                         bool &is_last_batch,
                         int64_t &processed_count,
                         common::ObIAllocator &allocator);

  // Handle empty batch (no remaining vectors)
  int handle_empty_batch_();

  // Rollback centroid lock (KV + cache) on benign skip
  int rollback_lock_();

  // Rebuild in-memory posting counts from persistent aux_type=05 rows after a
  // KV commit succeeded but a derived cache update failed.
  int reconcile_cache_posting_counts_(const common::ObIArray<int64_t> &affected_cids);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  uint64_t index_table_id_;
  uint64_t queue_tablet_id_;
  int64_t center_id_;
  int64_t ns_id_;
  uint64_t ltable_id_;
  ObSpfreshTaskQueue *task_queue_;
  ObSpfreshTableHelper table_helper_;
  ObIvfMapCentCache *cache_;
  ObVectorIndexDistAlgorithm dist_algo_;
  share::ObLSID physical_ls_id_;
  common::ObTabletID physical_tablet_id_;
  int64_t cutover_modify_ts_;
  bool has_committed_batch_;

  // State tracking
  SCN lock_scn_;
  ObIvfCentroidState lock_old_state_;

  // Execution output (populated during execute())
  int64_t total_processed_;

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshReassignTask);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_REASSIGN_TASK_H_
