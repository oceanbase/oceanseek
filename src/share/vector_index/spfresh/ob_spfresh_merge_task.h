/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_MERGE_TASK_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_MERGE_TASK_H_

#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "share/vector_index/spfresh/ob_spfresh_task_queue.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/scn.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace share
{

// Forward declarations
class ObSpfreshTaskQueue;

// Merge task implementation
// Implements the 4-phase Merge process:
// Phase 1: Acquire lock A (UPDATE state=MERGING WHERE state=NORMAL)
// Phase 2: Find nearest NORMAL centroid B as merge target
// Phase 3: Large transaction (A vectors migrate to B, A state=MERGED)
// Phase 4: Commit callback (update posting_list_length, push Reassign task)
class ObSpfreshMergeTask
{
public:
  ObSpfreshMergeTask();
  ~ObSpfreshMergeTask();

  int init(uint64_t tenant_id,
           common::ObMySQLProxy *sql_proxy,
           uint64_t index_table_id,
           const ObSpfreshKvTableInfo &kv_info,
           uint64_t queue_tablet_id,
           int64_t source_center_id,
           int64_t ns_id,
           uint64_t ltable_id,
           ObSpfreshTaskQueue *task_queue,
           ObIvfMapCentCache *cache,
           ObVectorIndexDistAlgorithm dist_algo = ObVectorIndexDistAlgorithm::VIDA_L2);
  void destroy();
  void set_cache_guard(const ObIvfCacheMgrGuard &guard) { cache_guard_ = guard; }

  // Execute the merge task
  // @return OB_SUCCESS on success, error code on failure
  int execute(const common::ObTabletID &physical_tablet_id);

  // Get the source center_id being processed
  int64_t get_source_center_id() const { return source_center_id_; }

  // Getters for task execution output (valid after execute() returns)
  int64_t get_target_center_id() const { return target_center_id_; }
  int64_t get_migrated_vector_count() const { return migrated_count_; }

private:
  typedef common::hash::ObHashMap<
      int64_t, common::ObSEArray<ObSpfreshVectorRow, 8>> ObSpfreshVectorGroupMap;

  class CountNormalCentroidFn
  {
  public:
    CountNormalCentroidFn(int64_t &normal_count, int64_t &normal_vector_count);
    int operator()(int64_t cid, const ObCentroidInfo &info);

  private:
    int64_t &normal_count_;
    int64_t &normal_vector_count_;
    DISALLOW_COPY_AND_ASSIGN(CountNormalCentroidFn);
  };

  // Phase 1: Acquire lock with small transaction
  // lock_success=true → lock acquired, proceed; false → state changed, skip
  int acquire_lock_(SCN &cur_scn, bool &lock_success);

  // Revalidate the source size and center floor after locking the source.
  int validate_merge_(int64_t &source_count, int64_t &merge_capacity);

  // Find the nearest NORMAL centroid with enough remaining capacity.
  int find_target_(int64_t max_target_count, int64_t &target_center_id);

  // Find the nearest NORMAL target that can accommodate the source batch.
  int find_vector_target_(const ObSpfreshVectorRow &row,
                          int64_t max_target_count,
                          common::ObIAllocator &allocator,
                          int64_t &target_center_id);

  int build_target_groups_(const common::ObIArray<ObSpfreshVectorRow> &vectors,
                           int64_t max_target_count,
                           common::ObIAllocator &allocator,
                           ObSpfreshVectorGroupMap &cid_groups);
  int insert_group_cid_vectors_(ObSpfreshVectorGroupMap &cid_groups,
                                SCN &commit_scn);
  int insert_group_rowid_cids_(ObSpfreshVectorGroupMap &cid_groups,
                               SCN &commit_scn);

  // Phase 3: Large transaction to migrate vectors
  int migrate_vectors_(const common::ObTabletID &physical_tablet_id,
                       int64_t target_center_id,
                       common::ObIAllocator &allocator);
  int prepare_merge_source_(common::ObIAllocator &allocator,
                            common::ObArray<ObSpfreshVectorRow> &vectors,
                            common::ObArray<common::ObString> &rowkeys);
  int delete_merge_source_vectors_(const common::ObIArray<common::ObString> &rowkeys,
                                   SCN &commit_scn);
  int delete_merge_source_rowid_cids_(
      const common::ObIArray<common::ObString> &rowkeys,
                                      SCN &commit_scn);
  int update_merge_source_state_(SCN &commit_scn);

  // Phase 4: Commit callback
  int commit_callback_(int64_t target_center_id, int64_t migrated_count);

  // Rollback on failure
  int rollback_lock_();

  // Push task to queue for retry
  int push_retry_task_();

  // Push reassign task for lazy recovery
  int push_reassign_();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  uint64_t index_table_id_;
  uint64_t queue_tablet_id_;
  int64_t source_center_id_;
  int64_t ns_id_;
  uint64_t ltable_id_;
  ObSpfreshTaskQueue *task_queue_;
  ObSpfreshTableHelper table_helper_;
  ObIvfMapCentCache *cache_;
  ObIvfCacheMgrGuard cache_guard_;
  ObVectorIndexDistAlgorithm dist_algo_;

  // State tracking
  SCN lock_scn_;
  int64_t cutover_modify_ts_;

  // Execution output (populated during execute())
  int64_t target_center_id_;
  int64_t migrated_count_;
  common::ObSEArray<int64_t, 8> target_center_ids_;
  common::ObSEArray<int64_t, 8> target_vector_counts_;

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshMergeTask);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_MERGE_TASK_H_
