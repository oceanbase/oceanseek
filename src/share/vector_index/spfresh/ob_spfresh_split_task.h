/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_SPLIT_TASK_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_SPLIT_TASK_H_

#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "share/vector_index/spfresh/ob_spfresh_task_queue.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_vector_kmeans_ctx.h"
#include "share/scn.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{

// Forward declarations
class ObSpfreshTaskQueue;
class ObSpfreshTriggerCallback;

// Split task implementation
// Implements the multi-phase Split process:
// Phase 1: Acquire lock (UPDATE state=SPLITTING WHERE state=NORMAL)
// Phase 2: Read all vectors from old head
// Phase 3: K-means clustering (nlist-means for first split, 2-means otherwise)
// Phase 4: Large transaction (INSERT N new centroids, migrate vectors, UPDATE state=SPLIT_DONE)
// Phase 5: Commit callback (add_centroid xN, push Reassign task)
class ObSpfreshSplitTask
{
public:
  ObSpfreshSplitTask();
  ~ObSpfreshSplitTask();

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
           const ObString &vec_index_param_str,
           ObVectorIndexDistAlgorithm dist_algo = ObVectorIndexDistAlgorithm::VIDA_L2);
  void destroy();
  void set_cache_guard(const ObIvfCacheMgrGuard &guard) { cache_guard_ = guard; }

  // Execute the split task
  // @return OB_SUCCESS on success, error code on failure
  int execute(const common::ObTabletID &physical_tablet_id);

  // Get the center_id being processed
  int64_t get_center_id() const { return center_id_; }

  // Getters for task execution output (valid after execute() returns)
  int64_t get_source_vector_count() const { return source_vector_count_; }
  const common::ObIArray<int64_t> &get_child_center_ids() const { return child_center_ids_; }
  const common::ObIArray<int64_t> &get_child_vector_counts() const { return child_vector_counts_; }

private:
  typedef ObArray<ObArray<ObSpfreshVectorRow>> ObSpfreshChildVectorArray;

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
  int execute_locked_(const common::ObTabletID &physical_tablet_id,
                      ObArray<float *> &new_centroids,
                      bool &lock_acquired);
  int finish_split_(int split_ret,
                    bool should_split,
                    bool can_split,
                    bool &lock_acquired);

  // Phase 2: Read all vectors from old head
  int read_vectors_(ObIArray<ObSpfreshVectorRow> &vectors, int64_t &vector_dim,
                    common::ObIAllocator &allocator);

  // Revalidate the source size and center deficit after locking the source.
  int validate_split_(int64_t source_count, bool &should_split);

  // Phase 3: K-means clustering (generalized to num_clusters)
  // Routes to parallel Elkan k-means (first split) or naive k-means (2-means)
  int do_kmeans_(const ObIArray<ObSpfreshVectorRow> &vectors,
                 int64_t vector_dim,
                 int64_t num_clusters,
                 ObArray<float *> &new_centroids,
                 ObArray<int64_t> &assignments);
  // Parallel Elkan k-means for first split (num_clusters > 2)
  int do_kmeans_parallel_(const ObIArray<ObSpfreshVectorRow> &vectors,
                          int64_t vector_dim,
                          int64_t num_clusters,
                          ObArray<float *> &new_centroids,
                          ObArray<int64_t> &assignments);
  int append_kmeans_samples_(ObSingleKmeansExecutor &executor,
                             const ObIArray<ObSpfreshVectorRow> &vectors);
  int copy_kmeans_centroids_(ObSingleKmeansExecutor &executor,
                             const ObIArray<ObSpfreshVectorRow> &vectors,
                             int64_t vector_dim,
                             int64_t num_clusters,
                             ObArray<float *> &new_centroids);
  int assign_vectors_to_centroids_(const ObIArray<ObSpfreshVectorRow> &vectors,
                                   int64_t vector_dim,
                                   const ObArray<float *> &centroids,
                                   ObArray<int64_t> &assignments);
  // Naive k-means for regular 2-means split (original implementation)
  int do_kmeans_naive_(const ObIArray<ObSpfreshVectorRow> &vectors,
                       int64_t vector_dim,
                       int64_t num_clusters,
                       ObArray<float *> &new_centroids,
                       ObArray<int64_t> &assignments);
  int init_naive_centroids_(const ObIArray<ObSpfreshVectorRow> &vectors,
                            int64_t vector_dim,
                            int64_t num_clusters,
                            ObArray<float *> &centroids);
  int run_naive_kmeans_(const ObIArray<ObSpfreshVectorRow> &vectors,
                        int64_t vector_dim,
                        ObArray<float *> &centroids,
                        ObArray<int64_t> &assignments);
  int assign_naive_clusters_(const ObIArray<ObSpfreshVectorRow> &vectors,
                             int64_t vector_dim,
                             const ObArray<float *> &centroids,
                             float *norm_buf,
                             bool is_initial,
                             bool &assignment_changed,
                             ObArray<int64_t> &assignments);
  int update_naive_centroids_(const ObIArray<ObSpfreshVectorRow> &vectors,
                              int64_t vector_dim,
                              const ObArray<int64_t> &assignments,
                              ObArray<float *> &centroids);
  // Remove empty clusters and reject geometrically degenerate split results.
  int compact_and_validate_clusters_(ObArray<float *> &new_centroids,
                                     ObArray<int64_t> &assignments,
                                     int64_t source_count,
                                     bool &can_split);

  // Phase 4: Large transaction to migrate vectors.
  // Allocates child center_ids from KV max under a per-tablet mutex (held by the
  // task's queue), then writes. No process-wide lock: center_id is unique per
  // centroid tablet, so unrelated tablets/tenants proceed in parallel.
  int migrate_vectors_(const common::ObTabletID &physical_tablet_id,
                       ObArray<float *> &new_centroids,
                       int64_t vector_dim,
                       bool &can_split);
  // Re-read and assign the current source while holding the migration tablet lock.
  int prepare_split_source_(ObArray<float *> &new_centroids,
                            int64_t vector_dim,
                            common::ObIAllocator &allocator,
                            ObArray<ObSpfreshVectorRow> &vectors,
                            ObArray<int64_t> &assignments,
                            bool &can_split);
  int allocate_child_center_ids_(int64_t num_children);
  int insert_child_centroids_(int64_t num_children,
                              const ObArray<float *> &new_centroids,
                              int64_t vector_dim,
                              SCN &commit_scn);
  int build_child_vectors_(int64_t num_children,
                           const ObIArray<ObSpfreshVectorRow> &vectors,
                           const ObArray<int64_t> &assignments,
                           ObSpfreshChildVectorArray &child_vectors,
                           ObArray<ObString> &rowkeys_to_delete);
  int delete_split_source_vectors_(const ObIArray<ObString> &rowkeys,
                                   SCN &commit_scn);
  int insert_split_child_vectors_(const ObSpfreshChildVectorArray &child_vectors,
                                  SCN &commit_scn);
  int update_split_rowid_cids_(const ObSpfreshChildVectorArray &child_vectors,
                               const ObIArray<ObString> &rowkeys,
                               SCN &commit_scn);
  int update_split_source_state_(SCN &commit_scn);

  // Phase 5: Commit callback
  int commit_callback_(const ObIArray<int64_t> &child_center_ids,
                       const ObArray<float *> &new_centroids,
                       int64_t vector_dim);

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
  int64_t center_id_;
  int64_t ns_id_;
  uint64_t ltable_id_;
  ObSpfreshTaskQueue *task_queue_;
  ObSpfreshTableHelper table_helper_;
  ObIvfMapCentCache *cache_;
  ObIvfCacheMgrGuard cache_guard_;
  common::ObSqlString vec_index_param_str_;
  ObVectorIndexDistAlgorithm dist_algo_;

  // State tracking
  SCN lock_scn_;
  int64_t cutover_modify_ts_;

  // Execution output (populated during execute())
  int64_t source_vector_count_;
  common::ObSEArray<int64_t, 16> child_center_ids_;
  common::ObSEArray<int64_t, 16> child_vector_counts_;

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshSplitTask);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_SPLIT_TASK_H_
