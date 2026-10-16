/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_split_task.h"
#include "share/vector_index/spfresh/ob_spfresh_policy.h"
#include "share/vector_index/spfresh/ob_spfresh_task_queue.h"
#include "share/vector_index/spfresh/ob_spfresh_vector_dist_util.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_mutex.h"
#include "share/parameter/ob_parameter_macro.h"
#include "share/ob_debug_sync.h"
#include "share/location_cache/ob_location_service.h"
#include "observer/ob_server_struct.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_kmeans_ctx.h"

namespace oceanbase
{
namespace share
{

ObSpfreshSplitTask::ObSpfreshSplitTask()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    sql_proxy_(nullptr),
    index_table_id_(OB_INVALID_ID),
    queue_tablet_id_(OB_INVALID_ID),
    center_id_(OB_INVALID_ID),
    ns_id_(ObIvfCacheMgrKey::INVALID_NS_ID),
    ltable_id_(OB_INVALID_LOGICAL_TABLE_ID),
    task_queue_(nullptr),
    table_helper_(),
    cache_(nullptr),
    vec_index_param_str_(),
    dist_algo_(ObVectorIndexDistAlgorithm::VIDA_L2),
    lock_scn_(),
    cutover_modify_ts_(0),
    source_vector_count_(0)
{
}

ObSpfreshSplitTask::~ObSpfreshSplitTask()
{
  destroy();
}

int ObSpfreshSplitTask::init(uint64_t tenant_id,
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
                             ObVectorIndexDistAlgorithm dist_algo)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSpfreshSplitTask init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(sql_proxy) ||
             OB_INVALID_ID == index_table_id || OB_INVALID_ID == queue_tablet_id ||
             OB_INVALID_ID == center_id || OB_ISNULL(task_queue) ||
             OB_ISNULL(cache) || vec_index_param_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(sql_proxy), K(index_table_id),
             K(queue_tablet_id), K(center_id), KP(task_queue), KP(cache),
             K(vec_index_param_str.length()));
  } else if (OB_FAIL(vec_index_param_str_.assign(vec_index_param_str))) {
    LOG_WARN("failed to copy vector index param", K(ret), K(center_id));
  } else if (OB_FAIL(table_helper_.init(tenant_id, sql_proxy, kv_info))) {
    LOG_WARN("failed to init table helper", K(ret));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    index_table_id_ = index_table_id;
    queue_tablet_id_ = queue_tablet_id;
    center_id_ = center_id;
    ns_id_ = ns_id;
    ltable_id_ = ltable_id;
    task_queue_ = task_queue;
    cache_ = cache;
    dist_algo_ = dist_algo;
    is_inited_ = true;
  }
  return ret;
}

void ObSpfreshSplitTask::destroy()
{
  if (is_inited_) {
    table_helper_.destroy();
    is_inited_ = false;
    tenant_id_ = OB_INVALID_ID;
    sql_proxy_ = nullptr;
    index_table_id_ = OB_INVALID_ID;
    queue_tablet_id_ = OB_INVALID_ID;
    center_id_ = OB_INVALID_ID;
    ns_id_ = ObIvfCacheMgrKey::INVALID_NS_ID;
    ltable_id_ = OB_INVALID_LOGICAL_TABLE_ID;
    task_queue_ = nullptr;
    cache_ = nullptr;
    cache_guard_.reset();
    vec_index_param_str_.reset();
    child_center_ids_.reset();
    child_vector_counts_.reset();
    dist_algo_ = ObVectorIndexDistAlgorithm::VIDA_L2;
    cutover_modify_ts_ = 0;
  }
}

int ObSpfreshSplitTask::execute(const common::ObTabletID &physical_tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObArray<float *> new_centroids;
  bool lock_acquired = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshSplitTask not init", K(ret));
  } else if (!physical_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid physical split tablet", K(ret), K(physical_tablet_id));
  } else if (OB_FAIL(acquire_lock_(lock_scn_, lock_acquired))) {
    // A state mismatch means the centroid already advanced or another worker
    // won the CAS, so this attempt is an idempotent skip rather than a failure.
    if (OB_STATE_NOT_MATCH == ret || OB_ENTRY_NOT_EXIST == ret) {
      LOG_TRACE("[SPFRESH_TRACE] centroid state not ready, benign skip split",
                KR(ret), K_(center_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to acquire lock", K(ret), K_(center_id));
    }
  } else if (!lock_acquired) {
    LOG_TRACE("[SPFRESH_TRACE] centroid state changed, skip split", K_(center_id));
  } else if (OB_FAIL(execute_locked_(physical_tablet_id, new_centroids, lock_acquired))) {
    LOG_WARN("failed to execute locked split", K(ret), K_(center_id));
  }

  // Free centroid memory allocated by do_kmeans_.
  for (int64_t i = 0; i < new_centroids.count(); ++i) {
    if (OB_NOT_NULL(new_centroids.at(i))) {
      ob_free(new_centroids.at(i));
    }
  }
  new_centroids.reset();

  int64_t elapsed_us = ObTimeUtility::current_time() - start_time;
  LOG_TRACE("[SPFRESH_TRACE] Split task done", KR(ret), K_(center_id), K(elapsed_us));

  return ret;
}

int ObSpfreshSplitTask::execute_locked_(
    const common::ObTabletID &physical_tablet_id,
    ObArray<float *> &new_centroids,
    bool &lock_acquired)
{
  int ret = OB_SUCCESS;
  int64_t vector_dim = 0;
  int64_t num_children = 2;
  int64_t centroid_count = 0;
  int64_t nlist = 0;
  bool should_split = true;
  bool can_split = false;
  ObArray<ObSpfreshVectorRow> vectors;
  ObArray<int64_t> assignments;
  ObArenaAllocator vec_allocator("SpfreshSpV", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);

  if (OB_FAIL(read_vectors_(vectors, vector_dim, vec_allocator))) {
    LOG_WARN("failed to read vectors", K(ret), K_(center_id));
  } else if (vectors.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no vectors to split", K(ret), K_(center_id));
  } else {
    source_vector_count_ = vectors.count();
    LOG_TRACE("[SPFRESH_TRACE] read vectors for split", K_(center_id),
              K_(source_vector_count), K(vector_dim),
              "first_vector_len", vectors.at(0).vector_len_);
  }
  if (OB_SUCC(ret) && OB_FAIL(validate_split_(vectors.count(), should_split))) {
    LOG_WARN("failed to validate split policy", K(ret), K_(center_id));
  } else if (OB_SUCC(ret) && !should_split) {
    if (OB_FAIL(rollback_lock_())) {
      LOG_WARN("failed to rollback skipped split", K(ret), K_(center_id));
    } else {
      lock_acquired = false;
      LOG_TRACE("[SPFRESH_TRACE] split condition changed, skip",
                K_(center_id), "physical_count", vectors.count());
    }
  }

  child_center_ids_.reset();
  child_vector_counts_.reset();
  if (OB_SUCC(ret) && should_split) {
    centroid_count = cache_->get_count();
    nlist = cache_->get_nlist_param();
    if (0 == center_id_ && 1 == centroid_count) {
      num_children = ObSpfreshPolicy::get_bootstrap_center_count(vectors.count(), nlist);
      LOG_TRACE("[SPFRESH_TRACE] progressive bootstrap split", K_(center_id),
                K(centroid_count), K(num_children));
    }
    if (OB_FAIL(do_kmeans_(vectors, vector_dim, num_children,
                           new_centroids, assignments))) {
      LOG_WARN("failed to do k-means clustering", K(ret), K_(center_id), K(num_children));
    } else if (OB_FAIL(compact_and_validate_clusters_(
        new_centroids, assignments, vectors.count(), can_split))) {
      LOG_WARN("failed to validate split clusters", K(ret), K_(center_id));
    }
  }

  if (OB_SUCC(ret) && can_split) {
    // The training snapshot must never be used as migration input. Release it
    // before taking the tablet lock and reading the current source again.
    vectors.reset();
    assignments.reset();
    vec_allocator.reuse();
    DEBUG_SYNC(SPFRESH_SPLIT_BEFORE_TABLET_LOCK);
    if (OB_FAIL(migrate_vectors_(physical_tablet_id, new_centroids, vector_dim, can_split))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to migrate vectors", K(ret), K_(center_id), K_(child_center_ids));
      }
    }
  }
  if (OB_SUCC(ret) && can_split) {
    DEBUG_SYNC(SPFRESH_SPLIT_BEFORE_CACHE_PUBLISH);
    if (OB_FAIL(commit_callback_(child_center_ids_, new_centroids, vector_dim))) {
      LOG_WARN("failed to commit callback", K(ret), K_(center_id));
    }
  }
  ret = finish_split_(ret, should_split, can_split, lock_acquired);
  return ret;
}

int ObSpfreshSplitTask::finish_split_(
    int split_ret,
    bool should_split,
    bool can_split,
    bool &lock_acquired)
{
  int ret = split_ret;
  int tmp_ret = OB_SUCCESS;
  int rollback_ret = OB_SUCCESS;
  int push_ret = OB_SUCCESS;
  int64_t retry_after_us = 0;

  // Keep a degenerate source queryable and writable. An in-memory cooldown
  // prevents a scheduling loop without leaving it in SPLITTING indefinitely.
  if (OB_SUCC(ret) && should_split && !can_split) {
    retry_after_us = ObTimeUtility::current_time()
        + ObSpfreshPolicy::get_split_retry_interval_us();
    tmp_ret = cache_->set_split_retry_after_us(center_id_, retry_after_us);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to set degenerate split cooldown", K(tmp_ret),
               K_(center_id), K(retry_after_us));
    }
    if (OB_SUCCESS != (tmp_ret = rollback_lock_())) {
      ret = tmp_ret;
      LOG_WARN("failed to rollback degenerate split lock", K(ret), K_(center_id));
    } else {
      LOG_INFO("degenerate split skipped with cooldown", K_(center_id),
               K_(source_vector_count), K(retry_after_us));
    }
  } else if (OB_FAIL(ret) && lock_acquired) {
    rollback_ret = rollback_lock_();
    if (OB_SUCCESS != rollback_ret) {
      LOG_ERROR("failed to rollback split lock", K(rollback_ret), K_(center_id));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_ENTRY_NOT_EXIST != split_ret) {
      push_ret = push_retry_task_();
      if (OB_SUCCESS != push_ret) {
        LOG_WARN("failed to push retry task", K(push_ret), K_(center_id));
      }
    }
  }
  return ret;
}

int ObSpfreshSplitTask::acquire_lock_(SCN &cur_scn, bool &lock_success)
{
  int ret = OB_SUCCESS;
  cur_scn = SCN::min_scn();
  lock_success = false;
  const char *lock_source = "none";

  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null", K(ret));
  } else {
    ObCentroidInfo centroid_info;
    if (OB_FAIL(cache_->get_center_info(center_id_, SCN::max_scn(), centroid_info))) {
      LOG_WARN("failed to get centroid info", K(ret), K_(center_id));
    } else if (ObIvfCentroidState::SPLIT_DONE == centroid_info.state_) {
      int push_ret = OB_SUCCESS;
      if (OB_SUCCESS != (push_ret = push_reassign_())) {
        LOG_WARN("failed to push reassign for SPLIT_DONE centroid", K(push_ret), K_(center_id));
      }
      ret = OB_STATE_NOT_MATCH;
      LOG_TRACE("[SPFRESH_TRACE] centroid already SPLIT_DONE, skip split", KR(ret), K_(center_id));
    } else {
      if (0 == center_id_) {
        // Bootstrap centroid (center_id=0): cache-only CAS.
        // This centroid only exists in cache, not in KV table.
        bool cas_ok = false;
        if (OB_FAIL(cache_->update_centroid_state_cas(center_id_,
            ObIvfCentroidState::NORMAL, ObIvfCentroidState::SPLITTING, cas_ok))) {
          LOG_WARN("failed to cas update centroid state in cache", K(ret), K_(center_id));
        } else {
          lock_success = cas_ok;
          lock_source = "bootstrap";
        }
      } else if (OB_FAIL(table_helper_.start_batch_transaction())) {
        LOG_WARN("start batch tx failed", K(ret));
      } else {
        // KV centroid (center_id!=0): KV CAS within batch transaction
        int64_t affected_rows = 0;
        if (OB_FAIL(table_helper_.update_centroid_value_cas(center_id_,
            ObIvfCentroidState::NORMAL, ObIvfCentroidState::SPLITTING, affected_rows))) {
          LOG_WARN("failed to cas update centroid value in KV", K(ret), K_(center_id));
        } else if (1 == affected_rows) {
          lock_success = true;
          lock_source = "kv";
        } else {
          LOG_TRACE("[SPFRESH_TRACE] KV CAS failed, centroid state changed", KR(ret), K_(center_id), K(affected_rows));
        }
        int end_ret = OB_SUCCESS;
        if (OB_SUCCESS != (end_ret = table_helper_.end_batch_transaction(
            lock_success && OB_SUCC(ret), cur_scn))) {
          LOG_WARN("end batch tx failed", K(end_ret));
          if (OB_SUCC(ret)) {
            ret = end_ret;
          }
          lock_success = false;
        }
        if (OB_SUCC(ret) && lock_success) {
          // Sync cache state after KV commit
          bool cas_ok = false;
          if (OB_FAIL(cache_->update_centroid_state_cas(center_id_,
              ObIvfCentroidState::NORMAL, ObIvfCentroidState::SPLITTING, cas_ok))) {
            LOG_WARN("failed to sync cache after KV CAS", K(ret), K_(center_id));
            lock_success = false;
            SCN rollback_scn;
            int kv_ret = OB_SUCCESS;
            if (OB_SUCCESS != (kv_ret = table_helper_.update_centroid_state(
                center_id_, ObIvfCentroidState::NORMAL, ObIvfCentroidState::SPLITTING,
                rollback_scn))) {
              LOG_ERROR("failed to rollback KV after cache sync failure, KV/cache inconsistent",
                        K(kv_ret), K_(center_id));
            }
          } else if (!cas_ok) {
            lock_success = false;
            LOG_TRACE("[SPFRESH_TRACE] cache CAS not ok, another worker won", KR(ret), K_(center_id));
            SCN rollback_scn;
            int kv_ret = OB_SUCCESS;
            if (OB_SUCCESS != (kv_ret = table_helper_.update_centroid_state(
                center_id_, ObIvfCentroidState::NORMAL, ObIvfCentroidState::SPLITTING,
                rollback_scn))) {
              LOG_ERROR("failed to rollback KV after cache CAS miss, KV/cache inconsistent",
                        K(kv_ret), K_(center_id));
            }
          }
        }
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] split lock acquired done", KR(ret), K_(center_id),
    K(lock_success), "source", lock_source);
  return ret;
}

ObSpfreshSplitTask::CountNormalCentroidFn::CountNormalCentroidFn(
    int64_t &normal_count,
    int64_t &normal_vector_count)
    : normal_count_(normal_count),
      normal_vector_count_(normal_vector_count)
{}

int ObSpfreshSplitTask::CountNormalCentroidFn::operator()(
    int64_t cid,
    const ObCentroidInfo &info)
{
  UNUSED(cid);
  if (ObIvfCentroidState::NORMAL == info.state_) {
    ++normal_count_;
    normal_vector_count_ += info.posting_list_count_;
  }
  return OB_SUCCESS;
}

int ObSpfreshSplitTask::validate_split_(int64_t source_count, bool &should_split)
{
  int ret = OB_SUCCESS;
  int64_t normal_count = 0;
  int64_t normal_vector_count = 0;

  should_split = false;
  if (OB_ISNULL(cache_) || source_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split validation argument", K(ret), KP_(cache), K(source_count));
  } else if (0 == center_id_) {
    should_split = cache_->get_nlist_param() >= 2
        && source_count >= ObSpfreshPolicy::get_bootstrap_trigger_rows();
  } else {
    CountNormalCentroidFn count_fn(normal_count, normal_vector_count);
    if (OB_FAIL(cache_->for_each_centroid_info(SCN::max_scn(), count_fn))) {
      LOG_WARN("failed to count normal centroids for split", K(ret));
    } else {
      int64_t active_count = normal_count + 1;
      int64_t live_count = normal_vector_count + source_count;
      int64_t desired_count = ObSpfreshPolicy::get_desired_center_count(
          live_count, cache_->get_nlist_param());
      should_split = source_count > ObSpfreshPolicy::get_split_high_rows()
          || active_count < desired_count;
      LOG_TRACE("[SPFRESH_TRACE] split policy rechecked", K_(center_id),
                K(source_count), K(active_count), K(desired_count), K(should_split));
    }
  }
  return ret;
}

int ObSpfreshSplitTask::read_vectors_(ObIArray<ObSpfreshVectorRow> &vectors, int64_t &vector_dim,
                                      common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t expected_vector_len = 0;
  vectors.reset();
  vector_dim = 0;

  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null while reading split vectors", K(ret), K_(center_id));
  } else if (FALSE_IT(vector_dim = cache_->get_cent_vec_dim())) {
  } else if (OB_FAIL(ObSpfreshTableHelper::get_raw_vector_expected_bytes(
                 vector_dim, expected_vector_len))) {
    LOG_WARN("invalid split vector dimension from cache", K(ret), K_(center_id),
             K(vector_dim));
  } else if (OB_FAIL(table_helper_.scan_cid_vector_by_center(
                 center_id_, vector_dim, allocator, vectors))) {
    LOG_WARN("failed to scan vectors", K(ret), K_(center_id), K(vector_dim));
  } else {
    // The cache dimension is authoritative. Do not infer the business
    // dimension from the first payload, which could itself be corrupt.
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
      ObSpfreshVectorRow &row = vectors.at(i);
      if (OB_ISNULL(row.vector_) || expected_vector_len != row.vector_len_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("inconsistent split vector length",
                 K(ret), K_(center_id), K(i), KP(row.vector_),
                 K(row.vector_len_), K(expected_vector_len));
      } else {
        row.rowkey_ = row.rowkey_buf_;
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] read vectors for split done", KR(ret), K_(center_id), K(vectors.count()), K(vector_dim));
  return ret;
}

int ObSpfreshSplitTask::do_kmeans_(const ObIArray<ObSpfreshVectorRow> &vectors,
                                   int64_t vector_dim,
                                   int64_t num_clusters,
                                   ObArray<float *> &new_centroids,
                                   ObArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  new_centroids.reset();
  assignments.reset();

  if (vectors.empty() || vector_dim <= 0 || num_clusters < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(vectors.count()), K(vector_dim), K(num_clusters));
  } else if (num_clusters > 2) {
    // First split: use parallel Elkan k-means
    if (OB_FAIL(do_kmeans_parallel_(vectors, vector_dim, num_clusters,
                                    new_centroids, assignments))) {
      LOG_WARN("failed to do parallel k-means", K(ret), K_(center_id), K(num_clusters));
    }
  } else {
    // Regular 2-means or no build helper: use naive k-means
    if (OB_FAIL(do_kmeans_naive_(vectors, vector_dim, num_clusters,
                                  new_centroids, assignments))) {
      LOG_WARN("failed to do naive k-means", K(ret), K_(center_id), K(num_clusters));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] k-means completed", KR(ret), K_(center_id), K(num_clusters),
            "total_vectors", assignments.count());
  return ret;
}

int ObSpfreshSplitTask::do_kmeans_parallel_(const ObIArray<ObSpfreshVectorRow> &vectors,
                                            int64_t vector_dim,
                                            int64_t num_clusters,
                                            ObArray<float *> &new_centroids,
                                            ObArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  common::ObIAllocator *helper_allocator =
      OB_NOT_NULL(service) ? &service->get_ivf_allocator() : nullptr;
  ObIvfFlatBuildHelper local_build_helper(helper_allocator, tenant_id_);
  ObSingleKmeansExecutor *executor = nullptr;
  ObString vec_index_param_str = vec_index_param_str_.string();
  new_centroids.reset();
  assignments.reset();

  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service is null", K(ret), K_(center_id));
  } else if (OB_FAIL(local_build_helper.init(
      vec_index_param_str,
      service->get_memory_context(),
      service->get_all_vsag_use_mem()))) {
    LOG_WARN("failed to init local ivf build helper", K(ret), K_(center_id));
  } else if (OB_FAIL(local_build_helper.set_kmeans_cluster_count(num_clusters))) {
    LOG_WARN("failed to set bootstrap cluster count", K(ret),
             K_(center_id), K(num_clusters));
  } else if (OB_FAIL(local_build_helper.init_ctx(vector_dim))) {
    LOG_WARN("failed to init local kmeans ctx", K(ret), K(vector_dim), K_(center_id));
  } else if (OB_ISNULL(executor = local_build_helper.get_kmeans_ctx())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("local kmeans executor is null", K(ret), K_(center_id));
  }

  if (OB_SUCC(ret) && OB_FAIL(append_kmeans_samples_(*executor, vectors))) {
    LOG_WARN("failed to append kmeans samples", K(ret), K_(center_id));
  } else if (OB_SUCC(ret) && executor->is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("kmeans executor has no samples after feeding", K(ret), K_(center_id));
  } else if (OB_SUCC(ret) && OB_FAIL(executor->build(nullptr /* insert_monitor */))) {
    LOG_WARN("failed to build kmeans", K(ret), K_(center_id));
  } else if (OB_SUCC(ret) && OB_FAIL(copy_kmeans_centroids_(
      *executor, vectors, vector_dim, num_clusters, new_centroids))) {
    LOG_WARN("failed to copy kmeans centroids", K(ret), K_(center_id));
  } else if (OB_SUCC(ret) && OB_FAIL(assign_vectors_to_centroids_(
      vectors, vector_dim, new_centroids, assignments))) {
    LOG_WARN("failed to assign vectors to centroids", K(ret), K_(center_id));
  }

  // Error cleanup: free allocated centroids
  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < new_centroids.count(); ++i) {
      if (OB_NOT_NULL(new_centroids.at(i))) {
        ob_free(new_centroids.at(i));
      }
    }
    new_centroids.reset();
    assignments.reset();
  }

  return ret;
}

int ObSpfreshSplitTask::append_kmeans_samples_(
    ObSingleKmeansExecutor &executor,
    const ObIArray<ObSpfreshVectorRow> &vectors)
{
  int ret = OB_SUCCESS;
  float *vec = nullptr;

  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    vec = reinterpret_cast<float *>(const_cast<char *>(vectors.at(i).vector_));
    if (OB_FAIL(executor.append_sample_vector(vec))) {
      LOG_WARN("failed to append sample vector", K(ret), K(i), K_(center_id));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("[SPFRESH_TRACE] sample vectors fed to kmeans executor",
              K_(center_id), "total_vectors", vectors.count(),
              "max_sample_count", executor.get_max_sample_count());
  }
  return ret;
}

int ObSpfreshSplitTask::copy_kmeans_centroids_(
    ObSingleKmeansExecutor &executor,
    const ObIArray<ObSpfreshVectorRow> &vectors,
    int64_t vector_dim,
    int64_t num_clusters,
    ObArray<float *> &new_centroids)
{
  int ret = OB_SUCCESS;
  int get_ret = OB_SUCCESS;
  int64_t centers_count = executor.get_centers_count();
  int64_t cluster_bytes = vector_dim * sizeof(float);
  float *center = nullptr;
  float *centroid_copy = nullptr;

  LOG_TRACE("[SPFRESH_TRACE] kmeans produced centers", K_(center_id),
            K(centers_count), K(num_clusters));
  for (int64_t k = 0; OB_SUCC(ret) && k < num_clusters; ++k) {
    center = nullptr;
    centroid_copy = nullptr;
    get_ret = executor.get_center(k, center);
    centroid_copy = static_cast<float *>(ob_malloc(cluster_bytes, "SpfreshSpC"));
    if (OB_ISNULL(centroid_copy)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc centroid copy", K(ret), K(k));
    } else {
      if (OB_SUCCESS == get_ret && OB_NOT_NULL(center)) {
        MEMCPY(centroid_copy, center, cluster_bytes);
      } else {
        MEMCPY(centroid_copy, vectors.at(0).vector_, cluster_bytes);
        LOG_TRACE("[SPFRESH_TRACE] fallback centroid for empty cluster",
                  K_(center_id), K(k));
      }
      if (OB_FAIL(ObSpfreshVectorDistUtil::normalize_centroid(
          centroid_copy, vector_dim, dist_algo_))) {
        LOG_WARN("failed to normalize centroid copy", K(ret), K(k));
        ob_free(centroid_copy);
      } else if (OB_FAIL(new_centroids.push_back(centroid_copy))) {
        ob_free(centroid_copy);
      }
    }
  }
  return ret;
}

int ObSpfreshSplitTask::assign_vectors_to_centroids_(
    const ObIArray<ObSpfreshVectorRow> &vectors,
    int64_t vector_dim,
    const ObArray<float *> &centroids,
    ObArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  bool need_norm = ObSpfreshVectorDistUtil::need_query_normalize(dist_algo_);
  float *norm_buf = nullptr;
  const float *vec = nullptr;
  const float *effective_vec = nullptr;
  int64_t best_k = 0;
  float best_dist = 0;
  float dist = 0;

  if (need_norm) {
    norm_buf = static_cast<float *>(ob_malloc(vector_dim * sizeof(float), "SpfreshSpN"));
    if (OB_ISNULL(norm_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc norm buf", K(ret), K(vector_dim), K_(center_id));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    vec = reinterpret_cast<const float *>(vectors.at(i).vector_);
    effective_vec = vec;
    best_k = 0;
    best_dist = std::numeric_limits<float>::max();
    if (need_norm && OB_FAIL(ObSpfreshVectorDistUtil::prepare_query_vector(
        vec, vector_dim, dist_algo_, norm_buf, effective_vec))) {
      LOG_WARN("failed to prepare query vector", K(ret), K(i), K_(center_id));
    }
    for (int64_t k = 0; OB_SUCC(ret) && k < centroids.count(); ++k) {
      dist = ObSpfreshVectorDistUtil::calc_squared_distance(
          effective_vec, centroids.at(k), vector_dim);
      if (dist < best_dist) {
        best_dist = dist;
        best_k = k;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(assignments.push_back(best_k))) {
      LOG_WARN("failed to push assignment", K(ret), K(i));
    }
  }
  if (OB_NOT_NULL(norm_buf)) {
    ob_free(norm_buf);
  }
  return ret;
}

int ObSpfreshSplitTask::do_kmeans_naive_(const ObIArray<ObSpfreshVectorRow> &vectors,
                                         int64_t vector_dim,
                                         int64_t num_clusters,
                                         ObArray<float *> &new_centroids,
                                         ObArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  ObArray<float *> centroids;
  new_centroids.reset();
  assignments.reset();

  if (vectors.empty() || vector_dim <= 0 || num_clusters < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(vectors.count()), K(vector_dim), K(num_clusters));
  } else if (OB_FAIL(init_naive_centroids_(
      vectors, vector_dim, num_clusters, centroids))) {
    LOG_WARN("failed to initialize naive centroids", K(ret), K_(center_id));
  } else if (OB_FAIL(run_naive_kmeans_(vectors, vector_dim, centroids, assignments))) {
    LOG_WARN("failed to run naive kmeans", K(ret), K_(center_id));
  }

  if (OB_SUCC(ret)) {
    for (int64_t k = 0; k < num_clusters; ++k) {
      if (OB_FAIL(new_centroids.push_back(centroids.at(k)))) {
        for (int64_t j = k; j < num_clusters; ++j) {
          ob_free(centroids.at(j));
        }
        LOG_WARN("failed to push centroid to output", K(ret), K(k));
        break;
      }
    }
  } else {
    for (int64_t k = 0; k < centroids.count(); ++k) {
      if (OB_NOT_NULL(centroids.at(k))) {
        ob_free(centroids.at(k));
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] k-means completed", KR(ret), K_(center_id), K(num_clusters),
            "total_vectors", assignments.count());
  return ret;
}

int ObSpfreshSplitTask::init_naive_centroids_(
    const ObIArray<ObSpfreshVectorRow> &vectors,
    int64_t vector_dim,
    int64_t num_clusters,
    ObArray<float *> &centroids)
{
  int ret = OB_SUCCESS;
  int64_t cluster_bytes = vector_dim * sizeof(float);
  int64_t sample_idx = 0;
  float *centroid = nullptr;

  for (int64_t k = 0; OB_SUCC(ret) && k < num_clusters; ++k) {
    centroid = static_cast<float *>(ob_malloc(cluster_bytes, "SpfreshSplit"));
    if (OB_ISNULL(centroid)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate centroid", K(ret), K(k));
    } else {
      sample_idx = (vectors.count() * k) / num_clusters;
      MEMCPY(centroid, vectors.at(sample_idx).vector_, cluster_bytes);
      if (OB_FAIL(ObSpfreshVectorDistUtil::normalize_centroid(
          centroid, vector_dim, dist_algo_))) {
        LOG_WARN("failed to normalize initial centroid", K(ret), K(k));
        ob_free(centroid);
      } else if (OB_FAIL(centroids.push_back(centroid))) {
        ob_free(centroid);
      }
    }
  }
  return ret;
}

int ObSpfreshSplitTask::run_naive_kmeans_(
    const ObIArray<ObSpfreshVectorRow> &vectors,
    int64_t vector_dim,
    ObArray<float *> &centroids,
    ObArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  const int64_t max_iters = 100;
  bool need_norm = ObSpfreshVectorDistUtil::need_query_normalize(dist_algo_);
  bool converged = false;
  bool assignment_changed = false;
  float *norm_buf = nullptr;
  assignments.reset();

  if (need_norm) {
    norm_buf = static_cast<float *>(ob_malloc(vector_dim * sizeof(float), "SpfreshSpN"));
    if (OB_ISNULL(norm_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc norm buf", K(ret), K(vector_dim), K_(center_id));
    }
  }
  for (int64_t iter = 0; OB_SUCC(ret) && iter < max_iters && !converged; ++iter) {
    assignment_changed = false;
    if (OB_FAIL(assign_naive_clusters_(vectors, vector_dim, centroids, norm_buf,
                                      0 == iter, assignment_changed, assignments))) {
      LOG_WARN("failed to assign naive clusters", K(ret), K(iter));
    } else {
      converged = 0 < iter && !assignment_changed;
      if (!converged && OB_FAIL(update_naive_centroids_(
          vectors, vector_dim, assignments, centroids))) {
        LOG_WARN("failed to update naive centroids", K(ret), K(iter));
      }
    }
  }
  if (OB_NOT_NULL(norm_buf)) {
    ob_free(norm_buf);
  }
  return ret;
}

int ObSpfreshSplitTask::assign_naive_clusters_(
    const ObIArray<ObSpfreshVectorRow> &vectors,
    int64_t vector_dim,
    const ObArray<float *> &centroids,
    float *norm_buf,
    bool is_initial,
    bool &assignment_changed,
    ObArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  bool need_norm = ObSpfreshVectorDistUtil::need_query_normalize(dist_algo_);
  const float *vec = nullptr;
  const float *effective_vec = nullptr;
  int64_t best_k = 0;
  float best_dist = 0;
  float dist = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    vec = reinterpret_cast<const float *>(vectors.at(i).vector_);
    effective_vec = vec;
    best_k = 0;
    best_dist = std::numeric_limits<float>::max();
    if (need_norm && OB_FAIL(ObSpfreshVectorDistUtil::prepare_query_vector(
        vec, vector_dim, dist_algo_, norm_buf, effective_vec))) {
      LOG_WARN("failed to prepare query vector", K(ret), K(i), K_(center_id));
    }
    for (int64_t k = 0; OB_SUCC(ret) && k < centroids.count(); ++k) {
      dist = ObSpfreshVectorDistUtil::calc_squared_distance(
          effective_vec, centroids.at(k), vector_dim);
      if (dist < best_dist) {
        best_dist = dist;
        best_k = k;
      }
    }
    if (OB_SUCC(ret) && is_initial) {
      assignment_changed = true;
      if (OB_FAIL(assignments.push_back(best_k))) {
        LOG_WARN("failed to push assignment", K(ret));
      }
    } else if (OB_SUCC(ret)) {
      assignment_changed = assignment_changed || assignments.at(i) != best_k;
      assignments.at(i) = best_k;
    }
  }
  return ret;
}

int ObSpfreshSplitTask::update_naive_centroids_(
    const ObIArray<ObSpfreshVectorRow> &vectors,
    int64_t vector_dim,
    const ObArray<int64_t> &assignments,
    ObArray<float *> &centroids)
{
  int ret = OB_SUCCESS;
  int64_t cluster_bytes = vector_dim * sizeof(float);
  int64_t cluster = 0;
  const float *vec = nullptr;
  ObArray<int64_t> counts;

  for (int64_t k = 0; k < centroids.count(); ++k) {
    MEMSET(centroids.at(k), 0, cluster_bytes);
    if (OB_FAIL(counts.push_back(0))) {
      LOG_WARN("failed to push count", K(ret));
      break;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    vec = reinterpret_cast<const float *>(vectors.at(i).vector_);
    cluster = assignments.at(i);
    for (int64_t d = 0; d < vector_dim; ++d) {
      centroids.at(cluster)[d] += vec[d];
    }
    ++counts.at(cluster);
  }
  for (int64_t k = 0; OB_SUCC(ret) && k < centroids.count(); ++k) {
    if (counts.at(k) > 0) {
      for (int64_t d = 0; d < vector_dim; ++d) {
        centroids.at(k)[d] /= static_cast<float>(counts.at(k));
      }
      if (OB_FAIL(ObSpfreshVectorDistUtil::normalize_centroid(
          centroids.at(k), vector_dim, dist_algo_))) {
        LOG_WARN("failed to normalize centroid", K(ret), K(k));
      }
    }
  }
  return ret;
}

int ObSpfreshSplitTask::compact_and_validate_clusters_(
    ObArray<float *> &new_centroids,
    ObArray<int64_t> &assignments,
    int64_t source_count,
    bool &can_split)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> cluster_counts;
  ObArray<int64_t> cluster_remap;
  int64_t non_empty_count = 0;
  int64_t min_child_count = INT64_MAX;
  can_split = false;

  if (source_count <= 0 || assignments.count() != source_count
      || new_centroids.count() < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cluster validation input", K(ret), K(source_count),
             "assignment_count", assignments.count(),
             "centroid_count", new_centroids.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_centroids.count(); ++i) {
    if (OB_FAIL(cluster_counts.push_back(0))) {
      LOG_WARN("failed to initialize cluster count", K(ret));
    } else if (OB_FAIL(cluster_remap.push_back(-1))) {
      LOG_WARN("failed to initialize cluster remap", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assignments.count(); ++i) {
    int64_t cluster_idx = assignments.at(i);
    if (cluster_idx < 0 || cluster_idx >= cluster_counts.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid cluster assignment", K(ret), K(i), K(cluster_idx));
    } else {
      ++cluster_counts.at(cluster_idx);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cluster_counts.count(); ++i) {
    if (cluster_counts.at(i) > 0) {
      cluster_remap.at(i) = non_empty_count;
      min_child_count = std::min(min_child_count, cluster_counts.at(i));
      if (non_empty_count != i) {
        new_centroids.at(non_empty_count) = new_centroids.at(i);
        new_centroids.at(i) = nullptr;
      }
      ++non_empty_count;
    } else if (OB_NOT_NULL(new_centroids.at(i))) {
      ob_free(new_centroids.at(i));
      new_centroids.at(i) = nullptr;
    }
  }
  if (OB_SUCC(ret)) {
    while (new_centroids.count() > non_empty_count) {
      new_centroids.pop_back();
    }
    for (int64_t i = 0; i < assignments.count(); ++i) {
      assignments.at(i) = cluster_remap.at(assignments.at(i));
    }
    can_split = non_empty_count >= 2 && min_child_count * 100 >= source_count;
    LOG_TRACE("[SPFRESH_TRACE] split cluster quality", K_(center_id),
              K(source_count), K(non_empty_count), K(min_child_count), K(can_split));
  }
  return ret;
}

int ObSpfreshSplitTask::prepare_split_source_(
    ObArray<float *> &new_centroids,
    int64_t vector_dim,
    common::ObIAllocator &allocator,
    ObArray<ObSpfreshVectorRow> &vectors,
    ObArray<int64_t> &assignments,
    bool &can_split)
{
  int ret = OB_SUCCESS;
  int64_t current_dim = 0;
  bool should_split = false;
  can_split = false;
  assignments.reset();
  if (vector_dim <= 0 || new_centroids.count() < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid split migration centroids", K(ret), K(vector_dim), K(new_centroids.count()));
  } else if (OB_FAIL(read_vectors_(vectors, current_dim, allocator))) {
    LOG_WARN("failed to read locked split source", K(ret), K_(center_id));
  } else if (!vectors.empty() && current_dim != vector_dim) {
    ret = OB_INVALID_DATA;
    LOG_WARN("split dimension changed", K(ret), K(current_dim), K(vector_dim));
  } else {
    source_vector_count_ = vectors.count();
    if (OB_FAIL(validate_split_(source_vector_count_, should_split))) {
      LOG_WARN("failed to validate locked split source", K(ret), K_(center_id));
    } else if (vectors.empty() || !should_split) {
      // The caller rolls back the migration and restores the source to NORMAL.
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("split source no longer meets policy", K(ret), K_(source_vector_count));
    } else if (OB_FAIL(assign_vectors_to_centroids_(
        vectors, vector_dim, new_centroids, assignments))) {
      LOG_WARN("failed to assign locked split source", K(ret), K_(center_id));
    } else if (OB_FAIL(compact_and_validate_clusters_(
        new_centroids, assignments, source_vector_count_, can_split))) {
      LOG_WARN("failed to validate locked split clusters", K(ret), K_(center_id));
    }
  }
  return ret;
}

int ObSpfreshSplitTask::migrate_vectors_(
    const common::ObTabletID &physical_tablet_id,
    ObArray<float *> &new_centroids,
    int64_t vector_dim,
    bool &can_split)
{
  int ret = OB_SUCCESS;
  int64_t num_children = new_centroids.count();
  bool transaction_started = false;
  SCN commit_scn;
  ObArenaAllocator allocator("SpfreshSpMig", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObArray<ObSpfreshVectorRow> vectors;
  ObArray<int64_t> assignments;
  can_split = false;
  child_center_ids_.reset();
  // Serialize child-id allocation through commit for splits of the same centroid tablet.
  // The physical tablet transaction lock separately excludes concurrent user DML.
  if (OB_ISNULL(task_queue_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task queue is null, cannot acquire per-tablet migrate mutex", K(ret));
  } else {
    ObLSID debug_sync_ls_id;
    bool debug_sync_ready = false;
    if (OB_UNLIKELY(GCONF.is_debug_sync_enabled())) {
      bool cache_hit = false;
      int location_ret = OB_SUCCESS;
      // Resolve before locking: a location cache miss may perform synchronous RPC.
      if (OB_ISNULL(GCTX.location_service_)) {
        location_ret = OB_NOT_INIT;
      } else {
        location_ret = GCTX.location_service_->get(
            tenant_id_, ObTabletID(queue_tablet_id_), 0, cache_hit, debug_sync_ls_id);
      }
      if (OB_SUCCESS != location_ret) {
        LOG_WARN("resolve split debug sync LS failed", K(location_ret), K_(queue_tablet_id));
      } else {
        debug_sync_ready = debug_sync_ls_id.is_valid();
      }
    }
    lib::ObMutexGuard alloc_guard(task_queue_->get_migrate_mutex());
    ObSpfreshChildVectorArray child_vectors;
    ObArray<ObString> rowkeys_to_delete;
    int end_ret = OB_SUCCESS;
    // This KV max query uses its own connection; keep it outside the batch transaction.
    if (OB_FAIL(allocate_child_center_ids_(num_children))) {
      LOG_WARN("failed to allocate child center ids", K(ret), K_(center_id));
    } else if (OB_FAIL(table_helper_.start_batch_transaction())) {
      LOG_WARN("failed to start batch transaction", K(ret));
    } else {
      transaction_started = true;
      if (OB_FAIL(table_helper_.lock_batch_tablet(physical_tablet_id))) {
        if (OB_EAGAIN == ret) {
          // Restore NORMAL and let the timer retry; do not spin on an active writer.
          ret = OB_ENTRY_NOT_EXIST;
          LOG_TRACE("skip split while physical tablet is busy", K(ret), K(physical_tablet_id));
        } else {
          LOG_WARN("failed to lock split tablet", K(ret), K(physical_tablet_id));
        }
      } else if (OB_FAIL(prepare_split_source_(
          new_centroids, vector_dim, allocator, vectors, assignments, can_split))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to prepare split migration source", K(ret), K_(center_id));
        }
      }
    }
    num_children = new_centroids.count();
    // Revalidation may compact empty clusters; unused ids have not been persisted.
    while (child_center_ids_.count() > num_children) {
      child_center_ids_.pop_back();
    }
    if (OB_SUCC(ret) && can_split && OB_FAIL(insert_child_centroids_(
        num_children, new_centroids, vector_dim, commit_scn))) {
      LOG_WARN("failed to insert child centroids", K(ret), K_(center_id));
    }
    if (OB_SUCC(ret) && can_split && OB_FAIL(build_child_vectors_(
        num_children, vectors, assignments, child_vectors, rowkeys_to_delete))) {
      LOG_WARN("failed to build child vectors", K(ret), K_(center_id));
    }
    if (OB_SUCC(ret) && can_split && OB_FAIL(delete_split_source_vectors_(
        rowkeys_to_delete, commit_scn))) {
      LOG_WARN("failed to delete source vectors", K(ret), K_(center_id));
    }
    if (OB_SUCC(ret) && can_split && OB_FAIL(insert_split_child_vectors_(
        child_vectors, commit_scn))) {
      LOG_WARN("failed to insert child vectors", K(ret), K_(center_id));
    }
    if (OB_SUCC(ret) && can_split && OB_FAIL(update_split_rowid_cids_(
        child_vectors, rowkeys_to_delete, commit_scn))) {
      LOG_WARN("failed to update rowid-cid mappings", K(ret), K_(center_id));
    }
    if (OB_SUCC(ret) && can_split && OB_FAIL(update_split_source_state_(commit_scn))) {
      LOG_WARN("failed to update split source state", K(ret), K_(center_id));
    }
    if (OB_SUCC(ret) && can_split && debug_sync_ready
        && OB_UNLIKELY(GCONF.is_debug_sync_enabled())) {
      // Keep the migrate mutex until commit: another split must not allocate
      // child ids from the old KV maximum while this transaction is uncommitted.
      // Tests must signal from an independent connection, without waiting for
      // another split on this tablet. Timeout must not change the commit decision.
      (void)GDS.execute(BEFORE_SPFRESH_SPLIT_TX_COMMIT,
          tenant_id_, debug_sync_ls_id.id(), queue_tablet_id_, index_table_id_);
    }

    if (transaction_started) {
      end_ret = table_helper_.end_batch_transaction(OB_SUCC(ret) && can_split, commit_scn);
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to end split transaction", K(end_ret), K(ret));
        if (OB_SUCC(ret) || OB_ENTRY_NOT_EXIST == ret) {
          ret = end_ret;
        }
      }
    }

    if (OB_FAIL(ret) || !can_split) {
      child_center_ids_.reset();
      child_vector_counts_.reset();
    }
  }
  return ret;
}

int ObSpfreshSplitTask::allocate_child_center_ids_(int64_t num_children)
{
  int ret = OB_SUCCESS;
  int probe_ret = OB_SUCCESS;
  uint64_t kv_max_cid = 0;
  int64_t max_cid = 0;
  ObCentroidInfo probe_info;

  if (OB_UNLIKELY(num_children <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid num_children", K(ret), K(num_children), K_(center_id));
  } else if (OB_FAIL(table_helper_.get_max_center_id_from_kv(kv_max_cid))) {
    LOG_WARN("failed to get max center_id from KV", K(ret), K_(center_id));
  } else if (kv_max_cid > static_cast<uint64_t>(INT64_MAX)
             || static_cast<uint64_t>(num_children)
                > static_cast<uint64_t>(INT64_MAX) - kv_max_cid) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("center_id allocation would overflow int64", K(ret),
             K(kv_max_cid), K(num_children), K_(center_id));
  } else {
    max_cid = static_cast<int64_t>(kv_max_cid);
    if (kv_max_cid > 0 && OB_NOT_NULL(cache_)) {
      probe_ret = cache_->get_center_info(max_cid, SCN::max_scn(), probe_info);
      if (OB_SUCCESS != probe_ret) {
        LOG_WARN("[SPFRESH] cache missing KV max centroid",
                 K(probe_ret), K_(center_id), K(kv_max_cid));
      }
    }
    for (int64_t i = 1; OB_SUCC(ret) && i <= num_children; ++i) {
      if (OB_FAIL(child_center_ids_.push_back(max_cid + i))) {
        LOG_WARN("failed to push child center id", K(ret), "child_id", max_cid + i);
      }
    }
  }
  return ret;
}

int ObSpfreshSplitTask::insert_child_centroids_(
    int64_t num_children,
    const ObArray<float *> &new_centroids,
    int64_t vector_dim,
    SCN &commit_scn)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < num_children; ++i) {
    if (OB_FAIL(table_helper_.insert_centroid(
        child_center_ids_.at(i),
        reinterpret_cast<const char *>(new_centroids.at(i)),
        vector_dim * sizeof(float),
        ObIvfCentroidState::NORMAL,
        commit_scn))) {
      LOG_WARN("failed to insert centroid", K(ret), "child_id", child_center_ids_.at(i));
    }
  }
  return ret;
}

int ObSpfreshSplitTask::build_child_vectors_(
    int64_t num_children,
    const ObIArray<ObSpfreshVectorRow> &vectors,
    const ObArray<int64_t> &assignments,
    ObSpfreshChildVectorArray &child_vectors,
    ObArray<ObString> &rowkeys_to_delete)
{
  int ret = OB_SUCCESS;
  int64_t cluster = 0;
  ObArray<ObSpfreshVectorRow> child;
  const ObSpfreshVectorRow *row = nullptr;
  ObString rowkey;

  for (int64_t k = 0; OB_SUCC(ret) && k < num_children; ++k) {
    child.reset();
    if (OB_FAIL(child_vectors.push_back(child))) {
      LOG_WARN("failed to push child vector array", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    row = &vectors.at(i);
    rowkey.assign_ptr(row->rowkey_, row->rowkey_len_);
    cluster = assignments.at(i);
    if (OB_FAIL(rowkeys_to_delete.push_back(rowkey))) {
      LOG_WARN("failed to push rowkey", K(ret));
    } else if (OB_FAIL(child_vectors.at(cluster).push_back(*row))) {
      LOG_WARN("failed to push vector to cluster", K(ret), K(cluster));
    }
  }
  // Each copied row must point to its own inline rowkey buffer after array moves.
  for (int64_t k = 0; OB_SUCC(ret) && k < num_children; ++k) {
    for (int64_t i = 0; i < child_vectors.at(k).count(); ++i) {
      child_vectors.at(k).at(i).rowkey_ = child_vectors.at(k).at(i).rowkey_buf_;
    }
  }
  return ret;
}

int ObSpfreshSplitTask::delete_split_source_vectors_(
    const ObIArray<ObString> &rowkeys,
    SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = 1000;
  int64_t batch_end = 0;
  ObSEArray<ObString, 8> batch_keys;

  for (int64_t off = 0; OB_SUCC(ret) && off < rowkeys.count(); off += batch_size) {
    batch_end = std::min(off + batch_size, rowkeys.count());
    batch_keys.reset();
    for (int64_t i = off; OB_SUCC(ret) && i < batch_end; ++i) {
      if (OB_FAIL(batch_keys.push_back(rowkeys.at(i)))) {
        LOG_WARN("failed to push batch key", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(table_helper_.delete_from_cid_vector(
        center_id_, batch_keys, commit_scn))) {
      LOG_WARN("failed to delete batch from cid_vector", K(ret), K_(center_id));
    }
  }
  return ret;
}

int ObSpfreshSplitTask::insert_split_child_vectors_(
    const ObSpfreshChildVectorArray &child_vectors,
    SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = 1000;
  int64_t batch_end = 0;
  const ObArray<ObSpfreshVectorRow> *child = nullptr;
  ObSEArray<ObSpfreshVectorRow, 8> batch_vectors;
  child_vector_counts_.reset();

  for (int64_t k = 0; OB_SUCC(ret) && k < child_vectors.count(); ++k) {
    child = &child_vectors.at(k);
    for (int64_t off = 0; OB_SUCC(ret) && off < child->count(); off += batch_size) {
      batch_end = std::min(off + batch_size, child->count());
      batch_vectors.reset();
      for (int64_t i = off; OB_SUCC(ret) && i < batch_end; ++i) {
        if (OB_FAIL(batch_vectors.push_back(child->at(i)))) {
          LOG_WARN("failed to push batch vector", K(ret));
        }
      }
      for (int64_t i = 0; i < batch_vectors.count(); ++i) {
        batch_vectors.at(i).rowkey_ = batch_vectors.at(i).rowkey_buf_;
      }
      if (OB_SUCC(ret) && !batch_vectors.empty()
          && OB_FAIL(table_helper_.insert_to_cid_vector(
              child_center_ids_.at(k), batch_vectors, commit_scn))) {
        LOG_WARN("failed to insert batch to cid_vector", K(ret),
                 "child_id", child_center_ids_.at(k));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(child_vector_counts_.push_back(child->count()))) {
      LOG_WARN("failed to push child vector count", K(ret));
    }
  }
  return ret;
}

int ObSpfreshSplitTask::update_split_rowid_cids_(
    const ObSpfreshChildVectorArray &child_vectors,
    const ObIArray<ObString> &rowkeys,
    SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = 1000;
  int64_t batch_end = 0;
  const ObArray<ObSpfreshVectorRow> *child = nullptr;
  ObSEArray<ObString, 8> batch_keys;
  ObSEArray<ObSpfreshVectorRow, 8> batch_vectors;

  for (int64_t off = 0; OB_SUCC(ret) && off < rowkeys.count(); off += batch_size) {
    batch_end = std::min(off + batch_size, rowkeys.count());
    batch_keys.reset();
    for (int64_t i = off; OB_SUCC(ret) && i < batch_end; ++i) {
      if (OB_FAIL(batch_keys.push_back(rowkeys.at(i)))) {
        LOG_WARN("failed to push batch key", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(table_helper_.delete_from_rowid_cid(
        center_id_, batch_keys, commit_scn))) {
      LOG_WARN("failed to delete rowid-cid mappings", K(ret), K_(center_id));
    }
  }
  for (int64_t k = 0; OB_SUCC(ret) && k < child_vectors.count(); ++k) {
    child = &child_vectors.at(k);
    for (int64_t off = 0; OB_SUCC(ret) && off < child->count(); off += batch_size) {
      batch_end = std::min(off + batch_size, child->count());
      batch_vectors.reset();
      for (int64_t i = off; OB_SUCC(ret) && i < batch_end; ++i) {
        if (OB_FAIL(batch_vectors.push_back(child->at(i)))) {
          LOG_WARN("failed to push batch vector", K(ret));
        }
      }
      for (int64_t i = 0; i < batch_vectors.count(); ++i) {
        batch_vectors.at(i).rowkey_ = batch_vectors.at(i).rowkey_buf_;
      }
      if (OB_SUCC(ret) && !batch_vectors.empty()
          && OB_FAIL(table_helper_.insert_to_rowid_cid(
              child_center_ids_.at(k), batch_vectors, commit_scn))) {
        LOG_WARN("failed to insert rowid-cid mappings", K(ret),
                 "child_id", child_center_ids_.at(k));
      }
    }
  }
  return ret;
}

int ObSpfreshSplitTask::update_split_source_state_(SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObCentroidInfo info;

  if (0 == center_id_) {
    if (OB_FAIL(cache_->get_center_info(center_id_, SCN::max_scn(), info))) {
      LOG_WARN("bootstrap centroid 0 not found in cache", K(ret), K_(center_id));
    } else {
      LOG_TRACE("[SPFRESH_TRACE] skip KV update for bootstrap centroid 0",
                K_(center_id));
    }
  } else if (OB_FAIL(table_helper_.update_centroid_state(
      center_id_, ObIvfCentroidState::SPLIT_DONE,
      ObIvfCentroidState::MAX_TYPE, commit_scn))) {
    LOG_WARN("failed to update centroid state to SPLIT_DONE", K(ret), K_(center_id));
  }
  return ret;
}

int ObSpfreshSplitTask::commit_callback_(const ObIArray<int64_t> &child_center_ids,
                                         const ObArray<float *> &new_centroids,
                                         int64_t vector_dim)
{
  int ret = OB_SUCCESS;
  int64_t num_children = child_center_ids.count();

  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null", K(ret));
  }

  // Add new centroids to cache with NORMAL state (already NORMAL in KV via migrate_vectors_)
  // Also set posting_list_count so merge check can see actual vector distribution
  for (int64_t i = 0; OB_SUCC(ret) && i < num_children; ++i) {
    if (OB_FAIL(cache_->add_centroid(child_center_ids.at(i),
               new_centroids.at(i), vector_dim, &lock_scn_,
               ObIvfCentroidState::NORMAL))) {
      LOG_WARN("failed to add centroid to cache", K(ret),
               "child_id", child_center_ids.at(i));
    } else if (OB_FAIL(cache_->incr_posting_list_length(
               child_center_ids.at(i), child_vector_counts_.at(i)))) {
      LOG_WARN("failed to set posting list count for child centroid", K(ret),
               "child_id", child_center_ids.at(i),
               "count", child_vector_counts_.at(i));
    }
  }

  // Update old centroid state to SPLIT_DONE in cache
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cache_->update_centroid_state(center_id_,
               ObIvfCentroidState::SPLIT_DONE))) {
      LOG_WARN("failed to update old centroid state to SPLIT_DONE", K(ret), K_(center_id));
    }
  }

  // Capture only after the source is closed in cache. Every writer registers
  // the physical tablet before selecting a target, so this timestamp includes
  // all writers that could have observed the old source. The timestamp is
  // carried by the in-memory REASSIGN task; durable recovery is out of scope.
  if (OB_SUCC(ret)) {
    cutover_modify_ts_ = ObTimeUtility::current_time();
  }

  // Push Reassign task for old centroid
  if (OB_SUCC(ret)) {
    ObSpfreshTaskKey task_key(index_table_id_, queue_tablet_id_, center_id_, ns_id_,
                              ltable_id_, ObSpfreshTaskType::REASSIGN);
    ObSpfreshTask reassign_task(
        ObSpfreshTaskType::REASSIGN, task_key, cutover_modify_ts_);
    reassign_task.cache_guard_ = cache_guard_;
    if (OB_FAIL(task_queue_->push(reassign_task))) {
      LOG_WARN("failed to push reassign task", K(ret), K_(center_id));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] split commit_callback done", KR(ret),
            K_(center_id), "num_children", child_center_ids.count(),
            K_(child_vector_counts));
  return ret;
}

int ObSpfreshSplitTask::rollback_lock_()
{
  int ret = OB_SUCCESS;

  // Rollback KV state for non-bootstrap centroids
  if (0 != center_id_) {
    SCN rollback_scn;
    int kv_ret = OB_SUCCESS;
    if (OB_SUCCESS != (kv_ret = table_helper_.update_centroid_state(
        center_id_, ObIvfCentroidState::NORMAL, ObIvfCentroidState::SPLITTING,
        rollback_scn))) {
      LOG_WARN("failed to rollback KV centroid state", K(kv_ret), K_(center_id));
    }
  }

  // Reset cache state
  if (OB_NOT_NULL(cache_)) {
    int cache_ret = OB_SUCCESS;
    if (OB_SUCCESS != (cache_ret = cache_->update_centroid_state(
        center_id_, ObIvfCentroidState::NORMAL))) {
      LOG_WARN("failed to rollback cache centroid state", K(cache_ret), K_(center_id));
    }
  }

  return ret;
}

int ObSpfreshSplitTask::push_retry_task_()
{
  int ret = OB_SUCCESS;

  ObSpfreshTaskKey task_key(index_table_id_, queue_tablet_id_, center_id_, ns_id_,
                            ltable_id_, ObSpfreshTaskType::SPLIT);
  ObSpfreshTask split_task(ObSpfreshTaskType::SPLIT, task_key);
  split_task.cache_guard_ = cache_guard_;
  if (OB_FAIL(task_queue_->push(split_task))) {
    LOG_WARN("failed to push split task for retry", K(ret), K_(center_id));
  }

  LOG_TRACE("[SPFRESH_TRACE] push split task for retry done", KR(ret), K_(center_id));
  return ret;
}

int ObSpfreshSplitTask::push_reassign_()
{
  int ret = OB_SUCCESS;

  ObSpfreshTaskKey task_key(index_table_id_, queue_tablet_id_, center_id_, ns_id_,
                            ltable_id_, ObSpfreshTaskType::REASSIGN);
  const int64_t cutover_modify_ts = ObTimeUtility::current_time();
  ObSpfreshTask reassign_task(
      ObSpfreshTaskType::REASSIGN, task_key, cutover_modify_ts);
  reassign_task.cache_guard_ = cache_guard_;
  if (OB_FAIL(task_queue_->push(reassign_task))) {
    LOG_WARN("failed to push reassign task for lazy recovery", K(ret), K_(center_id));
  }

  LOG_TRACE("[SPFRESH_TRACE] push reassign task for lazy recovery done", KR(ret), K_(center_id));
  return ret;
}

}  // namespace share
}  // namespace oceanbase
