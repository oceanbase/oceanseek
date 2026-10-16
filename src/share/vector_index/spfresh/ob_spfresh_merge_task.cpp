/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_merge_task.h"
#include "share/vector_index/spfresh/ob_spfresh_policy.h"
#include "share/vector_index/ob_vector_index_ivf_cache_util.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/parameter/ob_parameter_macro.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
namespace share
{

ObSpfreshMergeTask::ObSpfreshMergeTask()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    sql_proxy_(nullptr),
    index_table_id_(OB_INVALID_ID),
    queue_tablet_id_(OB_INVALID_ID),
    source_center_id_(OB_INVALID_ID),
    ns_id_(ObIvfCacheMgrKey::INVALID_NS_ID),
    ltable_id_(OB_INVALID_LOGICAL_TABLE_ID),
    task_queue_(nullptr),
    table_helper_(),
    cache_(nullptr),
    dist_algo_(ObVectorIndexDistAlgorithm::VIDA_L2),
    lock_scn_(),
    cutover_modify_ts_(0),
    target_center_id_(OB_INVALID_ID),
    migrated_count_(0),
    target_center_ids_(),
    target_vector_counts_()
{
}

ObSpfreshMergeTask::~ObSpfreshMergeTask()
{
  destroy();
}

int ObSpfreshMergeTask::init(uint64_t tenant_id,
                             common::ObMySQLProxy *sql_proxy,
                             uint64_t index_table_id,
                             const ObSpfreshKvTableInfo &kv_info,
                             uint64_t queue_tablet_id,
                             int64_t source_center_id,
                             int64_t ns_id,
                             uint64_t ltable_id,
                             ObSpfreshTaskQueue *task_queue,
                             ObIvfMapCentCache *cache,
                             ObVectorIndexDistAlgorithm dist_algo)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSpfreshMergeTask init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(sql_proxy) ||
             OB_INVALID_ID == index_table_id || OB_INVALID_ID == queue_tablet_id ||
             OB_INVALID_ID == source_center_id || OB_ISNULL(task_queue) ||
             OB_ISNULL(cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(sql_proxy),
             K(index_table_id), K(queue_tablet_id), K(source_center_id), KP(task_queue),
             KP(cache));
  } else if (OB_FAIL(table_helper_.init(tenant_id, sql_proxy, kv_info))) {
    LOG_WARN("failed to init table helper", K(ret));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;
    index_table_id_ = index_table_id;
    queue_tablet_id_ = queue_tablet_id;
    source_center_id_ = source_center_id;
    ns_id_ = ns_id;
    ltable_id_ = ltable_id;
    task_queue_ = task_queue;
    cache_ = cache;
    dist_algo_ = dist_algo;
    is_inited_ = true;
  }
  return ret;
}

void ObSpfreshMergeTask::destroy()
{
  if (is_inited_) {
    table_helper_.destroy();
    is_inited_ = false;
    tenant_id_ = OB_INVALID_ID;
    sql_proxy_ = nullptr;
    index_table_id_ = OB_INVALID_ID;
    queue_tablet_id_ = OB_INVALID_ID;
    source_center_id_ = OB_INVALID_ID;
    ns_id_ = ObIvfCacheMgrKey::INVALID_NS_ID;
    ltable_id_ = OB_INVALID_LOGICAL_TABLE_ID;
    task_queue_ = nullptr;
    cache_ = nullptr;
    cache_guard_.reset();
    dist_algo_ = ObVectorIndexDistAlgorithm::VIDA_L2;
    cutover_modify_ts_ = 0;
    lock_scn_.reset();
    target_center_id_ = OB_INVALID_ID;
    migrated_count_ = 0;
    target_center_ids_.reset();
    target_vector_counts_.reset();
  }
}

int ObSpfreshMergeTask::execute(const common::ObTabletID &physical_tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t target_center_id = OB_INVALID_ID;
  int64_t source_count = 0;
  int64_t merge_capacity = 0;
  bool lock_acquired = false;
  ObArenaAllocator vec_allocator("SpfreshMgV", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshMergeTask not init", K(ret));
  } else if (!physical_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid physical merge tablet", K(ret), K(physical_tablet_id));
  } else {
    // Phase 1: Acquire lock
    if (OB_FAIL(acquire_lock_(lock_scn_, lock_acquired))) {
      // OB_STATE_NOT_MATCH / OB_ENTRY_NOT_EXIST from acquire_lock_ are benign
      // skips (centroid state not ready for merge, or removed by a concurrent
      // worker). Clear ret so execute() returns success for skips and they are
      // not recorded as RET_CODE!=0 in task history.
      if (OB_STATE_NOT_MATCH == ret || OB_ENTRY_NOT_EXIST == ret) {
        LOG_TRACE("[SPFRESH_TRACE] centroid state not ready, benign skip merge",
                 KR(ret), K_(source_center_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("acquire lock system error", K(ret), K_(source_center_id));
      }
    } else if (!lock_acquired) {
      LOG_TRACE("[SPFRESH_TRACE] centroid state changed, skip merge", K_(source_center_id));
    } else {

    // Revalidate the policy before choosing a target with enough capacity.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(validate_merge_(source_count, merge_capacity))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to validate merge", K(ret), K_(source_center_id));
      } else {
        LOG_TRACE("[SPFRESH_TRACE] merge condition changed, skip",
                  K_(source_center_id));
      }
    } else if (OB_FAIL(find_target_(merge_capacity - source_count,
                                    target_center_id))) {
      // OB_ENTRY_NOT_EXIST means no valid NORMAL merge target is currently
      // available (only candidate is bootstrap fallback / SPLITTING /
      // MERGING). Treat as a graceful skip: rollback the MERGING lock so
      // the source can be picked up again by a future scheduler tick.
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to find merge target", K(ret), K_(source_center_id));
      } else {
        LOG_INFO("[SPFRESH_TRACE] no NORMAL merge target, will skip this round",
                 K_(source_center_id));
      }
    } else {
      target_center_id_ = target_center_id;
    }

    // Phase 3: Migrate vectors
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(migrate_vectors_(physical_tablet_id, target_center_id, vec_allocator))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to migrate vectors", K(ret), K_(source_center_id), K(target_center_id));
      }
    }

    // Phase 4: Commit callback
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(commit_callback_(target_center_id, migrated_count_))) {
      LOG_WARN("failed to commit callback", K(ret), K_(source_center_id));
    }

    // Handle failure (inside lock_acquired guard)
    if (OB_FAIL(ret)) {
      // Distinguish "no candidate, retry later via scheduler" from real failures.
      // We must NOT push a retry task in the skip path or the queue will hot-spin
      // on a centroid that has no merge partner.
      const bool is_skip = (OB_ENTRY_NOT_EXIST == ret);
      int rollback_ret = rollback_lock_();
      if (OB_SUCCESS != rollback_ret) {
        LOG_ERROR("failed to rollback lock after merge failure, KV/cache may be inconsistent",
                  K(rollback_ret), K_(source_center_id));
      }
      if (is_skip) {
        // Skip only after restoring NORMAL; a failed rollback remains visible.
        // The next scheduler trigger can retry a successfully restored source.
        ret = rollback_ret;
      } else {
        int push_ret = push_retry_task_();
        if (OB_SUCCESS != push_ret) {
          LOG_WARN("failed to push retry task", K(push_ret), K_(source_center_id));
        }
      }
    }

    }  // end lock_acquired guard
  }

  int64_t elapsed_us = ObTimeUtility::current_time() - start_time;
  LOG_TRACE("[SPFRESH_TRACE] Merge task done", KR(ret), K_(source_center_id), K(target_center_id), K(elapsed_us));

  return ret;
}

int ObSpfreshMergeTask::acquire_lock_(SCN &cur_scn, bool &lock_success)
{
  int ret = OB_SUCCESS;
  cur_scn = SCN::min_scn();
  lock_success = false;
  const char *lock_source = "none";

  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null", K(ret));
  } else {
    // Step 1: cache filter
    ObCentroidInfo centroid_info;
    if (OB_FAIL(cache_->get_center_info(source_center_id_, SCN::max_scn(), centroid_info))) {
      LOG_WARN("failed to get centroid info", K(ret), K_(source_center_id));
    } else if (ObIvfCentroidState::MERGED == centroid_info.state_) {
      // Lazy recovery: previous merge completed but reassign never queued
      int push_ret = OB_SUCCESS;
      if (OB_SUCCESS != (push_ret = push_reassign_())) {
        LOG_WARN("failed to push reassign for MERGED centroid", K(push_ret), K_(source_center_id));
      }
      LOG_TRACE("[SPFRESH_TRACE] centroid already MERGED, skip merge", K_(source_center_id));
    } else if (ObIvfCentroidState::NORMAL != centroid_info.state_) {
      LOG_TRACE("[SPFRESH_TRACE] centroid state is not NORMAL, skip",
               K_(source_center_id), "state", static_cast<int>(centroid_info.state_));
    } else {
      // Step 2: acquire lock
      if (0 == source_center_id_) {
        // Bootstrap centroid: cache-only CAS
        lock_success = true;
        cur_scn = SCN::min_scn();
        lock_source = "bootstrap";
      } else if (OB_FAIL(table_helper_.start_batch_transaction())) {
        LOG_WARN("start batch tx failed", K(ret));
      } else {
        int64_t affected_rows = 0;
        int end_ret = OB_SUCCESS;
        if (OB_FAIL(table_helper_.update_centroid_value_cas(
                source_center_id_,
                ObIvfCentroidState::NORMAL,
                ObIvfCentroidState::MERGING,
                affected_rows))) {
          LOG_WARN("update centroid CAS failed", K(ret));
        } else if (1 == affected_rows) {
          lock_success = true;
          lock_source = "kv";
        } else {
          LOG_TRACE("[SPFRESH_TRACE] centroid CAS failed, affected_rows",
                   K(affected_rows), K_(source_center_id));
        }

        if (OB_FAIL(ret)) {
          if (OB_SUCCESS != (end_ret = table_helper_.end_batch_transaction(false, cur_scn))) {
            LOG_WARN("end batch tx on error failed", K(end_ret));
          }
        } else if (OB_SUCCESS != (end_ret = table_helper_.end_batch_transaction(
            lock_success, cur_scn))) {
          LOG_WARN("end batch tx failed", K(end_ret));
          ret = end_ret;
          lock_success = false;
        }
      }

      // Step 3: cache CAS sync
      if (OB_FAIL(ret) || !lock_success) {
      } else {
        bool cas_ok = false;
        if (OB_FAIL(cache_->update_centroid_state_cas(source_center_id_,
                ObIvfCentroidState::NORMAL,
                ObIvfCentroidState::MERGING,
                cas_ok))) {
          LOG_WARN("cache CAS failed", K(ret), K_(source_center_id));
          lock_success = false;
          if (0 != source_center_id_) {
            SCN rollback_scn;
            int kv_ret = OB_SUCCESS;
            if (OB_SUCCESS != (kv_ret = table_helper_.update_centroid_state(
                source_center_id_, ObIvfCentroidState::NORMAL,
                ObIvfCentroidState::MERGING, rollback_scn))) {
              LOG_ERROR("failed to rollback KV after cache CAS failure, KV/cache inconsistent",
                        K(kv_ret), K_(source_center_id));
            }
          }
        } else if (!cas_ok) {
          lock_success = false;
          LOG_TRACE("[SPFRESH_TRACE] cache CAS not ok, another worker won", K_(source_center_id));
          if (0 != source_center_id_) {
            SCN rollback_scn;
            int kv_ret = OB_SUCCESS;
            if (OB_SUCCESS != (kv_ret = table_helper_.update_centroid_state(
                source_center_id_, ObIvfCentroidState::NORMAL,
                ObIvfCentroidState::MERGING, rollback_scn))) {
              LOG_ERROR("failed to rollback KV after cache CAS miss, KV/cache inconsistent",
                        K(kv_ret), K_(source_center_id));
            }
          }
        }
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] merge lock acquired",
            K_(source_center_id), K(lock_success), "source", lock_source, K(ret));
  return ret;
}

ObSpfreshMergeTask::CountNormalCentroidFn::CountNormalCentroidFn(
    int64_t &normal_count,
    int64_t &normal_vector_count)
    : normal_count_(normal_count),
      normal_vector_count_(normal_vector_count)
{}

int ObSpfreshMergeTask::CountNormalCentroidFn::operator()(
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

int ObSpfreshMergeTask::validate_merge_(int64_t &source_count,
                                        int64_t &merge_capacity)
{
  int ret = OB_SUCCESS;
  int64_t normal_count = 0;
  int64_t normal_vector_count = 0;

  source_count = 0;
  merge_capacity = ObSpfreshPolicy::get_merge_capacity_rows();
  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null", K(ret));
  } else {
    ObCentroidInfo source_info;
    CountNormalCentroidFn count_fn(normal_count, normal_vector_count);
    if (OB_FAIL(cache_->get_center_info(
        source_center_id_, SCN::max_scn(), source_info))) {
      LOG_WARN("failed to get merge source", K(ret), K_(source_center_id));
    } else if (ObIvfCentroidState::MERGING != source_info.state_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_TRACE("[SPFRESH_TRACE] merge source state changed", K(ret),
                K_(source_center_id), "state", static_cast<int>(source_info.state_));
    } else if (FALSE_IT(source_count = source_info.posting_list_count_)) {
    } else if (source_count > ObSpfreshPolicy::get_merge_low_rows()
               || source_count > merge_capacity) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("[SPFRESH_TRACE] merge source exceeds policy", K(ret),
                K_(source_center_id), K(source_count), K(merge_capacity));
    } else if (OB_FAIL(cache_->for_each_centroid_info(SCN::max_scn(), count_fn))) {
      LOG_WARN("failed to count normal centroids", K(ret));
    } else {
      int64_t active_count = normal_count + 1;
      int64_t live_count = normal_vector_count + source_count;
      int64_t desired_count = ObSpfreshPolicy::get_desired_center_count(
          live_count, cache_->get_nlist_param());
      int64_t center_floor = ObSpfreshPolicy::get_merge_center_floor(desired_count);
      if (active_count <= center_floor) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("[SPFRESH_TRACE] merge blocked by center floor", K(ret),
                  K_(source_center_id), K(active_count), K(desired_count),
                  K(center_floor));
      }
    }
  }
  return ret;
}

int ObSpfreshMergeTask::find_target_(int64_t max_target_count,
                                     int64_t &target_center_id)
{
  int ret = OB_SUCCESS;
  target_center_id = OB_INVALID_ID;

  ObArenaAllocator allocator("SpfreshMerge", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null", K(ret));
  } else if (max_target_count < 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_TRACE("[SPFRESH_TRACE] no capacity for merge target", K(ret),
              K_(source_center_id), K(max_target_count));
  } else if (OB_FAIL(cache_->find_nearest_normal_centroid_to(source_center_id_,
             share::SCN::max_scn(), dist_algo_, max_target_count,
             allocator, target_center_id))) {
    LOG_WARN("failed to find nearest normal centroid", K(ret), K_(source_center_id));
  }

  LOG_TRACE("[SPFRESH_TRACE] merge target selected",
            K_(source_center_id), K(target_center_id), K(ret));
  return ret;
}

int ObSpfreshMergeTask::find_vector_target_(
    const ObSpfreshVectorRow &row,
    int64_t max_target_count,
    common::ObIAllocator &allocator,
    int64_t &target_center_id)
{
  int ret = OB_SUCCESS;
  int64_t vector_dim = 0;
  int64_t expected_vector_bytes = 0;
  target_center_id = OB_INVALID_ID;
  if (OB_ISNULL(cache_) || OB_ISNULL(row.vector_) || max_target_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector target argument", K(ret), KP_(cache),
             KP(row.vector_), K(max_target_count));
  } else if (FALSE_IT(vector_dim = cache_->get_cent_vec_dim())) {
  } else if (OB_FAIL(ObSpfreshTableHelper::get_raw_vector_expected_bytes(
                 vector_dim, expected_vector_bytes))) {
    LOG_WARN("invalid merge vector dimension from cache", K(ret), K(vector_dim));
  } else if (OB_UNLIKELY(row.vector_len_ != expected_vector_bytes)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("merge vector length mismatches cache dimension", K(ret),
             K(row.vector_len_), K(expected_vector_bytes));
  } else {
    ObMapCacheForEachFunc for_each_fn(cache_, SCN::max_scn(),
        source_center_id_, max_target_count, true /* NORMAL only */);
    if (OB_FAIL(find_nearest_center_from_map(
        reinterpret_cast<float *>(const_cast<char *>(row.vector_)),
        vector_dim, for_each_fn, dist_algo_, allocator, target_center_id))) {
      LOG_WARN("failed to find vector merge target", K(ret), K_(source_center_id));
    } else if (OB_INVALID_ID == target_center_id) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_TRACE("[SPFRESH_TRACE] no vector merge target", K(ret),
                K_(source_center_id), K(max_target_count));
    } else {
      ObCentroidInfo target_info;
      if (OB_FAIL(cache_->get_center_info(
          target_center_id, SCN::max_scn(), target_info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to verify vector merge target", K(ret),
                   K(target_center_id));
        }
      } else if (ObIvfCentroidState::NORMAL != target_info.state_
                 || target_center_id == source_center_id_
                 || target_info.posting_list_count_ > max_target_count) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("[SPFRESH_TRACE] vector merge target no longer valid", K(ret),
                  K(target_center_id), K(max_target_count),
                  "state", static_cast<int>(target_info.state_),
                  "posting_count", target_info.posting_list_count_);
      }
    }
  }
  return ret;
}

int ObSpfreshMergeTask::build_target_groups_(
    const common::ObIArray<ObSpfreshVectorRow> &vectors,
    int64_t max_target_count,
    common::ObIAllocator &allocator,
    ObSpfreshVectorGroupMap &cid_groups)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObSpfreshVectorRow, 8> new_group;
  ObSpfreshVectorGroupMap::iterator iter;
  target_center_ids_.reset();
  target_vector_counts_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    int64_t vector_target_id = OB_INVALID_ID;
    if (OB_FAIL(find_vector_target_(vectors.at(i), max_target_count,
                                    allocator, vector_target_id))) {
      LOG_WARN("failed to reassign merge vector", K(ret), K(i),
               K(max_target_count));
    } else {
      common::ObSEArray<ObSpfreshVectorRow, 8> *group =
          cid_groups.get(vector_target_id);
      if (OB_NOT_NULL(group)) {
        if (OB_FAIL(group->push_back(vectors.at(i)))) {
          LOG_WARN("failed to append merge vector group", K(ret),
                   K(vector_target_id));
        }
      } else {
        new_group.reset();
        if (OB_FAIL(new_group.push_back(vectors.at(i)))) {
          LOG_WARN("failed to initialize merge vector group", K(ret),
                   K(vector_target_id));
        } else if (OB_FAIL(cid_groups.set_refactored(vector_target_id, new_group))) {
          LOG_WARN("failed to save merge vector group", K(ret),
                   K(vector_target_id));
        }
      }
    }
  }
  iter = cid_groups.begin();
  for (; OB_SUCC(ret) && iter != cid_groups.end(); ++iter) {
    if (OB_FAIL(target_center_ids_.push_back(iter->first))) {
      LOG_WARN("failed to record merge target", K(ret));
    } else if (OB_FAIL(target_vector_counts_.push_back(iter->second.count()))) {
      LOG_WARN("failed to record merge target count", K(ret));
    }
  }
  return ret;
}

int ObSpfreshMergeTask::insert_group_cid_vectors_(
    ObSpfreshVectorGroupMap &cid_groups,
    SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObSpfreshVectorGroupMap::iterator iter = cid_groups.begin();
  for (; OB_SUCC(ret) && iter != cid_groups.end(); ++iter) {
    if (OB_FAIL(table_helper_.insert_to_cid_vector(
        iter->first, iter->second, commit_scn))) {
      LOG_WARN("failed to insert reassigned merge vectors", K(ret),
               "target_center_id", iter->first);
    }
  }
  return ret;
}

int ObSpfreshMergeTask::insert_group_rowid_cids_(
    ObSpfreshVectorGroupMap &cid_groups,
    SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObSpfreshVectorGroupMap::iterator iter = cid_groups.begin();
  for (; OB_SUCC(ret) && iter != cid_groups.end(); ++iter) {
    if (OB_FAIL(table_helper_.insert_to_rowid_cid(
        iter->first, iter->second, commit_scn))) {
      LOG_WARN("failed to insert reassigned rowid-cid mappings", K(ret),
               "target_center_id", iter->first);
    }
  }
  return ret;
}

int ObSpfreshMergeTask::migrate_vectors_(
    const common::ObTabletID &physical_tablet_id,
    int64_t target_center_id,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  SCN commit_scn = SCN::min_scn();
  ObSpfreshVectorGroupMap cid_groups;
  bool groups_created = false;
  bool transaction_started = false;
  int64_t max_target_count = 0;
  ObArray<ObSpfreshVectorRow> vectors;
  ObArray<ObString> rowkeys_to_delete;
  target_center_ids_.reset();
  target_vector_counts_.reset();

  // Defensive: refuse to migrate to an invalid / self target. The caller
  // (execute) already filters this via find_target_, but a stray future
  // caller without the same precheck would otherwise delete the source
  // vectors and write them under a bogus cid, causing irrecoverable loss.
  if (OB_INVALID_ID == target_center_id || target_center_id == source_center_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid merge target, refusing to migrate to avoid data loss",
              K(ret), K_(source_center_id), K(target_center_id));
  }

  // Exclude DML before scanning so deleted or updated postings cannot be copied
  // back from an earlier snapshot. The exclusion lasts until commit/rollback.
  DEBUG_SYNC(SPFRESH_MERGE_BEFORE_TABLET_LOCK);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(table_helper_.start_batch_transaction())) {
    LOG_WARN("failed to start merge batch transaction", K(ret));
  } else {
    transaction_started = true;
    if (OB_FAIL(table_helper_.lock_batch_tablet(physical_tablet_id))) {
      if (OB_EAGAIN == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_TRACE("skip merge while physical tablet is busy", K(ret), K(physical_tablet_id));
      } else {
        LOG_WARN("failed to lock merge tablet", K(ret), K(physical_tablet_id));
      }
    } else if (OB_FAIL(prepare_merge_source_(allocator, vectors, rowkeys_to_delete))) {
      LOG_WARN("failed to prepare locked merge source", K(ret), K_(source_center_id));
    }
    if (OB_SUCC(ret)) {
      DEBUG_SYNC(SPFRESH_MERGE_AFTER_SOURCE_LOCK);
    }
    if (OB_SUCC(ret) && !rowkeys_to_delete.empty()
        && OB_FAIL(table_helper_.touch_cid_vector(
            source_center_id_, rowkeys_to_delete.at(0)))) {
      LOG_WARN("failed to register merge tablet modification intent", K(ret),
               K_(source_center_id));
    }
  }

  // Redistribute source vectors to the nearest NORMAL centroids with enough
  // capacity. The scheduler merges only small sources, so one target remains
  // within the capacity limit even if it receives every source vector.
  max_target_count = ObSpfreshPolicy::get_merge_capacity_rows() - vectors.count();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cid_groups.create(128, "MergeGroups", "MergeGroups", tenant_id_))) {
    LOG_WARN("failed to create merge target groups", K(ret));
  } else {
    groups_created = true;
  }
  if (OB_SUCC(ret) && OB_FAIL(build_target_groups_(
      vectors, max_target_count, allocator, cid_groups))) {
    LOG_WARN("failed to build merge target groups", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(delete_merge_source_vectors_(
      rowkeys_to_delete, commit_scn))) {
    LOG_WARN("failed to delete source cid vectors", K(ret), K_(source_center_id));
  }

  // Write grouped vectors to their selected centroids.
  if (OB_SUCC(ret) && OB_FAIL(insert_group_cid_vectors_(cid_groups, commit_scn))) {
    LOG_WARN("failed to insert grouped merge vectors", K(ret));
  }
  if (OB_SUCC(ret)) {
    migrated_count_ = vectors.count();
  }

  if (OB_SUCC(ret) && OB_FAIL(delete_merge_source_rowid_cids_(
      rowkeys_to_delete, commit_scn))) {
    LOG_WARN("failed to delete source rowid-cid mappings", K(ret), K_(source_center_id));
  }
  if (OB_SUCC(ret) && OB_FAIL(insert_group_rowid_cids_(cid_groups, commit_scn))) {
    LOG_WARN("failed to insert grouped rowid-cid mappings", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(update_merge_source_state_(commit_scn))) {
    LOG_WARN("failed to update merge source state", K(ret), K_(source_center_id));
  }

  // End batch transaction: commit if all succeeded, otherwise rollback
  if (transaction_started) {
    int end_ret = table_helper_.end_batch_transaction(OB_SUCC(ret), commit_scn);
    if (OB_SUCCESS != end_ret) {
      LOG_WARN("failed to end merge transaction", K(end_ret), K(ret));
      if (OB_SUCC(ret) || OB_ENTRY_NOT_EXIST == ret) {
        ret = end_ret;
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] merge migrate committed",
            K_(source_center_id), K(target_center_id), K_(migrated_count), K(ret));
  if (groups_created) {
    cid_groups.destroy();
  }
  return ret;
}

int ObSpfreshMergeTask::prepare_merge_source_(
    common::ObIAllocator &allocator,
    ObArray<ObSpfreshVectorRow> &vectors,
    ObArray<ObString> &rowkeys)
{
  int ret = OB_SUCCESS;
  const ObSpfreshVectorRow *row = nullptr;
  ObString rowkey;

  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null while reading merge source", K(ret), K_(source_center_id));
  } else if (OB_FAIL(table_helper_.scan_cid_vector_by_center(
                 source_center_id_, cache_->get_cent_vec_dim(), allocator, vectors))) {
    LOG_WARN("failed to scan vectors from source head", K(ret),
             K_(source_center_id), "dimension", cache_->get_cent_vec_dim());
  } else if (vectors.count() > ObSpfreshPolicy::get_merge_low_rows()
             || vectors.count() > ObSpfreshPolicy::get_merge_capacity_rows()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_TRACE("[SPFRESH_TRACE] physical source count exceeds merge policy",
              K(ret), K_(source_center_id), "physical_count", vectors.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    row = &vectors.at(i);
    rowkey.assign_ptr(row->rowkey_, row->rowkey_len_);
    if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("failed to push rowkey", K(ret));
    }
  }
  return ret;
}

int ObSpfreshMergeTask::delete_merge_source_vectors_(
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
        source_center_id_, batch_keys, commit_scn))) {
      LOG_WARN("failed to delete source cid vectors", K(ret), K_(source_center_id));
    }
  }
  return ret;
}

int ObSpfreshMergeTask::delete_merge_source_rowid_cids_(
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
    if (OB_SUCC(ret) && OB_FAIL(table_helper_.delete_from_rowid_cid(
        source_center_id_, batch_keys, commit_scn))) {
      LOG_WARN("failed to delete source rowid-cid mappings", K(ret),
               K_(source_center_id));
    }
  }
  return ret;
}

int ObSpfreshMergeTask::update_merge_source_state_(SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObCentroidInfo info;

  if (0 == source_center_id_) {
    if (OB_FAIL(cache_->get_center_info(source_center_id_, SCN::max_scn(), info))) {
      LOG_WARN("bootstrap centroid 0 not found in cache", K(ret), K_(source_center_id));
    }
  } else if (OB_FAIL(table_helper_.update_centroid_state(
      source_center_id_, ObIvfCentroidState::MERGED,
      ObIvfCentroidState::MAX_TYPE, commit_scn))) {
    LOG_WARN("failed to update centroid state to MERGED", K(ret), K_(source_center_id));
  }
  return ret;
}

int ObSpfreshMergeTask::commit_callback_(int64_t target_center_id, int64_t migrated_count)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null", K(ret));
  }

  // Update posting list lengths for all targets in the cache.
  for (int64_t i = 0; OB_SUCC(ret) && i < target_center_ids_.count(); ++i) {
    if (OB_FAIL(cache_->incr_posting_list_length(
        target_center_ids_.at(i), target_vector_counts_.at(i)))) {
      LOG_WARN("failed to incr posting list length", K(ret),
               "target_center_id", target_center_ids_.at(i),
               "migrated_count", target_vector_counts_.at(i));
    }
  }

  // 2. Update source centroid state to MERGED in cache
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cache_->update_centroid_state(source_center_id_,
                 ObIvfCentroidState::MERGED))) {
    LOG_WARN("failed to update source centroid state to MERGED", K(ret), K_(source_center_id));
  }

  if (OB_SUCC(ret)) {
    cutover_modify_ts_ = ObTimeUtility::current_time();
  }

  // 3. Push Reassign task for source centroid
  if (OB_SUCC(ret)) {
    ObSpfreshTaskKey task_key(index_table_id_, queue_tablet_id_, source_center_id_, ns_id_,
                              ltable_id_, ObSpfreshTaskType::REASSIGN);
    ObSpfreshTask reassign_task(
        ObSpfreshTaskType::REASSIGN, task_key, cutover_modify_ts_);
    reassign_task.cache_guard_ = cache_guard_;
    if (OB_FAIL(task_queue_->push(reassign_task))) {
      LOG_WARN("failed to push reassign task", K(ret), K_(source_center_id));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] merge commit_callback: REASSIGN pushed",
            K_(source_center_id), K(target_center_id), K(migrated_count), K(ret));
  return ret;
}

int ObSpfreshMergeTask::rollback_lock_()
{
  int ret = OB_SUCCESS;

  // Rollback KV state for non-bootstrap centroids
  if (0 != source_center_id_) {
    SCN rollback_scn;
    int kv_ret = OB_SUCCESS;
    if (OB_SUCCESS != (kv_ret = table_helper_.update_centroid_state(
        source_center_id_, ObIvfCentroidState::NORMAL,
        ObIvfCentroidState::MERGING, rollback_scn))) {
      LOG_WARN("failed to rollback KV centroid state", K(kv_ret), K_(source_center_id));
    }
  }

  // Reset cache state
  if (OB_NOT_NULL(cache_)) {
    int cache_ret = OB_SUCCESS;
    if (OB_SUCCESS != (cache_ret = cache_->update_centroid_state(
        source_center_id_, ObIvfCentroidState::NORMAL))) {
      LOG_WARN("failed to rollback cache centroid state", K(cache_ret), K_(source_center_id));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] rollback lock done", KR(ret), K_(source_center_id));
  return ret;
}

int ObSpfreshMergeTask::push_retry_task_()
{
  int ret = OB_SUCCESS;

  ObSpfreshTaskKey task_key(index_table_id_, queue_tablet_id_, source_center_id_, ns_id_,
                            ltable_id_, ObSpfreshTaskType::MERGE);
  ObSpfreshTask merge_task(ObSpfreshTaskType::MERGE, task_key);
  merge_task.cache_guard_ = cache_guard_;
  if (OB_FAIL(task_queue_->push(merge_task))) {
    LOG_WARN("failed to push merge task for retry", K(ret), K_(source_center_id));
  }

  LOG_TRACE("[SPFRESH_TRACE] push merge task for retry done", KR(ret), K_(source_center_id));
  return ret;
}

int ObSpfreshMergeTask::push_reassign_()
{
  int ret = OB_SUCCESS;

  ObSpfreshTaskKey task_key(index_table_id_, queue_tablet_id_, source_center_id_, ns_id_,
                            ltable_id_, ObSpfreshTaskType::REASSIGN);
  const int64_t cutover_modify_ts = ObTimeUtility::current_time();
  ObSpfreshTask reassign_task(
      ObSpfreshTaskType::REASSIGN, task_key, cutover_modify_ts);
  reassign_task.cache_guard_ = cache_guard_;
  if (OB_FAIL(task_queue_->push(reassign_task))) {
    LOG_WARN("failed to push reassign task for lazy recovery", K(ret), K_(source_center_id));
  }

  LOG_TRACE("[SPFRESH_TRACE] push reassign task for lazy recovery done", KR(ret), K_(source_center_id));
  return ret;
}

}  // namespace share
}  // namespace oceanbase
