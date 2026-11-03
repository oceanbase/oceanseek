/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_reassign_task.h"
#include "share/vector_index/ob_vector_index_ivf_cache_util.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "share/parameter/ob_parameter_macro.h"
#include "share/ob_debug_sync.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace share
{

// Out-of-class definition for BATCH_SIZE (required for ODR-use)
const int64_t ObSpfreshReassignTask::BATCH_SIZE;

ObSpfreshReassignTask::ObSpfreshReassignTask()
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
    dist_algo_(ObVectorIndexDistAlgorithm::VIDA_L2),
    physical_ls_id_(),
    physical_tablet_id_(),
    cutover_modify_ts_(0),
    has_committed_batch_(false),
    lock_scn_(),
    lock_old_state_(ObIvfCentroidState::MAX_TYPE),
    total_processed_(0)
{
}

ObSpfreshReassignTask::~ObSpfreshReassignTask()
{
  destroy();
}

int ObSpfreshReassignTask::init(uint64_t tenant_id,
                                common::ObMySQLProxy *sql_proxy,
                                uint64_t index_table_id,
                                const ObSpfreshKvTableInfo &kv_info,
                                uint64_t queue_tablet_id,
                                int64_t center_id,
                                int64_t ns_id,
                                uint64_t ltable_id,
                                ObSpfreshTaskQueue *task_queue,
                                ObIvfMapCentCache *cache,
                                ObVectorIndexDistAlgorithm dist_algo,
                                const share::ObLSID &physical_ls_id,
                                const common::ObTabletID &physical_tablet_id,
                                int64_t cutover_modify_ts)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSpfreshReassignTask init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(sql_proxy) ||
             OB_INVALID_ID == index_table_id || OB_INVALID_ID == queue_tablet_id ||
             OB_INVALID_ID == center_id || OB_ISNULL(task_queue) ||
             OB_ISNULL(cache) || !physical_ls_id.is_valid() ||
             !physical_tablet_id.is_valid() || cutover_modify_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(sql_proxy),
             K(index_table_id), K(queue_tablet_id), K(center_id), KP(task_queue),
             KP(cache));
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
    physical_ls_id_ = physical_ls_id;
    physical_tablet_id_ = physical_tablet_id;
    cutover_modify_ts_ = cutover_modify_ts;
    is_inited_ = true;
  }
  return ret;
}

void ObSpfreshReassignTask::destroy()
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
    dist_algo_ = ObVectorIndexDistAlgorithm::VIDA_L2;
    physical_ls_id_.reset();
    physical_tablet_id_.reset();
    cutover_modify_ts_ = 0;
    has_committed_batch_ = false;
    lock_scn_.reset();
    lock_old_state_ = ObIvfCentroidState::MAX_TYPE;
    total_processed_ = 0;
  }
}

int ObSpfreshReassignTask::execute()
{
  int ret = OB_SUCCESS;
  bool lock_acquired = false;
  bool compensation_failed = false;
  int64_t start_time = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshReassignTask not init", K(ret));
  } else {
    // Phase 1: Acquire lock (cache filter → KV CAS → cache CAS)
    if (OB_FAIL(acquire_lock_(lock_scn_, lock_acquired, compensation_failed))) {
      // OB_STATE_NOT_MATCH / OB_ENTRY_NOT_EXIST from acquire_lock_ are benign
      // skips: the centroid's state is not ready for reassign (e.g. still
      // NORMAL, or removed by a concurrent worker). They are NOT real failures
      // and must not be recorded as RET_CODE!=0 in task history.
      if (!compensation_failed
          && (OB_STATE_NOT_MATCH == ret || OB_ENTRY_NOT_EXIST == ret)) {
        LOG_TRACE("[SPFRESH_TRACE] centroid state not ready, benign skip reassign",
                 KR(ret), K_(center_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("acquire lock system error", K(ret), K_(center_id));
      }
    } else if (!lock_acquired) {
      LOG_TRACE("[SPFRESH_TRACE] centroid state changed, skip reassign", KR(ret), K_(center_id));
    } else {

    if (OB_UNLIKELY(GCONF.is_debug_sync_enabled())) {
      (void)GDS.execute(BEFORE_SPFRESH_REASSIGN_EXECUTE,
          tenant_id_, physical_ls_id_.id(), physical_tablet_id_.id(), index_table_id_);
    }

    // Phase 2: Process vectors in batches
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(wait_pre_cutover_writers_ended_())) {
      LOG_WARN("pre-cutover writers have not drained", K(ret), K_(center_id),
               K_(cutover_modify_ts), K_(physical_ls_id), K_(physical_tablet_id));
    } else if (OB_FAIL(process_batches_())) {
      LOG_WARN("failed to process batches", K(ret), K_(center_id));
    }

    }  // end lock_acquired guard

    // Handle failure - distinguish benign skip from real failure
    if (OB_FAIL(ret) && lock_acquired) {
      const bool is_retry_pending = OB_EAGAIN == ret;
      if (is_retry_pending) {
        // Keep the source closed while writers drain or tablet admission is busy.
        // Reopening it here
        // creates a new assignment window between scheduler retries.
        LOG_INFO("reassign writer barrier or tablet lock pending; preserve REASSIGNING state",
                 K_(center_id), K_(cutover_modify_ts), K_(physical_ls_id),
                 K_(physical_tablet_id));
      } else if (!has_committed_batch_) {
        int rollback_ret = rollback_lock_();
        if (OB_SUCCESS != rollback_ret) {
          LOG_ERROR("failed to rollback lock after reassign failure, KV/cache may be inconsistent",
                    K(rollback_ret), K_(center_id));
        }
      } else {
        // A committed 05/06 migration cannot be undone by changing centroid
        // state in a separate transaction. Preserve REASSIGNING for retry.
        LOG_ERROR("reassign failed after a committed batch; preserve state for retry",
                  K(ret), K_(center_id), K_(total_processed));
      }
      if (!is_retry_pending) {
        // All retryable failures must use the handler's atomic
        // requeue_in_flight_task_ path. Pushing a duplicate from execute()
        // can leave it parked behind this task's own in-flight key.
        ret = OB_EAGAIN;
      }
    }
  }

  int64_t elapsed_us = ObTimeUtility::current_time() - start_time;
  LOG_TRACE("[SPFRESH_TRACE] Reassign task done", KR(ret), K_(center_id), K_(total_processed), K(elapsed_us));

  return ret;
}

int ObSpfreshReassignTask::wait_pre_cutover_writers_ended_()
{
  int ret = OB_SUCCESS;
  storage::ObLSService *ls_service = MTL(storage::ObLSService *);
  storage::ObLSHandle ls_handle;
  transaction::ObTransID pending_tx_id;
  const common::ObTabletID &tablet_id = physical_tablet_id_;
  // ObLockMemCtx uses create_timestamp < timestamp. Advance the exclusive
  // upper bound by one microsecond so a writer registered in the same clock
  // tick as cutover is included (create_timestamp <= cutover_modify_ts_).
  const int64_t barrier_modify_ts = INT64_MAX == cutover_modify_ts_
      ? cutover_modify_ts_ : cutover_modify_ts_ + 1;

  if (!physical_ls_id_.is_valid() || !tablet_id.is_valid()
      || cutover_modify_ts_ <= 0 || INT64_MAX == cutover_modify_ts_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid centroid cutover barrier", K(ret), K_(physical_ls_id), K(tablet_id),
             K_(cutover_modify_ts));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(
      physical_ls_id_, ls_handle, storage::ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls for centroid cutover barrier", K(ret), K_(physical_ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->check_modify_time_elapsed(
      tablet_id, barrier_modify_ts, pending_tx_id))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("failed to check centroid cutover barrier", K(ret), K_(physical_ls_id),
               K(tablet_id), K_(cutover_modify_ts), K(pending_tx_id));
    }
  }
  return ret;
}

int ObSpfreshReassignTask::acquire_lock_(
    SCN &cur_scn, bool &lock_success, bool &compensation_failed)
{
  int ret = OB_SUCCESS;
  cur_scn = SCN::min_scn();
  lock_success = false;
  compensation_failed = false;
  int old_state_for_trace = -1;

  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null", K(ret));
  } else {
    // Step 1: cache filter — determine expected_old_state
    ObCentroidInfo centroid_info;
    ObIvfCentroidState expected_old_state = ObIvfCentroidState::MAX_TYPE;
    if (OB_FAIL(cache_->get_center_info(center_id_, SCN::max_scn(), centroid_info))) {
      LOG_WARN("failed to get centroid info", K(ret), K_(center_id));
    } else if (ObIvfCentroidState::NORMAL == centroid_info.state_) {
      LOG_TRACE("[SPFRESH_TRACE] centroid already NORMAL, reassign skipped", KR(ret), K_(center_id));
    } else if (ObIvfCentroidState::SPLIT_DONE != centroid_info.state_
               && ObIvfCentroidState::MERGED != centroid_info.state_
               && ObIvfCentroidState::REASSIGNING != centroid_info.state_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("centroid state not ready for reassign", K(ret), K_(center_id),
               "state", static_cast<int>(centroid_info.state_));
    } else {
      expected_old_state = centroid_info.state_;
      lock_old_state_ = expected_old_state;
      old_state_for_trace = static_cast<int>(expected_old_state);

      // Step 2: KV CAS (non-bootstrap centroids)
      if (0 == center_id_) {
        lock_success = true;
        cur_scn = SCN::min_scn();
      } else if (OB_FAIL(table_helper_.start_batch_transaction())) {
        LOG_WARN("start batch tx failed", K(ret));
      } else {
        int64_t affected_rows = 0;
        int end_ret = OB_SUCCESS;
        if (OB_FAIL(table_helper_.update_centroid_value_cas(
                center_id_, expected_old_state, ObIvfCentroidState::REASSIGNING,
                affected_rows))) {
          LOG_WARN("KV CAS failed", K(ret));
        } else if (1 == affected_rows) {
          // KV CAS succeeded
        } else {
          LOG_TRACE("[SPFRESH_TRACE] KV CAS failed, affected_rows",
                   K(affected_rows), K_(center_id));
        }

        if (OB_FAIL(ret)) {
          if (OB_SUCCESS != (end_ret = table_helper_.end_batch_transaction(false, cur_scn))) {
            LOG_WARN("end batch tx on error failed", K(end_ret));
          }
        } else if (OB_SUCCESS != (end_ret = table_helper_.end_batch_transaction(
            1 == affected_rows, cur_scn))) {
          LOG_WARN("end batch tx failed", K(end_ret));
          ret = end_ret;
        } else if (1 == affected_rows) {
          // Step 3: cache CAS sync
          bool cas_ok = false;
          if (OB_FAIL(cache_->update_centroid_state_cas(center_id_,
                  expected_old_state, ObIvfCentroidState::REASSIGNING, cas_ok))) {
            LOG_WARN("cache CAS failed", K(ret), K_(center_id));
            lock_success = false;
            // Rollback KV state
            SCN rollback_scn;
            int kv_ret = OB_SUCCESS;
            if (OB_SUCCESS != (kv_ret = table_helper_.rollback_centroid_state(
                center_id_, ObIvfCentroidState::REASSIGNING, expected_old_state,
                rollback_scn))) {
              compensation_failed = true;
              ret = kv_ret;
              LOG_ERROR("rollback KV after cache CAS failure, KV/cache inconsistent",
                        K(kv_ret), K_(center_id));
            }
          } else if (!cas_ok) {
            lock_success = false;
            LOG_TRACE("[SPFRESH_TRACE] cache CAS not ok, another worker won", KR(ret), K_(center_id));
            // Rollback KV state
            SCN rollback_scn;
            int kv_ret = OB_SUCCESS;
            if (OB_SUCCESS != (kv_ret = table_helper_.rollback_centroid_state(
                center_id_, ObIvfCentroidState::REASSIGNING, expected_old_state,
                rollback_scn))) {
              compensation_failed = true;
              ret = kv_ret;
              LOG_ERROR("rollback KV after cache CAS miss failed, KV/cache inconsistent",
                        K(kv_ret), K_(center_id));
            }
          } else {
            lock_success = true;
          }
        }
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] reassign lock acquired",
            K_(center_id), K(lock_success), K(old_state_for_trace), K(ret));
  return ret;
}

int ObSpfreshReassignTask::finish_batch_iteration_(
    int batch_ret,
    const char *next_rowkey,
    int64_t next_rowkey_len,
    bool is_last_batch,
    int64_t processed_count,
    char *last_rowkey_buf,
    int64_t last_rowkey_buf_len,
    const char *&last_rowkey,
    int64_t &last_rowkey_len,
    int64_t &total_processed,
    common::ObIAllocator &allocator)
{
  int ret = batch_ret;
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(processed_count < 0
        || total_processed > INT64_MAX - processed_count)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("invalid reassign batch progress", K(ret), K(processed_count),
               K(total_processed));
    } else if (!is_last_batch
               && (OB_ISNULL(next_rowkey) || OB_ISNULL(last_rowkey_buf)
                   || next_rowkey_len <= 0
                   || next_rowkey_len > last_rowkey_buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid reassign pagination cursor", K(ret), KP(next_rowkey),
               KP(last_rowkey_buf), K(next_rowkey_len), K(last_rowkey_buf_len));
    } else {
      if (!is_last_batch) {
        MEMCPY(last_rowkey_buf, next_rowkey, next_rowkey_len);
        last_rowkey = last_rowkey_buf;
        last_rowkey_len = next_rowkey_len;
      }
      total_processed += processed_count;
    }
  }

  // process_one_batch_ has returned, and the only cross-batch state (the
  // cursor/count) is now stored outside this allocator. This must remain the
  // final operation for both successful and failed batches.
  allocator.reuse();
  return ret;
}

int ObSpfreshReassignTask::process_batches_()
{
  int ret = OB_SUCCESS;
  char last_rowkey_buf[8];
  const char *last_rowkey = nullptr;
  int64_t last_rowkey_len = 0;
  bool is_last_batch = false;
  ObArenaAllocator vec_allocator("SpfreshRaV", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);

  // Loop until all vectors are processed.
  while (OB_SUCC(ret) && !is_last_batch) {
    int64_t processed_count = 0;
    char next_rowkey_buf[8];
    int64_t next_rowkey_len = 0;
    const int batch_ret = process_one_batch_(
        last_rowkey, last_rowkey_len, next_rowkey_buf, next_rowkey_len,
        is_last_batch, processed_count, vec_allocator);
    ret = finish_batch_iteration_(
        batch_ret, next_rowkey_buf, next_rowkey_len, is_last_batch,
        processed_count, last_rowkey_buf, sizeof(last_rowkey_buf),
        last_rowkey, last_rowkey_len, total_processed_, vec_allocator);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to process or finish batch", K(ret), K(batch_ret),
               K_(center_id));
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] reassign batches done",
            K_(center_id), K_(total_processed), K(ret));
  return ret;
}

int ObSpfreshReassignTask::process_one_batch_(const char *last_rowkey,
                                              int64_t last_rowkey_len,
                                              char *next_rowkey_buf,
                                              int64_t &next_rowkey_len,
                                              bool &is_last_batch,
                                              int64_t &processed_count,
                                              common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  is_last_batch = false;
  processed_count = 0;
  SCN commit_scn;

  // Each batch is atomic (DELETE + INSERT in one transaction).
  // On failure, the transaction is rolled back and last_rowkey is unchanged,
  // so the caller retries the same batch starting from the same rowkey cursor.
  // This guarantees idempotency: vectors already migrated in a committed batch
  // are not re-processed on retry.

  // Close DML admission before reading any source posting. The lock belongs to
  // this batch transaction and is released by commit/rollback, including empty scans.
  // The cache dimension is validated by the scan helper before it issues SQL.
  ObArray<ObSpfreshVectorRow> vectors;
  bool batch_started = false;
  int64_t vector_dim = 0;
  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null before reassign scan", K(ret), K_(center_id));
  } else if (FALSE_IT(vector_dim = cache_->get_cent_vec_dim())) {
  } else {
    DEBUG_SYNC(SPFRESH_REASSIGN_BEFORE_TABLET_LOCK);
    if (OB_FAIL(table_helper_.start_batch_transaction())) {
      LOG_WARN("failed to start reassign batch", K(ret));
    } else {
      batch_started = true;
      if (OB_FAIL(table_helper_.lock_batch_tablet(physical_tablet_id_))) {
        LOG_WARN("failed to acquire reassign tablet exclusion", K(ret), K_(physical_tablet_id));
      } else {
        DEBUG_SYNC(SPFRESH_REASSIGN_AFTER_SOURCE_LOCK);
        if (OB_FAIL(table_helper_.scan_cid_vector_by_center_limit(
            center_id_, last_rowkey, last_rowkey_len, BATCH_SIZE,
            vector_dim, allocator, vectors))) {
          LOG_WARN("failed to scan locked sources", K(ret), K_(center_id), K(vector_dim));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (batch_started) {
      const int end_ret = table_helper_.end_batch_transaction(false, commit_scn);
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to roll back reassign batch", K(end_ret), K(ret));
      }
      if (OB_EAGAIN == ret && OB_SUCCESS == end_ret) {
        DEBUG_SYNC(SPFRESH_REASSIGN_AFTER_TABLET_LOCK_CONFLICT);
      }
    }
  } else if (vectors.empty()) {
    is_last_batch = true;
    if (OB_FAIL(handle_empty_batch_())) {
      LOG_WARN("failed to handle empty batch", K(ret), K_(center_id));
    }
  } else {
    is_last_batch = (static_cast<int64_t>(vectors.count()) < BATCH_SIZE);
    ObArray<ObString> rowkeys_to_delete;

    // Find nearest centroid for each vector and group by target
    ObArenaAllocator centroid_alloc("SpfreshReas", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::hash::ObHashMap<int64_t, common::ObSEArray<ObSpfreshVectorRow, 8>> cid_groups;
    // Register a physical-tablet modification in the same transaction before
    // selecting a target. A concurrent target reclamation barrier must see and
    // wait for this transaction until its 05/06 changes commit or roll back.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(table_helper_.touch_cid_vector(
        center_id_, ObString(vectors.at(0).rowkey_len_, vectors.at(0).rowkey_)))) {
      LOG_WARN("failed to register reassign tablet modification", K(ret), K_(center_id));
    } else if (OB_ISNULL(cache_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("cache is null", K(ret));
    } else if (OB_FAIL(cid_groups.create(128, "ReasGroups", "ReasGroups", tenant_id_))) {
      LOG_WARN("failed to create cid groups map", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
        const ObSpfreshVectorRow &row = vectors.at(i);
        int64_t target_center_id = OB_INVALID_ID;
        ObMapCacheForEachFunc for_each_fn(
            cache_, SCN::max_scn(), center_id_, INT64_MAX, true /* NORMAL only */);
        if (OB_FAIL(rowkeys_to_delete.push_back(ObString(row.rowkey_len_, row.rowkey_)))) {
          LOG_WARN("failed to append locked source rowkey", K(ret));
        } else if (OB_FAIL(find_nearest_center_from_map(
            reinterpret_cast<float *>(const_cast<char *>(row.vector_)), vector_dim,
            for_each_fn, dist_algo_,
            centroid_alloc, target_center_id))) {
          LOG_WARN("failed to find nearest centroid for vector", K(ret), K(i));
        } else if (OB_INVALID_ID == target_center_id) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no writable centroid found", K(ret), K_(center_id));
        } else {
          ObCentroidInfo target_info;
          if (OB_FAIL(cache_->get_center_info(
              target_center_id, SCN::max_scn(), target_info))) {
            LOG_WARN("failed to verify reassign target", K(ret), K(target_center_id));
          } else if (ObIvfCentroidState::NORMAL != target_info.state_
                     || target_center_id == center_id_) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_TRACE("[SPFRESH_TRACE] reassign target no longer writable",
                      K(ret), K(target_center_id),
                      "state", static_cast<int>(target_info.state_));
          }
        }
        if (OB_SUCC(ret)) {
          common::ObSEArray<ObSpfreshVectorRow, 8> *group = cid_groups.get(target_center_id);
          if (OB_NOT_NULL(group)) {
            if (OB_FAIL(group->push_back(row))) {
              LOG_WARN("failed to push vector to group", K(ret));
            }
          } else {
            common::ObSEArray<ObSpfreshVectorRow, 8> new_group;
            if (OB_FAIL(new_group.push_back(row))) {
              LOG_WARN("failed to push vector to new group", K(ret));
            } else if (OB_FAIL(cid_groups.set_refactored(target_center_id, new_group))) {
              LOG_WARN("failed to set group", K(ret));
            }
          }
        }
      }
    }

    // Delete from old head
    if (OB_FAIL(ret)) {
    } else if (!rowkeys_to_delete.empty() && OB_FAIL(table_helper_.delete_from_cid_vector(
        center_id_, rowkeys_to_delete, commit_scn))) {
      LOG_WARN("failed to delete vectors from old head", K(ret), K_(center_id));
    }

    // Delete old aux_type=6 (rowid-cid) mappings
    if (OB_FAIL(ret)) {
    } else if (!rowkeys_to_delete.empty() &&
               OB_FAIL(table_helper_.delete_from_rowid_cid(
                   center_id_, rowkeys_to_delete, commit_scn))) {
      LOG_WARN("failed to delete old rowid-cid mappings", K(ret), K_(center_id));
    }

    // Batch insert vectors and aux_type=6 mappings grouped by target centroid
    if (OB_FAIL(ret)) {
    } else {
      common::hash::ObHashMap<int64_t, common::ObSEArray<ObSpfreshVectorRow, 8>>::iterator
          iter = cid_groups.begin();
      for (; OB_SUCC(ret) && iter != cid_groups.end(); ++iter) {
        if (!iter->second.empty() &&
            OB_FAIL(table_helper_.insert_to_cid_vector(
                iter->first, iter->second, commit_scn))) {
          LOG_WARN("failed to insert vectors to target head", K(ret), K(iter->first));
        } else if (!iter->second.empty() &&
                   OB_FAIL(table_helper_.insert_to_rowid_cid(
                       iter->first, iter->second, commit_scn))) {
          LOG_WARN("failed to insert new rowid-cid mappings", K(ret), K(iter->first));
        }
      }
    }

    // If last batch, delete centroid lifecycle metadata from KV.
    if (OB_SUCC(ret) && is_last_batch) {
      if (OB_FAIL(table_helper_.delete_centroid(center_id_, commit_scn))) {
        LOG_WARN("failed to delete centroid", K(ret), K_(center_id));
      }
    }

    // End batch transaction: commit if all succeeded, rollback on error
    {
      int end_ret = table_helper_.end_batch_transaction(OB_SUCC(ret), commit_scn);
      if (OB_SUCC(ret)) {
        if (OB_SUCCESS != end_ret) {
          ret = end_ret;
          LOG_WARN("failed to end batch transaction", K(ret));
        } else {
          has_committed_batch_ = true;
        }
      }
    }

    // Cache updates after commit (only if transaction succeeded)
    if (OB_SUCC(ret) && OB_NOT_NULL(cache_)) {
      int cache_count_ret = OB_SUCCESS;
      // Increment posting list length for each target centroid
      common::hash::ObHashMap<int64_t, common::ObSEArray<ObSpfreshVectorRow, 8>>::iterator iter = cid_groups.begin();
      for (; iter != cid_groups.end(); ++iter) {
        int64_t group_count = iter->second.count();
        if (group_count > 0) {
          int tmp_ret = cache_->incr_posting_list_length(iter->first, group_count);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("failed to incr posting list length for target; reconcile from KV",
                      K(tmp_ret), "target_cid", iter->first, K(group_count));
            if (OB_SUCCESS == cache_count_ret) {
              cache_count_ret = tmp_ret;
            }
          }
        }
      }

      // Mark centroid as deleted in cache (only on last batch)
      if (is_last_batch) {
        int tmp_ret = cache_->set_centroid_del_scn(center_id_, commit_scn);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("failed to set centroid del scn in cache", K(tmp_ret), K_(center_id));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
      }

      // Decrement posting list length for old centroid
      int tmp_ret = cache_->decr_posting_list_length(
          center_id_, vectors.count());
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to decr posting list length for old centroid; reconcile from KV",
                 K(tmp_ret), K_(center_id));
        if (OB_SUCCESS == cache_count_ret) {
          cache_count_ret = tmp_ret;
        }
      }

      if (OB_SUCCESS != cache_count_ret) {
        common::ObSEArray<int64_t, 16> affected_cids;
        int reconcile_ret = affected_cids.push_back(center_id_);
        for (common::hash::ObHashMap<
                 int64_t,
                 common::ObSEArray<ObSpfreshVectorRow, 8>>::iterator group = cid_groups.begin();
             OB_SUCCESS == reconcile_ret && group != cid_groups.end(); ++group) {
          reconcile_ret = affected_cids.push_back(group->first);
        }
        if (OB_SUCCESS == reconcile_ret) {
          reconcile_ret = reconcile_cache_posting_counts_(affected_cids);
        }
        if (OB_SUCCESS != reconcile_ret) {
          ret = reconcile_ret;
          LOG_ERROR("failed to reconcile reassign cache posting counts from KV",
                    K(ret), K(cache_count_ret), K_(center_id));
        } else {
          LOG_INFO("reconciled reassign cache posting counts from KV",
                   K(cache_count_ret), K_(center_id));
        }
      }
    }
    cid_groups.destroy();

    // Save last rowkey for pagination
    if (OB_SUCC(ret) && !is_last_batch && OB_NOT_NULL(next_rowkey_buf)) {
      const ObSpfreshVectorRow &last_row = vectors.at(vectors.count() - 1);
      if (OB_NOT_NULL(last_row.rowkey_) && last_row.rowkey_len_ >= 8) {
        MEMCPY(next_rowkey_buf, last_row.rowkey_, 8);
        next_rowkey_len = 8;
      } else {
        next_rowkey_len = 0;
      }
    }
    if (OB_SUCC(ret)) {
      processed_count = vectors.count();
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] reassign batch tx done",
            K_(center_id), K(is_last_batch), K(processed_count), K(ret));
  return ret;
}

int ObSpfreshReassignTask::handle_empty_batch_()
{
  int ret = OB_SUCCESS;
  SCN commit_scn;

  // The caller keeps the tablet locked from the empty scan through this commit.
  if (OB_FAIL(table_helper_.delete_centroid(center_id_, commit_scn))) {
    LOG_WARN("failed to delete centroid in empty batch", K(ret), K_(center_id));
  }

  {
    int end_ret = table_helper_.end_batch_transaction(OB_SUCC(ret), commit_scn);
    if (OB_SUCC(ret)) {
      if (OB_SUCCESS != end_ret) {
        ret = end_ret;
        LOG_WARN("failed to end batch transaction", K(ret));
      } else {
        has_committed_batch_ = true;
      }
    }
  }

  // Mark centroid as deleted in cache after commit
  if (OB_SUCC(ret) && OB_NOT_NULL(cache_)) {
    int tmp_ret = cache_->set_centroid_del_scn(center_id_, commit_scn);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to set centroid del scn in cache", K(tmp_ret), K_(center_id));
      ret = tmp_ret;
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] reassign empty batch done",
            K_(center_id), K_(total_processed), K(ret));
  return ret;
}

int ObSpfreshReassignTask::rollback_lock_()
{
  int ret = OB_SUCCESS;

  if (0 != center_id_) {
    SCN rollback_scn;
    int kv_ret = OB_SUCCESS;
    if (OB_SUCCESS != (kv_ret = table_helper_.rollback_centroid_state(
        center_id_, ObIvfCentroidState::REASSIGNING,
        lock_old_state_, rollback_scn))) {
      ret = kv_ret;
      LOG_WARN("failed to rollback KV centroid state", K(ret), K_(center_id));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(cache_)) {
    bool cas_ok = false;
    if (OB_FAIL(cache_->update_centroid_state_cas(center_id_,
        ObIvfCentroidState::REASSIGNING, lock_old_state_, cas_ok))) {
      LOG_WARN("failed to rollback cache centroid state", K(ret), K_(center_id));
    } else if (!cas_ok) {
      ObCentroidInfo info;
      if (OB_FAIL(cache_->get_center_info(center_id_, SCN::max_scn(), info))) {
        LOG_WARN("failed to confirm cache rollback target", K(ret), K_(center_id));
      } else if (lock_old_state_ != info.state_) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("cache changed to a third state during rollback", K(ret), K_(center_id),
                 "state", static_cast<int>(info.state_));
      }
    }
  }

  LOG_TRACE("[SPFRESH_TRACE] rollback reassign lock done",
            KR(ret), K_(center_id), K_(lock_old_state));
  return ret;
}

ObSpfreshReassignTask::CollectCacheInfoFn::CollectCacheInfoFn(
    common::ObIArray<std::pair<int64_t, ObCentroidInfo>> &infos,
    const common::ObIArray<int64_t> &cids)
  : infos_(infos), cids_(cids)
{
}

int ObSpfreshReassignTask::CollectCacheInfoFn::operator()(
    int64_t cid, const ObCentroidInfo &info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < cids_.count(); ++i) {
    if (cid == cids_.at(i)) {
      ret = infos_.push_back(std::make_pair(cid, info));
      break;
    }
  }
  return ret;
}

int ObSpfreshReassignTask::reconcile_cache_posting_counts_(
    const common::ObIArray<int64_t> &affected_cids)
{
  int ret = OB_SUCCESS;
  common::ObArray<std::pair<uint64_t, int64_t>> kv_counts;
  common::ObSEArray<std::pair<int64_t, ObCentroidInfo>, 64> cache_infos;
  kv_counts.set_attr(ObMemAttr(tenant_id_, "SpfreshRCnt"));

  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cache is null during posting-count reconciliation", K(ret));
  } else {
    CollectCacheInfoFn collect_fn(cache_infos, affected_cids);
    if (OB_FAIL(cache_->for_each_centroid_info_all(SCN::max_scn(), collect_fn))) {
      LOG_WARN("failed to collect cache centroids for reconciliation", K(ret),
               K_(center_id));
    }
  }

  // Read the persistent snapshot after observing cache counts. Reject a
  // changed count at publication rather than clobbering a concurrent update.
  if (OB_SUCC(ret) && OB_FAIL(table_helper_.count_vectors_group_by_center(
      cache_->get_cent_vec_dim(), kv_counts))) {
    LOG_WARN("failed to count persistent postings by centroid", K(ret), K_(center_id),
             "dimension", cache_->get_cent_vec_dim());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cache_infos.count(); ++i) {
    int64_t persistent_count = 0;
    for (int64_t j = 0; j < kv_counts.count(); ++j) {
      if (static_cast<uint64_t>(cache_infos.at(i).first) == kv_counts.at(j).first) {
        persistent_count = kv_counts.at(j).second;
        break;
      }
    }
    if (OB_FAIL(cache_->reconcile_posting_list_length(
        cache_infos.at(i).first, cache_infos.at(i).second.posting_list_count_,
        persistent_count))) {
      LOG_WARN("failed to publish reconciled centroid posting count", K(ret),
               "cid", cache_infos.at(i).first, K(persistent_count));
    }
  }

  return ret;
}

}  // namespace share
}  // namespace oceanbase
