/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/disk_bbq_cache/ob_disk_bbq_cache_task.h"
#include "lib/time/ob_time_utility.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace share
{

int ObDiskBBQCacheTaskUtil::check_sstable_exists(
    storage::ObLS &ls,
    const ObIvfCacheMgrKey &cache_key,
    bool &exists)
{
  int ret = OB_SUCCESS;
  storage::ObTabletHandle tablet_handle;
  storage::ObTableHandleV2 table_handle;
  exists = false;
  if (!cache_key.is_sstable_key()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DiskBBQ cache key", KR(ret), K(cache_key));
  } else if (OB_ISNULL(ls.get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("DiskBBQ cache tablet service is null", KR(ret), K(cache_key));
  } else if (OB_FAIL(ls.get_tablet_svr()->get_tablet(
                 cache_key.get_tablet_id(), tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get DiskBBQ cache tablet", KR(ret), K(cache_key));
    }
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("DiskBBQ cache tablet handle is null", KR(ret), K(cache_key));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_table(
                 cache_key.get_sstable_key(), table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_TABLE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get cached DiskBBQ SSTable", KR(ret), K(cache_key));
    }
  } else {
    exists = true;
  }
  return ret;
}

int ObDiskBBQCacheAsyncTask::clean_invalid_caches_()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLSCacheMgrKeyPair, 64> cache_keys;
  ObLSID ls_id = ls_id_;
  storage::ObLS *ls = OB_ISNULL(ctx_) ? nullptr : ctx_->get_ls();
  if (OB_ISNULL(vec_idx_mgr_) || OB_ISNULL(ls) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DiskBBQ cache clean task state",
             KR(ret), KP_(vec_idx_mgr), KP(ls), KP_(ctx));
  } else if (OB_FAIL(vec_idx_mgr_->get_cache_keys(ls_id, cache_keys))) {
    LOG_WARN("failed to snapshot DiskBBQ cache keys", KR(ret), K(ls_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cache_keys.count(); ++i) {
    const ObIvfCacheMgrKey &cache_key = cache_keys.at(i).key_;
    if (!cache_key.is_sstable_key()
        || cache_key.get_tablet_id() != ctx_->task_status_.tablet_id_) {
      continue;
    }
    bool exists = false;
    bool is_cancel = false;
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_is_cancel(ctx_, is_cancel))) {
      LOG_WARN("failed to check DiskBBQ cache clean task cancellation", KR(ret));
    } else if (is_cancel) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(ObDiskBBQCacheTaskUtil::check_sstable_exists(
            *ls, cache_key, exists))) {
      LOG_WARN("failed to validate DiskBBQ cache", KR(ret), K(cache_key));
    } else if (!exists) {
      ObIvfCacheMgrGuard cache_mgr_guard;
      ObIvfCacheMgr *cache_mgr = nullptr;
      ObIvfSSTableCentCache *cache = nullptr;
      const int tmp_ret = vec_idx_mgr_->get_ivf_cache_mgr_guard(
          cache_key, cache_mgr_guard);
      if (OB_HASH_NOT_EXIST == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
      } else if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to acquire invalid DiskBBQ cache manager",
                 KR(ret), K(cache_key));
      } else if (OB_ISNULL(cache_mgr = cache_mgr_guard.get_ivf_cache_mgr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null DiskBBQ cache manager", KR(ret), K(cache_key));
      } else {
        const int cache_ret = cache_mgr->get_cache_node<ObIvfSSTableCentCache>(
            IVF_SSTABLE_CENTROID_CACHE, cache);
        if (OB_SUCCESS != cache_ret && OB_HASH_NOT_EXIST != cache_ret
            && OB_ENTRY_NOT_EXIST != cache_ret) {
          ret = cache_ret;
          LOG_WARN("failed to inspect invalid DiskBBQ cache",
                   KR(ret), K(cache_key));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(cache_mgr)) {
        bool erased = false;
        uint64_t registered_hold = 0;
        const int64_t last_access_ts = OB_ISNULL(cache)
            ? 0 : cache->get_last_access_ts();
        if (OB_FAIL(vec_idx_mgr_->try_erase_sstable_cache_mgr(
                cache_key, cache_mgr, cache, last_access_ts,
                false /* failed_build */, erased, registered_hold))) {
          LOG_WARN("failed to erase invalid DiskBBQ cache", KR(ret), K(cache_key));
        } else if (!erased) {
          LOG_DEBUG("invalid DiskBBQ cache is still referenced", K(cache_key));
        }
      }
    }
  }
  return ret;
}

int ObDiskBBQCacheAsyncTask::build_one_hgraph_(
    ObIvfCacheMgr &cache_mgr,
    ObIvfSSTableCentCache &cache,
    bool &built)
{
  int ret = OB_SUCCESS;
  uint64_t reservation = 0;
  built = false;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ObVectorIndexParam hgraph_param = cache_mgr.get_vec_param();
  hgraph_param.nlist_ = cache.get_count();
  hgraph_param.dim_ = cache.get_cent_vec_dim();
  hgraph_param.type_ = VIAT_HGRAPH;
  hgraph_param.lib_ = VIAL_VSAG;
  hgraph_param.m_ = 16;
  hgraph_param.ef_construction_ = 200;
  hgraph_param.ef_search_ = 64;
  hgraph_param.extra_info_actual_size_ = 0;
  hgraph_param.refine_type_ = 0;
  hgraph_param.bq_bits_query_ = 32;
  hgraph_param.bq_use_fht_ = false;
  int64_t estimated = 0;
  if (OB_ISNULL(service) || OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service or manager is null for DiskBBQ HGraph build", KR(ret), KP(service), KP_(vec_idx_mgr));
  } else if (OB_FAIL(ObVectorIndexUtil::estimate_hgraph_memory_for_ivf_centers(
                 hgraph_param, estimated))) {
    LOG_WARN("failed to estimate DiskBBQ HGraph memory", KR(ret), K(hgraph_param));
  }

  reservation = estimated > 0 ? static_cast<uint64_t>(estimated) : 0;
  if (OB_FAIL(ret)) {
  } else if (0 == reservation) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DiskBBQ HGraph memory estimate", KR(ret), K(hgraph_param));
  } else {
    ObIvfSSTableCacheController &controller =
        service->get_ivf_sstable_cache_controller();
    if (!controller.try_reserve(reservation)) {
      reservation = 0;
      LOG_DEBUG("defer DiskBBQ HGraph build by cache admission",
                K(hgraph_param), K(controller));
    } else {
      if (OB_FAIL(cache.build_sstable_hgraph(hgraph_param))) {
        LOG_WARN("failed to build DiskBBQ centroid HGraph", KR(ret), K(hgraph_param));
      } else {
        // The dynamic threshold may have changed after task admission.
        built = cache.has_hgraph_index();
      }
      const uint64_t new_hold = cache.get_memory_hold();
      // The task guard keeps this cache alive even after map removal. Its graph
      // still consumes quota; a replacement cache has independent accounting.
      if (cache.update_registered_hold(new_hold, reservation)) {
        reservation = 0;
      } else {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "[DISK_BBQ] graph cache lost memory accounting",
            K(new_hold), K(reservation));
        if (OB_SUCC(ret)) { ret = OB_ERR_UNEXPECTED; }
      }
    }
    if (reservation > 0) {
      controller.release_reservation(reservation);
      reservation = 0;
    }
  }
  return ret;
}

int ObDiskBBQCacheAsyncTask::process_one_hgraph_(
    const ObIvfCacheMgrKey &cache_key)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  bool erase_pending = false;
  bool can_build = false;
  storage::ObLS *ls = OB_ISNULL(ctx_) ? nullptr : ctx_->get_ls();
  ObIvfCacheMgrGuard cache_mgr_guard;
  ObIvfCacheMgr *cache_mgr = nullptr;
  ObIvfSSTableCentCache *cache = nullptr;
  if (OB_ISNULL(vec_idx_mgr_) || OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DiskBBQ HGraph task state", KR(ret), K(cache_key));
  } else if (!vec_idx_mgr_->has_ivf_sstable_hgraph_pending(cache_key)) {
  } else if (OB_FAIL(ObDiskBBQCacheTaskUtil::check_sstable_exists(
                 *ls, cache_key, exists))) {
    LOG_WARN("failed to validate DiskBBQ HGraph cache", KR(ret), K(cache_key));
  } else if (!exists) {
    erase_pending = true;
  } else {
    const int tmp_ret = vec_idx_mgr_->get_ivf_cache_mgr_guard(
        cache_key, cache_mgr_guard);
    if (OB_HASH_NOT_EXIST == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
      // Remove an orphan only while the map still has no manager for this key.
      erase_pending = true;
    } else if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to acquire DiskBBQ HGraph cache manager",
               KR(ret), K(cache_key));
    } else if (OB_ISNULL(cache_mgr = cache_mgr_guard.get_ivf_cache_mgr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null DiskBBQ HGraph cache manager", KR(ret), K(cache_key));
    } else {
      const int cache_ret = cache_mgr->get_cache_node<ObIvfSSTableCentCache>(
          IVF_SSTABLE_CENTROID_CACHE, cache);
      if (OB_HASH_NOT_EXIST == cache_ret || OB_ENTRY_NOT_EXIST == cache_ret) {
        // Cache-node creation may be in progress. Its publisher will reuse the
        // idempotent pending key after the ARRAY cache becomes queryable.
      } else if (OB_SUCCESS != cache_ret) {
        ret = cache_ret;
        LOG_WARN("failed to get DiskBBQ HGraph cache", KR(ret), K(cache_key));
      } else if (OB_ISNULL(cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("DiskBBQ HGraph cache is null", KR(ret), K(cache_key));
      } else {
        common::RWLock::RLockGuard cache_guard(cache->get_lock());
        if (!cache->is_completed() || !cache->is_memory_registered()) {
          // Do not let an older task erase the pending marker of a replacement
          // cache generation that is still being published.
        } else if (cache->has_hgraph_index() || !cache->can_build_hgraph()) {
          erase_pending = true;
        } else if (OB_ISNULL(cache->get_centroids())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("DiskBBQ ARRAY cache is null before HGraph build",
                   KR(ret), K(cache_key));
        } else {
          can_build = true;
        }
      }
    }
  }

  if (OB_SUCC(ret) && can_build && cache->try_start_hgraph_build()) {
    bool built = false;
    const int64_t started_at = ObTimeUtility::current_time();
    ret = build_one_hgraph_(*cache_mgr, *cache, built);
    cache->finish_hgraph_build();
    if (OB_SUCC(ret) && built) {
      const int64_t elapsed_us = ObTimeUtility::current_time() - started_at;
      LOG_INFO("DiskBBQ centroid HGraph cache built", K(cache_key), K(elapsed_us),
          "centroid_count", cache->get_count(), "registered_hold", cache->get_registered_hold());
    }
    erase_pending = built;
  }
  if (OB_SUCC(ret) && erase_pending
      && OB_FAIL(vec_idx_mgr_->try_erase_ivf_sstable_hgraph_pending(cache_key, cache_mgr, cache))) {
    LOG_WARN("failed to erase DiskBBQ HGraph pending key", KR(ret), K(cache_key));
  }
  return ret;
}

int ObDiskBBQCacheAsyncTask::build_pending_hgraphs_()
{
  int ret = OB_SUCCESS;
  int first_ret = OB_SUCCESS;
  ObSEArray<ObLSCacheMgrKeyPair, 64> cache_keys;
  ObLSID ls_id = ls_id_;
  if (OB_ISNULL(vec_idx_mgr_) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DiskBBQ HGraph task state", KR(ret), KP_(vec_idx_mgr), KP_(ctx));
  } else if (OB_FAIL(vec_idx_mgr_->get_cache_keys(ls_id, cache_keys))) {
    LOG_WARN("failed to snapshot DiskBBQ HGraph cache keys", KR(ret), K(ls_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cache_keys.count(); ++i) {
    const ObIvfCacheMgrKey &cache_key = cache_keys.at(i).key_;
    if (!cache_key.is_sstable_key() || cache_key.get_group_id() != 0
        || cache_key.get_tablet_id() != ctx_->task_status_.tablet_id_) {
      continue;
    }
    bool is_cancel = false;
    if (OB_FAIL(ObVecIndexAsyncTaskUtil::check_task_is_cancel(ctx_, is_cancel))) {
      LOG_WARN("failed to check DiskBBQ HGraph task cancellation", KR(ret));
    } else if (is_cancel) {
      ret = OB_CANCELED;
    } else {
      const int tmp_ret = process_one_hgraph_(cache_key);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to process DiskBBQ HGraph pending key",
                 K(tmp_ret), K(cache_key));
        if (OB_SUCCESS == first_ret) {
          first_ret = tmp_ret;
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_SUCCESS != first_ret) {
    ret = first_ret;
  }
  return ret;
}

int ObDiskBBQCacheAsyncTask::do_work()
{
  int ret = OB_SUCCESS;
  const ObFunctionType function_type =
      task_type_ == OB_VECTOR_ASYNC_DISK_BBQ_CACHE_CLEAN
          ? ObFunctionType::PRIO_VECTOR_HIGH
          : ObFunctionType::PRIO_VECTOR_LOW;
  CONSUMER_GROUP_FUNC_GUARD(function_type);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("DiskBBQ cache task is not initialized", KR(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->get_ls())
             || OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DiskBBQ cache task state",
             KR(ret), KP_(ctx), KP_(vec_idx_mgr));
  } else {
    ObTraceIdGuard trace_guard(ctx_->task_status_.trace_id_);
    if (task_type_ == OB_VECTOR_ASYNC_DISK_BBQ_CACHE_CLEAN) {
      if (OB_FAIL(clean_invalid_caches_())) {
        LOG_WARN("failed to clean invalid DiskBBQ caches", KR(ret));
      }
    } else if (task_type_ == OB_VECTOR_ASYNC_DISK_BBQ_HGRAPH_BUILD) {
      if (OB_FAIL(build_pending_hgraphs_())) {
        LOG_WARN("failed to build pending DiskBBQ HGraphs", KR(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DiskBBQ cache task type", KR(ret), K_(task_type));
    }
  }
  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
