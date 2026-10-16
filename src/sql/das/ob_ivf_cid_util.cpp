/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_ivf_cid_util.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/spfresh/ob_spfresh_centroid_counter.h"
#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "share/vector_index/ob_vector_index_ivf_cache_util.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObIVFCidUtil::find_nearest_center_from_persistent_rows(
    const float *vector,
    const int64_t vector_dim,
    const ObIArray<share::ObSpfreshCentroidRow> &rows,
    const ObVectorIndexDistAlgorithm distance_algorithm,
    ObIAllocator &allocator,
    int64_t &center_id)
{
  int ret = OB_SUCCESS;
  center_id = 0;
  int64_t centroid_dim = 0;
  if (OB_ISNULL(vector) || vector_dim <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector for persistent centroid assignment",
             K(ret), KP(vector), K(vector_dim));
  } else if (rows.empty()) {
    // Before the first SPFresh split, no persisted centroid row exists. An
    // empty visible centroid set therefore maps to bootstrap center 0 so
    // initial writes can reach the split threshold. Publication consistency
    // between centroid generations is provided by the SPFresh state protocol.
    center_id = 0;
    LOG_INFO("persistent centroid scan uses SPFresh bootstrap center",
             K(center_id), K(vector_dim));
  } else {
    centroid_dim = rows.at(0).vector_len_ /
        static_cast<int64_t>(sizeof(float));
    const int64_t centroid_count = rows.count();
    const int64_t element_size = static_cast<int64_t>(sizeof(float));
    int64_t flat_size = 0;
    if (centroid_dim <= 0 || centroid_dim != vector_dim) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("persistent centroid dimension mismatches input vector",
               K(ret), K(centroid_dim), K(vector_dim));
    } else if (OB_UNLIKELY(centroid_count > INT64_MAX / centroid_dim / element_size)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("persistent centroid buffer size overflows int64",
               K(ret), K(centroid_count), K(centroid_dim), K(element_size));
    } else if (FALSE_IT(flat_size = centroid_count * centroid_dim * element_size)) {
    }
    float *centroids = OB_SUCC(ret)
        ? static_cast<float *>(allocator.alloc(flat_size)) : nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(centroids)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate persistent centroids failed", K(ret), K(flat_size));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < centroid_count; ++i) {
        if (OB_ISNULL(rows.at(i).vector_)
            || rows.at(i).vector_len_ != centroid_dim
                * static_cast<int64_t>(sizeof(float))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid persistent centroid row",
                   K(ret), K(i), K(rows.at(i).vector_len_), K(centroid_dim));
        } else {
          MEMCPY(centroids + i * centroid_dim, rows.at(i).vector_,
                 centroid_dim * sizeof(float));
        }
      }
      if (OB_SUCC(ret)) {
        ObVectorClusterHelper helper;
        ObVectorNormalizeInfo norm_info;
        int64_t local_idx = 0;
        if (OB_FAIL(helper.get_nearest_probe_centers(
                const_cast<float *>(vector), vector_dim,
                centroids, centroid_count, centroid_dim, 1 /*nprobe*/,
                allocator,
                VIDA_COS != distance_algorithm ? nullptr : &norm_info,
                0,
                -1,
                VIDA_L1 == distance_algorithm
                    ? ObExprVectorDistance::ObVecDisType::MANHATTAN
                    : ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED))) {
          LOG_WARN("find nearest persistent centroid failed", K(ret));
        } else if (OB_FAIL(helper.get_center_idx(0, local_idx))) {
          LOG_WARN("get nearest persistent centroid index failed", K(ret));
        } else {
          --local_idx;
          if (local_idx < 0 || local_idx >= centroid_count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("nearest persistent centroid index is invalid",
                     K(ret), K(local_idx), K(centroid_count));
          } else {
            center_id = rows.at(local_idx).center_id_;
          }
        }
      }
    }
  }
  return ret;
}

int ObIVFCidUtil::resolve_centroid_static(
    ObIArrayType *arr,
    const ObTableID table_id,
    const ObTabletID tablet_id,
    const ObVectorIndexDistAlgorithm dis_algo,
    common::ObArenaAllocator &allocator,
    ObEvalCtx &eval_ctx,
    int64_t &center_idx)
{
  int ret = OB_SUCCESS;
  UNUSED(eval_ctx);
  const int64_t ns_id = share::ObIvfCacheMgrKey::INVALID_NS_ID;
  const int64_t ltable_id = static_cast<int64_t>(share::ObIvfCacheMgrKey::INVALID_LTABLE_ID);
  share::ObIvfCacheMgrGuard cache_guard;
  share::ObIvfICache *cent_cache = nullptr;
  bool is_cache_usable = false;
  float *centers_data = nullptr;
  int64_t centers_count = 0;
  int64_t centers_dim = 0;

  if (OB_FAIL(ObVectorIndexUtil::get_ivf_centers_cache(
      false /*is_vectorized*/, false /*is_pq_centers*/,
      cache_guard, cent_cache, table_id, tablet_id, ns_id,
      ltable_id, is_cache_usable))) {
    LOG_WARN("failed to get centers cache", K(ret), K(table_id), K(tablet_id));
  } else if (is_cache_usable && cent_cache->has_hgraph_index()) {
    float *vector_data = reinterpret_cast<float*>(arr->get_data());
    if (OB_FAIL(ObVectorIndexUtil::get_nearest_center_with_hgraph(vector_data, cent_cache, center_idx))) {
      LOG_WARN("failed to get nearest center with hgraph", K(ret));
    } else {
      LOG_DEBUG("HGraph search succeeded", K(center_idx), K(tablet_id));
    }
  } else {
    if (is_cache_usable) {
      if (OB_ISNULL(cent_cache)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("cent cache is null", K(ret));
      } else {
        centers_data = cent_cache->get_centroids();
        if (OB_ISNULL(centers_data) && OB_FAIL(cent_cache->get_centroids(&allocator, &centers_data))) {
          LOG_WARN("fail to get centroids from cache", K(ret));
        } else if (OB_ISNULL(centers_data)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("centroids is null", K(ret));
        } else {
          centers_count = cent_cache->get_count();
          centers_dim = cent_cache->get_cent_vec_dim();
        }
      }
    } else {
      ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
      if (OB_ISNULL(service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("service is nullptr", K(ret));
      } else {
        centers_dim = arr->size();
        if (OB_FAIL(service->get_ivf_aux_info(table_id, tablet_id, centers_dim, allocator, centers_data, centers_count))) {
          LOG_WARN("failed to get centers by scanning table", K(ret), K(table_id), K(tablet_id), K(centers_dim));
        }
      }
    }
    if (OB_SUCC(ret) && centers_data != nullptr) {
      ObVectorClusterHelper helper;
      ObVectorNormalizeInfo norm_info;
      if (OB_FAIL(helper.get_nearest_probe_centers(
          reinterpret_cast<float*>(arr->get_data()),
          arr->size(),
          centers_data,
          centers_count,
          centers_dim,
          1/*nprobe*/,
          allocator,
          VIDA_COS != dis_algo ? nullptr: &norm_info,
          0,
          -1,
          VIDA_L1 == dis_algo ? ObExprVectorDistance::ObVecDisType::MANHATTAN
                              : ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED))) {
        LOG_WARN("failed to get nearest center", K(ret));
      } else if (OB_FAIL(helper.get_center_idx(0, center_idx))) {
        LOG_WARN("failed to get center idx", K(ret));
      }
    }
  }
  return ret;
}

int ObIVFCidUtil::resolve_centroid_spfresh(
    ObIArrayType *arr,
    const ObTableID table_id,
    const ObTabletID tablet_id,
    const uint64_t index_table_id,
    const int64_t ns_id,
    const uint64_t ltable_id,
    const ObVectorIndexDistAlgorithm dis_algo,
    common::ObArenaAllocator &allocator,
    ObEvalCtx &eval_ctx,
    int64_t &center_idx)
{
  int ret = OB_SUCCESS;
  UNUSED(index_table_id);
  bool skip_spfresh = false;
  uint64_t data_version = 0;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (data_version < DATA_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("resolve_centroid_spfresh is not supported in data version less than 4.6.1.0",
             KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "SPFresh centroid resolve in data version less than 4.6.1.0");
  } else if (OB_NOT_NULL(eval_ctx.exec_ctx_.get_sql_ctx()) &&
      OB_NOT_NULL(eval_ctx.exec_ctx_.get_sql_ctx()->session_info_)) {
    skip_spfresh = eval_ctx.exec_ctx_.get_sql_ctx()->session_info_->get_ddl_info().is_ddl()
                   || eval_ctx.exec_ctx_.get_sql_ctx()->session_info_->is_inner();
  }

  if (OB_FAIL(ret)) {
  } else if (skip_spfresh) {
    LOG_INFO("[SPFRESH_DML] Skip SPFresh logic for ddl or inner sql", K(table_id), K(tablet_id));
  } else {
    ObIvfCacheMgrGuard cache_guard;
    ObIvfICache *cent_cache = nullptr;
    ObIvfMapCentCache *map_cache = nullptr;
    bool is_cache_usable = false;
    float *centers_data = nullptr;
    int64_t centers_count = 0;
    int64_t centers_dim = 0;

    if (OB_FAIL(ObVectorIndexUtil::get_ivf_centers_cache(
        false /*is_vectorized*/, false /*is_pq_centers*/,
        cache_guard, cent_cache, table_id, tablet_id, ns_id,
        static_cast<int64_t>(ltable_id), is_cache_usable))) {
      LOG_WARN("failed to get centers cache", K(ret));
    } else if (!is_cache_usable || OB_ISNULL(cent_cache)) {
      // Cache not usable (not prewarmed yet, GC'd, etc.). Fall back to scan
      // KV table for centroids of (ns_id, ltable_id) so we don't blindly
      // assign center_idx=0.
      common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
      if (OB_ISNULL(sql_proxy)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("kv fallback: sql_proxy is null", K(ret));
      } else {
        share::ObSpfreshKvTableResolver resolver;
        share::ObSpfreshKvTableInfo kv_info;
        if (OB_FAIL(resolver.resolve(MTL_ID(), table_id, kv_info))) {
          LOG_WARN("kv fallback: failed to resolve kv table info",
                   K(ret), K(table_id));
        } else {
          // resolver defaults namespace_id_/logical_table_id_ to tenant_id /
          // data_table_id; override with SPFresh ns_id / ltable_id so that
          // the kv_key prefix filter matches.
          kv_info.namespace_id_ = static_cast<uint64_t>(ns_id);
          kv_info.logical_table_id_ = ltable_id;
          share::ObSpfreshTableHelper helper;
          if (OB_FAIL(helper.init(MTL_ID(), sql_proxy, kv_info))) {
            LOG_WARN("kv fallback: init helper failed", K(ret),
                     K(kv_info));
          } else {
            ObSEArray<share::ObSpfreshCentroidRow, 64> rows;
            rows.set_attr(ObMemAttr(MTL_ID(), "VecIvfKvFB"));
            if (OB_FAIL(helper.scan_centroids_by_ltable(
                    static_cast<uint64_t>(ns_id), ltable_id, allocator, rows))) {
              LOG_WARN("kv fallback: scan centroids failed", K(ret),
                       K(ns_id), K(ltable_id));
            } else {
              if (OB_FAIL(find_nearest_center_from_persistent_rows(
                      reinterpret_cast<float *>(arr->get_data()), arr->size(),
                      rows, dis_algo, allocator, center_idx))) {
                LOG_WARN("kv fallback: choose nearest persisted centroid failed",
                         K(ret), K(tablet_id), K(ns_id), K(ltable_id));
              } else if (!rows.empty()) {
                LOG_INFO("kv fallback: chose persisted centroid",
                         K(center_idx), K(tablet_id), K(ns_id), K(ltable_id),
                         "centroid_count", rows.count());
                ObPluginVectorIndexService *service =
                    MTL(ObPluginVectorIndexService*);
                if (OB_NOT_NULL(service)) {
                  int64_t new_count = 0;
                  int inc_ret = service->get_centroid_counter().inc_center_count(
                      center_idx, 1, new_count);
                  if (OB_SUCCESS != inc_ret) {
                    LOG_WARN("inc center count failed", K(inc_ret), K(center_idx));
                  }
                }
              } else {
                // Match the legacy expression: an empty persistent centroid
                // set uses center 0 without incrementing an absent in-memory
                // centroid. PREWARM later rebuilds counts from persisted rows.
                LOG_INFO("kv fallback: no centroid in kv, use bootstrap center without counter",
                         K(center_idx), K(tablet_id), K(ns_id), K(ltable_id));
              }
            }
          } // end resolve
        }
      }
    } else {
      centers_data = cent_cache->get_centroids();
      if (OB_NOT_NULL(centers_data)) {
        centers_count = cent_cache->get_count();
        centers_dim = cent_cache->get_cent_vec_dim();
        LOG_INFO("Found centroids from IVF cache", K(centers_count), K(centers_dim), K(ns_id));
        ObVectorClusterHelper helper;
        ObVectorNormalizeInfo norm_info;
        if (OB_FAIL(helper.get_nearest_probe_centers(
            reinterpret_cast<float*>(arr->get_data()),
            arr->size(),
            centers_data,
            centers_count,
            centers_dim,
            1/*nprobe*/,
            allocator,
            VIDA_COS != dis_algo ? nullptr: &norm_info,
            0,
            -1,
            VIDA_L1 == dis_algo ? ObExprVectorDistance::ObVecDisType::MANHATTAN
                                : ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED))) {
          LOG_WARN("failed to get nearest center", K(ret));
        } else if (OB_FAIL(helper.get_center_idx(0, center_idx))) {
          LOG_WARN("failed to get center idx", K(ret));
        } else {
          center_idx = center_idx - 1;
          LOG_DEBUG("SPFresh mode: found nearest centroid (flat)",
                    K(center_idx), K(tablet_id), K(centers_count));
        }
      } else if (IvfCacheType::IVF_MAP_CENTROID_CACHE == cent_cache->get_cache_type()) {
        map_cache = static_cast<ObIvfMapCentCache *>(cent_cache);
        if (map_cache->get_count() <= 0) {
          // An empty map cache is valid during SPFresh bootstrap. Use center 0
          // and update its counter/posting length below, as the legacy path did.
          center_idx = 0;
          LOG_INFO("empty map centroid cache, assigned bootstrap center",
                   K(center_idx), K(tablet_id), K(ns_id));
        } else {
          // Use the latest visible centroid layout so a new vector is not
          // assigned to a stale CID generation during split/merge.
          // cid_vec_ is accessed only inside foreach_refactored's per-bucket read lock,
          // so no ref_cnt_ / ObCentroidVecBatchGuard needed here (see for_each_centroid_vec comment).
          share::ObMapCacheForEachFunc for_each_fn(map_cache, SCN::max_scn());
          if (OB_FAIL(share::find_nearest_center_from_map(
                  reinterpret_cast<float*>(arr->get_data()), arr->size(),
                  for_each_fn, dis_algo, allocator, center_idx))) {
            LOG_WARN("failed to find nearest center from map cache", K(ret));
          } else {
            LOG_DEBUG("SPFresh mode: found nearest centroid (map)",
                      K(center_idx), K(tablet_id), "count", map_cache->get_count());
          }
        }
      } else {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("centroids is null", K(ret));
      }

      if (OB_SUCC(ret)) {
        ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
        if (OB_NOT_NULL(service)) {
          int64_t new_count = 0;
          int inc_ret = service->get_centroid_counter().inc_center_count(
              center_idx, 1, new_count);
          if (OB_SUCCESS != inc_ret) {
            LOG_WARN("inc center count failed", K(inc_ret), K(center_idx));
          }
        }
        if (OB_NOT_NULL(map_cache)) {
          int tmp_ret = map_cache->incr_posting_list_length(center_idx, 1);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("incr posting list length failed", K(tmp_ret), K(center_idx));
          }
        }
      }
    }
  }
  return ret;
}

int ObIVFCidUtil::assign_spfresh_center_id(
    const ObDatum &vector_datum,
    const uint16_t vector_subschema_id,
    const ObTableID centroid_table_id,
    const ObTabletID &centroid_tablet_id,
    const int64_t namespace_id,
    const uint64_t logical_table_id,
    const ObVectorIndexDistAlgorithm distance_algorithm,
    ObEvalCtx &eval_ctx,
    ObCenterId &center_id)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("IVFLeaderCID", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObIArrayType *vector = nullptr;
  int64_t center_idx = 0;
  center_id.reset();
  if (OB_UNLIKELY(vector_datum.is_null()
      || share::OB_LOGICAL_TABLE_INVALID_VECTOR_SUBSCHEMA_ID == vector_subschema_id
      || OB_INVALID_ID == centroid_table_id
      || !centroid_tablet_id.is_valid()
      || share::OB_INVALID_NAMESPACE_ID == static_cast<uint64_t>(namespace_id)
      || share::OB_INVALID_LOGICAL_TABLE_ID == logical_table_id
      || VIDA_MAX <= distance_algorithm)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct IVF assignment arguments", K(ret), K(vector_subschema_id),
             K(centroid_table_id), K(centroid_tablet_id), K(namespace_id),
             K(logical_table_id), K(distance_algorithm));
  } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(
      vector_subschema_id, vector_datum, eval_ctx, allocator, vector))) {
    LOG_WARN("decode StoredRow vector Datum failed", K(ret), K(vector_subschema_id));
  } else if (OB_ISNULL(vector) || vector->size() <= 0 || OB_ISNULL(vector->get_data())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("decoded StoredRow vector is invalid", K(ret), KP(vector));
  } else if (OB_FAIL(resolve_centroid_spfresh(
      vector,
      centroid_table_id,
      centroid_tablet_id,
      centroid_table_id,
      namespace_id,
      logical_table_id,
      distance_algorithm,
      allocator,
      eval_ctx,
      center_idx))) {
    LOG_WARN("resolve leader SPFresh centroid failed", K(ret), K(centroid_table_id),
             K(centroid_tablet_id), K(namespace_id), K(logical_table_id));
  } else {
    center_id = ObCenterId(centroid_tablet_id.id(), center_idx);
    if (OB_UNLIKELY(!center_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader SPFresh assignment produced invalid center id",
               K(ret), K(center_id), K(center_idx), K(centroid_tablet_id));
    }
  }
  return ret;
}
}
}
