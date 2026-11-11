/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/iter/ob_das_vec_index_ivf_scan_iter.h"
#include <cfloat>
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_type/ob_vector_ip_distance.h"
#include "sql/das/ob_das_define.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/vector/type_traits.h"
#include "sql/das/iter/ob_das_profile_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

static int read_ivf_vec_string_at(ObExpr &vec_expr,
                                  ObEvalCtx &eval_ctx,
                                  ObIAllocator &allocator,
                                  const int64_t batch_idx,
                                  bool &is_null,
                                  ObString &vec_str)
{
  int ret = OB_SUCCESS;
  is_null = true;
  vec_str.reset();
  if (vec_expr.enable_rich_format() && is_valid_format(vec_expr.get_format(eval_ctx))) {
    ObIVector *vec = vec_expr.get_vector(eval_ctx);
    if (OB_ISNULL(vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ivf vec expr vector is null", K(ret), K(batch_idx));
    } else if (vec->is_null(batch_idx)) {
      is_null = true;
    } else {
      is_null = false;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              allocator, vec, vec_expr.datum_meta_, vec_str, batch_idx))) {
        LOG_WARN("failed to read ivf vec string from vector format", K(ret), K(batch_idx));
      }
    }
  } else {
    ObDatum &datum = vec_expr.locate_expr_datum(eval_ctx, batch_idx);
    if (datum.is_null()) {
      is_null = true;
    } else {
      is_null = false;
      vec_str = datum.get_string();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              &allocator, ObLongTextType, CS_TYPE_BINARY, vec_str))) {
        LOG_WARN("failed to read ivf vec string from datum", K(ret), K(batch_idx));
      }
    }
  }
  return ret;
}

// Logical-table centroid scan writes CID via project_logical_columns into ObIVector
// (rich format); read through the same path as vector columns.
static int read_ivf_cid_string_at(ObExpr &cid_expr,
                                  ObEvalCtx &eval_ctx,
                                  ObIAllocator &allocator,
                                  const int64_t batch_idx,
                                  bool &is_null,
                                  ObString &cid_str)
{
  return read_ivf_vec_string_at(cid_expr, eval_ctx, allocator,
                                batch_idx, is_null, cid_str);
}

static int read_ivf_pq_ids_string_at(ObExpr &pq_ids_expr,
                                     ObEvalCtx &eval_ctx,
                                     ObIAllocator &allocator,
                                     const int64_t batch_idx,
                                     bool &is_null,
                                     ObString &pq_ids_str)
{
  return read_ivf_vec_string_at(pq_ids_expr, eval_ctx, allocator,
                                batch_idx, is_null, pq_ids_str);
}

static int read_ivf_rowkey_at(ObExpr &rk_expr,
                              ObEvalCtx &eval_ctx,
                              const int64_t batch_idx,
                              ObRowkey &main_rowkey,
                              ObObj &rk_obj)
{
  int ret = OB_SUCCESS;
  if (rk_expr.enable_rich_format() && is_valid_format(rk_expr.get_format(eval_ctx))) {
    ObIVector *vec = rk_expr.get_vector(eval_ctx);
    if (OB_ISNULL(vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ivf rowkey expr vector is null", K(ret), K(batch_idx));
    } else if (vec->is_null(batch_idx)) {
      ret = OB_ERR_NULL_VALUE;
    } else {
      rk_obj.set_uint64(vec->get_uint64(batch_idx));
      main_rowkey.assign(&rk_obj, 1);
    }
  } else {
    ObDatum &datum = rk_expr.locate_expr_datum(eval_ctx, batch_idx);
    if (OB_FAIL(datum.to_obj(rk_obj, rk_expr.obj_meta_, rk_expr.obj_datum_map_))) {
      LOG_WARN("convert rowkey datum to obj failed", K(ret), K(batch_idx));
    } else {
      main_rowkey.assign(&rk_obj, 1);
    }
  }
  return ret;
}

static int read_ivf_pk_int64_at(ObExpr &pk_expr,
                                ObEvalCtx &eval_ctx,
                                const int64_t batch_idx,
                                int64_t &pk_val)
{
  int ret = OB_SUCCESS;
  if (pk_expr.enable_rich_format() && is_valid_format(pk_expr.get_format(eval_ctx))) {
    ObIVector *vec = pk_expr.get_vector(eval_ctx);
    if (OB_ISNULL(vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ivf pk expr vector is null", K(ret), K(batch_idx));
    } else if (vec->is_null(batch_idx)) {
      ret = OB_ERR_NULL_VALUE;
    } else {
      pk_val = static_cast<int64_t>(vec->get_uint64(batch_idx));
    }
  } else {
    pk_val = pk_expr.locate_expr_datum(eval_ctx, batch_idx).get_int();
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDASVecIndexIVFScanIterParam, ls_id_);

int ObDASVecIndexIVFBaseScanIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *profile = nullptr;
  int profile_ret = ObDASProfileIter::init_runtime_profile(
      common::ObProfileId::HYBRID_SEARCH_VEC_IVF_SCAN, profile);
  if (OB_SUCCESS != profile_ret) {
    profile_ = nullptr;
    LOG_WARN("failed to init runtime profile for ivf scan", KR(profile_ret));
  } else {
    profile_ = profile;
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::rescan()
{
  int ret = OB_SUCCESS;
  common::ObOpProfile<common::ObMetric> *profile = nullptr;
  int profile_ret = ObDASProfileIter::init_runtime_profile(
      common::ObProfileId::HYBRID_SEARCH_VEC_IVF_SCAN, profile);
  if (OB_SUCCESS != profile_ret) {
    profile_ = nullptr;
    LOG_WARN("failed to init runtime profile for ivf scan", KR(profile_ret));
  } else {
    profile_ = profile;
  }
  // Refresh initialized auxiliary scans when switching async-index partitions.
  if (is_async_vector_query()
      && OB_FAIL(assign_async_scan_param_snapshots())) {
    LOG_WARN("failed to assign async vector scan snapshots", K(ret));
  }
  return ret;
}

// ============================================================================
// ObDASVecIndexIVFBaseScanIter — common infrastructure
// ============================================================================

int ObDASVecIndexIVFBaseScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDASIterType::DAS_ITER_VEC_INDEX_IVF_SCAN != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid das iter param type for ivf scan iter", K(ret), K(param));
  } else {
    ObDASVecIndexIVFScanIterParam &ivf_param = static_cast<ObDASVecIndexIVFScanIterParam &>(param);

    // save params
    ls_id_ = ivf_param.ls_id_;
    tx_desc_ = ivf_param.tx_desc_;
    snapshot_ = ivf_param.snapshot_;
    centroid_iter_ = ivf_param.centroid_iter_;
    cid_vec_iter_ = ivf_param.cid_vec_iter_;
    rowkey_cid_iter_ = ivf_param.rowkey_cid_iter_;
    com_aux_iter_ = ivf_param.com_aux_iter_;
    special_aux_iter_ = ivf_param.special_aux_iter_;
    vec_index_scan_ctdef_ = ivf_param.vec_index_scan_ctdef_;
    vec_index_scan_rtdef_ = ivf_param.vec_index_scan_rtdef_;
    vec_index_type_ = ivf_param.vec_index_type_;
    sort_expr_ = ivf_param.sort_expr_;
    search_ctx_ = ivf_param.search_ctx_;

    // init memory context
    if (OB_FAIL(init_memory_context())) {
      LOG_WARN("failed to init memory context", K(ret));
    }

    // parse index params and extract search params
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parse_vec_index_param())) {
      LOG_WARN("failed to parse vec index param", K(ret));
    }

    // extract dim and distance type from sort expr
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(vec_index_scan_ctdef_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vec_index_scan_ctdef_ is null", K(ret));
    } else {
      dim_ = vec_index_scan_ctdef_->dim_;

      // get distance type from sort expr
      ObExpr *sort_distance_expr = vec_index_scan_ctdef_->sort_expr_;
      if (OB_NOT_NULL(sort_distance_expr) && OB_NOT_NULL(vec_index_scan_rtdef_) &&
          OB_NOT_NULL(vec_index_scan_rtdef_->eval_ctx_)) {
        if (OB_FAIL(ObDasVecScanUtils::get_distance_expr_type(*sort_distance_expr,
                                                               *vec_index_scan_rtdef_->eval_ctx_,
                                                               dis_type_))) {
          LOG_WARN("failed to get distance type", K(ret));
        } else if (dis_type_ == ObExprVectorDistance::ObVecDisType::COSINE) {
          need_norm_ = true;
          dis_type_ = ObExprVectorDistance::ObVecDisType::DOT;
        }
      }

      // get nprobes: stmt-level > session-level > default
      if (OB_FAIL(ret)) {
      } else if (vec_index_scan_ctdef_->query_param_.is_set_ivf_nprobes_
          && 0 == vec_index_scan_ctdef_->query_param_.ivf_nprobes_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("automatic nprobes is only supported for DiskBBQ", K(ret));
      } else if (vec_index_scan_ctdef_->query_param_.is_set_ivf_nprobes_) {
        nprobes_ = vec_index_scan_ctdef_->query_param_.ivf_nprobes_;
      } else if (OB_NOT_NULL(vec_index_scan_rtdef_) && OB_NOT_NULL(vec_index_scan_rtdef_->eval_ctx_)) {
        ObSQLSessionInfo *session = vec_index_scan_rtdef_->eval_ctx_->exec_ctx_.get_my_session();
        uint64_t ob_ivf_nprobes = 0;
        if (OB_ISNULL(session)) {
          // use default
        } else if (OB_FAIL(session->get_ob_ivf_nprobes(ob_ivf_nprobes))) {
          LOG_WARN("failed to get ob_ivf_nprobes from session", K(ret));
        } else {
          nprobes_ = ob_ivf_nprobes;
        }
      }

      // similarity threshold and distance threshold
      if (OB_SUCC(ret)) {
        similarity_threshold_ = vec_index_param_.similarity_threshold_;
        if (similarity_threshold_ > 0) {
          share::get_distance_threshold(dis_type_, similarity_threshold_, distance_threshold_);
        }
      }

      // max scan vectors from tenant config
      if (OB_SUCC(ret)) {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
        max_scan_vectors_ = tenant_config->_ivf_max_scan_vectors;
      }
    }

    // init first scan flags
    centroid_iter_first_scan_ = true;
    cid_vec_iter_first_scan_ = true;
    com_aux_iter_first_scan_ = true;
    special_aux_iter_first_scan_ = true;

    // Validate cid_vec aux layout: classical path uses CID + VECTOR + hidden int64 vid;
    // logical Agent DB path rewrites scans to KV; rowid/vector are projected via logical_output_exprs_.
    if (OB_SUCC(ret)) {
      const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
      if (OB_NOT_NULL(cid_vec_ctdef)) {
        if (cid_vec_ctdef->is_logical_table()
            && share::LT_AUX_IVF_CID_ROWID == cid_vec_ctdef->logical_table_aux_type_) {
          is_single_int64_rowkey_ = true;
        } else {
          int64_t cid_vec_column_count = cid_vec_ctdef->access_column_ids_.count();
          int64_t rowkey_cnt =
              cid_vec_column_count - CID_VEC_COM_KEY_CNT - CID_VEC_FIXED_PRI_KEY_CNT;
          // cid_vec layout: [CID(0), VEC(1), ROWKEY(2), ...], rowkey starts at index 2
          if (rowkey_cnt == 1
              && cid_vec_ctdef->access_column_ids_.at(CID_VEC_ROWKEY_START_IDX)
                     == OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
            is_single_int64_rowkey_ = true;
          }
          if (!is_single_int64_rowkey_) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("hybrid search IVF only supports single int64 rowkey as vid",
                     K(ret), K(rowkey_cnt));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_ns_ltable_for_cache_())) {
      LOG_WARN("failed to resolve ns_id and ltable_id for cache", K(ret));
    }
  }

  return ret;
}

/**
 * Update every initialized IVF auxiliary scan parameter to the current
 * partition's fixed async read snapshot. Uninitialized scans set it at init.
 */
int ObDASVecIndexIVFBaseScanIter::assign_async_scan_param_snapshots()
{
  int ret = OB_SUCCESS;
  share::SCN read_scn;
  if (OB_FAIL(get_async_vector_read_snapshot_scn(read_scn))) {
    LOG_WARN("failed to get async vector read snapshot", K(ret));
  } else if (!centroid_iter_first_scan_
             && OB_FAIL(assign_vector_read_snapshot(
                 snapshot_, read_scn, centroid_scan_param_.snapshot_))) {
    LOG_WARN("failed to assign centroid snapshot", K(ret));
  } else if (!cid_vec_iter_first_scan_
             && OB_FAIL(assign_vector_read_snapshot(
                 snapshot_, read_scn, cid_vec_scan_param_.snapshot_))) {
    LOG_WARN("failed to assign cid vector snapshot", K(ret));
  } else if (!com_aux_iter_first_scan_
             && OB_FAIL(assign_vector_read_snapshot(
                 snapshot_, read_scn, com_aux_scan_param_.snapshot_))) {
    LOG_WARN("failed to assign common aux snapshot", K(ret));
  } else if (!special_aux_iter_first_scan_
             && OB_FAIL(assign_vector_read_snapshot(
                 snapshot_, read_scn, special_aux_scan_param_.snapshot_))) {
    LOG_WARN("failed to assign special aux snapshot", K(ret));
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::resolve_ns_ltable_for_cache_()
{
  int ret = OB_SUCCESS;
  ns_id_ = share::ObIvfCacheMgrKey::INVALID_NS_ID;
  ltable_id_ = share::ObIvfCacheMgrKey::INVALID_LTABLE_ID;
  const ObDASScanCtDef *centroid_ctdef = OB_NOT_NULL(vec_index_scan_ctdef_)
      ? vec_index_scan_ctdef_->get_centroid_table_ctdef() : nullptr;

  if (OB_NOT_NULL(search_ctx_)) {
    const ObLogicalTableSearchCtx &lt_ctx = search_ctx_->get_logical_table_ctx();
    if (lt_ctx.is_enabled_) {
      ns_id_ = static_cast<int64_t>(lt_ctx.namespace_id_);
      ltable_id_ = lt_ctx.logical_table_id_;
    }
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::init_memory_context()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), "IvfVecScan", ObCtxIds::DEFAULT_CTX_ID);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("failed to create ivf vec scan memory context", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::parse_vec_index_param()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vec_index_scan_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vec_index_scan_ctdef_ is null", K(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
                 vec_index_scan_ctdef_->vec_index_param_, ObVectorIndexType::VIT_IVF_INDEX, vec_index_param_))) {
    LOG_WARN("fail to parse params from string", K(ret), K(vec_index_scan_ctdef_->vec_index_param_));
  } else {
    vec_index_param_.dim_ = vec_index_scan_ctdef_->dim_;
  }

  return ret;
}

// ======================== calc_vec_dis ========================

int ObDASVecIndexIVFBaseScanIter::calc_vec_dis(float *a, float *b, int dim, float &dis, ObExprVectorDistance::ObVecDisType dis_type)
{
  int ret = OB_SUCCESS;
  if (dis_type != ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    double distance = 0;
    if (OB_FAIL(ObExprVectorDistance::DisFunc<float>::distance_funcs[dis_type](a, b, dim, distance))) {
      LOG_WARN("failed to calculate distance", K(ret), K(dis_type), KP(a), KP(b), K(dim));
    } else {
      dis = distance;
    }
  } else {
    dis = ObVectorL2Distance<float>::l2_square_flt_func(a, b, dim);
  }
  return ret;
}

// ======================== set_related_tablet_ids ========================

void ObDASVecIndexIVFBaseScanIter::set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids)
{
  centroid_tablet_id_ = related_tablet_ids.centroid_tablet_id_;
  cid_vec_tablet_id_ = related_tablet_ids.cid_vec_tablet_id_;
  rowkey_cid_tablet_id_ = related_tablet_ids.rowkey_cid_tablet_id_;
  com_aux_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  special_aux_tablet_id_ = related_tablet_ids.special_aux_tablet_id_;
}

void ObDASVecIndexIVFBaseScanIter::fill_related_tablet_ids(
    ObDASRelatedTabletID &related_tablet_ids) const
{
  related_tablet_ids.centroid_tablet_id_ = centroid_tablet_id_;
  related_tablet_ids.cid_vec_tablet_id_ = cid_vec_tablet_id_;
  related_tablet_ids.rowkey_cid_tablet_id_ = rowkey_cid_tablet_id_;
  related_tablet_ids.lookup_tablet_id_ = com_aux_tablet_id_;
  related_tablet_ids.special_aux_tablet_id_ = special_aux_tablet_id_;
}

// ======================== prepare_search ========================

int ObDASVecIndexIVFBaseScanIter::prepare_search()
{
  int ret = OB_SUCCESS;
  if (!ivf_query_cond_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ivf query condition not inited", K(ret));
  } else if (search_prepared_) {
    // already prepared, skip
  } else {
    real_search_vec_ = ivf_query_cond_.query_vector_;

    // normalize search vector for COSINE distance
    if (OB_SUCC(ret) && need_norm_ && real_search_vec_.length() > 0) {
      if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(dim_,
                                                          reinterpret_cast<float *>(real_search_vec_.ptr()),
                                                          reinterpret_cast<float *>(real_search_vec_.ptr())))) {
        LOG_WARN("failed to normalize search vector", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      search_prepared_ = true;
    }
  }
  return ret;
}

// ======================== Center cache helpers ========================

int ObDASVecIndexIVFBaseScanIter::get_centers_cache(
    bool is_vectorized,
    bool is_pq_centers,
    share::ObIvfCacheMgrGuard &cache_guard,
    share::ObIvfICache *&cent_cache,
    bool &is_cache_usable)
{
  int ret = OB_SUCCESS;
  is_cache_usable = false;
  cent_cache = nullptr;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObIvfCacheMgr *cache_mgr = nullptr;
  const ObDASScanCtDef *centroid_ctdef = vec_index_scan_ctdef_->get_centroid_table_ctdef();

  if (OB_ISNULL(vec_index_service) || OB_ISNULL(centroid_ctdef)) {
    is_cache_usable = false;
    LOG_DEBUG("vec index service or centroid ctdef is null, skip cache", KP(vec_index_service), KP(centroid_ctdef));
  } else {
    share::ObIvfCacheMgrKey cache_key(centroid_tablet_id_,
        is_pq_centers ? share::ObIvfCacheMgrKey::INVALID_NS_ID : ns_id_,
        is_pq_centers ? share::ObIvfCacheMgrKey::INVALID_LTABLE_ID : ltable_id_);
    bool cache_ready = false;
    int64_t role_epoch = 0;
    int state_ret = vec_index_service->get_ls_ivf_cache_state(
        ls_id_, cache_ready, role_epoch);
    if (OB_SUCCESS != state_ret) {
      cache_guard.reset();
      LOG_WARN_RET(state_ret, "fail to get ivf cache state, fallback to persistent scan",
                   K(state_ret), K_(ls_id));
      return OB_SUCCESS;
    } else if (!cache_ready) {
      cache_guard.reset();
      LOG_DEBUG("ivf cache is unavailable before guard acquisition, fallback to persistent scan",
                K(state_ret), K_(ls_id), K(role_epoch));
      return OB_SUCCESS;
    } else if (cache_guard.is_valid() && cache_guard.get_role_epoch() != role_epoch) {
      cache_guard.reset();
    }
    if (!cache_guard.is_valid() && OB_FAIL(vec_index_service->acquire_ivf_cache_mgr_guard(
                                       ls_id_, cache_key, vec_index_param_, dim_,
                                       centroid_ctdef->ref_table_id_, cache_guard))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        is_cache_usable = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to acquire ivf cache mgr guard", K(ret), K_(ls_id), K_(centroid_tablet_id));
      }
    } else if (FALSE_IT(cache_guard.set_role_epoch(role_epoch))) {
    } else if (OB_SUCCESS != (state_ret = vec_index_service->get_ls_ivf_cache_state(
                   ls_id_, cache_ready, role_epoch))) {
      cache_guard.reset();
      is_cache_usable = false;
      LOG_WARN_RET(state_ret, "fail to recheck ivf cache state, fallback to persistent scan",
                   K(state_ret), K_(ls_id));
    } else if (!cache_ready || cache_guard.get_role_epoch() != role_epoch) {
      cache_guard.reset();
      is_cache_usable = false;
      LOG_DEBUG("ivf cache role generation changed during guard acquisition, fallback",
                K(state_ret), K_(ls_id), K(cache_ready), K(role_epoch));
    } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null cache mgr", K(ret));
    } else if (OB_FAIL(cache_mgr->get_or_create_cache_node<share::ObIvfICache>(
                   is_pq_centers
                       ? share::IvfCacheType::IVF_PQ_CENTROID_CACHE
                       : (is_ns_id_valid() ? share::IvfCacheType::IVF_MAP_CENTROID_CACHE
                                           : share::IvfCacheType::IVF_CENTROID_CACHE),
                   cent_cache))) {
      LOG_WARN("fail to get or create cache node", K(ret), K(is_pq_centers), K(ns_id_));
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        is_cache_usable = false;
        ret = OB_SUCCESS;
      }
    } else if (!cent_cache->is_completed()) {
      const bool is_logic_table = OB_NOT_NULL(centroid_ctdef) && centroid_ctdef->is_logical_table();
      if (is_ns_id_valid() || is_logic_table) {
        LOG_DEBUG("skip try_write_centroid_cache for logic table / namespace mode",
                  K(ns_id_), K(ltable_id_), K(is_logic_table));
        is_cache_usable = false;
        ret = OB_SUCCESS;
      } else if (cent_cache->set_writing_if_idle()) {
        const common::ObMetricId build_metric_id =
            is_pq_centers
                ? common::ObMetricId::VEC_IVF_PQ_CENTROID_CACHE_BUILD_TIME
                : common::ObMetricId::VEC_IVF_CENTROID_CACHE_BUILD_TIME;
        common::ScopedTimer build_timer(build_metric_id, profile_);
        int tmp_ret = is_pq_centers ?
            try_write_pq_centroid_cache(*cent_cache, is_vectorized) :
            try_write_centroid_cache(*cent_cache, is_vectorized);
        if (OB_TMP_FAIL(tmp_ret)) {
          LOG_WARN("fail to try write centroid cache", K(ret), K(is_vectorized), KPC(cent_cache), K(is_pq_centers));
        } else {
          is_cache_usable = cent_cache->is_completed();
        }
      } else {
        LOG_INFO("other threads already writing centroids cache, skip", K(ret));
      }
    } else {
      is_cache_usable = true;
    }
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::try_write_centroid_cache(
    share::ObIvfICache &cent_cache,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  RWLock::WLockGuard guard(cent_cache.get_lock());
  if (!cent_cache.is_writing()) {
    LOG_INFO("other threads already written centroids cache, skip", K(ret));
  } else {
    if (OB_FAIL(do_centroid_table_full_scan(is_vectorized))) {
      LOG_WARN("failed to do centroid table scan", K(ret));
    } else {
      bool use_add_centroid = is_ns_id_valid();
      if (OB_FAIL(write_centroids_from_scan(cent_cache, is_vectorized, use_add_centroid))) {
        LOG_WARN("failed to write centroids from scan", K(ret), K(use_add_centroid));
      }
    }

    if (OB_SUCC(ret)) {
      if (cent_cache.get_count() > 0) {
        if (cent_cache.can_build_hgraph()) {
          ObVectorIndexParam hgraph_param = vec_index_param_;
          if (hgraph_param.dim_ <= 0) {
            hgraph_param.dim_ = dim_;
          }
          if (dis_type_ == ObExprVectorDistance::ObVecDisType::DOT && need_norm_) {
            hgraph_param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_COS;
          } else if (dis_type_ == ObExprVectorDistance::ObVecDisType::DOT) {
            hgraph_param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_IP;
          } else if (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN ||
                     dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED) {
            hgraph_param.dist_algorithm_ = ObVectorIndexDistAlgorithm::VIDA_L2;
          }
          if (OB_FAIL(cent_cache.build_hgraph_and_release_centers(hgraph_param))) {
            LOG_WARN("failed to build hgraph when writing centroid cache", K(ret), K(centroid_tablet_id_));
            cent_cache.reuse();
          } else {
            cent_cache.set_completed();
            LOG_INFO("success to write centroid table cache with hgraph", K(centroid_tablet_id_));
          }
        } else {
          cent_cache.set_completed();
          LOG_DEBUG("success to write centroid table cache", K(centroid_tablet_id_));
        }
      } else {
        cent_cache.reuse();
        LOG_DEBUG("Empty centroid table, no need to set cache", K(centroid_tablet_id_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    cent_cache.reuse();
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::write_centroids_from_scan(
    share::ObIvfICache &cent_cache,
    bool is_vectorized,
    bool use_add_centroid)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObCenterId cent_id;
  SCN min_scn = SCN::min_scn();
  const ObDASScanCtDef *centroid_ctdef = vec_index_scan_ctdef_->get_centroid_table_ctdef();

  if (is_vectorized) {
    IVF_GET_NEXT_ROWS_BEGIN(centroid_iter_)
    if (OB_SUCC(ret)) {
      ObEvalCtx::BatchInfoScopeGuard guard(*vec_index_scan_rtdef_->eval_ctx_);
      guard.set_batch_size(scan_row_cnt);
      ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
      ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
      ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;

      for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
        guard.set_batch_idx(i);
        bool cid_is_null = true;
        bool vec_is_null = true;
        ObString cid;
        ObString cid_vec;
        if (OB_FAIL(read_ivf_cid_string_at(*cid_expr, *eval_ctx, tmp_allocator, i, cid_is_null, cid))) {
          LOG_WARN("failed to read cid", K(ret), K(i));
        } else if (cid_is_null || cid.empty()) {
          // skip null cid
        } else if (OB_FAIL(read_ivf_vec_string_at(*cid_vec_expr, *eval_ctx,
                tmp_allocator, i, vec_is_null, cid_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (vec_is_null) {
          // skip null vector
        } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
                       cent_id, cid, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
          LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
        } else if (use_add_centroid) {
          if (OB_FAIL(cent_cache.add_centroid(cent_id.center_id_,
                         reinterpret_cast<float*>(cid_vec.ptr()),
                         cid_vec.length() / sizeof(float), &min_scn))) {
            LOG_WARN("fail to add centroid", K(ret), K(cent_id));
          }
        } else {
          if (OB_FAIL(cent_cache.write_centroid(cent_id.center_id_,
                         reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(cent_id));
          }
        }
      }
    }
    IVF_GET_NEXT_ROWS_END(centroid_iter_, centroid_scan_param_, centroid_tablet_id_)
  } else {
    centroid_iter_->clear_evaluated_flag();
    ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
    ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
    ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;

    for (int i = 0; OB_SUCC(ret); ++i) {
      if (OB_FAIL(centroid_iter_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to scan centroid iter", K(ret));
        }
      } else {
        bool cid_is_null = true;
        bool vec_is_null = true;
        ObString cid;
        ObString cid_vec;
        if (OB_FAIL(read_ivf_cid_string_at(*cid_expr, *eval_ctx, tmp_allocator, 0, cid_is_null, cid))) {
          LOG_WARN("failed to read cid", K(ret), K(i));
        } else if (cid_is_null || cid.empty()) {
          // skip null cid
        } else if (OB_FAIL(read_ivf_vec_string_at(*cid_vec_expr, *eval_ctx,
                tmp_allocator, 0, vec_is_null, cid_vec))) {
          LOG_WARN("failed to get real data.", K(ret));
        } else if (vec_is_null) {
          // skip null vector
        } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
                       cent_id, cid, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
          LOG_WARN("fail to get center idx from string", K(ret), KPHEX(cid.ptr(), cid.length()));
        } else if (use_add_centroid) {
          if (OB_FAIL(cent_cache.add_centroid(cent_id.center_id_,
                         reinterpret_cast<float*>(cid_vec.ptr()),
                         cid_vec.length() / sizeof(float), &min_scn))) {
            LOG_WARN("fail to add centroid", K(ret), K(cent_id));
          }
        } else {
          if (OB_FAIL(cent_cache.write_centroid(cent_id.center_id_,
                         reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
            LOG_WARN("fail to write centroid", K(ret), K(cent_id));
          }
        }
      }
    }
    int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
    if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, centroid_iter_, centroid_scan_param_, centroid_tablet_id_))) {
      LOG_WARN("failed to reuse centroid iter.", K(ret));
    } else {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::gen_near_cid_heap_from_cache(
    share::ObIvfICache &cent_cache,
    share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap)
{
  int ret = OB_SUCCESS;
  ObCenterId center_id;
  center_id.tablet_id_ = centroid_tablet_id_.id();
  CenterSaveMode center_save_mode = CenterSaveMode::SHALLOW_COPY_CENTER_VEC;
  if (is_ns_id_valid()) {
    if (cent_cache.get_cache_type() != share::IvfCacheType::IVF_MAP_CENTROID_CACHE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cache type for map centroid path", K(ret),
               "actual_type", cent_cache.get_cache_type());
    } else {
      // ns_id path: ref_cnt_ via batch_guard_ protects cid_vec_ across the
      // RLock boundary; no outer RLockGuard needed (try_erase_centroid will
      // see ref_cnt_ != 0 and skip).
      share::ObIvfMapCentCache *map_cache = static_cast<share::ObIvfMapCentCache *>(&cent_cache);
      ObSEArray<share::ObCentroidInfo, 64> centroids;
      centroids.set_attr(ObMemAttr(MTL_ID(), "IvfAcqCent"));
      // acquire_visible_centroids internally release_all()s the previous round
      // and rebinds batch_guard_ to map_cache. Strict semantics: on error
      // centroids is empty and ref_cnt_ is fully rolled back.
      // Namespace-aware centroid versions are selected at the effective async snapshot.
      share::SCN read_scn;
      if (OB_FAIL(get_vector_read_snapshot_scn(snapshot_, read_scn))) {
        LOG_WARN("failed to get vector read snapshot", K(ret));
      } else if (OB_FAIL(map_cache->acquire_visible_centroids(
                     read_scn, batch_guard_, centroids))) {
        LOG_WARN("fail to acquire visible centroids", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < centroids.count(); ++i) {
        center_id.center_id_ = centroids.at(i).cid_;
        if (OB_FAIL(nearest_cid_heap.push_center(center_id, centroids.at(i).cid_vec_,
                                                  dim_, center_save_mode))) {
          LOG_WARN("failed to push center.", K(ret), K(center_id));
        }
      }
      // batch_guard_ continues to hold ref_cnt_ across this function boundary;
      // released at next entry or at inner_reuse / inner_release.
    }
  } else {
    // Non-ns_id path uses array-based ObIvfCentCache; share rwlock_ with the
    // writer paths (scan_and_write_ivf_cent_cache, add_centroid replace branch)
    // to avoid reading half-written cid_vec_ data.
    RWLock::RLockGuard guard(cent_cache.get_lock());
    uint64_t capacity = cent_cache.get_count();
    float *cid_vec = nullptr;
    for (uint64_t i = 1; i <= capacity && OB_SUCC(ret); ++i) {
      if (OB_FAIL(cent_cache.read_centroid(i, cid_vec))) {
        LOG_WARN("fail to read centroid", K(ret), K(i));
      } else if (FALSE_IT(center_id.center_id_ = i)) {
      } else if (OB_FAIL(nearest_cid_heap.push_center(center_id, cid_vec, dim_, center_save_mode))) {
        LOG_WARN("failed to push center.", K(ret));
      }
    }
  }
  return ret;
}

// ======================== Table scan helpers ========================

int ObDASVecIndexIVFBaseScanIter::do_centroid_table_full_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *centroid_ctdef = vec_index_scan_ctdef_->get_centroid_table_ctdef();
  ObDASScanRtDef *centroid_rtdef = vec_index_scan_rtdef_->get_centroid_table_rtdef();

  if (OB_ISNULL(centroid_ctdef) || OB_ISNULL(centroid_rtdef) || OB_ISNULL(centroid_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("centroid ctdef/rtdef/iter is null", K(ret));
  } else if (centroid_iter_first_scan_) {
    // Apply the async snapshot when the centroid scan parameter is first initialized.
    ObNewRange scan_range;
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, centroid_tablet_id_, centroid_ctdef, centroid_rtdef,
                                                    tx_desc_, snapshot_, centroid_scan_param_,
                                                    false /*is_get*/, &mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init centroid scan param", K(ret));
    } else if (is_async_vector_query()
               && OB_FAIL(apply_async_vector_read_snapshot(
                   snapshot_, centroid_scan_param_.snapshot_))) {
      LOG_WARN("failed to apply async vector read snapshot", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, centroid_ctdef->ref_table_id_))) {
    } else if (OB_FAIL(centroid_scan_param_.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FALSE_IT(centroid_iter_->set_scan_param(centroid_scan_param_))) {
    } else if (OB_FAIL(centroid_iter_->do_table_scan())) {
      LOG_WARN("failed to do centroid table scan", K(ret));
    } else {
      centroid_iter_first_scan_ = false;
    }
  } else {
    const ObTabletID &scan_tablet_id = centroid_scan_param_.tablet_id_;
    centroid_scan_param_.need_switch_param_ =
        centroid_scan_param_.need_switch_param_ || (scan_tablet_id.is_valid() && (centroid_tablet_id_ != scan_tablet_id));
    centroid_scan_param_.tablet_id_ = centroid_tablet_id_;
    centroid_scan_param_.ls_id_ = ls_id_;

    ObNewRange scan_range;
    if (OB_FAIL(centroid_iter_->reuse())) {
      LOG_WARN("failed to reuse centroid iter", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, centroid_ctdef->ref_table_id_))) {
    } else if (OB_FAIL(centroid_scan_param_.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FAIL(centroid_iter_->rescan())) {
      LOG_WARN("failed to rescan centroid iter", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("[LT hybrid DAS][IVF centroid] do_centroid_table_full_scan ok",
             "ir_scan_type", static_cast<int32_t>(centroid_ctdef->ir_scan_type_),
             K(centroid_ctdef->is_logical_table()),
             "logical_aux_type", static_cast<int32_t>(centroid_ctdef->logical_table_aux_type_),
             K(centroid_ctdef->physical_data_table_id_),
             K(centroid_ctdef->ref_table_id_),
             K(centroid_scan_param_.tablet_id_),
             "key_range_cnt", centroid_scan_param_.key_ranges_.count(),
             K(centroid_iter_first_scan_));
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::do_special_aux_table_full_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *special_aux_ctdef = vec_index_scan_ctdef_->get_special_aux_table_ctdef();
  ObDASScanRtDef *special_aux_rtdef = vec_index_scan_rtdef_->get_special_aux_table_rtdef();

  if (OB_ISNULL(special_aux_ctdef) || OB_ISNULL(special_aux_rtdef) || OB_ISNULL(special_aux_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("special aux ctdef/rtdef/iter is null", K(ret));
  } else if (special_aux_iter_first_scan_) {
    // Apply the async snapshot when the special auxiliary scan is initialized.
    ObNewRange scan_range;
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(ls_id_, special_aux_tablet_id_, special_aux_ctdef, special_aux_rtdef,
                                                    tx_desc_, snapshot_, special_aux_scan_param_,
                                                    false /*is_get*/, &mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init special aux scan param", K(ret));
    } else if (is_async_vector_query()
               && OB_FAIL(apply_async_vector_read_snapshot(
                   snapshot_, special_aux_scan_param_.snapshot_))) {
      LOG_WARN("failed to apply async vector read snapshot", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, special_aux_ctdef->ref_table_id_))) {
    } else if (OB_FAIL(special_aux_scan_param_.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FALSE_IT(special_aux_iter_->set_scan_param(special_aux_scan_param_))) {
    } else if (OB_FAIL(special_aux_iter_->do_table_scan())) {
      LOG_WARN("failed to do special aux table scan", K(ret));
    } else {
      special_aux_iter_first_scan_ = false;
    }
  } else {
    const ObTabletID &scan_tablet_id = special_aux_scan_param_.tablet_id_;
    special_aux_scan_param_.need_switch_param_ =
        special_aux_scan_param_.need_switch_param_ || (scan_tablet_id.is_valid() && (special_aux_tablet_id_ != scan_tablet_id));
    special_aux_scan_param_.tablet_id_ = special_aux_tablet_id_;
    special_aux_scan_param_.ls_id_ = ls_id_;

    ObNewRange scan_range;
    if (OB_FAIL(special_aux_iter_->reuse())) {
      LOG_WARN("failed to reuse special aux iter", K(ret));
    } else if (OB_FALSE_IT(ObDasVecScanUtils::set_whole_range(scan_range, special_aux_ctdef->ref_table_id_))) {
    } else if (OB_FAIL(special_aux_scan_param_.key_ranges_.push_back(scan_range))) {
      LOG_WARN("failed to append scan range", K(ret));
    } else if (OB_FAIL(special_aux_iter_->rescan())) {
      LOG_WARN("failed to rescan special aux iter", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::try_write_pq_centroid_cache(
    share::ObIvfICache &cent_cache, bool is_vectorized)
{
  // Default: FLAT iter has no PQ centroid; return SUCCESS as no-op so that
  // accidental calls (without is_pq_centers guard) don't pollute upper-layer
  // ret with NOT_SUPPORTED. PQ subclass overrides this to do the real write.
  UNUSED(cent_cache);
  UNUSED(is_vectorized);
  return OB_SUCCESS;
}

int ObDASVecIndexIVFBaseScanIter::do_aux_table_scan(bool &first_scan,
                                                     ObTableScanParam &scan_param,
                                                     const ObDASScanCtDef *ctdef,
                                                     ObDASScanRtDef *rtdef,
                                                     ObDASScanIter *iter,
                                                     ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (first_scan) {
    // Override only async-index queries; normal IVF scans retain the SQL snapshot.
    scan_param.need_switch_param_ = false;
    if (OB_FAIL(ObDasVecScanUtils::init_scan_param(
            ls_id_, tablet_id, ctdef, rtdef, tx_desc_, snapshot_, scan_param,
            false /*is_get*/, &mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init scan param", K(ret));
    } else if (is_async_vector_query()
               && OB_FAIL(apply_async_vector_read_snapshot(
                   snapshot_, scan_param.snapshot_))) {
      LOG_WARN("failed to apply async vector read snapshot", K(ret));
    } else if (OB_FALSE_IT(iter->set_scan_param(scan_param))) {
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("failed to do scan", K(ret));
    } else {
      first_scan = false;
    }
  } else {
    const ObTabletID &scan_tablet_id = scan_param.tablet_id_;
    scan_param.need_switch_param_ =
        scan_param.need_switch_param_ || (scan_tablet_id.is_valid() && (tablet_id != scan_tablet_id));
    scan_param.tablet_id_ = tablet_id;
    scan_param.ls_id_ = ls_id_;
    if (OB_FAIL(iter->rescan())) {
      LOG_WARN("fail to rescan scan iterator", K(ret));
    }
  }
  return ret;
}

// ======================== CID range query helpers ========================

int ObDASVecIndexIVFBaseScanIter::build_cid_vec_query_rowkey(const ObString &cid,
                                                              bool is_min,
                                                              int64_t rowkey_cnt,
                                                              ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(mem_context_->get_arena_allocator().alloc(sizeof(ObObj) * rowkey_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    obj_ptr[0].set_varbinary(cid);
    for (int64_t i = 1; i < rowkey_cnt; ++i) {
      if (is_min) {
        obj_ptr[i].set_min_value();
      } else {
        obj_ptr[i].set_max_value();
      }
    }
    rowkey.assign(obj_ptr, rowkey_cnt);
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::build_cid_vec_query_range(const ObString &cid,
                                                              int64_t rowkey_cnt,
                                                              ObNewRange &cid_pri_key_range)
{
  int ret = OB_SUCCESS;
  ObRowkey cid_rowkey_min;
  ObRowkey cid_rowkey_max;
  if (cid.empty()) {
    cid_pri_key_range.set_whole_range();
  } else {
    if (OB_FAIL(build_cid_vec_query_rowkey(cid, true, rowkey_cnt, cid_rowkey_min))) {
      LOG_WARN("failed to build cid vec query rowkey min", K(ret));
    } else if (OB_FAIL(build_cid_vec_query_rowkey(cid, false, rowkey_cnt, cid_rowkey_max))) {
      LOG_WARN("failed to build cid vec query rowkey max", K(ret));
    } else {
      cid_pri_key_range.start_key_ = cid_rowkey_min;
      cid_pri_key_range.end_key_ = cid_rowkey_max;
    }
  }
  return ret;
}

int ObDASVecIndexIVFBaseScanIter::prepare_cid_range(const ObDASScanCtDef *cid_vec_ctdef,
                                                      int64_t &cid_vec_column_count,
                                                      int64_t &cid_vec_pri_key_cnt,
                                                      int64_t &rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cid_vec_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef is null", K(ret));
  } else if (OB_FALSE_IT(cid_vec_column_count = cid_vec_ctdef->access_column_ids_.count())) {
  } else if (OB_UNLIKELY(cid_vec_column_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column cnt", K(ret));
  } else if (cid_vec_ctdef->is_logical_table()
             && (share::LT_AUX_IVF_CID_ROWID == cid_vec_ctdef->logical_table_aux_type_
                 || share::LT_AUX_IVF_ROWID_CID == cid_vec_ctdef->logical_table_aux_type_)) {
    // Logical table KV layout has 2 access columns (key, value).
    // build_cid_vec_query_rowkey needs 2 columns [varbinary(cid), MIN/MAX]
    // so the range can be normalized to [uint64(cid), uint64(rowid)], matching
    // the 2 uint64 cols expected by encode_rowkey_bound_.
    cid_vec_pri_key_cnt = 2;
    rowkey_cnt = 1;
  } else {
    cid_vec_pri_key_cnt = cid_vec_column_count - CID_VEC_COM_KEY_CNT;
    rowkey_cnt = cid_vec_column_count - CID_VEC_COM_KEY_CNT - CID_VEC_FIXED_PRI_KEY_CNT;
  }
  return ret;
}

// ======================== scan_cid_range ========================

int ObDASVecIndexIVFBaseScanIter::scan_cid_range(const ObString &cid,
                                                   int64_t cid_vec_pri_key_cnt,
                                                   const ObDASScanCtDef *cid_vec_ctdef,
                                                   ObDASScanRtDef *cid_vec_rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cid_vec_ctdef) || OB_ISNULL(cid_vec_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctdef or rtdef is null", K(ret));
  } else {
    cid_vec_scan_param_.key_ranges_.reset();
    ObNewRange cid_pri_key_range;
    if (OB_FAIL(build_cid_vec_query_range(cid, cid_vec_pri_key_cnt, cid_pri_key_range))) {
      LOG_WARN("failed to build cid vec query range", K(ret));
    } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_range(
                   cid_pri_key_range, cid_vec_scan_param_, cid_vec_ctdef->ref_table_id_))) {
      LOG_WARN("failed to set lookup range", K(ret));
    } else if (OB_FAIL(do_aux_table_scan(cid_vec_iter_first_scan_,
                                         cid_vec_scan_param_,
                                         cid_vec_ctdef,
                                         cid_vec_rtdef,
                                         cid_vec_iter_,
                                         cid_vec_tablet_id_))) {
      LOG_WARN("fail to scan cid vec table", K(ret));
    }
  }
  return ret;
}

// ======================== get_nprobe ========================

int64_t ObDASVecIndexIVFBaseScanIter::get_nprobe(int64_t enlargement_factor)
{
  int64_t nprobe = nprobes_;
  if (enlargement_factor > 1) {
    nprobe = nprobe * enlargement_factor;
  }
  return nprobe;
}

// ======================== distance_to_score ========================

int ObDASVecIndexIVFBaseScanIter::distance_to_score(ObExpr *sort_expr, double distance, double &score)
{
  // Normal IVF path: dis_type_ = DOT (converted from COSINE in inner_init), distance is raw dot product.
  // Convert to cosine_distance first, then call parent which expects cosine_distance for T_FUN_SYS_COSINE_DISTANCE.
  // Brute force path: dis_type_ already reset to COSINE, distance is cosine_distance, skip conversion.
  if (need_norm_ && dis_type_ == ObExprVectorDistance::ObVecDisType::DOT) {
    double cosine_distance = 1.0 - distance;
    return ObDASVecIndexScanIter::distance_to_score(sort_expr, cosine_distance, score);
  }
  return ObDASVecIndexScanIter::distance_to_score(sort_expr, distance, score);
}

// ======================== build_vid_iter_from_rowkey_heap ========================

int ObDASVecIndexIVFBaseScanIter::build_vid_iter_from_rowkey_heap(ObIvfRowkeyHeap &nearest_rowkey_heap)
{
  int ret = OB_SUCCESS;
  int64_t count = nearest_rowkey_heap.count();

  if (count == 0) {
    adaptor_vid_iter_ = nullptr;
  } else {
    // extract rowkeys + distances from heap (worst-first order)
    rowkey_pairs_.reuse();
    distances_.reuse();
    if (OB_FAIL(nearest_rowkey_heap.get_nearest_probe_centers_vec_dist(rowkey_pairs_, distances_))) {
      LOG_WARN("failed to get results from heap", K(ret));
    } else {
      void *iter_buff = vec_op_alloc_.alloc(sizeof(share::ObVectorQueryVidIterator));
      if (OB_ISNULL(iter_buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc vid iter", K(ret));
      } else {
        adaptor_vid_iter_ = new(iter_buff) share::ObVectorQueryVidIterator(
            0 /*extra_column_count*/, 0 /*extra_info_actual_size*/,
            ivf_query_cond_.rel_count_, ivf_query_cond_.rel_map_ptr_);

        if (OB_FAIL(adaptor_vid_iter_->init(rowkey_pairs_.count(), &vec_op_alloc_))) {
          LOG_WARN("failed to init vid iter", K(ret), K(rowkey_pairs_.count()));
        } else {
          // reverse to get ascending order (best first)
          for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_pairs_.count(); ++i) {
            const ObObj &vid_cell = rowkey_pairs_.at(i).first.get_obj_ptr()[0];
            int64_t vid = 0;
            if (vid_cell.is_uint64()) {
              vid = static_cast<int64_t>(vid_cell.get_uint64());
            } else {
              vid = vid_cell.get_int();
            }
            float distance = distances_.at(i);
            if (OB_FAIL(adaptor_vid_iter_->add_result(vid, distance, nullptr /*extra_info*/))) {
              LOG_WARN("failed to add result to vid iter", K(ret), K(vid), K(distance));
            }
          }
        }
      }
    }
  }
  return ret;
}

void ObDASVecIndexIVFBaseScanIter::reset_adaptor_vid_iter()
{
  if (OB_NOT_NULL(adaptor_vid_iter_)) {
    adaptor_vid_iter_->reset();
    adaptor_vid_iter_->~ObVectorQueryVidIterator();
    adaptor_vid_iter_ = nullptr;
  }
}

// ======================== Brute-force scan (shared by FLAT/PQ) ========================
// Scans COM_AUX table directly, computes exact distance for all candidates.
// VIDS bitmap → batch rowkey lookup; otherwise → full table scan.

int ObDASVecIndexIVFBaseScanIter::do_brute_force_scan(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  float *search_vec = nullptr;
  int64_t limit_k = 0;
  const ObDASScanCtDef *com_aux_ctdef = nullptr;
  ObDASScanRtDef *com_aux_rtdef = nullptr;

  if (OB_UNLIKELY(!is_vectorized)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("non-vectorized brute force scan is not supported", K(ret));
  } else {
    // For COSINE (need_norm_), use original search vector and COSINE distance type
    // so that cosine_distance_func normalizes both vectors internally.
    if (need_norm_) {
      dis_type_ = ObExprVectorDistance::ObVecDisType::COSINE;
      search_vec = reinterpret_cast<float *>(ivf_query_cond_.query_vector_.ptr());
    } else {
      search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
    }
    limit_k = ivf_query_cond_.query_limit_;
    com_aux_ctdef = vec_index_scan_ctdef_->get_com_aux_table_ctdef();
    com_aux_rtdef = vec_index_scan_rtdef_->get_com_aux_table_rtdef();

    if (OB_ISNULL(search_vec) || limit_k <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid search vec or limit", K(ret), KP(search_vec), K(limit_k));
    } else if (OB_ISNULL(com_aux_ctdef) || OB_ISNULL(com_aux_rtdef) || OB_ISNULL(com_aux_iter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("com_aux ctdef/rtdef/iter is null", K(ret), KP(com_aux_ctdef), KP(com_aux_rtdef), KP(com_aux_iter_));
    }
  }

  // Prepare distance type: use EUCLIDEAN_SQUARED for heap comparison (avoids sqrt)
  ObExprVectorDistance::ObVecDisType heap_dis_type =
      (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN)
      ? ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
  ObIvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec, heap_dis_type, dim_,
                                      limit_k, similarity_threshold_);

  // Unified scan: VIDS → batch rowkey lookup, otherwise → full table scan
  bool has_vids = OB_NOT_NULL(bitmap_) && bitmap_->type_ == ObVecIndexBitmap::VIDS;
  int64_t *vids = has_vids ? bitmap_->get_vids() : nullptr;
  int64_t vid_cnt = has_vids ? bitmap_->get_valid_cnt() : 0;
  int64_t batch_size = ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE;
  int64_t num_batches = has_vids ? ((vid_cnt + batch_size - 1) / batch_size) : 1;

  ObExpr *vec_expr = nullptr;
  ObExpr *rk_expr = nullptr;
  ObEvalCtx *eval_ctx = nullptr;
  if (OB_SUCC(ret)) {
    vec_expr = com_aux_ctdef->result_output_[0];
    rk_expr = com_aux_ctdef->result_output_.at(1);
    eval_ctx = vec_index_scan_rtdef_->eval_ctx_;
  }

  for (int64_t batch_idx = 0; OB_SUCC(ret) && batch_idx < num_batches; ++batch_idx) {
    com_aux_scan_param_.key_ranges_.reset();

    if (has_vids) {
      // Batch rowkey lookup for this batch
      int64_t pos = batch_idx * batch_size;
      int64_t batch_end = OB_MIN(pos + batch_size, vid_cnt);
      int64_t batch_cnt = batch_end - pos;
      // Allocate ObObj array on arena so rowkey pointers remain valid until scan completes
      ObObj *vid_objs = nullptr;
      void *buf = mem_context_->get_arena_allocator().alloc(sizeof(ObObj) * batch_cnt);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate vid obj array failed", K(ret), K(batch_cnt));
      } else {
        vid_objs = new (buf) ObObj[batch_cnt];
      }
      for (int64_t i = pos; OB_SUCC(ret) && i < batch_end; ++i) {
        vid_objs[i - pos].set_uint64(static_cast<uint64_t>(vids[i]));
        ObRowkey vid_rowkey(&vid_objs[i - pos], 1);
        ObNewRange rk_range;
        if (OB_FAIL(rk_range.build_range(com_aux_ctdef->ref_table_id_, vid_rowkey))) {
          LOG_WARN("fail to build key range", K(ret), K(vids[i]));
        } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_range(rk_range, com_aux_scan_param_, com_aux_ctdef->ref_table_id_))) {
          LOG_WARN("failed to append scan range", K(ret));
        }
      }
    } else {
      // Full table scan
      ObNewRange scan_range;
      ObDasVecScanUtils::set_whole_range(scan_range, com_aux_ctdef->ref_table_id_);
      if (OB_FAIL(com_aux_scan_param_.key_ranges_.push_back(scan_range))) {
        LOG_WARN("failed to append scan range", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_aux_table_scan(com_aux_iter_first_scan_, com_aux_scan_param_,
                                          com_aux_ctdef, com_aux_rtdef,
                                          com_aux_iter_, com_aux_tablet_id_))) {
      LOG_WARN("failed to do com_aux table scan", K(ret));
    } else {
      IVF_GET_NEXT_ROWS_BEGIN(com_aux_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
        guard.set_batch_size(scan_row_cnt);
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          bool vec_is_null = true;
          ObString c_vec;
          if (OB_FAIL(read_ivf_vec_string_at(*vec_expr, *eval_ctx,
                  mem_context_->get_arena_allocator(), i, vec_is_null, c_vec))) {
            LOG_WARN("failed to get real data", K(ret));
          } else if (vec_is_null) {
            // skip null vector
          } else {
            float distance = 0.0f;
            ObRowkey main_rowkey;
            ObObj rk_obj;
            if (OB_FAIL(calc_vec_dis(reinterpret_cast<float *>(c_vec.ptr()), search_vec, dim_, distance, dis_type_))) {
              if (OB_ERR_NULL_VALUE == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to calc vec dis", K(ret));
              }
            } else if (OB_FAIL(read_ivf_rowkey_at(*rk_expr, *eval_ctx, i, main_rowkey, rk_obj))) {
              LOG_WARN("failed to read rowkey", K(ret));
            } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance))) {
              LOG_WARN("failed to push center", K(ret));
            }
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(com_aux_iter_, com_aux_scan_param_, com_aux_tablet_id_)
    }
  }

  // Build result vid iter from heap
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_vid_iter_from_rowkey_heap(nearest_rowkey_heap))) {
      LOG_WARN("failed to build vid iter from rowkey heap", K(ret));
    } else {
      brute_force_done_ = true;
      LOG_TRACE("brute force scan completed",
                K(limit_k), "heap_count", nearest_rowkey_heap.count(),
                "has_bitmap", OB_NOT_NULL(bitmap_),
                "adaptor_vid_iter_total", OB_NOT_NULL(adaptor_vid_iter_) ? adaptor_vid_iter_->get_total() : -1);
    }
  }

  return ret;
}

// ======================== Base inner_reuse / inner_release ========================

int ObDASVecIndexIVFBaseScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;

  if (!centroid_iter_first_scan_ &&
      OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, centroid_iter_, centroid_scan_param_, centroid_tablet_id_))) {
    LOG_WARN("failed to reuse centroid iter", K(ret));
  } else if (!cid_vec_iter_first_scan_ &&
             OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_))) {
    LOG_WARN("failed to reuse cid vec iter", K(ret));
  } else if (!com_aux_iter_first_scan_ &&
             OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, com_aux_iter_, com_aux_scan_param_, com_aux_tablet_id_))) {
    LOG_WARN("failed to reuse com aux iter", K(ret));
  } else if (!special_aux_iter_first_scan_ &&
             OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, special_aux_iter_, special_aux_scan_param_, special_aux_tablet_id_))) {
    LOG_WARN("failed to reuse special aux iter", K(ret));
  }

  reset_adaptor_vid_iter();

  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
  }

  vec_op_alloc_.reset();
  rowkey_pairs_.reuse();
  distances_.reuse();
  iterative_filter_ctx_.reuse();
  // Release any centroid ref_cnt_ held from the previous round before resetting
  // the cache mgr guard (which may drop the last ref to the underlying cache).
  {
    int tmp_ret = batch_guard_.release_all();
    if (common::OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to release batch_guard_ in inner_reuse", K(tmp_ret));
    }
  }
  center_cache_guard_.reset();
  cid_vec_scan_rows_ = 0;

  // NOTE: do NOT reset centroid_iter_first_scan_ / cid_vec_iter_first_scan_ to true here.
  // After reuse_iter, result_ is still held (non-null). Resetting first_scan to true would
  // cause do_table_scan() on the next partition, which asserts result_ == nullptr.
  // Keeping first_scan = false lets the next partition go through the rescan() path,
  // which correctly handles need_switch_param_ for partition tablet switching.

  // The driver resets its PRE bitmap after reusing this scan. Drop the borrowed
  // pointer so a following ITER/POST partition does not reject every candidate
  // through the previous partition's empty bitmap. PRE binds a fresh bitmap.
  bitmap_ = nullptr;
  ivf_filter_mode_ = ObIvfFilterMode::IVF_FILTER_MODE_PRE_FILTER;
  post_filter_centers_inited_ = false;
  brute_force_done_ = false;
  need_fallback_brute_force_ = false;
  cumulative_added_cnt_ = 0;
  profile_ = nullptr;

  return ret;
}

int ObDASVecIndexIVFBaseScanIter::inner_release()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_NOT_NULL(centroid_iter_) && OB_FAIL(release_input_iter(centroid_iter_))) {
    LOG_WARN("failed to release centroid_iter_", K(ret));
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(cid_vec_iter_) && OB_FAIL(release_input_iter(cid_vec_iter_))) {
    LOG_WARN("failed to release cid_vec_iter_", K(ret));
    tmp_ret = (tmp_ret == OB_SUCCESS) ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(rowkey_cid_iter_) && OB_FAIL(release_input_iter(rowkey_cid_iter_))) {
    LOG_WARN("failed to release rowkey_cid_iter_", K(ret));
    tmp_ret = (tmp_ret == OB_SUCCESS) ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(com_aux_iter_) && OB_FAIL(release_input_iter(com_aux_iter_))) {
    LOG_WARN("failed to release com_aux_iter_", K(ret));
    tmp_ret = (tmp_ret == OB_SUCCESS) ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }
  if (OB_NOT_NULL(special_aux_iter_) && OB_FAIL(release_input_iter(special_aux_iter_))) {
    LOG_WARN("failed to release special_aux_iter_", K(ret));
    tmp_ret = (tmp_ret == OB_SUCCESS) ? ret : tmp_ret;
    ret = OB_SUCCESS;
  }

  centroid_iter_ = nullptr;
  cid_vec_iter_ = nullptr;
  rowkey_cid_iter_ = nullptr;
  com_aux_iter_ = nullptr;
  special_aux_iter_ = nullptr;

  reset_adaptor_vid_iter();
  vec_op_alloc_.reset();
  rowkey_pairs_.reset();
  distances_.reset();
  iterative_filter_ctx_.reset();
  // Release any centroid ref_cnt_ before tearing down the cache mgr guard.
  {
    int tmp_ret = batch_guard_.release_all();
    if (common::OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to release batch_guard_ in inner_release", K(tmp_ret));
    }
  }
  center_cache_guard_.reset();

  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }

  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  // The async search context is owned by the upper search driver.
  search_ctx_ = nullptr;

  ObDasVecScanUtils::release_scan_param(centroid_scan_param_);
  ObDasVecScanUtils::release_scan_param(cid_vec_scan_param_);
  ObDasVecScanUtils::release_scan_param(com_aux_scan_param_);
  ObDasVecScanUtils::release_scan_param(special_aux_scan_param_);

  if (tmp_ret != OB_SUCCESS) {
    ret = tmp_ret;
  }
  profile_ = nullptr;
  return ret;
}

// ============================================================================
// ObDASVecIndexIVFScanIter — IVF_FLAT specific
// ============================================================================

int ObDASVecIndexIVFScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASVecIndexIVFBaseScanIter::inner_reuse())) {
    LOG_WARN("failed to reuse base", K(ret));
  } else {
    near_cid_.reuse();
    if (vid_vec_map_.created()) {
      vid_vec_map_.destroy();
    }
  }
  return ret;
}

int ObDASVecIndexIVFScanIter::inner_release()
{
  if (vid_vec_map_.created()) {
    vid_vec_map_.destroy();
  }
  return ObDASVecIndexIVFBaseScanIter::inner_release();
}

int ObDASVecIndexIVFScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  UNUSED(capacity);
  count = 0;

  common::ObProfileSwitcher profile_switcher(profile_);
  if (OB_FAIL(prepare_search())) {
    LOG_WARN("failed to prepare search", K(ret));
  } else if (is_pre_filter()) {
    if (OB_FAIL(do_ivf_flat_pre_filter(true /*is_vectorized*/))) {
      LOG_WARN("failed to do ivf flat pre filter", K(ret));
    }
  } else if (is_iter_filter()) {
    if (OB_FAIL(do_post_filter_collect_round(true /*is_vectorized*/))) {
      LOG_WARN("failed to do ivf flat post filter collect round", K(ret));
    }
  } else if (is_post_filter()) {
    if (OB_FAIL(do_post_filter_round(true /*is_vectorized*/))) {
      LOG_WARN("failed to do ivf flat post filter round", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexIVFScanIter::expand_to_next_centers(bool is_vectorized, int64_t next_nprobe)
{
  int ret = OB_SUCCESS;
  near_cid_.reuse();
  if (OB_FAIL(get_next_center_ids(is_vectorized, next_nprobe))) {
    LOG_WARN("failed to get next center ids", K(ret), K(next_nprobe));
  }
  return ret;
}

int ObDASVecIndexIVFScanIter::do_post_filter_round(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  int64_t limit_k = ivf_query_cond_.query_limit_;

  if (OB_ISNULL(search_vec) || limit_k <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid search vec or limit", K(ret), KP(search_vec), K(limit_k));
  }

  // Brute-force fallback: if go_brute_force_ is set (by driver after fallback), do brute force directly
  if (OB_SUCC(ret) && go_brute_force_) {
    if (OB_FAIL(do_brute_force_scan(is_vectorized))) {
      LOG_WARN("failed to do brute force scan", K(ret));
    }
  }

  // Step 1: get center IDs on first call
  if (OB_SUCC(ret) && !go_brute_force_ && !post_filter_centers_inited_) {
    if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("[IVF_VEC] post_filter_round: no centroids found, fallback to brute force");
        ret = OB_SUCCESS;
        need_fallback_brute_force_ = true;
      } else {
        LOG_WARN("failed to get nearest probe center ids", K(ret));
      }
    } else {
      post_filter_centers_inited_ = true;
    }
  }

  // Step 2: clean up previous round's adaptor_vid_iter
  if (OB_SUCC(ret) && !go_brute_force_ && !need_fallback_brute_force_) {
    reset_adaptor_vid_iter();
    vec_op_alloc_.reset();
  }

  // Step 3: build rowkey heap and scan current center batch
  if (OB_SUCC(ret) && !go_brute_force_ && !need_fallback_brute_force_ && near_cid_.count() > 0) {
    ObExprVectorDistance::ObVecDisType cur_dis_type =
        (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN)
        ? ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
    int64_t effective_heap_size = (heap_size_ > 0) ? heap_size_ : limit_k;
    ObIvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec, cur_dis_type, dim_,
                                        effective_heap_size, similarity_threshold_);

    if (OB_FAIL(get_nearest_limit_rowkeys_in_cids(is_vectorized, nearest_rowkey_heap))) {
      LOG_WARN("failed to get nearest limit rowkeys in cids", K(ret));
    } else if (OB_FAIL(build_vid_iter_from_rowkey_heap(nearest_rowkey_heap))) {
      LOG_WARN("failed to build vid iter from rowkey heap", K(ret));
    }
  }

  return ret;
}

// ======================== FLAT deferred distance mode ========================

int ObDASVecIndexIVFScanIter::do_post_filter_collect_round(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  // Step 1: get center IDs on first call
  if (!post_filter_centers_inited_) {
    if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("[IVF_VEC] collect_round: no centroids found, fallback to brute force");
        ret = OB_SUCCESS;
        need_fallback_brute_force_ = true;
      } else {
        LOG_WARN("failed to get nearest probe center ids", K(ret));
      }
    } else {
      post_filter_centers_inited_ = true;
    }
  }

  // Step 2: clean up previous round.
  // Reuse vid_vec_map_ across rounds to avoid bucket re-allocation. The value
  // type is `float *` pointing into vec_op_alloc_ which is reset below; clear()
  // does not invoke any value destructor, so order between clear() and reset()
  // is irrelevant for safety. Cross-query teardown is still done by
  // inner_reuse()/inner_release() which call destroy() because nlist_ may
  // change and the bucket-size estimate becomes invalid.
  if (OB_SUCC(ret) && !need_fallback_brute_force_) {
    if (vid_vec_map_.created() && OB_FAIL(vid_vec_map_.clear())) {
      LOG_WARN("failed to clear vid vec map", K(ret));
    }
    reset_adaptor_vid_iter();
    vec_op_alloc_.reset();
  }

  // Step 3: collect all vid+vec into vid_vec_map_
  if (OB_SUCC(ret) && !need_fallback_brute_force_ && near_cid_.count() > 0) {
    if (OB_FAIL(collect_all_vid_vecs_in_cids(is_vectorized))) {
      LOG_WARN("failed to collect all vid vecs in cids", K(ret));
    } else if (OB_FAIL(build_vid_iter_from_vid_vec_map())) {
      LOG_WARN("failed to build vid iter from vid vec map", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexIVFScanIter::collect_all_vid_vecs_in_cids(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
  char *buf = nullptr;
  ObString cid_str;

  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("failed to prepare cid range", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cid buf", K(ret));
  } else if (!vid_vec_map_.created()
             && OB_FAIL(vid_vec_map_.create(vec_index_param_.nlist_ * near_cid_.count() * 2, "VidVecMap"))) {
    LOG_WARN("failed to create vid vec map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < near_cid_.count(); ++i) {
    cid_str.assign_buffer(buf, buf_len);
    if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(near_cid_.at(i), cid_str))) {
      LOG_WARN("failed to set center id to string", K(ret));
    } else if (OB_FAIL(collect_vid_vecs_from_cid(cid_str, cid_vec_pri_key_cnt, cid_vec_column_count,
                                                  rowkey_cnt, is_vectorized))) {
      LOG_WARN("failed to collect vid vecs from cid", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexIVFScanIter::collect_vid_vecs_from_cid(const ObString &cid_str,
                                                         int64_t cid_vec_pri_key_cnt,
                                                         int64_t cid_vec_column_count,
                                                         int64_t rowkey_cnt,
                                                         bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const int64_t candidate_rows_before = cid_vec_scan_rows_;
  common::ScopedTimer candidate_scan_timer(common::ObMetricId::VEC_IVF_CANDIDATE_SCAN_TIME,
                                           profile_);
  const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
  ObDASScanRtDef *cid_vec_rtdef = vec_index_scan_rtdef_->get_cid_vec_table_rtdef();

  if (OB_FAIL(scan_cid_range(cid_str, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef))) {
    LOG_WARN("failed to scan cid range", K(ret));
  } else {
    ObExpr *vec_expr = cid_vec_ctdef->result_output_[CID_VECTOR_IDX];
    ObExpr *pk_expr = cid_vec_ctdef->result_output_.at(CID_VEC_ROWKEY_START_IDX);
    ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;
    if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(cid_vec_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
        guard.set_batch_size(scan_row_cnt);
        cid_vec_scan_rows_ += scan_row_cnt;

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          bool vec_is_null = true;
          ObString vec;
          if (OB_FAIL(read_ivf_vec_string_at(*vec_expr, *eval_ctx,
                  mem_context_->get_arena_allocator(), i, vec_is_null, vec))) {
            LOG_WARN("failed to get real data", K(ret));
          } else if (vec_is_null || OB_ISNULL(vec.ptr())) {
            // skip null vector
          } else {
            int64_t vid_val = 0;
            if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr, *eval_ctx, i, vid_val))) {
              LOG_WARN("failed to read pk", K(ret));
            } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
              // filtered out by bitmap
            } else {
              float *vec_copy = static_cast<float *>(vec_op_alloc_.alloc(dim_ * sizeof(float)));
              if (OB_ISNULL(vec_copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc vec copy", K(ret), K(dim_));
              } else {
                MEMCPY(vec_copy, vec.ptr(), dim_ * sizeof(float));
                if (OB_FAIL(vid_vec_map_.set_refactored(vid_val, vec_copy))) {
                  LOG_WARN("failed to set vid vec map", K(ret), K(vid_val));
                }
              }
            }
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
    } else {
      // non-vectorized path
      while (OB_SUCC(ret)) {
        if (OB_FAIL(cid_vec_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next cid_vec row", K(ret));
          }
        } else {
          cid_vec_scan_rows_++;
          bool vec_is_null = true;
          ObString vec;
          if (OB_FAIL(read_ivf_vec_string_at(*vec_expr, *eval_ctx,
                  mem_context_->get_arena_allocator(), 0, vec_is_null, vec))) {
            LOG_WARN("failed to get real data", K(ret));
          } else if (vec_is_null || OB_ISNULL(vec.ptr())) {
            // skip null vector
          } else {
            int64_t vid_val = 0;
            if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr, *eval_ctx, 0, vid_val))) {
              LOG_WARN("failed to read pk", K(ret));
            } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
              // filtered out by bitmap
            } else {
              float *vec_copy = static_cast<float *>(vec_op_alloc_.alloc(dim_ * sizeof(float)));
              if (OB_ISNULL(vec_copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc vec copy", K(ret), K(dim_));
              } else {
                MEMCPY(vec_copy, vec.ptr(), dim_ * sizeof(float));
                if (OB_FAIL(vid_vec_map_.set_refactored(vid_val, vec_copy))) {
                  LOG_WARN("failed to set vid vec map", K(ret), K(vid_val));
                }
              }
            }
          }
        }
      }

      if (OB_ITER_END == ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ObDasVecScanUtils::reuse_iter(ls_id_, cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_))) {
          LOG_WARN("failed to reuse cid vec iter", K(tmp_ret));
          ret = tmp_ret;
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  const int64_t candidate_rows = cid_vec_scan_rows_ - candidate_rows_before;
  if (OB_NOT_NULL(profile_)) {
    common::ObProfileSwitcher algorithm_switcher(profile_);
    if (OB_SUCC(ret) && candidate_rows > 0) {
      INC_METRIC_VAL(common::ObMetricId::VEC_IVF_CANDIDATE_ROWS_SCANNED, candidate_rows);
    }
  }
  return ret;
}

int ObDASVecIndexIVFScanIter::build_vid_iter_from_vid_vec_map()
{
  int ret = OB_SUCCESS;
  int64_t count = vid_vec_map_.size();
  if (count == 0) {
    adaptor_vid_iter_ = nullptr;
  } else {
    void *iter_buff = vec_op_alloc_.alloc(sizeof(share::ObVectorQueryVidIterator));
    if (OB_ISNULL(iter_buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vid iter", K(ret));
    } else {
      adaptor_vid_iter_ = new(iter_buff) share::ObVectorQueryVidIterator(
          0 /*extra_column_count*/, 0 /*extra_info_actual_size*/,
          ivf_query_cond_.rel_count_, ivf_query_cond_.rel_map_ptr_);

      if (OB_FAIL(adaptor_vid_iter_->init(count, &vec_op_alloc_))) {
        LOG_WARN("failed to init vid iter", K(ret), K(count));
      } else {
        for (hash::ObHashMap<int64_t, float *>::const_iterator it = vid_vec_map_.begin();
             OB_SUCC(ret) && it != vid_vec_map_.end(); ++it) {
          if (OB_FAIL(adaptor_vid_iter_->add_result(it->first, 0.0f, nullptr))) {
            LOG_WARN("failed to add result", K(ret), K(it->first));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASVecIndexIVFScanIter::calc_distance_for_vids(common::ObIArray<VidDistancePair> &all_pairs, int64_t start_idx)
{
  int ret = OB_SUCCESS;
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  ObExprVectorDistance::ObVecDisType cur_dis_type =
      (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN)
      ? ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;

  {
    common::ScopedTimer distance_timer(
        common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME,
        start_idx < all_pairs.count() ? profile_ : nullptr);
    for (int64_t i = start_idx; OB_SUCC(ret) && i < all_pairs.count(); ++i) {
      float *vec_data = nullptr;
      ret = vid_vec_map_.get_refactored(all_pairs.at(i).vid, vec_data);
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        all_pairs.at(i).distance = FLT_MAX;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to lookup vid vec", K(ret), K(all_pairs.at(i).vid));
      } else {
        float distance = 0.0f;
        if (OB_FAIL(calc_vec_dis(search_vec, vec_data, dim_, distance, cur_dis_type))) {
          LOG_WARN("failed to calc distance", K(ret), K(all_pairs.at(i).vid));
        } else {
          all_pairs.at(i).distance = distance;
        }
      }
    }
  }
  return ret;
}

// ======================== get_nearest_probe_center_ids ========================

int ObDASVecIndexIVFScanIter::get_nearest_probe_center_ids(
    bool is_vectorized,
    bool force_persistent_scan)
{
  int ret = OB_SUCCESS;
  common::ScopedTimer nlist_timer(common::ObMetricId::VEC_IVF_SEARCH_NLIST_TIME,
                                  profile_);
  const ObDASScanCtDef *centroid_ctdef = vec_index_scan_ctdef_->get_centroid_table_ctdef();

  if (OB_ISNULL(centroid_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("centroid ctdef is null", K(ret));
  } else {
    // full scan centroid table, build top-nprobe heap
    share::ObVectorCenterClusterHelper<float, ObCenterId> nearest_cid_heap(
        mem_context_->get_arena_allocator(),
        reinterpret_cast<const float *>(real_search_vec_.ptr()),
        dis_type_, dim_, nprobes_, 0.0,
        true /*is_save_all_center*/, &iterative_filter_ctx_.allocator_);

    // Try cache path first, fallback to table scan
    share::ObIvfICache *cent_cache = nullptr;
    bool is_cache_usable = false;
    // todo(wmj): 不查cache
    if (!force_persistent_scan
        && OB_FAIL(get_centers_cache(is_vectorized, false/*is_pq*/, center_cache_guard_,
                                     cent_cache, is_cache_usable))) {
      LOG_WARN("failed to get centers cache", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!force_persistent_scan && is_cache_usable && OB_NOT_NULL(cent_cache)) {
      // cache path: read centroids from cache
      if (OB_FAIL(gen_near_cid_heap_from_cache(*cent_cache, nearest_cid_heap))) {
        LOG_WARN("failed to gen near cid heap from cache", K(ret));
      }
    } else {
      const common::ObMetricId fallback_metric_id =
          is_ns_id_valid()
              ? common::ObMetricId::VEC_IVF_MAP_CENTROID_CACHE_FALLBACK_SCAN_TIME
              : common::ObMetricId::VEC_IVF_CENTROID_CACHE_FALLBACK_SCAN_TIME;
      common::ScopedTimer fallback_scan_timer(fallback_metric_id, profile_);
      if (OB_FAIL(do_centroid_table_full_scan(is_vectorized))) {
        LOG_WARN("failed to do centroid table full scan", K(ret));
      } else {
        // table scan path: scan centroid rows and push to heap
      ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
      ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
      ObCenterId center_id;
      ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;

      if (is_vectorized) {
        IVF_GET_NEXT_ROWS_BEGIN(centroid_iter_)
        if (OB_SUCC(ret)) {
          ObEvalCtx::BatchInfoScopeGuard guard(*vec_index_scan_rtdef_->eval_ctx_);
          guard.set_batch_size(scan_row_cnt);

          for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
            guard.set_batch_idx(i);
            bool cid_is_null = true;
            bool vec_is_null = true;
            ObString cid;
            ObString cid_vec;
            if (OB_FAIL(read_ivf_cid_string_at(*cid_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), i, cid_is_null, cid))) {
              LOG_WARN("failed to read cid", K(ret), K(i));
            } else if (cid_is_null || cid.empty()) {
              LOG_TRACE("[LT hybrid DAS][IVF centroid] skip: cid empty", K(i));
            } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, cid))) {
              LOG_WARN("failed to get center id from string", K(ret), K(i), K(cid.length()));
            } else if (OB_FAIL(read_ivf_vec_string_at(*cid_vec_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), i, vec_is_null, cid_vec))) {
              LOG_WARN("failed to get real data", K(ret));
            } else if (vec_is_null || cid_vec.empty()) {
              LOG_TRACE("[LT hybrid DAS][IVF centroid] M5 skip: cid_vec empty",
                       K(i), K(cid.length()));
            } else if (OB_FAIL(nearest_cid_heap.push_center(center_id,
                                                              reinterpret_cast<float *>(cid_vec.ptr()),
                                                              dim_))) {
              LOG_WARN("failed to push center", K(ret));
            } else {
              LOG_TRACE("[LT hybrid DAS][IVF centroid] M5 push center ok",
                       K(i), K(center_id), K(cid.length()),
                       K(cid_vec.length()));
            }
          }
        }
        IVF_GET_NEXT_ROWS_END(centroid_iter_, centroid_scan_param_, centroid_tablet_id_)
      } else {
        // non-vectorized path
        centroid_iter_->clear_evaluated_flag();

        for (int i = 0; OB_SUCC(ret); ++i) {
          if (OB_FAIL(centroid_iter_->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next centroid row", K(ret));
            }
          } else {
            bool cid_is_null = true;
            bool vec_is_null = true;
            ObString cid;
            ObString cid_vec;
            if (OB_FAIL(read_ivf_cid_string_at(*cid_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), 0, cid_is_null, cid))) {
              LOG_WARN("failed to read cid", K(ret), K(i));
            } else if (cid_is_null || cid.empty()) {
              // skip null cid
            } else if (OB_FAIL(read_ivf_vec_string_at(*cid_vec_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), 0, vec_is_null, cid_vec))) {
              LOG_WARN("failed to get real data", K(ret));
            } else if (vec_is_null || cid_vec.empty()) {
              // skip null vector
            } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, cid))) {
              LOG_WARN("failed to get center id from string", K(ret));
            } else if (OB_FAIL(nearest_cid_heap.push_center(center_id,
                                                              reinterpret_cast<float *>(cid_vec.ptr()),
                                                              dim_))) {
              LOG_WARN("failed to push center", K(ret));
            }
          }
        }

        if (OB_ITER_END == ret) {
          if (OB_FAIL(centroid_iter_->reuse())) {
            LOG_WARN("fail to reuse centroid iter", K(ret));
          }
        }
      }
      }
    }

    if (OB_SUCC(ret)) {
      if (nearest_cid_heap.get_center_count() == 0) {
        if (!force_persistent_scan && is_cache_usable) {
          // A completed namespace cache can still have no centroid visible at
          // the statement snapshot while leadership/cache generations are
          // changing.  Treat that as a cache miss and read the persistent
          // centroid table before falling back to brute force.
          LOG_INFO("no visible centroid in cache, fallback to persistent scan",
                   K(ns_id_), K(ltable_id_), K(ls_id_), K(centroid_tablet_id_));
          if (OB_FAIL(get_nearest_probe_center_ids(is_vectorized, true))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              LOG_WARN("failed to fallback to persistent centroid scan", K(ret));
            }
          }
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no centroids found", K(ret), K(force_persistent_scan));
        }
      } else if (nearest_cid_heap.is_save_all_center()) {
        if (OB_FAIL(nearest_cid_heap.get_all_centroids(iterative_filter_ctx_.centroids_))) {
          LOG_WARN("failed to get all centroids", K(ret));
        } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_center_ids(nprobes_, near_cid_))) {
          LOG_WARN("failed to get initial center ids", K(ret));
        }
      } else if (OB_FAIL(nearest_cid_heap.get_nearest_probe_center_ids(near_cid_))) {
        LOG_WARN("failed to get nearest center ids", K(ret));
      }
    }
  }
  return ret;
}

int ObDASVecIndexIVFScanIter::get_next_center_ids(bool is_vectorized, int64_t next_nprobe)
{
  int ret = OB_SUCCESS;
  common::ScopedTimer nlist_timer(common::ObMetricId::VEC_IVF_SEARCH_NLIST_TIME,
                                  profile_);
  UNUSED(is_vectorized);
  near_cid_.reuse();
  if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_center_ids(next_nprobe, near_cid_))) {
    LOG_WARN("failed to get next center ids", K(ret));
  }
  return ret;
}

// ======================== IVF_FLAT Pre-Filter ========================

int ObDASVecIndexIVFScanIter::do_ivf_flat_pre_filter(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  int64_t limit_k = ivf_query_cond_.query_limit_;

  if (OB_ISNULL(search_vec) || limit_k <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid search vec or limit", K(ret), KP(search_vec), K(limit_k));
  }

  // Brute-force: keep VIDS bitmap as-is, go directly to COM_AUX scan
  if (OB_SUCC(ret) && go_brute_force_ && OB_NOT_NULL(bitmap_) && bitmap_->type_ == ObVecIndexBitmap::VIDS) {
    return do_brute_force_scan(is_vectorized);
  }

  // Check centroid existence BEFORE upgrading bitmap from VIDS to BYTE_ARRAY.
  // If centroid table is empty, fallback to brute force with the original VIDS bitmap so that
  // do_brute_force_scan can perform a filtered VID lookup instead of a full table scan.
  if (OB_SUCC(ret) && OB_FAIL(get_nearest_probe_center_ids(is_vectorized))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("[IVF_VEC] pre_filter: no centroids found, fallback to brute force");
      ret = OB_SUCCESS;
      return do_brute_force_scan(is_vectorized);
    } else {
      LOG_WARN("failed to get nearest probe center ids", K(ret));
    }
  }

  // Upgrade VIDS bitmap to BYTE_ARRAY for bitmap_->test() per-row filtering during IVF search.
  // This must happen after the centroid check above so that the VIDS bitmap is preserved for the
  // centroid-empty brute-force fallback path.
  if (OB_SUCC(ret) && OB_NOT_NULL(bitmap_) && bitmap_->type_ == ObVecIndexBitmap::VIDS) {
    if (OB_FAIL(bitmap_->upgrade_to_byte_array())) {
      LOG_WARN("failed to upgrade bitmap to byte array", K(ret));
    }
  }

  // build rowkey distance heap (EUCLIDEAN → EUCLIDEAN_SQUARED for heap comparison)
  ObExprVectorDistance::ObVecDisType cur_dis_type =
      (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN)
      ? ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
  ObIvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec, cur_dis_type, dim_,
                                      limit_k, similarity_threshold_);

  // iterative loop: scan centers, expand if not enough results
  if (OB_SUCC(ret)) {
    int64_t iter_cnt = 0;
    bool iter_end = false;
    bool no_new_near_rowkeys = false;
    while (OB_SUCC(ret) && !iter_end && nearest_rowkey_heap.count() < limit_k) {
      int32_t start_idx = nearest_rowkey_heap.count();

      if (OB_FAIL(get_nearest_limit_rowkeys_in_cids(is_vectorized, nearest_rowkey_heap))) {
        LOG_WARN("failed to get nearest limit rowkeys in cids", K(ret));
      }

      no_new_near_rowkeys = (nearest_rowkey_heap.count() == start_idx);

      if (OB_FAIL(ret)) {
      } else if (nearest_rowkey_heap.count() < limit_k) {
        ++iter_cnt;
        int64_t left_search = limit_k - nearest_rowkey_heap.count();
        int64_t next_nprobe = OB_MIN(nprobes_, static_cast<int64_t>(nprobes_ * ((double)left_search / limit_k) + 1));
        LOG_INFO("[IVF_VEC] prefilter: not enough results, expanding centers",
                 K(limit_k), "heap_count", nearest_rowkey_heap.count(), K(next_nprobe), K(iter_cnt),
                 K_(cid_vec_scan_rows), K_(max_scan_vectors));
        near_cid_.reuse();

        if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
          iter_end = true;
          LOG_INFO("[IVF_VEC] no new results with similarity_threshold, stop iterative filter",
                   K_(similarity_threshold), K(no_new_near_rowkeys), "heap_count", nearest_rowkey_heap.count(),
                   K(start_idx), K(limit_k));
        } else if (max_scan_vectors_ > 0 && cid_vec_scan_rows_ > max_scan_vectors_) {
          iter_end = true;
          LOG_INFO("[IVF_VEC] reach max scan vectors, stop iterative filter",
                   K_(cid_vec_scan_rows), K_(max_scan_vectors), "heap_count", nearest_rowkey_heap.count(), K(limit_k));
        } else if (!iterative_filter_ctx_.has_next_center()) {
          iter_end = true;
        } else if (OB_FAIL(get_next_center_ids(is_vectorized, next_nprobe))) {
          LOG_WARN("failed to get next center ids", K(ret));
        } else if (near_cid_.count() == 0) {
          iter_end = true;
        }
      }
    }
  }

  // build result from heap
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_vid_iter_from_rowkey_heap(nearest_rowkey_heap))) {
      LOG_WARN("failed to build vid iter from rowkey heap", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexIVFScanIter::get_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    ObIvfRowkeyHeap &nearest_rowkey_heap)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
  char *buf = nullptr;
  ObString cid_str;

  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("failed to prepare cid range", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cid buf", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < near_cid_.count(); ++i) {
    cid_str.assign_buffer(buf, buf_len);
    if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(near_cid_.at(i), cid_str))) {
      LOG_WARN("failed to set center id to string", K(ret));
    } else if (OB_FAIL(get_rowkeys_to_heap(cid_str, cid_vec_pri_key_cnt, cid_vec_column_count,
                                           rowkey_cnt, is_vectorized, nearest_rowkey_heap))) {
      LOG_WARN("failed to get rowkeys to heap", K(ret));
    }
  }
  return ret;
}

int ObDASVecIndexIVFScanIter::get_rowkeys_to_heap(const ObString &cid_str,
                                                   int64_t cid_vec_pri_key_cnt,
                                                   int64_t cid_vec_column_count,
                                                   int64_t rowkey_cnt,
                                                   bool is_vectorized,
                                                   ObIvfRowkeyHeap &nearest_rowkey_heap)
{
  int ret = OB_SUCCESS;
  const int64_t candidate_rows_before = cid_vec_scan_rows_;
  common::ScopedTimer candidate_scan_timer(common::ObMetricId::VEC_IVF_CANDIDATE_SCAN_TIME,
                                           profile_);
  const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
  ObDASScanRtDef *cid_vec_rtdef = vec_index_scan_rtdef_->get_cid_vec_table_rtdef();

  if (OB_FAIL(scan_cid_range(cid_str, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef))) {
    LOG_WARN("failed to scan cid range", K(ret));
  } else {
    // cid_vec result output layout: [cid_col(0), vec_col(1), hidden_pk(2)]
    ObExpr *vec_expr = cid_vec_ctdef->result_output_[CID_VECTOR_IDX];
    ObExpr *pk_expr = cid_vec_ctdef->result_output_.at(CID_VEC_ROWKEY_START_IDX);
    ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;
    if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(cid_vec_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
        guard.set_batch_size(scan_row_cnt);
        cid_vec_scan_rows_ += scan_row_cnt;

        common::ScopedTimer distance_timer(
            common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME);
        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          guard.set_batch_idx(i);
          bool vec_is_null = true;
          ObString vec;
          if (OB_FAIL(read_ivf_vec_string_at(*vec_expr, *eval_ctx,
                  mem_context_->get_arena_allocator(), i, vec_is_null, vec))) {
            LOG_WARN("failed to get real data", K(ret));
          } else if (vec_is_null || OB_ISNULL(vec.ptr())) {
            // skip null vector
          } else {
            int64_t vid_val = 0;
            if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr, *eval_ctx, i, vid_val))) {
              LOG_WARN("failed to read pk", K(ret));
            } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
              // filtered out by bitmap
            } else {
              ObObj rk_obj;
              rk_obj.set_int(vid_val);
              ObRowkey main_rowkey(&rk_obj, 1);
              if (OB_FAIL(nearest_rowkey_heap.push_center(
                      main_rowkey, reinterpret_cast<float *>(vec.ptr()), dim_))) {
                LOG_WARN("failed to push center", K(ret), K(vid_val));
              }
            }
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
    } else {
      // non-vectorized path
      common::ScopedTimer distance_timer(
          common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME);
      while (OB_SUCC(ret)) {
        if (OB_FAIL(cid_vec_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next cid_vec row", K(ret));
          }
        } else {
          cid_vec_scan_rows_++;
          bool vec_is_null = true;
          ObString vec;
          if (OB_FAIL(read_ivf_vec_string_at(*vec_expr, *eval_ctx,
                  mem_context_->get_arena_allocator(), 0, vec_is_null, vec))) {
            LOG_WARN("failed to get real data", K(ret));
          } else if (vec_is_null || OB_ISNULL(vec.ptr())) {
            // skip null vector
          } else {
            int64_t vid_val = 0;
            if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr, *eval_ctx, 0, vid_val))) {
              LOG_WARN("failed to read pk", K(ret));
            } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
              // filtered out by bitmap
            } else {
              ObObj rk_obj;
              rk_obj.set_int(vid_val);
              ObRowkey main_rowkey(&rk_obj, 1);
              if (OB_FAIL(nearest_rowkey_heap.push_center(
                      main_rowkey, reinterpret_cast<float *>(vec.ptr()), dim_))) {
                LOG_WARN("failed to push center", K(ret), K(vid_val));
              }
            }
          }
        }
      }

      if (OB_ITER_END == ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ObDasVecScanUtils::reuse_iter(ls_id_, cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_))) {
          LOG_WARN("failed to reuse cid vec iter", K(tmp_ret));
          ret = tmp_ret;
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  const int64_t candidate_rows = cid_vec_scan_rows_ - candidate_rows_before;
  if (OB_NOT_NULL(profile_)) {
    common::ObProfileSwitcher algorithm_switcher(profile_);
    if (OB_SUCC(ret) && candidate_rows > 0) {
      INC_METRIC_VAL(common::ObMetricId::VEC_IVF_CANDIDATE_ROWS_SCANNED, candidate_rows);
    }
  }
  return ret;
}

// ============================================================================
// ObDASVecIndexIVFPQScanIter — IVF_PQ specific (stub)
// ============================================================================

int ObDASVecIndexIVFPQScanIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASVecIndexIVFBaseScanIter::inner_init(param))) {
    LOG_WARN("failed to init base", K(ret));
  } else {
    // extract PQ parameters from vec_index_param_
    m_ = vec_index_param_.m_;
    nbits_ = vec_index_param_.nbits_;
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDASVecIndexIVFBaseScanIter::inner_reuse())) {
    LOG_WARN("failed to reuse base", K(ret));
  } else {
    near_cid_vec_.reuse();
    near_cid_vec_dis_.reuse();
    if (vid_pq_map_.created()) {
      vid_pq_map_.destroy();
    }
    pq_pre_compute_ctx_.reset();
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::inner_release()
{
  // These ObArray members are allocated from the tenant's default memory context.
  // Their internal buffers must be explicitly freed here because create_das_iter
  // uses OB_NEWx + alloc.free() which does not invoke the C++ destructor.
  near_cid_vec_.reset();
  near_cid_vec_dis_.reset();
  // pq_pre_compute_ctx_.reset() only calls splited_residual_.reuse() (keeps buffer).
  // Explicitly reset to free the buffer (labeled "CommonArray").
  pq_pre_compute_ctx_.splited_residual_.reset();
  if (vid_pq_map_.created()) {
    vid_pq_map_.destroy();
  }
  return ObDASVecIndexIVFBaseScanIter::inner_release();
}

int ObDASVecIndexIVFPQScanIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  UNUSED(capacity);
  count = 0;

  common::ObProfileSwitcher profile_switcher(profile_);
  if (OB_FAIL(prepare_search())) {
    LOG_WARN("failed to prepare search", K(ret));
  } else if (is_pre_filter()) {
    if (OB_FAIL(do_ivf_pq_pre_filter(true /*is_vectorized*/))) {
      LOG_WARN("failed to do ivf pq pre filter", K(ret));
    }
  } else if (is_iter_filter()) {
    if (OB_FAIL(do_post_filter_collect_round(true /*is_vectorized*/))) {
      LOG_WARN("failed to do ivf pq post filter collect round", K(ret));
    }
  } else if (is_post_filter()) {
    if (OB_FAIL(do_post_filter_round(true /*is_vectorized*/))) {
      LOG_WARN("failed to do ivf pq post filter round", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexIVFPQScanIter::expand_to_next_centers(bool is_vectorized, int64_t next_nprobe)
{
  near_cid_vec_.reuse();
  near_cid_vec_dis_.reuse();
  return get_next_center_ids_with_vec(is_vectorized, next_nprobe);
}

int ObDASVecIndexIVFPQScanIter::do_post_filter_round(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  int64_t limit_k = ivf_query_cond_.query_limit_;
  int64_t sub_dim = m_ > 0 ? dim_ / m_ : dim_;
  if (OB_ISNULL(search_vec) || limit_k <= 0 || m_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid search vec or limit or m", K(ret), KP(search_vec), K(limit_k), K_(m));
  }

  // Brute-force fallback: if go_brute_force_ is set (by driver after fallback), do brute force directly
  if (OB_SUCC(ret) && go_brute_force_) {
    return do_brute_force_scan(is_vectorized);
  }

  // Step 1: get center IDs with vectors on first call
  if (OB_SUCC(ret) && !post_filter_centers_inited_) {
    if (OB_FAIL(get_nearest_probe_center_ids_with_vec(is_vectorized))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("[IVF_PQ] post_filter_round: no centroids found, fallback to brute force");
        ret = OB_SUCCESS;
        need_fallback_brute_force_ = true;
        return ret;
      } else {
        LOG_WARN("failed to get nearest probe center ids with vec", K(ret));
      }
    } else {
      post_filter_centers_inited_ = true;
    }
  }

  // Step 2: clean up previous round's adaptor_vid_iter
  if (OB_SUCC(ret)) {
    reset_adaptor_vid_iter();
    vec_op_alloc_.reset();
  }

  // Step 3: build rowkey heap and scan current center batch
  if (OB_SUCC(ret) && near_cid_vec_.count() > 0) {
    ObExprVectorDistance::ObVecDisType cur_dis_type =
        (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN)
        ? ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
    // PQ heap uses sub_dim (not dim_) — distance is sum of sub-space distances
    int64_t effective_heap_size = (heap_size_ > 0) ? heap_size_ : limit_k;
    ObIvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec, cur_dis_type, sub_dim,
                                        effective_heap_size, similarity_threshold_);

    if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(is_vectorized, search_vec, nearest_rowkey_heap))) {
      LOG_WARN("failed to calc nearest limit rowkeys in cids", K(ret));
    } else if (OB_FAIL(build_vid_iter_from_rowkey_heap(nearest_rowkey_heap))) {
      LOG_WARN("failed to build vid iter from rowkey heap", K(ret));
    }
  }

  return ret;
}

// ======================== PQ deferred distance mode ========================

int ObDASVecIndexIVFPQScanIter::do_post_filter_collect_round(bool is_vectorized)
{
  int ret = OB_SUCCESS;

  // Step 1: get center IDs with vectors on first call
  if (!post_filter_centers_inited_) {
    if (OB_FAIL(get_nearest_probe_center_ids_with_vec(is_vectorized))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_INFO("[IVF_PQ] collect_round: no centroids found, fallback to brute force");
        ret = OB_SUCCESS;
        need_fallback_brute_force_ = true;
        return ret;
      } else {
        LOG_WARN("failed to get nearest probe center ids with vec", K(ret));
      }
    } else {
      post_filter_centers_inited_ = true;
    }
  }

  // Step 2: clean up previous round
  if (OB_SUCC(ret)) {
    if (vid_pq_map_.created()) {
      vid_pq_map_.destroy();
    }
    reset_adaptor_vid_iter();
    vec_op_alloc_.reset();
  }

  // Step 3: collect all (vid, cid_idx, pq_ids) into vid_pq_map_
  if (OB_SUCC(ret) && near_cid_vec_.count() > 0) {
    if (OB_FAIL(collect_all_pq_vids_in_cids(is_vectorized))) {
      LOG_WARN("failed to collect all pq vids in cids", K(ret));
    } else if (OB_FAIL(build_vid_iter_from_vid_pq_map())) {
      LOG_WARN("failed to build vid iter from vid pq map", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexIVFPQScanIter::collect_all_pq_vids_in_cids(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
  char *buf = nullptr;
  ObString cid_str;

  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("failed to prepare cid range", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(mem_context_->get_arena_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cid buf", K(ret));
  } else if (OB_FAIL(vid_pq_map_.create(vec_index_param_.nlist_ * near_cid_vec_.count() * 2,
                                        "VidPqMap"))) {  // nlist * nlist = total_count and consider Imbalanced
    LOG_WARN("failed to create vid pq map", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < near_cid_vec_.count(); ++i) {
    const ObCenterId &cur_cid = near_cid_vec_.at(i).first;
    cid_str.assign_buffer(buf, buf_len);
    if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(cur_cid, cid_str))) {
      LOG_WARN("failed to set center id to string", K(ret));
    } else if (OB_FAIL(collect_pq_vids_from_cid(i, cid_str, cid_vec_pri_key_cnt, cid_vec_column_count,
                                                  rowkey_cnt, is_vectorized))) {
      LOG_WARN("failed to collect pq vids from cid", K(ret), K(i));
    }
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::collect_pq_vids_from_cid(int64_t cid_idx,
                                                           const ObString &cid_str,
                                                           int64_t cid_vec_pri_key_cnt,
                                                           int64_t cid_vec_column_count,
                                                           int64_t rowkey_cnt,
                                                           bool is_vectorized)
{
  int ret = OB_SUCCESS;
  const int64_t candidate_rows_before = cid_vec_scan_rows_;
  common::ScopedTimer candidate_scan_timer(common::ObMetricId::VEC_IVF_CANDIDATE_SCAN_TIME,
                                           profile_);
  const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
  ObDASScanRtDef *cid_vec_rtdef = vec_index_scan_rtdef_->get_cid_vec_table_rtdef();

  if (OB_FAIL(scan_cid_range(cid_str, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef))) {
    LOG_WARN("failed to scan cid range", K(ret));
  } else {
    ObExpr *pq_ids_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
    ObExpr *pk_expr = cid_vec_ctdef->result_output_.at(CID_VEC_ROWKEY_START_IDX);
    ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;

    if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(cid_vec_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
        guard.set_batch_size(scan_row_cnt);
        cid_vec_scan_rows_ += scan_row_cnt;

        for (int64_t j = 0; OB_SUCC(ret) && j < scan_row_cnt; ++j) {
          guard.set_batch_idx(j);
          bool pq_is_null = true;
          ObString pq_center_ids;
          if (OB_FAIL(read_ivf_pq_ids_string_at(*pq_ids_expr, *eval_ctx, vec_op_alloc_,
                  j, pq_is_null, pq_center_ids))) {
            LOG_WARN("failed to read pq ids", K(ret), K(j));
          } else if (pq_is_null || pq_center_ids.empty()) {
            // skip null
          } else {
            int64_t vid_val = 0;
            if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr, *eval_ctx, j, vid_val))) {
              LOG_WARN("failed to read pk", K(ret), K(j));
            } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
              // filtered out by bitmap
            } else {
              char *pq_ids_copy = static_cast<char *>(vec_op_alloc_.alloc(pq_center_ids.length()));
              if (OB_ISNULL(pq_ids_copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc pq ids copy", K(ret), K(pq_center_ids.length()));
              } else {
                MEMCPY(pq_ids_copy, pq_center_ids.ptr(), pq_center_ids.length());
                PQVidInfo info;
                info.cid_idx = cid_idx;
                info.pq_ids.assign_ptr(pq_ids_copy, pq_center_ids.length());
                if (OB_FAIL(vid_pq_map_.set_refactored(vid_val, info))) {
                  LOG_WARN("failed to set vid pq map", K(ret), K(vid_val));
                }
              }
            }
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
    } else {
      // non-vectorized path
      cid_vec_iter_->clear_evaluated_flag();

      while (OB_SUCC(ret)) {
        if (OB_FAIL(cid_vec_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan cid_vec iter", K(ret));
          }
        } else {
          cid_vec_scan_rows_++;
          bool pq_is_null = true;
          ObString pq_center_ids;
          if (OB_FAIL(read_ivf_pq_ids_string_at(*pq_ids_expr, *eval_ctx, vec_op_alloc_,
                  0, pq_is_null, pq_center_ids))) {
            LOG_WARN("failed to read pq ids", K(ret));
          } else if (pq_is_null || pq_center_ids.empty()) {
            // skip null
          } else {
            int64_t vid_val = 0;
            if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr, *eval_ctx, 0, vid_val))) {
              LOG_WARN("failed to read pk", K(ret));
            } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
              // filtered out by bitmap
            } else {
              char *pq_ids_copy = static_cast<char *>(vec_op_alloc_.alloc(pq_center_ids.length()));
              if (OB_ISNULL(pq_ids_copy)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc pq ids copy", K(ret));
              } else {
                MEMCPY(pq_ids_copy, pq_center_ids.ptr(), pq_center_ids.length());
                PQVidInfo info;
                info.cid_idx = cid_idx;
                info.pq_ids.assign_ptr(pq_ids_copy, pq_center_ids.length());
                if (OB_FAIL(vid_pq_map_.set_refactored(vid_val, info))) {
                  LOG_WARN("failed to set vid pq map", K(ret), K(vid_val));
                }
              }
            }
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(cid_vec_iter_->reuse())) {
          LOG_WARN("fail to reuse cid_vec iter", K(ret));
        }
      }
    }
  }
  const int64_t candidate_rows = cid_vec_scan_rows_ - candidate_rows_before;
  if (OB_SUCC(ret) && candidate_rows > 0 && OB_NOT_NULL(profile_)) {
    common::ObProfileSwitcher algorithm_switcher(profile_);
    INC_METRIC_VAL(common::ObMetricId::VEC_IVF_CANDIDATE_ROWS_SCANNED, candidate_rows);
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::build_vid_iter_from_vid_pq_map()
{
  int ret = OB_SUCCESS;
  int64_t count = vid_pq_map_.size();
  if (count == 0) {
    adaptor_vid_iter_ = nullptr;
  } else {
    void *iter_buff = vec_op_alloc_.alloc(sizeof(share::ObVectorQueryVidIterator));
    if (OB_ISNULL(iter_buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc vid iter", K(ret));
    } else {
      adaptor_vid_iter_ = new(iter_buff) share::ObVectorQueryVidIterator(
          0 /*extra_column_count*/, 0 /*extra_info_actual_size*/,
          ivf_query_cond_.rel_count_, ivf_query_cond_.rel_map_ptr_);

      if (OB_FAIL(adaptor_vid_iter_->init(count, &vec_op_alloc_))) {
        LOG_WARN("failed to init vid iter", K(ret), K(count));
      } else {
        for (hash::ObHashMap<int64_t, PQVidInfo>::const_iterator it = vid_pq_map_.begin();
             OB_SUCC(ret) && it != vid_pq_map_.end(); ++it) {
          if (OB_FAIL(adaptor_vid_iter_->add_result(it->first, 0.0f, nullptr))) {
            LOG_WARN("failed to add result", K(ret), K(it->first));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::prepare_pq_pre_compute_ctx()
{
  int ret = OB_SUCCESS;
  if (pq_pre_compute_ctx_.prepared_) {
    return ret;
  }

  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  int64_t ksub = 1L << nbits_;
  pq_pre_compute_ctx_.is_l2_ = (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN);

  if (OB_ISNULL(search_vec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("search vec is null", K(ret));
  } else if (OB_FAIL(check_can_pre_compute(true /*is_vectorized*/,
                                            pq_pre_compute_ctx_.pre_cache_guard_,
                                            pq_pre_compute_ctx_.pre_cent_cache_,
                                            pq_pre_compute_ctx_.pre_compute_table_))) {
    LOG_WARN("fail to check can use precompute table", K(ret));
  } else if (pq_pre_compute_ctx_.pre_compute_table_) {
    pq_pre_compute_ctx_.sim_table_ = static_cast<float *>(
        mem_context_->get_arena_allocator().alloc(sizeof(float) * (ksub * m_) * 2));
    if (OB_ISNULL(pq_pre_compute_ctx_.sim_table_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc sim table", K(ret));
    } else {
      pq_pre_compute_ctx_.sim_table_2_ = pq_pre_compute_ctx_.sim_table_ + (ksub * m_);
      if (pq_pre_compute_ctx_.is_l2_
          && OB_FAIL(pre_compute_inner_prod_table(search_vec, pq_pre_compute_ctx_.sim_table_2_, true /*is_vectorized*/))) {
        LOG_WARN("fail to pre compute inner prod table", K(ret));
      } else if (!pq_pre_compute_ctx_.is_l2_
          && OB_FAIL(pre_compute_inner_prod_table(search_vec, pq_pre_compute_ctx_.sim_table_, true /*is_vectorized*/))) {
        LOG_WARN("fail to pre compute inner prod table", K(ret));
      }
    }
  } else {
    if (OB_FAIL(pq_pre_compute_ctx_.splited_residual_.reserve(m_))) {
      LOG_WARN("fail to init splited residual array", K(ret), K_(m));
    } else if (pq_pre_compute_ctx_.is_l2_) {
      char *residual_buf = static_cast<char *>(
          mem_context_->get_arena_allocator().alloc(dim_ * sizeof(float)));
      if (OB_ISNULL(residual_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc residual buf", K(ret));
      } else {
        pq_pre_compute_ctx_.residual_ = new (residual_buf) float[dim_];
      }
    }
  }

  if (OB_SUCC(ret)) {
    pq_pre_compute_ctx_.prepared_ = true;
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::calc_distance_for_vids(common::ObIArray<VidDistancePair> &all_pairs, int64_t start_idx)
{
  int ret = OB_SUCCESS;
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());

  if (start_idx >= all_pairs.count() || OB_ISNULL(search_vec)) {
    // nothing to do
  } else if (OB_FAIL(prepare_pq_pre_compute_ctx())) {
    LOG_WARN("failed to prepare pq pre compute context", K(ret));
  } else {
    common::ScopedTimer distance_timer(
        common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME, profile_);
    common::ScopedTimer fallback_distance_timer(
        common::ObMetricId::VEC_IVF_PQ_PRECOMPUTE_CACHE_FALLBACK_CALC_TIME,
        pq_pre_compute_ctx_.pre_compute_table_ ? nullptr : profile_);
    int64_t ksub = 1L << nbits_;
    ObSEArray<ObSEArray<VidIdxInfo, 16>, 16> cid_groups;
    cid_groups.set_attr(ObMemAttr(MTL_ID(), "IvfCidGrp"));

    if (OB_FAIL(group_vids_by_cid(all_pairs, start_idx, cid_groups))) {
      LOG_WARN("failed to group vids by cid", K(ret));
    }

    for (int64_t c = 0; OB_SUCC(ret) && c < cid_groups.count(); ++c) {
      const ObSEArray<VidIdxInfo, 16> &group = cid_groups.at(c);
      if (group.empty()) {
        continue;
      }

      const ObCenterId &cur_cid = near_cid_vec_.at(c).first;
      float *cur_cid_vec = near_cid_vec_.at(c).second;
      float dis0 = near_cid_vec_dis_.at(c);

      if (pq_pre_compute_ctx_.pre_compute_table_) {
        if (pq_pre_compute_ctx_.is_l2_) {
          const float *sim_table_ptrs = pq_pre_compute_ctx_.pre_cent_cache_->get_centroids()
              + (cur_cid.center_id_ - 1) * ksub * m_;
          ObVectorL2Distance<float>::fvec_madd(m_ * ksub, sim_table_ptrs, -2.0,
              pq_pre_compute_ctx_.sim_table_2_, pq_pre_compute_ctx_.sim_table_);
        }
        if (OB_FAIL(calc_precompute_distance_for_group(group, dis0, all_pairs))) {
          LOG_WARN("failed to calc precompute distance for group", K(ret), K(c));
        }
      } else {
        pq_pre_compute_ctx_.splited_residual_.reuse();
        if (pq_pre_compute_ctx_.is_l2_) {
          if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(dim_, search_vec, cur_cid_vec,
                                                              pq_pre_compute_ctx_.residual_))) {
            LOG_WARN("fail to calc residual vector", K(ret));
          }
        } else {
          pq_pre_compute_ctx_.residual_ = search_vec;
        }
        if (OB_SUCC(ret) && OB_FAIL(ObVectorIndexUtil::split_vector(m_, dim_,
            pq_pre_compute_ctx_.residual_, pq_pre_compute_ctx_.splited_residual_))) {
          LOG_WARN("fail to split vector", K(ret));
        }
        if (OB_SUCC(ret) && OB_FAIL(calc_residual_distance_for_group(group, dis0, all_pairs))) {
          LOG_WARN("failed to calc residual distance for group", K(ret), K(c));
        }
      }
    }
  }

  return ret;
}

int ObDASVecIndexIVFPQScanIter::group_vids_by_cid(
    common::ObIArray<VidDistancePair> &all_pairs,
    int64_t start_idx,
    ObSEArray<ObSEArray<VidIdxInfo, 16>, 16> &cid_groups)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(cid_groups.prepare_allocate(near_cid_vec_.count()))) {
    LOG_WARN("failed to prepare allocate cid groups", K(ret));
  }
  for (int64_t i = start_idx; OB_SUCC(ret) && i < all_pairs.count(); ++i) {
    PQVidInfo info;
    ret = vid_pq_map_.get_refactored(all_pairs.at(i).vid, info);
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      all_pairs.at(i).distance = FLT_MAX;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to lookup vid pq info", K(ret), K(all_pairs.at(i).vid));
    } else if (info.cid_idx >= 0 && info.cid_idx < near_cid_vec_.count()) {
      VidIdxInfo idx_info;
      idx_info.pair_idx = i;
      idx_info.vid = all_pairs.at(i).vid;
      if (OB_FAIL(cid_groups.at(info.cid_idx).push_back(idx_info))) {
        LOG_WARN("failed to push back vid to group", K(ret));
      }
    } else {
      all_pairs.at(i).distance = FLT_MAX;
    }
  }

  return ret;
}

int ObDASVecIndexIVFPQScanIter::calc_precompute_distance_for_group(
    const ObSEArray<VidIdxInfo, 16> &group,
    float dis0,
    common::ObIArray<VidDistancePair> &all_pairs)
{
  int ret = OB_SUCCESS;
  int counter = 0;
  int64_t saved_j[4] = {0, 0, 0, 0};
  PQVidInfo saved_info[4];

  for (int64_t j = 0; OB_SUCC(ret) && j < group.count(); ++j) {
    PQVidInfo info;
    ret = vid_pq_map_.get_refactored(group.at(j).vid, info);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to lookup vid pq info", K(ret), K(group.at(j).vid));
    } else {
      saved_j[counter] = j;
      saved_info[counter] = info;
      counter++;
      if (counter == 4) {
        const uint8_t *ptr0 = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_info[0].pq_ids.ptr());
        const uint8_t *ptr1 = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_info[1].pq_ids.ptr());
        const uint8_t *ptr2 = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_info[2].pq_ids.ptr());
        const uint8_t *ptr3 = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_info[3].pq_ids.ptr());
        float d0 = 0, d1 = 0, d2 = 0, d3 = 0;
        ObVectorL2Distance<float>::distance_four_codes(
            m_, nbits_, pq_pre_compute_ctx_.sim_table_, ptr0, ptr1, ptr2, ptr3,
            d0, d1, d2, d3);
        all_pairs.at(group.at(saved_j[0]).pair_idx).distance = d0 + dis0;
        all_pairs.at(group.at(saved_j[1]).pair_idx).distance = d1 + dis0;
        all_pairs.at(group.at(saved_j[2]).pair_idx).distance = d2 + dis0;
        all_pairs.at(group.at(saved_j[3]).pair_idx).distance = d3 + dis0;
        counter = 0;
      }
    }
  }
  for (int k = 0; OB_SUCC(ret) && k < counter; ++k) {
    const uint8_t *ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_info[k].pq_ids.ptr());
    float dis = ObVectorL2Distance<float>::distance_one_code(m_, nbits_,
        pq_pre_compute_ctx_.sim_table_, ptr) + dis0;
    all_pairs.at(group.at(saved_j[k]).pair_idx).distance = dis;
  }

  return ret;
}

int ObDASVecIndexIVFPQScanIter::calc_residual_distance_for_group(
    const ObSEArray<VidIdxInfo, 16> &group,
    float dis0,
    common::ObIArray<VidDistancePair> &all_pairs)
{
  int ret = OB_SUCCESS;

  for (int64_t j = 0; OB_SUCC(ret) && j < group.count(); ++j) {
    PQVidInfo info;
    ret = vid_pq_map_.get_refactored(group.at(j).vid, info);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to lookup vid pq info", K(ret), K(group.at(j).vid));
    } else {
      float distance = 0.0f;
      if (OB_FAIL(calc_distance_between_pq_ids(true /*is_vectorized*/, info.pq_ids,
                                                pq_pre_compute_ctx_.splited_residual_, distance))) {
        LOG_WARN("fail to calc distance between pq ids", K(ret));
      } else {
        all_pairs.at(group.at(j).pair_idx).distance = distance + dis0;
      }
    }
  }

  return ret;
}

int ObDASVecIndexIVFPQScanIter::do_ivf_pq_pre_filter(bool is_vectorized)
{
  int ret = OB_SUCCESS;
  float *search_vec = reinterpret_cast<float *>(real_search_vec_.ptr());
  int64_t limit_k = ivf_query_cond_.query_limit_;
  int64_t sub_dim = m_ > 0 ? dim_ / m_ : dim_;
  if (OB_ISNULL(search_vec) || limit_k <= 0 || m_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid search vec or limit or m", K(ret), KP(search_vec), K(limit_k), K_(m));
  }

  // Brute-force: keep VIDS bitmap as-is, go directly to COM_AUX scan
  if (OB_SUCC(ret) && go_brute_force_ && OB_NOT_NULL(bitmap_) && bitmap_->type_ == ObVecIndexBitmap::VIDS) {
    return do_brute_force_scan(is_vectorized);
  }

  // Check centroid existence BEFORE upgrading bitmap from VIDS to BYTE_ARRAY.
  // If centroid table is empty, fallback to brute force with the original VIDS bitmap so that
  // do_brute_force_scan can perform a filtered VID lookup instead of a full table scan.
  if (OB_SUCC(ret) && OB_FAIL(get_nearest_probe_center_ids_with_vec(is_vectorized))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("[IVF_PQ] pre_filter: no centroids found, fallback to brute force");
      ret = OB_SUCCESS;
      return do_brute_force_scan(is_vectorized);
    } else {
      LOG_WARN("failed to get nearest probe center ids with vec", K(ret));
    }
  }

  // Upgrade VIDS bitmap to BYTE_ARRAY for bitmap_->test() per-row filtering during IVF search.
  // This must happen after the centroid check above so that the VIDS bitmap is preserved for the
  // centroid-empty brute-force fallback path.
  if (OB_SUCC(ret) && OB_NOT_NULL(bitmap_) && bitmap_->type_ == ObVecIndexBitmap::VIDS) {
    if (OB_FAIL(bitmap_->upgrade_to_byte_array())) {
      LOG_WARN("failed to upgrade bitmap to byte array", K(ret));
    }
  }

  // PQ uses EUCLIDEAN_SQUARED for heap comparison (same as old code)
  ObExprVectorDistance::ObVecDisType cur_dis_type =
      (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN)
      ? ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED : dis_type_;
  // PQ heap uses sub_dim (not dim_) — distance is sum of sub-space distances
  ObIvfRowkeyHeap nearest_rowkey_heap(vec_op_alloc_, search_vec, cur_dis_type, sub_dim,
                                      limit_k, similarity_threshold_);

  // iterative loop: scan centers, expand if not enough results
  if (OB_SUCC(ret)) {
    int64_t iter_cnt = 0;
    bool iter_end = false;
    bool no_new_near_rowkeys = false;
    while (OB_SUCC(ret) && !iter_end && nearest_rowkey_heap.count() < limit_k) {
      int32_t start_idx = nearest_rowkey_heap.count();

      if (OB_FAIL(calc_nearest_limit_rowkeys_in_cids(is_vectorized, search_vec, nearest_rowkey_heap))) {
        LOG_WARN("failed to calc nearest limit rowkeys in cids", K(ret));
      }

      no_new_near_rowkeys = (nearest_rowkey_heap.count() == start_idx);

      if (OB_FAIL(ret)) {
      } else if (nearest_rowkey_heap.count() < limit_k) {
        ++iter_cnt;
        int64_t left_search = limit_k - nearest_rowkey_heap.count();
        int64_t next_nprobe = OB_MIN(nprobes_, static_cast<int64_t>(nprobes_ * ((double)left_search / limit_k) + 1));
        LOG_INFO("[IVF_PQ] prefilter: not enough results, expanding centers",
                 K(limit_k), "heap_count", nearest_rowkey_heap.count(), K(next_nprobe), K(iter_cnt),
                 K_(cid_vec_scan_rows), K_(max_scan_vectors));
        near_cid_vec_.reuse();
        near_cid_vec_dis_.reuse();

        if (similarity_threshold_ != 0 && no_new_near_rowkeys) {
          iter_end = true;
        } else if (max_scan_vectors_ > 0 && cid_vec_scan_rows_ > max_scan_vectors_) {
          iter_end = true;
        } else if (!iterative_filter_ctx_.has_next_center()) {
          iter_end = true;
        } else if (OB_FAIL(get_next_center_ids_with_vec(is_vectorized, next_nprobe))) {
          LOG_WARN("failed to get next center ids with vec", K(ret));
        } else if (near_cid_vec_.count() == 0) {
          iter_end = true;
        } else if (near_cid_vec_.count() != near_cid_vec_dis_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("near_cid_vec count != near_cid_vec_dis count", K(ret),
                   "near_cid_vec_count", near_cid_vec_.count(),
                   "near_cid_vec_dis_count", near_cid_vec_dis_.count());
        }
      }
    }
  }

  // build result from heap
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_vid_iter_from_rowkey_heap(nearest_rowkey_heap))) {
      LOG_WARN("failed to build vid iter from rowkey heap", K(ret));
    }
  }
  return ret;
}

// ======================== PQ centroid cache write ========================

int ObDASVecIndexIVFPQScanIter::try_write_pq_centroid_cache(
    share::ObIvfICache &cent_cache, bool is_vectorized)
{
  int ret = OB_SUCCESS;
  RWLock::WLockGuard guard(cent_cache.get_lock());
  if (!cent_cache.is_writing()) {
    LOG_INFO("other threads already written pq centroids cache, skip", K(ret));
  } else {
    const ObDASScanCtDef *pq_ctdef = vec_index_scan_ctdef_->get_special_aux_table_ctdef();
    if (OB_ISNULL(pq_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("special aux (pq centroid) ctdef is null", K(ret));
    } else if (OB_FAIL(do_special_aux_table_full_scan(is_vectorized))) {
      LOG_WARN("failed to do pq centroid table full scan", K(ret));
    } else {
      ObArenaAllocator tmp_allocator;
      ObPqCenterId pq_cent_id;

      if (is_vectorized) {
        IVF_GET_NEXT_ROWS_BEGIN(special_aux_iter_)
        if (OB_SUCC(ret)) {
          ObEvalCtx::BatchInfoScopeGuard guard(*vec_index_scan_rtdef_->eval_ctx_);
          guard.set_batch_size(scan_row_cnt);
          ObExpr *cid_expr = pq_ctdef->result_output_[CID_IDX];
          ObExpr *cid_vec_expr = pq_ctdef->result_output_[CID_VECTOR_IDX];
          ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;

          for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
            guard.set_batch_idx(i);
            bool cid_is_null = true;
            bool vec_is_null = true;
            ObString cid;
            ObString cid_vec;
            if (OB_FAIL(read_ivf_cid_string_at(*cid_expr, *eval_ctx, tmp_allocator, i, cid_is_null, cid))) {
              LOG_WARN("failed to read pq cid", K(ret), K(i));
            } else if (cid_is_null || cid.empty()) {
              // skip null cid
            } else if (OB_FAIL(read_ivf_vec_string_at(*cid_vec_expr, *eval_ctx,
                    tmp_allocator, i, vec_is_null, cid_vec))) {
              LOG_WARN("failed to get real data", K(ret));
            } else if (vec_is_null) {
              // skip null vector
            } else if (OB_FAIL(ObVectorClusterHelper::get_pq_center_id_from_string(pq_cent_id, cid,
                ObVectorClusterHelper::IVF_PARSE_M_ID | ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
              LOG_WARN("fail to get pq center id from string", K(ret), KPHEX(cid.ptr(), cid.length()));
            } else if (OB_FAIL(cent_cache.write_pq_centroid(
                  pq_cent_id.m_id_, pq_cent_id.center_id_,
                  reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
              LOG_WARN("fail to write pq centroid", K(ret), K(pq_cent_id),
                       "cid_vec_len", cid_vec.length(), K(special_aux_tablet_id_));
            }
          }
        }
        IVF_GET_NEXT_ROWS_END(special_aux_iter_, special_aux_scan_param_, special_aux_tablet_id_)
      } else {
        special_aux_iter_->clear_evaluated_flag();
        ObExpr *cid_expr = pq_ctdef->result_output_[CID_IDX];
        ObExpr *vec_expr = pq_ctdef->result_output_[CID_VECTOR_IDX];
        ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;
        for (int i = 0; OB_SUCC(ret); ++i) {
          if (OB_FAIL(special_aux_iter_->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to scan pq centroid iter", K(ret));
            }
          } else {
            bool cid_is_null = true;
            bool vec_is_null = true;
            ObString cid;
            ObString cid_vec;
            if (OB_FAIL(read_ivf_cid_string_at(*cid_expr, *eval_ctx, tmp_allocator, 0, cid_is_null, cid))) {
              LOG_WARN("failed to read pq cid", K(ret), K(i));
            } else if (cid_is_null || cid.empty()) {
              // skip null cid
            } else if (OB_FAIL(read_ivf_vec_string_at(*vec_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), 0, vec_is_null, cid_vec))) {
              LOG_WARN("failed to get real data", K(ret));
            } else if (vec_is_null) {
              // skip null vector
            } else if (OB_FAIL(ObVectorClusterHelper::get_pq_center_id_from_string(pq_cent_id, cid,
                ObVectorClusterHelper::IVF_PARSE_M_ID | ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
              LOG_WARN("fail to get pq center id from string", K(ret), KPHEX(cid.ptr(), cid.length()));
            } else if (OB_FAIL(cent_cache.write_pq_centroid(
                  pq_cent_id.m_id_, pq_cent_id.center_id_,
                  reinterpret_cast<float*>(cid_vec.ptr()), cid_vec.length()))) {
              LOG_WARN("fail to write pq centroid", K(ret), K(pq_cent_id),
                       "cid_vec_len", cid_vec.length(), K(special_aux_tablet_id_));
            }
          }
        }
        int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
        if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, special_aux_iter_,
                                                   special_aux_scan_param_, special_aux_tablet_id_))) {
          LOG_WARN("failed to reuse special aux iter", K(ret));
        } else {
          ret = tmp_ret;
        }
      }

      if (OB_SUCC(ret)) {
        if (cent_cache.get_count() > 0) {
          cent_cache.set_completed();
          LOG_INFO("success to write pq centroid cache", K(special_aux_tablet_id_), K(cent_cache.get_count()));
        } else {
          cent_cache.reuse();
          LOG_DEBUG("Empty pq centroid table, no need to set cache", K(special_aux_tablet_id_));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    cent_cache.reuse();
  }
  return ret;
}

// ======================== PQ center scanning ========================

int ObDASVecIndexIVFPQScanIter::get_nearest_probe_center_ids_with_vec(
    bool is_vectorized,
    bool force_persistent_scan)
{
  int ret = OB_SUCCESS;
  common::ScopedTimer nlist_timer(common::ObMetricId::VEC_IVF_SEARCH_NLIST_TIME,
                                  profile_);
  const ObDASScanCtDef *centroid_ctdef = vec_index_scan_ctdef_->get_centroid_table_ctdef();
  // PQ uses EUCLIDEAN_SQUARED for center distance (precompute table compatibility)
  ObExprVectorDistance::ObVecDisType cur_dis_type = dis_type_;
  if (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    cur_dis_type = ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED;
  }

  if (OB_ISNULL(centroid_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("centroid ctdef is null", K(ret));
  } else {
    share::ObVectorCenterClusterHelper<float, ObCenterId> nearest_cid_heap(
        mem_context_->get_arena_allocator(),
        reinterpret_cast<const float *>(real_search_vec_.ptr()),
        cur_dis_type, dim_, nprobes_, 0.0,
        true /*is_save_all_center*/, &iterative_filter_ctx_.allocator_);

    // Try cache path first, fallback to table scan
    share::ObIvfICache *cent_cache = nullptr;
    bool is_cache_usable = false;
    if (!force_persistent_scan
        && OB_FAIL(get_centers_cache(is_vectorized, false/*is_pq*/, center_cache_guard_,
                                     cent_cache, is_cache_usable))) {
      LOG_WARN("failed to get centers cache", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (!force_persistent_scan && is_cache_usable && OB_NOT_NULL(cent_cache)) {
      if (OB_FAIL(gen_near_cid_heap_from_cache(*cent_cache, nearest_cid_heap))) {
        LOG_WARN("failed to gen near cid heap from cache", K(ret));
      }
    } else {
      const common::ObMetricId fallback_metric_id =
          is_ns_id_valid()
              ? common::ObMetricId::VEC_IVF_MAP_CENTROID_CACHE_FALLBACK_SCAN_TIME
              : common::ObMetricId::VEC_IVF_CENTROID_CACHE_FALLBACK_SCAN_TIME;
      common::ScopedTimer fallback_scan_timer(fallback_metric_id, profile_);
      if (OB_FAIL(do_centroid_table_full_scan(is_vectorized))) {
        LOG_WARN("failed to do centroid table full scan", K(ret));
      } else {
      ObExpr *cid_expr = centroid_ctdef->result_output_[CID_IDX];
      ObExpr *cid_vec_expr = centroid_ctdef->result_output_[CID_VECTOR_IDX];
      ObCenterId center_id;
      ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;

      if (is_vectorized) {
        IVF_GET_NEXT_ROWS_BEGIN(centroid_iter_)
        if (OB_SUCC(ret)) {
          ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
          guard.set_batch_size(scan_row_cnt);

          for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
            guard.set_batch_idx(i);
            bool cid_is_null = true;
            bool vec_is_null = true;
            ObString cid;
            ObString cid_vec;
            if (OB_FAIL(read_ivf_cid_string_at(*cid_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), i, cid_is_null, cid))) {
              LOG_WARN("failed to read cid", K(ret), K(i));
            } else if (cid_is_null || cid.empty()) {
              // skip null cid
            } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, cid))) {
              LOG_WARN("failed to get center id from string", K(ret));
            } else if (OB_FAIL(read_ivf_vec_string_at(*cid_vec_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), i, vec_is_null, cid_vec))) {
              LOG_WARN("failed to get real data", K(ret));
            } else if (vec_is_null || cid_vec.empty()) {
              // skip null vector
            } else if (OB_FAIL(nearest_cid_heap.push_center(center_id,
                                                              reinterpret_cast<float *>(cid_vec.ptr()),
                                                              dim_))) {
              LOG_WARN("failed to push center", K(ret));
            }
          }
        }
        IVF_GET_NEXT_ROWS_END(centroid_iter_, centroid_scan_param_, centroid_tablet_id_)
      } else {
        centroid_iter_->clear_evaluated_flag();

        for (int i = 0; OB_SUCC(ret); ++i) {
          if (OB_FAIL(centroid_iter_->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next centroid row", K(ret));
            }
          } else {
            bool cid_is_null = true;
            bool vec_is_null = true;
            ObString cid;
            ObString cid_vec;
            if (OB_FAIL(read_ivf_cid_string_at(*cid_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), 0, cid_is_null, cid))) {
              LOG_WARN("failed to read cid", K(ret), K(i));
            } else if (cid_is_null || cid.empty()) {
              // skip null cid
            } else if (OB_FAIL(read_ivf_vec_string_at(*cid_vec_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), 0, vec_is_null, cid_vec))) {
              LOG_WARN("failed to get real data", K(ret));
            } else if (vec_is_null || cid_vec.empty()) {
              // skip null vector
            } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(center_id, cid))) {
              LOG_WARN("failed to get center id from string", K(ret));
            } else if (OB_FAIL(nearest_cid_heap.push_center(center_id,
                                                              reinterpret_cast<float *>(cid_vec.ptr()),
                                                              dim_))) {
              LOG_WARN("failed to push center", K(ret));
            }
          }
        }

        if (OB_ITER_END == ret) {
          if (OB_FAIL(centroid_iter_->reuse())) {
            LOG_WARN("fail to reuse centroid iter", K(ret));
          }
        }
      }
      }
    }

    if (OB_SUCC(ret)) {
      if (nearest_cid_heap.get_center_count() == 0) {
        if (!force_persistent_scan && is_cache_usable) {
          LOG_INFO("[IVF_PQ] no visible centroid in cache, fallback to persistent scan",
                   K(ns_id_), K(ltable_id_), K(ls_id_), K(centroid_tablet_id_));
          if (OB_FAIL(get_nearest_probe_center_ids_with_vec(is_vectorized, true))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              LOG_WARN("[IVF_PQ] failed to fallback to persistent centroid scan", K(ret));
            }
          }
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("[IVF_PQ] no centroids found", K(ret), K(force_persistent_scan));
        }
      } else if (nearest_cid_heap.is_save_all_center()) {
        if (OB_FAIL(nearest_cid_heap.get_all_centroids(iterative_filter_ctx_.centroids_))) {
          LOG_WARN("failed to get all centroids", K(ret));
        } else if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_centers_vec_dist(
                       nprobes_, near_cid_vec_, near_cid_vec_dis_))) {
          LOG_WARN("failed to get initial center ids with vec dist", K(ret));
        }
      } else if (OB_FAIL(nearest_cid_heap.get_nearest_probe_centers_vec_dist(
                     near_cid_vec_, near_cid_vec_dis_))) {
        LOG_WARN("failed to get nearest center ids with vec dist", K(ret));
      }
    }
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::get_next_center_ids_with_vec(bool is_vectorized, int64_t next_nprobe)
{
  int ret = OB_SUCCESS;
  common::ScopedTimer nlist_timer(common::ObMetricId::VEC_IVF_SEARCH_NLIST_TIME,
                                  profile_);
  UNUSED(is_vectorized);
  near_cid_vec_.reuse();
  near_cid_vec_dis_.reuse();
  if (OB_FAIL(iterative_filter_ctx_.get_next_nearest_probe_centers_vec_dist(
                  next_nprobe, near_cid_vec_, near_cid_vec_dis_))) {
    LOG_WARN("failed to get next center ids with vec dist", K(ret));
  }
  return ret;
}

// ======================== PQ distance computation core ========================

int ObDASVecIndexIVFPQScanIter::calc_nearest_limit_rowkeys_in_cids(
    bool is_vectorized,
    float *search_vec,
    ObIvfRowkeyHeap &nearest_rowkey_heap)
{
  int ret = OB_SUCCESS;
  const int64_t candidate_rows_before = cid_vec_scan_rows_;
  common::ScopedTimer candidate_scan_timer(common::ObMetricId::VEC_IVF_CANDIDATE_SCAN_TIME,
                                           profile_);

  int64_t sub_dim = dim_ / m_;
  int64_t ksub = 1L << nbits_;
  const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
  ObDASScanRtDef *cid_vec_rtdef = vec_index_scan_rtdef_->get_cid_vec_table_rtdef();
  int64_t cid_vec_column_count = 0;
  int64_t cid_vec_pri_key_cnt = 0;
  int64_t rowkey_cnt = 0;
  ObArray<float *> splited_residual;
  int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
  char *buf = nullptr;
  ObString cid_str;
  float *residual = nullptr;
  bool pre_compute_table = false;
  share::ObIvfCacheMgrGuard pre_cache_guard;
  share::ObIvfICache *pre_cent_cache = nullptr;
  float *sim_table = nullptr;
  float *sim_table_2 = nullptr;
  const float *sim_table_ptrs = nullptr;
  ObRowkey filter_main_rowkey;
  bool is_l2 = (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN);

  if (OB_FAIL(prepare_cid_range(cid_vec_ctdef, cid_vec_column_count, cid_vec_pri_key_cnt, rowkey_cnt))) {
    LOG_WARN("fail to prepare cid range", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(mem_context_->get_arena_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc cid", K(ret));
  } else if (OB_FALSE_IT(cid_str.assign_buffer(buf, buf_len))) {
  } else if (OB_FAIL(check_can_pre_compute(is_vectorized, pre_cache_guard, pre_cent_cache, pre_compute_table))) {
    LOG_WARN("fail to check can use precomputetable", K(ret));
  } else if (!pre_compute_table) {
    char *residual_buf = nullptr;
    if (OB_FAIL(splited_residual.reserve(m_))) {
      LOG_WARN("fail to init splited residual array", K(ret), K_(m));
    } else if (is_l2) {
      if (OB_ISNULL(residual_buf = static_cast<char *>(mem_context_->get_arena_allocator().alloc(dim_ * sizeof(float))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc residual buf", K(ret));
      } else if (OB_FALSE_IT(residual = new (residual_buf) float[dim_])) {
      }
    }
  } else {
    // precompute table path
    ObObj *obj_ptr = nullptr;
    sim_table = (float *)mem_context_->get_arena_allocator().alloc(sizeof(float) * (ksub * m_) * 2);
    obj_ptr = (ObObj *)mem_context_->get_arena_allocator().alloc(sizeof(ObObj) * rowkey_cnt);
    if (OB_ISNULL(sim_table) || OB_ISNULL(obj_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for precompute table", K(ret), K(ksub), K_(m), K(rowkey_cnt));
    } else {
      filter_main_rowkey.assign(obj_ptr, rowkey_cnt);
      sim_table_2 = sim_table + (ksub * m_);
      if (is_l2 && OB_FAIL(pre_compute_inner_prod_table(search_vec, sim_table_2, is_vectorized))) {
        LOG_WARN("fail to pre compute inner prod table", K(ret));
      } else if (!is_l2 && OB_FAIL(pre_compute_inner_prod_table(search_vec, sim_table, is_vectorized))) {
        LOG_WARN("fail to pre compute inner prod table", K(ret));
      }
    }
  }

  // for every (cid, cid_vec)
  for (int i = 0; OB_SUCC(ret) && i < near_cid_vec_.count(); ++i) {
    const ObCenterId &cur_cid = near_cid_vec_.at(i).first;
    float *cur_cid_vec = near_cid_vec_.at(i).second;
    float dis0 = 0.0f;

    if (pre_compute_table) {
      if (is_l2) {
        sim_table_ptrs = pre_cent_cache->get_centroids() + (cur_cid.center_id_ - 1) * ksub * m_;
        ObVectorL2Distance<float>::fvec_madd(m_ * ksub, sim_table_ptrs, -2.0, sim_table_2, sim_table);
      }
      dis0 = near_cid_vec_dis_.at(i);
    } else {
      // compute residual r(x) = search_vec - cid_vec, split into m parts
      splited_residual.reuse();
      if (is_l2) {
        if (OB_FAIL(ObVectorIndexUtil::calc_residual_vector(dim_, search_vec, cur_cid_vec, residual))) {
          LOG_WARN("fail to calc residual vector", K(ret), K_(dim));
        }
      } else {
        dis0 = near_cid_vec_dis_.at(i);
        residual = search_vec; // IP: dis = dis0 + search_vec · pq_vec
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObVectorIndexUtil::split_vector(m_, dim_, residual, splited_residual))) {
        LOG_WARN("fail to split vector", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      // scan cid_vec table for this center
      cid_str.assign_buffer(buf, buf_len);
      if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(cur_cid, cid_str))) {
        LOG_WARN("failed to set center_id to string", K(ret), K(cur_cid));
      } else if (OB_FAIL(scan_cid_range(cid_str, cid_vec_pri_key_cnt, cid_vec_ctdef, cid_vec_rtdef))) {
        LOG_WARN("fail to scan cid range", K(ret), K(cur_cid), K(cid_vec_pri_key_cnt));
      } else if (is_vectorized) {
        IVF_GET_NEXT_ROWS_BEGIN(cid_vec_iter_)
        if (OB_SUCC(ret)) {
          ObEvalCtx::BatchInfoScopeGuard guard(*vec_index_scan_rtdef_->eval_ctx_);
          guard.set_batch_size(scan_row_cnt);
          ObExpr *pq_ids_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
          ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;
          cid_vec_scan_rows_ += scan_row_cnt;

          common::ScopedTimer distance_timer(
              common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME);
          common::ScopedTimer fallback_distance_timer(
              common::ObMetricId::VEC_IVF_PQ_PRECOMPUTE_CACHE_FALLBACK_CALC_TIME,
              pre_compute_table ? nullptr : profile_);
          if (pre_compute_table) {
            if (OB_FAIL(calc_distance_with_precompute(guard, scan_row_cnt, rowkey_cnt,
                                                       filter_main_rowkey, sim_table, dis0,
                                                       nearest_rowkey_heap))) {
              LOG_WARN("fail to calc distance with precompute table", K(ret));
            }
          } else {
            ObExpr *pk_expr = cid_vec_ctdef->result_output_.at(CID_VEC_ROWKEY_START_IDX);
            for (int64_t j = 0; OB_SUCC(ret) && j < scan_row_cnt; ++j) {
              guard.set_batch_idx(j);
              bool pq_is_null = true;
              ObString pq_center_ids;
              if (OB_FAIL(read_ivf_pq_ids_string_at(*pq_ids_expr, *eval_ctx,
                      mem_context_->get_arena_allocator(), j, pq_is_null, pq_center_ids))) {
                LOG_WARN("failed to read pq ids", K(ret), K(j));
              } else if (pq_is_null || pq_center_ids.empty()) {
                // skip null
              } else {
                int64_t vid_val = 0;
                if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr, *eval_ctx, j, vid_val))) {
                  LOG_WARN("failed to read pk", K(ret), K(j));
                } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
                  // skip rows filtered out by pre-filter bitmap
                } else {
                  ObRowkey main_rowkey;
                  float distance = 0;
                  ObObj rk_obj;
                  if (OB_FAIL(read_ivf_rowkey_at(*pk_expr, *eval_ctx, j, main_rowkey, rk_obj))) {
                    LOG_WARN("failed to read rowkey", K(ret), K(j));
                  } else if (OB_FAIL(calc_distance_between_pq_ids(is_vectorized, pq_center_ids,
                                                                    splited_residual, distance))) {
                    LOG_WARN("fail to calc distance between pq ids", K(ret));
                  } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance + dis0))) {
                    LOG_WARN("failed to push center", K(ret));
                  }
                }
              }
            }
          }
        }
        IVF_GET_NEXT_ROWS_END(cid_vec_iter_, cid_vec_scan_param_, cid_vec_tablet_id_)
      } else {
        // non-vectorized path
        cid_vec_iter_->clear_evaluated_flag();
        ObExpr *pq_ids_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
        ObExpr *pk_expr = cid_vec_ctdef->result_output_.at(CID_VEC_ROWKEY_START_IDX);
        ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;

        common::ScopedTimer distance_timer(
            common::ObMetricId::VEC_IVF_CANDIDATE_DISTANCE_CAL_TIME);
        common::ScopedTimer fallback_distance_timer(
            common::ObMetricId::VEC_IVF_PQ_PRECOMPUTE_CACHE_FALLBACK_CALC_TIME,
            pre_compute_table ? nullptr : profile_);
        while (OB_SUCC(ret)) {
          cid_vec_scan_rows_++;
          if (OB_FAIL(cid_vec_iter_->get_next_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to scan cid_vec iter", K(ret));
            }
          } else {
            bool pq_is_null = true;
            ObString pq_center_ids;
            if (OB_FAIL(read_ivf_pq_ids_string_at(*pq_ids_expr, *eval_ctx,
                    mem_context_->get_arena_allocator(), 0, pq_is_null, pq_center_ids))) {
              LOG_WARN("failed to read pq ids", K(ret));
            } else if (pq_is_null || pq_center_ids.empty()) {
              // skip null
            } else {
              int64_t vid_val = 0;
              if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr, *eval_ctx, 0, vid_val))) {
                LOG_WARN("failed to read pk", K(ret));
              } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
                // skip rows filtered out by pre-filter bitmap
              } else {
                ObRowkey main_rowkey;
                float distance = 0.0f;
                ObObj rk_obj;
                if (OB_FAIL(read_ivf_rowkey_at(*pk_expr, *eval_ctx, 0, main_rowkey, rk_obj))) {
                  LOG_WARN("failed to read rowkey", K(ret));
                } else if (pre_compute_table) {
                  const uint8_t *pq_id_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
                  distance = ObVectorL2Distance<float>::distance_one_code(m_, nbits_, sim_table, pq_id_ptr);
                } else if (OB_FAIL(calc_distance_between_pq_ids(is_vectorized, pq_center_ids,
                                                                  splited_residual, distance))) {
                  LOG_WARN("fail to calc distance between pq ids", K(ret));
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, distance + dis0))) {
                  LOG_WARN("failed to push center", K(ret));
                }
              }
            }
          }
        }

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(cid_vec_iter_->reuse())) {
            LOG_WARN("fail to reuse cid_vec iter", K(ret));
          }
        }
      }
    }
  }

  const int64_t candidate_rows = cid_vec_scan_rows_ - candidate_rows_before;
  if (OB_NOT_NULL(profile_)) {
    common::ObProfileSwitcher algorithm_switcher(profile_);
    if (OB_SUCC(ret) && candidate_rows > 0) {
      INC_METRIC_VAL(common::ObMetricId::VEC_IVF_CANDIDATE_ROWS_SCANNED, candidate_rows);
    }
  }

  return ret;
}

// ======================== 3 PQ distance paths ========================

int ObDASVecIndexIVFPQScanIter::calc_distance_between_pq_ids(
    bool is_vectorized,
    const ObString &pq_center_ids,
    const ObIArray<float *> &splited_residual,
    float &distance)
{
  int ret = OB_SUCCESS;
  share::ObIvfCacheMgrGuard cache_guard;
  share::ObIvfICache *cent_cache = nullptr;
  bool is_cache_usable = false;

  if (OB_FAIL(get_centers_cache(is_vectorized, true /*is_pq_centers*/, cache_guard, cent_cache, is_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret));
  } else if (is_cache_usable) {
    if (OB_FAIL(calc_distance_between_pq_ids_by_cache(*cent_cache, pq_center_ids, splited_residual, distance))) {
      LOG_WARN("fail to calc distance by cache", K(ret));
    }
  } else {
    if (OB_FAIL(calc_distance_between_pq_ids_by_table(is_vectorized, pq_center_ids, splited_residual,
                                                        ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE, distance))) {
      LOG_WARN("fail to calc distance by table", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexIVFPQScanIter::calc_distance_between_pq_ids_by_cache(
    share::ObIvfICache &cent_cache,
    const ObString &pq_center_ids,
    const ObIArray<float *> &splited_residual,
    float &distance)
{
  int ret = OB_SUCCESS;
  RWLock::RLockGuard guard(cent_cache.get_lock());
  float square = 0.0f;
  const uint8_t *pq_ids_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
  PQDecoderGeneric decoder(pq_ids_ptr, nbits_);

  for (int j = 0; OB_SUCC(ret) && j < m_; ++j) {
    float *pq_cid_vec = nullptr;
    if (OB_FAIL(cent_cache.read_pq_centroid(j + 1, decoder.decode() + 1, pq_cid_vec))) {
      LOG_WARN("fail to get pq cid vec from cache", K(ret), K(j + 1));
    } else if (OB_ISNULL(pq_cid_vec)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null pq_cid_vec", K(ret), K(j));
    } else {
      float dis = 0.0f;
      if (OB_FAIL(calc_vec_dis(splited_residual.at(j), pq_cid_vec, dim_ / m_, dis, dis_type_))) {
        SHARE_LOG(WARN, "failed to calc vec dis", K(ret));
      } else {
        square += dis;
      }
    }
  }

  if (OB_SUCC(ret)) {
    distance = square;
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::calc_distance_between_pq_ids_by_table(
    bool is_vectorized,
    const ObString &pq_center_ids,
    const ObIArray<float *> &splited_residual,
    int64_t batch_row_count,
    float &distance)
{
  int ret = OB_SUCCESS;
  float dis_square = 0.0f;
  const ObDASScanCtDef *pq_cid_vec_ctdef = vec_index_scan_ctdef_->get_special_aux_table_ctdef();
  ObDASScanRtDef *pq_cid_vec_rtdef = vec_index_scan_rtdef_->get_special_aux_table_rtdef();
  common::ScopedTimer fallback_scan_timer(
      common::ObMetricId::VEC_IVF_PQ_CENTROID_CACHE_FALLBACK_SCAN_TIME,
      OB_NOT_NULL(pq_cid_vec_ctdef) && OB_NOT_NULL(pq_cid_vec_rtdef) ? profile_ : nullptr);

  if (OB_ISNULL(pq_cid_vec_ctdef) || OB_ISNULL(pq_cid_vec_rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("special aux ctdef or rtdef is null", K(ret), KP(pq_cid_vec_ctdef), KP(pq_cid_vec_rtdef));
  }

  int count = 0;
  int pq_cid_idx = 0;
  int64_t pq_cid_count = m_;
  uint64_t tablet_id = ObVecIVFPQCenterIDS::get_tablet_id(pq_center_ids.ptr());
  const uint8_t *pq_ids_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(pq_center_ids.ptr());
  PQDecoderGeneric decoder(pq_ids_ptr, nbits_);

  while (OB_SUCC(ret) && count < pq_cid_count) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_row_count && count < pq_cid_count; ++i, ++count) {
      ObRowkey pq_cid_rowkey;
      ObString pq_center_id_str;
      ObPqCenterId pq_center_id(tablet_id, count + 1, decoder.decode() + 1);
      if (OB_FAIL(ObVectorClusterHelper::set_pq_center_id_to_string(
                      pq_center_id, pq_center_id_str, &mem_context_->get_arena_allocator()))) {
        LOG_WARN("fail to set pq center id to string", K(ret), K(pq_center_id));
      } else if (OB_FAIL(build_cid_vec_query_rowkey(pq_center_id_str, true /*is_min*/, CENTROID_PRI_KEY_CNT,
                                                      pq_cid_rowkey))) {
        LOG_WARN("failed to build cid vec query rowkey", K(ret));
      } else if (OB_FAIL(ObDasVecScanUtils::set_lookup_key(pq_cid_rowkey, special_aux_scan_param_,
                                                             pq_cid_vec_ctdef->ref_table_id_))) {
        LOG_WARN("failed to set lookup key", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_aux_table_scan(special_aux_iter_first_scan_, special_aux_scan_param_,
                                          pq_cid_vec_ctdef, pq_cid_vec_rtdef,
                                          special_aux_iter_, special_aux_tablet_id_))) {
      LOG_WARN("fail to rescan special aux table scan iterator", K(ret));
    } else if (is_vectorized) {
      IVF_GET_NEXT_ROWS_BEGIN(special_aux_iter_)
      if (OB_SUCC(ret)) {
        ObEvalCtx::BatchInfoScopeGuard guard(*vec_index_scan_rtdef_->eval_ctx_);
        guard.set_batch_size(scan_row_cnt);
        ObExpr *vec_expr = pq_cid_vec_ctdef->result_output_[PQ_CENTROID_VEC_IDX];
        ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;

        for (int64_t i = 0; OB_SUCC(ret) && i < scan_row_cnt; ++i) {
          float cur_dis = 0.0f;
          guard.set_batch_idx(i);
          bool vec_is_null = true;
          ObString vec;
          if (OB_FAIL(read_ivf_vec_string_at(*vec_expr, *eval_ctx,
                  mem_context_->get_arena_allocator(), i, vec_is_null, vec))) {
            LOG_WARN("failed to get real data", K(ret));
          } else if (vec_is_null || OB_ISNULL(vec.ptr())) {
            pq_cid_idx++;
          } else if (OB_FAIL(calc_vec_dis(splited_residual.at(pq_cid_idx),
                                                   reinterpret_cast<float *>(vec.ptr()),
                                                   dim_ / m_, cur_dis, dis_type_))) {
            SHARE_LOG(WARN, "failed to calc vec dis", K(ret));
          } else {
            dis_square += cur_dis;
            pq_cid_idx++;
          }
        }
      }
      IVF_GET_NEXT_ROWS_END(special_aux_iter_, special_aux_scan_param_, special_aux_tablet_id_)
    } else {
      special_aux_iter_->clear_evaluated_flag();
      ObExpr *vec_expr = pq_cid_vec_ctdef->result_output_[PQ_CENTROID_VEC_IDX];
      ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;
      for (int i = 0; OB_SUCC(ret) && i < batch_row_count; ++i) {
        float cur_dis = 0.0f;
        if (OB_FAIL(special_aux_iter_->get_next_row())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to scan special aux iter", K(ret));
          }
        } else {
          bool vec_is_null = true;
          ObString vec;
          if (OB_FAIL(read_ivf_vec_string_at(*vec_expr, *eval_ctx,
                  mem_context_->get_arena_allocator(), 0, vec_is_null, vec))) {
            LOG_WARN("failed to get real data", K(ret));
          } else if (vec_is_null || OB_ISNULL(vec.ptr())) {
            pq_cid_idx++;
          } else if (OB_FAIL(calc_vec_dis(splited_residual.at(pq_cid_idx),
                                                   reinterpret_cast<float *>(vec.ptr()),
                                                   dim_ / m_, cur_dis, dis_type_))) {
            SHARE_LOG(WARN, "failed to calc vec dis", K(ret));
          } else {
            dis_square += cur_dis;
            pq_cid_idx++;
          }
        }
      }
      int tmp_ret = (ret == OB_ITER_END) ? OB_SUCCESS : ret;
      if (OB_FAIL(ObDasVecScanUtils::reuse_iter(ls_id_, special_aux_iter_,
                                                  special_aux_scan_param_, special_aux_tablet_id_))) {
        LOG_WARN("failed to reuse special aux iter", K(ret));
      } else {
        ret = tmp_ret;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (pq_cid_idx != pq_cid_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pq cid idx", K(ret), K(pq_cid_idx), K(pq_cid_count));
    } else {
      distance = dis_square;
    }
  }
  return ret;
}

// ======================== Precompute table support ========================

int ObDASVecIndexIVFPQScanIter::check_can_pre_compute(
    bool is_vectorized,
    share::ObIvfCacheMgrGuard &pre_cache_guard,
    share::ObIvfICache *&pre_cent_cache,
    bool &pre_compute_table)
{
  int ret = OB_SUCCESS;
  pre_compute_table = false;
  if (dis_type_ == ObExprVectorDistance::ObVecDisType::EUCLIDEAN) {
    if (OB_FAIL(get_pq_precomputetable_cache(is_vectorized, pre_cache_guard, pre_cent_cache, pre_compute_table))) {
      LOG_WARN("fail to get pq precompute table cache", K(ret));
    }
  } else if (dis_type_ == ObExprVectorDistance::ObVecDisType::DOT) {
    share::ObIvfCacheMgrGuard pq_center_guard;
    share::ObIvfICache *pq_cent_cache = nullptr;
    bool is_pq_cent_cache_usable = false;
    if (OB_FAIL(get_centers_cache(is_vectorized, true /*is_pq_centers*/, pq_center_guard,
                                   pq_cent_cache, is_pq_cent_cache_usable))) {
      LOG_WARN("fail to get centers cache", K(ret));
    } else if (is_pq_cent_cache_usable) {
      pre_compute_table = true;
    }
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::pre_compute_inner_prod_table(
    const float *search_vec,
    float *dis_table,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  int64_t ksub = 1L << nbits_;
  int64_t sub_dim = dim_ / m_;
  share::ObIvfCacheMgrGuard pq_center_guard;
  share::ObIvfICache *pq_cent_cache = nullptr;
  bool is_pq_cent_cache_usable = false;

  if (OB_FAIL(get_centers_cache(is_vectorized, true /*is_pq_centers*/, pq_center_guard,
                                 pq_cent_cache, is_pq_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret));
  } else if (is_pq_cent_cache_usable) {
    float *pq_center = pq_cent_cache->get_centroids();
    for (int i = 0; i < m_; i++) {
      ObVectorIpDistance<float>::fvec_inner_products_ny(
          dis_table + i * ksub,
          search_vec + i * sub_dim,
          pq_center + i * ksub * sub_dim,
          sub_dim, ksub);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get pq center cache for precompute", K(ret));
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::calc_distance_with_precompute(
    ObEvalCtx::BatchInfoScopeGuard &guard,
    int64_t scan_row_cnt,
    int64_t rowkey_cnt,
    ObRowkey &filter_main_rowkey,
    float *sim_table,
    float dis0,
    ObIvfRowkeyHeap &nearest_rowkey_heap)
{
  int ret = OB_SUCCESS;
  const ObDASScanCtDef *cid_vec_ctdef = vec_index_scan_ctdef_->get_cid_vec_table_ctdef();
  ObExpr *pq_ids_expr = cid_vec_ctdef->result_output_[PQ_IDS_IDX];
  ObExpr *pk_expr_for_bitmap = cid_vec_ctdef->result_output_.at(CID_VEC_ROWKEY_START_IDX);
  ObEvalCtx *eval_ctx = vec_index_scan_rtdef_->eval_ctx_;
  ObIAllocator &allocator = mem_context_->get_arena_allocator();
  int counter = 0;
  size_t saved_j[4] = {0, 0, 0, 0};
  ObString saved_pq_ids[4];

  for (int64_t j = 0; OB_SUCC(ret) && j < scan_row_cnt; ++j) {
    guard.set_batch_idx(j);
    bool pq_is_null = true;
    ObString pq_ids_str;
    if (OB_FAIL(read_ivf_pq_ids_string_at(*pq_ids_expr, *eval_ctx, allocator, j, pq_is_null, pq_ids_str))) {
      LOG_WARN("failed to read pq ids", K(ret), K(j));
    } else if (pq_is_null || pq_ids_str.empty()) {
      // skip null
    } else {
      int64_t vid_val = 0;
      if (OB_FAIL(read_ivf_pk_int64_at(*pk_expr_for_bitmap, *eval_ctx, j, vid_val))) {
        LOG_WARN("failed to read pk", K(ret), K(j));
      } else if (OB_NOT_NULL(bitmap_) && !bitmap_->test(vid_val)) {
        // skip rows filtered out by pre-filter bitmap
      } else {
        saved_j[0] = (counter == 0) ? j : saved_j[0];
        saved_j[1] = (counter == 1) ? j : saved_j[1];
        saved_j[2] = (counter == 2) ? j : saved_j[2];
        saved_j[3] = (counter == 3) ? j : saved_j[3];
        saved_pq_ids[counter] = pq_ids_str;
        counter += 1;
        if (counter == 4) {
          const uint8_t *pq_id_ptr_0 = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_pq_ids[0].ptr());
          const uint8_t *pq_id_ptr_1 = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_pq_ids[1].ptr());
          const uint8_t *pq_id_ptr_2 = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_pq_ids[2].ptr());
          const uint8_t *pq_id_ptr_3 = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_pq_ids[3].ptr());
          float distance_0 = 0, distance_1 = 0, distance_2 = 0, distance_3 = 0;
          ObVectorL2Distance<float>::distance_four_codes(
              m_, nbits_, sim_table, pq_id_ptr_0, pq_id_ptr_1, pq_id_ptr_2, pq_id_ptr_3,
              distance_0, distance_1, distance_2, distance_3);
          distance_0 += dis0;
          distance_1 += dis0;
          distance_2 += dis0;
          distance_3 += dis0;

          // push each result to heap
          for (int k = 0; OB_SUCC(ret) && k < 4; ++k) {
            float dist = (k == 0) ? distance_0 : (k == 1) ? distance_1 : (k == 2) ? distance_2 : distance_3;
            if (nearest_rowkey_heap.should_push_center(dist)) {
              guard.set_batch_idx(saved_j[k]);
              ObRowkey main_rowkey;
              ObObj rk_obj;
              ObExpr *pk_expr = cid_vec_ctdef->result_output_.at(CID_VEC_ROWKEY_START_IDX);
              if (OB_FAIL(read_ivf_rowkey_at(*pk_expr, *eval_ctx, saved_j[k], main_rowkey, rk_obj))) {
                LOG_WARN("failed to read rowkey", K(ret), K(saved_j[k]));
              } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, dist))) {
                LOG_WARN("failed to push center", K(ret));
              }
            }
          }
          counter = 0;
        }
      }
    }
  }

  // process remaining (counter < 4)
  for (size_t kk = 0; kk < counter && OB_SUCC(ret); kk++) {
    const uint8_t *pq_id_ptr = ObVecIVFPQCenterIDS::get_pq_id_ptr(saved_pq_ids[kk].ptr());
    float dis = dis0 + ObVectorL2Distance<float>::distance_one_code(m_, nbits_, sim_table, pq_id_ptr);
    if (nearest_rowkey_heap.should_push_center(dis)) {
      guard.set_batch_idx(saved_j[kk]);
      ObRowkey main_rowkey;
      ObObj rk_obj;
      ObExpr *pk_expr = cid_vec_ctdef->result_output_.at(CID_VEC_ROWKEY_START_IDX);
      if (OB_FAIL(read_ivf_rowkey_at(*pk_expr, *eval_ctx, saved_j[kk], main_rowkey, rk_obj))) {
        LOG_WARN("failed to read rowkey", K(ret), K(saved_j[kk]));
      } else if (OB_FAIL(nearest_rowkey_heap.push_center(main_rowkey, dis))) {
        LOG_WARN("failed to push center", K(ret));
      }
    }
  }
  return ret;
}

// ======================== Cache helpers ========================
// get_centers_cache() is inherited from ObDASVecIndexIVFBaseScanIter

int ObDASVecIndexIVFPQScanIter::get_pq_precomputetable_cache(
    bool is_vectorized,
    share::ObIvfCacheMgrGuard &cache_guard,
    share::ObIvfICache *&cent_cache,
    bool &is_cache_usable)
{
  int ret = OB_SUCCESS;
  is_cache_usable = false;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObIvfCacheMgr *cache_mgr = nullptr;
  const ObDASScanCtDef *centroid_ctdef = vec_index_scan_ctdef_->get_centroid_table_ctdef();

  if (OB_ISNULL(vec_index_service) || OB_ISNULL(centroid_ctdef)) {
    is_cache_usable = false;
  } else if (OB_FAIL(vec_index_service->acquire_ivf_cache_mgr_guard(
                 ls_id_, ObIvfCacheMgrKey(centroid_tablet_id_), vec_index_param_, dim_,
                 centroid_ctdef->ref_table_id_, cache_guard))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      is_cache_usable = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to acquire ivf cache mgr guard", K(ret));
    }
  } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null cache mgr", K(ret));
  } else if (OB_FAIL(cache_mgr->get_or_create_cache_node<share::ObIvfICache>(
                 share::IvfCacheType::IVF_PQ_PRECOMPUTE_TABLE_CACHE, cent_cache))) {
    LOG_WARN("fail to get or create cache node", K(ret));
    if (ret == OB_ALLOCATE_MEMORY_FAILED) {
      is_cache_usable = false;
      ret = OB_SUCCESS;
    }
  } else if (!cent_cache->is_completed()) {
    if (cent_cache->set_writing_if_idle()) {
      common::ScopedTimer build_timer(
          common::ObMetricId::VEC_IVF_PQ_PRECOMPUTE_CACHE_BUILD_TIME,
          profile_);
      int tmp_ret = try_write_pq_precompute_table_cache(*cent_cache, is_vectorized);
      if (OB_TMP_FAIL(tmp_ret)) {
        if (tmp_ret != OB_EAGAIN) {
          LOG_WARN("fail to try write pq precompute table cache", K(tmp_ret), K(is_vectorized), KPC(cent_cache));
        } else {
          is_cache_usable = false;
        }
      } else {
        is_cache_usable = cent_cache->is_completed();
      }
    } else {
      LOG_INFO("other threads already writed pq precompute table cache, skip", K(ret));
    }
  } else {
    is_cache_usable = true;
  }
  return ret;
}

int ObDASVecIndexIVFPQScanIter::try_write_pq_precompute_table_cache(
    share::ObIvfICache &cent_cache,
    bool is_vectorized)
{
  int ret = OB_SUCCESS;
  share::ObIvfCacheMgrGuard pq_center_guard;
  share::ObIvfICache *pq_cent_cache = nullptr;
  bool is_pq_cent_cache_usable = false;
  share::ObIvfCacheMgrGuard center_guard;
  share::ObIvfICache *ivf_cent_cache = nullptr;
  bool is_ivf_cent_cache_usable = false;
  if (OB_FAIL(get_centers_cache(is_vectorized, true /*is_pq_centers*/, pq_center_guard, pq_cent_cache,
                 is_pq_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(pq_cent_cache));
  } else if (OB_FAIL(get_centers_cache(is_vectorized, false /*is_pq_centers*/, center_guard, ivf_cent_cache,
                 is_ivf_cent_cache_usable))) {
    LOG_WARN("fail to get centers cache", K(ret), K(is_vectorized), KPC(ivf_cent_cache));
  } else {
    RWLock::WLockGuard guard(cent_cache.get_lock());
    if (!cent_cache.is_writing()) {
      LOG_INFO("other threads already writed centroids cache, skip", K(ret));
    } else {
      if (is_pq_cent_cache_usable && is_ivf_cent_cache_usable) {
        ObArenaAllocator tmp_allocator;
        share::ObVsagSearchAlloc hgraph_allocator(MTL_ID());
        int64_t ksub = 1L << nbits_;
        int64_t sub_dim = dim_ / m_;
        float *ivf_centers = nullptr;
        float *pq_center = pq_cent_cache->get_centroids();
        float *precompute_table = cent_cache.get_centroids();
        float *r_norms = (float *)tmp_allocator.alloc(sizeof(float) * m_ * ksub);
        if (ivf_cent_cache->has_hgraph_index()) {
          if (OB_FAIL(ivf_cent_cache->get_centroids_from_hgraph(&hgraph_allocator, &ivf_centers))) {
            LOG_WARN("fail to get ivf centers", K(ret));
          }
        } else {
          ivf_centers = ivf_cent_cache->get_centroids();
        }

        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(r_norms)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc norms", K(ret), K(m_), K(ksub));
        } else {
          for (int i = 0; i < m_; i++) {
            for (int j = 0; j < ksub; j++) {
              float *pq_cid_vec = pq_center + (i * ksub + j) * sub_dim;
              r_norms[i * ksub + j] = ObVectorL2Distance<float>::l2_norm_square(pq_cid_vec, sub_dim);
            }
          }
          for (int i = 0; i < ivf_cent_cache->get_capacity(); i++) {
            float *ivf_center = ivf_centers + i * dim_;
            float *tab = &precompute_table[i * m_ * ksub];
            for (int j = 0; j < m_; j++) {
              ObVectorIpDistance<float>::fvec_inner_products_ny(tab + j * ksub, ivf_center + j * sub_dim,
                  pq_center + j * ksub * sub_dim, sub_dim, ksub);
            }
            ObVectorL2Distance<float>::fvec_madd(m_ * ksub, r_norms, 2.0, tab, tab);
          }
        }
        if (OB_SUCC(ret)) {
          cent_cache.set_completed();
        }
        if (OB_FAIL(ret)) {
          cent_cache.reuse();
        }
      } else {
        ret = OB_EAGAIN;
        cent_cache.reuse();
      }
    }
  }
  return ret;
}

// ======================== Strategy interface implementations ========================

void ObDASVecIndexIVFBaseScanIter::prepare_partition_path(
    ObIvfFilterMode mode, double selectivity)
{
  ivf_filter_mode_ = mode;
  selectivity_ = selectivity;
  // A partition rescan reuses the iterator tree, but none of the path runtime is
  // reusable: centers and expansion progress belong to the previous tablet.
  search_prepared_ = false;
  post_filter_centers_inited_ = false;
  brute_force_done_ = false;
  need_fallback_brute_force_ = false;
  cumulative_added_cnt_ = 0;
  cid_vec_scan_rows_ = 0;
  iterative_filter_ctx_.reuse();
}

int ObDASVecIndexIVFBaseScanIter::build_query_condition(ObIAllocator &alloc, ObEvalCtx &eval_ctx,
                                                         ObExpr *search_vec, bool is_iter_filter,
                                                         uint64_t limit)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(search_vec) || OB_ISNULL(snapshot_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("search vec or snapshot is null", K(ret));
  } else {
    share::SCN read_scn;
    ivf_query_cond_.query_limit_ = limit;
    ivf_query_cond_.query_order_ = true;
    ivf_query_cond_.is_post_with_filter_ = is_iter_filter;
    ivf_query_cond_.distance_threshold_ = distance_threshold_;
    ivf_query_cond_.nprobe_ = nprobes_;
    ivf_filter_mode_ = is_iter_filter
        ? ObIvfFilterMode::IVF_FILTER_MODE_ITER_FILTER
        : ObIvfFilterMode::IVF_FILTER_MODE_PRE_FILTER;
    post_filter_centers_inited_ = false;
    search_prepared_ = false;

    // Pass the effective async snapshot into cache visibility and IVF query state.
    if (OB_FAIL(get_vector_read_snapshot_scn(snapshot_, read_scn))) {
      LOG_WARN("failed to get vector read snapshot", K(ret));
    } else if (OB_FALSE_IT(ivf_query_cond_.query_scn_ = read_scn)) {
    } else if (OB_FAIL(ObDasVecScanUtils::get_real_search_vec(alloc, &eval_ctx,
                                                        search_vec, ivf_query_cond_.query_vector_))) {
      LOG_WARN("failed to get real search vec", K(ret));
    }
  }

  return ret;
}

int ObDASVecIndexIVFBaseScanIter::adjust_search_for_next_round(bool first_search,
    share::ObVectorQueryVidIterator *adaptor_vid_iter,
    int64_t iter_unfiltered_vid_cnt, int64_t iter_added_cnt,
    int64_t iter_scan_total_num, bool &stop_search)
{
  int ret = OB_SUCCESS;
  if (brute_force_done_) {
    // Brute-force already scanned all data, no need to expand
    stop_search = true;
  } else if (is_pre_filter()) {
    // Pre-filter mode: IVF handles its own iterative expansion internally.
    UNUSED(first_search);
    UNUSED(adaptor_vid_iter);
    UNUSED(iter_unfiltered_vid_cnt);
    UNUSED(iter_added_cnt);
    UNUSED(iter_scan_total_num);
    stop_search = true;
  } else if (first_search) {
    // First round: compute initial heap_size from optimizer selectivity
    heap_size_ = ObDasVecScanUtils::calc_heap_size(ivf_query_cond_.query_limit_, selectivity_);
    stop_search = false;
  } else {
    // Subsequent rounds: check termination conditions
    int64_t limit_k = ivf_query_cond_.query_limit_;
    int64_t this_round_added = iter_added_cnt;

    if (similarity_threshold_ != 0 && this_round_added == 0) {
      // Post-filter produced no new results this round → stop
      stop_search = true;
      LOG_TRACE("[IVF_VEC] post/iter_filter: no new results after filter, stop",
               K_(similarity_threshold), K(this_round_added), K(iter_unfiltered_vid_cnt));
    } else if (max_scan_vectors_ > 0 && cid_vec_scan_rows_ > max_scan_vectors_) {
      stop_search = true;
      LOG_TRACE("[IVF_VEC] post/iter_filter: reach max scan vectors, stop",
               K_(cid_vec_scan_rows), K_(max_scan_vectors));
    } else if (!iterative_filter_ctx_.has_next_center()) {
      stop_search = true;
      LOG_TRACE("[IVF_VEC] post/iter_filter: no more centers, stop");
    } else {
      // Use cumulative count (set by driver) for left/next_nprobe calculation
      int64_t left = limit_k > cumulative_added_cnt_
          ? limit_k - cumulative_added_cnt_ : 0;

      // Adaptive heap resizing based on observed selectivity
      double actual_ratio = (iter_unfiltered_vid_cnt > 0)
          ? (double)this_round_added / iter_unfiltered_vid_cnt : 1.0;
      heap_size_ = OB_MAX(ObDasVecScanUtils::calc_heap_size(left, actual_ratio), heap_size_);

      int64_t next_nprobe = OB_MIN(nprobes_,
          static_cast<int64_t>(nprobes_ * ((double)left / limit_k) + 1));
      if (OB_FAIL(expand_to_next_centers(true/*is_vectorized*/, next_nprobe))) {
        LOG_WARN("failed to expand to next centers", K(ret), K(next_nprobe));
      } else {
        stop_search = false;
        LOG_TRACE("[IVF_VEC] post/iter_filter: expanding to next centers",
                 K(limit_k), K(cumulative_added_cnt_), K(left), K(next_nprobe),
                 K(heap_size_), K(actual_ratio), K_(cid_vec_scan_rows));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
