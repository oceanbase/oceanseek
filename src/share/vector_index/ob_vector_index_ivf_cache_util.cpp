/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_vector_index_ivf_cache_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
namespace share
{
int ObIvfCacheUtil::ObIvfWriteCacheFunc::operator()(int64_t dim, float *data, int64_t cid /*= -1*/)
{
  int ret = OB_SUCCESS;
  if (cid != -1) {
    // MapCentCache: use add_centroid with explicit cid (cid is not contiguous)
    SCN min_scn = SCN::min_scn();
    if (OB_FAIL(cent_cache_.add_centroid(cid, data, dim, &min_scn))) {
      LOG_WARN("fail to add centroid with cid", K(ret), K(cid), K(dim));
    }
  } else {
    // Regular cache: write by sequential index
    if (OB_FAIL(cent_cache_.write_centroid_with_real_idx(cent_idx_, data, dim * sizeof(float)))) {
      LOG_WARN("fail to write centroid", K(ret), K(cent_idx_), K(dim));
    } else {
      ++cent_idx_;
    }
  }
  return ret;
}


int ObIvfCacheUtil::ObIvfUpdatePostingListCountFunc::operator()(int64_t cid, int64_t count)
{
  int ret = OB_SUCCESS;
  ObCentroidInfo info;
  info.posting_list_count_ = static_cast<int32_t>(count);
  if (OB_FAIL(cent_cache_.set_center_info(cid, info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_DEBUG("cid not found in cache, skip updating posting_list_count", K(cid), K(count));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to set center info", K(ret), K(cid), K(count));
    }
  }
  return ret;
}

int ObIvfCacheUtil::write_ivf_cent_posting_list_count(ObPluginVectorIndexService &service,
                                                      const ObTableID &table_id,
                                                      const ObTabletID &tablet_id,
                                                      int64_t ns_id,
                                                      ObIvfMapCentCache &cent_cache)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObIvfUpdatePostingListCountFunc update_func(cent_cache);
  if (OB_FAIL(service.process_ivf_center_list_count(table_id, tablet_id, ns_id, tmp_allocator, update_func))) {
    LOG_WARN("failed to process ivf center list count", K(ret), K(table_id), K(tablet_id), K(ns_id));
  }
  return ret;
}

int ObIvfCacheUtil::scan_and_write_ivf_cent_cache(ObPluginVectorIndexService &service,
                                                  const ObTableID &table_id,
                                                  const ObTabletID &tablet_id,
                                                  const ObVectorIndexParam &vec_param,
                                                  ObIvfICache &cent_cache,
                                                  ObVecIndexAsyncTaskCtx *task_ctx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  if (cent_cache.set_writing_if_idle()) {
    RWLock::WLockGuard guard(cent_cache.get_lock());
    ObIvfWriteCacheFunc write_func(cent_cache);
    if (OB_FAIL(service.process_ivf_aux_info(table_id, tablet_id, tmp_allocator, write_func, task_ctx))) {
      LOG_WARN("failed to get centers", K(ret));
      cent_cache.reuse();
    } else if (!cent_cache.is_full_cache()) {
      cent_cache.reuse();
    } else {
      if (cent_cache.get_cache_type() != IvfCacheType::IVF_CENTROID_CACHE) {
        cent_cache.set_completed();
      } else if (!cent_cache.can_build_hgraph()) {
        cent_cache.set_completed();
      } else {
        CHECK_TASK_CANCELLED(ret, task_ctx);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(cent_cache.build_hgraph_and_release_centers(vec_param))) {
          LOG_WARN("fail to build hgraph index after cache load", K(ret), K(table_id), K(tablet_id));
          cent_cache.reuse();
        } else {
          cent_cache.set_completed();
        }
      }
    }
  }

  return ret;
}

int ObIvfWriteMapFunc::operator()(int64_t dim, float *data, int64_t cid /*= -1*/)
{
  int ret = OB_SUCCESS;
  if (cid < 0 || OB_ISNULL(data) || dim <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for ObIvfWriteMapFunc", K(ret), K(cid), KP(data), K(dim));
  } else {
    // Deep copy the centroid vector
    int64_t vec_size = dim * sizeof(float);
    float *vec_copy = static_cast<float *>(allocator_.alloc(vec_size));
    if (OB_ISNULL(vec_copy)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc vec copy", K(ret), K(vec_size));
    } else {
      MEMCPY(vec_copy, data, vec_size);
      ObCentroidInfo info;
      info.cid_ = cid;
      info.cid_vec_ = vec_copy;
      info.create_scn_ = SCN::min_scn();
      info.delet_scn_ = SCN::max_scn();
      info.state_ = ObIvfCentroidState::NORMAL;
      info.posting_list_count_ = 0;
      if (OB_FAIL(cid_map_.set_refactored(cid, info))) {
        LOG_WARN("fail to insert cid into map", K(ret), K(cid));
        allocator_.free(vec_copy);
      }
    }
  }
  return ret;
}

int ObIvfCacheUtil::is_cache_writable(const ObLSID &ls_id, int64_t table_id,
                                      const ObTabletID &tablet_id,
                                      const ObVectorIndexParam &vec_param, int64_t dim,
                                      IvfCacheType cache_type, bool &is_writable)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObIvfCacheMgr *cache_mgr = nullptr;
  ObIvfCacheMgrGuard cache_guard;
  ObIvfICache *cent_cache = nullptr;

  if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected nullptr", K(ret), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->acquire_ivf_cache_mgr_guard(
                 ls_id, ObIvfCacheMgrKey(tablet_id), vec_param, dim, table_id, cache_guard))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      is_writable = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to acquire ivf cache mgr with vec param",
               K(ret), K(ls_id), K(tablet_id), K(vec_param), K(dim));
    }
  } else if (OB_ISNULL(cache_mgr = cache_guard.get_ivf_cache_mgr())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null cache mgr", K(ret));
  } else if (OB_FAIL(cache_mgr->get_or_create_cache_node<ObIvfICache>(cache_type, cent_cache))) {
    LOG_WARN("fail to get or create cache node", K(ret), K(cache_type));
  } else {
    is_writable = cent_cache->is_idle();
  }
  return ret;
}

template <typename ForEachFn>
int find_nearest_center_from_map(
    float *vector_data,
    int64_t dim,
    ForEachFn &for_each_fn,
    ObVectorIndexDistAlgorithm dis_algo,
    ObIAllocator &allocator,
    int64_t &center_idx)
{
  int ret = OB_SUCCESS;
  ObVectorClusterHelper helper;
  ObVectorNormalizeInfo norm_info;
  const sql::ObExprVectorDistance::ObVecDisType center_dis_type =
      VIDA_L1 == dis_algo ? sql::ObExprVectorDistance::ObVecDisType::MANHATTAN
                          : sql::ObExprVectorDistance::ObVecDisType::EUCLIDEAN_SQUARED;
  if (OB_FAIL(helper.get_nearest_probe_centers_from_map(
      vector_data, dim, for_each_fn, 1 /*nprobe*/, allocator,
      VIDA_COS != dis_algo ? nullptr : &norm_info, center_dis_type))) {
    LOG_WARN("failed to get nearest center from map", K(ret));
  } else if (OB_FAIL(helper.get_center_idx_raw(0, center_idx))) {
    LOG_WARN("failed to get center idx raw", K(ret));
  }
  return ret;
}

// Explicit instantiations
template int find_nearest_center_from_map<ObCidMapForEachFunc>(
    float *, int64_t, ObCidMapForEachFunc &, ObVectorIndexDistAlgorithm, ObIAllocator &, int64_t &);
template int find_nearest_center_from_map<ObMapCacheForEachFunc>(
    float *, int64_t, ObMapCacheForEachFunc &, ObVectorIndexDistAlgorithm, ObIAllocator &, int64_t &);

int ObIvfWarmupCtx::get_cid_vec_tablet_id(const ObTabletID &centroid_tablet_id, ObTabletID &cid_vec_tablet_id) const
{
  int ret = OB_SUCCESS;
  // Find index of centroid_tablet_id in cent_tablet_ids_
  int64_t idx = -1;
  for (int64_t i = 0; i < cent_tablet_ids_.count(); ++i) {
    if (cent_tablet_ids_.at(i) == centroid_tablet_id) {
      idx = i;
      break;
    }
  }
  if (idx >= 0 && idx < cid_vec_tablet_ids_.count()) {
    cid_vec_tablet_id = cid_vec_tablet_ids_.at(idx);
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("centroid tablet not found in warmup ctx tablet mapping",
             K(ret), K(centroid_tablet_id), K(cent_tablet_ids_.count()));
  }
  return ret;
}

// =====================================================================
// prepare_warmup_ctx: pre-resolve all schema info needed by warmup
// Call once before the per-key loop to avoid repeated schema lookups.
// =====================================================================
int ObIvfCacheUtil::prepare_warmup_ctx(
    schema::ObSchemaGetterGuard &schema_guard,
    uint64_t center_table_id,
    bool need_posting,
    ObIvfWarmupCtx &ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const schema::ObTableSchema *data_table_schema = nullptr;

  // 1. Get centroid table schema (full, needed for scan); only ivf_flat supported
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, center_table_id, ctx.cent_schema_))) {
    LOG_WARN("fail to get centroid table schema", K(ret), K(center_table_id));
  } else if (OB_ISNULL(ctx.cent_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("centroid table not exist", K(ret), K(center_table_id));
  } else if (!ctx.cent_schema_->is_vec_ivfflat_centroid_index()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only ivf_flat centroid table is supported for warmup",
             K(ret), K(center_table_id), "index_type", ctx.cent_schema_->get_index_type());
  }

  // 2. Get data table schema to resolve column IDs
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard.get_table_schema(
            tenant_id, ctx.cent_schema_->get_data_table_id(), data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table not exist", K(ret));
    }
  }

  // 3. Resolve centroid column IDs (vec + cid)
  if (OB_SUCC(ret)) {
    ObSEArray<uint64_t, 8> all_col_ids;
    if (OB_FAIL(ctx.cent_schema_->get_column_ids(all_col_ids))) {
      LOG_WARN("fail to get column ids from centroid schema", K(ret));
    }
    for (int64_t i = 0; i < all_col_ids.count() && OB_SUCC(ret); ++i) {
      const schema::ObColumnSchemaV2 *data_col =
          data_table_schema->get_column_schema(all_col_ids.at(i));
      if (OB_ISNULL(data_col)) {
        // skip hidden/internal columns
      } else if (data_col->is_vec_ivf_center_vector_column()) {
        ctx.cent_vec_col_id_ = all_col_ids.at(i);
      } else if (data_col->is_vec_ivf_center_id_column()) {
        ctx.cent_cid_col_id_ = all_col_ids.at(i);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID == ctx.cent_vec_col_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vector column not found in centroid schema", K(ret), K(center_table_id));
      } else if (OB_INVALID_ID == ctx.cent_cid_col_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ivf center id column not found in centroid schema", K(ret), K(center_table_id));
      }
    }
  }

  // 4. Find sibling cid_vector table and resolve its schema/column IDs
  if (OB_SUCC(ret) && need_posting) {
    // Find cid_vector table from data table's index list
    const ObIArray<schema::ObAuxTableMetaInfo> &index_infos = data_table_schema->get_simple_index_infos();
    for (int64_t i = 0; i < index_infos.count() && OB_SUCC(ret); ++i) {
      if (schema::is_vec_ivfflat_cid_vector_index(index_infos.at(i).index_type_)) {
        ctx.cid_vec_table_id_ = index_infos.at(i).table_id_;
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_INVALID_ID == ctx.cid_vec_table_id_) {
      // No cid_vector table found — non-fatal, just skip posting count
      LOG_DEBUG("no cid_vector table found, skip posting list count", K(center_table_id));
    } else {
      // Store centroid and cid_vector tablet arrays for partition mapping
      if (OB_FAIL(ctx.cent_schema_->get_tablet_ids(ctx.cent_tablet_ids_))) {
        LOG_WARN("fail to get centroid tablet ids", K(ret));
      }
      if (OB_SUCC(ret)) {
        const schema::ObSimpleTableSchemaV2 *cid_vec_simple = nullptr;
        if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, ctx.cid_vec_table_id_, cid_vec_simple))) {
          LOG_WARN("fail to get cid_vector simple schema", K(ret));
        } else if (OB_ISNULL(cid_vec_simple)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("cid_vector simple schema not exist", K(ret));
        } else if (OB_FAIL(cid_vec_simple->get_tablet_ids(ctx.cid_vec_tablet_ids_))) {
          LOG_WARN("fail to get cid_vector tablet ids", K(ret));
        }
      }
      // Get cid_vector full schema and resolve posting_cid_col_id
      if (OB_SUCC(ret)) {
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, ctx.cid_vec_table_id_, ctx.cid_vec_schema_))) {
          LOG_WARN("fail to get cid_vector table schema", K(ret));
        } else if (OB_ISNULL(ctx.cid_vec_schema_)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("cid_vector table schema not exist", K(ret));
        } else {
          // Resolve posting center_id column
          ObSEArray<uint64_t, 8> cid_col_ids;
          if (OB_FAIL(ctx.cid_vec_schema_->get_column_ids(cid_col_ids))) {
            LOG_WARN("fail to get cid_vector column ids", K(ret));
          }
          for (int64_t i = 0; i < cid_col_ids.count() && OB_SUCC(ret); ++i) {
            const schema::ObColumnSchemaV2 *data_col =
                data_table_schema->get_column_schema(cid_col_ids.at(i));
            if (OB_NOT_NULL(data_col) && data_col->is_vec_ivf_center_id_column()) {
              ctx.posting_cid_col_id_ = cid_col_ids.at(i);
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

// =====================================================================
// scan_ivf_table_local overload: accepts pre-resolved schema + column_ids
// =====================================================================
template <typename RowCallback>
static int scan_ivf_table_local(
    const ObLSID &ls_id,
    const schema::ObTableSchema &table_schema,
    const ObTabletID &tablet_id,
    const ObIArray<uint64_t> &column_ids,
    ObIAllocator &allocator,
    RowCallback &row_callback)
{
  int ret = OB_SUCCESS;
  storage::ObAccessService *access_service = MTL(storage::ObAccessService *);
  storage::ObLSService *ls_service = MTL(storage::ObLSService *);
  storage::ObLSHandle ls_handle;
  storage::ObTabletHandle tablet_handle;
  common::ObNewRowIterator *scan_iter = nullptr;

  if (OB_ISNULL(access_service) || OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access_service or ls_service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, storage::ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("fail to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(
                 tablet_id, tablet_handle, 0,
                 storage::ObMDSGetTabletMode::READ_READABLE_COMMITED,
                 SCN::max_scn()))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id));
  } else {
    SMART_VAR(storage::ObTableScanParam, scan_param) {
      schema::ObTableParam table_param(allocator);
      if (OB_FAIL(scan_param.column_ids_.assign(column_ids))) {
        LOG_WARN("fail to assign column ids", K(ret));
      } else {
        ObQueryFlag query_flag(ObQueryFlag::Forward, false, false, false, true, false, false,
                               ObQueryFlag::MysqlMode, false);
        query_flag.disable_cache();
        scan_param.scan_flag_.flag_ = query_flag.flag_;
        scan_param.ls_id_ = ls_id;
        scan_param.tablet_id_ = tablet_id;
        scan_param.schema_version_ = tablet_handle.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
        scan_param.reserved_cell_count_ = column_ids.count();
        scan_param.index_id_ = 0;
        scan_param.is_get_ = false;
        scan_param.timeout_ = INT64_MAX;
        scan_param.limit_param_.limit_ = -1;
        scan_param.limit_param_.offset_ = 0;
        scan_param.snapshot_.init_weak_read(SCN::max_scn());
        scan_param.sql_mode_ = SMO_DEFAULT;
        scan_param.allocator_ = &allocator;
        scan_param.scan_allocator_ = &allocator;
        scan_param.for_update_ = false;
        scan_param.for_update_wait_timeout_ = scan_param.timeout_;
        scan_param.frozen_version_ = -1;
        scan_param.force_refresh_lc_ = false;
        scan_param.output_exprs_ = nullptr;
        scan_param.aggregate_exprs_ = nullptr;
        scan_param.op_ = nullptr;
        scan_param.row2exprs_projector_ = nullptr;
        scan_param.need_scn_ = false;
        scan_param.pd_storage_flag_ = false;

        if (OB_FAIL(table_param.convert(table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
          LOG_WARN("fail to convert table param", K(ret));
        } else {
          scan_param.table_param_ = &table_param;
        }

        if (OB_SUCC(ret)) {
          int64_t rowkey_col_cnt = table_schema.get_rowkey_column_num();
          void *buf = allocator.alloc(sizeof(ObObj) * rowkey_col_cnt * 2);
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc key range objs", K(ret), K(rowkey_col_cnt));
          } else {
            ObObj *row_objs = static_cast<ObObj *>(buf);
            for (int64_t i = 0; i < rowkey_col_cnt; i++) {
              row_objs[i] = ObObj::make_min_obj();
            }
            for (int64_t i = rowkey_col_cnt; i < rowkey_col_cnt * 2; i++) {
              row_objs[i] = ObObj::make_max_obj();
            }
            ObNewRange range;
            range.table_id_ = table_schema.get_table_id();
            range.start_key_.assign(row_objs, rowkey_col_cnt);
            range.end_key_.assign(row_objs + rowkey_col_cnt, rowkey_col_cnt);
            range.border_flag_.set_inclusive_start();
            range.border_flag_.set_inclusive_end();
            if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
              LOG_WARN("fail to push key range", K(ret));
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(access_service->table_scan(scan_param, scan_iter))) {
          LOG_WARN("fail to do table scan", K(ret), K(ls_id), K(tablet_id));
        } else {
          storage::ObTableScanIterator *table_scan_iter =
              static_cast<storage::ObTableScanIterator *>(scan_iter);
          blocksstable::ObDatumRow *datum_row = nullptr;
          while (OB_SUCC(ret)) {
            if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              }
              LOG_WARN("fail to get next row", K(ret));
            } else if (OB_ISNULL(datum_row)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("datum_row is null", K(ret));
            } else if (OB_FAIL(row_callback(*datum_row))) {
              LOG_WARN("row callback failed", K(ret));
            }
          }
        }

        if (OB_NOT_NULL(scan_iter)) {
          access_service->revert_scan_iter(scan_iter);
          scan_iter = nullptr;
        }
      }
    }
  }
  return ret;
}

// Functor: row callback for ctx-based centroid scan (IVF_MAP_CENTROID_CACHE only)
// Column layout: idx 0 = vector, idx 1 = center_id
class ObIvfCtxCentVecRowFunc
{
public:
  explicit ObIvfCtxCentVecRowFunc(common::ObIAllocator &allocator, ObIvfICache &cent_cache)
      : allocator_(allocator), cent_cache_(cent_cache) {}
  int operator()(blocksstable::ObDatumRow &datum_row)
  {
    int ret = OB_SUCCESS;
    ObString vec_data = datum_row.storage_datums_[0].get_string();
    ObString cid_data = datum_row.storage_datums_[1].get_string();
    ObCenterId cent_id;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
            &allocator_, ObLongTextType, CS_TYPE_BINARY, vec_data))) {
      LOG_WARN("fail to strip lob header for centroid vector", K(ret), K(vec_data));
    } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
            cent_id, cid_data, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
      LOG_WARN("fail to get center id", K(ret));
    } else {
      int64_t dim = vec_data.length() / sizeof(float);
      SCN min_scn = SCN::min_scn();
      if (OB_FAIL(cent_cache_.add_centroid(
              cent_id.center_id_, reinterpret_cast<float *>(vec_data.ptr()),
              dim, &min_scn))) {
        LOG_WARN("fail to add centroid with cid", K(ret), K(cent_id.center_id_), K(dim));
      }
    }
    return ret;
  }
private:
  common::ObIAllocator &allocator_;
  ObIvfICache &cent_cache_;
};

// Functor: row callback for ctx-based posting list count scan
class ObIvfCtxPostingCountRowFunc
{
public:
  ObIvfCtxPostingCountRowFunc(ObIvfMapCentCache &cent_cache)
      : cent_cache_(cent_cache), cur_cid_(-1), cur_count_(0) {}
  int operator()(blocksstable::ObDatumRow &datum_row)
  {
    int ret = OB_SUCCESS;
    ObString cid_data = datum_row.storage_datums_[0].get_string();
    ObCenterId cent_id;
    if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
            cent_id, cid_data, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
      LOG_WARN("fail to get center id", K(ret));
    } else if (cent_id.center_id_ == cur_cid_) {
      cur_count_++;
    } else {
      if (OB_FAIL(flush_current())) {
        LOG_WARN("fail to flush posting count", K(ret), K(cur_cid_), K(cur_count_));
      } else {
        cur_cid_ = cent_id.center_id_;
        cur_count_ = 1;
      }
    }
    return ret;
  }
  int finish()
  {
    int ret = OB_SUCCESS;
    if (cur_cid_ >= 0) {
      ret = flush_current();
    }
    return ret;
  }
private:
  int flush_current()
  {
    int ret = OB_SUCCESS;
    if (cur_cid_ >= 0) {
      ObCentroidInfo info;
      info.posting_list_count_ = static_cast<int32_t>(cur_count_);
      if (OB_FAIL(cent_cache_.set_center_info(cur_cid_, info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_DEBUG("cid not found in cache, skip", K(cur_cid_), K(cur_count_));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to set center info", K(ret), K(cur_cid_), K(cur_count_));
        }
      }
    }
    return ret;
  }
  ObIvfMapCentCache &cent_cache_;
  int64_t cur_cid_;
  int64_t cur_count_;
};

// =====================================================================
// local_scan_write_ivf_cent_cache: ctx-based overload
// =====================================================================
int ObIvfCacheUtil::local_scan_write_ivf_cent_cache(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObIvfWarmupCtx &ctx,
    ObIvfMapCentCache &cent_cache)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("IvfLocalScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_ISNULL(ctx.cent_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cent_schema is null in warmup ctx", K(ret));
  } else if (cent_cache.set_writing_if_idle()) {
    RWLock::WLockGuard guard(cent_cache.get_lock());
    ObSEArray<uint64_t, 2> column_ids;
    if (OB_FAIL(column_ids.push_back(ctx.cent_vec_col_id_))) {
      LOG_WARN("fail to push vec col id", K(ret));
    } else if (OB_FAIL(column_ids.push_back(ctx.cent_cid_col_id_))) {
      LOG_WARN("fail to push cid col id", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObIvfCtxCentVecRowFunc row_func(tmp_allocator, cent_cache);
      if (OB_FAIL(scan_ivf_table_local(
              ls_id, *ctx.cent_schema_, tablet_id, column_ids, tmp_allocator, row_func))) {
        LOG_WARN("fail to scan centroid table via ctx", K(ret), K(ls_id), K(tablet_id));
        cent_cache.reuse();
      }
    } else {
      cent_cache.reuse();
    }
    // Write posting list count after centroids succeed
    if (OB_SUCC(ret)) {
      if (OB_FAIL(local_scan_write_posting_list_count(ls_id, tablet_id, ctx, cent_cache))) {
        LOG_WARN("fail to write posting list count", K(ret), K(ls_id), K(tablet_id));
        cent_cache.reuse();
      } else {
        cent_cache.set_completed();
      }
    }
  }
  return ret;
}

// =====================================================================
// local_scan_ivf_centroids_to_map: scan centroids into a HashMap
// =====================================================================
class ObIvfCtxCentVecToMapRowFunc
{
public:
  ObIvfCtxCentVecToMapRowFunc(common::hash::ObHashMap<int64_t, ObCentroidInfo> &cid_map,
                              ObIAllocator &allocator)
      : cid_map_(cid_map), allocator_(allocator) {}
  int operator()(blocksstable::ObDatumRow &datum_row)
  {
    int ret = OB_SUCCESS;
    ObString vec_data = datum_row.storage_datums_[0].get_string();
    ObString cid_data = datum_row.storage_datums_[1].get_string();
    ObCenterId cent_id;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
            &allocator_, ObLongTextType, CS_TYPE_BINARY, vec_data))) {
      LOG_WARN("fail to strip lob header for centroid vector", K(ret), K(vec_data));
    } else if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
            cent_id, cid_data, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
      LOG_WARN("fail to get center id", K(ret));
    } else {
      int64_t dim = vec_data.length() / sizeof(float);
      int64_t vec_size = dim * sizeof(float);
      float *vec_copy = static_cast<float *>(allocator_.alloc(vec_size));
      if (OB_ISNULL(vec_copy)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vec copy", K(ret), K(vec_size));
      } else {
        MEMCPY(vec_copy, vec_data.ptr(), vec_size);
        ObCentroidInfo info;
        info.cid_ = cent_id.center_id_;
        info.cid_vec_ = vec_copy;
        info.create_scn_ = SCN::min_scn();
        info.delet_scn_ = SCN::max_scn();
        info.state_ = ObIvfCentroidState::NORMAL;
        info.posting_list_count_ = 0;
        if (OB_FAIL(cid_map_.set_refactored(cent_id.center_id_, info))) {
          LOG_WARN("fail to insert cid into map", K(ret), K(cent_id.center_id_));
          allocator_.free(vec_copy);
          vec_copy = nullptr;
        }
      }
    }
    return ret;
  }
private:
  common::hash::ObHashMap<int64_t, ObCentroidInfo> &cid_map_;
  ObIAllocator &allocator_;
};

int ObIvfCacheUtil::local_scan_ivf_centroids_to_map(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObIvfWarmupCtx &ctx,
    common::hash::ObHashMap<int64_t, ObCentroidInfo> &cid_map,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("IvfLocalScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_ISNULL(ctx.cent_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cent_schema is null in warmup ctx", K(ret));
  } else {
    ObSEArray<uint64_t, 2> column_ids;
    if (OB_FAIL(column_ids.push_back(ctx.cent_vec_col_id_))) {
      LOG_WARN("fail to push vec col id", K(ret));
    } else if (OB_FAIL(column_ids.push_back(ctx.cent_cid_col_id_))) {
      LOG_WARN("fail to push cid col id", K(ret));
    } else {
      ObIvfCtxCentVecToMapRowFunc row_func(cid_map, allocator);
      if (OB_FAIL(scan_ivf_table_local(
              ls_id, *ctx.cent_schema_, tablet_id, column_ids, tmp_allocator, row_func))) {
        LOG_WARN("fail to scan centroid table into map", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

// =====================================================================
// local_scan_write_posting_list_count: ctx-based overload
// =====================================================================
int ObIvfCacheUtil::local_scan_write_posting_list_count(
    const ObLSID &ls_id,
    const ObTabletID &centroid_tablet_id,
    const ObIvfWarmupCtx &ctx,
    ObIvfMapCentCache &cent_cache)
{
  int ret = OB_SUCCESS;
  ObTabletID cid_vec_tablet_id;
  if (!ctx.has_cid_vector()) {
    LOG_DEBUG("no cid_vector info in warmup ctx, skip posting list count");
  } else if (OB_ISNULL(ctx.cid_vec_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cid_vec_schema is null in warmup ctx", K(ret));
  } else if (OB_FAIL(ctx.get_cid_vec_tablet_id(centroid_tablet_id, cid_vec_tablet_id))) {
    LOG_WARN("fail to map centroid tablet to cid_vector tablet", K(ret), K(centroid_tablet_id));
  } else {
    ObArenaAllocator tmp_allocator("IvfLocalScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObSEArray<uint64_t, 1> column_ids;
    if (OB_FAIL(column_ids.push_back(ctx.posting_cid_col_id_))) {
      LOG_WARN("fail to push posting cid col id", K(ret));
    } else {
      ObIvfCtxPostingCountRowFunc count_func(cent_cache);
      if (OB_FAIL(scan_ivf_table_local(
              ls_id, *ctx.cid_vec_schema_, cid_vec_tablet_id,
              column_ids, tmp_allocator, count_func))) {
        LOG_WARN("fail to scan cid_vector table via ctx", K(ret), K(ls_id));
      } else if (OB_FAIL(count_func.finish())) {
        LOG_WARN("fail to flush final posting count", K(ret));
      }
    }
  }
  return ret;
}

int ObIvfCacheUtil::check_bootstrap_cid0_restore_needed_(
    const ObIArray<ObSpfreshCentroidRow> &centroids,
    const common::hash::ObHashMap<uint64_t, int64_t> &count_map,
    bool &need_restore,
    int64_t &cid0_posting)
{
  int ret = OB_SUCCESS;
  bool has_cid0_in_centroids = false;
  need_restore = false;
  cid0_posting = 0;
  for (int64_t i = 0; i < centroids.count(); ++i) {
    if (0 == centroids.at(i).center_id_) {
      has_cid0_in_centroids = true;
      break;
    }
  }
  if (!has_cid0_in_centroids) {
    const int count_ret = count_map.get_refactored(0ULL, cid0_posting);
    if (OB_SUCCESS == count_ret) {
      need_restore = cid0_posting > 0;
    } else if (OB_HASH_NOT_EXIST == count_ret) {
      cid0_posting = 0;
    } else {
      ret = count_ret;
      LOG_WARN("get cid=0 count from map failed", K(ret));
    }
  }
  return ret;
}

int ObIvfCacheUtil::restore_bootstrap_cid0_from_kv_count_if_needed(
    const ObIArray<ObSpfreshCentroidRow> &centroids,
    const common::hash::ObHashMap<uint64_t, int64_t> &count_map,
    int64_t dim,
    const SCN &min_scn,
    ObIvfMapCentCache &cent_cache)
{
  int ret = OB_SUCCESS;
  bool need_restore = false;
  int64_t cid0_posting = 0;
  if (OB_FAIL(check_bootstrap_cid0_restore_needed_(
          centroids, count_map, need_restore, cid0_posting))) {
    LOG_WARN("fail to decide whether bootstrap cid=0 needs restore", K(ret));
  } else if (need_restore) {
    ObCentroidInfo cid0_info;
    const int probe_ret = cent_cache.get_center_info(0, SCN::max_scn(), cid0_info);
    if (OB_ENTRY_NOT_EXIST == probe_ret) {
      ObArenaAllocator tmp_allocator("IvfCid0Ph", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      int64_t zero_buf_size = 0;
      float *zero_vec = nullptr;
      if (OB_FAIL(ObSpfreshTableHelper::get_raw_vector_expected_bytes(
              dim, zero_buf_size))) {
        LOG_WARN("invalid dim for bootstrap cid=0 placeholder", K(ret), K(dim));
      } else if (OB_ISNULL(zero_vec = static_cast<float *>(
                               tmp_allocator.alloc(zero_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc bootstrap cid=0 buf", K(ret), K(zero_buf_size));
      } else {
        MEMSET(zero_vec, 0, zero_buf_size);
        if (OB_FAIL(cent_cache.add_centroid(
                0, zero_vec, dim, &min_scn, ObIvfCentroidState::NORMAL))) {
          LOG_WARN("fail to add bootstrap cid=0 placeholder from kv count",
                   K(ret), K(cid0_posting));
        }
      }
    } else if (OB_FAIL(probe_ret)) {
      ret = probe_ret;
      LOG_WARN("fail to probe bootstrap cid=0 in cache", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObCentroidInfo restored_info;
      if (OB_FAIL(cent_cache.get_center_info(0, SCN::max_scn(), restored_info))) {
        LOG_WARN("fail to get bootstrap cid=0 for posting count", K(ret));
      } else {
        restored_info.posting_list_count_ = static_cast<int32_t>(cid0_posting);
        if (OB_FAIL(cent_cache.set_center_info(0, restored_info))) {
          LOG_WARN("fail to set bootstrap cid=0 posting count", K(ret), K(cid0_posting));
        }
      }
    }
  }
  return ret;
}

int ObIvfCacheUtil::kv_write_ivf_cent_cache(
    ObSpfreshTableHelper &table_helper,
    int64_t dim,
    const ObIArray<ObSpfreshCentroidRow> &centroids,
    ObIvfMapCentCache &cent_cache,
    bool &write_owned)
{
  int ret = OB_SUCCESS;
  write_owned = false;
  if (cent_cache.set_writing_if_idle()) {
    write_owned = true;
    RWLock::WLockGuard guard(cent_cache.get_lock());
    const SCN min_scn = SCN::min_scn();
    for (int64_t i = 0; OB_SUCC(ret) && i < centroids.count(); ++i) {
      const ObSpfreshCentroidRow &row = centroids.at(i);
      if (0 == row.center_id_) {
        ObCentroidInfo existing;
        if (OB_SUCCESS == cent_cache.get_center_info(0, SCN::max_scn(), existing)) {
          continue;
        }
      }
      const int64_t vec_dim = row.vector_len_ / static_cast<int64_t>(sizeof(float));
      if (vec_dim != dim) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("centroid dim mismatch", K(ret), K(vec_dim), K(dim), K(row.center_id_));
      } else if (OB_FAIL(cent_cache.add_centroid(
                     row.center_id_,
                     reinterpret_cast<const float *>(row.vector_),
                     vec_dim,
                     &min_scn,
                     row.state_))) {
        LOG_WARN("fail to add centroid from kv", K(ret), K(row.center_id_));
      }
    }
    if (OB_SUCC(ret)) {
      // NOTE: do not skip cid==0 here. Phase 1 has just inserted it (or it
      // already existed); either way we still need to populate
      // posting_list_count_ from kv. Skipping leaves cid=0's count at the
      // default 0 forever, which breaks downstream consumers.
      // Batch SQL: a single GROUP BY query replaces N per-cid COUNT(*) calls.
      ObArray<std::pair<uint64_t, int64_t>> center_counts;
      center_counts.set_attr(ObMemAttr(MTL_ID(), "IvfCidCntArr"));
      common::hash::ObHashMap<uint64_t, int64_t> count_map;
      const int64_t bucket_num = (centroids.count() > 16) ? centroids.count() : 16;
      if (OB_FAIL(count_map.create(bucket_num, ObMemAttr(MTL_ID(), "IvfCidCntMap")))) {
        LOG_WARN("create count_map failed", K(ret), K(bucket_num));
      } else if (OB_FAIL(table_helper.count_vectors_group_by_center(
                     dim, center_counts))) {
        LOG_WARN("fail to count vectors group by center", K(ret), K(dim));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < center_counts.count(); ++i) {
          if (OB_FAIL(count_map.set_refactored(
                  center_counts.at(i).first, center_counts.at(i).second))) {
            LOG_WARN("set count_map failed", K(ret),
                K(center_counts.at(i).first), K(center_counts.at(i).second));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(restore_bootstrap_cid0_from_kv_count_if_needed(
                centroids, count_map, dim, min_scn, cent_cache))) {
          LOG_WARN("fail to restore bootstrap cid=0 from kv count", K(ret));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < centroids.count(); ++i) {
        const ObSpfreshCentroidRow &row = centroids.at(i);
        int64_t posting_count = 0;
        int tmp_ret = count_map.get_refactored(row.center_id_, posting_count);
        if (OB_SUCCESS == tmp_ret) {
          // got
        } else if (OB_HASH_NOT_EXIST == tmp_ret) {
          posting_count = 0;
        } else {
          ret = tmp_ret;
          LOG_WARN("get count from map failed", K(ret), K(row.center_id_));
        }
        if (OB_SUCC(ret)) {
          ObCentroidInfo info;
          if (OB_FAIL(cent_cache.get_center_info(row.center_id_, SCN::max_scn(), info))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get center info for posting count", K(ret), K(row.center_id_));
            }
          } else {
            info.posting_list_count_ = static_cast<int32_t>(posting_count);
            if (OB_FAIL(cent_cache.set_center_info(row.center_id_, info))) {
              LOG_WARN("fail to set posting list count", K(ret), K(row.center_id_), K(posting_count));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      cent_cache.set_completed();
    } else {
      cent_cache.reuse();
    }
  } else if (!cent_cache.is_completed()) {
    // Another PREWARM/rewarm writer owns this node. Returning success here
    // would let the caller publish a generation (and commit the hot-table row)
    // before that writer has completed or even after it later fails.
    ret = OB_EAGAIN;
    LOG_INFO("ivf centroid cache warmup is already in progress, retry",
             KR(ret), K(dim), "centroid_count", centroids.count());
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
