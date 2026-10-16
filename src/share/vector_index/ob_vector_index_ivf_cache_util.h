/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_IVF_CACHE_UTIL_H_
#define OCEANBASE_SHARE_IVF_CACHE_UTIL_H_
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}

// Pre-resolved schema context for warmup path.
// Built once before the per-key loop to avoid repeated schema lookups.
struct ObIvfWarmupCtx
{
  // centroid table
  const schema::ObTableSchema *cent_schema_;
  uint64_t cent_vec_col_id_;
  uint64_t cent_cid_col_id_;

  // cid_vector table (for posting list count)
  const schema::ObTableSchema *cid_vec_schema_;
  uint64_t cid_vec_table_id_;
  uint64_t posting_cid_col_id_;
  // partition mapping: centroid tablet_ids -> cid_vector tablet_ids (same index)
  common::ObSEArray<ObTabletID, 1> cent_tablet_ids_;
  common::ObSEArray<ObTabletID, 1> cid_vec_tablet_ids_;

  ObIvfWarmupCtx()
      : cent_schema_(nullptr), cent_vec_col_id_(OB_INVALID_ID), cent_cid_col_id_(OB_INVALID_ID),
        cid_vec_schema_(nullptr), cid_vec_table_id_(OB_INVALID_ID),
        posting_cid_col_id_(OB_INVALID_ID)
  {}
  bool has_cid_vector() const { return cid_vec_schema_ != nullptr; }
  // Map centroid tablet_id to the corresponding cid_vector tablet_id
  int get_cid_vec_tablet_id(const ObTabletID &centroid_tablet_id, ObTabletID &cid_vec_tablet_id) const;
};

class ObIvfCacheUtil
{
public:
  static int is_cache_writable(const ObLSID &ls_id, int64_t table_id, const ObTabletID &tablet_id,
                               const ObVectorIndexParam &vec_param, int64_t dim,
                               IvfCacheType cache_type, bool &is_writable);
  static int scan_and_write_ivf_cent_cache(ObPluginVectorIndexService &service,
                                           const ObTableID &table_id,
                                           const ObTabletID &tablet_id,
                                           const ObVectorIndexParam &vec_param,
                                           ObIvfICache &cent_cache,
                                           ObVecIndexAsyncTaskCtx *task_ctx = nullptr);
  static int write_ivf_cent_posting_list_count(ObPluginVectorIndexService &service,
                                               const ObTableID &table_id,
                                               const ObTabletID &tablet_id,
                                               int64_t ns_id,
                                               ObIvfMapCentCache &cent_cache);

  // Pre-resolve all schema info needed by warmup (call once before per-key loop)
  static int prepare_warmup_ctx(
      schema::ObSchemaGetterGuard &schema_guard,
      uint64_t center_table_id,
      bool need_posting,
      ObIvfWarmupCtx &ctx);

  // Overloads that use pre-resolved ctx (no internal schema lookups)
  // Writes centroids + posting list count + set_completed atomically under CAS guard
  static int local_scan_write_ivf_cent_cache(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObIvfWarmupCtx &ctx,
      ObIvfMapCentCache &cent_cache);

  static int local_scan_write_posting_list_count(
      const ObLSID &ls_id,
      const ObTabletID &centroid_tablet_id,
      const ObIvfWarmupCtx &ctx,
      ObIvfMapCentCache &cent_cache);

  // Local scan centroids into a HashMap (no cache required, no posting list count)
  static int local_scan_ivf_centroids_to_map(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObIvfWarmupCtx &ctx,
      common::hash::ObHashMap<int64_t, ObCentroidInfo> &cid_map,
      ObIAllocator &allocator);

  // Write centroids + posting count from KV scan result, then mark cache completed.
  static int kv_write_ivf_cent_cache(
      ObSpfreshTableHelper &table_helper,
      int64_t dim,
      const ObIArray<ObSpfreshCentroidRow> &centroids,
      ObIvfMapCentCache &cent_cache,
      bool &write_owned);

private:
  // Narrow decision point kept independent of SQL/cache state so bootstrap
  // restore can be verified with only a centroid list and a posting-count map.
  static int check_bootstrap_cid0_restore_needed_(
      const ObIArray<ObSpfreshCentroidRow> &centroids,
      const common::hash::ObHashMap<uint64_t, int64_t> &count_map,
      bool &need_restore,
      int64_t &cid0_posting);

  // SPFresh bootstrap cid=0 is cache-only. When aux_type=5 still has valid raw
  // vectors under cid=0 but the centroid scan omitted cid=0, restore cache entry.
  static int restore_bootstrap_cid0_from_kv_count_if_needed(
      const ObIArray<ObSpfreshCentroidRow> &centroids,
      const common::hash::ObHashMap<uint64_t, int64_t> &count_map,
      int64_t dim,
      const SCN &min_scn,
      ObIvfMapCentCache &cent_cache);

  class ObIvfWriteCacheFunc
  {
  public:
    ObIvfWriteCacheFunc(ObIvfICache &cent_cache) : cent_cache_(cent_cache), cent_idx_(0) {}
    int operator()(int64_t dim, float *data, int64_t cid = -1);
    bool need_get_cid() const { return cent_cache_.get_cache_type() == IvfCacheType::IVF_MAP_CENTROID_CACHE; }

  private:
    ObIvfICache &cent_cache_;
    int64_t cent_idx_;
  };

  class ObIvfUpdatePostingListCountFunc
  {
  public:
    ObIvfUpdatePostingListCountFunc(ObIvfMapCentCache &cent_cache) : cent_cache_(cent_cache) {}
    int operator()(int64_t cid, int64_t count);

  private:
    ObIvfMapCentCache &cent_cache_;
  };
  DISALLOW_COPY_AND_ASSIGN(ObIvfCacheUtil);
};

// Callback for process_ivf_aux_info: writes centroid data into a local ObHashMap<int64_t, ObCentroidInfo>
// Used in calc_center_id fallback path when cache is not available and ns_id > 0
class ObIvfWriteMapFunc
{
public:
  ObIvfWriteMapFunc(common::hash::ObHashMap<int64_t, ObCentroidInfo> &cid_map, ObIAllocator &allocator)
    : cid_map_(cid_map), allocator_(allocator) {}
  int operator()(int64_t dim, float *data, int64_t cid = -1);
  bool need_get_cid() const { return true; }

private:
  common::hash::ObHashMap<int64_t, ObCentroidInfo> &cid_map_;
  ObIAllocator &allocator_;
};

// Functor: wraps local ObHashMap<int64_t, ObCentroidInfo> iteration,
// extracts (cid, float *cid_vec) for each entry.
class ObCidMapForEachFunc
{
public:
  explicit ObCidMapForEachFunc(common::hash::ObHashMap<int64_t, ObCentroidInfo> &cid_map)
    : cid_map_(cid_map) {}
  template <typename Fn>
  int operator()(Fn &fn)
  {
    EntryAdapter<Fn> adapter(fn);
    return cid_map_.foreach_refactored(adapter);
  }
private:
  template <typename Fn>
  class EntryAdapter
  {
  public:
    explicit EntryAdapter(Fn &fn) : fn_(fn) {}
    int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
    {
      return fn_(entry.first, entry.second.cid_vec_);
    }
  private:
    Fn &fn_;
  };
  common::hash::ObHashMap<int64_t, ObCentroidInfo> &cid_map_;
};

// Functor: wraps ObIvfMapCentCache::for_each_centroid_vec, delegates
// visibility filtering and exclude_cid handling to the cache layer.
class ObMapCacheForEachFunc
{
public:
  ObMapCacheForEachFunc(ObIvfMapCentCache *cache,
                        const SCN &read_scn,
                        int64_t exclude_cid = -1,
                        int64_t max_posting_count = INT64_MAX,
                        bool normal_only = false)
    : cache_(cache),
      read_scn_(read_scn),
      exclude_cid_(exclude_cid),
      max_posting_count_(max_posting_count),
      normal_only_(normal_only)
  {}
  template <typename Fn>
  int operator()(Fn &fn)
  {
    int ret = common::OB_SUCCESS;
    if (normal_only_) {
      ret = cache_->for_each_normal_centroid_vec(
          read_scn_, fn, exclude_cid_, max_posting_count_);
    } else {
      ret = cache_->for_each_centroid_vec(read_scn_, fn, exclude_cid_);
    }
    return ret;
  }
private:
  ObIvfMapCentCache *cache_;
  SCN read_scn_;
  int64_t exclude_cid_;
  int64_t max_posting_count_;
  bool normal_only_;
};

// Helper: find nearest center from a map-based centroid source
// Explicit instantiations for ObCidMapForEachFunc and ObMapCacheForEachFunc in .cpp
template <typename ForEachFn>
int find_nearest_center_from_map(
    float *vector_data,
    int64_t dim,
    ForEachFn &for_each_fn,
    ObVectorIndexDistAlgorithm dis_algo,
    ObIAllocator &allocator,
    int64_t &center_idx);

}  // namespace share
}  // namespace oceanbase
#endif  // OCEANBASE_SHARE_IVF_CACHE_UTIL_H_
