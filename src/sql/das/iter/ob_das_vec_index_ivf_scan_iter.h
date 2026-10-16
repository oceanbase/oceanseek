/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_IVF_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_IVF_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

using ObIvfRowkeyHeap = share::ObVectorCenterClusterHelper<float, ObRowkey>;

enum class ObIvfFilterMode {
  IVF_FILTER_MODE_PRE_FILTER = 0,
  IVF_FILTER_MODE_ITER_FILTER,
  IVF_FILTER_MODE_POST_FILTER
};

// IVF-specific query conditions
struct ObIVFQueryConditions : public share::ObVecQueryCondBase
{
  ObIVFQueryConditions()
    : ObVecQueryCondBase(),
      nprobe_(8) {}

  bool is_inited() override { return query_vector_.length() > 0 && nprobe_ > 0; }
  void reset() override {
    ObVecQueryCondBase::reset();
    nprobe_ = 8;
  }

  int64_t nprobe_;

  TO_STRING_KV(K_(query_limit), K_(query_order), K_(query_vector), K_(nprobe));
};

struct ObDASVecIndexIVFScanIterParam : public ObDASIterParam
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexIVFScanIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_VEC_INDEX_IVF_SCAN),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      centroid_iter_(nullptr),
      cid_vec_iter_(nullptr),
      rowkey_cid_iter_(nullptr),
      com_aux_iter_(nullptr),
      special_aux_iter_(nullptr),
      vec_index_scan_ctdef_(nullptr),
      vec_index_scan_rtdef_(nullptr),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      sort_expr_(nullptr),
      search_ctx_(nullptr) {}

  virtual bool is_valid() const override
  {
    return ls_id_.is_valid() &&
           nullptr != tx_desc_ &&
           nullptr != snapshot_ &&
           nullptr != centroid_iter_ &&
           nullptr != cid_vec_iter_ &&
           nullptr != rowkey_cid_iter_ &&
           nullptr != com_aux_iter_ &&
           nullptr != vec_index_scan_ctdef_ &&
           nullptr != vec_index_scan_rtdef_;
  }

  TO_STRING_KV(K_(vec_index_scan_ctdef),
               K_(vec_index_scan_rtdef),
               K_(vec_index_type));

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *centroid_iter_;
  ObDASScanIter *cid_vec_iter_;
  ObDASScanIter *rowkey_cid_iter_;
  ObDASScanIter *com_aux_iter_;
  ObDASScanIter *special_aux_iter_;  // nullable for IVF_FLAT

  const ObDASVecIndexIVFScanCtDef *vec_index_scan_ctdef_;
  ObDASVecIndexIVFScanRtDef *vec_index_scan_rtdef_;

  ObVecIndexType vec_index_type_;
  ObExpr *sort_expr_;
  ObDASSearchCtx *search_ctx_;
};

// ==================== IVF Base Class ====================
// Common infrastructure shared by IVF_FLAT and IVF_PQ.
// Handles: memory management, sub-table iterators, centroid scanning framework,
// cid range helpers, heap building, search preparation, lifecycle.
class ObDASVecIndexIVFBaseScanIter : public ObDASVecIndexScanIter
{
public:
  ObDASVecIndexIVFBaseScanIter(ObDASIterType type = ObDASIterType::DAS_ITER_VEC_INDEX_IVF_SCAN)
    : ObDASVecIndexScanIter(type),
      mem_context_(nullptr),
      vec_op_alloc_("IvfVecScanOp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      centroid_iter_(nullptr),
      cid_vec_iter_(nullptr),
      rowkey_cid_iter_(nullptr),
      com_aux_iter_(nullptr),
      special_aux_iter_(nullptr),
      centroid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      cid_vec_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      rowkey_cid_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      com_aux_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      special_aux_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      centroid_scan_param_(),
      cid_vec_scan_param_(),
      com_aux_scan_param_(),
      special_aux_scan_param_(),
      centroid_iter_first_scan_(true),
      cid_vec_iter_first_scan_(true),
      com_aux_iter_first_scan_(true),
      special_aux_iter_first_scan_(true),
      vec_index_scan_ctdef_(nullptr),
      vec_index_scan_rtdef_(nullptr),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      sort_expr_(nullptr),
      vec_index_param_(),
      dim_(0),
      need_norm_(false),
      search_prepared_(false),
      nprobes_(8),
      similarity_threshold_(0),
      distance_threshold_(FLT_MAX),
      max_scan_vectors_(0),
      cid_vec_scan_rows_(0),
      real_search_vec_(),
      ivf_query_cond_(),
      adaptor_vid_iter_(nullptr),
      is_single_int64_rowkey_(false),
      center_cache_guard_(),
      ns_id_(share::ObIvfCacheMgrKey::INVALID_NS_ID),
      ltable_id_(share::ObIvfCacheMgrKey::INVALID_LTABLE_ID),
      ivf_filter_mode_(ObIvfFilterMode::IVF_FILTER_MODE_PRE_FILTER),
      post_filter_centers_inited_(false),
      brute_force_done_(false),
      need_fallback_brute_force_(false),
      selectivity_(0.0),
      heap_size_(0),
      cumulative_added_cnt_(0),
      profile_(nullptr)
  {
    dis_type_ = ObExprVectorDistance::ObVecDisType::MAX_TYPE;
    rowkey_pairs_.set_attr(ObMemAttr(MTL_ID(), "IvfBfRkPair"));
    distances_.set_attr(ObMemAttr(MTL_ID(), "IvfBfDist"));
  }
  virtual ~ObDASVecIndexIVFBaseScanIter() {}

  // ObDASVecIndexScanIter virtual overrides
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void set_adaptor(share::ObPluginVectorIndexAdaptor *adaptor) override
  { UNUSED(adaptor); }
  virtual void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids) override;
  void fill_related_tablet_ids(ObDASRelatedTabletID &related_tablet_ids) const;
  virtual void set_ls_id(const share::ObLSID &ls_id) override { ls_id_ = ls_id; }

  virtual share::ObVectorQueryVidIterator* get_adaptor_vid_iter() override
  { return adaptor_vid_iter_; }

  void set_ivf_filter_mode(ObIvfFilterMode mode) { ivf_filter_mode_ = mode; }
  void prepare_partition_path(ObIvfFilterMode mode, double selectivity);
  OB_INLINE bool is_pre_filter() const { return ivf_filter_mode_ == ObIvfFilterMode::IVF_FILTER_MODE_PRE_FILTER; }
  OB_INLINE bool is_iter_filter() const { return ivf_filter_mode_ == ObIvfFilterMode::IVF_FILTER_MODE_ITER_FILTER; }
  OB_INLINE bool is_post_filter() const { return ivf_filter_mode_ == ObIvfFilterMode::IVF_FILTER_MODE_POST_FILTER; }
  bool need_fallback_brute_force() const { return need_fallback_brute_force_; }
  void set_brute_force_query_limit(int64_t limit) { ivf_query_cond_.query_limit_ = limit; }

  // Selectivity and heap size for post/iter filter alignment with original scan iter
  void set_selectivity(double s) { selectivity_ = s; }
  double get_selectivity() const { return selectivity_; }
  void set_heap_size(int64_t hs) { heap_size_ = hs; }
  void set_cumulative_added(int64_t cnt) { cumulative_added_cnt_ = cnt; }
  int64_t get_nprobes() const { return nprobes_; }
  float get_similarity_threshold() const { return similarity_threshold_; }
  int64_t get_max_scan_vectors() const { return max_scan_vectors_; }
  int64_t get_cid_vec_scan_rows() const { return cid_vec_scan_rows_; }
  bool has_next_center() const { return iterative_filter_ctx_.has_next_center(); }

  static const uint64_t MAX_IVF_BRUTE_FORCE_SIZE = 10000;

  // ===== Strategy interface implementations =====
  virtual int build_query_condition(ObIAllocator &alloc, ObEvalCtx &eval_ctx,
                                    ObExpr *search_vec, bool is_iter_filter, uint64_t limit) override;
  virtual int adjust_search_for_next_round(bool first_search,
      share::ObVectorQueryVidIterator *adaptor_vid_iter,
      int64_t iter_unfiltered_vid_cnt, int64_t iter_added_cnt,
      int64_t iter_scan_total_num, bool &stop_search) override;
  virtual int64_t get_max_query_result_size() const override { return INT64_MAX; }

  // Deferred distance: compute distance only for filtered vids, rebuild adaptor_vid_iter_
  // Called by DriverIter in deferred-calc mode after filtering.
  virtual int calc_distance_for_vids(common::ObIArray<VidDistancePair> &all_pairs, int64_t start_idx) { return OB_NOT_IMPLEMENT; }

protected:
  // --- lifecycle ---
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override { return OB_NOT_IMPLEMENT; }
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override = 0;
  int assign_async_scan_param_snapshots();

  // Common pre-search logic: check ivf_query_cond_, set real_search_vec_, normalize if needed.
  // Subclasses call this at the beginning of inner_get_next_rows().
  int prepare_search();

  // --- Table scan helpers ---
  int do_centroid_table_full_scan(bool is_vectorized);
  int do_aux_table_scan(bool &first_scan,
                        ObTableScanParam &scan_param,
                        const ObDASScanCtDef *ctdef,
                        ObDASScanRtDef *rtdef,
                        ObDASScanIter *iter,
                        ObTabletID &tablet_id);

  // --- CID range query helpers ---
  int build_cid_vec_query_range(const ObString &cid, int64_t rowkey_cnt, ObNewRange &cid_pri_key_range);
  int build_cid_vec_query_rowkey(const ObString &cid, bool is_min, int64_t rowkey_cnt, ObRowkey &rowkey);
  int scan_cid_range(const ObString &cid, int64_t cid_vec_pri_key_cnt,
                     const ObDASScanCtDef *cid_vec_ctdef, ObDASScanRtDef *cid_vec_rtdef);
  int prepare_cid_range(const ObDASScanCtDef *cid_vec_ctdef,
                        int64_t &cid_vec_column_count,
                        int64_t &cid_vec_pri_key_cnt,
                        int64_t &rowkey_cnt);

  // --- Distance to score conversion ---
  // IVF normal path stores raw dot product for COSINE; convert to cosine_distance before scoring.
  int distance_to_score(ObExpr *sort_expr, double distance, double &score) override;

  // --- Brute-force scan (shared by FLAT/PQ) ---
  // Scans COM_AUX table directly, computes exact distance for all candidates.
  // Used when go_brute_force_ is set or centroid table is empty.
  int do_brute_force_scan(bool is_vectorized);

  // --- Post-filter mode (DriverIter-driven iterative expansion) ---
  // Subclass implements: expand to next batch of center IDs
  virtual int expand_to_next_centers(bool is_vectorized, int64_t next_nprobe) = 0;
  // Subclass implements: single-round scan of current center batch
  virtual int do_post_filter_round(bool is_vectorized) = 0;

  // --- Deferred distance mode (collect vids first, compute distance after filtering) ---
  // Subclass implements: collect all vid+vec/pq_ids without computing distance
  virtual int do_post_filter_collect_round(bool is_vectorized) { return OB_NOT_IMPLEMENT; }

  // --- Adaptor vid iter lifecycle ---
  void reset_adaptor_vid_iter();

  // --- Result construction ---
  int build_vid_iter_from_rowkey_heap(ObIvfRowkeyHeap &nearest_rowkey_heap);

  // --- General helpers ---
  int64_t get_nprobe(int64_t enlargement_factor = 1);
  int init_memory_context();
  int parse_vec_index_param();

  int calc_vec_dis(float *a, float *b, int dim, float &dis, ObExprVectorDistance::ObVecDisType dis_type);

  // --- Center cache helpers ---
  bool is_ns_id_valid() const { return ns_id_ != share::ObIvfCacheMgrKey::INVALID_NS_ID; }
  int resolve_ns_ltable_for_cache_();
  int get_centers_cache(bool is_vectorized, bool is_pq_centers,
                        share::ObIvfCacheMgrGuard &cache_guard,
                        share::ObIvfICache *&cent_cache,
                        bool &is_cache_usable);
  int try_write_centroid_cache(share::ObIvfICache &cent_cache, bool is_vectorized);
  virtual int try_write_pq_centroid_cache(share::ObIvfICache &cent_cache, bool is_vectorized);
  int write_centroids_from_scan(share::ObIvfICache &cent_cache, bool is_vectorized, bool use_add_centroid);
  int do_special_aux_table_full_scan(bool is_vectorized);
  int gen_near_cid_heap_from_cache(share::ObIvfICache &cent_cache,
                                   share::ObVectorCenterClusterHelper<float, ObCenterId> &nearest_cid_heap);

  // Constants
  static const int64_t CENTROID_PRI_KEY_CNT = 1;
  static const int64_t CID_VEC_COM_KEY_CNT = 1;
  static const int64_t CID_VEC_FIXED_PRI_KEY_CNT = 1;
  static const int64_t CID_IDX = 0;
  static const int64_t CID_VECTOR_IDX = 1;
  static const int64_t CID_VEC_ROWKEY_START_IDX = CID_VEC_COM_KEY_CNT + CID_VEC_FIXED_PRI_KEY_CNT; // 2

protected:
  // Memory management
  lib::MemoryContext mem_context_;
  ObArenaAllocator vec_op_alloc_;

  // Core params
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  // Sub-table iterators
  ObDASScanIter *centroid_iter_;
  ObDASScanIter *cid_vec_iter_;
  ObDASScanIter *rowkey_cid_iter_;
  ObDASScanIter *com_aux_iter_;
  ObDASScanIter *special_aux_iter_;

  // Tablet IDs
  ObTabletID centroid_tablet_id_;
  ObTabletID cid_vec_tablet_id_;
  ObTabletID rowkey_cid_tablet_id_;
  ObTabletID com_aux_tablet_id_;
  ObTabletID special_aux_tablet_id_;

  // Table scan params
  ObTableScanParam centroid_scan_param_;
  ObTableScanParam cid_vec_scan_param_;
  ObTableScanParam com_aux_scan_param_;
  ObTableScanParam special_aux_scan_param_;

  // First scan flags
  bool centroid_iter_first_scan_;
  bool cid_vec_iter_first_scan_;
  bool com_aux_iter_first_scan_;
  bool special_aux_iter_first_scan_;

  // CtDef / RtDef
  const ObDASVecIndexIVFScanCtDef *vec_index_scan_ctdef_;
  ObDASVecIndexIVFScanRtDef *vec_index_scan_rtdef_;

  ObVecIndexType vec_index_type_;
  ObExpr *sort_expr_;
  // Search parameters
  ObVectorIndexParam vec_index_param_;
  int64_t dim_;
  bool need_norm_;
  bool search_prepared_;
  ObExprVectorDistance::ObVecDisType dis_type_;
public:
  ObExprVectorDistance::ObVecDisType get_dis_type() const { return dis_type_; }
protected:
  int64_t nprobes_;
  float similarity_threshold_;
  float distance_threshold_;
  int64_t max_scan_vectors_;
  int64_t cid_vec_scan_rows_;
  ObString real_search_vec_;

  // IVF query conditions
  ObIVFQueryConditions ivf_query_cond_;

  // Result output
  share::ObVectorQueryVidIterator *adaptor_vid_iter_;

  // Rowkey type flag
  bool is_single_int64_rowkey_;

  // Centroid iteration (iterative expansion context)
  ObIvfIterativeFilterContext iterative_filter_ctx_;

  // Center cache
  share::ObIvfCacheMgrGuard center_cache_guard_;
  // Batch guard protecting cid_vec_ pointers shallow-copied into nearest_cid_heap.
  // Lifetime: ref_cnt_++ at gen_near_cid_heap_from_cache (ns_id path) entry,
  // released either at the next entry or at inner_reuse / inner_release.
  share::ObCentroidVecBatchGuard batch_guard_;
  int64_t ns_id_;
  uint64_t ltable_id_;

  // Filter mode for IVF scan
  ObIvfFilterMode ivf_filter_mode_;
  bool post_filter_centers_inited_;

  // Brute-force scan state
  bool brute_force_done_;              // set true after do_brute_force_scan completes
  bool need_fallback_brute_force_;     // set true when centroid is empty in post-filter, signals driver to fallback

  // Post/iter filter alignment with original scan iter
  double selectivity_;                   // filter selectivity from optimizer, used to compute initial heap_size_
  int64_t heap_size_;                    // 0 = not set, do_post_filter_round falls back to query_limit_
  int64_t cumulative_added_cnt_;         // cumulative filtered-pass count, set by driver each round

  // Temp arrays for build_vid_iter_from_rowkey_heap
  ObSEArray<std::pair<ObRowkey, float *>, 16> rowkey_pairs_;
  ObSEArray<float, 16> distances_;
  common::ObOpProfile<common::ObMetric> *profile_;
};

// ==================== IVF_FLAT Scan Iterator ====================
// Handles IVF_FLAT vector index scanning with raw vector distance computation.
class ObDASVecIndexIVFScanIter : public ObDASVecIndexIVFBaseScanIter
{
public:
  ObDASVecIndexIVFScanIter()
    : ObDASVecIndexIVFBaseScanIter(ObDASIterType::DAS_ITER_VEC_INDEX_IVF_SCAN)
  {
    near_cid_.set_attr(ObMemAttr(MTL_ID(), "IvfVecNearCid"));
  }
  virtual ~ObDASVecIndexIVFScanIter() {}

protected:
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int expand_to_next_centers(bool is_vectorized, int64_t next_nprobe) override;
  virtual int do_post_filter_round(bool is_vectorized) override;

  // Deferred distance mode overrides
  virtual int do_post_filter_collect_round(bool is_vectorized) override;
  virtual int calc_distance_for_vids(common::ObIArray<VidDistancePair> &all_pairs, int64_t start_idx) override;

private:
  // Centroid query: stores only center IDs (no center vectors needed for FLAT)
  int get_nearest_probe_center_ids(bool is_vectorized,
                                   bool force_persistent_scan = false);
  int get_next_center_ids(bool is_vectorized, int64_t next_nprobe);

  // IVF_FLAT pre-filter core
  int do_ivf_flat_pre_filter(bool is_vectorized);
  int get_nearest_limit_rowkeys_in_cids(bool is_vectorized,
                                        ObIvfRowkeyHeap &nearest_rowkey_heap);
  int get_rowkeys_to_heap(const ObString &cid_str,
                          int64_t cid_vec_pri_key_cnt,
                          int64_t cid_vec_column_count,
                          int64_t rowkey_cnt,
                          bool is_vectorized,
                          ObIvfRowkeyHeap &nearest_rowkey_heap);

  // Deferred distance: collect vid+vec without computing distance
  int collect_all_vid_vecs_in_cids(bool is_vectorized);
  int collect_vid_vecs_from_cid(const ObString &cid_str,
                                int64_t cid_vec_pri_key_cnt,
                                int64_t cid_vec_column_count,
                                int64_t rowkey_cnt,
                                bool is_vectorized);
  int build_vid_iter_from_vid_vec_map();

private:
  // FLAT stores only center IDs (unlike PQ which also stores center vectors)
  ObSEArray<ObCenterId, 16> near_cid_;

  // Deferred distance: vid -> deep-copied raw vector (allocated from vec_op_alloc_)
  hash::ObHashMap<int64_t, float *> vid_vec_map_;
};

// ==================== IVF_PQ Scan Iterator ====================
// Handles IVF_PQ vector index scanning with product quantization distance.
// PQ algorithm: residual = search_vec - center_vec, split into m subspaces,
// codebook lookup per subspace, total distance = sum(sub-distances).
// Supports 3 distance paths: precompute table, cache, table fallback.
class ObDASVecIndexIVFPQScanIter : public ObDASVecIndexIVFBaseScanIter
{
public:
  using IvfCidVecPair = std::pair<ObCenterId, float *>;

  ObDASVecIndexIVFPQScanIter()
    : ObDASVecIndexIVFBaseScanIter(ObDASIterType::DAS_ITER_VEC_INDEX_IVF_SCAN),
      m_(0),
      nbits_(0)
  {
    near_cid_vec_.set_attr(ObMemAttr(MTL_ID(), "IvfPQCidVec"));
    near_cid_vec_dis_.set_attr(ObMemAttr(MTL_ID(), "IvfPQCidDis"));
  }
  virtual ~ObDASVecIndexIVFPQScanIter() {}

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int expand_to_next_centers(bool is_vectorized, int64_t next_nprobe) override;
  virtual int do_post_filter_round(bool is_vectorized) override;

  // Deferred distance mode overrides
  virtual int do_post_filter_collect_round(bool is_vectorized) override;
  virtual int calc_distance_for_vids(common::ObIArray<VidDistancePair> &all_pairs, int64_t start_idx) override;

  // PQ centroid cache override
  virtual int try_write_pq_centroid_cache(share::ObIvfICache &cent_cache, bool is_vectorized) override;

private:
  // PQ pre-filter core
  int do_ivf_pq_pre_filter(bool is_vectorized);

  // PQ centroid scanning: stores center_id + center_vector pairs (needed for residual)
  int get_nearest_probe_center_ids_with_vec(bool is_vectorized,
                                            bool force_persistent_scan = false);
  int get_next_center_ids_with_vec(bool is_vectorized, int64_t next_nprobe);

  // PQ distance computation core
  int calc_nearest_limit_rowkeys_in_cids(bool is_vectorized,
                                         float *search_vec,
                                         ObIvfRowkeyHeap &nearest_rowkey_heap);

  // 3 PQ distance paths
  int calc_distance_between_pq_ids(bool is_vectorized,
                                   const ObString &pq_center_ids,
                                   const ObIArray<float *> &splited_residual,
                                   float &distance);
  int calc_distance_between_pq_ids_by_cache(share::ObIvfICache &cent_cache,
                                            const ObString &pq_center_ids,
                                            const ObIArray<float *> &splited_residual,
                                            float &distance);
  int calc_distance_between_pq_ids_by_table(bool is_vectorized,
                                            const ObString &pq_center_ids,
                                            const ObIArray<float *> &splited_residual,
                                            int64_t batch_row_count,
                                            float &distance);

  // Precompute table support
  int check_can_pre_compute(bool is_vectorized,
                            share::ObIvfCacheMgrGuard &pre_cache_guard,
                            share::ObIvfICache *&pre_cent_cache,
                            bool &pre_compute_table);
  int pre_compute_inner_prod_table(const float *search_vec, float *dis_table, bool is_vectorized);
  int calc_distance_with_precompute(ObEvalCtx::BatchInfoScopeGuard &guard,
                                    int64_t scan_row_cnt, int64_t rowkey_cnt,
                                    ObRowkey &filter_main_rowkey,
                                    float *sim_table, float dis0,
                                    ObIvfRowkeyHeap &nearest_rowkey_heap);

  // Cache helpers (get_centers_cache inherited from Base class)
  int get_pq_precomputetable_cache(bool is_vectorized,
                                   share::ObIvfCacheMgrGuard &cache_guard,
                                   share::ObIvfICache *&cent_cache,
                                   bool &is_cache_usable);
  int try_write_pq_precompute_table_cache(share::ObIvfICache &cent_cache, bool is_vectorized);

  // VidIdxInfo: used by calc_distance_for_vids to group vids by cid
  struct VidIdxInfo {
    int64_t pair_idx;  // index in all_pairs
    int64_t vid;
    TO_STRING_KV(K(pair_idx), K(vid));
  };
  int group_vids_by_cid(common::ObIArray<VidDistancePair> &all_pairs,
                        int64_t start_idx,
                        ObSEArray<ObSEArray<VidIdxInfo, 16>, 16> &cid_groups);
  int calc_precompute_distance_for_group(const ObSEArray<VidIdxInfo, 16> &group,
                                         float dis0,
                                         common::ObIArray<VidDistancePair> &all_pairs);
  int calc_residual_distance_for_group(const ObSEArray<VidIdxInfo, 16> &group,
                                       float dis0,
                                       common::ObIArray<VidDistancePair> &all_pairs);

  // Deferred distance: collect pq vids without computing distance
  struct PQVidInfo {
    int64_t cid_idx;  // index into near_cid_vec_ for restoring center context
    ObString pq_ids;  // deep-copied pq_ids string
  };
  int collect_all_pq_vids_in_cids(bool is_vectorized);
  int collect_pq_vids_from_cid(int64_t cid_idx, const ObString &cid_str,
                                int64_t cid_vec_pri_key_cnt,
                                int64_t cid_vec_column_count,
                                int64_t rowkey_cnt,
                                bool is_vectorized);
  int build_vid_iter_from_vid_pq_map();
  int prepare_pq_pre_compute_ctx();

  // Precomputed distance context (prepared once per search, reused across calc_distance_for_vids calls)
  struct PQPreComputeCtx {
    bool prepared_;
    bool pre_compute_table_;
    bool is_l2_;
    share::ObIvfCacheMgrGuard pre_cache_guard_;
    share::ObIvfICache *pre_cent_cache_;
    float *sim_table_;
    float *sim_table_2_;
    float *residual_;
    ObArray<float *> splited_residual_;

    PQPreComputeCtx()
      : prepared_(false),
        pre_compute_table_(false),
        is_l2_(false),
        pre_cent_cache_(nullptr),
        sim_table_(nullptr),
        sim_table_2_(nullptr),
        residual_(nullptr)
    {}
    void reset() {
      prepared_ = false;
      pre_compute_table_ = false;
      is_l2_ = false;
      pre_cent_cache_ = nullptr;
      sim_table_ = nullptr;
      sim_table_2_ = nullptr;
      residual_ = nullptr;
      pre_cache_guard_.reset();
      splited_residual_.reuse();
    }
  };

  // PQ-specific constants
  static const int64_t PQ_CENTROID_VEC_IDX = 1;
  static const int64_t CIDS_IDX = 0;
  static const int64_t PQ_IDS_IDX = 1;

  // PQ parameters
  int64_t m_;      // number of subspaces
  int64_t nbits_;  // bits per code per subspace

  // PQ centroid: stores center_id + center_vector (needed for residual computation)
  ObArray<IvfCidVecPair> near_cid_vec_;
  ObArray<float> near_cid_vec_dis_;
  ObArrayWrap<float *> near_cid_vec_ptrs_;

  // Deferred distance: vid -> (cid_idx, pq_ids) for deferred PQ distance computation
  hash::ObHashMap<int64_t, PQVidInfo> vid_pq_map_;

  PQPreComputeCtx pq_pre_compute_ctx_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_IVF_SCAN_ITER_H_ */
