/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_DRIVER_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_DRIVER_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/das/search/ob_das_search_define.h"
#include "share/hybrid_search/ob_query_parse.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

enum class ObVecFilterMode {
  VEC_FILTER_MODE_INVALID = 0,
  VEC_FILTER_MODE_PRE_FILTER,
  VEC_FILTER_MODE_SEARCH_DRIVER_FILTER,
  VEC_FILTER_MODE_EXPR_FILTER
};

enum class ObVecBruteAccessMode : uint8_t
{
  UNKNOWN = 0,
  WHOLE_RANGE_SCAN = 1,
  FILTERED_POINT_GET = 2
};

enum class ObVecBruteFallbackReason : uint8_t
{
  NONE = 0,
  NO_OR_EMPTY_FILTER = 1,
  OVER_ABSOLUTE_THRESHOLD = 2,
  OVER_SELECTIVITY_THRESHOLD = 3,
  STAT_UNAVAILABLE = 4,
  UNSUPPORTED = 5,
  SCALAR_FILTER_PUSHDOWN = 6
};

struct ObDASVecIndexDriverCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
ObDASVecIndexDriverCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_VEC_INDEX_DRIVER),
      sort_expr_(nullptr),
      limit_expr_(nullptr),
      offset_expr_(nullptr),
      vec_index_param_(),
      query_param_(),
      vector_index_param_(),
      algorithm_type_(ObVectorIndexAlgorithmType::VIAT_MAX),
      vec_type_(ObVecIndexType::VEC_INDEX_INVALID),
      row_count_(0),
      dim_(0),
      filter_mode_(ObKnnFilterMode::INVALID_KNN_FILTER_MODE),
      flags_(0) {}

  virtual ~ObDASVecIndexDriverCtDef() = default;

  bool need_pre_filter_iter() const
  { return ObVecIndexType::VEC_INDEX_INVALID == vec_type_
        || ObVecIndexType::VEC_INDEX_PRE == vec_type_
        || ObVecIndexType::VEC_INDEX_BRUTE_FORCE == vec_type_; }
  bool need_post_filter_iter() const
  { return ObVecIndexType::VEC_INDEX_PRE != vec_type_
        && ObVecIndexType::VEC_INDEX_BRUTE_FORCE != vec_type_; }

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef, 
                       K_(sort_expr), K_(limit_expr), K_(offset_expr), K_(vec_index_param), 
                       K_(dim), K_(vec_type), K_(algorithm_type), K_(row_count),
                       K_(vector_index_param), K_(query_param), K_(filter_mode),
                       K_(use_async_knn_pre_filter_group_alignment));
              
  ObExpr *sort_expr_;
  ObExpr *limit_expr_;
  ObExpr *offset_expr_;

  ObString vec_index_param_;
  ObVectorIndexQueryParam query_param_;
  ObVectorIndexParam vector_index_param_;
  ObVectorIndexAlgorithmType algorithm_type_;
  ObVecIndexType vec_type_;
  int64_t row_count_;
  int64_t dim_;
  ObKnnFilterMode filter_mode_;
  union {
    uint64_t flags_;
    struct {
      uint64_t use_async_knn_pre_filter_group_alignment_ : 1; // Whether the KNN PRE filter can use GroupMerge.
      uint64_t reserved_                                 : 63;
    };
  };
};

struct ObDASVecIndexDriverRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexDriverRtDef()
    : ObDASAttachRtDef(DAS_OP_VEC_INDEX_DRIVER) {}

  virtual ~ObDASVecIndexDriverRtDef() = default;
};

struct ObDASVecIndexDriverIterParam : public ObDASIterParam
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexDriverIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_VEC_INDEX_DRIVER),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      vec_index_scan_iter_(nullptr),
      filter_iter_(nullptr),
      pre_filter_iter_(nullptr),
      post_filter_iter_(nullptr),
      vec_index_driver_ctdef_(nullptr),
      vec_index_driver_rtdef_(nullptr),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      vec_idx_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN),
      search_ctx_(nullptr),
      sort_expr_(nullptr),
      limit_expr_(nullptr),
      offset_expr_(nullptr),
      score_expr_(nullptr),
      dim_(0),
      filter_mode_(ObVecFilterMode::VEC_FILTER_MODE_INVALID),
      scalar_scan_ctdef_(nullptr),
      scalar_scan_rtdef_(nullptr),
      filter_rtdef_for_reeval_(nullptr),
      go_brute_force_(false) {}

  virtual ~ObDASVecIndexDriverIterParam() {}

  virtual bool is_valid() const override
  {
    bool valid = nullptr != vec_index_driver_ctdef_ && 
                 nullptr != vec_index_driver_rtdef_ && 
                 nullptr != vec_index_scan_iter_ &&
                 nullptr != search_ctx_ && 
                 nullptr != sort_expr_ &&
                 nullptr != limit_expr_;
    return valid;
  }

  TO_STRING_KV(K_(vec_index_driver_ctdef),
              K_(vec_index_driver_rtdef),
              K_(filter_iter),
              K_(pre_filter_iter),
              K_(post_filter_iter),
              K_(vec_index_scan_iter),
              K_(vec_index_type),
              K_(vec_idx_try_path),
              K_(filter_mode));
  
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  
  ObDASVecIndexScanIter *vec_index_scan_iter_;
  ObDASIter *filter_iter_;
  ObDASIter *pre_filter_iter_;
  ObDASIter *post_filter_iter_;

  const ObDASVecIndexDriverCtDef *vec_index_driver_ctdef_;
  ObDASVecIndexDriverRtDef *vec_index_driver_rtdef_;

  ObVecIndexType vec_index_type_;
  ObVecIdxAdaTryPath vec_idx_try_path_;

  ObDASSearchCtx *search_ctx_;
  ObExpr* sort_expr_;
  ObExpr* limit_expr_;
  ObExpr* offset_expr_;
  ObExpr* score_expr_;

  int64_t dim_;

  // 表达式过滤模式所需参数
  ObVecFilterMode filter_mode_;
  const ObDASScalarScanCtDef *scalar_scan_ctdef_;
  ObDASScalarScanRtDef *scalar_scan_rtdef_;
  ObIDASSearchRtDef *filter_rtdef_for_reeval_;
  bool go_brute_force_;
};


class ObDASVecIndexDriverIter : public ObDASIter
{
public:
  ObDASVecIndexDriverIter()
    : ObDASIter(ObDASIterType::DAS_ITER_VEC_INDEX_DRIVER),
      ls_id_(ObLSID::INVALID_LS_ID),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      mem_context_(nullptr),
      vec_op_alloc_("VecIdxDriver", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      vec_index_scan_iter_(nullptr),
      filter_iter_(nullptr),
      pre_filter_iter_(nullptr),
      post_filter_iter_(nullptr),
      bitmap_(nullptr),
      vec_index_driver_ctdef_(nullptr),
      vec_index_driver_rtdef_(nullptr),
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID),
      vec_idx_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN),
      search_ctx_(nullptr),
      sort_expr_(nullptr),
      limit_expr_(nullptr),
      offset_expr_(nullptr),
      score_expr_(nullptr),
      limit_param_(),
      dim_(0),
      delta_buf_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      index_id_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      snapshot_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      com_aux_vec_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      ready_to_output_(false),
      query_cond_built_(false),
      search_vec_(nullptr),
      adaptor_vid_iter_(nullptr),
      iter_unfiltered_vid_cnt_(0),
      iter_added_cnt_(0),
      iter_scan_total_num_(0),
      vid_to_distance_(),
      filter_mode_(ObVecFilterMode::VEC_FILTER_MODE_INVALID),
      scalar_scan_ctdef_(nullptr),
      scalar_scan_rtdef_(nullptr),
      filter_rtdef_for_reeval_(nullptr),
      go_brute_force_(false),
      bruteforce_fallback_threshold_(ObVecIdxExtraInfo::MIN_DEFAULT_HNSW_BRUTEFORCE_FALLBACK_THRESHOLD),
      last_partition_row_count_(0),
      last_filter_est_row_count_(0),
      profile_(nullptr) {}

  virtual ~ObDASVecIndexDriverIter() {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids)
  {
    delta_buf_tablet_id_ = related_tablet_ids.delta_buf_tablet_id_;
    index_id_tablet_id_ = related_tablet_ids.index_id_tablet_id_;
    snapshot_tablet_id_ = related_tablet_ids.snapshot_tablet_id_;
    com_aux_vec_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  }
  void fill_related_tablet_ids(ObDASRelatedTabletID &related_tablet_ids) const;
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  int set_vec_index_param(ObString vec_index_param)
  {
    return ob_write_string(vec_op_alloc_, vec_index_param, vec_index_param_);
  }

  ObDASVecIndexScanIter *get_vec_index_scan_iter() const { return vec_index_scan_iter_; }
  void set_search_ctx(ObDASSearchCtx *search_ctx) { search_ctx_ = search_ctx; }
  const ObDASVecIndexDriverCtDef *get_driver_ctdef() const { return vec_index_driver_ctdef_; }

  static const uint64_t VSAG_MAX_EF_SEARCH = 160000;
  static constexpr double FIXED_MAGNIFICATION_RATIO = 2.0;
  static constexpr double FIXED_MAGNIFICATION_RATIO_EACH_ITERATIVE = 10.0;
  static constexpr double ITER_CONSIDER_LAST_SEARCH_SELETIVITY = 0.05;
  static constexpr double BRUTE_FORCE_POINT_GET_SELECTIVITY_THRESHOLD = 0.15;

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  struct ObIvfPathDecision
  {
    ObIvfPathDecision()
      : vec_index_type_(ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER),
        go_brute_force_(false),
        selectivity_(1.0)
    {}

    ObVecIndexType vec_index_type_;
    bool go_brute_force_;
    double selectivity_;
  };

  int init_limit_param();
  int init_sort();
  int process_pre_filter_mode(int64_t &count, int64_t capacity);
  int process_iterative_filter_mode(int64_t &count, int64_t capacity);
  int process_post_filter_mode(int64_t &count, int64_t capacity);
  int process_post_filter_mode_ivf(int64_t &count, int64_t capacity);
  int process_brute_force_mode(int64_t &count, int64_t capacity);
  int save_vec_socre_expr_result(ObNewRow *row, int64_t size);
  int distance_to_score(double distance, double &score);

  int build_bitmap_from_filter_iter();
  int set_bitmap_to_filter_iter();
  int fetch_vids_from_vec_index(int64_t &count, int64_t capacity);
  int post_query_vid_with_filter(ObIArray<VidDistancePair> *out_pairs = nullptr);
  int fill_results_to_eval_ctx(share::ObVectorQueryVidIterator *adaptor_vid_iter, int64_t &count, int64_t capacity);

  int post_query_vid_with_expr_filter();
  int do_expr_filter_batch();
  int filter_by_index_back(int64_t vid, float distance, bool &filter_res);
  int set_rowkey_by_vid(int64_t vid);
  int get_single_row_from_filter_iter(bool is_vectorized);
  
  int init_adaptor_vid_iter_if_null();
  int truncate_result_adaptor_vid_iter_by_limit();

  ObVecPathType get_vec_index_path_type();
  uint64_t get_bitmap_init_size() const;

  int evaluate_partition_path();
  int decide_ivf_partition_path_(int64_t filter_est_row_count,
                                 int64_t partition_row_count,
                                 ObIvfPathDecision &decision) const;
  int apply_ivf_partition_path_(const ObIvfPathDecision &decision);
  int refresh_bruteforce_fallback_threshold(int64_t partition_row_count);
  void update_go_brute_force_after_pre_filter();
  void switch_to_pre_filter();
  void switch_to_post_filter();

  // Exact knn without any vector index: the scan iter does the full scan, scoring and
  // top-K selection by itself, so none of the pre/post/iterative filter paths apply.
  OB_INLINE bool is_brute_force_scan() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_BRUTE_FORCE; }
  OB_INLINE bool is_pre_filter() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_PRE; }
  OB_INLINE bool is_iter_filter() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_POST_ITERATIVE_FILTER; }
  OB_INLINE bool is_post_filter() const { return is_post_without_filter() || is_post_with_filter(); }
  OB_INLINE bool is_ivf_index() const
  {
    return OB_NOT_NULL(vec_index_scan_iter_) &&
           vec_index_scan_iter_->get_type() == ObDASIterType::DAS_ITER_VEC_INDEX_IVF_SCAN;
  }
  OB_INLINE bool is_post_without_filter() const { return vec_index_type_ == ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER; }
  OB_INLINE bool is_post_with_filter() const
  {
    return vec_index_type_ == ObVecIndexType::VEC_INDEX_ADAPTIVE_SCAN &&
           vec_idx_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_POST_FILTER;
  }
  int process_iterative_filter_mode_ivf(int64_t &count, int64_t capacity);

  // IVF brute-force fallback helpers
  bool ivf_need_fallback_brute_force() const;
  int fallback_to_brute_force(int64_t &count, int64_t capacity);

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  lib::MemoryContext mem_context_;  // for temporary allocation 
  ObArenaAllocator vec_op_alloc_;

  ObDASVecIndexScanIter *vec_index_scan_iter_;
  ObDASIter *filter_iter_;
  ObDASIter *pre_filter_iter_;
  ObDASIter *post_filter_iter_;
  ObVecIndexBitmap bitmap_;

  const ObDASVecIndexDriverCtDef *vec_index_driver_ctdef_;
  ObDASVecIndexDriverRtDef *vec_index_driver_rtdef_;

  ObVecIndexType vec_index_type_;
  ObVecIdxAdaTryPath vec_idx_try_path_;

  ObDASSearchCtx *search_ctx_;
  ObExpr* sort_expr_;
  ObExpr* limit_expr_;
  ObExpr* offset_expr_;
  ObExpr* score_expr_;
  ObLimitParam limit_param_;
  int64_t dim_;

  ObTabletID delta_buf_tablet_id_;
  ObTabletID index_id_tablet_id_;
  ObTabletID snapshot_tablet_id_;
  ObTabletID com_aux_vec_tablet_id_;

  bool ready_to_output_;
  bool query_cond_built_;

  ObExpr* search_vec_;

  ObVectorQueryVidIterator* adaptor_vid_iter_;

  // for iterative filter mode: use index merge filter
  int64_t iter_unfiltered_vid_cnt_;
  int64_t iter_added_cnt_;
  int64_t iter_scan_total_num_;
  hash::ObHashMap<int64_t, float> vid_to_distance_;

  static const uint64_t MAX_BITMAP_INIT_SIZE = 20000;

  // for iterative filter mode： use expr filter
  ObVecFilterMode filter_mode_;
  const ObDASScalarScanCtDef *scalar_scan_ctdef_;
  ObDASScalarScanRtDef *scalar_scan_rtdef_;
  ObIDASSearchRtDef *filter_rtdef_for_reeval_;
  bool go_brute_force_;
  int64_t bruteforce_fallback_threshold_;
  int64_t last_partition_row_count_;
  int64_t last_filter_est_row_count_;
  ObString vec_index_param_;
  common::ObObj vid_obj_for_lookup_;
  common::ObOpProfile<common::ObMetric> *profile_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_DRIVER_ITER_H_ */
