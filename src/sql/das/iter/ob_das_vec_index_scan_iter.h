/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "src/share/vector_index/ob_plugin_vector_index_service.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

namespace oceanbase
{
using namespace common;
namespace share
{
class ObVecQueryCondBase;
class ObVectorQueryVidIterator;
}
namespace sql
{

struct ObVecIndexBitmap 
{
  static const uint64_t NORMAL_BITMAP_MAX_SIZE = 10000000; 
  enum FilterType {
    BYTE_ARRAY = 0,
    ROARING_BITMAP = 1,
    VIDS = 2,
  };

  union {
    uint8_t *bitmap_;
    roaring::api::roaring64_bitmap_t *roaring_bitmap_;
    int64_t *vids_;
  };

  FilterType type_;
  uint64_t capacity_;
  uint64_t valid_cnt_;
  ObIAllocator *allocator_;
  int64_t min_vid_;
  int64_t max_vid_;
  int64_t min_vid_bound_;
  int64_t max_vid_bound_;
  uint64_t tenant_id_;

  ObVecIndexBitmap(ObIAllocator *allocator) 
    : type_(VIDS), capacity_(0), valid_cnt_(0), allocator_(allocator), 
      min_vid_(INT64_MAX), max_vid_(INT64_MIN), min_vid_bound_(INT64_MAX),
      max_vid_bound_(INT64_MIN), tenant_id_(MTL_ID())
  {
    vids_ = nullptr;
  }

  void set_allocator(ObIAllocator *allocator) { allocator_ = allocator; }
  int init(int64_t min_vid, int64_t max_vid, uint64_t capacity);
  int add_vid(int64_t vid);
  bool test(int64_t vid);
  int64_t get_valid_cnt() const { return static_cast<int64_t>(valid_cnt_); }
  int get_roaring_cardinality(int64_t &cardinality) const;
  int64_t *get_vids() { return (type_ == VIDS) ? vids_ : nullptr; }
  void reset();
  

  int upgrade_to_byte_array();
  int upgrade_to_roaring_bitmap();
};

struct ObVecIndexBitmapIter
{
  ObVecIndexBitmapIter()
    : bitmap_(nullptr),
      iter_(nullptr),
      inited_(false),
      curr_vid_(OB_INVALID_INDEX)
  {}

  int init(ObVecIndexBitmap *bitmap);
  int get_vid_batch(int64_t *vids, int64_t capacity, int64_t &vid_cnt);
  int advance_to(int64_t target_vid, int64_t &return_vid);
  int next_vid(int64_t &next_vid);
  int get_curr_vid(int64_t &curr_vid);
  void reset();
  bool is_inited() const { return inited_; }
  bool is_end() const { return curr_vid_ == OB_INVALID_INDEX; }

private:
  ObVecIndexBitmap *bitmap_;
  roaring::api::roaring64_iterator_t *iter_;
  bool inited_;
  int64_t curr_vid_;
};


// Base class for vector index scan ctdef
struct ObDASVecIndexScanCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexScanCtDef(common::ObIAllocator &alloc, ObDASOpType op_type = DAS_OP_VEC_INDEX_SCAN)
    : ObDASAttachCtDef(alloc, op_type),
      vec_index_param_(),
      vector_index_param_(),
      algorithm_type_(ObVectorIndexAlgorithmType::VIAT_MAX),
      vec_type_(ObVecIndexType::VEC_INDEX_INVALID),
      dim_(0),
      sort_expr_(nullptr) {}

  virtual ~ObDASVecIndexScanCtDef() = default;

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       K_(vec_index_param), K_(vector_index_param), K_(algorithm_type), 
                       K_(vec_type), K_(dim), K_(sort_expr));

  ObString vec_index_param_;
  ObVectorIndexQueryParam query_param_;
  ObVectorIndexParam vector_index_param_;

  ObVectorIndexAlgorithmType algorithm_type_;
  ObVecIndexType vec_type_;
  int64_t dim_;
  ObExpr *sort_expr_;
};

// HNSW specific ctdef, inherits from base class
struct ObDASVecIndexHNSWScanCtDef : ObDASVecIndexScanCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexHNSWScanCtDef(common::ObIAllocator &alloc)
    : ObDASVecIndexScanCtDef(alloc, DAS_OP_VEC_INDEX_HNSW_SCAN) {}

  virtual ~ObDASVecIndexHNSWScanCtDef() = default;

  // HNSW specific child table indices
  static const int64_t DELTA_BUF_TABLE_IDX = 0;
  static const int64_t INDEX_ID_TABLE_IDX = 1;
  static const int64_t SNAPSHOT_TABLE_IDX = 2;
  static const int64_t COM_AUX_VEC_TABLE_IDX = 3;

  const ObDASScanCtDef *get_child_ctdef(int64_t index, ObTSCIRScanType scan_type) const
  {
    const ObDASScanCtDef *child_ctdef = nullptr;
    if (children_cnt_ > index && index >= 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[index]);
      if (child->ir_scan_type_ == scan_type) {
        child_ctdef = child;
      }
    }
    return child_ctdef;
  }

  const ObDASScanCtDef *get_delta_buf_table_ctdef() const
  {
    return get_child_ctdef(DELTA_BUF_TABLE_IDX, ObTSCIRScanType::OB_VEC_DELTA_BUF_SCAN);
  }

  const ObDASScanCtDef *get_index_id_table_ctdef() const
  {
    return get_child_ctdef(INDEX_ID_TABLE_IDX, ObTSCIRScanType::OB_VEC_IDX_ID_SCAN);
  }

  const ObDASScanCtDef *get_snapshot_table_ctdef() const
  {
    return get_child_ctdef(SNAPSHOT_TABLE_IDX, ObTSCIRScanType::OB_VEC_SNAPSHOT_SCAN);
  }

  const ObDASScanCtDef *get_com_aux_vec_table_ctdef() const
  {
    return get_child_ctdef(COM_AUX_VEC_TABLE_IDX, ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  }
};

// Brute force ctdef: no auxiliary vector index table, the only child is a main table scan
// that supplies (rowkey, vector column) for exact distance computation.
struct ObDASVecIndexBruteScanCtDef : ObDASVecIndexScanCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexBruteScanCtDef(common::ObIAllocator &alloc)
    : ObDASVecIndexScanCtDef(alloc, DAS_OP_VEC_INDEX_BRUTE_SCAN),
      filter_pushdown_(false) {}

  virtual ~ObDASVecIndexBruteScanCtDef() = default;

  static const int64_t MAIN_TABLE_IDX = 0;

  const ObDASScanCtDef *get_main_table_ctdef() const
  {
    const ObDASScanCtDef *child_ctdef = nullptr;
    if (children_cnt_ > MAIN_TABLE_IDX && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[MAIN_TABLE_IDX]);
      if (OB_NOT_NULL(child)
          && child->ir_scan_type_ == ObTSCIRScanType::OB_VEC_BRUTE_MAIN_SCAN) {
        child_ctdef = child;
      }
    }
    return child_ctdef;
  }

  INHERIT_TO_STRING_KV("ObDASVecIndexScanCtDef", ObDASVecIndexScanCtDef, K_(filter_pushdown));

  // Whether the complete KNN scalar predicate was compiled into the main-table scan.
  // An indexed filter may still retain a driver bitmap for selectivity estimation and
  // low-selectivity point gets.
  bool filter_pushdown_;
};

// Base class for vector index scan rtdef
struct ObDASVecIndexScanRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexScanRtDef(ObDASOpType op_type = DAS_OP_VEC_SCAN)
    : ObDASAttachRtDef(op_type) {}

  virtual ~ObDASVecIndexScanRtDef() = default;
};

struct ObDASVecIndexBruteScanRtDef : ObDASVecIndexScanRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexBruteScanRtDef()
    : ObDASVecIndexScanRtDef(DAS_OP_VEC_INDEX_BRUTE_SCAN) {}

  virtual ~ObDASVecIndexBruteScanRtDef() = default;

  ObDASScanRtDef *get_main_table_rtdef() const
  {
    ObDASScanRtDef *child_rtdef = nullptr;
    if (children_cnt_ > ObDASVecIndexBruteScanCtDef::MAIN_TABLE_IDX && children_ != nullptr) {
      child_rtdef = static_cast<ObDASScanRtDef *>(children_[ObDASVecIndexBruteScanCtDef::MAIN_TABLE_IDX]);
    }
    return child_rtdef;
  }
};

// IVF specific ctdef, inherits from base class
struct ObDASVecIndexIVFScanCtDef : ObDASVecIndexScanCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexIVFScanCtDef(common::ObIAllocator &alloc)
    : ObDASVecIndexScanCtDef(alloc, DAS_OP_VEC_INDEX_IVF_SCAN) {}

  virtual ~ObDASVecIndexIVFScanCtDef() = default;

  // IVF child table indices (order matches SQL path ob_tsc_cg_service.cpp:2314)
  static const int64_t CENTROID_TABLE_IDX = 0;
  static const int64_t CID_VEC_TABLE_IDX = 1;
  static const int64_t ROWKEY_CID_TABLE_IDX = 2;
  static const int64_t COM_AUX_TABLE_IDX = 3;
  static const int64_t SPECIAL_AUX_TABLE_IDX = 4;  // PQ: pq_id, not present for IVF_FLAT

  const ObDASScanCtDef *get_child_ctdef(int64_t index, ObTSCIRScanType scan_type) const
  {
    const ObDASScanCtDef *child_ctdef = nullptr;
    if (children_cnt_ > index && index >= 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[index]);
      if (child->ir_scan_type_ == scan_type) {
        child_ctdef = child;
      }
    }
    return child_ctdef;
  }

  const ObDASScanCtDef *get_centroid_table_ctdef() const
  {
    return get_child_ctdef(CENTROID_TABLE_IDX, ObTSCIRScanType::OB_VEC_IVF_CENTROID_SCAN);
  }

  const ObDASScanCtDef *get_cid_vec_table_ctdef() const
  {
    return get_child_ctdef(CID_VEC_TABLE_IDX, ObTSCIRScanType::OB_VEC_IVF_CID_VEC_SCAN);
  }

  const ObDASScanCtDef *get_rowkey_cid_table_ctdef() const
  {
    return get_child_ctdef(ROWKEY_CID_TABLE_IDX, ObTSCIRScanType::OB_VEC_IVF_ROWKEY_CID_SCAN);
  }

  const ObDASScanCtDef *get_com_aux_table_ctdef() const
  {
    return get_child_ctdef(COM_AUX_TABLE_IDX, ObTSCIRScanType::OB_VEC_COM_AUX_SCAN);
  }

  const ObDASScanCtDef *get_special_aux_table_ctdef() const
  {
    if (children_cnt_ > SPECIAL_AUX_TABLE_IDX) {
      return get_child_ctdef(SPECIAL_AUX_TABLE_IDX, ObTSCIRScanType::OB_VEC_IVF_SPECIAL_AUX_SCAN);
    }
    return nullptr;
  }
};

// IVF specific rtdef, inherits from base class
struct ObDASVecIndexIVFScanRtDef : ObDASVecIndexScanRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexIVFScanRtDef()
    : ObDASVecIndexScanRtDef(DAS_OP_VEC_INDEX_IVF_SCAN) {}

  virtual ~ObDASVecIndexIVFScanRtDef() = default;

  ObDASScanRtDef *get_child_rtdef(int64_t index) const
  {
    ObDASScanRtDef *child_rtdef = nullptr;
    if (children_cnt_ > index && index >= 0 && children_ != nullptr) {
      child_rtdef = static_cast<ObDASScanRtDef *>(children_[index]);
    }
    return child_rtdef;
  }

  ObDASScanRtDef *get_centroid_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexIVFScanCtDef::CENTROID_TABLE_IDX);
  }

  ObDASScanRtDef *get_cid_vec_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexIVFScanCtDef::CID_VEC_TABLE_IDX);
  }

  ObDASScanRtDef *get_rowkey_cid_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexIVFScanCtDef::ROWKEY_CID_TABLE_IDX);
  }

  ObDASScanRtDef *get_com_aux_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexIVFScanCtDef::COM_AUX_TABLE_IDX);
  }

  ObDASScanRtDef *get_special_aux_table_rtdef() const
  {
    if (ObDASVecIndexIVFScanCtDef::SPECIAL_AUX_TABLE_IDX < children_cnt_) {
      return get_child_rtdef(ObDASVecIndexIVFScanCtDef::SPECIAL_AUX_TABLE_IDX);
    }
    return nullptr;
  }
};

// HNSW specific rtdef, inherits from base class
struct ObDASVecIndexHNSWScanRtDef : ObDASVecIndexScanRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASVecIndexHNSWScanRtDef()
    : ObDASVecIndexScanRtDef(DAS_OP_VEC_INDEX_HNSW_SCAN) {}

  virtual ~ObDASVecIndexHNSWScanRtDef() = default;

  ObDASScanRtDef *get_child_rtdef(int64_t index) const
  {
    ObDASScanRtDef *child_rtdef = nullptr;
    if (children_cnt_ > index && index >= 0 && children_ != nullptr) {
      ObDASScanRtDef *child = static_cast<ObDASScanRtDef *>(children_[index]);
      child_rtdef = child;
    }
    return child_rtdef;
  }

  ObDASScanRtDef *get_delta_buf_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexHNSWScanCtDef::DELTA_BUF_TABLE_IDX);
  }

  ObDASScanRtDef *get_index_id_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexHNSWScanCtDef::INDEX_ID_TABLE_IDX);
  }

  ObDASScanRtDef *get_snapshot_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexHNSWScanCtDef::SNAPSHOT_TABLE_IDX);
  }

  ObDASScanRtDef *get_com_aux_vec_table_rtdef() const
  {
    return get_child_rtdef(ObDASVecIndexHNSWScanCtDef::COM_AUX_VEC_TABLE_IDX);
  }
};

class ObDASVecIndexScanIter : public ObDASIter
{
public:
  ObDASVecIndexScanIter(ObDASIterType type)
    : ObDASIter(type), 
      vec_index_type_(ObVecIndexType::VEC_INDEX_INVALID), 
      vec_idx_try_path_(ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN), 
      go_brute_force_(false), 
      bitmap_(nullptr), 
      limit_(0), 
      search_ctx_(nullptr) {}
  virtual ~ObDASVecIndexScanIter() {}

  void set_bitmap(ObVecIndexBitmap *bitmap) { bitmap_ = bitmap; }

  ObVecIndexType get_index_type() const { return vec_index_type_; }
  bool is_brute_force() const { return go_brute_force_; }
  void set_vec_index_type(ObVecIndexType vec_index_type, ObVecIdxAdaTryPath vec_idx_try_path, bool go_brute_force) {
    vec_index_type_ = vec_index_type;
    vec_idx_try_path_ = vec_idx_try_path;
    go_brute_force_ = go_brute_force;
  }
  void set_go_brute_force(bool go_brute_force) { go_brute_force_ = go_brute_force; }

  void set_limit(uint64_t limit) { limit_ = limit; }
  virtual void set_adaptor(share::ObPluginVectorIndexAdaptor *adaptor)
  {
    // Default no-op for subclasses (e.g. IVF) that do not consume an adaptor.
    // HNSW must override this; if the override is missing the adaptor is
    // silently dropped, so trace it for diagnosability.
    UNUSED(adaptor);
    LOG_DEBUG("base set_adaptor called, ignored by default impl", KP(adaptor));
  }
  virtual share::ObPluginVectorIndexAdaptor *get_adaptor() { return nullptr; }
  virtual void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids) = 0;
  virtual void set_ls_id(const share::ObLSID &ls_id) {}
  virtual share::ObVectorQueryVidIterator* get_adaptor_vid_iter() { return nullptr; }
  virtual int do_table_scan() override { return OB_SUCCESS; }
  virtual int rescan() override { return OB_SUCCESS; }
  virtual void clear_evaluated_flag() override {}

  // ===== Strategy interface for algorithm-agnostic driver =====

  /// Called by DriverIter once to build algorithm-specific query condition.
  virtual int build_query_condition(ObIAllocator &alloc, ObEvalCtx &eval_ctx,
                                    ObExpr *search_vec, bool is_iter_filter, uint64_t limit) = 0;

  /// Called by DriverIter after each iterative filter round. Sets stop_search=true when done.
  virtual int adjust_search_for_next_round(bool first_search,
      share::ObVectorQueryVidIterator *adaptor_vid_iter,
      int64_t iter_unfiltered_vid_cnt, int64_t iter_added_cnt,
      int64_t iter_scan_total_num, bool &stop_search) = 0;

  /// Max single-query result size for init_limit_param validation.
  virtual int64_t get_max_query_result_size() const = 0;

  /// Expected result buffer size (for init_adaptor_vid_iter_if_null). BQ overrides with reorder count.
  virtual uint64_t get_expected_result_buffer_size(uint64_t limit) const { return limit; }

  /// Distance to relevance score conversion. Default uses sort_expr type.
  virtual int distance_to_score(ObExpr *sort_expr, double distance, double &score);

  bool is_async_vector_query() const;
  int get_async_vector_read_snapshot_scn(share::SCN &read_scn) const;
  int get_vector_read_snapshot_scn(
      const transaction::ObTxReadSnapshot *sql_snapshot,
      share::SCN &read_scn) const;
  int assign_vector_read_snapshot(
      const transaction::ObTxReadSnapshot *sql_snapshot,
      const share::SCN &read_scn,
      transaction::ObTxReadSnapshot &target_snapshot) const;
  int apply_async_vector_read_snapshot(
      const transaction::ObTxReadSnapshot *sql_snapshot,
      transaction::ObTxReadSnapshot &target_snapshot) const;

protected:
  virtual int inner_get_next_row() override = 0;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override = 0;

protected:
  ObVecIndexType vec_index_type_;
  ObVecIdxAdaTryPath vec_idx_try_path_;
  bool go_brute_force_;
  ObVecIndexBitmap *bitmap_;
  uint64_t limit_;
  ObDASSearchCtx *search_ctx_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_SCAN_ITER_H_ */
