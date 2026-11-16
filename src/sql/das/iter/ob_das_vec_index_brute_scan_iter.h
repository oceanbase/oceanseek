/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_BRUTE_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_BRUTE_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/iter/ob_das_vec_scan_utils.h"
#include "sql/das/iter/ob_das_hnsw_scan_iter.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"

namespace oceanbase
{
namespace sql
{

struct ObDASVecIndexBruteScanIterParam : public ObDASIterParam
{
public:
  ObDASVecIndexBruteScanIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_VEC_INDEX_BRUTE_SCAN),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      main_table_iter_(nullptr),
      vec_index_scan_ctdef_(nullptr),
      vec_index_scan_rtdef_(nullptr),
      sort_expr_(nullptr),
      search_ctx_(nullptr) {}

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid() &&
           ls_id_.is_valid() &&
           nullptr != tx_desc_ &&
           nullptr != snapshot_ &&
           nullptr != main_table_iter_ &&
           nullptr != vec_index_scan_ctdef_ &&
           nullptr != vec_index_scan_rtdef_ &&
           nullptr != sort_expr_;
  }

  INHERIT_TO_STRING_KV("ObDASIterParam", ObDASIterParam,
                       K_(ls_id), K_(vec_index_scan_ctdef), K_(vec_index_scan_rtdef));

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *main_table_iter_;

  const ObDASVecIndexBruteScanCtDef *vec_index_scan_ctdef_;
  ObDASVecIndexBruteScanRtDef *vec_index_scan_rtdef_;

  ObExpr *sort_expr_;
  ObDASSearchCtx *search_ctx_;
};

// Exact KNN over a table without any vector index: sequentially scan the main table,
// score every row with the CG-generated distance expression and keep the best K in a
// bounded heap. The scoring kernel makes no assumption about the expression semantics,
// so the only vector-specific behaviour is skipping NULL vectors and the
// distance-to-score conversion adapted to the indexed-search distance representation.
class ObDASVecIndexBruteScanIter : public ObDASVecIndexScanIter
{
public:
  ObDASVecIndexBruteScanIter()
    : ObDASVecIndexScanIter(ObDASIterType::DAS_ITER_VEC_INDEX_BRUTE_SCAN),
      mem_context_(nullptr),
      vec_op_alloc_("BruteVecScanOp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      main_table_iter_(nullptr),
      main_table_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      main_table_scan_param_(),
      main_table_iter_first_scan_(true),
      vec_index_scan_ctdef_(nullptr),
      vec_index_scan_rtdef_(nullptr),
      sort_expr_(nullptr),
      vec_column_expr_(nullptr),
      vid_expr_(nullptr),
      use_filtered_get_(false),
      limit_k_(0),
      scan_done_(false),
      adaptor_vid_iter_(nullptr) {}

  virtual ~ObDASVecIndexBruteScanIter() {}

  virtual void set_related_tablet_ids(const ObDASRelatedTabletID &related_tablet_ids) override
  {
    main_table_tablet_id_ = related_tablet_ids.lookup_tablet_id_;
  }
  virtual void set_ls_id(const share::ObLSID &ls_id) override { ls_id_ = ls_id; }
  void set_use_filtered_get(bool use_filtered_get) { use_filtered_get_ = use_filtered_get; }
  virtual share::ObVectorQueryVidIterator *get_adaptor_vid_iter() override { return adaptor_vid_iter_; }
  virtual void clear_evaluated_flag() override;

  virtual int build_query_condition(ObIAllocator &alloc, ObEvalCtx &eval_ctx,
                                    ObExpr *search_vec, bool is_iter_filter, uint64_t limit) override;
  // Brute force is exact by construction: one pass over the table is always enough.
  virtual int adjust_search_for_next_round(bool first_search,
      share::ObVectorQueryVidIterator *adaptor_vid_iter,
      int64_t iter_unfiltered_vid_cnt, int64_t iter_added_cnt,
      int64_t iter_scan_total_num, bool &stop_search) override;
  virtual int64_t get_max_query_result_size() const override { return INT64_MAX; }
  virtual int distance_to_score(ObExpr *sort_expr, double distance, double &score) override;

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int init_memory_context();
  int resolve_scan_output_exprs();
  int do_brute_force_scan();
  int prepare_main_table_ranges(const ObDASScanCtDef &main_table_ctdef);
  int score_scanned_batch(int64_t scan_row_cnt,
                          ObSimpleMaxHeap &heap,
                          int64_t &distance_row_cnt);
  int build_vid_iter_from_heap(ObSimpleMaxHeap &heap);
  void reset_adaptor_vid_iter();

  static const int64_t VEC_COLUMN_IDX = 0;
  static const int64_t VID_COLUMN_IDX = 1;

  lib::MemoryContext mem_context_;
  ObArenaAllocator vec_op_alloc_;

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *main_table_iter_;
  ObTabletID main_table_tablet_id_;
  ObTableScanParam main_table_scan_param_;
  bool main_table_iter_first_scan_;

  const ObDASVecIndexBruteScanCtDef *vec_index_scan_ctdef_;
  ObDASVecIndexBruteScanRtDef *vec_index_scan_rtdef_;

  ObExpr *sort_expr_;
  // main table scan output: [0] = vector column, [1] = hidden pk (vid)
  ObExpr *vec_column_expr_;
  ObExpr *vid_expr_;

  // For a selective indexed filter, turn the matching heap-table hidden PKs into
  // point-get ranges.  Otherwise keep the sequential whole-table scan path.
  bool use_filtered_get_;

  uint64_t limit_k_;
  bool scan_done_;
  share::ObVectorQueryVidIterator *adaptor_vid_iter_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_VEC_INDEX_BRUTE_SCAN_ITER_H_ */
