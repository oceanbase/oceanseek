/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DAS_ASYNC_TOKEN_HELPER_H_
#define OCEANBASE_SQL_OB_DAS_ASYNC_TOKEN_HELPER_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/engine/expr/ob_expr_bm25.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/access/ob_async_inv_iterator_pool.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "storage/retrieval/ob_block_max_iter.h"
#include "storage/retrieval/ob_inv_idx_param_estimator.h"

namespace oceanbase
{
namespace storage
{
class ObAccessService;
}
namespace sql
{

struct ObDASAsyncTokenHelperParam
{
  ObDASAsyncTokenHelperParam()
    : ir_ctdef_(nullptr),
      ir_rtdef_(nullptr),
      query_token_(),
      index_table_id_(common::OB_INVALID_ID),
      group_slot_(-1),
      direct_bind_(false),
      token_boost_(0.0)
  {}
  bool is_valid() const
  {
    return nullptr != ir_ctdef_
        && nullptr != ir_rtdef_
        && !query_token_.empty()
        && common::OB_INVALID_ID != index_table_id_
        && (!direct_bind_ || group_slot_ >= 0)
        && token_boost_ > 0.0;
  }

  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  common::ObString query_token_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  double token_boost_;
};

// Per-token async inverted index helper for multi-token phrase matching.
// Exposes posting cursor + phrase block-max surface for find_intersection pruning.
class ObDASAsyncTokenHelper final
{
public:

  explicit ObDASAsyncTokenHelper(ObDASSearchCtx &search_ctx);
  ~ObDASAsyncTokenHelper();
  TO_STRING_EMPTY();

  int init(const ObDASAsyncTokenHelperParam &param);
  int init_bm25_param_estimator(storage::ObAsyncBM25ParamEstimator &estimator);
  int open();
  int rescan();
  int rescan_segment(const common::ObNewVersionRange &range);
  int close();
  void reuse();

  int get_next_row();
  // Materialize cursor at target doc id (loads posting row).
  int advance_to(const common::ObDatum &id_datum);
  // Position at posting block covering target doc (no row load); find_intersection pruning.
  int advance_shallow(const common::ObDatum &id_datum, const bool inclusive);
  int get_curr_phrase_block_max_info(const storage::ObMaxScoreTuple *&tuple) const;
  // Id-range only; used by the match-phrase shallow intersection exposed to parent operators.
  int get_block_id_range(uint64_t &min_id, uint64_t &max_id) const;
  int get_curr_bm25_param(int64_t &token_freq, int64_t &doc_length) const;
  int get_curr_id(const common::ObDatum *&id_datum) const;
  // Returned string aliases posting-iter buffer; invalidated on next()/advance_to(), same as get_curr_id().
  int get_curr_pos_list(common::ObString &pos_list) const;
  int get_max_token_relevance(double &max_token_relevance) const;
  int get_bm25_param(uint64_t &total_doc_cnt, double &avg_doc_length) const;

  const common::ObTabletID &get_inv_idx_tablet_id() const { return inv_idx_tablet_id_; }
  bool is_empty_segment() const { return is_empty_segment_; }
  storage::ObAsyncBM25ParamEstimator &get_param_estimator() { return param_estimator_; }

  // For tests: inject access service / tablet id without full DAS scalar scan defs.
  void set_tsc_service(storage::ObAccessService *tsc_service) { tsc_service_ = tsc_service; }
  void set_tablet_id(const common::ObTabletID &tablet_id) { inv_idx_tablet_id_ = tablet_id; }

private:
  int init_term_key(const common::ObString &query_token);
  bool is_search_text() const
  { return ir_ctdef_->search_index_fulltext_info_.has_search_index_fulltext(); }
  int init_posting_scan_param();
  int init_inv_idx_scan_param();
  int init_bm25_param_estimator();
  int open_posting_iter_from_tablet();
  void close_posting_iter();
  void reuse_posting_iter();

  ObDASSearchCtx &search_ctx_;
  // Term key / query token: lives for the whole helper lifetime.
  ObArenaAllocator arena_allocator_;
  // Posting iter pages: reset on every close/rescan (SQL mem-leak check).
  ObArenaAllocator posting_iter_allocator_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  ObTabletID inv_idx_tablet_id_;
  ObTableScanParam inv_idx_scan_param_;
  common::ObNewVersionRange version_range_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  bool is_empty_segment_;
  storage::ObAccessService *tsc_service_;
  common::ObQueryFlag query_flag_;
  blocksstable::ObInvSSTablePostingIter *posting_iter_;
  blocksstable::ObInvPostingIterEntry *posting_entry_;
  blocksstable::ObInvPostingScanParam posting_scan_param_;
  blocksstable::ObStorageDatum *term_datums_;
  blocksstable::ObDatumRowkey term_key_;
  common::ObString query_token_;
  storage::ObAsyncBM25ParamEstimator param_estimator_;
  double dim_max_score_;
  double token_boost_;
  mutable storage::ObMaxScoreTuple phrase_block_max_tuple_;
  bool inv_idx_scan_param_inited_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_ASYNC_TOKEN_HELPER_H_
