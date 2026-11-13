/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SQL_OB_DAS_ASYNC_TOKEN_OP_H_
#define OCEANBASE_SQL_OB_DAS_ASYNC_TOKEN_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_bm25_batch_kernel.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/engine/expr/ob_expr_bm25.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/access/ob_async_inv_iterator_pool.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "storage/retrieval/ob_inv_idx_param_estimator.h"

namespace oceanbase
{
namespace storage
{
class ObAccessService;
}
namespace sql
{
class SparseCandidateBuffer;

// Operator and parameters for single token retrieval on one async inverted index segment.
struct ObDASAsyncTokenOpParam : public ObIDASSearchOpParam
{
public:
  ObDASAsyncTokenOpParam();
  virtual ~ObDASAsyncTokenOpParam() {}

  bool is_valid() const;
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    // leaf node, no children
    return OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("ObDASAsyncTokenOpParam", ObIDASSearchOpParam,
      KPC_(ir_ctdef),
      KPC_(ir_rtdef),
      K_(version_range),
      K_(index_table_id),
      K_(group_slot),
      K_(direct_bind),
      K_(token_boost),
      K_(query_token));

  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  common::ObNewVersionRange version_range_;
  uint64_t index_table_id_;
  int64_t group_slot_;
  bool direct_bind_;
  double token_boost_;
  common::ObString query_token_;
};

class ObDASAsyncTokenOp : public ObIDASSearchOp
{
public:

  explicit ObDASAsyncTokenOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASAsyncTokenOp() {}

  void close_posting_iter();
  void release_scan_resources() override { close_posting_iter(); }

  bool can_probe_sorted_ids() const
  {
    return is_scoring_ && DAS_ROWID_TYPE_UINT64 == get_rowid_type()
        && nullptr != posting_iter_ && posting_iter_->can_probe_sorted_ids();
  }
  // Add one dimension to existing candidate partial sums in their original order.
  // The caller handles its cached head before passing strictly greater IDs here.
  int probe_sorted_ids(const uint64_t *ids, const int64_t count,
      double *scores, bool *matched, ObDASRowID &last_id,
      double &last_score, bool &score_materialized, bool &iter_end);

  // Consume next rows through the inclusive boundary into an unmasked accumulator.
  // The caller owns the current row. On success, next_id_after is the first row
  // beyond the boundary, or max at EOF.
  int drain_until_accumulated(
      const ObDASRowID &boundary,
      DocScoreAccumulator &accumulator,
      ObDASRowID &next_id_after,
      double &next_score_after);
  int advance_to_id_uint64(const uint64_t target, uint64_t &curr_id);
  // Intersect posting runs with compact candidates before computing their scores.
  int score_candidates(SparseCandidateBuffer &candidates,
      ObDASRowID &next_id_after, double &next_score_after);

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_rescan_segment(const common::ObNewVersionRange &range) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_advance_to_id(const ObDASRowID &target, ObDASRowID &curr_id) override;
  int do_advance_to_id_uint64(const uint64_t target, uint64_t &curr_id);
  int do_probe_sorted_ids(const uint64_t *ids, const int64_t count,
      double *scores, bool *matched, ObDASRowID &last_id,
      double &last_score, bool &score_materialized, bool &iter_end, int64_t &matched_count);
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_load_curr_score(double &score) override;
  int do_advance_shallow(const ObDASRowID &target,
                         const bool inclusive,
                         const MaxScoreTuple *&max_score_tuple) override;
  bool can_advance_to_id() const override
  { return true; }
  int do_calc_max_score(double &threshold) override;
  int do_calc_range_max_score(
      const ObDASRowID &start_id,
      const bool start_inclusive,
      const ObDASRowID &end_id,
      double &max_score,
      bool &can_calc) override;
  int do_prepare_parallel_work_estimate() override;
  int64_t get_self_estimated_work_row_count() const override
  { return estimated_work_row_count_; }
  int do_drain_until(const ObDASRowID &boundary,
                     int64_t capacity,
                     uint64_t *out_ids,
                     double *out_scores,
                     int64_t &count,
                     ObDASRowID &next_id_after,
                     double &next_score_after) override;
  int do_drain_until_accumulated(const ObDASRowID &boundary,
                               DocScoreAccumulator &accumulator,
                               int64_t &count,
                               ObDASRowID &next_id_after,
                               double &next_score_after);
  int do_drain_until_active(const uint64_t range_start,
                            const uint64_t range_end,
                            DocScoreAccumulator &accumulator,
                            int64_t &count) override;
  int do_drain_into_candidates(SparseCandidateBuffer &candidates, int64_t &count,
      ObDASRowID &next_id_after, double &next_score_after);

private:
  // Trim only the scoring prefix; the accumulator, drain cursor and finished state are unchanged.
  int get_active_posting_prefix_count(const blocksstable::ObPostingDocIdView &doc_ids,
                                     const int64_t posting_count,
                                     const DocScoreAccumulator &accumulator, int64_t &scoring_count) const;
  int init_term_key(const common::ObString &query_token);
  bool is_search_text() const
  { return ir_ctdef_->search_index_fulltext_info_.has_search_index_fulltext(); }
  int init_posting_scan_param();
  int init_inv_idx_scan_param();
  // Init the lazy BM25 param estimator.
  int init_bm25_param_estimator();
  int fill_async_bm25_param(blocksstable::ObInvPostingScanParam &param);
  // Conservative term-dimension max score (see ObExprBM25::max_term_bm25_score).
  int init_dim_max_score(double &max_score);
  // Open the single-segment posting iter via the access service's async_index_posting_scan.
  // Tests pre-inject tsc_service_ with a mock to avoid a real tablet.
  int open_posting_iter_from_tablet();
  int fetch_curr_id(ObDASRowID &curr_id);
  int fetch_curr(ObDASRowID &curr_id, double &score);
  int fetch_curr_on_demand(ObDASRowID &curr_id, double &score);
  int estimate_work_row_count();

private:
  ObArenaAllocator arena_allocator_;
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
  double query_token_weight_;
  double dim_max_score_;
  double token_boost_;
  ObBM25PrecomputedParams bm25_calc_params_;
  BM25ScoreKernels bm25_score_kernels_;
  BM25AccumulationKernels<false> bm25_expansion_kernels_;
  BM25AccumulationKernels<true> bm25_existing_kernels_;
  int64_t estimated_work_row_count_;
  bool inv_idx_scan_param_inited_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_ASYNC_TOKEN_OP_H_
