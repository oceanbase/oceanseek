/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_SQL_DAS_VEC_INDEX_ASYNC_IVF_SCAN_ITER_H_
#define OCEANBASE_SQL_DAS_VEC_INDEX_ASYNC_IVF_SCAN_ITER_H_
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "lib/rc/context.h"
#include "sql/das/search/ob_das_async_ivf_search_op.h"
#include "sql/das/search/ob_das_async_ivf_group_filter.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
namespace oceanbase
{
namespace sql
{
struct ObDASVecIndexAsyncIVFScanIterParam : ObDASIterParam
{
  ObDASVecIndexAsyncIVFScanIterParam()
    : ObDASIterParam(DAS_ITER_VEC_INDEX_ASYNC_IVF_SCAN), ctdef_(nullptr), rtdef_(nullptr),
      search_ctx_(nullptr), tx_desc_(nullptr), snapshot_(nullptr), filter_rtdef_(nullptr) {}
  bool is_valid() const override
  { return nullptr != ctdef_ && nullptr != rtdef_ && nullptr != search_ctx_ && nullptr != snapshot_; }
  const ObDASVecIndexAsyncIVFScanCtDef *ctdef_;
  ObDASVecIndexAsyncIVFScanRtDef *rtdef_;
  ObDASSearchCtx *search_ctx_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObIDASSearchRtDef *filter_rtdef_;
};

class ObDASVecIndexAsyncIVFScanIter final : public ObDASVecIndexScanIter
{
public:
  ObDASVecIndexAsyncIVFScanIter()
    : ObDASVecIndexScanIter(DAS_ITER_VEC_INDEX_ASYNC_IVF_SCAN),
      alloc_(CURRENT_CONTEXT->get_allocator()), root_(alloc_), group_filter_(alloc_),
      post_batch_alloc_(CURRENT_CONTEXT->get_allocator()),
      ctdef_(nullptr), rtdef_(nullptr), tx_desc_(nullptr), snapshot_(nullptr), filter_rtdef_(nullptr),
      pre_filter_bitmap_(alloc_), scan_budget_(0),
      vid_iter_(nullptr), nprobe_(8), next_nprobe_(8), selectivity_(0), need_post_filter_(false), opened_(false),
      round_ready_(false), group_ready_(false),
      post_round_end_(false), strategy_(share::ObVecIdxQueryStrategy::RECALL_FIRST),
      profile_(nullptr) {}
  ~ObDASVecIndexAsyncIVFScanIter() { reset_epoch(); }
  void set_related_tablet_ids(const ObDASRelatedTabletID &) override {}
  void set_ls_id(const share::ObLSID &) override {}
  share::ObVectorQueryVidIterator *get_adaptor_vid_iter() override { return vid_iter_; }
  int64_t get_max_query_result_size() const override { return INT64_MAX; }
  int build_query_condition(ObIAllocator &alloc, ObEvalCtx &ctx, ObExpr *search_vec,
                            bool need_post_filter, uint64_t limit) override;
  int adjust_search_for_next_round(bool first_search, share::ObVectorQueryVidIterator *iter,
      int64_t unfiltered, int64_t added, int64_t scanned, bool &stop) override;
  int refine_post_filter_candidates(const blocksstable::ObIvfKnnSearchResult &selected);
  void set_selectivity(double value) { selectivity_ = value; }
  static int estimate_partition_filter(ObDASSearchCtx &ctx, ObIDASSearchRtDef &filter,
      ObDASSearchCost &cost, double &selectivity);
  bool can_group_pre_filter() const;
  int prepare_group_pre_filter(int64_t &count);
  int prepare_partition_pre_filter(blocksstable::ObIvfPreFilterBitmap *&bitmap);
  // Only during partition bitmap construction, before any scanner borrows it.
  int abort_partition_pre_filter_build();
  // The hybrid Vector Iter node this scan hangs its children under; nullptr when
  // the ambient profile belongs to another family (e.g. the plain SQL vector path).
  static common::ObOpProfile<common::ObMetric> *get_current_hybrid_vector_iter_profile();
  int do_table_scan() override;
  int rescan() override;
protected:
  int inner_init(ObDASIterParam &param) override;
  int inner_reuse() override { reset_epoch(); return OB_SUCCESS; }
  int inner_release() override
  {
    deletion_.trace_cache_handle("async_ivf_release_before");
    reset_epoch();
    deletion_.trace_cache_handle("async_ivf_release_after");
    clear_partition_profile();
    return OB_SUCCESS;
  }
  int inner_get_next_row() override { int64_t count = 0; return inner_get_next_rows(count, 1); }
  int inner_get_next_rows(int64_t &count, int64_t capacity) override;
private:
  // Semantics match ObDASIVFAsyncIter::ProfileFinishTag.
  enum ProfileFinishTag { PROFILE_RETURN_RESULT = 0, PROFILE_VECTOR_RETRY = 1, PROFILE_QUERY_ERROR = 2 };
  void bind_partition_profile();
  void record_profile_finish(int scan_ret);
  void clear_partition_profile();
  int open_epoch();
  int search_post_without_filter(common::ObIArray<ObDASVectorCandidate> &candidates);
  int64_t calc_next_nprobe(int64_t matched_count) const;
  int publish(const common::ObIArray<ObDASVectorCandidate> &candidates, bool quantized_only);
  void reset_epoch();
  void reset_result();
  common::ObArenaAllocator alloc_;
  ObDASAsyncIVFTopKMergeOp root_;
  ObDASAsyncIVFGroupFilter group_filter_;
  common::ObArenaAllocator post_batch_alloc_;
  const ObDASVecIndexAsyncIVFScanCtDef *ctdef_;
  ObDASVecIndexAsyncIVFScanRtDef *rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObIDASSearchRtDef *filter_rtdef_;
  storage::ObTableScanParam scan_param_;
  storage::ObTableScanParam delete_param_;
  transaction::ObTxReadSnapshot effective_snapshot_;
  storage::ObDeletionBitmapHandle deletion_;
  blocksstable::ObIvfPreFilterBitmap pre_filter_bitmap_;
  blocksstable::ObIvfPreFilter pre_filter_;
  blocksstable::ObIvfScanBudget scan_budget_;
  blocksstable::ObIvfKnnSearchParam query_;
  blocksstable::ObIvfSSTableReadStats stats_;
  common::ObString query_vector_;
  share::ObVectorQueryVidIterator *vid_iter_;
  int64_t nprobe_;
  int64_t next_nprobe_;
  double selectivity_;
  bool need_post_filter_;
  bool opened_;
  bool round_ready_;
  bool group_ready_;
  bool post_round_end_;
  share::ObVecIdxQueryStrategy strategy_;
  common::ObOpProfile<common::ObMetric> *profile_;
};
} // namespace sql
} // namespace oceanbase
#endif
