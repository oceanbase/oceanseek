/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_SEARCHER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_SEARCHER_H_

#include "roaring/roaring64.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "storage/blocksstable/vector_index/ob_disk_bbq_fast_scanner.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"

namespace oceanbase
{
namespace storage
{
class ObStoreRowIterator;
}
namespace blocksstable
{

class ObDiskBBQSearcher final : public ObIvfSSTableSearcher
{
public:
  explicit ObDiskBBQSearcher(common::ObIAllocator &allocator);
  ~ObDiskBBQSearcher() override;

  int open(
      const storage::ObTableHandleV2 &table_handle,
      const storage::ObTableScanParam &scan_param,
      storage::ObTableAccessContext &access_ctx);

  // per-segment posting budget. Inputs use original vectors, not physical SSTable rows (which also include centroids and SOAR postings).
  static int64_t compute_auto_scan_budget(int64_t k, int64_t num_candidates, int64_t vector_count);
  static int64_t compute_auto_num_candidates(int64_t k, int64_t vector_count);
  static int64_t adjust_auto_scan_budget(int64_t budget, int64_t vector_count,
      double selectivity, bool is_pre_filter);
  static int64_t adjust_pre_filter_scan_budget(int64_t budget, int64_t vector_count,
      double selectivity);

private:
  // Bound the posting candidates retained before each round of RAW refinement.
  static constexpr int64_t MAX_PENDING_CANDIDATES = 64 * 1024;
  virtual int do_open_sstable() override;
  virtual void do_close_sstable() override;
  void do_reuse() override;
  int do_next_candidate_batch(const ObIvfKnnSearchParam &query_param,
      float distance_threshold, int64_t batch_size, ObIvfTopKCollector &collector,
      ObIvfCandidateProgress &progress) override;
  virtual int do_knn_search(
      const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &collector) override;
  int do_quantized_search(const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &collector) override;
  int do_refine_candidates(const ObIvfKnnSearchParam &query_param,
      const ObIvfKnnSearchResult &candidates,
      ObIvfTopKCollector &collector) override;
  virtual int do_brute_force_search(
      const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &collector) override;
  int prepare_query(const ObIvfKnnSearchParam &query_param);
  int estimate_legacy_vector_count(int64_t &vector_count);
  int prepare_centroid_batch(const ObIvfKnnSearchParam &query_param);
  int64_t get_effective_nprobe(const ObIvfKnnSearchParam &query_param) const;
  bool is_pre_filter_budget_scan() const { return remaining_pre_filter_budget_ >= 0; }
  void consume_pre_filter_budget(int64_t count);
  void finish_pre_filter_budget_scan();
  int collect_brute_force_candidates(
      const ObIvfKnnSearchParam &query_param,
      ObIvfPreFilterIter &allowed_iter,
      const quantization::ObQuantizationComputer &computer,
      ObIvfTopKCollector &final_collector);
  int read_raw_vector_batch(
      const ObIvfKnnSearchParam &query_param,
      const int64_t *vids,
      int64_t vid_count,
      const quantization::ObQuantizationComputer &computer,
      ObIvfTopKCollector &final_collector);
  int collect_approximate_candidates(
      const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &final_collector,
      bool quantized_only = false);
  int refine_pending_candidates(
      const ObIvfKnnSearchParam &query_param,
      common::ObSEArray<ObIvfKnnEntry, 256> &pending,
      const quantization::ObQuantizationComputer &computer,
      ObIvfTopKCollector &final_collector,
      float &distance_threshold);
  int refine_vector(
      const ObIvfKnnSearchParam &query_param,
      const ObIvfKnnEntry *candidates,
      int64_t candidate_count,
      const quantization::ObQuantizationComputer &computer,
      ObIvfTopKCollector &final_collector);
  int refine_raw_vector(
      const ObIvfKnnSearchParam &query_param,
      const ObIvfKnnEntry *candidates,
      int64_t candidate_count,
      const quantization::ObQuantizationComputer &computer,
      ObIvfTopKCollector &final_collector);
  int collect_raw_vector_row(
      const ObDatumRow *row,
      const ObIvfKnnEntry &candidate,
      const quantization::ObQuantizationComputer &computer,
      ObIvfTopKCollector &final_collector);
  void release_raw_vector_multi_getter();
  bool was_raw_vector_refined(const int64_t vid) const
  {
    return nullptr != refined_vids_
        && roaring::api::roaring64_bitmap_contains(refined_vids_, static_cast<uint64_t>(vid));
  }
  int mark_raw_vector_refined(const int64_t vid);
  void reset_refined_vids();

  ObDiskBBQFastScanner fast_scanner_;
  storage::ObStoreRowIterator *raw_vector_multi_getter_;
  int64_t post_centroids_remaining_;
  bool post_scan_open_;
  bool post_started_;
  // Child-count budget for initial centroid selection, before reading postings.
  // -1: not initialized/unused; positive: pending selection; 0: selection complete.
  int64_t initial_child_budget_;
  // Automatic PRE scans charge physical rows in nonempty posting batches.
  // Child counts estimate the first batch; this budget determines the full window.
  // -1: inactive; >= 0: finish the current CID before closing the window.
  int64_t remaining_pre_filter_budget_;
  // First-window centroid count, reused by later windows. PRE accumulates
  // the actual count while scanning; other paths resolve it from metadata.
  int64_t auto_nprobe_;
  // True until the metadata-based first centroid batch is prepared.
  bool need_init_auto_nprobe_;
  // Exact distances are CID-independent within one SSTable. Each KNN round
  // has a new Top-K collector, so its completed-refinement history starts empty.
  // POST batches do not use or populate this history.
  roaring::api::roaring64_bitmap_t *refined_vids_;
  DISALLOW_COPY_AND_ASSIGN(ObDiskBBQSearcher);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_DISK_BBQ_SEARCHER_H_
