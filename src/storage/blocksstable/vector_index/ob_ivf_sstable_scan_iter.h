/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_SCAN_ITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_SCAN_ITER_H_

#include "storage/access/ob_table_access_context.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_i_table.h"
#include "storage/tx_storage/ob_access_service.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
namespace storage
{

class ObTableScanParam;

// Partition-scoped owner for IVF SSTable reads. It keeps transaction/tablet/
// table-access state stable while a single searcher is rebound to the visible
// IVF SSTable selected by each version range.
class ObIvfSSTableScanIter final
{
public:
  explicit ObIvfSSTableScanIter(common::ObIAllocator &allocator);
  ~ObIvfSSTableScanIter();

  ObStoreCtxGuard &get_ctx_guard() { return ctx_guard_; }

  int init(
      ObTableScanParam &scan_param,
      const ObTabletHandle &tablet_handle);
  // Set the fixed query for this iterator. The query vector is copied and
  // remains unchanged across all search rounds and SSTable switches.
  // read_stats_ is query-scoped and must remain writable; deletion_bitmap_handle_
  // and pre_filter_ are borrowed and must remain alive and unchanged until reset().
  // scan_budget_ is also borrowed and must remain writable until reset().
  // Searches consume it; the caller may update its batch-stop flag between calls.
  int set_query(const blocksstable::ObIvfKnnSearchParam &query_param);
  // Async-index rows are read at the effective published snapshot M, while
  // the refresh transaction that materialized the SSTable is visible at the
  // SQL statement snapshot Q. Keep Q explicit instead of deriving it from the
  // scan parameter whose snapshot is M.
  int switch_sstable(
      const common::ObNewVersionRange &version_range,
      const share::SCN &sql_read_snapshot);
  // Consume at most next_nprobe new CIDs and return this round's Top-K result.
  int knn_search(
      int64_t next_nprobe,
      blocksstable::ObIvfKnnSearchResult &result,
      bool quantized_only = false);
  int refine_candidates(const blocksstable::ObIvfKnnSearchResult &candidates,
      blocksstable::ObIvfKnnSearchResult &result);
  // Update only the next round's candidate budget; retain the pinned query and CID cursor.
  int set_query_limit(const int64_t k)
  {
    if (!is_query_set_) { return OB_NOT_INIT; }
    if (k <= 0) { return OB_INVALID_ARGUMENT; }
    query_param_.k_ = k;
    return OB_SUCCESS;
  }
  bool has_remaining_centroids() const;
  int next_candidate_batch(int64_t nprobe, float distance_threshold,
      blocksstable::ObIvfKnnSearchResult &result, blocksstable::ObIvfCandidateProgress &progress);
  int brute_force_search(blocksstable::ObIvfKnnSearchResult &result);

  // Bind after switch_sstable(), which calls reuse(). The SQL Profile owns the node.
  void set_profile(common::ObOpProfile<common::ObMetric> *profile, bool post_filter = false)
  {
    profile_ = profile;
    profile_post_filter_ = nullptr != profile && post_filter;
  }
  common::ObOpProfile<common::ObMetric> *get_profile() const { return profile_; }

  bool is_inited() const { return is_inited_; }
  void reuse();
  void reset();
  TO_STRING_KV(KP_(scan_param), KP_(searcher), K_(is_inited),
               K_(is_query_set), K_(is_sstable_switched), KP_(profile),
               K_(profile_post_filter));

private:
  int check_status() const;
  int prepare_table_context();
  int resolve_ivf_sstable(
      const common::ObNewVersionRange &version_range,
      const share::SCN &sql_read_snapshot,
      ObTableHandleV2 &table_handle);
  int ensure_searcher(const ObTableHandleV2 &table_handle);

private:
  common::ObIAllocator &allocator_;
  ObStoreCtxGuard ctx_guard_;
  ObTabletHandle tablet_handle_;
  ObTableScanParam *scan_param_;
  ObTableAccessContext table_access_ctx_;
  blocksstable::ObIvfKnnSearchParam query_param_;
  float *query_buffer_;
  blocksstable::ObIvfSSTableSearcher *searcher_;
  bool is_inited_;
  bool is_query_set_;
  bool is_sstable_switched_;
  common::ObOpProfile<common::ObMetric> *profile_;
  bool profile_post_filter_;

  DISALLOW_COPY_AND_ASSIGN(ObIvfSSTableScanIter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_SCAN_ITER_H_
