/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_SEARCHER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_SEARCHER_H_

#include <climits>
#include <cmath>
#include "lib/ob_errno.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "storage/blocksstable/vector_index/ob_ivf_query_vector_context.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace storage
{
class ObDeletionBitmapHandle;
class ObTableAccessContext;
class ObTableScanParam;
}
namespace blocksstable
{

class ObIvfSSTable;
class ObIvfCentroidIterator;

// Query-owned VID set. Small sets retain Roaring's inline/set representation;
// bounded dense ranges use one bit per VID. Estimates guide construction;
// finish() selects the representation using the complete set's density.
class ObIvfPreFilterBitmap final
{
public:
  static constexpr uint64_t MAX_COMPACT_BITS = 20000000;
  explicit ObIvfPreFilterBitmap(common::ObIAllocator &allocator, int64_t estimated_cardinality = -1)
      : allocator_(allocator), compact_allocator_(lib::ObMemAttr(MTL_ID(), "VIBitmapADPV")),
        compact_alloc_(&compact_allocator_), roaring_(&allocator), fallback_(nullptr), words_(nullptr), base_vid_(0),
        bit_count_(0), cardinality_(0), min_vid_(INT64_MAX), max_vid_(0),
        estimated_cardinality_(estimated_cardinality), next_dense_check_(64),
        compact_attempted_(false), failed_(false) {}
  ~ObIvfPreFilterBitmap() { reset(); }
  int value_add(uint64_t vid) { return value_add_many(&vid, 1); }
  int value_add_many(const uint64_t *vids, int64_t count);
  // Call before publishing the immutable filter to Segment readers.
  int finish();
  void reset();
  bool is_compact() const { return nullptr != words_; }
  bool is_roaring() const { return nullptr != fallback_; }
  bool is_valid() const { return !failed_; }
  uint64_t get_cardinality() const
  { return is_compact() ? cardinality_ : get_roaring()->get_cardinality(); }
  bool is_contains(uint64_t vid) const
  {
    return !failed_ && vid <= INT64_MAX && (is_compact()
        ? (vid >= base_vid_ && vid - base_vid_ < bit_count_
           && 0 != (words_[(vid - base_vid_) / 64] & (uint64_t(1) << ((vid - base_vid_) % 64))))
        : get_roaring()->is_contains(vid));
  }
  common::ObRoaringBitmap *get_roaring() const
  { return is_compact() ? nullptr : nullptr != fallback_ ? fallback_ : &roaring_; }
  const uint64_t *get_words() const { return words_; }
  uint64_t get_base_vid() const { return base_vid_; }
  uint64_t get_bit_count() const { return bit_count_; }
  uint64_t get_min_vid() const { return min_vid_; }
  uint64_t get_max_vid() const { return max_vid_; }
  TO_STRING_KV(K_(base_vid), K_(bit_count), K_(cardinality),
      K_(estimated_cardinality), KP_(fallback), K_(failed));
private:
  uint64_t *alloc_words(uint64_t bit_count);
  void free_words(uint64_t *words, uint64_t bit_count);
  int reserve(uint64_t min_vid, uint64_t max_vid, bool force_compact = false);
  int convert_to_roaring();
  common::ObIAllocator &allocator_;
  // Query arenas ignore per-allocation labels and cannot release old buffers
  // during representation changes. Keep compact storage independently owned.
  common::ObMalloc compact_allocator_;
  common::ObIAllocator *compact_alloc_;
  mutable common::ObRoaringBitmap roaring_;
  common::ObRoaringBitmap *fallback_;
  uint64_t *words_;
  uint64_t base_vid_;
  uint64_t bit_count_;
  uint64_t cardinality_;
  uint64_t min_vid_;
  uint64_t max_vid_;
  int64_t estimated_cardinality_;
  uint64_t next_dense_check_;
  // At most one promotion during construction, followed by final correction.
  bool compact_attempted_;
  bool failed_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfPreFilterBitmap);
};

// Non-owning scalar pre-filter view. The caller-owned bitmap must outlive this
// filter and remain unchanged while a search uses the filter.
class ObIvfPreFilter final
{
public:
  ObIvfPreFilter() : allowed_vids_(nullptr), adaptive_vids_(nullptr) {}
  TO_STRING_KV(KP_(allowed_vids), KP_(adaptive_vids));

  int init(common::ObRoaringBitmap &allowed_vids)
  {
    int ret = OB_SUCCESS;
    if (is_valid()) {
      ret = OB_INIT_TWICE;
    } else {
      allowed_vids_ = &allowed_vids;
    }
    return ret;
  }
  int init(const ObIvfPreFilterBitmap &allowed_vids)
  {
    int ret = OB_SUCCESS;
    if (is_valid()) {
      ret = OB_INIT_TWICE;
    } else if (!allowed_vids.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      adaptive_vids_ = &allowed_vids;
    }
    return ret;
  }
  void reset() { allowed_vids_ = nullptr; adaptive_vids_ = nullptr; }
  bool is_valid() const { return nullptr != allowed_vids_ || nullptr != adaptive_vids_; }
  bool is_empty() const
  {
    return nullptr != adaptive_vids_ ? 0 == adaptive_vids_->get_cardinality()
        : nullptr != allowed_vids_ && 0 == allowed_vids_->get_cardinality();
  }
  bool contains(const int64_t vid) const
  {
    return vid >= 0 && (nullptr != adaptive_vids_ ? adaptive_vids_->is_contains(vid)
        : nullptr != allowed_vids_ && allowed_vids_->is_contains(static_cast<uint64_t>(vid)));
  }
  int get_vid_range(int64_t &min_vid, int64_t &max_vid) const;
  // Legacy borrowed Roaring view; compact storage returns nullptr. Use
  // ObIvfPreFilterIter for traversal independent of the representation.
  common::ObRoaringBitmap *get_allowed_vids() const
  { return nullptr != adaptive_vids_ ? adaptive_vids_->get_roaring() : allowed_vids_; }
  const ObIvfPreFilterBitmap *get_adaptive_vids() const { return adaptive_vids_; }

private:
  // ObRoaringBitmap::is_contains() is not const-qualified, so retain a
  // non-const pointer while only invoking its query API.
  common::ObRoaringBitmap *allowed_vids_;
  const ObIvfPreFilterBitmap *adaptive_vids_;
};

// Each Segment owns its cursor; the shared filter is immutable during traversal.
class ObIvfPreFilterIter final
{
public:
  explicit ObIvfPreFilterIter(const ObIvfPreFilter &filter)
      : filter_(filter), roaring_iter_(filter.get_allowed_vids()), word_pos_(0),
        pending_bits_(0), current_(0), initialized_(false), small_mode_(false),
        small_values_(), small_count_(0), small_pos_(0) {}
  ~ObIvfPreFilterIter() { deinit(); }
  int init();
  int get_next();
  uint64_t get_curr_value()
  { return small_mode_ ? small_values_[small_pos_]
      : is_compact() ? current_ : roaring_iter_.get_curr_value(); }
  void deinit();
private:
  bool is_compact() const
  { return nullptr != filter_.get_adaptive_vids() && filter_.get_adaptive_vids()->is_compact(); }
  const ObIvfPreFilter &filter_;
  common::ObRoaringBitmapIter roaring_iter_;
  uint64_t word_pos_;
  uint64_t pending_bits_;
  uint64_t current_;
  bool initialized_;
  bool small_mode_;
  uint64_t small_values_[MAX_BITMAP_SET_VALUES];
  int64_t small_count_;
  int64_t small_pos_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfPreFilterIter);
};

// Optional diagnostics for one search. The caller owns the instance and must
// keep it alive until that search completes. Each instance is exclusive to one
// search and is never serialized into SSTable metadata.
struct ObIvfSSTableReadStats final
{
  ObIvfSSTableReadStats() { reset(); }
  void reset()
  {
    low_precision_vector_scan_count_ = 0;
    low_precision_vector_count_ = 0;
    raw_vector_read_count_ = 0;
    raw_vector_key_count_ = 0;
    raw_vector_refine_count_ = 0;
    peak_pending_candidate_capacity_ = 0;
    first_low_precision_vector_scan_cid_ = -1;
  }
  TO_STRING_KV(K_(low_precision_vector_scan_count), K_(low_precision_vector_count),
      K_(raw_vector_read_count),
      K_(raw_vector_key_count), K_(raw_vector_refine_count),
      K_(peak_pending_candidate_capacity),
      K_(first_low_precision_vector_scan_cid));

  int64_t low_precision_vector_scan_count_;
  // Number of low-precision vector rows decoded and evaluated while scanning
  // the selected posting lists.
  int64_t low_precision_vector_count_;
  // RAW_VECTOR I/O is issued in batches: read_count_ counts read operations,
  // key_count_ counts all rowkeys carried by those operations, and
  // refine_count_ counts rows that still pass the latest exact Top-K threshold
  // when their FP32 distance is about to be computed.
  int64_t raw_vector_read_count_;
  int64_t raw_vector_key_count_;
  int64_t raw_vector_refine_count_;
  // Maximum pending buffer capacity in entries, including unused slots.
  int64_t peak_pending_candidate_capacity_;
  int64_t first_low_precision_vector_scan_cid_;
};

// Borrowed by all Segment iterators of one partition search. The first round
// always completes; only subsequent latency-sensitive rounds stop at batches.
struct ObIvfScanBudget final
{
  explicit ObIvfScanBudget(const int64_t max_scan_vectors)
    : max_scan_vectors_(max_scan_vectors), scanned_vectors_(0), allow_batch_stop_(false)
  {}
  void consume(const int64_t count)
  {
    if (count > 0) {
      scanned_vectors_ = count > INT64_MAX - scanned_vectors_
          ? INT64_MAX : scanned_vectors_ + count;
    }
  }
  bool is_exhausted() const
  {
    return max_scan_vectors_ > 0 && scanned_vectors_ > max_scan_vectors_;
  }
  bool should_stop() const { return allow_batch_stop_ && is_exhausted(); }
  TO_STRING_KV(K_(max_scan_vectors), K_(scanned_vectors), K_(allow_batch_stop));

  int64_t max_scan_vectors_;
  // Low-precision rows consumed across Segments and rounds, independent of read statistics.
  int64_t scanned_vectors_;
  bool allow_batch_stop_;
};

struct ObIvfKnnSearchParam
{
  ObIvfKnnSearchParam() { reset(); }
  void reset();
  bool is_common_valid() const
  {
    return nullptr != query_
        && dim_ > 0
        && dim_ <= INT32_MAX
        && k_ > 0
        && std::isfinite(refine_k_) && refine_k_ >= 1.0F;
  }
  bool is_auto_search() const
  {
    return 0 == nprobe_ && original_k_ > 0 && num_candidates_ >= original_k_;
  }
  bool is_valid() const
  {
    return is_common_valid() && (nprobe_ > 0 || is_auto_search());
  }
  bool is_brute_force_valid() const
  {
    return is_common_valid()
        && nullptr != pre_filter_
        && pre_filter_->is_valid();
  }
  TO_STRING_KV(KP_(query), K_(dim), K_(nprobe), K_(k), K_(original_k), K_(num_candidates),
      K_(refine_k), K_(is_set_refine_k), K_(selectivity), K_(cluster_size), KP_(read_stats), KP_(deletion_bitmap_handle),
      KP_(pre_filter), KP_(scan_budget));

  const float *query_;
  int64_t dim_;
  int64_t nprobe_;
  // Internal first-batch selection budget; counts posting rows of unique
  // leaves. Zero keeps the explicitly requested centroid count.
  int64_t centroid_child_budget_;
  bool iterative_centroids_; // Caller requests retained candidates for subsequent batches.
  // Posting queries skip empty leaves. Geometry/insertion routing keeps them.
  bool skip_empty_centroids_;
  // Optional plan metric; -1 preserves existing callers without plan metadata.
  int64_t expected_dist_type_;
  int64_t k_;
  // Keep the original_k_ separate from k_, which k_ may increase when a later filter round needs candidates.
  int64_t original_k_;
  int64_t num_candidates_;
  bool is_set_refine_k_;
  // Filter selectivity from the SQL estimate; negative when unavailable.
  double selectivity_;
  // Resolved query/index refinement multiplier, shared by parent and leaf budgets.
  float refine_k_;
  // Index build parameter used for automatic centroid batches and cache statistics.
  int64_t cluster_size_;
  ObIvfSSTableReadStats *read_stats_;
  ObIvfScanBudget *scan_budget_;
  // Borrowed for the current synchronous search only. The caller must keep
  // the handle and its bitmap alive and unchanged until that search returns.
  const storage::ObDeletionBitmapHandle *deletion_bitmap_handle_;
  // Borrowed for the current synchronous search only. The caller must keep
  // the filter and its backing bitmap alive and unchanged until that search
  // returns.
  const ObIvfPreFilter *pre_filter_;
};

struct ObIvfKnnEntry
{
  ObIvfKnnEntry() : cid_(0), vid_(0), distance_(0) {}
  TO_STRING_KV(K_(cid), K_(vid), K_(distance));
  int64_t cid_;
  int64_t vid_;
  float distance_;
};

class ObIvfKnnSearchResult final
{
public:
  ObIvfKnnSearchResult() : items_() {}
  explicit ObIvfKnnSearchResult(common::ObIAllocator &alloc)
    : items_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(alloc)) {}
  explicit ObIvfKnnSearchResult(const lib::ObMemAttr &attr)
    : items_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(attr)) {}
  void reset() { items_.reset(); }
  int add(const ObIvfKnnEntry &item) { return items_.push_back(item); }
  int64_t count() const { return items_.count(); }
  const ObIvfKnnEntry &at(const int64_t idx) const { return items_.at(idx); }
  TO_STRING_KV(K_(items));

private:
  common::ObSEArray<ObIvfKnnEntry, 16> items_;
};

class ObIvfTopKCollector final
{
public:
  ObIvfTopKCollector();
  ~ObIvfTopKCollector();

  int init(int64_t k, common::ObIAllocator &allocator);
  // Keep the best distance per VID. CID identifies the retained posting only.
  int push(int64_t cid, int64_t vid, float distance);
  // Only retained, exact-distance candidates are tracked, not all scanned VIDs.
  bool contains(int64_t vid) const;
  int get_distance_threshold(float &distance) const;
  int finish(ObIvfKnnSearchResult &result);
  void reset();

private:
  struct ObIvfKnnNode
  {
    ObIvfKnnNode() : item_(), next_(nullptr), heap_idx_(0) {}
    TO_STRING_KV(K_(item), K_(heap_idx));
    ObIvfKnnEntry item_;
    ObIvfKnnNode *next_;
    int64_t heap_idx_;
  };

  struct ObIvfKnnWorseCmp
  {
    bool operator()(const ObIvfKnnEntry &lhs, const ObIvfKnnEntry &rhs) const
    {
      return lhs.distance_ < rhs.distance_
          || (lhs.distance_ == rhs.distance_ && lhs.vid_ < rhs.vid_);
    }
    bool operator()(const ObIvfKnnNode *lhs, const ObIvfKnnNode *rhs) const
    {
      return operator()(lhs->item_, rhs->item_);
    }
    int get_error_code() { return OB_SUCCESS; }
  };

  typedef common::ObRemovableHeap<ObIvfKnnNode *, ObIvfKnnWorseCmp,
      &ObIvfKnnNode::heap_idx_, 16> ObIvfKnnHeap;

  ObIvfKnnNode **find_node_link(int64_t vid) const;

  ObIvfKnnWorseCmp cmp_;
  common::ObIAllocator *allocator_;
  ObIvfKnnHeap *heap_;
  // Intrusive VID hash chains share a fixed node pool with the heap. Replacing
  // the worst candidate reuses its node, including with an arena allocator.
  ObIvfKnnNode *nodes_;
  ObIvfKnnNode **buckets_;
  int64_t k_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfTopKCollector);
};

struct ObIvfCandidateProgress
{
  enum State { MORE, WINDOW_END, DOMAIN_END };
  static constexpr int64_t MAX_BATCH_SIZE = 256;
  ObIvfCandidateProgress() : state_(MORE), centroids_started_(0), postings_scanned_(0) {}
  void reset() { state_ = MORE; centroids_started_ = 0; postings_scanned_ = 0; }
  State state_;
  int64_t centroids_started_;
  int64_t postings_scanned_;
  TO_STRING_KV(K_(state), K_(centroids_started), K_(postings_scanned));
};

// Allocator-backed common search lifecycle. Derived classes implement
// only the algorithm-specific candidate production step. The owned SSTable
// handle keeps the current SSTable alive. The borrowed
// ObTableAccessContext, its statement allocator, and related scan state must
// outlive the Searcher and must not be concurrently mutated while operations
// are in flight. Callers must serialize knn_search(), brute_force_search(),
// open(), reuse(), and destroy(); concurrent or reentrant use is unsupported.
class ObIvfSSTableSearcher
{
public:
  virtual ~ObIvfSSTableSearcher();
  // Atomically leave the current SSTable and bind another one. All SSTables
  // switched into one searcher must belong to the same index and have identical
  // query-related metadata, including dimension, distance type, and FHT
  // configuration. Query preprocessing owned by the searcher is retained across
  // switches.
  int switch_sstable(const storage::ObTableHandleV2 &table_handle);
  // Query preprocessing is cached on the first call. All subsequent calls on
  // this searcher must use the same query vector and dimension.
  int knn_search(
      const ObIvfKnnSearchParam &query_param,
      ObIvfKnnSearchResult &result,
      bool quantized_only = false);
  // Refine only the candidates that passed the SQL predicate, on their source SSTable.
  int refine_candidates(const ObIvfKnnSearchParam &query_param,
      const ObIvfKnnSearchResult &candidates, ObIvfKnnSearchResult &result);
  // POST-only pull: retain CID-internal state across batches. The caller must
  // finish filtering each result before requesting another batch. Only a full
  // filtered Top-K may provide a finite competitive distance threshold. Query,
  // stats and filter handles remain fixed and alive until reuse()/destruction.
  int next_candidate_batch(const ObIvfKnnSearchParam &query_param, float distance_threshold,
      ObIvfKnnSearchResult &result, ObIvfCandidateProgress &progress,
      int64_t batch_size = ObIvfCandidateProgress::MAX_BATCH_SIZE);
  // Whether the current KNN centroid iterator still has an unvisited CID.
  // Returns false before the first KNN round and for an empty SSTable.
  bool has_remaining_centroids() const;
  // Brute-force search requires a valid query_param.pre_filter_ and ignores
  // query_param.nprobe_.
  int brute_force_search(
      const ObIvfKnnSearchParam &query_param,
      ObIvfKnnSearchResult &result);
  // Release state created by the previous search while keeping both the
  // fixed query preprocessing and current SSTable binding.
  void reuse();
  // Destroy a searcher returned by ObIvfSSTable::create_searcher() and return
  // its storage to the allocator used by the factory. Stack objects must use
  // normal scope-based destruction instead.
  static void destroy(ObIvfSSTableSearcher *searcher);

protected:
  explicit ObIvfSSTableSearcher(common::ObIAllocator &allocator);

  int init_common(
      const storage::ObTableHandleV2 &table_handle,
      const storage::ObTableScanParam &scan_param,
      storage::ObTableAccessContext &access_ctx);
  int create_centroid_iterator(
      const ObIvfKnnSearchParam &query_param,
      const float *rotated_query, bool iterative = false, int64_t child_budget = 0);
  virtual int do_knn_search(
      const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &collector) = 0;
  virtual int do_quantized_search(const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &collector) { return OB_NOT_SUPPORTED; }
  virtual int do_refine_candidates(const ObIvfKnnSearchParam &query_param,
      const ObIvfKnnSearchResult &candidates,
      ObIvfTopKCollector &collector) { return OB_NOT_SUPPORTED; }
  virtual int do_brute_force_search(
      const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &collector) = 0;
  virtual int do_next_candidate_batch(const ObIvfKnnSearchParam &query_param,
      float distance_threshold, int64_t batch_size, ObIvfTopKCollector &collector,
      ObIvfCandidateProgress &progress) { return OB_NOT_SUPPORTED; }
  virtual void do_reuse() {}
  virtual int do_open_sstable() { return OB_SUCCESS; }
  virtual void do_close_sstable() {}

  const storage::ObTableIterParam &iter_param() const
  {
    return table_access_param_.iter_param_;
  }
  const storage::ObITableReadInfo &read_info() const
  {
    return *table_access_param_.iter_param_.read_info_;
  }
  const ObIvfSSTableRowDesc &output_row_desc() const
  {
    return output_row_desc_;
  }
  ObIvfSSTable *sstable() const { return sstable_; }
  common::ObIAllocator &query_allocator() { return query_arena_; }
  ObIvfQueryVectorContext &query_vector_context() { return query_vector_ctx_; }
  common::ObIAllocator &search_allocator() { return search_arena_; }
  ObIvfCentroidIterator *centroid_iterator() const { return centroid_iterator_; }
  storage::ObTableAccessContext *access_ctx() const { return access_ctx_; }

private:
  int check_query_metadata(const ObIvfKnnSearchParam &query_param) const;
  void reuse_round();
  void release_centroid_iterator();
  void close_sstable();

  // The arena is tenant-attributed and outlives all per-search allocations.
  common::ObArenaAllocator query_arena_;
  // Destroyed after centroid readers and before the arena owning its vector.
  ObIvfQueryVectorContext query_vector_ctx_;
  // Centroid ordering and its cursor persist across iterative KNN rounds.
  common::ObArenaAllocator centroid_arena_;
  // Per-round distance calculation and RAW_VECTOR refinement scratch space.
  common::ObArenaAllocator search_arena_;
  common::ObIAllocator &allocator_;
  storage::ObTableHandleV2 table_handle_;
  ObIvfSSTable *sstable_;
  storage::ObTableAccessContext *access_ctx_;
  storage::ObTableAccessParam table_access_param_;
  ObIvfSSTableRowDesc output_row_desc_;
  ObIvfCentroidIterator *centroid_iterator_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfSSTableSearcher);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_SEARCHER_H_
