/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_SQL_DAS_ASYNC_IVF_SEARCH_OP_H_
#define OCEANBASE_SQL_DAS_ASYNC_IVF_SEARCH_OP_H_

#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "lib/hash/ob_hashmap.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_scan_iter.h"

namespace oceanbase
{
namespace sql
{
// Distance is quantized before POST validation and refined afterwards.
// Conversion to SQL distance/score belongs to the adapter/Vector Driver.
struct ObDASVectorCandidate
{
  blocksstable::ObIvfKnnEntry entry_;
  int64_t group_slot_;
  int64_t segment_slot_;
  TO_STRING_KV(K_(entry), K_(group_slot), K_(segment_slot));
};

// A partition epoch owns this set. Keep the first source identity until reset;
// SOAR CIDs are physical copies, not separate logical query results.
class ObDASAsyncIVFCandidateSet
{
  typedef common::hash::HashMapTypes<int64_t, ObDASVectorCandidate>::AllocType Node;
  class NodeAllocator
  {
  public:
    NodeAllocator() : allocator_(nullptr) {}
    explicit NodeAllocator(common::ObIAllocator &allocator) : allocator_(&allocator) {}
    Node *alloc()
    {
      void *buf = nullptr == allocator_ ? nullptr : allocator_->alloc(sizeof(Node));
      return nullptr == buf ? nullptr : new (buf) Node();
    }
    void free(Node *node)
    {
      if (nullptr != node) { node->~Node(); allocator_->free(node); }
    }
  private:
    common::ObIAllocator *allocator_;
  };
  typedef common::hash::ObHashMap<int64_t, ObDASVectorCandidate,
      common::hash::NoPthreadDefendMode, common::hash::hash_func<int64_t>,
      common::hash::equal_to<int64_t>, NodeAllocator, common::hash::NormalPointer,
      common::ModulePageAllocator> CandidateMap;
public:
  explicit ObDASAsyncIVFCandidateSet(common::ObIAllocator &allocator)
    : node_allocator_(allocator), bucket_allocator_(allocator), seen_() {}
  ~ObDASAsyncIVFCandidateSet() { reset(); }
  int retain_new(common::ObIArray<ObDASVectorCandidate> &candidates);
  void reset() { seen_.destroy(); }
private:
  NodeAllocator node_allocator_;
  common::ModulePageAllocator bucket_allocator_;
  CandidateMap seen_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncIVFCandidateSet);
};

class ObIDASVectorSearchOp
{
public:
  virtual ~ObIDASVectorSearchOp() = default;
  virtual int search_round(int64_t nprobe, common::ObIArray<ObDASVectorCandidate> &out, int64_t candidate_limit = 0) = 0;
  virtual int search_brute_force(common::ObIArray<ObDASVectorCandidate> &out) = 0;
  virtual bool has_more() const = 0;
  virtual void close() = 0;
};

// One scanner per visible range: switching another SSTable would lose its CID cursor.
class ObDASAsyncIVFSegmentScanOp final : public ObIDASVectorSearchOp
{
public:
  explicit ObDASAsyncIVFSegmentScanOp(common::ObIAllocator &alloc)
    : alloc_(alloc), scanner_(nullptr), profile_(nullptr), group_slot_(-1), segment_slot_(-1),
      opened_(false), first_round_(true) {}
  ~ObDASAsyncIVFSegmentScanOp() { close(); }
  int open(const share::ObLSID &ls_id, storage::ObTableScanParam &scan_param, const storage::ObTabletHandle &tablet,
           const common::ObNewVersionRange &range, const share::SCN &sql_read_snapshot,
           const blocksstable::ObIvfKnnSearchParam &query,
           int64_t group_slot, int64_t segment_slot, bool post_filter,
           common::ObOpProfile<common::ObMetric> *parent_profile);
  int search_round(int64_t nprobe, common::ObIArray<ObDASVectorCandidate> &out, int64_t candidate_limit = 0) override;
  int search_brute_force(common::ObIArray<ObDASVectorCandidate> &out) override;
  int search_quantized(int64_t nprobe, common::ObIArray<ObDASVectorCandidate> &out);
  int refine_candidates(const blocksstable::ObIvfKnnSearchResult &selected,
      common::ObIArray<ObDASVectorCandidate> &out);
  bool has_more() const override { return opened_ && (first_round_ || scanner_->has_remaining_centroids()); }
  void close() override;
  TO_STRING_KV(K_(group_slot), K_(segment_slot), K_(opened));
private:
  int append_result(const blocksstable::ObIvfKnnSearchResult &result,
                    common::ObIArray<ObDASVectorCandidate> &out);
  common::ObIAllocator &alloc_;
  storage::ObIvfSSTableScanIter *scanner_;
  // Owned by the parent async IVF scan node; reset on close().
  common::ObOpProfile<common::ObMetric> *profile_;
  int64_t group_slot_;
  int64_t segment_slot_;
  bool opened_;
  bool first_round_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncIVFSegmentScanOp);
};

// A partition epoch borrows the immutable catalog and completed PRE bitmaps.
// The caller owns refill policy. This node emits only new round candidates and
// retains each Segment's quantized Top-M before POST validation.
class ObDASAsyncIVFTopKMergeOp final : public ObIDASVectorSearchOp
{
public:
  explicit ObDASAsyncIVFTopKMergeOp(common::ObIAllocator &alloc)
    : alloc_(alloc), children_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(alloc)), seen_(alloc), opened_(false),
      profile_(nullptr), post_filter_(false), next_post_child_(0) {}
  ~ObDASAsyncIVFTopKMergeOp() { close(); }
  int open(const ObAsyncIndexPartitionSegCtx &partition, uint64_t logical_index_id,
           storage::ObTableScanParam &scan_param, const blocksstable::ObIvfKnnSearchParam &query,
           const common::ObIArray<const blocksstable::ObIvfPreFilter *> *group_filters = nullptr);
  // The SQL profile node that owns this epoch's per-segment children.
  void set_profile(common::ObOpProfile<common::ObMetric> *profile, bool post_filter)
  {
    profile_ = profile;
    post_filter_ = post_filter;
  }
  int search_round(int64_t nprobe, common::ObIArray<ObDASVectorCandidate> &out, int64_t candidate_limit = 0) override;
  int search_brute_force(common::ObIArray<ObDASVectorCandidate> &out) override;
  int next_post_filter_batch(int64_t nprobe,
      common::ObIArray<ObDASVectorCandidate> &out, bool &round_end);
  int refine_candidates(const blocksstable::ObIvfKnnSearchResult &selected,
      common::ObIArray<ObDASVectorCandidate> &out);
  bool has_more() const override;
  void close() override;
  static int collect_topk(const common::ObIArray<ObDASVectorCandidate> &candidates,
                          int64_t k, common::ObIAllocator &alloc,
                          blocksstable::ObIvfKnnSearchResult &out);
private:
  common::ObIAllocator &alloc_;
  common::ObSEArray<ObDASAsyncIVFSegmentScanOp *, 8> children_;
  ObDASAsyncIVFCandidateSet seen_;
  bool opened_;
  common::ObOpProfile<common::ObMetric> *profile_;
  bool post_filter_;
  int64_t next_post_child_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncIVFTopKMergeOp);
};
} // namespace sql
} // namespace oceanbase
#endif
