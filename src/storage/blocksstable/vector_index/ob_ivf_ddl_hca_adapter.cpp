/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <cmath>

#include "storage/blocksstable/vector_index/ob_ivf_ddl_hca_adapter.h"
#include "share/vector_index/hca/ob_vector_hca_refine.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_sorter.h"
#include "storage/ob_parallel_external_sort.h"
#include "share/vector_index/hca/ob_vector_hca_soar.h"
#include "lib/utility/ob_sort.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
namespace ddl_hca
{
class Neighborhood final : public share::ObHcaNeighborhoodStore
{
public:
  explicit Neighborhood(const ObIVFNeighborGraphView &graph)
    : graph_(graph)
  {}
  virtual int64_t center_count() const override { return graph_.center_count_; }
  virtual int64_t neighbor_count(const int64_t cid) const override
  {
    return cid >= 0 && cid < center_count()
        ? graph_.offsets_->at(cid + 1) - graph_.offsets_->at(cid) : -1;
  }
  virtual int get_neighbor(
      const int64_t cid,
      const int64_t offset,
      int32_t &neighbor_cid) const override
  {
    int ret = OB_SUCCESS;
    neighbor_cid = -1;
    if (cid < 0 || cid >= center_count() || offset < 0
        || offset >= neighbor_count(cid)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL neighborhood lookup", K(ret), K(cid), K(offset), "centers", center_count());
    } else {
      const int64_t value = graph_.cids_->at(
          graph_.offsets_->at(cid) + offset);
      if (value < 0 || value > INT32_MAX) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid DDL neighborhood CID", K(ret), K(cid), K(offset), K(value));
      } else {
        neighbor_cid = static_cast<int32_t>(value);
      }
    }
    return ret;
  }
  virtual int get_max_intra_distance(
      const int64_t cid,
      float &distance) const override
  {
    int ret = OB_SUCCESS;
    distance = 0.0F;
    if (cid < 0 || cid >= center_count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL neighborhood distance lookup", K(ret), K(cid), "centers", center_count());
    } else {
      distance = graph_.max_intra_distances_->at(cid);
    }
    return ret;
  }
  virtual int set_row(
      const int64_t,
      const int32_t *,
      const int64_t,
      const float) override
  {
    return OB_NOT_SUPPORTED;
  }
private:
  const ObIVFNeighborGraphView graph_;
};

// The numerical kernel emits rows in CID order. Keep only this task's output;
// global CIDs still address the shared center matrix and HGraph.
class NeighborRows final : public share::ObHcaNeighborhoodStore
{
public:
  NeighborRows(int64_t center_count, int64_t begin, int64_t end,
      ObIArray<int64_t> &offsets, ObIArray<int64_t> &cids, ObIArray<float> &distances)
    : center_count_(center_count), begin_(begin), end_(end),
      offsets_(offsets), cids_(cids), distances_(distances)
  {}
  int64_t center_count() const override { return center_count_; }
  int64_t neighbor_count(int64_t cid) const override
  {
    return cid >= begin_ && cid < end_ && cid - begin_ < distances_.count()
        ? offsets_.at(cid - begin_ + 1) - offsets_.at(cid - begin_) : -1;
  }
  int get_neighbor(int64_t cid, int64_t offset, int32_t &neighbor) const override
  {
    int ret = OB_SUCCESS;
    neighbor = -1;
    if (offset < 0 || offset >= neighbor_count(cid)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL neighborhood row lookup", K(ret), K(cid), K(offset), K_(begin), K_(end));
    } else {
      neighbor = static_cast<int32_t>(cids_.at(offsets_.at(cid - begin_) + offset));
    }
    return ret;
  }
  int get_max_intra_distance(int64_t cid, float &distance) const override
  {
    int ret = OB_SUCCESS;
    distance = 0.0F;
    if (cid < begin_ || cid >= end_ || cid - begin_ >= distances_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL neighborhood row distance lookup", K(ret), K(cid), K_(begin), K_(end), "rows",
               distances_.count());
    } else {
      distance = distances_.at(cid - begin_);
    }
    return ret;
  }
  int set_row(int64_t cid, const int32_t *neighbors, int64_t count, float distance) override
  {
    int ret = OB_SUCCESS;
    if (cid < begin_ || cid >= end_ || cid - begin_ != distances_.count()
        || offsets_.count() != distances_.count() + 1
        || offsets_.at(offsets_.count() - 1) != cids_.count()
        || count < 0 || (count > 0 && neighbors == nullptr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL neighborhood output row", K(ret), K(cid), K(count), K_(begin), K_(end), "offsets",
               offsets_.count(), "distances", distances_.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (neighbors[i] < 0 || neighbors[i] >= center_count_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid DDL output neighbor CID", K(ret), K(cid), K(i), "neighbor", neighbors[i], K_(center_count));
      } else if (OB_FAIL(cids_.push_back(neighbors[i]))) {
        LOG_WARN("failed to append DDL neighborhood CID", K(ret), K(cid), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(offsets_.push_back(cids_.count()))) {
      LOG_WARN("failed to append DDL neighborhood offset", K(ret), K(cid));
    }
    if (OB_SUCC(ret) && OB_FAIL(distances_.push_back(distance))) {
      LOG_WARN("failed to append DDL neighborhood distance", K(ret), K(cid));
    }
    return ret;
  }
private:
  const int64_t center_count_;
  const int64_t begin_;
  const int64_t end_;
  ObIArray<int64_t> &offsets_;
  ObIArray<int64_t> &cids_;
  ObIArray<float> &distances_;
};

class Remap final : public share::ObHcaCidRemapStore
{
public:
  explicit Remap(const int64_t count) : count_(count) {}
  virtual int64_t count() const override { return count_; }
  virtual int get_new_cid(
      const int64_t old_cid,
      int32_t &new_cid) const override
  {
    int ret = OB_SUCCESS;
    new_cid = -1;
    if (old_cid < 0 || old_cid >= count_ || old_cid > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL identity CID remap", K(ret), K(old_cid), K_(count));
    } else {
      new_cid = static_cast<int32_t>(old_cid);
    }
    return ret;
  }
  virtual int set_new_cid(const int64_t, const int32_t) override
  {
    return OB_NOT_SUPPORTED;
  }
private:
  int64_t count_;
};

class Members final : public share::ObHcaCidMemberStore
{
public:
  explicit Members(int64_t cid_count) : cid_count_(cid_count) {}
  int64_t cid_count() const override { return cid_count_; }
  int64_t member_count(int64_t) const override { return 0; }
  int get_member(int64_t cid, int64_t offset, int64_t &) const override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot read discarded IVF SOAR member rows", K(ret), K(cid), K(offset));
    return ret;
  }
  int append_member(int32_t cid, int64_t vector_idx) override
  {
    const int ret = cid >= 0 && cid < cid_count_ && vector_idx >= 0 ? OB_SUCCESS : OB_INVALID_ARGUMENT;
    if (OB_SUCCESS != ret) {
      LOG_WARN("invalid IVF SOAR member", K(ret), K(cid), K(cid_count_), K(vector_idx));
    }
    return ret;
  }
  int move_row(int64_t src_cid, int64_t dst_cid) override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot move discarded IVF SOAR member rows", K(ret), K(src_cid), K(dst_cid));
    return ret;
  }
  int bind_row(int64_t cid, const share::ObHcaCidMemberStore &, int64_t source_cid) override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot bind discarded IVF SOAR member rows", K(ret), K(cid), K(source_cid));
    return ret;
  }
private:
  int64_t cid_count_;
};


// Numerical kernels see task-local positions. Global assignments remain borrowed.
class Centers final : public share::ObHcaCenterStore
{
public:
  Centers(const share::ObVectorMatrix &values, int64_t dim, share::ObVectorMatrix *writable = nullptr)
    : values_(values), writable_(writable), dim_(dim) {}
  int64_t count() const override { return dim_ > 0 ? values_.count() / dim_ : 0; }
  int64_t capacity() const override { return count(); }
  int64_t dimension() const override { return dim_; }
  int get_center(int64_t cid, const float *&v) const override
  { v = cid >= 0 && cid < count() ? values_.data_at(cid * dim_, dim_) : nullptr;
    return v != nullptr ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
  int get_mutable_center(int64_t cid, float *&v) override
  { v = writable_ != nullptr && cid >= 0 && cid < count() ? writable_->data_at(cid * dim_, dim_) : nullptr;
    return v != nullptr ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
  int set_center(int64_t cid, const float *v) override
  { float *target = nullptr; int ret = get_mutable_center(cid, target);
    if (OB_SUCC(ret) && v == nullptr) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("null DDL center input", K(ret), K(cid), K_(dim));
    }
    if (OB_SUCC(ret) && target != v) { MEMCPY(target, v, dim_ * sizeof(float)); }
    return ret; }
private:
  const share::ObVectorMatrix &values_;
  share::ObVectorMatrix *writable_;
  int64_t dim_;
};

template <typename Values>
class Assignments final : public share::ObHcaAssignmentStore
{
public:
  Assignments(const Values &values, const share::ObVectorArray<int64_t> *indices = nullptr,
      Values *writable = nullptr)
    : values_(values), indices_(indices), writable_(writable) {}
  int64_t count() const override { return indices_ == nullptr ? values_.count() : indices_->count(); }
  int get_assignment(int64_t idx, int32_t &cid) const override
  {
    int ret = OB_SUCCESS;
    cid = -1;
    if (idx < 0 || idx >= count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL assignment index", K(ret), K(idx), "count", count());
    } else {
      const int64_t source = indices_ == nullptr ? idx : indices_->at(idx);
      if (source < 0 || source >= values_.count() || values_.at(source) < -1 || values_.at(source) > INT32_MAX) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid DDL assignment source or CID", K(ret), K(idx), K(source), "values", values_.count());
      } else {
        cid = static_cast<int32_t>(values_.at(source));
      }
    }
    return ret;
  }
  int set_assignment(int64_t idx, int32_t cid) override
  {
    if (writable_ == nullptr || indices_ != nullptr || idx < 0 || idx >= count()) {
      return OB_INVALID_ARGUMENT;
    }
    writable_->at(idx) = cid;
    return OB_SUCCESS;
  }
private:
  const Values &values_;
  const share::ObVectorArray<int64_t> *indices_;
  Values *writable_;
};

class Batch final : public share::ObHcaVectorBatch
{
public:
  Batch(const share::ObVectorMatrix &values, int64_t dim) : values_(values), dim_(dim) {}
  int64_t count() const override { return dim_ > 0 ? values_.count() / dim_ : 0; }
  int64_t dimension() const override { return dim_; }
  int get_vector(int64_t pos, int64_t &idx, const float *&v) const override
  { idx = pos; v = pos >= 0 && pos < count() ? values_.data_at(pos * dim_, dim_) : nullptr;
    return v == nullptr ? OB_INVALID_ARGUMENT : OB_SUCCESS; }
private:
  const share::ObVectorMatrix &values_;
  int64_t dim_;
};

// The Processor derives changed CIDs and reverse membership once at the barrier.
class Changed final : public share::ObHcaChangedCidSet
{
public:
  explicit Changed(int64_t k) : k_(k) {}
  bool has_changed(int64_t) const override { return false; }
  int set_changed(int64_t cid) override { return cid >= 0 && cid < k_ ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
private:
  int64_t k_;
};

// Gather each source range once, then external-sort its samples by epoch/batch.
// Every sealed shard contains consecutive training ranges. A single Reader per
// shard survives batch boundaries; only the current logical batch is copied.
// Fully resident sources bypass these stores and supply each batch on demand.
class SampleCache final : public share::ObHcaVectorAccessor
{
  struct Shard
  {
    using Sorter = ObExternalSort<ObIVFSortItem, ObIVFClusterSampleCompare>;
    explicit Shard(ObIAllocator &allocator)
        : store_(allocator),
          loaded_positions_(ObMemAttr(MTL_ID(), "IVFRefinePlan", ObCtxIds::VECTOR_CTX_ID)), started_(false),
          complete_(false)
    {
      ends_.set_attr(ObMemAttr(MTL_ID(), "IVFRefinePlan", ObCtxIds::VECTOR_CTX_ID));
    }
    ~Shard() { sorter_.clean_up(); }
    void release_sorter()
    {
      // clean_up() drops rows/files but retains the pointer-array capacity.
      // Reconstruct an empty sorter once all output has been copied, so its
      // pages are released without changing the common sorting implementation.
      sorter_.clean_up();
      sorter_.~Sorter();
      new (&sorter_) Sorter();
    }
    ObIVFReplayInput store_;
    ObIVFClusterSampleCompare compare_;
    Sorter sorter_;
    ObArray<int64_t> ends_;
    share::ObVectorArray<int64_t> loaded_positions_;
    bool started_;
    bool complete_;
    TO_STRING_KV(K_(started), K_(complete));
  };
public:
  SampleCache(ObIAllocator &allocator, int64_t count, int64_t dim, const volatile int *cancel)
      : allocator_(allocator), count_(count), dim_(dim), cancel_(cancel), ready_(false), memory_limit_(0),
        shard_count_(0), epochs_(0), batch_size_(0), batches_per_epoch_(0), rows_per_epoch_(0), next_batch_(0),
        read_rows_(0), resident_source_(nullptr), source_vids_(),
        indices_(ObMemAttr(MTL_ID(), "IVFRefinePlan", ObCtxIds::VECTOR_CTX_ID)),
        loaded_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(ObMemAttr(MTL_ID(), "IVFRefinePlan", ObCtxIds::VECTOR_CTX_ID))),
        ranks_(ObMemAttr(MTL_ID(), "IVFRefinePlan", ObCtxIds::VECTOR_CTX_ID)),
        batch_indices_(ObMemAttr(MTL_ID(), "IVFRefineBatch", ObCtxIds::VECTOR_CTX_ID)),
        batch_values_(ObMemAttr(MTL_ID(), "IVFRefineBatch", ObCtxIds::VECTOR_CTX_ID))
  {
    shards_.set_block_allocator(ObWrapperAllocator(allocator));
    batch_loaded_.set_attr(ObMemAttr(MTL_ID(), "IVFRefineBatch", ObCtxIds::VECTOR_CTX_ID));
    const int rc = batch_values_.set_dimension(dim_);
    OB_ASSERT(rc == OB_SUCCESS);
  }
  ~SampleCache()
  {
    for (int64_t i = 0; i < shards_.count(); ++i) { shards_.at(i)->~Shard(); }
  }
  int init(const share::ObVectorArray<int64_t> &samples, int64_t size, int64_t memory_limit, int64_t shards,
      int64_t epochs, const share::ObHcaRefineTrainer &trainer)
  {
    int ret = OB_SUCCESS;

    share::ObVectorArray<int64_t> shuffled(ObMemAttr(MTL_ID(), "IVFRefinePlan", ObCtxIds::VECTOR_CTX_ID));
    if (size > samples.count() || size <= 0 || size > count_ || dim_ <= 0 || size > INT64_MAX / sizeof(float) / dim_
        || memory_limit < 0 || shards <= 0 || epochs <= 0 || epochs > INT64_MAX / sizeof(int64_t) / size) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL refinement sample plan", K(ret), K(size), K_(count), K_(dim),
          K(memory_limit), K(shards), K(epochs));
    } else if (OB_FAIL(share::vector_array_resize(indices_, size))
        || OB_FAIL(loaded_.prepare_allocate(size))
        || OB_FAIL(share::vector_array_resize(ranks_, size * epochs))
        || OB_FAIL(share::vector_array_resize(shuffled, size))) {
      LOG_WARN("failed to allocate DDL refinement sample plan", K(ret), K(size), K(epochs));
    } else {
      for (int64_t i = 0; i < size; ++i) { indices_.at(i) = samples.at(i); }
      std::fill_n(loaded_.get_data(), loaded_.count(), 0);
      lib::ob_sort(share::vector_array_begin(indices_), share::vector_array_end(indices_));
      memory_limit_ = memory_limit;
      shard_count_ = MIN(shards, size);
      epochs_ = epochs;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      if (indices_.at(i) < 0 || indices_.at(i) >= count_ || (i > 0 && indices_.at(i - 1) >= indices_.at(i))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid or duplicate DDL refinement sample index", K(ret), K(i), K_(count));
      }
    }
    for (int64_t epoch = 0; OB_SUCC(ret) && epoch < epochs_; ++epoch) {
      if (OB_FAIL(trainer.get_epoch_sample_indices(epoch, shuffled, size, batch_size_))) {
        LOG_WARN("failed to plan DDL refinement epoch", K(ret), K(epoch), K(size));
      }
      for (int64_t rank = 0; OB_SUCC(ret) && rank < size; ++rank) {
        const int64_t pos = find_index(indices_, shuffled.at(rank));
        if (pos < 0) {
          ret = OB_INVALID_DATA;
          LOG_WARN("unknown DDL refinement epoch sample", K(ret), K(epoch), K(rank));
        } else {
          ranks_.at(epoch * size + pos) = rank;
        }
        if (OB_SUCC(ret) && rank % 4096 == 0) { ret = check_cancel(); }
      }
    }
    if (OB_SUCC(ret)) {
      batches_per_epoch_ = (size - 1) / batch_size_ + 1;
      if (batches_per_epoch_ > INT64_MAX / batch_size_
          || epochs_ > INT64_MAX / batches_per_epoch_ / batch_size_ / dim_ / sizeof(float)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("DDL refinement training layout overflows", K(ret), K_(epochs), K_(batch_size), K(size));
      } else {
        rows_per_epoch_ = batches_per_epoch_ * batch_size_;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < shard_count_; ++i) {
      void *buf = allocator_.alloc(sizeof(Shard));
      if (buf == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate DDL refinement sample shard", K(ret), K(i));
      } else {
        Shard *shard = new (buf) Shard(allocator_);
        if (OB_FAIL(shards_.push_back(shard))) {
          shard->~Shard();
          LOG_WARN("failed to retain DDL refinement sample shard", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("planned IVF DDL refinement batch stores", K(size), K_(dim), K_(memory_limit),
          K_(shard_count), K_(epochs), K_(batch_size), K_(batches_per_epoch),
          "stored_rows", rows_per_epoch_ * epochs_, "payload_bytes", rows_per_epoch_ * epochs_ * dim_ * sizeof(float));
    }
    return ret;
  }
  int init_resident(const ObIVFVectorStore &source, const ObIVFVectorIDView &vids)
  {
    int ret = OB_SUCCESS;
    if (!source.is_valid() || !source.is_fully_memory_resident()
        || source.get_vector_count() != count_ || source.get_dimension() != dim_
        || vids.count() != count_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid resident IVF refinement source", K(ret), K_(count), K_(dim),
          "source_count", source.get_vector_count(), "vid_count", vids.count());
    } else {
      // Both objects belong to the build runtime and outlive this session.
      resident_source_ = &source;
      source_vids_ = vids;
    }
    return ret;
  }
  bool uses_resident_source() const { return resident_source_ != nullptr; }
  int64_t count() const override { return count_; }
  int64_t dimension() const override { return dim_; }
  int64_t shard_count() const { return shard_count_; }
  int64_t memory_hold() const
  {
    int64_t bytes = 0;
    for (int64_t i = 0; i < shards_.count(); ++i) { bytes += shards_.at(i)->store_.get_mem_hold(); }
    return bytes;
  }
  int64_t file_size() const
  {
    int64_t bytes = 0;
    for (int64_t i = 0; i < shards_.count(); ++i) { bytes += shards_.at(i)->store_.get_file_size(); }
    return bytes;
  }
  int64_t read_rows() const { return read_rows_; }
  int begin_shard(int64_t shard)
  {
    int ret = OB_SUCCESS;
    if (ready_ || shard < 0 || shard >= shard_count_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid DDL sample shard start", K(ret), K(shard), K_(ready), K_(shard_count));
    } else {
      auto &s = *shards_.at(shard);
      // A retry owns this source range after the preceding attempt drains.
      // Clear its old completeness marks as well as its partially sorted rows.
      for (int64_t i = 0; i < s.loaded_positions_.count(); ++i) { loaded_.at(s.loaded_positions_.at(i)) = 0; }
      s.loaded_positions_.reuse();
      s.release_sorter();
      s.compare_.result_code_ = OB_SUCCESS;
      s.store_.~ObIVFReplayInput();
      new (&s.store_) ObIVFReplayInput(allocator_);
      s.started_ = false;
      s.complete_ = false;
      const int64_t limit = MAX(ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT, memory_limit_ / shard_count_);
      // Sorting is bounded workspace, separate from the retained sample quota.
      const int64_t sort_limit = MAX(8L << 20, (128L << 20) / shard_count_);
      if (OB_FAIL(s.ends_.prepare_allocate(epochs_ * batches_per_epoch_))) {
        LOG_WARN("failed to allocate DDL refinement batch ranges", K(ret), K(shard));
      } else if (OB_FAIL(s.store_.init(MTL_ID(), dim_, limit, 0, 0, -1, "IVFRefineStore"))) {
        LOG_WARN("failed to initialize DDL refinement sample store", K(ret), K(shard), K(limit));
      } else if (OB_FAIL(s.sorter_.init(sort_limit,
          OB_STORAGE_OBJECT_MGR.get_macro_block_size(), 0, MTL_ID(), &s.compare_))) {
        LOG_WARN("failed to initialize DDL refinement batch sorter", K(ret), K(shard), K(sort_limit));
      } else {
        MEMSET(&s.ends_.at(0), 0, s.ends_.count() * sizeof(int64_t));
        s.started_ = true;
      }
    }
    return ret;
  }
  int finish_shard(int64_t shard)
  {
    int ret = check_cancel();
    if (OB_FAIL(ret)) {
    } else if (ready_ || shard < 0 || shard >= shard_count_ || !shards_.at(shard)->started_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid DDL sample shard finish", K(ret), K(shard), K_(ready));
    } else {
      auto &s = *shards_.at(shard);
      if (s.complete_) { return OB_SUCCESS; }
      if (OB_FAIL(s.sorter_.do_sort(true))) {
        LOG_WARN("failed to sort DDL refinement batches", K(ret), K(shard));
      }
      const ObIVFSortItem *item = nullptr;
      int64_t previous_batch = -1;
      uint64_t previous_vid = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(s.sorter_.get_next_item(item))) {
          if (ret == OB_ITER_END) { ret = OB_SUCCESS; break; }
          LOG_WARN("failed to read sorted DDL refinement sample", K(ret), K(shard));
        } else if (item == nullptr || item->cid_ < 0 || item->cid_ >= s.ends_.count()
            || item->cid_ < previous_batch || (item->cid_ == previous_batch && item->vid_ < previous_vid)
            || item->vid_ >= static_cast<uint64_t>(count_) || item->payload_.length() != dim_ * sizeof(float)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid sorted DDL refinement sample", K(ret), K(shard), KP(item), K(previous_batch));
        } else if (OB_FAIL(s.store_.add_row(item->vid_, item->payload_))) {
          LOG_WARN("failed to write DDL refinement batch store", K(ret), K(shard), KPC(item));
        } else {
          ++s.ends_.at(item->cid_);
          previous_batch = item->cid_;
          previous_vid = item->vid_;
          if (s.store_.get_row_count() % 4096 == 0) { ret = check_cancel(); }
        }
      }
      for (int64_t i = 1; OB_SUCC(ret) && i < s.ends_.count(); ++i) { s.ends_.at(i) += s.ends_.at(i - 1); }
      if (OB_SUCC(ret) && OB_FAIL(s.store_.seal())) {
        LOG_WARN("failed to seal DDL refinement batch store", K(ret), K(shard));
      }
      s.release_sorter();
      if (OB_SUCC(ret)) { s.complete_ = true; }
    }
    return ret;
  }
  int get_vector(int64_t idx, const float *&v) const override
  {
    int ret = OB_SUCCESS;
    v = nullptr;
    const int64_t pos = find_index(batch_indices_, idx);
    if (!ready_ || pos < 0) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("DDL refinement sample is not prefetched", K(ret), K(idx), K_(ready));
    } else {
      v = batch_values_.data_at(pos * dim_, dim_);
    }
    return ret;
  }
  int prefetch_vectors(const share::ObVectorArray<int64_t> &idxs, int64_t count) const override
  {
    if (uses_resident_source()) { return prefetch_resident(idxs, count); }
    int ret = check_cancel();
    if (OB_FAIL(ret)) {
    } else if (!ready_ || count > idxs.count() || count != batch_size_ || next_batch_ >= epochs_ * batches_per_epoch_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid DDL refinement batch read", K(ret), K_(ready), K(count), K_(next_batch));
    } else if (OB_FAIL(share::vector_array_resize(batch_indices_, count))
        || (batch_values_.count() != count * dim_ && OB_FAIL(batch_values_.resize(count * dim_)))
        || OB_FAIL(batch_loaded_.prepare_allocate(count))) {
      LOG_WARN("failed to allocate DDL refinement batch vectors", K(ret), K(count), K_(dim));
    } else {
      for (int64_t i = 0; i < count; ++i) { batch_indices_.at(i) = idxs.at(i); }
      MEMSET(&batch_loaded_.at(0), 0, count);
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        if (idxs.at(i) < 0 || idxs.at(i) >= count_ || (i > 0 && idxs.at(i - 1) > idxs.at(i))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid DDL refinement batch indices", K(ret), K(i), K_(next_batch));
        }
      }
      int64_t copied = 0;
      for (int64_t shard = 0; OB_SUCC(ret) && shard < shard_count_; ++shard) {
        auto &s = *shards_.at(shard);
        const int64_t begin = next_batch_ == 0 ? 0 : s.ends_.at(next_batch_ - 1);
        const int64_t end = s.ends_.at(next_batch_);
        for (int64_t row = begin; OB_SUCC(ret) && row < end; ++row) {
          uint64_t idx = 0;
          const float *v = nullptr;
          if (row % 4096 == 0 && OB_FAIL(check_cancel())) {
          } else if (OB_FAIL(s.store_.get_row_at(row, idx, v))) {
            LOG_WARN("failed to read contiguous DDL refinement batch", K(ret), K(shard), K(row), K_(next_batch));
          } else {
            int64_t pos = idx < static_cast<uint64_t>(count_) ? find_index(batch_indices_, idx) : -1;
            // A wrapped batch can contain the same sample twice. Store both
            // occurrences, and restore their positions before distance/SGD.
            while (pos >= 0 && pos < count && batch_indices_.at(pos) == idx && batch_loaded_.at(pos)) { ++pos; }
            if (v == nullptr || pos < 0 || pos >= count || batch_indices_.at(pos) != idx) {
              ret = OB_INVALID_DATA;
              LOG_WARN("DDL refinement batch membership mismatch", K(ret), K(shard), K(row), K(idx), K_(next_batch));
            } else {
              MEMCPY(batch_values_.data_at(pos * dim_, dim_), v, dim_ * sizeof(float));
              batch_loaded_.at(pos) = 1;
              ++copied;
              ++read_rows_;
            }
          }
        }
      }
      if (OB_SUCC(ret) && copied != count) {
        ret = OB_INVALID_DATA;
        LOG_WARN("incomplete DDL refinement batch", K(ret), K(copied), K(count), K_(next_batch));
      }
      if (OB_SUCC(ret)) { ++next_batch_; }
    }
    return ret;
  }
  int load(const ObIArray<int64_t> &positions, const share::ObVectorMatrix &values, int64_t shard)
  {
    int ret = check_cancel();
    if (OB_FAIL(ret)) {
    } else if (ready_ || positions.empty() || positions.count() > INT64_MAX / dim_
        || values.count() != positions.count() * dim_ || shard < 0 || shard >= shard_count_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid DDL refinement sample load", K(ret), K(shard), K_(ready), K_(dim));
    } else if (!shards_.at(shard)->started_ || shards_.at(shard)->complete_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("DDL refinement sample shard is not writable", K(ret), K(shard));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < positions.count(); ++i) {
      if (positions.at(i) < 0 || positions.at(i) >= indices_.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid DDL refinement sample position", K(ret), K(i), "position", positions.at(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < positions.count(); ++i) {
      const int64_t pos = positions.at(i);
      auto &s = *shards_.at(shard);
      if (loaded_.at(pos)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("duplicate DDL refinement sample load", K(ret), K(shard), K(pos));
      }
      for (int64_t epoch = 0; OB_SUCC(ret) && epoch < epochs_; ++epoch) {
        for (int64_t rank = ranks_.at(epoch * indices_.count() + pos);
            OB_SUCC(ret) && rank < rows_per_epoch_; rank += indices_.count()) {
          ObIVFSortItem item;
          item.cid_ = epoch * batches_per_epoch_ + rank / batch_size_;
          item.vid_ = indices_.at(pos);
          item.payload_.assign_ptr(reinterpret_cast<const char *>(&values.at(i * dim_)), dim_ * sizeof(float));
          if (OB_FAIL(s.sorter_.add_item(item))) {
            LOG_WARN("failed to sort DDL refinement sample", K(ret), K(shard), K(pos), K(epoch));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(s.loaded_positions_.push_back(pos))) {
        LOG_WARN("failed to retain DDL refinement loaded position", K(ret), K(shard), K(pos));
      }
      if (OB_SUCC(ret)) { loaded_.at(pos) = 1; }
      if (OB_SUCC(ret) && i % 4096 == 0) { ret = check_cancel(); }
    }
    return ret;
  }
  int seal()
  {
    int ret = check_cancel();
    if (OB_FAIL(ret)) {
      LOG_WARN("DDL refinement sample sealing canceled", K(ret));
    } else if (uses_resident_source()) {
      ready_ = true;
      LOG_INFO("[IVF_BUILD_PERF]", "event", "refine_samples_bypass",
          "vector_count", count_, K_(dim), "stored_rows", 0, "file_size", 0);
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < loaded_.count(); ++i) {
        if (!loaded_.at(i)) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("DDL refinement sample is not loaded", K(ret), K(i), "samples", loaded_.count());
        }
      }
      int64_t rows = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < shards_.count(); ++i) {
        if (!shards_.at(i)->complete_) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("DDL refinement sample shard is incomplete", K(ret), K(i));
        } else { rows += shards_.at(i)->store_.get_row_count(); }
      }
      if (OB_SUCC(ret) && rows != rows_per_epoch_ * epochs_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("DDL refinement stored row count mismatch", K(ret), K(rows), K_(rows_per_epoch), K_(epochs));
      }
      if (OB_SUCC(ret)) {
        ready_ = true;
        ranks_.reset();
        for (int64_t i = 0; i < shards_.count(); ++i) { shards_.at(i)->loaded_positions_.reset(); }
        LOG_INFO("[IVF_BUILD_PERF]", "event", "refine_samples_seal", K_(memory_limit), K_(shard_count),
            "sample_rows", indices_.count(), "stored_rows", rows, "batch_count", epochs_ * batches_per_epoch_,
            "mem_hold", memory_hold(), "file_size", file_size());
        // Gathering succeeded for every shard. Training uses only the sealed
        // stores and per-batch indices; a failed seal retains retry metadata.
        indices_.reset();
        loaded_.reset();
      }
    }
    return ret;
  }
  const share::ObVectorArray<int64_t> &indices() const { return indices_; }
private:
  int prefetch_resident(const share::ObVectorArray<int64_t> &idxs, const int64_t count) const
  {
    int ret = check_cancel();
    const bool position_ids = resident_source_->uses_position_ids();

    share::ObVectorArray<ObIVFVectorVID> vids(ObMemAttr(MTL_ID(), "IVFRefineBatch", ObCtxIds::VECTOR_CTX_ID));
    if (OB_FAIL(ret)) {
    } else if (!ready_ || count > idxs.count() || count <= 0 || dim_ <= 0
        || count > INT64_MAX / dim_ / sizeof(float) || source_vids_.empty()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid resident IVF refinement batch", K(ret), K_(ready), K(count), K_(dim));
    } else if (OB_FAIL(share::vector_array_resize(batch_indices_, count))
        || (!position_ids && OB_FAIL(vids.reserve(count)))) {
      LOG_WARN("failed to allocate resident IVF refinement batch indices", K(ret), K(count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (i % 4096 == 0 && OB_FAIL(check_cancel())) {
      } else if (idxs.at(i) < 0 || idxs.at(i) >= count_ || (i > 0 && idxs.at(i - 1) > idxs.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid resident IVF refinement index", K(ret), K(i), "index", idxs.at(i), K_(count));
      } else if (!position_ids && OB_FAIL(vids.push_back(source_vids_.at(idxs.at(i))))) {
        LOG_WARN("failed to retain resident IVF refinement VID", K(ret), K(i));
      } else {
        // Keep duplicate indices from wrapped batches in their original positions.
        batch_indices_.at(i) = idxs.at(i);
      }
    }
    if (OB_SUCC(ret)) {
      // The store copies through task-private readers. This buffer survives all
      // distance/assign/update tasks and is reused only for the next batch.
      const ObIVFVectorIDView ids = position_ids ? ObIVFVectorIDView::positions(idxs) : ObIVFVectorIDView(vids);
      if (OB_FAIL(resident_source_->batch_get_by_vid(ids, 0, count, batch_values_))) {
        LOG_WARN("failed to read resident IVF refinement batch", K(ret), K(count));
      } else if (batch_values_.count() != count * dim_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("incomplete resident IVF refinement batch", K(ret), K(count), K_(dim),
            "values", batch_values_.count());
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_values_.count(); ++i) {
      if (!std::isfinite(batch_values_.at(i))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("non-finite resident IVF refinement value", K(ret), K(i), K(count), K_(dim));
      }
    }
    if (OB_SUCC(ret)) { read_rows_ += count; }
    else { batch_indices_.reuse(); }
    return ret;
  }
  int check_cancel() const
  {
    int ret = ATOMIC_LOAD(cancel_);
    if (OB_SUCC(ret)) { ret = share::dag_yield(); }
    return ret;
  }
  static int64_t find_index(const share::ObVectorArray<int64_t> &values, int64_t idx)
  {
    int64_t lo = 0, hi = values.count();
    while (lo < hi) {
      const int64_t mid = lo + (hi - lo) / 2;
      if (values.at(mid) < idx) { lo = mid + 1; } else { hi = mid; }
    }
    return lo < values.count() && values.at(lo) == idx ? lo : -1;
  }
  ObIAllocator &allocator_;
  int64_t count_;
  int64_t dim_;
  const volatile int *cancel_;
  bool ready_;
  int64_t memory_limit_;
  int64_t shard_count_;
  int64_t epochs_;
  int64_t batch_size_;
  int64_t batches_per_epoch_;
  int64_t rows_per_epoch_;
  mutable int64_t next_batch_;
  mutable int64_t read_rows_;
  const ObIVFVectorStore *resident_source_;
  ObIVFVectorIDView source_vids_;
  share::ObVectorArray<int64_t> indices_;
  ObArray<uint8_t> loaded_;
  share::ObVectorArray<int64_t> ranks_;
  ObArray<Shard *, ObWrapperAllocator> shards_;
  mutable share::ObVectorArray<int64_t> batch_indices_;
  mutable share::ObVectorMatrix batch_values_;
  mutable ObArray<uint8_t> batch_loaded_;
};
} // namespace ddl_hca

struct ObIVFDDLRefineSession
{
  ObIVFDDLRefineSession(ObIAllocator &allocator, int64_t n, int64_t dim,
      const share::ObVectorMatrix &centers, const share::ObVectorArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *graph, const volatile int *cancel)
    : cache_(allocator, n, dim, cancel), input_(centers, dim), assignments_(assignments),
        working_values_(share::vector_array_allocator(allocator)), working_(working_values_, dim, &working_values_),
        graph_(graph == nullptr ? ObIVFNeighborGraphView() : *graph), neighbors_(graph_), trainer_(allocator), rows_(0),
        batch_prepared_(false), phase_(ObIVFRefinePhase::DISTANCE)
  {
    const int rc = working_values_.set_dimension(dim);
    OB_ASSERT(rc == OB_SUCCESS);
  }
  ddl_hca::SampleCache cache_;
  ddl_hca::Centers input_;
  ddl_hca::Assignments<share::ObVectorArray<int64_t>> assignments_;
  share::ObVectorMatrix working_values_;
  ddl_hca::Centers working_;
  ObIVFNeighborGraphView graph_;
  ddl_hca::Neighborhood neighbors_;
  share::ObHcaRefineTrainer trainer_;
  int64_t rows_;
  bool batch_prepared_;
  ObIVFRefinePhase phase_;
};

ObIVFDDLHcaAdapter::ObIVFDDLHcaAdapter()
  : allocator_(ObMemAttr(MTL_ID(), "IVFDDLRefine", ObCtxIds::VECTOR_CTX_ID)),
    session_(nullptr), cancel_code_(OB_SUCCESS)
{}
ObIVFDDLHcaAdapter::~ObIVFDDLHcaAdapter() { release_samples(); }
void ObIVFDDLHcaAdapter::release_samples()
{
  if (session_ != nullptr) {
    LOG_INFO("[IVF_BUILD_PERF]", "event", "refine_samples_release",
        "read_rows", session_->cache_.read_rows(), "file_size", session_->cache_.file_size());
    session_->~ObIVFDDLRefineSession();
    session_ = nullptr;
  }
  allocator_.reset();
}
void ObIVFDDLHcaAdapter::cancel(int ret_code)
{
  ATOMIC_STORE(&cancel_code_, ret_code == OB_SUCCESS ? OB_CANCELED : ret_code);
  ObIVFHcaAlgorithmAdapter::cancel(ret_code);
}
int ObIVFDDLHcaAdapter::neighborhood_add_centers(
    const share::ObVectorMatrix &centers, const int64_t center_count, const int64_t dimension,
    const int64_t cid_begin, const int64_t cid_end)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  if (OB_FAIL(ret)) {
  } else if (dimension <= 0 || centers.count() % dimension != 0
      || centers.count() / dimension != center_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DDL neighborhood center matrix", K(ret), K(center_count), K(dimension));
  } else {
    ddl_hca::Centers view(centers, dimension);
    ret = neighborhood_add_center_view(view, cid_begin, cid_end);
  }
  return ret;
}

int ObIVFDDLHcaAdapter::neighborhood_find_neighbors(
    const ObIVFNeighborhoodMode mode, const share::ObVectorMatrix &centers,
    const int64_t center_count, const int64_t dimension,
    const int64_t cid_begin, const int64_t cid_end,
    ObIArray<int64_t> &offsets, ObIArray<int64_t> &cids, ObIArray<float> &distances)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  offsets.reset();
  cids.reset();
  distances.reset();
  if (OB_FAIL(ret)) {
  } else if (dimension <= 0 || centers.count() % dimension != 0
      || centers.count() / dimension != center_count
      || cid_begin < 0 || cid_end < cid_begin || cid_end > center_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DDL neighborhood row range", K(ret), K(center_count), K(dimension), K(cid_begin), K(cid_end));
  } else if (OB_FAIL(offsets.push_back(0))) {
    LOG_WARN("failed to initialize DDL neighborhood offsets", K(ret));
  } else {
    ddl_hca::Centers view(centers, dimension);
    ddl_hca::NeighborRows rows(center_count, cid_begin, cid_end, offsets, cids, distances);
    ret = neighborhood_build_row_view(mode, view, cid_begin, cid_end, rows);
  }
  if (OB_FAIL(ret)) { offsets.reset(); cids.reset(); distances.reset(); }
  return ret;
}

int ObIVFDDLHcaAdapter::begin_refine_samples(int64_t n, int64_t dim,
    const share::ObVectorMatrix &centers, const share::ObVectorArray<int64_t> &assignments,
    const ObIVFNeighborGraphView *graph, int64_t memory_limit, int64_t shards,
    const ObIVFVectorStore *source, ObIVFVectorIDView source_vids)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  if (OB_FAIL(ret)) {
  } else if (session_ != nullptr || !is_initialized() || dim <= 0 || centers.empty()
      || centers.count() % dim != 0 || assignments.count() != n
      || (source == nullptr) != source_vids.empty()
      || (source != nullptr && (!source->is_valid() || source->get_vector_count() != n
          || source->get_dimension() != dim || source_vids.count() != n))
      || (graph != nullptr && (!graph->is_valid() || graph->center_count_ != centers.count() / dim))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DDL refinement sample state", K(ret), KP_(session), K(n), K(dim), "initialized", is_initialized(),
             "center_values", centers.count(), "assignments", assignments.count(), KP(graph));
  } else {
    void *buf = allocator_.alloc(sizeof(ObIVFDDLRefineSession));
    if (buf == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate DDL refinement session", K(ret), K(n), K(dim));
    } else {
      session_ = new (buf) ObIVFDDLRefineSession(allocator_, n, dim, centers, assignments, graph, &cancel_code_);
      auto &s = *session_;
      const share::ObVectorArray<int64_t> *samples = nullptr;
      int64_t count = 0;
      if (OB_FAIL(s.working_values_.resize(centers.count()))) {
        LOG_WARN("failed to allocate DDL refinement centers", K(ret), "values", centers.count());
      } else if (OB_FAIL(s.trainer_.init(s.cache_, s.input_, s.assignments_,
          graph == nullptr ? nullptr : &s.neighbors_, parameters().refine_param_,
          parameters().center_norm_info_, s.working_))) {
        LOG_WARN("failed to initialize DDL refinement trainer", K(ret), K(n), K(dim));
      } else if (source != nullptr && source->is_fully_memory_resident()) {
        if (OB_FAIL(s.cache_.init_resident(*source, source_vids))) {
          LOG_WARN("failed to bind resident IVF refinement source", K(ret), K(n), K(dim));
        }
      } else if (OB_FAIL(s.trainer_.get_sample_indices(samples, count))) {
        LOG_WARN("failed to get DDL refinement sample indices", K(ret), K(n), K(dim));
      } else if (OB_FAIL(s.cache_.init(*samples, count, memory_limit, shards,
          parameters().refine_param_.max_iterations_, s.trainer_))) {
        LOG_WARN("failed to initialize DDL refinement sample cache", K(ret), K(count), K(n), K(dim));
      }
    }
    if (OB_FAIL(ret)) { release_samples(); }
  }
  return ret;
}
bool ObIVFDDLHcaAdapter::uses_resident_refine_source() const
{ return session_ != nullptr && session_->cache_.uses_resident_source(); }
int64_t ObIVFDDLHcaAdapter::refine_sample_shards() const
{ return session_ == nullptr ? 0 : session_->cache_.shard_count(); }
int64_t ObIVFDDLHcaAdapter::refine_sample_memory_hold() const
{ return session_ == nullptr ? 0 : session_->cache_.memory_hold(); }
int64_t ObIVFDDLHcaAdapter::refine_sample_read_rows() const
{ return session_ == nullptr ? 0 : session_->cache_.read_rows(); }
int64_t ObIVFDDLHcaAdapter::refine_sample_file_size() const
{ return session_ == nullptr ? 0 : session_->cache_.file_size(); }
int ObIVFDDLHcaAdapter::begin_refine_sample_shard(int64_t shard)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  if (OB_SUCC(ret) && session_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("DDL refinement session is not initialized", K(ret), K(shard));
  } else if (OB_SUCC(ret)) { ret = session_->cache_.begin_shard(shard); }
  return ret;
}
int ObIVFDDLHcaAdapter::finish_refine_sample_shard(int64_t shard)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  if (OB_SUCC(ret) && session_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("DDL refinement session is not initialized", K(ret), K(shard));
  } else if (OB_SUCC(ret)) { ret = session_->cache_.finish_shard(shard); }
  return ret;
}
const share::ObVectorArray<int64_t> *ObIVFDDLHcaAdapter::refine_sample_indices() const
{ return session_ == nullptr ? nullptr : &session_->cache_.indices(); }
int ObIVFDDLHcaAdapter::load_refine_samples(const ObIArray<int64_t> &positions,
    const share::ObVectorMatrix &vectors, int64_t shard)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  if (OB_FAIL(ret)) {
  } else if (session_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("DDL refinement session is not initialized", K(ret));
  } else if (OB_FAIL(session_->cache_.load(positions, vectors, shard))) {
    LOG_WARN("failed to load DDL refinement samples", K(ret), "positions", positions.count(), "values",
             vectors.count());
  }
  return ret;
}
int ObIVFDDLHcaAdapter::finish_refine_samples()
{
  int ret = OB_SUCCESS;
  if (session_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("DDL refinement session is not initialized", K(ret));
  } else if (OB_FAIL(session_->cache_.seal())) {
    LOG_WARN("failed to seal DDL refinement samples", K(ret));
  }
  return ret;
}
int ObIVFDDLHcaAdapter::refine_train_prepare(ObIVFVectorReader &reader,
    const ObIVFVectorIDView &vids, const share::ObVectorMatrix &centers,
    const share::ObVectorArray<int64_t> &assignments, const ObIVFNeighborGraphView *graph,
    int64_t k, int64_t &batch_size)
{
  UNUSEDx(reader, vids, centers, assignments, graph, k);
  int ret = ATOMIC_LOAD(&cancel_code_);
  batch_size = 0;
  if (OB_FAIL(ret)) {
  } else if (session_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot prepare DDL refinement batch without session", K(ret));
  } else {
    auto &s = *session_;
    if (!s.batch_prepared_ && OB_FAIL(s.trainer_.prepare_batch(s.rows_))) {
      LOG_WARN("failed to prepare cached IVF refinement batch", K(ret));
    } else { s.batch_prepared_ = true; batch_size = s.rows_; }
  }
  return ret;
}
int ObIVFDDLHcaAdapter::refine_train_distances(int64_t begin, int64_t end)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  if (OB_FAIL(ret)) {
  } else if (session_ == nullptr) {
    ret = OB_NOT_INIT;
    LOG_WARN("cannot compute DDL refinement distances without session", K(ret), K(begin), K(end));
  } else if (OB_FAIL(session_->trainer_.distance_range(begin, end))) {
    LOG_WARN("failed to compute DDL refinement distance range", K(ret), K(begin), K(end));
  }
  return ret;
}
int ObIVFDDLHcaAdapter::refine_train_finish_batch(share::ObVectorMatrix &centers, bool &finished)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  finished = false;
  if (OB_FAIL(ret)) {
  } else if (session_ == nullptr || !session_->batch_prepared_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("DDL refinement batch is not prepared", K(ret), KP_(session));
  } else {
    auto &s = *session_;
    if (OB_FAIL(s.trainer_.finish_batch())) {
      LOG_WARN("failed to finish cached DDL refinement batch", K(ret), K(s.rows_));
    } else {
      s.batch_prepared_ = false;
      if (OB_FAIL(s.trainer_.prepare_batch(s.rows_))) {
        if (ret == OB_ITER_END) {
          if (OB_FAIL(centers.assign(s.working_values_))) {
            LOG_WARN("failed to copy DDL refinement centers", K(ret), "values", s.working_values_.count());
          } else {
            finished = true;
            release_samples();
          }
        } else {
          LOG_WARN("failed to prepare next DDL refinement batch", K(ret), K(s.rows_));
        }
      } else { s.batch_prepared_ = true; }
    }
  }
  return ret;
}

ObIVFRefinePhase ObIVFDDLHcaAdapter::refine_train_phase() const
{ return session_ == nullptr ? ObIVFRefinePhase::DISTANCE : session_->phase_; }

int ObIVFDDLHcaAdapter::prepare_refine_phase(int64_t &count)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  count = 0;
  if (OB_FAIL(ret)) {
  } else if (session_ == nullptr) {
    ret = OB_NOT_INIT;
  } else {
    auto &s = *session_;
    if (!s.batch_prepared_ && OB_FAIL(s.trainer_.prepare_batch(s.rows_))) {
    } else {
      s.batch_prepared_ = true;
      count = s.phase_ == ObIVFRefinePhase::UPDATE ? s.working_.count() : s.rows_;
    }
  }
  if (OB_FAIL(ret)) { LOG_WARN("failed to prepare DDL refinement phase", K(ret), KP_(session)); }
  return ret;
}

int ObIVFDDLHcaAdapter::execute_refine_phase(int64_t begin, int64_t end)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  if (OB_FAIL(ret)) {
  } else if (session_ == nullptr || !session_->batch_prepared_) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    auto &s = *session_;
    ret = s.phase_ == ObIVFRefinePhase::DISTANCE ? s.trainer_.distance_range(begin, end)
        : s.phase_ == ObIVFRefinePhase::ASSIGN ? s.trainer_.assign_range(begin, end)
        : s.trainer_.update_range(begin, end);
  }
  if (OB_FAIL(ret)) { LOG_WARN("failed to execute DDL refinement phase", K(ret), K(begin), K(end)); }
  return ret;
}

int ObIVFDDLHcaAdapter::finish_refine_phase(share::ObVectorMatrix &centers, bool &finished)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  finished = false;
  if (OB_FAIL(ret)) {
  } else if (session_ == nullptr || !session_->batch_prepared_) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    auto &s = *session_;
    if (s.phase_ == ObIVFRefinePhase::DISTANCE) {
      if (OB_SUCC(ret = s.trainer_.prepare_assign())) { s.phase_ = ObIVFRefinePhase::ASSIGN; }
    } else if (s.phase_ == ObIVFRefinePhase::ASSIGN) {
      if (OB_SUCC(ret = s.trainer_.prepare_update())) { s.phase_ = ObIVFRefinePhase::UPDATE; }
    } else if (OB_FAIL(s.trainer_.finish_update())) {
    } else {
      s.batch_prepared_ = false;
      s.phase_ = ObIVFRefinePhase::DISTANCE;
      if (OB_FAIL(s.trainer_.prepare_batch(s.rows_))) {
        if (ret == OB_ITER_END) {
          if (OB_SUCC(ret = centers.set_dimension(s.input_.dimension()))
              && OB_SUCC(ret = centers.assign(s.working_values_))) {
            finished = true;
            release_samples();
          }
        }
      } else { s.batch_prepared_ = true; }
    }
  }
  if (OB_FAIL(ret)) { LOG_WARN("failed to finish DDL refinement phase", K(ret), KP_(session)); }
  return ret;
}

int ObIVFDDLHcaAdapter::refine_assign_loaded(const share::ObVectorMatrix &values,
    const share::ObVectorArray<int64_t> &indices, int64_t dim, const share::ObVectorMatrix &centers,
    const share::ObVectorArray<int64_t> &old_assignments, const ObIVFNeighborGraphView *graph,
    ObIArray<int64_t> &output)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  output.reuse();
  if (OB_FAIL(ret)) {
  } else if (!is_initialized() || dim <= 0 || indices.empty() || indices.count() > INT64_MAX / dim
      || values.count() != indices.count() * dim || centers.empty() || centers.count() % dim != 0
      || (graph != nullptr && (!graph->is_valid() || graph->center_count_ != centers.count() / dim))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid loaded DDL refinement input", K(ret), K(dim), "indices", indices.count(), "values",
             values.count(), "centers", centers.count(), KP(graph));
  } else if (OB_FAIL(output.prepare_allocate(indices.count()))) {
    LOG_WARN("failed to allocate DDL refinement assignments", K(ret), "count", indices.count());
  } else {
    ddl_hca::Batch batch(values, dim);
    ddl_hca::Centers center_view(centers, dim);
    ddl_hca::Assignments input(old_assignments, &indices);
    ddl_hca::Assignments result(output, nullptr, &output);
    ddl_hca::Members members(center_view.count());
    ddl_hca::Changed changed(center_view.count());
    ddl_hca::Neighborhood neighbors(graph == nullptr ? ObIVFNeighborGraphView() : *graph);
    if (OB_FAIL(share::hca_refine_assign_range(batch, center_view, input, graph == nullptr ? nullptr : &neighbors,
                                               result, members, changed))) {
      LOG_WARN("failed to assign loaded DDL refinement vectors", K(ret), K(dim), "count", indices.count());
    }
  }
  return ret;
}

int ObIVFDDLHcaAdapter::soar_assign_loaded(const share::ObVectorMatrix &values, int64_t dim,
    const share::ObVectorMatrix &centers, const common::ObIArray<int64_t> &primary,
    const ObIVFNeighborGraphView *graph, ObIArray<int64_t> &secondary)
{
  int ret = ATOMIC_LOAD(&cancel_code_);
  secondary.reuse();
  if (OB_FAIL(ret)) {
  } else if (!is_initialized() || dim <= 0 || primary.empty() || primary.count() > INT64_MAX / dim
      || values.count() != primary.count() * dim || centers.empty() || centers.count() % dim != 0
      || (graph != nullptr && (!graph->is_valid() || graph->center_count_ != centers.count() / dim))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid loaded DDL SOAR input", K(ret), K(dim), "primary", primary.count(), "values", values.count(),
             "centers", centers.count(), KP(graph));
  } else if (OB_FAIL(secondary.prepare_allocate(primary.count()))) {
    LOG_WARN("failed to allocate DDL SOAR secondary assignments", K(ret), "count", primary.count());
  } else {

    common::ObArray<int64_t> primary_output(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(ObMemAttr(MTL_ID(), "IVFDDLBatch", ObCtxIds::VECTOR_CTX_ID)));
    if (OB_FAIL(primary_output.prepare_allocate(primary.count()))) {
      LOG_WARN("failed to allocate DDL SOAR primary assignments", K(ret), "count", primary.count());
    } else {
      ddl_hca::Batch batch(values, dim);
      ddl_hca::Centers center_view(centers, dim);
      ddl_hca::Assignments input(primary);
      ddl_hca::Assignments a1(primary_output, nullptr, &primary_output);
      ddl_hca::Assignments a2(secondary, nullptr, &secondary);
      ddl_hca::Members members(center_view.count());
      ddl_hca::Remap identity(center_view.count());
      ddl_hca::Neighborhood neighbors(graph == nullptr ? ObIVFNeighborGraphView() : *graph);
      ObArenaAllocator scratch(ObMemAttr(MTL_ID(), "IVFDDLSoar", ObCtxIds::VECTOR_CTX_ID));
      if (OB_FAIL(share::hca_assign_soar_range(batch, center_view, input, identity,
                                               graph == nullptr ? nullptr : &neighbors, parameters().soar_param_, a1,
                                               &a2, &members, scratch))) {
        LOG_WARN("failed to assign loaded DDL SOAR vectors", K(ret), K(dim), "count", primary.count());
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
