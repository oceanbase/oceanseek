/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_hca_algorithm_adapter.h"

#include "lib/alloc/alloc_func.h"
#include "lib/allocator/page_arena.h"
#include "lib/time/ob_time_utility.h"
#include "lib/vector/ob_vector_util.h"
#include "share/vector_index/hca/ob_vector_hca_cluster.h"
#include "share/vector_index/hca/ob_vector_hca_simd.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/vector_index/hca/ob_vector_hca_neighborhood.h"
#include "share/vector_index/hca/ob_vector_hca_refine.h"
#include "share/vector_index/hca/ob_vector_hca_soar.h"
#include "share/vector_type/ob_vector_common_util.h"

#include <atomic>
#include <cmath>
#include <new>

namespace oceanbase
{
using namespace common;
namespace storage
{

namespace
{

class HcaVectorCache
{
public:
  HcaVectorCache(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &vids)
    : reader_(reader), vids_(vids), dimension_(reader.get_dimension()),
      prefetched_begin_(-1), prefetched_end_(-1),
      allocator_(ObMemAttr(MTL_ID(), "IVFHcaVecCache", ObCtxIds::VECTOR_CTX_ID)),
      prefetched_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(allocator_)),
      batch_buffer_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(allocator_)),
      cache_(), cache_initialized_(false), vector_reads_(0), batch_reads_(0), single_reads_(0), loaded_vectors_(0),
      batch_read_us_(0)
  {
    cache_.set_block_allocator(ObWrapperAllocator(allocator_));
  }

  int64_t count() const { return vids_.count(); }
  int64_t dimension() const { return dimension_; }

  void reset_cached_vectors()
  {
    // Arrays must drop their arena-backed capacity before the arena is reset.
    // Keep vector_reads_ so streaming windows retain the periodic yield check.
    prefetched_.reset();
    batch_buffer_.reset();
    cache_.reset();
    allocator_.reset();
    prefetched_begin_ = -1;
    prefetched_end_ = -1;
    cache_initialized_ = false;
  }

  int prefetch(const int64_t begin, const int64_t end)
  {
    int ret = OB_SUCCESS;
    prefetched_begin_ = -1;
    prefetched_end_ = -1;
    prefetched_.reuse();
    // The vector store owns output allocation. Reserving here would be discarded
    // by its reuse/reset, while the arena would retain the unused allocation.
    if (begin < 0 || end <= begin || end > vids_.count() || dimension_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF HCA vector prefetch range", K(ret), K(begin), K(end),
          "vid_count", vids_.count(), K_(dimension));
    } else if (end - begin > INT64_MAX / dimension_ / static_cast<int64_t>(sizeof(float))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF HCA prefetch size overflows", K(ret), K(begin), K(end), K_(dimension));
    } else if (OB_FAIL(reader_.batch_get_by_vid(
        vids_, begin, end, prefetched_))) {
      LOG_WARN("failed to prefetch IVF HCA vectors", K(ret), K(begin), K(end));
    } else if (prefetched_.count() != (end - begin) * dimension_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected IVF HCA prefetch vector count", K(ret), K(begin),
          K(end), K_(dimension), "value_count", prefetched_.count());
    }
    if (OB_SUCC(ret)) {
      prefetched_begin_ = begin;
      prefetched_end_ = end;
    }
    return ret;
  }

  int prefetch_indices(const int64_t *indices, const int64_t count)
  {
    int ret = OB_SUCCESS;
    const int64_t max_batch = 4096;
    ObSEArray<ObIVFVectorVID, 64> batch_vids;
    batch_vids.set_attr(ObMemAttr(MTL_ID(), "IVFHcaVecCache", ObCtxIds::VECTOR_CTX_ID));
    ObSEArray<int64_t, 64> positions;
    positions.set_attr(ObMemAttr(MTL_ID(), "IVFHcaVecCache", ObCtxIds::VECTOR_CTX_ID));
    if (count < 0 || (count > 0 && nullptr == indices) || dimension_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF refine prefetch arguments", K(ret), KP(indices), K(count), K_(dimension));
    } else if (count > 0 && OB_FAIL(init_cache_())) {
      LOG_WARN("failed to initialize IVF refine vector cache", K(ret));
    }
    for (int64_t begin = 0; OB_SUCC(ret) && begin < count;) {
      batch_vids.reuse();
      positions.reuse();
      int64_t end = begin;
      for (; OB_SUCC(ret) && end < count && positions.count() < max_batch; ++end) {
        const int64_t pos = indices[end];
        if (pos < 0 || pos >= vids_.count() || (end > 0 && pos < indices[end - 1])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid sorted IVF refine prefetch index", K(ret), K(pos), K(end));
        } else if ((pos >= prefetched_begin_ && pos < prefetched_end_)
            || nullptr != cache_.at(pos)
            || (!positions.empty() && positions.at(positions.count() - 1) == pos)) {
          // Repeated samples and later epochs reuse the immutable cached value.
        } else if (OB_FAIL(batch_vids.push_back(vids_.at(pos)))) {
          LOG_WARN("failed to collect IVF refine VIDs", K(ret));
        } else if (OB_FAIL(positions.push_back(pos))) {
          LOG_WARN("failed to collect IVF refine positions", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (positions.empty()) {
      } else if (positions.count() > INT64_MAX / dimension_ / sizeof(float)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("IVF vector buffer size overflow", K(ret), K(positions.count()), K(dimension_));
      } else {
        ++batch_reads_;
        const int64_t read_start = ObTimeUtility::current_time();
        const int read_ret = reader_.batch_get_by_vid(batch_vids, 0, batch_vids.count(), batch_buffer_);
        batch_read_us_ += ObTimeUtility::current_time() - read_start;
        if (OB_FAIL(read_ret)) {
          LOG_WARN("failed to batch read IVF refine samples", K(ret), "count", batch_vids.count());
        } else {
          const int64_t bytes = positions.count() * dimension_ * sizeof(float);
          float *owned = static_cast<float *>(allocator_.alloc_align(bytes, alignof(float)));
          if (nullptr == owned) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate IVF vector buffer", K(ret), K(bytes), K(positions.count()),
                K(dimension_));
          } else {
            MEMCPY(owned, batch_buffer_.get_data(), bytes);
            for (int64_t i = 0; i < positions.count(); ++i) {
              cache_.at(positions.at(i)) = owned + i * dimension_;
            }
            loaded_vectors_ += positions.count();
          }
        }
      }
      begin = end;
      if (OB_SUCC(ret) && OB_FAIL(share::dag_yield())) {
        LOG_WARN("IVF refine prefetch interrupted", K(ret));
      }
    }
    return ret;
  }

  int64_t batch_reads() const { return batch_reads_; }
  int64_t single_reads() const { return single_reads_; }
  int64_t loaded_vectors() const { return loaded_vectors_; }
  int64_t batch_read_us() const { return batch_read_us_; }
  int64_t memory_hold() const { return allocator_.total(); }

  int get_vector(const int64_t source_pos, const float *&vector) const
  {
    int ret = OB_SUCCESS;
    vector = nullptr;
    if (++vector_reads_ % 4096 == 0 && OB_FAIL(share::dag_yield())) {
      LOG_WARN("IVF algorithm input read interrupted", K(ret));
    } else if (source_pos < 0 || source_pos >= vids_.count() || dimension_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF HCA vector position", K(ret), K(source_pos),
          "vid_count", vids_.count(), K_(dimension));
    } else if (source_pos >= prefetched_begin_
        && source_pos < prefetched_end_) {
      vector = &prefetched_.at(0)
          + (source_pos - prefetched_begin_) * dimension_;
    } else {
      if (OB_FAIL(init_cache_())) {
        LOG_WARN("failed to initialize IVF HCA vector cache", K(ret),
            "vid_count", vids_.count());
      } else if (OB_NOT_NULL(cache_.at(source_pos))) {
        vector = cache_.at(source_pos);
      } else {
        ObSEArray<float, 16> values;
        values.set_attr(ObMemAttr(MTL_ID(), "IVFHcaVecCache", ObCtxIds::VECTOR_CTX_ID));
        float *owned = static_cast<float *>(allocator_.alloc_align(
            dimension_ * sizeof(float), alignof(float)));
        if (OB_ISNULL(owned)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate IVF HCA vector cache entry", K(ret),
              K(source_pos), K_(dimension));
        } else if (OB_FAIL(values.reserve(dimension_))) {
          LOG_WARN("failed to reserve IVF HCA vector buffer", K(ret),
              K(source_pos), K_(dimension));
        } else if (FALSE_IT(++single_reads_)) {
        } else if (OB_FAIL(reader_.batch_get_by_vid(
            vids_, source_pos, source_pos + 1, values))) {
          LOG_WARN("failed to read IVF HCA vector", K(ret), K(source_pos));
        } else if (values.count() != dimension_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected IVF HCA vector dimension", K(ret), K(source_pos),
              K_(dimension), "value_count", values.count());
        } else {
          for (int64_t d = 0; d < dimension_; ++d) {
            owned[d] = values.at(d);
          }
          vector = owned;
          cache_.at(source_pos) = owned;
          ++loaded_vectors_;
        }
      }
    }
    return ret;
  }

private:
  int init_cache_() const
  {
    int ret = OB_SUCCESS;
    if (!cache_initialized_) {
      if (OB_FAIL(cache_.prepare_allocate(vids_.count()))) {
        LOG_WARN("failed to allocate IVF HCA vector-cache index", K(ret),
            "vid_count", vids_.count());
      } else {
        for (int64_t i = 0; i < cache_.count(); ++i) {
          cache_.at(i) = nullptr;
        }
        cache_initialized_ = true;
      }
    }
    return ret;
  }

  ObIVFVectorReader &reader_;
  const common::ObIArray<ObIVFVectorVID> &vids_;
  int64_t dimension_;
  int64_t prefetched_begin_;
  int64_t prefetched_end_;
  mutable ObArenaAllocator allocator_;
  ObSEArray<float, 16, ObWrapperAllocator> prefetched_;
  ObSEArray<float, 16, ObWrapperAllocator> batch_buffer_;
  mutable ObArray<float *, ObWrapperAllocator> cache_;
  mutable bool cache_initialized_;
  mutable uint64_t vector_reads_;
  mutable int64_t batch_reads_;
  mutable int64_t single_reads_;
  mutable int64_t loaded_vectors_;
  mutable int64_t batch_read_us_;
};

class HcaCapturedSampleBatch final : public share::ObHcaVectorBatch
{
public:
  explicit HcaCapturedSampleBatch(ObIAllocator &allocator)
    : dimension_(0), vector_reads_(0), values_()
  { values_.set_block_allocator(ObWrapperAllocator(allocator)); }

  int init(const ObIVFFirstLevelSampleSet &samples, const int64_t vector_count,
      const int64_t dimension, const int64_t sample_limit, const uint64_t seed,
      ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    const int64_t available = samples.get_sample_count();
    const int64_t count = MIN(available, sample_limit);
    ObArray<int64_t, ObWrapperAllocator> ordinals;
    ordinals.set_block_allocator(ObWrapperAllocator(allocator));
    if (!samples.is_valid() || dimension <= 0 || samples.get_dimension() != dimension
        || available <= 0 || available > vector_count || count <= 0
        || count > INT64_MAX / dimension) {
      LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation", "init", K(dimension),
          "samples_get_dimension", samples.get_dimension(), K(available), K(vector_count), K(count), K(sample_limit),
          K(seed));
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(ordinals.prepare_allocate(count))) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "ordinals.prepare_allocate");
    } else if (OB_FAIL(share::hca_sample_ordinals(available, count, seed, &ordinals.at(0)))) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "share::hca_sample_ordinals");
    } else if (OB_FAIL(values_.reserve(count * dimension))) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "values_.reserve");
    } else {
      dimension_ = dimension;
      ObArray<ObIVFVectorVID, ObWrapperAllocator> vids;
      ObSEArray<float, 16, ObWrapperAllocator> chunk(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(allocator));
      vids.set_block_allocator(ObWrapperAllocator(allocator));
      for (int64_t i = 0; OB_SUCC(ret) && i < count;) {
        const int64_t begin = ordinals.at(i);
        const int64_t end = begin + MIN(int64_t(256), available - begin);
        vids.reuse();
        chunk.reuse();
        if (end - begin > INT64_MAX / dimension) {
          LOG_WARN("IVF merge size overflow", "ret", OB_SIZE_OVERFLOW, "operation", "init", K(end), K(begin),
              K(dimension), K(vector_count), K(sample_limit), K(seed));
          ret = OB_SIZE_OVERFLOW;
        } else if (OB_FAIL(share::dag_yield())) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "share::dag_yield");
        } else if (OB_FAIL(samples.batch_get_samples(begin, end, vids, chunk))) {
          LOG_WARN("IVF merge operation failed", K(ret), "operation", "samples.batch_get_samples");
        } else if (vids.count() != end - begin || chunk.count() != (end - begin) * dimension) {
          LOG_WARN("invalid IVF merge data", "ret", OB_INVALID_DATA, "operation", "init", "vids_count", vids.count(),
              K(end), K(begin), "chunk_count", chunk.count(), K(dimension), K(vector_count), K(sample_limit), K(seed));
          ret = OB_INVALID_DATA;
        } else {
          for (; OB_SUCC(ret) && i < count && ordinals.at(i) < end; ++i) {
            const int64_t offset = (ordinals.at(i) - begin) * dimension;
            for (int64_t d = 0; OB_SUCC(ret) && d < dimension; ++d) {
              const float value = chunk.at(offset + d);
              if (!std::isfinite(value)) {
                LOG_WARN("invalid IVF merge data", "ret", OB_INVALID_DATA, "operation", "init", K(value),
                    K(vector_count), K(dimension), K(sample_limit), K(seed));
                ret = OB_INVALID_DATA;
              }
              else {
                ret = values_.push_back(value);
                if (OB_FAIL(ret)) {
                  LOG_WARN("IVF merge operation failed", K(ret), "operation", "values_.push_back", K(vector_count),
                      K(dimension), K(sample_limit), K(seed));
                }
              }
            }
          }
        }
      }
    }
    return ret;
  }
  int64_t count() const override { return dimension_ <= 0 ? 0 : values_.count() / dimension_; }
  int64_t dimension() const override { return dimension_; }
  int get_vector(const int64_t pos, int64_t &vector_idx, const float *&vector) const override
  {
    int ret = OB_SUCCESS;
    vector_idx = -1;
    vector = nullptr;
    if (pos < 0 || pos >= count()) {
      LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation", "get_vector", K(pos));
      ret = OB_INVALID_ARGUMENT;
    }
    else if (++vector_reads_ % 4096 == 0 && OB_FAIL(share::dag_yield())) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "share::dag_yield");
    }
    else { vector_idx = pos; vector = &values_.at(pos * dimension_); }
    return ret;
  }
private:
  int64_t dimension_;
  mutable int64_t vector_reads_;
  ObArray<float, ObWrapperAllocator> values_;
};

class HcaVectorAccessor final : public share::ObHcaVectorAccessor
{
public:
  explicit HcaVectorAccessor(HcaVectorCache &cache) : cache_(cache) {}
  virtual int64_t count() const override { return cache_.count(); }
  virtual int64_t dimension() const override { return cache_.dimension(); }
  virtual int get_vector(
      const int64_t vector_idx,
      const float *&vector) const override
  {
    return cache_.get_vector(vector_idx, vector);
  }
  virtual int prefetch_vectors(const int64_t *indices, const int64_t count) const override
  {
    return cache_.prefetch_indices(indices, count);
  }

private:
  HcaVectorCache &cache_;
};

// Center-stat reduction consumes each vector before requesting the next one.
// Its member order can therefore be preserved without retaining the full payload.
class HcaStreamingStatsAccessor final : public share::ObHcaVectorAccessor
{
public:
  HcaStreamingStatsAccessor(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &vids)
    : cache_(reader, vids), prefetched_begin_(-1), prefetched_end_(-1)
  {}
  virtual int64_t count() const override { return cache_.count(); }
  virtual int64_t dimension() const override { return cache_.dimension(); }
  virtual int get_vector(
      const int64_t vector_idx,
      const float *&vector) const override
  {
    int ret = OB_SUCCESS;
    vector = nullptr;
    const int64_t max_batch_rows = 256;
    const int64_t max_batch_bytes = 1024 * 1024;
    const int64_t vector_dimension = dimension();
    if (vector_idx < 0 || vector_idx >= count() || vector_dimension <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF HCA streaming vector position", K(ret), K(vector_idx),
          K(vector_dimension), "vector_count", count());
    } else if (vector_dimension > INT64_MAX / static_cast<int64_t>(sizeof(float))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF HCA streaming vector size overflows", K(ret), K(vector_dimension));
    } else {
      if (vector_idx < prefetched_begin_ || vector_idx >= prefetched_end_) {
        const int64_t vector_bytes = vector_dimension * static_cast<int64_t>(sizeof(float));
        const int64_t batch_rows = MIN(max_batch_rows,
            MAX(int64_t(1), max_batch_bytes / vector_bytes));
        const int64_t end = vector_idx + MIN(batch_rows, count() - vector_idx);
        prefetched_begin_ = -1;
        prefetched_end_ = -1;
        cache_.reset_cached_vectors();
        if (OB_FAIL(share::dag_yield())) {
          LOG_WARN("IVF HCA streaming center stats interrupted", K(ret), K(vector_idx));
        } else if (OB_FAIL(cache_.prefetch(vector_idx, end))) {
          LOG_WARN("failed to prefetch IVF HCA center-stat window", K(ret),
              K(vector_idx), K(end));
        } else {
          prefetched_begin_ = vector_idx;
          prefetched_end_ = end;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(cache_.get_vector(vector_idx, vector))) {
        LOG_WARN("failed to read prefetched IVF HCA center-stat vector", K(ret),
            K(vector_idx));
      }
    }
    return ret;
  }
private:
  mutable HcaVectorCache cache_;
  mutable int64_t prefetched_begin_;
  mutable int64_t prefetched_end_;
};

class HcaVectorBatch final : public share::ObHcaVectorBatch
{
public:
  HcaVectorBatch(
      HcaVectorCache &cache,
      const int64_t source_begin,
      const int64_t source_end,
      const int64_t reported_begin)
    : cache_(cache),
      source_begin_(source_begin),
      source_end_(source_end),
      reported_begin_(reported_begin)
  {}
  virtual int64_t count() const override { return source_end_ - source_begin_; }
  virtual int64_t dimension() const override { return cache_.dimension(); }
  virtual int get_vector(
      const int64_t pos,
      int64_t &vector_idx,
      const float *&vector) const override
  {
    int ret = OB_SUCCESS;
    vector_idx = -1;
    vector = nullptr;
    if (pos < 0 || pos >= count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF HCA batch position", K(ret), K(pos),
          "batch_count", count());
    } else if (OB_FAIL(cache_.get_vector(source_begin_ + pos, vector))) {
      LOG_WARN("failed to get IVF HCA batch vector", K(ret), K(pos),
          K_(source_begin));
    } else {
      vector_idx = reported_begin_ + pos;
    }
    return ret;
  }
private:
  HcaVectorCache &cache_;
  int64_t source_begin_;
  int64_t source_end_;
  int64_t reported_begin_;
};

// Borrow already loaded payload and centers. A group only owns row positions;
// changing the parent does not copy the vectors or its center matrix again.
class HcaLoadedBatch final : public share::ObHcaVectorBatch
{
public:
  HcaLoadedBatch(const ObIArray<float> &values, const ObIArray<int64_t> &positions,
      int64_t begin, int64_t end, int64_t dimension)
    : values_(values), positions_(positions), begin_(begin), end_(end), dimension_(dimension) {}
  int64_t count() const override { return end_ - begin_; }
  int64_t dimension() const override { return dimension_; }
  int get_vector(int64_t pos, int64_t &idx, const float *&vector) const override
  {
    int ret = OB_SUCCESS;
    idx = -1;
    vector = nullptr;
    if (pos < 0 || pos >= count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      idx = positions_.at(begin_ + pos);
      if (idx < 0 || idx >= values_.count() / dimension_) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        vector = &values_.at(idx * dimension_);
      }
    }
    return ret;
  }
private:
  const ObIArray<float> &values_;
  const ObIArray<int64_t> &positions_;
  int64_t begin_;
  int64_t end_;
  int64_t dimension_;
};

class HcaBorrowedCenters final : public share::ObHcaCenterStore
{
public:
  HcaBorrowedCenters(const ObIArray<float> &values, int64_t dimension)
    : values_(values), dimension_(dimension) {}
  int64_t count() const override { return values_.count() / dimension_; }
  int64_t capacity() const override { return count(); }
  int64_t dimension() const override { return dimension_; }
  int get_center(int64_t cid, const float *&center) const override
  {
    center = cid >= 0 && cid < count() ? &values_.at(cid * dimension_) : nullptr;
    return center != nullptr ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  }
  int get_mutable_center(int64_t cid, float *&center) override
  { UNUSED(cid); center = nullptr; return OB_NOT_SUPPORTED; }
  int set_center(int64_t cid, const float *center) override
  { UNUSEDx(cid, center); return OB_NOT_SUPPORTED; }
private:
  const ObIArray<float> &values_;
  int64_t dimension_;
};

// Assignment's numerical kernel emits both directions. The runtime builds
// member lists in their original order at the barrier, so retain only CIDs.
class HcaLoadedAssignments final : public share::ObHcaAssignmentStore,
                                   public share::ObHcaCidMemberStore
{
public:
  HcaLoadedAssignments(ObIArray<int64_t> &cids, int64_t k) : cids_(cids), k_(k) {}
  int64_t count() const override { return cids_.count(); }
  int64_t cid_count() const override { return k_; }
  int get_assignment(int64_t idx, int32_t &cid) const override
  {
    cid = idx >= 0 && idx < count() ? static_cast<int32_t>(cids_.at(idx)) : -1;
    return idx >= 0 && idx < count() ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  }
  int set_assignment(int64_t idx, int32_t cid) override
  {
    int ret = append_member(cid, idx);
    if (OB_SUCC(ret)) { cids_.at(idx) = cid; }
    return ret;
  }
  int append_member(int32_t cid, int64_t idx) override
  { return cid >= 0 && cid < k_ && idx >= 0 && idx < count() ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
  int64_t member_count(int64_t cid) const override { UNUSED(cid); return -1; }
  int get_member(int64_t cid, int64_t offset, int64_t &idx) const override
  { UNUSEDx(cid, offset); idx = -1; return OB_NOT_SUPPORTED; }
  int move_row(int64_t old_cid, int64_t new_cid) override
  { UNUSEDx(old_cid, new_cid); return OB_NOT_SUPPORTED; }
  int bind_row(int64_t cid, const share::ObHcaCidMemberStore &input, int64_t input_cid) override
  { UNUSEDx(cid, input, input_cid); return OB_NOT_SUPPORTED; }
private:
  ObIArray<int64_t> &cids_;
  int64_t k_;
};

class HcaCenterBuffer final : public share::ObHcaCenterStore
{
public:
  HcaCenterBuffer(const int64_t capacity, const int64_t dimension)
    : capacity_(capacity), dimension_(dimension), count_(0),
      allocator_(ObMemAttr(MTL_ID(), "IVFHcaCenter", ObCtxIds::VECTOR_CTX_ID)),
      values_(), init_ret_(OB_SUCCESS)
  {
    values_.set_block_allocator(ObWrapperAllocator(allocator_));
    if (capacity <= 0 || dimension <= 0) {
      init_ret_ = OB_INVALID_ARGUMENT;
    } else if (capacity > INT64_MAX / dimension) {
      init_ret_ = OB_SIZE_OVERFLOW;
    } else if (OB_SUCCESS != (init_ret_ =
        values_.prepare_allocate(capacity * dimension))) {
      LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA center buffer", K_(init_ret),
          K(capacity), K(dimension));
    } else {
      for (int64_t i = 0; i < values_.count(); ++i) {
        values_.at(i) = 0.0F;
      }
    }
  }

  virtual int64_t count() const override { return count_; }
  virtual int64_t capacity() const override { return capacity_; }
  virtual int64_t dimension() const override { return dimension_; }
  int get_init_ret() const { return init_ret_; }
  virtual int get_center(
      const int64_t cid,
      const float *&center) const override
  {
    int ret = OB_SUCCESS;
    center = nullptr;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= count_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      center = &values_.at(cid * dimension_);
    }
    return ret;
  }
  virtual int get_mutable_center(
      const int64_t cid,
      float *&center) override
  {
    int ret = OB_SUCCESS;
    center = nullptr;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= capacity_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      center = &values_.at(cid * dimension_);
    }
    return ret;
  }
  virtual int set_center(
      const int64_t cid,
    const float *center) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= capacity_ || OB_ISNULL(center)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      float *destination = &values_.at(cid * dimension_);
      if (destination != center) {
        MEMCPY(destination, center, dimension_ * sizeof(float));
      }
      count_ = MAX(count_, cid + 1);
    }
    return ret;
  }

private:
  int64_t capacity_;
  int64_t dimension_;
  int64_t count_;
  ObArenaAllocator allocator_;
  ObArray<float, ObWrapperAllocator> values_;
  int init_ret_;
};

int load_centers(
    const common::ObIArray<float> &values,
    const int64_t center_count,
    const int64_t dimension,
    HcaCenterBuffer &centers)
{
  int ret = OB_SUCCESS;
  if (center_count <= 0 || dimension <= 0
      || values.count() != center_count * dimension
      || centers.capacity() < center_count
      || centers.dimension() != dimension) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < center_count; ++cid) {
    ret = centers.set_center(cid, &values.at(cid * dimension));
  }
  return ret;
}

int append_centers(
    const HcaCenterBuffer &centers,
    common::ObIArray<float> &values)
{
  int ret = OB_SUCCESS;
  values.reset();
  for (int64_t cid = 0; OB_SUCC(ret) && cid < centers.count(); ++cid) {
    const float *center = nullptr;
    if (OB_FAIL(centers.get_center(cid, center))) {
      LOG_WARN("failed to get IVF HCA center", K(ret), K(cid));
    }
    for (int64_t d = 0; OB_SUCC(ret) && d < centers.dimension(); ++d) {
      ret = values.push_back(center[d]);
    }
  }
  return ret;
}

class HcaAssignmentBuffer final : public share::ObHcaAssignmentStore
{
public:
  explicit HcaAssignmentBuffer(const int64_t count)
    : allocator_(ObMemAttr(MTL_ID(), "IVFHcaAssign", ObCtxIds::VECTOR_CTX_ID)),
      assignments_(), init_ret_(OB_SUCCESS)
  {
    assignments_.set_block_allocator(ObWrapperAllocator(allocator_));
    if (count < 0) {
      init_ret_ = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (init_ret_ = assignments_.prepare_allocate(count))) {
      LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA assignment buffer", K_(init_ret),
          K(count));
    } else {
      for (int64_t i = 0; i < assignments_.count(); ++i) {
        assignments_.at(i) = -1;
      }
    }
  }
  virtual int64_t count() const override { return assignments_.count(); }
  int get_init_ret() const { return init_ret_; }
  virtual int get_assignment(
      const int64_t vector_idx,
      int32_t &cid) const override
  {
    int ret = OB_SUCCESS;
    cid = -1;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (vector_idx < 0 || vector_idx >= count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      cid = assignments_.at(vector_idx);
    }
    return ret;
  }
  virtual int set_assignment(
      const int64_t vector_idx,
      const int32_t cid) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (vector_idx < 0 || vector_idx >= count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      assignments_.at(vector_idx) = cid;
    }
    return ret;
  }
private:
  ObArenaAllocator allocator_;
  ObArray<int32_t, ObWrapperAllocator> assignments_;
  int init_ret_;
};

int load_assignments(
    const common::ObIArray<int64_t> &source,
    HcaAssignmentBuffer &target)
{
  int ret = OB_SUCCESS;
  if (source.count() != target.count()) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < source.count(); ++i) {
    if (source.at(i) < INT32_MIN || source.at(i) > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ret = target.set_assignment(i, static_cast<int32_t>(source.at(i)));
    }
  }
  return ret;
}

class HcaMemberRows final : public share::ObHcaCidMemberStore
{
public:
  explicit HcaMemberRows(const int64_t cid_count)
    : allocator_(ObMemAttr(MTL_ID(), "IVFHcaMembers", ObCtxIds::VECTOR_CTX_ID)),
      rows_(), init_ret_(OB_SUCCESS)
  {
    rows_.set_block_allocator(ObWrapperAllocator(allocator_));
    if (cid_count < 0) {
      init_ret_ = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (init_ret_ = rows_.reserve(cid_count))) {
      LOG_WARN_RET(init_ret_, "failed to reserve IVF HCA member rows", K_(init_ret),
          K(cid_count));
    }
    for (int64_t cid = 0; OB_SUCCESS == init_ret_ && cid < cid_count; ++cid) {
      MemberRow *row = OB_NEWx(MemberRow, &allocator_);
      if (OB_ISNULL(row)) {
        init_ret_ = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA member row",
            K_(init_ret), K(cid));
      } else {
        row->set_block_allocator(ObWrapperAllocator(allocator_));
        if (OB_SUCCESS != (init_ret_ = rows_.push_back(row))) {
          LOG_WARN_RET(init_ret_, "failed to append IVF HCA member row",
              K_(init_ret), K(cid));
          row->~MemberRow();
        }
      }
    }
  }
  virtual ~HcaMemberRows()
  {
    for (int64_t i = 0; i < rows_.count(); ++i) {
      if (OB_NOT_NULL(rows_.at(i))) {
        rows_.at(i)->~MemberRow();
      }
    }
  }
  virtual int64_t cid_count() const override { return rows_.count(); }
  int get_init_ret() const { return init_ret_; }
  virtual int64_t member_count(const int64_t cid) const override
  {
    return OB_SUCCESS == init_ret_ && cid >= 0 && cid < cid_count()
        ? rows_.at(cid)->count() : -1;
  }
  virtual int get_member(
      const int64_t cid,
      const int64_t offset,
      int64_t &vector_idx) const override
  {
    int ret = OB_SUCCESS;
    vector_idx = -1;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= cid_count() || offset < 0
        || offset >= member_count(cid)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      vector_idx = rows_.at(cid)->at(offset);
    }
    return ret;
  }
  virtual int append_member(
      const int32_t cid,
      const int64_t vector_idx) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= cid_count() || vector_idx < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(rows_.at(cid)->push_back(vector_idx))) {
      LOG_WARN("failed to append IVF HCA member", K(ret), K(cid), K(vector_idx));
    }
    return ret;
  }
  virtual int move_row(
      const int64_t old_cid,
      const int64_t new_cid) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (old_cid < 0 || old_cid >= cid_count()
        || new_cid < 0 || new_cid >= cid_count()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (old_cid != new_cid) {
      rows_.at(new_cid)->reuse();
      if (OB_FAIL(rows_.at(new_cid)->assign(*rows_.at(old_cid)))) {
        LOG_WARN("failed to move IVF HCA member row", K(ret), K(old_cid),
            K(new_cid));
      } else {
        rows_.at(old_cid)->reuse();
      }
    }
    return ret;
  }
  virtual int bind_row(
      const int64_t output_cid,
      const share::ObHcaCidMemberStore &input_members,
      const int64_t input_cid) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (output_cid < 0 || output_cid >= cid_count()
        || input_cid < 0 || input_cid >= input_members.cid_count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      rows_.at(output_cid)->reuse();
    }
    for (int64_t i = 0;
         OB_SUCC(ret) && i < input_members.member_count(input_cid);
         ++i) {
      int64_t vector_idx = -1;
      if (OB_FAIL(input_members.get_member(input_cid, i, vector_idx))) {
        LOG_WARN("failed to get IVF HCA member", K(ret), K(input_cid), K(i));
      } else if (OB_FAIL(rows_.at(output_cid)->push_back(vector_idx))) {
        LOG_WARN("failed to bind IVF HCA member", K(ret), K(output_cid),
            K(input_cid), K(i));
      }
    }
    return ret;
  }
private:
  using MemberRow = ObArray<int64_t, ObWrapperAllocator>;
  ObArenaAllocator allocator_;
  ObArray<MemberRow *, ObWrapperAllocator> rows_;
  int init_ret_;
};

// A stats task owns one member list whose positions are already [0, count).
// Expose that identity row without allocating another index for every member.
class HcaSingleMemberRow final : public share::ObHcaCidMemberStore
{
public:
  explicit HcaSingleMemberRow(const int64_t member_count) : member_count_(member_count) {}
  virtual int64_t cid_count() const override { return 1; }
  virtual int64_t member_count(const int64_t cid) const override
  {
    return 0 == cid && member_count_ >= 0 ? member_count_ : -1;
  }
  virtual int get_member(
      const int64_t cid,
      const int64_t offset,
      int64_t &vector_idx) const override
  {
    int ret = OB_SUCCESS;
    vector_idx = -1;
    if (0 != cid || offset < 0 || offset >= member_count_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      vector_idx = offset;
    }
    return ret;
  }
  virtual int append_member(const int32_t cid, const int64_t vector_idx) override
  {
    UNUSEDx(cid, vector_idx);
    return OB_NOT_SUPPORTED;
  }
  virtual int move_row(const int64_t old_cid, const int64_t new_cid) override
  {
    UNUSEDx(old_cid, new_cid);
    return OB_NOT_SUPPORTED;
  }
  virtual int bind_row(
      const int64_t output_cid,
      const share::ObHcaCidMemberStore &input_members,
      const int64_t input_cid) override
  {
    UNUSEDx(output_cid, input_members, input_cid);
    return OB_NOT_SUPPORTED;
  }
private:
  int64_t member_count_;
};

class HcaCenterStats final : public share::ObHcaCenterStatsStore
{
public:
  HcaCenterStats(const int64_t count, const int64_t dimension)
    : count_(count), dimension_(dimension),
      allocator_(ObMemAttr(MTL_ID(), "IVFHcaStats", ObCtxIds::VECTOR_CTX_ID)),
      sums_(), member_counts_(), init_ret_(OB_SUCCESS)
  {
    sums_.set_block_allocator(ObWrapperAllocator(allocator_));
    member_counts_.set_block_allocator(ObWrapperAllocator(allocator_));
    if (count <= 0 || dimension <= 0) {
      init_ret_ = OB_INVALID_ARGUMENT;
    } else if (count > INT64_MAX / dimension) {
      init_ret_ = OB_SIZE_OVERFLOW;
    } else if (OB_SUCCESS != (init_ret_ =
        sums_.prepare_allocate(count * dimension))) {
      LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA center-stat sums", K_(init_ret),
          K(count), K(dimension));
    } else if (OB_SUCCESS != (init_ret_ =
        member_counts_.prepare_allocate(count))) {
      LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA center-stat counts", K_(init_ret),
          K(count));
    } else {
      for (int64_t i = 0; i < sums_.count(); ++i) {
        sums_.at(i) = 0.0F;
      }
      for (int64_t i = 0; i < member_counts_.count(); ++i) {
        member_counts_.at(i) = -1;
      }
    }
  }
  virtual int64_t count() const override { return count_; }
  virtual int64_t dimension() const override { return dimension_; }
  int get_init_ret() const { return init_ret_; }
  virtual int get_stats(
      const int64_t cid,
      const float *&sum,
      int64_t &member_count) const override
  {
    int ret = OB_SUCCESS;
    sum = nullptr;
    member_count = -1;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= count_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      sum = &sums_.at(cid * dimension_);
      member_count = member_counts_.at(cid);
    }
    return ret;
  }
  virtual int set_stats(
      const int64_t cid,
      const float *sum,
      const int64_t member_count) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= count_ || OB_ISNULL(sum) || member_count < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(&sums_.at(cid * dimension_), sum,
          dimension_ * sizeof(float));
      member_counts_.at(cid) = member_count;
    }
    return ret;
  }
private:
  int64_t count_;
  int64_t dimension_;
  ObArenaAllocator allocator_;
  ObArray<float, ObWrapperAllocator> sums_;
  ObArray<int64_t, ObWrapperAllocator> member_counts_;
  int init_ret_;
};

class HcaChangedSet final : public share::ObHcaChangedCidSet
{
public:
  explicit HcaChangedSet(const int64_t count)
    : allocator_(ObMemAttr(MTL_ID(), "IVFHcaChanged", ObCtxIds::VECTOR_CTX_ID)),
      changed_(), init_ret_(OB_SUCCESS)
  {
    changed_.set_block_allocator(ObWrapperAllocator(allocator_));
    if (count < 0) {
      init_ret_ = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (init_ret_ = changed_.prepare_allocate(count))) {
      LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA changed-center set", K_(init_ret),
          K(count));
    } else {
      for (int64_t i = 0; i < changed_.count(); ++i) {
        changed_.at(i) = 0;
      }
    }
  }
  int get_init_ret() const { return init_ret_; }
  virtual bool has_changed(const int64_t cid) const override
  {
    return OB_SUCCESS == init_ret_ && cid >= 0 && cid < changed_.count()
        && 0 != changed_.at(cid);
  }
  virtual int set_changed(const int64_t cid) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= changed_.count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      changed_.at(cid) = 1;
    }
    return ret;
  }
private:
  ObArenaAllocator allocator_;
  ObArray<uint8_t, ObWrapperAllocator> changed_;
  int init_ret_;
};

class HcaIdentityRemap final : public share::ObHcaCidRemapStore
{
public:
  explicit HcaIdentityRemap(const int64_t count) : count_(count) {}
  virtual int64_t count() const override { return count_; }
  virtual int get_new_cid(
      const int64_t old_cid,
      int32_t &new_cid) const override
  {
    int ret = OB_SUCCESS;
    new_cid = -1;
    if (old_cid < 0 || old_cid >= count_ || old_cid > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
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

class HcaNeighborhoodBuffer final : public share::ObHcaNeighborhoodStore
{
public:
  explicit HcaNeighborhoodBuffer(const int64_t center_count)
    : allocator_(ObMemAttr(MTL_ID(), "IVFHcaNbrs", ObCtxIds::VECTOR_CTX_ID)),
      rows_(), max_distances_(), written_(), init_ret_(OB_SUCCESS)
  {
    rows_.set_block_allocator(ObWrapperAllocator(allocator_));
    max_distances_.set_block_allocator(ObWrapperAllocator(allocator_));
    written_.set_block_allocator(ObWrapperAllocator(allocator_));
    if (center_count < 0) {
      init_ret_ = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (init_ret_ = rows_.reserve(center_count))) {
      LOG_WARN_RET(init_ret_, "failed to reserve IVF HCA neighborhood rows", K_(init_ret),
          K(center_count));
    } else if (OB_SUCCESS != (init_ret_ =
        max_distances_.prepare_allocate(center_count))) {
      LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA neighborhood distances",
          K_(init_ret), K(center_count));
    } else if (OB_SUCCESS != (init_ret_ =
        written_.prepare_allocate(center_count))) {
      LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA neighborhood state", K_(init_ret),
          K(center_count));
    }
    for (int64_t cid = 0;
         OB_SUCCESS == init_ret_ && cid < center_count;
         ++cid) {
      NeighborRow *row = OB_NEWx(NeighborRow, &allocator_);
      if (OB_ISNULL(row)) {
        init_ret_ = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN_RET(init_ret_, "failed to allocate IVF HCA neighborhood row",
            K_(init_ret), K(cid));
      } else {
        row->set_block_allocator(ObWrapperAllocator(allocator_));
        if (OB_SUCCESS != (init_ret_ = rows_.push_back(row))) {
          LOG_WARN_RET(init_ret_, "failed to append IVF HCA neighborhood row",
              K_(init_ret), K(cid));
          row->~NeighborRow();
        } else {
          max_distances_.at(cid) = 0.0F;
          written_.at(cid) = 0;
        }
      }
    }
  }
  virtual ~HcaNeighborhoodBuffer()
  {
    for (int64_t i = 0; i < rows_.count(); ++i) {
      if (OB_NOT_NULL(rows_.at(i))) {
        rows_.at(i)->~NeighborRow();
      }
    }
  }
  virtual int64_t center_count() const override { return rows_.count(); }
  int get_init_ret() const { return init_ret_; }
  virtual int64_t neighbor_count(const int64_t cid) const override
  {
    return OB_SUCCESS == init_ret_ && cid >= 0 && cid < center_count()
        ? rows_.at(cid)->count() : -1;
  }
  virtual int get_neighbor(
      const int64_t cid,
      const int64_t offset,
      int32_t &neighbor_cid) const override
  {
    int ret = OB_SUCCESS;
    neighbor_cid = -1;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= center_count() || offset < 0
        || offset >= neighbor_count(cid)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      neighbor_cid = rows_.at(cid)->at(offset);
    }
    return ret;
  }
  virtual int get_max_intra_distance(
      const int64_t cid,
      float &distance) const override
  {
    int ret = OB_SUCCESS;
    distance = 0.0F;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= center_count() || 0 == written_.at(cid)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      distance = max_distances_.at(cid);
    }
    return ret;
  }
  virtual int set_row(
      const int64_t cid,
      const int32_t *neighbor_cids,
      const int64_t neighbor_count,
      const float max_intra_distance) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != init_ret_) {
      ret = init_ret_;
    } else if (cid < 0 || cid >= center_count() || neighbor_count < 0
        || (neighbor_count > 0 && OB_ISNULL(neighbor_cids))
        || !std::isfinite(max_intra_distance)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      rows_.at(cid)->reuse();
      if (OB_FAIL(rows_.at(cid)->reserve(neighbor_count))) {
        LOG_WARN("failed to reserve IVF HCA neighborhood row", K(ret), K(cid),
            K(neighbor_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < neighbor_count; ++i) {
        if (OB_FAIL(rows_.at(cid)->push_back(neighbor_cids[i]))) {
          LOG_WARN("failed to append IVF HCA neighbor", K(ret), K(cid), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        max_distances_.at(cid) = max_intra_distance;
        written_.at(cid) = 1;
      }
    }
    return ret;
  }
private:
  using NeighborRow = ObArray<int32_t, ObWrapperAllocator>;
  ObArenaAllocator allocator_;
  ObArray<NeighborRow *, ObWrapperAllocator> rows_;
  ObArray<float, ObWrapperAllocator> max_distances_;
  ObArray<uint8_t, ObWrapperAllocator> written_;
  int init_ret_;
};

class HcaNeighborhoodView final : public share::ObHcaNeighborhoodStore
{
public:
  explicit HcaNeighborhoodView(const ObIVFNeighborGraphView &graph)
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
    } else {
      const int64_t value = graph_.cids_->at(
          graph_.offsets_->at(cid) + offset);
      if (value < 0 || value > INT32_MAX) {
        ret = OB_ERR_UNEXPECTED;
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
  const ObIVFNeighborGraphView &graph_;
};

int build_neighborhood_view(
    const ObIVFNeighborGraphView *graph,
    const share::ObHcaCenterStore &centers,
    ObIAllocator &allocator,
    HcaNeighborhoodView *&neighborhood)
{
  int ret = OB_SUCCESS;
  neighborhood = nullptr;
  if (OB_ISNULL(graph)) {
  } else if (!graph->is_valid()
      || graph->center_count_ != centers.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(neighborhood = OB_NEWx(
      HcaNeighborhoodView, &allocator, *graph))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  return ret;
}

class HcaVsagScratchAllocator final : public vsag::Allocator
{
public:
  HcaVsagScratchAllocator()
    : attr_(MTL_ID(), "IVFHcaScratch", ObCtxIds::VECTOR_CTX_ID)
  {}
  virtual std::string Name() override { return "IVFHcaScratch"; }
  virtual void *Allocate(const size_t size) override
  {
    void *result = nullptr;
    if (size > 0) {
      if (OB_UNLIKELY(size > SIZE_MAX - sizeof(size_t))) {
        LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED,
            "invalid HCA VSAG scratch allocation size", K(size));
        throw std::bad_alloc();
      } else {
        char *raw = static_cast<char *>(ob_malloc(size + sizeof(size_t), attr_));
        if (OB_ISNULL(raw)) {
          LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED,
              "failed to allocate HCA VSAG scratch memory", K(size), K(lbt()));
          throw std::bad_alloc();
        }
        *reinterpret_cast<size_t *>(raw) = size;
        result = raw + sizeof(size_t);
      }
    }
    return result;
  }
  virtual void Deallocate(void *pointer) override
  {
    if (OB_NOT_NULL(pointer)) {
      ob_free(static_cast<char *>(pointer) - sizeof(size_t));
    }
  }
  virtual void *Reallocate(void *pointer, const size_t size) override
  {
    void *result = nullptr;
    if (0 == size) {
      Deallocate(pointer);
    } else if (OB_ISNULL(pointer)) {
      result = Allocate(size);
    } else {
      char *raw = static_cast<char *>(pointer) - sizeof(size_t);
      const size_t old_size = *reinterpret_cast<size_t *>(raw);
      if (old_size >= size) {
        result = pointer;
      } else if (OB_NOT_NULL(result = Allocate(size))) {
        MEMCPY(result, pointer, old_size);
        Deallocate(pointer);
      }
    }
    return result;
  }
private:
  ObMemAttr attr_;
};

} // namespace


ObIVFHcaAlgorithmParam::ObIVFHcaAlgorithmParam()
  : train_param_(),
    neighborhood_param_(),
    refine_param_(),
    soar_param_(),
    center_norm_info_(nullptr),
    hgraph_m_(8),
    hgraph_ef_construction_(150)
{
}

bool ObIVFHcaAlgorithmParam::is_valid() const
{
  return train_param_.is_valid() && neighborhood_param_.is_valid()
      && refine_param_.is_valid() && soar_param_.is_valid()
      && (OB_ISNULL(center_norm_info_)
          || OB_NOT_NULL(center_norm_info_->normalize_func_))
      && hgraph_m_ > 0 && hgraph_ef_construction_ > 0;
}

// Own the reader and graph view across task boundaries; neither may refer to
// the initializing task's stack. Vector cache mutation occurs only in prepare.
class ObIVFRefineSession
{
public:
  ObIVFRefineSession(const ObIVFVectorReader &reader,
      const ObIArray<ObIVFVectorVID> &vids, int64_t center_count,
      const ObIVFNeighborGraphView *graph, ObIAllocator &allocator)
    : reader_(reader), cache_(reader_, vids), vectors_(cache_),
      input_(center_count, reader.get_dimension()),
      working_(center_count, reader.get_dimension()), assignments_(vids.count()),
      graph_(graph == nullptr ? ObIVFNeighborGraphView() : *graph), neighbors_(graph_),
      trainer_(allocator), rows_(0) {}
  int prepare() { return trainer_.prepare_batch(rows_); }
  int64_t task_count() const { return MIN(64L, (rows_ + 127) / 128); }
  ObIVFVectorReader reader_;
  HcaVectorCache cache_;
  HcaVectorAccessor vectors_;
  HcaCenterBuffer input_;
  HcaCenterBuffer working_;
  HcaAssignmentBuffer assignments_;
  ObIVFNeighborGraphView graph_;
  HcaNeighborhoodView neighbors_;
  share::ObHcaRefineTrainer trainer_;
  int64_t rows_;
};

ObIVFHcaAlgorithmAdapter::ObIVFHcaAlgorithmAdapter()
  : refine_allocator_(ObMemAttr(MTL_ID(), "IVFRefine", ObCtxIds::VECTOR_CTX_ID)),
    refine_cancel_code_(OB_SUCCESS),
    refine_session_(nullptr), is_inited_(false),
    param_(),
    hgraph_allocator_(nullptr),
    hgraph_(nullptr),
    neighborhood_mode_(ObIVFNeighborhoodMode::INVALID),
    hgraph_center_count_(0),
    hgraph_dimension_(0),
    insertion_centroids_(),
    insertion_source_queries_(0)
{
}

ObIVFHcaAlgorithmAdapter::~ObIVFHcaAlgorithmAdapter()
{
  release_refine();
  release_hgraph();
}

int ObIVFHcaAlgorithmAdapter::init(
    const ObIVFHcaAlgorithmParam &param,
    vsag::Allocator *hgraph_allocator)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF HCA algorithm adapter init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF HCA algorithm parameter", K(ret));
  } else {
    param_ = param;
    hgraph_allocator_ = hgraph_allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::check_cluster_quality(const ObIArray<int64_t> &counts, bool &degenerate)
{
  share::ObHcaClusterQuality quality;
  const int ret = share::hca_check_cluster_quality(counts.empty() ? nullptr : &counts.at(0), counts.count(), quality);
  degenerate = quality.degenerate_;
  LOG_INFO("IVF root assignment quality", K(ret), K(quality));
  return ret;
}

int ObIVFHcaAlgorithmAdapter::insertion_train_centers(ObIVFVectorReader &reader,
    const ObIArray<ObIVFVectorVID> &all_vids, const ObIVFFirstLevelSampleSet &first_level_samples,
    const ObIArray<float> &initial_centers,
    const int64_t center_count, ObIArray<float> &trained_centers)
{
  int ret = OB_SUCCESS;
  trained_centers.reset();
  const int64_t dimension = reader.get_dimension();
  if (!is_inited_ || dimension <= 0 || all_vids.empty()
      || center_count <= 0 || center_count > share::OB_HCA_MAX_LOCAL_K
      || center_count > all_vids.count() || all_vids.count() > INT64_MAX / dimension) {
    LOG_WARN("invalid IVF merge argument", "ret", OB_INVALID_ARGUMENT, "operation",
        "ObIVFHcaAlgorithmAdapter::insertion_train_centers", K(is_inited_), K(dimension), "all_vids_count",
        all_vids.count(), K(center_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (center_count > INT64_MAX / param_.train_param_.samples_per_cluster_) {
    LOG_WARN("IVF merge size overflow", "ret", OB_SIZE_OVERFLOW, "operation",
        "ObIVFHcaAlgorithmAdapter::insertion_train_centers", K(center_count),
        K(param_.train_param_.samples_per_cluster_));
    ret = OB_SIZE_OVERFLOW;
  } else {
    const int64_t sample_count = MIN(all_vids.count(),
        center_count * param_.train_param_.samples_per_cluster_);
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFInsertTrain", ObCtxIds::VECTOR_CTX_ID));
    HcaCapturedSampleBatch vectors(allocator);
    ret = vectors.init(first_level_samples, all_vids.count(), dimension, sample_count,
        param_.train_param_.random_seed_ + 1, allocator);
    HcaCenterBuffer input(center_count, dimension);
    HcaCenterBuffer output(center_count, dimension);
    share::ObHcaTrainParam train_param = param_.train_param_;
    train_param.max_iterations_ = 1;
    if (OB_FAIL(ret)) {
      LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
    }
    else if (OB_SUCCESS != (ret = input.get_init_ret())) {}
    else if (OB_SUCCESS != (ret = output.get_init_ret())) {}
    else if (OB_FAIL(load_centers(initial_centers, center_count, dimension, input))) {
      LOG_WARN("IVF merge operation failed", K(ret), "operation", "load_centers");
    }
    else if (OB_FAIL(share::hca_train_warm_centers(vectors, input, train_param,
        param_.center_norm_info_, output, allocator))) {
      LOG_WARN("failed to train sampled insertion root centers", K(ret), K(center_count), K(sample_count));
    } else {
      ret = append_centers(output, trained_centers);
      if (OB_FAIL(ret)) {
        LOG_WARN("IVF merge operation failed", K(ret), "operation", "append_centers", K(center_count));
      }
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::hca_collect_training_rows(const int64_t vector_count,
    const int64_t cluster_size, ObIArray<int64_t> &ordinals)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, share::OB_HCA_MAX_LOCAL_K> centers;
  centers.set_attr(ObMemAttr(MTL_ID(), "IVFClusterTrain", ObCtxIds::VECTOR_CTX_ID));
  ordinals.reuse();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(share::hca_plan_local_training(vector_count, cluster_size,
      param_.train_param_, centers, ordinals))) {
    LOG_WARN("failed to plan IVF training samples", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < centers.count(); ++i) {
    ret = ordinals.push_back(centers.at(i));
  }
  if (OB_SUCC(ret) && !ordinals.empty()) {
    lib::ob_sort(&ordinals.at(0), &ordinals.at(0) + ordinals.count());
    int64_t unique_count = 1;
    for (int64_t i = 1; i < ordinals.count(); ++i) {
      if (ordinals.at(i) != ordinals.at(unique_count - 1)) {
        ordinals.at(unique_count++) = ordinals.at(i);
      }
    }
    while (ordinals.count() > unique_count) { ordinals.pop_back(); }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::hca_train_loaded_samples(const int64_t vector_count,
    const int64_t cluster_size, const int64_t dimension,
    const ObIArray<int64_t> &ordinals, const ObIArray<float> &values,
    int64_t &local_k, ObIArray<float> &local_centers)
{
  // Original node ordinals are preserved through external sorting. The HCA
  // kernel performs exactly the same shuffles and minibatches as the old path.
  class SampleBatch final : public share::ObHcaVectorBatch
  {
  public:
    SampleBatch(int64_t count, int64_t dim, const ObIArray<int64_t> &ids,
        const ObIArray<float> &values) : count_(count), dim_(dim), ids_(ids), values_(values) {}
    int64_t count() const override { return count_; }
    int64_t dimension() const override { return dim_; }
    int get_vector(int64_t pos, int64_t &idx, const float *&value) const override
    {
      int64_t lo = 0, hi = ids_.count();
      while (lo < hi) {
        const int64_t mid = lo + (hi - lo) / 2;
        if (ids_.at(mid) < pos) { lo = mid + 1; } else { hi = mid; }
      }
      idx = -1; value = nullptr;
      if (lo >= ids_.count() || ids_.at(lo) != pos) { return OB_INVALID_DATA; }
      idx = pos; value = &values_.at(lo * dim_);
      return OB_SUCCESS;
    }
  private:
    int64_t count_, dim_;
    const ObIArray<int64_t> &ids_;
    const ObIArray<float> &values_;
  };
  int ret = OB_SUCCESS;
  local_k = 0;
  local_centers.reuse();
  if (!is_inited_ || vector_count <= 0 || dimension <= 0 || ordinals.empty()
      || ordinals.count() > INT64_MAX / dimension
      || values.count() != ordinals.count() * dimension) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid loaded IVF training samples", K(ret), K_(is_inited), K(vector_count), K(dimension), "ordinals",
             ordinals.count(), "values", values.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ordinals.count(); ++i) {
    if (ordinals.at(i) < 0 || ordinals.at(i) >= vector_count || (i > 0 && ordinals.at(i - 1) >= ordinals.at(i))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid loaded IVF training ordinal", K(ret), K(i), "ordinal", ordinals.at(i), K(vector_count));
    }
  }
  if (OB_SUCC(ret)) {
    SampleBatch batch(vector_count, dimension, ordinals, values);
    HcaCenterBuffer centers(share::OB_HCA_MAX_LOCAL_K, dimension);
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFClusterTrain", ObCtxIds::VECTOR_CTX_ID));
    if (OB_SUCCESS != (ret = centers.get_init_ret())) {
      LOG_WARN("failed to initialize loaded IVF training centers", K(ret), K(dimension));
    } else if (OB_FAIL(share::hca_train_prepared_centers(batch, cluster_size,
        param_.train_param_, param_.center_norm_info_, centers, local_k, allocator))) {
      LOG_WARN("failed to train prepared IVF samples", K(ret));
    } else {
      ret = append_centers(centers, local_centers);
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::hca_train_local_centers(
    const int64_t depth,
    const int64_t node_idx,
    ObIVFVectorReader &reader,
    const common::ObIArray<ObIVFVectorVID> &node_vids,
    const ObIVFFirstLevelSampleSet *first_level_samples,
    const int64_t cluster_size,
    int64_t &local_k,
    common::ObIArray<float> &local_centers)
{
  int ret = OB_SUCCESS;
  UNUSED(node_idx);
  local_k = 0;
  local_centers.reset();
  HcaVectorCache cache(reader, node_vids);
  HcaVectorBatch vectors(cache, 0, node_vids.count(), 0);
  HcaCenterBuffer centers(share::OB_HCA_MAX_LOCAL_K, reader.get_dimension());
  ObArenaAllocator allocator(
      "IVFHcaTrain", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  HcaCapturedSampleBatch samples(allocator);
  const bool use_captured_samples = depth == 0 && first_level_samples != nullptr
      && node_vids.count() > cluster_size;
  if (OB_SUCCESS != centers.get_init_ret()) {
    ret = centers.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA training buffers", K(ret));
  } else if (!is_inited_ || node_vids.empty() || cluster_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF HCA local-center training input", K(ret),
        "vector_count", node_vids.count(), K(cluster_size),
        "dimension", reader.get_dimension(), K_(is_inited));
  } else if (use_captured_samples && param_.train_param_.max_local_k_
      > INT64_MAX / param_.train_param_.samples_per_cluster_) {
    LOG_WARN("IVF merge size overflow", "ret", OB_SIZE_OVERFLOW, "operation",
        "ObIVFHcaAlgorithmAdapter::hca_train_local_centers", K(use_captured_samples),
        K(param_.train_param_.max_local_k_), K(param_.train_param_.samples_per_cluster_), K(depth), K(node_idx),
        K(cluster_size));
    ret = OB_SIZE_OVERFLOW;
  } else if (use_captured_samples && OB_FAIL(samples.init(*first_level_samples,
      node_vids.count(), reader.get_dimension(),
      param_.train_param_.max_local_k_ * param_.train_param_.samples_per_cluster_,
      param_.train_param_.random_seed_ + 1, allocator))) {
    LOG_WARN("failed to load captured IVF root samples", K(ret));
  } else if (OB_FAIL(share::hca_train_local_centers(
      vectors, cluster_size, param_.train_param_, param_.center_norm_info_,
      centers, local_k, allocator, use_captured_samples ? &samples : nullptr))) {
    LOG_WARN("failed to train IVF HCA local centers", K(ret),
        "vector_count", node_vids.count(), K(cluster_size));
  } else if (centers.count() != local_k) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected IVF HCA local center count", K(ret), K(local_k),
        "center_count", centers.count());
  } else {
    ret = append_centers(centers, local_centers);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append IVF HCA local centers", K(ret), K(local_k));
    }
  }
  return ret;
}

void ObIVFHcaAlgorithmAdapter::prepare_insertion_source(const ObIVFCentroidSource &source,
    const ObIVFMergePrior &prior, const share::ObVectorIndexDistAlgorithm distance,
    const int64_t leaf_cid_base)
{
  insertion_centroids_.reset();
  insertion_source_queries_ = 0;
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "[DISK_BBQ] IVF HCA adapter is not initialized for centroid source binding");
    return;
  }
  if (prior.dominant_segment_ < 0) { return; }
  if (prior.dominant_segment_ >= prior.segments_.count()) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "[DISK_BBQ] invalid insertion source segment",
        K(prior.dominant_segment_), "segment_count", prior.segments_.count());
    return;
  }
  const auto &segment = prior.segments_.at(prior.dominant_segment_);
  if (segment.centroid_count_ < 0 || segment.centroid_begin_ < 0
      || segment.centroid_begin_ > prior.old_cids_.count() - segment.centroid_count_
      || leaf_cid_base <= 0) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "[DISK_BBQ] invalid insertion source centroid range",
        K(segment.centroid_begin_), K(segment.centroid_count_), K(leaf_cid_base),
        "old_cid_count", prior.old_cids_.count());
    return;
  }
  // HCA uses squared L2. An IP source need not return equivalent candidates;
  // preserve exact numerical assignment when the source ordering is unsuitable.
  if ((distance != share::VIDA_L2 && distance != share::VIDA_COS)
      || segment.live_vector_count_ >= prior.vector_count_
      || segment.centroid_count_ == 0) { return; }
  for (int64_t c = 0; c < segment.centroid_count_; ++c) {
    if (prior.old_cids_.at(segment.centroid_begin_ + c) != leaf_cid_base + c) { return; }
  }
  insertion_centroids_.source_ = &source;
  insertion_centroids_.segment_ = prior.dominant_segment_;
  insertion_centroids_.count_ = segment.centroid_count_;
  insertion_centroids_.dim_ = prior.dimension_;
  LOG_INFO("IVF insertion centroid source bound", "segment", insertion_centroids_.segment_,
      "center_count", segment.centroid_count_, "dimension", prior.dimension_);
}

void ObIVFHcaAlgorithmAdapter::finish_insertion_assign()
{
  LOG_INFO("IVF insertion centroid search completed", K_(insertion_source_queries));
  insertion_centroids_.reset();
}

namespace {
// Read-only view: insertion and SOAR borrow the immutable stage centers.
class HcaCenterArrayView final : public share::ObHcaCenterStore
{
public:
  HcaCenterArrayView(const ObIArray<float> &values, int64_t dimension)
    : values_(values), dimension_(dimension) {}
  int64_t count() const override { return dimension_ > 0 ? values_.count() / dimension_ : 0; }
  int64_t capacity() const override { return count(); }
  int64_t dimension() const override { return dimension_; }
  int get_center(int64_t cid, const float *&center) const override
  {
    center = nullptr;
    if (cid < 0 || cid >= count()) {
      int ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF center array index", K(ret), K(cid), K(count()));
      return OB_INVALID_ARGUMENT;
    }
    center = &values_.at(cid * dimension_);
    return OB_SUCCESS;
  }
  int get_mutable_center(int64_t cid, float *&) override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot get mutable center from read-only IVF center view", K(ret), K(cid));
    return ret;
  }
  int set_center(int64_t cid, const float *) override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot modify read-only IVF center view", K(ret), K(cid));
    return ret;
  }
private:
  const ObIArray<float> &values_;
  int64_t dimension_;
};

// SOAR's shared interface writes both A2 and reverse rows. This adapter returns
// only A2; the processor builds durable member rows after all tasks join.
// Keep append validation without allocating another per-CID member table.
class HcaUnusedMemberOutput final : public share::ObHcaCidMemberStore
{
public:
  explicit HcaUnusedMemberOutput(int64_t cid_count) : cid_count_(cid_count) {}
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
}

int ObIVFHcaAlgorithmAdapter::insertion_assign_vectors(ObIVFVectorReader &reader,
    const ObIArray<ObIVFVectorVID> &vids, const ObIArray<float> &centers,
    const ObIArray<int64_t> &offsets, const ObIArray<int64_t> &candidates,
    const int64_t begin, const int64_t end, ObIArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  assignments.reuse();
  const int64_t dimension = reader.get_dimension();
  const int64_t k = dimension > 0 ? centers.count() / dimension : 0;
  HcaVectorCache cache(reader, vids);
  HcaCenterArrayView center_view(centers, dimension);
  const bool use_source = insertion_centroids_.matches(k, dimension);
  int64_t source_queries = 0;
  if (!is_inited_ || dimension <= 0 || k <= 0 || centers.count() != k * dimension
      || begin < 0 || end <= begin || end > vids.count() || offsets.count() != vids.count() + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF insertion assignment arguments", K(ret), K(begin), K(end), K(dimension), K(k),
        K(vids.count()), K(offsets.count()));
  } else if (OB_FAIL(cache.prefetch(begin, end))) {
    LOG_WARN("failed to prefetch insertion vectors", K(ret), K(begin), K(end));
  } else if (OB_FAIL(assignments.reserve(end - begin))) {
    LOG_WARN("failed to reserve IVF insertion assignments", K(ret), K(begin), K(end));
  }
  for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
    if (OB_FAIL(assignments.push_back(-1))) {
      LOG_WARN("failed to initialize IVF insertion assignment", K(ret), K(i));
    }
  }
  const float *batch[share::OB_HCA_ASSIGN_BATCH_SIZE];
  int64_t positions[share::OB_HCA_ASSIGN_BATCH_SIZE];
  int64_t batch_count = 0;
  auto flush_batch = [&]() -> int {
    int batch_ret = OB_SUCCESS;
    int64_t nearest[share::OB_HCA_ASSIGN_BATCH_SIZE];
    if (batch_count > 0) {
      batch_ret = share::hca_find_nearest_centers(batch, batch_count, center_view, nullptr, k, nearest);
      if (OB_SUCCESS != batch_ret) {
        LOG_WARN("failed to assign IVF insertion batch", K(batch_ret), K(batch_count), K(k));
      }
      for (int64_t q = 0; batch_ret == OB_SUCCESS && q < batch_count; ++q) {
        assignments.at(positions[q] - begin) = nearest[q];
      }
      batch_count = 0;
    }
    return batch_ret;
  };
  for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
    const float *vector = nullptr;
    const int64_t first = offsets.at(i), last = offsets.at(i + 1);
    if (first < 0 || last < first || last > candidates.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF insertion candidate range", K(ret), K(i), K(first), K(last),
          K(candidates.count()));
    } else if (OB_FAIL(cache.get_vector(i, vector))) {
      LOG_WARN("failed to read IVF insertion vector", K(ret), K(i));
    } else if (first != last) {
      // Old postings recover their working owner from inherited memberships.
      int64_t best = -1;
      float best_distance = 0.0F;
      for (int64_t j = first; OB_SUCC(ret) && j < last; ++j) {
        const int64_t cid = candidates.at(j);
        if (cid < 0 || cid >= k) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid cached insertion candidate center", K(ret), K(cid), K(k));
        }
        else {
          const float distance = share::hca_square_distance(vector, &centers.at(cid * dimension), dimension);
          if (!std::isfinite(distance)) {
            ret = OB_INVALID_DATA;
            LOG_WARN("non-finite insertion candidate distance", K(ret), K(cid), K(distance));
          }
          else if (best < 0 || distance < best_distance) { best = cid; best_distance = distance; }
        }
      }
      if (OB_SUCC(ret)) { assignments.at(i - begin) = best; }
    } else {
      bool assigned = false;
      if (use_source) {
        int32_t source_candidates[16];
        int64_t count = 0, nearest = -1;
        ++source_queries;
        if (OB_FAIL(insertion_centroids_.source_->search_centroids(insertion_centroids_.segment_,
            vector, dimension, source_candidates, ARRAYSIZEOF(source_candidates), count))) {
          LOG_WARN("[DISK_BBQ] insertion centroid source query failed", K(ret), K(i), K(begin), K(end));
        } else if (OB_FAIL(share::hca_find_nearest_centers(
            &vector, 1, center_view, source_candidates, count, &nearest))) {
          LOG_WARN("[DISK_BBQ] failed to assign insertion vector from source candidates", K(ret), K(i), K(count));
        } else {
          assignments.at(i - begin) = nearest;
          assigned = true;
        }
      }
      if (OB_SUCC(ret) && !assigned) {
        batch[batch_count] = vector;
        positions[batch_count++] = i;
        if (batch_count == share::OB_HCA_ASSIGN_BATCH_SIZE) { ret = flush_batch(); }
      }
    }
    if (OB_SUCC(ret) && (i - begin) % 64 == 0 && OB_FAIL(share::dag_yield())) {
      LOG_WARN("IVF insertion assignment yield failed", K(ret), K(i), K(begin), K(end));
    }
  }
  if (OB_SUCC(ret)) { ret = flush_batch(); }
  ATOMIC_AAF(&insertion_source_queries_, source_queries);
  if (OB_FAIL(ret)) { LOG_WARN("failed to assign insertion vectors", K(ret), K(begin), K(end)); }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::hca_assign_vectors(
    ObIVFVectorReader &reader,
    const common::ObIArray<ObIVFVectorVID> &node_vids,
    const common::ObIArray<float> &local_centers,
    const int64_t local_k,
    const int64_t vid_begin,
    const int64_t vid_end,
    common::ObIArray<int64_t> &local_center_idxs)
{
  int ret = OB_SUCCESS;
  local_center_idxs.reset();
  const bool valid_vid_range = vid_begin >= 0 && vid_end > vid_begin
      && vid_end <= node_vids.count();
  const int64_t batch_count = valid_vid_range ? vid_end - vid_begin : 0;
  HcaVectorCache cache(reader, node_vids);
  // Source positions stay node-relative; temporary assignments are batch-local.
  HcaVectorBatch vectors(cache, vid_begin, vid_end, 0);
  HcaCenterBuffer centers(local_k, reader.get_dimension());
  HcaAssignmentBuffer assignments(batch_count);
  HcaMemberRows members(local_k);
  ObSEArray<int32_t, share::OB_HCA_MAX_LOCAL_K> candidates;
  candidates.set_attr(ObMemAttr(MTL_ID(), "IVFHcaAssign", ObCtxIds::VECTOR_CTX_ID));
  if (local_k > 0 && OB_FAIL(candidates.prepare_allocate(local_k))) {
    LOG_WARN("failed to allocate IVF HCA assignment candidates", K(ret),
        K(local_k));
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < local_k; ++cid) {
    candidates.at(cid) = static_cast<int32_t>(cid);
  }
  if (OB_SUCC(ret) && OB_SUCCESS != centers.get_init_ret()) {
    ret = centers.get_init_ret();
  }
  if (OB_SUCC(ret) && OB_SUCCESS != assignments.get_init_ret()) {
    ret = assignments.get_init_ret();
  }
  if (OB_SUCC(ret) && OB_SUCCESS != members.get_init_ret()) {
    ret = members.get_init_ret();
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to initialize IVF HCA assignment buffers", K(ret),
        K(local_k), "vector_count", node_vids.count());
  } else if (!is_inited_ || !valid_vid_range || local_k <= 0
      || OB_FAIL(load_centers(local_centers, local_k,
          reader.get_dimension(), centers))) {
    if (OB_SUCCESS == ret) {
      ret = OB_INVALID_ARGUMENT;
    }
    LOG_WARN("invalid IVF HCA assignment input", K(ret), K(vid_begin),
        K(vid_end), K(local_k), "vector_count", node_vids.count(),
        "dimension", reader.get_dimension());
  } else if (OB_FAIL(cache.prefetch(vid_begin, vid_end))) {
    LOG_WARN("failed to prefetch IVF HCA assignment vectors", K(ret),
        K(vid_begin), K(vid_end));
  } else if (OB_FAIL(share::hca_assign_vectors(
      vectors, centers, &candidates.at(0), local_k, assignments, members))) {
    LOG_WARN("failed to assign IVF HCA vectors", K(ret), K(vid_begin),
        K(vid_end), K(local_k));
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < batch_count; ++idx) {
    int32_t cid = -1;
    if (OB_FAIL(assignments.get_assignment(idx, cid))) {
      LOG_WARN("failed to get IVF HCA assignment", K(ret), K(idx));
    } else {
      ret = local_center_idxs.push_back(cid);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to append IVF HCA assignment", K(ret), K(idx), K(cid));
      }
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::hca_assign_loaded_vectors(
    const ObIArray<float> &vectors, const ObIArray<int64_t> &positions,
    const int64_t begin, const int64_t end, const int64_t dimension,
    const ObIArray<float> &centers, const int64_t local_k,
    ObIArray<int64_t> &assignments)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || dimension <= 0 || local_k <= 0
      || local_k > share::OB_HCA_MAX_LOCAL_K || begin < 0 || end <= begin
      || end > positions.count() || vectors.count() % dimension != 0
      || assignments.count() != vectors.count() / dimension
      || local_k > INT64_MAX / dimension || centers.count() != local_k * dimension) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid loaded IVF assignment input", K(ret), K_(is_inited), K(dimension), K(local_k), K(begin), K(end),
             "positions", positions.count(), "vectors", vectors.count(), "centers", centers.count());
  } else {
    int32_t candidates[share::OB_HCA_MAX_LOCAL_K];
    for (int64_t cid = 0; cid < local_k; ++cid) { candidates[cid] = cid; }
    HcaLoadedBatch batch(vectors, positions, begin, end, dimension);
    HcaBorrowedCenters center_view(centers, dimension);
    HcaLoadedAssignments result(assignments, local_k);
    // Same distance kernel, candidate order and tie rule as VID-based assign.
    ret = share::hca_assign_vectors(batch, center_view, candidates, local_k, result, result);
    if (OB_SUCC(ret) && end == positions.count()) {
      ret = share::dag_yield();
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to assign loaded IVF vectors", K(ret), K(begin), K(end));
    }
  }
  return ret;
}

void ObIVFHcaAlgorithmAdapter::hca_accumulate_center_stats(
    float *sum, const float *vector, const int64_t dimension)
{
  share::hca_simd::axpy(sum, vector, 1.0F, dimension);
}

int ObIVFHcaAlgorithmAdapter::hca_finalize_center_stats(const float *sum,
    const int64_t count, const int64_t dimension, ObIArray<float> &center)
{
  int ret = OB_SUCCESS;
  center.reuse();
  if (!is_inited_ || nullptr == sum || count <= 0 || dimension <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF center statistics", K(ret), K_(is_inited), KP(sum), K(count), K(dimension));
  }
  for (int64_t d = 0; OB_SUCC(ret) && d < dimension; ++d) {
    if (OB_FAIL(center.push_back(sum[d] / count))) {
      LOG_WARN("failed to append IVF finalized center value", K(ret), K(d), K(dimension), K(count));
    }
  }
  if (OB_SUCC(ret)) {
    if (param_.center_norm_info_ != nullptr) {
      if (OB_FAIL(share::hca_normalize_center(&center.at(0), dimension, *param_.center_norm_info_))) {
        LOG_WARN("failed to normalize IVF finalized center", K(ret), K(dimension), K(count));
      }
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::hca_compute_local_center_stats_range(
    ObIVFVectorReader &reader,
    const common::ObIArray<ObIVFVectorVID> &member_vids,
    const common::ObIArray<float> &local_centers,
    const int64_t local_center_idx,
    common::ObIArray<float> &updated_center)
{
  int ret = OB_SUCCESS;
  UNUSED(local_centers);
  updated_center.reset();
  const int64_t dimension = reader.get_dimension();
  HcaStreamingStatsAccessor vectors(reader, member_vids);
  HcaSingleMemberRow members(member_vids.count());
  HcaCenterStats stats(1, dimension);
  ObArenaAllocator allocator(
      "IVFHcaStats", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_SUCCESS != stats.get_init_ret()) {
    ret = stats.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA center-stat buffer", K(ret));
  } else if (!is_inited_ || member_vids.empty() || local_center_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF HCA center-stat input", K(ret),
        "member_count", member_vids.count(), K(local_center_idx),
        K(dimension), K_(is_inited));
  }
  if (OB_SUCC(ret) && OB_FAIL(share::hca_compute_local_center_stats_range(
      vectors, members, 0, 1, dimension, stats, allocator))) {
    LOG_WARN("failed to compute IVF HCA local center stats", K(ret),
        "member_count", member_vids.count(), K(dimension));
  }
  const float *sum = nullptr;
  int64_t member_count = 0;
  if (OB_SUCC(ret) && OB_FAIL(stats.get_stats(0, sum, member_count))) {
    LOG_WARN("failed to get IVF HCA local center stats", K(ret));
  } else if (OB_SUCC(ret)
      && (member_count != member_vids.count() || member_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected IVF HCA center-stat member count", K(ret),
        K(member_count), "expected_count", member_vids.count());
  }
  for (int64_t d = 0; OB_SUCC(ret) && d < dimension; ++d) {
    ret = updated_center.push_back(sum[d] / member_count);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append IVF HCA updated center value", K(ret), K(d));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(param_.center_norm_info_)) {
    ret = share::hca_normalize_center(
        &updated_center.at(0), dimension, *param_.center_norm_info_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to normalize IVF HCA updated center", K(ret), K(dimension));
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::neighborhood_begin(
    const ObIVFNeighborhoodMode mode,
    const common::ObIArray<float> &centers,
    const int64_t center_count,
    const int64_t dimension)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || OB_NOT_NULL(hgraph_)
      || (ObIVFNeighborhoodMode::BRUTE_FORCE != mode
          && ObIVFNeighborhoodMode::HGRAPH != mode)
      || center_count <= share::OB_HCA_NEIGHBOR_COUNT
      || centers.count() != center_count * dimension) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF HCA neighborhood input", K(ret), K(mode),
        K(center_count), K(dimension), "center_value_count", centers.count(),
        K_(is_inited));
  } else if (ObIVFNeighborhoodMode::HGRAPH == mode) {
    if (OB_ISNULL(hgraph_allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("IVF HGraph allocator is null", K(ret));
    } else if (OB_FAIL(common::obvectorutil::create_index(
        hgraph_, common::obvsag::HGRAPH_TYPE, "float32", "l2", dimension,
        param_.hgraph_m_, param_.hgraph_ef_construction_,
        param_.neighborhood_param_.ef_search_, hgraph_allocator_))) {
      LOG_WARN("failed to create IVF HGraph index", K(ret), K(center_count),
          K(dimension));
    }
  }
  if (OB_SUCC(ret)) {
    neighborhood_mode_ = mode;
    hgraph_center_count_ = center_count;
    hgraph_dimension_ = dimension;
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::neighborhood_add_center_view(
    const share::ObHcaCenterStore &centers, const int64_t cid_begin, const int64_t cid_end)
{
  int ret = ATOMIC_LOAD(&refine_cancel_code_);
  if (OB_FAIL(ret)) {
  } else if (!is_inited_ || neighborhood_mode_ != ObIVFNeighborhoodMode::HGRAPH
      || OB_ISNULL(hgraph_) || centers.count() != hgraph_center_count_
      || centers.dimension() != hgraph_dimension_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF borrowed neighborhood-add state", K(ret), K(cid_begin), K(cid_end));
  } else if (OB_FAIL(share::hca_add_centers_to_index(hgraph_, centers, cid_begin, cid_end))) {
    LOG_WARN("failed to add borrowed IVF centers to HGraph", K(ret), K(cid_begin), K(cid_end));
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::neighborhood_build_row_view(
    const ObIVFNeighborhoodMode mode, const share::ObHcaCenterStore &centers,
    const int64_t cid_begin, const int64_t cid_end, share::ObHcaNeighborhoodStore &rows)
{
  int ret = ATOMIC_LOAD(&refine_cancel_code_);
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFDdlGraph", ObCtxIds::VECTOR_CTX_ID));
  HcaVsagScratchAllocator search_allocator;
  if (OB_FAIL(ret)) {
  } else if (!is_inited_ || mode != neighborhood_mode_
      || centers.count() != hgraph_center_count_ || centers.dimension() != hgraph_dimension_
      || (mode != ObIVFNeighborhoodMode::HGRAPH && mode != ObIVFNeighborhoodMode::BRUTE_FORCE)
      || (mode == ObIVFNeighborhoodMode::HGRAPH && OB_ISNULL(hgraph_))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF borrowed neighborhood lookup state", K(ret), K(mode), K(cid_begin), K(cid_end));
  } else if (OB_FAIL(share::hca_build_neighborhood_rows(
      mode == ObIVFNeighborhoodMode::HGRAPH ? hgraph_ : nullptr,
      centers, cid_begin, cid_end, param_.neighborhood_param_, rows, allocator,
      mode == ObIVFNeighborhoodMode::HGRAPH ? &search_allocator : nullptr))) {
    LOG_WARN("failed to build borrowed IVF neighborhood rows", K(ret), K(cid_begin), K(cid_end));
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::neighborhood_add_centers(
    const common::ObIArray<float> &centers,
    const int64_t center_count,
    const int64_t dimension,
    const int64_t cid_begin,
    const int64_t cid_end)
{
  int ret = OB_SUCCESS;
  HcaCenterBuffer center_store(center_count, dimension);
  if (OB_SUCCESS != center_store.get_init_ret()) {
    ret = center_store.get_init_ret();
    LOG_WARN("failed to initialize IVF HGraph center buffer", K(ret));
  } else if (!is_inited_ || ObIVFNeighborhoodMode::HGRAPH != neighborhood_mode_
      || OB_ISNULL(hgraph_) || center_count != hgraph_center_count_
      || dimension != hgraph_dimension_
      || OB_FAIL(load_centers(centers, center_count, dimension, center_store))) {
    if (OB_SUCCESS == ret) {
      ret = OB_STATE_NOT_MATCH;
    }
    LOG_WARN("invalid IVF HGraph center-add state", K(ret), K(center_count),
        K(dimension), K(cid_begin), K(cid_end));
  } else {
    ret = share::hca_add_centers_to_index(
        hgraph_, center_store, cid_begin, cid_end);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to add centers to IVF HGraph", K(ret), K(cid_begin),
          K(cid_end), K(center_count));
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::neighborhood_finish_add()
{
  int ret = OB_SUCCESS;
  int64_t actual_count = 0;
  if (!is_inited_ || ObIVFNeighborhoodMode::HGRAPH != neighborhood_mode_
      || OB_ISNULL(hgraph_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF HGraph finish-add state", K(ret),
        K_(neighborhood_mode), KP_(hgraph), K_(is_inited));
  } else if (OB_FAIL(common::obvectorutil::get_index_number(
      hgraph_, actual_count))) {
    LOG_WARN("failed to get IVF HGraph center count", K(ret));
  } else if (actual_count != hgraph_center_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected IVF HGraph center count", K(ret), K(actual_count),
        K_(hgraph_center_count));
  } else {
    ret = common::obvectorutil::immutable_optimize(hgraph_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to optimize IVF HGraph", K(ret), K(actual_count));
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::neighborhood_find_neighbors(
    const ObIVFNeighborhoodMode mode,
    const common::ObIArray<float> &centers,
    const int64_t center_count,
    const int64_t dimension,
    const int64_t cid_begin,
    const int64_t cid_end,
    common::ObIArray<int64_t> &neighbor_offsets,
    common::ObIArray<int64_t> &neighbor_cids,
    common::ObIArray<float> &max_intra_distances)
{
  int ret = OB_SUCCESS;
  neighbor_offsets.reset();
  neighbor_cids.reset();
  max_intra_distances.reset();
  HcaCenterBuffer center_store(center_count, dimension);
  HcaNeighborhoodBuffer neighborhood(center_count);
  ObArenaAllocator allocator(
      "IVFGraphBuild", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  HcaVsagScratchAllocator search_allocator;
  common::obvsag::VectorIndexPtr index = nullptr;
  if (OB_SUCCESS != center_store.get_init_ret()) {
    ret = center_store.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA neighborhood centers", K(ret));
  } else if (OB_SUCCESS != neighborhood.get_init_ret()) {
    ret = neighborhood.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA neighborhood buffer", K(ret));
  } else if (!is_inited_ || mode != neighborhood_mode_
      || center_count != hgraph_center_count_
      || dimension != hgraph_dimension_
      || OB_FAIL(load_centers(centers, center_count, dimension, center_store))) {
    if (OB_SUCCESS == ret) {
      ret = OB_STATE_NOT_MATCH;
    }
    LOG_WARN("invalid IVF HCA neighborhood lookup state", K(ret), K(mode),
        K(center_count), K(dimension), K(cid_begin), K(cid_end));
  } else if (ObIVFNeighborhoodMode::HGRAPH == mode) {
    if (OB_ISNULL(hgraph_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF HGraph is null during neighborhood lookup", K(ret));
    } else {
      index = hgraph_;
    }
  } else if (ObIVFNeighborhoodMode::BRUTE_FORCE != mode) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF neighborhood lookup mode", K(ret), K(mode));
  }
  if (OB_SUCC(ret)) {
    ret = share::hca_build_neighborhood_rows(
        index, center_store, cid_begin, cid_end,
        param_.neighborhood_param_, neighborhood, allocator,
        OB_NOT_NULL(index) ? &search_allocator : nullptr);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to build IVF HCA neighborhood rows", K(ret),
          K(cid_begin), K(cid_end), K(center_count));
    }
  }
  if (OB_SUCC(ret)) {
    ret = neighbor_offsets.push_back(0);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append initial IVF neighbor offset", K(ret));
    }
  }
  for (int64_t cid = cid_begin; OB_SUCC(ret) && cid < cid_end; ++cid) {
    for (int64_t offset = 0;
         OB_SUCC(ret) && offset < neighborhood.neighbor_count(cid);
         ++offset) {
      int32_t neighbor_cid = -1;
      if (OB_FAIL(neighborhood.get_neighbor(cid, offset, neighbor_cid))) {
        LOG_WARN("failed to get IVF HCA neighbor", K(ret), K(cid), K(offset));
      } else {
        ret = neighbor_cids.push_back(neighbor_cid);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to append IVF HCA neighbor", K(ret), K(cid),
              K(offset), K(neighbor_cid));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = neighbor_offsets.push_back(neighbor_cids.count());
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to append IVF HCA neighbor offset", K(ret), K(cid));
      }
    }
    if (OB_SUCC(ret)) {
      float max_intra_distance = 0.0F;
      if (OB_FAIL(neighborhood.get_max_intra_distance(
          cid, max_intra_distance))) {
        LOG_WARN("failed to get IVF HCA maximum intra distance", K(ret), K(cid));
      } else {
        ret = max_intra_distances.push_back(max_intra_distance);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to append IVF HCA maximum intra distance",
              K(ret), K(cid));
        }
      }
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::neighborhood_finish()
{
  int ret = OB_SUCCESS;
  if (!is_inited_
      || (ObIVFNeighborhoodMode::BRUTE_FORCE != neighborhood_mode_
          && ObIVFNeighborhoodMode::HGRAPH != neighborhood_mode_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF HCA neighborhood finish state", K(ret),
        K_(neighborhood_mode), K_(is_inited));
  } else {
    release_hgraph();
    neighborhood_mode_ = ObIVFNeighborhoodMode::INVALID;
    hgraph_center_count_ = 0;
    hgraph_dimension_ = 0;
  }
  return ret;
}



int ObIVFHcaAlgorithmAdapter::refine_train(
    ObIVFVectorReader &reader,
    const common::ObIArray<ObIVFVectorVID> &all_vids,
    const common::ObIArray<float> &centers,
    const common::ObIArray<int64_t> &assignments,
    const ObIVFNeighborGraphView *neighbor_graph,
    const int64_t center_count,
    common::ObIArray<float> &working_centers)
{
  int ret = OB_SUCCESS;
  working_centers.reset();
  const int64_t dimension = reader.get_dimension();
  HcaVectorCache cache(reader, all_vids);
  HcaVectorAccessor vectors(cache);
  HcaCenterBuffer input_centers(center_count, dimension);
  HcaCenterBuffer output_centers(center_count, dimension);
  HcaAssignmentBuffer input_assignments(assignments.count());
  ObArenaAllocator allocator(
      "IVFRefineTrain", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  HcaNeighborhoodView *neighborhood = nullptr;
  if (OB_SUCCESS != input_centers.get_init_ret()) {
    ret = input_centers.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA refine input centers", K(ret));
  } else if (OB_SUCCESS != output_centers.get_init_ret()) {
    ret = output_centers.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA refine output centers", K(ret));
  } else if (OB_SUCCESS != input_assignments.get_init_ret()) {
    ret = input_assignments.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA refine assignments", K(ret));
  } else if (!is_inited_ || all_vids.count() != assignments.count()
      || OB_FAIL(load_centers(
          centers, center_count, dimension, input_centers))) {
    if (OB_SUCCESS == ret) {
      ret = OB_INVALID_ARGUMENT;
    }
    LOG_WARN("invalid IVF HCA refine-training input", K(ret), K(center_count),
        K(dimension), "vector_count", all_vids.count(),
        "assignment_count", assignments.count());
  } else if (OB_FAIL(load_assignments(assignments, input_assignments))) {
    LOG_WARN("failed to load IVF HCA refine assignments", K(ret),
        "assignment_count", assignments.count());
  } else if (OB_FAIL(build_neighborhood_view(
      neighbor_graph, input_centers, allocator, neighborhood))) {
    LOG_WARN("failed to build IVF HCA refine neighborhood", K(ret),
        K(center_count));
  } else if (OB_FAIL(share::hca_refine_train_centers(
      vectors, input_centers, input_assignments, neighborhood,
      param_.refine_param_, param_.center_norm_info_, output_centers,
      allocator))) {
    LOG_WARN("failed to train refined IVF HCA centers", K(ret), K(center_count),
        "vector_count", all_vids.count());
  } else {
    ret = append_centers(output_centers, working_centers);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append refined IVF HCA centers", K(ret),
          K(center_count));
    }
  }
  LOG_INFO("IVF refine sample reads", K(ret), "batch_reads", cache.batch_reads(),
      "single_reads", cache.single_reads(), "loaded_vectors", cache.loaded_vectors(),
      "batch_read_us", cache.batch_read_us(),
      "cache_memory_hold", cache.memory_hold());
  return ret;
}

int ObIVFHcaAlgorithmAdapter::refine_train_begin(ObIVFVectorReader &reader,
    const ObIArray<ObIVFVectorVID> &vids, const ObIArray<float> &centers,
    const ObIArray<int64_t> &assignments, const ObIVFNeighborGraphView *graph,
    int64_t center_count, ObIArray<float> &working_centers)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || refine_session_ != nullptr) {
    LOG_WARN("invalid IVF refine session state", "ret", OB_STATE_NOT_MATCH, K(is_inited_));
    return OB_STATE_NOT_MATCH;
  }
  if (center_count <= 0 || vids.count() != assignments.count()
      || (graph != nullptr && !graph->is_valid())) {
    LOG_WARN("invalid IVF refine training arguments", "ret", OB_INVALID_ARGUMENT, K(center_count),
        K(vids.count()), K(assignments.count()));
    return OB_INVALID_ARGUMENT;
  }
  refine_session_ = OB_NEWx(ObIVFRefineSession, &refine_allocator_,
      reader, vids, center_count, graph, refine_allocator_);
  if (refine_session_ == nullptr) {
    LOG_WARN("failed to allocate IVF refine session", "ret", OB_ALLOCATE_MEMORY_FAILED, K(center_count));
    return OB_ALLOCATE_MEMORY_FAILED;
  }
  ObIVFRefineSession &s = *refine_session_;
  if (OB_FAIL(s.input_.get_init_ret())) {
    LOG_WARN("failed to initialize IVF refine input centers", K(ret), K(center_count));
  } else if (OB_FAIL(s.working_.get_init_ret())) {
    LOG_WARN("failed to initialize IVF refine working centers", K(ret), K(center_count));
  } else if (OB_FAIL(s.assignments_.get_init_ret())) {
    LOG_WARN("failed to initialize IVF refine assignments", K(ret), K(vids.count()));
  } else if (OB_FAIL(load_centers(centers, center_count, reader.get_dimension(), s.input_))) {
    LOG_WARN("failed to load IVF refine input centers", K(ret), K(center_count), K(reader.get_dimension()));
  } else if (OB_FAIL(load_assignments(assignments, s.assignments_))) {
    LOG_WARN("failed to load IVF refine input assignments", K(ret), K(assignments.count()));
  } else if (OB_FAIL(s.trainer_.init(s.vectors_, s.input_, s.assignments_,
      graph == nullptr ? nullptr : &s.neighbors_, param_.refine_param_,
      param_.center_norm_info_, s.working_))) {
    LOG_WARN("failed to initialize IVF parallel refine trainer", K(ret), K(center_count));
  } else if (OB_FAIL(s.prepare())) {
    LOG_WARN("failed to prepare initial IVF refine batch", K(ret), K(center_count));
  } else {
    working_centers.reuse();
    ret = append_centers(s.working_, working_centers);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to publish initial IVF refine working centers", K(ret), K(center_count));
    }
  }
  if (OB_FAIL(ret)) release_refine();
  return ret;
}

int64_t ObIVFHcaAlgorithmAdapter::refine_train_task_count() const
{ return refine_session_ == nullptr ? 0 : refine_session_->task_count(); }

int ObIVFHcaAlgorithmAdapter::refine_train_distance_task(int64_t task_idx)
{
  if (refine_session_ == nullptr) {
    int ret = OB_NOT_INIT;
    LOG_WARN("IVF refine session is not initialized", K(ret));
    return OB_NOT_INIT;
  }
  ObIVFRefineSession &s = *refine_session_;
  const int64_t count = s.task_count();
  if (task_idx < 0 || task_idx >= count) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF refine task index", K(ret), K(task_idx), K(count));
    return OB_INVALID_ARGUMENT;
  }
  const int64_t begin = s.rows_ * task_idx / count;
  const int64_t end = s.rows_ * (task_idx + 1) / count;
  int ret = OB_SUCCESS;
  for (int64_t row = begin; OB_SUCC(ret) && row < end; row += 32) {
    if (OB_FAIL(s.trainer_.distance_range(row, MIN(end, row + 32)))) {
      LOG_WARN("failed to calculate IVF refine task distances", K(ret), K(task_idx), K(row), K(end));
    } else if (OB_FAIL(share::dag_yield())) {
      LOG_WARN("failed to yield IVF merge task", K(ret));
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::refine_train_finish_batch(
    ObIArray<float> &centers, bool &finished)
{
  int ret = OB_SUCCESS;
  finished = false;
  if (refine_session_ == nullptr) {
    LOG_WARN("IVF refine session is not initialized", "ret", OB_NOT_INIT);
    return OB_NOT_INIT;
  }
  ObIVFRefineSession &s = *refine_session_;
  if (OB_FAIL(s.trainer_.finish_batch())) {
    LOG_WARN("failed to finish IVF parallel refine batch", K(ret), K(s.rows_));
  } else if (OB_FAIL(s.prepare())) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      centers.reuse();
      if (OB_FAIL(append_centers(s.working_, centers))) {
        LOG_WARN("failed to publish refined centers", K(ret));
      } else {
        finished = true;
      }
      LOG_INFO("IVF parallel refine sample reads", K(ret), "batch_reads", s.cache_.batch_reads(),
          "single_reads", s.cache_.single_reads(), "loaded_vectors", s.cache_.loaded_vectors(),
          "batch_read_us", s.cache_.batch_read_us(), "cache_memory_hold", s.cache_.memory_hold());
      release_refine();
    } else {
      LOG_WARN("failed to prepare next IVF refine batch", K(ret), K(s.rows_));
    }
  }
  return ret;
}

void ObIVFHcaAlgorithmAdapter::release_refine()
{
  // Release after the owning DAG has drained its workers.
  if (refine_session_ != nullptr) {
    refine_session_->~ObIVFRefineSession();
    refine_allocator_.free(refine_session_);
    refine_session_ = nullptr;
  }
  refine_allocator_.reset();
}

int ObIVFHcaAlgorithmAdapter::refine_assign_vectors(
    ObIVFVectorReader &reader,
    const common::ObIArray<ObIVFVectorVID> &all_vids,
    const common::ObIArray<float> &working_centers,
    const common::ObIArray<int64_t> &old_assignments,
    const ObIVFNeighborGraphView *neighbor_graph,
    const int64_t center_count,
    const int64_t vid_begin,
    const int64_t vid_end,
    common::ObIArray<int64_t> &new_cids)
{
  int ret = OB_SUCCESS;
  new_cids.reset();
  const int64_t dimension = reader.get_dimension();
  HcaVectorCache cache(reader, all_vids);
  HcaVectorBatch vectors(cache, vid_begin, vid_end, vid_begin);
  HcaCenterBuffer centers(center_count, dimension);
  HcaAssignmentBuffer input_assignments(old_assignments.count());
  HcaAssignmentBuffer output_assignments(all_vids.count());
  HcaMemberRows output_members(center_count);
  HcaChangedSet changed_cids(center_count);
  ObArenaAllocator allocator(
      "IVFRefineAsgn", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  HcaNeighborhoodView *neighborhood = nullptr;
  if (OB_SUCCESS != centers.get_init_ret()) {
    ret = centers.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA assignment centers", K(ret));
  } else if (OB_SUCCESS != input_assignments.get_init_ret()) {
    ret = input_assignments.get_init_ret();
    LOG_WARN("failed to initialize old IVF HCA assignment buffer", K(ret));
  } else if (OB_SUCCESS != output_assignments.get_init_ret()) {
    ret = output_assignments.get_init_ret();
    LOG_WARN("failed to initialize new IVF HCA assignment buffer", K(ret));
  } else if (OB_SUCCESS != output_members.get_init_ret()) {
    ret = output_members.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA assignment members", K(ret));
  } else if (OB_SUCCESS != changed_cids.get_init_ret()) {
    ret = changed_cids.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA changed-center set", K(ret));
  } else if (!is_inited_ || all_vids.count() != old_assignments.count()
      || vid_begin < 0 || vid_end <= vid_begin || vid_end > all_vids.count()
      || OB_FAIL(load_centers(
          working_centers, center_count, dimension, centers))) {
    if (OB_SUCCESS == ret) {
      ret = OB_INVALID_ARGUMENT;
    }
    LOG_WARN("invalid IVF HCA refine-assignment input", K(ret), K(center_count),
        K(dimension), K(vid_begin), K(vid_end),
        "vector_count", all_vids.count(),
        "assignment_count", old_assignments.count());
  } else if (OB_FAIL(cache.prefetch(vid_begin, vid_end))) {
    LOG_WARN("failed to prefetch IVF HCA refine vectors", K(ret),
        K(vid_begin), K(vid_end));
  } else if (OB_FAIL(load_assignments(
      old_assignments, input_assignments))) {
    LOG_WARN("failed to load old IVF HCA assignments", K(ret),
        "assignment_count", old_assignments.count());
  } else if (OB_FAIL(build_neighborhood_view(
      neighbor_graph, centers, allocator, neighborhood))) {
    LOG_WARN("failed to build IVF HCA assignment neighborhood", K(ret),
        K(center_count));
  } else if (OB_FAIL(share::hca_refine_assign_range(
      vectors, centers, input_assignments, neighborhood,
      output_assignments, output_members, changed_cids))) {
    LOG_WARN("failed to refine IVF HCA assignments", K(ret), K(vid_begin),
        K(vid_end), K(center_count));
  }
  for (int64_t idx = vid_begin; OB_SUCC(ret) && idx < vid_end; ++idx) {
    int32_t cid = -1;
    if (OB_FAIL(output_assignments.get_assignment(idx, cid))) {
      LOG_WARN("failed to get refined IVF HCA assignment", K(ret), K(idx));
    } else {
      ret = new_cids.push_back(cid);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to append refined IVF HCA assignment", K(ret),
            K(idx), K(cid));
      }
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::refine_update_center(
    ObIVFVectorReader &reader,
    const common::ObIArray<ObIVFVectorVID> &member_vids,
    const common::ObIArray<float> &working_centers,
    const int64_t center_count,
    const int64_t cid,
    common::ObIArray<float> &updated_center)
{
  int ret = OB_SUCCESS;
  updated_center.reset();
  const int64_t dimension = reader.get_dimension();
  HcaVectorCache cache(reader, member_vids);
  HcaVectorAccessor vectors(cache);
  // One task recomputes one global CID. Map it to local CID 0 so scratch
  // space scales with dimension/member count, not with all K centers.
  // The finish barrier still installs the result at the original global CID.
  const int64_t local_cid = 0;
  HcaCenterBuffer centers(1, dimension);
  HcaMemberRows members(1);
  HcaChangedSet changed_cids(1);
  ObArenaAllocator allocator(
      "IVFRefineUpd", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_SUCCESS != centers.get_init_ret()) {
    ret = centers.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA center-update centers", K(ret));
  } else if (OB_SUCCESS != members.get_init_ret()) {
    ret = members.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA center-update members", K(ret));
  } else if (OB_SUCCESS != changed_cids.get_init_ret()) {
    ret = changed_cids.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA center-update changed set", K(ret));
  } else if (center_count > INT64_MAX / dimension) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF HCA center-update shape overflows", K(ret), K(center_count), K(dimension));
  } else if (!is_inited_ || member_vids.empty() || center_count <= 0
      || cid < 0 || cid >= center_count
      || working_centers.count() != center_count * dimension
      || OB_FAIL(centers.set_center(local_cid, &working_centers.at(cid * dimension)))) {
    if (OB_SUCCESS == ret) {
      ret = OB_INVALID_ARGUMENT;
    }
    LOG_WARN("invalid IVF HCA center-update input", K(ret), K(center_count),
        K(cid), K(dimension), "member_count", member_vids.count());
  } else if (OB_FAIL(cache.prefetch(0, member_vids.count()))) {
    LOG_WARN("failed to prefetch IVF HCA center-update vectors", K(ret),
        K(cid), "member_count", member_vids.count());
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < member_vids.count(); ++idx) {
    ret = members.append_member(local_cid, idx);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append IVF HCA center-update member", K(ret),
          K(cid), K(idx));
    }
  }
  if (OB_SUCC(ret)) {
    ret = changed_cids.set_changed(local_cid);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to mark changed IVF HCA center", K(ret), K(cid));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(share::hca_refine_update_centers_range(
      vectors, members, changed_cids, local_cid, local_cid + 1,
      param_.center_norm_info_, centers, allocator))) {
    LOG_WARN("failed to update refined IVF HCA center", K(ret), K(cid));
  }
  const float *center = nullptr;
  if (OB_SUCC(ret) && OB_FAIL(centers.get_center(local_cid, center))) {
    LOG_WARN("failed to get updated IVF HCA center", K(ret), K(cid));
  }
  for (int64_t d = 0; OB_SUCC(ret) && d < dimension; ++d) {
    ret = updated_center.push_back(center[d]);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append updated IVF HCA center value", K(ret),
          K(cid), K(d));
    }
  }
  return ret;
}

int ObIVFHcaAlgorithmAdapter::soar_assign_secondary(
    ObIVFVectorReader &reader,
    const common::ObIArray<ObIVFVectorVID> &all_vids,
    const common::ObIArray<float> &final_centers,
    const common::ObIArray<int64_t> &primary_cids,
    const ObIVFNeighborGraphView *neighbor_graph,
    const int64_t center_count,
    const int64_t vid_begin,
    const int64_t vid_end,
    common::ObIArray<int64_t> &secondary_cids)
{
  int ret = OB_SUCCESS;
  secondary_cids.reset();
  const int64_t dimension = reader.get_dimension();
  if (!is_inited_ || dimension <= 0 || center_count <= 0
      || vid_begin < 0 || vid_end <= vid_begin || vid_end > all_vids.count()
      || primary_cids.count() != vid_end - vid_begin) {
    LOG_WARN("invalid IVF SOAR assignment arguments", "ret", OB_INVALID_ARGUMENT, K(vid_begin), K(vid_end),
        K(center_count), K(dimension), K(primary_cids.count()));
    return OB_INVALID_ARGUMENT;
  } else if (center_count > INT64_MAX / dimension) {
    LOG_WARN("IVF SOAR center size overflow", "ret", OB_SIZE_OVERFLOW, K(center_count), K(dimension));
    return OB_SIZE_OVERFLOW;
  } else if (final_centers.count() != center_count * dimension) {
    LOG_WARN("IVF SOAR center size mismatch", "ret", OB_INVALID_ARGUMENT, K(center_count), K(dimension),
        K(final_centers.count()));
    return OB_INVALID_ARGUMENT;
  }
  const int64_t range_count = vid_end - vid_begin;
  HcaVectorCache cache(reader, all_vids);
  // SOAR receives range-local primary_cids from the FULL_REBUILD seam, so the
  // HCA vector_idx exposed here is also range-local. The source vector still
  // comes from all_vids[vid_begin + pos].
  HcaVectorBatch vectors(cache, vid_begin, vid_end, 0);
  HcaCenterArrayView centers(final_centers, dimension);
  HcaAssignmentBuffer refined_assignments(range_count);
  HcaAssignmentBuffer primary_assignments(range_count);
  HcaAssignmentBuffer output_secondary(range_count);
  HcaUnusedMemberOutput secondary_members(center_count);
  HcaIdentityRemap identity_remap(center_count);
  ObArenaAllocator allocator(
      "IVFHcaSoar", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  HcaNeighborhoodView *neighborhood = nullptr;
  if (OB_SUCCESS != refined_assignments.get_init_ret()) {
    ret = refined_assignments.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA SOAR refined assignments", K(ret));
  } else if (OB_SUCCESS != primary_assignments.get_init_ret()) {
    ret = primary_assignments.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA SOAR primary assignments", K(ret));
  } else if (OB_SUCCESS != output_secondary.get_init_ret()) {
    ret = output_secondary.get_init_ret();
    LOG_WARN("failed to initialize IVF HCA SOAR secondary assignments", K(ret));
  } else if (OB_FAIL(cache.prefetch(vid_begin, vid_end))) {
    LOG_WARN("failed to prefetch IVF HCA SOAR vectors", K(ret),
        K(vid_begin), K(vid_end));
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < range_count; ++idx) {
    if (primary_cids.at(idx) < 0 || primary_cids.at(idx) > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid primary IVF HCA assignment for SOAR", K(ret), K(idx),
          K(primary_cids.at(idx)));
    } else {
      ret = refined_assignments.set_assignment(
          idx, static_cast<int32_t>(primary_cids.at(idx)));
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to set refined IVF HCA assignment for SOAR", K(ret),
            K(idx), K(primary_cids.at(idx)));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(build_neighborhood_view(
      neighbor_graph, centers, allocator, neighborhood))) {
    LOG_WARN("failed to build IVF HCA SOAR neighborhood", K(ret),
        K(center_count));
  } else if (OB_SUCC(ret) && OB_FAIL(share::hca_assign_soar_range(
      vectors, centers, refined_assignments, identity_remap,
      neighborhood, param_.soar_param_, primary_assignments,
      &output_secondary, &secondary_members, allocator))) {
    LOG_WARN("failed to assign IVF HCA SOAR secondary centers", K(ret),
        K(vid_begin), K(vid_end), K(center_count));
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < range_count; ++idx) {
    int32_t cid = -1;
    if (OB_FAIL(output_secondary.get_assignment(idx, cid))) {
      LOG_WARN("failed to get IVF HCA SOAR assignment", K(ret), K(idx));
    } else {
      ret = secondary_cids.push_back(cid);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to append IVF HCA SOAR assignment", K(ret),
            K(idx), K(cid));
      }
    }
  }
  return ret;
}

void ObIVFHcaAlgorithmAdapter::cancel(const int ret_code)
{
  ATOMIC_STORE(&refine_cancel_code_, ret_code == OB_SUCCESS ? OB_CANCELED : ret_code);
  // The processor stops new work and retains the canceled plan until DAG
  // teardown. In-flight tasks may still read HGraph/prior buffers, so their
  // owner releases them only after the tasks have drained, in the destructor.
  // The same lifetime rule applies to the current refine batch.
}

void ObIVFHcaAlgorithmAdapter::release_hgraph()
{
  if (OB_NOT_NULL(hgraph_)) {
    common::obvectorutil::delete_index(hgraph_);
    hgraph_ = nullptr;
  }
}

} // namespace storage
} // namespace oceanbase
