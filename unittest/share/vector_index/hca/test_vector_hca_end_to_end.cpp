/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "common/ob_target_specific.h"
#include "lib/utility/ob_sort.h"
#include "lib/vector/ob_vector_util.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_type/ob_vector_cosine_distance.h"
#include "share/vector_type/ob_vector_ip_distance.h"
#include "share/vector_index/hca/ob_vector_hca_cluster.h"
#include "share/vector_index/hca/ob_vector_hca_neighborhood.h"
#include "share/vector_index/hca/ob_vector_hca_refine.h"
#include "share/vector_index/hca/ob_vector_hca_soar.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace oceanbase {
namespace share {
namespace {

enum class TestDistanceType {
  L2,
  COSINE,
  INNER_PRODUCT,
};

const char *get_test_distance_name(const TestDistanceType distance_type) {
  const char *name = "unknown";
  switch (distance_type) {
  case TestDistanceType::L2:
    name = "l2";
    break;
  case TestDistanceType::COSINE:
    name = "cosine";
    break;
  case TestDistanceType::INNER_PRODUCT:
    name = "inner_product";
    break;
  }
  return name;
}

/**
 * Exact query distance used by Flat ground truth and center ranking.
 * Inner product is negated so every test distance keeps smaller-is-nearer
 * ordering.
 */
int get_test_distance(const float *left, const float *right,
                      const int64_t dimension,
                      const TestDistanceType distance_type, float &distance) {
  int ret = OB_SUCCESS;
  double value = 0.0;
  if (OB_ISNULL(left) || OB_ISNULL(right) || dimension <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    switch (distance_type) {
    case TestDistanceType::L2:
      distance = hca_square_distance(left, right, dimension);
      break;
    case TestDistanceType::COSINE:
      if (OB_FAIL(common::ObVectorCosineDistance<float>::cosine_distance_func(
              left, right, dimension, value))) {
      } else {
        distance = static_cast<float>(value);
      }
      break;
    case TestDistanceType::INNER_PRODUCT:
      if (OB_FAIL(common::ObVectorIpDistance<float>::ip_distance_func(
              left, right, dimension, value))) {
      } else {
        distance = static_cast<float>(-value);
      }
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      break;
    }
  }
  return ret;
}

/**
 * Real VSAG integration tests need an allocator whose lifetime covers the
 * shared index. The C allocation functions are safe for calls on independent
 * blocks from concurrent Add workers.
 */
class TestVsagAllocator : public vsag::Allocator {
public:
  static constexpr int64_t HEADER_SIZE = sizeof(int64_t);
  TestVsagAllocator() : outstanding_bytes_(0) {}
  std::string Name() override { return "HcaTestVsagAllocator"; }
  void *Allocate(const size_t size) override {
    void *result = nullptr;
    if (size > 0 && size <= SIZE_MAX - HEADER_SIZE) {
      const int64_t actual_size = static_cast<int64_t>(size + HEADER_SIZE);
      char *raw = static_cast<char *>(std::malloc(actual_size));
      if (nullptr != raw) {
        *reinterpret_cast<int64_t *>(raw) = actual_size;
        outstanding_bytes_.fetch_add(actual_size);
        result = raw + HEADER_SIZE;
      }
    }
    return result;
  }
  void Deallocate(void *pointer) override {
    if (nullptr != pointer) {
      char *raw = static_cast<char *>(pointer) - HEADER_SIZE;
      outstanding_bytes_.fetch_sub(*reinterpret_cast<int64_t *>(raw));
      std::free(raw);
    }
  }
  void *Reallocate(void *pointer, const size_t size) override {
    void *result = nullptr;
    if (0 == size) {
      Deallocate(pointer);
    } else if (nullptr == pointer) {
      result = Allocate(size);
    } else {
      char *raw = static_cast<char *>(pointer) - HEADER_SIZE;
      const int64_t old_size = *reinterpret_cast<int64_t *>(raw) - HEADER_SIZE;
      if (old_size >= static_cast<int64_t>(size)) {
        result = pointer;
      } else if (nullptr != (result = Allocate(size))) {
        MEMCPY(result, pointer, old_size);
        Deallocate(pointer);
      }
    }
    return result;
  }
  int64_t outstanding_bytes() const { return outstanding_bytes_.load(); }

private:
  std::atomic<int64_t> outstanding_bytes_;
};

/** Ensures a real VSAG index is released on every ASSERT/return path. */
class TestVsagIndexGuard {
public:
  explicit TestVsagIndexGuard(common::obvsag::VectorIndexPtr &index)
      : index_(index) {}
  ~TestVsagIndexGuard() {
    if (OB_NOT_NULL(index_)) {
      common::obvectorutil::delete_index(index_);
    }
  }

private:
  common::obvsag::VectorIndexPtr &index_;
};

/** Releases adaptor-owned KNN result buffers on every assertion path. */
class TestVsagSearchResultGuard {
public:
  TestVsagSearchResultGuard(TestVsagAllocator &allocator,
                            const float *&distances, const int64_t *&ids)
      : allocator_(allocator), distances_(distances), ids_(ids) {}
  ~TestVsagSearchResultGuard() {
    allocator_.Deallocate(const_cast<float *>(distances_));
    allocator_.Deallocate(const_cast<int64_t *>(ids_));
    distances_ = nullptr;
    ids_ = nullptr;
  }

private:
  TestVsagAllocator &allocator_;
  const float *&distances_;
  const int64_t *&ids_;
};

/**
 * This file is both an end-to-end algorithm test and an example of the data
 * contracts that the future DDL/DAG coordinator must provide.
 *
 * The test supplies these upper-layer objects entirely in memory:
 *
 *   TestVectorStore       global vector_idx -> vector random access
 *   TestVectorBatch       task/node position -> vector_idx/vector view
 *   TestCenterBuffer      center reader/writer/mutable storage
 *   TestAssignmentBuffer  vector_idx -> cid mapping
 *   TestMemberRows        cid -> vector_idx[] and row move/bind operations
 *   TestCenterStats       cid -> full-data sum/count
 *   TestNeighborhood      cid -> ordered neighbors/max distance
 *   TestChangedBitmap     task/global changed-cid bitmap
 *   TestCidRemap          old cid -> compact cid/-1
 *   TestLeafResultWriter  C0/A0/InitialMemberLists publication contract
 *
 * run_serial_hierarchy() and TestHcaCoordinator replace the DDL coordinator.
 * The parallel version submits vector_idx/cid/node tasks to a fixed FIFO
 * thread pool and places a coordinator-owned barrier between dependent stages.
 * The production-order flow is unchanged:
 *
 *   Hierarchy -> Leaf Publish -> Neighborhood -> Refine -> Compact -> SOAR
 */

/**
 * Simulates the upper-layer full VectorStore.
 *
 * count()/dimension() describe the global data set. get_vector() provides
 * random access by global vector_idx for Center Stats and Refine.
 * set_vector() is test-only checked initialization support.
 */
class TestVectorStore : public ObHcaVectorAccessor {
public:
  TestVectorStore(const int64_t count, const int64_t dimension)
      : count_(count), dimension_(dimension), data_(count * dimension, 0.0F) {}

  int64_t count() const override { return count_; }
  int64_t dimension() const override { return dimension_; }

  int get_vector(const int64_t vector_idx,
                 const float *&vector) const override {
    int ret = OB_SUCCESS;
    if (vector_idx < 0 || vector_idx >= count_) {
      ret = OB_INVALID_ARGUMENT;
      vector = nullptr;
    } else {
      vector = data_.data() + vector_idx * dimension_;
    }
    return ret;
  }

  int set_vector(const int64_t vector_idx, const float *vector) {
    int ret = OB_SUCCESS;
    if (vector_idx < 0 || vector_idx >= count_ || OB_ISNULL(vector)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(data_.data() + vector_idx * dimension_, vector,
             sizeof(float) * dimension_);
    }
    return ret;
  }

private:
  int64_t count_;
  int64_t dimension_;
  std::vector<float> data_;
};

/**
 * Simulates a hierarchy node or task-local vector range.
 *
 * get_vector(pos, ...) maps a contiguous local position to a potentially
 * non-contiguous global vector_idx and vector pointer. Local Train and range
 * Assignment consume this contract.
 */
class TestVectorBatch : public ObHcaVectorBatch {
public:
  TestVectorBatch(const TestVectorStore &store,
                  const std::vector<int64_t> &vector_indices)
      : TestVectorBatch(store, vector_indices, 0,
                        static_cast<int64_t>(vector_indices.size())) {}

  TestVectorBatch(const TestVectorStore &store,
                  const std::vector<int64_t> &vector_indices,
                  const int64_t begin_pos, const int64_t end_pos)
      : store_(store), vector_indices_(vector_indices), begin_pos_(begin_pos),
        end_pos_(end_pos) {}

  int64_t count() const override { return end_pos_ - begin_pos_; }
  int64_t dimension() const override { return store_.dimension(); }

  int get_vector(const int64_t pos, int64_t &vector_idx,
                 const float *&vector) const override {
    int ret = OB_SUCCESS;
    if (pos < 0 || pos >= count()) {
      ret = OB_INVALID_ARGUMENT;
      vector_idx = -1;
      vector = nullptr;
    } else {
      vector_idx = vector_indices_[begin_pos_ + pos];
      ret = store_.get_vector(vector_idx, vector);
    }
    return ret;
  }

private:
  const TestVectorStore &store_;
  const std::vector<int64_t> &vector_indices_;
  int64_t begin_pos_;
  int64_t end_pos_;
};

/**
 * Simulates center storage for provisional centers, C0, WorkingCenters and C1.
 *
 * get_center() reads committed rows. set_center() copies/commits one row and
 * advances count. get_mutable_center() exposes preallocated output memory used
 * by Finalize and Refine. capacity() supports pre-write bounds validation.
 */
class TestCenterBuffer : public ObHcaCenterStore {
public:
  TestCenterBuffer(const int64_t capacity, const int64_t dimension)
      : capacity_(capacity), dimension_(dimension), count_(0),
        data_(capacity * dimension, 0.0F) {}

  int64_t count() const override { return count_.load(); }
  int64_t capacity() const override { return capacity_; }
  int64_t dimension() const override { return dimension_; }

  int get_center(const int64_t cid, const float *&center) const override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= count()) {
      ret = OB_INVALID_ARGUMENT;
      center = nullptr;
    } else {
      center = data_.data() + cid * dimension_;
    }
    return ret;
  }

  int get_mutable_center(const int64_t cid, float *&center) override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= capacity_) {
      ret = OB_INVALID_ARGUMENT;
      center = nullptr;
    } else {
      center = data_.data() + cid * dimension_;
    }
    return ret;
  }

  int set_center(const int64_t cid, const float *center) override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= capacity_ || OB_ISNULL(center)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      float *destination = data_.data() + cid * dimension_;
      if (destination != center) {
        MEMCPY(destination, center, sizeof(float) * dimension_);
      }
      int64_t current_count = count_.load();
      while (current_count < cid + 1 &&
             !count_.compare_exchange_weak(current_count, cid + 1)) {
      }
    }
    return ret;
  }

private:
  int64_t capacity_;
  int64_t dimension_;
  std::atomic<int64_t> count_;
  std::vector<float> data_;
};

/**
 * Simulates dense A0/RefinedAssignments/A1/A2 storage.
 * Refine/SOAR use get_assignment(); Assignment/remap stages use
 * set_assignment().
 */
class TestAssignmentBuffer : public ObHcaAssignmentStore {
public:
  explicit TestAssignmentBuffer(const int64_t count)
      : assignments_(count, -1) {}

  int64_t count() const override {
    return static_cast<int64_t>(assignments_.size());
  }

  int get_assignment(const int64_t vector_idx, int32_t &cid) const override {
    int ret = OB_SUCCESS;
    if (vector_idx < 0 || vector_idx >= count()) {
      ret = OB_INVALID_ARGUMENT;
      cid = -1;
    } else {
      cid = assignments_[vector_idx];
    }
    return ret;
  }

  int set_assignment(const int64_t vector_idx, const int32_t cid) override {
    int ret = OB_SUCCESS;
    if (vector_idx < 0 || vector_idx >= count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      assignments_[vector_idx] = cid;
    }
    return ret;
  }

private:
  std::vector<int32_t> assignments_;
};

/**
 * Simulates the upper-layer cid-to-members matrix.
 *
 * append_member() is used by Assignment, get_member() by Stats/Center Update,
 * move_row() by local empty-cid compaction, and bind_row() by Leaf Publish and
 * final Compact. Assignment tasks may append to the same cid concurrently, so
 * the adapter protects each row independently. Readers run only after the
 * assignment barrier.
 */
class TestMemberRows : public ObHcaCidMemberStore {
public:
  explicit TestMemberRows(const int64_t cid_count)
      : rows_(cid_count), row_locks_(cid_count) {}

  int64_t cid_count() const override {
    return static_cast<int64_t>(rows_.size());
  }

  int64_t member_count(const int64_t cid) const override {
    return cid >= 0 && cid < cid_count()
               ? static_cast<int64_t>(rows_[cid].size())
               : -1;
  }

  int get_member(const int64_t cid, const int64_t offset,
                 int64_t &vector_idx) const override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= cid_count() || offset < 0 ||
        offset >= member_count(cid)) {
      ret = OB_INVALID_ARGUMENT;
      vector_idx = -1;
    } else {
      vector_idx = rows_[cid][offset];
    }
    return ret;
  }

  int append_member(const int32_t cid, const int64_t vector_idx) override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= cid_count() || vector_idx < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      std::lock_guard<std::mutex> guard(row_locks_[cid]);
      rows_[cid].push_back(vector_idx);
    }
    return ret;
  }

  int move_row(const int64_t old_cid, const int64_t new_cid) override {
    int ret = OB_SUCCESS;
    if (old_cid < 0 || old_cid >= cid_count() || new_cid < 0 ||
        new_cid >= cid_count()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (old_cid != new_cid) {
      rows_[new_cid] = std::move(rows_[old_cid]);
      rows_[old_cid].clear();
    }
    return ret;
  }

  int bind_row(const int64_t output_cid,
               const ObHcaCidMemberStore &input_members,
               const int64_t input_cid) override {
    int ret = OB_SUCCESS;
    if (output_cid < 0 || output_cid >= cid_count() || input_cid < 0 ||
        input_cid >= input_members.cid_count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      rows_[output_cid].clear();
      for (int64_t offset = 0;
           OB_SUCC(ret) && offset < input_members.member_count(input_cid);
           ++offset) {
        int64_t vector_idx = -1;
        if (OB_FAIL(input_members.get_member(input_cid, offset, vector_idx))) {
        } else {
          rows_[output_cid].push_back(vector_idx);
        }
      }
    }
    return ret;
  }

private:
  std::vector<std::vector<int64_t>> rows_;
  std::deque<std::mutex> row_locks_;
};

/**
 * Simulates GlobalCenterStats. set_stats() deep-copies one complete sum/count;
 * get_stats() exposes it to Finalize after the external cid-range barrier.
 */
class TestCenterStats : public ObHcaCenterStatsStore {
public:
  TestCenterStats(const int64_t count, const int64_t dimension)
      : count_(count), dimension_(dimension), sums_(count * dimension, 0.0F),
        member_counts_(count, -1) {}

  int64_t count() const override { return count_; }
  int64_t dimension() const override { return dimension_; }

  int get_stats(const int64_t cid, const float *&sum,
                int64_t &member_count) const override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= count_) {
      ret = OB_INVALID_ARGUMENT;
      sum = nullptr;
      member_count = -1;
    } else {
      sum = sums_.data() + cid * dimension_;
      member_count = member_counts_[cid];
    }
    return ret;
  }

  int set_stats(const int64_t cid, const float *sum,
                const int64_t member_count) override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= count_ || OB_ISNULL(sum) || member_count < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(sums_.data() + cid * dimension_, sum, sizeof(float) * dimension_);
      member_counts_[cid] = member_count;
    }
    return ret;
  }

private:
  int64_t count_;
  int64_t dimension_;
  std::vector<float> sums_;
  std::vector<int64_t> member_counts_;
};

/**
 * Simulates G0/G1. set_row() publishes one complete row. Accessor methods feed
 * ordered neighbors and max_intra_distance to Refine and SOAR.
 */
class TestNeighborhood : public ObHcaNeighborhoodStore {
public:
  explicit TestNeighborhood(const int64_t center_count)
      : rows_(center_count), max_distances_(center_count, 0.0F),
        written_(center_count, 0) {}

  int64_t center_count() const override {
    return static_cast<int64_t>(rows_.size());
  }

  int64_t neighbor_count(const int64_t cid) const override {
    return cid >= 0 && cid < center_count()
               ? static_cast<int64_t>(rows_[cid].size())
               : -1;
  }

  int get_neighbor(const int64_t cid, const int64_t offset,
                   int32_t &neighbor_cid) const override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= center_count() || offset < 0 ||
        offset >= neighbor_count(cid)) {
      ret = OB_INVALID_ARGUMENT;
      neighbor_cid = -1;
    } else {
      neighbor_cid = rows_[cid][offset];
    }
    return ret;
  }

  int get_max_intra_distance(const int64_t cid,
                             float &distance) const override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= center_count() || !written_[cid]) {
      ret = OB_INVALID_ARGUMENT;
      distance = 0.0F;
    } else {
      distance = max_distances_[cid];
    }
    return ret;
  }

  int set_row(const int64_t cid, const int32_t *neighbor_cids,
              const int64_t neighbor_count,
              const float max_intra_distance) override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= center_count() || neighbor_count < 0 ||
        (neighbor_count > 0 && OB_ISNULL(neighbor_cids))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      rows_[cid].clear();
      if (neighbor_count > 0) {
        rows_[cid].assign(neighbor_cids, neighbor_cids + neighbor_count);
      }
      max_distances_[cid] = max_intra_distance;
      written_[cid] = 1;
    }
    return ret;
  }

  bool is_written(const int64_t cid) const { return written_[cid]; }

private:
  std::vector<std::vector<int32_t>> rows_;
  std::vector<float> max_distances_;
  // vector<bool> packs multiple cids into one machine word and would race when
  // different neighborhood tasks publish adjacent rows.
  std::vector<uint8_t> written_;
};

/**
 * Simulates the changed-cid bitmap. Assignment marks both migration endpoints;
 * Center Update reads the merged result after the conceptual barrier.
 */
class TestChangedBitmap : public ObHcaChangedCidSet {
public:
  explicit TestChangedBitmap(const int64_t count) : changed_(count, 0) {}

  bool has_changed(const int64_t cid) const override {
    return cid >= 0 && cid < static_cast<int64_t>(changed_.size()) &&
           changed_[cid];
  }

  int set_changed(const int64_t cid) override {
    int ret = OB_SUCCESS;
    if (cid < 0 || cid >= static_cast<int64_t>(changed_.size())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      changed_[cid] = 1;
    }
    return ret;
  }

  int merge_from(const TestChangedBitmap &other) {
    int ret = OB_SUCCESS;
    if (changed_.size() != other.changed_.size()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      for (int64_t cid = 0; cid < static_cast<int64_t>(changed_.size());
           ++cid) {
        if (other.has_changed(cid)) {
          changed_[cid] = 1;
        }
      }
    }
    return ret;
  }

private:
  std::vector<uint8_t> changed_;
};

/**
 * Simulates old_to_new. Compact writes it and SOAR reads it to map pre-compact
 * RefinedAssignments into final A1 cids.
 */
class TestCidRemap : public ObHcaCidRemapStore {
public:
  explicit TestCidRemap(const int64_t count) : remap_(count, -2) {}

  int64_t count() const override { return static_cast<int64_t>(remap_.size()); }

  int get_new_cid(const int64_t old_cid, int32_t &new_cid) const override {
    int ret = OB_SUCCESS;
    if (old_cid < 0 || old_cid >= count()) {
      ret = OB_INVALID_ARGUMENT;
      new_cid = -1;
    } else {
      new_cid = remap_[old_cid];
    }
    return ret;
  }

  int set_new_cid(const int64_t old_cid, const int32_t new_cid) override {
    int ret = OB_SUCCESS;
    if (old_cid < 0 || old_cid >= count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      remap_[old_cid] = new_cid;
    }
    return ret;
  }

private:
  std::vector<int32_t> remap_;
};

/**
 * Simulates the coordinator's atomic leaf publication operation.
 *
 * One call allocates the next global cid, copies the center into C0, binds the
 * member row into InitialMemberLists and writes A0 for every member. A
 * single lock makes global-cid allocation and the three publications atomic
 * when different hierarchy-node tasks finish concurrently.
 */
class TestLeafResultWriter : public ObHcaLeafResultWriter {
public:
  TestLeafResultWriter(TestCenterBuffer &centers,
                       TestAssignmentBuffer &assignments,
                       TestMemberRows &members)
      : centers_(centers), assignments_(assignments), members_(members) {}

  int publish(const float *leaf_center, const int64_t dimension,
              const ObHcaCidMemberStore &leaf_members,
              const int64_t leaf_cid, int32_t &global_cid) override {
    int ret = OB_SUCCESS;
    std::lock_guard<std::mutex> guard(publish_lock_);
    global_cid = static_cast<int32_t>(centers_.count());
    if (dimension != centers_.dimension() ||
        global_cid >= centers_.capacity()) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(centers_.set_center(global_cid, leaf_center))) {
    } else if (OB_FAIL(members_.bind_row(global_cid, leaf_members, leaf_cid))) {
    }
    for (int64_t offset = 0;
         OB_SUCC(ret) && offset < members_.member_count(global_cid); ++offset) {
      int64_t vector_idx = -1;
      if (OB_FAIL(members_.get_member(global_cid, offset, vector_idx))) {
      } else if (OB_FAIL(assignments_.set_assignment(vector_idx, global_cid))) {
      }
    }
    return ret;
  }

private:
  TestCenterBuffer &centers_;
  TestAssignmentBuffer &assignments_;
  TestMemberRows &members_;
  std::mutex publish_lock_;
};

static const int64_t TEST_PARALLEL_WORKER_COUNT = 4;

/**
 * Counts one coordinator stage's submitted tasks. Workers publish the first
 * error and always decrement pending_count; only the coordinator calls wait().
 */
class TestTaskGroup {
public:
  TestTaskGroup()
      : pending_count_(0), first_error_(OB_SUCCESS), wait_started_(false) {}

  int add_task() {
    int ret = OB_SUCCESS;
    std::lock_guard<std::mutex> guard(lock_);
    if (wait_started_) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      ++pending_count_;
    }
    return ret;
  }

  void complete(const int task_ret) {
    std::lock_guard<std::mutex> guard(lock_);
    if (OB_SUCCESS == first_error_ && OB_SUCCESS != task_ret) {
      first_error_ = task_ret;
    }
    if (pending_count_ <= 0) {
      first_error_ = OB_ERR_UNEXPECTED;
    } else if (0 == --pending_count_) {
      condition_.notify_all();
    }
  }

  int wait() {
    std::unique_lock<std::mutex> guard(lock_);
    wait_started_ = true;
    condition_.wait(guard, [&]() { return 0 == pending_count_; });
    return first_error_;
  }

private:
  int64_t pending_count_;
  int first_error_;
  bool wait_started_;
  std::mutex lock_;
  std::condition_variable condition_;
};

/** One FIFO queue element executed by a thread-pool worker. */
class TestTask {
public:
  explicit TestTask(TestTaskGroup &group) : group_(group) {}
  virtual ~TestTask() {}
  virtual int process() = 0;
  void complete(const int ret) { group_.complete(ret); }

private:
  TestTaskGroup &group_;
};

/** Adapts a stage-specific range/node closure to the common task contract. */
class TestFunctionTask : public TestTask {
public:
  TestFunctionTask(TestTaskGroup &group, std::function<int()> function)
      : TestTask(group), function_(std::move(function)) {}
  int process() override { return function_(); }

private:
  std::function<int()> function_;
};

/**
 * Fixed worker pool backed by a normal FIFO queue.
 *
 * Workers never wait for child tasks or stage barriers. stop() drains submitted
 * work before joining the workers, and the destructor is only a safety net.
 */
class TestThreadPool {
public:
  TestThreadPool() : started_(false), stopping_(false) {}
  ~TestThreadPool() { stop(); }

  int start(const int64_t worker_count) {
    int ret = OB_SUCCESS;
    if (worker_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (started_) {
      ret = OB_INIT_TWICE;
    } else {
      started_ = true;
      stopping_ = false;
      workers_.reserve(worker_count);
      for (int64_t worker_idx = 0; worker_idx < worker_count; ++worker_idx) {
        workers_.emplace_back([this]() { worker_loop(); });
      }
    }
    return ret;
  }

  int submit(std::unique_ptr<TestTask> task) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(task.get())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      std::lock_guard<std::mutex> guard(lock_);
      if (!started_ || stopping_) {
        ret = OB_NOT_INIT;
      } else {
        task_queue_.push(std::move(task));
        condition_.notify_one();
      }
    }
    return ret;
  }

  void stop() {
    {
      std::lock_guard<std::mutex> guard(lock_);
      if (!started_) {
        return;
      }
      stopping_ = true;
      condition_.notify_all();
    }
    for (std::thread &worker : workers_) {
      worker.join();
    }
    workers_.clear();
    started_ = false;
  }

private:
  void worker_loop() {
    while (true) {
      std::unique_ptr<TestTask> task;
      {
        std::unique_lock<std::mutex> guard(lock_);
        condition_.wait(guard,
                        [&]() { return stopping_ || !task_queue_.empty(); });
        if (stopping_ && task_queue_.empty()) {
          break;
        }
        task = std::move(task_queue_.front());
        task_queue_.pop();
      }
      const int ret = task->process();
      task->complete(ret);
    }
  }

private:
  bool started_;
  bool stopping_;
  std::queue<std::unique_ptr<TestTask>> task_queue_;
  std::vector<std::thread> workers_;
  std::mutex lock_;
  std::condition_variable condition_;
};

/**
 * Upper-layer scheduling model used by all parallel tests.
 *
 * task_idx identifies task-local state such as ChangedCids. begin/end are
 * positions in the caller's vector_idx array, cids, or node indexes; each call
 * documents its coordinate. wait() is reached only after all tasks are queued.
 */
class TestHcaCoordinator {
public:
  explicit TestHcaCoordinator(TestThreadPool &thread_pool)
      : thread_pool_(thread_pool) {}

  template <typename Function>
  int run_ranges(const int64_t total_count, const int64_t requested_task_count,
                 Function function) {
    int ret = OB_SUCCESS;
    if (total_count < 0 || requested_task_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (total_count > 0) {
      const int64_t task_count = std::min(total_count, requested_task_count);
      TestTaskGroup group;
      for (int64_t task_idx = 0; task_idx < task_count; ++task_idx) {
        const int64_t begin = total_count * task_idx / task_count;
        const int64_t end = total_count * (task_idx + 1) / task_count;
        if (OB_FAIL(group.add_task())) {
          break;
        }
        std::unique_ptr<TestTask> task(new TestFunctionTask(
            group, [function, task_idx, begin, end]() mutable {
              return function(task_idx, begin, end);
            }));
        const int submit_ret = thread_pool_.submit(std::move(task));
        if (OB_SUCCESS != submit_ret) {
          group.complete(submit_ret);
          ret = submit_ret;
          break;
        }
      }
      const int wait_ret = group.wait();
      if (OB_SUCCESS == ret) {
        ret = wait_ret;
      }
    }
    return ret;
  }

  int run_hierarchy(const TestVectorStore &vectors, int64_t cluster_size,
                    TestCenterBuffer &initial_centers,
                    TestAssignmentBuffer &initial_assignments,
                    TestMemberRows &initial_members,
                    const ObVectorNormalizeInfo *center_norm_info = nullptr);

private:
  TestThreadPool &thread_pool_;
};

struct TestHierarchyNodeOutput {
  std::vector<std::vector<int64_t>> next_nodes_;
};

struct TestFlatResult {
  TestFlatResult(const float distance, const int64_t vector_idx)
      : distance_(distance), vector_idx_(vector_idx) {}

  float distance_;
  int64_t vector_idx_;
};

bool test_flat_result_less(const TestFlatResult &left,
                           const TestFlatResult &right) {
  return left.distance_ < right.distance_ ||
         (left.distance_ == right.distance_ &&
          left.vector_idx_ < right.vector_idx_);
}

/**
 * Exact Flat Top-K used as both ground truth and candidate re-ranking.
 * A null candidates pointer scans the complete vector store; otherwise only
 * the supplied deduplicated vector_idx values are scanned.
 */
int get_test_flat_topk(const TestVectorStore &vectors, const float *query,
                       const TestDistanceType distance_type,
                       const std::vector<int64_t> *candidates,
                       const int64_t topk,
                       std::vector<int64_t> &top_vector_indices) {
  int ret = OB_SUCCESS;
  const int64_t candidate_count =
      OB_ISNULL(candidates)
          ? vectors.count()
          : static_cast<int64_t>(candidates->size());
  std::vector<TestFlatResult> results;
  if (OB_ISNULL(query) || topk <= 0 || candidate_count < topk) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    results.reserve(candidate_count);
  }
  for (int64_t pos = 0; OB_SUCC(ret) && pos < candidate_count; ++pos) {
    const int64_t vector_idx =
        OB_ISNULL(candidates) ? pos : (*candidates)[pos];
    const float *vector = nullptr;
    if (OB_FAIL(vectors.get_vector(vector_idx, vector))) {
    } else if (OB_ISNULL(vector)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      float distance = 0.0F;
      if (OB_FAIL(get_test_distance(query, vector, vectors.dimension(),
                                    distance_type, distance))) {
      } else {
        results.emplace_back(distance, vector_idx);
      }
    }
  }
  if (OB_SUCC(ret)) {
    std::partial_sort(results.begin(), results.begin() + topk, results.end(),
                      test_flat_result_less);
    top_vector_indices.resize(topk);
    for (int64_t rank = 0; rank < topk; ++rank) {
      top_vector_indices[rank] = results[rank].vector_idx_;
    }
  }
  return ret;
}

int64_t count_test_recall_hits(const std::vector<int64_t> &ground_truth,
                               const std::vector<int64_t> &actual) {
  int64_t hit_count = 0;
  for (const int64_t actual_vector_idx : actual) {
    if (ground_truth.end() !=
        std::find(ground_truth.begin(), ground_truth.end(),
                  actual_vector_idx)) {
      ++hit_count;
    }
  }
  return hit_count;
}

/** Verify the center-normalization contract used by Cosine and IP HCA. */
void expect_test_centers_normalized(const ObHcaCenterStore &centers) {
  for (int64_t cid = 0; cid < centers.count(); ++cid) {
    const float *center = nullptr;
    ASSERT_EQ(OB_SUCCESS, centers.get_center(cid, center));
    long double squared_norm = 0.0L;
    for (int64_t d = 0; d < centers.dimension(); ++d) {
      squared_norm += static_cast<long double>(center[d]) * center[d];
    }
    EXPECT_NEAR(1.0, std::sqrt(static_cast<double>(squared_norm)), 1e-4)
        << "cid=" << cid;
  }
}

/** Append one member row to a Flat candidate set and suppress duplicates. */
int append_test_flat_candidates(const ObHcaCidMemberStore &members,
                                const int64_t cid,
                                std::vector<uint8_t> &seen,
                                std::vector<int64_t> &candidates) {
  int ret = OB_SUCCESS;
  int64_t member_count = -1;
  if (cid < 0 || cid >= members.cid_count()) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((member_count = members.member_count(cid)) < 0) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t offset = 0; OB_SUCC(ret) && offset < member_count; ++offset) {
    int64_t vector_idx = -1;
    if (OB_FAIL(members.get_member(cid, offset, vector_idx))) {
    } else if (vector_idx < 0 ||
               vector_idx >= static_cast<int64_t>(seen.size())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (!seen[vector_idx]) {
      seen[vector_idx] = 1;
      candidates.push_back(vector_idx);
    }
  }
  return ret;
}

/**
 * Runs one hierarchy node through Train -> Assignment -> Stats -> Finalize.
 *
 * Train and Finalize are node-local serial operations. When parallel_ranges is
 * true, Assignment is split by node-local vector position and Stats by cid.
 * When false, the caller is already processing different same-level nodes in
 * parallel, which avoids nested worker creation.
 */
int process_hierarchy_node(
    const TestVectorStore &vectors, const std::vector<int64_t> &node_members,
    const int64_t cluster_size, const bool parallel_ranges,
    const ObVectorNormalizeInfo *center_norm_info,
    TestHcaCoordinator &coordinator, TestAssignmentBuffer &local_assignments,
    TestLeafResultWriter &leaf_writer, TestHierarchyNodeOutput &output) {
  int ret = OB_SUCCESS;
  common::ObArenaAllocator node_allocator(common::ObModIds::TEST);
  TestVectorBatch node_vectors(vectors, node_members);
  TestCenterBuffer provisional_centers(OB_HCA_MAX_LOCAL_K, vectors.dimension());
  int64_t provisional_count = 0;
  ObHcaTrainParam train_param;
  if (OB_FAIL(hca_train_local_centers(node_vectors, cluster_size, train_param,
                                      center_norm_info, provisional_centers,
                                      provisional_count, node_allocator))) {
  }

  std::vector<int32_t> candidates;
  if (OB_SUCC(ret)) {
    candidates.resize(provisional_count);
    for (int64_t cid = 0; cid < provisional_count; ++cid) {
      candidates[cid] = static_cast<int32_t>(cid);
    }
  }
  TestMemberRows local_members(provisional_count);
  if (OB_SUCC(ret) && parallel_ranges) {
    ret = coordinator.run_ranges(
        node_vectors.count(), TEST_PARALLEL_WORKER_COUNT,
        [&](const int64_t, const int64_t begin_pos, const int64_t end_pos) {
          TestVectorBatch task_vectors(vectors, node_members, begin_pos,
                                       end_pos);
          return hca_assign_vectors(task_vectors, provisional_centers,
                                    candidates.data(), provisional_count,
                                    local_assignments, local_members);
        });
  } else if (OB_SUCC(ret) &&
             OB_FAIL(hca_assign_vectors(node_vectors, provisional_centers,
                                        candidates.data(), provisional_count,
                                        local_assignments, local_members))) {
  }

  // Assignment workers have joined, so member rows are immutable while
  // disjoint cid-range tasks calculate full-data sums and counts.
  TestCenterStats stats(provisional_count, vectors.dimension());
  if (OB_SUCC(ret) && parallel_ranges) {
    ret = coordinator.run_ranges(
        provisional_count, TEST_PARALLEL_WORKER_COUNT,
        [&](const int64_t, const int64_t begin_cid, const int64_t end_cid) {
          common::ObArenaAllocator task_allocator(common::ObModIds::TEST);
          return hca_compute_local_center_stats_range(
              vectors, local_members, begin_cid, end_cid, vectors.dimension(),
              stats, task_allocator);
        });
  } else if (OB_SUCC(ret) && OB_FAIL(hca_compute_local_center_stats_range(
                                 vectors, local_members, 0, provisional_count,
                                 vectors.dimension(), stats, node_allocator))) {
  }

  TestCenterBuffer final_centers(provisional_count, vectors.dimension());
  TestCidRemap local_remap(provisional_count);
  int64_t final_count = 0;
  ObHcaSplitStatus split_status = HCA_SPLIT_NO_PROGRESS;
  if (OB_SUCC(ret) &&
      OB_FAIL(hca_finalize_local_centers(
          stats, provisional_count, vectors.dimension(), center_norm_info,
          final_centers, local_assignments, local_members, local_remap,
          final_count, split_status))) {
  }

  for (int64_t cid = 0; OB_SUCC(ret) && cid < final_count; ++cid) {
    const int64_t child_count = local_members.member_count(cid);
    const bool publish_leaf =
        split_status == HCA_SPLIT_NO_PROGRESS || child_count <= cluster_size;
    if (publish_leaf) {
      const float *center = nullptr;
      int32_t global_cid = -1;
      if (OB_FAIL(final_centers.get_center(cid, center))) {
      } else if (OB_FAIL(hca_publish_leaf_result(center, vectors.dimension(),
                                                 local_members, cid,
                                                 leaf_writer, global_cid))) {
      }
    } else {
      std::vector<int64_t> child_members(child_count);
      for (int64_t offset = 0; OB_SUCC(ret) && offset < child_count; ++offset) {
        if (OB_FAIL(
                local_members.get_member(cid, offset, child_members[offset]))) {
        }
      }
      if (OB_SUCC(ret)) {
        output.next_nodes_.push_back(std::move(child_members));
      }
    }
  }
  return ret;
}

/**
 * Parallel stand-in for the level-by-level hierarchy coordinator.
 *
 * A level with one node splits that node's Assignment and Stats into range
 * tasks. Once a level contains multiple children, each worker owns complete
 * nodes, so their local Train operations run concurrently. All node tasks join
 * before their oversized children form the next level.
 */
int TestHcaCoordinator::run_hierarchy(const TestVectorStore &vectors,
                                      const int64_t cluster_size,
                                      TestCenterBuffer &initial_centers,
                                      TestAssignmentBuffer &initial_assignments,
                                      TestMemberRows &initial_members,
                                      const ObVectorNormalizeInfo *center_norm_info) {
  int ret = OB_SUCCESS;
  std::vector<std::vector<int64_t>> current_nodes(1);
  current_nodes[0].resize(vectors.count());
  for (int64_t vector_idx = 0; vector_idx < vectors.count(); ++vector_idx) {
    current_nodes[0][vector_idx] = vector_idx;
  }
  TestLeafResultWriter leaf_writer(initial_centers, initial_assignments,
                                   initial_members);
  TestAssignmentBuffer local_assignments(vectors.count());
  int64_t processed_node_count = 0;

  while (OB_SUCC(ret) && !current_nodes.empty()) {
    if (processed_node_count >
        vectors.count() * 4 - static_cast<int64_t>(current_nodes.size())) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    processed_node_count += current_nodes.size();
    std::vector<TestHierarchyNodeOutput> outputs(current_nodes.size());
    if (current_nodes.size() == 1) {
      ret = process_hierarchy_node(vectors, current_nodes[0], cluster_size,
                                   true, center_norm_info, *this,
                                   local_assignments, leaf_writer, outputs[0]);
    } else {
      ret = run_ranges(
          current_nodes.size(), TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_node, const int64_t end_node) {
            int task_ret = OB_SUCCESS;
            for (int64_t node_idx = begin_node;
                 OB_SUCCESS == task_ret && node_idx < end_node; ++node_idx) {
              task_ret = process_hierarchy_node(
                  vectors, current_nodes[node_idx], cluster_size, false,
                  center_norm_info, *this, local_assignments, leaf_writer,
                  outputs[node_idx]);
            }
            return task_ret;
          });
    }

    std::vector<std::vector<int64_t>> next_nodes;
    for (int64_t node_idx = 0;
         OB_SUCC(ret) && node_idx < static_cast<int64_t>(outputs.size());
         ++node_idx) {
      for (std::vector<int64_t> &child : outputs[node_idx].next_nodes_) {
        next_nodes.push_back(std::move(child));
      }
    }
    current_nodes.swap(next_nodes);
  }
  return ret;
}

/**
 * Serial stand-in for the hierarchy coordinator.
 *
 * Each pending node owns only global vector_idx values. The function creates a
 * local VectorBatch, calls Train/Assignment/Stats/Finalize, publishes final
 * leaves and enqueues oversized child rows. Child members are intentionally
 * non-contiguous, exercising position-to-vector_idx mapping.
 */
int run_serial_hierarchy(const TestVectorStore &vectors,
                         const int64_t cluster_size,
                         TestCenterBuffer &initial_centers,
                         TestAssignmentBuffer &initial_assignments,
                         TestMemberRows &initial_members,
                         common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  std::deque<std::vector<int64_t>> pending_nodes;
  std::vector<int64_t> root_members(vectors.count());
  for (int64_t i = 0; i < vectors.count(); ++i) {
    root_members[i] = i;
  }
  pending_nodes.push_back(root_members);
  TestLeafResultWriter leaf_writer(initial_centers, initial_assignments,
                                   initial_members);
  TestAssignmentBuffer local_assignments(vectors.count());
  int64_t processed_nodes = 0;

  while (OB_SUCC(ret) && !pending_nodes.empty()) {
    if (++processed_nodes > vectors.count() * 4) {
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    std::vector<int64_t> node_members = std::move(pending_nodes.front());
    pending_nodes.pop_front();
    TestVectorBatch node_vectors(vectors, node_members);
    TestCenterBuffer provisional_centers(OB_HCA_MAX_LOCAL_K,
                                         vectors.dimension());
    int64_t provisional_count = 0;
    ObHcaTrainParam train_param;
    if (OB_FAIL(hca_train_local_centers(node_vectors, cluster_size, train_param,
                                        nullptr, provisional_centers,
                                        provisional_count, allocator))) {
      break;
    }

    std::vector<int32_t> candidates(provisional_count);
    for (int64_t cid = 0; cid < provisional_count; ++cid) {
      candidates[cid] = static_cast<int32_t>(cid);
    }
    TestMemberRows local_members(provisional_count);
    if (OB_FAIL(hca_assign_vectors(node_vectors, provisional_centers,
                                   candidates.data(), provisional_count,
                                   local_assignments, local_members))) {
      break;
    }

    TestCenterStats stats(provisional_count, vectors.dimension());
    if (OB_FAIL(hca_compute_local_center_stats_range(
            vectors, local_members, 0, provisional_count, vectors.dimension(),
            stats, allocator))) {
      break;
    }

    TestCenterBuffer final_centers(provisional_count, vectors.dimension());
    TestCidRemap local_remap(provisional_count);
    int64_t final_count = 0;
    ObHcaSplitStatus split_status = HCA_SPLIT_NO_PROGRESS;
    if (OB_FAIL(hca_finalize_local_centers(
            stats, provisional_count, vectors.dimension(), nullptr,
            final_centers, local_assignments, local_members, local_remap,
            final_count, split_status))) {
      break;
    }

    for (int64_t cid = 0; OB_SUCC(ret) && cid < final_count; ++cid) {
      const int64_t child_count = local_members.member_count(cid);
      const bool publish_leaf =
          split_status == HCA_SPLIT_NO_PROGRESS || child_count <= cluster_size;
      if (publish_leaf) {
        const float *center = nullptr;
        int32_t global_cid = -1;
        if (OB_FAIL(final_centers.get_center(cid, center))) {
        } else if (OB_FAIL(hca_publish_leaf_result(center, vectors.dimension(),
                                                   local_members, cid,
                                                   leaf_writer, global_cid))) {
        }
      } else {
        std::vector<int64_t> child_members(child_count);
        for (int64_t offset = 0; OB_SUCC(ret) && offset < child_count;
             ++offset) {
          if (OB_FAIL(local_members.get_member(cid, offset,
                                               child_members[offset]))) {
          }
        }
        if (OB_SUCC(ret)) {
          pending_nodes.push_back(std::move(child_members));
        }
      }
    }
  }
  return ret;
}

/** Verify that assignments and member rows describe the same partition. */
void expect_assignment_members_consistent(
    const TestAssignmentBuffer &assignments, const TestMemberRows &members,
    const int64_t vector_count, const int64_t center_count) {
  std::vector<int64_t> seen(vector_count, 0);
  int64_t member_total = 0;
  for (int64_t cid = 0; cid < center_count; ++cid) {
    for (int64_t offset = 0; offset < members.member_count(cid); ++offset) {
      int64_t vector_idx = -1;
      int32_t assigned_cid = -1;
      ASSERT_EQ(OB_SUCCESS, members.get_member(cid, offset, vector_idx));
      ASSERT_GE(vector_idx, 0);
      ASSERT_LT(vector_idx, vector_count);
      ASSERT_EQ(OB_SUCCESS,
                assignments.get_assignment(vector_idx, assigned_cid));
      EXPECT_EQ(cid, assigned_cid);
      ++seen[vector_idx];
      ++member_total;
    }
  }
  EXPECT_EQ(vector_count, member_total);
  for (int64_t vector_idx = 0; vector_idx < vector_count; ++vector_idx) {
    EXPECT_EQ(1, seen[vector_idx]);
  }
}

/** Verify that A2 and the reverse SOAR member rows contain the same vectors. */
void expect_secondary_assignment_members_consistent(
    const TestAssignmentBuffer &primary_assignments,
    const TestAssignmentBuffer &secondary_assignments,
    const TestMemberRows &secondary_members, const int64_t vector_count,
    const int64_t center_count) {
  std::vector<int64_t> seen(vector_count, 0);
  for (int64_t cid = 0; cid < center_count; ++cid) {
    for (int64_t offset = 0; offset < secondary_members.member_count(cid);
         ++offset) {
      int64_t vector_idx = -1;
      int32_t primary_cid = -1;
      int32_t secondary_cid = -1;
      ASSERT_EQ(OB_SUCCESS,
                secondary_members.get_member(cid, offset, vector_idx));
      ASSERT_GE(vector_idx, 0);
      ASSERT_LT(vector_idx, vector_count);
      ASSERT_EQ(OB_SUCCESS,
                primary_assignments.get_assignment(vector_idx, primary_cid));
      ASSERT_EQ(OB_SUCCESS, secondary_assignments.get_assignment(
                                vector_idx, secondary_cid));
      EXPECT_EQ(cid, secondary_cid);
      EXPECT_NE(primary_cid, secondary_cid);
      ++seen[vector_idx];
    }
  }
  for (int64_t vector_idx = 0; vector_idx < vector_count; ++vector_idx) {
    int32_t secondary_cid = -1;
    ASSERT_EQ(OB_SUCCESS,
              secondary_assignments.get_assignment(vector_idx, secondary_cid));
    if (secondary_cid < 0) {
      EXPECT_EQ(0, seen[vector_idx]);
    } else {
      EXPECT_LT(secondary_cid, center_count);
      EXPECT_EQ(1, seen[vector_idx]);
    }
  }
}

/** Verify complete, unique Neighborhood rows and their maximum distance. */
void expect_neighborhood_rows_consistent(
    const TestCenterBuffer &centers, const TestNeighborhood &neighborhood,
    const int64_t expected_neighbor_count) {
  std::vector<int64_t> seen(centers.count(), -1);
  for (int64_t owner_cid = 0; owner_cid < centers.count(); ++owner_cid) {
    ASSERT_TRUE(neighborhood.is_written(owner_cid));
    ASSERT_EQ(expected_neighbor_count, neighborhood.neighbor_count(owner_cid));
    const float *owner = nullptr;
    ASSERT_EQ(OB_SUCCESS, centers.get_center(owner_cid, owner));
    float farthest_distance = 0.0F;
    for (int64_t offset = 0; offset < expected_neighbor_count; ++offset) {
      int32_t neighbor_cid = -1;
      const float *neighbor = nullptr;
      ASSERT_EQ(OB_SUCCESS,
                neighborhood.get_neighbor(owner_cid, offset, neighbor_cid));
      ASSERT_GE(neighbor_cid, 0);
      ASSERT_LT(neighbor_cid, centers.count());
      ASSERT_NE(owner_cid, neighbor_cid);
      ASSERT_NE(owner_cid, seen[neighbor_cid]);
      seen[neighbor_cid] = owner_cid;
      ASSERT_EQ(OB_SUCCESS, centers.get_center(neighbor_cid, neighbor));
      const float distance =
          hca_square_distance(owner, neighbor, centers.dimension());
      EXPECT_TRUE(std::isfinite(distance));
      farthest_distance = std::max(farthest_distance, distance);
    }
    float max_intra_distance = 0.0F;
    ASSERT_EQ(OB_SUCCESS, neighborhood.get_max_intra_distance(
                              owner_cid, max_intra_distance));
    EXPECT_FLOAT_EQ(farthest_distance, max_intra_distance);
  }
}

TEST(TestVectorHcaEndToEnd, BlockedAssignmentMatchesExactCandidatesAndTies)
{
  for (int64_t dim : {2, 7, 64, 768, 769}) {
    TestCenterBuffer centers(257, dim);
    std::vector<float> row(dim), data(32 * dim);
    for (int64_t c = 0; c < 257; ++c) {
      for (int64_t d = 0; d < dim; ++d) { row[d] = std::sin((c % 256) * 0.4 + d * 0.3); }
      ASSERT_EQ(OB_SUCCESS, centers.set_center(c, row.data()));
    }
    const float *queries[32];
    for (int64_t q = 0; q < 32; ++q) {
      queries[q] = data.data() + q * dim;
      for (int64_t d = 0; d < dim; ++d) { data[q * dim + d] = std::sin(q * 0.4 + d * 0.3); }
    }
    std::vector<int32_t> candidates(257);
    for (int64_t c = 0; c < 257; ++c) { candidates[c] = 256 - c; }
    for (int64_t count : {1, 3, 4, 5, 31, 32}) {
      for (bool subset : {false, true}) {
        int64_t actual[32];
        ASSERT_EQ(OB_SUCCESS, hca_find_nearest_centers(queries, count, centers,
            subset ? candidates.data() : nullptr, 257, actual));
        for (int64_t q = 0; q < count; ++q) {
          int64_t expected = -1; float best = 0;
          for (int64_t c = 0; c < 257; ++c) {
            const int64_t cid = subset ? candidates[c] : c;
            const float *center = nullptr;
            ASSERT_EQ(OB_SUCCESS, centers.get_center(cid, center));
            const float distance = hca_square_distance(queries[q], center, dim);
            if (expected < 0 || distance < best) { expected = cid; best = distance; }
          }
          EXPECT_EQ(expected, actual[q]) << dim << ":" << count << ":" << q;
        }
        EXPECT_EQ(subset ? 256 : 0, actual[0]);
      }
    }
    int64_t output[32];
    EXPECT_EQ(OB_INVALID_ARGUMENT, hca_find_nearest_centers(queries, 33, centers, nullptr, 257, output));
    int32_t invalid = 257;
    EXPECT_EQ(OB_INVALID_ARGUMENT, hca_find_nearest_centers(queries, 1, centers, &invalid, 1, output));
    const float *saved_query = queries[1];
    queries[1] = nullptr;
    EXPECT_EQ(OB_INVALID_ARGUMENT, hca_find_nearest_centers(queries, 4, centers, nullptr, 257, output));
    queries[1] = saved_query;
    data[0] = std::numeric_limits<float>::quiet_NaN();
    EXPECT_EQ(OB_INVALID_DATA, hca_find_nearest_centers(queries, 4, centers, nullptr, 257, output));
  }
}

TEST(TestVectorHcaEndToEnd, BlockedAssignmentPerformance)
{
  // Opt-in diagnostic on the actual shared kernel, never a timing assertion.
  if (std::getenv("IVF_ASSIGN_BENCH") == nullptr) { return; }
  common::init_arches();
  const int64_t k = 36386, dim = 768, n = 512, worker_count = 4;
  TestCenterBuffer centers(k, dim);
  std::vector<float> row(dim), data(n * dim);
  uint32_t seed = 42;
  auto next = [&]() { seed = seed * 1664525U + 1013904223U; return (static_cast<int32_t>(seed % 10001) - 5000) / 5000.0F; };
  for (int64_t c = 0; c < k; ++c) {
    for (float &value : row) { value = next(); }
    ASSERT_EQ(OB_SUCCESS, centers.set_center(c, row.data()));
  }
  for (float &value : data) { value = next(); }
  std::vector<int64_t> reference(n, -1);
  for (int64_t round = 0; round < 3; ++round) {
    for (int64_t variant = 0; variant < 2; ++variant) {
      const bool blocked = ((round + variant) % 2) != 0;
      std::vector<int64_t> result(n, -1);
      std::atomic<int> errors(0);
      std::vector<std::thread> workers;
      const auto start = std::chrono::steady_clock::now();
      for (int64_t t = 0; t < worker_count; ++t) {
        workers.emplace_back([&, t] {
          const int64_t begin = n * t / worker_count, end = n * (t + 1) / worker_count;
          if (!blocked) {
            for (int64_t q = begin; q < end; ++q) {
              float best = 0;
              for (int64_t c = 0; c < k; ++c) {
                const float *center = nullptr;
                if (centers.get_center(c, center) != OB_SUCCESS) { ++errors; break; }
                const float distance = hca_square_distance(data.data() + q * dim, center, dim);
                if (result[q] < 0 || distance < best) { best = distance; result[q] = c; }
              }
            }
          } else {
            for (int64_t q = begin; q < end; q += OB_HCA_ASSIGN_BATCH_SIZE) {
              const float *batch[OB_HCA_ASSIGN_BATCH_SIZE];
              for (int64_t i = 0; i < OB_HCA_ASSIGN_BATCH_SIZE; ++i) { batch[i] = data.data() + (q + i) * dim; }
              if (hca_find_nearest_centers(batch, OB_HCA_ASSIGN_BATCH_SIZE, centers, nullptr, k, result.data() + q) != OB_SUCCESS) { ++errors; }
            }
          }
        });
      }
      for (auto &worker : workers) { worker.join(); }
      const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
      ASSERT_EQ(0, errors.load());
      if (round == 0 && !blocked) { reference = result; }
      ASSERT_EQ(reference, result);
      std::printf("IVF_ASSIGN_BENCH round=%ld blocked=%d threads=%ld seconds=%.6f exact=1\n", round, blocked, worker_count, seconds);
    }
  }
}

TEST(TestVectorHcaEndToEnd, TrainingSamplesKeepFullNodeFanout) {
  const int64_t vector_count = 100;
  const int64_t dimension = 2;
  const int64_t cluster_size = 10;
  const int64_t preselected_count = 30;
  TestVectorStore left_vectors(vector_count, dimension);
  TestVectorStore right_vectors(vector_count, dimension);
  std::vector<int64_t> all_indices(vector_count);
  std::vector<int64_t> sample_indices(preselected_count);
  std::vector<float> vector(dimension, 0.0F);
  for (int64_t i = 0; i < vector_count; ++i) {
    all_indices[i] = i;
    if (i < preselected_count) {
      sample_indices[i] = i;
      vector[0] = static_cast<float>(i) * 1.25F;
      vector[1] = static_cast<float>((i * 7) % 13) * 0.5F;
      ASSERT_EQ(OB_SUCCESS, left_vectors.set_vector(i, vector.data()));
      ASSERT_EQ(OB_SUCCESS, right_vectors.set_vector(i, vector.data()));
    } else {
      vector[0] = 10000.0F + static_cast<float>(i);
      vector[1] = 20000.0F + static_cast<float>(i);
      ASSERT_EQ(OB_SUCCESS, left_vectors.set_vector(i, vector.data()));
      vector[0] = -10000.0F - static_cast<float>(i);
      vector[1] = -20000.0F - static_cast<float>(i);
      ASSERT_EQ(OB_SUCCESS, right_vectors.set_vector(i, vector.data()));
    }
  }

  TestVectorBatch left_full(left_vectors, all_indices);
  TestVectorBatch right_full(right_vectors, all_indices);
  TestVectorBatch left_samples(left_vectors, sample_indices);
  TestVectorBatch right_samples(right_vectors, sample_indices);
  TestCenterBuffer left_centers(OB_HCA_MAX_LOCAL_K, dimension);
  TestCenterBuffer right_centers(OB_HCA_MAX_LOCAL_K, dimension);
  common::ObArenaAllocator left_allocator(common::ObModIds::TEST);
  common::ObArenaAllocator right_allocator(common::ObModIds::TEST);
  ObHcaTrainParam train_param;
  train_param.samples_per_cluster_ = 2;
  int64_t left_count = 0;
  int64_t right_count = 0;

  ASSERT_EQ(OB_SUCCESS,
            hca_train_local_centers(
                left_full, cluster_size, train_param, nullptr,
                left_centers, left_count, left_allocator, &left_samples));
  ASSERT_EQ(OB_SUCCESS,
            hca_train_local_centers(
                right_full, cluster_size, train_param, nullptr,
                right_centers, right_count, right_allocator, &right_samples));
  // local_k is calculated from the 100-vector node, not the 30-vector sample
  // batch. Only min(100, 10 * 2) samples drive both training calls, so changing
  // every non-sampled vector cannot change the provisional centers.
  ASSERT_EQ(10, left_count);
  ASSERT_EQ(left_count, right_count);
  for (int64_t cid = 0; cid < left_count; ++cid) {
    const float *left = nullptr;
    const float *right = nullptr;
    ASSERT_EQ(OB_SUCCESS, left_centers.get_center(cid, left));
    ASSERT_EQ(OB_SUCCESS, right_centers.get_center(cid, right));
    for (int64_t d = 0; d < dimension; ++d) {
      EXPECT_FLOAT_EQ(left[d], right[d]);
    }
  }

  // The existing API requires at least local_k training vectors.
  std::vector<int64_t> short_sample_indices(
      sample_indices.begin(), sample_indices.begin() + 9);
  TestVectorBatch short_samples(left_vectors, short_sample_indices);
  TestCenterBuffer unused_centers(OB_HCA_MAX_LOCAL_K, dimension);
  common::ObArenaAllocator short_allocator(common::ObModIds::TEST);
  int64_t unused_count = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            hca_train_local_centers(
                left_full, cluster_size, train_param, nullptr,
                unused_centers, unused_count, short_allocator, &short_samples));
}

TEST(TestVectorHcaEndToEnd, SingleCenterIgnoresTrainingSamples) {
  const int64_t vector_count = 4;
  const int64_t dimension = 2;
  TestVectorStore vectors(vector_count, dimension);
  std::vector<int64_t> all_indices(vector_count);
  std::vector<int64_t> sample_indices(1, 0);
  std::vector<float> vector(dimension, 0.0F);
  for (int64_t i = 0; i < vector_count; ++i) {
    all_indices[i] = i;
    vector[0] = static_cast<float>(i + 1);
    vector[1] = static_cast<float>((i + 1) * 10);
    ASSERT_EQ(OB_SUCCESS, vectors.set_vector(i, vector.data()));
  }
  TestVectorBatch full_batch(vectors, all_indices);
  TestVectorBatch sample_batch(vectors, sample_indices);
  TestCenterBuffer centers(OB_HCA_MAX_LOCAL_K, dimension);
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObHcaTrainParam train_param;
  int64_t center_count = 0;

  ASSERT_EQ(OB_SUCCESS,
            hca_train_local_centers(
                full_batch, 10 /* cluster_size */, train_param,
                nullptr, centers, center_count, allocator, &sample_batch));
  ASSERT_EQ(1, center_count);
  const float *center = nullptr;
  ASSERT_EQ(OB_SUCCESS, centers.get_center(0, center));
  EXPECT_FLOAT_EQ(2.5F, center[0]);
  EXPECT_FLOAT_EQ(25.0F, center[1]);
}

/** Find the nearest cid without assuming any order inside a Neighborhood row. */
int get_test_nearest_neighbor(const TestCenterBuffer &centers,
                              const TestNeighborhood &neighborhood,
                              const int64_t owner_cid,
                              int32_t &nearest_neighbor_cid) {
  int ret = OB_SUCCESS;
  const float *owner = nullptr;
  float nearest_distance = std::numeric_limits<float>::max();
  nearest_neighbor_cid = -1;
  if (owner_cid < 0 || owner_cid >= centers.count() ||
      neighborhood.neighbor_count(owner_cid) <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(centers.get_center(owner_cid, owner))) {
  }
  for (int64_t offset = 0;
       OB_SUCC(ret) && offset < neighborhood.neighbor_count(owner_cid);
       ++offset) {
    int32_t neighbor_cid = -1;
    const float *neighbor = nullptr;
    if (OB_FAIL(neighborhood.get_neighbor(owner_cid, offset, neighbor_cid))) {
    } else if (OB_FAIL(centers.get_center(neighbor_cid, neighbor))) {
    } else {
      const float distance =
          hca_square_distance(owner, neighbor, centers.dimension());
      if (distance < nearest_distance ||
          (distance == nearest_distance &&
           (nearest_neighbor_cid < 0 || neighbor_cid < nearest_neighbor_cid))) {
        nearest_distance = distance;
        nearest_neighbor_cid = neighbor_cid;
      }
    }
  }
  return ret;
}

TEST(TestVectorHcaEndToEnd, RefinePrefetchPreservesTrainingAndPropagatesErrors) {
  class PrefetchStore final : public TestVectorStore {
  public:
    PrefetchStore() : TestVectorStore(131, 2) {}
    mutable int64_t batches_ = 0;
    mutable int64_t reads_ = 0;
    int error_ = OB_SUCCESS;
    int prefetch_vectors(const int64_t *indices, int64_t count) const override {
      ++batches_;
      for (int64_t i = 0; i < count; ++i) {
        if (indices[i] < 0 || indices[i] >= this->count() || (i && indices[i] < indices[i - 1])) {
          return OB_INVALID_ARGUMENT;
        }
      }
      return error_;
    }
    int get_vector(int64_t idx, const float *&vector) const override {
      ++reads_;
      return TestVectorStore::get_vector(idx, vector);
    }
  } prefetched;
  TestVectorStore plain(131, 2);
  TestCenterBuffer centers(4, 2), expected(4, 2), actual(4, 2), failed(4, 2);
  TestAssignmentBuffer assignments(131);
  for (int64_t i = 0; i < 131; ++i) {
    float v[2] = {static_cast<float>(i % 4 * 100 + i / 4), static_cast<float>(i % 4 * 100)};
    ASSERT_EQ(OB_SUCCESS, plain.set_vector(i, v));
    ASSERT_EQ(OB_SUCCESS, prefetched.set_vector(i, v));
    ASSERT_EQ(OB_SUCCESS, assignments.set_assignment(i, i % 4));
  }
  for (int64_t cid = 0; cid < 4; ++cid) {
    float v[2] = {static_cast<float>(cid * 100), static_cast<float>(cid * 100)};
    ASSERT_EQ(OB_SUCCESS, centers.set_center(cid, v));
  }
  ObHcaRefineParam param;
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ASSERT_EQ(OB_SUCCESS, hca_refine_train_centers(plain, centers, assignments, nullptr, param, nullptr, expected, allocator));
  ASSERT_EQ(OB_SUCCESS, hca_refine_train_centers(prefetched, centers, assignments, nullptr, param, nullptr, actual, allocator));
  EXPECT_EQ(32, prefetched.batches_);
  for (int64_t cid = 0; cid < 4; ++cid) {
    const float *left = nullptr, *right = nullptr;
    ASSERT_EQ(OB_SUCCESS, expected.get_center(cid, left));
    ASSERT_EQ(OB_SUCCESS, actual.get_center(cid, right));
    EXPECT_EQ(0, MEMCMP(left, right, 2 * sizeof(float)));
  }
  prefetched.error_ = OB_IO_ERROR;
  prefetched.reads_ = 0;
  ASSERT_EQ(OB_IO_ERROR, hca_refine_train_centers(prefetched, centers, assignments, nullptr, param, nullptr, failed, allocator));
  EXPECT_EQ(0, prefetched.reads_);
}

TEST(TestVectorHcaEndToEnd, RefineParallelBatchesMatchSerialBitwise) {
  TestThreadPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.start(7));
  TestHcaCoordinator coordinator(pool);
  // Exercise all-center and variable-length neighborhood candidates, a wrapped
  // final sample batch, multiple epochs, and uneven parallel task boundaries.
  for (bool normalize : {false, true}) {
    ObVectorNormalizeInfo norm;
    const ObVectorNormalizeInfo *norm_info = normalize ? &norm : nullptr;
    for (const int64_t k : {4L, 129L}) {
      const int64_t n = k * 7 + 3, dim = 17;
      TestVectorStore vectors(n, dim);
      TestCenterBuffer centers(k, dim), serial(k, dim), parallel(k, dim);
      TestAssignmentBuffer assignments(n);
      TestNeighborhood neighbors(k);
      std::vector<float> value(dim);
      for (int64_t i = 0; i < n; ++i) {
        for (int64_t d = 0; d < dim; ++d) {
          value[d] = static_cast<float>((i * 37 + d * 11) % 191) / 19.0F;
        }
        if (normalize) {
          ASSERT_EQ(OB_SUCCESS, norm.normalize_func_(dim, value.data(),
                                                     value.data(), nullptr));
        }
        ASSERT_EQ(OB_SUCCESS, vectors.set_vector(i, value.data()));
        ASSERT_EQ(OB_SUCCESS, assignments.set_assignment(i, i % k));
        if (i < k) {
          ASSERT_EQ(OB_SUCCESS, centers.set_center(i, value.data()));
          std::vector<int32_t> cids;
          const int64_t count = i % 3 == 0 ? std::min(k - 1, OB_HCA_NEIGHBOR_COUNT) : i % 3;
          for (int64_t j = 1; j <= count; ++j) {
            cids.push_back(static_cast<int32_t>((i + j) % k));
          }
          ASSERT_EQ(OB_SUCCESS, neighbors.set_row(i, cids.data(), count, 0.0F));
        }
      }
      ObHcaRefineParam param;
      param.samples_per_cluster_ = 5;
      param.max_iterations_ = 3;
      common::ObArenaAllocator allocator(common::ObModIds::TEST);
      const ObHcaNeighborhoodStore *graph =
          k > OB_HCA_NEIGHBOR_COUNT ? &neighbors : nullptr;
      ASSERT_EQ(OB_SUCCESS,
                hca_refine_train_centers(vectors, centers, assignments, graph,
                                         param, norm_info, serial, allocator));
      ObHcaRefineTrainer trainer(allocator);
      ASSERT_EQ(OB_SUCCESS, trainer.init(vectors, centers, assignments, graph,
                                         param, norm_info, parallel));
      int64_t batches = 0, rows = 0;
      while (true) {
        const int ret = trainer.prepare_batch(rows);
        if (ret == OB_ITER_END)
          break;
        ASSERT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(OB_STATE_NOT_MATCH, trainer.finish_batch());
        int64_t unused = 0;
        EXPECT_EQ(OB_STATE_NOT_MATCH, trainer.prepare_batch(unused));
        EXPECT_EQ(OB_INVALID_ARGUMENT, trainer.distance_range(-1, rows));
        ASSERT_EQ(OB_SUCCESS,
                  coordinator.run_ranges(
                      rows, 7, [&trainer](int64_t, int64_t begin, int64_t end) {
                        return trainer.distance_range(begin, end);
                      }));
        ASSERT_EQ(OB_SUCCESS, trainer.prepare_assign());
        EXPECT_EQ(OB_STATE_NOT_MATCH, trainer.prepare_update());
        ASSERT_EQ(OB_SUCCESS, coordinator.run_ranges(rows, 7,
            [&trainer](int64_t, int64_t begin, int64_t end) { return trainer.assign_range(begin, end); }));
        ASSERT_EQ(OB_SUCCESS, trainer.prepare_update());
        EXPECT_EQ(OB_STATE_NOT_MATCH, trainer.finish_update());
        ASSERT_EQ(OB_SUCCESS, coordinator.run_ranges(k, 7,
            [&trainer](int64_t, int64_t begin, int64_t end) { return trainer.update_range(begin, end); }));
        EXPECT_EQ(OB_STATE_NOT_MATCH, trainer.update_range(0, 1));
        ASSERT_EQ(OB_SUCCESS, trainer.finish_update());
        EXPECT_EQ((rows + 2 * k) * sizeof(int64_t), trainer.finish_workspace_bytes());
        EXPECT_EQ(OB_STATE_NOT_MATCH, trainer.finish_batch());
        ++batches;
      }
      EXPECT_EQ(9, batches);
      for (int64_t cid = 0; cid < k; ++cid) {
        const float *a = nullptr, *b = nullptr;
        ASSERT_EQ(OB_SUCCESS, serial.get_center(cid, a));
        ASSERT_EQ(OB_SUCCESS, parallel.get_center(cid, b));
        EXPECT_EQ(0, MEMCMP(a, b, dim * sizeof(float))) << "cid=" << cid;
      }
      trainer.reset();
      EXPECT_EQ(OB_NOT_INIT, trainer.distance_range(0, 1));
    }
  }
}

// Opt-in microbenchmark: both paths use the same parallel distance phase;
// only batch completion changes. Keep the serial implementation as the oracle.
TEST(TestVectorHcaEndToEnd, DISABLED_RefineFinishBenchmark) {
  const int64_t k = 32768, n = 2 * k, dim = 768, workers = 16;
  TestThreadPool pool;
  ASSERT_EQ(OB_SUCCESS, pool.start(workers));
  TestHcaCoordinator coordinator(pool);
  TestVectorStore vectors(n, dim);
  TestCenterBuffer centers(k, dim), serial(k, dim), parallel(k, dim);
  TestAssignmentBuffer assignments(n);
  TestNeighborhood neighbors(k);
  std::vector<float> value(dim);
  for (int64_t i = 0; i < n; ++i) {
    for (int64_t d = 0; d < dim; ++d) {
      value[d] = static_cast<float>((i * 37 + d * 11) % 191) / 19.0F;
    }
    ASSERT_EQ(OB_SUCCESS, vectors.set_vector(i, value.data()));
    ASSERT_EQ(OB_SUCCESS, assignments.set_assignment(i, i % k));
    if (i < k) {
      ASSERT_EQ(OB_SUCCESS, centers.set_center(i, value.data()));
      std::vector<int32_t> cids;
      for (int64_t j = 1; j <= OB_HCA_NEIGHBOR_COUNT; ++j) {
        cids.push_back(static_cast<int32_t>((i + j) % k));
      }
      ASSERT_EQ(OB_SUCCESS, neighbors.set_row(i, cids.data(), cids.size(), 0.0F));
    }
  }
  ObHcaRefineParam param;
  param.samples_per_cluster_ = 2;
  param.max_iterations_ = 3;
  for (int round = 0; round < 3; ++round) {
    for (int pass = 0; pass < 2; ++pass) {
      const bool staged = (pass + round) % 2 != 0;
      common::ObArenaAllocator allocator(common::ObModIds::TEST);
      ObHcaRefineTrainer trainer(allocator);
      ASSERT_EQ(OB_SUCCESS, trainer.init(vectors, centers, assignments, &neighbors,
          param, nullptr, staged ? parallel : serial));
      int64_t finish_us = 0, workspace = 0, batches = 0, rows = 0;
      const int64_t started = common::ObTimeUtility::current_time();
      while (true) {
        const int ret = trainer.prepare_batch(rows);
        if (ret == OB_ITER_END) { break; }
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_EQ(OB_SUCCESS, coordinator.run_ranges(rows, workers,
            [&trainer](int64_t, int64_t begin, int64_t end) {
              return trainer.distance_range(begin, end);
            }));
        const int64_t finish_start = common::ObTimeUtility::current_time();
        if (staged) {
          ASSERT_EQ(OB_SUCCESS, trainer.prepare_assign());
          ASSERT_EQ(OB_SUCCESS, coordinator.run_ranges(rows, workers,
              [&trainer](int64_t, int64_t begin, int64_t end) {
                return trainer.assign_range(begin, end);
              }));
          ASSERT_EQ(OB_SUCCESS, trainer.prepare_update());
          ASSERT_EQ(OB_SUCCESS, coordinator.run_ranges(k, workers,
              [&trainer](int64_t, int64_t begin, int64_t end) {
                return trainer.update_range(begin, end);
              }));
          ASSERT_EQ(OB_SUCCESS, trainer.finish_update());
        } else { ASSERT_EQ(OB_SUCCESS, trainer.finish_batch()); }
        finish_us += common::ObTimeUtility::current_time() - finish_start;
        workspace = trainer.finish_workspace_bytes();
        ++batches;
      }
      fprintf(stdout, "REFINE_BENCH round=%d staged=%d workers=%ld k=%ld n=%ld dim=%ld "
          "batches=%ld total_us=%ld finish_us=%ld workspace=%ld\n",
          round, staged, workers, k, n, dim, batches,
          common::ObTimeUtility::current_time() - started, finish_us, workspace);
    }
    for (int64_t cid = 0; cid < k; ++cid) {
      const float *a = nullptr, *b = nullptr;
      ASSERT_EQ(OB_SUCCESS, serial.get_center(cid, a));
      ASSERT_EQ(OB_SUCCESS, parallel.get_center(cid, b));
      ASSERT_EQ(0, MEMCMP(a, b, dim * sizeof(float))) << "cid=" << cid;
    }
  }
}

TEST(TestVectorHcaEndToEnd, SerialFullPipeline) {
  const int64_t vector_count = 384;
  const int64_t dimension = 2;
  const int64_t cluster_size = 2;
  TestVectorStore vectors(vector_count, dimension);
  std::vector<int64_t> all_vector_indices(vector_count);
  std::vector<float> vector(dimension, 0.0F);
  for (int64_t i = 0; i < vector_count; ++i) {
    all_vector_indices[i] = i;
    vector[0] = static_cast<float>(i) * 4.0F;
    vector[1] = static_cast<float>((i * 17) % 23) * 0.05F;
    ASSERT_EQ(OB_SUCCESS, vectors.set_vector(i, vector.data()));
  }
  TestVectorBatch all_vectors(vectors, all_vector_indices);
  common::ObArenaAllocator allocator(common::ObModIds::TEST);

  // Stage 1 validates the hierarchy contract: every input vector is eventually
  // published exactly once into C0/A0/InitialMemberLists.
  TestCenterBuffer initial_centers(vector_count, dimension);
  TestAssignmentBuffer initial_assignments(vector_count);
  TestMemberRows initial_members(vector_count);
  ASSERT_EQ(OB_SUCCESS, run_serial_hierarchy(
                            vectors, cluster_size, initial_centers,
                            initial_assignments, initial_members, allocator));
  const int64_t initial_center_count = initial_centers.count();
  ASSERT_GT(initial_center_count, OB_HCA_NEIGHBOR_COUNT);
  ASSERT_LT(initial_center_count, vector_count);
  expect_assignment_members_consistent(initial_assignments, initial_members,
                                       vector_count, initial_center_count);

  // Stage 2 validates the 128<K<10000 path: a null index selects brute force
  // and every C0 cid receives an ordered Top-128 G0 row.
  TestNeighborhood initial_neighborhood(initial_center_count);
  ObHcaNeighborhoodParam neighborhood_param;
  ASSERT_EQ(OB_SUCCESS,
            hca_build_neighborhood_rows(
                nullptr, initial_centers, 0, initial_center_count,
                neighborhood_param, initial_neighborhood, allocator, nullptr));
  expect_neighborhood_rows_consistent(initial_centers, initial_neighborhood,
                                      OB_HCA_NEIGHBOR_COUNT);

  // Stage 3 validates the Refine barrier order. Train creates an independent
  // WorkingCenters snapshot; one serial range builds RefinedAssignments,
  // RefinedMemberLists and ChangedCids before cid-range Center Update.
  TestCenterBuffer working_centers(initial_center_count, dimension);
  ObHcaRefineParam refine_param;
  ASSERT_EQ(OB_SUCCESS, hca_refine_train_centers(
                            vectors, initial_centers, initial_assignments,
                            &initial_neighborhood, refine_param, nullptr,
                            working_centers, allocator));

  TestAssignmentBuffer refined_assignments(vector_count);
  TestMemberRows refined_members(initial_center_count);
  TestChangedBitmap changed_cids(initial_center_count);
  ASSERT_EQ(OB_SUCCESS, hca_refine_assign_range(
                            all_vectors, working_centers, initial_assignments,
                            &initial_neighborhood, refined_assignments,
                            refined_members, changed_cids));

  ASSERT_EQ(OB_SUCCESS,
            hca_refine_update_centers_range(
                vectors, refined_members, changed_cids, 0, initial_center_count,
                nullptr, working_centers, allocator));

  // Stage 4 validates serial Compact: remove empty centers, rebind member rows
  // to dense cids and remap G0 into optional G1.
  TestCenterBuffer final_centers(initial_center_count, dimension);
  TestMemberRows final_members(initial_center_count);
  TestNeighborhood final_neighborhood(initial_center_count);
  TestCidRemap old_to_new(initial_center_count);
  int64_t final_center_count = 0;
  ASSERT_EQ(OB_SUCCESS, hca_refine_compact_clusters(
                            working_centers, refined_members,
                            &initial_neighborhood, final_centers, final_members,
                            &final_neighborhood, old_to_new, final_center_count,
                            allocator));
  ASSERT_GT(final_center_count, 1);
  ASSERT_LE(final_center_count, initial_center_count);

  const ObHcaNeighborhoodStore *final_neighborhood_ptr =
      final_center_count > OB_HCA_NEIGHBOR_COUNT ? &final_neighborhood
                                                 : nullptr;

  // Stage 5 validates output materialization. A1 must match compacted
  // FinalMemberLists; A2 is either -1 or a legal non-primary SOAR cid.
  TestAssignmentBuffer primary_assignments(vector_count);
  TestAssignmentBuffer secondary_assignments(vector_count);
  TestMemberRows secondary_members(final_center_count);
  ObHcaSoarParam soar_param;
  TestAssignmentBuffer invalid_primary_assignments(vector_count);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            hca_assign_soar_range(
                all_vectors, final_centers, refined_assignments, old_to_new,
                final_neighborhood_ptr, soar_param, invalid_primary_assignments,
                &secondary_assignments, nullptr, allocator));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            hca_assign_soar_range(
                all_vectors, final_centers, refined_assignments, old_to_new,
                final_neighborhood_ptr, soar_param, invalid_primary_assignments,
                nullptr, &secondary_members, allocator));
  ASSERT_EQ(OB_SUCCESS,
            hca_assign_soar_range(
                all_vectors, final_centers, refined_assignments, old_to_new,
                final_neighborhood_ptr, soar_param, primary_assignments,
                &secondary_assignments, &secondary_members, allocator));

  expect_assignment_members_consistent(primary_assignments, final_members,
                                       vector_count, final_center_count);
  expect_secondary_assignment_members_consistent(
      primary_assignments, secondary_assignments, secondary_members,
      vector_count, final_center_count);
  for (int64_t vector_idx = 0; vector_idx < vector_count; ++vector_idx) {
    int32_t primary_cid = -1;
    int32_t secondary_cid = -1;
    ASSERT_EQ(OB_SUCCESS,
              primary_assignments.get_assignment(vector_idx, primary_cid));
    ASSERT_EQ(OB_SUCCESS,
              secondary_assignments.get_assignment(vector_idx, secondary_cid));
    EXPECT_GE(primary_cid, 0);
    EXPECT_LT(primary_cid, final_center_count);
    EXPECT_TRUE(secondary_cid == -1 ||
                (secondary_cid >= 0 && secondary_cid < final_center_count));
    EXPECT_NE(primary_cid, secondary_cid);
  }

  for (int64_t cid = 0; cid < final_center_count; ++cid) {
    const float *center = nullptr;
    ASSERT_EQ(OB_SUCCESS, final_centers.get_center(cid, center));
    ASSERT_NE(nullptr, center);
    for (int64_t d = 0; d < dimension; ++d) {
      EXPECT_TRUE(std::isfinite(center[d]));
    }
  }
}

TEST(TestVectorHcaEndToEnd, ParallelFullPipeline) {
  const int64_t vector_count = 384;
  const int64_t dimension = 2;
  const int64_t cluster_size = 2;
  TestVectorStore vectors(vector_count, dimension);
  std::vector<int64_t> all_vector_indices(vector_count);
  std::vector<float> vector(dimension, 0.0F);
  for (int64_t i = 0; i < vector_count; ++i) {
    all_vector_indices[i] = i;
    vector[0] = static_cast<float>(i) * 4.0F;
    vector[1] = static_cast<float>((i * 17) % 23) * 0.05F;
    ASSERT_EQ(OB_SUCCESS, vectors.set_vector(i, vector.data()));
  }
  common::ObArenaAllocator serial_allocator(common::ObModIds::TEST);
  TestThreadPool thread_pool;
  ASSERT_EQ(OB_SUCCESS, thread_pool.start(TEST_PARALLEL_WORKER_COUNT));
  TestHcaCoordinator coordinator(thread_pool);

  // Stage 1 simulates two hierarchy scheduling strategies. The root splits
  // Assignment by vector_idx position and Stats by cid; later same-level child
  // nodes execute complete node-local pipelines in parallel. Concurrent leaf
  // publication fills the shared C0/A0/InitialMemberLists objects.
  TestCenterBuffer initial_centers(vector_count, dimension);
  TestAssignmentBuffer initial_assignments(vector_count);
  TestMemberRows initial_members(vector_count);
  ASSERT_EQ(OB_SUCCESS,
            coordinator.run_hierarchy(vectors, cluster_size, initial_centers,
                                      initial_assignments, initial_members));
  const int64_t initial_center_count = initial_centers.count();
  ASSERT_GT(initial_center_count, OB_HCA_NEIGHBOR_COUNT);
  ASSERT_LT(initial_center_count, vector_count);
  expect_assignment_members_consistent(initial_assignments, initial_members,
                                       vector_count, initial_center_count);

  // Stage 2 divides G0 by cid range. Each task owns complete output rows and
  // allocates its search scratch independently; only the center accessor is
  // shared read-only.
  TestNeighborhood initial_neighborhood(initial_center_count);
  ObHcaNeighborhoodParam neighborhood_param;
  ASSERT_EQ(
      OB_SUCCESS,
      coordinator.run_ranges(
          initial_center_count, TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_cid, const int64_t end_cid) {
            common::ObArenaAllocator task_allocator(common::ObModIds::TEST);
            return hca_build_neighborhood_rows(
                nullptr, initial_centers, begin_cid, end_cid,
                neighborhood_param, initial_neighborhood, task_allocator,
                nullptr);
          }));
  expect_neighborhood_rows_consistent(initial_centers, initial_neighborhood,
                                      OB_HCA_NEIGHBOR_COUNT);

  // Stage 3 keeps Balanced AS Train serial, then divides final Assignment by
  // vector_idx position. Member rows are shared and locked per cid. ChangedCids
  // is task-local and is OR-merged only after all Assignment tasks join.
  TestCenterBuffer working_centers(initial_center_count, dimension);
  ObHcaRefineParam refine_param;
  ASSERT_EQ(OB_SUCCESS, hca_refine_train_centers(
                            vectors, initial_centers, initial_assignments,
                            &initial_neighborhood, refine_param, nullptr,
                            working_centers, serial_allocator));

  TestAssignmentBuffer refined_assignments(vector_count);
  TestMemberRows refined_members(initial_center_count);
  std::vector<TestChangedBitmap> task_changed_cids;
  for (int64_t task_idx = 0; task_idx < TEST_PARALLEL_WORKER_COUNT;
       ++task_idx) {
    task_changed_cids.emplace_back(initial_center_count);
  }
  ASSERT_EQ(OB_SUCCESS,
            coordinator.run_ranges(
                vector_count, TEST_PARALLEL_WORKER_COUNT,
                [&](const int64_t task_idx, const int64_t begin_pos,
                    const int64_t end_pos) {
                  TestVectorBatch task_vectors(vectors, all_vector_indices,
                                               begin_pos, end_pos);
                  return hca_refine_assign_range(
                      task_vectors, working_centers, initial_assignments,
                      &initial_neighborhood, refined_assignments,
                      refined_members, task_changed_cids[task_idx]);
                }));
  TestChangedBitmap changed_cids(initial_center_count);
  for (int64_t task_idx = 0; task_idx < TEST_PARALLEL_WORKER_COUNT;
       ++task_idx) {
    ASSERT_EQ(OB_SUCCESS, changed_cids.merge_from(task_changed_cids[task_idx]));
  }

  // The Assignment barrier makes RefinedMemberLists immutable. Center Update
  // therefore divides by cid range and writes disjoint center/count rows.
  ASSERT_EQ(
      OB_SUCCESS,
      coordinator.run_ranges(
          initial_center_count, TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_cid, const int64_t end_cid) {
            common::ObArenaAllocator task_allocator(common::ObModIds::TEST);
            return hca_refine_update_centers_range(
                vectors, refined_members, changed_cids, begin_cid, end_cid,
                nullptr, working_centers, task_allocator);
          }));

  // Compact remains serial because it creates the dense old-to-new cid map.
  // Its output is immutable before parallel A1/A2 materialization begins.
  TestCenterBuffer final_centers(initial_center_count, dimension);
  TestMemberRows final_members(initial_center_count);
  TestNeighborhood final_neighborhood(initial_center_count);
  TestCidRemap old_to_new(initial_center_count);
  int64_t final_center_count = 0;
  ASSERT_EQ(OB_SUCCESS, hca_refine_compact_clusters(
                            working_centers, refined_members,
                            &initial_neighborhood, final_centers, final_members,
                            &final_neighborhood, old_to_new, final_center_count,
                            serial_allocator));
  ASSERT_GT(final_center_count, 1);
  ASSERT_LE(final_center_count, initial_center_count);
  const ObHcaNeighborhoodStore *final_neighborhood_ptr =
      final_center_count > OB_HCA_NEIGHBOR_COUNT ? &final_neighborhood
                                                 : nullptr;

  // Stage 5 partitions SOAR by vector_idx position. Every task owns disjoint
  // A1/A2 cells while reading the shared final centers/remap/G1 snapshots.
  TestAssignmentBuffer primary_assignments(vector_count);
  TestAssignmentBuffer secondary_assignments(vector_count);
  TestMemberRows secondary_members(final_center_count);
  ObHcaSoarParam soar_param;
  ASSERT_EQ(
      OB_SUCCESS,
      coordinator.run_ranges(
          vector_count, TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_pos, const int64_t end_pos) {
            common::ObArenaAllocator task_allocator(common::ObModIds::TEST);
            TestVectorBatch task_vectors(vectors, all_vector_indices, begin_pos,
                                         end_pos);
            return hca_assign_soar_range(
                task_vectors, final_centers, refined_assignments, old_to_new,
                final_neighborhood_ptr, soar_param, primary_assignments,
                &secondary_assignments, &secondary_members, task_allocator);
          }));

  expect_assignment_members_consistent(primary_assignments, final_members,
                                       vector_count, final_center_count);
  expect_secondary_assignment_members_consistent(
      primary_assignments, secondary_assignments, secondary_members,
      vector_count, final_center_count);
  for (int64_t vector_idx = 0; vector_idx < vector_count; ++vector_idx) {
    int32_t primary_cid = -1;
    int32_t secondary_cid = -1;
    ASSERT_EQ(OB_SUCCESS,
              primary_assignments.get_assignment(vector_idx, primary_cid));
    ASSERT_EQ(OB_SUCCESS,
              secondary_assignments.get_assignment(vector_idx, secondary_cid));
    EXPECT_GE(primary_cid, 0);
    EXPECT_LT(primary_cid, final_center_count);
    EXPECT_TRUE(secondary_cid == -1 ||
                (secondary_cid >= 0 && secondary_cid < final_center_count));
    EXPECT_NE(primary_cid, secondary_cid);
  }
}

void run_large_balanced_clustering(const TestDistanceType distance_type) {
  const int64_t vector_count = 20000;
  const int64_t dimension = 16;
  const int64_t cluster_size = 100;
  const int64_t source_cluster_count = vector_count / cluster_size;
  ObVectorNormalizeInfo normalize_info;
  const ObVectorNormalizeInfo *center_norm_info =
      TestDistanceType::L2 == distance_type ? nullptr : &normalize_info;
  TestVectorStore vectors(vector_count, dimension);
  std::vector<int64_t> all_vector_indices(vector_count);
  std::vector<float> vector(dimension, 0.0F);

  // Construct 200 deterministic, well-separated source clusters with exactly
  // 100 vectors each. vector_idx interleaves the source clusters so neither
  // reservoir sampling nor range partitioning receives cluster-sorted input.
  for (int64_t vector_idx = 0; vector_idx < vector_count; ++vector_idx) {
    all_vector_indices[vector_idx] = vector_idx;
    const int64_t source_cid = vector_idx % source_cluster_count;
    const int64_t point_idx = vector_idx / source_cluster_count;
    for (int64_t d = 0; d < dimension; ++d) {
      const int64_t center_code =
          (source_cid * (d * 29 + 17) + d * 43) % 257;
      const int64_t noise_code =
          (point_idx * (d * 13 + 7) + source_cid * 5 + d * 11) % 101 - 50;
      vector[d] = static_cast<float>(center_code) * 8.0F +
                  static_cast<float>(noise_code) * 0.01F;
    }
    if (TestDistanceType::COSINE == distance_type ||
        TestDistanceType::INNER_PRODUCT == distance_type) {
      ASSERT_EQ(OB_SUCCESS, normalize_info.normalize_func_(
                                dimension, vector.data(), vector.data(),
                                nullptr));
      if (TestDistanceType::INNER_PRODUCT == distance_type) {
        // HCA receives non-unit IP vectors. Keep their norms bounded so this
        // synthetic test exercises both direction and magnitude without the
        // original thousand-scale norms dominating Balanced OT temperature.
        const float norm = 1.25F + static_cast<float>(point_idx) * 0.002F;
        for (int64_t d = 0; d < dimension; ++d) {
          vector[d] *= norm;
        }
      }
    }
    ASSERT_EQ(OB_SUCCESS, vectors.set_vector(vector_idx, vector.data()));
  }
  if (TestDistanceType::L2 != distance_type) {
    const float *first_vector = nullptr;
    ASSERT_EQ(OB_SUCCESS, vectors.get_vector(0, first_vector));
    long double squared_norm = 0.0L;
    for (int64_t d = 0; d < dimension; ++d) {
      squared_norm +=
          static_cast<long double>(first_vector[d]) * first_vector[d];
    }
    const double actual_norm =
        std::sqrt(static_cast<double>(squared_norm));
    const double expected_norm =
        TestDistanceType::COSINE == distance_type ? 1.0 : 1.25;
    EXPECT_NEAR(expected_norm, actual_norm, 1e-4);
  }

  TestThreadPool thread_pool;
  ASSERT_EQ(OB_SUCCESS, thread_pool.start(TEST_PARALLEL_WORKER_COUNT));
  TestHcaCoordinator coordinator(thread_pool);
  common::ObArenaAllocator serial_allocator(common::ObModIds::TEST);

  // Run the complete primary-clustering path with the same upper-layer range
  // scheduling model as ParallelFullPipeline.
  TestCenterBuffer initial_centers(vector_count, dimension);
  TestAssignmentBuffer initial_assignments(vector_count);
  TestMemberRows initial_members(vector_count);
  ASSERT_EQ(OB_SUCCESS,
            coordinator.run_hierarchy(vectors, cluster_size, initial_centers,
                                      initial_assignments, initial_members,
                                      center_norm_info));
  const int64_t initial_center_count = initial_centers.count();
  ASSERT_GT(initial_center_count, OB_HCA_NEIGHBOR_COUNT);
  ASSERT_LT(initial_center_count, vector_count);
  expect_assignment_members_consistent(initial_assignments, initial_members,
                                       vector_count, initial_center_count);
  if (OB_NOT_NULL(center_norm_info)) {
    expect_test_centers_normalized(initial_centers);
  }

  TestNeighborhood initial_neighborhood(initial_center_count);
  ObHcaNeighborhoodParam neighborhood_param;
  ASSERT_EQ(
      OB_SUCCESS,
      coordinator.run_ranges(
          initial_center_count, TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_cid, const int64_t end_cid) {
            common::ObArenaAllocator task_allocator(common::ObModIds::TEST);
            return hca_build_neighborhood_rows(
                nullptr, initial_centers, begin_cid, end_cid,
                neighborhood_param, initial_neighborhood, task_allocator,
                nullptr);
          }));

  TestCenterBuffer working_centers(initial_center_count, dimension);
  ObHcaRefineParam refine_param;
  ASSERT_EQ(OB_SUCCESS, hca_refine_train_centers(
                            vectors, initial_centers, initial_assignments,
                            &initial_neighborhood, refine_param,
                            center_norm_info, working_centers,
                            serial_allocator));
  if (OB_NOT_NULL(center_norm_info)) {
    expect_test_centers_normalized(working_centers);
  }

  TestAssignmentBuffer refined_assignments(vector_count);
  TestMemberRows refined_members(initial_center_count);
  std::vector<TestChangedBitmap> task_changed_cids;
  for (int64_t task_idx = 0; task_idx < TEST_PARALLEL_WORKER_COUNT;
       ++task_idx) {
    task_changed_cids.emplace_back(initial_center_count);
  }
  ASSERT_EQ(OB_SUCCESS,
            coordinator.run_ranges(
                vector_count, TEST_PARALLEL_WORKER_COUNT,
                [&](const int64_t task_idx, const int64_t begin_pos,
                    const int64_t end_pos) {
                  TestVectorBatch task_vectors(vectors, all_vector_indices,
                                               begin_pos, end_pos);
                  return hca_refine_assign_range(
                      task_vectors, working_centers, initial_assignments,
                      &initial_neighborhood, refined_assignments,
                      refined_members, task_changed_cids[task_idx]);
                }));
  TestChangedBitmap changed_cids(initial_center_count);
  for (int64_t task_idx = 0; task_idx < TEST_PARALLEL_WORKER_COUNT;
       ++task_idx) {
    ASSERT_EQ(OB_SUCCESS, changed_cids.merge_from(task_changed_cids[task_idx]));
  }

  ASSERT_EQ(
      OB_SUCCESS,
      coordinator.run_ranges(
          initial_center_count, TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_cid, const int64_t end_cid) {
            common::ObArenaAllocator task_allocator(common::ObModIds::TEST);
            return hca_refine_update_centers_range(
                vectors, refined_members, changed_cids, begin_cid, end_cid,
                center_norm_info, working_centers, task_allocator);
          }));

  TestCenterBuffer final_centers(initial_center_count, dimension);
  TestMemberRows final_members(initial_center_count);
  TestNeighborhood final_neighborhood(initial_center_count);
  TestCidRemap old_to_new(initial_center_count);
  int64_t final_center_count = 0;
  ASSERT_EQ(OB_SUCCESS, hca_refine_compact_clusters(
                            working_centers, refined_members,
                            &initial_neighborhood, final_centers, final_members,
                            &final_neighborhood, old_to_new, final_center_count,
                            serial_allocator));
  ASSERT_GT(final_center_count, OB_HCA_NEIGHBOR_COUNT);
  ASSERT_LE(final_center_count, initial_center_count);
  if (OB_NOT_NULL(center_norm_info)) {
    expect_test_centers_normalized(final_centers);
  }

  // Materialize A1 and SOAR A2 together. FinalMemberLists remains the primary
  // reverse mapping; SecondaryMemberLists is the redundant reverse mapping
  // scanned by the Flat recall evaluation below.
  TestAssignmentBuffer primary_assignments(vector_count);
  TestAssignmentBuffer secondary_assignments(vector_count);
  TestMemberRows secondary_members(final_center_count);
  ObHcaSoarParam soar_param;
  ASSERT_EQ(
      OB_SUCCESS,
      coordinator.run_ranges(
          vector_count, TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_pos, const int64_t end_pos) {
            common::ObArenaAllocator task_allocator(common::ObModIds::TEST);
            TestVectorBatch task_vectors(vectors, all_vector_indices, begin_pos,
                                         end_pos);
            return hca_assign_soar_range(
                task_vectors, final_centers, refined_assignments, old_to_new,
                &final_neighborhood, soar_param, primary_assignments,
                &secondary_assignments, &secondary_members, task_allocator);
          }));
  expect_assignment_members_consistent(primary_assignments, final_members,
                                       vector_count, final_center_count);
  expect_secondary_assignment_members_consistent(
      primary_assignments, secondary_assignments, secondary_members,
      vector_count, final_center_count);

  // Evaluate the known source labels without relying on global cid order.
  // Parallel member appends can change the next level's reservoir sample, so
  // exact K and an exact source-to-output-cid mapping are not contracts.
  std::vector<int64_t> member_counts(final_center_count, 0);
  std::vector<int64_t> source_counts(source_cluster_count, 0);
  int64_t majority_member_count = 0;
  int64_t min_member_count = vector_count;
  int64_t max_member_count = 0;
  long double squared_count_sum = 0.0L;
  for (int64_t cid = 0; cid < final_center_count; ++cid) {
    const int64_t member_count = final_members.member_count(cid);
    member_counts[cid] = member_count;
    ASSERT_GT(member_count, 0);
    min_member_count = std::min(min_member_count, member_count);
    max_member_count = std::max(max_member_count, member_count);
    squared_count_sum += static_cast<long double>(member_count) * member_count;
    std::fill(source_counts.begin(), source_counts.end(), 0);
    for (int64_t offset = 0; offset < member_count; ++offset) {
      int64_t vector_idx = -1;
      ASSERT_EQ(OB_SUCCESS,
                final_members.get_member(cid, offset, vector_idx));
      ++source_counts[vector_idx % source_cluster_count];
    }
    majority_member_count +=
        *std::max_element(source_counts.begin(), source_counts.end());
  }
  lib::ob_sort(member_counts.begin(), member_counts.end());
  const long double mean =
      static_cast<long double>(vector_count) / final_center_count;
  const long double variance =
      squared_count_sum / final_center_count - mean * mean;
  const long double stddev = std::sqrt(std::max(0.0L, variance));
  const long double purity =
      static_cast<long double>(majority_member_count) / vector_count;
  const int64_t p10 = member_counts[(final_center_count - 1) / 10];
  const int64_t p90 = member_counts[(final_center_count - 1) * 9 / 10];
  const long double coefficient_of_variation = stddev / mean;
  // cluster_size is a target rather than a hard capacity. Hierarchical splits
  // may produce up to roughly 1.5 times the theoretical center count, so size
  // distribution is evaluated around the actual mean instead of requiring
  // every cluster to remain close to cluster_size.
  EXPECT_GE(final_center_count, source_cluster_count * 95 / 100);
  EXPECT_LE(final_center_count, source_cluster_count * 3 / 2);
  EXPECT_GE(static_cast<long double>(p10), mean * 0.5L);
  EXPECT_LE(static_cast<long double>(p90), mean * 1.5L);
  // IP magnitude variation can create an occasional larger tail cluster.
  // p10/p90, coefficient of variation and purity remain the primary global
  // balance checks; cluster_size is a target rather than a hard capacity.
  const int64_t max_cluster_size_factor =
      TestDistanceType::INNER_PRODUCT == distance_type ? 3 : 2;
  EXPECT_LE(max_member_count, cluster_size * max_cluster_size_factor);
  EXPECT_LE(coefficient_of_variation, 0.5L)
      << "min=" << min_member_count << ", max=" << max_member_count;
  EXPECT_GE(purity, 0.99L);

  // Build deterministic boundary queries from mutual nearest-center pairs.
  // These queries make nprobe=1 exercise the exact case SOAR targets: true
  // nearest vectors lie on both sides of a cluster boundary.
  std::vector<std::pair<int32_t, int32_t>> boundary_pairs;
  for (int64_t cid = 0; cid < final_center_count; ++cid) {
    int32_t neighbor_cid = -1;
    int32_t reverse_neighbor_cid = -1;
    ASSERT_EQ(OB_SUCCESS,
              get_test_nearest_neighbor(final_centers, final_neighborhood, cid,
                                        neighbor_cid));
    ASSERT_EQ(OB_SUCCESS,
              get_test_nearest_neighbor(final_centers, final_neighborhood,
                                        neighbor_cid, reverse_neighbor_cid));
    if (reverse_neighbor_cid == cid && cid < neighbor_cid) {
      boundary_pairs.emplace_back(static_cast<int32_t>(cid), neighbor_cid);
    }
  }
  ASSERT_FALSE(boundary_pairs.empty());

  static const int64_t RECALL_TOPK = 10;
  static const int64_t MAX_RECALL_QUERY_COUNT = 32;
  static const int64_t RECALL_NPROBE_COUNT = 4;
  const int64_t recall_nprobes[RECALL_NPROBE_COUNT] = {1, 2, 4, 8};
  std::vector<float> query(dimension, 0.0F);
  std::vector<uint8_t> seen(vector_count, 0);
  std::vector<TestFlatResult> center_ranking;
  std::vector<int64_t> primary_candidates;
  std::vector<int64_t> soar_candidates;
  std::vector<int64_t> ground_truth_topk;
  std::vector<int64_t> primary_topk;
  std::vector<int64_t> soar_topk;
  int64_t evaluated_query_count = 0;
  int64_t primary_hit_counts[RECALL_NPROBE_COUNT] = {0};
  int64_t soar_hit_counts[RECALL_NPROBE_COUNT] = {0};
  int64_t primary_candidate_counts[RECALL_NPROBE_COUNT] = {0};
  int64_t soar_candidate_counts[RECALL_NPROBE_COUNT] = {0};
  for (int64_t pair_idx = 0;
       pair_idx < static_cast<int64_t>(boundary_pairs.size()) &&
       evaluated_query_count < MAX_RECALL_QUERY_COUNT;
       ++pair_idx) {
    const float *left_center = nullptr;
    const float *right_center = nullptr;
    ASSERT_EQ(OB_SUCCESS, final_centers.get_center(
                              boundary_pairs[pair_idx].first, left_center));
    ASSERT_EQ(OB_SUCCESS, final_centers.get_center(
                              boundary_pairs[pair_idx].second, right_center));
    for (int64_t d = 0; d < dimension; ++d) {
      query[d] = (left_center[d] + right_center[d]) * 0.5F;
    }

    center_ranking.clear();
    center_ranking.reserve(final_center_count);
    for (int64_t cid = 0; cid < final_center_count; ++cid) {
      const float *center = nullptr;
      float distance = 0.0F;
      ASSERT_EQ(OB_SUCCESS, final_centers.get_center(cid, center));
      ASSERT_EQ(OB_SUCCESS,
                get_test_distance(query.data(), center, dimension,
                                  distance_type, distance));
      center_ranking.emplace_back(distance, cid);
    }
    lib::ob_sort(center_ranking.begin(), center_ranking.end(),
                 test_flat_result_less);
    if (final_members.member_count(center_ranking[0].vector_idx_) <
        RECALL_TOPK) {
      continue;
    }

    ASSERT_EQ(OB_SUCCESS,
              get_test_flat_topk(vectors, query.data(), distance_type, nullptr,
                                 RECALL_TOPK, ground_truth_topk));
    for (int64_t nprobe_idx = 0; nprobe_idx < RECALL_NPROBE_COUNT;
         ++nprobe_idx) {
      std::fill(seen.begin(), seen.end(), 0);
      primary_candidates.clear();
      for (int64_t probe_idx = 0;
           probe_idx < recall_nprobes[nprobe_idx]; ++probe_idx) {
        ASSERT_EQ(OB_SUCCESS,
                  append_test_flat_candidates(
                      final_members, center_ranking[probe_idx].vector_idx_,
                      seen, primary_candidates));
      }
      soar_candidates = primary_candidates;
      for (int64_t probe_idx = 0;
           probe_idx < recall_nprobes[nprobe_idx]; ++probe_idx) {
        ASSERT_EQ(OB_SUCCESS,
                  append_test_flat_candidates(
                      secondary_members, center_ranking[probe_idx].vector_idx_,
                      seen, soar_candidates));
      }
      ASSERT_EQ(OB_SUCCESS,
                get_test_flat_topk(vectors, query.data(), distance_type,
                                   &primary_candidates, RECALL_TOPK,
                                   primary_topk));
      ASSERT_EQ(OB_SUCCESS,
                get_test_flat_topk(vectors, query.data(), distance_type,
                                   &soar_candidates, RECALL_TOPK, soar_topk));

      const int64_t query_primary_hits =
          count_test_recall_hits(ground_truth_topk, primary_topk);
      const int64_t query_soar_hits =
          count_test_recall_hits(ground_truth_topk, soar_topk);
      EXPECT_GE(query_soar_hits, query_primary_hits);
      primary_hit_counts[nprobe_idx] += query_primary_hits;
      soar_hit_counts[nprobe_idx] += query_soar_hits;
      primary_candidate_counts[nprobe_idx] +=
          static_cast<int64_t>(primary_candidates.size());
      soar_candidate_counts[nprobe_idx] +=
          static_cast<int64_t>(soar_candidates.size());
    }
    ++evaluated_query_count;
  }
  ASSERT_GT(evaluated_query_count, 0);
  const long double recall_denominator =
      static_cast<long double>(evaluated_query_count * RECALL_TOPK);
  int64_t primary_nprobe_at_90 = -1;
  int64_t soar_nprobe_at_90 = -1;
  long double previous_primary_recall = 0.0L;
  long double previous_soar_recall = 0.0L;
  for (int64_t nprobe_idx = 0; nprobe_idx < RECALL_NPROBE_COUNT;
       ++nprobe_idx) {
    const long double primary_recall =
        primary_hit_counts[nprobe_idx] / recall_denominator;
    const long double soar_recall =
        soar_hit_counts[nprobe_idx] / recall_denominator;
    EXPECT_GE(primary_recall, previous_primary_recall);
    EXPECT_GE(soar_recall, previous_soar_recall);
    EXPECT_GE(soar_recall, primary_recall);
    EXPECT_GT(soar_candidate_counts[nprobe_idx],
              primary_candidate_counts[nprobe_idx]);
    if (primary_nprobe_at_90 < 0 && primary_recall >= 0.9L) {
      primary_nprobe_at_90 = recall_nprobes[nprobe_idx];
    }
    if (soar_nprobe_at_90 < 0 && soar_recall >= 0.9L) {
      soar_nprobe_at_90 = recall_nprobes[nprobe_idx];
    }
    std::fprintf(stderr,
                 "[LargeBalancedClustering Flat Recall@10] distance=%s "
                 "queries=%ld "
                 "nprobe=%ld primary=%.4Lf soar=%.4Lf "
                 "avg_primary_candidates=%.2Lf "
                 "avg_soar_candidates=%.2Lf\n",
                 get_test_distance_name(distance_type), evaluated_query_count,
                 recall_nprobes[nprobe_idx], primary_recall, soar_recall,
                 static_cast<long double>(
                     primary_candidate_counts[nprobe_idx]) /
                     evaluated_query_count,
                 static_cast<long double>(soar_candidate_counts[nprobe_idx]) /
                     evaluated_query_count);
    previous_primary_recall = primary_recall;
    previous_soar_recall = soar_recall;
  }
  EXPECT_GT(primary_nprobe_at_90, 0);
  EXPECT_GT(soar_nprobe_at_90, 0);
  EXPECT_LE(soar_nprobe_at_90, primary_nprobe_at_90);
  std::fprintf(stderr,
               "[LargeBalancedClustering Recall@10 >= 0.90] distance=%s "
               "primary_nprobe=%ld soar_nprobe=%ld\n",
               get_test_distance_name(distance_type), primary_nprobe_at_90,
               soar_nprobe_at_90);
}

TEST(TestVectorHcaEndToEnd, LargeBalancedClustering) {
  run_large_balanced_clustering(TestDistanceType::L2);
}

TEST(TestVectorHcaEndToEnd, LargeBalancedClusteringCosine) {
  run_large_balanced_clustering(TestDistanceType::COSINE);
}

TEST(TestVectorHcaEndToEnd, LargeBalancedClusteringInnerProduct) {
  run_large_balanced_clustering(TestDistanceType::INNER_PRODUCT);
}

TEST(TestVectorHcaEndToEnd, ConcurrentHGraphAdd) {
  const int64_t center_count = 512;
  const int64_t dimension = 8;
  TestThreadPool thread_pool;
  ASSERT_EQ(OB_SUCCESS, thread_pool.start(TEST_PARALLEL_WORKER_COUNT));
  TestHcaCoordinator coordinator(thread_pool);
  TestCenterBuffer centers(center_count, dimension);
  std::vector<float> center(dimension, 0.0F);
  for (int64_t cid = 0; cid < center_count; ++cid) {
    for (int64_t d = 0; d < dimension; ++d) {
      center[d] = static_cast<float>((cid * 37 + d * 17) % 101) +
                  static_cast<float>(cid) * 0.001F;
    }
    ASSERT_EQ(OB_SUCCESS, centers.set_center(cid, center.data()));
  }

  ASSERT_TRUE(common::obvsag::is_init());
  TestVsagAllocator vsag_allocator;
  common::obvsag::VectorIndexPtr shared_index = nullptr;
  TestVsagIndexGuard index_guard(shared_index);
  ASSERT_EQ(OB_SUCCESS,
            common::obvectorutil::create_index(
                shared_index, common::obvsag::HGRAPH_TYPE, "float32", "l2",
                dimension, 16, 200, OB_HCA_HGRAPH_QUERY_TOPK, &vsag_allocator));
  ASSERT_NE(nullptr, shared_index);

  // Four upper-layer tasks own disjoint cid ranges but call Add concurrently
  // on the same mutable HGraph. The range API adds each center independently.
  ObHcaNeighborhoodParam neighborhood_param;
  ASSERT_EQ(
      OB_SUCCESS,
      coordinator.run_ranges(
          center_count, TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_cid, const int64_t end_cid) {
            return hca_add_centers_to_index(shared_index, centers, begin_cid,
                                            end_cid);
          }));

  // The Add barrier must expose one graph element for every cid. HCA shifts
  // VSAG ids by one, so the complete expected id domain is [1, center_count].
  int64_t actual_count = 0;
  ASSERT_EQ(OB_SUCCESS,
            common::obvectorutil::get_index_number(shared_index, actual_count));
  ASSERT_EQ(center_count, actual_count);
  for (int64_t vsag_id = 1; vsag_id <= center_count; ++vsag_id) {
    bool exists = false;
    ASSERT_EQ(OB_SUCCESS, common::obvectorutil::check_id_exist(
                              shared_index, vsag_id, exists));
    ASSERT_TRUE(exists) << "missing concurrently added VSAG id " << vsag_id;
  }

  // No query starts before the immutable barrier. Probe several centers after
  // it through the production range API and shared FIFO worker pool.
  ASSERT_EQ(OB_SUCCESS, common::obvectorutil::immutable_optimize(shared_index));
  TestNeighborhood graph_neighborhood(center_count);
  ASSERT_EQ(
      OB_SUCCESS,
      coordinator.run_ranges(
          center_count, TEST_PARALLEL_WORKER_COUNT,
          [&](const int64_t, const int64_t begin_cid, const int64_t end_cid) {
            common::ObArenaAllocator task_allocator(common::ObModIds::TEST);
            TestVsagAllocator search_allocator;
            int task_ret = hca_build_neighborhood_rows(
                shared_index, centers, begin_cid, end_cid, neighborhood_param,
                graph_neighborhood, task_allocator, &search_allocator);
            if (OB_SUCCESS == task_ret &&
                0 != search_allocator.outstanding_bytes()) {
              task_ret = OB_ERR_UNEXPECTED;
            }
            return task_ret;
          }));
  expect_neighborhood_rows_consistent(centers, graph_neighborhood,
                                      OB_HCA_NEIGHBOR_COUNT);

  // Direct probes retain a small adaptor-level smoke check in addition to the
  // complete production Neighborhood path above.
  for (int64_t cid = 0; cid < center_count; cid += center_count / 8) {
    const float *query = nullptr;
    const float *distances = nullptr;
    const int64_t *ids = nullptr;
    TestVsagSearchResultGuard search_result_guard(vsag_allocator, distances,
                                                  ids);
    const char *extra_info = nullptr;
    int64_t result_count = 0;
    ASSERT_EQ(OB_SUCCESS, centers.get_center(cid, query));
    ASSERT_EQ(OB_SUCCESS, common::obvectorutil::knn_search(
                              shared_index, const_cast<float *>(query),
                              dimension, 16, distances, ids, extra_info,
                              result_count, OB_HCA_HGRAPH_QUERY_TOPK, nullptr,
                              false, false, 1.0F, &vsag_allocator, false));
    ASSERT_GT(result_count, 0);
    ASSERT_NE(nullptr, distances);
    ASSERT_NE(nullptr, ids);
    for (int64_t result_idx = 0; result_idx < result_count; ++result_idx) {
      EXPECT_GE(ids[result_idx], 1);
      EXPECT_LE(ids[result_idx], center_count);
      EXPECT_TRUE(std::isfinite(distances[result_idx]));
    }
  }
}

} // namespace
} // namespace share
} // namespace oceanbase

// ob_unittest() does not link gtest_main, so every standalone test binary owns
// its GoogleTest entry point.
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
