/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <limits>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/ob_target_specific.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "storage/blocksstable/vector_index/ob_disk_bbq_algorithm.h"
#define private public
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#undef private
#include "storage/blocksstable/vector_index/ob_ivf_refresh_monitor.h"
#include "storage/ddl/ob_ddl_dag_monitor_node.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"

#define private public
#include "storage/blocksstable/vector_index/ob_ivf_build_schema_ctx.h"
#undef private

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace share;
using namespace storage;

static_assert(1 == blocksstable::IVF_ROW_TYPE_CENTROID,
              "Disk BBQ centroid row type is persisted ABI");
static_assert(3 == blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
              "Disk BBQ low-precision row type is persisted ABI");
static_assert(5 == blocksstable::IVF_ROW_TYPE_RAW_VECTOR,
              "Disk BBQ raw-vector row type is persisted ABI");
static_assert(1 == blocksstable::IVF_SSTABLE_FORMAT_VERSION,
              "IVF SSTable format version is persisted ABI");
static_assert(1 == share::DISK_BBQ_RABITQ_BITS,
              "Disk BBQ RABITQ bits are persisted ABI");
static_assert(1 == quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION,
              "Disk BBQ FHT algorithm version is persisted ABI");

namespace
{

const uint64_t TEST_TENANT_ID = 1;
const int64_t TEST_REPLAY_MEMORY_LIMIT = 1L << 20;

// Monitor entries use the current tenant for their query-side allocator.
class ScopedRefreshTestTenant final
{
public:
  ScopedRefreshTestTenant() : tenant_(TEST_TENANT_ID), previous_(ObTenantEnv::get_tenant())
  { ObTenantEnv::set_tenant(&tenant_); }
  ~ScopedRefreshTestTenant() { ObTenantEnv::set_tenant(previous_); }
private:
  ObTenantBase tenant_;
  ObTenantBase *previous_;
};

class ToggleFailAllocator final : public common::ObIAllocator
{
public:
  ToggleFailAllocator() : allocator_("BBQFailAlloc"), fail_(false) {}

  void *alloc(const int64_t size) override
  { return fail_ ? nullptr : allocator_.alloc(size); }

  void *alloc(const int64_t size, const lib::ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }

  void free(void *ptr) override
  { allocator_.free(ptr); }

  void set_fail(const bool fail)
  { fail_ = fail; }

private:
  common::ObArenaAllocator allocator_;
  bool fail_;
};

common::ObString as_vector_bytes(const float *vector, const int64_t dim)
{
  return common::ObString(
      static_cast<int32_t>(dim * static_cast<int64_t>(sizeof(float))),
      reinterpret_cast<const char *>(vector));
}

bool sample_candidate_less(const ObIVFSampleCandidate &left,
                           const ObIVFSampleCandidate &right)
{
  return ObIVFSampleCandidate::is_better(left, right);
}

struct OwnedBuildRow
{
  int64_t row_type_;
  int64_t cid_;
  uint64_t vid_;
  std::string payload_;
};

class CollectingBuildRowSink final : public ObIVFBuildRowSink
{
public:
  int append_row(const ObIVFBuildRow &row) override
  {
    OwnedBuildRow owned_row;
    owned_row.row_type_ = row.row_type_;
    owned_row.cid_ = row.cid_;
    owned_row.vid_ = row.vid_;
    owned_row.payload_.assign(row.payload_.ptr(), row.payload_.length());
    rows_.push_back(owned_row);
    return OB_SUCCESS;
  }

  std::vector<OwnedBuildRow> rows_;
};

int64_t find_vid(const uint64_t *vids, const int64_t count,
                 const uint64_t target_vid)
{
  int64_t index = -1;
  for (int64_t i = 0; i < count && index < 0; ++i) {
    if (vids[i] == target_vid) {
      index = i;
    }
  }
  return index;
}

int prepare_disk_bbq_meta(const int32_t dim,
                          const bool is_empty,
                          blocksstable::ObIvfSSTableMeta &meta)
{
  int ret = OB_SUCCESS;
  meta.reset();
  if (is_empty) {
    meta.set_empty();
  } else if (OB_UNLIKELY(dim <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    meta.index_type_ = share::VIAT_DISK_BBQ;
    meta.dist_type_ = share::VIDA_L2;
    meta.dim_ = dim;
    meta.format_version_ = blocksstable::IVF_SSTABLE_FORMAT_VERSION;
    meta.nbits_ = share::DISK_BBQ_RABITQ_BITS;
    meta.fht_algo_version_ =
        quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
  }
  return ret;
}

} // namespace

TEST(TestDiskBBQBuild, FlatCenterPreservesSimdTailCoordinates)
{
  ScopedRefreshTestTenant tenant;
  for (const int64_t dim : {5, 9, 17, 768, 769}) {
    SCOPED_TRACE(::testing::Message() << "dim=" << dim);
    ObArenaAllocator allocator("BBQFlatTail");
    ObIVFBuildSchemaCtx schema(allocator);
    schema.vector_dim_ = dim;
    schema.vector_param_.type_ = VIAT_DISK_BBQ;
    schema.vector_param_.dist_algorithm_ = VIDA_L2;
    schema.vector_param_.nbits_ = DISK_BBQ_RABITQ_BITS;
    schema.vector_param_.cluster_size_ = 64;
    quantization::ObFhtKacRotator rotator;
    ASSERT_EQ(OB_SUCCESS, quantization::ObFhtKacRotator::generate(allocator, dim, rotator));
    std::vector<float> vector(dim);
    for (int64_t d = 0; d < dim; ++d) {
      vector[d] = static_cast<float>(d % 7 - 3);
    }
    ObIVFReplayInput replay(allocator);
    ASSERT_EQ(OB_SUCCESS, replay.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT));
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, replay.add_row(i, as_vector_bytes(vector.data(), dim)));
    }
    ASSERT_EQ(OB_SUCCESS, replay.seal());
    ObSEArray<ObIVFSampleRef, 1> samples;
    CollectingBuildRowSink sink;
    blocksstable::ObIvfSSTableMeta meta;
    int64_t centers = 0, vectors = 0;
    ASSERT_EQ(OB_SUCCESS, ObDiskBBQAlgorithm::build(TEST_TENANT_ID, schema,
        replay, rotator, samples, sink, meta, centers, vectors));
    ASSERT_EQ(7, sink.rows_.size());
    EXPECT_EQ(1, centers);
    EXPECT_EQ(3, vectors);
    const OwnedBuildRow &center = sink.rows_.back();
    ASSERT_EQ(blocksstable::IVF_ROW_TYPE_CENTROID, center.row_type_);
    EXPECT_EQ(0, center.cid_);
    EXPECT_EQ(1, center.vid_);
    const float *actual = nullptr;
    uint32_t children = 0;
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObIvfCentroidPayload::decode(
        ObString(center.payload_.size(), center.payload_.data()), dim, actual, children));
    EXPECT_EQ(3, children);
    for (int64_t d = 0; d < dim; ++d) {
      EXPECT_FLOAT_EQ(vector[d], actual[d]);
    }
  }
}

TEST(TestDiskBBQBuild, NonFlatSegmentsRequireTrainerAndRejectInvalidClusterSize)
{
  ScopedRefreshTestTenant tenant;
  const int64_t dim = 4;
  const float vector[] = {1.0F, 0.0F, 0.0F, 0.0F};
  const int64_t cases[][3] = {
    {1153, 384, OB_ERR_UNEXPECTED},
    {193, 64, OB_ERR_UNEXPECTED},
    {1, 0, OB_INVALID_ARGUMENT},
    {1, INT64_MAX, OB_INVALID_ARGUMENT}
  };
  for (const auto &test_case : cases) {
    const int64_t count = test_case[0], cluster_size = test_case[1];
    SCOPED_TRACE(::testing::Message() << "count=" << count << " cluster_size=" << cluster_size);
    ObArenaAllocator allocator("BBQThreshold");
    ObVectorIndexParam index_param;
    index_param.type_ = VIAT_DISK_BBQ;
    index_param.dim_ = dim;
    index_param.dist_algorithm_ = VIDA_L2;
    index_param.nbits_ = DISK_BBQ_RABITQ_BITS;
    index_param.cluster_size_ = cluster_size;
    ObIVFBuildSchemaCtx schema(allocator);
    schema.vector_param_ = index_param;
    schema.vector_dim_ = dim;
    quantization::ObFhtKacRotator rotator;
    ASSERT_EQ(OB_SUCCESS, quantization::ObFhtKacRotator::generate(allocator, dim, rotator));
    ObIVFReplayInput replay(allocator);
    ASSERT_EQ(OB_SUCCESS, replay.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT));
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, replay.add_row(i, as_vector_bytes(vector, dim)));
    }
    ASSERT_EQ(OB_SUCCESS, replay.seal());
    ObSEArray<ObIVFSampleRef, 1> samples;
    CollectingBuildRowSink sink;
    blocksstable::ObIvfSSTableMeta meta;
    int64_t center_count = -1, vector_count = -1;
    // This tenant has no vector service. A segment above the threshold must
    // require the normal trainer, rather than silently use the flat path.
    EXPECT_EQ(test_case[2], ObDiskBBQAlgorithm::build(TEST_TENANT_ID,
        schema, replay, rotator, samples, sink, meta,
        center_count, vector_count));
    EXPECT_EQ(0, center_count);
    EXPECT_TRUE(sink.rows_.empty());
  }
}

TEST(TestDiskBBQBuild, FlatSegmentsBuildOneMeanCenterAndPreserveEveryVector)
{
  ScopedRefreshTestTenant tenant;
  const int64_t dim = 4;
  const int64_t cases[][2] = {{0, 384}, {1, 384}, {75, 384}, {1151, 384}, {1152, 384}, {192, 64}};
  const ObVectorIndexDistAlgorithm metrics[] = {VIDA_L2, VIDA_IP, VIDA_COS};
  for (const auto metric : metrics) {
    for (const auto &test_case : cases) {
      const int64_t count = test_case[0], cluster_size = test_case[1];
      SCOPED_TRACE(::testing::Message() << "metric=" << metric << " count=" << count
                   << " cluster_size=" << cluster_size);
      ObArenaAllocator allocator("BBQFlatTest");
      ObVectorIndexParam index_param;
      index_param.dim_ = dim;
      index_param.type_ = VIAT_DISK_BBQ;
      index_param.dist_algorithm_ = metric;
      index_param.nbits_ = DISK_BBQ_RABITQ_BITS;
      index_param.cluster_size_ = cluster_size;
      // A one-row sample must not bias the flat center towards that row.
      index_param.nlist_ = 1;
      index_param.sample_per_nlist_ = 1;
      ObIVFBuildSchemaCtx schema(allocator);
      schema.vector_param_ = index_param;
      schema.vector_dim_ = dim;
      quantization::ObFhtKacRotator rotator;
      ASSERT_EQ(OB_SUCCESS, quantization::ObFhtKacRotator::generate(allocator, dim, rotator));
      ObIVFReplayInput replay(allocator);
      ASSERT_EQ(OB_SUCCESS, replay.init(TEST_TENANT_ID, dim,
          TEST_REPLAY_MEMORY_LIMIT, 1, 123, 0));
      std::vector<std::vector<float>> vectors;
      double sum[dim] = {};
      for (int64_t i = 0; i < count; ++i) {
        // Keep magnitudes bounded: IP reconstruction subtracts float32
        // squared norms, whose rounding error grows with the input norm.
        std::vector<float> vector = {float(i + 1) / count, float(i % 7 - 3) * 0.1F, 0.5F, -1.0F};
        if (VIDA_COS == metric) {
          double norm = 0;
          for (const float v : vector) { norm += double(v) * v; }
          for (float &v : vector) { v /= std::sqrt(norm); }
        }
        ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, vector.data()));
        for (int64_t d = 0; d < dim; ++d) { sum[d] += vector[d]; }
        ASSERT_EQ(OB_SUCCESS, replay.add_row(1000 + i, as_vector_bytes(vector.data(), dim)));
        vectors.push_back(vector);
      }
      ASSERT_EQ(OB_SUCCESS, replay.seal());
      ObSEArray<ObIVFSampleRef, 1> samples;
      for (int64_t i = 0; i < replay.get_sample_candidates().count(); ++i) {
        ASSERT_EQ(OB_SUCCESS, samples.push_back(
            ObIVFSampleRef(replay.get_sample_candidates().at(i), &replay)));
      }
      CollectingBuildRowSink sink;
      blocksstable::ObIvfSSTableMeta meta;
      int64_t center_count = -1, vector_count = -1;
      ASSERT_EQ(OB_SUCCESS, ObDiskBBQAlgorithm::build(TEST_TENANT_ID, schema,
          replay, rotator, samples, sink, meta, center_count, vector_count));
      ASSERT_TRUE(meta.is_valid());
      EXPECT_EQ(count, vector_count);
      EXPECT_EQ(count > 0 ? 1 : 0, center_count);
      EXPECT_EQ(center_count, meta.centroid_count_);
      EXPECT_EQ(count, meta.vector_count_);
      EXPECT_EQ(blocksstable::IVF_CENTROID_FLAT, meta.centroid_layout_);
      EXPECT_EQ(0, meta.parent_centroid_count_);
      ASSERT_EQ(center_count + 2 * count, sink.rows_.size());
      if (count > 0) {
        const OwnedBuildRow &center = sink.rows_.back();
        ASSERT_EQ(blocksstable::IVF_ROW_TYPE_CENTROID, center.row_type_);
        EXPECT_EQ(0, center.cid_);
        EXPECT_EQ(1, center.vid_);
        const float *actual = nullptr;
        uint32_t children = 0;
        ASSERT_EQ(OB_SUCCESS, blocksstable::ObIvfCentroidPayload::decode(
            ObString(center.payload_.size(), center.payload_.data()), dim, actual, children));
        EXPECT_EQ(count, children);
        double mean_norm = 0;
        for (int64_t d = 0; d < dim; ++d) { mean_norm += (sum[d] / count) * (sum[d] / count); }
        mean_norm = VIDA_L2 == metric ? 1.0 : std::sqrt(mean_norm);
        for (int64_t d = 0; d < dim; ++d) {
          EXPECT_NEAR(float(sum[d] / count / mean_norm), actual[d], 1e-6);
        }
        // A query at the center has zero residual, so its score is exact even
        // with one-bit codes. Check the persisted codes with the query reader.
        quantization::ObQuantizationParams params = {dim, DISK_BBQ_RABITQ_BITS, rotator};
        quantization::ObQuantizationComputer computer;
        ASSERT_EQ(OB_SUCCESS, computer.init(allocator,
            quantization::ObQuantizationType::RABITQ, metric, params, actual, actual));
        for (int64_t i = 0; i < count; ++i) {
          const OwnedBuildRow &posting = sink.rows_.at(2 * i);
          const OwnedBuildRow &raw = sink.rows_.at(1 + 2 * i);
          EXPECT_EQ(blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR, posting.row_type_);
          EXPECT_EQ(1, posting.cid_);
          EXPECT_EQ(1000 + i, posting.vid_);
          EXPECT_EQ(blocksstable::IVF_ROW_TYPE_RAW_VECTOR, raw.row_type_);
          EXPECT_EQ(blocksstable::IVF_RAW_VECTOR_CID, raw.cid_);
          EXPECT_EQ(1000 + i, raw.vid_);
          ASSERT_EQ(dim * sizeof(float), raw.payload_.size());
          EXPECT_EQ(0, MEMCMP(vectors[i].data(), raw.payload_.data(), dim * sizeof(float)));
          float score = 0;
          ASSERT_EQ(OB_SUCCESS, computer.compute(
              {reinterpret_cast<uint8_t *>(const_cast<char *>(posting.payload_.data())),
               static_cast<int64_t>(posting.payload_.size())}, score));
          double squared_l2 = 0, dot = 0, query_norm = 0, base_norm = 0;
          for (int64_t d = 0; d < dim; ++d) {
            const double q = actual[d], v = vectors[i][d];
            squared_l2 += (q - v) * (q - v);
            dot += q * v;
            query_norm += q * q;
            base_norm += v * v;
          }
          const double expected = VIDA_L2 == metric ? squared_l2
              : (VIDA_IP == metric ? 1.0 - dot : 1.0 - dot / std::sqrt(query_norm * base_norm));
          EXPECT_NEAR(expected, score, 1e-4 * std::max(1.0, std::abs(expected)));
        }
      }
    }
  }
}

TEST(TestDiskBBQBuild, FlatCenterSpansSlicesAndAllowsZeroMean)
{
  ScopedRefreshTestTenant tenant;
  ObArenaAllocator allocator("BBQFlatSlices");
  const int64_t dim = 3;
  const float positive[] = {1.0F, 2.0F, 3.0F};
  const float negative[] = {-1.0F, -2.0F, -3.0F};
  const float zero[] = {0.0F, 0.0F, 0.0F};
  ObIVFReplayInput first(allocator), second(allocator);
  ASSERT_EQ(OB_SUCCESS, first.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT));
  ASSERT_EQ(OB_SUCCESS, second.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT));
  ASSERT_EQ(OB_SUCCESS, first.add_row(1, as_vector_bytes(positive, dim)));
  ASSERT_EQ(OB_SUCCESS, second.add_row(2, as_vector_bytes(negative, dim)));
  ASSERT_EQ(OB_SUCCESS, second.add_row(3, as_vector_bytes(zero, dim)));
  ASSERT_EQ(OB_SUCCESS, first.seal());
  ASSERT_EQ(OB_SUCCESS, second.seal());
  ObSEArray<ObIVFReplayInput *, 2> inputs;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(&first));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(&second));
  ObIVFMultiSliceReplayReader replay;
  ASSERT_EQ(OB_SUCCESS, replay.init(dim, inputs));
  quantization::ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS, quantization::ObFhtKacRotator::generate(allocator, dim, rotator));
  ObVectorIndexParam index_param;
  index_param.dim_ = dim;
  index_param.type_ = VIAT_DISK_BBQ;
  index_param.nbits_ = DISK_BBQ_RABITQ_BITS;
  index_param.cluster_size_ = 64;
  ObSEArray<ObIVFSampleRef, 1> samples;
  for (const auto metric : {VIDA_L2, VIDA_IP, VIDA_COS}) {
    index_param.dist_algorithm_ = metric;
    ObIVFBuildSchemaCtx schema(allocator);
    schema.vector_param_ = index_param;
    schema.vector_dim_ = dim;
    CollectingBuildRowSink sink;
    blocksstable::ObIvfSSTableMeta meta;
    int64_t centers = 0, vectors = 0;
    ASSERT_EQ(OB_SUCCESS, ObDiskBBQAlgorithm::build(TEST_TENANT_ID, schema,
        replay, rotator, samples, sink, meta, centers, vectors));
    EXPECT_EQ(1, centers);
    EXPECT_EQ(3, vectors);
    ASSERT_EQ(7, sink.rows_.size());
    const OwnedBuildRow &center = sink.rows_.back();
    ASSERT_EQ(blocksstable::IVF_ROW_TYPE_CENTROID, center.row_type_);
    EXPECT_EQ(0, center.cid_);
    EXPECT_EQ(1, center.vid_);
    const float *actual = nullptr;
    uint32_t children = 0;
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObIvfCentroidPayload::decode(
        ObString(center.payload_.size(), center.payload_.data()), dim, actual, children));
    EXPECT_EQ(3, children);
    EXPECT_EQ(0, MEMCMP(zero, actual, sizeof(zero)));
  }
}

TEST(TestDiskBBQBuild, ReplayInputSamplesDuringCaptureAndRewinds)
{
  const int64_t dim = 2;
  const int64_t max_sample_count = 3;
  const int64_t slice_idx = 7;
  const uint64_t sample_seed =
      ObIVFReplayInput::build_sample_seed(500, 600);
  const uint64_t vids[] = {105, 102, 106, 101, 104, 103};
  const float vectors[][dim] = {
      {1.0F, 1.5F},
      {2.0F, 2.5F},
      {3.0F, 3.5F},
      {4.0F, 4.5F},
      {5.0F, 5.5F},
      {6.0F, 6.5F},
  };
  const int64_t row_count = ARRAYSIZEOF(vids);
  ObArenaAllocator allocator("AIVFReplayUT");
  ObIVFReplayInput replay_input(allocator);

  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, dim,
                              TEST_REPLAY_MEMORY_LIMIT, max_sample_count,
                              sample_seed, slice_idx, true /* capture_vids */));
  ASSERT_TRUE(replay_input.is_sampling_enabled());
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(OB_SUCCESS,
              replay_input.add_row(vids[i], as_vector_bytes(vectors[i], dim)));
  }
  uint64_t captured_vid = 0;
  EXPECT_EQ(OB_STATE_NOT_MATCH, replay_input.get_captured_vid(0, captured_vid));
  ASSERT_EQ(row_count, replay_input.get_row_count());
  ASSERT_EQ(row_count, replay_input.get_sample_scan_count());
  ASSERT_EQ(max_sample_count, replay_input.get_sample_count());

  std::vector<ObIVFSampleCandidate> expected_candidates;
  for (int64_t i = 0; i < row_count; ++i) {
    expected_candidates.push_back(ObIVFSampleCandidate(
        ObIVFReplayInput::calc_sample_priority(sample_seed, vids[i]),
        vids[i], slice_idx, i));
  }
  std::sort(expected_candidates.begin(), expected_candidates.end(),
            sample_candidate_less);
  expected_candidates.resize(max_sample_count);

  std::vector<ObIVFSampleCandidate> actual_candidates;
  const common::ObIArray<ObIVFSampleCandidate> &sample_candidates =
      replay_input.get_sample_candidates();
  for (int64_t i = 0; i < sample_candidates.count(); ++i) {
    actual_candidates.push_back(sample_candidates.at(i));
  }
  std::sort(actual_candidates.begin(), actual_candidates.end(),
            sample_candidate_less);
  ASSERT_EQ(expected_candidates.size(), actual_candidates.size());
  for (int64_t i = 0; i < max_sample_count; ++i) {
    EXPECT_EQ(expected_candidates[i].priority_, actual_candidates[i].priority_);
    EXPECT_EQ(expected_candidates[i].vid_, actual_candidates[i].vid_);
    EXPECT_EQ(expected_candidates[i].slice_idx_, actual_candidates[i].slice_idx_);
    EXPECT_EQ(expected_candidates[i].row_id_, actual_candidates[i].row_id_);
  }

  ASSERT_EQ(OB_SUCCESS, replay_input.seal());
  ASSERT_TRUE(replay_input.is_sealed());
  ASSERT_EQ(row_count, replay_input.get_inmemory_row_count());
  for (int64_t i = row_count - 1; i >= 0; --i) {
    ASSERT_EQ(OB_SUCCESS, replay_input.get_captured_vid(i, captured_vid));
    EXPECT_EQ(vids[i], captured_vid);
  }
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, replay_input.get_captured_vid(-1, captured_vid));
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, replay_input.get_captured_vid(row_count, captured_vid));
  // DDL discards the enumeration metadata after building its locators. Sample
  // locators and both random and sequential payload readers must remain valid.
  replay_input.release_captured_vids();
  replay_input.release_captured_vids();
  EXPECT_EQ(OB_STATE_NOT_MATCH, replay_input.get_captured_vid(0, captured_vid));
  ASSERT_EQ(max_sample_count, replay_input.get_sample_count());
  for (int64_t i = 0; i < max_sample_count; ++i) {
    uint64_t sampled_vid = 0;
    const float *sampled_vector = nullptr;
    const ObIVFSampleCandidate &candidate = actual_candidates[i];
    ASSERT_EQ(OB_SUCCESS,
              replay_input.get_row_at(candidate.row_id_, sampled_vid,
                                      sampled_vector));
    ASSERT_NE(nullptr, sampled_vector);
    EXPECT_EQ(candidate.vid_, sampled_vid);
    EXPECT_EQ(0, std::memcmp(vectors[candidate.row_id_], sampled_vector,
                             dim * sizeof(float)));
  }

  for (int64_t pass = 0; pass < 2; ++pass) {
    ASSERT_EQ(OB_SUCCESS, replay_input.rewind());
    for (int64_t i = 0; i < row_count; ++i) {
      uint64_t replayed_vid = 0;
      const float *replayed_vector = nullptr;
      ASSERT_EQ(OB_SUCCESS,
                replay_input.get_next_row(replayed_vid, replayed_vector));
      ASSERT_NE(nullptr, replayed_vector);
      EXPECT_EQ(vids[i], replayed_vid);
      EXPECT_EQ(0, std::memcmp(vectors[i], replayed_vector,
                               dim * sizeof(float)));
    }
    uint64_t end_vid = 0;
    const float *end_vector = nullptr;
    EXPECT_EQ(OB_ITER_END, replay_input.get_next_row(end_vid, end_vector));
  }
}

TEST(TestDiskBBQBuild, ReplayInputEmptyNoCacheSealsWithoutTemporaryFile)
{
  ObArenaAllocator allocator("AIVFNoCacheUT");
  ObIVFReplayInput replay_input(allocator);
  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, 3,
                              ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT));
  EXPECT_EQ(ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT,
            replay_input.get_memory_limit());
  ASSERT_EQ(OB_SUCCESS, replay_input.seal());
  ASSERT_TRUE(replay_input.is_sealed());
  EXPECT_EQ(0, replay_input.get_row_count());
  EXPECT_EQ(0, replay_input.get_inmemory_row_count());
  EXPECT_EQ(0, replay_input.get_file_size());
  EXPECT_EQ(0, replay_input.get_mem_hold());
  ASSERT_EQ(OB_SUCCESS, replay_input.rewind());
  uint64_t vid = 0;
  const float *vector = nullptr;
  EXPECT_EQ(OB_ITER_END, replay_input.get_next_row(vid, vector));
}

TEST(TestDiskBBQBuild, ReplayInputEmptyCapturedVidsHaveNoRows)
{
  ObArenaAllocator allocator("BBQEmptyVidUT");
  ObIVFReplayInput replay_input(allocator);
  ASSERT_EQ(OB_SUCCESS, replay_input.init(
      TEST_TENANT_ID, 3, ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT, 4, 123, 7, true));
  ASSERT_EQ(OB_SUCCESS, replay_input.seal());
  uint64_t vid = 0;
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, replay_input.get_captured_vid(0, vid));
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, replay_input.get_captured_vid(-1, vid));
  replay_input.release_captured_vids();
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, replay_input.get_captured_vid(0, vid));
  EXPECT_EQ(0, replay_input.get_sample_count());
  EXPECT_EQ(0, replay_input.get_file_size());
  const float *vector = nullptr;
  EXPECT_EQ(OB_ITER_END, replay_input.get_next_row(vid, vector));
}

TEST(TestDiskBBQBuild, ReplayInputPositiveCacheBudgetRetainsFinalRow)
{
  const int64_t dim = 3;
  const uint64_t expected_vid = 12345;
  const float expected_vector[dim] = {1.25F, -2.0F, 3.5F};
  ObArenaAllocator allocator("AIVFRetainUT");
  ObIVFReplayInput replay_input(allocator);
  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, dim,
                              TEST_REPLAY_MEMORY_LIMIT));
  EXPECT_EQ(TEST_REPLAY_MEMORY_LIMIT, replay_input.get_memory_limit());
  ASSERT_EQ(OB_SUCCESS,
            replay_input.add_row(expected_vid,
                as_vector_bytes(expected_vector, dim)));
  ASSERT_EQ(OB_SUCCESS, replay_input.seal());
  EXPECT_EQ(1, replay_input.get_row_count());
  EXPECT_EQ(1, replay_input.get_inmemory_row_count());
  EXPECT_EQ(0, replay_input.get_file_size());
  uint64_t vid = 0;
  const float *vector = nullptr;
  EXPECT_EQ(OB_STATE_NOT_MATCH, replay_input.get_captured_vid(0, vid));
  replay_input.release_captured_vids();
  ASSERT_EQ(OB_SUCCESS, replay_input.get_row_at(0, vid, vector));
  EXPECT_EQ(expected_vid, vid);
  ASSERT_NE(nullptr, vector);
  EXPECT_EQ(0, std::memcmp(expected_vector, vector, sizeof(expected_vector)));
}

TEST(TestDiskBBQBuild, ReplayInputBatchCopiesAreTaskLocalInMemory)
{
  const int64_t dim = 3;
  const int64_t row_count = 512;
  const int64_t batch_count = 32;
  const int64_t worker_count = 8;
  const int64_t repeat_count = 32;
  const uint64_t first_vid = 50000;
  ObArenaAllocator allocator("AIVFBatchRead");
  ObIVFReplayInput replay_input(allocator);
  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, dim,
                              TEST_REPLAY_MEMORY_LIMIT));
  for (int64_t i = 0; i < row_count; ++i) {
    const float vector[dim] = {
        static_cast<float>(i),
        static_cast<float>(i) + 0.25F,
        -static_cast<float>(i),
    };
    ASSERT_EQ(OB_SUCCESS,
              replay_input.add_row(
                  first_vid + i, as_vector_bytes(vector, dim)));
  }
  ASSERT_EQ(OB_SUCCESS, replay_input.seal());
  ASSERT_EQ(0, replay_input.get_file_size());
  ASSERT_EQ(row_count, replay_input.get_inmemory_row_count());

  std::atomic<int64_t> failure_count(0);
  std::vector<std::thread> workers;
  for (int64_t worker_idx = 0; worker_idx < worker_count; ++worker_idx) {
    workers.emplace_back([&, worker_idx]() {
      for (int64_t round = 0;
           0 == failure_count.load() && round < repeat_count;
           ++round) {
        ObSEArray<ObIVFReplayReadRequest, batch_count> requests;
        ObSEArray<float, batch_count * dim> vectors;
        int ret = vectors.prepare_allocate(batch_count * dim);
        for (int64_t i = 0; OB_SUCCESS == ret && i < batch_count; ++i) {
          const int64_t row_id =
              (worker_idx * batch_count + round + i * 7) % row_count;
          ret = requests.push_back(ObIVFReplayReadRequest(
              row_id, first_vid + row_id, batch_count - i - 1));
        }
        if (OB_SUCCESS == ret) {
          ret = replay_input.batch_copy_rows(
              requests, 0, requests.count(), vectors);
        }
        for (int64_t i = 0; OB_SUCCESS == ret && i < batch_count; ++i) {
          const ObIVFReplayReadRequest &request = requests.at(i);
          const int64_t output_offset = request.output_idx_ * dim;
          if (vectors.at(output_offset) != static_cast<float>(request.row_id_)
              || vectors.at(output_offset + 1)
                  != static_cast<float>(request.row_id_) + 0.25F
              || vectors.at(output_offset + 2)
                  != -static_cast<float>(request.row_id_)) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
        if (OB_SUCCESS != ret) {
          ++failure_count;
        }
      }
    });
  }
  for (std::thread &worker : workers) {
    worker.join();
  }
  EXPECT_EQ(0, failure_count.load());
  EXPECT_EQ(row_count, replay_input.get_inmemory_row_count());
}

TEST(TestDiskBBQBuild, ReplayInputRejectsMalformedAndNonFiniteVectors)
{
  const int64_t dim = 2;
  const uint64_t sample_seed =
      ObIVFReplayInput::build_sample_seed(700, 800);
  ObArenaAllocator allocator("AIVFInvalidUT");
  ObIVFReplayInput replay_input(allocator);
  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, dim,
                              TEST_REPLAY_MEMORY_LIMIT, 2,
                              sample_seed, 0));

  const float short_vector[] = {1.0F};
  const float nan_vector[] = {
      std::numeric_limits<float>::quiet_NaN(), 2.0F};
  const float infinite_vector[] = {
      1.0F, std::numeric_limits<float>::infinity()};
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            replay_input.add_row(1, as_vector_bytes(short_vector, 1)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            replay_input.add_row(2, as_vector_bytes(nan_vector, dim)));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            replay_input.add_row(3, as_vector_bytes(infinite_vector, dim)));
  EXPECT_EQ(0, replay_input.get_row_count());
  EXPECT_EQ(0, replay_input.get_sample_scan_count());
  EXPECT_EQ(0, replay_input.get_sample_count());

  const float valid_vector[] = {3.0F, 4.0F};
  ASSERT_EQ(OB_SUCCESS,
            replay_input.add_row(4, as_vector_bytes(valid_vector, dim)));
  EXPECT_EQ(1, replay_input.get_row_count());
  EXPECT_EQ(1, replay_input.get_sample_scan_count());
  EXPECT_EQ(1, replay_input.get_sample_count());
  ASSERT_EQ(OB_SUCCESS, replay_input.seal());
}

TEST(TestDiskBBQBuild, ReplayInputCrossesInlineSampleHeapCapacity)
{
  const int64_t dim = 2;
  const int64_t max_sample_count = 65;
  const int64_t row_count = 70;
  const uint64_t first_vid = 10000;
  const uint64_t sample_seed =
      ObIVFReplayInput::build_sample_seed(900, 1000);
  const std::set<uint64_t> excluded_vids = {
      10067, 10059, 10045, 10061, 10014
  };
  ObArenaAllocator allocator("BBQHeap65UT");
  ObIVFReplayInput replay_input(allocator);

  ASSERT_EQ(0xd08d1aa282fd7dc9ULL, sample_seed);
  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, dim,
                              TEST_REPLAY_MEMORY_LIMIT, max_sample_count,
                              sample_seed, 3));
  for (int64_t i = 0; i < row_count; ++i) {
    const float vector[dim] = {
        static_cast<float>(i), static_cast<float>(-i)
    };
    ASSERT_EQ(OB_SUCCESS,
              replay_input.add_row(first_vid + i,
                                   as_vector_bytes(vector, dim)));
  }
  ASSERT_EQ(row_count, replay_input.get_sample_scan_count());
  ASSERT_EQ(max_sample_count, replay_input.get_sample_count());

  std::set<uint64_t> actual_vids;
  const common::ObIArray<ObIVFSampleCandidate> &candidates =
      replay_input.get_sample_candidates();
  for (int64_t i = 0; i < candidates.count(); ++i) {
    ASSERT_TRUE(actual_vids.insert(candidates.at(i).vid_).second);
  }
  for (uint64_t vid = first_vid; vid < first_vid + row_count; ++vid) {
    EXPECT_EQ(0 == excluded_vids.count(vid), 0 != actual_vids.count(vid));
  }
  ASSERT_EQ(OB_SUCCESS, replay_input.seal());
}

TEST(TestDiskBBQBuild, ReplayInputSamplingAllocationFailureFailsClosed)
{
  const int64_t dim = 2;
  const int64_t inline_sample_capacity = 64;
  const float vector[dim] = {1.0F, 2.0F};
  ToggleFailAllocator allocator;
  ObIVFReplayInput replay_input(allocator);
  // Sampling owns a separate allocator. Inject failure into the heap itself
  // so this test still exercises a failed expansion after payload capture.
  replay_input.sample_heap_.~ObIVFSampleHeap();
  new (&replay_input.sample_heap_) ObIVFSampleHeap(replay_input.sample_cmp_, &allocator);

  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, dim,
                              TEST_REPLAY_MEMORY_LIMIT,
                              inline_sample_capacity + 1,
                              0x123456789abcdef0ULL, 11, true));
  for (int64_t i = 0; i < inline_sample_capacity; ++i) {
    ASSERT_EQ(OB_SUCCESS,
              replay_input.add_row(20000 + i,
                                   as_vector_bytes(vector, dim)));
  }
  ASSERT_EQ(inline_sample_capacity, replay_input.get_sample_count());
  allocator.set_fail(true);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            replay_input.add_row(30000, as_vector_bytes(vector, dim)));
  EXPECT_EQ(inline_sample_capacity + 1, replay_input.get_row_count());
  EXPECT_EQ(inline_sample_capacity, replay_input.get_sample_scan_count());
  EXPECT_EQ(inline_sample_capacity, replay_input.get_sample_count());
  EXPECT_EQ(OB_ERR_UNEXPECTED, replay_input.seal());
  EXPECT_FALSE(replay_input.is_sealed());
}

TEST(TestDiskBBQBuild, GlobalBottomKAcrossMultipleSliceLocators)
{
  const int64_t dim = 2;
  const int64_t max_sample_count = 4;
  const uint64_t sample_seed =
      ObIVFReplayInput::build_sample_seed(500, 600);
  ObArenaAllocator first_allocator("BBQSlice9UT");
  ObArenaAllocator empty_allocator("BBQSlice4UT");
  ObArenaAllocator second_allocator("BBQSlice2UT");
  ObIVFReplayInput first_slice(first_allocator);
  ObIVFReplayInput empty_slice(empty_allocator);
  ObIVFReplayInput second_slice(second_allocator);

  ASSERT_EQ(OB_SUCCESS,
            first_slice.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT,
                             max_sample_count, sample_seed, 9, true));
  ASSERT_EQ(OB_SUCCESS,
            empty_slice.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT,
                             max_sample_count, sample_seed, 4, true));
  ASSERT_EQ(OB_SUCCESS,
            second_slice.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT,
                              max_sample_count, sample_seed, 2, true));
  for (uint64_t vid = 101; vid <= 105; ++vid) {
    const float vector[dim] = {
        static_cast<float>(vid), -static_cast<float>(vid)
    };
    ASSERT_EQ(OB_SUCCESS,
              first_slice.add_row(vid, as_vector_bytes(vector, dim)));
  }
  for (uint64_t vid = 201; vid <= 205; ++vid) {
    const float vector[dim] = {
        static_cast<float>(vid), -static_cast<float>(vid)
    };
    ASSERT_EQ(OB_SUCCESS,
              second_slice.add_row(vid, as_vector_bytes(vector, dim)));
  }
  ASSERT_EQ(OB_SUCCESS, first_slice.seal());
  ASSERT_EQ(OB_SUCCESS, empty_slice.seal());
  ASSERT_EQ(OB_SUCCESS, second_slice.seal());
  ASSERT_EQ(0, empty_slice.get_sample_count());

  ObIVFSampleRefMaxCmp sample_cmp;
  common::ObBinaryHeap<ObIVFSampleRef,
                       ObIVFSampleRefMaxCmp, 64> global_heap(
                           sample_cmp);
  ObIVFReplayInput *slices[] = {
      &first_slice, &empty_slice, &second_slice
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(slices); ++i) {
    const common::ObIArray<ObIVFSampleCandidate> &candidates =
        slices[i]->get_sample_candidates();
    for (int64_t j = 0; j < candidates.count(); ++j) {
      const ObIVFSampleRef sample_ref(candidates.at(j), slices[i]);
      if (global_heap.count() < max_sample_count) {
        ASSERT_EQ(OB_SUCCESS, global_heap.push(sample_ref));
      } else if (sample_cmp(sample_ref, global_heap.top())) {
        ASSERT_EQ(OB_SUCCESS, global_heap.replace_top(sample_ref));
      }
    }
  }

  // Global sample references must outlive each slice's VID metadata,
  // including an empty slice in the middle of the input list.
  for (int64_t i = 0; i < ARRAYSIZEOF(slices); ++i) {
    slices[i]->release_captured_vids();
  }
  const std::set<uint64_t> expected_vids = {102, 103, 104, 203};
  std::set<uint64_t> actual_vids;
  const common::ObIArray<ObIVFSampleRef> &sample_refs =
      global_heap.get_heap_data();
  ASSERT_EQ(max_sample_count, sample_refs.count());
  for (int64_t i = 0; i < sample_refs.count(); ++i) {
    uint64_t vid = 0;
    const float *vector = nullptr;
    const ObIVFSampleRef &sample_ref = sample_refs.at(i);
    ASSERT_TRUE(sample_ref.is_valid());
    ASSERT_EQ(OB_SUCCESS,
              sample_ref.replay_input_->get_row_at(
                  sample_ref.candidate_.row_id_, vid, vector));
    ASSERT_NE(nullptr, vector);
    EXPECT_EQ(sample_ref.candidate_.vid_, vid);
    EXPECT_FLOAT_EQ(static_cast<float>(vid), vector[0]);
    ASSERT_TRUE(actual_vids.insert(vid).second);
  }
  EXPECT_EQ(expected_vids, actual_vids);
}

TEST(TestDiskBBQBuild, MultiSliceReplayReaderStreamsWithoutCombinedCopy)
{
  const int64_t dim = 2;
  ObArenaAllocator first_allocator("BBQMultiFirst");
  ObArenaAllocator empty_allocator("BBQMultiEmpty");
  ObArenaAllocator second_allocator("BBQMultiSecond");
  ObIVFReplayInput first_slice(first_allocator);
  ObIVFReplayInput empty_slice(empty_allocator);
  ObIVFReplayInput second_slice(second_allocator);
  ASSERT_EQ(OB_SUCCESS,
            first_slice.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT));
  ASSERT_EQ(OB_SUCCESS,
            empty_slice.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT));
  ASSERT_EQ(OB_SUCCESS,
            second_slice.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT));

  const float first_vectors[][dim] = {{1.0F, -1.0F}, {2.0F, -2.0F}};
  const float second_vectors[][dim] = {{3.0F, -3.0F}, {4.0F, -4.0F}};
  for (int64_t i = 0; i < ARRAYSIZEOF(first_vectors); ++i) {
    ASSERT_EQ(OB_SUCCESS,
              first_slice.add_row(101 + i,
                                  as_vector_bytes(first_vectors[i], dim)));
  }
  for (int64_t i = 0; i < ARRAYSIZEOF(second_vectors); ++i) {
    ASSERT_EQ(OB_SUCCESS,
              second_slice.add_row(201 + i,
                                   as_vector_bytes(second_vectors[i], dim)));
  }
  ASSERT_EQ(OB_SUCCESS, first_slice.seal());
  ASSERT_EQ(OB_SUCCESS, empty_slice.seal());
  ASSERT_EQ(OB_SUCCESS, second_slice.seal());

  const int64_t source_mem_hold = first_slice.get_mem_hold()
      + empty_slice.get_mem_hold() + second_slice.get_mem_hold();
  const int64_t source_file_size = first_slice.get_file_size()
      + empty_slice.get_file_size() + second_slice.get_file_size();
  ObSEArray<ObIVFReplayInput *, 3> inputs;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(&second_slice));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(&empty_slice));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(&first_slice));
  ObIVFMultiSliceReplayReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(dim, inputs));
  EXPECT_EQ(4, reader.get_row_count());
  EXPECT_EQ(dim, reader.get_vector_dim());
  EXPECT_EQ(3, reader.get_input_count());
  EXPECT_EQ(source_mem_hold, reader.get_source_mem_hold());
  EXPECT_EQ(source_file_size, reader.get_source_file_size());

  const uint64_t expected_vids[] = {201, 202, 101, 102};
  for (int round = 0; round < 2; ++round) {
    ASSERT_EQ(OB_SUCCESS, reader.rewind());
    for (int64_t i = 0; i < ARRAYSIZEOF(expected_vids); ++i) {
      uint64_t vid = 0;
      const float *vector = nullptr;
      ASSERT_EQ(OB_SUCCESS, reader.get_next_row(vid, vector));
      EXPECT_EQ(expected_vids[i], vid);
      ASSERT_NE(nullptr, vector);
      EXPECT_FLOAT_EQ(static_cast<float>(vid < 200 ? vid - 100 : vid - 198),
                      vector[0]);
    }
    uint64_t vid = 0;
    const float *vector = nullptr;
    EXPECT_EQ(OB_ITER_END, reader.get_next_row(vid, vector));
  }
  reader.reset();
  reader.reset();
  EXPECT_FALSE(reader.is_sealed());
  EXPECT_EQ(0, reader.get_input_count());
  EXPECT_EQ(0, reader.get_row_count());
  EXPECT_EQ(0, reader.get_source_mem_hold());
  EXPECT_EQ(OB_NOT_INIT, reader.rewind());
  uint64_t vid = 0;
  const float *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS, first_slice.get_row_at(0, vid, vector));
  EXPECT_EQ(101, vid); // reset only detached the reader; the source is alive.
  ASSERT_EQ(OB_SUCCESS, reader.init(dim, inputs));
  ASSERT_EQ(OB_SUCCESS, reader.rewind());
  ASSERT_EQ(OB_SUCCESS, reader.get_next_row(vid, vector));
  EXPECT_EQ(201, vid);
}

TEST(TestDiskBBQBuild, ReplayInputKeepsBlocksOutOfCallerArenaAndReadsOddDimensionRows)
{
  // A three-float payload makes consecutive packed StoredRows alternate
  // between 8-byte and 4-byte alignment.  Reading the second VID therefore
  // covers the byte-copy path instead of an aligned uint64 dereference.
  const int64_t dim = 3;
  const int64_t row_count = 8192;
  const uint64_t first_vid = 0x0102030405060708ULL;
  ObArenaAllocator lifecycle_allocator("AIVFOddDimUT");
  {
    ObIVFReplayInput replay_input(lifecycle_allocator);
    ASSERT_EQ(OB_SUCCESS,
              replay_input.init(TEST_TENANT_ID, dim,
                                TEST_REPLAY_MEMORY_LIMIT));
    for (int64_t i = 0; i < row_count; ++i) {
      const float vector[dim] = {
          static_cast<float>(i),
          static_cast<float>(i) + 0.25F,
          -static_cast<float>(i),
      };
      ASSERT_EQ(OB_SUCCESS,
                replay_input.add_row(first_vid + i,
                                     as_vector_bytes(vector, dim)));
    }
    // Sampling is disabled, so the caller arena must remain unused even
    // after enough rows have been appended to cross multiple store blocks.
    EXPECT_EQ(0, lifecycle_allocator.used());
    ASSERT_EQ(OB_SUCCESS, replay_input.seal());

    const int64_t rows_to_check[] = {0, 1, row_count - 1};
    for (int64_t i = 0; i < ARRAYSIZEOF(rows_to_check); ++i) {
      const int64_t row_id = rows_to_check[i];
      uint64_t replayed_vid = 0;
      const float *replayed_vector = nullptr;
      ASSERT_EQ(OB_SUCCESS,
                replay_input.get_row_at(row_id, replayed_vid,
                                        replayed_vector));
      EXPECT_EQ(first_vid + row_id, replayed_vid);
      ASSERT_NE(nullptr, replayed_vector);
      EXPECT_FLOAT_EQ(static_cast<float>(row_id), replayed_vector[0]);
      EXPECT_FLOAT_EQ(static_cast<float>(row_id) + 0.25F,
                      replayed_vector[1]);
      EXPECT_FLOAT_EQ(-static_cast<float>(row_id), replayed_vector[2]);
    }
  }
  EXPECT_EQ(0, lifecycle_allocator.used());
}

TEST(TestDiskBBQBuild, MockRotatedCentersMaterializeTypeOneThreeAndFiveRows)
{
  const int64_t dim = 4;
  const uint64_t vids[] = {1001, 1002, 1003};
  const float source_vectors[][dim] = {
      {0.5F, 0.25F, -0.25F, 0.75F},
      {8.5F, 7.5F, 8.25F, 7.75F},
      {1.0F, -1.0F, 0.5F, -0.5F},
  };
  const int64_t expected_cids[] = {0, 1, 0};
  const int64_t vector_count = ARRAYSIZEOF(vids);
  const float source_center_zero[dim] = {
      0.0F, 0.0F, 0.0F, 0.0F};
  const float source_center_one[dim] = {
      8.0F, 8.0F, 8.0F, 8.0F};
  float rotated_vectors[vector_count][dim] = {};
  float center_zero[dim] = {};
  float center_one[dim] = {};
  MEMCPY(center_zero, source_center_zero, sizeof(source_center_zero));
  MEMCPY(center_one, source_center_one, sizeof(source_center_one));
  common::ObSEArray<float *, 2> centers;
  ASSERT_EQ(OB_SUCCESS, centers.push_back(center_zero));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(center_one));

  ObArenaAllocator quantization_allocator("AIVFQuantUT");
  quantization::ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS,
            quantization::ObFhtKacRotator::generate(
                quantization_allocator, dim, rotator));
  for (int64_t i = 0; i < vector_count; ++i) {
    MEMCPY(rotated_vectors[i], source_vectors[i], sizeof(rotated_vectors[i]));
    ASSERT_EQ(OB_SUCCESS,
              rotator.rotate(dim, rotated_vectors[i]));
  }
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, center_zero));
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, center_one));

  ObArenaAllocator replay_allocator("AIVFBuildUT");
  ObIVFReplayInput replay_input(replay_allocator);
  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, dim,
                              TEST_REPLAY_MEMORY_LIMIT));
  for (int64_t i = 0; i < vector_count; ++i) {
    ASSERT_EQ(OB_SUCCESS,
              replay_input.add_row(
                  vids[i], as_vector_bytes(rotated_vectors[i], dim)));
  }
  ASSERT_EQ(OB_SUCCESS, replay_input.seal());

  quantization::ObQuantizationParams quantization_params = {
      dim,
      share::DISK_BBQ_RABITQ_BITS,
      rotator,
      quantization::calc_inv_sqrt_dim(dim)};
  quantization::ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(quantization_allocator,
                         quantization::ObQuantizationType::RABITQ,
                         share::VIDA_L2, quantization_params));
  ASSERT_EQ(16, encoder.code_size());

  CollectingBuildRowSink row_sink;
  int64_t materialized_vector_count = 0;
  ScopedRefreshTestTenant tenant;
  ObIVFRefreshMonitor progress;
  progress.init(nullptr, 104, 1, 300, 400, 500);
  ASSERT_EQ(OB_SUCCESS,
            ObDiskBBQAlgorithm::build_rows_with_centers(
                TEST_TENANT_ID, dim, share::VIDA_L2, centers,
                replay_input, encoder, row_sink,
                materialized_vector_count, &progress));
  progress.finish(OB_SUCCESS);
  ObDDLDagMonitorEntry assignment_entry;
  ASSERT_EQ(OB_SUCCESS, progress.to_entry(assignment_entry, ObIVFRefreshProgress::ASSIGN_ENCODE));
  const ObString &assignment_message = assignment_entry.get_message();
  EXPECT_NE(std::string::npos,
      std::string(assignment_message.ptr(), assignment_message.length()).find("\"completed\":3"));
  ASSERT_EQ(vector_count, materialized_vector_count);
  ASSERT_EQ(centers.count() + 2 * vector_count,
            static_cast<int64_t>(row_sink.rows_.size()));

  const float *expected_centers[] = {center_zero, center_one};
  bool centroid_seen[] = {false, false};
  bool posting_seen[] = {false, false, false};
  bool full_precision_seen[] = {false, false, false};
  int64_t centroid_count = 0;
  int64_t posting_count = 0;
  int64_t full_precision_count = 0;
  for (int64_t i = 0; i < static_cast<int64_t>(row_sink.rows_.size()); ++i) {
    const OwnedBuildRow &row = row_sink.rows_[i];
    if (blocksstable::IVF_ROW_TYPE_CENTROID == row.row_type_) {
      ASSERT_EQ(0, row.cid_);
      ASSERT_GE(row.vid_, 1);
      ASSERT_LE(row.vid_, centers.count());
      EXPECT_FALSE(centroid_seen[row.vid_ - 1]);
      centroid_seen[row.vid_ - 1] = true;
      ++centroid_count;
      ASSERT_EQ(blocksstable::ObIvfCentroidPayload::get_size(dim),
                static_cast<int64_t>(row.payload_.size()));
      const float *decoded = nullptr;
      uint32_t children = 0;
      ASSERT_EQ(OB_SUCCESS, blocksstable::ObIvfCentroidPayload::decode(
          ObString(row.payload_.size(), row.payload_.data()), dim, decoded, children));
      EXPECT_EQ(row.vid_ == 1 ? 2 : 1, children);
      EXPECT_EQ(0, std::memcmp(expected_centers[row.vid_ - 1], decoded, dim * sizeof(float)));
    } else if (blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR
               == row.row_type_) {
      const int64_t vector_idx = find_vid(vids, vector_count, row.vid_);
      ASSERT_GE(vector_idx, 0);
      EXPECT_FALSE(posting_seen[vector_idx]);
      posting_seen[vector_idx] = true;
      ++posting_count;
      EXPECT_EQ(expected_cids[vector_idx] + 1, row.cid_);
      EXPECT_EQ(encoder.code_size(),
                static_cast<int64_t>(row.payload_.size()));
      std::vector<uint8_t> expected_code(encoder.code_size());
      ASSERT_EQ(OB_SUCCESS,
                encoder.encode(rotated_vectors[vector_idx],
                               expected_centers[row.cid_ - 1],
                               {expected_code.data(), encoder.code_size()}));
      EXPECT_EQ(0, std::memcmp(expected_code.data(), row.payload_.data(),
                               expected_code.size()));
    } else if (blocksstable::IVF_ROW_TYPE_RAW_VECTOR
               == row.row_type_) {
      const int64_t vector_idx = find_vid(vids, vector_count, row.vid_);
      ASSERT_GE(vector_idx, 0);
      EXPECT_FALSE(full_precision_seen[vector_idx]);
      full_precision_seen[vector_idx] = true;
      ++full_precision_count;
      EXPECT_EQ(0, row.cid_);
      ASSERT_EQ(dim * static_cast<int64_t>(sizeof(float)),
                static_cast<int64_t>(row.payload_.size()));
      EXPECT_EQ(0, std::memcmp(rotated_vectors[vector_idx], row.payload_.data(),
                               row.payload_.size()));
    } else {
      ADD_FAILURE() << "unexpected Disk BBQ row type: " << row.row_type_;
    }
  }
  EXPECT_EQ(centers.count(), centroid_count);
  EXPECT_EQ(vector_count, posting_count);
  EXPECT_EQ(vector_count, full_precision_count);
  for (int64_t i = 0; i < centers.count(); ++i) {
    EXPECT_TRUE(centroid_seen[i]);
  }
  for (int64_t i = 0; i < vector_count; ++i) {
    EXPECT_TRUE(posting_seen[i]);
    EXPECT_TRUE(full_precision_seen[i]);
  }
}

TEST(TestDiskBBQBuild, RefreshRemainsFlatAtHierarchyThreshold)
{
  const int64_t center_count = blocksstable::IVF_CENTROID_HIERARCHY_THRESHOLD;
  const int64_t dim = 1;
  const uint64_t vid = 17;
  float vector[dim] = {0.0F};
  std::vector<float> values(center_count);
  common::ObSEArray<float *, 64> centers;
  ASSERT_EQ(OB_SUCCESS, centers.reserve(center_count));
  // Reverse the pointer order: FLAT refresh does not require a contiguous
  // parent-training view, even when the center count reaches the threshold.
  for (int64_t j = 0; j < center_count; ++j) {
    values[j] = static_cast<float>(j);
    ASSERT_EQ(OB_SUCCESS, centers.push_back(&values[center_count - 1 - j]));
  }
  ObArenaAllocator allocator("BBQFlatRefresh");
  ObIVFReplayInput replay(allocator);
  ASSERT_EQ(OB_SUCCESS, replay.init(TEST_TENANT_ID, dim, TEST_REPLAY_MEMORY_LIMIT));
  ASSERT_EQ(OB_SUCCESS, replay.add_row(vid, as_vector_bytes(vector, dim)));
  ASSERT_EQ(OB_SUCCESS, replay.seal());
  quantization::ObQuantizationParams params = {};
  params.dim_ = dim;
  params.bits_ = share::DISK_BBQ_RABITQ_BITS;
  quantization::ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(allocator, quantization::ObQuantizationType::RABITQ,
      share::VIDA_L2, params));
  CollectingBuildRowSink sink;
  int64_t materialized = 0;
  ASSERT_EQ(OB_SUCCESS, ObDiskBBQAlgorithm::build_rows_with_centers(
      TEST_TENANT_ID, dim, share::VIDA_L2, centers, replay, encoder, sink, materialized));
  ASSERT_EQ(1, materialized);
  ASSERT_EQ(center_count + 2, static_cast<int64_t>(sink.rows_.size()));
  ASSERT_EQ(blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR, sink.rows_[0].row_type_);
  EXPECT_EQ(center_count, sink.rows_[0].cid_);
  EXPECT_EQ(vid, sink.rows_[0].vid_);
  ASSERT_EQ(blocksstable::IVF_ROW_TYPE_RAW_VECTOR, sink.rows_[1].row_type_);
  EXPECT_EQ(blocksstable::IVF_RAW_VECTOR_CID, sink.rows_[1].cid_);
  EXPECT_EQ(vid, sink.rows_[1].vid_);
  for (int64_t j = 0; j < center_count; ++j) {
    const OwnedBuildRow &row = sink.rows_[j + 2];
    ASSERT_EQ(blocksstable::IVF_ROW_TYPE_CENTROID, row.row_type_);
    ASSERT_EQ(0, row.cid_);
    ASSERT_EQ(j + 1, row.vid_);
    const float *decoded = nullptr;
    uint32_t children = UINT32_MAX;
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObIvfCentroidPayload::decode(
        ObString(row.payload_.size(), row.payload_.data()), dim, decoded, children));
    EXPECT_EQ(j + 1 == center_count ? 1 : 0, children);
    EXPECT_EQ(0, MEMCMP(centers.at(j), decoded, sizeof(float)));
  }
}

TEST(TestDiskBBQBuild, InnerProductAssignsWithNormalizedQuickCenters)
{
  const int64_t dim = 4;
  const uint64_t vid = 40001;
  const float source_vector[dim] = {100.0F, 0.0F, 0.0F, 0.0F};
  const float source_large_center[dim] = {
      100.0F, 0.0F, 0.0F, 0.0F};
  const float source_orthogonal_center[dim] = {
      0.0F, 1.0F, 0.0F, 0.0F};
  float vector[dim] = {};
  float large_center[dim] = {};
  float orthogonal_center[dim] = {};
  MEMCPY(vector, source_vector, sizeof(source_vector));
  MEMCPY(large_center, source_large_center, sizeof(source_large_center));
  MEMCPY(orthogonal_center, source_orthogonal_center,
         sizeof(source_orthogonal_center));
  share::ObVectorNormalizeInfo normalize_info;
  ASSERT_EQ(OB_SUCCESS,
            normalize_info.normalize_func_(dim, large_center,
                                            large_center, nullptr));
  ASSERT_EQ(OB_SUCCESS,
            normalize_info.normalize_func_(dim, orthogonal_center,
                                            orthogonal_center, nullptr));
  common::ObSEArray<float *, 2> centers;
  ASSERT_EQ(OB_SUCCESS, centers.push_back(large_center));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(orthogonal_center));

  ObArenaAllocator quantization_allocator("BBQIPQuantUT");
  quantization::ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS,
            quantization::ObFhtKacRotator::generate(
                quantization_allocator, dim, rotator));
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, vector));
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, large_center));
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, orthogonal_center));

  ObArenaAllocator replay_allocator("BBQIPReplayUT");
  ObIVFReplayInput replay_input(replay_allocator);
  ASSERT_EQ(OB_SUCCESS,
            replay_input.init(TEST_TENANT_ID, dim,
                              TEST_REPLAY_MEMORY_LIMIT));
  ASSERT_EQ(OB_SUCCESS,
            replay_input.add_row(vid, as_vector_bytes(vector, dim)));
  ASSERT_EQ(OB_SUCCESS, replay_input.seal());

  quantization::ObQuantizationParams params = {
      dim,
      share::DISK_BBQ_RABITQ_BITS,
      rotator,
      quantization::calc_inv_sqrt_dim(dim)};
  quantization::ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(quantization_allocator,
                         quantization::ObQuantizationType::RABITQ,
                         share::VIDA_IP, params));

  CollectingBuildRowSink row_sink;
  int64_t materialized_vector_count = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObDiskBBQAlgorithm::build_rows_with_centers(
                TEST_TENANT_ID, dim, share::VIDA_IP, centers,
                replay_input, encoder, row_sink,
                materialized_vector_count));
  ASSERT_EQ(1, materialized_vector_count);

  int64_t posting_count = 0;
  for (int64_t i = 0; i < static_cast<int64_t>(row_sink.rows_.size()); ++i) {
    const OwnedBuildRow &row = row_sink.rows_[i];
    if (blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR
        == row.row_type_) {
      ++posting_count;
      EXPECT_EQ(vid, row.vid_);
      EXPECT_EQ(1, row.cid_);
    }
  }
  EXPECT_EQ(1, posting_count);
}

TEST(TestDiskBBQBuild, DiskBBQMetaValidatesAndCopiesFormatV1)
{
  ObArenaAllocator allocator("BBQMetaUT");
  blocksstable::ObIvfSSTableMeta source;
  ASSERT_EQ(OB_SUCCESS,
            prepare_disk_bbq_meta(10, false, source));
  ASSERT_TRUE(source.is_valid());
  EXPECT_EQ(10, source.dim_);

  blocksstable::ObIvfSSTableMeta invalid = source;
  ++invalid.format_version_;
  EXPECT_FALSE(invalid.is_valid());
  invalid = source;
  invalid.nbits_ = 0;
  EXPECT_FALSE(invalid.is_valid());
  invalid = source;
  invalid.fht_algo_version_ = 0;
  EXPECT_FALSE(invalid.is_valid());
  invalid = source;
  invalid.centroid_count_ = -1;
  EXPECT_FALSE(invalid.is_valid());

  blocksstable::ObIvfSSTableMeta empty;
  ASSERT_EQ(OB_SUCCESS,
            prepare_disk_bbq_meta(10, true, empty));
  EXPECT_TRUE(empty.is_valid());
  EXPECT_TRUE(empty.is_empty());
  EXPECT_EQ(0, empty.dim_);

  blocksstable::ObIvfSSTableMeta copied;
  ASSERT_EQ(OB_SUCCESS, copied.assign(allocator, source));
  ASSERT_TRUE(copied.is_valid());
  EXPECT_EQ(source.dim_, copied.dim_);
  EXPECT_EQ(source.format_version_, copied.format_version_);
  EXPECT_EQ(source.nbits_, copied.nbits_);
  EXPECT_EQ(source.fht_algo_version_, copied.fht_algo_version_);
  EXPECT_EQ(source.centroid_count_, copied.centroid_count_);
}

TEST(TestDiskBBQBuild, IvfSSTableRowDescResolvesFormatV1PhysicalLayout)
{
  blocksstable::ObIvfSSTableRowDesc row_desc;
  EXPECT_FALSE(row_desc.is_valid());
  EXPECT_FALSE(row_desc.is_valid_for_row(6));

  ASSERT_EQ(OB_SUCCESS,
            row_desc.init(blocksstable::IVF_SSTABLE_FORMAT_VERSION));
  ASSERT_TRUE(row_desc.is_valid());
  EXPECT_EQ(0, row_desc.row_type_col_idx_);
  EXPECT_EQ(1, row_desc.centroid_id_col_idx_);
  EXPECT_EQ(2, row_desc.vector_id_col_idx_);
  EXPECT_EQ(3, row_desc.trans_version_col_idx_);
  EXPECT_EQ(4, row_desc.sql_sequence_col_idx_);
  EXPECT_EQ(5, row_desc.payload_col_idx_);
  EXPECT_FALSE(row_desc.is_valid_for_row(5));
  EXPECT_TRUE(row_desc.is_valid_for_row(6));

  EXPECT_EQ(OB_NOT_SUPPORTED,
            row_desc.init(blocksstable::IVF_SSTABLE_FORMAT_VERSION + 1));
  EXPECT_FALSE(row_desc.is_valid());
}

TEST(TestDiskBBQBuild, DiskBBQMetaRoundTripsAndRejectsTruncatedPayload)
{
  blocksstable::ObIvfSSTableMeta source;
  ASSERT_EQ(OB_SUCCESS,
            prepare_disk_bbq_meta(10, false, source));
  source.centroid_count_ = 17;
  source.centroid_layout_ = blocksstable::IVF_CENTROID_HIERARCHICAL;
  source.parent_centroid_count_ = 2;
  const int64_t serialized_size = source.get_serialize_size();
  std::vector<char> buffer(serialized_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            source.serialize(buffer.data(), buffer.size(), pos));
  ASSERT_EQ(serialized_size, pos);

  blocksstable::ObIvfSSTableMeta decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            decoded.deserialize(buffer.data(), buffer.size(), pos));
  ASSERT_EQ(serialized_size, pos);
  ASSERT_TRUE(decoded.is_valid());
  EXPECT_EQ(source.format_version_, decoded.format_version_);
  EXPECT_EQ(source.nbits_, decoded.nbits_);
  EXPECT_EQ(source.fht_algo_version_, decoded.fht_algo_version_);
  EXPECT_EQ(source.centroid_count_, decoded.centroid_count_);
  EXPECT_EQ(source.centroid_layout_, decoded.centroid_layout_);
  EXPECT_EQ(source.parent_centroid_count_, decoded.parent_centroid_count_);

  blocksstable::ObIvfSSTableMeta truncated;
  pos = 0;
  EXPECT_NE(OB_SUCCESS,
            truncated.deserialize(buffer.data(), buffer.size() - 1, pos));
  EXPECT_FALSE(truncated.is_valid());
}

namespace
{
std::string refresh_message(ObIVFRefreshMonitor &perf, int64_t stage = -1)
{
  ObDDLDagMonitorEntry entry;
  EXPECT_EQ(OB_SUCCESS, perf.to_entry(entry, stage));
  return std::string(entry.get_message().ptr(), entry.get_message().length());
}
void expect_refresh_field(const std::string &message, const char *field)
{
  EXPECT_NE(std::string::npos, message.find(field)) << message;
}
}

TEST(TestIVFRefreshMonitor, progress_and_commit_failure_freeze_history)
{
  ScopedRefreshTestTenant tenant;
  ObIVFRefreshMonitor perf;
  perf.init(nullptr, 100, 2, 300, 400, 500);
  expect_refresh_field(refresh_message(perf, ObIVFRefreshProgress::CAPTURE), "\"total\":-1");
  perf.add_completed(ObIVFRefreshProgress::CAPTURE, 20);
  perf.set_total(ObIVFRefreshProgress::CAPTURE, 20);
  perf.begin_stage(ObIVFRefreshProgress::TRAIN_ITERATE);
  perf.iteration_finished(0, 17, 0.5, 1.2);
  perf.iteration_finished(1, 23, 0.01, 1.1);
  expect_refresh_field(refresh_message(perf), "\"iterations\":2");
  expect_refresh_field(refresh_message(perf), "\"iteration_sum_us\":40");
  expect_refresh_field(refresh_message(perf), "\"input_vectors\":20");
  expect_refresh_field(refresh_message(perf, ObIVFRefreshProgress::CAPTURE), "\"state\":\"FINISHED\"");
  perf.begin_stage(ObIVFRefreshProgress::COMMIT);
  expect_refresh_field(refresh_message(perf), "\"state\":\"RUNNING\"");
  perf.finish(OB_TIMEOUT, 0, 3);
  const std::string final = refresh_message(perf);
  expect_refresh_field(final, "\"state\":\"FAILED\"");
  expect_refresh_field(final, "\"retry_count\":3");
  expect_refresh_field(refresh_message(perf, ObIVFRefreshProgress::COMMIT), "\"state\":\"FAILED\"");
  perf.add_completed(ObIVFRefreshProgress::CAPTURE, 999);
  perf.iteration_finished(2, 99, 0, 0);
  perf.begin_stage(ObIVFRefreshProgress::WRITE);
  perf.finish(OB_SUCCESS);
  EXPECT_EQ(final, refresh_message(perf));
}

TEST(TestIVFRefreshMonitor, empty_refresh_skips_training)
{
  ScopedRefreshTestTenant tenant;
  ObIVFRefreshMonitor perf;
  perf.init(nullptr, 101, 1, 300, 400, 500);
  perf.set_total(ObIVFRefreshProgress::CAPTURE, 0);
  perf.begin_stage(ObIVFRefreshProgress::WRITE);
  perf.begin_stage(ObIVFRefreshProgress::PUBLISH);
  perf.begin_stage(ObIVFRefreshProgress::COMMIT);
  perf.finish(OB_SUCCESS);
  expect_refresh_field(refresh_message(perf), "\"state\":\"FINISHED\"");
  expect_refresh_field(refresh_message(perf), "\"input_vectors\":0");
  expect_refresh_field(refresh_message(perf, ObIVFRefreshProgress::TRAIN_ITERATE), "\"state\":\"SKIPPED\"");
}

TEST(TestIVFRefreshMonitor, concurrent_capture_and_queries)
{
  ScopedRefreshTestTenant tenant;
  ObIVFRefreshMonitor perf;
  perf.init(nullptr, 102, 1, 300, 400, 500);
  std::atomic<int> finished(0);
  std::vector<std::thread> writers;
  for (int i = 0; i < 4; ++i) {
    writers.emplace_back([&]() {
      for (int n = 0; n < 1000; ++n) { perf.add_completed(ObIVFRefreshProgress::CAPTURE, 16); }
      ++finished;
    });
  }
  while (finished.load() < 4) {
    expect_refresh_field(refresh_message(perf), "\"state\":\"RUNNING\"");
  }
  for (auto &writer : writers) { writer.join(); }
  perf.finish(OB_CANCELED);
  expect_refresh_field(refresh_message(perf), "\"input_vectors\":64000");
  expect_refresh_field(refresh_message(perf, ObIVFRefreshProgress::CAPTURE), "\"state\":\"CANCELED\"");
}

TEST(TestIVFRefreshMonitor, dag_cache_retains_snapshot_after_runtime_release)
{
  ScopedRefreshTestTenant tenant;
  ObArenaAllocator allocator("IVFPerfTest");
  ObDDLDagMonitorNode node(&allocator);
  ObCurTraceId::TraceId trace;
  ASSERT_EQ(OB_SUCCESS, trace.set("Y1-0000000000000001-0-0"));
  ASSERT_EQ(OB_SUCCESS, node.init(ObDDLDagMonitorNode::Key(&node, trace)));
  node.inc_ref(); // stack-owned test node; monitor acquires/releases another reference
  ObIVFRefreshMonitor *perf = OB_NEW(ObIVFRefreshMonitor, ObMemAttr(TEST_TENANT_ID, "IVFRefreshPerf"));
  ASSERT_NE(nullptr, perf);
  perf->init(&node, 103, 1, 300, 400, 500);
  perf->add_completed(ObIVFRefreshProgress::CAPTURE, 42);
  ASSERT_EQ(OB_SUCCESS, node.clean_infos(true)); // active records must survive cleanup
  perf->begin_stage(ObIVFRefreshProgress::COMMIT);
  perf->finish(OB_SUCCESS);
  perf->dec_ref(); // only cached entries now retain the immutable statistics
  ObSEArray<ObDDLDagMonitorEntry *, 16> entries;
  ASSERT_EQ(OB_SUCCESS, node.get_all_infos(allocator, entries));
  ASSERT_EQ(ObIVFRefreshProgress::STAGE_COUNT + 1, entries.count());
  const ObString &message = entries.at(0)->get_message();
  expect_refresh_field(std::string(message.ptr(), message.length()), "\"input_vectors\":42");
  for (auto *entry : entries) { entry->~ObDDLDagMonitorEntry(); }
  ASSERT_EQ(OB_SUCCESS, node.clean_infos(true)); // releases the final monitor reference
  entries.reset();
  ASSERT_EQ(OB_SUCCESS, node.get_all_infos(allocator, entries));
  EXPECT_EQ(0, entries.count());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::init_arches();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
