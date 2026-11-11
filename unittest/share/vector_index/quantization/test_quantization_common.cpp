/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <stdint.h>
#include <stdlib.h>
#include <type_traits>

#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/vector/ob_vsag_adaptor.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "share/vector_index/quantization/ob_quantization_struct.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "share/vector_index/quantization/ob_quantizer_registry.h"

namespace oceanbase
{
namespace quantization
{

using namespace common;

namespace
{

class TrackingAllocator : public ObIAllocator
{
public:
  TrackingAllocator()
      : allocation_(nullptr), allocation_count_(0), free_count_(0), fail_next_alloc_(false)
  {}

  ~TrackingAllocator() override
  {
    if (nullptr != allocation_) {
      ::free(allocation_);
    }
  }

  void *alloc(const int64_t size) override
  {
    ++allocation_count_;
    if (fail_next_alloc_) {
      fail_next_alloc_ = false;
      return nullptr;
    }
    allocation_ = ::malloc(size);
    return allocation_;
  }

  void *alloc(const int64_t size, const lib::ObMemAttr &) override
  {
    return alloc(size);
  }

  void free(void *ptr) override
  {
    if (ptr == allocation_) {
      ::free(allocation_);
      allocation_ = nullptr;
      ++free_count_;
    }
  }

  int64_t allocation_count() const { return allocation_count_; }
  int64_t free_count() const { return free_count_; }
  bool has_active_allocation() const { return nullptr != allocation_; }
  void fail_next_alloc() { fail_next_alloc_ = true; }

private:
  void *allocation_;
  int64_t allocation_count_;
  int64_t free_count_;
  bool fail_next_alloc_;
};

} // namespace

TEST(TestQuantizationCommon, QuantizationTypeIsSharedWithVectorIndexAdaptor)
{
  EXPECT_TRUE((std::is_same<ObQuantizationType,
                            common::obvsag::QuantizationType>::value));
}

TEST(TestQuantizationCommon, EnumSentinelsAreNotSelectable)
{
  EXPECT_EQ(0, static_cast<int64_t>(ObQuantizationType::FP32));
  EXPECT_EQ(1, static_cast<int64_t>(ObQuantizationType::SQ8));
  EXPECT_EQ(2, static_cast<int64_t>(ObQuantizationType::RABITQ));
  EXPECT_EQ(3, static_cast<int64_t>(ObQuantizationType::EXTEND_RABITQ));
  EXPECT_EQ(4, static_cast<int64_t>(ObQuantizationType::MAX_TYPE));
  EXPECT_LT(static_cast<int64_t>(share::VIDA_L2),
            static_cast<int64_t>(share::VIDA_MAX));
  EXPECT_LT(static_cast<int64_t>(share::VIDA_IP),
            static_cast<int64_t>(share::VIDA_MAX));
  EXPECT_LT(static_cast<int64_t>(share::VIDA_COS),
            static_cast<int64_t>(share::VIDA_MAX));
}

TEST(TestQuantizationCommon, RegistryRejectsInvalidTypeAndMetric)
{
  const ObQuantizerMetricOps *metric_ops = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObQuantizerRegistry::get_ops(static_cast<ObQuantizationType>(-1),
                                         share::VIDA_L2,
                                         metric_ops));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObQuantizerRegistry::get_ops(ObQuantizationType::MAX_TYPE,
                                         share::VIDA_L2,
                                         metric_ops));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObQuantizerRegistry::get_ops(ObQuantizationType::FP32,
                                         static_cast<share::ObVectorIndexDistAlgorithm>(-1),
                                         metric_ops));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObQuantizerRegistry::get_ops(ObQuantizationType::FP32,
                                         share::VIDA_MAX,
                                         metric_ops));
}

TEST(TestQuantizationCommon, RegistryReportsSq8AsUnsupported)
{
  const ObQuantizerMetricOps *sentinel = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObQuantizerRegistry::get_ops(ObQuantizationType::FP32,
                                         share::VIDA_L2,
                                         sentinel));
  ASSERT_NE(nullptr, sentinel);

  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int64_t metric = 0;
       metric < static_cast<int64_t>(sizeof(metrics) / sizeof(metrics[0]));
       ++metric) {
    const ObQuantizerMetricOps *metric_ops = sentinel;
    EXPECT_EQ(OB_NOT_SUPPORTED,
              ObQuantizerRegistry::get_ops(ObQuantizationType::SQ8,
                                           metrics[metric],
                                           metric_ops));
    EXPECT_EQ(nullptr, metric_ops);
  }
}

TEST(TestQuantizationCommon, RegistryRejectsInvalidMetricForSq8)
{
  const ObQuantizerMetricOps *sentinel = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObQuantizerRegistry::get_ops(ObQuantizationType::FP32,
                                         share::VIDA_L2,
                                         sentinel));
  ASSERT_NE(nullptr, sentinel);

  const share::ObVectorIndexDistAlgorithm invalid_metrics[] = {
      static_cast<share::ObVectorIndexDistAlgorithm>(-1), share::VIDA_MAX};
  for (int64_t metric = 0;
       metric < static_cast<int64_t>(sizeof(invalid_metrics)
                                     / sizeof(invalid_metrics[0]));
       ++metric) {
    const ObQuantizerMetricOps *metric_ops = sentinel;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObQuantizerRegistry::get_ops(ObQuantizationType::SQ8,
                                           invalid_metrics[metric],
                                           metric_ops));
    EXPECT_EQ(nullptr, metric_ops);
  }
}

TEST(TestQuantizationCommon, RegistryRoutesAllSupportedTypeMetricCombinations)
{
  const ObQuantizerMetricOps *metric_ops = nullptr;
  const ObQuantizationType types[] = {
      ObQuantizationType::FP32,
      ObQuantizationType::RABITQ,
      ObQuantizationType::EXTEND_RABITQ};
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int64_t type = 0; type < static_cast<int64_t>(sizeof(types) / sizeof(types[0])); ++type) {
    for (int64_t metric = 0;
         metric < static_cast<int64_t>(sizeof(metrics) / sizeof(metrics[0]));
         ++metric) {
      metric_ops = nullptr;
      EXPECT_EQ(OB_SUCCESS,
                ObQuantizerRegistry::get_ops(types[type], metrics[metric], metric_ops));
      EXPECT_NE(nullptr, metric_ops);
      if (ObQuantizationType::EXTEND_RABITQ == types[type]) {
        EXPECT_NE(nullptr, metric_ops->compute_xbits);
      } else {
        EXPECT_EQ(nullptr, metric_ops->compute_xbits);
      }
    }
  }
}

TEST(TestQuantizationCommon, EncoderFailedInitLeavesResetState)
{
  ObArenaAllocator allocator("QuantTest");
  ObQuantizationEncoder encoder;
  const ObQuantizationParams params = {3, 1, {}, 0.0F};
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            encoder.init(allocator,
                         ObQuantizationType::MAX_TYPE,
                         share::VIDA_L2,
                         params));
  EXPECT_EQ(0, encoder.code_size());
  EXPECT_EQ(OB_NOT_INIT, encoder.encode(nullptr, nullptr, {nullptr, 0}));
}

TEST(TestQuantizationCommon, ComputerRejectsComputeBeforeInit)
{
  ObQuantizationComputer computer;
  float metric_value = 0.0F;
  EXPECT_EQ(OB_NOT_INIT, computer.compute({nullptr, 0}, metric_value));
  EXPECT_EQ(OB_NOT_INIT,
            computer.compute_xbits({nullptr, 0}, 0.0F, metric_value));
}

TEST(TestQuantizationCommon, Fp32BorrowedQueryHasNoOwnedBufferAndCanSwitchOwnership)
{
  TrackingAllocator allocator;
  ObQuantizationComputer computer;
  ObQuantizationParams params = {};
  params.dim_ = 3;
  params.bits_ = 1;
  float query[3] = {1.0F, 2.0F, 3.0F};
  float base[3] = {};
  float distance = -1;
  const ObQuantizationBuffer code = {reinterpret_cast<uint8_t *>(base), sizeof(base)};
  ASSERT_EQ(OB_SUCCESS, computer.init(allocator, ObQuantizationType::RABITQ,
      share::VIDA_L2, params, query, base));
  ASSERT_EQ(1, allocator.allocation_count());
  computer.reset();
  ASSERT_EQ(1, allocator.free_count());
  ASSERT_EQ(OB_SUCCESS, computer.init(allocator, ObQuantizationType::FP32,
      share::VIDA_L2, params, query, nullptr));
  EXPECT_EQ(1, allocator.allocation_count());
  EXPECT_FALSE(allocator.has_active_allocation());
  ASSERT_EQ(OB_SUCCESS, computer.compute(code, distance));
  EXPECT_FLOAT_EQ(14.0F, distance);
  // Deliberately change the backing data to distinguish a view from a copy.
  // Normal queries keep this vector immutable while the context is in use.
  query[0] = 2.0F;
  ASSERT_EQ(OB_SUCCESS, computer.compute(code, distance));
  EXPECT_FLOAT_EQ(17.0F, distance);
  computer.reset();
  EXPECT_EQ(1, allocator.allocation_count());
  EXPECT_EQ(1, allocator.free_count());
  EXPECT_FLOAT_EQ(2.0F, query[0]);
  EXPECT_EQ(OB_NOT_INIT, computer.compute(code, distance));
  ASSERT_EQ(OB_SUCCESS, computer.init(allocator, ObQuantizationType::RABITQ,
      share::VIDA_L2, params, query, base));
  computer.reset();
  EXPECT_EQ(2, allocator.allocation_count());
  EXPECT_EQ(2, allocator.free_count());
}

TEST(TestQuantizationCommon, Fp32BorrowedQueryRejectsInvalidInitAndPreservesLiveContext)
{
  TrackingAllocator allocator;
  ObQuantizationComputer computer;
  ObQuantizationParams params = {};
  float query[3] = {1.0F, 2.0F, 3.0F};
  float base[3] = {};
  const ObQuantizationBuffer code = {reinterpret_cast<uint8_t *>(base), sizeof(base)};
  float distance = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.init(allocator, ObQuantizationType::FP32,
      share::VIDA_L2, params, query, nullptr));
  params.dim_ = INT64_MAX;
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.init(allocator, ObQuantizationType::FP32,
      share::VIDA_L2, params, query, nullptr));
  params.dim_ = 3;
  EXPECT_EQ(OB_INVALID_ARGUMENT, computer.init(allocator, ObQuantizationType::FP32,
      share::VIDA_L2, params, nullptr, nullptr));
  EXPECT_EQ(OB_NOT_INIT, computer.compute(code, distance));
  ASSERT_EQ(OB_SUCCESS, computer.init(allocator, ObQuantizationType::FP32,
      share::VIDA_L2, params, query, nullptr));
  EXPECT_EQ(OB_INIT_TWICE, computer.init(allocator, ObQuantizationType::FP32,
      share::VIDA_IP, params, query, nullptr));
  EXPECT_EQ(OB_INIT_TWICE, computer.init(allocator, ObQuantizationType::RABITQ,
      share::VIDA_L2, params, query, base));
  ASSERT_EQ(OB_SUCCESS, computer.compute(code, distance));
  EXPECT_FLOAT_EQ(14.0F, distance);
  computer.reset();
  EXPECT_EQ(0, allocator.allocation_count());
  EXPECT_EQ(0, allocator.free_count());
}

TEST(TestQuantizationCommon, RabitqComputerRejectsXBitsCompute)
{
  ObArenaAllocator allocator("QuantXBits");
  ObQuantizationComputer computer;
  const ObQuantizationParams params = {3, 1, {}, 0.0F};
  const float query[3] = {1.0F, 2.0F, 3.0F};
  const float centroid[3] = {};
  ASSERT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_L2,
                          params,
                          query,
                          centroid));
  float metric_value = 0.0F;
  EXPECT_EQ(OB_NOT_SUPPORTED,
            computer.compute_xbits({nullptr, 0}, 0.0F, metric_value));
}

TEST(TestQuantizationCommon, ComputerFailedInitLeavesResetState)
{
  ObArenaAllocator allocator("QuantTest");
  ObQuantizationComputer computer;
  const ObQuantizationParams params = {3, 1, {}, 0.0F};
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            computer.init(allocator,
                          ObQuantizationType::MAX_TYPE,
                          share::VIDA_L2,
                          params,
                          nullptr,
                          nullptr));
  float metric_value = 0.0F;
  EXPECT_EQ(OB_NOT_INIT, computer.compute({nullptr, 0}, metric_value));
  EXPECT_EQ(OB_NOT_INIT,
            computer.compute_xbits({nullptr, 0}, 0.0F, metric_value));
}

TEST(TestQuantizationCommon, ComputerFailedPrepareReleasesQueryCode)
{
  TrackingAllocator allocator;
  ObQuantizationComputer computer;
  const ObQuantizationParams params = {3, 1, {}, 0.0F};
  const float centroid[3] = {};
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_L2,
                          params,
                          nullptr,
                          centroid));
  EXPECT_EQ(1, allocator.allocation_count());
  EXPECT_EQ(1, allocator.free_count());
  EXPECT_FALSE(allocator.has_active_allocation());

  float metric_value = 0.0F;
  EXPECT_EQ(OB_NOT_INIT, computer.compute({nullptr, 0}, metric_value));
}

TEST(TestQuantizationCommon, EncoderAllocationFailureLeavesResetAndCanRetry)
{
  TrackingAllocator allocator;
  ObQuantizationEncoder encoder;
  const ObQuantizationParams params = {3, 1, {}, 0.0F};

  allocator.fail_next_alloc();
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  EXPECT_EQ(1, allocator.allocation_count());
  EXPECT_EQ(0, allocator.free_count());
  EXPECT_FALSE(allocator.has_active_allocation());
  EXPECT_EQ(0, encoder.code_size());
  EXPECT_EQ(OB_NOT_INIT, encoder.encode(nullptr, nullptr, {nullptr, 0}));

  EXPECT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  EXPECT_EQ(2, allocator.allocation_count());
  EXPECT_TRUE(allocator.has_active_allocation());
  EXPECT_GT(encoder.code_size(), 0);
}

TEST(TestQuantizationCommon, ComputerAllocationFailureLeavesResetAndCanRetry)
{
  TrackingAllocator allocator;
  ObQuantizationComputer computer;
  const ObQuantizationParams params = {3, 1, {}, 0.0F};
  const float query[3] = {1.0F, 2.0F, 3.0F};
  const float centroid[3] = {};

  allocator.fail_next_alloc();
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_L2,
                          params,
                          query,
                          centroid));
  EXPECT_EQ(1, allocator.allocation_count());
  EXPECT_EQ(0, allocator.free_count());
  EXPECT_FALSE(allocator.has_active_allocation());
  float metric_value = 0.0F;
  EXPECT_EQ(OB_NOT_INIT, computer.compute({nullptr, 0}, metric_value));

  EXPECT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_L2,
                          params,
                          query,
                          centroid));
  EXPECT_EQ(2, allocator.allocation_count());
  EXPECT_TRUE(allocator.has_active_allocation());
}

TEST(TestQuantizationCommon, ComputerResetAndDestructionReleaseQueryCode)
{
  TrackingAllocator allocator;
  const ObQuantizationParams params = {3, 1, {}, 0.0F};
  const float query[3] = {1.0F, 2.0F, 3.0F};
  const float centroid[3] = {};
  {
    ObQuantizationComputer computer;
    ASSERT_EQ(OB_SUCCESS,
              computer.init(allocator,
                            ObQuantizationType::RABITQ,
                            share::VIDA_L2,
                            params,
                            query,
                            centroid));
    ASSERT_TRUE(allocator.has_active_allocation());
    computer.reset();
    EXPECT_EQ(1, allocator.free_count());
    EXPECT_FALSE(allocator.has_active_allocation());

    ASSERT_EQ(OB_SUCCESS,
              computer.init(allocator,
                            ObQuantizationType::RABITQ,
                            share::VIDA_L2,
                            params,
                            query,
                            centroid));
    ASSERT_TRUE(allocator.has_active_allocation());
  }
  EXPECT_EQ(2, allocator.free_count());
  EXPECT_FALSE(allocator.has_active_allocation());
}

TEST(TestQuantizationCommon, Fp32FailedInitLeavesEncoderReset)
{
  const ObQuantizationParams params = {3, 0, {}, 0.0F};
  ObArenaAllocator allocator("Fp32BatchAlign");
  ObQuantizationEncoder encoder;
  EXPECT_EQ(OB_NOT_SUPPORTED,
            encoder.init(allocator,
                         ObQuantizationType::FP32,
                         share::VIDA_L2,
                         params));
  EXPECT_EQ(0, encoder.code_size());
  EXPECT_EQ(OB_NOT_INIT, encoder.encode(nullptr, nullptr, {nullptr, 0}));
}

} // namespace quantization
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
