/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <string.h>

#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "share/vector_index/quantization/ob_quantizer_registry.h"
#include "share/vector_index/quantization/fp32_quantization/ob_fp32_quantizer.h"
#include "share/vector_type/ob_vector_cosine_distance.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_type/ob_vector_ip_distance.h"
#include "share/vector_type/ob_vector_l2_distance.h"

namespace oceanbase
{
namespace quantization
{

using namespace common;

namespace
{

const int64_t DIM = 7;
const int64_t CODE_SIZE = DIM * static_cast<int64_t>(sizeof(float));
const float VECTOR[DIM] = {1.25F, -2.0F, 3.5F, 4.0F, -5.25F, 0.5F, 7.0F};
const float QUERY[DIM] = {-1.0F, 2.5F, 0.25F, 4.5F, 2.0F, -3.0F, 1.5F};
const float CENTROID[DIM] = {8.0F, 7.0F, 6.0F, 5.0F, 4.0F, 3.0F, 2.0F};

ObQuantizationBuffer test_buffer(const void *data, const int64_t size)
{
  return {static_cast<uint8_t *>(const_cast<void *>(data)), size};
}

ObQuantizationParams make_params()
{
  // FP32 operations must not require RaBitQ rotation state.
  return {DIM, 3, {}, 0.0F};
}

ObQuantizationBuffer mutable_code(float (&code)[DIM])
{
  return {reinterpret_cast<uint8_t *>(code), CODE_SIZE};
}

ObQuantizationBuffer test_code(const float (&code)[DIM])
{
  return test_buffer(code, CODE_SIZE);
}

int compute_metric(const share::ObVectorIndexDistAlgorithm metric,
                   const float *query,
                   const float (&base_code)[DIM],
                   float &value,
                   const bool is_norm = false)
{
  const ObQuantizerMetricOps *ops = nullptr;
  int ret = ObQuantizerRegistry::get_ops(ObQuantizationType::FP32, metric, ops);
  if (OB_SUCC(ret)) {
    ObQuantizationParams params = make_params();
    params.is_norm_ = is_norm;
    ret = ops->compute(
        params,
        test_buffer(query, CODE_SIZE),
        test_code(base_code),
        value);
  }
  return ret;
}

int compute_metric_with_buffers(const share::ObVectorIndexDistAlgorithm metric,
                                const ObQuantizationBuffer &query_code,
                                const ObQuantizationBuffer &base_code,
                                float &value)
{
  const ObQuantizerMetricOps *ops = nullptr;
  int ret = ObQuantizerRegistry::get_ops(ObQuantizationType::FP32, metric, ops);
  if (OB_SUCC(ret)) {
    ret = ops->compute(make_params(), query_code, base_code, value);
  }
  return ret;
}

const ObQuantizerMetricOps *get_l2_ops()
{
  const ObQuantizerMetricOps *ops = nullptr;
  EXPECT_EQ(OB_SUCCESS,
            ObQuantizerRegistry::get_ops(ObQuantizationType::FP32,
                                         share::VIDA_L2,
                                         ops));
  EXPECT_NE(nullptr, ops);
  return ops;
}

void expect_buffer_unchanged(const uint8_t *before,
                             const ObQuantizationBuffer &after,
                             const int64_t size)
{
  EXPECT_EQ(0, memcmp(before, after.data_, size));
}

} // namespace

TEST(TestFp32Quantizer, EncodeIsNotSupported)
{
  const ObQuantizerMetricOps *ops = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObQuantizerRegistry::get_ops(ObQuantizationType::FP32,
                                         share::VIDA_L2,
                                         ops));
  alignas(float) float code[DIM] = {};
  EXPECT_EQ(OB_NOT_SUPPORTED,
            ops->encode(make_params(),
                        VECTOR,
                        CENTROID,
                        {nullptr, 0},
                        mutable_code(code)));
}

TEST(TestFp32Quantizer, L2SquaredMatchesObVectorL2Distance)
{
  double expected = 0.0;
  ASSERT_EQ(OB_SUCCESS, ObVectorL2Distance<float>::l2_square_func(QUERY, VECTOR, DIM, expected));
  float actual = 0.0F;
  ASSERT_EQ(OB_SUCCESS, compute_metric(share::VIDA_L2, QUERY, VECTOR, actual));
  EXPECT_FLOAT_EQ(static_cast<float>(expected), actual);
}

TEST(TestFp32Quantizer, InnerProductReturnsDistance)
{
  double similarity = 0.0;
  ASSERT_EQ(OB_SUCCESS,
            ObVectorIpDistance<float>::ip_distance_func(
                QUERY, VECTOR, DIM, similarity));
  float actual = 0.0F;
  ASSERT_EQ(OB_SUCCESS, compute_metric(share::VIDA_IP, QUERY, VECTOR, actual));
  EXPECT_FLOAT_EQ(static_cast<float>(1.0 - similarity), actual);
}

TEST(TestFp32Quantizer, CosineDefaultsToUnnormalizedDistance)
{
  ObQuantizationParams default_params;
  EXPECT_FALSE(default_params.is_norm_);
  EXPECT_FALSE(make_params().is_norm_);
  double expected = 0.0;
  ASSERT_EQ(OB_SUCCESS,
      ObVectorCosineDistance<float>::cosine_distance_func(QUERY, VECTOR, DIM, expected));
  float actual = 0.0F;
  ASSERT_EQ(OB_SUCCESS, compute_metric(share::VIDA_COS, QUERY, VECTOR, actual));
  EXPECT_FLOAT_EQ(static_cast<float>(expected), actual);
}

TEST(TestFp32Quantizer, UnnormalizedCosineZeroVectorsReturnNullValue)
{
  const float zero[DIM] = {};
  float actual = 123.0F;
  EXPECT_EQ(OB_ERR_NULL_VALUE, compute_metric(share::VIDA_COS, QUERY, zero, actual));
  EXPECT_FLOAT_EQ(123.0F, actual);
  EXPECT_EQ(OB_ERR_NULL_VALUE, compute_metric(share::VIDA_COS, zero, VECTOR, actual));
  EXPECT_FLOAT_EQ(123.0F, actual);
  EXPECT_EQ(OB_ERR_NULL_VALUE, compute_metric(share::VIDA_COS, zero, zero, actual));
  EXPECT_FLOAT_EQ(123.0F, actual);
}

TEST(TestFp32Quantizer, ComputerPreservesCosineNormalizationMode)
{
  const float zero[DIM] = {};
  for (const bool is_norm : {false, true}) {
    ObArenaAllocator allocator("Fp32NormMode");
    ObQuantizationParams params = make_params();
    params.is_norm_ = is_norm;
    ObQuantizationComputer computer;
    ASSERT_EQ(OB_SUCCESS, computer.init(allocator, ObQuantizationType::FP32,
        share::VIDA_COS, params, zero, nullptr));
    float actual = 123.0F;
    EXPECT_EQ(is_norm ? OB_SUCCESS : OB_ERR_NULL_VALUE,
        computer.compute(test_code(VECTOR), actual));
    EXPECT_FLOAT_EQ(is_norm ? 1.0F : 123.0F, actual);
  }
}

TEST(TestFp32Quantizer, CosineUsesInnerProductOfNormalizedVectors)
{
  float query[DIM];
  float base[DIM];
  MEMCPY(query, QUERY, sizeof(query));
  MEMCPY(base, VECTOR, sizeof(base));
  ASSERT_EQ(OB_SUCCESS, share::ObVectorNormalize::L2_normalize_vector(DIM, query, query));
  ASSERT_EQ(OB_SUCCESS, share::ObVectorNormalize::L2_normalize_vector(DIM, base, base));
  double expected = 0.0;
  ASSERT_EQ(OB_SUCCESS,
      ObVectorCosineDistance<float>::cosine_distance_func(query, base, DIM, expected));
  float actual = 0.0F;
  ASSERT_EQ(OB_SUCCESS, compute_metric(share::VIDA_COS, query, base, actual, true));
  EXPECT_NEAR(static_cast<float>(expected), actual, 1e-6F);
  float ip_distance = 0.0F;
  ASSERT_EQ(OB_SUCCESS, compute_metric(share::VIDA_IP, query, base, ip_distance));
  EXPECT_FLOAT_EQ(ip_distance, actual);
}

TEST(TestFp32Quantizer, CosineZeroNormUsesRaBitQDistance)
{
  const float zero[DIM] = {};
  float actual = 123.0F;
  ASSERT_EQ(OB_SUCCESS, compute_metric(share::VIDA_COS, QUERY, zero, actual, true));
  EXPECT_FLOAT_EQ(1.0F, actual);
  ASSERT_EQ(OB_SUCCESS, compute_metric(share::VIDA_COS, zero, VECTOR, actual, true));
  EXPECT_FLOAT_EQ(1.0F, actual);
  ASSERT_EQ(OB_SUCCESS, compute_metric(share::VIDA_COS, zero, zero, actual, true));
  EXPECT_FLOAT_EQ(1.0F, actual);

  // Index ordering does not change the scalar cosine NULL contract.
  double distance = 0.0;
  EXPECT_EQ(OB_ERR_NULL_VALUE,
      ObVectorCosineDistance<float>::cosine_distance_func(zero, VECTOR, DIM, distance));
}

TEST(TestFp32Quantizer, CosineIdenticalVectorIsExactlyZero)
{
  const float vector[2] = {1.0F, 0.0F};
  ObQuantizationParams params = make_params();
  params.dim_ = 2;
  const ObQuantizerMetricOps *ops = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObQuantizerRegistry::get_ops(ObQuantizationType::FP32,
                                         share::VIDA_COS,
                                         ops));
  ASSERT_NE(nullptr, ops);
  float actual = 123.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         test_buffer(vector, sizeof(vector)),
                         test_buffer(vector, sizeof(vector)),
                         actual));
  EXPECT_FLOAT_EQ(0.0F, actual);
}

TEST(TestFp32Quantizer, ComputeRejectsShortBuffers)
{
  float actual = 123.0F;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            compute_metric_with_buffers(share::VIDA_L2,
                                        test_buffer(QUERY, CODE_SIZE - 1),
                                        test_buffer(VECTOR, CODE_SIZE),
                                        actual));
  EXPECT_FLOAT_EQ(123.0F, actual);

  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            compute_metric_with_buffers(share::VIDA_L2,
                                        test_buffer(QUERY, CODE_SIZE),
                                        test_buffer(VECTOR, CODE_SIZE - 1),
                                        actual));
  EXPECT_FLOAT_EQ(123.0F, actual);
}

TEST(TestFp32Quantizer, ComputeRejectsNullQueryBuffer)
{
  float actual = 123.0F;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            compute_metric_with_buffers(share::VIDA_L2,
                                        {nullptr, CODE_SIZE},
                                        test_buffer(VECTOR, CODE_SIZE),
                                        actual));
  EXPECT_FLOAT_EQ(123.0F, actual);
}

TEST(TestFp32Quantizer, ComputeRejectsNullBaseBuffer)
{
  float actual = 123.0F;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            compute_metric_with_buffers(share::VIDA_L2,
                                        test_buffer(QUERY, CODE_SIZE),
                                        {nullptr, CODE_SIZE},
                                        actual));
  EXPECT_FLOAT_EQ(123.0F, actual);
}

TEST(TestFp32Quantizer, ComputeAllowsMisalignedBuffers)
{
  alignas(float) uint8_t query_storage[CODE_SIZE + 1];
  alignas(float) uint8_t vector_storage[CODE_SIZE + 1];
  uint8_t *misaligned_query = query_storage + 1;
  uint8_t *misaligned_vector = vector_storage + 1;
  MEMCPY(misaligned_query, QUERY, CODE_SIZE);
  MEMCPY(misaligned_vector, VECTOR, CODE_SIZE);
  ASSERT_NE(0,
      reinterpret_cast<int64_t>(misaligned_query) % alignof(float));
  ASSERT_NE(0,
      reinterpret_cast<int64_t>(misaligned_vector) % alignof(float));

  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (const auto metric : metrics) {
    SCOPED_TRACE(::testing::Message() << "metric=" << metric);
    float expected = 0.0F;
    ASSERT_EQ(OB_SUCCESS, compute_metric(metric, QUERY, VECTOR, expected));
    float actual = 123.0F;
    ASSERT_EQ(OB_SUCCESS,
        compute_metric_with_buffers(
            metric,
            {misaligned_query, CODE_SIZE},
            {misaligned_vector, CODE_SIZE},
            actual));
    EXPECT_FLOAT_EQ(expected, actual);
  }
}

TEST(TestFp32Quantizer, EncoderInitIsNotSupported)
{
  ObArenaAllocator allocator("Fp32NullCtr");
  ObQuantizationEncoder encoder;
  EXPECT_EQ(OB_NOT_SUPPORTED,
            encoder.init(allocator,
                         ObQuantizationType::FP32,
                         share::VIDA_L2,
                         make_params()));
}

TEST(TestFp32Quantizer, QuerySizingIsSupportedButEncodingSizingIsNot)
{
  const ObQuantizerMetricOps *ops = get_l2_ops();
  int64_t scratch_size = -1;
  ASSERT_NE(nullptr, ops->calc_encode_scratch_size);
  EXPECT_EQ(OB_NOT_SUPPORTED,
            ObFp32Quantizer::calc_encode_scratch_size(make_params(), scratch_size));
  EXPECT_EQ(-1, scratch_size);
  int64_t code_size = -1;
  EXPECT_EQ(OB_NOT_SUPPORTED,
            ObFp32Quantizer::calc_code_size(make_params(), code_size));
  EXPECT_EQ(-1, code_size);
  int64_t query_code_size = -1;
  ASSERT_NE(nullptr, ops->calc_query_code_size);
  EXPECT_EQ(OB_SUCCESS, ObFp32Quantizer::calc_query_code_size(make_params(), query_code_size));
  EXPECT_EQ(CODE_SIZE, query_code_size);
  query_code_size = -1;
  EXPECT_EQ(OB_SUCCESS, ops->calc_query_code_size(make_params(), query_code_size));
  EXPECT_EQ(CODE_SIZE, query_code_size);
}

TEST(TestFp32Quantizer, ComputerPreparesAndComputesExactFp32Query)
{
  ObArenaAllocator allocator("Fp32Computer");
  ObQuantizationComputer computer;
  ASSERT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::FP32,
                          share::VIDA_L2,
                          make_params(),
                          QUERY,
                          nullptr));
  float actual = 123.0F;
  ASSERT_EQ(OB_SUCCESS, computer.compute(test_code(VECTOR), actual));
  double expected = 0.0;
  ASSERT_EQ(OB_SUCCESS, ObVectorL2Distance<float>::l2_square_func(QUERY, VECTOR, DIM, expected));
  EXPECT_FLOAT_EQ(static_cast<float>(expected), actual);
}

TEST(TestFp32Quantizer, ComputerReprepareRejectsBorrowedQueryWithoutWriting)
{
  ObArenaAllocator allocator("Fp32Computer");
  for (auto metric : {share::VIDA_L2, share::VIDA_IP, share::VIDA_COS}) {
    float query[DIM];
    memcpy(query, QUERY, CODE_SIZE);
    ObQuantizationComputer computer;
    ASSERT_EQ(OB_SUCCESS,
              computer.init(allocator, ObQuantizationType::FP32, metric,
                            make_params(), query, nullptr));
    EXPECT_EQ(OB_NOT_SUPPORTED, computer.reprepare_query(VECTOR, nullptr));
    EXPECT_EQ(0, memcmp(query, QUERY, CODE_SIZE));
    EXPECT_FALSE(computer.is_inited());
  }
}

TEST(TestFp32Quantizer, ComputerReprepareRejectsReadOnlyQueryAndAllowsReinit)
{
  ObArenaAllocator allocator("Fp32Computer");
  const float query_raw_norm = 1.0F;
  for (auto metric : {share::VIDA_L2, share::VIDA_IP, share::VIDA_COS}) {
    ObQuantizationComputer computer;
    // QUERY has static const storage: reprepare must not write through the
    // pointer borrowed by init, even when the optional norm is supplied.
    ASSERT_EQ(OB_SUCCESS,
              computer.init(allocator, ObQuantizationType::FP32, metric,
                            make_params(), QUERY, nullptr));
    ASSERT_EQ(OB_NOT_SUPPORTED,
              computer.reprepare_query(VECTOR, nullptr, &query_raw_norm));
    EXPECT_FALSE(computer.is_inited());
    float actual = 0.0F;
    EXPECT_EQ(OB_NOT_INIT, computer.compute(test_code(VECTOR), actual));
    ASSERT_EQ(OB_SUCCESS,
              computer.init(allocator, ObQuantizationType::FP32, metric,
                            make_params(), VECTOR, nullptr));
    ASSERT_EQ(OB_SUCCESS, computer.compute(test_code(QUERY), actual));
    float expected = 0.0F;
    ASSERT_EQ(OB_SUCCESS, compute_metric(metric, VECTOR, QUERY, expected));
    EXPECT_FLOAT_EQ(expected, actual);
  }
}

TEST(TestFp32Quantizer, PrepareQueryCopiesQueryAndIgnoresCentroid)
{
  const ObQuantizerMetricOps *ops = get_l2_ops();
  alignas(float) float query_code[DIM] = {};
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(make_params(),
                               QUERY,
                               nullptr,
                               mutable_code(query_code)));
  EXPECT_EQ(0, memcmp(QUERY, query_code, CODE_SIZE));
}

TEST(TestFp32Quantizer, PrepareQueryAllowsAliasedInputAndOutput)
{
  const ObQuantizerMetricOps *ops = get_l2_ops();
  alignas(float) float query_code[DIM] = {};
  memcpy(query_code, QUERY, CODE_SIZE);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(make_params(),
                               query_code,
                               nullptr,
                               mutable_code(query_code)));
  EXPECT_EQ(0, memcmp(QUERY, query_code, CODE_SIZE));
}

TEST(TestFp32Quantizer, QuerySizingRejectsInvalidDimensionsWithoutChangingSize)
{
  int64_t size = 123;
  ObQuantizationParams params = make_params();
  params.dim_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObFp32Quantizer::calc_query_code_size(params, size));
  EXPECT_EQ(123, size);
  params.dim_ = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObFp32Quantizer::calc_query_code_size(params, size));
  EXPECT_EQ(123, size);
}

TEST(TestFp32Quantizer, PrepareQueryRejectsInvalidInputWithoutChangingOutput)
{
  const ObQuantizerMetricOps *ops = get_l2_ops();
  uint8_t storage[CODE_SIZE] = {};
  memset(storage, 0xA5, sizeof(storage));
  uint8_t before[CODE_SIZE] = {};
  memcpy(before, storage, sizeof(before));
  ObQuantizationBuffer output = {storage, CODE_SIZE};

  EXPECT_EQ(OB_INVALID_ARGUMENT, ops->prepare_query(make_params(), nullptr, nullptr, output));
  expect_buffer_unchanged(before, output, CODE_SIZE);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
            ops->prepare_query(make_params(), QUERY, nullptr, {storage, CODE_SIZE - 1}));
  expect_buffer_unchanged(before, output, CODE_SIZE);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->prepare_query(make_params(), QUERY, nullptr, {nullptr, CODE_SIZE}));
  expect_buffer_unchanged(before, output, CODE_SIZE);

  ObQuantizationParams invalid_params = make_params();
  invalid_params.dim_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->prepare_query(invalid_params, QUERY, nullptr, output));
  expect_buffer_unchanged(before, output, CODE_SIZE);
}

TEST(TestFp32Quantizer, ComputeRejectsInvalidDimensionWithoutChangingMetric)
{
  const ObQuantizerMetricOps *ops = get_l2_ops();
  float actual = 123.0F;
  ObQuantizationParams invalid_params = make_params();
  invalid_params.dim_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->compute(invalid_params, test_buffer(QUERY, CODE_SIZE),
                         test_buffer(VECTOR, CODE_SIZE), actual));
  EXPECT_FLOAT_EQ(123.0F, actual);

}

} // namespace quantization
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
