/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <algorithm>
#include <float.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <random>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "common/ob_target_specific.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "share/vector_index/quantization/ob_quantizer_registry.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_simd.h"

namespace oceanbase
{
namespace quantization
{

using namespace common;

namespace
{

const int64_t DIM = 9;

ObQuantizationBuffer test_buffer(const void *data, const int64_t size)
{
  return {static_cast<uint8_t *>(const_cast<void *>(data)), size};
}

using DotPackedCodesResult = decltype(get_rabitq_simd_ops().dot_packed_codes(
    nullptr, nullptr, 0, 0.0F));
static_assert(std::is_same<DotPackedCodesResult, float>::value,
              "RaBitQ packed-code dot product must use the native float result type");

class AlignedByteBuffer
{
public:
  explicit AlignedByteBuffer(const int64_t size = 0, const uint8_t value = 0)
      : storage_(), size_(0)
  {
    assign(size, value);
  }

  AlignedByteBuffer(const AlignedByteBuffer &) = default;
  AlignedByteBuffer &operator=(const AlignedByteBuffer &) = default;

  void assign(const int64_t size, const uint8_t value)
  {
    size_ = size;
    storage_.assign(
        (size + static_cast<int64_t>(sizeof(float)) - 1) / sizeof(float),
        0.0F);
    if (size_ > 0) {
      memset(data(), value, size_);
    }
  }

  uint8_t *data() { return reinterpret_cast<uint8_t *>(storage_.data()); }
  const uint8_t *data() const
  {
    return reinterpret_cast<const uint8_t *>(storage_.data());
  }
  int64_t size() const { return size_; }
  uint8_t &operator[](const int64_t index) { return data()[index]; }
  const uint8_t &operator[](const int64_t index) const { return data()[index]; }

private:
  std::vector<float> storage_;
  int64_t size_;
};

int64_t packed_bytes(const int64_t dim)
{
  return dim / 8 + (dim % 8 == 0 ? 0 : 1);
}

ObQuantizationParams make_params(const int8_t bits,
                                 const uint8_t *signs,
                                 const int64_t signs_size)
{
  ObFhtKacRotator rotator;
  if (nullptr != signs) {
    EXPECT_EQ(OB_SUCCESS, rotator.init(signs, signs_size));
  }
  return {DIM, bits, rotator, calc_inv_sqrt_dim(DIM)};
}

ObQuantizationParams make_params(const int64_t dim,
                                 const int8_t bits,
                                 const uint8_t *signs,
                                 const int64_t signs_size)
{
  ObFhtKacRotator rotator;
  if (nullptr != signs) {
    EXPECT_EQ(OB_SUCCESS, rotator.init(signs, signs_size));
  }
  return {dim, bits, rotator, calc_inv_sqrt_dim(dim)};
}

std::vector<uint8_t> make_generated_signs(const int64_t dim)
{
  ObArenaAllocator allocator("RaBQGenSigns");
  ObFhtKacRotator rotator;
  const int ret = ObFhtKacRotator::generate(allocator, dim, rotator);
  EXPECT_EQ(OB_SUCCESS, ret);
  const uint8_t *signs = rotator.signs();
  const int64_t signs_size = 4 * packed_bytes(dim);
  return OB_SUCCESS == ret
             ? std::vector<uint8_t>(signs, signs + signs_size)
             : std::vector<uint8_t>();
}

void pack_sign_codes_reference(const float *normalized_residual,
                               const int64_t dim,
                               uint8_t *packed_codes)
{
  const int64_t packed_size = packed_bytes(dim);
  memset(packed_codes, 0, packed_size);
  for (int64_t i = 0; i < dim; ++i) {
    if (!(normalized_residual[i] < 0.0F)) {
      packed_codes[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
    }
  }
}

int64_t metadata_offset(const int64_t dim, const int64_t field_index)
{
  const int64_t packed_size = packed_bytes(dim);
  const int64_t aligned_packed_size =
      (packed_size + static_cast<int64_t>(sizeof(float)) - 1)
      / static_cast<int64_t>(sizeof(float)) * sizeof(float);
  return aligned_packed_size + field_index * sizeof(float);
}

uint8_t decode_scalar_code(const uint8_t *base_code,
                           const int64_t dim,
                           const int64_t index)
{
  return 0 != (base_code[index / 8]
               & static_cast<uint8_t>(1U << (index % 8)));
}

double decoded_objective(const uint8_t *base_code,
                         const int64_t dim,
                         const float *normalized_residual)
{
  double dot = 0.0;
  double norm2 = 0.0;
  for (int64_t i = 0; i < dim; ++i) {
    const double y = static_cast<double>(decode_scalar_code(base_code, dim, i)) - 0.5;
    dot += static_cast<double>(normalized_residual[i]) * y;
    norm2 += y * y;
  }
  return dot / sqrt(norm2);
}

float binary_inner_product_reference(const float *query,
                                     const uint8_t *base_code,
                                     const int64_t dim,
                                     const float inv_sqrt_dim)
{
  const float positive = inv_sqrt_dim > 1e-3F ? inv_sqrt_dim : 1.0F;
  const float negative = inv_sqrt_dim > 1e-3F ? -inv_sqrt_dim : 0.0F;
  float result = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    result += query[i] * (decode_scalar_code(base_code, dim, i) ? positive : negative);
  }
  return result;
}

float simd_tolerance(const float expected)
{
  return 1e-5F * std::max(1.0F, fabsf(expected));
}

void init_arches_quietly()
{
  fflush(stdout);
  const int saved_stdout = dup(STDOUT_FILENO);
  if (saved_stdout >= 0) {
    freopen("/dev/null", "w", stdout);
    common::init_arches();
    fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);
  } else {
    common::init_arches();
  }
}

float euclidean_norm(const float *vector, const int64_t dim);

void make_normalized_base_residual(const float *vector,
                                  const float *centroid,
                                  const int64_t dim,
                                  std::vector<float> &normalized_residual,
                                  float &base_residual_norm)
{
  normalized_residual.resize(dim);
  double norm2 = 0.0;
  for (int64_t i = 0; i < dim; ++i) {
    normalized_residual[i] = vector[i] - centroid[i];
    norm2 += static_cast<double>(normalized_residual[i]) * normalized_residual[i];
  }
  base_residual_norm = static_cast<float>(sqrt(norm2));
  ASSERT_GT(base_residual_norm, 0.0F);
  for (int64_t i = 0; i < dim; ++i) {
    normalized_residual[i] /= base_residual_norm;
  }
}

int rotate_input(const ObQuantizationParams &params,
                 const float *input,
                 std::vector<float> &output)
{
  output.assign(input, input + params.dim_);
  return params.rotator_.rotate(params.dim_, output.data());
}

float load_float_native(const uint8_t *data)
{
  return *reinterpret_cast<const float *>(data);
}

void store_float_native(uint8_t *data, const float value)
{
  *reinterpret_cast<float *>(data) = value;
}

void expect_byte_span(const uint8_t *data,
                      const int64_t size,
                      const uint8_t value)
{
  const std::vector<uint8_t> expected(size, value);
  EXPECT_EQ(0, memcmp(expected.data(), data, size));
}

void expect_compute_rejection(const ObQuantizerMetricOps &ops,
                              const ObQuantizationParams &params,
                              const ObQuantizationBuffer &query_code,
                              const ObQuantizationBuffer &base_code,
                              const int expected_ret)
{
  const float metric_sentinel = -123.25F;
  float metric_value = metric_sentinel;
  EXPECT_EQ(expected_ret,
            ops.compute(params, query_code, base_code, metric_value));
  EXPECT_FLOAT_EQ(metric_sentinel, metric_value);
}

class MisaligningAllocator : public ObIAllocator
{
public:
  MisaligningAllocator() : last_size_(0), last_pointer_(nullptr) {}

  ~MisaligningAllocator() override
  {
    for (int64_t i = 0; i < static_cast<int64_t>(allocations_.size()); ++i) {
      ::free(allocations_[i].raw_);
    }
  }

  void *alloc(const int64_t size) override
  {
    void *raw = malloc(size + 1);
    void *pointer = nullptr == raw ? nullptr : static_cast<uint8_t *>(raw) + 1;
    if (nullptr != raw) {
      allocations_.push_back({raw, pointer});
      last_size_ = size;
      last_pointer_ = pointer;
    }
    return pointer;
  }

  void *alloc(const int64_t size, const lib::ObMemAttr &) override
  {
    return alloc(size);
  }

  void free(void *pointer) override
  {
    for (int64_t i = 0; i < static_cast<int64_t>(allocations_.size()); ++i) {
      Allocation &allocation = allocations_[i];
      if (allocation.pointer_ == pointer && nullptr != allocation.raw_) {
        ::free(allocation.raw_);
        allocation.raw_ = nullptr;
        allocation.pointer_ = nullptr;
      }
    }
  }

  int64_t last_size() const { return last_size_; }
  const void *last_pointer() const { return last_pointer_; }

private:
  struct Allocation
  {
    void *raw_;
    void *pointer_;
  };

  std::vector<Allocation> allocations_;
  int64_t last_size_;
  void *last_pointer_;
};

int64_t required_base_code_size(const int64_t dim)
{
  return metadata_offset(dim, 2) + static_cast<int64_t>(sizeof(float));
}

float euclidean_norm(const float *vector, const int64_t dim)
{
  float norm2 = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    norm2 += vector[i] * vector[i];
  }
  return sqrtf(norm2);
}

void generate_uniform_vectors(const int64_t count,
                              const int64_t dim,
                              const uint32_t seed,
                              std::vector<float> &vectors)
{
  std::mt19937 generator(seed);
  std::uniform_real_distribution<float> distribution(0.1F, 0.9F);
  vectors.resize(count * dim);
  for (int64_t i = 0; i < count * dim; ++i) {
    vectors[i] = distribution(generator);
  }
}

std::vector<uint8_t> make_deterministic_signs(const int64_t dim,
                                              const uint32_t seed)
{
  const int64_t bytes_per_round = packed_bytes(dim);
  std::vector<uint8_t> signs(4 * bytes_per_round, 0);
  std::mt19937 generator(seed);
  std::uniform_int_distribution<int> distribution(0, 255);
  for (int64_t round = 0; round < 4; ++round) {
    for (int64_t byte_index = 0; byte_index < bytes_per_round; ++byte_index) {
      signs[round * bytes_per_round + byte_index] =
          static_cast<uint8_t>(distribution(generator));
    }
    if (dim % 8 != 0) {
      signs[(round + 1) * bytes_per_round - 1] &=
          static_cast<uint8_t>((1U << (dim % 8)) - 1U);
    }
  }
  return signs;
}

float fp32_distance_reference(const share::ObVectorIndexDistAlgorithm metric,
                              const float *query,
                              const float *base,
                              const int64_t dim)
{
  double dot = 0.0;
  double query_norm2 = 0.0;
  double base_norm2 = 0.0;
  double l2_squared = 0.0;
  for (int64_t i = 0; i < dim; ++i) {
    const double query_value = query[i];
    const double base_value = base[i];
    const double delta = query_value - base_value;
    dot += query_value * base_value;
    query_norm2 += query_value * query_value;
    base_norm2 += base_value * base_value;
    l2_squared += delta * delta;
  }
  float result = static_cast<float>(l2_squared);
  if (share::VIDA_IP == metric) {
    result = static_cast<float>(1.0 - dot);
  } else if (share::VIDA_COS == metric) {
    result = query_norm2 <= 0.0 || base_norm2 <= 0.0
                 ? 1.0F
                 : static_cast<float>(1.0 - dot / sqrt(query_norm2 * base_norm2));
  }
  return result;
}

int make_normalized_rotated_query_residual(const ObQuantizationParams &params,
                                           const float *rotated_query,
                                           const float *rotated_centroid,
                                           std::vector<float> &normalized_residual,
                                           float &residual_norm)
{
  int ret = OB_SUCCESS;
  normalized_residual.assign(rotated_query, rotated_query + params.dim_);
  for (int64_t i = 0; i < params.dim_; ++i) {
    normalized_residual[i] -= rotated_centroid[i];
  }
  residual_norm = euclidean_norm(normalized_residual.data(), params.dim_);
  if (residual_norm > 0.0F) {
    for (int64_t i = 0; i < params.dim_; ++i) {
      normalized_residual[i] /= residual_norm;
    }
  }
  return ret;
}

double independent_normalized_dot_estimate(const share::ObVectorIndexDistAlgorithm metric,
                                           const ObQuantizationParams &params,
                                           const float *rotated_query,
                                           const float *rotated_centroid,
                                           const uint8_t *base_code,
                                           const int64_t base_code_size,
                                           float &query_residual_norm,
                                           float &base_residual_norm)
{
  if (params.dim_ <= 0 || nullptr == rotated_query || nullptr == rotated_centroid
      || nullptr == base_code || base_code_size < required_base_code_size(params.dim_)) {
    ADD_FAILURE() << "invalid independent normalized-dot input";
    return NAN;
  }
  std::vector<float> normalized_residual;
  if (OB_SUCCESS != make_normalized_rotated_query_residual(params,
                                                           rotated_query,
                                                           rotated_centroid,
                                                           normalized_residual,
                                                           query_residual_norm)) {
    ADD_FAILURE() << "failed to normalize independent rotated query";
    return NAN;
  }
  if (query_residual_norm < 0.0F) {
    ADD_FAILURE() << "invalid independent query residual norm";
    return NAN;
  }
  base_residual_norm =
      load_float_native(base_code + metadata_offset(params.dim_, 0));
  const float correction =
      load_float_native(base_code + metadata_offset(params.dim_, 1));
  if (base_residual_norm < 0.0F || correction <= 0.0F) {
    ADD_FAILURE() << "invalid independent base metadata";
    return NAN;
  }
  const float binary_inner_product = binary_inner_product_reference(
      normalized_residual.data(),
      base_code,
      params.dim_,
      params.inv_sqrt_dim_);
  float base_error = correction;
  if (fabsf(base_error) < 1e-5F) {
    base_error = base_error > 0.0F ? 1.0F : -1.0F;
  }
  const float normalized_dot_estimate = binary_inner_product / base_error;
  return normalized_dot_estimate;
}

float independent_estimate(const share::ObVectorIndexDistAlgorithm metric,
                           const ObQuantizationParams &params,
                           const float *rotated_query,
                           const float *rotated_centroid,
                           const uint8_t *query_code,
                           const int64_t query_code_size,
                           const uint8_t *base_code,
                           const int64_t base_code_size)
{
  if (params.dim_ <= 0 || nullptr == rotated_query || nullptr == rotated_centroid
      || nullptr == base_code || base_code_size < required_base_code_size(params.dim_)) {
    ADD_FAILURE() << "invalid independent estimate base input";
    return NAN;
  }
  const int64_t query_raw_norm_offset =
      (params.dim_ + 1) * static_cast<int64_t>(sizeof(float));
  if (share::VIDA_L2 != metric
      && (nullptr == query_code
          || query_code_size < query_raw_norm_offset + static_cast<int64_t>(sizeof(float)))) {
    ADD_FAILURE() << "query code has no raw-norm metadata";
    return NAN;
  }
  float query_residual_norm = 0.0F;
  float base_residual_norm = 0.0F;
  const double normalized_dot_estimate = independent_normalized_dot_estimate(
      metric,
      params,
      rotated_query,
      rotated_centroid,
      base_code,
      base_code_size,
      query_residual_norm,
      base_residual_norm);
  const float estimated_l2_squared =
      base_residual_norm * base_residual_norm
      + query_residual_norm * query_residual_norm
      - 2.0F * base_residual_norm * query_residual_norm
            * static_cast<float>(normalized_dot_estimate);
  if (share::VIDA_L2 == metric) {
    return estimated_l2_squared;
  }
  const float query_raw_norm =
      load_float_native(query_code + query_raw_norm_offset);
  const float expected_query_raw_norm =
      euclidean_norm(rotated_query, params.dim_);
  if (query_raw_norm < 0.0F) {
    ADD_FAILURE() << "invalid independent query raw norm";
    return NAN;
  }
  EXPECT_NEAR(expected_query_raw_norm, query_raw_norm, 1e-6F);
  const float base_raw_norm =
      load_float_native(base_code + metadata_offset(params.dim_, 2));
  if (base_raw_norm < 0.0F) {
    ADD_FAILURE() << "invalid independent base raw norm";
    return NAN;
  }
  if (static_cast<double>(query_raw_norm) < 1e-5
      || static_cast<double>(base_raw_norm) < 1e-5) {
    return 1.0F;
  }
  const float recovered_inner_product =
      (query_raw_norm * query_raw_norm
       + base_raw_norm * base_raw_norm
       - estimated_l2_squared) * 0.5F;
  if (share::VIDA_IP == metric) {
    return 1.0F - recovered_inner_product;
  }
  return 1.0F - recovered_inner_product / (query_raw_norm * base_raw_norm);
}

int encode_and_compute(const share::ObVectorIndexDistAlgorithm metric,
                       const ObQuantizationParams &params,
                       const float *rotated_query,
                       const float *vector,
                       const float *rotated_centroid,
                       AlignedByteBuffer &base_code,
                       float &metric_value,
                       AlignedByteBuffer *prepared_query_code = nullptr)
{
  std::vector<float> rotated_vector;
  ObArenaAllocator allocator("RaBitQMetric");
  ObQuantizationEncoder encoder;
  int ret = rotate_input(params, vector, rotated_vector);
  if (OB_SUCCESS == ret) {
    ret = encoder.init(allocator, ObQuantizationType::RABITQ, metric, params);
  }
  if (OB_SUCCESS == ret) {
    base_code.assign(encoder.code_size(), 0);
    ret = encoder.encode(rotated_vector.data(),
                         rotated_centroid,
                         {base_code.data(), static_cast<int64_t>(base_code.size())});
  }
  if (OB_SUCCESS == ret && nullptr != prepared_query_code) {
    int64_t query_code_size = 0;
    const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
        static_cast<int64_t>(metric)];
    if (nullptr == ops) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_SUCCESS == (ret = ObRaBitQQuantizer::calc_query_code_size(
                                params, query_code_size))) {
      prepared_query_code->assign(query_code_size, 0);
      ret = ops->prepare_query(params,
                               rotated_query,
                               rotated_centroid,
                               {prepared_query_code->data(), prepared_query_code->size()});
    }
  }
  ObQuantizationComputer computer;
  if (OB_SUCCESS == ret) {
    ret = computer.init(allocator,
                        ObQuantizationType::RABITQ,
                        metric,
                        params,
                        rotated_query,
                        rotated_centroid);
  }
  if (OB_SUCCESS == ret) {
    ret = computer.compute({base_code.data(), static_cast<int64_t>(base_code.size())},
                           metric_value);
  }
  return ret;
}

void expect_encoder_init_failure(const ObQuantizationParams &params,
                                 const int expected_ret)
{
  ObArenaAllocator allocator("RaBQBadInit");
  ObQuantizationEncoder encoder;
  EXPECT_EQ(expected_ret,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  EXPECT_EQ(0, encoder.code_size());
  EXPECT_EQ(OB_NOT_INIT, encoder.encode(nullptr, nullptr, {nullptr, 0}));
}

} // namespace

TEST(TestRaBitQQuantizer, AcceptsOnlyOneBit)
{
  const std::vector<uint8_t> signs = make_generated_signs(DIM);
  const ObQuantizationParams one_bit = make_params(1, signs.data(), signs.size());
  int64_t size = -1;
  EXPECT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_code_size(one_bit, size));
  EXPECT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(one_bit, size));
  EXPECT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_encode_scratch_size(one_bit, size));

  const int8_t rejected[] = {-1, 0, 2, 3, 4, 8, 64};
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(rejected)); ++i) {
    const ObQuantizationParams params =
        make_params(rejected[i], signs.data(), signs.size());
    size = 123;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObRaBitQQuantizer::calc_code_size(params, size));
    EXPECT_EQ(123, size);
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObRaBitQQuantizer::calc_query_code_size(params, size));
    EXPECT_EQ(123, size);
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObRaBitQQuantizer::calc_encode_scratch_size(params, size));
    EXPECT_EQ(123, size);
  }
}

TEST(TestRaBitQQuantizer, PublicSizingRejectsNonPositiveDimensions)
{
  const int64_t rejected_dimensions[] = {0, -1};
  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(rejected_dimensions) / sizeof(rejected_dimensions[0]));
       ++i) {
    SCOPED_TRACE(::testing::Message() << "dim=" << rejected_dimensions[i]);
    const ObQuantizationParams params =
        make_params(rejected_dimensions[i], 1, nullptr, 0);
    int64_t size = 123;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObRaBitQQuantizer::calc_code_size(params, size));
    EXPECT_EQ(123, size);
    size = 123;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObRaBitQQuantizer::calc_query_code_size(params, size));
    EXPECT_EQ(123, size);
    size = 123;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObRaBitQQuantizer::calc_encode_scratch_size(params, size));
    EXPECT_EQ(123, size);
  }
}

TEST(TestRaBitQQuantizer, DirectOpsRejectNonPositiveDimensions)
{
  const float vector[1] = {0.0F};
  const int64_t rejected_dimensions[] = {0, -1};
  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);
  AlignedByteBuffer scratch(sizeof(float));
  AlignedByteBuffer query_code(2 * sizeof(float));
  AlignedByteBuffer base_code(3 * sizeof(float));
  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(rejected_dimensions) / sizeof(rejected_dimensions[0]));
       ++i) {
    SCOPED_TRACE(::testing::Message() << "dim=" << rejected_dimensions[i]);
    ObQuantizationParams params =
        make_params(rejected_dimensions[i], 1, nullptr, 0);
    params.inv_sqrt_dim_ = 1.0F;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ops->encode(params,
                          vector,
                          vector,
                          {scratch.data(), scratch.size()},
                          {base_code.data(), base_code.size()}));
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ops->prepare_query(params,
                                 vector,
                                 vector,
                                 {query_code.data(), query_code.size()}));
    expect_compute_rejection(*ops,
                             params,
                             {query_code.data(), query_code.size()},
                             {base_code.data(), base_code.size()},
                             OB_INVALID_ARGUMENT);
  }
}

TEST(TestRaBitQQuantizer, ComputerInitRejectsUnsupportedBits)
{
  const float query[DIM] = {1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F};
  const float centroid[DIM] = {};
  const std::vector<uint8_t> signs = make_generated_signs(DIM);
  const int8_t rejected[] = {0, 2, 3, 4, 8, -1, 64};
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(rejected)); ++i) {
    SCOPED_TRACE(::testing::Message() << "bits=" << static_cast<int>(rejected[i]));
    ObArenaAllocator allocator("RaBQBadBits");
    ObQuantizationComputer computer;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              computer.init(allocator,
                            ObQuantizationType::RABITQ,
                            share::VIDA_L2,
                            make_params(rejected[i], signs.data(), signs.size()),
                            query,
                            centroid));
  }
}

TEST(TestRaBitQQuantizer, EncoderInitRejectsInvalidSizingParamsAndRemainsReset)
{
  const std::vector<uint8_t> signs = make_generated_signs(DIM);
  ObQuantizationParams params = make_params(1, signs.data(), signs.size());
  params.dim_ = 0;
  expect_encoder_init_failure(params, OB_INVALID_ARGUMENT);

  const int8_t rejected_bits[] = {0, 2, 3, 4, 8, -1, 64};
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(rejected_bits)); ++i) {
    SCOPED_TRACE(::testing::Message()
                 << "bits=" << static_cast<int>(rejected_bits[i]));
    params = make_params(rejected_bits[i], signs.data(), signs.size());
    expect_encoder_init_failure(params, OB_INVALID_ARGUMENT);
  }
}

TEST(TestRaBitQQuantizer, PublicSizingMatchesCurrentBaseCodeLayout)
{
  const ObQuantizationParams params = make_params(DIM, 1, nullptr, 0);
  int64_t base_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_code_size(params, base_size));
  EXPECT_EQ(required_base_code_size(DIM), base_size);
  EXPECT_EQ(16, base_size);

  // Query layout contains nine floats, residual norm, and raw norm.
  int64_t query_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_size));
  EXPECT_EQ(44, query_size);

  int64_t scratch_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_encode_scratch_size(params, scratch_size));
  EXPECT_EQ(DIM * static_cast<int64_t>(sizeof(float)), scratch_size);
  EXPECT_EQ(36, scratch_size);
}

TEST(TestRaBitQQuantizer, SizingAndQueryPreparationIgnoreZeroDimensionScale)
{
  const int64_t dim = 9;
  const float query[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                            -6.0F, 7.0F, -8.0F, 9.0F};
  const float rotated_centroid[dim] = {};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  params.inv_sqrt_dim_ = 0.0F;

  int64_t base_code_size = 0;
  int64_t query_code_size = 0;
  int64_t scratch_size = 0;
  EXPECT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_code_size(params, base_code_size));
  EXPECT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  EXPECT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_encode_scratch_size(params, scratch_size));

  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);
  AlignedByteBuffer query_code(query_code_size, 0xA5);
  EXPECT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               rotated_query.data(),
                               rotated_centroid,
                               {query_code.data(), query_code.size()}));
}

TEST(TestRaBitQQuantizer, DirectOpsRejectNonPositiveDimensionScaleAtConsumption)
{
  const int64_t dim = 9;
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                             -6.0F, 7.0F, -8.0F, 9.0F};
  const float query[dim] = {-1.0F, 0.5F, 2.0F, -3.0F, 1.0F,
                            8.0F, 4.0F, -2.0F, 0.25F};
  const float rotated_centroid[dim] = {};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams valid_params =
      make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(valid_params, query, rotated_query));
  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);

  int64_t base_code_size = 0;
  int64_t query_code_size = 0;
  int64_t scratch_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_code_size(valid_params, base_code_size));
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_query_code_size(valid_params, query_code_size));
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_encode_scratch_size(valid_params, scratch_size));
  AlignedByteBuffer scratch(scratch_size);
  AlignedByteBuffer valid_base_code(base_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->encode(valid_params,
                        vector,
                        rotated_centroid,
                        {scratch.data(), scratch.size()},
                        {valid_base_code.data(), valid_base_code.size()}));
  AlignedByteBuffer query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(valid_params,
                               rotated_query.data(),
                               rotated_centroid,
                               {query_code.data(), query_code.size()}));

  const float invalid_scales[] = {0.0F, -1.0F, -INFINITY};
  const uint8_t base_sentinel = 0xA5;
  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(invalid_scales) / sizeof(float));
       ++i) {
    SCOPED_TRACE(::testing::Message() << "inv_sqrt_dim=" << invalid_scales[i]);
    ObQuantizationParams invalid_params = valid_params;
    invalid_params.inv_sqrt_dim_ = invalid_scales[i];
    AlignedByteBuffer rejected_base_code(base_code_size, base_sentinel);
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ops->encode(invalid_params,
                          vector,
                          rotated_centroid,
                          {scratch.data(), scratch.size()},
                          {rejected_base_code.data(), rejected_base_code.size()}));
    expect_byte_span(rejected_base_code.data(),
                     rejected_base_code.size(),
                     base_sentinel);
    expect_compute_rejection(*ops,
                             invalid_params,
                             {query_code.data(), query_code.size()},
                             {valid_base_code.data(), valid_base_code.size()},
                             OB_INVALID_ARGUMENT);
  }

  const float unchecked_scales[] = {NAN, INFINITY};
  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(unchecked_scales) / sizeof(float));
       ++i) {
    SCOPED_TRACE(::testing::Message() << "inv_sqrt_dim=" << unchecked_scales[i]);
    ObQuantizationParams unchecked_params = valid_params;
    unchecked_params.inv_sqrt_dim_ = unchecked_scales[i];
    AlignedByteBuffer unchecked_base_code(base_code_size);
    ASSERT_EQ(OB_SUCCESS,
              ops->encode(unchecked_params,
                          vector,
                          rotated_centroid,
                          {scratch.data(), scratch.size()},
                          {unchecked_base_code.data(), unchecked_base_code.size()}));
    float metric_value = 0.0F;
    ASSERT_EQ(OB_SUCCESS,
              ops->compute(unchecked_params,
                           {query_code.data(), query_code.size()},
                           {unchecked_base_code.data(), unchecked_base_code.size()},
                           metric_value));
  }
}

TEST(TestRaBitQQuantizer, BaseCodeStoresRawNorm)
{
  const int64_t dim = 9;
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                             -6.0F, 7.0F, -8.0F, 9.0F};
  const float centroid[dim] = {0.25F, -0.5F, 0.75F, -1.0F, 1.25F,
                               -1.5F, 1.75F, -2.0F, 2.25F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_vector;
  std::vector<float> rotated_centroid;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, vector, rotated_vector));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int64_t metric_index = 0; metric_index < 3; ++metric_index) {
    SCOPED_TRACE(::testing::Message() << "metric=" << metrics[metric_index]);
    ObArenaAllocator allocator("RaBQBaseRaw");
    ObQuantizationEncoder encoder;
    ASSERT_EQ(OB_SUCCESS,
              encoder.init(allocator,
                           ObQuantizationType::RABITQ,
                           metrics[metric_index],
                           params));
    EXPECT_EQ(required_base_code_size(dim), encoder.code_size());
    AlignedByteBuffer base_code(required_base_code_size(dim));
    ASSERT_EQ(OB_SUCCESS,
              encoder.encode(rotated_vector.data(),
                             rotated_centroid.data(),
                             {base_code.data(), base_code.size()}));
    ASSERT_GE(base_code.size(), required_base_code_size(dim));
    EXPECT_NEAR(euclidean_norm(rotated_vector.data(), dim),
                load_float_native(base_code.data() + metadata_offset(dim, 2)),
                1e-6F);
  }
}

TEST(TestRaBitQQuantizer, QueryPreparationConsumesAlreadyRotatedQueryAndCentroid)
{
  const int64_t dim = 9;
  const float query[dim] = {1e10F, 1.5F, -1.25e10F, 1.25e-6F, -5.5e10F,
                            5e8F, -2.5e9F, -3.3e10F, -8e9F};
  const float centroid[dim] = {5e8F, -1e9F, 2.5e8F, 1.5e9F, -5e8F,
                               1e9F, -7.5e8F, 5e8F, 2.5e8F};
  const uint8_t signs[] = {0x57, 0x00, 0xD4, 0x01,
                          0x1E, 0x00, 0xBF, 0x00};
  const int64_t bytes_per_round = packed_bytes(dim);
  ASSERT_EQ(4 * bytes_per_round, static_cast<int64_t>(sizeof(signs)));
  for (int64_t round = 0; round < 4; ++round) {
    const int64_t tail_index = (round + 1) * bytes_per_round - 1;
    ASSERT_EQ(0,
              static_cast<uint8_t>(signs[tail_index] & ~0x01U));
  }
  const ObQuantizationParams params = make_params(dim, 1, signs, sizeof(signs));
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  std::vector<float> rotated_centroid;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  const float origin_query_norm = euclidean_norm(query, dim);
  const float rotated_query_norm = euclidean_norm(rotated_query.data(), dim);
  const float raw_norm_delta = fabsf(origin_query_norm - rotated_query_norm);
  SCOPED_TRACE(::testing::Message()
               << "raw_norm pre=" << origin_query_norm
               << " rotated_query_norm=" << rotated_query_norm
               << " abs_delta=" << raw_norm_delta);
  ASSERT_GT(raw_norm_delta, 8192.0F);
  std::vector<float> expected;
  float query_residual_norm = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            make_normalized_rotated_query_residual(params,
                                                   rotated_query.data(),
                                                   rotated_centroid.data(),
                                                   expected,
                                                   query_residual_norm));
  ASSERT_GT(query_residual_norm, 0.0F);

  std::vector<float> double_rotated_query;
  ASSERT_EQ(OB_SUCCESS,
            rotate_input(params, rotated_query.data(), double_rotated_query));
  for (int64_t i = 0; i < dim; ++i) {
    double_rotated_query[i] -= rotated_centroid[i];
  }
  const float double_rotated_residual_norm =
      euclidean_norm(double_rotated_query.data(), dim);
  ASSERT_GT(double_rotated_residual_norm, 0.0F);
  for (int64_t i = 0; i < dim; ++i) {
    double_rotated_query[i] /= double_rotated_residual_norm;
  }
  float max_component_difference = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    max_component_difference = std::max(
        max_component_difference, fabsf(expected[i] - double_rotated_query[i]));
  }
  EXPECT_GT(max_component_difference, 1e-6F);
  EXPECT_GT(fabsf(query_residual_norm - double_rotated_residual_norm), 1e-6F);
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int64_t metric_index = 0; metric_index < 3; ++metric_index) {
    SCOPED_TRACE(::testing::Message() << "metric=" << metrics[metric_index]);
    const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
        static_cast<int64_t>(metrics[metric_index])];
    ASSERT_NE(nullptr, ops);
    AlignedByteBuffer query_code(query_code_size);
    ASSERT_EQ(OB_SUCCESS,
              ops->prepare_query(params,
                                 rotated_query.data(),
                                 rotated_centroid.data(),
                                 {query_code.data(), query_code.size()}));
    for (int64_t i = 0; i < dim; ++i) {
      EXPECT_NEAR(expected[i],
                  load_float_native(query_code.data() + i * sizeof(float)),
                  1e-6F);
    }
    EXPECT_NEAR(query_residual_norm,
                load_float_native(query_code.data() + dim * sizeof(float)),
                1e-6F);
    const float stored_query_raw_norm =
        load_float_native(query_code.data() + (dim + 1) * sizeof(float));
    EXPECT_EQ(rotated_query_norm, stored_query_raw_norm);
    EXPECT_NE(origin_query_norm, stored_query_raw_norm);
  }
}

TEST(TestRaBitQQuantizer, ComputerUsesAlreadyRotatedQueryAndCentroidContract)
{
  const int64_t dim = 9;
  const float query[dim] = {2.5F, -1.0F, 3.0F, -4.0F, 0.5F,
                            1.25F, -2.0F, 4.5F, -3.5F};
  const float vector[dim] = {-1.5F, 2.0F, 0.25F, 3.5F, -2.0F,
                             4.0F, 1.0F, -0.75F, 2.5F};
  const float centroid[dim] = {0.5F, -0.75F, 1.0F, -1.5F, 0.25F,
                               0.5F, -1.0F, 1.5F, -0.5F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_vector;
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, vector, rotated_vector));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  ObArenaAllocator allocator("RaBQCompCtr");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  AlignedByteBuffer base_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(rotated_vector.data(),
                           rotated_centroid.data(),
                           {base_code.data(), base_code.size()}));

  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer direct_query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               rotated_query.data(),
                               rotated_centroid.data(),
                               {direct_query_code.data(), direct_query_code.size()}));
  float direct_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {direct_query_code.data(), direct_query_code.size()},
                         {base_code.data(), base_code.size()},
                         direct_value));

  std::vector<float> double_rotated_centroid;
  ASSERT_EQ(OB_SUCCESS,
            rotate_input(params, rotated_centroid.data(), double_rotated_centroid));
  AlignedByteBuffer double_rotated_query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               rotated_query.data(),
                               double_rotated_centroid.data(),
                               {double_rotated_query_code.data(),
                                double_rotated_query_code.size()}));
  float double_rotated_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {double_rotated_query_code.data(),
                          double_rotated_query_code.size()},
                         {base_code.data(), base_code.size()},
                         double_rotated_value));
  EXPECT_GT(fabsf(direct_value - double_rotated_value), 1e-6F);

  ObQuantizationComputer computer;
  ASSERT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_L2,
                          params,
                          rotated_query.data(),
                          rotated_centroid.data()));
  float wrapper_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            computer.compute({base_code.data(), base_code.size()}, wrapper_value));
  EXPECT_FLOAT_EQ(direct_value, wrapper_value);
}

TEST(TestRaBitQQuantizer, EncoderInitOverridesCallerDimensionScale)
{
  const int64_t dim = 9;
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                             -6.0F, 7.0F, -8.0F, 9.0F};
  const float centroid[dim] = {0.25F, -0.5F, 0.75F, -1.0F, 1.25F,
                               -1.5F, 1.75F, -2.0F, 2.25F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  ObQuantizationParams caller_params =
      make_params(dim, 1, signs.data(), signs.size());
  const float derived_scale = calc_inv_sqrt_dim(dim);
  caller_params.inv_sqrt_dim_ = derived_scale * 0.25F;
  ASSERT_NE(derived_scale, caller_params.inv_sqrt_dim_);
  std::vector<float> rotated_vector;
  std::vector<float> rotated_centroid;
  ASSERT_EQ(OB_SUCCESS, rotate_input(caller_params, vector, rotated_vector));
  ASSERT_EQ(OB_SUCCESS, rotate_input(caller_params, centroid, rotated_centroid));

  ObArenaAllocator allocator("RaBQEncScale");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         caller_params));
  AlignedByteBuffer wrapper_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(rotated_vector.data(),
                           rotated_centroid.data(),
                           {wrapper_code.data(), wrapper_code.size()}));

  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);
  int64_t scratch_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_encode_scratch_size(caller_params,
                                                        scratch_size));
  AlignedByteBuffer scratch(scratch_size);
  ObQuantizationParams derived_params = caller_params;
  derived_params.inv_sqrt_dim_ = derived_scale;
  AlignedByteBuffer derived_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            ops->encode(derived_params,
                        rotated_vector.data(),
                        rotated_centroid.data(),
                        {scratch.data(), scratch.size()},
                        {derived_code.data(), derived_code.size()}));
  AlignedByteBuffer retained_caller_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            ops->encode(caller_params,
                        rotated_vector.data(),
                        rotated_centroid.data(),
                        {scratch.data(), scratch.size()},
                        {retained_caller_code.data(), retained_caller_code.size()}));
  ASSERT_NE(0,
            memcmp(derived_code.data(),
                   retained_caller_code.data(),
                   derived_code.size()));
  EXPECT_EQ(0,
            memcmp(wrapper_code.data(), derived_code.data(), derived_code.size()));
}

TEST(TestRaBitQQuantizer, ComputerInitOverridesCallerDimensionScale)
{
  const int64_t dim = 9;
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                             -6.0F, 7.0F, -8.0F, 9.0F};
  const float query[dim] = {-1.0F, 0.5F, 2.0F, -3.0F, 1.0F,
                            8.0F, 4.0F, -2.0F, 0.25F};
  const float centroid[dim] = {0.25F, -0.5F, 0.75F, -1.0F, 1.25F,
                               -1.5F, 1.75F, -2.0F, 2.25F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  ObQuantizationParams caller_params =
      make_params(dim, 1, signs.data(), signs.size());
  ObQuantizationParams derived_params = caller_params;
  derived_params.inv_sqrt_dim_ = calc_inv_sqrt_dim(dim);
  caller_params.inv_sqrt_dim_ = derived_params.inv_sqrt_dim_ * 0.25F;
  ASSERT_NE(derived_params.inv_sqrt_dim_, caller_params.inv_sqrt_dim_);
  std::vector<float> rotated_vector;
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(caller_params, vector, rotated_vector));
  ASSERT_EQ(OB_SUCCESS, rotate_input(caller_params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(caller_params, query, rotated_query));

  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);
  int64_t base_code_size = 0;
  int64_t query_code_size = 0;
  int64_t scratch_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_code_size(derived_params, base_code_size));
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_query_code_size(derived_params,
                                                    query_code_size));
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_encode_scratch_size(derived_params,
                                                        scratch_size));
  AlignedByteBuffer scratch(scratch_size);
  AlignedByteBuffer base_code(base_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->encode(derived_params,
                        rotated_vector.data(),
                        rotated_centroid.data(),
                        {scratch.data(), scratch.size()},
                        {base_code.data(), base_code.size()}));
  AlignedByteBuffer query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(derived_params,
                               rotated_query.data(),
                               rotated_centroid.data(),
                               {query_code.data(), query_code.size()}));
  float derived_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(derived_params,
                         {query_code.data(), query_code.size()},
                         {base_code.data(), base_code.size()},
                         derived_value));
  float retained_caller_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(caller_params,
                         {query_code.data(), query_code.size()},
                         {base_code.data(), base_code.size()},
                         retained_caller_value));
  ASSERT_GT(fabsf(derived_value - retained_caller_value), 1e-6F);

  ObArenaAllocator allocator("RaBQCompScale");
  ObQuantizationComputer computer;
  ASSERT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_L2,
                          caller_params,
                          rotated_query.data(),
                          rotated_centroid.data()));
  float wrapper_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            computer.compute({base_code.data(), base_code.size()}, wrapper_value));
  EXPECT_FLOAT_EQ(derived_value, wrapper_value);
}

TEST(TestRaBitQQuantizer, DirectOpsRequireAlignedWorkspacesAndAcceptMisalignedPersistedBaseCode)
{
  const int64_t dim = 9;
  const int8_t bits = 1;
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                             -6.0F, 7.0F, -8.0F, 9.0F};
  const float query[dim] = {-1.0F, 0.5F, 2.0F, -3.0F, 1.0F,
                            8.0F, 4.0F, -2.0F, 0.25F};
  const float centroid[dim] = {};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(
      dim, bits, signs.data(), signs.size());
  std::vector<float> rotated_vector;
  std::vector<float> rotated_query;
  std::vector<float> rotated_centroid;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, vector, rotated_vector));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);

  int64_t base_code_size = 0;
  int64_t query_code_size = 0;
  int64_t scratch_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_code_size(params, base_code_size));
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_encode_scratch_size(params, scratch_size));
  const uint8_t output_sentinel = 0xA5;
  AlignedByteBuffer rejected_base_code(base_code_size, output_sentinel);
  AlignedByteBuffer misaligned_scratch_storage(scratch_size + 1,
                                               output_sentinel);
  uint8_t *misaligned_scratch = misaligned_scratch_storage.data() + 1;
  ASSERT_NE(0U, reinterpret_cast<uintptr_t>(misaligned_scratch) % alignof(float));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->encode(params,
                        rotated_vector.data(),
                        rotated_centroid.data(),
                        {misaligned_scratch, scratch_size},
                        {rejected_base_code.data(), base_code_size}));
  expect_byte_span(misaligned_scratch, scratch_size, output_sentinel);
  expect_byte_span(rejected_base_code.data(), base_code_size, output_sentinel);

  const int64_t scratch_float_count =
      (scratch_size + static_cast<int64_t>(sizeof(float)) - 1) / sizeof(float);
  std::vector<float> aligned_scratch_storage(scratch_float_count + 1);
  float *aligned_scratch_floats = aligned_scratch_storage.data();
  if (0 == reinterpret_cast<uintptr_t>(aligned_scratch_floats)
               % (2 * alignof(float))) {
    ++aligned_scratch_floats;
  }
  uint8_t *aligned_scratch =
      reinterpret_cast<uint8_t *>(aligned_scratch_floats);
  AlignedByteBuffer aligned_base_code(base_code_size);
  ASSERT_EQ(0U, reinterpret_cast<uintptr_t>(aligned_scratch) % alignof(float));
  ASSERT_NE(0U,
            reinterpret_cast<uintptr_t>(aligned_scratch)
            % (2 * alignof(float)));
  ASSERT_GE(reinterpret_cast<uint8_t *>(aligned_scratch_storage.data()
                                        + aligned_scratch_storage.size())
                - aligned_scratch,
            scratch_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->encode(params,
                        rotated_vector.data(),
                        rotated_centroid.data(),
                        {aligned_scratch, scratch_size},
                        {aligned_base_code.data(), base_code_size}));

  memset(aligned_scratch, output_sentinel, scratch_size);
  AlignedByteBuffer misaligned_base_destination_storage(base_code_size + 1,
                                                        output_sentinel);
  uint8_t *misaligned_base_destination =
      misaligned_base_destination_storage.data() + 1;
  ASSERT_NE(0U,
            reinterpret_cast<uintptr_t>(misaligned_base_destination) % alignof(float));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->encode(params,
                        rotated_vector.data(),
                        rotated_centroid.data(),
                        {aligned_scratch, scratch_size},
                        {misaligned_base_destination, base_code_size}));
  expect_byte_span(aligned_scratch, scratch_size, output_sentinel);
  expect_byte_span(misaligned_base_destination,
                   base_code_size,
                   output_sentinel);

  AlignedByteBuffer misaligned_query_storage(query_code_size + 1,
                                             output_sentinel);
  uint8_t *misaligned_query = misaligned_query_storage.data() + 1;
  ASSERT_NE(0U, reinterpret_cast<uintptr_t>(misaligned_query) % alignof(float));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ops->prepare_query(params,
                               rotated_query.data(),
                               rotated_centroid.data(),
                               {misaligned_query, query_code_size}));
  expect_byte_span(misaligned_query, query_code_size, output_sentinel);

  AlignedByteBuffer aligned_query(query_code_size);
  ASSERT_EQ(0U,
            reinterpret_cast<uintptr_t>(aligned_query.data()) % alignof(float));
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               rotated_query.data(),
                               rotated_centroid.data(),
                               {aligned_query.data(), query_code_size}));
  memcpy(misaligned_query, aligned_query.data(), query_code_size);
  expect_compute_rejection(*ops,
                           params,
                           {misaligned_query, query_code_size},
                           {aligned_base_code.data(), base_code_size},
                           OB_INVALID_ARGUMENT);

  AlignedByteBuffer misaligned_base_storage(base_code_size + 1);
  uint8_t *misaligned_base = misaligned_base_storage.data() + 1;
  ASSERT_NE(0U, reinterpret_cast<uintptr_t>(misaligned_base) % alignof(float));
  memcpy(misaligned_base, aligned_base_code.data(), base_code_size);
  float aligned_value = 0.0F;
  float misaligned_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {aligned_query.data(), query_code_size},
                         {aligned_base_code.data(), base_code_size},
                         aligned_value));
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {aligned_query.data(), query_code_size},
                         {misaligned_base, base_code_size},
                         misaligned_value));
  EXPECT_FLOAT_EQ(aligned_value, misaligned_value);
}

TEST(TestRaBitQQuantizer, WrappersRequestAlignedTypedBuffers)
{
  const int64_t dim = 9;
  const int8_t bits = 1;
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                             -6.0F, 7.0F, -8.0F, 9.0F};
  const float query[dim] = {-1.0F, 0.5F, 2.0F, -3.0F, 1.0F,
                            8.0F, 4.0F, -2.0F, 0.25F};
  const float centroid[dim] = {};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(
      dim, bits, signs.data(), signs.size());
  std::vector<float> rotated_vector;
  std::vector<float> rotated_query;
  std::vector<float> rotated_centroid;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, vector, rotated_vector));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  int64_t scratch_size = 0;
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_encode_scratch_size(params, scratch_size));
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));

  MisaligningAllocator allocator;
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  EXPECT_NE(0U, reinterpret_cast<uintptr_t>(allocator.last_pointer()) % 16);
  EXPECT_GT(allocator.last_size(), scratch_size);
  AlignedByteBuffer base_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(rotated_vector.data(),
                           rotated_centroid.data(),
                           {base_code.data(), encoder.code_size()}));

  ObQuantizationComputer computer;
  ASSERT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_L2,
                          params,
                          rotated_query.data(),
                          rotated_centroid.data()));
  EXPECT_NE(0U, reinterpret_cast<uintptr_t>(allocator.last_pointer()) % 16);
  EXPECT_GT(allocator.last_size(), query_code_size);
  float value = 0.0F;
  EXPECT_EQ(OB_SUCCESS,
            computer.compute({base_code.data(), encoder.code_size()}, value));
}

TEST(TestRaBitQQuantizer, PackSignCodesPreservesSignAndLsbLayout)
{
  const float normalized_residual[DIM] = {
      -1.0F, 1.0F, -0.5F, 0.5F, -0.0F, 0.0F, -2.0F, 2.0F, 3.0F};
  const uint8_t expected[] = {0xBA, 0x01};
  const uint8_t canary = 0xA5;
  const int64_t packed_size = packed_bytes(DIM);
  std::vector<uint8_t> guarded_codes(packed_size + 2, canary);
  uint8_t *packed_codes = guarded_codes.data() + 1;
  get_rabitq_simd_ops().pack_sign_codes(normalized_residual, DIM, packed_codes);

  EXPECT_EQ(canary, guarded_codes.front());
  EXPECT_EQ(canary, guarded_codes.back());
  EXPECT_EQ(0, memcmp(expected, packed_codes, sizeof(expected)));
  for (int64_t i = 0; i < DIM; ++i) {
    EXPECT_EQ(normalized_residual[i] < 0.0F ? 0 : 1,
              decode_scalar_code(packed_codes, DIM, i));
  }
  EXPECT_EQ(0, packed_codes[1] & 0xFE);
}

TEST(TestRaBitQQuantizer, DotPackedCodesMatchesScalarReference)
{
  const uint8_t packed_codes[] = {0xAA, 0x01};
  const float query[DIM] = {
      0.1F, -0.2F, 0.3F, -0.4F, 0.5F, -0.6F, 0.7F, -0.8F, 0.9F};
  const float inv_sqrt_dim = calc_inv_sqrt_dim(DIM);
  const float expected = binary_inner_product_reference(
      query, packed_codes, DIM, inv_sqrt_dim);
  EXPECT_FLOAT_EQ(expected,
                  get_rabitq_simd_ops().dot_packed_codes(
                      query, packed_codes, DIM, inv_sqrt_dim));
}

TEST(TestRaBitQQuantizer, RowMajorSimdBatchPreservesScalarAccumulation)
{
  for (const auto target : {common::ObTargetArch::Default, common::ObTargetArch::AVX2,
      common::ObTargetArch::AVX512, common::ObTargetArch::NEON}) {
    const ObRaBitQSimdOps *ops = get_rabitq_simd_ops(target);
    if (nullptr == ops || nullptr == ops->dot_packed_codes_batch) { continue; }
    ops->dot_packed_codes_batch(nullptr, nullptr, 0, 0, 1.0F, nullptr);
    for (const int64_t dim : {0, 1, 7, 8, 9, 15, 16, 17, 63, 65, 768, 1024}) {
      std::vector<float> query(dim + 2, 123.0F);
      for (int64_t d = 0; d < dim; ++d) { query[d + 1] = ((d * 17) % 23 - 11) * 0.125F; }
      const int64_t bytes = (dim + 7) / 8;
      for (const int64_t count : {1, 2, 3, 4, 5, 7, 8, 15, 31, 32, 33}) {
        std::vector<std::vector<uint8_t>> buffers(count, std::vector<uint8_t>(bytes + 2, 0xA5));
        std::vector<const uint8_t *> codes(count);
        for (int64_t row = 0; row < count; ++row) {
          for (int64_t byte = 0; byte < bytes; ++byte) { buffers[row][byte + 1] = (row * 73 + byte * 29) & 255; }
          codes[row] = buffers[row].data() + 1; // Deliberately unaligned, with guard bytes.
        }
        const auto query_before = query;
        const auto codes_before = buffers;
        for (float scale : {1.0F / std::sqrt(float(std::max<int64_t>(dim, 1))), 0.0005F, 0.001F, 0.0011F}) {
          std::vector<float> output(count + 2, 123.0F);
          ops->dot_packed_codes_batch(query.data() + 1, codes.data(), count, dim, scale, output.data() + 1);
          EXPECT_FLOAT_EQ(123.0F, output.front());
          EXPECT_FLOAT_EQ(123.0F, output.back());
          for (int64_t row = 0; row < count; ++row) {
            const float reference = ops->dot_packed_codes(query.data() + 1, codes[row], dim, scale);
            EXPECT_EQ(0, MEMCMP(&reference, &output[row + 1], sizeof(float)))
                << "dim=" << dim << " count=" << count << " row=" << row;
          }
        }
        EXPECT_EQ(query_before, query);
        EXPECT_EQ(codes_before, buffers);
      }
    }
  }
}

TEST(TestRaBitQQuantizer, DotPackedCodesMatchesScalarAcrossSupportedTargets)
{
  const int64_t dimensions[] = {
      0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 960, 1024, 1536};
  const common::ObTargetArch targets[] = {
      common::ObTargetArch::Default,
      common::ObTargetArch::AVX2,
      common::ObTargetArch::AVX512,
      common::ObTargetArch::NEON};
  const int64_t target_count =
      static_cast<int64_t>(sizeof(targets) / sizeof(targets[0]));
  const ObRaBitQSimdOps *target_ops[target_count] = {};
  for (int64_t target_index = 0; target_index < target_count; ++target_index) {
    target_ops[target_index] = get_rabitq_simd_ops(targets[target_index]);
  }
  const ObRaBitQSimdOps *default_ops = target_ops[0];
  ASSERT_NE(nullptr, default_ops);
  ASSERT_NE(nullptr, default_ops->dot_packed_codes);
  for (int64_t target_index = 1; target_index < target_count; ++target_index) {
    const common::ObTargetArch target = targets[target_index];
#if OB_USE_MULTITARGET_CODE
    if (common::is_arch_supported(target)) {
      ASSERT_NE(nullptr, target_ops[target_index]);
    }
#endif
    if (nullptr != target_ops[target_index]) {
      ASSERT_NE(nullptr, target_ops[target_index]->dot_packed_codes);
      EXPECT_NE(default_ops->dot_packed_codes,
                target_ops[target_index]->dot_packed_codes);
    }
  }

  for (int64_t dim_index = 0;
       dim_index < static_cast<int64_t>(sizeof(dimensions) / sizeof(dimensions[0]));
       ++dim_index) {
    const int64_t dim = dimensions[dim_index];
    const int64_t packed_size = packed_bytes(dim);
    std::vector<float> guarded_query(dim + 2, 0.0F);
    guarded_query.front() = 12345.5F;
    guarded_query.back() = -9876.25F;
    for (int64_t i = 0; i < dim; ++i) {
      const int64_t magnitude = (i * 37 + dim * 11) % 41;
      guarded_query[i + 1] = static_cast<float>(magnitude - 20) * 0.125F;
    }
    std::vector<uint8_t> guarded_codes(packed_size + 2, 0);
    guarded_codes.front() = 0xD3;
    guarded_codes.back() = 0x6C;

    const float inv_sqrt_dims[] = {
        0.999e-3F,
        1e-3F,
        1.001e-3F,
        1.0F / sqrtf(static_cast<float>(std::max<int64_t>(dim, 1)))};
    for (int64_t scale_index = 0;
         scale_index < static_cast<int64_t>(sizeof(inv_sqrt_dims) / sizeof(inv_sqrt_dims[0]));
         ++scale_index) {
      const float inv_sqrt_dim = inv_sqrt_dims[scale_index];
      for (int64_t pattern = 0; pattern < 4; ++pattern) {
        for (int64_t byte_index = 0; byte_index < packed_size; ++byte_index) {
          uint8_t value = 0;
          if (1 == pattern) {
            value = 0xFF;
          } else if (2 == pattern) {
            value = 0xAA;
          } else if (3 == pattern) {
            value = static_cast<uint8_t>(
                (byte_index * 73 + dim * 29 + 0x35) & 0xFF);
          }
          guarded_codes[byte_index + 1] = value;
        }
        const float expected = binary_inner_product_reference(
            guarded_query.data() + 1,
            guarded_codes.data() + 1,
            dim,
            inv_sqrt_dim);
        for (int64_t target_index = 0; target_index < target_count; ++target_index) {
          const common::ObTargetArch target = targets[target_index];
          const ObRaBitQSimdOps *ops = target_ops[target_index];
          SCOPED_TRACE(::testing::Message()
                       << "dim=" << dim
                       << ", inv_sqrt_dim=" << inv_sqrt_dim
                       << ", pattern=" << pattern
                       << ", target=" << static_cast<uint32_t>(target));
          if (nullptr == ops) {
            continue;
          }
          const std::vector<float> query_snapshot = guarded_query;
          const std::vector<uint8_t> codes_snapshot = guarded_codes;
          const float actual = ops->dot_packed_codes(
              guarded_query.data() + 1,
              guarded_codes.data() + 1,
              dim,
              inv_sqrt_dim);
          EXPECT_NEAR(expected, actual, simd_tolerance(expected));
          EXPECT_EQ(0,
                    memcmp(query_snapshot.data(),
                           guarded_query.data(),
                           guarded_query.size() * sizeof(float)));
          EXPECT_EQ(0,
                    memcmp(codes_snapshot.data(),
                           guarded_codes.data(),
                           guarded_codes.size() * sizeof(uint8_t)));
        }
      }
    }
  }
}

TEST(TestRaBitQQuantizer, DotPackedCodesUsesStrictScaleThreshold)
{
  const int64_t dim = 2;
  const float query[dim] = {2.0F, 3.0F};
  const uint8_t packed_codes[] = {0x02};

  const float above_threshold = 1.001e-3F;
  const float negative_query[dim] = {1.0F, 0.0F};
  const float positive_query[dim] = {0.0F, 1.0F};
  EXPECT_FLOAT_EQ(-above_threshold,
                  get_rabitq_simd_ops().dot_packed_codes(
                      negative_query, packed_codes, dim, above_threshold));
  EXPECT_FLOAT_EQ(above_threshold,
                  get_rabitq_simd_ops().dot_packed_codes(
                      positive_query, packed_codes, dim, above_threshold));
  EXPECT_FLOAT_EQ(3.0F,
                  get_rabitq_simd_ops().dot_packed_codes(
                      query, packed_codes, dim, 1e-3F));
  EXPECT_FLOAT_EQ(3.0F,
                  get_rabitq_simd_ops().dot_packed_codes(
                      query, packed_codes, dim, 0.999e-3F));
}

TEST(TestRaBitQQuantizer, SimdResolverReturnsStableGenericOps)
{
  const ObRaBitQSimdOps *avx512_ops =
      get_rabitq_simd_ops(common::ObTargetArch::AVX512);
  const ObRaBitQSimdOps *avx2_ops =
      get_rabitq_simd_ops(common::ObTargetArch::AVX2);
  const ObRaBitQSimdOps *neon_ops =
      get_rabitq_simd_ops(common::ObTargetArch::NEON);
  const ObRaBitQSimdOps *default_ops =
      get_rabitq_simd_ops(common::ObTargetArch::Default);
  ASSERT_NE(nullptr, default_ops);
  const ObRaBitQSimdOps *expected_ops = nullptr != avx512_ops
                                           ? avx512_ops
                                           : (nullptr != avx2_ops ? avx2_ops
                                                                 : (nullptr != neon_ops ? neon_ops : default_ops));
  const ObRaBitQSimdOps &first = get_rabitq_simd_ops();
  const ObRaBitQSimdOps &second = get_rabitq_simd_ops();
  EXPECT_EQ(expected_ops, &first);
  EXPECT_EQ(expected_ops, &second);
  EXPECT_EQ(&first, &second);
  ASSERT_NE(nullptr, first.pack_sign_codes);
  ASSERT_NE(nullptr, first.dot_packed_codes);
}

TEST(TestRaBitQQuantizer, EncodesOneBitSigns)
{
  const int64_t dim = 5;
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 0.5F};
  const float centroid[dim] = {};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  ObArenaAllocator allocator("RaBitQEncode");
  ObQuantizationEncoder encoder;
  const ObQuantizationParams params = make_params(
      dim, 1, signs.data(), signs.size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  AlignedByteBuffer code(encoder.code_size(), 0xFF);
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(vector,
                           centroid,
                           {code.data(), encoder.code_size()}));
  EXPECT_EQ(0x15, code[0]);
  EXPECT_EQ(0, code[0] & 0xE0);
  EXPECT_EQ(0, code[1]);
  EXPECT_EQ(0, code[2]);
  EXPECT_EQ(0, code[3]);
  EXPECT_FLOAT_EQ(5.5F, load_float_native(code.data() + 4));
  double expected_dot = 0.0;
  for (int64_t i = 0; i < dim; ++i) {
    expected_dot += fabs(static_cast<float>(vector[i] / 5.5F)) * 0.5;
  }
  EXPECT_NEAR(expected_dot / (sqrt(static_cast<double>(dim)) / 2.0),
              load_float_native(code.data() + 8),
              1e-7);
}

TEST(TestRaBitQQuantizer, MetadataMatchesDecodedCenteredVector)
{
  const int64_t dim = 5;
  const int8_t bits = 1;
  const float vector[dim] = {2.0F, -1.0F, 4.0F, 0.5F, -3.0F};
  const float centroid[dim] = {0.5F, 0.5F, 1.0F, -0.5F, -1.0F};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  ObArenaAllocator allocator("RaBitQMetadata");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         make_params(dim, bits, signs.data(), signs.size())));
  AlignedByteBuffer code(encoder.code_size(), 0xFF);
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(vector,
                           centroid,
                           {code.data(), encoder.code_size()}));

  std::vector<float> normalized_residual;
  float base_residual_norm = 0.0F;
  make_normalized_base_residual(vector,
                                centroid,
                                dim,
                                normalized_residual,
                                base_residual_norm);
  const float correction = static_cast<float>(decoded_objective(
      code.data(), dim, normalized_residual.data()));
  const float tolerance = 1e-5F;
  EXPECT_NEAR(base_residual_norm,
              load_float_native(code.data() + metadata_offset(dim, 0)),
              tolerance);
  EXPECT_NEAR(correction,
              load_float_native(code.data() + metadata_offset(dim, 1)),
              tolerance);
  const float stored_correction =
      load_float_native(code.data() + metadata_offset(dim, 1));
  EXPECT_GE(stored_correction, 1e-6F);
  EXPECT_LE(stored_correction, 1.0F);
  EXPECT_EQ(0, code[0] & 0xE0);
}

TEST(TestRaBitQQuantizer, BaseEncodingDoesNotUseRotatorSigns)
{
  const int64_t dim = 4;
  const int8_t bits = 1;
  const float vector[dim] = {1.0F, 2.0F, 3.0F, 4.0F};
  const float centroid[dim] = {};
  const uint8_t first_signs[4] = {};
  const uint8_t second_signs[4] = {1, 0, 0, 0};
  ObArenaAllocator first_allocator("RaBQSigns1");
  ObArenaAllocator second_allocator("RaBQSigns2");
  ObQuantizationEncoder first_encoder;
  ObQuantizationEncoder second_encoder;
  ASSERT_EQ(OB_SUCCESS,
            first_encoder.init(first_allocator,
                               ObQuantizationType::RABITQ,
                               share::VIDA_L2,
                               make_params(dim, bits, first_signs, sizeof(first_signs))));
  ASSERT_EQ(OB_SUCCESS,
            second_encoder.init(second_allocator,
                                ObQuantizationType::RABITQ,
                                share::VIDA_L2,
                                make_params(dim, bits, second_signs, sizeof(second_signs))));
  ASSERT_EQ(first_encoder.code_size(), second_encoder.code_size());
  AlignedByteBuffer first_code(first_encoder.code_size());
  AlignedByteBuffer second_code(second_encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            first_encoder.encode(vector,
                                 centroid,
                                 {first_code.data(), first_encoder.code_size()}));
  ASSERT_EQ(OB_SUCCESS,
            second_encoder.encode(vector,
                                  centroid,
                                  {second_code.data(), second_encoder.code_size()}));
  EXPECT_EQ(0,
            memcmp(first_code.data(), second_code.data(), first_code.size()));
}

TEST(TestRaBitQQuantizer, EncoderReusesScratchAcrossVectors)
{
  const int64_t dim = 5;
  const int8_t bits = 1;
  const float first_vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F};
  const float second_vector[dim] = {-9.0F, 8.0F, -7.0F, 6.0F, -5.0F};
  const float centroid[dim] = {};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  ObArenaAllocator allocator("RaBitQReuse");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_IP,
                         make_params(dim, bits, signs.data(), signs.size())));
  AlignedByteBuffer first_code(encoder.code_size());
  AlignedByteBuffer second_code(encoder.code_size());
  AlignedByteBuffer repeated_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(first_vector,
                           centroid,
                           {first_code.data(), encoder.code_size()}));
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(second_vector,
                           centroid,
                           {second_code.data(), encoder.code_size()}));
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(first_vector,
                           centroid,
                           {repeated_code.data(), encoder.code_size()}));
  EXPECT_NE(0,
            memcmp(first_code.data(), second_code.data(), first_code.size()));
  EXPECT_EQ(0,
            memcmp(first_code.data(), repeated_code.data(), first_code.size()));
}

TEST(TestRaBitQQuantizer, ZeroResidualUsesCanonicalMetadata)
{
  const int64_t dim = 5;
  const int8_t bits = 1;
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  ObArenaAllocator allocator("RaBitQZero");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         make_params(dim, bits, signs.data(), signs.size())));
  AlignedByteBuffer code(encoder.code_size(), 0xFF);
  ASSERT_EQ(OB_SUCCESS, encoder.encode(vector, vector, {code.data(), encoder.code_size()}));
  for (int64_t i = 0; i < packed_bytes(dim); ++i) {
    EXPECT_EQ(0, code[i]);
  }
  EXPECT_FLOAT_EQ(0.0F,
                  load_float_native(code.data() + metadata_offset(dim, 0)));
  EXPECT_FLOAT_EQ(1.0F,
                  load_float_native(code.data() + metadata_offset(dim, 1)));
  EXPECT_NEAR(euclidean_norm(vector, dim),
              load_float_native(code.data() + metadata_offset(dim, 2)),
              1e-6F);
}

TEST(TestRaBitQQuantizer, EncodeAllowsFloatNormOverflowForZeroResidual)
{
  const int64_t dim = 4;
  const int8_t bits = 1;
  const float magnitude = 2.0e19F;
  const float vector[dim] = {magnitude, 0.0F, 0.0F, 0.0F};
  const float centroid[dim] = {magnitude, 0.0F, 0.0F, 0.0F};
  ASSERT_TRUE(isinf(magnitude * magnitude));
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, bits, signs.data(), signs.size());
  ObArenaAllocator allocator("RaBQOverflow");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  AlignedByteBuffer base_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(vector,
                           centroid,
                           {base_code.data(), encoder.code_size()}));
  EXPECT_FLOAT_EQ(0.0F,
                  load_float_native(base_code.data() + metadata_offset(dim, 0)));
  EXPECT_FLOAT_EQ(1.0F,
                  load_float_native(base_code.data() + metadata_offset(dim, 1)));
  EXPECT_TRUE(isinf(
      load_float_native(base_code.data() + metadata_offset(dim, 2))));
}

TEST(TestRaBitQQuantizer, PrepareQueryDoesNotValidateExternallyRotatedFhtOverflow)
{
  const int64_t dim = 4;
  const int8_t bits = 1;
  const float magnitude = FLT_MAX * 0.60F;
  const float query[dim] = {magnitude, magnitude, 0.0F, 0.0F};
  const float centroid[dim] = {};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, bits, signs.data(), signs.size());
  std::vector<float> rotated_query;
  std::vector<float> rotated_centroid;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer query_code(query_code_size);
  const int64_t residual_norm_offset = dim * sizeof(float);
  const int64_t raw_norm_offset = (dim + 1) * sizeof(float);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               rotated_query.data(),
                               rotated_centroid.data(),
                               {query_code.data(), query_code_size}));
  EXPECT_TRUE(isnan(
      load_float_native(query_code.data() + residual_norm_offset)));
  EXPECT_TRUE(isnan(
      load_float_native(query_code.data() + raw_norm_offset)));
}

TEST(TestRaBitQQuantizer, CosineBaseZeroVectorReturnsMaximumDistance)
{
  const int64_t dim = 5;
  const float query[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F};
  const float vector[dim] = {};
  const float centroid[dim] = {};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  AlignedByteBuffer base_code;
  float actual = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            encode_and_compute(share::VIDA_COS,
                               params,
                               rotated_query.data(),
                               vector,
                               rotated_centroid.data(),
                               base_code,
                               actual));
  EXPECT_FLOAT_EQ(1.0F, actual);
}

TEST(TestRaBitQQuantizer, L2EstimatorMatchesIndependentCodeFormula)
{
  const int64_t dim = 9;
  const float query[dim] = {1.5F, -2.0F, 0.25F, 4.0F, -1.0F,
                            3.0F, 0.75F, -0.5F, 2.25F};
  const float vector[dim] = {-0.5F, 1.25F, 3.5F, -2.0F, 0.75F,
                             1.0F, -4.0F, 2.5F, 0.125F};
  const float centroid[dim] = {0.25F, -0.75F, 1.0F, 0.5F, -1.5F,
                               0.25F, 2.0F, -1.0F, 0.5F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  AlignedByteBuffer base_code;
  float actual = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            encode_and_compute(share::VIDA_L2,
                               params,
                               rotated_query.data(),
                               vector,
                               rotated_centroid.data(),
                               base_code,
                               actual));
  ASSERT_GE(base_code.size(), required_base_code_size(dim));
  EXPECT_NEAR(independent_estimate(share::VIDA_L2,
                                   params,
                                   rotated_query.data(),
                                   rotated_centroid.data(),
                                   nullptr,
                                   0,
                                   base_code.data(),
                                   base_code.size()),
              actual,
              1e-5F);
}

TEST(TestRaBitQQuantizer, InnerProductEstimatorRecoversFromCenteredL2)
{
  const int64_t dim = 9;
  const float query[dim] = {2.0F, -1.0F, 0.5F, 3.0F, -2.5F,
                            1.5F, 0.25F, -0.75F, 4.0F};
  const float vector[dim] = {1.0F, 2.0F, -3.0F, 0.5F, 1.25F,
                             -2.0F, 3.5F, 0.75F, -1.0F};
  const float centroid[dim] = {0.5F, -1.5F, 0.25F, 1.0F, -0.5F,
                               2.0F, -0.75F, 0.5F, 0.25F};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  AlignedByteBuffer base_code;
  AlignedByteBuffer query_code;
  float actual = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            encode_and_compute(share::VIDA_IP,
                               params,
                               rotated_query.data(),
                               vector,
                               rotated_centroid.data(),
                               base_code,
                               actual,
                               &query_code));
  ASSERT_GE(base_code.size(), required_base_code_size(dim));
  const float expected = independent_estimate(share::VIDA_IP,
                                               params,
                                               rotated_query.data(),
                                               rotated_centroid.data(),
                                               query_code.data(),
                                               query_code.size(),
                                               base_code.data(),
                                               base_code.size());
  EXPECT_NEAR(expected, actual, 1e-5F);
}

TEST(TestRaBitQQuantizer, CosineEstimatorRecoversFromCenteredL2AndRawNorms)
{
  const int64_t dim = 9;
  const float query[dim] = {5.0F, -2.0F, 1.0F, 0.5F, -3.0F,
                            2.0F, 4.0F, -1.0F, 0.75F};
  const float vector[dim] = {1.0F, 3.0F, -2.0F, 4.0F, 0.5F,
                             -1.5F, 2.5F, 0.25F, -3.0F};
  const float centroid[dim] = {2.0F, -1.0F, 0.25F, 1.5F, -0.5F,
                               0.75F, 1.0F, -0.25F, 0.5F};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  AlignedByteBuffer base_code;
  AlignedByteBuffer query_code;
  float actual = -1.0F;
  ASSERT_EQ(OB_SUCCESS,
            encode_and_compute(share::VIDA_COS,
                               params,
                               rotated_query.data(),
                               vector,
                               rotated_centroid.data(),
                               base_code,
                               actual,
                               &query_code));
  ASSERT_GE(base_code.size(), required_base_code_size(dim));
  EXPECT_NEAR(independent_estimate(share::VIDA_COS,
                                   params,
                                   rotated_query.data(),
                                   rotated_centroid.data(),
                                   query_code.data(),
                                   query_code.size(),
                                   base_code.data(),
                                   base_code.size()),
              actual,
              1e-5F);
}

TEST(TestRaBitQQuantizer, CosineUsesRawPreRotatedInputsAndRawNormRecovery)
{
  const int64_t dim = 9;
  const float query[dim] = {8.0F, -3.0F, 2.0F, 1.5F, -5.0F,
                            4.0F, 7.0F, -2.0F, 0.5F};
  const float base[dim] = {-6.0F, 5.0F, 1.0F, -4.0F, 3.0F,
                           2.5F, -7.0F, 6.0F, -1.5F};
  const float centroid[dim] = {0.20F, -0.10F, 0.15F, -0.30F, 0.25F,
                               0.10F, -0.20F, 0.05F, 0.35F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_query;
  std::vector<float> rotated_base;
  std::vector<float> rotated_centroid;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, base, rotated_base));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  const std::vector<float> query_before = rotated_query;
  const std::vector<float> base_before = rotated_base;

  std::vector<float> expected_query_residual;
  std::vector<float> expected_base_residual;
  float query_raw_norm = 0.0F;
  float query_residual_norm = 0.0F;
  float base_raw_norm = 0.0F;
  float base_residual_norm = 0.0F;
  query_raw_norm = euclidean_norm(rotated_query.data(), dim);
  base_raw_norm = euclidean_norm(rotated_base.data(), dim);
  ASSERT_EQ(OB_SUCCESS,
            make_normalized_rotated_query_residual(params,
                                                   rotated_query.data(),
                                                   rotated_centroid.data(),
                                                   expected_query_residual,
                                                   query_residual_norm));
  make_normalized_base_residual(rotated_base.data(),
                                rotated_centroid.data(),
                                dim,
                                expected_base_residual,
                                base_residual_norm);
  ASSERT_GT(query_raw_norm, 1.0F);
  ASSERT_GT(base_raw_norm, 1.0F);
  ASSERT_GT(query_residual_norm, 0.0F);
  ASSERT_GT(base_residual_norm, 0.0F);

  std::vector<float> incorrectly_pre_normalized_query = rotated_query;
  for (int64_t i = 0; i < dim; ++i) {
    incorrectly_pre_normalized_query[i] /= query_raw_norm;
  }
  std::vector<float> pre_normalized_residual;
  float pre_normalized_residual_norm = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            make_normalized_rotated_query_residual(params,
                                                   incorrectly_pre_normalized_query.data(),
                                                   rotated_centroid.data(),
                                                   pre_normalized_residual,
                                                   pre_normalized_residual_norm));
  float max_pre_normalized_difference = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    max_pre_normalized_difference = std::max(
        max_pre_normalized_difference,
        fabsf(expected_query_residual[i] - pre_normalized_residual[i]));
  }
  ASSERT_GT(max_pre_normalized_difference, 1e-3F);

  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_COS)];
  ASSERT_NE(nullptr, ops);
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               rotated_query.data(),
                               rotated_centroid.data(),
                               {query_code.data(), query_code.size()}));
  EXPECT_EQ(query_before, rotated_query);
  for (int64_t i = 0; i < dim; ++i) {
    EXPECT_NEAR(expected_query_residual[i],
                load_float_native(query_code.data() + i * sizeof(float)),
                1e-6F);
  }
  EXPECT_NEAR(query_residual_norm,
              load_float_native(query_code.data() + dim * sizeof(float)),
              1e-6F);
  // Raw norms recover COS only after unnormalized residual estimation.
  EXPECT_NEAR(query_raw_norm,
              load_float_native(query_code.data() + (dim + 1) * sizeof(float)),
              1e-6F);
  EXPECT_NE(1.0F,
            load_float_native(query_code.data() + (dim + 1) * sizeof(float)));

  ObArenaAllocator allocator("RaBQCosRaw");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_COS,
                         params));
  AlignedByteBuffer base_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(rotated_base.data(),
                           rotated_centroid.data(),
                           {base_code.data(), base_code.size()}));
  EXPECT_EQ(base_before, rotated_base);
  AlignedByteBuffer expected_base_code(base_code.size());
  pack_sign_codes_reference(expected_base_residual.data(), dim, expected_base_code.data());
  EXPECT_EQ(0,
            memcmp(expected_base_code.data(), base_code.data(), packed_bytes(dim)));
  EXPECT_NEAR(base_residual_norm,
              load_float_native(base_code.data() + metadata_offset(dim, 0)),
              1e-6F);
  EXPECT_NEAR(base_raw_norm,
              load_float_native(base_code.data() + metadata_offset(dim, 2)),
              1e-6F);
  EXPECT_NE(1.0F,
            load_float_native(base_code.data() + metadata_offset(dim, 2)));

  const float correction = load_float_native(base_code.data() + metadata_offset(dim, 1));
  const float normalized_dot_estimate = binary_inner_product_reference(
      expected_query_residual.data(), base_code.data(), dim, params.inv_sqrt_dim_) / correction;
  const float estimated_l2_squared = base_residual_norm * base_residual_norm
                                     + query_residual_norm * query_residual_norm
                                     - 2.0F * base_residual_norm * query_residual_norm
                                       * normalized_dot_estimate;
  const float expected_cosine_distance = 1.0F -
      (query_raw_norm * query_raw_norm + base_raw_norm * base_raw_norm
       - estimated_l2_squared) * 0.5F / (query_raw_norm * base_raw_norm);
  float direct_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {query_code.data(), query_code.size()},
                         {base_code.data(), base_code.size()},
                         direct_value));
  EXPECT_NEAR(expected_cosine_distance, direct_value, 1e-5F);

  ObQuantizationComputer computer;
  ASSERT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_COS,
                          params,
                          rotated_query.data(),
                          rotated_centroid.data()));
  float computer_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            computer.compute({base_code.data(), base_code.size()}, computer_value));
  EXPECT_FLOAT_EQ(direct_value, computer_value);
}

TEST(TestRaBitQQuantizer, CosinePreRotatedInputDoesNotPreNormalize)
{
  const int64_t dim = 2;
  // These are already rotated values.  Normalizing before subtracting the
  // centroid chooses an orthogonal residual direction, so this catches a
  // forbidden COS pre-normalization before VSAG's residual quantization.
  const float raw[dim] = {18.0F, 49.125F};
  const float rotated_centroid[dim] = {0.34404406F, 0.938953459F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  const float raw_norm = euclidean_norm(raw, dim);
  ASSERT_GT(raw_norm, 1.0F);

  auto make_residual = [&](const int normalization_count,
                           std::vector<float> &normalized_residual,
                           float &residual_norm) {
    normalized_residual.assign(raw, raw + dim);
    for (int normalization = 0; normalization < normalization_count; ++normalization) {
      const float norm = euclidean_norm(normalized_residual.data(), dim);
      ASSERT_GT(norm, 0.0F);
      for (int64_t i = 0; i < dim; ++i) {
        normalized_residual[i] /= norm;
      }
    }
    for (int64_t i = 0; i < dim; ++i) {
      normalized_residual[i] -= rotated_centroid[i];
    }
    residual_norm = euclidean_norm(normalized_residual.data(), dim);
    ASSERT_GT(residual_norm, 0.0F);
    for (int64_t i = 0; i < dim; ++i) {
      normalized_residual[i] /= residual_norm;
    }
  };

  std::vector<float> raw_residual;
  std::vector<float> pre_normalized_residual;
  float raw_residual_norm = 0.0F;
  float pre_normalized_residual_norm = 0.0F;
  make_residual(0, raw_residual, raw_residual_norm);
  make_residual(1, pre_normalized_residual, pre_normalized_residual_norm);
  float max_raw_pre_normalized_difference = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    max_raw_pre_normalized_difference = std::max(
        max_raw_pre_normalized_difference,
        fabsf(raw_residual[i] - pre_normalized_residual[i]));
  }
  ASSERT_GT(max_raw_pre_normalized_difference, 0.5F);

  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_COS)];
  ASSERT_NE(nullptr, ops);
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               raw,
                               rotated_centroid,
                               {query_code.data(), query_code.size()}));
  float max_actual_pre_normalized_difference = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    const float actual = load_float_native(query_code.data() + i * sizeof(float));
    EXPECT_NEAR(raw_residual[i], actual, 1e-6F);
    max_actual_pre_normalized_difference = std::max(
        max_actual_pre_normalized_difference,
        fabsf(actual - pre_normalized_residual[i]));
  }
  EXPECT_GT(max_actual_pre_normalized_difference, 0.5F);
  EXPECT_NEAR(raw_residual_norm,
              load_float_native(query_code.data() + dim * sizeof(float)),
              1e-6F);
  EXPECT_NEAR(raw_norm,
              load_float_native(query_code.data() + (dim + 1) * sizeof(float)),
              1e-6F);

  ObArenaAllocator allocator("RaBQCosRaw");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_COS,
                         params));
  AlignedByteBuffer base_code(encoder.code_size());
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(raw,
                           rotated_centroid,
                           {base_code.data(), base_code.size()}));
  AlignedByteBuffer raw_code(base_code.size());
  AlignedByteBuffer pre_normalized_code(base_code.size());
  pack_sign_codes_reference(raw_residual.data(), dim, raw_code.data());
  pack_sign_codes_reference(pre_normalized_residual.data(), dim, pre_normalized_code.data());
  ASSERT_NE(0, memcmp(raw_code.data(), pre_normalized_code.data(), packed_bytes(dim)));
  EXPECT_EQ(0, memcmp(raw_code.data(), base_code.data(), packed_bytes(dim)));
  EXPECT_NEAR(raw_norm,
              load_float_native(base_code.data() + metadata_offset(dim, 2)),
              1e-6F);
}

TEST(TestRaBitQQuantizer, QueryAndBaseUsePreRotatedInputs)
{
  const int64_t dim = 9;
  const float query[dim] = {1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F, 9.0F};
  const float vector[dim] = {-2.0F, 0.5F, 1.0F, -3.0F, 4.0F,
                             2.5F, -1.5F, 0.25F, 3.5F};
  const float centroid[dim] = {0.5F, -0.5F, 0.25F, 1.0F, -1.0F,
                               0.75F, 0.5F, -0.25F, 1.5F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer query_code(query_code_size);
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_IP)];
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                              rotated_query.data(),
                              rotated_centroid.data(),
                              {query_code.data(), static_cast<int64_t>(query_code.size())}));
  std::vector<float> expected_query;
  float query_residual_norm = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            make_normalized_rotated_query_residual(params,
                                                   rotated_query.data(),
                                                   rotated_centroid.data(),
                                                   expected_query,
                                                   query_residual_norm));
  ASSERT_GT(query_residual_norm, 0.0F);
  for (int64_t i = 0; i < dim; ++i) {
    EXPECT_NEAR(expected_query[i],
                load_float_native(query_code.data() + i * sizeof(float)),
                1e-6F);
  }
  EXPECT_NEAR(euclidean_norm(rotated_query.data(), dim),
              load_float_native(query_code.data() + (dim + 1) * sizeof(float)),
              1e-6F);

  ObArenaAllocator allocator("RaBitQSameSigns");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_IP,
                         params));
  AlignedByteBuffer base_code(encoder.code_size());
  std::vector<float> rotated_vector;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, vector, rotated_vector));
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(rotated_vector.data(),
                           rotated_centroid.data(),
                           {base_code.data(), static_cast<int64_t>(base_code.size())}));
  std::vector<float> normalized_base_residual;
  float base_residual_norm = 0.0F;
  make_normalized_base_residual(rotated_vector.data(),
                                rotated_centroid.data(),
                                dim,
                                normalized_base_residual,
                                base_residual_norm);
  double quantized_norm2 = 0.0;
  double dot = 0.0;
  const double center = 0.5;
  for (int64_t i = 0; i < dim; ++i) {
    const double centered = decode_scalar_code(base_code.data(), dim, i) - center;
    quantized_norm2 += centered * centered;
    dot += normalized_base_residual[i] * centered;
  }
  EXPECT_NEAR(dot / sqrt(quantized_norm2),
              load_float_native(base_code.data() + metadata_offset(dim, 1)),
              1e-6F);
}

TEST(TestRaBitQQuantizer, QueryZeroVectorCosineReturnsMaximumDistance)
{
  const int64_t dim = 9;
  const float query[dim] = {};
  const float vector[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                             -6.0F, 7.0F, -8.0F, 9.0F};
  const float centroid[dim] = {};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  AlignedByteBuffer base_code;
  float actual = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            encode_and_compute(share::VIDA_COS,
                               params,
                               rotated_query.data(),
                               vector,
                               rotated_centroid.data(),
                               base_code,
                               actual));
  EXPECT_FLOAT_EQ(1.0F, actual);
}

TEST(TestRaBitQQuantizer, BaseZeroResidualRecoversAllMetrics)
{
  const int64_t dim = 9;
  const float query[dim] = {2.0F, 1.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F};
  const float vector[dim] = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F};
  const float centroid[dim] = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F, 0.0F};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  const share::ObVectorIndexDistAlgorithm metrics[] = {share::VIDA_L2,
                                          share::VIDA_IP,
                                          share::VIDA_COS};
  const float expected_metrics[] = {2.0F, -1.0F, 1.0F - 2.0F / sqrtf(5.0F)};
  for (int64_t i = 0; i < 3; ++i) {
    AlignedByteBuffer base_code;
    AlignedByteBuffer query_code;
    float actual = 0.0F;
    ASSERT_EQ(OB_SUCCESS,
              encode_and_compute(metrics[i],
                                 params,
                                 rotated_query.data(),
                                 vector,
                                 rotated_centroid.data(),
                                 base_code,
                                 actual,
                                 &query_code));
    ASSERT_GE(base_code.size(), required_base_code_size(dim));
    EXPECT_NEAR(expected_metrics[i], actual, 1e-5F);
    EXPECT_NEAR(independent_estimate(metrics[i],
                                     params,
                                     rotated_query.data(),
                                     rotated_centroid.data(),
                                     query_code.data(),
                                     query_code.size(),
                                     base_code.data(),
                                     base_code.size()),
                actual,
                1e-6F);
  }
}

TEST(TestRaBitQQuantizer, L2EstimateIsNotClampedToZero)
{
  const int64_t dim = 9;
  const float query[dim] = {3.0F, -2.0F, 1.0F, 4.0F, -3.0F,
                            2.0F, 0.5F, -1.0F, 5.0F};
  const float centroid[dim] = {0.5F, -1.0F, 0.25F, 1.5F, -0.5F,
                               1.0F, -0.75F, 0.5F, 0.25F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               rotated_query.data(),
                               rotated_centroid.data(),
                               {query_code.data(), query_code.size()}));
  AlignedByteBuffer base_code(required_base_code_size(dim));
  ASSERT_GE(base_code.size(), required_base_code_size(dim));
  pack_sign_codes_reference(reinterpret_cast<const float *>(query_code.data()),
                            dim,
                            base_code.data());
  const float binary_inner_product = binary_inner_product_reference(
      reinterpret_cast<const float *>(query_code.data()),
      base_code.data(),
      dim,
      params.inv_sqrt_dim_);
  ASSERT_GT(binary_inner_product, 0.0F);
  const float query_residual_norm =
      load_float_native(query_code.data() + dim * sizeof(float));
  const float base_residual_norm = query_residual_norm * 0.5F;
  store_float_native(base_code.data() + metadata_offset(dim, 0), base_residual_norm);
  store_float_native(base_code.data() + metadata_offset(dim, 1),
                     binary_inner_product / 1.5F);
  store_float_native(base_code.data() + metadata_offset(dim, 2), 1.0F);
  float helper_query_residual_norm = 0.0F;
  float helper_base_residual_norm = 0.0F;
  const double normalized_dot_estimate = independent_normalized_dot_estimate(
      share::VIDA_L2,
      params,
      rotated_query.data(),
      rotated_centroid.data(),
      base_code.data(),
      base_code.size(),
      helper_query_residual_norm,
      helper_base_residual_norm);
  ASSERT_GT(normalized_dot_estimate, 1.0);
  const float unclamped_l2 = independent_estimate(share::VIDA_L2,
                                                   params,
                                                   rotated_query.data(),
                                                   rotated_centroid.data(),
                                                   nullptr,
                                                   0,
                                                   base_code.data(),
                                                   base_code.size());
  ASSERT_LT(unclamped_l2, 0.0F);
  float actual = -1.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {query_code.data(), query_code.size()},
                         {base_code.data(), base_code.size()},
                         actual));
  EXPECT_NEAR(unclamped_l2, actual, 1e-5F);
}

TEST(TestRaBitQQuantizer, CosineSimilarityIsNotClamped)
{
  const int64_t dim = 9;
  const float query[dim] = {4.0F, 0.0F, 0.0F, 0.0F, 0.0F,
                            0.0F, 0.0F, 0.0F, 0.0F};
  const float centroid[dim] = {1.0F, 1.0F, 0.0F, 0.0F, 0.0F,
                               0.0F, 0.0F, 0.0F, 0.0F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_COS)];
  ASSERT_NE(nullptr, ops);
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                               rotated_query.data(),
                               rotated_centroid.data(),
                               {query_code.data(), query_code.size()}));
  std::vector<float> normalized_residual;
  float query_residual_norm = 0.0F;
  const float query_raw_norm = euclidean_norm(rotated_query.data(), dim);
  ASSERT_EQ(OB_SUCCESS,
            make_normalized_rotated_query_residual(params,
                                                   rotated_query.data(),
                                                   rotated_centroid.data(),
                                                   normalized_residual,
                                                   query_residual_norm));
  ASSERT_GT(query_residual_norm, 0.0F);
  ASSERT_NE(query_raw_norm, query_residual_norm);
  EXPECT_NEAR(query_raw_norm,
              load_float_native(query_code.data() + (dim + 1) * sizeof(float)),
              1e-6F);
  AlignedByteBuffer base_code(required_base_code_size(dim));
  ASSERT_GE(base_code.size(), required_base_code_size(dim));
  pack_sign_codes_reference(normalized_residual.data(), dim, base_code.data());
  const float binary_inner_product = binary_inner_product_reference(
      normalized_residual.data(),
      base_code.data(),
      dim,
      params.inv_sqrt_dim_);
  ASSERT_GT(binary_inner_product, 0.0F);
  const float base_residual_norm = query_residual_norm * 0.5F;
  const float base_raw_norm = query_raw_norm * 2.0F;
  ASSERT_NE(base_raw_norm, base_residual_norm);
  store_float_native(base_code.data() + metadata_offset(dim, 0), base_residual_norm);
  store_float_native(base_code.data() + metadata_offset(dim, 1),
                     binary_inner_product / 1.5F);
  store_float_native(base_code.data() + metadata_offset(dim, 2), base_raw_norm);
  float helper_query_residual_norm = 0.0F;
  float helper_base_residual_norm = 0.0F;
  const double normalized_dot_estimate = independent_normalized_dot_estimate(
      share::VIDA_COS,
      params,
      rotated_query.data(),
      rotated_centroid.data(),
      base_code.data(),
      base_code.size(),
      helper_query_residual_norm,
      helper_base_residual_norm);
  const double unclamped_l2_squared =
      static_cast<double>(helper_query_residual_norm) * helper_query_residual_norm
      + static_cast<double>(helper_base_residual_norm) * helper_base_residual_norm
      - 2.0 * helper_query_residual_norm * helper_base_residual_norm * normalized_dot_estimate;
  ASSERT_GT(normalized_dot_estimate, 1.0);
  ASSERT_LT(unclamped_l2_squared, 0.0);
  const double unclamped_cosine_distance = 1.0 -
      (static_cast<double>(query_raw_norm) * query_raw_norm
       + static_cast<double>(base_raw_norm) * base_raw_norm
       - unclamped_l2_squared) * 0.5 / (query_raw_norm * base_raw_norm);
  ASSERT_LT(unclamped_cosine_distance, 0.0);
  float actual = -1.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {query_code.data(), query_code.size()},
                         {base_code.data(), base_code.size()},
                         actual));
  EXPECT_NEAR(unclamped_cosine_distance, actual, 1e-6F);
}

TEST(TestRaBitQQuantizer, QueryZeroResidualUsesCanonicalRepresentationForAllMetrics)
{
  const int64_t dim = 9;
  const float query[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                            -6.0F, 7.0F, -8.0F, 9.0F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int64_t metric_index = 0; metric_index < 3; ++metric_index) {
    SCOPED_TRACE(::testing::Message() << "metric=" << metrics[metric_index]);
    const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
        static_cast<int64_t>(metrics[metric_index])];
    ASSERT_NE(nullptr, ops);
    AlignedByteBuffer query_code(query_code_size);
    ASSERT_EQ(OB_SUCCESS,
              ops->prepare_query(params,
                                 rotated_query.data(),
                                 rotated_query.data(),
                                 {query_code.data(), query_code.size()}));
    for (int64_t i = 0; i < dim; ++i) {
      EXPECT_FLOAT_EQ(0.0F,
                      load_float_native(query_code.data() + i * sizeof(float)));
    }
    EXPECT_FLOAT_EQ(0.0F,
                    load_float_native(query_code.data() + dim * sizeof(float)));
    EXPECT_NEAR(euclidean_norm(rotated_query.data(), dim),
                load_float_native(query_code.data() + (dim + 1) * sizeof(float)),
                1e-6F);
  }
}

TEST(TestRaBitQQuantizer, InnerProductZeroQueryReturnsMaximumDistance)
{
  const int64_t dim = 9;
  const float query[dim] = {};
  const float vector[dim] = {2.0F, -1.0F, 3.0F, -2.0F, 4.0F,
                             -3.0F, 5.0F, -4.0F, 6.0F};
  const float centroid[dim] = {0.5F, -1.0F, 0.25F, 1.5F, -0.5F,
                               1.0F, -0.75F, 0.5F, 0.25F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  AlignedByteBuffer base_code;
  float actual = -1.0F;
  ASSERT_EQ(OB_SUCCESS,
            encode_and_compute(share::VIDA_IP,
                               params,
                               rotated_query.data(),
                               vector,
                               rotated_centroid.data(),
                               base_code,
                               actual));
  EXPECT_FLOAT_EQ(1.0F, actual);
}

TEST(TestRaBitQQuantizer, InnerProductZeroBaseReturnsMaximumDistance)
{
  const int64_t dim = 9;
  const float query[dim] = {2.0F, -1.0F, 3.0F, -2.0F, 4.0F,
                            -3.0F, 5.0F, -4.0F, 6.0F};
  const float vector[dim] = {};
  const float centroid[dim] = {0.5F, -1.0F, 0.25F, 1.5F, -0.5F,
                               1.0F, -0.75F, 0.5F, 0.25F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  AlignedByteBuffer base_code;
  float actual = -1.0F;
  ASSERT_EQ(OB_SUCCESS,
            encode_and_compute(share::VIDA_IP,
                               params,
                               rotated_query.data(),
                               vector,
                               rotated_centroid.data(),
                               base_code,
                               actual));
  EXPECT_FLOAT_EQ(1.0F, actual);
}

TEST(TestRaBitQQuantizer, RawNormApproxZeroBoundaryUsesFloatThresholdBeforePropagatingL2Overflow)
{
  const int64_t dim = 4;
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  const float huge_base_residual_norm = 2.9e38F;
  const float approx_zero_threshold = 1e-5F;
  const float below_approx_zero_threshold =
      nextafterf(approx_zero_threshold, 0.0F);
  const float metric_sentinel = -123.25F;
  ASSERT_TRUE(isinf(huge_base_residual_norm * huge_base_residual_norm));
  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer query_code(query_code_size);
  AlignedByteBuffer base_code(required_base_code_size(dim));
  store_float_native(query_code.data() + dim * sizeof(float), 1.0F);
  store_float_native(query_code.data() + (dim + 1) * sizeof(float), 0.0F);
  store_float_native(base_code.data() + metadata_offset(dim, 0),
                     huge_base_residual_norm);
  store_float_native(base_code.data() + metadata_offset(dim, 1), 1.0F);
  store_float_native(base_code.data() + metadata_offset(dim, 2), 1.0F);
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_IP, share::VIDA_COS};
  for (int64_t metric_index = 0; metric_index < 2; ++metric_index) {
    SCOPED_TRACE(::testing::Message() << "metric=" << metrics[metric_index]);
    const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
        static_cast<int64_t>(metrics[metric_index])];
    ASSERT_NE(nullptr, ops);
    float metric_value = metric_sentinel;
    EXPECT_EQ(OB_SUCCESS,
              ops->compute(params,
                           {query_code.data(), query_code.size()},
                           {base_code.data(), base_code.size()},
                           metric_value));
    EXPECT_FLOAT_EQ(1.0F, metric_value);
    AlignedByteBuffer below_threshold_query = query_code;
    store_float_native(below_threshold_query.data() + (dim + 1) * sizeof(float),
                       below_approx_zero_threshold);
    metric_value = metric_sentinel;
    EXPECT_EQ(OB_SUCCESS,
              ops->compute(params,
                           {below_threshold_query.data(), below_threshold_query.size()},
                           {base_code.data(), base_code.size()},
                           metric_value));
    EXPECT_FLOAT_EQ(1.0F, metric_value);
    AlignedByteBuffer at_threshold_query = query_code;
    store_float_native(at_threshold_query.data() + (dim + 1) * sizeof(float),
                       approx_zero_threshold);
    metric_value = metric_sentinel;
    ASSERT_EQ(OB_SUCCESS,
              ops->compute(
                  params,
                  {at_threshold_query.data(), at_threshold_query.size()},
                  {base_code.data(), base_code.size()},
                  metric_value));
    EXPECT_TRUE(isnan(metric_value));
    AlignedByteBuffer valid_query = query_code;
    store_float_native(valid_query.data() + (dim + 1) * sizeof(float), 1.0F);
    AlignedByteBuffer below_threshold_base = base_code;
    store_float_native(below_threshold_base.data() + metadata_offset(dim, 2),
                       below_approx_zero_threshold);
    metric_value = metric_sentinel;
    ASSERT_EQ(OB_SUCCESS,
              ops->compute(
                  params,
                  {valid_query.data(), valid_query.size()},
                  {below_threshold_base.data(), below_threshold_base.size()},
                  metric_value));
    EXPECT_FLOAT_EQ(1.0F, metric_value);
    AlignedByteBuffer at_threshold_base = base_code;
    store_float_native(at_threshold_base.data() + metadata_offset(dim, 2),
                       approx_zero_threshold);
    metric_value = metric_sentinel;
    ASSERT_EQ(OB_SUCCESS,
              ops->compute(
                  params,
                  {valid_query.data(), valid_query.size()},
                  {at_threshold_base.data(), at_threshold_base.size()},
                  metric_value));
    EXPECT_TRUE(isnan(metric_value));
  }
  const ObQuantizerMetricOps *l2_ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, l2_ops);
  float metric_value = metric_sentinel;
  ASSERT_EQ(OB_SUCCESS,
            l2_ops->compute(params,
                            {query_code.data(), query_code.size()},
                            {base_code.data(), base_code.size()},
                            metric_value));
  EXPECT_TRUE(isnan(metric_value));
}

TEST(TestRaBitQQuantizer, ComputeRejectsNegativeAndDoesNotValidateNonfiniteRawNormMetadata)
{
  const int64_t dim = 9;
  const float query[dim] = {2.0F, -1.0F, 3.0F, -2.0F, 4.0F,
                            -3.0F, 5.0F, -4.0F, 6.0F};
  const float vector[dim] = {1.0F, 3.0F, -2.0F, 4.0F, 0.5F,
                             -1.5F, 2.5F, 0.25F, -3.0F};
  const float centroid[dim] = {0.5F, -1.0F, 0.25F, 1.5F, -0.5F,
                               0.75F, 1.0F, -0.25F, 0.5F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  const float raw_norms[] = {-1.0F, NAN, INFINITY};
  for (int64_t metric_index = 0; metric_index < 3; ++metric_index) {
    SCOPED_TRACE(::testing::Message() << "metric=" << metrics[metric_index]);
    const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
        static_cast<int64_t>(metrics[metric_index])];
    ASSERT_NE(nullptr, ops);
    int64_t query_code_size = 0;
    ASSERT_EQ(OB_SUCCESS,
              ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
    AlignedByteBuffer query_code(query_code_size);
    ASSERT_EQ(OB_SUCCESS,
              ops->prepare_query(params,
                                 rotated_query.data(),
                                 rotated_centroid.data(),
                                 {query_code.data(), query_code.size()}));
    std::vector<float> rotated_vector;
    ASSERT_EQ(OB_SUCCESS, rotate_input(params, vector, rotated_vector));
    ObArenaAllocator allocator("RaBQBadRaw");
    ObQuantizationEncoder encoder;
    ASSERT_EQ(OB_SUCCESS,
              encoder.init(allocator,
                           ObQuantizationType::RABITQ,
                           metrics[metric_index],
                           params));
    AlignedByteBuffer base_code(required_base_code_size(dim));
    ASSERT_EQ(OB_SUCCESS,
              encoder.encode(rotated_vector.data(),
                             rotated_centroid.data(),
                             {base_code.data(), base_code.size()}));
    ASSERT_GE(base_code.size(), required_base_code_size(dim));
    store_float_native(base_code.data() + metadata_offset(dim, 2),
                       euclidean_norm(rotated_vector.data(), dim));
    store_float_native(query_code.data() + (dim + 1) * sizeof(float),
                       euclidean_norm(rotated_query.data(), dim));
    for (int64_t value_index = 0; value_index < 3; ++value_index) {
      SCOPED_TRACE(::testing::Message()
                   << "case_index=" << value_index
                   << " raw_norm=" << raw_norms[value_index]);
      {
        SCOPED_TRACE(::testing::Message() << "metadata=query_raw_norm");
        AlignedByteBuffer invalid_query = query_code;
        store_float_native(invalid_query.data() + (dim + 1) * sizeof(float),
                           raw_norms[value_index]);
        if (0 == value_index) {
          expect_compute_rejection(*ops,
                                   params,
                                   {invalid_query.data(), invalid_query.size()},
                                   {base_code.data(), base_code.size()},
                                   OB_ERR_UNEXPECTED);
        } else {
          float metric_value = 0.0F;
          EXPECT_EQ(OB_SUCCESS,
                    ops->compute(params,
                                 {invalid_query.data(), invalid_query.size()},
                                 {base_code.data(), base_code.size()},
                                 metric_value));
        }
      }
      {
        SCOPED_TRACE(::testing::Message() << "metadata=base_raw_norm");
        AlignedByteBuffer invalid_base = base_code;
        ASSERT_GE(invalid_base.size(), required_base_code_size(dim));
        store_float_native(invalid_base.data() + metadata_offset(dim, 2),
                           raw_norms[value_index]);
        if (0 == value_index) {
          expect_compute_rejection(*ops,
                                   params,
                                   {query_code.data(), query_code.size()},
                                   {invalid_base.data(), invalid_base.size()},
                                   OB_ERR_UNEXPECTED);
        } else {
          float metric_value = 0.0F;
          EXPECT_EQ(OB_SUCCESS,
                    ops->compute(params,
                                 {query_code.data(), query_code.size()},
                                 {invalid_base.data(), invalid_base.size()},
                                 metric_value));
        }
      }
    }
    if (share::VIDA_IP == metrics[metric_index]
        || share::VIDA_COS == metrics[metric_index]) {
      SCOPED_TRACE(::testing::Message() << "case=zero_raw_norm");
      AlignedByteBuffer zero_query = query_code;
      store_float_native(zero_query.data() + (dim + 1) * sizeof(float), 0.0F);
      float zero_query_value = 0.0F;
      ASSERT_EQ(OB_SUCCESS,
                ops->compute(params,
                             {zero_query.data(), zero_query.size()},
                             {base_code.data(), base_code.size()},
                             zero_query_value));
      EXPECT_FLOAT_EQ(1.0F, zero_query_value);
      AlignedByteBuffer zero_base = base_code;
      ASSERT_GE(zero_base.size(), required_base_code_size(dim));
      store_float_native(zero_base.data() + metadata_offset(dim, 2), 0.0F);
      float zero_base_value = 0.0F;
      ASSERT_EQ(OB_SUCCESS,
                ops->compute(params,
                             {query_code.data(), query_code.size()},
                             {zero_base.data(), zero_base.size()},
                             zero_base_value));
      EXPECT_FLOAT_EQ(1.0F, zero_base_value);
    }
  }
}

TEST(TestRaBitQQuantizer, OneBitProducesFiniteMetricsAtOddDimension)
{
  const int64_t dim = 9;
  const float query[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                            -6.0F, 7.0F, -8.0F, 9.0F};
  const float vector[dim] = {-3.0F, 2.0F, -1.0F, 4.0F, 0.5F,
                             6.0F, -2.5F, 1.5F, 5.0F};
  const float centroid[dim] = {0.1F, -0.2F, 0.3F, -0.4F, 0.5F,
                               -0.6F, 0.7F, -0.8F, 0.9F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const share::ObVectorIndexDistAlgorithm metrics[] = {share::VIDA_L2,
                                          share::VIDA_IP,
                                          share::VIDA_COS};
  const ObQuantizationParams params = make_params(dim, 1, signs.data(), signs.size());
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  for (int64_t metric = 0; metric < 3; ++metric) {
    AlignedByteBuffer base_code;
    AlignedByteBuffer query_code;
    float actual = 0.0F;
    ASSERT_EQ(OB_SUCCESS,
              encode_and_compute(metrics[metric],
                                 params,
                                 rotated_query.data(),
                                 vector,
                                 rotated_centroid.data(),
                                 base_code,
                                 actual,
                                 &query_code));
    ASSERT_GE(base_code.size(), required_base_code_size(dim));
    EXPECT_NEAR(independent_estimate(metrics[metric],
                                     params,
                                     rotated_query.data(),
                                     rotated_centroid.data(),
                                     query_code.data(),
                                     query_code.size(),
                                     base_code.data(),
                                     base_code.size()),
                actual,
                1e-4F);
  }
}

TEST(TestRaBitQQuantizer, ComputerComputeDoesNotDependOnInputsAfterInit)
{
  const int64_t dim = 9;
  const int8_t bits = 1;
  float query[dim] = {0.5F, -1.0F, 1.5F, -2.0F, 2.5F,
                      -3.0F, 3.5F, -4.0F, 4.5F};
  const float vector[dim] = {1.0F, 2.0F, -3.0F, 4.0F, -5.0F,
                             6.0F, -7.0F, 8.0F, 9.0F};
  const float centroid[dim] = {};
  std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(
      dim, bits, signs.data(), signs.size());
  ObArenaAllocator allocator("RaBQQueryOwn");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  AlignedByteBuffer base_code(encoder.code_size());
  std::vector<float> rotated_vector;
  std::vector<float> rotated_query;
  std::vector<float> rotated_centroid;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, vector, rotated_vector));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(rotated_vector.data(),
                           rotated_centroid.data(),
                           {base_code.data(), encoder.code_size()}));
  ObQuantizationComputer computer;
  ASSERT_EQ(OB_SUCCESS,
            computer.init(allocator,
                          ObQuantizationType::RABITQ,
                          share::VIDA_L2,
                          params,
                          rotated_query.data(),
                          rotated_centroid.data()));
  float compute_before = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            computer.compute({base_code.data(), encoder.code_size()},
                             compute_before));

  memset(rotated_query.data(),
         0xA5,
         rotated_query.size() * sizeof(rotated_query[0]));
  memset(signs.data(), 0xFF, signs.size());
  float compute_after = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            computer.compute({base_code.data(), encoder.code_size()},
                             compute_after));
  EXPECT_FLOAT_EQ(compute_before, compute_after);
}

TEST(TestRaBitQQuantizer, OneBitDistanceErrorEnvelopeMatchesFp32AcrossHighDimensions)
{
  struct ErrorScenario
  {
    uint32_t base_seed_;
    uint32_t query_seed_;
    uint32_t sign_seed_;
  };
  const int64_t dimensions[] = {960, 1024, 1536};
  const ErrorScenario scenarios[] = {
      {47, 165, 42},
      {271, 811, 314159}};
  const int64_t base_count = 100;
  const int64_t query_count = 10;
  const float relative_error_limit = 0.05F;
  const float absolute_error_outlier_rate = 0.05F;
  // The FHT error envelope permits 15% relative-error outliers.
  const float relative_error_outlier_rate = 0.15F;
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};

  for (int64_t scenario_index = 0;
       scenario_index < static_cast<int64_t>(sizeof(scenarios) / sizeof(scenarios[0]));
       ++scenario_index) {
    const ErrorScenario &scenario = scenarios[scenario_index];
    for (int64_t dimension_index = 0;
         dimension_index < static_cast<int64_t>(sizeof(dimensions) / sizeof(dimensions[0]));
         ++dimension_index) {
      const int64_t dim = dimensions[dimension_index];
      std::vector<float> bases;
      std::vector<float> queries;
      generate_uniform_vectors(base_count, dim, scenario.base_seed_, bases);
      generate_uniform_vectors(query_count, dim, scenario.query_seed_, queries);
      std::fill(bases.begin(), bases.begin() + dim, 0.0F);
      std::copy(bases.begin() + dim, bases.begin() + 2 * dim, queries.begin());
      std::fill(queries.begin() + query_count / 2 * dim,
                queries.begin() + (query_count / 2 + 1) * dim,
                0.0F);

      std::vector<float> centroid(dim, 0.0F);
      for (int64_t base_index = 0; base_index < base_count; ++base_index) {
        for (int64_t d = 0; d < dim; ++d) {
          centroid[d] += bases[base_index * dim + d];
        }
      }
      for (int64_t d = 0; d < dim; ++d) {
        centroid[d] /= static_cast<float>(base_count);
      }

      const std::vector<uint8_t> signs =
          make_deterministic_signs(dim, scenario.sign_seed_);
      ASSERT_FALSE(signs.empty());
      const ObQuantizationParams params =
          make_params(dim, 1, signs.data(), signs.size());
      std::vector<float> rotated_centroid;
      ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid.data(), rotated_centroid));
      std::vector<float> rotated_bases(base_count * dim);
      for (int64_t base_index = 0; base_index < base_count; ++base_index) {
        std::vector<float> rotated_base;
        ASSERT_EQ(OB_SUCCESS,
                  rotate_input(params, bases.data() + base_index * dim, rotated_base));
        std::copy(rotated_base.begin(),
                  rotated_base.end(),
                  rotated_bases.begin() + base_index * dim);
      }

      for (int64_t metric_index = 0;
           metric_index < static_cast<int64_t>(sizeof(metrics) / sizeof(metrics[0]));
           ++metric_index) {
        const share::ObVectorIndexDistAlgorithm metric = metrics[metric_index];
        SCOPED_TRACE(::testing::Message()
                     << "scenario=" << scenario_index
                     << ", dim=" << dim
                     << ", metric=" << metric);
        ObArenaAllocator allocator("RaBQErrorEnv");
        ObQuantizationEncoder encoder;
        ASSERT_EQ(OB_SUCCESS,
                  encoder.init(allocator, ObQuantizationType::RABITQ, metric, params));
        std::vector<AlignedByteBuffer> base_codes(base_count);
        for (int64_t base_index = 0; base_index < base_count; ++base_index) {
          base_codes[base_index].assign(encoder.code_size(), 0);
          ASSERT_EQ(OB_SUCCESS,
                    encoder.encode(rotated_bases.data() + base_index * dim,
                                   rotated_centroid.data(),
                                   {base_codes[base_index].data(),
                                    base_codes[base_index].size()}));
        }

        int64_t absolute_error_count = 0;
        int64_t relative_error_count = 0;
        const float absolute_error_limit = sqrtf(static_cast<float>(dim));
        for (int64_t query_index = 0; query_index < query_count; ++query_index) {
          std::vector<float> rotated_query;
          ASSERT_EQ(OB_SUCCESS,
                    rotate_input(params,
                                 queries.data() + query_index * dim,
                                 rotated_query));
          ObQuantizationComputer computer;
          ASSERT_EQ(OB_SUCCESS,
                    computer.init(allocator,
                                  ObQuantizationType::RABITQ,
                                  metric,
                                  params,
                                  rotated_query.data(),
                                  rotated_centroid.data()));
          for (int64_t base_index = 0; base_index < base_count; ++base_index) {
            float actual = 0.0F;
            ASSERT_EQ(OB_SUCCESS,
                      computer.compute({base_codes[base_index].data(),
                                        base_codes[base_index].size()},
                                       actual));
            const float expected = fp32_distance_reference(
                metric,
                queries.data() + query_index * dim,
                bases.data() + base_index * dim,
                dim);
            const float error = fabsf(expected - actual);
            if (0 == query_index && 1 == base_index) {
              EXPECT_NEAR(expected,
                          actual,
                          1e-4F * std::max(1.0F, fabsf(expected)));
            }
            if (error > absolute_error_limit) {
              ++absolute_error_count;
            }
            if (error > fabsf(relative_error_limit * expected)) {
              ++relative_error_count;
            }
          }
        }
        const float comparison_count = static_cast<float>(base_count * query_count);
        EXPECT_LE(static_cast<float>(absolute_error_count) / comparison_count,
                  absolute_error_outlier_rate);
        EXPECT_LE(static_cast<float>(relative_error_count) / comparison_count,
                  relative_error_outlier_rate);
      }
    }
  }
}

TEST(TestRaBitQQuantizer, CodeMetadataRejectsInvalidShapeAndNegativeNormsOnly)
{
  const int64_t dim = 9;
  const int8_t bits = 1;
  const float query[dim] = {1.0F, -2.0F, 3.0F, -4.0F, 5.0F,
                            -6.0F, 7.0F, -8.0F, 9.0F};
  const float vector[dim] = {-3.0F, 2.0F, -1.0F, 4.0F, 0.5F,
                             6.0F, -2.5F, 1.5F, 5.0F};
  const float centroid[dim] = {0.1F, -0.2F, 0.3F, -0.4F, 0.5F,
                               -0.6F, 0.7F, -0.8F, 0.9F};
  const std::vector<uint8_t> signs = make_generated_signs(dim);
  const ObQuantizationParams params = make_params(
      dim, bits, signs.data(), signs.size());
  const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
      static_cast<int64_t>(share::VIDA_L2)];
  ASSERT_NE(nullptr, ops);
  std::vector<float> rotated_centroid;
  std::vector<float> rotated_query;
  std::vector<float> rotated_vector;
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid, rotated_centroid));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, query, rotated_query));
  ASSERT_EQ(OB_SUCCESS, rotate_input(params, vector, rotated_vector));

  int64_t query_code_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
  AlignedByteBuffer query_code(query_code_size);
  ASSERT_EQ(OB_SUCCESS,
            ops->prepare_query(params,
                              rotated_query.data(),
                              rotated_centroid.data(),
                              {query_code.data(), static_cast<int64_t>(query_code.size())}));

  ObArenaAllocator allocator("RaBQBadCode");
  ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS,
            encoder.init(allocator,
                         ObQuantizationType::RABITQ,
                         share::VIDA_L2,
                         params));
  AlignedByteBuffer base_code(encoder.code_size());
  const int64_t base_code_size = encoder.code_size();
  ASSERT_EQ(OB_SUCCESS,
            encoder.encode(rotated_vector.data(),
                           rotated_centroid.data(),
                           {base_code.data(), static_cast<int64_t>(base_code.size())}));

  expect_compute_rejection(*ops,
                           params,
                           {query_code.data(), query_code_size - 1},
                           {base_code.data(), base_code_size},
                           OB_BUF_NOT_ENOUGH);
  expect_compute_rejection(*ops,
                           params,
                           {query_code.data(), query_code_size},
                           {base_code.data(), base_code_size - 1},
                           OB_BUF_NOT_ENOUGH);

  const int64_t metadata_offsets[] = {metadata_offset(dim, 0),
                                      metadata_offset(dim, 1),
                                      metadata_offset(dim, 2)};
  for (int64_t i = 0; i < 3; ++i) {
    AlignedByteBuffer malformed = base_code;
    store_float_native(malformed.data() + metadata_offsets[i], NAN);
    float metric_value = 0.0F;
    EXPECT_EQ(OB_SUCCESS,
              ops->compute(params,
                           {query_code.data(), query_code_size},
                           {malformed.data(), base_code_size},
                           metric_value));
  }

  AlignedByteBuffer negative_residual = base_code;
  store_float_native(
      negative_residual.data() + metadata_offset(dim, 0), -1.0F);
  expect_compute_rejection(*ops,
                           params,
                           {query_code.data(), query_code_size},
                           {negative_residual.data(), base_code_size},
                           OB_ERR_UNEXPECTED);

  const float invalid_corrections[] = {NAN, INFINITY, -INFINITY};
  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(invalid_corrections) / sizeof(float));
       ++i) {
    SCOPED_TRACE(::testing::Message() << "correction=" << invalid_corrections[i]);
    AlignedByteBuffer malformed = base_code;
    store_float_native(malformed.data() + metadata_offset(dim, 1),
                       invalid_corrections[i]);
    float metric_value = 0.0F;
    EXPECT_EQ(OB_SUCCESS,
              ops->compute(params,
                           {query_code.data(), query_code_size},
                           {malformed.data(), base_code_size},
                           metric_value));
  }

  AlignedByteBuffer positive_one = base_code;
  store_float_native(positive_one.data() + metadata_offset(dim, 1), 1.0F);
  float positive_one_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {query_code.data(), query_code_size},
                         {positive_one.data(), base_code_size},
                         positive_one_value));
  AlignedByteBuffer small_positive = base_code;
  store_float_native(small_positive.data() + metadata_offset(dim, 1), 1e-6F);
  float small_positive_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {query_code.data(), query_code_size},
                         {small_positive.data(), base_code_size},
                         small_positive_value));
  EXPECT_FLOAT_EQ(positive_one_value, small_positive_value);

  AlignedByteBuffer negative_one = base_code;
  store_float_native(negative_one.data() + metadata_offset(dim, 1), -1.0F);
  float negative_one_value = 0.0F;
  ASSERT_EQ(OB_SUCCESS,
            ops->compute(params,
                         {query_code.data(), query_code_size},
                         {negative_one.data(), base_code_size},
                         negative_one_value));
  const float small_nonpositive_errors[] = {0.0F, -1e-6F};
  for (int64_t i = 0; i < 2; ++i) {
    AlignedByteBuffer small_nonpositive = base_code;
    store_float_native(small_nonpositive.data() + metadata_offset(dim, 1),
                       small_nonpositive_errors[i]);
    float small_nonpositive_value = 0.0F;
    ASSERT_EQ(OB_SUCCESS,
              ops->compute(params,
                           {query_code.data(), query_code_size},
                           {small_nonpositive.data(), base_code_size},
                           small_nonpositive_value));
    EXPECT_FLOAT_EQ(negative_one_value, small_nonpositive_value);
  }
}

TEST(TestRaBitQQuantizer, DimensionAndMetricMatrix)
{
  const int64_t dimensions[] = {7, 8, 9, 32, 33, 48};
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  const float tolerance = 1e-4F;

  for (int64_t dimension_index = 0;
       dimension_index < static_cast<int64_t>(sizeof(dimensions) / sizeof(dimensions[0]));
       ++dimension_index) {
    const int64_t dim = dimensions[dimension_index];
    const std::vector<uint8_t> signs = make_generated_signs(dim);
    ASSERT_FALSE(signs.empty());
    const ObQuantizationParams params = make_params(
        dim, 1, signs.data(), signs.size());

    for (int64_t case_id = 0; case_id < 2; ++case_id) {
      std::vector<float> query(dim);
      std::vector<float> base(dim);
      std::vector<float> centroid(dim);
      for (int64_t d = 0; d < dim; ++d) {
        const int64_t query_term = (d * 17 + case_id * 5) % 23;
        const int64_t base_term = (d * 13 + case_id * 7) % 29;
        const int64_t centroid_term = (d * 7 + case_id * 3) % 11;
        query[d] = static_cast<float>(query_term - 11) * 0.25F
                   + 0.125F * static_cast<float>(case_id + 1);
        base[d] = static_cast<float>(base_term - 14) * 0.20F
                  - 0.075F * static_cast<float>(case_id);
        centroid[d] = static_cast<float>(centroid_term - 5) * 0.05F;
      }

      std::vector<float> rotated_centroid;
      std::vector<float> rotated_query;
      std::vector<float> rotated_base;
      ASSERT_EQ(OB_SUCCESS, rotate_input(params, centroid.data(), rotated_centroid));
      ASSERT_EQ(OB_SUCCESS, rotate_input(params, query.data(), rotated_query));
      ASSERT_EQ(OB_SUCCESS, rotate_input(params, base.data(), rotated_base));

      for (int64_t metric_index = 0;
           metric_index < static_cast<int64_t>(sizeof(metrics) / sizeof(metrics[0]));
           ++metric_index) {
        const share::ObVectorIndexDistAlgorithm metric = metrics[metric_index];
        SCOPED_TRACE(::testing::Message() << "dim=" << dim
                                          << ", case_id=" << case_id
                                          << ", metric=" << metric);
        const ObQuantizerMetricOps *ops = ObRaBitQQuantizer::get_ops().metric_ops[
            static_cast<int64_t>(metric)];
        ASSERT_NE(nullptr, ops);

        int64_t query_code_size = 0;
        ASSERT_EQ(OB_SUCCESS,
                  ObRaBitQQuantizer::calc_query_code_size(params, query_code_size));
        ObArenaAllocator allocator("RaBQMetric");
        ObQuantizationEncoder encoder;
        ASSERT_EQ(OB_SUCCESS,
                  encoder.init(allocator, ObQuantizationType::RABITQ, metric, params));
        AlignedByteBuffer base_code(encoder.code_size());
        AlignedByteBuffer repeated_base_code(encoder.code_size());
        ASSERT_EQ(OB_SUCCESS,
                  encoder.encode(rotated_base.data(),
                                 rotated_centroid.data(),
                                 {base_code.data(), base_code.size()}));
        ASSERT_EQ(OB_SUCCESS,
                  encoder.encode(rotated_base.data(),
                                 rotated_centroid.data(),
                                 {repeated_base_code.data(), repeated_base_code.size()}));
        EXPECT_EQ(0,
                  memcmp(base_code.data(),
                         repeated_base_code.data(),
                         base_code.size()));

        AlignedByteBuffer query_code(query_code_size);
        ASSERT_EQ(OB_SUCCESS,
                  ops->prepare_query(params,
                                     rotated_query.data(),
                                     rotated_centroid.data(),
                                     {query_code.data(), query_code.size()}));
        float direct_value = 0.0F;
        ASSERT_EQ(OB_SUCCESS,
                  ops->compute(params,
                               {query_code.data(), query_code.size()},
                               {base_code.data(), base_code.size()},
                               direct_value));

        ObQuantizationComputer computer;
        ASSERT_EQ(OB_SUCCESS,
                  computer.init(allocator,
                                ObQuantizationType::RABITQ,
                                metric,
                                params,
                                rotated_query.data(),
                                rotated_centroid.data()));
        float wrapper_value = 0.0F;
        ASSERT_EQ(OB_SUCCESS,
                  computer.compute({base_code.data(), base_code.size()}, wrapper_value));
        const float expected_value = independent_estimate(metric,
                                                           params,
                                                           rotated_query.data(),
                                                           rotated_centroid.data(),
                                                           query_code.data(),
                                                           query_code.size(),
                                                           base_code.data(),
                                                           base_code.size());
        EXPECT_EQ(direct_value, wrapper_value);
        EXPECT_NEAR(expected_value, direct_value, tolerance);
        EXPECT_NEAR(expected_value, wrapper_value, tolerance);
      }
    }

    std::vector<float> nonzero_vector(dim);
    std::vector<float> zero_vector(dim, 0.0F);
    for (int64_t d = 0; d < dim; ++d) {
      const int64_t value_term = (d * 17) % 23;
      nonzero_vector[d] = static_cast<float>(value_term - 11) * 0.25F + 0.125F;
    }
    std::vector<float> rotated_zero_centroid;
    std::vector<float> rotated_nonzero_query;
    ASSERT_EQ(OB_SUCCESS,
              rotate_input(params, zero_vector.data(), rotated_zero_centroid));
    ASSERT_EQ(OB_SUCCESS,
              rotate_input(params, nonzero_vector.data(), rotated_nonzero_query));
    for (int64_t zero_case = 0; zero_case < 2; ++zero_case) {
      for (int64_t metric_index = 1;
           metric_index < static_cast<int64_t>(sizeof(metrics) / sizeof(metrics[0]));
           ++metric_index) {
        const share::ObVectorIndexDistAlgorithm metric = metrics[metric_index];
        SCOPED_TRACE(::testing::Message() << "dim=" << dim
                                          << ", case="
                                          << (zero_case == 0 ? "zero_query" : "zero_base")
                                          << ", metric=" << metric);
        AlignedByteBuffer base_code;
        float value = 0.0F;
        const float *rotated_query =
            zero_case == 0 ? rotated_zero_centroid.data() : rotated_nonzero_query.data();
        const float *base = zero_case == 0 ? nonzero_vector.data() : zero_vector.data();
        ASSERT_EQ(OB_SUCCESS,
                  encode_and_compute(metric,
                                     params,
                                     rotated_query,
                                     base,
                                     rotated_zero_centroid.data(),
                                     base_code,
                                     value));
        EXPECT_FLOAT_EQ(1.0F, value);
      }
    }
  }
}

} // namespace quantization
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::quantization::init_arches_quietly();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
