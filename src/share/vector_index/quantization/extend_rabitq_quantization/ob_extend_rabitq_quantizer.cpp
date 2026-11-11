/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/quantization/extend_rabitq_quantization/ob_extend_rabitq_quantizer.h"

#include <cmath>
#include <stdint.h>
#include <string.h>

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "share/vector_index/quantization/extend_rabitq_quantization/ob_extend_rabitq_simd.h"

namespace oceanbase
{
namespace quantization
{
namespace
{

constexpr int8_t MIN_EXTEND_RABITQ_BITS = 2;
constexpr int8_t MAX_EXTEND_RABITQ_BITS = 9;
constexpr int64_t SMALL_RADIX_EVENT_LIMIT = 1024;
constexpr int64_t SMALL_RADIX_BUCKET_COUNT = 4 * 256;
constexpr int64_t LARGE_RADIX_BUCKET_COUNT = 2048 + 2048 + 1024;

constexpr float QUANTIZATION_EPS = 1e-5F;
constexpr float TIGHT_START[9] = {
    0.0F, 0.15F, 0.20F, 0.52F, 0.59F, 0.71F, 0.75F, 0.77F, 0.81F};

static bool is_valid_bits(const int8_t bits)
{
  return bits >= MIN_EXTEND_RABITQ_BITS
         && bits <= MAX_EXTEND_RABITQ_BITS;
}

static bool is_aligned(const void *pointer, const int64_t alignment)
{
  return nullptr != pointer
         && 0 == reinterpret_cast<uintptr_t>(pointer) % alignment;
}

} // namespace

// base_code 保存基础向量的最终 Extended RaBitQ 编码，内部布局如下，所有 offset 的单位均为字节。
// 编码端输入的 x 和 c 已使用查询侧的同一 rotator 预旋转；正交旋转保持模长不变。
// 三种 metric 均量化同一残差 r_x = R(x) - R(c)：
//
// base_code.data_
//   + 0                     -> uint8_t packed_msb[ceil(dim / 8)]：每个维度占 1 bit
//   + extra_code_offset_    -> uint8_t packed_extra[align_up(dim, 64) / 8 * (bits - 1)]：每维 extra bits
//   + residual_norm_offset_ -> float base_residual_norm：残差 r_x = x - c 的模长
//   + raw_norm_offset_      -> float base_raw_norm：旋转后原始基础向量 x 的模长
//   + nbits_rescale_offset_ -> float nbits_rescale：量化方向相关性的校正系数
//   + size_                 -> 基础向量编码缓冲区末尾
//
// packed extra 之后填充到 float 对齐，再依次保存三个浮点元数据。
// NOTE: The layout calculate functions below are used on hot paths and
// intentionally do not check arithmetic overflow. They retain only lightweight
// dim/bits contract checks; callers must validate the parameters during
// initialization and ensure every computed byte size fits in int64_t before
// invoking encode/prepare_query/compute.
struct ObExtendRaBitQQuantizer::BaseCodeLayout
{
  int64_t msb_code_size_;
  int64_t extra_code_offset_;
  int64_t extra_padded_dim_;
  int64_t extra_code_size_;
  int64_t residual_norm_offset_;
  int64_t raw_norm_offset_;
  int64_t nbits_rescale_offset_;
  int64_t size_;

  static int calculate(const int64_t dim,
                       const int8_t bits,
                       BaseCodeLayout &layout)
  {
    int ret = OB_SUCCESS;
    BaseCodeLayout result = {};
    if (OB_UNLIKELY(dim <= 0
                    || dim > static_cast<int64_t>(UINT32_MAX)
                    || !is_valid_bits(bits))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Extend RaBitQ base code layout arguments",
               K(ret), K(dim), K(bits));
    } else {
      result.msb_code_size_ = dim / 8 + (dim % 8 == 0 ? 0 : 1);
      result.extra_code_offset_ = result.msb_code_size_;
      result.extra_padded_dim_ = (dim + 63) / 64 * 64;
      result.extra_code_size_ =
          result.extra_padded_dim_ / 8 * static_cast<int64_t>(bits - 1);
      const int64_t metadata_end =
          result.extra_code_offset_ + result.extra_code_size_;
      result.residual_norm_offset_ =
          (metadata_end + sizeof(float) - 1) / sizeof(float) * sizeof(float);
      result.raw_norm_offset_ = result.residual_norm_offset_ + sizeof(float);
      result.nbits_rescale_offset_ = result.raw_norm_offset_ + sizeof(float);
      result.size_ = result.nbits_rescale_offset_ + sizeof(float);
    }
    if (OB_SUCC(ret)) {
      layout = result;
    }
    return ret;
  }
};

// query_code 是一次查询的临时预处理结果，内部布局如下，所有 offset 的单位均为字节。
// prepare_query 接收已旋转的查询 q_rot = R(q) 和 centroid c_rot = R(c)；
// 查询端和 base 端均使用旋转坐标中的残差 r = R(vector) - R(centroid)：
//
// query_code.data_
//   + 0                           -> float normalized_residual[dim]：归一化后的旋转坐标残差
//   + residual_norm_offset_       -> float residual_norm：r_q 的模长
//   + raw_norm_offset_            -> float raw_norm：旋转查询向量 q_rot 的模长
//   + sumq_offset_                -> float sumq：归一化残差各维之和，供所有 base 复用
//   + size_                       -> 查询编码缓冲区末尾
//
// 各区域按 float 对齐，prepare_query 生成该布局，后续 compute 直接复用其中的数据。
struct ObExtendRaBitQQuantizer::QueryCodeLayout
{
  int64_t residual_norm_offset_;
  int64_t raw_norm_offset_;
  int64_t sumq_offset_;
  int64_t size_;

  static int calculate(const int64_t dim, QueryCodeLayout &layout)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout result = {};
    if (OB_UNLIKELY(dim <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Extend RaBitQ query dimension", K(ret), K(dim));
    } else {
      result.residual_norm_offset_ =
          dim * static_cast<int64_t>(sizeof(float));
      result.raw_norm_offset_ = result.residual_norm_offset_ + sizeof(float);
      result.sumq_offset_ = result.raw_norm_offset_ + sizeof(float);
      result.size_ = result.sumq_offset_ + sizeof(float);
    }
    if (OB_SUCC(ret)) {
      layout = result;
    }
    return ret;
  }
};

// encode_scratch 是编码单个 base 向量时使用的临时工作区，内部布局如下，所有 offset 的单位均为字节。
//
// encode_scratch.data_
//   + normalized_residual_offset_ -> float normalized_residual[dim]：归一化后的旋转坐标残差，起始 offset 为 0，供搜索 best_t 和打包编码使用
//   + absolute_residual_offset_   -> float absolute_residual[dim]：缓存归一化残差的绝对值，供生成事件和更新评分使用
//   + current_magnitude_offset_   -> uint8_t current_magnitudes[dim]：各维在 t_start 处的量化幅值，扫描事件时逐次递增
//   + event_offset_               -> uint64_t events[event_capacity_]：幅值递增事件，高 32 位保存 float 阈值的位表示，低 32 位保存维度下标
//   + radix_scratch_offset_       -> uint64_t radix_scratch[event_capacity_]：基数排序的中转数组，与 events 交替作为每轮排序的输出
//   + radix_histogram_offset_     -> int64_t histogram[]：生成事件时或单独遍历时统计排序桶，容量按事件上界选择 1024 或 5120
//   + size_                       -> 编码临时工作区末尾
//
// current_magnitudes 之后填充到 uint64_t 对齐；工作区首地址也须满足 uint64_t 对齐。
// event_capacity_ = dim * ((1LL << (bits - 1)) - 1)，按每维所有可能的幅值递增次数预留容量；
// 实际事件数由搜索区间决定，可以小于该容量。工作区内容不写入最终 base_code，可在后续编码时复用。
struct ObExtendRaBitQQuantizer::EncodeScratchLayout
{
  int64_t normalized_residual_offset_;
  int64_t absolute_residual_offset_;
  int64_t current_magnitude_offset_;
  int64_t event_offset_;
  int64_t radix_scratch_offset_;
  int64_t radix_histogram_offset_;
  int64_t event_capacity_;
  int64_t size_;

  static int calculate(const int64_t dim,
                       const int8_t bits,
                       EncodeScratchLayout &layout)
  {
    int ret = OB_SUCCESS;
    EncodeScratchLayout result = {};
    if (OB_UNLIKELY(dim <= 0
                    || dim > static_cast<int64_t>(UINT32_MAX)
                    || !is_valid_bits(bits))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Extend RaBitQ scratch arguments",
               K(ret), K(dim), K(bits));
    } else {
      const int64_t normalized_size =
          dim * static_cast<int64_t>(sizeof(float));
      const int64_t magnitude_size =
          dim * static_cast<int64_t>(sizeof(uint8_t));
      const int64_t event_alignment = alignof(uint64_t);
      const int64_t max_magnitude = (1LL << (bits - 1)) - 1;
      result.normalized_residual_offset_ = 0;
      result.absolute_residual_offset_ = normalized_size;
      result.current_magnitude_offset_ =
          result.absolute_residual_offset_ + normalized_size;
      result.event_offset_ =
          (result.current_magnitude_offset_ + magnitude_size
           + event_alignment - 1) / event_alignment
          * event_alignment;
      result.event_capacity_ = dim * max_magnitude;
      result.radix_scratch_offset_ =
          result.event_offset_
          + result.event_capacity_ * static_cast<int64_t>(sizeof(uint64_t));
      result.radix_histogram_offset_ =
          result.radix_scratch_offset_
          + result.event_capacity_ * static_cast<int64_t>(sizeof(uint64_t));
      const int64_t histogram_count = result.event_capacity_ <= SMALL_RADIX_EVENT_LIMIT
          ? SMALL_RADIX_BUCKET_COUNT : LARGE_RADIX_BUCKET_COUNT;
      result.size_ = result.radix_histogram_offset_
          + histogram_count * static_cast<int64_t>(sizeof(int64_t));
    }
    if (OB_SUCC(ret)) {
      layout = result;
    }
    return ret;
  }
};

template <share::ObVectorIndexDistAlgorithm METRIC>
struct ObExtendRaBitQQuantizer::MetricImpl
{
  static int encode(const ObQuantizationParams &params,
                    const float *vector,
                    const float *centroid,
                    const ObQuantizationBuffer &encode_scratch,
                    const ObQuantizationBuffer &base_code)
  {
    int ret = OB_SUCCESS;
    BaseCodeLayout base_layout = {};
    EncodeScratchLayout scratch_layout = {};
    if (OB_FAIL(BaseCodeLayout::calculate(params.dim_, params.bits_, base_layout))) {
      LOG_WARN("failed to calculate Extend RaBitQ base layout",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_FAIL(EncodeScratchLayout::calculate(
                           params.dim_, params.bits_, scratch_layout))) {
      LOG_WARN("failed to calculate Extend RaBitQ scratch layout",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_UNLIKELY(nullptr == vector || nullptr == centroid
                           || nullptr == encode_scratch.data_
                           || nullptr == base_code.data_)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null Extend RaBitQ encode argument",
               K(ret), KP(vector), KP(centroid), KP(encode_scratch.data_),
               KP(base_code.data_));
    } else if (OB_UNLIKELY(encode_scratch.size_ < scratch_layout.size_
                           || base_code.size_ < base_layout.size_)) {
      ret = common::OB_BUF_NOT_ENOUGH;
      LOG_WARN("Extend RaBitQ encode buffer is too small",
               K(ret), K(encode_scratch.size_), K(scratch_layout.size_),
               K(base_code.size_), K(base_layout.size_));
    } else if (OB_UNLIKELY(!is_aligned(encode_scratch.data_,
                                       alignof(uint64_t)))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("Extend RaBitQ encode scratch is not event-aligned",
               K(ret), KP(encode_scratch.data_));
    } else {
      MEMSET(base_code.data_, 0, base_layout.size_);
      float *normalized_residual = reinterpret_cast<float *>(
          encode_scratch.data_ + scratch_layout.normalized_residual_offset_);
      float *absolute_residual = reinterpret_cast<float *>(
          encode_scratch.data_ + scratch_layout.absolute_residual_offset_);
      uint8_t *current_magnitudes =
          encode_scratch.data_ + scratch_layout.current_magnitude_offset_;
      uint64_t *events = reinterpret_cast<uint64_t *>(
          encode_scratch.data_ + scratch_layout.event_offset_);
      uint64_t *radix_scratch = reinterpret_cast<uint64_t *>(
          encode_scratch.data_ + scratch_layout.radix_scratch_offset_);
      int64_t *radix_histogram = reinterpret_cast<int64_t *>(
          encode_scratch.data_ + scratch_layout.radix_histogram_offset_);
      const float raw_norm = calc_vector_norm(params.dim_, vector);
      float residual_norm = 0.0F;
      prepare_normalized_residual(params.dim_,
                                  vector,
                                  centroid,
                                  normalized_residual,
                                  residual_norm);
      if (OB_UNLIKELY(!std::isfinite(raw_norm)
                      || !std::isfinite(residual_norm))) {
        ret = common::OB_INVALID_ARGUMENT;
        LOG_WARN("invalid non-finite Extend RaBitQ vector",
                 K(ret), K(raw_norm), K(residual_norm));
      } else if (0.0F == residual_norm) {
        store_base_metadata(base_code.data_,
                            base_layout,
                            0.0F,
                            raw_norm,
                            1.0F);
      } else {
        float best_t = 0.0F;
        if (OB_FAIL(find_best_t(params.dim_,
                                params.bits_ - 1,
                                normalized_residual,
                                absolute_residual,
                                current_magnitudes,
                                events,
                                radix_scratch,
                                radix_histogram,
                                scratch_layout.event_capacity_,
                                best_t))) {
          LOG_WARN("failed to quantize Extend RaBitQ magnitudes",
                   K(ret), K(params.dim_), K(params.bits_));
        } else {
          float correction = 0.0F;
          get_extend_rabitq_simd_ops().pack_codes(
              normalized_residual,
              params.dim_,
              params.bits_ - 1,
              best_t,
              base_code.data_,
              base_code.data_ + base_layout.extra_code_offset_,
              correction);
          const float nbits_rescale = 1.0F / correction;
          if (OB_UNLIKELY(!std::isfinite(correction) || correction <= 0.0F
                          || !std::isfinite(nbits_rescale)
                          || nbits_rescale <= 0.0F)) {
            ret = common::OB_ERR_UNEXPECTED;
            LOG_WARN("invalid Extend RaBitQ rescale",
                     K(ret), K(correction), K(nbits_rescale));
          } else {
            store_base_metadata(base_code.data_,
                                base_layout,
                                residual_norm,
                                raw_norm,
                                nbits_rescale);
          }
        }
      }
    }
    return ret;
  }

  static int prepare_query(const ObQuantizationParams &params,
                           const float *query,
                           const float *centroid,
                           const ObQuantizationBuffer &query_code)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout layout = {};
    if (OB_UNLIKELY(!is_valid_bits(params.bits_))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Extend RaBitQ query bits", K(ret), K(params.bits_));
    } else if (OB_FAIL(QueryCodeLayout::calculate(params.dim_, layout))) {
      LOG_WARN("failed to calculate Extend RaBitQ query layout",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_UNLIKELY(nullptr == query || nullptr == centroid
                           || nullptr == query_code.data_)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null Extend RaBitQ query argument",
               K(ret), KP(query), KP(centroid), KP(query_code.data_));
    } else if (OB_UNLIKELY(query_code.size_ < layout.size_)) {
      ret = common::OB_BUF_NOT_ENOUGH;
      LOG_WARN("Extend RaBitQ query buffer is too small",
               K(ret), K(query_code.size_), K(layout.size_));
    } else if (OB_UNLIKELY(!is_aligned(query_code.data_, alignof(float)))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("Extend RaBitQ query buffer is not float-aligned",
               K(ret), KP(query_code.data_));
    } else {
      float *normalized_residual = reinterpret_cast<float *>(query_code.data_);
      const float raw_norm = calc_vector_norm(params.dim_, query);
      float residual_norm = 0.0F;
      prepare_normalized_residual(params.dim_,
                                  query,
                                  centroid,
                                  normalized_residual,
                                  residual_norm);
      float sumq = 0.0F;
      for (int64_t i = 0; i < params.dim_; ++i) {
        sumq += normalized_residual[i];
      }
      if (OB_UNLIKELY(!std::isfinite(raw_norm)
                      || !std::isfinite(residual_norm)
                      || !std::isfinite(sumq))) {
        ret = common::OB_INVALID_ARGUMENT;
        LOG_WARN("invalid non-finite Extend RaBitQ query",
                 K(ret), K(raw_norm), K(residual_norm), K(sumq));
      } else {
        MEMCPY(query_code.data_ + layout.residual_norm_offset_,
               &residual_norm,
               sizeof(residual_norm));
        MEMCPY(query_code.data_ + layout.raw_norm_offset_,
               &raw_norm,
               sizeof(raw_norm));
        MEMCPY(query_code.data_ + layout.sumq_offset_,
               &sumq,
               sizeof(sumq));
      }
    }
    return ret;
  }

  static int compute(const ObQuantizationParams &params,
                     const ObQuantizationBuffer &query_code,
                     const ObQuantizationBuffer &base_code,
                     float &metric_value)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout query_layout = {};
    BaseCodeLayout base_layout = {};
    if (OB_FAIL(validate_compute(params,
                                 query_code,
                                 base_code,
                                 query_layout,
                                 base_layout))) {
      LOG_WARN("invalid Extend RaBitQ compute input",
               K(ret), K(params.dim_), K(params.bits_));
    } else {
      const float *normalized_query =
          reinterpret_cast<const float *>(query_code.data_);
      const float ip_msb = get_extend_rabitq_simd_ops().dot_msb_codes(
          normalized_query, base_code.data_, params.dim_);
      if (OB_UNLIKELY(!std::isfinite(ip_msb))) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("invalid Extend RaBitQ computed MSB inner product",
                 K(ret), K(ip_msb), K(params.dim_), K(params.bits_));
      } else if (OB_FAIL(compute_xbits_impl(params,
                                           query_code.data_,
                                           base_code.data_,
                                           query_layout,
                                           base_layout,
                                           ip_msb,
                                           metric_value))) {
        LOG_WARN("failed to compute Extend RaBitQ metric",
                 K(ret), K(params.dim_), K(params.bits_));
      }
    }
    return ret;
  }

  static int compute_xbits(const ObQuantizationParams &params,
                           const ObQuantizationBuffer &query_code,
                           const ObQuantizationBuffer &base_code,
                           const float ip_msb,
                           float &metric_value)
  {
    int ret = OB_SUCCESS;
    QueryCodeLayout query_layout = {};
    BaseCodeLayout base_layout = {};
    if (OB_UNLIKELY(!std::isfinite(ip_msb))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Extend RaBitQ MSB inner product", K(ret), K(ip_msb));
    } else if (OB_FAIL(validate_compute(params,
                                        query_code,
                                        base_code,
                                        query_layout,
                                        base_layout))) {
      LOG_WARN("invalid Extend RaBitQ XBits compute input",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_FAIL(compute_xbits_impl(params,
                                          query_code.data_,
                                          base_code.data_,
                                          query_layout,
                                          base_layout,
                                          ip_msb,
                                          metric_value))) {
      LOG_WARN("failed to compute Extend RaBitQ XBits metric",
               K(ret), K(params.dim_), K(params.bits_));
    }
    return ret;
  }

private:
  static float calc_vector_norm(const int64_t dim, const float *vector)
  {
    float squared_norm = 0.0F;
    for (int64_t i = 0; i < dim; ++i) {
      squared_norm += vector[i] * vector[i];
    }
    return std::sqrt(squared_norm);
  }

  static void prepare_normalized_residual(const int64_t dim,
                                          const float *vector,
                                          const float *centroid,
                                          float *normalized_residual,
                                          float &norm)
  {
    for (int64_t i = 0; i < dim; ++i) {
      normalized_residual[i] = vector[i] - centroid[i];
    }
    norm = calc_vector_norm(dim, normalized_residual);
    if (norm > 0.0F && std::isfinite(norm)) {
      for (int64_t i = 0; i < dim; ++i) {
        normalized_residual[i] /= norm;
      }
    }
  }

  static void store_base_metadata(uint8_t *base_code,
                                  const BaseCodeLayout &layout,
                                  const float residual_norm,
                                  const float raw_norm,
                                  const float nbits_rescale)
  {
    MEMCPY(base_code + layout.residual_norm_offset_,
           &residual_norm,
           sizeof(residual_norm));
    MEMCPY(base_code + layout.raw_norm_offset_,
           &raw_norm,
           sizeof(raw_norm));
    MEMCPY(base_code + layout.nbits_rescale_offset_,
           &nbits_rescale,
           sizeof(nbits_rescale));
  }

  static uint32_t float_bits(const float value)
  {
    uint32_t bits = 0;
    MEMCPY(&bits, &value, sizeof(bits));
    return bits;
  }

  static float float_from_bits(const uint32_t bits)
  {
    float value = 0.0F;
    MEMCPY(&value, &bits, sizeof(value));
    return value;
  }

  static uint64_t pack_event(const float threshold, const uint32_t index)
  {
    return static_cast<uint64_t>(float_bits(threshold)) << 32
           | static_cast<uint64_t>(index);
  }

  // Positive float keys sort in unsigned bit order. Count all digits in one
  // traversal; stable scatters preserve the original order of tied thresholds.
  // Return the active buffer so skipped passes never require a copy-back.
  template <int WIDTH>
  static const uint64_t *radix_sort_events(uint64_t *events,
                                           uint64_t *scratch,
                                           int64_t *histogram,
                                           const int64_t count)
  {
    constexpr int ROUNDS = (32 + WIDTH - 1) / WIDTH;
    constexpr int BUCKETS = 1 << WIDTH;
    constexpr int LAST_BUCKETS = 1 << (32 - WIDTH * (ROUNDS - 1));
    constexpr int HISTOGRAM_COUNT = (ROUNDS - 1) * BUCKETS + LAST_BUCKETS;
    MEMSET(histogram, 0, HISTOGRAM_COUNT * sizeof(int64_t));
    for (int64_t i = 0; i < count; ++i) {
      const uint32_t key = static_cast<uint32_t>(events[i] >> 32);
      for (int round = 0; round < ROUNDS; ++round) {
        ++histogram[round * BUCKETS + ((key >> (round * WIDTH)) & (BUCKETS - 1))];
      }
    }
    uint64_t *source = events;
    uint64_t *destination = scratch;
    for (int round = 0; round < ROUNDS; ++round) {
      int64_t *offsets = histogram + round * BUCKETS;
      const int bucket_count = round + 1 == ROUNDS ? LAST_BUCKETS : BUCKETS;
      int64_t total = 0;
      bool uniform = false;
      for (int bucket = 0; bucket < bucket_count; ++bucket) {
        const int64_t bucket_size = offsets[bucket];
        uniform |= bucket_size == count;
        offsets[bucket] = total;
        total += bucket_size;
      }
      if (!uniform) {
        const int shift = 32 + round * WIDTH;
        for (int64_t i = 0; i < count; ++i) {
          const uint64_t value = source[i];
          destination[offsets[(value >> shift) & (BUCKETS - 1)]++] = value;
        }
        uint64_t *temporary = source;
        source = destination;
        destination = temporary;
      }
    }
    return source;
  }

  static const uint64_t *radix_sort_positive_f32_indices(uint64_t *events,
                                                        uint64_t *scratch,
                                                        int64_t *histogram,
                                                        const int64_t count)
  {
    const uint64_t *sorted = events;
    if (count > 1) {
      sorted = count <= SMALL_RADIX_EVENT_LIMIT
          ? radix_sort_events<8>(events, scratch, histogram, count)
          : radix_sort_events<11>(events, scratch, histogram, count);
    }
    return sorted;
  }

  // A translation of positive float bit keys preserves their full ordering.
  // When the translated interval fits in 23 bits, three 8-bit stable passes
  // need at most 256 + 256 + 128 counters. The histogram is populated while
  // generating events, so this path also avoids a separate counting traversal.
  static const uint64_t *radix_sort_bounded_events(uint64_t *events,
                                                   uint64_t *scratch,
                                                   int64_t *histogram,
                                                   const int64_t count,
                                                   const uint32_t key_base,
                                                   const uint32_t key_span)
  {
    uint64_t *source = events;
    uint64_t *destination = scratch;
    for (int round = 0; count > 1 && round < 3; ++round) {
      int64_t *offsets = histogram + round * 256;
      const int bucket_count = round == 2 ? (key_span >> 16) + 1 : 256;
      int64_t total = 0;
      bool uniform = false;
      for (int bucket = 0; bucket < bucket_count; ++bucket) {
        const int64_t bucket_size = offsets[bucket];
        uniform |= bucket_size == count;
        offsets[bucket] = total;
        total += bucket_size;
      }
      if (!uniform) {
        const int shift = round * 8;
        for (int64_t i = 0; i < count; ++i) {
          const uint64_t event = source[i];
          const uint32_t key = static_cast<uint32_t>(event >> 32) - key_base;
          destination[offsets[(key >> shift) & 0xFFU]++] = event;
        }
        uint64_t *temporary = source;
        source = destination;
        destination = temporary;
      }
    }
    return source;
  }

  static int find_best_t(const int64_t dim,
                         const int8_t extra_bits,
                         const float *normalized_residual,
                         float *absolute_residual,
                         uint8_t *current_magnitudes,
                         uint64_t *events,
                         uint64_t *radix_scratch,
                         int64_t *radix_histogram,
                         const int64_t event_capacity,
                         float &best_t)
  {
    int ret = OB_SUCCESS;
    const int32_t max_magnitude = (1 << extra_bits) - 1;
    const float max_absolute = get_extend_rabitq_simd_ops().absolute_and_max(
        normalized_residual, absolute_residual, dim);
    if (OB_UNLIKELY(!(max_absolute > 0.0F)
                    || !std::isfinite(max_absolute))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("invalid Extend RaBitQ normalized residual maximum",
               K(ret), K(max_absolute));
    } else {
      const float t_end =
          static_cast<float>(max_magnitude + 10) / max_absolute;
      const float t_start = t_end * TIGHT_START[extra_bits];
      float squared_denominator = static_cast<float>(dim) * 0.25F;
      float numerator = 0.0F;
      int64_t event_count = 0;
      const uint32_t key_base = float_bits(t_start);
      const uint32_t key_limit = float_bits(t_end) - 1;
      const uint32_t key_span = key_limit - key_base;
      const bool use_bounded_sort = key_limit >= key_base && key_span < (1U << 23);
      if (use_bounded_sort) {
        const int64_t bucket_count = 512 + (key_span >> 16) + 1;
        MEMSET(radix_histogram, 0, bucket_count * sizeof(int64_t));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < dim; ++i) {
        const float absolute = absolute_residual[i];
        int32_t magnitude = static_cast<int32_t>(
            std::floor(t_start * absolute + QUANTIZATION_EPS));
        if (magnitude < 0) {
          magnitude = 0;
        } else if (magnitude > max_magnitude) {
          magnitude = max_magnitude;
        }
        current_magnitudes[i] = static_cast<uint8_t>(magnitude);
        squared_denominator +=
            static_cast<float>(magnitude * magnitude + magnitude);
        numerator += (static_cast<float>(magnitude) + 0.5F) * absolute;
        if (absolute > 0.0F) {
          for (int32_t next = magnitude + 1;
               OB_SUCC(ret) && next <= max_magnitude;
               ++next) {
            const float threshold = static_cast<float>(next) / absolute;
            if (threshold < t_end) {
              if (OB_UNLIKELY(event_count >= event_capacity)) {
                ret = common::OB_ERR_UNEXPECTED;
                LOG_WARN("Extend RaBitQ event capacity exceeded",
                         K(ret), K(dim), K(event_capacity), K(event_count));
              } else {
                events[event_count++] =
                    pack_event(threshold, static_cast<uint32_t>(i));
                if (use_bounded_sort) {
                  // next exceeds t_start * absolute, so rounding next/absolute
                  // cannot move it below the representable t_start. The strict
                  // threshold < t_end check supplies the upper bound.
                  const uint32_t key = float_bits(threshold) - key_base;
                  ++radix_histogram[key & 0xFFU];
                  ++radix_histogram[256 + ((key >> 8) & 0xFFU)];
                  ++radix_histogram[512 + (key >> 16)];
                }
              }
            } else {
              // Later magnitudes only increase the threshold for this dimension.
              break;
            }
          }
        }
      }

      // Scores are positive, so comparing their squares preserves ordering
      // without a square root per event. For a unit residual, Cauchy-Schwarz
      // bounds numerator^2 by dim * (max_magnitude + 0.5)^2, safely within float.
      float max_squared_ip = 0.0F;
      if (OB_SUCC(ret)) {
        const uint64_t *sorted_events = use_bounded_sort
            ? radix_sort_bounded_events(events, radix_scratch, radix_histogram,
                                        event_count, key_base, key_span)
            : radix_sort_positive_f32_indices(events, radix_scratch,
                                              radix_histogram, event_count);
        max_squared_ip = numerator * numerator / squared_denominator;
        best_t = t_start;
        for (int64_t i = 0; i < event_count; ++i) {
          const uint64_t event = sorted_events[i];
          const uint32_t dim_index = static_cast<uint32_t>(event);
          const int32_t updated = ++current_magnitudes[dim_index];
          squared_denominator += static_cast<float>(2 * updated);
          numerator += absolute_residual[dim_index];
          const float current_squared_ip = numerator * numerator / squared_denominator;
          if (current_squared_ip > max_squared_ip) {
            max_squared_ip = current_squared_ip;
            best_t = float_from_bits(static_cast<uint32_t>(event >> 32));
          }
        }
      }
      if (OB_SUCC(ret)
          && OB_UNLIKELY(!std::isfinite(best_t) || !(max_squared_ip > 0.0F))) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("failed to select Extend RaBitQ scale",
                 K(ret), K(best_t), K(max_squared_ip));
      }
    }
    return ret;
  }

  static int validate_compute(const ObQuantizationParams &params,
                              const ObQuantizationBuffer &query_code,
                              const ObQuantizationBuffer &base_code,
                              QueryCodeLayout &query_layout,
                              BaseCodeLayout &base_layout)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_bits(params.bits_))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Extend RaBitQ compute bits", K(ret), K(params.bits_));
    } else if (OB_FAIL(QueryCodeLayout::calculate(params.dim_, query_layout))) {
      LOG_WARN("failed to calculate Extend RaBitQ query layout",
               K(ret), K(params.dim_));
    } else if (OB_FAIL(BaseCodeLayout::calculate(params.dim_,
                                                 params.bits_,
                                                 base_layout))) {
      LOG_WARN("failed to calculate Extend RaBitQ base layout",
               K(ret), K(params.dim_), K(params.bits_));
    } else if (OB_UNLIKELY(nullptr == query_code.data_
                           || nullptr == base_code.data_)) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null Extend RaBitQ compute buffer",
               K(ret), KP(query_code.data_), KP(base_code.data_));
    } else if (OB_UNLIKELY(query_code.size_ < query_layout.size_
                           || base_code.size_ < base_layout.size_)) {
      ret = common::OB_BUF_NOT_ENOUGH;
      LOG_WARN("Extend RaBitQ compute buffer is too small",
               K(ret), K(query_code.size_), K(query_layout.size_),
               K(base_code.size_), K(base_layout.size_));
    } else if (OB_UNLIKELY(!is_aligned(query_code.data_, alignof(float)))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WARN("Extend RaBitQ query buffer is not float-aligned",
               K(ret), KP(query_code.data_));
    }
    return ret;
  }

  static bool is_approx_zero(const float value)
  {
    return std::abs(value) < 1e-5F;
  }

  static float recover_metric(const float query_residual_norm,
                              const float query_raw_norm,
                              const float base_residual_norm,
                              const float base_raw_norm,
                              const float normalized_dot)
  {
    if constexpr (share::VIDA_IP == METRIC || share::VIDA_COS == METRIC) {
      if (is_approx_zero(query_raw_norm) || is_approx_zero(base_raw_norm)) {
        return 1.0F;
      }
    }
    const float estimated_l2_squared =
        base_residual_norm * base_residual_norm
        + query_residual_norm * query_residual_norm
        - 2.0F * base_residual_norm * query_residual_norm * normalized_dot;
    if constexpr (share::VIDA_IP == METRIC) {
      return 1.0F - (query_raw_norm * query_raw_norm
                     + base_raw_norm * base_raw_norm - estimated_l2_squared)
                        * 0.5F;
    } else if constexpr (share::VIDA_COS == METRIC) {
      return 1.0F - (query_raw_norm * query_raw_norm
                     + base_raw_norm * base_raw_norm - estimated_l2_squared)
                        * 0.5F / (query_raw_norm * base_raw_norm);
    } else {
      return estimated_l2_squared;
    }
  }

  static int compute_xbits_impl(const ObQuantizationParams &params,
                                const uint8_t *query_code,
                                const uint8_t *base_code,
                                const QueryCodeLayout &query_layout,
                                const BaseCodeLayout &base_layout,
                                const float ip_msb,
                                float &metric_value)
  {
    int ret = OB_SUCCESS;
    float query_residual_norm = 0.0F;
    float query_raw_norm = 0.0F;
    float sumq = 0.0F;
    float base_residual_norm = 0.0F;
    float base_raw_norm = 0.0F;
    float nbits_rescale = 0.0F;
    MEMCPY(&query_residual_norm,
           query_code + query_layout.residual_norm_offset_,
           sizeof(query_residual_norm));
    MEMCPY(&query_raw_norm,
           query_code + query_layout.raw_norm_offset_,
           sizeof(query_raw_norm));
    MEMCPY(&sumq,
           query_code + query_layout.sumq_offset_,
           sizeof(sumq));
    MEMCPY(&base_residual_norm,
           base_code + base_layout.residual_norm_offset_,
           sizeof(base_residual_norm));
    MEMCPY(&base_raw_norm,
           base_code + base_layout.raw_norm_offset_,
           sizeof(base_raw_norm));
    MEMCPY(&nbits_rescale,
           base_code + base_layout.nbits_rescale_offset_,
           sizeof(nbits_rescale));
    if (OB_UNLIKELY(!std::isfinite(query_residual_norm)
                    || !std::isfinite(query_raw_norm)
                    || !std::isfinite(sumq)
                    || !std::isfinite(base_residual_norm)
                    || !std::isfinite(base_raw_norm)
                    || !std::isfinite(nbits_rescale)
                    || query_residual_norm < 0.0F
                    || query_raw_norm < 0.0F
                    || base_residual_norm < 0.0F
                    || base_raw_norm < 0.0F
                    || nbits_rescale <= 0.0F
                    || !std::isfinite(ip_msb))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("invalid Extend RaBitQ encoded metadata",
               K(ret), K(query_residual_norm), K(query_raw_norm), K(sumq),
               K(base_residual_norm), K(base_raw_norm), K(nbits_rescale),
               K(ip_msb));
    } else {
      const float *normalized_query =
          reinterpret_cast<const float *>(query_code);
      const int64_t extra_bits_index = params.bits_ - 2;
      const float ip_extra =
          get_extend_rabitq_simd_ops().dot_extra_codes[extra_bits_index](
              normalized_query,
              base_code + base_layout.extra_code_offset_,
              params.dim_);
      const float factor = static_cast<float>(1U << (params.bits_ - 1));
      const float ip_xbits =
          factor * ip_msb + ip_extra - (factor - 0.5F) * sumq;
      const float normalized_dot = ip_xbits * nbits_rescale;
      const float result = recover_metric(query_residual_norm,
                                          query_raw_norm,
                                          base_residual_norm,
                                          base_raw_norm,
                                          normalized_dot);
      if (OB_UNLIKELY(!std::isfinite(ip_extra) || !std::isfinite(sumq)
                      || !std::isfinite(normalized_dot)
                      || !std::isfinite(result))) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("invalid Extend RaBitQ distance result",
                 K(ret), K(ip_msb), K(ip_extra), K(sumq),
                 K(normalized_dot), K(result));
      } else {
        metric_value = result;
      }
    }
    return ret;
  }
};

template <share::ObVectorIndexDistAlgorithm METRIC>
const ObQuantizerMetricOps &ObExtendRaBitQQuantizer::get_metric_ops()
{
  static const ObQuantizerMetricOps OPS = {
      calc_code_size,
      calc_query_code_size,
      calc_encode_scratch_size,
      MetricImpl<METRIC>::encode,
      MetricImpl<METRIC>::prepare_query,
      MetricImpl<METRIC>::compute,
      nullptr,
      MetricImpl<METRIC>::compute_xbits};
  return OPS;
}

int ObExtendRaBitQQuantizer::calc_code_size(const ObQuantizationParams &params,
                                           int64_t &size)
{
  int ret = OB_SUCCESS;
  BaseCodeLayout layout = {};
  if (OB_FAIL(BaseCodeLayout::calculate(params.dim_, params.bits_, layout))) {
    LOG_WARN("failed to calculate Extend RaBitQ code size",
             K(ret), K(params.dim_), K(params.bits_));
  } else {
    size = layout.size_;
  }
  return ret;
}

int ObExtendRaBitQQuantizer::calc_query_code_size(
    const ObQuantizationParams &params,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  QueryCodeLayout layout = {};
  if (OB_UNLIKELY(!is_valid_bits(params.bits_))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Extend RaBitQ query code bits", K(ret), K(params.bits_));
  } else if (OB_FAIL(QueryCodeLayout::calculate(params.dim_, layout))) {
    LOG_WARN("failed to calculate Extend RaBitQ query code size",
             K(ret), K(params.dim_), K(params.bits_));
  } else {
    size = layout.size_;
  }
  return ret;
}

int ObExtendRaBitQQuantizer::calc_encode_scratch_size(
    const ObQuantizationParams &params,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  EncodeScratchLayout layout = {};
  if (OB_UNLIKELY(!is_valid_bits(params.bits_))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid Extend RaBitQ encode scratch bits",
             K(ret), K(params.bits_));
  } else if (OB_FAIL(EncodeScratchLayout::calculate(
                         params.dim_, params.bits_, layout))) {
    LOG_WARN("failed to calculate Extend RaBitQ encode scratch size",
             K(ret), K(params.dim_), K(params.bits_));
  } else {
    size = layout.size_;
  }
  return ret;
}

const ObQuantizerOps &ObExtendRaBitQQuantizer::get_ops()
{
  static const ObQuantizerOps OPS = {{
      &get_metric_ops<share::VIDA_L2>(),
      &get_metric_ops<share::VIDA_IP>(),
      &get_metric_ops<share::VIDA_COS>()}};
  return OPS;
}

} // namespace quantization
} // namespace oceanbase
