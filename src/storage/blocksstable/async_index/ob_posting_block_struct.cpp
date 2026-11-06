/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "common/ob_target_specific.h"

#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#elif OB_ARM_USE_MULTITARGET_CODE
#include <arm_neon.h>
#endif

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

#if OB_USE_MULTITARGET_CODE
OB_DECLARE_AVX512_SPECIFIC_CODE(
static int64_t count_less_than_target_avx512(
    const uint64_t *doc_ids,
    const uint64_t target)
{
  const __m512i values = _mm512_loadu_si512(reinterpret_cast<const void *>(doc_ids));
  const __m512i target_vec = _mm512_set1_epi64(static_cast<int64_t>(target));
  const __mmask8 less_mask = _mm512_cmp_epu64_mask(values, target_vec, _MM_CMPINT_LT);
  return static_cast<int64_t>(__builtin_popcount(static_cast<unsigned int>(less_mask)));
}
)
#endif

#if OB_USE_MULTITARGET_CODE
OB_DECLARE_AVX512_SPECIFIC_CODE(
static int64_t count_less_than_target_uint32_avx512(
    const uint32_t *doc_ids, const uint32_t target)
{
  const __m512i values = _mm512_loadu_si512(reinterpret_cast<const void *>(doc_ids));
  const __m512i target_vec = _mm512_set1_epi32(static_cast<int32_t>(target));
  const __mmask16 less_mask = _mm512_cmp_epu32_mask(values, target_vec, _MM_CMPINT_LT);
  return static_cast<int64_t>(__builtin_popcount(static_cast<unsigned int>(less_mask)));
}
)
#endif

#if OB_ARM_USE_MULTITARGET_CODE
OB_DECLARE_NEON_SPECIFIC_CODE(
static int64_t count_less_than_target_neon(
    const uint64_t *doc_ids,
    const uint64_t target)
{
  const uint64x2_t target_vec = vdupq_n_u64(target);
  uint64x2_t counts = vshrq_n_u64(vcltq_u64(vld1q_u64(doc_ids), target_vec), 63);
  counts = vaddq_u64(counts, vshrq_n_u64(vcltq_u64(vld1q_u64(doc_ids + 2), target_vec), 63));
  counts = vaddq_u64(counts, vshrq_n_u64(vcltq_u64(vld1q_u64(doc_ids + 4), target_vec), 63));
  counts = vaddq_u64(counts, vshrq_n_u64(vcltq_u64(vld1q_u64(doc_ids + 6), target_vec), 63));
  return static_cast<int64_t>(vaddvq_u64(counts));
}
)
#endif

#if OB_ARM_USE_MULTITARGET_CODE
OB_DECLARE_NEON_SPECIFIC_CODE(
static int64_t count_less_than_target_uint32_neon(
    const uint32_t *doc_ids, const uint32_t target)
{
  const uint32x4_t target_vec = vdupq_n_u32(target);
  uint32x4_t counts = vshrq_n_u32(vcltq_u32(vld1q_u32(doc_ids), target_vec), 31);
  counts = vaddq_u32(counts, vshrq_n_u32(vcltq_u32(vld1q_u32(doc_ids + 4), target_vec), 31));
  counts = vaddq_u32(counts, vshrq_n_u32(vcltq_u32(vld1q_u32(doc_ids + 8), target_vec), 31));
  counts = vaddq_u32(counts, vshrq_n_u32(vcltq_u32(vld1q_u32(doc_ids + 12), target_vec), 31));
  return static_cast<int64_t>(vaddvq_u32(counts));
}
)
#endif

ObFlatCacheReader::ObFlatCacheReader()
  : header_(nullptr),
    descs_(nullptr),
    doc_ids_(),
    relevance_(),
    row_count_(0)
{}

void ObFlatCacheReader::reset()
{
  header_ = nullptr;
  descs_ = nullptr;
  doc_ids_.reset();
  relevance_.reset();
  row_count_ = 0;
}

int ObFlatCacheReader::init(
    const char *micro_block_buf,
    const int64_t micro_block_size)
{
  int ret = OB_SUCCESS;
  reset();
  ObMicroBlockHeader micro_header;
  int64_t pos = 0;
  if (OB_ISNULL(micro_block_buf) || OB_UNLIKELY(micro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting micro block cache input", K(ret), KP(micro_block_buf), K(micro_block_size));
  } else if (OB_FAIL(micro_header.deserialize(micro_block_buf, micro_block_size, pos))) {
    STORAGE_LOG(WARN, "failed to deserialize posting micro block cache header", K(ret), K(micro_block_size));
  } else if (OB_UNLIKELY(common::INV_IDX_ROW_STORE != micro_header.get_row_store_type()
             || MICRO_BLOCK_INV_IDX_POSTING
                != static_cast<ObMicroBlockInvertedIndexType>(micro_header.inverted_index_type_)
             || micro_header.data_length_ != micro_header.data_zlength_
             || 0 == micro_header.row_count_
             || static_cast<int64_t>(micro_header.header_size_)
                + static_cast<int64_t>(micro_header.data_length_) != micro_block_size
             || micro_header.data_length_
                < static_cast<int64_t>(sizeof(ObFlatCacheHeader)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid posting micro block cache value", K(ret), K(micro_header), K(micro_block_size));
  } else {
    const char *payload = micro_block_buf + micro_header.header_size_;
    const int64_t payload_size = micro_header.data_length_;
    const ObFlatCacheHeader *header = reinterpret_cast<const ObFlatCacheHeader *>(payload);
    const uint64_t base = reinterpret_cast<uintptr_t>(payload);
    const uint64_t doc_ids_offset = ObFlatCacheHeader::doc_ids_offset(header->block_count_);
    const uint64_t relevance_offset = ObFlatCacheHeader::relevance_offset(
        header->block_count_, micro_header.row_count_, header->doc_id_width_);
    const uint64_t expected_size = ObFlatCacheHeader::payload_size(
        header->block_count_, micro_header.row_count_, header->doc_id_width_, header->relevance_width_);
    const bool layout_valid = header->is_valid()
                              && 0 == base % sizeof(uint64_t)
                              && expected_size == static_cast<uint64_t>(payload_size);
    if (OB_UNLIKELY(!layout_valid)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid posting micro block cache layout", K(ret), K(payload_size), KPC(header));
    } else {
      header_ = header;
      row_count_ = micro_header.row_count_;
      descs_ = reinterpret_cast<const ObFlatCacheBlockDesc *>(payload + sizeof(ObFlatCacheHeader));
      doc_ids_.values_ = payload + doc_ids_offset;
      doc_ids_.width_ = header->doc_id_width_;
      if (header->has_relevance_params()) {
        relevance_.token_freqs_ = payload + relevance_offset;
        relevance_.doc_lengths_ = payload + relevance_offset
            + static_cast<uint64_t>(micro_header.row_count_) * header->relevance_width_;
        relevance_.width_ = header->relevance_width_;
      }
    }
  }
  return ret;
}

int ObFlatCacheReader::assign_columns(
    const int64_t start_idx, const int64_t doc_count, ObPostingBlockData &block_data) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == header_ || start_idx < 0 || doc_count <= 0
      || start_idx > row_count_ || doc_count > row_count_ - start_idx)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObPostingDocIdView ids = doc_ids_;
    if (ids.is_uint32()) {
      ids.values_ = doc_ids_.get_uint32() + start_idx;
    } else {
      ids.values_ = doc_ids_.get_uint64() + start_idx;
    }
    ObPostingRelevanceView relevance = relevance_;
    if (relevance.has_relevance_params()) {
      if (relevance.is_uint32()) {
        relevance.token_freqs_ = relevance_.get_token_freqs32() + start_idx;
        relevance.doc_lengths_ = relevance_.get_doc_lengths32() + start_idx;
      } else {
        relevance.token_freqs_ = relevance_.get_token_freqs64() + start_idx;
        relevance.doc_lengths_ = relevance_.get_doc_lengths64() + start_idx;
      }
    }
    if (OB_FAIL(block_data.assign(doc_count, ids, relevance))) {
      STORAGE_LOG(WARN, "failed to bind posting columns", K(ret), K(start_idx), K(doc_count));
    }
  }
  return ret;
}

int ObFlatCacheReader::get_doc_id_view(
    const int64_t first_block_idx,
    const int64_t last_block_idx,
    const uint64_t *&doc_ids,
    int64_t &doc_count) const
{
  int ret = OB_SUCCESS;
  doc_ids = nullptr;
  doc_count = 0;
  if (OB_ISNULL(header_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting micro block cache reader is not initialized", K(ret));
  } else if (OB_UNLIKELY(first_block_idx < 0 || last_block_idx < first_block_idx
             || last_block_idx >= header_->block_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "posting micro block view range is invalid", K(ret), K(first_block_idx), K(last_block_idx), K(header_->block_count_));
  } else {
    const ObFlatCacheBlockDesc &first = descs_[first_block_idx];
    const ObFlatCacheBlockDesc &last = descs_[last_block_idx];
    const uint64_t end = static_cast<uint64_t>(last.doc_offset_) + last.doc_count_;
    const uint64_t *flat_doc_ids = doc_ids_.get_uint64();
    if (nullptr == flat_doc_ids) {
      ret = OB_NOT_SUPPORTED;
    } else {
      doc_ids = flat_doc_ids + first.doc_offset_;
      doc_count = static_cast<int64_t>(end - first.doc_offset_);
    }
  }
  return ret;
}

int ObFlatCacheReader::get_posting_block_data(
    const int64_t block_idx,
    ObPostingBlockData &block_data) const
{
  int ret = OB_SUCCESS;
  block_data.reset();
  if (OB_ISNULL(header_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "posting micro block cache reader is not initialized", K(ret));
  } else if (OB_UNLIKELY(block_idx < 0 || block_idx >= header_->block_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "posting micro block index is invalid", K(ret), K(block_idx), K(header_->block_count_));
  } else {
    const ObFlatCacheBlockDesc &desc = descs_[block_idx];
    if (OB_FAIL(assign_columns(desc.doc_offset_, desc.doc_count_, block_data))) {
      STORAGE_LOG(WARN, "failed to assign posting block columns", K(ret), K(block_idx));
    }
  }
  return ret;
}

int ObPostingBlockData::assign(
    const int64_t doc_count, const ObPostingDocIdView &doc_ids,
    const ObPostingRelevanceView &relevance)
{
  int ret = OB_SUCCESS;
  reset();
  const bool has_relevance = relevance.has_relevance_params();
  if (OB_UNLIKELY(doc_count <= 0 || !doc_ids.is_valid()
      || (nullptr == relevance.token_freqs_) != (nullptr == relevance.doc_lengths_)
      || (has_relevance
          ? (sizeof(uint32_t) != relevance.width_ && sizeof(uint64_t) != relevance.width_)
          : 0 != relevance.width_))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    doc_count_ = doc_count;
    doc_ids_ = doc_ids;
    relevance_ = relevance;
  }
  return ret;
}

int ObPostingBlockData::get_doc_id_view(
    const int64_t start_idx, ObPostingDocIdView &view) const
{
  int ret = OB_SUCCESS;
  view.reset();
  if (OB_UNLIKELY(!is_valid() || start_idx < 0 || start_idx >= doc_count_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    view = doc_ids_;
    if (view.is_uint32()) {
      view.values_ = doc_ids_.get_uint32() + start_idx;
    } else {
      view.values_ = doc_ids_.get_uint64() + start_idx;
    }
  }
  return ret;
}

int ObPostingBlockData::get_relevance_view(
    const int64_t start_idx, ObPostingRelevanceView &view) const
{
  int ret = OB_SUCCESS;
  view.reset();
  if (OB_UNLIKELY(!is_valid() || start_idx < 0 || start_idx >= doc_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid relevance projection", K(ret), K(start_idx), K_(doc_count));
  } else if (relevance_.has_relevance_params()) {
    view = relevance_;
    if (view.is_uint32()) {
      view.token_freqs_ = relevance_.get_token_freqs32() + start_idx;
      view.doc_lengths_ = relevance_.get_doc_lengths32() + start_idx;
    } else {
      view.token_freqs_ = relevance_.get_token_freqs64() + start_idx;
      view.doc_lengths_ = relevance_.get_doc_lengths64() + start_idx;
    }
  }
  return ret;
}

int ObPostingBlockData::get_doc_id(const int64_t idx, uint64_t &doc_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || idx < 0 || idx >= doc_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid doc_id access", K(ret), KPC(this), K(idx));
  } else {
    doc_id = doc_id_at(idx);
  }
  return ret;
}

int ObPostingBlockData::get_relevance_params(
    const int64_t idx,
    uint64_t &token_freq,
    uint64_t &doc_length) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!has_relevance_params() || idx < 0 || idx >= doc_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid relevance params access", K(ret), KPC(this), K(idx));
  } else {
    if (relevance_.is_uint32()) {
      token_freq = relevance_.get_token_freqs32()[idx];
      doc_length = relevance_.get_doc_lengths32()[idx];
    } else {
      token_freq = relevance_.get_token_freqs64()[idx];
      doc_length = relevance_.get_doc_lengths64()[idx];
    }
  }
  return ret;
}

int ObPostingBlockData::upper_bound(
    const uint64_t target, const int64_t start, const int64_t end, int64_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = 0;
  // Search with target + 1 only when the last DocID exceeds target, preventing overflow.
  if (OB_UNLIKELY(!is_valid() || start < 0 || end <= start || end > doc_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid upper_bound range", K(ret), K(start), K(end), K_(doc_count));
  } else if (target >= doc_id_at(end - 1)) {
    idx = end;
  } else if (OB_FAIL(lower_bound(target + 1, start, end, idx))) {
    STORAGE_LOG(WARN, "failed to locate posting upper bound", K(ret), K(target), K(start), K(end));
  }
  return ret;
}

int ObPostingBlockData::lower_bound(
    const uint64_t target,
    const int64_t start,
    const int64_t end,
    int64_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = 0;
  const uint32_t *doc_ids32 = doc_ids_.get_uint32();
  const uint64_t *doc_ids = doc_ids_.get_uint64();
  if (OB_UNLIKELY(!is_valid() || start < 0 || end <= start || end > doc_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid lower_bound range", K(ret), KPC(this), K(start), K(end));
  } else if (nullptr != doc_ids32) {
    if (target > UINT32_MAX) {
      idx = end;
      ret = OB_BEYOND_THE_RANGE;
    } else {
      static constexpr int64_t WINDOW_WIDTH = 16;
      int64_t pos_idx = start;
      while (pos_idx + WINDOW_WIDTH < end && doc_ids32[pos_idx + WINDOW_WIDTH] < target) {
        pos_idx += WINDOW_WIDTH + 1;
      }
#if OB_USE_MULTITARGET_CODE
      if (pos_idx + WINDOW_WIDTH <= end && common::is_arch_supported(common::ObTargetArch::AVX512)) {
        pos_idx += specific::avx512::count_less_than_target_uint32_avx512(
            doc_ids32 + pos_idx, static_cast<uint32_t>(target));
      } else
#elif OB_ARM_USE_MULTITARGET_CODE
      if (pos_idx + WINDOW_WIDTH <= end && common::is_arch_supported(common::ObTargetArch::NEON)) {
        pos_idx += specific::neon::count_less_than_target_uint32_neon(
            doc_ids32 + pos_idx, static_cast<uint32_t>(target));
      } else
#endif
      {
        while (pos_idx < end && doc_ids32[pos_idx] < target) { ++pos_idx; }
      }
      idx = pos_idx;
      if (idx >= end) { ret = OB_BEYOND_THE_RANGE; }
    }
  } else {
    static constexpr int64_t WINDOW_WIDTH = 8;
    int64_t pos_idx = start;
    while (pos_idx + WINDOW_WIDTH < end && doc_ids[pos_idx + WINDOW_WIDTH] < target) {
      pos_idx += WINDOW_WIDTH + 1;
    }
#if OB_USE_MULTITARGET_CODE
    if (pos_idx + WINDOW_WIDTH < end && common::is_arch_supported(common::ObTargetArch::AVX512)) {
      pos_idx += specific::avx512::count_less_than_target_avx512(doc_ids + pos_idx, target);
    } else
#elif OB_ARM_USE_MULTITARGET_CODE
    if (pos_idx + WINDOW_WIDTH < end && common::is_arch_supported(common::ObTargetArch::NEON)) {
      pos_idx += specific::neon::count_less_than_target_neon(doc_ids + pos_idx, target);
    } else
#endif
    {
      while (pos_idx < end && doc_ids[pos_idx] < target) {
        ++pos_idx;
      }
    }
    idx = pos_idx;
    if (OB_UNLIKELY(idx >= end)) {
      ret = OB_BEYOND_THE_RANGE;
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
