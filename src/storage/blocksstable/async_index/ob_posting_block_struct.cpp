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

ObFlatCacheReader::ObFlatCacheReader()
  : header_(nullptr),
    descs_(nullptr),
    doc_ids_(nullptr),
    token_freqs_(nullptr),
    doc_lengths_(nullptr)
{}

void ObFlatCacheReader::reset()
{
  header_ = nullptr;
  descs_ = nullptr;
  doc_ids_ = nullptr;
  token_freqs_ = nullptr;
  doc_lengths_ = nullptr;
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
    const uint64_t doc_ids_offset = sizeof(ObFlatCacheHeader) + static_cast<uint64_t>(header->block_count_) * sizeof(ObFlatCacheBlockDesc);
    const uint64_t column_bytes = static_cast<uint64_t>(micro_header.row_count_) * sizeof(uint64_t);
    const uint64_t expected_size = doc_ids_offset + column_bytes * header->column_count_;
    const bool layout_valid = header->is_valid()
                              && 0 == base % sizeof(uint64_t)
                              && expected_size == static_cast<uint64_t>(payload_size);
    if (OB_UNLIKELY(!layout_valid)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid posting micro block cache layout", K(ret), K(payload_size), KPC(header));
    } else {
      header_ = header;
      descs_ = reinterpret_cast<const ObFlatCacheBlockDesc *>(payload + sizeof(ObFlatCacheHeader));
      doc_ids_ = reinterpret_cast<const uint64_t *>(payload + doc_ids_offset);
      if (header->has_relevance_params()) {
        token_freqs_ = doc_ids_ + micro_header.row_count_;
        doc_lengths_ = token_freqs_ + micro_header.row_count_;
      }
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
    doc_ids = doc_ids_ + first.doc_offset_;
    doc_count = static_cast<int64_t>(end - first.doc_offset_);
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
    block_data.doc_count_ = desc.doc_count_;
    block_data.doc_ids_ = doc_ids_ + desc.doc_offset_;
    block_data.token_freqs_ = OB_NOT_NULL(token_freqs_) ? token_freqs_ + desc.doc_offset_ : nullptr;
    block_data.doc_lengths_ = OB_NOT_NULL(doc_lengths_) ? doc_lengths_ + desc.doc_offset_ : nullptr;
  }
  return ret;
}

int ObPostingBlockData::init(const char *block_buf, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  doc_count_ = 0;
  doc_ids_ = nullptr;
  token_freqs_ = nullptr;
  doc_lengths_ = nullptr;
  const ObPostingBlockHeader *header = reinterpret_cast<const ObPostingBlockHeader *>(block_buf);
  if (OB_ISNULL(block_buf) || OB_UNLIKELY(block_size < HEADER_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid decoded posting block buffer", K(ret), KP(block_buf), K(block_size));
  } else if (OB_UNLIKELY(!header->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid decoded posting block header", K(ret), KPC(header));
  } else {
    const int64_t row_count = header->row_count_;
    const int64_t col_cnt = header->get_column_count();
    const int64_t expected_size = HEADER_SIZE + col_cnt * row_count * COL_WIDTH;
    if (OB_UNLIKELY(block_size < expected_size)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "decoded posting block buffer too small", K(ret), K(block_size),
          K(expected_size), K(row_count), K(col_cnt));
    } else {
      const uint64_t *columns = reinterpret_cast<const uint64_t *>(block_buf + HEADER_SIZE);
      doc_count_ = row_count;
      doc_ids_ = columns;
      if (header->has_relevance_params_) {
        token_freqs_ = doc_ids_ + row_count;
        doc_lengths_ = token_freqs_ + row_count;
      }
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
    doc_id = doc_ids_[idx];
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
    token_freq = token_freqs_[idx];
    doc_length = doc_lengths_[idx];
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
  if (OB_UNLIKELY(!is_valid() || start < 0 || end <= start || end > doc_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid lower_bound range", K(ret), KPC(this), K(start), K(end));
  } else {
    static constexpr int64_t WINDOW_WIDTH = 8;
    int64_t pos_idx = start;
    while (pos_idx + WINDOW_WIDTH < end && doc_ids_[pos_idx + WINDOW_WIDTH] < target) {
      pos_idx += WINDOW_WIDTH + 1;
    }
#if OB_USE_MULTITARGET_CODE
    if (pos_idx + WINDOW_WIDTH < end && common::is_arch_supported(common::ObTargetArch::AVX512)) {
      pos_idx += specific::avx512::count_less_than_target_avx512(doc_ids_ + pos_idx, target);
    } else
#elif OB_ARM_USE_MULTITARGET_CODE
    if (pos_idx + WINDOW_WIDTH < end && common::is_arch_supported(common::ObTargetArch::NEON)) {
      pos_idx += specific::neon::count_less_than_target_neon(doc_ids_ + pos_idx, target);
    } else
#endif
    {
      while (pos_idx < end && doc_ids_[pos_idx] < target) {
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
