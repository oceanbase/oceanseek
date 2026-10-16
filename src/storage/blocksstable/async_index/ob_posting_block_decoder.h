/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_DECODER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_DECODER_H_

#include "storage/blocksstable/async_index/ob_posting_block_struct.h"

#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace blocksstable
{

// Decoder of one binary posting block built by ObPostingBlockEncoder
class ObPostingBlockDecoder final
{
public:
  ObPostingBlockDecoder();
  ~ObPostingBlockDecoder();

  // init from encoded posting block buffer; decode_alloc is kept for API compatibility
  // (column materialization and codec scratch use decode_arena_, not decode_alloc)
  int init(const char *buf, const int64_t size, common::ObIAllocator &decode_alloc);

  OB_INLINE bool is_inited() const { return is_inited_; }
  OB_INLINE bool has_relevance_params() const
  {
    return nullptr != header_ && header_->has_relevance_params_;
  }
  OB_INLINE int64_t get_row_count() const
  {
    return nullptr != header_ ? header_->row_count_ : 0;
  }

  // get one group of data by index
  int get_doc_id(const int64_t idx, uint64_t &doc_id) const;
  int get_relevance_params(
      const int64_t idx,
      uint64_t &token_freq,
      uint64_t &doc_length) const;

  // return the index of the first doc_id >= target_doc_id,
  // return OB_BEYOND_THE_RANGE if no such doc_id exists
  int lower_bound(
      const uint64_t target_doc_id,
      const int64_t start_idx,
      const int64_t end_idx,
      int64_t &target_idx) const;

  // Batch decode the entire posting block into ObPostingBlockData (contiguous uint64 columns).
  // @param [in] out_alloc allocator for output column arrays
  // @param [out] out decoded column arrays, valid until out_alloc is reset
  int decode_to_block_data(common::ObIAllocator &out_alloc, ObPostingBlockData &out);

  // reuse the decoder to read next block
  void reuse();
  void reset();

  TO_STRING_KV(K_(is_inited), KPC_(header));

private:
  // decode context of one column in posting block
  struct ObPostingColumnDecodeCtx
  {
    ObPostingColumnDecodeCtx() { reset(); }
    void reset()
    {
      meta_ = nullptr;
      col_data_ = nullptr;
      col_data_size_ = 0;
      decoded_values_ = nullptr;
    }
    OB_INLINE bool is_decoded() const { return nullptr != decoded_values_; }
    TO_STRING_KV(KPC_(meta), KP_(col_data), K_(col_data_size), KP_(decoded_values));

    const ObPostingColumnMeta *meta_; // column meta in block buffer
    const char *col_data_;            // encoded column data in block buffer
    int64_t col_data_size_;           // payload bytes at col_data_
    const uint64_t *decoded_values_;
  };

  int get_packed_base_value(
      const ObPostingColumnDecodeCtx &col_ctx,
      const int64_t idx,
      uint64_t &value) const;
  int ensure_column_materialized(
      const int64_t col_idx,
      ObPostingColumnDecodeCtx &col_ctx) const;
  int ensure_ob_codec_decoded(
      const int64_t col_idx,
      ObPostingColumnDecodeCtx &col_ctx) const;
  static int lower_bound_uint64_array(
      const uint64_t *values,
      const uint64_t target,
      const int64_t start_idx,
      const int64_t end_idx,
      int64_t &target_idx);
  int get_column_value(
      const int64_t col_idx,
      const int64_t row_idx,
      uint64_t &value) const;
  int decode_column_batch(
      ObPostingColumnDecodeCtx &col_ctx,
      const int64_t row_count,
      uint64_t *dst);

  static int decode_packed_base_column_batch(
      const ObPostingColumnMeta *meta,
      const char *col_data,
      const int64_t row_count,
      uint64_t *dst);
  int decode_ob_codec_column_batch(
      ObPostingColumnDecodeCtx &col_ctx,
      const int64_t row_count,
      uint64_t *dst) const;
  static int get_column_data_size(
      const ObPostingColumnMeta &meta,
      const uint16_t row_count,
      const int64_t col_idx,
      const int64_t col_cnt,
      const int64_t block_size,
      const ObPostingColumnMeta *metas,
      int64_t &col_data_size);
  static int validate_ob_codec_column_payload(
      const ObPostingColumnMeta &meta,
      const uint16_t row_count,
      const int64_t col_data_size);
  int get_materialized_buf(const int64_t col_idx, uint64_t *&buf) const;
  int get_ob_codec_scratch_buf(char *&buf) const;

  const ObPostingBlockHeader *header_;   // block header in block buffer
  const ObPostingColumnMeta *col_metas_; // column meta array in block buffer
  mutable ObPostingColumnDecodeCtx col_ctxs_[ObPostingBlockHeader::MAX_POSTING_COL_CNT];
  char *decode_buf_; // POSTING_DECODE_BUF_BYTES from decode_arena_; nulled in reset()
  mutable common::ObArenaAllocator decode_arena_;
  int64_t col_cnt_;                    // initialized column count in col_ctxs_
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObPostingBlockDecoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_DECODER_H_
