/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_ENCODER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_ENCODER_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_fixed_array.h"
#include "storage/blocksstable/async_index/ob_inv_idx_bm25_max_score_aggregator.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

// Buffer a group of rows and encode into one binary posting block
class ObPostingBlockEncoder final
{
public:
  ObPostingBlockEncoder();
  ~ObPostingBlockEncoder();

  int init(
      const ObPostingBlockEncodingCtx &ctx,
      common::ObIAllocator &allocator,
      const int64_t max_pair_count);

  // Extract doc_id / token_freq / doc_length etc. from row by the column indexes in encoding ctx
  // doc_id is guaranteed to be strictly ascending
  // return OB_BUF_NOT_ENOUGH if the block reaches capacity
  int append_row(const ObDatumRow &row);

  int append_batch(
      const ObBatchDatumRows &batch,
      const int64_t start,
      const int64_t cnt,
      int64_t &processed_cnt) { return OB_NOT_SUPPORTED; }

  // Serialize encoded posting block into buffer,
  // Buffer in posting_block is held by the encoder and valid until next reuse() / reset()
  int build_block(ObPostingBlockDesc &posting_block);

  OB_INLINE int64_t get_doc_count() const { return doc_ids_.count(); }
  OB_INLINE bool is_full() const
  {
    return IS_INIT && nullptr != ctx_ && doc_ids_.count() >= ctx_->block_capacity_;
  }

  void reuse();
  void reset();

  TO_STRING_KV(K_(is_inited), KPC_(ctx), K_(last_doc_id), "doc_count", doc_ids_.count());

private:
  typedef common::ObFixedArray<uint64_t, common::ObIAllocator> ColumnValueArray;

  static int validate_col_desc(
      const ObPostingBlockEncodingCtx &ctx,
      const int16_t col_idx,
      const char *col_name);
  static int validate_col_descs(const ObPostingBlockEncodingCtx &ctx);
  int append_col_value(
      const ObDatumRow &row,
      const int64_t col_idx,
      ColumnValueArray &values);
  int encode_column(
      const ColumnValueArray &values,
      const ObPostingColEncodingType encoding_type,
      const int64_t col_meta_idx);
  int validate_doc_ids_strictly_ascending() const;
  int encode_packed_base(
      const ColumnValueArray &values,
      const int64_t col_meta_idx);
  int encode_with_ob_codec(
      const ColumnValueArray &values,
      const ObPostingColEncodingType encoding_type,
      const int64_t col_meta_idx);
  static uint8_t determine_uint_bytes(const ColumnValueArray &values);
  static int fill_uint_buffer(
      const ColumnValueArray &values,
      const uint8_t uint_bytes,
      char *buf,
      const int64_t buf_size);
  static int dispatch_ob_codec_encode(
      const ObPostingColEncodingType encoding_type,
      const uint8_t uint_bytes,
      const char *in_buf,
      const uint64_t in_len,
      char *out_buf,
      const uint64_t out_buf_len,
      uint64_t &out_pos);
  static uint8_t calc_packed_width(const uint64_t max_diff);
  static int assign_column_data_offset(
      const int64_t offset,
      ObPostingColumnMeta &meta);
  OB_INLINE ObPostingBlockHeader *get_header()
  {
    return reinterpret_cast<ObPostingBlockHeader *>(data_buffer_.data());
  }
  OB_INLINE ObPostingColumnMeta *get_col_meta(const int64_t idx)
  {
    return reinterpret_cast<ObPostingColumnMeta *>(
        data_buffer_.data() + sizeof(ObPostingBlockHeader) + idx * sizeof(ObPostingColumnMeta));
  }

  const ObPostingBlockEncodingCtx *ctx_;
  common::ObIAllocator *allocator_;
  common::ObArenaAllocator codec_scratch_arena_; // temp buffers for ob_codec encode
  ColumnValueArray doc_ids_;
  ColumnValueArray token_freqs_;
  ColumnValueArray doc_lengths_;
  ObMicroBufferWriter data_buffer_;  // serialized block buffer
  uint64_t last_doc_id_;            // for strictly ascending check
  bool is_inited_;
  ObInvIdxBM25MaxScoreAggregator bm25_max_score_aggregator_;

  DISALLOW_COPY_AND_ASSIGN(ObPostingBlockEncoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_BLOCK_ENCODER_H_
