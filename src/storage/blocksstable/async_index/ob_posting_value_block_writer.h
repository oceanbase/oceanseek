/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_VALUE_BLOCK_WRITER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_VALUE_BLOCK_WRITER_H_

#include "lib/container/ob_fixed_array.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_imicro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

// Currently posting value block is aligned with posting block by row index
class ObPostingValueBlockWriter final
{
public:
  ObPostingValueBlockWriter();
  ~ObPostingValueBlockWriter();

  int init(const ObPostingValueBlockWriteParam &param, common::ObIAllocator &allocator);

  int append_row(const ObDatumRow &row);
  int estimate_row_size(const ObDatumRow &row, int64_t &row_size) const;
  int estimate_size_after_append(const int64_t row_size, int64_t &logical_block_size) const;

  int append_batch(
      const ObBatchDatumRows &batch,
      const int64_t start,
      const int64_t cnt,
      int64_t &processed_cnt) { return OB_NOT_SUPPORTED; }

  int build_block(ObPostingValueBlockDesc &value_block);

  OB_INLINE int64_t get_row_count() const { return row_offsets_.count(); }
  OB_INLINE bool is_full() const
  {
    return IS_INIT && nullptr != param_ && row_offsets_.count() >= param_->block_capacity_;
  }

  void reuse();
  void reset();

  TO_STRING_KV(K_(is_inited), KPC_(param), "row_count", row_offsets_.count());

private:
  typedef common::ObFixedArray<int64_t, common::ObIAllocator> RowOffsetArray;

  int prepare_row(
      const ObDatumRow &row,
      const char *(&col_ptrs)[2],
      int64_t (&col_lens)[2],
      uint8_t &col_idx_byte_width,
      int64_t &row_size) const;
  int build_row_index();
  int append_binary_col(
      const ObDatumRow &row,
      const int16_t col_idx,
      const bool null_as_empty,
      const char *&ptr,
      int64_t &len) const;
  int reserve_block_header_if_needed();
  OB_INLINE ObPostingValueBlockHeader *get_header()
  {
    return reinterpret_cast<ObPostingValueBlockHeader *>(data_buffer_.data());
  }

  const ObPostingValueBlockWriteParam *param_;
  common::ObIAllocator *allocator_;
  ObMicroBufferWriter data_buffer_;     // serialized block buffer
  RowOffsetArray row_offsets_;         // offset of each row, serialized as row index on build
  bool has_header_reserved_;           // whether block header space is reserved in data_buffer_
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObPostingValueBlockWriter);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_VALUE_BLOCK_WRITER_H_
