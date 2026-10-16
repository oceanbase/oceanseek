/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_VALUE_BLOCK_READER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_VALUE_BLOCK_READER_H_

#include "share/datum/ob_datum.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/encoding/ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{

class ObPostingValueBlockReader final
{
public:
  ObPostingValueBlockReader();
  ~ObPostingValueBlockReader();

  int init(const char *buf, const int64_t size);

  int get_position_list(const int64_t idx, ObDatum &value) const;
  int get_nested_key(const int64_t idx, ObDatum &value) const;

  void reuse();

  TO_STRING_KV(K_(is_inited), KPC_(header), KP_(row_data), KP_(row_index));

private:
  int locate_row(const int64_t idx, const char *&row_buf, int64_t &row_len) const;
  int get_col_value(
      const char *row_buf,
      const int64_t row_len,
      const int64_t col_idx,
      ObDatum &value) const;

private:
  const ObPostingValueBlockHeader *header_; // block header in block buffer
  const char *row_data_;                    // row buffer in block buffer
  const char *row_index_;                   // row index array in block buffer
  // Cached once in init(); avoids re-init ObIntegerArrayGenerator per locate_row.
  ObIntegerArrayGenerator row_idx_gen_;
  bool row_idx_gen_inited_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObPostingValueBlockReader);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_POSTING_VALUE_BLOCK_READER_H_
