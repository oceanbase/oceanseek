/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_READER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_READER_H_

#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"

namespace oceanbase
{
namespace blocksstable
{

// Reader of encoded inverted-index logical blocks. init() validates an
// uncompressed micro block and then parses its payload; init_payload() accepts
// an already materialized payload without the outer micro block header.
class ObInvIdxMicroBlockReader final
{
public:
  ObInvIdxMicroBlockReader();
  ~ObInvIdxMicroBlockReader();

  int init(
      const char *micro_block_buf,
      const int64_t micro_block_size,
      const bool is_posting_value);

  // Return the total number of logical blocks in this micro block payload.
  int64_t get_block_count() const;

  // Get the block_idx-th logical block payload. block_buf / block_size delimit the
  // encoded logical block inside the held payload.
  int get_payload(
      const int64_t block_idx,
      const char *&block_buf,
      int64_t &block_size) const;
  // Init from a materialized payload buffer (ObInvIdxMicroBlockPayloadHeader +
  // logical blocks + offset table) without the outer micro block header.
  int init_payload(const char *payload, const int64_t payload_size);

  void reuse();

  TO_STRING_KV(K_(is_inited), KP_(payload), K_(payload_size),
      KPC_(header), KP_(offsets));

private:
  const char *payload_;
  int64_t payload_size_;
  const ObInvIdxMicroBlockPayloadHeader *header_;
  const uint32_t *offsets_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObInvIdxMicroBlockReader);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_READER_H_
