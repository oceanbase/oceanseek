/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_READER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_READER_H_

#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"

namespace oceanbase
{
namespace blocksstable
{

// Reader of one inverted index data micro block (posting data or posting value
// data) built by ObInvIdxMicroBlockWriter. The input includes
// ObMicroBlockHeader and payload; this reader validates the common header,
// strips it and then parses the payload layout.
//
// This is the read-side companion of ObInvIdxMicroBlockWriter. Inverted-index
// callers use init_flat_cache() for transformed posting-data cache values.
// init() retains the encoded logical-block path used by on-disk readers and
// posting-value blocks. Only FlatCache exposes ObPostingBlockData to callers.
class ObInvIdxMicroBlockReader final
{
public:
  ObInvIdxMicroBlockReader();
  ~ObInvIdxMicroBlockReader();

  int init_flat_cache(
      const char *micro_block_buf,
      const int64_t micro_block_size);
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
  int get_posting_block_data(
      const int64_t block_idx,
      ObPostingBlockData &block_data) const;
  // Return one contiguous doc-id view spanning inclusive logical block indexes.
  // Only the FlatCache layout supports this physical-group view.
  int get_doc_id_view(
      const int64_t first_block_idx,
      const int64_t last_block_idx,
      const uint64_t *&doc_ids,
      int64_t &doc_count) const;

  // Init from a materialized payload buffer (ObInvIdxMicroBlockPayloadHeader +
  // logical blocks + offset table). Used by diagnostic dumpers that have already
  // stripped and materialized the ObMicroBlockHeader frame.
  int init_payload(const char *payload, const int64_t payload_size);

  void reuse();

  TO_STRING_KV(K_(is_inited), KP_(payload), K_(payload_size),
      KPC_(header), KP_(offsets));

private:
  friend class ObInvIdxLogicalBlockIterator;

private:
  const char *payload_;
  int64_t payload_size_;
  const ObInvIdxMicroBlockPayloadHeader *header_;
  const uint32_t *offsets_;
  ObFlatCacheReader flat_cache_reader_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObInvIdxMicroBlockReader);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_READER_H_
