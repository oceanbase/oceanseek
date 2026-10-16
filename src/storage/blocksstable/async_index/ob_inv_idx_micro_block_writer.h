/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_WRITER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_WRITER_H_

#include "lib/container/ob_array.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_struct.h"

namespace oceanbase
{
namespace blocksstable
{

// Format-agnostic container that concatenates several already-encoded logical
// blocks (produced by ObPostingBlockEncoder or ObPostingValueBlockWriter) into
// one micro block payload.
// It does not parse the logical block content and does not produce the physical
// header / compression / checksum: those are applied later by the macro block writer.
class ObInvIdxMicroBlockWriter final
{
public:
  ObInvIdxMicroBlockWriter();
  ~ObInvIdxMicroBlockWriter();

  int init(common::ObIAllocator &allocator);

  // Estimate the payload size after appending one logical block of the given size.
  int estimate_size_after_append(const int64_t logical_block_size, int64_t &micro_block_size);

  // Append one already-encoded logical block. Outputs block_idx, the index
  // of this logical block within the current micro block.
  int append_logical_block(const char *buf, const int64_t size, int64_t &block_idx);

  // Assemble the uncompressed payload: [header][encoded blocks][block offsets],
  // common header excluded. buf points into the writer-owned buffer and is valid
  // until the next reuse() / reset().
  int build_micro_block(const char *&buf, int64_t &size);

  OB_INLINE int64_t get_block_count() const { return block_offsets_.count(); }
  OB_INLINE bool is_empty() const { return block_offsets_.empty(); }

  int reuse();
  void reset();

  TO_STRING_KV(K_(is_inited), K_(is_built), "block_count", block_offsets_.count(),
      K_(cur_blocks_size));

private:
  // reserve header space at the front of data_buf_ on init / reuse
  int reserve_header();

private:
  ObSelfBufferWriter data_buf_;             // [reserved header][encoded blocks], offset table appended on build
  common::ObArray<uint32_t> block_offsets_; // per-block relative offset, serialized as offset table on build
  int64_t cur_blocks_size_;                 // bytes of encoded blocks accumulated so far
  bool is_built_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObInvIdxMicroBlockWriter);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_WRITER_H_
