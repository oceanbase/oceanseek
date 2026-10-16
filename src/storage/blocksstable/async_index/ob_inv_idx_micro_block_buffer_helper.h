/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_BUFFER_HELPER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_BUFFER_HELPER_H_

#include "storage/blocksstable/ob_macro_block.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObDataStoreDesc;

// Manages the compression/encryption buffers for one inverted-index data micro
// block stream. The input payload always remains caller-owned. A compressed or
// encrypted output is helper-owned and remains valid until the next call or
// reset(); when compression is skipped or expands the data, raw_desc.payload_
// may alias the caller-owned input and inherits its lifetime.
class ObInvIdxMicroBlockBufferHelper final
{
public:
  ObInvIdxMicroBlockBufferHelper();
  ~ObInvIdxMicroBlockBufferHelper();

  int open(const ObDataStoreDesc &data_store_desc);

  // Only the payload-related fields in raw_desc are overwritten:
  // payload_, payload_size_, uncompressed_size_, and block_checksum_.
  // Header bytes, row count, and last rowkey remain the caller's responsibility.
  //
  // The caller must keep uncompressed_buf valid until raw_desc is consumed.
  // Helper-owned output is invalidated by the next call or reset(); a raw
  // compression fallback continues to point to uncompressed_buf.
  int compress_encrypt_micro_block(
      const char *uncompressed_buf,
      const int64_t uncompressed_size,
      const int64_t macro_seq,
      const int64_t block_offset,
      ObRawMicroBlockDesc &raw_desc);

  void reset();

  TO_STRING_KV(KP_(data_store_desc), K_(micro_block_merge_verify_level));

private:
  int check_micro_block(
      const char *uncompressed_payload,
      const int64_t uncompressed_size,
      const char *stored_payload,
      const int64_t stored_size,
      const bool is_compressed);

private:
  const ObDataStoreDesc *data_store_desc_;
  int64_t micro_block_merge_verify_level_;
  ObMicroBlockCompressor compressor_;

  DISALLOW_COPY_AND_ASSIGN(ObInvIdxMicroBlockBufferHelper);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_BUFFER_HELPER_H_
