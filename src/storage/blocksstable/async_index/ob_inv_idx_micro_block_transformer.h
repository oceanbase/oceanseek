/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_TRANSFORMER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_TRANSFORMER_H_

#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace blocksstable
{

class ObMacroBlockReader;
struct ObMicroBlockDesMeta;

// Transforms one on-disk encoded posting-data micro block into FlatCache.
// The output keeps a rewritten ObMicroBlockHeader prefix, and its payload is
// documented on ObFlatCacheHeader.
class ObInvIdxMicroBlockTransformer final
{
public:
  ObInvIdxMicroBlockTransformer();
  ~ObInvIdxMicroBlockTransformer();

  int init(
      const ObMicroBlockHeader &header,
      const char *micro_block_buf,
      const int64_t micro_block_size,
      const ObMicroBlockDesMeta &des_meta,
      ObMacroBlockReader &reader,
      common::ObIAllocator &decode_allocator);
  int get_buf_size(int64_t &buf_size) const;
  int transform(char *block_buf, const int64_t buf_size);
  void reset();

  TO_STRING_KV(K_(is_inited), K_(header), K_(reader));

private:
  int decompress(
      const char *micro_block_buf,
      const ObMicroBlockDesMeta &des_meta,
      ObMacroBlockReader &reader);
  int calc_payload_size(int64_t &payload_size) const;
  int write_payload(char *dst, const int64_t dst_size, int64_t &payload_size);
  int get_flat_cache_column_count(int64_t &column_count) const;
  int write_flat_cache_block(
      const int64_t block_idx,
      ObFlatCacheBlockDesc &desc,
      uint64_t *doc_ids,
      int64_t &doc_offset);

private:
  bool is_inited_;
  ObMicroBlockHeader header_;  // copy of the source micro block header (caller-deserialized)
  ObInvIdxMicroBlockReader reader_;
  common::ObIAllocator *decode_allocator_;
  char *decomp_buf_;           // owned decompressed source copy; null when input is uncompressed

private:
  DISALLOW_COPY_AND_ASSIGN(ObInvIdxMicroBlockTransformer);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_TRANSFORMER_H_
