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

  // Uncompressed input must remain valid and unchanged until reset().
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
  struct FlatCacheLayoutPlan
  {
    FlatCacheLayoutPlan()
      : block_count_(0), column_count_(0), doc_id_width_(0), relevance_width_(0),
        doc_ids_offset_(0), token_freqs_offset_(0), doc_lengths_offset_(0),
        payload_size_(0), buf_size_(0)
    {}
    bool is_valid() const { return buf_size_ > 0; }

    int64_t block_count_;
    int64_t column_count_;
    uint32_t doc_id_width_;
    uint32_t relevance_width_;
    int64_t doc_ids_offset_;
    int64_t token_freqs_offset_;
    int64_t doc_lengths_offset_;
    int64_t payload_size_;
    int64_t buf_size_;
  };

  int decompress(
      const char *micro_block_buf,
      const ObMicroBlockDesMeta &des_meta,
      ObMacroBlockReader &reader);
  int prepare_layout_plan() const;
  int build_layout_plan(FlatCacheLayoutPlan &plan) const;
  int write_payload(const FlatCacheLayoutPlan &plan,
      const int64_t dst_size, char *dst, int64_t &payload_size);
  int write_flat_cache_block(
      const int64_t block_idx,
      const FlatCacheLayoutPlan &plan,
      ObFlatCacheBlockDesc &desc,
      char *doc_ids,
      char *token_freqs,
      char *doc_lengths,
      int64_t &doc_offset);

private:
  bool is_inited_;
  ObMicroBlockHeader header_;  // copy of the source micro block header (caller-deserialized)
  ObInvIdxMicroBlockReader reader_;
  mutable FlatCacheLayoutPlan layout_plan_; // built lazily once; immutable until reset()
  common::ObIAllocator *decode_allocator_;
  char *decomp_buf_;           // owned decompressed source copy; null when input is uncompressed

private:
  DISALLOW_COPY_AND_ASSIGN(ObInvIdxMicroBlockTransformer);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_TRANSFORMER_H_
