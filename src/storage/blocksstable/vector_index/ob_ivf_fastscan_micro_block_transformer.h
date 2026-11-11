/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_FASTSCAN_MICRO_BLOCK_TRANSFORMER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_FASTSCAN_MICRO_BLOCK_TRANSFORMER_H_

#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"

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

// Cache value produced by ObIvfVectorMicroBlockTransformer. The row-major
// RaBitQ codes are packed in 32-row batches. Metadata is stored as padded SoA
// arrays so a FastScan kernel can load one complete batch without tail checks.
// Each batch contains rows from exactly one cid. batch_cids/batch_row_counts
// describe the batch boundaries, while vids contain only actual rows in disk
// order. Codes and metadata are padded to 32 lanes per batch.
struct ObIvfFastScanBlockHeader final
{
  static const uint32_t MAGIC = 0x46534251U; // "QBSF"
  static const uint16_t VERSION = 1;

  bool is_valid(const int64_t buf_size) const;
  int64_t padded_row_count() const { return static_cast<int64_t>(batch_count_) * 32; }
  TO_STRING_KV(K_(magic), K_(version), K_(header_size), K_(dim), K_(row_count),
      K_(batch_count), K_(code_bytes), K_(packed_code_size), K_(batch_cids_offset),
      K_(batch_row_counts_offset), K_(vids_offset), K_(codes_offset),
      K_(residual_norms_offset), K_(corrections_offset), K_(raw_norms_offset),
      K_(total_size), K_(min_vid), K_(max_vid));

  uint32_t magic_;
  uint16_t version_;
  uint16_t header_size_;
  int32_t dim_;
  int32_t row_count_;
  int32_t batch_count_;
  int32_t code_bytes_;
  int32_t packed_code_size_;
  int32_t batch_cids_offset_;
  int32_t batch_row_counts_offset_;
  int32_t vids_offset_;
  int32_t codes_offset_;
  int32_t residual_norms_offset_;
  int32_t corrections_offset_;
  int32_t raw_norms_offset_;
  int32_t total_size_;
  int32_t reserved_;
  // Inclusive VID bounds across every row in this cached block.
  int64_t min_vid_;
  int64_t max_vid_;
};

static_assert(sizeof(ObIvfFastScanBlockHeader) == 80,
    "unexpected IVF FastScan block header size");

// PRE cache: sorted CID/VID arrays followed by fixed-size RaBitQ payloads,
// including their norm/correction metadata. All offsets are relative to this
// header so KVCache can relocate the value without fixing internal pointers.
struct ObIvfQuantizedRowBlockHeader final
{
  static const uint32_t MAGIC = 0x51524251U;
  static const uint16_t VERSION = 1;

  bool is_valid(int64_t buf_size) const;
  const int64_t *cids() const
  { return reinterpret_cast<const int64_t *>(reinterpret_cast<const char *>(this) + cids_offset_); }
  const int64_t *vids() const
  { return reinterpret_cast<const int64_t *>(reinterpret_cast<const char *>(this) + vids_offset_); }
  const char *payload(int64_t row) const
  { return reinterpret_cast<const char *>(this) + payloads_offset_ + row * static_cast<int64_t>(code_size_); }

  uint32_t magic_;
  uint16_t version_;
  uint16_t header_size_;
  int32_t dim_;
  int32_t row_count_;
  int32_t code_size_;
  int32_t cids_offset_;
  int32_t vids_offset_;
  int32_t payloads_offset_;
  int32_t total_size_;
  int32_t reserved_;
};

static_assert(sizeof(ObIvfQuantizedRowBlockHeader) == 40,
    "unexpected IVF quantized row block header size");

class ObIvfVectorMicroBlockTransformer final
{
public:
  ObIvfVectorMicroBlockTransformer();
  ~ObIvfVectorMicroBlockTransformer();

  int init(
      const ObMicroBlockHeader &header,
      const char *micro_block_buf,
      const int64_t micro_block_size,
      const ObMicroBlockDesMeta &des_meta,
      ObMacroBlockReader &reader,
      common::ObIAllocator &allocator,
      const ObIvfFastScanTransformContext &fastscan_ctx);
  int transform(char *&block_buf, int64_t &buf_size);
  int transform_quantized_rows(char *&block_buf, int64_t &buf_size);
  void reset();

  TO_STRING_KV(K_(is_inited), K_(header), K_(fastscan_ctx));

private:
  int count_batches_by_cid(int64_t expected_code_size, int64_t &batch_count);
  int materialize_rows(char *dst, ObIvfFastScanBlockHeader &out_header);
  int pack_batch_codes(const uint8_t *codes,
                       int64_t code_count,
                       int64_t code_bytes,
                       int64_t batch_index,
                       char *dst,
                       const ObIvfFastScanBlockHeader &out_header);

private:
  bool is_inited_;
  ObMicroBlockHeader header_;
  ObIvfFastScanTransformContext fastscan_ctx_;
  ObIvfSSTableRowDesc row_desc_;
  ObMicroBlockData decoded_block_;
  ObMicroBlockReaderHelper reader_helper_;
  ObIMicroBlockReader *micro_reader_;
  common::ObIAllocator *allocator_;
  char *decoded_buf_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIvfVectorMicroBlockTransformer);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_FASTSCAN_MICRO_BLOCK_TRANSFORMER_H_
