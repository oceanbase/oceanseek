/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_STRUCT_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_STRUCT_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace blocksstable
{

// Header of the inverted index micro block payload.
// Layout of the (uncompressed) micro block payload, physical header excluded:
// ```
//   +-----------------------------------------------------+
//   | ObInvIdxMicroBlockPayloadHeader                     |
//   +-----------------------------------------------------+
//   | encoded logical blocks                              | <- blocks_offset_
//   |   block[0] | block[1] | ... | block[block_count_-1] |
//   +-----------------------------------------------------+
//   | block offsets (uint32 x (block_count_ + 1))         | <- block_offsets_offset_
//   +-----------------------------------------------------+
// ```
// block_offsets_[i] / block_offsets_[i + 1] delimit block i within the encoded
// blocks region (relative to blocks_offset_); the last entry is a sentinel equal
// to the total size of the encoded blocks region.
struct ObInvIdxMicroBlockPayloadHeader
{
  static const uint8_t VERSION = 1;
  static const int64_t BLOCK_OFFSET_SIZE = sizeof(uint32_t);
  static const int64_t MAX_BLOCK_COUNT = UINT16_MAX;

  ObInvIdxMicroBlockPayloadHeader()
    : version_(VERSION),
      reserved_(0),
      block_count_(0),
      blocks_offset_(0),
      block_offsets_offset_(0),
      total_size_(0)
  {}
  OB_INLINE bool is_valid() const
  {
    return VERSION == version_
        && block_count_ > 0
        && blocks_offset_ >= sizeof(ObInvIdxMicroBlockPayloadHeader)
        && block_offsets_offset_ >= blocks_offset_
        && total_size_ == block_offsets_offset_ + (block_count_ + 1) * BLOCK_OFFSET_SIZE;
  }
  TO_STRING_KV(K_(version), K_(block_count), K_(blocks_offset),
      K_(block_offsets_offset), K_(total_size));

  uint8_t version_;               // header version
  uint8_t reserved_;
  uint16_t block_count_;          // number of logical blocks in this micro block
  uint32_t blocks_offset_;        // start offset of encoded blocks region (relative to payload head)
  uint32_t block_offsets_offset_; // start offset of block offset table (relative to payload head)
  uint32_t total_size_;           // total size of the payload
} __attribute__((packed));

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_STRUCT_H_
