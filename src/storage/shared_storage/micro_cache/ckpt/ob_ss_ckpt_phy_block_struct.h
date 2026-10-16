/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_STRUCT_H_

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_define.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"

namespace oceanbase
{
namespace storage
{

bool need_compress_segment(const ObCompressorType &comp_type);
int64_t get_blk_payload_offset();
int64_t get_seg_payload_offset();

/*
 * Each segment contains several items; We can compress segment and save the meta into its header.
 */
struct ObSSCkptPhyBlockSegmentHeader final
{
public:
  static const int32_t SS_CKPT_BLK_SEG_HEADER_VERSION = 1;

public:
  ObSSCkptPhyBlockSegmentHeader() { reset(); }
  ~ObSSCkptPhyBlockSegmentHeader() { reset(); }
  void reset();
  bool need_compress() const;
  uint8_t get_compressor_type() const;
  bool is_valid() const { return payload_size_ > 0; }
  int32_t get_payload_size() const;
  int32_t get_real_payload_size() const;
  // NOTICE: if update this struct field, please handle compatibility
  int serialize(char *buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(header_size), K_(version), K_(payload_size), K_(payload_zsize), K_(payload_checksum), 
    K_(tailer_offset), K_(tailer_length), K_(compressor_type));

  int32_t header_size_;
  int32_t version_;
  int32_t payload_size_;
  int32_t payload_zsize_;
  uint32_t payload_checksum_;
  int32_t tailer_offset_;
  int32_t tailer_length_;
  union {
    uint32_t attr_;
    struct {
      uint32_t compressor_type_ : 8;
      uint32_t reserved_ : 24;
    };
  };
};

struct ObSSCkptPhyBlockItemHeader final
{
public:
  int32_t payload_size_;

  ObSSCkptPhyBlockItemHeader() : payload_size_(0) {}
  ~ObSSCkptPhyBlockItemHeader() { reset(); }
  void reset() { payload_size_ = 0; }
  // NOTICE: no need to consider compatibility, just used to calc blk_ckpt reserved phy_block count
  static int64_t get_max_serialize_size() 
  {
    return sizeof(ObSSCkptPhyBlockItemHeader) + SS_SERIALIZE_EXTRA_BUF_LEN;
  }

  TO_STRING_KV(K_(payload_size));

public:
  static const int64_t OB_SS_CKPT_PHY_BLOCK_ITEM_HEADER_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_CKPT_PHY_BLOCK_ITEM_HEADER_VERSION);
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_STRUCT_H_ */