/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_READER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_READER_H_

#include "lib/compress/ob_compressor_pool.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_struct.h"

namespace oceanbase
{
namespace common
{
class ObCompressor;
}
namespace storage
{
class ObSSPhysicalBlockManager;

class ObSSCkptPhyBlockReader final
{
public:
  ObSSCkptPhyBlockReader();
  ~ObSSCkptPhyBlockReader() { reset(); };
  ObSSCkptPhyBlockReader(const ObSSCkptPhyBlockReader &) = delete;
  ObSSCkptPhyBlockReader &operator=(const ObSSCkptPhyBlockReader &) = delete;

  int init(const int64_t entry_blk_id, const uint64_t tenant_id, ObSSPhysicalBlockManager &phy_blk_mgr);
  int read_next_block(char *&buf, int64_t &buf_data_len, int64_t &read_blk_id);
  void reset();
  
  static bool is_valid_entry_block(const int64_t entry_blk_id);

private:
  int inner_read_block_(const int64_t entry_blk_id, int64_t &payload_offset);

private:
  bool is_inited_;
  char *io_buf_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  int64_t phy_blk_size_;
  int64_t cur_blk_id_;
  ObSSPhyBlockCommonHeader common_header_;
  ObSSCkptBlockHeader ckpt_blk_header_;
  ObSSCkptPhyBlockSegmentHeader seg_header_;
};

class ObSSCkptPhyBlockItemReader final
{
public:
  ObSSCkptPhyBlockItemReader();
  ~ObSSCkptPhyBlockItemReader() { reset(); };
  ObSSCkptPhyBlockItemReader(const ObSSCkptPhyBlockItemReader &) = delete;
  ObSSCkptPhyBlockItemReader &operator=(const ObSSCkptPhyBlockItemReader &) = delete;

  int init(const int64_t entry_blk_id, const uint64_t tenant_id, ObSSPhysicalBlockManager &phy_blk_mgr);
  int init(char *buf, const int64_t buf_data_len, const int64_t phy_blk_size); // only used for ob_admin parse single ckpt block.
  int get_next_item(char *&item_buf, int64_t &item_buf_len);
  void reset();

  bool is_inited() const { return is_inited_; }
  int get_block_id_list(common::ObIArray<int64_t> &blk_id_list) const;

private:
  int read_item_block_(int64_t &read_blk_id);
  int parse_item_(char *&item_buf, int64_t &item_buf_len);
  int read_item_segment_();
  int alloc_segment_buf_(const uint64_t tenant_id, const int64_t seg_buf_size);
  int parse_segment_();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObSSCkptPhyBlockReader block_reader_;
  char *buf_;
  int64_t buf_pos_;
  int64_t buf_data_len_;
  ObSSCkptPhyBlockSegmentHeader seg_header_;
  char *seg_buf_;
  int64_t seg_buf_pos_;
  int64_t seg_buf_size_; // capacity of this buffer
  int64_t seg_data_len_; // valid data length
  common::ObCompressor *compressor_;
  common::ObCompressorType compressor_type_;
  ObArray<int64_t> blk_id_list_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_READER_H_ */