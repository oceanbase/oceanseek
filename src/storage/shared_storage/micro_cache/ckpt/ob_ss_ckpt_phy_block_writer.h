/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_WRITER_H_

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
/*
 * Usage: ObSSCkptPhyBlockItemWriter -> init -> write_item -> close -> get_entry_block
 *        ObSSCkptPhyBlockItemWriter -> init -> get_next_item (until get error code OB_ITER_END)
 * 
 * The ObSSCkptPhyBlockWriter/Reader classes are designed to
 * handle the writing/reading of data blocks to/from the disk.
 * The Writer class is responsible for writing data to the disk
 * once the in-memory block is filled with items,
 * while the Reader class facilitates the retrieval of these blocks from the disk into memory.
 * These classes are intended for internal operation and should not be directly manipulated by the user.
 * 
 */
class ObSSCkptPhyBlockWriter final
{
public:
  ObSSCkptPhyBlockWriter();
  ~ObSSCkptPhyBlockWriter() { reset(); }
  ObSSCkptPhyBlockWriter(const ObSSCkptPhyBlockWriter &) = delete;
  ObSSCkptPhyBlockWriter &operator=(const ObSSCkptPhyBlockWriter &) = delete;

  int init(ObSSPhysicalBlockManager &phy_blk_mgr, const ObSSPhyBlockType block_type);
  int write_block(const char *buf, const int64_t buf_len, common::ObArray<int64_t> &blk_id_list, 
                  common::ObArray<int64_t> &failed_blk_id_list, int64_t &phy_blk_id);
  void reset();

private:
  bool is_inited_;
  int64_t phy_blk_size_;
  ObSSPhyBlockType blk_type_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
};

class ObSSCkptPhyBlockItemWriter final
{
public:
  ObSSCkptPhyBlockItemWriter();
  ~ObSSCkptPhyBlockItemWriter() { reset(); }
  ObSSCkptPhyBlockItemWriter(const ObSSCkptPhyBlockItemWriter &) = delete;
  ObSSCkptPhyBlockItemWriter &operator=(const ObSSCkptPhyBlockItemWriter &) = delete;

  int init(const uint64_t tenant_id, ObSSPhysicalBlockManager &phy_blk_mgr, const ObSSPhyBlockType blk_type,
           const common::ObCompressorType compressor_type = ObCompressorType::INVALID_COMPRESSOR);
  int write_item(const char *item_buf, const int64_t item_buf_len);
  int close();
  int close_segment(bool &persist_prev_seg, int64_t &seg_offset, int64_t &seg_len);
  void reset();

  // must be called after closed
  int get_entry_block(int64_t &entry_blk_id) const;
  int get_block_id_list(common::ObIArray<int64_t> &blk_id_list, const bool is_failed_blk_list = false) const;
  int64_t get_latest_block_id() const;

private:
  int init_compressor_info_(const uint64_t tenant_id, const int64_t phy_blk_size, 
                            const common::ObCompressorType compressor_type);
  void reuse_for_next_write_(const int64_t pre_block_id);
  int write_block_();
  int write_item_(const ObSSCkptPhyBlockItemHeader &item_header, const char *item_buf, const int64_t item_buf_len);
  int write_segment_();
  int write_segment_(bool &persist_prev_seg, int64_t &seg_offset, int64_t &seg_len);
  int do_build_seg_header_(ObSSCkptPhyBlockSegmentHeader &seg_header);
  int do_write_segment_(const ObSSCkptPhyBlockSegmentHeader &seg_header, 
                        char *item_buf, const int64_t item_buf_len);
  int64_t get_remaining_size() const { return io_buf_size_ - io_buf_pos_; }
  int64_t get_seg_remaining_size() const { return io_seg_buf_size_ - io_seg_buf_pos_; }

private:
  bool is_inited_;
  bool is_closed_;
  ObSSPhyBlockType blk_type_;
  ObSSPhysicalBlockManager *phy_blk_mgr_;
  int64_t seg_items_cnt_;    // count of items which were written into segment
  int64_t written_items_cnt_;// count of items which were written into whole io_buf
  int64_t segs_cnt_;

  ObSSCkptPhyBlockWriter block_writer_;

  // its layout just like below:
  // |-common header-|-ckpt blk header-|-seg header-|-seg payload-|-......-|
  char *io_buf_;
  int64_t io_buf_size_;
  int64_t io_buf_pos_;
  // its layout just like below:
  // |-seg header-|-seg payload-|
  char *io_seg_buf_;
  int64_t io_seg_buf_size_;
  int64_t io_seg_buf_pos_;
  // its layout just like below:
  // |-seg header-|-seg compressed payload-|
  char *io_seg_comp_buf_;
  int64_t io_seg_comp_buf_size_;
  ObSSPhyBlockCommonHeader common_header_;
  ObSSCkptBlockHeader ckpt_blk_header_;
  ObArray<int64_t> blk_id_list_;
  ObArray<int64_t> failed_blk_id_list_;
  common::ObCompressor *compressor_;
  common::ObCompressorType compressor_type_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CKPT_PHY_BLOCK_WRITER_H_ */