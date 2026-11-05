/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_WRITER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_WRITER_H_

#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/blocksstable/async_index/ob_inv_idx_index_block_builder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_row_desc.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_buffer_helper.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_writer.h"
#include "storage/blocksstable/async_index/ob_posting_block_encoder.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObMicroBlockData;
struct ObMacroBlockDesc;

class ObInvIdxMacroBlockWriter
{
public:
  ObInvIdxMacroBlockWriter();
  ~ObInvIdxMacroBlockWriter();

  int open(
      const ObInvIdxRowDesc &inv_idx_row_desc,
      const ObDataStoreDesc &data_store_desc,
      const int64_t parallel_idx,
      const blocksstable::ObMacroSeqParam &macro_seq_param,
      const share::ObPreWarmerParam &pre_warm_param,
      ObISSTableObjectCleaner &object_cleaner,
      ObIMacroBlockFlushCallback *callback = nullptr,
      const bool write_clustered_micro_idx = true);

  int append_row(const ObDatumRow &row);
  int append_batch(const ObBatchDatumRows &rows);
  int append_logical_block(
      const ObDatumRowkey &term,
      const ObPostingBlockDesc &posting_block,
      const ObPostingValueBlockDesc &posting_value_block);
  int append_macro_block(
      const ObMacroBlockDesc &macro_desc,
      const ObInvMacroMeta &inv_macro_meta,
      const ObMicroBlockData *micro_block_data = nullptr);

  int close();
  void reset();

  ObMacroBlocksWriteCtx &get_macro_block_write_ctx() { return block_write_ctx_; }
  int64_t get_last_macro_seq() const { return last_macro_seq_; }
  bool is_opened() const { return is_opened_; }
  const compaction::ObMergeBlockInfo &get_merge_block_info() const { return merge_block_info_; }
  void inc_incremental_row_count() { ++merge_block_info_.incremental_row_count_; }
  void inc_filter_row_count() { ++merge_block_info_.filter_row_count_; }
  const ObInvSSTableMeta &get_seg_inv_meta() const { return seg_inv_meta_; }

private:
  static constexpr int64_t POSTING_VALUE_MICRO_BLOCK_SIZE_LIMIT_FACTOR = 3;
  static constexpr int64_t POSTING_MICRO_BLOCK_SIZE_LIMIT_PERCENT = 15;
  // One index row is generated per term. Keep an indivisible pair below the
  // encoder row-count limit.
  static constexpr int64_t MICRO_BLOCK_PAIR_TERM_COUNT_LIMIT = 65535;
  static constexpr int64_t MICRO_BLOCK_PAIR_INDEX_ROW_SIZE_LIMIT_PERCENT = 20;
  static constexpr int64_t INDEX_BLOCK_SIZE_LIMIT_PERCENT = 60;
  static constexpr int64_t MACRO_SPLIT_SAFETY_MARGIN_PERCENT = 10;
  static constexpr int64_t PERCENT_BASE = 100;

  struct ObInvIdxMicroBlockStreamDesc
  {
    ObInvIdxMicroBlockStreamDesc();
    void reset();
    bool is_valid() const;

    ObRawMicroBlockDesc raw_desc_;
    ObInvIdxMicroBlockLocator locator_;
    const char *uncompressed_payload_;
    int64_t uncompressed_payload_size_;

    DISALLOW_COPY_AND_ASSIGN(ObInvIdxMicroBlockStreamDesc);
  };

  struct ObInvIdxMicroBlockPairDesc
  {
    ObInvIdxMicroBlockPairDesc();
    void reset();
    bool is_valid() const;

    ObInvIdxMicroBlockStreamDesc posting_;
    ObInvIdxMicroBlockStreamDesc posting_value_;
    ObInvIdxMicroBlockIndexDesc index_desc_;
    int64_t index_block_upper_bound_;

    DISALLOW_COPY_AND_ASSIGN(ObInvIdxMicroBlockPairDesc);
  };

  class ObInvIdxDefaultMacroBlockFlusher final : public ObIMacroBlockFlusher
  {
  public:
    ObInvIdxDefaultMacroBlockFlusher();
    virtual ~ObInvIdxDefaultMacroBlockFlusher();
    void set_param(
        const ObDataStoreDesc &data_store_desc,
        ObStorageObjectHandle &macro_handle,
        ObIODevice *device_handle);
    virtual int write_disk(ObMacroBlock &macro_block, const bool is_close_flush) override;

  private:
    const ObDataStoreDesc *data_store_desc_;
    ObStorageObjectHandle *macro_handle_;
    ObIODevice *device_handle_;
  };

private:
  bool is_pre_alloc() const;
  int init_macro_seq_generator(const blocksstable::ObMacroSeqParam &macro_seq_param);
  int init_inv_meta();

  int inner_append_row(const ObDatumRow &row);
  int extract_term(const ObDatumRow &row, ObDatumRowkey &term) const;
  int check_term_order(const ObDatumRowkey &term) const;
  int compare_term(const ObDatumRowkey &left, const ObDatumRowkey &right, int &cmp_ret) const;

  int flush_logical_block();
  int append_encoded_logical_block(
      const ObPostingBlockDesc &posting_block,
      const ObPostingValueBlockDesc &posting_value_block);
  int check_logical_block_order(const ObInvIdxLogicalBlockMeta &meta, bool &same_term) const;
  int finalize_term_group();
  int need_flush_micro_block_pair(
      const ObPostingBlockDesc &posting_block,
      const ObPostingValueBlockDesc &posting_value_block,
      const ObInvIdxLogicalBlockMeta &meta,
      bool &need_flush,
      int64_t &projected_pending_index_size);
  int append_logical_block_meta(
      const ObInvIdxLogicalBlockMeta &meta,
      const int64_t projected_pending_index_size);

  int flush_micro_block_pair();
  int build_micro_block_pair(ObInvIdxMicroBlockPairDesc &pair_desc);
  int prepare_micro_block_pair(ObInvIdxMicroBlockPairDesc &pair_desc);
  int prepare_micro_block_stream(
      ObInvIdxMicroBlockBufferHelper &buffer_helper,
      const bool is_posting_value,
      const int64_t macro_seq,
      const int64_t block_offset,
      ObInvIdxMicroBlockStreamDesc &stream_desc);
  int build_micro_block_header(
      const ObRawMicroBlockDesc &raw_desc,
      const bool is_posting_value,
      ObMicroBlockHeader &header,
      int64_t &header_size);
  int estimate_projected_macro_meta_block_size(int64_t &estimate_size);
  int commit_micro_block_pair(const ObInvIdxMicroBlockPairDesc &pair_desc);
  int write_micro_block(const ObInvIdxMicroBlockStreamDesc &stream_desc);

  int flush_macro_block(const bool is_close_flush);
  int switch_to_next_macro_block();
  int wait_macro_block_io();
  int exec_callback();
  void inner_reset();

  int inner_append_macro_block(
      const ObDataMacroBlockMeta &macro_meta,
      const ObInvMacroMeta &inv_macro_meta,
      const ObMicroBlockData *micro_block_data);
  int flush_reuse_macro_block(
      const ObDataMacroBlockMeta &macro_meta,
      const ObInvMacroMeta &inv_macro_meta);

  int alloc_block();
  int build_logic_macro_seq(const int64_t physical_macro_seq, int64_t &logic_macro_seq) const;
  void gen_logic_macro_id(ObLogicMacroBlockId &logic_macro_id);

  OB_INLINE bool has_posting_value() const
  { return inv_idx_row_desc_.has_posting_value(); }
private:
  const ObDataStoreDesc *data_store_desc_;
  ObInvIdxRowDesc inv_idx_row_desc_;
  int16_t term_col_count_;
  int64_t parallel_idx_;
  ObISSTableObjectCleaner *object_cleaner_;
  ObIMacroBlockFlushCallback *callback_;
  bool is_opened_;

  ObPostingBlockEncodingCtx posting_ctx_;
  ObPostingValueBlockWriteParam posting_value_param_;
  ObPostingBlockEncoder posting_encoder_;
  ObPostingValueBlockWriter posting_value_writer_;
  ObInvIdxMicroBlockWriter posting_micro_writer_;
  ObInvIdxMicroBlockWriter posting_value_micro_writer_;
  ObInvIdxMicroBlockBufferHelper posting_micro_buffer_helper_;
  ObInvIdxMicroBlockBufferHelper posting_value_micro_buffer_helper_;

  ObInvIdxIndexBlockBuilder index_block_builder_;

  ObMacroBlock macro_block_;
  ObStorageObjectHandle macro_handle_;
  ObIODevice *device_handle_;
  blocksstable::ObMacroSeqGenerator *macro_seq_generator_;
  int64_t last_macro_seq_;
  ObMacroBlocksWriteCtx block_write_ctx_;
  compaction::ObMergeBlockInfo merge_block_info_;

  ObDatumRowkey current_term_;
  bool has_pending_rows_;
  common::ObSEArray<ObInvIdxLogicalBlockGroup, 4> micro_term_groups_;
  ObInvIdxLogicalBlockGroup pending_term_group_;
  int64_t micro_index_size_;
  int64_t pending_index_size_;
  ObInvIdxMacroBlockStats current_micro_stats_;

  ObInvIdxMacroBlockStats macro_stats_;
  ObInvSSTableMeta seg_inv_meta_;

  volatile bool concurrent_lock_;
  compaction::ObLocalArena allocator_;
  compaction::ObLocalArena current_term_allocator_;
  compaction::ObLocalArena candidate_allocator_;
  compaction::ObLocalArena macro_allocator_;
  compaction::ObLocalArena scratch_allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObInvIdxMacroBlockWriter);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MACRO_BLOCK_WRITER_H_
