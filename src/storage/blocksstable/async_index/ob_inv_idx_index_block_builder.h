/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_INDEX_BLOCK_BUILDER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_INDEX_BLOCK_BUILDER_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/index_block/ob_clustered_index_block_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObMicroBlockData;

// Physical locator of one inverted-index data micro block inside its macro block.
// The standard index row header uses the posting locator; the posting value
// locator is serialized into the inverted-index agg data.
struct ObInvIdxMicroBlockLocator
{
  ObInvIdxMicroBlockLocator()
    : offset_(-1),
      size_(0)
  {}

  void reset()
  {
    offset_ = -1;
    size_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return offset_ >= 0 && size_ > 0;
  }
  TO_STRING_KV(K_(offset), K_(size));

  int64_t offset_;
  int64_t size_;
};

// Logical block group for one (data micro block, term) pair. Each group maps to
// one standard inverted-index row whose rowkey is (term, max_doc_id_).
struct ObInvIdxLogicalBlockGroup
{
  ObInvIdxLogicalBlockGroup()
    : term_(),
      max_doc_id_(0),
      doc_count_(0),
      logical_block_metas_()
  {}

  void reset()
  {
    term_.reset();
    max_doc_id_ = 0;
    doc_count_ = 0;
    logical_block_metas_.reset();
  }
  OB_INLINE bool is_valid() const
  {
    return term_.is_valid() && doc_count_ > 0
        && logical_block_metas_.count() > 0;
  }
  int assign(const ObInvIdxLogicalBlockGroup &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      if (OB_FAIL(logical_block_metas_.assign(other.logical_block_metas_))) {
        STORAGE_LOG(WARN, "fail to assign inverted index logical block metas", K(ret), K(other));
      } else {
        term_ = other.term_;
        max_doc_id_ = other.max_doc_id_;
        doc_count_ = other.doc_count_;
      }
    }
    return ret;
  }
  TO_STRING_KV(K_(term), K_(max_doc_id), K_(doc_count), K_(logical_block_metas));

  ObDatumRowkey term_;
  uint64_t max_doc_id_;
  int64_t doc_count_;
  common::ObSEArray<ObInvIdxLogicalBlockMeta, 4> logical_block_metas_;
};

// Index-side description of one locked posting/posting-value data micro block
// pair. It is produced by ObInvIdxMacroBlockWriter after both data micro blocks
// have been written and their physical locators are known.
struct ObInvIdxMicroBlockIndexDesc
{
  ObInvIdxMicroBlockIndexDesc()
    : posting_locator_(),
      posting_value_locator_(),
      logical_block_groups_(nullptr),
      macro_id_(),
      logic_micro_id_(),
      data_checksum_(0)
  {}

  void reset()
  {
    posting_locator_.reset();
    posting_value_locator_.reset();
    logical_block_groups_ = nullptr;
    macro_id_.reset();
    logic_micro_id_.reset();
    data_checksum_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return posting_locator_.is_valid()
        && nullptr != logical_block_groups_
        && logical_block_groups_->count() > 0;
  }
  TO_STRING_KV(K_(posting_locator), K_(posting_value_locator),
      KPC_(logical_block_groups), K_(macro_id),
      K_(logic_micro_id), K_(data_checksum));

  ObInvIdxMicroBlockLocator posting_locator_;
  ObInvIdxMicroBlockLocator posting_value_locator_;
  const common::ObIArray<ObInvIdxLogicalBlockGroup> *logical_block_groups_;
  // macro_id_ and logic_micro_id_ are intentionally not covered by is_valid(): they are only
  // filled on gated paths (pre-alloc macro id; major merge + cluster version >= 4.3.3.0 for
  // logic_micro_id_).
  MacroBlockId macro_id_;
  ObLogicMicroBlockId logic_micro_id_;
  int64_t data_checksum_;
};

struct ObInvIdxMacroBlockStats
{
  ObInvIdxMacroBlockStats()
    : endkey_(),
      doc_count_(0),
      inv_meta_()
  {}

  int assign(const ObInvIdxMacroBlockStats &other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(inv_meta_.assign(other.inv_meta_))) {
      STORAGE_LOG(WARN, "failed to assign inverted macro metadata", K(ret), K(other));
    } else {
      endkey_ = other.endkey_;
      doc_count_ = other.doc_count_;
    }
    return ret;
  }
  void reset()
  {
    endkey_.reset();
    doc_count_ = 0;
    inv_meta_.reset();
  }
  void reuse()
  {
    endkey_.reset();
    doc_count_ = 0;
    inv_meta_.reuse();
  }
  OB_INLINE bool is_valid() const
  {
    return endkey_.is_valid()
        && doc_count_ > 0
        && inv_meta_.is_valid();
  }
  TO_STRING_KV(K_(endkey), K_(doc_count), K_(inv_meta));

  ObDatumRowkey endkey_;
  int64_t doc_count_;
  ObInvMacroMeta inv_meta_;
};

class ObInvIdxIndexBlockBuilder final
{
public:
  ObInvIdxIndexBlockBuilder();
  ~ObInvIdxIndexBlockBuilder();

  int init(
      const ObDataStoreDesc &data_store_desc,
      ObSSTableIndexBuilder &sstable_builder,
      const blocksstable::ObMacroSeqParam &macro_seq_param,
      const share::ObPreWarmerParam &pre_warm_param,
      const bool write_clustered_micro_idx,
      ObIMacroBlockFlushCallback *callback,
      common::ObIAllocator &allocator);
  int estimate_index_row_size_after_append_logical_block(
      const ObInvIdxLogicalBlockGroup &projected_group,
      const int64_t posting_value_size_upper_bound,
      int64_t &estimate_size) const;
  int append_micro_block(
      const ObInvIdxMicroBlockIndexDesc &desc,
      const int64_t index_block_upper_bound,
      bool &is_appended);
  int estimate_projected_macro_meta_block_size(
      const ObInvIdxMacroBlockStats &projected_stats,
      int64_t &estimate_size);
  int write_index_and_meta_blocks(
      ObMacroBlock &macro_block,
      const MacroBlockId &macro_id,
      const ObInvIdxMacroBlockStats &macro_stats,
      const int64_t ddl_start_row_offset);
  int publish_macro_meta(const MacroBlockId &macro_id);
  int reuse_macro_scope();
  int append_macro_block(
      const ObDataMacroBlockMeta &macro_meta,
      const ObMicroBlockData *micro_block_data = nullptr);
  int close(ObMacroBlocksWriteCtx &write_ctx);
  void reset();
  static int build_index_rowkey(
      const ObDatumRowkey &term,
      const uint64_t max_doc_id,
      const int64_t snapshot_version,
      common::ObIAllocator &allocator,
      ObDatumRowkey &rowkey);

private:
  int try_append_micro_block(
      const ObInvIdxMicroBlockIndexDesc &desc,
      const int64_t index_block_upper_bound,
      bool &can_append);
  int commit_micro_block(
      const ObInvIdxMicroBlockIndexDesc &desc,
      const int64_t index_block_upper_bound);
  int build_index_row_desc(
      const ObInvIdxMicroBlockIndexDesc &micro_desc,
      const ObInvIdxLogicalBlockGroup &logical_block_group,
      const int64_t candidate_row_offset,
      ObIndexBlockRowDesc &row_desc);
  int append_index_row(
      ObIMicroBlockWriter &micro_writer,
      const ObIndexBlockRowDesc &row_desc);
  int append_clustered_index_row(const ObIndexBlockRowDesc &row_desc);

  int build_estimate_macro_meta_row(
      const ObInvIdxMacroBlockStats &projected_stats,
      ObDataMacroBlockMeta &estimate_macro_meta,
      ObDatumRow &estimate_meta_row);

  int write_index_block(
      ObMacroBlock &macro_block,
      const MacroBlockId &macro_id);
  int build_index_block(ObMicroBlockDesc &index_micro_desc);
  int prewarm_index_block(
      const MacroBlockId &macro_id,
      const ObMicroBlockDesc &uncompressed_index_micro_desc,
      ObMicroBlockDesc &prepared_index_micro_desc);
  int write_meta_block(
      ObMacroBlock &macro_block,
      const ObInvIdxMacroBlockStats &macro_stats,
      const int64_t ddl_start_row_offset);
  int generate_macro_row(
      ObMacroBlock &macro_block,
      const ObInvIdxMacroBlockStats &macro_stats,
      const int64_t ddl_start_row_offset);
  int build_meta_block(ObMicroBlockDesc &meta_micro_desc);
  int write_clustered_index_block();

  void inner_reset();

private:
  const ObDataStoreDesc *data_store_desc_;
  ObDataStoreDesc *leaf_store_desc_;
  ObDataStoreDesc *index_store_desc_;
  ObSSTableIndexBuilder *sstable_builder_;
  ObIndexTreeRootCtx *index_tree_root_ctx_;
  common::ObIAllocator *allocator_;

  compaction::ObLocalArena row_allocator_;
  compaction::ObLocalArena agg_data_allocator_;
  compaction::ObLocalArena meta_row_allocator_;

  ObIndexBlockRowBuilder row_builder_;
  ObIMicroBlockWriter *actual_index_micro_writer_;
  ObIMicroBlockWriter *trial_index_micro_writer_;
  ObIMicroBlockWriter *meta_block_writer_;
  ObClusteredIndexBlockWriter *clustered_index_writer_;

  ObMicroBlockBufferHelper index_micro_buffer_helper_;
  ObMicroBlockBufferHelper meta_micro_buffer_helper_;
  share::ObIPreWarmer *index_pre_warmer_;

  ObBaseIndexBlockDumper macro_meta_dumper_;
  ObDataMacroBlockMeta macro_meta_;
  ObDatumRow meta_row_;
  ObDatumRowkey last_rowkey_;

  int64_t row_offset_;
  int64_t index_row_count_;
  int64_t index_block_offset_;
  int64_t index_block_size_;
  int64_t meta_block_offset_;
  int64_t meta_block_size_;
  bool micro_index_clustered_;
  bool is_inited_;
  bool is_closed_;

  DISALLOW_COPY_AND_ASSIGN(ObInvIdxIndexBlockBuilder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_INDEX_BLOCK_BUILDER_H_
