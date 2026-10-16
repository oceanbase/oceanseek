/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_index_block_builder.h"

namespace oceanbase
{
namespace blocksstable
{

ObInvIdxIndexBlockBuilder::ObInvIdxIndexBlockBuilder()
  : data_store_desc_(nullptr),
    leaf_store_desc_(nullptr),
    index_store_desc_(nullptr),
    sstable_builder_(nullptr),
    index_tree_root_ctx_(nullptr),
    allocator_(nullptr),
    row_allocator_("InvIdxBlkRow"),
    agg_data_allocator_("InvIdxBlkData"),
    meta_row_allocator_("InvIdxBlkMeta"),
    row_builder_(),
    actual_index_micro_writer_(nullptr),
    trial_index_micro_writer_(nullptr),
    meta_block_writer_(nullptr),
    clustered_index_writer_(nullptr),
    index_micro_buffer_helper_(),
    meta_micro_buffer_helper_(),
    index_pre_warmer_(nullptr),
    macro_meta_dumper_(),
    macro_meta_(),
    meta_row_(),
    last_rowkey_(),
    row_offset_(-1),
    index_row_count_(0),
    index_block_offset_(0),
    index_block_size_(0),
    meta_block_offset_(0),
    meta_block_size_(0),
    micro_index_clustered_(false),
    is_inited_(false),
    is_closed_(false)
{}

ObInvIdxIndexBlockBuilder::~ObInvIdxIndexBlockBuilder()
{
  reset();
}

int ObInvIdxIndexBlockBuilder::init(
    const ObDataStoreDesc &data_store_desc,
    ObSSTableIndexBuilder &sstable_builder,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    const share::ObPreWarmerParam &pre_warm_param,
    const bool write_clustered_micro_idx,
    ObIMacroBlockFlushCallback *callback,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc *tmp_data_store_desc = nullptr;
  ObDataStoreDesc *index_store_desc = nullptr;
  ObDataStoreDesc *container_store_desc = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "is inited", K(ret));
  } else if (FALSE_IT(data_store_desc_ = &data_store_desc)) {
  } else if (FALSE_IT(allocator_ = &allocator)) {
  } else if (FALSE_IT(micro_index_clustered_ =
      data_store_desc.micro_index_clustered() && write_clustered_micro_idx)) {
  } else if (OB_FAIL(sstable_builder.init_builder_ptrs(
      sstable_builder_,
      tmp_data_store_desc,
      index_store_desc,
      leaf_store_desc_,
      container_store_desc,
      index_tree_root_ctx_))) {
    STORAGE_LOG(WARN, "fail to init reference pointer members", K(ret));
  } else if (OB_FAIL(index_micro_buffer_helper_.open(*index_store_desc, allocator))) {
    STORAGE_LOG(WARN, "fail to open index micro block buffer helper", K(ret),
        KPC(index_store_desc));
  } else if (OB_FAIL(meta_micro_buffer_helper_.open(*index_store_desc, allocator))) {
    STORAGE_LOG(WARN, "fail to open meta micro block buffer helper", K(ret),
        KPC(index_store_desc));
  } else if (OB_FAIL(meta_row_.init(
      allocator, index_store_desc->get_row_column_count()))) {
    STORAGE_LOG(WARN, "fail to init meta row", K(ret),
        K(index_store_desc->get_row_column_count()));
  } else if (OB_FAIL(row_builder_.init(allocator, data_store_desc, *leaf_store_desc_))) {
    STORAGE_LOG(WARN, "fail to init index row builder", K(ret),
        K(data_store_desc), KPC_(leaf_store_desc));
  } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(
      leaf_store_desc_, allocator, actual_index_micro_writer_))) {
    STORAGE_LOG(WARN, "fail to build actual index micro writer", K(ret),
        KPC_(leaf_store_desc));
  } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(
      leaf_store_desc_, allocator, trial_index_micro_writer_))) {
    STORAGE_LOG(WARN, "fail to build trial index micro writer", K(ret),
        KPC_(leaf_store_desc));
  } else if (OB_FAIL(ObMacroBlockWriter::build_micro_writer(
      index_store_desc, allocator, meta_block_writer_))) {
    STORAGE_LOG(WARN, "fail to build macro meta block writer", K(ret),
        KPC(index_store_desc));
  } else if (OB_FAIL(macro_meta_dumper_.init(
      *index_store_desc,
      *container_store_desc,
      &sstable_builder,
      *index_tree_root_ctx_->allocator_,
      allocator,
      !index_store_desc->is_cg(),
      sstable_builder_->enable_dump_disk()))) {
    STORAGE_LOG(WARN, "fail to init macro meta dumper", K(ret),
        KPC(index_store_desc), KPC(container_store_desc));
  } else if (OB_FAIL(ObMacroBlockWriter::build_pre_warmer(
      pre_warm_param,
      true /*is_for_index*/,
      index_store_desc->get_tablet_id().is_user_tablet(),
      allocator,
      index_pre_warmer_))) {
    STORAGE_LOG(WARN, "fail to build index micro block pre warmer",
        K(ret), K(pre_warm_param));
  } else if (micro_index_clustered_) {
    if (OB_ISNULL(clustered_index_writer_ = OB_NEWx(ObClusteredIndexBlockWriter, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc clustered index block writer", K(ret));
    } else if (OB_FAIL(clustered_index_writer_->init(
        data_store_desc,
        *leaf_store_desc_,
        macro_seq_param,
        pre_warm_param,
        index_tree_root_ctx_,
        *allocator_,
        callback))) {
      STORAGE_LOG(WARN, "fail to init clustered index block writer", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    index_store_desc_ = index_store_desc;
    index_tree_root_ctx_->task_type_ = index_store_desc->is_cg()
        ? ObIndexBuildTaskType::MERGE_CG_TASK
        : ObIndexBuildTaskType::MERGE_TASK;
    is_inited_ = true;
    is_closed_ = false;
  } else {
    inner_reset();
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::estimate_index_row_size_after_append_logical_block(
    const ObInvIdxLogicalBlockGroup &projected_group,
    const int64_t posting_value_size_upper_bound,
    int64_t &estimate_size) const
{
  int ret = OB_SUCCESS;
  int64_t agg_data_size = 0;
  const int64_t extra_rowkey_cnt =
      storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t fixed_index_row_size =
      sizeof(ObIndexBlockRowHeader) + sizeof(ObIndexBlockRowMinorMetaInfo) + sizeof(int64_t);
  estimate_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "builder is closed", K(ret));
  } else if (OB_UNLIKELY(!projected_group.is_valid()
      || posting_value_size_upper_bound < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(projected_group),
        K(posting_value_size_upper_bound));
  } else if (OB_FAIL(ObInvIdxAggDataEncoder::estimate_size(
      0 /*posting_value_offset*/, posting_value_size_upper_bound,
      projected_group.logical_block_metas_, agg_data_size))) {
    STORAGE_LOG(WARN, "fail to estimate inverted index agg data size", K(ret),
        K(posting_value_size_upper_bound), K(projected_group));
  } else {
    estimate_size = fixed_index_row_size
        + agg_data_size
        + projected_group.term_.get_deep_copy_size()
        + (1 + extra_rowkey_cnt) * sizeof(ObStorageDatum)
        + sizeof(int32_t);
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::append_micro_block(
    const ObInvIdxMicroBlockIndexDesc &desc,
    const int64_t index_block_upper_bound,
    bool &is_appended)
{
  int ret = OB_SUCCESS;
  bool can_append = false;
  is_appended = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "builder is closed", K(ret));
  } else if (OB_FAIL(try_append_micro_block(desc, index_block_upper_bound, can_append))) {
    STORAGE_LOG(WARN, "fail to try append micro block", K(ret),
        K(desc), K(index_block_upper_bound));
  } else if (!can_append) {
  } else if (OB_FAIL(commit_micro_block(desc, index_block_upper_bound))) {
    STORAGE_LOG(WARN, "fail to commit micro block", K(ret),
        K(desc), K(index_block_upper_bound));
  } else {
    is_appended = true;
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::try_append_micro_block(
    const ObInvIdxMicroBlockIndexDesc &desc,
    const int64_t index_block_upper_bound,
    bool &can_append)
{
  int ret = OB_SUCCESS;
  int64_t candidate_row_offset = row_offset_;
  can_append = false;
  if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid micro block index desc", K(ret), K(desc));
  } else if (index_block_upper_bound <= 0) {
  } else {
    trial_index_micro_writer_->set_block_size_upper_bound(index_block_upper_bound);
    can_append = true;
    for (int64_t i = 0;
        OB_SUCC(ret) && can_append && i < desc.logical_block_groups_->count();
        ++i) {
      const ObInvIdxLogicalBlockGroup &logical_block_group = desc.logical_block_groups_->at(i);
      ObIndexBlockRowDesc row_desc;
      if (OB_UNLIKELY(!logical_block_group.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid logical block group", K(ret), K(i), K(logical_block_group));
      } else if (FALSE_IT(candidate_row_offset += logical_block_group.doc_count_)) {
      } else if (OB_FAIL(build_index_row_desc(
          desc, logical_block_group, candidate_row_offset, row_desc))) {
        STORAGE_LOG(WARN, "fail to build trial inverted index row", K(ret),
            K(i), K(desc), K(logical_block_group), K(candidate_row_offset));
      } else if (OB_FAIL(append_index_row(*trial_index_micro_writer_, row_desc))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          ret = OB_SUCCESS;
          can_append = false;
        } else {
          STORAGE_LOG(WARN, "fail to append trial inverted index row", K(ret), K(i), K(row_desc));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    can_append = false;
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::commit_micro_block(
    const ObInvIdxMicroBlockIndexDesc &desc,
    const int64_t index_block_upper_bound)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!desc.is_valid() || index_block_upper_bound <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(desc), K(index_block_upper_bound));
  } else if (OB_UNLIKELY(actual_index_micro_writer_->get_row_count() != index_row_count_
      || trial_index_micro_writer_->get_row_count()
          != actual_index_micro_writer_->get_row_count() + desc.logical_block_groups_->count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected actual or trial index writer state before commit", K(ret),
        "actual_row_count", actual_index_micro_writer_->get_row_count(),
        "trial_row_count", trial_index_micro_writer_->get_row_count(),
        K_(index_row_count), "candidate_row_count", desc.logical_block_groups_->count());
  } else {
    actual_index_micro_writer_->set_block_size_upper_bound(index_block_upper_bound);
    for (int64_t i = 0; OB_SUCC(ret) && i < desc.logical_block_groups_->count(); ++i) {
      const ObInvIdxLogicalBlockGroup &logical_block_group = desc.logical_block_groups_->at(i);
      ObIndexBlockRowDesc row_desc;
      if (OB_UNLIKELY(!logical_block_group.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid logical block group", K(ret), K(i), K(logical_block_group));
      } else if (FALSE_IT(row_offset_ += logical_block_group.doc_count_)) {
      } else if (OB_FAIL(build_index_row_desc(
          desc, logical_block_group, row_offset_, row_desc))) {
        STORAGE_LOG(WARN, "fail to build committed inverted index row", K(ret),
            K(i), K(desc), K(logical_block_group), K_(row_offset));
      } else if (OB_FAIL(append_index_row(*actual_index_micro_writer_, row_desc))) {
        STORAGE_LOG(WARN, "fail to append committed inverted index row", K(ret), K(i), K(row_desc));
      } else if (OB_FAIL(append_clustered_index_row(row_desc))) {
        STORAGE_LOG(WARN, "fail to append clustered inverted index row", K(ret), K(i), K(row_desc));
      } else if (OB_FAIL(row_desc.row_key_.deep_copy(last_rowkey_, row_allocator_))) {
        STORAGE_LOG(WARN, "fail to deep copy committed last rowkey", K(ret), K(i), K(row_desc));
      } else {
        ++index_row_count_;
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t actual_row_count = actual_index_micro_writer_->get_row_count();
    const int64_t trial_row_count = trial_index_micro_writer_->get_row_count();
    const int64_t actual_block_size = actual_index_micro_writer_->get_block_size();
    const int64_t trial_block_size = trial_index_micro_writer_->get_block_size();
    const int64_t actual_original_size = actual_index_micro_writer_->get_original_size();
    const int64_t trial_original_size = trial_index_micro_writer_->get_original_size();
    if (OB_UNLIKELY(actual_row_count != trial_row_count
        || actual_row_count != index_row_count_
        || actual_block_size != trial_block_size
        || actual_original_size != trial_original_size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "actual and trial index writer state mismatch after commit", K(ret),
          K(actual_row_count), K(trial_row_count), K_(index_row_count),
          K(actual_block_size), K(trial_block_size),
          K(actual_original_size), K(trial_original_size));
    }
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::build_index_row_desc(
    const ObInvIdxMicroBlockIndexDesc &micro_desc,
    const ObInvIdxLogicalBlockGroup &logical_block_group,
    const int64_t row_offset,
    ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  char *agg_data = nullptr;
  int64_t agg_data_size = 0;
  ObDatumRowkey rowkey;
  const int64_t posting_value_offset = micro_desc.posting_value_locator_.is_valid()
      ? micro_desc.posting_value_locator_.offset_
      : 0;
  const int64_t posting_value_size = micro_desc.posting_value_locator_.is_valid()
      ? micro_desc.posting_value_locator_.size_
      : 0;
  if (OB_UNLIKELY(!micro_desc.is_valid() || !logical_block_group.is_valid()
      || row_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(micro_desc), K(logical_block_group),
        K(row_offset));
  } else if (FALSE_IT(agg_data_allocator_.reuse())) {
  } else if (OB_FAIL(ObInvIdxAggDataEncoder::build(
      agg_data_allocator_,
      posting_value_offset,
      posting_value_size,
      logical_block_group.logical_block_metas_,
      agg_data,
      agg_data_size))) {
    STORAGE_LOG(WARN, "fail to build inverted index agg data", K(ret),
        K(micro_desc.posting_value_locator_), K(logical_block_group));
  } else if (OB_FAIL(build_index_rowkey(
      logical_block_group.term_,
      logical_block_group.max_doc_id_,
      data_store_desc_->get_snapshot_version(),
      row_allocator_,
      rowkey))) {
    STORAGE_LOG(WARN, "fail to build inverted index rowkey", K(ret), K(logical_block_group));
  } else {
    row_desc = ObIndexBlockRowDesc(*data_store_desc_);
    row_desc.set_row_store_type(common::INV_IDX_ROW_STORE);
    row_desc.row_key_ = rowkey;
    row_desc.macro_id_ = micro_desc.macro_id_;
    row_desc.block_offset_ = micro_desc.posting_locator_.offset_;
    row_desc.block_size_ = micro_desc.posting_locator_.size_;
    row_desc.row_count_ = logical_block_group.doc_count_;
    row_desc.row_offset_ = row_offset;
    row_desc.is_data_block_ = true;
    row_desc.micro_block_count_ = 1;
    row_desc.max_merged_trans_version_ = data_store_desc_->get_snapshot_version();
    row_desc.logic_micro_id_ = micro_desc.logic_micro_id_;
    row_desc.data_checksum_ = micro_desc.data_checksum_;
    row_desc.agg_type_ = ObIndexRowAggType::INVERTED_INDEX_AGG;
    row_desc.inverted_index_agg_.buf_ = agg_data;
    row_desc.inverted_index_agg_.size_ = agg_data_size;
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::build_index_rowkey(
    const ObDatumRowkey &term,
    const uint64_t max_doc_id,
    const int64_t snapshot_version,
    common::ObIAllocator &allocator,
    ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObStorageDatum *datums = nullptr;
  const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t term_datum_cnt = term.get_datum_cnt();
  const int64_t rowkey_datum_cnt = term_datum_cnt + 1 + extra_rowkey_cnt;
  const int64_t buf_size = term.get_deep_copy_size()
      + (1 + extra_rowkey_cnt) * sizeof(ObStorageDatum);
  int64_t pos = sizeof(ObStorageDatum) * rowkey_datum_cnt;
  rowkey.reset();
  if (OB_ISNULL(buf = allocator.alloc(buf_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc rowkey datums", K(ret), K(buf_size), K(rowkey_datum_cnt));
  } else {
    datums = new (buf) ObStorageDatum[rowkey_datum_cnt]();
    for (int64_t i = 0; OB_SUCC(ret) && i < term_datum_cnt; ++i) {
      if (OB_FAIL(datums[i].deep_copy(term.get_datum(i),
          static_cast<char *>(buf), buf_size, pos))) {
        STORAGE_LOG(WARN, "fail to deep copy term datum", K(ret), K(i), K(term));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      datums[term_datum_cnt].set_uint(max_doc_id);
      datums[term_datum_cnt + 1].set_int(-snapshot_version);   // trans_version
      datums[term_datum_cnt + 2].set_int(0);                   // sql_sequence
      if (OB_FAIL(rowkey.assign(datums, rowkey_datum_cnt))) {
        STORAGE_LOG(WARN, "fail to assign inverted index rowkey", K(ret), K(rowkey_datum_cnt));
      }
    }
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::append_index_row(
    ObIMicroBlockWriter &micro_writer,
    const ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  const ObDatumRow *row_to_append = nullptr;
  if (OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inverted index row desc", K(ret), K(row_desc));
  } else if (OB_FAIL(row_builder_.build_row(row_desc, row_to_append))) {
    STORAGE_LOG(WARN, "fail to build inverted index row", K(ret), K(row_desc));
  } else if (OB_FAIL(micro_writer.append_row(*row_to_append))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      STORAGE_LOG(WARN, "fail to append inverted index row to micro writer",
          K(ret), KPC(row_to_append));
    }
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::append_clustered_index_row(
    const ObIndexBlockRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (!micro_index_clustered_) {
    // skip
  } else if (OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inverted index row desc", K(ret), K(row_desc));
  } else {
    ObIndexBlockRowDesc clustered_row_desc = row_desc;
    clustered_row_desc.set_for_clustered_index();
    if (OB_FAIL(clustered_index_writer_->append_row(clustered_row_desc))) {
      STORAGE_LOG(WARN, "fail to append clustered index row", K(ret), K(row_desc));
    }
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::estimate_projected_macro_meta_block_size(
    const ObInvIdxMacroBlockStats &projected_stats,
    int64_t &estimate_size)
{
  int ret = OB_SUCCESS;
  int64_t original_size = 0;
  ObMicroBlockDesc estimate_meta_desc;
  estimate_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "builder is closed", K(ret));
  } else if (OB_UNLIKELY(macro_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "macro meta is pending publication", K(ret), K_(macro_meta));
  } else if (OB_UNLIKELY(!projected_stats.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid projected macro block stats", K(ret), K(projected_stats));
  } else {
    ObDataMacroBlockMeta estimate_macro_meta;
    meta_block_writer_->reuse();
    meta_row_.reuse();
    meta_row_allocator_.reuse();
    if (OB_FAIL(build_estimate_macro_meta_row(
        projected_stats, estimate_macro_meta, meta_row_))) {
      STORAGE_LOG(WARN, "fail to build estimate macro meta row", K(ret), K(projected_stats));
    } else if (OB_FAIL(meta_block_writer_->append_row(meta_row_))) {
      STORAGE_LOG(WARN, "fail to append estimate macro meta row", K(ret), K_(meta_row));
    } else if (FALSE_IT(original_size = meta_block_writer_->get_original_size())) {
    } else if (OB_FAIL(meta_block_writer_->build_micro_block_desc(estimate_meta_desc))) {
      STORAGE_LOG(WARN, "fail to build estimate macro meta block", K(ret), K_(meta_row));
    } else if (FALSE_IT(estimate_meta_desc.last_rowkey_ = estimate_macro_meta.end_key_)) {
    } else if (OB_UNLIKELY(!estimate_meta_desc.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "built invalid estimate macro meta block", K(ret), K(estimate_meta_desc));
    } else {
      estimate_size = MAX(estimate_meta_desc.get_block_size(),
          original_size + estimate_meta_desc.header_->header_size_);
#ifdef OB_BUILD_TDE_SECURITY
      const int64_t encrypted_size = share::ObEncryptionUtil::encrypted_length(
          static_cast<share::ObCipherOpMode>(index_store_desc_->get_encrypt_id()), estimate_size);
      estimate_size = MAX(estimate_size, encrypted_size);
#endif
    }
    meta_block_writer_->reuse();
    meta_row_.reuse();
    meta_row_allocator_.reuse();
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::build_estimate_macro_meta_row(
    const ObInvIdxMacroBlockStats &projected_stats,
    ObDataMacroBlockMeta &estimate_macro_meta,
    ObDatumRow &estimate_meta_row)
{
  int ret = OB_SUCCESS;
  char *agg_buf = nullptr;
  int64_t agg_size = 0;
  int64_t agg_pos = 0;
  const int64_t rowkey_count = projected_stats.endkey_.get_datum_cnt();
  uint64_t data_version = 0;
  estimate_macro_meta.reset();
  if (OB_UNLIKELY(rowkey_count != estimate_meta_row.count_ - 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected estimate meta rowkey count", K(ret),
        K(rowkey_count), K(estimate_meta_row), K(projected_stats));
  } else if (FALSE_IT(data_version = ObBaseIndexBlockBuilder::get_data_version(*leaf_store_desc_))) {
  } else if (OB_FAIL(projected_stats.endkey_.deep_copy(
      estimate_macro_meta.end_key_, meta_row_allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy projected macro endkey", K(ret), K(projected_stats));
  } else {
    estimate_macro_meta.val_.rowkey_count_ = rowkey_count;
    estimate_macro_meta.val_.column_count_ = data_store_desc_->get_row_column_count();
    estimate_macro_meta.val_.block_offset_ = INT64_MAX;
    estimate_macro_meta.val_.block_size_ = INT64_MAX;
    estimate_macro_meta.val_.row_count_ = projected_stats.doc_count_;
    estimate_macro_meta.val_.max_merged_trans_version_ = data_store_desc_->get_snapshot_version();
    estimate_macro_meta.val_.compressor_type_ = data_store_desc_->get_compressor_type();
    estimate_macro_meta.val_.row_store_type_ = common::INV_IDX_ROW_STORE;
    estimate_macro_meta.val_.snapshot_version_ = data_store_desc_->get_snapshot_version();
    estimate_macro_meta.val_.logic_id_.logic_version_ = data_store_desc_->get_logical_version();
    estimate_macro_meta.val_.logic_id_.column_group_idx_ = data_store_desc_->get_table_cg_idx();
    estimate_macro_meta.val_.logic_id_.tablet_id_ = data_store_desc_->get_tablet_id().id();
    estimate_macro_meta.val_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID;
    agg_size = projected_stats.inv_meta_.get_serialize_size();
    if (OB_UNLIKELY(agg_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected projected inv meta serialize size", K(ret),
          K(agg_size), K(projected_stats));
    } else if (OB_ISNULL(agg_buf = static_cast<char *>(
        meta_row_allocator_.alloc(agg_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc estimate inv macro meta buffer", K(ret), K(agg_size));
    } else if (OB_FAIL(projected_stats.inv_meta_.serialize(agg_buf, agg_size, agg_pos))) {
      STORAGE_LOG(WARN, "fail to serialize projected inv macro meta", K(ret),
          K(agg_size), K(projected_stats));
    } else if (OB_UNLIKELY(agg_pos != agg_size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected projected inv macro meta serialize size", K(ret),
          K(agg_pos), K(agg_size), K(projected_stats));
    } else {
      estimate_macro_meta.val_.agg_row_buf_ = agg_buf;
      estimate_macro_meta.val_.agg_row_len_ = agg_size;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(estimate_macro_meta.build_estimate_row(
      estimate_meta_row, meta_row_allocator_, data_version))) {
    STORAGE_LOG(WARN, "fail to build estimate macro meta row", K(ret),
        K(estimate_macro_meta), K(data_version));
  } else {
    estimate_meta_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::write_index_and_meta_blocks(
    ObMacroBlock &macro_block,
    const MacroBlockId &macro_id,
    const ObInvIdxMacroBlockStats &macro_stats,
    const int64_t ddl_start_row_offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "builder is closed", K(ret));
  } else if (OB_UNLIKELY(!macro_block.is_dirty()
      || !macro_id.is_valid()
      || !macro_stats.is_valid()
      || ddl_start_row_offset < -1)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(macro_block.is_dirty()),
        K(macro_id), K(macro_stats), K(ddl_start_row_offset));
  } else if (OB_UNLIKELY(row_offset_ + 1 != macro_stats.doc_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected final inverted index builder state", K(ret),
        K_(row_offset), K(macro_stats));
  } else if (OB_FAIL(write_index_block(macro_block, macro_id))) {
    STORAGE_LOG(WARN, "fail to write final index micro block", K(ret), K(macro_id));
  } else if (OB_FAIL(write_meta_block(macro_block, macro_stats, ddl_start_row_offset))) {
    STORAGE_LOG(WARN, "fail to write final macro meta block", K(ret), K(macro_stats));
  } else if (OB_FAIL(write_clustered_index_block())) {
    STORAGE_LOG(WARN, "fail to write clustered index micro block", K(ret));
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::write_index_block(
    ObMacroBlock &macro_block,
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t actual_index_offset = 0;
  ObMicroBlockDesc index_micro_desc;
  ObMicroBlockDesc uncompressed_index_micro_desc;
  if (OB_FAIL(build_index_block(index_micro_desc))) {
    STORAGE_LOG(WARN, "fail to build final index micro block", K(ret));
  } else if (OB_NOT_NULL(index_pre_warmer_)
      && OB_TMP_FAIL(index_micro_desc.deep_copy(row_allocator_, uncompressed_index_micro_desc))) {
    STORAGE_LOG(WARN, "fail to preserve uncompressed index micro block for prewarm",
        K(tmp_ret), K(index_micro_desc));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(index_micro_buffer_helper_.compress_encrypt_micro_block(
      index_micro_desc,
      macro_block.get_current_macro_seq(),
      macro_block.get_data_size()))) {
    STORAGE_LOG(WARN, "fail to compress and encrypt final index micro block", K(ret),
        K(index_micro_desc));
  } else if (OB_FAIL(macro_block.write_index_micro_block(
      index_micro_desc, true /*is_leaf_index_block*/, actual_index_offset))) {
    STORAGE_LOG(WARN, "fail to write final index micro block", K(ret), K(index_micro_desc));
  } else if (FALSE_IT(index_block_offset_ = actual_index_offset)) {
  } else if (FALSE_IT(index_block_size_ = index_micro_desc.get_block_size())) {
  } else if (OB_SUCC(tmp_ret) && OB_TMP_FAIL(prewarm_index_block(
      macro_id, uncompressed_index_micro_desc, index_micro_desc))) {
    STORAGE_LOG(WARN, "fail to prewarm final index micro block", K(tmp_ret), K(macro_id));
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::build_index_block(
    ObMicroBlockDesc &index_micro_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(actual_index_micro_writer_->get_row_count() <= 0
      || actual_index_micro_writer_->get_row_count() != index_row_count_
      || !last_rowkey_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected final index micro writer state", K(ret),
        "actual_row_count", actual_index_micro_writer_->get_row_count(),
        K_(index_row_count), K_(last_rowkey));
  } else if (OB_FAIL(actual_index_micro_writer_->build_micro_block_desc(index_micro_desc))) {
    STORAGE_LOG(WARN, "fail to build final index micro block desc", K(ret));
  } else if (FALSE_IT(index_micro_desc.last_rowkey_ = last_rowkey_)) {
  } else if (OB_UNLIKELY(!index_micro_desc.is_valid()
      || index_micro_desc.row_count_ != index_row_count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "built invalid final index micro block", K(ret),
        K(index_micro_desc), K_(index_row_count));
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::prewarm_index_block(
    const MacroBlockId &macro_id,
    const ObMicroBlockDesc &uncompressed_index_micro_desc,
    ObMicroBlockDesc &prepared_index_micro_desc)
{
  int ret = OB_SUCCESS;
  bool reserve_succ = false;
  if (OB_ISNULL(index_pre_warmer_)) {
    // skip
  } else if (OB_UNLIKELY(!macro_id.is_valid()
      || !uncompressed_index_micro_desc.is_valid()
      || !uncompressed_index_micro_desc.is_complete_micro_block_memory()
      || !prepared_index_micro_desc.is_valid()
      || index_block_offset_ < 0
      || index_block_size_ != prepared_index_micro_desc.get_block_size())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(macro_id),
        K(uncompressed_index_micro_desc), K(prepared_index_micro_desc),
        K_(index_block_offset), K_(index_block_size));
  } else {
    IGNORE_RETURN index_pre_warmer_->reserve(uncompressed_index_micro_desc, reserve_succ, 0);
    prepared_index_micro_desc.macro_id_ = macro_id;
    prepared_index_micro_desc.block_offset_ = index_block_offset_;
    IGNORE_RETURN index_pre_warmer_->add(prepared_index_micro_desc, reserve_succ);
  }
  if (OB_NOT_NULL(index_pre_warmer_)) {
    index_pre_warmer_->reuse();
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::write_meta_block(
    ObMacroBlock &macro_block,
    const ObInvIdxMacroBlockStats &macro_stats,
    const int64_t ddl_start_row_offset)
{
  int ret = OB_SUCCESS;
  int64_t actual_meta_offset = 0;
  ObMicroBlockDesc meta_micro_desc;
  if (OB_FAIL(generate_macro_row(
      macro_block, macro_stats, ddl_start_row_offset))) {
    STORAGE_LOG(WARN, "fail to generate final macro meta row", K(ret), K(macro_stats));
  } else if (OB_FAIL(build_meta_block(meta_micro_desc))) {
    STORAGE_LOG(WARN, "fail to build final macro meta block", K(ret));
  } else if (OB_FAIL(meta_micro_buffer_helper_.compress_encrypt_micro_block(
      meta_micro_desc,
      macro_block.get_current_macro_seq(),
      macro_block.get_data_size()))) {
    STORAGE_LOG(WARN, "fail to compress and encrypt final macro meta block", K(ret),
        K(meta_micro_desc));
  } else if (OB_FAIL(macro_block.write_index_micro_block(
      meta_micro_desc, false /*is_leaf_index_block*/, actual_meta_offset))) {
    STORAGE_LOG(WARN, "fail to write final macro meta block", K(ret), K(meta_micro_desc));
  } else {
    meta_block_offset_ = actual_meta_offset;
    meta_block_size_ = meta_micro_desc.get_block_size();
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::generate_macro_row(
    ObMacroBlock &macro_block,
    const ObInvIdxMacroBlockStats &macro_stats,
    const int64_t ddl_start_row_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index_block_offset_ < 0
      || index_block_size_ <= 0
      || index_block_offset_ + index_block_size_ != macro_block.get_data_size())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected written index block", K(ret),
        K_(index_block_offset), K_(index_block_size),
        "macro_data_size", macro_block.get_data_size());
  } else if (FALSE_IT(macro_meta_.reset())) {
  } else if (FALSE_IT(meta_row_allocator_.reuse())) {
  } else if (OB_FAIL(macro_block.get_macro_block_meta(macro_meta_))) {
    STORAGE_LOG(WARN, "fail to get macro block meta", K(ret));
  } else if (OB_FAIL(macro_stats.endkey_.deep_copy(
      macro_meta_.end_key_, meta_row_allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy macro endkey", K(ret), K(macro_stats));
  } else {
    macro_meta_.val_.row_store_type_ = common::INV_IDX_ROW_STORE;
    macro_meta_.val_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID; // actual id filled later
    macro_meta_.val_.row_count_ = macro_stats.doc_count_; // cannot be derived from index micro block
    macro_meta_.val_.rowkey_count_ = macro_stats.endkey_.get_datum_cnt();
    macro_meta_.val_.block_offset_ = index_block_offset_;
    macro_meta_.val_.block_size_ = index_block_size_;
    macro_meta_.val_.max_merged_trans_version_ = data_store_desc_->get_snapshot_version();
    macro_meta_.val_.ddl_end_row_offset_ = ddl_start_row_offset >= 0
        ? ddl_start_row_offset + macro_stats.doc_count_ - 1
        : -1;
    const int64_t agg_size = macro_stats.inv_meta_.get_serialize_size();
    char *agg_buf = nullptr;
    int64_t agg_pos = 0;
    if (OB_UNLIKELY(agg_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected macro meta serialize size", K(ret),
          K(agg_size), K(macro_stats));
    } else if (OB_ISNULL(agg_buf = static_cast<char *>(meta_row_allocator_.alloc(agg_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc macro meta buffer", K(ret), K(agg_size));
    } else if (OB_FAIL(macro_stats.inv_meta_.serialize(agg_buf, agg_size, agg_pos))) {
      STORAGE_LOG(WARN, "fail to serialize macro meta", K(ret),
          K(agg_size), K(macro_stats));
    } else if (OB_UNLIKELY(agg_pos != agg_size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected macro meta serialized size", K(ret),
          K(agg_pos), K(agg_size), K(macro_stats));
    } else {
      macro_meta_.val_.agg_row_buf_ = agg_buf;
      macro_meta_.val_.agg_row_len_ = agg_size;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!macro_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "built invalid macro meta", K(ret), K_(macro_meta), K(macro_stats));
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::build_meta_block(
    ObMicroBlockDesc &meta_micro_desc)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(!macro_meta_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro meta", K(ret), K_(macro_meta));
  } else if (FALSE_IT(data_version =
      ObBaseIndexBlockBuilder::get_data_version(*leaf_store_desc_))) {
  } else if (FALSE_IT(meta_row_.reuse())) {
  } else if (FALSE_IT(meta_block_writer_->reuse())) {
  } else if (OB_FAIL(macro_meta_.build_row(meta_row_, meta_row_allocator_, data_version))) {
    STORAGE_LOG(WARN, "fail to build macro meta row", K(ret), K_(macro_meta), K(data_version));
  } else if (OB_FAIL(meta_block_writer_->append_row(meta_row_))) {
    STORAGE_LOG(WARN, "fail to append macro meta row", K(ret), K_(meta_row));
  } else if (OB_FAIL(meta_block_writer_->build_micro_block_desc(meta_micro_desc))) {
    STORAGE_LOG(WARN, "fail to build macro meta block desc", K(ret));
  } else if (FALSE_IT(meta_micro_desc.last_rowkey_ = macro_meta_.end_key_)) {
  } else if (OB_UNLIKELY(!meta_micro_desc.is_valid() || meta_micro_desc.row_count_ != 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "built invalid macro meta block", K(ret), K(meta_micro_desc));
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::write_clustered_index_block()
{
  int ret = OB_SUCCESS;
  if (!micro_index_clustered_) {
    // skip
  } else if (OB_FAIL(clustered_index_writer_->build_and_append_clustered_index_micro_block())) {
    STORAGE_LOG(WARN, "fail to write clustered index micro block", K(ret));
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::publish_macro_meta(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "builder is closed", K(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro id", K(ret), K(macro_id));
  } else if (FALSE_IT(data_version =
      ObBaseIndexBlockBuilder::get_data_version(*leaf_store_desc_))) {
  } else if (FALSE_IT(macro_meta_.val_.macro_id_ = macro_id)) { // fill actual macro id
  } else if (OB_FAIL(macro_meta_.build_row(meta_row_, meta_row_allocator_, data_version))) {
    STORAGE_LOG(WARN, "fail to build macro meta row", K(ret), K_(macro_meta), K(data_version));
  } else if (OB_FAIL(macro_meta_dumper_.append_row(meta_row_))) {
    STORAGE_LOG(WARN, "fail to publish macro meta row", K(ret), K_(macro_meta));
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::reuse_macro_scope()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "builder is closed", K(ret));
  } else {
    row_allocator_.reuse();
    agg_data_allocator_.reuse();
    meta_row_allocator_.reuse();
    row_builder_.reuse();
    actual_index_micro_writer_->reuse();
    trial_index_micro_writer_->reuse();
    if (OB_FAIL(trial_index_micro_writer_->sync_adaptive_estimate_state(
        *actual_index_micro_writer_))) {
      STORAGE_LOG(WARN, "fail to sync adaptive estimate state", K(ret));
    }
    meta_block_writer_->reuse();
    macro_meta_.reset();
    meta_row_.reuse();
    last_rowkey_.reset();
    row_offset_ = -1;
    index_row_count_ = 0;
    index_block_offset_ = 0;
    index_block_size_ = 0;
    meta_block_offset_ = 0;
    meta_block_size_ = 0;
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::append_macro_block(
    const ObDataMacroBlockMeta &macro_meta,
    const ObMicroBlockData *micro_block_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "builder is closed", K(ret));
  } else if (OB_UNLIKELY(!macro_meta.is_valid() || !macro_meta.get_macro_id().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid reused macro meta", K(ret), K(macro_meta));
  } else if (micro_index_clustered_ && OB_ISNULL(micro_block_data)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected null clustered index micro block", K(ret));
  } else {
    const uint64_t data_version = ObBaseIndexBlockBuilder::get_data_version(*leaf_store_desc_);
    meta_row_.reuse();
    meta_row_allocator_.reuse();
    if (OB_FAIL(macro_meta.build_row(meta_row_, meta_row_allocator_, data_version))) {
      STORAGE_LOG(WARN, "fail to build reused macro meta row", K(ret),
          K(macro_meta), K(data_version));
    } else if (OB_FAIL(macro_meta_dumper_.append_row(meta_row_))) {
      STORAGE_LOG(WARN, "fail to append reused macro meta row", K(ret), K(macro_meta));
    } else if (micro_index_clustered_
        && OB_FAIL(clustered_index_writer_->reuse_clustered_micro_block(
            macro_meta.get_macro_id(), *micro_block_data))) {
      STORAGE_LOG(WARN, "fail to reuse clustered index micro block", K(ret),
          K(macro_meta), KPC(micro_block_data));
    } else {
      row_offset_ = -1;
    }
  }
  return ret;
}

int ObInvIdxIndexBlockBuilder::close(ObMacroBlocksWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "builder is closed", K(ret));
  } else if (micro_index_clustered_ && OB_ISNULL(clustered_index_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "clustered index writer is null", K(ret));
  } else if (micro_index_clustered_ && OB_FAIL(clustered_index_writer_->close())) {
    STORAGE_LOG(WARN, "fail to close clustered index block writer", K(ret));
  } else if (0 == macro_meta_dumper_.get_row_count()) {
    // Empty partial sstable, do not append root to sstable builder.
  } else if (OB_FAIL(write_ctx.deep_copy(
      index_tree_root_ctx_->data_write_ctx_, *index_tree_root_ctx_->allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy data write ctx", K(ret));
  } else if (OB_FAIL(macro_meta_dumper_.close(index_tree_root_ctx_->meta_block_info_))) {
    STORAGE_LOG(WARN, "fail to close macro meta dumper", K(ret));
  } else if (OB_NOT_NULL(index_pre_warmer_) && OB_FAIL(index_pre_warmer_->close())) {
    STORAGE_LOG(WARN, "fail to close index micro block pre warmer", K(ret));
  } else {
    if (1 == index_tree_root_ctx_->meta_block_info_.get_row_count()) {
      index_tree_root_ctx_->meta_block_size_ = meta_block_size_;
      index_tree_root_ctx_->meta_block_offset_ = meta_block_offset_;
    }
    if (index_tree_root_ctx_->meta_block_info_.in_mem()) {
      index_tree_root_ctx_->last_key_ =
          index_tree_root_ctx_->meta_block_info_.micro_block_desc_->last_rowkey_;
    } else if (OB_FAIL(macro_meta_dumper_.get_last_rowkey().deep_copy(
        index_tree_root_ctx_->last_key_, *index_tree_root_ctx_->allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy last macro meta rowkey", K(ret));
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "succeed to close index block builder", KPC_(index_tree_root_ctx));
    }
  }
  if (is_inited_ && !is_closed_) {
    is_closed_ = true;
  }
  return ret;
}

void ObInvIdxIndexBlockBuilder::reset()
{
  inner_reset();
  row_allocator_.reset();
  agg_data_allocator_.reset();
  meta_row_allocator_.reset();
}

void ObInvIdxIndexBlockBuilder::inner_reset()
{
  data_store_desc_ = nullptr;
  leaf_store_desc_ = nullptr;
  index_store_desc_ = nullptr;
  sstable_builder_ = nullptr;
  index_tree_root_ctx_ = nullptr;
  if (OB_NOT_NULL(actual_index_micro_writer_)) {
    actual_index_micro_writer_->~ObIMicroBlockWriter();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(actual_index_micro_writer_);
    }
    actual_index_micro_writer_ = nullptr;
  }
  if (OB_NOT_NULL(trial_index_micro_writer_)) {
    trial_index_micro_writer_->~ObIMicroBlockWriter();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(trial_index_micro_writer_);
    }
    trial_index_micro_writer_ = nullptr;
  }
  if (OB_NOT_NULL(meta_block_writer_)) {
    meta_block_writer_->~ObIMicroBlockWriter();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(meta_block_writer_);
    }
    meta_block_writer_ = nullptr;
  }
  if (OB_NOT_NULL(clustered_index_writer_)) {
    clustered_index_writer_->~ObClusteredIndexBlockWriter();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(clustered_index_writer_);
    }
    clustered_index_writer_ = nullptr;
  }
  if (OB_NOT_NULL(index_pre_warmer_)) {
    index_pre_warmer_->~ObIPreWarmer();
    if (OB_NOT_NULL(allocator_)) {
      allocator_->free(index_pre_warmer_);
    }
    index_pre_warmer_ = nullptr;
  }
  allocator_ = nullptr;
  row_builder_.reset();
  index_micro_buffer_helper_.reset();
  meta_micro_buffer_helper_.reset();
  macro_meta_dumper_.reset();
  macro_meta_.reset();
  meta_row_.reset();
  last_rowkey_.reset();
  row_offset_ = -1;
  index_row_count_ = 0;
  index_block_offset_ = 0;
  index_block_size_ = 0;
  meta_block_offset_ = 0;
  meta_block_size_ = 0;
  micro_index_clustered_ = false;
  is_inited_ = false;
  is_closed_ = false;
}

} // end namespace blocksstable
} // end namespace oceanbase
