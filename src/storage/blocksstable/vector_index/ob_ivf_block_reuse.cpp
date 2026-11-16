/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/vector_index/ob_ivf_block_reuse.h"
#include "storage/blocksstable/vector_index/ob_ivf_macro_block_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_layout.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/ob_i_store.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;
using namespace share;

int ObIVFBlockReuseSource::restore_centroid_layout(const ObIArray<float> &leaves,
    ObIvfCentroidLayout &layout, bool &reused) const
{
  int ret = OB_SUCCESS;
  reused = false;
  if (table_ == nullptr || !has_prior_view() || layout.is_ready()) { return ret; }
  const auto &meta = table_->get_ivf_meta();
  if (meta.dim_ != dimension_ || meta.centroid_count_ != centroid_count_
      || leaves.count() / dimension_ != centroid_count_
      || leaves.count() % dimension_ != 0) { return ret; }
  for (int64_t i = 0; i < centroid_count_; ++i) {
    if (old_cid(i) != meta.leaf_storage_cid(i)
        || MEMCMP(&leaves.at(i * dimension_),
            &leaf_coordinates_->at((centroid_begin_ + i) * dimension_),
            dimension_ * sizeof(float)) != 0) { return ret; }
  }
  if (!meta.is_hierarchical()) {
    if (centers_.count() != centroid_count_) { return ret; }
    for (int64_t i = 0; i < centers_.count(); ++i) {
      const auto &c = centers_.at(i);
      if (!c.canonical_payload_ || c.key_.type_ != IVF_ROW_TYPE_CENTROID
          || c.key_.cid_ != 0 || c.key_.vid_ != static_cast<uint64_t>(i + 1)
          || coordinates(c) == nullptr
          || MEMCMP(coordinates(c), &leaves.at(i * dimension_), dimension_ * sizeof(float)) != 0) { return ret; }
    }
    ret = layout.init_flat(centroid_count_, dimension_);
  } else {
    const int64_t parents = meta.parent_centroid_count_;
    if (parents <= 0 || centers_.count() <= parents
        || parent_centers_.count() / dimension_ != parents
        || parent_centers_.count() % dimension_ != 0) { return ret; }
    // The captured stream is ordered by (group, CID), including SOAR replicas.
    // Restore that stream exactly; no parent training or reassignment is needed.
    for (int64_t p = 0; p < parents; ++p) {
      const auto &c = centers_.at(p);
      if (!c.canonical_payload_ || c.key_.type_ != IVF_ROW_TYPE_CENTROID
          || c.key_.cid_ != 0 || c.key_.vid_ != static_cast<uint64_t>(p + 1)
          || coordinates(c) == nullptr
          || MEMCMP(coordinates(c), &parent_centers_.at(p * dimension_),
              dimension_ * sizeof(float)) != 0) { return ret; }
    }
    ret = layout.begin_grouped(parent_centers_, centroid_count_, dimension_);
    ObIVFReuseKey previous;
    for (int64_t i = parents; OB_SUCC(ret) && i < centers_.count(); ++i) {
      const auto &c = centers_.at(i);
      const int64_t leaf = static_cast<int64_t>(c.key_.vid_) - meta.leaf_cid_base();
      if (!c.canonical_payload_ || c.key_.type_ != IVF_ROW_TYPE_CENTROID
          || c.key_.cid_ < 1 || c.key_.cid_ > parents
          || leaf < 0 || leaf >= centroid_count_ || coordinates(c) == nullptr
          || (i > parents && previous.compare(c.key_) >= 0)
          || MEMCMP(coordinates(c), &leaves.at(leaf * dimension_), dimension_ * sizeof(float)) != 0) {
        ret = OB_INVALID_DATA;
      } else {
        const bool secondary = layout.primary_parent(leaf) > 0;
        ret = layout.set_parent(leaf, c.key_.cid_ - 1, secondary);
      }
      previous = c.key_;
    }
    if (OB_SUCC(ret)) { ret = layout.finish_grouped(); }
    for (int64_t p = 0; OB_SUCC(ret) && p < parents; ++p) {
      if (layout.group_count(p) != centers_.at(p).count_) { ret = OB_INVALID_DATA; }
    }
  }
  reused = OB_SUCC(ret);
  if (!reused) { layout.reset(); }
  return ret;
}

bool ObIVFBlockReuseSource::has_prior_view() const
{
  return source_slot_ >= 0 && centroid_begin_ >= 0 && centroid_count_ > 0 && dimension_ > 0
      && leaf_coordinates_ != nullptr && old_cids_ != nullptr && posting_counts_ != nullptr
      && centroid_begin_ <= old_cids_->count() - centroid_count_
      && centroid_begin_ <= posting_counts_->count() - centroid_count_
      && centroid_begin_ <= leaf_coordinates_->count() / dimension_ - centroid_count_;
}

int64_t ObIVFBlockReuseSource::local_centroid_index(int64_t prior_index) const
{
  return prior_index >= centroid_begin_ && prior_index - centroid_begin_ < centroid_count_
      ? prior_index - centroid_begin_ : -1;
}

int64_t ObIVFBlockReuseSource::old_cid(int64_t local_index) const
{ return old_cids_->at(centroid_begin_ + local_index); }

int64_t ObIVFBlockReuseSource::posting_count(int64_t local_index) const
{ return posting_counts_->at(centroid_begin_ + local_index); }

const float *ObIVFBlockReuseSource::coordinates(const ObIVFReuseCentroid &record) const
{
  if (dimension_ <= 0 || record.coordinate_idx_ == INT64_MIN) { return nullptr; }
  const bool leaf = record.coordinate_idx_ >= 0;
  if (leaf && (record.coordinate_idx_ % dimension_ != 0
      || record.coordinate_idx_ / dimension_ >= centroid_count_ || !has_prior_view())) { return nullptr; }
  const auto *values = leaf ? leaf_coordinates_ : &parent_centers_;
  const int64_t offset = leaf ? centroid_begin_ * dimension_ + record.coordinate_idx_
      : -record.coordinate_idx_ - 1;
  return values != nullptr && offset >= 0 && offset <= values->count() - dimension_
      ? &values->at(offset) : nullptr;
}

int ObIVFReuseKey::compare(const ObIVFReuseKey &other) const
{
  return type_ != other.type_ ? (type_ < other.type_ ? -1 : 1)
      : cid_ != other.cid_ ? (cid_ < other.cid_ ? -1 : 1)
      : vid_ == other.vid_ ? 0 : vid_ < other.vid_ ? -1 : 1;
}

bool ObIVFReuseKey::from_rowkey(const ObDatumRowkey &key, ObIVFReuseKey &value)
{
  bool valid = key.datums_ != nullptr && key.get_datum_cnt() >= IVF_ROWKEY_COL_COUNT;
  for (int64_t i = 0; valid && i < IVF_ROWKEY_COL_COUNT; ++i) {
    valid = !key.datums_[i].is_null() && !key.datums_[i].is_ext();
  }
  if (valid) { value = ObIVFReuseKey(key.datums_[0].get_int(), key.datums_[1].get_int(), key.datums_[2].get_uint()); }
  return valid;
}

ObIVFBlockReuseWriter::ObIVFBlockReuseWriter(ObIAllocator &allocator)
  : allocator_(allocator), range_index_allocator_(ObMemAttr(MTL_ID(), "IVFReuseRange", ObCtxIds::VECTOR_CTX_ID)),
    micro_index_allocator_(ObMemAttr(MTL_ID(), "IVFReuseIndex", ObCtxIds::VECTOR_CTX_ID)),
    sources_(nullptr), source_compatible_(),
    source_(nullptr), ranges_(nullptr), desc_(nullptr), writer_(nullptr),
    task_range_(), active_range_(), range_pos_(-1), scan_range_(), start_(), end_(), macro_iter_(nullptr), micro_iter_(nullptr),
    macro_(), meta_(), micro_pos_(-1), candidate_end_(), first_(), last_(), last_output_(),
    candidate_rows_(0), consumed_(0), rows_(0), reused_macros_(0), copied_micros_(0),
    macro_payload_bytes_(0), micro_read_bytes_(0),
    candidate_(false), macro_candidate_(false), ended_(false), has_output_(false), finished_(false), error_(OB_SUCCESS)
{
  macro_.macro_meta_ = &meta_;
  source_compatible_.set_block_allocator(ObWrapperAllocator(allocator_));
}

void ObIVFBlockReuseWriter::destroy_iter(ObIMacroBlockIterator *&iter, ObIAllocator &allocator)
{
  if (iter != nullptr) { iter->~ObIMacroBlockIterator(); allocator.free(iter); iter = nullptr; }
}

ObIVFBlockReuseWriter::~ObIVFBlockReuseWriter()
{ destroy_iter(micro_iter_, micro_index_allocator_); destroy_iter(macro_iter_, range_index_allocator_); }

int ObIVFBlockReuseWriter::check_source(const ObIVFBlockReuseSource &source,
    const ObDataStoreDesc &desc, bool &compatible)
{
  int ret = OB_SUCCESS;
  compatible = false;
  ObSSTableMetaHandle handle;
  if (source.table_ == nullptr || !desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF reuse source descriptor", K(ret), K(source.source_slot_), KP(source.table_), K(desc.is_valid()));
    return ret;
  }
  const auto &table = *source.table_;
  const bool output_supported = desc.is_major_merge_type() || compaction::is_inc_major_merge(desc.get_merge_type());
  const int64_t rowkey_count = IVF_ROWKEY_COL_COUNT + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  if ((!table.is_major_sstable() && !table.is_inc_ivf_sstable()) || !table.is_ivf_sstable() || table.is_empty()
      || table.is_small_sstable() || table.contain_uncommitted_row()
      || !output_supported || !desc.get_need_submit_io() || desc.is_cg()
      || source.commit_version_ <= 0 || source.commit_version_ == INT64_MAX
      || table.get_key().tablet_id_ != desc.get_tablet_id()
      || table.get_snapshot_version() > desc.get_snapshot_version()
      || !table.get_ivf_meta().is_valid() || table.get_ivf_meta().is_empty()
      || desc.get_schema_rowkey_col_cnt() != IVF_ROWKEY_COL_COUNT
      || desc.get_rowkey_column_count() != rowkey_count || desc.get_row_column_count() != rowkey_count + 1) {
  } else if (OB_FAIL(table.get_meta(handle))) {
    LOG_WARN("failed to read IVF reuse source metadata", K(ret), K(source.source_slot_), K(table.get_key()));
  } else {
    const auto &meta = handle.get_sstable_meta().get_basic_meta();
    // An INC table may retain major-origin macros alongside native INC macros.
    // As in full-text reuse, certify the fixed IVF row layout, not the schema
    // version: SS refresh sealing can publish schema_version=0. The IVF format
    // and payload parameters are checked by set_reuse_sources; each macro keeps
    // its own decoding metadata and is checked below before append_macro_block.
    compatible = meta.column_cnt_ == desc.get_row_column_count()
        && meta.rowkey_column_count_ == desc.get_rowkey_column_count()
        && meta.encrypt_id_ == desc.get_encrypt_id() && meta.master_key_id_ == desc.get_master_key_id()
        && !meta.contain_uncommitted_row_ && meta.max_merged_trans_version_ <= desc.get_snapshot_version()
        && 0 == MEMCMP(meta.encrypt_key_, desc.get_encrypt_key(), desc.get_encrypt_key_size());
  }
  return ret;
}

int ObIVFBlockReuseWriter::init(const ObIArray<ObIVFBlockReuseSource *> &sources,
    const ObIArray<ObIVFReuseRange> &ranges,
    const ObIVFReuseRange &task_range, const ObDataStoreDesc &desc, ObIvfMacroBlockWriter &writer)
{
  int ret = OB_SUCCESS;
  if (writer_ != nullptr || !desc.is_valid()
      || task_range.first_.compare(task_range.last_) > 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF reuse writer initialization", K(ret), KP_(writer), K(desc.is_valid()), K(task_range));
    return ret;
  }
  sources_ = &sources; ranges_ = &ranges; desc_ = &desc; writer_ = &writer; task_range_ = task_range;
  for (int64_t slot = 0; OB_SUCC(ret) && slot < sources.count(); ++slot) {
    bool compatible = false;
    const auto *source = sources.at(slot);
    if (source != nullptr) {
      if (source->source_slot_ != slot || source->table_ == nullptr || source->read_info_ == nullptr
          || source->read_info_->get_schema_rowkey_count() != IVF_ROWKEY_COL_COUNT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid IVF reuse source binding", K(ret), K(slot), KPC(source));
      }
      else { ret = check_source(*source, desc, compatible); }
    }
    if (OB_SUCC(ret) && OB_FAIL(source_compatible_.push_back(compatible ? 1 : 0))) {
      LOG_WARN("failed to retain IVF reuse source compatibility", K(ret), K(slot));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
    const auto &range = ranges.at(i);
    if (range.source_slot_ < 0 || range.source_slot_ >= sources.count()
        || sources.at(range.source_slot_) == nullptr
        || range.first_.compare(range.last_) > 0
        || (i > 0 && ranges.at(i - 1).last_.compare(ranges.at(i).first_) >= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid or overlapping IVF reuse range", K(ret), K(i), K(range), K(sources.count()));
    }
  }
  if (OB_SUCC(ret)) { ret = next_candidate(); }
  error_ = ret;
  return ret;
}

int ObIVFBlockReuseWriter::open_next_range()
{
  int ret = OB_SUCCESS;
  destroy_iter(micro_iter_, micro_index_allocator_);
  destroy_iter(macro_iter_, range_index_allocator_);
  micro_index_allocator_.reuse(); range_index_allocator_.reuse();
  candidate_end_.reset(); meta_.reset(); macro_.reset(); macro_.macro_meta_ = &meta_;
  micro_pos_ = -1;
  while (OB_SUCC(ret) && ++range_pos_ < ranges_->count()) {
    const auto &range = ranges_->at(range_pos_);
    if (!source_compatible_.at(range.source_slot_) || range.last_.compare(task_range_.first_) < 0) { continue; }
    if (range.first_.compare(task_range_.last_) > 0) { break; }
    active_range_ = range;
    if (active_range_.first_.compare(task_range_.first_) < 0) { active_range_.first_ = task_range_.first_; }
    if (active_range_.last_.compare(task_range_.last_) > 0) { active_range_.last_ = task_range_.last_; }
    source_ = sources_->at(range.source_slot_);
    start_[0].set_int(active_range_.first_.type_); start_[1].set_int(active_range_.first_.cid_);
    start_[2].set_uint(active_range_.first_.vid_);
    end_[0].set_int(active_range_.last_.type_); end_[1].set_int(active_range_.last_.cid_);
    end_[2].set_uint(active_range_.last_.vid_);
    // Range MAX is distinct from signed -1 and also includes real UINT64_MAX.
    if (active_range_.last_.vid_ == UINT64_MAX) { end_[2].set_max(); }
    scan_range_.start_key_.assign(start_, 3); scan_range_.end_key_.assign(end_, 3);
    scan_range_.set_left_closed(); scan_range_.set_right_closed();
    if (OB_FAIL(source_->table_->scan_macro_block(scan_range_, *source_->read_info_, range_index_allocator_,
        macro_iter_, false, desc_->micro_index_clustered(), true))) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        destroy_iter(macro_iter_, range_index_allocator_); range_index_allocator_.reuse();
      } else {
        LOG_WARN("failed to scan IVF reuse range", K(ret), K_(active_range), K_(scan_range));
      }
    }
    if (OB_FAIL(ret) || macro_iter_ != nullptr) { return ret; }
  }
  ended_ = true;
  return ret;
}

bool ObIVFBlockReuseWriter::eligible(const ObDatumRowkey &begin, const ObDatumRowkey &end) const
{
  ObIVFReuseKey first, last;
  if (!ObIVFReuseKey::from_rowkey(begin, first) || !ObIVFReuseKey::from_rowkey(end, last)
      || first.type_ != last.type_ || first.compare(last) >= 0
      || first.compare(active_range_.first_) < 0 || last.compare(active_range_.last_) > 0) { return false; }
  return true;
}

bool ObIVFBlockReuseWriter::may_reuse(const ObDatumRowkey &begin, const ObDatumRowkey &end) const
{
  ObIVFReuseKey first, last;
  if (!ObIVFReuseKey::from_rowkey(begin, first) || !ObIVFReuseKey::from_rowkey(end, last)) { return true; }
  return active_range_.first_.compare(last) <= 0 && active_range_.last_.compare(first) >= 0;
}

bool ObIVFBlockReuseWriter::compatible_macro() const
{
  const auto &meta = meta_.val_;
  const uint8_t origin_merge_type = meta_.get_logic_id().merge_type_;
  // append_macro_block preserves the original logical ID and column sums.
  // Native INC macros also have a checksum array, but it can be zero/partial;
  // only an explicit major origin certifies complete sums without reading data.
  const bool checksum_compatible = !desc_->is_major_merge_type()
      || ((origin_merge_type == compaction::MAJOR_MERGE || origin_merge_type == compaction::MEDIUM_MERGE)
          && meta.column_checksums_.count() == meta.column_count_);
  return macro_.is_valid_with_macro_meta() && macro_.row_count_ == meta.row_count_
      && macro_.row_count_ > 0 && !macro_.contain_uncommitted_row_ && !macro_.is_deleted_
      && !meta.contain_uncommitted_row_ && !meta.is_deleted_
      && meta.progressive_merge_round_ == desc_->get_progressive_merge_round()
      && meta.rowkey_count_ == desc_->get_rowkey_column_count() && meta.column_count_ == desc_->get_row_column_count()
      && (ObStoreFormat::is_row_store_type_with_flat(meta.row_store_type_)
          || ObStoreFormat::is_row_store_type_with_encoding(meta.row_store_type_))
      && checksum_compatible
      // Both ordinary and clustered macro append preserve source data decoding
      // metadata. The clustered writer rebuilds index rows with the old data
      // macro ID, schema, row format and compressor; no data re-encoding is needed.
      && meta.max_merged_trans_version_ <= desc_->get_snapshot_version()
      && meta.encrypt_id_ == desc_->get_encrypt_id() && meta.master_key_id_ == desc_->get_master_key_id()
      && 0 == MEMCMP(meta.encrypt_key_, desc_->get_encrypt_key(), desc_->get_encrypt_key_size())
      && macro_.is_clustered_index_tree_ == desc_->micro_index_clustered();
}

int ObIVFBlockReuseWriter::next_candidate()
{
  int ret = OB_SUCCESS;
  candidate_ = false; consumed_ = 0;
  while (OB_SUCC(ret) && !ended_ && !candidate_) {
    if (macro_iter_ == nullptr) {
      if (OB_FAIL(open_next_range()) || ended_) { break; }
    }
    if (micro_iter_ != nullptr) {
      const auto &infos = micro_iter_->get_micro_index_infos();
      const auto &keys = micro_iter_->get_micro_endkeys();
      if (infos.count() != keys.count()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("IVF reuse micro index count mismatch", K(ret), K_(active_range),
            K(macro_.macro_block_id_), K(infos.count()), K(keys.count()));
      }
      while (OB_SUCC(ret) && !candidate_ && ++micro_pos_ < infos.count()) {
        const auto &begin = micro_pos_ == 0 ? macro_.range_.start_key_ : keys.at(micro_pos_ - 1);
        if (!infos.at(micro_pos_).is_valid()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid IVF reuse micro index", K(ret), K_(active_range),
              K(macro_.macro_block_id_), K_(micro_pos));
        }
        else if (!infos.at(micro_pos_).contain_uncommitted_row()
            && infos.at(micro_pos_).row_header_->get_schema_version() == desc_->get_schema_version()
            && eligible(begin, keys.at(micro_pos_))) {
          ObIVFReuseKey::from_rowkey(begin, first_); ObIVFReuseKey::from_rowkey(keys.at(micro_pos_), last_);
          candidate_end_ = keys.at(micro_pos_); candidate_rows_ = infos.at(micro_pos_).get_row_count();
          candidate_ = true; macro_candidate_ = false;
        }
      }
      if (!candidate_) { destroy_iter(micro_iter_, micro_index_allocator_); micro_index_allocator_.reuse(); }
    }
    if (OB_FAIL(ret) || candidate_) { break; }
    if (OB_FAIL(macro_iter_->get_next_macro_block(macro_))) {
      if (ret == OB_ITER_END) { ret = open_next_range(); }
      else { LOG_WARN("failed to advance IVF reuse macro", K(ret), K_(active_range)); }
    } else if (OB_FAIL(dag_yield())) {
      LOG_WARN("IVF reuse macro scan interrupted", K(ret), K_(active_range), K(macro_.macro_block_id_));
    } else if (!compatible_macro()) {
      // Physical incompatibility keeps the caller's ordinary row output.
    } else if (eligible(macro_.range_.start_key_, macro_.range_.end_key_)) {
      ObIVFReuseKey::from_rowkey(macro_.range_.start_key_, first_);
      ObIVFReuseKey::from_rowkey(macro_.range_.end_key_, last_);
      candidate_end_ = macro_.range_.end_key_; candidate_rows_ = macro_.row_count_;
      candidate_ = true; macro_candidate_ = true;
    } else if (!macro_.is_clustered_index_tree_ && !meta_.val_.is_encrypted_ && meta_.val_.encrypt_id_ == 0
        && meta_.val_.schema_version_ == desc_->get_schema_version()
        && meta_.val_.row_store_type_ == desc_->get_row_store_type()
        && meta_.val_.compressor_type_ == desc_->get_compressor_type()
        && may_reuse(macro_.range_.start_key_, macro_.range_.end_key_)) {
      // Do not decrypt/re-encode a micro merely to call it a copy. Unsupported
      // micro formats remain on the already validated row stream.
      // The macro iterator deliberately reports MIN as the lower bound of its
      // FIRST returned macro, even for a posting/RAW subrange. Expanding that
      // synthetic range would restart at the table's centroid macros.
      ObDatumRange *range = OB_NEWx(ObDatumRange, &micro_index_allocator_, macro_.range_);
      if (range == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate IVF micro expansion range", K(ret), K_(active_range), K(macro_.macro_block_id_));
      }
      else if (range->start_key_.is_min_rowkey()) {
        range->start_key_ = scan_range_.start_key_;
        range->border_flag_.set_inclusive_start();
      }
      if (OB_SUCC(ret) && OB_FAIL(source_->table_->scan_macro_block(*range, *source_->read_info_, micro_index_allocator_,
          micro_iter_, false, true, true))) {
        LOG_WARN("failed to expand IVF reuse macro", K(ret), K_(active_range), K(macro_.macro_block_id_), KPC(range));
      } else if (OB_SUCC(ret)) {
        ObMacroBlockDesc expanded;
        ObDataMacroBlockMeta expanded_meta;
        expanded.macro_meta_ = &expanded_meta;
        if (OB_FAIL(micro_iter_->get_next_macro_block(expanded))) {
          LOG_WARN("failed to read expanded IVF reuse macro", K(ret), K_(active_range), K(macro_.macro_block_id_));
        } else if (expanded.macro_block_id_ != macro_.macro_block_id_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("IVF micro expansion changed source macro", K(ret), K(expanded.macro_block_id_), K(macro_.macro_block_id_));
        }
        else { micro_pos_ = -1; }
      }
    }
  }
  return ret;
}

int ObIVFBlockReuseWriter::copy_micro(const ObMicroIndexInfo &index, const ObDatumRowkey &end)
{
  int ret = OB_SUCCESS;
  const int64_t size = index.get_block_size();
  ObArenaAllocator scratch(ObMemAttr(MTL_ID(), "IVFReuseMicro", ObCtxIds::VECTOR_CTX_ID));
  ObStorageObjectReadInfo io;
  ObStorageObjectHandle handle;
  io.offset_ = index.get_block_offset(); io.size_ = size;
  io.io_desc_.set_mode(ObIOMode::READ); io.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  io.io_desc_.set_sys_module_id(ObIOModule::INDEX_BLOCK_MICRO_ITER_IO);
  io.io_timeout_ms_ = MAX(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  io.macro_block_id_ = index.get_macro_id(); io.mtl_tenant_id_ = MTL_ID();
  if (size <= 0 || size > desc_->get_macro_block_size() || io.offset_ < 0
      || io.offset_ > desc_->get_macro_block_size() - size) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid IVF reused micro bounds", K(ret), K_(active_range),
        K(io.macro_block_id_), K(io.offset_), K(size), "macro_size", desc_->get_macro_block_size());
  } else if (OB_ISNULL(io.buf_ = static_cast<char *>(scratch.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF reused micro buffer", K(ret), K_(active_range), K(size));
  } else if (OB_FAIL(ObObjectManager::async_read_object(io, handle))) {
    LOG_WARN("failed to submit IVF reused micro read", K(ret), K_(active_range),
        K(io.macro_block_id_), K(io.offset_), K(size));
  } else if (OB_FAIL(handle.wait())) {
    LOG_WARN("failed to wait for IVF reused micro read", K(ret), K_(active_range),
        K(io.macro_block_id_), K(io.offset_), K(size));
  } else {
    const char *buf = handle.get_buffer();
    ObMicroBlockHeader header;
    int64_t pos = 0;
    if (buf == nullptr || handle.get_data_size() < size) {
      ret = OB_INVALID_DATA;
      LOG_WARN("incomplete IVF reused micro read", K(ret), K_(active_range),
          K(io.macro_block_id_), K(io.offset_), K(size), KP(buf), K(handle.get_data_size()));
    }
    else if (OB_FAIL(header.deserialize(buf, size, pos))) {
      LOG_WARN("failed to deserialize IVF reused micro header", K(ret), K_(active_range),
          K(io.macro_block_id_), K(io.offset_), K(size));
    } else if (OB_FAIL(header.check_record(buf, size, MICRO_BLOCK_HEADER_MAGIC))) {
      LOG_WARN("corrupt IVF reused micro record", K(ret), K_(active_range),
          K(io.macro_block_id_), K(io.offset_), K(size));
    } else if (header.row_count_ != candidate_rows_
        || (desc_->is_major_merge_type() && !header.has_column_checksum_)
        || (header.has_column_checksum_ && header.column_checksums_ == nullptr)
        || header.column_count_ != desc_->get_row_column_count()
        || header.row_store_type_ != desc_->get_row_store_type()
        || header.max_merged_trans_version_ > desc_->get_snapshot_version()
        || header.header_size_ + int64_t(header.data_zlength_) != size) {
      ret = OB_INVALID_DATA;
      LOG_WARN("incompatible IVF reused micro header", K(ret), K_(active_range),
          K(io.macro_block_id_), K(io.offset_), K(size), K(header), K_(candidate_rows),
          "expected_columns", desc_->get_row_column_count(), "expected_store", desc_->get_row_store_type(),
          "snapshot", desc_->get_snapshot_version());
    }
    else {
      ObMicroBlockDesc block;
      // Keep the complete source header. For a legacy non-checksummed micro,
      // its union stores min_merged_trans_version_, not a checksum pointer;
      // serialize() preserves that value while the index aggregator retains
      // the source micro's min/max summary. ObMicroBlockDesc has no min field.
      block.header_ = &header; block.last_rowkey_ = end;
      block.buf_ = buf + header.header_size_; block.buf_size_ = header.data_zlength_;
      block.data_size_ = header.data_length_; block.original_size_ = header.original_length_;
      block.row_count_ = header.row_count_; block.column_count_ = header.column_count_;
      block.max_merged_trans_version_ = header.max_merged_trans_version_;
      const bool flags = header.row_store_type_ == FLAT_ROW_STORE || header.row_store_type_ == FLAT_OPT_ROW_STORE
          || header.row_store_type_ == CS_ENCODING_ROW_STORE;
      block.is_last_row_last_flag_ = flags && header.is_last_row_last_flag_;
      block.is_first_row_first_flag_ = flags && header.is_first_row_first_flag_;
      block.single_version_rows_ = flags && header.single_version_rows_;
      block.contain_uncommitted_row_ = false; block.has_string_out_row_ = header.has_string_out_row();
      block.has_lob_out_row_ = header.has_lob_out_row(); block.logic_micro_id_ = index.get_logic_micro_id();
      ObMicroIndexData index_data(index);
      if (OB_FAIL(writer_->flush_micro_block())) {
        LOG_WARN("failed to flush rows before IVF micro reuse", K(ret), K_(active_range),
            K(io.macro_block_id_), K(io.offset_), K(size));
      } else if (OB_FAIL(writer_->append_micro_block(block, index_data))) {
        LOG_WARN("failed to append IVF reused micro", K(ret), K_(active_range),
            K(io.macro_block_id_), K(io.offset_), K(size), K_(candidate_rows));
      } else { ++copied_micros_; micro_read_bytes_ += size; }
    }
  }
  return ret;
}

int ObIVFBlockReuseWriter::write_candidate()
{
  int ret = OB_SUCCESS;
  if (consumed_ != candidate_rows_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("incomplete IVF reuse candidate", K(ret), K_(active_range),
        K(macro_.macro_block_id_), K_(consumed), K_(candidate_rows), K_(first), K_(last));
  }
  else if (macro_candidate_) {
    const ObMicroBlockData *clustered = nullptr;
    if (macro_.is_clustered_index_tree_ && OB_FAIL(macro_iter_->get_current_clustered_index_info(clustered))) {
      LOG_WARN("failed to read IVF reused macro clustered index", K(ret), K_(active_range), K(macro_.macro_block_id_));
    } else if (OB_FAIL(writer_->append_macro_block(macro_, clustered))) {
      LOG_WARN("failed to append IVF reused macro", K(ret), K_(active_range),
          K(macro_.macro_block_id_), K_(candidate_rows));
    } else { ++reused_macros_; macro_payload_bytes_ += meta_.val_.data_zsize_; }
  } else { ret = copy_micro(micro_iter_->get_micro_index_infos().at(micro_pos_), candidate_end_); }
  return ret;
}

int ObIVFBlockReuseWriter::append_row(const ObDatumRow &row)
{
  int ret = error_;
  if (OB_SUCC(ret) && (row.storage_datums_ == nullptr || row.count_ < IVF_ROWKEY_COL_COUNT)) {
    error_ = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF reuse row", "ret", error_, K_(task_range), KP(row.storage_datums_), K(row.count_));
    return error_;
  }
  if (OB_FAIL(ret)) { return ret; }
  ObIVFReuseKey key;
  ObDatumRowkey rowkey;
  rowkey.assign(row.storage_datums_, IVF_ROWKEY_COL_COUNT);
  if (OB_SUCC(ret) && (writer_ == nullptr || finished_ || !ObIVFReuseKey::from_rowkey(rowkey, key)
      || key.compare(task_range_.first_) < 0 || key.compare(task_range_.last_) > 0
      || (has_output_ && last_output_.compare(key) >= 0))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid IVF reuse output key or state", K(ret), K(key), K_(task_range),
        K_(last_output), K_(has_output), K_(finished), KP_(writer));
  }
  if (OB_FAIL(ret)) {
  } else if (candidate_ && key.compare(last_) > 0) {
    ret = OB_INVALID_DATA; // Missing certified source rows; never silently drop the candidate.
    LOG_WARN("IVF reuse output skipped certified rows", K(ret), K(key), K_(active_range),
        K_(first), K_(last), K_(consumed), K_(candidate_rows));
  } else if (candidate_ && key.compare(first_) > 0) {
    if (++consumed_ > candidate_rows_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("IVF reuse output exceeds certified row count", K(ret), K(key),
          K_(active_range), K_(consumed), K_(candidate_rows));
    }
    else if (key.compare(last_) == 0) {
      if (OB_SUCC(ret = write_candidate())) { ret = next_candidate(); }
    }
  } else if (OB_FAIL(writer_->append_row(row))) {
    LOG_WARN("failed to append ordinary IVF reuse output row", K(ret), K(key), K_(task_range));
  }
  if (OB_SUCC(ret)) { ++rows_; last_output_ = key; has_output_ = true; }
  error_ = ret;
  return ret;
}

int ObIVFBlockReuseWriter::try_reuse_raw_keys(const ObIArray<uint64_t> &vids,
    int64_t begin, int64_t end, int64_t &reused_rows, int64_t &read_end)
{
  int ret = error_;
  reused_rows = 0;
  read_end = begin;
  if (OB_FAIL(ret)) { return ret; }
  if (writer_ == nullptr || finished_ || begin < 0 || end <= begin || end > vids.count()
      || task_range_.first_.type_ != IVF_ROW_TYPE_RAW_VECTOR
      || task_range_.last_.type_ != IVF_ROW_TYPE_RAW_VECTOR
      || task_range_.first_.cid_ != IVF_RAW_VECTOR_CID
      || task_range_.last_.cid_ != IVF_RAW_VECTOR_CID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF RAW reuse slice", K(ret), K(begin), K(end), K(vids.count()),
        K_(task_range), K_(finished), KP_(writer));
  } else {
    const ObIVFReuseKey key(IVF_ROW_TYPE_RAW_VECTOR, IVF_RAW_VECTOR_CID, vids.at(begin));
    if (key.compare(task_range_.first_) < 0 || key.compare(task_range_.last_) > 0
        || (has_output_ && last_output_.compare(key) >= 0)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF RAW reuse output order", K(ret), K(key), K_(task_range),
          K_(last_output), K_(has_output));
    } else if (!candidate_) {
      read_end = end;
    } else if (first_.type_ != IVF_ROW_TYPE_RAW_VECTOR || last_.type_ != IVF_ROW_TYPE_RAW_VECTOR
        || first_.cid_ != IVF_RAW_VECTOR_CID || last_.cid_ != IVF_RAW_VECTOR_CID
        || key.compare(last_) > 0) {
      ret = OB_INVALID_DATA;
      LOG_WARN("IVF RAW reuse candidate does not cover output key", K(ret), K(key),
          K_(active_range), K_(first), K_(last));
    } else {
      // Candidate ranges are (first_, last_]. The preceding boundary key
      // remains ordinary output, even when it belongs to another source.
      const bool before = key.compare(first_) <= 0;
      const uint64_t boundary = before ? first_.vid_ : last_.vid_;
      int64_t low = begin, high = end;
      while (low < high) {
        const int64_t mid = low + (high - low) / 2;
        if (vids.at(mid) <= boundary) { low = mid + 1; }
        else { high = mid; }
      }
      if (before) {
        read_end = low;
      } else if (low <= begin || vids.at(low - 1) != last_.vid_
          || candidate_rows_ <= consumed_ || low - begin != candidate_rows_ - consumed_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("incomplete IVF RAW reuse key span", K(ret), K_(active_range),
            K(begin), K(low), K(end), K_(last), K_(consumed), K_(candidate_rows));
      } else {
        for (int64_t i = begin + 1; OB_SUCC(ret) && i < low; ++i) {
          if (vids.at(i - 1) >= vids.at(i)) {
            ret = OB_INVALID_DATA;
            LOG_WARN("unordered IVF RAW reuse keys", K(ret), K_(active_range), K(i),
                "previous_vid", vids.at(i - 1), "vid", vids.at(i));
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t count = low - begin;
          const ObIVFReuseKey last = last_;
          consumed_ += count;
          if (OB_SUCC(ret = write_candidate())) {
            rows_ += count;
            last_output_ = last;
            has_output_ = true;
            reused_rows = count;
            read_end = low;
            ret = next_candidate();
          }
        }
      }
    }
  }
  error_ = ret;
  return ret;
}

int ObIVFBlockReuseWriter::finish()
{
  if (error_ == OB_SUCCESS && (writer_ == nullptr || finished_ || candidate_)) {
    error_ = OB_STATE_NOT_MATCH;
    const int ret = error_;
    LOG_WARN("unfinished IVF reuse output", K(ret), K_(task_range),
        K_(active_range), K_(finished), K_(candidate), K_(consumed), K_(candidate_rows), KP_(writer));
  }
  if (error_ == OB_SUCCESS && (reused_macros_ > 0 || copied_micros_ > 0)) {
    LOG_INFO("IVF physical block reuse finished", K_(rows), K_(reused_macros), K_(copied_micros),
        K_(macro_payload_bytes), K_(micro_read_bytes),
        "first_type", task_range_.first_.type_, "last_type", task_range_.last_.type_);
  }
  finished_ = true;
  return error_;
}
} // namespace blocksstable
} // namespace oceanbase
