/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_macro_block_writer.h"

namespace oceanbase
{
namespace blocksstable
{

ObIvfMacroBlockWriter::ObIvfMacroBlockWriter(int64_t posting_micro_block_size_limit)
  : ObMacroBlockWriter(true /* is_need_macro_buffer */),
    row_desc_(),
    ivf_data_store_desc_(),
    schema_micro_block_size_(0),
    previous_row_type_(-1),
    previous_posting_cid_(-1),
    appended_row_count_(0),
    posting_counts_(nullptr),
    first_posting_cid_(0),
    posting_split_size_(0),
    posting_cluster_payload_bytes_(0),
    posting_cluster_is_large_(false),
    posting_micro_block_size_limit_(posting_micro_block_size_limit)
{
}

ObIvfMacroBlockWriter::~ObIvfMacroBlockWriter()
{
  reset();
}

void ObIvfMacroBlockWriter::reset()
{
  ObMacroBlockWriter::reset();
  row_desc_.reset();
  ivf_data_store_desc_.reset();
  schema_micro_block_size_ = 0;
  previous_row_type_ = -1;
  previous_posting_cid_ = -1;
  appended_row_count_ = 0;
  posting_counts_ = nullptr;
  first_posting_cid_ = 0;
  posting_split_size_ = 0;
  posting_cluster_payload_bytes_ = 0;
  posting_cluster_is_large_ = false;
}

int ObIvfMacroBlockWriter::open(
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    const share::ObPreWarmerParam &pre_warm_param,
    ObISSTableObjectCleaner &object_cleaner,
    ObIMacroBlockFlushCallback *callback,
    ObIMacroBlockValidator *validator,
    ObIODevice *device_handle)
{
  row_desc_.reset();
  previous_row_type_ = -1;
  previous_posting_cid_ = -1;
  appended_row_count_ = 0;
  posting_counts_ = nullptr;
  first_posting_cid_ = 0;
  posting_split_size_ = 0;
  posting_cluster_payload_bytes_ = 0;
  posting_cluster_is_large_ = false;
  return ObMacroBlockWriter::open(data_store_desc,
                                  parallel_idx,
                                  macro_seq_param,
                                  pre_warm_param,
                                  object_cleaner,
                                  callback,
                                  validator,
                                  device_handle);
}

int ObIvfMacroBlockWriter::open(
    const ObIvfSSTableRowDesc &row_desc,
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    const share::ObPreWarmerParam &pre_warm_param,
    ObISSTableObjectCleaner &object_cleaner,
    ObIMacroBlockFlushCallback *callback,
    ObIMacroBlockValidator *validator,
    ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row_desc.is_valid() || !data_store_desc.is_valid() || posting_micro_block_size_limit_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable row descriptor", K(ret), K(row_desc));
  } else if (OB_FAIL(ivf_data_store_desc_.shallow_copy(data_store_desc))) {
    LOG_WARN("failed to copy IVF writer descriptor", K(ret));
  } else {
    row_desc_ = row_desc;
    schema_micro_block_size_ = data_store_desc.get_micro_block_size();
    ivf_data_store_desc_.need_pre_warm_ = data_store_desc.need_pre_warm_;
    ivf_data_store_desc_.need_build_hash_index_for_micro_block_ = data_store_desc.need_build_hash_index_for_micro_block_;
    // Reserve the encoder's largest soft budget once. Each append below sets
    // the current row type's split target without modifying the shared desc.
    const int64_t posting_limit = MAX(schema_micro_block_size_,
        data_store_desc.get_micro_block_size_limit() - schema_micro_block_size_);
    ivf_data_store_desc_.micro_block_size_ = MAX(schema_micro_block_size_,
        MIN(posting_micro_block_size_limit_, posting_limit));
    previous_row_type_ = -1;
    previous_posting_cid_ = -1;
    appended_row_count_ = 0;
    posting_counts_ = nullptr;
    first_posting_cid_ = 0;
    posting_split_size_ = 0;
    posting_cluster_payload_bytes_ = 0;
    posting_cluster_is_large_ = false;
    if (OB_FAIL(ObMacroBlockWriter::open(ivf_data_store_desc_,
                                         parallel_idx,
                                         macro_seq_param,
                                         pre_warm_param,
                                         object_cleaner,
                                         callback,
                                         validator,
                                         device_handle))) {
      LOG_WARN("failed to open IVF macro block writer", K(ret), K(row_desc));
      row_desc_.reset();
      previous_row_type_ = -1;
      appended_row_count_ = 0;
    }
  }
  return ret;
}

int ObIvfMacroBlockWriter::set_posting_counts(
    const common::ObIArray<uint32_t> &counts, int64_t first_cid)
{
  int ret = OB_SUCCESS;
  if (data_store_desc_ == nullptr || !row_desc_.is_valid()) {
    ret = OB_NOT_INIT;
  } else if (first_cid < 0 || counts.empty() || counts.count() > INT64_MAX - first_cid) {
    ret = OB_INVALID_ARGUMENT;
  } else if (appended_row_count_ != 0) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    posting_counts_ = &counts;
    first_posting_cid_ = first_cid;
  }
  return ret;
}

int64_t ObIvfMacroBlockWriter::posting_size_limit() const
{
  // The generic physical limit reserves only 1 KiB. A nearly macro-sized
  // FLAT micro-block must also leave room for its local index and macro meta.
  // Reserve one schema-sized page for those structures before choosing a cap.
  const int64_t limit = MAX(schema_micro_block_size_,
      data_store_desc_->get_micro_block_size_limit() - schema_micro_block_size_);
  return MAX(schema_micro_block_size_,
      MIN(posting_micro_block_size_limit_, limit));
}

int64_t ObIvfMacroBlockWriter::get_posting_count(int64_t cid) const
{
  return posting_counts_ != nullptr && cid >= first_posting_cid_
      && cid - first_posting_cid_ < posting_counts_->count()
      ? MAX(1L, static_cast<int64_t>(posting_counts_->at(cid - first_posting_cid_))) : 1;
}

int64_t ObIvfMacroBlockWriter::estimate_posting_target(int64_t row_count, int64_t row_size) const
{
  const int64_t limit = posting_size_limit();
  // One schema-sized allowance covers micro/column headers. Saturate before
  // multiplication and alignment, including very large cardinality hints.
  int64_t target = limit;
  if (row_count <= (limit - schema_micro_block_size_) / row_size) {
    const int64_t bytes = row_count * row_size;
    target = MIN(limit, schema_micro_block_size_
        + ((bytes + schema_micro_block_size_ - 1) / schema_micro_block_size_) * schema_micro_block_size_);
  }
  return target;
}

int64_t ObIvfMacroBlockWriter::update_posting_target(const ObDatumRow &row, int64_t cid)
{
  const int64_t payload_size = MAX(1L, static_cast<int64_t>(
      row.storage_datums_[row_desc_.payload_col_idx_].len_));
  // In-memory datum/key overhead is a safety allowance, not an estimate of
  // encoded posting size. Using it for classification fragments short codes.
  const int64_t row_size = payload_size
      + row.get_column_count() * (sizeof(int64_t) + sizeof(uint32_t)) + 32;
  const int64_t limit = posting_size_limit();
  if (previous_row_type_ != IVF_ROW_TYPE_LOW_PRECISION_VECTOR || previous_posting_cid_ != cid) {
    const int64_t count = get_posting_count(cid);
    posting_cluster_payload_bytes_ = 0;
    posting_cluster_is_large_ = count > schema_micro_block_size_ / payload_size;
    posting_split_size_ = posting_cluster_is_large_
        ? estimate_posting_target(count, payload_size) : schema_micro_block_size_;
  }
  // Missing/underestimated hints are optional: promote after observing enough
  // payload bytes, even if a prefix has already been sealed at the schema size.
  // This counter belongs to the CID, not the current micro-block.
  if (!posting_cluster_is_large_
      && posting_cluster_payload_bytes_ > schema_micro_block_size_ - MIN(payload_size, schema_micro_block_size_)) {
    posting_cluster_is_large_ = true;
    posting_split_size_ = estimate_posting_target(1,
        posting_cluster_payload_bytes_ + MIN(payload_size, limit));
  }
  const int64_t current_size = micro_writer_->get_block_size();
  if (posting_cluster_is_large_ && posting_split_size_ < limit
      && row_size >= posting_split_size_ - current_size) {
    const int64_t needed = current_size >= limit - MIN(row_size, limit)
        ? limit : current_size + row_size;
    posting_split_size_ = MIN(limit, MAX(needed,
        posting_split_size_ > limit / 2 ? limit : 2 * posting_split_size_));
  }
  return row_size;
}

int ObIvfMacroBlockWriter::append_row(
    const ObDatumRow &row,
    const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  if (!row_desc_.is_valid()) {
    if (OB_FAIL(ObMacroBlockWriter::append_row(row, curr_macro_desc))) {
      LOG_WARN("failed to append generic IVF row", K(ret), K(row));
    } else {
      ++appended_row_count_;
    }
  } else {
    ObBlockWriterConcurrentGuard guard(concurrent_lock_);
    if (OB_FAIL(inner_append_row(row, curr_macro_desc))) {
      LOG_WARN("failed to append IVF row", K(ret), K(row), K_(row_desc));
    }
  }
  return ret;
}

int ObIvfMacroBlockWriter::append_batch(
    const ObBatchDatumRows &datum_rows,
    const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  if (OB_UNLIKELY(datum_rows.row_count_ < 0
      || (datum_rows.row_count_ > 0 && datum_rows.get_column_count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF batch rows", K(ret), K(datum_rows));
  } else if (0 == datum_rows.row_count_) {
  } else if (!row_desc_.is_valid()) {
    if (OB_FAIL(ObMacroBlockWriter::append_batch(datum_rows, curr_macro_desc))) {
      LOG_WARN("failed to append generic IVF batch", K(ret), K(datum_rows));
    } else {
      appended_row_count_ += datum_rows.row_count_;
    }
  } else if (OB_FAIL(row.init(datum_rows.get_column_count()))) {
    LOG_WARN("failed to initialize IVF batch row", K(ret),
             K(datum_rows.get_column_count()));
  } else {
    ObBlockWriterConcurrentGuard guard(concurrent_lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_rows.row_count_; ++i) {
      if (OB_FAIL(datum_rows.to_datum_row(i, row))) {
        LOG_WARN("failed to materialize IVF batch row", K(ret), K(i), K(datum_rows));
      } else if (OB_FAIL(inner_append_row(row, curr_macro_desc))) {
        LOG_WARN("failed to append IVF batch row", K(ret), K(i), K(row));
      }
    }
  }
  return ret;
}

int ObIvfMacroBlockWriter::inner_append_row(
    const ObDatumRow &row,
    const ObMacroBlockDesc *curr_macro_desc)
{
  int ret = OB_SUCCESS;
  int64_t row_type = -1;
  int64_t cid = -1;
  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IVF macro block writer is not opened", K(ret));
  } else if (OB_UNLIKELY(!row_desc_.is_valid_for_row(row.get_column_count()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable row", K(ret), K(row), K_(row_desc));
  } else if (OB_UNLIKELY(
                 row.storage_datums_[row_desc_.row_type_col_idx_].is_null())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("IVF SSTable row type is null", K(ret), K(row), K_(row_desc));
  } else if (FALSE_IT(row_type =
                 row.storage_datums_[row_desc_.row_type_col_idx_].get_int())) {
  } else if (OB_UNLIKELY(row_type < IVF_ROW_TYPE_CENTROID
                         || row_type >= IVF_ROW_TYPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF SSTable row type", K(ret), K(row_type), K(row));
  } else if (row_type == IVF_ROW_TYPE_LOW_PRECISION_VECTOR
      && (row.storage_datums_[row_desc_.centroid_id_col_idx_].is_null()
          || row.storage_datums_[row_desc_.centroid_id_col_idx_].is_ext()
          || row.storage_datums_[row_desc_.centroid_id_col_idx_].get_int() < 0
          || row.storage_datums_[row_desc_.payload_col_idx_].is_null()
          || row.storage_datums_[row_desc_.payload_col_idx_].is_ext())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF posting key or payload", K(ret), K(row));
  } else {
    const bool posting = row_type == IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
    cid = posting ? row.storage_datums_[row_desc_.centroid_id_col_idx_].get_int() : -1;
    const bool new_cid = posting && (previous_row_type_ != row_type || previous_posting_cid_ != cid);
    const int64_t payload_size = posting ? MAX(1L, static_cast<int64_t>(
        row.storage_datums_[row_desc_.payload_col_idx_].len_)) : 0;
    const bool next_large = new_cid && get_posting_count(cid) > schema_micro_block_size_ / payload_size;
    // Small lists share schema-sized blocks. Preserve boundaries around known
    // large lists so their larger target does not absorb unrelated small CIDs.
    // Different row types must always remain in separate micro-blocks.
    if (previous_row_type_ >= 0
        && (previous_row_type_ != row_type
            || (new_cid && (posting_cluster_is_large_ || next_large)))
        && OB_FAIL(inner_flush_micro_block())) {
      LOG_WARN("failed to flush IVF type/CID boundary", K(ret), K(row_type), K(cid));
    } else {
      const int64_t row_size = posting ? update_posting_target(row, cid) : 0;
      const int64_t split_size = posting ? posting_split_size_ : schema_micro_block_size_;
      ivf_data_store_desc_.micro_block_size_ = split_size;
      // The compressed-size estimate may be arbitrarily optimistic. Bound
      // encoded postings independently with the buffered unencoded bytes,
      // leaving one schema-sized allowance for encoding headers/metadata.
      // A bad estimate may produce more blocks, but must not overfill a block.
      const int64_t unencoded_limit = posting
          ? MAX(schema_micro_block_size_, posting_size_limit() - schema_micro_block_size_) : 0;
      const bool unencoded_buffer_full = posting && micro_writer_->micro_block_row_data_buffered()
          && micro_writer_->get_original_size() > unencoded_limit - MIN(row_size, unencoded_limit);
      // Do not consume the local index/meta reservation with the last row of
      // a full posting block. The generic split check runs after appending.
      if (posting && micro_writer_->get_row_count() > 0
          && ((posting_cluster_is_large_
               && micro_writer_->get_block_size() > split_size - MIN(row_size, split_size))
              || unencoded_buffer_full)
          && OB_FAIL(inner_flush_micro_block())) {
        LOG_WARN("failed to flush IVF posting block before append", K(ret), K(cid));
      } else if (OB_FAIL(ObMacroBlockWriter::append_row_inner(row, curr_macro_desc))) {
        LOG_WARN("failed to append IVF row", K(ret), K(row_type), K(cid));
      } else {
        previous_row_type_ = row_type;
        previous_posting_cid_ = cid;
        ++appended_row_count_;
        if (posting && !posting_cluster_is_large_) {
          // update_posting_target promotes before this sum exceeds the schema
          // target. Once large, no per-row byte counter is needed.
          posting_cluster_payload_bytes_ += payload_size;
        }
        // Keep CS small-list/non-posting buffering near its ordinary 2x schema estimate
        // budget even though the encoder reserved the larger posting budget.
        // This is a soft estimate: one large row can exceed it; hard limits
        // remain owned by the existing encoder. PAX keeps its normal budget.
        const int64_t hard_limit = data_store_desc_->get_micro_block_size_limit();
        const int64_t estimate_limit = posting && posting_cluster_is_large_ ? split_size
            : data_store_desc_->get_row_store_type() == CS_ENCODING_ROW_STORE
              ? (schema_micro_block_size_ > hard_limit / 2 ? hard_limit : 2 * schema_micro_block_size_) : 0;
        if (estimate_limit > 0 && micro_writer_->get_row_count() > 0
            && micro_writer_->get_block_size() >= estimate_limit
            && OB_FAIL(inner_flush_micro_block())) {
          LOG_WARN("failed to flush full IVF micro block", K(ret), K(row_type), K(cid));
        }
      }
    }
  }
  return ret;
}

int ObIvfMacroBlockWriter::flush_micro_block()
{
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  return inner_flush_micro_block();
}

int ObIvfMacroBlockWriter::inner_flush_micro_block()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(micro_writer_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("micro writer is null", K(ret));
  } else if (OB_UNLIKELY(micro_writer_->get_row_count() == 0)) {
    LOG_INFO("micro block has no rows, skip flush");
  } else if (OB_FAIL(build_micro_block())) {
    LOG_WARN("fail to flush current micro block", K(ret));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
