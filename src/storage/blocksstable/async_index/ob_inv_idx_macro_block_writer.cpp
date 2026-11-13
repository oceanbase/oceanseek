/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_writer.h"

#include "lib/checksum/ob_crc64.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_schema.h"
#include "share/search_index/ob_search_index_config_filter.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/ob_block_writer_concurrent_guard.h"
#include "storage/blocksstable/ob_imacro_block_flush_callback.h"
#include "storage/blocksstable/ob_macro_block_flusher.h"
#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/blocksstable/encoding/ob_micro_block_encoder.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/backup/ob_backup_data_struct.h"

namespace oceanbase
{
namespace blocksstable
{

// ------------------------------ ObInvIdxMicroBlockStreamDesc ------------------------------

ObInvIdxMacroBlockWriter::ObInvIdxMicroBlockStreamDesc::ObInvIdxMicroBlockStreamDesc()
  : raw_desc_(),
    locator_(),
    uncompressed_payload_(nullptr),
    uncompressed_payload_size_(0)
{}

void ObInvIdxMacroBlockWriter::ObInvIdxMicroBlockStreamDesc::reset()
{
  raw_desc_.reset();
  locator_.reset();
  uncompressed_payload_ = nullptr;
  uncompressed_payload_size_ = 0;
}

bool ObInvIdxMacroBlockWriter::ObInvIdxMicroBlockStreamDesc::is_valid() const
{
  return raw_desc_.is_valid()
      && locator_.is_valid()
      && nullptr != uncompressed_payload_
      && uncompressed_payload_size_ > 0
      && uncompressed_payload_size_ == raw_desc_.uncompressed_size_
      && locator_.size_ == raw_desc_.header_size_ + raw_desc_.payload_size_;
}

// ------------------------------ ObInvIdxMicroBlockPairDesc ------------------------------

ObInvIdxMacroBlockWriter::ObInvIdxMicroBlockPairDesc::ObInvIdxMicroBlockPairDesc()
  : posting_(),
    posting_value_(),
    index_desc_(),
    index_block_upper_bound_(0)
{}

void ObInvIdxMacroBlockWriter::ObInvIdxMicroBlockPairDesc::reset()
{
  posting_.reset();
  posting_value_.reset();
  index_desc_.reset();
  index_block_upper_bound_ = 0;
}

bool ObInvIdxMacroBlockWriter::ObInvIdxMicroBlockPairDesc::is_valid() const
{
  const bool posting_value_consistent = index_desc_.posting_value_locator_.is_valid()
      ? posting_value_.is_valid()
          && index_desc_.posting_value_locator_.offset_ == posting_value_.locator_.offset_
          && index_desc_.posting_value_locator_.size_ == posting_value_.locator_.size_
      : !posting_value_.is_valid();
  return posting_.is_valid()
      && index_desc_.is_valid()
      && index_desc_.macro_id_.is_valid()
      && index_desc_.posting_locator_.offset_ == posting_.locator_.offset_
      && index_desc_.posting_locator_.size_ == posting_.locator_.size_
      && index_desc_.data_checksum_ == posting_.raw_desc_.block_checksum_
      && posting_value_consistent;
}

// ------------------------------ ObInvIdxDefaultMacroBlockFlusher ------------------------------

ObInvIdxMacroBlockWriter::ObInvIdxDefaultMacroBlockFlusher::ObInvIdxDefaultMacroBlockFlusher()
  : data_store_desc_(nullptr),
    macro_handle_(nullptr),
    device_handle_(nullptr)
{}

ObInvIdxMacroBlockWriter::ObInvIdxDefaultMacroBlockFlusher::~ObInvIdxDefaultMacroBlockFlusher()
{}

void ObInvIdxMacroBlockWriter::ObInvIdxDefaultMacroBlockFlusher::set_param(
    const ObDataStoreDesc &data_store_desc,
    ObStorageObjectHandle &macro_handle,
    ObIODevice *device_handle)
{
  data_store_desc_ = &data_store_desc;
  macro_handle_ = &macro_handle;
  device_handle_ = device_handle;
}

int ObInvIdxMacroBlockWriter::ObInvIdxDefaultMacroBlockFlusher::write_disk(
    ObMacroBlock &macro_block,
    const bool is_close_flush)
{
  int ret = OB_SUCCESS;
  UNUSED(is_close_flush);
  if (OB_UNLIKELY(OB_ISNULL(data_store_desc_) || OB_ISNULL(macro_handle_))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null inverted index macro block flusher argument",
        K(ret), KP_(data_store_desc), KP_(macro_handle));
  } else if (is_flush_macro_exec_mode(data_store_desc_->get_exec_mode())
      && data_store_desc_->get_need_submit_io()) {
    ObStorageObjectWriteInfo object_info;
    object_info.buffer_ = macro_block.get_data_buf();
    object_info.offset_ = 0;
    const int64_t data_buf_size = upper_align(macro_block.get_data_size(), DIO_ALIGN_SIZE);
    if (GCTX.is_shared_storage_mode() && OB_LIKELY(data_buf_size > macro_block.get_data_size())) {
      MEMSET(macro_block.get_data_buf() + macro_block.get_data_size(),
          0,
          data_buf_size - macro_block.get_data_size());
    }
    if (backup::ObBackupDeviceMacroBlockId::is_backup_block_file(
        macro_handle_->get_macro_id().first_id())) {
      object_info.size_ = macro_block.get_data_capacity();
    } else {
      object_info.size_ = data_buf_size;
    }
    object_info.mtl_tenant_id_ = MTL_ID();
    object_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    object_info.io_desc_.set_sealed();
    object_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_MACRO_BLOCK_WRITE_IO);
    object_info.io_timeout_ms_ =
        std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
    object_info.device_handle_ = device_handle_;
    object_info.has_backup_device_handle_ = OB_NOT_NULL(device_handle_);
    object_info.write_strategy_ = data_store_desc_->get_io_write_strategy();
    if (OB_FAIL(macro_handle_->async_write(object_info))) {
      STORAGE_LOG(WARN, "fail to async write inverted index macro block",
          K(ret), KPC_(macro_handle), K(object_info));
    } else {
      macro_block.print_flush_log(*macro_handle_);
    }
  }
  return ret;
}

// ------------------------------ ObInvIdxMacroBlockWriter ------------------------------

ObInvIdxMacroBlockWriter::ObInvIdxMacroBlockWriter()
  : data_store_desc_(nullptr),
    inv_idx_row_desc_(),
    term_col_count_(0),
    parallel_idx_(0),
    object_cleaner_(nullptr),
    callback_(nullptr),
    is_opened_(false),
    posting_ctx_(),
    posting_value_param_(),
    posting_encoder_(),
    posting_value_writer_(),
    posting_micro_writer_(),
    posting_value_micro_writer_(),
    posting_micro_buffer_helper_(),
    posting_value_micro_buffer_helper_(),
    index_block_builder_(),
    macro_block_(),
    macro_handle_(),
    device_handle_(nullptr),
    macro_seq_generator_(nullptr),
    last_macro_seq_(-1),
    block_write_ctx_(),
    merge_block_info_(),
    current_term_(),
    has_pending_rows_(false),
    micro_term_groups_(),
    pending_term_group_(),
    micro_index_size_(0),
    pending_index_size_(0),
    current_micro_stats_(),
    macro_stats_(),
    seg_inv_meta_(),
    concurrent_lock_(false),
    allocator_("InvIdxMacBlk"),
    current_term_allocator_("InvIdxCurTerm"),
    candidate_allocator_("InvIdxCand"),
    macro_allocator_("InvIdxMacro"),
    scratch_allocator_("InvIdxScratch")
{}

ObInvIdxMacroBlockWriter::~ObInvIdxMacroBlockWriter()
{
  reset();
}

void ObInvIdxMacroBlockWriter::reset()
{
  inner_reset();
  allocator_.reset();
  current_term_allocator_.reset();
  candidate_allocator_.reset();
  macro_allocator_.reset();
  scratch_allocator_.reset();
}

void ObInvIdxMacroBlockWriter::inner_reset()
{
  data_store_desc_ = nullptr;
  inv_idx_row_desc_.reset();
  term_col_count_ = 0;
  parallel_idx_ = 0;
  object_cleaner_ = nullptr;
  callback_ = nullptr;
  if (OB_NOT_NULL(macro_seq_generator_)) {
    macro_seq_generator_->~ObMacroSeqGenerator();
    allocator_.free(macro_seq_generator_);
    macro_seq_generator_ = nullptr;
  }
  last_macro_seq_ = -1;
  posting_encoder_.reset();
  posting_value_writer_.reset();
  posting_ctx_.reset();
  posting_value_param_ = ObPostingValueBlockWriteParam();
  posting_micro_writer_.reset();
  posting_value_micro_writer_.reset();
  posting_micro_buffer_helper_.reset();
  posting_value_micro_buffer_helper_.reset();
  index_block_builder_.reset();
  macro_block_.reset();
  macro_handle_.reset();
  device_handle_ = nullptr;
  block_write_ctx_.reset();
  merge_block_info_.reset();
  current_term_.reset();
  has_pending_rows_ = false;
  micro_term_groups_.reset();
  pending_term_group_.reset();
  micro_index_size_ = 0;
  pending_index_size_ = 0;
  current_micro_stats_.reset();
  macro_stats_.reset();
  seg_inv_meta_.reset();
  is_opened_ = false;
}

int ObInvIdxMacroBlockWriter::open(
    const ObInvIdxRowDesc &inv_idx_row_desc,
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    const share::ObPreWarmerParam &pre_warm_param,
    ObISSTableObjectCleaner &object_cleaner,
    ObIMacroBlockFlushCallback *callback,
    const bool write_clustered_micro_idx)
{
  int ret = OB_SUCCESS;
  ObSSTableIndexBuilder *sstable_index_builder = data_store_desc.sstable_index_builder_;
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "inverted index macro block writer is already opened", K(ret));
  } else if (OB_UNLIKELY(!inv_idx_row_desc.is_valid() || !data_store_desc.is_valid()
      || parallel_idx < 0 || !macro_seq_param.is_valid() || !pre_warm_param.is_valid()
      || OB_ISNULL(sstable_index_builder))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument to open inverted index macro block writer", K(ret),
        K(inv_idx_row_desc), K(parallel_idx), K(macro_seq_param), K(pre_warm_param),
        KP(sstable_index_builder));
  } else {
    inner_reset();
    sstable_index_builder->set_is_async_index(true);
    data_store_desc_ = &data_store_desc;
    object_cleaner_ = &object_cleaner;
    callback_ = callback;
    inv_idx_row_desc_ = inv_idx_row_desc;
    // schema rowkey is (term..., doc_id); the grouping term excludes the trailing doc_id column.
    term_col_count_ = static_cast<int16_t>(data_store_desc.get_schema_rowkey_col_cnt() - 1);
    parallel_idx_ = parallel_idx;
    device_handle_ = nullptr;  // allocate macro id via OB_STORAGE_OBJECT_MGR at flush time
    const common::ObIArray<share::schema::ObColDesc> &col_descs =
        data_store_desc.get_full_stored_col_descs();
    int64_t first_macro_seq = -1;
    int64_t logic_macro_seq = -1;
    if (OB_FAIL(inv_idx_row_desc_.build_encoding_params(
        col_descs, posting_ctx_, posting_value_param_))) {
      STORAGE_LOG(WARN, "fail to build inverted index encoding params", K(ret), K(inv_idx_row_desc_));
    } else if (INV_IDX_TYPE_SEARCH_TEXT == inv_idx_row_desc_.index_type_
        && OB_UNLIKELY(!is_supported_search_text_write_layout(
            posting_ctx_.has_token_freq() && posting_ctx_.has_doc_length(),
            posting_value_param_.has_nested_key()))) {
      // Search Text currently always writes a three-column posting; the value can only be POS.
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "unexpected search text encoding layout", K(ret),
          K(inv_idx_row_desc_), K(posting_ctx_), K(posting_value_param_));
    } else if (OB_UNLIKELY(!is_pre_alloc())) {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "inverted index macro writer requires the pre-alloc path",
          K(ret), K(data_store_desc.data_store_type_));
    } else if (OB_FAIL(posting_encoder_.init(
        posting_ctx_, allocator_, ObInvIdxLogicalBlockMeta::MAX_FRONTIER_PAIR_COUNT))) {
      STORAGE_LOG(WARN, "fail to init posting block encoder", K(ret));
    } else if (OB_FAIL(posting_micro_writer_.init(allocator_))) {
      STORAGE_LOG(WARN, "fail to init posting data micro block writer", K(ret));
    } else if (has_posting_value()
        && OB_FAIL(posting_value_writer_.init(posting_value_param_, allocator_))) {
      STORAGE_LOG(WARN, "fail to init posting value block writer", K(ret));
    } else if (has_posting_value()
        && OB_FAIL(posting_value_micro_writer_.init(allocator_))) {
      STORAGE_LOG(WARN, "fail to init posting value data micro block writer", K(ret));
    } else if (OB_FAIL(posting_micro_buffer_helper_.open(data_store_desc))) {
      STORAGE_LOG(WARN, "fail to open posting micro block buffer helper", K(ret));
    } else if (has_posting_value()
        && OB_FAIL(posting_value_micro_buffer_helper_.open(data_store_desc))) {
      STORAGE_LOG(WARN, "fail to open posting value micro block buffer helper", K(ret));
    } else if (OB_FAIL(init_macro_seq_generator(macro_seq_param))) {
      STORAGE_LOG(WARN, "fail to init macro seq generator", K(ret));
    } else if (OB_FAIL(macro_seq_generator_->get_next(first_macro_seq))) {
      STORAGE_LOG(WARN, "fail to get first macro seq", K(ret));
    } else if (OB_FAIL(build_logic_macro_seq(first_macro_seq, logic_macro_seq))) {
      STORAGE_LOG(WARN, "fail to build inverted index logical macro sequence", K(ret));
    } else if (OB_FAIL(macro_block_.init(data_store_desc, logic_macro_seq, merge_block_info_, 0))) {
      STORAGE_LOG(WARN, "fail to init macro block", K(ret));
    } else if (OB_FAIL(index_block_builder_.init(data_store_desc, *sstable_index_builder,
        macro_seq_param, pre_warm_param, write_clustered_micro_idx, callback_, allocator_))) {
      STORAGE_LOG(WARN, "fail to init inverted index block builder", K(ret));
    } else if (OB_FAIL(init_inv_meta())) {
      STORAGE_LOG(WARN, "fail to init inv meta", K(ret), K(inv_idx_row_desc_));
    } else {
      last_macro_seq_ = first_macro_seq;
      is_opened_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    inner_reset();
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::init_macro_seq_generator(
    const blocksstable::ObMacroSeqParam &macro_seq_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(macro_seq_generator_)) {
    if (blocksstable::ObMacroSeqParam::SEQ_TYPE_INC == macro_seq_param.seq_type_) {
      macro_seq_generator_ = OB_NEWx(blocksstable::ObMacroIncSeqGenerator, &allocator_);
    } else if (blocksstable::ObMacroSeqParam::SEQ_TYPE_SKIP == macro_seq_param.seq_type_) {
      macro_seq_generator_ = OB_NEWx(blocksstable::ObMacroSkipSeqGenerator, &allocator_);
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "macro sequence generator not supported", K(ret), K(macro_seq_param));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(macro_seq_generator_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc macro seq generator", K(ret), K(macro_seq_param));
    } else if (OB_FAIL(macro_seq_generator_->init(macro_seq_param))) {
      STORAGE_LOG(WARN, "fail to init macro seq generator", K(ret), K(macro_seq_param));
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::init_inv_meta()
{
  int ret = OB_SUCCESS;
  ObInvIdxStatsMeta initial_meta;
  current_micro_stats_.reset();
  macro_stats_.reset();
  seg_inv_meta_.reset();
  if (OB_ISNULL(data_store_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "data store desc is null when initializing inverted index stats", K(ret));
  } else {
    const common::ObIArray<share::schema::ObColDesc> &col_descs =
        data_store_desc_->get_full_stored_col_descs();
    if (OB_UNLIKELY(inv_idx_row_desc_.doc_id_col_idx_ < 0
        || inv_idx_row_desc_.doc_id_col_idx_ >= col_descs.count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "doc id column index out of range", K(ret), K(inv_idx_row_desc_),
          "column_count", col_descs.count());
    } else {
      initial_meta.index_type_ = inv_idx_row_desc_.index_type_;
      initial_meta.has_relevance_params_ = inv_idx_row_desc_.token_freq_col_idx_ >= 0;
      initial_meta.has_position_list_ = inv_idx_row_desc_.pos_list_col_idx_ >= 0;
      initial_meta.has_nested_key_ = inv_idx_row_desc_.nested_key_col_idx_ >= 0;
      initial_meta.doc_id_obj_meta_ = col_descs.at(inv_idx_row_desc_.doc_id_col_idx_).col_type_;
      if (OB_FAIL(current_micro_stats_.inv_meta_.init(initial_meta))) {
        STORAGE_LOG(WARN, "failed to initialize micro stats", K(ret));
      } else if (OB_FAIL(macro_stats_.inv_meta_.init(initial_meta))) {
        STORAGE_LOG(WARN, "failed to initialize macro stats", K(ret));
      } else if (OB_FAIL(seg_inv_meta_.init(initial_meta))) {
        STORAGE_LOG(WARN, "failed to initialize segment stats", K(ret));
      }
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::append_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "inverted index macro block writer is not opened", K(ret));
  } else if (OB_FAIL(inner_append_row(row))) {
    STORAGE_LOG(WARN, "fail to append row to inverted index macro block writer", K(ret), K(row));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::inner_append_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey term;
  int cmp_ret = 0;
  if (INV_IDX_TYPE_SEARCH_TEXT == inv_idx_row_desc_.index_type_
      && OB_FAIL(check_search_text_row(row))) {
    STORAGE_LOG(WARN, "invalid search text row", K(ret), K(row));
  } else if (OB_FAIL(extract_term(row, term))) {
    STORAGE_LOG(WARN, "fail to extract inverted index term", K(ret), K(row));
  } else if (OB_FAIL(check_term_order(term))) {
    STORAGE_LOG(WARN, "fail to check inverted index input order", K(ret), K(term));
  } else {
    if (has_pending_rows_) {
      if (OB_FAIL(compare_term(term, current_term_, cmp_ret))) {
        STORAGE_LOG(WARN, "fail to compare row term with current term", K(ret),
            K(term), K_(current_term));
      } else if (cmp_ret > 0 || posting_encoder_.is_full() || posting_value_writer_.is_full()) {
        if (OB_FAIL(flush_logical_block())) {
          STORAGE_LOG(WARN, "fail to flush inverted index logical block before append row",
              K(ret), K(term), K_(current_term), K(cmp_ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!has_pending_rows_
        && OB_FAIL(term.deep_copy(current_term_, current_term_allocator_))) {
      STORAGE_LOG(WARN, "fail to set current inverted index term", K(ret), K(term));
    } else if (OB_FAIL(posting_encoder_.append_row(row))) {
      STORAGE_LOG(WARN, "fail to append row to posting encoder", K(ret), K(row));
    } else if (has_posting_value() && OB_FAIL(posting_value_writer_.append_row(row))) {
      STORAGE_LOG(WARN, "fail to append row to posting value writer", K(ret), K(row));
    } else {
      has_pending_rows_ = true;
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::append_batch(const ObBatchDatumRows &rows)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "inverted index macro block writer is not opened", K(ret));
  } else if (OB_UNLIKELY(rows.row_count_ < 0
      || (rows.row_count_ > 0 && rows.get_column_count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inverted index batch rows", K(ret), K(rows));
  } else if (0 == rows.row_count_) {
  } else if (OB_FAIL(row.init(rows.get_column_count()))) {
    STORAGE_LOG(WARN, "fail to init datum row for inverted index batch append",
        K(ret), K(rows.get_column_count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.row_count_; ++i) {
      if (OB_FAIL(rows.to_datum_row(i, row))) {
        STORAGE_LOG(WARN, "fail to convert inverted index batch row", K(ret), K(i), K(rows));
      } else if (OB_FAIL(inner_append_row(row))) {
        STORAGE_LOG(WARN, "fail to append converted inverted index batch row", K(ret), K(i), K(row));
      }
    }
  }
  return ret;
}

// An encoded search text datum must not be NULL/NOP. PATH may be an empty string, while
// TOKEN/POS must not be (integer datums are never empty).
static int check_search_text_datum(const ObDatumRow &row, const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_idx >= row.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "search text column index out of row", K(ret), K(col_idx), K(row));
  } else {
    const ObStorageDatum &datum = row.storage_datums_[col_idx];
    const bool allow_empty = SEARCH_TEXT_PATH == col_idx;
    if (OB_UNLIKELY(datum.is_null() || datum.is_nop() || (!allow_empty && 0 == datum.len_))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "search text row has null, nop or empty datum", K(ret), K(col_idx), K(row));
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::check_search_text_row(const ObDatumRow &row) const
{
  int ret = OB_SUCCESS;
  // Validate only the encoded columns: the term prefix and the posting columns. Multi-version
  // columns are not stored by the inverted format and stay NOP in compaction rewrite rows
  // (the logical block iterator leaves them NOP and the merge iterator only fills the term).
  // Check before any append operation so an invalid row leaves no partial posting behind.
  const int64_t posting_col_idxs[] = {
      inv_idx_row_desc_.doc_id_col_idx_,
      inv_idx_row_desc_.token_freq_col_idx_,
      inv_idx_row_desc_.doc_length_col_idx_,
      inv_idx_row_desc_.pos_list_col_idx_,  // -1 for MATCH tables
  };
  if (OB_UNLIKELY(!row.is_valid() || term_col_count_ <= 0
      || row.get_column_count() < term_col_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid search text row", K(ret), K_(term_col_count), K(row));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < term_col_count_; ++i) {
    ret = check_search_text_datum(row, i);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(posting_col_idxs); ++i) {
    if (posting_col_idxs[i] >= 0) {
      ret = check_search_text_datum(row, posting_col_idxs[i]);
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::extract_term(const ObDatumRow &row, ObDatumRowkey &term) const
{
  int ret = OB_SUCCESS;
  term.reset();
  if (OB_UNLIKELY(!row.is_valid() || term_col_count_ <= 0
      || row.get_column_count() < term_col_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row to extract inverted index term", K(ret),
        K(row.is_valid()), K_(term_col_count), K(row.get_column_count()));
  } else if (OB_FAIL(term.assign(row.storage_datums_, term_col_count_))) {
    STORAGE_LOG(WARN, "fail to assign inverted index term rowkey", K(ret), K_(term_col_count));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::check_term_order(const ObDatumRowkey &term) const
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  ObDatumRowkey last_term;
  if (OB_UNLIKELY(!term.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid term to check order", K(ret), K(term));
  } else if (has_pending_rows_) {
    if (OB_FAIL(compare_term(term, current_term_, cmp_ret))) {
      STORAGE_LOG(WARN, "fail to compare current pending term", K(ret), K(term), K_(current_term));
    } else if (OB_UNLIKELY(cmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "inverted index input term order decreases", K(ret),
          K(term), K_(current_term), K(cmp_ret));
    }
  } else if (pending_term_group_.logical_block_metas_.count() > 0) {
    if (OB_FAIL(compare_term(term, pending_term_group_.term_, cmp_ret))) {
      STORAGE_LOG(WARN, "fail to compare last open micro block term", K(ret),
          K(term), K_(pending_term_group));
    } else if (OB_UNLIKELY(cmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "inverted index input term order decreases in open micro block",
          K(ret), K(term), K_(pending_term_group), K(cmp_ret));
    }
  } else if (macro_stats_.endkey_.is_valid()) {
    if (OB_FAIL(last_term.assign(macro_stats_.endkey_.datums_, term_col_count_))) {
      STORAGE_LOG(WARN, "fail to assign last macro term", K(ret), K_(macro_stats));
    } else if (OB_FAIL(compare_term(term, last_term, cmp_ret))) {
      STORAGE_LOG(WARN, "fail to compare last macro term", K(ret), K(term), K_(macro_stats));
    } else if (OB_UNLIKELY(cmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "inverted index input term order decreases after committed micro block",
          K(ret), K(term), K_(macro_stats), K(cmp_ret));
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::compare_term(
    const ObDatumRowkey &left,
    const ObDatumRowkey &right,
    int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (OB_UNLIKELY(!left.is_valid() || !right.is_valid()
      || left.get_datum_cnt() != term_col_count_ || right.get_datum_cnt() != term_col_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid term rowkey to compare", K(ret),
        K(left), K(right), K_(term_col_count));
  } else if (OB_FAIL(left.compare(right, data_store_desc_->get_datum_utils(), cmp_ret))) {
    STORAGE_LOG(WARN, "fail to compare inverted index term", K(ret), K(left), K(right));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::flush_logical_block()
{
  int ret = OB_SUCCESS;
  ObPostingBlockDesc posting_block;
  ObPostingValueBlockDesc posting_value_block;
  if (OB_UNLIKELY(!has_pending_rows_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "flush logical block called with no pending rows", K(ret));
  } else if (OB_FAIL(posting_encoder_.build_block(posting_block))) {
    STORAGE_LOG(WARN, "fail to build inverted index posting block", K(ret));
  } else if (has_posting_value()
      && OB_FAIL(posting_value_writer_.build_block(posting_value_block))) {
    STORAGE_LOG(WARN, "fail to build inverted index posting value block", K(ret));
  } else if (OB_FAIL(append_encoded_logical_block(posting_block, posting_value_block))) {
    STORAGE_LOG(WARN, "fail to append encoded logical block", K(ret), K(posting_block));
  } else {
    posting_encoder_.reuse();
    if (has_posting_value()) {
      posting_value_writer_.reuse();
    }
    current_term_.reset();
    current_term_allocator_.reuse();
    has_pending_rows_ = false;
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::append_encoded_logical_block(
    const ObPostingBlockDesc &posting_block,
    const ObPostingValueBlockDesc &posting_value_block)
{
  int ret = OB_SUCCESS;
  ObInvIdxLogicalBlockMeta meta;
  int64_t posting_block_idx = -1;
  int64_t posting_value_block_idx = -1;
  int64_t projected_pending_index_size = 0;
  bool need_flush = false;
  bool same_term = false;
  if (OB_UNLIKELY(!current_term_.is_valid() || !posting_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid encoded logical block argument", K(ret),
        K_(current_term), K(posting_block));
  } else if (has_posting_value()
      && OB_UNLIKELY(!posting_value_block.is_valid()
          || posting_block.doc_count_ != posting_value_block.row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "posting block and value block row count mismatch", K(ret),
        K(posting_block), K(posting_value_block));
  } else if (!has_posting_value() && OB_UNLIKELY(posting_value_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected posting value block for schema without posting value",
        K(ret), K(posting_value_block));
  } else {
    meta.block_idx_ = posting_micro_writer_.get_block_count();
    meta.doc_count_ = posting_block.doc_count_;
    meta.min_doc_id_ = posting_block.min_id_;
    meta.max_doc_id_ = posting_block.max_id_;
    meta.total_doc_length_ = posting_block.total_doc_length_;
    if (OB_NOT_NULL(posting_block.max_score_pairs_)
        && OB_FAIL(meta.frontier_pairs_.assign(*posting_block.max_score_pairs_))) {
      STORAGE_LOG(WARN, "fail to assign bm25 frontier pairs", K(ret), K(posting_block));
    }
  }

  if (FAILEDx(check_logical_block_order(meta, same_term))) {
    STORAGE_LOG(WARN, "fail to check logical block order before append", K(ret), K(meta));
  } else if (!same_term && OB_FAIL(finalize_term_group())) {
    STORAGE_LOG(WARN, "fail to finalize inverted index term group", K(ret), K_(current_term));
  } else if (OB_FAIL(need_flush_micro_block_pair(
      posting_block, posting_value_block, meta, need_flush, projected_pending_index_size))) {
    STORAGE_LOG(WARN, "fail to check inverted index micro block pair capacity", K(ret),
        K(posting_block), K(posting_value_block));
  } else if (need_flush) {
    if (OB_UNLIKELY(posting_micro_writer_.is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "empty micro block pair unexpectedly needs soft-limit flush", K(ret),
          K(posting_block), K(projected_pending_index_size));
    } else if (OB_FAIL(flush_micro_block_pair())) {
      STORAGE_LOG(WARN, "fail to flush inverted index micro block pair", K(ret));
    } else if (FALSE_IT(meta.block_idx_ = posting_micro_writer_.get_block_count())) {
    } else if (OB_FAIL(need_flush_micro_block_pair(
        posting_block, posting_value_block, meta, need_flush, projected_pending_index_size))) {
      STORAGE_LOG(WARN, "fail to recheck empty inverted index micro block pair", K(ret),
          K(posting_block), K(posting_value_block));
    } else if (OB_UNLIKELY(need_flush)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "empty micro block pair still needs soft-limit flush", K(ret),
          K(posting_block), K(projected_pending_index_size));
    }
  }

  if (FAILEDx(posting_micro_writer_.append_logical_block(
      posting_block.buf_, posting_block.size_, posting_block_idx))) {
    STORAGE_LOG(WARN, "fail to append posting logical block", K(ret), K(posting_block));
  } else if (has_posting_value()
      && OB_FAIL(posting_value_micro_writer_.append_logical_block(
          posting_value_block.buf_, posting_value_block.size_, posting_value_block_idx))) {
    STORAGE_LOG(WARN, "fail to append posting value logical block", K(ret),
        K(posting_value_block));
  } else if (OB_UNLIKELY(posting_block_idx != meta.block_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected posting block idx", K(ret), K(posting_block_idx), K(meta));
  } else if (has_posting_value() && OB_UNLIKELY(posting_block_idx != posting_value_block_idx)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "posting and posting value block idx mismatch", K(ret),
        K(posting_block_idx), K(posting_value_block_idx));
  } else if (OB_FAIL(append_logical_block_meta(meta, projected_pending_index_size))) {
    STORAGE_LOG(WARN, "fail to append inverted index logical block meta", K(ret),
        K(posting_block_idx), K(posting_block), K(projected_pending_index_size));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::check_logical_block_order(
    const ObInvIdxLogicalBlockMeta &meta,
    bool &same_term) const
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  ObDatumRowkey last_macro_term;
  same_term = false;
  if (OB_UNLIKELY(!current_term_.is_valid() || !meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid logical block order argument", K(ret),
        K_(current_term), K(meta));
  } else if (pending_term_group_.logical_block_metas_.count() > 0) {
    if (OB_FAIL(compare_term(current_term_, pending_term_group_.term_, cmp_ret))) {
      STORAGE_LOG(WARN, "fail to compare current term with pending term group", K(ret),
          K_(current_term), K_(pending_term_group));
    } else if (OB_UNLIKELY(cmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "current term is smaller than pending term group", K(ret),
          K_(current_term), K_(pending_term_group));
    } else if (FALSE_IT(same_term = (0 == cmp_ret))) {
    } else if (same_term
        && OB_UNLIKELY(meta.min_doc_id_ <= pending_term_group_.max_doc_id_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "doc id order decreases within inverted index term", K(ret),
          K(meta), K_(pending_term_group));
    }
  } else if (macro_stats_.endkey_.is_valid()) {
    if (OB_FAIL(last_macro_term.assign(macro_stats_.endkey_.datums_, term_col_count_))) {
      STORAGE_LOG(WARN, "fail to assign last macro term", K(ret), K_(macro_stats));
    } else if (OB_FAIL(compare_term(current_term_, last_macro_term, cmp_ret))) {
      STORAGE_LOG(WARN, "fail to compare current term with last macro term", K(ret),
          K_(current_term), K_(macro_stats));
    } else if (OB_UNLIKELY(cmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "current term is smaller than last macro term", K(ret),
          K_(current_term), K_(macro_stats));
    } else if (0 == cmp_ret
        && OB_UNLIKELY(meta.min_doc_id_
            <= macro_stats_.endkey_.get_datum(term_col_count_).get_uint64())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "doc id order decreases across inverted index micro block pairs", K(ret),
          K(meta), K_(macro_stats));
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::finalize_term_group()
{
  int ret = OB_SUCCESS;
  if (pending_term_group_.logical_block_metas_.empty()) {
    // skip
  } else if (OB_UNLIKELY(!pending_term_group_.is_valid()
      || pending_index_size_ <= 0 || micro_index_size_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid pending term group state", K(ret),
        K_(pending_term_group), K_(pending_index_size), K_(micro_index_size));
  } else if (OB_UNLIKELY(micro_index_size_ > INT64_MAX - pending_index_size_)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "inverted index rows size overflows", K(ret),
        K_(micro_index_size), K_(pending_index_size));
  } else if (OB_FAIL(micro_term_groups_.push_back(pending_term_group_))) {
    STORAGE_LOG(WARN, "fail to append finalized term group", K(ret), K_(pending_term_group));
  } else {
    micro_index_size_ += pending_index_size_;
    pending_term_group_.reset();
    pending_index_size_ = 0;
  }
  return ret;
}

// Admit logical blocks conservatively at pair boundaries. Term count, micro block size,
// and projected index row size are checked so that a normally sized micro block pair
// plus its index and macro meta blocks can fit in an empty macro block.
//
// However, since a logical block is indivisible, these checks are applied only when the
// current micro block pair can be flushed; an empty pair has no smaller fallback and
// gets one final chance to benefit from compression.
int ObInvIdxMacroBlockWriter::need_flush_micro_block_pair(
    const ObPostingBlockDesc &posting_block,
    const ObPostingValueBlockDesc &posting_value_block,
    const ObInvIdxLogicalBlockMeta &meta,
    bool &need_flush,
    int64_t &projected_pending_index_size)
{
  int ret = OB_SUCCESS;
  const bool pair_is_empty = posting_micro_writer_.is_empty();
  int64_t posting_micro_size = 0;
  int64_t posting_value_micro_size = 0;
  int64_t projected_index_row_size = 0;
  need_flush = false;
  projected_pending_index_size = 0;

  // Check term count
  if (!pair_is_empty
      && OB_UNLIKELY(micro_term_groups_.count() + 1 >= MICRO_BLOCK_PAIR_TERM_COUNT_LIMIT)) {
    need_flush = true;
  }

  // Check micro writer capacity
  if (need_flush) {
  } else if (OB_FAIL(posting_micro_writer_.estimate_size_after_append(
      posting_block.size_, posting_micro_size))) {
    if (OB_BUF_NOT_ENOUGH == ret && !pair_is_empty) {
      ret = OB_SUCCESS;
      need_flush = true;
    } else {
      STORAGE_LOG(WARN, "fail to estimate posting micro block size", K(ret),
          K(pair_is_empty), K(posting_block));
    }
  } else if (has_posting_value()
      && OB_FAIL(posting_value_micro_writer_.estimate_size_after_append(
          posting_value_block.size_, posting_value_micro_size))) {
    if (OB_BUF_NOT_ENOUGH == ret && !pair_is_empty) {
      ret = OB_SUCCESS;
      need_flush = true;
    } else {
      STORAGE_LOG(WARN, "fail to estimate posting value micro block size", K(ret),
          K(pair_is_empty), K(posting_value_block));
    }
  }

  // Check micro block size
  const int64_t posting_micro_block_size_limit
      = MIN(data_store_desc_->get_micro_block_size(),
          data_store_desc_->get_macro_block_size()
              * POSTING_MICRO_BLOCK_SIZE_LIMIT_PERCENT / PERCENT_BASE);
  const int64_t posting_value_micro_block_size_limit
      = posting_micro_block_size_limit * POSTING_VALUE_MICRO_BLOCK_SIZE_LIMIT_FACTOR;
  if (OB_SUCC(ret) && !need_flush && !pair_is_empty
      && (posting_micro_size > posting_micro_block_size_limit
          || (has_posting_value()
              && posting_value_micro_size > posting_value_micro_block_size_limit))) {
    need_flush = true;
  }

  // Project logical block group
  ObInvIdxLogicalBlockGroup projected_group;
  if (OB_FAIL(ret) || need_flush) {
  } else if (OB_UNLIKELY(meta.doc_count_ > INT64_MAX - current_micro_stats_.doc_count_)
      || OB_UNLIKELY(meta.doc_count_ > INT64_MAX - pending_term_group_.doc_count_)) {
    need_flush = true;
  } else if (OB_FAIL(projected_group.term_.assign(
      current_term_.datums_, current_term_.get_datum_cnt()))) {
    STORAGE_LOG(WARN, "fail to initialize projected logical block group term", K(ret),
        K_(current_term));
  } else if (OB_FAIL(projected_group.logical_block_metas_.assign(
      pending_term_group_.logical_block_metas_))) {
    STORAGE_LOG(WARN, "fail to copy logical block metas for admission", K(ret),
        K_(pending_term_group));
  } else if (OB_FAIL(projected_group.logical_block_metas_.push_back(meta))) {
    STORAGE_LOG(WARN, "fail to append projected logical block meta", K(ret), K(meta));
  } else {
    projected_group.max_doc_id_ = meta.max_doc_id_;
    projected_group.doc_count_ = meta.doc_count_ + pending_term_group_.doc_count_;
  }

  // Estimate projected index row size
  if (OB_FAIL(ret) || need_flush) {
  } else if (OB_FAIL(index_block_builder_.estimate_index_row_size_after_append_logical_block(
      projected_group, posting_value_micro_size, projected_pending_index_size))) {
    if (OB_SIZE_OVERFLOW == ret && !pair_is_empty) {
      ret = OB_SUCCESS;
      need_flush = true;
    } else {
      STORAGE_LOG(WARN, "fail to estimate projected inverted index row size", K(ret),
          K(projected_group), K(posting_value_micro_size));
    }
  } else if (OB_UNLIKELY(projected_pending_index_size > INT64_MAX - micro_index_size_)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "projected inverted index rows size overflows", K(ret),
        K(projected_pending_index_size), K_(micro_index_size));
  } else {
    projected_index_row_size = micro_index_size_ + projected_pending_index_size;
  }

  // Check projected index row size
  const int64_t projected_index_row_size_limit = data_store_desc_->get_macro_block_size()
      * MICRO_BLOCK_PAIR_INDEX_ROW_SIZE_LIMIT_PERCENT / PERCENT_BASE;
  if (OB_SUCC(ret) && !need_flush && !pair_is_empty
      && (projected_index_row_size > projected_index_row_size_limit)) {
    need_flush = true;
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::append_logical_block_meta(
    const ObInvIdxLogicalBlockMeta &meta,
    const int64_t projected_pending_index_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!current_term_.is_valid() || !meta.is_valid()
      || projected_pending_index_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid logical block meta argument", K(ret), K_(current_term),
        K(meta), K(projected_pending_index_size));
  } else if (pending_term_group_.logical_block_metas_.empty()
      && OB_FAIL(current_term_.deep_copy(pending_term_group_.term_, candidate_allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy logical block group term", K(ret), K_(current_term));
  } else if (OB_FAIL(pending_term_group_.logical_block_metas_.push_back(meta))) {
    STORAGE_LOG(WARN, "fail to push inverted index logical block meta", K(ret), K(meta));
  } else if (OB_UNLIKELY(meta.doc_count_ > INT64_MAX - pending_term_group_.doc_count_
      || meta.doc_count_ > INT64_MAX - current_micro_stats_.doc_count_)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "inverted index micro block pair doc count overflows", K(ret),
        K_(pending_term_group), K_(current_micro_stats), K(meta));
  } else {
    pending_term_group_.max_doc_id_ = meta.max_doc_id_;
    pending_term_group_.doc_count_ += meta.doc_count_;
    current_micro_stats_.doc_count_ += meta.doc_count_;
    if (OB_FAIL(ObInvIdxIndexBlockBuilder::build_index_rowkey(
        current_term_,
        meta.max_doc_id_,
        data_store_desc_->get_snapshot_version(),
        candidate_allocator_,
        current_micro_stats_.endkey_))) {
      STORAGE_LOG(WARN, "fail to update inverted index micro block pair endkey", K(ret),
          K_(current_term), K(meta));
    } else if (OB_FAIL(current_micro_stats_.inv_meta_.update(
        meta.min_doc_id_, meta.max_doc_id_, meta.total_doc_length_))) {
      STORAGE_LOG(WARN, "fail to update inverted index micro block pair stats", K(ret), K(meta));
    } else {
      pending_index_size_ = projected_pending_index_size;
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::flush_micro_block_pair()
{
  int ret = OB_SUCCESS;
  bool is_index_committed = false;
  ObInvIdxMicroBlockPairDesc pair_desc;
  if (OB_UNLIKELY(posting_micro_writer_.is_empty()
      || (has_posting_value() == posting_value_micro_writer_.is_empty())
      || (has_posting_value()
          && posting_micro_writer_.get_block_count()
              != posting_value_micro_writer_.get_block_count())
      || current_micro_stats_.doc_count_ <= 0
      || (micro_term_groups_.empty() && pending_term_group_.logical_block_metas_.empty()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "inconsistent inverted index micro block pair state", K(ret),
        K_(posting_micro_writer), K_(posting_value_micro_writer),
        K_(current_micro_stats), K_(micro_term_groups), K_(pending_term_group));
  } else if (OB_FAIL(finalize_term_group())) {
    STORAGE_LOG(WARN, "fail to finalize inverted index term group before flush", K(ret));
  } else if (OB_FAIL(build_micro_block_pair(pair_desc))) {
    STORAGE_LOG(WARN, "fail to build inverted index micro block pair", K(ret));
  } else if (OB_FAIL(prepare_micro_block_pair(pair_desc))) {
    STORAGE_LOG(WARN, "fail to prepare inverted index micro block pair", K(ret));
  } else if (OB_FAIL(index_block_builder_.append_micro_block(
      pair_desc.index_desc_, pair_desc.index_block_upper_bound_, is_index_committed))) {
    STORAGE_LOG(WARN, "fail to append inverted index micro block index", K(ret),
        K(pair_desc.index_desc_), K(pair_desc.index_block_upper_bound_));
  } else if (!is_index_committed) {
    if (0 == macro_stats_.doc_count_) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "empty macro block cannot contain inverted index micro block pair",
          K(ret), K(pair_desc.posting_.locator_), K(pair_desc.posting_value_.locator_),
          K(pair_desc.index_block_upper_bound_));
    } else if (OB_FAIL(flush_macro_block(false /*is_close_flush*/))) {
      STORAGE_LOG(WARN, "fail to flush inverted index macro block before retry", K(ret));
    } else if (OB_FAIL(prepare_micro_block_pair(pair_desc))) {
      STORAGE_LOG(WARN, "fail to prepare inverted index micro block pair after macro switch", K(ret));
    } else if (OB_FAIL(index_block_builder_.append_micro_block(
        pair_desc.index_desc_, pair_desc.index_block_upper_bound_, is_index_committed))) {
      STORAGE_LOG(WARN, "fail to append inverted index micro block index", K(ret),
          K(pair_desc.index_desc_), K(pair_desc.index_block_upper_bound_));
    } else if (!is_index_committed) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "new macro block cannot contain inverted index micro block pair",
          K(ret), K(pair_desc.posting_.locator_), K(pair_desc.posting_value_.locator_),
          K(pair_desc.index_block_upper_bound_));
    }
  }
  if (FAILEDx(commit_micro_block_pair(pair_desc))) {
    STORAGE_LOG(WARN, "fail to commit inverted index micro block pair", K(ret));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::build_micro_block_pair(
    ObInvIdxMicroBlockPairDesc &pair_desc)
{
  int ret = OB_SUCCESS;
  pair_desc.reset();
  if (OB_FAIL(posting_micro_writer_.build_micro_block(
      pair_desc.posting_.uncompressed_payload_,
      pair_desc.posting_.uncompressed_payload_size_))) {
    STORAGE_LOG(WARN, "fail to build candidate posting micro block", K(ret));
  } else if (has_posting_value()
      && OB_FAIL(posting_value_micro_writer_.build_micro_block(
          pair_desc.posting_value_.uncompressed_payload_,
          pair_desc.posting_value_.uncompressed_payload_size_))) {
    STORAGE_LOG(WARN, "fail to build candidate posting value micro block", K(ret));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::prepare_micro_block_pair(
    ObInvIdxMicroBlockPairDesc &pair_desc)
{
  int ret = OB_SUCCESS;
  int64_t posting_offset = 0;
  int64_t posting_value_offset = 0;
  int64_t pair_size = 0;
  const int64_t macro_remain_size = macro_block_.get_remain_size();
  const bool is_empty_macro = (0 == macro_stats_.doc_count_);
  if (!macro_handle_.get_macro_id().is_valid() && OB_FAIL(alloc_block())) {
    STORAGE_LOG(WARN, "fail to ensure macro block for candidate micro block pair", K(ret));
  } else if (FALSE_IT(posting_offset = macro_block_.get_data_size())) {
  } else if (OB_FAIL(prepare_micro_block_stream(
      posting_micro_buffer_helper_,
      false /*is_posting_value*/,
      macro_block_.get_current_macro_seq(),
      posting_offset,
      pair_desc.posting_))) {
    STORAGE_LOG(WARN, "fail to prepare posting micro block for actual layout", K(ret));
  } else if (FALSE_IT(posting_value_offset = posting_offset + pair_desc.posting_.locator_.size_)) {
  } else if (has_posting_value() && OB_FAIL(prepare_micro_block_stream(
      posting_value_micro_buffer_helper_,
      true /*is_posting_value*/,
      macro_block_.get_current_macro_seq(),
      posting_value_offset,
      pair_desc.posting_value_))) {
    STORAGE_LOG(WARN, "fail to prepare posting value micro block for actual layout", K(ret));
  } else if (FALSE_IT(pair_size = pair_desc.posting_.locator_.size_
      + (has_posting_value() ? pair_desc.posting_value_.locator_.size_ : 0))) {
  } else if (is_empty_macro && pair_size >= macro_remain_size) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "empty macro block cannot contain inverted index data micro block pair",
        K(ret), K(pair_size), K(macro_remain_size));
  }

  if (OB_SUCC(ret)) {
    pair_desc.index_desc_.reset();
    pair_desc.index_desc_.posting_locator_ = pair_desc.posting_.locator_;
    if (has_posting_value()) {
      pair_desc.index_desc_.posting_value_locator_ = pair_desc.posting_value_.locator_;
    }
    pair_desc.index_desc_.logical_block_groups_ = &micro_term_groups_;
    pair_desc.index_desc_.macro_id_ = macro_handle_.get_macro_id();
    pair_desc.index_desc_.data_checksum_ = pair_desc.posting_.raw_desc_.block_checksum_;
    if (data_store_desc_->is_major_merge_type()) {
      ObLogicMacroBlockId logic_macro_id;
      gen_logic_macro_id(logic_macro_id);
      pair_desc.index_desc_.logic_micro_id_.init(posting_offset, logic_macro_id);
    }
    int64_t projected_meta_block_size = 0;
    if (is_empty_macro) {
      // An empty macro block is the final placement target. Skip conservative
      // index/meta budgets so actual encoding and compression get one last chance;
      // intrinsic writer limits and final macro writes still enforce hard capacity.
      pair_desc.index_block_upper_bound_ = INT64_MAX;
    } else if (OB_FAIL(estimate_projected_macro_meta_block_size(projected_meta_block_size))) {
      STORAGE_LOG(WARN, "fail to estimate projected inverted index macro meta block", K(ret),
          K_(current_micro_stats), K_(macro_stats));
    } else {
      const int64_t safety_margin = data_store_desc_->get_macro_block_size()
          * MACRO_SPLIT_SAFETY_MARGIN_PERCENT / PERCENT_BASE;
      const int64_t index_block_size_limit = data_store_desc_->get_macro_block_size()
          * INDEX_BLOCK_SIZE_LIMIT_PERCENT / PERCENT_BASE;
      pair_desc.index_block_upper_bound_ = MIN(index_block_size_limit,
          macro_remain_size - pair_size - projected_meta_block_size - safety_margin);
      if (OB_UNLIKELY(!pair_desc.is_valid()
          || (data_store_desc_->is_major_merge_type()
              && !pair_desc.index_desc_.logic_micro_id_.is_valid()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid prepared inverted index micro block pair", K(ret),
            K(pair_desc.posting_.locator_), K(pair_desc.posting_value_.locator_),
            K(pair_desc.index_desc_), K(pair_desc.index_block_upper_bound_));
      }
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::prepare_micro_block_stream(
    ObInvIdxMicroBlockBufferHelper &buffer_helper,
    const bool is_posting_value,
    const int64_t macro_seq,
    const int64_t block_offset,
    ObInvIdxMicroBlockStreamDesc &stream_desc)
{
  int ret = OB_SUCCESS;
  char *header_buf = nullptr;
  int64_t header_size = 0;
  int64_t header_pos = 0;
  ObMicroBlockHeader header;
  stream_desc.raw_desc_.reset();
  stream_desc.locator_.reset();
  if (OB_FAIL(buffer_helper.compress_encrypt_micro_block(
      stream_desc.uncompressed_payload_,
      stream_desc.uncompressed_payload_size_,
      macro_seq,
      block_offset,
      stream_desc.raw_desc_))) {
    STORAGE_LOG(WARN, "fail to compress inverted index micro block stream", K(ret),
        K(is_posting_value), K(macro_seq), K(block_offset));
  } else if (FALSE_IT(stream_desc.raw_desc_.row_count_ =
      is_posting_value ? 0 : current_micro_stats_.doc_count_)) {
  } else if (!is_posting_value
      && OB_FAIL(stream_desc.raw_desc_.last_rowkey_.assign(
          current_micro_stats_.endkey_.datums_, current_micro_stats_.endkey_.get_datum_cnt()))) {
    STORAGE_LOG(WARN, "fail to assign posting micro block last rowkey", K(ret),
        K_(current_micro_stats));
  } else if (OB_FAIL(build_micro_block_header(
      stream_desc.raw_desc_, is_posting_value, header, header_size))) {
    STORAGE_LOG(WARN, "fail to build inverted index micro block header", K(ret),
        K(is_posting_value));
  } else if (OB_ISNULL(header_buf =
      static_cast<char *>(candidate_allocator_.alloc(header_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc candidate micro block header buffer", K(ret),
        K(header_size), K(is_posting_value));
  } else if (OB_FAIL(header.serialize(header_buf, header_size, header_pos))) {
    STORAGE_LOG(WARN, "fail to serialize candidate micro block header", K(ret),
        K(header), K(is_posting_value));
  } else {
    stream_desc.raw_desc_.header_buf_ = header_buf;
    stream_desc.raw_desc_.header_size_ = header_pos;
    stream_desc.locator_.offset_ = block_offset;
    stream_desc.locator_.size_ = header_pos + stream_desc.raw_desc_.payload_size_;
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::build_micro_block_header(
    const ObRawMicroBlockDesc &raw_desc,
    const bool is_posting_value,
    ObMicroBlockHeader &header,
    int64_t &header_size)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = is_posting_value
      ? posting_value_param_.get_column_cnt()
      : posting_ctx_.get_column_cnt();
  header.reset();
  header_size = 0;
  if (OB_FAIL(header.init(
      data_store_desc_->get_major_working_cluster_version(), column_count,
      0 /*rowkey_column_count*/, common::INV_IDX_ROW_STORE, false /*has_column_checksum*/))) {
    STORAGE_LOG(WARN, "fail to init inverted index micro block header", K(ret),
        K(is_posting_value), K(column_count));
  } else {
    header.inverted_index_type_ = static_cast<uint8_t>(is_posting_value
        ? MICRO_BLOCK_INV_IDX_POSTING_VALUE
        : MICRO_BLOCK_INV_IDX_POSTING);
    header.data_length_ = static_cast<int32_t>(raw_desc.uncompressed_size_);
    header.data_zlength_ = static_cast<int32_t>(raw_desc.payload_size_);
    header.original_length_ = static_cast<int32_t>(raw_desc.uncompressed_size_);
    header.data_checksum_ = raw_desc.block_checksum_;
    header.row_count_ = static_cast<uint32_t>(raw_desc.row_count_);
    header.set_header_checksum();
    header_size = header.get_serialize_size();
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::estimate_projected_macro_meta_block_size(int64_t &estimate_size)
{
  int ret = OB_SUCCESS;
  ObInvIdxMacroBlockStats projected_macro_stats;
  estimate_size = 0;
  if (OB_FAIL(projected_macro_stats.assign(macro_stats_))) {
    STORAGE_LOG(WARN, "failed to copy projected macro stats", K(ret));
  } else if (OB_FAIL(projected_macro_stats.endkey_.assign(
      current_micro_stats_.endkey_.datums_,
      current_micro_stats_.endkey_.get_datum_cnt()))) {
    STORAGE_LOG(WARN, "fail to assign projected inverted index macro endkey", K(ret),
        K_(current_micro_stats));
  } else if (OB_UNLIKELY(macro_stats_.doc_count_ > INT64_MAX - current_micro_stats_.doc_count_)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "projected inverted index macro doc count overflows", K(ret),
        K_(macro_stats), K_(current_micro_stats));
  } else if (FALSE_IT(projected_macro_stats.doc_count_ += current_micro_stats_.doc_count_)) {
  } else if (OB_FAIL(projected_macro_stats.inv_meta_.merge(
      current_micro_stats_.inv_meta_))) {
    STORAGE_LOG(WARN, "fail to update projected inverted index macro stats", K(ret),
        K_(current_micro_stats));
  } else if (OB_FAIL(index_block_builder_.estimate_projected_macro_meta_block_size(
      projected_macro_stats, estimate_size))) {
    STORAGE_LOG(WARN, "fail to estimate projected inverted index macro meta block", K(ret),
        K(projected_macro_stats));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::commit_micro_block_pair(
    const ObInvIdxMicroBlockPairDesc &pair_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_micro_block(pair_desc.posting_))) {
    STORAGE_LOG(WARN, "fail to write posting micro block", K(ret),
        K(pair_desc.posting_.locator_));
  } else if (has_posting_value()
      && OB_FAIL(write_micro_block(pair_desc.posting_value_))) {
    STORAGE_LOG(WARN, "fail to write posting value micro block", K(ret),
        K(pair_desc.posting_value_.locator_));
  } else if (OB_FAIL(current_micro_stats_.endkey_.deep_copy(
      macro_stats_.endkey_, macro_allocator_))) {
    STORAGE_LOG(WARN, "fail to copy committed inverted index macro endkey", K(ret),
        K_(current_micro_stats));
  } else if (OB_UNLIKELY(macro_stats_.doc_count_ > INT64_MAX - current_micro_stats_.doc_count_)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "inverted index macro doc count overflows on commit", K(ret),
        K_(macro_stats), K_(current_micro_stats));
  } else if (FALSE_IT(macro_stats_.doc_count_ += current_micro_stats_.doc_count_)) {
  } else if (OB_FAIL(macro_stats_.inv_meta_.merge(current_micro_stats_.inv_meta_))) {
    STORAGE_LOG(WARN, "fail to update committed inverted index macro stats", K(ret),
        K_(current_micro_stats));
  } else if (OB_FAIL(posting_micro_writer_.reuse())) {
    STORAGE_LOG(WARN, "fail to reuse posting micro block writer", K(ret));
  } else if (has_posting_value() && OB_FAIL(posting_value_micro_writer_.reuse())) {
    STORAGE_LOG(WARN, "fail to reuse posting value micro block writer", K(ret));
  } else {
    micro_term_groups_.reset();
    pending_term_group_.reset();
    micro_index_size_ = 0;
    pending_index_size_ = 0;
    current_micro_stats_.reuse();
    candidate_allocator_.reuse();
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::write_micro_block(const ObInvIdxMicroBlockStreamDesc &stream_desc)
{
  int ret = OB_SUCCESS;
  int64_t data_offset = 0;
  const int64_t stored_size =
      stream_desc.raw_desc_.header_size_ + stream_desc.raw_desc_.payload_size_;
  if (OB_UNLIKELY(macro_block_.get_data_size() != stream_desc.locator_.offset_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "inverted index micro block planned layout mismatch", K(ret),
        K(stored_size), K(stream_desc.locator_),
        "macro_data_size", macro_block_.get_data_size());
  } else if (OB_FAIL(macro_block_.write_raw_micro_block(stream_desc.raw_desc_, data_offset))) {
    STORAGE_LOG(WARN, "fail to write raw inverted index micro block", K(ret),
        K(stream_desc.raw_desc_), K(stream_desc.locator_));
  } else if (OB_UNLIKELY(data_offset != stream_desc.locator_.offset_
      || macro_block_.get_data_size() != data_offset + stored_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "written inverted index micro block layout mismatch", K(ret),
        K(data_offset), K(stored_size), K(stream_desc.locator_),
        "macro_data_size", macro_block_.get_data_size());
  } else {
    ++merge_block_info_.new_micro_count_in_new_macro_;
    merge_block_info_.original_size_ += stream_desc.raw_desc_.uncompressed_size_;
    merge_block_info_.compressed_size_ += stream_desc.raw_desc_.payload_size_;
    merge_block_info_.new_micro_info_.add_data_micro_size(stream_desc.raw_desc_.payload_size_);
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::flush_macro_block(const bool is_close_flush)
{
  int ret = OB_SUCCESS;
  const int64_t ddl_start_row_offset = OB_ISNULL(callback_)
      ? -1
      : callback_->get_ddl_start_row_offset();
  if (!macro_block_.is_dirty()) {
    // skip
  } else if (OB_UNLIKELY(!macro_handle_.get_macro_id().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid macro id", K(ret), K_(macro_handle));
  } else if (OB_FAIL(index_block_builder_.write_index_and_meta_blocks(
      macro_block_, macro_handle_.get_macro_id(), macro_stats_, ddl_start_row_offset))) {
    STORAGE_LOG(WARN, "fail to write index and meta blocks", K(ret),
        K_(macro_stats), K_(macro_handle));
  } else {
    ObInvIdxDefaultMacroBlockFlusher flusher;
    flusher.set_param(*data_store_desc_, macro_handle_, device_handle_);
    if (OB_FAIL(macro_block_.flush(flusher, is_close_flush))) {
      STORAGE_LOG(WARN, "fail to flush inverted index macro block", K(ret),
          K(is_close_flush), K_(macro_stats));
    } else if (OB_FAIL(index_block_builder_.publish_macro_meta(
        macro_handle_.get_macro_id()))) {
      STORAGE_LOG(WARN, "fail to publish inverted index macro meta", K(ret),
          K(macro_handle_.get_macro_id()));
    } else if (OB_FAIL(block_write_ctx_.add_macro_block_id(macro_handle_.get_macro_id()))) {
      STORAGE_LOG(WARN, "fail to add inverted index macro id to write ctx", K(ret),
          K(macro_handle_.get_macro_id()));
    } else if (OB_FAIL(seg_inv_meta_.merge_macro(macro_stats_.inv_meta_))) {
      STORAGE_LOG(WARN, "fail to update inverted index segment stats", K(ret),
          K_(macro_stats));
    }
  }
  if (OB_SUCC(ret) && !is_close_flush && macro_block_.is_dirty()
      && OB_FAIL(switch_to_next_macro_block())) {
    STORAGE_LOG(WARN, "fail to switch to next inverted index macro block", K(ret));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::switch_to_next_macro_block()
{
  int ret = OB_SUCCESS;
  int64_t next_macro_seq = -1;
  int64_t logic_macro_seq = -1;
  if (OB_FAIL(wait_macro_block_io())) {
    STORAGE_LOG(WARN, "fail to wait inverted index macro block io before switch", K(ret),
        K_(macro_handle));
  } else if (OB_NOT_NULL(callback_) && OB_FAIL(exec_callback())) {
    STORAGE_LOG(WARN, "fail to execute inverted index macro block callback", K(ret));
  } else if (OB_FAIL(macro_seq_generator_->get_next(next_macro_seq))) {
    STORAGE_LOG(WARN, "fail to get next inverted index macro sequence", K(ret));
  } else if (OB_FAIL(build_logic_macro_seq(next_macro_seq, logic_macro_seq))) {
    STORAGE_LOG(WARN, "fail to build inverted index logical macro sequence", K(ret));
  } else if (OB_FAIL(index_block_builder_.reuse_macro_scope())) {
    STORAGE_LOG(WARN, "fail to reuse inverted index builder macro scope", K(ret));
  } else if (OB_FAIL(macro_block_.init(*data_store_desc_, logic_macro_seq, merge_block_info_, 0))) {
    STORAGE_LOG(WARN, "fail to initialize next inverted index macro block", K(ret),
        K(next_macro_seq));
  } else {
    last_macro_seq_ = next_macro_seq;
    macro_handle_.reset();
    macro_stats_.reuse();
    macro_allocator_.reuse();
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::wait_macro_block_io()
{
  int ret = OB_SUCCESS;
  if (!macro_handle_.get_macro_id().is_valid()) {
  } else if (OB_FAIL(macro_handle_.wait())) {
    STORAGE_LOG(WARN, "fail to wait inverted index macro block io", K(ret), K_(macro_handle));
  } else if (OB_ISNULL(device_handle_) && !macro_handle_.is_empty()) {
    int64_t block_io_us = 0;
    if (OB_SUCCESS == macro_handle_.get_io_time_us(block_io_us)) {
      merge_block_info_.block_io_us_ += block_io_us;
    }
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::append_logical_block(
    const ObDatumRowkey &term,
    const ObPostingBlockDesc &posting_block,
    const ObPostingValueBlockDesc &posting_value_block)
{
  int ret = OB_SUCCESS;
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "inverted index macro block writer is not opened", K(ret));
  } else if (OB_UNLIKELY(!term.is_valid() || term.get_datum_cnt() != term_col_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid term for reused logical block", K(ret),
        K(term), K_(term_col_count));
  } else if (OB_UNLIKELY(!posting_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid posting block desc to reuse", K(ret), K(posting_block));
  } else if (has_posting_value()
      && OB_UNLIKELY(!posting_value_block.is_valid()
          || posting_block.doc_count_ != posting_value_block.row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "posting and value block not paired for reuse", K(ret),
        K(posting_block), K(posting_value_block));
  } else if (!has_posting_value() && OB_UNLIKELY(posting_value_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected posting value block for schema without posting value",
        K(ret), K(posting_value_block));
  } else if (OB_FAIL(check_term_order(term))) {
    STORAGE_LOG(WARN, "reused logical block term out of order", K(ret), K(term));
  } else if (has_pending_rows_ && OB_FAIL(flush_logical_block())) {
    STORAGE_LOG(WARN, "fail to flush pending logical block before reuse", K(ret), K(term));
  } else if (OB_FAIL(term.deep_copy(current_term_, current_term_allocator_))) {
    STORAGE_LOG(WARN, "fail to deep copy reused logical block term", K(ret), K(term));
  } else if (OB_FAIL(append_encoded_logical_block(posting_block, posting_value_block))) {
    STORAGE_LOG(WARN, "fail to append reused logical block", K(ret),
        K(term), K(posting_block), K(posting_value_block));
  } else {
    current_term_.reset();
    current_term_allocator_.reuse();
    ++merge_block_info_.multiplexed_logical_block_count_;
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::append_macro_block(
    const ObMacroBlockDesc &macro_desc,
    const ObInvMacroMeta &inv_macro_meta,
    const ObMicroBlockData *micro_block_data)
{
  int ret = OB_SUCCESS;
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  bool is_micro_index_clustered = false;
  const ObDataMacroBlockMeta *macro_meta = macro_desc.macro_meta_;
  if (OB_UNLIKELY(!is_opened_) || OB_ISNULL(data_store_desc_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "inverted index macro block writer is not opened", K(ret));
  } else if (OB_ISNULL(macro_meta)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected null inverted index macro meta to reuse", K(ret), K(macro_desc));
  } else if (FALSE_IT(is_micro_index_clustered = data_store_desc_->micro_index_clustered())) {
  } else if (OB_UNLIKELY(macro_desc.is_clustered_index_tree_ != is_micro_index_clustered)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected index tree type for reused macro block", K(ret),
        K(macro_desc), K(is_micro_index_clustered));
  } else if (OB_UNLIKELY(!macro_meta->is_valid() || !macro_meta->get_macro_id().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inverted index macro desc to reuse", K(ret), K(macro_desc));
  } else if (OB_UNLIKELY(macro_desc.row_count_ <= 0
      || macro_desc.row_count_ != macro_meta->val_.row_count_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "inconsistent inverted index macro row count to reuse", K(ret),
        K(macro_desc.row_count_), K(macro_meta->val_.row_count_), K(macro_desc));
  } else if (OB_UNLIKELY(!inv_macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid inverted index macro metadata to reuse", K(ret),
        K(inv_macro_meta), K(macro_desc));
  } else if (OB_UNLIKELY(macro_meta->end_key_.get_datum_cnt()
      != data_store_desc_->get_rowkey_column_count())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "reused inverted index macro endkey column count does not match", K(ret),
        "endkey_column_count", macro_meta->end_key_.get_datum_cnt(),
        "expected_rowkey_column_count", data_store_desc_->get_rowkey_column_count(),
        KPC(macro_meta));
  } else if (is_micro_index_clustered && OB_ISNULL(micro_block_data)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null clustered index micro block data to reuse", K(ret),
        K(macro_desc));
  } else if (OB_FAIL(seg_inv_meta_.check_merge_macro(inv_macro_meta))) {
    STORAGE_LOG(WARN, "invalid inverted index macro metadata before reuse", K(ret),
        K(inv_macro_meta), K_(seg_inv_meta));
  } else if (OB_FAIL(inner_append_macro_block(
      *macro_meta, inv_macro_meta, is_micro_index_clustered ? micro_block_data : nullptr))) {
    STORAGE_LOG(WARN, "fail to append reused inverted index macro block", K(ret),
        K(macro_desc), K(is_micro_index_clustered));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::inner_append_macro_block(
    const ObDataMacroBlockMeta &macro_meta,
    const ObInvMacroMeta &inv_macro_meta,
    const ObMicroBlockData *micro_block_data)
{
  int ret = OB_SUCCESS;
  if (has_pending_rows_ && OB_FAIL(flush_logical_block())) {
    STORAGE_LOG(WARN, "fail to flush pending logical block before reuse macro block", K(ret));
  } else if (!posting_micro_writer_.is_empty() && OB_FAIL(flush_micro_block_pair())) {
    STORAGE_LOG(WARN, "fail to flush micro block pair before reuse macro block", K(ret));
  } else if (OB_FAIL(flush_macro_block(false /*is_close_flush*/))) {
    STORAGE_LOG(WARN, "fail to flush in-progress macro block before reuse", K(ret),
        K(macro_meta));
  } else if (OB_FAIL(index_block_builder_.append_macro_block(macro_meta, micro_block_data))) {
    STORAGE_LOG(WARN, "fail to append reused macro meta to index builder", K(ret),
        K(macro_meta));
  } else if (OB_FAIL(flush_reuse_macro_block(macro_meta, inv_macro_meta))) {
    STORAGE_LOG(WARN, "fail to flush reused inverted index macro block", K(ret), K(macro_meta));
  } else if (OB_FAIL(macro_meta.end_key_.deep_copy(macro_stats_.endkey_, macro_allocator_))) {
    STORAGE_LOG(WARN, "fail to record reused macro endkey", K(ret), K(macro_meta));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::flush_reuse_macro_block(
    const ObDataMacroBlockMeta &macro_meta,
    const ObInvMacroMeta &inv_macro_meta)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &macro_id = macro_meta.get_macro_id();
  if (OB_FAIL(block_write_ctx_.add_macro_block_id(macro_id))) {
    STORAGE_LOG(WARN, "fail to add reused inverted index macro id to write ctx",
        K(ret), K(macro_id));
  } else if (OB_FAIL(seg_inv_meta_.merge_macro(inv_macro_meta))) {
    STORAGE_LOG(WARN, "fail to accumulate reused inverted index macro metadata",
        K(ret), K(inv_macro_meta), K(macro_meta));
  } else {
    block_write_ctx_.increment_old_block_count();
    merge_block_info_.multiplexed_macro_block_count_++;
    merge_block_info_.macro_block_count_++;
    merge_block_info_.total_row_count_ += macro_meta.val_.row_count_;
    merge_block_info_.occupy_size_ += macro_meta.val_.occupy_size_;
    merge_block_info_.original_size_ += macro_meta.val_.original_size_;
    merge_block_info_.compressed_size_ += macro_meta.val_.data_zsize_;
    FLOG_INFO("reuse inverted index macro block", K(macro_id), K(macro_meta.end_key_), K(macro_meta));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::exec_callback()
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta macro_block_meta;
  if (OB_ISNULL(callback_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid null inverted index macro block callback", K(ret));
  } else if (OB_UNLIKELY(!macro_handle_.get_macro_id().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid macro handle for inverted index macro block callback",
        K(ret), K_(macro_handle));
  } else if (OB_FAIL(macro_block_.get_macro_block_meta(macro_block_meta))) {
    STORAGE_LOG(WARN, "fail to get inverted index macro block meta for callback", K(ret));
  } else if (OB_FAIL(callback_->wait())) {
    STORAGE_LOG(WARN, "fail to wait inverted index macro block callback", K(ret));
  } else if (OB_FAIL(callback_->write(
      macro_handle_,
      macro_block_meta.get_logic_id(),
      macro_block_.get_data_buf(),
      upper_align(macro_block_.get_data_size(), DIO_ALIGN_SIZE),
      macro_stats_.doc_count_))) {
    STORAGE_LOG(WARN, "fail to do inverted index macro block callback", K(ret));
  }
  return ret;
}

bool ObInvIdxMacroBlockWriter::is_pre_alloc() const
{
  // small sstable or external flusher not supported, should always pre-allocate
  return OB_NOT_NULL(data_store_desc_) && data_store_desc_->is_for_sstable_data();
}

int ObInvIdxMacroBlockWriter::alloc_block()
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt storage_opt;
  if (OB_ISNULL(data_store_desc_) || OB_ISNULL(macro_seq_generator_) || OB_ISNULL(object_cleaner_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "inverted index macro writer is not ready to alloc block",
        K(ret), KP_(data_store_desc), KP_(macro_seq_generator), KP_(object_cleaner));
  } else if (!is_local_with_private_block_mode(data_store_desc_->get_exec_mode())) {
    const bool index_or_meta = data_store_desc_->is_for_index_or_meta();
    const compaction::ObMergeType merge_type = data_store_desc_->get_merge_type();
    const bool is_inc_major_compaction =
        compaction::is_inc_major_merge(merge_type);
    const bool is_inc_major_ddl =
        !is_inc_major_compaction && data_store_desc_->get_is_inc_major();
    ObStorageObjectType obj_type = ObStorageObjectType::MAX;
#define CONVERT_TO_OBJ_TYPE(T1) \
    (index_or_meta ? \
        ObStorageObjectType::SHARED_##T1##_META_MACRO : \
        ObStorageObjectType::SHARED_##T1##_DATA_MACRO)
    if (is_inc_major_compaction) {
      obj_type = CONVERT_TO_OBJ_TYPE(INC_MAJOR_COMPACTION);
    } else if (is_inc_major_ddl) {
      obj_type = CONVERT_TO_OBJ_TYPE(INC_MAJOR);
    } else if (compaction::is_major_merge_type(merge_type)) {
      obj_type = CONVERT_TO_OBJ_TYPE(MAJOR);
    } else if (compaction::is_mini_merge(merge_type)) {
      obj_type = CONVERT_TO_OBJ_TYPE(MINI);
    } else if (compaction::is_minor_merge(merge_type)) {
      obj_type = CONVERT_TO_OBJ_TYPE(MINOR);
    } else {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "merge type not supported to alloc inverted index macro block",
          K(ret), K(merge_type));
    }
#undef CONVERT_TO_OBJ_TYPE
    if (OB_SUCC(ret)) {
      int64_t fourth_id = data_store_desc_->get_op_id();
      if (is_inc_major_ddl) {
        fourth_id = data_store_desc_->get_inc_major_ddl_block_op_id();
      } else if (compaction::is_major_merge(merge_type)
          || compaction::is_medium_merge(merge_type)) {
        fourth_id = data_store_desc_->get_snapshot_version();
      }
      storage_opt.set_ss_share_object_opt(
          obj_type,
          data_store_desc_->get_tablet_id().is_ls_inner_tablet(),
          data_store_desc_->get_ls_id().id(),
          data_store_desc_->get_tablet_id().id(),
          fourth_id,
          macro_seq_generator_->get_current(),
          data_store_desc_->get_table_cg_idx(),
          data_store_desc_->get_reorganization_scn().get_val_for_tx());
    }
  } else {
    if (data_store_desc_->is_for_index_or_meta()) {
      storage_opt.set_private_meta_macro_object_opt(
          data_store_desc_->get_tablet_id().id(),
          data_store_desc_->get_tablet_id().is_ls_inner_tablet(),
          data_store_desc_->get_ls_id().id(),
          data_store_desc_->get_reorganization_scn().get_val_for_tx(),
          data_store_desc_->get_op_id());
    } else {
      storage_opt.set_private_object_opt(
          data_store_desc_->get_tablet_id().id(),
          data_store_desc_->get_tablet_id().is_ls_inner_tablet(),
          data_store_desc_->get_ls_id().id(),
          data_store_desc_->get_reorganization_scn().get_val_for_tx(),
          data_store_desc_->get_op_id());
    }
  }
  if (OB_FAIL(ret)) {
  } else if (macro_handle_.get_macro_id().is_valid() || macro_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "inverted index macro handle should be empty before alloc",
        K(ret), K_(macro_handle));
  } else if (OB_NOT_NULL(device_handle_)) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "device handle allocation is not supported by inverted index macro writer yet",
        K(ret), KP_(device_handle));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, macro_handle_))) {
    STORAGE_LOG(WARN, "fail to alloc inverted index macro block",
        K(ret), "current_macro_seq", macro_seq_generator_->get_current());
  } else if (OB_FAIL(object_cleaner_->add_new_macro_block_id(macro_handle_.get_macro_id()))) {
    STORAGE_LOG(WARN, "fail to add inverted index macro block id to cleaner",
        K(ret), K(macro_handle_.get_macro_id()));
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::close()
{
  int ret = OB_SUCCESS;
  ObBlockWriterConcurrentGuard guard(concurrent_lock_);
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "inverted index macro block writer is not opened", K(ret));
  } else if (has_pending_rows_ && OB_FAIL(flush_logical_block())) {
    STORAGE_LOG(WARN, "fail to flush pending inverted index logical block on close", K(ret));
  } else if (!posting_micro_writer_.is_empty() && OB_FAIL(flush_micro_block_pair())) {
    STORAGE_LOG(WARN, "fail to flush inverted index micro block pair on close", K(ret));
  } else if (OB_FAIL(flush_macro_block(true /*is_close_flush*/))) {
    STORAGE_LOG(WARN, "fail to flush inverted index macro block on close", K(ret));
  } else if (OB_FAIL(wait_macro_block_io())) {
    STORAGE_LOG(WARN, "fail to wait inverted index macro block io", K(ret), K_(macro_handle));
  } else if (macro_handle_.get_macro_id().is_valid()
      && OB_NOT_NULL(callback_)
      && OB_FAIL(exec_callback())) {
    STORAGE_LOG(WARN, "fail to execute inverted index macro block callback", K(ret));
  } else if (OB_FAIL(index_block_builder_.close(block_write_ctx_))) {
    STORAGE_LOG(WARN, "fail to close inverted index block builder", K(ret));
  } else {
    macro_handle_.reset();
    is_opened_ = false;
  }
  return ret;
}

int ObInvIdxMacroBlockWriter::build_logic_macro_seq(
    const int64_t physical_macro_seq, int64_t &logic_macro_seq) const
{
  int ret = OB_SUCCESS;
  logic_macro_seq = -1;
  if (data_store_desc_->get_sstable_logic_seq() < 0) {
    logic_macro_seq = physical_macro_seq;
  } else if (OB_UNLIKELY(physical_macro_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid physical macro sequence", K(ret), K(physical_macro_seq));
  } else if (OB_UNLIKELY(static_cast<uint64_t>(physical_macro_seq) >= ObMacroDataSeq::VERSION_LOCAL_SEQ_LIMIT)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "physical macro sequence overflows version local namespace", K(ret), K(physical_macro_seq));
  } else {
    // Keep physical allocation unchanged; only the logical data-block ID carries
    // the SSTable generation, as in ObMacroBlockWriter::build_logic_macro_seq().
    ObMacroDataSeq macro_data_seq(physical_macro_seq);
    if (OB_FAIL(macro_data_seq.set_sstable_seq(data_store_desc_->get_sstable_logic_seq()))) {
      STORAGE_LOG(WARN, "failed to set sstable logic seq", K(ret), K(data_store_desc_->get_sstable_logic_seq()));
    } else {
      macro_data_seq.set_data_block();
      logic_macro_seq = macro_data_seq.macro_data_seq_;
    }
  }
  return ret;
}

void ObInvIdxMacroBlockWriter::gen_logic_macro_id(ObLogicMacroBlockId &logic_macro_id)
{
  logic_macro_id.logic_version_ = data_store_desc_->get_logical_version();
  logic_macro_id.column_group_idx_ = data_store_desc_->get_table_cg_idx();
  logic_macro_id.data_seq_.macro_data_seq_ = macro_block_.get_current_macro_seq();
  logic_macro_id.tablet_id_ = data_store_desc_->get_tablet_id().id();
  logic_macro_id.merge_type_ = (uint8_t)data_store_desc_->get_merge_type();
}

} // end namespace blocksstable
} // end namespace oceanbase
