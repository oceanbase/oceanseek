/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "lib/allocator/page_arena.h"
#include "storage/lob/common/ob_lob_context.h"
#define USING_LOG_PREFIX STORAGE

#include "lib/charset/ob_charset.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_errno.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "ob_lob_meta.h"
#include "ob_lob_persistent_adaptor.h"
#include "ob_lob_persistent_iterator.h"
namespace oceanbase
{
namespace storage
{
//==================== ObLobMetaScanIter ====================
ObLobMetaScanIter::ObLobMetaScanIter()
  : is_opened_(false),
    adaptor_(nullptr),
    meta_iter_(nullptr),
    coll_type_(ObCollationType::CS_TYPE_INVALID),
    offset_(0),
    len_(0),
    scan_backward_(false),
    cur_pos_(0),
    cur_byte_pos_(0),
    cur_char_pos_(0),
    cur_info_()
{}

void ObLobMetaScanIter::reset()
{
  if (nullptr != meta_iter_) {
    if (nullptr != adaptor_) {
      (void)adaptor_->revert_scan_iter(static_cast<ObLobAuxTableIterator<ObLobMetaInfo> *>(meta_iter_));
      meta_iter_ = nullptr;
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "meta_iter not null, but adaptor is null, may not release iter", KPC(this));
    }
  }
  adaptor_ = nullptr;
  coll_type_ = ObCollationType::CS_TYPE_INVALID;
  offset_ = 0;
  len_ = 0;
  scan_backward_ = false;
  cur_pos_ = 0;
  cur_byte_pos_ = 0;
  cur_char_pos_ = 0;
  cur_info_.reset();
  is_opened_ = false;
}

int ObLobMetaScanIter::open(ObLobReadContext &read_ctx, ObPersistentLobAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  // build seq idx 1 to skip the meta summary row
  ObString seq_id_start(sizeof(seq_id_start_buf_), seq_id_start_buf_);
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    LOG_WARN("open twice", KR(ret), K(is_opened_));
  } else if (OB_ISNULL(adaptor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_ctx), KP(adaptor));
  } else if (OB_FAIL(ObLobSeqId::get_seq_id(1, seq_id_start))) {
    LOG_WARN("failed to build start seq id", K(ret));
  } else {
    read_ctx.seq_id_start_ = seq_id_start;
    if (read_ctx.byte_size_ > 0 && read_ctx.byte_size_ < ObLobMetaUtil::LOB_PIECE_DATA_SIZE) {
      read_ctx.seq_id_end_ = seq_id_start;
    } else {
      read_ctx.seq_id_end_ = ObString();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(adaptor->scan_lob_meta(read_ctx, meta_iter_))) {
    LOG_WARN("failed to open meta iter", K(ret), K(read_ctx));
  } else {
    adaptor_ = adaptor;
    coll_type_ = read_ctx.coll_type_;
    offset_ = read_ctx.offset_;
    len_ = read_ctx.len_;
    scan_backward_ = read_ctx.scan_backward_;
    cur_byte_pos_ = 0;
    cur_char_pos_ = 0;
    cur_pos_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObLobMetaScanIter::rescan(ObLobReadContext &read_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not open", KR(ret), KPC(this));
  } else if (OB_ISNULL(meta_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta iter is null", KR(ret), KPC(this));
  } else if (OB_FAIL(meta_iter_->rescan(read_ctx))) {
    LOG_WARN("failed to rescan meta iter", K(ret), K(read_ctx));
  } else {
    offset_ = read_ctx.offset_;
    len_ = read_ctx.len_;
    scan_backward_ = read_ctx.scan_backward_;
    cur_pos_ = 0;
    cur_byte_pos_ = 0;
    cur_char_pos_ = 0;
    cur_info_.reset();
  }
  return ret;
}

void ObLobMetaScanIter::update_cur_pos_()
{
  if (is_char()) {
    cur_pos_ = cur_char_pos_;
  } else {
    cur_pos_ = cur_byte_pos_;
  }
}

bool ObLobMetaScanIter::is_in_range(const ObLobMetaInfo &info)
{
  // current meta info covers [start, end), query range is [offset_, offset_ + len_)
  uint64_t start = cur_pos_; 
  uint64_t end = start + (is_char() ? info.char_len_ : info.byte_len_);
  return std::max(start, offset_) < std::min(end, offset_ + len_);
}

bool ObLobMetaScanIter::is_range_over()
{
  return cur_pos_ >= offset_ + len_;
}

int ObLobMetaScanIter::get_next_row(ObLobMetaScanResult &result)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo &row = result.info_;
  row.reset();
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not open", KR(ret), KPC(this));
  } else {
    bool has_found = false;
    while (OB_SUCC(ret) && !has_found) {
      if (is_range_over()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(meta_iter_->get_next_row(row))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next row", K(ret), KPC(this));
        }
      } else {
        cur_info_ = row;
        if (is_in_range(row)) {
          has_found = true;
        }
        // advance scan position by current meta length
        cur_char_pos_ += row.char_len_;
        cur_byte_pos_ += row.byte_len_;
        update_cur_pos_();
      }
    }
  }

  if (OB_SUCC(ret)) {
    uint64_t cur_len = is_char() ? cur_info_.char_len_ : cur_info_.byte_len_;
    uint64_t last_pos = cur_pos_ - cur_len;
    uint64_t range_end = offset_ + len_;
    result.st_ = 0;
    result.len_ = cur_len;
    if (last_pos < offset_) { // cross left boundary
      if (cur_pos_ < offset_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("meta range is not in query range", K(ret), K(last_pos), K(cur_pos_), K(cur_len), K(offset_), K(len_));
      } else {
        result.len_ -= (offset_ - last_pos);
        if (!scan_backward_) {
          result.st_ = offset_ - last_pos;
        }
      }
    }
    if (OB_SUCC(ret) && cur_pos_ > range_end) { // cross right boundary
      if (last_pos > range_end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("meta range is not in query range", K(ret), K(last_pos), K(cur_pos_), K(cur_len), K(offset_), K(len_));
      } else {
        result.len_ -= (cur_pos_ - range_end);
        if (scan_backward_) {
          result.st_ = cur_pos_ - range_end;
        }
      }
    }
  }
  return ret;
}

//==================== ObLobChunkScanIter ====================
ObLobChunkScanIter::ObLobChunkScanIter()
  : is_opened_(false),
    piece_opened_(false),
    is_char_(false),
    scan_backward_(false),
    coll_type_(ObCollationType::CS_TYPE_INVALID),
    adaptor_(nullptr),
    allocator_(ObMemAttr(MTL_ID(), "LobChunkScan")),
    outtable_reader_(),
    read_ctx_(nullptr),
    meta_iter_(),
    piece_iter_(nullptr)
{
  reset();
}

ObLobChunkScanIter::~ObLobChunkScanIter()
{
  reset();
}

void ObLobChunkScanIter::reset()
{
  if (OB_NOT_NULL(piece_iter_)) {
    if (OB_NOT_NULL(adaptor_)) {
      (void)adaptor_->revert_scan_iter(static_cast<ObLobAuxTableIterator<ObLobPieceInfo> *>(piece_iter_));
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "piece_iter not null, but adaptor is null, may not release iter", KPC(this));
    }
    piece_iter_ = nullptr;
  }
  meta_iter_.reset();
  outtable_reader_.reset();
  allocator_.reset();
  adaptor_ = nullptr;

  if (read_ctx_ != nullptr) {
    read_ctx_->seq_id_start_.reset();
    read_ctx_->seq_id_end_.reset();
  }
  read_ctx_ = nullptr;
  is_char_ = false;
  scan_backward_ = false;
  coll_type_ = ObCollationType::CS_TYPE_INVALID;
  piece_opened_ = false;
  is_opened_ = false;
}

int ObLobChunkScanIter::open(ObLobReadContext &read_ctx, ObPersistentLobAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    LOG_WARN("open twice", KR(ret), K(is_opened_));
  } else if (OB_ISNULL(adaptor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_ctx), KP(adaptor));
  } else if (OB_FAIL(meta_iter_.open(read_ctx, adaptor))) {
    LOG_WARN("failed to open meta iter", K(ret), K(read_ctx));
  } else if (OB_FAIL(outtable_reader_.open(&read_ctx))) {
    LOG_WARN("failed to open outtable chunk reader", KR(ret), K(read_ctx));
  } else {
    adaptor_ = adaptor;
    read_ctx_ = &read_ctx;
    coll_type_ = read_ctx.coll_type_;
    is_char_ = read_ctx.is_char();
    scan_backward_ = read_ctx.scan_backward_;
    if (OB_FAIL(prepare_piece_iter_())) {
      LOG_WARN("failed to prepare piece scan", K(ret), K(read_ctx));
    } else {
      is_opened_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObLobChunkScanIter::prepare_piece_iter_()
{
  int ret = OB_SUCCESS;
  bool has_first_meta = false;
  ObLobMetaScanResult meta_result;
  uint64_t rescan_offset = 0;

  ObStringBuffer start_seq_id_buf(&allocator_, ObLobSeqId::LOB_SEQ_ID_MIN_LEN);
  ObStringBuffer last_seq_id_buf(&allocator_, ObLobSeqId::LOB_SEQ_ID_MIN_LEN);
  while (OB_SUCC(ret)) {
    if (OB_FAIL(meta_iter_.get_next_row(meta_result))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next meta row", K(ret), KPC(this));
      }
    } else {
      const uint64_t meta_length = is_char_ ? meta_result.info_.char_len_ : meta_result.info_.byte_len_;
      if (!has_first_meta) {
        if (OB_FAIL(start_seq_id_buf.append(meta_result.info_.seq_id_))) {
          LOG_WARN("failed to save start seq id", K(ret), K(meta_result));
        } else {
          rescan_offset = scan_backward_
                          ? (meta_length - meta_result.st_ - meta_result.len_)
                          : (meta_result.st_);
          has_first_meta = true;
        }
      }
      if (OB_SUCC(ret)) {
        last_seq_id_buf.reuse();
        if (OB_FAIL(last_seq_id_buf.append(meta_result.info_.seq_id_))) {
          LOG_WARN("failed to save last seq id", K(ret), K(meta_result));
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
  } else if (has_first_meta) {
    ObString start_seq_id;
    ObString last_seq_id;
    if (OB_FAIL(start_seq_id_buf.get_result_string(start_seq_id))) {
      LOG_WARN("failed to get start seq id", K(ret));
    } else if (OB_FAIL(last_seq_id_buf.get_result_string(last_seq_id))) {
      LOG_WARN("failed to get last seq id", K(ret));
    } else {
      // when scan_backward_ is true, the start_seq_id can be greater than the last_seq_id
      if (ObLobSeqId(start_seq_id).compare(last_seq_id) > 0) {
        std::swap(start_seq_id, last_seq_id);
      }
      const uint64_t query_offset = read_ctx_->offset_;
      read_ctx_->seq_id_start_ = start_seq_id;
      read_ctx_->seq_id_end_ = last_seq_id;
      // The bounded rescan uses positions relative to the first matched meta row.
      read_ctx_->offset_ = rescan_offset;
      if (OB_FAIL(meta_iter_.rescan(*read_ctx_))) {
        LOG_WARN("failed to rescan meta iter", K(ret), KPC(read_ctx_));
      } else if (OB_FAIL(open_piece_iter_(start_seq_id, last_seq_id))) {
        LOG_WARN("failed to open piece iter", K(ret), KPC(read_ctx_));
      }
      read_ctx_->offset_ = query_offset;
    }
  }
  return ret;
}

int ObLobChunkScanIter::open_piece_iter_(const ObString &lower_seq_id, const ObString &upper_seq_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(lower_seq_id.empty()
      || upper_seq_id.empty()
      || ObLobSeqId(lower_seq_id).compare(upper_seq_id) > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(lower_seq_id), K(upper_seq_id));
  } else {
    read_ctx_->seq_id_start_ = lower_seq_id;
    read_ctx_->seq_id_end_ = upper_seq_id;
  }

  if (FAILEDx(adaptor_->scan_lob_piece(*read_ctx_, piece_iter_))) {
    LOG_WARN("failed to open piece iter", K(ret), KPC(read_ctx_), K(lower_seq_id), K(upper_seq_id));
  } else {
    piece_opened_ = true;
  }
  return ret;
}

int ObLobChunkScanIter::read_in_table_data(
    const ObLobMetaScanResult &meta_result,
    ObString &data)
{
  int ret = OB_SUCCESS;
  ObLobPieceInfo piece;
  data.reset();
  if (OB_UNLIKELY(!piece_opened_ || OB_ISNULL(piece_iter_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece iter is not open", KR(ret), K(meta_result), KPC(this));
  } else if (OB_FAIL(piece_iter_->get_next_row(piece))) {
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("piece iter end but meta still has row", KR(ret), K(meta_result), KPC(this));
    } else {
      LOG_WARN("failed to get next piece row", KR(ret), K(meta_result), KPC(this));
    }
  } else if (OB_UNLIKELY(0 != ObLobSeqId(meta_result.info_.seq_id_).compare(piece.seq_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta and piece seq_id mismatch", KR(ret), K(meta_result), K(piece), KPC(this));
  } else {
    data = piece.lob_data_;
  }
  return ret;
}

int ObLobChunkScanIter::read_outtable_data(
    const ObLobMetaScanResult &meta_result,
    ObString &data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(outtable_reader_.read(meta_result.info_, data))) {
    LOG_WARN("failed to read outtable lob chunk", KR(ret), K(meta_result), KPC(this));
  }
  return ret;
}

int ObLobChunkScanIter::fill_chunk_(
    const ObLobMetaScanResult &meta_result,
    const ObString &data,
    ObLobChunk &chunk)
{
  int ret = OB_SUCCESS;
  const ObLobMetaInfo &meta = meta_result.info_;
  // Full length of the current chunk in the query's unit (bytes or characters).
  const uint64_t chunk_len = is_char_ ? meta.char_len_ : meta.byte_len_;
  chunk.lob_col_idx_ = meta.lob_col_idx_;
  chunk.lob_id_ = meta.lob_id_;
  chunk.seq_id_ = meta.seq_id_;
  chunk.lob_block_id_ = meta.lob_block_id_;

  if (OB_UNLIKELY(static_cast<uint64_t>(data.length()) != meta.byte_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob chunk data length does not match meta", KR(ret), K(meta), K(data.length()));
  } else if (meta_result.st_ == 0 && meta_result.len_ == chunk_len) {
    // Reuse the read buffer when the query range covers the entire chunk.
    chunk.lob_data_ = data;
    chunk.byte_len_ = meta.byte_len_;
    chunk.char_len_ = meta.char_len_;
  } else if (!is_char_) {
    // st_ and len_ use byte semantics for binary data.
    if (OB_UNLIKELY(meta_result.st_ + meta_result.len_ > data.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("slice out of range", K(ret), K(meta_result), "data_len", data.length());
    } else {
      chunk.lob_data_.assign_ptr(data.ptr() + meta_result.st_, static_cast<int32_t>(meta_result.len_));
      chunk.byte_len_ = meta_result.len_;
      chunk.char_len_ = meta_result.len_;
    }
  } else {
    // Convert st_ and len_ to a byte range within the current chunk for character data.
    const int64_t start_byte = ObCharset::charpos(coll_type_, data.ptr(), data.length(), meta_result.st_);
    if (OB_UNLIKELY(start_byte < 0 || start_byte > data.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid start byte", K(ret), K(start_byte), K(meta_result), "data_len", data.length());
    } else {
      const int64_t res_byte_len = ObCharset::charpos(
          coll_type_, data.ptr() + start_byte, data.length() - start_byte, meta_result.len_);
      if (OB_UNLIKELY(res_byte_len <= 0 || start_byte + res_byte_len > data.length())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid byte len", K(ret), K(start_byte), K(res_byte_len),
                 K(meta_result), "data_len", data.length());
      } else {
        chunk.lob_data_.assign_ptr(data.ptr() + start_byte, static_cast<int32_t>(res_byte_len));
        chunk.byte_len_ = res_byte_len;
        chunk.char_len_ = meta_result.len_;
      }
    }
  }
  return ret;
}

int ObLobChunkScanIter::get_next_chunk(ObLobChunk &chunk)
{
  int ret = OB_SUCCESS;
  ObLobMetaScanResult meta_result;
  ObString data;
  chunk.reset();
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    LOG_WARN("not open", KR(ret), KPC(this));
  } else if (OB_FAIL(meta_iter_.get_next_row(meta_result))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next meta row", KR(ret), KPC(this));
    }
  } else if (OB_FAIL(chunk.lob_extra_info_.deserialize(allocator_,
                                                       meta_result.info_.lob_extra_info_))) {
    LOG_WARN("failed to deserialize lob extra info", KR(ret), K(meta_result));
  } else if (OB_UNLIKELY(
      ObLobMetaExtraInfo::LOB_DATA != chunk.lob_extra_info_.extra_info_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob meta extra info type", KR(ret), K(meta_result), K(chunk));
  } else if (chunk.lob_extra_info_.lob_chunk_type_.in_table()) {
    if (OB_FAIL(read_in_table_data(meta_result, data))) {
      LOG_WARN("failed to read in-table lob chunk", KR(ret), K(meta_result), KPC(this));
    }
  } else if (chunk.lob_extra_info_.lob_chunk_type_.out_table()) {
    if (OB_FAIL(read_outtable_data(meta_result, data))) {
      LOG_WARN("failed to read outtable lob chunk", KR(ret), K(meta_result), KPC(this));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported lob chunk type", KR(ret), K(meta_result), K(chunk));
  }
  if (OB_SUCC(ret) && OB_FAIL(fill_chunk_(meta_result, data, chunk))) {
    LOG_WARN("failed to fill lob chunk", KR(ret), K(meta_result), K(data.length()), KPC(this));
  }
  return ret;
}

//==================== ObLobWriteIter ====================
ObLobWriteIter::ObLobWriteIter()
  : is_inited_(false),
    state_(State::INVALID),
    result_allocator_(nullptr),
    query_iter_(nullptr),
    locator_builder_(),
    pending_locator_(),
    coll_type_(CS_TYPE_INVALID),
    lob_col_idx_(OB_INVALID_INDEX_INT64),
    lob_id_(),
    seq_id_generator_(),
    chunk_meta_infos_(),
    summary_info_(),
    meta_idx_(0),
    source_byte_len_(0),
    write_byte_len_(0),
    write_char_len_(0),
    chunk_count_(0),
    piece_result_count_(0),
    meta_result_count_(0)
{
  chunk_meta_infos_.set_attr(ObMemAttr(MTL_ID(), "LobWriteMeta"));
}

int ObLobWriteIter::open(
    const ObLobSourceReadCtx &source_ctx,
    const ObLobTargetWriteCtx &target_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob write iterator init twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!source_ctx.is_valid() || !target_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob write context", KR(ret), K(source_ctx), K(target_ctx));
  } else {
    result_allocator_ = target_ctx.result_allocator_;
    coll_type_ = target_ctx.coll_type_;
    lob_col_idx_ = target_ctx.lob_col_idx_;
    lob_id_ = target_ctx.lob_id_;
    seq_id_generator_.set_allocator(result_allocator_);
    if (OB_FAIL(build_pending_locator_(target_ctx))) {
      LOG_WARN("failed to build pending lob locator", KR(ret), K(target_ctx));
    } else if (source_ctx.read_ctx_->src_loc_.is_mem_external_lob()) {
      if (OB_FAIL(open_external_(source_ctx, target_ctx))) {
        LOG_WARN("failed to open external lob reference", KR(ret), K(source_ctx), K(target_ctx));
      } else {
        state_ = State::META_SUMMARY;
      }
    } else if (OB_FAIL(open_source_(source_ctx))) {
      LOG_WARN("failed to open source lob", KR(ret), K(source_ctx));
    } else {
      state_ = State::PIECE;
    }
    if (OB_FAIL(ret)) {
      reset();
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLobWriteIter::open_external_(
    const ObLobSourceReadCtx &source_ctx,
    const ObLobTargetWriteCtx &target_ctx)
{
  int ret = OB_SUCCESS;
  ObExternalBlobInfo external_info;
  ObLobChunk summary;
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString seq_id(sizeof(seq_id_buf), seq_id_buf);
  if (OB_UNLIKELY(CS_TYPE_BINARY != coll_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid external lob target collation", KR(ret), K_(coll_type), K(lob_id_));
  } else if (OB_FAIL(source_ctx.read_ctx_->src_loc_.get_external_lob_info(external_info))) {
    LOG_WARN("failed to get external lob descriptor", KR(ret), K(source_ctx));
  } else if (OB_FAIL(ObExternalLobMacroIdPrefix::serialize(*result_allocator_,
                                                           target_ctx.lob_meta_tablet_id_.id(),
                                                           target_ctx.external_lob_macro_seq_id_,
                                                           summary.lob_block_id_))) {
    LOG_WARN("failed to encode target external lob identity", KR(ret), K(target_ctx));
  } else if (OB_FAIL(ObLobSeqId::get_summary_seq_id(seq_id))) {
    LOG_WARN("failed to build external lob summary sequence", KR(ret), K(lob_id_));
  } else if (OB_FAIL(summary.lob_extra_info_.set_external_lob_info(*result_allocator_,
                                                                   external_info))) {
    LOG_WARN("failed to copy external lob descriptor", KR(ret), K(lob_id_));
  } else {
    summary.lob_col_idx_ = lob_col_idx_;
    summary.lob_id_ = lob_id_;
    summary.seq_id_ = seq_id;
    summary.byte_len_ = external_info.length_;
    summary.char_len_ = external_info.length_;
    summary.lob_extra_info_.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
    summary.lob_extra_info_.chunk_count_ = 0;
    summary.lob_extra_info_.last_write_type_ = ObLobLastWriteType::INSERT;
    if (OB_FAIL(summary.to_meta(*result_allocator_, summary_info_))) {
      LOG_WARN("failed to build external lob summary meta", KR(ret), K(lob_id_));
    }
  }
  return ret;
}

int ObLobWriteIter::open_source_(const ObLobSourceReadCtx &source_ctx)
{
  int ret = OB_SUCCESS;
  ObLobReadContext query_ctx;
  if (OB_UNLIKELY(source_ctx.read_ctx_->coll_type_ != coll_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("source and target lob collation mismatch", KR(ret), K(source_ctx), K_(coll_type));
  } else if (OB_FAIL(query_ctx.assign(*source_ctx.read_ctx_))) {
    LOG_WARN("failed to copy lob query parameter", KR(ret), K(source_ctx));
  } else if (OB_FAIL(source_ctx.lob_manager_->query(query_ctx, query_iter_))) {
    LOG_WARN("failed to open lob query iterator", KR(ret), K(source_ctx));
  } else {
    source_byte_len_ = source_ctx.source_byte_len_;
  }
  return ret;
}

int ObLobWriteIter::build_pending_locator_(const ObLobTargetWriteCtx &target_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(locator_builder_.init_outrow(*target_ctx.locator_allocator_))) {
    LOG_WARN("failed to init outrow lob locator", KR(ret), K(target_ctx));
  } else if (OB_FAIL(locator_builder_.set_lob_id(target_ctx.lob_id_))) {
    LOG_WARN("failed to set target lob id", KR(ret), K(target_ctx));
  } else if (OB_FAIL(locator_builder_.set_seq_no(target_ctx.seq_no_))) {
    LOG_WARN("failed to set target lob sequence", KR(ret), K(target_ctx));
  } else if (OB_FAIL(locator_builder_.to_locator(pending_locator_))) {
    LOG_WARN("failed to build target lob locator", KR(ret), K(target_ctx));
  }
  return ret;
}

int ObLobWriteIter::get_next_row(ObLobWriteResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob write iterator is not initialized", KR(ret), KPC(this));
  } else {
    switch (state_) {
      case State::PIECE: {
        ret = get_next_piece_(result);
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(finish_piece_phase_())) {
            LOG_WARN("failed to finish lob piece phase", KR(ret), KPC(this));
          } else if (OB_FAIL(get_next_meta_(result))) {
            LOG_WARN("failed to get lob summary meta", KR(ret), KPC(this));
          }
        }
        break;
      }
      case State::META_SUMMARY:
      case State::META_CHUNK: {
        ret = get_next_meta_(result);
        break;
      }
      case State::END: {
        ret = OB_ITER_END;
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected lob write iterator state", KR(ret), KPC(this));
        break;
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret) {
      state_ = State::INVALID;
    }
  }
  return ret;
}

int ObLobWriteIter::get_next_piece_(ObLobWriteResult &result)
{
  int ret = OB_SUCCESS;
  ObLobChunk chunk;
  if (OB_FAIL(get_next_source_chunk_(chunk))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get source lob chunk", KR(ret), KPC(this));
    }
  } else if (OB_FAIL(build_chunk_write_result_(chunk, result))) {
    LOG_WARN("failed to build lob chunk write result", KR(ret), K(chunk), KPC(this));
  }
  return ret;
}

int ObLobWriteIter::build_chunk_write_result_(
    ObLobChunk &chunk,
    ObLobWriteResult &result)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo meta_info;
  chunk.lob_extra_info_.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  chunk.lob_extra_info_.lob_chunk_type_.set_in_table();
  chunk.lob_col_idx_ = lob_col_idx_;
  chunk.lob_id_ = lob_id_;
  if (OB_FAIL(seq_id_generator_.get_next_seq_id(chunk.seq_id_))) {
    LOG_WARN("failed to generate target lob sequence id", KR(ret), KPC(this));
  } else if (OB_FAIL(add_chunk_stats_(chunk))) {
    LOG_WARN("failed to add lob chunk statistics", KR(ret), K(chunk));
  } else if (OB_FAIL(chunk.to_meta(*result_allocator_, meta_info))) {
    LOG_WARN("failed to build lob chunk meta", KR(ret), K(chunk));
  } else if (OB_UNLIKELY(!chunk_meta_infos_.empty()
             && ObLobSeqId(meta_info.seq_id_).compare(
                  chunk_meta_infos_.at(chunk_meta_infos_.count() - 1).seq_id_) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("target lob sequence id is not increasing", KR(ret), K(meta_info),
             "last_meta", chunk_meta_infos_.at(chunk_meta_infos_.count() - 1));
  } else if (OB_FAIL(chunk_meta_infos_.push_back(meta_info))) {
    LOG_WARN("failed to save lob chunk meta", KR(ret), K(meta_info));
  } else if (OB_FAIL(chunk.to_piece(result.piece_info_))) {
    LOG_WARN("failed to build lob piece", KR(ret), K(chunk));
  } else {
    result.type_ = ObLobWriteResultType::PIECE;
    ++piece_result_count_;
  }
  return ret;
}

int ObLobWriteIter::finish_piece_phase_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::PIECE != state_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("lob write iterator is not in piece phase", KR(ret), KPC(this));
  } else if (OB_FAIL(build_summary_())) {
    LOG_WARN("failed to build lob meta summary", KR(ret), KPC(this));
  } else {
    state_ = State::META_SUMMARY;
  }
  return ret;
}

int ObLobWriteIter::get_next_source_chunk_(ObLobChunk &chunk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source lob query iterator is null", KR(ret), KPC(this));
  } else if (OB_FAIL(query_iter_->get_next_chunk(
      chunk, ObLobMetaUtil::LOB_PIECE_DATA_SIZE))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get source lob chunk", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObLobWriteIter::get_next_meta_(ObLobWriteResult &result)
{
  int ret = OB_SUCCESS;
  if (State::META_SUMMARY == state_) {
    result.type_ = ObLobWriteResultType::META_SUMMARY;
    result.meta_info_ = summary_info_;
    ++meta_result_count_;
    state_ = chunk_meta_infos_.empty() ? State::END : State::META_CHUNK;
  } else if (State::META_CHUNK == state_) {
    if (OB_UNLIKELY(meta_idx_ < 0 || meta_idx_ >= chunk_meta_infos_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob chunk meta index", KR(ret), K_(meta_idx), K(chunk_meta_infos_.count()));
    } else {
      result.type_ = ObLobWriteResultType::META_CHUNK;
      result.meta_info_ = chunk_meta_infos_.at(meta_idx_);
      ++meta_idx_;
      ++meta_result_count_;
      if (meta_idx_ == chunk_meta_infos_.count()) {
        state_ = State::END;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob meta iterator state", KR(ret), KPC(this));
  }
  return ret;
}

int ObLobWriteIter::add_chunk_stats_(const ObLobChunk &chunk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(chunk.byte_len_ == 0
      || chunk.byte_len_ != static_cast<uint64_t>(chunk.lob_data_.length())
      || chunk.char_len_ > chunk.byte_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid source lob chunk", KR(ret), K(chunk));
  } else if (OB_UNLIKELY(UINT64_MAX - write_byte_len_ < chunk.byte_len_
             || UINT64_MAX - write_char_len_ < chunk.char_len_
             || UINT64_MAX == chunk_count_)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("lob write statistics overflow", KR(ret), K(chunk), KPC(this));
  } else {
    write_byte_len_ += chunk.byte_len_;
    write_char_len_ += chunk.char_len_;
    ++chunk_count_;
  }
  return ret;
}

int ObLobWriteIter::build_summary_()
{
  int ret = OB_SUCCESS;
  ObLobChunk summary;
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString seq_id(sizeof(seq_id_buf), seq_id_buf);
  if (OB_UNLIKELY((UINT64_MAX != source_byte_len_ && source_byte_len_ != write_byte_len_)
      || chunk_count_ != static_cast<uint64_t>(chunk_meta_infos_.count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob write length or chunk count mismatch", KR(ret), KPC(this));
  } else if (OB_FAIL(ObLobSeqId::get_summary_seq_id(seq_id))) {
    LOG_WARN("failed to build lob summary sequence id", KR(ret), KPC(this));
  } else {
    summary.lob_col_idx_ = lob_col_idx_;
    summary.lob_id_ = lob_id_;
    summary.seq_id_ = seq_id;
    summary.byte_len_ = write_byte_len_;
    summary.char_len_ = write_char_len_;
    summary.lob_extra_info_.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
    summary.lob_extra_info_.lob_chunk_type_.set_in_table();
    summary.lob_extra_info_.chunk_count_ = chunk_count_;
    summary.lob_extra_info_.last_write_type_ = ObLobLastWriteType::INSERT;
    if (OB_FAIL(summary.to_meta(*result_allocator_, summary_info_))) {
      LOG_WARN("failed to build lob summary meta", KR(ret), K(summary));
    }
  }
  return ret;
}

int ObLobWriteIter::check_write_result_() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(State::END != state_
      || (UINT64_MAX != source_byte_len_ && source_byte_len_ != write_byte_len_)
      || piece_result_count_ != chunk_count_
      || meta_result_count_ != chunk_count_ + 1
      || meta_idx_ != chunk_meta_infos_.count())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("lob write result is incomplete", KR(ret), KPC(this));
  }
  return ret;
}

int ObLobWriteIter::get_pending_locator(ObLobLocatorV2 &locator) const
{
  int ret = OB_SUCCESS;
  locator = ObLobLocatorV2();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob write iterator is not initialized", KR(ret), KPC(this));
  } else if (OB_FAIL(check_write_result_())) {
    LOG_WARN("lob write result is incomplete", KR(ret), KPC(this));
  } else {
    locator = pending_locator_;
  }
  return ret;
}

void ObLobWriteIter::reset_query_iter_()
{
  if (nullptr != query_iter_) {
    query_iter_->reset();
    OB_DELETE(ObLobQueryIter, "unused", query_iter_);
    query_iter_ = nullptr;
  }
}

void ObLobWriteIter::reset()
{
  reset_query_iter_();
  seq_id_generator_.reset();
  seq_id_generator_.set_allocator(nullptr);
  chunk_meta_infos_.reset();
  locator_builder_.reset();
  pending_locator_ = ObLobLocatorV2();
  summary_info_.reset();
  result_allocator_ = nullptr;
  coll_type_ = CS_TYPE_INVALID;
  lob_col_idx_ = OB_INVALID_INDEX_INT64;
  lob_id_.reset();
  meta_idx_ = 0;
  source_byte_len_ = 0;
  write_byte_len_ = 0;
  write_char_len_ = 0;
  chunk_count_ = 0;
  piece_result_count_ = 0;
  meta_result_count_ = 0;
  state_ = State::INVALID;
  is_inited_ = false;
}
}
}
