/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information and may not be used or disclosed without authorization.
 */

#include "storage/lob/store/ob_lob_persistent_appender.h"

#define USING_LOG_PREFIX STORAGE
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_aux_manager.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_store_common.h"

namespace oceanbase
{
namespace storage
{

using namespace common;

ObLobPersistentAppender::ObLobPersistentAppender()
{
  reset();
}

ObLobPersistentAppender::~ObLobPersistentAppender()
{
  reset();
}

void ObLobPersistentAppender::reset()
{
  write_stats_.reset();
  source_iter_.reset();
  old_piece_.reset();
  first_buffer_.reset();
  append_data_.reset();
  old_summary_info_.reset();
  last_meta_info_.reset();
  piece_mgr_ = nullptr;
  meta_mgr_ = nullptr;
  read_ctx_.reset();
  ctx_ = nullptr;
  is_inited_ = false;
  is_no_logging_ = false;
}

int ObLobPersistentAppender::init(
    ObLobAppendContext *ctx,
    ObLobMetaManager *meta_mgr,
    ObLobPieceManager *piece_mgr,
    const ObLobMetaInfo &old_summary_info,
    const ObLobMetaInfo &last_meta_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("persistent lob appender initialized twice", KR(ret));
  } else if (OB_ISNULL(ctx) || OB_ISNULL(meta_mgr) || OB_ISNULL(piece_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persistent lob appender argument", KR(ret), KPC(ctx), KPC(meta_mgr), KPC(piece_mgr));
  } else if (OB_FAIL(ctx->build_read_context(read_ctx_))) {
    LOG_WARN("failed to build persistent append read context", KR(ret), KPC(ctx));
  } else if (OB_FAIL(detect_no_logging_(*ctx))) {
    LOG_WARN("failed to detect no logging", KR(ret), KPC(ctx), K(old_summary_info));
  } else {
    read_ctx_.allocator_ = &ctx->own_allocator_;
    ctx_ = ctx;
    meta_mgr_ = meta_mgr;
    piece_mgr_ = piece_mgr;
    old_summary_info_ = old_summary_info;
    last_meta_info_ = last_meta_info;
    is_inited_ = true;
  }
  return ret;
}

int ObLobPersistentAppender::detect_no_logging_(const ObLobAppendContext &ctx)
{
  int ret = OB_SUCCESS;
  bool is_write_clog = false;
  if (OB_FAIL(ObLobOutTableIO::enable_outtable_clog(ctx.tenant_id_, is_write_clog))) {
    LOG_WARN("failed to check whether to write outtable lob clog", KR(ret), K(ctx));
  } else {
    is_no_logging_ = !is_write_clog;
  }
  return ret;
}

int ObLobPersistentAppender::prepare_source_()
{
  int ret = OB_SUCCESS;
  source_iter_.reset();

  // this function may be called repeatedly because outtable lob appender rereads the data when writing CLOG.
  // first_buffer_ does not need to be filled twice.
  if (first_buffer_.empty()) {
    if (ctx_->old_loc_.is_inrow()) {
      ObString inrow_data;
      if (OB_FAIL(ctx_->old_loc_.get_inrow_data(inrow_data))) {
        LOG_WARN("failed to get inrow data", KR(ret), KPC(this));
      } else {
        first_buffer_.assign_ptr(inrow_data.ptr(), inrow_data.length());
      }
    } else if (ctx_->update_first_) {
      // When update_first_ is true, the tail chunk is not full. Read it into first_buffer_ before appending.
      if (OB_FAIL(read_last_chunk_())) {
        LOG_WARN("failed to read last lob chunk", KR(ret), KPC(this));
      }
    } else {
      // the source lob is not inrow, or the last chunk is full. so first buffer is empty.
      // do nothing.
    }
  }

  ObString append_data;
  const ObMemLobExternHeader *_header = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ctx_->delta_loc_.get_append_delta(_header, append_data))) {
    LOG_WARN("failed to get append data", KR(ret), KPC(this));
  } else if (OB_FAIL(source_iter_.open(&ctx_->own_allocator_, first_buffer_, append_data, ctx_->coll_type_))) {
    LOG_WARN("failed to open lob append source iterator", KR(ret), KPC(ctx_));
  }
  return ret;
}

int ObLobPersistentAppender::validate_write_stats_(uint64_t &total_chunk_count)
{
  int ret = OB_SUCCESS;
  uint64_t old_chunk_count = 0;
  total_chunk_count = 0;

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(ctx_->cur_byte_length_ + write_stats_.byte_len_ != ctx_->total_byte_length_
                         || ctx_->cur_char_length_ + write_stats_.char_len_ != ctx_->total_char_length_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append write statistics mismatch", KR(ret), K(write_stats_), KPC(this));
  } else {
    total_chunk_count = ctx_->cur_chunk_count_ + write_stats_.chunk_count_;
  }
  return ret;
}

int ObLobPersistentAppender::write_meta_summary_(const uint64_t total_chunk_count)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo summary_info;
  ObLobMetaExtraInfo extra_info;
  if (OB_FAIL(ObLobSeqId::get_seq_id(ctx_->own_allocator_, 0, summary_info.seq_id_))) {
    LOG_WARN("failed to build lob summary seq id", KR(ret));
  } else {
    summary_info.lob_col_idx_ = ctx_->store_column_idx_;
    summary_info.lob_id_ = ctx_->lob_id_;
    summary_info.byte_len_ = ctx_->total_byte_length_;
    summary_info.char_len_ = ctx_->total_char_length_;
    extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
    extra_info.chunk_count_ = total_chunk_count;
    extra_info.last_write_type_ = ObLobLastWriteType::APPEND;
    if (is_outtable()) {
      extra_info.lob_chunk_type_.set_out_table();
    } else {
      extra_info.lob_chunk_type_.set_in_table();
    }
    if (OB_FAIL(extra_info.serialize(ctx_->own_allocator_, summary_info.lob_extra_info_))) {
      LOG_WARN("failed to serialize appended lob summary", KR(ret), K(extra_info));
    } else if (ctx_->old_loc_.is_inrow()) {
      ctx_->dml_op_ = ObLobDmlOp::INSERT;
      if (OB_FAIL(meta_mgr_->insert(*ctx_, summary_info))) {
        LOG_WARN("failed to insert appended lob summary", KR(ret), K(summary_info));
      }
    } else {
      ctx_->dml_op_ = ObLobDmlOp::UPDATE;
      if (OB_FAIL(meta_mgr_->update(*ctx_, old_summary_info_, summary_info))) {
        LOG_WARN("failed to update appended lob summary", KR(ret), K(old_summary_info_), K(summary_info));
      }
    }
  }
  return ret;
}

int ObLobPersistentAppender::after_write_summary()
{
  // do nothing.
  return OB_SUCCESS;
}

int ObLobPersistentAppender::append()
{
  int ret = OB_SUCCESS;
  uint64_t total_chunk_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("persistent lob appender is not initialized", KR(ret));
  } else if (OB_FAIL(prepare_source_())) {
    LOG_WARN("failed to prepare persistent append source", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(write_data())) {
    LOG_WARN("failed to write appended lob data", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(validate_write_stats_(total_chunk_count))) {
    LOG_WARN("failed to validate append write statistics", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(write_meta_summary_(total_chunk_count))) {
    LOG_WARN("failed to write appended lob summary", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(OB_E(EventTable::ERRSIM_LOB_APPEND_AFTER_SUMMARY_UPDATE) OB_SUCCESS)) {
    LOG_WARN("injected failure after updating appended lob summary", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(after_write_summary())) {
    LOG_WARN("failed to finish persistent lob append", KR(ret), KPC(ctx_));
  }
  return ret;
}

//==================== ObLobInTablePersistentAppender ====================
ObLobInTablePersistentAppender::ObLobInTablePersistentAppender()
  : ObLobPersistentAppender()
{
  reset();
}

ObLobInTablePersistentAppender::~ObLobInTablePersistentAppender()
{
  reset();
}

void ObLobInTablePersistentAppender::reset()
{
  ObLobPersistentAppender::reset();
}

int ObLobInTablePersistentAppender::write_data()
{
  int ret = OB_SUCCESS;
  ObArray<ObLobMetaInfo> metas;
  ObLobInTablePieceInsertIter piece_iter;
  metas.set_attr(ObMemAttr(MTL_ID(), "LobAppendMetas"));
  if (OB_FAIL(piece_iter.init(&source_iter_, &ctx_->own_allocator_, ctx_->get_chunk_size(false/*is_outtable*/),
                              ctx_->store_column_idx_, ctx_->lob_id_, &metas, &write_stats_))) {
    LOG_WARN("failed to initialize append piece iterator", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(piece_iter.set_start_seq_id(ctx_->next_seq_id_, true))) {
    LOG_WARN("failed to set append start seq id", KR(ret), K(ctx_->next_seq_id_));
  } else if (ctx_->update_first_) {
    blocksstable::ObDatumRow *row = nullptr;
    if (OB_FAIL(piece_iter.get_next_row(row))) {
      LOG_WARN("failed to build first appended intable chunk", KR(ret));
    } else if (OB_ISNULL(row) || OB_UNLIKELY(1 != metas.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid first appended intable chunk", KR(ret), KP(row), K(metas.count()));
    } else {
      ObLobPieceInfo &new_piece = piece_iter.get_current_piece();
      ObLobMetaInfo &new_meta = metas.at(0);
      ctx_->dml_op_ = ObLobDmlOp::UPDATE;
      if (OB_FAIL(piece_mgr_->update(*ctx_, old_piece_, new_piece))) {
        LOG_WARN("failed to update last lob piece", KR(ret), K(old_piece_), K(new_piece));
      } else if (OB_FAIL(OB_E(EventTable::ERRSIM_LOB_APPEND_AFTER_TAIL_PIECE_UPDATE) OB_SUCCESS)) {
        LOG_WARN("injected failure after updating last lob piece", KR(ret), K(old_piece_), K(new_piece));
      } else if (OB_FAIL(meta_mgr_->update(*ctx_, last_meta_info_, new_meta))) {
        LOG_WARN("failed to update last lob meta", KR(ret), K(last_meta_info_), K(new_meta));
      } else if (OB_FAIL(OB_E(EventTable::ERRSIM_LOB_APPEND_AFTER_TAIL_META_UPDATE) OB_SUCCESS)) {
        LOG_WARN("injected failure after updating last lob meta", KR(ret), K(last_meta_info_), K(new_meta));
      } else {
        metas.reuse();
      }
    }
  }
  if (OB_SUCC(ret) && !source_iter_.is_end()) {
    ctx_->dml_op_ = ObLobDmlOp::INSERT;
    ObLobMetaTableBatchInsertIter meta_iter;
    if (OB_FAIL(piece_mgr_->batch_insert(*ctx_, piece_iter))) {
      LOG_WARN("failed to insert appended lob pieces", KR(ret), KPC(ctx_));
    } else if (OB_FAIL(OB_E(EventTable::ERRSIM_LOB_APPEND_AFTER_PIECE_BATCH_INSERT) OB_SUCCESS)) {
      LOG_WARN("injected failure after inserting appended lob pieces", KR(ret), KPC(ctx_));
    } else if (OB_FAIL(meta_iter.init(&metas))) {
      LOG_WARN("failed to initialize appended meta iterator", KR(ret));
    } else if (OB_FAIL(meta_mgr_->batch_insert(*ctx_, meta_iter))) {
      LOG_WARN("failed to insert appended lob meta rows", KR(ret), KPC(ctx_));
    } else if (OB_FAIL(OB_E(EventTable::ERRSIM_LOB_APPEND_AFTER_META_BATCH_INSERT) OB_SUCCESS)) {
      LOG_WARN("injected failure after inserting appended lob metas", KR(ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObLobInTablePersistentAppender::read_last_chunk_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(piece_mgr_->get_piece_info(read_ctx_, last_meta_info_.seq_id_, old_piece_))) {
    LOG_WARN("failed to read last lob piece", KR(ret), K(last_meta_info_));
  } else if (OB_FAIL(ob_write_string(ctx_->own_allocator_, old_piece_.lob_data_, first_buffer_))) {
    LOG_WARN("failed to copy intable tail block", KR(ret), K(last_meta_info_));
  }
  return ret;
}

//==================== ObLobOutTablePersistentAppender ====================
ObLobOutTablePersistentAppender::ObLobOutTablePersistentAppender()
  : ObLobPersistentAppender(),
    clog_metas_()
{
  clog_metas_.set_attr(ObMemAttr(MTL_ID(), "LobAppendClog"));
  reset();
}

ObLobOutTablePersistentAppender::~ObLobOutTablePersistentAppender()
{
  reset();
}

void ObLobOutTablePersistentAppender::reset()
{
  clog_metas_.reset();
  ObLobPersistentAppender::reset();
}

int ObLobOutTablePersistentAppender::write_data()
{
  int ret = OB_SUCCESS;
  ObArray<ObLobMetaInfo> metas;
  ObLobOutTableInsertIter write_iter;
  metas.set_attr(ObMemAttr(MTL_ID(), "LobAppendOut"));
  clog_metas_.reset();
  const int64_t chunk_size = ctx_->get_chunk_size(true/*is_outtable*/);
  const int64_t append_byte_len = ctx_->total_byte_length_ - ctx_->cur_byte_length_;
  int64_t mbmaxlen = 0;
  int64_t append_chunk_count = 0;
  if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(ctx_->coll_type_, mbmaxlen))) {
    LOG_WARN("failed to get max character length", KR(ret), K(ctx_->coll_type_));
  } else {
    const int64_t min_chunk_size = chunk_size - mbmaxlen + 1;
    append_chunk_count = (append_byte_len + min_chunk_size - 1) / min_chunk_size;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLobUtil::refresh_lob_tablet_id(*ctx_))) {
    LOG_WARN("failed to prepare lob tablet id for outtable append", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(write_iter.init(ctx_, &source_iter_, &ctx_->own_allocator_, chunk_size, append_chunk_count,
                     ctx_->store_column_idx_, ctx_->lob_id_, &metas, &write_stats_))) {
    LOG_WARN("failed to initialize outtable append iterator", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(write_iter.set_start_seq_id(ctx_->next_seq_id_, true))) {
    LOG_WARN("failed to set outtable append start seq id", KR(ret), K(ctx_->next_seq_id_));
  } else if (OB_FAIL(write_iter.write_all())) {
    LOG_WARN("failed to write appended outtable blocks", KR(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(metas.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outtable append generated no meta", KR(ret));
  } else if (ctx_->update_first_) {
    ctx_->dml_op_ = ObLobDmlOp::UPDATE;
    if (OB_FAIL(meta_mgr_->update(*ctx_, last_meta_info_, metas.at(0)))) {
      LOG_WARN("failed to update outtable tail meta", KR(ret), K(last_meta_info_), K(metas.at(0)));
    } else if (OB_FAIL(clog_metas_.push_back(metas.at(0)))) {
      LOG_WARN("failed to record appended outtable meta for clog", KR(ret), KPC(this));
    } else {
      metas.remove(0);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!metas.empty()) {
    ObLobMetaTableBatchInsertIter meta_iter;
    ctx_->dml_op_ = ObLobDmlOp::INSERT;
    if (OB_FAIL(meta_iter.init(&metas))) {
      LOG_WARN("failed to initialize appended meta iterator", KR(ret), KPC(this));
    } else if (OB_FAIL(meta_mgr_->batch_insert(*ctx_, meta_iter))) {
      LOG_WARN("failed to insert appended lob meta rows", KR(ret), KPC(this));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < metas.count(); ++i) {
      if (OB_FAIL(clog_metas_.push_back(metas.at(i)))) {
        LOG_WARN("failed to record appended outtable meta for clog", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObLobOutTablePersistentAppender::read_last_chunk_()
{
  int ret = OB_SUCCESS;
  ObString chunk_data;
  ObLobOutTableChunkReader outtable_reader;
  if (OB_FAIL(outtable_reader.open(&read_ctx_))) {
    LOG_WARN("failed to open outtable tail reader", KR(ret), K(last_meta_info_));
  } else if (OB_FAIL(outtable_reader.read(last_meta_info_, chunk_data))) {
    LOG_WARN("failed to read outtable tail block", KR(ret), K(last_meta_info_));
  } else if (OB_FAIL(ob_write_string(ctx_->own_allocator_, chunk_data, first_buffer_))) {
    LOG_WARN("failed to copy outtable tail block", KR(ret), K(last_meta_info_));
  }
  return ret;
}

int ObLobOutTablePersistentAppender::after_write_summary()
{
  int ret = OB_SUCCESS;
  ObLobQueryIter *iter = is_no_logging_ ? nullptr : &source_iter_;
  if (!is_no_logging_ && OB_FAIL(prepare_source_())) {
    LOG_WARN("failed to prepare source for clog write", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(ObLobOutTableIO::write_clog(*ctx_, iter, clog_metas_, !is_no_logging_))) {
    LOG_WARN("failed to write outtable clog", KR(ret), KPC(ctx_));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
