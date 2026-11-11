/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <algorithm>

#include "lib/allocator/page_arena.h"
#include "storage/lob/store/ob_lob_store_common.h"
#define USING_LOG_PREFIX STORAGE
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "ob_lob_handler.h"
#include "storage/lob/store/ob_lob_persistent_iterator.h"
#include "storage/lob/access/ob_lob_retry.h"
#include "storage/lob/access/ob_lob_iterator.h"

namespace oceanbase
{
namespace storage
{

/*************ObLobQueryBaseHandler*****************/

int ObLobQueryBaseHandler::init_base(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lob_meta_mngr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob meta manager is null", KR(ret), KP(lob_meta_mngr));
  } else {
    lob_meta_mngr_ = lob_meta_mngr;
  }
  return ret;
}

int ObLobQueryBaseHandler::execute()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t retry_cnt = 0;
  bool is_continue = true;
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
  do {
    if (OB_FAIL(do_execute())) {
      LOG_WARN("do_execute fail, check need rerty", KR(ret), K(retry_cnt), K(read_ctx_));
      is_continue = false;
      if (read_ctx_.no_need_retry_) {
        LOG_INFO("no need retry", K(ret), K(is_continue), K(retry_cnt), K(read_ctx_));
        is_continue = false;
      } else if (OB_TMP_FAIL(ObLobRetryUtil::check_need_retry(read_ctx_, ret, retry_cnt, is_continue))) {
        LOG_WARN("check_need_retry fail", K(tmp_ret), K(ret), K(is_continue), K(retry_cnt), K(read_ctx_));
        // check fail, do not retry
        is_continue = false;
      }
    } else {
      is_continue = false;
    }
    if (is_continue) {
      if (OB_NOT_NULL(read_ctx_.access_ctx_)) {
        read_ctx_.access_ctx_->reset();
      }
      read_ctx_.iter_allocator_.reset();
      ++retry_cnt;
      LOG_INFO("[LOB RETRY] retry again", K(ret), K(retry_cnt), K(read_ctx_),
          K(read_ctx_.allocator_->total()), K(read_ctx_.allocator_->used()));
    }
  } while (is_continue);
  return ret;
}

/*************ObLobQueryIterHandler*****************/
int ObLobQueryIterHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", K(ret), K(read_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobQueryIterHandler::do_execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_ISNULL(result_ = OB_NEW(ObLobOutRowQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc lob meta scan iterator fail", K(ret));
  } else if (OB_FAIL(result_->open(read_ctx_, lob_meta_mngr_))) {
    LOG_WARN("open lob query iter failed", K(ret), K(read_ctx_));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    result_->reset();
    OB_DELETE(ObLobOutRowQueryIter, "unused", result_);
    result_ = nullptr;
  }
  return ret;
}

/*************ObLobQueryDataHandler*****************/
int ObLobQueryDataHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", K(ret), K(read_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobQueryDataHandler::do_execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else {
    ObLobOutRowQueryIter iter;
    if (OB_FAIL(iter.open(read_ctx_, lob_meta_mngr_))) {
      LOG_WARN("failed to open outrow query iter", K(ret), K(read_ctx_));
    } else {
      while (OB_SUCC(ret) && result_.remain() > 0) {
        if (OB_FAIL(iter.get_next_row(result_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row", K(ret), K(read_ctx_));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    iter.reset();
  }

  if (OB_FAIL(ret)) {
    result_.set_length(0);
  } 
  return ret;
}

/*************ObLobQueryLengthHandler*****************/
int ObLobQueryLengthHandler::init(
    ObLobMetaManager *lob_meta_mngr,
    QueryType query_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", K(ret), K(read_ctx_));
  } else {
    query_type_ = query_type;
    is_inited_ = true;
  }
  return ret;
}

int ObLobQueryLengthHandler::do_execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else {
    if (query_type_ == BYTE_LENGTH) {
      if (OB_FAIL(lob_meta_mngr_->get_byte_length(read_ctx_, result_))) {
        LOG_WARN("fail to get byte length", K(ret));
      }
    } else if (query_type_ == CHAR_LENGTH) {
      if (OB_FAIL(lob_meta_mngr_->get_char_length(read_ctx_, result_))) {
        LOG_WARN("fail to get char length", K(ret));
      }
    } else {
      LOG_WARN("fail to get length", K(ret));
    }
  }
  return ret;
}

/*************ObLobQueryInfoHandler*****************/
int ObLobQueryInfoHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("initialize lob query info handler failed", KR(ret), K(read_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobQueryInfoHandler::do_execute()
{
  int ret = OB_SUCCESS;
  result_.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob query info handler is not initialized", KR(ret));
  } else if (OB_FAIL(lob_meta_mngr_->get_meta_summary_info(read_ctx_, result_))) {
    LOG_WARN("get lob meta summary info failed", KR(ret), K(read_ctx_));
  }
  return ret;
}

//==================== ObLobInserter ====================
void ObLobInserter::reset()
{
  ctx_ = nullptr;
  meta_mgr_ = nullptr;
  piece_mgr_ = nullptr;
  is_inited_ = false;
}

int ObLobInserter::init(
    ObLobInsertContext *ctx,
    ObLobMetaManager *meta_mgr,
    ObLobPieceManager *piece_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(ctx) || OB_ISNULL(meta_mgr) || OB_ISNULL(piece_mgr)) {
    // TODO fengyun param.is_valid()
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(ctx), KPC(meta_mgr), KPC(piece_mgr));
  } else {
    is_inited_ = true;
    ctx_ = ctx;
    meta_mgr_ = meta_mgr;
    piece_mgr_ = piece_mgr;
  }
  return ret;
}

int ObLobInserter::insert()
{
  int ret = OB_SUCCESS;
  ObLobDiskLocatorWrapper dest_wrapper;
  ObLobId lob_id;
  ObLobQueryIter *query_iter = nullptr;
  uint64_t source_byte_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(dest_wrapper.init(ctx_->dest_loc_.ptr_, ctx_->dest_loc_.size_))) {
    LOG_WARN("failed to init dest locator wrapper", KR(ret), KPC(this));
  } else if (OB_FAIL(dest_wrapper.get_lob_id(lob_id))) {
    LOG_WARN("failed to get lob id from dest locator", KR(ret), KPC(this));
  } else if (OB_FAIL(open_src_lob_query_(query_iter, source_byte_len))) {
    LOG_WARN("failed to open source lob query", KR(ret), KPC(this));
  } else {
    // insert 采用流式写入的方式，当输入是 outrow lob 时，流式写入可避免完整加载 lob 的内存压力
    // 采用边 load 边写入的方式，内存常驻的仅有单个分片的大小。
    // 流式写入步骤：
    // 1. 先写 Piece 表，写入过程中维护每个分片的 byte length 和 char length
    // 2. 再写 meta 表
    ObArray<ObLobMetaInfo> metas;
    ObLobWriteStats write_stats;
    ObLobInTablePieceInsertIter piece_insert_iter;
    ObLobMetaTableBatchInsertIter meta_insert_iter;
    metas.set_attr(ObMemAttr(MTL_ID(), "LobMetas"));
    // 设置 lob 辅助表的 dml 操作符号，以及所用的 seq_no
    ctx_->dml_op_ = ObLobDmlOp::INSERT;
    ctx_->insert_lob_seq_no_ = dest_wrapper.get_seq_no();

    if (OB_FAIL(piece_insert_iter.init(query_iter, &ctx_->own_allocator_, ctx_->get_chunk_size(),
                           ctx_->store_column_idx_, lob_id, &metas, &write_stats))) {
      LOG_WARN("failed to initialize piece insert iterator", KR(ret), KPC(this));
    } else if (OB_FAIL(piece_mgr_->batch_insert(*ctx_, piece_insert_iter))) {
      LOG_WARN("failed to batch insert piece info", KR(ret), KPC(this));
    } else if (OB_UNLIKELY(!piece_insert_iter.is_end())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("piece insert did not consume source iterator", KR(ret), K(piece_insert_iter), KPC(this));
    } else if (OB_UNLIKELY(write_stats.byte_len_ != source_byte_len
                           || write_stats.chunk_count_ != static_cast<uint64_t>(metas.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob stream statistics do not match source", KR(ret), K(source_byte_len),
               K(write_stats), "meta_count", metas.count(), KPC(this));
    }

    if (FAILEDx(write_meta_summary_(
        lob_id, write_stats.byte_len_, write_stats.char_len_,
        write_stats.chunk_count_))) {
      LOG_WARN("failed to write meta summary row", KR(ret), K(lob_id),
               K(write_stats), KPC(this));
    } else if (OB_FAIL(meta_insert_iter.init(&metas))) {
      LOG_WARN("failed to initialize meta insert iterator", KR(ret), KPC(this));
    } else if (OB_FAIL(meta_mgr_->batch_insert(*ctx_, meta_insert_iter))) {
      LOG_WARN("failed to batch insert meta info", KR(ret), KPC(this));
    }
  }
  if (OB_NOT_NULL(query_iter)) {
    OB_DELETE(ObLobQueryIter, "unused", query_iter);
    query_iter = nullptr;
  }
  return ret;
}

int ObLobInserter::open_src_lob_query_(
    ObLobQueryIter *&query_iter,
    uint64_t &source_byte_len)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = MTL(ObLobManager*);
  ObLobReadContext read_ctx;
  
  query_iter = nullptr;
  source_byte_len = 0;
  if (OB_ISNULL(lob_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob manager is null", KR(ret));
  } else if (OB_FAIL(lob_mgr->build_lob_read_context(read_ctx, ctx_->own_allocator_, ctx_->coll_type_,
                  0, UINT64_MAX, ctx_->timeout_, ctx_->src_loc_,
                  ctx_->tx_desc_, nullptr/*lob_access_ctx*/))) {
    LOG_WARN("failed to build source read context", KR(ret), KPC(this));
  } else if (read_ctx.byte_size_ > OB_MAX_LONGTEXT_LENGTH) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src lob byte size", KR(ret), K(read_ctx));
  } else if (FALSE_IT(read_ctx.offset_ = 0)) {
  } else if (FALSE_IT(read_ctx.len_ = static_cast<int64_t>(read_ctx.byte_size_))) {
  } else if (OB_FAIL(lob_mgr->query(read_ctx, query_iter))) {
    LOG_WARN("failed to open source lob iterator", KR(ret), K(read_ctx));
  } else {
    source_byte_len = read_ctx.byte_size_;
  }
  return ret;
}

int ObLobInserter::write_meta_summary_(
    const ObLobId &lob_id,
    const uint64_t total_byte_len,
    const uint64_t total_char_len,
    const uint64_t chunk_count)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo summary;
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString seq_id(sizeof(seq_id_buf), seq_id_buf);
  ObArenaAllocator &allocator = ctx_->own_allocator_;
  if (OB_FAIL(ObLobSeqId::get_summary_seq_id(seq_id))) {
    LOG_WARN("failed to build lob summary seq id", KR(ret), KPC(this));
  } else {
    summary.lob_col_idx_ = ctx_->store_column_idx_;
    summary.lob_id_ = lob_id;
    summary.seq_id_ = seq_id;
    summary.byte_len_ = total_byte_len;
    summary.char_len_ = total_char_len;

    ObLobMetaExtraInfo lob_extra_info;
    lob_extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
    lob_extra_info.chunk_count_ = chunk_count;
    lob_extra_info.lob_chunk_type_.set_in_table();
    lob_extra_info.last_write_type_ = ObLobLastWriteType::INSERT;
    if (OB_FAIL(lob_extra_info.serialize(allocator, summary.lob_extra_info_))) {
      LOG_WARN("failed to serialize lob_extra_info", KR(ret), K(lob_extra_info), KPC(this));
    } else if (OB_FAIL(meta_mgr_->insert(*ctx_, summary))) {
      LOG_WARN("failed to write meta summary row", KR(ret), K(summary), KPC(this));
    }
  }
  return ret;
}

void ObLobAppender::reset()
{
  is_inited_ = false;
  ctx_ = nullptr;
  meta_mgr_ = nullptr;
  piece_mgr_ = nullptr;
}

int ObLobAppender::init(
    ObLobAppendContext *ctx,
    ObLobMetaManager *meta_mgr,
    ObLobPieceManager *piece_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lob appender initialized twice", KR(ret));
  } else if (OB_ISNULL(ctx) || OB_ISNULL(meta_mgr) || OB_ISNULL(piece_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob appender argument", KR(ret), KPC(ctx), KPC(meta_mgr), KPC(piece_mgr));
  } else if (OB_FAIL(ctx->build_read_context(read_ctx_))) {
    LOG_WARN("failed to build append read context", KR(ret), KPC(ctx));
  } else {
    ctx_ = ctx;
    meta_mgr_ = meta_mgr;
    piece_mgr_ = piece_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObLobAppender::read_old_summary_(
    const ObLobId &lob_id,
    ObLobMetaInfo &summary,
    ObLobMetaInfo &last_meta,
    uint64_t &chunk_count)
{
  int ret = OB_SUCCESS;
  ObLobMetaExtraInfo extra_info;
  bool found_last_meta = false;
  chunk_count = 0;
  if (OB_FAIL(meta_mgr_->get_meta_summary_info(read_ctx_, summary))) {
    LOG_WARN("failed to read old lob summary", KR(ret), K(lob_id), KPC(ctx_));
  } else if (OB_UNLIKELY(summary.lob_id_ != lob_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old lob summary has unexpected lob id", KR(ret), K(lob_id), K(summary));
  } else if (OB_FAIL(extra_info.deserialize(summary.lob_extra_info_))) {
    LOG_WARN("failed to deserialize old lob summary", KR(ret), K(summary));
  } else if (OB_UNLIKELY(extra_info.extra_info_type_ != ObLobMetaExtraInfo::LOB_META_SUMMARY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid old lob summary type", KR(ret), K(extra_info), K(summary));
  } else if (FALSE_IT(chunk_count = extra_info.chunk_count_)) {
  } else if (OB_FAIL(meta_mgr_->get_last_meta_info(read_ctx_, last_meta, found_last_meta))) {
    LOG_WARN("failed to read last lob meta before append", KR(ret), K(lob_id));
  } else if (OB_UNLIKELY(!found_last_meta || 0 == chunk_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("out-row lob has no data meta before append",
             KR(ret), K(found_last_meta), K(chunk_count), K(summary));
  } else if (OB_UNLIKELY(last_meta.lob_id_ != lob_id
                         || last_meta.lob_col_idx_ != ctx_->store_column_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last lob meta does not match append target",
             KR(ret), K(lob_id), K(last_meta), KPC(ctx_));
  }
  return ret;
}

bool ObLobAppender::is_piece_consistent_with_meta_(
    const ObLobPieceInfo &piece,
    const ObLobMetaInfo &meta)
{
  const int64_t piece_byte_len = piece.lob_data_.length();
  return piece.lob_col_idx_ == meta.lob_col_idx_
      && piece.lob_id_ == meta.lob_id_
      && piece.seq_id_ == meta.seq_id_
      && piece_byte_len >= 0
      && static_cast<uint64_t>(piece_byte_len) == meta.byte_len_;
}

int ObLobAppender::try_fill_last_piece_(
    ObLobMetaInfo &last_meta,
    const ObString &append_data,
    uint64_t &consumed_byte_len,
    uint64_t &consumed_char_len)
{
  int ret = OB_SUCCESS;
  ObLobPieceInfo old_piece;
  ObLobPieceInfo new_piece;
  ObLobMetaInfo new_meta;
  bool found_piece = false;
  consumed_byte_len = 0;
  consumed_char_len = 0;
  const int64_t chunk_size = ctx_->get_chunk_size();
  if (OB_UNLIKELY(last_meta.byte_len_ > static_cast<uint64_t>(chunk_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last lob meta exceeds chunk size", KR(ret), K(last_meta), K(chunk_size));
  } else if (last_meta.byte_len_ == static_cast<uint64_t>(chunk_size)) {
    // 最后一个数据分片已写满，调用方会将全部追加数据作为新分片插入。
  } else if (OB_FAIL(piece_mgr_->get_piece_info(read_ctx_, last_meta.seq_id_,
                                                old_piece, found_piece))) {
    LOG_WARN("failed to read last lob piece", KR(ret), K(last_meta), KPC(ctx_));
  } else if (OB_UNLIKELY(!found_piece
                         || !is_piece_consistent_with_meta_(old_piece,
                                                            last_meta))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last lob piece does not match its meta",
             KR(ret), K(found_piece), K(old_piece), K(last_meta));
  } else {
    const int64_t tail_capacity = chunk_size - old_piece.lob_data_.length();
    const int64_t max_append_bytes = std::min(tail_capacity,
                                               static_cast<int64_t>(append_data.length()));
    int64_t append_char_len = 0;
    const uint64_t append_byte_len = ObCharset::max_bytes_charpos(
        ctx_->coll_type_, append_data.ptr(), append_data.length(),
        max_append_bytes, append_char_len);
    if (OB_UNLIKELY(append_byte_len > static_cast<uint64_t>(max_append_bytes)
                    || append_char_len < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid append prefix for last lob piece", KR(ret),
               K(append_byte_len), K(append_char_len), K(max_append_bytes));
    } else if (0 == append_byte_len) {
      // 剩余空间无法容纳下一个完整的多字节字符。
    } else {
      const int64_t combined_len = old_piece.lob_data_.length()
                                   + static_cast<int64_t>(append_byte_len);
      char *combined_buf = static_cast<char *>(ctx_->own_allocator_.alloc(combined_len));
      if (OB_ISNULL(combined_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate last lob piece buffer", KR(ret), K(combined_len));
      } else {
        MEMCPY(combined_buf, old_piece.lob_data_.ptr(), old_piece.lob_data_.length());
        MEMCPY(combined_buf + old_piece.lob_data_.length(),
               append_data.ptr(), append_byte_len);
        new_piece.lob_col_idx_ = old_piece.lob_col_idx_;
        new_piece.lob_id_ = old_piece.lob_id_;
        new_piece.seq_id_ = old_piece.seq_id_;
        new_piece.lob_data_.assign_ptr(combined_buf, combined_len);
        new_meta = last_meta;
        new_meta.byte_len_ = static_cast<uint64_t>(combined_len);
        new_meta.char_len_ += static_cast<uint64_t>(append_char_len);
        if (OB_FAIL(piece_mgr_->update(*ctx_, old_piece, new_piece))) {
          LOG_WARN("failed to update last lob piece", KR(ret), K(old_piece), K(new_piece));
        } else if (OB_FAIL(OB_E(
                       EventTable::ERRSIM_LOB_APPEND_AFTER_TAIL_PIECE_UPDATE) OB_SUCCESS)) {
          LOG_WARN("injected failure after updating last lob piece", KR(ret),
                   K(old_piece), K(new_piece));
        } else if (OB_FAIL(meta_mgr_->update(*ctx_, last_meta, new_meta))) {
          LOG_WARN("failed to update last lob meta", KR(ret), K(last_meta), K(new_meta));
        } else if (OB_FAIL(OB_E(
                       EventTable::ERRSIM_LOB_APPEND_AFTER_TAIL_META_UPDATE) OB_SUCCESS)) {
          LOG_WARN("injected failure after updating last lob meta", KR(ret),
                   K(last_meta), K(new_meta));
        } else {
          last_meta.byte_len_ = new_meta.byte_len_;
          last_meta.char_len_ = new_meta.char_len_;
          consumed_byte_len = append_byte_len;
          consumed_char_len = static_cast<uint64_t>(append_char_len);
        }
      }
    }
  }
  return ret;
}

int ObLobAppender::write_meta_summary_(
    const ObLobId &lob_id,
    const bool old_is_outrow,
    ObLobMetaInfo &old_summary,
    const uint64_t total_byte_len,
    const uint64_t total_char_len,
    const uint64_t chunk_count)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo new_summary;
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString seq_id(sizeof(seq_id_buf), seq_id_buf);
  ObLobMetaExtraInfo extra_info;
  if (OB_FAIL(ObLobSeqId::get_summary_seq_id(seq_id))) {
    LOG_WARN("failed to build lob summary seq id", KR(ret));
  } else {
    new_summary.lob_col_idx_ = ctx_->store_column_idx_;
    new_summary.lob_id_ = lob_id;
    new_summary.seq_id_ = seq_id;
    new_summary.byte_len_ = total_byte_len;
    new_summary.char_len_ = total_char_len;
    extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
    extra_info.chunk_count_ = chunk_count;
    extra_info.last_write_type_ = ObLobLastWriteType::APPEND;
    // 暂时只支持行内和行外两种类型，不支持外部存储类型。
    extra_info.lob_chunk_type_.set_in_table();
    if (OB_FAIL(extra_info.serialize(ctx_->own_allocator_, new_summary.lob_extra_info_))) {
      LOG_WARN("failed to serialize appended lob summary", KR(ret), K(extra_info));
    } else if (old_is_outrow
               && OB_FAIL(meta_mgr_->update(*ctx_, old_summary, new_summary))) {
      LOG_WARN("failed to update appended lob summary", KR(ret), K(old_summary), K(new_summary));
    } else if (!old_is_outrow
               && OB_FAIL(meta_mgr_->insert(*ctx_, new_summary))) {
      LOG_WARN("failed to insert appended lob summary", KR(ret), K(new_summary));
    }
  }
  return ret;
}

int ObLobAppender::parse_append_data_(
    ObString &append_data,
    uint64_t &append_char_len)
{
  int ret = OB_SUCCESS;
  const ObMemLobExternHeader *extern_header = nullptr;
  append_char_len = 0;
  if (OB_FAIL(ctx_->delta_loc_.get_append_delta(extern_header, append_data))) {
    LOG_WARN("failed to parse append delta", KR(ret), KPC(ctx_));
  } else if (OB_UNLIKELY(append_data.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("append delta is empty", KR(ret), KPC(ctx_));
  } else {
    const int64_t char_len = ObCharset::strlen_char(
        ctx_->coll_type_, append_data.ptr(), append_data.length());
    if (OB_UNLIKELY(char_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to calculate append character length",
               KR(ret), K(char_len), K(append_data.length()), KPC(ctx_));
    } else {
      append_char_len = static_cast<uint64_t>(char_len);
    }
  }
  return ret;
}

int ObLobAppender::calculate_total_length_(
    const uint64_t old_byte_len,
    const uint64_t old_char_len,
    const uint64_t append_byte_len,
    const uint64_t append_char_len,
    uint64_t &total_byte_len,
    uint64_t &total_char_len)
{
  int ret = OB_SUCCESS;
  total_byte_len = 0;
  total_char_len = 0;
  if (OB_UNLIKELY(old_byte_len > ObLobManager::LOB_WRITEAPPEND_MAX_BYTE_LEN
                  || append_byte_len
                         > ObLobManager::LOB_WRITEAPPEND_MAX_BYTE_LEN - old_byte_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("accumulated lob byte length exceeds LOB_WRITEAPPEND limit",
             KR(ret), K(old_byte_len), K(append_byte_len),
             "max_byte_len", ObLobManager::LOB_WRITEAPPEND_MAX_BYTE_LEN);
  } else {
    total_byte_len = old_byte_len + append_byte_len;
    total_char_len = old_char_len + append_char_len;
  }
  return ret;
}

int ObLobAppender::prepare_remaining_append_data_(
    const ObString &append_data,
    const uint64_t append_char_len,
    const uint64_t consumed_byte_len,
    const uint64_t consumed_char_len,
    ObString &remaining_append_data)
{
  int ret = OB_SUCCESS;
  remaining_append_data.reset();
  if (OB_UNLIKELY(consumed_byte_len
                      > static_cast<uint64_t>(append_data.length())
                  || consumed_char_len > append_char_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid consumed append prefix length", KR(ret),
             K(consumed_byte_len), K(consumed_char_len),
             K(append_data.length()), K(append_char_len));
  } else {
    const int64_t consumed_byte_offset =
        static_cast<int64_t>(consumed_byte_len);
    remaining_append_data.assign_ptr(
        append_data.ptr() + consumed_byte_offset,
        append_data.length() - consumed_byte_offset);
  }
  return ret;
}

int ObLobAppender::write_new_chunks_(
    const ObLobId &lob_id,
    const ObString &old_inrow_data,
    const ObString &remaining_append_data,
    const ObString &start_seq_id,
    ObLobWriteStats &write_stats)
{
  int ret = OB_SUCCESS;
  ObArray<ObLobMetaInfo> metas;
  ObLobAppendSourceIter source_iter;
  ObLobInTablePieceInsertIter piece_insert_iter;
  ObLobMetaTableBatchInsertIter meta_insert_iter;
  metas.set_attr(ObMemAttr(MTL_ID(), "LobAppendMetas"));
  if (remaining_append_data.empty()) {
    // 最后一个数据分片已容纳全部追加数据。
  } else if (OB_FAIL(source_iter.open(&ctx_->own_allocator_, old_inrow_data,
                                      remaining_append_data, ctx_->coll_type_))) {
    LOG_WARN("failed to open append source iterator", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(piece_insert_iter.init(&source_iter, &ctx_->own_allocator_,
                                            ctx_->get_chunk_size(),
                                            ctx_->store_column_idx_, lob_id,
                                            &metas, &write_stats))) {
    LOG_WARN("failed to initialize append piece iterator", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(piece_insert_iter.set_start_seq_id(start_seq_id))) {
    LOG_WARN("failed to set append start seq id", KR(ret), K(start_seq_id));
  } else if (OB_FAIL(piece_mgr_->batch_insert(*ctx_, piece_insert_iter))) {
    LOG_WARN("failed to insert appended lob pieces", KR(ret), KPC(ctx_));
  } else if (OB_UNLIKELY(!piece_insert_iter.is_end())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append piece iterator did not consume source",
             KR(ret), K(piece_insert_iter));
  } else if (OB_FAIL(OB_E(
                 EventTable::ERRSIM_LOB_APPEND_AFTER_PIECE_BATCH_INSERT) OB_SUCCESS)) {
    LOG_WARN("injected failure after inserting appended lob pieces",
             KR(ret), KPC(ctx_));
  } else if (OB_FAIL(meta_insert_iter.init(&metas))) {
    LOG_WARN("failed to initialize appended meta iterator", KR(ret));
  } else if (OB_FAIL(meta_mgr_->batch_insert(*ctx_, meta_insert_iter))) {
    LOG_WARN("failed to insert appended lob meta rows", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(OB_E(
                 EventTable::ERRSIM_LOB_APPEND_AFTER_META_BATCH_INSERT) OB_SUCCESS)) {
    LOG_WARN("injected failure after inserting appended lob metas",
             KR(ret), KPC(ctx_));
  } else if (OB_UNLIKELY(write_stats.chunk_count_
                         != static_cast<uint64_t>(metas.count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append meta count does not match chunk count",
             KR(ret), K(write_stats), K(metas.count()));
  }
  return ret;
}

int ObLobAppender::validate_write_stats_(
    const ObLobWriteStats &write_stats,
    const uint64_t expected_byte_len,
    const uint64_t expected_char_len,
    const uint64_t old_chunk_count,
    uint64_t &total_chunk_count)
{
  int ret = OB_SUCCESS;
  total_chunk_count = 0;
  if (OB_UNLIKELY(write_stats.byte_len_ != expected_byte_len
                  || write_stats.char_len_ != expected_char_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append write statistics mismatch", KR(ret), K(write_stats),
             K(expected_byte_len), K(expected_char_len));
  } else {
    total_chunk_count = old_chunk_count + write_stats.chunk_count_;
  }
  return ret;
}

int ObLobAppender::append()
{
  int ret = OB_SUCCESS;
  ObString append_data;
  ObString old_inrow_data;
  ObLobDiskLocatorWrapper dest_wrapper;
  ObLobId lob_id;
  ObLobMetaInfo old_summary;
  ObLobMetaInfo last_meta;
  uint64_t old_chunk_count = 0;
  const bool old_is_outrow = OB_NOT_NULL(ctx_) && ctx_->old_loc_.is_outrow_disk_lob_locator();
  uint64_t old_byte_len = 0;
  uint64_t old_char_len = 0;
  uint64_t append_char_len = 0;
  uint64_t total_byte_len = 0;
  uint64_t total_char_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob appender is not initialized", KR(ret));
  } else if (OB_FAIL(parse_append_data_(append_data, append_char_len))) {
    LOG_WARN("failed to prepare append data", KR(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dest_wrapper.init(ctx_->dest_loc_.ptr_, ctx_->dest_loc_.size_))) {
    LOG_WARN("failed to initialize append destination", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(dest_wrapper.get_lob_id(lob_id))) {
    LOG_WARN("failed to get append lob id", KR(ret), K(dest_wrapper));
  } else if (old_is_outrow) {
    if (OB_FAIL(read_old_summary_(lob_id, old_summary, last_meta, old_chunk_count))) {
      LOG_WARN("failed to read summary before append", KR(ret), K(lob_id));
    } else {
      old_byte_len = old_summary.byte_len_;
      old_char_len = old_summary.char_len_;
    }
  } else if (OB_FAIL(ctx_->old_loc_.get_inrow_data(old_inrow_data))) {
    LOG_WARN("failed to get old inrow data before append", KR(ret), KPC(ctx_));
  } else {
    old_byte_len = old_inrow_data.length();
    old_char_len = ObCharset::strlen_char(ctx_->coll_type_, old_inrow_data.ptr(), old_inrow_data.length());
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calculate_total_length_(
                 old_byte_len, old_char_len, static_cast<uint64_t>(append_data.length()),
                 append_char_len, total_byte_len, total_char_len))) {
    LOG_WARN("failed to calculate appended lob length",
             KR(ret), K(old_byte_len), K(old_char_len), K(append_data.length()),
             K(append_char_len));
  } else {
    ctx_->byte_size_ = total_byte_len;
  }

  ObLobWriteStats write_stats;
  // start_seq_buf 仅用于行内场景，行外场景直接通过 ObString 赋值。
  char start_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString start_seq_id;
  ObString remaining_append_data;
  uint64_t tail_consumed_byte_len = 0;
  uint64_t tail_consumed_char_len = 0;
  if (OB_FAIL(ret)) {
  } else {
    ctx_->dml_op_ = ObLobDmlOp::UPDATE; // 在 write_new_chunks_ 前切换为 ObLobDmlOp::INSERT
    ctx_->insert_lob_seq_no_ = dest_wrapper.get_seq_no();
    ctx_->update_lob_seq_no_ = dest_wrapper.get_seq_no();
    if (old_is_outrow) {
      start_seq_id = last_meta.seq_id_;
      if (OB_FAIL(try_fill_last_piece_(last_meta, append_data, tail_consumed_byte_len,
                                      tail_consumed_char_len))) {
        LOG_WARN("failed to fill last lob piece before append", KR(ret), K(last_meta));
      }
    } else {
      start_seq_id.assign_ptr(start_seq_buf, sizeof(start_seq_buf));
      if (OB_FAIL(ObLobSeqId::get_seq_id(0, start_seq_id))) {
        LOG_WARN("failed to build initial append seq id", KR(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(ctx_->dml_op_ = ObLobDmlOp::INSERT)) {
  } else if (OB_FAIL(prepare_remaining_append_data_(
                 append_data, append_char_len, tail_consumed_byte_len,
                 tail_consumed_char_len, remaining_append_data))) {
    LOG_WARN("failed to prepare remaining append data", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(write_new_chunks_(lob_id, old_inrow_data,
                                       remaining_append_data, start_seq_id, write_stats))) {
    LOG_WARN("failed to write new append chunks", KR(ret), K(lob_id), KPC(ctx_));
  }

  uint64_t total_chunk_count = 0;
  const uint64_t expected_insert_byte_len = old_is_outrow
      ? static_cast<uint64_t>(remaining_append_data.length())
      : static_cast<uint64_t>(old_inrow_data.length())
            + static_cast<uint64_t>(append_data.length());
  const uint64_t expected_insert_char_len = old_is_outrow
      ? append_char_len - tail_consumed_char_len : old_char_len + append_char_len;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(validate_write_stats_(
                 write_stats, expected_insert_byte_len, expected_insert_char_len,
                 old_chunk_count, total_chunk_count))) {
    LOG_WARN("failed to validate append write statistics",
             KR(ret), K(write_stats), K(old_chunk_count));
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(ctx_->dml_op_ = old_is_outrow     // 旧 LOB 为行外格式时，只需更新元信息摘要
                                          ? ObLobDmlOp::UPDATE
                                          : ObLobDmlOp::INSERT)) {
  } else if (OB_FAIL(write_meta_summary_(lob_id, old_is_outrow, old_summary,
                                         total_byte_len, total_char_len, total_chunk_count))) {
    LOG_WARN("failed to write appended lob summary", KR(ret), K(write_stats),
             K(total_byte_len), K(total_char_len), K(total_chunk_count));
  } else if (OB_FAIL(OB_E(
                 EventTable::ERRSIM_LOB_APPEND_AFTER_SUMMARY_UPDATE) OB_SUCCESS)) {
    LOG_WARN("injected failure after updating appended lob summary", KR(ret),
             K(total_byte_len), K(total_char_len), K(total_chunk_count), KPC(ctx_));
  }
  return ret;
}

//==================== ObLobDelete ====================
ObLobDeleter::ObLobDeleter()
  : is_inited_(false),
    ctx_(nullptr),
    meta_mgr_(nullptr),
    piece_mgr_(nullptr)
{}

void ObLobDeleter::reset()
{
  is_inited_ = false;
  ctx_ = nullptr;
  meta_mgr_ = nullptr;
  piece_mgr_ = nullptr;
}

int ObLobDeleter::init(
    ObLobDeleteContext *ctx,
    ObLobMetaManager *meta_mgr, 
    ObLobPieceManager *piece_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(ctx) || OB_ISNULL(meta_mgr) || OB_ISNULL(piece_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(ctx), KPC(meta_mgr), KPC(piece_mgr));
  } else {
    uint64_t byte_size = 0;
    ObLobManager *lob_mgr = MTL(ObLobManager*);
    ObLobReadContext read_ctx;
    if (OB_ISNULL(lob_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob manager is null", KR(ret));
    } else if (OB_FAIL(ctx->build_read_context(read_ctx))) {
      LOG_WARN("failed to build read context", K(ret), KPC(ctx));
    } else if (FALSE_IT(read_ctx.allocator_ = &ctx->own_allocator_)) {
    } else if (OB_FAIL(lob_mgr->get_byte_length(read_ctx, byte_size))) {
      LOG_WARN("failed to get byte length", KR(ret));
    } else {
      ctx->byte_size_ = static_cast<int64_t>(byte_size);
      ctx->lob_meta_tablet_id_ = read_ctx.lob_meta_tablet_id_;
      ctx->lob_piece_tablet_id_ = read_ctx.lob_piece_tablet_id_;
    }
  }
  
  if (OB_SUCC(ret)) {
    is_inited_ = true;
    ctx_ = ctx;
    meta_mgr_ = meta_mgr;
    piece_mgr_ = piece_mgr;
  }
  return ret;
}

int ObLobDeleter::del()
{
  int ret = OB_SUCCESS;
  ObLobReadContext read_ctx;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_FAIL(ctx_->build_read_context(read_ctx))) {
    LOG_WARN("failed to build read context", K(ret), KPC(ctx_));
  } else {
    read_ctx.allocator_ = &ctx_->own_allocator_;
    ObLobMetaTableIter *meta_iter = nullptr;
    ObLobPieceTableIter *piece_iter = nullptr;
    ObLobAuxTableBatchDeleteIterator<ObLobMetaInfo> meta_delete_iter;
    ObLobAuxTableBatchDeleteIterator<ObLobPieceInfo> piece_delete_iter;

    if (OB_FAIL(meta_mgr_->scan(read_ctx, meta_iter))) {
      LOG_WARN("failed to open meta iter", KR(ret), K(read_ctx));
    } else if (FALSE_IT(ctx_->lob_meta_tablet_id_ = read_ctx.lob_meta_tablet_id_)) {
    } else if (OB_FAIL(meta_delete_iter.init(ctx_, meta_iter))) {
      LOG_WARN("failed to init meta delete iter", KR(ret), KPC(ctx_));
    } else if (FALSE_IT(ctx_->dml_op_ = ObLobDmlOp::DELETE)) {
    } else if (OB_FAIL(meta_mgr_->batch_delete(*ctx_, meta_delete_iter))) {
      LOG_WARN("failed to batch delete meta row", KR(ret), KPC(ctx_));
    }

    if (FAILEDx(piece_mgr_->scan(read_ctx, piece_iter))) {
      LOG_WARN("failed to open piece iter", KR(ret), K(read_ctx));
    } else if (FALSE_IT(ctx_->lob_piece_tablet_id_ = read_ctx.lob_piece_tablet_id_)) {
    } else if (OB_FAIL(piece_delete_iter.init(ctx_, piece_iter))) {
      LOG_WARN("failed to init piece delete iter", KR(ret), KPC(ctx_));
    } else if (FALSE_IT(ctx_->dml_op_ = ObLobDmlOp::DELETE)) {
    } else if (OB_FAIL(piece_mgr_->batch_delete(*ctx_, piece_delete_iter))) {
      LOG_WARN("failed to batch delete piece row", KR(ret), KPC(ctx_));
    }

    if (OB_NOT_NULL(meta_iter)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(meta_mgr_->revert_scan_iter(meta_iter))) {
        LOG_WARN("failed to revert meta iter", KR(tmp_ret), KR(ret), KPC(ctx_));
      } else {
        meta_iter = nullptr;
      }
    }

    if (OB_NOT_NULL(piece_iter)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(piece_mgr_->revert_scan_iter(piece_iter))) {
        LOG_WARN("failed to revert piece iter", KR(tmp_ret), KR(ret), KPC(ctx_));
      } else {
        piece_iter = nullptr;
      }
    }
  }
  return ret;
}


} // storage
} // oceanbase
