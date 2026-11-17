/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/allocator/page_arena.h"
#include "storage/lob/store/ob_lob_store_common.h"
#define USING_LOG_PREFIX STORAGE
#include "lib/ob_errno.h"
#include "lib/worker.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_tablet_autoincrement_service.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_persistent_appender.h"
#include "ob_lob_handler.h"
#include "storage/lob/store/ob_lob_persistent_iterator.h"
#include "storage/lob/access/ob_lob_retry.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "share/ob_cluster_version.h"
#include "share/ob_server_struct.h"
#include "storage/tx_storage/ob_access_service.h"

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
      if (OB_TMP_FAIL(ObLobRetryUtil::check_need_retry(read_ctx_, ret, retry_cnt, is_continue))) {
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
  } else if (OB_UNLIKELY(!read_ctx_.src_loc_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to check source lob locator", KR(ret), K(read_ctx_.tablet_id_),
             "locator_size", read_ctx_.src_loc_.size_);
  } else if (!read_ctx_.src_loc_.is_mem_external_lob()) {
    if (OB_FAIL(set_as_outrow_lob_iter_(result_))){
      LOG_WARN("failed when set_as_outrow_lob_iter_", K(ret), KPC(&read_ctx_));
    }
  } else if (OB_UNLIKELY(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "External LOB before cluster version 4.6.3.1");
  } else if (read_ctx_.src_loc_.is_mem_external_lob()){
    if (OB_FAIL(set_as_external_lob_iter_(result_))){
      LOG_WARN("failed when set_as_external_lob_iter_", K(ret), KPC(&read_ctx_));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    result_->reset();
    OB_DELETE(ObLobQueryIter, "unused", result_);
    result_ = nullptr;
  }
  return ret;
}

int ObLobQueryIterHandler::set_as_external_lob_iter_(ObLobQueryIter *&result)
{
  int ret = OB_SUCCESS;
  // Mem already carries the External descriptor; summary only provides the persisted identity.
  ObLobMetaInfo summary;
  ObExternalBlobInfo read_info;
  ObLobManager *lob_mgr = MTL(ObLobManager *);
  uint64_t lob_meta_tablet_id = 0;
  uint64_t external_lob_macro_seq_id = 0;
  share::ObBackupStorageInfo access_info;
  ObExternalLobQueryIter *external_iter = nullptr;
  if (OB_ISNULL(lob_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob manager is null for external lob query", KR(ret), K(read_ctx_.tenant_id_), K(read_ctx_.tablet_id_));
  } else if (OB_ISNULL(read_ctx_.access_ctx_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "reading external LOB without SQL access context");
    LOG_WARN("external lob access provider is unavailable", KR(ret), K(read_ctx_.tablet_id_),
                                                            KP(read_ctx_.access_ctx_));
  } else if (OB_FAIL(lob_mgr->get_lob_summary_info(read_ctx_, summary))) {
    LOG_WARN("failed to read lob summary", KR(ret), K(read_ctx_.tablet_id_), K(read_ctx_.store_column_idx_));
  } else if (OB_FAIL(lob_mgr->get_byte_length(read_ctx_, read_ctx_.byte_size_))) {
    LOG_WARN("failed to get external lob read length", KR(ret));
  } else if (OB_FAIL(read_ctx_.src_loc_.get_external_lob_info(read_info))) {
    LOG_WARN("failed to get external lob read descriptor from memory locator", KR(ret), K(read_ctx_.tablet_id_),
              "locator_size", read_ctx_.src_loc_.size_);
  } else if (OB_FAIL(read_ctx_.access_ctx_->get_external_access_info(read_info.url_,
                                                                     access_info))) {
    LOG_WARN("failed to resolve external lob access info", KR(ret), K(read_ctx_.tablet_id_),
             "url_length", read_info.url_.length());
  } else if (OB_FAIL(ObExternalLobMacroIdPrefix::deserialize(summary.lob_block_id_,
                                                             lob_meta_tablet_id,
                                                             external_lob_macro_seq_id))) {
    LOG_WARN("failed to deserialize external lob identity", KR(ret));
  } else if (OB_ISNULL(external_iter = OB_NEW(ObExternalLobQueryIter,
                                              ObMemAttr(MTL_ID(),"ExternalLobIter")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate external lob query iterator", KR(ret), K(lob_meta_tablet_id),
             K(external_lob_macro_seq_id), "alloc_size", sizeof(ObExternalLobQueryIter));
  } else if (OB_FAIL(external_iter->open(read_ctx_, read_info, lob_meta_tablet_id,
                                         external_lob_macro_seq_id, access_info))) {
    LOG_WARN("failed to open external lob iterator", KR(ret), K(lob_meta_tablet_id), K(external_lob_macro_seq_id));
  } else {
    // Transfer ownership to the caller only after open succeeds; free it here on failure.
    result = external_iter;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(external_iter)) {
    OB_DELETE(ObExternalLobQueryIter, "unused", external_iter);
  }
  return ret;
}

int ObLobQueryIterHandler::set_as_outrow_lob_iter_(ObLobQueryIter *&result)
{
  int ret = OB_SUCCESS;
  ObLobOutRowQueryIter *outrow_iter = nullptr;
  if (OB_ISNULL(outrow_iter = OB_NEW(
      ObLobOutRowQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc lob meta scan iterator fail", K(ret));
  } else if (OB_FAIL(outrow_iter->open(read_ctx_, lob_meta_mngr_))) {
    LOG_WARN("open lob query iter failed", K(ret), K(read_ctx_));
  } else {
    // Transfer ownership to the caller only after open succeeds; free it here on failure.
    result = outrow_iter;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(outrow_iter)) {
    OB_DELETE(ObLobOutRowQueryIter, "unused", outrow_iter);
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
    ObLobQueryIterHandler handler(read_ctx_);
    ObLobQueryIter *iter = nullptr;
    if (OB_FAIL(handler.init(lob_meta_mngr_))) {
      LOG_WARN("failed to initialize lob query iterator", KR(ret));
    } else if (OB_FAIL(handler.execute())) {
      LOG_WARN("failed to open lob query iterator", KR(ret), K(read_ctx_.tablet_id_), K(read_ctx_.len_));
    } else if (OB_ISNULL(iter = handler.result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob query iterator is null", KR(ret), K(read_ctx_.tablet_id_), K(read_ctx_.len_));
    } else {
      while (OB_SUCC(ret) && result_.remain() > 0) {
        if (OB_FAIL(iter->get_next_row(result_))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row", K(ret), K(read_ctx_));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(iter)) {
      iter->reset();
      OB_DELETE(ObLobQueryIter, "unused", iter);
      handler.result_ = nullptr;
    }
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
ObLobInserter::ObLobInserter()
  : is_inited_(false),
    ctx_(nullptr),
    meta_mgr_(nullptr),
    piece_mgr_(nullptr),
    lob_id_(),
    metas_(),
    source_byte_len_(0),
    is_logging_(true)
{
  metas_.set_attr(ObMemAttr(MTL_ID(), "LobMetas"));
  reset();
}

ObLobInserter::~ObLobInserter()
{
  reset();
}

void ObLobInserter::reset()
{
  is_logging_ = true;
  source_byte_len_ = 0;
  metas_.reset();
  lob_id_.reset();
  piece_mgr_ = nullptr;
  meta_mgr_ = nullptr;
  ctx_ = nullptr;
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
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(ctx), KPC(meta_mgr), KPC(piece_mgr));
  } else if (OB_FAIL(ObLobOutTableIO::enable_outtable_clog(ctx->tenant_id_, is_logging_))) {
    LOG_WARN("failed to check whether to write outtable lob clog", KR(ret), KPC(this));
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
  ObLobQueryIter *query_iter = nullptr;
  bool is_outtable = false;
  ObLobMetaExtraInfo source_extra_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(dest_wrapper.init(ctx_->dest_loc_.ptr_, ctx_->dest_loc_.size_))) {
    LOG_WARN("failed to init dest locator wrapper", KR(ret), KPC(this));
  } else if (OB_FAIL(dest_wrapper.get_lob_id(lob_id_))) {
    LOG_WARN("failed to get lob id from dest locator", KR(ret), KPC(this));
  } else if (OB_FAIL(get_source_lob_extra_info_(source_extra_info))) {
    LOG_WARN("failed to get source lob extra info", KR(ret));
  } else if (source_extra_info.is_external_lob()) {
    ctx_->dml_op_ = ObLobDmlOp::INSERT;
    ctx_->insert_lob_seq_no_ = dest_wrapper.get_seq_no();
    if (OB_FAIL(insert_external_lob_(lob_id_, source_extra_info.external_lob_info_))) {
      LOG_WARN("failed to insert external lob", KR(ret), K(lob_id_));
    }
  } else if (OB_FAIL(open_src_lob_query_(query_iter))) {
    LOG_WARN("failed to open source lob query", KR(ret), KPC(this));
  } else {
    ObLobMetaTableBatchInsertIter meta_insert_iter;
    ObLobWriteStats write_stats;
    metas_.reset();
    // 设置 lob 辅助表的 dml 操作符号，以及所用的 seq_no
    ctx_->dml_op_ = ObLobDmlOp::INSERT;
    ctx_->insert_lob_seq_no_ = dest_wrapper.get_seq_no();
    if (OB_FAIL(ObLobUtil::check_write_outtable(*ctx_, source_byte_len_, is_outtable))) {
      LOG_WARN("failed to check whether to write outtable lob", KR(ret), K(source_byte_len_));
    }
#ifdef ERRSIM
    lib::ErrsimModuleGuard errsim_guard(is_outtable
        ? ObErrsimModuleType::ERRSIM_MODULE_OUTTABLE_LOB : lib::Worker::get_module_type().type_);
#endif
    if (OB_FAIL(ret)) {
    } else if (is_outtable) {
      if (OB_FAIL(insert_outtable_piece_(query_iter, write_stats))) {
        LOG_WARN("failed to insert outtable lob pieces", KR(ret), KPC(this));
      }
    } else {
      if (OB_FAIL(insert_outrow_piece_(query_iter, write_stats))) {
        LOG_WARN("failed to insert outrow lob pieces", KR(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret) && OB_UNLIKELY(write_stats.byte_len_ != source_byte_len_
        || write_stats.chunk_count_ != static_cast<uint64_t>(metas_.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob stream statistics do not match source", KR(ret), K(source_byte_len_),
               K(write_stats), "meta_count", metas_.count(), K(is_outtable), KPC(this));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(write_meta_summary_(
          lob_id_, write_stats.byte_len_, write_stats.char_len_,
          write_stats.chunk_count_, write_stats.compressed_len_, is_outtable))) {
        LOG_WARN("failed to write meta summary row", KR(ret), K(lob_id_),
            K(write_stats), KPC(this));
      } else if (is_outtable) {
        if (OB_FAIL(insert_outtable_meta_())) {
          LOG_WARN("failed to insert outtable lob meta with redo payloads", KR(ret), KPC(this));
        }
      } else if (OB_FAIL(meta_insert_iter.init(&metas_))) {
        LOG_WARN("failed to initialize meta insert iterator", KR(ret), KPC(this));
      } else if (OB_FAIL(meta_mgr_->batch_insert(*ctx_, meta_insert_iter))) {
        LOG_WARN("failed to batch insert meta info", KR(ret), KPC(this));
      }
    }
  }
  if (OB_NOT_NULL(query_iter)) {
    OB_DELETE(ObLobQueryIter, "unused", query_iter);
    query_iter = nullptr;
  }
  return ret;
}

int ObLobInserter::insert_outrow_piece_(
    ObLobQueryIter *input_iter,
    ObLobWriteStats &write_stats)
{
  int ret = OB_SUCCESS;
  ObLobInTablePieceInsertIter piece_insert_iter;
  if (OB_FAIL(piece_insert_iter.init(input_iter, &ctx_->own_allocator_, ctx_->get_chunk_size(false/*is_outtable*/),
              ctx_->store_column_idx_, lob_id_, &metas_, &write_stats))) {
    LOG_WARN("failed to initialize piece insert iterator", KR(ret), KPC(this));
  } else if (OB_FAIL(piece_mgr_->batch_insert(*ctx_, piece_insert_iter))) {
    LOG_WARN("failed to batch insert piece info", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!piece_insert_iter.is_end())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("piece insert did not consume source iterator", KR(ret),
             K(piece_insert_iter), KPC(this));
  }
  return ret;
}

int ObLobInserter::insert_outtable_piece_(
    ObLobQueryIter *input_iter,
    ObLobWriteStats &write_stats)
{
  int ret = OB_SUCCESS;
  ObLobOutTableInsertIter outtable_write_iter;
  const int64_t chunk_size = ctx_->get_chunk_size(true/*is_outtable*/);
  int64_t mbmaxlen = 0;
  int64_t chunk_count = 0;

  // There may be not enough space in the chunk to store the last character,
  // so we need to calculate the chunk count based on the worst case
  if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(ctx_->coll_type_, mbmaxlen))) {
    LOG_WARN("failed to get max character length", KR(ret), K(ctx_->coll_type_));
  } else {
    const uint64_t min_chunk_size = static_cast<uint64_t>(chunk_size - mbmaxlen + 1);
    chunk_count = (source_byte_len_ + min_chunk_size - 1) / min_chunk_size;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLobUtil::refresh_lob_tablet_id(*ctx_))) {
    LOG_WARN("failed to prepare lob tablet id for outtable insert", KR(ret), KPC(ctx_));

  } else if (OB_FAIL(outtable_write_iter.init(ctx_, input_iter, &ctx_->own_allocator_, chunk_size, chunk_count,
                     ctx_->store_column_idx_, lob_id_, &metas_, &write_stats))) {
    LOG_WARN("failed to initialize outtable insert iterator", KR(ret), KPC(this));
  } else if (OB_FAIL(outtable_write_iter.write_all())) {
    LOG_WARN("failed to write outtable lob objects", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!outtable_write_iter.is_end())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outtable insert did not consume source iterator", KR(ret), K(outtable_write_iter), KPC(this));
  }
  return ret;
}

int ObLobInserter::open_src_lob_query_(ObLobQueryIter *&query_iter)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = MTL(ObLobManager*);
  ObLobReadContext read_ctx;
  
  query_iter = nullptr;
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
    source_byte_len_ = read_ctx.byte_size_;
  }
  return ret;
}

int ObLobInserter::write_meta_summary_(
    const ObLobId &lob_id,
    const uint64_t total_byte_len,
    const uint64_t total_char_len,
    const uint64_t chunk_count,
    const uint64_t compressed_len,
    const bool is_outtable)
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
    if (is_outtable) {
      lob_extra_info.lob_chunk_type_.set_out_table();
      lob_extra_info.compressed_len_ = compressed_len;
    } else {
      lob_extra_info.lob_chunk_type_.set_in_table();
    }
    lob_extra_info.last_write_type_ = ObLobLastWriteType::INSERT;
    if (OB_FAIL(lob_extra_info.serialize(allocator, summary.lob_extra_info_))) {
      LOG_WARN("failed to serialize lob_extra_info", KR(ret), K(lob_extra_info), KPC(this));
    } else if (OB_FAIL(meta_mgr_->insert(*ctx_, summary))) {
      LOG_WARN("failed to write meta summary row", KR(ret), K(summary), KPC(this));
    }
  }
  return ret;
}

int ObLobInserter::get_source_lob_extra_info_(ObLobMetaExtraInfo &extra_info)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 &source = ctx_->src_loc_;
  extra_info.reset();
  if (0 == source.size_) {
    // Empty LOBs have no source summary.
  } else if (OB_UNLIKELY(!source.is_valid() || source.is_outline_lob_disk_locator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid source lob locator", KR(ret), K(source.size_), K(ctx_->tablet_id_));
  } else if (source.has_inrow_data()) {
    // Inrow data uses the existing Internal write path.
  } else if (source.is_external_temp_lob()) {
    ObExternalBlobInfo source_info;
    // Temporary External has no summary; read its reference descriptor from Mem.
    if (OB_FAIL(source.get_external_lob_info(source_info))) {
      LOG_WARN("failed to get temporary external lob info", KR(ret), K(source.size_), K(ctx_->tablet_id_));
    } else if (OB_FAIL(extra_info.set_external_lob_info(ctx_->own_allocator_, source_info))) {
      LOG_WARN("failed to copy temporary external lob info", KR(ret));
    }
  } else if (!source.is_persist_lob()) {
    // Ordinary temporary LOBs use the existing read path without querying persisted summary.
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_1) {
    // Ordinary LOB copies in mixed versions use the old read path with no extra summary query.
    if (OB_UNLIKELY(source.is_mem_external_lob())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "External LOB before cluster version 4.6.3.1");
    }
  } else {
    ObLobReadContext read_ctx;
    ObLobManager *lob_mgr = MTL(ObLobManager*);
    // Persisted copies use summary across tablets/nodes without reading remote objects.
    if (OB_ISNULL(lob_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob manager is null", KR(ret), K(ctx_->tenant_id_), K(ctx_->tablet_id_));
    } else if (OB_FAIL(read_ctx.build(source, ctx_->own_allocator_, 0, UINT64_MAX,
                                      ctx_->tx_desc_))) {
      LOG_WARN("failed to build source summary context", KR(ret));
    } else if (OB_FAIL(lob_mgr->get_meta_summary_extra_info(read_ctx, extra_info))) {
      LOG_WARN("failed to read source lob summary", KR(ret), K(read_ctx.tablet_id_), K(read_ctx.store_column_idx_));
    }
  }
  return ret;
}

int ObLobInserter::insert_external_lob_(const ObLobId &lob_id, const ObExternalBlobInfo &source_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObExternalBlobKind::MAX <= source_info.kind_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert an UNDEFINED kind external LOB");
    LOG_WARN("unsupported external lob kind for insert", KR(ret), K(lob_id), K(source_info.kind_));
  } else if (OB_FAIL(write_external_lob_meta_summary_(lob_id, source_info))) {
    LOG_WARN("failed to write external lob summary", KR(ret), K(lob_id));
  }
  return ret;
}

int ObLobInserter::write_external_lob_meta_summary_(
    const ObLobId &lob_id, const ObExternalBlobInfo &external_info)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo summary;
  ObLobMetaExtraInfo lob_extra_info;
  // seq_id is only used to write summary, so it is always 0 and the buffer is large enough.
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString seq_id(sizeof(seq_id_buf), seq_id_buf);
  uint64_t external_lob_macro_seq_id = 0;

  ObArenaAllocator &allocator = ctx_->own_allocator_;
  // summary stores logical length (declared for External); binary chars equal bytes.
  // Store undeclared length as 0; extra_info.has_length indicates whether length was declared.
  summary.byte_len_ = external_info.length_;
  summary.char_len_ = external_info.length_;
  lob_extra_info.chunk_count_ = 0;

  // Each insert gets a new identity, including persisted reference copies; never reuse the source.
  // TODO by renhe: fetch_tablet_inc_id may alloc tablet_inc_id from splited new tablet_id,
  //                instead of insert_ctx.lob_meta_tablet_id_.id()
  if (OB_UNLIKELY(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "writing External LOB before cluster version 4.6.3.1");
  } else if (OB_FAIL(lob_extra_info.set_external_lob_info(allocator, external_info))) {
    LOG_WARN("failed to set external lob summary", KR(ret), K(lob_id));
  } else if (OB_FAIL(ObLobUtil::fetch_tablet_inc_id(*ctx_, external_lob_macro_seq_id))) {
    LOG_WARN("failed to allocate external lob sequence", KR(ret), K(ctx_->lob_meta_tablet_id_), K(lob_id));
  } else if (OB_FAIL(ObExternalLobMacroIdPrefix::serialize(allocator,
                                                           ctx_->lob_meta_tablet_id_.id(),
                                                           external_lob_macro_seq_id,
                                                           summary.lob_block_id_))) {
    LOG_WARN("failed to serialize external lob identity", KR(ret),
             K(ctx_->lob_meta_tablet_id_), K(external_lob_macro_seq_id), K(lob_id));
  } else if (OB_FAIL(ObLobSeqId::get_seq_id(0, seq_id))) {
    LOG_WARN("failed to build seq_id 0", KR(ret), K(lob_id), K(ctx_->tablet_id_));
  } else {
    summary.lob_col_idx_ = ctx_->store_column_idx_;
    summary.lob_id_ = lob_id;
    summary.seq_id_ = seq_id;
    lob_extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
    lob_extra_info.last_write_type_ = ObLobLastWriteType::INSERT;
    if (OB_FAIL(lob_extra_info.serialize(allocator, summary.lob_extra_info_))) {
      LOG_WARN("failed to serialize external lob extra info", KR(ret), K(lob_id), K(external_lob_macro_seq_id));
    } else if (OB_FAIL(meta_mgr_->insert(*ctx_, summary))) {
      LOG_WARN("failed to write external lob summary row", KR(ret), K(lob_id), K(ctx_->tablet_id_),
                                                           K(external_lob_macro_seq_id));
    }
  }
  return ret;
}

//==================== ObLobAppender ====================
ObLobAppender::ObLobAppender()
  : is_inited_(false),
    ctx_(nullptr),
    meta_mgr_(nullptr),
    piece_mgr_(nullptr)
{
  reset();
}

ObLobAppender::~ObLobAppender()
{
  reset();
}

void ObLobAppender::reset()
{
  piece_mgr_ = nullptr;
  meta_mgr_ = nullptr;
  ctx_ = nullptr;
  is_inited_ = false;
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
  } else {
    ctx_ = ctx;
    meta_mgr_ = meta_mgr;
    piece_mgr_ = piece_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObLobAppender::set_next_seq_id_(
    const bool old_is_inrow,
    const ObLobMetaInfo &last_meta)
{
  int ret = OB_SUCCESS;
  ObString next_seq_id;
  char first_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString first_seq_id(sizeof(first_seq_buf), first_seq_buf);
  ObArenaAllocator seq_allocator("LobAppendSeq", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (old_is_inrow) {
    if (OB_FAIL(ObLobSeqId::get_seq_id(1, first_seq_id))) {
      LOG_WARN("failed to build first lob data seq id", KR(ret));
    } else {
      next_seq_id = first_seq_id;
    }
  } else if (ctx_->update_first_) {
    next_seq_id = last_meta.seq_id_;
  } else {
    ObLobSeqIdGenerator seq_id_gen(last_meta.seq_id_, &seq_allocator);
    if (OB_FAIL(seq_id_gen.get_next_seq_id(next_seq_id))) {
      LOG_WARN("failed to build next lob data seq id", KR(ret), K(last_meta));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ob_write_string(ctx_->own_allocator_, next_seq_id, ctx_->next_seq_id_))) {
    LOG_WARN("failed to copy next lob data seq id", KR(ret), K(next_seq_id));
  }
  return ret;
}

int ObLobAppender::prepare_append_context_(
    ObLobMetaInfo &summary_info,
    ObLobMetaInfo &last_meta_info,
    bool &is_outtable)
{
  int ret = OB_SUCCESS;
  const bool old_is_inrow = ctx_->old_loc_.is_inrow();
  uint64_t old_byte_len = 0;
  uint64_t old_char_len = 0;
  uint64_t old_chunk_count = 0;
  ObLobMetaExtraInfo summary_extra_info;
  ObLobReadContext read_ctx;
  ObString append_data;
  ObString old_inrow_data;
  uint64_t append_byte_len = 0;
  uint64_t append_char_len = 0;
  summary_info.reset();
  last_meta_info.reset();
  is_outtable = false;
  ctx_->update_first_ = false;

  if (OB_FAIL(parse_append_data_(append_data))) {
    LOG_WARN("failed to parse append data", KR(ret), KPC(this));
  } else {
    // Calculate the post-append length and determine the storage type.
    if (old_is_inrow) {
      if (OB_FAIL(ctx_->old_loc_.get_inrow_data(old_inrow_data))) {
        LOG_WARN("failed to get inrow data", KR(ret), KPC(this));
      } else {
        old_byte_len = old_inrow_data.length();
        old_char_len = ObCharset::strlen_char(ctx_->coll_type_, old_inrow_data.ptr(), old_inrow_data.length());
      }
    } else {
      if (OB_FAIL(ctx_->build_read_context(read_ctx))) {
        LOG_WARN("failed to build append read context", KR(ret), KPC(this));
      } else if (FALSE_IT(read_ctx.allocator_ = &ctx_->own_allocator_)) {
      } else if (OB_FAIL(meta_mgr_->get_meta_summary_info(read_ctx, summary_info))) {
        LOG_WARN("failed to read old lob summary", KR(ret), KPC(this));
      } else if (OB_FAIL(summary_extra_info.deserialize(ctx_->own_allocator_,
                                                        summary_info.lob_extra_info_))) {
        LOG_WARN("failed to deserialize old lob summary", KR(ret), KPC(this), K(summary_info));
      } else if (OB_UNLIKELY(summary_extra_info.is_external_lob())) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "appending an external LOB");
        LOG_WARN("appending an external LOB is not supported", KR(ret), KPC(this), K(summary_info));
      } else if (OB_FAIL(meta_mgr_->get_last_meta_info(read_ctx, last_meta_info))) {
        LOG_WARN("failed to read last lob meta before append", KR(ret), KPC(this));
      } else if (OB_UNLIKELY(summary_extra_info.extra_info_type_ != ObLobMetaExtraInfo::LOB_META_SUMMARY
                             || 0 == summary_extra_info.chunk_count_
                             || summary_info.byte_len_ < last_meta_info.byte_len_
                             || summary_info.char_len_ < last_meta_info.char_len_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob meta before append", KR(ret), K(summary_info), K(summary_extra_info),
                 K(last_meta_info));
      } else {
        old_byte_len = summary_info.byte_len_;
        old_char_len = summary_info.char_len_;
        old_chunk_count = summary_extra_info.chunk_count_;
        is_outtable = summary_extra_info.lob_chunk_type_.out_table();
      }
    }
  }

  // Ensure the post-append length does not exceed the limit.
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(append_byte_len = append_data.length())) {
  } else if (FALSE_IT(append_char_len = ObCharset::strlen_char(ctx_->coll_type_, append_data.ptr(), append_data.length()))) {
  } else if (OB_UNLIKELY(append_byte_len > ObLobManager::LOB_WRITEAPPEND_MAX_BYTE_LEN
             || old_byte_len > ObLobManager::LOB_WRITEAPPEND_MAX_BYTE_LEN - append_byte_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("accumulated lob byte length exceeds LOB_WRITEAPPEND limit", KR(ret), K(old_byte_len), K(append_byte_len));
  } else if (OB_UNLIKELY(append_char_len == 0 || append_byte_len == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append data is empty", KR(ret), K(append_byte_len), K(append_char_len));
  } else {
    ctx_->total_byte_length_ = old_byte_len + append_byte_len;
    ctx_->total_char_length_ = old_char_len + append_char_len;
  }


  if (OB_FAIL(ret)) {
  } else if (old_is_inrow) {
    if (OB_FAIL(ObLobUtil::check_write_outtable(*ctx_, ctx_->total_byte_length_, is_outtable))) {
      LOG_WARN("failed to check whether append writes outtable lob", KR(ret), K(ctx_->total_byte_length_));
    }
  } else {
    // Append to the tail chunk first when it has enough room for at least one character.
    const int64_t chunk_size = ctx_->get_chunk_size(is_outtable);
    if (last_meta_info.byte_len_ < static_cast<uint64_t>(chunk_size)) {
      const int64_t remain_size = chunk_size - static_cast<int64_t>(last_meta_info.byte_len_);
      const int64_t max_append_size = OB_MIN(remain_size, append_data.length());
      int64_t append_prefix_char_len = 0;
      const uint64_t append_prefix_byte_len = ObCharset::max_bytes_charpos(
          ctx_->coll_type_, append_data.ptr(), append_data.length(),
          max_append_size, append_prefix_char_len);
      ctx_->update_first_ = append_prefix_byte_len > 0;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!ctx_->old_loc_.is_inrow()) {
    ctx_->cur_byte_length_ = old_byte_len;
    ctx_->cur_char_length_ = old_char_len;
    ctx_->cur_chunk_count_ = old_chunk_count;

    if (ctx_->update_first_) {
      ctx_->cur_byte_length_ -= last_meta_info.byte_len_;
      ctx_->cur_char_length_ -= last_meta_info.char_len_;
      ctx_->cur_chunk_count_ --;
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(set_next_seq_id_(old_is_inrow, last_meta_info))) {
    LOG_WARN("failed to prepare next lob data seq id", KR(ret), K(last_meta_info));
  }
  return ret;
}

int ObLobAppender::parse_append_data_(ObString &append_data)
{
  int ret = OB_SUCCESS;
  const ObMemLobExternHeader *extern_header = nullptr;
  if (OB_FAIL(ctx_->delta_loc_.get_append_delta(extern_header, append_data))) {
    LOG_WARN("failed to parse append delta", KR(ret), KPC(ctx_));
  } else if (OB_UNLIKELY(append_data.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("append delta is empty", KR(ret), KPC(ctx_));
  }
  return ret;
}

int ObLobAppender::append()
{
  int ret = OB_SUCCESS;
  ObLobDiskLocatorWrapper dest_wrapper;
  ObLobMetaInfo summary_info;
  ObLobMetaInfo last_meta_info;
  bool is_outtable = false;
  ObLobInTablePersistentAppender intable_appender;
  ObLobOutTablePersistentAppender outtable_appender;
  ObLobPersistentAppender *persistent_appender = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob appender is not initialized", KR(ret), KPC(this));
  } else if (OB_FAIL(dest_wrapper.init(ctx_->dest_loc_.ptr_, ctx_->dest_loc_.size_))) {
    LOG_WARN("failed to initialize append destination", KR(ret), KPC(ctx_));
  } else if (OB_FAIL(dest_wrapper.get_lob_id(ctx_->lob_id_))) {
    LOG_WARN("failed to get append lob id", KR(ret), K(dest_wrapper));
  } else if (FALSE_IT(ctx_->insert_lob_seq_no_ = dest_wrapper.get_seq_no())) {
  } else if (FALSE_IT(ctx_->update_lob_seq_no_ = dest_wrapper.get_seq_no())) {
  } else if (OB_FAIL(prepare_append_context_(
                 summary_info, last_meta_info, is_outtable))) {
    LOG_WARN("failed to prepare lob append context", KR(ret), KPC(this));
  } else if (is_outtable) {
    persistent_appender = &outtable_appender;
  } else {
    persistent_appender = &intable_appender;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(persistent_appender->init(ctx_, meta_mgr_, piece_mgr_, summary_info, last_meta_info))) {
    LOG_WARN("failed to initialize persistent lob appender",
             KR(ret), K(is_outtable), KPC(ctx_));
  } else if (OB_FAIL(persistent_appender->append())) {
    LOG_WARN("failed to persist lob append", KR(ret), K(is_outtable), KPC(ctx_));
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


int ObLobInserter::insert_outtable_meta_()
{
  int ret = OB_SUCCESS;
  ObLobQueryIter *query_iter = nullptr;
  if (is_logging_) {
    if (OB_FAIL(open_src_lob_query_(query_iter))) {
      LOG_WARN("failed to open source lob query", KR(ret), KPC(this));
    }
  }

  if (FAILEDx(meta_mgr_->batch_insert_outtable(*ctx_, query_iter, metas_, is_logging_))) {
    LOG_WARN("failed to insert outtable lob meta with redo payloads", KR(ret), K(lob_id_));
  }
  if (OB_SUCC(ret) && is_logging_) {
    ObLobChunk extra_chunk;
    if (OB_ITER_END == (ret = query_iter->get_next_chunk(
        extra_chunk, ctx_->get_chunk_size(true)))) {
      ret = OB_SUCCESS;
    } else if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("outtable lob redo source has unconsumed data", KR(ret), K(lob_id_));
    } else {
      LOG_WARN("failed to finish reading outtable lob redo source", KR(ret), K(lob_id_));
    }
  }

  if (OB_NOT_NULL(query_iter)) {
    query_iter->reset();
    OB_DELETE(ObLobQueryIter, "unused", query_iter);
    query_iter = nullptr;
  }
  return ret;
}

} // storage
} // oceanbase
