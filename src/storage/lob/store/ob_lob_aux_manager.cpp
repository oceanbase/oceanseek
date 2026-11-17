/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/worker.h"
#include "storage/blocksstable/ob_datum_row_iterator.h"
#include "ob_lob_aux_manager.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/lob/common/ob_lob_util.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_outtable_redo_payload.h"
#include "ob_lob_persistent_iterator.h"
#include "ob_lob_persistent_adaptor.h"

namespace oceanbase
{
namespace storage
{

//==================== ObLobMetaManager ====================
int ObLobMetaManager::init(ObPersistentLobAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobMetaManager init twice", KR(ret));
  } else if (OB_ISNULL(adaptor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor is nullptr", KR(ret));
  } else {
    adaptor_ = adaptor;
    is_inited_ = true;
  }
  return ret;
}

void ObLobMetaManager::reset()
{
  // adaptor_ is injected and owned by ObLobManager, do not destroy here
  adaptor_ = nullptr;
  is_inited_ = false;
}

int ObLobMetaManager::insert(ObLobInsertContext &insert_ctx, ObLobMetaInfo &in_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (FALSE_IT(ret = OB_E(EventTable::EN_LOB_META_DML_FAILED, insert_ctx.get_inrow_threshold()) OB_SUCCESS)) {
  } else if (OB_FAIL(ret) && insert_ctx.tablet_id_.is_user_tablet()) {
    LOG_ERROR("due to error injection, lob meta not being inserted into the auxiliary table", K(ret), K(insert_ctx));
    ret = OB_SUCCESS;
  } else if (insert_ctx.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote write not support", K(ret), K(insert_ctx));
  } else if (OB_FAIL(adaptor_->write_lob_meta(insert_ctx, in_row))) {
    LOG_WARN("write lob meta failed.", K(ret), K(insert_ctx));
  }
  return ret;
}

int ObLobMetaManager::batch_insert(
    ObLobInsertContext &insert_ctx,
    blocksstable::ObDatumRowIterator &iter,
    const ObLobOutTableRedoCtx *outtable_redo_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (FALSE_IT(ret = OB_E(EventTable::EN_LOB_META_DML_FAILED, insert_ctx.get_inrow_threshold()) OB_SUCCESS)) {
  } else if (OB_FAIL(ret) && insert_ctx.tablet_id_.is_user_tablet()) {
    LOG_ERROR("due to error injection, lob meta not being inserted from the auxiliary table",
        KR(ret), K(insert_ctx.tablet_id_));
    ret = OB_SUCCESS;
  } else if (insert_ctx.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote write not support", K(ret), K(insert_ctx));
  } else if (OB_FAIL(adaptor_->write_lob_meta(insert_ctx, iter, outtable_redo_ctx))) {
    LOG_WARN("batch write lob meta failed.", K(ret), K(insert_ctx));
  }
  return ret;
}

int ObLobMetaManager::batch_delete(ObLobDeleteContext &delete_ctx, blocksstable::ObDatumRowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (FALSE_IT(ret = OB_E(EventTable::EN_LOB_META_DML_FAILED, delete_ctx.get_inrow_threshold()) OB_SUCCESS)) {
  } else if (OB_FAIL(ret) && delete_ctx.tablet_id_.is_user_tablet()) {
    LOG_ERROR("due to error injection, lob meta not being deleted from the auxiliary table",
        KR(ret), K(delete_ctx.tablet_id_));
    ret = OB_SUCCESS;
  } else if (delete_ctx.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote delete not support", K(ret), K(delete_ctx));
  } else if (OB_FAIL(adaptor_->delete_lob_meta(delete_ctx, iter))) {
    LOG_WARN("batch write lob meta failed.", K(ret), K(delete_ctx));
  }
  return ret;
}



int ObLobMetaManager::scan(ObLobReadContext &read_ctx, ObLobMetaTableIter *&iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->scan_lob_meta(read_ctx, iter))) {
    LOG_WARN("scan lob meta failed.", K(ret), K(read_ctx));
  }
  return ret;
}

int ObLobMetaManager::revert_scan_iter(ObLobMetaTableIter *iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->revert_scan_iter(iter))) {
    LOG_WARN("revert scan iter failed.", K(ret));
  }
  return ret;
}

// erase specified range
int ObLobMetaManager::del(ObLobDeleteContext &delete_ctx, ObLobMetaInfo &in_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (FALSE_IT(ret = OB_E(EventTable::EN_LOB_META_DML_FAILED, delete_ctx.get_inrow_threshold()) OB_SUCCESS)) {
  } else if (OB_FAIL(ret) && delete_ctx.tablet_id_.is_user_tablet()) {
    LOG_ERROR("due to error injection, lob meta not being erased from the auxiliary table", K(ret), K(delete_ctx));
    ret = OB_SUCCESS;
  } else if (delete_ctx.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote delete not support", K(ret), K(delete_ctx));
  } else if (OB_FAIL(adaptor_->delete_lob_meta(delete_ctx, in_row))) {
    LOG_WARN("delete lob meta failed.", K(ret), K(delete_ctx));
  }
  return ret;
}

// update specified range
int ObLobMetaManager::update(
    ObLobUpdateContext &update_ctx, ObLobMetaInfo &old_row, ObLobMetaInfo &new_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (update_ctx.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote erase not support", K(ret), K(update_ctx));
  } else if (OB_FAIL(adaptor_->update_lob_meta(update_ctx, old_row, new_row))) {
    LOG_WARN("update lob meta failed.");
  }
  return ret;
}

int ObLobMetaManager::get_byte_length(ObLobReadContext &read_ctx, uint64_t &byte_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else {
    if (read_ctx.is_remote()) {
      ObLobRemoteQueryHandler handler;
      if (OB_FAIL(ObLobRemoteUtil::query(
          read_ctx,
          ObLobQueryArg::QueryType::GET_BYTE_LENGTH,
          read_ctx.addr_,
          handler))) {
        LOG_WARN("fail to init remote query ctx", K(ret), K(read_ctx));
      } else if (OB_FAIL(handler.get_byte_length(byte_length))) {
        LOG_WARN("failed to get byte length from remote query handler", KR(ret));
      }
    } else {
      ObLobMetaInfo meta_summary_info;
      if (OB_FAIL(adaptor_->get_meta_summary_info(read_ctx, meta_summary_info))) {
        LOG_WARN("failed to get meta summary info", KR(ret), K(read_ctx));
      } else {
        byte_length = meta_summary_info.byte_len_;
      }
    }
  }
  return ret;
}

int ObLobMetaManager::get_char_length(ObLobReadContext &read_ctx, uint64_t &char_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else {
    if (read_ctx.is_remote()) {
      ObLobRemoteQueryHandler handler;
      if (OB_FAIL(ObLobRemoteUtil::query(
          read_ctx,
          ObLobQueryArg::QueryType::GET_CHAR_LENGTH,
          read_ctx.addr_,
          handler))) {
        LOG_WARN("fail to init remote query ctx", K(ret), K(read_ctx));
      } else if (OB_FAIL(handler.get_char_length(char_length))) {
        LOG_WARN("failed to get char length from remote query handler", KR(ret));
      }
    } else {
      ObLobMetaInfo meta_summary_info;
      if (OB_FAIL(adaptor_->get_meta_summary_info(read_ctx, meta_summary_info))) {
        LOG_WARN("failed to get meta summary info", KR(ret), K(read_ctx));
      } else {
        char_length = meta_summary_info.char_len_;
      }
    }
  }
  return ret;
}

int ObLobMetaManager::get_meta_summary_info(
    ObLobReadContext &read_ctx,
    ObLobMetaInfo &summary)
{
  int ret = OB_SUCCESS;
  summary.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (read_ctx.is_remote()) {
    ObLobRemoteQueryHandler handler;
    if (OB_FAIL(ObLobRemoteUtil::query(
        read_ctx,
        ObLobQueryArg::QueryType::GET_LOB_SUMMARY_INFO,
        read_ctx.addr_,
        handler))) {
      LOG_WARN("initialize remote lob summary info query failed", KR(ret), K(read_ctx));
    } else if (OB_FAIL(handler.get_lob_summary_info(summary))) {
      LOG_WARN("failed to get lob summary info from remote query handler", KR(ret),
          K(read_ctx));
    }
  } else if (OB_FAIL(adaptor_->get_meta_summary_info(read_ctx, summary))) {
    LOG_WARN("failed to get lob meta summary info", KR(ret), K(read_ctx));
  }
  return ret;
}

int ObLobMetaManager::get_last_meta_info(
    ObLobReadContext &read_ctx,
    ObLobMetaInfo &last_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->get_last_meta_info(read_ctx, last_meta))) {
    LOG_WARN("failed to get last lob meta info", KR(ret), K(read_ctx));
  }
  return ret;
}


int ObLobMetaManager::get_table_param(const ObTableParam *&table_param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->get_aux_table_param(LobAuxTarget::META, table_param))) {
    LOG_WARN("get table param fail", K(ret));
  }
  return ret;
}

int ObLobMetaManager::get_table_dml_param(const ObTableDMLParam *&table_dml_param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->get_aux_table_dml_param(LobAuxTarget::META, table_dml_param))) {
    LOG_WARN("get table dml param fail", K(ret));
  }
  return ret;
}



int ObLobPieceManager::init(ObPersistentLobAdaptor *adaptor)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobPieceManager init twice", KR(ret));
  } else if (OB_ISNULL(adaptor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("adaptor is nullptr", KR(ret));
  } else {
    adaptor_ = adaptor;
    is_inited_ = true;
  }
  return ret;
}

void ObLobPieceManager::reset()
{
  // adaptor_ is injected and owned by ObLobManager, do not destroy here
  adaptor_ = nullptr;
  is_inited_ = false;
}

int ObLobPieceManager::batch_insert(ObLobInsertContext &insert_ctx, blocksstable::ObDatumRowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPiece Manager not inited", KR(ret));
  } else if (insert_ctx.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote write not support", K(ret), K(insert_ctx));
  } else if (OB_FAIL(adaptor_->write_lob_piece(insert_ctx, iter))) {
    LOG_WARN("batch write lob piece failed.", K(ret), K(insert_ctx));
  }
  return ret;
}

int ObLobPieceManager::batch_delete(ObLobDeleteContext &delete_ctx, blocksstable::ObDatumRowIterator &iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (delete_ctx.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote delete not supported", KR(ret), K(delete_ctx));
  } else if (OB_FAIL(adaptor_->delete_lob_piece(delete_ctx, iter))) {
    LOG_WARN("failed to batch delete lob piece", KR(ret), K(delete_ctx));
  }
  return ret;
}

int ObLobPieceManager::insert(ObLobInsertContext &insert_ctx, ObLobPieceInfo &in_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPieceManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->write_lob_piece(insert_ctx, in_row))) {
    LOG_WARN("write lob piece failed.", KR(ret));
  }
  return ret;
}

int ObLobPieceManager::del(ObLobDeleteContext &delete_ctx, ObLobPieceInfo &in_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPieceManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->delete_lob_piece(delete_ctx, in_row))) {
    LOG_WARN("delete lob piece failed.", KR(ret));
  }
  return ret;
}

int ObLobPieceManager::update(ObLobUpdateContext &update_ctx, ObLobPieceInfo &old_row, ObLobPieceInfo &new_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPieceManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->update_lob_piece(update_ctx, old_row, new_row))) {
    LOG_WARN("update lob piece failed.", KR(ret));
  }
  return ret;
}

int ObLobPieceManager::scan(ObLobReadContext &read_ctx, ObLobPieceTableIter *&iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPieceManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->scan_lob_piece(read_ctx, iter))) {
    LOG_WARN("scan lob piece failed.", KR(ret));
  }
  return ret;
}

int ObLobPieceManager::revert_scan_iter(ObLobPieceTableIter *iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPieceManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->revert_scan_iter(iter))) {
    LOG_WARN("revert scan iter failed.", KR(ret));
  }
  return ret;
}

int ObLobPieceManager::get_piece_info(
    ObLobReadContext &read_ctx,
    const common::ObString &seq_id,
    ObLobPieceInfo &piece_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPieceManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->get_lob_piece_info(read_ctx, seq_id, piece_info))) {
    LOG_WARN("failed to get lob piece info", KR(ret), K(seq_id), K(read_ctx));
  }
  return ret;
}

int ObLobPieceManager::get_table_param(const ObTableParam *&table_param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPieceManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->get_aux_table_param(LobAuxTarget::PIECE, table_param))) {
    LOG_WARN("get piece table param fail", KR(ret));
  }
  return ret;
}

int ObLobPieceManager::get_table_dml_param(const ObTableDMLParam *&table_dml_param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobPieceManager not inited", KR(ret));
  } else if (OB_FAIL(adaptor_->get_aux_table_dml_param(LobAuxTarget::PIECE, table_dml_param))) {
    LOG_WARN("get piece table dml param fail", KR(ret));
  }
  return ret;
}

int ObLobMetaManager::batch_insert_outtable(
    ObLobInsertContext &insert_ctx, ObLobQueryIter *iter,
    const ObIArray<ObLobMetaInfo> &metas, const bool is_logging)
{
  int ret = OB_SUCCESS;
  uint64_t lob_piece_table_id = OB_INVALID_ID;
  int64_t meta_idx = 0;
  ObArenaAllocator allocator(ObMemAttr(insert_ctx.tenant_id_, "LobMetaRedo"));
  ObSEArray<ObLobOutTableRedoPayload, REDO_BATCH_SIZE> payloads;
  ObSEArray<ObLobMetaInfo, REDO_BATCH_SIZE> batch_metas;
  ObLobOutTableRedoPayload payload;
  const ObLobOutTableRedoCtx redo_ctx(payloads);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if (metas.empty() || (is_logging && OB_ISNULL(iter))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable meta insert input", KR(ret), K(metas.count()), K(is_logging));
  } else if (OB_FAIL(ObLobUtil::get_lob_piece_table_id(insert_ctx, lob_piece_table_id))) {
    LOG_WARN("failed to get lob piece table id for meta redo", KR(ret));
  }
  while (OB_SUCC(ret) && meta_idx < metas.count()) {
    payloads.reuse();
    batch_metas.reuse();
    allocator.reuse();
    while (OB_SUCC(ret) && meta_idx < metas.count() && payloads.count() < REDO_BATCH_SIZE) {
      const ObLobMetaInfo &meta = metas.at(meta_idx);
      if (OB_FAIL(prepare_outtable_redo_payload_(
          insert_ctx, iter, meta, lob_piece_table_id, is_logging, allocator, payload))) {
        LOG_WARN("failed to prepare outtable meta redo", KR(ret), K(meta_idx));
      } else if (OB_FAIL(payloads.push_back(payload))) {
        LOG_WARN("failed to retain outtable meta redo payload", KR(ret), K(meta_idx));
      } else if (OB_FAIL(batch_metas.push_back(meta))) {
        LOG_WARN("failed to retain outtable meta row", KR(ret), K(meta_idx));
      } else {
        ++meta_idx;
      }
    }
    ObLobMetaTableBatchInsertIter meta_iter;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(insert_ctx.is_timeout())) {
      LOG_WARN("outtable meta insert timed out before writing", KR(ret));
    } else if (OB_FAIL(meta_iter.init(&batch_metas))) {
      LOG_WARN("failed to initialize outtable meta batch iterator", KR(ret));
    } else if (OB_FAIL(batch_insert(insert_ctx, meta_iter, &redo_ctx))) {
      LOG_WARN("failed to insert outtable meta batch with redo", KR(ret), K(meta_idx));
    }
  }
  return ret;
}

int ObLobMetaManager::prepare_outtable_redo_payload_(
    const ObLobWriteContext &ctx, ObLobQueryIter *iter,
    const ObLobMetaInfo &meta, const uint64_t lob_piece_table_id, const bool is_logging,
    ObIAllocator &allocator, ObLobOutTableRedoPayload &payload)
{
  int ret = OB_SUCCESS;
  ObLobMetaExtraInfo extra_info;
  ObLobChunk chunk;
  payload.reset();
  if (!ctx.insert_lob_seq_no_.is_valid() || ctx.timeout_ <= 0
      || 0 == lob_piece_table_id || OB_INVALID_ID == lob_piece_table_id
      || meta.lob_col_idx_ < 0 || !meta.lob_id_.is_valid()
      || meta.seq_id_.empty() || OB_ISNULL(meta.seq_id_.ptr())
      || meta.lob_extra_info_.empty() || OB_ISNULL(meta.lob_extra_info_.ptr())
      || meta.lob_block_id_.empty() || OB_ISNULL(meta.lob_block_id_.ptr())
      || 0 == meta.byte_len_ || meta.byte_len_ > ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE
      || (is_logging && OB_ISNULL(iter))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable meta redo input", KR(ret), K(meta), K(lob_piece_table_id));
  } else if (OB_FAIL(extra_info.deserialize(allocator, meta.lob_extra_info_))) {
    LOG_WARN("failed to decode outtable meta redo type", KR(ret), K(meta));
  } else if (ObLobMetaExtraInfo::LOB_DATA != extra_info.extra_info_type_
      || !extra_info.lob_chunk_type_.out_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("outtable redo requires a data meta row", KR(ret), K(extra_info));
  } else if (OB_FAIL(ObLobOutTableIO::deserialize_macro_id(
      meta.lob_block_id_, payload.macro_id_))) {
    LOG_WARN("failed to decode outtable meta redo macro id", KR(ret), K(meta));
  } else if (meta.lob_block_id_.length() != payload.macro_id_.get_serialize_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected outtable meta redo macro id length", KR(ret), K(meta));
  } else if (OB_FAIL(ctx.is_timeout())) {
    LOG_WARN("outtable meta redo preparation timed out", KR(ret));
  } else {
    payload.lob_col_idx_ = meta.lob_col_idx_;
    payload.lob_id_ = meta.lob_id_;
    payload.seq_id_ = meta.seq_id_;
    payload.lob_piece_table_id_ = lob_piece_table_id;
    payload.lob_operation_seq_no_ = ctx.insert_lob_seq_no_;
    payload.is_logging_ = is_logging;
    payload.byte_len_ = meta.byte_len_;
    if (is_logging) {
      if (OB_FAIL(iter->get_next_chunk(chunk, ctx.get_chunk_size(true /*is_outtable*/)))) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("failed to read outtable meta redo source", KR(ret), K(meta));
      } else if (meta.byte_len_ != chunk.byte_len_
          || meta.byte_len_ != static_cast<uint64_t>(chunk.lob_data_.length())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("outtable meta redo source length mismatch", KR(ret), K(meta), K(chunk));
      } else if (OB_FAIL(ob_write_string(allocator, chunk.lob_data_, payload.data_))) {
        LOG_WARN("failed to copy outtable meta redo source", KR(ret), K(meta));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!payload.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid prepared outtable meta redo payload", KR(ret), K(payload));
    }
  }
  if (OB_FAIL(ret)) {
    payload.reset();
  }
  return ret;
}

int ObLobMetaManager::update_outtable(
    ObLobUpdateContext &update_ctx, ObLobMetaInfo &old_row, ObLobMetaInfo &new_row,
    ObLobQueryIter *iter, const bool is_logging)
{
  int ret = OB_SUCCESS;
  uint64_t lob_piece_table_id = OB_INVALID_ID;
  ObArenaAllocator allocator(ObMemAttr(update_ctx.tenant_id_, "LobMetaRedo"));
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ObLobOutTableRedoPayload payload;
  const ObLobOutTableRedoCtx redo_ctx(payloads);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobMetaManager not inited", KR(ret));
  } else if ((is_logging && OB_ISNULL(iter)) || old_row.lob_col_idx_ != new_row.lob_col_idx_
      || old_row.lob_id_ != new_row.lob_id_ || old_row.seq_id_ != new_row.seq_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid outtable meta update input", KR(ret), K(old_row), K(new_row));
  } else if (update_ctx.is_remote()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("remote outtable meta update is not supported", KR(ret), K(update_ctx));
  } else if (OB_FAIL(ObLobUtil::get_lob_piece_table_id(update_ctx, lob_piece_table_id))) {
    LOG_WARN("failed to get lob piece table id for meta update redo", KR(ret));
  } else if (OB_FAIL(prepare_outtable_redo_payload_(
      update_ctx, iter, new_row, lob_piece_table_id, is_logging, allocator, payload))) {
    LOG_WARN("failed to prepare outtable meta update redo", KR(ret));
  } else if (OB_FAIL(payloads.push_back(payload))) {
    LOG_WARN("failed to retain outtable meta update redo", KR(ret));
  } else if (OB_FAIL(adaptor_->update_lob_meta(update_ctx, old_row, new_row, &redo_ctx))) {
    LOG_WARN("failed to update outtable meta row with redo", KR(ret));
  }
  return ret;
}

} // storage
} // oceanbase
