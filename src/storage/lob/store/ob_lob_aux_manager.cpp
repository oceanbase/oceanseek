/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/ob_datum_row_iterator.h"
#include "ob_lob_aux_manager.h"
#include "storage/lob/common/ob_lob_util.h"
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

int ObLobMetaManager::batch_insert(ObLobInsertContext &insert_ctx, blocksstable::ObDatumRowIterator &iter)
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
  } else if (OB_FAIL(adaptor_->write_lob_meta(insert_ctx, iter))) {
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
int ObLobMetaManager::update(ObLobUpdateContext &update_ctx, ObLobMetaInfo &old_row, ObLobMetaInfo &new_row)
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

} // storage
} // oceanbase
