/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/ob_errno.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/common/ob_lob_util.h"

#include "ob_lob_persistent_adaptor.h"
#include "ob_lob_meta.h"
#include "ob_lob_persistent_iterator.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "ob_lob_persistent_reader_cache.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{

ObPersistentLobAdaptor::~ObPersistentLobAdaptor()
{
  destroy();
}

void ObPersistentLobAdaptor::destroy()
{
  STORAGE_LOG(INFO, "[LOB] destroy lob persist", K(tenant_id_));
  free_table_param(meta_table_param_);
  free_table_param(meta_table_dml_param_);
  free_table_param(piece_table_param_);
  free_table_param(piece_table_dml_param_);
  table_param_inited_ = false;
  allocator_.reset();
}

int ObPersistentLobAdaptor::init_meta_column_ids(ObSEArray<uint64_t, 7> &meta_column_ids)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; OB_SUCC(ret) && i < ObLobMetaUtil::LOB_META_COLUMN_CNT; i++) {
    if (OB_FAIL(meta_column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
      LOG_WARN("push meta col id fail", KR(ret), K(i));
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::init_piece_column_ids(ObSEArray<uint64_t, 4> &piece_column_ids)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; OB_SUCC(ret) && i < ObLobPieceUtil::LOB_PIECE_COLUMN_CNT; i++) {
    if (OB_FAIL(piece_column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
      LOG_WARN("push piece col id fail", KR(ret), K(i));
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::try_init_table_param()
{
  int ret = OB_SUCCESS;
  if (!ATOMIC_LOAD(&table_param_inited_)) {
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!ATOMIC_LOAD(&table_param_inited_)) {
      if (OB_FAIL(init_table_param())) {
        LOG_ERROR("init_table_param fail", KR(ret));
      }
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::get_aux_table_param(
    const LobAuxTarget target,
    const ObTableParam *&table_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (OB_FAIL(try_init_table_param())) {
    LOG_WARN("try_init_table_param fail", KR(ret));
  } else if (LobAuxTarget::META == target) {
    table_param = meta_table_param_;
  } else if (LobAuxTarget::PIECE == target) {
    table_param = piece_table_param_;
  }
  return ret;
}

int ObPersistentLobAdaptor::get_aux_table_dml_param(
    const LobAuxTarget target,
    const ObTableDMLParam *&table_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (OB_FAIL(try_init_table_param())) {
    LOG_WARN("try_init_table_param fail", KR(ret));
  } else if (LobAuxTarget::META == target) {
    table_param = meta_table_dml_param_;
  } else if (LobAuxTarget::PIECE == target) {
    table_param = piece_table_dml_param_;
  }
  return ret;
}

int ObPersistentLobAdaptor::init_table_param()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("TmpLobPersist", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObSEArray<uint64_t, 7> meta_column_ids;
  ObSEArray<uint64_t, 4> piece_column_ids;
  HEAP_VARS_2((ObTableSchema, meta_schema, &tmp_allocator),
              (ObTableSchema, piece_schema, &tmp_allocator)) {
    if (ATOMIC_LOAD(&table_param_inited_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("init again", KR(ret), K(table_param_inited_));
    } else if (nullptr != meta_table_param_ || nullptr != meta_table_dml_param_
               || nullptr != piece_table_param_ || nullptr != piece_table_dml_param_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("init again", KR(ret), KP(meta_table_param_), KP(meta_table_dml_param_),
                KP(piece_table_param_), KP(piece_table_dml_param_));
    } else if (OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_meta_schema(meta_schema))) {
      LOG_ERROR("get lob meta schema fail", KR(ret));
    } else if (OB_FAIL(share::ObInnerTableSchema::all_column_aux_lob_piece_schema(piece_schema))) {
      LOG_ERROR("get lob piece schema fail", KR(ret));
    } else if (OB_FAIL(init_meta_column_ids(meta_column_ids))) {
      LOG_ERROR("init_meta_column_ids fail", KR(ret));
    } else if (OB_FAIL(init_piece_column_ids(piece_column_ids))) {
      LOG_ERROR("init_piece_column_ids fail", KR(ret));
    } else if (OB_FALSE_IT(meta_table_param_ = OB_NEWx(ObTableParam, &allocator_, allocator_))) {
    } else if (OB_ISNULL(meta_table_param_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc meta_table_param fail", KR(ret), "size", sizeof(ObTableParam));
    } else if (OB_FAIL(meta_table_param_->convert(meta_schema, meta_column_ids, sql::ObStoragePushdownFlag()))) {
      LOG_ERROR("Fail to convert meta table param", KR(ret), K(meta_schema));
    } else if (OB_FALSE_IT(meta_table_dml_param_ = OB_NEWx(ObTableDMLParam, &allocator_, allocator_))) {
    } else if (OB_ISNULL(meta_table_dml_param_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc meta_table_dml_param fail", KR(ret), "size", sizeof(ObTableDMLParam));
    } else if (OB_FAIL(meta_table_dml_param_->convert(&meta_schema, meta_schema.get_schema_version(), meta_column_ids))) {
      LOG_ERROR("Fail to convert meta dml table param", KR(ret), K(meta_schema));
    } else if (OB_FALSE_IT(piece_table_param_ = OB_NEWx(ObTableParam, &allocator_, allocator_))) {
    } else if (OB_ISNULL(piece_table_param_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc piece_table_param fail", KR(ret), "size", sizeof(ObTableParam));
    } else if (OB_FAIL(piece_table_param_->convert(piece_schema, piece_column_ids, sql::ObStoragePushdownFlag()))) {
      LOG_ERROR("Fail to convert piece table param", KR(ret), K(piece_schema));
    } else if (OB_FALSE_IT(piece_table_dml_param_ = OB_NEWx(ObTableDMLParam, &allocator_, allocator_))) {
    } else if (OB_ISNULL(piece_table_dml_param_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc piece_table_dml_param fail", KR(ret), "size", sizeof(ObTableDMLParam));
    } else if (OB_FAIL(piece_table_dml_param_->convert(&piece_schema, piece_schema.get_schema_version(), piece_column_ids))) {
      LOG_ERROR("Fail to convert piece dml table param", KR(ret), K(piece_schema));
    } else {
      ATOMIC_STORE(&table_param_inited_, true);
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::prepare_table_param(
    const LobAuxTarget target,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(scan_param.table_param_ != NULL)) {
    // already prepared
  } else if (OB_FAIL(get_aux_table_param(target, scan_param.table_param_))) {
    LOG_WARN("get_aux_table_param fail", KR(ret), "target", get_lob_aux_target_str(target));
  }
  return ret;
}

// === lob aux DML (meta + piece) ===
// Unified DML preparation path for both meta and piece aux tables. Each ObLobWriteContext
// holds two independent dml_base_param slots (dml_base_meta_param_ / dml_base_piece_param_),
// lazily allocated on the statement arena and reused across all chunks of one DML.
// Only spec_seq_no_ and store_ctx_guard_ are refreshed per chunk.

int ObPersistentLobAdaptor::build_lob_aux_table_dml(
    const LobAuxTarget target,
    const ObLobWriteContext &write_ctx,
    ObDMLBaseParam &dml_base_param,
    ObStoreCtxGuard *store_ctx_guard)
{
  int ret = OB_SUCCESS;
  const ObTableDMLParam *table_dml_param = nullptr;
  if (OB_FAIL(get_aux_table_dml_param(target, table_dml_param))) {
    LOG_WARN("get_aux_table_dml_param fail", KR(ret), K(write_ctx));
  } else {
    dml_base_param.timeout_ = write_ctx.timeout_;
    dml_base_param.is_total_quantity_log_ = write_ctx.is_total_quantity_log_;
    dml_base_param.tz_info_ = nullptr;
    dml_base_param.sql_mode_ = SMO_DEFAULT;
    dml_base_param.encrypt_meta_ = &dml_base_param.encrypt_meta_legacy_;
    dml_base_param.check_schema_version_ = false; // lob tablet should not check schema version
    dml_base_param.data_row_for_lob_ = write_ctx.data_row_;
    dml_base_param.schema_version_ = 0;
    dml_base_param.store_ctx_guard_ = store_ctx_guard;
    dml_base_param.write_flag_.reset();
    dml_base_param.write_flag_.set_is_insert_up();
    if (write_ctx.skip_flush_redo()) {
      dml_base_param.write_flag_.set_skip_flush_redo();
    }
    dml_base_param.dml_allocator_ = write_ctx.allocator_;
    dml_base_param.table_param_ = table_dml_param;
    if (OB_FAIL(dml_base_param.snapshot_.assign(write_ctx.snapshot_))) {
      LOG_WARN("assign snapshot fail", K(ret), K(write_ctx));
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::prepare_lob_aux_dml(
    const LobAuxTarget target,
    ObLobWriteContext &write_ctx)
{
  int ret = OB_SUCCESS;
  ObDMLBaseParam *cur_dml = nullptr;
  if (OB_FAIL(write_ctx.get_aux_dml_base_param(target, cur_dml))) {
    LOG_WARN("get aux dml base param fail", KR(ret), K(write_ctx));
  } else if (nullptr == cur_dml) {
    if (OB_ISNULL(write_ctx.allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("allocator is null", KR(ret), K(write_ctx));
    } else {
      ObStoreCtxGuard *store_ctx_guard = nullptr;
      void *buf = write_ctx.allocator_->alloc(sizeof(ObDMLBaseParam) + sizeof(ObStoreCtxGuard));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc lob aux dml base param", K(ret), K(write_ctx));
      } else {
        cur_dml = new(buf) ObDMLBaseParam();
        store_ctx_guard = new((char*)buf + sizeof(ObDMLBaseParam)) ObStoreCtxGuard();
        if (OB_FAIL(build_lob_aux_table_dml(target, write_ctx, *cur_dml, store_ctx_guard))) {
          LOG_WARN("failed to build lob aux table dml param", K(ret), K(write_ctx));
        } else if (OB_FAIL(write_ctx.set_aux_dml_base_param(target, cur_dml))) {
          LOG_WARN("set aux dml base param fail", KR(ret), K(write_ctx));
        }
        if (OB_FAIL(ret)) {
          store_ctx_guard->~ObStoreCtxGuard();
          cur_dml->~ObDMLBaseParam();
          cur_dml = nullptr;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(write_ctx.update_seq_no(target))) {
    LOG_WARN("update seq no fail", KR(ret), K(write_ctx));
  } else {
    cur_dml->store_ctx_guard_->reset();
    ObAccessService *oas = MTL(ObAccessService *);
    if (OB_FAIL(oas->get_write_store_ctx_guard(write_ctx.ls_id_,
                                               write_ctx.timeout_,
                                               *write_ctx.tx_desc_,
                                               write_ctx.snapshot_,
                                               0, /*branch_id*/
                                               cur_dml->write_flag_,
                                               *cur_dml->store_ctx_guard_,
                                               cur_dml->spec_seq_no_))) {
      LOG_WARN("fail to get write store tx ctx guard", K(ret), K(write_ctx));
    }
  }
  return ret;
}

// errsim hook for META write path. See header for return-value semantics.
OB_NOINLINE int ObPersistentLobAdaptor::errsim_meta_dml_failed(
    ObLobInsertContext &insert_ctx,
    blocksstable::ObDatumRowIterator &row_iter,
    bool &is_injected)
{
  int ret = OB_SUCCESS;
  is_injected = false;
  ret = OB_E(EventTable::EN_LOB_META_DML_FAILED, insert_ctx.get_inrow_threshold()) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    if (insert_ctx.tablet_id_.is_user_tablet()) {
      is_injected = true;
      LOG_ERROR("due to error injection, lob meta not being inserted into the auxiliary table",
                K(ret), K(insert_ctx));
      ObDatumRow *row = nullptr;
      while (OB_SUCC(ret) && OB_SUCC(row_iter.get_next_row(row))) {
        // drain remaining rows so the upstream iter is fully consumed
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("drain row_iter fail under error injection", K(ret), K(insert_ctx));
      }
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::build_lob_aux_dml_common(
    const LobAuxTarget target,
    ObLobWriteContext &write_ctx,
    ObTabletID &tablet_id,
    ObDMLBaseParam *&dml,
    ObSEArray<uint64_t, 8> &column_ids)
{
  int ret = OB_SUCCESS;
  uint64_t column_cnt = 0;
  if (OB_FAIL(write_ctx.get_aux_column_cnt(target, column_cnt))) {
    LOG_WARN("get aux column cnt fail", KR(ret), K(write_ctx));
  } else if (OB_ISNULL(MTL(ObAccessService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get access service failed", K(ret), K(write_ctx));
  } else if (OB_ISNULL(write_ctx.tx_desc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tx desc null", K(ret), K(write_ctx));
  } else if (OB_FAIL(ObLobUtil::refresh_lob_tablet_id(write_ctx))) {
    LOG_WARN("failed to get tablets", K(ret), K(write_ctx));
  } else if (OB_FAIL(prepare_lob_aux_dml(target, write_ctx))) {
    LOG_WARN("failed to prepare lob aux dml", K(ret), K(write_ctx));
  } else if (OB_FAIL(write_ctx.get_aux_tablet_id(target, tablet_id))) {
    LOG_WARN("get aux tablet_id fail", KR(ret), K(write_ctx));
  } else if (OB_FAIL(write_ctx.get_aux_dml_base_param(target, dml))) {
    LOG_WARN("get aux dml base param fail", KR(ret), K(write_ctx));
  } else {
    column_ids.reuse();
    for (uint64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      if (OB_FAIL(column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
        LOG_WARN("push column ids failed", K(ret), K(i), K(write_ctx));
      }
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::do_write_lob_aux(
    const LobAuxTarget target,
    ObLobInsertContext &insert_ctx,
    blocksstable::ObDatumRowIterator &row_iter,
    const ObLobOutTableRedoCtx *outtable_redo_ctx)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTabletID tablet_id;
  ObDMLBaseParam *dml = nullptr;
  ObSEArray<uint64_t, 8> column_ids;
  bool is_injected = false;
  if (OB_UNLIKELY(LobAuxTarget::META != target && nullptr != outtable_redo_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("outtable redo context only supports lob meta writes", KR(ret), K(target));
  } else if (OB_FAIL(build_lob_aux_dml_common(target, insert_ctx, tablet_id, dml, column_ids))) {
    LOG_WARN("build_lob_aux_dml_common fail", K(ret), K(insert_ctx));
  } else if (OB_FAIL(errsim_meta_dml_failed(insert_ctx, row_iter, is_injected))) {
    LOG_WARN("errsim_meta_dml_failed fail", K(ret), K(insert_ctx));
  }
  if (OB_FAIL(ret)) {
  } else if (!is_injected) {
    const ObLobOutTableRedoCtx *saved_redo_ctx = dml->outtable_redo_ctx_;
    dml->outtable_redo_ctx_ = outtable_redo_ctx;
    if (OB_FAIL(MTL(ObAccessService*)->insert_rows(
          insert_ctx.ls_id_, tablet_id, *insert_ctx.tx_desc_, *dml,
          column_ids, &row_iter, affected_rows))) {
      LOG_WARN("insert_rows fail", K(ret), K(insert_ctx));
    }
    dml->outtable_redo_ctx_ = saved_redo_ctx;
  }
  return ret;
}

int ObPersistentLobAdaptor::do_erase_lob_aux(
    const LobAuxTarget target,
    ObLobDeleteContext &delete_ctx,
    blocksstable::ObDatumRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTabletID tablet_id;
  ObDMLBaseParam *dml = nullptr;
  ObSEArray<uint64_t, 8> column_ids;
  if (OB_FAIL(build_lob_aux_dml_common(target, delete_ctx, tablet_id, dml, column_ids))) {
    LOG_WARN("build_lob_aux_dml_common fail", K(ret), K(delete_ctx));
  } else if (OB_FAIL(MTL(ObAccessService*)->delete_rows(
                 delete_ctx.ls_id_, tablet_id, *delete_ctx.tx_desc_, *dml,
                 column_ids, &row_iter, affected_rows))) {
    LOG_WARN("delete_rows fail", K(ret), K(delete_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::do_update_lob_aux(
    const LobAuxTarget target,
    ObLobUpdateContext &update_ctx,
    blocksstable::ObDatumRowIterator &row_iter,
    const ObLobOutTableRedoCtx *outtable_redo_ctx)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTabletID tablet_id;
  ObDMLBaseParam *dml = nullptr;
  ObSEArray<uint64_t, 8> column_ids;
  ObSEArray<uint64_t, 8> update_column_ids;
  uint64_t column_cnt = 0;
  uint64_t rowkey_cnt = 0;
  if (OB_UNLIKELY(LobAuxTarget::META != target && nullptr != outtable_redo_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("outtable redo context only supports lob meta updates", KR(ret), K(target));
  } else if (OB_FAIL(update_ctx.get_aux_column_cnt(target, column_cnt))) {
    LOG_WARN("get_aux_column_cnt fail", K(ret), K(update_ctx));
  } else if (OB_FAIL(update_ctx.get_aux_rowkey_col_cnt(target, rowkey_cnt))) {
    LOG_WARN("get_aux_rowkey_col_cnt fail", K(ret), K(update_ctx));
  } else if (OB_FAIL(build_lob_aux_dml_common(target, update_ctx, tablet_id, dml, column_ids))) {
    LOG_WARN("build_lob_aux_dml_common fail", K(ret), K(update_ctx));
  }
  // Update columns start after rowkey; normal columns may be just 1 (piece) or 4 (meta).
  for (uint64_t i = rowkey_cnt; OB_SUCC(ret) && i < column_cnt; ++i) {
    if (OB_FAIL(update_column_ids.push_back(OB_APP_MIN_COLUMN_ID + i))) {
      LOG_WARN("push update column ids failed", K(ret), K(i), K(update_ctx));
    }
  }
  if (OB_SUCC(ret)) {
    const ObLobOutTableRedoCtx *saved_redo_ctx = dml->outtable_redo_ctx_;
    dml->outtable_redo_ctx_ = outtable_redo_ctx;
    if (OB_FAIL(MTL(ObAccessService*)->update_rows(
            update_ctx.ls_id_, tablet_id, *update_ctx.tx_desc_, *dml,
            column_ids, update_column_ids, &row_iter, affected_rows))) {
      LOG_WARN("update_rows fail", K(ret), K(update_ctx));
    }
    dml->outtable_redo_ctx_ = saved_redo_ctx;
  }
  return ret;
}

int ObPersistentLobAdaptor::write_lob_piece(ObLobInsertContext &insert_ctx, blocksstable::ObDatumRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_write_lob_aux(LobAuxTarget::PIECE, insert_ctx, row_iter))) {
    LOG_WARN("do_write_lob_aux fail", K(ret), K(insert_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::delete_lob_piece(ObLobDeleteContext &delete_ctx, blocksstable::ObDatumRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_erase_lob_aux(LobAuxTarget::PIECE, delete_ctx, row_iter))) {
    LOG_WARN("do_erase_lob_aux fail", K(ret), K(delete_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::update_lob_piece(ObLobUpdateContext &update_ctx, blocksstable::ObDatumRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_update_lob_aux(LobAuxTarget::PIECE, update_ctx, row_iter))) {
    LOG_WARN("do_update_lob_aux fail", K(ret), K(update_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::write_lob_piece(ObLobInsertContext &insert_ctx, ObLobPieceInfo &row_info)
{
  int ret = OB_SUCCESS;
  ObDatumRow new_row;
  ObLobPersistInsertSingleRowIter single_iter;
  if (OB_FAIL(new_row.init(ObLobPieceUtil::LOB_PIECE_COLUMN_CNT))) {
    LOG_WARN("failed to init datum row", K(ret), K(insert_ctx));
  } else if (FALSE_IT(set_lob_piece_row(new_row, row_info))) {
  } else if (OB_FAIL(single_iter.init(&insert_ctx, LobAuxTarget::PIECE, &new_row))) {
    LOG_WARN("single_iter init fail", K(ret), K(insert_ctx));
  } else if (OB_FAIL(write_lob_piece(insert_ctx, single_iter))) {
    LOG_WARN("write_lob_piece fail", K(ret), K(insert_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::delete_lob_piece(ObLobDeleteContext &delete_ctx, ObLobPieceInfo &row_info)
{
  int ret = OB_SUCCESS;
  ObDatumRow new_row;
  ObLobPersistDeleteSingleRowIter single_iter;
  if (OB_FAIL(new_row.init(ObLobPieceUtil::LOB_PIECE_COLUMN_CNT))) {
    LOG_WARN("failed to init datum row", K(ret), K(delete_ctx));
  } else if (FALSE_IT(set_lob_piece_row(new_row, row_info))) {
  } else if (OB_FAIL(single_iter.init(&delete_ctx, &new_row))) {
    LOG_WARN("single_iter init fail", K(ret), K(delete_ctx));
  } else if (OB_FAIL(delete_lob_piece(delete_ctx, single_iter))) {
    LOG_WARN("erase_lob_piece fail", K(ret), K(delete_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::update_lob_piece(
    ObLobUpdateContext &update_ctx,
    ObLobPieceInfo &old_row,
    ObLobPieceInfo &new_row)
{
  int ret = OB_SUCCESS;
  ObDatumRow new_datum_row;
  ObDatumRow old_datum_row;
  ObLobPersistUpdateSingleRowIter upd_iter;
  if (OB_FAIL(new_datum_row.init(ObLobPieceUtil::LOB_PIECE_COLUMN_CNT))) {
    LOG_WARN("failed to init new datum row", K(ret), K(update_ctx));
  } else if (OB_FAIL(old_datum_row.init(ObLobPieceUtil::LOB_PIECE_COLUMN_CNT))) {
    LOG_WARN("failed to init old datum row", K(ret), K(update_ctx));
  } else if (FALSE_IT(set_lob_piece_row(new_datum_row, new_row))) {
  } else if (FALSE_IT(set_lob_piece_row(old_datum_row, old_row))) {
  } else if (OB_FAIL(upd_iter.init(&update_ctx, &old_datum_row, &new_datum_row))) {
    LOG_WARN("upd_iter init fail", K(ret), K(update_ctx));
  } else if (OB_FAIL(update_lob_piece(update_ctx, upd_iter))) {
    LOG_WARN("update_lob_piece fail", K(ret), K(update_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::scan_lob_piece(ObLobReadContext &read_ctx, ObLobPieceTableIter *&iter)
{
  int ret = OB_SUCCESS;
  ObLobAuxTableIterator<ObLobPieceInfo> *piece_iter = nullptr;
  if (OB_FAIL(scan_lob_aux(LobAuxTarget::PIECE, read_ctx, piece_iter))) {
    LOG_WARN("scan_lob_aux fail", K(ret), K(read_ctx));
  } else {
    iter = reinterpret_cast<ObLobPieceTableIter *>(piece_iter);
  }
  return ret;
}

int ObPersistentLobAdaptor::revert_scan_iter(ObLobAuxTableBaseIterator *iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("iter is null", K(ret));
  } else if (nullptr != iter->get_access_ctx()) {
    LOG_DEBUG("alloced from ctx, no need actual release", KPC(iter));
  } else {
    LOG_DEBUG("release scan iter", K(ret), KPC(iter));
    iter->reset();
    iter->~ObLobAuxTableBaseIterator();
  }
  return ret;
}

int ObPersistentLobAdaptor::prepare_table_scan_param(
    const LobAuxTarget target,
    const ObLobReadContext &read_ctx,
    const bool is_get,
    ObTableScanParam &scan_param,
    ObIAllocator *stmt_allocator,
    ObIAllocator *scan_allocator)
{
  int ret = OB_SUCCESS;
  uint64_t col_num = 0;
  if (OB_FAIL(get_read_aux_column_cnt(target, col_num))) {
    LOG_WARN("get_aux_column_cnt fail", K(ret), K(read_ctx));
  } else if (OB_FAIL(build_common_scan_param(
      target, read_ctx, is_get, col_num, scan_param, stmt_allocator, scan_allocator))) {
    LOG_WARN("build common scan param fail", K(ret));
  } else if (OB_FAIL(prepare_table_param(target, scan_param))) {
    LOG_WARN("prepare lob aux table param fail", K(ret), K(read_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::build_common_scan_param(
    const LobAuxTarget target,
    const ObLobReadContext &read_ctx,
    const bool is_get,
    uint64_t col_num,
    ObTableScanParam &scan_param,
    ObIAllocator *stmt_allocator,
    ObIAllocator *scan_allocator)
{
  int ret = OB_SUCCESS;

  ObQueryFlag query_flag(ObQueryFlag::Forward, // scan_order
                          false, // daily_merge
                          false, // optimize
                          false, // sys scan
                          true, // full_row
                          false, // index_back
                          false, // query_stat
                          ObQueryFlag::MysqlMode, // sql_mode
                          read_ctx.need_read_latest_ // read_latest
                        );
  query_flag.disable_cache();
  if (LobAuxTarget::META == target || read_ctx.enable_block_cache()) {
    query_flag.set_use_block_cache();
  }
  query_flag.scan_order_ = read_ctx.scan_backward_ ? ObQueryFlag::Reverse : ObQueryFlag::Forward;
  scan_param.scan_flag_.flag_ = query_flag.flag_;
  // set column ids
  scan_param.column_ids_.reuse();
  for (uint64_t i = 0; OB_SUCC(ret) && i < col_num; i++) {
    if (OB_FAIL(scan_param.column_ids_.push_back(OB_APP_MIN_COLUMN_ID + i))) {
      LOG_WARN("push col id failed.", K(ret), K(i));
    }
  }
  if (FAILEDx(get_read_aux_tablet_id(target, read_ctx, scan_param.tablet_id_))) {
    LOG_WARN("get_aux_tablet_id fail", K(ret), K(read_ctx));
  } else {
    scan_param.ls_id_ = read_ctx.ls_id_;
    scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
    // table param
    scan_param.index_id_ = 0; // table id
    scan_param.is_get_ = is_get;
    // set timeout
    scan_param.timeout_ = read_ctx.timeout_;
    // scan_param.virtual_column_exprs_
    scan_param.limit_param_.limit_ = -1;
    scan_param.limit_param_.offset_ = 0;
    // sessions
    if(read_ctx.read_latest_ || read_ctx.need_read_latest_) {
      scan_param.tx_id_ = read_ctx.snapshot_.core_.tx_id_;
    }
    scan_param.sql_mode_ = read_ctx.sql_mode_;
    // common set
    scan_param.allocator_ = stmt_allocator;
    scan_param.for_update_ = false;
    scan_param.for_update_wait_timeout_ = scan_param.timeout_;
    scan_param.scan_allocator_ = scan_allocator;
    scan_param.frozen_version_ = -1;
    scan_param.force_refresh_lc_ = false;
    scan_param.output_exprs_ = nullptr;
    scan_param.aggregate_exprs_ = nullptr;
    scan_param.op_ = nullptr;
    scan_param.row2exprs_projector_ = nullptr;
    scan_param.need_scn_ = false;
    scan_param.pd_storage_flag_ = false;
    scan_param.fb_snapshot_ = read_ctx.fb_snapshot_;
    if (OB_FAIL(scan_param.snapshot_.assign(read_ctx.snapshot_))) {
      LOG_WARN("assign snapshot fail", K(ret));
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::get_read_aux_tablet_id(
    const LobAuxTarget target,
    const ObLobReadContext &read_ctx,
    common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (LobAuxTarget::META == target) {
    tablet_id = read_ctx.lob_meta_tablet_id_;
  } else {
    tablet_id = read_ctx.lob_piece_tablet_id_;
  }
  return ret;
}

int ObPersistentLobAdaptor::get_read_aux_column_cnt(
    const LobAuxTarget target,
    uint64_t &column_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (LobAuxTarget::META == target) {
    column_cnt = ObLobMetaUtil::LOB_META_COLUMN_CNT;
  } else {
    column_cnt = ObLobPieceUtil::LOB_PIECE_COLUMN_CNT;
  }
  return ret;
}

void ObPersistentLobAdaptor::set_lob_meta_row(
    blocksstable::ObDatumRow &datum_row,
    const ObLobMetaInfo &in_row)
{
  datum_row.reuse();
  datum_row.storage_datums_[ObLobMetaUtil::LOB_COL_IDX_COL_ID].set_int(in_row.lob_col_idx_);
  datum_row.storage_datums_[ObLobMetaUtil::LOB_ID_COL_ID].set_string(in_row.lob_id_.get_string());
  datum_row.storage_datums_[ObLobMetaUtil::SEQ_ID_COL_ID].set_string(in_row.seq_id_);
  datum_row.storage_datums_[ObLobMetaUtil::BYTE_LEN_COL_ID].set_uint(in_row.byte_len_);
  datum_row.storage_datums_[ObLobMetaUtil::CHAR_LEN_COL_ID].set_uint(in_row.char_len_);
  if (in_row.lob_extra_info_.length() > 0) {
    datum_row.storage_datums_[ObLobMetaUtil::LOB_INFO_COL_ID].set_string(in_row.lob_extra_info_);
  } else {
    datum_row.storage_datums_[ObLobMetaUtil::LOB_INFO_COL_ID].set_null();
  }
  // phase1: lob_block_id_ is always null
  if (in_row.lob_block_id_.length() > 0) {
    datum_row.storage_datums_[ObLobMetaUtil::LOB_BLOCK_ID_COL_ID].set_string(in_row.lob_block_id_);
  } else {
    datum_row.storage_datums_[ObLobMetaUtil::LOB_BLOCK_ID_COL_ID].set_null();
  }
}

void ObPersistentLobAdaptor::set_lob_piece_row(
    blocksstable::ObDatumRow &datum_row,
    const ObLobPieceInfo &in_row)
{
  datum_row.reuse();
  datum_row.storage_datums_[ObLobPieceUtil::PIECE_LOB_COL_IDX_COL_ID].set_int(in_row.lob_col_idx_);
  datum_row.storage_datums_[ObLobPieceUtil::PIECE_LOB_ID_COL_ID].set_string(in_row.lob_id_.get_string());
  datum_row.storage_datums_[ObLobPieceUtil::PIECE_SEQ_ID_COL_ID].set_string(in_row.seq_id_);
  if (in_row.lob_data_.length() > 0) {
    datum_row.storage_datums_[ObLobPieceUtil::PIECE_LOB_DATA_COL_ID].set_string(in_row.lob_data_);
  } else {
    datum_row.storage_datums_[ObLobPieceUtil::PIECE_LOB_DATA_COL_ID].set_null();
  }
}

void ObPersistentLobAdaptor::set_lob_row(
    blocksstable::ObDatumRow &datum_row,
    const ObLobMetaInfo &meta_info)
{
  set_lob_meta_row(datum_row, meta_info);
}
void ObPersistentLobAdaptor::set_lob_row(
    blocksstable::ObDatumRow &datum_row,
    const ObLobPieceInfo &piece_info)
{
  set_lob_piece_row(datum_row, piece_info);
}

template<typename T>
int ObPersistentLobAdaptor::scan_lob_aux(
    const LobAuxTarget target,
    ObLobReadContext &read_ctx,
    ObLobAuxTableIterator<T> *&iter)
{
  int ret = OB_SUCCESS;
  ObLobAuxTableIterator<T> *tmp_iter = nullptr;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target), K(read_ctx));
  } else if (OB_NOT_NULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input iter is not null", K(ret), KPC(iter), K(read_ctx));
  } else if (OB_NOT_NULL(read_ctx.access_ctx_) && !read_ctx.snapshot_.tx_id().is_valid()) {
    // Do not reuse readers via read_latest in a transaction, or its latest writes may be missed.
    // Only reader caching is restricted; the LOCATION provider in read_ctx remains available.
    ObLobAuxTableBaseIterator *base_iter = nullptr;
    if (OB_FAIL(scan_with_ctx(target, read_ctx, base_iter))) {
      LOG_WARN("scan_with_ctx fail", K(ret), K(read_ctx));
    } else {
      // same cache only holds iters of this T, so the downcast is safe
      iter = static_cast<ObLobAuxTableIterator<T> *>(base_iter);
    }
  } else if (OB_ISNULL(tmp_iter = OB_NEWx(ObLobAuxTableIterator<T>,
                                          &read_ctx.iter_allocator_, nullptr/*access_ctx*/))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc iter fail", K(ret), K(read_ctx),
             "alloc_size", sizeof(ObLobAuxTableIterator<T>));
  } else if (OB_FAIL(tmp_iter->open(target, read_ctx, this, &read_ctx.iter_allocator_))) {
    LOG_WARN("open iter fail", K(ret), K(read_ctx), KPC(tmp_iter));
  } else {
    iter = tmp_iter;
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(tmp_iter)) {
      LOG_INFO("release iter after failed", KPC(tmp_iter));
      tmp_iter->reset();
      tmp_iter->~ObLobAuxTableIterator();
    }
  }
  return ret;
}

int ObPersistentLobAdaptor::get_meta_summary_info(ObLobReadContext &read_ctx, ObLobMetaInfo &meta_summary_info)
{
  int ret = OB_SUCCESS;
  ObLobAuxTableIterator<ObLobMetaInfo> *iter = nullptr;
  ObLobMetaInfo temp_summary;
  char seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString summary_seq_id(sizeof(seq_id_buf), seq_id_buf);
  const ObString saved_seq_id_start = read_ctx.seq_id_start_;
  const ObString saved_seq_id_end = read_ctx.seq_id_end_;
  meta_summary_info.reset();
  if (OB_ISNULL(read_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", KR(ret), K(read_ctx));
  } else if (OB_FAIL(ObLobSeqId::get_summary_seq_id(summary_seq_id))) {
    LOG_WARN("failed to build meta summary seq id", K(ret));
  } else if (FALSE_IT(read_ctx.seq_id_start_ = summary_seq_id)) {
  } else if (FALSE_IT(read_ctx.seq_id_end_ = summary_seq_id)) {
  } else if (OB_FAIL(scan_lob_meta(read_ctx, iter))) {
    LOG_WARN("failed to get meta iter", KR(ret), K(read_ctx), KPC(iter));
  } else if (OB_FAIL(iter->get_next_row(temp_summary))) {
    if (OB_ITER_END == ret) {
      ret = OB_LOB_VALUE_NOT_EXIST;
    }
    LOG_WARN("failed to get meta summary info", KR(ret), K(read_ctx), KPC(iter));
  } else if (OB_FAIL(meta_summary_info.deep_copy(*read_ctx.allocator_, temp_summary))) {
    LOG_WARN("failed to copy meta summary info", KR(ret), K(temp_summary));
  }

  if (OB_NOT_NULL(iter)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(revert_scan_iter(iter))) {
      LOG_WARN("failed to revert scan iter", KR(tmp_ret), KR(ret), K(read_ctx), KPC(iter));
    } else {
      iter = nullptr;
    }
  }
  read_ctx.seq_id_start_ = saved_seq_id_start;
  read_ctx.seq_id_end_ = saved_seq_id_end;
  return ret;
}

int ObPersistentLobAdaptor::get_last_meta_info(
    ObLobReadContext &read_ctx,
    ObLobMetaInfo &last_meta_info)
{
  int ret = OB_SUCCESS;
  ObLobAuxTableIterator<ObLobMetaInfo> *iter = nullptr;
  ObLobMetaInfo scanned_meta;
  char first_seq_id_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString first_seq_id(sizeof(first_seq_id_buf), first_seq_id_buf);
  const ObString saved_seq_id_start = read_ctx.seq_id_start_;
  const ObString saved_seq_id_end = read_ctx.seq_id_end_;
  const bool saved_scan_backward = read_ctx.scan_backward_;
  last_meta_info.reset();
  if (OB_ISNULL(read_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", KR(ret), K(read_ctx));
  } else if (OB_FAIL(ObLobSeqId::get_seq_id(1, first_seq_id))) {
    LOG_WARN("failed to build first data meta seq id", KR(ret));
  } else {
    read_ctx.scan_backward_ = true;
    read_ctx.seq_id_start_ = first_seq_id;
    read_ctx.seq_id_end_.reset();
    if (OB_FAIL(scan_lob_aux(LobAuxTarget::META, read_ctx, iter))) {
      LOG_WARN("failed to scan last lob meta info", KR(ret), K(read_ctx));
    } else if (OB_FAIL(iter->get_next_row(scanned_meta))) {
      if (OB_ITER_END == ret) {
        ret = OB_LOB_VALUE_NOT_EXIST;
      }
      LOG_WARN("failed to read last lob meta info", KR(ret), K(read_ctx));
    } else if (OB_FAIL(last_meta_info.deep_copy(*read_ctx.allocator_, scanned_meta))) {
      LOG_WARN("failed to copy last lob meta info", KR(ret), K(scanned_meta));
    }
  }

  if (OB_NOT_NULL(iter)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(revert_scan_iter(iter))) {
      LOG_WARN("failed to revert last meta scan iter", KR(tmp_ret), KR(ret), K(read_ctx));
    } else {
      iter = nullptr;
    }
  }
  read_ctx.seq_id_start_ = saved_seq_id_start;
  read_ctx.seq_id_end_ = saved_seq_id_end;
  read_ctx.scan_backward_ = saved_scan_backward;
  return ret;
}

int ObPersistentLobAdaptor::get_lob_piece_info(
    ObLobReadContext &read_ctx,
    const common::ObString &seq_id,
    ObLobPieceInfo &piece_info)
{
  int ret = OB_SUCCESS;
  ObLobAuxTableIterator<ObLobPieceInfo> *iter = nullptr;
  ObLobPieceInfo scanned_piece;
  const ObString saved_seq_id_start = read_ctx.seq_id_start_;
  const ObString saved_seq_id_end = read_ctx.seq_id_end_;
  const bool saved_scan_backward = read_ctx.scan_backward_;
  piece_info.reset();
  if (OB_ISNULL(read_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", KR(ret), K(read_ctx));
  } else if (OB_UNLIKELY(!ObLobSeqId(seq_id).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob piece seq id", KR(ret), K(seq_id));
  } else {
    read_ctx.scan_backward_ = false;
    read_ctx.seq_id_start_ = seq_id;
    read_ctx.seq_id_end_ = seq_id;
    if (OB_FAIL(scan_lob_aux(LobAuxTarget::PIECE, read_ctx, iter))) {
      LOG_WARN("failed to scan lob piece info", KR(ret), K(seq_id), K(read_ctx));
    } else if (OB_FAIL(iter->get_next_row(scanned_piece))) {
      if (OB_ITER_END == ret) {
        ret = OB_LOB_VALUE_NOT_EXIST;
      }
      LOG_WARN("failed to read lob piece info", KR(ret), K(seq_id), K(read_ctx));
    } else if (OB_FAIL(piece_info.deep_copy(*read_ctx.allocator_, scanned_piece))) {
      LOG_WARN("failed to copy lob piece info", KR(ret), K(scanned_piece));
    }
  }

  if (OB_NOT_NULL(iter)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(revert_scan_iter(iter))) {
      LOG_WARN("failed to revert lob piece scan iter", KR(tmp_ret), KR(ret), K(read_ctx));
    } else {
      iter = nullptr;
    }
  }
  read_ctx.seq_id_start_ = saved_seq_id_start;
  read_ctx.seq_id_end_ = saved_seq_id_end;
  read_ctx.scan_backward_ = saved_scan_backward;
  return ret;
}

int ObPersistentLobAdaptor::scan_lob_meta(
    ObLobReadContext &read_ctx,
    ObLobMetaTableIter *&iter)
{
  int ret = OB_SUCCESS;
  ObLobAuxTableIterator<ObLobMetaInfo> *meta_iter = nullptr;
  if (OB_FAIL(scan_lob_aux(LobAuxTarget::META, read_ctx, meta_iter))) {
    LOG_WARN("scan_lob_aux fail", K(ret), K(read_ctx), KPC(iter));
  } else {
    iter = reinterpret_cast<ObLobMetaTableIter *>(meta_iter);
  }
  return ret;
}

int ObPersistentLobAdaptor::scan_with_ctx(
    const LobAuxTarget target,
    ObLobReadContext &read_ctx,
    ObLobAuxTableBaseIterator *&iter)
{
  int ret = OB_SUCCESS;
  bool is_hit = false;
  ObPersistLobReaderCacheKey key;
  ObLobAuxTableBaseIterator *reader = nullptr;
  // meta and piece each hold an independent reader cache to avoid mutual eviction
  ObPersistLobReaderCache *cache = nullptr;
  if (OB_FAIL(read_ctx.access_ctx_->get_aux_reader_cache(target, cache))) {
    LOG_WARN("get_aux_reader_cache fail", K(ret), K(read_ctx));
  } else if (FALSE_IT(key.ls_id_ = read_ctx.ls_id_)) {
  } else if (FALSE_IT(key.snapshot_ = read_ctx.snapshot_.core_.version_.get_val_for_tx())) {
  } else if (FALSE_IT(key.fb_snapshot_ = read_ctx.fb_snapshot_)) {
  } else if (FALSE_IT(key.tablet_id_ = read_ctx.tablet_id_)) {
  } else if (FALSE_IT(key.target_ = target)) {
  } else if (OB_FAIL(cache->get(key, reader))) {
    LOG_WARN("get read from cache fail", KR(ret), K(key));
  } else if (nullptr != reader) { // use cache
    if (!reader->is_open()) {
      if (OB_FAIL(reader->open(target, read_ctx, this, &(cache->get_allocator())))) {
        LOG_WARN("open reader fail", KR(ret), K(key));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(is_hit = true)) {
    } else if (OB_FAIL(reader->rescan(read_ctx))) {
      LOG_WARN("rescan reader fail", KR(ret), K(key));
    } else {
      iter = reader;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null", KR(ret), K(key));
  }

  return ret;
}

// TODO aozeliu.azl read need?
int ObPersistentLobAdaptor::prepare_scan_param_schema_version(
    const LobAuxTarget target,
    ObLobReadContext &read_ctx,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  ObTabletHandle aux_tablet;
  ObTabletID aux_tablet_id;
  if (OB_FAIL(get_read_aux_tablet_id(target, read_ctx, aux_tablet_id))) {
    LOG_WARN("get_aux_tablet_id fail", K(ret), K(read_ctx));
  } else if (!aux_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob aux tablet_id is invalid", K(ret), K(read_ctx));
  } else if (OB_FAIL(ObLobUtil::get_tablet(read_ctx, aux_tablet_id, aux_tablet))) {
    LOG_WARN("get lob aux tablet fail", K(ret), K(read_ctx), K(aux_tablet_id));
  } else {
    scan_param.schema_version_ = aux_tablet.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
  }
  return ret;
}

int ObPersistentLobAdaptor::delete_lob_meta(ObLobDeleteContext &delete_ctx, ObDatumRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_erase_lob_aux(LobAuxTarget::META, delete_ctx, row_iter))) {
    LOG_WARN("do_erase_lob_aux fail", K(ret), K(delete_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::write_lob_meta(
    ObLobInsertContext &insert_ctx,
    ObDatumRowIterator &row_iter,
    const ObLobOutTableRedoCtx *outtable_redo_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_write_lob_aux(LobAuxTarget::META, insert_ctx, row_iter, outtable_redo_ctx))) {
    LOG_WARN("do_write_lob_aux fail", K(ret), K(insert_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::update_lob_meta(
    ObLobUpdateContext &update_ctx,
    ObDatumRowIterator &row_iter,
    const ObLobOutTableRedoCtx *outtable_redo_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_update_lob_aux(LobAuxTarget::META, update_ctx, row_iter, outtable_redo_ctx))) {
    LOG_WARN("do_update_lob_aux fail", K(ret), K(update_ctx));
  }
  return ret;
}

int ObPersistentLobAdaptor::write_lob_meta(ObLobInsertContext &insert_ctx, ObLobMetaInfo &row_info)
{
  int ret = OB_SUCCESS;
  ObDatumRow new_row;
  ObLobPersistInsertSingleRowIter single_iter;
  if (OB_FAIL(new_row.init(ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
    LOG_WARN("failed to init datum row", K(ret));
  } else if (FALSE_IT(set_lob_meta_row(new_row, row_info))) {
  } else if (OB_FAIL(single_iter.init(&insert_ctx, LobAuxTarget::META, &new_row))) {
    LOG_WARN("single_iter init fail", K(ret));
  } else if (OB_FAIL(write_lob_meta(insert_ctx, single_iter))) {
    LOG_WARN("write_lob_meta fail", K(ret));
  }
  return ret;
}

int ObPersistentLobAdaptor::delete_lob_meta(ObLobDeleteContext &delete_ctx, ObLobMetaInfo &row_info)
{
  int ret = OB_SUCCESS;
  ObDatumRow new_row;
  ObLobPersistDeleteSingleRowIter single_iter;
  if (OB_FAIL(new_row.init(ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
    LOG_WARN("failed to init datum row", K(ret));
  } else if (FALSE_IT(set_lob_meta_row(new_row, row_info))) {
  } else if (OB_FAIL(single_iter.init(&delete_ctx, &new_row))) {
    LOG_WARN("single_iter init fail", K(ret));
  } else if (OB_FAIL(delete_lob_meta(delete_ctx, single_iter))) {
    LOG_WARN("erase_lob_meta fail", K(ret));
  }
  return ret;
}

int ObPersistentLobAdaptor::update_lob_meta(
    ObLobUpdateContext &update_ctx,
    ObLobMetaInfo &old_row,
    ObLobMetaInfo &new_row,
    const ObLobOutTableRedoCtx *outtable_redo_ctx)
{
  int ret = OB_SUCCESS;
  ObDatumRow new_datum_row;
  ObDatumRow old_datum_row;
  ObLobPersistUpdateSingleRowIter upd_iter;
  if (OB_FAIL(new_datum_row.init(ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
    LOG_WARN("failed to init new datum row", K(ret));
  } else if (OB_FAIL(old_datum_row.init(ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
    LOG_WARN("failed to init old datum row", K(ret));
  } else if (FALSE_IT(set_lob_meta_row(new_datum_row, new_row))) {
  } else if (FALSE_IT(set_lob_meta_row(old_datum_row, old_row))) {
  } else if (OB_FAIL(upd_iter.init(&update_ctx, &old_datum_row, &new_datum_row))) {
    LOG_WARN("upd_iter init fail", K(ret));
  } else if (OB_FAIL(update_lob_meta(update_ctx, upd_iter, outtable_redo_ctx))) {
    LOG_WARN("update_lob_meta fail", K(ret));
  }
  return ret;
}

} // storage
} // oceanbase
