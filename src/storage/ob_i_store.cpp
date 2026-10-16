/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_i_store.h"
#include "storage/access/ob_table_access_context.h"
#include "share/ob_fork_table_util.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace transaction;
using namespace share;
namespace common
{
OB_SERIALIZE_MEMBER(ObQueryFlag, flag_);
}

namespace storage
{
using namespace common;

int ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(ObColDescIArray &store_out_cols)
{
  int ret = OB_SUCCESS;
  share::schema::ObColDesc desc;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_extra_rowkey_col_cnt(); ++i) {
    const ObMultiVersionExtraRowkey &mv_ext_rowkey = OB_MULTI_VERSION_EXTRA_ROWKEY[i];
    desc.col_id_ = mv_ext_rowkey.column_index_;
    (desc.col_type_.*mv_ext_rowkey.set_meta_type_func_)();
    // by default the trans_version column value would be multiplied by -1
    // so in effect we store the latest version first
    desc.col_order_ = ObOrderType::ASC;
    if (OB_FAIL(store_out_cols.push_back(desc))) {
      STORAGE_LOG(WARN, "add store utput columns failed", K(ret));
    }
  }
  return ret;
}

void ObStoreCtx::reset()
{
  ls_id_.reset();
  ls_ = nullptr;
  branch_ = 0;
  tablet_id_.reset();
  table_iter_ = nullptr;
  table_version_ = INT64_MAX;
  timeout_ = 0;
  mvcc_acc_ctx_.reset();
  tablet_stat_.reset();
  is_read_store_ctx_ = false;
  update_full_column_ = false;
  fork_source_tablet_id_.reset();
  fork_snapshot_scn_.reset();
  fork_snapshot_inited_ = false;
  check_seq_ = 0;
}

int ObStoreCtx::init_for_read(const ObLSID &ls_id,
                              const common::ObTabletID tablet_id,
                              const int64_t timeout,
                              const int64_t tx_lock_timeout,
                              const SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "get_ls from ls service fail.", K(ret), K(*ls_svr));
  } else {
    tablet_id_ = tablet_id;
    ret = init_for_read(ls_handle, timeout, tx_lock_timeout, snapshot_version);
  }
  return ret;
}

int ObStoreCtx::init_for_read(const ObLSHandle &ls_handle,
                              const int64_t timeout,
                              const int64_t tx_lock_timeout,
                              const SCN &snapshot_version)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObTxTable *tx_table = nullptr;
  if (!ls_handle.is_valid() || timeout < 0 || !snapshot_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid arguments", K(ret), K(ls_handle), K(timeout), K(tx_lock_timeout), K(snapshot_version));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_id_));
  } else if (ls->is_logonly_replica()) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(WARN, "logonly replica has no tx table", KR(ret), KPC(ls));
  } else if (OB_ISNULL(tx_table = ls->get_tx_table())) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "get_tx_table from log stream fail.", K(ret), K(*ls));
  } else if (OB_FAIL(mvcc_acc_ctx_.init_read(tx_table, snapshot_version, timeout, tx_lock_timeout))) {
    STORAGE_LOG(WARN, "mvcc_acc_ctx init read fail", KR(ret), K(mvcc_acc_ctx_));
  } else {
    ls_id_ = ls->get_ls_id();
    timeout_ = timeout;
  }
  return ret;
}

void ObStoreCtx::force_print_trace_log()
{
  if (NULL != mvcc_acc_ctx_.tx_desc_) {
    mvcc_acc_ctx_.tx_desc_->print_trace();
  }
  if (NULL != mvcc_acc_ctx_.tx_ctx_) {
    mvcc_acc_ctx_.tx_ctx_->print_trace_log();
  }
}

bool ObStoreCtx::is_uncommitted_data_rollbacked() const
{
  bool bret = false;
  if (NULL != mvcc_acc_ctx_.tx_ctx_) {
    bret = mvcc_acc_ctx_.tx_ctx_->is_data_rollbacked();
  }
  return bret;
}

int ObStoreCtx::get_all_tables(ObIArray<ObITable *> &iter_tables)
{
  int ret = OB_SUCCESS;
  table_iter_->resume();
  while (OB_SUCC(ret)) {
    ObITable *table_ptr = nullptr;
    if (OB_FAIL(table_iter_->get_next(table_ptr))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "failed to get next tables", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_ISNULL(table_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "table must not be null", K(ret), KPC(table_iter_));
    } else if (OB_FAIL(iter_tables.push_back(table_ptr))) {
      TRANS_LOG(WARN, "rowkey_exists check::", K(ret), KPC(table_ptr));
    }
  }
  return ret;
}

void ObStoreCtx::set_table_iter(ObTableStoreIterator *table_iter)
{
  table_iter_ = table_iter;
  fork_source_tablet_id_.reset();
  fork_snapshot_scn_.reset();
  fork_snapshot_inited_ = false;
}

bool ObStoreCtx::has_fork_snapshot() const
{
  return OB_NOT_NULL(table_iter_) && OB_NOT_NULL(table_iter_->get_fork_info());
}

int ObStoreCtx::get_fork_snapshot_scn(
    const common::ObTabletID &tablet_id,
    share::SCN &fork_snapshot_scn)
{
  int ret = OB_SUCCESS;
  fork_snapshot_scn.reset();
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "[FORKTABLE] invalid tablet id", K(ret), K(tablet_id));
  } else if (OB_ISNULL(table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "[FORKTABLE] table iter is null", K(ret));
  } else if (has_fork_snapshot() && OB_UNLIKELY(!tablet_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "[FORKTABLE] invalid fork destination tablet id", K(ret), K_(tablet_id));
  } else if (!fork_snapshot_inited_) {
    const share::ObForkTabletInfo *fork_info = table_iter_->get_fork_info();
    if (OB_ISNULL(fork_info)) {
      fork_snapshot_inited_ = true;
    } else if (OB_UNLIKELY(!fork_info->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "[FORKTABLE] invalid fork info", K(ret), KPC(fork_info));
    } else if (OB_FAIL(fork_snapshot_scn_.convert_for_tx(
                   fork_info->get_fork_snapshot_version()))) {
      STORAGE_LOG(WARN, "[FORKTABLE] failed to convert fork snapshot", K(ret), KPC(fork_info));
    } else {
      fork_source_tablet_id_ = fork_info->get_src_tablet_id();
      fork_snapshot_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      fork_source_tablet_id_.reset();
      fork_snapshot_scn_.reset();
    }
  }
  if (OB_SUCC(ret) && has_fork_snapshot()) {
    if (tablet_id == tablet_id_) {
      fork_snapshot_scn.reset();
    } else if (OB_UNLIKELY(!fork_source_tablet_id_.is_valid()
                           || !fork_snapshot_scn_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "[FORKTABLE] fork snapshot is not prepared", K(ret), K(tablet_id));
    } else if (OB_UNLIKELY(tablet_id != fork_source_tablet_id_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "[FORKTABLE] table tablet is neither fork destination nor source",
          K(ret), K(tablet_id), "dst_tablet_id", tablet_id_,
          "src_tablet_id", fork_source_tablet_id_);
    } else {
      fork_snapshot_scn = fork_snapshot_scn_;
    }
  }
  return ret;
}

int ObStoreCtx::enter_fork_snapshot(const share::SCN &fork_snapshot_scn,
                                    share::SCN &saved_snapshot_version,
                                    bool &saved_fork_source_check)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fork_snapshot_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "[FORKTABLE] invalid fork snapshot scn", K(ret), K(fork_snapshot_scn));
  } else {
    saved_snapshot_version = mvcc_acc_ctx_.snapshot_.version_;
    saved_fork_source_check = mvcc_acc_ctx_.is_fork_source_check_;
    mvcc_acc_ctx_.snapshot_.version_ = fork_snapshot_scn;
    mvcc_acc_ctx_.is_fork_source_check_ = true;
  }
  return ret;
}

void ObStoreCtx::exit_fork_snapshot(const share::SCN &saved_snapshot_version,
                                    const bool saved_fork_source_check)
{
  mvcc_acc_ctx_.snapshot_.version_ = saved_snapshot_version;
  mvcc_acc_ctx_.is_fork_source_check_ = saved_fork_source_check;
}

ObAccessCtxForkGuard::ObAccessCtxForkGuard(ObTableAccessContext &ctx)
  : ctx_(ctx),
    saved_snapshot_version_(),
    saved_fork_source_check_(false),
    saved_tablet_id_(),
    saved_effective_tablet_id_(),
    opened_(false)
{
}

ObAccessCtxForkGuard::~ObAccessCtxForkGuard()
{
  if (opened_) {
    ctx_.exit_fork_snapshot(saved_snapshot_version_, saved_fork_source_check_,
                            saved_tablet_id_, saved_effective_tablet_id_);
  }
}

int ObAccessCtxForkGuard::enter_fork_snapshot_if_needed(const ObITable &table)
{
  int ret = OB_SUCCESS;
  share::SCN fork_snapshot_scn;
  if (OB_ISNULL(ctx_.store_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "[FORKTABLE] store context is null", K(ret));
  } else if (OB_UNLIKELY(opened_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "[FORKTABLE] fork snapshot entered twice", K(ret));
  } else if (!ctx_.store_ctx_->has_fork_snapshot()) {
  } else if (OB_FAIL(ctx_.store_ctx_->get_fork_snapshot_scn(
                 table.get_key().get_tablet_id(), fork_snapshot_scn))) {
    STORAGE_LOG(WARN, "[FORKTABLE] failed to get fork snapshot", K(ret), K(table));
  } else if (fork_snapshot_scn.is_valid()
             && OB_FAIL(enter_fork_snapshot(fork_snapshot_scn, table.get_key().get_tablet_id()))) {
    STORAGE_LOG(WARN, "[FORKTABLE] failed to enter fork snapshot", K(ret), K(table));
  }
  return ret;
}

int ObAccessCtxForkGuard::enter_fork_snapshot(
    const share::SCN &fork_snapshot_scn,
    const common::ObTabletID &src_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(opened_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "[FORKTABLE] fork snapshot entered twice", K(ret));
  } else if (OB_FAIL(ctx_.enter_fork_snapshot(fork_snapshot_scn, src_tablet_id,
                                              saved_snapshot_version_, saved_fork_source_check_,
                                              saved_tablet_id_, saved_effective_tablet_id_))) {
    STORAGE_LOG(WARN, "[FORKTABLE] failed to enter fork snapshot", K(ret), K(fork_snapshot_scn));
  } else {
    opened_ = true;
  }
  return ret;
}

void ObStoreRowLockState::reset()
{
  is_locked_ = false;
  trans_version_ = SCN::min_scn();
  lock_trans_id_.reset();
  lock_data_sequence_.reset();
  lock_dml_flag_ = blocksstable::ObDmlFlag::DF_NOT_EXIST;
  is_delayed_cleanout_ = false;
  mvcc_row_ = NULL;
  trans_scn_ = SCN::max_scn();
}

OB_DEF_SERIALIZE(ObStoreRow)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              row_val_,
              flag_,
              is_get_,
              scan_index_,
              from_base_,
              row_type_flag_.flag_,
              is_sparse_row_);
  if (OB_SUCC(ret) && is_sparse_row_) {
    if (OB_ISNULL(column_ids_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sparse row's column id is null", K(ret));
    } else {
      OB_UNIS_ENCODE_ARRAY(column_ids_, row_val_.count_);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObStoreRow)
{
  int ret = OB_SUCCESS;
  int col_id_count = 0;
  LST_DO_CODE(OB_UNIS_DECODE,
              row_val_,
              flag_,
              is_get_,
              scan_index_,
              from_base_,
              row_type_flag_.flag_,
              is_sparse_row_);
  if (OB_SUCC(ret) && is_sparse_row_) {
    OB_UNIS_DECODE(col_id_count);
    if (OB_SUCC(ret)) {
      if (row_val_.count_ != col_id_count) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "column id count is not equal", K(ret), K(col_id_count),
            K(row_val_.count_));
      } else {
        OB_UNIS_DECODE_ARRAY(column_ids_, row_val_.count_);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObStoreRow)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              row_val_,
              flag_,
              is_get_,
              scan_index_,
              from_base_,
              row_type_flag_.flag_,
              is_sparse_row_);
  if (is_sparse_row_) {
    OB_UNIS_ADD_LEN_ARRAY(column_ids_, row_val_.count_);
  }
  return len;
}

void ObStoreRow::reset()
{
  row_val_.reset();
  flag_.reset();
  capacity_ = 0;
  is_get_ = false;
  from_base_ = false;
  scan_index_ = 0;
  row_type_flag_.reset();
  is_sparse_row_ = false;
  column_ids_ = NULL;
  snapshot_version_ = 0;
  group_idx_ = 0;
  trans_id_.reset();
  fast_filter_skipped_ = false;
  last_purge_ts_ = 0;
}

int ObStoreRow::deep_copy(const ObStoreRow &src, char *buf, const int64_t len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;

  if (NULL == buf || len <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(len), KP(buf));
  } else if (!src.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "src is not valid", K(ret));
  } else {
    flag_ = src.flag_;
    capacity_ = src.capacity_;
    is_get_ = src.is_get_;
    from_base_ = src.from_base_;
    scan_index_ = src.scan_index_;
    row_type_flag_ = src.row_type_flag_;
    is_sparse_row_ = src.is_sparse_row_;
    trans_id_ = src.trans_id_;
    fast_filter_skipped_ = src.fast_filter_skipped_;
    if (OB_FAIL(row_val_.deep_copy(src.row_val_, buf, len, pos))) {
      STORAGE_LOG(WARN, "failed to deep copy row_val", K(ret));
    }
  }
  return ret;
}

int64_t ObStoreRow::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;

  if (NULL != buffer && length >= 0) {

    pos += flag_.to_string(buffer + pos, length - pos);
    common::databuff_printf(buffer, length, pos, " capacity_=%ld ", capacity_);
    common::databuff_printf(buffer, length, pos, "is_get=%d ", is_get_);
    common::databuff_printf(buffer, length, pos, "from_base=%d ", from_base_);
    common::databuff_printf(buffer, length, pos, "trans_id=%ld ", trans_id_.hash());
    common::databuff_printf(buffer, length, pos, "scan_index=%ld ", scan_index_);
    common::databuff_printf(buffer, length, pos, "multi_version_row_flag=%d ", row_type_flag_.flag_);
    pos += row_type_flag_.to_string(buffer + pos, length - pos);
    common::databuff_printf(buffer, length, pos, " row_val={count=%ld,", row_val_.count_);
    common::databuff_printf(buffer, length, pos, "cells=[");
    if (NULL != row_val_.cells_) {
      for (int64_t i = 0; i < row_val_.count_; ++i) {
        common::databuff_print_obj(buffer, length, pos, row_val_.cells_[i]);
        common::databuff_printf(buffer, length, pos, ",");
      }
    }
    common::databuff_printf(buffer, length, pos, "] ");
    common::databuff_printf(buffer, length, pos, "is_sparse_row=%d ", is_sparse_row_);
    common::databuff_printf(buffer, length, pos, "snapshot_version=%ld ", snapshot_version_);
    common::databuff_printf(buffer, length, pos, "fast_filtered=%d ", fast_filter_skipped_);
    common::databuff_printf(buffer, length, pos, "group_idx_=%ld ", group_idx_);
    common::databuff_printf(buffer, length, pos, "last_purge_ts=%ld ", last_purge_ts_);
    if (is_sparse_row_ && NULL != column_ids_) {
      common::databuff_printf(buffer, length, pos, "column_id=[");
      for (int64_t i = 0; i < row_val_.count_; ++i) {
        common::databuff_printf(buffer, length, pos, "%u,", column_ids_[i]);
      }
      common::databuff_printf(buffer, length, pos, "] ");
    }
  }
  return pos;
}

int ObLockRowChecker::check_lock_row_valid(
    const ObDatumRow &row,
    const int64_t rowkey_cnt,
    bool is_memtable_iter_row_check)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.get_column_count() < rowkey_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "row count is less than rowkey count", K(row), K(rowkey_cnt));
  } else if (row.is_uncommitted_row() || is_memtable_iter_row_check) {
    bool pure_empty_row = true;
    for (int i = rowkey_cnt; pure_empty_row && i < row.get_column_count(); ++i) {
      if (!row.storage_datums_[i].is_nop()) { // not nop value
        pure_empty_row = false;
      }
    }
    if (row.is_uncommitted_row()) {
      if (!pure_empty_row) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "uncommitted lock row have normal cells", K(ret), K(row), K(rowkey_cnt));
      }
    } else if (is_memtable_iter_row_check && pure_empty_row) { // a pure lock committed row from memtable
      // a pure empty lock row from upgrade sstable need to be compatible
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "a committed lock row only have rowkey", K(ret), K(row), K(rowkey_cnt));
    }
  }
  return ret;
}

int ObLockRowChecker::check_lock_row_valid(const blocksstable::ObDatumRow &row, const ObITableReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_read_cnt = MIN(read_info.get_seq_read_column_count(), read_info.get_rowkey_count());
  if (OB_UNLIKELY(!read_info.is_valid() || row.get_column_count() < rowkey_read_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info), K(row));
  } else if (row.is_uncommitted_row()) {
    const ObColumnIndexArray &col_index = read_info.get_columns_index();
    for (int i = rowkey_read_cnt; i < row.get_column_count(); ++i) {
      if (col_index.at(i) < read_info.get_rowkey_count()) {
        // not checking rowkey col
      } else if (!row.storage_datums_[i].is_nop()) { // not nop value
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "uncommitted lock row have normal cells", K(ret), K(row),
          K(rowkey_read_cnt), K(read_info));
        break;
      }
    }
  }
  return ret;
}

}
}
