/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mds_table/ob_mds_table_datum_row_converter.h"
#include "storage/mds_table/ob_mds_value_type_name.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/mds_table/ob_mds_trans_node_value.h"
#include "storage/mds_table/ob_mds_type_registry.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_nop_bitmap.h"

namespace oceanbase
{
namespace storage
{

using MdsCol = ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX;
using SchemaDef = ObMdsTabletSchemaDef;

int ObMdsTableDatumRowConverter::decode_mds_row_(
    const blocksstable::ObDatumRow &row,
    common::ObTabletID &tablet_id,
    share::SCN &transfer_scn,
    uint8_t &mds_type,
    common::ObString &mds_key,
    common::ObString &user_data,
    ObMdsValueMeta &meta,
    bool &need_commit_patch)
{
  int ret = common::OB_SUCCESS;
  const int64_t cc = row.get_column_count();
  const blocksstable::ObStorageDatum *datums = row.storage_datums_;
  if (OB_UNLIKELY(!row.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum row", K(ret), K(row));
  } else if (OB_UNLIKELY(!SchemaDef::is_known_mds_row_layout(cc))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported row layout for ls aggregate converter", K(ret), K(cc), K(row));
  } else if (OB_UNLIKELY(datums[SchemaDef::get_phy_col_idx(cc, MdsCol::MDS_VALUE)].is_nop())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("mds value column must not be nop", K(ret), K(cc), K(row));
  } else {
    user_data = datums[SchemaDef::get_phy_col_idx(cc, MdsCol::MDS_VALUE)].get_string();
    if (OB_UNLIKELY(user_data.empty())) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("mds value column must not be empty", K(ret), K(cc), K(row));
    }
    int64_t tablet_id_val = datums[SchemaDef::get_phy_col_idx(cc, MdsCol::TABLET_ID)].get_int();
    int64_t transfer_scn_val = datums[SchemaDef::get_phy_col_idx(cc, MdsCol::TRANSFER_SCN)].get_int();
    int64_t trans_id_val = datums[SchemaDef::get_phy_col_idx(cc, MdsCol::WRITER_TRANS_ID)].get_int();
    int64_t commit_version_val = datums[SchemaDef::get_phy_col_idx(cc, MdsCol::COMMIT_VERSION)].get_int();
    int64_t commit_scn_val = datums[SchemaDef::get_phy_col_idx(cc, MdsCol::COMMIT_SCN)].get_int();
    mds_type = static_cast<uint8_t>(datums[SchemaDef::get_phy_col_idx(cc, MdsCol::MDS_TYPE)].get_int());
    mds_key = datums[SchemaDef::get_phy_col_idx(cc, MdsCol::MDS_KEY)].get_string();
    need_commit_patch = SchemaDef::commit_version_hint_need_patch(commit_version_val);
    if (need_commit_patch) {
      commit_version_val = SchemaDef::normalize_commit_version_for_tx(commit_version_val);
    }
    tablet_id = common::ObTabletID(tablet_id_val);
    transfer_scn.convert_for_tx(transfer_scn_val);
    meta.init(trans_id_val, commit_version_val, commit_scn_val);
    if (need_commit_patch && !meta.is_committed()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid commit version or commit scn for commit_patch", K(ret), K(meta), K(row));
    }
  }
  return ret;
}

int ObMdsTableDatumRowConverter::convert_memtable_row(
    const memtable::ObMemtableDataHeader &hdr,
    const ObITableReadInfo &read_info,
    blocksstable::ObDatumRow &row,
    memtable::ObNopBitMap &nop_bitmap,
    bool &read_finished,
    const blocksstable::ObRowHeader *&row_header)
{
  int ret = OB_SUCCESS;
  row_header = nullptr;
  read_finished = false;
  const void *const inner = hdr.mds_inner_value_ptr();
  const ObMdsValueMeta *meta = hdr.mds_meta_ptr();
  const ObMdsTypeRegistry &registry = ObMdsTypeRegistry::get_instance();
  const ObMdsTypeRegistry::TypeOps *ops = nullptr;
  int64_t ser_size = 0;
  char *ser_buf = nullptr;
  int64_t ser_pos = 0;
  const ObColumnIndexArray &cols_index = read_info.get_memtable_columns_index();
  const int64_t schema_col_cnt = read_info.get_schema_column_count();

  int64_t writer_trans_id_val = 0;
  int64_t commit_version_val = 0;
  int64_t commit_scn_val = 0;

  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read info for mds memtable convert", KR(ret));
  } else if (OB_UNLIKELY(nullptr == meta || nullptr == inner)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mds meta or inner value ptr is null", KR(ret), KP(meta), KP(inner));
  } else if (OB_UNLIKELY(nullptr == (ops = registry.get_ops(meta->mds_type_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mds type not registered", KR(ret), K(meta->mds_type_));
  } else {
    // Validated read (seqlock): redo/commit callbacks may patch the live
    // [meta][V] record in place (ObMvccRowCallback::invoke_mds_on_redo /
    // invoke_mds_on_commit), which can change both field values and the
    // serialized length between get_serialize_size() and serialize().
    // Snapshot everything under one patch_seq_ epoch and retry on change.
    // A node is patched at most twice in its lifetime, so this converges
    // within a couple of rounds.
    //
    // NOTE: the seqlock is applied UNCONDITIONALLY, although only reads of
    // not-yet-committed nodes (read_uncommitted=true) can actually race with
    // a patch. Do NOT try to skip it for committed nodes:
    // 1. For a stable node it degenerates to two atomic loads within the
    //    meta struct this function reads anyway (patch_seq_ is at the front
    //    of ObMdsValueMeta) -- there is nothing to save.
    // 2. This function only sees the ObMemtableDataHeader; the committed
    //    state lives in the ObMvccTransNode owned by the caller. Deciding
    //    via meta->is_committed() would be UNSOUND on weak memory models:
    //    without acquire semantics the meta commit fields may be visible
    //    while the V patch stores are not. Only tnode's committed flag
    //    (acquire) would be a sound gate, which reintroduces a cross-module
    //    ordering dependency -- the exact fragility that caused the original
    //    -4016 race this seqlock fixes.
    const int64_t MAX_PATCH_RETRY_CNT = 16;
    int64_t retry_cnt = 0;
    int64_t ser_buf_cap = 0;
    bool consistent = false;
    while (OB_SUCC(ret) && !consistent) {
      const uint32_t read_seq = meta->acquire_read_seq();
      int read_ret = OB_SUCCESS;
      ser_pos = 0;
      ser_size = ops->get_serialize_size(inner);
      if (OB_UNLIKELY(ser_size <= 0)) {
        // a torn read may in principle yield a nonsense size; whether this is
        // a retryable race or a real error is decided by seq validation below
        read_ret = OB_ERR_UNEXPECTED;
      } else if (OB_UNLIKELY(ser_size > static_cast<int64_t>(SchemaDef::MDS_VALUE_COLUMN_LENGTH))) {
        read_ret = OB_BUF_NOT_ENOUGH;
      } else if (ser_size > ser_buf_cap) {
        // First round, or a rare concurrent patch enlarged the value: grow.
        // The buffer is reused across retries otherwise; an abandoned smaller
        // buffer is intentionally left to the arena (ObArenaAllocator::free()
        // is a no-op, memory is reclaimed with the row), and the number of
        // grow rounds is bounded by MAX_PATCH_RETRY_CNT.
        if (OB_ISNULL(ser_buf = static_cast<char *>(row.local_allocator_.alloc(ser_size)))) {
          // allocation failure is never race induced, fail immediately
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc serialize buffer for mds read failed", KR(ret), K(ser_size));
        } else {
          ser_buf_cap = ser_size;
        }
      }
      if (OB_FAIL(ret) || OB_SUCCESS != read_ret) {
        // fall through to the seq validation / error handling below
      } else if (OB_SUCCESS != (read_ret = ops->serialize(inner, ser_buf, ser_size, ser_pos))) {
        // may be caused by a concurrent patch, judged by seq validation below
      } else if (OB_UNLIKELY(ser_pos != ser_size)) {
        read_ret = OB_ERR_UNEXPECTED;
      } else {
        meta->convert_to_int64(writer_trans_id_val, commit_version_val, commit_scn_val);
      }

      if (OB_FAIL(ret)) {
        // real error already set above
      } else if (OB_UNLIKELY(!meta->validate_read_seq(read_seq))) {
        // torn read caused by concurrent on_redo/on_commit patch, discard and retry
        if (OB_UNLIKELY(++retry_cnt >= MAX_PATCH_RETRY_CNT)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("mds live value keeps changing during read", KR(ret), K(retry_cnt), KPC(meta));
        }
      } else if (OB_UNLIKELY(OB_SUCCESS != read_ret)) {
        // seq validated: the failure happened on a stable record, a real error
        ret = read_ret;
        const char *mds_value_type_name = get_mds_value_type_name(meta->mds_type_);
        if (OB_BUF_NOT_ENOUGH == ret) {
          LOG_WARN("mds serialized size out of range", KR(ret), K(ser_size));
        } else {
          LOG_ERROR("mds serialize for memtable read failed",
                    KR(ret),
                    K(meta->mds_type_),
                    K(mds_value_type_name),
                    K(ser_pos),
                    K(ser_size));
        }
      } else {
        consistent = true;
      }
    }
  }
  for (int64_t idx = 0; OB_SUCC(ret) && idx < row.count_; ++idx) {
    if (!nop_bitmap.test(idx)) {
      continue;
    }
    const int64_t store_idx = cols_index.at(idx);
    if (store_idx < 0 || store_idx >= schema_col_cnt) {
      // Out-of-schema column: leave it nop, same as before.
      continue;
    }
    switch (static_cast<MdsCol>(store_idx)) {
    case MdsCol::TABLET_ID:
    case MdsCol::TRANSFER_SCN:
    case MdsCol::MDS_TYPE:
    case MdsCol::MDS_KEY:
      // Rowkey columns: data is materialized from the memtable key path,
      // so the read here just clears the nop marker.
      nop_bitmap.set_false(idx);
      break;
    case MdsCol::WRITER_TRANS_ID:
      row.storage_datums_[idx].set_int(writer_trans_id_val);
      nop_bitmap.set_false(idx);
      break;
    case MdsCol::COMMIT_VERSION:
      row.storage_datums_[idx].set_int(commit_version_val);
      nop_bitmap.set_false(idx);
      break;
    case MdsCol::COMMIT_SCN:
      row.storage_datums_[idx].set_int(commit_scn_val);
      nop_bitmap.set_false(idx);
      break;
    case MdsCol::MDS_VALUE:
      if (OB_FAIL(row.storage_datums_[idx].from_buf_enhance(ser_buf, ser_size))) {
        LOG_WARN("set mds value datum from serialized buf failed", KR(ret), K(idx), K(ser_size));
      } else {
        nop_bitmap.set_false(idx);
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected store index", KR(ret), K(store_idx));
      break;
    }
  }

  if (OB_SUCC(ret)) {
    row.row_flag_.set_flag(static_cast<blocksstable::ObDmlFlag>(hdr.dml_flag_));
    read_finished = nop_bitmap.is_empty() || blocksstable::ObDmlFlag::DF_DELETE == hdr.dml_flag_
                    || blocksstable::ObDmlFlag::DF_INSERT == hdr.dml_flag_;
  }
  return ret;
}

namespace {
void fill_datum_row_commit_meta_columns_(
    blocksstable::ObDatumRow &row,
    const transaction::ObTransID &trans_id,
    const int64_t trans_version,
    const int64_t commit_scn_val)
{
  const int64_t col_cnt = row.get_column_count();
  const int64_t writer_trans_id_idx = SchemaDef::get_phy_col_idx(col_cnt, MdsCol::WRITER_TRANS_ID);
  const int64_t commit_version_idx = SchemaDef::get_phy_col_idx(col_cnt, MdsCol::COMMIT_VERSION);
  const int64_t commit_scn_idx = SchemaDef::get_phy_col_idx(col_cnt, MdsCol::COMMIT_SCN);
  const int64_t writer_trans_id = row.storage_datums_[writer_trans_id_idx].get_int();
  if (writer_trans_id == trans_id.get_id()) {
    row.storage_datums_[commit_version_idx].set_int(trans_version);
    row.storage_datums_[commit_scn_idx].set_int(commit_scn_val);
  }
}

} // namespace

int ObMdsTableDatumRowConverter::resolve_meta_column(
    blocksstable::ObDatumRow &row,
    const transaction::ObTransID &trans_id,
    const int64_t trans_version,
    const int64_t prepare_version,
    const share::SCN &end_scn,
    const transaction::ObTxState tx_state)
{
  int ret = OB_SUCCESS;
  if (tx_state >= transaction::ObTxState::COMMIT) {
    if (!end_scn.is_valid() || !transaction::is_effective_trans_version(trans_version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("end_scn/trans_version is invalid for committed row", K(ret), K(end_scn), K(row), K(trans_id), K(trans_version), K(prepare_version), K(tx_state));
    } else {
      const int64_t mds_type_idx = SchemaDef::get_phy_col_idx(row.get_column_count(), MdsCol::MDS_TYPE);
      const uint16_t mds_type = static_cast<uint16_t>(row.storage_datums_[mds_type_idx].get_int());
      const bool need_patch_mds_value = ObMdsTypeRegistry::get_instance().has_on_commit(mds_type);
      const int64_t commit_version = need_patch_mds_value ? -trans_version : trans_version;
      const int64_t commit_scn_val = end_scn.get_val_for_tx();
      fill_datum_row_commit_meta_columns_(row, trans_id, commit_version, commit_scn_val);
    }
  } else if (!row.is_have_uncommited_row() || row.snapshot_version_ != INT64_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row is not uncommitted row", K(ret), K(row), K(trans_id), K(trans_version), K(prepare_version), K(end_scn), K(tx_state));
  } else {
    // In-flight tx: COMMIT_VERSION column is best-effort populated with
    // prepare_version (when known) so readers can still order the in-flight
    // row alongside committed siblings. trans_version_ is intentionally min
    // for undecided states under lock_for_read, so we read prepare_version.
    const int64_t commit_version = prepare_version;
    const int64_t commit_scn_val = -(int64_t)tx_state;
    fill_datum_row_commit_meta_columns_(row, trans_id, commit_version, commit_scn_val);
  }
  return ret;
}

int ObMdsTableDatumRowConverter::apply_uncommitted_state_to_datum_row(
    blocksstable::ObDatumRow &row,
    const transaction::ObTransID &row_tx_id,
    const transaction::ObTransID &tx_id,
    const share::SCN &prepare_version,
    const transaction::ObTxState &tx_state)
{
  int ret = OB_SUCCESS;
  share::SCN ignored_end_scn;
  ignored_end_scn.set_invalid();
  const int64_t ignored_trans_version = 0;
  if (row.snapshot_version_ != INT64_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row snapshot version is not max", K(ret), K(row.snapshot_version_), K(row), K(prepare_version), K(tx_state));
  } else if (tx_state == transaction::ObTxState::COMMIT || tx_state == transaction::ObTxState::ABORT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected tx state for uncommitted row", K(ret), K(tx_state), K(row), K(prepare_version), K(row));
  } else if (row_tx_id != tx_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("iter start is aborted, expected iter previous row is committed, but got uncommitted",
              K(ret), K(row_tx_id), K(tx_id), K(row), K(prepare_version), K(tx_state));
  } else {
    const int64_t writer_tx_id_idx =
        ObMdsTabletSchemaDef::get_phy_col_idx(row.get_column_count(), MdsCol::WRITER_TRANS_ID);
    const transaction::ObTransID writer_tx_id(row.storage_datums_[writer_tx_id_idx].get_int());
    if (writer_tx_id != tx_id) {
      // uncommitted row:
      // but the writer tx id is differ, this means it come from relocation (transfer/split etc.)
      // overwrite the writer tx id to the trans node's tx id, the reader can see real writter
      row.storage_datums_[writer_tx_id_idx].set_int(tx_id.get_id());
    }
    if (OB_FAIL(resolve_meta_column(row, tx_id, ignored_trans_version,
                                    prepare_version.get_val_for_tx(),
                                    ignored_end_scn, tx_state))) {
      LOG_WARN("resolve meta column failed", KR(ret), K(row), K(prepare_version), K(tx_state));
    }
  }
  return ret;
}

void ObMdsTableDatumRowConverter::patch_value_from_row_if_needed(
    const blocksstable::ObDatumRow &row,
    const ObMdsTypeRegistry::TypeOps &ops,
    void *value_buf)
{
  if (ops.has_on_commit) {
    using SchemaCol = ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX;
    const int64_t cc = row.get_column_count();
    const int64_t cv_idx = ObMdsTabletSchemaDef::get_phy_col_idx(cc, SchemaCol::COMMIT_VERSION);
    const int64_t cv_val = row.storage_datums_[cv_idx].get_int();
    if (ObMdsTabletSchemaDef::commit_version_hint_need_patch(cv_val)) {
      share::SCN cv;
      cv.convert_for_tx(ObMdsTabletSchemaDef::normalize_commit_version_for_tx(cv_val));
      share::SCN cs;
      const int64_t cs_idx = ObMdsTabletSchemaDef::get_phy_col_idx(cc, SchemaCol::COMMIT_SCN);
      cs.convert_for_tx(row.storage_datums_[cs_idx].get_int());
      ops.on_commit(value_buf, cv, cs);
    }
  }
}

} // namespace storage
} // namespace oceanbase
