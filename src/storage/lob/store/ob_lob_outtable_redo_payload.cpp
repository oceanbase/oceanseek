/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/lob/store/ob_lob_outtable_redo_payload.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/checksum/ob_crc64.h"

namespace oceanbase
{
namespace storage
{

ObLobOutTableRedoPayload::ObLobOutTableRedoPayload()
{
  reset();
}

void ObLobOutTableRedoPayload::reset()
{
  lob_col_idx_ = -1;
  lob_id_.reset();
  seq_id_.reset();
  lob_piece_table_id_ = OB_INVALID_ID;
  lob_operation_seq_no_.reset();
  macro_id_.reset();
  is_logging_ = true;
  byte_len_ = 0;
  data_.reset();
}

bool ObLobOutTableRedoPayload::is_valid() const
{
  return lob_col_idx_ >= 0 && lob_id_.is_valid() && !seq_id_.empty()
      && nullptr != seq_id_.ptr()
      && 0 != lob_piece_table_id_ && OB_INVALID_ID != lob_piece_table_id_
      && lob_operation_seq_no_.is_valid() && macro_id_.is_valid() && byte_len_ > 0
      && byte_len_ <= ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE
      && (is_logging_ ? (byte_len_ == data_.length() && nullptr != data_.ptr()) : data_.empty());
}

OB_SERIALIZE_MEMBER(ObLobOutTableRedoPayload, lob_col_idx_, lob_id_, seq_id_, lob_piece_table_id_,
    lob_operation_seq_no_, macro_id_, is_logging_, byte_len_, data_);

int ObLobOutTableRedoInfo::init(const uint64_t tenant_id, const ObLobOutTableRedoPayload &payload)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  const int64_t size = payload.get_serialize_size();
  const common::ObMemAttr attr(tenant_id, "LobRedoPayload", common::ObCtxIds::TX_CALLBACK_CTX_ID);
  if (!payload.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 != redo_size_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(buf = static_cast<char *>(common::ob_malloc(size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    int64_t pos = 0;
    if (OB_FAIL(payload.serialize(buf, size, pos))) {
      LOG_WARN("failed to copy LOB redo bytes", KR(ret));
    } else if (pos != size) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      redo_buf_ = buf;
      redo_size_ = size;
    }
    if (OB_FAIL(ret)) {
      common::ob_free(buf);
    }
  }
  return ret;
}

void ObLobOutTableRedoInfo::init_replay(const common::ObString &serialized_payload)
{
  redo_size_ = serialized_payload.length();
  // Replay borrows the log buffer, so cache the digest before advancing the iterator.
  checksum_ = common::ob_crc64(serialized_payload.ptr(), serialized_payload.length());
  checksum_ready_ = true;
}

void ObLobOutTableRedoInfo::ensure_checksum_()
{
  if (!checksum_ready_ && nullptr != redo_buf_) {
    checksum_ = common::ob_crc64(redo_buf_, redo_size_);
    checksum_ready_ = true;
  }
}

void ObLobOutTableRedoInfo::release()
{
  // Submission or callback removal can precede transaction checksum calculation.
  ensure_checksum_();
  // The node is the sole owner. Callback lifecycle events and failure cleanup
  // may both arrive; clearing the pointer makes repeated cleanup harmless.
  char *buf = redo_buf_;
  redo_buf_ = nullptr;
  if (nullptr != buf) {
    common::ob_free(buf);
  }
}

void ObLobOutTableRedoInfo::checksum(common::ObBatchChecksum &bc)
{
  ensure_checksum_();
  bc.fill(&checksum_, sizeof(checksum_));
}

int ObLobOutTableRedoCtx::find_payload(
    const common::ObStoreRowkey &rowkey,
    const ObLobOutTableRedoPayload *&payload) const
{
  int ret = OB_SUCCESS;
  payload = nullptr;
  if (payloads_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob meta redo payloads are empty", KR(ret));
  } else if (!rowkey.is_valid()
      || ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT != rowkey.get_obj_cnt()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob meta redo rowkey", KR(ret), K(rowkey));
  } else {
    const common::ObObj *cells = rowkey.get_obj_ptr();
    const common::ObObj &col_idx = cells[ObLobMetaUtil::LOB_COL_IDX_COL_ID];
    const common::ObObj &lob_id = cells[ObLobMetaUtil::LOB_ID_COL_ID];
    const common::ObObj &seq_id = cells[ObLobMetaUtil::SEQ_ID_COL_ID];
    if (!col_idx.is_int() || !lob_id.is_string_type() || !seq_id.is_string_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected lob meta redo rowkey types", KR(ret), K(rowkey));
    } else {
      // Match the actual written key: batch sorting and split retries may reorder rows.
      for (int64_t i = 0; OB_SUCC(ret) && i < payloads_.count(); ++i) {
        const ObLobOutTableRedoPayload &item = payloads_.at(i);
        if (!item.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid lob meta redo payload", KR(ret), K(i));
        } else if (col_idx.get_int() == item.lob_col_idx_
            && lob_id.get_string() == item.lob_id_.get_string()
            && seq_id.get_string() == item.seq_id_) {
          if (OB_NOT_NULL(payload)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicate lob meta redo payload", KR(ret), K(i));
          } else {
            payload = &item;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(payload)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("lob meta redo payload is missing", KR(ret), K(rowkey));
      }
    }
  }
  return ret;
}

ObLobOutTableRedoInfo::ObLobOutTableRedoInfo()
  : redo_buf_(nullptr),
    redo_size_(0),
    checksum_(0),
    checksum_ready_(false)
{
}

ObLobOutTableRedoCtx::ObLobOutTableRedoCtx(
    const common::ObIArray<ObLobOutTableRedoPayload> &payloads)
  : payloads_(payloads)
{
}

} // namespace storage
} // namespace oceanbase
