/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_EXT_INFO_CALLBACK_
#define OCEANBASE_STORAGE_OB_EXT_INFO_CALLBACK_

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "share/lob/ob_lob_ext_info_log_allocator.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "storage/tx/ob_tx_seq.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{

enum ObExtInfoLogType //FARM COMPAT WHITELIST
{
  OB_INVALID_EXT_INFO_LOG = 0,
  OB_LOB_DELETE_EXT_INFO_LOG = 1, // Deprecated: 仅保留该枚举值以兼容老版本 Observer 日志，禁止复用该编号。
  OB_LOB_SUMMARY_EXT_INFO_LOG = 2,
  OB_LOB_DATA_EXT_INFO_LOG = 3,
};


struct ObExtInfoLogHeader
{
  OB_UNIS_VERSION_V(1);
public:
  ObExtInfoLogHeader(const ObExtInfoLogType type = OB_INVALID_EXT_INFO_LOG)
    : type_(type)
  {}

  ObExtInfoLogType get_type() const { return static_cast<ObExtInfoLogType>(type_); }
  void reset();

  uint8_t type_;

public:
  TO_STRING_KV(K(type_));

};

class ObLobSummaryExtInfoLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObLobSummaryExtInfoLog() { reset(); }
  ~ObLobSummaryExtInfoLog() { reset(); }
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K(lob_aux_table_id_), K(lob_operation_seq_no_),
      "seq_id", ObLobSeqId(seq_id_), K(byte_len_), K(char_len_), K(lob_info_));
public:
  uint64_t lob_aux_table_id_;
  transaction::ObTxSEQ lob_operation_seq_no_;
  ObString seq_id_;
  uint64_t byte_len_;
  uint64_t char_len_;
  ObString lob_info_;
};

class ObLobDataExtInfoLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObLobDataExtInfoLog() { reset(); }
  ~ObLobDataExtInfoLog() { reset(); }
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K(lob_aux_table_id_), K(lob_operation_seq_no_),
      "seq_id", ObLobSeqId(seq_id_), "lob_data_len", lob_data_.length());
public:
  uint64_t lob_aux_table_id_;
  transaction::ObTxSEQ lob_operation_seq_no_;
  ObString seq_id_;
  ObString lob_data_;
};

class ObExtInfoCallback : public memtable::ObITransCallback
{
public:
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_MIN_COUNT = 2;
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_COUNT = 3;
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_KEY_IDX = 0;
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_KEY_CNT = 1;
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_VALUE_IDX = 1;
  static const int32_t OB_EXT_INFO_MUTATOR_ROW_LOB_ID_IDX = 2;

public:
  ObExtInfoCallback() :
      ObITransCallback(),
      allocator_(nullptr),
      seq_no_cur_(),
      lob_id_(),
      dml_flag_(blocksstable::ObDmlFlag::DF_MAX),
      key_obj_(),
      key_(),
      rowkey_(),
      mutator_row_buf_(nullptr),
      mutator_row_len_(0)
  {}

  virtual ~ObExtInfoCallback();

  virtual memtable::MutatorType get_mutator_type() const override;
  virtual transaction::ObTxSEQ get_seq_no() const override { return seq_no_cur_; }
  virtual int64_t get_data_size() override { return mutator_row_len_; };
  virtual int log_submitted(const SCN scn, storage::ObIMemtable *&last_mt);
  virtual int release_resource();

  int get_redo(memtable::RedoDataNode &redo_node);
  int set(
      ObIAllocator &allocator,
      const blocksstable::ObDmlFlag dml_flag,
      const transaction::ObTxSEQ &seq_no_cur,
      const ObLobId &lob_id,
      ObString &data);

public:
  TO_STRING_KV(K(seq_no_cur_), K(lob_id_), K(dml_flag_), K(mutator_row_len_), KP(mutator_row_buf_));

private:
  int set_mutator_row_(const blocksstable::ObDatumRow &datum_row, const int64_t data_len);

private:
  ObIAllocator *allocator_;
  transaction::ObTxSEQ seq_no_cur_;
  ObLobId lob_id_;
  blocksstable::ObDmlFlag dml_flag_;
  ObObj key_obj_;
  memtable::ObMemtableKey key_;
  common::ObStoreRowkey rowkey_;
  char *mutator_row_buf_;
  int32_t mutator_row_len_;
};

class ObExtInfoCbRegister
{
public:
  static const int32_t OB_EXT_INFO_LOG_HEADER_LEN = 1;

public:
  ObExtInfoCbRegister():
      tmp_allocator_(lib::ObMemAttr(MTL_ID(), "ExtInfoLogReg")),
      mvcc_ctx_(nullptr)
  {}

  int register_lob_summary_cb(
      memtable::ObIMvccCtx *ctx,
      storage::ObStoreCtx &store_ctx,
      const int64_t timeout,
      const blocksstable::ObDmlFlag dml_flag,
      const ObLobId &lob_id,
      const ObLobSummaryExtInfoLog &log);

  int register_lob_data_cb(
      memtable::ObIMvccCtx *ctx,
      storage::ObStoreCtx &store_ctx,
      const int64_t timeout,
      const blocksstable::ObDmlFlag dml_flag,
      const ObLobId &lob_id,
      const ObLobDataExtInfoLog &log);

private:
  template <typename LogType>
  int serialize_lob_ext_info_log_(
      const ObExtInfoLogType type,
      const LogType &log,
      ObString &data);
  template <typename LogType>
  int register_lob_ext_info_log_cb_(
      memtable::ObIMvccCtx *ctx,
      storage::ObStoreCtx &store_ctx,
      const int64_t timeout,
      const blocksstable::ObDmlFlag dml_flag,
      const ObExtInfoLogType type,
      const ObLobId &lob_id,
      const LogType &log);
  int append_callback_with_retry(
      storage::ObStoreCtx &store_ctx,
      const blocksstable::ObDmlFlag dml_flag,
      const transaction::ObTxSEQ &seq_no_cur,
      const ObLobId &lob_id,
      ObString &data);
  int append_callback(
      storage::ObStoreCtx &store_ctx,
      const blocksstable::ObDmlFlag dml_flag,
      const transaction::ObTxSEQ &seq_no_cur,
      const ObLobId &lob_id,
      ObString &data,
      int64_t &freeze_start_time_us,
      int64_t &freeze_start_time_change_cnt,
      const int64_t retry_cnt);
  int check_is_during_freeze(
      const share::ObLSID &ls_id,
      bool &is_during_freeze,
      int64_t &freeze_start_time_us,
      int64_t &freeze_start_time_change_cnt,
      const int64_t retry_cnt);

public:
  TO_STRING_KV(KP(mvcc_ctx_));

private:
  ObArenaAllocator tmp_allocator_;
  memtable::ObIMvccCtx *mvcc_ctx_;
};

}; // end namespace memtable
}; // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_EXT_INFO_CALLBACK_ */
