/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/tx/ob_tx_seq.h"
#define USING_LOG_PREFIX STORAGE

#include "ob_ext_info_callback.h"
#include "logservice/palf/log_define.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/ls/ob_freezer.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{
//==================== ObExtInfoLobHeader ====================
OB_SERIALIZE_MEMBER(ObExtInfoLogHeader,
                    type_); 

void ObExtInfoLogHeader::reset()
{
  type_ = ObExtInfoLogType::OB_INVALID_EXT_INFO_LOG;
}

//==================== ObLobSummaryExtInfoLog ====================
OB_SERIALIZE_MEMBER(ObLobSummaryExtInfoLog,
                    lob_aux_table_id_,
                    lob_operation_seq_no_,
                    seq_id_,
                    byte_len_,
                    char_len_,
                    lob_info_);

void ObLobSummaryExtInfoLog::reset()
{
  lob_aux_table_id_ = OB_INVALID_ID;
  lob_operation_seq_no_.reset();
  seq_id_.reset();
  byte_len_ = 0;
  char_len_ = 0;
  lob_info_.reset();
}

bool ObLobSummaryExtInfoLog::is_valid() const
{
  char summary_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString summary_seq_id(sizeof(summary_seq_buf), summary_seq_buf);
  return (OB_INVALID_ID != lob_aux_table_id_)
      && lob_operation_seq_no_.is_valid()
      && (OB_SUCCESS == ObLobSeqId::get_summary_seq_id(summary_seq_id))
      && (seq_id_ == summary_seq_id)
      && (char_len_ <= byte_len_)
      && !lob_info_.empty();
}

//==================== ObLobDataExtInfoLog ====================
OB_SERIALIZE_MEMBER(ObLobDataExtInfoLog,
                    lob_aux_table_id_,
                    lob_operation_seq_no_,
                    seq_id_,
                    lob_data_,
                    is_logging_);

void ObLobDataExtInfoLog::reset()
{
  lob_aux_table_id_ = OB_INVALID_ID;
  lob_operation_seq_no_.reset();
  seq_id_.reset();
  lob_data_.reset();
  is_logging_ = true;
}

bool ObLobDataExtInfoLog::is_valid() const
{
  char summary_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString summary_seq_id(sizeof(summary_seq_buf), summary_seq_buf);
  const ObLobSeqId seq_id(seq_id_);
  return (OB_INVALID_ID != lob_aux_table_id_)
      && lob_operation_seq_no_.is_valid()
      && (OB_SUCCESS == ObLobSeqId::get_summary_seq_id(summary_seq_id))
      && seq_id.is_valid()
      && (seq_id.compare(summary_seq_id) > 0)
      && (is_logging_ ? !lob_data_.empty() : lob_data_.empty());
}

//==================== ObExtInfoCallback ====================
ObExtInfoCallback::~ObExtInfoCallback()
{
  if (OB_NOT_NULL(mutator_row_buf_)) {
    allocator_->free(mutator_row_buf_);
    mutator_row_buf_ = nullptr;
    mutator_row_len_ = 0;
  }
}

memtable::MutatorType ObExtInfoCallback::get_mutator_type() const
{
  return memtable::MutatorType::MUTATOR_ROW_EXT_INFO;
}

int ObExtInfoCallback::log_submitted(const SCN scn, storage::ObIMemtable *&last_mt)
{
  return release_resource();
}

int ObExtInfoCallback::release_resource()
{
  int ret = OB_SUCCESS;
  if (nullptr == mutator_row_buf_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mutator_row_buf is null", K(ret), KPC(this));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), KPC(this));
  } else {
    allocator_->free(mutator_row_buf_);
    mutator_row_buf_ = nullptr;
    mutator_row_len_ = 0;
  }
  return ret;
}

int ObExtInfoCallback::get_redo(memtable::RedoDataNode &redo_node)
{
  int ret = OB_SUCCESS;
  memtable::ObRowData old_row;
  memtable::ObRowData new_row;
  ObTabletID tablet_id;
  if (OB_ISNULL(mutator_row_buf_) || mutator_row_len_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mutator_row_buf is empty", K(ret), KP(mutator_row_buf_), K(mutator_row_len_));
  } else {
    new_row.set(mutator_row_buf_, mutator_row_len_);
    redo_node.set(&key_,
                  old_row,
                  new_row,
                  dml_flag_,
                  1, /* modify_count */
                  0, /* acc_checksum */
                  0, /* version */
                  0, /* flag */
                  seq_no_cur_,
                  tablet_id,
                  OB_EXT_INFO_MUTATOR_ROW_COUNT);
    redo_node.set_callback(this);
  }
  return ret;
}

int ObExtInfoCallback::set(
    ObIAllocator &allocator,
    const blocksstable::ObDmlFlag dml_flag,
    const transaction::ObTxSEQ &seq_no_cur,
    const ObLobId &lob_id,
    ObString &data)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  blocksstable::ObDatumRow datum_row;

  dml_flag_ = dml_flag;
  seq_no_cur_ = seq_no_cur;
  lob_id_ = lob_id;
  allocator_ = &(lob_mngr->get_ext_info_log_allocator());

  key_obj_.reset();
  rowkey_.reset();
  key_.reset();
  key_obj_.set_uint64(seq_no_cur_.cast_to_int());
  rowkey_.assign(&key_obj_, OB_EXT_INFO_MUTATOR_ROW_KEY_CNT);
  if (data.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is empty", K(ret));
  } else if (OB_FAIL(key_.encode(&rowkey_))) {
    LOG_WARN("encode memtable key failed", K(ret), K(rowkey_));
  } else if (OB_NOT_NULL(mutator_row_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mutator_row_buf is not null", K(ret), KP(mutator_row_buf_));
  } else if (OB_FAIL(datum_row.init(allocator, OB_EXT_INFO_MUTATOR_ROW_COUNT))) {
    LOG_WARN("init datum row fail", K(ret));
  } else if (OB_FAIL(datum_row.storage_datums_[OB_EXT_INFO_MUTATOR_ROW_KEY_IDX].from_obj_enhance(key_obj_))) {
    LOG_WARN("set datum fail", K(ret), K(data));
  } else if (OB_FALSE_IT(datum_row.storage_datums_[OB_EXT_INFO_MUTATOR_ROW_VALUE_IDX].set_string(data))) {
  } else if (OB_FALSE_IT(datum_row.storage_datums_[OB_EXT_INFO_MUTATOR_ROW_LOB_ID_IDX]
                             .set_string(lob_id_.get_string()))) {
  } else if (OB_FAIL(set_mutator_row_(datum_row, data.length()))) {
    LOG_WARN("set ext info mutator row failed", KR(ret), K(data.length()));
  }
  return ret;
}

int ObExtInfoCallback::set_mutator_row_(
    const blocksstable::ObDatumRow &datum_row,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  // 仅作初始预留，不依赖具体行格式；编码空间不足时再扩容。
  const int64_t INITIAL_ROW_EXTRA_SIZE = 1024;
  int64_t buf_len = 0;
  if (OB_ISNULL(allocator_) || OB_UNLIKELY(!datum_row.is_valid()
      || (OB_EXT_INFO_MUTATOR_ROW_COUNT != datum_row.count_) || (data_len <= 0)
      || (data_len > static_cast<int64_t>(palf::MAX_LOG_BODY_SIZE)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ext info mutator row", KR(ret), KP(allocator_), K(datum_row.count_), K(data_len));
  } else if (OB_NOT_NULL(mutator_row_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mutator_row_buf is not null", KR(ret), KP(mutator_row_buf_));
  } else {
    // 预留量超出日志上限时截到上限，是否装得下由 writer 判断。
    buf_len = MIN(data_len + INITIAL_ROW_EXTRA_SIZE, static_cast<int64_t>(palf::MAX_LOG_BODY_SIZE));
  }

  SMART_VAR(blocksstable::ObRowWriterV0, row_writer) {
    bool write_done = false;
    while (OB_SUCC(ret) && !write_done) {
      char *buf = nullptr;
      int64_t pos = 0;
      row_writer.reset();
      if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc mutator row buffer failed", KR(ret), K(buf_len));
      } else if (OB_FAIL(row_writer.write(OB_EXT_INFO_MUTATOR_ROW_KEY_CNT, datum_row, buf, buf_len, pos))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("write ext info mutator row failed", KR(ret), K(buf_len), K(pos));
        }
      } else if (OB_UNLIKELY((pos <= 0) || (pos > buf_len))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ext info mutator row length", KR(ret), K(buf_len), K(pos));
      } else {
        // 仅成功后转移所有权，记录实际行长（pos）而非缓冲区容量（buf_len）。
        mutator_row_buf_ = buf;
        mutator_row_len_ = static_cast<int32_t>(pos);
        buf = nullptr;
        write_done = true;
      }
      if (OB_NOT_NULL(buf)) {
        allocator_->free(buf);
        buf = nullptr;
      }
      if (OB_BUF_NOT_ENOUGH == ret) {
        if (static_cast<int64_t>(palf::MAX_LOG_BODY_SIZE) == buf_len) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("ext info mutator row size overflow", KR(ret), K(buf_len));
        } else {
          // 释放本次失败的缓冲区后扩容，下次从头编码；容量不能超过单条日志 body 上限。
          buf_len = MIN(buf_len * 2, static_cast<int64_t>(palf::MAX_LOG_BODY_SIZE));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObExtInfoCbRegister::register_lob_summary_cb(
    memtable::ObIMvccCtx *ctx,
    storage::ObStoreCtx &store_ctx,
    const int64_t timeout,
    const blocksstable::ObDmlFlag dml_flag,
    const ObLobId &lob_id,
    const ObLobSummaryExtInfoLog &log)
{
  return register_lob_ext_info_log_cb_(ctx, store_ctx, timeout, dml_flag,
      OB_LOB_SUMMARY_EXT_INFO_LOG, lob_id, log);
}

int ObExtInfoCbRegister::register_lob_data_cb(
    memtable::ObIMvccCtx *ctx,
    storage::ObStoreCtx &store_ctx,
    const int64_t timeout,
    const blocksstable::ObDmlFlag dml_flag,
    const ObLobId &lob_id,
    const ObLobDataExtInfoLog &log)
{
  return register_lob_ext_info_log_cb_(ctx, store_ctx, timeout, dml_flag,
      OB_LOB_DATA_EXT_INFO_LOG, lob_id, log);
}

template <typename LogType>
int ObExtInfoCbRegister::serialize_lob_ext_info_log_(
    const ObExtInfoLogType type,
    const LogType &log,
    ObString &data)
{
  int ret = OB_SUCCESS;
  ObExtInfoLogHeader header(type);
  const int64_t buf_size = header.get_serialize_size() + log.get_serialize_size();
  char *buf = nullptr;
  int64_t pos = 0;
  data.reset();
  tmp_allocator_.reuse();
  if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ext info log", KR(ret), K(type), K(log));
  } else if (OB_UNLIKELY((buf_size <= 0)
      || (buf_size > static_cast<int64_t>(palf::MAX_LOG_BODY_SIZE)))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("lob ext info log size overflow", KR(ret), K(type), K(buf_size), K(log));
  } else if (OB_ISNULL(buf = static_cast<char *>(tmp_allocator_.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate lob ext info log buffer failed", KR(ret), K(type), K(buf_size));
  } else if (OB_FAIL(header.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize lob ext info log header failed", KR(ret), K(header), K(log));
  } else if (OB_FAIL(log.serialize(buf, buf_size, pos))) {
    LOG_WARN("serialize lob ext info log body failed", KR(ret), K(type), K(log));
  } else if (OB_UNLIKELY(pos != buf_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob ext info log size", KR(ret), K(type), K(pos), K(buf_size), K(log));
  } else {
    data.assign_ptr(buf, static_cast<int32_t>(buf_size));
  }
  return ret;
}

template <typename LogType>
int ObExtInfoCbRegister::register_lob_ext_info_log_cb_(
    memtable::ObIMvccCtx *ctx,
    storage::ObStoreCtx &store_ctx,
    const int64_t timeout,
    const blocksstable::ObDmlFlag dml_flag,
    const ObExtInfoLogType type,
    const ObLobId &lob_id,
    const LogType &log)
{
  int ret = OB_SUCCESS;
  ObString data;
  ObLobManager *lob_mgr = MTL(ObLobManager *);
  if (OB_FAIL(serialize_lob_ext_info_log_(type, log, data))) {
    LOG_WARN("serialize lob ext info log failed", KR(ret), K(type), K(lob_id), K(log));
  } else if (OB_ISNULL(mvcc_ctx_ = ctx) || OB_ISNULL(lob_mgr)
      || OB_UNLIKELY(!lob_id.is_valid()
      || !log.lob_operation_seq_no_.is_valid()
      || data.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ext info callback argument", KR(ret), KP(ctx), KP(lob_mgr),
        K(type), K(lob_id), K(log), K(data.length()));
  } else {
    ObLobExtInfoLogThrottleGuard throttle_guard(
        timeout, &(lob_mgr->get_ext_info_log_throttle_tool()));
    if (OB_FAIL(append_callback_with_retry(
        store_ctx, dml_flag, log.lob_operation_seq_no_, lob_id, data))) {
      LOG_WARN("append lob ext info callback failed", KR(ret), K(type), K(lob_id), K(log));
    }
  }
  return ret;
}

int ObExtInfoCbRegister::append_callback_with_retry(
    storage::ObStoreCtx &store_ctx,
    const blocksstable::ObDmlFlag dml_flag,
    const transaction::ObTxSEQ &seq_no_cur,
    const ObLobId &lob_id,
    ObString &data)
{
  int ret = OB_SUCCESS;
  static const int64_t SLEEP_INTERVAL_US = 10L * 1000L; //10ms
  static const int64_t MAX_SLEEP_INTERVAL_US = 50L * 1000L; //50ms
  static const int64_t tigger_sleep_retry_cnt = 5;
  int64_t retry_cnt = 0;
  int64_t freeze_start_time_us = 0;
  int64_t freeze_start_time_change_cnt = 0;
  do {
    ret = OB_SUCCESS;
    if (retry_cnt > tigger_sleep_retry_cnt) {
      const int64_t timeout_remain_us = THIS_WORKER.get_timeout_remain();
      if (OB_UNLIKELY(timeout_remain_us <= 0)) {
        ret = OB_TIMEOUT;
        LOG_WARN("worker timeout", KR(ret), K(THIS_WORKER.get_timeout_ts()),
            K(retry_cnt), K(timeout_remain_us));
      } else {
        ob_usleep(OB_MIN(OB_MIN(SLEEP_INTERVAL_US * (retry_cnt - tigger_sleep_retry_cnt),
                                MAX_SLEEP_INTERVAL_US),
                         timeout_remain_us));
      }
    }
    if (OB_SUCC(ret)) {
      const lib::Worker::Status worker_status = THIS_WORKER.check_wait();
      if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
        ret = OB_TIMEOUT;
        LOG_WARN("worker timeout before appending callback", KR(ret),
            K(THIS_WORKER.get_timeout_ts()), K(retry_cnt));
      } else if (IS_INTERRUPTED()) {
        common::ObInterruptCode code = GET_INTERRUPT_CODE();
        ret = code.code_;
        LOG_WARN("received an interrupt before appending callback", K(code), KR(ret), K(retry_cnt));
      } else if (lib::Worker::WS_OUT_OF_THROTTLE == worker_status) {
        ret = OB_KILLED_BY_THROTTLING;
        LOG_INFO("retry is interrupted by worker check wait", K(ret), K(retry_cnt));
      } else {
        ++retry_cnt;
        if (OB_FAIL(append_callback(store_ctx, dml_flag, seq_no_cur, lob_id, data,
                        freeze_start_time_us, freeze_start_time_change_cnt, retry_cnt))) {
          if (OB_NEED_RETRY != ret) {
            LOG_WARN("set row callback failed", KR(ret), K(dml_flag), K(seq_no_cur),
                K(lob_id), K(freeze_start_time_us), K(freeze_start_time_change_cnt), K(retry_cnt));
          }
        }
      }
    }
  } while (OB_NEED_RETRY == ret);
  return ret;
}

int ObExtInfoCbRegister::append_callback(
    storage::ObStoreCtx &store_ctx,
    const blocksstable::ObDmlFlag dml_flag,
    const transaction::ObTxSEQ &seq_no_cur,
    const ObLobId &lob_id,
    ObString &data,
    int64_t &freeze_start_time_us,
    int64_t &freeze_start_time_change_cnt,
    const int64_t retry_cnt)
{
  int ret = OB_SUCCESS;
  memtable::ObMvccWriteGuard guard(false);
  storage::ObExtInfoCallback *cb = nullptr;
  bool is_during_freeze = false;
  if (OB_FAIL(check_is_during_freeze(store_ctx.ls_id_, is_during_freeze, freeze_start_time_us,
                  freeze_start_time_change_cnt, retry_cnt))) {
    LOG_WARN("check is during freeze failed", KR(ret), "ls_id", store_ctx.ls_id_, K(is_during_freeze),
        K(freeze_start_time_us), K(freeze_start_time_change_cnt), K(retry_cnt));
  } else if (is_during_freeze) {
    ret = OB_NEED_RETRY;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      LOG_WARN("during freeze, we need wait freeze finished", KR(ret), "ls_id", store_ctx.ls_id_,
          K(freeze_start_time_us), K(freeze_start_time_change_cnt), K(retry_cnt));
    }
  } else if (OB_ISNULL(cb = mvcc_ctx_->alloc_ext_info_callback())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc row callback failed", K(ret));
  } else if (OB_FAIL(cb->set(tmp_allocator_, dml_flag, seq_no_cur, lob_id, data))) {
    LOG_WARN("set row callback failed", K(ret), K(*cb));
  } else if (OB_FAIL(guard.write_auth(store_ctx))) {
    LOG_WARN("write_auth fail", K(ret), K(store_ctx));
  } else if (OB_FAIL(mvcc_ctx_->append_callback(cb))) {
    LOG_WARN("register ext info callback failed", K(ret), K(*this),K(*cb));
  } else {
    guard.set_is_lob_ext_info_log(true);
    LOG_DEBUG("append ext info callback success", KPC(cb), K(seq_no_cur));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(cb)) {
    // append callback fail, need free to avoid memory leak
    mvcc_ctx_->free_ext_info_callback(cb);
    LOG_WARN("append fail, free callback", K(ret), K(seq_no_cur));
  }
  return ret;
}

// 背景：LS freeze → 提交旧 redo → memtable dump → checkpoint 推进 → 旧 clog 才可能回收
//     （freeze 要提交排在被冻结 memtable 的行 redo 之前的 ext-info log）
// 问题：如果 freeze 期间仍持续产生 ext-info log，可能造成：
//      clog 空间紧张 → freeze 提交 redo 困难 → checkpoint 难推进 → clog 难回收 → 空间继续紧张
// 缓解办法：检测到有日志流冻结时，等日志流冻结完成后，再写 ext-info log，避免导致日志流冻结完不成了，继而导致 clog 盘爆。
//         但遇到租户合并时，由于租户合并会给每一个tablet触发一次冻结，在极端情况下，每次检查时，都发现正在冻结
//        （其实是新一轮冻结），继而导致 ext-info log 写一直等待。考虑到 freeze flag 与 freeze stat 非原子更新，
//         确认 freeze start time 发生两次非零变化后，才停止等待（第一次非零变化，不一定是真的又发生了新一轮冻结）。
int ObExtInfoCbRegister::check_is_during_freeze(
    const share::ObLSID &ls_id,
    bool &is_during_freeze,
    int64_t &freeze_start_time_us,
    int64_t &freeze_start_time_change_cnt,
    const int64_t retry_cnt)
{
  int ret = OB_SUCCESS;
  static const int64_t SKIP_FREEZE_RETRY_CNT = 5;
  static const int64_t SKIP_FREEZE_START_TIME_CHANGE_CNT = 2;
  ObLSHandle ls_handle;
  ObFreezer *freezer = nullptr;
  is_during_freeze = false;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls failed", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K(ls_id));
  } else if (ls_handle.get_ls()->is_logonly_replica()) {
    is_during_freeze = false;
    LOG_TRACE("logonly replica do not need to freeze", K(ls_id));
  } else if (OB_ISNULL(freezer = ls_handle.get_ls()->get_freezer())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get freezer failed", KR(ret), K(ls_id));
  } else {
    if (!freezer->is_freeze(freezer->get_freeze_flag())) {
      is_during_freeze = false;
    } else {
      const int64_t cur_freeze_start_time_us = freezer->get_stat().get_start_time();
      if (0 == freeze_start_time_us) {
        freeze_start_time_us = cur_freeze_start_time_us;
      } else if ((0 != cur_freeze_start_time_us)
                 && (freeze_start_time_us != cur_freeze_start_time_us)) {
        freeze_start_time_us = cur_freeze_start_time_us;
        ++freeze_start_time_change_cnt;
      }
      if ((retry_cnt >= SKIP_FREEZE_RETRY_CNT)
          && (freeze_start_time_change_cnt >= SKIP_FREEZE_START_TIME_CHANGE_CNT)) {
        is_during_freeze = false;
        LOG_INFO("freeze start time changed enough times after enough retries, stop waiting freeze",
                 K(freeze_start_time_us), K(freeze_start_time_change_cnt), K(retry_cnt));
      } else {
        is_during_freeze = true;
      }
    }
  }
  return ret;
}

}; // end namespace storage
}; // end namespace oceanbase
