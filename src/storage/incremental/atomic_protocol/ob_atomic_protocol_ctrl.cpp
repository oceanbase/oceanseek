/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_atomic_protocol_ctrl.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;


ObAtomicProtocolCtrl::ObAtomicProtocolCtrl()
  : is_inited_(false),
    meta_key_info_(),
    sql_proxy_(nullptr),
    sql_timeout_us_(ObAtomicProtocolUtil::DEFAULT_ATOMIC_PROTOCOL_TRANS_TIMEOUT_US)
{
}

ObAtomicProtocolCtrl::~ObAtomicProtocolCtrl()
{
}

int ObAtomicProtocolCtrl::init(
    const ObSSMetaCtrlKeyInfo &meta_key_info)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("atomic protocol ctrl already init", K(ret));
  } else if (!meta_key_info.is_valid() || meta_key_info.key_.is_lock_row()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("atomic protocol ctrl init get invalid argument", K(ret), K(meta_key_info));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_trans_timeout_us(meta_key_info.tenant_id_, sql_timeout_us_))) {
    LOG_WARN("failed to get trans timeout", K(ret), K(meta_key_info));
  } else if (OB_FAIL(meta_key_info_.assign(meta_key_info))) {
    LOG_WARN("failed to assign meta key", K(ret), K(meta_key_info));
  } else {
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

int ObAtomicProtocolCtrl::cancel_atomic_protocol()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("atomic protocol ctrl do not init", K(ret));
  } else {
    ObTimeoutCtx timeout_ctx;
    const int64_t stmt_timeout = sql_timeout_us_;
    HEAP_VARS_3((ObMySQLTransaction, trans),
                (ObSSMetaCtrlLockRowValue, lock_row_value),
                (ObSSMetaCtrlValue, ctrl_value)) {
      if (OB_FAIL(start_trans_(stmt_timeout, timeout_ctx, trans))) {
        LOG_WARN("failed to start trans", K(ret), K(meta_key_info_));
      } else {
        if (OB_FAIL(lock_lock_row_(trans, lock_row_value))) {
          LOG_WARN("failed to lock lock row", K(ret));
        } else if (OB_FAIL(select_ctrl_row_(trans, ctrl_value))) {
          LOG_WARN("failed to select ctrl row", K(ret), K(meta_key_info_));
        } else if (ctrl_value.ctrl_info_.status_.is_abandon_status()) {
          //do nothing
        } else if (!ctrl_value.ctrl_info_.status_.is_start_status() && !ctrl_value.ctrl_info_.status_.is_complete_status()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctrl value ctrl info status is unexpected", K(ret), K(meta_key_info_));
        } else if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_abandon())) {
          LOG_WARN("failed to set status abandon", K(ret), K(meta_key_info_), K(ctrl_value));
        } else if (OB_FAIL(update_ctrl_row_(ctrl_value, trans))) {
          LOG_WARN("failed to update ctrl row", K(ret), K(meta_key_info_));
        }

        if (OB_TMP_FAIL(commit_trans_(ret, trans))) {
          LOG_WARN("failed to commit trans", K(tmp_ret), K(ret));
          if (OB_SUCCESS == ret) {
            ret = tmp_ret;
          }
        }
      }
    }
  }
  return ret;
}

int ObAtomicProtocolCtrl::start_trans_(
    const int64_t stmt_timeout,
    ObTimeoutCtx &timeout_ctx,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = meta_key_info_.tenant_id_;
  const bool with_snapshot = false;
  const int32_t group_id = share::OBCG_SHARED_META;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("atomic protocol ctrl do not init", K(ret));
  } else if (stmt_timeout <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start trans get invalid argument", K(ret), K(stmt_timeout), K(group_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(tenant_id, stmt_timeout, with_snapshot, group_id, timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id), K(stmt_timeout), K(group_id));
  } 
  return ret;
}

int ObAtomicProtocolCtrl::commit_trans_(
    const int32_t &result,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("atomic protocol ctrl do not init", K(ret));
  } else if (OB_TMP_FAIL(ObAtomicProtocolUtil::commit_trans(result, trans))) {
    LOG_WARN("failed to commit trans", K(tmp_ret), K(ret));
    if (OB_SUCCESS == ret) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObAtomicProtocolCtrl::lock_lock_row_(
    ObMySQLTransaction &trans,
    ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;
  lock_row_value.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("atomic protocol ctrl do not init", K(ret));
  } else if (OB_FAIL(prepare_lock_row_(lock_row))) {
    LOG_WARN("failed to prepare lock row", K(ret), K(meta_key_info_));
  } else if (OB_FAIL(lock_row.lock_lock_row(trans, lock_row_value))) {
    LOG_WARN("failed to lock lock row", K(ret), K(meta_key_info_));
  }
  return ret;
}

int ObAtomicProtocolCtrl::prepare_lock_row_(
    ObLockAtomicProtocol &lock_row)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlKey ctrl_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("atomic protocol ctrl do not init", K(ret));
  } else if (OB_FAIL(meta_key_info_.key_.get_lock_row_key(ctrl_key))) {
    LOG_WARN("failed to get lock row key", K(ret), K(meta_key_info_));
  } else if (OB_FAIL(lock_row.init(
      meta_key_info_.tenant_id_, meta_key_info_.ls_id_, ctrl_key))) {
    LOG_WARN("failed to init lock row", K(ret), K(meta_key_info_));
  }
  return ret;
}

int ObAtomicProtocolCtrl::select_ctrl_row_(
    common::ObISQLClient &trans,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  ctrl_value.reset();
  common::ObArenaAllocator allocator("APSelet");
  ObString meta_key;
  ObString meta_value;
  ObSSMetaCtrlTableOperator table_operator;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("atomic protocol ctrl do not init", K(ret));
  } else if (OB_FAIL(get_row_meta_key_(meta_key_info_.key_, allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(meta_key_info_));
  } else if (OB_FAIL(table_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init table operator", K(ret), K(meta_key_info_), K_(sql_timeout_us));
  } else if (OB_FAIL(table_operator.read_row(meta_key_info_.tenant_id_, meta_key_info_.ls_id_,
      meta_key_info_.key_.type_, meta_key, allocator, meta_value))) {
    LOG_WARN("failed to read row", K(ret), K(meta_key_info_));
  } else if (OB_FAIL(get_row_ctrl_value_(meta_value, ctrl_value))) {
    LOG_WARN("failed to get row ctrl value", K(ret), K(meta_key_info_));
  }
  return ret;
}

int ObAtomicProtocolCtrl::update_ctrl_row_(
    const ObSSMetaCtrlValue &ctrl_value,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("APUpdate");
  ObString meta_key;
  ObString meta_value;
  ObSSMetaCtrlTableOperator table_operator;
  int64_t affected_rows = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("atomic protocol ctrl do not init", K(ret));
  } else if (!ctrl_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update ctrl row get invalid argument", K(ret), K(meta_key_info_), K(ctrl_value));
  } else if (OB_FAIL(get_row_meta_key_(meta_key_info_.key_, allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(meta_key_info_));
  } else if (OB_FAIL(get_row_meta_value_(ctrl_value, allocator, meta_value))) {
    LOG_WARN("failed to get row meta value", K(ret), K(ctrl_value), K(meta_key_info_));
  } else if (OB_FAIL(table_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init table operator", K(ret), K(meta_key_info_), K_(sql_timeout_us));
  } else if (OB_FAIL(table_operator.update_row(meta_key_info_.tenant_id_, meta_key_info_.ls_id_,
      meta_key_info_.key_.type_, meta_key, meta_value, affected_rows))) {
    LOG_WARN("failed to insert row", K(ret), K(meta_key_info_), K(ctrl_value));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctrl row return affected rows unexpected", K(ret), K(meta_key_info_), K(affected_rows));
  }
  return ret;
}

int ObAtomicProtocolCtrl::get_row_meta_key_(
    const ObSSMetaCtrlKey &ctrl_key,
    common::ObIAllocator &allocator,
    ObString &meta_key)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  if (!ctrl_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get row meta key get invalid argument", K(ret), K(ctrl_key));
  } else if (OB_FAIL(ctrl_key.get_meta_key_serialize_size(buf_len))) {
    LOG_WARN("failed to get meta key serialize size", K(ret), K(ctrl_key));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_len));
  } else if (OB_FAIL(ctrl_key.serialize_meta_key(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize meta key", K(ret), K(ctrl_key));
  } else {
    meta_key.assign(buf, pos);
  }
  return ret;
}

int ObAtomicProtocolCtrl::get_row_meta_value_(
    const ObSSMetaCtrlValue &row_value,
    common::ObIAllocator &allocator,
    ObString &meta_value)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;

  if (!row_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get row value get invalid argument", K(ret), K(row_value));
  } else if (FALSE_IT(buf_len = row_value.get_serialize_size())) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_len));
  } else if (OB_FAIL(row_value.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize row value", K(ret), K(row_value), K(buf_len));
  } else {
    meta_value.assign(buf, pos);
  }
  return ret;
}

int ObAtomicProtocolCtrl::get_row_ctrl_value_(
    const ObString &meta_value,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  ctrl_value.reset();
  int64_t pos = 0;

  if (meta_value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get row ctrl value get invalid argument", K(ret), K(meta_value));
  } else if (OB_FAIL(ctrl_value.deserialize(meta_value.ptr(), meta_value.length(), pos))) {
    LOG_WARN("failed to deserialize meta value", K(ret), K(meta_value));
  }
  return ret;
}
