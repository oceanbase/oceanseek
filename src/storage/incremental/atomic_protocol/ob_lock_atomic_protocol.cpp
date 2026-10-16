/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_lock_atomic_protocol.h"
#include "share/ob_cluster_version.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

ObLockAtomicProtocol::ObLockAtomicProtocol()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    meta_ctrl_key_(),
    sql_timeout_us_(ObAtomicProtocolUtil::DEFAULT_ATOMIC_PROTOCOL_TRANS_TIMEOUT_US)
{
}

ObLockAtomicProtocol::~ObLockAtomicProtocol()
{
}

int ObLockAtomicProtocol::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &meta_key)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("loc atomic protocol already init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !meta_key.is_valid()
      || !meta_key.is_lock_row()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lock atomic protocol init get invalid argument", K(ret), K(tenant_id), K(ls_id), K(meta_key));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_trans_timeout_us(tenant_id, sql_timeout_us_))) {
    LOG_WARN("failed to get trans timeout", K(ret), K(tenant_id));
  } else if (OB_FAIL(meta_ctrl_key_.assign(meta_key))) {
    LOG_WARN("failed to assign meta key", K(ret), K(tenant_id_), K(ls_id), K(meta_key));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    is_inited_ = true;
  }
  return ret;
}

int ObLockAtomicProtocol::check_and_create_lock_row(
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlTableOperator ctrl_operator;
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  bool row_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol get invalid argument", K(ret));
  } else if (OB_FAIL(ctrl_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init ctrl operator", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_), K_(sql_timeout_us));
  } else if (OB_FAIL(get_row_meta_key_(allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(ctrl_operator.read_row(tenant_id_, ls_id_, meta_ctrl_key_.type_, meta_key, allocator, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to read row", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
    } else {
      row_exist = false;
      ret = OB_SUCCESS;
    }
  } else {
    row_exist = true;
  }

  if (OB_FAIL(ret)) {
  } else if (row_exist) {
    //do nothing
  } else {
    ObSSMetaCtrlLockRowValue row_value;
    row_value.meta_scn_.set_min();
    row_value.deleted_scn_.reset();
    row_value.is_mark_deleted_ = false;
    row_value.op_id_ = 0;
    int64_t affected_rows = 0;
    if (OB_FAIL(get_row_meta_value_(row_value, allocator, meta_value))) {
      LOG_WARN("failed to get row value", K(ret), K(row_value));
    } else if (OB_FAIL(ctrl_operator.insert_row(
        tenant_id_, ls_id_, meta_ctrl_key_.type_, meta_key, meta_value, affected_rows))) {
      LOG_WARN("failed to insert lock row", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
    } else if (1 != affected_rows) {
      ret = OB_NEED_RETRY;
      LOG_WARN("insert lock row affected rows is not 1, need retry", K(ret),
          K(affected_rows), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
    }
  }
  return ret;
}


int ObLockAtomicProtocol::get_row_meta_key_(
    common::ObIAllocator &allocator,
    ObString &meta_value)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol get invalid argument", K(ret));
  } else if (OB_FAIL(meta_ctrl_key_.get_meta_key_serialize_size(buf_len))) {
    LOG_WARN("failed to get meta key serialize size", K(ret), K(meta_ctrl_key_));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_len));
  } else if (OB_FAIL(meta_ctrl_key_.serialize_meta_key(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize meta key", K(ret), K(meta_ctrl_key_));
  } else {
    meta_value.assign(buf, pos);
  }
  return ret;
}

int ObLockAtomicProtocol::get_row_meta_value_(
    const ObSSMetaCtrlLockRowValue &row_value,
    common::ObIAllocator &allocator,
    ObString &meta_value)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol get invalid argument", K(ret));
  } else if (!row_value.is_valid()) {
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

int ObLockAtomicProtocol::lock_lock_row(
    common::ObISQLClient &trans,
    ObSSMetaCtrlLockRowValue &row_value)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlTableOperator ctrl_operator;
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  int64_t pos = 0;
  row_value.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol get invalid argument", K(ret));
  } else if (OB_FAIL(ctrl_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init ctrl operator", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_), K_(sql_timeout_us));
  } else if (OB_FAIL(get_row_meta_key_(allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(ctrl_operator.lock_row(tenant_id_, ls_id_, meta_ctrl_key_.type_, meta_key, allocator, meta_value))) {
    LOG_WARN("failed to get lock row", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(row_value.deserialize(meta_value.ptr(), meta_value.length(), pos))) {
    LOG_WARN("failed to deserialize lock row value", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  }
  return ret;
}

int ObLockAtomicProtocol::read_lock_row(
    common::ObISQLClient &sql_client,
    ObSSMetaCtrlLockRowValue &row_value)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlTableOperator ctrl_operator;
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  int64_t pos = 0;
  row_value.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol get invalid argument", K(ret));
  } else if (OB_FAIL(ctrl_operator.init(sql_client, sql_timeout_us_))) {
    LOG_WARN("failed to init ctrl operator", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_), K_(sql_timeout_us));
  } else if (OB_FAIL(get_row_meta_key_(allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(ctrl_operator.read_row(tenant_id_, ls_id_, meta_ctrl_key_.type_, meta_key, allocator, meta_value))) {
    LOG_WARN("failed to read lock row", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(row_value.deserialize(meta_value.ptr(), meta_value.length(), pos))) {
    LOG_WARN("failed to deserialize lock row value", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  }
  return ret;
}

int ObLockAtomicProtocol::update_lock_row(
    const ObSSMetaCtrlLockRowValue &row_value,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlTableOperator ctrl_operator;
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  int64_t pos = 0;
  ObSSMetaCtrlLockRowValue original_value;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol get invalid argument", K(ret));
  } else if (!row_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update lock row get invalid argument", K(ret), K(row_value));
  } else if (OB_FAIL(ctrl_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init ctrl operator", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_), K_(sql_timeout_us));
  } else if (OB_FAIL(get_row_meta_key_(allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(ctrl_operator.read_row(tenant_id_, ls_id_, meta_ctrl_key_.type_, meta_key, allocator, meta_value))) {
    LOG_WARN("failed to read lock row", K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(original_value.deserialize(meta_value.ptr(), meta_value.length(), pos))) {
    LOG_WARN("failed to deserialize lock row value", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(check_can_update_(original_value, row_value))) {
    LOG_WARN("failed to check can update", K(ret), K(original_value), K(row_value));
  } else if (original_value == row_value) {
    // 幂等重试：内容未变化时无需 UPDATE，避免 MySQL 返回 0 affected rows。
  } else if (OB_FAIL(write_lock_row_value_(row_value, trans))) {
    LOG_WARN("failed to write lock row", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  }
  return ret;
}

int ObLockAtomicProtocol::write_lock_row_value_(
    const ObSSMetaCtrlLockRowValue &row_value,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlTableOperator ctrl_operator;
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  int64_t affected_rows = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol is not initialized", K(ret));
  } else if (!row_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write lock row get invalid argument", K(ret), K(row_value));
  } else if (OB_FAIL(ctrl_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init ctrl operator", K(ret), K(tenant_id_), K(ls_id_),
        K(meta_ctrl_key_), K_(sql_timeout_us));
  } else if (OB_FAIL(get_row_meta_key_(allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (OB_FAIL(get_row_meta_value_(row_value, allocator, meta_value))) {
    LOG_WARN("failed to get row value", K(ret), K(row_value));
  } else if (OB_FAIL(ctrl_operator.update_row(
                 tenant_id_, ls_id_, meta_ctrl_key_.type_, meta_key, meta_value, affected_rows))) {
    LOG_WARN("failed to update lock row", K(ret), K(tenant_id_), K(ls_id_), K(meta_ctrl_key_));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update lock row affected rows unexpected", K(ret), K(tenant_id_),
        K(ls_id_), K(meta_ctrl_key_), K(affected_rows));
  }
  return ret;
}

int ObLockAtomicProtocol::persist_kept_meta_scn(
    const share::SCN &kept_meta_scn,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObSSMetaCtrlLockRowValue row_value;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol is not initialized", K(ret));
  } else if (OB_UNLIKELY(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK != meta_ctrl_key_.type_
             || !kept_meta_scn.is_valid()
             || kept_meta_scn.is_min()
             || kept_meta_scn.is_max())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("kept meta scn is not supported for this lock row", K(ret),
        K(meta_ctrl_key_), K(kept_meta_scn));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("failed to get tenant data version", K(ret), K(tenant_id_), K(kept_meta_scn));
  } else if (data_version < DATA_VERSION_4_6_2_1) {
    // 所有 Lock Row 写者完成升级前，不写入旧版本无法保留的有效位点。
    LOG_TRACE("skip to persist kept meta scn before data version gate", K(tenant_id_),
        K(data_version), K(meta_ctrl_key_), K(kept_meta_scn));
  } else if (OB_FAIL(lock_lock_row(trans, row_value))) {
    LOG_WARN("failed to lock tablet lock row", K(ret), K(meta_ctrl_key_));
  } else if (row_value.kept_meta_scn_ < kept_meta_scn) {
    const SCN old_kept_meta_scn = row_value.kept_meta_scn_;
    row_value.kept_meta_scn_ = kept_meta_scn;
    // mark-delete 仅表示 tablet 不再产生新版本；仍需记录历史版本消费者的安全边界。
    // 保留终态业务字段不变，仅在持有行锁时单调推进 kept_meta_scn。
    // 已持有当前行的 FOR UPDATE 锁，直接精确更新，避免再次 SELECT。
    if (OB_FAIL(write_lock_row_value_(row_value, trans))) {
      LOG_WARN("failed to advance kept meta scn", K(ret), K(meta_ctrl_key_),
          K(old_kept_meta_scn), K(kept_meta_scn), K(row_value));
    }
  } else if (row_value.kept_meta_scn_ > kept_meta_scn) {
    ret = OB_CANCELED;
    LOG_WARN("cancel stale kept meta scn persist plan", K(ret), K(meta_ctrl_key_),
        "current_kept_meta_scn", row_value.kept_meta_scn_, K(kept_meta_scn));
  } else {
    // row_value.kept_meta_scn_ == kept_meta_scn
    // 当前位点与计划位点相同，属于幂等重试，无需更新。
  }
  return ret;
}

int ObLockAtomicProtocol::check_can_update_(
    const ObSSMetaCtrlLockRowValue &src_value,
    const ObSSMetaCtrlLockRowValue &dest_value)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock atomic protocol get invalid argument", K(ret));
  } else if (!src_value.is_valid() || !dest_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update lock row get invalid argument", K(ret), K(src_value), K(dest_value));
  } else if (src_value.is_mark_deleted_
      || src_value.meta_scn_ > dest_value.meta_scn_
      || src_value.op_id_ > dest_value.op_id_
      || src_value.kept_meta_scn_ > dest_value.kept_meta_scn_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("can not update lock row value", K(ret), K(src_value), K(dest_value));
  }
  return ret;
}
