/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <stdint.h>
#include "ob_atomic_protocol.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#include "lib/profile/ob_trace_id.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

// Separate errsim points for each fail-gc-eligible protocol flavour so
// tests can target a single orphan-ctrl-row kind without simultaneously
// poisoning every concurrent atomic finish in the system.
//   - NONE_MAJOR_SSTABLE : MINI / MINOR / MDS_* / SPLIT_* sstable variants
//   - MAJOR_SSTABLE : MAJOR sstable
//   - TABLET_META   : TABLET_META
// LS-level protocols (LS_META / CHECKPOINT) are excluded because they
// never produce __all_shared_gc_task rows
ERRSIM_POINT_DEF(EN_SS_ATOMIC_PROTOCOL_FINISH_FAIL_NONE_MAJOR_SSTABLE);
ERRSIM_POINT_DEF(EN_SS_ATOMIC_PROTOCOL_FINISH_FAIL_MAJOR_SSTABLE);
ERRSIM_POINT_DEF(EN_SS_ATOMIC_PROTOCOL_FINISH_FAIL_NON_IDEM_MAJOR_SSTABLE);
ERRSIM_POINT_DEF(EN_SS_ATOMIC_PROTOCOL_FINISH_FAIL_TABLET_META);
ERRSIM_POINT_DEF(EN_SS_ATOMIC_PROTOCOL_ABORT_FAIL);

/******************ObAtomicProtocol*********************/

ObAtomicProtocol::ObAtomicProtocol()
  : status_(STATUS::INVALID),
    lease_epoch_(0),
    type_(),
    control_scn_(share::SCN::min_scn()),
    meta_scn_(share::SCN::min_scn()),
    sql_proxy_(nullptr),
    op_id_(0),
    is_deleted_(false),
    deleted_scn_(),
    last_major_snapshot_(0),
    sql_timeout_us_(ObAtomicProtocolUtil::DEFAULT_ATOMIC_PROTOCOL_TRANS_TIMEOUT_US)
{
}

ObAtomicProtocol::~ObAtomicProtocol()
{
}

int ObAtomicProtocol::check_can_do_create_()
{
  int ret = OB_SUCCESS;
  if (STATUS::INIT != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("can not do create", K(ret), K(status_));
  }
  return ret;
}

int ObAtomicProtocol::check_can_do_finish_()
{
  int ret = OB_SUCCESS;
  if (STATUS::START != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("can not do finish", K(ret), K(status_));
  }
  return ret;
}

int ObAtomicProtocol::check_can_do_abort_()
{
  int ret = OB_SUCCESS;
  if (STATUS::START != status_
      && STATUS::INIT != status_
      && STATUS::COMPLETE != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("can not do abort", K(ret), K(status_));
  }
  return ret;
}

// 这个函数是给ObSSTableAtomicProtocol专供的
// 其他类型的原子协议采用下面的默认实现
int ObAtomicProtocol::check_before_create_under_lock_(
    common::ObISQLClient &sql_client,
    bool &need_create)
{
  UNUSED(sql_client);
  need_create = true;
  return OB_SUCCESS;
}

int ObAtomicProtocol::on_start_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool need_check_sswriter,
    int64_t &current_epoch)
{
  int ret = OB_SUCCESS;
  current_epoch = 0;
  if (OB_LIKELY(need_check_sswriter)) {
    bool is_sswriter = false;
    if (OB_FAIL(check_lease_(ls_id, tablet_id, is_sswriter, current_epoch))) {
      LOG_WARN("failed to check lease", K(ret));
    } else if (!is_sswriter) {
      ret = OB_NOT_MASTER;
      LOG_WARN("write qualification not satisfied", K(ret));
    } else if (lease_epoch_ != current_epoch) {
      lease_epoch_ = current_epoch;
    }
  }
  return ret;
}

int ObAtomicProtocol::on_finish_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(need_check_sswriter_())) {
    bool is_sswriter = false;
    int64_t current_epoch = 0;
    if (OB_FAIL(check_lease_(ls_id, tablet_id, is_sswriter, current_epoch))) {
      LOG_WARN("failed to check lease", K(ret));
    } else if (!is_sswriter || lease_epoch_ != current_epoch) {
      ret = OB_NOT_MASTER;
      LOG_WARN("write qualification lost or epoch changed", K(ret), K(lease_epoch_), K(current_epoch));
    }
  }
  return ret;
}


int ObAtomicProtocol::check_lease_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    bool &is_sswriter,
    int64_t &current_epoch)
{
  int ret = OB_SUCCESS;
  ObSSWriterType sswriter_type = ObSSWriterType::INVALID;
  if (OB_FAIL(get_sswriter_type(sswriter_type))) {
    LOG_WARN("failed to get sswriter type", K(ret), K(ls_id), K(tablet_id));
  } else {
    const ObSSWriterKey key(sswriter_type, ls_id, tablet_id);
    if (OB_FAIL(MTL(ObSSWriterService*)->check_lease_for_atomic_protocol(key,
                                                     is_sswriter,
                                                     current_epoch))) {
      LOG_WARN("failed to check lease", K(ret), K(key));
    }
  }
  return ret;
}

int ObAtomicProtocol::get_sswriter_type(
    ObSSWriterType &sswriter_type) const
{
  int ret = OB_SUCCESS;
  sswriter_type = ObSSWriterType::INVALID;
  switch (type_.type_) {
    #define REGISTER_ATOMIC_PROTOCOL(TYPE_NAME, ID, PATH_TYPE, SSWRITER_TYPE, ...) \
      case ObAtomicProtocolTypeEnum::TYPE_NAME: { \
        sswriter_type = ObSSWriterType::SSWRITER_TYPE; \
        break; \
      }
    #define NEED_REGISTER_ATOMIC_PROTOCOL
      #include "storage/incremental/atomic_protocol/compile_utility/ob_atomic_protocol_register.h"

    #undef NEED_REGISTER_ATOMIC_PROTOCOL
    #undef REGISTER_ATOMIC_PROTOCOL

    default:
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "not support atomic file type", K(type_));
  }
  return ret;
}

int ObAtomicProtocol::select_ctrl_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &ctrl_key,
    common::ObISQLClient &trans,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  ctrl_value.reset();
  common::ObArenaAllocator allocator("APSelect");
  ObString meta_key;
  ObString meta_value;
  ObSSMetaCtrlTableOperator table_operator;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !ctrl_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("select ctrl row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (OB_FAIL(get_row_meta_key_(ctrl_key, allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(ctrl_key));
  } else if (OB_FAIL(table_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init table operator", K(ret), K(ctrl_key), K_(sql_timeout_us));
  } else if (OB_FAIL(table_operator.read_row(tenant_id, ls_id, ctrl_key.type_, meta_key, allocator, meta_value))) {
    LOG_WARN("failed to read row", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (OB_FAIL(get_row_ctrl_value_(meta_value, ctrl_value))) {
    LOG_WARN("failed to get row ctrl value", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
  }
  return ret;
}

int ObAtomicProtocol::insert_ctrl_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &ctrl_key,
    const ObSSMetaCtrlValue &ctrl_value,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("APInsert");
  ObString meta_key;
  ObString meta_value;
  ObSSMetaCtrlTableOperator table_operator;
  int64_t affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !ctrl_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert ctrl row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (OB_FAIL(get_row_meta_key_(ctrl_key, allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(ctrl_key));
  } else if (OB_FAIL(get_row_meta_value_(ctrl_value, allocator, meta_value))) {
    LOG_WARN("failed to get row meta value", K(ret), K(ctrl_value), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (OB_FAIL(table_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init table operator", K(ret), K(ctrl_key), K_(sql_timeout_us));
  } else if (OB_FAIL(table_operator.insert_row(tenant_id, ls_id, ctrl_key.type_, meta_key, meta_value, affected_rows))) {
    LOG_WARN("failed to insert row", K(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(ctrl_value));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert ctrl row return affected rows unexpected", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
  }
  return ret;
}

int ObAtomicProtocol::delete_ctrl_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &ctrl_key,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("APDelete");
  ObString meta_key;
  ObSSMetaCtrlTableOperator table_operator;
  int64_t affected_rows = 0;
  const bool prefix_delete = false;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !ctrl_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("delete ctrl row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (OB_FAIL(get_row_meta_key_(ctrl_key, allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(ctrl_key));
  } else if (OB_FAIL(table_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init table operator", K(ret), K(ctrl_key), K_(sql_timeout_us));
  } else if (OB_FAIL(table_operator.delete_row(tenant_id, ls_id, ctrl_key.type_, meta_key, prefix_delete, affected_rows))) {
    LOG_WARN("failed to delete row", K(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(ctrl_key));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete ctrl row return affected rows unexpected", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
  }
  return ret;
}

int ObAtomicProtocol::update_ctrl_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &ctrl_key,
    const ObSSMetaCtrlValue &ctrl_value,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("APUpdate");
  ObString meta_key;
  ObString meta_value;
  ObSSMetaCtrlTableOperator table_operator;
  int64_t affected_rows = 0;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !ctrl_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update ctrl row get invalid argument", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (OB_FAIL(get_row_meta_key_(ctrl_key, allocator, meta_key))) {
    LOG_WARN("failed to get row meta key", K(ret), K(ctrl_key));
  } else if (OB_FAIL(get_row_meta_value_(ctrl_value, allocator, meta_value))) {
    LOG_WARN("failed to get row meta value", K(ret), K(ctrl_value), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (OB_FAIL(table_operator.init(trans, sql_timeout_us_))) {
    LOG_WARN("failed to init table operator", K(ret), K(ctrl_key), K_(sql_timeout_us));
  } else if (OB_FAIL(table_operator.update_row(tenant_id, ls_id, ctrl_key.type_, meta_key, meta_value, affected_rows))) {
    LOG_WARN("failed to insert row", K(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(ctrl_value));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctrl row return affected rows unexpected", K(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(affected_rows));
  }
  return ret;
}

int ObAtomicProtocol::get_row_meta_key_(
    const ObSSMetaCtrlKey &ctrl_key,
    common::ObIAllocator &allocator,
    ObString &meta_value)
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
    meta_value.assign(buf, pos);
  }
  return ret;
}

int ObAtomicProtocol::get_row_meta_key_prefix_(
    const ObSSMetaCtrlKey &ctrl_key,
    common::ObIAllocator &allocator,
    ObString &meta_key_prefix)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  // The prefix only serializes id_1/id_2 (meta_scn is excluded), so the key may
  // legitimately have an unset meta_scn here; do not enforce ctrl_key.is_valid().
  if (OB_FAIL(ctrl_key.get_prefix_meta_key_serialize_size(buf_len))) {
    LOG_WARN("failed to get prefix meta key serialize size", K(ret), K(ctrl_key));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_len));
  } else if (OB_FAIL(ctrl_key.serialize_prefix_meta_key(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize prefix meta key", K(ret), K(ctrl_key));
  } else {
    meta_key_prefix.assign(buf, pos);
  }
  return ret;
}

int ObAtomicProtocol::get_row_meta_value_(
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

int ObAtomicProtocol::get_row_ctrl_key_(
    const ObString &meta_key,
    ObSSMetaCtrlKey &ctrl_key)
{
  int ret = OB_SUCCESS;
  ctrl_key.reset();
  int64_t pos = 0;

  if (meta_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get row ctrl key get invalid argument", K(ret), K(meta_key));
  } else if (OB_FAIL(ctrl_key.deserialize_meta_key(meta_key.ptr(), meta_key.length(), pos))) {
    LOG_WARN("failed to deserialize meta key", K(ret), K(meta_key));
  }
  return ret;
}

int ObAtomicProtocol::get_row_ctrl_value_(
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

int ObAtomicProtocol::get_control_scn_(share::SCN &control_scn)
{
  int ret = OB_SUCCESS;
  control_scn.reset();

  if (STATUS::START != status_
      && STATUS::COMPLETE != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status is not match, can not get control scn", K(ret), K(status_));
  } else {
    control_scn = control_scn_;
  }
  return ret;
}

int ObAtomicProtocol::get_op_id(int64_t &op_id)
{
  int ret = OB_SUCCESS;
  op_id = 0;

  if (STATUS::START != status_
      && STATUS::COMPLETE != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status is not match, can not get meta scn", K(ret), K(status_));
  } else {
    op_id = op_id_;
  }
  return ret;
}

int ObAtomicProtocol::get_protocol_type(ObAtomicProtocolType &type)
{
  int ret = OB_SUCCESS;
  type.reset();

  if (STATUS::START != status_
      && STATUS::COMPLETE != status_
      && STATUS::ABANDON != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status is not match, can not get protocol type", K(ret), K(status_));
  } else {
    type = type_;
  }
  return ret;
}

int ObAtomicProtocol::get_meta_scn(share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  meta_scn.reset();

  if (STATUS::COMPLETE != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status is not match, can not get meta scn", K(ret), K(status_));
  } else {
    meta_scn = meta_scn_;
  }
  return ret;
}

int ObAtomicProtocol::start_trans_(
    const int64_t stmt_timeout,
    ObTimeoutCtx &timeout_ctx,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const bool with_snapshot = false;
  const int32_t group_id = share::OBCG_SHARED_META;

  if (stmt_timeout <= 0 || group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start trans get invalid argument", K(ret), K(stmt_timeout), K(group_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::start_trans(tenant_id, stmt_timeout, with_snapshot, group_id, timeout_ctx, trans))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id), K(stmt_timeout), K(group_id));
  }
  return ret;
}

int ObAtomicProtocol::commit_trans_(
    const int32_t &result,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObAtomicProtocolUtil::commit_trans(result, trans))) {
    LOG_WARN("failed to commit trans", K(ret), K(result));
  }
  return ret;
}

int ObAtomicProtocol::get_control_scn_(
    const uint64_t tenant_id,
    const ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  SCN gts;
  uint64_t real_tenant_id = OB_INVALID_ID;
  if (!lock_row_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get control scn get invalid argument", K(ret), K(lock_row_value));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_real_tenant_id(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_gts(real_tenant_id, gts))) {
    LOG_WARN("failed to get gts", K(ret), K(tenant_id), K(real_tenant_id));
  } else {
    // control_scn only identifies this protocol round in __all_shared_meta_control;
    // there is no need to consult max_reserved_scn (that belongs to business meta_scn at finish).
    SCN lock_meta_scn = lock_row_value.meta_scn_;
    if (gts <= lock_meta_scn) {
      lock_meta_scn = share::SCN::scn_inc(lock_meta_scn);
      if (!lock_meta_scn.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to increment lock row meta scn", K(ret), K(lock_row_value));
      }
    }
    if (OB_SUCC(ret)) {
      control_scn_ = SCN::max(gts, lock_meta_scn);
    }
  }
  return ret;
}

int ObAtomicProtocol::create_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool need_check_sswrite)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = sql_timeout_us_;
  bool need_create = true;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create get invalid argument", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(check_can_do_create_())) {
    LOG_WARN("failed to check can do create", K(ret));
  } else if (OB_FAIL(on_start_(ls_id, tablet_id, need_check_sswrite, lease_epoch_))) {
    LOG_WARN("failed to do on start", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(check_and_create_lock_row_())) {
    LOG_WARN("failed to check and create lock row", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
  } else {
    HEAP_VARS_4((ObMySQLTransaction, trans),
                (ObSSMetaCtrlLockRowValue, lock_row_value),
                (ObSSMetaCtrlKey, ctrl_key),
                (ObSSMetaCtrlValue, ctrl_value)) {
      if (OB_FAIL(start_trans_(stmt_timeout, timeout_ctx, trans))) {
        LOG_WARN("failed to start trans", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
      } else {
        if (OB_FAIL(lock_parent_lock_row_(trans))) {
          LOG_WARN("failed to lock parent lock row", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        } else if (OB_FAIL(lock_lock_row_(trans, lock_row_value))) {
          LOG_WARN("failed to lock lock row", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        } else if (lock_row_value.is_mark_deleted_) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("entity is already mark deleted", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(lock_row_value));
        } else if (OB_FAIL(check_before_create_under_lock_(trans, need_create))) {
          LOG_WARN("failed to check before create under lock", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        } else if (!need_create) {
          LOG_INFO("ctrl row for the same target already exists, adopt it and skip create",
              K(tenant_id), K(ls_id), K(tablet_id));
        } else if (OB_FAIL(get_control_scn_(tenant_id, lock_row_value))) {
          LOG_WARN("failed to get control scn", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        } else if (OB_FAIL(prepare_ss_meta_ctrl_key_(ctrl_key))) {
          LOG_WARN("failed to prepare ss meta ctrl key", K(ret), K(tenant_id), K(ls_id));
        } else if (OB_FAIL(prepare_atomic_ctrl_info_(lock_row_value, ctrl_value))) {
          LOG_WARN("failed to prepare atomic ctrl info", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        } else if (OB_FAIL(insert_ctrl_row_(tenant_id, ls_id, ctrl_key, ctrl_value, trans))) {
          LOG_WARN("failed to insert ctrl row", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        } else if (OB_FAIL(update_lock_row_(lock_row_value, trans))) {
          LOG_WARN("failed to update lock row", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
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

  if (OB_SUCC(ret) && need_create) {
    set_status_start_();
  }
  return ret;
}

int ObAtomicProtocol::abort_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  ObEmptyAtomicProtocolOP empty_op;
  return abort_(tenant_id, ls_id, empty_op);
}

int ObAtomicProtocol::abort_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    ObIAtomicProtocolOP &op)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = sql_timeout_us_;

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("abort get invalid argument", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(check_can_do_abort_())) {
    LOG_WARN("failed to check can do abort", K(ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret) && ObSSGCUtil::is_errsim_target(tenant_id)) {
    bool should_inject = ObSSGCUtil::is_errsim_target(
        tenant_id, ObTabletID::INVALID_TABLET_ID);
    uint64_t key_tablet_id = ObTabletID::INVALID_TABLET_ID;
    if (!should_inject) {
      ObSSMetaCtrlKey ctrl_key;
      if (OB_SUCCESS == prepare_ss_meta_ctrl_key_(ctrl_key)) {
        if (type_.is_sstable_level()) {
          key_tablet_id = ctrl_key.sstable_key_.tablet_id_.id();
        } else if (type_.is_tablet_level()) {
          key_tablet_id = ctrl_key.tablet_key_.tablet_id_.id();
        }
        should_inject = ObSSGCUtil::is_errsim_target(tenant_id, key_tablet_id);
      }
    }
    if (should_inject) {
      INJECT_SS_GC_ERRSIM(EN_SS_ATOMIC_PROTOCOL_ABORT_FAIL, tenant_id);
    }
  }
#endif
  if (OB_FAIL(ret)) {
  } else {
    HEAP_VARS_4((ObMySQLTransaction, trans),
                (ObSSMetaCtrlLockRowValue, lock_row_value),
                (ObSSMetaCtrlKey, ctrl_key),
                (ObSSMetaCtrlValue, ctrl_value)) {
      if (OB_FAIL(start_trans_(stmt_timeout, timeout_ctx, trans))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        if (OB_FAIL(lock_lock_row_(trans, lock_row_value))) {
          if (OB_ITER_END == ret) {
            //lock row not exist, overwrtie ret
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to lock lock row", K(ret));
          }
        } else if (OB_FAIL(op.process(trans, share::SCN::min_scn()))) {
          LOG_WARN("failed to do op process", K(ret), K(tenant_id), K(ls_id));
        } else if (OB_FAIL(prepare_ss_meta_ctrl_key_(ctrl_key))) {
          LOG_WARN("failed to prepare ss meta ctrl key", K(ret), K(tenant_id), K(ls_id));
        } else if (OB_FAIL(select_ctrl_row_(tenant_id, ls_id, ctrl_key, trans, ctrl_value))) {
          if (OB_ITER_END == ret) {
            //do nothing
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to select ctrl row", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
          }
        } else if (!ctrl_value.ctrl_info_.status_.is_start_status()
            && !ctrl_value.ctrl_info_.status_.is_complete_status()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctrl value ctrl info status is unexpected", K(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(ctrl_value));
        } else if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_abandon())) {
          LOG_WARN("failed to set status abandon", K(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(ctrl_value));
        } else if (OB_FAIL(update_ctrl_row_(tenant_id, ls_id, ctrl_key, ctrl_value, trans))) {
          LOG_WARN("failed to update ctrl row", K(ret), K(tenant_id), K(ctrl_key), K(ctrl_value));
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

  if (OB_SUCC(ret)) {
    set_status_abandon_();
  } else {
#ifdef ERRSIM
    SERVER_EVENT_ADD("SHARED_GC", "SS_GC_ATOMIC_PROTOCOL_ABORT_FAIL",
        "tenant_id", tenant_id,
        "ls_id", ls_id.id(),
        "type", type_,
        "ret", ret,
        "trace_id", ObCurTraceId::get_trace_id_str());
#endif
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_ATOMIC_PROTOCOL_FINISH_FAIL);
ERRSIM_POINT_DEF(EN_ATOMIC_PROTOCOL_TABLET_FINISH_FAIL);
int ObAtomicProtocol::finish_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool need_delete_row,
    const bool need_check_op_id,
    ObIAtomicProtocolOP &op)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  const int64_t stmt_timeout = sql_timeout_us_;
#ifdef ERRSIM
  bool should_inject = false;
#endif

  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("finish get invalid argument",
        K(ret), K(tenant_id), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(check_can_do_finish_())) {
    LOG_WARN("failed to check can do finish", K(ret));
  } else if (OB_FAIL(on_finish_(ls_id, tablet_id))) {
    LOG_WARN("failed to on finish", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret) && ObSSGCUtil::is_errsim_target(tenant_id)) {
    should_inject = ObSSGCUtil::is_errsim_target(tenant_id, ObTabletID::INVALID_TABLET_ID);
    uint64_t key_tablet_id = ObTabletID::INVALID_TABLET_ID;
    if (!should_inject) {
      ObSSMetaCtrlKey ctrl_key;
      if (OB_SUCCESS == prepare_ss_meta_ctrl_key_(ctrl_key)) {
        if (type_.is_sstable_level()) {
          key_tablet_id = ctrl_key.sstable_key_.tablet_id_.id();
        } else if (type_.is_tablet_level()) {
          key_tablet_id = ctrl_key.tablet_key_.tablet_id_.id();
        }
        should_inject = ObSSGCUtil::is_errsim_target(tenant_id, key_tablet_id);
      }
    }
    if (should_inject) {
      if (type_.is_sstable_level()) {
        ObSSMetaCtrlType ctrl_type;
        int tmp_ret = type_.get_ss_meta_ctrl_type(ctrl_type);
        if (OB_SUCCESS == tmp_ret && ctrl_type.is_major_sstable_type()) {
          INJECT_SS_GC_ERRSIM(EN_SS_ATOMIC_PROTOCOL_FINISH_FAIL_MAJOR_SSTABLE, tenant_id);
        } else if (ObAtomicProtocolTypeEnum::NON_IDEM_MAJOR_SSTABLE == type_.type_) {
          INJECT_SS_GC_ERRSIM(EN_SS_ATOMIC_PROTOCOL_FINISH_FAIL_NON_IDEM_MAJOR_SSTABLE, tenant_id);
        } else {
          INJECT_SS_GC_ERRSIM(EN_SS_ATOMIC_PROTOCOL_FINISH_FAIL_NONE_MAJOR_SSTABLE, tenant_id);
        }
      } else if (type_.is_tablet_level()) {
        INJECT_SS_GC_ERRSIM(EN_SS_ATOMIC_PROTOCOL_FINISH_FAIL_TABLET_META, tenant_id);
      }
      // LS-level intentionally has no errsim hook -- fail-gc never generates
      // tasks for ls-type ctrl rows, so injecting here would only cause noise.
    }
  }
#endif
  if (OB_SUCC(ret)) {
    HEAP_VARS_4((ObMySQLTransaction, trans),
                (ObSSMetaCtrlLockRowValue, lock_row_value),
                (ObSSMetaCtrlKey, ctrl_key),
                (ObSSMetaCtrlValue, ctrl_value)) {
      if (OB_FAIL(start_trans_(stmt_timeout, timeout_ctx, trans))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        if (OB_FAIL(lock_parent_lock_row_(trans))) {
          LOG_WARN("failed to lock parent lock row", K(ret), K(tenant_id), K(ls_id));
        } else if (OB_FAIL(lock_lock_row_(trans, lock_row_value))) {
          LOG_WARN("failed to lock lock row", K(ret));
        } else if (lock_row_value.is_mark_deleted_) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("entity is already mark deleted", K(ret), K(tenant_id), K(ls_id), K(lock_row_value));
        } else if (need_check_op_id && (lock_row_value.op_id_ != op_id_ + 1)) {
          ret = OB_NEED_RETRY;
          LOG_WARN("op id is already changed, need retry", K(ret), K(lock_row_value), K(op_id_), K(tenant_id), K(ls_id));
        } else if (OB_FAIL(get_meta_scn_(tenant_id, lock_row_value, trans))) {
          LOG_WARN("failed to get meta scn at finish", K(ret), K(tenant_id), K(ls_id));
        } else if (OB_FAIL(op.process(trans, meta_scn_))) {
          LOG_WARN("failed to do op process", K(ret), K(tenant_id), K(ls_id));
        } else if (OB_FAIL(prepare_ss_meta_ctrl_key_(ctrl_key))) {
          LOG_WARN("failed to prepare ss meta ctrl key", K(ret), K(tenant_id), K(ls_id));
        } else if (OB_FAIL(select_ctrl_row_(tenant_id, ls_id, ctrl_key, trans, ctrl_value))) {
          LOG_WARN("failed to select ctrl row", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
        } else if (!ctrl_value.ctrl_info_.status_.is_start_status()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctrl value ctrl info status is unexpected", K(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(ctrl_value));
        } else if (need_delete_row) {
          if (OB_FAIL(delete_ctrl_row_(tenant_id, ls_id, ctrl_key, trans))) {
            LOG_WARN("failed to delete ctrl row", K(ret), K(tenant_id), K(ls_id), K(ctrl_key));
          }
        } else if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_complete())) {
          LOG_WARN("failed to set status complete", K(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(ctrl_value));
        } else if (OB_FAIL(update_ctrl_row_(tenant_id, ls_id, ctrl_key, ctrl_value, trans))) {
          LOG_WARN("failed to update ctrl row", K(ret), K(tenant_id), K(ctrl_key), K(ctrl_value));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(update_lock_row_at_finish_(lock_row_value, trans))) {
            LOG_WARN("failed to update lock row at finish", K(ret), K(meta_scn_), K_(is_deleted),
                K_(deleted_scn), K_(last_major_snapshot));
          }
        }

#ifdef ERRSIM
        if (OB_SUCC(ret) && should_inject) {
          ret = EN_ATOMIC_PROTOCOL_FINISH_FAIL ? : OB_SUCCESS;
          if (OB_FAIL(ret)) {
            STORAGE_LOG(ERROR, "fake EN_ATOMIC_PROTOCOL_FINISH_FAIL", K(ret));
          }
        }
#endif

#ifdef ERRSIM
        if (OB_SUCC(ret) && should_inject && type_.is_tablet_level()) {
          ret = EN_ATOMIC_PROTOCOL_TABLET_FINISH_FAIL ? : OB_SUCCESS;
          if (OB_FAIL(ret)) {
            STORAGE_LOG(ERROR, "fake EN_ATOMIC_PROTOCOL_TABLET_FINISH_FAIL", K(ret));
          }
        }
#endif

        if (OB_TMP_FAIL(commit_trans_(ret, trans))) {
          LOG_WARN("failed to commit trans", K(tmp_ret), K(ret));
          if (OB_SUCCESS == ret) {
            ret = tmp_ret;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    set_status_complete_();
  } else {
#ifdef ERRSIM
    SERVER_EVENT_ADD("SHARED_GC", "SS_GC_ATOMIC_PROTOCOL_FINISH_FAIL",
        "tenant_id", tenant_id,
        "ls_id", ls_id.id(),
        "type", type_,
        "ret", ret,
        "trace_id", ObCurTraceId::get_trace_id_str());
#endif
  }
  return ret;
}

int ObAtomicProtocol::lock_parent_lock_row_(ObMySQLTransaction &trans)
{
  // Default: this entity has no parent lock row to lock. Overridden by
  // sstable-level protocols which must lock the owning tablet's lock row first.
  UNUSED(trans);
  return OB_SUCCESS;
}

int ObAtomicProtocol::lock_lock_row_(
    ObMySQLTransaction &trans,
    ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;
  lock_row_value.reset();

  if (OB_FAIL(prepare_lock_row_(lock_row))) {
    LOG_WARN("failed to prepare lock row", K(ret));
  } else if (OB_FAIL(lock_row.lock_lock_row(trans, lock_row_value))) {
    LOG_WARN("failed to lock lock row", K(ret));
  }
  return ret;
}

int ObAtomicProtocol::check_and_create_lock_row_()
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;

  if (OB_FAIL(prepare_lock_row_(lock_row))) {
    LOG_WARN("failed to prepare lock row", K(ret));
  } else if (OB_FAIL(lock_row.check_and_create_lock_row(*sql_proxy_))) {
    LOG_WARN("failed to check and create lock row", K(ret));
  }
  return ret;
}

int ObAtomicProtocol::update_lock_row_at_finish_(
    const ObSSMetaCtrlLockRowValue &lock_row_value,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;
  if (!lock_row_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update lock row at finish get invalid argument", K(ret), K(lock_row_value));
  } else if (!meta_scn_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta scn is not allocated before updating lock row", K(ret), K(type_));
  } else if (is_deleted_ && (!deleted_scn_.is_valid() || last_major_snapshot_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("deleted lock row info is invalid", K(ret), K_(deleted_scn), K_(last_major_snapshot));
  } else if (OB_FAIL(prepare_lock_row_(lock_row))) {
    LOG_WARN("failed to prepare lock row", K(ret));
  } else {
    ObSSMetaCtrlLockRowValue new_lock_row_value = lock_row_value;
    // Finish updates lock row exactly once to avoid stale-meta_scn overwrite:
    // always advance meta_scn_; optionally mark deleted in the same write.
    new_lock_row_value.meta_scn_ = meta_scn_;
    if (is_deleted_) {
      new_lock_row_value.is_mark_deleted_ = true;
      new_lock_row_value.deleted_scn_ = deleted_scn_;
      new_lock_row_value.last_major_snapshot_ = last_major_snapshot_;
    }
    if (OB_FAIL(lock_row.update_lock_row(new_lock_row_value, trans))) {
      LOG_WARN("failed to update lock row at finish", K(ret), K(lock_row_value),
          K(new_lock_row_value), K_(is_deleted), K_(deleted_scn), K_(last_major_snapshot));
    }
  }
  return ret;
}

int ObAtomicProtocol::prepare_lock_row_key_(
  const ObSSMetaCtrlKey &src,
  ObSSMetaCtrlKey &dest)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlType type;
  dest.reset();
  if (!src.is_valid() || src.is_lock_row()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get lock row key get invalid argument", K(ret), K(src));
  } else if (OB_FAIL(src.get_lock_row_key(dest))) {
    LOG_WARN("failed to get lock row key", K(ret), K(src));
  }
  return ret;
}

int ObAtomicProtocol::get_max_reserved_scn_(
    const uint64_t tenant_id,
    const bool need_lock,
    ObMySQLTransaction &trans,
    share::SCN &max_reserved_scn)
{  
  int ret = OB_SUCCESS;
  ObSharedMetaReservedMgr shared_meta_reserved_mgr;
  max_reserved_scn = SCN::min_scn();
  if (is_meta_tenant(tenant_id) || is_sys_tenant(tenant_id)) { 
    //do nothing
  } else if (type_.is_sstable_level()) {
    //do nothing
  } else if (OB_FAIL(shared_meta_reserved_mgr.init(tenant_id))) {
    LOG_WARN("failed to init shared meta reserved mgr", K(ret), K(tenant_id));
  } else if (OB_FAIL(shared_meta_reserved_mgr.get_max_reserved_scn(need_lock, trans, max_reserved_scn))) {
    LOG_WARN("failed to get max reserved scn", K(ret), K(tenant_id));
  }
  return ret;
}

int ObAtomicProtocol::get_meta_scn_(
    const uint64_t tenant_id,
    const ObSSMetaCtrlLockRowValue &lock_row_value,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  SCN gts;
  SCN max_reserved_scn;
  const bool need_lock = true;
  uint64_t real_tenant_id = OB_INVALID_ID;
  if (!lock_row_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get meta scn get invalid argument", K(ret), K(lock_row_value));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_real_tenant_id(tenant_id, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_gts(real_tenant_id, gts))) {
    LOG_WARN("failed to get gts", K(ret), K(tenant_id), K(real_tenant_id));
  } else if (OB_FAIL(get_max_reserved_scn_(tenant_id, need_lock, trans, max_reserved_scn))) {
    LOG_WARN("failed to get max reserved scn", K(ret), K(tenant_id));
  } else {
    SCN candidate = gts;
    SCN lock_meta_scn = lock_row_value.meta_scn_;
    if (lock_meta_scn.is_valid()) {
      lock_meta_scn = share::SCN::scn_inc(lock_meta_scn);
      if (!lock_meta_scn.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to increment lock row meta scn", K(ret), K(lock_row_value));
      } else {
        candidate = SCN::max(candidate, lock_meta_scn);
      }
    }
    if (OB_SUCC(ret) && max_reserved_scn.is_valid()) {
      SCN reserved_plus_one = max_reserved_scn;
      reserved_plus_one = share::SCN::scn_inc(reserved_plus_one);
      if (!reserved_plus_one.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to increment max reserved scn", K(ret), K(max_reserved_scn), K(tenant_id));
      } else {
        candidate = SCN::max(candidate, reserved_plus_one);
      }
    }
    if (OB_SUCC(ret)) {
      meta_scn_ = candidate;
    }
    if (OB_SUCC(ret) && max_reserved_scn.is_valid() && meta_scn_ <= max_reserved_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("meta scn is not greater than max reserved scn, unexpected", K(ret),
          K(meta_scn_), K(max_reserved_scn), K(tenant_id), K(lock_row_value));
    }
  }
  return ret;
}
