/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ls_atomic_protocol.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;


ObLSAtomicProtocol::ObLSAtomicProtocol()
  : ObAtomicProtocol(),
    is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_()
{
}

ObLSAtomicProtocol::~ObLSAtomicProtocol()
{
}

int ObLSAtomicProtocol::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const bool is_deleted,
    const share::SCN &deleted_scn)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls atomic protocol init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || (is_deleted && !deleted_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ls atomic protocol get invalid argument", K(ret), K(tenant_id), K(ls_id),
        K(is_deleted), K(deleted_scn));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_trans_timeout_us(tenant_id, sql_timeout_us_))) {
    LOG_WARN("failed to get trans timeout", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    sql_proxy_ = GCTX.sql_proxy_;
    type_.type_ = ObAtomicProtocolTypeEnum::LS_META;
    is_deleted_ = is_deleted;
    deleted_scn_ = deleted_scn;
    set_status_init_();
    is_inited_ = true;
  }
  return ret;
}

int ObLSAtomicProtocol::create(
    const bool need_check_sswrite)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls atomic protocol do not init", K(ret));
  } else if (OB_FAIL(create_(tenant_id_, ls_id_, tablet_id, need_check_sswrite))) {
    LOG_WARN("failed to do create", K(ret), K(tenant_id_), K(ls_id_));
  }
  return ret;
}

int ObLSAtomicProtocol::prepare_atomic_ctrl_info_(
    const ObSSMetaCtrlLockRowValue &lock_row_value,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  ctrl_value.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls atomic protocol do not init", K(ret));
  } else {
    ctrl_value.ctrl_info_.lease_epoch_ = lease_epoch_;
    ctrl_value.ctrl_info_.op_id_ = lock_row_value.op_id_;
    ctrl_value.ctrl_info_.set_cur_trace_id();
    if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_start())) {
      LOG_WARN("failed to set status start", K(ret), K(lock_row_value), K(ctrl_value));
    } else {
      ctrl_value.object_info_.set_none();
    }
  }
  return ret;
}

int ObLSAtomicProtocol::update_lock_row_(
    const ObSSMetaCtrlLockRowValue &lock_row_value,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls atomic protocol do not init", K(ret));
  } else if (OB_FAIL(prepare_lock_row_(lock_row))) {
    LOG_WARN("failed to prepare lock row", K(ret));
  } else {
    ObSSMetaCtrlLockRowValue new_lock_row_value = lock_row_value;
    new_lock_row_value.op_id_++;
    // create() only bumps op_id; lock_row.meta_scn_ is updated at finish().
    if (OB_FAIL(lock_row.update_lock_row(new_lock_row_value, trans))) {
      LOG_WARN("failed to update lock row", K(ret), K(new_lock_row_value));
    } else {
      op_id_ = lock_row_value.op_id_;
    }
  }
  return ret;
}

int ObLSAtomicProtocol::prepare_lock_row_(
    ObLockAtomicProtocol &lock_row)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlKey ctrl_key;
  ObSSMetaCtrlKey lock_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls atomic protocol do not init", K(ret));
  } else if (OB_FAIL(prepare_ss_meta_ctrl_key_(ctrl_key))) {
    LOG_WARN("failed to prepare ss meta ctrl key", K(ret));
  } else if (OB_FAIL(prepare_lock_row_key_(ctrl_key, lock_key))) {
    LOG_WARN("failed to prepare lock row key", K(ret));
  } else if (OB_FAIL(lock_row.init(tenant_id_, ls_id_, lock_key))) {
    LOG_WARN("failed to init lock row", K(ret), K(ls_id_));
  }
  return ret;
}

int ObLSAtomicProtocol::prepare_ss_meta_ctrl_key_(
    ObSSMetaCtrlKey &ctrl_key)
{
  int ret = OB_SUCCESS;
  ctrl_key.reset();
  ObSSMetaCtrlType type;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls atomic protocol do not init", K(ret));
  } else if (OB_FAIL(type_.get_ss_meta_ctrl_type(type))) {
    LOG_WARN("failed to get ss meta ctrl type", K(ret), K(type_));
  } else {
    ctrl_key.assign_type(type);
    ctrl_key.ls_key_.first_id_ = ls_id_.id();
    ctrl_key.ls_key_.second_id_ = 0;
    ctrl_key.ls_key_.control_scn_ = control_scn_;
  }
  return ret;
}

int ObLSAtomicProtocol::finish(ObIAtomicProtocolOP &op)
{
  int ret = OB_SUCCESS;
  const bool need_delete_row = true;
  const bool need_check_op_id = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls atomic protocol do not init", K(ret));
  } else if (OB_FAIL(finish_(
      tenant_id_, ls_id_, ObTabletID(), need_delete_row, need_check_op_id, op))) {
    LOG_WARN("failed to do ls atomic protocol finish", K(ret), K(tenant_id_), K(ls_id_));
  }
  return ret;
}

int ObLSAtomicProtocol::abort()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable atomic protocol do not init", K(ret));
  } else if (OB_FAIL(abort_(tenant_id_, ls_id_))) {
    LOG_WARN("failed to do abort", K(ret), K(tenant_id_), K(ls_id_));
  }
  return ret;
}
