/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_atomic_protocol.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;


ObTabletAtomicProtocolParam::ObTabletAtomicProtocolParam()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    reorg_scn_(),
    key_infos_(),
    is_deleted_(false),
    deleted_scn_(),
    last_major_snapshot_(-1),
    transfer_seq_(0)
{
}

void ObTabletAtomicProtocolParam::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  reorg_scn_.reset();
  key_infos_.reset();
  is_deleted_ = false;
  deleted_scn_.reset();
  last_major_snapshot_ = -1;
  transfer_seq_ = 0;
}

bool ObTabletAtomicProtocolParam::is_valid() const
{
  // key_infos_ is allowed to be empty (tablet may have no sstable to track).
  // deleted_scn_ and last_major_snapshot_ are both required iff is_deleted_ is true.
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && tablet_id_.is_valid()
      && reorg_scn_.is_valid()
      && (!is_deleted_ || (deleted_scn_.is_valid() && last_major_snapshot_ >= 0));
}

int ObTabletAtomicProtocolParam::assign(const ObTabletAtomicProtocolParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign tablet atomic protocol param get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(key_infos_.assign(param.key_infos_))) {
    LOG_WARN("failed to assign key infos", K(ret), K(param));
  } else {
    tenant_id_ = param.tenant_id_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    reorg_scn_ = param.reorg_scn_;
    is_deleted_ = param.is_deleted_;
    deleted_scn_ = param.deleted_scn_;
    last_major_snapshot_ = param.last_major_snapshot_;
    transfer_seq_ = param.transfer_seq_;
  }
  return ret;
}

ObTabletAtomicProtocol::ObTabletAtomicProtocol()
  : ObAtomicProtocol(),
    is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    reorg_scn_(),
    key_infos_()
{
}

ObTabletAtomicProtocol::~ObTabletAtomicProtocol()
{
}

int ObTabletAtomicProtocol::init(const ObTabletAtomicProtocolParam &param)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet atomic protocol tablet init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet atomic protocol get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_trans_timeout_us(param.tenant_id_, sql_timeout_us_))) {
    LOG_WARN("failed to get trans timeout", K(ret), K(param));
  } else if (OB_FAIL(key_infos_.assign(param.key_infos_))) {
    LOG_WARN("failed to assign key infos", K(ret), K(param));
  } else {
    tenant_id_ = param.tenant_id_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    reorg_scn_ = param.reorg_scn_;
    sql_proxy_ = GCTX.sql_proxy_;
    is_deleted_ = param.is_deleted_;
    deleted_scn_ = param.deleted_scn_;
    last_major_snapshot_ = param.last_major_snapshot_;
    type_.type_ = ObAtomicProtocolTypeEnum::TABLET_META;
    set_status_init_();
    is_inited_ = true;
  }
  return ret;
}

int ObTabletAtomicProtocol::create(
    const bool need_check_sswrite)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
  } else if (OB_FAIL(create_(tenant_id_, ls_id_, tablet_id_, need_check_sswrite))) {
    LOG_WARN("failed to do create", K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_));
  }

  return ret;
}

int ObTabletAtomicProtocol::prepare_atomic_ctrl_info_(
    const ObSSMetaCtrlLockRowValue &lock_row_value,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  ctrl_value.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
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

int ObTabletAtomicProtocol::update_lock_row_(
    const ObSSMetaCtrlLockRowValue &lock_row_value,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLockAtomicProtocol lock_row;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
  } else if (OB_FAIL(prepare_lock_row_(lock_row))) {
    LOG_WARN("failed to prepare lock row", K(ret));
  } else {
    ObSSMetaCtrlLockRowValue new_lock_row_value = lock_row_value;
    new_lock_row_value.op_id_++;
    // create() only bumps op_id; lock_row.meta_scn_ (last committed business meta) is
    // updated at finish() when meta_scn is allocated.
    if (OB_FAIL(lock_row.update_lock_row(new_lock_row_value, trans))) {
      LOG_WARN("failed to update lock row", K(ret), K(new_lock_row_value));
    } else {
      op_id_ = lock_row_value.op_id_;
    }
  }
  return ret;
}

int ObTabletAtomicProtocol::prepare_lock_row_(
    ObLockAtomicProtocol &lock_row)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlKey ctrl_key;
  ObSSMetaCtrlKey lock_key;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
  } else if (OB_FAIL(prepare_ss_meta_ctrl_key_(ctrl_key))) {
    LOG_WARN("failed to prepare ss meta ctrl key", K(ret));
  } else if (OB_FAIL(prepare_lock_row_key_(ctrl_key, lock_key))) {
    LOG_WARN("failed to prepare lock row key", K(ret));
  } else if (OB_FAIL(lock_row.init(tenant_id_, ls_id_, lock_key))) {
    LOG_WARN("failed to init lock row", K(ret), K(ls_id_));
  }
  return ret;
}

int ObTabletAtomicProtocol::prepare_ss_meta_ctrl_key_(
    ObSSMetaCtrlKey &ctrl_key)
{
  int ret = OB_SUCCESS;
  ctrl_key.reset();
  ObSSMetaCtrlType type;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
  } else if (OB_FAIL(type_.get_ss_meta_ctrl_type(type))) {
    LOG_WARN("failed to get ss meta ctrl type", K(ret), K(type_));
  } else {
    ctrl_key.assign_type(type);
    ctrl_key.tablet_key_.tablet_id_ = tablet_id_;
    ctrl_key.tablet_key_.reorg_scn_ = reorg_scn_;
    ctrl_key.tablet_key_.control_scn_ = control_scn_;
    ctrl_key.tablet_key_.ls_id_ = ls_id_;
  }
  return ret;
}

int ObTabletAtomicProtocol::DeleteKeyInfosOP::process(
    common::ObMySQLTransaction &trans,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(user_op_.process(trans, meta_scn))) {
    LOG_WARN("failed to do user op process", K(ret));
  } else if (OB_FAIL(owner_.delete_key_infos_(trans))) {
    LOG_WARN("failed to delete key infos", K(ret));
  }
  return ret;
}

int ObTabletAtomicProtocol::finish(ObIAtomicProtocolOP &op)
{
  int ret = OB_SUCCESS;
  const bool need_delete_row = true;
  const bool need_check_op_id = true;
  DeleteKeyInfosOP composite_op(*this, op);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
  } else if (OB_FAIL(finish_(
      tenant_id_, ls_id_, tablet_id_, need_delete_row, need_check_op_id, composite_op))) {
    LOG_WARN("failed to do tablet atomic protocol finish",
             K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  }
  return ret;
}

int ObTabletAtomicProtocol::abort()
{
  int ret = OB_SUCCESS;
  AbandonKeyInfosOP composite_op(*this);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
  } else if (OB_FAIL(abort_(tenant_id_, ls_id_, composite_op))) {
    LOG_WARN("failed to do abort", K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  }
  return ret;
}

int ObTabletAtomicProtocol::abort_self()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
  } else if (OB_FAIL(abort_(tenant_id_, ls_id_))) {
    LOG_WARN("failed to abort tablet atomic protocol itself",
        K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  }
  return ret;
}

int ObTabletAtomicProtocol::delete_key_infos_(ObMySQLTransaction &trans)
{
  return handle_key_infos_(KeyInfoAction::DELETE_ROW, trans);
}

int ObTabletAtomicProtocol::AbandonKeyInfosOP::process(
    common::ObMySQLTransaction &trans,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  UNUSED(meta_scn);
  if (OB_FAIL(owner_.abandon_key_infos_(trans))) {
    LOG_WARN("failed to abandon key infos", K(ret));
  }
  return ret;
}

int ObTabletAtomicProtocol::abandon_key_infos_(ObMySQLTransaction &trans)
{
  return handle_key_infos_(KeyInfoAction::ABANDON_ROW, trans);
}

int ObTabletAtomicProtocol::lock_key_info_lock_row_(
    const ObSSMetaCtrlKeyInfo &key_info,
    hash::ObHashSet<ObSSMetaCtrlType> &lock_set,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlType type;
  int hash_ret = OB_SUCCESS;
  if (!key_info.is_valid() || !key_info.key_.is_sstable_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key info type is not sstable, unexpected", K(ret), K(key_info));
  } else if (OB_FAIL(key_info.key_.get_lock_meta_type(type))) {
    LOG_WARN("failed to get lock type", K(ret), K(key_info));
  } else if (!type.is_sstable_type()
      || key_info.tenant_id_ != tenant_id_
      || key_info.ls_id_ != ls_id_
      || key_info.key_.sstable_key_.tablet_id_ != tablet_id_
      || (!tablet_id_.is_ls_inner_tablet()
          && key_info.key_.sstable_key_.reorg_scn_ != reorg_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    if (!type.is_sstable_type()) {
      LOG_WARN("lock type is not sstable type, unexpected", K(ret), K(type), K(key_info));
    } else {
      LOG_WARN("key info does not match current tablet/reorg", K(ret), K(key_info),
          K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn));
    }
  } else if (FALSE_IT(hash_ret = lock_set.exist_refactored(type))) {
  } else if (OB_HASH_NOT_EXIST == hash_ret) {
    ObLockAtomicProtocol lock_row;
    ObSSMetaCtrlKey lock_row_key;
    ObSSMetaCtrlLockRowValue unused_lock_row_value;
    if (OB_FAIL(prepare_lock_row_key_(key_info.key_, lock_row_key))) {
      LOG_WARN("failed to prepare lock row key", K(ret), K(key_info));
    } else if (OB_FAIL(lock_row.init(tenant_id_, ls_id_, lock_row_key))) {
      LOG_WARN("failed to init lock row", K(ret), K(tenant_id_), K(ls_id_), K(lock_row_key));
    } else if (OB_FAIL(lock_row.lock_lock_row(trans, unused_lock_row_value))) {
      LOG_WARN("failed to lock row", K(ret), K(tenant_id_), K(ls_id_), K(lock_row_key));
    } else if (OB_FAIL(lock_set.set_refactored(type))) {
      LOG_WARN("failed to set lock type", K(ret), K(tenant_id_), K(ls_id_), K(lock_row_key));
    }
  } else if (OB_HASH_EXIST == hash_ret) {
    //do nothing
  } else {
    ret = ((OB_SUCCESS == hash_ret) ? OB_ERR_UNEXPECTED : hash_ret);
    LOG_WARN("failed to get type from lock set", K(ret), K(hash_ret), K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  }
  return ret;
}

int ObTabletAtomicProtocol::handle_key_infos_(
    const KeyInfoAction action,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<ObSSMetaCtrlType> lock_set;
  ObSSMetaCtrlValue ctrl_value;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet atomic protocol do not init", K(ret));
  } else if (key_infos_.empty()) {
    //do nothing
  } else if (OB_FAIL(lock_set.create(key_infos_.count()))) {
    LOG_WARN("failed to create lock map", K(ret), K(tenant_id_), K(ls_id_), K(tablet_id_), K(reorg_scn_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_infos_.count(); ++i) {
      const ObSSMetaCtrlKeyInfo &key_info = key_infos_.at(i);
      ctrl_value.reset();
      // Lock the sstable lock row (tablet -> sstable order) before touching its ctrl row.
      if (OB_FAIL(lock_key_info_lock_row_(key_info, lock_set, trans))) {
        LOG_WARN("failed to lock key info lock row", K(ret), K(key_info));
      } else if (KeyInfoAction::ABANDON_ROW == action) {
        if (OB_FAIL(select_ctrl_row_(tenant_id_, ls_id_, key_info.key_, trans, ctrl_value))) {
          if (OB_ITER_END == ret) {
            // sstable ctrl row does not exist, nothing to abandon (e.g. abort retried
            // after a previous round, or the sstable protocol never created its row).
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to select ctrl row", K(ret), K(tenant_id_), K(ls_id_), K(key_info));
          }
        } else if (ctrl_value.ctrl_info_.status_.is_abandon_status()) {
          // already abandoned, keep abort idempotent.
        } else if (!ctrl_value.ctrl_info_.status_.is_start_status()
            && !ctrl_value.ctrl_info_.status_.is_complete_status()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sstable ctrl value status is unexpected", K(ret), K(key_info), K(ctrl_value));
        } else if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_abandon())) {
          LOG_WARN("failed to set status abandon", K(ret), K(tenant_id_), K(ls_id_), K(key_info), K(ctrl_value));
        } else if (OB_FAIL(update_ctrl_row_(tenant_id_, ls_id_, key_info.key_, ctrl_value, trans))) {
          LOG_WARN("failed to update ctrl row", K(ret), K(tenant_id_), K(ls_id_), K(key_info), K(ctrl_value));
        }
      } else { // KeyInfoAction::DELETE_ROW
        if (OB_FAIL(select_ctrl_row_(tenant_id_, ls_id_, key_info.key_, trans, ctrl_value))) {
          LOG_WARN("failed to select ctrl row", K(ret), K(tenant_id_), K(ls_id_), K(key_info));
        } else if (!ctrl_value.ctrl_info_.status_.is_complete_status()) {
          ret = OB_NEED_RETRY;
          LOG_WARN("sstable ctrl value status is not complete, may already canceled", K(ret), K(key_info), K(ctrl_value));
        } else if (OB_FAIL(delete_ctrl_row_(tenant_id_, ls_id_, key_info.key_, trans))) {
          LOG_WARN("failed to delete ctrl row", K(ret), K(tenant_id_), K(ls_id_), K(key_info));
        }
      }
    }
  }
  return ret;
}
