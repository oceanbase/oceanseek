/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_ss_transfer_backfill_struct.h"

namespace oceanbase 
{
namespace storage
{

ObSSTransferBackfillType::ObSSTransferBackfillType()
  : type_(ObSSTransferBackfillType::BACKFILL_MAX)
{
  static_assert(sizeof(type_str_array_) / sizeof(const char *) == static_cast<uint8_t>(ObSSTransferBackfillType::BACKFILL_MAX),
      "backfill type str array size does not equal to enum value count");
}
ObSSTransferBackfillType::ObSSTransferBackfillType(const TYPE &type)
  : type_(type)
{
}

ObSSTransferBackfillType::operator TYPE() const
{
  return type_;
}

ObSSTransferBackfillType &ObSSTransferBackfillType::operator=(
    const TYPE &backfill_type)
{
  type_ = backfill_type;
  return *this;
}

uint64_t ObSSTransferBackfillType::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&type_, sizeof(type_), hash_val);
  return hash_val;
}

void ObSSTransferBackfillType::reset()
{
  type_ = BACKFILL_MAX;
}

bool ObSSTransferBackfillType::is_valid() const
{
  return type_ >= BACKFILL_UPLOAD
      && type_ < BACKFILL_MAX;
}

const char *ObSSTransferBackfillType::get_str(const ObSSTransferBackfillType &backfill_type)
{
  const char *str = nullptr;
  if (backfill_type.type_ >= ObSSTransferBackfillType::BACKFILL_MAX) {
    str = "UNKNOWN";
  } else {
    str = type_str_array_[backfill_type.type_];
  }
  return str;
}

bool ObSSTransferBackfillType::operator==(const TYPE &backfill_type)
{
  return type_ == backfill_type;
}

bool ObSSTransferBackfillType::operator!=(const TYPE &backfill_type)
{
  return type_ != backfill_type;
}

ObSSTransferBackfillTXParam::ObSSTransferBackfillTXParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    task_id_(),
    src_ls_id_(),
    dest_ls_id_(),
    tablet_infos_(),
    backfill_type_(),
    dag_net_id_()
{
}

bool ObSSTransferBackfillTXParam::is_valid() const
{
  return tenant_id_ > 0 
      && task_id_.is_valid() 
      && src_ls_id_.is_valid()
      && dest_ls_id_.is_valid()
      && backfill_type_.is_valid()
      && !tablet_infos_.empty()
      && dag_net_id_.is_valid();
}

void ObSSTransferBackfillTXParam::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  task_id_.reset();
  src_ls_id_.reset();
  dest_ls_id_.reset();
  tablet_infos_.reset();
  backfill_type_.reset();
  dag_net_id_.reset();
}

bool ObBackfillScnCompare::operator()(
    const ObTabletBackfillInfo &linfo, const ObTabletBackfillInfo &rinfo) const
{
  bool bret = false;
  if (OB_SUCCESS != result_code_) {
  } else if (OB_SUCCESS != (result_code_ = compare_by_backfill_scn_(linfo, rinfo, bret))) {
    LOG_WARN_RET(result_code_, "failed to compare with backfill scn", K(result_code_), K(linfo), K(rinfo));
  }
  return bret;
}

int ObBackfillScnCompare::compare_by_backfill_scn_(
    const ObTabletBackfillInfo &linfo, const ObTabletBackfillInfo &rinfo, bool &bret) const
{
  int ret = OB_SUCCESS;
  bret = false;
  if (OB_UNLIKELY(!linfo.is_valid() || !rinfo.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("left/right info must be valid", K(ret), K(linfo), K(rinfo));
  } else if (linfo.backfill_scn_ == rinfo.backfill_scn_) {
    bret = linfo.tablet_id_ < rinfo.tablet_id_;
  } else {
    bret = linfo.backfill_scn_ < rinfo.backfill_scn_;
  }
  return ret;
}

/****************** ObSSTransferBackfillTXCtx ******************/
ObSSTransferBackfillTXCtx::ObSSTransferBackfillTXCtx()
  : ObTransferBackfillTXBaseCtx(),
    backfill_type_(),
    start_time_(0),
    finish_time_(0),
    dag_net_id_(),
    replace_scn_range_(),
    replace_ss_clog_ckpt_scn_(share::SCN::min_scn())
{
  replace_scn_range_.reset();
}

ObSSTransferBackfillTXCtx::~ObSSTransferBackfillTXCtx()
{
}

void ObSSTransferBackfillTXCtx::reuse()
{
  backfill_type_.reset();
  start_time_ = 0;
  finish_time_ = 0;
  dag_net_id_.reset();
  replace_scn_range_.reset();
  replace_ss_clog_ckpt_scn_ = share::SCN::min_scn();
  ObTransferBackfillTXBaseCtx::reuse();
}

void ObSSTransferBackfillTXCtx::reset()
{
  backfill_type_.reset();
  start_time_ = 0;
  finish_time_ = 0;
  dag_net_id_.reset();
  replace_scn_range_.reset();
  replace_ss_clog_ckpt_scn_ = share::SCN::min_scn();
  ObTransferBackfillTXBaseCtx::reset();
}

int ObSSTransferBackfillTXCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    ret = databuff_printf(buf, buf_len, pos, "transfer backfill tx ctx : ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, "backfill_type = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, backfill_type_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tenant_id = %lu", tenant_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", task_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, task_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", src_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, src_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dest_ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, dest_ls_id_);
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", dag_net_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, dag_net_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
    }
  }

  return ret;
}

bool ObSSTransferBackfillTXCtx::is_valid() const
{
  return ObTransferBackfillTXBaseCtx::is_valid()
      && backfill_type_.is_valid() 
      && dag_net_id_.is_valid();
}

int ObSSTransferBackfillTXCtx::init_ls_handle()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;

  if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret));
  } else {
    // for ss upload and backfill, src ls handle need to be held (backfill need src tx data)
    // for ss refresh, dest ls handle need to be held (refresh need dest ls handle)
    switch (backfill_type_) {
      case ObSSTransferBackfillType::BACKFILL_UPLOAD:
      case ObSSTransferBackfillType::BACKFILL_TX: {
        if (OB_FAIL(ls_service->get_ls(src_ls_id_, src_ls_handle_, ObLSGetMod::HA_MOD))) {
          LOG_WARN("failed to get src ls", K(ret), K(src_ls_id_));
        } else if (OB_ISNULL(ls = src_ls_handle_.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("src ls should not be NULL", K(ret), K(src_ls_handle_));
        } 
        break;
      }
      case ObSSTransferBackfillType::BACKFILL_REPLACE_TABLE:
      case ObSSTransferBackfillType::BACKFILL_REFRESH: {
        if (OB_FAIL(ls_service->get_ls(dest_ls_id_, dest_ls_handle_, ObLSGetMod::HA_MOD))) {
          LOG_WARN("failed to get dest ls", K(ret), K(dest_ls_id_));
        } else if (OB_ISNULL(ls = dest_ls_handle_.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dest ls should not be NULL", K(ret), K(dest_ls_handle_));
        }
        break;
      }
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid backfill type", K(ret), K(backfill_type_));
        break;
    }

    if (OB_SUCC(ret)) {
      if (!ls->is_running()) {
        ret = OB_LS_OFFLINE;
        LOG_WARN("ls is not running, should not do transfer backfill tx", K(ret), KPC(ls));
      }
    }
  }
  return ret;
}

int ObSSTransferBackfillTXCtx::get_src_ls(ObLS *&src_ls)
{
  int ret = OB_SUCCESS;
  src_ls = nullptr;
  if (!allow_to_get_src_ls()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not allow to get src ls", K(ret), KPC(this));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::get_src_ls(src_ls))) {
    LOG_WARN("failed to get src ls", K(ret));
  }
  return ret;
}

int ObSSTransferBackfillTXCtx::get_dest_ls(ObLS *&dest_ls)
{
  int ret = OB_SUCCESS;
  dest_ls = nullptr;
  if (!allow_to_get_dest_ls()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not allow to get dest ls", K(ret), KPC(this));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::get_dest_ls(dest_ls))) {
    LOG_WARN("failed to get dest ls", K(ret));
  }
  return ret;
}

int ObSSTransferBackfillTXCtx::get_src_ls_handle(ObLSHandle &src_ls_handle)
{
  int ret = OB_SUCCESS;
  src_ls_handle.reset();
  if (!allow_to_get_src_ls()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not allow to get src ls", K(ret), KPC(this));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::get_src_ls_handle(src_ls_handle))) {
    LOG_WARN("failed to get src ls", K(ret));
  }
  return ret;
}

int ObSSTransferBackfillTXCtx::get_dest_ls_handle(ObLSHandle &dest_ls_handle)
{
  int ret = OB_SUCCESS;
  dest_ls_handle.reset();
  if (!allow_to_get_dest_ls()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("not allow to get dest ls", K(ret), KPC(this));
  } else if (OB_FAIL(ObTransferBackfillTXBaseCtx::get_dest_ls_handle(dest_ls_handle))) {
    LOG_WARN("failed to get dest ls", K(ret));
  }
  return ret;
}

share::ObLSID ObSSTransferBackfillTXCtx::get_ls_id() const
{
  ObLSID ls_id;
  if (allow_to_get_src_ls()) {
    ls_id = src_ls_id_;
  } else if (allow_to_get_dest_ls()) {
    ls_id = dest_ls_id_;
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid backfill type", KPC(this));
  }
  return ls_id;
}

int ObSSTransferBackfillTXCtx::get_ls(ObLS *&ls)
{
  int ret = OB_SUCCESS;
  if (allow_to_get_src_ls()) {
    if (OB_FAIL(get_src_ls(ls))) {
      LOG_WARN("failed to get src ls", K(ret));
    }
  } else if (allow_to_get_dest_ls()) {
    if (OB_FAIL(get_dest_ls(ls))) {
      LOG_WARN("failed to get dest ls", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backfill type", K(ret), KPC(this));
  }
  return ret;
}

bool ObSSTransferBackfillTXCtx::allow_to_get_src_ls() const
{
  return backfill_type_ == ObSSTransferBackfillType::BACKFILL_UPLOAD
      || backfill_type_ == ObSSTransferBackfillType::BACKFILL_TX;
}

bool ObSSTransferBackfillTXCtx::allow_to_get_dest_ls() const
{
  return backfill_type_ == ObSSTransferBackfillType::BACKFILL_REPLACE_TABLE
      || backfill_type_ == ObSSTransferBackfillType::BACKFILL_REFRESH;
}

} //storage
} //oceanbase
