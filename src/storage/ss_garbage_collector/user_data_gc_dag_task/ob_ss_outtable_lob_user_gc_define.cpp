/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_outtable_lob_user_gc_define.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;

ObSSOuttableLobUserGCRoundParam::ObSSOuttableLobUserGCRoundParam()
  : tenant_id_(OB_INVALID_TENANT_ID), round_id_(0), safe_tx_scn_()
{}

void ObSSOuttableLobUserGCRoundParam::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  round_id_ = 0;
  safe_tx_scn_.reset();
}

bool ObSSOuttableLobUserGCRoundParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && round_id_ > 0 && safe_tx_scn_.is_valid_and_not_min();
}

OB_SERIALIZE_MEMBER(ObSSOuttableLobCtrlCandidate, macro_id_, hash_, mark_row_scn_);

ObSSOuttableLobCtrlCandidate::ObSSOuttableLobCtrlCandidate()
  : macro_id_(), hash_(0), mark_row_scn_(0)
{}

void ObSSOuttableLobCtrlCandidate::reset()
{
  macro_id_.reset();
  hash_ = 0;
  mark_row_scn_ = 0;
}

bool ObSSOuttableLobCtrlCandidate::is_valid() const
{
  return macro_id_.is_valid() && mark_row_scn_ > 0 && mark_row_scn_ < INT64_MAX;
}

int64_t ObSSOuttableLobCtrlCandidate::get_deep_copy_size() const
{
  return 0;
}

int ObSSOuttableLobCtrlCandidate::deep_copy(
    const ObSSOuttableLobCtrlCandidate &src, char *buf, int64_t len, int64_t &pos)
{
  UNUSEDx(buf, len, pos);
  *this = src;
  return OB_SUCCESS;
}

ObSSLobCtrlGCScanCompare::ObSSLobCtrlGCScanCompare() : result_code_(OB_SUCCESS)
{}

bool ObSSLobCtrlGCScanCompare::operator()(
    const ObSSOuttableLobCtrlCandidate *left, const ObSSOuttableLobCtrlCandidate *right)
{
  bool bret = false;
  if (OB_ISNULL(left) || OB_ISNULL(right) || !left->is_valid() || !right->is_valid()) {
    result_code_ = OB_INVALID_ARGUMENT;
  } else {
    bret = left->macro_id_ < right->macro_id_;
  }
  return bret;
}

ObSSLobCtrlGCScanFile::ObSSLobCtrlGCScanFile() : file_handle_(), sample_item_(), item_count_(0)
{}

ObSSLobCtrlGCScanFile::~ObSSLobCtrlGCScanFile()
{
  reset();
}

int ObSSLobCtrlGCScanFile::assign(const ObSSLobCtrlGCScanFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(file_handle_.assign(other.file_handle_))) {
      LOG_WARN("failed to assign lob ctrl scan file handle", KR(ret), K(other));
    } else {
      sample_item_ = other.sample_item_;
      item_count_ = other.item_count_;
    }
  }
  return ret;
}

void ObSSLobCtrlGCScanFile::reset()
{
  file_handle_.reset();
  sample_item_.reset();
  item_count_ = 0;
}

bool ObSSLobCtrlGCScanFile::is_valid() const
{
  return file_handle_.is_valid() && sample_item_.is_valid() && item_count_ > 0;
}

int64_t ObSSLobCtrlGCScanFile::get_fd() const
{
  return file_handle_.get_fd();
}

int64_t ObSSLobCtrlGCScanFile::get_dir_id() const
{
  return file_handle_.get_dir_id();
}

} // namespace storage
} // namespace oceanbase
