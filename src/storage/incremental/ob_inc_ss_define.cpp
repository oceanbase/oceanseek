/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ob_inc_ss_define.h"

namespace oceanbase
{
namespace storage
{
OB_SERIALIZE_MEMBER(ObSSWriterKey,
                    type_,
                    ls_id_,
                    tablet_id_);

void ObSSWriterAddr::reset()
{
  addr_.reset();
  epoch_ = -1;
}

int ObSSWriterAddr::is_valid() const
{
  return addr_.is_valid() && epoch_ > 0;
}

ObSSWriterAddr &ObSSWriterAddr::operator=(const ObSSWriterAddr &sswriter_addr)
{
  if (this != &sswriter_addr) {
    addr_ = sswriter_addr.addr_;
    epoch_ = sswriter_addr.epoch_;
  }
  return *this;
}

ObSSWriterKey::ObSSWriterKey(
    const ObSSWriterType type)
{
  type_ = type;
}

ObSSWriterKey::ObSSWriterKey(
    const ObSSWriterType type,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  type_ = type;
  ls_id_ = ls_id;
  tablet_id_ = tablet_id;
}

ObSSWriterKey::ObSSWriterKey(
    const ObSSWriterType type,
    const share::ObLSID &ls_id)
{
  type_ = type;
  ls_id_ = ls_id;
}

void ObSSWriterKey::reset()
{
  type_ = ObSSWriterType::INVALID;
  ls_id_.reset();
  tablet_id_.reset();
}

bool ObSSWriterKey::is_valid() const
{
  bool is_valid = false;
  if (ObSSWriterType::FREEZE == type_) {
    if (ls_id_.is_valid() && tablet_id_.is_valid()) {
      is_valid = true;
    }
  } else if (ObSSWriterType::COMPACTION == type_) {
    if (ls_id_.is_valid()) {
      is_valid = true;
    }
  } else if (ObSSWriterType::META == type_) {
    if (ls_id_.is_valid()) {
      is_valid = true;
    }
  } else if (ObSSWriterType::GC == type_) {
    is_valid = true;
  } else {
    // is_valid is false
  }
  return is_valid;
}

ObSSWriterKey &ObSSWriterKey::operator=(const ObSSWriterKey &key)
{
  if (this != &key) {
    type_ = key.type_;
    ls_id_ = key.ls_id_;
    tablet_id_ = key.tablet_id_;
  }
  return *this;
}

bool ObSSWriterKey::operator==(const ObSSWriterKey &key) const
{
  return !(*this != key);
}

bool ObSSWriterKey::operator!=(const ObSSWriterKey &key) const
{
  return type_ != key.type_
         || ls_id_ != key.ls_id_
         || tablet_id_ != key.tablet_id_;
}

int ObSSWriterKey::compare(const ObSSWriterKey &other) const
{
  int compare_ret = 0;
  if (type_ < other.type_) {
    compare_ret = -1;
  } else if (type_ > other.type_) {
    compare_ret = 1;
  } else if (ls_id_ < other.ls_id_) {
    compare_ret = -1;
  } else if (ls_id_ > other.ls_id_) {
    compare_ret = 1;
  } else if (tablet_id_ < other.tablet_id_) {
    compare_ret = -1;
  } else if (tablet_id_ > other.tablet_id_) {
    compare_ret = 1;
  } else {
    compare_ret = 0;
  }
  return compare_ret;
}

share::ObLSID ObSSWriterKey::get_mgr_ls_id() const
{
  share::ObLSID mgr_ls_id(-1);
  switch (type_) {
  case ObSSWriterType::FREEZE:
  case ObSSWriterType::COMPACTION:
  case ObSSWriterType::META:
    mgr_ls_id = ls_id_;
    break;
  case ObSSWriterType::GC:
    mgr_ls_id = share::SYS_LS;
    break;
  default: {
    // mgr_ls_id(-1)
  }
  }
  return mgr_ls_id;
}

ObLsSSWriterKey::ObLsSSWriterKey(const ObSSWriterType type, const share::ObLSID &ls_id)
{
  type_ = type;
  ls_id_ = ls_id;
}

void ObLsSSWriterKey::reset()
{
  type_ = ObSSWriterType::INVALID;
  ls_id_.reset();
}

bool ObLsSSWriterKey::is_valid() const
{
  bool is_valid = false;
  if (ObSSWriterType::FREEZE == type_
      || ObSSWriterType::COMPACTION == type_
      || ObSSWriterType::META == type_) {
    if (ls_id_.is_valid()) {
      is_valid = true;
    }
  } else {
    // is_valid is false
  }
  return is_valid;
}

ObLsSSWriterKey &ObLsSSWriterKey::operator=(const ObLsSSWriterKey &key)
{
  if (this != &key) {
    type_ = key.type_;
    ls_id_ = key.ls_id_;
  }
  return *this;
}

bool ObLsSSWriterKey::operator==(const ObLsSSWriterKey &key) const
{
  return !(*this != key);
}

bool ObLsSSWriterKey::operator!=(const ObLsSSWriterKey &key) const
{
  return type_ != key.type_
         || ls_id_ != key.ls_id_;
}

int ObLsSSWriterKey::compare(const ObLsSSWriterKey &other) const
{
  int compare_ret = 0;
  if (type_ < other.type_) {
    compare_ret = -1;
  } else if (type_ > other.type_) {
    compare_ret = 1;
  } else if (ls_id_ < other.ls_id_) {
    compare_ret = -1;
  } else if (ls_id_ > other.ls_id_) {
    compare_ret = 1;
  } else {
    compare_ret = 0;
  }
  return compare_ret;
}

} // storage
} // oceanbase

