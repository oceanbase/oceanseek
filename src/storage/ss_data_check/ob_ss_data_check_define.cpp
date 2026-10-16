/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_define.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

bool ObSSDataCheckModeUtil::is_valid(const ObSSDataCheckMode mode)
{
  return ObSSDataCheckMode::OFF == mode || ObSSDataCheckMode::SINGLE == mode
      || ObSSDataCheckMode::FULL == mode;
}

bool ObSSDataCheckModeUtil::is_enabled(const ObSSDataCheckMode mode)
{
  return ObSSDataCheckMode::SINGLE == mode || ObSSDataCheckMode::FULL == mode;
}

int ObSSDataCheckModeUtil::parse(
    const ObString &mode_str,
    ObSSDataCheckMode &mode)
{
  int ret = OB_SUCCESS;
  mode = ObSSDataCheckMode::MAX;
  if (0 == mode_str.case_compare("OFF")) {
    mode = ObSSDataCheckMode::OFF;
  } else if (0 == mode_str.case_compare("SINGLE")) {
    mode = ObSSDataCheckMode::SINGLE;
  } else if (0 == mode_str.case_compare("FULL")) {
    mode = ObSSDataCheckMode::FULL;
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid shared storage data check mode", KR(ret), K(mode_str));
  }
  return ret;
}

const char *ObSSDataCheckModeUtil::to_string(const ObSSDataCheckMode mode)
{
  const char *str = "INVALID";
  if (ObSSDataCheckMode::OFF == mode) {
    str = "OFF";
  } else if (ObSSDataCheckMode::SINGLE == mode) {
    str = "SINGLE";
  } else if (ObSSDataCheckMode::FULL == mode) {
    str = "FULL";
  }
  return str;
}

ObSSDataCheckExpectedItem::ObSSDataCheckExpectedItem()
  : macro_id_(),
    ls_id_(),
    tablet_id_(),
    reorganize_scn_(),
    meta_scn_()
{
}

int ObSSDataCheckExpectedItem::init(
    const MacroBlockId &macro_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &reorganize_scn,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid() || !macro_id.is_in_object_storage() || !ls_id.is_valid()
      || !tablet_id.is_valid() || !reorganize_scn.is_valid() || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check expected item", KR(ret), K(macro_id),
        K(ls_id), K(tablet_id), K(reorganize_scn), K(meta_scn));
  } else {
    macro_id_ = macro_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    reorganize_scn_ = reorganize_scn;
    meta_scn_ = meta_scn;
  }
  return ret;
}

void ObSSDataCheckExpectedItem::reset()
{
  macro_id_.reset();
  ls_id_.reset();
  tablet_id_.reset();
  reorganize_scn_.reset();
  meta_scn_.reset();
}

bool ObSSDataCheckExpectedItem::is_valid() const
{
  return macro_id_.is_valid() && macro_id_.is_in_object_storage() && ls_id_.is_valid()
      && tablet_id_.is_valid() && reorganize_scn_.is_valid() && meta_scn_.is_valid();
}

int ObSSDataCheckExpectedItem::assign(
    const ObSSDataCheckExpectedItem &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check expected item to assign", KR(ret), K(other));
  } else {
    macro_id_ = other.macro_id_;
    ls_id_ = other.ls_id_;
    tablet_id_ = other.tablet_id_;
    reorganize_scn_ = other.reorganize_scn_;
    meta_scn_ = other.meta_scn_;
  }
  return ret;
}

bool ObSSDataCheckExpectedItem::is_same_object(
    const ObSSDataCheckExpectedItem &other) const
{
  return macro_id_ == other.macro_id_;
}

bool ObSSDataCheckExpectedItem::operator==(
    const ObSSDataCheckExpectedItem &other) const
{
  return is_same_object(other);
}

bool ObSSDataCheckExpectedItem::operator!=(
    const ObSSDataCheckExpectedItem &other) const
{
  return !(*this == other);
}

int64_t ObSSDataCheckExpectedItem::get_deep_copy_size() const
{
  return 0;
}

int ObSSDataCheckExpectedItem::deep_copy(
    const ObSSDataCheckExpectedItem &src,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  UNUSEDx(buf, buf_len, pos);
  macro_id_ = src.macro_id_;
  ls_id_ = src.ls_id_;
  tablet_id_ = src.tablet_id_;
  reorganize_scn_ = src.reorganize_scn_;
  meta_scn_ = src.meta_scn_;
  return OB_SUCCESS;
}

OB_DEF_SERIALIZE(ObSSDataCheckExpectedItem)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check expected item", KR(ret), KPC(this));
  } else {
    OB_UNIS_ENCODE(macro_id_);
    OB_UNIS_ENCODE(ls_id_);
    OB_UNIS_ENCODE(tablet_id_);
    OB_UNIS_ENCODE(reorganize_scn_);
    OB_UNIS_ENCODE(meta_scn_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSSDataCheckExpectedItem)
{
  int ret = OB_SUCCESS;
  reset();
  OB_UNIS_DECODE(macro_id_);
  OB_UNIS_DECODE(ls_id_);
  OB_UNIS_DECODE(tablet_id_);
  OB_UNIS_DECODE(reorganize_scn_);
  OB_UNIS_DECODE(meta_scn_);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("deserialized invalid data check expected item", KR(ret), KPC(this));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObSSDataCheckExpectedItem)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(macro_id_);
  OB_UNIS_ADD_LEN(ls_id_);
  OB_UNIS_ADD_LEN(tablet_id_);
  OB_UNIS_ADD_LEN(reorganize_scn_);
  OB_UNIS_ADD_LEN(meta_scn_);
  return len;
}

ObSSDataCheckSortedFileInfo::ObSSDataCheckSortedFileInfo()
  : file_handle_(), item_count_(0)
{
}

ObSSDataCheckSortedFileInfo::~ObSSDataCheckSortedFileInfo()
{
  reset();
}

int ObSSDataCheckSortedFileInfo::init(
    const ObMarkSweepTmpFileHandle &file_handle,
    const int64_t item_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_handle.is_valid() || item_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check sorted file info", KR(ret), K(file_handle), K(item_count));
  } else {
    reset();
    if (OB_FAIL(file_handle_.assign(file_handle))) {
      LOG_WARN("failed to assign data check tmp file handle", KR(ret), K(file_handle));
    } else {
      item_count_ = item_count;
    }
  }
  return ret;
}

int ObSSDataCheckSortedFileInfo::assign(
    const ObSSDataCheckSortedFileInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid data check sorted file info", KR(ret), K(other));
    } else if (OB_FAIL(init(other.file_handle_, other.item_count_))) {
      LOG_WARN("failed to assign data check sorted file info", KR(ret), K(other));
    }
  }
  return ret;
}

void ObSSDataCheckSortedFileInfo::reset()
{
  file_handle_.reset();
  item_count_ = 0;
}

bool ObSSDataCheckSortedFileInfo::is_valid() const
{
  return file_handle_.is_valid() && item_count_ > 0;
}

} // namespace storage
} // namespace oceanbase
