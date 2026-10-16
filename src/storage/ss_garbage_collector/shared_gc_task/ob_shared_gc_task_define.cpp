/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_gc_task_define.h"
#include "lib/charset/ob_charset.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

OB_SERIALIZE_MEMBER(ObSharedGCMacroIdList, macro_id_array_);
OB_SERIALIZE_MEMBER(ObSharedGCFailInfo,
                    meta_type_, op_id_,
                    ls_id_, tablet_id_, transfer_scn_,
                    ctrl_object_info_);
OB_SERIALIZE_MEMBER(ObSharedGCLSMetaInfo, ls_id_, meta_scn_);
OB_SERIALIZE_MEMBER(ObDeleteTenantTaskValue, tenant_id_, delete_ts_);
OB_SERIALIZE_MEMBER(ObDeleteServerTaskValue, server_id_, server_addr_, delete_ts_);

const char *ob_shared_gc_task_type_to_string(const ObSharedGCTaskType type)
{
  const char *str = "UNKNOWN";
  switch (type) {
    case ObSharedGCTaskType::INVALID_TYPE: {
      str = "INVALID_TYPE";
      break;
    }
    case ObSharedGCTaskType::MARK_SWEEP_GC: {
      str = "MARK_SWEEP_GC";
      break;
    }
    case ObSharedGCTaskType::FAIL_GC: {
      str = "FAIL_GC";
      break;
    }
    case ObSharedGCTaskType::FAIL_GC_MACRO_ID: {
      str = "FAIL_GC_MACRO_ID";
      break;
    }
    case ObSharedGCTaskType::LS_META_GC: {
      str = "LS_META_GC";
      break;
    }
    case ObSharedGCTaskType::DELETE_TENANT: {
      str = "DELETE_TENANT";
      break;
    }
    case ObSharedGCTaskType::DELETE_SERVER: {
      str = "DELETE_SERVER";
      break;
    }
    case ObSharedGCTaskType::LS_CKPT_GC: {
      str = "LS_CKPT_GC";
      break;
    }
    case ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT: {
      str = "MARK_SWEEP_GC_IN_TABLE_STAT";
      break;
    }
    case ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT: {
      str = "MARK_SWEEP_GC_OBJECT_STORAGE_STAT";
      break;
    }
    case ObSharedGCTaskType::MARK_SWEEP_GC_DIAG: {
      str = "MARK_SWEEP_GC_DIAG";
      break;
    }
    case ObSharedGCTaskType::MAX_TYPE: {
      str = "MAX_TYPE";
      break;
    }
    default: {
      str = "UNKNOWN";
      break;
    }
  }
  return str;
}

ObSharedGCTaskType ob_string_to_shared_gc_task_type(const ObString &str)
{
  ObSharedGCTaskType type = ObSharedGCTaskType::INVALID_TYPE;
  if (str == ObString::make_string("MARK_SWEEP_GC")) {
    type = ObSharedGCTaskType::MARK_SWEEP_GC;
  } else if (str == ObString::make_string("FAIL_GC")) {
    type = ObSharedGCTaskType::FAIL_GC;
  } else if (str == ObString::make_string("FAIL_GC_MACRO_ID")) {
    type = ObSharedGCTaskType::FAIL_GC_MACRO_ID;
  } else if (str == ObString::make_string("LS_META_GC")) {
    type = ObSharedGCTaskType::LS_META_GC;
  } else if (str == ObString::make_string("DELETE_TENANT")) {
    type = ObSharedGCTaskType::DELETE_TENANT;
  } else if (str == ObString::make_string("DELETE_SERVER")) {
    type = ObSharedGCTaskType::DELETE_SERVER;
  } else if (str == ObString::make_string("LS_CKPT_GC")) {
    type = ObSharedGCTaskType::LS_CKPT_GC;
  } else if (str == ObString::make_string("MARK_SWEEP_GC_IN_TABLE_STAT")) {
    type = ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT;
  } else if (str == ObString::make_string("MARK_SWEEP_GC_OBJECT_STORAGE_STAT")) {
    type = ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT;
  } else if (str == ObString::make_string("MARK_SWEEP_GC_DIAG")) {
    type = ObSharedGCTaskType::MARK_SWEEP_GC_DIAG;
  } else {
    type = ObSharedGCTaskType::INVALID_TYPE;
  }
  return type;
}

// ===========================================================================
// ObSharedGCTaskKey
// ===========================================================================

void ObSharedGCTaskKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  type_ = ObSharedGCTaskType::INVALID_TYPE;
  round_id_ = 0;
  task_id_ = 0;
}

bool ObSharedGCTaskKey::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
      && is_valid_shared_gc_task_type(type_)
      && round_id_ >= 0
      && task_id_ >= 0;
}

// ===========================================================================
// ObSharedGCTask
// ===========================================================================

void ObSharedGCTask::reset()
{
  key_.reset();
  value_.reset();
  extra_info_.reset();
  server_id_ = INVALID_SERVER_ID;
  trace_id_.reset();
}

bool ObSharedGCTask::is_valid() const
{
  const int64_t MAX_INT64_VALUE_LENGTH = 19;
  bool bool_ret = key_.is_valid()
      && (!value_.empty() || is_mark_sweep_gc_diag_type(key_.type_))
      && value_.length() <= MAX_VALUE_LENGTH
      && extra_info_.length() <= MAX_EXTRA_INFO_LENGTH
      && trace_id_.length() <= MAX_TRACE_ID_LENGTH;
  int parse_err = 0;
  char *parse_end = nullptr;
  int64_t macro_block_count = 0;
  if (!bool_ret) {
  } else if (is_mark_sweep_gc_diag_type(key_.type_)) {
    // make_string("") is still empty; a DIAG marker is identified by its round key.
    bool_ret = (key_.round_id_ > 0) && (0 == key_.task_id_);
  } else if (is_mark_sweep_gc_stat_type(key_.type_)) {
    // 统计行只允许保存规范的正十进制 int64 文本。
    bool_ret = nullptr != value_.ptr()
        && value_.length() <= MAX_INT64_VALUE_LENGTH
        && value_.ptr()[0] >= '1'
        && value_.ptr()[0] <= '9';
    if (bool_ret) {
      macro_block_count = ObCharset::strntoll(
          value_.ptr(), value_.length(), 10, &parse_end, &parse_err);
      bool_ret = 0 == parse_err
          && parse_end == value_.ptr() + value_.length()
          && macro_block_count > 0;
    }
  }
  return bool_ret;
}

// ===========================================================================
// ObSharedGCMacroIdList
// ===========================================================================

ObSharedGCMacroIdList::ObSharedGCMacroIdList()
  : macro_id_array_()
{
  macro_id_array_.set_attr(ObMemAttr(MTL_ID(), "SharedGCIds"));
}

void ObSharedGCMacroIdList::reset()
{
  macro_id_array_.reset();
}

bool ObSharedGCMacroIdList::is_valid() const
{
  return !macro_id_array_.empty();
}

int ObSharedGCMacroIdList::add(const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", K(ret), K(macro_id));
  } else if (OB_FAIL(macro_id_array_.push_back(macro_id))) {
    LOG_WARN("failed to add macro id", K(ret));
  }
  return ret;
}

int ObSharedGCMacroIdList::get_serialize_size_after_add(
    const blocksstable::MacroBlockId &macro_id,
    const int64_t cur_serialize_size,
    int64_t &next_serialize_size) const
{
  int ret = OB_SUCCESS;
  next_serialize_size = 0;
  if (OB_UNLIKELY(!macro_id.is_valid() || (cur_serialize_size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(macro_id), K(cur_serialize_size));
  } else {
    const int64_t cur_count = count();
    next_serialize_size = cur_serialize_size
        - serialization::encoded_length_vi64(cur_count)
        + serialization::encoded_length_vi64(cur_count + 1)
        + macro_id.get_serialize_size();
  }
  return ret;
}

int ObSharedGCMacroIdList::to_value(ObIAllocator &allocator, ObString &value) const
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_size));
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id list", K(ret), K(*this));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(serialize(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize macro id list", K(ret));
  } else {
    value.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObSharedGCMacroIdList::from_value(const ObString &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  reset();
  if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty value", K(ret));
  } else if (OB_FAIL(deserialize(value.ptr(), value.length(), pos))) {
    LOG_WARN("failed to deserialize macro id list", K(ret));
  }
  return ret;
}

// ===========================================================================
// ObSharedGCFailInfo
// ===========================================================================

ObSharedGCFailInfo::ObSharedGCFailInfo()
  : meta_type_(blocksstable::ObStorageObjectType::MAX),
    op_id_(0),
    ls_id_(),
    tablet_id_(),
    transfer_scn_(),
    ctrl_object_info_()
{
}

void ObSharedGCFailInfo::reset()
{
  meta_type_ = blocksstable::ObStorageObjectType::MAX;
  op_id_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  transfer_scn_.reset();
  ctrl_object_info_.reset();
}

bool ObSharedGCFailInfo::is_valid() const
{
  return is_valid_ss_meta_ctrl_type(meta_type_);
}

int ObSharedGCFailInfo::to_value(ObIAllocator &allocator, ObString &value) const
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_size));
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fail gc info", K(ret), K(*this));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(serialize(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize fail gc info", K(ret));
  } else {
    value.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObSharedGCFailInfo::from_value(const ObString &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  reset();
  if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty value", K(ret));
  } else if (OB_FAIL(deserialize(value.ptr(), value.length(), pos))) {
    LOG_WARN("failed to deserialize fail gc info", K(ret));
  }
  return ret;
}

// ===========================================================================
// ObSharedGCLSMetaInfo
// ===========================================================================

ObSharedGCLSMetaInfo::ObSharedGCLSMetaInfo()
  : ls_id_(),
    meta_scn_()
{
}

void ObSharedGCLSMetaInfo::reset()
{
  ls_id_.reset();
  meta_scn_.reset();
}

bool ObSharedGCLSMetaInfo::is_valid() const
{
  return ls_id_.is_valid() && meta_scn_.is_valid();
}

int ObSharedGCLSMetaInfo::to_value(ObIAllocator &allocator, ObString &value) const
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_size));
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta info", K(ret), K(*this));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(serialize(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize ls meta info", K(ret));
  } else {
    value.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObSharedGCLSMetaInfo::from_value(const ObString &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  reset();
  if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty value", K(ret));
  } else if (OB_FAIL(deserialize(value.ptr(), value.length(), pos))) {
    LOG_WARN("failed to deserialize ls meta info", K(ret));
  }
  return ret;
}

// ===========================================================================
// ObDeleteTenantTaskValue 实现
// ===========================================================================

ObDeleteTenantTaskValue::ObDeleteTenantTaskValue()
  : tenant_id_(OB_INVALID_TENANT_ID),
    delete_ts_(0)
{
}

void ObDeleteTenantTaskValue::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  delete_ts_ = 0;
}

bool ObDeleteTenantTaskValue::is_valid() const
{
  return (is_user_tenant(tenant_id_) || is_meta_tenant(tenant_id_))
      && delete_ts_ > 0;
}

int ObDeleteTenantTaskValue::to_value(ObIAllocator &allocator, ObString &value) const
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_size));
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delete tenant task value", K(ret), K(*this));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(serialize(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize delete tenant task value", K(ret));
  } else {
    value.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObDeleteTenantTaskValue::from_value(const ObString &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  reset();
  if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty value", K(ret));
  } else if (OB_FAIL(deserialize(value.ptr(), value.length(), pos))) {
    LOG_WARN("failed to deserialize delete tenant task value", K(ret));
  }
  return ret;
}

// ===========================================================================
// ObDeleteServerTaskValue 实现
// ===========================================================================

ObDeleteServerTaskValue::ObDeleteServerTaskValue()
  : server_id_(OB_INVALID_ID),
    server_addr_(),
    delete_ts_(0)
{
}

void ObDeleteServerTaskValue::reset()
{
  server_id_ = OB_INVALID_ID;
  server_addr_.reset();
  delete_ts_ = 0;
}

bool ObDeleteServerTaskValue::is_valid() const
{
  return is_valid_server_id(server_id_)
      && server_addr_.is_valid()
      && delete_ts_ > 0;
}

int ObDeleteServerTaskValue::to_value(ObIAllocator &allocator, ObString &value) const
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_size));
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delete server task value", K(ret), K(*this));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(serialize(buf, buf_size, pos))) {
    LOG_WARN("failed to serialize delete server task value", K(ret));
  } else {
    value.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObDeleteServerTaskValue::from_value(const ObString &value)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  reset();
  if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty value", K(ret));
  } else if (OB_FAIL(deserialize(value.ptr(), value.length(), pos))) {
    LOG_WARN("failed to deserialize delete server task value", K(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
