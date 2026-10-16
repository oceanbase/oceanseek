/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_meta_reserved_define.h"

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/utility.h"
#include "share/ob_define.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

/******************ObSharedMetaReservedType*********************/
ObSharedMetaReservedType::ObSharedMetaReservedType()
  : type_(ObSharedMetaReservedTypeEnum::INVALID)
{
}

ObSharedMetaReservedType::ObSharedMetaReservedType(const ObSharedMetaReservedTypeEnum type)
  : type_(type)
{
}

void ObSharedMetaReservedType::reset()
{
  type_ = ObSharedMetaReservedTypeEnum::INVALID;
}

bool ObSharedMetaReservedType::is_valid() const
{
  return type_ > ObSharedMetaReservedTypeEnum::INVALID
      && type_ < ObSharedMetaReservedTypeEnum::MAX;
}

int64_t ObSharedMetaReservedType::to_string(
    char *buf,
    const int64_t buf_len) const
{
  int64_t pos = 0;
  (void)databuff_printf(buf, buf_len, pos, "%s", shared_meta_reserved_type_to_string(type_));
  return pos;
}

int ObSharedMetaReservedType::parse_from_str(
    const ObString &str,
    ObSharedMetaReservedType &type)
{
  int ret = OB_SUCCESS;
  type.reset();
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty type string", KR(ret));
  } else {
    bool found = false;
#define CASE_PARSE_TYPE_(name, value, key_mode)                    \
    if (!found && 0 == str.case_compare(#name)) {                  \
      type.type_ = ObSharedMetaReservedTypeEnum::name;             \
      found = true;                                                \
    }
    OB_SHARED_META_RESERVED_TYPE_DEF(CASE_PARSE_TYPE_)
#undef CASE_PARSE_TYPE_
    if (!found) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unknown shared meta reserved type string", KR(ret), K(str));
    } else if (!type.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("type string maps to a sentinel value", KR(ret), K(str), K(type));
      type.reset();
    }
  }
  return ret;
}

int ObSharedMetaReservedType::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = murmurhash(&type_, sizeof(type_), 0);
  return ret;
}

uint64_t ObSharedMetaReservedType::hash() const
{
  uint64_t hash_val = 0;
  (void)hash(hash_val);
  return hash_val;
}

/******************ObSharedMetaReservedInfo*********************/
ObSharedMetaReservedInfo::ObSharedMetaReservedInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    type_(),
    meta_scn_(),
    owner_()
{
}

ObSharedMetaReservedInfo::ObSharedMetaReservedInfo(
    const uint64_t tenant_id,
    const ObSharedMetaReservedType &type,
    const SCN &meta_scn)
  : tenant_id_(tenant_id),
    type_(type),
    meta_scn_(meta_scn),
    owner_()
{
}

void ObSharedMetaReservedInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  type_.reset();
  meta_scn_.reset();
  owner_.reset();
}

bool ObSharedMetaReservedInfo::is_valid() const
{
  bool valid = OB_INVALID_TENANT_ID != tenant_id_
      && type_.is_valid()
      && meta_scn_.is_valid();
  if (valid && type_.is_owner_keyed() && !has_owner()) {
    valid = false;
  } else if (valid && type_.is_scn_keyed() && has_owner()) {
    valid = false;
  }
  return valid;
}

int ObSharedMetaReservedInfo::set_owner(const ObString &owner)
{
  int ret = OB_SUCCESS;
  const bool contains_null = (nullptr != owner.find('\0'));
  if (OB_UNLIKELY(owner.length() > SHARED_META_RESERVED_OWNER_MAX_LENGTH || contains_null)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared meta reserved owner", KR(ret), K(owner.length()), K(contains_null));
  } else if (owner.empty()) {
    owner_.reset();
  } else if (OB_FAIL(owner_.assign(owner))) {
    LOG_WARN("failed to assign shared meta reserved owner", KR(ret), K(owner.length()));
  }
  return ret;
}

int ObSharedMetaReservedInfo::assign(const ObSharedMetaReservedInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (this != &other) {
    tenant_id_ = other.tenant_id_;
    type_ = other.type_;
    meta_scn_ = other.meta_scn_;
    if (OB_FAIL(owner_.assign(other.owner_))) {
      LOG_WARN("failed to assign owner", KR(ret), K(other));
    }
  }
  return ret;
}

bool ObSharedMetaReservedInfo::operator==(const ObSharedMetaReservedInfo &other) const
{
  return tenant_id_ == other.tenant_id_
      && type_ == other.type_
      && meta_scn_ == other.meta_scn_
      && owner_ == other.owner_;
}

bool ObSharedMetaReservedInfo::operator!=(const ObSharedMetaReservedInfo &other) const
{
  return !(*this == other);
}

int ObSharedMetaReservedInfo::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  uint64_t type_hash = 0;
  if (OB_FAIL(type_.hash(type_hash))) {
    LOG_WARN("failed to hash type", KR(ret), K(type_));
  } else {
    hash_val = murmurhash(&type_hash, sizeof(type_hash), hash_val);
    const int64_t scn_val = meta_scn_.get_val_for_tx();
    hash_val = murmurhash(&scn_val, sizeof(scn_val), hash_val);
    const ObString owner = owner_.str();
    hash_val = murmurhash(owner.ptr(), owner.length(), hash_val);
  }
  return ret;
}

uint64_t ObSharedMetaReservedInfo::hash() const
{
  uint64_t hash_val = 0;
  (void)hash(hash_val);
  return hash_val;
}

/******************ObSharedMetaReservedUtil*********************/
int ObSharedMetaReservedUtil::get_real_tenant_id(
    const uint64_t tenant_id,
    uint64_t &real_tenant_id)
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get real tenant id get invalid tenant id", KR(ret), K(tenant_id));
  } else if (is_user_tenant(tenant_id)) {
    real_tenant_id = gen_meta_tenant_id(tenant_id);
  } else if (is_meta_tenant(tenant_id)) {
    real_tenant_id = OB_SYS_TENANT_ID;
  } else if (is_sys_tenant(tenant_id)) {
    real_tenant_id = tenant_id;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown tenant type", KR(ret), K(tenant_id));
  }
  return ret;
}
