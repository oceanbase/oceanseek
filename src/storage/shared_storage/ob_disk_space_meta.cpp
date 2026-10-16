/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_disk_space_meta.h"
#include "storage/shared_storage/ob_disk_space_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

/**
 * --------------------------------ObTenantDiskSpaceMetaBody------------------------------------
 */
ObTenantDiskSpaceMetaBody::ObTenantDiskSpaceMetaBody()
  : tenant_id_(OB_INVALID_TENANT_ID), macro_cache_stats_(), dir_sizes_(), dir_nums_()
{
}

int ObTenantDiskSpaceMetaBody::set_macro_cache_stats()
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space is null", KR(ret));
  } else {
    for (uint8_t i = 0; OB_SUCC(ret) && (i < SS_MACRO_CACHE_MAX_TYPE_VAL); ++i) {
      ObSSMacroCacheStat macro_cache_stat;
      if (OB_FAIL(disk_space_mgr->get_macro_cache_stat(static_cast<ObSSMacroCacheType>(i), macro_cache_stat))) {
        LOG_WARN("fail to get macro cache stat", KR(ret), K(i));
      } else if (OB_FAIL(macro_cache_stats_.push_back(macro_cache_stat))) {
        LOG_WARN("fail to push back macro cache stat", KR(ret), K(macro_cache_stat));
      }
    }
  }
  return ret;
}

int ObTenantDiskSpaceMetaBody::set_dir_infos()
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space is null", KR(ret));
  } else {
    for (uint8_t i = 0; OB_SUCC(ret) && (i < SS_MACRO_CACHE_MAX_TYPE_VAL); ++i) {
      int64_t dir_size = 0;
      int64_t dir_num = 0;
      if (OB_FAIL(disk_space_mgr->get_dir_info(static_cast<ObSSMacroCacheType>(i), dir_size, dir_num))) {
        LOG_WARN("fail to get dir info", KR(ret), K(i));
      } else if (OB_FAIL(dir_sizes_.push_back(dir_size))) {
        LOG_WARN("fail to push back dir size", KR(ret), K(dir_size));
      } else if (OB_FAIL(dir_nums_.push_back(dir_num))) {
        LOG_WARN("fail to push back dir num", KR(ret), K(dir_num));
      }
    }
  }
  return ret;
}

bool ObTenantDiskSpaceMetaBody::is_valid() const
{
  bool b_ret = true;
  if (OB_INVALID_TENANT_ID == tenant_id_) {
    b_ret = false;
  }
  const int64_t macro_cache_stat_cnt = macro_cache_stats_.count();
  for (int64_t i = 0; (i < macro_cache_stat_cnt) && b_ret; ++i) {
    if (!macro_cache_stats_.at(i).is_valid()) {
      b_ret = false;
    }
  }
  return b_ret;
}

void ObTenantDiskSpaceMetaBody::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  macro_cache_stats_.reset();
  dir_sizes_.reset();
  dir_nums_.reset();
}

OB_SERIALIZE_MEMBER(ObTenantDiskSpaceMetaBody, tenant_id_, macro_cache_stats_, dir_sizes_, dir_nums_);


/**
 * --------------------------------ObTenantDiskSpaceMeta------------------------------------
 */

DEFINE_SERIALIZE(ObTenantDiskSpaceMeta)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || (buf_len - new_pos) < ser_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("failed to encode meta header", K(ret), K(buf_len), K(new_pos), K(*this));
  } else if (OB_FAIL(body_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("failed to encode meta body", K(ret), K(buf_len), K(new_pos), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTenantDiskSpaceMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  uint32_t calc_crc = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize meta head", KR(ret));
  } else if (OB_UNLIKELY(header_.body_crc_ !=
      (calc_crc = static_cast<uint32_t>(ob_crc64(buf + new_pos, header_.body_size_))))) {
    ret = OB_PHYSIC_CHECKSUM_ERROR;
    LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "failed to check crc", KR(ret), KP(buf),
      K(data_len), K(pos), K_(header), K(calc_crc));
  } else if (OB_FAIL(body_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize meta body", KR(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant disk space meta", KR(ret), K_(header), K_(body));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTenantDiskSpaceMeta)
{
  int64_t len = 0;
  len += header_.get_serialize_size();
  len += body_.get_serialize_size();
  return len;
}

ObTenantDiskSpaceMeta::ObTenantDiskSpaceMeta()
{
  reset();
}

ObTenantDiskSpaceMeta::ObTenantDiskSpaceMeta(const uint64_t tenant_id)
{
  reset();
  body_.tenant_id_ = tenant_id;
}

void ObTenantDiskSpaceMeta::reset()
{
  header_.reset();
  body_.reset();
}

bool ObTenantDiskSpaceMeta::is_valid() const
{
  return (header_.is_valid() && body_.is_valid());
}

} // namespace storage
} // namespace oceanbase
