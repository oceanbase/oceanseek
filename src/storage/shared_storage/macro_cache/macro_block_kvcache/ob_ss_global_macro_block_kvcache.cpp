/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_global_macro_block_kvcache.h"
#include "lib/checksum/ob_crc64.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

ObSSMacroBlockCacheKey::ObSSMacroBlockCacheKey()
  : tenant_id_(OB_INVALID_TENANT_ID), macro_id_()
{
}

ObSSMacroBlockCacheKey::ObSSMacroBlockCacheKey(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id)
  : tenant_id_(tenant_id), macro_id_(macro_id)
{
}

bool ObSSMacroBlockCacheKey::operator==(const ObIKVCacheKey &other) const
{
  const ObSSMacroBlockCacheKey &other_key =
      reinterpret_cast<const ObSSMacroBlockCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_ && macro_id_ == other_key.macro_id_;
}

uint64_t ObSSMacroBlockCacheKey::hash() const
{
  uint64_t hash_value = murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
  return murmurhash(&macro_id_, sizeof(macro_id_), hash_value);
}

int ObSSMacroBlockCacheKey::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), "size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid macro block cache key", KR(ret), KPC(this));
  } else {
    key = new (buf) ObSSMacroBlockCacheKey(tenant_id_, macro_id_);
  }
  return ret;
}

bool ObSSMacroBlockCacheKey::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
      && !is_virtual_tenant_id(tenant_id_)
      && macro_id_.is_valid();
}

ObSSMacroBlockCacheValue::ObSSMacroBlockCacheValue()
  : effective_tablet_id_(OB_INVALID_ID), data_size_(0), crc_(0), data_(nullptr)
{
}

ObSSMacroBlockCacheValue::ObSSMacroBlockCacheValue(
    const uint64_t effective_tablet_id,
    const char *data,
    const int64_t data_size,
    const uint32_t crc)
  : effective_tablet_id_(effective_tablet_id),
    data_size_(static_cast<int32_t>(data_size)),
    crc_(crc),
    data_(data)
{
}

int64_t ObSSMacroBlockCacheValue::size() const
{
  return sizeof(*this) + data_size_;
}

int ObSSMacroBlockCacheValue::deep_copy(
    char *buf,
    const int64_t buf_len,
    ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), "size", size());
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid macro block cache value", KR(ret), KPC(this));
  } else {
    ObSSMacroBlockCacheValue *copied_value =
        new (buf) ObSSMacroBlockCacheValue(effective_tablet_id_, buf + sizeof(*this), data_size_, crc_);
    MEMCPY(buf + sizeof(*this), data_, data_size_);
    value = copied_value;
  }
  return ret;
}

bool ObSSMacroBlockCacheValue::is_valid() const
{
  return data_size_ > 0 && OB_NOT_NULL(data_);
}

int ObSSMacroBlockCacheValue::check_crc() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block cache value", KR(ret), KPC(this));
  } else {
    const uint32_t actual_crc = static_cast<uint32_t>(ob_crc64(data_, data_size_));
    if (OB_UNLIKELY(actual_crc != crc_)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("macro block cache value crc mismatch",
          KR(ret), K(actual_crc), KPC(this));
    }
  }
  return ret;
}

ObSSGlobalMacroBlockKVCache::ObSSGlobalMacroBlockKVCache()
  : BaseKVCache(), is_inited_(false)
{
}

ObSSGlobalMacroBlockKVCache::~ObSSGlobalMacroBlockKVCache()
{
  destroy();
}

ObSSGlobalMacroBlockKVCache &ObSSGlobalMacroBlockKVCache::get_instance()
{
  static ObSSGlobalMacroBlockKVCache instance;
  return instance;
}

int ObSSGlobalMacroBlockKVCache::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss global macro block KVCACHE is already inited", KR(ret));
  } else if (OB_FAIL(BaseKVCache::init(
      get_cache_name(),
      DEFAULT_PRIORITY))) {
    LOG_WARN("fail to init ss global macro block KVCACHE", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSSGlobalMacroBlockKVCache::destroy()
{
  BaseKVCache::destroy();
  is_inited_ = false;
}

int ObSSGlobalMacroBlockKVCache::erase_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss global macro block KVCACHE is not inited",
        KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().erase_cache(
      tenant_id, get_cache_name()))) {
    LOG_WARN("fail to erase tenant data from ss global macro block KVCACHE",
        KR(ret), K(tenant_id));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
