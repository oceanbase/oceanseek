/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_deletion_bitmap_cache.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace storage
{

// ======================== ObDeletionBitmapCacheKey ========================

ObDeletionBitmapCacheKey::ObDeletionBitmapCacheKey()
    : tenant_id_(OB_INVALID_TENANT_ID),
      tablet_id_(),
      snapshot_version_(0),
      base_snapshot_(0)
{
}

ObDeletionBitmapCacheKey::ObDeletionBitmapCacheKey(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const int64_t snapshot_version,
    const int64_t base_snapshot)
    : tenant_id_(tenant_id),
      tablet_id_(tablet_id),
      snapshot_version_(snapshot_version),
      base_snapshot_(base_snapshot)
{
}

int ObDeletionBitmapCacheKey::equal(const ObIKVCacheKey &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  const ObDeletionBitmapCacheKey &other_key = reinterpret_cast<const ObDeletionBitmapCacheKey&>(other);
  equal = (tenant_id_ == other_key.tenant_id_
           && tablet_id_ == other_key.tablet_id_
           && snapshot_version_ == other_key.snapshot_version_
           && base_snapshot_ == other_key.base_snapshot_);
  return ret;
}

int ObDeletionBitmapCacheKey::hash(uint64_t &hash_value) const
{
  hash_value = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
  hash_value = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_value);
  hash_value = common::murmurhash(&snapshot_version_, sizeof(snapshot_version_), hash_value);
  hash_value = common::murmurhash(&base_snapshot_, sizeof(base_snapshot_), hash_value);
  
  return OB_SUCCESS;
}

int ObDeletionBitmapCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), "need_size", size());
  } else {
    ObDeletionBitmapCacheKey *new_key = new (buf) ObDeletionBitmapCacheKey(
        tenant_id_, tablet_id_, snapshot_version_, base_snapshot_);
    key = new_key;
  }
  return ret;
}

bool ObDeletionBitmapCacheKey::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
      && tablet_id_.is_valid()
      && snapshot_version_ > 0
      && base_snapshot_ >= 0 && base_snapshot_ <= snapshot_version_;
}

// ======================== ObDeletionBitmapCacheValue ========================

ObDeletionBitmapCacheValue::ObDeletionBitmapCacheValue()
    : deletion_count_(0),
      bitmap_bin_()
{
}

ObDeletionBitmapCacheValue::~ObDeletionBitmapCacheValue()
{
  // bitmap_bin_ lifetime managed by KVCache allocator, no explicit free needed
  bitmap_bin_.reset();
  deletion_count_ = 0;
}

int ObDeletionBitmapCacheValue::init(const common::ObString &bitmap_bin, const int64_t deletion_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(deletion_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(deletion_count));
  } else {
    bitmap_bin_ = bitmap_bin;
    deletion_count_ = deletion_count;
  }
  return ret;
}

int64_t ObDeletionBitmapCacheValue::size() const
{
  return sizeof(*this) + bitmap_bin_.length();
}

int ObDeletionBitmapCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  const int64_t need_size = size();
  if (OB_UNLIKELY(nullptr == buf || buf_len < need_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(need_size));
  } else {
    ObDeletionBitmapCacheValue *new_value = new (buf) ObDeletionBitmapCacheValue();
    char *bin_buf = buf + sizeof(ObDeletionBitmapCacheValue);
    if (bitmap_bin_.length() > 0 && nullptr != bitmap_bin_.ptr()) {
      MEMCPY(bin_buf, bitmap_bin_.ptr(), bitmap_bin_.length());
      new_value->bitmap_bin_.assign_ptr(bin_buf, bitmap_bin_.length());
    } else {
      new_value->bitmap_bin_.reset();
    }
    new_value->deletion_count_ = deletion_count_;
    value = new_value;
  }
  return ret;
}

// ======================== ObDeletionBitmapCache ========================

int ObDeletionBitmapCache::get_bitmap(
    const ObDeletionBitmapCacheKey &key,
    ObDeletionBitmapCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObDeletionBitmapCacheValue *value = nullptr;
  if (OB_FAIL(get(key, value, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get from kv cache failed", K(ret), K(key));
    }
  } else {
    handle.value_ = value;
  }
  return ret;
}

int ObDeletionBitmapCache::put_bitmap(
    const ObDeletionBitmapCacheKey &key,
    const ObDeletionBitmapCacheValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(put(key, value))) {
    LOG_WARN("put to kv cache failed", K(ret), K(key));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
