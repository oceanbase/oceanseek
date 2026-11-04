/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_STORAGE_FTS_OB_DELETION_BITMAP_CACHE_H_
#define OB_STORAGE_FTS_OB_DELETION_BITMAP_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "common/ob_tablet_id.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace storage
{

class ObDeletionBitmapCacheKey : public common::ObIKVCacheKey
{
public:
  ObDeletionBitmapCacheKey();
  ObDeletionBitmapCacheKey(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const int64_t base_snapshot = 0);
  virtual ~ObDeletionBitmapCacheKey() = default;

  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override;
  virtual int hash(uint64_t &hash_value) const override;
  virtual uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual int64_t size() const override { return sizeof(*this); }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(snapshot_version), K_(base_snapshot));

private:
  uint64_t tenant_id_;
  common::ObTabletID tablet_id_;       // Delete Log tablet id
  int64_t snapshot_version_;           // query snapshot (refresh_scn)
  int64_t base_snapshot_;              // exclusive lower bound; queries use 0

  DISALLOW_COPY_AND_ASSIGN(ObDeletionBitmapCacheKey);
};

class ObDeletionBitmapCacheValue : public common::ObIKVCacheValue
{
public:
  ObDeletionBitmapCacheValue();
  virtual ~ObDeletionBitmapCacheValue();

  // Initialize from serialized roaring bitmap binary
  int init(const common::ObString &bitmap_bin, const int64_t deletion_count);
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  bool is_valid() const { return deletion_count_ >= 0; }

  int64_t get_deletion_count() const { return deletion_count_; }
  bool is_empty() const { return 0 == deletion_count_; }

  // Get serialized bitmap binary (for constructing ObRoaringBitmap/Iter at query time)
  const common::ObString &get_bitmap_bin() const { return bitmap_bin_; }

  TO_STRING_KV(K_(deletion_count), K_(bitmap_bin));

private:
  int64_t deletion_count_;        // number of deleted doc_ids (for fast empty check)
  common::ObString bitmap_bin_;   // serialized roaring64 bitmap binary

  DISALLOW_COPY_AND_ASSIGN(ObDeletionBitmapCacheValue);
};

struct ObDeletionBitmapCacheHandle
{
  const ObDeletionBitmapCacheValue *value_;
  common::ObKVCacheHandle handle_;

  ObDeletionBitmapCacheHandle() : value_(nullptr), handle_() {}
  ~ObDeletionBitmapCacheHandle() {}
  bool is_valid() const { return nullptr != value_ && handle_.is_valid(); }
  void reset() { value_ = nullptr; handle_.reset(); }
  TO_STRING_KV(KP_(value), K_(handle));
};

class ObDeletionBitmapCache : public common::ObKVCache<ObDeletionBitmapCacheKey, ObDeletionBitmapCacheValue>
{
public:
  static ObDeletionBitmapCache &get_instance()
  {
    static ObDeletionBitmapCache instance;
    return instance;
  }

  int get_bitmap(const ObDeletionBitmapCacheKey &key, ObDeletionBitmapCacheHandle &handle);
  int put_bitmap(const ObDeletionBitmapCacheKey &key, const ObDeletionBitmapCacheValue &value);

private:
  ObDeletionBitmapCache() {}
  virtual ~ObDeletionBitmapCache() {}
  DISALLOW_COPY_AND_ASSIGN(ObDeletionBitmapCache);
};

#define OB_DELETION_BITMAP_CACHE oceanbase::storage::ObDeletionBitmapCache::get_instance()

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_FTS_OB_DELETION_BITMAP_CACHE_H_
