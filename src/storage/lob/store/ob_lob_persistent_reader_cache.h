/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEABASE_STORAGE_OB_LOB_PERSISTENT_READER_CACHE_
#define OCEABASE_STORAGE_OB_LOB_PERSISTENT_READER_CACHE_

#include "storage/lob/common/ob_lob_context.h"

namespace oceanbase
{
namespace common
{
class ObObjectStorageInfo;
}
namespace storage
{


class ObLobAuxTableBaseIterator;
class ObLobAccessCtx;

// Resolves access information for a URL through LOCATION.
// LOCATION lookup uses a schema guard and requires the current session's
// privileges, enabled roles and tenant information.
// URLs may come from row data, so access information is resolved on demand
// rather than prepared and passed through every SQL read path.
// Storage defines this base interface, while SQL implements it using its
// execution context and passes the provider to storage. This lets storage
// request access information without depending on SQL headers or concrete
// SQL implementations.
class ObExternalAccessProvider
{
public:
  virtual ~ObExternalAccessProvider() = default;
  virtual int get_access_info(const common::ObString &url,
                              common::ObObjectStorageInfo &access_info) = 0;
};

// @brief Lob reader cache key
struct ObPersistLobReaderCacheKey
{
  ObPersistLobReaderCacheKey():
    ls_id_(),
    tablet_id_(),
    snapshot_(0),
    fb_snapshot_(),
    target_(LobAuxTarget::MAX)
  {}

  bool operator==(const ObPersistLobReaderCacheKey &other) const
  {
    return ls_id_ == other.ls_id_
           && tablet_id_ == other.tablet_id_
           && snapshot_ == other.snapshot_
           && fb_snapshot_ == other.fb_snapshot_
           && target_ == other.target_;
  }

  void reset();

  TO_STRING_KV(K(ls_id_), K(tablet_id_), K(snapshot_), K(fb_snapshot_),
               "target", get_lob_aux_target_str(target_));

  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t snapshot_;
  // One SQL can mix flashback and current LOB reads, so the flashback snapshot is part of the key.
  share::SCN fb_snapshot_;
  LobAuxTarget target_;
};

class ObPersistLobReaderCache
{
public:
  static const uint32_t TIMEOUT_CHECK_THROTTLE_INTERVAL = 1024;

  ObPersistLobReaderCache(const ObLobAccessCtx *access_ctx)
    : access_ctx_(access_ctx),
      allocator_(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE/*8KB*/, MTL_ID()),
      cached_key_(),
      cached_reader_(nullptr),
      last_cached_time_us_(0),
      timeout_check_call_count_(0)
  {}
  ~ObPersistLobReaderCache();

  // If cached reader exists and param.tablet_id matches, return it; else return nullptr.
  int get(const ObPersistLobReaderCacheKey key, ObLobAuxTableBaseIterator *&reader);
  // Replace cached reader: reset and free old one if exists, then store (key, reader).
  int put(ObPersistLobReaderCacheKey key, ObLobAuxTableBaseIterator *reader);

  ObIAllocator &get_allocator() { return allocator_; }

  void reset() { reset_and_clear(); }
  // If cached iter has been held longer than timeout_us (default 10s), reset it to release memtable ref.
  void check_and_release_if_timeout(const int64_t timeout_us = 10 * 1000 * 1000);

private:
  // new the concrete ObLobAuxTableIterator<T> chosen by target, returned as base pointer
  ObLobAuxTableBaseIterator *alloc_reader(LobAuxTarget target);
  void reset_and_clear();

private:
  const ObLobAccessCtx *access_ctx_;
  ObArenaAllocator allocator_;
  ObPersistLobReaderCacheKey cached_key_;
  ObLobAuxTableBaseIterator *cached_reader_;
  int64_t last_cached_time_us_;
  uint32_t timeout_check_call_count_;  // throttle: only do real check every TIMEOUT_CHECK_THROTTLE_INTERVAL calls
};


class ObLobAccessCtx
{
public:
  explicit ObLobAccessCtx(ObExternalAccessProvider &provider):
    meta_reader_cache_(this),
    piece_reader_cache_(this),
    external_lob_access_provider_(provider)
  {}
  ~ObLobAccessCtx() = default;

  void reset()
  {
    meta_reader_cache_.reset();
    piece_reader_cache_.reset();
  }

  void check_and_release_if_timeout(const int64_t timeout_us = 10 * 1000 * 1000)
  {
    meta_reader_cache_.check_and_release_if_timeout(timeout_us);
    piece_reader_cache_.check_and_release_if_timeout(timeout_us);
  }

  // Dispatch by target; OB_INVALID_ARGUMENT on LobAuxTarget::MAX so callers must
  // propagate the error rather than silently fall through to one cache.
  int get_aux_reader_cache(const LobAuxTarget &target, ObPersistLobReaderCache *&reader_cache)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
    } else if (LobAuxTarget::META == target) {
      reader_cache = &meta_reader_cache_;
    } else if (LobAuxTarget::PIECE == target) {
      reader_cache = &piece_reader_cache_;
    }
    return ret;
  }

  int get_external_access_info(const common::ObString &url,
                               common::ObObjectStorageInfo &access_info) const
  {
    return external_lob_access_provider_.get_access_info(url, access_info);
  }

public:
  ObPersistLobReaderCache meta_reader_cache_;
  ObPersistLobReaderCache piece_reader_cache_;
  // Borrowed from the owning exec context; storage needs no SQL header dependencies.
  ObExternalAccessProvider &external_lob_access_provider_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobAccessCtx);
};


} // storage
} // oceanbase

#endif
