/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_persistent_reader_cache.h"
#include "lib/time/ob_time_utility.h"
#include "storage/lob/store/ob_lob_persistent_iterator.h"

namespace oceanbase
{
namespace storage
{

void ObPersistLobReaderCacheKey::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  snapshot_ = 0;
  fb_snapshot_.reset();
  target_ = LobAuxTarget::MAX;
}

ObPersistLobReaderCache::~ObPersistLobReaderCache()
{
  reset_and_clear();
}

void ObPersistLobReaderCache::reset_and_clear()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cached_reader_)) {
    if (OB_FAIL(cached_reader_->reset())) {
      LOG_WARN("failed to reset cached reader", KR(ret), K(cached_key_));
    }
    
    if (cached_key_.target_ == LobAuxTarget::META) {
      ObLobAuxTableIterator<ObLobMetaInfo> *meta_iter = reinterpret_cast<ObLobAuxTableIterator<ObLobMetaInfo> *>(cached_reader_);
      OB_DELETEx(ObLobAuxTableIterator<ObLobMetaInfo>, &allocator_, meta_iter);
    } else if (cached_key_.target_ == LobAuxTarget::PIECE) {
      ObLobAuxTableIterator<ObLobPieceInfo> *piece_iter = reinterpret_cast<ObLobAuxTableIterator<ObLobPieceInfo> *>(cached_reader_);
      OB_DELETEx(ObLobAuxTableIterator<ObLobPieceInfo>, &allocator_, piece_iter);
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid target, may be memory leak", K(cached_key_));
    }
    cached_reader_ = nullptr;
  }
  cached_key_.reset();
  allocator_.reset();
  last_cached_time_us_ = 0;
}

int ObPersistLobReaderCache::get(ObPersistLobReaderCacheKey key, ObLobAuxTableBaseIterator *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  if (cached_key_ == key) {
    if (OB_NOT_NULL(cached_reader_)) {
      reader = cached_reader_;
      last_cached_time_us_ = ObTimeUtility::current_time();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cached reader is null", KR(ret), K(key));
    }
  } else {
    reset_and_clear();
    ObLobAuxTableBaseIterator *iter = nullptr;
    if (OB_ISNULL(iter = alloc_reader(key.target_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc reader fail", KR(ret), K(key));
    } else if (OB_FAIL(put(key, iter))) {
      LOG_WARN("put reader fail", KR(ret), K(key));
    } else {
      reader = iter;
    }
  }
  return ret;
}

int ObPersistLobReaderCache::put(ObPersistLobReaderCacheKey key, ObLobAuxTableBaseIterator *reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reader is null", K(ret), K(key));
  } else {
    cached_key_ = key;
    cached_reader_ = reader;
    last_cached_time_us_ = ObTimeUtility::current_time();
  }
  return ret;
}

void ObPersistLobReaderCache::check_and_release_if_timeout(const int64_t timeout_us)
{
  if (OB_ISNULL(cached_reader_)) {
  } else if (OB_UNLIKELY((++timeout_check_call_count_ == TIMEOUT_CHECK_THROTTLE_INTERVAL))) {
    const int64_t now_us = ObTimeUtility::current_time();
    timeout_check_call_count_ = 0;
    if (now_us - last_cached_time_us_ > timeout_us) {
      reset_and_clear();
    }
  }
}

ObLobAuxTableBaseIterator *ObPersistLobReaderCache::alloc_reader(LobAuxTarget target)
{
  ObLobAuxTableBaseIterator *reader = nullptr;
  if (LobAuxTarget::META == target) {
    reader = OB_NEWx(ObLobAuxTableIterator<ObLobMetaInfo>, &allocator_, access_ctx_);
  } else if (LobAuxTarget::PIECE == target) {
    reader = OB_NEWx(ObLobAuxTableIterator<ObLobPieceInfo>, &allocator_, access_ctx_);
  }
  return reader;
}

} // storage
} // oceanbase