/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_buf_mgr.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_util.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

ObSSMemMacroCacheBufMgr::ObSSMemMacroCacheBufMgr(ObSSMemMacroCacheStat &cache_stat)
  : is_inited_(false), bucket_cnt_(0), buckets_(nullptr), mem_blk_pool_(), cache_stat_(cache_stat)
{}

int ObSSMemMacroCacheBufMgr::init(const uint64_t tenant_id, const int32_t blk_size, const int64_t total_blk_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_cnt = SS_DEF_MEM_MACRO_CACHE_BUCKET_CNT;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited), K(tenant_id));
  } else if (OB_FAIL(mem_blk_pool_.init(tenant_id, blk_size, total_blk_cnt))) {
    LOG_WARN("fail to init mem_block pool", KR(ret), K(tenant_id), K(blk_size), K(total_blk_cnt));
  } else if (OB_FAIL(inner_create_buckets(tenant_id, bucket_cnt))) {
    LOG_WARN("fail to create buckets", KR(ret), K(tenant_id), K(bucket_cnt));
  } else {
    bucket_cnt_ = bucket_cnt;
    is_inited_ = true;
  }
  return ret;
}

void ObSSMemMacroCacheBufMgr::destroy()
{
  if (nullptr != buckets_) {
    inner_free_buckets();
  }
  bucket_cnt_ = 0;
  is_inited_ = false;
}

void ObSSMemMacroCacheBufMgr::clear()
{
  if (nullptr != buckets_) {
    inner_clear_buckets();
  }
}

void ObSSMemMacroCacheBufMgr::update_total_mem_block_cnt(const int64_t new_total_cnt)
{
  mem_blk_pool_.update_total_block_cnt(new_total_cnt);
}

int ObSSMemMacroCacheBufMgr::inner_create_buckets(const uint64_t tenant_id, const int64_t bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buckets_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("buckets were created", KR(ret), KP_(buckets));
  } else if (OB_FAIL(ObSSMemMacroCacheUtil::alloc_buckets(tenant_id, bucket_cnt, mem_blk_pool_, buckets_))) {
    LOG_WARN("fail to alloc buckets", KR(ret), K(tenant_id), K(bucket_cnt), KP_(buckets));
  }
  return ret;
}

void ObSSMemMacroCacheBufMgr::inner_free_buckets()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSMemMacroCacheUtil::free_buckets(buckets_, bucket_cnt_))) {
    LOG_WARN("fail to free buckets", KR(ret), KP_(buckets), K_(bucket_cnt));
  } else {
    buckets_ = nullptr;
  }
}

void ObSSMemMacroCacheBufMgr::inner_clear_buckets()
{
  if (nullptr != buckets_ && bucket_cnt_ > 0) {
    for (int64_t i = 0; i < bucket_cnt_; ++i) {
      buckets_[i]->clear();
    }
  }
}

int ObSSMemMacroCacheBufMgr::add_macro_block_data(
    const MacroBlockId &macro_id, 
    const char *buf, 
    const int64_t buf_size,
    ObSSMacroCacheMemBlkHandle &mem_blk_handle,
    int32_t &offset,
    uint32_t &crc)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!macro_id.is_valid() || buf_size <= 0) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(buf_size), KP(buf));
  } else {
    ObSSMacroCacheMemBucket *target_bucket = inner_get_bucket(macro_id, buf_size);
    if (OB_ISNULL(target_bucket)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target bucket should be valid", KR(ret), K(macro_id), K(buf_size), K_(bucket_cnt), KP_(buckets));
    } else if (OB_FAIL(target_bucket->try_add_macro_block_data(macro_id, buf, buf_size, mem_blk_handle, offset))) {
      LOG_WARN("fail to try add macro_block data", KR(ret), K(macro_id), K(buf_size), KP(buf));
    } else {
      crc = static_cast<uint32_t>(ob_crc64(buf, buf_size));
      LOG_TRACE("ss_mem_macro_cache: succ add macro data", K(macro_id), K(buf_size), K(offset), K(crc), K(mem_blk_handle));
    }
  }
  return ret;
}

int ObSSMemMacroCacheBufMgr::acquire_cold_mem_blocks(
    const int64_t exp_mem_blk_cnt, 
    ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(exp_mem_blk_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(exp_mem_blk_cnt));
  } else if (OB_ISNULL(buckets_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buckets should be valid", KR(ret), KP_(buckets));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < bucket_cnt_ - 1); ++i) { // ready to free large macro_blocks' mem_blk
      if (OB_ISNULL(buckets_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bucket should be valid", KR(ret), K(i), KP(buckets_[i]), K_(bucket_cnt));
      } else if (OB_FAIL(buckets_[i]->try_acquire_cold_mem_blocks(exp_mem_blk_cnt, mem_blk_handles))) {
        LOG_WARN("fail to try acqure cold mem_blocks", KR(ret), K(i), K_(bucket_cnt), K(exp_mem_blk_cnt));
      }
      ret = OB_SUCCESS; // continue to handle next
    }

    if (OB_SUCC(ret) && (mem_blk_handles.count() < exp_mem_blk_cnt)) { // ready to free small macro_blocks' mem_blk
      if (OB_ISNULL(buckets_[bucket_cnt_ - 1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last bucket should be valid", KR(ret), KP(buckets_[bucket_cnt_ - 1]), K_(bucket_cnt));
      } else if (OB_FAIL(buckets_[bucket_cnt_ - 1]->try_acquire_cold_mem_blocks(exp_mem_blk_cnt, mem_blk_handles))) {
        LOG_WARN("fail to try acqure cold mem_blocks", KR(ret), K_(bucket_cnt), K(exp_mem_blk_cnt));
      }
    }

    if (OB_SUCC(ret) && (mem_blk_handles.count() < exp_mem_blk_cnt)) { // ready to free unsealed mem_blk
      for (int64_t i = 0; OB_SUCC(ret) && (i < bucket_cnt_); ++i) {
        if (OB_ISNULL(buckets_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bucket should be valid", KR(ret), K(i), KP(buckets_[i]), K_(bucket_cnt));
        } else if (mem_blk_handles.count() < exp_mem_blk_cnt) {
          ObSSMacroCacheMemBlkHandle mem_blk_handle;
          buckets_[i]->get_first_mem_block(mem_blk_handle);
          if (mem_blk_handle.is_valid() && OB_FAIL(mem_blk_handles.push_back(mem_blk_handle))) {
            LOG_WARN("fail to push back", KR(ret), K(i), K(mem_blk_handle), K(mem_blk_handles));
          }
        } else {
          break;
        }
        ret = OB_SUCCESS; // continue to handle next
      }
    }
  }
  return ret;
}

int ObSSMemMacroCacheBufMgr::free_cold_mem_blocks(ObArray<ObSSMacroCacheMemBlkHandle> &mem_blk_handles, int64_t &evict_blk_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t mem_blk_cnt = mem_blk_handles.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < mem_blk_cnt); ++i) {
    ObSSMacroCacheMemBlkHandle &mem_blk_handle = mem_blk_handles.at(i);
    if (OB_UNLIKELY(!mem_blk_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("mem_block handle should be valid", KR(ret), K(i), K(mem_blk_cnt), K(mem_blk_handle));
    } else {
      ObSSMacroCacheMemBucket *bucket = inner_get_bucket(mem_blk_handle()->get_bucket_idx());
      if (OB_ISNULL(bucket)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bucket should be valid", KR(ret), KPC(mem_blk_handle.get_ptr()), K_(bucket_cnt), KP_(buckets));
      } else if (OB_FAIL(bucket->try_free_cold_mem_block(mem_blk_handle, evict_blk_cnt))) {
        LOG_WARN("fail to try free cold mem_block", KR(ret), K(i), K(mem_blk_handle), KPC(mem_blk_handle.get_ptr()));
      }
    }
    ret = OB_SUCCESS; // continue to handle next
  }
  return ret;
}

ObSSMacroCacheMemBucket *ObSSMemMacroCacheBufMgr::inner_get_bucket(const MacroBlockId &macro_id, const int64_t buf_size)
{
  ObSSMacroCacheMemBucket *bucket = nullptr;
  if (nullptr != buckets_ && bucket_cnt_ > 0) {
    if (1 == bucket_cnt_) {
      bucket = buckets_[0];
    } else {
      if (buf_size < SS_SMALL_MACRO_BLOCK_SIZE) {
        bucket = buckets_[bucket_cnt_ - 1];
      } else {
        int64_t idx = macro_id.hash() % (bucket_cnt_ - 1);
        bucket = buckets_[idx];
      }
    }
  }
  return bucket;
}

ObSSMacroCacheMemBucket *ObSSMemMacroCacheBufMgr::inner_get_bucket(const int32_t bucket_idx)
{
  ObSSMacroCacheMemBucket *bucket = nullptr;
  if (bucket_idx >= 0 && bucket_idx < bucket_cnt_ && nullptr != buckets_) {
    bucket = buckets_[bucket_idx];
  }
  return bucket;
}

} // namespace storage
} // namespace oceanbase
