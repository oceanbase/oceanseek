/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

int ObSSMemMacroCacheUtil::alloc_mem_block(
    const uint64_t tenant_id,
    const int32_t blk_size,
    ObSSMacroCacheMemBlockPool &mem_blk_pool,
    ObSSMacroCacheMemBlock *&mem_blk)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (blk_size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(blk_size));
  } else {
    ObMemAttr buf_attr(tenant_id, "MaCMemBlkBuf");
    ObMemAttr blk_attr(tenant_id, "MaCMemBlk");
    char *buffer = nullptr;
    void *mem_blk_ptr = nullptr;
    const int64_t ptr_size = sizeof(ObSSMacroCacheMemBlock);
    if (OB_ISNULL(buffer = static_cast<char *>(ob_malloc_align(OB_DIRECT_IO_ALIGN, blk_size, buf_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(blk_size), K(buf_attr));
    } else if (OB_ISNULL(mem_blk_ptr = ob_malloc(ptr_size, blk_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(ptr_size), K(blk_attr));
    } else {
      mem_blk = new (mem_blk_ptr) ObSSMacroCacheMemBlock(mem_blk_pool);
    }

    if (OB_FAIL(ret)) {
      if (nullptr != mem_blk_ptr) {
        ob_free(mem_blk_ptr);
        mem_blk_ptr = nullptr;
      }
      if (nullptr != buffer) {
        ob_free_align(buffer);
        buffer = nullptr;
      }
    } else if (OB_FAIL(mem_blk->init(tenant_id, blk_size, buffer))) {
      LOG_WARN("fail to init mem_block", KR(ret), K(tenant_id), KP(buffer), K(blk_size));
      if (OB_TMP_FAIL(free_mem_block(mem_blk))) { // release memory when fail to init mem_block
        LOG_ERROR("fail to free mem_block", KR(tmp_ret), K(tenant_id), K(blk_size), KP(mem_blk));
        tmp_ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSSMemMacroCacheUtil::free_mem_block(ObSSMacroCacheMemBlock *mem_blk)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_blk)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mem_blk));
  } else {
    if (OB_NOT_NULL(mem_blk->buffer_)) {
      ob_free_align(mem_blk->buffer_);
    }
    mem_blk->~ObSSMacroCacheMemBlock();
    ob_free(mem_blk);
  }
  return ret;
}

int ObSSMemMacroCacheUtil::alloc_buckets(
    const uint64_t tenant_id, 
    const int64_t bucket_cnt,
    ObSSMacroCacheMemBlockPool &mem_blk_pool,
    ObSSMacroCacheMemBucket **&buckets)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (bucket_cnt <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(bucket_cnt));
  } else {
    ObMemAttr bkt_attr(tenant_id, "MaCMemBkt");
    ObMemAttr ptr_attr(tenant_id, "MaCMemBktPtr");
    char *bkts = nullptr;
    void *bkts_ptr = nullptr;
    const int64_t bkt_size = sizeof(ObSSMacroCacheMemBucket);
    const int64_t ptr_size = sizeof(ObSSMacroCacheMemBucket *);
    const int64_t bkts_size = bkt_size * bucket_cnt;
    const int64_t bkts_ptr_size = ptr_size * bucket_cnt;
    if (OB_ISNULL(bkts = static_cast<char *>(ob_malloc(bkts_size, bkt_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(bkts_size), K(bkt_attr));
    } else if (OB_ISNULL(bkts_ptr = ob_malloc(bkts_ptr_size, ptr_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(bkts_ptr_size), K(ptr_attr));
    } else if (FALSE_IT(buckets = static_cast<ObSSMacroCacheMemBucket **>(bkts_ptr))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < bucket_cnt); ++i) {
        buckets[i] = new (bkts + i * bkt_size) ObSSMacroCacheMemBucket(mem_blk_pool);
        if (OB_ISNULL(buckets[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("bucket should be valid", KR(ret), K(i), K(bkt_size), K(bkts_size), KP(bkts));
        } else {
          buckets[i]->set_bucket_idx(i);
        }
      }

      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i < bucket_cnt; ++i) {
          if (nullptr != buckets[i]) {
            buckets[i]->~ObSSMacroCacheMemBucket();
          }
        }
      }
    }
    
    if (OB_FAIL(ret)) {
      if (nullptr != bkts) {
        ob_free(bkts);
      }
      if (nullptr != bkts_ptr) {
        ob_free(bkts_ptr);
      }
      buckets = nullptr;
    } 
  }
  return ret;
}

int ObSSMemMacroCacheUtil::free_buckets(ObSSMacroCacheMemBucket **buckets, const int64_t bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buckets) || OB_UNLIKELY(bucket_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buckets), K(bucket_cnt));
  } else {
    for (int64_t i = 0; i < bucket_cnt; ++i) {
      if (nullptr != buckets[i]) {
        buckets[i]->~ObSSMacroCacheMemBucket();
      }
    }
    if (nullptr != buckets[0]) {
      ob_free(buckets[0]);
    }
    ob_free(buckets);
  }
  return ret;
}

int64_t ObSSMemMacroCacheUtil::get_random_close_ttl_time(const int64_t ttl_time_us, const int64_t max_wait_time_us)
{
  int ret = OB_SUCCESS;
  int64_t result_time_us = ObTimeUtility::current_time_us();
  if (OB_UNLIKELY(ttl_time_us < 0 || max_wait_time_us < 0 || max_wait_time_us > ttl_time_us)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ttl_time_us), K(max_wait_time_us));
  } else {
    const int64_t random_wait_time_us = ObRandom::rand(0, max_wait_time_us);
    const int64_t time_offset = ttl_time_us - random_wait_time_us;
    result_time_us = ObTimeUtility::current_time_us() - time_offset;
  }
  return result_time_us;
}

} // namespace storage
} // namespace oceanbase
