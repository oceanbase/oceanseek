/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <vector>
#define private public
#include "lib/container/ob_array.h"
#undef private
#include "lib/alloc/alloc_func.h"
#include "lib/resource/ob_resource_mgr.h"
namespace oceanbase
{
using namespace common;
namespace lib
{
TEST(TestTenantMemoryMgr, basic)
{
  ObTenantMemoryMgr memory_mgr(1);
  const int64_t limit = 1 * 1024 * 1024 * 1024;
  memory_mgr.set_max(limit);
  ASSERT_TRUE(NULL ==  memory_mgr.alloc_chunk(-1, ObMemAttr()));
  ObMemAttr attr;
  attr.tenant_id_ = 2;
  ASSERT_TRUE(NULL == memory_mgr.alloc_chunk(1024, attr));
  attr.tenant_id_ = 1;
  attr.ctx_id_ = 0;

  // alloc, then free
  const int64_t alloc_size = ACHUNK_SIZE;
  const int64_t aligned_size = CHUNK_MGR.aligned(alloc_size);
  const int64_t max_alloc_count = limit / aligned_size;
  ObArray<void *> chunks;
  for (int64_t i = 0; i < max_alloc_count; ++i) {
    ObMemAttr attr;
    attr.tenant_id_ = 1;
    attr.ctx_id_ = (i % 2 == 0 ? 0 : 1);
    AChunk *chunk = NULL;
    chunk = memory_mgr.alloc_chunk(alloc_size, attr);
    ASSERT_TRUE(NULL != chunk);
    ASSERT_EQ(OB_SUCCESS, chunks.push_back(chunk));
  }
  ASSERT_EQ(limit, memory_mgr.get_sum_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  ASSERT_EQ(limit / 2, memory_mgr.get_ctx_hold_bytes()[0]);
  ASSERT_EQ(limit / 2, memory_mgr.get_ctx_hold_bytes()[1]);
  for (int64_t i = 2; i < ObCtxIds::MAX_CTX_ID; ++i) {
    ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[i]);
  }

  AChunk * chunk = NULL;
  chunk = memory_mgr.alloc_chunk(alloc_size, attr);
  ASSERT_TRUE(NULL == chunk);
  for (int64_t i = 0; i < chunks.count(); ++i) {
    attr.tenant_id_ = 1;
    attr.ctx_id_ = (i % 2 == 0 ? 0 : 1);
    memory_mgr.free_chunk((AChunk *)chunks.at(i), attr);
  }
  ASSERT_EQ(0, memory_mgr.get_sum_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; ++i) {
    ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[i]);
  }
  chunks.reuse();

  // 10 cache alloc and free
  for (int64_t i = 0; i < max_alloc_count - 10; ++i) {
    ObMemAttr attr;
    attr.tenant_id_ = 1;
    attr.ctx_id_ = (i % 2 == 0 ? 0 : 1);
    AChunk *chunk = NULL;
    chunk = memory_mgr.alloc_chunk(alloc_size, attr);
    ASSERT_TRUE(NULL != chunk);
    ASSERT_EQ(OB_SUCCESS, chunks.push_back(chunk));
  }
  ObArray<void *> cache_mbs;
  for (int64_t i = 0; i < 10; ++i) {
    void *ptr = NULL;
    ptr = memory_mgr.alloc_cache_mb(alloc_size);
    ASSERT_TRUE(NULL != ptr);
    ASSERT_EQ(OB_SUCCESS, cache_mbs.push_back(ptr));
  }
  ASSERT_EQ(limit, memory_mgr.get_sum_hold());
  ASSERT_EQ(aligned_size * 10, memory_mgr.get_cache_hold());
  ASSERT_EQ(10, memory_mgr.get_cache_item_count());
  ASSERT_EQ((max_alloc_count - 10) * aligned_size /2, memory_mgr.get_ctx_hold_bytes()[0]);
  ASSERT_EQ((max_alloc_count - 10) * aligned_size /2, memory_mgr.get_ctx_hold_bytes()[1]);
  for (int64_t i = 2; i < ObCtxIds::MAX_CTX_ID; ++i) {
    ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[i]);
  }
  chunk = NULL;
  chunk = memory_mgr.alloc_chunk(alloc_size, attr);
  ASSERT_TRUE(NULL == chunk);
  for (int64_t i = 0; i < chunks.count(); ++i) {
    attr.tenant_id_ = 1;
    attr.ctx_id_ = (i % 2 == 0 ? 0 : 1);
    memory_mgr.free_chunk((AChunk *)chunks.at(i), attr);
  }
  for (int64_t i = 0; i < cache_mbs.count(); ++i) {
    memory_mgr.free_cache_mb(cache_mbs.at(i));
  }
  ASSERT_EQ(0, memory_mgr.get_sum_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  for (int64_t i = 0; i < ObCtxIds::MAX_CTX_ID; ++i) {
    ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[i]);
  }
  chunks.reuse();
  cache_mbs.reuse();
}

class FakeCacheWasher : public ObICacheWasher
{
public:
  FakeCacheWasher(const uint64_t tenant_id, const int64_t mb_size)
    : tenant_id_(tenant_id), mb_size_(mb_size), mb_blocks_(NULL), memory_mgr_(NULL)
  {
  }
  virtual ~FakeCacheWasher() {}

  int erase_cache(const uint64_t ) override { return 0; }
  int alloc_mb(ObTenantMemoryMgr &mgr)
  {
    int ret = OB_SUCCESS;
    void *ptr = NULL;
    memory_mgr_ = &mgr;
    if (NULL == (ptr = mgr.alloc_cache_mb(mb_size_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "alloc_cache_mb failed", K(ret), K_(tenant_id), K_(mb_size));
    } else {
      ObCacheMemBlock *block = new (ptr) ObCacheMemBlock();
      block->next_ = mb_blocks_;
      mb_blocks_ = block;
    }
    return ret;
  }
  int sync_wash_mbs(const uint64_t tenant_id, const int64_t wash_size,
                    ObCacheMemBlock *&wash_blocks)
  {
    int ret = OB_SUCCESS;
    int64_t left_to_washed = wash_size;
    ObCacheMemBlock *washed_blocks = NULL;
    if (tenant_id != tenant_id_) {
      ret = OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
    }
    while (OB_SUCC(ret) && left_to_washed > 0) {
      if (NULL == mb_blocks_) {
        ret = OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
        LIB_LOG(WARN, "free block not enough", K(ret), K(wash_size));
      } else {
        ObCacheMemBlock *free_block = mb_blocks_;
        mb_blocks_ = free_block->next_;
        free_block->next_ = washed_blocks;
        washed_blocks = free_block;
        left_to_washed -= CHUNK_MGR.aligned(mb_size_);
      }
    }
    if (OB_SUCC(ret)) {
      wash_blocks = washed_blocks;
    } else {
      // Match ObKVCacheStore's ownership contract: partial blocks are
      // released internally when the requested wash size cannot be met.
      while (NULL != washed_blocks) {
        ObCacheMemBlock *block = washed_blocks;
        washed_blocks = block->next_;
        if (NULL != memory_mgr_) {
          memory_mgr_->free_cache_mb(block);
        }
      }
    }
    return ret;
  }
  void free_mbs(ObTenantMemoryMgr &mgr)
  {
    ObCacheMemBlock *mb = mb_blocks_;
    while (NULL != mb) {
      mb_blocks_ = mb_blocks_->next_;
      mgr.free_cache_mb(mb);
      mb = mb_blocks_;
    }
  }
private:
  uint64_t tenant_id_;
  int64_t mb_size_;
  ObCacheMemBlock *mb_blocks_;
  ObTenantMemoryMgr *memory_mgr_;
};

class ServerFallbackCacheWasher : public ObICacheWasher
{
public:
  ServerFallbackCacheWasher(const uint64_t tenant_id,
                            ObTenantMemoryMgr &tenant_mgr,
                            ObTenantMemoryMgr &server_mgr,
                            const int64_t mb_size)
    : tenant_id_(tenant_id),
      tenant_mgr_(tenant_mgr),
      server_mgr_(server_mgr),
      mb_size_(mb_size),
      tenant_blocks_(NULL),
      server_blocks_(NULL),
      tenant_wash_count_(0),
      server_wash_count_(0),
      first_wash_tenant_id_(OB_INVALID_TENANT_ID)
  {
  }

  ~ServerFallbackCacheWasher()
  {
    free_blocks(tenant_mgr_, tenant_blocks_);
    free_blocks(server_mgr_, server_blocks_);
  }

  int erase_cache(const uint64_t) override { return OB_SUCCESS; }

  void add_tenant_block(void *ptr)
  {
    add_block(ptr, tenant_blocks_);
  }

  void add_server_block(void *ptr)
  {
    add_block(ptr, server_blocks_);
  }

  int64_t get_tenant_wash_count() const { return tenant_wash_count_; }
  int64_t get_server_wash_count() const { return server_wash_count_; }
  uint64_t get_first_wash_tenant_id() const { return first_wash_tenant_id_; }

  void free_remaining_blocks()
  {
    free_blocks(tenant_mgr_, tenant_blocks_);
    free_blocks(server_mgr_, server_blocks_);
  }

  int sync_wash_mbs(const uint64_t tenant_id,
                    const int64_t wash_size,
                    ObCacheMemBlock *&wash_blocks) override
  {
    int ret = OB_SUCCESS;
    wash_blocks = NULL;
    if (OB_INVALID_TENANT_ID == first_wash_tenant_id_) {
      first_wash_tenant_id_ = tenant_id;
    }
    if (tenant_id_ == tenant_id) {
      ++tenant_wash_count_;
      // Model the real store's partial-wash contract: it may release some
      // tenant cache internally and return NOT_ENOUGH with no output list.
      if (NULL != tenant_blocks_) {
        ObCacheMemBlock *block = tenant_blocks_;
        tenant_blocks_ = block->next_;
        tenant_mgr_.free_cache_mb(block);
      }
      ret = OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
    } else if (OB_SERVER_TENANT_ID == tenant_id) {
      ++server_wash_count_;
      int64_t left_to_wash = wash_size;
      ObCacheMemBlock *washed_blocks = NULL;
      while (left_to_wash > 0 && NULL != server_blocks_) {
        ObCacheMemBlock *block = server_blocks_;
        server_blocks_ = block->next_;
        block->next_ = washed_blocks;
        washed_blocks = block;
        left_to_wash -= CHUNK_MGR.aligned(mb_size_);
      }
      if (left_to_wash > 0) {
        // The production KVCache store owns and frees a partial result when it
        // returns an error. Keep the fake's ownership contract identical.
        free_blocks(server_mgr_, washed_blocks);
        ret = OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
      } else {
        wash_blocks = washed_blocks;
      }
    } else {
      ret = OB_CACHE_FREE_BLOCK_NOT_ENOUGH;
    }
    return ret;
  }

private:
  static void add_block(void *ptr, ObCacheMemBlock *&head)
  {
    ObCacheMemBlock *block = new (ptr) ObCacheMemBlock();
    block->next_ = head;
    head = block;
  }

  static void free_blocks(ObTenantMemoryMgr &mgr, ObCacheMemBlock *&head)
  {
    while (NULL != head) {
      ObCacheMemBlock *block = head;
      head = block->next_;
      mgr.free_cache_mb(block);
    }
  }

  uint64_t tenant_id_;
  ObTenantMemoryMgr &tenant_mgr_;
  ObTenantMemoryMgr &server_mgr_;
  int64_t mb_size_;
  ObCacheMemBlock *tenant_blocks_;
  ObCacheMemBlock *server_blocks_;
  int64_t tenant_wash_count_;
  int64_t server_wash_count_;
  uint64_t first_wash_tenant_id_;
};

class MemoryLimitGuard
{
public:
  MemoryLimitGuard() : old_limit_(get_memory_limit()) {}
  ~MemoryLimitGuard() { set_memory_limit(old_limit_); }
private:
  int64_t old_limit_;
};

TEST(TestTenantMemoryMgr, global_oom_washes_server_tenant_before_local_cache)
{
  MemoryLimitGuard memory_limit_guard;
  CHUNK_MGR.sync_wash();

  ObTenantResourceMgrHandle server_resource_handle;
  ASSERT_EQ(OB_SUCCESS,
            ObResourceMgr::get_instance().get_tenant_resource_mgr(
                OB_SERVER_TENANT_ID, server_resource_handle));
  ASSERT_NE(nullptr, server_resource_handle.get_memory_mgr());
  ObTenantMemoryMgr &server_mgr = *server_resource_handle.get_memory_mgr();

  const uint64_t tenant_id = 1001;
  const int64_t mb_size = ACHUNK_SIZE;
  const int64_t request_size = 2 * ACHUNK_SIZE;
  const int64_t request_hold = CHUNK_MGR.hold(request_size);
  const int64_t block_hold = CHUNK_MGR.aligned(mb_size);
  const int64_t wash_size = request_hold
      + ObTenantMemoryMgr::LARGE_REQUEST_EXTRA_MB_COUNT * INTACT_ACHUNK_SIZE;
  const int64_t tenant_cache_count = wash_size / block_hold + 2;
  const int64_t server_cache_count = (wash_size + block_hold - 1) / block_hold;

  ObTenantMemoryMgr tenant_mgr(tenant_id);
  tenant_mgr.set_limit(INT64_MAX);
  tenant_mgr.set_max(INT64_MAX);
  ServerFallbackCacheWasher washer(tenant_id, tenant_mgr, server_mgr, mb_size);

  std::vector<void *> tenant_unwashable_blocks;
  for (int64_t i = 0; i < tenant_cache_count; ++i) {
    void *block = tenant_mgr.alloc_cache_mb(mb_size);
    ASSERT_NE(nullptr, block);
    if (0 == i) {
      washer.add_tenant_block(block);
    } else {
      tenant_unwashable_blocks.push_back(block);
    }
  }

  const int64_t server_cache_hold_before = server_mgr.get_cache_hold();
  for (int64_t i = 0; i < server_cache_count; ++i) {
    void *block = server_mgr.alloc_cache_mb(mb_size);
    ASSERT_NE(nullptr, block);
    washer.add_server_block(block);
  }
  ASSERT_EQ(server_cache_hold_before + server_cache_count * block_hold,
            server_mgr.get_cache_hold());

  tenant_mgr.set_cache_washer(washer);
  // Leave no server-level headroom. The tenant is below its own limit, so the
  // initial failure is SERVER_HOLD_REACH_LIMIT. Both tenants have washable
  // cache, but tenant 500 must satisfy the allocation before local cache is
  // considered.
  set_memory_limit(get_memory_hold());
  ObMemAttr attr(tenant_id, "FallbackTest", 1);
  AChunk *chunk = tenant_mgr.alloc_chunk(request_size, attr);

  EXPECT_NE(nullptr, chunk);
  EXPECT_EQ(OB_SERVER_TENANT_ID, washer.get_first_wash_tenant_id());
  EXPECT_EQ(0, washer.get_tenant_wash_count());
  EXPECT_EQ(1, washer.get_server_wash_count());
  EXPECT_EQ(server_cache_hold_before, server_mgr.get_cache_hold());
  EXPECT_EQ(tenant_cache_count * block_hold, tenant_mgr.get_cache_hold());

  if (nullptr != chunk) {
    tenant_mgr.free_chunk(chunk, attr);
  }
  for (void *block : tenant_unwashable_blocks) {
    tenant_mgr.free_cache_mb(block);
  }
  washer.free_remaining_blocks();
  EXPECT_EQ(0, tenant_mgr.get_cache_hold());
  EXPECT_EQ(server_cache_hold_before, server_mgr.get_cache_hold());
}

TEST(TestTenantMemoryMgr, global_oom_combines_partial_server_and_small_local_cache)
{
  MemoryLimitGuard memory_limit_guard;
  CHUNK_MGR.sync_wash();

  ObTenantResourceMgrHandle server_resource_handle;
  ASSERT_EQ(OB_SUCCESS,
            ObResourceMgr::get_instance().get_tenant_resource_mgr(
                OB_SERVER_TENANT_ID, server_resource_handle));
  ASSERT_NE(nullptr, server_resource_handle.get_memory_mgr());
  ObTenantMemoryMgr &server_mgr = *server_resource_handle.get_memory_mgr();

  const uint64_t tenant_id = 1003;
  const int64_t mb_size = ACHUNK_SIZE;
  const int64_t request_size = 2 * ACHUNK_SIZE;
  const int64_t block_hold = CHUNK_MGR.aligned(mb_size);

  ObTenantMemoryMgr tenant_mgr(tenant_id);
  tenant_mgr.set_limit(INT64_MAX);
  tenant_mgr.set_max(INT64_MAX);
  ServerFallbackCacheWasher washer(tenant_id, tenant_mgr, server_mgr, mb_size);

  // Neither cache can satisfy a two-chunk request alone. The allocation can
  // succeed only if the server cache is tried first and the single local
  // block is not skipped merely because it is smaller than wash_size.
  void *tenant_block = tenant_mgr.alloc_cache_mb(mb_size);
  ASSERT_NE(nullptr, tenant_block);
  washer.add_tenant_block(tenant_block);

  const int64_t server_cache_hold_before = server_mgr.get_cache_hold();
  void *server_block = server_mgr.alloc_cache_mb(mb_size);
  ASSERT_NE(nullptr, server_block);
  washer.add_server_block(server_block);

  tenant_mgr.set_cache_washer(washer);
  set_memory_limit(get_memory_hold());
  ObMemAttr attr(tenant_id, "FallbackOrder", 1);
  AChunk *chunk = tenant_mgr.alloc_chunk(request_size, attr);

  EXPECT_NE(nullptr, chunk);
  EXPECT_EQ(OB_SERVER_TENANT_ID, washer.get_first_wash_tenant_id());
  EXPECT_EQ(1, washer.get_server_wash_count());
  EXPECT_EQ(1, washer.get_tenant_wash_count());
  EXPECT_EQ(server_cache_hold_before, server_mgr.get_cache_hold());
  EXPECT_EQ(0, tenant_mgr.get_cache_hold());

  if (nullptr != chunk) {
    tenant_mgr.free_chunk(chunk, attr);
  }
  washer.free_remaining_blocks();
  EXPECT_EQ(0, tenant_mgr.get_cache_hold());
  EXPECT_EQ(server_cache_hold_before, server_mgr.get_cache_hold());
}

TEST(TestTenantMemoryMgr, tenant_limit_failure_does_not_wash_server_tenant)
{
  ObTenantResourceMgrHandle server_resource_handle;
  ASSERT_EQ(OB_SUCCESS,
            ObResourceMgr::get_instance().get_tenant_resource_mgr(
                OB_SERVER_TENANT_ID, server_resource_handle));
  ASSERT_NE(nullptr, server_resource_handle.get_memory_mgr());

  const uint64_t tenant_id = 1002;
  ObTenantMemoryMgr tenant_mgr(tenant_id);
  tenant_mgr.set_limit(1);
  tenant_mgr.set_max(1);
  ServerFallbackCacheWasher washer(
      tenant_id, tenant_mgr, *server_resource_handle.get_memory_mgr(), ACHUNK_SIZE);
  tenant_mgr.set_cache_washer(washer);

  ObMemAttr attr(tenant_id, "TenantLimitTest", 1);
  EXPECT_EQ(nullptr, tenant_mgr.alloc_chunk(ACHUNK_SIZE, attr));
  EXPECT_EQ(0, washer.get_tenant_wash_count());
  EXPECT_EQ(0, washer.get_server_wash_count());
}

TEST(TestTenantMemoryMgr, sync_wash)
{
  int ret = OB_SUCCESS;
  const int64_t limit = 2L * 1024L * 1024L * 1024L;
  oceanbase::lib::set_memory_limit(limit);
  FakeCacheWasher washer(1, ACHUNK_SIZE);
  ObTenantMemoryMgr memory_mgr(1);
  ObArray<void *> chunks;
  chunks.reserve(512);
  const int64_t tenant_limit = 2 * limit;
  memory_mgr.set_limit(tenant_limit);
  int64_t mb_count = 0;
  while (OB_SUCC(washer.alloc_mb(memory_mgr))) {
    ++mb_count;
  }
  ret = OB_SUCCESS;
  // check cache stat
  const int64_t aligned_size = CHUNK_MGR.aligned(ACHUNK_SIZE);
  ASSERT_EQ(aligned_size * mb_count, memory_mgr.get_cache_hold());
  ASSERT_EQ(mb_count, memory_mgr.get_cache_item_count());
  ASSERT_EQ(aligned_size * mb_count, memory_mgr.get_sum_hold());

  memory_mgr.set_cache_washer(washer);
  int64_t alloc_count = 0;
  ObMemAttr attr;
  attr.tenant_id_ = 1;
  attr.ctx_id_ = 1;
  while (OB_SUCC(ret)) {
    void *ptr = NULL;
    if (NULL == (ptr = memory_mgr.alloc_chunk(ACHUNK_SIZE, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "alloc_chunk failed", K(ret));
    } else if (OB_FAIL(chunks.push_back(ptr))) {
      LIB_LOG(WARN, "push_back failed", K(ret));
    } else {
      ++alloc_count;
    }
  }
  // check stat, left one mb in cache
  mb_count -= alloc_count;
  ASSERT_EQ(aligned_size * mb_count, memory_mgr.get_cache_hold());
  ASSERT_EQ(mb_count, memory_mgr.get_cache_item_count());
  ASSERT_EQ(aligned_size * (mb_count + alloc_count), memory_mgr.get_sum_hold());

  for (int64_t i = 0; i < chunks.count(); ++i) {
    memory_mgr.free_chunk((AChunk *)chunks.at(i), attr);
  }
  for (int64_t i = 0; i < mb_count; ++i) {
    washer.free_mbs(memory_mgr);
  }
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[0]);
  ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[1]);
  ASSERT_EQ(0, memory_mgr.get_sum_hold());
}

TEST(TestTenantMemoryMgr, DISABLED_large_sync_wash)
{
  int ret = OB_SUCCESS;
  const int64_t limit = 1L * 1024L * 1024L * 1024L;
  const int64_t aligned_size = CHUNK_MGR.aligned(ACHUNK_SIZE);
  oceanbase::lib::set_memory_limit(limit);
  FakeCacheWasher washer(1, ACHUNK_SIZE);
  ObTenantMemoryMgr memory_mgr(1);
  ObArray<void *> chunks;
  chunks.reserve(512);
  const int64_t tenant_limit = 2 * limit;
  memory_mgr.set_limit(tenant_limit);
  int64_t mb_count = 0;
  while (OB_SUCC(washer.alloc_mb(memory_mgr))) {
    ++mb_count;
  }
  ret = OB_SUCCESS;

  memory_mgr.set_cache_washer(washer);
  int64_t alloc_count = 0;
  ObMemAttr attr;
  attr.tenant_id_ = 1;
  attr.ctx_id_ = 1;
  ASSERT_EQ(aligned_size * mb_count, memory_mgr.get_sum_hold());
  while (OB_SUCC(ret)) {
    void *ptr = NULL;
    if (NULL == (ptr = memory_mgr.alloc_chunk(ACHUNK_SIZE * 2, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "alloc_chunk failed", K(ret));
    } else if (OB_FAIL(chunks.push_back(ptr))) {
      LIB_LOG(WARN, "push_back failed", K(ret));
    } else {
      ++alloc_count;
    }
  }
  // left 4 mb
  ASSERT_EQ((mb_count - 2 * alloc_count) * aligned_size, memory_mgr.get_cache_hold());
  ASSERT_EQ((mb_count - 2 * alloc_count) , memory_mgr.get_cache_item_count());

  ASSERT_TRUE(mb_count - 2 * alloc_count <= 4);

  for (int64_t i = 0; i < chunks.count(); ++i) {
    memory_mgr.free_chunk((AChunk *)chunks.at(i), attr);
  }
  washer.free_mbs(memory_mgr);
  ASSERT_EQ(0, memory_mgr.get_cache_hold());
  ASSERT_EQ(0, memory_mgr.get_cache_item_count());
  ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[0]);
  ASSERT_EQ(0, memory_mgr.get_ctx_hold_bytes()[1]);
  ASSERT_EQ(0, memory_mgr.get_sum_hold());
}

TEST(TestResourceMgr, basic)
{
  ObResourceMgr mgr;
  FakeCacheWasher washer(1, ACHUNK_SIZE);
  ObTenantResourceMgrHandle tenant_mgr;
  ObTenantResourceMgrHandle tenant_mgrs[ObResourceMgr::MAX_TENANT_COUNT];
  ASSERT_EQ(OB_NOT_INIT, mgr.set_cache_washer(washer));
  ASSERT_EQ(OB_NOT_INIT, mgr.get_tenant_resource_mgr(1, tenant_mgr));
  ASSERT_EQ(OB_SUCCESS, mgr.init());

  ASSERT_EQ(OB_INVALID_ARGUMENT, mgr.get_tenant_resource_mgr(OB_INVALID_ID, tenant_mgr));
  ASSERT_EQ(OB_INIT_TWICE, mgr.init());

  ASSERT_EQ(OB_SUCCESS, mgr.set_cache_washer(washer));
  for (int64_t i = 0; i < ObResourceMgr::MAX_TENANT_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr.get_tenant_resource_mgr(i, tenant_mgrs[i]));
    ASSERT_EQ(i, tenant_mgrs[i].get_memory_mgr()->get_tenant_id());
  }

  ObArray<void *> mbs;
  for (int64_t i = 0; i < 10; ++i) {
    void *ptr = NULL;
    ptr = tenant_mgrs[i].get_memory_mgr()->alloc_cache_mb(ACHUNK_SIZE);
    ASSERT_TRUE(NULL != ptr);
    ASSERT_EQ(OB_SUCCESS, mbs.push_back(ptr));
  }

  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(CHUNK_MGR.aligned(ACHUNK_SIZE), tenant_mgrs[i].get_memory_mgr()->get_cache_hold());
    ASSERT_EQ(i, tenant_mgrs[i].get_memory_mgr()->get_tenant_id());
  }
  for (int64_t i = 0; i < 10; ++i) {
    tenant_mgrs[i].get_memory_mgr()->free_cache_mb(mbs.at(i));
  }

  for (int64_t i = 0; i < ObResourceMgr::MAX_TENANT_COUNT; ++i) {
    ObTenantResourceMgrHandle tenant_mgr;
    ASSERT_EQ(OB_SUCCESS, mgr.get_tenant_resource_mgr(i, tenant_mgr));
    ASSERT_EQ(0, tenant_mgr.get_memory_mgr()->get_cache_hold());
  }
  for (int64_t i = 0; i < ObResourceMgr::MAX_TENANT_COUNT; ++i) {
    tenant_mgrs[i].reset();
  }

  for (int64_t i = 0; i < ObResourceMgr::MAX_TENANT_COUNT; ++i) {
    ObTenantResourceMgr *resource_mgr = NULL;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, mgr.get_tenant_resource_mgr_unsafe(i, resource_mgr));
  }
  mgr.destroy();
}
}//end namespace lib
}//end namespace oceanbase

int main(int argc, char *argv[])
{
  OB_LOGGER.set_disable_logging(true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
