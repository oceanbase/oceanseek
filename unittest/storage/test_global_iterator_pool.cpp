/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/access/ob_async_inv_iterator_pool.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "lib/time/ob_time_utility.h"
#include "observer/omt/ob_tenant_config_mgr.h"
namespace oceanbase
{

using namespace share;
using namespace common;
using namespace storage;

namespace blocksstable
{
struct TestPooledIter
{
  bool *destroyed_ = nullptr;
  ~TestPooledIter() { if (nullptr != destroyed_) { *destroyed_ = true; } }
};
template <>
class ObInvIteratorTraits<TestPooledIter>
{
public:
  static constexpr ObInvIteratorType TYPE = ObInvIteratorType::POSTING;
  struct ReuseKey {};
};
}

class ObTestQueryRowIterator : public ObQueryRowIterator
{
public:
  ObTestQueryRowIterator(const ObQRIterType type) : ObQueryRowIterator(type) {}
  virtual int get_next_row(blocksstable::ObDatumRow *&row)
  {
    return OB_SUCCESS;
  }
  virtual void reset()
  {
  }
  virtual void reclaim()
  {
  }
};

class ObGlobalIteratorPoolTest: public ::testing::Test
{
public:
  ObGlobalIteratorPoolTest();
  virtual ~ObGlobalIteratorPoolTest() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
private:
  uint64_t tenant_id_;
  ObTenantBase tenant_base_;
};

ObGlobalIteratorPoolTest::ObGlobalIteratorPoolTest()
  : tenant_id_(1),
    tenant_base_(tenant_id_)
{
}

void ObGlobalIteratorPoolTest::SetUpTestCase()
{
  ASSERT_EQ(MockTenantModuleEnv::get_instance().init(), OB_SUCCESS);
}

void ObGlobalIteratorPoolTest::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void ObGlobalIteratorPoolTest::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
}

void ObGlobalIteratorPoolTest::TearDown()
{
}

TEST_F(ObGlobalIteratorPoolTest, posting_entry_traits_and_reuse)
{
  typedef blocksstable::ObInvIteratorEntry<blocksstable::TestPooledIter> TestEntry;
  bool destroyed = false;
  void *buf = ob_malloc(sizeof(TestEntry), ObMemAttr(MTL_ID(), "TestInvEntry"));
  ASSERT_NE(nullptr, buf);
  TestEntry *test_entry = new (buf) TestEntry();
  test_entry->iter_.destroyed_ = &destroyed;
  // Destruction follows the bound template callback, not the type enum.
  ObAsyncInvIteratorPool::destroy_inv_entry(test_entry);
  EXPECT_TRUE(destroyed);
  ObAsyncInvIteratorPool::destroy_inv_entry(nullptr);
  typedef blocksstable::ObInvPostingIterEntry Entry;
  Entry entry;
  Entry::ReuseKey key;
  entry.reuse_key_ = key;
  EXPECT_EQ(blocksstable::ObInvIteratorType::POSTING, entry.type_);
  EXPECT_EQ(sizeof(Entry), entry.accounted_bytes_);
  EXPECT_EQ(sizeof(Entry), entry.get_cached_mem_size());
  EXPECT_FALSE(entry.is_idle_in_global_pool_);
  EXPECT_EQ(nullptr, entry.next_global_entry_);
  EXPECT_EQ(nullptr, entry.prev_global_entry_);
  EXPECT_TRUE(entry.matches(key));
  entry.reuse();
  EXPECT_EQ(nullptr, entry.iter_.allocator_);
  EXPECT_TRUE(entry.matches(key));
}

class PostingPoolShrinkContext
{
public:
  void init()
  {
    omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
    if (!config.is_valid()) {
      ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(MTL_ID()));
    }
    omt::ObTenantConfigGuard valid_config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(valid_config.is_valid());
    old_limit_ = valid_config->async_inverted_index_iter_cache_max_size;
    config_saved_ = true;
    set_limit(8 * sizeof(blocksstable::ObInvPostingIterEntry));
    ASSERT_EQ(OB_SUCCESS, pool_.init());
  }

  ~PostingPoolShrinkContext()
  {
    for (int64_t i = 0; i < 8; ++i) {
      pool_.release_inv_entry(entries_[i]);
    }
    pool_.drain_idle_inv_entries();
    if (config_saved_) {
      set_limit(old_limit_);
    }
  }

  void set_limit(const int64_t bytes)
  {
    omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(config.is_valid());
    config->async_inverted_index_iter_cache_max_size = bytes;
  }

  void fill()
  {
    for (int64_t i = 0; i < 8; ++i) {
      ASSERT_EQ(OB_SUCCESS, pool_.get_inv_entry(blocksstable::ObInvPostingIterEntry::ReuseKey{}, entries_[i]));
      ASSERT_NE(nullptr, entries_[i]);
    }
    for (int64_t i = 0; i < 8; ++i) {
      pool_.release_inv_entry(entries_[i]);
    }
    ASSERT_EQ(8, pool_.inv_idle_count_);
  }

  void expire_window()
  {
    pool_.inv_shrink_window_start_ = ObTimeUtility::current_monotonic_time()
        - 2 * ObAsyncInvIteratorPool::INV_SHRINK_INTERVAL;
  }

  void check_counts(const int64_t idle, const int64_t leased = 0)
  {
    const int64_t bytes = sizeof(blocksstable::ObInvPostingIterEntry);
    EXPECT_EQ(idle, pool_.inv_idle_count_);
    EXPECT_EQ(idle * bytes, pool_.inv_idle_bytes_);
    EXPECT_EQ((idle + leased) * bytes, pool_.inv_total_bytes_);
    int64_t count = 0;
    blocksstable::ObInvIteratorEntryBase *prev = nullptr;
    blocksstable::ObInvIteratorEntryBase *entry = pool_.inv_idle_head_;
    while (nullptr != entry && count <= idle) {
      EXPECT_EQ(prev, entry->prev_global_entry_);
      EXPECT_TRUE(entry->is_idle_in_global_pool_);
      prev = entry;
      entry = entry->next_global_entry_;
      ++count;
    }
    EXPECT_EQ(idle, count);
    EXPECT_EQ(nullptr, entry);
    EXPECT_EQ(prev, pool_.inv_idle_tail_);
  }

  ObAsyncInvIteratorPool pool_;
  blocksstable::ObInvPostingIterEntry *entries_[8] = {};
  bool config_saved_ = false;
  int64_t old_limit_ = 0;
};

TEST_F(ObGlobalIteratorPoolTest, posting_shrink_window_and_retain_floor)
{
  PostingPoolShrinkContext context;
  ASSERT_NO_FATAL_FAILURE(context.init());
  ASSERT_NO_FATAL_FAILURE(context.fill());
  context.set_limit(64L << 20);
  context.pool_.shrink_idle_inv_entries();
  EXPECT_GT(context.pool_.inv_shrink_window_start_, 0);
  EXPECT_EQ(8 * sizeof(blocksstable::ObInvPostingIterEntry), context.pool_.inv_idle_bytes_low_water_);
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(8);
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(8);
  context.set_limit(8 * sizeof(blocksstable::ObInvPostingIterEntry));
  // A future start must reset the window, not reclaim entries.
  context.pool_.inv_shrink_window_start_ = ObTimeUtility::current_monotonic_time()
      + ObAsyncInvIteratorPool::INV_SHRINK_INTERVAL;
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(8);
  context.pool_.inv_idle_bytes_low_water_ = 0;
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(8);
  EXPECT_EQ(8 * sizeof(blocksstable::ObInvPostingIterEntry),
      context.pool_.inv_idle_bytes_low_water_);
  context.pool_.inv_idle_bytes_low_water_ = 4 * sizeof(blocksstable::ObInvPostingIterEntry);
  blocksstable::ObInvIteratorEntryBase *newest = context.pool_.inv_idle_head_;
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(6);
  EXPECT_EQ(newest, context.pool_.inv_idle_head_);
  EXPECT_EQ(6 * sizeof(blocksstable::ObInvPostingIterEntry), context.pool_.inv_idle_bytes_low_water_);
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(4);
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(4);
  // A positive budget smaller than one Entry permits shrinking to an empty list.
  context.set_limit(1);
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(0);

  // Synthetic charges distinguish the 16 MiB floor from the 64 MiB budget.
  context.set_limit(64L << 20);
  ASSERT_NO_FATAL_FAILURE(context.fill());
  for (blocksstable::ObInvIteratorEntryBase *entry = context.pool_.inv_idle_head_;
       nullptr != entry; entry = entry->next_global_entry_) {
    entry->accounted_bytes_ = 4L << 20;
  }
  context.pool_.inv_idle_bytes_ = 32L << 20;
  context.pool_.inv_total_bytes_ = 32L << 20;
  context.pool_.inv_idle_bytes_low_water_ = 32L << 20;
  for (int64_t round = 0; round < 2; ++round) {
    context.expire_window();
    context.pool_.shrink_idle_inv_entries();
    EXPECT_EQ(4, context.pool_.inv_idle_count_);
    EXPECT_EQ(16L << 20, context.pool_.inv_idle_bytes_);
    EXPECT_EQ(16L << 20, context.pool_.inv_total_bytes_);
    EXPECT_EQ(16L << 20, context.pool_.inv_idle_bytes_low_water_);
  }
}

TEST_F(ObGlobalIteratorPoolTest, posting_shrink_borrowing_and_eviction_update_low_water)
{
  ObArenaAllocator scan_allocator;
  PostingPoolShrinkContext context;
  ASSERT_NO_FATAL_FAILURE(context.init());
  ASSERT_NO_FATAL_FAILURE(context.fill());
  context.pool_.shrink_idle_inv_entries();
  blocksstable::ObInvIteratorEntryBase *newest = context.pool_.inv_idle_head_;
  ASSERT_EQ(OB_SUCCESS, context.pool_.get_inv_entry(blocksstable::ObInvPostingIterEntry::ReuseKey{}, context.entries_[0]));
  EXPECT_EQ(newest, context.entries_[0]);
  EXPECT_FALSE(context.entries_[0]->is_idle_in_global_pool_);
  EXPECT_EQ(nullptr, context.entries_[0]->next_global_entry_);
  EXPECT_EQ(nullptr, context.entries_[0]->prev_global_entry_);
  EXPECT_EQ(7 * sizeof(blocksstable::ObInvPostingIterEntry), context.pool_.inv_idle_bytes_low_water_);
  context.check_counts(7, 1);
  ASSERT_NE(nullptr, scan_allocator.alloc(64));
  context.entries_[0]->iter_.allocator_ = &scan_allocator;
  context.pool_.release_inv_entry(context.entries_[0]);
  EXPECT_GT(scan_allocator.total(), 0);
  scan_allocator.reset();
  EXPECT_EQ(nullptr, context.entries_[0]);
  EXPECT_EQ(newest, context.pool_.inv_idle_head_);
  EXPECT_EQ(7 * sizeof(blocksstable::ObInvPostingIterEntry),
      context.pool_.inv_idle_bytes_low_water_);
  ASSERT_EQ(OB_SUCCESS, context.pool_.get_inv_entry(blocksstable::ObInvPostingIterEntry::ReuseKey{}, context.entries_[0]));
  EXPECT_EQ(newest, context.entries_[0]);
  EXPECT_EQ(nullptr, context.entries_[0]->iter_.allocator_);
  EXPECT_EQ(0, scan_allocator.total());
  ASSERT_EQ(OB_SUCCESS, context.pool_.get_inv_entry(blocksstable::ObInvPostingIterEntry::ReuseKey{}, context.entries_[1]));
  EXPECT_EQ(6 * sizeof(blocksstable::ObInvPostingIterEntry), context.pool_.inv_idle_bytes_low_water_);
  // A second same-type lease reuses an idle entry and lowers the watermark.
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(6, 2);
  // Lowering the budget evicts old idle entries and lowers the window minimum.
  context.set_limit(6 * sizeof(blocksstable::ObInvPostingIterEntry));
  context.pool_.release_inv_entry(context.entries_[0]);
  context.check_counts(5, 1);
  EXPECT_EQ(5 * sizeof(blocksstable::ObInvPostingIterEntry), context.pool_.inv_idle_bytes_low_water_);
  context.pool_.inv_idle_bytes_low_water_ = sizeof(blocksstable::ObInvPostingIterEntry);
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(4, 1);
  EXPECT_NE(nullptr, context.entries_[1]);
}

TEST_F(ObGlobalIteratorPoolTest, posting_shrink_zero_budget_and_reenable)
{
  PostingPoolShrinkContext context;
  ASSERT_NO_FATAL_FAILURE(context.init());
  for (int64_t i = 0; i < 2; ++i) {
    context.set_limit(8 * sizeof(blocksstable::ObInvPostingIterEntry));
    ASSERT_NO_FATAL_FAILURE(context.fill());
    ASSERT_EQ(OB_SUCCESS, context.pool_.get_inv_entry(blocksstable::ObInvPostingIterEntry::ReuseKey{}, context.entries_[0]));
    context.pool_.shrink_idle_inv_entries();
    context.set_limit(0);
    context.pool_.shrink_idle_inv_entries();
    context.check_counts(0, 1);
    EXPECT_EQ(0, context.pool_.inv_idle_bytes_low_water_);
    EXPECT_EQ(0, context.pool_.inv_shrink_window_start_);
    context.pool_.shrink_idle_inv_entries();
    context.check_counts(0, 1);
    context.pool_.release_inv_entry(context.entries_[0]);
    context.check_counts(0);
  }
}

TEST_F(ObGlobalIteratorPoolTest, posting_shrink_byte_charges_and_tail_floor)
{
  PostingPoolShrinkContext context;
  ASSERT_NO_FATAL_FAILURE(context.init());
  ASSERT_NO_FATAL_FAILURE(context.fill());
  const int64_t bytes = sizeof(blocksstable::ObInvPostingIterEntry);
  // Model unequal charges without adding a production iterator type.
  context.set_limit(10 * bytes);
  context.pool_.inv_idle_tail_->accounted_bytes_ += 2 * bytes;
  context.pool_.inv_idle_bytes_ += 2 * bytes;
  context.pool_.inv_total_bytes_ += 2 * bytes;
  context.pool_.shrink_idle_inv_entries();
  EXPECT_EQ(10 * bytes, context.pool_.inv_idle_bytes_low_water_);
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(5);
  EXPECT_EQ(5 * bytes, context.pool_.inv_idle_bytes_low_water_);
  // The remaining half-entry of headroom cannot accommodate a whole Entry.
  context.set_limit(9 * bytes);
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(5);
  // A later window can reclaim the same tail when the floor allows it.
  context.set_limit(8 * bytes);
  context.expire_window();
  context.pool_.shrink_idle_inv_entries();
  context.check_counts(4);
}

TEST_F(ObGlobalIteratorPoolTest, inv_stop_rejects_borrow_and_destroys_returns)
{
  PostingPoolShrinkContext context;
  ASSERT_NO_FATAL_FAILURE(context.init());
  ASSERT_NO_FATAL_FAILURE(context.fill());
  ASSERT_EQ(OB_SUCCESS, context.pool_.get_inv_entry(
      blocksstable::ObInvPostingIterEntry::ReuseKey{}, context.entries_[0]));
  EXPECT_EQ(2, context.pool_.refs_);
  context.pool_.stop();
  context.pool_.stop();
  context.check_counts(0, 1);
  ASSERT_EQ(OB_SUCCESS, context.pool_.get_inv_entry(
      blocksstable::ObInvPostingIterEntry::ReuseKey{}, context.entries_[1]));
  EXPECT_EQ(nullptr, context.entries_[1]);
  context.pool_.release_inv_entry(context.entries_[0]);
  context.check_counts(0);
  EXPECT_EQ(1, context.pool_.refs_);
}

TEST_F(ObGlobalIteratorPoolTest, inv_mtl_destroy_preserves_outstanding_lease)
{
  PostingPoolShrinkContext config;
  ASSERT_NO_FATAL_FAILURE(config.init());
  ObAsyncInvIteratorPool *pool = OB_NEW(ObAsyncInvIteratorPool, ObMemAttr(MTL_ID(), "TestInvPool"));
  ASSERT_NE(nullptr, pool);
  ASSERT_EQ(OB_SUCCESS, pool->init());
  blocksstable::ObInvPostingIterEntry *entry = nullptr;
  ASSERT_EQ(OB_SUCCESS, pool->get_inv_entry(blocksstable::ObInvPostingIterEntry::ReuseKey{}, entry));
  ASSERT_NE(nullptr, entry);
  ObAsyncInvIteratorPool *owner = pool;
  ObAsyncInvIteratorPool::mtl_destroy(pool);
  EXPECT_EQ(nullptr, pool);
  EXPECT_EQ(1, owner->refs_);
  EXPECT_TRUE(owner->stopped_);
  owner->release_inv_entry(entry);
  EXPECT_EQ(nullptr, entry);
  // No owner access after returning the last lease.
  ObAsyncInvIteratorPool::mtl_destroy(pool);
}

TEST_F(ObGlobalIteratorPoolTest, init)
{
  int ret = 0;
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = ObGlobalIteratorPool::ITER_POOL_TENANT_MIN_MEM_THRESHOLD + 1;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  const int64_t mem_limit = iter_pool.tenant_mem_user_limit_ * ObGlobalIteratorPool::ITER_POOL_MAX_MEM_PERCENT;
  const int64_t bucket_cnt = mem_limit / ObGlobalIteratorPool::ALL_ITER_TYPES_MEM_LIMIT;
  ASSERT_EQ(1, iter_pool.tenant_id_);
  ASSERT_EQ(bucket_cnt, iter_pool.bucket_cnt_);
  for (int64_t i = 0; i <= ObGlobalIteratorPool::ITER_POOL_MAX_CACHED_ITER_TYPE; ++i) {
    CachedIteratorNode *nodes = iter_pool.cached_node_array_[i];
    ASSERT_TRUE(nullptr != nodes);
    for (int64_t j = 0; j < iter_pool.bucket_cnt_; ++j) {
      CachedIteratorNode &node = nodes[j];
      ASSERT_FALSE(node.is_occupied_);
      ASSERT_TRUE(nullptr == node.iter_);
      ASSERT_TRUE(nullptr == node.stmt_iter_pool_);
    }
  }
}

TEST_F(ObGlobalIteratorPoolTest, get)
{
  int ret = 0;
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = ObGlobalIteratorPool::ITER_POOL_TENANT_MIN_MEM_THRESHOLD + 1;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ObQRIterType type = T_INVALID_ITER_TYPE;
  CachedIteratorNode *cached_node = nullptr;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ASSERT_TRUE(nullptr == cached_node);

  type = T_MAX_ITER_TYPE;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ASSERT_TRUE(nullptr == cached_node);

  type = T_SINGLE_GET;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node);
  ASSERT_TRUE(cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node->iter_);
  ASSERT_TRUE(nullptr != cached_node->stmt_iter_pool_);

  ObArenaAllocator *iter_alloc = cached_node->get_iter_allocator();
  void *buf =iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  ObTestQueryRowIterator *merge = new (buf) ObTestQueryRowIterator(type);
  cached_node->set_iter(merge);

  type = T_SINGLE_GET;
  CachedIteratorNode *cached_node1 = nullptr;
  ret = iter_pool.get(type, cached_node1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node1);

  iter_pool.release(cached_node);
  iter_pool.release(cached_node1);
  ret = iter_pool.get(type, cached_node1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node1);
  ASSERT_TRUE(cached_node1->is_occupied_);
  ASSERT_TRUE(nullptr != cached_node1->iter_);
  ASSERT_TRUE(nullptr != cached_node1->stmt_iter_pool_);
  ASSERT_TRUE(merge == cached_node1->iter_);
  iter_pool.release(cached_node1);

  type = T_MULTI_GET;
  CachedIteratorNode *mg_cached_node = nullptr;
  ret = iter_pool.get(type, mg_cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != mg_cached_node);
  ASSERT_TRUE(mg_cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == mg_cached_node->iter_);
  ASSERT_TRUE(nullptr != mg_cached_node->stmt_iter_pool_);
  iter_pool.release(mg_cached_node);

  type = T_SINGLE_SCAN;
  CachedIteratorNode *ss_cached_node = nullptr;
  ret = iter_pool.get(type, ss_cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != ss_cached_node);
  ASSERT_TRUE(ss_cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == ss_cached_node->iter_);
  ASSERT_TRUE(nullptr != ss_cached_node->stmt_iter_pool_);
  iter_pool.release(ss_cached_node);

  type = T_MULTI_SCAN;
  CachedIteratorNode *ms_cached_node = nullptr;
  ret = iter_pool.get(type, ms_cached_node);
  ASSERT_EQ(ret, OB_INVALID_ARGUMENT);
  ASSERT_TRUE(nullptr == ms_cached_node);
}

TEST_F(ObGlobalIteratorPoolTest, release)
{
  int ret = 0;
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = ObGlobalIteratorPool::ITER_POOL_TENANT_MIN_MEM_THRESHOLD + 1;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ObQRIterType type = T_MULTI_GET;
  CachedIteratorNode *cached_node = nullptr;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node);
  ASSERT_TRUE(cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node->iter_);
  ASSERT_TRUE(nullptr != cached_node->stmt_iter_pool_);

  ObArenaAllocator *iter_alloc = cached_node->get_iter_allocator();
  void *buf = iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  ObTestQueryRowIterator *merge = new (buf) ObTestQueryRowIterator(type);
  cached_node->set_iter(merge);
  iter_pool.release(cached_node);

  CachedIteratorNode *cached_node1 = nullptr;
  ret = iter_pool.get(type, cached_node1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node1);
  ASSERT_TRUE(cached_node1->is_occupied_);
  ASSERT_TRUE(nullptr != cached_node1->iter_);
  ASSERT_TRUE(nullptr != cached_node1->stmt_iter_pool_);
  ASSERT_TRUE(merge == cached_node1->iter_);

  ObArenaAllocator *iter_alloc1 = cached_node1->get_iter_allocator();
  ASSERT_TRUE(iter_alloc == iter_alloc1);
  ASSERT_TRUE(iter_alloc1->total() > 0);
  buf = iter_alloc1->alloc(ObGlobalIteratorPool::get_iter_type_mem_limit(type) + 1);
  ASSERT_TRUE(nullptr != buf);
  ASSERT_TRUE(iter_alloc1->total() > ObGlobalIteratorPool::get_iter_type_mem_limit(type));
  iter_pool.release(cached_node1);

  CachedIteratorNode *cached_node2 = nullptr;
  ret = iter_pool.get(type, cached_node2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node2);
  ASSERT_TRUE(cached_node2->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node2->iter_);
  ASSERT_TRUE(nullptr != cached_node2->stmt_iter_pool_);
  ObArenaAllocator *iter_alloc2 = cached_node2->get_iter_allocator();
  ASSERT_TRUE(iter_alloc == iter_alloc2);
  ASSERT_TRUE(iter_alloc2->total() < ObGlobalIteratorPool::get_iter_type_mem_limit(type));

  buf = iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  merge = new (buf) ObTestQueryRowIterator(type);
  cached_node2->set_iter(merge);
  cached_node2->set_exception_occur(true);
  iter_pool.release(cached_node2);

  CachedIteratorNode *cached_node3 = nullptr;
  ret = iter_pool.get(type, cached_node3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node3);
  ASSERT_TRUE(cached_node3->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node3->iter_);
  ASSERT_TRUE(nullptr != cached_node3->stmt_iter_pool_);
  ObArenaAllocator *iter_alloc3 = cached_node3->get_iter_allocator();
  ASSERT_TRUE(iter_alloc == iter_alloc3);
  ASSERT_TRUE(iter_alloc3->total() < ObGlobalIteratorPool::get_iter_type_mem_limit(type));
}

TEST_F(ObGlobalIteratorPoolTest, destroy)
{
  int ret = 0;
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = ObGlobalIteratorPool::ITER_POOL_TENANT_MIN_MEM_THRESHOLD + 1;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);

  ObQRIterType type = T_SINGLE_SCAN;
  CachedIteratorNode *cached_node = nullptr;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node);
  ASSERT_TRUE(cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node->iter_);
  ASSERT_TRUE(nullptr != cached_node->stmt_iter_pool_);

  ObArenaAllocator *iter_alloc = cached_node->get_iter_allocator();
  void *buf = iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  ObTestQueryRowIterator *merge = new (buf) ObTestQueryRowIterator(type);
  cached_node->set_iter(merge);
  iter_pool.release(cached_node);

  iter_pool.destroy();
  ASSERT_EQ(OB_INVALID_TENANT_ID, iter_pool.tenant_id_);
  ASSERT_EQ(0, iter_pool.bucket_cnt_);
  for (int64_t i = 0; i <= ObGlobalIteratorPool::ITER_POOL_MAX_CACHED_ITER_TYPE; ++i) {
    CachedIteratorNode *nodes = iter_pool.cached_node_array_[i];
    ASSERT_TRUE(nullptr == nodes);
  }
}

TEST_F(ObGlobalIteratorPoolTest, wash)
{
  int ret = 0;
  const int64_t tenant_mem_limit = lib::get_tenant_memory_limit(tenant_id_);
  const int64_t tenant_mem_hold = lib::get_tenant_memory_hold(tenant_id_);
  ObGlobalIteratorPool iter_pool;
  iter_pool.tenant_mem_user_limit_ = tenant_mem_limit;
  ret = iter_pool.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ObQRIterType type = T_SINGLE_SCAN;
  CachedIteratorNode *cached_node = nullptr;
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr != cached_node);
  ASSERT_TRUE(cached_node->is_occupied_);
  ASSERT_TRUE(nullptr == cached_node->iter_);

  ObArenaAllocator *iter_alloc = cached_node->get_iter_allocator();
  void *buf = iter_alloc->alloc(sizeof(ObTestQueryRowIterator));
  ASSERT_TRUE(nullptr != buf);
  ObTestQueryRowIterator *merge = new (buf) ObTestQueryRowIterator(type);
  cached_node->set_iter(merge);
  buf = iter_alloc->alloc(256 * 1024);
  ASSERT_TRUE(nullptr != buf);

  ASSERT_EQ(tenant_mem_limit, iter_pool.tenant_mem_user_limit_);

  ASSERT_TRUE(tenant_mem_limit > tenant_mem_hold) << "tenant_mem_hold=" << tenant_mem_hold
                                                  << "tenant_mem_hold=" << tenant_mem_hold;
  const int64_t test_tenant_mem_limit_low = tenant_mem_hold * 100 / ObGlobalIteratorPool::ITER_POOL_WASH_HIGH_THRESHOLD;

  set_tenant_memory_limit(tenant_id_, test_tenant_mem_limit_low - 10);
  iter_pool.wash();
  STORAGE_LOG(INFO, "after wash", K(iter_pool), K(test_tenant_mem_limit_low));
  ASSERT_FALSE(iter_pool.is_washing_);
  ASSERT_TRUE(iter_pool.is_disabled_);

  iter_pool.release(cached_node);
  ret = iter_pool.get(type, cached_node);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_TRUE(nullptr == cached_node);
  STORAGE_LOG(INFO, "after release", K(iter_pool));

  set_tenant_memory_limit(tenant_id_, tenant_mem_limit);
  iter_pool.wash();
  STORAGE_LOG(INFO, "after wash", K(iter_pool), K(test_tenant_mem_limit_low));
  ASSERT_FALSE(iter_pool.is_washing_);
  ASSERT_FALSE(iter_pool.is_disabled_);

  set_tenant_memory_limit(tenant_id_, tenant_mem_hold + 10);
  iter_pool.wash();
  STORAGE_LOG(INFO, "after wash", K(iter_pool), K(test_tenant_mem_limit_low));
  ASSERT_FALSE(iter_pool.is_washing_);
  ASSERT_TRUE(iter_pool.is_disabled_);
  ASSERT_EQ(tenant_mem_hold + 10, iter_pool.tenant_mem_user_limit_);

  set_tenant_memory_limit(tenant_id_, tenant_mem_limit);
  iter_pool.wash();
  STORAGE_LOG(INFO, "after wash", K(iter_pool), K(test_tenant_mem_limit_low));
  ASSERT_FALSE(iter_pool.is_washing_);
  ASSERT_FALSE(iter_pool.is_disabled_);
  ASSERT_EQ(tenant_mem_limit, iter_pool.tenant_mem_user_limit_);
}

}

int main(int argc, char **argv)
{
  system("rm -f test_global_iterator_pool.log*");
  OB_LOGGER.set_file_name("test_global_iterator_pool.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
