/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "lib/rc/context.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "common/ob_clock_generator.h"
#undef private
#undef protected

namespace oceanbase {
namespace share {

static const uint64_t TEST_TENANT_ID = 1001;
static const int64_t TEST_DIM = 8;
static const int64_t TEST_BUCKET_NUM = 8192;
// mark_gc_erasable 用 delet_scn=1000；传一个远大于它的保留快照，等价于
// "删除版本对所有活跃读已不可见"，从而保持这些纯 ref/state 用例的原有语义。
static const int64_t TEST_MIN_RESERVED_SNAPSHOT = INT64_MAX;

// ==================== Fixture ====================

class TestIvfCentroidGC : public ::testing::Test
{
public:
  TestIvfCentroidGC()
    : allocator_(ObMemAttr(TEST_TENANT_ID, "GcTest")),
      vsag_use_mem_(0),
      sub_ctx_(nullptr),
      cache_(nullptr)
  {}

  void SetUp() override
  {
    lib::ContextParam param;
    param.set_mem_attr(ObMemAttr(TEST_TENANT_ID, "GcTstSubCtx"))
         .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
         .set_properties(lib::ALLOC_THREAD_SAFE
                         | lib::RETURN_MALLOC_DEFAULT
                         | lib::ADD_CHILD_THREAD_SAFE);
    int ret = ROOT_CONTEXT->CREATE_CONTEXT(mem_context_, param);
    ASSERT_EQ(common::OB_SUCCESS, ret);

    void *buf = allocator_.alloc(sizeof(ObIvfMemContext));
    ASSERT_NE(nullptr, buf);
    sub_ctx_ = new (buf) ObIvfMemContext(&vsag_use_mem_);
    sub_ctx_->mem_context_ = mem_context_;
    sub_ctx_->memory_context_ = mem_context_;
    sub_ctx_->check_cnt_ = 0;

    void *cache_buf = allocator_.alloc(sizeof(ObIvfMapCentCache));
    ASSERT_NE(nullptr, cache_buf);
    cache_ = new (cache_buf) ObIvfMapCentCache(allocator_, TEST_TENANT_ID);
    cache_->sub_mem_ctx_ = sub_ctx_;
    cache_->cent_vec_dim_ = TEST_DIM;
    cache_->is_inited_ = true;
    cache_->key_ = IvfCacheKey(IvfCacheType::IVF_MAP_CENTROID_CACHE);
    cache_->node_allocer_.set_mem_ctx(sub_ctx_);
    cache_->bucket_allocer_.set_mem_ctx(sub_ctx_);

    ret = cache_->cent_map_.create(TEST_BUCKET_NUM,
                                   &cache_->node_allocer_,
                                   &cache_->bucket_allocer_);
    ASSERT_EQ(common::OB_SUCCESS, ret);
    sub_ctx_->check_cnt_ = 0;

    MEMSET(dummy_vec_, 0, sizeof(dummy_vec_));
  }

  void TearDown() override
  {
    // ROOT_CONTEXT cleans up at process exit — same pattern as
    // test_ivf_map_cent_cache_ref_cnt.cpp / test_ivf_cache_eviction.cpp.
    if (OB_NOT_NULL(cache_)) {
      cache_->~ObIvfMapCentCache();
      cache_ = nullptr;
    }
  }

  void insert_centroid(int64_t cid)
  {
    SCN min_scn = SCN::min_scn();
    int ret = cache_->add_centroid(cid, dummy_vec_, TEST_DIM, &min_scn);
    ASSERT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
    sub_ctx_->check_cnt_ = 0;
  }

  void set_state(int64_t cid, ObIvfCentroidState state)
  {
    ObCentroidInfo info;
    info.state_ = state;
    info.posting_list_count_ = 0;
    int ret = cache_->set_center_info(cid, info);
    ASSERT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
  }

  void set_del_scn(int64_t cid, const SCN &del_scn)
  {
    int ret = cache_->set_centroid_del_scn(cid, del_scn);
    ASSERT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
  }

  void mark_gc_erasable(int64_t cid)
  {
    SCN del_scn = SCN::min_scn();
    del_scn.convert_for_tx(1000);
    set_del_scn(cid, del_scn);
  }

  void clear_del_scn(int64_t cid)
  {
    set_del_scn(cid, SCN::max_scn());
  }

  // Insert a centroid in the given state. ref_cnt_ stays 0 (set_center_info
  // does not touch ref_cnt_; matches the production add_centroid path).
  void insert_with_state(int64_t cid, ObIvfCentroidState state)
  {
    insert_centroid(cid);
    if (state != ObIvfCentroidState::NORMAL) {
      set_state(cid, state);
    }
  }

  void insert_gc_erasable(int64_t cid, ObIvfCentroidState state)
  {
    insert_with_state(cid, state);
    mark_gc_erasable(cid);
  }

protected:
  ObArenaAllocator allocator_;
  uint64_t vsag_use_mem_;
  lib::MemoryContext mem_context_;
  ObIvfMemContext *sub_ctx_;
  ObIvfMapCentCache *cache_;
  float dummy_vec_[TEST_DIM];

private:
  DISALLOW_COPY_AND_ASSIGN(TestIvfCentroidGC);
};

// ==================== Test cases ====================

// Case 1: mixed states; collect_terminal_cids returns only logically-deleted entries with ref=0.
TEST_F(TestIvfCentroidGC, case1_collect_terminal_basic)
{
  // 30 NORMAL + 20 SPLITTING + 25 SPLIT_DONE + 25 MERGED, all ref=0
  for (int64_t i = 0; i < 30; ++i)  insert_with_state(i,         ObIvfCentroidState::NORMAL);
  for (int64_t i = 30; i < 50; ++i) insert_with_state(i,         ObIvfCentroidState::SPLITTING);
  for (int64_t i = 50; i < 75; ++i) insert_gc_erasable(i,         ObIvfCentroidState::SPLIT_DONE);
  for (int64_t i = 75; i < 100; ++i) insert_gc_erasable(i,        ObIvfCentroidState::MERGED);

  ObSEArray<int64_t, 128> cids;
  cids.set_attr(ObMemAttr(TEST_TENANT_ID, "GcTstCids"));
  int ret = cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_EQ(50, cids.count());

  // Verify every collected cid is in [50, 100).
  for (int64_t i = 0; i < cids.count(); ++i) {
    int64_t cid = cids.at(i);
    EXPECT_GE(cid, 50);
    EXPECT_LT(cid, 100);
  }
}

// Case 2: terminal entries with ref_cnt_ > 0 are skipped by collect.
TEST_F(TestIvfCentroidGC, case2_collect_skips_acquired)
{
  const int64_t N = 20;
  // Insert as NORMAL (visible to acquire); first 5 will be acquired, then all
  // flipped to SPLIT_DONE so collect can see them as terminal.
  for (int64_t i = 0; i < N; ++i) {
    insert_with_state(i, ObIvfCentroidState::NORMAL);
  }
  // Acquire first 5 cids → their ref_cnt_ becomes 1.
  ObCentroidVecGuard guards[5];
  ObCentroidInfo infos[5];
  for (int64_t i = 0; i < 5; ++i) {
    int ret = cache_->acquire_centroid_vec(i, SCN::min_scn(), guards[i], infos[i]);
    ASSERT_EQ(common::OB_SUCCESS, ret) << "cid=" << i;
    ASSERT_TRUE(guards[i].is_held());
  }
  // Now flip every cid to SPLIT_DONE and mark logically deleted; first 5 retain ref_cnt_=1.
  for (int64_t i = 0; i < N; ++i) {
    set_state(i, ObIvfCentroidState::SPLIT_DONE);
    mark_gc_erasable(i);
  }

  ObSEArray<int64_t, 32> cids;
  cids.set_attr(ObMemAttr(TEST_TENANT_ID, "GcTstCids"));
  int ret = cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_EQ(N - 5, cids.count());
  for (int64_t i = 0; i < cids.count(); ++i) {
    EXPECT_GE(cids.at(i), 5);
  }
  // Even if we tried to try_erase the acquired cids, it must refuse.
  for (int64_t i = 0; i < 5; ++i) {
    bool erased = true;
    int er = cache_->try_erase_centroid(i, TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, er);
    EXPECT_FALSE(erased) << "cid=" << i << " should be blocked by ref_cnt_";
  }
}

// Case 3: collect → try_erase loop drains all terminal entries; non-terminal stays.
TEST_F(TestIvfCentroidGC, case3_collect_then_erase_drains)
{
  // 10 NORMAL + 30 gc-erasable (mixed SPLIT_DONE/MERGED)
  for (int64_t i = 0; i < 10; ++i)  insert_with_state(i, ObIvfCentroidState::NORMAL);
  for (int64_t i = 10; i < 25; ++i) insert_gc_erasable(i, ObIvfCentroidState::SPLIT_DONE);
  for (int64_t i = 25; i < 40; ++i) insert_gc_erasable(i, ObIvfCentroidState::MERGED);
  ASSERT_EQ(40, cache_->count_);

  ObSEArray<int64_t, 64> cids;
  cids.set_attr(ObMemAttr(TEST_TENANT_ID, "GcTstCids"));
  ASSERT_EQ(common::OB_SUCCESS, cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids));
  ASSERT_EQ(30, cids.count());

  int64_t erased_total = 0;
  for (int64_t i = 0; i < cids.count(); ++i) {
    bool erased = false;
    int er = cache_->try_erase_centroid(cids.at(i), TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, er);
    if (erased) ++erased_total;
  }
  EXPECT_EQ(30, erased_total);
  EXPECT_EQ(10, cache_->count_);

  // NORMAL entries untouched.
  for (int64_t i = 0; i < 10; ++i) {
    bool erased = true;
    int er = cache_->try_erase_centroid(i, TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, er);
    EXPECT_FALSE(erased);
  }
}

// Case 4: del_scn drift — collect captures gc-erasable cid; before try_erase half
// have del_scn cleared → try_erase no-ops on those.
TEST_F(TestIvfCentroidGC, case4_state_drift_safe)
{
  const int64_t N = 8;
  for (int64_t i = 0; i < N; ++i) {
    insert_gc_erasable(i, ObIvfCentroidState::SPLIT_DONE);
  }
  ObSEArray<int64_t, 16> cids;
  cids.set_attr(ObMemAttr(TEST_TENANT_ID, "GcTstCids"));
  ASSERT_EQ(common::OB_SUCCESS, cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids));
  ASSERT_EQ(N, cids.count());

  // Clear del_scn on half of them between collect and try_erase.
  for (int64_t i = 0; i < N / 2; ++i) {
    clear_del_scn(cids.at(i));
  }
  int64_t erased_total = 0;
  for (int64_t i = 0; i < cids.count(); ++i) {
    bool erased = false;
    int er = cache_->try_erase_centroid(cids.at(i), TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, er);
    if (erased) ++erased_total;
  }
  EXPECT_EQ(N / 2, erased_total);
  EXPECT_EQ(N / 2, cache_->count_);
}

// Case 5: empty cent_map_ → collect returns 0, no error.
TEST_F(TestIvfCentroidGC, case5_empty_map)
{
  ObSEArray<int64_t, 4> cids;
  cids.set_attr(ObMemAttr(TEST_TENANT_ID, "GcTstCids"));
  int ret = cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_EQ(0, cids.count());
}

// Case 6: throttle simulation — 5000 terminal cids cleaned in batches of 1024
// (matches MAX_ERASE_PER_MGR in scheduler). Multiple rounds converge to 0.
TEST_F(TestIvfCentroidGC, case6_throttle_multi_round)
{
  const int64_t TOTAL = 5000;
  const int64_t MAX_ERASE_PER_ROUND = 1024;
  for (int64_t i = 0; i < TOTAL; ++i) {
    insert_gc_erasable(i, ObIvfCentroidState::SPLIT_DONE);
  }
  ASSERT_EQ(TOTAL, cache_->count_);

  int64_t round = 0;
  int64_t remaining = TOTAL;
  while (remaining > 0) {
    ObSEArray<int64_t, 1024> cids;
    cids.set_attr(ObMemAttr(TEST_TENANT_ID, "GcTstCids"));
    ASSERT_EQ(common::OB_SUCCESS, cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids));
    ASSERT_EQ(remaining, cids.count());
    const int64_t batch = std::min<int64_t>(cids.count(), MAX_ERASE_PER_ROUND);
    int64_t erased_this_round = 0;
    for (int64_t i = 0; i < batch; ++i) {
      bool erased = false;
      int er = cache_->try_erase_centroid(cids.at(i), TEST_MIN_RESERVED_SNAPSHOT, erased);
      ASSERT_EQ(common::OB_SUCCESS, er);
      if (erased) ++erased_this_round;
    }
    EXPECT_EQ(batch, erased_this_round);
    remaining -= erased_this_round;
    ++round;
    ASSERT_LE(round, 6) << "should drain in <= ceil(5000/1024)=5 rounds";
  }
  EXPECT_EQ(0, cache_->count_);
  EXPECT_EQ(5, round);  // 4 * 1024 + 904
}

// Case 7: erase nonexistent cid mid-loop → OB_ENTRY_NOT_EXIST, not fatal.
TEST_F(TestIvfCentroidGC, case7_erase_missing_cid_returns_not_exist)
{
  insert_gc_erasable(1, ObIvfCentroidState::SPLIT_DONE);
  bool erased = false;
  int er = cache_->try_erase_centroid(1, TEST_MIN_RESERVED_SNAPSHOT, erased);
  EXPECT_EQ(common::OB_SUCCESS, er);
  EXPECT_TRUE(erased);

  // Second try_erase of the same cid (now gone) returns OB_ENTRY_NOT_EXIST.
  erased = true;
  er = cache_->try_erase_centroid(1, TEST_MIN_RESERVED_SNAPSHOT, erased);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, er);
  EXPECT_FALSE(erased);
}

// Case 8: entries without del_scn are not gc-erasable regardless of state.
// collect_terminal_cids must skip them; try_erase must refuse them.
TEST_F(TestIvfCentroidGC, case8_non_terminal_states_safe)
{
  // Insert MERGING (3) and REASSIGNING (5) centroids without logical delete.
  for (int64_t i = 0; i < 4; ++i) {
    insert_with_state(i,     ObIvfCentroidState::MERGING);
    insert_with_state(i + 4, ObIvfCentroidState::REASSIGNING);
  }
  ASSERT_EQ(8, cache_->count_);

  // collect must return 0: del_scn not set.
  ObSEArray<int64_t, 16> cids;
  cids.set_attr(ObMemAttr(TEST_TENANT_ID, "GcTstCids"));
  int ret = cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_EQ(0, cids.count()) << "entries without del_scn must not be collected";

  // Direct try_erase must also refuse (is_gc_erasable returns false).
  for (int64_t i = 0; i < 4; ++i) {
    bool erased = true;
    int er = cache_->try_erase_centroid(i, TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, er);
    EXPECT_FALSE(erased) << "MERGING cid=" << i << " must not be erased";
  }
  for (int64_t i = 4; i < 8; ++i) {
    bool erased = true;
    int er = cache_->try_erase_centroid(i, TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, er);
    EXPECT_FALSE(erased) << "REASSIGNING cid=" << i << " must not be erased";
  }
  // All 8 entries must still exist.
  EXPECT_EQ(8, cache_->count_);
}

// Case 9: mgr-level collect_terminal_cids_from_map_caches.
// Same mgr contains IVF_MAP_CENTROID_CACHE (with terminal cids) and
// IVF_CENTROID_CACHE (should be skipped). Verifies type-based filtering.
TEST_F(TestIvfCentroidGC, case9_mgr_collect_and_skip)
{
  // cache_ is the IVF_MAP_CENTROID_CACHE; insert 6 gc-erasable + 4 NORMAL.
  for (int64_t i = 0; i < 4; ++i)  insert_with_state(i,     ObIvfCentroidState::NORMAL);
  for (int64_t i = 4; i < 8; ++i)  insert_gc_erasable(i,     ObIvfCentroidState::SPLIT_DONE);
  for (int64_t i = 8; i < 10; ++i) insert_gc_erasable(i,     ObIvfCentroidState::MERGED);
  // 6 gc-erasable entries (cid 4..9), 4 NORMAL (cid 0..3).

  // Build a minimal ObIvfCacheMgr with #define private public access.
  // We only need cache_objs_ populated; avoid calling init() which needs MTL.
  void *mgr_buf = allocator_.alloc(sizeof(ObIvfCacheMgr));
  ASSERT_NE(nullptr, mgr_buf);
  ObIvfCacheMgr *mgr = new (mgr_buf) ObIvfCacheMgr(allocator_, TEST_TENANT_ID);

  // create cache_objs_ hashmap with tenant-aware attr.
  int ret = mgr->cache_objs_.create(16,
                                    ObMemAttr(TEST_TENANT_ID, "GcMgrTstBkt"),
                                    ObMemAttr(TEST_TENANT_ID, "GcMgrTstNod"));
  ASSERT_EQ(common::OB_SUCCESS, ret);

  // Register cache_ as IVF_MAP_CENTROID_CACHE.
  IvfCacheKey map_key(IvfCacheType::IVF_MAP_CENTROID_CACHE);
  ret = mgr->cache_objs_.set_refactored(map_key, cache_);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  // Register a dummy non-null pointer as IVF_CENTROID_CACHE.
  // Its content does not matter; the mgr must skip it by key type.
  // We reuse cache_ pointer just to have a non-null value; the mgr only
  // checks the key type and will NOT call collect on it.
  IvfCacheKey arr_key(IvfCacheType::IVF_CENTROID_CACHE);
  ret = mgr->cache_objs_.set_refactored(arr_key, cache_);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  // collect_terminal_cids_from_map_caches must return exactly 6 (map cache only).
  ObSEArray<int64_t, 16> cids;
  cids.set_attr(ObMemAttr(TEST_TENANT_ID, "GcMgrCids"));
  ret = mgr->collect_terminal_cids_from_map_caches(TEST_MIN_RESERVED_SNAPSHOT, cids);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_EQ(6, cids.count())
      << "Only IVF_MAP_CENTROID_CACHE terminal cids should be collected";

  // Verify every cid is in [4, 10) (the terminal range).
  for (int64_t i = 0; i < cids.count(); ++i) {
    EXPECT_GE(cids.at(i), 4);
    EXPECT_LT(cids.at(i), 10);
  }

  // Cleanup mgr (do NOT destroy cache_, it is owned by fixture TearDown).
  mgr->cache_objs_.destroy();
  mgr->~ObIvfCacheMgr();
}

}  // namespace share
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObClockGenerator::init();
  oceanbase::lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(
      oceanbase::share::TEST_TENANT_ID);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
