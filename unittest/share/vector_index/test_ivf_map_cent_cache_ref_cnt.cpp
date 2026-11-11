/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <pthread.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include <vector>
#define private public
#define protected public
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/vector_index/disk_bbq_cache/ob_ivf_sstable_cache_controller.h"
#include "lib/rc/context.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "common/ob_clock_generator.h"
#undef private
#undef protected

namespace oceanbase {
namespace share {

static const uint64_t TEST_TENANT_ID = 1001;
static const int64_t TEST_DIM = 8;
static const int64_t TEST_BUCKET_NUM = 128;
// 这些用例聚焦 ref_cnt_/state 并发安全。mark_gc_erasable 用 delet_scn=1000；
// 传一个远大于它的保留快照，满足 is_gc_erasable 的 snapshot 前置条件，
// 从而只验证 delet_scn_/ref_cnt_ 语义，不测 snapshot 边界本身。
static const int64_t TEST_MIN_RESERVED_SNAPSHOT = INT64_MAX;

static storage::ObITable::TableKey make_ivf_sstable_key(
    const uint64_t tablet_id, const int64_t snapshot_version)
{
  storage::ObITable::TableKey table_key;
  table_key.table_type_ = storage::ObITable::TableType::IVF_INDEX_SSTABLE;
  table_key.tablet_id_ = tablet_id;
  table_key.version_range_.base_version_ = 0;
  table_key.version_range_.snapshot_version_ = snapshot_version;
  return table_key;
}

TEST(TestIvfSSTableCacheKey, full_table_key_is_the_cache_identity)
{
  const storage::ObITable::TableKey first_table = make_ivf_sstable_key(1001, 10);
  const storage::ObITable::TableKey same_table = make_ivf_sstable_key(1001, 10);
  const storage::ObITable::TableKey next_table = make_ivf_sstable_key(1001, 20);
  const ObIvfCacheMgrKey first(first_table);
  const ObIvfCacheMgrKey same(same_table);
  const ObIvfCacheMgrKey next(next_table);
  const ObIvfCacheMgrKey tablet_only(first_table.get_tablet_id());

  ASSERT_TRUE(first.is_valid());
  ASSERT_TRUE(first.is_sstable_key());
  EXPECT_EQ(first_table.get_tablet_id(), first.get_tablet_id());
  EXPECT_EQ(first, same);
  EXPECT_EQ(first.hash(), same.hash());
  EXPECT_FALSE(first == next);
  EXPECT_FALSE(first == tablet_only);
}

TEST(TestIvfSSTableCacheController, high_low_watermark_has_hysteresis)
{
  ObIvfSSTableCacheController controller;
  uint64_t vector_used = 0;
  controller.is_inited_ = true;
  controller.tenant_id_ = TEST_TENANT_ID;
  controller.all_vector_used_ = &vector_used;
  ATOMIC_STORE(&controller.vector_mem_limit_, 1000);
  ATOMIC_STORE(&controller.mem_limit_, 800);
  ATOMIC_STORE(&controller.mem_high_, 720);
  ATOMIC_STORE(&controller.mem_low_, 400);
  ATOMIC_STORE(&controller.admission_closed_, false);

  ATOMIC_STORE(&controller.build_reserved_, 720);
  controller.update_admission_state_();
  EXPECT_TRUE(controller.is_admission_closed());
  EXPECT_FALSE(controller.has_eviction_pending());
  controller.release_reservation(720);
  EXPECT_FALSE(controller.is_admission_closed());

  controller.on_cache_allocated(720);
  EXPECT_EQ(720, controller.get_resident_hold());
  EXPECT_TRUE(controller.has_eviction_pending());
  EXPECT_TRUE(controller.is_admission_closed());
  EXPECT_TRUE(controller.try_start_eviction());

  controller.on_cache_released(319);
  controller.finish_eviction();
  EXPECT_EQ(401, controller.get_resident_hold());
  EXPECT_TRUE(controller.has_eviction_pending());
  EXPECT_TRUE(controller.is_admission_closed());

  ASSERT_TRUE(controller.try_start_eviction());
  controller.on_cache_released(1);
  controller.finish_eviction();
  EXPECT_EQ(400, controller.get_resident_hold());
  EXPECT_FALSE(controller.has_eviction_pending());
  EXPECT_FALSE(controller.is_admission_closed());
}

TEST(TestIvfSSTableCacheController, reservation_conversion_preserves_other_builders_budget)
{
  ObIvfSSTableCacheController controller;
  uint64_t vector_used = 0;
  controller.is_inited_ = true;
  controller.tenant_id_ = TEST_TENANT_ID;
  controller.all_vector_used_ = &vector_used;
  ATOMIC_STORE(&controller.vector_mem_limit_, 4000);
  ATOMIC_STORE(&controller.mem_limit_, 2000);
  ATOMIC_STORE(&controller.mem_high_, 1800);
  ATOMIC_STORE(&controller.mem_low_, 1000);
  ATOMIC_STORE(&controller.build_reserved_, 1200); // This builder 1000, another builder 200.
  controller.update_admission_state_();
  EXPECT_FALSE(controller.is_admission_closed());

  controller.update_cache_hold(0, 800, 1000);
  EXPECT_EQ(800, controller.get_resident_hold());
  EXPECT_EQ(200, controller.get_reserved_hold());
  EXPECT_FALSE(controller.is_admission_closed());
  EXPECT_FALSE(controller.has_eviction_pending());

  controller.release_reservation(200);
  EXPECT_EQ(800, controller.get_resident_hold());
  EXPECT_EQ(0, controller.get_reserved_hold());
  controller.update_cache_hold(800, 1850);
  EXPECT_TRUE(controller.has_eviction_pending()); // Actual memory reaches the high watermark.
  EXPECT_TRUE(controller.is_admission_closed());
  controller.on_cache_released(1850);
  EXPECT_EQ(0, controller.get_resident_hold());
  EXPECT_FALSE(controller.is_admission_closed());
}

TEST(TestIvfSSTableCentCache, owns_lru_and_memory_accounting_state)
{
  ObArenaAllocator allocator("IvfSSTCacheUT");
  ObIvfSSTableCacheController controller;
  uint64_t reservation = 2048;
  ATOMIC_STORE(&controller.build_reserved_, reservation);
  {
    ObIvfSSTableCentCache cache(allocator, TEST_TENANT_ID);
    EXPECT_EQ(0, cache.get_last_access_ts());
    EXPECT_EQ(0, cache.get_registered_hold());
    EXPECT_FALSE(cache.is_memory_registered());
    EXPECT_FALSE(cache.is_hgraph_building());
    cache.touch();
    EXPECT_GT(cache.get_last_access_ts(), 0);
    EXPECT_TRUE(cache.register_memory_once(1024, &controller, &reservation));
    EXPECT_EQ(0, reservation);
    EXPECT_EQ(0, controller.get_reserved_hold());
    EXPECT_FALSE(cache.register_memory_once(2048, &controller));
    EXPECT_TRUE(cache.is_memory_registered());
    EXPECT_EQ(1024, cache.get_registered_hold());
    EXPECT_EQ(1024, controller.get_resident_hold());
    EXPECT_TRUE(cache.try_start_hgraph_build());
    EXPECT_FALSE(cache.try_start_hgraph_build());
    cache.finish_hgraph_build();
    EXPECT_FALSE(cache.is_hgraph_building());
  }
  EXPECT_EQ(0, controller.get_resident_hold());
}

TEST(TestIvfSSTableCentCache, iterator_stats_keep_load_time_and_monotonic_access_time)
{
  ObArenaAllocator allocator("IvfSSTCacheUT");
  ObIvfSSTableCentCache cache(allocator, TEST_TENANT_ID);
  EXPECT_EQ(0, cache.get_hit_count());
  EXPECT_EQ(0, cache.get_first_load_ts());
  // Completion through the common cache interface must also publish the load time.
  ObIvfICache &base = cache;
  base.set_completed();
  const int64_t first_load = cache.get_first_load_ts();
  EXPECT_GT(first_load, 0);
  EXPECT_EQ(0, cache.get_last_access_ts());
  cache.record_iterator_access(false); // The builder used its new cache after a miss.
  EXPECT_EQ(0, cache.get_hit_count());
  EXPECT_GE(cache.get_last_access_ts(), first_load);
  std::vector<std::thread> workers;
  for (int64_t i = 0; i < 4; ++i) {
    workers.emplace_back([&cache] {
      for (int64_t j = 0; j < 1000; ++j) { cache.record_iterator_access(true); }
    });
  }
  for (auto &worker : workers) { worker.join(); }
  EXPECT_EQ(4000, cache.get_hit_count());
  const int64_t access_time = cache.get_last_access_ts();
  base.set_completed();
  EXPECT_EQ(first_load, cache.get_first_load_ts());
  EXPECT_EQ(access_time, cache.get_last_access_ts());
  EXPECT_EQ(4000, cache.get_hit_count());
  // A delayed timestamp update (or a backward clock adjustment) cannot age the cache.
  const int64_t future = ObTimeUtility::current_time() + 1000000;
  ATOMIC_STORE(&cache.last_access_ts_, future);
  cache.touch();
  EXPECT_EQ(future, cache.get_last_access_ts());
}

// ==================== Read functors (no lambda allowed) ====================

class ReadRefCntFn
{
public:
  explicit ReadRefCntFn(int32_t &out) : out_(out) {}
  int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    out_ = entry.second.ref_cnt_;
    return common::OB_SUCCESS;
  }
private:
  int32_t &out_;
};

class ReadStateFn
{
public:
  explicit ReadStateFn(ObIvfCentroidState &out) : out_(out) {}
  int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    out_ = entry.second.state_;
    return common::OB_SUCCESS;
  }
private:
  ObIvfCentroidState &out_;
};

class ReadCidVecPtrFn
{
public:
  explicit ReadCidVecPtrFn(float *&out) : out_(out) {}
  int operator()(const common::hash::HashMapPair<int64_t, ObCentroidInfo> &entry)
  {
    out_ = entry.second.cid_vec_;
    return common::OB_SUCCESS;
  }
private:
  float *&out_;
};

// ==================== Test fixture ====================

class TestIvfMapCentCacheRefCnt : public ::testing::Test
{
public:
  TestIvfMapCentCacheRefCnt()
    : allocator_(ObMemAttr(TEST_TENANT_ID, "RefCntTest")),
      vsag_use_mem_(0),
      sub_ctx_(nullptr),
      cache_(nullptr)
  {}

  void SetUp() override
  {
    // 1) Build a working ObIvfMemContext via direct field init (avoid MTL deps)
    lib::ContextParam param;
    param.set_mem_attr(ObMemAttr(TEST_TENANT_ID, "TstSubCtx"))
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

    // 2) Build the cache and create cent_map_
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

    // dummy vec used for all add_centroid calls (data content irrelevant)
    MEMSET(dummy_vec_, 0, sizeof(dummy_vec_));
  }

  void TearDown() override
  {
    // Tear down cent_map_ which owns sub_mem_ctx_-allocated cid_vec_.
    // The underlying lib::MemoryContext (mem_context_) is rooted at ROOT_CONTEXT
    // and will be cleaned up at process exit; matches the pattern in
    // test_ivf_cache_eviction.cpp.
    if (OB_NOT_NULL(cache_)) {
      cache_->~ObIvfMapCentCache();
      cache_ = nullptr;
    }
  }

  // Insert a centroid with create_scn=MIN so it is visible at any read_scn.
  void insert_centroid(int64_t cid)
  {
    SCN min_scn = SCN::min_scn();
    int ret = cache_->add_centroid(cid, dummy_vec_, TEST_DIM, &min_scn);
    ASSERT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
    sub_ctx_->check_cnt_ = 0;
  }

  // Use set_center_info to flip state_ (and posting_list_count_) without touching
  // create_scn_/delet_scn_/cid_vec_/ref_cnt_.
  void set_state(int64_t cid, ObIvfCentroidState state)
  {
    ObCentroidInfo info;
    info.state_ = state;
    info.posting_list_count_ = 0;
    int ret = cache_->set_center_info(cid, info);
    ASSERT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
  }

  // try_erase_centroid 要求 delet_scn_ 已设置（逻辑删除）且 ref_cnt_==0。
  // TEST_MIN_RESERVED_SNAPSHOT 远大于 delet_scn，snapshot 检查恒通过。
  void mark_gc_erasable(int64_t cid)
  {
    SCN del_scn = SCN::min_scn();
    del_scn.convert_for_tx(1000);
    int ret = cache_->set_centroid_del_scn(cid, del_scn);
    ASSERT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
  }

  int32_t read_ref_cnt(int64_t cid)
  {
    int32_t rc = -1;
    ReadRefCntFn fn(rc);
    int ret = cache_->cent_map_.read_atomic(cid, fn);
    EXPECT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
    return rc;
  }

  ObIvfCentroidState read_state(int64_t cid)
  {
    ObIvfCentroidState st = ObIvfCentroidState::NORMAL;
    ReadStateFn fn(st);
    int ret = cache_->cent_map_.read_atomic(cid, fn);
    EXPECT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
    return st;
  }

  bool entry_exists(int64_t cid)
  {
    float *p = nullptr;
    ReadCidVecPtrFn fn(p);
    int ret = cache_->cent_map_.read_atomic(cid, fn);
    return common::OB_SUCCESS == ret;
  }

protected:
  ObArenaAllocator allocator_;
  uint64_t vsag_use_mem_;
  lib::MemoryContext mem_context_;
  ObIvfMemContext *sub_ctx_;
  ObIvfMapCentCache *cache_;
  float dummy_vec_[TEST_DIM];

private:
  DISALLOW_COPY_AND_ASSIGN(TestIvfMapCentCacheRefCnt);
};

// ==================== Test cases ====================

// Case A: state=NORMAL → try_erase_centroid 拒绝删除 (erased=false)，cid 仍存在
TEST_F(TestIvfMapCentCacheRefCnt, case_a_try_erase_normal_state_no_op)
{
  const int64_t cid = 42;
  insert_centroid(cid);
  ASSERT_EQ(ObIvfCentroidState::NORMAL, read_state(cid));
  ASSERT_EQ(0, read_ref_cnt(cid));
  const int64_t old_count = cache_->count_;

  bool erased = true;
  int ret = cache_->try_erase_centroid(cid, TEST_MIN_RESERVED_SNAPSHOT, erased);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_FALSE(erased);

  EXPECT_TRUE(entry_exists(cid));
  EXPECT_EQ(ObIvfCentroidState::NORMAL, read_state(cid));
  EXPECT_EQ(old_count, cache_->count_);
}

TEST_F(TestIvfMapCentCacheRefCnt, sstable_array_cache_keeps_dense_centroids)
{
  ObVectorIndexParam param;
  param.type_ = VIAT_DISK_BBQ;
  param.lib_ = VIAL_OB;
  param.dist_algorithm_ = VIDA_L2;
  param.dim_ = TEST_DIM;
  ObIvfSSTableCentCache array_cache(allocator_, TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, array_cache.init(
      sub_ctx_, IvfCacheKey(IVF_SSTABLE_CENTROID_CACHE),
      param, &vsag_use_mem_));
  EXPECT_EQ(nullptr, array_cache.get_centroids());
  EXPECT_EQ(OB_STATE_NOT_MATCH, array_cache.allocate_array(2));
  ASSERT_TRUE(array_cache.set_writing_if_idle());
  EXPECT_FALSE(array_cache.set_writing_if_idle());
  ASSERT_EQ(OB_SUCCESS, array_cache.allocate_array(2));
  ASSERT_NE(nullptr, array_cache.get_centroids());

  float first[TEST_DIM] = {0};
  float second[TEST_DIM] = {0};
  first[0] = 1.0F;
  second[0] = 2.0F;
  ASSERT_EQ(OB_SUCCESS, array_cache.write_centroid_with_real_idx(
      0, first, sizeof(first)));
  ASSERT_EQ(OB_SUCCESS, array_cache.write_centroid_with_real_idx(
      1, second, sizeof(second)));
  array_cache.set_completed();

  const float *centroids = nullptr;
  common::obvsag::VectorIndexPtr hgraph = nullptr;
  int64_t count = 0;
  int64_t dim = 0;
  {
    common::RWLock::RLockGuard guard(array_cache.get_lock());
    centroids = array_cache.get_centroids();
    hgraph = array_cache.get_hgraph_index();
    count = array_cache.get_count();
    dim = array_cache.get_cent_vec_dim();
  }
  ASSERT_NE(nullptr, centroids);
  EXPECT_EQ(nullptr, hgraph);
  EXPECT_EQ(2, count);
  EXPECT_EQ(TEST_DIM, dim);
  EXPECT_FLOAT_EQ(1.0F, centroids[0]);
  EXPECT_FLOAT_EQ(2.0F, centroids[TEST_DIM]);
}

// Case B: 逻辑删除 + ref_cnt_=0 → try_erase 成功，count_-- 且 entry 消失
// (聚焦 ref_cnt 并发安全，snapshot 检查由 TEST_MIN_RESERVED_SNAPSHOT 满足)
TEST_F(TestIvfMapCentCacheRefCnt, case_b_try_erase_split_done_no_ref)
{
  const int64_t cid = 7;
  insert_centroid(cid);
  set_state(cid, ObIvfCentroidState::SPLIT_DONE);
  mark_gc_erasable(cid);
  ASSERT_EQ(0, read_ref_cnt(cid));
  const int64_t old_count = cache_->count_;
  ASSERT_GT(old_count, 0);

  bool erased = false;
  int ret = cache_->try_erase_centroid(cid, TEST_MIN_RESERVED_SNAPSHOT, erased);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_TRUE(erased);

  EXPECT_FALSE(entry_exists(cid));
  EXPECT_EQ(old_count - 1, cache_->count_);
}

// Case C: state=MERGED；acquire 持有期间 try_erase 被阻挡；release 后 try_erase 成功
TEST_F(TestIvfMapCentCacheRefCnt, case_c_try_erase_blocked_by_ref_then_release)
{
  const int64_t cid = 13;
  insert_centroid(cid);  // NORMAL — visible to acquire

  // acquire 单 cid → ref_cnt_ 应为 1（acquire 前必须为 visible 状态）
  {
    ObCentroidVecGuard guard;
    ObCentroidInfo info;
    int ret = cache_->acquire_centroid_vec(cid, SCN::min_scn(), guard, info);
    ASSERT_EQ(common::OB_SUCCESS, ret);
    ASSERT_TRUE(guard.is_held());
    ASSERT_NE(nullptr, info.cid_vec_);
    EXPECT_EQ(1, read_ref_cnt(cid));

    // 持有 ref 的同时切换到终态 MERGED；try_erase 必须被 ref_cnt_ 阻挡
    set_state(cid, ObIvfCentroidState::MERGED);

    // try_erase 必须失败（ref_cnt_ != 0）
    bool erased = true;
    int erase_ret = cache_->try_erase_centroid(cid, TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, erase_ret);
    EXPECT_FALSE(erased);
    EXPECT_TRUE(entry_exists(cid));
    EXPECT_EQ(1, read_ref_cnt(cid));
  }
  // guard 出 scope → release_centroid_vec_ 把 ref_cnt_ 归零
  EXPECT_EQ(0, read_ref_cnt(cid));

  // 逻辑删除 + ref=0 后 try_erase 应成功
  mark_gc_erasable(cid);
  bool erased = false;
  int ret = cache_->try_erase_centroid(cid, TEST_MIN_RESERVED_SNAPSHOT, erased);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_TRUE(erased);
  EXPECT_FALSE(entry_exists(cid));
}

// Case D: try_erase 不存在的 cid → 返回 OB_ENTRY_NOT_EXIST，erased=false
TEST_F(TestIvfMapCentCacheRefCnt, case_d_try_erase_nonexistent_cid)
{
  const int64_t cid = 999;
  ASSERT_FALSE(entry_exists(cid));

  bool erased = true;
  int ret = cache_->try_erase_centroid(cid, TEST_MIN_RESERVED_SNAPSHOT, erased);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, ret);
  EXPECT_FALSE(erased);
}

// Case E: 多个可见 centroid，acquire_visible_centroids 后 batch_guard 析构正确归零所有 ref
TEST_F(TestIvfMapCentCacheRefCnt, case_e_acquire_visible_centroids_guard_release_ref)
{
  const int64_t N = 6;
  for (int64_t i = 0; i < N; ++i) {
    insert_centroid(i);
  }
  ASSERT_EQ(N, cache_->count_);

  {
    ObCentroidVecBatchGuard batch_guard;
    common::ObSEArray<ObCentroidInfo, 8> out;
    out.set_attr(ObMemAttr(TEST_TENANT_ID, "TstAcqOut"));

    int ret = cache_->acquire_visible_centroids(SCN::min_scn(), batch_guard, out);
    EXPECT_EQ(common::OB_SUCCESS, ret);
    EXPECT_EQ(N, out.count());
    EXPECT_EQ(N, batch_guard.count());

    for (int64_t i = 0; i < N; ++i) {
      EXPECT_EQ(1, read_ref_cnt(i)) << "cid=" << i;
    }

    // ref 持有期间，把每个 entry 改成 SPLIT_DONE 后 try_erase 仍应被阻挡
    for (int64_t i = 0; i < N; ++i) {
      set_state(i, ObIvfCentroidState::SPLIT_DONE);
      bool erased = true;
      int erase_ret = cache_->try_erase_centroid(i, TEST_MIN_RESERVED_SNAPSHOT, erased);
      EXPECT_EQ(common::OB_SUCCESS, erase_ret);
      EXPECT_FALSE(erased) << "cid=" << i << " should be blocked by ref_cnt_=1";
    }
  }
  // batch_guard 析构后所有 ref_cnt_ 归零
  for (int64_t i = 0; i < N; ++i) {
    EXPECT_EQ(0, read_ref_cnt(i)) << "cid=" << i;
  }

  // 逻辑删除 + ref=0 后依次 try_erase 应全部成功
  for (int64_t i = 0; i < N; ++i) {
    mark_gc_erasable(i);
    bool erased = false;
    int ret = cache_->try_erase_centroid(i, TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, ret);
    EXPECT_TRUE(erased) << "cid=" << i;
  }
  EXPECT_EQ(0, cache_->count_);
}

// Case F: 单 cid acquire/release 配对；显式调 guard.release()
TEST_F(TestIvfMapCentCacheRefCnt, case_f_acquire_release_pair)
{
  const int64_t cid = 21;
  insert_centroid(cid);

  ObCentroidVecGuard guard;
  ObCentroidInfo info;
  int ret = cache_->acquire_centroid_vec(cid, SCN::min_scn(), guard, info);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  EXPECT_TRUE(guard.is_held());
  EXPECT_EQ(1, read_ref_cnt(cid));

  // 显式 release（idempotent）
  EXPECT_EQ(common::OB_SUCCESS, guard.release());
  EXPECT_FALSE(guard.is_held());
  EXPECT_EQ(0, read_ref_cnt(cid));

  // 再调一次 release noop
  EXPECT_EQ(common::OB_SUCCESS, guard.release());
  EXPECT_EQ(0, read_ref_cnt(cid));
}

// Case H: entry 被标记删除（delet_scn_ < read_scn）后 acquire 应返回 OB_ENTRY_NOT_EXIST，
// 且不修改 ref_cnt_
TEST_F(TestIvfMapCentCacheRefCnt, case_h_acquire_invisible_returns_no_ref)
{
  const int64_t cid = 33;
  insert_centroid(cid);
  ASSERT_EQ(0, read_ref_cnt(cid));

  // 把 delet_scn_ 设为 base_scn (= 1)，read_scn 也用 base_scn → 不可见
  // 因为 is_visible 要求 read_scn < delet_scn 且 delet_scn 不为 max。
  const SCN del_scn = SCN::base_scn();
  int set_ret = cache_->set_centroid_del_scn(cid, del_scn);
  ASSERT_EQ(common::OB_SUCCESS, set_ret);

  ObCentroidVecGuard guard;
  ObCentroidInfo info;
  int ret = cache_->acquire_centroid_vec(cid, del_scn, guard, info);
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, ret);
  EXPECT_FALSE(guard.is_held());
  EXPECT_EQ(0, read_ref_cnt(cid));   // 失败路径绝不增加 ref_cnt_
}

// ==================== case_g: lifecycle race ====================

// Case G: multi-cid lifecycle race. 32 cids start NORMAL; 8 worker threads
// continuously acquire / touch_canary / release on rotating cids; 1 flipper
// thread monotonically transitions every cid through
//   NORMAL → SPLITTING → SPLIT_DONE → REASSIGNING → MERGED
// with small delays; 1 GC thread loops collect_terminal_cids + try_erase.
// After flipper finishes, all threads stop; a final cleanup pass drains any
// remaining terminal cids.
//
// Failure modes this catches:
// - acquire/try_erase race on ref_cnt_: ASan/TSan would flag use-after-free
//   when worker reads info.cid_vec_[0] while ref_cnt_>=1 if try_erase
//   incorrectly freed the vec.
// - state visibility / collect/erase atomicity bug: count_ would not converge
//   to 0 in the final cleanup loop.
TEST_F(TestIvfMapCentCacheRefCnt, case_g_lifecycle_concurrency)
{
  const int64_t N_CIDS = 32;
  const int N_WORKERS = 8;

  for (int64_t i = 0; i < N_CIDS; ++i) {
    insert_centroid(i);  // NORMAL, visible to acquire
  }
  ASSERT_EQ(N_CIDS, cache_->count_);

  std::atomic<bool> stop(false);
  std::atomic<int> ok_acquires(0);
  std::atomic<int> not_exist_acquires(0);

  // Worker: rotate over all cids, acquire, touch cid_vec_[0] (canary read),
  // release via guard dtor on scope exit.
  auto worker_fn = [&](int worker_id) {
    int64_t iter = 0;
    while (!stop.load(std::memory_order_acquire)) {
      const int64_t cid = (worker_id * 13 + iter * 17) % N_CIDS;
      ObCentroidVecGuard guard;
      ObCentroidInfo info;
      int ret = cache_->acquire_centroid_vec(cid, SCN::min_scn(), guard, info);
      if (common::OB_SUCCESS == ret) {
        // While ref_cnt_>=1, the vec must stay alive. ASan/TSan catch any UAF.
        if (info.cid_vec_ != nullptr) {
          volatile float v = info.cid_vec_[0];
          (void)v;
        }
        ok_acquires.fetch_add(1, std::memory_order_relaxed);
      } else if (common::OB_ENTRY_NOT_EXIST == ret) {
        not_exist_acquires.fetch_add(1, std::memory_order_relaxed);
      }
      // guard auto-releases on scope exit
      ++iter;
    }
  };

  // Flipper: monotonically push every cid through the full lifecycle.
  // 100us spacing keeps total flipper time ~12.8ms — enough for workers/gc to
  // interleave, short enough to keep test fast.
  auto flipper_fn = [&]() {
    static const ObIvfCentroidState kSeq[] = {
      ObIvfCentroidState::SPLITTING,
      ObIvfCentroidState::SPLIT_DONE,
      ObIvfCentroidState::REASSIGNING,
      ObIvfCentroidState::MERGED,
    };
    for (int64_t cid = 0; cid < N_CIDS; ++cid) {
      for (size_t s = 0; s < sizeof(kSeq) / sizeof(kSeq[0]); ++s) {
        ObCentroidInfo info;
        info.state_ = kSeq[s];
        info.posting_list_count_ = 0;
        // set_center_info only flips state_/posting_list_count_;
        // create_scn_/delet_scn_/cid_vec_/ref_cnt_ stay intact.
        (void)cache_->set_center_info(cid, info);
        // 生产路径在 reassign 提交后 set_centroid_del_scn；MERGED 时标记逻辑删除供 GC。
        if (ObIvfCentroidState::MERGED == kSeq[s]) {
          SCN del_scn = SCN::min_scn();
          del_scn.convert_for_tx(1000);
          (void)cache_->set_centroid_del_scn(cid, del_scn);
        }
        usleep(100);
      }
    }
  };

  // GC: collect_terminal_cids + try_erase loop.
  auto gc_fn = [&]() {
    while (!stop.load(std::memory_order_acquire)) {
      common::ObSEArray<int64_t, 64> cids;
      cids.set_attr(ObMemAttr(TEST_TENANT_ID, "TstGcCidsG"));
      (void)cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids);
      for (int64_t i = 0; i < cids.count(); ++i) {
        bool erased = false;
        (void)cache_->try_erase_centroid(cids.at(i), TEST_MIN_RESERVED_SNAPSHOT, erased);
      }
      usleep(50);
    }
  };

  std::vector<std::thread> workers;
  workers.reserve(N_WORKERS);
  for (int i = 0; i < N_WORKERS; ++i) {
    workers.emplace_back(worker_fn, i);
  }
  std::thread gc_thread(gc_fn);
  std::thread flipper_thread(flipper_fn);

  // Wait for flipper to finish — every cid is now in MERGED state.
  flipper_thread.join();

  // Signal workers/gc to stop and join.
  stop.store(true, std::memory_order_release);
  for (auto &t : workers) {
    t.join();
  }
  gc_thread.join();

  // Final cleanup: drain any remaining terminal cids whose erase was blocked
  // by a still-running worker's ref_cnt_ at stop time.
  for (int retry = 0; retry < 100 && cache_->count_ > 0; ++retry) {
    common::ObSEArray<int64_t, 64> cids;
    cids.set_attr(ObMemAttr(TEST_TENANT_ID, "TstGcCidsG"));
    ASSERT_EQ(common::OB_SUCCESS, cache_->collect_terminal_cids(TEST_MIN_RESERVED_SNAPSHOT, cids));
    if (cids.count() == 0) {
      break;
    }
    for (int64_t i = 0; i < cids.count(); ++i) {
      bool erased = false;
      (void)cache_->try_erase_centroid(cids.at(i), TEST_MIN_RESERVED_SNAPSHOT, erased);
    }
  }

  // Every cid must be cleaned up.
  EXPECT_EQ(0, cache_->count_);
  // Must have observed real concurrency: at least some acquires succeeded
  // (ensures the test actually exercised the race window, not a degenerate
  // "all entries gone before any acquire ran" path).
  EXPECT_GT(ok_acquires.load(), 0)
      << "no successful acquire — concurrency window was too narrow; "
      << "not_exist=" << not_exist_acquires.load();
}

// Case I: 40 SPLIT_DONE centroids; 8 batch_guards each acquire 5 cids; while
// all 40 refs are held, try_erase must refuse all; release_all then unblocks
// the next sweep which removes every cid.
TEST_F(TestIvfMapCentCacheRefCnt, case_i_multi_cid_acquire_block_then_release)
{
  const int kGroups = 8;
  const int kPerGroup = 5;
  const int64_t N = kGroups * kPerGroup;  // 40

  // Insert as NORMAL first so acquire (which requires is_visible_stat) succeeds.
  for (int64_t i = 0; i < N; ++i) {
    insert_centroid(i);
  }
  ASSERT_EQ(N, cache_->count_);

  // Phase 1: 8 batch_guards each acquire its 5 cids individually via
  // acquire_centroid_vec, recording them into the batch_guard for release_all.
  ObCentroidVecBatchGuard batch_guards[kGroups];
  for (int g = 0; g < kGroups; ++g) {
    batch_guards[g].bind(cache_);
    for (int j = 0; j < kPerGroup; ++j) {
      const int64_t cid = g * kPerGroup + j;
      ObCentroidVecGuard tmp;
      ObCentroidInfo info;
      int ret = cache_->acquire_centroid_vec(cid, SCN::min_scn(), tmp, info);
      ASSERT_EQ(common::OB_SUCCESS, ret) << "cid=" << cid;
      ASSERT_TRUE(tmp.is_held());
      // Detach: release ownership from tmp (do not let dtor release), and
      // hand the ref to batch_guard for explicit release_all.
      tmp.held_ = false;
      ASSERT_EQ(common::OB_SUCCESS, batch_guards[g].add_acquired(cid));
    }
  }
  // Now flip every cid to SPLIT_DONE: terminal state with held ref.
  for (int64_t i = 0; i < N; ++i) {
    set_state(i, ObIvfCentroidState::SPLIT_DONE);
  }

  // All 40 cids should now have ref_cnt_=1.
  for (int64_t i = 0; i < N; ++i) {
    EXPECT_EQ(1, read_ref_cnt(i)) << "cid=" << i;
  }

  // Phase 2: try_erase on every cid must refuse (ref_cnt_ != 0).
  for (int64_t i = 0; i < N; ++i) {
    bool erased = true;
    int ret = cache_->try_erase_centroid(i, TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, ret);
    EXPECT_FALSE(erased) << "cid=" << i << " should be blocked by ref_cnt_=1";
  }
  EXPECT_EQ(N, cache_->count_);

  // Phase 3: release_all on every batch_guard, then try_erase must succeed.
  for (int g = 0; g < kGroups; ++g) {
    EXPECT_EQ(common::OB_SUCCESS, batch_guards[g].release_all());
  }
  for (int64_t i = 0; i < N; ++i) {
    EXPECT_EQ(0, read_ref_cnt(i)) << "cid=" << i;
  }
  for (int64_t i = 0; i < N; ++i) {
    mark_gc_erasable(i);
    bool erased = false;
    int ret = cache_->try_erase_centroid(i, TEST_MIN_RESERVED_SNAPSHOT, erased);
    EXPECT_EQ(common::OB_SUCCESS, ret);
    EXPECT_TRUE(erased) << "cid=" << i;
  }
  EXPECT_EQ(0, cache_->count_);
}

// Case J: release_all() must be truly idempotent.
// Contract from ob_vector_index_ivf_cache_mgr.cpp:1605-1628:
//   release_all() iterates acquired_, calls do_release_one for each cid, then
//   unconditionally acquired_.reset(). A second call therefore must be a true
//   noop (acquired_ is empty → no ObReleaseCentroidFn invocation → no risk of
//   ref_cnt_ underflow / OB_ERR_UNEXPECTED / LOG_ERROR).
//
// This test verifies:
//   1. First release_all drives every ref_cnt_ to 0 and resets acquired_.
//   2. Second release_all returns OB_SUCCESS and does NOT mutate ref_cnt_.
//   3. After release_all, the guard can be reused (re-acquire works cleanly).
TEST_F(TestIvfMapCentCacheRefCnt, case_j_release_all_idempotent)
{
  const int64_t N = 10;
  for (int64_t i = 0; i < N; ++i) {
    insert_centroid(i);  // NORMAL, visible to acquire
  }
  ASSERT_EQ(N, cache_->count_);

  ObCentroidVecBatchGuard batch_guard;
  common::ObSEArray<ObCentroidInfo, 16> out;
  out.set_attr(ObMemAttr(TEST_TENANT_ID, "TstAcqOutJ"));

  int ret = cache_->acquire_visible_centroids(SCN::min_scn(), batch_guard, out);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(N, out.count());
  ASSERT_EQ(N, batch_guard.count());
  for (int64_t i = 0; i < N; ++i) {
    EXPECT_EQ(1, read_ref_cnt(i)) << "cid=" << i;
  }

  // (1) First release_all: drives every ref_cnt_ to 0 and resets acquired_.
  EXPECT_EQ(common::OB_SUCCESS, batch_guard.release_all());
  EXPECT_EQ(0, batch_guard.count());
  for (int64_t i = 0; i < N; ++i) {
    EXPECT_EQ(0, read_ref_cnt(i)) << "cid=" << i;
  }

  // (2) Second release_all: must be a true noop because acquired_ is empty.
  // If release_all did NOT clear acquired_, this call would feed each cid
  // through ObReleaseCentroidFn at ref_cnt_=0, triggering OB_ERR_UNEXPECTED
  // + LOG_ERROR (see ob_vector_index_ivf_cache_mgr.cpp:1315-1331). Both ret
  // and ref_cnt_ must remain clean.
  EXPECT_EQ(common::OB_SUCCESS, batch_guard.release_all());
  EXPECT_EQ(0, batch_guard.count());
  for (int64_t i = 0; i < N; ++i) {
    EXPECT_EQ(0, read_ref_cnt(i)) << "cid=" << i;
  }

  // (3) The guard must be reusable after release_all — re-acquire all visible
  // centroids and verify ref_cnt_ goes back to 1 across the board.
  ObCentroidVecBatchGuard batch_guard2;
  common::ObSEArray<ObCentroidInfo, 16> out2;
  out2.set_attr(ObMemAttr(TEST_TENANT_ID, "TstAcqOutJ2"));
  EXPECT_EQ(common::OB_SUCCESS,
            cache_->acquire_visible_centroids(SCN::min_scn(), batch_guard2, out2));
  EXPECT_EQ(N, out2.count());
  EXPECT_EQ(N, batch_guard2.count());
  for (int64_t i = 0; i < N; ++i) {
    EXPECT_EQ(1, read_ref_cnt(i)) << "cid=" << i;
  }
  // batch_guard2 dtor releases all refs.
}

TEST_F(TestIvfMapCentCacheRefCnt, spfresh_compensation_cas_does_not_overwrite_third_state)
{
  insert_centroid(1);
  bool cas_ok = false;
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->update_centroid_state_cas(1, ObIvfCentroidState::NORMAL,
                                              ObIvfCentroidState::MERGING, cas_ok));
  ASSERT_TRUE(cas_ok);

  // Simulate a newer task advancing the cache before the old Merge compensates.
  set_state(1, ObIvfCentroidState::REASSIGNING);
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->update_centroid_state_cas(1, ObIvfCentroidState::MERGING,
                                              ObIvfCentroidState::NORMAL, cas_ok));
  EXPECT_FALSE(cas_ok);
  ObCentroidInfo info;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), info));
  EXPECT_EQ(ObIvfCentroidState::REASSIGNING, info.state_);
}

TEST_F(TestIvfMapCentCacheRefCnt, spfresh_compensation_target_state_is_idempotent)
{
  insert_centroid(1);
  bool cas_ok = true;
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->update_centroid_state_cas(1, ObIvfCentroidState::MERGING,
                                              ObIvfCentroidState::NORMAL, cas_ok));
  EXPECT_FALSE(cas_ok);
  ObCentroidInfo info;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), info));
  EXPECT_EQ(ObIvfCentroidState::NORMAL, info.state_);
}

TEST_F(TestIvfMapCentCacheRefCnt, max_center_id_includes_retired_entry_until_gc)
{
  insert_centroid(129);
  insert_centroid(130);
  ASSERT_EQ(common::OB_SUCCESS, cache_->incr_posting_list_length(130, 50));
  ObCentroidVecGuard guard;
  ObCentroidInfo info;
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->acquire_centroid_vec(130, SCN::min_scn(), guard, info));
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->update_centroid_state(130, ObIvfCentroidState::MERGED));
  mark_gc_erasable(130);
  ASSERT_EQ(common::OB_ENTRY_NOT_EXIST,
            cache_->get_center_info(130, SCN::max_scn(), info));

  int64_t max_cid = 0;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_max_center_id(max_cid));
  EXPECT_EQ(130, max_cid);
  bool erased = false;
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->try_erase_centroid(130, TEST_MIN_RESERVED_SNAPSHOT, erased));
  EXPECT_FALSE(erased);
  ASSERT_EQ(common::OB_SUCCESS, guard.release());
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_max_center_id(max_cid));
  EXPECT_EQ(130, max_cid);
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->try_erase_centroid(130, TEST_MIN_RESERVED_SNAPSHOT, erased));
  EXPECT_TRUE(erased);
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_max_center_id(max_cid));
  EXPECT_EQ(129, max_cid);
  insert_centroid(max_cid + 1);
  ASSERT_EQ(common::OB_SUCCESS, cache_->incr_posting_list_length(130, 2000));
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(130, SCN::max_scn(), info));
  EXPECT_EQ(2000, info.posting_list_count_);
}

TEST_F(TestIvfMapCentCacheRefCnt, max_center_id_handles_empty_and_uninitialized_map)
{
  int64_t max_cid = -1;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_max_center_id(max_cid));
  EXPECT_EQ(0, max_cid);
  cache_->cent_map_.destroy();
  EXPECT_EQ(common::OB_NOT_INIT, cache_->get_max_center_id(max_cid));
}

TEST_F(TestIvfMapCentCacheRefCnt, replacement_waits_for_single_and_batch_readers)
{
  insert_centroid(1);
  ObCentroidVecGuard guard;
  ObCentroidInfo info;
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->acquire_centroid_vec(1, SCN::min_scn(), guard, info));
  float *old_vec = info.cid_vec_;
  float replacement[TEST_DIM] = {};
  SCN scn = SCN::base_scn();
  ASSERT_EQ(common::OB_EAGAIN, cache_->add_centroid(1, replacement, TEST_DIM, &scn));
  EXPECT_FLOAT_EQ(dummy_vec_[0], old_vec[0]);
  EXPECT_EQ(1, read_ref_cnt(1));
  ASSERT_EQ(common::OB_SUCCESS, guard.release());

  ObCentroidVecBatchGuard batch_guard;
  common::ObSEArray<ObCentroidInfo, 4> infos;
  ASSERT_EQ(common::OB_SUCCESS,
            cache_->acquire_visible_centroids(SCN::min_scn(), batch_guard, infos));
  ASSERT_EQ(common::OB_EAGAIN, cache_->add_centroid(1, replacement, TEST_DIM, &scn));
  EXPECT_FLOAT_EQ(dummy_vec_[0], old_vec[0]);
  ASSERT_EQ(common::OB_SUCCESS, batch_guard.release_all());
  EXPECT_EQ(common::OB_SUCCESS, cache_->add_centroid(1, replacement, TEST_DIM, &scn));
}

TEST_F(TestIvfMapCentCacheRefCnt, reconciliation_rejects_aba_and_replacement)
{
  insert_centroid(1);
  ASSERT_EQ(common::OB_SUCCESS, cache_->incr_posting_list_length(1, 10));
  ObCentroidInfo snapshot;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), snapshot));
  ASSERT_EQ(common::OB_SUCCESS, cache_->incr_posting_list_length(1, 1));
  ASSERT_EQ(common::OB_SUCCESS, cache_->decr_posting_list_length(1, 1));
  EXPECT_EQ(common::OB_EAGAIN, cache_->reconcile_posting_list_length(1, snapshot, 7));
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), snapshot));
  ASSERT_EQ(common::OB_SUCCESS, cache_->set_center_info(1, snapshot));
  EXPECT_EQ(common::OB_EAGAIN, cache_->reconcile_posting_list_length(1, snapshot, 7));
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), snapshot));
  SCN scn = SCN::base_scn();
  ASSERT_EQ(common::OB_SUCCESS, cache_->add_centroid(1, dummy_vec_, TEST_DIM, &scn));
  EXPECT_EQ(common::OB_EAGAIN, cache_->reconcile_posting_list_length(1, snapshot, 7));
}

TEST_F(TestIvfMapCentCacheRefCnt, concurrent_centroid_inserts_preserve_total_count)
{
  constexpr int64_t THREADS = 4;
  constexpr int64_t WAVES = 32;
  std::atomic<int> failures(0);
  for (int64_t wave = 0; wave < WAVES; ++wave) {
    // The fixture has no tenant throttle tool. Reset between joined waves,
    // keeping each wave below the allocator's periodic throttle check.
    sub_ctx_->check_cnt_ = 0;
    std::atomic<bool> start(false);
    std::vector<std::thread> workers;
    for (int64_t t = 0; t < THREADS; ++t) {
      workers.emplace_back([&, t]() {
        SCN scn = SCN::min_scn();
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        if (common::OB_SUCCESS != cache_->add_centroid(
            wave * THREADS + t, dummy_vec_, TEST_DIM, &scn)) {
          ++failures;
        }
      });
    }
    start.store(true, std::memory_order_release);
    for (auto &worker : workers) {
      worker.join();
    }
  }
  EXPECT_EQ(0, failures.load());
  EXPECT_EQ(THREADS * WAVES, cache_->get_count());
}

TEST_F(TestIvfMapCentCacheRefCnt, reconcile_count_preserves_new_lifecycle_state)
{
  insert_centroid(1);
  ASSERT_EQ(common::OB_SUCCESS, cache_->incr_posting_list_length(1, 10));
  ObCentroidInfo snapshot;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), snapshot));
  ASSERT_EQ(common::OB_SUCCESS, cache_->update_centroid_state(1, ObIvfCentroidState::REASSIGNING));
  ASSERT_EQ(common::OB_SUCCESS, cache_->reconcile_posting_list_length(1, snapshot, 7));
  ObCentroidInfo info;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), info));
  EXPECT_EQ(7, info.posting_list_count_);
  EXPECT_EQ(ObIvfCentroidState::REASSIGNING, info.state_);
}

TEST_F(TestIvfMapCentCacheRefCnt, reconcile_count_rejects_concurrent_count_change)
{
  insert_centroid(1);
  ASSERT_EQ(common::OB_SUCCESS, cache_->incr_posting_list_length(1, 10));
  ObCentroidInfo snapshot;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), snapshot));
  ASSERT_EQ(common::OB_SUCCESS, cache_->incr_posting_list_length(1, 1));
  EXPECT_EQ(common::OB_EAGAIN, cache_->reconcile_posting_list_length(1, snapshot, 7));
  ObCentroidInfo info;
  ASSERT_EQ(common::OB_SUCCESS, cache_->get_center_info(1, SCN::max_scn(), info));
  EXPECT_EQ(11, info.posting_list_count_);
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            cache_->reconcile_posting_list_length(1, info, static_cast<int64_t>(INT32_MAX) + 1));
  EXPECT_EQ(common::OB_ENTRY_NOT_EXIST, cache_->reconcile_posting_list_length(2, info, 1));
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
