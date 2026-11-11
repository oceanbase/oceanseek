/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */
// DiskBBQ SSTable unit and integration tests.

#define USING_LOG_PREFIX STORAGE

#include "mtlenv/mock_tenant_module_env.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <deque>
#include <functional>
#include <future>
#include <list>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/mman.h>
#include <queue>
#include <set>
#include <string>
#include <vector>

#define private public
#define protected public

#include "share/ob_cluster_version.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "common/ob_target_specific.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/block_set.h"
#include "lib/alloc/ob_tenant_ctx_allocator.h"
#include "lib/cpu/ob_cpu_topology.h"
#include "lib/rc/context.h"
#include "lib/utility/ob_defer.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "share/vector_type/ob_vector_cosine_distance.h"
#include "share/vector_type/ob_vector_ip_distance.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/access/ob_sstable_row_multi_getter.h"
#include "storage/access/ob_sstable_row_scanner.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_iterator.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_layout.h"
#include "storage/blocksstable/vector_index/ob_ivf_hca_algorithm_adapter.h"
#include "storage/blocksstable/vector_index/ob_ivf_parent_centroid_build.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#include "storage/blocksstable/vector_index/ob_disk_bbq_fast_scanner.h"
#include "storage/blocksstable/vector_index/ob_disk_bbq_searcher.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "storage/ob_micro_block_index_iterator.h"
#include "storage/ob_i_store.h"
#include "storage/schema_utils.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"

#include "test_disk_bbq_common.h"
#include "sql/das/search/ob_das_async_ivf_search_op.h"
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_async_ivf_scan_iter.h"
#include "sql/das/iter/ob_das_vec_index_driver_iter.h"
#include "sql/das/iter/ob_das_iter_utils.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "sql/das/iter/ob_das_vec_index_ivf_scan_iter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/hybrid_search/ob_hybrid_search_dsl_resolver.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_base.h"
#include "share/system_variable/ob_system_variable.h"
#include "storage/test_dml_common.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;
using namespace compaction;

namespace unittest
{
using blocksstable::ObIvfPreFilter;

static int create_ivf_searcher(
    ObIvfSSTable &sstable,
    const ObTableScanParam &scan_param,
    ObTableAccessContext &access_ctx,
    ObIvfSSTableSearcher *&searcher)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  searcher = nullptr;
  if (OB_FAIL(table_handle.set_sstable_with_tablet(&sstable))) {
  } else if (OB_FAIL(sstable.create_searcher(
      table_handle, scan_param, access_ctx, searcher))) {
  }
  return ret;
}

static int init_test_deletion_bitmap(
    ObIAllocator &allocator,
    const int64_t *deleted_vids,
    const int64_t deleted_vid_count,
    ObDeletionBitmapHandle &handle)
{
  int ret = OB_SUCCESS;
  ObRoaringBitmap *deleted = nullptr;
  if (OB_UNLIKELY(deleted_vid_count < 0
      || (deleted_vid_count > 0 && OB_ISNULL(deleted_vids)))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(deleted = OB_NEWx(ObRoaringBitmap, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < deleted_vid_count; ++i) {
    if (OB_FAIL(deleted->value_add(deleted_vids[i]))) {
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(handle.init_from_bitmap(allocator, deleted))) {
  } else if (OB_SUCC(ret)) {
    // Ownership transfers to the handle after successful initialization.
    deleted = nullptr;
  }
  if (nullptr != deleted) {
    deleted->set_empty();
    deleted->~ObRoaringBitmap();
    allocator.free(deleted);
  }
  return ret;
}

class ObIvfCountingAllocator : public ObIAllocator
{
public:
  explicit ObIvfCountingAllocator(ObIAllocator &allocator, const bool fail_alloc = false)
    : allocator_(allocator),
      fail_on_alloc_(fail_alloc ? 1 : 0),
      fail_alloc_size_(-1),
      fail_after_counter_(nullptr),
      alloc_count_(0),
      free_count_(0),
      last_alloc_size_(-1),
      watched_size_(-1),
      watched_alloc_count_(0),
      watched_ptrs_(),
      live_ptrs_(),
      live_sizes_(),
      live_allocated_bytes_(0)
  {}
  void set_fail_on_alloc(const int64_t alloc_count) { fail_on_alloc_ = alloc_count; }
  void set_fail_alloc_size(const int64_t size) { fail_alloc_size_ = size; }
  void set_fail_alloc_size_after_count(const int64_t size, const int64_t &counter)
  { fail_alloc_size_ = size; fail_after_counter_ = &counter; }
  void watch_alloc_size(const int64_t size)
  {
    watched_size_ = size;
    watched_alloc_count_ = 0;
    watched_ptrs_.clear();
  }
  int64_t count_watched_copies(const void *data) const
  {
    int64_t count = 0;
    for (int64_t i = 0; i < static_cast<int64_t>(watched_ptrs_.size()); ++i) {
      count += 0 == MEMCMP(watched_ptrs_[i], data, watched_size_);
    }
    return count;
  }
  int64_t live_allocation_count() const
  {
    return static_cast<int64_t>(live_ptrs_.size());
  }
  int64_t live_allocated_bytes() const { return live_allocated_bytes_; }
  virtual void *alloc(const int64_t size) override
  {
    ++alloc_count_;
    last_alloc_size_ = size;
    watched_alloc_count_ += size == watched_size_;
    void *ptr = fail_on_alloc_ == alloc_count_
        || (fail_alloc_size_ == size && (nullptr == fail_after_counter_ || *fail_after_counter_ > 0))
        ? nullptr : allocator_.alloc(size);
    if (nullptr != ptr) {
      live_ptrs_.push_back(ptr);
      live_sizes_.push_back(size);
      live_allocated_bytes_ += size;
    }
    if (nullptr != ptr && size == watched_size_) {
      watched_ptrs_.push_back(ptr);
    }
    return ptr;
  }
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    ++alloc_count_;
    requested_attrs_.push_back(attr);
    last_alloc_size_ = size;
    watched_alloc_count_ += size == watched_size_;
    void *ptr = fail_on_alloc_ == alloc_count_
        || (fail_alloc_size_ == size && (nullptr == fail_after_counter_ || *fail_after_counter_ > 0))
        ? nullptr : allocator_.alloc(size, attr);
    if (nullptr != ptr) {
      live_ptrs_.push_back(ptr);
      live_sizes_.push_back(size);
      live_allocated_bytes_ += size;
    }
    if (nullptr != ptr && size == watched_size_) {
      watched_ptrs_.push_back(ptr);
    }
    return ptr;
  }
  virtual void free(void *ptr) override
  {
    ++free_count_;
    for (int64_t i = 0; i < static_cast<int64_t>(live_ptrs_.size()); ++i) {
      if (live_ptrs_[i] == ptr) {
        live_allocated_bytes_ -= live_sizes_[i];
        live_ptrs_.erase(live_ptrs_.begin() + i);
        live_sizes_.erase(live_sizes_.begin() + i);
        break;
      }
    }
    for (int64_t i = 0; i < static_cast<int64_t>(watched_ptrs_.size()); ++i) {
      if (watched_ptrs_[i] == ptr) {
        watched_ptrs_.erase(watched_ptrs_.begin() + i);
        break;
      }
    }
    allocator_.free(ptr);
  }

  ObIAllocator &allocator_;
  int64_t fail_on_alloc_;
  int64_t fail_alloc_size_;
  const int64_t *fail_after_counter_;
  int64_t alloc_count_;
  int64_t free_count_;
  int64_t last_alloc_size_;
  int64_t watched_size_;
  int64_t watched_alloc_count_;
  std::vector<void *> watched_ptrs_;
  std::vector<ObMemAttr> requested_attrs_;
  std::vector<void *> live_ptrs_;
  std::vector<int64_t> live_sizes_;
  int64_t live_allocated_bytes_;
};

class ObIvfSortedMultiGetterSeam final : public ObSSTableRowMultiGetter
{
public:
  bool is_rowkey_sorted() const { return prefetcher_.is_rowkey_sorted_; }
};

class TestIVFSSTable : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // Reuse MockTenantModuleEnv's sys tenant. Do not replace it with a local
    // ObTenantBase, which drops the IO manager and timer run_wrapper.
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }

  int build_empty_sstable_param(
      const ObITable::TableType table_type,
      ObStorageSchema &storage_schema,
      ObTabletCreateSSTableParam &param)
  {
    int ret = OB_SUCCESS;
    share::schema::ObTableSchema table_schema;
    const ObTabletID tablet_id(1001);
    const int64_t snapshot_version = 100;
    TestSchemaUtils::prepare_data_schema(table_schema);
    if (OB_FAIL(storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL))) {
      LOG_WARN("failed to init storage schema", K(ret));
    } else if (OB_FAIL(param.init_for_empty_major_sstable(
        tablet_id, storage_schema, snapshot_version, -1, false, false))) {
      LOG_WARN("failed to init empty major sstable param", K(ret));
    } else {
      param.table_key_.table_type_ = table_type;
      param.table_key_.tablet_id_ = tablet_id;
      param.table_key_.version_range_.base_version_ = 0;
      param.table_key_.version_range_.snapshot_version_ = snapshot_version;
      if (param.table_key_.is_ivf_sstable()) {
        param.ivf_meta_.set_empty();
      }
    }
    return ret;
  }

  void fill_ivf_meta(
      const int32_t dim,
      ObIvfSSTableMeta &meta)
  {
    meta.reset();
    meta.index_type_ = share::VIAT_DISK_BBQ;
    meta.dist_type_ = share::VIDA_L2;
    meta.dim_ = dim;
    meta.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
    meta.nbits_ = 1;
    meta.fht_algo_version_ =
        quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
  }

  int fill_inc_major_uncommit_tx(ObTabletCreateSSTableParam &param)
  {
    int ret = OB_SUCCESS;
    compaction::ObUncommitTxDesc tx_desc(100 /*tx_id*/, 200 /*sql_seq*/);
    if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
      LOG_WARN("failed to push uncommit tx desc", K(ret));
    } else {
      param.uncommit_tx_info_.set_info_status_record_seq();
    }
    return ret;
  }

  int build_inc_ivf_sstable(ObIvfSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObIvfSSTableMeta ivf_meta;
    fill_ivf_meta(128, ivf_meta);
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::INC_IVF_INDEX_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build inc ivf sstable param", K(ret));
    } else if (OB_FAIL(fill_inc_major_uncommit_tx(param))) {
      LOG_WARN("failed to fill inc major uncommit tx", K(ret));
    } else if (FALSE_IT(param.ivf_meta_ = ivf_meta)) {
    } else if (OB_FAIL(sstable.init(param, &allocator_))) {
      LOG_WARN("failed to init inc ivf sstable", K(ret));
    }
    return ret;
  }

  int build_ivf_sstable(ObIvfSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObIvfSSTableMeta ivf_meta;
    fill_ivf_meta(128, ivf_meta);
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::IVF_INDEX_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build sstable param", K(ret));
    } else if (FALSE_IT(param.ivf_meta_ = ivf_meta)) {
    } else if (OB_FAIL(sstable.init(param, &allocator_))) {
      LOG_WARN("failed to init ivf sstable", K(ret));
    }
    return ret;
  }

  int build_normal_sstable(ObSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::MAJOR_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build normal sstable param", K(ret));
    } else if (OB_FAIL(sstable.init(param, &allocator_))) {
      LOG_WARN("failed to init normal sstable", K(ret));
    }
    return ret;
  }

  int build_async_ivf_storage_schema(ObStorageSchema &storage_schema)
  {
    int ret = OB_SUCCESS;
    share::schema::ObTableSchema table_schema;
    TestSchemaUtils::prepare_data_schema(table_schema);
    table_schema.set_table_type(USER_INDEX);
    table_schema.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
    table_schema.set_data_table_id(table_schema.get_table_id());
    if (OB_FAIL(storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL))) {
      LOG_WARN("failed to init async ivf storage schema", K(ret));
    }
    return ret;
  }

  int build_inv_sstable(ObInvSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObInvIdxStatsMeta inv_meta;
    inv_meta.reset();
    inv_meta.min_doc_id_ = 7;
    inv_meta.max_doc_id_ = 100;
    inv_meta.doc_id_obj_meta_.set_uint64();
    inv_meta.index_type_ = INV_IDX_TYPE_FTS;
    inv_meta.has_relevance_params_ = true;
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::INV_INDEX_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build inv sstable param", K(ret));
    } else if (OB_FAIL(param.inv_meta_.init(inv_meta))) {
      LOG_WARN("failed to init inv sstable meta", K(ret));
    } else if (OB_FAIL(sstable.init(param, &allocator_))) {
      LOG_WARN("failed to init inv sstable", K(ret));
    }
    return ret;
  }

  ObArenaAllocator allocator_;
};

// Only used by cases that construct a FLAT query through the public factory.
static ObIvfCentroidGroupReader &flat_group_reader_for_test(const ObIvfCentroidIterator *iterator)
{
  return const_cast<ObIvfCentroidGroupReader &>(
      static_cast<const ObIvfFlatCentroidIterator *>(iterator)->reader_);
}

TEST_F(TestIVFSSTable, parent_hgraph_parallel_add_uses_tenant_memory_and_cleans_up)
{
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  uint64_t *vsag_memory = service->get_all_vsag_use_mem();
  ASSERT_NE(nullptr, vsag_memory);
  const int64_t count = 16384, dim = 4;
  std::vector<float> values(count * dim);
  uint32_t seed = 42;
  for (float &value : values) {
    seed = seed * 1664525U + 1013904223U;
    value = static_cast<float>(seed % 100003) / 100003.0F;
  }
  const std::vector<float> original = values;
  ObArrayWrap<float> leaves(values.data(), values.size());
  ObIVFHcaAlgorithmParam param;
  param.hgraph_m_ = 4;
  param.hgraph_ef_construction_ = 32;
  // Cover both a completed add barrier and teardown with unconsumed tasks.
  for (const bool finish_add : {true, false}) {
    SCOPED_TRACE(finish_add);
    const uint64_t memory_before = ATOMIC_LOAD(vsag_memory);
    ObIVFMergeRuntimeAllocator memory;
    {
      ObIvfCentroidLayout layout(memory);
      ObIVFParentCentroidBuilder builder(memory);
      ASSERT_EQ(OB_SUCCESS, builder.init(leaves, dim, param, 1 /* cluster_size */));
      bool saw_hgraph_add = false;
      for (int64_t plan_id = 0; !saw_hgraph_add && plan_id < 256; ++plan_id) {
        int64_t task_count = 0;
        ASSERT_EQ(OB_SUCCESS, builder.prepare(plan_id, task_count));
        ASSERT_GT(task_count, 0);
        const bool graph_add = builder.get_step() == ObIVFMergeStep::NEIGHBORHOOD_ADD;
        if (graph_add) { ASSERT_GT(task_count, 4); }
        const int64_t tasks_to_run = graph_add && !finish_add ? 4 : task_count;
        std::vector<int> results(tasks_to_run, OB_ERR_UNEXPECTED);
        std::atomic<int64_t> next(0);
        std::atomic<bool> start(false);
        std::vector<std::thread> workers;
        ObTenantBase *tenant = ObTenantEnv::get_tenant();
        for (int64_t t = 0; t < MIN(4L, tasks_to_run); ++t) {
          workers.emplace_back([&] {
            ObTenantEnv::set_tenant(tenant);
            while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
            for (int64_t i = next.fetch_add(1); i < tasks_to_run; i = next.fetch_add(1)) {
              results[i] = builder.execute(i);
            }
            ObTenantEnv::set_tenant(nullptr);
          });
        }
        start.store(true, std::memory_order_release);
        for (auto &worker : workers) { worker.join(); }
        for (const int result : results) { ASSERT_EQ(OB_SUCCESS, result); }
        if (graph_add) {
          saw_hgraph_add = true;
          // Query arenas bypass this counter. Parent construction must use
          // the same tenant-accounted graph allocator as leaf DDL/Merge.
          EXPECT_GT(ATOMIC_LOAD(vsag_memory), memory_before);
        }
        if (!graph_add || finish_add) {
          bool finished = false;
          ASSERT_EQ(OB_SUCCESS, builder.finish(plan_id, layout, finished));
          ASSERT_FALSE(finished);
          EXPECT_FALSE(layout.is_ready());
        }
      }
      // This test must reach HGraph rather than silently use brute force.
      ASSERT_TRUE(saw_hgraph_add);
      EXPECT_EQ(original, values);
    }
    EXPECT_EQ(0, memory.used());
    EXPECT_EQ(memory_before, ATOMIC_LOAD(vsag_memory));
  }
}

TEST_F(TestIVFSSTable, create_sstable_helper_builds_ivf)
{
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  ObIvfSSTable sstable;

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::IVF_INDEX_SSTABLE, storage_schema, param));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
      param, allocator_, sstable));
  ASSERT_TRUE(sstable.is_ivf_sstable());
  ASSERT_TRUE(sstable.is_loaded());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, sstable.get_meta_type());
}

TEST_F(TestIVFSSTable, create_sstable_helper_builds_inc_ivf)
{
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  ObIvfSSTable sstable;

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::INC_IVF_INDEX_SSTABLE, storage_schema, param));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_uncommit_tx(param));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
      param, allocator_, sstable));
  ASSERT_TRUE(sstable.is_ivf_sstable());
  ASSERT_TRUE(sstable.is_inc_ivf_sstable());
  ASSERT_TRUE(sstable.is_loaded());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, sstable.get_meta_type());
}

TEST_F(TestIVFSSTable, empty_ivf_sstable_serializes_reset_meta)
{
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  ObIvfSSTable sstable;
  const uint64_t data_version = DATA_CURRENT_VERSION;

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::IVF_INDEX_SSTABLE, storage_schema, param));
  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));
  ASSERT_TRUE(sstable.get_ivf_meta().is_valid());
  ASSERT_TRUE(sstable.get_ivf_meta().is_empty());

  const int64_t buf_len = sstable.get_full_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_TRUE(sstable.no_data_to_read());
  ASSERT_EQ(OB_SUCCESS, sstable.serialize(data_version, buf, buf_len, pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, sstable.serialize_full_table(data_version, buf, buf_len, pos));
}

TEST_F(TestIVFSSTable, knn_search_rejects_invalid)
{
  ObIvfSSTable sstable;
  ObIvfKnnSearchParam invalid_query_param;
  ObTableScanParam scan_param;
  scan_param.allocator_ = &allocator_;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_ctx;
  share::schema::ObTableSchema table_schema;
  share::schema::ObTableParam table_param(allocator_);
  ObSEArray<uint64_t, 4> output_column_ids;
  float query[4] = {0.0f, 0.0f, 0.0f, 0.0f};
  ObIvfSSTableSearcher *searcher = reinterpret_cast<ObIvfSSTableSearcher *>(1);

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  ASSERT_EQ(OB_SUCCESS,
      init_ivf_access_context(sstable, scan_param, store_ctx, access_ctx));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      create_ivf_searcher(sstable, scan_param, access_ctx, searcher));
  ASSERT_EQ(nullptr, searcher);

  prepare_ivf_schema(table_schema);
  for (int64_t i = 0; i < table_schema.get_column_count(); ++i) {
    const ObColumnSchemaV2 *column = table_schema.get_column_schema_by_idx(i);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(OB_SUCCESS, output_column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, table_param.convert(
      table_schema, output_column_ids, scan_param.pd_storage_flag_));
  scan_param.table_param_ = &table_param;
  scan_param.tablet_id_ = sstable.get_key().get_tablet_id();

  invalid_query_param.query_ = query;
  invalid_query_param.dim_ = 4;
  invalid_query_param.nprobe_ = 1;
  invalid_query_param.k_ = 1;
  invalid_query_param.dim_ = static_cast<int64_t>(INT32_MAX) + 1;
  ASSERT_FALSE(invalid_query_param.is_valid());
  searcher = reinterpret_cast<ObIvfSSTableSearcher *>(1);
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable, scan_param, access_ctx, searcher));
  ASSERT_NE(nullptr, searcher);
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      searcher->knn_search(invalid_query_param, result));
  ASSERT_EQ(0, result.count());
  ObIvfSSTableSearcher::destroy(searcher);
  searcher = nullptr;

  ObIvfSSTable not_inited_sstable;
  invalid_query_param.dim_ = 4;
  searcher = reinterpret_cast<ObIvfSSTableSearcher *>(1);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      create_ivf_searcher(not_inited_sstable, scan_param, access_ctx, searcher));
  ASSERT_EQ(nullptr, searcher);
}

TEST_F(TestIVFSSTable, create_searcher_rejects_unsupported_index_type)
{
  ObIvfSSTable sstable;
  ObTableScanParam scan_param;
  scan_param.allocator_ = &allocator_;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_ctx;
  ObTableHandleV2 table_handle;
  ObIvfSSTableSearcher *searcher = nullptr;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  ASSERT_EQ(OB_SUCCESS,
      init_ivf_access_context(sstable, scan_param, store_ctx, access_ctx));
  ASSERT_EQ(OB_SUCCESS, table_handle.set_sstable_with_tablet(&sstable));

  const uint16_t unsupported_types[] = {
      share::VIAT_HNSW,
      share::VIAT_IVF_FLAT,
      share::VIAT_MAX,
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(unsupported_types); ++i) {
    sstable.ivf_meta_.index_type_ = unsupported_types[i];
    searcher = reinterpret_cast<ObIvfSSTableSearcher *>(1);
    ASSERT_EQ(OB_NOT_SUPPORTED,
        sstable.create_searcher(table_handle, scan_param, access_ctx, searcher));
    ASSERT_EQ(nullptr, searcher);
  }
}

TEST(ObIvfKnnSearchParamTest, disk_bbq_query_contract)
{
  ObIvfKnnSearchParam query_param;
  EXPECT_EQ(0, query_param.cluster_size_);
  float query[4] = {0.0F, 0.0F, 0.0F, 0.0F};
  query_param.query_ = query;
  query_param.dim_ = 4;
  query_param.nprobe_ = 1;
  query_param.k_ = 1;
  ASSERT_TRUE(query_param.is_valid());
  query_param.k_ = 0;
  ASSERT_FALSE(query_param.is_valid());
  query_param.cluster_size_ = 512;
  ObIvfKnnSearchParam copied_param = query_param;
  EXPECT_EQ(512, copied_param.cluster_size_);
  query_param.reset();
  EXPECT_EQ(0, query_param.cluster_size_);
}

TEST(ObDiskBBQDistanceBoundsTest, exact_distance_is_inside_rabitq_bounds)
{
  const int64_t dim = 64;
  float query[dim] = {0};
  float vector[dim] = {0};
  float centroid[dim] = {0};
  query[0] = 1.0F;
  query[1] = -0.5F;
  query[7] = 0.25F;
  // Keep the base vector non-unit and greater than one in norm so that the
  // cosine bound must scale by exactly ||query|| * ||base||.
  vector[0] = 7.5F;
  vector[2] = -2.5F;
  vector[9] = 5.0F;
  centroid[0] = 0.1F;
  centroid[3] = -0.2F;
  quantization::ObQuantizationParams params;
  params.dim_ = dim;
  params.bits_ = 1;
  const share::ObVectorIndexDistAlgorithm metrics[] = {
      share::VIDA_L2, share::VIDA_IP, share::VIDA_COS};
  for (int64_t i = 0; i < ARRAYSIZEOF(metrics); ++i) {
    SCOPED_TRACE(::testing::Message() << "metric=" << metrics[i]);
    ObArenaAllocator allocator("BBQBounds");
    quantization::ObQuantizationEncoder encoder;
    ASSERT_EQ(OB_SUCCESS, encoder.init(allocator,
        quantization::ObQuantizationType::RABITQ, metrics[i], params));
    std::vector<uint8_t> code(encoder.code_size());
    ASSERT_EQ(OB_SUCCESS, encoder.encode(
        vector, centroid, {code.data(), static_cast<int64_t>(code.size())}));
    quantization::ObQuantizationComputer rabitq_computer;
    ASSERT_EQ(OB_SUCCESS, rabitq_computer.init(allocator,
        quantization::ObQuantizationType::RABITQ, metrics[i], params,
        query, centroid));
    float estimate = 0.0F;
    float lower_bound = 0.0F;
    float upper_bound = 0.0F;
    ASSERT_EQ(OB_SUCCESS, rabitq_computer.compute_bounds(
        {code.data(), static_cast<int64_t>(code.size())},
        estimate, lower_bound, upper_bound));

    quantization::ObQuantizationComputer exact_computer;
    quantization::ObQuantizationParams fp32_params;
    fp32_params.dim_ = dim;
    ASSERT_EQ(OB_SUCCESS, exact_computer.init(allocator,
        quantization::ObQuantizationType::FP32, metrics[i], fp32_params,
        query, nullptr));
    float exact_distance = 0.0F;
    ASSERT_EQ(OB_SUCCESS, exact_computer.compute(
        {reinterpret_cast<uint8_t *>(vector), sizeof(vector)}, exact_distance));
    ASSERT_LE(lower_bound, estimate);
    ASSERT_LE(estimate, upper_bound);
    ASSERT_LE(lower_bound, exact_distance + 1e-5F);
    ASSERT_GE(upper_bound, exact_distance - 1e-5F);
  }
}

class AsyncIVFFilterEstimateRtDef : public sql::ObIDASSearchRtDef
{
public:
  AsyncIVFFilterEstimateRtDef()
    : ObIDASSearchRtDef(sql::DAS_OP_SCALAR_QUERY), rows_(0), calls_(0),
      tablet_id_(0), group_slot_(-2), index_id_(0), direct_(true) {}
  int generate_op(sql::ObDASSearchCost, sql::ObDASSearchCtx &, sql::ObIDASSearchOp *&) override
  { return OB_NOT_SUPPORTED; }
  int compute_cost(sql::ObDASSearchCtx &ctx, sql::ObDASSearchCost &cost) override
  {
    ++calls_;
    tablet_id_ = ctx.get_root_tablet_id().id();
    group_slot_ = ctx.get_async_group_slot();
    index_id_ = ctx.get_async_index_table_id();
    direct_ = ctx.get_async_direct_bind();
    if (children_cnt_ > 0) {
      return static_cast<sql::ObIDASSearchRtDef *>(children_[0])->get_cost(ctx, cost);
    }
    cost = sql::ObDASSearchCost(rows_);
    return OB_SUCCESS;
  }
  int64_t rows_;
  int64_t calls_;
  uint64_t tablet_id_;
  int64_t group_slot_;
  uint64_t index_id_;
  bool direct_;
};

TEST_F(TestIVFSSTable, async_ivf_partition_selectivity_refresh)
{
  sql::ObDASScanOp scan_op(allocator_);
  sql::ObDASSearchCtx ctx(allocator_, scan_op);
  sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
  sql::ObDASVecIndexDriverIter driver;
  sql::ObDASVecIndexAsyncIVFScanIter scan, pre, post;
  AsyncIVFFilterEstimateRtDef filter, leaf;
  sql::ObIDASSearchCtDef filter_ctdef(allocator_, sql::DAS_OP_SCALAR_QUERY);
  sql::ObDASBaseRtDef *children[] = {&leaf};
  leaf.ctdef_ = &filter_ctdef;
  filter.children_ = children;
  filter.children_cnt_ = 1;
  driver.vec_index_scan_iter_ = &scan;
  driver.search_ctx_ = &ctx;
  driver.vec_index_driver_ctdef_ = &ctdef;
  driver.filter_rtdef_for_reeval_ = &filter;
  driver.pre_filter_iter_ = &pre;
  driver.post_filter_iter_ = &post;
  ctdef.row_count_ = 10000000; // Deliberately differs from every partition estimate.
  const int64_t partition_rows[] = {500000, 1000000, 1000000, 1000000};
  const int64_t filter_rows[] = {250000, 950000, 200000, 5000};
  const ObVecIndexType paths[] = {VEC_INDEX_POST_ITERATIVE_FILTER,
      VEC_INDEX_POST_WITHOUT_FILTER, VEC_INDEX_PRE, VEC_INDEX_PRE};
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(i);
    const ObTabletID tablet(200100 + i);
    scan_op.get_scan_param().tablet_id_ = tablet;
    ctx.table_row_count_ = sql::ObDASSearchCost(partition_rows[i]);
    ctx.row_count_tablet_id_ = tablet;
    // Simulate a cached filter cost from a previous Group / parameter binding.
    ctx.set_async_binding(500123, i, true);
    filter.cost_ = sql::ObDASSearchCost(7);
    leaf.cost_ = sql::ObDASSearchCost(3);
    leaf.rows_ = filter_rows[i];
    scan.set_selectivity(0.1);
    ASSERT_EQ(OB_SUCCESS, driver.evaluate_partition_path());
    EXPECT_DOUBLE_EQ(static_cast<double>(filter_rows[i]) / partition_rows[i], scan.selectivity_);
    EXPECT_EQ(partition_rows[i], driver.last_partition_row_count_);
    EXPECT_EQ(filter_rows[i], driver.last_filter_est_row_count_);
    EXPECT_EQ(paths[i], driver.vec_index_type_);
    EXPECT_EQ(i == 3, driver.go_brute_force_);
    EXPECT_EQ(i + 1, leaf.calls_);
    EXPECT_EQ(tablet.id(), leaf.tablet_id_);
    EXPECT_EQ(-1, leaf.group_slot_);
    EXPECT_EQ(OB_INVALID_ID, leaf.index_id_);
    EXPECT_FALSE(leaf.direct_);
    EXPECT_EQ(i, ctx.get_async_group_slot());
    EXPECT_EQ(500123, ctx.get_async_index_table_id());
    EXPECT_TRUE(ctx.get_async_direct_bind());
  }
  // Same Tablet, changed binding/parameter: the numerator must be recomputed.
  leaf.rows_ = 600000;
  ASSERT_EQ(OB_SUCCESS, driver.rescan());
  EXPECT_DOUBLE_EQ(0.6, scan.selectivity_);
  EXPECT_EQ(5, leaf.calls_);
  EXPECT_EQ(VEC_INDEX_POST_ITERATIVE_FILTER, driver.vec_index_type_);
  // Do not leave borrowed stack children for rtdef cleanup.
  filter.children_ = nullptr;
  filter.children_cnt_ = 0;
}

TEST_F(TestIVFSSTable, async_ivf_unknown_selectivity_keeps_search_enabled)
{
  sql::ObDASScanOp scan_op(allocator_);
  sql::ObDASSearchCtx ctx(allocator_, scan_op);
  sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
  sql::ObDASVecIndexDriverIter driver;
  sql::ObDASVecIndexAsyncIVFScanIter scan, post;
  AsyncIVFFilterEstimateRtDef filter;
  driver.vec_index_scan_iter_ = &scan;
  driver.search_ctx_ = &ctx;
  driver.vec_index_driver_ctdef_ = &ctdef;
  driver.filter_rtdef_for_reeval_ = &filter;
  driver.post_filter_iter_ = &post;
  driver.filter_iter_ = &post;
  ctdef.vec_type_ = VEC_INDEX_POST_ITERATIVE_FILTER;
  ctdef.filter_mode_ = ObKnnFilterMode::POST_INDEX_MERGE;
  const ObTabletID tablet(200200);
  scan_op.get_scan_param().tablet_id_ = tablet;
  ctx.row_count_tablet_id_ = tablet;
  const int64_t totals[] = {100000, 0, 100000};
  const int64_t hits[] = {0, 10, 120000};
  for (int i = 0; i < 3; ++i) {
    SCOPED_TRACE(i);
    ctx.table_row_count_ = sql::ObDASSearchCost(totals[i]);
    filter.rows_ = hits[i];
    scan.set_selectivity(0.8);
    ASSERT_EQ(OB_SUCCESS, driver.evaluate_partition_path());
    EXPECT_DOUBLE_EQ(0.1, scan.selectivity_);
    EXPECT_EQ(VEC_INDEX_POST_ITERATIVE_FILTER, driver.vec_index_type_);
    EXPECT_FALSE(driver.ready_to_output_);
    EXPECT_FALSE(scan.round_ready_);
    EXPECT_EQ(100, sql::ObDasVecScanUtils::calc_heap_size(10, scan.selectivity_));
  }
}


TEST_F(TestIVFSSTable, async_ivf_factory_rejects_missing_eval_context)
{
  sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
  sql::ObDASVecIndexDriverRtDef rtdef;
  sql::ObDASIter *iter = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, sql::ObDASIterUtils::create_vec_search_iter(
      allocator_, nullptr, ObLSID(1), nullptr, nullptr, nullptr, &ctdef, &rtdef, iter));
  EXPECT_EQ(nullptr, iter);
}

TEST_F(TestIVFSSTable, async_ivf_auto_rescan_preserves_post_filter_definitions)
{
  for (bool expression_filter : {false, true}) {
    SCOPED_TRACE(expression_filter);
    sql::ObDASScanOp scan_op(allocator_);
    sql::ObDASSearchCtx ctx(allocator_, scan_op);
    sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
    sql::ObDASScalarScanCtDef scalar_ctdef(allocator_);
    sql::ObDASScalarScanRtDef scalar_rtdef;
    sql::ObDASVecIndexDriverIter driver;
    sql::ObDASVecIndexAsyncIVFScanIter scan, pre, post;
    AsyncIVFFilterEstimateRtDef filter;
    driver.vec_index_scan_iter_ = &scan;
    driver.search_ctx_ = &ctx;
    driver.vec_index_driver_ctdef_ = &ctdef;
    driver.filter_rtdef_for_reeval_ = &filter;
    driver.pre_filter_iter_ = &pre;
    driver.post_filter_iter_ = &post;
    driver.scalar_scan_ctdef_ = expression_filter ? &scalar_ctdef : nullptr;
    driver.scalar_scan_rtdef_ = expression_filter ? &scalar_rtdef : nullptr;
    const int64_t hits[] = {200000, 600000, 100000};
    for (int i = 0; i < 3; ++i) {
      SCOPED_TRACE(i);
      const ObTabletID tablet(200300 + i);
      scan_op.get_scan_param().tablet_id_ = tablet;
      ctx.row_count_tablet_id_ = tablet;
      ctx.table_row_count_ = sql::ObDASSearchCost(1000000);
      filter.rows_ = hits[i];
      ASSERT_EQ(OB_SUCCESS, driver.rescan());
      const bool is_pre = i != 1;
      EXPECT_EQ(is_pre ? VEC_INDEX_PRE : VEC_INDEX_POST_ITERATIVE_FILTER, driver.vec_index_type_);
      EXPECT_EQ(is_pre ? &pre : &post, driver.filter_iter_);
      EXPECT_EQ(is_pre ? sql::ObVecFilterMode::VEC_FILTER_MODE_PRE_FILTER
          : expression_filter ? sql::ObVecFilterMode::VEC_FILTER_MODE_EXPR_FILTER
                              : sql::ObVecFilterMode::VEC_FILTER_MODE_SEARCH_DRIVER_FILTER,
          driver.filter_mode_);
      EXPECT_EQ(expression_filter ? &scalar_ctdef : nullptr, driver.scalar_scan_ctdef_);
      EXPECT_EQ(expression_filter ? &scalar_rtdef : nullptr, driver.scalar_scan_rtdef_);
      EXPECT_DOUBLE_EQ(static_cast<double>(hits[i]) / 1000000, scan.selectivity_);
      EXPECT_EQ(driver.vec_index_type_, scan.vec_index_type_);
      EXPECT_FALSE(driver.go_brute_force_);
      EXPECT_EQ(i + 1, filter.calls_);
    }
  }
}

TEST_F(TestIVFSSTable, async_ivf_prefilter_uses_plan_estimate_without_changing_path)
{
  sql::ObDASVecAuxScanCtDef ctdef(allocator_);
  sql::ObDASIVFAsyncIter scan;
  scan.vec_aux_ctdef_ = &ctdef;
  scan.vec_index_type_ = VEC_INDEX_PRE;
  scan.vec_idx_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER;
  ctdef.row_count_ = 10000000;
  ctdef.selectivity_ = 0.1;
  scan.adaptive_ctx_.selectivity_ = 1.0; // A range estimate must not replace the predicate estimate.
  EXPECT_EQ(1000000, scan.get_pre_filter_estimated_rows());
  EXPECT_EQ(VEC_INDEX_PRE, scan.vec_index_type_);
  EXPECT_EQ(ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER, scan.vec_idx_try_path_);
  ctdef.selectivity_ = 0;
  EXPECT_EQ(0, scan.get_pre_filter_estimated_rows());
  ctdef.row_count_ = 33;
  ctdef.selectivity_ = 0.1;
  EXPECT_EQ(4, scan.get_pre_filter_estimated_rows());
  for (const double value : {-1.0, 1.1, std::numeric_limits<double>::quiet_NaN(),
                             std::numeric_limits<double>::infinity()}) {
    ctdef.selectivity_ = value;
    EXPECT_EQ(-1, scan.get_pre_filter_estimated_rows());
  }
  ctdef.selectivity_ = 1;
  ctdef.row_count_ = static_cast<double>(INT64_MAX);
  EXPECT_EQ(-1, scan.get_pre_filter_estimated_rows());
  ctdef.row_count_ = 0;
  EXPECT_EQ(-1, scan.get_pre_filter_estimated_rows());
  scan.vec_aux_ctdef_ = nullptr;
  EXPECT_EQ(-1, scan.get_pre_filter_estimated_rows());
}

TEST_F(TestIVFSSTable, async_ivf_prefilter_partition_range_estimate_excludes_residual_filters)
{
  sql::ObDASVecAuxScanCtDef ctdef(allocator_);
  sql::ObDASScanCtDef input_ctdef(allocator_);
  sql::ObDASBaseCtDef *children[] = {&input_ctdef};
  ctdef.children_ = children;
  ctdef.children_cnt_ = 1;
  ctdef.row_count_ = 10000000;
  ctdef.selectivity_ = 0.1;
  sql::ObDASScanIter input;
  sql::ObDASIVFAsyncIter scan;
  scan.vec_aux_ctdef_ = &ctdef;
  scan.inv_idx_scan_iter_ = &input;
  scan.profile_est_total_rows_ = 500000;
  scan.profile_est_rows_ = 80000;
  EXPECT_EQ(80000, scan.get_pre_filter_estimated_rows());
  sql::ObExpr predicate;
  ASSERT_EQ(OB_SUCCESS, input_ctdef.pd_expr_spec_.pushdown_filters_.init(1));
  ASSERT_EQ(OB_SUCCESS, input_ctdef.pd_expr_spec_.pushdown_filters_.push_back(&predicate));
  EXPECT_EQ(50000, scan.get_pre_filter_estimated_rows());
  input_ctdef.pd_expr_spec_.pushdown_filters_.reuse();
  ASSERT_EQ(OB_SUCCESS, input_ctdef.logical_residual_filters_.init(1));
  ASSERT_EQ(OB_SUCCESS, input_ctdef.logical_residual_filters_.push_back(&predicate));
  EXPECT_EQ(50000, scan.get_pre_filter_estimated_rows());
  input_ctdef.logical_residual_filters_.reuse();
  scan.profile_est_total_rows_ = 1000000;
  scan.profile_est_rows_ = 0;
  EXPECT_EQ(0, scan.get_pre_filter_estimated_rows());
  scan.profile_est_rows_ = 1000001;
  EXPECT_EQ(100000, scan.get_pre_filter_estimated_rows());
  ASSERT_EQ(OB_SUCCESS, scan.reset_partition_filter_path());
  EXPECT_EQ(1000000, scan.get_pre_filter_estimated_rows());
  ctdef.children_ = nullptr;
  ctdef.children_cnt_ = 0;
}

TEST_F(TestIVFSSTable, async_ivf_fixed_pre_rescan_without_cost_definition)
{
  for (const auto mode : {ObKnnFilterMode::PRE_KNN, ObKnnFilterMode::PRE_BRUTE_FORCE}) {
    sql::ObDASScanOp scan_op(allocator_);
    sql::ObDASSearchCtx ctx(allocator_, scan_op);
    sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
    sql::ObDASVecIndexDriverIter driver;
    sql::ObDASVecIndexAsyncIVFScanIter scan, pre;
    driver.vec_index_scan_iter_ = &scan;
    driver.search_ctx_ = &ctx;
    driver.vec_index_driver_ctdef_ = &ctdef;
    driver.pre_filter_iter_ = &pre;
    ctdef.vec_type_ = VEC_INDEX_PRE;
    ctdef.filter_mode_ = mode;
    for (int i = 0; i < 2; ++i) {
      driver.last_partition_row_count_ = 123;
      driver.last_filter_est_row_count_ = 456;
      driver.go_brute_force_ = mode != ObKnnFilterMode::PRE_BRUTE_FORCE;
      scan.set_selectivity(0.01);
      ASSERT_EQ(OB_SUCCESS, driver.rescan());
      EXPECT_EQ(VEC_INDEX_PRE, driver.vec_index_type_);
      EXPECT_EQ(VEC_INDEX_PRE, scan.vec_index_type_);
      EXPECT_EQ(&pre, driver.filter_iter_);
      EXPECT_EQ(mode == ObKnnFilterMode::PRE_BRUTE_FORCE, driver.go_brute_force_);
      EXPECT_DOUBLE_EQ(1.0, scan.selectivity_);
      EXPECT_EQ(0, driver.last_partition_row_count_);
      EXPECT_EQ(0, driver.last_filter_est_row_count_);
    }
    // A mode alone is not a filter; clear state left by the previous partition.
    driver.pre_filter_iter_ = nullptr;
    ASSERT_EQ(OB_SUCCESS, driver.rescan());
    EXPECT_EQ(VEC_INDEX_POST_WITHOUT_FILTER, driver.vec_index_type_);
    EXPECT_EQ(nullptr, driver.filter_iter_);
    EXPECT_FALSE(driver.go_brute_force_);
    EXPECT_DOUBLE_EQ(1.0, scan.selectivity_);
  }
}

TEST_F(TestIVFSSTable, async_ivf_explicit_post_retains_iterative_path)
{
  for (const auto mode : {ObKnnFilterMode::POST_FILTER, ObKnnFilterMode::POST_INDEX_MERGE}) {
    sql::ObDASScanOp scan_op(allocator_);
    sql::ObDASSearchCtx ctx(allocator_, scan_op);
    sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
    sql::ObDASVecIndexDriverIter driver;
    sql::ObDASVecIndexAsyncIVFScanIter scan, post;
    AsyncIVFFilterEstimateRtDef filter;
    driver.vec_index_scan_iter_ = &scan;
    driver.search_ctx_ = &ctx;
    driver.vec_index_driver_ctdef_ = &ctdef;
    driver.filter_rtdef_for_reeval_ = &filter;
    driver.post_filter_iter_ = &post;
    ctdef.vec_type_ = VEC_INDEX_POST_ITERATIVE_FILTER;
    ctdef.filter_mode_ = mode;
    const ObTabletID tablet(200400);
    scan_op.get_scan_param().tablet_id_ = tablet;
    ctx.row_count_tablet_id_ = tablet;
    ctx.table_row_count_ = sql::ObDASSearchCost(1000000);
    for (const int64_t hits : {1000, 950000}) {
      filter.rows_ = hits;
      ASSERT_EQ(OB_SUCCESS, driver.rescan());
      EXPECT_EQ(VEC_INDEX_POST_ITERATIVE_FILTER, driver.vec_index_type_);
      EXPECT_EQ(VEC_INDEX_POST_ITERATIVE_FILTER, scan.vec_index_type_);
      EXPECT_EQ(&post, driver.filter_iter_);
      EXPECT_FALSE(driver.go_brute_force_);
    }
    // An invalid auto estimate must also keep recall enabled.
    ctdef.vec_type_ = VEC_INDEX_INVALID;
    ctx.table_row_count_ = sql::ObDASSearchCost(0);
    ASSERT_EQ(OB_SUCCESS, driver.rescan());
    EXPECT_EQ(VEC_INDEX_POST_ITERATIVE_FILTER, driver.vec_index_type_);
    EXPECT_DOUBLE_EQ(0.1, scan.selectivity_);
  }
}


class AsyncIVFExprEmptyFilterIter : public sql::ObDASIter
{
public:
  AsyncIVFExprEmptyFilterIter() : terminal_ret_(OB_ITER_END), calls_(0)
  { inited_ = true; }
  int inner_init(sql::ObDASIterParam &) override { return OB_SUCCESS; }
  int inner_reuse() override { return OB_SUCCESS; }
  int inner_release() override { return OB_SUCCESS; }
  int rescan() override { return OB_SUCCESS; }
  int inner_get_next_row() override { ++calls_; return terminal_ret_; }
  int inner_get_next_rows(int64_t &count, int64_t) override
  { ++calls_; count = 0; return terminal_ret_; }
  int terminal_ret_;
  int64_t calls_;
};

class AsyncIVFExprTestDriver : public sql::ObDASVecIndexDriverIter
{
public:
  ~AsyncIVFExprTestDriver() { inner_release(); }
};

TEST_F(TestIVFSSTable, async_ivf_expr_round_buffers_are_bounded_and_reset)
{
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
  sql::ObDASVecIndexDriverRtDef rtdef;
  rtdef.eval_ctx_ = &eval_ctx;
  sql::ObDASScalarScanCtDef scalar_ctdef(allocator_);
  scalar_ctdef.ref_table_id_ = 200001;
  sql::ObDASScalarScanRtDef scalar_rtdef;
  sql::ObExpr vid_expr;
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.push_back(&vid_expr));
  sql::ObDASVecIndexAsyncIVFScanIter scan;
  scan.inited_ = true;
  AsyncIVFExprEmptyFilterIter filter;
  AsyncIVFExprTestDriver driver;
  driver.vec_index_scan_iter_ = &scan;
  driver.vec_index_driver_ctdef_ = &ctdef;
  driver.vec_index_driver_rtdef_ = &rtdef;
  driver.scalar_scan_ctdef_ = &scalar_ctdef;
  driver.scalar_scan_rtdef_ = &scalar_rtdef;
  driver.filter_iter_ = &filter;
  driver.post_filter_iter_ = &filter;
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), "IVFExprTest", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS, CURRENT_CONTEXT->CREATE_CONTEXT(driver.mem_context_, param));
  ObSEArray<sql::VidDistancePair, 4> accepted;
  for (int partition = 0; partition < 2; ++partition) {
    // The scan owns and destroys its result iterator on reuse.
    void *buf = scan.alloc_.alloc(sizeof(share::ObVectorQueryVidIterator));
    ASSERT_NE(nullptr, buf);
    scan.vid_iter_ = new (buf) share::ObVectorQueryVidIterator(0, 0, 0, nullptr);
    ASSERT_EQ(OB_SUCCESS, scan.vid_iter_->init(2, &scan.alloc_));
    auto &arena = driver.mem_context_->get_arena_allocator();
    const int64_t empty_used = arena.used();
    ASSERT_EQ(OB_SUCCESS, driver.do_expr_filter_batch(&accepted));
    EXPECT_EQ(empty_used, arena.used());
    EXPECT_EQ(nullptr, driver.expr_filter_vid_objs_);
    EXPECT_EQ(nullptr, driver.expr_filter_matched_flags_);
    int64_t warm_used = -1;
    ObObj *vid_buffer = nullptr;
    bool *flag_buffer = nullptr;
    for (int round = 0; round < 64; ++round) {
      SCOPED_TRACE(partition);
      SCOPED_TRACE(round);
      ASSERT_EQ(OB_SUCCESS, scan.vid_iter_->add_result(round * 2 + 7, 1.0F, nullptr));
      ASSERT_EQ(OB_SUCCESS, scan.vid_iter_->add_result(round * 2 + 8, 2.0F, nullptr));
      ASSERT_EQ(OB_SUCCESS, driver.do_expr_filter_batch(&accepted));
      EXPECT_EQ(0, accepted.count()); // Low-selectivity POST keeps refilling.
      if (0 == round) {
        warm_used = arena.used();
        vid_buffer = driver.expr_filter_vid_objs_;
        flag_buffer = driver.expr_filter_matched_flags_;
        ASSERT_NE(nullptr, vid_buffer);
        ASSERT_NE(nullptr, flag_buffer);
      }
      EXPECT_EQ(warm_used, arena.used());
      EXPECT_EQ(vid_buffer, driver.expr_filter_vid_objs_);
      EXPECT_EQ(flag_buffer, driver.expr_filter_matched_flags_);
      scan.vid_iter_->set_total(0); // SOAR dedup can produce empty middle rounds.
      const int64_t calls = filter.calls_;
      ASSERT_EQ(OB_SUCCESS, driver.do_expr_filter_batch(&accepted));
      EXPECT_EQ(calls, filter.calls_);
      EXPECT_EQ(warm_used, arena.used());
    }
    ASSERT_EQ(OB_SUCCESS, scan.vid_iter_->add_result(999, 1.0F, nullptr));
    filter.terminal_ret_ = OB_TIMEOUT;
    EXPECT_EQ(OB_TIMEOUT, driver.do_expr_filter_batch(&accepted));
    EXPECT_EQ(warm_used, arena.used());
    filter.terminal_ret_ = OB_ITER_END;
    ASSERT_EQ(OB_SUCCESS, driver.inner_reuse());
    EXPECT_EQ(nullptr, driver.expr_filter_vid_objs_);
    EXPECT_EQ(nullptr, driver.expr_filter_matched_flags_);
    EXPECT_EQ(0, scalar_rtdef.key_ranges_.count());
  }
  ASSERT_EQ(OB_SUCCESS, driver.inner_release());
  EXPECT_EQ(nullptr, driver.mem_context_);
}

class AsyncIVFPreFilterRowsIter : public sql::ObDASIter
{
public:
  explicit AsyncIVFPreFilterRowsIter(ObDatum *datums)
    : datums_(datums), remaining_(0), terminal_ret_(OB_ITER_END), calls_(0)
  {
    inited_ = true;
  }
  int inner_init(sql::ObDASIterParam &) override { return OB_SUCCESS; }
  int inner_reuse() override { return OB_SUCCESS; }
  int inner_release() override { return OB_SUCCESS; }
  int inner_get_next_row() override { return OB_NOT_SUPPORTED; }
  int inner_get_next_rows(int64_t &count, int64_t capacity) override
  {
    ++calls_;
    count = std::min(remaining_, capacity);
    for (int64_t i = 0; i < count; ++i) {
      datums_[i].set_uint(7); // Duplicate VIDs exercise insertion-count semantics.
    }
    remaining_ -= count;
    return count > 0 ? OB_SUCCESS : terminal_ret_;
  }
  ObDatum *datums_;
  int64_t remaining_;
  int terminal_ret_;
  int64_t calls_;
};

class AsyncIVFDatumFilterIter : public sql::ObDASIter
{
public:
  AsyncIVFDatumFilterIter(ObDatum *datums, uint64_t *buffers, const std::vector<uint64_t> &values)
    : datums_(datums), buffers_(buffers), values_(values), pos_(0), terminal_ret_(OB_ITER_END),
      end_with_rows_(false), empty_batch_(false), invalid_pos_(-1), invalid_kind_(0)
  {
    inited_ = true;
    max_size_ = 128;
  }
  int inner_init(sql::ObDASIterParam &) override { return OB_SUCCESS; }
  int inner_reuse() override { return OB_SUCCESS; }
  int inner_release() override { return OB_SUCCESS; }
  int inner_get_next_row() override
  {
    int ret = terminal_ret_;
    if (pos_ < values_.size()) {
      fill_datum(0);
      ret = OB_SUCCESS;
    }
    return ret;
  }
  int inner_get_next_rows(int64_t &count, int64_t capacity) override
  {
    count = 0;
    int ret = OB_SUCCESS;
    if (empty_batch_) {
      empty_batch_ = false;
    } else if (pos_ == values_.size()) {
      ret = terminal_ret_;
    } else {
      count = std::min<int64_t>(capacity, values_.size() - pos_);
      for (int64_t i = 0; i < count; ++i) {
        fill_datum(i);
      }
      if (end_with_rows_ && pos_ == values_.size() && OB_ITER_END == terminal_ret_) {
        ret = OB_ITER_END;
      }
    }
    return ret;
  }
  void fill_datum(int64_t idx)
  {
    // Reuse the same datum buffers on every fetch, as the storage iterator does.
    datums_[idx].ptr_ = reinterpret_cast<char *>(&buffers_[idx]);
    datums_[idx].set_uint(values_[pos_]);
    if (pos_ == invalid_pos_) {
      if (invalid_kind_ == 0) {
        datums_[idx].set_null();
      } else if (invalid_kind_ == 1) {
        datums_[idx].ptr_ = nullptr;
      } else {
        datums_[idx].len_ = 4;
      }
    }
    ++pos_;
  }
  ObDatum *datums_;
  uint64_t *buffers_;
  const std::vector<uint64_t> &values_;
  int64_t pos_;
  int terminal_ret_;
  bool end_with_rows_;
  bool empty_batch_;
  int64_t invalid_pos_;
  int invalid_kind_;
};

TEST_F(TestIVFSSTable, async_ivf_prefilter_materializes_vid_datums)
{
  enum Scenario {
    EMPTY, DUPLICATES, LIMITS, BROADCAST, NULL_VID, NEGATIVE_VID, OVERFLOW_VID,
    NULL_BUFFER, SHORT_DATUM, WRONG_TYPE, SCAN_ERROR, LATENCY_RETRY, RECALL_RETRY,
    MISSING_ROWKEY, SCENARIO_COUNT
  };
  for (bool vectorized : {false, true}) {
    for (bool end_with_rows : {false, true}) {
      for (const ObObjType type : {ObIntType, ObUInt64Type}) {
        for (int scenario = EMPTY; scenario < SCENARIO_COUNT; ++scenario) {
          SCOPED_TRACE(::testing::Message() << "vectorized=" << vectorized
              << " end_with_rows=" << end_with_rows << " type=" << type << " scenario=" << scenario);
          sql::ObExecContext exec_ctx(allocator_);
          sql::ObEvalCtx eval_ctx(exec_ctx);
          ObDatum datums[128];
          uint64_t buffers[128] = {};
          char *frames[] = {reinterpret_cast<char *>(datums)};
          eval_ctx.frames_ = frames;
          eval_ctx.max_batch_size_ = 128;
          sql::ObExpr vid;
          vid.frame_idx_ = 0;
          vid.datum_off_ = 0;
          vid.batch_idx_mask_ = vectorized && scenario != BROADCAST ? UINT64_MAX : 0;
          vid.datum_meta_.type_ = scenario == WRONG_TYPE ? ObVarcharType : type;
          std::vector<uint64_t> values(scenario == EMPTY ? 0 : 600);
          for (int64_t i = 0; i < values.size(); ++i) {
            values[i] = scenario == BROADCAST ? 7 : i % 73;
          }
          if (scenario == LIMITS) {
            values = {0, uint64_t(INT64_MAX), 0};
          } else if (scenario == NEGATIVE_VID || scenario == OVERFLOW_VID) {
            values[300] = scenario == NEGATIVE_VID ? UINT64_MAX : uint64_t(INT64_MAX) + 1;
          }
          AsyncIVFDatumFilterIter input(datums, buffers, values);
          input.end_with_rows_ = end_with_rows;
          input.empty_batch_ = true;
          if (scenario == NULL_VID || scenario == NULL_BUFFER || scenario == SHORT_DATUM) {
            input.invalid_pos_ = 300;
            input.invalid_kind_ = scenario == NULL_VID ? 0 : scenario == NULL_BUFFER ? 1 : 2;
          }
          input.terminal_ret_ = scenario == SCAN_ERROR ? OB_TIMEOUT
              : scenario == LATENCY_RETRY || scenario == RECALL_RETRY
                  ? OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY : OB_ITER_END;
          sql::ObDASScanCtDef scan_ctdef(allocator_);
          sql::ObDASScanRtDef scan_rtdef;
          scan_rtdef.eval_ctx_ = &eval_ctx;
          scan_ctdef.ir_scan_type_ = sql::ObTSCIRScanType::OB_VEC_ASYNC_IVF_INDEX_SCAN;
          ASSERT_EQ(OB_SUCCESS, scan_ctdef.rowkey_exprs_.init(1));
          if (scenario != MISSING_ROWKEY) {
            ASSERT_EQ(OB_SUCCESS, scan_ctdef.rowkey_exprs_.push_back(&vid));
          }
          sql::ObDASVecAuxScanCtDef ctdef(allocator_);
          sql::ObDASVecAuxScanRtDef rtdef;
          const int64_t child_count = static_cast<int64_t>(ObVecAuxTableIdx::FIRST_VEC_AUX_TBL_IDX) + 1;
          sql::ObDASBaseCtDef *ctdefs[child_count] = {};
          sql::ObDASBaseRtDef *rtdefs[child_count] = {};
          ctdefs[child_count - 1] = &scan_ctdef;
          rtdefs[child_count - 1] = &scan_rtdef;
          ctdef.is_async_index_ = true;
          ctdef.children_ = ctdefs;
          ctdef.children_cnt_ = child_count;
          rtdef.ctdef_ = &ctdef;
          rtdef.children_ = rtdefs;
          rtdef.children_cnt_ = child_count;
          rtdef.eval_ctx_ = &eval_ctx;
          ASSERT_EQ(OB_SUCCESS, ctdef.result_output_.init(1));
          ASSERT_EQ(OB_SUCCESS, ctdef.result_output_.push_back(&vid));
          sql::ObDASIVFAsyncIter scan;
          scan.vec_aux_ctdef_ = &ctdef;
          scan.vec_aux_rtdef_ = &rtdef;
          scan.inv_idx_scan_iter_ = &input;
          scan.strategy_ = scenario == LATENCY_RETRY ? ObVecIdxQueryStrategy::LATENCY_FIRST
                                                     : ObVecIdxQueryStrategy::RECALL_FIRST;
          ObIvfPreFilterBitmap bitmap(allocator_);
          const int expected = scenario == SCAN_ERROR ? OB_TIMEOUT
              : scenario == RECALL_RETRY ? OB_VECTOR_INDEX_ADAPTIVE_NEED_RETRY
              : (scenario >= NULL_VID && scenario <= WRONG_TYPE) || scenario == MISSING_ROWKEY
                  ? OB_ERR_UNEXPECTED : OB_SUCCESS;
          ASSERT_EQ(expected, scan.materialize_pre_filter(vectorized, bitmap));
          EXPECT_EQ(0, scan.pre_fileter_rowkeys_.count());
          if (OB_SUCCESS == expected) {
            const std::set<uint64_t> unique(values.begin(), values.end());
            EXPECT_EQ(unique.size(), bitmap.get_cardinality());
            EXPECT_EQ(unique.size(), scan.adaptive_ctx_.pre_scan_row_cnt_);
            for (uint64_t value : unique) {
              EXPECT_TRUE(bitmap.is_contains(value));
            }
          } else {
            EXPECT_EQ(0, bitmap.get_cardinality());
            EXPECT_EQ(0, scan.adaptive_ctx_.pre_scan_row_cnt_);
          }
          eval_ctx.frames_ = nullptr;
        }
      }
    }
  }
}

TEST_F(TestIVFSSTable, async_ivf_partition_pre_failure_rolls_back_binding)
{
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  ObDatum datums[128];
  uint64_t values[128] = {};
  for (int64_t i = 0; i < 128; ++i) {
    datums[i].ptr_ = reinterpret_cast<char *>(&values[i]);
  }
  char *frames[] = {reinterpret_cast<char *>(datums)};
  eval_ctx.frames_ = frames;
  eval_ctx.max_batch_size_ = 128;
  sql::ObExpr vid;
  vid.frame_idx_ = 0;
  vid.datum_off_ = 0;
  vid.vector_header_off_ = UINT32_MAX;
  sql::ExprFixedArray rowids(allocator_);
  ASSERT_EQ(OB_SUCCESS, rowids.init(1));
  ASSERT_EQ(OB_SUCCESS, rowids.push_back(&vid));
  sql::ObDASScanOp scan_op(allocator_);
  sql::ObDASSearchCtx ctx(allocator_, scan_op);
  ctx.rowid_exprs_ = &rowids;
  sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
  sql::ObDASVecIndexDriverRtDef rtdef;
  rtdef.eval_ctx_ = &eval_ctx;
  sql::ObDASVecIndexDriverIter driver;
  sql::ObDASVecIndexAsyncIVFScanIter scan;
  AsyncIVFPreFilterRowsIter filter(datums);
  scan.vec_index_type_ = VEC_INDEX_PRE;
  driver.vec_index_scan_iter_ = &scan;
  driver.vec_index_driver_ctdef_ = &ctdef;
  driver.vec_index_driver_rtdef_ = &rtdef;
  driver.search_ctx_ = &ctx;
  driver.filter_iter_ = &filter;
  ObRbMemMgr *mgr = MTL(ObRbMemMgr *);
  ASSERT_NE(nullptr, mgr);
  const int64_t before = mgr->get_vec_idx_used();
  for (int i = 0; i < 2; ++i) {
    filter.remaining_ = 257; // Multiple successful batches before the error.
    filter.terminal_ret_ = OB_ALLOCATE_MEMORY_FAILED;
    int64_t count = 99;
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, driver.build_bitmap_from_filter_iter(count));
    EXPECT_EQ(0, count);
    EXPECT_FALSE(scan.pre_filter_.is_valid());
    EXPECT_EQ(0, scan.pre_filter_bitmap_.get_cardinality());
    EXPECT_FALSE(scan.opened_);
    EXPECT_EQ(before, mgr->get_vec_idx_used());
    ASSERT_EQ(OB_SUCCESS, scan.abort_partition_pre_filter_build());
    ASSERT_EQ(OB_SUCCESS, scan.rescan());
    EXPECT_EQ(before, mgr->get_vec_idx_used());
  }
  EXPECT_GE(filter.calls_, 8);
  filter.remaining_ = 1;
  int64_t count = 0;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, driver.process_pre_filter_mode(count, 128));
  EXPECT_FALSE(scan.pre_filter_.is_valid());
  filter.remaining_ = 0;
  filter.terminal_ret_ = OB_ITER_END;
  EXPECT_EQ(OB_ITER_END, driver.process_pre_filter_mode(count, 128));
  EXPECT_FALSE(scan.opened_); // A completed empty filter never opens the scanner.
  ASSERT_EQ(OB_SUCCESS, scan.inner_reuse());
  EXPECT_EQ(before, mgr->get_vec_idx_used());
  eval_ctx.frames_ = nullptr;
}

TEST_F(TestIVFSSTable, async_ivf_partition_pre_uses_distinct_cardinality)
{
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  // Exceed the driver's 256-element inline PRE buffer in a single batch.
  ObDatum datums[1000];
  uint64_t values[1000] = {};
  for (int64_t i = 0; i < 1000; ++i) {
    datums[i].ptr_ = reinterpret_cast<char *>(&values[i]);
  }
  char *frames[] = {reinterpret_cast<char *>(datums)};
  eval_ctx.frames_ = frames;
  eval_ctx.max_batch_size_ = 1000;
  sql::ObExpr vid;
  vid.frame_idx_ = 0;
  vid.datum_off_ = 0;
  vid.vector_header_off_ = UINT32_MAX;
  sql::ExprFixedArray rowids(allocator_);
  ASSERT_EQ(OB_SUCCESS, rowids.init(1));
  ASSERT_EQ(OB_SUCCESS, rowids.push_back(&vid));
  sql::ObDASScanOp scan_op(allocator_);
  sql::ObDASSearchCtx ctx(allocator_, scan_op);
  ctx.rowid_exprs_ = &rowids;
  sql::ObDASVecIndexDriverCtDef ctdef(allocator_);
  sql::ObDASVecIndexDriverRtDef rtdef;
  rtdef.eval_ctx_ = &eval_ctx;
  sql::ObDASVecIndexDriverIter driver;
  sql::ObDASVecIndexAsyncIVFScanIter scan;
  AsyncIVFPreFilterRowsIter filter(datums);
  scan.vec_index_type_ = VEC_INDEX_PRE;
  driver.vec_index_scan_iter_ = &scan;
  driver.vec_index_driver_ctdef_ = &ctdef;
  driver.vec_index_driver_rtdef_ = &rtdef;
  driver.search_ctx_ = &ctx;
  driver.filter_iter_ = &filter;
  const int64_t threshold = sql::ObDASVecIndexIVFBaseScanIter::MAX_IVF_BRUTE_FORCE_SIZE;
  for (const int64_t rows : {threshold, threshold + 1}) {
    filter.remaining_ = rows;
    int64_t allowed_count = 0;
    ASSERT_EQ(OB_SUCCESS, driver.build_bitmap_from_filter_iter(allowed_count));
    ASSERT_EQ(1, allowed_count);
    EXPECT_EQ(1, scan.pre_filter_bitmap_.get_cardinality());
    for (const auto mode : {ObKnnFilterMode::PRE_KNN, ObKnnFilterMode::PRE_BRUTE_FORCE,
                           ObKnnFilterMode::PRE_ADAPTIVE}) {
      ctdef.filter_mode_ = mode;
      driver.update_async_ivf_go_brute_force_after_pre_filter(allowed_count);
      EXPECT_TRUE(driver.go_brute_force_);
      EXPECT_TRUE(scan.go_brute_force_);
    }
    ASSERT_EQ(OB_SUCCESS, scan.rescan());
  }
  for (const auto mode : {ObKnnFilterMode::PRE_KNN, ObKnnFilterMode::PRE_BRUTE_FORCE,
                         ObKnnFilterMode::PRE_ADAPTIVE}) {
    ctdef.filter_mode_ = mode;
    driver.update_async_ivf_go_brute_force_after_pre_filter(threshold);
    EXPECT_TRUE(driver.go_brute_force_);
    EXPECT_TRUE(scan.go_brute_force_);
    driver.update_async_ivf_go_brute_force_after_pre_filter(threshold + 1);
    const bool forced = mode == ObKnnFilterMode::PRE_BRUTE_FORCE;
    EXPECT_EQ(forced, driver.go_brute_force_);
    EXPECT_EQ(forced, scan.go_brute_force_);
  }
  eval_ctx.frames_ = nullptr;
}

TEST_F(TestIVFSSTable, async_ivf_partition_pre_abort_rejects_borrowed_bitmap)
{
  sql::ObDASVecIndexAsyncIVFScanIter scan;
  scan.vec_index_type_ = VEC_INDEX_PRE;
  ObIvfPreFilterBitmap *bitmap = nullptr;
  ASSERT_EQ(OB_SUCCESS, scan.prepare_partition_pre_filter(bitmap));
  ASSERT_EQ(OB_SUCCESS, bitmap->value_add(42));
  ASSERT_EQ(OB_SUCCESS, bitmap->finish());
  for (bool group_ready : {false, true}) {
    scan.opened_ = !group_ready;
    scan.group_ready_ = group_ready;
    EXPECT_EQ(OB_STATE_NOT_MATCH, scan.abort_partition_pre_filter_build());
    EXPECT_TRUE(scan.pre_filter_.is_valid());
    EXPECT_EQ(1, bitmap->get_cardinality());
  }
  scan.opened_ = false;
  scan.group_ready_ = false;
  // open_epoch publishes the binding before root_.open can fail.
  scan.query_.pre_filter_ = &scan.pre_filter_;
  EXPECT_EQ(OB_STATE_NOT_MATCH, scan.abort_partition_pre_filter_build());
  EXPECT_TRUE(scan.pre_filter_.is_valid());
  EXPECT_EQ(1, bitmap->get_cardinality());
  scan.query_.pre_filter_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, scan.abort_partition_pre_filter_build());
  EXPECT_FALSE(scan.pre_filter_.is_valid());
  ObIvfPreFilterBitmap *next_bitmap = nullptr;
  ASSERT_EQ(OB_SUCCESS, scan.prepare_partition_pre_filter(next_bitmap));
  EXPECT_EQ(bitmap, next_bitmap); // Member address remains stable across retries.
  ASSERT_EQ(OB_SUCCESS, scan.rescan());
  int64_t count = 99;
  EXPECT_EQ(OB_INVALID_ARGUMENT, scan.prepare_group_pre_filter(count));
  EXPECT_EQ(0, count);
}

TEST_F(TestIVFSSTable, async_ivf_adaptive_pre_bitmap_release)
{
  ObRbMemMgr *mgr = MTL(ObRbMemMgr *);
  ASSERT_NE(nullptr, mgr);
  const int64_t before = mgr->get_vec_idx_used();
  for (int mode = 0; mode < 3; ++mode) {
    for (const int64_t count : {0, 1, 64, 62500}) {
      sql::ObDASVecIndexAsyncIVFScanIter iter;
      iter.vec_index_type_ = VEC_INDEX_PRE;
      ObIvfPreFilterBitmap *bitmap = nullptr;
      ASSERT_EQ(OB_SUCCESS, iter.prepare_partition_pre_filter(bitmap));
      ASSERT_NE(nullptr, bitmap);
      for (int64_t i = 0; i < count; ++i) {
        ASSERT_EQ(OB_SUCCESS, bitmap->value_add(i * 8));
      }
      ASSERT_EQ(count, bitmap->get_cardinality());
      ASSERT_EQ(OB_SUCCESS, bitmap->finish());
      if (count <= MAX_BITMAP_SET_VALUES) {
        EXPECT_FALSE(bitmap->is_compact());
        EXPECT_FALSE(bitmap->is_roaring());
      } else {
        EXPECT_TRUE(bitmap->is_compact());
      }
      // Cover release, rescan and destruction of the completed bitmap.
      if (0 == mode) { ASSERT_EQ(OB_SUCCESS, iter.inner_release()); }
      if (1 == mode) { ASSERT_EQ(OB_SUCCESS, iter.rescan()); }
      if (mode < 2) { EXPECT_EQ(before, mgr->get_vec_idx_used()); }
    }
    EXPECT_EQ(before, mgr->get_vec_idx_used());
  }
}

TEST_F(TestIVFSSTable, async_ivf_group_adaptive_pre_bitmap_reset)
{
  ObRbMemMgr *mgr = MTL(ObRbMemMgr *);
  ASSERT_NE(nullptr, mgr);
  const int64_t before = mgr->get_vec_idx_used();
  sql::ObDASAsyncIVFGroupFilter group(allocator_);
  for (int g = 0; g < 3; ++g) {
    auto *bitmap = OB_NEWx(ObIvfPreFilterBitmap, &allocator_, allocator_);
    auto *filter = OB_NEWx(ObIvfPreFilter, &allocator_);
    ASSERT_NE(nullptr, bitmap);
    ASSERT_NE(nullptr, filter);
    const uint64_t stride = g == 2 ? (uint64_t(1) << 32) : 8;
    const uint64_t count = g == 2 ? 64 : 62500;
    for (uint64_t i = 0; i < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, bitmap->value_add(i * stride + g));
    }
    ASSERT_EQ(OB_SUCCESS, bitmap->finish());
    ASSERT_EQ(g != 2, bitmap->is_compact());
    ASSERT_EQ(g == 2, bitmap->is_roaring());
    ASSERT_EQ(OB_SUCCESS, filter->init(*bitmap));
    ASSERT_EQ(OB_SUCCESS, group.filters_.push_back(filter));
  }
  EXPECT_GT(mgr->get_vec_idx_used(), before);
  group.reset();
  EXPECT_EQ(before, mgr->get_vec_idx_used());
  group.reset();
  EXPECT_EQ(before, mgr->get_vec_idx_used());
}


TEST_F(TestIVFSSTable, ivf_pre_filter_borrows_allow_bitmap)
{
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(7));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(42));

  ObIvfPreFilter filter;
  ASSERT_FALSE(filter.is_valid());
  ASSERT_FALSE(filter.contains(-1));
  ASSERT_FALSE(filter.contains(7));
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ASSERT_TRUE(filter.is_valid());
  ASSERT_FALSE(filter.contains(-1));
  ASSERT_TRUE(filter.contains(7));
  ASSERT_FALSE(filter.contains(8));

  ObRoaringBitmap second(&allocator_);
  ASSERT_EQ(OB_SUCCESS, second.value_add(99));
  ASSERT_EQ(OB_INIT_TWICE, filter.init(second));
  ASSERT_TRUE(filter.contains(7));
  ASSERT_FALSE(filter.contains(99));

  ASSERT_EQ(OB_SUCCESS, allowed.value_add(8));
  ASSERT_TRUE(filter.contains(8));
  filter.reset();
  ASSERT_FALSE(filter.is_valid());
  ASSERT_FALSE(filter.contains(7));
  ASSERT_FALSE(filter.contains(-1));
  ASSERT_TRUE(allowed.is_contains(7));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(123));
  ASSERT_TRUE(allowed.is_contains(123));
}

TEST_F(TestIVFSSTable, disk_bbq_searcher_query_arena_uses_current_tenant)
{
  ObDiskBBQSearcher searcher(allocator_);
  ASSERT_EQ(MTL_ID(), searcher.query_arena_.get_arena().get_tenant_id());
  ASSERT_EQ(MTL_ID(), searcher.centroid_arena_.get_arena().get_tenant_id());
  ASSERT_EQ(MTL_ID(), searcher.search_arena_.get_arena().get_tenant_id());
}

TEST_F(TestIVFSSTable, get_frozen_schema_version_delegates_to_base)
{
  ObIvfSSTable sstable;
  int64_t schema_version = -1;
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  ASSERT_EQ(OB_SUCCESS, sstable.get_frozen_schema_version(schema_version));
  ASSERT_GE(schema_version, 0);
}

TEST_F(TestIVFSSTableWriter, ivf_row_desc_and_query_param_validity)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, sstable_.get_row_desc(row_desc));
  ASSERT_TRUE(row_desc.is_valid());
  ASSERT_EQ(0, row_desc.row_type_col_idx_);
  ASSERT_EQ(1, row_desc.centroid_id_col_idx_);
  ASSERT_EQ(2, row_desc.vector_id_col_idx_);
  ASSERT_EQ(3, row_desc.trans_version_col_idx_);
  ASSERT_EQ(4, row_desc.sql_sequence_col_idx_);
  ASSERT_EQ(5, row_desc.payload_col_idx_);

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(4, read_info.get_request_count());
  ASSERT_EQ(OB_INVALID_INDEX, read_info.get_trans_col_index());
  ASSERT_EQ(0, read_info.get_columns_index().at(0));
  ASSERT_EQ(1, read_info.get_columns_index().at(1));
  ASSERT_EQ(2, read_info.get_columns_index().at(2));
  ASSERT_EQ(5, read_info.get_columns_index().at(3));

  float query[4] = {1.0F, 2.0F, 3.0F, 4.0F};
  ObIvfKnnSearchParam query_param;
  ASSERT_FALSE(query_param.is_valid());
  ASSERT_EQ(nullptr, query_param.read_stats_);
  ASSERT_FLOAT_EQ(ObVectorIndexParam::DEFAULT_REFINE_K, query_param.refine_k_);
  query_param.query_ = query;
  query_param.dim_ = ARRAYSIZEOF(query);
  query_param.nprobe_ = 1;
  query_param.k_ = 1;
  ASSERT_TRUE(query_param.is_valid());

  for (const float invalid_refine_k : {0.5F, std::numeric_limits<float>::infinity(),
      std::numeric_limits<float>::quiet_NaN()}) {
    query_param.refine_k_ = invalid_refine_k;
    EXPECT_FALSE(query_param.is_valid());
  }
  query_param.refine_k_ = 1.5F;
  ASSERT_TRUE(query_param.is_valid());

  query_param.query_ = nullptr;
  ASSERT_FALSE(query_param.is_valid());
  query_param.query_ = query;
  query_param.dim_ = 0;
  ASSERT_FALSE(query_param.is_valid());
  query_param.dim_ = static_cast<int64_t>(INT32_MAX) + 1;
  ASSERT_FALSE(query_param.is_valid());
  query_param.dim_ = ARRAYSIZEOF(query);
  query_param.nprobe_ = 0;
  ASSERT_FALSE(query_param.is_valid());
  query_param.nprobe_ = 1;
  query_param.k_ = 0;
  ASSERT_FALSE(query_param.is_valid());
  query_param.k_ = 1;
  ObIvfSSTableReadStats read_stats;
  query_param.read_stats_ = &read_stats;
  ASSERT_TRUE(query_param.is_valid());
  query_param.reset();
  ASSERT_FALSE(query_param.is_valid());
  ASSERT_EQ(nullptr, query_param.read_stats_);
  ASSERT_FLOAT_EQ(ObVectorIndexParam::DEFAULT_REFINE_K, query_param.refine_k_);
}

struct ObIvfCentroidMetricCase
{
  share::ObVectorIndexDistAlgorithm dist_type_;
  int64_t expected_cid_;
};

class TestIVFCentroidMetric
    : public TestIVFSSTableWriter,
      public ::testing::WithParamInterface<ObIvfCentroidMetricCase>
{};

static uint32_t next_recall_test_random(uint32_t &state)
{
  state ^= state << 13;
  state ^= state >> 17;
  state ^= state << 5;
  return state;
}

static float next_recall_test_noise(uint32_t &state)
{
  return (static_cast<float>(next_recall_test_random(state) & 0x00ffffffU)
      / static_cast<float>(0x00ffffffU) - 0.5f) * 0.3f;
}

static void build_recall_test_centroids(
    const int32_t dim,
    const int32_t nlist,
    const share::ObVectorIndexDistAlgorithm dist_type,
    std::vector<float> &centroids)
{
  centroids.resize(static_cast<int64_t>(dim) * nlist);
  if (share::VIDA_L2 == dist_type) {
    for (int32_t cid = 0; cid < nlist; ++cid) {
      for (int32_t d = 0; d < dim; ++d) {
        centroids[static_cast<int64_t>(cid) * dim + d] =
            static_cast<float>(cid) * 3.0f
            + static_cast<float>(d) * 0.0001f;
      }
    }
  } else {
    std::fill(centroids.begin(), centroids.end(), 0.0f);
    for (int32_t cid = 0; cid < nlist; ++cid) {
      centroids[static_cast<int64_t>(cid) * dim + cid] = 10.0f;
    }
  }
}

static void build_recall_test_vectors(
    const std::vector<float> &centroids,
    const int32_t dim,
    const int32_t nlist,
    const int32_t vectors_per_list,
    const int32_t query_count,
    const share::ObVectorIndexDistAlgorithm dist_type,
    std::vector<float> &vectors,
    std::vector<int64_t> &cids,
    std::vector<int64_t> &vids,
    std::vector<float> &queries)
{
  uint32_t random_state = 0x4f1bbcdcU;
  const int32_t vector_count = nlist * vectors_per_list;
  vectors.resize(static_cast<int64_t>(vector_count) * dim);
  cids.resize(vector_count);
  vids.resize(vector_count);
  if (share::VIDA_L2 == dist_type) {
    for (int32_t cid = 0; cid < nlist; ++cid) {
      const float *centroid = centroids.data() + static_cast<int64_t>(cid) * dim;
      for (int32_t offset = 0; offset < vectors_per_list; ++offset) {
        const int32_t row = cid * vectors_per_list + offset;
        float *vector = vectors.data() + static_cast<int64_t>(row) * dim;
        cids[row] = cid;
        vids[row] = row;
        for (int32_t d = 0; d < dim; ++d) {
          vector[d] = centroid[d] + next_recall_test_noise(random_state);
        }
      }
    }
    queries.resize(static_cast<int64_t>(query_count) * dim);
    for (int32_t query_idx = 0; query_idx < query_count; ++query_idx) {
      const float *centroid = centroids.data()
          + static_cast<int64_t>(query_idx % nlist) * dim;
      float *query = queries.data() + static_cast<int64_t>(query_idx) * dim;
      for (int32_t d = 0; d < dim; ++d) {
        query[d] = centroid[d] + next_recall_test_noise(random_state);
      }
    }
  } else {
    std::fill(vectors.begin(), vectors.end(), 0.0f);
    for (int32_t cid = 0; cid < nlist; ++cid) {
      const float *centroid = centroids.data() + static_cast<int64_t>(cid) * dim;
      const int32_t residual_dim = nlist + cid;
      for (int32_t offset = 0; offset < vectors_per_list; ++offset) {
        const int32_t row = cid * vectors_per_list + offset;
        float *vector = vectors.data() + static_cast<int64_t>(row) * dim;
        cids[row] = cid;
        vids[row] = row;
        std::copy_n(centroid, dim, vector);
        vector[residual_dim] = static_cast<float>(offset + 1) * 0.1f;
      }
    }
    queries.assign(static_cast<int64_t>(query_count) * dim, 0.0f);
    for (int32_t query_idx = 0; query_idx < query_count; ++query_idx) {
      const int32_t cid = query_idx % nlist;
      const float *centroid = centroids.data() + static_cast<int64_t>(cid) * dim;
      float *query = queries.data() + static_cast<int64_t>(query_idx) * dim;
      std::copy_n(centroid, dim, query);
      query[nlist + cid] = share::VIDA_COS == dist_type ? 0.05f : 1.0f;
    }
  }
}

static int brute_force_recall_test_topk(
    const std::vector<float> &vectors,
    const std::vector<int64_t> &vids,
    const float *query,
    const int32_t dim,
    const int32_t topk,
    const share::ObVectorIndexDistAlgorithm dist_type,
    std::vector<int64_t> &top_vids)
{
  int ret = OB_SUCCESS;
  std::vector<std::pair<double, int64_t> > candidates;
  candidates.reserve(vids.size());
  for (int64_t row = 0; OB_SUCC(ret) && row < static_cast<int64_t>(vids.size()); ++row) {
    const float *vector = vectors.data() + row * dim;
    double distance = 0.0;
    if (share::VIDA_L2 == dist_type) {
      for (int32_t d = 0; d < dim; ++d) {
        const double delta = static_cast<double>(vector[d]) - query[d];
        distance += delta * delta;
      }
    } else if (share::VIDA_COS == dist_type) {
      ret = ObVectorCosineDistance<float>::cosine_distance_func(
          vector, query, dim, distance);
    } else if (share::VIDA_IP == dist_type) {
      double inner_product = 0.0;
      if (OB_FAIL(ObVectorIpDistance<float>::ip_distance_func(
          vector, query, dim, inner_product))) {
      } else {
        distance = 1.0 - inner_product;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCC(ret)) {
      candidates.push_back(std::make_pair(distance, vids[row]));
    }
  }
  if (OB_SUCC(ret)) {
    std::nth_element(candidates.begin(), candidates.begin() + topk, candidates.end(),
      [](const std::pair<double, int64_t> &lhs, const std::pair<double, int64_t> &rhs) {
        return lhs.first != rhs.first ? lhs.first < rhs.first : lhs.second < rhs.second;
      });
    std::sort(candidates.begin(), candidates.begin() + topk,
      [](const std::pair<double, int64_t> &lhs, const std::pair<double, int64_t> &rhs) {
        return lhs.first != rhs.first ? lhs.first < rhs.first : lhs.second < rhs.second;
      });
    top_vids.resize(topk);
    for (int32_t i = 0; i < topk; ++i) {
      top_vids[i] = candidates[i].second;
    }
  }
  return ret;
}

void TestIVFSSTableWriter::run_disk_bbq_sstable_10k_512d_recall_at_10(
    const share::ObVectorIndexDistAlgorithm dist_type)
{
  const int32_t kDim = 512;
  const int32_t kNList = 100;
  const int32_t kVectorsPerList = 100;
  const int32_t kVectorCount = kNList * kVectorsPerList;
  const int32_t kQueryCount = 100;
  const int32_t kTopK = 10;
  std::vector<float> centroids;
  std::vector<float> vectors;
  std::vector<int64_t> cids;
  std::vector<int64_t> vids;
  std::vector<float> queries;
  quantization::ObFhtKacRotator rotator;
  ASSERT_EQ(OB_SUCCESS,
      quantization::ObFhtKacRotator::generate(allocator_, kDim, rotator));
  build_recall_test_centroids(kDim, kNList, dist_type, centroids);
  build_recall_test_vectors(centroids, kDim, kNList, kVectorsPerList,
      kQueryCount, dist_type, vectors, cids, vids, queries);
  ASSERT_EQ(kVectorCount, static_cast<int32_t>(vids.size()));
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      kVectorCount, centroids.data(), kNList, kDim,
      reinterpret_cast<const char *>(rotator.signs()), 1,
      nullptr, dist_type));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  int64_t recall_hits = 0;
  for (int32_t query_idx = 0; query_idx < kQueryCount; ++query_idx) {
    const float *query = queries.data() + static_cast<int64_t>(query_idx) * kDim;
    std::vector<int64_t> ground_truth;
    ASSERT_EQ(OB_SUCCESS, brute_force_recall_test_topk(
        vectors, vids, query, kDim, kTopK, dist_type, ground_truth));
    ObIvfKnnSearchParam query_param;
    fill_search_params(query_param, query, kDim, kTopK);
    query_param.nprobe_ = 1;
    ObIvfKnnSearchResult result;
    ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
    ASSERT_EQ(kTopK, result.count()) << "query_idx=" << query_idx;
    std::unordered_set<int64_t> result_vids;
    for (int64_t i = 0; i < result.count(); ++i) {
      ASSERT_TRUE(result_vids.insert(result.at(i).vid_).second)
          << "query_idx=" << query_idx << ", vid=" << result.at(i).vid_;
      if (std::find(ground_truth.begin(), ground_truth.end(), result.at(i).vid_)
          != ground_truth.end()) {
        ++recall_hits;
      }
    }
  }
  const double average_recall = static_cast<double>(recall_hits)
      / static_cast<double>(kQueryCount * kTopK);
  LOG_INFO("DiskBBQ SSTable recall", K(average_recall), K(kDim), K(kVectorCount),
      K(kQueryCount), K(kTopK), K(dist_type));
  ASSERT_GE(average_recall, 0.95);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_sstable_10k_512d_l2_recall_at_10)
{
  run_disk_bbq_sstable_10k_512d_recall_at_10(share::VIDA_L2);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_sstable_10k_512d_cosine_recall_at_10)
{
  run_disk_bbq_sstable_10k_512d_recall_at_10(share::VIDA_COS);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_sstable_10k_512d_ip_recall_at_10)
{
  run_disk_bbq_sstable_10k_512d_recall_at_10(share::VIDA_IP);
}

struct ObIvfRefineMetricCase
{
  share::ObVectorIndexDistAlgorithm dist_type_;
};

class TestIVFRefineMetric
    : public TestIVFSSTableWriter,
      public ::testing::WithParamInterface<ObIvfRefineMetricCase>
{};


INSTANTIATE_TEST_CASE_P(
    L2IpAndCosine,
    TestIVFRefineMetric,
    ::testing::Values(
        ObIvfRefineMetricCase{share::VIDA_L2},
        ObIvfRefineMetricCase{share::VIDA_IP},
        ObIvfRefineMetricCase{share::VIDA_COS}));

TEST_P(TestIVFRefineMetric,
    disk_bbq_normalizes_only_cosine_query_before_rotation)
{
  const int32_t dim = 4;
  const int64_t cid = 0;
  const int64_t vid = 10;
  const share::ObVectorIndexDistAlgorithm metric = GetParam().dist_type_;
  float centroid[] = {1.0F, 0.0F, 0.0F, 0.0F};
  float vector[] = {1.0F, 0.0F, 0.0F, 0.0F};
  float query[] = {3.0F, 4.0F, 0.0F, 0.0F};
  char flip[4 * dim / CHAR_BIT] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, metric, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  EXPECT_EQ(vid, result.at(0).vid_);
  const float expected_distance = share::VIDA_COS == metric ? 0.4F
      : (share::VIDA_IP == metric ? -2.0F : 20.0F);
  EXPECT_NEAR(expected_distance, result.at(0).distance_, 1e-5F);

  ObDiskBBQSearcher *disk_searcher = static_cast<ObDiskBBQSearcher *>(searcher.get());
  const float *prepared_query = disk_searcher->query_vector_context().query();
  ASSERT_NE(nullptr, prepared_query);
  const float expected_norm = share::VIDA_COS == metric ? 1.0F : 25.0F;
  EXPECT_NEAR(expected_norm,
      ObVectorL2Distance<float>::l2_norm_square(prepared_query, dim), 1e-5F);
  EXPECT_FLOAT_EQ(3.0F, query[0]);
  EXPECT_FLOAT_EQ(4.0F, query[1]);
  ASSERT_EQ(OB_SUCCESS, disk_searcher->prepare_query(query_param));
  EXPECT_EQ(prepared_query, disk_searcher->query_vector_context().query());
  const auto *query_context = &disk_searcher->query_vector_context();
  const auto *computer = &query_context->computer();
  ASSERT_NE(nullptr, disk_searcher->centroid_iterator());
  EXPECT_EQ(query_context, disk_searcher->centroid_iterator()->query_vector_ctx_);
  EXPECT_FALSE(disk_searcher->centroid_iterator()->owns_query_vector_ctx_);
  disk_searcher->reuse();
  EXPECT_EQ(prepared_query, query_context->query());
  result.reset();
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  EXPECT_NEAR(expected_distance, result.at(0).distance_, 1e-5F);
  EXPECT_EQ(query_context, disk_searcher->centroid_iterator()->query_vector_ctx_);
  EXPECT_EQ(computer, &disk_searcher->query_vector_context().computer());
}

TEST_P(TestIVFRefineMetric,
    disk_bbq_refine_collects_misaligned_raw_vector_payload)
{
  const int32_t dim = 64;
  const int64_t cid = 0;
  const int64_t vid = 10;
  const ObIvfRefineMetricCase metric_case = GetParam();
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  vector[0] = 2.0F;
  vector[1] = 1.0F;
  query[0] = 1.0F;
  query[1] = 2.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, metric_case.dist_type_, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  ObIvfSearcherGuard searcher_guard;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(
          sstable_, scan_param_, scan_context_, searcher_guard.get()));
  ObDiskBBQSearcher *searcher =
      static_cast<ObDiskBBQSearcher *>(searcher_guard.get());
  ASSERT_NE(nullptr, searcher);

  float rotated_query[dim];
  float rotated_vector[dim];
  MEMCPY(rotated_query, query, sizeof(rotated_query));
  MEMCPY(rotated_vector, vector, sizeof(rotated_vector));
  ASSERT_EQ(OB_SUCCESS,
      sstable_.get_fht_rotator().rotate(dim, rotated_query));
  ASSERT_EQ(OB_SUCCESS,
      sstable_.get_fht_rotator().rotate(dim, rotated_vector));
  quantization::ObQuantizationParams fp32_params;
  fp32_params.dim_ = dim;
  quantization::ObQuantizationComputer computer;
  ASSERT_EQ(OB_SUCCESS, computer.init(allocator_,
      quantization::ObQuantizationType::FP32,
      metric_case.dist_type_, fp32_params, rotated_query, nullptr));
  float expected_distance = 0.0F;
  ASSERT_EQ(OB_SUCCESS, computer.compute(
      {reinterpret_cast<uint8_t *>(rotated_vector), sizeof(rotated_vector)},
      expected_distance));

  alignas(float) char raw_storage[sizeof(rotated_vector) + 1];
  char *misaligned_payload = raw_storage + 1;
  MEMCPY(misaligned_payload, rotated_vector, sizeof(rotated_vector));
  ASSERT_NE(0,
      reinterpret_cast<int64_t>(misaligned_payload) % alignof(float));
  const ObIvfSSTableRowDesc &row_desc = searcher->output_row_desc();
  const int64_t column_count = searcher->read_info().get_request_count();
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, column_count));
  row.count_ = static_cast<int16_t>(column_count);
  row.storage_datums_[row_desc.row_type_col_idx_].set_int(
      IVF_ROW_TYPE_RAW_VECTOR);
  row.storage_datums_[row_desc.centroid_id_col_idx_].set_int(
      IVF_RAW_VECTOR_CID);
  row.storage_datums_[row_desc.vector_id_col_idx_].set_int(vid);
  row.storage_datums_[row_desc.payload_col_idx_].set_string(
      ObString(sizeof(rotated_vector), misaligned_payload));

  ObIvfKnnEntry candidate;
  candidate.cid_ = cid + 1; // The fixture maps working indices to stored CIDs.
  candidate.vid_ = vid;
  ObIvfTopKCollector collector;
  ASSERT_EQ(OB_SUCCESS, collector.init(1, allocator_));
  ASSERT_EQ(OB_SUCCESS,
      searcher->collect_raw_vector_row(
          &row, candidate, computer, collector));
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(cid + 1, result.at(0).cid_);
  ASSERT_EQ(vid, result.at(0).vid_);
  ASSERT_NEAR(expected_distance, result.at(0).distance_, 1e-6F);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_raw_vector_multi_getter_sorted_prefetch_seam)
{
  const int32_t dim = 64;
  const int64_t count = 3;
  float centroid[dim] = {0};
  float vectors[count * dim] = {0};
  const int64_t cids[count] = {0, 0, 0};
  const int64_t vids[count] = {10, 20, 30};
  char flip[4 * dim / CHAR_BIT] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vectors, cids, vids, count, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObTableIterParam iter_param;
  iter_param.table_id_ = scan_context_.tablet_id_.id();
  iter_param.tablet_id_ = scan_context_.tablet_id_;
  iter_param.read_info_ = &read_info;

  ObStorageDatum sorted_datums[count][3];
  ObStorageDatum unsorted_datums[count][3];
  ObSEArray<ObDatumRowkey, count> sorted_rowkeys;
  ObSEArray<ObDatumRowkey, count> unsorted_rowkeys;
  const int64_t unsorted_vids[count] = {30, 10, 20};
  for (int64_t i = 0; i < count; ++i) {
    sorted_datums[i][0].set_int(IVF_ROW_TYPE_RAW_VECTOR);
    sorted_datums[i][1].set_int(IVF_RAW_VECTOR_CID);
    sorted_datums[i][2].set_int(vids[i]);
    ObDatumRowkey sorted_rowkey;
    ASSERT_EQ(OB_SUCCESS, sorted_rowkey.assign(sorted_datums[i], 3));
    ASSERT_EQ(OB_SUCCESS, sorted_rowkeys.push_back(sorted_rowkey));
    unsorted_datums[i][0].set_int(IVF_ROW_TYPE_RAW_VECTOR);
    unsorted_datums[i][1].set_int(IVF_RAW_VECTOR_CID);
    unsorted_datums[i][2].set_int(unsorted_vids[i]);
    ObDatumRowkey unsorted_rowkey;
    ASSERT_EQ(OB_SUCCESS, unsorted_rowkey.assign(unsorted_datums[i], 3));
    ASSERT_EQ(OB_SUCCESS, unsorted_rowkeys.push_back(unsorted_rowkey));
  }

  const uint64_t saved_scan_order = scan_context_.query_flag_.scan_order_;
  scan_context_.query_flag_.scan_order_ = ObQueryFlag::NoOrder;
  {
    ObIvfSortedMultiGetterSeam getter;
    ASSERT_EQ(OB_SUCCESS,
        getter.init(iter_param, scan_context_, &sstable_, &unsorted_rowkeys));
    ASSERT_FALSE(getter.is_rowkey_sorted());
  }
  scan_context_.query_flag_.scan_order_ = ObQueryFlag::Forward;
  {
    ObIvfSortedMultiGetterSeam getter;
    ASSERT_EQ(OB_SUCCESS,
        getter.init(iter_param, scan_context_, &sstable_, &sorted_rowkeys));
    ASSERT_TRUE(getter.is_rowkey_sorted());
  }
  scan_context_.query_flag_.scan_order_ = saved_scan_order;
}

TEST_P(TestIVFRefineMetric,
    disk_bbq_distance_bounds_select_refine_candidates_and_rescue_exact_neighbors)
{
  const int32_t dim = 64;
  const int64_t count = 9;
  const ObIvfRefineMetricCase metric_case = GetParam();
  float centroid[dim] = {0};
  float encoded[count * dim] = {0};
  float raw[count * dim] = {0};
  int64_t cids[count] = {0};
  int64_t vids[count] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  for (int64_t i = 0; i < count; ++i) {
    vids[i] = 10 + i;
    encoded[i * dim] = i < count - 1 ? 1.0F : -1.0F;
    if (i < 2 || i == count - 1) {
      raw[i * dim + 1] = share::VIDA_L2 == metric_case.dist_type_ ? 4.0F : 1.0F;
    } else {
      raw[i * dim] = share::VIDA_IP == metric_case.dist_type_ ? 2.0F : 1.0F;
    }
  }

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, cids, vids, count,
      centroid, 1, dim, flip, 1, nullptr, metric_case.dist_type_, true, raw));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result))
        << "metric=" << metric_case.dist_type_;
  }
  ASSERT_EQ(2, result.count());
  ASSERT_LE(result.count(), query_param.k_);
  ASSERT_LE(result.at(0).distance_, result.at(1).distance_);
  ASSERT_EQ(12, result.at(0).vid_);
  ASSERT_EQ(13, result.at(1).vid_);
  const float expected_exact_distance =
      share::VIDA_IP == metric_case.dist_type_ ? -1.0F : 0.0F;
  ASSERT_NEAR(expected_exact_distance, result.at(0).distance_, 1e-6F);
  ASSERT_NEAR(expected_exact_distance, result.at(1).distance_, 1e-6F);
  ASSERT_GE(stats.raw_vector_key_count_, query_param.k_);
  ASSERT_LE(stats.raw_vector_key_count_, count);
}

TEST_P(TestIVFRefineMetric,
    disk_bbq_soar_deduplicates_raw_reads_across_cids_and_resets_each_round)
{
  const int32_t dim = 64;
  const int64_t unique_count = ObDistBBQFastScanBatch::CAPACITY + 7;
  const int32_t nlist = 3;
  const int64_t count = unique_count * nlist;
  const auto metric = GetParam().dist_type_;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<float> centroids(nlist * dim, 0.0F);
  std::vector<int64_t> cids(count);
  std::vector<int64_t> vids(count);
  float query[dim] = {0};
  query[0] = 1.0F;
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / unique_count;
    vids[i] = i % unique_count;
    vectors[i * dim] = 1.0F;
  }
  for (int64_t cid = 0; cid < nlist; ++cid) {
    centroids[cid * dim + 1] = 0.1F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), nlist, dim, nullptr, 1, nullptr, metric, true,
      nullptr, nullptr, 0, true /* allow_shared_raw_vectors */));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, unique_count);
  param.iterative_centroids_ = true;
  param.nprobe_ = nlist;
  param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(unique_count, result.count());
  ASSERT_EQ(count, stats.low_precision_vector_count_);
  ASSERT_EQ(unique_count, stats.raw_vector_key_count_);
  ASSERT_EQ(unique_count, stats.raw_vector_refine_count_);
  for (int64_t i = 0; i < unique_count; ++i) {
    ASSERT_EQ(i, result.at(i).vid_); // Equal vectors with distinct VIDs survive.
  }

  searcher->reuse();
  param.nprobe_ = 1;
  for (int64_t round = 0; round < nlist; ++round) {
    stats.reset();
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
    ASSERT_EQ(unique_count, result.count());
    ASSERT_EQ(unique_count, stats.raw_vector_key_count_);
    ASSERT_EQ(round + 1 < nlist, searcher->has_remaining_centroids());
  }

  // A new SSTable binding and filters must not inherit retained VIDs.
  ObTableHandleV2 handle;
  ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&sstable_));
  ASSERT_EQ(OB_SUCCESS, searcher->switch_sstable(handle));
  ObRoaringBitmap allowed(&allocator_);
  for (int64_t vid = 0; vid < unique_count; vid += 2) {
    ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid));
  }
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  const int64_t deleted_vid = 0;
  ObDeletionBitmapHandle deletion;
  ASSERT_EQ(OB_SUCCESS, init_test_deletion_bitmap(allocator_, &deleted_vid, 1, deletion));
  param.pre_filter_ = &filter;
  param.deletion_bitmap_handle_ = &deletion;
  param.nprobe_ = nlist;
  stats.reset();
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ((unique_count + 1) / 2 - 1, result.count());
  ASSERT_EQ(result.count(), stats.raw_vector_key_count_);
  for (int64_t i = 0; i < result.count(); ++i) {
    ASSERT_EQ(2 * (i + 1), result.at(i).vid_);
  }
}

TEST_P(TestIVFRefineMetric,
    disk_bbq_pending_chunks_preserve_late_neighbors_and_distance_ties)
{
  const int32_t dim = 64;
  const int64_t capacity = ObDiskBBQSearcher::MAX_PENDING_CANDIDATES;
  const int64_t first_count = capacity + 17;
  const int64_t tail_count = 33;
  const int64_t count = first_count + tail_count;
  const auto metric = GetParam().dist_type_;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count);
  std::vector<int64_t> vids(count);
  float query[dim] = {1.0F};
  float centroids[2 * dim] = {0};
  centroids[0] = centroids[dim] = 1.0F;
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i < first_count ? 0 : 1;
    vids[i] = i < first_count ? tail_count + i : i - first_count;
    vectors[i * dim + 1] = 1.0F;
  }
  // The best vector and the smallest equal-distance VIDs arrive after the
  // first chunk, including continuation of the still-open first posting list.
  vectors[(count - 1) * dim] = 1.0F;
  vectors[(count - 1) * dim + 1] = 0.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids, 2, dim, nullptr, 1, nullptr, metric));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 3);
  param.nprobe_ = 2;
  param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, param, result));
  ASSERT_EQ(count, stats.low_precision_vector_count_);
  ASSERT_EQ(capacity, stats.peak_pending_candidate_capacity_);
  ASSERT_EQ(3, result.count());
  ASSERT_EQ(tail_count - 1, result.at(0).vid_);
  ASSERT_NEAR(0.0F, result.at(0).distance_, 1e-6F);
  for (int64_t i = 1; i < result.count(); ++i) {
    ASSERT_EQ(i - 1, result.at(i).vid_);
    ASSERT_NEAR(share::VIDA_L2 == metric ? 2.0F : 1.0F,
        result.at(i).distance_, 1e-6F);
  }
  RecordProperty("pending_capacity", stats.peak_pending_candidate_capacity_);
  RecordProperty("scanned_postings", stats.low_precision_vector_count_);
  RecordProperty("raw_keys", stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_pending_chunks_deduplicate_soar_and_reuse)
{
  const int32_t dim = 64;
  const int64_t capacity = ObDiskBBQSearcher::MAX_PENDING_CANDIDATES;
  const int64_t unique_count = capacity + 9;
  const int64_t count = 2 * unique_count;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count);
  std::vector<int64_t> vids(count);
  float query[dim] = {0};
  float centroids[2 * dim] = {0};
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / unique_count;
    vids[i] = i % unique_count;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids, 2, dim, nullptr, 1, nullptr, share::VIDA_L2, true,
      nullptr, nullptr, 0, true /* allow_shared_raw_vectors */));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 3);
  param.nprobe_ = 2;
  param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  for (int64_t round = 0; round < 2; ++round) {
    searcher->reuse();
    stats.reset();
    ASSERT_EQ(0, stats.peak_pending_candidate_capacity_);
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
    ASSERT_EQ(count, stats.low_precision_vector_count_);
    ASSERT_EQ(capacity, stats.peak_pending_candidate_capacity_);
    ASSERT_EQ(unique_count, stats.raw_vector_key_count_);
    ASSERT_EQ(unique_count, stats.raw_vector_refine_count_);
    ASSERT_EQ(3, result.count());
    for (int64_t i = 0; i < result.count(); ++i) {
      ASSERT_EQ(i, result.at(i).vid_);
      ASSERT_FLOAT_EQ(0.0F, result.at(i).distance_);
    }
  }
  RecordProperty("pending_capacity", stats.peak_pending_candidate_capacity_);
  RecordProperty("scanned_postings", stats.low_precision_vector_count_);
  RecordProperty("raw_keys", stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_pending_chunks_propagate_mid_scan_refine_failure)
{
  const int32_t dim = 64;
  const int64_t capacity = ObDiskBBQSearcher::MAX_PENDING_CANDIDATES;
  const int64_t count = capacity + 33;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0);
  std::vector<int64_t> vids(count);
  float query[dim] = {0};
  float centroid[dim] = {0};
  for (int64_t i = 0; i < count; ++i) { vids[i] = i; }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroid, 1, dim, nullptr, 1, nullptr, share::VIDA_L2,
      false /* write_raw_vectors */));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 3);
  param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  const uint64_t scan_order = scan_context_.query_flag_.scan_order_;
  for (int64_t round = 0; round < 2; ++round) {
    searcher->reuse();
    stats.reset();
    ASSERT_EQ(OB_INVALID_DATA, searcher->knn_search(param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(capacity, stats.peak_pending_candidate_capacity_);
    ASSERT_GE(stats.low_precision_vector_count_, capacity);
    ASSERT_LT(stats.low_precision_vector_count_, count);
    ASSERT_EQ(scan_order, scan_context_.query_flag_.scan_order_);
  }
}

TEST_F(TestIVFSSTableWriter, disk_bbq_soar_rejected_bound_does_not_hide_later_posting)
{
  const int32_t dim = 64;
  // CID 0 seeds Top-1 with VID 10 and rejects VID 20 at the latest threshold.
  // CID 1's alternate encoding of VID 20 must still reach exact refinement.
  float encoded[3 * dim] = {0};
  float raw[3 * dim] = {0};
  float centroids[2 * dim] = {0};
  float query[dim] = {0};
  const int64_t cids[] = {0, 0, 1};
  const int64_t vids[] = {10, 20, 20};
  centroids[dim + 1] = 0.1F;
  encoded[0] = raw[0] = 1.0F;
  encoded[dim] = 1000.0F;
  encoded[2 * dim] = raw[dim] = raw[2 * dim] = 0.01F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, cids, vids, 3,
      centroids, 2, dim, nullptr, 1, nullptr, share::VIDA_L2, true,
      raw, nullptr, 0, true /* allow_shared_raw_vectors */));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  param.nprobe_ = 2;
  param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, param, result));
  ASSERT_EQ(1, stats.first_low_precision_vector_scan_cid_);
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  // SOAR CID provenance can depend on refinement order; compare VID/distance.
  ASSERT_EQ(2, result.at(0).cid_);
  ASSERT_NEAR(0.0001F, result.at(0).distance_, 1e-6F);
  ASSERT_EQ(2, stats.raw_vector_refine_count_);
}

TEST_P(TestIVFRefineMetric,
    disk_bbq_refine_all_deleted_skips_raw_vector_io)
{
  const int32_t dim = 64;
  const ObIvfRefineMetricCase metric_case = GetParam();
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float query[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  vector[0] = 1.0F;
  query[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, metric_case.dist_type_, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObDeletionBitmapHandle deletion;
  ASSERT_EQ(OB_SUCCESS, init_test_deletion_bitmap(
      allocator_, &vid, 1, deletion));
  ObIvfKnnSearchResult result;

  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search_with_deletion_bitmap(sstable_, scan_param_, query_param, &deletion, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, stats.raw_vector_read_count_);
  ASSERT_EQ(0, stats.raw_vector_key_count_);
}

TEST_P(TestIVFRefineMetric,
    disk_bbq_refine_missing_raw_vector_is_atomic)
{
  const int32_t dim = 64;
  const ObIvfRefineMetricCase metric_case = GetParam();
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float query[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  vector[0] = 1.0F;
  query[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, metric_case.dist_type_, false));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));

  ASSERT_EQ(OB_INVALID_DATA,
      run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(1, stats.raw_vector_key_count_);
}

TEST_P(TestIVFRefineMetric,
    disk_bbq_refine_corrupt_raw_vector_is_atomic)
{
  const int32_t dim = 64;
  const ObIvfRefineMetricCase metric_case = GetParam();
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float corrupt_raw[dim] = {0};
  float query[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  vector[0] = 1.0F;
  corrupt_raw[0] = std::numeric_limits<float>::quiet_NaN();
  query[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr,
      metric_case.dist_type_, true, corrupt_raw));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));

  ASSERT_EQ(OB_NUMERIC_OVERFLOW,
      run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(0, result.count());
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_l2_search_refines_with_exact_rotated_raw_vector)
{
  const int32_t dim = 17;
  const int64_t cid = 0;
  const int64_t vid = 10;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float query[dim] = {0};
  quantization::ObFhtKacRotator rotator;
  vector[0] = 1.0F;
  vector[1] = -0.25F;
  vector[7] = 0.5F;
  query[0] = 0.2F;
  query[3] = 0.9F;
  query[9] = -0.3F;
  ASSERT_EQ(OB_SUCCESS,
      quantization::ObFhtKacRotator::generate(allocator_, dim, rotator));
  ASSERT_EQ(4 * ((dim + CHAR_BIT - 1) / CHAR_BIT), rotator.signs_size());
  const char *flip = reinterpret_cast<const char *>(rotator.signs());

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  std::vector<float> rotated_query(query, query + dim);
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, rotated_query.data()));
  quantization::ObQuantizationParams params;
  params.dim_ = dim;
  std::vector<float> rotated_raw(vector, vector + dim);
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, rotated_raw.data()));
  quantization::ObQuantizationComputer expected_computer;
  ASSERT_EQ(OB_SUCCESS, expected_computer.init(allocator_,
      quantization::ObQuantizationType::FP32, share::VIDA_L2, params,
      rotated_query.data(), nullptr));
  float expected_distance = 0.0F;
  ASSERT_EQ(OB_SUCCESS, expected_computer.compute(
      {reinterpret_cast<uint8_t *>(rotated_raw.data()),
          static_cast<int64_t>(rotated_raw.size() * sizeof(float))},
      expected_distance));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(vid, result.at(0).vid_);
  ASSERT_NEAR(expected_distance, result.at(0).distance_, 1e-5F);
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(1, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_refine_nonzero_candidate_cid_reads_zero_cid_raw_vector)
{
  const int32_t dim = 64;
  const int32_t nlist = 2;
  const int64_t cid = 1;
  const int64_t vid = 20;
  float centroids[nlist * dim] = {0};
  float vector[dim] = {0};
  float query[dim] = {0};
  quantization::ObFhtKacRotator rotator;
  query[0] = 2.0F;
  query[3] = -1.0F;
  query[9] = 0.5F;
  MEMCPY(centroids + dim, query, sizeof(query));
  vector[0] = 2.25F;
  vector[3] = -0.75F;
  vector[9] = 0.125F;
  ASSERT_EQ(OB_SUCCESS,
      quantization::ObFhtKacRotator::generate(allocator_, dim, rotator));
  ASSERT_EQ(4 * dim / CHAR_BIT, rotator.signs_size());
  const char *flip = reinterpret_cast<const char *>(rotator.signs());

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroids, nlist, dim, flip, 1));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  std::vector<float> rotated_query(query, query + dim);
  std::vector<float> rotated_raw(vector, vector + dim);
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, rotated_query.data()));
  ASSERT_EQ(OB_SUCCESS, rotator.rotate(dim, rotated_raw.data()));
  float expected_distance = 0.0F;
  for (int64_t i = 0; i < dim; ++i) {
    const float delta = rotated_query[i] - rotated_raw[i];
    expected_distance += delta * delta;
  }

  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS,
      run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(cid + 1, result.at(0).cid_);
  ASSERT_EQ(vid, result.at(0).vid_);
  ASSERT_NEAR(expected_distance, result.at(0).distance_, 1e-5F);
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(1, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_ip_refine_returns_one_minus_inner_product_in_ascending_order)
{
  const int32_t dim = 64;
  const int64_t count = 2;
  float centroid[dim] = {0};
  float vectors[count * dim] = {0};
  float query[dim] = {0};
  const int64_t cids[count] = {0, 0};
  const int64_t vids[count] = {10, 11};
  char flip[4 * dim / CHAR_BIT] = {0};
  vectors[0] = 3.0F;
  vectors[dim] = -1.0F;
  query[0] = 2.0F;

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors, cids, vids, count,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_IP, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, count);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;

  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(2, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_FLOAT_EQ(-5.0F, result.at(0).distance_);
  ASSERT_EQ(11, result.at(1).vid_);
  ASSERT_FLOAT_EQ(3.0F, result.at(1).distance_);
  ASSERT_LT(result.at(0).distance_, result.at(1).distance_);
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(2, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_cos_rerank_updates_exact_threshold_inline_and_orders_results)
{
  const int32_t dim = 64;
  const int32_t nlist = 2;
  const int64_t count = 4;
  float centroids[nlist * dim] = {0};
  float encoded[count * dim] = {0};
  float raw[count * dim] = {0};
  const int64_t cids[count] = {0, 0, 1, 1};
  const int64_t vids[count] = {10, 11, 20, 21};
  char flip[4 * dim / CHAR_BIT] = {0};
  centroids[2] = 0.05F;
  centroids[dim + 3] = 0.05F;
  encoded[0 * dim] = 1.0F;
  encoded[1 * dim] = -1.0F;
  encoded[2 * dim] = 0.9F;
  encoded[3 * dim] = -0.9F;
  raw[0 * dim + 1] = 1.0F; // vid=10 exact cosine distance 1
  raw[1 * dim] = -1.0F;
  raw[2 * dim] = 1.0F;     // vid=20 exact cosine distance 0
  raw[3 * dim] = -1.0F;

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, cids, vids, count,
      centroids, nlist, dim, flip, 1, nullptr, share::VIDA_COS, true, raw));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.nprobe_ = nlist;
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;

  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(2, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  ASSERT_NEAR(0.0F, result.at(0).distance_, 1e-6F);
  ASSERT_EQ(10, result.at(1).vid_);
  ASSERT_NEAR(1.0F, result.at(1).distance_, 1e-6F);
  // Global scheduling batches candidates from both CIDs in one multi-get.
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(4, stats.raw_vector_key_count_);
  // VID 21 is rejected after VID 20 tightens the exact threshold.
  ASSERT_EQ(3, stats.raw_vector_refine_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_cos_rerank_uses_bound_qualified_candidates)
{
  const int32_t dim = 64;
  const int64_t count = 3;
  float centroid[dim] = {0};
  float encoded[count * dim] = {0};
  float raw[count * dim] = {0};
  const int64_t cids[count] = {0, 0, 0};
  const int64_t vids[count] = {10, 11, 12};
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  encoded[0 * dim] = 1.0F;
  encoded[1 * dim] = 0.8F;
  encoded[1 * dim + 1] = 0.2F;
  encoded[2 * dim] = 0.5F;
  raw[0 * dim + 1] = 1.0F;
  raw[1 * dim] = 1.0F;
  raw[1 * dim + 1] = 1.0F;
  raw[2 * dim] = 1.0F; // bound-qualified candidate is the true nearest

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, cids, vids, count,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, true, raw));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;

  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(2, result.count());
  ASSERT_EQ(12, result.at(0).vid_);
  ASSERT_NEAR(0.0F, result.at(0).distance_, 1e-6F);
  ASSERT_EQ(11, result.at(1).vid_);
  ASSERT_NEAR(1.0F - 1.0F / std::sqrt(2.0F),
      result.at(1).distance_, 1e-6F);
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(3, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_cos_rerank_rescues_bound_qualified_candidates_beyond_k)
{
  const int32_t dim = 64;
  const int64_t count = 9;
  float centroid[dim] = {0};
  float encoded[count * dim] = {0};
  float raw[count * dim] = {0};
  int64_t cids[count] = {0};
  int64_t vids[count] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  for (int64_t i = 0; i < count; ++i) {
    vids[i] = 10 + i;
    encoded[i * dim] = i < count - 1 ? 1.0F : -1.0F;
    if (i < 2 || i == count - 1) {
      raw[i * dim + 1] = 1.0F;
    } else {
      raw[i * dim] = 1.0F;
    }
  }

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, cids, vids, count,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, true, raw));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;

  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(2, result.count());
  for (int64_t i = 0; i < result.count(); ++i) {
    ASSERT_GE(result.at(i).vid_, 12);
    ASSERT_LE(result.at(i).vid_, 17);
    ASSERT_NEAR(0.0F, result.at(i).distance_, 1e-6F);
  }
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(9, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_refine_all_deleted_succeeds_without_raw_vector_io)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  vector[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObDeletionBitmapHandle deletion;
  ASSERT_EQ(OB_SUCCESS, init_test_deletion_bitmap(
      allocator_, &vid, 1, deletion));
  ObIvfKnnSearchResult result;

  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search_with_deletion_bitmap(sstable_, scan_param_, query_param, &deletion, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, stats.raw_vector_read_count_);
  ASSERT_EQ(0, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_refine_missing_raw_vector_preserves_error_and_empty_result)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  vector[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, false));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));

  ASSERT_EQ(OB_INVALID_DATA, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(1, stats.raw_vector_key_count_);

  // Profiling must preserve the same missing-RAW error and atomic result.
  ObOpProfile<ObMetric> profile(ObProfileId::PHY_HASH_JOIN, &allocator_);
  stats.reset();
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  {
    ObProfileSwitcher switcher(&profile);
    ASSERT_EQ(OB_INVALID_DATA,
        run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  }
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(1, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_refine_corrupt_raw_vector_preserves_error_and_empty_result)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float encoded[dim] = {0};
  float corrupt_raw[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  encoded[0] = 1.0F;
  corrupt_raw[0] = std::numeric_limits<float>::quiet_NaN();
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr,
      share::VIDA_COS, true, corrupt_raw));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));

  ASSERT_EQ(OB_NUMERIC_OVERFLOW,
      run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(1, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_refine_zero_cosine_raw_uses_rabitq_distance)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float encoded[dim] = {0};
  float zero_raw[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  encoded[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, true, zero_raw));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));

  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  EXPECT_EQ(vid, result.at(0).vid_);
  EXPECT_FLOAT_EQ(1.0F, result.at(0).distance_);

  auto *disk_bbq_searcher = static_cast<ObDiskBBQSearcher *>(searcher.get());
  ASSERT_NE(nullptr, disk_bbq_searcher->refined_vids_);
  searcher->reuse();
  ASSERT_EQ(nullptr, disk_bbq_searcher->refined_vids_);
  ObIvfCandidateProgress progress;
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(query_param,
      std::numeric_limits<float>::infinity(), result, progress, 32));
  ASSERT_EQ(1, result.count());
  EXPECT_EQ(vid, result.at(0).vid_);
  EXPECT_FLOAT_EQ(1.0F, result.at(0).distance_);
  EXPECT_EQ(nullptr, disk_bbq_searcher->refined_vids_);
  EXPECT_EQ(2, stats.raw_vector_refine_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_refine_propagates_final_push_failure)
{
  const int32_t dim = 64;
  const int64_t count = 17;
  float centroid[dim] = {0};
  std::vector<float> encoded(count * dim, 0.0F);
  std::vector<float> raw(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0);
  std::vector<int64_t> vids(count, 0);
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  for (int64_t i = 0; i < count; ++i) {
    encoded[i * dim] = 1.0F;
    raw[i * dim] = 1.0F;
    vids[i] = 10 + i;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded.data(), cids.data(),
      vids.data(), count, centroid, 1, dim, flip, 1, nullptr,
      share::VIDA_COS, true, raw.data()));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  int64_t heap_spill_alloc_size = -1;
  {
    ObIvfCountingAllocator probe_allocator(allocator_);
    ObIvfTopKCollector probe_collector;
    ASSERT_EQ(OB_SUCCESS, probe_collector.init(count, probe_allocator));
    for (int64_t i = 0; i < count - 1; ++i) {
      ASSERT_EQ(OB_SUCCESS, probe_collector.push(0, i, static_cast<float>(i)));
    }
    const int64_t alloc_count_before_spill = probe_allocator.alloc_count_;
    ASSERT_EQ(OB_SUCCESS, probe_collector.push(
        0, count - 1, static_cast<float>(count - 1)));
    ASSERT_EQ(alloc_count_before_spill + 1, probe_allocator.alloc_count_);
    heap_spill_alloc_size = probe_allocator.last_alloc_size_;
  }
  ASSERT_GT(heap_spill_alloc_size, 0);

  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTableReadStats stats;
  ObIvfCountingAllocator caller_allocator(allocator_);
  ObTableScanParam counting_scan_param;
  ObStoreCtx counting_store_ctx;
  ObTableAccessContext counting_access_ctx;
  prepare_ivf_scan_param(scan_param_, caller_allocator, counting_scan_param);
  ASSERT_EQ(OB_SUCCESS, init_ivf_access_context(
      sstable_, counting_scan_param, counting_store_ctx, counting_access_ctx));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, count);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(
        sstable_, counting_scan_param, counting_access_ctx, searcher.get()));
    // Inject at the collector directly: normal searches now allocate their
    // collector from the owned round arena, not the caller's allocator.
    ObIvfTopKCollector collector;
    ASSERT_EQ(OB_SUCCESS, collector.init(count, caller_allocator));
    caller_allocator.set_fail_alloc_size(heap_spill_alloc_size);

    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED,
        static_cast<ObDiskBBQSearcher *>(searcher.get())->do_knn_search(query_param, collector));
    ASSERT_EQ(OB_SUCCESS, collector.finish(result));
    ASSERT_EQ(count - 1, result.count());
    ASSERT_EQ(1, stats.raw_vector_read_count_);
    ASSERT_EQ(count, stats.raw_vector_key_count_);
    ASSERT_EQ(count - 1, stats.raw_vector_refine_count_);
  }
  ASSERT_EQ(0, caller_allocator.live_allocation_count());
  ASSERT_EQ(0, caller_allocator.live_allocated_bytes());
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_l2_zero_query_and_zero_raw_vector_refine_exactly)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float zero_query[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_L2, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, zero_query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;

  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(vid, result.at(0).vid_);
  ASSERT_FLOAT_EQ(0.0F, result.at(0).distance_);
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(1, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_searcher_continues_cids_across_rounds_and_resets_on_rebind)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float query[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  vector[0] = 1.0F;
  query[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_L2, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObIvfKnnSearchResult first_result;
  ObIvfKnnSearchResult second_result;
  ObIvfKnnSearchResult rebound_result;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS, table_handle.set_sstable_with_tablet(&sstable_));

  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, first_result));
  ASSERT_EQ(1, first_result.count());
  ASSERT_EQ(vid, first_result.at(0).vid_);
  ASSERT_FALSE(searcher->has_remaining_centroids());

  // The next iterative-filter round continues from the current CID cursor.
  query_param.nprobe_ = 2;
  query_param.k_ = 2;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, second_result));
  ASSERT_EQ(0, second_result.count());
  ASSERT_FALSE(searcher->has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, searcher->switch_sstable(table_handle));
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, rebound_result));
  ASSERT_EQ(1, rebound_result.count());
  ASSERT_EQ(vid, rebound_result.at(0).vid_);
  ASSERT_FALSE(searcher->has_remaining_centroids());

  sstable_.ivf_meta_.set_empty();
  ObIvfKnnSearchResult empty_result;
  ASSERT_EQ(OB_SUCCESS, searcher->switch_sstable(table_handle));
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, empty_result));
  ASSERT_EQ(0, empty_result.count());
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_repeated_segment_search_keeps_arena_memory_bounded)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float query[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  vector[0] = 1.0F;
  query[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_L2, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  const int64_t copy_size = sstable_.get_deep_copy_size();
  char *copy_buf = static_cast<char *>(allocator_.alloc(copy_size));
  ASSERT_NE(nullptr, copy_buf);
  ObIStorageMetaObj *copied_obj = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      sstable_.deep_copy(copy_buf, copy_size, copied_obj));
  ASSERT_NE(nullptr, copied_obj);
  ObIvfSSTable *second_segment = static_cast<ObIvfSSTable *>(copied_obj);
  ASSERT_NE(&sstable_, second_segment);

  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObIvfKnnSearchResult result;
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(
            sstable_, scan_param_, scan_context_, searcher.get()));
    ObTableHandleV2 first_handle;
    ObTableHandleV2 second_handle;
    ASSERT_EQ(OB_SUCCESS, first_handle.set_sstable_with_tablet(&sstable_));
    ASSERT_EQ(OB_SUCCESS,
        second_handle.set_sstable_with_tablet(second_segment));

    // Warm up both segments before recording the round-arena high water mark.
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
    ASSERT_EQ(1, result.count());
    ASSERT_EQ(vid, result.at(0).vid_);
    const int64_t first_query_used = searcher->query_arena_.used();
    const int64_t first_query_total = searcher->query_arena_.total();
    const int64_t first_search_used = searcher->search_arena_.used();
    const int64_t first_search_total = searcher->search_arena_.total();
    ASSERT_GT(first_query_used, 0);
    ASSERT_GT(first_search_used, 0);

    ASSERT_EQ(OB_SUCCESS, searcher->switch_sstable(second_handle));
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
    ASSERT_EQ(1, result.count());
    ASSERT_EQ(vid, result.at(0).vid_);
    ASSERT_EQ(first_query_used, searcher->query_arena_.used());
    ASSERT_EQ(first_query_total, searcher->query_arena_.total());
    const int64_t search_used_limit =
        std::max(first_search_used, searcher->search_arena_.used());
    const int64_t search_total_limit =
        std::max(first_search_total, searcher->search_arena_.total());

    const int64_t repeat_count = 32;
    for (int64_t i = 0; i < repeat_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, searcher->switch_sstable(first_handle));
      ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
      ASSERT_EQ(1, result.count());
      ASSERT_EQ(vid, result.at(0).vid_);
      ASSERT_EQ(first_query_used, searcher->query_arena_.used());
      ASSERT_EQ(first_query_total, searcher->query_arena_.total());
      ASSERT_LE(searcher->search_arena_.used(), search_used_limit);
      ASSERT_LE(searcher->search_arena_.total(), search_total_limit);

      ASSERT_EQ(OB_SUCCESS, searcher->switch_sstable(second_handle));
      ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
      ASSERT_EQ(1, result.count());
      ASSERT_EQ(vid, result.at(0).vid_);
      ASSERT_EQ(first_query_used, searcher->query_arena_.used());
      ASSERT_EQ(first_query_total, searcher->query_arena_.total());
      ASSERT_LE(searcher->search_arena_.used(), search_used_limit);
      ASSERT_LE(searcher->search_arena_.total(), search_total_limit);
    }
  }
  second_segment->~ObIvfSSTable();
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_l2_missing_raw_vector_fails_atomically)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float zero_query[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_L2, false));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, zero_query, dim, 1);
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));

  ASSERT_EQ(OB_INVALID_DATA, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(1, stats.raw_vector_read_count_);
  ASSERT_EQ(1, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_cos_zero_query_searches_cold_and_cached_centroids_and_brute_force)
{
  const int32_t dim = 4;
  const int64_t cids[] = {0, 0};
  const int64_t vids[] = {10, 20};
  float vectors[] = {1.0F, 0.0F, 0.0F, 0.0F,
                     0.0F, 1.0F, 0.0F, 0.0F};
  float centroid[] = {1.0F, 1.0F, 0.0F, 0.0F};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors, cids, vids, 2,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, true));
  sstable_.ivf_meta_.centroid_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(
      share::ObLSID(IVF_LS_ID), ls_mgr, false /* check_ls_exist */));

  scan_context_.ls_id_ = IVF_LS_ID;
  const int64_t limits[] = {1, 1, 10};
  for (const int64_t k : limits) {
    ObIvfKnnSearchParam query_param;
    fill_search_params(query_param, query, dim, k);
    ObIvfKnnSearchResult result;
    {
      ObIvfSearcherGuard searcher;
      ASSERT_EQ(OB_SUCCESS,
          create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
      ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
    }
    ASSERT_EQ(std::min(k, static_cast<int64_t>(2)), result.count());
    for (int64_t i = 0; i < result.count(); ++i) {
      EXPECT_FLOAT_EQ(1.0F, result.at(i).distance_);
    }
    ObIvfCacheMgrGuard cache_guard;
    ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(
        share::ObLSID(IVF_LS_ID), ObIvfCacheMgrKey(sstable_.get_key()), cache_guard));
    ObIvfSSTableCentCache *cache = nullptr;
    ASSERT_EQ(OB_SUCCESS, cache_guard.get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
        IVF_SSTABLE_CENTROID_CACHE, cache));
    ASSERT_NE(nullptr, cache);
    EXPECT_TRUE(cache->is_completed());

    ObIvfSearcherGuard post_searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(sstable_, scan_param_, scan_context_, post_searcher.get()));
    ObIvfCandidateProgress progress;
    ASSERT_EQ(OB_SUCCESS, post_searcher->next_candidate_batch(query_param,
        std::numeric_limits<float>::infinity(), result, progress, 32));
    ASSERT_EQ(2, result.count());
    for (int64_t i = 0; i < result.count(); ++i) {
      EXPECT_EQ(vids[i], result.at(i).vid_);
      EXPECT_FLOAT_EQ(1.0F, result.at(i).distance_);
    }
  }

  {
    const ObIvfCacheMgrKey cache_key(sstable_.get_key());
    ObIvfCacheMgrGuard cache_guard;
    ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(
        share::ObLSID(IVF_LS_ID), cache_key, cache_guard));
    ObIvfCacheMgr *cache_mgr = cache_guard.get_ivf_cache_mgr();
    ObIvfSSTableCentCache *cache = nullptr;
    ASSERT_EQ(OB_SUCCESS, cache_mgr->get_cache_node<ObIvfSSTableCentCache>(
        IVF_SSTABLE_CENTROID_CACHE, cache));
    bool erased = false;
    uint64_t removed_hold = 0;
    ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(
        share::ObLSID(IVF_LS_ID), cache_key, cache_mgr, cache,
        cache->get_last_access_ts(), false, erased, removed_hold));
    ASSERT_TRUE(erased);
  }

  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids[1]));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 10);
  query_param.pre_filter_ = &filter;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(1, result.count());
  EXPECT_EQ(vids[1], result.at(0).vid_);
  EXPECT_FLOAT_EQ(1.0F, result.at(0).distance_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_cos_non_finite_query_rejected_before_raw_vector_io)
{
  const int32_t dim = 64;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 10;
  char flip[4 * dim / CHAR_BIT] = {0};
  centroid[2] = 0.05F;
  vector[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  const float invalid_values[] = {
      std::numeric_limits<float>::quiet_NaN(),
      std::numeric_limits<float>::infinity(),
      -std::numeric_limits<float>::infinity()};
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_values); ++i) {
    float query[dim] = {0};
    query[0] = 1.0F;
    query[dim - 1] = invalid_values[i];
    ObIvfSSTableReadStats stats;
    ObIvfKnnSearchParam query_param;
    fill_search_params(query_param, query, dim, 1);
    query_param.read_stats_ = &stats;
    ObIvfKnnSearchResult result;
    ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
    // Reject non-finite queries before normalization and RAW_VECTOR refinement.
    ASSERT_EQ(OB_INVALID_ARGUMENT, run_ivf_knn_search(
        sstable_, scan_param_, query_param, result))
        << "invalid_query_case=" << i;
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(0, stats.raw_vector_read_count_);
    ASSERT_EQ(0, stats.raw_vector_key_count_);
  }
}

TEST_F(TestIVFSSTableWriter, write_one_type_then_init_ivf_sstable)
{
  const int64_t row_cnt = 8;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_CENTROID, 0, row_cnt, 0));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ASSERT_TRUE(sstable_.is_ivf_sstable());
  ASSERT_TRUE(sstable_.is_major_sstable());
  ASSERT_TRUE(sstable_.is_valid());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, sstable_.get_meta_type());
  ASSERT_EQ(row_cnt, sstable_.get_row_count());
  ASSERT_GT(sstable_.get_data_macro_block_count(), 0);
  ASSERT_FALSE(sstable_.no_data_to_read());
  ASSERT_TRUE(sstable_.get_ivf_meta().is_valid());
  ASSERT_EQ(8, sstable_.get_ivf_meta().dim_);
}

TEST_F(TestIVFSSTableWriter, write_types_with_independent_writers)
{
  const int64_t centroid_cnt = 4;
  const int64_t posting_cnt = 6;
  const int64_t raw_cnt = 5;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_CENTROID, 0, centroid_cnt, 0));
  ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 100, posting_cnt, 1));
  ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_RAW_VECTOR, 200, raw_cnt, 0));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ASSERT_TRUE(sstable_.is_ivf_sstable());
  ASSERT_EQ(centroid_cnt + posting_cnt + raw_cnt, sstable_.get_row_count());
  ASSERT_GE(sstable_.get_data_macro_block_count(), 3);
}
class TestIVFSSTableSearcher final : public ObIvfSSTableSearcher
{
public:
  TestIVFSSTableSearcher(
      ObIAllocator &allocator, int do_ret, bool *destroyed = nullptr)
    : ObIvfSSTableSearcher(allocator),
      do_ret_(do_ret),
      calls_(0),
      brute_force_ret_(OB_SUCCESS),
      brute_force_calls_(0),
      destroyed_(destroyed)
  {}
  ~TestIVFSSTableSearcher() override
  {
    if (nullptr != destroyed_) {
      *destroyed_ = true;
    }
  }
  int open_for_test(
      ObTableScanParam &scan_param,
      ObTableAccessContext &access_ctx,
      ObIvfSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObTableHandleV2 table_handle;
    if (OB_FAIL(table_handle.set_sstable_with_tablet(&sstable))) {
    } else if (OB_FAIL(init_common(table_handle, scan_param, access_ctx))) {
    }
    return ret;
  }
  int create_centroid_for_test(
      const ObIvfKnnSearchParam &query_param,
      const float *rotated_query)
  { return create_centroid_iterator(query_param, rotated_query); }
  ObIvfCentroidIterator *borrowed_centroid() const { return centroid_iterator(); }
  ObIvfSSTable *opened_sstable() const { return sstable(); }
  bool initialized_for_test() const { return is_inited_; }
protected:
  int do_knn_search(
      const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &collector) override
  {
    ++calls_;
    return OB_SUCCESS == do_ret_
        ? collector.push(1, static_cast<int64_t>(query_param.query_[0]), 0.25f)
        : do_ret_;
  }
  int do_brute_force_search(
      const ObIvfKnnSearchParam &query_param,
      ObIvfTopKCollector &collector) override
  {
    ++brute_force_calls_;
    return OB_SUCCESS == brute_force_ret_
        ? collector.push(1, static_cast<int64_t>(query_param.query_[0]), 0.25f)
        : brute_force_ret_;
  }
public:
  int do_ret_;
  int64_t calls_;
  int brute_force_ret_;
  int64_t brute_force_calls_;
  bool *destroyed_;
};

TEST_F(TestIVFSSTableWriter,
    ivf_searcher_large_topk_rounds_keep_statement_memory_bounded)
{
  const int32_t dim = 8;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  ObArenaAllocator statement_allocator;
  ObTableScanParam scan_param;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_ctx;
  prepare_ivf_scan_param(scan_param_, statement_allocator, scan_param);
  ASSERT_EQ(OB_SUCCESS,
      init_ivf_access_context(sstable_, scan_param, store_ctx, access_ctx));
  TestIVFSSTableSearcher searcher(statement_allocator, OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, searcher.open_for_test(scan_param, access_ctx, sstable_));
  float query[dim] = {7};
  ObIvfKnnSearchParam param;
  // LIMIT 11001 with the post-filter candidate expansion used by nprobe cases.
  fill_search_params(param, query, dim, 110010);
  param.nprobe_ = 1;
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(7));
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed));
  param.pre_filter_ = &pre_filter;

  ObIvfKnnSearchResult first_result;
  ASSERT_EQ(OB_SUCCESS, searcher.knn_search(param, first_result));
  ASSERT_EQ(1, first_result.count());
  const int64_t statement_used = statement_allocator.used();
  const int64_t round_total = searcher.search_arena_.total();
  ObIvfKnnSearchResult result;
  for (int64_t round = 0; round < 128; ++round) {
    SCOPED_TRACE(round);
    ASSERT_EQ(OB_SUCCESS, searcher.knn_search(param, result));
    ASSERT_EQ(1, result.count());
    ASSERT_EQ(7, result.at(0).vid_);
    ASSERT_EQ(OB_SUCCESS, searcher.brute_force_search(param, result));
    ASSERT_EQ(1, result.count());
    ASSERT_EQ(7, result.at(0).vid_);
    ASSERT_EQ(statement_used, statement_allocator.used());
    ASSERT_LE(searcher.search_arena_.total(), round_total);
  }
  searcher.reuse();
  // Results own their values and survive reuse of the collector's scratch.
  ASSERT_EQ(1, first_result.count());
  ASSERT_EQ(7, first_result.at(0).vid_);
  ASSERT_FLOAT_EQ(0.25F, first_result.at(0).distance_);
}

TEST_F(TestIVFSSTableWriter, ivf_result_expansion_uses_parent_and_releases_before_parent)
{
  ObIvfCountingAllocator parent(*lib::ObMallocAllocator::get_instance());
  for (int retry = 0; retry < 2; ++retry) {
    ObIvfKnnSearchResult result(parent);
    for (int64_t i = 0; i < 129; ++i) {
      ObIvfKnnEntry entry;
      entry.vid_ = i;
      ASSERT_EQ(OB_SUCCESS, result.add(entry));
    }
    ASSERT_GT(parent.live_allocation_count(), 0);
    ASSERT_EQ(128, result.at(128).vid_);
    result.reset();
    EXPECT_EQ(0, parent.live_allocation_count());
  }
  EXPECT_EQ(parent.alloc_count_, parent.free_count_);
  ObArenaAllocator arena_parent(parent);
  {
    ObIvfKnnSearchResult result(arena_parent);
    for (int64_t i = 0; i < 65; ++i) {
      ObIvfKnnEntry entry;
      entry.vid_ = i;
      ASSERT_EQ(OB_SUCCESS, result.add(entry));
    }
  }
  EXPECT_GT(parent.live_allocation_count(), 0);
  arena_parent.reset();
  EXPECT_EQ(0, parent.live_allocation_count());
}

TEST_F(TestIVFSSTableWriter, ivf_result_memattr_preserves_heap_tenant_and_label)
{
  const uint64_t tenant_id = MTL_ID();
  ASSERT_NE(OB_SERVER_TENANT_ID, tenant_id);
  ASSERT_NE(OB_INVALID_TENANT_ID, tenant_id);
  const ObMemAttr attr(tenant_id, "DASIVFTmp");
  // Inspect live allocation headers: monitoring label snapshots are refreshed
  // asynchronously and cannot prove the ownership of these short-lived buffers.
  const auto check_allocation = [&](const ObIvfKnnSearchResult &result) {
    ASSERT_GT(result.count(), 16);
#ifndef OB_USE_ASAN
    const auto *object = reinterpret_cast<const lib::AObject *>(
        reinterpret_cast<const char *>(&result.at(0)) - lib::AOBJECT_HEADER_SIZE);
    ASSERT_TRUE(object->is_valid());
    EXPECT_STREQ("DASIVFTmp", object->label());
    auto *block_set = object->block()->chunk()->block_set_;
    ASSERT_NE(nullptr, block_set);
    auto *owner = block_set->get_tenant_ctx_allocator();
    ASSERT_NE(nullptr, owner);
    EXPECT_EQ(tenant_id, owner->get_tenant_id());
    EXPECT_EQ(attr.ctx_id_, owner->get_ctx_id());
#endif
  };
  for (int retry = 0; retry < 2; ++retry) {
    ObIvfKnnSearchResult result(attr);
    const auto *inline_data = result.items_.get_data();
    for (const int64_t count : {17, 64, 1024}) {
      for (int64_t i = result.count(); i < count; ++i) {
        ObIvfKnnEntry entry;
        entry.vid_ = i;
        entry.distance_ = static_cast<float>(i);
        ASSERT_EQ(OB_SUCCESS, result.add(entry));
      }
      ASSERT_NE(inline_data, result.items_.get_data());
      check_allocation(result);
      EXPECT_EQ(count - 1, result.at(count - 1).vid_);
      result.reset();
      EXPECT_EQ(0, result.count());
      EXPECT_EQ(inline_data, result.items_.get_data());
    }
    // Also destroy a populated heap result without an explicit reset.
    for (int64_t i = 0; i < 33; ++i) {
      ObIvfKnnEntry entry;
      entry.vid_ = i;
      ASSERT_EQ(OB_SUCCESS, result.add(entry));
    }
    check_allocation(result);
  }
}

TEST(TestAsyncIVFHybridRoot, epoch_candidates_are_unique_before_post_maps_and_reset)
{
  ObArenaAllocator alloc;
  sql::ObDASAsyncIVFCandidateSet seen(alloc);
  ObSEArray<sql::ObDASVectorCandidate, 4> round;
  sql::ObDASVectorCandidate candidate;
  candidate.group_slot_ = 0;
  candidate.segment_slot_ = 0;
  candidate.entry_.vid_ = 7;
  candidate.entry_.distance_ = 1;
  ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
  candidate.entry_.cid_ = 1;
  ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
  ASSERT_EQ(OB_SUCCESS, seen.retain_new(round));
  ASSERT_EQ(1, round.count()); // both POST modes can insert a unique VID map
  round.reuse();
  ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
  ASSERT_EQ(OB_SUCCESS, seen.retain_new(round));
  ASSERT_EQ(0, round.count()); // still only one effective result at K=2
  candidate.entry_.vid_ = 9; // same vector/distance, different logical identity
  candidate.entry_.cid_ = 2;
  ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
  ASSERT_EQ(OB_SUCCESS, seen.retain_new(round));
  ASSERT_EQ(1, round.count());
  ASSERT_EQ(9, round.at(0).entry_.vid_);
  round.reuse();
  candidate.segment_slot_ = 1;
  candidate.group_slot_ = 1;
  ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
  ASSERT_EQ(OB_SUCCESS, seen.retain_new(round));
  ASSERT_EQ(0, round.count()); // repeated visible VID across physical segments
  seen.reset(); // rescan/new query/new partition must allow the same bare VID
  ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
  ASSERT_EQ(OB_SUCCESS, seen.retain_new(round));
  ASSERT_EQ(1, round.count());
  candidate.entry_.vid_ = -1;
  ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
  ASSERT_EQ(OB_ERR_UNEXPECTED, seen.retain_new(round));
  EXPECT_EQ(0, round.count());
}

TEST(TestAsyncIVFHybridRoot, epoch_candidate_allocation_failure_is_atomic)
{
  for (int64_t fail_on : {1, 2}) {
    ObIvfCountingAllocator parent(*lib::ObMallocAllocator::get_instance());
    parent.set_fail_on_alloc(fail_on);
    sql::ObDASAsyncIVFCandidateSet seen(parent);
    ObSEArray<sql::ObDASVectorCandidate, 4> round;
    sql::ObDASVectorCandidate candidate;
    candidate.group_slot_ = 0; candidate.segment_slot_ = 0;
    candidate.entry_.vid_ = 7;
    ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, seen.retain_new(round));
    EXPECT_EQ(0, round.count());
    EXPECT_EQ(0, parent.live_allocation_count());
    parent.set_fail_on_alloc(0);
    ASSERT_EQ(OB_SUCCESS, round.push_back(candidate));
    ASSERT_EQ(OB_SUCCESS, seen.retain_new(round));
    ASSERT_EQ(1, round.count());
    seen.reset();
    EXPECT_EQ(0, parent.live_allocation_count());
  }
}

TEST_F(TestIVFSSTableWriter,
    ivf_searcher_static_destroy_dispatches_derived_destructor)
{
  ObIvfCountingAllocator counting_allocator(allocator_);
  bool destroyed = false;
  ObIvfSSTableSearcher *searcher = OB_NEWx(
      TestIVFSSTableSearcher, &counting_allocator,
      counting_allocator, OB_SUCCESS, &destroyed);
  ASSERT_NE(nullptr, searcher);
  ASSERT_EQ(1, counting_allocator.live_allocation_count());

  ObIvfSSTableSearcher::destroy(searcher);
  searcher = nullptr;

  ASSERT_TRUE(destroyed);
  ASSERT_EQ(0, counting_allocator.live_allocation_count());
  ASSERT_EQ(counting_allocator.alloc_count_, counting_allocator.free_count_);
}

TEST_F(TestIVFSSTableWriter, ivf_prefilter_eager_bitmap_shared_iterators)
{
  const int64_t sizes[] = {0, 1, 8, 256};
  const uint64_t first_vid = (uint64_t(1) << 40);
  ObRoaringBitmap allowed(&allocator_);
  for (const int64_t size : sizes) {
    ASSERT_EQ(OB_SUCCESS, allowed.convert_to_bitmap());
    for (int64_t i = size; i > 0; --i) {
      ASSERT_EQ(OB_SUCCESS, allowed.value_add(first_vid + i - 1));
      ASSERT_EQ(OB_SUCCESS, allowed.value_add(first_vid + i - 1));
    }
    ObIvfPreFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
    ASSERT_EQ(&allowed, filter.get_allowed_vids());
    ASSERT_EQ(size, allowed.get_cardinality());
    ASSERT_EQ(0 == size, filter.is_empty());
    int64_t min_vid = -1;
    int64_t max_vid = -1;
    ASSERT_EQ(0 == size ? OB_ITER_END : OB_SUCCESS,
        filter.get_vid_range(min_vid, max_vid));
    if (size > 0) {
      ASSERT_EQ(first_vid, min_vid);
      ASSERT_EQ(first_vid + size - 1, max_vid);
    }
    ASSERT_FALSE(filter.contains(-1));
    ASSERT_FALSE(filter.contains(first_vid + size));
    ObRoaringBitmapIter first(filter.get_allowed_vids());
    ObRoaringBitmapIter second(filter.get_allowed_vids());
    ASSERT_EQ(0 == size ? OB_ITER_END : OB_SUCCESS, first.init());
    ASSERT_EQ(0 == size ? OB_ITER_END : OB_SUCCESS, second.init());
    for (int64_t i = 0; i < size; ++i) {
      ASSERT_EQ(first_vid + i, first.get_curr_value());
      ASSERT_TRUE(filter.contains(first_vid + i));
      // Advancing one scanner must leave the other scanner's cursor unchanged.
      ASSERT_EQ(i + 1 == size ? OB_ITER_END : OB_SUCCESS, first.get_next());
      ASSERT_EQ(first_vid + i, second.get_curr_value());
      ASSERT_EQ(i + 1 == size ? OB_ITER_END : OB_SUCCESS, second.get_next());
    }
    first.deinit();
    second.deinit();
    filter.reset();
    ASSERT_EQ(size, allowed.get_cardinality());
    allowed.set_empty();
  }
}

TEST_F(TestIVFSSTableWriter, compact_prefilter_matches_roaring_and_independent_iterators)
{
  const int64_t sizes[] = {0, 1, 32, 33, 10000, 10001};
  for (int64_t s = 0; s < ARRAYSIZEOF(sizes); ++s) {
    blocksstable::ObIvfPreFilterBitmap bitmap(allocator_);
    ObRoaringBitmap reference(&allocator_);
    const uint64_t base = (uint64_t(1) << 40) + 17;
    uint64_t batch[37] = {0};
    for (int64_t end = sizes[s]; end > 0;) {
      const int64_t count = MIN(end, 37);
      for (int64_t i = 0; i < count; ++i) {
        batch[i] = base + (end - i - 1) * 3;
        ASSERT_EQ(OB_SUCCESS, reference.value_add(batch[i]));
      }
      ASSERT_EQ(OB_SUCCESS, bitmap.value_add_many(batch, count));
      ASSERT_EQ(OB_SUCCESS, bitmap.value_add_many(batch, count));
      end -= count;
    }
    ASSERT_EQ(sizes[s], bitmap.get_cardinality());
    ASSERT_EQ(sizes[s] > MAX_BITMAP_SET_VALUES, bitmap.is_compact());
    ObIvfPreFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(bitmap));
    int64_t min_vid = -1;
    int64_t max_vid = -1;
    ASSERT_EQ(0 == sizes[s] ? OB_ITER_END : OB_SUCCESS,
        filter.get_vid_range(min_vid, max_vid));
    if (sizes[s] > 0) {
      ASSERT_EQ(base, min_vid);
      ASSERT_EQ(base + (sizes[s] - 1) * 3, max_vid);
    }
    blocksstable::ObIvfPreFilterIter first(filter);
    blocksstable::ObIvfPreFilterIter second(filter);
    ObRoaringBitmapIter expected(&reference);
    ASSERT_EQ(sizes[s] == 0 ? OB_ITER_END : OB_SUCCESS, first.init());
    ASSERT_EQ(sizes[s] == 0 ? OB_ITER_END : OB_SUCCESS, second.init());
    ASSERT_EQ(sizes[s] == 0 ? OB_ITER_END : OB_SUCCESS, expected.init());
    if (sizes[s] <= MAX_BITMAP_SET_VALUES) {
      ASSERT_FALSE(bitmap.get_roaring()->is_bitmap_type());
    }
    for (int64_t i = 0; i < sizes[s]; ++i) {
      const uint64_t vid = expected.get_curr_value();
      ASSERT_EQ(vid, first.get_curr_value());
      ASSERT_TRUE(filter.contains(vid));
      ASSERT_FALSE(filter.contains(vid + 1));
      ASSERT_EQ(i + 1 == sizes[s] ? OB_ITER_END : OB_SUCCESS, first.get_next());
      ASSERT_EQ(vid, second.get_curr_value());
      ASSERT_EQ(i + 1 == sizes[s] ? OB_ITER_END : OB_SUCCESS, second.get_next());
      ASSERT_EQ(i + 1 == sizes[s] ? OB_ITER_END : OB_SUCCESS, expected.get_next());
    }
    first.deinit();
    ASSERT_EQ(sizes[s] == 0 ? OB_ITER_END : OB_SUCCESS, first.init());
    expected.deinit();
    reference.set_empty();
  }
}

TEST_F(TestIVFSSTableWriter, adaptive_prefilter_preserves_members_for_dense_and_sparse_distributions)
{
  const uint64_t base = (uint64_t(1) << 40) + 17;
  for (const uint64_t stride : {uint64_t(1), uint64_t(10), uint64_t(4096)}) {
    for (const int64_t estimate : {-1, 0, 1, 1000, 1000000}) {
      SCOPED_TRACE(::testing::Message() << "stride=" << stride << " estimate=" << estimate);
      blocksstable::ObIvfPreFilterBitmap bitmap(allocator_, estimate);
      std::vector<uint64_t> expected;
      for (int64_t end = 1000; end > 0;) {
        uint64_t batch[37];
        const int64_t count = MIN(end, 37);
        for (int64_t i = 0; i < count; ++i) {
          batch[i] = base + (end - i - 1) * stride;
          expected.push_back(batch[i]);
        }
        ASSERT_EQ(OB_SUCCESS, bitmap.value_add_many(batch, count));
        ASSERT_EQ(OB_SUCCESS, bitmap.value_add_many(batch, count));
        end -= count;
      }
      ASSERT_EQ(OB_SUCCESS, bitmap.finish());
      ASSERT_EQ(1000, bitmap.get_cardinality());
      ASSERT_EQ(stride <= 10, bitmap.is_compact());
      ASSERT_EQ(stride > 10, bitmap.is_roaring());
      std::sort(expected.begin(), expected.end());
      ObIvfPreFilter filter;
      ASSERT_EQ(OB_SUCCESS, filter.init(bitmap));
      ObIvfPreFilterIter first(filter), second(filter);
      ASSERT_EQ(OB_SUCCESS, first.init());
      ASSERT_EQ(OB_SUCCESS, second.init());
      for (int64_t i = 0; i < expected.size(); ++i) {
        EXPECT_TRUE(bitmap.is_contains(expected[i]));
        EXPECT_EQ(expected[i], first.get_curr_value());
        EXPECT_EQ(expected[i], second.get_curr_value());
        EXPECT_EQ(i + 1 == expected.size() ? OB_ITER_END : OB_SUCCESS, first.get_next());
        EXPECT_EQ(i + 1 == expected.size() ? OB_ITER_END : OB_SUCCESS, second.get_next());
      }
      EXPECT_FALSE(bitmap.is_contains(base - 1));
      EXPECT_FALSE(bitmap.is_contains(base + 1000 * stride));
      // Repeated finalization must preserve the immutable view.
      ASSERT_EQ(OB_SUCCESS, bitmap.finish());
    }
  }
}

TEST_F(TestIVFSSTableWriter, adaptive_prefilter_estimate_selects_early_and_underestimate_promotes)
{
  auto *mem_mgr = MTL(ObRbMemMgr *);
  ASSERT_NE(nullptr, mem_mgr);
  const int64_t baseline = mem_mgr->get_vec_idx_used();
  blocksstable::ObIvfPreFilterBitmap sparse(allocator_, 64), dense(allocator_, 1000000);
  uint64_t batch[64];
  for (uint64_t i = 0; i < 64; ++i) {
    batch[i] = i * 1024;
  }
  ASSERT_EQ(OB_SUCCESS, sparse.value_add_many(batch, 64));
  ASSERT_EQ(OB_SUCCESS, dense.value_add_many(batch, 64));
  ASSERT_TRUE(sparse.is_roaring());
  ASSERT_TRUE(dense.is_compact());
  // Fill the same bounded range: the sparse estimate must not lock the set
  // into Roaring, nor pay Roaring insertion for the entire dense result.
  for (uint64_t start = 0; start < 4096; start += 64) {
    for (uint64_t i = 0; i < 64; ++i) {
      batch[i] = start + i;
    }
    ASSERT_EQ(OB_SUCCESS, sparse.value_add_many(batch, 64));
  }
  ASSERT_TRUE(sparse.is_compact());
  EXPECT_EQ(baseline + (sparse.get_bit_count() + dense.get_bit_count()) / 8,
      mem_mgr->get_vec_idx_used());
  ASSERT_EQ(OB_SUCCESS, sparse.finish());
  ASSERT_EQ(OB_SUCCESS, dense.finish());
  ASSERT_TRUE(dense.is_roaring());
  EXPECT_EQ(64, dense.get_cardinality());
  EXPECT_EQ(4156, sparse.get_cardinality());
  for (uint64_t i = 0; i < 64; ++i) {
    EXPECT_TRUE(sparse.is_contains(i * 1024));
    EXPECT_TRUE(dense.is_contains(i * 1024));
  }
  dense.reset();
  sparse.reset();
  EXPECT_EQ(baseline, mem_mgr->get_vec_idx_used());
}

TEST_F(TestIVFSSTableWriter, adaptive_prefilter_final_density_boundary_and_span_limit)
{
  for (const int64_t count : {63, 64}) {
    blocksstable::ObIvfPreFilterBitmap bitmap(allocator_, 0);
    for (int64_t i = 0; i + 1 < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, bitmap.value_add(i));
    }
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(8191));
    ASSERT_EQ(OB_SUCCESS, bitmap.finish());
    EXPECT_EQ(count == 64, bitmap.is_compact());
    EXPECT_EQ(count, bitmap.get_cardinality());
    EXPECT_TRUE(bitmap.is_contains(8191));
  }
  blocksstable::ObIvfPreFilterBitmap bitmap(allocator_, INT64_MAX);
  for (uint64_t i = 0; i < 64; ++i) {
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(i));
  }
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(INT64_MAX));
  ASSERT_EQ(OB_SUCCESS, bitmap.finish());
  EXPECT_TRUE(bitmap.is_roaring());
  EXPECT_EQ(65, bitmap.get_cardinality());
  EXPECT_TRUE(bitmap.is_contains(INT64_MAX));
  bitmap.reset();
  EXPECT_EQ(-1, bitmap.estimated_cardinality_);
  ASSERT_EQ(OB_SUCCESS, bitmap.finish());
}

TEST_F(TestIVFSSTableWriter, adaptive_prefilter_promotion_failure_preserves_roaring_until_reset)
{
  ObIvfCountingAllocator allocator(allocator_);
  blocksstable::ObIvfPreFilterBitmap bitmap(allocator, 0);
  bitmap.compact_alloc_ = &allocator;
  auto *mem_mgr = MTL(ObRbMemMgr *);
  ASSERT_NE(nullptr, mem_mgr);
  const int64_t baseline = mem_mgr->get_vec_idx_used();
  uint64_t vids[64];
  for (uint64_t i = 0; i < 64; ++i) {
    vids[i] = i * 128;
  }
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add_many(vids, 64));
  ASSERT_TRUE(bitmap.is_roaring());
  allocator.set_fail_on_alloc(allocator.alloc_count_ + 1);
  // Final density is above 1/128, while construction requires 1/64 to promote.
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, bitmap.finish());
  EXPECT_FALSE(bitmap.is_valid());
  EXPECT_TRUE(bitmap.is_roaring());
  EXPECT_EQ(64, bitmap.get_cardinality());
  for (uint64_t i = 0; i < 64; ++i) {
    EXPECT_TRUE(bitmap.get_roaring()->is_contains(i * 128));
  }
  EXPECT_EQ(OB_STATE_NOT_MATCH, bitmap.finish());
  bitmap.reset();
  EXPECT_EQ(0, allocator.live_allocation_count());
  EXPECT_EQ(baseline, mem_mgr->get_vec_idx_used());
}

TEST_F(TestIVFSSTableWriter, adaptive_prefilter_accounts_compact_roaring_and_iterator_memory)
{
  auto *mem_mgr = MTL(ObRbMemMgr *);
  ASSERT_NE(nullptr, mem_mgr);
  const int64_t baseline = mem_mgr->get_vec_idx_used();
  // Label usage views are asynchronously refreshed by ObMemoryDump. Inspect
  // the live allocation header instead of reading a stale monitoring snapshot.
  const auto allocation_label = [](const void *ptr) {
    const auto *object = reinterpret_cast<const lib::AObject *>(
        static_cast<const char *>(ptr) - lib::AOBJECT_HEADER_SIZE);
    return object->label();
  };
  {
    // The implementation must establish its own label, even under another
    // allocation guard, and restore that guard before returning to the caller.
    lib::ObMallocHookAttrGuard outer_guard(lib::ObMemAttr(MTL_ID(), "IvfBitmapTest"));
    ObIvfPreFilterBitmap bitmap(allocator_);
    for (uint64_t i = 1024; i < 1057; ++i) {
      ASSERT_EQ(OB_SUCCESS, bitmap.value_add(i));
    }
    ASSERT_TRUE(bitmap.is_compact());
    EXPECT_EQ(baseline + bitmap.get_bit_count() / 8, mem_mgr->get_vec_idx_used());
    EXPECT_EQ(MTL_ID(), bitmap.compact_allocator_.memattr_.tenant_id_);
    EXPECT_STREQ("VIBitmapADPV", allocation_label(bitmap.get_words()));
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(0));
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(8192));
    EXPECT_EQ(baseline + bitmap.get_bit_count() / 8, mem_mgr->get_vec_idx_used());
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(ObIvfPreFilterBitmap::MAX_COMPACT_BITS + 1));
    ASSERT_TRUE(bitmap.is_roaring());
    const int64_t roaring_used = mem_mgr->get_vec_idx_used();
    EXPECT_GT(roaring_used, baseline);
    // The Roaring memory hook prefixes its payload with manager, tenant, size.
    const char *roaring_payload = reinterpret_cast<const char *>(bitmap.get_roaring()->get_bitmap());
    const void *roaring_allocation = roaring_payload - sizeof(ObRbMemMgr *) - sizeof(uint64_t) - sizeof(size_t);
    EXPECT_STREQ("VIBitmapADPV", allocation_label(roaring_allocation));
    ObIvfPreFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(bitmap));
    {
      ObIvfPreFilterIter iter(filter);
      ASSERT_EQ(OB_SUCCESS, iter.init());
      EXPECT_GT(mem_mgr->get_vec_idx_used(), roaring_used);
      int ret = OB_SUCCESS;
      while (OB_SUCCESS == (ret = iter.get_next())) {}
      EXPECT_EQ(OB_ITER_END, ret);
    }
    EXPECT_EQ(roaring_used, mem_mgr->get_vec_idx_used());
    bitmap.reset();
    EXPECT_EQ(baseline, mem_mgr->get_vec_idx_used());
    // Reuse and destruction must account for a fresh compact allocation too.
    for (uint64_t i = 0; i < 33; ++i) {
      ASSERT_EQ(OB_SUCCESS, bitmap.value_add(i));
    }
    EXPECT_GT(mem_mgr->get_vec_idx_used(), baseline);
    EXPECT_STREQ("IvfBitmapTest", lib::ObMallocHookAttrGuard::get_tl_mem_attr().label_);
  }
  EXPECT_EQ(baseline, mem_mgr->get_vec_idx_used());
}

TEST_F(TestIVFSSTableWriter, compact_prefilter_grows_both_sides_and_falls_back)
{
  blocksstable::ObIvfPreFilterBitmap bitmap(allocator_);
  for (uint64_t i = 1024; i < 1057; ++i) {
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(i));
  }
  ASSERT_TRUE(bitmap.is_compact());
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(100));
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(8192));
  ASSERT_TRUE(bitmap.is_compact());
  ASSERT_LE(bitmap.get_bit_count(), ObIvfPreFilterBitmap::MAX_COMPACT_BITS);
  ASSERT_EQ(35, bitmap.get_cardinality());
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(ObIvfPreFilterBitmap::MAX_COMPACT_BITS + 100));
  ASSERT_TRUE(bitmap.is_roaring());
  ASSERT_FALSE(bitmap.is_compact());
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(100));
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(INT64_MAX));
  ASSERT_EQ(37, bitmap.get_cardinality());
  ASSERT_TRUE(bitmap.is_contains(INT64_MAX));
  for (uint64_t i = 1024; i < 1057; ++i) {
    ASSERT_TRUE(bitmap.is_contains(i));
  }
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(bitmap));
  int64_t min_vid = -1;
  int64_t max_vid = -1;
  ASSERT_EQ(OB_SUCCESS, filter.get_vid_range(min_vid, max_vid));
  ASSERT_EQ(100, min_vid);
  ASSERT_EQ(INT64_MAX, max_vid);
  blocksstable::ObIvfPreFilterIter iter(filter);
  ASSERT_EQ(OB_SUCCESS, iter.init());
  ASSERT_EQ(100, iter.get_curr_value());
  uint64_t previous = 0;
  int ret = OB_SUCCESS;
  int64_t count = 0;
  while (OB_SUCCESS == ret) {
    ASSERT_GT(iter.get_curr_value(), previous);
    previous = iter.get_curr_value();
    ++count;
    ret = iter.get_next();
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(37, count);
  ASSERT_EQ(INT64_MAX, previous);
}

TEST_F(TestIVFSSTableWriter, compact_prefilter_capacity_is_bounded)
{
  blocksstable::ObIvfPreFilterBitmap bitmap(allocator_);
  const uint64_t max_bits = ObIvfPreFilterBitmap::MAX_COMPACT_BITS;
  for (uint64_t i = 0; i < 33; ++i) {
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(i));
  }
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(max_bits - 1));
  ASSERT_TRUE(bitmap.is_compact());
  ASSERT_EQ(max_bits, bitmap.get_bit_count());
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(max_bits));
  ASSERT_TRUE(bitmap.is_roaring());
  ASSERT_FALSE(bitmap.is_compact());
  ASSERT_EQ(35, bitmap.get_cardinality());
  ASSERT_TRUE(bitmap.is_contains(max_bits - 1));
  ASSERT_TRUE(bitmap.is_contains(max_bits));
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(0));
  ASSERT_TRUE(bitmap.is_roaring());
}

TEST_F(TestIVFSSTableWriter, compact_prefilter_signed_limit_and_invalid_batches)
{
  blocksstable::ObIvfPreFilterBitmap bitmap(allocator_);
  for (uint64_t i = 0; i < 80; ++i) {
    ASSERT_EQ(OB_SUCCESS, bitmap.value_add(INT64_MAX - i));
  }
  ASSERT_TRUE(bitmap.is_compact());
  ASSERT_EQ(80, bitmap.get_cardinality());
  ASSERT_TRUE(bitmap.is_contains(INT64_MAX));
  ASSERT_FALSE(bitmap.is_contains(uint64_t(INT64_MAX) + 1));
  ObIvfPreFilter valid_filter;
  ASSERT_EQ(OB_SUCCESS, valid_filter.init(bitmap));
  blocksstable::ObIvfPreFilterIter iter(valid_filter);
  ASSERT_EQ(OB_SUCCESS, iter.init());
  for (uint64_t i = 0; i < 80; ++i) {
    ASSERT_EQ(uint64_t(INT64_MAX) - 79 + i, iter.get_curr_value());
    ASSERT_EQ(i == 79 ? OB_ITER_END : OB_SUCCESS, iter.get_next());
  }
  iter.deinit();
  valid_filter.reset();
  const uint64_t invalid[] = {0, UINT64_MAX};
  ASSERT_EQ(OB_INVALID_ARGUMENT, bitmap.value_add_many(invalid, 2));
  ASSERT_EQ(80, bitmap.get_cardinality());
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.init(bitmap));
  ASSERT_EQ(OB_STATE_NOT_MATCH, bitmap.value_add(0));
  bitmap.reset();
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add_many(nullptr, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, bitmap.value_add_many(nullptr, 1));
  bitmap.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, bitmap.value_add_many(invalid, -1));
  bitmap.reset();
  ASSERT_EQ(OB_SUCCESS, bitmap.value_add(0));
  ASSERT_EQ(1, bitmap.get_cardinality());
}

TEST_F(TestIVFSSTableWriter, compact_prefilter_allocation_failure_and_cleanup)
{
  ObIvfCountingAllocator allocator(allocator_);
  blocksstable::ObIvfPreFilterBitmap bitmap(allocator);
  bitmap.compact_alloc_ = &allocator;
  auto *mem_mgr = MTL(ObRbMemMgr *);
  ASSERT_NE(nullptr, mem_mgr);
  const int64_t baseline = mem_mgr->get_vec_idx_used();
  for (int64_t phase = 0; phase < 5; ++phase) {
    allocator.set_fail_on_alloc(0);
    bitmap.reset();
    const int64_t count = (phase == 0 || phase == 4) ? 32 : 33;
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, bitmap.value_add(1024 + i));
    }
    allocator.set_fail_on_alloc(allocator.alloc_count_ + 1);
    const uint64_t next = phase == 0 ? 1056 : phase == 1 ? 0 : phase == 2 ? 8192
        : ObIvfPreFilterBitmap::MAX_COMPACT_BITS + 1024;
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, bitmap.value_add(next));
    ASSERT_FALSE(bitmap.is_valid());
    ASSERT_EQ(count, bitmap.get_cardinality());
    bitmap.reset();
    ASSERT_EQ(0, allocator.live_allocation_count());
    ASSERT_EQ(baseline, mem_mgr->get_vec_idx_used());
  }
}

TEST_F(TestIVFSSTableWriter, ivf_searcher_ignores_prefilters)
{
  const int32_t dim = 8;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(10));
  ObIvfPreFilter valid_filter;
  ASSERT_EQ(OB_SUCCESS, valid_filter.init(allowed));
  ObIvfKnnSearchResult result;
  TestIVFSSTableSearcher filtered_searcher(allocator_, OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, filtered_searcher.open_for_test(scan_param_, scan_context_, sstable_));
  query_param.pre_filter_ = &valid_filter;
  ASSERT_EQ(OB_SUCCESS, filtered_searcher.knn_search(query_param, result));
  ASSERT_EQ(1, filtered_searcher.calls_);
  ASSERT_EQ(1, result.count());
  query_param.pre_filter_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, filtered_searcher.knn_search(query_param, result));
  ASSERT_EQ(2, filtered_searcher.calls_);
  TestIVFSSTableSearcher invalid_filter_searcher(allocator_, OB_SUCCESS);
  ObIvfPreFilter invalid_filter;
  ASSERT_EQ(OB_SUCCESS, invalid_filter_searcher.open_for_test(scan_param_, scan_context_, sstable_));
  query_param.pre_filter_ = &invalid_filter;
  ASSERT_EQ(OB_SUCCESS, invalid_filter_searcher.knn_search(query_param, result));
  ASSERT_EQ(1, invalid_filter_searcher.calls_);
  ASSERT_EQ(1, result.count());
  query_param.pre_filter_ = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      invalid_filter_searcher.knn_search(query_param, result));
  ASSERT_EQ(2, invalid_filter_searcher.calls_);
}

TEST_F(TestIVFSSTableWriter,
    ivf_searcher_brute_force_uses_operation_specific_validation)
{
  const int32_t dim = 8;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {7};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.nprobe_ = 0;
  ObIvfKnnSearchResult result;
  TestIVFSSTableSearcher unopened_searcher(allocator_, OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_NOT_INIT, unopened_searcher.brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, unopened_searcher.brute_force_calls_);

  TestIVFSSTableSearcher searcher(allocator_, OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, searcher.open_for_test(scan_param_, scan_context_, sstable_));
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher.brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, searcher.brute_force_calls_);

  ObIvfPreFilter uninitialized_filter;
  query_param.pre_filter_ = &uninitialized_filter;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher.brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, searcher.brute_force_calls_);

  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(7));
  ObIvfPreFilter initialized_filter;
  ASSERT_EQ(OB_SUCCESS, initialized_filter.init(allowed));
  ObRoaringBitmapIter allowed_iter(initialized_filter.get_allowed_vids());
  ASSERT_EQ(OB_SUCCESS, allowed_iter.init());
  ASSERT_EQ(7, allowed_iter.get_curr_value());
  allowed_iter.deinit();
  query_param.pre_filter_ = &initialized_filter;
  ASSERT_EQ(OB_SUCCESS, searcher.brute_force_search(query_param, result));
  ASSERT_EQ(1, searcher.brute_force_calls_);
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(7, result.at(0).vid_);

  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher.knn_search(query_param, result));
  ASSERT_EQ(0, searcher.calls_);
  ASSERT_EQ(0, result.count());

  searcher.brute_force_ret_ = OB_ERR_UNEXPECTED;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_ERR_UNEXPECTED, searcher.brute_force_search(query_param, result));
  ASSERT_EQ(2, searcher.brute_force_calls_);
  ASSERT_EQ(0, result.count());
}

TEST_F(TestIVFSSTableWriter, ivf_searcher_lifecycle_preopen_does_not_consume_initialization)
{
  const int32_t dim = 8;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  TestIVFSSTableSearcher searcher(allocator_, OB_SUCCESS);
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_NOT_INIT, searcher.knn_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, searcher.calls_);
  ASSERT_EQ(OB_SUCCESS,
      searcher.open_for_test(scan_param_, scan_context_, sstable_));
  ASSERT_EQ(OB_SUCCESS, searcher.knn_search(query_param, result));
  ASSERT_EQ(1, searcher.calls_);
}

TEST_F(TestIVFSSTableWriter, ivf_searcher_lifecycle_reopen_preserves_original_state)
{
  const int32_t dim = 8;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObIvfCountingAllocator counting_allocator(allocator_);
  ObTableScanParam counting_scan_param;
  ObStoreCtx counting_store_ctx;
  ObTableAccessContext counting_access_ctx;
  prepare_ivf_scan_param(scan_param_, counting_allocator, counting_scan_param);
  ASSERT_EQ(OB_SUCCESS, init_ivf_access_context(
      sstable_, counting_scan_param, counting_store_ctx, counting_access_ctx));
  {
    TestIVFSSTableSearcher searcher(counting_allocator, OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, searcher.open_for_test(
        counting_scan_param, counting_access_ctx, sstable_));
    ASSERT_EQ(&sstable_, searcher.opened_sstable());
    const int64_t live_allocations_after_open =
        counting_allocator.live_allocation_count();
    ASSERT_EQ(0, live_allocations_after_open);
    ASSERT_EQ(OB_STATE_NOT_MATCH,
        searcher.open_for_test(counting_scan_param, counting_access_ctx, sstable_));
    ASSERT_EQ(&sstable_, searcher.opened_sstable());
    ASSERT_TRUE(searcher.initialized_for_test());
    ASSERT_EQ(live_allocations_after_open,
        counting_allocator.live_allocation_count());
    ObIvfKnnSearchResult result;
    ASSERT_EQ(OB_SUCCESS, searcher.knn_search(query_param, result));
    ASSERT_EQ(1, searcher.calls_);
    ASSERT_EQ(1, result.count());
    ASSERT_EQ(live_allocations_after_open,
        counting_allocator.live_allocation_count());
  }
  ASSERT_EQ(0, counting_allocator.live_allocation_count());
}

TEST_F(TestIVFSSTableWriter, ivf_searcher_lifecycle_owns_centroid_iterator)
{
  const int32_t dim = 64;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  TestIVFSSTableSearcher searcher(allocator_, OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, searcher.open_for_test(scan_param_, scan_context_, sstable_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher.create_centroid_for_test(query_param, nullptr));
  ASSERT_EQ(nullptr, searcher.borrowed_centroid());
  ASSERT_EQ(OB_SUCCESS, searcher.create_centroid_for_test(query_param, query));
  ObIvfCentroidIterator *const original_iterator = searcher.borrowed_centroid();
  ASSERT_NE(nullptr, original_iterator);
  ASSERT_EQ(OB_STATE_NOT_MATCH, searcher.create_centroid_for_test(query_param, query));
  ASSERT_EQ(original_iterator, searcher.borrowed_centroid());

}

TEST_F(TestIVFSSTableWriter, knn_search_rejects_meta_dim_mismatch)
{
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, 8));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(8, nullptr, 1));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[4] = {0, 0, 0, 0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, 4, 1);
  ObIvfKnnSearchResult results;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      searcher->knn_search(query_param, results));
  ASSERT_EQ(0, results.count());
}

TEST_F(TestIVFSSTableWriter, disk_bbq_meta_rejects_non_disk_bbq_index_type)
{
  const int32_t dim = 64;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_TRUE(sstable_.ivf_meta_.is_valid());
  sstable_.ivf_meta_.index_type_ = share::VIAT_IVF_FLAT;
  ASSERT_FALSE(sstable_.ivf_meta_.is_valid());
}

TEST_F(TestIVFSSTableWriter, knn_search_factory_rejects_invalid_read_info)
{
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, 8));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(8));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo invalid_read_info;
  ASSERT_FALSE(invalid_read_info.is_valid());
  float query[8] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, 8, 1);
  ObIvfCountingAllocator counting_allocator(allocator_);
  const share::schema::ObTableParam *saved_table_param = scan_param_.table_param_;
  scan_param_.table_param_ = nullptr;
  ObIvfSSTableSearcher *searcher = reinterpret_cast<ObIvfSSTableSearcher *>(1);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher));
  ASSERT_EQ(nullptr, searcher);
  scan_param_.table_param_ = saved_table_param;
  ASSERT_EQ(0, counting_allocator.alloc_count_);
  ASSERT_EQ(0, counting_allocator.free_count_);
  ASSERT_EQ(0, counting_allocator.live_allocation_count());
}

TEST_F(TestIVFSSTableWriter,
    knn_search_factory_rejects_read_info_missing_required_payload)
{
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, 8));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(8));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObSEArray<uint64_t, 4> rowkey_column_ids;
  for (int64_t i = 0; i < table_schema_.get_rowkey_column_num(); ++i) {
    const ObColumnSchemaV2 *column = table_schema_.get_column_schema_by_idx(i);
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(OB_SUCCESS, rowkey_column_ids.push_back(column->get_column_id()));
  }
  share::schema::ObTableParam rowkey_only_table_param(allocator_);
  ASSERT_EQ(OB_SUCCESS, rowkey_only_table_param.convert(
      table_schema_, rowkey_column_ids, scan_param_.pd_storage_flag_));
  ASSERT_TRUE(rowkey_only_table_param.get_read_info().is_valid());
  ASSERT_EQ(table_schema_.get_rowkey_column_num(),
      rowkey_only_table_param.get_read_info().get_request_count());

  const share::schema::ObTableParam *saved_table_param = scan_param_.table_param_;
  scan_param_.table_param_ = &rowkey_only_table_param;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(nullptr, searcher.get());
  scan_param_.table_param_ = saved_table_param;
}

TEST_F(TestIVFSSTableWriter,
    knn_search_allows_selected_centroid_without_posting_data)
{
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, 8));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(8, nullptr, 1));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[8] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, 8, 1);
  // nprobe may exceed the available centroid count. A selected centroid may
  // legitimately have no posting rows, in which case the search is empty.
  query_param.nprobe_ = 2;
  ObIvfKnnSearchResult results;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, results));
  ASSERT_EQ(0, results.count());
}

TEST_F(TestIVFSSTableWriter,
    knn_search_skips_empty_centroid_between_nonempty_postings)
{
  const int32_t dim = 64;
  const int32_t nlist = 3;
  const int64_t vector_count = 2;
  const int64_t cids[vector_count] = {0, 2};
  const int64_t vids[vector_count] = {10, 20};
  float vectors[vector_count][dim] = {{0}};
  float centroids[nlist][dim] = {{0}};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  centroids[0][0] = 10.0F;
  centroids[2][0] = 20.0F;
  vectors[0][0] = 11.0F;
  vectors[1][0] = 21.0F;

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      &vectors[0][0], cids, vids, vector_count,
      &centroids[0][0], nlist, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  query_param.nprobe_ = 1;
  ObIvfKnnSearchResult results;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, results));
  ASSERT_EQ(1, results.count());
  ASSERT_EQ(10, results.at(0).vid_);
  ASSERT_TRUE(searcher->has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, results));
  ASSERT_EQ(1, results.count());
  ASSERT_EQ(20, results.at(0).vid_);
  ASSERT_FALSE(searcher->has_remaining_centroids());
}

class CentroidHgraphThresholdGuard
{
public:
  CentroidHgraphThresholdGuard()
    : config_(TENANT_CONF(MTL_ID())), saved_(ObVecIdxExtraInfo::IVF_CENTERS_HGRAPH_THRESHOLD)
  {
    if (config_.is_valid()) { saved_ = config_->_disk_bbq_centroid_hgraph_threshold; }
  }
  ~CentroidHgraphThresholdGuard()
  {
    if (config_.is_valid()) { config_->_disk_bbq_centroid_hgraph_threshold = saved_; }
  }
  omt::ObTenantConfigGuard config_;
  int64_t saved_;
};

TEST_F(TestIVFSSTableWriter, centroid_cache_hgraph_threshold_config_and_root_eligibility)
{
  CentroidHgraphThresholdGuard guard;
  ASSERT_TRUE(guard.config_.is_valid());
  auto &threshold = guard.config_->_disk_bbq_centroid_hgraph_threshold;
  EXPECT_EQ(static_cast<int64_t>(ObVecIdxExtraInfo::IVF_CENTERS_HGRAPH_THRESHOLD),
      static_cast<int64_t>(threshold));
  for (int64_t value : {0, 1, 32, 3000, INT32_MAX}) {
    threshold = value;
    EXPECT_TRUE(threshold.check());
  }
  for (int64_t value : {int64_t(-1), int64_t(INT32_MAX) + 1}) {
    threshold = value;
    EXPECT_FALSE(threshold.check());
  }
  ObArenaAllocator allocator;
  ObIvfSSTableCentCache cache(allocator, MTL_ID());
  cache.key_.type_ = IVF_SSTABLE_CENTROID_CACHE;
  cache.count_ = 32;
  threshold = 33;
  EXPECT_FALSE(cache.can_build_hgraph());
  threshold = 32;
  EXPECT_TRUE(cache.can_build_hgraph());
  threshold = 1;
  cache.group_id_ = 1;
  EXPECT_FALSE(cache.can_build_hgraph()); // Child groups always remain ARRAY.
  cache.group_id_ = 0;
  EXPECT_TRUE(cache.can_build_hgraph());
  threshold = 0;
  EXPECT_FALSE(cache.can_build_hgraph());
}

class CentroidCacheMemoryPercentageGuard
{
public:
  CentroidCacheMemoryPercentageGuard() : config_(TENANT_CONF(MTL_ID())), saved_(80)
  {
    if (config_.is_valid()) { saved_ = config_->_disk_bbq_centroid_cache_mem_percentage; }
  }
  ~CentroidCacheMemoryPercentageGuard()
  {
    if (config_.is_valid()) { config_->_disk_bbq_centroid_cache_mem_percentage = saved_; }
    auto *service = MTL(ObPluginVectorIndexService *);
    if (service != nullptr) { (void)service->get_ivf_sstable_cache_controller().refresh_config(); }
  }
  omt::ObTenantConfigGuard config_;
  int64_t saved_;
};

TEST_F(TestIVFSSTableWriter, centroid_cache_memory_percentage_config_and_budget)
{
  CentroidCacheMemoryPercentageGuard guard;
  ASSERT_TRUE(guard.config_.is_valid());
  auto &percentage = guard.config_->_disk_bbq_centroid_cache_mem_percentage;
  EXPECT_EQ(80, static_cast<int64_t>(percentage));
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  auto &controller = service->get_ivf_sstable_cache_controller();
  int64_t vector_limit = 0;
  ASSERT_EQ(OB_SUCCESS, ObPluginVectorIndexHelper::get_vector_memory_limit_size(MTL_ID(), vector_limit));
  ASSERT_GT(vector_limit, 0);
  for (int64_t value : {0, 1, 25, 80, 100}) {
    percentage = value;
    EXPECT_TRUE(percentage.check());
    ASSERT_EQ(OB_SUCCESS, controller.refresh_config());
    EXPECT_EQ(static_cast<uint64_t>(vector_limit) * value / 100, controller.get_limit());
    EXPECT_EQ(value > 0, controller.is_cache_enabled());
    if (value == 0) {
      EXPECT_TRUE(controller.is_admission_closed());
      EXPECT_FALSE(controller.try_reserve(1));
    }
  }
  for (int64_t value : {-1, 101}) {
    percentage = value;
    EXPECT_FALSE(percentage.check());
  }
}

static void clear_hierarchical_test_cache(const ObLSID &ls_id,
    const ObITable::TableKey &table_key, const int64_t parent_count)
{
  auto *service = MTL(ObPluginVectorIndexService *);
  if (service == nullptr) { return; }
  for (int64_t i = 0; i <= parent_count; ++i) {
    const int64_t group = (i + 1) % (parent_count + 1); // Children before their shared owner.
    ObIvfCacheMgrGuard guard;
    const ObIvfCacheMgrKey key(table_key, group);
    const int ret = service->acquire_ivf_cache_mgr_guard(ls_id, key, guard);
    if (ret == OB_HASH_NOT_EXIST || ret == OB_ENTRY_NOT_EXIST) { continue; }
    ASSERT_EQ(OB_SUCCESS, ret);
    auto *manager = guard.get_ivf_cache_mgr();
    ObIvfSSTableCentCache *cache = nullptr;
    ASSERT_EQ(OB_SUCCESS, manager->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, cache));
    ASSERT_NE(nullptr, cache);
    bool erased = false;
    uint64_t hold = 0;
    ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(ls_id, key,
        manager, cache, cache->get_last_access_ts(), false, erased, hold));
    EXPECT_TRUE(erased);
  }
}

class TestIVFNonemptyQuery : public TestIVFSSTableWriter,
    public ::testing::WithParamInterface<bool> {};
INSTANTIATE_TEST_CASE_P(FlatAndGrouped, TestIVFNonemptyQuery, ::testing::Bool());

TEST_P(TestIVFNonemptyQuery, explicit_nprobe_fills_nonempty_leaves_but_routing_keeps_empty_leaves)
{
  const int32_t dim = 64;
  const int64_t k = 12;
  const int64_t parents = GetParam() ? 3 : 0;
  std::vector<float> centers(k * dim, 0.0F), vectors(2 * dim, 0.0F);
  const int64_t cids[] = {10, 11}, vids[] = {410, 411};
  float query[dim] = {};
  for (int64_t c = 0; c < k; ++c) { centers[c * dim] = c; }
  vectors[0] = 10.25F; vectors[dim] = 11.25F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids, vids, 2,
      centers.data(), k, dim, nullptr, 1, nullptr, VIDA_L2, true,
      nullptr, nullptr, 0, false, parents));
  // The shared helper leaves FLAT's count at legacy zero, which deliberately
  // disables cache construction. This fixture has known current metadata.
  sstable_.ivf_meta_.centroid_count_ = k;
  sstable_.ivf_meta_.vector_count_ = 2;
  ASSERT_EQ(k, sstable_.get_ivf_meta().centroid_count_);
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  CentroidCacheMemoryPercentageGuard cache;
  CentroidHgraphThresholdGuard graph;
  ASSERT_TRUE(cache.config_.is_valid());
  ASSERT_TRUE(graph.config_.is_valid());
  scan_context_.ls_id_ = context.ls_id_;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(context.ls_id_, ls_mgr, false));
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(410));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(411));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  // Cold storage, ARRAY and HGraph. Ten nearest leaves are empty; grouped
  // routing must pass two empty posting groups before finding the live leaves.
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    SCOPED_TRACE(GetParam());
    clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), parents);
    cache.config_->_disk_bbq_centroid_cache_mem_percentage = mode == 0 ? 0 : 80;
    graph.config_->_disk_bbq_centroid_hgraph_threshold = 0;
    ASSERT_EQ(OB_SUCCESS, service->get_ivf_sstable_cache_controller().refresh_config());
    if (mode == 2) {
      // Publishing ARRAY only enqueues optional graph construction. Build it
      // deterministically here, after releasing the warming query's ARRAY lock.
      {
        ObIvfKnnSearchParam param;
        fill_search_params(param, query, dim, 1);
        param.nprobe_ = 1;
        ObIvfKnnSearchResult result;
        ObIvfSearcherGuard warm;
        ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, warm.get()));
        ASSERT_EQ(OB_SUCCESS, warm->knn_search(param, result));
        ASSERT_EQ(1, result.count());
      }
      graph.config_->_disk_bbq_centroid_hgraph_threshold = 1;
      ObIvfCacheMgrGuard cache_guard;
      ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
          ObIvfCacheMgrKey(sstable_.get_key(), 0), cache_guard));
      ObIvfSSTableCentCache *root_cache = nullptr;
      ASSERT_EQ(OB_SUCCESS, cache_guard.get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
          IVF_SSTABLE_CENTROID_CACHE, root_cache));
      ASSERT_NE(nullptr, root_cache);
      ObVectorIndexParam graph_param;
      graph_param.type_ = VIAT_HGRAPH;
      graph_param.lib_ = VIAL_VSAG;
      graph_param.dist_algorithm_ = VIDA_L2;
      graph_param.dim_ = dim;
      graph_param.nlist_ = parents > 0 ? parents : k;
      graph_param.m_ = 16;
      graph_param.ef_construction_ = 200;
      graph_param.ef_search_ = 64;
      graph_param.extra_info_actual_size_ = 0;
      graph_param.refine_type_ = 0;
      graph_param.bq_bits_query_ = 32;
      graph_param.bq_use_fht_ = false;
      ASSERT_EQ(OB_SUCCESS, root_cache->build_sstable_hgraph(graph_param));
      ASSERT_TRUE(root_cache->has_hgraph_index());
      root_cache->update_registered_hold(root_cache->get_memory_hold());
    }
    for (const bool pre : {false, true}) {
      SCOPED_TRACE(pre);
      for (int64_t probes : {1L, 2L, 12L}) {
        SCOPED_TRACE(probes);
        ObIvfKnnSearchParam param;
        fill_search_params(param, query, dim, 2);
        param.nprobe_ = probes;
        param.pre_filter_ = pre ? &filter : nullptr;
        ObIvfKnnSearchResult result;
        ObIvfSearcherGuard searcher;
        ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
        // This is the KNN path used by a non-brute PRE segment, not a RAW-only scan.
        ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
        ASSERT_EQ(probes == 1 ? 1 : 2, result.count());
        EXPECT_EQ(410, result.at(0).vid_);
        if (probes > 1) { EXPECT_EQ(411, result.at(1).vid_); }
        if (parents == 0) {
          auto &reader = flat_group_reader_for_test(searcher->centroid_iterator());
          EXPECT_EQ(mode == 2, reader.has_hgraph());
          if (mode != 2) { EXPECT_EQ(mode == 1, reader.has_array()); }
        }
      }
    }
    {
      // The default remains geometry-only semantics. Insertion's source
      // lookup must still be able to route a new vector to the empty CID.
      ObArenaAllocator arena;
      ObIvfKnnSearchParam param;
      fill_search_params(param, query, dim, 1);
      param.nprobe_ = 1;
      ASSERT_FALSE(param.skip_empty_centroids_);
      ObIvfCentroidIteratorGuard iterator(arena);
      ASSERT_EQ(OB_SUCCESS, sstable_.create_centroid_iterator(param, query, arena, arena,
          iter_param, row_desc, context, iterator.get()));
      ASSERT_EQ(OB_SUCCESS, iterator->prepare_next_batch(1));
      ObIvfCentroidInfo item;
      ASSERT_EQ(OB_SUCCESS, iterator->get_next(item));
      EXPECT_EQ(parents + 1, item.cid_);
      EXPECT_EQ(0, item.child_count_);
      ASSERT_NE(nullptr, item.data_);
    }
  }
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), parents);
}

class TestIvfSSTableCacheAccounting : public TestIVFSSTableWriter
{
protected:
  static constexpr int64_t DIM = 64;

  void init_cache_env(const int64_t ls_id)
  {
    float vectors[2 * DIM] = {};
    vectors[0] = 1.0F;
    vectors[DIM] = 2.0F;
    const int64_t cids[] = {0, 1};
    const int64_t vids[] = {1, 2};
    ASSERT_EQ(OB_SUCCESS, prepare_write_env());
    ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors, cids, vids, 2,
        vectors, 2, DIM, nullptr, 1, nullptr, VIDA_L2, true,
        nullptr, nullptr, 0, false, 1 /* parent_count */));
    ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
    ls_id_ = ObLSID(ls_id);
    service_ = MTL(ObPluginVectorIndexService *);
    ASSERT_NE(nullptr, service_);
    ASSERT_EQ(OB_SUCCESS, service_->acquire_vector_index_mgr(ls_id_, ls_mgr_, false));
    ASSERT_NE(nullptr, ls_mgr_);
  }

  void create_cache(const int64_t group, ObIvfCacheMgrGuard &guard,
      ObIvfSSTableCentCache *&cache, const bool publish = true, const int64_t dimension = DIM)
  {
    const int64_t count = group == 0 ? 1 : 2;
    const ObIvfCacheMgrKey key(sstable_.get_key(), group);
    ObVectorIndexParam param;
    param.type_ = VIAT_DISK_BBQ;
    param.lib_ = VIAL_OB;
    param.dist_algorithm_ = VIDA_L2;
    param.dim_ = dimension;
    param.nlist_ = count;
    ASSERT_EQ(OB_SUCCESS, service_->acquire_ivf_cache_mgr_guard(
        ls_id_, key, param, dimension, IVF_TABLE_ID, guard));
    auto *mgr = guard.get_ivf_cache_mgr();
    ASSERT_NE(nullptr, mgr);
    ASSERT_EQ(OB_SUCCESS, mgr->get_or_create_cache_node<ObIvfSSTableCentCache>(
        IVF_SSTABLE_CENTROID_CACHE, cache));
    ASSERT_NE(nullptr, cache);
    ASSERT_TRUE(cache->set_writing_if_idle());
    ASSERT_EQ(OB_SUCCESS, cache->allocate_array(count, group));
    for (int64_t slot = 0; slot < count; ++slot) {
      std::vector<float> vector(dimension, 0);
      vector[0] = 17.0F + slot;
      ASSERT_EQ(OB_SUCCESS, cache->write_entry(slot, group == 0 ? 1 : slot + 2,
          vector.data(), group == 0 ? 2 : 1));
    }
    if (publish) {
      bool published = false;
      ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(
          key, mgr, cache, cache->get_memory_hold(), published));
      ASSERT_TRUE(published);
    }
  }

  void create_shared_parent(ObIvfCacheMgrGuard &guard, ObIvfSSTableCentCache *&cache,
      const int64_t leaf_count = 4, const int64_t dim = DIM)
  {
    ASSERT_NO_FATAL_FAILURE(create_cache(0, guard, cache, false, dim));
    ASSERT_EQ(OB_SUCCESS, cache->init_shared_leaves(leaf_count, 2));
    bool published = false;
    ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(ObIvfCacheMgrKey(sstable_.get_key()),
        guard.get_ivf_cache_mgr(), cache, cache->get_memory_hold(), published));
    ASSERT_TRUE(published);
  }

  void create_child(const int64_t group, const ObIvfCacheMgrGuard &parent,
      ObIvfCacheMgrGuard &guard, ObIvfSSTableCentCache *&cache,
      const int64_t count = 3, const int64_t dim = DIM)
  {
    ObVectorIndexParam param;
    param.type_ = VIAT_DISK_BBQ;
    param.lib_ = VIAL_OB;
    param.dist_algorithm_ = VIDA_L2;
    param.dim_ = dim;
    const ObIvfCacheMgrKey key(sstable_.get_key(), group);
    ASSERT_EQ(OB_SUCCESS, service_->acquire_ivf_cache_mgr_guard(ls_id_, key, param, dim, IVF_TABLE_ID, guard));
    ASSERT_EQ(OB_SUCCESS, guard.get_ivf_cache_mgr()->get_or_create_cache_node<ObIvfSSTableCentCache>(
        IVF_SSTABLE_CENTROID_CACHE, cache));
    ASSERT_TRUE(cache->set_writing_if_idle());
    if (parent.is_valid()) { ASSERT_EQ(OB_SUCCESS, cache->attach_parent_cache(parent)); }
    ASSERT_EQ(OB_SUCCESS, cache->allocate_array(count, group));
  }

  void TearDown() override
  {
    if (ls_mgr_ != nullptr) {
      for (int64_t group : {1, 2, 0}) {
        const int ret = ls_mgr_->erase_ivf_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key(), group));
        EXPECT_TRUE(ret == OB_SUCCESS || ret == OB_HASH_NOT_EXIST);
      }
    }
    TestIVFSSTableWriter::TearDown();
  }

  ObLSID ls_id_;
  ObPluginVectorIndexService *service_ = nullptr;
  ObPluginVectorIndexMgr *ls_mgr_ = nullptr;
};

TEST_F(TestIvfSSTableCacheAccounting, shared_leaves_concurrent_build_and_parent_lifetime)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419820));
  ObIvfCacheMgrGuard root, first, second;
  ObIvfSSTableCentCache *parent = nullptr, *a = nullptr, *b = nullptr;
  ASSERT_NO_FATAL_FAILURE(create_shared_parent(root, parent));
  const uint64_t parent_used = parent->get_actual_memory_used();
  ASSERT_NO_FATAL_FAILURE(create_child(1, root, first, a));
  ASSERT_NO_FATAL_FAILURE(create_child(2, root, second, b));
  const uint64_t tenant = MTL_ID();
  std::atomic<int> ready{0};
  auto fill = [&](ObIvfSSTableCentCache *cache) {
    ObTenantSwitchGuard tenant_guard;
    int ret = tenant_guard.switch_to(tenant);
    ready.fetch_add(1);
    while (ready.load() != 2) { std::this_thread::yield(); }
    for (int64_t i = 0; OB_SUCC(ret) && i < 3; ++i) {
      float vector[DIM] = {};
      vector[0] = static_cast<float>(i + 1);
      ret = cache->write_entry(i, i + 2, vector, 7 + i);
    }
    return ret;
  };
  auto f1 = std::async(std::launch::async, fill, a);
  auto f2 = std::async(std::launch::async, fill, b);
  ASSERT_EQ(OB_SUCCESS, f1.get());
  ASSERT_EQ(OB_SUCCESS, f2.get());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_NE(nullptr, parent->shared_leaves_[i]);
    EXPECT_EQ(2, parent->shared_leaves_[i]->ref_count_);
    EXPECT_EQ(a->get_centroid_vector(i), b->get_centroid_vector(i));
    EXPECT_FLOAT_EQ(i + 1, a->get_centroid_vector(i)[0]);
  }
  bool published = false;
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(ObIvfCacheMgrKey(sstable_.get_key(), 1),
      first.get_ivf_cache_mgr(), a, a->get_memory_hold(), published));
  ASSERT_TRUE(published);
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(ObIvfCacheMgrKey(sstable_.get_key(), 2),
      second.get_ivf_cache_mgr(), b, b->get_memory_hold(), published));
  ASSERT_TRUE(published);
  EXPECT_EQ(parent->get_memory_hold(), parent->get_registered_hold());
  bool erased = false; uint64_t removed = 0;
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_erase_sstable_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key()),
      root.get_ivf_cache_mgr(), parent, parent->get_last_access_ts(), false, erased, removed));
  EXPECT_FALSE(erased); // Even with no active query, child caches retain the owner.
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key())));
  EXPECT_TRUE(parent->is_memory_registered()); // Detached owner remains charged and readable.

  ObIvfCacheMgrGuard query(first);
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_erase_sstable_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key(), 1),
      first.get_ivf_cache_mgr(), a, a->get_last_access_ts(), false, erased, removed));
  EXPECT_FALSE(erased);
  query.reset();
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_erase_sstable_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key(), 1),
      first.get_ivf_cache_mgr(), a, a->get_last_access_ts(), false, erased, removed));
  ASSERT_TRUE(erased);
  first.reset();
  for (int i = 0; i < 3; ++i) { EXPECT_EQ(1, parent->shared_leaves_[i]->ref_count_); }
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key(), 2)));
  second.reset();
  for (int i = 0; i < 4; ++i) { EXPECT_EQ(nullptr, parent->shared_leaves_[i]); }
  EXPECT_EQ(parent_used, parent->get_actual_memory_used());
  EXPECT_EQ(parent->get_memory_hold(), parent->get_registered_hold());
}

TEST_F(TestIvfSSTableCacheAccounting, shared_leaf_allocation_failure_rolls_back_only_owned_references)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419821));
  ObIvfCacheMgrGuard root, first, second;
  ObIvfSSTableCentCache *parent = nullptr, *a = nullptr, *b = nullptr;
  ASSERT_NO_FATAL_FAILURE(create_shared_parent(root, parent));
  ASSERT_NO_FATAL_FAILURE(create_child(1, root, first, a, 1));
  float vector[DIM] = {17};
  ASSERT_EQ(OB_SUCCESS, a->write_entry(0, 2, vector, 1));
  const uint64_t parent_used = parent->get_actual_memory_used();
  bool published = false;
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(ObIvfCacheMgrKey(sstable_.get_key(), 1),
      first.get_ivf_cache_mgr(), a, a->get_memory_hold(), published));
  ASSERT_TRUE(published);
  auto &controller = service_->get_ivf_sstable_cache_controller();
  const uint64_t reserved_before = controller.get_reserved_hold();
  uint64_t reservation = ObIvfSSTableCentCache::estimate_shared_group_memory(3, DIM);
  ASSERT_TRUE(controller.try_reserve(reservation));
  DEFER(if (reservation > 0) { controller.release_reservation(reservation); });
  ASSERT_NO_FATAL_FAILURE(create_child(2, root, second, b));
  ASSERT_EQ(OB_SUCCESS, b->write_entry(0, 2, vector, 1, &reservation)); // Shared with A.
  ASSERT_EQ(OB_SUCCESS, b->write_entry(1, 3, vector, 1, &reservation)); // Owned only by B.
  {
    // Inject a failure into the real parent context without a production test hook.
    auto &ctx = parent->sub_mem_ctx_->get_mem_context();
    ObIAllocator *saved = ctx->freeable_alloc_;
    ObIvfCountingAllocator fail(*saved, true);
    ctx->freeable_alloc_ = &fail;
    DEFER(ctx->freeable_alloc_ = saved);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, b->write_entry(2, 4, vector, 1, &reservation));
    EXPECT_EQ(0, fail.live_allocation_count());
  }
  EXPECT_EQ(2, b->get_count());
  EXPECT_EQ(2, parent->shared_leaves_[0]->ref_count_);
  EXPECT_EQ(nullptr, parent->shared_leaves_[2]);
  bool erased = false; uint64_t removed = 0;
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_erase_sstable_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key(), 2),
      second.get_ivf_cache_mgr(), b, b->get_last_access_ts(), true, erased, removed));
  ASSERT_TRUE(erased);
  second.reset();
  controller.release_reservation(reservation);
  reservation = 0;
  EXPECT_EQ(reserved_before, controller.get_reserved_hold());
  ASSERT_NE(nullptr, parent->shared_leaves_[0]);
  EXPECT_EQ(1, parent->shared_leaves_[0]->ref_count_);
  EXPECT_EQ(nullptr, parent->shared_leaves_[1]);
  EXPECT_EQ(parent_used, parent->get_actual_memory_used());
  EXPECT_FLOAT_EQ(17, a->get_centroid_vector(0)[0]);
  EXPECT_EQ(parent->get_memory_hold(), parent->get_registered_hold());
}

TEST_F(TestIvfSSTableCacheAccounting, shared_child_publish_rejects_removed_or_replaced_parent)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419824));
  auto &controller = service_->get_ivf_sstable_cache_controller();
  const uint64_t resident_before = controller.get_resident_hold();
  const uint64_t reserved_before = controller.get_reserved_hold();
  ObIvfCacheMgrGuard old_parent_guard, child_guard, new_parent_guard;
  ObIvfSSTableCentCache *old_parent = nullptr, *child = nullptr, *new_parent = nullptr;
  ASSERT_NO_FATAL_FAILURE(create_shared_parent(old_parent_guard, old_parent));
  uint64_t reservation = ObIvfSSTableCentCache::estimate_shared_group_memory(1, DIM);
  ASSERT_TRUE(controller.try_reserve(reservation));
  DEFER(if (reservation > 0) { controller.release_reservation(reservation); });
  ASSERT_NO_FATAL_FAILURE(create_child(1, old_parent_guard, child_guard, child, 1));
  float vector[DIM] = {23.0F};
  ASSERT_EQ(OB_SUCCESS, child->write_entry(0, 2, vector, 1, &reservation));
  ASSERT_GT(reservation, 0);
  const ObIvfCacheMgrKey parent_key(sstable_.get_key());
  const ObIvfCacheMgrKey child_key(sstable_.get_key(), 1);
  const uint64_t old_parent_hold = old_parent->get_registered_hold();

  // Real invalidation removes visibility, but the child's guard retains both
  // its owner and the owner's charge. Being charged is not publication permission.
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(parent_key));
  ASSERT_TRUE(old_parent->is_memory_registered());
  EXPECT_EQ(resident_before + old_parent_hold, controller.get_resident_hold());
  bool published = true;
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(child_key,
      child_guard.get_ivf_cache_mgr(), child, child->get_memory_hold(), published));
  EXPECT_FALSE(published);
  EXPECT_TRUE(child->is_writing());
  EXPECT_FALSE(child->is_memory_registered()); // Its build reservation still owns this charge.
  EXPECT_EQ(resident_before + old_parent_hold, controller.get_resident_hold());
  EXPECT_FLOAT_EQ(23.0F, child->get_centroid_vector(0)[0]);

  ASSERT_NO_FATAL_FAILURE(create_shared_parent(new_parent_guard, new_parent));
  ASSERT_NE(old_parent_guard.get_ivf_cache_mgr(), new_parent_guard.get_ivf_cache_mgr());
  const uint64_t new_parent_hold = new_parent->get_registered_hold();
  published = true;
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(child_key,
      child_guard.get_ivf_cache_mgr(), child, child->get_memory_hold(), published));
  EXPECT_FALSE(published);
  EXPECT_EQ(resident_before + old_parent_hold + new_parent_hold, controller.get_resident_hold());

  // Private fallback transfers the child's reservation into its actual hold.
  // Both it and the detached owner stay charged until the reader releases them.
  {
    ObIvfCentroidGroupReader builder(allocator_);
    builder.cache_mgr_guard_ = child_guard;
    ASSERT_TRUE(builder.finish_array_cache_build(OB_SUCCESS, ls_id_, child_key, reservation, child));
    EXPECT_TRUE(child->is_completed());
    EXPECT_TRUE(child->is_memory_registered());
    EXPECT_FALSE(builder.shared_cache_);
    EXPECT_EQ(0, reservation);
    EXPECT_EQ(reserved_before, controller.get_reserved_hold());
    const uint64_t child_hold = child->get_registered_hold();
    EXPECT_EQ(resident_before + old_parent_hold + new_parent_hold + child_hold, controller.get_resident_hold());
    ObIvfCacheMgrGuard absent;
    EXPECT_EQ(OB_HASH_NOT_EXIST, ls_mgr_->get_ivf_cache_mgr_guard(child_key, absent));
    old_parent_guard.reset();
    child_guard.reset();
    EXPECT_FLOAT_EQ(23.0F, child->get_centroid_vector(0)[0]);
    EXPECT_EQ(resident_before + old_parent_hold + new_parent_hold + child_hold, controller.get_resident_hold());
  }
  EXPECT_EQ(resident_before + new_parent_hold, controller.get_resident_hold());
  EXPECT_EQ(new_parent_hold, new_parent->get_registered_hold());
}

// Opt-in microbenchmark: cache fill and centroid distance batches, not SQL QPS.
TEST_F(TestIvfSSTableCacheAccounting, DISABLED_shared_leaf_cache_memory_and_distance_benchmark)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419823));
  constexpr int64_t dim = 768, groups = 64, primary = 384;
  constexpr int64_t leaves = groups * primary, members = 2 * primary, batches = 512;
  std::vector<float> source(leaves * dim);
  for (int64_t i = 0; i < leaves * dim; ++i) { source[i] = ((i * 13) % 1009) / 1009.0F; }
  const uint64_t tenant = MTL_ID();
  auto &controller = service_->get_ivf_sstable_cache_controller();
  const uint64_t resident_before = controller.get_resident_hold();
  double reference_checksum = -1;
  for (int build_workers : {1, 16}) {
    for (int repetition = 0; repetition < 2; ++repetition) {
      for (int order = 0; order < 2; ++order) {
        const bool shared = (order != repetition);
        ObIvfCacheMgrGuard root, empty;
        ObIvfSSTableCentCache *parent = nullptr;
        ASSERT_NO_FATAL_FAILURE(create_child(0, empty, root, parent, groups, dim));
        for (int i = 0; i < groups; ++i) {
          ASSERT_EQ(OB_SUCCESS, parent->write_entry(i, i + 1, source.data() + i * dim, members));
        }
        if (shared) { ASSERT_EQ(OB_SUCCESS, parent->init_shared_leaves(leaves, groups + 1)); }
        bool published = false;
        ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(ObIvfCacheMgrKey(sstable_.get_key()),
            root.get_ivf_cache_mgr(), parent, parent->get_memory_hold(), published));
        ASSERT_TRUE(published);
        std::vector<ObIvfCacheMgrGuard> guards(groups);
        std::vector<ObIvfSSTableCentCache *> caches(groups, nullptr);
        std::vector<uint64_t> reservations(groups, 0);
        for (int g = 0; g < groups; ++g) {
          reservations[g] = ObIvfSSTableCentCache::estimate_shared_group_memory(members, dim);
          ASSERT_TRUE(controller.try_reserve(reservations[g]));
          ASSERT_NO_FATAL_FAILURE(create_child(g + 1, shared ? root : empty, guards[g], caches[g], members, dim));
        }
        auto fill = [&](int worker) {
          ObTenantSwitchGuard tenant_guard;
          int ret = tenant_guard.switch_to(tenant);
          for (int g = worker; OB_SUCC(ret) && g < groups; g += build_workers) {
            for (int i = 0; OB_SUCC(ret) && i < members; ++i) {
              const int64_t idx = (g * primary + i) % leaves;
              ret = caches[g]->write_entry(i, groups + 1 + idx,
                  source.data() + idx * dim, 384, &reservations[g]);
            }
          }
          return ret;
        };
        const auto fill_begin = std::chrono::steady_clock::now();
        std::vector<std::future<int>> fills;
        for (int w = 0; w < build_workers; ++w) { fills.push_back(std::async(std::launch::async, fill, w)); }
        for (auto &f : fills) { ASSERT_EQ(OB_SUCCESS, f.get()); }
        const double fill_ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - fill_begin).count();
        for (int g = 0; g < groups; ++g) {
          ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(ObIvfCacheMgrKey(sstable_.get_key(), g + 1),
              guards[g].get_ivf_cache_mgr(), caches[g], caches[g]->get_memory_hold(), published));
          ASSERT_TRUE(published);
          controller.release_reservation(reservations[g]);
          reservations[g] = 0;
        }
        uint64_t used = parent->get_actual_memory_used(), hold = parent->get_memory_hold();
        for (auto *cache : caches) { used += cache->get_actual_memory_used(); hold += cache->get_memory_hold(); }
        for (int workers : {1, 16}) {
          auto search = [&](int worker, int64_t count) {
            ObTenantSwitchGuard tenant_guard;
            int ret = tenant_guard.switch_to(tenant);
            ObArenaAllocator arena;
            std::vector<float> query(dim, 0.125F);
            ObIvfQueryVectorContext context;
            if (OB_SUCC(ret)) { ret = context.init(arena, query.data(), dim, VIDA_L2); }
            std::vector<uint8_t> seen((leaves + 7) / 8, 0);
            double checksum = 0;
            for (int64_t q = worker; OB_SUCC(ret) && q < count; q += workers) {
              MEMSET(seen.data(), 0, seen.size());
              for (int offset : {0, 1, 9, 31}) {
                auto *cache = caches[(q * 7 + offset) % groups];
                for (int slot = 0; OB_SUCC(ret) && slot < members; ++slot) {
                  const int64_t idx = cache->get_centroid_id(slot) - groups - 1;
                  const uint8_t mask = 1U << (idx % 8);
                  if ((seen[idx / 8] & mask) == 0) {
                    seen[idx / 8] |= mask;
                    float distance = 0;
                    ret = context.computer().compute({reinterpret_cast<uint8_t *>(
                        const_cast<float *>(cache->get_centroid_vector(slot))), dim * sizeof(float)}, distance);
                    checksum += distance;
                  }
                }
              }
            }
            return std::make_pair(ret, checksum);
          };
          for (int w = 0; w < workers; ++w) { ASSERT_EQ(OB_SUCCESS, search(w, 32).first); }
          const auto begin = std::chrono::steady_clock::now();
          std::vector<std::future<std::pair<int, double>>> searches;
          for (int w = 0; w < workers; ++w) {
            searches.push_back(std::async(std::launch::async, search, w, batches));
          }
          double checksum = 0;
          for (auto &f : searches) { auto r = f.get(); ASSERT_EQ(OB_SUCCESS, r.first); checksum += r.second; }
          const double elapsed_ms = std::chrono::duration<double, std::milli>(
              std::chrono::steady_clock::now() - begin).count();
          if (reference_checksum < 0) { reference_checksum = checksum; }
          EXPECT_NEAR(reference_checksum, checksum, 0.01);
          std::printf("SHARED_LEAF_BENCH {\"shared\":%s,\"repeat\":%d,\"build_workers\":%d,"
              "\"search_workers\":%d,\"fill_ms\":%.6f,\"cache_used\":%lu,\"cache_hold\":%lu,"
              "\"batches\":%ld,\"distance_ms\":%.6f,\"checksum\":%.6f}\n",
              shared ? "true" : "false", repetition, build_workers, workers, fill_ms, used, hold,
              batches, elapsed_ms, checksum);
        }
        for (int g = 0; g < groups; ++g) {
          ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key(), g + 1)));
          guards[g].reset();
        }
        root.reset();
        ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(ObIvfCacheMgrKey(sstable_.get_key())));
        EXPECT_EQ(resident_before, controller.get_resident_hold());
      }
    }
  }
}

TEST_F(TestIvfSSTableCacheAccounting, map_removal_preserves_charge_until_last_guard)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419810));
  auto &controller = service_->get_ivf_sstable_cache_controller();
  const uint64_t resident_before = controller.get_resident_hold();
  const uint64_t vector_used_before = ATOMIC_LOAD(service_->get_all_vsag_use_mem());
  ObIvfCacheMgrGuard guards[2];
  ObIvfSSTableCentCache *caches[2] = {};
  uint64_t remaining = resident_before;
  for (int64_t group : {0, 1}) {
    ASSERT_NO_FATAL_FAILURE(create_cache(group, guards[group], caches[group]));
    remaining += caches[group]->get_registered_hold();
  }
  ASSERT_EQ(remaining, controller.get_resident_hold());
  const ObIvfCacheMgrKey root_key(sstable_.get_key(), 0);
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->enqueue_ivf_sstable_hgraph_build(root_key));
  ASSERT_TRUE(ls_mgr_->has_ivf_sstable_hgraph_pending(root_key));
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->enqueue_ivf_sstable_hgraph_build(ObIvfCacheMgrKey(sstable_.get_key(), 1)));
  EXPECT_FALSE(ls_mgr_->has_ivf_sstable_hgraph_pending(ObIvfCacheMgrKey(sstable_.get_key(), 1)));
  for (int64_t group : {0, 1}) {
    const ObIvfCacheMgrKey key(sstable_.get_key(), group);
    auto *cache = caches[group];
    const uint64_t hold = cache->get_registered_hold();
    ASSERT_GT(hold, 0);
    {
      // Removal must not acquire the representation write lock: an active
      // query can still hold the ARRAY read lock and borrow these vectors.
      common::RWLock::RLockGuard read_guard(cache->get_lock());
      const float *vector = cache->get_centroids();
      ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(key));
      EXPECT_EQ(remaining, controller.get_resident_hold());
      EXPECT_TRUE(cache->is_memory_registered());
      EXPECT_EQ(hold, cache->get_registered_hold());
      EXPECT_FLOAT_EQ(17.0F, vector[0]);
      EXPECT_FALSE(ls_mgr_->has_ivf_sstable_hgraph_pending(key));
      EXPECT_EQ(OB_HASH_NOT_EXIST, ls_mgr_->erase_ivf_cache_mgr(key));
      EXPECT_EQ(remaining, controller.get_resident_hold());
    }
    guards[group].reset();
    remaining -= hold;
    EXPECT_EQ(remaining, controller.get_resident_hold());
  }
  EXPECT_EQ(resident_before, controller.get_resident_hold());
  EXPECT_EQ(vector_used_before, ATOMIC_LOAD(service_->get_all_vsag_use_mem()));
  // A delayed enqueue after publication cannot leave an orphan for a removed key.
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->enqueue_ivf_sstable_hgraph_build(root_key));
  EXPECT_FALSE(ls_mgr_->has_ivf_sstable_hgraph_pending(root_key));
}

TEST_F(TestIvfSSTableCacheAccounting, erasing_unpublished_cache_preserves_other_charges_and_reservation)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419811));
  auto &controller = service_->get_ivf_sstable_cache_controller();
  ObIvfCacheMgrGuard root_guard, child_guard;
  ObIvfSSTableCentCache *root_cache = nullptr, *child_cache = nullptr;
  ASSERT_NO_FATAL_FAILURE(create_cache(0, root_guard, root_cache));
  const uint64_t resident = controller.get_resident_hold();
  const uint64_t reserved_before = controller.get_reserved_hold();
  const uint64_t reservation = 4096;
  ASSERT_TRUE(controller.try_reserve(reservation));
  DEFER(controller.release_reservation(reservation));
  ASSERT_NO_FATAL_FAILURE(create_cache(1, child_guard, child_cache, false));
  EXPECT_FALSE(child_cache->is_memory_registered());
  const ObIvfCacheMgrKey key(sstable_.get_key(), 1);
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(key));
  EXPECT_EQ(resident, controller.get_resident_hold());
  EXPECT_EQ(reserved_before + reservation, controller.get_reserved_hold());
  // An admitted builder may finish its private buffer after removal, but it
  // must not publish or change another cache's resident charge.
  bool published = true;
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(key,
      child_guard.get_ivf_cache_mgr(), child_cache, child_cache->get_memory_hold(), published));
  EXPECT_FALSE(published);
  child_cache->update_registered_hold(child_cache->get_memory_hold());
  EXPECT_EQ(resident, controller.get_resident_hold());
  child_guard.reset();
  EXPECT_EQ(resident, controller.get_resident_hold());
}

TEST_F(TestIvfSSTableCacheAccounting, failed_build_keeps_charge_until_observer_guard_releases)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419825));
  auto &controller = service_->get_ivf_sstable_cache_controller();
  const uint64_t resident_before = controller.get_resident_hold();
  const uint64_t reserved_before = controller.get_reserved_hold();
  const uint64_t used_before = ATOMIC_LOAD(service_->get_all_vsag_use_mem());
  uint64_t reservation = 4096;
  ASSERT_TRUE(controller.try_reserve(reservation));
  DEFER(if (reservation > 0) { controller.release_reservation(reservation); });
  ObIvfCacheMgrGuard empty_parent, build_guard;
  ObIvfSSTableCentCache *partial = nullptr;
  ASSERT_NO_FATAL_FAILURE(create_child(1, empty_parent, build_guard, partial, 2));
  float vector[DIM] = {29.0F};
  ASSERT_EQ(OB_SUCCESS, partial->write_entry(0, 2, vector, 1));
  ASSERT_LT(partial->get_count(), partial->get_capacity());
  const uint64_t partial_hold = partial->get_memory_hold();
  ObIvfCacheMgrGuard observer_guard(build_guard);
  ObIvfCentroidGroupReader builder(allocator_);
  builder.cache_mgr_guard_ = build_guard;
  build_guard.reset();
  const ObIvfCacheMgrKey key(sstable_.get_key(), 1);
  EXPECT_FALSE(builder.finish_array_cache_build(OB_ALLOCATE_MEMORY_FAILED,
      ls_id_, key, reservation, partial));
  EXPECT_EQ(nullptr, builder.cache_mgr_guard_.get_ivf_cache_mgr());
  EXPECT_EQ(0, reservation);
  EXPECT_EQ(reserved_before, controller.get_reserved_hold());
  EXPECT_EQ(resident_before + partial_hold, controller.get_resident_hold());
  EXPECT_TRUE(partial->is_memory_registered());
  EXPECT_FLOAT_EQ(29.0F, partial->get_centroid_vector(0)[0]);
  ObIvfCacheMgrGuard absent;
  EXPECT_EQ(OB_HASH_NOT_EXIST, ls_mgr_->get_ivf_cache_mgr_guard(key, absent));
  observer_guard.reset();
  EXPECT_EQ(resident_before, controller.get_resident_hold());
  EXPECT_EQ(used_before, ATOMIC_LOAD(service_->get_all_vsag_use_mem()));
}

TEST_F(TestIvfSSTableCacheAccounting, detached_graph_updates_only_its_own_charge)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419812));
  auto &controller = service_->get_ivf_sstable_cache_controller();
  const uint64_t resident_before = controller.get_resident_hold();
  ObIvfCacheMgrGuard old_guard, new_guard;
  ObIvfSSTableCentCache *old_cache = nullptr, *new_cache = nullptr;
  ASSERT_NO_FATAL_FAILURE(create_cache(0, old_guard, old_cache));
  const ObIvfCacheMgrKey key(sstable_.get_key(), 0);
  auto *old_mgr = old_guard.get_ivf_cache_mgr();
  const uint64_t old_hold = old_cache->get_registered_hold();
  for (uint64_t hold : {old_hold + 4096, old_hold + 8192, old_hold}) {
    old_cache->update_registered_hold(hold);
    old_cache->update_registered_hold(hold);
    EXPECT_EQ(resident_before + hold, controller.get_resident_hold());
  }
  ASSERT_TRUE(old_cache->try_start_hgraph_build());
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->enqueue_ivf_sstable_hgraph_build(key));
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(key));
  EXPECT_EQ(resident_before + old_hold, controller.get_resident_hold());
  old_cache->update_registered_hold(old_hold + 8192);
  EXPECT_EQ(resident_before + old_hold + 8192, controller.get_resident_hold());
  EXPECT_EQ(old_hold + 8192, old_cache->get_registered_hold());
  ASSERT_NO_FATAL_FAILURE(create_cache(0, new_guard, new_cache, false /* publish */));
  ASSERT_NE(old_mgr, new_guard.get_ivf_cache_mgr());
  // Enqueue only checks map membership. A replacement still being built may
  // retain its marker; execution checks readiness and a later enqueue is idempotent.
  EXPECT_FALSE(new_cache->is_completed());
  EXPECT_FALSE(new_cache->is_memory_registered());
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->enqueue_ivf_sstable_hgraph_build(key));
  EXPECT_TRUE(ls_mgr_->has_ivf_sstable_hgraph_pending(key));
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->enqueue_ivf_sstable_hgraph_build(key));
  old_cache->update_registered_hold(old_hold + 16384);
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_erase_ivf_sstable_hgraph_pending(key, old_mgr, old_cache));
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_erase_ivf_sstable_hgraph_pending(key, nullptr, nullptr));
  EXPECT_TRUE(ls_mgr_->has_ivf_sstable_hgraph_pending(key));
  bool published = false;
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_publish_sstable_cache(key, new_guard.get_ivf_cache_mgr(),
      new_cache, new_cache->get_memory_hold(), published));
  ASSERT_TRUE(published);
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->enqueue_ivf_sstable_hgraph_build(key));
  const uint64_t new_hold = new_cache->get_registered_hold();
  EXPECT_EQ(resident_before + new_hold + old_hold + 16384, controller.get_resident_hold());
  EXPECT_EQ(new_hold, new_cache->get_registered_hold());
  EXPECT_TRUE(ls_mgr_->has_ivf_sstable_hgraph_pending(key));
  old_cache->finish_hgraph_build();
  old_guard.reset();
  EXPECT_EQ(resident_before + new_hold, controller.get_resident_hold());
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->try_erase_ivf_sstable_hgraph_pending(
      key, new_guard.get_ivf_cache_mgr(), new_cache));
  EXPECT_FALSE(ls_mgr_->has_ivf_sstable_hgraph_pending(key));
  ASSERT_EQ(OB_SUCCESS, ls_mgr_->erase_ivf_cache_mgr(key));
  EXPECT_EQ(resident_before + new_hold, controller.get_resident_hold());
  new_guard.reset();
  EXPECT_EQ(resident_before, controller.get_resident_hold());
}

TEST_F(TestIvfSSTableCacheAccounting, erase_does_not_release_inflight_graph_accounting)
{
  CentroidCacheMemoryPercentageGuard percentage;
  ASSERT_TRUE(percentage.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(init_cache_env(419813));
  auto &controller = service_->get_ivf_sstable_cache_controller();
  const uint64_t resident_before = controller.get_resident_hold();
  ObIvfCacheMgrGuard guard;
  ObIvfSSTableCentCache *cache = nullptr;
  ASSERT_NO_FATAL_FAILURE(create_cache(0, guard, cache));
  const ObIvfCacheMgrKey key(sstable_.get_key());
  const uint64_t old_hold = cache->get_registered_hold();
  const uint64_t new_hold = old_hold + 4096;
  const uint64_t tenant_id = MTL_ID();
  std::future<int> update, erase;
  std::promise<void> update_started, erase_started;
  auto update_ready = update_started.get_future();
  auto erase_ready = erase_started.get_future();
  bool update_waiting = false;
  bool erase_finished = false;
  bool still_registered = false;
  {
    // Pause before the accounting commit. Neither actual hold nor reservation
    // may change outside the controller lock; map removal remains independent.
    common::ObSpinLockGuard pause(controller.state_lock_);
    update = std::async(std::launch::async, [&]() {
      ObTenantSwitchGuard tenant_guard;
      int ret = tenant_guard.switch_to(tenant_id);
      update_started.set_value();
      if (OB_SUCC(ret)) { cache->update_registered_hold(new_hold); }
      return ret;
    });
    if (update_ready.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
      update_waiting = update.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout;
    }
    EXPECT_EQ(resident_before + old_hold, controller.get_resident_hold());
    erase = std::async(std::launch::async, [&]() {
      ObTenantSwitchGuard tenant_guard;
      int ret = tenant_guard.switch_to(tenant_id);
      erase_started.set_value();
      if (OB_SUCC(ret)) { ret = ls_mgr_->erase_ivf_cache_mgr(key); }
      return ret;
    });
    if (erase_ready.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
      erase_finished = erase.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    }
    still_registered = cache->is_memory_registered() && cache->get_registered_hold() == old_hold;
  }
  // Always release the pause and join both workers before asserting outcomes.
  const int update_ret = update.get();
  const int erase_ret = erase.get();
  EXPECT_TRUE(update_waiting);
  EXPECT_TRUE(erase_finished);
  EXPECT_TRUE(still_registered);
  EXPECT_EQ(OB_SUCCESS, update_ret);
  EXPECT_EQ(OB_SUCCESS, erase_ret);
  EXPECT_EQ(resident_before + new_hold, controller.get_resident_hold());
  EXPECT_EQ(new_hold, cache->get_registered_hold());
  EXPECT_TRUE(cache->is_memory_registered());
  EXPECT_FLOAT_EQ(17.0F, cache->get_centroids()[0]);
  guard.reset();
  EXPECT_EQ(resident_before, controller.get_resident_hold());
}

TEST_F(TestIVFSSTableWriter, flat_centroid_retention_follows_explicit_iteration_mode)
{
  CentroidCacheMemoryPercentageGuard cache_percentage;
  CentroidHgraphThresholdGuard graph_threshold;
  ASSERT_TRUE(cache_percentage.config_.is_valid());
  ASSERT_TRUE(graph_threshold.config_.is_valid());
  graph_threshold.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  const int32_t dim = 64;
  const int64_t count = 4;
  float centroids[count][dim] = {}, vectors[count][dim] = {}, query[dim] = {};
  int64_t cids[count] = {}, vids[count] = {};
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i; vids[i] = 10 + i;
    centroids[i][0] = i * 10;
    vectors[i][0] = centroids[i][0] + 1;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(&vectors[0][0], cids, vids, count,
      &centroids[0][0], count, dim, nullptr, 1, nullptr, share::VIDA_L2, true));
  // The hand-written SSTable helper does not populate the cache sizing metadata.
  sstable_.ivf_meta_.centroid_count_ = count;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  const ObLSID cache_ls_id(IVF_LS_ID);
  scan_context_.ls_id_ = cache_ls_id;
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(cache_ls_id, ls_mgr, false));
  for (int64_t cache_percent : {0, 80}) {
    cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = cache_percent;
    for (bool iterative : {false, true}) {
      SCOPED_TRACE(cache_percent);
      SCOPED_TRACE(iterative);
      ObIvfSearcherGuard searcher;
      ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
      ObIvfKnnSearchParam param;
      fill_search_params(param, query, dim, 1);
      param.nprobe_ = 1;
      param.iterative_centroids_ = iterative;
      ObIvfSSTableReadStats stats;
      param.read_stats_ = &stats;
      ObIvfKnnSearchResult result;
      ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
      ASSERT_EQ(1, result.count());
      EXPECT_EQ(vids[0], result.at(0).vid_);
      auto *centers = static_cast<ObDiskBBQSearcher *>(searcher.get())->centroid_iterator();
      ASSERT_NE(nullptr, centers);
      if (cache_percent > 0) {
        ASSERT_NE(nullptr, flat_group_reader_for_test(centers).array_cache_guard_.cache_);
        EXPECT_FALSE(centers->owns_result_vectors_);
      } else {
        EXPECT_EQ(nullptr, flat_group_reader_for_test(centers).array_cache_guard_.cache_);
        EXPECT_EQ(!iterative, centers->owns_result_vectors_);
      }
      EXPECT_EQ(iterative ? count : 1, centers->result_count_);
      EXPECT_EQ(iterative, searcher->has_remaining_centroids());
      if (iterative) {
        for (int64_t i = 1; i < count; ++i) {
          ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
          ASSERT_EQ(1, result.count());
          EXPECT_EQ(vids[i], result.at(0).vid_);
        }
      }
      EXPECT_FALSE(searcher->has_remaining_centroids());
      stats.reset();
      ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
      EXPECT_EQ(0, result.count());
      EXPECT_EQ(0, stats.low_precision_vector_count_);
    }
  }
  clear_hierarchical_test_cache(cache_ls_id, sstable_.get_key(), 0);
}

TEST_F(TestIVFSSTableWriter, grouped_cache_access_is_recorded_once_on_iterator_release)
{
  CentroidCacheMemoryPercentageGuard percentage;
  CentroidHgraphThresholdGuard graph_threshold;
  ASSERT_TRUE(percentage.config_.is_valid());
  ASSERT_TRUE(graph_threshold.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  graph_threshold.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  const int64_t dim = 64;
  float vector[dim] = {}, query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t p = 1; p <= 2; ++p) {
    vector[0] = p;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, p, vector, dim, 2));
  }
  for (int64_t p = 1; p <= 2; ++p) {
    for (int64_t cid = p + 2; cid <= p + 3; ++cid) {
      vector[0] = cid;
      ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, cid, vector, dim, 1));
    }
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = 2;
  sstable_.ivf_meta_.centroid_count_ = 3;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(context.ls_id_, ls_mgr, false));
  ObIvfCacheMgrGuard guards[3];
  ObIvfSSTableCentCache *caches[3] = {};
  int64_t loaded_at[3] = {}, accessed_at[3] = {};
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  // First build, one-shot hit, then iterative hits across three result batches.
  for (int64_t round = 0; round < 3; ++round) {
    ObArenaAllocator arena;
    ObIvfHierarchicalCentroidIterator iterator(arena);
    param.iterative_centroids_ = round == 2;
    param.nprobe_ = param.iterative_centroids_ ? 1 : 3;
    const int64_t parent_refs = round == 0 ? 0 : guards[0].get_ivf_cache_mgr()->get_ref_cnt();
    ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
    for (int64_t i = 0; i < 3; ++i) {
      if (i == 0 || param.iterative_centroids_) {
        ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(param.nprobe_));
      }
      ObIvfCentroidInfo center;
      ASSERT_EQ(OB_SUCCESS, iterator.get_next(center));
      EXPECT_EQ(3 + i, center.cid_);
    }
    EXPECT_FALSE(iterator.has_remaining_centroids());
    EXPECT_EQ(OB_ITER_END, iterator.prepare_next_batch(1));
    for (int64_t group = 0; group < 3; ++group) {
      if (round == 0) {
        ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
            ObIvfCacheMgrKey(sstable_.get_key(), group), guards[group]));
        ASSERT_EQ(OB_SUCCESS, guards[group].get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
            IVF_SSTABLE_CENTROID_CACHE, caches[group]));
        ASSERT_NE(nullptr, caches[group]);
        loaded_at[group] = caches[group]->get_first_load_ts();
        EXPECT_GT(loaded_at[group], 0);
      }
      EXPECT_EQ(round == 0 ? 0 : round - 1, caches[group]->get_hit_count());
      EXPECT_EQ(accessed_at[group], caches[group]->get_last_access_ts());
      EXPECT_EQ(loaded_at[group], caches[group]->get_first_load_ts());
    }
    if (round > 0) {
      // Even one-shot selection retains the parent's own query guard.
      EXPECT_EQ(parent_refs + 1, guards[0].get_ivf_cache_mgr()->get_ref_cnt());
    }
    iterator.reset();
    for (int64_t group = 0; group < 3; ++group) {
      EXPECT_EQ(round, caches[group]->get_hit_count());
      EXPECT_GE(caches[group]->get_last_access_ts(), loaded_at[group]);
      EXPECT_GE(caches[0]->get_last_access_ts(), caches[group]->get_last_access_ts());
      accessed_at[group] = caches[group]->get_last_access_ts();
    }
    iterator.reset();
    for (int64_t group = 0; group < 3; ++group) {
      EXPECT_EQ(round, caches[group]->get_hit_count());
      EXPECT_EQ(accessed_at[group], caches[group]->get_last_access_ts());
    }
  }
  {
    ObArenaAllocator arena;
    ObIvfCountingAllocator allocator(arena);
    ObIvfHierarchicalCentroidIterator iterator(allocator);
    ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
    allocator.set_fail_on_alloc(allocator.alloc_count_ + 1);
    // Fail before opening the first child; the already-hit parent is still counted once.
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, iterator.prepare_next_batch(1));
    EXPECT_EQ(3, caches[0]->get_hit_count());
    EXPECT_EQ(2, caches[1]->get_hit_count());
    EXPECT_EQ(2, caches[2]->get_hit_count());
    iterator.reset();
    EXPECT_EQ(3, caches[0]->get_hit_count());
    EXPECT_EQ(0, allocator.live_allocation_count());
  }
  for (auto &guard : guards) { guard.reset(); }
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), 2);
}

TEST_F(TestIVFSSTableWriter, grouped_centroids_resume_deduplicate_and_pin_child_cache)
{
  CentroidCacheMemoryPercentageGuard cache_percentage;
  ASSERT_TRUE(cache_percentage.config_.is_valid());
  // Use a vector size distinct from iterator objects counted by the allocator.
  const int32_t dim = 768;
  ASSERT_NE(dim * sizeof(float), sizeof(ObIvfCentroidIterator));
  float parents[3][dim] = {}, leaves[4][dim] = {}, query[dim] = {};
  for (int64_t p = 0; p < 3; ++p) { parents[p][0] = p + 1; }
  leaves[0][0] = 10; leaves[1][0] = 20; leaves[2][0] = 0.1F; leaves[3][0] = 0.2F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t p = 0; p < 3; ++p) {
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, p + 1, parents[p], dim, p == 2 ? 1 : 2));
  }
  ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 1, 4, leaves[0], dim, 11));
  ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 1, 5, leaves[1], dim, 12));
  ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 2, 4, leaves[0], dim, 11));
  ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 2, 6, leaves[2], dim, 13));
  ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 3, 7, leaves[3], dim, 14));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = 3;
  sstable_.ivf_meta_.centroid_count_ = 4;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(context.ls_id_, ls_mgr, false));
  auto &controller = service->get_ivf_sstable_cache_controller();
  // First query exercises storage with cache admission disabled, then cold and hot cache.
  for (int64_t mode = 0; mode < 3; ++mode) {
    cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = mode == 0 ? 0 : 80;
    for (bool iterative : {false, true}) {
      ObArenaAllocator query_arena;
      ObIvfCountingAllocator query_memory(query_arena);
      query_memory.watch_alloc_size((mode == 0 ? 2 : 1) * dim * sizeof(float));
      ObIvfQueryVectorContext shared_query;
      ASSERT_EQ(OB_SUCCESS, shared_query.init(query_memory, query, dim, share::VIDA_L2));
      ObIvfHierarchicalCentroidIterator iterator(query_memory);
      ObIvfKnnSearchParam param;
      fill_search_params(param, query, dim, 1);
      // Keep later parent groups for iterative expansion in this fixture.
      param.refine_k_ = 2.0F;
      param.nprobe_ = iterative ? 1 : 3;
      param.iterative_centroids_ = iterative;
      ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context,
          iterative ? &shared_query : nullptr));
      EXPECT_EQ(!iterative, iterator.owns_query_vector_ctx_);
      if (iterative) { EXPECT_EQ(&shared_query, iterator.query_vector_ctx_); }
      // L=1 reaches its parent budget in groups 1 and 2. Group 3 is opened
      // after those pending leaves are consumed. L=3 opens every group.
      const int64_t expected_iterative[] = {6, 4, 5, 7};
      const int64_t expected_knn[] = {6, 7, 4};
      const int64_t count = iterative ? 4 : 3;
      ObIvfCentroidInfo retained_result;
      if (!iterative) { ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(3)); }
      for (int64_t i = 0; i < count; ++i) {
        if (iterative) { ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(1)); }
        ObIvfCentroidInfo result;
        ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
        if (result.cid_ == 4) { retained_result = result; }
        EXPECT_EQ(iterative ? expected_iterative[i] : expected_knn[i], result.cid_);
        EXPECT_EQ(11 + result.cid_ - 4, result.child_count_);
        EXPECT_EQ(0, MEMCMP(leaves[result.cid_ - 4], result.data_, dim * sizeof(float)));
      }
      EXPECT_FALSE(iterator.has_remaining_centroids());
      EXPECT_EQ(OB_ITER_END, iterator.prepare_next_batch(1));
      if (mode == 0) {
        EXPECT_EQ(0, query_memory.watched_alloc_count_);
        // Cold iterative queries materialize one selected center per batch,
        // without retaining any two-center child-group buffers.
        for (int64_t group = 0; group <= 3; ++group) {
          ObIvfCacheMgrGuard absent;
          EXPECT_EQ(OB_HASH_NOT_EXIST, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
              ObIvfCacheMgrKey(sstable_.get_key(), group), absent));
        }
      } else {
        EXPECT_EQ(0, query_memory.watched_alloc_count_); // Cache hits do not copy centroid vectors.
        ObIvfCacheMgrGuard guard;
        const ObIvfCacheMgrKey key(sstable_.get_key(), 1);
        ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_, key, guard));
        auto *manager = guard.get_ivf_cache_mgr();
        ObIvfSSTableCentCache *cache = nullptr;
        ASSERT_EQ(OB_SUCCESS, manager->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, cache));
        ASSERT_NE(nullptr, cache);
        EXPECT_EQ(2, cache->get_capacity());
        EXPECT_EQ(4, cache->get_centroid_id(0));
        EXPECT_EQ(11, cache->get_child_count(0));
        EXPECT_FALSE(cache->can_build_hgraph());
        bool erased = false;
        uint64_t hold = 0;
        ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(context.ls_id_, key,
            manager, cache, cache->get_last_access_ts(), false, erased, hold));
        EXPECT_FALSE(erased);
        EXPECT_EQ(cache->get_centroid_vector(0), retained_result.data_);
        iterator.reset();
        ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(context.ls_id_, key,
            manager, cache, cache->get_last_access_ts(), false, erased, hold));
        EXPECT_TRUE(erased);
      }
      iterator.reset();
      // Releasing all parent/child readers must not reset the caller's query.
      float self_distance = -1;
      EXPECT_EQ(OB_SUCCESS, shared_query.computer().compute(
          {reinterpret_cast<uint8_t *>(query), dim * sizeof(float)}, self_distance));
      EXPECT_FLOAT_EQ(0.0F, self_distance);
    }
  }
  // Disabling caches evicts unpinned groups in the background, but preserves
  // a child group until its last query guard is released.
  ObIvfCacheMgrGuard pinned;
  const ObIvfCacheMgrKey pinned_key(sstable_.get_key(), 2);
  ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_, pinned_key, pinned));
  ObIvfSSTableCentCache *pinned_cache = nullptr;
  ASSERT_EQ(OB_SUCCESS, pinned.get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
      IVF_SSTABLE_CENTROID_CACHE, pinned_cache));
  ASSERT_NE(nullptr, pinned_cache);
  auto *pinned_parent = pinned_cache->parent_cache_;
  ASSERT_NE(nullptr, pinned_parent);
  cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 0;
  ASSERT_EQ(OB_SUCCESS, service->run_ivf_sstable_cache_maintenance());
  const uint64_t pinned_hold = pinned_cache->get_registered_hold() + pinned_parent->get_registered_hold();
  EXPECT_EQ(pinned_hold, controller.get_resident_hold());
  EXPECT_TRUE(controller.has_eviction_pending());
  {
    ObArenaAllocator query_arena;
    ObIvfHierarchicalCentroidIterator iterator(query_arena);
    ObIvfKnnSearchParam param;
    fill_search_params(param, query, dim, 1);
    param.nprobe_ = 3;
    ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
    ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(3));
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
    EXPECT_EQ(6, result.cid_);
    EXPECT_NE(pinned_cache->get_centroid_vector(1), result.data_);
    EXPECT_EQ(0, MEMCMP(leaves[2], result.data_, dim * sizeof(float)));
    EXPECT_EQ(pinned_hold, controller.get_resident_hold());
    EXPECT_EQ(0, controller.get_reserved_hold());
  }
  pinned.reset();
  ASSERT_EQ(OB_SUCCESS, service->run_ivf_sstable_cache_maintenance());
  EXPECT_EQ(0, controller.get_resident_hold());
  EXPECT_FALSE(controller.has_eviction_pending());
  EXPECT_TRUE(controller.is_admission_closed());
  for (int64_t group = 0; group <= 3; ++group) {
    ObIvfCacheMgrGuard absent;
    EXPECT_EQ(OB_HASH_NOT_EXIST, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
        ObIvfCacheMgrKey(sstable_.get_key(), group), absent));
  }
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(),
      sstable_.get_ivf_meta().parent_centroid_count_);
}

TEST_F(TestIVFSSTableWriter, grouped_mixed_cache_candidates_resume_and_clean_up)
{
  CentroidCacheMemoryPercentageGuard cache_percentage;
  ASSERT_TRUE(cache_percentage.config_.is_valid());
  cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  const int64_t parents = 6, dim = 768;
  ASSERT_NE(dim * sizeof(float), sizeof(ObIvfCentroidIterator));
  float leaves[parents][dim] = {}, query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t p = 1; p <= parents; ++p) {
    float center[dim] = {}; center[0] = p;
    leaves[p - 1][0] = (parents + 1 - p) * 10;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, p, center, dim, p % 2 == 0 ? 2 : 1));
  }
  for (int64_t p = 1; p <= parents; ++p) {
    if (p % 2 == 0) {
      ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, parents + p - 1, leaves[p - 2], dim, 1));
    }
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, parents + p, leaves[p - 1], dim, 1));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = parents;
  sstable_.ivf_meta_.centroid_count_ = parents;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(context.ls_id_, ls_mgr, false));
  auto &controller = service->get_ivf_sstable_cache_controller();
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  // Keep later parent groups for iterative expansion in this fixture.
  param.refine_k_ = 2.0F;
  param.nprobe_ = 1;
  param.iterative_centroids_ = true;

  // The initial parent budget opens groups 1..3. Their pending leaves are
  // consumed before groups 4..6 are expanded on demand.
  const int64_t expected_cids[] = {9, 8, 7, 10, 11, 12};
  for (int64_t hot_parity : {-1, 0, 1}) {
    // Keep the parent cache; remove all children (-1) or alternating groups.
    // New cache admission is suspended while existing entries remain readable.
    {
      ObArenaAllocator arena;
      ObIvfHierarchicalCentroidIterator warm(arena);
      ASSERT_EQ(OB_SUCCESS, warm.open(param, query, sstable_, iter_param, row_desc, context));
      ASSERT_EQ(OB_SUCCESS, warm.prepare_next_batch(parents));
    }
    for (int64_t p = 1; p <= parents; ++p) {
      if (p % 2 == hot_parity) { continue; }
      ObIvfCacheMgrGuard guard;
      const ObIvfCacheMgrKey key(sstable_.get_key(), p);
      ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_, key, guard));
      auto *mgr = guard.get_ivf_cache_mgr();
      ObIvfSSTableCentCache *cache = nullptr;
      ASSERT_EQ(OB_SUCCESS, mgr->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, cache));
      bool erased = false; uint64_t hold = 0;
      ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(context.ls_id_, key,
          mgr, cache, cache->get_last_access_ts(), false, erased, hold));
      ASSERT_TRUE(erased);
    }
    struct SuspendAdmission {
      explicit SuspendAdmission(ObIvfSSTableCacheController &c) : c_(c), saved_(c.is_eviction_running())
      { ATOMIC_STORE(&c_.eviction_running_, true); }
      ~SuspendAdmission()
      { ATOMIC_STORE(&c_.eviction_running_, saved_); (void)c_.refresh_config(); }
      ObIvfSSTableCacheController &c_; bool saved_;
    } suspend(controller);
    int64_t allocation_count = 0;
    for (int64_t fail_at = 0; fail_at <= allocation_count; ++fail_at) {
      ObArenaAllocator arena;
      ObIvfCountingAllocator memory(arena);
      memory.watch_alloc_size(dim * sizeof(float));
      if (fail_at > 0) { memory.set_fail_on_alloc(fail_at); }
      ObIvfHierarchicalCentroidIterator iterator(memory);
      int ret = iterator.open(param, query, sstable_, iter_param, row_desc, context);
      if (ret == OB_SUCCESS) { ret = iterator.prepare_next_batch(1); }
      if (fail_at == 0) {
        ASSERT_EQ(OB_SUCCESS, ret);
        allocation_count = memory.alloc_count_;
        EXPECT_EQ(hot_parity == 1 ? 0 : 1, memory.watched_alloc_count_); // First selected leaf is in group 3.
        ObIvfCentroidInfo result;
        ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
        EXPECT_EQ(expected_cids[0], result.cid_);
        EXPECT_FLOAT_EQ(1600.0F, result.distance_);
        EXPECT_EQ(0, MEMCMP(leaves[2], result.data_, dim * sizeof(float)));
        // Borrowed cache vectors remain unchanged beside the current cold batch.
        for (int64_t p = 1; p <= 3; ++p) {
          if (p % 2 != hot_parity) { continue; }
          ObIvfCacheMgrGuard guard;
          const ObIvfCacheMgrKey key(sstable_.get_key(), p);
          ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_, key, guard));
          auto *mgr = guard.get_ivf_cache_mgr();
          ObIvfSSTableCentCache *cache = nullptr;
          ASSERT_EQ(OB_SUCCESS, mgr->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, cache));
          for (int64_t slot = 0; slot < cache->get_count(); ++slot) {
            EXPECT_EQ(0, MEMCMP(leaves[cache->get_centroid_id(slot) - parents - 1],
                cache->get_centroid_vector(slot), dim * sizeof(float)));
          }
          if (p == 3) { EXPECT_EQ(cache->get_centroid_vector(0), result.data_); }
          bool erased = false; uint64_t hold = 0;
          ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(context.ls_id_, key,
              mgr, cache, cache->get_last_access_ts(), false, erased, hold));
          EXPECT_FALSE(erased);
        }
        const ObIvfCentroidInfo first = result;
        for (int64_t i = 1; i < parents; ++i) {
          EXPECT_TRUE(iterator.has_remaining_centroids());
          ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(1));
          ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
          EXPECT_EQ(expected_cids[i], result.cid_);
          EXPECT_EQ(result.cid_ - parents, result.parent_id_);
          const float value = leaves[expected_cids[i] - parents - 1][0];
          EXPECT_FLOAT_EQ(value * value, result.distance_);
          EXPECT_EQ(0, MEMCMP(leaves[result.cid_ - parents - 1], result.data_, dim * sizeof(float)));
        }
        EXPECT_FALSE(iterator.has_remaining_centroids());
        EXPECT_EQ(1, memory.watched_alloc_count_); // All cold batches reuse one vector buffer.
        if (hot_parity == 1) { // Cache views remain valid beyond a batch.
          EXPECT_EQ(0, MEMCMP(leaves[2], first.data_, dim * sizeof(float)));
        }
      } else {
        EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret) << fail_at;
        EXPECT_EQ(OB_NOT_INIT, iterator.prepare_next_batch(1)) << fail_at;
      }
      iterator.reset();
      EXPECT_EQ(0, memory.live_allocation_count()) << fail_at;
    }
    if (hot_parity == -1) {
      // A cached parent must also support iterative continuation when every
      // child group comes from storage and no child cache can be admitted.
      ObArenaAllocator arena;
      ObIvfHierarchicalCentroidIterator iterator(arena);
      ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
      for (int64_t i = 0; i < parents; ++i) {
        ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(1));
        ObIvfCentroidInfo result;
        ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
        EXPECT_EQ(expected_cids[i], result.cid_);
        const float value = leaves[expected_cids[i] - parents - 1][0];
        EXPECT_FLOAT_EQ(value * value, result.distance_);
      }
      EXPECT_EQ(OB_ITER_END, iterator.prepare_next_batch(1));
      ObIvfCacheMgrGuard parent_guard;
      ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
          ObIvfCacheMgrKey(sstable_.get_key(), 0), parent_guard));
      for (int64_t p = 1; p <= parents; ++p) {
        ObIvfCacheMgrGuard absent;
        EXPECT_EQ(OB_HASH_NOT_EXIST, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
            ObIvfCacheMgrKey(sstable_.get_key(), p), absent));
      }
      EXPECT_EQ(0, controller.get_reserved_hold());
    }
  }
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), parents);
}

TEST_F(TestIVFSSTableWriter, cold_centroid_batches_preserve_order_and_reuse_vectors)
{
  CentroidCacheMemoryPercentageGuard cache_percentage;
  ASSERT_TRUE(cache_percentage.config_.is_valid());
  cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 0;
  const int64_t dim = 768, count = 8;
  float centers[count][dim] = {}, query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t i = 0; i < count; ++i) {
    centers[i][0] = count - i;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, i + 1, centers[i], dim, i + 10));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_count_ = count;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  param.nprobe_ = INT64_MAX;
  param.iterative_centroids_ = true;
  ObArenaAllocator arena;
  ObIvfCountingAllocator memory(arena);
  memory.watch_alloc_size(3 * dim * sizeof(float));
  ObIvfFlatCentroidIterator iterator(memory);
  ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
  ASSERT_EQ(count, iterator.result_count_);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_TRUE(iterator.results_[i].is_valid());
    EXPECT_FALSE(iterator.results_[i].has_vector());
  }
  EXPECT_EQ(0, memory.watched_alloc_count_);
  int64_t consumed = 0;
  for (int64_t batch : {3, 2, 3}) {
    const auto order = context.query_flag_.scan_order_;
    ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(batch));
    EXPECT_EQ(order, context.query_flag_.scan_order_);
    for (int64_t i = 0; i < consumed; ++i) { EXPECT_FALSE(iterator.results_[i].has_vector()); }
    for (int64_t i = 0; i < batch; ++i) {
      ObIvfCentroidInfo result;
      ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
      EXPECT_EQ(count - consumed, result.cid_); // Distance order is opposite to rowkey order.
      EXPECT_EQ(0, result.parent_id_);
      EXPECT_EQ(result.cid_ + 9, result.child_count_);
      EXPECT_EQ(0, MEMCMP(centers[result.cid_ - 1], result.data_, dim * sizeof(float)));
      ++consumed;
    }
  }
  EXPECT_EQ(1, memory.watched_alloc_count_);
  EXPECT_FALSE(iterator.has_remaining_centroids());
  iterator.reset();
  EXPECT_EQ(0, memory.live_allocation_count());

  // A failure in deferred materialization invalidates the iterator, with no
  // partial batch available to retry and no leaked query-owned allocations.
  ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
  memory.set_fail_alloc_size(3 * dim * sizeof(float));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, iterator.prepare_next_batch(3));
  EXPECT_FALSE(iterator.is_inited_);
  EXPECT_EQ(0, memory.live_allocation_count());
  memory.set_fail_alloc_size(-1);

  // Preserve direct get_next() consumers as well as explicitly batched ones.
  ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
  for (int64_t i = 0; i < count; ++i) {
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
    EXPECT_EQ(count - i, result.cid_);
    EXPECT_EQ(0, MEMCMP(centers[result.cid_ - 1], result.data_, dim * sizeof(float)));
  }
  iterator.reset();
  EXPECT_EQ(0, memory.live_allocation_count());

  // Parent routing must succeed even when a single vector allocation would fail.
  memory.set_fail_alloc_size(dim * sizeof(float));
  iterator.route_only_ = true;
  ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
  EXPECT_EQ(nullptr, iterator.vector_reader_);
  for (int64_t i = 0; i < count; ++i) {
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
    EXPECT_TRUE(result.is_valid());
    EXPECT_FALSE(result.has_vector());
    EXPECT_EQ(count - i, result.cid_);
    EXPECT_EQ(result.cid_ + 9, result.child_count_);
  }
  iterator.reset();
  EXPECT_EQ(0, memory.live_allocation_count());
}

TEST_F(TestIVFSSTableWriter, grouped_duplicate_cache_vectors_are_not_read)
{
  CentroidCacheMemoryPercentageGuard cache_percentage;
  ASSERT_TRUE(cache_percentage.config_.is_valid());
  cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  const int64_t dim = 64;
  float first[dim] = {1}, second[dim] = {2}, query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, 1, first, dim, 2));
  ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, 2, second, dim, 2));
  for (int64_t p = 1; p <= 2; ++p) {
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, 3, first, dim, 1));
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, 4, second, dim, 1));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = 2;
  sstable_.ivf_meta_.centroid_count_ = 2;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(context.ls_id_, ls_mgr, false));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  param.nprobe_ = 1;
  param.iterative_centroids_ = true;
  {
    ObArenaAllocator arena;
    ObIvfHierarchicalCentroidIterator warm(arena);
    ASSERT_EQ(OB_SUCCESS, warm.open(param, query, sstable_, iter_param, row_desc, context));
    ASSERT_EQ(OB_SUCCESS, warm.prepare_next_batch(1));
  }
  ObIvfCacheMgrGuard guard;
  ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
      ObIvfCacheMgrKey(sstable_.get_key(), 2), guard));
  ObIvfSSTableCentCache *duplicate_cache = nullptr;
  ASSERT_EQ(OB_SUCCESS, guard.get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
      IVF_SSTABLE_CENTROID_CACHE, duplicate_cache));
  ASSERT_NE(nullptr, duplicate_cache);
  const int64_t page_size = sysconf(_SC_PAGESIZE);
  void *blocked = mmap(nullptr, 4 * page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ASSERT_NE(MAP_FAILED, blocked);
  DEFER(munmap(blocked, 4 * page_size));
  using SharedLeaf = ObIvfSSTableCentCache::SharedLeaf;
  SharedLeaf *unreadable[2];
  for (int i = 0; i < 2; ++i) {
    char *vector_page = static_cast<char *>(blocked) + (2 * i + 1) * page_size;
    unreadable[i] = new (vector_page - sizeof(SharedLeaf)) SharedLeaf{3 + i, 1, 1};
    ASSERT_EQ(0, mprotect(vector_page, page_size, PROT_NONE));
  }
  {
    // Group 2 identities are readable, but touching duplicate vector data faults.
    SharedLeaf **saved = duplicate_cache->leaf_refs_;
    duplicate_cache->leaf_refs_ = unreadable;
    DEFER(duplicate_cache->leaf_refs_ = saved);
    ObArenaAllocator arena;
    ObIvfHierarchicalCentroidIterator iterator(arena);
    ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
    const int64_t count = 2;
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(1));
      ObIvfCentroidInfo result;
      ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
      EXPECT_EQ(3 + i, result.cid_);
      EXPECT_FLOAT_EQ(static_cast<float>((i + 1) * (i + 1)), result.distance_);
    }
    EXPECT_FALSE(iterator.has_remaining_centroids());
  }
  guard.reset();
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), 2);
}

TEST_F(TestIVFSSTableWriter, grouped_candidate_heap_matches_sort_and_cleans_up_failures)
{
  CentroidCacheMemoryPercentageGuard cache_percentage;
  CentroidHgraphThresholdGuard graph_threshold;
  ASSERT_TRUE(cache_percentage.config_.is_valid());
  ASSERT_TRUE(graph_threshold.config_.is_valid());
  graph_threshold.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  const int64_t parents = 3, leaves = 40, dim = 64;
  float vectors[leaves][dim] = {}, query[dim] = {};
  std::vector<std::pair<float, int64_t>> expected;
  for (int64_t i = 0; i < leaves; ++i) {
    vectors[i][0] = (leaves - i + 1) / 2;
    expected.emplace_back(vectors[i][0] * vectors[i][0], parents + 1 + i);
  }
  std::sort(expected.begin(), expected.end());
  const auto belongs = [&](int64_t p, int64_t i) {
    const int64_t primary = (leaves - i - 1) % parents + 1;
    return p == primary || p == primary % parents + 1;
  };
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t p = 1; p <= parents; ++p) {
    float parent[dim] = {}; parent[0] = p;
    int64_t count = 0;
    for (int64_t i = 0; i < leaves; ++i) { count += belongs(p, i); }
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, p, parent, dim, count));
  }
  for (int64_t p = 1; p <= parents; ++p) {
    for (int64_t i = 0; i < leaves; ++i) {
      if (belongs(p, i)) {
        ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, parents + 1 + i, vectors[i], dim, 1));
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = parents;
  sstable_.ivf_meta_.centroid_count_ = leaves;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(context.ls_id_, ls_mgr, false));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  for (int64_t cache_percent : {0, 80}) {
    cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = cache_percent;
    for (bool iterative : {false, true}) {
      // These budgets open all three parents, allowing comparison with a
      // global sort. Small parent budgets are covered by the routing tests.
      for (int64_t nprobe : {16, 17, 39, 45}) {
        ObArenaAllocator arena;
        ObIvfHierarchicalCentroidIterator iterator(arena);
        param.nprobe_ = nprobe;
        param.iterative_centroids_ = iterative;
        ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
        const int64_t total = iterative ? leaves : MIN(leaves, nprobe);
        int64_t returned = 0;
        while (returned < total) {
          ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(nprobe));
          const int64_t batch = MIN(nprobe, total - returned);
          for (int64_t i = 0; i < batch; ++i, ++returned) {
            ObIvfCentroidInfo result;
            ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
            ASSERT_EQ(expected[returned].second, result.cid_);
            EXPECT_FLOAT_EQ(expected[returned].first, result.distance_);
            EXPECT_EQ(0, MEMCMP(vectors[result.cid_ - parents - 1], result.data_, dim * sizeof(float)));
          }
          ObIvfCentroidInfo exhausted;
          EXPECT_EQ(OB_ITER_END, iterator.get_next(exhausted));
        }
        EXPECT_EQ(OB_ITER_END, iterator.prepare_next_batch(nprobe));
        iterator.reset(); // Cache access times are updated at reader release.
        if (cache_percent > 0) {
          ObIvfCacheMgrGuard parent_guard;
          ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
              ObIvfCacheMgrKey(sstable_.get_key(), 0), parent_guard));
          ObIvfSSTableCentCache *parent_cache = nullptr;
          ASSERT_EQ(OB_SUCCESS, parent_guard.get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
              IVF_SSTABLE_CENTROID_CACHE, parent_cache));
          for (int64_t p = 1; p <= parents; ++p) {
            ObIvfCacheMgrGuard child_guard;
            ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(context.ls_id_,
                ObIvfCacheMgrKey(sstable_.get_key(), p), child_guard));
            ObIvfSSTableCentCache *child_cache = nullptr;
            ASSERT_EQ(OB_SUCCESS, child_guard.get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
                IVF_SSTABLE_CENTROID_CACHE, child_cache));
            EXPECT_GT(child_cache->get_last_access_ts(), 0);
            EXPECT_GE(parent_cache->get_last_access_ts(), child_cache->get_last_access_ts());
          }
        }
      }
    }
  }
  // Exceed the heap's inline capacity, then fail every query allocation.
  // This also verifies that a CID marked before a failed insertion cannot be
  // silently skipped by retrying a partially completed iterator.
  cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 0;
  param.nprobe_ = 17;
  param.iterative_centroids_ = true;
  int64_t allocations = 0;
  for (int64_t fail_at = 0; fail_at <= allocations; ++fail_at) {
    ObArenaAllocator arena;
    ObIvfCountingAllocator memory(arena);
    if (fail_at > 0) { memory.set_fail_on_alloc(fail_at); }
    ObIvfHierarchicalCentroidIterator iterator(memory);
    int ret = iterator.open(param, query, sstable_, iter_param, row_desc, context);
    if (ret == OB_SUCCESS) { ret = iterator.prepare_next_batch(param.nprobe_); }
    if (fail_at == 0) {
      ASSERT_EQ(OB_SUCCESS, ret);
      allocations = memory.alloc_count_;
    } else {
      EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret) << fail_at;
      EXPECT_EQ(OB_NOT_INIT, iterator.prepare_next_batch(param.nprobe_)) << fail_at;
    }
    iterator.reset();
    EXPECT_EQ(0, memory.live_allocation_count()) << fail_at;
  }

  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), parents);
}

TEST_F(TestIVFSSTableWriter, flat_child_budget_selects_distance_prefix_and_keeps_filter_tail)
{
  const int32_t dim = 64;
  const uint32_t weights[] = {1, 3, 2, 5, 1, 8};
  float vector[dim] = {}, query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t i = 0; i < 6; ++i) {
    vector[0] = i + 1;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, i + 1, vector, dim, weights[i]));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_count_ = 6;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  CentroidCacheMemoryPercentageGuard cache;
  CentroidHgraphThresholdGuard graph;
  ASSERT_TRUE(cache.config_.is_valid());
  ASSERT_TRUE(graph.config_.is_valid());
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, MTL(ObPluginVectorIndexService *)->acquire_vector_index_mgr(
      context.ls_id_, ls_mgr, false));
  graph.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  for (const int64_t percentage : {0, 80}) {
    cache.config_->_disk_bbq_centroid_cache_mem_percentage = percentage;
    for (const bool iterative : {false, true}) {
      for (const float query_value : {0.0F, 7.0F}) {
        SCOPED_TRACE(query_value);
        query[0] = query_value;
        ObArenaAllocator arena;
        ObIvfFlatCentroidIterator iterator(arena);
        ObIvfKnnSearchParam param;
        fill_search_params(param, query, dim, 1);
        param.nprobe_ = iterative ? INT64_MAX : 1;
        param.iterative_centroids_ = iterative;
        param.centroid_child_budget_ = 7;
        ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
        int64_t selected = 0;
        ASSERT_EQ(OB_SUCCESS, iterator.prepare_initial_batch(7, param.cluster_size_, selected));
        // Forward distances need four centers; reverse distances eventually
        // replace and trim that prefix to the last center, whose weight is 8.
        ASSERT_EQ(query_value == 0 ? 4 : 1, selected);
        ObIvfCentroidInfo item;
        for (int64_t i = 0; i < selected; ++i) {
          ASSERT_EQ(OB_SUCCESS, iterator.get_next(item));
          const int64_t cid = query_value == 0 ? i + 1 : 6 - i;
          EXPECT_EQ(cid, item.cid_);
          ASSERT_NE(nullptr, item.data_);
          EXPECT_FLOAT_EQ(cid, item.data_[0]);
        }
        EXPECT_EQ(iterative, iterator.has_remaining_centroids());
        if (iterative) {
          ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(6 - selected));
          for (int64_t i = selected; i < 6; ++i) {
            ASSERT_EQ(OB_SUCCESS, iterator.get_next(item));
            EXPECT_EQ(query_value == 0 ? i + 1 : 6 - i, item.cid_);
          }
        }
      }
    }
  }
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), 0);
}

TEST_F(TestIVFSSTableWriter, grouped_child_budget_deduplicates_soar_and_revises_parent_demand)
{
  const int32_t dim = 64;
  float vector[dim] = {}, query[dim] = {};
  const uint32_t parent_counts[] = {3, 3, 2};
  const int64_t members[][3] = {{4, 5, 6}, {5, 6, 7}, {7, 8, 0}};
  const float distances[] = {10, 6, 5, 2, 1};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t parent = 1; parent <= 3; ++parent) {
    vector[0] = parent;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, parent, vector, dim, parent_counts[parent - 1]));
  }
  for (int64_t parent = 1; parent <= 3; ++parent) {
    for (int64_t i = 0; i < parent_counts[parent - 1]; ++i) {
      const int64_t cid = members[parent - 1][i];
      vector[0] = distances[cid - 4];
      ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, parent, cid, vector, dim, cid - 2));
    }
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = 3;
  sstable_.ivf_meta_.centroid_count_ = 5;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  CentroidCacheMemoryPercentageGuard cache;
  ASSERT_TRUE(cache.config_.is_valid());
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, MTL(ObPluginVectorIndexService *)->acquire_vector_index_mgr(
      context.ls_id_, ls_mgr, false));
  for (const int64_t percentage : {0, 80}) {
    cache.config_->_disk_bbq_centroid_cache_mem_percentage = percentage;
    for (const bool iterative : {false, true}) {
      for (const bool automatic : {false, true}) {
        ObArenaAllocator arena;
        ObIvfHierarchicalCentroidIterator iterator(arena);
        ObIvfKnnSearchParam param;
        fill_search_params(param, query, dim, 1);
        param.nprobe_ = 2;
        param.iterative_centroids_ = iterative;
        param.centroid_child_budget_ = automatic ? 6 : 0;
        ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
        int64_t selected = 2;
        if (automatic) {
          ASSERT_EQ(OB_SUCCESS, iterator.prepare_initial_batch(6, param.cluster_size_, selected));
        } else {
          ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(2));
        }
        ASSERT_EQ(automatic ? 1 : 2, selected);
        ObIvfCentroidInfo item;
        for (int64_t i = 0; i < selected; ++i) {
          ASSERT_EQ(OB_SUCCESS, iterator.get_next(item));
          EXPECT_EQ(8 - i, item.cid_);
          ASSERT_NE(nullptr, item.data_);
          EXPECT_FLOAT_EQ(distances[item.cid_ - 4], item.data_[0]);
        }
        if (iterative) {
          for (int64_t cid = 8 - selected; cid >= 4; --cid) {
            ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(1));
            ASSERT_EQ(OB_SUCCESS, iterator.get_next(item));
            EXPECT_EQ(cid, item.cid_);
          }
        }
        EXPECT_FALSE(iterator.has_remaining_centroids());
      }
    }
  }
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), 3);
}

TEST_F(TestIVFSSTableWriter, grouped_parent_budget_tracks_requested_unique_leaf_count)
{
  const int32_t dim = 64;
  const int64_t parents = 34;
  float vector[dim] = {}, query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t p = 1; p <= parents; ++p) {
    vector[0] = p;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, p, vector, dim, 1));
  }
  for (int64_t p = 1; p <= parents; ++p) {
    // Parents route in ascending order, while the closest child is in the
    // last expanded group. This exposes the actual initial search width.
    vector[0] = parents - p;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, parents + p, vector, dim, 1));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = parents;
  sstable_.ivf_meta_.centroid_count_ = parents;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  struct ProbeCase { int64_t leaves_; double refine_k_; int64_t last_parent_; };
  const ProbeCase cases[] = {
      {1, 4.0, 8}, {2, 4.0, 16}, {2, 2.5, 10},
      {1, 2.0, 4}, {2, 2.0, 8}, {4, 2.0, 16}, {9, 2.0, 34}, {34, 2.0, 34},
      {1, 1.0, 2}, {2, 1.5, 6}, {2, 3.0, 12}, {2, 1000.0, 34}};
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, MTL(ObPluginVectorIndexService *)->acquire_vector_index_mgr(
      context.ls_id_, ls_mgr, false));
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.leaves_);
    SCOPED_TRACE(test_case.refine_k_);
    ObArenaAllocator arena;
    ObIvfKnnSearchParam param;
    fill_search_params(param, query, dim, 1);
    EXPECT_FLOAT_EQ(ObVectorIndexParam::DEFAULT_REFINE_K, param.refine_k_);
    param.refine_k_ = test_case.refine_k_;
    param.nprobe_ = test_case.leaves_;
    param.iterative_centroids_ = true;
    ObIvfHierarchicalCentroidIterator iterator(arena);
    ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
    // The iterator keeps the resolved query multiplier copied at open().
    param.refine_k_ = 1000.0F;
    ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(test_case.leaves_));
    ObIvfCentroidInfo result;
    for (int64_t i = 0; i < test_case.leaves_; ++i) {
      ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
      EXPECT_EQ(parents + test_case.last_parent_ - i, result.cid_);
    }
  }
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), parents);
}

TEST_F(TestIVFSSTableWriter, grouped_parent_hgraph_prefetch_keeps_actual_child_budgets)
{
  const int32_t dim = 64;
  const int64_t parents = 4;
  const int64_t group_size = IVF_PARENT_CLUSTER_SIZE;
  float vector[dim] = {}, query[dim] = {};
  CentroidCacheMemoryPercentageGuard cache_percentage;
  CentroidHgraphThresholdGuard graph_threshold;
  ASSERT_TRUE(cache_percentage.config_.is_valid());
  ASSERT_TRUE(graph_threshold.config_.is_valid());
  cache_percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  graph_threshold.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t p = 1; p <= parents; ++p) {
    vector[0] = p;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, p, vector, dim, group_size));
  }
  for (int64_t p = 1; p <= parents; ++p) {
    for (int64_t j = 0; j < group_size; ++j) {
      // Later parent groups contain closer leaves. Prefetching extra parents
      // must not expand them before the actual child budgets require it.
      vector[0] = (parents - p) * 1000 + j + 1;
      const int64_t cid = parents + (p - 1) * group_size + j + 1;
      ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, cid, vector, dim, 100));
    }
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = parents;
  sstable_.ivf_meta_.centroid_count_ = parents * group_size;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  auto *service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(context.ls_id_, ls_mgr, false));
  {
    ObArenaAllocator arena;
    ObIvfFlatCentroidIterator parent_reader(arena, true /* route_only */);
    ObIvfKnnSearchParam param;
    fill_search_params(param, query, dim, 1);
    param.nprobe_ = INT64_MAX;
    ASSERT_EQ(OB_SUCCESS, parent_reader.open(param, query, sstable_, iter_param, row_desc, context));
  }
  {
    ObIvfCacheMgrGuard guard;
    ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(
        context.ls_id_, ObIvfCacheMgrKey(sstable_.get_key(), 0), guard));
    auto *manager = guard.get_ivf_cache_mgr();
    ObIvfSSTableCentCache *cache = nullptr;
    ASSERT_EQ(OB_SUCCESS, manager->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, cache));
    ASSERT_NE(nullptr, cache);
    ObVectorIndexParam graph = manager->get_vec_param();
    graph.type_ = VIAT_HGRAPH;
    graph.lib_ = VIAL_VSAG;
    graph.dist_algorithm_ = VIDA_L2;
    graph.dim_ = dim;
    graph.nlist_ = parents;
    graph.m_ = 16;
    graph.ef_construction_ = 200;
    graph.ef_search_ = 64;
    graph.extra_info_actual_size_ = 0;
    graph.refine_type_ = 0;
    graph.bq_bits_query_ = 32;
    graph.bq_use_fht_ = false;
    graph_threshold.config_->_disk_bbq_centroid_hgraph_threshold = 1;
    ASSERT_EQ(OB_SUCCESS, cache->build_sstable_hgraph(graph));
    ASSERT_TRUE(cache->has_hgraph_index());
    cache->update_registered_hold(cache->get_memory_hold());
    graph_threshold.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  }
  struct ProbeCase {
    int64_t nprobe_;
    int64_t child_budget_;
    int64_t cluster_size_;
    int64_t selected_count_;
    int64_t expanded_parents_;
    float refine_k_ = ObVectorIndexParam::DEFAULT_REFINE_K;
  };
  const ProbeCase cases[] = {
      {1, 0, 384, 1, 1}, {100, 0, 384, 100, 3},
      {100, 0, 384, 100, 2, 2.0F}, {100, 0, 384, 100, 1, 1.5F},
      {1, 100, 1, 1, 1}, {1, 1000, 1, 10, 1}, // Overestimate, including the parent-count cap.
      {1, 1000, 0, 10, 1},                    // Default leaf cluster size.
      {1, 100000, 100000, 1000, 4}};          // Underestimate; expand beyond the first graph batch.
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.nprobe_);
    SCOPED_TRACE(test_case.child_budget_);
    SCOPED_TRACE(test_case.cluster_size_);
    ObArenaAllocator arena;
    ObIvfHierarchicalCentroidIterator iterator(arena);
    ObIvfKnnSearchParam param;
    fill_search_params(param, query, dim, 1);
    param.refine_k_ = test_case.refine_k_;
    param.nprobe_ = test_case.nprobe_;
    param.centroid_child_budget_ = test_case.child_budget_;
    param.cluster_size_ = test_case.cluster_size_;
    param.iterative_centroids_ = true;
    ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
    int64_t selected = test_case.nprobe_;
    if (test_case.child_budget_ > 0) {
      ASSERT_EQ(OB_SUCCESS, iterator.prepare_initial_batch(test_case.child_budget_, param.cluster_size_, selected));
    } else {
      ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(test_case.nprobe_));
    }
    EXPECT_EQ(test_case.selected_count_, selected);
    EXPECT_EQ(test_case.selected_count_, iterator.result_count_);
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
    EXPECT_EQ(parents + (test_case.expanded_parents_ - 1) * group_size + 1, result.cid_);
    EXPECT_FLOAT_EQ((parents - test_case.expanded_parents_) * 1000 + 1, result.data_[0]);
  }
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(), parents);
}

TEST_F(TestIVFSSTableWriter, grouped_centroids_finish_initial_parent_batch_before_topk)
{
  const int32_t dim = 64;
  const int64_t parents = 34; // Default refine_k=4 requires eight child memberships for L=1.
  float vector[dim] = {}, query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t p = 1; p <= parents; ++p) {
    vector[0] = p;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, 0, p, vector, dim, 1));
  }
  for (int64_t p = 1; p <= parents; ++p) {
    vector[0] = p == 2 ? 0.1F : 100.0F + p;
    ASSERT_EQ(OB_SUCCESS, write_centroid_row(writer, p, parents + p, vector, dim, 1));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  sstable_.ivf_meta_.parent_centroid_count_ = parents;
  sstable_.ivf_meta_.centroid_count_ = parents;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(sstable_, read_info, row_desc));
  ObArenaAllocator arena;
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  ObIvfHierarchicalCentroidIterator iterator(arena);
  ASSERT_EQ(OB_SUCCESS, iterator.open(param, query, sstable_, iter_param, row_desc, context));
  ASSERT_EQ(OB_SUCCESS, iterator.prepare_next_batch(1));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator.get_next(result));
  EXPECT_EQ(parents + 2, result.cid_);

  iterator.reset();
  clear_hierarchical_test_cache(context.ls_id_, sstable_.get_key(),
      sstable_.get_ivf_meta().parent_centroid_count_);
}

TEST_P(TestIVFCentroidMetric,
    centroid_iterator_selects_by_persisted_metric)
{
  const int32_t dim = 64;
  const int32_t nlist = 3;
  const ObIvfCentroidMetricCase metric_case = GetParam();
  std::vector<float> centroids(nlist * dim, 0.0f);
  centroids[0 * dim] = 100.0f;
  centroids[0 * dim + 1] = 1.0f;
  centroids[1 * dim] = 1.0f;
  centroids[1 * dim + 1] = 1.0f;
  centroids[2 * dim] = 2.0f;
  if (share::VIDA_COS == metric_case.dist_type_) {
    for (int64_t cid = 0; cid < nlist; ++cid) {
      float *center = centroids.data() + cid * dim;
      ASSERT_EQ(OB_SUCCESS,
          share::ObVectorNormalize::L2_normalize_vector(dim, center, center));
    }
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t cid = 0; cid < nlist; ++cid) {
    ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
        0, cid + 1, centroids.data() + cid * dim, dim));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(
      dim, nullptr, 1, metric_case.dist_type_));
  ASSERT_EQ(metric_case.dist_type_, sstable_.get_ivf_meta().dist_type_);
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float rotated_query[dim] = {0};
  rotated_query[0] = 1.0f;
  int64_t oracle_cid = -1;
  float oracle_distance = std::numeric_limits<float>::infinity();
  for (int64_t cid = 0; cid < nlist; ++cid) {
    const float *centroid = centroids.data() + cid * dim;
    float distance = 0.0f;
    if (share::VIDA_L2 == metric_case.dist_type_) {
      for (int64_t d = 0; d < dim; ++d) {
        const float delta = rotated_query[d] - centroid[d];
        distance += delta * delta;
      }
    } else if (share::VIDA_IP == metric_case.dist_type_) {
      float inner_product = 0.0f;
      for (int64_t d = 0; d < dim; ++d) {
        inner_product += rotated_query[d] * centroid[d];
      }
      distance = 1.0f - inner_product;
    } else {
      float inner_product = 0.0f;
      float query_norm_square = 0.0f;
      float centroid_norm_square = 0.0f;
      for (int64_t d = 0; d < dim; ++d) {
        inner_product += rotated_query[d] * centroid[d];
        query_norm_square += rotated_query[d] * rotated_query[d];
        centroid_norm_square += centroid[d] * centroid[d];
      }
      distance = 1.0f - inner_product /
          std::sqrt(query_norm_square * centroid_norm_square);
    }
    if (distance < oracle_distance
        || (distance == oracle_distance && cid < oracle_cid)) {
      oracle_cid = cid;
      oracle_distance = distance;
    }
  }
  ASSERT_EQ(metric_case.expected_cid_, oracle_cid);
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, rotated_query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(sstable_, query_param, rotated_query, query_allocator, read_info, iterator.get()));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
  ASSERT_EQ(metric_case.expected_cid_ + 1, result.cid_);
  ASSERT_NEAR(oracle_distance, result.distance_, 1e-5f);
  ASSERT_EQ(OB_ITER_END, iterator->get_next(result));
}

INSTANTIATE_TEST_CASE_P(
    L2IpAndCosine,
    TestIVFCentroidMetric,
    ::testing::Values(
        ObIvfCentroidMetricCase{share::VIDA_L2, 1},
        ObIvfCentroidMetricCase{share::VIDA_IP, 0},
        ObIvfCentroidMetricCase{share::VIDA_COS, 2}));

TEST(TestIvfSSTableCacheController, access_counts_are_concurrent_and_reset_with_controller)
{
  ObIvfSSTableCacheController controller;
  EXPECT_EQ(0, controller.get_hit_count());
  EXPECT_EQ(0, controller.get_miss_count());
  std::vector<std::thread> workers;
  for (int64_t worker = 0; worker < 4; ++worker) {
    workers.emplace_back([&controller]() {
      for (int64_t i = 0; i < 1000; ++i) {
        controller.record_cache_access(true);
        controller.record_cache_access(false);
      }
    });
  }
  for (std::thread &worker : workers) {
    worker.join();
  }
  EXPECT_EQ(4000, controller.get_hit_count());
  EXPECT_EQ(4000, controller.get_miss_count());
  controller.reset();
  EXPECT_EQ(0, controller.get_hit_count());
  EXPECT_EQ(0, controller.get_miss_count());
}

TEST_F(TestIVFSSTableWriter,
    centroid_iterator_builds_array_cache_on_miss_and_hits_next_query)
{
  const int32_t dim = 64;
  const int64_t nlist = 3;
  const int64_t cluster_size = 512;
  const share::ObLSID cache_ls_id(IVF_LS_ID);
  std::vector<float> centroids(nlist * dim, 0.0F);
  float query[dim] = {0};
  centroids[0 * dim] = 5.0F;
  centroids[1 * dim] = 1.0F;
  centroids[2 * dim] = 3.0F;

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t cid = 0; cid < nlist; ++cid) {
    ASSERT_EQ(OB_SUCCESS, write_float_vec_row(
        writer, IVF_ROW_TYPE_CENTROID, 0, cid + 1,
        centroids.data() + cid * dim, dim));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_count_ = nlist;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_ivf_output_row_desc_for_test(
      sstable_, read_info, row_desc));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.nprobe_ = 2;
  query_param.cluster_size_ = cluster_size;

  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(
      cache_ls_id, ls_mgr, false /* check_ls_exist */));
  ASSERT_NE(nullptr, ls_mgr);
  const ObIvfCacheMgrKey cache_key(sstable_.get_key());
  ObIvfCacheMgrGuard missing_guard;
  ASSERT_EQ(OB_HASH_NOT_EXIST, service->acquire_ivf_cache_mgr_guard(
      cache_ls_id, cache_key, missing_guard));

  ObIvfSSTableCacheController &controller =
      service->get_ivf_sstable_cache_controller();
  const uint64_t resident_before = controller.get_resident_hold();
  const uint64_t hits_before = controller.get_hit_count();
  const uint64_t misses_before = controller.get_miss_count();
  ObTableIterParam first_iter_param;
  ObTableAccessContext first_access_ctx;
  ObStoreCtx first_store_ctx;
  ASSERT_EQ(OB_SUCCESS, init_scan_context(
      read_info, first_iter_param, first_access_ctx, first_store_ctx));
  ObArenaAllocator first_query_allocator;
  {
    ObIvfCentroidIteratorGuard first_iterator(allocator_);
    ASSERT_EQ(OB_SUCCESS, sstable_.create_centroid_iterator(
        query_param, query, allocator_, first_query_allocator,
        first_iter_param, row_desc, first_access_ctx, first_iterator.get()));
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, first_iterator->get_next(result));
    EXPECT_EQ(2, result.cid_);
    EXPECT_FLOAT_EQ(1.0F, result.distance_);
    ASSERT_EQ(OB_SUCCESS, first_iterator->get_next(result));
    EXPECT_EQ(3, result.cid_);
    EXPECT_FLOAT_EQ(9.0F, result.distance_);
    EXPECT_EQ(OB_ITER_END, first_iterator->get_next(result));
  }

  ObIvfCacheMgrGuard cache_mgr_guard;
  ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(
      cache_ls_id, cache_key, cache_mgr_guard));
  ObIvfCacheMgr *cache_mgr = cache_mgr_guard.get_ivf_cache_mgr();
  ASSERT_NE(nullptr, cache_mgr);
  EXPECT_EQ(cluster_size, cache_mgr->get_vec_param().cluster_size_);
  ObIvfSSTableCentCache *cache = nullptr;
  ASSERT_EQ(OB_SUCCESS, cache_mgr->get_cache_node<ObIvfSSTableCentCache>(
      IVF_SSTABLE_CENTROID_CACHE, cache));
  ASSERT_NE(nullptr, cache);
  EXPECT_EQ(misses_before + 1, controller.get_miss_count());
  EXPECT_EQ(hits_before, controller.get_hit_count());
  EXPECT_EQ(0, cache->get_hit_count());
  const int64_t first_load_ts = cache->get_first_load_ts();
  const int64_t first_access_ts = cache->get_last_access_ts();
  EXPECT_GT(first_load_ts, 0);
  EXPECT_GE(first_access_ts, first_load_ts);
  const float *cached_centroids = nullptr;
  {
    common::RWLock::RLockGuard guard(cache->get_lock());
    EXPECT_TRUE(cache->is_completed());
    EXPECT_EQ(nullptr, cache->get_hgraph_index());
    EXPECT_EQ(nlist, cache->get_count());
    EXPECT_EQ(dim, cache->get_cent_vec_dim());
    cached_centroids = cache->get_centroids();
  }
  ASSERT_NE(nullptr, cached_centroids);
  EXPECT_TRUE(cache->is_memory_registered());
  const uint64_t registered_hold = cache->get_registered_hold();
  ASSERT_GT(registered_hold, 0);
  EXPECT_EQ(resident_before + registered_hold, controller.get_resident_hold());

  ObIvfCountingAllocator second_query_allocator(allocator_);
  const int64_t centroid_bytes = dim * static_cast<int64_t>(sizeof(float));
  second_query_allocator.watch_alloc_size(centroid_bytes);
  second_query_allocator.set_fail_alloc_size(centroid_bytes);
  ObTableIterParam second_iter_param;
  ObTableAccessContext second_access_ctx;
  ObStoreCtx second_store_ctx;
  ASSERT_EQ(OB_SUCCESS, init_scan_context(
      read_info, second_iter_param, second_access_ctx, second_store_ctx));
  {
    ObIvfCentroidIteratorGuard second_iterator(allocator_);
    ASSERT_EQ(OB_SUCCESS, sstable_.create_centroid_iterator(
        query_param, query, allocator_, second_query_allocator,
        second_iter_param, row_desc, second_access_ctx, second_iterator.get()));
    EXPECT_EQ(cache_mgr, flat_group_reader_for_test(second_iterator.get()).cache_mgr_guard_.get_ivf_cache_mgr());
    EXPECT_EQ(cache, flat_group_reader_for_test(second_iterator.get()).array_cache_guard_.cache_);
    EXPECT_FALSE(second_iterator->owns_result_vectors_);
    EXPECT_EQ(hits_before + 1, controller.get_hit_count());
    EXPECT_EQ(0, cache->get_hit_count());
    EXPECT_EQ(first_access_ts, cache->get_last_access_ts());
    ASSERT_EQ(OB_SUCCESS, second_iterator->prepare_next_batch(1));
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, second_iterator->get_next(result));
    EXPECT_EQ(2, result.cid_);
    EXPECT_EQ(cached_centroids + dim, result.data_);
    EXPECT_FLOAT_EQ(1.0F, result.distance_);
    ASSERT_EQ(OB_SUCCESS, second_iterator->get_next(result));
    EXPECT_EQ(3, result.cid_);
    EXPECT_EQ(cached_centroids + 2 * dim, result.data_);
    EXPECT_FLOAT_EQ(9.0F, result.distance_);
    EXPECT_EQ(OB_ITER_END, second_iterator->get_next(result));
  }
  EXPECT_EQ(1, cache->get_hit_count());
  EXPECT_EQ(first_load_ts, cache->get_first_load_ts());
  EXPECT_GE(cache->get_last_access_ts(), first_access_ts);
  EXPECT_EQ(0, second_query_allocator.watched_alloc_count_);
  EXPECT_EQ(0, second_query_allocator.live_allocation_count());
  EXPECT_EQ(registered_hold, cache->get_registered_hold());
  EXPECT_EQ(resident_before + registered_hold, controller.get_resident_hold());

  EXPECT_EQ(misses_before + 1, controller.get_miss_count());
  EXPECT_EQ(hits_before + 1, controller.get_hit_count());
  {
    ObIvfFlatCentroidIterator lookup(first_query_allocator);
    bool hit = true;
    // Explicitly bypassing cache is neither a hit nor a miss.
    ASSERT_EQ(OB_SUCCESS, lookup.reader_.try_open_cache(
        query_param, ObLSID(), cache_key, hit));
    EXPECT_FALSE(hit);
    EXPECT_EQ(misses_before + 1, controller.get_miss_count());

    // A concurrent builder's entry cannot satisfy a lookup yet.
    cache->set_idle();
    DEFER(cache->set_completed());
    ASSERT_TRUE(cache->set_writing_if_idle());
    ASSERT_EQ(OB_SUCCESS, lookup.reader_.try_open_cache(
        query_param, cache_ls_id, cache_key, hit));
    EXPECT_FALSE(hit);
    EXPECT_EQ(misses_before + 2, controller.get_miss_count());
    EXPECT_EQ(hits_before + 1, controller.get_hit_count());
  }
  {
    // An invalid query against a completed cache is an error, not a miss.
    ObIvfFlatCentroidIterator lookup(first_query_allocator);
    EXPECT_EQ(OB_INVALID_ARGUMENT, lookup.open(query_param, nullptr, sstable_,
        first_iter_param, row_desc, first_access_ctx));
    EXPECT_EQ(nullptr, lookup.reader_.cache_mgr_guard_.get_ivf_cache_mgr());
    EXPECT_EQ(misses_before + 2, controller.get_miss_count());
    EXPECT_EQ(hits_before + 1, controller.get_hit_count());
  }
  {
    // Exercise the same STATISTICS formatter used by vector_index_info.
    // This fixture has no schema for the synthetic index table.
    const uint64_t saved_table_id = cache_mgr->table_id_;
    DEFER(cache_mgr->table_id_ = saved_table_id);
    cache_mgr->table_id_ = OB_INVALID_ID;
    ObVectorIndexInfo info;
    ASSERT_EQ(OB_SUCCESS, cache_mgr->fill_cache_info(info));
    char expected[160];
    snprintf(expected, sizeof(expected),
        "\"tenant_cache_hit_count\":%lu,\"tenant_cache_miss_count\":%lu",
        hits_before + 1, misses_before + 2);
    EXPECT_NE(nullptr, std::strstr(info.statistics_, expected));
    const double hit_ratio = 100.0 * static_cast<double>(hits_before + 1)
        / (static_cast<double>(hits_before + 1) + static_cast<double>(misses_before + 2));
    snprintf(expected, sizeof(expected), "\"tenant_cache_hit_ratio\":\"%.2f%%\"", hit_ratio);
    EXPECT_NE(nullptr, std::strstr(info.statistics_, expected));
    snprintf(expected, sizeof(expected), "\"hit_count\":1,\"first_load_ts\":%ld", first_load_ts);
    EXPECT_NE(nullptr, std::strstr(info.statistics_, expected));
    EXPECT_EQ(1, cache->get_hit_count()); // Statistics reads do not count as iterator hits.
    snprintf(expected, sizeof(expected), "cluster_size=%ld\"", cluster_size);
    EXPECT_NE(nullptr, std::strstr(info.statistics_, expected));
  }
  {
    CentroidCacheMemoryPercentageGuard percentage;
    ASSERT_TRUE(percentage.config_.is_valid());
    percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 0;
    ObArenaAllocator cold_query_allocator;
    ObTableIterParam cold_iter_param;
    ObTableAccessContext cold_access_ctx;
    ObStoreCtx cold_store_ctx;
    ASSERT_EQ(OB_SUCCESS, init_scan_context(
        read_info, cold_iter_param, cold_access_ctx, cold_store_ctx));
    ObIvfCentroidIteratorGuard cold_iterator(allocator_);
    ASSERT_EQ(OB_SUCCESS, sstable_.create_centroid_iterator(query_param, query,
        allocator_, cold_query_allocator, cold_iter_param, row_desc, cold_access_ctx, cold_iterator.get()));
    EXPECT_EQ(nullptr, flat_group_reader_for_test(cold_iterator.get()).cache_mgr_guard_.get_ivf_cache_mgr());
    EXPECT_TRUE(cold_iterator->owns_result_vectors_);
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, cold_iterator->get_next(result));
    EXPECT_EQ(2, result.cid_);
    EXPECT_NE(cached_centroids + dim, result.data_);
    EXPECT_FLOAT_EQ(1.0F, result.distance_);
    EXPECT_EQ(0, controller.get_reserved_hold());
  }

  bool erased = false;
  uint64_t removed_hold = 0;
  ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(
      cache_ls_id, cache_key, cache_mgr, cache,
      cache->get_last_access_ts(), false /* failed_build */,
      erased, removed_hold));
  ASSERT_TRUE(erased);
  EXPECT_EQ(registered_hold, removed_hold);
  cache_mgr_guard.reset();
  EXPECT_EQ(resident_before, controller.get_resident_hold());
  EXPECT_EQ(misses_before + 2, controller.get_miss_count());
  EXPECT_EQ(hits_before + 1, controller.get_hit_count());
  {
    // A miss remains observable even if no cache entry is subsequently created.
    ObIvfFlatCentroidIterator lookup(first_query_allocator);
    bool hit = true;
    ASSERT_EQ(OB_SUCCESS, lookup.reader_.try_open_cache(
        query_param, cache_ls_id, cache_key, hit));
    EXPECT_FALSE(hit);
    EXPECT_EQ(misses_before + 3, controller.get_miss_count());
    EXPECT_EQ(hits_before + 1, controller.get_hit_count());
  }

  // A builder admitted before disablement retains its completed buffer for
  // the current query, without republishing it or leaking its reservation.
  {
    CentroidCacheMemoryPercentageGuard percentage;
    ASSERT_TRUE(percentage.config_.is_valid());
    percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
    ObArenaAllocator private_allocator;
    ObIvfCentroidGroupReader builder(private_allocator);
    builder.meta_ = sstable_.get_ivf_meta();
    builder.expected_center_count_ = nlist;
    ObTableIterParam build_param;
    ObTableAccessContext build_ctx;
    ObStoreCtx build_store_ctx;
    ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, build_param, build_ctx, build_store_ctx));
    uint64_t reservation = 0;
    ObIvfSSTableCentCache *building_cache = nullptr;
    ASSERT_EQ(OB_SUCCESS, builder.prepare_array_cache_build(sstable_, build_param, cluster_size,
        cache_ls_id, cache_key, reservation, building_cache));
    ASSERT_NE(nullptr, building_cache);
    ASSERT_GT(reservation, 0);
    ASSERT_EQ(OB_SUCCESS, builder.build_array_cache(sstable_, build_param, build_ctx,
        row_desc, *building_cache, reservation));
    percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 0;
    ASSERT_TRUE(builder.finish_array_cache_build(OB_SUCCESS, cache_ls_id, cache_key,
        reservation, building_cache));
    EXPECT_TRUE(building_cache->is_completed());
    EXPECT_TRUE(building_cache->is_memory_registered());
    EXPECT_FALSE(builder.shared_cache_);
    EXPECT_EQ(0, reservation);
    EXPECT_EQ(0, controller.get_reserved_hold());
    EXPECT_EQ(resident_before + building_cache->get_memory_hold(), controller.get_resident_hold());
    ObIvfCacheMgrGuard absent;
    EXPECT_EQ(OB_HASH_NOT_EXIST, service->acquire_ivf_cache_mgr_guard(cache_ls_id, cache_key, absent));
    EXPECT_EQ(0, MEMCMP(centroids.data(), building_cache->get_centroids(), nlist * dim * sizeof(float)));
    builder.reset();
    EXPECT_EQ(resident_before, controller.get_resident_hold());
  }
}

// Production source reads resolve the LS transaction table. Keep their setup
// in a separate suite so the plain SSTable/query fixtures remain unchanged.
class TestIVFInsertionCentroidSource : public TestIVFSSTableWriter
{
protected:
  // Avoid reserved LS 1, whose creation requires additional ID services.
  static constexpr int64_t SOURCE_LS_ID = 9107;
  static void SetUpTestCase()
  {
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    SERVER_STORAGE_META_SERVICE.is_started_ = true;
    int ret = MTL(ObLSService *)->get_ls(ObLSID(SOURCE_LS_ID), ls_handle_, ObLSGetMod::STORAGE_MOD);
    if (ret == OB_LS_NOT_EXIST) {
      ret = TestDmlCommon::create_ls(OB_SERVER_TENANT_ID, ObLSID(SOURCE_LS_ID), ls_handle_);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  static void TearDownTestCase() { ls_handle_.reset(); }
  static ObLSHandle ls_handle_;
};
ObLSHandle TestIVFInsertionCentroidSource::ls_handle_;

TEST_F(TestIVFInsertionCentroidSource, insertion_root_uses_unified_centroid_source)
{
  const int64_t dim = 16, nlist = 5000;
  const ObLSID ls_id(SOURCE_LS_ID);
  CentroidCacheMemoryPercentageGuard percentage;
  CentroidHgraphThresholdGuard threshold;
  ASSERT_TRUE(percentage.config_.is_valid());
  ASSERT_TRUE(threshold.config_.is_valid());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 0;
  threshold.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(ls_id, ls_mgr, false));
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ObArray<float> centers;
  for (int64_t c = 0; c < nlist; ++c) {
    float row[dim] = {}; row[0] = c + 1;
    for (float value : row) { ASSERT_EQ(OB_SUCCESS, centers.push_back(value)); }
    ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID, 0, c + 1, row, dim));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  sstable_.ivf_meta_.centroid_count_ = nlist;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObRowkeyReadInfo read_info;
  // Merge readers include hidden MVCC columns, unlike SQL output projections.
  ASSERT_EQ(OB_SUCCESS, read_info.init(allocator_, table_schema_.get_column_count(),
      table_schema_.get_rowkey_column_num(), false,
      whole_desc_.get_desc().get_full_stored_col_descs()));
  // Bind the real production source adapter. RAW capture is unrelated to this
  // center-only test; a locator supplies its initialized source-set contract.
  ObIVFMergeInputAdapter source(allocator_);
  source.index_table_id_ = IVF_TABLE_ID;
  source.index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
  source.vector_dim_ = dim;
  source.dist_type_ = VIDA_L2;
  source.ls_id_ = ls_id;
  source.read_info_ = &read_info;
  ASSERT_EQ(OB_SUCCESS, source.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  ObTableHandleV2 handle;
  ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&sstable_));
  ASSERT_EQ(OB_SUCCESS, source.source_tables_.add_table(handle));
  ASSERT_EQ(OB_SUCCESS, source.locators_.push_back(ObAsyncIVFSourceLocator(1, 0, 0, 0)));
  ASSERT_EQ(OB_SUCCESS, source.sample_locators_.push_back(ObAsyncIVFSourceLocator(1, 0, 0, 0)));
  // Only centroid access is exercised; no replay vector is read by this fixture.
  ASSERT_EQ(OB_SUCCESS, source.replay_inputs_.push_back(nullptr));
  ASSERT_EQ(OB_SUCCESS, source.replay_row_offsets_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, source.replay_row_offsets_.push_back(1));
  source.is_inited_ = true;
  ASSERT_TRUE(source.is_valid());
  class Input final : public ObIVFVectorStore {
  public:
    bool is_valid() const override { return true; }
    int64_t get_vector_count() const override { return 67; }
    int64_t get_dimension() const override { return 16; }
    int get_all_vids(ObIArray<ObIVFVectorVID> &vids) const override {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < 67; ++i) { ret = vids.push_back(101 + i * 3); }
      return ret;
    }
    int batch_get_by_vid(const ObIArray<ObIVFVectorVID> &vids, int64_t begin,
        int64_t end, ObIArray<float> &values) const override {
      int ret = OB_SUCCESS; values.reuse();
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        for (int64_t d = 0; OB_SUCC(ret) && d < 16; ++d) {
          ret = values.push_back(d == 0 ? (vids.at(i) - 101) / 3 + 1 : 0);
        }
      }
      return ret;
    }
  } input;
  ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(input));
  ObArray<ObIVFVectorVID> vids;
  ObArray<int64_t> offsets, candidates, assigned, reference;
  ASSERT_EQ(OB_SUCCESS, input.get_all_vids(vids));
  for (int64_t i = 0; i < vids.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, offsets.push_back(candidates.count()));
    if (i % 3 == 0) { ASSERT_EQ(OB_SUCCESS, candidates.push_back(i + 1)); }
  }
  ASSERT_EQ(OB_SUCCESS, offsets.push_back(candidates.count()));
  ObIVFMergePrior prior(allocator_);
  prior.dimension_ = dim; prior.vector_count_ = 10000; prior.dominant_segment_ = 0;
  ObIVFSegmentPriorInfo segment;
  segment.centroid_count_ = nlist; segment.live_vector_count_ = 9500;
  ASSERT_EQ(OB_SUCCESS, prior.segments_.push_back(segment));
  for (int64_t c = 0; c < nlist; ++c) { ASSERT_EQ(OB_SUCCESS, prior.old_cids_.push_back(c + 1)); }
  ObIVFHcaAlgorithmAdapter algorithm;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
  ASSERT_EQ(OB_SUCCESS, algorithm.insertion_assign_vectors(reader, vids, centers,
      offsets, candidates, 1, 66, reference));
  auto check_assignment = [&]() {
    algorithm.prepare_insertion_source(source, prior, VIDA_L2, 1);
    ASSERT_TRUE(algorithm.insertion_centroids_.matches(nlist, dim));
    ASSERT_EQ(OB_SUCCESS, algorithm.insertion_assign_vectors(reader, vids, centers,
        offsets, candidates, 1, 66, assigned));
    ASSERT_EQ(reference.count(), assigned.count());
    for (int64_t i = 0; i < assigned.count(); ++i) {
      EXPECT_EQ(reference.at(i), assigned.at(i));
      EXPECT_EQ((i + 1) % 3 == 0 ? i + 2 : i + 1, assigned.at(i));
    }
    EXPECT_EQ(44, algorithm.insertion_source_queries_); // Existing memberships bypass source search.
    algorithm.finish_insertion_assign();
    EXPECT_FALSE(algorithm.insertion_centroids_.matches(nlist, dim));
  };
  const ObIvfCacheMgrKey key(sstable_.get_key());
  ASSERT_NO_FATAL_FAILURE(check_assignment()); // Disabled cache: storage through the same iterator.
  {
    ObIvfCacheMgrGuard absent;
    EXPECT_EQ(OB_HASH_NOT_EXIST, service->acquire_ivf_cache_mgr_guard(ls_id, key, absent));
  }
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  ASSERT_NO_FATAL_FAILURE(check_assignment()); // Miss builds ARRAY; subsequent vectors use it.
  ObIvfCacheMgrGuard guard;
  ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(ls_id, key, guard));
  auto *mgr = guard.get_ivf_cache_mgr();
  ObIvfSSTableCentCache *cache = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, cache));
  ASSERT_TRUE(cache->is_completed());
  ASSERT_NE(nullptr, cache->get_centroids());
  EXPECT_FALSE(cache->has_hgraph_index());
  ASSERT_NO_FATAL_FAILURE(check_assignment()); // Ready ARRAY uses exactly the same source entry.
  const int64_t first_load_ts = cache->get_first_load_ts();
  const uint64_t array_hits = cache->get_hit_count();
  const int64_t array_access_ts = cache->get_last_access_ts();
  ObVectorIndexParam graph_param = mgr->get_vec_param();
  graph_param.type_ = VIAT_HGRAPH;
  graph_param.lib_ = VIAL_VSAG;
  graph_param.m_ = 16;
  graph_param.ef_construction_ = 200;
  graph_param.ef_search_ = 64;
  graph_param.extra_info_actual_size_ = 0;
  graph_param.refine_type_ = 0;
  graph_param.bq_bits_query_ = 32;
  graph_param.bq_use_fht_ = false;
  for (int64_t value : {int64_t(0), nlist + 1}) {
    threshold.config_->_disk_bbq_centroid_hgraph_threshold = value;
    ASSERT_EQ(OB_SUCCESS, cache->build_sstable_hgraph(graph_param));
    EXPECT_FALSE(cache->has_hgraph_index());
  }
  threshold.config_->_disk_bbq_centroid_hgraph_threshold = 1;
  ASSERT_EQ(OB_SUCCESS, cache->build_sstable_hgraph(graph_param));
  ASSERT_TRUE(cache->has_hgraph_index());
  EXPECT_EQ(first_load_ts, cache->get_first_load_ts());
  EXPECT_EQ(array_hits, cache->get_hit_count());
  EXPECT_EQ(array_access_ts, cache->get_last_access_ts());
  threshold.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  ASSERT_NO_FATAL_FAILURE(check_assignment()); // Existing HGraph remains queryable.
  EXPECT_EQ(array_hits + 44, cache->get_hit_count());

  // The production adapter creates independent iterator/read contexts per task.
  share::ObTenantBase *tenant = share::ObTenantEnv::get_tenant();
  std::atomic<int64_t> failures(0);
  std::vector<std::thread> workers;
  for (int64_t t = 0; t < 4; ++t) {
    workers.emplace_back([&, t] {
      share::ObTenantEnv::set_tenant(tenant);
      for (int64_t i = 0; i < 16; ++i) {
        float query[dim] = {};
        const int32_t expected = 100 + t * 100 + i;
        query[0] = expected + 1;
        int32_t found[16]; int64_t count = 0;
        if (source.search_centroids(0, query, dim, found, 16, count) != OB_SUCCESS
            || std::find(found, found + count, expected) == found + count) { ++failures; }
      }
      share::ObTenantEnv::set_tenant(nullptr);
    });
  }
  for (auto &worker : workers) { worker.join(); }
  EXPECT_EQ(0, failures.load());
  EXPECT_EQ(array_hits + 44 + 64, cache->get_hit_count());
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 0;
  ASSERT_NO_FATAL_FAILURE(check_assignment()); // Disable also bypasses an already built HGraph.
  percentage.config_->_disk_bbq_centroid_cache_mem_percentage = 80;
  bool erased = false; uint64_t removed = 0;
  ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(ls_id, key, mgr, cache,
      cache->get_last_access_ts(), false, erased, removed));
  EXPECT_TRUE(erased); // No insertion-owned graph guard survives source queries.
  guard.reset();

  // Unsupported source bindings retain exact assignment; a bound source's
  // actual read error propagates instead of being hidden by a separate fast path.
  class ErrorSource final : public ObIVFCentroidSource {
  public:
    mutable int64_t queries_ = 0;
    int error_ = OB_IO_ERROR;
    int search_centroids(int64_t, const float *query, int64_t,
        int32_t *candidates, int64_t, int64_t &count) const override {
      ++queries_; count = 0;
      if (error_ == OB_SUCCESS) { candidates[count++] = static_cast<int32_t>(query[0]) - 1; }
      return error_;
    }
  } errors;
  prior.old_cids_.at(1) = 5001;
  algorithm.prepare_insertion_source(errors, prior, VIDA_L2, 1);
  EXPECT_FALSE(algorithm.insertion_centroids_.matches(nlist, dim));
  ASSERT_EQ(OB_SUCCESS, algorithm.insertion_assign_vectors(reader, vids, centers,
      offsets, candidates, 1, 66, assigned));
  EXPECT_EQ(0, errors.queries_);
  for (int64_t i = 0; i < assigned.count(); ++i) { EXPECT_EQ(reference.at(i), assigned.at(i)); }
  prior.old_cids_.at(1) = 2;
  algorithm.prepare_insertion_source(errors, prior, VIDA_IP, 1);
  EXPECT_FALSE(algorithm.insertion_centroids_.matches(nlist, dim));
  algorithm.prepare_insertion_source(errors, prior, VIDA_L2, 1);
  EXPECT_FALSE(algorithm.insertion_centroids_.matches(nlist - 1, dim));
  EXPECT_FALSE(algorithm.insertion_centroids_.matches(nlist, dim + 1));
  for (int error : {OB_IO_ERROR, OB_CANCELED, OB_TIMEOUT}) {
    errors.error_ = error;
    EXPECT_EQ(error, algorithm.insertion_assign_vectors(reader, vids, centers,
        offsets, candidates, 1, 66, assigned));
  }
  algorithm.finish_insertion_assign();
  EXPECT_EQ(nullptr, algorithm.insertion_centroids_.source_);
  for (int64_t c = 0; c < nlist; ++c) { prior.old_cids_.at(c) = c + 3; }
  errors.error_ = OB_SUCCESS; errors.queries_ = 0;
  algorithm.prepare_insertion_source(errors, prior, VIDA_L2, 3);
  ASSERT_EQ(OB_SUCCESS, algorithm.insertion_assign_vectors(reader, vids, centers,
      offsets, candidates, 1, 66, assigned));
  EXPECT_EQ(44, errors.queries_); // The bound working indices do not include parent CIDs.
  for (int64_t i = 0; i < assigned.count(); ++i) { EXPECT_EQ(reference.at(i), assigned.at(i)); }
  algorithm.finish_insertion_assign();
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_orders_selected_centroids_and_ends_stably)
{
  const int32_t dim = 64;
  const int32_t nlist = 4;
  std::vector<float> centroids(nlist * dim, 0.0f);
  centroids[0 * dim] = 3.0f;
  centroids[1 * dim] = 1.0f;
  centroids[2 * dim] = -1.0f;
  centroids[3 * dim] = 2.0f;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t cid = 0; cid < nlist; ++cid) {
    ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
        0, cid + 1, centroids.data() + cid * dim, dim));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, write_type_rows(
      IVF_ROW_TYPE_HIGH_PRECISION_VECTOR, 200, 2, 0));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float rotated_query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, rotated_query, dim, 1);
  query_param.nprobe_ = 3;
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
      sstable_, query_param, rotated_query, allocator_, query_allocator,
      read_info, iterator.get()));
  static_assert(std::has_virtual_destructor<ObIvfCentroidIterator>::value,
      "centroid interface must destroy its concrete implementation");
  static_assert(std::is_abstract<ObIvfCentroidIterator>::value,
      "centroid queries must use a concrete search implementation");
  ObTableIterParam invalid_iter_param;
  ObIvfSSTableRowDesc invalid_row_desc;
  ASSERT_EQ(OB_INIT_TWICE, iterator->open(
      query_param, rotated_query, sstable_, invalid_iter_param,
      invalid_row_desc, scan_context_));

  const int64_t expected_cids[] = {1, 2, 3};
  const float expected_distances[] = {1.0f, 1.0f, 4.0f};
  const float *first_payload = nullptr;
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_cids); ++i) {
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
    ASSERT_EQ(expected_cids[i] + 1, result.cid_);
    ASSERT_FLOAT_EQ(expected_distances[i], result.distance_);
    ASSERT_TRUE(result.is_valid());
    ASSERT_EQ(dim, result.dim_);
    ASSERT_EQ(0, MEMCMP(centroids.data() + expected_cids[i] * dim,
        result.data_, dim * sizeof(float)));
    if (0 == i) {
      first_payload = result.data_;
    }
  }
  ASSERT_NE(nullptr, first_payload);
  ASSERT_FLOAT_EQ(1.0f, first_payload[0]);
  ObIvfCentroidInfo end_result;
  ASSERT_EQ(OB_ITER_END, iterator->get_next(end_result));
  ASSERT_EQ(-1, end_result.cid_);
  ASSERT_FALSE(end_result.is_valid());
  ASSERT_FLOAT_EQ(0.0f, end_result.distance_);
  ASSERT_EQ(OB_ITER_END, iterator->get_next(end_result));
  ASSERT_EQ(-1, end_result.cid_);
  ASSERT_FALSE(end_result.is_valid());
  ASSERT_FLOAT_EQ(0.0f, end_result.distance_);
  ASSERT_FLOAT_EQ(1.0f, first_payload[0]);
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_get_next_before_open_resets_result)
{
  ObIvfCountingAllocator query_allocator(allocator_);
  ObIvfFlatCentroidIterator iterator(query_allocator);
  ObIvfCentroidInfo result;
  result.cid_ = 42;
  result.distance_ = 7.0f;
  ASSERT_EQ(OB_NOT_INIT, iterator.get_next(result));
  ASSERT_EQ(-1, result.cid_);
  ASSERT_FALSE(result.is_valid());
  ASSERT_FLOAT_EQ(0.0f, result.distance_);
}

TEST_F(TestIVFSSTableWriter,
    centroid_iterator_returns_all_requested_noncontiguous_cids)
{
  const int32_t dim = 64;
  const int32_t nlist = 3;
  const int64_t cids[nlist] = {7, 42, 100};
  std::vector<float> centroids(nlist * dim, 0.0f);
  centroids[0 * dim] = 8.0f;
  centroids[1 * dim] = -2.0f;
  centroids[2 * dim] = 4.0f;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  for (int64_t i = 0; i < nlist; ++i) {
    ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
        0, cids[i], centroids.data() + i * dim, dim));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float rotated_query[dim] = {0};
  rotated_query[0] = 1.0f;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, rotated_query, dim, 1);
  query_param.nprobe_ = nlist;
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(sstable_, query_param, rotated_query, query_allocator, read_info, iterator.get()));
  const int64_t expected_cids[] = {42, 100, 7};
  const int64_t expected_centroid_indexes[] = {1, 2, 0};
  const float expected_distances[] = {9.0f, 9.0f, 49.0f};
  for (int64_t i = 0; i < nlist; ++i) {
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
    ASSERT_EQ(expected_cids[i], result.cid_);
    ASSERT_FLOAT_EQ(expected_distances[i], result.distance_);
    ASSERT_FLOAT_EQ(centroids[expected_centroid_indexes[i] * dim],
        result.data_[0]);
  }
}

TEST_F(TestIVFSSTableWriter,
    centroid_iterator_supports_known_metrics_and_propagates_validation_errors)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  centroid[0] = 1.0f;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(
      dim, nullptr, 1, share::VIDA_COS));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float rotated_query[dim] = {0};
  rotated_query[0] = 1.0f;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, rotated_query, dim, 1);
  ObArenaAllocator query_allocator;
  {
    ObIvfCentroidIteratorGuard iterator(allocator_);
    ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
        sstable_, query_param, rotated_query, allocator_, query_allocator,
        read_info, iterator.get()));
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
    ASSERT_EQ(1, result.cid_);
    ASSERT_TRUE(std::isfinite(result.distance_));
  }

  sstable_.ivf_meta_.dist_type_ = share::VIDA_IP;
  {
    ObIvfCentroidIteratorGuard iterator(allocator_);
    ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
        sstable_, query_param, rotated_query, allocator_, query_allocator,
        read_info, iterator.get()));
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
    ASSERT_EQ(1, result.cid_);
    ASSERT_FLOAT_EQ(0.0f, result.distance_);
  }

  ObIvfCentroidIterator *iterator = reinterpret_cast<ObIvfCentroidIterator *>(1);
  sstable_.ivf_meta_.dist_type_ = share::VIDA_MAX;
  iterator = reinterpret_cast<ObIvfCentroidIterator *>(1);
  ASSERT_EQ(OB_NOT_INIT, create_centroid_iterator_with_reader(
      sstable_, query_param, rotated_query, allocator_, query_allocator,
      read_info, iterator));
  ASSERT_EQ(nullptr, iterator);
  sstable_.ivf_meta_.dist_type_ = share::VIDA_L2;
  rotated_query[dim - 1] = std::numeric_limits<float>::infinity();
  {
    ObIvfCentroidIteratorGuard iterator_guard(allocator_);
    ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
        sstable_, query_param, rotated_query, allocator_, query_allocator,
        read_info, iterator_guard.get()));
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator_guard->get_next(result));
    ASSERT_FALSE(std::isfinite(result.distance_));
  }
  rotated_query[dim - 1] = 0.0f;
  query_param.nprobe_ = 2;
  {
    ObIvfCentroidIteratorGuard iterator_guard(allocator_);
    ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
        sstable_, query_param, rotated_query, allocator_, query_allocator,
        read_info, iterator_guard.get()));
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator_guard->get_next(result));
    ASSERT_EQ(OB_ITER_END, iterator_guard->get_next(result));
  }
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_keeps_zero_and_non_finite_cosine_query_distances)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  centroid[0] = 1.0f;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(
      dim, nullptr, 1, share::VIDA_COS));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float rotated_query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, rotated_query, dim, 1);
  ObArenaAllocator query_allocator;
  {
    ObIvfCentroidIteratorGuard iterator(allocator_);
    ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
        sstable_, query_param, rotated_query, query_allocator, read_info, iterator.get()));
    ObIvfCentroidInfo result;
    ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
    EXPECT_FLOAT_EQ(1.0F, result.distance_);
  }

  rotated_query[0] = 1.0f;
  rotated_query[dim - 1] = std::numeric_limits<float>::infinity();
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
      sstable_, query_param, rotated_query, query_allocator, read_info, iterator.get()));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
  EXPECT_TRUE(std::isnan(result.distance_));
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_rejects_wrong_payload_size)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim - 1, 0.0f);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), centroid.size()));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIterator *iterator = reinterpret_cast<ObIvfCentroidIterator *>(1);
  ASSERT_EQ(OB_INVALID_DATA, create_centroid_iterator_with_reader(sstable_, query_param, query, query_allocator, read_info, iterator));
  ASSERT_EQ(nullptr, iterator);
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_keeps_non_finite_l2_distance)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  centroid[dim / 2] = std::numeric_limits<float>::quiet_NaN();
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0f;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(sstable_, query_param, query, query_allocator, read_info, iterator.get()));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
  ASSERT_FALSE(std::isfinite(result.distance_));
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_keeps_non_finite_cosine_distance)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  centroid[dim / 2] = std::numeric_limits<float>::infinity();
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(
      dim, nullptr, 1, share::VIDA_COS));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0f;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
      sstable_, query_param, query, query_allocator, read_info, iterator.get()));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
  EXPECT_TRUE(std::isnan(result.distance_));
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_keeps_non_finite_ip_distance)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  centroid[dim / 2] = std::numeric_limits<float>::infinity();
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(
      dim, nullptr, 1, share::VIDA_IP));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0f;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(sstable_, query_param, query, query_allocator, read_info, iterator.get()));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
  ASSERT_FALSE(std::isfinite(result.distance_));
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_zero_norm_cosine_uses_rabitq_distance)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(
      dim, nullptr, 1, share::VIDA_COS));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  query[0] = 1.0f;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
      sstable_, query_param, query, query_allocator, read_info, iterator.get()));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
  EXPECT_EQ(1, result.cid_);
  EXPECT_FLOAT_EQ(1.0F, result.distance_);
  EXPECT_EQ(OB_ITER_END, iterator->get_next(result));
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_rejects_negative_cid)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, -1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIterator *iterator = reinterpret_cast<ObIvfCentroidIterator *>(1);
  ASSERT_EQ(OB_INVALID_DATA, create_centroid_iterator_with_reader(sstable_, query_param, query, query_allocator, read_info, iterator));
  ASSERT_EQ(nullptr, iterator);
}

TEST_F(TestIVFSSTableWriter,
    centroid_iterator_returns_available_rows_when_nprobe_is_larger)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.nprobe_ = 2;
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(sstable_, query_param, query, query_allocator, read_info, iterator.get()));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
  ASSERT_EQ(1, result.cid_);
  ASSERT_EQ(OB_ITER_END, iterator->get_next(result));
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_rejects_bad_centroid_vid)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 0, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIterator *iterator = reinterpret_cast<ObIvfCentroidIterator *>(1);
  ASSERT_EQ(OB_INVALID_DATA, create_centroid_iterator_with_reader(sstable_, query_param, query, query_allocator, read_info, iterator));
  ASSERT_EQ(nullptr, iterator);
}

TEST_F(TestIVFSSTableWriter, centroid_iterator_limits_results_to_nprobe)
{
  const int32_t dim = 64;
  std::vector<float> centroid(dim, 0.0f);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 1, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, write_float_vec_row(writer, IVF_ROW_TYPE_CENTROID,
      0, 2, centroid.data(), dim));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIteratorGuard iterator(allocator_);
  ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(sstable_, query_param, query, query_allocator, read_info, iterator.get()));
  ObIvfCentroidInfo result;
  ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
  ASSERT_EQ(OB_ITER_END, iterator->get_next(result));
}

TEST_F(TestIVFSSTableWriter, disk_bbq_searcher_centroid_iterator_query_allocation_failures_clean_up)
{
  const int32_t dim = 64;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);

  ObIvfCountingAllocator failed_object_allocator(allocator_);
  failed_object_allocator.set_fail_on_alloc(1);
  ObArenaAllocator query_allocator;
  ObIvfCentroidIterator *iterator = reinterpret_cast<ObIvfCentroidIterator *>(1);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, create_centroid_iterator_with_reader(
      sstable_, query_param, query, failed_object_allocator,
      query_allocator, read_info, iterator));
  ASSERT_EQ(nullptr, iterator);
  ASSERT_EQ(0, failed_object_allocator.live_allocation_count());

  // Only the iterator object remains live in the object allocator after the
  // Reader closes its transient lower iterator and contexts. Final selected
  // results, centroid payloads and the standalone query context persist in
  // the query allocator. Searcher-backed iterators borrow its existing context.
  int64_t query_alloc_count = 0;
  ObIvfCountingAllocator successful_object_allocator(
      *lib::ObMallocAllocator::get_instance());
  ObIvfCountingAllocator successful_query_allocator(
      *lib::ObMallocAllocator::get_instance());
  {
    ObIvfCentroidIteratorGuard successful_iterator(successful_object_allocator);
    ASSERT_EQ(OB_SUCCESS, create_centroid_iterator_with_reader(
        sstable_, query_param, query, successful_object_allocator,
        successful_query_allocator, read_info, successful_iterator.get()));
    ASSERT_NE(nullptr, successful_iterator.get());
    query_alloc_count = successful_query_allocator.alloc_count_;
    ASSERT_EQ(1, successful_object_allocator.live_allocation_count());
    ASSERT_GT(successful_object_allocator.alloc_count_, 1);
    ASSERT_EQ(successful_object_allocator.alloc_count_ - 1,
        successful_object_allocator.free_count_);
    ASSERT_EQ(3, query_alloc_count);
    ASSERT_EQ(3, successful_query_allocator.live_allocation_count());
  }
  ASSERT_EQ(0, successful_object_allocator.live_allocation_count());
  ASSERT_EQ(0, successful_query_allocator.live_allocation_count());

  for (int64_t fail_on = 1; fail_on <= query_alloc_count; ++fail_on) {
    ObIvfCountingAllocator object_allocator(allocator_);
    ObIvfCountingAllocator failed_query_allocator(allocator_);
    failed_query_allocator.set_fail_on_alloc(fail_on);
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, create_centroid_iterator_with_reader(
        sstable_, query_param, query, object_allocator,
        failed_query_allocator, read_info, iterator)) << "fail_on=" << fail_on;
    ASSERT_EQ(nullptr, iterator) << "fail_on=" << fail_on;
    ASSERT_EQ(object_allocator.alloc_count_, object_allocator.free_count_)
        << "fail_on=" << fail_on;
    ASSERT_EQ(0, object_allocator.live_allocation_count()) << "fail_on=" << fail_on;
    ASSERT_EQ(0, failed_query_allocator.live_allocation_count()) << "fail_on=" << fail_on;
  }
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_soar_duplicates_do_not_consume_k)
{
  for (const int64_t k : {1, 4, 10, 20, 32, 65}) {
    for (const bool reverse : {false, true}) {
      ObIvfTopKCollector collector;
      ObIvfKnnSearchResult result;
      ASSERT_EQ(OB_SUCCESS, collector.init(k, allocator_));
      for (int64_t i = 0; i < k + 3; ++i) {
        const int64_t vid = reverse ? k + 2 - i : i;
        ASSERT_EQ(OB_SUCCESS, collector.push(0, vid, static_cast<float>(vid)));
        ASSERT_EQ(OB_SUCCESS, collector.push(1, vid, static_cast<float>(vid)));
      }
      ASSERT_EQ(OB_SUCCESS, collector.finish(result));
      ASSERT_EQ(k, result.count());
      for (int64_t i = 0; i < k; ++i) {
        ASSERT_EQ(i, result.at(i).vid_) << "k=" << k << " reverse=" << reverse;
        ASSERT_FLOAT_EQ(static_cast<float>(i), result.at(i).distance_);
      }
    }
  }
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_duplicate_does_not_tighten_threshold)
{
  ObIvfTopKCollector collector;
  float threshold = 0.0F;
  ASSERT_EQ(OB_SUCCESS, collector.init(4, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 10, 1.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(1, 10, 1.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 20, 2.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(1, 20, 2.0F));
  ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
  ASSERT_FLOAT_EQ(std::numeric_limits<float>::max(), threshold);
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 30, 3.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 40, 4.0F));
  ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
  ASSERT_FLOAT_EQ(4.0F, threshold);
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_improves_duplicate_and_allows_reentry)
{
  ObIvfTopKCollector collector;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, collector.init(3, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 10, 9.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 20, 8.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 30, 7.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(1, 20, 1.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(2, 20, 10.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 40, 6.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(1, 10, 2.0F));
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(3, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  ASSERT_EQ(10, result.at(1).vid_);
  ASSERT_EQ(40, result.at(2).vid_);
  ASSERT_FLOAT_EQ(1.0F, result.at(0).distance_);
  ASSERT_FLOAT_EQ(2.0F, result.at(1).distance_);
  ASSERT_FLOAT_EQ(6.0F, result.at(2).distance_);
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_orders_distance_then_vid)
{
  ObIvfTopKCollector collector;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, collector.init(3, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.push(2, 30, 3.0f));
  ASSERT_EQ(OB_SUCCESS, collector.push(1, 20, 2.0f));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 40, 2.0f));
  ASSERT_EQ(OB_SUCCESS, collector.push(9, 10, 1.0f));
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(3, result.count());
  ASSERT_FLOAT_EQ(1.0f, result.at(0).distance_);
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_FLOAT_EQ(2.0f, result.at(1).distance_);
  ASSERT_EQ(20, result.at(1).vid_);
  ASSERT_FLOAT_EQ(2.0f, result.at(2).distance_);
  ASSERT_EQ(40, result.at(2).vid_);
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_exposes_dynamic_distance_threshold)
{
  ObIvfTopKCollector collector;
  float threshold = 0.0F;
  ASSERT_EQ(OB_NOT_INIT, collector.get_distance_threshold(threshold));
  ASSERT_EQ(OB_SUCCESS, collector.init(2, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
  ASSERT_FLOAT_EQ(std::numeric_limits<float>::max(), threshold);
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 10, 3.0F));
  ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
  ASSERT_FLOAT_EQ(std::numeric_limits<float>::max(), threshold);
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 20, 2.0F));
  ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
  ASSERT_FLOAT_EQ(3.0F, threshold);
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 30, 1.0F));
  ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
  ASSERT_FLOAT_EQ(2.0F, threshold);
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_does_not_track_discarded_candidates)
{
  ObIvfCountingAllocator counting_allocator(allocator_);
  ObIvfTopKCollector collector;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, collector.init(3, counting_allocator));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 0, 0.0f));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 1, 1.0f));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 2, 2.0f));
  const int64_t allocation_count_after_fill = counting_allocator.alloc_count_;
  for (int64_t vid = 3; vid < 1003; ++vid) {
    ASSERT_EQ(OB_SUCCESS, collector.push(0, vid, static_cast<float>(vid)));
  }
  ASSERT_EQ(allocation_count_after_fill, counting_allocator.alloc_count_);
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(3, result.count());
  ASSERT_EQ(0, result.at(0).vid_);
  ASSERT_EQ(1, result.at(1).vid_);
  ASSERT_EQ(2, result.at(2).vid_);
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_deduplicates_before_tightening_threshold)
{
  ObIvfTopKCollector collector;
  ObIvfKnnSearchResult result;
  float threshold = 0.0F;
  ASSERT_FALSE(collector.contains(10));
  ASSERT_EQ(OB_SUCCESS, collector.init(3, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 10, 1.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(1, 10, 1.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(2, 10, 2.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 20, 2.0F));
  ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
  ASSERT_FLOAT_EQ(std::numeric_limits<float>::max(), threshold);
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 30, 3.0F));
  ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
  ASSERT_FLOAT_EQ(3.0F, threshold);
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(3, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_EQ(0, result.at(0).cid_);
  ASSERT_EQ(20, result.at(1).vid_);
  ASSERT_EQ(30, result.at(2).vid_);
  ASSERT_TRUE(collector.contains(10));
  ASSERT_FALSE(collector.contains(-1));
  collector.reset();
  ASSERT_FALSE(collector.contains(10));
  ASSERT_EQ(OB_SUCCESS, collector.init(1, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.push(1, 10, 4.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(2, 20, 3.0F));
  ASSERT_FALSE(collector.contains(10));
  ASSERT_EQ(OB_SUCCESS, collector.push(3, 20, 2.0F));
  ASSERT_EQ(OB_SUCCESS, collector.push(4, 10, 1.0F));
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_EQ(4, result.at(0).cid_);
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_matches_unique_best_distance_reference)
{
  // Repeated VIDs, ties, root/non-root improvements and re-entry after eviction.
  for (const int64_t k : {1, 3, 17, 64}) {
    ObIvfCountingAllocator counting_allocator(allocator_);
    ObIvfTopKCollector collector;
    ASSERT_EQ(OB_SUCCESS, collector.init(k, counting_allocator));
    std::unordered_map<int64_t, ObIvfKnnEntry> best;
    uint64_t state = 42;
    for (int64_t i = 0; i < 2000; ++i) {
      state = state * 6364136223846793005ULL + 1;
      const int64_t vid = i < k ? i : static_cast<int64_t>((state >> 32) % 113);
      const float distance = static_cast<float>((state >> 16) % 97);
      const int64_t alloc_before = counting_allocator.alloc_count_;
      ASSERT_EQ(OB_SUCCESS, collector.push(i, vid, distance));
      if (i >= k) {
        ASSERT_EQ(alloc_before, counting_allocator.alloc_count_);
      }
      if (best.end() == best.find(vid) || distance < best[vid].distance_) {
        best[vid].cid_ = i;
        best[vid].vid_ = vid;
        best[vid].distance_ = distance;
      }
      std::vector<ObIvfKnnEntry> expected;
      for (const auto &entry : best) {
        expected.push_back(entry.second);
      }
      std::sort(expected.begin(), expected.end(), [](const ObIvfKnnEntry &lhs,
          const ObIvfKnnEntry &rhs) {
        return lhs.distance_ < rhs.distance_
            || (lhs.distance_ == rhs.distance_ && lhs.vid_ < rhs.vid_);
      });
      expected.resize(std::min(expected.size(), static_cast<size_t>(k)));
      ObIvfKnnSearchResult result;
      ASSERT_EQ(OB_SUCCESS, collector.finish(result));
      ASSERT_EQ(expected.size(), result.count());
      for (int64_t j = 0; j < result.count(); ++j) {
        ASSERT_EQ(expected[j].vid_, result.at(j).vid_);
        ASSERT_FLOAT_EQ(expected[j].distance_, result.at(j).distance_);
        ASSERT_TRUE(collector.contains(expected[j].vid_));
      }
      for (int64_t candidate_vid = 0; candidate_vid < 113; ++candidate_vid) {
        const bool retained = std::any_of(expected.begin(), expected.end(),
            [candidate_vid](const ObIvfKnnEntry &entry) { return entry.vid_ == candidate_vid; });
        ASSERT_EQ(retained, collector.contains(candidate_vid));
      }
      float threshold = 0.0F;
      ASSERT_EQ(OB_SUCCESS, collector.get_distance_threshold(threshold));
      ASSERT_FLOAT_EQ(expected.size() < static_cast<size_t>(k)
          ? std::numeric_limits<float>::max() : expected.back().distance_, threshold);
    }
    collector.reset();
    ASSERT_EQ(0, counting_allocator.live_allocation_count());
  }
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_replaces_colliding_head_middle_and_tail)
{
  for (int64_t worst = 0; worst < 3; ++worst) {
    ObIvfTopKCollector collector;
    ASSERT_EQ(OB_SUCCESS, collector.init(3, allocator_));
    int64_t vids[4];
    int64_t count = 0;
    for (int64_t vid = 0; count < 4; ++vid) {
      if (collector.find_node_link(vid) == collector.find_node_link(0)) {
        vids[count++] = vid;
      }
    }
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(OB_SUCCESS, collector.push(0, vids[i], i == worst ? 10.0F : 5.0F));
    }
    ASSERT_EQ(OB_SUCCESS, collector.push(1, vids[3], 1.0F));
    ASSERT_FALSE(collector.contains(vids[worst]));
    ASSERT_TRUE(collector.contains(vids[3]));
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_EQ(i != worst, collector.contains(vids[i]));
    }
    // An evicted VID can re-enter the same collision chain.
    ASSERT_EQ(OB_SUCCESS, collector.push(2, vids[worst], 0.0F));
    ASSERT_TRUE(collector.contains(vids[worst]));
    ObIvfKnnSearchResult result;
    ASSERT_EQ(OB_SUCCESS, collector.finish(result));
    ASSERT_EQ(3, result.count());
    ASSERT_EQ(vids[worst], result.at(0).vid_);
    ASSERT_EQ(vids[3], result.at(1).vid_);
    ASSERT_NE(result.at(0).vid_, result.at(2).vid_);
    ASSERT_NE(result.at(1).vid_, result.at(2).vid_);
  }
}

TEST_F(TestIVFSSTableWriter, DISABLED_ivf_topk_collector_push_benchmark)
{
  constexpr int64_t candidate_count = 4096;
  constexpr int64_t query_count = 256;
  for (const int64_t k : {10, 100, 1000}) {
    for (const char *workload : {"reject", "replace", "duplicate", "mixed"}) {
      std::vector<ObIvfKnnEntry> candidates(candidate_count);
      uint64_t state = 42;
      for (int64_t i = 0; i < candidate_count; ++i) {
        ObIvfKnnEntry &item = candidates[i];
        item.vid_ = i;
        if (0 == strcmp(workload, "reject")) {
          item.distance_ = static_cast<float>(k + i);
        } else if (0 == strcmp(workload, "mixed")) {
          state = state * 6364136223846793005ULL + 1;
          item.vid_ = static_cast<int64_t>((state >> 32) % (4 * k));
          item.distance_ = static_cast<float>((state >> 16) % candidate_count);
        } else {
          item.distance_ = -static_cast<float>(i + 1);
          if (0 == strcmp(workload, "duplicate")) {
            item.vid_ %= k;
          }
        }
      }
      int64_t elapsed_ns = 0;
      uint64_t checksum = 0;
      for (int64_t query = -8; query < query_count; ++query) {
        ObIvfTopKCollector collector;
        ASSERT_EQ(OB_SUCCESS, collector.init(k, allocator_));
        for (int64_t i = 0; i < k; ++i) {
          ASSERT_EQ(OB_SUCCESS, collector.push(0, candidate_count + i, static_cast<float>(i)));
        }
        int ret = OB_SUCCESS;
        const auto begin = std::chrono::steady_clock::now();
        for (const auto &item : candidates) {
          ret |= collector.push(item.cid_, item.vid_, item.distance_);
        }
        const auto end = std::chrono::steady_clock::now();
        ASSERT_EQ(OB_SUCCESS, ret);
        ObIvfKnnSearchResult result;
        ASSERT_EQ(OB_SUCCESS, collector.finish(result));
        ASSERT_EQ(k, result.count());
        if (query >= 0) {
          elapsed_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
          for (int64_t i = 0; i < result.count(); ++i) {
            checksum = checksum * 31 + result.at(i).vid_;
            checksum = checksum * 31 + static_cast<int64_t>(result.at(i).distance_);
          }
        }
      }
      fprintf(stdout, "COLLECTOR_BENCH k=%ld workload=%s ns_per_push=%.3f checksum=%lu\n",
          k, workload, static_cast<double>(elapsed_ns) / (query_count * candidate_count), checksum);
    }
  }
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_allocation_failure_leaves_lookup_reusable)
{
  ObIvfCountingAllocator counting_allocator(allocator_);
  ObIvfTopKCollector collector;
  for (int64_t fail_on = 1; fail_on <= 2; ++fail_on) {
    counting_allocator.set_fail_on_alloc(counting_allocator.alloc_count_ + fail_on);
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, collector.init(32, counting_allocator));
    ASSERT_FALSE(collector.contains(0));
    ASSERT_EQ(0, counting_allocator.live_allocation_count());
  }
  counting_allocator.set_fail_on_alloc(0);
  ASSERT_EQ(OB_SUCCESS, collector.init(32, counting_allocator));
  for (int64_t vid = 0; vid < 16; ++vid) {
    ASSERT_EQ(OB_SUCCESS, collector.push(0, vid, static_cast<float>(vid)));
  }
  counting_allocator.set_fail_on_alloc(counting_allocator.alloc_count_ + 1);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, collector.push(0, 16, 16.0F));
  ASSERT_FALSE(collector.contains(16));
  counting_allocator.set_fail_on_alloc(0);
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 16, 16.0F));
  ASSERT_TRUE(collector.contains(16));
  collector.reset();
  ASSERT_EQ(0, counting_allocator.live_allocation_count());
  ASSERT_EQ(OB_INVALID_ARGUMENT, collector.init(INT64_MAX, counting_allocator));
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_empty_finish_is_safe)
{
  ObIvfTopKCollector collector;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_SUCCESS, collector.init(3, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(0, result.count());
}

TEST_F(TestIVFSSTable, async_ivf_topk_merge_bounds_capacity_by_candidates)
{
  ObIvfCountingAllocator alloc(allocator_);
  ObSEArray<sql::ObDASVectorCandidate, 16> candidates;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_SUCCESS, sql::ObDASAsyncIVFTopKMergeOp::collect_topk(
      candidates, INT64_MAX, alloc, result));
  EXPECT_EQ(0, result.count());
  EXPECT_EQ(0, alloc.alloc_count_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, sql::ObDASAsyncIVFTopKMergeOp::collect_topk(
      candidates, 0, alloc, result));
  for (int64_t i = 0; i < 6; ++i) {
    sql::ObDASVectorCandidate candidate;
    candidate.entry_.vid_ = i % 3;
    candidate.entry_.distance_ = static_cast<float>(6 - i);
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(candidate));
  }
  ASSERT_EQ(OB_SUCCESS, sql::ObDASAsyncIVFTopKMergeOp::collect_topk(
      candidates, INT64_MAX, alloc, result));
  ASSERT_EQ(3, result.count());
  EXPECT_EQ(2, result.at(0).vid_);
  EXPECT_FLOAT_EQ(1.0F, result.at(0).distance_);
  EXPECT_LT(alloc.last_alloc_size_, 64 * 1024);
  EXPECT_EQ(0, alloc.live_allocation_count());
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_keeps_smaller_vid_on_equal_distance_boundary)
{
  ObIvfTopKCollector collector;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, collector.init(2, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 10, 1.0f));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 20, 1.0f));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 5, 1.0f));
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(2, result.count());
  ASSERT_EQ(5, result.at(0).vid_);
  ASSERT_EQ(10, result.at(1).vid_);
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_rejects_invalid_candidates_without_mutation)
{
  ObIvfTopKCollector collector;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, collector.init(1, allocator_));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 10, 1.0f));
  ASSERT_EQ(OB_INVALID_DATA, collector.push(-1, 11, 0.0f));
  ASSERT_EQ(OB_INVALID_DATA, collector.push(0, -1, 0.0f));
  ASSERT_EQ(OB_NUMERIC_OVERFLOW, collector.push(0, 12, NAN));
  ASSERT_EQ(OB_NUMERIC_OVERFLOW, collector.push(0, 13, INFINITY));
  ASSERT_EQ(OB_SUCCESS, collector.finish(result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
}

TEST_F(TestIVFSSTableWriter, ivf_topk_collector_finish_allocation_failure_clears_result)
{
  ObIvfCountingAllocator failing_allocator(allocator_);
  ObIvfTopKCollector collector;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_SUCCESS, collector.init(1, failing_allocator));
  ASSERT_EQ(OB_SUCCESS, collector.push(0, 10, 1.0f));
  failing_allocator.set_fail_on_alloc(failing_allocator.alloc_count_ + 1);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, collector.finish(result));
  ASSERT_EQ(0, result.count());
}

TEST_F(TestIVFSSTableWriter, ivf_searcher_lifecycle_derived_error_allows_retry)
{
  const int32_t dim = 8;
  const int64_t cid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_zero_centroids(0, &cid, 1, dim));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[dim] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  TestIVFSSTableSearcher searcher(allocator_, OB_ERR_UNEXPECTED);
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ASSERT_EQ(OB_SUCCESS,
      searcher.open_for_test(scan_param_, scan_context_, sstable_));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      searcher.knn_search(query_param, result));
  ASSERT_EQ(1, searcher.calls_);
  ASSERT_EQ(0, result.count());
  searcher.do_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS,
      searcher.knn_search(query_param, result));
  ASSERT_EQ(2, searcher.calls_);
  ASSERT_EQ(1, result.count());
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_low_precision_vector_scanner_low_precision_scan_spans_multiple_macro_blocks)
{
  const int32_t dim = 384;
  const int32_t nlist = 1;
  const int32_t nbits = 1;
  const int64_t vector_count = 80000;
  quantization::ObQuantizationParams quantization_params = {};
  int64_t code_size = 0;
  float query[dim] = {0};
  float centroid[dim] = {0};
  char flip[4 * ((dim + CHAR_BIT - 1) / CHAR_BIT)] = {0};
  quantization_params.dim_ = dim;
  quantization_params.bits_ = static_cast<int8_t>(nbits);
  ASSERT_EQ(OB_SUCCESS,
      quantization::ObRaBitQQuantizer::calc_code_size(
          quantization_params, code_size));
  ASSERT_LE(code_size, INT32_MAX);
  const int32_t code_len = static_cast<int32_t>(code_size);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS,
      write_low_precision_vector_rows(100, vector_count, 0, code_len));
  ASSERT_EQ(OB_SUCCESS,
      close_builder_and_init_ivf_sstable(dim, flip, nbits));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_GE(sstable_.get_data_macro_block_count(), 3);

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));
  const auto original_scan_order = scan_context_.query_flag_.scan_order_;
  ASSERT_EQ(OB_SUCCESS,
      scanner.open_scan(0, query_param.read_stats_, query, centroid));
  ASSERT_EQ(OB_STATE_NOT_MATCH,
      scanner.open_scan(0, query_param.read_stats_, query, centroid));
  int64_t row_count = 0;
  bool is_iter_end = false;
  while (!is_iter_end) {
    ObDistBBQFastScanBatch batch;
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
    for (int64_t i = 0; i < batch.count(); ++i) {
      ASSERT_EQ(row_count, batch.at(i).vid_);
      ++row_count;
    }
  }
  scanner.close_scan();
  ASSERT_EQ(original_scan_order, scan_context_.query_flag_.scan_order_);
  ASSERT_EQ(vector_count, row_count);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_low_precision_vector_scanner_low_precision_scan_uses_single_cid_prefix)
{
  const int32_t dim = 64;
  const int32_t nlist = 3;
  const int32_t nbits = 1;
  const int64_t cids[] = {0, 0, 1, 1, 2, 2};
  const int64_t vids[] = {10, 11, 20, 21, 30, 31};
  float vectors[ARRAYSIZEOF(cids)][dim] = {{0}};
  float centroids[nlist][dim] = {{0}};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < ARRAYSIZEOF(cids); ++i) {
    vectors[i][i] = static_cast<float>(i + 1) * 0.25F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(&vectors[0][0], cids, vids,
      ARRAYSIZEOF(cids), &centroids[0][0], nlist, dim, flip, nbits));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));
  const auto original_scan_order = scan_context_.query_flag_.scan_order_;

  const int64_t requested_cid = 1;
  const int64_t expected_vids[] = {20, 21};
  ASSERT_EQ(OB_SUCCESS,
      scanner.open_scan(requested_cid + 1, query_param.read_stats_,
          query, centroids[requested_cid]));
  int64_t row_count = 0;
  bool is_iter_end = false;
  while (!is_iter_end) {
    ObDistBBQFastScanBatch batch;
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
    for (int64_t i = 0; i < batch.count(); ++i) {
      ASSERT_LT(row_count, ARRAYSIZEOF(expected_vids));
      ASSERT_EQ(expected_vids[row_count], batch.at(i).vid_);
      ++row_count;
    }
  }
  ASSERT_EQ(ARRAYSIZEOF(expected_vids), row_count);
  scanner.close_scan();
  ASSERT_EQ(original_scan_order, scan_context_.query_flag_.scan_order_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_fast_scanner_computes_approximate_distances_in_32_row_batches)
{
  const int32_t dim = 64;
  const int64_t vector_count = ObDistBBQFastScanBatch::CAPACITY + 1;
  float vectors[vector_count][dim] = {{0}};
  int64_t cids[vector_count] = {0};
  int64_t vids[vector_count] = {0};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < vector_count; ++i) {
    vectors[i][i % dim] = static_cast<float>(i + 1) * 0.01F;
    vids[i] = 100 + i;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(&vectors[0][0], cids, vids,
      vector_count, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(
      1, query_param.read_stats_, query, centroid));

  ObDistBBQFastScanBatch batch;
  bool is_iter_end = false;
  ASSERT_EQ(OB_SUCCESS,
      scanner.get_next_batch(batch, is_iter_end));
  ASSERT_FALSE(is_iter_end);
  ASSERT_EQ(ObDistBBQFastScanBatch::CAPACITY, batch.count());
  for (int64_t i = 0; i < batch.count(); ++i) {
    ASSERT_EQ(vids[i], batch.at(i).vid_);
  }
  ASSERT_EQ(OB_SUCCESS,
      scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(1, batch.count());
  ASSERT_EQ(vids[vector_count - 1], batch.at(0).vid_);
  scanner.close_scan();
}

TEST_F(TestIVFSSTableWriter, disk_bbq_fast_scanner_prefilter_keeps_posting_range)
{
  const int32_t dim = 64;
  const int64_t count = 2 * ObDistBBQFastScanBatch::CAPACITY + 6;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0), vids(count);
  float centroid[dim] = {0};
  float query[dim] = {0};
  for (int64_t i = 0; i < count; ++i) {
    vids[i] = 100 + i;
    vectors[i * dim + i % dim] = 0.1F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroid, 1, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(135));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(137));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.nprobe_ = 1;
  query_param.pre_filter_ = &filter;
  ObIvfSSTableReadStats search_stats;
  query_param.read_stats_ = &search_stats;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, query_param, result));
  ASSERT_EQ(2, result.count());
  ASSERT_TRUE((result.at(0).vid_ == 135 && result.at(1).vid_ == 137)
      || (result.at(0).vid_ == 137 && result.at(1).vid_ == 135));
  ASSERT_LE(search_stats.low_precision_vector_count_, count);

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));

  ObIvfSSTableReadStats stats;
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid, &filter));
  ASSERT_TRUE(scanner.posting_start_datums_[IVF_ROWKEY_COL_VID].is_min());
  ASSERT_TRUE(scanner.posting_end_datums_[IVF_ROWKEY_COL_VID].is_max());
  ObDistBBQFastScanBatch batch;
  bool is_iter_end = false;
  std::vector<int64_t> scanned_vids;
  while (!is_iter_end) {
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
    for (int64_t i = 0; i < batch.count(); ++i) {
      scanned_vids.push_back(batch.at(i).vid_);
    }
  }
  ASSERT_NE(scanned_vids.end(), std::find(scanned_vids.begin(), scanned_vids.end(), 135));
  ASSERT_NE(scanned_vids.end(), std::find(scanned_vids.begin(), scanned_vids.end(), 137));
  ASSERT_LE(stats.low_precision_vector_count_, count);
  scanner.close_scan();

  allowed.set_empty();
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid, &filter));
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(0, batch.count());
  scanner.close_scan();
}

class TestIVFFastScannerRowStore
    : public TestIVFSSTableWriter,
      public ::testing::WithParamInterface<ObRowStoreType>
{};

INSTANTIATE_TEST_CASE_P(
    SupportedRowStores,
    TestIVFFastScannerRowStore,
    ::testing::Values(
        FLAT_ROW_STORE,
        ENCODING_ROW_STORE,
        SELECTIVE_ENCODING_ROW_STORE,
        CS_ENCODING_ROW_STORE));

TEST_P(TestIVFFastScannerRowStore,
    disk_bbq_fast_scanner_uses_fastscan_cache_for_supported_row_store)
{
  const ObRowStoreType row_store_type = GetParam();
  const int32_t dim = 64;
  const int64_t vector_count = 3;
  float vectors[vector_count][dim] = {{0}};
  int64_t cids[vector_count] = {0};
  int64_t vids[vector_count] = {101, 102, 103};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < vector_count; ++i) {
    vectors[i][i] = static_cast<float>(i + 1) * 0.1F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, row_store_type));
  ASSERT_EQ(row_store_type, table_schema_.get_row_store_type());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(&vectors[0][0], cids, vids,
      vector_count, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_,
      access_param, read_info, row_desc));
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(
      1, query_param.read_stats_, query, centroid));

  EXPECT_TRUE(scanner.fastscan_computer_.is_inited_);
  EXPECT_FALSE(scanner.computer_.is_inited());
  bool is_iter_end = false;
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(is_iter_end));
  ASSERT_FALSE(is_iter_end);
  ASSERT_EQ(nullptr, scanner.micro_reader_);
  ASSERT_NE(nullptr, scanner.fastscan_header_);
  ASSERT_TRUE(scanner.fastscan_header_->is_valid(
      scanner.fastscan_header_->total_size_));
  ASSERT_EQ(101, scanner.fastscan_header_->min_vid_);
  ASSERT_EQ(103, scanner.fastscan_header_->max_vid_);

  ObDistBBQFastScanBatch batch;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(vector_count, batch.count());
  std::vector<int64_t> actual_vids;
  for (int64_t i = 0; i < batch.count(); ++i) {
    actual_vids.push_back(batch.at(i).vid_);
    ASSERT_TRUE(std::isfinite(batch.at(i).distance_));
  }
  std::sort(actual_vids.begin(), actual_vids.end());
  ASSERT_EQ(std::vector<int64_t>(vids, vids + vector_count), actual_vids);
  EXPECT_FALSE(scanner.row_computer_ready_);
  EXPECT_FALSE(scanner.computer_.is_inited());
  scanner.close_scan();

  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(
      1, query_param.read_stats_, query, centroid));
  ASSERT_EQ(ObSSTableMicroBlockState::IN_BLOCK_CACHE,
      scanner.data_ring_[0].data_handle_.block_state_);
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(is_iter_end));
  ASSERT_FALSE(is_iter_end);
  ASSERT_NE(nullptr, scanner.fastscan_header_);
  ASSERT_EQ(nullptr, scanner.micro_reader_);
  ASSERT_EQ(101, scanner.fastscan_header_->min_vid_);
  ASSERT_EQ(103, scanner.fastscan_header_->max_vid_);
  EXPECT_FALSE(scanner.row_computer_ready_);
  EXPECT_FALSE(scanner.computer_.is_inited());
  scanner.close_scan();

  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(102));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfSSTableReadStats stats;
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid, &filter));
  EXPECT_TRUE(scanner.row_computer_ready_);
  EXPECT_TRUE(scanner.computer_.is_inited());
  EXPECT_FALSE(scanner.fastscan_computer_.is_inited_);
  // PRE populates an independent quantized row cache without replacing FastScan.
  ASSERT_EQ(ObSSTableMicroBlockState::IN_BLOCK_IO,
      scanner.data_ring_[0].data_handle_.block_state_);
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(is_iter_end));
  ASSERT_FALSE(is_iter_end);
  ASSERT_EQ(nullptr, scanner.micro_reader_);
  ASSERT_NE(nullptr, scanner.quantized_row_header_);
  ASSERT_EQ(nullptr, scanner.fastscan_header_);
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(1, batch.count());
  ASSERT_EQ(102, batch.at(0).vid_);
  ASSERT_EQ(vector_count, batch.scan_count());
  ASSERT_EQ(1, stats.low_precision_vector_count_);
  scanner.close_scan();

  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, nullptr, query, centroid));
  ASSERT_EQ(ObSSTableMicroBlockState::IN_BLOCK_CACHE,
      scanner.current_data_item().data_handle_.block_state_);
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(is_iter_end));
  ASSERT_FALSE(is_iter_end);
  ASSERT_NE(nullptr, scanner.fastscan_header_);
  ASSERT_EQ(nullptr, scanner.micro_reader_);
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(vector_count, batch.count());
  EXPECT_FALSE(scanner.row_computer_ready_);
  EXPECT_FALSE(scanner.computer_.is_inited());
  scanner.close_scan();
}


TEST_P(TestIVFFastScannerRowStore, disk_bbq_row_computer_is_prepared_on_sync_fallback)
{
  const int32_t dim = 64;
  const int64_t count = 80;
  float query[dim] = {0};
  std::vector<float> vectors(count * dim, 0.0F), centroids(2 * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  centroids[dim] = 0.2F;
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / 40;
    vids[i] = 100 + i;
    vectors[i * dim + i % dim] = (i + 1) * 0.01F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, GetParam()));
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), 2, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner, reference;
  ObTableAccessParam param, ref_param;
  ObIvfSSTableRowDesc desc, ref_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(scanner,
      scan_param_, sstable_, scan_context_, param, read_info, desc));
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(reference,
      scan_param_, sstable_, scan_context_, ref_param, read_info, ref_desc));
  ObRoaringBitmap allowed(&allocator_);
  for (int64_t vid : vids) { ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid)); }
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  for (bool query_scope : {false, true}) {
    for (bool lower_bound : {false, true}) {
      query[0] = lower_bound ? 0.7F : 0.3F;
      if (query_scope) { ASSERT_EQ(OB_SUCCESS, scanner.begin_query(query)); }
      for (int64_t cid = 0; cid < 2; ++cid) {
        SCOPED_TRACE(::testing::Message() << query_scope << ":" << lower_bound << ":" << cid);
        ObIvfSSTableReadStats stats, ref_stats;
        const float *centroid = centroids.data() + cid * dim;
        ASSERT_EQ(OB_SUCCESS, scanner.open_scan(cid + 1, &stats, query, centroid, nullptr, lower_bound));
        EXPECT_FALSE(scanner.row_computer_ready_);
        EXPECT_EQ(query_scope && cid > 0, scanner.computer_.is_inited());
        // Exercise real ordinary-block IO, including when a previous CID's
        // query-scoped computer is still initialized. All blocks use row scoring.
        for (int64_t i = scanner.data_read_idx_; i < scanner.data_prefetch_idx_; ++i) {
          auto &handle = scanner.data_ring_[i % ObDiskBBQFastScanner::DATA_RING_CAP].data_handle_;
          if (ObSSTableMicroBlockState::IN_BLOCK_IO == handle.block_state_) {
            ASSERT_EQ(OB_SUCCESS, handle.io_handle_.wait());
          }
          handle.block_state_ = ObSSTableMicroBlockState::NEED_SYNC_IO;
        }
        bool end = false, ref_end = false;
        ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(end));
        ASSERT_FALSE(end);
        ASSERT_NE(nullptr, scanner.micro_reader_);
        ASSERT_TRUE(scanner.row_computer_ready_);
        ASSERT_TRUE(scanner.computer_.is_inited());
        // PRE's row cache uses the same quantization, without FastScan LUT rounding.
        ASSERT_EQ(OB_SUCCESS, reference.open_scan(cid + 1, &ref_stats, query, centroid, &filter, lower_bound));
        int64_t result_count = 0;
        while (!end) {
          ObDistBBQFastScanBatch batch, ref_batch;
          ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, end));
          ASSERT_EQ(OB_SUCCESS, reference.get_next_batch(ref_batch, ref_end));
          ASSERT_EQ(ref_end, end);
          ASSERT_EQ(ref_batch.count(), batch.count());
          for (int64_t i = 0; i < batch.count(); ++i) {
            EXPECT_EQ(ref_batch.at(i).vid_, batch.at(i).vid_);
            EXPECT_FLOAT_EQ(ref_batch.at(i).distance_, batch.at(i).distance_);
          }
          result_count += batch.count();
        }
        EXPECT_EQ(40, result_count);
        EXPECT_EQ(ref_stats.low_precision_vector_count_, stats.low_precision_vector_count_);
        scanner.close_scan();
        reference.close_scan();
        EXPECT_FALSE(scanner.row_computer_ready_);
        EXPECT_EQ(nullptr, scanner.scan_query_);
        EXPECT_EQ(nullptr, scanner.scan_centroid_);
      }
      scanner.end_query();
      EXPECT_FALSE(scanner.computer_.is_inited());
    }
  }
}

TEST_P(TestIVFFastScannerRowStore,
    disk_bbq_prefilter_selects_tail_row_without_affecting_unfiltered_scan)
{
  const int32_t dim = 64;
  const int64_t vector_count = ObDistBBQFastScanBatch::CAPACITY + 1;
  std::vector<float> vectors(vector_count * dim, 0.0F);
  std::vector<int64_t> cids(vector_count, 0);
  std::vector<int64_t> vids(vector_count);
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < vector_count; ++i) {
    vids[i] = 100 + i;
    vectors[i * dim + i % dim] = 0.1F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, GetParam()));
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      vector_count, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));

  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids.back()));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfSSTableReadStats stats;
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid, &filter));
  ObDistBBQFastScanBatch batch;
  bool is_iter_end = false;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(1, batch.count());
  ASSERT_EQ(vids.back(), batch.at(0).vid_);
  ASSERT_EQ(1, stats.low_precision_vector_count_);
  scanner.close_scan();

  stats.reset();
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid));
  int64_t unfiltered_count = 0;
  is_iter_end = false;
  while (!is_iter_end) {
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
    unfiltered_count += batch.count();
  }
  ASSERT_EQ(vector_count, unfiltered_count);
  ASSERT_EQ(vector_count, stats.low_precision_vector_count_);
  scanner.close_scan();
}

TEST_P(TestIVFFastScannerRowStore,
    disk_bbq_fast_scanner_skips_batches_without_allowed_vids)
{
  const int32_t dim = 64;
  const int64_t vector_count = 2 * ObDistBBQFastScanBatch::CAPACITY + 1;
  std::vector<float> vectors(vector_count * dim, 0.0F);
  std::vector<int64_t> cids(vector_count, 0);
  std::vector<int64_t> vids(vector_count);
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < vector_count; ++i) {
    vids[i] = 100 + i;
    vectors[i * dim + i % dim] = 0.1F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, GetParam()));
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      vector_count, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));

  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids.front()));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids.back()));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfSSTableReadStats stats;
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid, &filter));
  ObDistBBQFastScanBatch batch;
  bool is_iter_end = false;
  int64_t candidate_count = 0;
  while (!is_iter_end) {
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
    candidate_count += batch.count();
  }
  // The middle batch is within the overall VID range but contains no
  // allowed VID. The first and last batches still produce candidates.
  ASSERT_EQ(2, candidate_count);
  ASSERT_EQ(candidate_count, stats.low_precision_vector_count_);
  scanner.close_scan();
}

TEST_P(TestIVFFastScannerRowStore, disk_bbq_prefilter_sparse_rows_and_cache_reuse)
{
  const int32_t dim = 64;
  const int64_t count = 70;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0), vids(count);
  float centroid[dim] = {0};
  float query[dim] = {0};
  for (int64_t i = 0; i < count; ++i) {
    vids[i] = 100 + 3 * i;
    vectors[i * dim + i % dim] = static_cast<float>(i + 1) * 0.01F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, GetParam()));
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroid, 1, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));

  // Cold PRE, warm PRE, all-rejected PRE, and all-accepted PRE. Include
  // matching rows on both sides of a batch boundary and in the final tail.
  for (int mode = 0; mode < 4; ++mode) {
    SCOPED_TRACE(mode);
    ObRoaringBitmap allowed(&allocator_);
    std::vector<int64_t> expected;
    if (mode < 2) {
      for (int64_t i : {int64_t(1), int64_t(31), int64_t(64), int64_t(69)}) {
        ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids[i]));
        expected.push_back(vids[i]);
      }
    } else if (mode == 2) {
      // A nonempty bitmap spanning the block, but with no actual matches.
      ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids.front() - 1));
      ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids.back() + 1));
    } else {
      for (int64_t vid : vids) { ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid)); }
      expected = vids;
    }
    ObIvfPreFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
    ObIvfSSTableReadStats stats;
    ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid, &filter, mode != 1));
    EXPECT_FALSE(scanner.fastscan_computer_.is_inited_);
    if (mode > 0) {
      EXPECT_EQ(ObSSTableMicroBlockState::IN_BLOCK_CACHE,
          scanner.current_data_item().data_handle_.block_state_);
    }
    bool end = false;
    ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(end));
    ASSERT_FALSE(end);
    ASSERT_EQ(nullptr, scanner.fastscan_header_);
    ASSERT_EQ(nullptr, scanner.micro_reader_);
    ASSERT_NE(nullptr, scanner.quantized_row_header_);
    std::vector<int64_t> actual;
    int64_t charged = 0;
    while (!end) {
      ObDistBBQFastScanBatch batch;
      ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, end));
      charged += batch.scan_count();
      for (int64_t i = 0; i < batch.count(); ++i) {
        actual.push_back(batch.at(i).vid_);
        const float amplitude = static_cast<float>((batch.at(i).vid_ - 100) / 3 + 1) * 0.01F;
        EXPECT_NEAR(amplitude * amplitude, batch.at(i).distance_, 1e-5F);
      }
    }
    EXPECT_EQ(expected, actual);
    EXPECT_EQ(expected.size(), stats.low_precision_vector_count_);
    EXPECT_EQ(mode < 2 ? 38 : mode == 2 ? 0 : count, charged);
    scanner.close_scan();
  }
}

TEST_P(TestIVFFastScannerRowStore, quantized_row_cache_handles_cid_gaps_and_changed_filter)
{
  const int32_t dim = 64;
  const int64_t count = 75;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  float centroids[3 * dim] = {0};
  float query[dim] = {0};
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i < 35 ? 0 : 2;
    vids[i] = 100 + i;
    vectors[i * dim + i % dim] = static_cast<float>(i + 1) * 0.01F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, GetParam()));
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids, 3, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));
  for (int round = 0; round < 2; ++round) {
    ObRoaringBitmap allowed(&allocator_);
    if (round == 0) {
      for (int64_t i : {int64_t(1), int64_t(34), int64_t(35), int64_t(74)}) {
        ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids[i]));
      }
    } else {
      for (int64_t vid : vids) { ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid)); }
    }
    ObIvfPreFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
    for (int64_t cid = 0; cid < 3; ++cid) {
      ASSERT_EQ(OB_SUCCESS, scanner.open_scan(cid + 1, nullptr, query, centroids + cid * dim, &filter));
      bool end = false;
      std::vector<int64_t> actual, expected;
      for (int64_t i = 0; i < count; ++i) {
        if (cids[i] == cid && filter.contains(vids[i])) { expected.push_back(vids[i]); }
      }
      while (!end) {
        ObDistBBQFastScanBatch batch;
        ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, end));
        for (int64_t i = 0; i < batch.count(); ++i) {
          EXPECT_EQ(cid + 1, batch.at(i).cid_);
          const float amplitude = static_cast<float>(batch.at(i).vid_ - 99) * 0.01F;
          EXPECT_NEAR(amplitude * amplitude, batch.at(i).distance_, 1e-5F);
          actual.push_back(batch.at(i).vid_);
        }
      }
      EXPECT_EQ(expected, actual);
      scanner.close_scan();
    }
  }
}

class CountingDiskBBQPreFilterDecoder : public ObMicroBlockDecoder
{
public:
  using ObMicroBlockDecoder::get_rows;
  CountingDiskBBQPreFilterDecoder() : vid_rows_(0), payload_rows_(0), vid_column_(-1), payload_column_(-1) {}
  int get_rows(const ObIArray<int32_t> &cols,
      const ObIArray<const share::schema::ObColumnParam *> &params, const bool padding,
      const int32_t *row_ids, const char **cell_datas, const int64_t row_cap,
      ObIArray<ObSqlDatumInfo> &infos, const int64_t offset = 0) override
  {
    EXPECT_EQ(1, cols.count());
    if (cols.at(0) == vid_column_) {
      vid_rows_ += row_cap;
    } else {
      EXPECT_EQ(payload_column_, cols.at(0));
      payload_rows_ += row_cap;
      for (int64_t i = 0; i < row_cap; ++i) { payload_row_ids_.push_back(row_ids[i]); }
    }
    return ObMicroBlockDecoder::get_rows(cols, params, padding, row_ids, cell_datas, row_cap, infos, offset);
  }
  int64_t vid_rows_;
  int64_t payload_rows_;
  int32_t vid_column_;
  int32_t payload_column_;
  std::vector<int32_t> payload_row_ids_;
};

TEST_F(TestIVFSSTableWriter, disk_bbq_prefilter_projects_payload_only_after_vid_selection)
{
  const int32_t dim = 64;
  const int64_t count = 70;
  std::vector<float> vectors(count * dim, 0.1F);
  std::vector<int64_t> cids(count, 0), vids(count);
  std::iota(vids.begin(), vids.end(), 100);
  float centroid[dim] = {0};
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, ENCODING_ROW_STORE));
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroid, 1, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  CountingDiskBBQPreFilterDecoder decoder;
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(101));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(169));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfSSTableReadStats stats;
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid, &filter));
  bool end = false;
  // Force the synchronous ordinary-block fallback after successful async IO.
  // This checks that retained deserialize metadata survives the index leaf.
  auto &handle = scanner.current_data_item().data_handle_;
  ASSERT_EQ(OB_SUCCESS, handle.io_handle_.wait());
  handle.block_state_ = ObSSTableMicroBlockState::NEED_SYNC_IO;
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(end));
  ASSERT_FALSE(end);
  ASSERT_NE(nullptr, scanner.micro_reader_);
  ASSERT_EQ(count, scanner.current_last_row_idx_ - scanner.current_row_idx_ + 1);
  const int32_t start_row = scanner.current_row_idx_;
  ObMicroBlockData block;
  ASSERT_EQ(OB_SUCCESS, scanner.current_data_item().data_handle_.get_micro_block_data(nullptr, block, true));
  ASSERT_EQ(OB_SUCCESS, decoder.init(block, read_info));
  decoder.vid_column_ = row_desc.vector_id_col_idx_;
  decoder.payload_column_ = row_desc.payload_col_idx_;
  scanner.micro_reader_->reset();
  scanner.micro_reader_ = &decoder;
  int64_t charged = 0;
  while (!end) {
    ObDistBBQFastScanBatch batch;
    ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, end));
    charged += batch.scan_count();
  }
  EXPECT_EQ(count, decoder.vid_rows_);
  EXPECT_EQ(2, decoder.payload_rows_);
  EXPECT_EQ((std::vector<int32_t>{start_row + 1, start_row + 69}), decoder.payload_row_ids_);
  EXPECT_EQ(2, stats.low_precision_vector_count_);
  EXPECT_EQ(38, charged);
  scanner.close_scan();
}

TEST_P(TestIVFFastScannerRowStore,
    centroid_scan_stops_before_cached_fastscan_boundary)
{
  CentroidCacheMemoryPercentageGuard percentage;
  CentroidHgraphThresholdGuard graph_threshold;
  ASSERT_TRUE(percentage.config_.is_valid());
  ASSERT_TRUE(graph_threshold.config_.is_valid());
  graph_threshold.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  const int32_t dim = 64;
  const int64_t vector_count = 3;
  float vectors[vector_count][dim] = {{0}};
  const int64_t cids[vector_count] = {0, 0, 0};
  const int64_t vids[vector_count] = {101, 102, 103};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < vector_count; ++i) {
    vectors[i][i] = static_cast<float>(i + 1);
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, GetParam()));
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(&vectors[0][0], cids, vids,
      vector_count, centroid, 1, dim, flip));
  sstable_.ivf_meta_.centroid_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));
  // Populate the vector cache through the real FastScan IO path,
  // then verify centroid iteration stops at its declared row count.
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, nullptr, query, centroid));
  bool is_iter_end = false;
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(is_iter_end));
  ASSERT_FALSE(is_iter_end);
  ASSERT_NE(nullptr, scanner.fastscan_header_);
  scanner.close_scan();
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, nullptr, query, centroid));
  ASSERT_EQ(ObSSTableMicroBlockState::IN_BLOCK_CACHE,
      scanner.current_data_item().data_handle_.block_state_);
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(is_iter_end));
  ASSERT_NE(nullptr, scanner.fastscan_header_);
  ASSERT_EQ(nullptr, scanner.micro_reader_);

  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  const share::ObLSID cache_ls_id(409700 + static_cast<int64_t>(GetParam()));
  const auto &posting_handle = scanner.current_data_item().data_handle_;
  ObMicroBlockCacheKey posting_key;
  if (posting_handle.micro_info_.logic_micro_id_.is_valid()) {
    posting_key.set(MTL_ID(), posting_handle.micro_info_.logic_micro_id_,
        posting_handle.micro_info_.data_checksum_);
  } else {
    posting_key.set(MTL_ID(), posting_handle.macro_block_id_,
        posting_handle.micro_info_.offset_, posting_handle.micro_info_.size_);
  }
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(cache_ls_id, ls_mgr, false));
  for (int64_t cache_percentage : {0, 80}) {
    percentage.config_->_disk_bbq_centroid_cache_mem_percentage = cache_percentage;
    for (bool iterative : {false, true}) {
      clear_hierarchical_test_cache(cache_ls_id, sstable_.get_key(), 0);
      ObTableIterParam iter_param;
      ObTableAccessContext context;
      ObStoreCtx store_ctx;
      ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
      context.ls_id_ = cache_ls_id;
      context.query_flag_.set_use_block_cache();
      ObIvfKnnSearchParam param;
      fill_search_params(param, query, dim, vector_count);
      param.nprobe_ = 1;
      param.iterative_centroids_ = iterative;
      ObArenaAllocator query_allocator;
      ObIvfCentroidIteratorGuard iterator(allocator_);
      ASSERT_EQ(OB_SUCCESS, sstable_.create_centroid_iterator(param, query, allocator_,
          query_allocator, iter_param, row_desc, context, iterator.get()));
      EXPECT_EQ(cache_percentage > 0, flat_group_reader_for_test(iterator.get()).array_cache_guard_.cache_ != nullptr);
      ObIvfCentroidInfo result;
      ASSERT_EQ(OB_SUCCESS, iterator->get_next(result));
      EXPECT_EQ(1, result.cid_);
      EXPECT_EQ(OB_ITER_END, iterator->get_next(result));
    }
  }
  clear_hierarchical_test_cache(cache_ls_id, sstable_.get_key(), 0);
  scanner.close_scan();
  // The next fixture can reuse the same logical block ID and checksum.
  ASSERT_EQ(OB_SUCCESS, ObStorageCacheSuite::get_instance().get_ivf_fastscan_cache().erase(posting_key));
}

TEST_F(TestIVFSSTableWriter, disk_bbq_prepared_query_spans_micro_blocks_and_changes_filter)
{
  const int dim = 64, count = 520;
  float query[dim] = {0.3F};
  std::vector<float> vectors(count * dim, 0.0F), centroids(2 * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  for (int i = 0; i < count; ++i) {
    cids[i] = i / 260; vids[i] = 100 + i;
    vectors[i * dim + i % dim] = (i + 1) * 0.01F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), 2, dim, nullptr, 1, nullptr, share::VIDA_L2,
      true, nullptr, nullptr, 4096));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner, reference;
  ObTableAccessParam param, ref_param;
  ObIvfSSTableRowDesc desc, ref_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(scanner,
      scan_param_, sstable_, scan_context_, param, read_info, desc));
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(reference,
      scan_param_, sstable_, scan_context_, ref_param, read_info, ref_desc));
  ObRoaringBitmap allowed_a(&allocator_), allowed_b(&allocator_);
  for (int64_t vid : vids) {
    if (vid % 3 == 0) { ASSERT_EQ(OB_SUCCESS, allowed_a.value_add(vid)); }
    if (vid % 5 == 0) { ASSERT_EQ(OB_SUCCESS, allowed_b.value_add(vid)); }
  }
  ObIvfPreFilter a, b;
  ASSERT_EQ(OB_SUCCESS, a.init(allowed_a));
  ASSERT_EQ(OB_SUCCESS, b.init(allowed_b));
  // Abandon a partial scan, then reuse the scanner with changed queries and filters.
  ASSERT_EQ(OB_SUCCESS, scanner.begin_query(query));
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, nullptr, query, centroids.data(), &a));
  bool end = false;
  ObDistBBQFastScanBatch partial;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(partial, end));
  ASSERT_FALSE(end);
  scanner.close_scan(); scanner.end_query();
  for (int pass = 0; pass < 4; ++pass) {
    query[0] = (pass + 1) * 0.17F;
    ObIvfPreFilter *filter = pass % 2 ? &b : &a;
    ASSERT_EQ(OB_SUCCESS, scanner.begin_query(query));
    for (int cid = 0; cid < 2; ++cid) {
      ASSERT_EQ(OB_SUCCESS, scanner.open_scan(cid + 1, nullptr, query, centroids.data() + cid * dim, filter));
      ASSERT_EQ(OB_SUCCESS, reference.open_scan(cid + 1, nullptr, query, centroids.data() + cid * dim, filter));
      bool done = false, ref_done = false;
      std::vector<int64_t> matched_vids;
      while (!done) {
        ObDistBBQFastScanBatch got, expected;
        ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(got, done));
        ASSERT_EQ(OB_SUCCESS, reference.get_next_batch(expected, ref_done));
        ASSERT_EQ(ref_done, done);
        ASSERT_EQ(expected.count(), got.count());
        EXPECT_EQ(expected.scan_count(), got.scan_count());
        for (int64_t i = 0; i < got.count(); ++i) {
          matched_vids.push_back(got.at(i).vid_);
          EXPECT_EQ(expected.at(i).vid_, got.at(i).vid_);
          EXPECT_FLOAT_EQ(expected.at(i).distance_, got.at(i).distance_);
        }
      }
      std::vector<int64_t> expected_vids;
      for (int i = 0; i < count; ++i) {
        if (cids[i] == cid && vids[i] % (pass % 2 ? 5 : 3) == 0) {
          expected_vids.push_back(vids[i]);
        }
      }
      EXPECT_EQ(expected_vids, matched_vids);
      scanner.close_scan(); reference.close_scan();
    }
    scanner.end_query();
  }
}

TEST_F(TestIVFSSTableWriter, disk_bbq_prepared_query_lifetime_and_changed_query)
{
  const int dim = 64, count = 80;
  float query[dim] = {0.3F}, other_query[dim] = {0.7F};
  std::vector<float> vectors(count * dim, 0.0F), centroids(2 * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  centroids[dim] = 0.1F;
  for (int i = 0; i < count; ++i) {
    cids[i] = i / 40; vids[i] = 100 + i;
    vectors[i * dim + i % dim] = (i + 1) * 0.01F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), 2, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner, reference;
  EXPECT_EQ(OB_NOT_INIT, scanner.begin_query(query));
  ObTableAccessParam param, ref_param;
  ObIvfSSTableRowDesc desc, ref_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(scanner,
      scan_param_, sstable_, scan_context_, param, read_info, desc));
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(reference,
      scan_param_, sstable_, scan_context_, ref_param, read_info, ref_desc));
  ObRoaringBitmap allowed(&allocator_);
  for (int64_t vid : vids) { if (vid % 3 == 0) { ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid)); } }
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  for (int q = 0; q < 3; ++q) {
    query[0] = q * 0.4F; // Same address, changed contents, explicit new query boundary.
    ASSERT_EQ(OB_SUCCESS, scanner.begin_query(query));
    EXPECT_FLOAT_EQ(query[0], scanner.query_raw_norm_);
    EXPECT_EQ(OB_INVALID_ARGUMENT, scanner.open_scan(1, nullptr, other_query, centroids.data(), &filter));
    for (int cid = 0; cid < 2; ++cid) {
      ObIvfSSTableReadStats stats, ref_stats;
      ASSERT_EQ(OB_SUCCESS, scanner.open_scan(cid + 1, &stats, query, centroids.data() + cid * dim, &filter));
      EXPECT_EQ(OB_STATE_NOT_MATCH, scanner.begin_query(query));
      ASSERT_EQ(OB_SUCCESS, reference.open_scan(cid + 1, &ref_stats, query, centroids.data() + cid * dim, &filter));
      bool end = false, ref_end = false;
      while (!end) {
        ObDistBBQFastScanBatch batch, ref_batch;
        ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, end));
        ASSERT_EQ(OB_SUCCESS, reference.get_next_batch(ref_batch, ref_end));
        ASSERT_EQ(ref_end, end);
        ASSERT_EQ(ref_batch.count(), batch.count());
        EXPECT_EQ(ref_batch.scan_count(), batch.scan_count());
        for (int64_t i = 0; i < batch.count(); ++i) {
          EXPECT_EQ(ref_batch.at(i).vid_, batch.at(i).vid_);
          EXPECT_FLOAT_EQ(ref_batch.at(i).distance_, batch.at(i).distance_);
        }
      }
      EXPECT_EQ(ref_stats.low_precision_vector_count_, stats.low_precision_vector_count_);
      scanner.close_scan(); reference.close_scan();
      EXPECT_TRUE(scanner.computer_.is_inited());
      EXPECT_FALSE(reference.computer_.is_inited());
      void *noise = scanner.scan_allocator_.alloc(dim * sizeof(float) + 128);
      ASSERT_NE(nullptr, noise);
      MEMSET(noise, 0xA5, dim * sizeof(float) + 128);
    }
    scanner.end_query();
    EXPECT_FALSE(scanner.computer_.is_inited());
    EXPECT_EQ(nullptr, scanner.prepared_query_);
  }
}

TEST_P(TestIVFFastScannerRowStore,
    vector_cache_formats_are_isolated_and_handles_survive_erase)
{
  const int32_t dim = 64;
  const int64_t vector_count = 3;
  float vectors[vector_count][dim] = {{0}};
  const int64_t cids[vector_count] = {0, 0, 0};
  const int64_t vids[vector_count] = {101, 102, 103};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < vector_count; ++i) {
    vectors[i][i] = static_cast<float>(i + 1);
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(0, GetParam()));
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(&vectors[0][0], cids, vids,
      vector_count, centroid, 1, dim, flip));
  sstable_.ivf_meta_.centroid_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_,
      access_param, read_info, row_desc));

  // Locate the posting block without reading its payload or populating its cache.
  scanner.set_posting_cid(1);
  ObSSTableIndexScanParam index_param;
  ASSERT_EQ(OB_SUCCESS, index_param.init(read_info, scan_context_.query_flag_,
      scan_context_.tablet_id_, ObSSTableIndexScanParam::LEAF));
  ObSSTableIndexBaseScanner index_scanner;
  ASSERT_EQ(OB_SUCCESS, index_scanner.init(
      scanner.posting_range_, index_param, sstable_, allocator_));
  ObMicroIndexInfo posting_info;
  ASSERT_EQ(OB_SUCCESS, index_scanner.get_next(posting_info));
  int64_t row_type = 0;
  ASSERT_EQ(OB_SUCCESS, posting_info.endkey_.get_column_int(
      row_desc.row_type_col_idx_, row_type));
  ASSERT_EQ(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, row_type);
  const ObMicroBlockCacheKey posting_key(MTL_ID(), posting_info);
  auto &suite = ObStorageCacheSuite::get_instance();
  ObDataMicroBlockCache &cache = suite.get_block_cache();
  // Fixtures can reuse logical block IDs and checksums while the caches persist.
  // Establish cold entries for this key before checking format isolation.
  for (auto *format_cache : {&cache, &suite.get_ivf_fastscan_cache(), &suite.get_ivf_quantized_row_cache()}) {
    const int erase_ret = format_cache->erase(posting_key);
    ASSERT_TRUE(OB_SUCCESS == erase_ret || OB_ENTRY_NOT_EXIST == erase_ret) << erase_ret;
  }
  ObMicroBlockBufferHandle cached_posting;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, cache.get_cache_block(posting_key, cached_posting));

  // Populate FastScan first, then read the same posting through the
  // ordinary row cache to verify the representations remain independent.
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, nullptr, query, centroid));
  bool fast_end = false;
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(fast_end));
  ASSERT_NE(nullptr, scanner.fastscan_header_);
  scanner.close_scan();
  ObMicroBlockBufferHandle fast_handle;
  ASSERT_EQ(OB_SUCCESS, suite.get_ivf_fastscan_cache().get_cache_block(posting_key, fast_handle));
  ASSERT_EQ(ObMicroBlockData::IVF_FASTSCAN_BLOCK, fast_handle.get_block_data()->type_);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, cache.get_cache_block(posting_key, cached_posting));

  // Exercise the ordinary row reader directly. A count-bounded centroid scan
  // no longer needs to read this posting block as an EOF boundary check.
  ObTableIterParam plain_iter_param;
  ObTableAccessContext plain_context;
  ObStoreCtx plain_store_ctx;
  ASSERT_EQ(OB_SUCCESS, init_scan_context(
      read_info, plain_iter_param, plain_context, plain_store_ctx));
  plain_context.query_flag_.set_use_block_cache();
  {
    ObSSTableRowScanner<> plain_scanner;
    ASSERT_EQ(OB_SUCCESS, plain_scanner.init(
        plain_iter_param, plain_context, &sstable_, &scanner.posting_range_));
    const ObDatumRow *row = nullptr;
    int ret = OB_SUCCESS;
    int64_t row_count = 0;
    while (OB_SUCCESS == (ret = plain_scanner.get_next_row(row))) {
      ASSERT_NE(nullptr, row);
      EXPECT_EQ(IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
          row->storage_datums_[row_desc.row_type_col_idx_].get_int());
      ++row_count;
    }
    ASSERT_EQ(OB_ITER_END, ret);
    EXPECT_EQ(vector_count, row_count);
    plain_scanner.reuse();
  }

  ASSERT_EQ(OB_SUCCESS, cache.get_cache_block(posting_key, cached_posting));
  ASSERT_NE(nullptr, cached_posting.get_block_data());
  ASSERT_EQ(ObMicroBlockData::DATA_BLOCK, cached_posting.get_block_data()->type_);

  ObIvfSSTableReadStats stats;
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid));
  ASSERT_EQ(ObSSTableMicroBlockState::IN_BLOCK_CACHE,
      scanner.current_data_item().data_handle_.block_state_);
  bool is_iter_end = false;
  ASSERT_EQ(OB_SUCCESS, scanner.open_next_micro_block(is_iter_end));
  ASSERT_FALSE(is_iter_end);
  ASSERT_NE(nullptr, scanner.fastscan_header_);
  ASSERT_EQ(nullptr, scanner.micro_reader_);

  ObDistBBQFastScanBatch batch;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(vector_count, batch.count());
  for (int64_t i = 0; i < vector_count; ++i) {
    EXPECT_EQ(1, batch.at(i).cid_);
    EXPECT_EQ(vids[i], batch.at(i).vid_);
    // q == centroid makes the residual error zero, so the L2 lower bound
    // equals the exact squared norm, independently of the sign quantization.
    EXPECT_NEAR(static_cast<float>((i + 1) * (i + 1)), batch.at(i).distance_, 1e-4F);
  }
  EXPECT_EQ(vector_count, stats.low_precision_vector_count_);
  scanner.close_scan();

  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids[1]));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  stats.reset();
  ASSERT_EQ(OB_SUCCESS, scanner.open_scan(1, &stats, query, centroid, &filter));
  ASSERT_EQ(ObSSTableMicroBlockState::IN_BLOCK_IO,
      scanner.current_data_item().data_handle_.block_state_);
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(1, batch.count());
  ASSERT_EQ(vids[1], batch.at(0).vid_);
  ASSERT_EQ(1, stats.low_precision_vector_count_);
  scanner.close_scan();

  // All three representations of the same key coexist. Erasing their cache
  // entries must not invalidate handles already held by a reader.
  ObMicroBlockBufferHandle row_handle;
  ASSERT_EQ(OB_SUCCESS, suite.get_ivf_quantized_row_cache().get_cache_block(posting_key, row_handle));
  ASSERT_EQ(ObMicroBlockData::IVF_QUANTIZED_ROW_BLOCK, row_handle.get_block_data()->type_);
  ASSERT_EQ(OB_SUCCESS, suite.get_ivf_quantized_row_cache().erase(posting_key));
  ASSERT_EQ(OB_SUCCESS, suite.get_ivf_fastscan_cache().erase(posting_key));
  for (const auto *source : {row_handle.get_block_data(), fast_handle.get_block_data()}) {
    auto &target_cache = source->type_ == ObMicroBlockData::IVF_FASTSCAN_BLOCK
        ? suite.get_ivf_fastscan_cache() : suite.get_ivf_quantized_row_cache();
    std::array<ObKVCacheHandle, 4> handles;
    std::array<const ObMicroBlockCacheValue *, 4> values = {};
    std::array<int, 4> results = {};
    std::atomic<int> ready(0);
    std::atomic<bool> start(false);
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
      workers.emplace_back([&, i]() {
        ++ready;
        while (!start.load()) { std::this_thread::yield(); }
        results[i] = target_cache.put_posting_cache_block(source->get_buf(), source->get_buf_size(),
            posting_key, source->type_, values[i], handles[i]);
      });
    }
    while (ready.load() < 4) { std::this_thread::yield(); }
    start.store(true);
    for (auto &worker : workers) { worker.join(); }
    ASSERT_EQ(OB_SUCCESS, target_cache.erase(posting_key));
    for (int i = 0; i < 4; ++i) {
      ASSERT_EQ(OB_SUCCESS, results[i]);
      ASSERT_TRUE(handles[i].is_valid());
      ASSERT_NE(nullptr, values[i]);
      ASSERT_EQ(source->type_, values[i]->get_block_data().type_);
      EXPECT_EQ(0, MEMCMP(source->get_buf(), values[i]->get_block_data().get_buf(), source->get_buf_size()));
    }
  }
  const auto *rows = reinterpret_cast<const ObIvfQuantizedRowBlockHeader *>(row_handle.get_block_data()->get_buf());
  ASSERT_TRUE(rows->is_valid(row_handle.get_block_data()->get_buf_size()));
  ASSERT_EQ(vids[1], rows->vids()[1]);
  ASSERT_EQ(ObMicroBlockData::IVF_FASTSCAN_BLOCK, fast_handle.get_block_data()->type_);
  row_handle.reset();
  fast_handle.reset();
  cached_posting.reset();
  ASSERT_EQ(OB_SUCCESS, cache.erase(posting_key));

}


TEST_F(TestIVFSSTableWriter,
    disk_bbq_low_precision_vector_scanner_failed_low_precision_scan_does_not_poison_reuse)
{
  const int32_t dim = 64;
  const int32_t nlist = 1;
  const int32_t nbits = 1;
  const int64_t cid = 0;
  const int64_t vid = 10;
  float vector[dim] = {0};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  vector[0] = 0.5F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vector, &cid, &vid, 1, centroid, nlist, dim, flip, nbits));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObDiskBBQFastScanner scanner;
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));
  const auto original_scan_order = scan_context_.query_flag_.scan_order_;

  ASSERT_EQ(OB_INVALID_ARGUMENT,
      scanner.open_scan(cid + 1, query_param.read_stats_, nullptr, centroid));
  ASSERT_EQ(original_scan_order, scan_context_.query_flag_.scan_order_);
  ASSERT_EQ(OB_SUCCESS,
      scanner.open_scan(cid + 1, query_param.read_stats_, query, centroid));
  ObDistBBQFastScanBatch batch;
  bool is_iter_end = false;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(1, batch.count());
  ASSERT_EQ(vid, batch.at(0).vid_);
  scanner.close_scan();
  ASSERT_EQ(original_scan_order, scan_context_.query_flag_.scan_order_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_low_precision_vector_scanner_failed_open_does_not_poison_retry)
{
  const int32_t dim = 64;
  const int32_t nlist = 1;
  const int32_t nbits = 1;
  const int64_t cid = 0;
  const int64_t vid = 10;
  float vector[dim] = {0};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  vector[0] = 0.5F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vector, &cid, &vid, 1, centroid, nlist, dim, flip, nbits));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObDiskBBQFastScanner scanner;
  ObTableIterParam invalid_iter_param;
  ObIvfSSTableRowDesc invalid_row_desc;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      scanner.init(invalid_iter_param, invalid_row_desc, sstable_, scan_context_));
  ObTableAccessParam access_param;
  ObIvfSSTableRowDesc row_desc;
  ASSERT_EQ(OB_SUCCESS, init_disk_bbq_fast_scanner(
      scanner, scan_param_, sstable_, scan_context_, access_param, read_info, row_desc));
  ASSERT_EQ(OB_SUCCESS,
      scanner.open_scan(cid + 1, query_param.read_stats_, query, centroid));
  ObDistBBQFastScanBatch batch;
  bool is_iter_end = false;
  ASSERT_EQ(OB_SUCCESS, scanner.get_next_batch(batch, is_iter_end));
  ASSERT_TRUE(is_iter_end);
  ASSERT_EQ(1, batch.count());
  ASSERT_EQ(vid, batch.at(0).vid_);
  scanner.close_scan();
}

TEST_F(TestIVFSSTableWriter, ivf_searcher_factory_handles_empty_meta_before_quantization)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_TRUE(sstable_.is_ivf_sstable());
  ASSERT_EQ(0, sstable_.get_data_macro_block_count());
  ASSERT_TRUE(sstable_.no_data_to_read());
  sstable_.ivf_meta_.set_empty();
  ASSERT_TRUE(sstable_.get_ivf_meta().is_empty());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[8] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, 8, 3);
  ObIvfKnnSearchResult results;
  ObIvfCountingAllocator other_allocator(allocator_);
  ObDiskBBQSearcher mismatched_allocator_searcher(other_allocator);
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS, table_handle.set_sstable_with_tablet(&sstable_));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      mismatched_allocator_searcher.open(table_handle, scan_param_, scan_context_));
  ObDiskBBQSearcher unopened_searcher(allocator_);
  ASSERT_EQ(OB_NOT_INIT,
      unopened_searcher.knn_search(query_param, results));
  ObIvfCountingAllocator counting_allocator(allocator_);
  ObTableScanParam counting_scan_param;
  ObStoreCtx counting_store_ctx;
  ObTableAccessContext counting_access_ctx;
  prepare_ivf_scan_param(
      scan_param_, counting_allocator, counting_scan_param);
  ASSERT_EQ(OB_SUCCESS, init_ivf_access_context(
      sstable_, counting_scan_param, counting_store_ctx, counting_access_ctx));
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(
        sstable_, counting_scan_param, counting_access_ctx, searcher.get()));
    ASSERT_NE(nullptr, searcher.get());
    ASSERT_EQ(1, counting_allocator.alloc_count_);
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, results));
    ASSERT_EQ(0, results.count());
    // TopK scratch uses the round arena. The caller only owns the Searcher,
    // without a Reader or CentroidIterator for the empty SSTable.
    ASSERT_EQ(1, counting_allocator.alloc_count_);
    ASSERT_EQ(0, counting_allocator.free_count_);
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, results));
    ASSERT_EQ(0, results.count());
    ASSERT_EQ(1, counting_allocator.alloc_count_);
    ASSERT_EQ(0, counting_allocator.free_count_);
  }
  ASSERT_EQ(1, counting_allocator.alloc_count_);
  ASSERT_EQ(1, counting_allocator.free_count_);
}

TEST_F(TestIVFSSTableWriter,
    ivf_searcher_factory_treats_empty_meta_as_empty_with_physical_data)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_CENTROID, 0, 1, 0));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(8));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_EQ(1, sstable_.get_row_count());
  ASSERT_GT(sstable_.get_data_macro_block_count(), 0);
  ASSERT_FALSE(sstable_.no_data_to_read());

  sstable_.ivf_meta_.set_empty();
  ASSERT_TRUE(sstable_.get_ivf_meta().is_empty());

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[8] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, ARRAYSIZEOF(query), 1);
  ObIvfCountingAllocator counting_allocator(allocator_);
  ObTableScanParam counting_scan_param;
  ObStoreCtx counting_store_ctx;
  ObTableAccessContext counting_access_ctx;
  prepare_ivf_scan_param(
      scan_param_, counting_allocator, counting_scan_param);
  ASSERT_EQ(OB_SUCCESS, init_ivf_access_context(
      sstable_, counting_scan_param, counting_store_ctx, counting_access_ctx));
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(
        sstable_, counting_scan_param, counting_access_ctx, searcher.get()));
    ASSERT_NE(nullptr, searcher.get());
    // The factory allocated only the DiskBBQ Searcher: no Reader was created.
    ASSERT_EQ(1, counting_allocator.alloc_count_);
    ASSERT_EQ(0, counting_allocator.free_count_);
    ASSERT_EQ(1, counting_allocator.live_allocation_count());
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
    ASSERT_EQ(0, result.count());
    // KNN scratch stays in the round arena; empty metadata also avoids
    // creating a CentroidIterator for the physically non-empty SSTable.
    ASSERT_EQ(1, counting_allocator.alloc_count_);
    ASSERT_EQ(0, counting_allocator.free_count_);
    ASSERT_EQ(1, counting_allocator.live_allocation_count());
  }
  ASSERT_EQ(1, counting_allocator.alloc_count_);
  ASSERT_EQ(1, counting_allocator.free_count_);
  ASSERT_EQ(0, counting_allocator.live_allocation_count());
}

TEST_F(TestIVFSSTableWriter,
    ivf_searcher_accepts_empty_deletion_bitmap_handle)
{
  const int32_t dim = 64;
  const int64_t cid = 0;
  const int64_t vid = 10;
  float centroid[dim] = {0};
  float vector[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  vector[0] = 0.1f;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vector, &cid, &vid, 1, centroid, 1, dim, flip, 1));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  ObIvfSSTableReadStats read_stats;
  query_param.read_stats_ = &read_stats;
  ObDeletionBitmapHandle invalid_deletion_handle;
  query_param.deletion_bitmap_handle_ = &invalid_deletion_handle;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));

  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(1, read_stats.low_precision_vector_scan_count_);
}

TEST_F(TestIVFSSTableWriter, ivf_searchers_handle_different_queries_independently)
{
  const int32_t dim = 64;
  const int64_t vector_count = 2;
  const int64_t cids[vector_count] = {0, 0};
  const int64_t vids[vector_count] = {10, 20};
  float centroid[dim] = {0};
  float vectors[vector_count * dim] = {0};
  float query_a[dim] = {0};
  float query_b[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  vectors[0] = 3.0F;
  vectors[dim] = -1.0F;
  query_a[0] = 2.0F;
  query_b[0] = -2.0F;

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vectors, cids, vids, vector_count, centroid, 1, dim, flip, 1,
      nullptr, share::VIDA_IP, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  ObIvfKnnSearchParam query_param_a;
  ObIvfKnnSearchParam query_param_b;
  fill_search_params(query_param_a, query_a, dim, 1);
  fill_search_params(query_param_b, query_b, dim, 2);
  ObIvfKnnSearchResult result_a;
  ObIvfKnnSearchResult result_b;
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param_a, result_a));
  }
  ASSERT_EQ(1, result_a.count());
  ASSERT_EQ(10, result_a.at(0).vid_);
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param_b, result_b));
  }
  ASSERT_EQ(2, result_b.count());
  ASSERT_EQ(20, result_b.at(0).vid_);
  ASSERT_NE(result_a.at(0).vid_, result_b.at(0).vid_);
}

TEST(TestDiskBBQAutoSearch, budget_with_minimum_candidates_and_bounds)
{
  EXPECT_EQ(ObDiskBBQSearcher::compute_auto_scan_budget(10, 110, 1000000),
      ObDiskBBQSearcher::compute_auto_scan_budget(10, 10000, 1000000));
  EXPECT_EQ(2, ObDiskBBQSearcher::compute_auto_scan_budget(1, 4, 1));
  EXPECT_EQ(32, ObDiskBBQSearcher::compute_auto_scan_budget(100, 400, 16));
  EXPECT_EQ(80, ObDiskBBQSearcher::compute_auto_scan_budget(10, 40, 128));
  EXPECT_EQ(800, ObDiskBBQSearcher::compute_auto_scan_budget(100, 400, 512));
  EXPECT_EQ(0, ObDiskBBQSearcher::compute_auto_scan_budget(0, 4, 100));
  EXPECT_EQ(0, ObDiskBBQSearcher::compute_auto_scan_budget(10, 4, 100));
  EXPECT_EQ(0, ObDiskBBQSearcher::compute_auto_scan_budget(1, 4, 0));
  EXPECT_EQ(INT64_MAX, ObDiskBBQSearcher::compute_auto_scan_budget(INT64_MAX, INT64_MAX, INT64_MAX));
}

TEST(TestDiskBBQAutoSearch, budget_caps_large_segment_scan)
{
  const int64_t k = 100;
  const int64_t num_candidates = 400;
  const int64_t reference_budget = ObDiskBBQSearcher::compute_auto_scan_budget(k, num_candidates, 1000000);
  const int64_t large_budget = ObDiskBBQSearcher::compute_auto_scan_budget(k, num_candidates, 10000000);
  EXPECT_EQ(47926, reference_budget);
  EXPECT_EQ(402015, large_budget);
  EXPECT_GT(large_budget, reference_budget);
  EXPECT_EQ(large_budget, ObDiskBBQSearcher::compute_auto_scan_budget(k, 300, 10000000));
  EXPECT_EQ(large_budget, ObDiskBBQSearcher::compute_auto_scan_budget(k, 1100, 10000000));

  const int64_t sizes[] = {1, 16, 128, 512, 10000, 999999, 1000000, 1000001,
      10000000, 100000000, INT64_MAX};
  int64_t previous_budget = 0;
  for (const int64_t size : sizes) {
    SCOPED_TRACE(size);
    const int64_t budget = ObDiskBBQSearcher::compute_auto_scan_budget(k, num_candidates, size);
    EXPECT_GE(budget, previous_budget);
    EXPECT_GE(budget, 2 * std::min(num_candidates, size));
    EXPECT_LE(budget, size > INT64_MAX / 2 ? INT64_MAX : 2 * size);
    previous_budget = budget;
  }
  EXPECT_LE(ObDiskBBQSearcher::compute_auto_scan_budget(k, num_candidates, 1000001)
      - ObDiskBBQSearcher::compute_auto_scan_budget(k, num_candidates, 999999), 1);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_auto_tiny_segment_returns_all_available_candidates)
{
  const int32_t dim = 64;
  const int64_t count = 16;
  float query[dim] = {0};
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = vids[i] = i;
    vectors[i * dim] = 0.01F * (i + 1);
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, vectors.data(), count, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  sstable_.ivf_meta_.vector_count_ = count;
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObRoaringBitmap allowed(&allocator_);
  for (int64_t i = 1; i < count; i += 2) { ASSERT_EQ(OB_SUCCESS, allowed.value_add(i)); }
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  for (const bool filtered : {false, true}) {
    ObIvfKnnSearchParam param;
    fill_search_params(param, query, dim, 100);
    param.nprobe_ = 0;
    param.original_k_ = 100;
    param.num_candidates_ = 400;
    param.pre_filter_ = filtered ? &filter : nullptr;
    ObIvfSSTableReadStats stats;
    param.read_stats_ = &stats;
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
    ObIvfKnnSearchResult result;
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
    ASSERT_EQ(filtered ? count / 2 : count, result.count());
    for (int64_t i = 0; i < result.count(); ++i) {
      ASSERT_EQ(filtered ? 2 * i + 1 : i, result.at(i).vid_);
    }
    ASSERT_EQ(filtered ? count / 2 : count, stats.low_precision_vector_count_);
    ASSERT_FALSE(searcher->has_remaining_centroids());
    auto *disk = static_cast<ObDiskBBQSearcher *>(searcher.get());
    ASSERT_EQ(count, disk->auto_nprobe_);
    ASSERT_FALSE(disk->need_init_auto_nprobe_);
  }
}

class TestIVFAutoPreFilterBudget : public TestIVFSSTableWriter,
    public ::testing::WithParamInterface<int32_t>
{};

INSTANTIATE_TEST_CASE_P(FlatAndHierarchical, TestIVFAutoPreFilterBudget, ::testing::Values(0, 8));

TEST_P(TestIVFAutoPreFilterBudget, auto_pre_budget_preserves_centroid_windows)
{
  const int32_t dim = 64;
  const int64_t nlist = 64;
  const int64_t per_center = 64;
  const int64_t count = nlist * per_center;
  const int32_t parents = GetParam();
  scan_context_.ls_id_ = share::ObLSID(419800 + parents);
  std::vector<float> vectors(count * dim, 0.0F), centers(nlist * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  float query[dim] = {};
  for (int64_t cid = 0; cid < nlist; ++cid) { centers[cid * dim] = cid * 2.0F; }
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / per_center;
    vids[i] = i;
    vectors[i * dim] = centers[cids[i] * dim] + 0.1F;
  }
  CentroidCacheMemoryPercentageGuard cache;
  CentroidHgraphThresholdGuard graph;
  ASSERT_TRUE(cache.config_.is_valid());
  ASSERT_TRUE(graph.config_.is_valid());
  graph.config_->_disk_bbq_centroid_hgraph_threshold = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(), count,
      centers.data(), nlist, dim, nullptr, 1, nullptr, share::VIDA_L2, true,
      nullptr, nullptr, 0, false, parents));
  sstable_.ivf_meta_.vector_count_ = count;
  sstable_.ivf_meta_.centroid_count_ = nlist;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(count - 1));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  auto *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(scan_context_.ls_id_, ls_mgr, false));
  for (int64_t percentage : {0, 80}) {
    cache.config_->_disk_bbq_centroid_cache_mem_percentage = percentage;
    for (bool pull : {false, true}) {
      SCOPED_TRACE(percentage);
      SCOPED_TRACE(pull);
      ObIvfSearcherGuard searcher;
      ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
      auto *disk = static_cast<ObDiskBBQSearcher *>(searcher.get());
      ObIvfKnnSearchParam param;
      fill_search_params(param, query, dim, 1);
      param.nprobe_ = 0;
      param.original_k_ = 1;
      param.num_candidates_ = 4;
      param.is_set_refine_k_ = true;
      param.cluster_size_ = per_center;
      param.pre_filter_ = &filter;
      // PRE callers need not declare SQL iterative filtering: the scanner's
      // automatic budget itself requires a resumable centroid stream.
      ASSERT_FALSE(param.iterative_centroids_);
      ObIvfSSTableReadStats stats;
      param.read_stats_ = &stats;
      ObIvfKnnSearchResult result;
      const int64_t budget = ObDiskBBQSearcher::compute_auto_scan_budget(1, 4, count);
      ASSERT_LT(budget, (nlist - 8) * per_center);
      if (!pull) {
        ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
        ASSERT_EQ(1, result.count());
        EXPECT_EQ(count - 1, result.at(0).vid_);
      } else {
        ObIvfCandidateProgress progress;
        int64_t returned = 0, calls = 0, started = 0;
        do {
          ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
              std::numeric_limits<float>::infinity(), result, progress, 32));
          started += progress.centroids_started_;
          for (int64_t i = 0; i < result.count(); ++i) {
            EXPECT_EQ(count - 1, result.at(i).vid_);
            ++returned;
          }
          ASSERT_LT(++calls, 3 * nlist);
        } while (progress.state_ == ObIvfCandidateProgress::MORE);
        EXPECT_EQ(ObIvfCandidateProgress::DOMAIN_END, progress.state_);
        EXPECT_EQ(1, returned);
        EXPECT_EQ(nlist, started);
      }
      EXPECT_EQ(nlist, disk->auto_nprobe_);
      EXPECT_FALSE(disk->is_pre_filter_budget_scan());
      EXPECT_FALSE(searcher->has_remaining_centroids());
      EXPECT_GE(stats.low_precision_vector_count_, 1);
      EXPECT_LE(stats.low_precision_vector_count_, ObDistBBQFastScanBatch::CAPACITY);
      EXPECT_EQ(1, stats.raw_vector_read_count_);
      {
        ObIvfCacheMgrGuard root_guard;
        ASSERT_EQ(percentage == 0 ? OB_HASH_NOT_EXIST : OB_SUCCESS,
            service->acquire_ivf_cache_mgr_guard(scan_context_.ls_id_,
                ObIvfCacheMgrKey(sstable_.get_key(), 0), root_guard));
        if (percentage > 0) {
          ObIvfSSTableCentCache *root_cache = nullptr;
          ASSERT_EQ(OB_SUCCESS, root_guard.get_ivf_cache_mgr()->get_cache_node<ObIvfSSTableCentCache>(
              IVF_SSTABLE_CENTROID_CACHE, root_cache));
          ASSERT_NE(nullptr, root_cache);
          EXPECT_TRUE(root_cache->is_completed());
        }
      }
      stats.reset();
      if (!pull) {
        ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
      } else {
        ObIvfCandidateProgress progress;
        ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
            std::numeric_limits<float>::infinity(), result, progress, 32));
        EXPECT_EQ(ObIvfCandidateProgress::DOMAIN_END, progress.state_);
      }
      EXPECT_EQ(0, result.count());
      EXPECT_EQ(0, stats.low_precision_vector_count_);
      // A manual probe remains a bounded first window despite the PRE bitmap.
      searcher->reuse();
      param.nprobe_ = 1;
      ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
      EXPECT_EQ(0, result.count());
      EXPECT_EQ(-1, disk->remaining_pre_filter_budget_);

      // With all VIDs allowed, B is met inside a CID. Finish that CID, retain
      // the center cursor, then use the measured count for the next window.
      searcher->reuse();
      allowed.set_empty();
      for (int64_t vid : vids) { ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid)); }
      param.nprobe_ = 0;
      const int64_t expected_centers = (budget + per_center - 1) / per_center;
      ASSERT_LT(2 * expected_centers, nlist);
      ASSERT_NE(0, budget % per_center);
      for (int64_t window = 0; window < 2; ++window) {
        stats.reset();
        if (!pull) {
          ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
          ASSERT_EQ(1, result.count());
          EXPECT_GE(result.at(0).vid_, window * expected_centers * per_center);
          EXPECT_LT(result.at(0).vid_, (window * expected_centers + 1) * per_center);
        } else {
          ObIvfCandidateProgress progress;
          int64_t returned = 0, calls = 0, started = 0;
          do {
            ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
                std::numeric_limits<float>::infinity(), result, progress, 32));
            started += progress.centroids_started_;
            for (int64_t i = 0; i < result.count(); ++i) {
              EXPECT_GE(result.at(i).vid_, window * expected_centers * per_center);
              EXPECT_LT(result.at(i).vid_, (window + 1) * expected_centers * per_center);
              ++returned;
            }
            ASSERT_LT(++calls, 3 * nlist);
          } while (progress.state_ == ObIvfCandidateProgress::MORE);
          EXPECT_EQ(ObIvfCandidateProgress::WINDOW_END, progress.state_);
          EXPECT_EQ(expected_centers, started);
          EXPECT_EQ(expected_centers * per_center, returned);
        }
        EXPECT_EQ(expected_centers, disk->auto_nprobe_);
        EXPECT_EQ(expected_centers * per_center, stats.low_precision_vector_count_);
        EXPECT_FALSE(disk->is_pre_filter_budget_scan());
        EXPECT_TRUE(searcher->has_remaining_centroids());
      }
      allowed.set_empty();
      ASSERT_EQ(OB_SUCCESS, allowed.value_add(count - 1));
    }
  }
  clear_hierarchical_test_cache(scan_context_.ls_id_, sstable_.get_key(), parents);
}

TEST(ObDiskBBQSearcherAutoRefineTest, candidate_target_tracks_vector_count)
{
  constexpr int64_t k = 10;
  EXPECT_EQ(k, ObDiskBBQSearcher::compute_auto_num_candidates(k, 100));
  EXPECT_EQ(2 * k, ObDiskBBQSearcher::compute_auto_num_candidates(k, 10000));
  EXPECT_EQ(4 * k, ObDiskBBQSearcher::compute_auto_num_candidates(k, 1000000));
  EXPECT_EQ(5 * k, ObDiskBBQSearcher::compute_auto_num_candidates(k, 10000000));
  EXPECT_EQ(8 * k, ObDiskBBQSearcher::compute_auto_num_candidates(k, INT64_MAX));
  EXPECT_EQ(INT64_MAX, ObDiskBBQSearcher::compute_auto_num_candidates(INT64_MAX / 2, INT64_MAX));
}

TEST(ObDiskBBQSearcherAutoRefineTest, pre_filter_budget_respects_selectivity_and_size)
{
  const int64_t small_budget = ObDiskBBQSearcher::compute_auto_scan_budget(100, 400, 1000000);
  const int64_t large_budget = ObDiskBBQSearcher::compute_auto_scan_budget(100, 500, 10000000);
  EXPECT_EQ(47926, small_budget);
  EXPECT_EQ(402015, large_budget);
  EXPECT_GE(ObDiskBBQSearcher::adjust_pre_filter_scan_budget(small_budget, 1000000, 0.99), 50000);
  EXPECT_GE(ObDiskBBQSearcher::adjust_pre_filter_scan_budget(small_budget, 1000000, 0.1), 110000);
  EXPECT_GT(ObDiskBBQSearcher::adjust_pre_filter_scan_budget(small_budget, 1000000, 0.1),
      ObDiskBBQSearcher::adjust_pre_filter_scan_budget(small_budget, 1000000, 0.99));
  EXPECT_GT(ObDiskBBQSearcher::adjust_pre_filter_scan_budget(large_budget, 10000000, 0.1), large_budget);
  EXPECT_EQ(small_budget, ObDiskBBQSearcher::adjust_pre_filter_scan_budget(small_budget, 1000000, 0.0));
  EXPECT_EQ(200000, ObDiskBBQSearcher::adjust_pre_filter_scan_budget(100000, 100000, 0.001));

  const int64_t small_post_1p = ObDiskBBQSearcher::adjust_auto_scan_budget(
      small_budget, 1000000, 0.99, false);
  const int64_t small_post_80p = ObDiskBBQSearcher::adjust_auto_scan_budget(
      small_budget, 1000000, 0.2, false);
  const int64_t large_post_1p = ObDiskBBQSearcher::adjust_auto_scan_budget(
      large_budget, 10000000, 0.99, false);
  const int64_t large_post_80p = ObDiskBBQSearcher::adjust_auto_scan_budget(
      large_budget, 10000000, 0.2, false);
  EXPECT_GE(small_post_1p, 59000);
  EXPECT_GE(small_post_80p, 100000);
  EXPECT_LE(small_post_80p, 120000);
  EXPECT_GE(large_post_1p, 135000);
  EXPECT_GE(large_post_1p, 160000);
  EXPECT_LT(large_post_1p, 165000);
  EXPECT_GT(large_post_80p, large_post_1p);
  EXPECT_LT(large_post_80p, large_budget);
  EXPECT_EQ(307015, ObDiskBBQSearcher::adjust_auto_scan_budget(
      large_budget, 10000000, 0.1, false));
  EXPECT_EQ(ObDiskBBQSearcher::adjust_auto_scan_budget(
      large_budget, 10000000, 0.1, false),
      ObDiskBBQSearcher::adjust_auto_scan_budget(
          large_budget, 10000000, 0.01, false));
  EXPECT_GT(ObDiskBBQSearcher::adjust_auto_scan_budget(
      small_budget, 1000000, 0.1, true), small_post_1p);
  const int64_t large_pre_90p = ObDiskBBQSearcher::adjust_auto_scan_budget(
      large_budget, 10000000, 0.1, true);
  const int64_t large_pre_99p = ObDiskBBQSearcher::adjust_auto_scan_budget(
      large_budget, 10000000, 0.01, true);
  EXPECT_GT(large_pre_90p, large_pre_99p);
  EXPECT_NEAR(320724, large_pre_90p, 3);
  EXPECT_NEAR(266767, large_pre_99p, 3);
  const int64_t large_pre_unknown = ObDiskBBQSearcher::adjust_auto_scan_budget(
      large_budget, 10000000, 0.0, true);
  EXPECT_GE(large_pre_unknown, 121602);
  EXPECT_LE(large_pre_unknown, 121604);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_centroid_refill_uses_selected_count)
{
  ObDiskBBQSearcher searcher(allocator_);
  ObIvfKnnSearchParam param;
  param.nprobe_ = 620;
  EXPECT_EQ(620, searcher.get_effective_nprobe(param));
  param.nprobe_ = 0;
  searcher.auto_nprobe_ = 786;
  EXPECT_EQ(786, searcher.get_effective_nprobe(param));
  param.cluster_size_ = 1;
  EXPECT_EQ(786, searcher.get_effective_nprobe(param));
}

TEST_F(TestIVFSSTableWriter, disk_bbq_auto_budget_follows_mainline_tuning)
{
  const int32_t dim = 64;
  float vector[dim] = {}, query[dim] = {}, center[dim] = {};
  vector[0] = 0.1F;
  const int64_t cid = 0, vid = 1, k = 100;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1, center, 1, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  auto *disk = static_cast<ObDiskBBQSearcher *>(searcher.get());
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, k);
  param.nprobe_ = 0;
  param.original_k_ = k;
  param.selectivity_ = 0.1;
  // Only prepare_query is exercised: vary the segment's population without
  // writing those vectors. Small populations distinguish the two candidate
  // targets before the large-segment cap makes their budgets equal.
  for (int64_t population : {10000L, 100000L, 1000000L, 10000000L, 100000000L}) {
    sstable_.ivf_meta_.vector_count_ = population;
    for (float configured_refine : {2.5F, 4.0F}) {
      param.refine_k_ = configured_refine;
      param.num_candidates_ = static_cast<int64_t>(std::ceil(k * configured_refine));
      for (bool explicit_refine : {false, true}) {
        param.is_set_refine_k_ = explicit_refine;
        for (bool pre : {false, true}) {
          SCOPED_TRACE(::testing::Message() << population << ":" << configured_refine
              << ":" << explicit_refine << ":" << pre);
          searcher->reuse();
          param.pre_filter_ = pre ? &filter : nullptr;
          const int64_t candidates = explicit_refine ? param.num_candidates_
              : ObDiskBBQSearcher::compute_auto_num_candidates(k, population);
          int64_t expected = ObDiskBBQSearcher::compute_auto_scan_budget(k, candidates, population);
          if (!explicit_refine) {
            expected = ObDiskBBQSearcher::adjust_auto_scan_budget(expected, population, param.selectivity_, pre);
            if (population == 10000) {
              const int64_t configured_budget = ObDiskBBQSearcher::adjust_auto_scan_budget(
                  ObDiskBBQSearcher::compute_auto_scan_budget(k, param.num_candidates_, population),
                  population, param.selectivity_, pre);
              ASSERT_NE(configured_budget, expected);
            }
          }
          ASSERT_EQ(OB_SUCCESS, disk->prepare_query(param));
          EXPECT_EQ(expected, disk->initial_child_budget_);
          EXPECT_EQ(pre ? expected : -1, disk->remaining_pre_filter_budget_);
          EXPECT_TRUE(disk->need_init_auto_nprobe_);
          EXPECT_FLOAT_EQ(configured_refine, param.refine_k_);
        }
      }
    }
  }
  searcher->reuse();
  param.nprobe_ = 7;
  param.is_set_refine_k_ = false;
  ASSERT_EQ(OB_SUCCESS, disk->prepare_query(param));
  EXPECT_EQ(-1, disk->initial_child_budget_);
  EXPECT_EQ(-1, disk->remaining_pre_filter_budget_);
  EXPECT_FALSE(disk->need_init_auto_nprobe_);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_auto_budget_refill_and_legacy_count)
{
  const int32_t dim = 64;
  const int64_t nlist = 64;
  const int64_t per_center = 8;
  const int64_t count = nlist * per_center;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<float> centroids(nlist * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  float query[dim] = {0};
  for (int64_t cid = 0; cid < nlist; ++cid) { centroids[cid * dim] = cid * 2.0F; }
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / per_center;
    vids[i] = i;
    vectors[i * dim] = centroids[cids[i] * dim] + 0.1F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), nlist, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  // This fixture writes raw rows directly, bypassing the production writers.
  sstable_.ivf_meta_.vector_count_ = count;
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, count);
  param.iterative_centroids_ = true;
  param.original_k_ = 10;
  param.num_candidates_ = 50;
  param.nprobe_ = 0;
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(0));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  param.pre_filter_ = &filter;
  param.selectivity_ = 0.1;
  ObIvfSSTableReadStats stats;
  param.read_stats_ = &stats;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  auto *disk = static_cast<ObDiskBBQSearcher *>(searcher.get());
  ASSERT_EQ(OB_SUCCESS, disk->prepare_query(param));
  EXPECT_EQ(ObDiskBBQSearcher::adjust_auto_scan_budget(
      ObDiskBBQSearcher::compute_auto_scan_budget(10,
          ObDiskBBQSearcher::compute_auto_num_candidates(10, count), count), count, 0.1, true),
      disk->initial_child_budget_);
  searcher->reuse();
  param.is_set_refine_k_ = true;
  ASSERT_EQ(OB_SUCCESS, disk->prepare_query(param));
  EXPECT_EQ(ObDiskBBQSearcher::compute_auto_scan_budget(10, 50, count),
      disk->initial_child_budget_);
  searcher->reuse();
  param.pre_filter_ = nullptr;
  param.is_set_refine_k_ = true;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(104, result.count());
  ASSERT_EQ(104, stats.low_precision_vector_count_);
  ASSERT_EQ(13, disk->auto_nprobe_);
  ASSERT_EQ(0, disk->initial_child_budget_);
  ASSERT_FALSE(disk->need_init_auto_nprobe_);
  std::unordered_set<int64_t> seen;
  for (int64_t i = 0; i < result.count(); ++i) { ASSERT_TRUE(seen.insert(result.at(i).vid_).second); }

  // A second segment/search has its own first-window state, even with shared
  // diagnostics. An exhausted/advanced first segment must not consume its B.
  ObIvfSearcherGuard other;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, other.get()));
  ASSERT_EQ(OB_SUCCESS, other->knn_search(param, result));
  ASSERT_EQ(104, result.count());
  ASSERT_EQ(13, static_cast<ObDiskBBQSearcher *>(other.get())->auto_nprobe_);

  param.k_ = count * 2; // SQL candidate compensation must not recompute B or m.
  int64_t rounds = 1;
  while (searcher->has_remaining_centroids()) {
    stats.reset();
    const int64_t expected_rows = std::min<int64_t>(104, count - seen.size());
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
    ASSERT_EQ(expected_rows, result.count());
    ASSERT_EQ(expected_rows, stats.low_precision_vector_count_);
    ASSERT_EQ(0, disk->initial_child_budget_);
    ASSERT_EQ(13, disk->auto_nprobe_);
    for (int64_t i = 0; i < result.count(); ++i) { ASSERT_TRUE(seen.insert(result.at(i).vid_).second); }
    ASSERT_LE(++rounds, nlist);
  }
  ASSERT_EQ(count, seen.size());

  searcher->reuse();
  param.original_k_ = 0;
  param.num_candidates_ = 0;
  param.nprobe_ = 8;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(64, result.count());
  ASSERT_EQ(-1, disk->initial_child_budget_);

  // Manual nprobe must not depend on unused automatic candidate parameters.
  searcher->reuse();
  stats.reset();
  ObIvfKnnSearchParam manual_param = param;
  manual_param.k_ = 10;
  manual_param.nprobe_ = 10;
  manual_param.original_k_ = 10;
  manual_param.num_candidates_ = 0;
  ASSERT_TRUE(manual_param.is_valid());
  ASSERT_FALSE(manual_param.is_auto_search());
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(manual_param, result));
  ASSERT_EQ(10, result.count());
  ASSERT_EQ(10 * per_center, stats.low_precision_vector_count_);
  ASSERT_EQ(-1, disk->initial_child_budget_);
  manual_param.nprobe_ = 0;
  ASSERT_FALSE(manual_param.is_valid());

  param.nprobe_ = 0;
  param.original_k_ = 10;
  param.num_candidates_ = 50;
  param.read_stats_ = &stats;
  // Old serialized metadata decodes vector_count as zero. Recover a budget
  // from RAW_VECTOR index rows without rewriting the metadata or counting all
  // centroid/posting rows as original vectors.
  sstable_.ivf_meta_.vector_count_ = 0;
  searcher->reuse();
  stats.reset();
  int64_t legacy_count = 0;
  ASSERT_EQ(OB_SUCCESS, disk->estimate_legacy_vector_count(legacy_count));
  ASSERT_GE(legacy_count, count);
  ASSERT_LT(legacy_count, count * 2);
  ASSERT_EQ(0, stats.low_precision_vector_count_);
  ASSERT_EQ(0, stats.raw_vector_read_count_);
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(104, result.count());
  ASSERT_EQ(13, disk->auto_nprobe_);
  ASSERT_EQ(0, sstable_.ivf_meta_.vector_count_);
  searcher->reuse();
  stats.reset();
  ObIvfCandidateProgress legacy_progress;
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), result, legacy_progress));
  ASSERT_GT(result.count(), 0);
  ASSERT_GT(stats.low_precision_vector_count_, 0);
  ASSERT_EQ(0, sstable_.ivf_meta_.vector_count_);

  // Negative metadata remains invalid; do not hide corruption behind fallback.
  for (const int64_t vector_count : {int64_t(-1)}) {
    SCOPED_TRACE(vector_count);
    sstable_.ivf_meta_.vector_count_ = vector_count;
    searcher->reuse();
    stats.reset();
    ASSERT_EQ(OB_INVALID_DATA, searcher->knn_search(param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(0, stats.low_precision_vector_count_);
    ASSERT_EQ(-1, disk->initial_child_budget_);
    ASSERT_FALSE(disk->need_init_auto_nprobe_);

    searcher->reuse();
    ObIvfCandidateProgress progress;
    ASSERT_EQ(OB_INVALID_DATA, searcher->next_candidate_batch(param,
        std::numeric_limits<float>::infinity(), result, progress));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(0, progress.postings_scanned_);
    ASSERT_EQ(-1, disk->initial_child_budget_);
    ASSERT_FALSE(disk->need_init_auto_nprobe_);
  }
  sstable_.ivf_meta_.vector_count_ = count;
  searcher->reuse();
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(104, result.count());
  ASSERT_EQ(0, disk->initial_child_budget_);

  // A canonical empty segment legitimately has no vector count and needs no budget.
  const ObIvfSSTableMeta nonempty_meta = sstable_.ivf_meta_;
  searcher->reuse();
  sstable_.ivf_meta_.set_empty();
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(0, result.count());
  searcher->reuse();
  ObIvfCandidateProgress progress;
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), result, progress));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(ObIvfCandidateProgress::DOMAIN_END, progress.state_);
  sstable_.ivf_meta_ = nonempty_meta;
}

TEST_F(TestIVFSSTableWriter, disk_bbq_auto_filtering_preserves_scan_windows)
{
  const int32_t dim = 64;
  const int64_t nlist = 64;
  const int64_t per_center = 8;
  const int64_t count = nlist * per_center;
  std::vector<float> vectors(count * dim, 0.0F), centroids(nlist * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  float query[dim] = {0};
  for (int64_t cid = 0; cid < nlist; ++cid) { centroids[cid * dim] = cid * 2.0F; }
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / per_center;
    vids[i] = i;
    vectors[i * dim] = centroids[cids[i] * dim] + 0.1F;
  }
  // The fourth CID has a closer eligible vector than the first CID.
  vectors[3 * per_center * dim] = 0.01F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), nlist, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  sstable_.ivf_meta_.vector_count_ = count;
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  param.iterative_centroids_ = true;
  param.original_k_ = 1;
  param.num_candidates_ = 5;
  param.nprobe_ = 0;
  param.is_set_refine_k_ = true; // Keep this window/refill test on its explicit candidate budget.
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(73));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  param.pre_filter_ = &filter;
  ObIvfSSTableReadStats stats;
  param.read_stats_ = &stats;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  auto *disk = static_cast<ObDiskBBQSearcher *>(searcher.get());
  ObIvfKnnSearchResult result;
  const int64_t auto_budget = ObDiskBBQSearcher::compute_auto_scan_budget(
      param.original_k_, param.num_candidates_, count);
  // PRE skips empty batches without spending the budget. With only one
  // matching CID, the first window reaches domain end and returns its match.
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(73, result.at(0).vid_);
  ASSERT_EQ(nlist, disk->auto_nprobe_);
  ASSERT_GE(stats.low_precision_vector_count_, 1);
  ASSERT_LE(stats.low_precision_vector_count_, per_center);
  ASSERT_EQ(1, stats.low_precision_vector_count_);
  ASSERT_LT(stats.low_precision_vector_count_, auto_budget);
  ASSERT_EQ(-1, disk->remaining_pre_filter_budget_);
  ASSERT_FALSE(searcher->has_remaining_centroids());
  ASSERT_FALSE(disk->is_pre_filter_budget_scan());
  ASSERT_FALSE(disk->need_init_auto_nprobe_);

  searcher->reuse();
  allowed.set_empty();
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(0));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(3 * per_center));
  stats.reset();
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(3 * per_center, result.at(0).vid_);
  ASSERT_EQ(nlist, disk->auto_nprobe_);
  ASSERT_GE(stats.low_precision_vector_count_, 2);
  ASSERT_LE(stats.low_precision_vector_count_, 2 * per_center);
  ASSERT_EQ(2, stats.low_precision_vector_count_);
  ASSERT_EQ(-1, disk->remaining_pre_filter_budget_);
  ASSERT_FALSE(searcher->has_remaining_centroids());
  ASSERT_FALSE(disk->is_pre_filter_budget_scan());
  ASSERT_FALSE(disk->need_init_auto_nprobe_);

  // A finite POST threshold still prunes candidates, but must not shorten
  // the first budgeted window or a subsequent m-centroid refill window.
  searcher->reuse();
  param.pre_filter_ = nullptr;
  param.original_k_ = 1;
  param.num_candidates_ = 5;
  param.k_ = 10; // Compensated heap size must not inflate the automatic budget.
  stats.reset();
  ObIvfCandidateProgress progress;
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), result, progress, 32));
  ASSERT_EQ(per_center, result.count());
  ASSERT_EQ(ObIvfCandidateProgress::MORE, progress.state_);
  ASSERT_FALSE(disk->need_init_auto_nprobe_); // Metadata resolved the window before posting reads.
  bool found_closer = false;
  for (int64_t window = 0; window < 2; ++window) {
    int64_t calls = 0;
    do {
      ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param, 1.0F, result, progress, 32));
      for (int64_t i = 0; i < result.count(); ++i) {
        if (3 * per_center == result.at(i).vid_) { found_closer = true; }
      }
      ASSERT_LT(++calls, 16);
    } while (ObIvfCandidateProgress::MORE == progress.state_);
    ASSERT_EQ(ObIvfCandidateProgress::WINDOW_END, progress.state_);
    ASSERT_EQ(4, disk->auto_nprobe_);
    ASSERT_EQ((window + 1) * 4 * per_center, stats.low_precision_vector_count_);
    ASSERT_EQ(0, disk->initial_child_budget_);
    ASSERT_FALSE(disk->need_init_auto_nprobe_);
  }
  ASSERT_TRUE(found_closer);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_filter_empty_and_short_results_exhaust_auto_and_manual_ranges)
{
  const int32_t dim = 64;
  const int64_t nlist = 16, per_center = 8, count = nlist * per_center;
  std::vector<float> vectors(count * dim, 0.0F), centroids(nlist * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  float query[dim] = {0};
  for (int64_t cid = 0; cid < nlist; ++cid) { centroids[cid * dim] = 2.0F * cid; }
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / per_center;
    vids[i] = i;
    vectors[i * dim] = centroids[cids[i] * dim] + 0.1F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), nlist, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  sstable_.ivf_meta_.vector_count_ = count;
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  for (const int64_t nprobe : {0, 3, 1000}) {
    for (const bool empty : {false, true}) {
      for (const bool post : {false, true}) {
        SCOPED_TRACE(nprobe);
        SCOPED_TRACE(empty);
        SCOPED_TRACE(post);
        ObRoaringBitmap allowed(&allocator_);
        if (!empty) {
          ASSERT_EQ(OB_SUCCESS, allowed.value_add(1));
          ASSERT_EQ(OB_SUCCESS, allowed.value_add(111));
        }
        ObIvfPreFilter filter;
        ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
        ObIvfKnnSearchParam param;
        fill_search_params(param, query, dim, 10);
  param.iterative_centroids_ = true;
        param.nprobe_ = nprobe;
        if (0 == nprobe) {
          param.original_k_ = 10;
          param.num_candidates_ = 40;
          param.is_set_refine_k_ = true; // Keep the expected ten-centroid window independent of auto tuning.
        }
        param.pre_filter_ = post ? nullptr : &filter;
        ObIvfSSTableReadStats stats;
        param.read_stats_ = &stats;
        ObIvfSearcherGuard searcher;
        ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
        std::unordered_set<int64_t> accepted;
        bool more = true;
        int64_t calls = 0;
        do {
          ObIvfKnnSearchResult result;
          if (post) {
            ObIvfCandidateProgress progress;
            ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
                std::numeric_limits<float>::infinity(), result, progress, 32));
            more = ObIvfCandidateProgress::DOMAIN_END != progress.state_;
          } else {
            ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
            more = searcher->has_remaining_centroids();
          }
          for (int64_t i = 0; i < result.count(); ++i) {
            const int64_t vid = result.at(i).vid_;
            if (!post) { ASSERT_TRUE(filter.contains(vid)); }
            if (filter.contains(vid)) { ASSERT_TRUE(accepted.insert(vid).second); }
          }
          ASSERT_LT(++calls, 256);
        } while (more);
        ASSERT_EQ(empty ? 0 : 2, accepted.size());
        if (!empty) { ASSERT_EQ(1, accepted.count(1)); ASSERT_EQ(1, accepted.count(111)); }
        if (!post && empty) {
          ASSERT_EQ(0, stats.low_precision_vector_count_);
        } else if (0 == nprobe && !post) {
          ASSERT_GE(stats.low_precision_vector_count_, accepted.size());
          ASSERT_LT(stats.low_precision_vector_count_, count);
        } else if (post) {
          ASSERT_EQ(count, stats.low_precision_vector_count_);
        } else {
          ASSERT_GE(stats.low_precision_vector_count_, accepted.size());
          ASSERT_LE(stats.low_precision_vector_count_, count);
        }
        if (0 == nprobe) {
          auto *disk = static_cast<ObDiskBBQSearcher *>(searcher.get());
          if (!post && empty) {
            ASSERT_EQ(0, disk->auto_nprobe_);
          } else if (!post) {
            ASSERT_GE(disk->auto_nprobe_, 10);
            ASSERT_LE(disk->auto_nprobe_, nlist);
          } else {
            ASSERT_EQ(10, disk->auto_nprobe_);
          }
          ASSERT_FALSE(disk->is_pre_filter_budget_scan());
          ASSERT_FALSE(disk->need_init_auto_nprobe_);
        }
      }
    }
  }
}

TEST_F(TestIVFSSTableWriter, disk_bbq_auto_post_keeps_budget_across_batches_and_fixed_refill)
{
  const int32_t dim = 64;
  const int64_t nlist = 3;
  const int64_t per_center = 300;
  const int64_t count = nlist * per_center;
  std::vector<float> vectors(count * dim, 0.0F), centroids(nlist * dim, 0.0F);
  std::vector<int64_t> cids(count), vids(count);
  float query[dim] = {0};
  for (int64_t cid = 0; cid < nlist; ++cid) { centroids[cid * dim] = cid * 2.0F; }
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / per_center;
    vids[i] = i;
    vectors[i * dim] = centroids[cids[i] * dim] + 0.1F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), nlist, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  sstable_.ivf_meta_.vector_count_ = count;
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 10);
  param.original_k_ = 10;
  param.num_candidates_ = 50;
  param.nprobe_ = 0;
  param.is_set_refine_k_ = true; // Spend the explicit budget inside the first CID and refill one CID at a time.
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  auto *disk = static_cast<ObDiskBBQSearcher *>(searcher.get());
  std::unordered_set<int64_t> seen;
  int64_t windows = 0, started = 0, scanned = 0;
  for (int64_t call = 0; call < 100; ++call) {
    ObIvfKnnSearchResult result;
    ObIvfCandidateProgress progress;
    ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
        std::numeric_limits<float>::infinity(), result, progress, 32));
    started += progress.centroids_started_;
    scanned += progress.postings_scanned_;
    for (int64_t i = 0; i < result.count(); ++i) { ASSERT_TRUE(seen.insert(result.at(i).vid_).second); }
    if (0 == windows && ObIvfCandidateProgress::MORE == progress.state_) {
      ASSERT_FALSE(disk->need_init_auto_nprobe_); // Metadata fixed the window; POST still retains the CID tail.
      ASSERT_EQ(1, disk->auto_nprobe_);
    }
    if (ObIvfCandidateProgress::MORE != progress.state_) {
      ++windows;
      ASSERT_EQ(1, disk->auto_nprobe_);
      ASSERT_EQ(windows * per_center, scanned);
    }
    if (ObIvfCandidateProgress::DOMAIN_END == progress.state_) { break; }
  }
  ASSERT_EQ(nlist, windows);
  ASSERT_EQ(nlist, started);
  ASSERT_EQ(count, seen.size());
  ASSERT_EQ(0, disk->initial_child_budget_);
  ASSERT_FALSE(disk->need_init_auto_nprobe_);
}

TEST_F(TestIVFSSTableWriter, ivf_scan_budget_preserves_first_round_and_stops_additional_batches)
{
  const int32_t dim = 64;
  const int32_t nlist = 3;
  const int64_t rows_per_center = ObDistBBQFastScanBatch::CAPACITY + 5;
  const int64_t vector_count = rows_per_center * nlist;
  std::vector<int64_t> cids(vector_count);
  std::vector<int64_t> vids(vector_count);
  std::vector<float> vectors(vector_count * dim, 0.0F);
  float centroids[nlist * dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < nlist; ++i) {
    centroids[i * dim] = static_cast<float>(i * 10);
  }
  for (int64_t i = 0; i < vector_count; ++i) {
    cids[i] = i / rows_per_center;
    vids[i] = i;
    vectors[i * dim] = centroids[cids[i] * dim] + 0.1F + (i % rows_per_center) * 0.001F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      vector_count, centroids, nlist, dim, flip, 1));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  for (int mode = 0; mode < 4; ++mode) {
    ObIvfSSTableReadStats stats;
    // Unlimited, RECALL_FIRST, LATENCY_FIRST with/without read statistics.
    // RAW_VECTOR refinement must not consume this budget a second time.
    ObIvfScanBudget budget(mode == 0 ? 0 : rows_per_center + 1);
    ObIvfKnnSearchParam param;
    fill_search_params(param, query, dim, vector_count);
    param.iterative_centroids_ = true;
    param.read_stats_ = mode == 3 ? nullptr : &stats;
    param.scan_budget_ = &budget;
    param.nprobe_ = 1;
    ObIvfKnnSearchResult result;
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
    ASSERT_EQ(rows_per_center, budget.scanned_vectors_);
    ASSERT_EQ(mode == 3 ? 0 : rows_per_center, stats.low_precision_vector_count_);
    ASSERT_EQ(mode == 3 ? 0 : 1, stats.low_precision_vector_scan_count_);
    ASSERT_EQ(rows_per_center, result.count());
    ASSERT_FALSE(budget.is_exhausted());
    if (mode == 2) {
      // Resetting optional statistics must not restore the consumed budget.
      stats.reset();
      ASSERT_EQ(rows_per_center, budget.scanned_vectors_);
    }
    budget.allow_batch_stop_ = mode >= 2;
    param.nprobe_ = 2;
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
    if (mode >= 2) {
      ASSERT_GT(budget.scanned_vectors_, budget.max_scan_vectors_);
      // Stopping only between CIDs would consume all of the second center.
      ASSERT_LT(budget.scanned_vectors_, rows_per_center * 2);
      ASSERT_GT(result.count(), 0);
      ASSERT_EQ(budget.scanned_vectors_ - rows_per_center, result.count());
      ASSERT_EQ(mode == 3 ? 0 : result.count(), stats.low_precision_vector_count_);
      // Exhausting the budget must also stop opening and prefetching later CIDs.
      ASSERT_EQ(mode == 3 ? 0 : 1, stats.low_precision_vector_scan_count_);
      ASSERT_TRUE(budget.should_stop());
      const int64_t scanned = budget.scanned_vectors_;
      // A second Segment uses the same exhausted context and performs no scan.
      ObIvfSSTableReadStats next_segment_stats;
      param.read_stats_ = &next_segment_stats;
      ObIvfSearcherGuard next_segment;
      ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, next_segment.get()));
      ASSERT_EQ(OB_SUCCESS, next_segment->knn_search(param, result));
      ASSERT_EQ(0, result.count());
      ASSERT_EQ(scanned, budget.scanned_vectors_);
      ASSERT_EQ(0, next_segment_stats.low_precision_vector_count_);
      ASSERT_EQ(0, next_segment_stats.low_precision_vector_scan_count_);
    } else {
      ASSERT_EQ(vector_count, budget.scanned_vectors_);
      ASSERT_EQ(vector_count, stats.low_precision_vector_count_);
      ASSERT_EQ(nlist, stats.low_precision_vector_scan_count_);
      ASSERT_EQ(rows_per_center * 2, result.count());
      ASSERT_EQ(mode != 0, budget.is_exhausted());
    }
  }
  ObIvfSSTableReadStats first_round_stats;
  ObIvfScanBudget first_round_budget(1);
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, vector_count);
  param.nprobe_ = nlist;
  param.read_stats_ = &first_round_stats;
  param.scan_budget_ = &first_round_budget;
  ObIvfKnnSearchResult result;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
  ASSERT_EQ(vector_count, first_round_stats.low_precision_vector_count_);
  ASSERT_EQ(nlist, first_round_stats.low_precision_vector_scan_count_);
  ASSERT_EQ(vector_count, first_round_budget.scanned_vectors_);
  ASSERT_EQ(vector_count, result.count());
  // Separate Segment statistics share one partition budget.
  ObIvfSSTableReadStats next_segment_stats;
  param.read_stats_ = &next_segment_stats;
  ObIvfSearcherGuard next_segment;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, next_segment.get()));
  ASSERT_EQ(OB_SUCCESS, next_segment->knn_search(param, result));
  ASSERT_EQ(vector_count, first_round_stats.low_precision_vector_count_);
  ASSERT_EQ(vector_count, next_segment_stats.low_precision_vector_count_);
  ASSERT_EQ(nlist, next_segment_stats.low_precision_vector_scan_count_);
  ASSERT_EQ(vector_count * 2, first_round_budget.scanned_vectors_);
  ASSERT_EQ(vector_count, result.count());
}

TEST(TestIvfScanBudget, strict_limit_and_partition_reset)
{
  ObIvfScanBudget budget(10);
  budget.allow_batch_stop_ = true;
  budget.consume(10);
  EXPECT_FALSE(budget.should_stop());
  budget.consume(1);
  EXPECT_TRUE(budget.should_stop());
  ObIvfScanBudget next_partition(10);
  EXPECT_EQ(0, next_partition.scanned_vectors_);
  EXPECT_FALSE(next_partition.should_stop());
  EXPECT_FALSE(next_partition.allow_batch_stop_);
  EXPECT_EQ(11, budget.scanned_vectors_);
  next_partition.consume(INT64_MAX);
  next_partition.consume(1);
  EXPECT_EQ(INT64_MAX, next_partition.scanned_vectors_);
  next_partition.consume(0);
  next_partition.consume(-1);
  EXPECT_EQ(INT64_MAX, next_partition.scanned_vectors_);
  ObIvfKnnSearchParam param;
  param.scan_budget_ = &budget;
  param.reset();
  EXPECT_EQ(nullptr, param.scan_budget_);
}

TEST_F(TestIVFSSTableWriter, ivf_searcher_read_stats_are_per_query)
{
  const int32_t dim = 64;
  const int32_t nlist = 2;
  const int64_t vector_count = 6;
  const int64_t cids[vector_count] = {0, 0, 0, 1, 1, 1};
  const int64_t vids[vector_count] = {10, 11, 12, 20, 21, 22};
  float centroids[nlist * dim] = {0};
  float vectors[vector_count * dim] = {0};
  float query_a[dim] = {0};
  float query_b[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  centroids[0] = 1.0F;
  centroids[dim] = -1.0F;
  vectors[0 * dim] = 4.0F;
  vectors[1 * dim] = 3.0F;
  vectors[2 * dim] = 2.0F;
  vectors[3 * dim] = -4.0F;
  vectors[4 * dim] = -3.0F;
  vectors[5 * dim] = -2.0F;
  query_a[0] = 2.0F;
  query_b[0] = -2.0F;

  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vectors, cids, vids, vector_count, centroids, nlist, dim, flip, 1,
      nullptr, share::VIDA_IP, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  ObIvfSSTableReadStats stats_a;
  ObIvfSSTableReadStats stats_b;
  ObIvfKnnSearchParam query_param_a;
  ObIvfKnnSearchParam query_param_b;
  fill_search_params(query_param_a, query_a, dim, 1);
  fill_search_params(query_param_b, query_b, dim, 2);
  query_param_a.nprobe_ = 1;
  query_param_b.nprobe_ = 2;
  query_param_a.read_stats_ = &stats_a;
  query_param_b.read_stats_ = &stats_b;
  ObIvfKnnSearchResult result_a;
  ObIvfKnnSearchResult result_b;
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param_a, result_a));
  }
  ASSERT_EQ(1, result_a.count());
  ASSERT_EQ(10, result_a.at(0).vid_);
  ASSERT_EQ(1, stats_a.low_precision_vector_scan_count_);
  ASSERT_GT(stats_a.raw_vector_read_count_, 0);
  ASSERT_LE(stats_a.raw_vector_read_count_, 3);
  ASSERT_GE(stats_a.raw_vector_key_count_, stats_a.raw_vector_read_count_);
  ASSERT_LE(stats_a.raw_vector_key_count_, 3);
  ASSERT_EQ(0, stats_b.low_precision_vector_scan_count_);
  ASSERT_EQ(0, stats_b.raw_vector_read_count_);
  ASSERT_EQ(0, stats_b.raw_vector_key_count_);
  const ObIvfSSTableReadStats stats_a_snapshot = stats_a;
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param_b, result_b));
  }
  ASSERT_EQ(2, result_b.count());
  ASSERT_EQ(20, result_b.at(0).vid_);
  ASSERT_NE(result_a.at(0).vid_, result_b.at(0).vid_);
  ASSERT_EQ(stats_a_snapshot.low_precision_vector_scan_count_, stats_a.low_precision_vector_scan_count_);
  ASSERT_EQ(stats_a_snapshot.raw_vector_read_count_, stats_a.raw_vector_read_count_);
  ASSERT_EQ(stats_a_snapshot.raw_vector_key_count_, stats_a.raw_vector_key_count_);
  ASSERT_EQ(stats_a_snapshot.first_low_precision_vector_scan_cid_, stats_a.first_low_precision_vector_scan_cid_);
  ASSERT_EQ(2, stats_b.low_precision_vector_scan_count_);
  ASSERT_GT(stats_b.raw_vector_read_count_, 0);
  ASSERT_LE(stats_b.raw_vector_read_count_, vector_count);
  ASSERT_GE(stats_b.raw_vector_key_count_, stats_b.raw_vector_read_count_);
  ASSERT_LE(stats_b.raw_vector_key_count_, vector_count);
  ASSERT_EQ(2, stats_b.first_low_precision_vector_scan_cid_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_brute_force_reads_only_pre_filter_raw_vectors)
{
  const int32_t dim = 64;
  const int64_t count = 3;
  const int64_t cids[count] = {0, 0, 0};
  const int64_t vids[count] = {10, 20, 30};
  float vectors[count][dim] = {{0}};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  query[0] = 1.0F;
  vectors[0][0] = 1.0F;
  vectors[1][0] = 2.0F;
  vectors[2][0] = 3.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      &vectors[0][0], cids, vids, count, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(20));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(30));
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(999));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  const int64_t deleted_vid = 20;
  ObDeletionBitmapHandle deletion;
  ASSERT_EQ(OB_SUCCESS,
      init_test_deletion_bitmap(allocator_, &deleted_vid, 1, deletion));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.nprobe_ = 0;
  query_param.pre_filter_ = &filter;
  query_param.deletion_bitmap_handle_ = &deletion;
  ObIvfSSTableReadStats stats;
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(IVF_RAW_VECTOR_CID, result.at(0).cid_);
  ASSERT_EQ(30, result.at(0).vid_);
  ASSERT_GE(stats.raw_vector_read_count_, 1);
  ASSERT_EQ(2, stats.raw_vector_key_count_);
  ASSERT_EQ(1, stats.raw_vector_refine_count_);

  // Repeat with an eagerly converted wrapper, as published by async-IVF PRE.
  // Duplicates must not add RAW reads or change the returned VID/distance.
  const double first_distance = result.at(0).distance_;
  allowed.set_empty();
  ASSERT_EQ(OB_SUCCESS, allowed.convert_to_bitmap());
  const uint64_t allowed_vids[] = {20, 30, 999, 30};
  ASSERT_EQ(OB_SUCCESS, allowed.value_add_many(allowed_vids, 4));
  ASSERT_EQ(3, allowed.get_cardinality());
  ASSERT_EQ(&allowed, filter.get_allowed_vids());
  stats.reset();
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(30, result.at(0).vid_);
  ASSERT_DOUBLE_EQ(first_distance, result.at(0).distance_);
  ASSERT_EQ(2, stats.raw_vector_key_count_);

  // Compact traversal must preserve deletion precedence and missing-VID handling.
  blocksstable::ObIvfPreFilterBitmap compact(allocator_);
  ASSERT_EQ(OB_SUCCESS, compact.value_add_many(allowed_vids, 4));
  for (uint64_t vid = 1000; vid < 1033; ++vid) {
    ASSERT_EQ(OB_SUCCESS, compact.value_add(vid));
  }
  ASSERT_TRUE(compact.is_compact());
  filter.reset();
  ASSERT_EQ(OB_SUCCESS, filter.init(compact));
  for (int64_t segment = 0; segment < 2; ++segment) {
    stats.reset();
    searcher->reuse();
    ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
    ASSERT_EQ(1, result.count());
    ASSERT_EQ(30, result.at(0).vid_);
    ASSERT_DOUBLE_EQ(first_distance, result.at(0).distance_);
    ASSERT_EQ(35, stats.raw_vector_key_count_);
  }
  filter.reset();
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));

  query_param.expected_dist_type_ = share::VIDA_COS;
  stats.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, stats.raw_vector_key_count_);
  query_param.expected_dist_type_ = share::VIDA_L2;
  query_param.dim_ = dim - 1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, stats.raw_vector_key_count_);
  query_param.dim_ = dim;

  const int64_t saved_timeout = scan_context_.timeout_;
  scan_context_.timeout_ = 1;
  stats.reset();
  ASSERT_EQ(OB_TIMEOUT, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, stats.raw_vector_key_count_);
  scan_context_.timeout_ = saved_timeout;
  searcher->reuse();
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(30, result.at(0).vid_);

  // The same pinned deletion view can eliminate the complete allowed set.
  // Rebuild only between synchronous calls, when no search borrows the set.
  allowed.set_empty();
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(deleted_vid));
  stats.reset();
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, stats.raw_vector_read_count_);
  ASSERT_EQ(0, stats.raw_vector_key_count_);
}

TEST_P(TestIVFRefineMetric,
    disk_bbq_non_aligned_dim_knn_and_brute_force_match_exact_topk)
{
  const int32_t dim = 65;
  const int64_t count = 4;
  const int64_t cids[count] = {0, 0, 0, 0};
  const int64_t vids[count] = {10, 20, 30, 40};
  const ObIvfRefineMetricCase metric_case = GetParam();
  float centroid[dim] = {0};
  float vectors[count][dim] = {{0}};
  float query[dim] = {0};
  centroid[0] = 1.0F;
  centroid[1] = 1.0F;
  query[0] = 1.0F;
  query[1] = 1.0F;
  vectors[0][0] = 1.0F;
  vectors[0][1] = 1.0F;
  vectors[1][0] = 2.0F;
  vectors[1][1] = 1.0F;
  vectors[2][0] = 3.0F;
  vectors[2][1] = 1.0F;
  vectors[3][0] = -1.0F;
  vectors[3][1] = -1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      &vectors[0][0], cids, vids, count, centroid, 1, dim, nullptr,
      1, nullptr, metric_case.dist_type_));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  blocksstable::ObIvfPreFilterBitmap allowed(allocator_);
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids[i]));
  }
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(999));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.pre_filter_ = &filter;
  ObIvfKnnSearchResult knn_result;
  ObIvfKnnSearchResult brute_force_result;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, knn_result));
  query_param.nprobe_ = 0;
  ASSERT_EQ(OB_SUCCESS,
      searcher->brute_force_search(query_param, brute_force_result));
  ASSERT_EQ(2, knn_result.count());
  ASSERT_EQ(2, brute_force_result.count());
  const int64_t expected_first_vid =
      share::VIDA_IP == metric_case.dist_type_ ? 30 : 10;
  const int64_t expected_second_vid = 20;
  ASSERT_EQ(expected_first_vid, knn_result.at(0).vid_);
  ASSERT_EQ(expected_second_vid, knn_result.at(1).vid_);
  ASSERT_EQ(expected_first_vid, brute_force_result.at(0).vid_);
  ASSERT_EQ(expected_second_vid, brute_force_result.at(1).vid_);
  ASSERT_NEAR(knn_result.at(0).distance_,
      brute_force_result.at(0).distance_, 1e-5F);
  ASSERT_NEAR(knn_result.at(1).distance_,
      brute_force_result.at(1).distance_, 1e-5F);

  blocksstable::ObIvfPreFilterBitmap compact(allocator_);
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, compact.value_add(vids[i]));
  }
  for (uint64_t vid = 1000; vid < 1033; ++vid) {
    ASSERT_EQ(OB_SUCCESS, compact.value_add(vid));
  }
  ASSERT_TRUE(compact.is_compact());
  filter.reset();
  ASSERT_EQ(OB_SUCCESS, filter.init(compact));
  searcher->reuse();
  query_param.nprobe_ = 1;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, knn_result));
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, brute_force_result));
  ASSERT_EQ(2, knn_result.count());
  ASSERT_EQ(2, brute_force_result.count());
  ASSERT_EQ(expected_first_vid, knn_result.at(0).vid_);
  ASSERT_EQ(expected_second_vid, knn_result.at(1).vid_);
  ASSERT_EQ(expected_first_vid, brute_force_result.at(0).vid_);
  ASSERT_EQ(expected_second_vid, brute_force_result.at(1).vid_);
  ASSERT_NEAR(knn_result.at(0).distance_, brute_force_result.at(0).distance_, 1e-5F);
  ASSERT_NEAR(knn_result.at(1).distance_, brute_force_result.at(1).distance_, 1e-5F);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_knn_cursor_survives_brute_force_and_resets_explicitly)
{
  const int32_t dim = 64;
  const int64_t count = 3;
  const int64_t cids[count] = {0, 1, 2};
  const int64_t vids[count] = {10, 20, 30};
  float centroids[count][dim] = {{0}};
  float vectors[count][dim] = {{0}};
  float query[dim] = {0};
  centroids[1][0] = 10.0F;
  centroids[2][0] = 20.0F;
  vectors[1][0] = 10.0F;
  vectors[2][0] = 20.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      &vectors[0][0], cids, vids, count, &centroids[0][0], count, dim,
      nullptr, 1, nullptr, share::VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  ObRoaringBitmap allowed(&allocator_);
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids[i]));
  }
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  query_param.pre_filter_ = &filter;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS, table_handle.set_sstable_with_tablet(&sstable_));
  ObIvfKnnSearchResult result;

  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(searcher->has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(searcher->has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  ASSERT_TRUE(searcher->has_remaining_centroids());
  query_param.nprobe_ = 2;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(30, result.at(0).vid_);
  ASSERT_FALSE(searcher->has_remaining_centroids());

  searcher->reuse();
  query_param.nprobe_ = 1;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(searcher->has_remaining_centroids());
  ASSERT_EQ(OB_SUCCESS, searcher->switch_sstable(table_handle));
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(searcher->has_remaining_centroids());
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_brute_force_keeps_ten_thousand_vids_batched)
{
  const int32_t dim = 8;
  const int64_t count = 10000;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0);
  std::vector<int64_t> vids(count);
  float centroid[dim] = {0};
  float query[dim] = {0};
  std::iota(vids.begin(), vids.end(), 0);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vectors.data(), cids.data(), vids.data(), count, centroid, 1, dim,
      nullptr, 1, nullptr, share::VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  ObRoaringBitmap allowed(&allocator_);
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids[i]));
  }
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(count + 1));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.nprobe_ = 0;
  query_param.pre_filter_ = &filter;
  ObIvfSSTableReadStats stats;
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(0, result.at(0).vid_);
  const int64_t expected_key_count = count + 1;
  const int64_t expected_batch_count =
      (expected_key_count + ObDistBBQFastScanBatch::CAPACITY - 1)
      / ObDistBBQFastScanBatch::CAPACITY;
  ASSERT_EQ(expected_batch_count, stats.raw_vector_read_count_);
  ASSERT_EQ(expected_key_count, stats.raw_vector_key_count_);
  ASSERT_EQ(count, stats.raw_vector_refine_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_brute_force_initialized_empty_pre_filter_has_zero_raw_io)
{
  const int32_t dim = 64;
  const int64_t cid = 0;
  const int64_t vid = 10;
  float vector[dim] = {0};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vector, &cid, &vid, 1, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObRoaringBitmap allowed(&allocator_);
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim - 1, 1);
  query_param.nprobe_ = 0;
  query_param.pre_filter_ = &filter;
  ObIvfSSTableReadStats stats;
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, stats.raw_vector_read_count_);
  ASSERT_EQ(0, stats.raw_vector_key_count_);
  ASSERT_EQ(0, stats.raw_vector_refine_count_);
  query_param.expected_dist_type_ = share::VIDA_L2;
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, stats.raw_vector_key_count_);
  query_param.dim_ = dim;
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
  ASSERT_EQ(0, stats.raw_vector_key_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_brute_force_reuses_raw_vector_batches_and_skips_missing_vids)
{
  const int32_t dim = 64;
  const int64_t count = ObDistBBQFastScanBatch::CAPACITY + 1;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0);
  std::vector<int64_t> vids(count);
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < count; ++i) {
    vids[i] = 100 + i;
    vectors[i * dim] = static_cast<float>(i + 1);
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vectors.data(), cids.data(), vids.data(), count, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObRoaringBitmap allowed(&allocator_);
  for (int64_t i = 0; i < count; ++i) {
    ASSERT_EQ(OB_SUCCESS, allowed.value_add(vids[i]));
  }
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(999999));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, count);
  query_param.nprobe_ = 0;
  query_param.pre_filter_ = &filter;
  ObIvfSSTableReadStats stats;
  query_param.read_stats_ = &stats;
  ObIvfKnnSearchResult result;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(count, result.count());
  ASSERT_EQ(2, stats.raw_vector_read_count_);
  ASSERT_EQ(count + 1, stats.raw_vector_key_count_);
  ASSERT_EQ(count, stats.raw_vector_refine_count_);
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_brute_force_rejects_vids_above_int64_max)
{
  const int32_t dim = 64;
  const int64_t cid = 0;
  const int64_t vid = 10;
  float vector[dim] = {0};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vector, &cid, &vid, 1, centroid, 1, dim, flip));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(static_cast<uint64_t>(INT64_MAX) + 1));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.nprobe_ = 0;
  query_param.pre_filter_ = &filter;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_brute_force_clears_result_when_raw_vector_distance_fails)
{
  const int32_t dim = 64;
  const int64_t cid = 0;
  const int64_t vid = 10;
  float vector[dim] = {0};
  float raw_vector[dim] = {0};
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  query[0] = 1.0F;
  vector[0] = 1.0F;
  raw_vector[0] = std::numeric_limits<float>::quiet_NaN();
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, true, raw_vector));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.nprobe_ = 0;
  query_param.pre_filter_ = &filter;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS,
      create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ASSERT_EQ(OB_NUMERIC_OVERFLOW, searcher->brute_force_search(query_param, result));
  ASSERT_EQ(0, result.count());
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_empty_sstable_ignores_pre_filter_without_reader)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  sstable_.ivf_meta_.set_empty();
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float query[8] = {0};
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, ARRAYSIZEOF(query), 1);
  ObIvfCountingAllocator counting_allocator(allocator_);
  ObTableScanParam counting_scan_param;
  ObStoreCtx counting_store_ctx;
  ObTableAccessContext counting_access_ctx;
  prepare_ivf_scan_param(scan_param_, counting_allocator, counting_scan_param);
  ASSERT_EQ(OB_SUCCESS, init_ivf_access_context(
      sstable_, counting_scan_param, counting_store_ctx, counting_access_ctx));
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(10));
  ObIvfPreFilter valid_filter;
  ASSERT_EQ(OB_SUCCESS, valid_filter.init(allowed));
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));

  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(
            sstable_, counting_scan_param, counting_access_ctx, searcher.get()));
    ASSERT_EQ(1, counting_allocator.alloc_count_);
    query_param.pre_filter_ = &valid_filter;
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(1, counting_allocator.alloc_count_);
    ASSERT_EQ(0, counting_allocator.free_count_);
    query_param.pre_filter_ = nullptr;
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(query_param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(1, counting_allocator.alloc_count_);
    ASSERT_EQ(0, counting_allocator.free_count_);
  }
  ASSERT_EQ(1, counting_allocator.free_count_);

  ObIvfSSTableReadStats brute_force_stats;
  query_param.nprobe_ = 0;
  query_param.pre_filter_ = &valid_filter;
  query_param.read_stats_ = &brute_force_stats;
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
    ASSERT_EQ(OB_SUCCESS, result.add(ObIvfKnnEntry()));
    ASSERT_EQ(OB_SUCCESS, searcher->brute_force_search(query_param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(0, brute_force_stats.low_precision_vector_scan_count_);
    ASSERT_EQ(0, brute_force_stats.low_precision_vector_count_);
    ASSERT_EQ(0, brute_force_stats.raw_vector_read_count_);
    ASSERT_EQ(0, brute_force_stats.raw_vector_key_count_);
    ASSERT_EQ(0, brute_force_stats.raw_vector_refine_count_);
  }
}

static ObIvfSSTableRowDesc make_valid_row_desc()
{
  ObIvfSSTableRowDesc desc;
  desc.row_type_col_idx_ = 0;
  desc.centroid_id_col_idx_ = 1;
  desc.vector_id_col_idx_ = 2;
  desc.trans_version_col_idx_ = 3;
  desc.sql_sequence_col_idx_ = 4;
  desc.payload_col_idx_ = 5;
  return desc;
}

typedef int16_t ObIvfSSTableRowDesc::*RowDescColumnMember;

struct NamedRowDescColumn
{
  const char *name_;
  RowDescColumnMember member_;
};

static const NamedRowDescColumn ROW_DESC_COLUMNS[] = {
  {"row_type", &ObIvfSSTableRowDesc::row_type_col_idx_},
  {"centroid_id", &ObIvfSSTableRowDesc::centroid_id_col_idx_},
  {"vector_id", &ObIvfSSTableRowDesc::vector_id_col_idx_},
  {"payload", &ObIvfSSTableRowDesc::payload_col_idx_},
};

TEST(TestIVFSSTableRowDesc, default_is_invalid)
{
  ObIvfSSTableRowDesc desc;
  ASSERT_FALSE(desc.is_valid());
}

TEST(TestIVFSSTableRowDesc, requires_nonnegative_columns)
{
  const int64_t column_count = ARRAYSIZEOF(ROW_DESC_COLUMNS);
  ObIvfSSTableRowDesc desc = make_valid_row_desc();
  ASSERT_TRUE(desc.is_valid());

  for (int64_t i = 0; i < column_count; ++i) {
    SCOPED_TRACE(ROW_DESC_COLUMNS[i].name_);
    desc = make_valid_row_desc();
    desc.*ROW_DESC_COLUMNS[i].member_ = -1;
    ASSERT_FALSE(desc.is_valid());
  }

  desc = make_valid_row_desc();
  desc.trans_version_col_idx_ = -1;
  desc.sql_sequence_col_idx_ = -1;
  ASSERT_TRUE(desc.is_valid());
}

TEST(TestIVFSSTableRowDesc, supports_value_copy)
{
  ObIvfSSTableRowDesc desc = make_valid_row_desc();

  ObIvfSSTableRowDesc copy = desc;
  desc.reset();
  ASSERT_FALSE(desc.is_valid());
  ASSERT_TRUE(copy.is_valid());
  ASSERT_EQ(3, copy.trans_version_col_idx_);
  ASSERT_EQ(4, copy.sql_sequence_col_idx_);
  ASSERT_EQ(5, copy.payload_col_idx_);
}

TEST(TestIVFSSTableRowDesc, payload_need_not_be_last_column)
{
  ObIvfSSTableRowDesc desc;
  desc.row_type_col_idx_ = 2;
  desc.centroid_id_col_idx_ = 3;
  desc.vector_id_col_idx_ = 4;
  desc.trans_version_col_idx_ = 0;
  desc.sql_sequence_col_idx_ = 5;
  desc.payload_col_idx_ = 1;

  ASSERT_TRUE(desc.is_valid());
}

TEST_F(TestIVFSSTableWriter,
    disk_bbq_empty_sstable_accepts_self_valid_queries_and_recovers_from_errors)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  sstable_.ivf_meta_.set_empty();
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  float first_query[8] = {0};
  float second_query[12] = {0};
  ObIvfKnnSearchParam first_param;
  ObIvfKnnSearchParam second_param;
  fill_search_params(first_param, first_query, ARRAYSIZEOF(first_query), 1);
  fill_search_params(second_param, second_query, ARRAYSIZEOF(second_query), 3);
  ObIvfKnnSearchParam invalid_param = first_param;
  invalid_param.query_ = nullptr;
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(10));
  ObIvfPreFilter valid_filter;
  ASSERT_EQ(OB_SUCCESS, valid_filter.init(allowed));
  ObIvfCountingAllocator counting_allocator(allocator_);
  ObTableScanParam counting_scan_param;
  ObStoreCtx counting_store_ctx;
  ObTableAccessContext counting_access_ctx;
  prepare_ivf_scan_param(scan_param_, counting_allocator, counting_scan_param);
  ASSERT_EQ(OB_SUCCESS, init_ivf_access_context(
      sstable_, counting_scan_param, counting_store_ctx, counting_access_ctx));
  ObIvfKnnSearchResult result;
  {
    ObIvfSearcherGuard searcher;
    ASSERT_EQ(OB_SUCCESS,
        create_ivf_searcher(
            sstable_, counting_scan_param, counting_access_ctx, searcher.get()));

    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(first_param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(second_param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(0, searcher->query_arena_.used());

    ASSERT_EQ(OB_INVALID_ARGUMENT, searcher->knn_search(invalid_param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(first_param, result));
    ASSERT_EQ(0, result.count());
    second_param.pre_filter_ = &valid_filter;
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(second_param, result));
    ASSERT_EQ(0, result.count());
    second_param.pre_filter_ = nullptr;
    ASSERT_EQ(OB_SUCCESS, searcher->knn_search(second_param, result));
    ASSERT_EQ(0, result.count());
    ASSERT_EQ(nullptr, searcher->centroid_iterator_);
    ASSERT_EQ(0, searcher->query_arena_.used());
    ASSERT_EQ(1, counting_allocator.live_allocation_count());
  }
  ASSERT_EQ(0, counting_allocator.live_allocation_count());
  ASSERT_EQ(counting_allocator.alloc_count_, counting_allocator.free_count_);
}

TEST(TestAsyncIVFHybridRoot, merges_segments_before_partition_topk)
{
  ObArenaAllocator alloc;
  ObSEArray<sql::ObDASVectorCandidate, 4> candidates;
  const int64_t vids[] = {90, 20, 10, 30};
  const float distances[] = {9.0F, 1.0F, 1.0F, 3.0F};
  for (int64_t i = 0; i < 4; ++i) {
    sql::ObDASVectorCandidate c;
    c.entry_.vid_ = vids[i]; c.entry_.cid_ = i; c.entry_.distance_ = distances[i];
    c.group_slot_ = i / 2; c.segment_slot_ = i;
    ASSERT_EQ(OB_SUCCESS, candidates.push_back(c));
  }
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, sql::ObDASAsyncIVFTopKMergeOp::collect_topk(candidates, 2, alloc, result));
  ASSERT_EQ(2, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_EQ(20, result.at(1).vid_);
  // Reversing child visitation cannot change distance/VID ordering.
  std::reverse(candidates.begin(), candidates.end());
  ASSERT_EQ(OB_SUCCESS, sql::ObDASAsyncIVFTopKMergeOp::collect_topk(candidates, 2, alloc, result));
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_EQ(20, result.at(1).vid_);
  // Repeating a physical candidate must not consume another logical TopK slot.
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(candidates.at(1)));
  ASSERT_EQ(OB_SUCCESS, sql::ObDASAsyncIVFTopKMergeOp::collect_topk(candidates, 5, alloc, result));
  ASSERT_EQ(4, result.count());
  candidates.reuse();
  ASSERT_EQ(OB_SUCCESS, sql::ObDASAsyncIVFTopKMergeOp::collect_topk(candidates, 2, alloc, result));
  ASSERT_EQ(0, result.count());
}




TEST_F(TestIVFSSTableWriter, disk_bbq_profile_preserves_results_and_tail)
{
  const int32_t dim = 64;
  // Exercise profiling across full RAW batches and a partial tail.
  const int64_t count = 32 * 32 + 16;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0);
  std::vector<int64_t> vids(count);
  float centroid[dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  for (int64_t i = 0; i < count; ++i) {
    vids[i] = i + 10;
    vectors[i * dim] = static_cast<float>(i + 1) / count;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroid, 1, dim, flip, 1));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, count);
  param.nprobe_ = 1;
  ObIvfSSTableReadStats plain_stats;
  param.read_stats_ = &plain_stats;
  ObIvfKnnSearchResult plain_result;
  {
    ObProfileSwitcher disabled(static_cast<ObOpProfile<ObMetric> *>(nullptr));
    ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, param, plain_result));
  }
  ASSERT_EQ(count, plain_result.count());
  // Cached FastScan batches can end at micro-block boundaries.
  ASSERT_GE(plain_stats.raw_vector_read_count_, 33);
  ASSERT_LE(plain_stats.raw_vector_read_count_, count);
  ASSERT_EQ(count, plain_stats.raw_vector_key_count_);

  ObIvfSSTableReadStats profiled_stats;
  param.read_stats_ = &profiled_stats;
  ObIvfKnnSearchResult profiled_result;
  ObOpProfile<ObMetric> profile(ObProfileId::PHY_HASH_JOIN, &allocator_);
  auto *previous_profile = get_current_profile();
  {
    ObProfileSwitcher enabled(&profile);
    ASSERT_EQ(OB_SUCCESS, run_ivf_knn_search(sstable_, scan_param_, param, profiled_result));
  }
  EXPECT_EQ(previous_profile, get_current_profile());
  ASSERT_EQ(count, profiled_result.count());
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(vids[i], plain_result.at(i).vid_);
    EXPECT_EQ(plain_result.at(i).vid_, profiled_result.at(i).vid_);
    EXPECT_FLOAT_EQ(plain_result.at(i).distance_, profiled_result.at(i).distance_);
  }
  EXPECT_EQ(plain_stats.raw_vector_read_count_, profiled_stats.raw_vector_read_count_);
  EXPECT_EQ(plain_stats.raw_vector_key_count_, profiled_stats.raw_vector_key_count_);
  EXPECT_EQ(plain_stats.raw_vector_refine_count_, profiled_stats.raw_vector_refine_count_);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_post_hgraph_batches_preserve_open_posting_scan)
{
  const int32_t dim = 64;
  const int64_t nlist = 5000;
  ASSERT_GE(nlist, ObVecIdxExtraInfo::IVF_CENTERS_HGRAPH_THRESHOLD);
  const int64_t first_count = 600;
  const int64_t count = first_count + nlist - 1;
  std::vector<float> centroids(nlist * dim, 0.0F);
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0);
  std::vector<int64_t> vids(count);
  float query[dim] = {0};
  for (int64_t cid = 0; cid < nlist; ++cid) {
    centroids[cid * dim] = static_cast<float>(cid) * 10.0F;
  }
  for (int64_t i = 0; i < count; ++i) {
    vids[i] = i + 1;
    cids[i] = i < first_count ? 0 : i - first_count + 1;
    vectors[i * dim] = i < first_count
        ? static_cast<float>(i) * 0.001F : centroids[cids[i] * dim];
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), nlist, dim, nullptr));
  sstable_.ivf_meta_.centroid_count_ = nlist;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  const ObLSID cache_ls_id(IVF_LS_ID);
  scan_context_.ls_id_ = cache_ls_id;
  ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);
  ASSERT_NE(nullptr, service);
  ObPluginVectorIndexMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, service->acquire_vector_index_mgr(
      cache_ls_id, ls_mgr, false /* check_ls_exist */));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 10);
  param.nprobe_ = 1;
  ObIvfKnnSearchResult batch;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, batch));
  searcher->reuse(); // Release the ARRAY read lock before the representation switch.
  const ObIvfCacheMgrKey cache_key(sstable_.get_key());
  ObIvfCacheMgrGuard cache_guard;
  ASSERT_EQ(OB_SUCCESS, service->acquire_ivf_cache_mgr_guard(cache_ls_id, cache_key, cache_guard));
  ObIvfCacheMgr *cache_mgr = cache_guard.get_ivf_cache_mgr();
  ASSERT_NE(nullptr, cache_mgr);
  ObIvfSSTableCentCache *cache = nullptr;
  ASSERT_EQ(OB_SUCCESS, cache_mgr->get_cache_node<ObIvfSSTableCentCache>(
      IVF_SSTABLE_CENTROID_CACHE, cache));
  ASSERT_NE(nullptr, cache);
  ObVectorIndexParam graph;
  graph.type_ = VIAT_HGRAPH;
  graph.lib_ = VIAL_VSAG;
  graph.dist_algorithm_ = VIDA_L2;
  graph.dim_ = dim;
  graph.nlist_ = nlist;
  graph.m_ = 16;
  graph.ef_construction_ = 200;
  graph.ef_search_ = 64;
  graph.extra_info_actual_size_ = 0;
  graph.refine_type_ = 0;
  graph.bq_bits_query_ = 32;
  graph.bq_use_fht_ = false;
  const int64_t checked_centroid_count = 2;
  const int64_t checked_cids[checked_centroid_count] = {1, nlist};
  float expected_centroids[checked_centroid_count][dim] = {};
  {
    common::RWLock::RLockGuard guard(cache->get_lock());
    ObIvfICache &cache_iface = *cache;
    ObVsagSearchAlloc read_allocator(MTL_ID());
    float *center = nullptr;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST,
        cache_iface.read_centroid_from_hgraph(1, center, read_allocator));
    EXPECT_EQ(nullptr, center);
    // SSTable stores rotated centroids. Copy ARRAY samples before HGraph replaces it.
    for (int64_t i = 0; i < checked_centroid_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, cache_iface.read_centroid(checked_cids[i], center));
      ASSERT_NE(nullptr, center);
      MEMCPY(expected_centroids[i], center, dim * sizeof(float));
    }
  }
  ASSERT_EQ(OB_SUCCESS, cache->build_sstable_hgraph(graph));
  ASSERT_TRUE(cache->has_hgraph_index());
  EXPECT_EQ(first_count, cache->get_child_count(0));
  EXPECT_EQ(1, cache->get_child_count(nlist - 1));
  cache->update_registered_hold(cache->get_memory_hold());
  {
    ObIvfICache &cache_iface = *cache;
    ObVsagSearchAlloc read_allocator(MTL_ID());
    float *center = nullptr;
    for (int64_t i = 0; i < checked_centroid_count; ++i) {
      const int64_t cid = checked_cids[i];
      SCOPED_TRACE(cid);
      ASSERT_EQ(OB_SUCCESS, cache_iface.read_centroid_from_hgraph(cid, center, read_allocator));
      ASSERT_NE(nullptr, center);
      for (int64_t d = 0; d < dim; ++d) {
        EXPECT_FLOAT_EQ(expected_centroids[i][d], center[d]);
      }
      read_allocator.Deallocate(center);
      center = nullptr;
    }
    EXPECT_EQ(OB_INVALID_ARGUMENT, cache_iface.read_centroid_from_hgraph(0, center, read_allocator));
    EXPECT_EQ(OB_INVALID_ARGUMENT, cache_iface.read_centroid_from_hgraph(nlist + 1, center, read_allocator));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, cache_iface.read_centroid(1, center));
  }
  for (const int64_t batch_size : {32, 256}) {
    SCOPED_TRACE(batch_size);
    searcher->reuse();
    ObIvfCandidateProgress progress;
    int64_t started = 0;
    int64_t returned = 0;
    int64_t calls = 0;
    do {
      ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
          std::numeric_limits<float>::infinity(), batch, progress, batch_size));
      ASSERT_NE(ObIvfCandidateProgress::DOMAIN_END, progress.state_);
      ASSERT_TRUE(flat_group_reader_for_test(searcher->centroid_iterator()).hgraph_ctx_.is_active());
      started += progress.centroids_started_;
      returned += batch.count();
      for (int64_t i = 0; i < batch.count(); ++i) {
        ASSERT_EQ(1, batch.at(i).cid_);
      }
      ASSERT_LT(++calls, 100);
    } while (ObIvfCandidateProgress::WINDOW_END != progress.state_);
    ASSERT_GT(calls, 1);
    ASSERT_EQ(1, started);
    ASSERT_EQ(first_count, returned);
    // A new window must fetch the next HGraph batch, without replaying CID 1.
    ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
        std::numeric_limits<float>::infinity(), batch, progress, batch_size));
    ASSERT_EQ(1, progress.centroids_started_);
    ASSERT_EQ(1, batch.count());
    ASSERT_EQ(2, batch.at(0).cid_);
    ASSERT_EQ(first_count + 1, batch.at(0).vid_);
  }
  // Automatic search resolves its first window from centroid child counts.
  // A dense first CID can meet B alone; surplus graph candidates survive refills.
  sstable_.ivf_meta_.vector_count_ = count;
  param.original_k_ = 10;
  param.num_candidates_ = 50;
  param.is_set_refine_k_ = true; // Keep this cursor test on its fixed posting budget.
  param.nprobe_ = 0;
  // The mainline estimate requests a large first batch; it is not capped at 64.
  param.cluster_size_ = 2;
  ASSERT_LT(ObDiskBBQSearcher::compute_auto_scan_budget(10, 50, count), first_count);
  const int64_t budget = ObDiskBBQSearcher::compute_auto_scan_budget(10, 50, count);
  const int64_t initial_graph_count = (budget + 1) / 2;
  ASSERT_GT(initial_graph_count, 64);
  for (const bool post : {false, true}) {
    SCOPED_TRACE(post);
    searcher->reuse();
    auto *disk = static_cast<ObDiskBBQSearcher *>(searcher.get());
    if (!post) {
      ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, batch));
    } else {
      ObIvfCandidateProgress progress;
      int64_t calls = 0;
      do {
        ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
            std::numeric_limits<float>::infinity(), batch, progress, 32));
        ASSERT_LT(++calls, 100);
      } while (ObIvfCandidateProgress::MORE == progress.state_);
      ASSERT_EQ(ObIvfCandidateProgress::WINDOW_END, progress.state_);
    }
    ASSERT_EQ(1, disk->auto_nprobe_);
    ASSERT_TRUE(searcher->centroid_iterator()->has_buffered_centroids());
    const int64_t prefetched = flat_group_reader_for_test(searcher->centroid_iterator()).hgraph_ctx_.visited_count_;
    ASSERT_EQ(initial_graph_count, prefetched);
    for (int64_t cid = 2; cid <= 4; ++cid) {
      if (!post) {
        ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, batch));
      } else {
        ObIvfCandidateProgress progress;
        ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
            std::numeric_limits<float>::infinity(), batch, progress, 32));
        ASSERT_EQ(ObIvfCandidateProgress::WINDOW_END, progress.state_);
      }
      ASSERT_EQ(1, batch.count());
      ASSERT_EQ(cid, batch.at(0).cid_);
      ASSERT_EQ(first_count + cid - 1, batch.at(0).vid_);
      ASSERT_EQ(1, disk->auto_nprobe_);
      ASSERT_EQ(prefetched, flat_group_reader_for_test(searcher->centroid_iterator()).hgraph_ctx_.visited_count_);
    }
  }
  // Start from the sparse end: each visited CID has one posting. Metadata
  // selection must collect enough graph candidates to satisfy B before scans.
  // The query has changed, so use a fresh searcher.
  searcher->reuse();
  query[0] = centroids[(nlist - 1) * dim];
  // The first estimated batch is insufficient at this end of the graph.
  // Further batches contain eight centers, keeping the final batch's surplus.
  const int64_t expected_prefetched = initial_graph_count
      + ((budget - initial_graph_count + 7) / 8) * 8;
  for (const bool post : {false, true}) {
    SCOPED_TRACE(post);
    ObIvfSearcherGuard sparse_searcher;
    ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, sparse_searcher.get()));
    auto *disk = static_cast<ObDiskBBQSearcher *>(sparse_searcher.get());
    if (!post) {
      ASSERT_EQ(OB_SUCCESS, sparse_searcher->knn_search(param, batch));
    } else {
      ObIvfCandidateProgress progress;
      int64_t calls = 0;
      do {
        ASSERT_EQ(OB_SUCCESS, sparse_searcher->next_candidate_batch(param,
            std::numeric_limits<float>::infinity(), batch, progress, 32));
        // With a 32-slot output batch, one sparse posting can fill this call.
        ASSERT_LT(++calls, budget + 2);
      } while (ObIvfCandidateProgress::MORE == progress.state_);
      ASSERT_EQ(ObIvfCandidateProgress::WINDOW_END, progress.state_);
    }
    ASSERT_EQ(0, disk->initial_child_budget_);
    ASSERT_EQ(budget, disk->auto_nprobe_);
    ASSERT_FALSE(disk->need_init_auto_nprobe_);
    ASSERT_EQ(expected_prefetched, flat_group_reader_for_test(sparse_searcher->centroid_iterator()).hgraph_ctx_.visited_count_);
  }
  searcher->reuse();
  bool erased = false;
  uint64_t removed_hold = 0;
  ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(cache_ls_id, cache_key,
      cache_mgr, cache, cache->get_last_access_ts(), false /* failed_build */,
      erased, removed_hold));
  ASSERT_TRUE(erased);
  cache_guard.reset();
}

TEST_F(TestIVFSSTableWriter, disk_bbq_post_stream_keeps_candidates_beyond_unfiltered_topm)
{
  const int32_t dim = 64;
  const int64_t count = 600;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<int64_t> cids(count, 0);
  std::vector<int64_t> vids(count);
  float centroid[dim] = {0};
  float query[dim] = {0};
  for (int64_t i = 0; i < count; ++i) {
    vids[i] = i + 1;
    vectors[i * dim] = static_cast<float>(count - i) * 0.001F;
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroid, 1, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 10);
  param.expected_dist_type_ = share::VIDA_L2;
  ObIvfSSTableReadStats stats;
  param.read_stats_ = &stats;
  ObIvfKnnSearchResult batch;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, batch));
  ASSERT_EQ(10, batch.count());
  for (int64_t i = 0; i < batch.count(); ++i) {
    ASSERT_NE(0, batch.at(i).vid_ % 97); // Old fixed Top-M loses every eligible row.
  }

  const int64_t deleted_vid = 582;
  ObDeletionBitmapHandle deletion;
  ASSERT_EQ(OB_SUCCESS, init_test_deletion_bitmap(allocator_, &deleted_vid, 1, deletion));
  for (const bool deleted : {false, true}) {
    param.deletion_bitmap_handle_ = deleted ? &deletion : nullptr;
    for (const int64_t batch_size : {32, 256}) {
      searcher->reuse();
      stats.reset();
      ObIvfTopKCollector accepted;
      ASSERT_EQ(OB_SUCCESS, accepted.init(2, allocator_));
      ObIvfCandidateProgress progress;
      int64_t calls = 0;
      int64_t started = 0;
      int64_t scanned = 0;
      do {
        float threshold = 0;
        ASSERT_EQ(OB_SUCCESS, accepted.get_distance_threshold(threshold));
        threshold = std::nextafter(threshold, std::numeric_limits<float>::infinity());
        ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param, threshold, batch, progress, batch_size));
        ASSERT_LE(batch.count(), batch_size);
        ASSERT_NE(ObIvfCandidateProgress::WINDOW_END, progress.state_);
        started += progress.centroids_started_;
        scanned += progress.postings_scanned_;
        for (int64_t i = 0; i < batch.count(); ++i) {
          const auto &entry = batch.at(i);
          ASSERT_FALSE(deleted && entry.vid_ == deleted_vid);
          if (0 == entry.vid_ % 97) {
            ASSERT_EQ(OB_SUCCESS, accepted.push(entry.cid_, entry.vid_, entry.distance_));
          }
        }
        ASSERT_LT(++calls, 100);
        // Even when the first 256-row batch fills K, later candidates must compete.
      } while (ObIvfCandidateProgress::DOMAIN_END != progress.state_);
      ASSERT_GT(calls, 1);
      ASSERT_EQ(1, started);
      ASSERT_EQ(count, scanned);
      ASSERT_EQ(count, stats.low_precision_vector_count_);
      ObIvfKnnSearchResult final_result;
      ASSERT_EQ(OB_SUCCESS, accepted.finish(final_result));
      ASSERT_EQ(2, final_result.count());
      ASSERT_EQ(deleted ? 485 : 582, final_result.at(0).vid_);
      ASSERT_EQ(deleted ? 388 : 485, final_result.at(1).vid_);
      ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
          std::numeric_limits<float>::infinity(), batch, progress, batch_size));
      ASSERT_EQ(0, batch.count());
      ASSERT_EQ(ObIvfCandidateProgress::DOMAIN_END, progress.state_);
    }
  }

  searcher->reuse();
  ObIvfCandidateProgress progress;
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), batch, progress, 32));
  ASSERT_EQ(ObIvfCandidateProgress::MORE, progress.state_);
  const int64_t timeout = scan_context_.timeout_;
  scan_context_.timeout_ = 1;
  const int interrupted = searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), batch, progress, 32);
  scan_context_.timeout_ = timeout;
  ASSERT_EQ(OB_TIMEOUT, interrupted);
  ASSERT_EQ(0, batch.count());
  searcher->reuse();
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), batch, progress, 32));
  ASSERT_EQ(32, batch.count());
  ASSERT_EQ(OB_STATE_NOT_MATCH, searcher->knn_search(param, batch));
  ASSERT_EQ(0, batch.count());
  ObRoaringBitmap allowed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed.value_add(1));
  ObIvfPreFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
  param.pre_filter_ = &filter;
  ASSERT_EQ(OB_STATE_NOT_MATCH, searcher->brute_force_search(param, batch));
  ASSERT_EQ(0, batch.count());
  param.pre_filter_ = nullptr;
  searcher->reuse();
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, batch));
  ASSERT_EQ(10, batch.count());
  ASSERT_EQ(OB_STATE_NOT_MATCH, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), batch, progress, 32));
  ASSERT_EQ(0, batch.count());
  ASSERT_EQ(0, progress.postings_scanned_);
  searcher->reuse();
  for (const int64_t invalid_size : {0, 31, 33, 65568}) {
    ASSERT_EQ(OB_INVALID_ARGUMENT, searcher->next_candidate_batch(param,
        std::numeric_limits<float>::infinity(), batch, progress, invalid_size));
    ASSERT_EQ(0, batch.count());
    ASSERT_EQ(0, progress.centroids_started_);
  }
  param.deletion_bitmap_handle_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), batch, progress,
      ObIvfCandidateProgress::MAX_BATCH_SIZE));
  ASSERT_EQ(ObIvfCandidateProgress::MAX_BATCH_SIZE, batch.count());
  ASSERT_EQ(ObIvfCandidateProgress::MORE, progress.state_);
  searcher->reuse();
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), batch, progress));
  ASSERT_EQ(ObIvfCandidateProgress::MAX_BATCH_SIZE, batch.count());
  ASSERT_EQ(ObIvfCandidateProgress::MORE, progress.state_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::quiet_NaN(), batch, progress, 32));
  ASSERT_EQ(0, batch.count());
  searcher->reuse();
  ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
      std::numeric_limits<float>::infinity(), batch, progress, 32));
  ASSERT_EQ(32, batch.count());
}

TEST_P(TestIVFRefineMetric, disk_bbq_post_stream_soar_windows_preserve_visible_identity)
{
  const int32_t dim = 64;
  const int64_t unique_count = 39;
  const int32_t nlist = 3;
  const int64_t count = unique_count * nlist;
  const auto metric = GetParam().dist_type_;
  std::vector<float> vectors(count * dim, 0.0F);
  std::vector<float> centroids(nlist * dim, 0.0F);
  std::vector<int64_t> cids(count);
  std::vector<int64_t> vids(count);
  float query[dim] = {0};
  query[0] = 1.0F;
  for (int64_t i = 0; i < count; ++i) {
    cids[i] = i / unique_count;
    vids[i] = i % unique_count;
    vectors[i * dim] = 1.0F;
  }
  for (int64_t cid = 0; cid < nlist; ++cid) { centroids[cid * dim + 1] = 0.1F; }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
      count, centroids.data(), nlist, dim, nullptr, 1, nullptr, metric, true,
      nullptr, nullptr, 0, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  const int64_t deleted_vid = 0;
  ObDeletionBitmapHandle deletion;
  ASSERT_EQ(OB_SUCCESS, init_test_deletion_bitmap(allocator_, &deleted_vid, 1, deletion));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  param.nprobe_ = 1;
  param.expected_dist_type_ = metric;
  param.deletion_bitmap_handle_ = &deletion;
  ObIvfTopKCollector accepted;
  ASSERT_EQ(OB_SUCCESS, accepted.init(3, allocator_));
  int64_t windows = 0;
  int64_t scanned = 0;
  for (int64_t call = 0; call < 20; ++call) {
    ObIvfKnnSearchResult batch;
    ObIvfCandidateProgress progress;
    ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param,
        std::numeric_limits<float>::infinity(), batch, progress, 32));
    ASSERT_EQ(nullptr, static_cast<ObDiskBBQSearcher *>(searcher.get())->refined_vids_);
    scanned += progress.postings_scanned_;
    for (int64_t i = 0; i < batch.count(); ++i) {
      const auto &entry = batch.at(i);
      ASSERT_NE(deleted_vid, entry.vid_);
      if (0 == entry.vid_ % 2) { ASSERT_EQ(OB_SUCCESS, accepted.push(entry.cid_, entry.vid_, entry.distance_)); }
    }
    if (ObIvfCandidateProgress::MORE != progress.state_) { ++windows; }
    if (ObIvfCandidateProgress::DOMAIN_END == progress.state_) { break; }
  }
  ASSERT_EQ(nlist, windows);
  ASSERT_EQ(count, scanned);
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, accepted.finish(result));
  ASSERT_EQ(3, result.count());
  for (int64_t i = 0; i < result.count(); ++i) { ASSERT_EQ(2 * (i + 1), result.at(i).vid_); }
}

TEST_F(TestIVFSSTable, async_ivf_query_selects_auto_or_explicit_nprobes)
{
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  std::unique_ptr<sql::ObSQLSessionInfo> session(new sql::ObSQLSessionInfo());
  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session->test_init(0, 0, 0, &allocator_));
  ASSERT_EQ(OB_SUCCESS, session->load_default_sys_variable(true, true));
  exec_ctx.set_my_session(session.get());

  float vector[] = {1.0F, 0.0F};
  struct ExprFrame {
    ObDatum datum_;
    sql::ObEvalInfo eval_info_;
  } frame;
  frame.datum_.set_string(reinterpret_cast<const char *>(vector), sizeof(vector));
  char *frames[] = {reinterpret_cast<char *>(&frame)};
  eval_ctx.frames_ = frames;
  sql::ObExpr vector_expr;
  vector_expr.frame_idx_ = 0;
  vector_expr.datum_off_ = offsetof(ExprFrame, datum_);
  vector_expr.eval_info_off_ = offsetof(ExprFrame, eval_info_);
  vector_expr.datum_meta_.type_ = ObVarcharType;
  vector_expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
  sql::ObExpr distance_expr;
  distance_expr.type_ = T_FUN_SYS_L2_DISTANCE;
  sql::ObDASVecIndexAsyncIVFScanCtDef ctdef(allocator_);
  ctdef.dim_ = 2;
  ctdef.sort_expr_ = &distance_expr;
  ctdef.vector_index_param_.type_ = VIAT_DISK_BBQ;
  ctdef.vector_index_param_.cluster_size_ = 512;

  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_tx(100));
  transaction::ObTxReadSnapshot snapshot;
  snapshot.init_special_read(scn);
  sql::ObAsyncIndexPartitionSegCtx partition;
  partition.storage_group_ctx_.async_index_read_snapshot_ = scn;
  sql::ObDASScanOp scan_op(allocator_);
  sql::ObDASSearchCtx search_ctx(allocator_, scan_op);
  search_ctx.set_current_async_partition(&partition);
  sql::ObDASVecIndexAsyncIVFScanIter iter;
  iter.ctdef_ = &ctdef;
  iter.snapshot_ = &snapshot;
  iter.search_ctx_ = &search_ctx;

  // An omitted query parameter keeps the existing fixed session default.
  uint64_t session_nprobe = 1;
  ASSERT_EQ(OB_SUCCESS, session->get_ob_ivf_nprobes(session_nprobe));
  ASSERT_EQ(8, session_nprobe);
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, false, 3));
  ASSERT_EQ(3, iter.query_.k_);
  ASSERT_EQ(8, iter.query_.nprobe_);
  ASSERT_FLOAT_EQ(ObVectorIndexParam::DEFAULT_REFINE_K, iter.query_.refine_k_);
  ASSERT_EQ(0, iter.query_.original_k_);
  ASSERT_EQ(0, iter.query_.num_candidates_);
  ASSERT_FALSE(iter.query_.is_set_refine_k_);
  // Exercise the JSON entry point, not just a manually constructed zero.
  sql::ObResolverParams resolver_params;
  resolver_params.allocator_ = &allocator_;
  resolver_params.session_info_ = session.get();
  sql::TableItem table_item(allocator_);
  sql::ObDMLStmt stmt(stmt::T_SELECT, allocator_);
  sql::ObDSLResolver resolver(resolver_params, nullptr, &stmt, table_item);
  for (const char *json : {"{\"nprobes\":0}", "{\"ivf_nprobes\":0}", "{\"NPROBES\":0}"}) {
    SCOPED_TRACE(json);
    ObIJsonBase *node = nullptr;
    sql::ObDSLKnnQuery::SearchOption *options = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator_, ObString::make_string(json),
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, node));
    ASSERT_EQ(OB_SUCCESS, resolver.resolve_search_options(*node, options));
    ASSERT_NE(nullptr, options);
    ASSERT_TRUE(options->param_.is_set_ivf_nprobes_);
    ASSERT_EQ(0, options->param_.ivf_nprobes_);
    ctdef.query_param_ = options->param_;
    for (const bool post : {false, true}) {
      ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, post, 3));
      EXPECT_TRUE(iter.query_.is_auto_search());
      EXPECT_EQ(0, iter.query_.nprobe_);
      EXPECT_EQ(3, iter.query_.original_k_);
      EXPECT_EQ(12, iter.query_.num_candidates_);
    }
    ObVectorIndexParam index_param;
    ObVectorIndexParam search_param;
    index_param.type_ = VIAT_DISK_BBQ;
    EXPECT_EQ(OB_SUCCESS, ObVectorIndexParam::build_search_param(index_param, options->param_, search_param));
    for (const auto type : {VIAT_IVF_FLAT, VIAT_IVF_PQ, VIAT_HNSW}) {
      index_param.type_ = type;
      EXPECT_EQ(OB_INVALID_ARGUMENT,
          ObVectorIndexParam::build_search_param(index_param, options->param_, search_param));
    }
  }
  for (const char *json : {"{\"nprobes\":-1}", "{\"nprobes\":65537}",
       "{\"nprobes\":0.0}", "{\"nprobes\":\"0\"}", "{\"nprobes\":null}",
       "{\"nprobes\":true}", "{\"nprobes\":0,\"ivf_nprobes\":1}",
       "{\"ivf_nprobes\":0,\"nprobes\":0}"}) {
    SCOPED_TRACE(json);
    ObIJsonBase *node = nullptr;
    sql::ObDSLKnnQuery::SearchOption *options = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator_, ObString::make_string(json),
        ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, node));
    EXPECT_EQ(OB_INVALID_ARGUMENT, resolver.resolve_search_options(*node, options));
  }
  // Only explicit query-level zero enables auto, including default refine_k.
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, false, 3));
  ASSERT_EQ(3, iter.query_.k_);
  ASSERT_EQ(3, iter.query_.original_k_);
  ASSERT_EQ(12, iter.query_.num_candidates_);
  ASSERT_FALSE(iter.query_.is_set_refine_k_);
  ASSERT_FLOAT_EQ(ObVectorIndexParam::DEFAULT_REFINE_K, iter.query_.refine_k_);
  ASSERT_EQ(512, iter.query_.cluster_size_);
  ASSERT_EQ(0, iter.query_.nprobe_);
  ctdef.vector_index_param_.refine_k_ = 2.5F;
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, false, 3));
  ASSERT_EQ(8, iter.query_.num_candidates_);
  ASSERT_FALSE(iter.query_.is_set_refine_k_);
  ASSERT_FLOAT_EQ(2.5F, iter.query_.refine_k_);
  ctdef.query_param_.is_set_refine_k_ = 1;
  ctdef.query_param_.refine_k_ = 1.5F;
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, true, 3));
  ASSERT_EQ(45, iter.query_.k_);
  ASSERT_EQ(3, iter.query_.original_k_);
  ASSERT_EQ(5, iter.query_.num_candidates_);
  ASSERT_TRUE(iter.query_.is_set_refine_k_);
  ASSERT_FLOAT_EQ(1.5F, iter.query_.refine_k_);
  ASSERT_FLOAT_EQ(2.5F, ctdef.vector_index_param_.refine_k_);

  ctdef.query_param_.refine_k_ = 1000.0F;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.build_query_condition(
      allocator_, eval_ctx, &vector_expr, false, INT64_MAX / 10));
  ctdef.query_param_.refine_k_ = 1.0F;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.build_query_condition(
      allocator_, eval_ctx, &vector_expr, false, INT64_MAX));
  ctdef.query_param_.refine_k_ = 1.5F;

  ctdef.query_param_.is_set_ivf_nprobes_ = 1;
  ctdef.query_param_.ivf_nprobes_ = 13;
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, true, 3));
  ASSERT_EQ(13, iter.query_.nprobe_);
  ASSERT_FLOAT_EQ(1.5F, iter.query_.refine_k_);
  ctdef.query_param_.is_set_refine_k_ = 0;
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, true, 3));
  ASSERT_FLOAT_EQ(2.5F, iter.query_.refine_k_);
  ctdef.query_param_.is_set_refine_k_ = 1;
  ASSERT_EQ(0, iter.query_.original_k_);
  ASSERT_EQ(0, iter.query_.num_candidates_);
  ctdef.query_param_.is_set_ivf_nprobes_ = 0;
  ObObj explicit_probe;
  explicit_probe.set_uint64(64);
  ASSERT_EQ(OB_SUCCESS, session->update_sys_variable(SYS_VAR_OB_IVF_NPROBES, explicit_probe));
  session->reset_session_changed_info();
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, true, 3));
  ASSERT_EQ(64, iter.query_.nprobe_);
  ASSERT_FLOAT_EQ(1.5F, iter.query_.refine_k_);
  ASSERT_EQ(0, iter.query_.num_candidates_);
  ASSERT_EQ(45, iter.query_.k_);
  // Query-level zero overrides a positive session value and re-enables auto.
  ctdef.query_param_.is_set_ivf_nprobes_ = 1;
  ctdef.query_param_.ivf_nprobes_ = 0;
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, true, 3));
  ASSERT_EQ(3, iter.query_.original_k_);
  ASSERT_EQ(5, iter.query_.num_candidates_);
  ASSERT_EQ(0, iter.query_.nprobe_);
  // Omitting the parameter again restores fixed mode, even with a stale zero value.
  ctdef.query_param_.is_set_ivf_nprobes_ = 0;
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, true, 3));
  ASSERT_EQ(64, iter.query_.nprobe_);
  ASSERT_EQ(0, iter.query_.original_k_);
  ASSERT_EQ(0, iter.query_.num_candidates_);
  ctdef.query_param_.is_set_ivf_nprobes_ = 1;
  ctdef.query_param_.ivf_nprobes_ = 21;
  ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, true, 3));
  ASSERT_EQ(21, iter.query_.nprobe_);
  ASSERT_EQ(0, iter.query_.num_candidates_);
  for (double selectivity : {0.0, 0.01, 0.25, 1.0, -1.0,
                             std::numeric_limits<double>::quiet_NaN()}) {
    iter.set_selectivity(selectivity);
    ASSERT_EQ(OB_SUCCESS, iter.build_query_condition(allocator_, eval_ctx, &vector_expr, true, 3));
    const int64_t expected = selectivity == 0.25 ? 18
        : (selectivity >= 0 && selectivity < 0.1 ? 45 : 5);
    EXPECT_EQ(expected, iter.query_.k_);
    EXPECT_EQ(21, iter.next_nprobe_);
    EXPECT_EQ(&iter.scan_budget_, iter.query_.scan_budget_);
  }
  iter.set_selectivity(0.1);
  ctdef.query_param_.refine_k_ = 1000.0F;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.build_query_condition(
      allocator_, eval_ctx, &vector_expr, true, INT64_MAX / 10));
  eval_ctx.frames_ = nullptr;
  exec_ctx.set_my_session(nullptr);
}

TEST_F(TestIVFSSTable, async_ivf_post_finishes_round_before_target_or_budget_stop)
{
  sql::ObDASVecIndexAsyncIVFScanIter iter;
  iter.need_post_filter_ = true;
  iter.limit_ = 10;
  iter.nprobe_ = 8;
  auto *child = OB_NEWx(sql::ObDASAsyncIVFSegmentScanOp, &iter.alloc_, iter.alloc_);
  ASSERT_NE(nullptr, child);
  child->opened_ = true; // first_round_ makes this an unvisited Segment.
  iter.root_.opened_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.root_.children_.push_back(child));
  iter.scan_budget_ = ObIvfScanBudget(1000);
  iter.scan_budget_.consume(5000);
  bool stop = true;
  ASSERT_EQ(OB_SUCCESS, iter.adjust_search_for_next_round(false, nullptr, 256, 10, 256, stop));
  ASSERT_FALSE(stop); // The first round completes every Segment even over budget.
  iter.post_round_end_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.adjust_search_for_next_round(false, nullptr, 256, 10, 256, stop));
  ASSERT_TRUE(stop);
  ASSERT_EQ(OB_SUCCESS, iter.adjust_search_for_next_round(false, nullptr, 256, 3, 256, stop));
  ASSERT_TRUE(stop);
  iter.post_round_end_ = false;
  iter.scan_budget_.allow_batch_stop_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.adjust_search_for_next_round(false, nullptr, 256, 3, 256, stop));
  ASSERT_TRUE(stop); // Later latency-first rounds may stop within a Segment.
  iter.scan_budget_ = ObIvfScanBudget(1000);
  iter.scan_budget_.consume(1000); // SQL stops only after exceeding the limit.
  iter.post_round_end_ = true;
  for (auto strategy : {ObVecIdxQueryStrategy::RECALL_FIRST, ObVecIdxQueryStrategy::LATENCY_FIRST}) {
    iter.strategy_ = strategy;
    ASSERT_EQ(OB_SUCCESS, iter.adjust_search_for_next_round(false, nullptr, 256, 3, 256, stop));
    EXPECT_FALSE(stop);
    EXPECT_EQ(6, iter.next_nprobe_);
    EXPECT_EQ(strategy == ObVecIdxQueryStrategy::LATENCY_FIRST, iter.scan_budget_.allow_batch_stop_);
  }
  iter.nprobe_ = 0;
  ASSERT_EQ(OB_SUCCESS, iter.adjust_search_for_next_round(false, nullptr, 256, 3, 256, stop));
  EXPECT_EQ(0, iter.next_nprobe_); // Auto nprobes remains auto on refill.
  child->opened_ = false;
  ASSERT_EQ(OB_SUCCESS, iter.adjust_search_for_next_round(false, nullptr, 0, 0, 256, stop));
  EXPECT_TRUE(stop);
  iter.reset_epoch();
  ASSERT_FALSE(iter.post_round_end_);
  ASSERT_EQ(0, iter.scan_budget_.scanned_vectors_);
}

TEST_F(TestIVFSSTable, async_ivf_quantized_candidates_are_not_scored_before_refinement)
{
  sql::ObDASVecIndexAsyncIVFScanCtDef ctdef(allocator_);
  sql::ObExpr distance_expr;
  distance_expr.type_ = T_FUN_SYS_L2_DISTANCE;
  ctdef.sort_expr_ = &distance_expr;
  ctdef.query_param_.is_set_similarity_threshold_ = 1;
  ctdef.query_param_.similarity_threshold_ = 0.4F;
  sql::ObDASVecIndexAsyncIVFScanIter iter;
  iter.ctdef_ = &ctdef;
  iter.need_post_filter_ = true;
  iter.query_.expected_dist_type_ = share::VIDA_L2;
  ObSEArray<sql::ObDASVectorCandidate, 4> candidates;
  sql::ObDASVectorCandidate candidate;
  candidate.entry_.vid_ = 42;
  candidate.entry_.distance_ = -100.0F; // A quantized estimate is not a SQL L2 distance.
  candidate.group_slot_ = 0;
  candidate.segment_slot_ = 0;
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(candidate));
  ASSERT_EQ(OB_SUCCESS, iter.publish(candidates, true /* quantized_only */));
  ASSERT_EQ(1, iter.get_adaptor_vid_iter()->get_total());
  ASSERT_EQ(42, iter.get_adaptor_vid_iter()->get_vids()[0]);
  candidates.at(0).entry_.distance_ = 1.0F;
  ASSERT_EQ(OB_SUCCESS, iter.publish(candidates, false /* quantized_only */));
  ASSERT_EQ(1, iter.get_adaptor_vid_iter()->get_total());
  ASSERT_FLOAT_EQ(1.0F, iter.get_adaptor_vid_iter()->get_distance()[0]);
  candidates.at(0).entry_.distance_ = 4.0F;
  ASSERT_EQ(OB_SUCCESS, iter.publish(candidates, false /* quantized_only */));
  EXPECT_EQ(0, iter.get_adaptor_vid_iter()->get_total());
}

TEST_F(TestIVFSSTableWriter, disk_bbq_quantized_topm_refines_only_filtered_vids)
{
  const int32_t dim = 64;
  float vectors[3 * dim] = {0};
  float centroids[2 * dim] = {0};
  float query[dim] = {0};
  const int64_t cids[] = {0, 0, 1};
  const int64_t vids[] = {10, 20, 30};
  vectors[0] = 1.0F;
  vectors[dim] = 2.0F;
  vectors[2 * dim] = 11.0F;
  centroids[dim] = 10.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors, cids, vids, 3, centroids, 2, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObDeletionBitmapHandle deletion;
  ASSERT_EQ(OB_SUCCESS, init_test_deletion_bitmap(allocator_, vids, 1, deletion));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 2);
  param.nprobe_ = 2;
  param.deletion_bitmap_handle_ = &deletion;
  ObIvfSSTableReadStats stats;
  param.read_stats_ = &stats;
  ObIvfKnnSearchResult candidates;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, candidates, true));
  ASSERT_EQ(2, candidates.count());
  ASSERT_EQ(20, candidates.at(0).vid_);
  ASSERT_EQ(30, candidates.at(1).vid_);
  ASSERT_EQ(0, stats.raw_vector_key_count_);
  ASSERT_EQ(0, stats.raw_vector_refine_count_);

  // Simulate a SQL predicate accepting only VID 30.
  ObIvfKnnSearchResult selected;
  ObIvfKnnSearchResult refined;
  ObIvfKnnEntry entry = candidates.at(1);
  entry.distance_ = std::numeric_limits<float>::max();
  ASSERT_EQ(OB_SUCCESS, selected.add(entry));
  ASSERT_EQ(OB_SUCCESS, searcher->refine_candidates(param, selected, refined));
  ASSERT_EQ(1, refined.count());
  ASSERT_EQ(30, refined.at(0).vid_);
  ASSERT_NEAR(121.0F, refined.at(0).distance_, 1e-3F);
  ASSERT_EQ(1, stats.raw_vector_key_count_);
  ASSERT_EQ(1, stats.raw_vector_refine_count_);

  // An estimated score must never prune a candidate during exact refinement.
  entry = candidates.at(0);
  entry.distance_ = std::numeric_limits<float>::max();
  ASSERT_EQ(OB_SUCCESS, selected.add(entry));
  param.k_ = 1;
  stats.reset();
  ASSERT_EQ(OB_SUCCESS, searcher->refine_candidates(param, selected, refined));
  ASSERT_EQ(1, refined.count());
  ASSERT_EQ(20, refined.at(0).vid_);
  ASSERT_NEAR(4.0F, refined.at(0).distance_, 1e-3F);
  ASSERT_EQ(2, stats.raw_vector_refine_count_);
}

TEST_P(TestIVFRefineMetric, disk_bbq_quantized_then_refine_preserves_metric)
{
  const int32_t dim = 4;
  const int64_t cid = 0;
  const int64_t vid = 10;
  const auto metric = GetParam().dist_type_;
  float centroid[] = {1.0F, 0.0F, 0.0F, 0.0F};
  float vector[] = {1.0F, 0.0F, 0.0F, 0.0F};
  float query[] = {3.0F, 4.0F, 0.0F, 0.0F};
  char flip[4 * dim / CHAR_BIT] = {0};
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1,
      centroid, 1, dim, flip, 1, nullptr, metric, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  ObIvfSSTableReadStats stats;
  param.read_stats_ = &stats;
  ObIvfKnnSearchResult candidates;
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, candidates, true));
  ASSERT_EQ(1, candidates.count());
  ASSERT_EQ(0, stats.raw_vector_key_count_);
  ASSERT_EQ(OB_SUCCESS, searcher->refine_candidates(param, candidates, result));
  ASSERT_EQ(1, result.count());
  const float expected = share::VIDA_COS == metric ? 0.4F
      : (share::VIDA_IP == metric ? -2.0F : 20.0F);
  ASSERT_NEAR(expected, result.at(0).distance_, 1e-5F);
  ASSERT_EQ(1, stats.raw_vector_refine_count_);
}

TEST_F(TestIVFSSTableWriter, async_ivf_plain_post_refills_live_unique_vids)
{
  const int32_t dim = 64;
  const int64_t cids[] = {0, 1, 3}; // CID 2 has no posting rows.
  const int64_t vids[] = {10, 20, 30};
  float vectors[3][dim] = {{0}};
  float centroids[4][dim] = {{0}};
  float query[dim] = {0};
  vectors[0][0] = 1.0F;
  vectors[1][0] = 11.0F;
  vectors[2][0] = 31.0F;
  for (int64_t i = 0; i < 4; ++i) { centroids[i][0] = i * 10.0F; }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      &vectors[0][0], cids, vids, 3, &centroids[0][0], 4, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDeletionBitmapHandle deletion;
  ObDeletionBitmapHandle all_deleted;
  ASSERT_EQ(OB_SUCCESS, init_test_deletion_bitmap(allocator_, vids, 1, deletion));
  ASSERT_EQ(OB_SUCCESS, init_test_deletion_bitmap(allocator_, vids, 3, all_deleted));
  ObIvfPreFilterBitmap allowed(allocator_);
  for (int64_t vid : vids) { ASSERT_EQ(OB_SUCCESS, allowed.value_add(vid)); }
  ASSERT_EQ(OB_SUCCESS, allowed.finish());
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed));

  ObIvfPreFilterBitmap single_allowed(allocator_);
  ASSERT_EQ(OB_SUCCESS, single_allowed.value_add(vids[1]));
  ASSERT_EQ(OB_SUCCESS, single_allowed.finish());
  ObIvfPreFilter single_pre_filter;
  ASSERT_EQ(OB_SUCCESS, single_pre_filter.init(single_allowed));

  struct Case {
    const char *name_;
    int64_t nprobe_;
    int64_t limit_;
    int64_t segments_;
    int64_t expected_count_;
    ObVecIndexType path_ = VEC_INDEX_POST_WITHOUT_FILTER;
    bool delete_all_ = false;
    float similarity_threshold_ = 0;
    bool select_pre_path_ = false;
  };
  const Case cases[] = {
      {"deleted first CID and empty middle CID", 1, 2, 1, 2},
      {"duplicate VIDs across Segments", 1, 2, 2, 2},
      {"round buffer grows across many Segments", 1, 2, 32, 2},
      {"domain exhausted before K", 1, 4, 2, 2},
      {"stop once K is reached", 1, 1, 1, 1},
      {"automatic nprobes", 0, 2, 2, 2},
      {"empty catalog", 1, 2, 0, 0},
      {"all VIDs deleted", 1, 2, 1, 0, VEC_INDEX_POST_WITHOUT_FILTER, true},
      {"PRE KNN remains single round", 1, 2, 1, 0, VEC_INDEX_PRE},
      {"PRE KNN small bitmap finds VID outside nprobe", 1, 1, 1, 1,
          VEC_INDEX_PRE, false, 0, true},
      {"similarity threshold applied after TopK", 1, 1, 1, 0,
          VEC_INDEX_POST_WITHOUT_FILTER, false, 0.5F}};
  for (const Case &test : cases) {
    SCOPED_TRACE(test.name_);
    sql::ObDASVecIndexAsyncIVFScanCtDef ctdef(allocator_);
    sql::ObExpr distance_expr;
    distance_expr.type_ = T_FUN_SYS_L2_DISTANCE;
    ctdef.sort_expr_ = &distance_expr;
    ctdef.query_param_.is_set_similarity_threshold_ = test.similarity_threshold_ > 0;
    ctdef.query_param_.similarity_threshold_ = test.similarity_threshold_;
    sql::ObDASVecIndexAsyncIVFScanIter iter;
    iter.ctdef_ = &ctdef;
    iter.vec_index_type_ = test.path_;
    iter.limit_ = test.limit_;
    iter.nprobe_ = test.nprobe_;
    iter.opened_ = iter.root_.opened_ = true;
    // A POST-filter budget must not truncate an unfiltered search.
    iter.scan_budget_ = ObIvfScanBudget(1);
    iter.scan_budget_.consume(1000);
    fill_search_params(iter.query_, query, dim, test.limit_);
    iter.query_.nprobe_ = test.nprobe_;
    iter.query_.expected_dist_type_ = share::VIDA_L2;
    iter.query_.iterative_centroids_ = test.path_ == VEC_INDEX_POST_WITHOUT_FILTER;
    iter.query_.pre_filter_ = test.path_ == VEC_INDEX_PRE ? &pre_filter : nullptr;
    if (test.select_pre_path_) {
      sql::ObDASVecIndexDriverCtDef driver_ctdef(allocator_);
      driver_ctdef.filter_mode_ = ObKnnFilterMode::PRE_KNN;
      sql::ObDASVecIndexDriverIter driver;
      driver.vec_index_driver_ctdef_ = &driver_ctdef;
      driver.vec_index_scan_iter_ = &iter;
      driver.update_async_ivf_go_brute_force_after_pre_filter(single_allowed.get_cardinality());
      EXPECT_TRUE(iter.go_brute_force_);
      iter.query_.pre_filter_ = &single_pre_filter;
    }
    iter.query_.deletion_bitmap_handle_ = test.delete_all_ ? &all_deleted : &deletion;
    iter.query_.read_stats_ = &iter.stats_;
    if (test.nprobe_ == 0) {
      iter.query_.original_k_ = test.limit_;
      iter.query_.num_candidates_ = test.limit_ * 4;
    }
    scan_param_.timeout_ = ObClockGenerator::getClock() + 60 * 1000 * 1000;
    for (int64_t slot = 0; slot < test.segments_; ++slot) {
      auto *child = OB_NEWx(sql::ObDASAsyncIVFSegmentScanOp, &iter.alloc_, iter.alloc_);
      ASSERT_NE(nullptr, child);
      ASSERT_EQ(OB_SUCCESS, iter.root_.children_.push_back(child));
      child->scanner_ = OB_NEWx(storage::ObIvfSSTableScanIter, &iter.alloc_, iter.alloc_);
      ASSERT_NE(nullptr, child->scanner_);
      auto *scanner = child->scanner_;
      scanner->is_inited_ = scanner->is_query_set_ = scanner->is_sstable_switched_ = true;
      scanner->scan_param_ = &scan_param_;
      scanner->query_param_ = iter.query_;
      ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(
          sstable_, scan_param_, scan_context_, scanner->searcher_));
      child->opened_ = true;
      child->group_slot_ = 0;
      child->segment_slot_ = slot;
    }
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, iter.inner_get_next_rows(count, 128));
    ASSERT_EQ(test.expected_count_, count);
    auto *result = iter.get_adaptor_vid_iter();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(test.expected_count_, result->get_total());
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(vids[i + 1], result->get_vids()[i]);
      EXPECT_NEAR(vectors[i + 1][0], result->get_distance()[i], 1e-4F);
    }
    if (test.path_ == VEC_INDEX_POST_WITHOUT_FILTER && test.limit_ == 1) {
      EXPECT_TRUE(iter.root_.has_more()); // Neither K nor the score threshold drains later CIDs.
    } else if (test.expected_count_ < test.limit_) {
      EXPECT_FALSE(iter.root_.has_more());
    }
    EXPECT_EQ(OB_ITER_END, iter.inner_get_next_rows(count, 128));
    EXPECT_EQ(0, count);
  }
}

TEST_F(TestIVFSSTableWriter, async_ivf_post_root_filters_before_refining_each_segment)
{
  const int32_t dim = 64;
  float vector[dim] = {1.0F};
  float centroid[dim] = {0};
  const int64_t cid = 0;
  const int64_t vid = 1;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vector, &cid, &vid, 1, centroid, 1, dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  // Exercise the real ScanIter/Segment/Root wrappers. Only selected VIDs may
  // reach RAW refinement, and they must return to their originating Segment.
  class BatchSearcher : public ObIvfSSTableSearcher
  {
  public:
    BatchSearcher(ObIAllocator &alloc, ObIvfSSTable &table, int64_t slot)
      : ObIvfSSTableSearcher(alloc), slot_(slot), calls_(0), refined_(0)
    { sstable_ = &table; is_inited_ = true; }
    int do_knn_search(const ObIvfKnnSearchParam &, ObIvfTopKCollector &) override
    { return OB_ERR_UNEXPECTED; }
    int do_brute_force_search(const ObIvfKnnSearchParam &, ObIvfTopKCollector &) override
    { return OB_ERR_UNEXPECTED; }
    int do_quantized_search(const ObIvfKnnSearchParam &, ObIvfTopKCollector &collector) override
    {
      ++calls_;
      int ret = OB_SUCCESS;
      if (slot_ < 2) {
        // Force the Segment result to grow beyond its 16 inline entries.
        for (int64_t i = 1; OB_SUCC(ret) && i <= 64; ++i) {
          ret = collector.push(0, slot_ * 1000 + i, i * 100.0F);
        }
      }
      return ret;
    }
    int do_refine_candidates(const ObIvfKnnSearchParam &, const ObIvfKnnSearchResult &selected,
        ObIvfTopKCollector &collector) override
    {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < selected.count(); ++i) {
        EXPECT_EQ(slot_ * 1000 + (i + 1) * 2, selected.at(i).vid_);
        ++refined_;
        ret = collector.push(0, selected.at(i).vid_, 4.0F - slot_ + i * 0.01F);
      }
      return ret;
    }
    int64_t slot_;
    int64_t calls_;
    int64_t refined_;
  };
  scan_param_.timeout_ = ObClockGenerator::getClock() + 60 * 1000 * 1000;
  sql::ObDASAsyncIVFTopKMergeOp root(allocator_);
  root.opened_ = true;
  ObIvfKnnSearchParam param;
  fill_search_params(param, vector, dim, 64);
  BatchSearcher *sources[3] = {nullptr};
  for (int64_t slot = 0; slot < 3; ++slot) {
    void *buf = allocator_.alloc(sizeof(sql::ObDASAsyncIVFSegmentScanOp));
    ASSERT_NE(nullptr, buf);
    auto *child = new (buf) sql::ObDASAsyncIVFSegmentScanOp(allocator_);
    ASSERT_EQ(OB_SUCCESS, root.children_.push_back(child));
    buf = allocator_.alloc(sizeof(storage::ObIvfSSTableScanIter));
    ASSERT_NE(nullptr, buf);
    child->scanner_ = new (buf) storage::ObIvfSSTableScanIter(allocator_);
    auto *scanner = child->scanner_;
    scanner->is_inited_ = scanner->is_query_set_ = scanner->is_sstable_switched_ = true;
    scanner->scan_param_ = &scan_param_;
    scanner->query_param_ = param;
    buf = allocator_.alloc(sizeof(BatchSearcher));
    ASSERT_NE(nullptr, buf);
    sources[slot] = new (buf) BatchSearcher(allocator_, sstable_, slot);
    scanner->searcher_ = sources[slot];
    child->opened_ = true;
    child->group_slot_ = 0;
    child->segment_slot_ = slot;
  }
  ObIvfTopKCollector accepted;
  ASSERT_EQ(OB_SUCCESS, accepted.init(64, allocator_));
  ObSEArray<sql::ObDASVectorCandidate, 16> batch;
  ObSEArray<sql::ObDASVectorCandidate, 16> refined;
  bool round_end = false;
  for (int64_t call = 0; call < 3; ++call) {
    ASSERT_EQ(OB_SUCCESS, root.next_post_filter_batch(1, batch, round_end));
    ASSERT_EQ(2 == call, round_end);
    ASSERT_EQ(1, sources[call]->calls_);
    ASSERT_EQ(0, sources[call]->refined_);
    if (call < 2) {
      ASSERT_EQ(64, batch.count());
      ASSERT_FLOAT_EQ(200.0F, batch.at(1).entry_.distance_);
      ObIvfKnnSearchResult selected;
      for (int64_t i = 1; i < batch.count(); i += 2) {
        ASSERT_EQ(OB_SUCCESS, selected.add(batch.at(i).entry_));
      }
      ASSERT_EQ(OB_SUCCESS, root.refine_candidates(selected, refined));
      ASSERT_EQ(32, sources[call]->refined_);
      ASSERT_EQ(32, refined.count());
      ASSERT_EQ(call, refined.at(0).segment_slot_);
      ASSERT_FLOAT_EQ(4.0F - call, refined.at(0).entry_.distance_);
      for (int64_t i = 0; i < refined.count(); ++i) {
        const auto &entry = refined.at(i).entry_;
        ASSERT_EQ(OB_SUCCESS, accepted.push(entry.cid_, entry.vid_, entry.distance_));
      }
    } else {
      ASSERT_TRUE(batch.empty());
    }
  }
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, accepted.finish(result));
  ASSERT_EQ(64, result.count());
  ASSERT_EQ(1002, result.at(0).vid_);
  ASSERT_EQ(2, result.at(32).vid_);
  ASSERT_FALSE(root.has_more());
  root.close();
  ASSERT_TRUE(root.children_.empty());
  ASSERT_EQ(0, root.next_post_child_);
}

TEST_F(TestIVFSSTableWriter, disk_bbq_post_stream_finite_threshold_does_not_hide_soar_copy)
{
  const int32_t dim = 64;
  float encoded[3 * dim] = {0};
  float raw[3 * dim] = {0};
  float centroids[2 * dim] = {0};
  float query[dim] = {0};
  const int64_t cids[] = {0, 0, 1};
  const int64_t vids[] = {10, 20, 20};
  centroids[dim + 1] = 0.1F;
  encoded[0] = raw[0] = 1.0F;
  encoded[dim] = 1000.0F;
  encoded[2 * dim] = raw[dim] = raw[2 * dim] = 0.01F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, cids, vids, 3,
      centroids, 2, dim, nullptr, 1, nullptr, share::VIDA_L2, true,
      raw, nullptr, 0, true));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  param.nprobe_ = 1;
  ObIvfSearcherGuard searcher;
  ASSERT_EQ(OB_SUCCESS, create_ivf_searcher(sstable_, scan_param_, scan_context_, searcher.get()));
  ObIvfKnnSearchResult batch;
  ObIvfCandidateProgress progress;
  sstable_.ivf_meta_.vector_count_ = 2;
  for (const int64_t nprobe : {1, 0}) {
    SCOPED_TRACE(nprobe);
    searcher->reuse();
    param.nprobe_ = nprobe;
    param.original_k_ = 1;
    param.num_candidates_ = 4;
    ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param, 2.0F, batch, progress, 32));
    ASSERT_EQ(0 == nprobe ? ObIvfCandidateProgress::MORE : ObIvfCandidateProgress::WINDOW_END, progress.state_);
    ASSERT_EQ(1, batch.count());
    ASSERT_EQ(10, batch.at(0).vid_);
    ASSERT_EQ(OB_SUCCESS, searcher->next_candidate_batch(param, 1.0F, batch, progress, 32));
    ASSERT_EQ(ObIvfCandidateProgress::DOMAIN_END, progress.state_);
    ASSERT_EQ(1, batch.count());
    ASSERT_EQ(20, batch.at(0).vid_);
    ASSERT_NEAR(0.0001F, batch.at(0).distance_, 1e-6F);
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase;
  signal(49, SIG_IGN);
  system("rm -f test_ivf_sstable.log*");
  OB_LOGGER.set_file_name("test_ivf_sstable.log", true);
  OB_LOGGER.set_log_level("INFO");
  // Suppress grep output from CpuFlagSet::init_from_os() when detecting CPU flags.
  fflush(stdout);
  const int saved_stdout = dup(STDOUT_FILENO);
  if (saved_stdout >= 0) {
    freopen("/dev/null", "w", stdout);
    oceanbase::common::init_arches();
    fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);
  } else {
    oceanbase::common::init_arches();
  }
  testing::InitGoogleTest(&argc, argv);
  MTL_BIND2(mtl_new_default,
      oceanbase::share::ObPluginVectorIndexService::mtl_init,
      mtl_start_default,
      mtl_stop_default,
      mtl_wait_default,
      mtl_destroy_default);
  if (OB_SUCCESS != common::ObRbMemMgr::init_memory_hook()) { return 1; }
  if (OB_SUCCESS != oceanbase::MockTenantModuleEnv::get_instance().init()) {
    return 1;
  }
  const int ret = RUN_ALL_TESTS();
  oceanbase::MockTenantModuleEnv::get_instance().destroy();
  std::_Exit(ret);
}
