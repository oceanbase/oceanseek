/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX STORAGE

#include "test_disk_bbq_common.h"
#include "lib/utility/ob_defer.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_scan_iter.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_capture_pipeline.h"
#include "storage/compaction/ob_async_index_merge_task.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "sql/das/iter/ob_das_ivf_scan_iter.h"
#undef private
#undef protected

#include <limits>
#include <memory>
#include <chrono>
#include <algorithm>

#include "sql/das/search/ob_das_async_ivf_search_op.h"
#include "storage/test_dml_common.h"
#include "storage/test_tablet_helper.h"

namespace oceanbase
{
namespace unittest
{

using blocksstable::ObIvfPreFilter;

class TestIvfSSTableScanIter : public TestIVFSSTableWriter
{
protected:
  static constexpr int64_t TEST_LS_ID = 9107;
  static constexpr int64_t SQL_READ_SNAPSHOT_VERSION = 10 * IVF_SNAPSHOT_VERSION;

  int build_ordered_raw_input(const uint64_t *vids, const int64_t count,
      const int64_t source_idx, const bool split_macros = true)
  {
    int ret = OB_SUCCESS;
    sstable_.reset();
    sstable_builder_.reset();
    object_cleaner_.reset();
    whole_desc_.reset();
    table_schema_.reset();
    ObMacroBlockWriter writer;
    ObDatumRow row;
    float vector[64] = {1.0F};
    vector[1] = static_cast<float>(source_idx);
    if (OB_FAIL(prepare_write_env())) {
    } else if (OB_FAIL(open_writer(writer, 0))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      vector[2] = static_cast<float>(vids[i]);
      ObString bytes(sizeof(vector), reinterpret_cast<char *>(vector));
      if (OB_FAIL(make_ivf_row(row, IVF_ROW_TYPE_RAW_VECTOR, IVF_RAW_VECTOR_CID, vids[i], bytes))) {
      } else if (OB_FAIL(writer.append_row(row))) {
      } else if (split_macros) {
        if (OB_FAIL(writer.build_micro_block())) {
        } else if (OB_FAIL(writer.try_switch_macro_block())) {
        }
      }
    }
    if (OB_SUCC(ret)) { ret = writer.close(); }
    if (OB_SUCC(ret)) { ret = close_builder_and_init_ivf_sstable(64); }
    if (OB_SUCC(ret)) { ret = object_cleaner_.mark_succeed(); }
    return ret;
  }

  int build_merge_raw_input(const bool split_macros = false, const bool zero_first = false,
      const bool valid_payloads = false)
  {
    int ret = OB_SUCCESS;
    ObMacroBlockWriter writer;
    ObDatumRow row;
    float vector[64] = {1.0F};
    if (OB_FAIL(prepare_write_env())) {
    } else if (OB_FAIL(open_writer(writer, 0))) {
    }
    // Gapped VIDs and valid storage checksums, including malformed vector
    // payloads: replay capture validates live vectors and skips deleted ones.
    for (int64_t i = 0; OB_SUCC(ret) && i < 6; ++i) {
      vector[1] = static_cast<float>(i);
      vector[0] = !valid_payloads && i == 2 ? std::numeric_limits<float>::infinity()
          : !valid_payloads && i == 3 ? std::numeric_limits<float>::quiet_NaN() : 1.0F;
      ObString bytes(!valid_payloads && i == 4 ? sizeof(float) : sizeof(vector), reinterpret_cast<char *>(vector));
      const uint64_t vid = zero_first && i == 0 ? 0 : 10 + i * 20;
      if (OB_FAIL(make_ivf_row(row, IVF_ROW_TYPE_RAW_VECTOR, IVF_RAW_VECTOR_CID, vid, bytes))) {
      } else {
        if (!valid_payloads && i == 5) { row.storage_datums_[5].set_null(); }
        ret = writer.append_row(row);
        if (OB_SUCC(ret) && split_macros) {
          if (OB_FAIL(writer.build_micro_block())) {
          } else if (OB_FAIL(writer.try_switch_macro_block())) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) { ret = writer.close(); }
    if (OB_SUCC(ret)) { ret = close_builder_and_init_ivf_sstable(64); }
    if (OB_SUCC(ret)) { ret = object_cleaner_.mark_succeed(); }
    return ret;
  }

  void prepare_merge_input(compaction::ObAsyncIndexMergeCtx &ctx,
      compaction::ObIVFMergeInputAdapter &input, const bool delete_bad, const bool delete_all,
      const bool zero_first = false)
  {
    ctx.static_param_.is_ivf_index_ = true;
    ctx.static_param_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
    ASSERT_EQ(OB_SUCCESS, ctx.read_info_.init(allocator_, table_schema_.get_column_count(),
        table_schema_.get_rowkey_column_num(), false, whole_desc_.get_desc().get_full_stored_col_descs()));
    ObTableHandleV2 handle;
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&sstable_));
    ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(handle));
    ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
    ASSERT_NE(nullptr, bitmap);
    if (delete_bad || delete_all) {
      for (int64_t i = delete_all ? 0 : 2; i < 6; ++i) {
        ASSERT_EQ(OB_SUCCESS, bitmap->value_add(zero_first && i == 0 ? 0 : 10 + i * 20));
      }
    }
    ASSERT_EQ(OB_SUCCESS, ctx.deletion_bitmap_handle_.init_from_bitmap(allocator_, bitmap));
    input.index_table_id_ = table_schema_.get_table_id();
    input.index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
    input.vector_dim_ = 64;
    input.max_sample_count_ = 16;
    input.dist_type_ = share::VIDA_L2;
    ASSERT_EQ(OB_SUCCESS, input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  }
  static void expect_profile_metric(common::ObOpProfile<common::ObMetric> &profile,
                                    common::ObMetricId id, uint64_t expected)
  {
    bool exists = false;
    uint64_t value = 0;
    profile.get_metric_value(id, exists, value);
    ASSERT_TRUE(exists);
    ASSERT_EQ(expected, value);
  }

  static void SetUpTestCase()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    SERVER_STORAGE_META_SERVICE.is_started_ = true;
    ASSERT_EQ(OB_SUCCESS, TestDmlCommon::create_ls(
        OB_SERVER_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle_));
  }

  static void TearDownTestCase()
  {
    ls_handle_.reset();
    ObLSService *ls_service = MTL(ObLSService *);
    ASSERT_NE(nullptr, ls_service);
    ASSERT_EQ(OB_SUCCESS, ls_service->remove_ls(ObLSID(TEST_LS_ID)));
  }

  void SetUp() override
  {
    TestIVFSSTableWriter::SetUp();
    tablet_handle_.reset();
  }

  void TearDown() override
  {
    if (tablet_handle_.is_valid()) {
      ObTabletTableStore *table_store = tablet_handle_.get_obj()->table_store_addr_.get_ptr();
      if (nullptr != table_store) {
        table_store->major_tables_.reset();
        table_store->inc_major_tables_.reset();
      }
      tablet_handle_.reset();
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
      ASSERT_NE(nullptr, t3m);
      ASSERT_EQ(OB_SUCCESS, t3m->del_tablet(
          ObTabletMapKey(ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID))));
    }
    TestIVFSSTableWriter::TearDown();
  }

  int build_two_vector_sstable(const int32_t dim = 64)
  {
    int ret = OB_SUCCESS;
    const int64_t cids[] = {0, 1};
    const int64_t vids[] = {10, 20};
    std::vector<float> vectors(ARRAYSIZEOF(cids) * dim, 0);
    std::vector<float> centroids(ARRAYSIZEOF(cids) * dim, 0);
    std::vector<char> flip(4 * dim / CHAR_BIT, 0);
    vectors[0] = 1.0F;
    vectors[dim] = -1.0F;
    if (OB_FAIL(prepare_write_env())) {
      LOG_WARN("failed to prepare ivf test write environment", K(ret));
    } else if (OB_FAIL(write_disk_bbq_index(
        vectors.data(), cids, vids, ARRAYSIZEOF(cids), centroids.data(),
        ARRAYSIZEOF(cids), dim, flip.data()))) {
      LOG_WARN("failed to write ivf test sstable", K(ret));
    } else if (OB_FAIL(object_cleaner_.mark_succeed())) {
      LOG_WARN("failed to retain ivf test macro blocks", K(ret));
    }
    return ret;
  }

  int build_five_vector_sstable(const int32_t dim = 64)
  {
    int ret = OB_SUCCESS;
    const int64_t cids[] = {0, 1, 2, 3, 4};
    const int64_t vids[] = {10, 20, 30, 40, 50};
    std::vector<float> vectors(ARRAYSIZEOF(cids) * dim, 0);
    std::vector<float> centroids(ARRAYSIZEOF(cids) * dim, 0);
    for (int64_t i = 1; i < ARRAYSIZEOF(cids); ++i) {
      vectors[i * dim] = static_cast<float>(i * 10);
      centroids[i * dim] = static_cast<float>(i * 10);
    }
    if (OB_FAIL(prepare_write_env())) {
      LOG_WARN("failed to prepare ivf test write environment", K(ret));
    } else if (OB_FAIL(write_disk_bbq_index(
        vectors.data(), cids, vids, ARRAYSIZEOF(cids), centroids.data(),
        ARRAYSIZEOF(cids), dim, nullptr))) {
      LOG_WARN("failed to write five-cid ivf test sstable", K(ret));
    } else if (OB_FAIL(object_cleaner_.mark_succeed())) {
      LOG_WARN("failed to retain five-cid ivf test macro blocks", K(ret));
    }
    return ret;
  }

  int clone_as_inc_ivf_sstable(
      const int64_t base_version,
      const int64_t snapshot_version,
      ObIvfSSTable *&inc_sstable)
  {
    int ret = OB_SUCCESS;
    ObSSTable *copied_sstable = nullptr;
    inc_sstable = nullptr;
    if (OB_FAIL(sstable_.ObSSTable::deep_copy(allocator_, copied_sstable))) {
      LOG_WARN("failed to clone ivf test sstable", K(ret));
    } else if (OB_ISNULL(copied_sstable)
        || OB_UNLIKELY(!copied_sstable->is_ivf_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cloned ivf test sstable", K(ret), KP(copied_sstable));
    } else {
      copied_sstable->key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
      copied_sstable->meta_cache_.min_merged_trans_version_ = base_version;
      copied_sstable->meta_cache_.max_merged_trans_version_ = snapshot_version;
      if (OB_FAIL(copied_sstable->key_.scn_range_.start_scn_.convert_for_tx(
          base_version + 1000))) {
        LOG_WARN("failed to set inc ivf physical start scn", K(ret), K(base_version));
      } else if (OB_FAIL(copied_sstable->key_.scn_range_.end_scn_.convert_for_tx(
          snapshot_version + 1000))) {
        LOG_WARN("failed to set inc ivf physical end scn", K(ret), K(snapshot_version));
      } else if (FALSE_IT(inc_sstable = static_cast<ObIvfSSTable *>(copied_sstable))) {
      } else if (OB_FAIL(inc_sstable->set_upper_trans_version(
          allocator_, snapshot_version + 1))) {
        LOG_WARN("failed to set inc ivf commit version", K(ret), K(snapshot_version));
      } else if (OB_UNLIKELY(!inc_sstable->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid cloned inc ivf test sstable", K(ret), KPC(inc_sstable));
      }
    }
    return ret;
  }

  int install_ivf_sstable(const ObIvfSSTable *inc_sstable = nullptr)
  {
    int ret = OB_SUCCESS;
    table_schema_.set_table_type(USER_INDEX);
    table_schema_.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
    table_schema_.set_data_table_id(table_schema_.get_table_id());
    if (OB_FAIL(TestTabletHelper::create_tablet(
        ls_handle_, ObTabletID(IVF_TABLET_ID), table_schema_, allocator_,
        ObTabletStatus::NORMAL, SCN::min_scn(), tablet_handle_))) {
      LOG_WARN("failed to create ivf test tablet", K(ret));
    } else {
      ObTablet *tablet = tablet_handle_.get_obj();
      ObTabletTableStore *table_store = nullptr == tablet
          ? nullptr : tablet->table_store_addr_.get_ptr();
      if (OB_ISNULL(table_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ivf test tablet table store is null", K(ret));
      } else {
        table_store->major_tables_.reset();
        table_store->inc_major_tables_.reset();
        if (OB_FAIL(table_store->major_tables_.init(allocator_, &sstable_))) {
          LOG_WARN("failed to install ivf sstable", K(ret));
        } else if (nullptr != inc_sstable
            && OB_FAIL(table_store->inc_major_tables_.init(allocator_, inc_sstable))) {
          LOG_WARN("failed to install inc ivf sstable", K(ret), KPC(inc_sstable));
        } else {
          table_store->is_inited_ = true;
          tablet->table_store_cache_.last_major_snapshot_version_ =
              sstable_.get_snapshot_version();
        }
      }
    }
    return ret;
  }

  int prepare_scan_param_and_ctx(ObIvfSSTableScanIter &iter)
  {
    int ret = OB_SUCCESS;
    SCN snapshot_scn;
    scan_param_.tenant_id_ = OB_SERVER_TENANT_ID;
    scan_param_.ls_id_ = ObLSID(TEST_LS_ID);
    scan_param_.tablet_id_ = ObTabletID(IVF_TABLET_ID);
    scan_param_.index_id_ = IVF_TABLE_ID;
    scan_param_.timeout_ = ObClockGenerator::getClock() + 60_s;
    scan_param_.reserved_cell_count_ = table_schema_.get_column_count();
    scan_param_.schema_version_ = 10;
    scan_param_.tenant_schema_version_ = 10;
    scan_param_.allocator_ = &allocator_;
    scan_param_.scan_allocator_ = &allocator_;
    if (OB_FAIL(snapshot_scn.convert_for_tx(SQL_READ_SNAPSHOT_VERSION))) {
      LOG_WARN("failed to build ivf test snapshot", K(ret));
    } else {
      scan_param_.snapshot_.init_special_read(snapshot_scn);
      ObStoreCtxGuard &ctx_guard = iter.get_ctx_guard();
      if (OB_FAIL(ctx_guard.init(ObLSID(TEST_LS_ID)))) {
        LOG_WARN("failed to init ivf store context guard", K(ret));
      } else if (OB_FAIL(MTL(ObLSService *)->get_ls(
          ObLSID(TEST_LS_ID), ctx_guard.get_ls_handle(), ObLSGetMod::STORAGE_MOD))) {
        LOG_WARN("failed to get ivf test ls", K(ret));
      } else {
        ObStoreCtx &store_ctx = ctx_guard.get_store_ctx();
        store_ctx.tablet_id_ = ObTabletID(IVF_TABLET_ID);
        store_ctx.ls_ = ctx_guard.get_ls_handle().get_ls();
        if (OB_FAIL(store_ctx.init_for_read(
            ctx_guard.get_ls_handle(), scan_param_.timeout_,
            scan_param_.tx_lock_timeout_, snapshot_scn))) {
          LOG_WARN("failed to init ivf read store context", K(ret));
        }
      }
    }
    return ret;
  }

  void run_iterative_metric_filter(
      const share::ObVectorIndexDistAlgorithm dist_type);

  static ObLSHandle ls_handle_;
  ObTabletHandle tablet_handle_;
};

ObLSHandle TestIvfSSTableScanIter::ls_handle_;

TEST_F(TestIvfSSTableScanIter, rejects_operations_before_init)
{
  ObIvfSSTableScanIter iter(allocator_);
  ObIvfKnnSearchParam query_param;
  ObIvfKnnSearchResult result;
  ObNewVersionRange version_range;
  float query[4] = {0};
  query_param.query_ = query;
  query_param.dim_ = ARRAYSIZEOF(query);
  query_param.nprobe_ = 1;
  query_param.k_ = 1;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;

  ASSERT_EQ(OB_NOT_INIT, iter.set_query(query_param));
  ASSERT_EQ(OB_NOT_INIT, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));
  ASSERT_EQ(OB_NOT_INIT, iter.knn_search(1, result));
  ASSERT_EQ(OB_NOT_INIT, iter.brute_force_search(result));
  iter.reuse();
  iter.reset();
  ASSERT_FALSE(iter.is_inited());
}

TEST_F(TestIvfSSTableScanIter, merge_replay_capture_rejects_invalid_live_payloads)
{
  ASSERT_EQ(OB_SUCCESS, build_merge_raw_input());
  for (int64_t bad = 2; bad < 6; ++bad) {
    SCOPED_TRACE(bad);
    compaction::ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
    compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
    compaction::ObIVFMergeInputAdapter input(allocator_);
    ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, input, false, false));
    ctx.deletion_bitmap_handle_.reset();
    ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
    ASSERT_NE(nullptr, bitmap);
    for (int64_t i = 2; i < 6; ++i) {
      if (i != bad) { ASSERT_EQ(OB_SUCCESS, bitmap->value_add(10 + i * 20)); }
    }
    ASSERT_EQ(OB_SUCCESS, ctx.deletion_bitmap_handle_.init_from_bitmap(allocator_, bitmap));
    EXPECT_NE(OB_SUCCESS, input.init_source_(ctx));
    EXPECT_FALSE(input.is_valid());
  }
}

TEST_F(TestIvfSSTableScanIter, merge_vid_capture_filters_deleted_payloads_and_samples)
{
  ASSERT_EQ(OB_SUCCESS, build_merge_raw_input());
  compaction::ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
  compaction::ObIVFMergeInputAdapter input(allocator_);
  ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, input, true, false));
  ASSERT_EQ(OB_SUCCESS, input.init_source_(ctx));
  ASSERT_EQ(2, input.get_vector_count());
  ASSERT_EQ(1, input.replay_inputs_.count());
  EXPECT_EQ(0, input.replay_memory_reservation_.get_reserved_bytes());
  EXPECT_GT(input.replay_inputs_.at(0)->get_file_size(), 0);
  EXPECT_EQ(0, input.replay_inputs_.at(0)->get_inmemory_row_count());
  // Vector reads use replay only, after capture releases its slices.
  input.source_tables_.reset();
  input.read_info_ = nullptr;
  input.ls_id_.reset();
  ObArray<storage::ObIVFVectorVID> vids;
  ObArray<float> values;
  ASSERT_EQ(OB_SUCCESS, input.batch_get_samples(0, 2, vids, values));
  ASSERT_EQ(10, vids.at(0));
  ASSERT_EQ(30, vids.at(1));
  ASSERT_EQ(128, values.count());
  vids.reset();
  ASSERT_EQ(OB_SUCCESS, vids.push_back(50));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, input.batch_get_by_vid(vids, 0, 1, values));
  ASSERT_TRUE(values.empty());
}

TEST_F(TestIvfSSTableScanIter, merge_batch_reuses_capacity_and_preserves_requested_order)
{
  ASSERT_EQ(OB_SUCCESS, build_merge_raw_input());
  compaction::ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
  compaction::ObIVFMergeInputAdapter input(allocator_);
  ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, input, true, false));
  ASSERT_EQ(OB_SUCCESS, input.init_source_(ctx));
  ObArenaAllocator output_allocator("IVFReadTest");
  ObSEArray<float, 16, ObWrapperAllocator> values(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(output_allocator));
  ASSERT_EQ(OB_SUCCESS, values.prepare_allocate(65536));
  const float *data = values.get_data();
  const int64_t used = output_allocator.used();
  ObArray<storage::ObIVFVectorVID> vids;
  for (const auto vid : {10, 30, 30}) { ASSERT_EQ(OB_SUCCESS, vids.push_back(vid)); }
  for (int round = 0; round < 3; ++round) {
    ASSERT_EQ(OB_SUCCESS, input.batch_get_by_vid(vids, 0, 3, values));
    ASSERT_EQ(192, values.count());
    EXPECT_FLOAT_EQ(0.0F, values.at(1));
    EXPECT_FLOAT_EQ(1.0F, values.at(65));
    EXPECT_FLOAT_EQ(1.0F, values.at(129));
    EXPECT_EQ(data, values.get_data());
    EXPECT_EQ(used, output_allocator.used());
    ASSERT_EQ(OB_SUCCESS, input.batch_get_by_vid(vids, 1, 2, values));
    ASSERT_EQ(64, values.count());
    EXPECT_FLOAT_EQ(1.0F, values.at(1));
    EXPECT_EQ(data, values.get_data());
  }
  vids.reuse();
  for (const auto vid : {30, 10, 30}) { ASSERT_EQ(OB_SUCCESS, vids.push_back(vid)); }
  ASSERT_EQ(OB_SUCCESS, input.batch_get_by_vid(vids, 0, 3, values));
  EXPECT_FLOAT_EQ(1.0F, values.at(1));
  EXPECT_FLOAT_EQ(0.0F, values.at(65));
  EXPECT_FLOAT_EQ(1.0F, values.at(129));
  for (const auto missing : {0, 20, 50, 200}) {
    vids.reuse();
    ASSERT_EQ(OB_SUCCESS, vids.push_back(10));
    ASSERT_EQ(OB_SUCCESS, vids.push_back(missing));
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, input.batch_get_by_vid(vids, 0, 2, values));
    EXPECT_EQ(0, values.count());
    EXPECT_EQ(data, values.get_data());
    EXPECT_EQ(used, output_allocator.used());
  }
  ASSERT_EQ(OB_INVALID_ARGUMENT, input.batch_get_by_vid(vids, INT64_MAX, INT64_MAX, values));
  EXPECT_TRUE(values.empty());
  ASSERT_EQ(OB_SUCCESS, input.batch_get_by_vid(vids, 0, 1, values));
  EXPECT_EQ(64, values.count());
  EXPECT_EQ(data, values.get_data());
  EXPECT_EQ(used, output_allocator.used());
}

TEST_F(TestIvfSSTableScanIter, merge_vid_capture_all_deleted_is_empty)
{
  ASSERT_EQ(OB_SUCCESS, build_merge_raw_input());
  compaction::ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
  compaction::ObIVFMergeInputAdapter input(allocator_);
  ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, input, false, true));
  ASSERT_EQ(OB_SUCCESS, input.init_source_(ctx));
  ASSERT_TRUE(input.is_empty());
  ASSERT_EQ(0, input.get_sample_count());
}

TEST_F(TestIvfSSTableScanIter, parallel_capture_matches_serial_across_macro_boundaries)
{
  ASSERT_EQ(OB_SUCCESS, build_merge_raw_input(true, true /* zero_first */, true /* valid_payloads */));
  ASSERT_GE(sstable_.get_data_macro_block_count(), 6);
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    compaction::ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
    compaction::ObAsyncIndexMajorMergeDag dag;
    compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
    compaction::ObIVFMergeInputAdapter serial(allocator_);
    ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, serial, mode == 1, mode == 2, true /* zero_first */));
    table_schema_.set_tablet_size(OB_STORAGE_OBJECT_MGR.get_macro_block_size());
    ObStorageSchema *schema = OB_NEWx(ObStorageSchema, &allocator_);
    ASSERT_NE(nullptr, schema);
    ASSERT_EQ(OB_SUCCESS, schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    ctx.static_param_.schema_ = schema;
    ctx.merge_dag_ = &dag;
    serial.max_sample_count_ = 2;
    ASSERT_EQ(OB_SUCCESS, serial.init_source_(ctx));
    compaction::ObIVFMergeInputAdapter parallel(allocator_);
    parallel.index_table_id_ = serial.index_table_id_;
    parallel.index_tablet_id_ = serial.index_tablet_id_;
    parallel.vector_dim_ = serial.vector_dim_;
    parallel.max_sample_count_ = serial.max_sample_count_;
    parallel.dist_type_ = serial.dist_type_;
    parallel.row_desc_ = serial.row_desc_;
    ASSERT_EQ(OB_SUCCESS, parallel.prepare_source_(ctx));
    compaction::ObIVFMergeCaptureCtx capture;
    ASSERT_EQ(OB_SUCCESS, capture.init(ctx, parallel));
    ASSERT_GT(capture.get_task_count(), 1);
    int64_t worker_limit = 0;
    ASSERT_EQ(OB_SUCCESS, MTL(share::ObTenantDagScheduler *)->get_limit(ctx.get_dag_priority(), worker_limit));
    ASSERT_LE(capture.get_task_count(), worker_limit);
    // Fan-in may not publish a partial input. Complete slices out of order.
    ASSERT_EQ(OB_STATE_NOT_MATCH, capture.finish());
    for (int64_t i = capture.get_task_count() - 1; i >= 0; --i) {
      ASSERT_EQ(OB_SUCCESS, capture.scan(i));
    }
    ASSERT_EQ(OB_SUCCESS, capture.finish());
    ASSERT_EQ(serial.is_empty(), parallel.is_empty());
    ASSERT_EQ(serial.get_vector_count(), parallel.get_vector_count());
    ASSERT_EQ(serial.get_sample_count(), parallel.get_sample_count());
    if (mode != 2) {
      ASSERT_EQ(0, parallel.locators_.at(0).vid_);
      ObArray<storage::ObIVFVectorVID> zero_vid;
      ObArray<float> values;
      ASSERT_EQ(OB_SUCCESS, zero_vid.push_back(0));
      ASSERT_EQ(OB_SUCCESS, parallel.batch_get_by_vid(zero_vid, 0, 1, values));
      ASSERT_EQ(64, values.count());
      EXPECT_FLOAT_EQ(1.0F, values.at(0));
    }
    for (int64_t i = 0; i < serial.locators_.count(); ++i) {
      EXPECT_EQ(serial.locators_.at(i).vid_, parallel.locators_.at(i).vid_);
      EXPECT_EQ(serial.locators_.at(i).table_idx_, parallel.locators_.at(i).table_idx_);
    }
    for (int64_t i = 0; i < serial.sample_locators_.count(); ++i) {
      EXPECT_EQ(serial.sample_locators_.at(i).vid_, parallel.sample_locators_.at(i).vid_);
    }
    EXPECT_EQ(serial.prior_.segments_.at(0).live_vector_count_, parallel.prior_.segments_.at(0).live_vector_count_);
    EXPECT_EQ(serial.prior_.dominant_segment_, parallel.prior_.dominant_segment_);
    ASSERT_EQ(OB_STATE_NOT_MATCH, capture.finish());
  }
}

TEST_F(TestIvfSSTableScanIter, parallel_capture_rejects_duplicate_sources_at_fan_in)
{
  ASSERT_EQ(OB_SUCCESS, build_merge_raw_input(true, false, true));
  compaction::ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
  compaction::ObIVFMergeInputAdapter input(allocator_);
  ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, input, false, false));
  ObTableHandleV2 handle;
  ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&sstable_));
  ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(handle));
  table_schema_.set_tablet_size(OB_STORAGE_OBJECT_MGR.get_macro_block_size());
  ObStorageSchema *schema = OB_NEWx(ObStorageSchema, &allocator_);
  ASSERT_NE(nullptr, schema);
  ASSERT_EQ(OB_SUCCESS, schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  ctx.static_param_.schema_ = schema;
  compaction::ObAsyncIndexMajorMergeDag dag;
  ctx.merge_dag_ = &dag;
  ASSERT_EQ(OB_SUCCESS, input.prepare_source_(ctx));
  compaction::ObIVFMergeCaptureCtx capture;
  ASSERT_EQ(OB_SUCCESS, capture.init(ctx, input));
  ASSERT_GT(capture.get_task_count(), 1);
  for (int64_t i = 0; i < capture.get_task_count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, capture.scan(i));
  }
  ASSERT_EQ(OB_INVALID_DATA, capture.finish());
  ASSERT_FALSE(input.is_valid());
}

TEST_F(TestIvfSSTableScanIter, merge_capture_orders_interleaved_segments_and_preserves_payloads)
{
  const uint64_t first_vids[] = {0, 30, 50};
  const uint64_t second_vids[] = {20, 40, 60};
  auto destroy_table = [](ObIvfSSTable *table) {
    if (nullptr != table) { table->~ObIvfSSTable(); }
  };
  ObIvfSSTable *copied = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_ordered_raw_input(first_vids, 3, 0));
  ASSERT_EQ(OB_SUCCESS, clone_as_inc_ivf_sstable(
      IVF_SNAPSHOT_VERSION, 2 * IVF_SNAPSHOT_VERSION, copied));
  std::unique_ptr<ObIvfSSTable, decltype(destroy_table)> first(copied, destroy_table);
  // deep_copy alone does not retain blocks when the fixture writer is reused.
  bool retained = false;
  ASSERT_EQ(OB_SUCCESS, first->inc_macro_ref(retained));
  ASSERT_TRUE(retained);
  first->is_tmp_sstable_ = true;
  ASSERT_EQ(OB_SUCCESS, build_ordered_raw_input(second_vids, 3, 1));
  ASSERT_EQ(OB_SUCCESS, clone_as_inc_ivf_sstable(
      2 * IVF_SNAPSHOT_VERSION, 3 * IVF_SNAPSHOT_VERSION, copied));
  std::unique_ptr<ObIvfSSTable, decltype(destroy_table)> second(copied, destroy_table);

  const uint64_t expected[] = {0, 20, 30, 40, 50, 60};
  const int64_t sources[] = {0, 1, 0, 1, 0, 1};
  // Include an empty live source and an entirely deleted input. The remaining
  // VIDs must be independent of task boundaries and sample budget.
  for (int mode = 0; mode < 4; ++mode) {
    std::vector<uint64_t> live_vids;
    std::vector<int64_t> live_sources;
    auto deleted = [&](uint64_t vid) {
      return mode == 3 || (mode == 1 && (vid == 30 || vid == 40))
          || (mode == 2 && (vid == 0 || vid == 30 || vid == 50));
    };
    for (int64_t i = 0; i < 6; ++i) {
      if (!deleted(expected[i])) {
        live_vids.push_back(expected[i]);
        live_sources.push_back(sources[i]);
      }
    }
    for (const int64_t sample_budget : {16, 2}) {
      std::vector<uint64_t> serial_samples;
      for (const bool parallel : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "mode=" << mode << " parallel=" << parallel
            << " sample_budget=" << sample_budget);
        compaction::ObTabletMergeDagParam param(mode == 0 ? INC_MAJOR_MERGE : MAJOR_MERGE,
            ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
        compaction::ObAsyncIndexMajorMergeDag dag;
        compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
        compaction::ObIVFMergeInputAdapter input(allocator_);
        ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, input, false, false));
        ctx.static_param_.tables_handle_.reset();
        for (ObIvfSSTable *table : {first.get(), second.get()}) {
          ObTableHandleV2 handle;
          ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(table));
          ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(handle));
        }
        ctx.deletion_bitmap_handle_.reset();
        if (mode != 0) {
          ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
          ASSERT_NE(nullptr, bitmap);
          for (const auto vid : expected) {
            if (deleted(vid)) { ASSERT_EQ(OB_SUCCESS, bitmap->value_add(vid)); }
          }
          ASSERT_EQ(OB_SUCCESS, ctx.deletion_bitmap_handle_.init_from_bitmap(allocator_, bitmap));
        }
        table_schema_.set_tablet_size(OB_STORAGE_OBJECT_MGR.get_macro_block_size());
        ObStorageSchema *schema = OB_NEWx(ObStorageSchema, &allocator_);
        ASSERT_NE(nullptr, schema);
        ASSERT_EQ(OB_SUCCESS, schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
        ctx.static_param_.schema_ = schema;
        ctx.merge_dag_ = &dag;
        input.max_sample_count_ = sample_budget;
        ASSERT_EQ(OB_SUCCESS, input.prepare_source_(ctx));
        compaction::ObIVFMergeCaptureCtx capture;
        ASSERT_EQ(OB_SUCCESS, capture.init(ctx, input, parallel));
        if (parallel) { ASSERT_GT(capture.get_task_count(), 1); }
        else { ASSERT_EQ(1, capture.get_task_count()); }
        ASSERT_EQ(OB_STATE_NOT_MATCH, capture.finish());
        for (int64_t i = capture.get_task_count() - 1; i >= 0; --i) {
          ASSERT_EQ(OB_SUCCESS, capture.scan(i));
        }
        ASSERT_EQ(OB_SUCCESS, capture.finish());
        ASSERT_EQ(live_vids.size(), input.get_vector_count());
        if (live_vids.empty()) {
          ASSERT_TRUE(input.is_empty());
          ASSERT_EQ(0, input.get_sample_count());
          continue;
        }
        ObArray<storage::ObIVFVectorVID> vids;
        ObArray<float> values;
        ASSERT_EQ(OB_SUCCESS, input.get_all_vids(vids));
        ASSERT_EQ(live_vids.size(), vids.count());
        ASSERT_EQ(OB_SUCCESS, input.batch_get_by_vid(vids, 0, vids.count(), values));
        ASSERT_EQ(vids.count() * 64, values.count());
        for (int64_t i = 0; i < vids.count(); ++i) {
          EXPECT_EQ(live_vids[i], vids.at(i));
          EXPECT_FLOAT_EQ(static_cast<float>(live_sources[i]), values.at(i * 64 + 1));
          EXPECT_FLOAT_EQ(static_cast<float>(live_vids[i]), values.at(i * 64 + 2));
        }
        ASSERT_EQ(OB_SUCCESS, input.batch_get_samples(0, input.get_sample_count(), vids, values));
        ASSERT_EQ(MIN(sample_budget, static_cast<int64_t>(live_vids.size())), vids.count());
        ASSERT_EQ(vids.count() * 64, values.count());
        for (int64_t i = 0; i < vids.count(); ++i) {
          const auto found = std::find(live_vids.begin(), live_vids.end(), vids.at(i));
          ASSERT_NE(live_vids.end(), found);
          const int64_t idx = found - live_vids.begin();
          EXPECT_FLOAT_EQ(static_cast<float>(live_sources[idx]), values.at(i * 64 + 1));
          EXPECT_FLOAT_EQ(static_cast<float>(vids.at(i)), values.at(i * 64 + 2));
          if (!parallel) { serial_samples.push_back(vids.at(i)); }
          else { EXPECT_EQ(serial_samples[i], vids.at(i)); }
        }
      }
    }
  }
}

TEST_F(TestIvfSSTableScanIter, merge_capture_large_ordered_and_interleaved_inputs)
{
  const int64_t count = 100000;
  auto destroy_table = [](ObIvfSSTable *table) {
    if (nullptr != table) { table->~ObIvfSSTable(); }
  };
  for (const bool interleaved : {false, true}) {
    SCOPED_TRACE(interleaved);
    std::vector<uint64_t> first_vids(count / 2);
    std::vector<uint64_t> second_vids(count / 2);
    for (int64_t i = 0; i < count / 2; ++i) {
      first_vids[i] = interleaved ? 2 * i : i;
      second_vids[i] = interleaved ? 2 * i + 1 : count / 2 + i;
    }
    ObIvfSSTable *copied = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_ordered_raw_input(first_vids.data(), count / 2, 0, false));
    ASSERT_EQ(OB_SUCCESS, clone_as_inc_ivf_sstable(
        IVF_SNAPSHOT_VERSION, 2 * IVF_SNAPSHOT_VERSION, copied));
    std::unique_ptr<ObIvfSSTable, decltype(destroy_table)> first(copied, destroy_table);
    bool retained = false;
    ASSERT_EQ(OB_SUCCESS, first->inc_macro_ref(retained));
    ASSERT_TRUE(retained);
    first->is_tmp_sstable_ = true;
    ASSERT_EQ(OB_SUCCESS, build_ordered_raw_input(second_vids.data(), count / 2, 1, false));
    ASSERT_EQ(OB_SUCCESS, clone_as_inc_ivf_sstable(
        2 * IVF_SNAPSHOT_VERSION, 3 * IVF_SNAPSHOT_VERSION, copied));
    std::unique_ptr<ObIvfSSTable, decltype(destroy_table)> second(copied, destroy_table);
    for (int round = 0; round < 3; ++round) {
      compaction::ObTabletMergeDagParam param(INC_MAJOR_MERGE,
          ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
      compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
      compaction::ObIVFMergeInputAdapter input(allocator_);
      ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, input, false, false));
      ctx.static_param_.tables_handle_.reset();
      for (ObIvfSSTable *table : {first.get(), second.get()}) {
        ObTableHandleV2 handle;
        ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(table));
        ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(handle));
      }
      ctx.deletion_bitmap_handle_.reset();
      // Exercise sample relocation for every VID, including the batch boundary.
      input.max_sample_count_ = count;
      ASSERT_EQ(OB_SUCCESS, input.prepare_source_(ctx));
      compaction::ObIVFMergeCaptureCtx capture;
      ASSERT_EQ(OB_SUCCESS, capture.init(ctx, input, false));
      const auto start = std::chrono::steady_clock::now();
      ASSERT_EQ(OB_SUCCESS, capture.scan(0));
      const auto scanned = std::chrono::steady_clock::now();
      ASSERT_EQ(OB_SUCCESS, capture.finish());
      const auto sealed = std::chrono::steady_clock::now();
      printf("IVF_CAPTURE_TIMING interleaved=%d rows=%ld samples=%ld round=%d scan_ms=%.3f seal_ms=%.3f\n",
          interleaved, count, input.get_sample_count(), round,
          std::chrono::duration<double, std::milli>(scanned - start).count(),
          std::chrono::duration<double, std::milli>(sealed - scanned).count());
      ASSERT_EQ(count, input.get_vector_count());
      ASSERT_EQ(count, input.get_sample_count());
      ObArray<storage::ObIVFVectorVID> vids;
      ObArray<float> values;
      ASSERT_EQ(OB_SUCCESS, input.get_all_vids(vids));
      ASSERT_EQ(count, vids.count());
      for (int64_t i = 0; i < count; ++i) { ASSERT_EQ(i, vids.at(i)); }
      for (const int64_t begin : {0L, count / 2 - 4, count - 8}) {
        ASSERT_EQ(OB_SUCCESS, input.batch_get_samples(begin, begin + 8, vids, values));
        ASSERT_EQ(8, vids.count());
        ASSERT_EQ(8 * 64, values.count());
        for (int64_t i = 0; i < 8; ++i) {
          const int64_t vid = begin + i;
          EXPECT_EQ(vid, vids.at(i));
          EXPECT_FLOAT_EQ(static_cast<float>(interleaved ? vid % 2 : vid >= count / 2), values.at(i * 64 + 1));
          EXPECT_FLOAT_EQ(static_cast<float>(vid), values.at(i * 64 + 2));
        }
      }
    }
  }
}

TEST_F(TestIvfSSTableScanIter, merge_replay_spills_reads_concurrently_and_releases_files)
{
  const int64_t count = 600;
  std::vector<uint64_t> captured(count);
  for (int64_t i = 0; i < count; ++i) { captured[i] = 3 * i; }
  ASSERT_EQ(OB_SUCCESS, build_ordered_raw_input(captured.data(), count, 11, false));
  compaction::ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(TEST_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  compaction::ObAsyncIndexMergeCtx ctx(param, allocator_);
  auto *budget = MTL(share::ObIVFReplayMemoryMgr *);
  ASSERT_NE(nullptr, budget);
  const int64_t active_before = budget->get_active_build_count();
  int64_t fd = -1;
  {
    compaction::ObIVFMergeInputAdapter input(allocator_);
    ASSERT_NO_FATAL_FAILURE(prepare_merge_input(ctx, input, false, false));
    ASSERT_EQ(OB_SUCCESS, input.init_source_(ctx));
    ASSERT_EQ(active_before + 1, budget->get_active_build_count());
    ASSERT_EQ(1, input.replay_inputs_.count());
    auto *replay = input.replay_inputs_.at(0);
    ASSERT_TRUE(replay->is_sealed());
    ASSERT_GT(replay->get_file_size(), 0);
    ASSERT_EQ(0, replay->get_inmemory_row_count());
    fd = replay->store_.fd_;
    ASSERT_GE(fd, 0);
    input.source_tables_.reset();
    input.read_info_ = nullptr;
    input.ls_id_.reset();
    ASSERT_TRUE(input.supports_source_order());
    ASSERT_FALSE(input.is_fully_memory_resident());
    ObArray<int64_t> source_indices;
    for (int64_t i = 0; i < count; ++i) {
      int64_t source = -1;
      ASSERT_EQ(OB_SUCCESS, input.get_source_row_idx(i, source));
      ASSERT_EQ(i, source);
      ASSERT_EQ(OB_SUCCESS, source_indices.push_back(i));
    }
    ObArray<float> source_values;
    ASSERT_EQ(OB_SUCCESS, input.batch_get_by_source(source_indices, source_values));
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_FLOAT_EQ(captured[i], source_values.at(i * 64 + 2));
    }
    std::swap(source_indices.at(0), source_indices.at(1));
    EXPECT_EQ(OB_INVALID_ARGUMENT, input.batch_get_by_source(source_indices, source_values));
    EXPECT_TRUE(source_values.empty());
    ObArray<storage::ObIVFVectorVID> requested;
    // Out-of-order requests, duplicate VIDs, VID zero and several 256-row read batches.
    for (int64_t i = 0; i < count + 1; ++i) {
      ASSERT_EQ(OB_SUCCESS, requested.push_back(captured[(count - i % count) % count]));
    }
    share::ObTenantBase *tenant = share::ObTenantEnv::get_tenant();
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    for (int64_t worker = 0; worker < 4; ++worker) {
      workers.emplace_back([&] {
        share::ObTenantEnv::set_tenant(tenant);
        ObArray<float> values;
        for (int round = 0; round < 3; ++round) {
          if (OB_SUCCESS != input.batch_get_by_vid(requested, 0, requested.count(), values)
              || values.count() != requested.count() * 64) {
            ++failures;
          } else {
            for (int64_t i = 0; i < requested.count(); ++i) {
              if (values.at(i * 64) != 1.0F || values.at(i * 64 + 1) != 11.0F
                  || values.at(i * 64 + 2) != static_cast<float>(requested.at(i))) {
                ++failures;
                break;
              }
            }
          }
        }
        share::ObTenantEnv::set_tenant(nullptr);
      });
    }
    for (auto &worker : workers) { worker.join(); }
    EXPECT_EQ(0, failures.load());
    // A corrupt locator must fail before touching a different replay row.
    input.locators_.at(0).row_id_ = count;
    ObArray<float> values;
    EXPECT_EQ(OB_INVALID_DATA, input.batch_get_by_vid(requested, 0, 1, values));
    EXPECT_TRUE(values.empty());
  }
  EXPECT_EQ(active_before, budget->get_active_build_count());
  int64_t file_size = 0;
  EXPECT_NE(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_size(MTL_ID(), fd, file_size));
}

TEST_F(TestIvfSSTableScanIter, switches_sstable_then_runs_knn_and_brute_force)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  query[0] = 1.0F;
  ObIvfSSTable *inc_sstable = nullptr;

  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, clone_as_inc_ivf_sstable(
      IVF_SNAPSHOT_VERSION, 2 * IVF_SNAPSHOT_VERSION, inc_sstable));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable(inc_sstable));

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_TRUE(scan_param_.is_valid());
  ASSERT_TRUE(iter.get_ctx_guard().get_store_ctx().is_valid());
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ASSERT_TRUE(iter.is_inited());

  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_TRUE(version_range.is_valid());
  ObIvfKnnSearchResult knn_result;
  ObIvfKnnSearchResult brute_force_result;
  ASSERT_EQ(OB_NOT_INIT, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchParam query_param;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.set_query(query_param));
  fill_search_params(query_param, query, dim, 2);
  query_param.iterative_centroids_ = true;
  ObRoaringBitmap allowed_vids(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(20));
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  query_param.pre_filter_ = &pre_filter;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ASSERT_NE(query, iter.query_param_.query_);
  ASSERT_FLOAT_EQ(1.0F, iter.query_param_.query_[0]);
  query[0] = -1.0F;
  ASSERT_FLOAT_EQ(1.0F, iter.query_param_.query_[0]);
  ASSERT_EQ(OB_INIT_TWICE, iter.set_query(query_param));

  ObNewVersionRange invalid_range;
  ASSERT_FALSE(invalid_range.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.switch_sstable(invalid_range, scan_param_.snapshot_.version()));

  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.knn_search(0, knn_result));
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, knn_result));
  ASSERT_EQ(0, knn_result.count());
  ASSERT_TRUE(iter.has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, knn_result));
  ASSERT_EQ(1, knn_result.count());
  ASSERT_EQ(2, knn_result.at(0).cid_);
  ASSERT_EQ(20, knn_result.at(0).vid_);
  ASSERT_FLOAT_EQ(4.0F, knn_result.at(0).distance_);
  ASSERT_FALSE(iter.has_remaining_centroids());

  // An exhausted KNN iterator does not rescan either posting.
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, knn_result));
  ASSERT_EQ(0, knn_result.count());
  ASSERT_EQ(OB_SUCCESS, iter.brute_force_search(brute_force_result));
  ASSERT_EQ(1, brute_force_result.count());
  ASSERT_EQ(IVF_RAW_VECTOR_CID, brute_force_result.at(0).cid_);
  ASSERT_EQ(20, brute_force_result.at(0).vid_);

  ObNewVersionRange inc_version_range;
  inc_version_range.base_version_ = IVF_SNAPSHOT_VERSION;
  inc_version_range.snapshot_version_ = 2 * IVF_SNAPSHOT_VERSION;
  ASSERT_TRUE(inc_version_range.is_valid());
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(inc_version_range, scan_param_.snapshot_.version()));
  ASSERT_NE(nullptr, iter.searcher_);
  ASSERT_NE(nullptr, iter.searcher_->sstable());
  ASSERT_TRUE(iter.searcher_->sstable()->is_inc_ivf_sstable());
  ObNewVersionRange actual_inc_version_range;
  ASSERT_EQ(OB_SUCCESS, iter.searcher_->sstable()->get_async_index_version_range(
      actual_inc_version_range));
  ASSERT_EQ(inc_version_range, actual_inc_version_range);
  ASSERT_NE(inc_version_range,
      iter.searcher_->sstable()->get_key().version_range_);
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, knn_result));
  ASSERT_EQ(1, knn_result.count());
  ASSERT_EQ(20, knn_result.at(0).vid_);
  ASSERT_FALSE(iter.has_remaining_centroids());

  iter.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));
  ASSERT_FALSE(iter.searcher_->sstable()->is_inc_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, knn_result));
  ASSERT_EQ(1, knn_result.count());
  ASSERT_EQ(20, knn_result.at(0).vid_);

  ObNewVersionRange missing_range;
  missing_range.base_version_ = 0;
  missing_range.snapshot_version_ = IVF_SNAPSHOT_VERSION + 1;
  ASSERT_TRUE(missing_range.is_valid());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, iter.switch_sstable(missing_range, scan_param_.snapshot_.version()));
  ASSERT_EQ(OB_NOT_INIT, iter.knn_search(1, knn_result));
  ASSERT_EQ(OB_NOT_INIT, iter.brute_force_search(brute_force_result));

  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(inc_version_range, scan_param_.snapshot_.version()));
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, knn_result));
  ASSERT_EQ(1, knn_result.count());
  ASSERT_EQ(20, knn_result.at(0).vid_);
  ASSERT_EQ(OB_INIT_TWICE, iter.init(scan_param_, tablet_handle_));
  iter.reset();
  ASSERT_FALSE(iter.is_inited());
  ASSERT_EQ(OB_NOT_INIT, iter.brute_force_search(brute_force_result));
}

TEST_F(TestIvfSSTableScanIter, rejects_invalid_init_argument)
{
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable());
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ObTabletHandle invalid_tablet_handle;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(scan_param_, invalid_tablet_handle));
  ASSERT_FALSE(iter.is_inited());
  ASSERT_EQ(nullptr, iter.scan_param_);
}

TEST_F(TestIvfSSTableScanIter, pinned_tablet_survives_borrower_and_range_failure)
{
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable());
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ObTabletHandle borrowed;
  ASSERT_EQ(OB_SUCCESS, borrowed.assign(tablet_handle_));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, borrowed));
  borrowed.reset();
  ASSERT_TRUE(iter.tablet_handle_.is_valid());
  ASSERT_EQ(tablet_handle_.get_obj(), iter.tablet_handle_.get_obj());
  float query[64] = {0};
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, 64, 2);
  ASSERT_EQ(OB_SUCCESS, iter.set_query(param));
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = IVF_SNAPSHOT_VERSION + 1;
  ASSERT_NE(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
  range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, result));
  EXPECT_EQ(2, result.count());
  iter.reset();
  EXPECT_FALSE(iter.tablet_handle_.is_valid());
  EXPECT_FALSE(iter.is_inited());
  EXPECT_TRUE(tablet_handle_.is_valid());
}

TEST_F(TestIvfSSTableScanIter, pinned_access_rejects_mismatch_and_keeps_normal_read_checks)
{
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable());
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  ObIvfSSTableScanIter preparation(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(preparation));
  ObAccessService *access = MTL(ObAccessService *);
  ASSERT_NE(nullptr, access);
  class TrackingAllocator final : public ObIAllocator
  {
  public:
    explicit TrackingAllocator(ObIAllocator &parent) : parent_(parent), allocations_(0), frees_(0) {}
    void *alloc(const int64_t size) override
    { ++allocations_; return parent_.alloc(size); }
    void *alloc(const int64_t size, const ObMemAttr &attr) override
    { ++allocations_; return parent_.alloc(size, attr); }
    void free(void *ptr) override
    { ++frees_; parent_.free(ptr); }
    ObIAllocator &parent_;
    int64_t allocations_;
    int64_t frees_;
  } tracking(allocator_);
  scan_param_.allocator_ = &tracking;
  ObIvfSSTableScanIter *output = nullptr;
  ObTabletHandle invalid;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      access->async_index_ivf_scan(ObLSID(TEST_LS_ID), &invalid, scan_param_, output));
  EXPECT_EQ(nullptr, output);
  scan_param_.tablet_id_ = ObTabletID(IVF_TABLET_ID + 1);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      access->async_index_ivf_scan(ObLSID(TEST_LS_ID), &tablet_handle_, scan_param_, output));
  EXPECT_EQ(nullptr, output);
  EXPECT_EQ(0, tracking.allocations_);
  scan_param_.tablet_id_ = ObTabletID(IVF_TABLET_ID);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      access->async_index_ivf_scan(ObLSID(), &tablet_handle_, scan_param_, output));
  EXPECT_EQ(nullptr, output);
  scan_param_.allocator_ = nullptr;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      access->async_index_ivf_scan(ObLSID(TEST_LS_ID), &tablet_handle_, scan_param_, output));
  EXPECT_EQ(nullptr, output);
  scan_param_.allocator_ = &tracking;
  EXPECT_EQ(0, tracking.allocations_);
  // A pinned Tablet must not bypass LS/read admission. This LS was not created.
  scan_param_.ls_id_ = ObLSID(TEST_LS_ID + 1);
  const int null_ret = access->async_index_ivf_scan(scan_param_.ls_id_, nullptr, scan_param_, output);
  ASSERT_NE(OB_SUCCESS, null_ret);
  ASSERT_EQ(nullptr, output);
  const int pinned_ret = access->async_index_ivf_scan(scan_param_.ls_id_, &tablet_handle_, scan_param_, output);
  EXPECT_EQ(null_ret, pinned_ret);
  EXPECT_EQ(nullptr, output);
  EXPECT_EQ(2, tracking.allocations_);
  EXPECT_EQ(tracking.allocations_, tracking.frees_);
  EXPECT_TRUE(tablet_handle_.is_valid());
  class FailAllocator final : public ObIAllocator
  {
  public:
    FailAllocator() : frees_(0) {}
    void *alloc(const int64_t) override { return nullptr; }
    void *alloc(const int64_t, const ObMemAttr &) override { return nullptr; }
    void free(void *) override { ++frees_; }
    int64_t frees_;
  } failure;
  scan_param_.allocator_ = &failure;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
      access->async_index_ivf_scan(scan_param_.ls_id_, &tablet_handle_, scan_param_, output));
  EXPECT_EQ(nullptr, output);
  EXPECT_EQ(0, failure.frees_);
  EXPECT_TRUE(tablet_handle_.is_valid());
  scan_param_.allocator_ = &allocator_;
}

TEST_F(TestIvfSSTableScanIter, candidate_batch_invalid_state_and_nprobe_clear_output)
{
  ObIvfSSTableScanIter iter(allocator_);
  ObIvfKnnSearchResult result;
  ObIvfCandidateProgress progress;
  progress.centroids_started_ = 1;
  EXPECT_EQ(OB_NOT_INIT, iter.next_candidate_batch(1,
      std::numeric_limits<float>::infinity(), result, progress));
  EXPECT_EQ(0, result.count());
  EXPECT_EQ(0, progress.centroids_started_);

  const int32_t dim = 64;
  float query[dim] = {1.0F};
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 1);
  ASSERT_EQ(OB_SUCCESS, iter.set_query(param));
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
  for (const int64_t nprobe : {0, -1}) {
    progress.postings_scanned_ = 1;
    EXPECT_EQ(OB_INVALID_ARGUMENT, iter.next_candidate_batch(nprobe,
        std::numeric_limits<float>::infinity(), result, progress));
    EXPECT_EQ(0, result.count());
    EXPECT_EQ(0, progress.postings_scanned_);
  }
  EXPECT_EQ(OB_SUCCESS, iter.next_candidate_batch(1,
      std::numeric_limits<float>::infinity(), result, progress));
  EXPECT_GT(result.count(), 0);
}

TEST_F(TestIvfSSTableScanIter, zero_nprobe_runs_automatic_knn_and_candidate_batches)
{
  const int32_t dim = 64;
  float query[dim] = {1.0F};
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  sstable_.ivf_meta_.vector_count_ = 2;
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfKnnSearchParam param;
  fill_search_params(param, query, dim, 2);
  param.nprobe_ = 0;
  ASSERT_FALSE(param.is_valid());
  param.original_k_ = 2;
  param.num_candidates_ = 8;
  ASSERT_TRUE(param.is_valid());
  ASSERT_EQ(OB_SUCCESS, iter.set_query(param));
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ObIvfKnnSearchResult result;
  for (const bool post : {false, true}) {
    iter.reuse();
    ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
    int64_t total = 0;
    int64_t calls = 0;
    bool seen_10 = false;
    bool seen_20 = false;
    do {
      if (post) {
        ObIvfCandidateProgress progress;
        ASSERT_EQ(OB_SUCCESS, iter.next_candidate_batch(0,
            std::numeric_limits<float>::infinity(), result, progress));
      } else {
        ASSERT_EQ(OB_SUCCESS, iter.knn_search(0, result));
      }
      for (int64_t i = 0; i < result.count(); ++i) {
        if (10 == result.at(i).vid_) { ASSERT_FALSE(seen_10); seen_10 = true; }
        else { ASSERT_EQ(20, result.at(i).vid_); ASSERT_FALSE(seen_20); seen_20 = true; }
      }
      total += result.count();
      ASSERT_LT(++calls, 16);
    } while (iter.has_remaining_centroids());
    ASSERT_EQ(2, total);
    ASSERT_TRUE(seen_10 && seen_20);
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.knn_search(-1, result));
  }
  // The SQL segment wrapper must preserve the same zero-valued auto request.
  param.expected_dist_type_ = share::VIDA_L2;
  sql::ObDASAsyncIVFSegmentScanOp segment(allocator_);
  ASSERT_EQ(OB_SUCCESS, segment.open(ObLSID(TEST_LS_ID), scan_param_, tablet_handle_,
      range, scan_param_.snapshot_.version(), param, 0, 0, false, nullptr));
  ObSEArray<sql::ObDASVectorCandidate, 4> candidates;
  ASSERT_EQ(OB_INVALID_ARGUMENT, segment.search_round(-1, candidates));
  int64_t rounds = 0;
  while (segment.has_more()) {
    ASSERT_EQ(OB_SUCCESS, segment.search_round(0, candidates));
    ASSERT_LT(++rounds, 16);
  }
  ASSERT_EQ(2, candidates.count());
}

TEST_F(TestIvfSSTableScanIter, timeout_does_not_poison_following_search)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  query[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfKnnSearchParam query_param;
  ObIvfKnnSearchResult result;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));

  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));
  scan_param_.timeout_ = ObClockGenerator::getClock() - 1;
  ASSERT_EQ(OB_TIMEOUT, iter.knn_search(1, result));

  scan_param_.timeout_ = ObClockGenerator::getClock() + 60_s;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
}

TEST_F(TestIvfSSTableScanIter, reuse_restarts_current_sstable_knn_cursor)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, build_five_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());

  iter.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
}

TEST_F(TestIvfSSTableScanIter, async_ivf_soar_k2_refills_third_centroid)
{
  const int32_t dim = 64;
  const int64_t cids[] = {0, 1, 2};
  const int64_t vids[] = {7, 7, 9};
  float vectors[3 * dim] = {0};
  float centroids[3 * dim] = {0};
  float query[dim] = {0};
  for (int i = 0; i < 3; ++i) {
    vectors[i * dim] = 1.0F;
    centroids[i * dim + 1] = static_cast<float>(i);
  }
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(vectors, cids, vids, 3,
      centroids, 3, dim, nullptr, 1, nullptr, share::VIDA_L2, true,
      nullptr, nullptr, 0, true /* allow_shared_raw_vectors */));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_FALSE(sstable_.get_ivf_meta().is_hierarchical());
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  sql::ObDASVecAuxScanCtDef ctdef(allocator_);
  ctdef.algorithm_type_ = share::VIAT_DISK_BBQ;
  sql::ObDASIVFAsyncIter das_iter;
  das_iter.vec_aux_ctdef_ = &ctdef;
  das_iter.vec_index_type_ = share::ObVecIndexType::VEC_INDEX_POST_WITHOUT_FILTER;
  das_iter.real_search_vec_.assign_ptr(reinterpret_cast<char *>(query), sizeof(query));
  das_iter.dim_ = dim;
  das_iter.nprobes_ = 1;
  das_iter.limit_param_.limit_ = 2;
  das_iter.limit_param_.offset_ = 0;
  das_iter.ls_id_ = ObLSID(TEST_LS_ID);
  das_iter.async_ivf_index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
  auto &scan_param = das_iter.async_ivf_scan_param_;
  scan_param.tenant_id_ = OB_SERVER_TENANT_ID;
  scan_param.ls_id_ = das_iter.ls_id_;
  scan_param.tablet_id_ = das_iter.async_ivf_index_tablet_id_;
  scan_param.index_id_ = IVF_TABLE_ID;
  scan_param.timeout_ = ObClockGenerator::getClock() + 60_s;
  scan_param.reserved_cell_count_ = table_schema_.get_column_count();
  scan_param.schema_version_ = 10;
  scan_param.tenant_schema_version_ = 10;
  scan_param.allocator_ = &allocator_;
  scan_param.scan_allocator_ = &allocator_;
  scan_param.table_param_ = &table_param_;
  scan_param.pd_storage_flag_ = scan_param_.pd_storage_flag_;
  SCN sql_scn;
  ASSERT_EQ(OB_SUCCESS, sql_scn.convert_for_tx(SQL_READ_SNAPSHOT_VERSION));
  scan_param.snapshot_.init_special_read(sql_scn);
  das_iter.sql_snapshot_ = &scan_param.snapshot_;
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ObSEArray<ObNewVersionRange, 1> ranges;
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(range));
  ASSERT_EQ(OB_SUCCESS, das_iter.segment_group_ctx_.add_index_tablet_version_ranges(
      tablet_handle_, ranges));
  ASSERT_EQ(OB_SUCCESS, das_iter.segment_group_ctx_.calc_query_version_ranges(sql_scn));

  for (int mode : {0, 1, 2, 3}) { // unfiltered, PRE, all filtered, VID7 deleted
    SCOPED_TRACE(mode);
    const bool use_das = mode == 0 || mode == 3;
    ObIvfSearcherGuard searcher;
    ObRoaringBitmap allowed(&allocator_);
    ObIvfPreFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(allowed));
    if (mode == 1) {
      ASSERT_EQ(OB_SUCCESS, allowed.value_add(7));
      ASSERT_EQ(OB_SUCCESS, allowed.value_add(9));
    }
    if (mode == 3) {
      ObRoaringBitmap *deleted = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
      ASSERT_NE(nullptr, deleted);
      ASSERT_EQ(OB_SUCCESS, deleted->value_add(7));
      ASSERT_EQ(OB_SUCCESS, das_iter.deletion_bitmap_handle_.init_from_bitmap(allocator_, deleted));
    }
    ObSEArray<ObIvfSSTableScanIter *, 8> scan_iters;
    DEFER(das_iter.release_async_ivf_segment_scan_iters(scan_iters));
    ObIvfKnnSearchParam param;
    if (use_das) {
      // Exercise real DAS parameter binding for both SOAR duplicates and deleted VIDs.
      ASSERT_FALSE(das_iter.is_iter_filter());
      ASSERT_EQ(OB_SUCCESS, das_iter.prepare_async_ivf_scan(2, nullptr, scan_iters));
      ASSERT_EQ(1, scan_iters.count());
    } else {
      // Retain the storage-level PRE continuation and empty-whitelist coverage.
      fill_search_params(param, query, dim, 2);
      param.iterative_centroids_ = true;
      param.nprobe_ = 1;
      param.pre_filter_ = &filter;
      ObTableHandleV2 handle;
      ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&sstable_));
      ASSERT_EQ(OB_SUCCESS, sstable_.create_searcher(
          handle, scan_param_, scan_context_, searcher.get()));
    }
    sql::ObDASAsyncIVFCandidateSet seen(allocator_);
    ObSEArray<sql::ObDASVectorCandidate, 4> round;
    ObIvfKnnSearchResult result;
    int64_t accepted = 0;
    int64_t rounds = 0;
    bool has_active_segment = false;
    do {
      if (use_das) {
        ASSERT_EQ(OB_SUCCESS, das_iter.scan_async_ivf_segments(
            rounds == 0, 1, scan_iters, result, has_active_segment));
        ASSERT_EQ(mode == 3 && rounds < 2 ? 0 : 1, result.count());
        if (result.count() > 0) { EXPECT_EQ(rounds < 2 ? 7 : 9, result.at(0).vid_); }
      } else {
        ASSERT_EQ(OB_SUCCESS, searcher->knn_search(param, result));
        has_active_segment = searcher->has_remaining_centroids();
      }
      round.reuse();
      for (int64_t i = 0; i < result.count(); ++i) {
        sql::ObDASVectorCandidate c;
        c.entry_ = result.at(i); c.group_slot_ = 0; c.segment_slot_ = 0;
        ASSERT_EQ(OB_SUCCESS, round.push_back(c));
      }
      ASSERT_EQ(OB_SUCCESS, seen.retain_new(round));
      accepted += round.count();
      ++rounds;
    } while (accepted < 2 && has_active_segment);
    // A complete empty PRE set ends immediately without opening a CID cursor.
    EXPECT_EQ(mode == 2 ? 1 : 3, rounds);
    EXPECT_EQ(mode == 2 ? 0 : mode == 3 ? 1 : 2, accepted);
  }
}

TEST_F(TestIvfSSTableScanIter,
    operation_specific_validation_keeps_knn_cursor)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  ASSERT_EQ(nullptr, query_param.pre_filter_);
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());

  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.brute_force_search(result));
  ASSERT_EQ(0, result.count());
  ASSERT_TRUE(iter.has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  ASSERT_FALSE(iter.has_remaining_centroids());
}

TEST_F(TestIvfSSTableScanIter,
    brute_force_timeout_clears_result_and_keeps_knn_cursor)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObRoaringBitmap allowed_vids(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(10));
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(20));
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  query_param.pre_filter_ = &pre_filter;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());

  scan_param_.timeout_ = ObClockGenerator::getClock() - 1;
  ASSERT_EQ(OB_TIMEOUT, iter.brute_force_search(result));
  ASSERT_EQ(0, result.count());
  ASSERT_TRUE(iter.has_remaining_centroids());

  scan_param_.timeout_ = ObClockGenerator::getClock() + 60_s;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  ASSERT_FALSE(iter.has_remaining_centroids());
}

void TestIvfSSTableScanIter::run_iterative_metric_filter(
    const share::ObVectorIndexDistAlgorithm dist_type)
{
  const int32_t dim = 64;
  const int64_t cids[] = {0, 1, 2};
  const int64_t vids[] = {10, 20, 30};
  float vectors[ARRAYSIZEOF(cids)][dim] = {{0}};
  float centroids[ARRAYSIZEOF(cids)][dim] = {{0}};
  float query[dim] = {0};
  query[0] = 1.0F;
  vectors[0][0] = centroids[0][0] = 1.0F;
  vectors[1][0] = centroids[1][0] = 0.8F;
  vectors[1][1] = centroids[1][1] = 0.6F;
  vectors[2][1] = centroids[2][1] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      &vectors[0][0], cids, vids, ARRAYSIZEOF(cids), &centroids[0][0],
      ARRAYSIZEOF(cids), dim, nullptr, 1, nullptr, dist_type));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObRoaringBitmap allowed_vids(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(20));
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(30));
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  query_param.pre_filter_ = &pre_filter;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(0, result.count());
  ASSERT_TRUE(iter.has_remaining_centroids());
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(30, result.at(0).vid_);
  ASSERT_FALSE(iter.has_remaining_centroids());
}

TEST_F(TestIvfSSTableScanIter, iterative_filter_works_for_ip)
{
  run_iterative_metric_filter(share::VIDA_IP);
}

TEST_F(TestIvfSSTableScanIter, iterative_filter_works_for_cosine)
{
  run_iterative_metric_filter(share::VIDA_COS);
}

TEST_F(TestIvfSSTableScanIter,
    iterative_knn_cursor_survives_brute_force)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, build_five_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObRoaringBitmap allowed_vids(&allocator_);
  for (int64_t vid = 10; vid <= 50; vid += 10) {
    ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(vid));
  }
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  query_param.pre_filter_ = &pre_filter;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, iter.brute_force_search(result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
}

TEST_F(TestIvfSSTableScanIter,
    iterative_switch_sstable_resets_partial_knn_cursor)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ObIvfSSTable *inc_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_five_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, clone_as_inc_ivf_sstable(
      IVF_SNAPSHOT_VERSION, 2 * IVF_SNAPSHOT_VERSION, inc_sstable));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable(inc_sstable));

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange base_range;
  base_range.base_version_ = 0;
  base_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(base_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());

  ObNewVersionRange inc_range;
  inc_range.base_version_ = IVF_SNAPSHOT_VERSION;
  inc_range.snapshot_version_ = 2 * IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(inc_range, scan_param_.snapshot_.version()));
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
}

TEST_F(TestIvfSSTableScanIter,
    iterative_sparse_filter_spans_posting_batches)
{
  const int32_t dim = 64;
  const int64_t first_posting_count = ObDistBBQFastScanBatch::CAPACITY + 1;
  const int64_t vector_count = first_posting_count + 1;
  std::vector<int64_t> cids(vector_count, 0);
  std::vector<int64_t> vids(vector_count, 0);
  std::vector<float> vectors(vector_count * dim, 0);
  float centroids[2][dim] = {{0}};
  float query[dim] = {0};
  for (int64_t i = 0; i < first_posting_count; ++i) {
    vids[i] = 100 + i;
    vectors[i * dim] = static_cast<float>(i + 1);
  }
  cids[vector_count - 1] = 1;
  vids[vector_count - 1] = 200;
  vectors[(vector_count - 1) * dim] = 1000.0F;
  centroids[1][0] = 1000.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(
      vectors.data(), cids.data(), vids.data(), vector_count,
      &centroids[0][0], ARRAYSIZEOF(centroids), dim, nullptr));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObRoaringBitmap allowed_vids(&allocator_);
  const int64_t expected_vids[] = {100, 131, 132, 200};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_vids); ++i) {
    ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(expected_vids[i]));
  }
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, ARRAYSIZEOF(expected_vids));
  query_param.iterative_centroids_ = true;
  query_param.pre_filter_ = &pre_filter;
  query_param.read_stats_ = &stats;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(3, result.count());
  ASSERT_EQ(100, result.at(0).vid_);
  ASSERT_EQ(131, result.at(1).vid_);
  ASSERT_EQ(132, result.at(2).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
  ASSERT_EQ(1, stats.low_precision_vector_scan_count_);
  // PRE filtering skips distance evaluation for the other posting rows.
  ASSERT_EQ(3, stats.low_precision_vector_count_);
  ASSERT_EQ(3, stats.raw_vector_refine_count_);

  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(200, result.at(0).vid_);
  ASSERT_FALSE(iter.has_remaining_centroids());
  ASSERT_EQ(2, stats.low_precision_vector_scan_count_);
  ASSERT_EQ(ARRAYSIZEOF(expected_vids), stats.low_precision_vector_count_);
  ASSERT_EQ(ARRAYSIZEOF(expected_vids), stats.raw_vector_refine_count_);
}

TEST_F(TestIvfSSTableScanIter,
    iterative_timeout_between_rounds_preserves_knn_cursor)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, build_five_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  query_param.read_stats_ = &stats;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(10, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
  ASSERT_EQ(1, stats.low_precision_vector_scan_count_);

  scan_param_.timeout_ = ObClockGenerator::getClock() - 1;
  ASSERT_EQ(OB_TIMEOUT, iter.knn_search(1, result));
  ASSERT_EQ(0, result.count());
  ASSERT_TRUE(iter.has_remaining_centroids());
  ASSERT_EQ(1, stats.low_precision_vector_scan_count_);

  scan_param_.timeout_ = ObClockGenerator::getClock() + 60_s;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(1, result.count());
  ASSERT_EQ(20, result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
  ASSERT_EQ(2, stats.low_precision_vector_scan_count_);
}

TEST_F(TestIvfSSTableScanIter,
    iterative_filter_varies_nprobe_and_merges_round_results)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, build_five_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObRoaringBitmap allowed_vids(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(40));
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(50));
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.iterative_centroids_ = true;
  query_param.pre_filter_ = &pre_filter;
  query_param.read_stats_ = &stats;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult round_result;
  ObIvfKnnSearchResult merged_result;
  ObIvfTopKCollector merged_collector;
  ASSERT_EQ(OB_SUCCESS, merged_collector.init(query_param.k_, allocator_));
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, round_result));
  ASSERT_EQ(0, round_result.count());
  ASSERT_TRUE(iter.has_remaining_centroids());
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, round_result));
  ASSERT_EQ(0, round_result.count());
  ASSERT_TRUE(iter.has_remaining_centroids());

  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, round_result));
  ASSERT_EQ(1, round_result.count());
  ASSERT_EQ(40, round_result.at(0).vid_);
  ASSERT_TRUE(iter.has_remaining_centroids());
  ASSERT_EQ(OB_SUCCESS, merged_collector.push(
      round_result.at(0).cid_, round_result.at(0).vid_,
      round_result.at(0).distance_));

  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, round_result));
  ASSERT_EQ(1, round_result.count());
  ASSERT_EQ(50, round_result.at(0).vid_);
  ASSERT_FALSE(iter.has_remaining_centroids());
  ASSERT_EQ(OB_SUCCESS, merged_collector.push(
      round_result.at(0).cid_, round_result.at(0).vid_,
      round_result.at(0).distance_));
  ASSERT_EQ(OB_SUCCESS, merged_collector.finish(merged_result));
  ASSERT_EQ(2, merged_result.count());
  ASSERT_EQ(40, merged_result.at(0).vid_);
  ASSERT_EQ(50, merged_result.at(1).vid_);
  ASSERT_EQ(5, stats.low_precision_vector_scan_count_);
  ASSERT_EQ(2, stats.raw_vector_refine_count_);

  ASSERT_EQ(OB_SUCCESS, iter.knn_search(3, round_result));
  ASSERT_EQ(0, round_result.count());
  ASSERT_EQ(5, stats.low_precision_vector_scan_count_);
  ASSERT_EQ(2, stats.raw_vector_refine_count_);
}

TEST_F(TestIvfSSTableScanIter,
    iterative_filter_empty_pre_filter_exhausts_nonempty_sstable)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObRoaringBitmap allowed_vids(&allocator_);
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 1);
  query_param.iterative_centroids_ = true;
  query_param.pre_filter_ = &pre_filter;
  query_param.read_stats_ = &stats;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));

  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(0, result.count());
  // An empty allowed set exhausts the search without opening posting scans.
  ASSERT_FALSE(iter.has_remaining_centroids());
  ASSERT_EQ(0, stats.low_precision_vector_scan_count_);
  ASSERT_EQ(0, stats.low_precision_vector_count_);
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(10, result));
  ASSERT_EQ(0, result.count());
  ASSERT_FALSE(iter.has_remaining_centroids());
  ASSERT_EQ(0, stats.low_precision_vector_scan_count_);
  ASSERT_EQ(0, stats.low_precision_vector_count_);
  ASSERT_EQ(0, stats.raw_vector_read_count_);
  ASSERT_EQ(0, stats.raw_vector_refine_count_);

  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(0, result.count());
  ASSERT_FALSE(iter.has_remaining_centroids());
  ASSERT_EQ(0, stats.low_precision_vector_scan_count_);
  ASSERT_EQ(0, stats.low_precision_vector_count_);
  ASSERT_EQ(0, stats.raw_vector_read_count_);
}

TEST_F(TestIvfSSTableScanIter, profile_survives_rounds_switch_reuse_and_destruction)
{
  const int32_t dim = 64;
  float query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, build_five_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  common::ObOpProfile<common::ObMetric> scan_profile(
      common::ObProfileId::VECTOR_SEARCH_VEC_ASYNC_IVF_SCAN, &allocator_);
  common::ObOpProfile<common::ObMetric> *first = nullptr;
  common::ObOpProfile<common::ObMetric> *second = nullptr;
  ASSERT_EQ(OB_SUCCESS, scan_profile.register_child(common::ObProfileId::VECTOR_SEARCH_IVF_SEGMENT, first));
  ASSERT_EQ(OB_SUCCESS, scan_profile.register_child(common::ObProfileId::VECTOR_SEARCH_IVF_SEGMENT, second));
  ObIvfSSTableReadStats caller_stats;
  {
    ObIvfSSTableScanIter iter(allocator_);
    ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
    ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
    ObIvfKnnSearchParam query_param;
    fill_search_params(query_param, query, dim, 1);
    query_param.iterative_centroids_ = true;
    query_param.read_stats_ = &caller_stats;
    ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
    ObNewVersionRange range;
    range.base_version_ = 0;
    range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
    ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
    iter.set_profile(first, true);
    ObIvfKnnSearchResult result;
    ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
    ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
    expect_profile_metric(*first, common::ObMetricId::VEC_IVF_LOW_PRECISION_VECTOR_SCAN_COUNT, 2);
    expect_profile_metric(*first, common::ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID, 1);
    expect_profile_metric(*first, common::ObMetricId::VEC_IVF_SEGMENT_RETURN_ROWS_COUNT, 2);
    ASSERT_NE(nullptr, first->get_metric(common::ObMetricId::VEC_IVF_ITER_CALL_BACK_TIME));
    ASSERT_NE(nullptr, first->get_metric(common::ObMetricId::VEC_IVF_SEGMENT_TOTAL_TIME));
    EXPECT_LE(first->get_metric(common::ObMetricId::VEC_IVF_ITER_CALL_BACK_TIME)->value(),
              first->get_metric(common::ObMetricId::VEC_IVF_SEGMENT_TOTAL_TIME)->value());
    ASSERT_EQ(2, caller_stats.low_precision_vector_scan_count_);
    ASSERT_EQ(1, caller_stats.first_low_precision_vector_scan_cid_);
    scan_param_.timeout_ = ObClockGenerator::getClock() - 1;
    ASSERT_EQ(OB_TIMEOUT, iter.knn_search(1, result));
    expect_profile_metric(*first, common::ObMetricId::VEC_IVF_SEGMENT_RETURN_ROWS_COUNT, 2);
    ASSERT_EQ(2, caller_stats.low_precision_vector_scan_count_);
    scan_param_.timeout_ = ObClockGenerator::getClock() + 60_s;
    ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
    ASSERT_EQ(nullptr, iter.get_profile());
    iter.set_profile(second);
    ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
    expect_profile_metric(*second, common::ObMetricId::VEC_IVF_LOW_PRECISION_VECTOR_SCAN_COUNT, 1);
    ASSERT_EQ(3, caller_stats.low_precision_vector_scan_count_);
    iter.reuse();
    iter.reuse();
    ASSERT_EQ(nullptr, iter.get_profile());
  }
  expect_profile_metric(*first, common::ObMetricId::VEC_IVF_LOW_PRECISION_VECTOR_SCAN_COUNT, 2);
  expect_profile_metric(*second, common::ObMetricId::VEC_IVF_LOW_PRECISION_VECTOR_SCAN_COUNT, 1);
  const char *persist = nullptr;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, scan_profile.to_persist_profile(persist, size, &allocator_));
  common::ObOpProfile<common::ObMetric> *restored = nullptr;
  ASSERT_EQ(OB_SUCCESS, common::convert_persist_profile_to_realtime(persist, size, restored, &allocator_));
  ASSERT_NE(nullptr, restored);
  ASSERT_NE(nullptr, restored->get_child_head()->next_);
  ASSERT_EQ(nullptr, restored->get_child_head()->next_->next_);
  restored->~ObOpProfile();
}

TEST_F(TestIvfSSTableScanIter, profile_knn_refine_with_and_without_caller_stats)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  common::ObOpProfile<common::ObMetric> profile(common::ObProfileId::VECTOR_SEARCH_IVF_SEGMENT, &allocator_);
  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  query_param.read_stats_ = &stats;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
  iter.set_profile(&profile);
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, result));
  ASSERT_EQ(2, result.count());
  ASSERT_EQ(2, stats.raw_vector_refine_count_);
  expect_profile_metric(profile, common::ObMetricId::VEC_IVF_RAW_VECTOR_REFINE_COUNT, 2);

  // Restart the same search without caller statistics; Profile must still count both vectors.
  iter.query_param_.read_stats_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
  iter.set_profile(&profile);
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(2, result));
  ASSERT_EQ(2, result.count());
  ASSERT_EQ(2, stats.raw_vector_refine_count_);
  expect_profile_metric(profile, common::ObMetricId::VEC_IVF_RAW_VECTOR_REFINE_COUNT, 4);
}

TEST_F(TestIvfSSTableScanIter, profile_keeps_failed_work_and_saturating_caller_stats)
{
  const int32_t dim = 64;
  const int64_t cids[] = {0, 0};
  const int64_t vids[] = {10, 20};
  float centroid[dim] = {0};
  float encoded[2 * dim] = {0};
  float corrupt_raw[2 * dim] = {0};
  float query[dim] = {0};
  char flip[4 * dim / CHAR_BIT] = {0};
  encoded[0] = encoded[dim] = query[0] = 1.0F;
  centroid[2] = 0.05F;
  corrupt_raw[0] = std::numeric_limits<float>::quiet_NaN();
  corrupt_raw[dim] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_disk_bbq_index(encoded, cids, vids, 2,
      centroid, 1, dim, flip, 1, nullptr, share::VIDA_COS, true, corrupt_raw));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  common::ObOpProfile<common::ObMetric> profile(common::ObProfileId::VECTOR_SEARCH_IVF_SEGMENT, &allocator_);
  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfSSTableReadStats caller_stats;
  caller_stats.raw_vector_key_count_ = INT64_MAX - 1;
  caller_stats.first_low_precision_vector_scan_cid_ = 7;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  ObRoaringBitmap allowed_vids(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(10));
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(20));
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  query_param.pre_filter_ = &pre_filter;
  query_param.read_stats_ = &caller_stats;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
  iter.set_profile(&profile);
  ObIvfKnnSearchResult result;
  // RAW_VECTOR I/O has been submitted when decoding the corrupt vector fails.
  ASSERT_EQ(OB_NUMERIC_OVERFLOW, iter.brute_force_search(result));
  ASSERT_EQ(0, result.count());
  expect_profile_metric(profile, common::ObMetricId::VEC_IVF_RAW_VECTOR_READ_COUNT, 1);
  expect_profile_metric(profile, common::ObMetricId::VEC_IVF_RAW_VECTOR_KEY_COUNT, 2);
  ASSERT_EQ(INT64_MAX, caller_stats.raw_vector_key_count_);
  ASSERT_EQ(7, caller_stats.first_low_precision_vector_scan_cid_);
  bool exists = true;
  uint64_t value = 0;
  profile.get_metric_value(common::ObMetricId::VEC_IVF_SEGMENT_RETURN_ROWS_COUNT, exists, value);
  ASSERT_FALSE(exists);
  profile.get_metric_value(common::ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID, exists, value);
  ASSERT_FALSE(exists);
  profile.get_metric_value(common::ObMetricId::VEC_IVF_LOW_PRECISION_VECTOR_COUNT, exists, value);
  ASSERT_FALSE(exists);
  iter.reset();
  allowed_vids.set_empty();
}

TEST_F(TestIvfSSTableScanIter, profile_brute_force_accumulates_raw_reads_only)
{
  const int32_t dim = 64;
  float query[dim] = {};
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());
  common::ObOpProfile<common::ObMetric> profile(common::ObProfileId::VECTOR_SEARCH_IVF_SEGMENT, &allocator_);
  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfSSTableReadStats stats;
  ObIvfKnnSearchParam query_param;
  fill_search_params(query_param, query, dim, 2);
  ObRoaringBitmap allowed_vids(&allocator_);
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(10));
  ASSERT_EQ(OB_SUCCESS, allowed_vids.value_add(20));
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  query_param.pre_filter_ = &pre_filter;
  query_param.read_stats_ = &stats;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(range, scan_param_.snapshot_.version()));
  iter.set_profile(&profile);
  ObIvfKnnSearchResult result;
  ASSERT_EQ(OB_SUCCESS, iter.brute_force_search(result));
  ASSERT_EQ(OB_SUCCESS, iter.brute_force_search(result));
  expect_profile_metric(profile, common::ObMetricId::VEC_IVF_SEGMENT_RETURN_ROWS_COUNT, 4);
  expect_profile_metric(profile, common::ObMetricId::VEC_IVF_RAW_VECTOR_REFINE_COUNT, stats.raw_vector_refine_count_);
  ASSERT_EQ(4, stats.raw_vector_refine_count_);
  // Profile collection does not require or replace the caller's read statistics.
  iter.query_param_.read_stats_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.brute_force_search(result));
  expect_profile_metric(profile, common::ObMetricId::VEC_IVF_SEGMENT_RETURN_ROWS_COUNT, 6);
  expect_profile_metric(profile, common::ObMetricId::VEC_IVF_RAW_VECTOR_REFINE_COUNT, 6);
  ASSERT_EQ(4, stats.raw_vector_refine_count_);
  bool exists = true;
  uint64_t value = 0;
  profile.get_metric_value(common::ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID, exists, value);
  ASSERT_FALSE(exists);
  iter.reset();
  allowed_vids.set_empty();
}

TEST_F(TestIvfSSTableScanIter, searches_empty_ivf_sstable)
{
  const int32_t dim = 64;
  float query[dim] = {0};
  query[0] = 1.0F;
  ASSERT_EQ(OB_SUCCESS, build_two_vector_sstable(dim));
  sstable_.ivf_meta_.set_empty();
  ASSERT_TRUE(sstable_.get_ivf_meta().is_empty());
  ASSERT_EQ(OB_SUCCESS, install_ivf_sstable());

  ObIvfSSTableScanIter iter(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_scan_param_and_ctx(iter));
  ASSERT_EQ(OB_SUCCESS, iter.init(scan_param_, tablet_handle_));
  ObIvfKnnSearchParam query_param;
  ObIvfKnnSearchResult result;
  fill_search_params(query_param, query, dim, 1);
  ObRoaringBitmap allowed_vids(&allocator_);
  ObIvfPreFilter pre_filter;
  ASSERT_EQ(OB_SUCCESS, pre_filter.init(allowed_vids));
  query_param.pre_filter_ = &pre_filter;
  ASSERT_EQ(OB_SUCCESS, iter.set_query(query_param));

  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));
  ASSERT_EQ(OB_SUCCESS, iter.knn_search(1, result));
  ASSERT_EQ(0, result.count());
  ASSERT_FALSE(iter.has_remaining_centroids());
  ASSERT_NE(nullptr, iter.searcher_);

  ASSERT_EQ(OB_SUCCESS, iter.switch_sstable(version_range, scan_param_.snapshot_.version()));
  ASSERT_EQ(OB_SUCCESS, iter.brute_force_search(result));
  ASSERT_EQ(0, result.count());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase;
  signal(49, SIG_IGN);
  system("rm -f test_ivf_sstable_scan_iter.log*");
  OB_LOGGER.set_file_name("test_ivf_sstable_scan_iter.log", true);
  OB_LOGGER.set_log_level("INFO");
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
  MTL_BIND2(mtl_new_default,
      oceanbase::share::ObIVFReplayMemoryMgr::mtl_init,
      nullptr, nullptr, nullptr, mtl_destroy_default);
  if (OB_SUCCESS != oceanbase::MockTenantModuleEnv::get_instance().init()) {
    return 1;
  }
  const int ret = RUN_ALL_TESTS();
  oceanbase::MockTenantModuleEnv::get_instance().destroy();
  std::_Exit(ret);
}
