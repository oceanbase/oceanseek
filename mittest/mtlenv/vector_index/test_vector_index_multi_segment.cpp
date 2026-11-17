/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// owner: ningxin.ning
// owner group: shenzhen
// Exercise DDL parallel cutover, quantized tails, reload and existing cleanup.
#include <gtest/gtest.h>
#include <atomic>
#include <sstream>
#include <thread>
#include <vector>
#define private public
#define protected public
#include "mtlenv/mock_tenant_module_env.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_vector_index_segment.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_merge_task.h"
#include "share/vector_index/ob_vector_mem_sync_executor.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "lib/allocator/ob_sql_mem_leak_checker.h"
#include "lib/alloc/ob_malloc_callback.h"
#include "storage/lob/test_lob_common.h"
#include "storage/high_availability/ob_migration_vector_index.h"
#include "storage/high_availability/ob_migration_vector_index_processor.h"
#undef protected
#undef private
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace share
{
using namespace common;

// Check the implementation helper's outputs without adding a public header API.
void calc_segment_ef_params(ObVectorIndexSegQueryHandler &handler,
                           const common::ObIArray<ObVectorIndexSegmentQuerier *> &segments,
                           int segment_cnt, int64_t vector_count,
                           int64_t &actual_ef_search, int64_t &actual_query_limit);

class TestVectorIndexMultiSegment : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::NORMAL_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    storage::ObServerStorageMetaService::get_instance().is_started_ = true;
    MTL(transaction::ObTransService*)->tx_desc_mgr_.tx_id_allocator_ =
        [](transaction::ObTransID &tx_id) {
          static std::atomic<int64_t> next_tx_id{1001};
          tx_id = transaction::ObTransID(next_tx_id.fetch_add(1));
          return OB_SUCCESS;
        };
    ASSERT_EQ(OB_SUCCESS, storage::TestLobCommon::create_data_tablet(
        MTL_ID(), ObLSID(storage::TestLobCommon::TEST_LOB_LS_ID),
        ObTabletID(storage::TestLobCommon::TEST_LOB_TABLE_ID),
        ObTabletID(storage::TestLobCommon::TEST_LOB_META_TABLE_ID),
        ObTabletID(storage::TestLobCommon::TEST_LOB_PIECE_TABLE_ID)));
  }
  static void TearDownTestCase()
  {
    // Stop all users before clearing the cache, while its object pools are alive.
    // Keep cleanup active if a fatal assertion below returns early.
    DEFER(MockTenantModuleEnv::get_instance().destroy();
          ObTabletAutoincrementService::get_instance().destroy());
    auto *ls_service = MTL(storage::ObLSService*);
    const ObLSID ls_id(storage::TestLobCommon::TEST_LOB_LS_ID);
    storage::ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, storage::ObLSGetMod::STORAGE_MOD));
    int64_t tx_count = 0;
    const int64_t deadline = ObTimeUtility::current_time() + 30 * 1000 * 1000;
    do {
      ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_ctx_count(tx_count));
      if (tx_count > 0) {
        ob_usleep(20 * 1000);
      }
    } while (tx_count > 0 && ObTimeUtility::current_time() < deadline);
    ASSERT_EQ(0, tx_count);
    ls_handle.reset();
    ASSERT_EQ(OB_SUCCESS, ls_service->remove_ls(ls_id));
  }
  void SetUp() override
  {
    lib::ContextParam config;
    config.set_mem_attr(MTL_ID(), "TestVecDdl")
        .set_properties(lib::ADD_CHILD_THREAD_SAFE | lib::ALLOC_THREAD_SAFE
            | lib::RETURN_MALLOC_DEFAULT);
    ASSERT_EQ(OB_SUCCESS, ROOT_CONTEXT->CREATE_CONTEXT(context_, config));
    adaptor_ = new ObPluginVectorIndexAdaptor(
        &context_->get_malloc_allocator(), context_, MTL_ID());
  }
  void TearDown() override
  {
    delete adaptor_;
    EXPECT_EQ(0, used_);
    DESTROY_CONTEXT(context_);
  }
  void prepare(const int64_t limit, const char *type = "HNSW", const bool duplicate = false)
  {
    init_adaptor(type, duplicate);
    init_build(limit);
  }
  void init_adaptor(const char *type = "HNSW", const bool duplicate = false)
  {
    ObSqlString params;
    ASSERT_EQ(OB_SUCCESS, params.assign_fmt(
        "TYPE=%s,DISTANCE=L2,LIB=VSAG,M=16,EF_CONSTRUCTION=40,EF_SEARCH=64%s",
        type, duplicate ? ",DUPLICATE_GROUP=TRUE" : ""));
    ASSERT_EQ(OB_SUCCESS, adaptor_->init(params.string(), 8, context_, &used_));
    ASSERT_EQ(OB_SUCCESS, adaptor_->set_tablet_id(VIRT_SNAP, ObTabletID(200001)));
  }
  void init_build(const int64_t limit)
  {
    // Initialize through the existing entry, then fix the capacity for these builder tests.
    ASSERT_EQ(OB_SUCCESS, adaptor_->init_snap_data_for_build(false));
    auto &snap = snapshot_data();
    snap.enable_multi_segment_ = true;
    snap.segment_max_vectors_ = limit;
  }
  ObVecIdxSnapshotData &snapshot_data() { return *adaptor_->get_snap_data().operator->(); }
  int append(const int64_t first, const int count, const bool duplicate = false)
  {
    int ret = OB_SUCCESS;
    ObSEArray<float, 64> vectors;
    ObSEArray<int64_t, 8> vids;
    for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(vids.push_back(first + i))) {
        ADD_FAILURE() << "allocate test VIDs failed: " << ret;
      }
      for (int d = 0; OB_SUCC(ret) && d < 8; ++d) {
        if (OB_FAIL(vectors.push_back(duplicate ? 1.0F : static_cast<float>(first + i + d)))) {
          ADD_FAILURE() << "allocate test vectors failed: " << ret;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = adaptor_->add_snap_index(vectors.empty() ? nullptr : &vectors.at(0),
          vids.empty() ? nullptr : &vids.at(0), nullptr, 0, count, nullptr, true);
    }
    return ret;
  }
  void prepare_merge(const char *type = "HNSW", const int64_t source_count = 12,
      const int64_t capacity = 4)
  {
    init_adaptor(type);
    adaptor_->set_created_by_segment_merge(true);
    ASSERT_EQ(OB_SUCCESS, adaptor_->init_snap_data_for_build(true));
    auto &first = *snapshot_data().builder_;
    ASSERT_EQ(OB_SUCCESS, first.ibitmap_->init(true, false));
    ASSERT_EQ(OB_SUCCESS, first.vbitmap_->init(true, true));
    ObVectorIndexSegmentHandle source;
    ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(VIAT_HNSW, source));
    for (int64_t vid = 1; vid <= source_count; ++vid) {
      float vector[8] = {static_cast<float>(vid)};
      ASSERT_EQ(OB_SUCCESS, source->add_index(vector, &vid, 8, nullptr, 1));
    }
    source->set_read_vid_bound(ObVidBound(1, source_count));
    first.need_vid_check_ = true;
    first.vid_bound_.set_vid_bound(source_count + 8, 1); // Includes nonmembers, so exact membership matters.
    ASSERT_EQ(OB_SUCCESS, first.merge_source_seg_handles_.push_back(source));
    {
      lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
      roaring::api::roaring64_bitmap_add(first.vbitmap_->delete_bitmap_, 42000);
    }
    ASSERT_EQ(OB_SUCCESS, adaptor_->enable_merge_multi_segment(capacity));
    ASSERT_TRUE(snapshot_data().enable_multi_segment_);
  }
  void verify_merge_bitmaps(const int64_t expected_count, const int64_t expected_first_insert_count = 0,
      const ObVectorIndexSegmentType expected_type = ObVectorIndexSegmentType::BASE)
  {
    int64_t total = 0;
    auto &bases = snapshot_data().meta_.bases_;
    for (int64_t i = 0; i < bases.count(); ++i) {
      const auto &meta = bases.at(i);
      ASSERT_EQ(expected_type, meta.seg_type_);
      ASSERT_TRUE(meta.has_segment_meta_row_);
      const auto &handle = meta.segment_handle_;
      int64_t count = 0;
      ASSERT_EQ(OB_SUCCESS, handle->get_index_number(count));
      total += count;
      ASSERT_NE(nullptr, handle->ibitmap_);
      ASSERT_NE(nullptr, meta.vbitmap_);
      EXPECT_EQ(meta.scn_, meta.bitmap_scn_);
      EXPECT_EQ(count, roaring::api::roaring64_bitmap_get_cardinality(handle->ibitmap_->insert_bitmap_));
      EXPECT_EQ(i == 0 ? 1 : 0, roaring::api::roaring64_bitmap_get_cardinality(meta.vbitmap_->delete_bitmap_));
      EXPECT_EQ(i == 0, roaring::api::roaring64_bitmap_contains(meta.vbitmap_->delete_bitmap_, 42000));
      EXPECT_EQ(i == 0 ? expected_first_insert_count : 0,
          roaring::api::roaring64_bitmap_get_cardinality(meta.vbitmap_->insert_bitmap_));
      for (int64_t vid = 1; vid <= 20; ++vid) {
        bool exists = false;
        ASSERT_EQ(OB_SUCCESS, handle->check_id_exist(vid, exists));
        EXPECT_EQ(exists, roaring::api::roaring64_bitmap_contains(handle->ibitmap_->insert_bitmap_, vid));
      }
      if (i > 0) {
        EXPECT_NE(bases.at(0).segment_handle_->ibitmap_, handle->ibitmap_);
        EXPECT_NE(bases.at(0).vbitmap_, meta.vbitmap_);
      }
    }
    EXPECT_EQ(expected_count, total);
  }
  void prepare_bitmap_merge(const bool full, ObVectorIndexSegmentMeta &source,
      const char *type = "HNSW", const bool multi_output = true)
  {
    prepare_merge(type);
    auto &first = *snapshot_data().builder_;
    source.segment_handle_ = first.merge_source_seg_handles_.at(0);
    source.set_vbitmap(first.vbitmap_);
    {
      lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
      for (uint64_t vid = 1; vid <= 12; ++vid) {
        roaring::api::roaring64_bitmap_add(source.vbitmap_->insert_bitmap_, vid);
      }
      if (full) {
        roaring::api::roaring64_bitmap_add(source.vbitmap_->delete_bitmap_, 11);
        roaring::api::roaring64_bitmap_add(source.vbitmap_->delete_bitmap_, 12);
      }
    }
    snapshot_data().free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
    ObVecIdxMergeTask task;
    task.tenant_id_ = MTL_ID();
    task.merge_from_base_ = true;
    task.min_vid_ = 1;
    task.max_vid_ = 20;
    ObVectorIndexSegmentMeta meta;
    meta.seg_type_ = ObVectorIndexSegmentType::BASE;
    meta.segment_handle_ = source.segment_handle_;
    meta.set_vbitmap(source.vbitmap_);
    ASSERT_EQ(OB_SUCCESS, task.merge_source_meta_.bases_.push_back(meta));
    if (!full) {
      ASSERT_EQ(OB_SUCCESS, task.merge_source_meta_.bases_.push_back(meta));
    }
    task.merge_plan_.begin_ = 0;
    task.merge_plan_.end_ = 1;
    task.merge_plan_.output_base_ = true;
    task.merge_plan_.output_type_ = ObVectorIndexSegmentType::BASE;
    task.merge_plan_.strategy_ = full ? ObVecMergePlan::FULL_MERGE : ObVecMergePlan::DELETE_MERGE;
    ASSERT_EQ(OB_SUCCESS, task.merge_segments_.push_back(&task.merge_source_meta_.bases_.at(0)));
    ASSERT_EQ(OB_SUCCESS, task.merge_bitmap(adaptor_));
    if (multi_output) {
      ASSERT_EQ(OB_SUCCESS, adaptor_->enable_merge_multi_segment(4));
    }
  }
  void verify_membership(const int64_t first, const int64_t count)
  {
    auto &snap = snapshot_data();
    ObSEArray<ObVectorIndexSegmentHandle, 16> segments;
    for (auto *builder : snap.sealed_builders_) {
      ASSERT_EQ(OB_SUCCESS, segments.push_back(builder->segment_handle_));
    }
    if (nullptr != snap.builder_ && !snap.builder_->is_build_finished()) {
      if (snap.builder_->segment_handle_.is_valid()) {
        int64_t active_count = 0;
        ASSERT_EQ(OB_SUCCESS, snap.builder_->segment_handle_->get_index_number(active_count));
        if (active_count > 0) {
          ASSERT_EQ(OB_SUCCESS, segments.push_back(snap.builder_->segment_handle_));
        }
      } else {
        ASSERT_TRUE(snap.builder_->is_empty_buffer());
      }
    }
    for (auto &base : snap.meta_.bases_) {
      ASSERT_EQ(OB_SUCCESS, segments.push_back(base.segment_handle_));
    }
    int64_t total = 0;
    for (auto &segment : segments) {
      ASSERT_TRUE(segment.is_valid());
      int64_t segment_count = 0;
      ASSERT_EQ(OB_SUCCESS, segment->get_index_number(segment_count));
      ASSERT_GT(segment_count, 0);
      total += segment_count;
    }
    EXPECT_EQ(count, total);
    for (int64_t vid = first; vid < first + count; ++vid) {
      int occurrences = 0;
      for (auto &segment : segments) {
        bool exists = false;
        ASSERT_EQ(OB_SUCCESS, segment->check_id_exist(vid, exists));
        occurrences += exists;
      }
      ASSERT_EQ(1, occurrences) << "VID=" << vid;
    }
  }
  void init_serialize_param(ObHNSWSerializeCallback::CbParam &param,
      ObVecIdxSnapshotDataWriteCtx &ctx, transaction::ObTxDesc &tx,
      transaction::ObTxReadSnapshot &snapshot, ObIAllocator &allocator)
  {
    param.vctx_ = &ctx;
    param.allocator_ = &allocator;
    param.tmp_allocator_ = &allocator;
    param.tx_desc_ = &tx;
    param.snapshot_ = &snapshot;
    param.tablet_id_ = ObTabletID(200001);
    param.snapshot_version_ = 123456789;
    param.lob_inrow_threshold_ = OB_MAX_LOB_INROW_THRESHOLD;
  }
  void verify_quantized_mixed(const char *type, ObVectorIndexAlgorithmType expected_type)
  {
    prepare(10001, type);
    ASSERT_EQ(OB_SUCCESS, append(1, 10001));
    // Replayed VIDs must also be filtered from a trained SQ/BQ sealed segment.
    ASSERT_EQ(OB_SUCCESS, append(1, 64));
    ASSERT_EQ(OB_SUCCESS, append(10002, 3));
    ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
    auto &snap = snapshot_data();
    ASSERT_EQ(1, snap.sealed_builders_.count());
    EXPECT_EQ(expected_type, snap.sealed_builders_.at(0)->segment_handle_->get_index_type());
    ASSERT_NE(nullptr, snap.builder_);
    ASSERT_TRUE(snap.builder_->segment_handle_.is_valid());
    EXPECT_EQ(VIAT_HGRAPH, snap.builder_->segment_handle_->get_index_type());
    ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
    verify_membership(1, 10004);
    ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
    ASSERT_EQ(2, snap.meta_.bases_.count());
    EXPECT_EQ(expected_type, snap.meta_.bases_.at(0).index_type_);
    EXPECT_EQ(VIAT_HGRAPH, snap.meta_.bases_.at(1).index_type_);
    verify_membership(1, 10004);
    for (int i = 0; i < snap.meta_.bases_.count(); ++i) {
      auto &source = snap.meta_.bases_.at(i).segment_handle_;
      std::stringstream stream;
      ASSERT_EQ(OB_SUCCESS, source->fserialize(stream));
      ObVectorIndexSegmentHandle loaded;
      ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(source->get_index_type(), loaded));
      ASSERT_EQ(OB_SUCCESS, loaded->fdeserialize(stream));
      int64_t count = 0;
      ASSERT_EQ(OB_SUCCESS, loaded->get_index_number(count));
      EXPECT_EQ(i == 0 ? 10001 : 3, count);
      bool exists = false;
      ASSERT_EQ(OB_SUCCESS, loaded->check_id_exist(i == 0 ? 10001 : 10004, exists));
      EXPECT_TRUE(exists);
    }
  }
  int serialize_snapshot(const bool is_rebuild = false, const bool skip_for_merge = true)
  {
    int ret = OB_SUCCESS;
    auto *tx_service = MTL(transaction::ObTransService*);
    transaction::ObTxDesc *tx = nullptr;
    transaction::ObTxParam tx_param;
    transaction::ObTxSEQ savepoint;
    transaction::ObTxReadSnapshot snapshot;
    ObVecIdxSnapshotDataWriteCtx ctx;
    ObHNSWSerializeCallback::CbParam param;
    const int64_t deadline = ObTimeUtility::current_time() + storage::TestLobCommon::TX_LOB_EXPIRE_TIME_US;
    storage::TestDmlCommon::build_tx_param(tx_param);
    if (OB_FAIL(storage::TestDmlCommon::build_tx_desc(MTL_ID(), tx))) {
    } else if (OB_FAIL(tx_service->create_implicit_savepoint(*tx, tx_param, savepoint, true))) {
    } else if (OB_FAIL(tx_service->get_read_snapshot(*tx, transaction::ObTxIsolationLevel::RC,
        deadline, snapshot))) {
    } else {
      init_serialize_param(param, ctx, *tx, snapshot, context_->get_malloc_allocator());
      ctx.ls_id_ = ObLSID(storage::TestLobCommon::TEST_LOB_LS_ID);
      ctx.data_tablet_id_ = ObTabletID(storage::TestLobCommon::TEST_LOB_TABLE_ID);
      ctx.lob_meta_tablet_id_ = ObTabletID(storage::TestLobCommon::TEST_LOB_META_TABLE_ID);
      ctx.lob_piece_tablet_id_ = ObTabletID(storage::TestLobCommon::TEST_LOB_PIECE_TABLE_ID);
      ctx.lob_col_idx_ = 3;
      param.timeout_ = deadline;
      param.is_vec_tablet_rebuild_ = is_rebuild;
      ret = adaptor_->serialize_snapshot(param, skip_for_merge);
      const int rollback_ret = tx_service->rollback_to_implicit_savepoint(*tx, savepoint, deadline, nullptr);
      EXPECT_EQ(OB_SUCCESS, rollback_ret);
      if (OB_SUCC(ret)) {
        ret = rollback_ret;
      }
    }
    if (nullptr != tx) {
      const int commit_ret = tx_service->commit_tx(*tx, deadline);
      EXPECT_EQ(OB_SUCCESS, commit_ret);
      if (OB_SUCC(ret)) {
        ret = commit_ret;
      }
      tx_service->release_tx(*tx);
    }
    return ret;
  }
  void verify_quantized_exact_threshold(const char *type)
  {
    prepare(4, type);
    ASSERT_EQ(OB_SUCCESS, append(1, 4));
    auto &snap = snapshot_data();
    ASSERT_NE(nullptr, snap.builder_);
    ASSERT_TRUE(snap.builder_->is_empty_buffer());
    ASSERT_EQ(1, snap.sealed_builders_.count());
    ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
    ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
    ASSERT_NE(nullptr, snap.builder_);
    ASSERT_TRUE(snap.builder_->is_empty_buffer());
    ASSERT_EQ(1, snap.sealed_builders_.count());
    verify_membership(1, 4);
    ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
    EXPECT_EQ(nullptr, snap.builder_);
    ASSERT_EQ(1, snap.meta_.bases_.count());
    EXPECT_EQ(VIAT_HGRAPH, snap.meta_.bases_.at(0).index_type_);
    verify_membership(1, 4);
  }
  void verify_quantized_tail_rescan(const char *type)
  {
    prepare(4, type);
    ASSERT_EQ(OB_SUCCESS, append(1, 4));
    ASSERT_EQ(OB_SUCCESS, append(5, 2));
    ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
    auto &snap = snapshot_data();
    ASSERT_EQ(1, snap.sealed_builders_.count());
    ASSERT_NE(nullptr, snap.builder_);
    ASSERT_FALSE(snap.builder_->has_build_);
    ASSERT_EQ(nullptr, snap.builder_->vid_array_);
    ASSERT_TRUE(snap.builder_->segment_handle_.is_valid());
    // A failure after check_snap_index (e.g. allocating the key prefix) leaves
    // this unsealed tail in place. Retry the scan on the same adaptor.
    ASSERT_EQ(OB_SUCCESS, append(1, 6));
    ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
    verify_membership(1, 6);
    ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
    ASSERT_EQ(2, snap.meta_.bases_.count());
    verify_membership(1, 6);
  }
  lib::MemoryContext context_;
  ObPluginVectorIndexAdaptor *adaptor_ = nullptr;
  uint64_t used_ = 0;
};

TEST_F(TestVectorIndexMultiSegment, empty_input_does_not_allocate_segment)
{
  init_adaptor();
  // Exercise an empty batch after multi-segment mode selection, before the first builder.
  snapshot_data().enable_multi_segment_ = true;
  snapshot_data().segment_max_vectors_ = 10;
  ASSERT_EQ(OB_SUCCESS, adaptor_->add_snap_index(nullptr, nullptr, nullptr, 0, 0, nullptr, true));
  EXPECT_EQ(nullptr, snapshot_data().builder_);
  EXPECT_FALSE(snapshot_data().is_inited());
  EXPECT_EQ(0, used_);
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  auto &snap = snapshot_data();
  EXPECT_EQ(nullptr, snap.builder_);
  EXPECT_EQ(0, snap.sealed_builders_.count());
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  param.vctx_ = &ctx;
  param.allocator_ = &context_->get_malloc_allocator();
  EXPECT_EQ(OB_NOT_INIT, adaptor_->serialize_snapshot(param));
  EXPECT_EQ(0, ctx.vals_.count());
}

TEST_F(TestVectorIndexMultiSegment, retry_after_partial_scan_does_not_duplicate_sealed_vids)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 2));
  // Retry the scan with both previously sealed and current-builder VIDs.
  ASSERT_EQ(OB_SUCCESS, append(1, 8));
  ASSERT_EQ(OB_SUCCESS, append(1, 8));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  EXPECT_EQ(2, snapshot_data().meta_.bases_.count());
  verify_membership(1, 8);
}

TEST_F(TestVectorIndexMultiSegment, retry_checks_membership_not_only_sealed_vid_range)
{
  prepare(2);
  ASSERT_EQ(OB_SUCCESS, append(1, 1));
  ASSERT_EQ(OB_SUCCESS, append(3, 1));
  // VID 2 is inside the sealed segment's range but must still be inserted.
  ASSERT_EQ(OB_SUCCESS, append(1, 3));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  verify_membership(1, 3);
}

TEST_F(TestVectorIndexMultiSegment, sq_retry_filters_sealed_vids_before_buffering)
{
  prepare(4, "HNSW_SQ");
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(1, 6));
  ASSERT_NE(nullptr, snapshot_data().builder_);
  EXPECT_EQ(2, snapshot_data().builder_->vid_array_->count());
  EXPECT_EQ(6, snapshot_data().builder_->total_cnt_);
  EXPECT_EQ(4, snapshot_data().builder_->skip_cnt_);
  EXPECT_EQ(2, snapshot_data().builder_->add_cnt_);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  verify_membership(1, 6);
}

TEST_F(TestVectorIndexMultiSegment, bq_retry_of_all_sealed_vids_drops_empty_tail)
{
  prepare(4, "HNSW_BQ");
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_NE(nullptr, snapshot_data().builder_);
  EXPECT_TRUE(snapshot_data().builder_->is_empty_buffer());
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  EXPECT_EQ(1, snapshot_data().meta_.bases_.count());
  verify_membership(1, 4);
}

TEST_F(TestVectorIndexMultiSegment, sparse_buffer_filter_keeps_vector_offsets)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ObVectorIndexSegmentBuilder builder;
  auto &allocator = context_->get_malloc_allocator();
  ASSERT_EQ(OB_SUCCESS, builder.init_vec_buffer(MTL_ID(), allocator, true));
  int64_t vids[] = {2, 20, 4};
  uint32_t lens[] = {1, 2, 3};
  uint32_t dims[] = {1, 2, 3, 4, 5, 6};
  float vals[] = {10, 20, 30, 40, 50, 60};
  EXPECT_EQ(OB_SUCCESS, builder.add_to_buffer(lens, dims, vals, vids, nullptr, 3, 0,
      &snapshot_data().sealed_builders_));
  EXPECT_EQ(1, builder.vid_array_->count());
  EXPECT_EQ(20, builder.vid_array_->at(0));
  EXPECT_EQ(2, builder.lens_array_->at(0));
  EXPECT_EQ(2, builder.dims_array_->count());
  EXPECT_EQ(2, builder.dims_array_->at(0));
  EXPECT_EQ(3, builder.dims_array_->at(1));
  EXPECT_FLOAT_EQ(20, builder.vals_array_->at(0));
  EXPECT_FLOAT_EQ(30, builder.vals_array_->at(1));
  builder.free(allocator);
}

TEST_F(TestVectorIndexMultiSegment, invalid_sealed_builder_is_an_error)
{
  ObVectorIndexSegmentBuilder builder;
  ObSEArray<ObVectorIndexSegmentBuilder *, 1> sealed;
  ASSERT_EQ(OB_SUCCESS, sealed.push_back(nullptr));
  bool skip = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED, builder.check_vid_range(1, skip, &sealed));
  EXPECT_EQ(0, builder.add_cnt_);
  EXPECT_EQ(0, builder.skip_cnt_);
}

TEST_F(TestVectorIndexMultiSegment, merge_source_filter_remains_an_inclusion_check)
{
  prepare(2);
  ASSERT_EQ(OB_SUCCESS, append(1, 1));
  ASSERT_EQ(OB_SUCCESS, append(3, 1));
  ObVectorIndexSegmentBuilder builder;
  builder.need_vid_check_ = true;
  builder.vid_bound_.set_vid_bound(3, 1);
  ASSERT_EQ(OB_SUCCESS, builder.merge_source_seg_handles_.push_back(
      snapshot_data().sealed_builders_.at(0)->segment_handle_));
  bool skip = true;
  ASSERT_EQ(OB_SUCCESS, builder.check_vid_range(1, skip));
  EXPECT_FALSE(skip);
  ASSERT_EQ(OB_SUCCESS, builder.check_vid_range(2, skip));
  EXPECT_TRUE(skip);
  ASSERT_EQ(OB_SUCCESS, builder.check_vid_range(4, skip));
  EXPECT_TRUE(skip);
  EXPECT_EQ(3, builder.total_cnt_);
  EXPECT_EQ(1, builder.add_cnt_);
  EXPECT_EQ(2, builder.skip_cnt_);
}

TEST_F(TestVectorIndexMultiSegment, merge_policy_input_separates_delete_statistics)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  auto &snap = snapshot_data();
  snap.set_inited();
  ASSERT_EQ(2, snap.meta_.bases_.count());
  // Exercise Base + Incr ordering with a real graph and its persisted bitmap.
  ASSERT_EQ(OB_SUCCESS, snap.meta_.incrs_.push_back(snap.meta_.bases_.at(1)));
  snap.meta_.bases_.pop_back();
  snap.meta_.incrs_.at(0).seg_type_ = ObVectorIndexSegmentType::FREEZE_PERSIST;
  {
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
    roaring::api::roaring64_bitmap_add(snap.meta_.bases_.at(0).vbitmap_->delete_bitmap_, 1);
  }
  ObVectorIndexMeta source;
  ObVecMergePolicyInput input;
  ObVecMergeConfig config;
  struct StatsAllocationCounter : public lib::ObMallocCallback
  {
    void operator()(const lib::ObMemAttr &attr, int64_t used, const lib::AObject &) override
    {
      if (used > 0 && attr.label_ == lib::ObLabel("VIBMergeStats")) { ++allocations_; }
    }
    int64_t allocations_ = 0;
  } allocations;
  // Full -> unrefreshed cache -> refreshed cache -> full checks input reuse.
  for (int pass = 0; pass < 4; ++pass) {
    const bool is_full_merge = pass == 0 || pass == 3;
    if (pass == 2) {
      snap.has_complete_ = true;
      ASSERT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts());
    }
    {
      lib::ObMallocCallbackGuard allocation_guard(allocations);
      ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(
          source, input, config, is_full_merge));
    }
    EXPECT_EQ(0, allocations.allocations_);
    ASSERT_EQ(1, input.base_count_);
    ASSERT_EQ(2, input.segments_.count());
    EXPECT_EQ(0, input.deleted_vid_count_); // Preparation never builds a global bitmap.
    for (int64_t i = 0; i < 2; ++i) {
      EXPECT_TRUE(input.segments_.at(i).readable_);
      EXPECT_EQ(is_full_merge ? 0 : 4, input.segments_.at(i).vector_count_);
      EXPECT_EQ(pass == 2 && i == 0 ? 1 : 0, input.segments_.at(i).deleted_vector_count_);
      EXPECT_EQ(snap.meta_.flat_seg_meta_at(i).scn_, input.segments_.at(i).scn_);
      EXPECT_EQ(snap.meta_.flat_seg_meta_at(i).segment_handle_.operator->(),
          source.flat_seg_meta_at(i).segment_handle_.operator->());
    }
  }
}

TEST_F(TestVectorIndexMultiSegment, segment_delete_count_follows_graph_without_persistence)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ObVecIdxSnapshotData &snap = snapshot_data();
  ASSERT_EQ(1, snap.meta_.bases_.count());
  ObVectorIndexSegmentMeta &meta = snap.meta_.bases_.at(0);
  ObVectorIndexSegmentHandle &graph = meta.segment_handle_;
  EXPECT_EQ(0, graph->get_deleted_vector_count());

  std::stringstream graph_before;
  ASSERT_EQ(OB_SUCCESS, graph->fserialize(graph_before));
  const int64_t size = meta.get_serialize_size();
  std::vector<char> meta_before(size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, meta.serialize(meta_before.data(), size, pos));
  graph->set_deleted_vector_count(2);

  ObVectorIndexSegmentMeta reused;
  ASSERT_EQ(OB_SUCCESS, reused.deep_copy_seg_key(meta.start_key_, meta.end_key_));
  ASSERT_EQ(OB_SUCCESS, ObPluginVectorIndexUtils::find_segment_in_old_meta(reused, snap.meta_));
  ASSERT_EQ(graph.get(), reused.segment_handle_.get());
  EXPECT_EQ(2, reused.segment_handle_->get_deleted_vector_count());
  reused.segment_handle_->set_deleted_vector_count(3);
  EXPECT_EQ(3, graph->get_deleted_vector_count());
  reused.release();
  EXPECT_EQ(3, graph->get_deleted_vector_count());

  std::stringstream graph_after;
  ASSERT_EQ(OB_SUCCESS, graph->fserialize(graph_after));
  EXPECT_EQ(graph_before.str(), graph_after.str());
  ASSERT_EQ(size, meta.get_serialize_size());
  std::vector<char> meta_after(size);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, meta.serialize(meta_after.data(), size, pos));
  EXPECT_EQ(meta_before, meta_after);

  ObVectorIndexSegmentHandle loaded;
  ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(VIAT_HNSW, loaded));
  ASSERT_EQ(OB_SUCCESS, loaded->fdeserialize(graph_after));
  EXPECT_EQ(0, loaded->get_deleted_vector_count());
  int64_t vector_count = 0;
  ASSERT_EQ(OB_SUCCESS, loaded->get_index_number(vector_count));
  EXPECT_EQ(4, vector_count);
}

TEST_F(TestVectorIndexMultiSegment, segment_delete_counts_union_sources_and_read_cache)
{
  prepare_merge("HNSW", 40, 4);
  ASSERT_EQ(OB_SUCCESS, append(10, 2));
  ASSERT_EQ(OB_SUCCESS, append(14, 2));
  ASSERT_EQ(OB_SUCCESS, append(20, 2));
  ASSERT_EQ(OB_SUCCESS, append(24, 2));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ObVecIdxSnapshotData &snap = snapshot_data();
  snap.set_inited();
  snap.has_complete_ = true;
  ASSERT_EQ(2, snap.meta_.bases_.count());
  ASSERT_EQ(OB_SUCCESS, snap.meta_.incrs_.push_back(snap.meta_.bases_.at(1)));
  snap.meta_.bases_.pop_back();
  snap.meta_.incrs_.at(0).seg_type_ = ObVectorIndexSegmentType::FREEZE_PERSIST;
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_vbitmap_data(adaptor_->vbitmap_data_));
  ASSERT_EQ(OB_SUCCESS, adaptor_->create_vbitmap_data(adaptor_->frozen_data_->vbitmap_));
  adaptor_->frozen_data_->set_has_frozen();
  roaring::api::roaring64_bitmap_t *persisted = snap.meta_.bases_.at(0).vbitmap_->delete_bitmap_;
  roaring::api::roaring64_bitmap_t *active = adaptor_->vbitmap_data_->bitmap_->delete_bitmap_;
  roaring::api::roaring64_bitmap_t *frozen = adaptor_->frozen_data_->vbitmap_->bitmap_->delete_bitmap_;
  lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
  // Each source alone must count historical Base and Incr members, excluding holes.
  for (roaring::api::roaring64_bitmap_t *only_source : {persisted, frozen, active}) {
    for (roaring::api::roaring64_bitmap_t *bitmap : {persisted, frozen, active}) {
      while (!roaring::api::roaring64_bitmap_is_empty(bitmap)) {
        roaring::api::roaring64_bitmap_remove(bitmap, roaring::api::roaring64_bitmap_minimum(bitmap));
      }
    }
    for (const int64_t vid : {1, 10, 12, 15, 20, 22, 100}) {
      roaring::api::roaring64_bitmap_add(only_source, vid);
    }
    ASSERT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts());
    EXPECT_EQ(2, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
    EXPECT_EQ(1, snap.meta_.incrs_.at(0).segment_handle_->get_deleted_vector_count());
  }
  for (roaring::api::roaring64_bitmap_t *bitmap : {persisted, frozen}) {
    for (const int64_t vid : {10, 15, 20}) { roaring::api::roaring64_bitmap_add(bitmap, vid); }
  }
  ASSERT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts());
  EXPECT_EQ(2, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(1, snap.meta_.incrs_.at(0).segment_handle_->get_deleted_vector_count());
  roaring::api::roaring64_bitmap_add(active, 25);
  ObVectorIndexMeta source;
  ObVecMergePolicyInput input;
  ObVecMergeConfig config;
  ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(source, input, config));
  EXPECT_EQ(1, input.segments_.at(1).deleted_vector_count_); // No implicit refresh.
  EXPECT_EQ(0, input.deleted_vid_count_);
  for (int pass = 0; pass < 2; ++pass) {
    ASSERT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts());
    ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(source, input, config));
    EXPECT_EQ(2, input.segments_.at(0).deleted_vector_count_);
    EXPECT_EQ(2, input.segments_.at(1).deleted_vector_count_);
  }
  // Active and frozen may refer to the same bitmap during freeze.
  adaptor_->frozen_data_->vbitmap_ = adaptor_->vbitmap_data_;
  ASSERT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts());
  EXPECT_EQ(2, snap.meta_.incrs_.at(0).segment_handle_->get_deleted_vector_count());
}

TEST_F(TestVectorIndexMultiSegment, segment_delete_count_seek_boundaries)
{
  prepare_merge("HNSW", 40, 4);
  ASSERT_EQ(OB_SUCCESS, append(10, 2));
  ASSERT_EQ(OB_SUCCESS, append(14, 2));
  ASSERT_EQ(OB_SUCCESS, append(20, 2));
  ASSERT_EQ(OB_SUCCESS, append(24, 2));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ObVecIdxSnapshotData &snap = snapshot_data();
  snap.set_inited();
  snap.has_complete_ = true;
  ASSERT_EQ(2, snap.meta_.bases_.count());
  lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
  roaring::api::roaring64_bitmap_t *bitmap = snap.meta_.bases_.at(0).vbitmap_->delete_bitmap_;
  const std::vector<std::vector<uint64_t>> cases = {
      {}, {1, 2}, {18}, {26}, {10, 12, 15, 20, 22, 25}, {11, 14, 21, 24}};
  for (const std::vector<uint64_t> &vids : cases) {
    while (!roaring::api::roaring64_bitmap_is_empty(bitmap)) {
      roaring::api::roaring64_bitmap_remove(bitmap, roaring::api::roaring64_bitmap_minimum(bitmap));
    }
    for (const uint64_t vid : vids) { roaring::api::roaring64_bitmap_add(bitmap, vid); }
    ASSERT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts());
    for (const ObVectorIndexSegmentMeta &meta : snap.meta_.bases_) {
      int64_t expected = 0;
      for (const uint64_t vid : vids) {
        bool exists = false;
        ASSERT_EQ(OB_SUCCESS, meta.segment_handle_->check_id_exist(vid, exists));
        expected += exists ? 1 : 0;
      }
      EXPECT_EQ(expected, meta.segment_handle_->get_deleted_vector_count());
    }
  }
}

TEST_F(TestVectorIndexMultiSegment, segment_delete_refresh_failure_preserves_all_counts)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ObVecIdxSnapshotData &snap = snapshot_data();
  snap.set_inited();
  snap.has_complete_ = true;
  ASSERT_EQ(2, snap.meta_.bases_.count());
  snap.meta_.bases_.at(0).segment_handle_->set_deleted_vector_count(2);
  snap.meta_.bases_.at(1).segment_handle_->set_deleted_vector_count(3);
  ObVectorIndexSegmentHandle saved = snap.meta_.bases_.at(1).segment_handle_;
  snap.meta_.bases_.at(1).segment_handle_.reset();
  EXPECT_EQ(OB_EAGAIN, adaptor_->refresh_segment_delete_counts());
  EXPECT_EQ(2, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(3, saved->get_deleted_vector_count());
  snap.meta_.bases_.at(1).segment_handle_ = saved;
  snap.has_complete_ = false;
  EXPECT_EQ(OB_EAGAIN, adaptor_->refresh_segment_delete_counts());
  EXPECT_EQ(2, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(3, snap.meta_.bases_.at(1).segment_handle_->get_deleted_vector_count());
  snap.has_complete_ = true;
  ASSERT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts());
  EXPECT_EQ(0, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(0, snap.meta_.bases_.at(1).segment_handle_->get_deleted_vector_count());
}

TEST_F(TestVectorIndexMultiSegment, segment_delete_refresh_rejects_ddl_cleanup)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ObVecIdxSnapshotData &snap = snapshot_data();
  ASSERT_TRUE(snap.is_inited());
  ASSERT_TRUE(snap.has_complete_);
  ASSERT_EQ(2, snap.meta_.bases_.count());
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_vbitmap_data(adaptor_->vbitmap_data_));

  ObVectorIndexSegmentHandle first = snap.meta_.bases_.at(0).segment_handle_;
  ObVectorIndexSegmentHandle last = snap.meta_.bases_.at(1).segment_handle_;
  first->set_deleted_vector_count(2);
  last->set_deleted_vector_count(3);
  const int64_t refs_before = last->get_ref();
  auto *tenant = MTL_CTX();
  std::atomic<int> result{OB_ERR_UNEXPECTED};
  std::thread worker;
  {
    // Block bitmap collection until DDL has cleared the captured metadata.
    TCWLockGuard bitmap_guard(adaptor_->vbitmap_data_->bitmap_rwlock_);
    worker = std::thread([&] {
      ObTenantSwitchGuard tenant_guard(tenant);
      result.store(adaptor_->refresh_segment_delete_counts());
    });
    const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000;
    while (last->get_ref() == refs_before
        && ObTimeUtility::current_time() < deadline) {
      ob_usleep(1000);
    }
    const bool source_captured = last->get_ref() > refs_before;
    EXPECT_TRUE(source_captured);
    if (source_captured) {
      EXPECT_EQ(OB_SUCCESS, adaptor_->renew_single_snap_index(true));
      EXPECT_EQ(0, snap.meta_.segment_count());
    }
  }
  worker.join();

  EXPECT_EQ(OB_EAGAIN, result.load());
  EXPECT_EQ(2, first->get_deleted_vector_count());
  EXPECT_EQ(3, last->get_deleted_vector_count());
}

TEST_F(TestVectorIndexMultiSegment, segment_delete_refresh_updates_captured_graphs)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ObVecIdxSnapshotData &snap = snapshot_data();
  ASSERT_TRUE(snap.is_inited());
  ASSERT_TRUE(snap.has_complete_);
  ASSERT_EQ(2, snap.meta_.bases_.count());
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_vbitmap_data(adaptor_->vbitmap_data_));

  ObVectorIndexSegmentHandle first = snap.meta_.bases_.at(0).segment_handle_;
  ObVectorIndexSegmentHandle last = snap.meta_.bases_.at(1).segment_handle_;
  ObVectorIndexSegmentHandle replacement;
  std::stringstream graph_data;
  ASSERT_EQ(OB_SUCCESS, first->fserialize(graph_data));
  ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(VIAT_HNSW, replacement));
  ASSERT_EQ(OB_SUCCESS, replacement->fdeserialize(graph_data));
  replacement->set_deleted_vector_count(3);
  {
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
    roaring::api::roaring64_bitmap_add(adaptor_->vbitmap_data_->bitmap_->delete_bitmap_, 1);
    roaring::api::roaring64_bitmap_add(adaptor_->vbitmap_data_->bitmap_->delete_bitmap_, 5);
    roaring::api::roaring64_bitmap_add(adaptor_->vbitmap_data_->bitmap_->delete_bitmap_, 6);
  }
  const int64_t refs_before = last->get_ref();
  auto *tenant = MTL_CTX();
  std::atomic<int> result{OB_ERR_UNEXPECTED};
  std::thread worker;
  {
    TCWLockGuard bitmap_guard(adaptor_->vbitmap_data_->bitmap_rwlock_);
    worker = std::thread([&] {
      ObTenantSwitchGuard tenant_guard(tenant);
      result.store(adaptor_->refresh_segment_delete_counts());
    });
    const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000;
    while (last->get_ref() == refs_before
        && ObTimeUtility::current_time() < deadline) {
      ob_usleep(1000);
    }
    const bool source_captured = last->get_ref() > refs_before;
    EXPECT_TRUE(source_captured);
    if (source_captured) {
      TCWLockGuard metadata_guard(snap.mem_data_rwlock_);
      // Keep counts and SCN unchanged: publication must follow graph identity.
      snap.meta_.bases_.at(0).segment_handle_ = replacement;
    }
  }
  worker.join();

  EXPECT_EQ(OB_SUCCESS, result.load());
  EXPECT_EQ(1, first->get_deleted_vector_count());
  EXPECT_EQ(3, replacement->get_deleted_vector_count());
  EXPECT_EQ(3, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(2, last->get_deleted_vector_count());
  EXPECT_EQ(2, snap.meta_.bases_.at(1).segment_handle_->get_deleted_vector_count());
}

TEST_F(TestVectorIndexMultiSegment, segment_delete_refresh_cancel_preserves_all_counts)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ObVecIdxSnapshotData &snap = snapshot_data();
  snap.set_inited();
  snap.has_complete_ = true;
  snap.meta_.bases_.at(0).segment_handle_->set_deleted_vector_count(2);
  snap.meta_.bases_.at(1).segment_handle_->set_deleted_vector_count(3);
  ObVecIndexAsyncTaskCtx ctx;
  ctx.task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL;
  EXPECT_EQ(OB_CANCELED, adaptor_->refresh_segment_delete_counts(&ctx));
  EXPECT_EQ(2, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(3, snap.meta_.bases_.at(1).segment_handle_->get_deleted_vector_count());

  ObVecIndexAsyncTaskCtx mem_sync_ctx;
  mem_sync_ctx.task_status_.task_type_ = OB_VECTOR_ASYNC_MEM_SYNC_TASK;
  const ObTabletID tablet_id(200002);
  ASSERT_EQ(OB_SUCCESS, mem_sync_ctx.task_status_.task_info_.batch_cancel_tablets_.push_back(tablet_id.id()));
  EXPECT_EQ(OB_CANCELED, adaptor_->refresh_segment_delete_counts(&mem_sync_ctx, tablet_id));
  EXPECT_EQ(2, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(3, snap.meta_.bases_.at(1).segment_handle_->get_deleted_vector_count());
  // Cancelling one tablet must not prevent another tablet from refreshing.
  EXPECT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts(&mem_sync_ctx, ObTabletID(200003)));
  EXPECT_EQ(0, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(0, snap.meta_.bases_.at(1).segment_handle_->get_deleted_vector_count());
}

TEST_F(TestVectorIndexMultiSegment, segment_delete_refresh_cancel_during_collection)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ObVecIdxSnapshotData &snap = snapshot_data();
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_vbitmap_data(adaptor_->vbitmap_data_));
  ObVectorIndexSegmentHandle first = snap.meta_.bases_.at(0).segment_handle_;
  ObVectorIndexSegmentHandle last = snap.meta_.bases_.at(1).segment_handle_;
  first->set_deleted_vector_count(2);
  last->set_deleted_vector_count(3);
  const int64_t refs_before = last->get_ref();
  ObVecIndexAsyncTaskCtx ctx;
  auto *tenant = MTL_CTX();
  std::atomic<int> result{OB_ERR_UNEXPECTED};
  std::thread worker;
  {
    TCWLockGuard bitmap_guard(adaptor_->vbitmap_data_->bitmap_rwlock_);
    worker = std::thread([&] {
      ObTenantSwitchGuard tenant_guard(tenant);
      result.store(adaptor_->refresh_segment_delete_counts(&ctx));
    });
    const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000;
    while (last->get_ref() == refs_before
        && ObTimeUtility::current_time() < deadline) {
      ob_usleep(1000);
    }
    EXPECT_GT(last->get_ref(), refs_before);
    common::ObSpinLockGuard ctx_guard(ctx.lock_);
    ctx.task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL;
  }
  worker.join();
  EXPECT_EQ(OB_CANCELED, result.load());
  EXPECT_EQ(2, first->get_deleted_vector_count());
  EXPECT_EQ(3, last->get_deleted_vector_count());
}

TEST_F(TestVectorIndexMultiSegment, mem_sync_preserves_delete_counts_when_reload_is_skipped)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ObVecIdxSnapshotData &snap = snapshot_data();
  snap.set_inited();
  snap.has_complete_ = true;
  adaptor_->set_ready_state(ADAPTOR_COMPLETE);
  adaptor_->set_reload_finish(true);
  const ObTabletID tablet_id(200002);
  ASSERT_EQ(OB_SUCCESS, adaptor_->set_tablet_id(VIRT_INC, tablet_id));
  {
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
    roaring::api::roaring64_bitmap_add(snap.meta_.bases_.at(0).vbitmap_->delete_bitmap_, 1);
  }
  ObPluginVectorIndexMgr mgr(context_, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, mgr.get_vec_adaptor_map().create(16, "TestVecMap", "TestVecMap", MTL_ID()));
  DEFER(mgr.get_vec_adaptor_map().destroy());
  ASSERT_EQ(OB_SUCCESS, mgr.get_vec_adaptor_map().set_refactored(tablet_id, adaptor_));
  mgr.set_ls_leader(true);
  // Guards borrow the fixture's adaptor; its allocation remains owned by TearDown.
  adaptor_->inc_ref();
  DEFER(adaptor_->dec_ref_and_check_release());
  ObVecIndexAsyncTaskCtx ctx;
  ctx.task_status_.task_type_ = OB_VECTOR_ASYNC_MEM_SYNC_TASK;
  ObVecMemSyncTask task;
  task.ctx_ = &ctx;
  int64_t estimate = 0;
  bool skipped = true;
  const int64_t sync_before = adaptor_->follower_sync_statistics_.sync_count_;
  ASSERT_EQ(OB_SUCCESS, task.process_one_tablet(&mgr, tablet_id, estimate, skipped));
  EXPECT_FALSE(skipped);
  EXPECT_TRUE(adaptor_->get_reload_finish());
  EXPECT_EQ(0, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(sync_before + 1, adaptor_->follower_sync_statistics_.sync_count_);
  // Skipping reload must not refresh statistics or fail on an incomplete snapshot.
  snap.has_complete_ = false;
  const int64_t failures_before = adaptor_->follower_sync_statistics_.sync_fail_;
  ASSERT_EQ(OB_SUCCESS, task.process_one_tablet(&mgr, tablet_id, estimate, skipped));
  EXPECT_EQ(0, snap.meta_.bases_.at(0).segment_handle_->get_deleted_vector_count());
  EXPECT_EQ(failures_before, adaptor_->follower_sync_statistics_.sync_fail_);
  EXPECT_EQ(sync_before + 2, adaptor_->follower_sync_statistics_.sync_count_);
}

TEST_F(TestVectorIndexMultiSegment, merge_wait_updates_snapshot_before_preparing_input)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  snapshot_data().set_inited();
  snapshot_data().has_complete_ = true;
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_vbitmap_data(adaptor_->vbitmap_data_));
  adaptor_->data_tablet_id_ = ObTabletID(storage::TestLobCommon::TEST_LOB_TABLE_ID);
  ObVecIndexAsyncTaskCtx ctx;
  ObVecIdxMergeTask task;
  task.ctx_ = &ctx;
  task.ls_id_ = ObLSID(storage::TestLobCommon::TEST_LOB_LS_ID);
  task.set_new_adapter(adaptor_);
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  ASSERT_EQ(OB_SUCCESS, txs->get_max_commit_version(ctx.task_status_.target_scn_));
  const SCN initial = ctx.task_status_.target_scn_;
  const SCN deletion_commit = SCN::scn_inc(initial);
  txs->tx_version_mgr_.update_max_commit_ts(deletion_commit, false);
  {
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
    roaring::api::roaring64_bitmap_add(adaptor_->vbitmap_data_->bitmap_->delete_bitmap_, 1);
    adaptor_->vbitmap_data_->scn_ = deletion_commit;
  }
  ASSERT_EQ(OB_SUCCESS, task.check_and_wait_write());
  EXPECT_GT(ctx.task_status_.target_scn_, initial);
  EXPECT_GE(ctx.task_status_.target_scn_, deletion_commit);
  ASSERT_EQ(OB_SUCCESS, adaptor_->refresh_segment_delete_counts());
  ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(
      task.merge_source_meta_, task.merge_policy_input_, task.merge_config_));
  EXPECT_EQ(1, task.merge_policy_input_.segments_.at(0).deleted_vector_count_);
}

TEST_F(TestVectorIndexMultiSegment, merge_policy_input_preserves_unready_and_empty_sources)
{
  init_adaptor();
  ObVectorIndexMeta source;
  ObVecMergePolicyInput input;
  ObVecMergeConfig config;
  ObVecMergePlan plan;
  EXPECT_EQ(OB_EAGAIN, adaptor_->prepare_merge_policy_input(source, input, config, true));
  snapshot_data().set_inited();
  ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(source, input, config, true));
  EXPECT_EQ(OB_NO_NEED_MERGE, ObVectorIndexMergePolicy(config).find_merge_segments(
      input, OB_VEC_TRIGGER_ALTER_INDEX_PARAM, plan));
  ObVectorIndexSegmentMeta unready;
  ASSERT_EQ(OB_SUCCESS, snapshot_data().meta_.bases_.push_back(unready));
  ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(source, input, config, true));
  ASSERT_EQ(1, input.segments_.count());
  EXPECT_FALSE(input.segments_.at(0).readable_);
  EXPECT_EQ(OB_EAGAIN, ObVectorIndexMergePolicy(config).find_merge_segments(
      input, OB_VEC_TRIGGER_ALTER_INDEX_PARAM, plan));
}

TEST_F(TestVectorIndexMultiSegment, capacity_window_uses_all_six_persisted_graphs)
{
  prepare_merge("HNSW", 12000, 2000);
  // A batch is consumed atomically; cutover happens between batches.
  for (int64_t first = 1; first <= 12000; first += 2000) {
    ASSERT_EQ(OB_SUCCESS, append(first, 2000));
  }
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  snapshot_data().set_inited();
  ASSERT_EQ(6, snapshot_data().meta_.bases_.count());
  for (const bool exact : {false, true}) {
    ObVectorIndexMeta source;
    ObVecMergePolicyInput input;
    ObVecMergeConfig config;
    ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(source, input, config));
    ASSERT_EQ(6, input.base_count_);
    ASSERT_EQ(6, input.segments_.count());
    for (int64_t i = 0; i < input.segments_.count(); ++i) {
      EXPECT_TRUE(input.segments_.at(i).readable_);
      EXPECT_EQ(2000, input.segments_.at(i).vector_count_);
      EXPECT_EQ(0, input.segments_.at(i).deleted_vector_count_);
      EXPECT_EQ(snapshot_data().meta_.bases_.at(i).start_key_, source.bases_.at(i).start_key_);
    }
    config.max_vectors_ = 12000;
    config.merge_factor_ = 2;
    ObVectorIndexMergePolicy policy(config);
    ObVecMergePlan plan;
    if (exact) {
      ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, OB_VEC_TRIGGER_AUTO, plan));
    } else {
      bool need_task = false;
      ASSERT_EQ(OB_SUCCESS, policy.check_need_merge_task(input, need_task, plan));
      ASSERT_TRUE(need_task);
    }
    EXPECT_EQ(ObVecMergePlan::TARGET_MERGE, plan.strategy_);
    EXPECT_EQ(0, plan.begin_);
    EXPECT_EQ(6, plan.end_);
    EXPECT_TRUE(plan.output_base_);
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, plan.get_vector_count(input, count));
    EXPECT_EQ(12000, count);
  }
}

TEST_F(TestVectorIndexMultiSegment, target_merge_above_M_builds_one_graph_and_reloads)
{
  prepare_merge("HNSW", 1200, 200);
  ASSERT_EQ(OB_SUCCESS, append(1, 800));
  ASSERT_EQ(OB_SUCCESS, append(801, 200));
  ASSERT_EQ(OB_SUCCESS, append(1001, 200));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  snapshot_data().set_inited();
  ASSERT_EQ(3, snapshot_data().meta_.bases_.count());
  ObVecIdxMergeTask task;
  task.tenant_id_ = MTL_ID();
  ObVecMergePolicyInput input;
  ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(
      task.merge_source_meta_, input, task.merge_config_));
  EXPECT_EQ(800, input.segments_.at(0).live_vector_count());
  EXPECT_EQ(200, input.segments_.at(1).live_vector_count());
  EXPECT_EQ(200, input.segments_.at(2).live_vector_count());
  task.merge_config_.max_vectors_ = 1000;
  task.merge_config_.merge_factor_ = 2;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(task.merge_config_).find_merge_segments(
      input, OB_VEC_TRIGGER_AUTO, task.merge_plan_));
  ASSERT_EQ(ObVecMergePlan::TARGET_MERGE, task.merge_plan_.strategy_);
  ASSERT_EQ(0, task.merge_plan_.begin_);
  ASSERT_EQ(3, task.merge_plan_.end_);
  ASSERT_TRUE(task.merge_plan_.output_base_);
  task.merge_from_base_ = task.merge_plan_.output_base_;
  task.min_vid_ = 1;
  task.max_vid_ = 1200;
  for (int64_t i = task.merge_plan_.begin_; i < task.merge_plan_.end_; ++i) {
    ASSERT_EQ(OB_SUCCESS, task.merge_segments_.push_back(&task.merge_source_meta_.bases_.at(i)));
  }
  snapshot_data().free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  ASSERT_EQ(OB_SUCCESS, task.merge_bitmap(adaptor_));
  ASSERT_FALSE(snapshot_data().enable_multi_segment_);
  ASSERT_EQ(OB_SUCCESS, append(1, 800));
  ASSERT_EQ(OB_SUCCESS, append(801, 200));
  ASSERT_EQ(OB_SUCCESS, append(1001, 200));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  verify_membership(1, 1200);
  verify_merge_bitmaps(1200);
  const auto &meta = snapshot_data().meta_.bases_.at(0);
  EXPECT_EQ(ObVectorIndexSegmentType::BASE, meta.seg_type_);
  std::stringstream stream;
  ASSERT_EQ(OB_SUCCESS, meta.segment_handle_->fserialize(stream));
  ObVectorIndexSegmentHandle loaded;
  ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(meta.index_type_, loaded));
  ASSERT_EQ(OB_SUCCESS, loaded->fdeserialize(stream));
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, loaded->get_index_number(count));
  EXPECT_EQ(1200, count);
  for (int64_t vid = 1; vid <= 1200; ++vid) {
    bool exists = false;
    ASSERT_EQ(OB_SUCCESS, loaded->check_id_exist(vid, exists));
    ASSERT_TRUE(exists) << vid;
  }
}

TEST_F(TestVectorIndexMultiSegment, merge_selected_sources_preserve_alter_memory_admission)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  snapshot_data().set_inited();
  ObVecIndexAsyncTaskCtx ctx;
  ObVecIdxMergeTask task;
  task.ctx_ = &ctx;
  ctx.task_status_.task_type_ = OB_VECTOR_ASYNC_INDEX_MERGE;
  ctx.task_status_.trigger_type_ = OB_VEC_TRIGGER_ALTER_INDEX_PARAM;
  auto &info = ctx.task_status_.task_info_;
  info.alter_param_info_.parent_task_id_ = 1;
  info.alter_param_info_.idx_parallel_build_ = 1;
  ASSERT_EQ(OB_SUCCESS, info.alter_param_info_.index_params_.assign("TYPE=HNSW"));
  ASSERT_TRUE(task.is_alter_param_child());
  ASSERT_EQ(OB_SUCCESS, ctx.task_status_.target_scn_.convert_for_inner_table_field(100));
  ObVecMergePolicyInput input;
  ASSERT_EQ(OB_SUCCESS, adaptor_->prepare_merge_policy_input(
      task.merge_source_meta_, input, task.merge_config_, true));
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(task.merge_config_).find_merge_segments(
      input, OB_VEC_TRIGGER_ALTER_INDEX_PARAM, task.merge_plan_));
  ASSERT_EQ(ObVecMergePlan::FULL_MERGE, task.merge_plan_.strategy_);
  for (auto &meta : task.merge_source_meta_.bases_) { meta.scn_ = 100; }
  info.task_estimate_memory_ = 123456;
  ASSERT_EQ(OB_SUCCESS, task.prepare_merge_segment());
  EXPECT_EQ(2, task.merge_segments_.count());
  EXPECT_TRUE(task.merge_from_base_);
  EXPECT_EQ(123456, info.task_estimate_memory_);
  EXPECT_EQ(0, task.pre_alloc_size_);
  task.merge_source_meta_.bases_.at(1).scn_ = 101;
  EXPECT_EQ(OB_EAGAIN, task.prepare_merge_segment());
  EXPECT_EQ(123456, info.task_estimate_memory_);
  EXPECT_EQ(0, task.pre_alloc_size_);
  task.merge_source_meta_.bases_.at(1).scn_ = 100;
  ctx.task_status_.trigger_type_ = OB_VEC_TRIGGER_AUTO;
  // FULL_MERGE alone must not bypass ordinary memory admission. This ALTER
  // fixture lacks the task identity and row statistics required by the ordinary
  // estimator, so switching to AUTO must reach that estimator and be rejected.
  const int ret = task.prepare_merge_segment();
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  EXPECT_EQ(0, task.pre_alloc_size_);
  EXPECT_EQ(123456, info.task_estimate_memory_);
}

TEST_F(TestVectorIndexMultiSegment, merge_multi_output_filters_replay_and_nonmembers)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(1, 4)); // Replay sealed output.
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, append(13, 8)); // Range hit but not a selected source member.
  ASSERT_EQ(OB_SUCCESS, append(9, 2));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(3, snapshot_data().meta_.bases_.count());
  verify_membership(1, 10);
  verify_merge_bitmaps(10);
}

TEST_F(TestVectorIndexMultiSegment, full_merge_outputs_omit_bitmaps_and_preserve_source)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(true, source);
  ASSERT_EQ(nullptr, snapshot_data().builder_->ibitmap_);
  ASSERT_EQ(nullptr, snapshot_data().builder_->vbitmap_);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(1, 4)); // Retry must still exclude sealed VIDs.
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, append(13, 8)); // Not a member of any source graph.
  ASSERT_EQ(OB_SUCCESS, append(9, 2)); // Snapshot omitted deleted VIDs 11 and 12.
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(3, snapshot_data().meta_.bases_.count());
  verify_membership(1, 10);
  for (const auto &meta : snapshot_data().meta_.bases_) {
    EXPECT_FALSE(meta.has_segment_meta_row_);
    EXPECT_EQ(0, meta.bitmap_scn_);
    EXPECT_EQ(nullptr, meta.segment_handle_->ibitmap_);
    EXPECT_EQ(nullptr, meta.vbitmap_);
    std::stringstream stream;
    ASSERT_EQ(OB_SUCCESS, meta.segment_handle_->fserialize(stream));
    ObVectorIndexSegmentHandle reloaded;
    ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(meta.index_type_, reloaded));
    ASSERT_EQ(OB_SUCCESS, reloaded->fdeserialize(stream));
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, reloaded->get_index_number(count));
    EXPECT_GT(count, 0);
  }
  // The bridge and queries holding the old snapshot still own the original bitmap.
  ASSERT_NE(nullptr, source.vbitmap_);
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(source.vbitmap_->delete_bitmap_, 42000));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(source.vbitmap_->delete_bitmap_, 11));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(source.vbitmap_->insert_bitmap_, 12));
}

TEST_F(TestVectorIndexMultiSegment, partial_base_merge_preserves_residual_insert_and_source)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(false, source);
  // Build only 1..10 while retaining source visibility for 1..12. This fixture
  // stops at serialization; it does not run publication-time bitmap reclamation.
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, append(9, 2));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_EQ(true, snapshot_data().insert_bitmap_pruned_);
  ASSERT_EQ(3, snapshot_data().meta_.bases_.count());
  verify_membership(1, 10);
  verify_merge_bitmaps(10, 2);
  const auto *merged_bitmap = snapshot_data().meta_.bases_.at(0).vbitmap_;
  ASSERT_NE(nullptr, merged_bitmap);
  ASSERT_NE(nullptr, source.vbitmap_);
  EXPECT_NE(source.vbitmap_, merged_bitmap);
  for (uint64_t vid = 1; vid <= 12; ++vid) {
    EXPECT_EQ(vid > 10, roaring::api::roaring64_bitmap_contains(merged_bitmap->insert_bitmap_, vid));
    EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(source.vbitmap_->insert_bitmap_, vid));
  }
  EXPECT_EQ(12, roaring::api::roaring64_bitmap_get_cardinality(source.vbitmap_->insert_bitmap_));
  EXPECT_EQ(1, roaring::api::roaring64_bitmap_get_cardinality(source.vbitmap_->delete_bitmap_));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(source.vbitmap_->delete_bitmap_, 42000));
}

TEST_F(TestVectorIndexMultiSegment, merge_prunes_insert_single_output)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(false, source, "HNSW", false);
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_EQ(true, snapshot_data().insert_bitmap_pruned_);
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  verify_merge_bitmaps(10, 2);
  const auto &meta = snapshot_data().meta_.bases_.at(0);
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(meta.vbitmap_->insert_bitmap_, 11));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(meta.vbitmap_->insert_bitmap_, 12));
  EXPECT_EQ(12, roaring::api::roaring64_bitmap_get_cardinality(source.vbitmap_->insert_bitmap_));
}

// Compare real query dispatch on the same serialized graph. No active/frozen
// visibility supplies the VIDs already covered by this persisted merge output.
TEST_F(TestVectorIndexMultiSegment, async_query_preserves_incr_insert_visibility)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(false, source, "HNSW", false);
  snapshot_data().builder_->seg_type_ = ObVectorIndexSegmentType::INCR_MERGE;
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_EQ(false, snapshot_data().insert_bitmap_pruned_);
  auto &snap = snapshot_data();
  ASSERT_EQ(1, snap.meta_.bases_.count());
  ObVectorIndexSegmentMeta merged = snap.meta_.bases_.at(0);
  ASSERT_EQ(ObVectorIndexSegmentType::INCR_MERGE, merged.seg_type_);
  ASSERT_EQ(12, roaring::api::roaring64_bitmap_get_cardinality(merged.vbitmap_->insert_bitmap_));
  // Build completion stages the output in bases_; publication places an
  // INCR_MERGE output in incrs_ according to the selected merge plan.
  ASSERT_EQ(OB_SUCCESS, snap.meta_.incrs_.push_back(merged));
  snap.meta_.bases_.reuse();
  snap.set_inited();
  SCN published_scn;
  ASSERT_EQ(OB_SUCCESS, published_scn.convert_for_inner_table_field(merged.scn_ + 1));
  ASSERT_EQ(OB_SUCCESS, adaptor_->advance_async_query_scn(published_scn));

  auto query = [&](const bool async, const char *scenario) {
    SCOPED_TRACE(scenario);
    ObArenaAllocator allocator("TestPruneAsync");
    ObVectorQueryAdaptorResultContext ctx(MTL_ID(), ObLSID(1), 0, &allocator, &allocator);
    ctx.set_async_query(async);
    ctx.set_scn(published_scn);
    SCN barrier;
    ASSERT_EQ(OB_SUCCESS, adaptor_->check_query_barrier_scn(
        ObLSID(1), published_scn, async, barrier));
    ASSERT_EQ(OB_SUCCESS, ctx.init_bitmaps());
    ObVectorQueryConditions condition;
    condition.ef_search_ = 64;
    condition.query_limit_ = 10;
    float vector[8] = {};
    ObVectorQueryVidIterator iterator(0, 0, 0, nullptr);
    ObVectorQueryVidIterator *iter = &iterator;
    ASSERT_EQ(OB_SUCCESS, adaptor_->vsag_query_vids(&ctx, &condition, 8, vector, iter));
    const int64_t visible_inserts = roaring::api::roaring64_bitmap_get_cardinality(ctx.bitmaps_->insert_bitmap_);
    printf("[ PRUNE_QUERY ] %s: insert_count=%ld result_count=%ld\n",
        scenario, visible_inserts, iterator.get_total());
    EXPECT_EQ(10, iterator.get_total());
    for (int64_t vid = 1; vid <= 10; ++vid) {
      bool found = false;
      for (int64_t i = 0; i < iterator.get_total(); ++i) {
        found = found || iterator.get_vids()[i] == vid;
      }
      EXPECT_TRUE(found) << "missing VID " << vid;
    }
  };

  query(false, "sync INCR_MERGE with preserved visibility");
  query(true, "async INCR_MERGE with preserved visibility");
}

TEST_F(TestVectorIndexMultiSegment, async_query_reads_pruned_incr_base)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(false, source, "HNSW", false);
  snapshot_data().builder_->seg_type_ = ObVectorIndexSegmentType::INCR_BASE;
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_EQ(true, snapshot_data().insert_bitmap_pruned_);
  auto &snap = snapshot_data();
  ASSERT_EQ(1, snap.meta_.bases_.count());
  ObVectorIndexSegmentMeta merged = snap.meta_.bases_.at(0);
  ASSERT_EQ(ObVectorIndexSegmentType::INCR_BASE, merged.seg_type_);
  verify_merge_bitmaps(10, 2, ObVectorIndexSegmentType::INCR_BASE);
  ASSERT_EQ(OB_SUCCESS, snap.meta_.incrs_.push_back(merged));
  snap.meta_.bases_.reuse();
  snap.set_inited();
  EXPECT_EQ(merged.segment_handle_->get_index_type(), snap.get_snap_index_type());
  SCN published_scn;
  SCN older_scn;
  SCN barrier;
  ASSERT_EQ(OB_SUCCESS, published_scn.convert_for_inner_table_field(merged.scn_ + 1));
  ASSERT_EQ(OB_SUCCESS, older_scn.convert_for_inner_table_field(merged.scn_));
  ASSERT_EQ(OB_SUCCESS, adaptor_->advance_async_query_scn(published_scn));
  EXPECT_EQ(OB_SCHEMA_EAGAIN, adaptor_->check_query_barrier_scn(ObLSID(1), older_scn, true, barrier));
  for (const bool async : {false, true}) {
    ObArenaAllocator allocator("TestIncrBase");
    ObVectorQueryAdaptorResultContext ctx(MTL_ID(), ObLSID(1), 0, &allocator, &allocator);
    ctx.set_async_query(async);
    ctx.set_scn(published_scn);
    ASSERT_EQ(OB_SUCCESS, adaptor_->check_query_barrier_scn(ObLSID(1), published_scn, async, barrier));
    ASSERT_EQ(OB_SUCCESS, ctx.init_bitmaps());
    ObVectorQueryConditions condition;
    condition.ef_search_ = 64;
    condition.query_limit_ = 10;
    float vector[8] = {};
    ObVectorQueryVidIterator iterator(0, 0, 0, nullptr);
    ObVectorQueryVidIterator *iter = &iterator;
    ASSERT_EQ(OB_SUCCESS, adaptor_->vsag_query_vids(&ctx, &condition, 8, vector, iter));
    EXPECT_EQ(10, iterator.get_total());
    EXPECT_EQ(2, roaring::api::roaring64_bitmap_get_cardinality(ctx.bitmaps_->insert_bitmap_));
  }
  EXPECT_EQ(12, roaring::api::roaring64_bitmap_get_cardinality(source.vbitmap_->insert_bitmap_));
}

// An INCR_BASE alone must not invalidate readers: only a real removal does.
TEST_F(TestVectorIndexMultiSegment, incr_base_disjoint_insert_bitmap_does_not_advance_watermark)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(false, source, "HNSW", false);
  auto &builder = *snapshot_data().builder_;
  builder.seg_type_ = ObVectorIndexSegmentType::INCR_BASE;
  roaring::api::roaring64_bitmap_remove_range(builder.vbitmap_->insert_bitmap_, 1, 13);
  roaring::api::roaring64_bitmap_add(builder.vbitmap_->insert_bitmap_, 12);
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_FALSE(snapshot_data().insert_bitmap_pruned_);
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(
      snapshot_data().meta_.bases_.at(0).vbitmap_->insert_bitmap_, 12));
}

TEST_F(TestVectorIndexMultiSegment, incr_base_empty_insert_bitmap_does_not_advance_watermark)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(false, source, "HNSW", false);
  auto &builder = *snapshot_data().builder_;
  builder.seg_type_ = ObVectorIndexSegmentType::INCR_BASE;
  roaring::api::roaring64_bitmap_remove_range(builder.vbitmap_->insert_bitmap_, 1, 13);
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_FALSE(snapshot_data().insert_bitmap_pruned_);
}

TEST_F(TestVectorIndexMultiSegment, insert_prune_event_survives_retry_and_builder_cleanup)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(false, source, "HNSW", false);
  auto &builder = *snapshot_data().builder_;
  builder.seg_type_ = ObVectorIndexSegmentType::INCR_BASE;
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  ASSERT_EQ(OB_SUCCESS, builder.prune_insert_bitmap(builder));
  ASSERT_TRUE(builder.insert_bitmap_pruned_);
  ASSERT_EQ(2, roaring::api::roaring64_bitmap_get_cardinality(builder.vbitmap_->insert_bitmap_));
  // Serialization prunes again, now with no intersection. The event must survive.
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_TRUE(snapshot_data().insert_bitmap_pruned_);
  snapshot_data().free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  EXPECT_FALSE(snapshot_data().insert_bitmap_pruned_);
}

TEST_F(TestVectorIndexMultiSegment, full_merge_single_output_omits_bitmaps)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(true, source, "HNSW", false);
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_EQ(false, snapshot_data().insert_bitmap_pruned_);
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  const auto &meta = snapshot_data().meta_.bases_.at(0);
  EXPECT_FALSE(meta.has_segment_meta_row_);
  EXPECT_EQ(0, meta.bitmap_scn_);
  EXPECT_EQ(nullptr, meta.segment_handle_->ibitmap_);
  EXPECT_EQ(nullptr, meta.vbitmap_);
  verify_membership(1, 10);
}

TEST_F(TestVectorIndexMultiSegment, full_merge_empty_output_omits_bitmaps)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(true, source);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_EQ(false, snapshot_data().insert_bitmap_pruned_);
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  const auto &meta = snapshot_data().meta_.bases_.at(0);
  EXPECT_FALSE(meta.has_segment_meta_row_);
  EXPECT_EQ(nullptr, meta.segment_handle_->ibitmap_);
  EXPECT_EQ(nullptr, meta.vbitmap_);
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, meta.segment_handle_->get_index_number(count));
  EXPECT_EQ(0, count);
}

TEST_F(TestVectorIndexMultiSegment, full_merge_quantized_outputs_omit_bitmaps)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(true, source, "HNSW_SQ");
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, append(9, 2));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(3, snapshot_data().meta_.bases_.count());
  verify_membership(1, 10);
  for (const auto &meta : snapshot_data().meta_.bases_) {
    EXPECT_FALSE(meta.has_segment_meta_row_);
    EXPECT_EQ(nullptr, meta.segment_handle_->ibitmap_);
    EXPECT_EQ(nullptr, meta.vbitmap_);
  }
}

TEST_F(TestVectorIndexMultiSegment, merge_hnsw_sq_empty)
{
  prepare_merge("HNSW_SQ");
  // No live input: retain the tombstone carrier.
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  verify_merge_bitmaps(0);
  for (const auto &meta : snapshot_data().meta_.bases_) {
    EXPECT_EQ(VIAT_HGRAPH, meta.index_type_);
    EXPECT_EQ(adaptor_->index_param_, meta.index_param_);
  }
}

TEST_F(TestVectorIndexMultiSegment, merge_hnsw_sq_exact_capacity)
{
  prepare_merge("HNSW_SQ");
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, append(1, 8));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(2, snapshot_data().meta_.bases_.count());
  verify_merge_bitmaps(8);
  for (const auto &meta : snapshot_data().meta_.bases_) {
    EXPECT_EQ(VIAT_HGRAPH, meta.index_type_);
    EXPECT_EQ(adaptor_->index_param_, meta.index_param_);
  }
}

TEST_F(TestVectorIndexMultiSegment, merge_hnsw_bq_empty)
{
  prepare_merge("HNSW_BQ");
  // No live input: retain the tombstone carrier.
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  verify_merge_bitmaps(0);
  for (const auto &meta : snapshot_data().meta_.bases_) {
    EXPECT_EQ(VIAT_HGRAPH, meta.index_type_);
    EXPECT_EQ(adaptor_->index_param_, meta.index_param_);
  }
}

TEST_F(TestVectorIndexMultiSegment, merge_hnsw_bq_exact_capacity)
{
  prepare_merge("HNSW_BQ");
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, append(1, 8));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(2, snapshot_data().meta_.bases_.count());
  verify_merge_bitmaps(8);
  for (const auto &meta : snapshot_data().meta_.bases_) {
    EXPECT_EQ(VIAT_HGRAPH, meta.index_type_);
    EXPECT_EQ(adaptor_->index_param_, meta.index_param_);
  }
}

TEST_F(TestVectorIndexMultiSegment, merge_trained_SQ_and_fallback_tail)
{
  prepare_merge("HNSW_SQ", 10004, 10001);
  ASSERT_EQ(OB_SUCCESS, append(1, 10001));
  ASSERT_EQ(OB_SUCCESS, append(1, 64));
  ASSERT_EQ(OB_SUCCESS, append(10002, 3));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(2, snapshot_data().meta_.bases_.count());
  EXPECT_EQ(VIAT_HNSW_SQ, snapshot_data().meta_.bases_.at(0).index_type_);
  EXPECT_EQ(VIAT_HGRAPH, snapshot_data().meta_.bases_.at(1).index_type_);
  verify_membership(1, 10004);
  verify_merge_bitmaps(10004);
}

TEST_F(TestVectorIndexMultiSegment, merge_trained_BQ_and_fallback_tail)
{
  prepare_merge("HNSW_BQ", 10004, 10001);
  ASSERT_EQ(OB_SUCCESS, append(1, 10001));
  ASSERT_EQ(OB_SUCCESS, append(1, 64));
  ASSERT_EQ(OB_SUCCESS, append(10002, 3));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(2, snapshot_data().meta_.bases_.count());
  EXPECT_EQ(VIAT_HNSW_BQ, snapshot_data().meta_.bases_.at(0).index_type_);
  EXPECT_EQ(VIAT_HGRAPH, snapshot_data().meta_.bases_.at(1).index_type_);
  verify_membership(1, 10004);
  verify_merge_bitmaps(10004);
}

TEST_F(TestVectorIndexMultiSegment, merge_multi_parallel_bitmap_membership)
{
  prepare_merge();
  auto *tenant = MTL_CTX();
  std::atomic<int> error{OB_SUCCESS};
  std::thread writers[3];
  for (int i = 0; i < 3; ++i) {
    writers[i] = std::thread([&, i] {
      ObTenantSwitchGuard guard(tenant);
      for (int j = 0; j < 4; ++j) {
        const int ret = append(i * 4 + j + 1, 1);
        if (ret != OB_SUCCESS) { error.store(ret); }
      }
    });
  }
  for (auto &writer : writers) { writer.join(); }
  ASSERT_EQ(OB_SUCCESS, error.load());
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  verify_membership(1, 12);
  verify_merge_bitmaps(12);
}

TEST_F(TestVectorIndexMultiSegment, merge_skip_precedes_multi_segment_dispatch)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 2));
  // Exercise entry policy independently of today's merge multi-segment opt-out.
  adaptor_->set_created_by_segment_merge(true);
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  param.vctx_ = &ctx;
  param.allocator_ = &context_->get_malloc_allocator();
  EXPECT_EQ(OB_NOT_INIT, adaptor_->serialize_snapshot(param));
  EXPECT_TRUE(ctx.vals_.empty());
  EXPECT_TRUE(snapshot_data().sealed_builders_.empty());
  ASSERT_NE(nullptr, snapshot_data().builder_);
  EXPECT_FALSE(snapshot_data().has_complete_);
}

TEST_F(TestVectorIndexMultiSegment, single_merge_serialization_preserves_incr_type)
{
  init_adaptor();
  adaptor_->set_created_by_segment_merge(true);
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_snap_data_for_build(true));
  auto &builder = *snapshot_data().builder_;
  builder.seg_type_ = ObVectorIndexSegmentType::INCR_MERGE;
  ASSERT_EQ(OB_SUCCESS, builder.ibitmap_->init(true, false));
  ASSERT_EQ(OB_SUCCESS, builder.vbitmap_->init(true, true));
  ASSERT_EQ(OB_SUCCESS, append(1, 2));
  ASSERT_FALSE(snapshot_data().enable_multi_segment_);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  const auto &meta = snapshot_data().meta_.bases_.at(0);
  EXPECT_EQ(ObVectorIndexSegmentType::INCR_MERGE, meta.seg_type_);
  EXPECT_TRUE(meta.has_segment_meta_row_);
  EXPECT_EQ(meta.scn_, meta.bitmap_scn_);
  EXPECT_FALSE(snapshot_data().meta_.is_persistent_);
  EXPECT_TRUE(meta.segment_handle_->is_base());
  // Single-segment completion keeps an empty builder shell; metadata owns the graph.
  ASSERT_NE(nullptr, snapshot_data().builder_);
  EXPECT_TRUE(snapshot_data().builder_->is_build_finished());
  EXPECT_FALSE(snapshot_data().builder_->segment_handle_.is_valid());
  verify_membership(1, 2);
}

TEST_F(TestVectorIndexMultiSegment, single_empty_merge_preserves_bitmap_segment)
{
  init_adaptor();
  adaptor_->set_created_by_segment_merge(true);
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_snap_data_for_build(true));
  auto &builder = *snapshot_data().builder_;
  builder.seg_type_ = ObVectorIndexSegmentType::INCR_MERGE;
  ASSERT_EQ(OB_SUCCESS, builder.ibitmap_->init(true, false));
  ASSERT_EQ(OB_SUCCESS, builder.vbitmap_->init(true, true));
  {
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "TestVecDdl"));
    roaring::api::roaring64_bitmap_add(builder.vbitmap_->delete_bitmap_, 42);
  }
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  const auto &meta = snapshot_data().meta_.bases_.at(0);
  EXPECT_EQ(ObVectorIndexSegmentType::EMPTY, meta.seg_type_);
  EXPECT_TRUE(meta.has_segment_meta_row_);
  EXPECT_EQ(meta.scn_, meta.bitmap_scn_);
  ASSERT_NE(nullptr, meta.vbitmap_);
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(
      meta.vbitmap_->delete_bitmap_, 42));
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, meta.segment_handle_->get_index_number(count));
  EXPECT_EQ(0, count);
}

TEST_F(TestVectorIndexMultiSegment, invalid_tablet_is_rejected_before_builder_initialization)
{
  init_adaptor();
  adaptor_->get_snap_tablet_id() = ObTabletID();
  float vectors[8] = {};
  int64_t vid = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, adaptor_->add_snap_index(vectors, &vid, nullptr, 0, 1));
  EXPECT_EQ(nullptr, snapshot_data().builder_);
  EXPECT_FALSE(snapshot_data().is_inited());
  EXPECT_EQ(0, used_);
}

TEST_F(TestVectorIndexMultiSegment, excluded_build_keeps_legacy_empty_input_initialization)
{
  init_adaptor();
  adaptor_->set_created_by_tablet_rebuild(true);
  ASSERT_EQ(OB_SUCCESS, adaptor_->add_snap_index(nullptr, nullptr, nullptr, 0, 0, nullptr, true));
  EXPECT_FALSE(snapshot_data().enable_multi_segment_);
  ASSERT_NE(nullptr, snapshot_data().builder_);
  EXPECT_TRUE(snapshot_data().builder_->is_inited());
  EXPECT_TRUE(snapshot_data().is_inited());
  EXPECT_TRUE(snapshot_data().sealed_builders_.empty());
}

TEST_F(TestVectorIndexMultiSegment, exact_threshold_discards_empty_tail)
{
  prepare(10);
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  auto &snap = snapshot_data();
  ASSERT_NE(nullptr, snap.builder_);
  ASSERT_TRUE(snap.builder_->segment_handle_.is_valid());
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, snap.builder_->segment_handle_->get_index_number(count));
  EXPECT_EQ(0, count);
  ASSERT_EQ(1, snap.sealed_builders_.count());
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(1, snap.sealed_builders_.count());
  verify_membership(1, 10);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  EXPECT_EQ(nullptr, snap.builder_);
  ASSERT_EQ(1, snap.meta_.bases_.count());
  verify_membership(1, 10);
}

TEST_F(TestVectorIndexMultiSegment, oversized_batch_is_never_replayed)
{
  prepare(10);
  ASSERT_EQ(OB_SUCCESS, append(1, 7));
  ASSERT_EQ(OB_SUCCESS, append(8, 17));
  ASSERT_EQ(OB_SUCCESS, append(25, 3));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  auto &snap = snapshot_data();
  ASSERT_EQ(1, snap.sealed_builders_.count());
  ASSERT_NE(nullptr, snap.builder_);
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, snap.sealed_builders_.at(0)->segment_handle_->get_index_number(count));
  EXPECT_EQ(24, count);
  verify_membership(1, 27);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ASSERT_EQ(2, snap.meta_.bases_.count());
  verify_membership(1, 27);
}

TEST_F(TestVectorIndexMultiSegment, parallel_cutover_more_than_ten_segments)
{
  prepare(9);
  std::atomic<int> first_error{OB_SUCCESS};
  auto *tenant = MTL_CTX();
  std::thread writers[4];
  for (int t = 0; t < 4; ++t) {
    writers[t] = std::thread([&, t] {
      ObTenantSwitchGuard guard(tenant);
      for (int i = 0; i < 32; ++i) {
        const int ret = append(t * 96 + i * 3 + 1, 3);
        if (ret != OB_SUCCESS) {
          first_error.store(ret);
          break;
        }
      }
    });
  }
  for (auto &writer : writers) {
    writer.join();
  }
  ASSERT_EQ(OB_SUCCESS, first_error.load());
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_GT(adaptor_->get_snap_data()->sealed_builders_.count(), 10);
  verify_membership(1, 384);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ASSERT_GT(snapshot_data().meta_.bases_.count(), 10);
  verify_membership(1, 384);
}

TEST_F(TestVectorIndexMultiSegment, duplicate_vectors_count_distinct_vids)
{
  prepare(4, "HNSW", true);
  ASSERT_EQ(OB_SUCCESS, append(1, 4, true));
  ASSERT_EQ(OB_SUCCESS, append(5, 4, true));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(2, adaptor_->get_snap_data()->sealed_builders_.count());
  verify_membership(1, 8);
}

TEST_F(TestVectorIndexMultiSegment, quantized_small_segments_fall_back)
{
  prepare(10, "HNSW_SQ");
  ASSERT_EQ(OB_SUCCESS, append(1, 10));
  ASSERT_EQ(OB_SUCCESS, append(11, 3));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  auto &snap = snapshot_data();
  ASSERT_EQ(1, snap.sealed_builders_.count());
  ASSERT_NE(nullptr, snap.builder_);
  ASSERT_TRUE(snap.builder_->segment_handle_.is_valid());
  EXPECT_EQ(VIAT_HGRAPH, snap.builder_->segment_handle_->get_index_type());
  for (auto *builder : snap.sealed_builders_) {
    EXPECT_EQ(VIAT_HGRAPH, builder->segment_handle_->get_index_type());
  }
  verify_membership(1, 13);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ASSERT_EQ(2, snap.meta_.bases_.count());
  for (auto &base : snap.meta_.bases_) {
    EXPECT_EQ(VIAT_HGRAPH, base.index_type_);
  }
  verify_membership(1, 13);
}

TEST_F(TestVectorIndexMultiSegment, bq_small_tail_falls_back)
{
  prepare(4, "HNSW_BQ");
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 2));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(1, adaptor_->get_snap_data()->sealed_builders_.count());
  ASSERT_NE(nullptr, snapshot_data().builder_);
  ASSERT_TRUE(snapshot_data().builder_->segment_handle_.is_valid());
  EXPECT_EQ(VIAT_HGRAPH, snapshot_data().builder_->segment_handle_->get_index_type());
  for (auto *builder : adaptor_->get_snap_data()->sealed_builders_) {
    EXPECT_EQ(VIAT_HGRAPH, builder->segment_handle_->get_index_type());
  }
  verify_membership(1, 6);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ASSERT_EQ(2, snapshot_data().meta_.bases_.count());
  for (auto &base : snapshot_data().meta_.bases_) {
    EXPECT_EQ(VIAT_HGRAPH, base.index_type_);
  }
  verify_membership(1, 6);
}

TEST_F(TestVectorIndexMultiSegment, sq_exact_threshold_discards_empty_tail)
{
  verify_quantized_exact_threshold("HNSW_SQ");
}

TEST_F(TestVectorIndexMultiSegment, bq_exact_threshold_discards_empty_tail)
{
  verify_quantized_exact_threshold("HNSW_BQ");
}

TEST_F(TestVectorIndexMultiSegment, sq_trained_segment_and_hgraph_tail_reload)
{
  verify_quantized_mixed("HNSW_SQ", VIAT_HNSW_SQ);
}

TEST_F(TestVectorIndexMultiSegment, bq_trained_segment_and_hgraph_tail_reload)
{
  verify_quantized_mixed("HNSW_BQ", VIAT_HNSW_BQ);
}

TEST_F(TestVectorIndexMultiSegment, caller_without_ddl_opt_in_keeps_single_builder)
{
  prepare(4);
  auto &snap = snapshot_data();
  snap.enable_multi_segment_ = false;
  float vectors[8 * 10] = {};
  int64_t vids[10];
  for (int i = 0; i < 10; ++i) {
    vids[i] = i + 1;
  }
  ASSERT_EQ(OB_SUCCESS, adaptor_->add_snap_index(vectors, vids, nullptr, 0, 10));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  EXPECT_TRUE(snap.sealed_builders_.empty());
  ASSERT_NE(nullptr, snap.builder_);
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, snap.builder_->segment_handle_->get_index_number(count));
  EXPECT_EQ(10, count);
}

TEST_F(TestVectorIndexMultiSegment, invalid_input_returns_error_and_cleanup_releases_all_graphs)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 2));
  auto &snap = snapshot_data();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      adaptor_->add_snap_index(nullptr, nullptr, nullptr, 0, 1, nullptr, true));
  ASSERT_EQ(1, snap.sealed_builders_.count());
  ASSERT_NE(nullptr, snap.builder_);
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, snap.builder_->segment_handle_->get_index_number(count));
  EXPECT_EQ(2, count);
  EXPECT_TRUE(snap.meta_.bases_.empty());
  EXPECT_FALSE(snap.has_complete_);
  EXPECT_GT(used_, 0);
  snap.free_memdata_resource(&context_->get_malloc_allocator(), MTL_ID());
  EXPECT_EQ(nullptr, snap.builder_);
  EXPECT_TRUE(snap.sealed_builders_.empty());
  EXPECT_EQ(0, used_);
}

TEST_F(TestVectorIndexMultiSegment, final_small_tail_is_sealed)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 3));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_NE(nullptr, snapshot_data().builder_);
  EXPECT_TRUE(snapshot_data().sealed_builders_.empty());
  verify_membership(1, 3);
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  EXPECT_EQ(nullptr, snapshot_data().builder_);
  EXPECT_TRUE(snapshot_data().sealed_builders_.empty());
  ASSERT_EQ(1, snapshot_data().meta_.bases_.count());
  verify_membership(1, 3);
}

TEST_F(TestVectorIndexMultiSegment, bitmap_view_reuses_graph_without_mutating_old_queries)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 3));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ObVectorIndexMeta old_meta;
  ASSERT_EQ(OB_SUCCESS, old_meta.assign(snapshot_data().meta_));
  ASSERT_EQ(1, old_meta.segment_count());
  const auto &old_segment = old_meta.bases_.at(0);
  EXPECT_EQ(old_segment.scn_, old_segment.bitmap_scn_);
  const int64_t new_version = old_segment.bitmap_scn_ + 1;
  const ObVectorIndexRoaringBitMap *old_bitmap = nullptr;
  old_bitmap = old_segment.vbitmap_;
  ASSERT_NE(nullptr, old_bitmap);
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(old_bitmap->delete_bitmap_, 42000));
  ObVectorIndexSegmentMeta changed;
  ASSERT_EQ(OB_SUCCESS, changed.assign(old_segment));
  changed.segment_handle_.reset();
  changed.bitmap_scn_ = new_version;
  ASSERT_EQ(OB_SUCCESS, ObPluginVectorIndexUtils::find_segment_in_old_meta(changed, old_meta));
  EXPECT_EQ(old_segment.segment_handle_.get(), changed.segment_handle_.get());
  EXPECT_EQ(old_segment.bitmap_scn_, changed.bitmap_scn_); // Reused bitmap keeps its version.
  const ObVectorIndexRoaringBitMap *visible = nullptr;
  visible = changed.vbitmap_;
  EXPECT_EQ(old_bitmap, visible);
  auto &allocator = context_->get_malloc_allocator();
  const int64_t size = old_segment.segment_handle_->get_serialize_meta_size(old_segment.vbitmap_);
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_segment.segment_handle_->serialize_meta(buf, size, pos, old_segment.vbitmap_));
  ObVectorIndexSegmentMeta loaded;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexSegment::create_bitmap_only(loaded.segment_handle_, MTL_ID(), allocator));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, loaded.segment_handle_->deserialize_meta(buf, size, pos, loaded.vbitmap_));
  changed.set_vbitmap(loaded.vbitmap_);
  allocator.free(buf);
  {
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
    roaring::api::roaring64_bitmap_remove(changed.vbitmap_->delete_bitmap_, 42000);
  }
  changed.bitmap_scn_ = new_version; // Set the new version only after loading the replacement.
  visible = changed.vbitmap_;
  EXPECT_FALSE(roaring::api::roaring64_bitmap_contains(visible->delete_bitmap_, 42000));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(old_bitmap->delete_bitmap_, 42000));
  ObVectorIndexMeta current;
  ASSERT_EQ(OB_SUCCESS, current.add_base_seg_meta(changed));
  ObVectorIndexSegmentMeta reused;
  ASSERT_EQ(OB_SUCCESS, reused.assign(changed));
  reused.segment_handle_.reset();
  reused.set_vbitmap(nullptr);
  ASSERT_EQ(OB_SUCCESS, ObPluginVectorIndexUtils::find_segment_in_old_meta(reused, current));
  EXPECT_EQ(new_version, reused.bitmap_scn_);
  EXPECT_EQ(changed.vbitmap_, reused.vbitmap_);
  reused.bitmap_scn_ = new_version + 1;
  ASSERT_EQ(OB_SUCCESS, ObPluginVectorIndexUtils::find_segment_in_old_meta(reused, current));
  EXPECT_EQ(new_version, reused.bitmap_scn_); // Requested version differs: caller must reload.
  EXPECT_EQ(changed.vbitmap_, reused.vbitmap_);
  reused.release();
  EXPECT_EQ(nullptr, reused.vbitmap_);
  EXPECT_EQ(0, reused.bitmap_scn_);
}

TEST_F(TestVectorIndexMultiSegment, merge_candidates_follow_actual_graph_membership)
{
  prepare_merge("HNSW_SQ");
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, append(9, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(3, snapshot_data().meta_.segment_count());
  {
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
    auto *deleted = snapshot_data().meta_.bases_.at(2).vbitmap_->delete_bitmap_;
    roaring::api::roaring64_bitmap_add(deleted, 2);
    roaring::api::roaring64_bitmap_add(deleted, 6);
    roaring::api::roaring64_bitmap_add(deleted, 10);
  }
  ObVecIdxMergeTask task;
  task.tenant_id_ = MTL_ID();
  task.old_adapter_ = adaptor_;
  ASSERT_EQ(OB_SUCCESS, task.merge_source_meta_.assign(snapshot_data().meta_));
  task.merge_plan_.begin_ = 0;
  task.merge_plan_.end_ = 2;
  task.merge_plan_.output_base_ = true;
  ASSERT_EQ(OB_SUCCESS, task.collect_merge_deleted_vids());
  ASSERT_TRUE(task.merge_deleted_vids_valid_);
  EXPECT_EQ(2, roaring::api::roaring64_bitmap_get_cardinality(task.merge_deleted_vids_.delete_bitmap_));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(task.merge_deleted_vids_.delete_bitmap_, 2));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(task.merge_deleted_vids_.delete_bitmap_, 6));
  // A retained graph containing the same VID protects it from reclamation.
  ASSERT_EQ(OB_SUCCESS, task.merge_source_meta_.add_base_seg_meta(snapshot_data().meta_.bases_.at(0)));
  ASSERT_EQ(OB_SUCCESS, task.collect_merge_deleted_vids());
  EXPECT_EQ(1, roaring::api::roaring64_bitmap_get_cardinality(task.merge_deleted_vids_.delete_bitmap_));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(task.merge_deleted_vids_.delete_bitmap_, 6));
  // Selecting all source graphs still collects candidates; output membership
  // and the presence of bitmap rows decide the subsequent cleanup.
  task.merge_plan_.end_ = task.merge_source_meta_.segment_count();
  ASSERT_EQ(OB_SUCCESS, task.collect_merge_deleted_vids());
  EXPECT_TRUE(task.merge_deleted_vids_valid_);
  EXPECT_EQ(3, roaring::api::roaring64_bitmap_get_cardinality(task.merge_deleted_vids_.delete_bitmap_));
}

TEST_F(TestVectorIndexMultiSegment, reclaimed_vids_exclude_every_serialized_output)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, append(9, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(3, snapshot_data().meta_.segment_count());
  ObVecIdxMergeTask task;
  task.new_adapter_ = adaptor_;
  task.tenant_id_ = MTL_ID();
  task.merge_deleted_vids_valid_ = true;
  ASSERT_EQ(OB_SUCCESS, task.merge_deleted_vids_.init(false, true));
  {
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
    for (uint64_t vid = 1; vid <= 13; ++vid) {
      roaring::api::roaring64_bitmap_add(task.merge_deleted_vids_.delete_bitmap_, vid);
    }
  }
  ASSERT_EQ(OB_SUCCESS, task.filter_reclaimed_vids());
  EXPECT_TRUE(task.merge_deleted_vids_valid_);
  EXPECT_EQ(1, roaring::api::roaring64_bitmap_get_cardinality(task.merge_deleted_vids_.delete_bitmap_));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(task.merge_deleted_vids_.delete_bitmap_, 13));
  auto &last = snapshot_data().meta_.bases_.at(2).segment_handle_;
  auto *saved = last->ibitmap_;
  last->ibitmap_ = nullptr;
  EXPECT_EQ(OB_SUCCESS, task.filter_reclaimed_vids());
  EXPECT_FALSE(task.merge_deleted_vids_valid_);
  last->ibitmap_ = saved;
  task.new_adapter_ = nullptr;
}

TEST_F(TestVectorIndexMultiSegment, migrated_bitmap_preserves_version_and_completion)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 3));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  snapshot_data().meta_.bases_.at(0).bitmap_scn_ = 100;
  snapshot_data().meta_.last_bitmap_update_scn_ = 100;
  ObVectorIndexMeta root;
  ASSERT_EQ(OB_SUCCESS, root.assign(snapshot_data().meta_));
  ObVectorIndexSegmentMeta source;
  ASSERT_EQ(OB_SUCCESS, storage::ObVectorIndexMigrationUtil::resolve_vec_idx_segment_for_migration(
      adaptor_, 0, source));
  ASSERT_EQ(100, source.bitmap_scn_);
  ASSERT_EQ(snapshot_data().meta_.bases_.at(0).vbitmap_, source.vbitmap_);
  ObVectorIndexSegmentMeta received = source;
  received.set_vbitmap(nullptr);
  received.bitmap_scn_ = 100;
  auto &allocator = context_->get_malloc_allocator();
  const int64_t size = source.segment_handle_->get_serialize_meta_size(source.vbitmap_);
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.segment_handle_->serialize_meta(buf, size, pos, source.vbitmap_));
  ObVectorIndexSegmentHandle parsed;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexSegment::create_bitmap_only(parsed, MTL_ID(), allocator));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, parsed->deserialize_meta(buf, size, pos, received.vbitmap_));
  allocator.free(buf);
  ASSERT_NE(source.vbitmap_, received.vbitmap_);
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(received.vbitmap_->delete_bitmap_, 42000));
  auto handle = source.segment_handle_;
  ASSERT_EQ(OB_SUCCESS, adaptor_->clear_migrated_segments());
  adaptor_->set_ready_state(ADAPTOR_COMPLETE);
  ASSERT_EQ(OB_SUCCESS, adaptor_->install_migrated_segment_handle(0, received, handle, root, true, true));
  EXPECT_TRUE(adaptor_->get_snap_has_complete());
  EXPECT_EQ(100, snapshot_data().meta_.bases_.at(0).bitmap_scn_);
  EXPECT_EQ(100, snapshot_data().meta_.last_bitmap_update_scn_);
  EXPECT_EQ(received.vbitmap_, snapshot_data().meta_.bases_.at(0).vbitmap_);
  EXPECT_EQ(handle.get(), snapshot_data().meta_.bases_.at(0).segment_handle_.get());
  EXPECT_EQ(root.max_base_scn(), adaptor_->get_bounded_read_base_scn());
}

TEST_F(TestVectorIndexMultiSegment, migration_stream_preserves_per_segment_bitmaps)
{
  using namespace storage;
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  ASSERT_EQ(2, snapshot_data().meta_.bases_.count());
  for (int64_t i = 0; i < 2; ++i) {
    auto &meta = snapshot_data().meta_.bases_.at(i);
    ASSERT_NE(nullptr, meta.vbitmap_);
    lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
    roaring::api::roaring64_bitmap_remove(meta.vbitmap_->delete_bitmap_, 42000);
    for (uint64_t vid = i * 4 + 1; vid <= i * 4 + 4; ++vid) {
      roaring::api::roaring64_bitmap_add(meta.vbitmap_->insert_bitmap_, vid);
    }
    roaring::api::roaring64_bitmap_add(meta.vbitmap_->delete_bitmap_, i * 4 + 4);
    meta.bitmap_scn_ = 100 + i;
  }
  snapshot_data().meta_.last_bitmap_update_scn_ = 101;

  // Root metadata must arrive over the RPC format, without source runtime pointers.
  obrpc::ObFetchVectorIndexSegmentMetasRes sent_root;
  sent_root.adaptor_handle_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, sent_root.vec_index_meta_.assign(snapshot_data().meta_));
  ASSERT_EQ(OB_SUCCESS, sent_root.vec_index_right_boundary_scn_.convert_for_tx(200));
  std::vector<char> root_buf(sent_root.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, sent_root.serialize(root_buf.data(), root_buf.size(), pos));
  obrpc::ObFetchVectorIndexSegmentMetasRes received_root;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, received_root.deserialize(root_buf.data(), pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  EXPECT_EQ(sent_root.vec_index_right_boundary_scn_, received_root.vec_index_right_boundary_scn_);
  ASSERT_EQ(101, received_root.vec_index_meta_.last_bitmap_update_scn_);

  ObPluginVectorIndexAdaptor dest(&context_->get_malloc_allocator(), context_, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, dest.init(adaptor_->get_index_param(), 8, context_, &used_));
  ASSERT_EQ(OB_SUCCESS, dest.init_snap_data_for_build(false));
  ASSERT_EQ(OB_SUCCESS, dest.clear_migrated_segments());
  dest.set_ready_state(ADAPTOR_COMPLETE);
  ObVectorIndexAdaptorMigrationDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.set_vec_index_meta(received_root.vec_index_meta_));
  ObLSCompleteMigrationCtx ctx;
  ctx.tenant_id_ = MTL_ID();
  ctx.arg_.ls_id_ = ObLSID(TestLobCommon::TEST_LOB_LS_ID);
  const int64_t slot_size = ObMigrationTenantWindowMgr::MIGRATION_WINDOW_DEFAULT_SLOT_BUF_SIZE;
  ObMigrationTenantWindowMgr window_mgr;
  ASSERT_EQ(OB_SUCCESS, window_mgr.init(MTL_ID(), 8, slot_size, 8));

  // The fixture owns adaptor_ via new/delete; keep guard teardown from releasing it.
  adaptor_->inc_ref();
  DEFER(adaptor_->dec_ref_and_check_release());
  for (int64_t i = 0; i < 2; ++i) {
    SCOPED_TRACE(i);
    const auto &expected = snapshot_data().meta_.bases_.at(i);
    ObMigrationVectorIndexProcessor source;
    ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowSourceController::create(
        &window_mgr, nullptr, ObDagPrio::DAG_PRIO_VECTOR_INDEX, source.src_controller_handle_));
    source.is_inited_ = true;
    source.processor_id_ = i + 1;
    source.tenant_id_ = MTL_ID();
    source.ls_id_ = ctx.arg_.ls_id_;
    source.data_tablet_id_ = ObTabletID(TestLobCommon::TEST_LOB_TABLE_ID);
    source.segment_idx_ = i;
    ASSERT_EQ(OB_SUCCESS, source.adaptor_guard_.set_adapter(adaptor_));

    // Replace only RPC delivery. Preserve the production META/VSAG chunk stream.
    struct LocalTransport : ObIMigrationWindowSlidCallback
    {
      ObMigrationVectorIndexProcessor &source_;
      ObVecIdxSegFetchDriver *driver_;
      std::vector<char> buf_;
      LocalTransport(ObMigrationVectorIndexProcessor &source, int64_t size)
        : source_(source), driver_(nullptr), buf_(size) {}
      void on_window_slid(int64_t start, int64_t count) override
      {
        for (int64_t seq = start; seq < start + count; ++seq) {
          int ret = OB_EAGAIN;
          int64_t len = 0;
          const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000;
          while (OB_EAGAIN == (ret = source_.try_get_data(seq, buf_.data(), buf_.size(), len))) {
            if (ObTimeUtility::current_time() >= deadline) {
              ret = OB_TIMEOUT;
              break;
            }
            ob_usleep(1000);
          }
          if (OB_ITER_END == ret) {
            driver_->notify_src_exhausted(seq);
            break;
          } else if (OB_SUCCESS == ret) {
            ret = driver_->fill_data(seq, buf_.data(), len);
          }
          if (OB_SUCCESS != ret) {
            driver_->abort_segment_fetch(ret);
            break;
          }
        }
      }
    } transport(source, slot_size);
    auto *driver = OB_NEW(ObVecIdxSegFetchDriver, "MigVecIdxDrv");
    ASSERT_NE(nullptr, driver);
    ObVecIdxSegFetchDriverHandle fetch(driver);
    transport.driver_ = driver;
    ASSERT_EQ(OB_SUCCESS, ObMigrationSlidingWindowDestController::create(
        &window_mgr, &transport, ObDagPrio::DAG_PRIO_VECTOR_INDEX, driver->dest_controller_handle_));
    driver->is_inited_ = true;
    DEFER(driver->stop());

    std::atomic<int> send_ret{OB_SUCCESS};
    auto *tenant = MTL_CTX();
    std::thread producer([&] {
      ObTenantSwitchGuard guard(tenant);
      send_ret.store(source.do_serialize());
    });
    DEFER(if (producer.joinable()) {
      source.src_controller_handle_->stop();
      source.src_controller_handle_->wakeup_waiters();
      producer.join();
    });
    int64_t head = 0;
    int64_t window_size = 0;
    ASSERT_EQ(OB_SUCCESS, driver->dest_controller_handle_->get_runtime_snapshot(head, window_size));
    transport.on_window_slid(head, window_size);

    ObVecIndexSegmentCopyTask copy;
    copy.set_dag(dag);
    copy.ctx_ = &ctx;
    copy.segment_idx_ = i;
    copy.processor_id_ = i + 1;
    ObVectorIndexSegmentMeta received;
    ASSERT_EQ(OB_SUCCESS, received.assign(received_root.vec_index_meta_.bases_.at(i)));
    ASSERT_FALSE(received.segment_handle_.is_valid());
    ASSERT_EQ(nullptr, received.vbitmap_);
    ObVectorIndexSegmentHandle graph;
    ASSERT_EQ(OB_SUCCESS, copy.validate_and_create_dest_segment_(&dest, graph));
    ObArenaAllocator alloc("VIMigTest");
    ASSERT_EQ(OB_SUCCESS, copy.read_and_deserialize_meta_(fetch, slot_size, alloc, graph, received.vbitmap_));
    ASSERT_EQ(OB_SUCCESS, copy.stream_deserialize_vsag_(fetch, slot_size, alloc, graph, i));
    producer.join();
    ASSERT_EQ(OB_SUCCESS, send_ret.load());
    ASSERT_EQ(OB_SUCCESS, graph->immutable_optimize());
    ASSERT_EQ(OB_SUCCESS, dest.install_migrated_segment_handle(
        i, received, graph, received_root.vec_index_meta_, true, i == 1));

    const auto &actual = dest.get_snap_data()->meta_.bases_.at(i);
    ASSERT_NE(expected.segment_handle_.get(), actual.segment_handle_.get());
    ASSERT_NE(nullptr, actual.vbitmap_);
    EXPECT_NE(expected.vbitmap_, actual.vbitmap_);
    EXPECT_EQ(expected.bitmap_scn_, actual.bitmap_scn_);
    ASSERT_NE(nullptr, actual.vbitmap_->insert_bitmap_);
    ASSERT_NE(nullptr, actual.vbitmap_->delete_bitmap_);
    EXPECT_EQ(0, roaring::api::roaring64_bitmap_xor_cardinality(
        expected.vbitmap_->insert_bitmap_, actual.vbitmap_->insert_bitmap_));
    EXPECT_EQ(0, roaring::api::roaring64_bitmap_xor_cardinality(
        expected.vbitmap_->delete_bitmap_, actual.vbitmap_->delete_bitmap_));
    ASSERT_NE(nullptr, actual.segment_handle_->ibitmap_);
    ASSERT_NE(nullptr, actual.segment_handle_->ibitmap_->insert_bitmap_);
    EXPECT_EQ(0, roaring::api::roaring64_bitmap_xor_cardinality(
        expected.segment_handle_->ibitmap_->insert_bitmap_, actual.segment_handle_->ibitmap_->insert_bitmap_));
    int64_t graph_count = 0;
    ASSERT_EQ(OB_SUCCESS, actual.segment_handle_->get_index_number(graph_count));
    EXPECT_EQ(4, graph_count);
    for (int64_t vid = 1; vid <= 8; ++vid) {
      bool exists = false;
      ASSERT_EQ(OB_SUCCESS, actual.segment_handle_->check_id_exist(vid, exists));
      EXPECT_EQ(vid > i * 4 && vid <= i * 4 + 4, exists);
    }
    EXPECT_EQ(i == 1, dest.get_snap_has_complete());
  }
  EXPECT_EQ(101, dest.get_snap_data()->meta_.last_bitmap_update_scn_);
  EXPECT_EQ(sent_root.vec_index_meta_.max_base_scn(), dest.get_bounded_read_base_scn());
}

TEST_F(TestVectorIndexMultiSegment, bitmap_versions_roundtrip_and_old_array_boundaries)
{
  ObArenaAllocator allocator;
  ObVectorIndexSegmentMeta segment;
  ObVectorIndexRoaringBitMap bitmap(&allocator, MTL_ID());
  DEFER(bitmap.reset_bitmap());
  ASSERT_EQ(OB_SUCCESS, bitmap.init(true, true));
  ObSEArray<ObVecIdxSnapshotBlockData, 1> blocks;
  ASSERT_EQ(OB_SUCCESS, blocks.push_back(ObVecIdxSnapshotBlockData(true, ObString("bitmap"))));
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexSegmentMeta::prepare_new_segment_meta(
      allocator, segment, ObVectorIndexSegmentType::BASE, VIAT_HNSW,
      ObString("param"), ObTabletID(200001), 42, blocks, &bitmap));
  EXPECT_EQ(42, segment.bitmap_scn_);
  segment.bitmap_scn_ = 99;
  ObVectorIndexMeta root;
  root.set_persistent();
  root.last_bitmap_update_scn_ = 99;
  ASSERT_EQ(OB_SUCCESS, root.add_base_seg_meta(segment));
  segment.bitmap_scn_ = 0;
  ASSERT_EQ(OB_SUCCESS, root.add_incr_seg_meta(segment));
  ObString blob;
  ASSERT_EQ(OB_SUCCESS, root.serialize(allocator, blob));
  ObVectorIndexMeta restored;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, restored.deserialize(blob.ptr() + sizeof(ObLobCommon),
      blob.length() - sizeof(ObLobCommon), pos));
  EXPECT_EQ(99, restored.last_bitmap_update_scn_);
  EXPECT_EQ(99, restored.bases_.at(0).bitmap_scn_);
  EXPECT_EQ(0, restored.incrs_.at(0).bitmap_scn_);
  EXPECT_EQ(nullptr, restored.bases_.at(0).vbitmap_);
  ObVectorIndexMeta old_root;
  pos = 0;
  EXPECT_NE(OB_SUCCESS, old_root.deserialize(blob.ptr() + sizeof(ObLobCommon),
      blob.length() - sizeof(ObLobCommon) - serialization::encoded_length_vi64(99), pos));
  old_root.release(); // Discard the partial result of the truncated decode.
  root.last_bitmap_update_scn_ = 0;
  ASSERT_EQ(OB_SUCCESS, root.serialize(allocator, blob));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_root.deserialize(blob.ptr() + sizeof(ObLobCommon),
      blob.length() - sizeof(ObLobCommon), pos));
  EXPECT_EQ(0, old_root.last_bitmap_update_scn_);
  EXPECT_EQ(2, old_root.segment_count());
  // Make the first array element's payload match the old format (no bitmap_scn).
  char buf[4096];
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, segment.serialize(buf, sizeof(buf), pos));
  int64_t header_pos = 0, version = 0, payload_len = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf, pos, header_pos, &version));
  const int64_t length_pos = header_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf, pos, header_pos, &payload_len));
  const int64_t old_length = pos - serialization::encoded_length_vi64(segment.bitmap_scn_);
  int64_t length_write_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(buf + length_pos,
      header_pos - length_pos, length_write_pos, payload_len - (pos - old_length)));
  pos = old_length;
  segment.bitmap_scn_ = 1234;
  ASSERT_EQ(OB_SUCCESS, segment.serialize(buf, sizeof(buf), pos));
  ObVectorIndexSegmentMeta old_element, new_element;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_element.deserialize(buf, pos, read_pos));
  EXPECT_EQ(old_length, read_pos);
  EXPECT_EQ(0, old_element.bitmap_scn_);
  ASSERT_EQ(OB_SUCCESS, new_element.deserialize(buf, pos, read_pos));
  EXPECT_EQ(pos, read_pos);
  EXPECT_EQ(1234, new_element.bitmap_scn_);
  root.release();
  EXPECT_EQ(0, root.last_bitmap_update_scn_);
}

TEST_F(TestVectorIndexMultiSegment, root_metadata_preserves_following_rpc_scn)
{
  char buf[4096];
  int64_t pos = 0;
  ObVectorIndexMeta root;
  root.set_persistent();
  root.header_.scn_ = 42;
  root.header_.version_ = ObVectorIndexMetaHeader::VERSION_V1;
  SCN boundary;
  ASSERT_EQ(OB_SUCCESS, boundary.convert_for_tx(123456));

  // Legacy wire format: header, flags, empty Base/Incr arrays, then RPC SCN.
  MEMCPY(buf, &root.header_, ObVectorIndexMetaHeader::HEAD_LEN);
  pos = ObVectorIndexMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(buf, sizeof(buf), pos, root.flags_));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(buf, sizeof(buf), pos, 0));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(buf, sizeof(buf), pos, 0));
  const int64_t root_end = pos;
  ASSERT_EQ(OB_SUCCESS, boundary.serialize(buf, sizeof(buf), pos));
  ObVectorIndexMeta decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
  EXPECT_EQ(root_end, read_pos);
  EXPECT_EQ(0, decoded.last_bitmap_update_scn_);
  SCN decoded_boundary;
  ASSERT_EQ(OB_SUCCESS, decoded_boundary.deserialize(buf, pos, read_pos));
  EXPECT_EQ(boundary, decoded_boundary);
  EXPECT_EQ(pos, read_pos);

  // The current-format writer must preserve the following RPC field as well.
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, root.serialize(buf, sizeof(buf), pos));
  const int64_t current_root_end = pos;
  EXPECT_EQ(root.get_serialize_size(), pos);
  ASSERT_EQ(OB_SUCCESS, boundary.serialize(buf, sizeof(buf), pos));
  read_pos = 0;
  ObVectorIndexMeta current;
  ASSERT_EQ(OB_SUCCESS, current.deserialize(buf, pos, read_pos));
  EXPECT_EQ(current_root_end, read_pos);
  ASSERT_EQ(OB_SUCCESS, decoded_boundary.deserialize(buf, pos, read_pos));
  EXPECT_EQ(boundary, decoded_boundary);
  EXPECT_EQ(pos, read_pos);
}

TEST_F(TestVectorIndexMultiSegment, segment_metadata_and_graph_reload)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 3));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  auto &snap = snapshot_data();
  ObVectorIndexMeta meta;
  ASSERT_EQ(1, snap.sealed_builders_.count());
  ASSERT_NE(nullptr, snap.builder_);
  {
    TCWLockGuard guard(snap.complete_lock_);
    ASSERT_EQ(OB_SUCCESS, adaptor_->seal_snap_builder());
  }
  ASSERT_EQ(2, snap.sealed_builders_.count());
  for (int i = 0; i < 2; ++i) {
    auto &builder = *snap.sealed_builders_.at(i);
    ASSERT_EQ(OB_SUCCESS, snap.append_base_meta(meta, builder, ObTabletID(200001),
        123456789, i * 2, 2, adaptor_->index_param_));
    std::stringstream stream;
    ASSERT_EQ(OB_SUCCESS, builder.segment_handle_->fserialize(stream));
    ObVectorIndexSegmentHandle reloaded;
    ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(
        builder.segment_handle_->get_index_type(), reloaded));
    ASSERT_EQ(OB_SUCCESS, reloaded->fdeserialize(stream));
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, reloaded->get_index_number(count));
    EXPECT_EQ(i == 0 ? 4 : 3, count);
    for (int64_t vid = i == 0 ? 1 : 5; vid <= (i == 0 ? 4 : 7); ++vid) {
      bool exists = false;
      ASSERT_EQ(OB_SUCCESS, reloaded->check_id_exist(vid, exists));
      EXPECT_TRUE(exists);
    }
  }
  ASSERT_LT(meta.bases_.at(0).start_key_.compare(meta.bases_.at(0).end_key_), 0);
  ASSERT_LT(meta.bases_.at(0).end_key_.compare(meta.bases_.at(1).start_key_), 0);
  meta.is_persistent_ = true;
  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, meta.serialize(context_->get_malloc_allocator(), serialized));
  ObVectorIndexMeta reloaded_meta;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, reloaded_meta.deserialize(serialized.ptr() + sizeof(ObLobCommon),
      serialized.length() - sizeof(ObLobCommon), pos));
  EXPECT_EQ(2, reloaded_meta.bases_.count());
  ASSERT_EQ(OB_SUCCESS, snap.meta_.assign(meta));
  ASSERT_EQ(OB_SUCCESS, snap.build_finished(context_->get_malloc_allocator()));
  EXPECT_TRUE(snap.has_complete_);
  EXPECT_EQ(7, snap.last_res_seg_info_.vector_cnt_);
  EXPECT_EQ(1, snap.last_res_seg_info_.min_vid_);
  EXPECT_EQ(7, snap.last_res_seg_info_.max_vid_);
  EXPECT_TRUE(snap.sealed_builders_.empty());
}

TEST_F(TestVectorIndexMultiSegment, reloaded_metadata_outlives_sql_and_is_freed_by_snapshot)
{
  ObVectorIndexMeta disk_meta;
  disk_meta.header_.scn_ = 123;
  disk_meta.is_persistent_ = true;
  for (int64_t i = 0; i < 2; ++i) {
    ObVectorIndexSegmentMeta base;
    base.seg_type_ = ObVectorIndexSegmentType::BASE;
    base.index_type_ = VIAT_HNSW;
    base.scn_ = 123;
    base.blocks_cnt_ = 1;
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexSegmentMeta::get_segment_persist_key(
        VIAT_HNSW, ObTabletID(200001), 123, i, base.start_key_buf_,
        sizeof(base.start_key_buf_), base.start_key_));
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexSegmentMeta::get_segment_persist_key(
        VIAT_HNSW, ObTabletID(200001), 123, i, base.end_key_buf_,
        sizeof(base.end_key_buf_), base.end_key_));
    ASSERT_EQ(OB_SUCCESS, disk_meta.add_base_seg_meta(base));
  }
  ObArenaAllocator allocator;
  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, disk_meta.serialize(allocator, serialized));
  struct MetadataAllocationCounter : public lib::ObMallocCallback
  {
    void operator()(const lib::ObMemAttr &attr, int64_t used, const lib::AObject &object) override
    {
      if (attr.label_ == lib::ObLabel("VIBases")) {
        used_ += used;
        if (used > 0) {
          EXPECT_EQ(MTL_ID(), attr.tenant_id_);
          EXPECT_EQ(0, attr.ctx_id_);
          EXPECT_EQ(expect_ignore_version_, static_cast<bool>(object.ignore_version_));
        }
      }
    }
    int64_t used_ = 0;
    bool expect_ignore_version_ = false;
  } counter;
  lib::ObMallocCallbackGuard allocation_guard(counter);
  ObString bytes(serialized.length() - sizeof(ObLobCommon),
      serialized.ptr() + sizeof(ObLobCommon));
  char *invalid_data = static_cast<char *>(allocator.alloc(bytes.length() + 1));
  ASSERT_NE(nullptr, invalid_data);
  MEMCPY(invalid_data, bytes.ptr(), bytes.length());
  ASSERT_EQ(0, invalid_data[bytes.length() - 1]);
  int64_t invalid_pos = bytes.length() - 1;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      invalid_data, bytes.length() + 1, invalid_pos, 128));
  ASSERT_EQ(bytes.length() + 1, invalid_pos);
  {
    ObMemVersionNodeGuard version;
    ObSqlMemoryLeakGuard sql_scope(true);
    EXPECT_FALSE(lib::ObMemVersionNode::tl_ignore_node);
    ObVectorIndexMeta temporary_meta;
    EXPECT_FALSE(temporary_meta.bases_.block_allocator_.get_attr().ignore_version());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, temporary_meta.deserialize(bytes.ptr(), bytes.length(), pos));
    EXPECT_GT(counter.used_, 0);
    temporary_meta.release();
    EXPECT_EQ(0, counter.used_);
    init_adaptor();
    EXPECT_FALSE(snapshot_data().meta_.incrs_.block_allocator_.get_attr().ignore_version());
  }
  counter.expect_ignore_version_ = true;
  // Exercise successful reload, partial deserialization failure, then reuse after cleanup.
  for (int64_t i = 0; i < 3; ++i) {
    SCOPED_TRACE(i);
    {
      ObMemVersionNodeGuard version;
      ObSqlMemoryLeakGuard sql_scope(true);
      EXPECT_FALSE(lib::ObMemVersionNode::tl_ignore_node);
      // Truncate a multi-byte Incr count after both Bases; omitting the entire field is allowed.
      ObString input(bytes.length(), i == 1 ? invalid_data : bytes.ptr());
      const int ret = adaptor_->deserialize_snap_meta(input);
      if (i == 1) {
        ASSERT_EQ(OB_DESERIALIZE_ERROR, ret);
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ASSERT_EQ(2, snapshot_data().meta_.bases_.count());
      EXPECT_GT(counter.used_, 0);
    }
    // SQL completion keeps the cache alive; existing snapshot cleanup frees its allocation.
    EXPECT_EQ(2, snapshot_data().meta_.bases_.count());
    EXPECT_GT(counter.used_, 0);
    snapshot_data().free_memdata_resource(adaptor_->get_allocator(), MTL_ID());
    EXPECT_EQ(0, counter.used_);
  }
}

TEST_F(TestVectorIndexMultiSegment, sealed_builders_use_tenant_memory_and_release_after_failure)
{
  struct BuilderArrayAllocationCounter : public lib::ObMallocCallback
  {
    void operator()(const lib::ObMemAttr &attr, int64_t used, const lib::AObject &object) override
    {
      if (attr.label_ == lib::ObLabel("VIBuilders")) {
        used_ += used;
        if (used > 0) {
          EXPECT_EQ(MTL_ID(), attr.tenant_id_);
          EXPECT_EQ(0, attr.ctx_id_);
          EXPECT_TRUE(object.ignore_version_);
        }
      }
    }
    int64_t used_ = 0;
  } counter;
  lib::ObMallocCallbackGuard allocation_guard(counter);
  prepare(4);
  for (int64_t first = 1; first <= 20; first += 4) {
    ASSERT_EQ(OB_SUCCESS, append(first, 4));
  }
  auto &snap = snapshot_data();
  ASSERT_EQ(5, snap.sealed_builders_.count());
  // The fifth builder exceeds the inline capacity and must allocate tenant memory.
  EXPECT_GT(counter.used_, 0);
  ObArenaAllocator allocator;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  param.vctx_ = &ctx;
  param.allocator_ = &allocator;
  // Missing transaction/snapshot retains the sealed builders for a later retry.
  EXPECT_EQ(OB_INVALID_ARGUMENT, adaptor_->serialize_snapshot(param));
  EXPECT_EQ(5, snap.sealed_builders_.count());
  EXPECT_GT(counter.used_, 0);
  verify_membership(1, 20);
  snap.free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  EXPECT_TRUE(snap.sealed_builders_.empty());
  EXPECT_EQ(0, counter.used_);
  EXPECT_EQ(0, used_);
}

TEST_F(TestVectorIndexMultiSegment, serialize_root_ranges_and_reload_all_bases)
{
  prepare(4);
  for (int i = 0; i < 12; ++i) {
    ASSERT_EQ(OB_SUCCESS, append(i * 4 + 1, 4));
  }
  ObArenaAllocator allocator;
  transaction::ObTxDesc tx;
  transaction::ObTxReadSnapshot snapshot;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  init_serialize_param(param, ctx, tx, snapshot, allocator);
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ASSERT_EQ(OB_SUCCESS, adaptor_->serialize_snapshot(param));
  ASSERT_EQ(12, adaptor_->get_snap_data()->meta_.bases_.count());
  ASSERT_EQ(13, ctx.vals_.count());
  ASSERT_TRUE(ctx.vals_.at(0).is_meta_block());
  ObVectorIndexMeta meta;
  const ObString &root = ctx.vals_.at(0).get_data();
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, meta.deserialize(root.ptr() + sizeof(ObLobCommon),
      root.length() - sizeof(ObLobCommon), pos));
  ASSERT_EQ(12, meta.bases_.count());
  ObString previous;
  int64_t total = 0;
  for (int i = 0; i < ctx.vals_.count(); ++i) {
    ObString key, data;
    ASSERT_EQ(OB_SUCCESS, ctx.get_key_and_data(VIAT_HNSW, param.tablet_id_,
        param.snapshot_version_, i, allocator, key, data));
    if (i > 0) {
      EXPECT_LT(previous.compare(key), 0);
    }
    previous = key;
  }
  for (auto &base : meta.bases_) {
    EXPECT_EQ(param.snapshot_version_, base.scn_);
    std::stringstream stream;
    int64_t blocks = 0;
    for (int i = 1; i < ctx.vals_.count(); ++i) {
      auto &block = ctx.vals_.at(i);
      if (block.get_persist_key().compare(base.start_key_) >= 0
          && block.get_persist_key().compare(base.end_key_) <= 0) {
        ObString bytes;
        ObLobLocatorV2 lob(block.get_data().ptr(), block.get_data().length());
        ASSERT_EQ(OB_SUCCESS, lob.get_inrow_data(bytes));
        stream.write(bytes.ptr(), bytes.length());
        ++blocks;
      }
    }
    EXPECT_EQ(base.blocks_cnt_, blocks);
    ObVectorIndexSegmentHandle loaded;
    ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(base.index_type_, loaded));
    ASSERT_EQ(OB_SUCCESS, loaded->fdeserialize(stream));
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, loaded->get_index_number(count));
    total += count;
  }
  EXPECT_EQ(48, total);
  auto &snap = snapshot_data();
  ASSERT_TRUE(snap.has_complete_);
  ASSERT_TRUE(snap.sealed_builders_.empty());
  auto *first_segment = snap.meta_.bases_.at(0).segment_handle_.get();
  EXPECT_GT(used_, 0);
  ASSERT_EQ(OB_SUCCESS, adaptor_->renew_single_snap_index(false));
  ASSERT_EQ(12, snap.meta_.bases_.count());
  EXPECT_EQ(first_segment, snap.meta_.bases_.at(0).segment_handle_.get());
  EXPECT_TRUE(snap.has_complete_);
  EXPECT_GT(used_, 0);
  ASSERT_EQ(OB_SUCCESS, adaptor_->renew_single_snap_index(true));
  EXPECT_TRUE(snap.meta_.bases_.empty());
  EXPECT_FALSE(snap.has_complete_);
  EXPECT_EQ(0, used_);
}

TEST_F(TestVectorIndexMultiSegment, sq_tail_rescan_restores_buffer)
{
  verify_quantized_tail_rescan("HNSW_SQ");
}

TEST_F(TestVectorIndexMultiSegment, bq_tail_rescan_restores_buffer)
{
  verify_quantized_tail_rescan("HNSW_BQ");
}

TEST_F(TestVectorIndexMultiSegment, rescan_after_completed_snapshot_releases_old_graphs)
{
  prepare(4);
  for (int64_t first = 1; first <= 12; first += 4) {
    ASSERT_EQ(OB_SUCCESS, append(first, 4));
  }
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ASSERT_EQ(OB_SUCCESS, adaptor_->renew_single_snap_index(false));
  auto &snap = snapshot_data();
  ASSERT_TRUE(snap.has_complete_);
  ASSERT_TRUE(snap.sealed_builders_.empty());
  ASSERT_EQ(nullptr, snap.builder_);
  ASSERT_EQ(3, snap.meta_.bases_.count());
  const uint64_t completed_memory = used_;
  // Simulate a downstream DDL failure followed by a new scan on this adaptor.
  ASSERT_EQ(OB_SUCCESS, append(1, 1));
  EXPECT_TRUE(snap.meta_.bases_.empty());
  EXPECT_FALSE(snap.has_complete_);
  EXPECT_LT(used_, completed_memory);
  ASSERT_EQ(OB_SUCCESS, append(2, 11));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  verify_membership(1, 12);
}

class FailDdlKeyAllocator : public ObIAllocator
{
public:
  explicit FailDdlKeyAllocator(ObIAllocator &allocator, const int fail_at = 2)
      : allocator_(allocator), fail_at_(fail_at) {}
  void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    if (size != OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH || ++key_count_ != fail_at_) {
      ptr = allocator_.alloc(size);
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override { allocator_.free(ptr); }
  int key_count_ = 0;
private:
  ObIAllocator &allocator_;
  const int fail_at_;
};

TEST_F(TestVectorIndexMultiSegment, second_segment_failure_uses_existing_cleanup)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ObArenaAllocator arena;
  FailDdlKeyAllocator allocator(arena);
  transaction::ObTxDesc tx;
  transaction::ObTxReadSnapshot snapshot;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  init_serialize_param(param, ctx, tx, snapshot, allocator);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, adaptor_->serialize_snapshot(param));
  EXPECT_EQ(2, allocator.key_count_);
  auto &snap = snapshot_data();
  EXPECT_TRUE(snap.meta_.bases_.empty());
  EXPECT_EQ(2, snap.sealed_builders_.count());
  EXPECT_FALSE(snap.has_complete_);
  EXPECT_GT(used_, 0);
  EXPECT_EQ(nullptr, snap.builder_);
  verify_membership(1, 8);
  // Existing snapshot cleanup owns both sealed builders and the current one.
  snap.free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  EXPECT_TRUE(snap.sealed_builders_.empty());
  EXPECT_EQ(nullptr, snap.builder_);
  EXPECT_EQ(0, used_);
}

TEST_F(TestVectorIndexMultiSegment, merge_partial_serialization_releases_owned_bitmaps)
{
  prepare_merge();
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ObArenaAllocator arena;
  FailDdlKeyAllocator allocator(arena, 3); // First output has bitmap and graph blocks.
  transaction::ObTxDesc tx;
  transaction::ObTxReadSnapshot snapshot;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  init_serialize_param(param, ctx, tx, snapshot, allocator);
  param.is_vec_tablet_rebuild_ = true;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, adaptor_->serialize_snapshot(param, false));
  ASSERT_EQ(2, snapshot_data().sealed_builders_.count());
  EXPECT_TRUE(snapshot_data().meta_.bases_.empty());
  EXPECT_FALSE(snapshot_data().has_complete_);
  for (const auto &block : ctx.vals_) { EXPECT_FALSE(block.is_meta_block()); }
  snapshot_data().free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  EXPECT_EQ(0, used_);
  EXPECT_TRUE(snapshot_data().sealed_builders_.empty());
}

TEST_F(TestVectorIndexMultiSegment, retry_after_serialization_failure_keeps_each_vid_once)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ObArenaAllocator arena;
  FailDdlKeyAllocator allocator(arena);
  transaction::ObTxDesc tx;
  transaction::ObTxReadSnapshot snapshot;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  init_serialize_param(param, ctx, tx, snapshot, allocator);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, adaptor_->serialize_snapshot(param));
  ASSERT_EQ(2, snapshot_data().sealed_builders_.count());
  ASSERT_FALSE(snapshot_data().has_complete_);
  // Retry using the same adaptor, without clearing its retained sealed builders.
  ASSERT_EQ(OB_SUCCESS, append(1, 8));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot());
  ASSERT_EQ(2, snapshot_data().meta_.bases_.count());
  verify_membership(1, 8);
}

TEST_F(TestVectorIndexMultiSegment, partial_merge_retry_keeps_bitmap_policy_after_ownership_transfer)
{
  ObVectorIndexSegmentMeta source;
  prepare_bitmap_merge(false, source);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 4));
  ObArenaAllocator arena;
  FailDdlKeyAllocator allocator(arena, 3);
  transaction::ObTxDesc tx;
  transaction::ObTxReadSnapshot snapshot;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  init_serialize_param(param, ctx, tx, snapshot, allocator);
  param.is_vec_tablet_rebuild_ = true;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, adaptor_->serialize_snapshot(param, false));
  ASSERT_EQ(2, snapshot_data().sealed_builders_.count());
  const auto *first = snapshot_data().sealed_builders_.at(0);
  ASSERT_NE(nullptr, first->vbitmap_);
  ASSERT_NE(nullptr, first->segment_handle_->ibitmap_);
  ASSERT_EQ(OB_SUCCESS, append(1, 12));
  ASSERT_EQ(OB_SUCCESS, serialize_snapshot(true, false));
  EXPECT_EQ(true, snapshot_data().insert_bitmap_pruned_);
  verify_membership(1, 12);
  verify_merge_bitmaps(12, 0);
  // Retrying after bitmap ownership transfer must prune output visibility and preserve the source.
  ASSERT_FALSE(snapshot_data().meta_.bases_.empty());
  const auto *merged_bitmap = snapshot_data().meta_.bases_.at(0).vbitmap_;
  ASSERT_NE(nullptr, merged_bitmap);
  ASSERT_NE(nullptr, source.vbitmap_);
  EXPECT_NE(source.vbitmap_, merged_bitmap);
  for (uint64_t vid = 1; vid <= 12; ++vid) {
    EXPECT_FALSE(roaring::api::roaring64_bitmap_contains(merged_bitmap->insert_bitmap_, vid));
    EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(source.vbitmap_->insert_bitmap_, vid));
  }
  EXPECT_EQ(12, roaring::api::roaring64_bitmap_get_cardinality(source.vbitmap_->insert_bitmap_));
  EXPECT_EQ(1, roaring::api::roaring64_bitmap_get_cardinality(source.vbitmap_->delete_bitmap_));
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(source.vbitmap_->delete_bitmap_, 42000));
  for (const auto &meta : snapshot_data().meta_.bases_) {
    // Each tiny graph needs one graph block plus its persisted bitmap block,
    // including the first output whose bitmap ownership moved before retry.
    EXPECT_EQ(2, meta.blocks_cnt_);
  }
}

TEST_F(TestVectorIndexMultiSegment, invalid_nonempty_serialize_context_keeps_existing_cleanup)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ObArenaAllocator allocator;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  param.vctx_ = &ctx;
  param.allocator_ = &allocator;
  // A nonempty build requires the LOB transaction and read snapshot.
  EXPECT_EQ(OB_INVALID_ARGUMENT, adaptor_->serialize_snapshot(param));
  EXPECT_TRUE(snapshot_data().meta_.bases_.empty());
  verify_membership(1, 4);
  EXPECT_TRUE(ctx.vals_.empty());
  EXPECT_EQ(1, snapshot_data().sealed_builders_.count());
  EXPECT_FALSE(snapshot_data().has_complete_);
  EXPECT_GT(used_, 0);
  snapshot_data().free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  EXPECT_TRUE(snapshot_data().sealed_builders_.empty());
  EXPECT_EQ(0, used_);
}

TEST_F(TestVectorIndexMultiSegment, cancellation_uses_existing_snapshot_cleanup)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 2));
  ObArenaAllocator allocator;
  transaction::ObTxDesc tx;
  transaction::ObTxReadSnapshot snapshot;
  ObVecIdxSnapshotDataWriteCtx ctx;
  ObHNSWSerializeCallback::CbParam param;
  init_serialize_param(param, ctx, tx, snapshot, allocator);
  ObVecIndexAsyncTaskCtx task;
  task.task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_CANCEL;
  param.task_ctx_ = &task;
  EXPECT_EQ(OB_CANCELED, adaptor_->serialize_snapshot(param));
  EXPECT_EQ(2, snapshot_data().sealed_builders_.count());
  EXPECT_TRUE(snapshot_data().meta_.bases_.empty());
  EXPECT_FALSE(snapshot_data().has_complete_);
  EXPECT_GT(used_, 0);
  verify_membership(1, 6);
  snapshot_data().free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  EXPECT_TRUE(snapshot_data().sealed_builders_.empty());
  EXPECT_EQ(nullptr, snapshot_data().builder_);
  EXPECT_EQ(0, used_);
}

TEST_F(TestVectorIndexMultiSegment, existing_snapshot_cleanup_resets_failed_build)
{
  prepare(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 4));
  ASSERT_EQ(OB_SUCCESS, append(5, 2));
  auto &snap = snapshot_data();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      adaptor_->add_snap_index(nullptr, nullptr, nullptr, 0, 1, nullptr, true));
  EXPECT_GT(used_, 0);
  snap.free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  EXPECT_TRUE(snap.sealed_builders_.empty());
  EXPECT_EQ(nullptr, snap.builder_);
  EXPECT_FALSE(snap.enable_multi_segment_);
  EXPECT_FALSE(snap.is_inited());
  EXPECT_EQ(0, snap.segment_max_vectors_);
  EXPECT_FALSE(snap.need_switch_segment_);
  EXPECT_EQ(0, used_);
  // Exercise a fresh build after the pre-existing cleanup entry has run.
  // This does not introduce or simulate a new task-level retry hook.
  init_build(4);
  ASSERT_EQ(OB_SUCCESS, append(1, 6));
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  verify_membership(1, 6);
  snap.free_memdata_resource_with_lock(adaptor_->get_allocator(), MTL_ID());
  EXPECT_EQ(0, used_);
}

TEST_F(TestVectorIndexMultiSegment, vsag_alloc_nothrow_preserves_accounting_and_release)
{
  uint64_t used = 0;
  ObVsagMemContext mem_ctx(&used);
  ASSERT_EQ(OB_SUCCESS, mem_ctx.init(context_, &used, MTL_ID()));
  EXPECT_EQ(nullptr, mem_ctx.alloc_nothrow(0));
  EXPECT_EQ(nullptr, mem_ctx.Allocate(0));
  EXPECT_EQ(0, used);

  void *buffer = mem_ctx.alloc_nothrow(64);
  DEFER(mem_ctx.Deallocate(buffer));
  ASSERT_NE(nullptr, buffer);
  EXPECT_EQ(64 + sizeof(int64_t), used);
  MEMSET(buffer, 0x5a, 64);

  // A buffer allocated by the OB entry can use VSAG's existing reallocate/free path.
  void *resized = mem_ctx.Reallocate(buffer, 128);
  ASSERT_NE(nullptr, resized);
  buffer = resized;
  EXPECT_EQ(128 + sizeof(int64_t), used);
  for (int64_t i = 0; i < 64; ++i) {
    EXPECT_EQ(0x5a, static_cast<unsigned char *>(buffer)[i]);
  }
  mem_ctx.Deallocate(buffer);
  buffer = nullptr;
  EXPECT_EQ(0, used);

  buffer = mem_ctx.Allocate(64);
  ASSERT_NE(nullptr, buffer);
  EXPECT_EQ(64 + sizeof(int64_t), used);
  mem_ctx.Deallocate(buffer);
  buffer = nullptr;
  EXPECT_EQ(0, used);
}

TEST_F(TestVectorIndexMultiSegment, vsag_alloc_nothrow_failure_keeps_vsag_exception_contract)
{
  uint64_t used = 0;
  ObVsagMemContext mem_ctx(&used);
  ASSERT_EQ(OB_SUCCESS, mem_ctx.init(context_, &used, MTL_ID()));
  const int64_t limit = mem_ctx.throttle_tool_->get_resource_limit<ObTenantVectorAllocator>();
  ASSERT_GE(limit, 0);
  ASSERT_LT(limit, INT64_MAX / 2);
  // Reject at the tenant limit before attempting a large physical allocation.
  const size_t over_limit = static_cast<size_t>(MAX(limit + 1, ObVectorMemContext::CHECK_RESOURCE_UNIT_SIZE));
  EXPECT_EQ(nullptr, mem_ctx.alloc_nothrow(over_limit));
  EXPECT_THROW(mem_ctx.Allocate(over_limit), std::bad_alloc);
  EXPECT_EQ(0, used);

  // Include both signed-size overflow after the header and unsigned addition overflow.
  const size_t invalid_sizes[] = {static_cast<size_t>(INT64_MAX), static_cast<size_t>(-1)};
  for (const size_t size : invalid_sizes) {
    EXPECT_EQ(nullptr, mem_ctx.alloc_nothrow(size));
    EXPECT_THROW(mem_ctx.Allocate(size), std::bad_alloc);
    EXPECT_EQ(0, used);
  }
}

TEST_F(TestVectorIndexMultiSegment, brute_force_single_segment_preserves_positions)
{
  init_adaptor();
  ObVectorIndexParam *param = nullptr;
  ASSERT_EQ(OB_SUCCESS, adaptor_->get_hnsw_param(param));
  ASSERT_NE(nullptr, param);
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_incr_data(adaptor_->incr_data_, VIAT_HNSW, param));
  auto &segment = adaptor_->incr_data_->segment_handle_;
  int64_t vid = 21;
  float vector[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  ASSERT_EQ(OB_SUCCESS, segment->add_index(vector, &vid, 8, nullptr, 1));
  const int64_t candidates[] = {21, 2, 21, 100};
  ObVecIdxQueryResult result;
  DEFER(adaptor_->free_result(result));
  ASSERT_EQ(OB_SUCCESS, adaptor_->vsag_query_vids(vector, candidates, 4, result, 0));
  ASSERT_EQ(1, result.segments_.count());
  ASSERT_EQ(1, result.distances_.count());
  ASSERT_NE(nullptr, result.distances_.at(0));
  const float *baseline = nullptr;
  DEFER(segment->mem_ctx()->Deallocate(const_cast<float *>(baseline)));
  ASSERT_EQ(OB_SUCCESS, segment->cal_distance_by_id(vector, candidates, 4, baseline));
  ASSERT_NE(nullptr, baseline);
  for (int64_t i = 0; i < 4; ++i) {
    EXPECT_FLOAT_EQ(baseline[i], result.distances_.at(0)[i]);
  }
}

TEST_F(TestVectorIndexMultiSegment, brute_force_grouped_vids_preserve_positions)
{
  prepare(2, "HNSW_SQ");
  ObVectorIndexParam *param = nullptr;
  ASSERT_EQ(OB_SUCCESS, adaptor_->get_hnsw_param(param));
  ASSERT_NE(nullptr, param);
  const int64_t build_vids[] = {1, 3, 3, 5, 9, 11};
  for (int64_t i = 0; i < 6; ++i) {
    ASSERT_EQ(OB_SUCCESS, append(build_vids[i], 1));
  }
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  ObVecIdxSnapshotData &snap = snapshot_data();
  ASSERT_EQ(2, snap.sealed_builders_.count());
  ASSERT_NE(nullptr, snap.builder_);
  ASSERT_TRUE(snap.builder_->segment_handle_.is_valid());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, snap.append_base_meta(snap.meta_, *snap.sealed_builders_.at(i),
        ObTabletID(200001), 123456789, i + 1, 1, adaptor_->index_param_));
  }
  ASSERT_EQ(OB_SUCCESS, snap.meta_.incrs_.push_back(snap.meta_.bases_.at(1)));
  snap.meta_.bases_.pop_back();
  snap.meta_.incrs_.at(0).seg_type_ = ObVectorIndexSegmentType::FREEZE_PERSIST;
  // An absent range must still permit exact membership grouping.
  snap.meta_.incrs_.at(0).segment_handle_->vid_bound_.reset();
  snap.set_inited();
  adaptor_->frozen_data_->segment_handle_ = snap.builder_->segment_handle_;
  adaptor_->frozen_data_->set_has_frozen();
  ASSERT_EQ(OB_SUCCESS, adaptor_->init_incr_data(
      adaptor_->incr_data_, VIAT_HGRAPH, param));
  int64_t active_vid = 21;
  float vector[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  ASSERT_EQ(OB_SUCCESS, adaptor_->incr_data_->segment_handle_->add_index(vector, &active_vid, 8, nullptr, 1));
  // Active bounds may lag insertion and must never exclude a present VID.
  adaptor_->incr_data_->segment_handle_->set_read_vid_bound(ObVidBound(1, 1));
  const int64_t candidates[][7] = {
      {3, 2, 1, 3, 5, 100, 21}, // Overlap, holes, duplicate positions and an empty frozen group.
      {3, 3, 3, 3, 3, 3, 3},   // Full groups can retain their VSAG output directly.
      {2, 100, 2, 100, 2, 100, 2}};
  for (int pass = 0; pass < 3; ++pass) {
    // Frozen HGraph with an outstanding writer must use the safe fallback.
    if (pass == 1) {
      adaptor_->frozen_data_->segment_handle_->inc_write_ref();
    } else if (pass == 2) {
      ObVectorIndexSegmentHandle active;
      ASSERT_EQ(OB_SUCCESS, ObVectorIndexSegment::create(active, MTL_ID(), *adaptor_->get_allocator(),
          *param, VIAT_HNSW, 16, adaptor_));
      ASSERT_EQ(OB_SUCCESS, active->add_index(vector, &active_vid, 8, nullptr, 1));
      active->set_read_vid_bound(ObVidBound(1, 1));
      adaptor_->incr_data_->segment_handle_ = active;
    }
    DEFER(if (pass == 1) { adaptor_->frozen_data_->segment_handle_->dec_write_ref(); });
    for (int batch = 0; batch < 3; ++batch) {
      // Cross multiple grouping batches and leave a partial final batch.
      int64_t query_vids[519];
      for (int64_t i = 0; i < 519; ++i) {
        query_vids[i] = candidates[batch][i % 7];
      }
      ObVecIdxQueryResult result;
      DEFER(adaptor_->free_result(result));
      ASSERT_EQ(OB_SUCCESS, adaptor_->vsag_query_vids(vector, query_vids, 519, result, 0));
      ASSERT_EQ(4, result.segments_.count());
      ASSERT_EQ(4, result.distances_.count());
      for (int64_t i = 0; i < result.segments_.count(); ++i) {
        ASSERT_NE(nullptr, result.distances_.at(i));
        const float *baseline = nullptr;
        ASSERT_EQ(OB_SUCCESS, result.segments_.at(i)->cal_distance_by_id(vector, query_vids, 519, baseline));
        ASSERT_NE(nullptr, baseline);
        for (int j = 0; j < 519; ++j) {
          EXPECT_FLOAT_EQ(baseline[j], result.distances_.at(i)[j]);
        }
        result.segments_.at(i)->mem_ctx()->Deallocate(const_cast<float *>(baseline));
      }
    }
  }
}

TEST_F(TestVectorIndexMultiSegment, size_weighted_ef_search_and_bypass)
{
  struct SwitchGuard
  {
    ~SwitchGuard()
    {
      EventTable::set_event("ERRSIM_VEC_DISABLE_SIZE_WEIGHTED_EF", EventItem());
      EventTable::set_event("ERRSIM_VEC_ENABLE_SEGMENT_EF_REDUCTION", EventItem());
    }
  } switch_guard;
  EventItem enabled;
  enabled.error_code_ = OB_ERR_UNEXPECTED;
  enabled.trigger_freq_ = 1;
  prepare(10001, "HNSW_SQ");
  // Collinear vectors used by DDL fixtures can collapse under SQ and return
  // fewer than Q even with the original ef. Use reproducible, dispersed data
  // so result counts distinguish unchanged Q from the legacy 2.5x policy.
  float query[8] = {};
  uint32_t random_state = 20261021;
  for (int segment = 0; segment < 2; ++segment) {
    std::vector<float> vectors(10001 * 8);
    std::vector<int64_t> ids(10001);
    for (int i = 0; i < 10001; ++i) {
      ids[i] = segment * 10001 + i + 1;
      for (int d = 0; d < 8; ++d) {
        random_state ^= random_state << 13;
        random_state ^= random_state >> 17;
        random_state ^= random_state << 5;
        vectors[i * 8 + d] = static_cast<float>(random_state & 0xffffffU) / 16777216.0F;
        if (segment == 0 && i == 0) { query[d] = vectors[d]; }
      }
    }
    ASSERT_EQ(OB_SUCCESS, adaptor_->add_snap_index(
        vectors.data(), ids.data(), nullptr, 0, 10001, nullptr, true));
  }
  ASSERT_EQ(OB_SUCCESS, adaptor_->check_snap_index());
  auto &snap = snapshot_data();
  ASSERT_EQ(2, snap.sealed_builders_.count());
  ObArenaAllocator alloc("TestSegEf");
  ObVectorQueryAdaptorResultContext ctx(MTL_ID(), ObLSID(1), 0, &alloc, &alloc);
  for (auto *builder : snap.sealed_builders_) {
    auto &handle = builder->segment_handle_;
    ASSERT_EQ(VIAT_HNSW_SQ, handle->get_index_type());
    handle->set_is_base();
    ASSERT_EQ(OB_SUCCESS, ctx.add_segment_querier(handle, false, false, nullptr, false));
  }
  ObVectorQueryConditions condition;
  condition.ef_search_ = 300;
  condition.query_limit_ = 100;
  condition.original_query_limit_ = 100;
  condition.filter_selectivity_ = 1.0;
  ObVectorIndexSegQueryHandler handler;
  handler.tenant_id_ = MTL_ID();
  handler.ctx_ = &ctx;
  handler.query_cond_ = &condition;
  handler.valid_ratio_ = 1.0;
  handler.dim_ = 8;
  handler.query_vector_ = query;
  handler.segment_querier_ = ctx.segments_.at(0);
  ObVsagQueryResult baseline;
  ASSERT_EQ(OB_SUCCESS, handler.knn_search(baseline, 2));
  EXPECT_EQ(20002, handler.total_vector_count_); // Size weighting is enabled by default.
  ASSERT_EQ(100, baseline.total_);
  EXPECT_EQ(1, baseline.vids_[0]);

  int64_t actual_ef = -1;
  int64_t actual_topk = -1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_VEC_DISABLE_SIZE_WEIGHTED_EF", enabled));
  handler.total_vector_count_ = -1; // Start another search round.
  calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
  EXPECT_EQ(0, handler.total_vector_count_);
  EXPECT_EQ(300, actual_ef);
  EXPECT_EQ(100, actual_topk);

  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_VEC_DISABLE_SIZE_WEIGHTED_EF", EventItem()));
  handler.total_vector_count_ = -1;
  for (int i = 0; i < 2; ++i) {
    calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
    EXPECT_EQ(288, actual_ef);
    EXPECT_EQ(100, actual_topk);
    EXPECT_EQ(20002, handler.total_vector_count_);
  }
  handler.total_vector_count_ = -1;
  ObVsagQueryResult results[2];
  ASSERT_EQ(OB_SUCCESS, handler.knn_search(results[0], 2));
  EXPECT_EQ(20002, handler.total_vector_count_);
  EXPECT_EQ(100, results[0].total_);
  // A switch change within a round must not stack the old candidate reduction.
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_VEC_ENABLE_SEGMENT_EF_REDUCTION", enabled));
  handler.segment_querier_ = ctx.segments_.at(1);
  calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
  EXPECT_EQ(288, actual_ef);
  EXPECT_EQ(100, actual_topk);
  ASSERT_EQ(OB_SUCCESS, handler.knn_search(results[1], 2));
  EXPECT_EQ(100, results[1].total_);
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_VEC_ENABLE_SEGMENT_EF_REDUCTION", EventItem()));

  ObHnswBitmapFilter deleted(MTL_ID(), ObHnswBitmapFilter::BYTE_ARRAY, 0, &alloc);
  ASSERT_EQ(OB_SUCCESS, deleted.init(1, 20002));
  ASSERT_EQ(OB_SUCCESS, deleted.add(1));
  auto *first = ctx.segments_.at(0);
  first->filter_ = &deleted;
  deleted.segment_handle_ = first->handle_;
  handler.segment_querier_ = first;
  ObVsagQueryResult filtered;
  ASSERT_EQ(OB_SUCCESS, handler.knn_search(filtered, 2));
  EXPECT_EQ(100, filtered.total_);
  for (int64_t i = 0; i < filtered.total_; ++i) { EXPECT_NE(1, filtered.vids_[i]); }
  first->filter_ = nullptr;
  deleted.reset();

  // Exercise asymmetric distributions using an already sampled total of 100000.
  // Only the cached total and current segment count enter the allocation formula.
  handler.total_vector_count_ = 100000;
  const int64_t vector_counts[] = {90000, 10000, 99000, 1000};
  const int64_t expected_efs[] = {300, 113, 300, 100};
  for (int i = 0; i < 4; ++i) {
    calc_segment_ef_params(handler, ctx.segments_, 2, vector_counts[i], actual_ef, actual_topk);
    EXPECT_EQ(expected_efs[i], actual_ef);
    EXPECT_EQ(100, actual_topk);
  }

  // Both pre-filter and post-filter allocate candidates, with the SQL limit as a floor.
  condition.query_limit_ = 300;
  const int64_t expected_limits[] = {300, 113, 300, 100};
  for (const bool post_filter : {false, true}) {
    condition.is_post_with_filter_ = post_filter;
    for (int i = 0; i < 4; ++i) {
      calc_segment_ef_params(handler, ctx.segments_, 2, vector_counts[i], actual_ef, actual_topk);
      EXPECT_EQ(expected_limits[i], actual_topk);
      EXPECT_EQ(actual_topk, actual_ef);
    }
  }
  condition.query_limit_ = 20; // A continuation round can request fewer than the SQL limit.
  calc_segment_ef_params(handler, ctx.segments_, 2, 1000, actual_ef, actual_topk);
  EXPECT_EQ(100, actual_topk);
  EXPECT_EQ(100, actual_ef);
  condition.is_post_with_filter_ = false;

  condition.query_limit_ = 300;
  for (const double selectivity : {-1.0, 0.0, 0.1, 0.10000001, 0.1499, 0.15, 1.1}) {
    condition.filter_selectivity_ = selectivity;
    handler.total_vector_count_ = -1;
    calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
    EXPECT_EQ(0, handler.total_vector_count_);
    EXPECT_EQ(300, actual_topk);
    EXPECT_EQ(300, actual_ef);
  }
  // A new round must still bypass weighting when the current selectivity is 0.15.
  condition.filter_selectivity_ = 0.15;
  condition.query_limit_ = 500;
  handler.total_vector_count_ = -1;
  calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
  EXPECT_EQ(0, handler.total_vector_count_);
  EXPECT_EQ(500, actual_topk);
  EXPECT_EQ(500, actual_ef);
  condition.query_limit_ = 300;
  condition.filter_selectivity_ = 0.1501;
  handler.total_vector_count_ = -1;
  calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
  EXPECT_EQ(288, actual_topk);
  EXPECT_EQ(288, actual_ef);
  // Hybrid callers leave selectivity unknown; both filter paths bypass size weighting.
  condition.original_query_limit_ = 0;
  condition.filter_selectivity_ = -1.0;
  handler.total_vector_count_ = -1;
  calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
  EXPECT_EQ(0, handler.total_vector_count_);
  EXPECT_EQ(300, actual_topk);
  EXPECT_EQ(300, actual_ef);
  condition.is_post_with_filter_ = true;
  handler.total_vector_count_ = -1;
  calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
  EXPECT_EQ(0, handler.total_vector_count_);
  EXPECT_EQ(300, actual_topk);
  EXPECT_EQ(300, actual_ef);
  condition.is_post_with_filter_ = false;
  condition.original_query_limit_ = 100;
  condition.filter_selectivity_ = 1.0;
  condition.query_limit_ = 100;

  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_VEC_ENABLE_SEGMENT_EF_REDUCTION", enabled));
  handler.total_vector_count_ = -1;
  calc_segment_ef_params(handler, ctx.segments_, 2, 10001, actual_ef, actual_topk);
  EXPECT_EQ(150, actual_ef);
  EXPECT_EQ(125, actual_topk);
  ObVsagQueryResult legacy;
  ASSERT_EQ(OB_SUCCESS, handler.knn_search(legacy, 2));
  EXPECT_EQ(0, handler.total_vector_count_);
  EXPECT_EQ(125, legacy.total_); // The legacy switch overrides default size weighting.
  calc_segment_ef_params(handler, ctx.segments_, 10, 10001, actual_ef, actual_topk);
  EXPECT_EQ(100, actual_topk);
  EXPECT_EQ(100, actual_ef);
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_VEC_ENABLE_SEGMENT_EF_REDUCTION", EventItem()));

  // Exercise the existing adaptor loop and merge with the new calculation.
  for (int64_t i = 0; i < snap.sealed_builders_.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, snap.append_base_meta(snap.meta_, *snap.sealed_builders_.at(i),
        ObTabletID(200001), 123456789, i + 1, 1, adaptor_->index_param_));
  }
  ObVectorQueryAdaptorResultContext dispatch_ctx(MTL_ID(), ObLSID(1), 0, &alloc, &alloc);
  ASSERT_EQ(OB_SUCCESS, dispatch_ctx.init_bitmaps());
  ObVectorQueryVidIterator iterator(0, 0, 0, nullptr);
  ObVectorQueryVidIterator *iterator_ptr = &iterator;
  ASSERT_EQ(OB_SUCCESS, adaptor_->vsag_query_vids(&dispatch_ctx, &condition, 8, query, iterator_ptr));
  ASSERT_EQ(100, iterator.get_total());
  for (int64_t i = 1; i < iterator.get_total(); ++i) {
    EXPECT_LE(iterator.get_distance()[i - 1], iterator.get_distance()[i]);
    for (int64_t j = 0; j < i; ++j) { EXPECT_NE(iterator.get_vids()[j], iterator.get_vids()[i]); }
  }

  // The existing continuation loop uses a fresh handler for every round.
  ObVectorQueryAdaptorResultContext iterative_ctx(MTL_ID(), ObLSID(1), 0, &alloc, &alloc);
  ASSERT_EQ(OB_SUCCESS, iterative_ctx.init_bitmaps());
  condition.is_post_with_filter_ = true;
  condition.query_vector_.assign_ptr(reinterpret_cast<char *>(query), sizeof(query));
  ObVectorQueryVidIterator first_round(0, 0, 0, nullptr);
  ObVectorQueryVidIterator *round_iter = &first_round;
  ASSERT_EQ(OB_SUCCESS, adaptor_->vsag_query_vids(&iterative_ctx, &condition, 8, query, round_iter));
  condition.query_limit_ = 250;
  ASSERT_EQ(OB_SUCCESS, adaptor_->query_next_result(&iterative_ctx, &condition, round_iter));
  ASSERT_NE(nullptr, round_iter);
  EXPECT_GT(round_iter->get_total(), 0);
  EXPECT_LE(round_iter->get_total(), condition.query_limit_);
  EXPECT_EQ(250, condition.query_limit_);
  EXPECT_EQ(300, condition.ef_search_);
  round_iter->~ObVectorQueryVidIterator();

  // A physically nonempty base can contribute no candidates after deletion
  // filtering. A full reduced quota in the other base must permit continuation.
  for (const bool weighted : {false, true}) {
    ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_VEC_DISABLE_SIZE_WEIGHTED_EF",
        weighted ? EventItem() : enabled));
    ObVectorQueryAdaptorResultContext deleted_ctx(MTL_ID(), ObLSID(1), 0, &alloc, &alloc);
    ASSERT_EQ(OB_SUCCESS, deleted_ctx.init_bitmaps());
    for (int64_t vid = 10002; vid <= 20002; ++vid) {
      roaring::api::roaring64_bitmap_add(deleted_ctx.bitmaps_->delete_bitmap_, vid);
    }
    condition.query_limit_ = 300;
    ObVectorQueryVidIterator initial(0, 0, 0, nullptr);
    ObVectorQueryVidIterator *current = &initial;
    ASSERT_EQ(OB_SUCCESS, adaptor_->vsag_query_vids(&deleted_ctx, &condition, 8, query, current));
    ASSERT_EQ(weighted ? 288 : 300, current->get_total());
    for (int64_t i = 0; i < current->get_total(); ++i) { EXPECT_LE(current->get_vids()[i], 10001); }

    ASSERT_EQ(OB_SUCCESS, adaptor_->query_next_result(&deleted_ctx, &condition, current));
    ASSERT_NE(nullptr, current);
    ASSERT_EQ(weighted ? 288 : 300, current->get_total());
    current->~ObVectorQueryVidIterator();

    // Preserve the cache-only last-search path and cover an empty result round.
    condition.is_last_search_ = true;
    condition.distance_threshold_ = -1.0F;
    ASSERT_EQ(OB_SUCCESS, adaptor_->query_next_result(&deleted_ctx, &condition, current));
    ASSERT_NE(nullptr, current);
    EXPECT_EQ(0, current->get_total());
    EXPECT_TRUE(condition.is_last_search_);
    EXPECT_EQ(300, condition.query_limit_);
    EXPECT_EQ(300, condition.ef_search_);
    current->~ObVectorQueryVidIterator();
    condition.is_last_search_ = false;
    condition.distance_threshold_ = FLT_MAX;
  }

  // A partial nonempty result is distinct from an empty result round.
  ObVectorQueryAdaptorResultContext exhausted_ctx(MTL_ID(), ObLSID(1), 0, &alloc, &alloc);
  ASSERT_EQ(OB_SUCCESS, exhausted_ctx.init_bitmaps());
  for (int64_t vid = 11; vid <= 20002; ++vid) {
    roaring::api::roaring64_bitmap_add(exhausted_ctx.bitmaps_->delete_bitmap_, vid);
  }
  ObVectorQueryVidIterator exhausted(0, 0, 0, nullptr);
  ObVectorQueryVidIterator *exhausted_ptr = &exhausted;
  ASSERT_EQ(OB_SUCCESS, adaptor_->vsag_query_vids(&exhausted_ctx, &condition, 8, query, exhausted_ptr));
  EXPECT_EQ(10, exhausted.get_total());
}

TEST_F(TestVectorIndexMultiSegment, bq_size_weighted_candidates_follow_ef)
{
  init_adaptor("HNSW_BQ");
  ObVectorIndexSegmentHandle handle;
  ASSERT_EQ(OB_SUCCESS, adaptor_->create_snap_segment(VIAT_HNSW_BQ, handle));
  ASSERT_EQ(VIAT_HNSW_BQ, handle->get_index_type());
  ObArenaAllocator alloc("TestBqSegEf");
  ObVectorQueryAdaptorResultContext ctx(MTL_ID(), ObLSID(1), 0, &alloc, &alloc);
  ASSERT_EQ(OB_SUCCESS, ctx.add_segment_querier(handle, false, false, nullptr, false));
  ObVectorQueryConditions condition;
  condition.query_limit_ = 300;
  condition.ef_search_ = 600;
  condition.original_query_limit_ = 100;
  condition.filter_selectivity_ = 1.0;
  ObVectorIndexSegQueryHandler handler;
  handler.query_cond_ = &condition;
  handler.segment_querier_ = ctx.segments_.at(0);
  // Exercise per-segment allocation after the round's size sampling has completed.
  handler.total_vector_count_ = 100000;
  int64_t actual_ef = 0;
  int64_t actual_topk = 0;
  for (const bool post_filter : {false, true}) {
    condition.is_post_with_filter_ = post_filter;
    calc_segment_ef_params(handler, ctx.segments_, 2, 10000, actual_ef, actual_topk);
    EXPECT_EQ(177, actual_ef);
    EXPECT_EQ(177, actual_topk); // Preserve ef instead of reducing it to the old topk of 113.
    calc_segment_ef_params(handler, ctx.segments_, 2, 90000, actual_ef, actual_topk);
    EXPECT_EQ(600, actual_ef);
    EXPECT_EQ(600, actual_topk); // BQ candidates can exceed the shared query_limit.
    calc_segment_ef_params(handler, ctx.segments_, 2, 1000, actual_ef, actual_topk);
    EXPECT_EQ(100, actual_ef);
    EXPECT_EQ(100, actual_topk); // Both remain equal after the SQL lower-bound protection.
  }

  // A round that bypasses weighting must keep ef and topk independent.
  handler.total_vector_count_ = 0;
  calc_segment_ef_params(handler, ctx.segments_, 2, 10000, actual_ef, actual_topk);
  EXPECT_EQ(600, actual_ef);
  EXPECT_EQ(300, actual_topk);
  condition.original_query_limit_ = 0;
  condition.query_limit_ = 100;
  condition.ef_search_ = 64;
  calc_segment_ef_params(handler, ctx.segments_, 2, 10000, actual_ef, actual_topk);
  EXPECT_EQ(64, actual_ef);
  EXPECT_EQ(100, actual_topk);

  struct SwitchGuard
  {
    ~SwitchGuard()
    {
      EventTable::set_event("ERRSIM_VEC_ENABLE_SEGMENT_EF_REDUCTION", EventItem());
    }
  } switch_guard;
  EventItem enabled;
  enabled.error_code_ = OB_ERR_UNEXPECTED;
  enabled.trigger_freq_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_VEC_ENABLE_SEGMENT_EF_REDUCTION", enabled));
  condition.original_query_limit_ = 100;
  condition.ef_search_ = 600;
  calc_segment_ef_params(handler, ctx.segments_, 2, 10000, actual_ef, actual_topk);
  EXPECT_EQ(300, actual_ef);
  EXPECT_EQ(125, actual_topk);
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_vector_index_multi_segment.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
