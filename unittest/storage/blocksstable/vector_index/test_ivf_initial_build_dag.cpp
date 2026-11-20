/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "storage/ob_storage_schema.h"
#include "storage/schema_utils.h"

#define private public
#define protected public
#include "storage/compaction/ob_async_index_merge_task.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#undef protected
#undef private

#include "lib/hash_func/splitmix64.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace compaction;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace
{

class TestFullRebuildInput final
    : public ObIVFVectorStore,
      public ObIVFFirstLevelSampleSet
{
public:
  static constexpr ObIVFVectorVID VID = 7;

  virtual bool is_valid() const override { return true; }
  virtual int64_t get_vector_count() const override { return 1; }
  virtual int64_t get_dimension() const override { return 1; }
  virtual int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override
  {
    vids.reset();
    return vids.push_back(VID);
  }
  virtual int batch_get_by_vid(
      const ObIVFVectorIDView &vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      share::ObVectorMatrix &vectors) const override
  {
    int ret = OB_SUCCESS;
    vectors.reset();
    if (vid_begin < 0 || vid_end <= vid_begin || vid_end > vids.count()) {
      ret = OB_INVALID_ARGUMENT;
    }
    for (int64_t i = vid_begin; OB_SUCC(ret) && i < vid_end; ++i) {
      if (VID != vids.at(i)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        ret = vectors.push_back(3.0F);
      }
    }
    return ret;
  }
  virtual int64_t get_sample_count() const override { return 1; }
  virtual int batch_get_samples(
      const int64_t sample_begin,
      const int64_t sample_end,
      ObIArray<ObIVFVectorVID> &sample_vids,
      share::ObVectorMatrix &sample_vectors) const override
  {
    int ret = OB_SUCCESS;
    sample_vids.reset();
    sample_vectors.reset();
    if (0 != sample_begin || 1 != sample_end) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(sample_vids.push_back(VID))) {
    } else {
      ret = sample_vectors.push_back(3.0F);
    }
    return ret;
  }
};

class TestFullRebuildAlgorithm
    : public ObIVFMergeAlgorithm,
      public ObIVFMergeQuantizer
{
public:
  TestFullRebuildAlgorithm()
    : quantize_count_(0), quantization_finish_count_(0), raw_write_count_(0),
      raw_finish_count_(0), root_first_level_samples_(nullptr)
  {}

  virtual int hca_train_local_centers(
      const int64_t depth,
      const int64_t node_idx,
      ObIVFVectorReader &reader,
      const ObIVFVectorIDView &node_vids,
      const ObIVFFirstLevelSampleSet *first_level_samples,
      const int64_t cluster_size,
      int64_t &local_k,
      share::ObVectorMatrix &local_centers) override
  {
    UNUSEDx(node_idx, reader);
    if (0 == depth) {
      root_first_level_samples_ = first_level_samples;
    }
    local_centers.reset();
    local_k = 1;
    return 1 == node_vids.count() && cluster_size > 0
        ? local_centers.push_back(3.0F)
        : OB_INVALID_ARGUMENT;
  }

  virtual int hca_assign_vectors(
      ObIVFVectorReader &reader,
      const ObIVFVectorIDView &node_vids,
      const share::ObVectorMatrix &local_centers,
      const int64_t local_k,
      const int64_t vid_begin,
      const int64_t vid_end,
      ObIArray<int64_t> &local_center_idxs) override
  {
    UNUSED(reader);
    local_center_idxs.reset();
    if (1 != node_vids.count() || 1 != local_centers.count()
        || 1 != local_k || 0 != vid_begin || 1 != vid_end) {
      return OB_INVALID_ARGUMENT;
    }
    return local_center_idxs.push_back(0);
  }

  virtual int hca_compute_local_center_stats_range(
      ObIVFVectorReader &reader,
      const ObIVFVectorIDView &member_vids,
      const share::ObVectorMatrix &local_centers,
      const int64_t local_center_idx,
      share::ObVectorMatrix &updated_center) override
  {
    UNUSEDx(reader, local_centers);
    updated_center.reset();
    return 1 == member_vids.count() && 0 == local_center_idx
        ? updated_center.push_back(3.0F)
        : OB_INVALID_ARGUMENT;
  }

  virtual int quantize_cluster(ObIVFVectorReader &reader, const share::ObVectorMatrix &final_centers,
      const int64_t center_count, const int64_t cid, const ObIVFVectorIDView &primary_member_vids,
      const storage::ObIVFVectorIDView *secondary_member_vids) override
  {
    UNUSED(reader);
    if (1 != center_count || 0 != cid || 1 != final_centers.count()
        || 1 != primary_member_vids.count()
        || TestFullRebuildInput::VID != primary_member_vids.at(0)
        || OB_NOT_NULL(secondary_member_vids)) {
      return OB_INVALID_ARGUMENT;
    }
    ++quantize_count_;
    return OB_SUCCESS;
  }

  virtual int finish_quantization(
      const int64_t vector_count,
      const int64_t center_count,
      const int64_t posting_count) override
  {
    if (1 != vector_count || 1 != center_count || 1 != posting_count) {
      return OB_INVALID_ARGUMENT;
    }
    ++quantization_finish_count_;
    return OB_SUCCESS;
  }

  virtual int write_raw_vectors(
      ObIVFVectorReader &reader,
      const ObIVFVectorIDView &all_vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      const int64_t task_idx) override
  {
    if (1 != reader.get_dimension() || 1 != all_vids.count()
        || TestFullRebuildInput::VID != all_vids.at(0)
        || 0 != vid_begin || 1 != vid_end || 0 != task_idx) {
      return OB_INVALID_ARGUMENT;
    }
    ++raw_write_count_;
    return OB_SUCCESS;
  }

  virtual int finish_raw_write(const int64_t vector_count) override
  {
    if (1 != vector_count || 1 != raw_write_count_) {
      return OB_INVALID_ARGUMENT;
    }
    ++raw_finish_count_;
    return OB_SUCCESS;
  }

  int64_t quantize_count_;
  int64_t quantization_finish_count_;
  int64_t raw_write_count_;
  int64_t raw_finish_count_;
  const ObIVFFirstLevelSampleSet *root_first_level_samples_;
};

int prepare_async_ivf_schema(
    ObIAllocator &allocator,
    ObStorageSchema &storage_schema)
{
  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  table_schema.set_data_table_id(table_schema.get_table_id());
  return storage_schema.init(
      allocator, table_schema, lib::Worker::CompatMode::MYSQL);
}

void run_materialized_full_rebuild(
    ObIVFMergeDagCtx &dag_ctx,
    ObITask &prepare_task)
{
  ObITask *parent_task = &prepare_task;
  int64_t plan_count = 0;
  while (ObIVFMergeStage::FINISH
      != dag_ctx.full_rebuild_planner_.get_stage()) {
    ASSERT_EQ(1, parent_task->get_child_nodes().count());
    ObIVFMergeTask *first_task = static_cast<ObIVFMergeTask *>(
        parent_task->get_child_nodes().at(0));
    ASSERT_NE(nullptr, first_task);
    const ObIVFTaskPlan *task_plan = first_task->task_plan_;
    ASSERT_NE(nullptr, task_plan);
    ObIVFMergeFinishTask *finish_task =
        static_cast<ObIVFMergeFinishTask *>(
            first_task->get_child_nodes().at(0));
    ASSERT_NE(nullptr, finish_task);

    ObSEArray<ObIVFMergeTask *, 8> tasks;
    ASSERT_EQ(OB_SUCCESS, tasks.push_back(first_task));
    for (int64_t task_idx = 1;
         task_idx < task_plan->get_task_count();
         ++task_idx) {
      ObITask *next_task = nullptr;
      ASSERT_EQ(OB_SUCCESS,
          tasks.at(task_idx - 1)->generate_next_task(next_task));
      ASSERT_NE(nullptr, next_task);
      ASSERT_EQ(OB_SUCCESS,
          tasks.at(task_idx - 1)->copy_children_to(*next_task));
      ASSERT_EQ(OB_SUCCESS,
          tasks.push_back(static_cast<ObIVFMergeTask *>(next_task)));
    }
    ObITask *next_task = nullptr;
    ASSERT_EQ(OB_ITER_END,
        tasks.at(tasks.count() - 1)->generate_next_task(next_task));
    ASSERT_EQ(nullptr, next_task);
    ASSERT_EQ(task_plan->get_task_count(), finish_task->get_indegree());

    for (int64_t task_idx = 0; task_idx < tasks.count(); ++task_idx) {
      ASSERT_EQ(OB_SUCCESS, tasks.at(task_idx)->process());
    }
    ASSERT_EQ(OB_SUCCESS, finish_task->process());
    parent_task = finish_task;
    ++plan_count;
  }

  ASSERT_GT(plan_count, 0);
  ASSERT_EQ(1, parent_task->get_child_nodes().count());
  ASSERT_NE(nullptr, static_cast<ObAsyncIndexMergeFinishTask *>(
      parent_task->get_child_nodes().at(0)));
}

class SuspendFullRebuildAlgorithm final : public TestFullRebuildAlgorithm
{
public:
  SuspendFullRebuildAlgorithm()
    : cancel_count_(0), cancel_ret_(OB_SUCCESS)
  {}

  virtual int hca_train_local_centers(
      const int64_t depth,
      const int64_t node_idx,
      ObIVFVectorReader &reader,
      const ObIVFVectorIDView &node_vids,
      const ObIVFFirstLevelSampleSet *first_level_samples,
      const int64_t cluster_size,
      int64_t &local_k,
      share::ObVectorMatrix &local_centers) override
  {
    UNUSEDx(depth, node_idx, reader, node_vids, first_level_samples,
        cluster_size, local_k, local_centers);
    return OB_EAGAIN;
  }

  virtual void cancel(const int ret_code) override
  {
    ++cancel_count_;
    cancel_ret_ = ret_code;
  }

  int64_t cancel_count_;
  int cancel_ret_;
};

} // namespace

TEST(TestIVFInitialBuildDag, first_level_samples_are_shared_by_initial_build_and_merge)
{
  ObArenaAllocator allocator(
      "IVFSampleGate", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  TestFullRebuildInput vector_store;
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS,
      input.init(vector_store, vector_store, 1,
          true /* vectors_normalized */, true /* vectors_rotated */, false /* enable_soar */));

  {
    TestFullRebuildAlgorithm algorithm;
    ObIVFMergeProcessor processor;
    ObIVFFullBuildPlanner planner;
    const ObIVFTaskPlan *task_plan = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        processor.init(allocator, input, algorithm, algorithm, ObIVFProcessType::INITIAL_BUILD));
    ASSERT_EQ(OB_SUCCESS, planner.init(
        allocator, processor, ObIVFProcessType::INITIAL_BUILD));
    ASSERT_EQ(OB_SUCCESS, planner.start(task_plan));
    ASSERT_NE(nullptr, task_plan);
    ASSERT_EQ(OB_SUCCESS, planner.process_task(*task_plan, 0));
    EXPECT_EQ(&vector_store, algorithm.root_first_level_samples_);
  }

  {
    TestFullRebuildAlgorithm algorithm;
    ObIVFMergeProcessor processor;
    ObIVFFullBuildPlanner planner;
    const ObIVFTaskPlan *task_plan = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        processor.init(allocator, input, algorithm, algorithm));
    ASSERT_EQ(OB_SUCCESS, planner.init(
        allocator, processor, ObIVFProcessType::FULL_REBUILD));
    ASSERT_EQ(OB_SUCCESS, planner.start(task_plan));
    ASSERT_NE(nullptr, task_plan);
    ASSERT_EQ(OB_SUCCESS, planner.process_task(*task_plan, 0));
    EXPECT_EQ(&vector_store, algorithm.root_first_level_samples_);
  }
}

TEST(TestIVFInitialBuildDag, start_with_runtime_uses_test_owned_adapters)
{
  ObArenaAllocator allocator(
      "IVFMergeEntry", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  TestFullRebuildInput vector_store;
  ObIVFMergeInput input;
  TestFullRebuildAlgorithm algorithm;
  ASSERT_EQ(OB_SUCCESS,
      input.init(vector_store, vector_store, 1,
          true /* vectors_normalized */, true /* vectors_rotated */, false /* enable_soar */));

  ObTabletMergeDagParam merge_param(
      MAJOR_MERGE, ObLSID(1), ObTabletID(1001), -1);
  merge_param.merge_version_ = 100;
  merge_param.skip_get_tablet_ = true;
  merge_param.is_use_async_index_dag_ = true;
  merge_param.data_tablet_id_ = ObTabletID(1000);
  ObAsyncIndexMergeCtx merge_ctx(merge_param, allocator);
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, prepare_async_ivf_schema(allocator, storage_schema));
  ASSERT_TRUE(storage_schema.is_async_ivf_index());
  merge_ctx.static_param_.schema_ = &storage_schema;

  ObAsyncIndexMajorMergeDag owner_dag;
  ASSERT_EQ(OB_SUCCESS, owner_dag.basic_init(allocator));
  ObFakeTask *prepare_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, owner_dag.alloc_task(prepare_task));
  ASSERT_NE(nullptr, prepare_task);

  ASSERT_EQ(OB_INVALID_ARGUMENT,
      owner_dag.generate_merge_task(merge_ctx, prepare_task));
  ASSERT_FALSE(owner_dag.ivf_merge_dag_ctx_.is_inited_);
  ASSERT_EQ(OB_SUCCESS,
      owner_dag.ivf_merge_dag_ctx_.start_with_runtime(
          owner_dag, merge_ctx, input, algorithm, algorithm,
          *prepare_task));
  ASSERT_TRUE(owner_dag.ivf_merge_dag_ctx_.is_inited_);
  ASSERT_NO_FATAL_FAILURE(run_materialized_full_rebuild(
      owner_dag.ivf_merge_dag_ctx_, *prepare_task));

  EXPECT_EQ(&vector_store, algorithm.root_first_level_samples_);
  EXPECT_TRUE(ObIVFMergeTestPeer::are_quant_intermediates_released(
      owner_dag.ivf_merge_dag_ctx_.merge_processor_));
  EXPECT_EQ(1, algorithm.quantize_count_);
  EXPECT_EQ(1, algorithm.quantization_finish_count_);
  EXPECT_EQ(1, algorithm.raw_write_count_);
  EXPECT_EQ(1, algorithm.raw_finish_count_);
  const ObIVFDagArtifact *artifact = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      owner_dag.ivf_merge_dag_ctx_.planner_->get_artifact(artifact));
  const ObIVFMergeArtifact *full_build_artifact =
      static_cast<const ObIVFMergeArtifact *>(artifact);
  ASSERT_NE(nullptr, full_build_artifact);
  EXPECT_EQ(1, full_build_artifact->vector_count_);
  EXPECT_EQ(1, full_build_artifact->cluster_count_);
}

TEST(TestIVFInitialBuildDag, cancellation_defers_shared_runtime_teardown)
{
  ObArenaAllocator allocator(
      "IVFMergeSuspend", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  TestFullRebuildInput vector_store;
  ObIVFMergeInput input;
  SuspendFullRebuildAlgorithm algorithm;
  ObIVFMergeProcessor processor;
  ObIVFFullBuildPlanner planner;
  ASSERT_EQ(OB_SUCCESS,
      input.init(vector_store, vector_store, 1,
          true /* vectors_normalized */, true /* vectors_rotated */, false /* enable_soar */));
  ASSERT_EQ(OB_SUCCESS,
      processor.init(allocator, input, algorithm, algorithm));
  ASSERT_EQ(OB_SUCCESS, planner.init(
      allocator, processor, ObIVFProcessType::FULL_REBUILD));
  const ObIVFTaskPlan *task_plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(task_plan));
  ASSERT_NE(nullptr, task_plan);

  ObTabletMergeDagParam merge_param;
  ObAsyncIndexMergeCtx merge_ctx(merge_param, allocator);
  ObAsyncIndexMajorMergeDag owner_dag;
  ASSERT_EQ(OB_SUCCESS, owner_dag.basic_init(allocator));
  ObIVFMergeDagCtx dag_ctx;
  dag_ctx.is_inited_ = true;
  dag_ctx.process_type_ = ObIVFProcessType::FULL_REBUILD;
  dag_ctx.owner_dag_ = &owner_dag;
  dag_ctx.merge_ctx_ = &merge_ctx;
  dag_ctx.planner_ = &planner;

  ASSERT_EQ(OB_ERR_UNEXPECTED, dag_ctx.process_task(*task_plan, 0));
  EXPECT_TRUE(ObIVFMergeTestPeer::is_canceled(processor));
  EXPECT_TRUE(ObIVFMergeTestPeer::has_active_plan(processor));
  EXPECT_EQ(0, algorithm.cancel_count_);
  EXPECT_EQ(OB_SUCCESS, algorithm.cancel_ret_);
}

TEST(TestIVFInitialBuildDag, runtime_root_borrows_all_vids)
{
  ObArenaAllocator allocator(
      "IVFRootView", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  TestFullRebuildInput vector_store;
  ObIVFMergeInput input;
  TestFullRebuildAlgorithm algorithm;
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS,
      input.init(vector_store, vector_store, 1,
          true /* vectors_normalized */, true /* vectors_rotated */, false /* enable_soar */));
  ASSERT_EQ(OB_SUCCESS,
      processor.init(allocator, input, algorithm, algorithm));

  EXPECT_TRUE(ObIVFMergeTestPeer::root_uses_identity_view(processor));
  EXPECT_EQ(0,
      ObIVFMergeTestPeer::get_root_owned_vid_count(processor));
  EXPECT_EQ(0,
      ObIVFMergeTestPeer::get_root_owned_vector_idx_count(processor));
  ObIVFVectorVID vid = 0;
  int64_t vector_idx = -1;
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::get_root_vector_ref(
      processor, 0, vid, vector_idx));
  EXPECT_EQ(TestFullRebuildInput::VID, vid);
  EXPECT_EQ(0, vector_idx);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObIVFMergeTestPeer::get_root_vector_ref(
          processor, 1, vid, vector_idx));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
