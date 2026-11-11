/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <vector>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include "lib/allocator/page_arena.h"

#define private public
#define protected public
#include "storage/blocksstable/vector_index/ob_ivf_merge_runtime.h"
#include "storage/compaction/ob_async_index_merge_task.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_dag.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace compaction;
using namespace share;
using namespace storage;

struct TestStepSpec
{
  ObIVFMergeStage stage_;
  ObIVFMergeStep step_;
  int64_t depth_;
  int64_t task_count_;
};

static const TestStepSpec TEST_STEP_SPECS[] = {
  {ObIVFMergeStage::HIERARCHY,
      ObIVFMergeStep::HIERARCHY_CLUSTER, 0, 1},
  {ObIVFMergeStage::HIERARCHY,
      ObIVFMergeStep::HIERARCHY_ASSIGN, 0, 3},
  {ObIVFMergeStage::HIERARCHY,
      ObIVFMergeStep::HIERARCHY_UPDATE, 0, 3},
  {ObIVFMergeStage::HIERARCHY,
      ObIVFMergeStep::HIERARCHY_CLUSTER, 1, 1},
  {ObIVFMergeStage::HIERARCHY,
      ObIVFMergeStep::HIERARCHY_ASSIGN, 1, 3},
  {ObIVFMergeStage::HIERARCHY,
      ObIVFMergeStep::HIERARCHY_UPDATE, 1, 3},
  {ObIVFMergeStage::NEIGHBORHOOD,
      ObIVFMergeStep::NEIGHBORHOOD_ADD,
      IVF_MERGE_INVALID_DEPTH, 3},
  {ObIVFMergeStage::NEIGHBORHOOD,
      ObIVFMergeStep::NEIGHBORHOOD_BUILD,
      IVF_MERGE_INVALID_DEPTH, 3},
  {ObIVFMergeStage::REFINE,
      ObIVFMergeStep::REFINE_TRAIN,
      IVF_MERGE_INVALID_DEPTH, 1},
  {ObIVFMergeStage::REFINE,
      ObIVFMergeStep::REFINE_ASSIGN,
      IVF_MERGE_INVALID_DEPTH, 3},
  {ObIVFMergeStage::REFINE,
      ObIVFMergeStep::REFINE_UPDATE,
      IVF_MERGE_INVALID_DEPTH, 3},
  {ObIVFMergeStage::REFINE,
      ObIVFMergeStep::REFINE_COMPACT,
      IVF_MERGE_INVALID_DEPTH, 1},
  {ObIVFMergeStage::SOAR,
      ObIVFMergeStep::SOAR_ASSIGN,
      IVF_MERGE_INVALID_DEPTH, 2},
  {ObIVFMergeStage::QUANT,
      ObIVFMergeStep::QUANT,
      IVF_MERGE_INVALID_DEPTH, 3},
  {ObIVFMergeStage::RAW_WRITE,
      ObIVFMergeStep::RAW_WRITE,
      IVF_MERGE_INVALID_DEPTH, 1}
};

static const int64_t TEST_STEP_COUNT = ARRAYSIZEOF(TEST_STEP_SPECS);

TEST(TestIVFMergeOperation, centroid_index_uses_clustering_steps_and_depths)
{
  EXPECT_STREQ("CENTROID_INDEX", ivf_merge_stage_name(ObIVFMergeStage::CENTROID_INDEX));
  EXPECT_TRUE(is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX,
      ObIVFMergeStep::HIERARCHY_CLUSTER, 0));
  EXPECT_TRUE(is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX,
      ObIVFMergeStep::HIERARCHY_ASSIGN, 2));
  EXPECT_FALSE(is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX,
      ObIVFMergeStep::HIERARCHY_ASSIGN, IVF_MERGE_INVALID_DEPTH));
  EXPECT_TRUE(is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX,
      ObIVFMergeStep::REFINE_TRAIN, IVF_MERGE_INVALID_DEPTH));
  EXPECT_TRUE(is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX,
      ObIVFMergeStep::SOAR_ASSIGN, IVF_MERGE_INVALID_DEPTH));
  EXPECT_FALSE(is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX,
      ObIVFMergeStep::SOAR_ASSIGN, 0));
  EXPECT_FALSE(is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX,
      ObIVFMergeStep::QUANT, IVF_MERGE_INVALID_DEPTH));
  EXPECT_FALSE(is_valid_ivf_merge_operation(ObIVFMergeStage::CENTROID_INDEX,
      ObIVFMergeStep::RAW_WRITE, IVF_MERGE_INVALID_DEPTH));
}

class TestIVFMergeProcessor final
  : public ObIVFMergeProcessor
{
public:
  explicit TestIVFMergeProcessor(
      const int64_t fail_task_idx = -1,
      const bool suspend_once = false,
      const int64_t fail_prepare_plan = -1)
    : fail_prepare_plan_(fail_prepare_plan),
      fail_task_idx_(fail_task_idx),
      suspend_once_(suspend_once),
      execute_count_(0),
      cancel_count_(0),
      cancel_code_(OB_SUCCESS)
  {}

  virtual int prepare_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      const int64_t plan_id,
      int64_t &task_count) override
  {
    int ret = OB_SUCCESS;
    task_count = 0;
    if (plan_id == fail_prepare_plan_) {
      ret = OB_ERR_UNEXPECTED;
    } else if (plan_id < 0 || plan_id >= TEST_STEP_COUNT) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      const TestStepSpec &spec = TEST_STEP_SPECS[plan_id];
      if (stage != spec.stage_ || step != spec.step_ || depth != spec.depth_) {
        ret = OB_STATE_NOT_MATCH;
      } else {
        task_count = spec.task_count_;
      }
    }
    return ret;
  }

  virtual int execute_task(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      const int64_t task_idx,
      ObIVFTaskResult &task_result) override
  {
    int ret = OB_SUCCESS;
    ++execute_count_;
    const int64_t plan_id = task_result.get_plan_id();
    if (suspend_once_ && 1 == execute_count_) {
      ret = OB_EAGAIN;
    } else if (task_idx == fail_task_idx_) {
      ret = OB_ERR_UNEXPECTED;
    } else if (plan_id < 0 || plan_id >= TEST_STEP_COUNT
        || !task_result.is_valid() || task_result.is_sealed()) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      const TestStepSpec &spec = TEST_STEP_SPECS[plan_id];
      if (stage != spec.stage_ || step != spec.step_ || depth != spec.depth_) {
        ret = OB_STATE_NOT_MATCH;
      }
    }
    return ret;
  }

  virtual int finish_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      bool &hierarchy_finished) override
  {
    int ret = OB_SUCCESS;
    hierarchy_finished = false;
    if (ObIVFMergeStep::HIERARCHY_UPDATE == step) {
      hierarchy_finished = depth >= 1;
    }
    if (stage <= ObIVFMergeStage::INVALID
        || step <= ObIVFMergeStep::INVALID) {
      ret = OB_STATE_NOT_MATCH;
    }
    return ret;
  }

  virtual int build_final_artifact(
      ObIVFMergeArtifact &final_artifact) override
  {
    final_artifact.finalized_ = true;
    final_artifact.vector_count_ = 1;
    final_artifact.cluster_count_ = 1;
    final_artifact.secondary_assignment_count_ = 0;
    return OB_SUCCESS;
  }

  virtual void cancel(const int ret_code) override
  {
    ++cancel_count_;
    cancel_code_ = ret_code;
  }

  int64_t fail_prepare_plan_;
  int64_t fail_task_idx_;
  bool suspend_once_;
  int64_t execute_count_;
  int64_t cancel_count_;
  int cancel_code_;
};

TEST(TestIVFDagPlanner, task_plan_and_result_contract)
{
  ObIVFTaskPlan task_plan;
  ASSERT_EQ(OB_SUCCESS, task_plan.init(
      ObIVFProcessType::FULL_REBUILD, 7, 3));
  ASSERT_TRUE(task_plan.is_valid());
  ASSERT_EQ(7, task_plan.get_plan_id());
  ASSERT_EQ(3, task_plan.get_task_count());
  ASSERT_TRUE(task_plan.is_valid_task_idx(0));
  ASSERT_FALSE(task_plan.is_valid_task_idx(3));

  ObIVFTaskResult task_result;
  ASSERT_EQ(OB_SUCCESS, task_result.init(task_plan, 0));
  ASSERT_TRUE(task_result.is_valid());
  ASSERT_EQ(OB_SUCCESS, task_result.seal());
  ASSERT_TRUE(task_result.is_sealed());
  ASSERT_EQ(OB_STATE_NOT_MATCH, task_result.seal());

  ObIVFTaskPlan invalid_task_plan;
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_task_plan.init(
      ObIVFProcessType::FULL_REBUILD, 0, 0));
}

TEST(TestIVFDagPlanner, full_rebuild_runs_through_common_interface)
{
  ObArenaAllocator allocator("IVFPlannerTest");
  TestIVFMergeProcessor processor;
  ObIVFFullBuildPlanner merge_planner;
  ASSERT_EQ(OB_SUCCESS, processor.init());
  ASSERT_EQ(OB_SUCCESS,
      merge_planner.init(
          allocator, processor, ObIVFProcessType::FULL_REBUILD));
  ObIVFDagPlanner &planner = merge_planner;

  const ObIVFTaskPlan *task_plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(task_plan));
  for (int64_t plan_id = 0; plan_id < TEST_STEP_COUNT; ++plan_id) {
    const TestStepSpec &spec = TEST_STEP_SPECS[plan_id];
    ASSERT_NE(nullptr, task_plan);
    ASSERT_EQ(ObIVFProcessType::FULL_REBUILD,
        task_plan->get_process_type());
    ASSERT_EQ(plan_id, task_plan->get_plan_id());
    ASSERT_EQ(spec.task_count_, task_plan->get_task_count());
    for (int64_t task_idx = 0; task_idx < spec.task_count_; ++task_idx) {
      ASSERT_EQ(OB_SUCCESS, planner.process_task(*task_plan, task_idx));
    }
    const ObIVFTaskPlan *next_task_plan = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        planner.finish_task_plan(*task_plan, next_task_plan));
    if (plan_id + 1 == TEST_STEP_COUNT) {
      ASSERT_EQ(nullptr, next_task_plan);
    } else {
      ASSERT_NE(nullptr, next_task_plan);
      ASSERT_GT(next_task_plan->get_plan_id(), task_plan->get_plan_id());
    }
    task_plan = next_task_plan;
  }

  const ObIVFDagArtifact *dag_artifact = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      merge_planner.get_artifact(dag_artifact));
  const ObIVFMergeArtifact *artifact =
      static_cast<const ObIVFMergeArtifact *>(dag_artifact);
  ASSERT_NE(nullptr, artifact);
  ASSERT_TRUE(artifact->is_valid());
  ASSERT_EQ(1, artifact->vector_count_);
  ASSERT_EQ(1, artifact->cluster_count_);
  ASSERT_EQ(0, processor.cancel_count_);
}

class TestIVFPerfObserver final : public ObIVFMergePerfObserver
{
public:
  virtual void on_step_perf(const ObIVFMergeStepPerf &perf) override
  {
    snapshots_.push_back(perf);
  }
  std::vector<ObIVFMergeStepPerf> snapshots_;
};

TEST(TestIVFDagPlanner, perf_closes_each_step_before_preparing_next)
{
  ObArenaAllocator allocator("IVFPerfTest");
  TestIVFMergeProcessor processor;
  ObIVFFullBuildPlanner planner;
  TestIVFPerfObserver observer;
  ASSERT_EQ(OB_SUCCESS, processor.init());
  ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, ObIVFProcessType::INITIAL_BUILD));
  planner.set_perf_observer(&observer);
  const ObIVFTaskPlan *plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(plan));
  for (int64_t i = 0; i < TEST_STEP_COUNT; ++i) {
    ASSERT_NE(nullptr, plan);
    for (int64_t task = 0; task < plan->get_task_count(); ++task) {
      ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, task));
    }
    const ObIVFTaskPlan *next = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
    plan = next;
  }
  ASSERT_EQ(nullptr, plan);
  ASSERT_EQ(TEST_STEP_COUNT * 4, observer.snapshots_.size());
  for (int64_t i = 0; i < TEST_STEP_COUNT; ++i) {
    const ObIVFMergeStepPerf &begin = observer.snapshots_.at(i * 4);
    const ObIVFMergeStepPerf &prepared = observer.snapshots_.at(i * 4 + 1);
    const ObIVFMergeStepPerf &finishing = observer.snapshots_.at(i * 4 + 2);
    const ObIVFMergeStepPerf &end = observer.snapshots_.at(i * 4 + 3);
    EXPECT_EQ(i, begin.plan_id_);
    EXPECT_EQ(TEST_STEP_SPECS[i].step_, end.step_);
    EXPECT_EQ(TEST_STEP_SPECS[i].depth_, end.depth_);
    EXPECT_EQ(TEST_STEP_SPECS[i].task_count_, end.task_count_);
    EXPECT_GT(begin.start_us_, 0);
    EXPECT_EQ(0, begin.prepared_us_);
    EXPECT_EQ(0, finishing.end_us_);
    EXPECT_GE(prepared.prepared_us_, begin.start_us_);
    EXPECT_GE(finishing.finish_begin_us_, prepared.prepared_us_);
    EXPECT_GE(end.end_us_, finishing.finish_begin_us_);
    EXPECT_EQ(OB_SUCCESS, end.ret_code_);
    if (i + 1 < TEST_STEP_COUNT) {
      EXPECT_GE(observer.snapshots_.at((i + 1) * 4).start_us_, end.end_us_);
    }
  }
}

TEST(TestIVFDagPlanner, perf_preserves_failed_prepare)
{
  ObArenaAllocator allocator("IVFPerfFailure");
  TestIVFMergeProcessor processor(-1, false, 0);
  ObIVFFullBuildPlanner planner;
  TestIVFPerfObserver observer;
  ASSERT_EQ(OB_SUCCESS, processor.init());
  ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, ObIVFProcessType::INITIAL_BUILD));
  planner.set_perf_observer(&observer);
  const ObIVFTaskPlan *plan = nullptr;
  ASSERT_EQ(OB_ERR_UNEXPECTED, planner.start(plan));
  ASSERT_EQ(nullptr, plan);
  ASSERT_EQ(2, observer.snapshots_.size());
  const ObIVFMergeStepPerf &end = observer.snapshots_.at(1);
  EXPECT_EQ(OB_ERR_UNEXPECTED, end.ret_code_);
  EXPECT_EQ(end.prepared_us_, end.end_us_);
  EXPECT_EQ(0, end.finish_begin_us_);
  EXPECT_EQ(1, processor.cancel_count_);
}

TEST(TestIVFDagPlanner, perf_keeps_step_open_across_task_suspension)
{
  ObArenaAllocator allocator("IVFPerfSuspend");
  TestIVFMergeProcessor processor(-1, true);
  ObIVFFullBuildPlanner planner;
  TestIVFPerfObserver observer;
  ASSERT_EQ(OB_SUCCESS, processor.init());
  ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, ObIVFProcessType::INITIAL_BUILD));
  planner.set_perf_observer(&observer);
  const ObIVFTaskPlan *plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(plan));
  ASSERT_NE(nullptr, plan);
  const int64_t start_us = observer.snapshots_.at(0).start_us_;
  ASSERT_EQ(OB_EAGAIN, planner.process_task(*plan, 0));
  ASSERT_EQ(2, observer.snapshots_.size());
  for (int64_t task = 0; task < plan->get_task_count(); ++task) {
    ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, task));
  }
  const ObIVFTaskPlan *next = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
  ASSERT_EQ(6, observer.snapshots_.size()); // completed step, then next prepare
  EXPECT_EQ(start_us, observer.snapshots_.at(3).start_us_);
  EXPECT_GT(observer.snapshots_.at(3).end_us_, 0);
  EXPECT_EQ(OB_SUCCESS, observer.snapshots_.at(3).ret_code_);
  EXPECT_EQ(1, observer.snapshots_.at(4).plan_id_);
}


TEST(TestIVFDagPlanner, cluster_gather_barrier_precedes_training_and_assignment)
{
  const auto hierarchy = ObIVFMergeStage::HIERARCHY;
  const auto sample_step = ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE;
  EXPECT_STREQ("HIERARCHY_SAMPLE_PREPARE", ivf_merge_step_name(sample_step));
  EXPECT_TRUE(is_valid_ivf_merge_operation(hierarchy, sample_step, 1));
  EXPECT_FALSE(is_valid_ivf_merge_operation(hierarchy, sample_step, 0));
  EXPECT_FALSE(is_valid_ivf_merge_operation(ObIVFMergeStage::REFINE, sample_step, 1));
  for (const bool needs_samples : {false, true}) {
    ObIVFFullRebuildState state;
    ObIVFMergeStepResult result;
    ObIVFMergeStage stage;
    ObIVFMergeStep step;
    int64_t depth = -1;
    for (int64_t level = 0; level < 3; ++level) {
      state.get_operation(stage, step, depth);
      EXPECT_EQ(hierarchy, stage);
      EXPECT_EQ(level, depth);
      if (needs_samples && level > 0) {
        EXPECT_EQ(sample_step, step);
        ASSERT_EQ(OB_SUCCESS, state.advance(result)); // gather barrier
        state.get_operation(stage, step, depth);
        EXPECT_EQ(level, depth);
      }
      EXPECT_EQ(ObIVFMergeStep::HIERARCHY_CLUSTER, step);
      ASSERT_EQ(OB_SUCCESS, state.advance(result)); // training barrier
      state.get_operation(stage, step, depth);
      EXPECT_EQ(ObIVFMergeStep::HIERARCHY_ASSIGN, step);
      ASSERT_EQ(OB_SUCCESS, state.advance(result)); // assign barrier
      state.get_operation(stage, step, depth);
      EXPECT_EQ(ObIVFMergeStep::HIERARCHY_UPDATE, step);
      result.next_cluster_needs_samples_ = needs_samples;
      ASSERT_EQ(OB_SUCCESS, state.advance(result)); // next depth
      result.next_cluster_needs_samples_ = false;
    }
  }
}

TEST(TestIVFDagPlanner, refine_train_stays_at_barrier_until_last_batch)
{
  ObIVFFullRebuildState state;
  ObIVFMergeStepResult result;
  result.hierarchy_finished_ = true;
  result.neighborhood_mode_ = ObIVFNeighborhoodMode::SKIP;
  ASSERT_EQ(OB_SUCCESS, state.advance(result)); // cluster -> assign
  ASSERT_EQ(OB_SUCCESS, state.advance(result)); // assign -> update
  ASSERT_EQ(OB_SUCCESS, state.advance(result)); // update -> refine
  ObIVFMergeStage stage;
  ObIVFMergeStep step;
  int64_t depth;
  result.refine_train_finished_ = false;
  for (int64_t batch = 0; batch < 3; ++batch) {
    state.get_operation(stage, step, depth);
    EXPECT_EQ(ObIVFMergeStep::REFINE_TRAIN, step);
    ASSERT_EQ(OB_SUCCESS, state.advance(result));
  }
  result.refine_train_finished_ = true;
  ASSERT_EQ(OB_SUCCESS, state.advance(result));
  state.get_operation(stage, step, depth);
  EXPECT_EQ(ObIVFMergeStep::REFINE_ASSIGN, step);
}

TEST(TestIVFDagPlanner, initial_build_preserves_process_type)
{
  ObArenaAllocator allocator("IVFInitialBuild");
  TestIVFMergeProcessor processor;
  ObIVFFullBuildPlanner planner;
  const ObIVFTaskPlan *task_plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, processor.init());
  ASSERT_EQ(OB_SUCCESS, planner.init(
      allocator, processor, ObIVFProcessType::INITIAL_BUILD));
  ASSERT_EQ(OB_SUCCESS, planner.start(task_plan));
  ASSERT_NE(nullptr, task_plan);
  ASSERT_EQ(ObIVFProcessType::INITIAL_BUILD,
      task_plan->get_process_type());
  ObIVFFullBuildPlanner second_planner;
  EXPECT_EQ(OB_INIT_TWICE, second_planner.init(
      allocator, processor, ObIVFProcessType::FULL_REBUILD));
}

TEST(TestIVFDagPlanner, rejects_incomplete_task_plan)
{
  ObArenaAllocator allocator("IVFIncomplete");
  TestIVFMergeProcessor processor;
  ObIVFFullBuildPlanner planner;
  ASSERT_EQ(OB_SUCCESS, processor.init());
  ASSERT_EQ(OB_SUCCESS, planner.init(
      allocator, processor, ObIVFProcessType::FULL_REBUILD));
  const ObIVFTaskPlan *task_plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(task_plan));
  ASSERT_EQ(OB_SUCCESS, planner.process_task(*task_plan, 0));
  ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*task_plan, task_plan));
  ASSERT_EQ(3, task_plan->get_task_count());
  ASSERT_EQ(OB_SUCCESS, planner.process_task(*task_plan, 0));
  const ObIVFTaskPlan *next_task_plan = nullptr;
  ASSERT_EQ(OB_STATE_NOT_MATCH,
      planner.finish_task_plan(*task_plan, next_task_plan));
  ASSERT_EQ(nullptr, next_task_plan);
  ASSERT_EQ(1, processor.cancel_count_);
}

TEST(TestIVFDagPlanner, cancels_task_failure_but_not_suspend)
{
  ObArenaAllocator fail_allocator("IVFFail");
  TestIVFMergeProcessor fail_processor(0 /* fail_task_idx */);
  ObIVFFullBuildPlanner fail_planner;
  ASSERT_EQ(OB_SUCCESS, fail_processor.init());
  ASSERT_EQ(OB_SUCCESS,
      fail_planner.init(fail_allocator, fail_processor,
          ObIVFProcessType::FULL_REBUILD));
  const ObIVFTaskPlan *task_plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, fail_planner.start(task_plan));
  ASSERT_EQ(OB_ERR_UNEXPECTED, fail_planner.process_task(*task_plan, 0));
  ASSERT_EQ(1, fail_processor.cancel_count_);
  ASSERT_EQ(OB_ERR_UNEXPECTED, fail_processor.cancel_code_);

  ObArenaAllocator suspend_allocator("IVFSuspend");
  TestIVFMergeProcessor suspend_processor(
      -1 /* fail_task_idx */, true /* suspend_once */);
  ObIVFFullBuildPlanner suspend_planner;
  ASSERT_EQ(OB_SUCCESS, suspend_processor.init());
  ASSERT_EQ(OB_SUCCESS,
      suspend_planner.init(suspend_allocator, suspend_processor,
          ObIVFProcessType::FULL_REBUILD));
  task_plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, suspend_planner.start(task_plan));
  ASSERT_EQ(OB_EAGAIN, suspend_planner.process_task(*task_plan, 0));
  ASSERT_EQ(0, suspend_processor.cancel_count_);
  ASSERT_EQ(OB_SUCCESS, suspend_planner.process_task(*task_plan, 0));
}

TEST(TestIVFDagPlanner, dag_ctx_defers_runtime_planner_initialization)
{
  const ObTabletID tablet_id(1001);
  ObFakeTask parent_task;
  ObColumnClusteredDag owner_dag;
  owner_dag.is_inited_ = true;
  ObIVFDDLDagCtx initial_build_dag_ctx;
  ASSERT_EQ(OB_SUCCESS, initial_build_dag_ctx.init(
      owner_dag, ObIVFProcessType::INITIAL_BUILD, tablet_id));
  ASSERT_TRUE(initial_build_dag_ctx.is_inited_);
  ASSERT_FALSE(initial_build_dag_ctx.runtime_initialized_);
  ASSERT_EQ(tablet_id, initial_build_dag_ctx.tablet_id_);
  ASSERT_EQ(nullptr, initial_build_dag_ctx.full_build_planner_);
  ASSERT_EQ(nullptr, initial_build_dag_ctx.planner_);

  ObIVFDDLDagCtx refresh_dag_ctx;
  ASSERT_EQ(OB_NOT_SUPPORTED, refresh_dag_ctx.init(
      owner_dag, ObIVFProcessType::REFRESH, tablet_id));
  ASSERT_EQ(nullptr, refresh_dag_ctx.planner_);
  ASSERT_FALSE(refresh_dag_ctx.is_inited_);
  ASSERT_EQ(OB_NOT_INIT, refresh_dag_ctx.start(parent_task));

  ObIVFDDLDagCtx invalid_dag_ctx;
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_dag_ctx.init(
      owner_dag, ObIVFProcessType::INVALID, tablet_id));
  ASSERT_EQ(OB_NOT_INIT, invalid_dag_ctx.start(parent_task));

}

TEST(TestIVFDagPlanner, phase_results_do_not_accumulate_in_caller_arena)
{
  class ResultPlanner final : public ObIVFDagPlanner
  {
  public:
    class Artifact final : public ObIVFDagArtifact
    {
    public:
      bool is_valid() const override { return true; }
    } artifact_;
    int init(ObIAllocator &allocator, ObIVFDagProcessor &processor, ObIVFProcessType type)
    { return ObIVFDagPlanner::init(allocator, type, processor); }
    int prepare_first_task_plan(int64_t, int64_t &count) override
    { count = 4096; return OB_SUCCESS; }
    int execute_process_task(const ObIVFTaskPlan &, int64_t, ObIVFTaskResult &) override
    { return OB_SUCCESS; }
    int finish_process_task_plan(const ObIVFTaskPlan &, const ObIArray<ObIVFTaskResult> &,
        int64_t next_id, bool &finished, int64_t &count) override
    {
      finished = next_id == 16;
      count = next_id % 2 == 0 ? 4096 : 1;
      return finished ? set_artifact(artifact_) : OB_SUCCESS;
    }
  };
  for (const auto type : {ObIVFProcessType::INITIAL_BUILD, ObIVFProcessType::FULL_REBUILD}) {
    ObArenaAllocator allocator("IVFResultUT");
    ObIVFDagProcessor processor;
    ResultPlanner planner;
    ASSERT_EQ(OB_SUCCESS, processor.init());
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, type));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    for (int64_t phase = 0; phase < 16; ++phase) {
      ASSERT_NE(nullptr, plan);
      ASSERT_EQ(phase, plan->get_plan_id());
      if (phase % 2 != 0) {
        // A one-task phase must not keep the previous 4096-task allocation.
        EXPECT_LT(planner.task_result_allocator_.total(), 64L * 1024);
      }
      for (int64_t task = 0; task < plan->get_task_count(); ++task) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, task));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
      plan = next;
      // The caller arena retains only small immutable plan headers.
      EXPECT_LT(allocator.used(), 64L * 1024);
    }
    EXPECT_EQ(nullptr, plan);
    EXPECT_EQ(0, planner.task_result_allocator_.total());
    EXPECT_EQ(0, planner.task_results_.count());
  }
}

TEST(TestIVFDagPlanner, repeated_start_preserves_existing_runtime)
{
  const ObTabletID tablet_id(1001);
  ObFakeTask prepare_task;
  ObColumnClusteredDag owner_dag;
  owner_dag.is_inited_ = true;
  ObIVFFullBuildPlanner existing_planner;
  ObIVFDDLDagCtx dag_ctx;
  ASSERT_EQ(OB_SUCCESS, dag_ctx.init(
      owner_dag, ObIVFProcessType::INITIAL_BUILD, tablet_id));
  dag_ctx.runtime_initialized_ = true;
  dag_ctx.planner_ = &existing_planner;

  ASSERT_EQ(OB_INIT_TWICE, dag_ctx.start(prepare_task));
  EXPECT_TRUE(dag_ctx.runtime_initialized_);
  EXPECT_EQ(&existing_planner, dag_ctx.planner_);
}

TEST(TestIVFDagPlanner, merge_lazy_fanout_preserves_barrier)
{
  ObArenaAllocator allocator("IVFFanout");
  ObIVFTaskPlan task_plan;
  ASSERT_EQ(OB_SUCCESS, task_plan.init(
      ObIVFProcessType::FULL_REBUILD, 9, 3));
  ObAsyncIndexMajorMergeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ObTabletMergeDagParam merge_param;
  ObAsyncIndexMergeCtx merge_ctx(merge_param, allocator);
  ObIVFMergeDagCtx dag_ctx;
  dag_ctx.is_inited_ = true;
  dag_ctx.process_type_ = ObIVFProcessType::FULL_REBUILD;
  dag_ctx.owner_dag_ = &dag;
  dag_ctx.merge_ctx_ = &merge_ctx;

  ObFakeTask *parent_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(parent_task));
  ASSERT_EQ(OB_SUCCESS,
      dag_ctx.materialize_task_plan(*parent_task, task_plan));
  ASSERT_EQ(1, parent_task->get_child_nodes().count());
  ObIVFMergeTask *task0 = static_cast<ObIVFMergeTask *>(
      parent_task->get_child_nodes().at(0));
  ObIVFMergeFinishTask *finish_task =
      static_cast<ObIVFMergeFinishTask *>(task0->get_child_nodes().at(0));
  ASSERT_EQ(1, finish_task->get_indegree());

  ObITask *task1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, task0->generate_next_task(task1));
  ASSERT_EQ(OB_SUCCESS, task0->copy_children_to(*task1));
  ASSERT_EQ(2, finish_task->get_indegree());
  ObITask *task2 = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObIVFMergeTask *>(task1)->generate_next_task(task2));
  ASSERT_EQ(OB_SUCCESS, task1->copy_children_to(*task2));
  ASSERT_EQ(3, finish_task->get_indegree());
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_ITER_END,
      static_cast<ObIVFMergeTask *>(task2)->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
}

TEST(TestIVFDagPlanner, ddl_materializes_parallel_tasks_and_barrier)
{
  ObArenaAllocator allocator("IVFDDLGraph");
  ObIVFTaskPlan task_plan;
  ASSERT_EQ(OB_SUCCESS, task_plan.init(
      ObIVFProcessType::INITIAL_BUILD, 11, 2));
  ObColumnClusteredDag owner_dag;
  ASSERT_EQ(OB_SUCCESS, owner_dag.ObIDag::basic_init(allocator));
  owner_dag.is_inited_ = true;
  owner_dag.ddl_thread_count_ = 2;
  owner_dag.px_thread_count_ = 2;
  ObIVFDDLDagCtx dag_ctx;
  ASSERT_EQ(OB_SUCCESS,
      dag_ctx.init(owner_dag, ObIVFProcessType::INITIAL_BUILD,
          ObTabletID(1001)));

  ObFakeTask *parent_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, owner_dag.alloc_task(parent_task));
  ASSERT_EQ(OB_SUCCESS,
      dag_ctx.materialize_task_plan(*parent_task, task_plan));
  ASSERT_EQ(2, parent_task->get_child_nodes().count());
  ObIVFDDLTask *task0 = static_cast<ObIVFDDLTask *>(
      parent_task->get_child_nodes().at(0));
  ObIVFDDLTask *task1 = static_cast<ObIVFDDLTask *>(
      parent_task->get_child_nodes().at(1));
  ASSERT_EQ(task0->get_child_nodes().at(0), task1->get_child_nodes().at(0));
  ObIVFDDLFinishTask *finish_task =
      static_cast<ObIVFDDLFinishTask *>(task0->get_child_nodes().at(0));
  ASSERT_EQ(2, finish_task->get_indegree());
}


namespace
{
class DispatchTestAllocator final : public ObIAllocator
{
public:
  DispatchTestAllocator() : arena_("IVFDagTest"), safe_(arena_),
      fail_(false), live_(0), peak_(0) {}
  void *alloc(const int64_t size) override
  {
    void *ptr = fail_.load() ? nullptr : safe_.alloc(size);
    if (nullptr != ptr) {
      const int64_t live = ++live_;
      int64_t peak = peak_.load();
      while (peak < live && !peak_.compare_exchange_weak(peak, live)) {}
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  { UNUSED(attr); return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) { --live_; safe_.free(ptr); }
  }
  ObArenaAllocator arena_;
  ObSafeArenaAllocator safe_;
  std::atomic<bool> fail_;
  std::atomic<int64_t> live_;
  std::atomic<int64_t> peak_;
};

class DispatchTestPlanner final : public ObIVFDagPlanner
{
public:
  DispatchTestPlanner(const int64_t first_count, const int64_t second_count)
    : counts_{first_count, second_count}, fail_idx_(-1), suspend_idx_(-1),
      running_(0), peak_running_(0), attempts_(new std::atomic<int64_t>[first_count + second_count])
  {
    for (int64_t i = 0; i < first_count + second_count; ++i) { attempts_[i] = 0; }
    completed_[0] = 0;
    completed_[1] = 0;
  }
  int init(ObIAllocator &allocator, ObIVFDagProcessor &processor)
  { return ObIVFDagPlanner::init(allocator, ObIVFProcessType::INITIAL_BUILD, processor); }
  int prepare_first_task_plan(const int64_t plan_id, int64_t &task_count) override
  { UNUSED(plan_id); task_count = counts_[0]; return OB_SUCCESS; }
  int execute_process_task(const ObIVFTaskPlan &plan, const int64_t idx,
      ObIVFTaskResult &result) override
  {
    UNUSED(result);
    const int64_t running = ++running_;
    int64_t peak = peak_running_.load();
    while (peak < running && !peak_running_.compare_exchange_weak(peak, running)) {}
    std::this_thread::yield();
    const int64_t offset = 0 == plan.get_plan_id() ? 0 : counts_[0];
    const int64_t attempt = ++attempts_[offset + idx];
    int ret = OB_SUCCESS;
    if (0 == plan.get_plan_id() && idx == fail_idx_) {
      ret = OB_ERR_UNEXPECTED;
    } else if (0 == plan.get_plan_id() && idx == suspend_idx_ && 1 == attempt) {
      ret = OB_EAGAIN;
    } else {
      ++completed_[plan.get_plan_id()];
    }
    --running_;
    return ret;
  }
  int finish_process_task_plan(const ObIVFTaskPlan &plan,
      const ObIArray<ObIVFTaskResult> &results, const int64_t next_plan_id,
      bool &finished, int64_t &task_count) override
  {
    UNUSEDx(results, next_plan_id);
    finished = false;
    task_count = counts_[1];
    return 0 == plan.get_plan_id() && task_count > 0 ? OB_SUCCESS : OB_ERR_UNEXPECTED;
  }
  const int64_t counts_[2];
  int64_t fail_idx_;
  int64_t suspend_idx_;
  std::atomic<int64_t> running_;
  std::atomic<int64_t> peak_running_;
  std::unique_ptr<std::atomic<int64_t>[]> attempts_;
  std::atomic<int64_t> completed_[2];
};

class DispatchTestGraph
{
public:
  DispatchTestGraph(const int64_t first_count, const int64_t second_count = 0)
    : planner_allocator_("IVFDagPlan"), planner_(first_count, second_count), plan_(nullptr) {}
  int init(const int64_t ddl_workers, const int64_t px_workers)
  {
    int ret = OB_SUCCESS;
    ObFakeTask *parent = nullptr;
    ObITask *running_parent = nullptr;
    bool suspended = false;
    owner_.ddl_thread_count_ = ddl_workers;
    owner_.px_thread_count_ = px_workers;
    owner_.is_inited_ = true;
    if (OB_FAIL(owner_.ObIDag::basic_init(allocator_))
        || OB_FAIL(owner_.cond_.init(ObWaitEventIds::INDEPENDENT_DAG_COND_WAIT))
        || OB_FAIL(ctx_.init(owner_, ObIVFProcessType::INITIAL_BUILD, ObTabletID(1001)))
        || OB_FAIL(processor_.init())
        || OB_FAIL(planner_.init(planner_allocator_, processor_))
        || OB_FAIL(planner_.start(plan_))) {
    } else {
      ctx_.planner_ = &planner_;
      owner_.set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);
      if (OB_FAIL(owner_.alloc_task(parent))
          || OB_FAIL(owner_.add_task(*parent))
          || OB_FAIL(owner_.schedule_one(running_parent))
          || OB_FAIL(ctx_.materialize_task_plan(*running_parent, *plan_))
          || OB_FAIL(owner_.deal_with_finish_task(running_parent, OB_SUCCESS, suspended))) {
      }
    }
    return ret;
  }

  // Exercise the real independent-DAG schedule/process/retire path, including
  // post-generation and retries. Leave the phase barrier scheduled for inspection.
  int drain(const int64_t thread_count, ObITask *&finish)
  {
    std::atomic<bool> done(false);
    std::atomic<int> error(OB_SUCCESS);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    std::vector<std::thread> threads;
    finish = nullptr;
    for (int64_t i = 0; i < thread_count; ++i) {
      threads.emplace_back([&]() {
        while (!done.load() && OB_SUCCESS == error.load()) {
          ObITask *task = nullptr;
          int task_ret = OB_SUCCESS;
          int ret = owner_.schedule_one(task);
          if (OB_ITER_END == ret) {
            if (std::chrono::steady_clock::now() > deadline) {
              error = OB_TIMEOUT;
            } else {
              std::this_thread::yield();
            }
          } else if (OB_SUCCESS != ret) {
            error = ret;
          } else if (ObITask::TASK_TYPE_DDL_MERGE_PREPARE == task->get_type()) {
            finish = task;
            done = true;
          } else if (OB_SUCCESS != (ret = owner_.process_task(task, task_ret))) {
            error = ret;
          } else if (OB_SUCCESS != task_ret) {
            error = task_ret;
          }
        }
      });
    }
    for (auto &thread : threads) { thread.join(); }
    return error.load();
  }
  DispatchTestAllocator allocator_;
  ObArenaAllocator planner_allocator_;
  ObIVFDagProcessor processor_;
  DispatchTestPlanner planner_;
  ObColumnClusteredDag owner_;
  ObIVFDDLDagCtx ctx_;
  const ObIVFTaskPlan *plan_;
};

class CanceledDispatchWorker final : public lib::Worker
{
public:
  int check_status() override { return OB_CANCELED; }
};

class ScopedDispatchWorker
{
public:
  explicit ScopedDispatchWorker(lib::Worker &worker) : previous_(&THIS_WORKER)
  { lib::Worker::set_worker_to_thread_local(&worker); }
  ~ScopedDispatchWorker() { lib::Worker::set_worker_to_thread_local(previous_); }
private:
  lib::Worker *previous_;
};
} // namespace

TEST(TestIVFDagPlanner, ddl_large_plan_bounds_physical_graph_by_allocated_parallelism)
{
  DispatchTestGraph graph(386030);
  ASSERT_EQ(OB_SUCCESS, graph.init(16, 16));
  EXPECT_EQ(16, graph.ctx_.next_task_idx_);
  EXPECT_EQ(17, graph.allocator_.live_.load()); // 16 work nodes plus the barrier
  EXPECT_EQ(0, graph.planner_.completed_[0].load());
  EXPECT_EQ(386030, graph.plan_->get_task_count());
}

TEST(TestIVFDagPlanner, ddl_px_helpers_do_not_double_single_tablet_parallelism)
{
  const int64_t parallelisms[] = {1, 8, 16};
  for (const int64_t parallelism : parallelisms) {
    SCOPED_TRACE(parallelism);
    DispatchTestGraph graph(67);
    ASSERT_EQ(OB_SUCCESS, graph.init(parallelism, parallelism));
    std::vector<ObITask *> running;
    for (int64_t i = 0; i < parallelism; ++i) {
      ObITask *task = nullptr;
      ASSERT_EQ(OB_SUCCESS, graph.owner_.schedule_one(task));
      ASSERT_NE(nullptr, task);
      running.push_back(task);
    }
    // All allocated lanes are occupied. Extra PX helpers cannot start a
    // second set of tasks while these tasks are still running.
    ObITask *extra = nullptr;
    EXPECT_EQ(OB_ITER_END, graph.owner_.schedule_one(extra));
    EXPECT_EQ(nullptr, extra);
    for (ObITask *task : running) {
      int task_ret = OB_SUCCESS;
      ASSERT_EQ(OB_SUCCESS, graph.owner_.process_task(task, task_ret));
      ASSERT_EQ(OB_SUCCESS, task_ret);
    }
    ObITask *finish = nullptr;
    ASSERT_EQ(OB_SUCCESS, graph.drain(2 * parallelism, finish));
    ASSERT_NE(nullptr, finish);
    EXPECT_EQ(67, graph.planner_.completed_[0].load());
    EXPECT_EQ(0, graph.planner_.running_.load());
    EXPECT_LE(graph.planner_.peak_running_.load(), parallelism);
    EXPECT_EQ(0, finish->get_indegree());
    EXPECT_EQ(1, graph.allocator_.live_.load());
    for (int64_t i = 0; i < 67; ++i) {
      EXPECT_EQ(1, graph.planner_.attempts_[i].load());
    }
  }
}

TEST(TestIVFDagPlanner, ddl_concurrent_replenishment_preserves_results_and_phase_barriers)
{
  DispatchTestGraph graph(4097, 3);
  ASSERT_EQ(OB_SUCCESS, graph.init(16, 16));
  ObITask *finish = nullptr;
  ASSERT_EQ(OB_SUCCESS, graph.drain(32, finish));
  ASSERT_NE(nullptr, finish);
  EXPECT_EQ(4097, graph.planner_.completed_[0].load());
  EXPECT_EQ(4097, graph.ctx_.next_task_idx_);
  for (int64_t i = 0; i < 4097; ++i) { ASSERT_EQ(1, graph.planner_.attempts_[i].load()); }
  EXPECT_EQ(OB_SUCCESS, graph.planner_.check_task_plan_finished(*graph.plan_));
  EXPECT_EQ(0, finish->get_indegree());
  EXPECT_EQ(1, graph.allocator_.live_.load());
  EXPECT_EQ(0, graph.planner_.running_.load());
  EXPECT_LE(graph.planner_.peak_running_.load(), 16);
  // A worker may still retire its completed node after publishing a successor.
  // Bound compute lanes plus at most one retiring node per physical worker.
  EXPECT_LE(graph.allocator_.peak_.load(), 16 + 32 + 2);

  int task_ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, graph.owner_.process_task(finish, task_ret));
  ASSERT_EQ(OB_SUCCESS, task_ret);
  EXPECT_EQ(3, graph.ctx_.next_task_idx_);
  EXPECT_EQ(4, graph.allocator_.live_.load());
  ASSERT_EQ(OB_SUCCESS, graph.drain(32, finish));
  EXPECT_EQ(3, graph.planner_.completed_[1].load());
  for (int64_t i = 4097; i < 4100; ++i) { EXPECT_EQ(1, graph.planner_.attempts_[i].load()); }
  EXPECT_EQ(0, finish->get_indegree());
  EXPECT_EQ(OB_SUCCESS, graph.planner_.check_task_plan_finished(*graph.ctx_.dispatch_task_plan_));
}

TEST(TestIVFDagPlanner, ddl_suspension_retries_same_item_before_replenishing)
{
  DispatchTestGraph graph(5);
  ASSERT_EQ(OB_SUCCESS, graph.init(1, 0));
  graph.planner_.suspend_idx_ = 0;
  ObITask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, graph.owner_.schedule_one(task));
  ASSERT_EQ(0, task->get_sub_task_id());
  const int ret = graph.owner_.execute_task(*task);
  ASSERT_EQ(OB_DAG_TASK_IS_SUSPENDED, ret);
  EXPECT_EQ(1, graph.ctx_.next_task_idx_);
  EXPECT_EQ(2, graph.allocator_.live_.load());
  bool suspended = false;
  ASSERT_EQ(OB_SUCCESS, graph.owner_.deal_with_finish_task(task, ret, suspended));
  ASSERT_TRUE(suspended);
  ObITask *retry = nullptr;
  ASSERT_EQ(OB_SUCCESS, graph.owner_.schedule_one(retry));
  ASSERT_EQ(task, retry);
  int task_ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, graph.owner_.process_task(retry, task_ret));
  ASSERT_EQ(OB_SUCCESS, task_ret);
  EXPECT_EQ(2, graph.ctx_.next_task_idx_);

  ObITask *finish = nullptr;
  ASSERT_EQ(OB_SUCCESS, graph.drain(1, finish));
  EXPECT_EQ(5, graph.planner_.completed_[0].load());
  EXPECT_EQ(2, graph.planner_.attempts_[0].load());
  for (int64_t i = 1; i < 5; ++i) { EXPECT_EQ(1, graph.planner_.attempts_[i].load()); }
}

TEST(TestIVFDagPlanner, ddl_task_failure_stops_all_replenishment)
{
  DispatchTestGraph graph(100);
  ASSERT_EQ(OB_SUCCESS, graph.init(2, 2));
  graph.planner_.fail_idx_ = 0;
  ObITask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, graph.owner_.schedule_one(task));
  ASSERT_EQ(0, task->get_sub_task_id());
  EXPECT_EQ(OB_ERR_UNEXPECTED, graph.owner_.execute_task(*task));
  EXPECT_EQ(OB_ERR_UNEXPECTED, graph.ctx_.dispatch_ret_);
  EXPECT_TRUE(graph.owner_.has_set_stop());
  ObITask *next = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, graph.ctx_.generate_next_task(*graph.plan_, next));
  EXPECT_EQ(nullptr, next);
  EXPECT_EQ(2, graph.ctx_.next_task_idx_);
  EXPECT_EQ(0, graph.planner_.completed_[0].load());
  EXPECT_EQ(OB_ITER_END, graph.owner_.schedule_one(next));
}

TEST(TestIVFDagPlanner, ddl_worker_cancel_prevents_execution_and_replenishment)
{
  DispatchTestGraph graph(100);
  ASSERT_EQ(OB_SUCCESS, graph.init(2, 2));
  ObITask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, graph.owner_.schedule_one(task));
  CanceledDispatchWorker worker;
  {
    ScopedDispatchWorker guard(worker);
    EXPECT_EQ(OB_CANCELED, graph.owner_.execute_task(*task));
  }
  EXPECT_EQ(0, graph.planner_.attempts_[0].load());
  EXPECT_EQ(2, graph.ctx_.next_task_idx_);
  EXPECT_TRUE(graph.owner_.has_set_stop());
  EXPECT_EQ(OB_CANCELED, graph.ctx_.dispatch_ret_);
  ObITask *next = nullptr;
  EXPECT_EQ(OB_CANCELED, graph.ctx_.generate_next_task(*graph.plan_, next));
  EXPECT_EQ(nullptr, next);
}

TEST(TestIVFDagPlanner, ddl_replenishment_allocation_failure_stops_without_leaking_node)
{
  DispatchTestGraph graph(100);
  ASSERT_EQ(OB_SUCCESS, graph.init(1, 0));
  ObITask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, graph.owner_.schedule_one(task));
  graph.allocator_.fail_ = true;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, graph.owner_.execute_task(*task));
  EXPECT_EQ(1, graph.planner_.completed_[0].load());
  EXPECT_EQ(2, graph.allocator_.live_.load());
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, graph.ctx_.dispatch_ret_);
  EXPECT_TRUE(graph.owner_.has_set_stop());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
