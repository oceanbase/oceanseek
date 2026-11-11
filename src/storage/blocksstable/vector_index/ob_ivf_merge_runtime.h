/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_RUNTIME_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_RUNTIME_H_

#include "storage/blocksstable/vector_index/ob_ivf_build_interfaces.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_malloc.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "storage/blocksstable/vector_index/ob_ivf_dag_planner.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_strategy.h"
#include <atomic>

namespace oceanbase
{
namespace storage
{

struct ObIVFMergeRuntime;
class ObIVFDDLHcaAdapter;
class ObIVFMergeTestPeer;

// Per-DAG accounting over the existing tenant malloc allocator. Unlike an
// arena, free() returns retired arrays and their old growth buffers immediately.
// Counters cover requested runtime bytes, not tenant/RSS or allocator padding.
class ObIVFMergeRuntimeAllocator final : public common::ObIAllocator
{
public:
  ObIVFMergeRuntimeAllocator();
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const common::ObMemAttr &attr) override
  { UNUSED(attr); return alloc(size); }
  virtual void free(void *ptr) override;
  virtual void set_attr(const common::ObMemAttr &attr) override;
  virtual int64_t used() const override { return used_.load(std::memory_order_relaxed); }
  int64_t peak() const { return peak_.load(std::memory_order_relaxed); }
  int64_t allocation_count() const { return allocation_count_.load(std::memory_order_relaxed); }
private:
  struct alignas(16) Header
  {
    int64_t size_;
    ObIVFMergeRuntimeAllocator *owner_;
  };
  common::ObMalloc allocator_;
  std::atomic<int64_t> used_;
  std::atomic<int64_t> peak_;
  std::atomic<int64_t> allocation_count_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFMergeRuntimeAllocator);
};

class ObIVFMergeProcessor : public ObIVFDagProcessor
{
public:
  // Shared by the RAW task planner and the physical writer window allocation.
  static constexpr int64_t MAX_RAW_TASK_COUNT = 64;
  static constexpr int64_t MAX_POSTING_TASK_COUNT = 16;
  static constexpr int64_t DDL_OUTPUT_SLICE_COUNT = 1 + MAX_POSTING_TASK_COUNT + MAX_RAW_TASK_COUNT;
  ObIVFMergeProcessor();
  virtual ~ObIVFMergeProcessor();
  int init() { return ObIVFDagProcessor::init(); }
  int get_work_perf(const int64_t task_idx, ObIVFMergeWorkPerf &perf) const;
  bool quantizes_during_soar() const;
  ObIVFRefinePhase refine_train_phase() const;
  // Input, numerical algorithm and physical output are borrowed for the whole
  // Processor lifetime. The owning DDL or Merge DagCtx must own them.
  int init(
      common::ObIAllocator &allocator,
      const ObIVFMergeInput &merge_input,
      ObIVFMergeAlgorithm &algorithm,
      ObIVFMergeQuantizer &physical_output,
      const ObIVFProcessType process_type = ObIVFProcessType::FULL_REBUILD,
      const ObIVFMergePrior *prior = nullptr);
  int configure_ddl_runtime(ObIVFDDLHcaAdapter &algorithm, int64_t parallelism);
  int export_centroid_layout(blocksstable::ObIvfCentroidLayout &layout) const;
  bool uses_prior_prefix() const;
  ObIVFProcessType get_process_type() const;
  virtual int prepare_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      const int64_t plan_id,
      int64_t &task_count);
  virtual int execute_task(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      const int64_t task_idx,
      ObIVFTaskResult &task_result);
  // Compatibility seam used by existing framework-only processors.
  virtual int finish_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      bool &hierarchy_finished);
  int finish_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      const int64_t plan_id,
      ObIVFMergeStepResult &step_result);
  virtual int build_final_artifact(
      ObIVFMergeArtifact &final_artifact);
  virtual void cancel(const int ret_code) override;

private:
  friend class ObIVFMergeTestPeer;
  friend class ObIVFMergePlanner;
  bool is_runtime_enabled() const;
  // Resolve the actual clustering operation before publishing a task plan.
  // A FLAT result installs its layout and skips the centroid-index stage.
  int resolve_centroid_index_operation(ObIVFMergeStep &step, int64_t &depth, bool &required);
  int prepare_centroid_index_step(ObIVFMergeStep step, int64_t depth,
      int64_t plan_id, int64_t &task_count);
  int execute_centroid_index_task(ObIVFMergeStep step, int64_t depth,
      int64_t plan_id, int64_t task_idx);
  int finish_centroid_index_step(ObIVFMergeStep step, int64_t depth,
      int64_t plan_id, ObIVFMergeStepResult &step_result);
  int install_centroid_index_layout();
  ObIVFRefinePhase centroid_index_refine_phase() const;
  int init_merge_runtime(
      common::ObIAllocator &allocator,
      const ObIVFMergeInput &merge_input,
      ObIVFMergeAlgorithm &algorithm,
      ObIVFMergeQuantizer &physical_output,
      const ObIVFProcessType process_type, const ObIVFMergePrior *prior);
  int init_insertion_memberships();
  int init_concat_memberships();
  int prepare_prior_step(const int64_t plan_id, int64_t &task_count);
  int execute_prior_task(const int64_t plan_id, const int64_t task_idx);
  int finish_prior_step(const int64_t plan_id, ObIVFMergeStepResult &step_result);
  int prepare_hierarchy_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      const int64_t plan_id,
      int64_t &task_count);
  int execute_hierarchy_task(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      const int64_t plan_id,
      const int64_t task_idx);
  int finish_hierarchy_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t depth,
      const int64_t plan_id,
      ObIVFMergeStepResult &step_result);
  int prepare_post_hierarchy_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t plan_id,
      int64_t &task_count);
  int execute_post_hierarchy_task(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t plan_id,
      const int64_t task_idx);
  int finish_post_hierarchy_step(
      const ObIVFMergeStage stage,
      const ObIVFMergeStep step,
      const int64_t plan_id,
      ObIVFMergeStepResult &step_result);

  // INITIAL_BUILD owns a concurrent FIFO allocator so DDL barriers can reclaim
  // retired arrays. MERGE borrows its DagCtx's reclaiming allocator, retaining
  // the merge context attribution and per-DAG requested-byte peak accounting.
  common::ObConcurrentFIFOAllocator runtime_allocator_;
  common::ObIAllocator *allocator_;
  ObIVFMergeAlgorithm *algorithm_;
  ObIVFMergeQuantizer *physical_output_;
  ObIVFMergeRuntime *runtime_;
  // Bound by ObIVFMergePlanner before the first task is materialized.
  ObIVFProcessType process_type_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFMergeProcessor);
};

bool is_valid_ivf_merge_operation(ObIVFMergeStage stage, ObIVFMergeStep step, int64_t depth);

// Test and benchmark observation access to the processor's private runtime.
// Reads centers, assignments, hierarchy and cancellation state for assertions,
// prior construction and metrics; production merge execution does not use it.
// Callers must observe at a completed task barrier or after the DAG finishes.
// The explicit finish hook below is reserved for tests that drive the barrier.
class ObIVFMergeTestPeer
{
public:
  static int set_parent_build_threshold(ObIVFMergeProcessor &processor, int64_t threshold);
  // Test-only barrier hook for focused Hierarchy tests. It seals the current
  // Hierarchy ActivePlan without advancing into the remaining FULL_REBUILD.
  static int finish_hierarchy_step(
      ObIVFMergeProcessor &processor,
      const ObIVFTaskPlan &task_plan,
      ObIVFMergeStepResult &step_result);
  static bool is_hierarchy_finished(const ObIVFMergeProcessor &processor);
  static int64_t get_k(const ObIVFMergeProcessor &processor);
  static int64_t get_actual_max_depth(const ObIVFMergeProcessor &processor);
  static int64_t get_no_progress_leaf_count(const ObIVFMergeProcessor &processor);
  static int64_t get_no_progress_member_count(const ObIVFMergeProcessor &processor);
  static bool is_canceled(const ObIVFMergeProcessor &processor);
  static bool has_active_plan(const ObIVFMergeProcessor &processor);
  static bool root_uses_identity_view(
      const ObIVFMergeProcessor &processor);
  static int64_t get_root_owned_vid_count(
      const ObIVFMergeProcessor &processor);
  static int64_t get_root_owned_vector_idx_count(
      const ObIVFMergeProcessor &processor);
  static int get_root_vector_ref(
      const ObIVFMergeProcessor &processor,
      const int64_t offset,
      ObIVFVectorVID &vid,
      int64_t &vector_idx);
  static int64_t get_member_count(
      const ObIVFMergeProcessor &processor,
      const int64_t global_cid);
  static int get_member_vid(
      const ObIVFMergeProcessor &processor,
      const int64_t global_cid,
      const int64_t member_idx,
      ObIVFVectorVID &vid);
  static int get_center_value(
      const ObIVFMergeProcessor &processor,
      const int64_t global_cid,
      const int64_t dimension_idx,
      float &value);
  static ObIVFNeighborhoodMode choose_neighborhood_mode(const int64_t k);
  static bool is_merge_finished(
      const ObIVFMergeProcessor &processor);
  static int64_t get_final_k(const ObIVFMergeProcessor &processor);
  static bool are_quant_intermediates_released(
      const ObIVFMergeProcessor &processor);
  static int get_primary_assignment(
      const ObIVFMergeProcessor &processor,
      const ObIVFVectorVID vid,
      int64_t &cid);
  static int get_secondary_assignment(
      const ObIVFMergeProcessor &processor,
      const ObIVFVectorVID vid,
      int64_t &cid);
  static int get_primary_assignment_by_vector_idx(
      const ObIVFMergeProcessor &processor,
      const int64_t vector_idx,
      int64_t &cid);
  static int get_secondary_assignment_by_vector_idx(
      const ObIVFMergeProcessor &processor,
      const int64_t vector_idx,
      int64_t &cid);
  static int64_t get_final_member_count(
      const ObIVFMergeProcessor &processor,
      const int64_t cid);
  static int get_final_center_value(
      const ObIVFMergeProcessor &processor,
      const int64_t cid,
      const int64_t dimension_idx,
      float &value);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_MERGE_RUNTIME_H_
