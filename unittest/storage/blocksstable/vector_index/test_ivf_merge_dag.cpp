/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <set>
#include <map>
#include <vector>
#include <atomic>
#include <chrono>
#include <functional>
#include <rapidjson/document.h>
#include <cmath>
#include <cfloat>
#include <cstdio>
#include <cstdlib>
#include <sys/resource.h>
#include <sys/mman.h>
#include <unistd.h>
// The allocation regression observes the cache buffer's actual arena owner.
#define private public
#define protected public
#include "lib/container/ob_se_array.h"
#undef protected
#undef private
#include "common/ob_target_specific.h"
#include "share/vector_type/ob_vector_soar_distance.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/ob_sort.h"
#define private public
#define protected public
#include "lib/alloc/memory_dump.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#undef protected
#undef private
#include "storage/ob_storage_schema.h"
#include "storage/schema_utils.h"

#define private public
#define protected public
#include "storage/blocksstable/vector_index/ob_ivf_merge_runtime.h"
#include "storage/blocksstable/vector_index/ob_ivf_parent_centroid_build.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_strategy.h"
#include "storage/compaction/ob_async_index_merge_task.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_capture_pipeline.h"
#undef protected
#undef private

#include "lib/hash_func/splitmix64.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_index/hca/ob_vector_hca_common.h"
#include "share/vector_index/hca/ob_vector_hca_simd.h"
#include "storage/blocksstable/vector_index/ob_ivf_hca_algorithm_adapter.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_hca_adapter.h"
#define private public
#include "storage/blocksstable/vector_index/ob_ivf_ddl_runtime.h"
#undef private
#include "storage/blocksstable/vector_index/ob_ivf_build_state.h"
#include "storage/blocksstable/vector_index/ob_ivf_build_monitor.h"
#include "storage/ddl/ob_ddl_dag_monitor_mgr.h"
#include "observer/omt/ob_multi_tenant.h"
#include "lib/task/ob_timer_service.h"

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

class BoundedLocatorAllocator final : public ObIAllocator
{
public:
  BoundedLocatorAllocator() : largest_request_(0), remaining_allocations_(-1), backing_() {}
  void *alloc(int64_t size) override
  {
    largest_request_ = MAX(largest_request_, size);
    if (size <= 0 || size > (1L << 20) || remaining_allocations_ == 0) { return nullptr; }
    if (remaining_allocations_ > 0) { --remaining_allocations_; }
    return backing_.alloc(size);
  }
  void *alloc(int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *pointer) override { backing_.free(pointer); }
  int64_t used() const override { return backing_.used(); }
  int64_t largest_request_;
  int64_t remaining_allocations_;
private:
  ObIVFMergeRuntimeAllocator backing_;
};

TEST(TestIVFSourceLocators, transfers_segmented_storage_without_copy_or_double_free)
{
  BoundedLocatorAllocator allocator;
  {
    ObIVFSourceLocators source(allocator), target(allocator);
    const int64_t count = (1L << 20) / sizeof(ObAsyncIVFSourceLocator) + 3;
    ASSERT_EQ(OB_SUCCESS, source.prepare_allocate(count));
    for (int64_t i = 0; i < count; ++i) {
      source.at(i) = ObAsyncIVFSourceLocator(count - i, 0, 0, i);
    }
    lib::ob_sort(source.begin(), source.end(),
        [](const auto &a, const auto &b) { return a.vid_ < b.vid_; });
    const auto *first = &source.at(0);
    const auto *last = &source.at(count - 1);
    const int64_t used = allocator.used();
    ASSERT_EQ(OB_SUCCESS, target.take_over(source));
    EXPECT_TRUE(source.empty());
    EXPECT_EQ(0, source.get_capacity());
    EXPECT_EQ(count, target.count());
    EXPECT_EQ(first, &target.at(0));
    EXPECT_EQ(last, &target.at(count - 1));
    EXPECT_EQ(used, allocator.used());
    source.reset();
    EXPECT_EQ(used, allocator.used());
    EXPECT_EQ(1, target.at(0).vid_);
    EXPECT_EQ(count, target.at(count - 1).vid_);
    EXPECT_LE(allocator.largest_request_, 1L << 20);
  }
  EXPECT_EQ(0, allocator.used());
}

TEST(TestIVFSourceLocators, failed_growth_remains_owned_and_takeover_rejects_other_allocators)
{
  BoundedLocatorAllocator allocator, other_allocator;
  {
    ObIVFSourceLocators source(allocator), wrong_owner(other_allocator), target(allocator);
    const int64_t block_capacity = ObIVFSourceLocators::Storage::BLOCK_CAPACITY;
    const int64_t count = block_capacity * 2;
    ASSERT_EQ(OB_SUCCESS, source.prepare_allocate(count));
    source.at(0) = ObAsyncIVFSourceLocator(17, 0, 0, 0);
    // The pointer directory is already allocated. Retain one new payload block,
    // then fail the next block without publishing additional logical rows.
    allocator.remaining_allocations_ = 1;
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, source.reserve(count * 2));
    EXPECT_EQ(count, source.count());
    EXPECT_EQ(count + block_capacity, source.get_capacity());
    EXPECT_EQ(17, source.at(0).vid_);
    EXPECT_EQ(OB_INVALID_ARGUMENT, wrong_owner.take_over(source));
    EXPECT_EQ(count, source.count());
    allocator.remaining_allocations_ = -1;
    ASSERT_EQ(OB_SUCCESS, source.push_back(source.at(0)));
    EXPECT_EQ(17, source.at(count).vid_);
    ASSERT_EQ(OB_SUCCESS, target.take_over(source));
    EXPECT_EQ(count + 1, target.count());
    EXPECT_LE(allocator.largest_request_, 1L << 20);
  }
  EXPECT_EQ(0, allocator.used());
  EXPECT_EQ(0, other_allocator.used());
}

// These tests only exercise resident stores. External sort still allocates a
// directory during init; provide that metadata service and reject file opens.
class ResidentTmpFileManager final : public tmp_file::ObTenantTmpFileManager
{
  class Directories final : public tmp_file::ObSNTenantTmpFileManager
  {
  public:
    Directories() : next_(0) {}
    int alloc_dir(int64_t &dir) override { dir = ATOMIC_AAF(&next_, 1); return OB_SUCCESS; }
  private:
    int64_t next_;
  };
public:
  ResidentTmpFileManager() { is_inited_ = true; }
  ~ResidentTmpFileManager() override { is_inited_ = false; }
  tmp_file::ObSNTenantTmpFileManager &get_sn_file_manager() override { return directories_; }
  int open(int64_t &, const int64_t &, const char *const) override { return OB_NOT_SUPPORTED; }
private:
  Directories directories_;
};

class TestMergeInput final
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

class TestMergeAlgorithm
    : public ObIVFMergeAlgorithm,
      public ObIVFMergeQuantizer
{
public:
  TestMergeAlgorithm()
    : quantize_count_(0), quantization_finish_count_(0), raw_write_count_(0),
      raw_finish_count_(0)
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
    UNUSEDx(depth, node_idx, reader, first_level_samples);
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
        || TestMergeInput::VID != primary_member_vids.at(0)
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
        || TestMergeInput::VID != all_vids.at(0)
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

void run_materialized_merge(
    ObIVFMergeDagCtx &dag_ctx,
    ObITask &prepare_task,
    ObIArray<ObIVFMergeStep> *steps = nullptr,
    ObIArray<int64_t> *depths = nullptr,
    const std::function<void(const ObIVFMergeDagCtx &)> &sample_memory = {})
{
  ObITask *parent_task = &prepare_task;
  int64_t plan_count = 0;
  const ObIVFMergePlanner *planner = static_cast<const ObIVFMergePlanner *>(dag_ctx.planner_);
  while (ObIVFMergeStage::FINISH != planner->get_stage()) {
    ASSERT_LT(plan_count, 512);
    ASSERT_EQ(1, parent_task->get_child_nodes().count());
    ObIVFMergeTask *first_task = static_cast<ObIVFMergeTask *>(
        parent_task->get_child_nodes().at(0));
    ASSERT_NE(nullptr, first_task);
    const ObIVFTaskPlan *task_plan = first_task->task_plan_;
    ASSERT_NE(nullptr, task_plan);
    EXPECT_EQ(plan_count, task_plan->get_plan_id());
    EXPECT_EQ(planner->get_process_type(), task_plan->get_process_type());
    if (steps != nullptr) { ASSERT_EQ(OB_SUCCESS, steps->push_back(planner->get_step())); }
    if (depths != nullptr) { ASSERT_EQ(OB_SUCCESS, depths->push_back(planner->get_depth())); }
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

    if (sample_memory) { sample_memory(dag_ctx); }
    for (int64_t task_idx = 0; task_idx < tasks.count(); ++task_idx) {
      ASSERT_EQ(OB_SUCCESS, tasks.at(task_idx)->process());
    }
    if (sample_memory) { sample_memory(dag_ctx); }
    ASSERT_EQ(OB_SUCCESS, finish_task->process());
    parent_task = finish_task;
    ++plan_count;
  }

  ASSERT_GT(plan_count, 0);
  ASSERT_EQ(1, parent_task->get_child_nodes().count());
  ASSERT_NE(nullptr, static_cast<ObAsyncIndexMergeFinishTask *>(
      parent_task->get_child_nodes().at(0)));
}

class SuspendMergeAlgorithm final : public TestMergeAlgorithm
{
public:
  SuspendMergeAlgorithm()
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

TEST(TestIVFMergeDag, generated_capture_tasks_share_input_ready_barrier)
{
  ObArenaAllocator allocator("IVFCaptureGraph", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObIVFMergeCaptureCtx capture;
  for (int64_t i = 0; i < 3; ++i) {
    ObIVFMergeCaptureSlice *slice = OB_NEWx(ObIVFMergeCaptureSlice, &capture.allocator_, allocator, nullptr);
    ASSERT_NE(nullptr, slice);
    ASSERT_EQ(OB_SUCCESS, capture.slices_.push_back(slice));
  }
  ObAsyncIndexMajorMergeDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ObIVFMergeCapturePipeline *first = nullptr;
  ObFakeTask *ready = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(first));
  ASSERT_EQ(OB_SUCCESS, first->init(&capture, 0));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*first));
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(ready));
  ASSERT_EQ(OB_SUCCESS, first->add_child(*ready));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*ready));
  ASSERT_EQ(1, ready->get_indegree());
  // Exercise the scheduler wrapper, including copying the fan-in dependency.
  ASSERT_EQ(OB_SUCCESS, first->ObITask::generate_next_task());
  ASSERT_EQ(2, ready->get_indegree());
  const ObIArray<ObINodeWithChild *> &children = first->get_child_nodes();
  ASSERT_EQ(1, children.count());
  EXPECT_EQ(ready, children.at(0));
}

TEST(TestIVFMergeInput, validates_normalization_and_rotation_contract)
{
  struct InputCase {
    ObVectorIndexDistAlgorithm dist_type_;
    bool normalized_;
    bool rotated_;
    int expected_ret_;
  };
  const InputCase cases[] = {
    {VIDA_L2, false, false, OB_INVALID_ARGUMENT},
    {VIDA_L2, false, true, OB_SUCCESS},
    {VIDA_L2, true, false, OB_INVALID_ARGUMENT},
    {VIDA_L2, true, true, OB_SUCCESS},
    {VIDA_IP, false, false, OB_INVALID_ARGUMENT},
    {VIDA_IP, false, true, OB_SUCCESS},
    {VIDA_IP, true, false, OB_INVALID_ARGUMENT},
    {VIDA_IP, true, true, OB_SUCCESS},
    {VIDA_COS, false, false, OB_INVALID_ARGUMENT},
    {VIDA_COS, false, true, OB_INVALID_ARGUMENT},
    {VIDA_COS, true, false, OB_INVALID_ARGUMENT},
    {VIDA_COS, true, true, OB_SUCCESS}
  };
  TestMergeInput vectors;
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.dist_type_);
    SCOPED_TRACE(test_case.normalized_);
    SCOPED_TRACE(test_case.rotated_);
    ObIVFMergeInput input;
    EXPECT_EQ(test_case.expected_ret_, input.init(vectors, vectors, 1,
        test_case.normalized_, test_case.rotated_, false, test_case.dist_type_));
    EXPECT_EQ(test_case.expected_ret_ == OB_SUCCESS, input.is_valid());
    if (test_case.expected_ret_ == OB_SUCCESS) {
      EXPECT_EQ(test_case.normalized_, input.vectors_normalized_);
      EXPECT_EQ(test_case.rotated_, input.vectors_rotated_);
      input.vectors_rotated_ = false;
      EXPECT_FALSE(input.is_valid());
    }
  }
}


// Exercise the production registration/serialization path, including reads
// after the DAG and its tasks have been destroyed.
class ScopedMergeMonitor
{
public:
  ScopedMergeMonitor() : previous_(MTL(ObDDLDagMonitorMgr *)) {}
  ~ScopedMergeMonitor()
  {
    MTL_CTX()->set(previous_);
    ObTenantEnv::set_tenant(MTL_CTX());
  }
  int init()
  {
    const int ret = mgr_.init();
    if (ret == OB_SUCCESS) {
      ObDDLDagMonitorMgr *ptr = &mgr_;
      MTL_CTX()->set(ptr);
      ObTenantEnv::set_tenant(MTL_CTX());
    }
    return ret;
  }
  ObDDLDagMonitorMgr mgr_;
private:
  ObDDLDagMonitorMgr *previous_;
};

struct MergeMonitorRow
{
  std::string kind_;
  std::string message_;
  bool node_finished_;
};

void read_merge_monitor(ObDDLDagMonitorMgr &mgr, std::vector<MergeMonitorRow> &rows)
{
  rows.clear();
  ObArenaAllocator allocator;
  ObSEArray<ObDDLDagMonitorNode *, 4> nodes;
  ASSERT_EQ(OB_SUCCESS, mgr.get_all_nodes(nodes));
  for (auto *node : nodes) {
    ObSEArray<ObDDLDagMonitorEntry *, 16> entries;
    const int ret = node->get_all_infos(allocator, entries);
    EXPECT_EQ(OB_SUCCESS, ret);
    for (auto *entry : entries) {
      const auto &kind = entry->get_task_info();
      const auto &message = entry->get_message();
      rows.push_back({std::string(kind.ptr(), kind.length()),
          std::string(message.ptr(), message.length()), node->is_finished()});
      EXPECT_EQ(1, entry->get_format_version());
      entry->~ObDDLDagMonitorEntry();
      allocator.free(entry);
    }
    node->dec_ref();
  }
}

void set_merge_monitor_dag_id(ObAsyncIndexMergeDag &dag)
{
  ObCurTraceId::TraceId id;
  ObAddr address;
  ASSERT_TRUE(address.set_ip_addr("127.0.0.1", 2882));
  id.init(address);
  ASSERT_EQ(OB_SUCCESS, dag.set_dag_id(id));
}

TEST(TestIVFMergeMonitor, shared_record_preserves_initial_build_contract_and_large_merge_ids)
{
  ObArenaAllocator allocator;
  ObIVFBuildMonitorInfo info(&allocator, nullptr);
  ObDDLTaskParam param;
  param.ddl_task_id_ = 91;
  param.execution_id_ = 7;
  ObIVFMergeStepPerf perf;
  perf.stage_ = ObIVFMergeStage::HIERARCHY;
  perf.step_ = ObIVFMergeStep::HIERARCHY_CLUSTER;
  perf.plan_id_ = 2;
  perf.depth_ = 1;
  perf.start_us_ = 100;
  perf.prepared_us_ = 110;
  perf.finish_begin_us_ = 140;
  perf.end_us_ = 150;
  info.init("IVF_STEP", param, 1001, perf);
  ObDDLDagMonitorEntry entry;
  ASSERT_EQ(OB_SUCCESS, info.convert_to_monitor_entry(entry));
  rapidjson::Document json;
  json.Parse(entry.get_message().ptr(), entry.get_message().length());
  ASSERT_FALSE(json.HasParseError());
  EXPECT_STREQ("INITIAL_BUILD", json["process_type"].GetString());
  EXPECT_STREQ("ivf_runtime", json["scope"].GetString());
  EXPECT_EQ(91, json["ddl_task_id"].GetInt64());
  EXPECT_EQ(7, json["execution_id"].GetInt64());
  EXPECT_FALSE(json.HasMember("strategy"));
  EXPECT_EQ(10, json["prepare_us"].GetInt64());
  EXPECT_EQ(30, json["parallel_elapsed_us"].GetInt64());
  EXPECT_EQ(10, json["finish_us"].GetInt64());
  EXPECT_EQ(50, json["total_us"].GetInt64());

  ObIVFMergeWorkPerf work;
  work.node_idx_ = work.center_idx_ = work.input_vectors_ = work.secondary_vectors_
      = work.posting_rows_ = work.reused_posting_rows_ = work.centers_ = work.first_level_samples_ = INT64_MAX;
  ObIVFBuildMonitorInfo task(&allocator, nullptr);
  task.init_merge("IVF_MERGE_TASK", INT64_MAX, INT64_MAX, UINT64_MAX, INT64_MAX,
      "INC_MAJOR_MERGE", "CONCATENATION", ObCurTraceId::TraceId(), perf, INT64_MAX, work);
  task.begin_task();
  ObIVFMergeWorkPerf actual = work;
  actual.posting_rows_ = 17;
  actual.reused_posting_rows_ = 5;
  task.end_task(OB_SUCCESS, 123, &actual);
  entry.reuse();
  ASSERT_EQ(OB_SUCCESS, task.convert_to_monitor_entry(entry));
  json.Parse(entry.get_message().ptr(), entry.get_message().length());
  ASSERT_FALSE(json.HasParseError());
  EXPECT_EQ(UINT64_MAX, json["tablet_id"].GetUint64());
  EXPECT_EQ(INT64_MAX, json["input_vectors"].GetInt64());
  EXPECT_EQ(123, json["exec_time_us"].GetInt64());
  EXPECT_EQ(123, json["max_attempt_us"].GetInt64());
  EXPECT_FALSE(json.HasMember("ddl_task_id"));
  EXPECT_EQ(17, json["posting_rows"].GetInt64());
  EXPECT_EQ(5, json["reused_posting_rows"].GetInt64());
  auto returned = task.get_work();
  EXPECT_EQ(17, returned.posting_rows_);
  returned.posting_rows_ = 999; // A caller gets a snapshot, not mutable monitor storage.
  EXPECT_EQ(17, task.get_work().posting_rows_);

  info.task_started();
  info.add_task(OB_SUCCESS, 123, task.get_work());
  ObIVFMergeWorkPerf failed;
  failed.input_vectors_ = 77;
  failed.posting_rows_ = 101;
  failed.reused_posting_rows_ = 53;
  task.begin_task();
  task.end_task(OB_IO_ERROR, 11, &failed);
  EXPECT_EQ(101, task.get_work().posting_rows_);
  info.task_started();
  info.add_task(OB_IO_ERROR, 11, task.get_work());
  ObIVFMergeWorkPerf copied;
  copied.input_vectors_ = 0;
  copied.posting_rows_ = copied.reused_posting_rows_ = 9;
  info.task_started();
  info.add_task(OB_SUCCESS, 13, copied);
  entry.reuse();
  ASSERT_EQ(OB_SUCCESS, info.convert_to_monitor_entry(entry));
  json.Parse(entry.get_message().ptr(), entry.get_message().length());
  ASSERT_FALSE(json.HasParseError());
  EXPECT_EQ(2, json["completed_tasks"].GetInt64());
  EXPECT_EQ(1, json["failed_tasks"].GetInt64());
  EXPECT_EQ(INT64_MAX, json["completed_input_vectors"].GetInt64());
  EXPECT_EQ(26, json["completed_posting_rows"].GetInt64());
  EXPECT_EQ(14, json["reused_posting_rows"].GetInt64());
}

TEST(TestIVFMergeMonitor, runtime_steps_tasks_and_history_match)
{
  ScopedMergeMonitor monitor;
  ASSERT_EQ(OB_SUCCESS, monitor.init());
  ObArenaAllocator allocator;
  TestMergeInput vectors;
  TestMergeAlgorithm algorithm;
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 1, true, true, false));
  ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(1), ObTabletID(1001), -1);
  ObAsyncIndexMergeCtx ctx(param, allocator);
  std::vector<MergeMonitorRow> rows;
  {
    ObAsyncIndexMajorMergeDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
    ASSERT_NO_FATAL_FAILURE(set_merge_monitor_dag_id(dag));
    ObFakeTask *prepare = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
    ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.start_with_runtime(
        dag, ctx, input, algorithm, algorithm, *prepare));
    ASSERT_NO_FATAL_FAILURE(read_merge_monitor(monitor.mgr_, rows));
    bool queued = false, running = false;
    for (const auto &row : rows) {
      rapidjson::Document json;
      json.Parse(row.message_.c_str());
      ASSERT_FALSE(json.HasParseError());
      if (row.kind_ == "IVF_MERGE_TASK") { queued |= std::string(json["state"].GetString()) == "QUEUED"; }
      if (row.kind_ == "IVF_MERGE") { running |= std::string(json["state"].GetString()) == "RUNNING"; }
    }
    EXPECT_TRUE(queued);
    EXPECT_TRUE(running);
    ASSERT_NO_FATAL_FAILURE(run_materialized_merge(dag.ivf_merge_dag_ctx_, *prepare));
  }
  ASSERT_NO_FATAL_FAILURE(read_merge_monitor(monitor.mgr_, rows));
  int64_t step_count = 0, task_count = 0, declared_tasks = 0;
  int64_t task_us = 0, step_task_us = 0, build_task_us = -1;
  int64_t completed_steps = -1, completed_tasks = -1;
  int64_t build_count = 0;
  for (const auto &row : rows) {
    SCOPED_TRACE(row.message_);
    EXPECT_TRUE(row.node_finished_);
    rapidjson::Document json;
    json.Parse(row.message_.c_str());
    ASSERT_FALSE(json.HasParseError());
    EXPECT_STREQ("MERGE", json["process_type"].GetString());
    EXPECT_STREQ("FULL_REBUILD", json["strategy"].GetString());
    EXPECT_STREQ("FINISHED", json["state"].GetString());
    EXPECT_EQ(0, json["ret_code"].GetInt());
    EXPECT_EQ(1, json["ls_id"].GetInt64());
    EXPECT_EQ(1001, json["tablet_id"].GetInt64());
    EXPECT_GT(json["execution_id"].GetInt64(), 0);
    if (row.kind_ == "IVF_MERGE_STEP") {
      ++step_count;
      EXPECT_EQ(json["total_us"].GetInt64(), json["prepare_us"].GetInt64()
          + json["parallel_elapsed_us"].GetInt64() + json["finish_us"].GetInt64());
      EXPECT_EQ(json["task_count"].GetInt64(), json["completed_tasks"].GetInt64());
      declared_tasks += json["task_count"].GetInt64();
      step_task_us += json["task_exec_sum_us"].GetInt64();
    } else if (row.kind_ == "IVF_MERGE_TASK") {
      ++task_count;
      EXPECT_EQ(1, json["schedule_count"].GetInt64());
      EXPECT_GT(json["last_worker_tid"].GetInt64(), 0);
      task_us += json["exec_time_us"].GetInt64();
    } else if (row.kind_ == "IVF_MERGE") {
      ++build_count;
      EXPECT_EQ(1, json["input_vectors"].GetInt64());
      completed_steps = json["completed_steps"].GetInt64();
      completed_tasks = json["completed_tasks"].GetInt64();
      build_task_us = json["task_exec_sum_us"].GetInt64();
    }
  }
  EXPECT_EQ(1, build_count);
  EXPECT_GT(step_count, 0);
  EXPECT_EQ(step_count, completed_steps);
  EXPECT_EQ(task_count, completed_tasks);
  EXPECT_EQ(task_count, declared_tasks);
  EXPECT_EQ(task_us, step_task_us);
  EXPECT_EQ(task_us, build_task_us);
  std::vector<MergeMonitorRow> again;
  ASSERT_NO_FATAL_FAILURE(read_merge_monitor(monitor.mgr_, again));
  ASSERT_EQ(rows.size(), again.size());
  for (size_t i = 0; i < rows.size(); ++i) { EXPECT_EQ(rows[i].message_, again[i].message_); }
  EXPECT_EQ(OB_SUCCESS, monitor.mgr_.clean_nodes(ObDDLDagMonitorMgr::CLEAN_FINISHED_INFO));
  ASSERT_NO_FATAL_FAILURE(read_merge_monitor(monitor.mgr_, again));
  EXPECT_TRUE(again.empty());
}

TEST(TestIVFMergeMonitor, failed_and_queued_tasks_survive_dag_destruction)
{
  ScopedMergeMonitor monitor;
  ASSERT_EQ(OB_SUCCESS, monitor.init());
  for (const bool execute : {false, true}) {
    ObArenaAllocator allocator;
    TestMergeInput vectors;
    SuspendMergeAlgorithm algorithm;
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 1, true, true, false));
    ObTabletMergeDagParam param(INC_MAJOR_MERGE, ObLSID(1), ObTabletID(execute ? 1002 : 1001), -1);
    ObAsyncIndexMergeCtx ctx(param, allocator);
    {
      ObAsyncIndexIncMergeDag dag;
      ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
      ASSERT_NO_FATAL_FAILURE(set_merge_monitor_dag_id(dag));
      ObFakeTask *prepare = nullptr;
      ASSERT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
      ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.start_with_runtime(
          dag, ctx, input, algorithm, algorithm, *prepare));
      if (execute) {
        auto *task = static_cast<ObIVFMergeTask *>(prepare->get_child_nodes().at(0));
        ASSERT_EQ(OB_ERR_UNEXPECTED, task->process());
        dag.set_dag_ret(OB_ERR_UNEXPECTED);
      } else {
        dag.set_dag_ret(OB_CANCELED);
      }
    }
  }
  std::vector<MergeMonitorRow> rows;
  ASSERT_NO_FATAL_FAILURE(read_merge_monitor(monitor.mgr_, rows));
  EXPECT_EQ(6, rows.size()); // one runtime, one active step and one task per DAG
  for (const auto &row : rows) {
    rapidjson::Document json;
    json.Parse(row.message_.c_str());
    ASSERT_FALSE(json.HasParseError());
    EXPECT_TRUE(row.node_finished_);
    EXPECT_STREQ("FAILED", json["state"].GetString());
    EXPECT_EQ(json["tablet_id"].GetInt64() == 1001 ? OB_CANCELED : OB_ERR_UNEXPECTED,
        json["ret_code"].GetInt());
    if (row.kind_ == "IVF_MERGE_TASK") {
      EXPECT_EQ(json["tablet_id"].GetInt64() == 1001 ? 0 : 1, json["schedule_count"].GetInt64());
    }
  }
}

TEST(TestIVFMergeMonitor, unmonitored_async_dag_destroys_tasks_once)
{
  class DestructionTask : public ObFakeTask
  {
  public:
    DestructionTask() : destruction_count_(nullptr) {}
    ~DestructionTask() override
    {
      if (nullptr != destruction_count_) { ++*destruction_count_; }
    }
    int64_t *destruction_count_;
  };
  // Full-text uses the same DAG types without initializing the IVF context.
  // Its pending tasks must still be drained by the base-class destructor.
  for (const auto type : {ObDagType::DAG_TYPE_ASYNC_INDEX_MAJOR_MERGE,
                         ObDagType::DAG_TYPE_ASYNC_INDEX_INC_MERGE}) {
    ObArenaAllocator allocator;
    int64_t destruction_count = 0;
    {
      ObAsyncIndexMergeDag dag(type);
      ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
      DestructionTask *task = nullptr;
      ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
      task->destruction_count_ = &destruction_count;
      EXPECT_FALSE(dag.ivf_merge_dag_ctx_.has_monitor());
      EXPECT_EQ(0, destruction_count);
    }
    EXPECT_EQ(1, destruction_count);
  }
}

TEST(TestIVFMergeMonitor, unavailable_monitor_does_not_fail_merge)
{
  // An installed but uninitialized manager makes registration fail.
  ObDDLDagMonitorMgr unavailable;
  ObDDLDagMonitorMgr *previous = MTL(ObDDLDagMonitorMgr *);
  ObDDLDagMonitorMgr *ptr = &unavailable;
  MTL_CTX()->set(ptr);
  ObTenantEnv::set_tenant(MTL_CTX());
  {
    ObArenaAllocator allocator;
    TestMergeInput vectors;
    TestMergeAlgorithm algorithm;
    ObIVFMergeInput input;
    EXPECT_EQ(OB_SUCCESS, input.init(vectors, vectors, 1, true, true, false));
    ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(1), ObTabletID(1001), -1);
    ObAsyncIndexMergeCtx ctx(param, allocator);
    ObAsyncIndexMajorMergeDag dag;
    EXPECT_EQ(OB_SUCCESS, dag.basic_init(allocator));
    set_merge_monitor_dag_id(dag);
    ObFakeTask *prepare = nullptr;
    EXPECT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
    EXPECT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.start_with_runtime(
        dag, ctx, input, algorithm, algorithm, *prepare));
    EXPECT_EQ(nullptr, dag.ivf_merge_dag_ctx_.monitor_node_);
    run_materialized_merge(dag.ivf_merge_dag_ctx_, *prepare);
    EXPECT_EQ(1, algorithm.raw_finish_count_);
  }
  MTL_CTX()->set(previous);
  ObTenantEnv::set_tenant(MTL_CTX());
}

TEST(TestIVFMergeDag, start_with_runtime_uses_test_owned_adapters)
{
  for (const ObMergeType type : {MAJOR_MERGE, INC_MAJOR_MERGE}) {
    SCOPED_TRACE(type);
    ObArenaAllocator allocator(
        "IVFMergeEntry", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    TestMergeInput vector_store;
    ObIVFMergeInput input;
    TestMergeAlgorithm algorithm;
    ASSERT_EQ(OB_SUCCESS,
        input.init(vector_store, vector_store, 1, true /* normalized */, true /* rotated */, false /* enable_soar */));

    ObTabletMergeDagParam merge_param(
        type, ObLSID(1), ObTabletID(1001), -1);
    merge_param.merge_version_ = 100;
    merge_param.skip_get_tablet_ = true;
    merge_param.is_use_async_index_dag_ = true;
    merge_param.data_tablet_id_ = ObTabletID(1000);
    ObAsyncIndexMergeCtx merge_ctx(merge_param, allocator);
    ObStorageSchema storage_schema;
    ASSERT_EQ(OB_SUCCESS, prepare_async_ivf_schema(allocator, storage_schema));
    ASSERT_TRUE(storage_schema.is_async_ivf_index());
    merge_ctx.static_param_.schema_ = &storage_schema;

    ObAsyncIndexMergeDag owner_dag(type == INC_MAJOR_MERGE
        ? ObDagType::DAG_TYPE_ASYNC_INDEX_INC_MERGE
        : ObDagType::DAG_TYPE_ASYNC_INDEX_MAJOR_MERGE);
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
    ASSERT_NO_FATAL_FAILURE(run_materialized_merge(
        owner_dag.ivf_merge_dag_ctx_, *prepare_task));

    EXPECT_EQ(1, algorithm.quantize_count_);
    EXPECT_EQ(1, algorithm.quantization_finish_count_);
    EXPECT_EQ(1, algorithm.raw_write_count_);
    EXPECT_EQ(1, algorithm.raw_finish_count_);
    const ObIVFDagArtifact *artifact = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        owner_dag.ivf_merge_dag_ctx_.planner_->get_artifact(artifact));
    const ObIVFMergeArtifact *merge_artifact =
        static_cast<const ObIVFMergeArtifact *>(artifact);
    ASSERT_NE(nullptr, merge_artifact);
    EXPECT_EQ(1, merge_artifact->vector_count_);
    EXPECT_EQ(1, merge_artifact->cluster_count_);
  }
}

TEST(TestIVFMergeDag, merge_rejects_suspended_processor_result)
{
  ObArenaAllocator allocator(
      "IVFMergeSuspend", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  TestMergeInput vector_store;
  ObIVFMergeInput input;
  SuspendMergeAlgorithm algorithm;
  ObIVFMergeProcessor processor;
  ObIVFFullRebuildPlanner planner;
  ASSERT_EQ(OB_SUCCESS,
      input.init(vector_store, vector_store, 1, true /* normalized */, true /* rotated */, false /* enable_soar */));
  ASSERT_EQ(OB_SUCCESS,
      processor.init(allocator, input, algorithm, algorithm));
  ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor));
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


namespace {
class SoarArrayStore final : public ObIVFVectorStore
{
public:
  SoarArrayStore(int64_t n, int64_t dim) : n_(n), dim_(dim), data_(n * dim), fail_(false) {}
  bool is_valid() const override { return n_ > 0 && dim_ > 0; }
  int64_t get_vector_count() const override { return n_; }
  int64_t get_dimension() const override { return dim_; }
  bool is_fully_memory_resident() const override { return resident_; }
  bool uses_position_ids() const override { return position_ids_; }
  int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override
  {
    int ret = OB_SUCCESS; vids.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < n_; ++i) { ret = vids.push_back(position_ids_ ? i : 17 + 3 * i); }
    return ret;
  }
  int batch_get_by_vid(const ObIVFVectorIDView &vids, int64_t begin, int64_t end,
      share::ObVectorMatrix &out) const override {
    if (fail_) { return OB_IO_ERROR; }
    read_rows_ += end - begin;
    int ret = OB_SUCCESS; out.reuse();
    if (OB_FAIL(out.reserve((end - begin) * dim_))) { return ret; }
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      const int64_t pos = position_ids_ ? vids.at(i) : (vids.at(i) - 17) / 3;
      if (pos < 0 || pos >= n_ || vids.at(i) != static_cast<uint64_t>(position_ids_ ? pos : 17 + 3 * pos)) {
        return OB_INVALID_ARGUMENT;
      }
      for (int64_t d = 0; OB_SUCC(ret) && d < dim_; ++d) { ret = out.push_back(data_[pos * dim_ + d]); }
    }
    return ret;
  }
  int64_t n_, dim_;
  std::vector<float> data_;
  bool fail_;
  bool resident_ = false;
  bool position_ids_ = false;
  mutable std::atomic<int64_t> read_rows_{0};
};

TEST(TestIVFMergeInput, soar_secondary_matches_original_bulk4_and_preserves_centers)
{
  common::init_arches();
  for (int64_t dim : {1, 7, 64, 768}) {
    for (int64_t k : {1, 3, 128, 129}) {
      SoarArrayStore store(67, dim);
      share::ObVectorMatrix centers;
      ObArray<float> limits;
      ObArray<int64_t> offsets;
      share::ObVectorArray<int64_t> neighbors;
      ObArray<int64_t> primary;
      ObArray<int64_t> actual;
      share::ObVectorArray<ObIVFVectorVID> vids;
      ASSERT_EQ(OB_SUCCESS, centers.set_dimension(dim));
      for (int64_t c = 0; c < k; ++c) {
        for (int64_t d = 0; d < dim; ++d) { ASSERT_EQ(OB_SUCCESS, centers.push_back(std::sin(c * 0.3 + d * 0.7))); }
        ASSERT_EQ(OB_SUCCESS, offsets.push_back(neighbors.count()));
        if (k > 128) {
          const int64_t count = c % 7 == 0 ? 0 : c % 7 == 1 ? 3 : 128;
          for (int64_t j = 0; j < count; ++j) { ASSERT_EQ(OB_SUCCESS, neighbors.push_back((c + j) % k)); }
        }
        ASSERT_EQ(OB_SUCCESS, limits.push_back(1.0F));
      }
      ASSERT_EQ(OB_SUCCESS, offsets.push_back(neighbors.count()));
      for (int64_t i = 0; i < store.n_; ++i) {
        const int64_t cid = i * 19 % k;
        for (int64_t d = 0; d < dim; ++d) {
          store.data_[i * dim + d] = centers.at(cid * dim + d) + (i == 19 ? 0 : 0.02F * std::cos(i + d));
        }
        if (i >= 1 && i < 66) { ASSERT_EQ(OB_SUCCESS, primary.push_back(cid)); }
      }
      ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
      ObIVFVectorReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.init(store));
      ObIVFNeighborGraphView graph(offsets, neighbors, limits, k);
      ObIVFHcaAlgorithmAdapter algorithm;
      ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
      std::vector<float> before((&centers.at(0)), (&centers.at(0)) + centers.count());
      ASSERT_EQ(OB_SUCCESS, algorithm.soar_assign_secondary(reader, vids, centers, primary,
          k > 128 ? &graph : nullptr, k, 1, 66, actual));
      ASSERT_EQ(65, actual.count());
      EXPECT_EQ(0, MEMCMP(before.data(), (&centers.at(0)), before.size() * sizeof(float)));
      for (int64_t i = 1; i < 66; ++i) {
        const int64_t pc = primary.at(i - 1);
        const float *v = store.data_.data() + i * dim;
        const float *p = &centers.at(pc * dim);
        const float pd = hca_square_distance(v, p, dim);
        int64_t best = -1; float best_score = 0;
        if (k > 1 && pd > ObHcaSoarParam().min_primary_distance_) {
          std::vector<float> residual(dim);
          std::vector<int64_t> candidates;
          for (int64_t d = 0; d < dim; ++d) { residual[d] = v[d] - p[d]; }
          if (k <= 128) {
            for (int64_t c = 0; c < k; ++c) { if (c != pc) { candidates.push_back(c); } }
          } else {
            for (int64_t j = offsets.at(pc); j < offsets.at(pc + 1); ++j) {
              if (neighbors.at(j) != pc) { candidates.push_back(neighbors.at(j)); }
            }
          }
          int64_t j = 0;
          auto keep = [&](int64_t cid, float score) { if (best < 0 || score < best_score) { best = cid; best_score = score; } };
          for (; j + 4 <= static_cast<int64_t>(candidates.size()); j += 4) {
            float scores[4];
            ObVectorSoarDistance<float>::soar_distance_bulk4(v,
                &centers.at(candidates[j] * dim), &centers.at(candidates[j + 1] * dim),
                &centers.at(candidates[j + 2] * dim), &centers.at(candidates[j + 3] * dim),
                residual.data(), dim, 1.0F, pd, scores);
            for (int64_t x = 0; x < 4; ++x) { keep(candidates[j + x], scores[x]); }
          }
          for (; j < static_cast<int64_t>(candidates.size()); ++j) {
            keep(candidates[j], ObVectorSoarDistance<float>::soar_distance(v,
                &centers.at(candidates[j] * dim), residual.data(), dim, 1.0F, pd));
          }
        }
        EXPECT_EQ(best, actual.at(i - 1)) << "dim=" << dim << " k=" << k << " row=" << i;
      }
      store.fail_ = true;
      EXPECT_EQ(OB_IO_ERROR, algorithm.soar_assign_secondary(reader, vids, centers, primary,
          k > 128 ? &graph : nullptr, k, 1, 66, actual));
      EXPECT_TRUE(actual.empty());
      store.fail_ = false;
      EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.soar_assign_secondary(reader, vids, centers, primary,
          k > 128 ? &graph : nullptr, k + 1, 1, 66, actual));
      primary.at(0) = k;
      EXPECT_NE(OB_SUCCESS, algorithm.soar_assign_secondary(reader, vids, centers, primary,
          k > 128 ? &graph : nullptr, k, 1, 66, actual));
    }
  }
}

TEST(TestIVFMergeInput, soar_adapter_performance)
{
  if (std::getenv("IVF_SOAR_BENCH") == nullptr) { return; }
  common::init_arches();
  const int64_t n = 16384, dim = 768, threads = 4;
  const char *output = std::getenv("IVF_SOAR_OUTPUT");
  FILE *file = output == nullptr ? nullptr : std::fopen(output, "wb");
  ASSERT_TRUE(output == nullptr || file != nullptr);
  for (int64_t k : {3733, 36386}) {
    SoarArrayStore store(n, dim);
    share::ObVectorMatrix centers;
    ObArray<float> limits;
    ObArray<int64_t> offsets;
    share::ObVectorArray<int64_t> neighbors;
    share::ObVectorArray<ObIVFVectorVID> vids;
    uint32_t seed = 42;
    auto random = [&]() { seed = seed * 1664525U + 1013904223U; return (static_cast<int32_t>(seed % 10001) - 5000) / 5000.0F; };
    ASSERT_EQ(OB_SUCCESS, centers.set_dimension(dim));
    ASSERT_EQ(OB_SUCCESS, centers.reserve(k * dim));
    for (int64_t i = 0; i < k * dim; ++i) { ASSERT_EQ(OB_SUCCESS, centers.push_back(random())); }
    for (int64_t c = 0; c < k; ++c) {
      ASSERT_EQ(OB_SUCCESS, offsets.push_back(neighbors.count()));
      for (int64_t j = 1; j <= 128; ++j) { ASSERT_EQ(OB_SUCCESS, neighbors.push_back((c + j * 491) % k)); }
      ASSERT_EQ(OB_SUCCESS, limits.push_back(10000));
    }
    ASSERT_EQ(OB_SUCCESS, offsets.push_back(neighbors.count()));
    for (int64_t i = 0; i < n; ++i) {
      const int64_t cid = i * 7919 % k;
      for (int64_t d = 0; d < dim; ++d) { store.data_[i * dim + d] = centers.at(cid * dim + d) + random() * 0.03F; }
    }
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    ObIVFNeighborGraphView graph(offsets, neighbors, limits, k);
    ASSERT_TRUE(graph.is_valid());
    std::vector<int64_t> reference;
    for (int64_t round = 0; round < 3; ++round) {
      std::vector<int64_t> result(n, -1);
      std::vector<std::thread> workers;
      std::atomic<int> errors(0);
      const auto start = std::chrono::steady_clock::now();
      for (int64_t t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
          ObIVFHcaAlgorithmAdapter algorithm;
          ObIVFVectorReader reader;
          ObArray<int64_t> primary, secondary;
          const int64_t begin = t * 4096, end = begin + 4096;
          int ret = algorithm.init(ObIVFHcaAlgorithmParam());
          if (OB_SUCC(ret)) { ret = reader.init(store); }
          for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) { ret = primary.push_back(i * 7919 % k); }
          if (OB_SUCC(ret)) { ret = algorithm.soar_assign_secondary(reader, vids, centers, primary, &graph, k, begin, end, secondary); }
          if (ret != OB_SUCCESS || secondary.count() != 4096) { ++errors; }
          else { for (int64_t i = 0; i < 4096; ++i) { result[begin + i] = secondary.at(i); } }
        });
      }
      for (auto &worker : workers) { worker.join(); }
      const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
      ASSERT_EQ(0, errors.load());
      if (round == 0) {
        reference = result;
        if (file != nullptr) { ASSERT_EQ(result.size(), std::fwrite(result.data(), sizeof(int64_t), result.size(), file)); }
      } else { EXPECT_EQ(reference, result); }
      struct rusage usage; getrusage(RUSAGE_SELF, &usage);
      std::printf("SOAR_BENCH k=%ld n=%ld dim=%ld threads=%ld round=%ld seconds=%.6f rss_peak_kib=%ld\n",
          k, n, dim, threads, round, seconds, usage.ru_maxrss);
      std::fflush(stdout);
    }
  }
  if (file != nullptr) { std::fclose(file); }
}

class ReuseVectorStore : public ObIVFVectorStore, public ObIVFFirstLevelSampleSet
{
public:
  ReuseVectorStore(int64_t count = 512, int64_t members = 16) : count_(count), members_(members), jitter_(false) {}
  bool is_valid() const override { return true; }
  int64_t get_vector_count() const override { return count_; }
  int64_t get_sample_count() const override { return count_; }
  int64_t get_dimension() const override { return 2; }
  int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override
  {
    int ret = OB_SUCCESS; vids.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) { ret = vids.push_back(i + 1); }
    return ret;
  }
  int batch_get_by_vid(const ObIVFVectorIDView &vids, int64_t begin, int64_t end,
      share::ObVectorMatrix &vectors) const override
  {
    int ret = OB_SUCCESS; vectors.reuse();
    ++read_calls_;
    read_vectors_ += end - begin;
    max_read_rows_ = MAX(max_read_rows_, end - begin);
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      const int64_t index = vids.at(i) - 1;
      if (index < 0 || index >= count_) { ret = OB_INVALID_ARGUMENT; }
      else if (OB_FAIL(vectors.push_back(static_cast<float>((index / members_ % 8) * 100 + (jitter_ ? (index % members_) * 0.5 : 0))))) {}
      else { ret = vectors.push_back(static_cast<float>((index / members_ / 8) * 100)); }
    }
    return ret;
  }
  int batch_get_samples(int64_t begin, int64_t end, ObIArray<ObIVFVectorVID> &vids,
      share::ObVectorMatrix &vectors) const override
  {
    int ret = OB_SUCCESS; vids.reuse();
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) { ret = vids.push_back(i + 1); }
    if (OB_SUCC(ret)) { ret = batch_get_by_vid(vids, 0, vids.count(), vectors); }
    return ret;
  }
  int64_t count_;
  int64_t members_;
  bool jitter_;
  mutable int64_t read_calls_ = 0;
  mutable int64_t read_vectors_ = 0;
  mutable int64_t max_read_rows_ = 0;
};

TEST(TestIVFMergeInput, refine_prefetch_reads_samples_in_batches_and_propagates_failure)
{
  class CountingStore final : public ObIVFVectorStore {
  public:
    mutable int64_t calls_ = 0;
    mutable int64_t loaded_ = 0;
    mutable int64_t max_batch_ = 0;
    mutable int64_t single_reads_ = 0;
    int64_t fail_call_ = 0;
    bool is_valid() const override { return true; }
    int64_t get_vector_count() const override { return 131; }
    int64_t get_dimension() const override { return 2; }
    int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override {
      int ret = OB_SUCCESS;
      vids.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < get_vector_count(); ++i) { ret = vids.push_back(17 + i * 3); }
      return ret;
    }
    int batch_get_by_vid(const ObIVFVectorIDView &vids, int64_t begin, int64_t end,
        share::ObVectorMatrix &values) const override {
      int ret = OB_SUCCESS;
      values.reuse();
      ++calls_;
      if (calls_ == fail_call_) { return OB_IO_ERROR; }
      max_batch_ = MAX(max_batch_, end - begin);
      single_reads_ += end - begin == 1;
      loaded_ += end - begin;
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        const int64_t idx = (vids.at(i) - 17) / 3;
        if (OB_FAIL(values.push_back(static_cast<float>(idx % 4 * 100 + idx / 4) ))) {
        } else { ret = values.push_back(static_cast<float>(idx % 4 * 100)); }
      }
      return ret;
    }
  } store;
  ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  share::ObVectorArray<ObIVFVectorVID> vids;
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  share::ObVectorMatrix centers, working, repeated;
  share::ObVectorArray<int64_t> assignments;
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, centers.push_back(static_cast<float>(i * 100)));
    ASSERT_EQ(OB_SUCCESS, centers.push_back(static_cast<float>(i * 100)));
  }
  for (int64_t i = 0; i < store.get_vector_count(); ++i) { ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % 4)); }
  ObIVFHcaAlgorithmParam param;
  ObIVFHcaAlgorithmAdapter algorithm;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  ASSERT_EQ(OB_SUCCESS, algorithm.refine_train(reader, vids, centers, assignments, nullptr, 4, working));
  EXPECT_EQ(16, store.calls_);
  EXPECT_EQ(128, store.loaded_);
  EXPECT_EQ(8, store.max_batch_);
  EXPECT_EQ(0, store.single_reads_);
  ASSERT_EQ(8, working.count());
  ASSERT_EQ(OB_SUCCESS, algorithm.refine_train(reader, vids, centers, assignments, nullptr, 4, repeated));
  ASSERT_EQ(working.count(), repeated.count());
  EXPECT_EQ(0,
      MEMCMP(
          working.data_at(0, working.count()), repeated.data_at(0, repeated.count()), working.count() * sizeof(float)));
  // The initializing reader is task-local; subsequent batches must own a copy.
  {
    ObIVFVectorReader task_reader;
    ASSERT_EQ(OB_SUCCESS, task_reader.init(store));
    ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_begin(task_reader, vids, centers,
        assignments, nullptr, 4, repeated));
  }
  int64_t batches = 0;
  bool finished = false;
  while (!finished) {
    ASSERT_GT(algorithm.refine_train_task_count(), 0);
    for (int64_t task = 0; task < algorithm.refine_train_task_count(); ++task) {
      ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_distance_task(task));
    }
    ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_finish_batch(repeated, finished));
    ASSERT_LT(++batches, 100);
  }
  EXPECT_EQ(32, batches);
  EXPECT_EQ(0, algorithm.refine_train_task_count());
  ASSERT_EQ(working.count(), repeated.count());
  EXPECT_EQ(0,
      MEMCMP(
          working.data_at(0, working.count()), repeated.data_at(0, repeated.count()), working.count() * sizeof(float)));
  store.calls_ = 0;
  store.fail_call_ = 2;
  ASSERT_EQ(OB_IO_ERROR, algorithm.refine_train(reader, vids, centers, assignments, nullptr, 4, repeated));
  EXPECT_TRUE(repeated.empty());
  for (int64_t i = 0; i < 4; ++i) { EXPECT_FLOAT_EQ(i * 100.0F, centers.at(i * 2)); }
  store.calls_ = 0;
  ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_begin(reader, vids, centers,
      assignments, nullptr, 4, repeated));
  ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_distance_task(0));
  finished = false;
  EXPECT_EQ(OB_IO_ERROR, algorithm.refine_train_finish_batch(repeated, finished));
  EXPECT_FALSE(finished);
  // An incomplete batch must not publish mutable centers to the runtime.
  EXPECT_EQ(0, MEMCMP((&centers.at(0)), repeated.data_at(0, repeated.count()), centers.count() * sizeof(float)));
  algorithm.cancel(OB_IO_ERROR); // state is retained until the owner drains tasks
}

void make_reuse_prior(ObIVFMergePrior &prior, const ReuseVectorStore &vectors,
    const int64_t segment_count, const bool duplicate = false)
{
  prior.dimension_ = 2;
  prior.vector_count_ = prior.covered_vector_count_ = vectors.count_;
  prior.dominant_segment_ = 0;
  const int64_t count = vectors.count_ / vectors.members_;
  for (int64_t segment_idx = 0; segment_idx < segment_count; ++segment_idx) {
    ObIVFSegmentPriorInfo segment;
    segment.provides_prior_ = true;
    segment.centroid_begin_ = segment_idx * (count / segment_count);
    segment.centroid_count_ = segment_idx + 1 == segment_count
        ? count - segment.centroid_begin_ : count / segment_count;
    segment.live_vector_count_ = segment.centroid_count_ * vectors.members_;
    ASSERT_EQ(OB_SUCCESS, prior.segments_.push_back(segment));
    for (int64_t i = 0; i < segment.centroid_count_; ++i) {
      const int64_t center = duplicate ? 0 : segment.centroid_begin_ + i;
      ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(static_cast<float>((center % 8) * 100)));
      ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(static_cast<float>((center / 8) * 100)));
      ASSERT_EQ(OB_SUCCESS, prior.old_cids_.push_back(i));
      ASSERT_EQ(OB_SUCCESS, prior.posting_mass_.push_back(vectors.members_));
    }
    if (segment.live_vector_count_ > prior.segments_.at(prior.dominant_segment_).live_vector_count_) {
      prior.dominant_segment_ = segment_idx;
    }
  }
  ASSERT_EQ(OB_SUCCESS, prior.validate());
}

void make_reuse_postings(ObIVFMergePrior &prior, const ReuseVectorStore &vectors)
{
  ASSERT_TRUE(prior.postings_.empty());
  for (int64_t i = 0; i < vectors.count_; ++i) {
    ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(i + 1, i / vectors.members_)));
  }
  prior.postings_complete_ = true;
}

class ReuseCountingOutput : public ObIVFMergeQuantizer
{
public:
  int64_t primary_count_ = 0;
  int64_t secondary_count_ = 0;
  int64_t raw_count_ = 0;
  int64_t clusters_ = 0;
  share::ObVectorMatrix centers_;
  ObArray<ObIVFPriorPosting> primary_postings_;
  ObArray<ObIVFPriorPosting> secondary_postings_;
  int quantize_cluster(ObIVFVectorReader &, const share::ObVectorMatrix &centers, int64_t, int64_t cid,
      const ObIVFVectorIDView &members, const storage::ObIVFVectorIDView *secondary) override
  {
    int ret = OB_SUCCESS;
    if (centers_.empty()) { ret = centers_.assign(centers); }
    for (int64_t i = 0; OB_SUCC(ret) && i < members.count(); ++i) {
      ret = primary_postings_.push_back(ObIVFPriorPosting(members.at(i), cid));
    }
    for (int64_t i = 0; OB_SUCC(ret) && secondary != nullptr && i < secondary->count(); ++i) {
      ret = secondary_postings_.push_back(ObIVFPriorPosting(secondary->at(i), cid));
    }
    primary_count_ += members.count();
    secondary_count_ += secondary == nullptr ? 0 : secondary->count();
    ++clusters_;
    return ret;
  }
  int finish_quantization(int64_t n, int64_t k, int64_t postings) override
  { return n == primary_count_ && k == clusters_ && postings == n + secondary_count_ ? OB_SUCCESS : OB_ERR_UNEXPECTED; }
  int write_raw_vectors(ObIVFVectorReader &, const ObIVFVectorIDView &, int64_t begin,
      int64_t end, int64_t) override { raw_count_ += end - begin; return OB_SUCCESS; }
  int finish_raw_write(int64_t n) override { return n == raw_count_ ? OB_SUCCESS : OB_ERR_UNEXPECTED; }
};

class RangeCountingOutput final : public ReuseCountingOutput
{
public:
  int mode_ = 0;
  int64_t opened_ = 0, closed_ = 0, expected_ranges_ = 0;
  int64_t begin_ = 0, end_ = 0, next_ = 0;
  bool active_ = false, closed_success_ = false;
  ObIVFMergeProcessor *processor_ = nullptr;
  int prepare_quantization(const share::ObVectorMatrix &, int64_t k, int64_t &tasks) override
  { tasks = expected_ranges_ = MIN(3L, k); return OB_SUCCESS; }
  int open_quantization_range(int64_t begin, int64_t end, int64_t,
      ObIAllocator &, ObIVFMergeQuantizer *&output) override
  {
    if (active_ || begin != clusters_ || end <= begin) return OB_STATE_NOT_MATCH;
    begin_ = next_ = begin; end_ = end; active_ = true; ++opened_; output = this;
    return OB_SUCCESS;
  }
  int quantize_cluster(ObIVFVectorReader &r, const share::ObVectorMatrix &c, int64_t k, int64_t cid,
      const ObIVFVectorIDView &v, const storage::ObIVFVectorIDView *s) override
  {
    if (!active_ || cid != next_ || cid >= end_) return OB_STATE_NOT_MATCH;
    if (mode_ == 1) return OB_IO_ERROR;
    if (mode_ == 3) processor_->cancel(OB_CANCELED);
    ++next_;
    return ReuseCountingOutput::quantize_cluster(r, c, k, cid, v, s);
  }
  int close_quantization_range(ObIVFMergeQuantizer *&output, bool success) override
  {
    const bool valid = active_ && output == this && (!success || next_ == end_);
    ++closed_; active_ = false; closed_success_ = success; output = nullptr;
    return !valid ? OB_STATE_NOT_MATCH : mode_ == 2 ? OB_IO_ERROR : OB_SUCCESS;
  }
};

class ConcatTestSource : public ObIVFEncodedPostingSource
{
public:
  explicit ConcatTestSource(const ObIVFMergePrior &prior) : prior_(prior) {}
  const ObIVFMergePrior &prior_;
  int error_ = OB_SUCCESS;
  bool omit_last_ = false;
  mutable int64_t scanned_ = 0;
  int scan_postings(int64_t cid, ObIVFEncodedPostingConsumer &out) const override
  {
    if (error_ != OB_SUCCESS) { return error_; }
    std::vector<uint64_t> vids;
    for (int64_t i = 0; i < prior_.postings_.count(); ++i) {
      const auto &p = prior_.postings_.at(i);
      if (p.centroid_idx_ == cid) { vids.push_back(p.vid_); }
    }
    std::sort(vids.begin(), vids.end());
    if (omit_last_ && !vids.empty()) { vids.pop_back(); }
    for (const auto vid : vids) {
      const int ret = out.append(vid, ObString(sizeof(vid), reinterpret_cast<const char *>(&vid)));
      if (ret != OB_SUCCESS) { return ret; }
      ++scanned_;
    }
    return OB_SUCCESS;
  }
};

class ConcatCountingOutput : public ReuseCountingOutput
{
public:
  bool loaded_ = false, hierarchical_ = false;
  int64_t copied_ = 0, encoded_ = 0, layout_calls_ = 0;
  int copy_error_ = OB_SUCCESS;
  ObIVFMergeProcessor *cancel_during_copy_ = nullptr;
  std::map<std::pair<int64_t, uint64_t>, std::string> rows_;
  ObArray<uint32_t> counts_;
  bool supports_loaded_quantization() const override { return true; }
  bool supports_loaded_posting_reuse() const override { return true; }
  int prepare_loaded_output(int64_t k, int64_t) override
  { loaded_ = true; clusters_ = k; return OB_SUCCESS; }
  int set_centroid_layout(const blocksstable::ObIvfCentroidLayout &layout) override
  { hierarchical_ = layout.is_hierarchical(); ++layout_calls_; return OB_SUCCESS; }
  int copy_loaded_postings(const ObIVFEncodedPostingSource &source, int64_t from, int64_t to,
      const ObIVFVectorIDView &expected) override
  {
    if (copy_error_ != OB_SUCCESS) { return copy_error_; }
    if (cancel_during_copy_ != nullptr) { cancel_during_copy_->cancel(OB_CANCELED); }
    class Consumer : public ObIVFEncodedPostingConsumer {
    public:
      Consumer(ConcatCountingOutput &out, int64_t cid, const ObIVFVectorIDView &expected)
        : out_(out), cid_(cid), expected_(expected) {}
      int append(ObIVFVectorVID vid, const ObString &payload) override
      {
        if (pos_ >= expected_.count() || expected_.at(pos_) != vid || payload.length() != sizeof(vid)
            || MEMCMP(&vid, payload.ptr(), sizeof(vid)) != 0
            || !out_.rows_.emplace(std::make_pair(cid_, vid), std::string(payload.ptr(), payload.length())).second) {
          return OB_INVALID_DATA;
        }
        ++pos_; ++out_.copied_; ++out_.primary_count_; return OB_SUCCESS;
      }
      ConcatCountingOutput &out_;
      int64_t cid_, pos_ = 0;
      ObIVFVectorIDView expected_;
    } consumer(*this, to, expected);
    int ret = source.scan_postings(from, consumer);
    return ret == OB_SUCCESS && consumer.pos_ != expected.count() ? OB_INVALID_DATA : ret;
  }
  int set_leaf_posting_counts(const ObIArray<uint32_t> &counts) override
  {
    for (int64_t cid = 0; cid < counts.count(); ++cid) {
      int64_t actual = 0;
      for (const auto &row : rows_) { actual += row.first.first == cid; }
      if (actual != counts.at(cid)) { return OB_INVALID_DATA; }
    }
    return counts_.assign(counts);
  }
  int write_loaded_centers(const share::ObVectorMatrix &centers, int64_t, int64_t, int64_t) override
  { return centers_.assign(centers); }
  int quantize_loaded_vectors(const share::ObVectorMatrix &, int64_t, const ObIVFVectorIDView &vids,
      const share::ObVectorMatrix &, const common::ObIArray<int64_t> &, const common::ObIArray<int64_t> *) override
  { encoded_ += vids.count(); return OB_ERR_UNEXPECTED; }
  int finish_quantization(int64_t n, int64_t k, int64_t postings) override
  {
    return !loaded_ ? ReuseCountingOutput::finish_quantization(n, k, postings)
        : copied_ == postings && k == clusters_ && counts_.count() == k && postings >= n
            && centers_.count() == k * 2 ? OB_SUCCESS : OB_INVALID_DATA;
  }
};

class RecordingReuseAlgorithm : public ObIVFHcaAlgorithmAdapter
{
public:
  int parent_error_ = OB_SUCCESS;
  int get_parent_cluster_params(ObIVFHcaAlgorithmParam &param) const override
  {
    return parent_error_ == OB_SUCCESS
        ? ObIVFHcaAlgorithmAdapter::get_parent_cluster_params(param) : parent_error_;
  }
  int64_t trained_vectors_ = 0;
  int hca_train_local_centers(int64_t depth, int64_t node, ObIVFVectorReader &reader,
      const ObIVFVectorIDView &vids, const ObIVFFirstLevelSampleSet *samples,
      int64_t cluster_size, int64_t &k, share::ObVectorMatrix &centers) override
  {
    trained_vectors_ += vids.count();
    return ObIVFHcaAlgorithmAdapter::hca_train_local_centers(depth, node, reader, vids, samples, cluster_size, k, centers);
  }
  int64_t warm_calls_ = 0;
  int64_t refine_calls_ = 0;
  bool force_assignment_fallback_ = false;
  bool keep_refine_assignments_ = false;
  int insertion_error_ = OB_SUCCESS;
  int64_t insertion_finish_calls_ = 0;
  void finish_insertion_assign() override
  {
    ++insertion_finish_calls_;
    ObIVFHcaAlgorithmAdapter::finish_insertion_assign();
  }
  int insertion_assign_vectors(ObIVFVectorReader &reader, const ObIVFVectorIDView &vids,
      const share::ObVectorMatrix &centers, const common::ObIArray<int64_t> &offsets,
      const ObIArray<int64_t> &candidates, int64_t begin, int64_t end,
      ObIArray<int64_t> &assignments) override
  {
    return insertion_error_ == OB_SUCCESS
        ? ObIVFHcaAlgorithmAdapter::insertion_assign_vectors(reader, vids, centers, offsets, candidates, begin, end, assignments)
        : insertion_error_;
  }
  int check_cluster_quality(const ObIArray<int64_t> &counts, bool &degenerate) override
  {
    const int ret = ObIVFHcaAlgorithmAdapter::check_cluster_quality(counts, degenerate);
    if (force_assignment_fallback_ && ret == OB_SUCCESS) { degenerate = true; }
    return ret;
  }
  int insertion_train_centers(ObIVFVectorReader &reader, const ObIVFVectorIDView &vids,
      const ObIVFFirstLevelSampleSet &samples, const share::ObVectorMatrix &centers, int64_t k, share::ObVectorMatrix &trained) override
  {
    ++warm_calls_;
    return ObIVFHcaAlgorithmAdapter::insertion_train_centers(reader, vids, samples, centers, k, trained);
  }
  int refine_assign_vectors(ObIVFVectorReader &reader, const ObIVFVectorIDView &vids,
      const share::ObVectorMatrix &centers, const share::ObVectorArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *graph, int64_t k, int64_t begin, int64_t end,
      ObIArray<int64_t> &new_cids) override
  {
    if (!keep_refine_assignments_) {
      return ObIVFHcaAlgorithmAdapter::refine_assign_vectors(reader, vids, centers, assignments, graph, k, begin, end, new_cids);
    }
    int ret = OB_SUCCESS; new_cids.reuse();
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) { ret = new_cids.push_back(assignments.at(i)); }
    return ret;
  }
  int refine_train_begin(ObIVFVectorReader &reader, const ObIVFVectorIDView &vids,
      const share::ObVectorMatrix &centers, const share::ObVectorArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *graph, int64_t k, share::ObVectorMatrix &working) override
  {
    ++refine_calls_;
    return ObIVFHcaAlgorithmAdapter::refine_train_begin(reader, vids, centers, assignments, graph, k, working);
  }
};
} // namespace

namespace
{
// Exercise both reader output-allocation contracts and measure the arena while
// the real stats accessor still owns each window.
class PrefetchAllocationStore final : public ObIVFVectorStore
{
public:
  explicit PrefetchAllocationStore(const bool reset_output,
      const int64_t count = 4097, const int64_t dimension = 73)
    : reset_output_(reset_output), count_(count), dimension_(dimension),
      rounding_values_(false), failure_call_(0), failure_ret_(OB_SUCCESS),
      short_call_(0)
  { reset_observations(); }
  bool is_valid() const override { return count_ > 0 && dimension_ > 0; }
  int64_t get_vector_count() const override { return count_; }
  int64_t get_dimension() const override { return dimension_; }
  int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override
  {
    int ret = OB_SUCCESS;
    vids.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      ret = vids.push_back(11 + 3 * i);
    }
    return ret;
  }
  float value_at(const int64_t ordinal, const int64_t d) const
  {
    // Large cancellation terms make regrouping additions across windows visible.
    const float rounding_values[] = {
        16777216.0F, 0.25F, -16777216.0F, 0.00001F,
        -3.25F, 65536.0F, -65535.0F};
    return rounding_values_
        ? rounding_values[ordinal % 7] + static_cast<float>(d % 3) * 0.03125F
        : static_cast<float>((ordinal % 5) * 10 + d);
  }
  int batch_get_by_vid(const ObIVFVectorIDView &vids,
      const int64_t begin, const int64_t end, share::ObVectorMatrix &vectors) const override
  {
    int ret = OB_SUCCESS;
    ++read_calls_;
    const ObIAllocator *owner = vectors.get_block_allocator().allocator_;
    max_used_before_ = MAX(max_used_before_, owner == nullptr ? 0 : owner->used());
    if (begin < 0 || end <= begin || end > vids.count()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (read_calls_ == failure_call_) {
      ret = failure_ret_;
    } else {
      const int64_t rows = end - begin;
      const int64_t value_count = rows * dimension_;
      sequential_reads_ = sequential_reads_ && begin == read_rows_;
      read_rows_ += rows;
      max_read_rows_ = MAX(max_read_rows_, rows);
      max_read_bytes_ = MAX(max_read_bytes_, value_count * static_cast<int64_t>(sizeof(float)));
      if (reset_output_) {
        vectors.reset();
        ret = vectors.reserve(value_count);
      } else {
        vectors.reuse();
        ret = vectors.resize(value_count);
      }
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        const int64_t ordinal = (vids.at(i) - 11) / 3;
        if (vids.at(i) < 11 || (vids.at(i) - 11) % 3 != 0 || ordinal >= count_) {
          ret = OB_INVALID_ARGUMENT;
        }
        for (int64_t d = 0; OB_SUCC(ret) && d < dimension_; ++d) {
          if (reset_output_) {
            ret = vectors.push_back(value_at(ordinal, d));
          } else {
            vectors.at((i - begin) * dimension_ + d) = value_at(ordinal, d);
          }
        }
      }
      if (OB_SUCC(ret) && read_calls_ == short_call_) {
        for (int64_t d = 0; d < dimension_; ++d) { vectors.pop_back(); }
      }
    }
    peak_used_ = MAX(peak_used_, (owner == nullptr ? 0 : owner->used()));
    return ret;
  }
  void reset_observations()
  {
    read_calls_ = 0;
    read_rows_ = 0;
    max_used_before_ = 0;
    peak_used_ = 0;
    max_read_rows_ = 0;
    max_read_bytes_ = 0;
    sequential_reads_ = true;
  }
  const bool reset_output_;
  const int64_t count_;
  const int64_t dimension_;
  bool rounding_values_;
  int64_t failure_call_;
  int failure_ret_;
  int64_t short_call_;
  mutable int64_t read_calls_;
  mutable int64_t read_rows_;
  mutable int64_t max_used_before_;
  mutable int64_t peak_used_;
  mutable int64_t max_read_rows_;
  mutable int64_t max_read_bytes_;
  mutable bool sequential_reads_;
};
} // namespace

TEST(TestIVFHcaAdapter, large_prefetch_owns_only_one_vector_buffer)
{
  for (const bool reset_output : {false, true}) {
    SCOPED_TRACE(reset_output);
    PrefetchAllocationStore store(reset_output);
    ObIVFVectorReader reader;
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    share::ObVectorArray<ObIVFVectorVID> vids;
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    ObIVFHcaAlgorithmParam param;
    ObIVFHcaAlgorithmAdapter algorithm;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
    share::ObVectorMatrix centers, updated;
    ASSERT_EQ(OB_SUCCESS, algorithm.hca_compute_local_center_stats_range(
        reader, vids, centers, 0, updated));
    ASSERT_EQ(store.get_dimension(), updated.count());
    int64_t expected_sum = 0;
    for (int64_t i = 0; i < store.get_vector_count(); ++i) {
      expected_sum += (i % 5) * 10;
    }
    for (int64_t d = 0; d < store.get_dimension(); ++d) {
      const float expected = static_cast<float>(expected_sum + d * store.count_) / store.count_;
      EXPECT_FLOAT_EQ(expected, updated.at(d));
    }
    const int64_t window_bytes = 256 * store.get_dimension() * sizeof(float);
    ASSERT_GT(window_bytes, OB_MALLOC_NORMAL_BLOCK_SIZE * 4);
    EXPECT_EQ(17, store.read_calls_);
    EXPECT_EQ(store.count_, store.read_rows_);
    EXPECT_TRUE(store.sequential_reads_);
    EXPECT_EQ(256, store.max_read_rows_);
    EXPECT_EQ(0, store.max_used_before_);
    EXPECT_GE(store.peak_used_, window_bytes);
    // Both reset-output and reuse-output readers must release prior windows.
    EXPECT_LT(store.peak_used_, window_bytes + 2 * OB_MALLOC_NORMAL_BLOCK_SIZE);
    EXPECT_LT(store.peak_used_, 2 * window_bytes);
  }
}

TEST(TestIVFHcaAdapter, streaming_center_stats_preserve_order_and_normalization_bits)
{
  for (const bool reset_output : {false, true}) {
    for (const int64_t count : {255, 256, 257, 769}) {
      for (const bool normalize : {false, true}) {
        SCOPED_TRACE(reset_output);
        SCOPED_TRACE(count);
        SCOPED_TRACE(normalize);
        PrefetchAllocationStore store(reset_output, count, 17);
        store.rounding_values_ = true;
        ObIVFVectorReader reader;
        ASSERT_EQ(OB_SUCCESS, reader.init(store));
        ObArray<ObIVFVectorVID> vids;
        for (int64_t i = count - 1; i >= 0; --i) {
          ASSERT_EQ(OB_SUCCESS, vids.push_back(11 + 3 * i));
        }
        ObVectorNormalizeInfo norm;
        ObIVFHcaAlgorithmParam param;
        param.center_norm_info_ = normalize ? &norm : nullptr;
        ObIVFHcaAlgorithmAdapter algorithm;
        ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
        ObArray<float> expected, vector;
        share::ObVectorMatrix centers, updated;
        ASSERT_EQ(OB_SUCCESS, expected.prepare_allocate(store.dimension_));
        ASSERT_EQ(OB_SUCCESS, vector.prepare_allocate(store.dimension_));
        for (int64_t d = 0; d < store.dimension_; ++d) { expected.at(d) = 0.0F; }
        // Reference the original single ordered reduction, with no window sums.
        for (int64_t i = 0; i < vids.count(); ++i) {
          const int64_t ordinal = (vids.at(i) - 11) / 3;
          for (int64_t d = 0; d < store.dimension_; ++d) {
            vector.at(d) = store.value_at(ordinal, d);
          }
          hca_simd::axpy(&expected.at(0), &vector.at(0), 1.0F, store.dimension_);
        }
        for (int64_t d = 0; d < store.dimension_; ++d) { expected.at(d) /= count; }
        if (param.center_norm_info_ != nullptr) {
          ASSERT_EQ(OB_SUCCESS, hca_normalize_center(
              &expected.at(0), store.dimension_, *param.center_norm_info_));
        }
        ASSERT_EQ(OB_SUCCESS, algorithm.hca_compute_local_center_stats_range(
            reader, vids, centers, 0, updated));
        ASSERT_EQ(expected.count(), updated.count());
        EXPECT_EQ(0, MEMCMP(&expected.at(0), &updated.at(0), expected.count() * sizeof(float)));
        EXPECT_EQ((count + 255) / 256, store.read_calls_);
        EXPECT_EQ(count, store.read_rows_);
        EXPECT_TRUE(store.sequential_reads_);
        EXPECT_EQ(0, store.max_used_before_);
      }
    }
  }
}

TEST(TestIVFHcaAdapter, streaming_center_stats_reject_failed_or_short_second_window)
{
  for (const bool reset_output : {false, true}) {
    // OB_SUCCESS injects a short successful read; OB_CANCELED covers reader
    // cancellation propagation without adding a scheduler-only test hook.
    for (const int injected_ret : {OB_IO_ERROR, OB_CANCELED, OB_SUCCESS}) {
      SCOPED_TRACE(reset_output);
      SCOPED_TRACE(injected_ret);
      PrefetchAllocationStore store(reset_output, 513, 17);
      store.failure_call_ = injected_ret == OB_SUCCESS ? 0 : 2;
      store.failure_ret_ = injected_ret;
      store.short_call_ = injected_ret == OB_SUCCESS ? 2 : 0;
      ObIVFVectorReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.init(store));
      share::ObVectorArray<ObIVFVectorVID> vids;
      ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
      ObIVFHcaAlgorithmParam param;
      ObIVFHcaAlgorithmAdapter algorithm;
      ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
      share::ObVectorMatrix centers, updated;
      ASSERT_EQ(OB_SUCCESS, updated.push_back(999.0F));
      EXPECT_EQ(injected_ret == OB_SUCCESS ? OB_ERR_UNEXPECTED : injected_ret,
          algorithm.hca_compute_local_center_stats_range(reader, vids, centers, 0, updated));
      EXPECT_EQ(2, store.read_calls_);
      EXPECT_EQ(0, updated.count());
      EXPECT_EQ(0, store.max_used_before_);
      store.failure_call_ = 0;
      store.short_call_ = 0;
      store.reset_observations();
      ASSERT_EQ(OB_SUCCESS, algorithm.hca_compute_local_center_stats_range(
          reader, vids, centers, 0, updated));
      EXPECT_EQ(store.dimension_, updated.count());
      EXPECT_EQ(3, store.read_calls_);
      EXPECT_EQ(store.count_, store.read_rows_);
    }
  }
}

TEST(TestIVFHcaAdapter, streaming_center_stats_bound_bytes_and_read_at_least_one_row)
{
  struct Case { int64_t count_; int64_t dimension_; int64_t max_rows_; };
  const int64_t max_dimension = share::ObVectorArray<float>::BLOCK_CAPACITY;
  const Case cases[] = {{257, 2048, 128}, {3, max_dimension, 1}, {3, max_dimension + 1, 0}};
  for (const bool reset_output : {false, true}) {
    for (const Case &test_case : cases) {
      SCOPED_TRACE(reset_output);
      SCOPED_TRACE(test_case.dimension_);
      PrefetchAllocationStore store(reset_output, test_case.count_, test_case.dimension_);
      ObIVFVectorReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.init(store));
      share::ObVectorArray<ObIVFVectorVID> vids;
      ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
      ObIVFHcaAlgorithmParam param;
      ObIVFHcaAlgorithmAdapter algorithm;
      ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
      share::ObVectorMatrix centers, updated;
      if (test_case.max_rows_ == 0) {
        // One vector must remain contiguous within a matrix block.
        EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.hca_compute_local_center_stats_range(
            reader, vids, centers, 0, updated));
        EXPECT_EQ(0, store.read_calls_);
        EXPECT_TRUE(updated.empty());
        continue;
      }
      ASSERT_EQ(OB_SUCCESS, algorithm.hca_compute_local_center_stats_range(
          reader, vids, centers, 0, updated));
      ASSERT_EQ(store.dimension_, updated.count());
      EXPECT_EQ(3, store.read_calls_);
      EXPECT_EQ(test_case.max_rows_, store.max_read_rows_);
      EXPECT_EQ(store.count_, store.read_rows_);
      EXPECT_TRUE(store.sequential_reads_);
      EXPECT_EQ(0, store.max_used_before_);
      const int64_t max_bytes = MAX(int64_t(1024 * 1024), store.dimension_ * int64_t(sizeof(float)));
      EXPECT_LE(store.max_read_bytes_, max_bytes);
      EXPECT_LT(store.peak_used_, max_bytes + 2 * OB_MALLOC_NORMAL_BLOCK_SIZE);
    }
  }
}

TEST(TestIVFHcaAdapter, hierarchy_assignment_batches_preserve_source_positions)
{
  ReuseVectorStore store(20011, 73);
  ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ObIVFHcaAlgorithmParam param;
  ObIVFHcaAlgorithmAdapter algorithm;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  share::ObVectorMatrix centers;
  for (int64_t cid = 0; cid < 8; ++cid) {
    ASSERT_EQ(OB_SUCCESS, centers.push_back(static_cast<float>(cid * 100)));
    ASSERT_EQ(OB_SUCCESS, centers.push_back(0.0F));
  }
  for (const bool child_node : {false, true}) {
    SCOPED_TRACE(child_node);
    ObArray<ObIVFVectorVID> vids;
    const int64_t count = child_node ? 8209 : store.count_;
    for (int64_t i = 0; i < count; ++i) {
      // A child node contains a noncontiguous, reordered subset of global VIDs.
      const int64_t ordinal = child_node ? (19 * i + 7) % store.count_ : i;
      ASSERT_EQ(OB_SUCCESS, vids.push_back(ordinal + 1));
    }
    ObArray<int64_t> full_assignments, batch_assignments;
    ASSERT_EQ(OB_SUCCESS, algorithm.hca_assign_vectors(
        reader, vids, centers, 8, 0, count, full_assignments));
    ASSERT_EQ(count, full_assignments.count());
    for (int64_t begin = 0; begin < count; begin += 4096) {
      SCOPED_TRACE(begin);
      const int64_t end = MIN(begin + 4096, count);
      ASSERT_EQ(OB_SUCCESS, algorithm.hca_assign_vectors(
          reader, vids, centers, 8, begin, end, batch_assignments));
      ASSERT_EQ(end - begin, batch_assignments.count());
      for (int64_t i = begin; i < end; ++i) {
        const int64_t expected_cid = ((vids.at(i) - 1) / store.members_) % 8;
        ASSERT_EQ(expected_cid, full_assignments.at(i));
        ASSERT_EQ(expected_cid, batch_assignments.at(i - begin));
      }
    }
    EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.hca_assign_vectors(
        reader, vids, centers, 8, 1, 1, batch_assignments));
    EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.hca_assign_vectors(
        reader, vids, centers, 8, 2, 1, batch_assignments));
    EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.hca_assign_vectors(
        reader, vids, centers, 8, -1, 1, batch_assignments));
    EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.hca_assign_vectors(
        reader, vids, centers, 8, count - 1, count + 1, batch_assignments));
    EXPECT_EQ(0, batch_assignments.count());
    for (int64_t cid = 0; cid < 8; ++cid) {
      EXPECT_FLOAT_EQ(static_cast<float>(cid * 100), centers.at(2 * cid));
      EXPECT_FLOAT_EQ(0.0F, centers.at(2 * cid + 1));
    }
  }
}

namespace
{
// Fix membership topology so these tests isolate the runtime's leaf decision;
// center statistics still execute the production adapter and vector reader.
class HierarchyStatsAlgorithm final : public ObIVFHcaAlgorithmAdapter
{
public:
  enum Scenario { MULTI_LEVEL, MIXED_LEAVES, NO_PROGRESS };
  explicit HierarchyStatsAlgorithm(const Scenario scenario) : scenario_(scenario) {}
  int hca_train_local_centers(int64_t depth, int64_t node_idx, ObIVFVectorReader &reader,
      const ObIVFVectorIDView &vids, const ObIVFFirstLevelSampleSet *samples,
      int64_t cluster_size, int64_t &local_k, share::ObVectorMatrix &centers) override
  {
    UNUSEDx(node_idx, reader, vids, samples, cluster_size);
    int ret = OB_SUCCESS;
    centers.reuse();
    local_k = scenario_ == NO_PROGRESS ? 3
        : scenario_ == MIXED_LEAVES ? (depth == 0 ? 4 : 3) : 2;
    for (int64_t cid = 0; OB_SUCC(ret) && cid < local_k; ++cid) {
      if (OB_FAIL(centers.push_back(10000.0F + cid))) {
      } else {
        ret = centers.push_back(-10000.0F - cid);
      }
    }
    return ret;
  }
  int hca_assign_vectors(ObIVFVectorReader &reader,
      const ObIVFVectorIDView &vids, const share::ObVectorMatrix &centers,
      int64_t local_k, int64_t begin, int64_t end, ObIArray<int64_t> &assignments) override
  {
    UNUSEDx(reader, centers);
    int ret = OB_SUCCESS;
    assignments.reuse();
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      // Mixed root: two small leaves, one large child, and an empty CID in
      // between. The child then splits into three two-member leaves.
      const int64_t cid = scenario_ == NO_PROGRESS ? 1
          : scenario_ == MIXED_LEAVES
              ? (local_k == 4 ? (i < 2 ? 0 : i < 8 ? 1 : 3) : i / 2)
              : i / (vids.count() / 2);
      ret = assignments.push_back(cid);
    }
    return ret;
  }
  int hca_compute_local_center_stats_range(ObIVFVectorReader &reader,
      const ObIVFVectorIDView &members, const share::ObVectorMatrix &centers,
      int64_t cid, share::ObVectorMatrix &updated) override
  {
    ++stats_calls_;
    stats_vectors_ += members.count();
    return ObIVFHcaAlgorithmAdapter::hca_compute_local_center_stats_range(
        reader, members, centers, cid, updated);
  }
  const Scenario scenario_;
  int64_t stats_calls_ = 0;
  int64_t stats_vectors_ = 0;
};
} // namespace

TEST(TestIVFInitialBuildRuntime, hierarchy_updates_skip_only_nonleaf_statistics)
{
  for (const HierarchyStatsAlgorithm::Scenario scenario : {
      HierarchyStatsAlgorithm::MULTI_LEVEL, HierarchyStatsAlgorithm::MIXED_LEAVES,
      HierarchyStatsAlgorithm::NO_PROGRESS}) {
    SCOPED_TRACE(static_cast<int>(scenario));
    const bool mixed = scenario == HierarchyStatsAlgorithm::MIXED_LEAVES;
    const bool no_progress = scenario == HierarchyStatsAlgorithm::NO_PROGRESS;
    const int64_t vector_count = no_progress ? 513 : mixed ? 10 : 16;
    const int64_t last_depth = no_progress ? 0 : mixed ? 1 : 2;
    ObArenaAllocator allocator;
    ReuseVectorStore vectors(vector_count, 1);
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 2,
        false /* normalized */, true /* rotated */, false /* enable_soar */, VIDA_L2));
    HierarchyStatsAlgorithm algorithm(scenario);
    ObIVFHcaAlgorithmParam param;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
    ReuseCountingOutput output;
    ObIVFMergeProcessor processor;
    ObIVFFullBuildPlanner planner;
    ASSERT_EQ(OB_SUCCESS, processor.init(
        allocator, input, algorithm, output, ObIVFProcessType::INITIAL_BUILD));
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, ObIVFProcessType::INITIAL_BUILD));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    bool finished = false;
    for (int64_t steps = 0; !finished && steps < 12; ++steps) {
      ASSERT_NE(nullptr, plan);
      ASSERT_EQ(ObIVFMergeStage::HIERARCHY, planner.get_stage());
      const int64_t depth = planner.get_depth();
      ASSERT_LE(depth, last_depth);
      const bool update = planner.get_step() == ObIVFMergeStep::HIERARCHY_UPDATE;
      const int64_t stats_before = algorithm.stats_calls_;
      const int64_t reads_before = vectors.read_vectors_;
      if (update) {
        const int64_t expected_tasks = no_progress ? 1 : mixed ? 3 : int64_t(2) << depth;
        EXPECT_EQ(expected_tasks, plan->get_task_count());
        for (int64_t task_idx = 0; task_idx < plan->get_task_count(); ++task_idx) {
          ObIVFMergeWorkPerf perf;
          ASSERT_EQ(OB_SUCCESS, processor.get_work_perf(task_idx, perf));
          const bool skipped = !no_progress && (mixed ? depth == 0 && task_idx == 1 : depth < last_depth);
          EXPECT_EQ(skipped ? 0 : no_progress ? vector_count : 2, perf.input_vectors_);
          EXPECT_EQ(skipped ? 0 : 1, perf.centers_);
          if (mixed && depth == 0) {
            EXPECT_EQ(task_idx == 2 ? 3 : task_idx, perf.center_idx_);
          }
        }
      }
      // Completion order must not change task-slot ownership or leaf order.
      for (int64_t task_idx = plan->get_task_count(); task_idx-- > 0;) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, task_idx));
      }
      if (update) {
        const int64_t expected_stats = no_progress ? 1 : mixed ? (depth == 0 ? 2 : 3)
            : depth == last_depth ? 8 : 0;
        EXPECT_EQ(expected_stats, algorithm.stats_calls_ - stats_before);
        EXPECT_EQ(no_progress ? vector_count : 2 * expected_stats,
            vectors.read_vectors_ - reads_before);
      }
      if (update && depth == last_depth) {
        ObIVFMergeStepResult result;
        ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::finish_hierarchy_step(processor, *plan, result));
        EXPECT_TRUE(result.committed_);
        EXPECT_TRUE(result.hierarchy_finished_);
        finished = true;
      } else {
        const ObIVFTaskPlan *next = nullptr;
        ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
        ASSERT_NE(nullptr, next);
        if (update) {
          EXPECT_EQ(ObIVFMergeStep::HIERARCHY_CLUSTER, planner.get_step());
          EXPECT_EQ(depth + 1, planner.get_depth());
        }
        plan = next;
      }
    }
    ASSERT_TRUE(finished);
    EXPECT_TRUE(ObIVFMergeTestPeer::is_hierarchy_finished(processor));
    EXPECT_EQ(vector_count, algorithm.stats_vectors_);
    EXPECT_EQ(vector_count, vectors.read_vectors_);
    const int64_t expected_k = no_progress ? 1 : mixed ? 5 : 8;
    EXPECT_EQ(expected_k, ObIVFMergeTestPeer::get_k(processor));
    EXPECT_EQ(expected_k, algorithm.stats_calls_);
    EXPECT_EQ(last_depth + 1, ObIVFMergeTestPeer::get_actual_max_depth(processor));
    EXPECT_EQ(no_progress ? 1 : 0, ObIVFMergeTestPeer::get_no_progress_leaf_count(processor));
    EXPECT_EQ(no_progress ? vector_count : 0, ObIVFMergeTestPeer::get_no_progress_member_count(processor));
    if (no_progress) {
      EXPECT_EQ(3, vectors.read_calls_);
      EXPECT_EQ(256, vectors.max_read_rows_);
    }
    const int64_t mixed_first_vids[] = {1, 9, 3, 5, 7};
    for (int64_t cid = 0; cid < expected_k; ++cid) {
      const int64_t member_count = no_progress ? vector_count : 2;
      const int64_t first_vid = no_progress ? 1 : mixed ? mixed_first_vids[cid] : 2 * cid + 1;
      EXPECT_EQ(member_count, ObIVFMergeTestPeer::get_member_count(processor, cid));
      float expected[2] = {0.0F, 0.0F};
      for (int64_t i = 0; i < member_count; ++i) {
        ObIVFVectorVID vid = 0;
        ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::get_member_vid(processor, cid, i, vid));
        EXPECT_EQ(first_vid + i, vid);
        const int64_t ordinal = first_vid + i - 1;
        expected[0] += static_cast<float>((ordinal % 8) * 100);
        expected[1] += static_cast<float>((ordinal / 8) * 100);
      }
      for (int64_t d = 0; d < 2; ++d) {
        float actual = 0.0F;
        ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::get_center_value(processor, cid, d, actual));
        const float mean = expected[d] / member_count;
        EXPECT_EQ(0, MEMCMP(&mean, &actual, sizeof(float)));
      }
    }
  }
}

namespace {
class SourceOrderStore final : public ReuseVectorStore
{
public:
  SourceOrderStore() : ReuseVectorStore(12289, 1) {}
  bool enabled_ = true;
  bool duplicate_ = false;
  bool invalid_ = false;
  int read_error_ = OB_SUCCESS;
  bool short_read_ = false;
  ObIVFMergeProcessor *cancel_on_read_ = nullptr;
  mutable std::atomic<int64_t> source_calls_{0};
  mutable std::atomic<int64_t> source_rows_{0};
  bool supports_source_order() const override { return enabled_; }
  int64_t get_available_replay_memory() const override { return INT64_MAX; }
  int get_source_row_idx(int64_t idx, int64_t &source) const override
  {
    if (idx < 0 || idx >= count_) { return OB_INVALID_ARGUMENT; }
    source = invalid_ ? count_ : count_ - 1 - (duplicate_ && idx == 2 ? 1 : idx);
    return OB_SUCCESS;
  }
  int batch_get_by_vid(const ObIVFVectorIDView &vids, int64_t begin, int64_t end,
      share::ObVectorMatrix &values) const override
  {
    int ret = OB_SUCCESS;
    values.reuse();
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      const int64_t idx = vids.at(i) - 1;
      if (idx < 0 || idx >= count_) { ret = OB_INVALID_ARGUMENT; }
      else if (OB_FAIL(values.push_back(idx == 0 ? -10000.0F : idx % 2 ? -100.0F : 100.0F))) {}
      else { ret = values.push_back(idx == 0 ? 0.0F : idx % 11 == 0 ? 15.0F : (idx / 2 % 4) * 10.0F); }
    }
    return ret;
  }
  int batch_get_by_source(const share::ObVectorArray<int64_t> &idxs, share::ObVectorMatrix &values) const override
  {
    ++source_calls_;
    if (cancel_on_read_ != nullptr) { cancel_on_read_->cancel(OB_CANCELED); }
    source_rows_ += idxs.count();
    if (read_error_ != OB_SUCCESS) { return read_error_; }
    ObArray<ObIVFVectorVID> vids;
    int ret = OB_SUCCESS;
    int64_t previous = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < idxs.count(); ++i) {
      int64_t source = -1;
      if (OB_FAIL(get_source_row_idx(idxs.at(i), source))) {}
      else if (source <= previous || idxs.at(i) == 0) { ret = OB_INVALID_DATA; }
      else { previous = source; ret = vids.push_back(idxs.at(i) + 1); }
    }
    if (OB_SUCC(ret)) { ret = batch_get_by_vid(vids, 0, vids.count(), values); }
    if (OB_SUCC(ret) && short_read_ && !values.empty()) { values.pop_back(); }
    return ret;
  }
};

class SourceAssignAlgorithm final : public ObIVFHcaAlgorithmAdapter
{
public:
  bool prepared_enabled_ = false;
  bool supports_prepared_cluster() const override { return prepared_enabled_; }
  bool enabled_ = true;
  bool supports_loaded_assign() const override { return enabled_; }
  bool update_enabled_ = false;
  bool supports_source_update() const override { return update_enabled_; }
  int hca_train_local_centers(int64_t depth, int64_t node, ObIVFVectorReader &reader,
      const ObIVFVectorIDView &vids, const ObIVFFirstLevelSampleSet *samples,
      int64_t cluster_size, int64_t &k, share::ObVectorMatrix &centers) override
  {
    UNUSEDx(node, reader, samples, cluster_size);
    int ret = OB_SUCCESS;
    centers.reuse();
    k = depth == 0 ? 4 : depth == 1 ? 3 : 1;
    for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
      const float x = depth == 0 ? (cid == 1 ? -100 : cid == 2 ? 100 : -10000)
          : ((vids.at(0) - 1) % 2 ? -100 : 100);
      const float y = depth == 0 ? (cid == 1 || cid == 2 ? 15 : 0)
          : depth == 1 ? (cid == 0 ? 0 : 30) : 15;
      if (OB_FAIL(centers.push_back(x))) {} else { ret = centers.push_back(y); }
    }
    return ret;
  }
};

void run_source_hierarchy(SourceOrderStore &store, const ObIVFProcessType type,
    bool algorithm_enabled, bool parallel, ObArray<ObIVFVectorVID> &members,
    ObArray<float> &centers, ObArray<int64_t> &counts, int64_t &source_tasks,
    bool update_enabled = false)
{
  ObArenaAllocator allocator;
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(store, store, 64, false, true, false, VIDA_L2));
  SourceAssignAlgorithm algorithm;
  algorithm.enabled_ = algorithm_enabled;
  algorithm.update_enabled_ = update_enabled;
  ObIVFHcaAlgorithmParam param;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  ReuseCountingOutput output;
  ObIVFMergeProcessor processor;
  ObIVFFullBuildPlanner planner;
  ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type));
  ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, type));
  const ObIVFTaskPlan *plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(plan));
  bool finished = false;
  source_tasks = 0;
  for (int64_t step = 0; !finished && step < 12; ++step) {
    ASSERT_NE(nullptr, plan);
    const bool source = store.enabled_ && planner.get_depth() > 0
        && ((algorithm_enabled && planner.get_step() == ObIVFMergeStep::HIERARCHY_ASSIGN)
            || (update_enabled && planner.get_depth() == 2
                && planner.get_step() == ObIVFMergeStep::HIERARCHY_UPDATE));
    int64_t inputs = 0;
    for (int64_t t = 0; t < plan->get_task_count(); ++t) {
      ObIVFMergeWorkPerf perf;
      ASSERT_EQ(OB_SUCCESS, processor.get_work_perf(t, perf));
      if (source) {
        EXPECT_EQ(-1, perf.node_idx_);
        EXPECT_EQ(-1, perf.centers_);
        EXPECT_GT(perf.input_vectors_, 0);
        EXPECT_LE(perf.input_vectors_, 4096);
        inputs += perf.input_vectors_;
        ++source_tasks;
      } else { EXPECT_GE(perf.node_idx_, 0); }
    }
    if (source) { EXPECT_EQ(store.count_ - 1, inputs); }
    if (source && parallel) {
      std::vector<int> results(plan->get_task_count(), OB_ERR_UNEXPECTED);
      std::vector<std::thread> workers;
      for (int64_t t = plan->get_task_count(); t-- > 0;) {
        workers.emplace_back([&, t]() { results[t] = planner.process_task(*plan, t); });
      }
      for (auto &worker : workers) { worker.join(); }
      for (int result : results) { ASSERT_EQ(OB_SUCCESS, result); }
    } else {
      for (int64_t t = plan->get_task_count(); t-- > 0;) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t));
      }
    }
    if (planner.get_step() == ObIVFMergeStep::HIERARCHY_UPDATE && planner.get_depth() == 2) {
      ObIVFMergeStepResult result;
      ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::finish_hierarchy_step(processor, *plan, result));
      ASSERT_TRUE(result.hierarchy_finished_);
      finished = true;
    } else {
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
      plan = next;
    }
  }
  ASSERT_TRUE(finished);
  for (int64_t cid = 0; cid < ObIVFMergeTestPeer::get_k(processor); ++cid) {
    const int64_t n = ObIVFMergeTestPeer::get_member_count(processor, cid);
    ASSERT_EQ(OB_SUCCESS, counts.push_back(n));
    for (int64_t i = 0; i < n; ++i) {
      ObIVFVectorVID vid = 0;
      ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::get_member_vid(processor, cid, i, vid));
      ASSERT_EQ(OB_SUCCESS, members.push_back(vid));
    }
    for (int64_t d = 0; d < 2; ++d) {
      float value = 0;
      ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::get_center_value(processor, cid, d, value));
      ASSERT_EQ(OB_SUCCESS, centers.push_back(value));
    }
  }
}
} // namespace

TEST(TestIVFInitialBuildRuntime, source_assign_preserves_members_centers_and_merge_path)
{
  ObArray<ObIVFVectorVID> expected_members;
  ObArray<float> expected_centers;
  ObArray<int64_t> expected_counts;
  int64_t tasks = 0;
  SourceOrderStore baseline;
  baseline.enabled_ = false;
  run_source_hierarchy(baseline, ObIVFProcessType::INITIAL_BUILD, true, false,
      expected_members, expected_centers, expected_counts, tasks);
  ASSERT_EQ(baseline.count_, expected_members.count());
  ASSERT_EQ(5, expected_counts.count()); // one root leaf, four no-progress leaves
  for (int mode = 0; mode < 4; ++mode) {
    SCOPED_TRACE(mode);
    SourceOrderStore store;
    ObArray<ObIVFVectorVID> members;
    ObArray<float> centers;
    ObArray<int64_t> counts;
    const auto type = mode == 2 ? ObIVFProcessType::FULL_REBUILD : ObIVFProcessType::INITIAL_BUILD;
    run_source_hierarchy(store, type, mode != 3, mode == 1, members, centers, counts, tasks);
    ASSERT_EQ(expected_members.count(), members.count());
    ASSERT_EQ(expected_centers.count(), centers.count());
    ASSERT_EQ(expected_counts.count(), counts.count());
    for (int64_t i = 0; i < members.count(); ++i) { EXPECT_EQ(expected_members.at(i), members.at(i)); }
    for (int64_t i = 0; i < centers.count(); ++i) {
      EXPECT_EQ(0, MEMCMP(&expected_centers.at(i), &centers.at(i), sizeof(float)));
    }
    for (int64_t i = 0; i < counts.count(); ++i) { EXPECT_EQ(expected_counts.at(i), counts.at(i)); }
    if (mode < 3) {
      EXPECT_GT(tasks, 0);
      EXPECT_EQ(tasks, store.source_calls_.load());
      EXPECT_EQ(2 * (store.count_ - 1), store.source_rows_.load());
    } else {
      EXPECT_EQ(0, tasks);
      EXPECT_EQ(0, store.source_calls_.load());
    }
  }
}

TEST(TestIVFInitialBuildRuntime, rebuild_and_ddl_prepare_cluster_samples_in_source_order)
{
  for (auto type : {ObIVFProcessType::INITIAL_BUILD, ObIVFProcessType::FULL_REBUILD}) {
    ObArenaAllocator allocator;
    SourceOrderStore store;
    SourceAssignAlgorithm algorithm;
    algorithm.prepared_enabled_ = true;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(store, store, 64, false, true, false, VIDA_L2));
    ReuseCountingOutput output;
    ObIVFMergeProcessor processor;
    ObIVFFullBuildPlanner planner;
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type));
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, type));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    bool prepared = false;
    for (int step = 0; step < 6 && plan != nullptr; ++step) {
      const bool sample = planner.get_step() == ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE;
      const auto before = store.source_rows_.load();
      for (int64_t t = plan->get_task_count(); t-- > 0;) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
      plan = next;
      if (sample) {
        EXPECT_GT(store.source_rows_.load() - before, 0);
        EXPECT_LE(store.source_rows_.load() - before, store.count_ - 1);
        EXPECT_EQ(ObIVFMergeStep::HIERARCHY_CLUSTER, planner.get_step());
        // Consume the staged samples too, validating the source-to-node ordinal mapping.
        for (int64_t t = 0; t < plan->get_task_count(); ++t) {
          ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t));
        }
        prepared = true;
        break;
      }
    }
    EXPECT_TRUE(prepared);
  }
}

TEST(TestIVFInitialBuildRuntime, source_assign_rejects_bad_mapping_and_failed_reads)
{
  for (int mode = 0; mode < 6; ++mode) {
    SCOPED_TRACE(mode);
    SourceOrderStore store;
    store.duplicate_ = mode == 0;
    store.invalid_ = mode == 1;
    store.read_error_ = mode == 2 ? OB_IO_ERROR : mode == 3 ? OB_CANCELED : OB_SUCCESS;
    store.short_read_ = mode == 4;
    ObArenaAllocator allocator;
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(store, store, 64, false, true, false, VIDA_L2));
    SourceAssignAlgorithm algorithm;
    ObIVFHcaAlgorithmParam param;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
    ReuseCountingOutput output;
    ObIVFMergeProcessor processor;
    ObIVFFullBuildPlanner planner;
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, ObIVFProcessType::INITIAL_BUILD));
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, ObIVFProcessType::INITIAL_BUILD));
    if (mode == 5) { store.cancel_on_read_ = &processor; }
    const ObIVFTaskPlan *plan = nullptr;
    int ret = planner.start(plan);
    for (int step = 0; ret == OB_SUCCESS && plan != nullptr && step < 8; ++step) {
      for (int64_t t = 0; ret == OB_SUCCESS && t < plan->get_task_count(); ++t) {
        ret = planner.process_task(*plan, t);
      }
      if (ret == OB_SUCCESS) {
        const ObIVFTaskPlan *next = nullptr;
        ret = planner.finish_task_plan(*plan, next);
        plan = next;
      }
    }
    EXPECT_EQ(mode <= 1 || mode == 4 ? OB_INVALID_DATA
        : mode == 5 ? OB_CANCELED : store.read_error_, ret);
    EXPECT_EQ(0, output.raw_count_);
    EXPECT_EQ(0, output.primary_count_);
    processor.cancel(ret);
    EXPECT_TRUE(ObIVFMergeTestPeer::is_canceled(processor));
    // Cancellation keeps in-flight state alive; scope destruction drains the
    // test-owned processor after all synchronous tasks have returned.
    if (mode >= 2) { EXPECT_TRUE(ObIVFMergeTestPeer::has_active_plan(processor)); }
  }
}

namespace {
struct SourceUpdateFixture
{
  ObArenaAllocator allocator_;
  SourceOrderStore store_;
  SourceAssignAlgorithm algorithm_;
  ObIVFMergeInput input_;
  ReuseCountingOutput output_;
  ObIVFMergeProcessor processor_;
  ObIVFFullBuildPlanner planner_;
  const ObIVFTaskPlan *plan_ = nullptr;
  ObVectorNormalizeInfo norm_;

  void start(bool normalized = false, ObIVFProcessType type = ObIVFProcessType::INITIAL_BUILD)
  {
    // More than 32 source windows, with every leaf spanning many windows.
    store_.count_ = 131073;
    algorithm_.update_enabled_ = true;
    ObIVFHcaAlgorithmParam param;
    param.center_norm_info_ = normalized ? &norm_ : nullptr;
    ASSERT_EQ(OB_SUCCESS, algorithm_.init(param));
    ASSERT_EQ(OB_SUCCESS, input_.init(store_, store_, 64, false, true, false, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, processor_.init(allocator_, input_, algorithm_, output_,
        type));
    ASSERT_EQ(OB_SUCCESS, planner_.init(allocator_, processor_, type));
    ASSERT_EQ(OB_SUCCESS, planner_.start(plan_));
    for (int step = 0; step < 12; ++step) {
      if (planner_.get_step() == ObIVFMergeStep::HIERARCHY_UPDATE && planner_.get_depth() == 2) {
        ASSERT_NE(nullptr, plan_);
        EXPECT_EQ(32, plan_->get_task_count());
        return;
      }
      ASSERT_NE(nullptr, plan_);
      for (int64_t i = 0; i < plan_->get_task_count(); ++i) {
        ASSERT_EQ(OB_SUCCESS, planner_.process_task(*plan_, i));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner_.finish_task_plan(*plan_, next));
      plan_ = next;
    }
    FAIL() << "leaf UPDATE not reached";
  }

  void finish_and_check(bool normalized)
  {
    ObIVFMergeStepResult result;
    ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::finish_hierarchy_step(processor_, *plan_, result));
    ASSERT_TRUE(result.hierarchy_finished_);
    int64_t total = 0;
    for (int64_t cid = 0; cid < ObIVFMergeTestPeer::get_k(processor_); ++cid) {
      ObArray<ObIVFVectorVID> vids;
      share::ObVectorMatrix values;
      const int64_t n = ObIVFMergeTestPeer::get_member_count(processor_, cid);
      total += n;
      for (int64_t i = 0; i < n; ++i) {
        ObIVFVectorVID vid = 0;
        ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::get_member_vid(processor_, cid, i, vid));
        if (!vids.empty()) { EXPECT_LT(vids.at(vids.count() - 1), vid); }
        ASSERT_EQ(OB_SUCCESS, vids.push_back(vid));
      }
      ASSERT_EQ(OB_SUCCESS, store_.batch_get_by_vid(vids, 0, n, values));
      double sums[2] = {0, 0};
      for (int64_t i = 0; i < n; ++i) {
        for (int d = 0; d < 2; ++d) { sums[d] += values.at(i * 2 + d); }
      }
      float expected[2] = {static_cast<float>(sums[0] / n), static_cast<float>(sums[1] / n)};
      if (normalized) { ASSERT_EQ(OB_SUCCESS, hca_normalize_center(expected, 2, norm_)); }
      for (int d = 0; d < 2; ++d) {
        float value = 0;
        ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::get_center_value(processor_, cid, d, value));
        EXPECT_NEAR(expected[d], value, normalized ? 1e-6F : 1e-4F);
      }
    }
    EXPECT_EQ(store_.count_, total);
    EXPECT_EQ(0, output_.raw_count_);
  }
};
}

TEST(TestIVFInitialBuildRuntime, source_update_capability_enables_merge_reads)
{
  SourceOrderStore store;
  // Merge uses the same physical source windows while retaining VID membership order.
  ObArray<ObIVFVectorVID> members;
  ObArray<float> centers;
  ObArray<int64_t> counts;
  int64_t tasks = 0;
  run_source_hierarchy(store, ObIVFProcessType::FULL_REBUILD, true, true,
      members, centers, counts, tasks, true);
  EXPECT_EQ(store.count_, members.count());
  EXPECT_EQ(5, counts.count());
  EXPECT_EQ(tasks, store.source_calls_.load());
  EXPECT_GT(tasks, 0);
  // The earlier level has no leaf statistics to update; only the final UPDATE reads vectors.
  EXPECT_EQ(3 * (store.count_ - 1), store.source_rows_.load());
}

TEST(TestIVFInitialBuildRuntime, source_update_reads_each_leaf_once_and_reduces_concurrently)
{
  for (auto type : {ObIVFProcessType::INITIAL_BUILD, ObIVFProcessType::FULL_REBUILD}) {
  for (bool normalize : {false, true}) {
    SourceUpdateFixture f;
    ASSERT_NO_FATAL_FAILURE(f.start(normalize, type));
    const int64_t before = f.store_.source_rows_.load();
    std::vector<int> results(f.plan_->get_task_count(), OB_ERR_UNEXPECTED);
    std::vector<std::thread> workers;
    int64_t input_count = 0;
    for (int64_t t = 0; t < f.plan_->get_task_count(); ++t) {
      ObIVFMergeWorkPerf perf;
      ASSERT_EQ(OB_SUCCESS, f.processor_.get_work_perf(t, perf));
      EXPECT_EQ(-1, perf.node_idx_);
      EXPECT_GT(perf.input_vectors_, 0);
      EXPECT_LE(perf.input_vectors_, 4096);
      input_count += perf.input_vectors_;
    }
    EXPECT_EQ(f.store_.count_ - 1, input_count);
    for (int64_t t = f.plan_->get_task_count(); t-- > 0;) {
      workers.emplace_back([&, t]() { results[t] = f.planner_.process_task(*f.plan_, t); });
    }
    for (auto &worker : workers) { worker.join(); }
    for (int result : results) { ASSERT_EQ(OB_SUCCESS, result); }
    EXPECT_EQ(f.store_.count_ - 1, f.store_.source_rows_.load() - before);
    ASSERT_NO_FATAL_FAILURE(f.finish_and_check(normalize));
  }
  }
}

TEST(TestIVFInitialBuildRuntime, source_update_failed_batch_can_retry_without_double_count)
{
  for (int mode = 0; mode < 2; ++mode) {
    SourceUpdateFixture f;
    ASSERT_NO_FATAL_FAILURE(f.start());
    // Commit another source window first: failure must leave its sums intact.
    ASSERT_EQ(OB_SUCCESS, f.planner_.process_task(*f.plan_, 1));
    f.store_.read_error_ = mode == 0 ? OB_IO_ERROR : OB_SUCCESS;
    f.store_.short_read_ = mode == 1;
    ObIVFTaskResult result;
    ASSERT_EQ(OB_SUCCESS, result.init(*f.plan_, 0));
    EXPECT_EQ(mode == 0 ? OB_IO_ERROR : OB_INVALID_DATA,
        f.processor_.execute_task(ObIVFMergeStage::HIERARCHY, ObIVFMergeStep::HIERARCHY_UPDATE,
            2, 0, result));
    // Exercise the processor retry seam; the public planner cancels failures.
    f.store_.read_error_ = OB_SUCCESS;
    f.store_.short_read_ = false;
    for (int64_t t = 0; t < f.plan_->get_task_count(); ++t) {
      if (t != 1) { ASSERT_EQ(OB_SUCCESS, f.planner_.process_task(*f.plan_, t)); }
    }
    ASSERT_NO_FATAL_FAILURE(f.finish_and_check(false));
  }
}

TEST(TestIVFInitialBuildRuntime, source_update_cancellation_and_incomplete_barrier_publish_no_centers)
{
  SourceUpdateFixture f;
  ASSERT_NO_FATAL_FAILURE(f.start());
  ASSERT_EQ(OB_SUCCESS, f.planner_.process_task(*f.plan_, 0));
  ObIVFMergeStepResult result;
  EXPECT_EQ(OB_STATE_NOT_MATCH,
      ObIVFMergeTestPeer::finish_hierarchy_step(f.processor_, *f.plan_, result));
  EXPECT_FALSE(ObIVFMergeTestPeer::is_hierarchy_finished(f.processor_));
  f.store_.cancel_on_read_ = &f.processor_;
  EXPECT_EQ(OB_CANCELED, f.planner_.process_task(*f.plan_, 1));
  EXPECT_TRUE(ObIVFMergeTestPeer::is_canceled(f.processor_));
  EXPECT_EQ(0, f.output_.raw_count_);
  EXPECT_EQ(0, f.output_.primary_count_);
}

TEST(TestIVFHcaAdapter, source_update_numeric_seam_preserves_kernel_and_normalization)
{
  for (int64_t dim : {1, 17, 768}) {
    for (bool normalize : {false, true}) {
      ObVectorNormalizeInfo norm;
      ObIVFHcaAlgorithmParam param;
      param.center_norm_info_ = normalize ? &norm : nullptr;
      ObIVFHcaAlgorithmAdapter algorithm;
      ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
      ObArray<float> sum, expected, vector;
      share::ObVectorMatrix center;
      ASSERT_EQ(OB_SUCCESS, sum.prepare_allocate(dim));
      ASSERT_EQ(OB_SUCCESS, expected.prepare_allocate(dim));
      ASSERT_EQ(OB_SUCCESS, vector.prepare_allocate(dim));
      for (int64_t d = 0; d < dim; ++d) { sum.at(d) = expected.at(d) = 0; }
      for (int64_t i = 0; i < 625; ++i) {
        for (int64_t d = 0; d < dim; ++d) {
          vector.at(d) = static_cast<float>((i * 13 + d * 7) % 31 - 15) / 31.0F;
        }
        algorithm.hca_accumulate_center_stats(&sum.at(0), &vector.at(0), dim);
        hca_simd::axpy(&expected.at(0), &vector.at(0), 1.0F, dim);
      }
      for (int64_t d = 0; d < dim; ++d) { expected.at(d) /= 625; }
      if (param.center_norm_info_ != nullptr) {
        ASSERT_EQ(OB_SUCCESS, hca_normalize_center(&expected.at(0), dim, *param.center_norm_info_));
      }
      ASSERT_EQ(OB_SUCCESS, algorithm.hca_finalize_center_stats(&sum.at(0), 625, dim, center));
      EXPECT_EQ(0, MEMCMP(&expected.at(0), &center.at(0), dim * sizeof(float)));
      // Reversing arrival order models source order differing from member
      // order. Compare to a double-precision reference, not to the new loop.
      std::vector<double> reference(dim, 0.0);
      for (int64_t d = 0; d < dim; ++d) { sum.at(d) = 0; }
      for (int64_t i = 625; i-- > 0;) {
        for (int64_t d = 0; d < dim; ++d) {
          vector.at(d) = static_cast<float>((i * 13 + d * 7) % 31 - 15) / 31.0F;
          reference[d] += vector.at(d);
        }
        algorithm.hca_accumulate_center_stats(&sum.at(0), &vector.at(0), dim);
      }
      for (int64_t d = 0; d < dim; ++d) { expected.at(d) = reference[d] / 625; }
      if (param.center_norm_info_ != nullptr) {
        ASSERT_EQ(OB_SUCCESS, hca_normalize_center(&expected.at(0), dim, *param.center_norm_info_));
      }
      ASSERT_EQ(OB_SUCCESS, algorithm.hca_finalize_center_stats(&sum.at(0), 625, dim, center));
      for (int64_t d = 0; d < dim; ++d) { EXPECT_NEAR(expected.at(d), center.at(d), 1e-5F); }
      EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.hca_finalize_center_stats(nullptr, 625, dim, center));
      EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.hca_finalize_center_stats(&sum.at(0), 0, dim, center));
    }
  }
}

TEST(TestIVFInitialBuildRuntime, builds_multiple_clusters_without_merge_prior)
{
  ObArenaAllocator allocator;
  ReuseVectorStore vectors;
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 64,
      false /* normalized */, true /* rotated */, true /* enable_soar */, VIDA_L2));
  RecordingReuseAlgorithm algorithm;
  ObIVFHcaAlgorithmParam algorithm_param;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(algorithm_param));
  ReuseCountingOutput output;
  ObIVFMergeProcessor processor;
  ObIVFFullBuildPlanner planner;
  ASSERT_EQ(OB_SUCCESS, processor.init(
      allocator, input, algorithm, output, ObIVFProcessType::INITIAL_BUILD));
  ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, ObIVFProcessType::INITIAL_BUILD));
  const ObIVFTaskPlan *plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(plan));
  while (plan != nullptr) {
    ASSERT_EQ(ObIVFProcessType::INITIAL_BUILD, plan->get_process_type());
    ASSERT_NE(ObIVFMergeStage::PRIOR, planner.get_stage());
    for (int64_t task_idx = 0; task_idx < plan->get_task_count(); ++task_idx) {
      ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, task_idx));
    }
    const ObIVFTaskPlan *next = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
    plan = next;
  }
  const ObIVFDagArtifact *base_artifact = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.get_artifact(base_artifact));
  const auto &artifact = *static_cast<const ObIVFMergeArtifact *>(base_artifact);
  EXPECT_EQ(ObIVFProcessType::INITIAL_BUILD, artifact.selected_type_);
  EXPECT_EQ(ObIVFProcessType::INITIAL_BUILD, artifact.effective_type_);
  EXPECT_EQ(vectors.count_, artifact.vector_count_);
  EXPECT_GT(artifact.cluster_count_, 1);
  EXPECT_EQ(vectors.count_, output.primary_count_);
  EXPECT_EQ(vectors.count_, output.raw_count_);
  EXPECT_EQ(0, algorithm.warm_calls_);
  EXPECT_TRUE(ObIVFMergeTestPeer::are_quant_intermediates_released(processor));
}

TEST(TestIVFReuseMerge, selector_uses_live_counts_and_prior_availability)
{
  ObArenaAllocator allocator;
  ReuseVectorStore store;
  ObIVFMergePrior prior(allocator);
  ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, store, 2));
  ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, store));
  ConcatTestSource source(prior); prior.posting_source_ = &source;
  ObIVFProcessType selected;
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 128, selected));
  EXPECT_EQ(ObIVFProcessType::CONCATENATION, selected);
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 129, selected));
  EXPECT_EQ(ObIVFProcessType::FULL_REBUILD, selected);
  ObIVFMergePrior dominant(allocator);
  ASSERT_NO_FATAL_FAILURE(make_reuse_prior(dominant, store, 1));
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(dominant, 128, selected));
  EXPECT_EQ(ObIVFProcessType::FULL_REBUILD, selected); // No membership: never choose legacy insertion.
  ASSERT_NO_FATAL_FAILURE(make_reuse_postings(dominant, store));
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(dominant, 128, selected));
  EXPECT_EQ(ObIVFProcessType::INSERTION, selected);
  dominant.posting_mass_.at(0) = -1;
  EXPECT_EQ(OB_INVALID_DATA, ObIVFMergeStrategy::select(dominant, 128, selected));
}

TEST(TestIVFReuseMerge, errsim_overrides_heuristics_but_preserves_input_contract)
{
  const char *names[] = {"ERRSIM_IVF_FORCE_FULL_REBUILD", "ERRSIM_IVF_FORCE_CONCAT",
      "ERRSIM_IVF_FORCE_INSERTION"};
  struct ResetEvents {
    const char **names_;
    ~ResetEvents() {
      for (int64_t i = 0; i < 3; ++i) { EventTable::set_event(names_[i], EventItem()); }
    }
  } reset{names};
  ObArenaAllocator allocator;
  ReuseVectorStore store;
  ObIVFMergePrior prior(allocator);
  ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, store, 2));
  ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, store));
  ConcatTestSource source(prior); prior.posting_source_ = &source;
  const ObIVFProcessType expected[] = {ObIVFProcessType::FULL_REBUILD,
      ObIVFProcessType::CONCATENATION, ObIVFProcessType::INSERTION};
  EventItem enabled;
  enabled.error_code_ = OB_ERR_UNEXPECTED;
  enabled.occur_ = 100;
  ObIVFProcessType selected;
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, EventTable::set_event(names[i], enabled));
    EXPECT_EQ(i != 0, ObIVFMergeStrategy::should_capture_prior(prior.vector_count_, 1024));
    // Rebuild overrides natural concat; the other two override small input
    // (and insertion also overrides the 50% dominant ratio).
    ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, i == 0 ? 128 : 1024, selected));
    EXPECT_EQ(expected[i], selected);
    ASSERT_EQ(OB_SUCCESS, EventTable::set_event(names[i], EventItem()));
  }
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 1024, selected));
  EXPECT_EQ(ObIVFProcessType::FULL_REBUILD, selected);
  EXPECT_FALSE(ObIVFMergeStrategy::should_capture_prior(prior.vector_count_, 1024));
  enabled.occur_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event(names[2], enabled));
  EXPECT_TRUE(ObIVFMergeStrategy::should_capture_prior(prior.vector_count_, 1024));
  EXPECT_TRUE(ObIVFMergeStrategy::should_capture_prior(prior.vector_count_, 1024));
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 1024, selected));
  EXPECT_EQ(ObIVFProcessType::INSERTION, selected);
  EXPECT_FALSE(ObIVFMergeStrategy::should_capture_prior(prior.vector_count_, 1024));
  enabled.occur_ = 100;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event(names[2], enabled));
  prior.postings_complete_ = false;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObIVFMergeStrategy::select(prior, 1024, selected));
  prior.postings_complete_ = true;
  prior.posting_mass_.at(0) = -1;
  EXPECT_EQ(OB_INVALID_DATA, ObIVFMergeStrategy::select(prior, 1024, selected));
  prior.posting_mass_.at(0) = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event(names[0], enabled));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObIVFMergeStrategy::select(prior, 1024, selected));
}

TEST(TestIVFReuseMerge, selector_distinguishes_coverage_mass_and_dominant_threshold)
{
  ObArenaAllocator allocator;
  ReuseVectorStore vectors;
  ObIVFMergePrior prior(allocator);
  ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 1));
  ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, vectors));
  ObIVFSegmentPriorInfo raw_only;
  raw_only.centroid_begin_ = prior.posting_mass_.count();
  raw_only.live_vector_count_ = 128;
  ASSERT_EQ(OB_SUCCESS, prior.segments_.push_back(raw_only));
  prior.vector_count_ = 640;
  // 512 / 640 = exactly 80%; unsupported priors still contribute live RAW.
  ObIVFProcessType type;
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 128, type));
  EXPECT_EQ(ObIVFProcessType::INSERTION, type);
  ++prior.segments_.at(1).live_vector_count_;
  ++prior.vector_count_;
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 128, type));
  EXPECT_EQ(ObIVFProcessType::FULL_REBUILD, type); // Incomplete coverage cannot concatenate.
  // SOAR mass is not a substitute for the number of reusable centers.
  prior.posting_mass_.pop_back();
  prior.old_cids_.pop_back();
  prior.centroids_.pop_back();
  prior.centroids_.pop_back();
  --prior.segments_.at(0).centroid_count_;
  --prior.segments_.at(1).centroid_begin_;
  prior.posting_mass_.at(0) = 1000000;
  ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 128, type));
  EXPECT_EQ(ObIVFProcessType::FULL_REBUILD, type);
  // Missing/shifted metadata is an error, never a fallback decision.
  prior.old_cids_.pop_back();
  EXPECT_NE(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 128, type));
}

TEST(TestIVFInitialBuildRuntime, prepared_samples_preserve_training_bits)
{
  for (const bool normalize : {false, true}) {
    for (const int64_t count : {17, 256, 257, 1023, 8193, 33001}) {
      for (const uint64_t seed : {0UL, 42UL, 987654321UL}) {
        SCOPED_TRACE(count);
        SCOPED_TRACE(seed);
        SCOPED_TRACE(normalize);
        ReuseVectorStore store(count, 17);
        store.jitter_ = true;
        ObIVFVectorReader reader;
        share::ObVectorArray<ObIVFVectorVID> vids;
        ObArray<ObIVFVectorVID> selected_vids;
        ObArray<int64_t> ordinals;
        share::ObVectorMatrix values, old_centers, new_centers;
        ObVectorNormalizeInfo norm;
        ObIVFHcaAlgorithmParam param;
        param.train_param_.random_seed_ = seed;
        param.train_param_.max_iterations_ = 3;
        param.center_norm_info_ = normalize ? &norm : nullptr;
        ObIVFHcaAlgorithmAdapter algorithm;
        ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
        ASSERT_EQ(OB_SUCCESS, reader.init(store));
        ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
        int64_t old_k = 0, new_k = 0;
        ASSERT_EQ(OB_SUCCESS, algorithm.hca_train_local_centers(2, 0, reader,
            vids, nullptr, 256, old_k, old_centers));
        ASSERT_EQ(OB_SUCCESS, algorithm.hca_collect_training_rows(count, 256, ordinals));
        for (int64_t i = 0; i < ordinals.count(); ++i) {
          ASSERT_GE(ordinals.at(i), 0); ASSERT_LT(ordinals.at(i), count);
          if (i > 0) { ASSERT_LT(ordinals.at(i - 1), ordinals.at(i)); }
          ASSERT_EQ(OB_SUCCESS, selected_vids.push_back(vids.at(ordinals.at(i))));
        }
        if (old_k > 1) { EXPECT_LE(ordinals.count(), MIN(count, old_k * 65)); }
        if (old_k == 1) { EXPECT_EQ(count, ordinals.count()); }
        ASSERT_EQ(OB_SUCCESS, store.batch_get_by_vid(selected_vids, 0, selected_vids.count(), values));
        const int64_t reads_before = store.read_calls_;
        ASSERT_EQ(OB_SUCCESS, algorithm.hca_train_loaded_samples(count, 256, 2,
            ordinals, values, new_k, new_centers));
        EXPECT_EQ(reads_before, store.read_calls_);
        ASSERT_EQ(old_k, new_k); ASSERT_EQ(old_centers.count(), new_centers.count());
        EXPECT_EQ(0, MEMCMP(&old_centers.at(0), &new_centers.at(0), old_centers.count() * sizeof(float)));
      }
    }
  }
}

TEST(TestIVFInitialBuildRuntime, prepared_samples_reject_missing_and_invalid_rows)
{
  ReuseVectorStore store(1024, 17);
  ObIVFHcaAlgorithmAdapter algorithm;
  ObIVFHcaAlgorithmParam param;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  ObArray<int64_t> ids;
  ObArray<ObIVFVectorVID> vids;
  share::ObVectorMatrix values, centers;
  int64_t k = 0;
  ASSERT_EQ(OB_SUCCESS, algorithm.hca_collect_training_rows(1024, 256, ids));
  for (int64_t i = 0; i < ids.count(); ++i) { ASSERT_EQ(OB_SUCCESS, vids.push_back(ids.at(i) + 1)); }
  ASSERT_EQ(OB_SUCCESS, store.batch_get_by_vid(vids, 0, vids.count(), values));
  values.pop_back();
  EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.hca_train_loaded_samples(1024, 256, 2, ids, values, k, centers));
  ASSERT_EQ(OB_SUCCESS, store.batch_get_by_vid(vids, 0, vids.count(), values));
  const int64_t first = ids.at(0);
  ids.at(0) = ids.at(1);
  EXPECT_EQ(OB_INVALID_DATA, algorithm.hca_train_loaded_samples(1024, 256, 2, ids, values, k, centers));
  ids.at(0) = first;
  ids.pop_back(); values.pop_back(); values.pop_back();
  EXPECT_EQ(OB_INVALID_DATA, algorithm.hca_train_loaded_samples(1024, 256, 2, ids, values, k, centers));
  EXPECT_EQ(0, centers.count());
}

TEST(TestIVFReuseMerge, root_training_consumes_captured_samples)
{
  class SamplingStore final : public ObIVFVectorStore, public ObIVFFirstLevelSampleSet
  {
  public:
    explicit SamplingStore(int64_t n) : n_(n), sample_ret_(OB_SUCCESS) {}
    bool is_valid() const override { return n_ > 0; }
    int64_t get_vector_count() const override { return n_; }
    int64_t get_sample_count() const override { return MIN(n_, int64_t(256)); }
    int64_t get_dimension() const override { return 2; }
    int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override {
      int ret = OB_SUCCESS; vids.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < n_; ++i) { ret = vids.push_back(17 + 3 * i); }
      return ret;
    }
    int batch_get_by_vid(const ObIVFVectorIDView &vids, int64_t begin, int64_t end,
        share::ObVectorMatrix &values) const override {
      int ret = OB_SUCCESS; values.reuse();
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        const int64_t ordinal = (vids.at(i) - 17) / 3;
        if (vids.at(i) < 17 || (vids.at(i) - 17) % 3 != 0 || ordinal >= n_) { ret = OB_INVALID_ARGUMENT; }
        else if (OB_FAIL(reads_.push_back(vids.at(i)))) {}
        else if (OB_FAIL(values.push_back(static_cast<float>(ordinal % 32)))) {}
        else { ret = values.push_back(static_cast<float>(ordinal / 32)); }
      }
      return ret;
    }
    int batch_get_samples(int64_t begin, int64_t end, ObIArray<ObIVFVectorVID> &vids,
        share::ObVectorMatrix &values) const override {
      int ret = sample_ret_; vids.reuse(); values.reuse();
      if (begin < 0 || end <= begin || end > get_sample_count()) { return OB_INVALID_ARGUMENT; }
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        const int64_t ordinal = i * n_ / get_sample_count();
        const ObIVFVectorVID vid = 17 + 3 * ordinal;
        if (OB_FAIL(vids.push_back(vid))) {}
        else if (OB_FAIL(sample_reads_.push_back(vid))) {}
        else if (OB_FAIL(values.push_back(static_cast<float>(ordinal % 32)))) {}
        else { ret = values.push_back(static_cast<float>(ordinal / 32)); }
      }
      return ret;
    }
    int64_t n_;
    int sample_ret_;
    mutable ObArray<ObIVFVectorVID> reads_;
    mutable ObArray<ObIVFVectorVID> sample_reads_;
  };
  for (const int64_t n : {128, 4096}) {
    SCOPED_TRACE(n);
    SamplingStore store(n);
    ObIVFVectorReader reader;
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    share::ObVectorArray<ObIVFVectorVID> vids;
    ObArray<ObIVFVectorVID> first_reads;
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    share::ObVectorMatrix centers, first_centers, second_centers;
    for (int64_t c = 0; c < 4; ++c) {
      ASSERT_EQ(OB_SUCCESS, centers.push_back(static_cast<float>(c * 8)));
      ASSERT_EQ(OB_SUCCESS, centers.push_back(static_cast<float>(c * 16)));
    }
    ObIVFHcaAlgorithmParam param;
    ObIVFHcaAlgorithmAdapter algorithm;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
    ASSERT_EQ(OB_SUCCESS, algorithm.insertion_train_centers(reader, vids, store, centers, 4, first_centers));
    EXPECT_EQ(0, store.reads_.count());
    ASSERT_EQ(store.get_sample_count(), store.sample_reads_.count());
    ASSERT_EQ(8, first_centers.count());
    ASSERT_EQ(n, vids.count());
    for (int64_t i = 0; i < store.sample_reads_.count(); ++i) {
      EXPECT_EQ(17 + 3 * (i * n / store.get_sample_count()), store.sample_reads_.at(i));
    }
    ASSERT_EQ(OB_SUCCESS, first_reads.assign(store.sample_reads_));
    store.sample_reads_.reuse();
    ASSERT_EQ(OB_SUCCESS, algorithm.insertion_train_centers(reader, vids, store, centers, 4, second_centers));
    ASSERT_EQ(first_reads.count(), store.sample_reads_.count());
    for (int64_t i = 0; i < first_reads.count(); ++i) { EXPECT_EQ(first_reads.at(i), store.sample_reads_.at(i)); }
    for (int64_t i = 0; i < first_centers.count(); ++i) { EXPECT_FLOAT_EQ(first_centers.at(i), second_centers.at(i)); }
    for (int64_t c = 0; c < 4; ++c) {
      EXPECT_FLOAT_EQ(static_cast<float>(c * 8), centers.at(2 * c));
      EXPECT_FLOAT_EQ(static_cast<float>(c * 16), centers.at(2 * c + 1));
    }

    int64_t k = 0;
    store.sample_reads_.reuse();
    ASSERT_EQ(OB_SUCCESS, algorithm.hca_train_local_centers(0, 0, reader, vids, &store, 128, k, first_centers));
    EXPECT_EQ(n == 128 ? 1 : 32, k);
    if (n == 128) {
      EXPECT_EQ(0, store.sample_reads_.count());
      EXPECT_EQ(n, store.reads_.count());
      ASSERT_EQ(2, first_centers.count());
      EXPECT_FLOAT_EQ(15.5F, first_centers.at(0));
      EXPECT_FLOAT_EQ(1.5F, first_centers.at(1));
    } else {
      EXPECT_EQ(0, store.reads_.count());
      EXPECT_EQ(store.get_sample_count(), store.sample_reads_.count());
    }
    store.sample_ret_ = OB_IO_ERROR;
    EXPECT_EQ(OB_IO_ERROR, algorithm.insertion_train_centers(reader, vids, store, centers, 4, second_centers));
    if (n > 128) {
      EXPECT_EQ(OB_IO_ERROR, algorithm.hca_train_local_centers(0, 0, reader, vids, &store, 128, k, second_centers));
      ASSERT_EQ(OB_SUCCESS, algorithm.hca_train_local_centers(1, 0, reader, vids, &store, 128, k, second_centers));
      EXPECT_EQ(32, k);
      EXPECT_GT(store.reads_.count(), 0);
    }
  }
}

// A single-CID update must work for a sparse/high global CID, preserve the
// caller's center matrix, and use exactly that CID's supplied members.
TEST(TestIVFReuseMerge, refine_update_preserves_global_cid_and_exact_mean)
{
  ReuseVectorStore store;
  ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ObArray<ObIVFVectorVID> members;
  // The four vectors are (0,0), (100,0), (0,100), (100,100).
  for (const ObIVFVectorVID vid : {1, 17, 129, 145}) { ASSERT_EQ(OB_SUCCESS, members.push_back(vid)); }
  for (const bool normalize : {false, true}) {
    SCOPED_TRACE(normalize);
    ObVectorNormalizeInfo norm;
    ObIVFHcaAlgorithmParam param;
    param.center_norm_info_ = normalize ? &norm : nullptr;
    ObIVFHcaAlgorithmAdapter algorithm;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
    for (const int64_t k : {1, 4096}) {
      SCOPED_TRACE(k);
      share::ObVectorMatrix centers, updated;
      for (int64_t i = 0; i < k; ++i) {
        ASSERT_EQ(OB_SUCCESS, centers.push_back(7.0F));
        ASSERT_EQ(OB_SUCCESS, centers.push_back(-3.0F));
      }
      ASSERT_EQ(OB_SUCCESS, algorithm.refine_update_center(reader, members, centers, k, k - 1, updated));
      ASSERT_EQ(2, updated.count());
      const float expected = normalize ? static_cast<float>(1.0 / std::sqrt(2.0)) : 50.0F;
      EXPECT_NEAR(expected, updated.at(0), 1e-6);
      EXPECT_NEAR(expected, updated.at(1), 1e-6);
      for (int64_t i = 0; i < k; ++i) {
        EXPECT_FLOAT_EQ(7.0F, centers.at(2 * i));
        EXPECT_FLOAT_EQ(-3.0F, centers.at(2 * i + 1));
      }
      EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.refine_update_center(reader, members, centers, k, k, updated));
      centers.pop_back();
      EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.refine_update_center(reader, members, centers, k, k - 1, updated));
    }
  }
}

TEST(TestIVFReuseMerge, quality_checks_detect_empty_and_oversized_clusters)
{
  ObIVFHcaAlgorithmAdapter algorithm;
  ObSEArray<int64_t, 16> counts;
  for (int64_t i = 0; i < 16; ++i) { ASSERT_EQ(OB_SUCCESS, counts.push_back(64)); }
  bool degenerate = true;
  ASSERT_EQ(OB_SUCCESS, algorithm.check_cluster_quality(counts, degenerate));
  EXPECT_FALSE(degenerate);
  counts.at(0) = 640;
  ASSERT_EQ(OB_SUCCESS, algorithm.check_cluster_quality(counts, degenerate));
  EXPECT_TRUE(degenerate); // sample z-score = 3.75, no empty clusters
  counts.at(0) = 0;
  counts.at(1) = 0;
  ASSERT_EQ(OB_SUCCESS, algorithm.check_cluster_quality(counts, degenerate));
  EXPECT_TRUE(degenerate); // 12.5% empty before compaction
  counts.at(0) = -1;
  EXPECT_EQ(OB_INVALID_DATA, algorithm.check_cluster_quality(counts, degenerate));
}



// Production MemoryDump label scanning is disabled under ASAN.
#ifndef OB_USE_ASAN
TEST(TestIVFMergeMonitor, merge_labels_reach_memory_view_statistics)
{
  // Use the same snapshot producer and label iterator as GV$OB_MEMORY.
  // Refresh synchronously only in this test: production refresh is periodic,
  // so this demonstrates label visibility, not real-time stage attribution.
  auto &dump = ObMemoryDump::get_instance();
  ASSERT_EQ(OB_SUCCESS, dump.init());
  dump.stop();
  dump.wait();
  struct StopDump { ~StopDump() { ObMemoryDump::get_instance().destroy(); } } stop_dump;
  std::set<std::string> observed_labels;
  auto sample = [&](const char *step) {
    ObMemoryDumpTask *task = dump.alloc_task();
    ASSERT_NE(nullptr, task);
    task->type_ = STAT_LABEL;
    task->memory_check_ctx_ = nullptr;
    task->min_print_size_ = INT64_MAX;
    dump.handle(task);
    for (int ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ++ctx_id) {
      auto ta = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(MTL_ID(), ctx_id);
      if (nullptr != ta) {
        ASSERT_EQ(OB_SUCCESS, ta->iter_label([&](lib::ObLabel &label, LabelItem *item) {
          if (0 == STRNCMP(label.str_, "IVF", 3) && item->used_ > 0) {
            observed_labels.insert(label.str_);
            printf("[MEMORY_VIEW_SAMPLE] step=%s ctx=%d label=%s used=%ld hold=%ld\n",
                step, ctx_id, label.str_, item->used_, item->hold_);
          }
          return OB_SUCCESS;
        }));
      }
    }
  };
  {
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 128, false, true, false, VIDA_L2));
    RecordingReuseAlgorithm algorithm;
    ObIVFHcaAlgorithmParam algorithm_param;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(algorithm_param));
    ReuseCountingOutput output;
    ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(1), ObTabletID(1001), -1);
    ObAsyncIndexMergeCtx ctx(param, allocator);
    ObLocalArena input_allocator(ctx.mem_ctx_, "IVFMergeInput");
    ObIVFMergeInputAdapter captured_input(input_allocator, &ctx.mem_ctx_);
    ASSERT_EQ(OB_SUCCESS, captured_input.locators_.prepare_allocate(4096));
    // Locator payload now has a reclaiming allocator. The input arena still
    // owns slice offsets, so verify both actual allocation paths are visible.
    ASSERT_EQ(OB_SUCCESS, captured_input.replay_row_offsets_.push_back(0));
    ASSERT_EQ(OB_SUCCESS, captured_input.replay_row_offsets_.push_back(4096));
    ObAsyncIndexMajorMergeDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
    ObFakeTask *prepare = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
    ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.start_with_runtime(
        dag, ctx, input, algorithm, output, *prepare));
    ASSERT_NO_FATAL_FAILURE(run_materialized_merge(dag.ivf_merge_dag_ctx_, *prepare, nullptr, nullptr,
        [&](const ObIVFMergeDagCtx &runtime) {
          const auto *planner = static_cast<const ObIVFMergePlanner *>(runtime.planner_);
          sample(ivf_merge_step_name(planner->get_step()));
        }));
  }
  EXPECT_EQ(1, observed_labels.count("IVFMergeRun"));
  EXPECT_EQ(1, observed_labels.count("IVFHcaCenter"));
  EXPECT_EQ(1, observed_labels.count("IVFHcaAssign"));
  EXPECT_EQ(1, observed_labels.count("IVFMergeInput"));
  EXPECT_EQ(1, observed_labels.count("IVFMergeLoc"));
}
#endif

namespace {
void make_replay_merge_input(ObIVFMergeInputAdapter &replay, ReuseVectorStore &source)
{
  replay.vector_dim_ = source.get_dimension();
  replay.dist_type_ = VIDA_L2;
  ASSERT_EQ(OB_SUCCESS, replay.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  share::ObVectorArray<ObIVFVectorVID> vids;
  share::ObVectorMatrix values;
  ASSERT_EQ(OB_SUCCESS, source.get_all_vids(vids));
  ASSERT_EQ(OB_SUCCESS, source.batch_get_by_vid(vids, 0, vids.count(), values));
  for (int64_t slice = 0; slice < 2; ++slice) {
    auto *store = OB_NEWx(ObIVFReplayInput, &replay.allocator_, replay.allocator_);
    ASSERT_NE(nullptr, store);
    ASSERT_EQ(OB_SUCCESS, replay.replay_inputs_.push_back(store));
    ASSERT_EQ(OB_SUCCESS, store->init(MTL_ID(), replay.vector_dim_, 1L << 20));
    for (int64_t i = slice; i < vids.count(); i += 2) {
      ASSERT_EQ(OB_SUCCESS, store->add_row(vids.at(i), ObString(replay.vector_dim_ * sizeof(float),
          reinterpret_cast<char *>(&values.at(i * replay.vector_dim_)))));
    }
    ASSERT_EQ(OB_SUCCESS, store->seal());
  }
  ASSERT_EQ(OB_SUCCESS, replay.replay_row_offsets_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, replay.replay_row_offsets_.push_back(replay.replay_inputs_.at(0)->get_row_count()));
  ASSERT_EQ(OB_SUCCESS, replay.replay_row_offsets_.push_back(vids.count()));
  for (int64_t i = 0; i < vids.count(); ++i) {
    ObAsyncIVFSourceLocator locator(vids.at(i), i % 2, i % 2, i / 2);
    ASSERT_EQ(OB_SUCCESS, replay.locators_.push_back(locator));
    ASSERT_EQ(OB_SUCCESS, replay.sample_locators_.push_back(locator));
  }
  replay.is_inited_ = true;
  ASSERT_TRUE(replay.is_valid());
}
}

TEST(TestIVFReuseMerge, concat_deduplicates_centers_and_relations_without_vector_computation)
{
  for (const bool all_equal : {false, true}) {
  for (const bool grouped : {false, true}) {
    if (all_equal && grouped) { continue; }
    SCOPED_TRACE(all_equal);
    SCOPED_TRACE(grouped);
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 2, all_equal));
    ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, vectors));
    // Equal centers in one source collapse a primary/secondary pair.
    for (int64_t d = 0; d < 2; ++d) { prior.centroids_.at(2 + d) = prior.centroids_.at(d); }
    for (int64_t i = 0; i < vectors.members_; ++i) {
      ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(i + 1, 1)));
      ++prior.posting_mass_.at(1);
    }
    // Equal centers across sources have disjoint members which must be retained.
    for (int64_t d = 0; d < 2; ++d) { prior.centroids_.at(32 + d) = prior.centroids_.at(d); }
    // Retain an empty source leaf, and keep merely close coordinates distinct.
    if (!all_equal) { prior.centroids_.at(4) = std::nextafter(prior.centroids_.at(0), 1.0F); }
    for (int64_t i = 0; i < prior.postings_.count(); ++i) {
      auto &p = prior.postings_.at(i);
      if (p.centroid_idx_ == 31) { p.centroid_idx_ = 30; ++prior.posting_mass_.at(30); --prior.posting_mass_.at(31); }
    }
    ConcatTestSource source(prior); prior.posting_source_ = &source;
    std::set<std::string> centers;
    std::set<std::pair<std::string, uint64_t>> expected;
    for (int64_t c = 0; c < prior.posting_mass_.count(); ++c) {
      centers.insert(std::string(reinterpret_cast<const char *>(&prior.centroids_.at(c * 2)), 2 * sizeof(float)));
    }
    for (int64_t i = 0; i < prior.postings_.count(); ++i) {
      const auto &p = prior.postings_.at(i);
      expected.emplace(std::string(reinterpret_cast<const char *>(&prior.centroids_.at(p.centroid_idx_ * 2)),
          2 * sizeof(float)), p.vid_);
    }
    ObIVFMergeInput input;
    RecordingReuseAlgorithm algorithm;
    ConcatCountingOutput output;
    ObIVFMergeProcessor processor;
    ObIVFConcatPlanner planner;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 1, false, true, true, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output,
        ObIVFProcessType::CONCATENATION, &prior));
    if (grouped) { ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::set_parent_build_threshold(processor, 1)); }
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    int64_t copies = 0, quant = 0, raw = 0, parent = 0;
    while (plan != nullptr) {
      const auto stage = planner.get_stage();
      if (stage == ObIVFMergeStage::CENTROID_INDEX) { ++parent; }
      else {
        EXPECT_TRUE(stage == ObIVFMergeStage::PRIOR || stage == ObIVFMergeStage::QUANT
            || stage == ObIVFMergeStage::RAW_WRITE);
        copies += stage == ObIVFMergeStage::PRIOR;
        quant += stage == ObIVFMergeStage::QUANT;
        raw += stage == ObIVFMergeStage::RAW_WRITE;
      }
      for (int64_t t = plan->get_task_count(); t-- > 0;) { ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t)); }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next)); plan = next;
    }
    EXPECT_EQ(1, copies); EXPECT_EQ(1, quant); EXPECT_EQ(1, raw);
    EXPECT_EQ(grouped, parent > 0); EXPECT_EQ(grouped, output.hierarchical_);
    EXPECT_EQ(0, vectors.read_calls_); EXPECT_EQ(0, algorithm.trained_vectors_);
    EXPECT_EQ(0, algorithm.refine_calls_); EXPECT_EQ(0, output.encoded_);
    EXPECT_EQ(vectors.count_, output.raw_count_);
    EXPECT_EQ(centers.size(), output.clusters_); EXPECT_EQ(expected.size(), output.copied_);
    EXPECT_EQ(prior.postings_.count(), source.scanned_);
    std::set<std::pair<std::string, uint64_t>> actual;
    for (const auto &row : output.rows_) {
      actual.emplace(std::string(reinterpret_cast<const char *>(&output.centers_.at(row.first.first * 2)),
          2 * sizeof(float)), row.first.second);
    }
    EXPECT_EQ(expected, actual);
    const ObIVFDagArtifact *artifact = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.get_artifact(artifact));
    const auto &merge = *static_cast<const ObIVFMergeArtifact *>(artifact);
    EXPECT_EQ(ObIVFProcessType::CONCATENATION, merge.effective_type_);
    EXPECT_EQ(expected.size() - vectors.count_, merge.secondary_assignment_count_);
  }
  }
}

TEST(TestIVFReuseMerge, concat_preserves_contiguous_rows_across_segment_boundaries)
{
  const int64_t dim = 768;
  const int64_t count = share::ObVectorArray<float>::BLOCK_CAPACITY / dim + 2;
  class WideStore final : public ReuseVectorStore
  {
  public:
    explicit WideStore(int64_t count) : ReuseVectorStore(count, 1) {}
    int64_t get_dimension() const override { return 768; }
  } vectors(count);
  ObArenaAllocator allocator;
  ObIVFMergePrior prior(allocator);
  prior.dimension_ = dim;
  prior.vector_count_ = prior.covered_vector_count_ = count;
  prior.dominant_segment_ = 0;
  ObIVFSegmentPriorInfo segment;
  segment.provides_prior_ = true;
  segment.live_vector_count_ = segment.centroid_count_ = count;
  ASSERT_EQ(OB_SUCCESS, prior.segments_.push_back(segment));
  ASSERT_EQ(OB_SUCCESS, prior.centroids_.set_dimension(dim));
  for (int64_t c = 0; c < count; ++c) {
    ASSERT_EQ(OB_SUCCESS, prior.old_cids_.push_back(c));
    ASSERT_EQ(OB_SUCCESS, prior.posting_mass_.push_back(1));
    ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(c + 1, c)));
    for (int64_t d = 0; d < dim; ++d) {
      ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(static_cast<float>(c * dim + d)));
    }
  }
  prior.postings_complete_ = true;
  ConcatTestSource source(prior);
  prior.posting_source_ = &source;
  ObIVFMergeInput input;
  RecordingReuseAlgorithm algorithm;
  ConcatCountingOutput output;
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 1, false, true, true, VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
  ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output,
      ObIVFProcessType::CONCATENATION, &prior));
  ASSERT_EQ(OB_SUCCESS, processor.init_concat_memberships());
  const auto &runtime = *processor.runtime_;
  ASSERT_EQ(count, runtime.final_cluster_count_);
  for (int64_t c = 0; c < count; ++c) {
    const float *row = runtime.c1_->data_at(runtime.old_to_new_->at(c) * dim, dim);
    ASSERT_NE(nullptr, row);
    EXPECT_EQ(0, MEMCMP(prior.centroids_.data_at(c * dim, dim), row, dim * sizeof(float)));
  }
  EXPECT_EQ(0, vectors.read_calls_);
}

TEST(TestIVFReuseMerge, concat_rejects_incomplete_membership_before_copying)
{
  for (int mode = 0; mode < 5; ++mode) {
    SCOPED_TRACE(mode);
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 2));
    ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, vectors));
    ConcatTestSource source(prior); prior.posting_source_ = &source;
    if (mode == 0) { prior.postings_complete_ = false; }
    if (mode == 1) { prior.posting_source_ = nullptr; }
    if (mode == 2) { prior.postings_.pop_back(); --prior.posting_mass_.at(31); }
    if (mode == 3) {
      ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(prior.postings_.at(0))); ++prior.posting_mass_.at(0);
    }
    if (mode == 4) { ++prior.posting_mass_.at(0); }
    if (mode < 2) {
      ObIVFProcessType selected;
      ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 128, selected));
      EXPECT_EQ(ObIVFProcessType::FULL_REBUILD, selected);
    }
    ObIVFMergeInput input; RecordingReuseAlgorithm algorithm; ConcatCountingOutput output;
    ObIVFMergeProcessor processor; ObIVFConcatPlanner planner;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 128, false, true, true, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    const int rc = processor.init(allocator, input, algorithm, output, ObIVFProcessType::CONCATENATION, &prior);
    if (mode < 2) { EXPECT_EQ(OB_NOT_SUPPORTED, rc); }
    else {
      ASSERT_EQ(OB_SUCCESS, rc);
      ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor));
      const ObIVFTaskPlan *plan = nullptr;
      EXPECT_EQ(OB_INVALID_DATA, planner.start(plan));
    }
    EXPECT_EQ(0, source.scanned_); EXPECT_EQ(0, output.copied_); EXPECT_EQ(0, output.raw_count_);
  }
}

TEST(TestIVFReuseMerge, concat_copy_failure_and_cancel_never_publish_or_restart_training)
{
  for (int mode = 0; mode < 5; ++mode) {
    SCOPED_TRACE(mode);
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 2));
    ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, vectors));
    ConcatTestSource source(prior); prior.posting_source_ = &source;
    ObIVFMergeInput input; RecordingReuseAlgorithm algorithm; ConcatCountingOutput output;
    ObIVFMergeProcessor processor; ObIVFConcatPlanner planner;
    if (mode == 0) { source.error_ = OB_IO_ERROR; }
    if (mode == 1) { source.omit_last_ = true; }
    if (mode == 2) { output.copy_error_ = OB_IO_ERROR; }
    if (mode == 3) { output.cancel_during_copy_ = &processor; }
    if (mode == 4) { algorithm.parent_error_ = OB_IO_ERROR; }
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 128, false, true, true, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, ObIVFProcessType::CONCATENATION, &prior));
    if (mode == 4) { ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::set_parent_build_threshold(processor, 1)); }
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    int ret = OB_SUCCESS;
    for (int64_t t = 0; ret == OB_SUCCESS && t < plan->get_task_count(); ++t) { ret = planner.process_task(*plan, t); }
    const ObIVFTaskPlan *next = nullptr;
    if (ret == OB_SUCCESS) { ret = planner.finish_task_plan(*plan, next); }
    EXPECT_EQ(mode == 1 ? OB_INVALID_DATA : mode == 3 ? OB_CANCELED : OB_IO_ERROR, ret);
    EXPECT_TRUE(ObIVFMergeTestPeer::is_canceled(processor)); EXPECT_EQ(nullptr, next);
    const ObIVFDagArtifact *artifact = nullptr;
    EXPECT_NE(OB_SUCCESS, planner.get_artifact(artifact)); EXPECT_EQ(nullptr, artifact);
    EXPECT_EQ(0, output.raw_count_); EXPECT_EQ(0, algorithm.trained_vectors_); EXPECT_EQ(0, algorithm.refine_calls_);
  }
}

TEST(TestIVFReuseMerge, full_and_concat_share_major_and_inc_dag_protocol)
{
  for (const ObMergeType merge_type : {MAJOR_MERGE, INC_MAJOR_MERGE}) {
  for (const ObIVFProcessType type : {ObIVFProcessType::FULL_REBUILD,
      ObIVFProcessType::CONCATENATION}) {
    SCOPED_TRACE(static_cast<int>(type));
    ScopedMergeMonitor monitor;
    ASSERT_EQ(OB_SUCCESS, monitor.init());
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, type == ObIVFProcessType::INSERTION ? 1 : 2));
    ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, vectors));
    ConcatTestSource source(prior); prior.posting_source_ = &source;
    ObIVFMergeInputAdapter replay(allocator);
    ASSERT_NO_FATAL_FAILURE(make_replay_merge_input(replay, vectors));
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(replay, replay, 128, false /* normalized */, true /* rotated */, false, VIDA_L2));
    RecordingReuseAlgorithm algorithm;
    ObIVFHcaAlgorithmParam algorithm_param;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(algorithm_param));
    ConcatCountingOutput output;
    ObTabletMergeDagParam param(merge_type, ObLSID(1), ObTabletID(1001), -1);
    param.merge_version_ = 100;
    ObAsyncIndexMergeCtx ctx(param, allocator);
    ObAsyncIndexIncMergeDag inc_dag;
    ObAsyncIndexMajorMergeDag major_dag;
    ObAsyncIndexMergeDag &dag = merge_type == INC_MAJOR_MERGE
        ? static_cast<ObAsyncIndexMergeDag &>(inc_dag) : static_cast<ObAsyncIndexMergeDag &>(major_dag);
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
    ASSERT_NO_FATAL_FAILURE(set_merge_monitor_dag_id(dag));
    ObFakeTask *prepare = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
    ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.start_with_runtime(
        dag, ctx, input, algorithm, output, *prepare, type, type == ObIVFProcessType::FULL_REBUILD ? nullptr : &prior));
    ObSEArray<ObIVFMergeStep, 32> steps;
    ObSEArray<int64_t, 32> depths;
    ASSERT_NO_FATAL_FAILURE(run_materialized_merge(dag.ivf_merge_dag_ctx_, *prepare, &steps, &depths));
    std::vector<MergeMonitorRow> monitor_rows;
    ASSERT_NO_FATAL_FAILURE(read_merge_monitor(monitor.mgr_, monitor_rows));
    int64_t observed_steps = 0;
    bool observed_build = false;
    for (const auto &row : monitor_rows) {
      rapidjson::Document json;
      json.Parse(row.message_.c_str());
      ASSERT_FALSE(json.HasParseError());
      EXPECT_STREQ(dag.ivf_merge_dag_ctx_.strategy_name(), json["strategy"].GetString());
      EXPECT_STREQ("FINISHED", json["state"].GetString());
      if (row.kind_ == "IVF_MERGE_STEP") { ++observed_steps; }
      if (row.kind_ == "IVF_MERGE") {
        observed_build = true;
        EXPECT_EQ(512, json["input_vectors"].GetInt64());
      }
    }
    EXPECT_TRUE(observed_build);
    EXPECT_EQ(steps.count(), observed_steps);
    ASSERT_FALSE(steps.empty());
    EXPECT_EQ(type == ObIVFProcessType::FULL_REBUILD ? ObIVFMergeStep::HIERARCHY_CLUSTER
        : ObIVFMergeStep::PRIOR_WORK, steps.at(0));
    int64_t warm_steps = 0;
    for (int64_t i = 0; i < steps.count(); ++i) {
      if (type != ObIVFProcessType::FULL_REBUILD) {
        EXPECT_NE(ObIVFMergeStep::REFINE_TRAIN, steps.at(i));
      }
      if (type != ObIVFProcessType::FULL_REBUILD) {
        if (depths.at(i) == 0) {
          EXPECT_NE(ObIVFMergeStep::HIERARCHY_CLUSTER, steps.at(i));
        }
      } else {
        EXPECT_NE(ObIVFMergeStep::PRIOR_WORK, steps.at(i));
      }
      warm_steps += steps.at(i) == ObIVFMergeStep::HIERARCHY_WARM_START;
    }
    EXPECT_EQ(0, warm_steps);
    ASSERT_GE(steps.count(), 3);
    EXPECT_EQ(type == ObIVFProcessType::CONCATENATION ? ObIVFMergeStep::PRIOR_WORK
        : ObIVFMergeStep::SOAR_ASSIGN, steps.at(steps.count() - 3));
    EXPECT_EQ(ObIVFMergeStep::QUANT, steps.at(steps.count() - 2));
    EXPECT_EQ(ObIVFMergeStep::RAW_WRITE, steps.at(steps.count() - 1));
    const ObIVFDagArtifact *base_artifact = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.planner_->get_artifact(base_artifact));
    const auto &artifact = *static_cast<const ObIVFMergeArtifact *>(base_artifact);
    EXPECT_EQ(type, artifact.selected_type_);
    EXPECT_EQ(type, artifact.effective_type_);
    EXPECT_EQ(ObIVFQualityFallback::NONE, artifact.fallback_reason_);
    EXPECT_EQ(512, artifact.vector_count_);
    EXPECT_EQ(512, output.primary_count_);
    EXPECT_EQ(512, output.raw_count_);
    EXPECT_EQ(type == ObIVFProcessType::FULL_REBUILD ? 1 : 0, algorithm.refine_calls_);
    EXPECT_EQ(type == ObIVFProcessType::INSERTION ? 1 : 0, algorithm.insertion_finish_calls_);
    EXPECT_EQ(0, algorithm.warm_calls_);
    EXPECT_EQ(merge_type == INC_MAJOR_MERGE ? ObDagPrio::DAG_PRIO_ASYNC_INDEX_MERGE_MID
        : ObDagPrio::DAG_PRIO_ASYNC_INDEX_MERGE_LOW, dag.get_priority());
  }
}
}



TEST(TestIVFReuseMerge, quant_ranges_close_on_success_error_and_cancel)
{
  for (const auto type : {ObIVFProcessType::FULL_REBUILD}) {
    for (int mode = 0; mode < 4; ++mode) {
      SCOPED_TRACE(static_cast<int>(type));
      SCOPED_TRACE(mode);
      ObArenaAllocator allocator;
      ReuseVectorStore vectors(64, 16);
      ObIVFMergeInput input;
      ObIVFMergePrior prior(allocator);
      if (type != ObIVFProcessType::FULL_REBUILD) {
        ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 1));
        ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, vectors));
      }
      RecordingReuseAlgorithm algorithm;
      ObIVFHcaAlgorithmParam param;
      RangeCountingOutput output;
      output.mode_ = mode;
      ObIVFMergeProcessor processor;
      output.processor_ = &processor;
      ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 8, false, true, false, VIDA_L2));
      ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
      ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type,
          type == ObIVFProcessType::FULL_REBUILD ? nullptr : &prior));
      ObIVFFullRebuildPlanner full;
      ObIVFConcatPlanner concat;
      ObIVFInsertionPlanner insertion;
      ObIVFMergePlanner *planner = type == ObIVFProcessType::FULL_REBUILD
          ? static_cast<ObIVFMergePlanner *>(&full) : type == ObIVFProcessType::CONCATENATION
          ? static_cast<ObIVFMergePlanner *>(&concat) : static_cast<ObIVFMergePlanner *>(&insertion);
      ASSERT_EQ(OB_SUCCESS, type == ObIVFProcessType::FULL_REBUILD ? full.init(allocator, processor)
          : type == ObIVFProcessType::CONCATENATION ? concat.init(allocator, processor)
          : insertion.init(allocator, processor));
      const ObIVFTaskPlan *plan = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner->start(plan));
      int ret = OB_SUCCESS;
      int steps = 0;
      while (OB_SUCC(ret) && plan != nullptr && ++steps < 100) {
        for (int64_t i = 0; OB_SUCC(ret) && i < plan->get_task_count(); ++i) {
          ret = planner->process_task(*plan, i);
        }
        if (OB_SUCC(ret)) {
          const ObIVFTaskPlan *next = nullptr;
          ret = planner->finish_task_plan(*plan, next); plan = next;
        }
      }
      EXPECT_EQ(mode == 0 ? OB_SUCCESS : mode == 3 ? OB_CANCELED : OB_IO_ERROR, ret);
      EXPECT_EQ(output.opened_, output.closed_);
      EXPECT_FALSE(output.active_);
      if (mode == 0) {
        EXPECT_EQ(nullptr, plan);
        EXPECT_EQ(output.expected_ranges_, output.closed_);
        EXPECT_EQ(vectors.count_, output.raw_count_);
      } else {
        EXPECT_EQ(1, output.closed_);
        EXPECT_EQ(0, output.raw_count_);
        EXPECT_EQ(mode == 2, output.closed_success_);
        EXPECT_TRUE(ObIVFMergeTestPeer::is_canceled(processor));
      }
    }
  }
}



TEST(TestIVFReuseMerge, insertion_rejects_incomplete_and_duplicate_membership)
{
  for (int error = 0; error < 5; ++error) {
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 1));
    for (int64_t i = 0; i < vectors.count_; ++i) {
      ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(i + 1, i / 16)));
    }
    prior.postings_complete_ = error != 0;
    if (error == 1) { prior.postings_.pop_back(); }
    if (error == 2) { prior.postings_.at(1).vid_ = 1; }
    if (error == 3) { prior.postings_.at(0).vid_ = 9999; }
    if (error == 4) {
      prior.dimension_ = 1;
      while (prior.centroids_.count() > 32) { prior.centroids_.pop_back(); }
      ASSERT_EQ(OB_SUCCESS, prior.validate());
    }
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 32, false, true, false, VIDA_L2));
    RecordingReuseAlgorithm algorithm;
    ObIVFHcaAlgorithmParam param;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
    ReuseCountingOutput output;
    ObIVFMergeProcessor processor;
    EXPECT_NE(OB_SUCCESS, processor.init(allocator, input, algorithm, output, ObIVFProcessType::INSERTION, &prior));
  }
}

TEST(TestIVFReuseMerge, as_refinement_skips_update_when_membership_is_unchanged)
{
  for (const ObIVFProcessType type : {ObIVFProcessType::FULL_REBUILD}) {
    SCOPED_TRACE(static_cast<int>(type));
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 128, false /* normalized */, true /* rotated */, false, VIDA_L2));
    RecordingReuseAlgorithm algorithm;
    ObIVFHcaAlgorithmParam algorithm_param;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(algorithm_param));
    algorithm.keep_refine_assignments_ = true;
    ReuseCountingOutput output;
    ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(1), ObTabletID(1001), -1);
    param.merge_version_ = 100;
    ObAsyncIndexMergeCtx ctx(param, allocator);
    ObAsyncIndexMajorMergeDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
    ObFakeTask *prepare = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
    ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.start_with_runtime(dag, ctx, input, algorithm, output,
        *prepare, type, nullptr));
    ObSEArray<ObIVFMergeStep, 32> steps;
    ASSERT_NO_FATAL_FAILURE(run_materialized_merge(dag.ivf_merge_dag_ctx_, *prepare, &steps));
    EXPECT_EQ(1, algorithm.refine_calls_);
    for (int64_t i = 0; i < steps.count(); ++i) { EXPECT_NE(ObIVFMergeStep::REFINE_UPDATE, steps.at(i)); }
    EXPECT_EQ(vectors.count_, output.raw_count_);
    EXPECT_EQ(vectors.count_, output.primary_count_);
    const ObIVFDagArtifact *artifact = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.planner_->get_artifact(artifact));
    ASSERT_NE(nullptr, artifact);
    EXPECT_EQ(type, static_cast<const ObIVFMergeArtifact *>(artifact)->effective_type_);
  }
}



TEST(TestIVFParentBuild, borrowed_centers_reuse_parallel_clustering_and_stop_after_soar)
{
  const int64_t count = 8192, dim = 4, cluster_size = 16;
  std::vector<float> values(count * dim);
  uint32_t seed = 42;
  for (float &value : values) {
    seed = seed * 1664525U + 1013904223U;
    value = static_cast<float>(seed % 100003) / 100003.0F;
  }
  const std::vector<float> original = values;
  share::ObVectorMatrix leaves;
  ASSERT_EQ(OB_SUCCESS, leaves.set_dimension(dim));
  ASSERT_EQ(OB_SUCCESS, leaves.resize(values.size(), false));
  leaves.copy_from(0, values.data(), values.size());
  ObIVFHcaAlgorithmParam param;
  ObIVFMergeRuntimeAllocator memory;
  {
    blocksstable::ObIvfCentroidLayout layout(memory);
    ObIVFParentCentroidBuilder builder(memory);
    ASSERT_EQ(OB_SUCCESS, builder.init(leaves, dim, param, cluster_size));
    bool finished = false, saw_refine = false, saw_soar = false;
    int64_t largest_plan = 0, plans = 0;
    for (; !finished && plans < 256; ++plans) {
      int64_t tasks = 0;
      ObIVFMergeStep expected_step = ObIVFMergeStep::INVALID;
      int64_t expected_depth = IVF_MERGE_INVALID_DEPTH;
      ASSERT_EQ(OB_SUCCESS, builder.get_next_operation(expected_step, expected_depth));
      ASSERT_EQ(OB_SUCCESS, builder.prepare(plans, tasks));
      EXPECT_EQ(expected_step, builder.get_step());
      EXPECT_EQ(expected_depth, builder.get_depth());
      ASSERT_GT(tasks, 0);
      largest_plan = MAX(largest_plan, tasks);
      EXPECT_NE(ObIVFMergeStage::CENTROID_INDEX, builder.get_stage());
      EXPECT_NE(ObIVFMergeStage::QUANT, builder.get_stage());
      EXPECT_NE(ObIVFMergeStage::RAW_WRITE, builder.get_stage());
      saw_refine |= builder.get_step() == ObIVFMergeStep::REFINE_TRAIN;
      saw_soar |= builder.get_step() == ObIVFMergeStep::SOAR_ASSIGN;
      std::vector<int> results(tasks, OB_ERR_UNEXPECTED);
      std::atomic<int64_t> next(0);
      std::vector<std::thread> workers;
      share::ObTenantBase *tenant = share::ObTenantEnv::get_tenant();
      for (int64_t t = 0; t < MIN(4L, tasks); ++t) {
        workers.emplace_back([&] {
          share::ObTenantEnv::set_tenant(tenant);
          for (int64_t i = next.fetch_add(1); i < tasks; i = next.fetch_add(1)) {
            results[i] = builder.execute(i);
          }
          share::ObTenantEnv::set_tenant(nullptr);
        });
      }
      for (auto &worker : workers) { worker.join(); }
      for (int result : results) { ASSERT_EQ(OB_SUCCESS, result); }
      ASSERT_EQ(OB_SUCCESS, builder.finish(plans, layout, finished));
      if (!finished) { EXPECT_FALSE(layout.is_ready()); }
    }
    ASSERT_TRUE(finished);
    EXPECT_TRUE(saw_refine);
    EXPECT_TRUE(saw_soar);
    EXPECT_GT(largest_plan, 1);
    EXPECT_GT(plans, 5);
    EXPECT_EQ(count, layout.leaf_count());
    EXPECT_EQ(0, MEMCMP(values.data(), original.data(), values.size() * sizeof(float)));
    for (int64_t i = 0; i < leaves.count(); ++i) {
      EXPECT_FLOAT_EQ(original[i], leaves.at(i));
    }
    // The compatibility entry drives exactly the same stages synchronously.
    ObArenaAllocator oracle_memory;
    blocksstable::ObIvfCentroidLayout oracle(oracle_memory);
    ASSERT_EQ(OB_SUCCESS, oracle.build(leaves, dim, param.train_param_, param.soar_param_, nullptr, 1, cluster_size));
    ASSERT_EQ(oracle.parent_count(), layout.parent_count());
    EXPECT_EQ(oracle.center_row_count(), layout.center_row_count());
    for (int64_t j = 0; j < count; ++j) {
      EXPECT_EQ(oracle.primary_parent(j), layout.primary_parent(j));
      EXPECT_EQ(oracle.secondary_parent(j), layout.secondary_parent(j));
    }
    for (int64_t p = 0; p < layout.parent_count(); ++p) {
      for (int64_t d = 0; d < dim; ++d) {
        EXPECT_FLOAT_EQ(oracle.parent_vector(p)[d], layout.parent_vector(p)[d]);
      }
    }
  }
  EXPECT_EQ(0, memory.used());
}

TEST(TestIVFReuseMerge, parent_barriers_preserve_leaf_output_for_all_build_paths)
{
  class ParentOutput : public ReuseCountingOutput
  {
  public:
    int64_t layout_calls_ = 0, leaf_count_ = 0;
    int set_centroid_layout(const blocksstable::ObIvfCentroidLayout &layout) override
    {
      if (!layout.is_ready() || !layout.is_hierarchical()) { return OB_STATE_NOT_MATCH; }
      ++layout_calls_; leaf_count_ = layout.leaf_count();
      return OB_SUCCESS;
    }
    int quantize_cluster(ObIVFVectorReader &r, const share::ObVectorMatrix &c, int64_t k, int64_t cid,
        const ObIVFVectorIDView &members, const storage::ObIVFVectorIDView *secondary) override
    {
      return layout_calls_ != 1 || leaf_count_ != k ? OB_STATE_NOT_MATCH
          : ReuseCountingOutput::quantize_cluster(r, c, k, cid, members, secondary);
    }
  };
  class CentroidPerfObserver : public ObIVFMergePerfObserver
  {
  public:
    void on_step_perf(const ObIVFMergeStepPerf &perf) override
    {
      if (perf.stage_ == ObIVFMergeStage::CENTROID_INDEX && perf.end_us_ > 0) { completed_.push_back(perf); }
    }
    std::vector<ObIVFMergeStepPerf> completed_;
  };
  for (const ObIVFProcessType type : {ObIVFProcessType::INITIAL_BUILD, ObIVFProcessType::FULL_REBUILD}) {
    SCOPED_TRACE(static_cast<int>(type));
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 1));
    if (type == ObIVFProcessType::INSERTION) { ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, vectors)); }
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 128, false, true, true, VIDA_L2));
    ObIVFHcaAlgorithmAdapter algorithm;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ParentOutput output;
    ObIVFMergeProcessor processor;
    const bool full = is_ivf_full_build_process_type(type);
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type, full ? nullptr : &prior));
    ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::set_parent_build_threshold(processor, 1));
    ObIVFFullBuildPlanner build;
    ObIVFConcatPlanner concat;
    ObIVFInsertionPlanner insertion;
    ObIVFMergePlanner *planner = full ? static_cast<ObIVFMergePlanner *>(&build)
        : type == ObIVFProcessType::CONCATENATION ? static_cast<ObIVFMergePlanner *>(&concat)
        : static_cast<ObIVFMergePlanner *>(&insertion);
    ASSERT_EQ(OB_SUCCESS, full ? build.init(allocator, processor, type)
        : type == ObIVFProcessType::CONCATENATION ? concat.init(allocator, processor)
        : insertion.init(allocator, processor));
    CentroidPerfObserver observer;
    planner->set_perf_observer(&observer);
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner->start(plan));
    int64_t parent_plans = 0;
    bool saw_cluster = false, saw_assign = false, saw_update = false, saw_soar = false;
    std::vector<ObIVFMergeStep> index_steps;
    std::vector<int64_t> index_depths, index_task_counts, index_plan_ids;
    while (plan != nullptr) {
      if (planner->get_stage() == ObIVFMergeStage::CENTROID_INDEX) {
        ++parent_plans;
        const auto step = planner->get_step();
        const int64_t depth = planner->get_depth();
        EXPECT_NE(ObIVFMergeStep::INVALID, step);
        EXPECT_NE(ObIVFMergeStep::QUANT, step);
        EXPECT_NE(ObIVFMergeStep::RAW_WRITE, step);
        saw_cluster |= step == ObIVFMergeStep::HIERARCHY_CLUSTER;
        saw_assign |= step == ObIVFMergeStep::HIERARCHY_ASSIGN;
        saw_update |= step == ObIVFMergeStep::HIERARCHY_UPDATE;
        saw_soar |= step == ObIVFMergeStep::SOAR_ASSIGN;
        if (step == ObIVFMergeStep::HIERARCHY_CLUSTER || step == ObIVFMergeStep::HIERARCHY_ASSIGN
            || step == ObIVFMergeStep::HIERARCHY_UPDATE) {
          EXPECT_GE(depth, 0);
        }
        index_steps.push_back(step);
        index_depths.push_back(depth);
        index_task_counts.push_back(plan->get_task_count());
        index_plan_ids.push_back(plan->get_plan_id());
        // No inner state may advance before all work in this visible plan joins.
        ObIVFMergeStepResult premature;
        EXPECT_EQ(OB_STATE_NOT_MATCH, processor.finish_step(ObIVFMergeStage::CENTROID_INDEX,
            step, depth, plan->get_plan_id(), premature));
        EXPECT_EQ(0, output.primary_count_);
        EXPECT_EQ(0, output.raw_count_);
      }
      for (int64_t i = 0; i < plan->get_task_count(); ++i) {
        ASSERT_EQ(OB_SUCCESS, planner->process_task(*plan, i));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner->finish_task_plan(*plan, next));
      plan = next;
    }
    EXPECT_GT(parent_plans, 3);
    EXPECT_TRUE(saw_cluster && saw_assign && saw_update && saw_soar);
    ASSERT_EQ(parent_plans, observer.completed_.size());
    for (int64_t i = 0; i < parent_plans; ++i) {
      const auto &perf = observer.completed_[i];
      EXPECT_EQ(index_steps[i], perf.step_);
      EXPECT_EQ(index_depths[i], perf.depth_);
      EXPECT_EQ(index_task_counts[i], perf.task_count_);
      EXPECT_EQ(index_plan_ids[i], perf.plan_id_);
      EXPECT_FALSE(perf.quantize_in_soar_);
      EXPECT_EQ(OB_SUCCESS, perf.ret_code_);
      EXPECT_GE(perf.end_us_, perf.prepared_us_);
    }
    EXPECT_EQ(1, output.layout_calls_);
    EXPECT_EQ(vectors.get_vector_count(), output.primary_count_);
    EXPECT_EQ(vectors.get_vector_count(), output.raw_count_);
    const ObIVFDagArtifact *artifact = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner->get_artifact(artifact));
    ASSERT_NE(nullptr, artifact);
    EXPECT_TRUE(artifact->is_valid());
  }
}

TEST(TestIVFReuseMerge, parent_build_failure_cancels_all_build_paths_without_fallback)
{
  for (const ObIVFProcessType type : {ObIVFProcessType::INITIAL_BUILD, ObIVFProcessType::FULL_REBUILD}) {
    SCOPED_TRACE(static_cast<int>(type));
    ObArenaAllocator allocator;
    ReuseVectorStore vectors;
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 1));
    if (type == ObIVFProcessType::INSERTION) { ASSERT_NO_FATAL_FAILURE(make_reuse_postings(prior, vectors)); }
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 128, false, true, false, VIDA_L2));
    RecordingReuseAlgorithm algorithm;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    algorithm.parent_error_ = OB_IO_ERROR;
    ReuseCountingOutput output;
    ObIVFMergeProcessor processor;
    const bool full = type == ObIVFProcessType::INITIAL_BUILD || type == ObIVFProcessType::FULL_REBUILD;
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type, full ? nullptr : &prior));
    ASSERT_EQ(OB_SUCCESS, ObIVFMergeTestPeer::set_parent_build_threshold(processor, 1));
    ObIVFFullBuildPlanner build;
    ObIVFConcatPlanner concat;
    ObIVFInsertionPlanner insertion;
    ObIVFMergePlanner *planner = full ? static_cast<ObIVFMergePlanner *>(&build)
        : type == ObIVFProcessType::CONCATENATION ? static_cast<ObIVFMergePlanner *>(&concat)
        : static_cast<ObIVFMergePlanner *>(&insertion);
    ASSERT_EQ(OB_SUCCESS, full ? build.init(allocator, processor, type)
        : type == ObIVFProcessType::CONCATENATION ? concat.init(allocator, processor)
        : insertion.init(allocator, processor));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner->start(plan));
    int parent_ret = OB_SUCCESS;
    while (plan != nullptr && parent_ret == OB_SUCCESS) {
      for (int64_t i = 0; i < plan->get_task_count(); ++i) {
        ASSERT_EQ(OB_SUCCESS, planner->process_task(*plan, i));
      }
      const ObIVFMergeStage previous = planner->get_stage();
      const ObIVFTaskPlan *next = nullptr;
      parent_ret = planner->finish_task_plan(*plan, next);
      if (parent_ret != OB_SUCCESS) {
        EXPECT_EQ(ObIVFMergeStage::SOAR, previous);
        EXPECT_EQ(nullptr, next);
      }
      plan = next;
    }
    EXPECT_EQ(OB_IO_ERROR, parent_ret);
    EXPECT_TRUE(ObIVFMergeTestPeer::is_canceled(processor));
    EXPECT_EQ(0, output.primary_count_);
    EXPECT_EQ(0, output.raw_count_);
    const ObIVFDagArtifact *artifact = nullptr;
    EXPECT_NE(OB_SUCCESS, planner->get_artifact(artifact));
    EXPECT_EQ(nullptr, artifact);
  }
}



namespace {
namespace
{
class PriorCodeSource final : public ObIVFEncodedPostingSource
{
public:
  explicit PriorCodeSource(const ObIVFMergePrior &prior) : prior_(prior) {}
  mutable int64_t copied_ = 0;
  int error_ = OB_SUCCESS;
  bool frozen_ = false;
  bool hierarchical_ = false;
  int layout_error_ = OB_SUCCESS;

  int scan_postings(int64_t source_cid, ObIVFEncodedPostingConsumer &consumer) const override
  {
    if (error_ != OB_SUCCESS) { return error_; }
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < prior_.postings_.count(); ++i) {
      const auto &p = prior_.postings_.at(i);
      if (p.centroid_idx_ == source_cid) {
        uint64_t code = p.vid_;
        ret = consumer.append(p.vid_, ObString(sizeof(code), reinterpret_cast<char *>(&code)));
        if (OB_SUCC(ret)) { ++copied_; }
      }
    }
    return ret;
  }
  const ObIVFMergePrior &prior_;
};

class IncrementalOutput : public ReuseCountingOutput
{
public:
  int64_t reused_ = 0;
  int64_t encoded_ = 0;
  bool supports_encoded_posting_reuse() const override { return true; }
  int quantize_cluster(ObIVFVectorReader &reader, const share::ObVectorMatrix &centers, int64_t k, int64_t cid,
      const ObIVFVectorIDView &members, const storage::ObIVFVectorIDView *secondary) override
  {
    encoded_ += members.count() + (secondary == nullptr ? 0 : secondary->count());
    return ReuseCountingOutput::quantize_cluster(reader, centers, k, cid, members, secondary);
  }
  int quantize_cluster_with_reuse(ObIVFVectorReader &reader, const share::ObVectorMatrix &centers,
      int64_t k, int64_t cid, const ObIVFVectorIDView &members,
      const storage::ObIVFVectorIDView *secondary, const ObIVFEncodedPostingSource &source,
      int64_t source_cid, int64_t prefix) override
  {
    struct Consumer final : ObIVFEncodedPostingConsumer {
      Consumer(const ObIVFVectorIDView &members, int64_t prefix) : members_(members), prefix_(prefix) {}
      int append(ObIVFVectorVID vid, const ObString &bytes) override {
        uint64_t code = 0;
        if (count_ >= prefix_ || members_.at(count_) != vid || bytes.length() != sizeof(code)) {
          return OB_INVALID_DATA;
        }
        MEMCPY(&code, bytes.ptr(), sizeof(code));
        if (code != vid) { return OB_INVALID_DATA; }
        ++count_; return OB_SUCCESS;
      }
      ObIVFVectorIDView members_;
      int64_t prefix_;
      int64_t count_ = 0;
    } consumer(members, prefix);
    int ret = source.scan_postings(source_cid, consumer);
    if (ret == OB_SUCCESS && consumer.count_ != prefix) { ret = OB_INVALID_DATA; }
    if (OB_SUCC(ret)) {
      reused_ += prefix;
      encoded_ += members.count() - prefix + (secondary == nullptr ? 0 : secondary->count());
      ret = ReuseCountingOutput::quantize_cluster(reader, centers, k, cid, members, secondary);
    }
    return ret;
  }
  int finish_quantization(int64_t n, int64_t k, int64_t postings) override
  {
    return primary_count_ >= n && k == clusters_
        && postings == primary_count_ + secondary_count_
        && reused_ + encoded_ == postings ? OB_SUCCESS : OB_ERR_UNEXPECTED;
  }
};

class IncrementalAlgorithm final : public RecordingReuseAlgorithm
{
public:
  ObArray<ObIVFVectorVID> soar_vids_;
  int soar_assign_secondary(ObIVFVectorReader &reader, const ObIVFVectorIDView &vids,
      const share::ObVectorMatrix &centers, const common::ObIArray<int64_t> &primary,
      const ObIVFNeighborGraphView *graph, int64_t k, int64_t begin, int64_t end,
      ObIArray<int64_t> &secondary) override
  {
    int ret = OB_SUCCESS;
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) { ret = soar_vids_.push_back(vids.at(i)); }
    return ret == OB_SUCCESS ? ObIVFHcaAlgorithmAdapter::soar_assign_secondary(
        reader, vids, centers, primary, graph, k, begin, end, secondary) : ret;
  }
};

void make_incremental_prior(ObIVFMergePrior &prior, const ReuseVectorStore &vectors)
{
  ASSERT_NO_FATAL_FAILURE(make_reuse_prior(prior, vectors, 1));
  const int64_t k = vectors.count_ / vectors.members_;
  for (int64_t i = 0; i < vectors.count_; ++i) {
    const int64_t cid = i / vectors.members_;
    ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(i + 1, cid)));
    if (i % vectors.members_ == 0) {
      const int64_t other = (cid + 1) % k;
      ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(i + 1, other)));
      ++prior.posting_mass_.at(other);
    }
  }
  prior.postings_complete_ = true;
}

int execute_incremental(ObIAllocator &allocator, const ObIVFMergeInput &input,
    ObIVFMergePrior &prior, ObIVFMergeAlgorithm &algorithm, ObIVFMergeQuantizer &output)
{
  ObArenaAllocator plans;
  ObIVFMergeProcessor processor;
  ObIVFInsertionPlanner planner;
  const ObIVFTaskPlan *plan = nullptr;
  int ret = processor.init(allocator, input, algorithm, output, ObIVFProcessType::INSERTION, &prior);
  if (OB_SUCC(ret)) { ret = planner.init(plans, processor); }
  if (OB_SUCC(ret)) { ret = planner.start(plan); }
  while (OB_SUCC(ret) && plan != nullptr) {
    for (int64_t i = 0; OB_SUCC(ret) && i < plan->get_task_count(); ++i) {
      ret = planner.process_task(*plan, i);
    }
    if (OB_SUCC(ret)) {
      const ObIVFTaskPlan *next = nullptr;
      ret = planner.finish_task_plan(*plan, next);
      plan = next;
    }
  }
  if (ret != OB_SUCCESS) { planner.cancel(ret); }
  return ret;
}
} // namespace



TEST(TestIVFIncrementalPosting, source_copy_failure_is_propagated)
{
  ObArenaAllocator allocator;
  ReuseVectorStore vectors;
  ObIVFMergePrior prior(allocator);
  ASSERT_NO_FATAL_FAILURE(make_incremental_prior(prior, vectors));
  PriorCodeSource source(prior);
  source.error_ = OB_IO_ERROR;
  prior.posting_source_ = &source;
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 32, false, true, true, VIDA_L2));
  IncrementalAlgorithm algorithm;
  ObIVFHcaAlgorithmParam param;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  IncrementalOutput output;
  EXPECT_EQ(OB_IO_ERROR, execute_incremental(allocator, input, prior, algorithm, output));
  EXPECT_EQ(0, output.raw_count_);
}



class FailingRuntimeAllocator final : public ObIAllocator
{
public:
  explicit FailingRuntimeAllocator(int64_t fail_at = -1, bool fail_remaining = false)
    : fail_at_(fail_at), calls_(0), fail_remaining_(fail_remaining) {}
  void *alloc(const int64_t size) override
  {
    ++calls_;
    return fail_at_ > 0 && (calls_ == fail_at_ || (fail_remaining_ && calls_ >= fail_at_))
        ? nullptr : memory_.alloc(size);
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  { UNUSED(attr); return alloc(size); }
  void free(void *ptr) override { memory_.free(ptr); }
  int64_t used() const override { return memory_.used(); }
  ObIVFMergeRuntimeAllocator memory_;
  int64_t fail_at_;
  int64_t calls_;
  bool fail_remaining_;
};

int run_incremental_failure(FailingRuntimeAllocator &memory, const bool cancel_at_soar = false)
{
  ObArenaAllocator owner;
  ReuseVectorStore vectors(64, 16); vectors.jitter_ = true;
  ObIVFMergePrior prior(owner);
  make_incremental_prior(prior, vectors);
  PriorCodeSource source(prior); prior.posting_source_ = &source;
  ObIVFSegmentPriorInfo delta;
  delta.live_vector_count_ = 8; delta.centroid_begin_ = 4;
  int ret = prior.segments_.push_back(delta);
  vectors.count_ += 8; prior.vector_count_ += 8;
  ObIVFMergeInput input;
  IncrementalAlgorithm algorithm; ObIVFHcaAlgorithmParam param;
  IncrementalOutput output;
  if (OB_SUCC(ret)) { ret = input.init(vectors, vectors, 16, false, true, true, VIDA_L2); }
  if (OB_SUCC(ret)) { ret = algorithm.init(param); }
  if (OB_FAIL(ret)) { return ret; }
  if (!cancel_at_soar) { return execute_incremental(memory, input, prior, algorithm, output); }
  ObIVFMergeProcessor processor; ObIVFInsertionPlanner planner;
  const ObIVFTaskPlan *plan = nullptr;
  if (OB_FAIL(processor.init(memory, input, algorithm, output, ObIVFProcessType::INSERTION, &prior))) {
  } else if (OB_FAIL(planner.init(owner, processor))) {
  } else if (OB_FAIL(planner.start(plan))) {
  }
  while (OB_SUCC(ret) && plan != nullptr) {
    for (int64_t i = 0; OB_SUCC(ret) && i < plan->get_task_count(); ++i) {
      ret = planner.process_task(*plan, i);
      if (OB_SUCC(ret) && planner.get_stage() == ObIVFMergeStage::SOAR) {
        const int64_t before = memory.used();
        processor.cancel(OB_CANCELED);
        EXPECT_EQ(before, memory.used());
        EXPECT_GT(before, 0);
        return OB_CANCELED;
      }
    }
    if (OB_SUCC(ret)) {
      const ObIVFTaskPlan *next = nullptr;
      ret = planner.finish_task_plan(*plan, next); plan = next;
    }
  }
  return ret;
}

TEST(TestIVFIncrementalPosting, allocation_failures_and_cancel_release_partial_state)
{
  FailingRuntimeAllocator reference;
  ASSERT_EQ(OB_SUCCESS, run_incremental_failure(reference));
  EXPECT_EQ(0, reference.used());
  ASSERT_GT(reference.calls_, 0);
  for (bool persistent : {false, true}) {
    for (int64_t fail = 1; fail <= reference.calls_; ++fail) {
      SCOPED_TRACE(fail);
      SCOPED_TRACE(persistent);
      FailingRuntimeAllocator memory(fail, persistent);
      run_incremental_failure(memory);
      EXPECT_EQ(0, memory.used());
    }
  }
  FailingRuntimeAllocator canceled;
  EXPECT_EQ(OB_CANCELED, run_incremental_failure(canceled, true));
  EXPECT_EQ(0, canceled.used());
}


TEST(TestIVFIncrementalPosting, cancellation_keeps_inflight_copy_input_alive)
{
  FailingRuntimeAllocator memory;
  {
    ObArenaAllocator owner;
    ReuseVectorStore vectors(64, 16);
    ObIVFMergePrior prior(owner);
    ASSERT_NO_FATAL_FAILURE(make_incremental_prior(prior, vectors));
    PriorCodeSource inner(prior);
    class BlockingSource final : public ObIVFEncodedPostingSource {
    public:
      explicit BlockingSource(const ObIVFEncodedPostingSource &inner) : inner_(inner) {}
      int scan_postings(int64_t cid, ObIVFEncodedPostingConsumer &consumer) const override {
        {
          std::unique_lock<std::mutex> lock(mutex_);
          entered_ = true; condition_.notify_all();
          condition_.wait(lock, [&] { return release_; });
        }
        return inner_.scan_postings(cid, consumer);
      }
      const ObIVFEncodedPostingSource &inner_;
      mutable std::mutex mutex_;
      mutable std::condition_variable condition_;
      mutable bool entered_ = false;
      mutable bool release_ = false;
    } source(inner);
    prior.posting_source_ = &source;
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 32, false, true, true, VIDA_L2));
    IncrementalAlgorithm algorithm; ObIVFHcaAlgorithmParam param;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
    IncrementalOutput output;
    ObIVFMergeProcessor processor; ObIVFInsertionPlanner planner;
    ASSERT_EQ(OB_SUCCESS, processor.init(memory, input, algorithm, output, ObIVFProcessType::INSERTION, &prior));
    ASSERT_EQ(OB_SUCCESS, planner.init(owner, processor));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    while (plan != nullptr && planner.get_stage() != ObIVFMergeStage::QUANT) {
      for (int64_t i = 0; i < plan->get_task_count(); ++i) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, i));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next)); plan = next;
    }
    ASSERT_NE(nullptr, plan);
    int worker_ret = OB_SUCCESS;
    std::thread worker([&] { worker_ret = planner.process_task(*plan, 0); });
    bool entered = false;
    {
      std::unique_lock<std::mutex> lock(source.mutex_);
      entered = source.condition_.wait_for(lock, std::chrono::seconds(10), [&] { return source.entered_; });
      const int64_t held = memory.used();
      processor.cancel(OB_CANCELED);
      EXPECT_GT(held, 0);
      EXPECT_EQ(held, memory.used());
      source.release_ = true; source.condition_.notify_all();
    }
    worker.join();
    EXPECT_TRUE(entered);
    EXPECT_EQ(OB_CANCELED, worker_ret);
    EXPECT_EQ(0, output.raw_count_);
    EXPECT_GT(memory.used(), 0);
  }
  EXPECT_EQ(0, memory.used());
}

int run_reclaiming_merge(FailingRuntimeAllocator &memory, int64_t &released_barriers,
    const ObIVFProcessType type = ObIVFProcessType::FULL_REBUILD)
{
  ObArenaAllocator plans;
  ReuseVectorStore vectors(64, 16);
  ObIVFMergeInput input;
  ObIVFMergePrior prior(plans);
  ConcatTestSource source(prior); prior.posting_source_ = &source;
  RecordingReuseAlgorithm algorithm;
  ObIVFHcaAlgorithmParam param;
  ConcatCountingOutput output;
  ObIVFMergeProcessor processor;
  ObIVFFullRebuildPlanner full;
  ObIVFConcatPlanner concat;
  ObIVFInsertionPlanner insertion;
  ObIVFMergePlanner *planner = type == ObIVFProcessType::FULL_REBUILD
      ? static_cast<ObIVFMergePlanner *>(&full)
      : type == ObIVFProcessType::CONCATENATION
          ? static_cast<ObIVFMergePlanner *>(&concat)
          : static_cast<ObIVFMergePlanner *>(&insertion);
  if (type != ObIVFProcessType::FULL_REBUILD) {
    make_reuse_prior(prior, vectors, 1);
    make_reuse_postings(prior, vectors);
  }
  int ret = input.init(vectors, vectors, 8, false, true, true, VIDA_L2);
  const ObIVFTaskPlan *plan = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(algorithm.init(param))) {
  } else if (OB_FAIL(processor.init(memory, input, algorithm, output, type,
      type == ObIVFProcessType::FULL_REBUILD ? nullptr : &prior))) {
  } else if (OB_FAIL(type == ObIVFProcessType::FULL_REBUILD ? full.init(plans, processor)
      : type == ObIVFProcessType::CONCATENATION ? concat.init(plans, processor)
      : insertion.init(plans, processor))) {
  } else if (OB_FAIL(planner->start(plan))) {
  } else {
    for (int64_t steps = 0; OB_SUCC(ret) && plan != nullptr && steps < 100; ++steps) {
      for (int64_t i = 0; OB_SUCC(ret) && i < plan->get_task_count(); ++i) {
        ret = planner->process_task(*plan, i);
      }
      if (OB_SUCC(ret)) {
        const int64_t before = memory.used();
        const ObIVFTaskPlan *next = nullptr;
        ret = planner->finish_task_plan(*plan, next);
        if (memory.used() < before) { ++released_barriers; }
        plan = next;
      }
    }
    if (OB_SUCC(ret) && (plan != nullptr || output.raw_count_ != vectors.count_
        || output.primary_count_ != vectors.count_)) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  if (OB_FAIL(ret)) { processor.cancel(ret); }
  return ret;
}
} // namespace

TEST(TestIVFMergeMemory, retires_results_at_barriers_and_frees_all_runtime_data)
{
  FailingRuntimeAllocator memory;
  int64_t released_barriers = 0;
  ASSERT_EQ(OB_SUCCESS, run_reclaiming_merge(memory, released_barriers));
  EXPECT_GT(released_barriers, 0);
  EXPECT_GT(memory.memory_.peak(), 0);
  EXPECT_EQ(0, memory.used());
}

TEST(TestIVFMergeMemory, vid_enumeration_owns_the_only_output_reservation)
{
  class ReservingStore final : public ReuseVectorStore
  {
  public:
    explicit ReservingStore(ObIVFMergeRuntimeAllocator &memory)
      : ReuseVectorStore(4097), memory_(memory)
    {}
    int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override
    {
      const int64_t allocations = memory_.allocation_count();
      EXPECT_EQ(0, vids.get_capacity());
      int ret = OB_SUCCESS;
      vids.reuse();
      if (OB_FAIL(vids.reserve(count_))) {
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
          ret = vids.push_back(i + 1);
        }
      }
      EXPECT_EQ(allocations + 1, memory_.allocation_count());
      return ret;
    }
    ObIVFMergeRuntimeAllocator &memory_;
  };
  ObIVFMergeRuntimeAllocator memory;
  ReservingStore vectors(memory);
  ObIVFMergeInput input;
  RecordingReuseAlgorithm algorithm;
  ConcatCountingOutput output;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 16, false, true, true, VIDA_L2));
  {
    ObIVFMergeProcessor processor;
    ASSERT_EQ(OB_SUCCESS, processor.init(memory, input, algorithm, output));
    ASSERT_EQ(vectors.count_, processor.runtime_->all_vids_.count());
    for (int64_t i = 0; i < vectors.count_; ++i) {
      EXPECT_EQ(i + 1, processor.runtime_->all_vids_.at(i));
    }
  }
  EXPECT_EQ(0, memory.used());
}

TEST(TestIVFMergeMemory, initial_build_position_ids_skip_all_vid_enumeration)
{
  class PositionStore final : public ReuseVectorStore
  {
  public:
    PositionStore() : ReuseVectorStore(10000000) {}
    bool uses_position_ids() const override { return true; }
    int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &) const override
    { ADD_FAILURE() << "PosID input must not enumerate all VIDs"; return OB_ERR_UNEXPECTED; }
  } vectors;
  ObArenaAllocator owner;
  ObIVFMergeInput input;
  RecordingReuseAlgorithm algorithm;
  ConcatCountingOutput output;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 16, false, true, true, VIDA_L2));
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS, processor.init(owner, input, algorithm, output, ObIVFProcessType::INITIAL_BUILD));
  EXPECT_EQ(10000000, processor.runtime_->all_vids_.count());
  EXPECT_EQ(9999999, processor.runtime_->all_vids_.at(9999999));
  EXPECT_EQ(0, processor.runtime_->all_vids_.allocated_vid_bytes());
}

TEST(TestIVFMergeMemory, initial_build_routes_ten_billion_positions_without_materializing_ids)
{
  constexpr int64_t vector_count = 10000000000L;
  class PositionStore final : public ReuseVectorStore
  {
  public:
    PositionStore() : ReuseVectorStore(10000000000L) {}
    bool uses_position_ids() const override { return true; }
    bool supports_source_order() const override { return true; }
    int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &) const override
    {
      ADD_FAILURE() << "Position routing must not materialize ten billion VIDs";
      return OB_ERR_UNEXPECTED;
    }
    int get_source_row_idx(int64_t, int64_t &) const override
    {
      ADD_FAILURE() << "Identity routing must not enumerate source rows";
      return OB_ERR_UNEXPECTED;
    }
  } vectors;
  ObArenaAllocator owner;
  ObIVFMergeInput input;
  RecordingReuseAlgorithm algorithm;
  ConcatCountingOutput output;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 384, false, true, true, VIDA_L2));
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS, processor.init(owner, input, algorithm, output, ObIVFProcessType::INITIAL_BUILD));
  ObIVFDDLHcaAdapter ddl_algorithm;
  ObIVFDDLRuntime routes(owner, ddl_algorithm, 64);
  ASSERT_EQ(OB_SUCCESS, routes.prepare_source_routes(*processor.runtime_));
  const int64_t positions[] = {0, int64_t(UINT32_MAX) - 1, int64_t(UINT32_MAX),
                              int64_t(UINT32_MAX) + 1, vector_count - 1};
  for (int64_t position : positions) {
    EXPECT_EQ(position, routes.vector_index(position));
    EXPECT_EQ(static_cast<uint64_t>(position), processor.runtime_->all_vids_.at(position));
  }
  EXPECT_EQ(0, processor.runtime_->all_vids_.allocated_vid_bytes());
  EXPECT_EQ(-1, routes.vector_index(-1));
  EXPECT_EQ(-1, routes.vector_index(vector_count));
  processor.runtime_->all_vids_.set_identity(vector_count - 1);
  EXPECT_EQ(OB_STATE_NOT_MATCH, routes.prepare_source_routes(*processor.runtime_));
  routes.release_routes();
  EXPECT_EQ(-1, routes.vector_index(0));
  ASSERT_EQ(OB_SUCCESS, routes.prepare_source_routes(*processor.runtime_));
  EXPECT_EQ(vector_count - 2, routes.vector_index(vector_count - 2));
  EXPECT_EQ(-1, routes.vector_index(vector_count - 1));
}

TEST(TestIVFMergeMemory, compact_sample_routes_preserve_positions_above_uint32)
{
  const int64_t high_position = 9999999999L;
  EXPECT_EQ(3 * sizeof(int64_t), sizeof(ObIVFClusterRead));
  share::ObVectorArray<ObIVFClusterRead> requests;
  ASSERT_EQ(OB_SUCCESS, requests.push_back({high_position, 7, high_position - 2}));
  ASSERT_EQ(OB_SUCCESS, requests.push_back({int64_t(UINT32_MAX) + 1, 8, high_position - 4}));
  ASSERT_EQ(OB_SUCCESS, requests.push_back({int64_t(UINT32_MAX), 9, high_position - 6}));
  std::sort(share::vector_array_begin(requests), share::vector_array_end(requests));
  EXPECT_EQ(int64_t(UINT32_MAX), requests.at(0).source_);
  EXPECT_EQ(int64_t(UINT32_MAX) + 1, requests.at(1).source_);
  EXPECT_EQ(high_position, requests.at(2).source_);
  EXPECT_EQ(7, requests.at(2).node_idx_);
  EXPECT_EQ(high_position - 2, requests.at(2).ordinal_);
}

TEST(TestIVFMergeMemory, source_route_storage_is_implicit_only_for_position_ids)
{
  class RouteStore final : public ObIVFVectorStore
  {
  public:
    explicit RouteStore(int mode) : mode_(mode) {}
    bool is_valid() const override { return true; }
    bool uses_position_ids() const override { return mode_ != 0; }
    bool supports_source_order() const override { return true; }
    int64_t get_vector_count() const override { return 9; }
    int64_t get_dimension() const override { return 2; }
    int get_source_row_idx(int64_t idx, int64_t &source) const override
    {
      source = mode_ == 1 ? idx : (2 * idx) % 9;
      return OB_SUCCESS;
    }
    int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &) const override { return OB_NOT_SUPPORTED; }
    int batch_get_by_vid(const ObIVFVectorIDView &, int64_t, int64_t, share::ObVectorMatrix &) const override
    {
      return OB_NOT_SUPPORTED;
    }
    int mode_;
  };
  for (int mode : {0, 1, 2}) {
    SCOPED_TRACE(mode); // VID permutation, identity PosID, invalid PosID permutation.
    ObArenaAllocator allocator("IVFRouteUT");
    RouteStore store(mode);
    ObIVFMergeRuntime runtime(allocator);
    runtime.vector_store_ = &store;
    runtime.dimension_ = 2;
    runtime.all_vids_.set_identity(9);
    ObIVFHierarchyVIDList members(allocator, store.uses_position_ids());
    for (int64_t idx : {0, 2, 5}) {
      ASSERT_EQ(OB_SUCCESS, members.vector_idxs_.push_back(idx));
      if (!store.uses_position_ids()) {
        ASSERT_EQ(OB_SUCCESS, members.vids_.push_back(idx));
      }
    }
    ObIVFHierarchyNodeState node(allocator, 2);
    node.vid_list_ = &members;
    node.local_k_ = 1;
    ASSERT_EQ(OB_SUCCESS, node.local_centers_.resize(2));
    ASSERT_EQ(OB_SUCCESS, runtime.current_nodes_.push_back(&node));
    ObIVFMergeActivePlan plan(allocator);
    struct Guard
    {
      ObIAllocator &allocator_;
      ObIVFMergeActivePlan &plan_;
      ~Guard() { free_runtime_object(allocator_, plan_.ddl_); }
    } guard{allocator, plan};
    const int ret = prepare_source_assign(runtime, plan, allocator);
    ASSERT_NE(nullptr, plan.ddl_);
    EXPECT_EQ(mode == 0 ? 9 : 0, plan.ddl_->source_vector_idxs_.count());
    if (mode != 0) {
      EXPECT_EQ(0, plan.ddl_->source_vector_idxs_.get_capacity());
    }
    if (mode == 2) {
      EXPECT_EQ(OB_INVALID_DATA, ret);
      continue;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(9, plan.ddl_->source_nodes_.count());
    ASSERT_EQ(1, plan.work_items_.count());
    EXPECT_EQ(3, plan.work_items_.at(0).source_input_count_);
    for (int64_t idx : {0, 2, 5}) {
      int64_t source = -1;
      ASSERT_EQ(OB_SUCCESS, store.get_source_row_idx(idx, source));
      EXPECT_EQ(0, plan.ddl_->source_nodes_.at(source));
      EXPECT_EQ(idx, plan.ddl_->source_vector_index(source));
    }
    EXPECT_EQ(-1, plan.ddl_->source_nodes_.at(8));
  }
}

TEST(TestIVFMergeMemory, initial_build_returns_retired_buffers_before_runtime_destruction)
{
  ObArenaAllocator owner;
  ReuseVectorStore vectors(64, 16);
  ObIVFMergeInput input;
  RecordingReuseAlgorithm algorithm;
  ConcatCountingOutput output;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 8, false, true, true, VIDA_L2));
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS, processor.init(owner, input, algorithm, output, ObIVFProcessType::INITIAL_BUILD));
  ASSERT_NE(&owner, processor.allocator_);
  const int64_t baseline = processor.runtime_allocator_.allocated();
  for (int round = 0; round < 3; ++round) {
    {
      ObArray<int64_t, ObWrapperAllocator> retired;
      retired.set_block_allocator(ObWrapperAllocator(*processor.allocator_));
      // Exceed the FIFO page size so the measurement excludes cached pages.
      ASSERT_EQ(OB_SUCCESS, retired.reserve(2 * OB_MALLOC_MIDDLE_BLOCK_SIZE / sizeof(int64_t)));
      EXPECT_GT(processor.runtime_allocator_.allocated(), baseline);
    }
    EXPECT_EQ(baseline, processor.runtime_allocator_.allocated());
    EXPECT_EQ(vectors.count_, processor.runtime_->all_vids_.count());
  }
}

TEST(TestIVFMergeMemory, allocator_preserves_alignment_and_accounts_concurrent_live_bytes)
{
  ObIVFMergeRuntimeAllocator memory;
  EXPECT_EQ(nullptr, memory.alloc(0));
  EXPECT_EQ(nullptr, memory.alloc(-1));
  EXPECT_EQ(nullptr, memory.alloc(INT64_MAX));
  std::mutex mutex;
  std::condition_variable condition;
  int arrived = 0;
  bool release = false;
  std::thread threads[4];
  for (int i = 0; i < 4; ++i) {
    threads[i] = std::thread([&] {
      void *ptr = memory.alloc(65537);
      EXPECT_NE(nullptr, ptr);
      EXPECT_EQ(0, reinterpret_cast<uintptr_t>(ptr) % 16);
      {
        std::unique_lock<std::mutex> lock(mutex);
        ++arrived;
        condition.notify_all();
        condition.wait(lock, [&] { return release; });
      }
      memory.free(ptr);
    });
  }
  {
    std::unique_lock<std::mutex> lock(mutex);
    condition.wait(lock, [&] { return arrived == 4; });
    EXPECT_EQ(4 * 65537, memory.used());
    EXPECT_EQ(4 * 65537, memory.peak());
    release = true;
    condition.notify_all();
  }
  for (auto &thread : threads) { thread.join(); }
  EXPECT_EQ(0, memory.used());
  EXPECT_EQ(4, memory.allocation_count());
}

TEST(TestIVFMergeMemory, allocation_failures_release_owned_data)
{
  for (const auto type : {ObIVFProcessType::FULL_REBUILD,
      ObIVFProcessType::CONCATENATION}) {
  SCOPED_TRACE(static_cast<int>(type));
  FailingRuntimeAllocator reference;
  int64_t released = 0;
  ASSERT_EQ(OB_SUCCESS, run_reclaiming_merge(reference, released, type));
  ASSERT_EQ(0, reference.used());
  // Sweep all runtime allocations of a multi-stage run, including every
  // prepare/execute/barrier allocation and their ownership rollback paths.
  for (int64_t fail_at = 1; fail_at <= reference.calls_; ++fail_at) {
    for (bool persistent : {false, true}) {
    SCOPED_TRACE(fail_at);
    SCOPED_TRACE(persistent);
    FailingRuntimeAllocator memory(fail_at, persistent);
    int64_t drops = 0;
    run_reclaiming_merge(memory, drops, type);
    EXPECT_EQ(0, memory.used());
    }
  }
  }
}

TEST(TestIVFMergeMemory, cancellation_keeps_inflight_results_until_destruction)
{
  FailingRuntimeAllocator memory;
  ObArenaAllocator plans;
  TestMergeInput vectors;
  TestMergeAlgorithm algorithm;
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 1, true, true, false));
  {
    ObIVFMergeProcessor processor;
    ObIVFFullRebuildPlanner planner;
    ASSERT_EQ(OB_SUCCESS, processor.init(memory, input, algorithm, algorithm));
    ASSERT_EQ(OB_SUCCESS, planner.init(plans, processor));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, 0));
    const int64_t before = memory.used();
    ASSERT_GT(before, 0);
    processor.cancel(OB_CANCELED);
    EXPECT_EQ(before, memory.used());
    EXPECT_TRUE(ObIVFMergeTestPeer::is_canceled(processor));
  }
  EXPECT_EQ(0, memory.used());
}

TEST(TestIVFMergeMemory, cancellation_does_not_free_a_running_workers_buffers)
{
  class BlockingAlgorithm final : public TestMergeAlgorithm
  {
  public:
    int hca_train_local_centers(int64_t depth, int64_t node, ObIVFVectorReader &reader,
        const ObIVFVectorIDView &vids, const ObIVFFirstLevelSampleSet *samples,
        int64_t cluster_size, int64_t &k, share::ObVectorMatrix &centers) override
    {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        entered_ = true;
        condition_.notify_all();
        condition_.wait(lock, [&] { return resume_; });
      }
      return TestMergeAlgorithm::hca_train_local_centers(
          depth, node, reader, vids, samples, cluster_size, k, centers);
    }
    std::mutex mutex_;
    std::condition_variable condition_;
    bool entered_ = false;
    bool resume_ = false;
  } algorithm;
  FailingRuntimeAllocator memory;
  ObArenaAllocator plans;
  TestMergeInput vectors;
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(vectors, vectors, 1, true, true, false));
  {
    ObIVFMergeProcessor processor;
    ObIVFFullRebuildPlanner planner;
    ASSERT_EQ(OB_SUCCESS, processor.init(memory, input, algorithm, algorithm));
    ASSERT_EQ(OB_SUCCESS, planner.init(plans, processor));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    int worker_ret = OB_SUCCESS;
    std::thread worker([&] { worker_ret = planner.process_task(*plan, 0); });
    {
      std::unique_lock<std::mutex> lock(algorithm.mutex_);
      algorithm.condition_.wait(lock, [&] { return algorithm.entered_; });
    }
    const int64_t before = memory.used();
    processor.cancel(OB_CANCELED);
    EXPECT_EQ(before, memory.used());
    {
      std::lock_guard<std::mutex> lock(algorithm.mutex_);
      algorithm.resume_ = true;
      algorithm.condition_.notify_all();
    }
    worker.join();
    EXPECT_EQ(OB_CANCELED, worker_ret);
    EXPECT_GT(memory.used(), 0);
  }
  EXPECT_EQ(0, memory.used());
}

namespace {
class DDLCountingStore final : public ObIVFVectorStore, public ObIVFFirstLevelSampleSet
{
public:
  explicit DDLCountingStore(int64_t n = 8193) : n_(n) {}
  int64_t n_;
  bool resident_ = true;
  int64_t prior_groups_ = 0;
  int64_t delta_begin_ = INT64_MAX;
  int64_t delta_groups_ = 1;
  int read_error_ = OB_SUCCESS;
  bool short_read_ = false;
  float invalid_value_ = 0.0F;
  mutable std::atomic<int64_t> source_rows_{0}, vid_rows_{0};
  mutable std::atomic<int64_t> max_batch_bytes_{0};
  bool track_source_visits_ = false;
  mutable std::mutex source_visits_lock_;
  mutable std::vector<int64_t> source_visits_;
  void start_source_tracking()
  {
    source_visits_.assign(n_, 0);
    track_source_visits_ = true;
  }
  int64_t get_available_replay_memory() const override { return INT64_MAX; }
  bool is_valid() const override { return true; }
  bool supports_source_order() const override { return true; }
  bool is_fully_memory_resident() const override { return resident_; }
  int64_t get_vector_count() const override { return n_; }
  int64_t get_sample_count() const override { return n_; }
  int64_t get_dimension() const override { return 2; }
  uint64_t vid(int64_t idx) const { return idx == 0 ? 0 : (1ULL << 48) + idx * 3; }
  float value(int64_t idx, int64_t d) const
  {
    if (prior_groups_ > 0) {
      const int64_t group = idx >= delta_begin_ ? idx % delta_groups_ : idx % prior_groups_;
      return static_cast<float>(group * 100 + d * 10 + (idx / prior_groups_) % 7);
    }
    const int64_t group = idx * 137 % 127;
    return static_cast<float>((d == 0 ? group % 16 : group / 16) * 20
        + (idx * (d == 0 ? 7 : 11) % 19)) / 3.0F;
  }
  int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &vids) const override
  { int ret = OB_SUCCESS; vids.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < n_; ++i) { ret = vids.push_back(vid(i)); }
    return ret; }
  int get_source_row_idx(int64_t idx, int64_t &source) const override
  { source = (idx * 2) % n_; return idx >= 0 && idx < n_ ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
  int batch_get_by_vid(const ObIVFVectorIDView &vids, int64_t begin, int64_t end,
      share::ObVectorMatrix &values) const override
  {
    if (read_error_ != OB_SUCCESS) { return read_error_; }
    int ret = OB_SUCCESS; values.reuse(); vid_rows_ += end - begin;
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      const int64_t idx = vids.at(i) == 0 ? 0 : (vids.at(i) - (1ULL << 48)) / 3;
      if (idx < 0 || idx >= n_ || vids.at(i) != vid(idx)) { ret = OB_INVALID_DATA; }
      for (int d = 0; OB_SUCC(ret) && d < 2; ++d) { ret = values.push_back(value(idx, d)); }
    }
    if (OB_SUCC(ret) && short_read_ && !values.empty()) { values.pop_back(); }
    if (OB_SUCC(ret) && !std::isfinite(invalid_value_) && !values.empty()) {
      values.at(values.count() - 1) = invalid_value_;
    }
    return ret;
  }
  int batch_get_by_source(const share::ObVectorArray<int64_t> &idxs, share::ObVectorMatrix &values) const override
  {
    if (read_error_ != OB_SUCCESS) { return read_error_; }
    int ret = OB_SUCCESS; values.reuse(); source_rows_ += idxs.count();
    const int64_t bytes = idxs.count() * 2 * sizeof(float);
    auto old = max_batch_bytes_.load();
    while (old < bytes && !max_batch_bytes_.compare_exchange_weak(old, bytes)) {}
    std::unique_lock<std::mutex> visits_guard(source_visits_lock_, std::defer_lock);
    if (track_source_visits_) { visits_guard.lock(); }
    int64_t previous = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < idxs.count(); ++i) {
      int64_t source = -1;
      if (OB_FAIL(get_source_row_idx(idxs.at(i), source))) {
      } else if (source <= previous) { ret = OB_INVALID_DATA; }
      else {
        previous = source;
        if (track_source_visits_) { ++source_visits_.at(idxs.at(i)); }
        for (int d = 0; OB_SUCC(ret) && d < 2; ++d) { ret = values.push_back(value(idxs.at(i), d)); }
      }
    }
    if (OB_SUCC(ret) && short_read_ && !values.empty()) { values.pop_back(); }
    return ret;
  }
  int batch_get_samples(int64_t begin, int64_t end, ObIArray<ObIVFVectorVID> &vids,
      share::ObVectorMatrix &values) const override
  {
    int ret = OB_SUCCESS; vids.reuse();
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) { ret = vids.push_back(vid(i)); }
    return OB_FAIL(ret) ? ret : batch_get_by_vid(vids, 0, vids.count(), values);
  }
};
// Force one legitimate frozen-center change so membership moves and the
// fused UPDATE must run. The numerical trainer itself is tested separately.
class DDLShiftedRefineAdapter final : public ObIVFDDLHcaAdapter
{
public:
  int finish_refine_phase(share::ObVectorMatrix &centers, bool &finished) override
  {
    int ret = ObIVFDDLHcaAdapter::finish_refine_phase(centers, finished);
    if (OB_SUCC(ret) && finished) {
      if (centers.count() < 4) { ret = OB_ERR_UNEXPECTED; }
      else { centers.at(0) += 10000.0F; centers.at(1) += 10000.0F; }
    }
    return ret;
  }
  int refine_train_finish_batch(share::ObVectorMatrix &centers, bool &finished) override
  {
    int ret = ObIVFDDLHcaAdapter::refine_train_finish_batch(centers, finished);
    if (OB_SUCC(ret) && finished) {
      if (centers.count() < 4) { ret = OB_ERR_UNEXPECTED; }
      else { centers.at(0) += 10000.0F; centers.at(1) += 10000.0F; }
    }
    return ret;
  }
};
class DDLCountingOutput : public ReuseCountingOutput
{
public:
  explicit DDLCountingOutput(int64_t expected_primary = 0) : expected_primary_(expected_primary) {}
  int64_t expected_primary_;
  bool prepared_ = false;
  int posting_drains_ = 0;
  int prepare_loaded_output(int64_t, int64_t) override { prepared_ = true; return OB_SUCCESS; }
  int64_t posting_write_task_count() const override { return expected_primary_ > 0 ? 3 : 0; }
  int write_posting_range(int64_t task) override
  {
    std::lock_guard<std::mutex> guard(output_lock_);
    if (!prepared_ || primary_count_ != expected_primary_ || task < 0 || task >= 3
        || (posting_drains_ & (1 << task)) != 0) { return OB_STATE_NOT_MATCH; }
    posting_drains_ |= 1 << task;
    return OB_SUCCESS;
  }
  int finish_quantization(int64_t n, int64_t k, int64_t postings) override
  {
    return expected_primary_ > 0 && posting_drains_ != 7 ? OB_STATE_NOT_MATCH
        : ReuseCountingOutput::finish_quantization(n, k, postings);
  }
  int output_error_ = OB_SUCCESS;
  ObIVFMergeProcessor *cancel_after_output_ = nullptr;
  std::mutex output_lock_;
  ObArray<uint32_t> leaf_posting_counts_;
  bool supports_loaded_quantization() const override { return true; }
  int set_leaf_posting_counts(const ObIArray<uint32_t> &counts) override
  { return leaf_posting_counts_.assign(counts); }
  int write_loaded_centers(const share::ObVectorMatrix &centers, int64_t, int64_t begin, int64_t end) override
  { int ret = centers_.empty() ? centers_.assign(centers) : OB_SUCCESS; clusters_ += end - begin; return ret; }
  int quantize_loaded_vectors(const share::ObVectorMatrix &, int64_t dim, const ObIVFVectorIDView &vids,
      const share::ObVectorMatrix &values, const common::ObIArray<int64_t> &primary, const common::ObIArray<int64_t> *secondary) override
  {
    std::lock_guard<std::mutex> guard(output_lock_);
    int ret = OB_SUCCESS;
    if (expected_primary_ > 0 && (!prepared_ || posting_drains_ != 0)) { return OB_STATE_NOT_MATCH; }
    if (dim != 2 || values.count() != vids.count() * dim || primary.count() != vids.count()) {
      return OB_INVALID_DATA;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < vids.count(); ++i) {
      ret = primary_postings_.push_back(ObIVFPriorPosting(vids.at(i), primary.at(i)));
      if (OB_SUCC(ret)) { ++primary_count_; }
      if (OB_SUCC(ret) && secondary != nullptr && secondary->at(i) >= 0) {
        ret = secondary_postings_.push_back(ObIVFPriorPosting(vids.at(i), secondary->at(i)));
        if (OB_SUCC(ret)) { ++secondary_count_; }
      }
      if (OB_SUCC(ret) && output_error_ != OB_SUCCESS) { ret = output_error_; }
    }
    if (OB_SUCC(ret) && cancel_after_output_ != nullptr) { cancel_after_output_->cancel(OB_CANCELED); }
    return ret;
  }
};
}

TEST(TestIVFDDLRuntime, position_routes_accept_ten_billion_rows_without_reverse_array)
{
  class PositionStore final : public ObIVFVectorStore {
  public:
    bool is_valid() const override { return true; }
    bool uses_position_ids() const override { return true; }
    bool supports_source_order() const override { return true; }
    int64_t get_vector_count() const override { return 10000000000L; }
    int64_t get_dimension() const override { return 1024; }
    int get_all_vids(share::ObVectorArray<ObIVFVectorVID> &) const override { return OB_NOT_SUPPORTED; }
    int batch_get_by_vid(const ObIVFVectorIDView &, int64_t, int64_t, share::ObVectorMatrix &) const override
    {
      return OB_NOT_SUPPORTED;
    }
  } store;
  ObArenaAllocator allocator;
  ObIVFMergeRuntime runtime(allocator);
  runtime.vector_store_ = &store;
  runtime.dimension_ = store.get_dimension();
  runtime.all_vids_.set_identity(store.get_vector_count());
  ObIVFDDLHcaAdapter algorithm;
  ObIVFDDLRuntime ddl(allocator, algorithm, 4);
  ASSERT_EQ(OB_SUCCESS, ddl.prepare_source_routes(runtime));
  EXPECT_TRUE(ddl.position_mode_);
  EXPECT_TRUE(ddl.source_to_vector_.empty());
  EXPECT_EQ(store.get_vector_count() - 1, ddl.vector_index(store.get_vector_count() - 1));
  EXPECT_EQ(-1, ddl.vector_index(store.get_vector_count()));
  const ObIVFClusterRead request = {store.get_vector_count() - 1, 0, store.get_vector_count() - 3};
  EXPECT_EQ(9999999999L, request.source_);
  EXPECT_EQ(9999999997L, request.ordinal_);
}

TEST(TestIVFDDLRuntime, cached_refine_matches_canonical_trainer_without_epoch_reads)
{
  DDLCountingStore store(131);
  share::ObVectorArray<ObIVFVectorVID> vids;
  share::ObVectorMatrix centers, expected, actual;
  share::ObVectorArray<int64_t> assignments;
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  for (int64_t i = 0; i < 4; ++i) {
    for (int d = 0; d < 2; ++d) { ASSERT_EQ(OB_SUCCESS, centers.push_back(store.value(i, d))); }
  }
  for (int64_t i = 0; i < store.n_; ++i) { ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % 4)); }
  ObIVFHcaAlgorithmParam param;
  param.refine_param_.random_seed_ = 7919;
  ObIVFHcaAlgorithmAdapter reference;
  ObIVFDDLHcaAdapter algorithm;
  ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, reference.init(param));
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  ASSERT_EQ(OB_SUCCESS, reference.refine_train(reader, vids, centers, assignments, nullptr, 4, expected));
  const int64_t before = store.vid_rows_.load();
  ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_samples(store.n_, 2, centers, assignments, nullptr));
  ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_sample_shard(0));
  EXPECT_EQ(before, store.vid_rows_.load()); // planning has no payload I/O
  const auto &samples = *algorithm.refine_sample_indices();
  ASSERT_EQ(128, samples.count());
  EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.finish_refine_samples());
  // Reverse task completion and intentionally permute each loaded subset.
  for (int64_t end = samples.count(); end > 0;) {
    const int64_t begin = MAX(0L, end - 13);
    ObArray<int64_t> positions;
    share::ObVectorMatrix values;
    for (int64_t pos = end; pos-- > begin;) {
      ASSERT_EQ(OB_SUCCESS, positions.push_back(pos));
      for (int d = 0; d < 2; ++d) { ASSERT_EQ(OB_SUCCESS, values.push_back(store.value(samples.at(pos), d))); }
    }
    ASSERT_EQ(OB_SUCCESS, algorithm.load_refine_samples(positions, values));
    end = begin;
  }
  ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_sample_shard(0));
  ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_samples());
  bool finished = false;
  for (int batch = 0; !finished && batch < 100; ++batch) {
    int64_t rows = 0;
    ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(reader, vids, centers, assignments, nullptr, 4, rows));
    std::vector<std::thread> workers;
    std::vector<int> results(2);
    for (int t = 0; t < 2; ++t) {
      workers.emplace_back([&, t] { results[t] = algorithm.refine_train_distances(rows * t / 2, rows * (t + 1) / 2); });
    }
    for (auto &worker : workers) { worker.join(); }
    for (int result : results) { ASSERT_EQ(OB_SUCCESS, result); }
    ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_finish_batch(actual, finished));
  }
  ASSERT_TRUE(finished);
  ASSERT_EQ(expected.count(), actual.count());
  EXPECT_EQ(0, MEMCMP(&expected.at(0), &actual.at(0), expected.count() * sizeof(float)));
  EXPECT_EQ(before, store.vid_rows_.load());
  EXPECT_EQ(nullptr, algorithm.refine_sample_indices());
}

TEST(TestIVFDDLRuntime, resident_and_cached_refine_preserve_epoch_and_sample_boundaries)
{
  struct Case { int64_t n_, k_, dim_, samples_per_cluster_; bool cosine_; };
  const Case cases[] = {
      {53, 5, 17, 3, false}, {53, 5, 17, 3, true},
      {7, 5, 17, 32, true}, // One wrapped batch containing all samples.
      {1003, 129, 17, 3, true}, // Variable-length neighborhood rows.
      {10003, 129, 17, 33, true},
      {53, 5, 17, 1, false}, // Sample pool smaller than a batch.
      {10003, 1024, 768, 33, true}, // Matrix and distance blocks with non-power-of-two rows.
      {10003, 129, 1024, 33, true}}; // Large multi-epoch resident batch store.
  for (const auto &c : cases) {
    SCOPED_TRACE(::testing::Message() << c.n_ << "/" << c.k_ << "/" << c.dim_
        << "/" << c.samples_per_cluster_ << "/" << c.cosine_);
    SoarArrayStore store(c.n_, c.dim_);
    ObVectorNormalizeInfo norm;
    share::ObVectorArray<ObIVFVectorVID> vids;
    share::ObVectorMatrix centers;
    ObArray<float> limits;
    share::ObVectorMatrix expected, actual;
    ObArray<int64_t> offsets;
    share::ObVectorArray<int64_t> assignments, neighbors;
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    ASSERT_EQ(OB_SUCCESS, centers.set_dimension(c.dim_));
    for (int64_t i = 0; i < c.n_; ++i) {
      float *vector = &store.data_[i * c.dim_];
      for (int64_t d = 0; d < c.dim_; ++d) {
        vector[d] = static_cast<float>(((i + 7) * (d + 11)) % 101 - 50) * 0.03125F;
      }
      if (c.cosine_) {
        ASSERT_EQ(OB_SUCCESS, norm.normalize_func_(c.dim_, vector, vector, nullptr));
      }
      ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % c.k_));
      if (i < c.k_) {
        for (int64_t d = 0; d < c.dim_; ++d) {
          ASSERT_EQ(OB_SUCCESS, centers.push_back(vector[d]));
        }
      }
    }
    for (int64_t cid = 0; cid < c.k_; ++cid) {
      ASSERT_EQ(OB_SUCCESS, offsets.push_back(neighbors.count()));
      const int64_t candidates[] = {(cid + 1) % c.k_, (cid + 7) % c.k_, (cid + 19) % c.k_};
      for (int64_t j = 0; j < 1 + cid % 3; ++j) {
        ASSERT_EQ(OB_SUCCESS, neighbors.push_back(candidates[j]));
      }
      ASSERT_EQ(OB_SUCCESS, limits.push_back(0.0F));
    }
    ASSERT_EQ(OB_SUCCESS, offsets.push_back(neighbors.count()));
    ObIVFNeighborGraphView graph(offsets, neighbors, limits, c.k_);
    const auto *graph_ptr = c.k_ > OB_HCA_NEIGHBOR_COUNT ? &graph : nullptr;
    ObIVFHcaAlgorithmParam param;
    param.refine_param_.samples_per_cluster_ = c.samples_per_cluster_;
    param.refine_param_.max_iterations_ = 3;
    param.refine_param_.random_seed_ = 9123;
    param.center_norm_info_ = c.cosine_ ? &norm : nullptr;
    ObIVFHcaAlgorithmAdapter reference;
    ObIVFVectorReader reader;
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    ASSERT_EQ(OB_SUCCESS, reference.init(param));
    ASSERT_EQ(OB_SUCCESS, reference.refine_train(reader, vids, centers, assignments, graph_ptr, c.k_, expected));
    const int64_t sample_count = MIN(c.n_, c.k_ * c.samples_per_cluster_);
    for (int mode : {0, 1, 2}) {
      const bool resident = mode != 0;
      store.position_ids_ = mode == 2;
      const ObIVFVectorIDView input_ids
          = store.position_ids_ ? ObIVFVectorIDView::identity(c.n_) : ObIVFVectorIDView(vids);
      SCOPED_TRACE(mode);
      ObIVFDDLHcaAdapter algorithm;
      ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
      store.resident_ = resident;
      store.fail_ = !resident; // Materialized training must not read the original payload.
      const int64_t read_before = store.read_rows_.load();
      ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_samples(c.n_, c.dim_, centers, assignments,
          graph_ptr, INT64_MAX, 1, &store, input_ids));
      EXPECT_EQ(resident, algorithm.uses_resident_refine_source());
      EXPECT_EQ(read_before, store.read_rows_.load());
      if (resident) {
        EXPECT_EQ(0, algorithm.refine_sample_shards());
        EXPECT_EQ(0, algorithm.refine_sample_memory_hold());
        EXPECT_EQ(0, algorithm.refine_sample_file_size());
        ASSERT_NE(nullptr, algorithm.refine_sample_indices());
        EXPECT_TRUE(algorithm.refine_sample_indices()->empty());
        int64_t rows = 0;
        EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.refine_train_prepare(
            reader, input_ids, centers, assignments, graph_ptr, c.k_, rows));
        EXPECT_EQ(0, rows);
      } else {
        ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_sample_shard(0));
        const auto &samples = *algorithm.refine_sample_indices();
        ASSERT_EQ(sample_count, samples.count());
        for (int64_t end = samples.count(); end > 0;) {
          const int64_t begin = MAX(0L, end - 257);
          ObArray<int64_t> positions;
          share::ObVectorMatrix values;
          for (int64_t pos = end; pos-- > begin;) {
            ASSERT_EQ(OB_SUCCESS, positions.push_back(pos));
            for (int64_t d = 0; d < c.dim_; ++d) {
              ASSERT_EQ(OB_SUCCESS, values.push_back(store.data_[samples.at(pos) * c.dim_ + d]));
            }
          }
          ASSERT_EQ(OB_SUCCESS, algorithm.load_refine_samples(positions, values));
          end = begin;
        }
        ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_sample_shard(0));
      }
      ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_samples());
      EXPECT_EQ(read_before, store.read_rows_.load());
      bool finished = false;
      int64_t batches = 0;
      while (!finished && batches < 100) {
        int64_t rows = 0;
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(
            reader, input_ids, centers, assignments, graph_ptr, c.k_, rows));
        ASSERT_EQ(MIN(c.n_, c.k_ * 2), rows);
        std::vector<std::thread> workers;
        std::vector<int> results(4);
        for (int t = 0; t < 4; ++t) {
          workers.emplace_back([&, t] {
            results[t] = algorithm.refine_train_distances(rows * t / 4, rows * (t + 1) / 4);
          });
        }
        for (auto &worker : workers) { worker.join(); }
        for (int result : results) { ASSERT_EQ(OB_SUCCESS, result); }
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_finish_batch(actual, finished));
        ++batches;
      }
      ASSERT_TRUE(finished);
      const int64_t batch_size = MIN(c.n_, c.k_ * 2);
      EXPECT_EQ(3 * ((sample_count + batch_size - 1) / batch_size), batches);
      EXPECT_EQ(resident ? batch_size * batches : 0, store.read_rows_.load() - read_before);
      ASSERT_EQ(expected.count(), actual.count());
      for (int64_t cid = 0; cid < c.k_; ++cid) {
        EXPECT_EQ(0,
            MEMCMP(
                expected.data_at(cid * c.dim_, c.dim_), actual.data_at(cid * c.dim_, c.dim_), c.dim_ * sizeof(float)));
      }
      EXPECT_EQ(nullptr, algorithm.refine_sample_indices());
      EXPECT_FALSE(algorithm.uses_resident_refine_source());
    }
  }
}

TEST(TestIVFDDLRuntime, cached_refine_errors_and_cancel_do_not_publish)
{
  DDLCountingStore store(23);
  share::ObVectorArray<ObIVFVectorVID> vids;
  share::ObVectorMatrix centers, published, values;
  ObArray<int64_t> positions;
  share::ObVectorArray<int64_t> assignments;
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  for (int64_t i = 0; i < 3; ++i) {
    for (int d = 0; d < 2; ++d) { ASSERT_EQ(OB_SUCCESS, centers.push_back(store.value(i, d))); }
  }
  for (int64_t i = 0; i < store.n_; ++i) { ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % 3)); }
  ASSERT_EQ(OB_SUCCESS, published.push_back(123.0F));
  ObIVFDDLHcaAdapter algorithm;
  ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  int64_t rows = 0;
  bool finished = true;
  EXPECT_EQ(OB_NOT_INIT, algorithm.refine_train_prepare(reader, vids, centers, assignments, nullptr, 3, rows));
  EXPECT_EQ(OB_NOT_INIT, algorithm.refine_train_distances(0, 1));
  ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
  assignments.at(0) = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, algorithm.begin_refine_samples(store.n_, 2, centers, assignments, nullptr));
  EXPECT_EQ(nullptr, algorithm.refine_sample_indices());
  assignments.at(0) = 0;
  ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_samples(store.n_, 2, centers, assignments, nullptr));
  ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_sample_shard(0));
  EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.finish_refine_samples());
  EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.refine_train_prepare(reader, vids, centers, assignments, nullptr, 3, rows));
  const auto &samples = *algorithm.refine_sample_indices();
  for (int64_t i = 0; i < samples.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, positions.push_back(i));
    for (int d = 0; d < 2; ++d) { ASSERT_EQ(OB_SUCCESS, values.push_back(store.value(samples.at(i), d))); }
  }
  const float last_value = values.at(values.count() - 1);
  values.pop_back();
  EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.load_refine_samples(positions, values));
  ASSERT_EQ(OB_SUCCESS, values.push_back(last_value));
  positions.at(0) = samples.count();
  EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.load_refine_samples(positions, values));
  positions.at(0) = 0;
  EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.finish_refine_samples());
  ASSERT_EQ(OB_SUCCESS, algorithm.load_refine_samples(positions, values));
  ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_sample_shard(0));
  ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_samples());
  ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(reader, vids, centers, assignments, nullptr, 3, rows));
  EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.refine_train_distances(-1, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, algorithm.refine_train_distances(0, rows + 1));
  ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_distances(0, 1));
  EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.refine_train_finish_batch(published, finished));
  EXPECT_FALSE(finished);
  ASSERT_EQ(1, published.count());
  EXPECT_FLOAT_EQ(123.0F, published.at(0));
  algorithm.cancel(OB_TIMEOUT);
  EXPECT_EQ(OB_TIMEOUT, algorithm.refine_train_distances(1, rows));
  EXPECT_EQ(OB_TIMEOUT, algorithm.refine_train_finish_batch(published, finished));
  EXPECT_FALSE(finished);
  ASSERT_EQ(1, published.count());
  EXPECT_FLOAT_EQ(123.0F, published.at(0));
  algorithm.release_samples();
  EXPECT_EQ(nullptr, algorithm.refine_sample_indices());
}

TEST(TestIVFDDLRuntime, resident_refine_read_errors_do_not_publish)
{
  // Fail both the first gather and the gather performed after an otherwise
  // successful batch update; neither may publish partially trained centers.
  for (const bool after_first_batch : {false, true}) {
    for (int mode = 0; mode < 5; ++mode) {
      SCOPED_TRACE(::testing::Message() << after_first_batch << "/" << mode);
      DDLCountingStore store(23);
      share::ObVectorArray<ObIVFVectorVID> vids;
      share::ObVectorMatrix centers, published;
      share::ObVectorArray<int64_t> assignments;
      ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
      for (int64_t i = 0; i < store.n_; ++i) {
        ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % 3));
        if (i < 3) {
          for (int d = 0; d < 2; ++d) { ASSERT_EQ(OB_SUCCESS, centers.push_back(store.value(i, d))); }
        }
      }
      ASSERT_EQ(OB_SUCCESS, published.push_back(123.0F));
      ObIVFDDLHcaAdapter algorithm;
      ObIVFVectorReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.init(store));
      ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
      ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_samples(
          store.n_, 2, centers, assignments, nullptr, 0, 4, &store, vids));
      ASSERT_TRUE(algorithm.uses_resident_refine_source());
      ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_samples());
      int64_t rows = 0;
      if (after_first_batch) {
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(
            reader, vids, centers, assignments, nullptr, 3, rows));
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_distances(0, rows));
      }
      store.read_error_ = mode == 0 ? OB_IO_ERROR : mode == 4 ? OB_CANCELED : OB_SUCCESS;
      store.short_read_ = mode == 1;
      store.invalid_value_ = mode == 2 ? NAN : mode == 3 ? INFINITY : 0.0F;
      const int expected_error = mode == 0 ? OB_IO_ERROR : mode == 4 ? OB_CANCELED : OB_INVALID_DATA;
      bool finished = true;
      if (after_first_batch) {
        EXPECT_EQ(expected_error, algorithm.refine_train_finish_batch(published, finished));
        EXPECT_FALSE(finished);
      } else {
        EXPECT_EQ(expected_error, algorithm.refine_train_prepare(
            reader, vids, centers, assignments, nullptr, 3, rows));
        EXPECT_EQ(0, rows);
      }
      EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.refine_train_finish_batch(published, finished));
      EXPECT_FALSE(finished);
      ASSERT_EQ(1, published.count());
      EXPECT_FLOAT_EQ(123.0F, published.at(0));
      EXPECT_EQ(0, algorithm.refine_sample_shards());
      EXPECT_EQ(0, algorithm.refine_sample_file_size());
      store.read_error_ = OB_SUCCESS;
      store.short_read_ = false;
      store.invalid_value_ = 0.0F;
      ObIVFHcaAlgorithmAdapter reference;
      share::ObVectorMatrix expected;
      ASSERT_EQ(OB_SUCCESS, reference.init(ObIVFHcaAlgorithmParam()));
      ASSERT_EQ(OB_SUCCESS, reference.refine_train(reader, vids, centers, assignments, nullptr, 3, expected));
      // Retry the failed gather in the same session, including after an update.
      // Its successful predecessor must not be applied a second time.
      for (int batch = 0; !finished && batch < 100; ++batch) {
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(
            reader, vids, centers, assignments, nullptr, 3, rows));
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_distances(0, rows));
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_finish_batch(published, finished));
      }
      ASSERT_TRUE(finished);
      ASSERT_EQ(expected.count(), published.count());
      EXPECT_EQ(0, MEMCMP(&expected.at(0), &published.at(0), expected.count() * sizeof(float)));
      algorithm.release_samples();
      EXPECT_EQ(nullptr, algorithm.refine_sample_indices());
      EXPECT_FALSE(algorithm.uses_resident_refine_source());
    }
  }
}

TEST(TestIVFDDLRuntime, resident_refine_cancel_does_not_publish)
{
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    DDLCountingStore store(23);
    share::ObVectorArray<ObIVFVectorVID> vids;
    share::ObVectorMatrix centers, published;
    share::ObVectorArray<int64_t> assignments;
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    for (int64_t i = 0; i < store.n_; ++i) {
      ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % 3));
      if (i < 3) {
        for (int d = 0; d < 2; ++d) { ASSERT_EQ(OB_SUCCESS, centers.push_back(store.value(i, d))); }
      }
    }
    ASSERT_EQ(OB_SUCCESS, published.push_back(123.0F));
    ObIVFDDLHcaAdapter algorithm;
    ObIVFVectorReader reader;
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_samples(
        store.n_, 2, centers, assignments, nullptr, 0, 4, &store, vids));
    ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_samples());
    int64_t rows = 0;
    if (mode != 0) {
      ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(
          reader, vids, centers, assignments, nullptr, 3, rows));
      ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_distances(0, mode == 1 ? 1 : rows));
    }
    algorithm.cancel(OB_TIMEOUT);
    bool finished = true;
    EXPECT_EQ(OB_TIMEOUT, algorithm.refine_train_prepare(
        reader, vids, centers, assignments, nullptr, 3, rows));
    EXPECT_EQ(OB_TIMEOUT, algorithm.refine_train_distances(0, 1));
    EXPECT_EQ(OB_TIMEOUT, algorithm.refine_train_finish_batch(published, finished));
    EXPECT_FALSE(finished);
    ASSERT_EQ(1, published.count());
    EXPECT_FLOAT_EQ(123.0F, published.at(0));
    EXPECT_EQ(mode == 0 ? 0 : 6, store.vid_rows_.load());
    algorithm.release_samples();
    EXPECT_FALSE(algorithm.uses_resident_refine_source());
  }
}

TEST(TestIVFDDLRuntime, configuration_rejects_output_without_loaded_quantization)
{
  ObArenaAllocator allocator;
  DDLCountingStore store(3);
  ObIVFMergeInput input;
  ObIVFDDLHcaAdapter algorithm;
  ReuseCountingOutput output;
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
  ASSERT_EQ(OB_SUCCESS, input.init(store, store, 64, false, true, true, VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, ObIVFProcessType::INITIAL_BUILD));
  EXPECT_EQ(OB_NOT_SUPPORTED, processor.configure_ddl_runtime(algorithm, 4));
  EXPECT_EQ(nullptr, processor.runtime_->ddl_runtime_);
  EXPECT_EQ(0, store.source_rows_.load());
}

// Snapshot task results before the SOAR commit retires dense assignment state.
// Output/readback checks below must not keep that production memory alive.
static void snapshot_soar_assignments(const ObIVFMergeRuntime &runtime,
    ObIArray<int64_t> &primary, ObIArray<int64_t> &secondary)
{
  ASSERT_NE(nullptr, runtime.active_plan_);
  ASSERT_NE(nullptr, runtime.ddl_runtime_);
  const auto &plan = *runtime.active_plan_;
  const int64_t count = runtime.all_vids_.count();
  ASSERT_EQ(OB_SUCCESS, primary.prepare_allocate(count));
  for (int64_t i = 0; i < count; ++i) { primary.at(i) = -1; }
  const bool has_secondary = runtime.enable_soar_ && runtime.final_cluster_count_ > 1;
  if (has_secondary) {
    ASSERT_EQ(OB_SUCCESS, secondary.prepare_allocate(count));
    for (int64_t i = 0; i < count; ++i) { secondary.at(i) = -1; }
  }
  for (int64_t t = 0; t < plan.work_items_.count(); ++t) {
    const auto &item = plan.work_items_.at(t);
    const auto &slot = plan.result_slots_.at(t);
    ASSERT_NE(nullptr, slot.center_idxs_);
    ASSERT_EQ(item.vid_end_ - item.vid_begin_, slot.center_idxs_->count());
    if (has_secondary) {
      ASSERT_NE(nullptr, slot.secondary_center_idxs_);
      ASSERT_EQ(slot.center_idxs_->count(), slot.secondary_center_idxs_->count());
    }
    for (int64_t j = 0; j < slot.center_idxs_->count(); ++j) {
      const int64_t idx = runtime.ddl_runtime_->vector_index(item.vid_begin_ + j);
      ASSERT_GE(idx, 0);
      ASSERT_LT(idx, count);
      ASSERT_EQ(-1, primary.at(idx));
      primary.at(idx) = slot.center_idxs_->at(j);
      if (has_secondary) { secondary.at(idx) = slot.secondary_center_idxs_->at(j); }
    }
  }
}

TEST(TestIVFDDLRuntime, full_pipeline_source_batches_preserve_members_and_zero_read_update)
{
  class PerfObserver final : public ObIVFMergePerfObserver
  {
  public:
    void on_step_perf(const ObIVFMergeStepPerf &perf) override { snapshots_.push_back(perf); }
    std::vector<ObIVFMergeStepPerf> snapshots_;
  };
  for (const auto type : {ObIVFProcessType::INITIAL_BUILD, ObIVFProcessType::FULL_REBUILD}) {
  SCOPED_TRACE(static_cast<int>(type));
  for (int mode = 0; mode < 4; ++mode) {
    const bool soar = (mode & 1) != 0;
    const bool resident = (mode & 2) != 0;
    SCOPED_TRACE(::testing::Message() << "soar=" << soar << "/resident=" << resident);
    // FULL_REBUILD borrows this allocator for concurrent source/update tasks.
    ObIVFMergeRuntimeAllocator allocator;
    DDLCountingStore store;
    store.resident_ = resident;
    ObIVFMergeInput input;
    DDLShiftedRefineAdapter algorithm;
    DDLCountingOutput output(store.n_);
    ObIVFMergeProcessor processor;
    ObIVFFullBuildPlanner planner;
    PerfObserver observer;
    planner.set_perf_observer(&observer);
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ASSERT_EQ(OB_SUCCESS, input.init(store, store, 64, false, true, soar, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type));
    ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 4));
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, type));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    int sample_stages = 0, assign_stages = 0, update_stages = 0, quant_stages = 0;
    bool injected_failure = false;
    bool injected_sample_failure = false;
    int64_t refine_vid_reads = 0;
    for (int guard = 0; plan != nullptr && guard < 200; ++guard) {
      const auto step = planner.get_step();
      if (resident) { EXPECT_NE(ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE, step); }
      const int64_t source_before = store.source_rows_.load(), vid_before = store.vid_rows_.load();
      if (step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE && resident) {
        EXPECT_TRUE(algorithm.uses_resident_refine_source());
        EXPECT_EQ(1, plan->get_task_count());
        EXPECT_EQ(0, algorithm.refine_sample_shards());
        EXPECT_EQ(0, algorithm.refine_sample_memory_hold());
        EXPECT_EQ(0, algorithm.refine_sample_file_size());
        EXPECT_EQ(0, algorithm.refine_sample_read_rows());
      }
      if (step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE && !resident && !injected_sample_failure) {
        ObIVFTaskResult result;
        ASSERT_EQ(OB_SUCCESS, result.init(*plan, 0));
        store.read_error_ = OB_IO_ERROR;
        EXPECT_EQ(OB_IO_ERROR, processor.execute_task(planner.get_stage(), step, planner.get_depth(), 0, result));
        EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.finish_refine_samples());
        store.read_error_ = OB_SUCCESS;
        injected_sample_failure = true;
      }
      if (step == ObIVFMergeStep::REFINE_ASSIGN && !injected_failure) {
        ObIVFTaskResult result;
        ASSERT_EQ(OB_SUCCESS, result.init(*plan, 0));
        store.read_error_ = OB_IO_ERROR;
        EXPECT_EQ(OB_IO_ERROR, processor.execute_task(planner.get_stage(), step, planner.get_depth(), 0, result));
        store.read_error_ = OB_SUCCESS;
        injected_failure = true;
      }
      // Exercise interleaved source windows, stats commits and fused posting output.
      if (step == ObIVFMergeStep::REFINE_ASSIGN || step == ObIVFMergeStep::SOAR_ASSIGN
          || step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE || step == ObIVFMergeStep::REFINE_UPDATE) {
        std::atomic<int64_t> next(plan->get_task_count());
        std::atomic<int> errors(0);
        std::vector<std::thread> workers;
        for (int t = 0; t < 4; ++t) {
          ObTenantBase *tenant = ObTenantEnv::get_tenant();
          workers.emplace_back([&, tenant] {
            ObTenantEnv::set_tenant(tenant);
            for (;;) {
              const int64_t task = --next;
              if (task < 0) { break; }
              if (planner.process_task(*plan, task) != OB_SUCCESS) { ++errors; }
            }
            ObTenantEnv::set_tenant(nullptr);
          });
        }
        for (auto &worker : workers) { worker.join(); }
        ASSERT_EQ(0, errors.load()) << ivf_merge_step_name(step);
      } else {
        for (int64_t t = plan->get_task_count(); t-- > 0;) { ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t)); }
      }
      const int64_t reads = store.source_rows_.load() - source_before;
      if (step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE) {
        ++sample_stages;
        EXPECT_EQ(vid_before, store.vid_rows_.load());
        if (resident) { EXPECT_EQ(0, reads); }
        else { EXPECT_GT(reads, 0); EXPECT_LE(reads, store.n_); }
      }
      if (step == ObIVFMergeStep::REFINE_ASSIGN) { ++assign_stages; EXPECT_EQ(store.n_, reads); }
      if (step == ObIVFMergeStep::REFINE_UPDATE) { ++update_stages; EXPECT_EQ(0, reads); }
      if (step == ObIVFMergeStep::REFINE_TRAIN || step == ObIVFMergeStep::REFINE_UPDATE) {
        EXPECT_EQ(vid_before, store.vid_rows_.load()); EXPECT_EQ(0, reads);
      }
      if (step == ObIVFMergeStep::SOAR_ASSIGN) { EXPECT_EQ(store.n_, reads); }
      if (step == ObIVFMergeStep::QUANT) { EXPECT_EQ(0, reads); }
      if (step == ObIVFMergeStep::QUANT) { ++quant_stages; }
      ObArray<int64_t> saved_primary, saved_secondary;
      if (step == ObIVFMergeStep::SOAR_ASSIGN) {
        ASSERT_NO_FATAL_FAILURE(snapshot_soar_assignments(*processor.runtime_, saved_primary, saved_secondary));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next)) << ivf_merge_step_name(step);
      plan = next;
      if (step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE || step == ObIVFMergeStep::REFINE_TRAIN) {
        refine_vid_reads += store.vid_rows_.load() - vid_before;
      }
      if (step == ObIVFMergeStep::REFINE_ASSIGN) {
        auto &r = *processor.runtime_;
        ASSERT_NE(nullptr, r.refined_assignments_);
        std::vector<int64_t> seen(store.n_, 0);
        for (int64_t cid = 0; cid < r.refined_member_lists_->lists_.count(); ++cid) {
          const auto &members = *r.refined_member_lists_->lists_.at(cid);
          for (int64_t j = 0; j < members.vids_.count(); ++j) {
            const int64_t idx = members.vector_idxs_.at(j);
            EXPECT_EQ(store.vid(idx), members.vids_.at(j));
            EXPECT_EQ(cid, r.refined_assignments_->at(idx)); ++seen[idx];
          }
        }
        for (auto count : seen) { EXPECT_EQ(1, count); }
      }
      if (step == ObIVFMergeStep::SOAR_ASSIGN) {
        EXPECT_EQ(nullptr, processor.runtime_->soar_member_lists_);
        EXPECT_EQ(nullptr, processor.runtime_->a1_);
        EXPECT_EQ(nullptr, processor.runtime_->a2_);
        EXPECT_FALSE(processor.runtime_->ddl_runtime_->routes_ready_);
        EXPECT_EQ(0, processor.runtime_->ddl_runtime_->route_allocator_.total());
        EXPECT_EQ(store.n_, output.primary_count_);
        EXPECT_EQ(processor.runtime_->secondary_assignment_count_, output.secondary_count_);
        for (const bool secondary : {false, true}) {
          if (secondary && !soar) { EXPECT_EQ(0, output.secondary_count_); continue; }
          const auto &postings = secondary ? output.secondary_postings_ : output.primary_postings_;
          const auto &assignments = secondary ? saved_secondary : saved_primary;
          for (int64_t i = 0; i < postings.count(); ++i) {
            const auto &posting = postings.at(i);
            const int64_t idx = posting.vid_ == 0 ? 0 : (posting.vid_ - (1ULL << 48)) / 3;
            ASSERT_GE(idx, 0);
            ASSERT_LT(idx, store.n_);
            EXPECT_EQ(assignments.at(idx), posting.centroid_idx_);
          }
        }
      }
      if (step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE) {
        const auto &r = *processor.runtime_;
        EXPECT_TRUE(r.initial_member_lists_.empty());
        ASSERT_NE(nullptr, r.a0_);
        EXPECT_EQ(store.n_, r.a0_->count());
        EXPECT_EQ(r.hierarchy_k_ * r.dimension_, r.c0_.count());
      }
      if (step == ObIVFMergeStep::REFINE_UPDATE) {
        auto &r = *processor.runtime_;
        for (int64_t cid = 0; cid < r.changed_cids_->count(); ++cid) {
          const auto &members = *r.refined_member_lists_->lists_.at(cid);
          if (r.changed_cids_->at(cid) == 0 || members.vids_.empty()) { continue; }
          for (int d = 0; d < 2; ++d) {
            double sum = 0; for (int64_t j = 0; j < members.vector_idxs_.count(); ++j) { sum += store.value(members.vector_idxs_.at(j), d); }
            EXPECT_NEAR(sum / members.vids_.count(), r.working_centers_->at(cid * 2 + d), 1e-3);
          }
        }
      }
    }
    ASSERT_EQ(nullptr, plan);
    EXPECT_EQ(1, sample_stages); EXPECT_EQ(1, assign_stages); EXPECT_EQ(1, quant_stages);
    EXPECT_EQ(1, update_stages); // Ensure the fused changed-center mean path was exercised.
    EXPECT_TRUE(injected_failure);
    EXPECT_EQ(!resident, injected_sample_failure);
    if (resident) { EXPECT_GT(refine_vid_reads, 0); }
    else { EXPECT_EQ(0, refine_vid_reads); }
    EXPECT_EQ(store.n_, output.primary_count_); EXPECT_EQ(store.n_, output.raw_count_);
    EXPECT_LE(store.max_batch_bytes_.load(), 16L * 1024 * 1024);
    std::set<uint64_t> primary_vids;
    for (int64_t i = 0; i < output.primary_postings_.count(); ++i) { EXPECT_TRUE(primary_vids.insert(output.primary_postings_.at(i).vid_).second); }
    EXPECT_EQ(store.n_, primary_vids.size()); EXPECT_EQ(1, primary_vids.count(0));
    EXPECT_TRUE(ObIVFMergeTestPeer::are_quant_intermediates_released(processor));
    int soar_snapshots = 0, quant_snapshots = 0;
    for (const auto &perf : observer.snapshots_) {
      if (perf.step_ == ObIVFMergeStep::SOAR_ASSIGN) {
        ++soar_snapshots;
        EXPECT_TRUE(perf.quantize_in_soar_);
        EXPECT_STREQ("SOAR_QUANT", ivf_merge_step_name(perf));
      } else if (perf.step_ == ObIVFMergeStep::QUANT) {
        ++quant_snapshots;
        EXPECT_TRUE(perf.quantize_in_soar_);
        EXPECT_STREQ("QUANT_WRITE", ivf_merge_step_name(perf));
      } else {
        EXPECT_FALSE(perf.quantize_in_soar_);
        if (perf.step_ == ObIVFMergeStep::REFINE_TRAIN) {
          EXPECT_STREQ(perf.refine_phase_ == ObIVFRefinePhase::ASSIGN ? "REFINE_TRAIN_ASSIGN"
              : perf.refine_phase_ == ObIVFRefinePhase::UPDATE ? "REFINE_TRAIN_UPDATE" : "REFINE_TRAIN",
              ivf_merge_step_name(perf));
        } else { EXPECT_STREQ(ivf_merge_step_name(perf.step_), ivf_merge_step_name(perf)); }
      }
    }
    // Names must agree from prepare start through completion, including after
    // the QUANT barrier releases centers, assignments and source routes.
    EXPECT_EQ(4, soar_snapshots);
    EXPECT_EQ(4, quant_snapshots);
  }
  }
}




TEST(TestIVFDDLRuntime, soar_quant_matches_separate_quantization_for_single_and_multiple_centers)
{
  class PerfObserver final : public ObIVFMergePerfObserver
  {
  public:
    void on_step_perf(const ObIVFMergeStepPerf &) override {}
  };
  for (const auto type : {ObIVFProcessType::INITIAL_BUILD, ObIVFProcessType::FULL_REBUILD}) {
  SCOPED_TRACE(static_cast<int>(type));
  for (int mode = 0; mode < 4; ++mode) {
    const bool soar = (mode & 1) != 0;
    const bool single_center = (mode & 2) != 0;
    SCOPED_TRACE(soar);
    SCOPED_TRACE(single_center);
    ObArenaAllocator allocator;
    DDLCountingStore store;
    ObIVFMergeInput input;
    ObIVFDDLHcaAdapter algorithm;
    DDLCountingOutput output(store.n_), reference;
    ObIVFMergeProcessor processor;
    PerfObserver observer;
    ObIVFFullBuildPlanner planner;
    planner.set_perf_observer(&observer);
    share::ObVectorMatrix expected_centers;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ASSERT_EQ(OB_SUCCESS, input.init(store, store, single_center ? store.n_ * 2 : 64,
        false, true, soar, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type));
    ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 4));
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, type));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    int soar_steps = 0, quant_steps = 0;
    for (int guard = 0; plan != nullptr && guard < 200; ++guard) {
      const auto step = planner.get_step();
      auto &runtime = *processor.runtime_;
      const int64_t reads_before = store.source_rows_.load();
      if (step == ObIVFMergeStep::SOAR_ASSIGN) {
        ++soar_steps;
        if (single_center) { ASSERT_EQ(1, runtime.final_cluster_count_); }
        else { ASSERT_GT(runtime.final_cluster_count_, 1); }
        ASSERT_EQ(OB_SUCCESS, expected_centers.assign(*runtime.c1_));
        EXPECT_STREQ("SOAR_QUANT", ivf_merge_step_name(planner.get_step_perf()));
      } else if (step == ObIVFMergeStep::QUANT) {
        ++quant_steps;
        EXPECT_STREQ("QUANT_WRITE", ivf_merge_step_name(planner.get_step_perf()));
      }
      for (int64_t t = plan->get_task_count(); t-- > 0;) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t));
      }
      if (step == ObIVFMergeStep::SOAR_ASSIGN || step == ObIVFMergeStep::QUANT) {
        const bool reads_payload = step == ObIVFMergeStep::SOAR_ASSIGN;
        EXPECT_EQ(reads_payload ? store.n_ : 0, store.source_rows_.load() - reads_before);
      }
      ObArray<int64_t> saved_primary, saved_secondary;
      if (step == ObIVFMergeStep::SOAR_ASSIGN) {
        ASSERT_NO_FATAL_FAILURE(snapshot_soar_assignments(runtime, saved_primary, saved_secondary));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
      plan = next;
      if (step == ObIVFMergeStep::SOAR_ASSIGN) {
        ASSERT_EQ(expected_centers.count(), runtime.c1_->count());
        for (int64_t i = 0; i < expected_centers.count(); ++i) {
          EXPECT_EQ(0, MEMCMP(&expected_centers.at(i), &runtime.c1_->at(i), sizeof(float)));
        }
        EXPECT_EQ(nullptr, runtime.a1_);
        EXPECT_EQ(nullptr, runtime.a2_);
        EXPECT_FALSE(runtime.ddl_runtime_->routes_ready_);
        EXPECT_EQ(store.n_, output.primary_count_);
        // Reproduce the former QUANT input using the test's snapshot. The
        // production dense assignments have already been released.
        share::ObVectorMatrix values;
        const int64_t vid_reads_before = store.vid_rows_.load();
        ASSERT_EQ(OB_SUCCESS, store.batch_get_by_vid(runtime.all_vids_, 0, store.n_, values));
        EXPECT_EQ(store.n_, store.vid_rows_.load() - vid_reads_before);
        ASSERT_EQ(OB_SUCCESS, reference.quantize_loaded_vectors(expected_centers, 2,
            runtime.all_vids_, values, saved_primary, saved_secondary.empty() ? nullptr : &saved_secondary));
      }
    }
    ASSERT_EQ(nullptr, plan);
    EXPECT_EQ(1, soar_steps);
    EXPECT_EQ(1, quant_steps);
    ASSERT_EQ(expected_centers.count(), output.centers_.count());
    for (int64_t i = 0; i < expected_centers.count(); ++i) {
      EXPECT_EQ(0, MEMCMP(&expected_centers.at(i), &output.centers_.at(i), sizeof(float)));
    }
    EXPECT_EQ(store.n_, output.primary_count_);
    EXPECT_EQ(store.n_, output.raw_count_);
    if (single_center || !soar) { EXPECT_EQ(0, output.secondary_count_); }
    ASSERT_EQ(expected_centers.count() / 2, output.leaf_posting_counts_.count());
    std::vector<int64_t> expected_counts(output.leaf_posting_counts_.count(), 0);
    for (int64_t i = 0; i < output.primary_postings_.count(); ++i) {
      ++expected_counts.at(output.primary_postings_.at(i).centroid_idx_);
    }
    for (int64_t i = 0; i < output.secondary_postings_.count(); ++i) {
      ++expected_counts.at(output.secondary_postings_.at(i).centroid_idx_);
    }
    for (int64_t i = 0; i < output.leaf_posting_counts_.count(); ++i) {
      EXPECT_EQ(expected_counts.at(i), output.leaf_posting_counts_.at(i));
    }
    for (const bool secondary : {false, true}) {
      const auto &actual = secondary ? output.secondary_postings_ : output.primary_postings_;
      const auto &expected = secondary ? reference.secondary_postings_ : reference.primary_postings_;
      std::vector<std::pair<uint64_t, int64_t>> actual_rows, expected_rows;
      for (int64_t i = 0; i < actual.count(); ++i) {
        actual_rows.emplace_back(actual.at(i).vid_, actual.at(i).centroid_idx_);
      }
      for (int64_t i = 0; i < expected.count(); ++i) {
        expected_rows.emplace_back(expected.at(i).vid_, expected.at(i).centroid_idx_);
      }
      std::sort(actual_rows.begin(), actual_rows.end());
      std::sort(expected_rows.begin(), expected_rows.end());
      EXPECT_EQ(expected_rows, actual_rows);
    }
  }
  }
}


TEST(TestIVFDDLRuntime, fused_soar_output_failure_and_cancel_do_not_publish_or_append_twice)
{
  for (const auto type : {ObIVFProcessType::INITIAL_BUILD, ObIVFProcessType::FULL_REBUILD}) {
  SCOPED_TRACE(static_cast<int>(type));
  for (int mode = 0; mode < 4; ++mode) {
    const bool cancel = (mode & 1) != 0;
    const bool single_center = (mode & 2) != 0;
    SCOPED_TRACE(cancel);
    SCOPED_TRACE(single_center);
    ObArenaAllocator allocator;
    DDLCountingStore store;
    ObIVFMergeInput input;
    ObIVFDDLHcaAdapter algorithm;
    DDLCountingOutput output(store.n_);
    ObIVFMergeProcessor processor;
    ObIVFFullBuildPlanner planner;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ASSERT_EQ(OB_SUCCESS, input.init(store, store, single_center ? store.n_ * 2 : 64,
        false, true, true, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type));
    ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 4));
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor, type));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    for (int guard = 0; plan != nullptr && planner.get_step() != ObIVFMergeStep::SOAR_ASSIGN && guard < 200;
         ++guard) {
      for (int64_t t = 0; t < plan->get_task_count(); ++t) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
      plan = next;
    }
    ASSERT_NE(nullptr, plan);
    ASSERT_EQ(ObIVFMergeStep::SOAR_ASSIGN, planner.get_step());
    output.output_error_ = cancel ? OB_SUCCESS : OB_IO_ERROR;
    output.cancel_after_output_ = cancel ? &processor : nullptr;
    ObIVFTaskResult result;
    ASSERT_EQ(OB_SUCCESS, result.init(*plan, 0));
    EXPECT_EQ(cancel ? OB_CANCELED : OB_IO_ERROR,
        processor.execute_task(planner.get_stage(), planner.get_step(), planner.get_depth(), 0, result));
    EXPECT_GT(output.primary_count_, 0); // The sink accepted a prefix before failing/canceling.
    EXPECT_FALSE(processor.runtime_->active_plan_->result_slots_.at(0).installed_);
    EXPECT_FALSE(processor.runtime_->soar_finished_);
    EXPECT_FALSE(processor.runtime_->quant_finished_);
    EXPECT_EQ(0, output.raw_count_);
    const int64_t rows = output.primary_count_, reads = store.source_rows_.load();
    output.output_error_ = OB_SUCCESS;
    output.cancel_after_output_ = nullptr;
    EXPECT_EQ(cancel ? OB_STATE_NOT_MATCH : OB_IO_ERROR,
        processor.execute_task(planner.get_stage(), planner.get_step(), planner.get_depth(), 0, result));
    EXPECT_EQ(rows, output.primary_count_);
    EXPECT_EQ(reads, store.source_rows_.load());
    planner.cancel(OB_CANCELED);
  }
  }
}


namespace
{
class LoadedIncrementalOutput : public DDLCountingOutput
{
public:
  int64_t reused_ = 0, encoded_ = 0, loaded_rows_ = 0, prepare_calls_ = 0;
  int drain_mask_ = 0;
  std::set<uint64_t> loaded_vids_;
  bool supports_encoded_posting_reuse() const override { return true; }
  bool supports_loaded_posting_reuse() const override { return true; }
  int prepare_loaded_output(int64_t, int64_t) override { ++prepare_calls_; return OB_SUCCESS; }
  int64_t posting_write_task_count() const override { return 3; }
  int write_posting_range(int64_t task) override
  {
    if (task < 0 || task >= 3 || (drain_mask_ & (1 << task)) != 0) { return OB_STATE_NOT_MATCH; }
    drain_mask_ |= 1 << task;
    return OB_SUCCESS;
  }
  int quantize_loaded_vectors(const share::ObVectorMatrix &centers, int64_t dim,
      const ObIVFVectorIDView &vids, const share::ObVectorMatrix &values,
      const ObIArray<int64_t> &primary, const ObIArray<int64_t> *secondary) override
  {
    for (int64_t i = 0; i < vids.count(); ++i) { loaded_vids_.insert(vids.at(i)); }
    const int64_t before = primary_count_ + secondary_count_;
    const int ret = DDLCountingOutput::quantize_loaded_vectors(centers, dim, vids, values, primary, secondary);
    encoded_ += primary_count_ + secondary_count_ - before;
    loaded_rows_ += vids.count();
    return ret;
  }
  int copy_loaded_postings(const ObIVFEncodedPostingSource &source, int64_t source_cid,
      int64_t target_cid, const ObIVFVectorIDView &expected) override
  {
    class Consumer final : public ObIVFEncodedPostingConsumer
    {
    public:
      Consumer(LoadedIncrementalOutput &output, int64_t cid, const ObIVFVectorIDView &expected)
        : output_(output), cid_(cid), expected_(expected) {}
      int append(ObIVFVectorVID vid, const ObString &bytes) override
      {
        uint64_t code = 0;
        if (count_ >= expected_.count() || expected_.at(count_) != vid || bytes.length() != sizeof(code)) {
          return OB_INVALID_DATA;
        }
        MEMCPY(&code, bytes.ptr(), sizeof(code));
        if (code != vid) { return OB_INVALID_DATA; }
        int ret = output_.primary_postings_.push_back(ObIVFPriorPosting(vid, cid_));
        if (OB_SUCC(ret)) { ++count_; ++output_.primary_count_; ++output_.reused_; }
        return ret == OB_SUCCESS ? output_.output_error_ : ret;
      }
      LoadedIncrementalOutput &output_;
      int64_t cid_;
      ObIVFVectorIDView expected_;
      int64_t count_ = 0;
    } consumer(*this, target_cid, expected);
    int ret = source.scan_postings(source_cid, consumer);
    if (OB_SUCC(ret) && consumer.count_ != expected.count()) { ret = OB_INVALID_DATA; }
    if (OB_SUCC(ret) && cancel_after_output_ != nullptr) { cancel_after_output_->cancel(OB_CANCELED); }
    return ret;
  }
  int finish_quantization(int64_t n, int64_t k, int64_t postings) override
  {
    return primary_count_ >= n && clusters_ == k && postings == primary_count_ + secondary_count_
        && reused_ + encoded_ == postings && prepare_calls_ == 1 && drain_mask_ == 7
        ? OB_SUCCESS : OB_ERR_UNEXPECTED;
  }
};

void make_loaded_incremental_prior(ObIVFMergePrior &prior, DDLCountingStore &store, int64_t old_count,
    bool old_secondary = true)
{
  store.prior_groups_ = 8;
  store.delta_begin_ = old_count;
  prior.dimension_ = 2;
  prior.vector_count_ = store.n_;
  prior.covered_vector_count_ = old_count;
  prior.dominant_segment_ = 0;
  prior.postings_complete_ = true;
  ObIVFSegmentPriorInfo segment;
  segment.provides_prior_ = true;
  segment.centroid_count_ = store.prior_groups_;
  segment.live_vector_count_ = old_count;
  ASSERT_EQ(OB_SUCCESS, prior.segments_.push_back(segment));
  for (int64_t c = 0; c < store.prior_groups_; ++c) {
    ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(c * 100.0F + 3.0F));
    ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(c * 100.0F + 13.0F));
    ASSERT_EQ(OB_SUCCESS, prior.old_cids_.push_back(101 + c));
    ASSERT_EQ(OB_SUCCESS, prior.posting_mass_.push_back(0));
  }
  for (int64_t idx = 0; idx < old_count; ++idx) {
    const int64_t cid = idx % store.prior_groups_;
    ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(store.vid(idx), cid)));
    ++prior.posting_mass_.at(cid);
    if (old_secondary && idx < store.prior_groups_) {
      const int64_t secondary = (cid + 1) % store.prior_groups_;
      ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(store.vid(idx), secondary)));
      ++prior.posting_mass_.at(secondary);
    }
  }
  if (old_count < store.n_) {
    ObIVFSegmentPriorInfo delta;
    delta.centroid_begin_ = store.prior_groups_;
    delta.live_vector_count_ = store.n_ - old_count;
    ASSERT_EQ(OB_SUCCESS, prior.segments_.push_back(delta));
  }
  ASSERT_EQ(OB_SUCCESS, prior.validate());
}
}

namespace
{
struct RootSourceUpdateSnapshot
{
  std::vector<float> centers_;
  std::vector<std::vector<std::pair<uint64_t, int64_t>>> members_;
  int64_t reused_ = 0;
};

void run_root_source_update(ObIVFProcessType type, int64_t delta_count,
    bool source_update, RootSourceUpdateSnapshot &snapshot)
{
  class Algorithm final : public ObIVFDDLHcaAdapter
  {
  public:
    bool source_update_ = true;
    bool supports_source_update() const override { return source_update_; }
  } algorithm;
  algorithm.source_update_ = source_update;
  ObArenaAllocator allocator;
  DDLCountingStore store(16385);
  const int64_t old_count = store.n_ - delta_count;
  ObIVFMergePrior prior(allocator);
  ASSERT_NO_FATAL_FAILURE(make_loaded_incremental_prior(prior, store, old_count, false));
  store.delta_groups_ = 2;
  const bool insertion = type == ObIVFProcessType::INSERTION;
  if (insertion) {
    // The ninth prior center owns only secondary relations. Equal distances
    // keep their working owner at CID 0, so its root member list is empty.
    ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(prior.centroids_.at(0)));
    ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(prior.centroids_.at(1)));
    ASSERT_EQ(OB_SUCCESS, prior.old_cids_.push_back(909));
    ASSERT_EQ(OB_SUCCESS, prior.posting_mass_.push_back(0));
    ++prior.segments_.at(0).centroid_count_;
    if (delta_count > 0) { ++prior.segments_.at(1).centroid_begin_; }
    for (int64_t idx = 0; idx < old_count; idx += store.prior_groups_) {
      ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(store.vid(idx), 8)));
      ++prior.posting_mass_.at(8);
    }
  }
  ASSERT_EQ(OB_SUCCESS, prior.validate());
  PriorCodeSource source(prior);
  prior.posting_source_ = &source;
  ObIVFMergeInput input;
  LoadedIncrementalOutput output;
  ObIVFMergeProcessor processor;
  ObIVFFullBuildPlanner full;
  ObIVFConcatPlanner concat;
  ObIVFInsertionPlanner incremental;
  ObIVFMergePlanner &planner = insertion ? static_cast<ObIVFMergePlanner &>(incremental)
      : type == ObIVFProcessType::CONCATENATION ? static_cast<ObIVFMergePlanner &>(concat)
      : static_cast<ObIVFMergePlanner &>(full);
  ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
  ASSERT_EQ(OB_SUCCESS, input.init(store, store, 4096, false, true, true, VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, type,
      type == ObIVFProcessType::FULL_REBUILD ? nullptr : &prior));
  ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 4));
  ASSERT_EQ(OB_SUCCESS, insertion ? incremental.init(allocator, processor)
      : type == ObIVFProcessType::CONCATENATION ? concat.init(allocator, processor)
      : full.init(allocator, processor, type));
  const ObIVFTaskPlan *plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(plan));
  for (int guard = 0; plan != nullptr && guard < 200; ++guard) {
    if (planner.get_step() == ObIVFMergeStep::HIERARCHY_UPDATE && planner.get_depth() == 0) { break; }
    for (int64_t t = 0; t < plan->get_task_count(); ++t) { ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t)); }
    const ObIVFTaskPlan *next = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
    plan = next;
  }
  ASSERT_NE(nullptr, plan);
  ASSERT_EQ(ObIVFMergeStep::HIERARCHY_UPDATE, planner.get_step());
  ASSERT_EQ(0, planner.get_depth());
  auto &runtime = *processor.runtime_;
  ASSERT_EQ(1, runtime.current_nodes_.count());
  const auto &root = *runtime.current_nodes_.at(0);
  int64_t expected_reads = 0, dirty_leaves = 0;
  std::vector<bool> preserved(root.local_k_, false);
  int64_t minimum_reused = 0;
  for (int64_t cid = 0; cid < root.local_k_; ++cid) {
    const auto &members = *root.member_lists_->lists_.at(cid);
    bool contains_delta = false;
    for (int64_t i = 0; i < members.vector_idxs_.count(); ++i) {
      const int64_t idx = members.vector_idxs_.at(i);
      EXPECT_EQ(store.vid(idx), members.vids_.at(i));
      contains_delta |= idx >= old_count;
    }
    if (insertion && !contains_delta) {
      preserved.at(cid) = true;
      minimum_reused += prior.posting_mass_.at(cid);
    } else if (!members.vids_.empty() && members.vids_.count() <= 4096) {
      expected_reads += members.vids_.count();
      ++dirty_leaves;
    }
  }
  if (!insertion || delta_count == 8) {
    ASSERT_GE(expected_reads, 4096);
    ASSERT_GE(dirty_leaves, 2);
  } else if (delta_count == 0) {
    EXPECT_EQ(0, expected_reads);
  } else {
    EXPECT_GT(expected_reads, 0);
    EXPECT_LT(expected_reads, 4096);
  }
  if (insertion) {
    ASSERT_EQ(9, root.local_k_);
    EXPECT_TRUE(root.member_lists_->lists_.at(8)->vids_.empty());
  }
  if (source_update) {
    ASSERT_NE(nullptr, runtime.active_plan_->source_update_);
    int64_t planned_reads = 0;
    for (int64_t t = 0; t < plan->get_task_count(); ++t) {
      ObIVFMergeWorkPerf perf;
      ASSERT_EQ(OB_SUCCESS, processor.get_work_perf(t, perf));
      EXPECT_EQ(-1, perf.node_idx_);
      EXPECT_LE(perf.input_vectors_, 4096);
      planned_reads += perf.input_vectors_;
    }
    EXPECT_EQ(expected_reads, planned_reads);
    if (expected_reads == 0) {
      EXPECT_EQ(1, plan->get_task_count());
      EXPECT_TRUE(runtime.active_plan_->ddl_->source_nodes_.empty());
    }
  } else {
    EXPECT_EQ(nullptr, runtime.active_plan_->source_update_);
  }
  const int64_t source_before = store.source_rows_.load(), vid_before = store.vid_rows_.load();
  // Reverse windows to exercise a reduction order different from the legacy
  // VID reader while keeping the original per-center member mapping.
  for (int64_t t = plan->get_task_count(); t-- > 0;) { ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t)); }
  EXPECT_EQ(source_update ? expected_reads : 0, store.source_rows_.load() - source_before);
  EXPECT_EQ(source_update ? 0 : expected_reads, store.vid_rows_.load() - vid_before);
  const ObIVFTaskPlan *next = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
  plan = next;
  for (int64_t i = 0; i < runtime.c0_.count(); ++i) { snapshot.centers_.push_back(runtime.c0_.at(i)); }
  for (int64_t cid = 0; cid < runtime.initial_member_lists_.count(); ++cid) {
    const auto &members = *runtime.initial_member_lists_.at(cid);
    std::vector<std::pair<uint64_t, int64_t>> row;
    for (int64_t i = 0; i < members.vids_.count(); ++i) {
      EXPECT_EQ(store.vid(members.vector_idxs_.at(i)), members.vids_.at(i));
      row.emplace_back(members.vids_.at(i), members.vector_idxs_.at(i));
    }
    snapshot.members_.push_back(row);
  }
  if (!insertion) {
    planner.cancel(OB_CANCELED);
    return;
  }
  ASSERT_EQ(18, runtime.c0_.count());
  for (int64_t cid = 0; cid < 9; ++cid) {
    if (preserved.at(cid)) {
      EXPECT_EQ(0, MEMCMP(&prior.centroids_.at(cid * 2), &runtime.c0_.at(cid * 2), 2 * sizeof(float)));
    }
  }
  for (int guard = 0; plan != nullptr && guard < 200; ++guard) {
    for (int64_t t = plan->get_task_count(); t-- > 0;) { ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t)); }
    const ObIVFTaskPlan *following = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, following));
    plan = following;
  }
  ASSERT_EQ(nullptr, plan);
  EXPECT_EQ(store.n_, output.raw_count_);
  EXPECT_EQ(prior.postings_.count() + delta_count, output.primary_count_);
  EXPECT_GE(output.reused_, minimum_reused);
  if (delta_count == 0) { EXPECT_EQ(prior.postings_.count(), output.reused_); }
  snapshot.reused_ = output.reused_;
}
}





namespace
{
struct GlobalInsertionCase
{
  int64_t old_count_, delta_count_, cluster_size_;
  bool soar_, empty_owner_;
  int reuse_mode_; // 0: none, 1: some, 2: every old code.
};

struct GlobalInsertionSnapshot
{
  std::vector<float> centers_;
  std::set<std::pair<uint64_t, int64_t>> primary_, secondary_;
  std::map<uint64_t, std::pair<float, float>> raw_;
  int64_t reused_ = 0;
};

template <typename Base>
class RawRecordingOutput final : public Base
{
public:
  std::map<uint64_t, std::pair<float, float>> raw_;
  int write_raw_vectors(ObIVFVectorReader &reader, const ObIVFVectorIDView &vids,
      int64_t begin, int64_t end, int64_t task) override
  {
    share::ObVectorMatrix values;
    int ret = reader.batch_get_by_vid(vids, begin, end, values);
    if (ret == OB_SUCCESS && values.count() != (end - begin) * 2) { ret = OB_INVALID_DATA; }
    for (int64_t i = begin; ret == OB_SUCCESS && i < end; ++i) {
      if (!raw_.insert({vids.at(i), {values.at((i - begin) * 2), values.at((i - begin) * 2 + 1)}}).second) {
        ret = OB_INVALID_DATA;
      }
    }
    return ret == OB_SUCCESS ? Base::write_raw_vectors(reader, vids, begin, end, task) : ret;
  }
};

void make_global_insertion_prior(ObIVFMergePrior &prior, DDLCountingStore &store,
    const GlobalInsertionCase &test)
{
  ASSERT_NO_FATAL_FAILURE(make_loaded_incremental_prior(prior, store, test.old_count_, false));
  store.delta_groups_ = 2;
  if (test.empty_owner_) {
    ASSERT_TRUE(test.soar_);
    ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(prior.centroids_.at(0)));
    ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(prior.centroids_.at(1)));
    ASSERT_EQ(OB_SUCCESS, prior.old_cids_.push_back(909));
    ASSERT_EQ(OB_SUCCESS, prior.posting_mass_.push_back(0));
    ++prior.segments_.at(0).centroid_count_;
    if (test.delta_count_ > 0) { ++prior.segments_.at(1).centroid_begin_; }
  }
  if (test.soar_) {
    for (int64_t idx = 0; idx < test.old_count_; ++idx) {
      const int64_t primary = idx % store.prior_groups_;
      const int64_t secondary = test.empty_owner_ && idx % 16 == 0
          ? 8 : (primary + 4) % store.prior_groups_;
      ASSERT_EQ(OB_SUCCESS, prior.postings_.push_back(ObIVFPriorPosting(store.vid(idx), secondary)));
      ++prior.posting_mass_.at(secondary);
    }
    // Real source posting streams are ordered by VID. The two old parents of
    // the same vector deliberately live in different former CID task batches.
    lib::ob_sort(share::vector_array_begin(prior.postings_), share::vector_array_end(prior.postings_),
        [](const ObIVFPriorPosting &a, const ObIVFPriorPosting &b) {
          return a.vid_ < b.vid_ || (a.vid_ == b.vid_ && a.centroid_idx_ < b.centroid_idx_);
        });
    int64_t first_four_parent_postings = 0;
    for (int64_t c = 0; c < 4; ++c) { first_four_parent_postings += prior.posting_mass_.at(c); }
    EXPECT_GT(first_four_parent_postings, 4096);
  }
  ASSERT_EQ(OB_SUCCESS, prior.validate());
}

void run_global_insertion_case(const GlobalInsertionCase &test, bool loaded,
    FailingRuntimeAllocator &memory, GlobalInsertionSnapshot &snapshot, int failure_mode = -1)
{
  ObArenaAllocator allocator;
  DDLCountingStore store(test.old_count_ + test.delta_count_);
  ObIVFMergePrior prior(allocator);
  ASSERT_NO_FATAL_FAILURE(make_global_insertion_prior(prior, store, test));
  PriorCodeSource source(prior);
  prior.posting_source_ = &source;
  ObIVFMergeInput input;
  ObIVFDDLHcaAdapter algorithm;
  RawRecordingOutput<LoadedIncrementalOutput> output;
  RawRecordingOutput<IncrementalOutput> reference;
  ObIVFMergeQuantizer &sink = loaded ? static_cast<ObIVFMergeQuantizer &>(output)
      : static_cast<ObIVFMergeQuantizer &>(reference);
  ReuseCountingOutput &rows = loaded ? static_cast<ReuseCountingOutput &>(output)
      : static_cast<ReuseCountingOutput &>(reference);
  ObIVFMergeProcessor processor;
  ObIVFInsertionPlanner planner;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
  ASSERT_EQ(OB_SUCCESS, input.init(store, store, test.cluster_size_, false, true, test.soar_, VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, processor.init(memory, input, algorithm, sink, ObIVFProcessType::INSERTION, &prior));
  ASSERT_NE(nullptr, processor.runtime_->insertion_prior_);
  if (loaded) { ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 4)); }
  ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor));
  const ObIVFTaskPlan *plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(plan));
  int soar_stages = 0, quant_stages = 0;
  for (int guard = 0; plan != nullptr && guard < 200; ++guard) {
    const auto step = planner.get_step();
    const int64_t source_before = store.source_rows_.load(), vid_before = store.vid_rows_.load();
    const int64_t encoded_before = output.encoded_, copied_before = output.reused_;
    if (step == ObIVFMergeStep::SOAR_ASSIGN && loaded) {
      store.start_source_tracking();
      output.loaded_vids_.clear();
      if (failure_mode >= 0) {
        std::vector<int64_t> source_tasks;
        for (int64_t t = 0; t < plan->get_task_count(); ++t) {
          ObIVFMergeWorkPerf perf;
          ASSERT_EQ(OB_SUCCESS, processor.get_work_perf(t, perf));
          if (perf.input_vectors_ > 0) { source_tasks.push_back(t); }
        }
        ASSERT_GE(source_tasks.size(), 3);
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, source_tasks.at(0)));
        const int64_t accepted_before = output.primary_count_ + output.secondary_count_;
        ASSERT_GT(accepted_before, 0);
        output.output_error_ = failure_mode == 0 ? OB_IO_ERROR : OB_SUCCESS;
        output.cancel_after_output_ = failure_mode == 1 ? &processor : nullptr;
        store.read_error_ = failure_mode == 2 ? OB_IO_ERROR : OB_SUCCESS;
        const int64_t failed_task = source_tasks.at(1);
        ObIVFTaskResult result;
        ASSERT_EQ(OB_SUCCESS, result.init(*plan, failed_task));
        EXPECT_EQ(failure_mode == 1 ? OB_CANCELED : OB_IO_ERROR,
            processor.execute_task(planner.get_stage(), step, planner.get_depth(), failed_task, result));
        EXPECT_FALSE(processor.runtime_->active_plan_->result_slots_.at(failed_task).installed_);
        EXPECT_FALSE(processor.runtime_->soar_finished_);
        EXPECT_FALSE(processor.runtime_->quant_finished_);
        EXPECT_EQ(0, output.drain_mask_);
        EXPECT_EQ(0, output.raw_count_);
        const int64_t appended = output.primary_count_ + output.secondary_count_;
        const int64_t source_reads = store.source_rows_.load();
        if (failure_mode != 2) { EXPECT_GT(appended, accepted_before); }
        output.output_error_ = OB_SUCCESS;
        output.cancel_after_output_ = nullptr;
        store.read_error_ = OB_SUCCESS;
        // Neither replaying the failed window nor starting another window may
        // append into a sink which already accepted a failing attempt's prefix.
        for (const int64_t task : {failed_task, source_tasks.at(2)}) {
          ObIVFTaskResult retry;
          ASSERT_EQ(OB_SUCCESS, retry.init(*plan, task));
          EXPECT_EQ(failure_mode == 1 ? OB_STATE_NOT_MATCH : OB_IO_ERROR,
              processor.execute_task(planner.get_stage(), step, planner.get_depth(), task, retry));
        }
        EXPECT_EQ(appended, output.primary_count_ + output.secondary_count_);
        EXPECT_EQ(source_reads, store.source_rows_.load());
        for (auto visits : store.source_visits_) { EXPECT_LE(visits, 1); }
        planner.cancel(OB_CANCELED);
        return;
      }
    }
    int64_t source_tasks = 0, perf_inputs = 0, perf_postings = 0, perf_reused = 0;
    for (int64_t t = plan->get_task_count(); t-- > 0;) {
      const int64_t reads_before_task = store.source_rows_.load();
      const int64_t postings_before_task = output.primary_count_ + output.secondary_count_;
      const int64_t reused_before_task = output.reused_;
      ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t)) << ivf_merge_step_name(step);
      source_tasks += store.source_rows_.load() > reads_before_task;
      if (loaded && step == ObIVFMergeStep::SOAR_ASSIGN) {
        ObIVFMergeWorkPerf actual;
        ASSERT_EQ(OB_SUCCESS, processor.get_work_perf(t, actual));
        EXPECT_EQ(store.source_rows_.load() - reads_before_task, actual.input_vectors_);
        EXPECT_EQ(output.primary_count_ + output.secondary_count_ - postings_before_task, actual.posting_rows_);
        EXPECT_EQ(output.reused_ - reused_before_task, actual.reused_posting_rows_);
        perf_inputs += actual.input_vectors_;
        perf_postings += actual.posting_rows_;
        perf_reused += actual.reused_posting_rows_;
      }
    }
    if (step == ObIVFMergeStep::SOAR_ASSIGN) {
      ++soar_stages;
      if (loaded) {
        EXPECT_EQ(vid_before, store.vid_rows_.load());
        EXPECT_EQ(output.loaded_vids_.size(), store.source_rows_.load() - source_before);
        EXPECT_EQ(output.loaded_vids_.size(), perf_inputs);
        EXPECT_EQ(output.primary_count_ + output.secondary_count_, perf_postings);
        EXPECT_EQ(output.reused_, perf_reused);
        for (int64_t idx = 0; idx < store.n_; ++idx) {
          EXPECT_EQ(output.loaded_vids_.count(store.vid(idx)), store.source_visits_.at(idx)) << idx;
          EXPECT_LE(store.source_visits_.at(idx), 1) << "raw vector reread across SOAR windows: " << idx;
        }
        if (test.reuse_mode_ == 2) {
          EXPECT_EQ(0, source_tasks);
          EXPECT_EQ(0, store.source_rows_.load() - source_before);
        } else {
          EXPECT_GT(source_tasks, 1);
          EXPECT_GT(store.source_rows_.load() - source_before, 4096);
        }
        EXPECT_EQ(prior.postings_.count() + test.delta_count_, output.primary_count_);
        EXPECT_EQ(0, output.drain_mask_);
      }
    } else if (step == ObIVFMergeStep::QUANT) {
      ++quant_stages;
      if (loaded) {
        EXPECT_EQ(source_before, store.source_rows_.load());
        EXPECT_EQ(vid_before, store.vid_rows_.load());
        EXPECT_EQ(encoded_before, output.encoded_);
        EXPECT_EQ(copied_before, output.reused_);
        EXPECT_EQ(7, output.drain_mask_);
      }
    }
    const ObIVFTaskPlan *next = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next)) << ivf_merge_step_name(step);
    plan = next;
    if (step == ObIVFMergeStep::SOAR_ASSIGN && loaded) {
      store.track_source_visits_ = false;
      // Posting output has been materialized. Only the per-CID count shells
      // may survive this barrier; no second complete posting VID list remains.
      EXPECT_EQ(nullptr, processor.runtime_->soar_member_lists_);
      ASSERT_NE(nullptr, processor.runtime_->final_member_lists_);
      const auto &lists = processor.runtime_->final_member_lists_->lists_;
      for (int64_t i = 0; i < lists.count(); ++i) { EXPECT_TRUE(lists.at(i)->vids_.empty()); }
    }
  }
  ASSERT_EQ(nullptr, plan);
  EXPECT_EQ(1, soar_stages);
  EXPECT_EQ(1, quant_stages);
  EXPECT_TRUE(ObIVFMergeTestPeer::are_quant_intermediates_released(processor));
  EXPECT_EQ(store.n_, rows.raw_count_);
  EXPECT_EQ(prior.postings_.count() + test.delta_count_, rows.primary_count_);
  snapshot.reused_ = loaded ? output.reused_ : reference.reused_;
  if (test.cluster_size_ <= 4096) { EXPECT_GT(rows.clusters_, prior.segments_.at(0).centroid_count_); }
  if (test.reuse_mode_ == 0) {
    EXPECT_EQ(0, snapshot.reused_);
    EXPECT_GT(rows.clusters_, store.prior_groups_);
  } else if (test.reuse_mode_ == 1) {
    EXPECT_GT(snapshot.reused_, 0);
    EXPECT_LT(snapshot.reused_, prior.postings_.count());
  } else {
    EXPECT_EQ(prior.postings_.count(), snapshot.reused_);
  }
  std::map<uint64_t, int64_t> copies;
  for (int64_t i = 0; i < rows.primary_postings_.count(); ++i) {
    const auto &p = rows.primary_postings_.at(i);
    EXPECT_TRUE(snapshot.primary_.insert({p.vid_, p.centroid_idx_}).second);
    ++copies[p.vid_];
  }
  for (int64_t idx = 0; idx < store.n_; ++idx) {
    EXPECT_EQ(idx < test.old_count_ && test.soar_ ? 2 : 1, copies[store.vid(idx)]);
  }
  for (int64_t i = 0; i < rows.secondary_postings_.count(); ++i) {
    const auto &p = rows.secondary_postings_.at(i);
    EXPECT_TRUE(snapshot.secondary_.insert({p.vid_, p.centroid_idx_}).second);
    EXPECT_EQ(0, snapshot.primary_.count({p.vid_, p.centroid_idx_}));
    const int64_t idx = p.vid_ == 0 ? 0 : (p.vid_ - (1ULL << 48)) / 3;
    EXPECT_GE(idx, test.old_count_);
  }
  if (!test.soar_) { EXPECT_TRUE(snapshot.secondary_.empty()); }
  for (int64_t i = 0; i < rows.centers_.count(); ++i) { snapshot.centers_.push_back(rows.centers_.at(i)); }
  snapshot.raw_ = loaded ? output.raw_ : reference.raw_;
  EXPECT_EQ(store.n_, snapshot.raw_.size());
  for (int64_t idx = 0; idx < store.n_; ++idx) {
    EXPECT_EQ(std::make_pair(store.value(idx, 0), store.value(idx, 1)), snapshot.raw_.at(store.vid(idx)));
  }
}
}





TEST(TestIVFInsertionMatrix, child_routing_preserves_rows_across_blocks)
{
  class Store final : public ReuseVectorStore
  {
  public:
    explicit Store(int64_t target) : ReuseVectorStore(1), target_(target) {}
    int64_t get_dimension() const override { return 3000; }
    bool supports_source_order() const override { return true; }
    int get_source_row_idx(int64_t idx, int64_t &source) const override
    { source = idx; return idx == 0 ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
    int read(share::ObVectorMatrix &values) const
    {
      values.reuse();
      int ret = values.set_dimension(get_dimension());
      if (OB_SUCC(ret) && OB_SUCC(ret = values.resize(get_dimension()))) { values.at(0) = target_; }
      return ret;
    }
    int batch_get_by_vid(const ObIVFVectorIDView &, int64_t, int64_t,
        share::ObVectorMatrix &values) const override { return read(values); }
    int batch_get_by_source(const share::ObVectorArray<int64_t> &,
        share::ObVectorMatrix &values) const override { return read(values); }
    int64_t target_;
  };
  class Algorithm final : public ObIVFDDLHcaAdapter
  {
  public:
    int insertion_assign_vectors(ObIVFVectorReader &reader, const ObIVFVectorIDView &vids,
        const share::ObVectorMatrix &centers, const ObIArray<int64_t> &offsets,
        const ObIArray<int64_t> &candidates, int64_t begin, int64_t end, ObIArray<int64_t> &assigned) override
    {
      // Check before data_at(), so the regression fails cleanly in assertion builds too.
      const int64_t dim = reader.get_dimension();
      for (int64_t i = 0; i < centers.count(); i += dim) {
        if (centers.contiguous_count(i) < dim) { return OB_ERR_UNEXPECTED; }
      }
      return ObIVFHcaAlgorithmAdapter::insertion_assign_vectors(
          reader, vids, centers, offsets, candidates, begin, end, assigned);
    }
  };
  class Output final : public ReuseCountingOutput
  {
  public:
    explicit Output(bool loaded) : loaded_(loaded) {}
    bool supports_loaded_quantization() const override { return loaded_; }
    bool supports_loaded_posting_reuse() const override { return loaded_; }
    int quantize_loaded_vectors(const share::ObVectorMatrix &, int64_t,
        const ObIVFVectorIDView &, const share::ObVectorMatrix &,
        const ObIArray<int64_t> &primary, const ObIArray<int64_t> *) override
    { return routed_.assign(primary); }
    bool loaded_;
    ObArray<int64_t> routed_;
  };
  for (bool loaded : {false, true}) {
    for (int64_t children : {87, 88}) {
      SCOPED_TRACE(loaded);
      SCOPED_TRACE(children);
      ObArenaAllocator allocator;
      Store store(children - 1);
      Algorithm algorithm;
      Output output(loaded);
      ObIVFMergeInput input;
      ObIVFMergePrior prior(allocator);
      ObIVFMergeProcessor processor;
      ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
      ASSERT_EQ(OB_SUCCESS, input.init(store, store, 384, false, true, true, VIDA_L2));
      ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output));
      auto &r = *processor.runtime_;
      // Current INSERTION freezes leaves. Exercise its retained multi-child routing explicitly.
      r.effective_type_ = ObIVFProcessType::INSERTION;
      r.insertion_prior_ = &prior;
      r.final_cluster_count_ = children + 1;
      ASSERT_EQ(OB_SUCCESS, alloc_runtime_array(allocator, r.c1_));
      ASSERT_EQ(OB_SUCCESS, r.c1_->set_dimension(store.get_dimension()));
      ASSERT_EQ(OB_SUCCESS, r.c1_->resize((children + 1) * store.get_dimension()));
      ASSERT_EQ(OB_SUCCESS, alloc_runtime_array(allocator, r.a0_));
      ASSERT_EQ(OB_SUCCESS, r.a0_->push_back(children));
      ASSERT_EQ(OB_SUCCESS, alloc_runtime_array(allocator, r.old_to_new_));
      for (int64_t cid = 0; cid <= children; ++cid) {
        r.c1_->at(cid * store.get_dimension()) = cid == children ? -1000.0F : static_cast<float>(cid);
        ASSERT_EQ(OB_SUCCESS, r.old_to_new_->push_back(cid));
        ASSERT_EQ(OB_SUCCESS, r.insertion_origins_.push_back(cid == children ? 0 : 1));
        ASSERT_EQ(OB_SUCCESS, r.insertion_reuse_cids_.push_back(-1));
        ASSERT_EQ(OB_SUCCESS, r.insertion_posting_counts_.push_back(0));
      }
      for (int64_t offset : {0L, 1L, children + 1}) {
        ASSERT_EQ(OB_SUCCESS, r.insertion_child_offsets_.push_back(offset));
      }
      ASSERT_EQ(OB_SUCCESS, r.insertion_child_cids_.push_back(children));
      for (int64_t cid = 0; cid < children; ++cid) {
        ASSERT_EQ(OB_SUCCESS, r.insertion_child_cids_.push_back(cid));
      }
      for (int64_t offset : {0, 0, 1}) { ASSERT_EQ(OB_SUCCESS, r.insertion_source_offsets_.push_back(offset)); }
      ASSERT_EQ(OB_SUCCESS, r.insertion_source_idxs_.push_back(0));
      ASSERT_EQ(OB_SUCCESS, r.insertion_offsets_.push_back(0));
      ASSERT_EQ(OB_SUCCESS, r.insertion_offsets_.push_back(2));
      ASSERT_EQ(OB_SUCCESS, r.insertion_candidates_.push_back(0));
      ASSERT_EQ(OB_SUCCESS, r.insertion_candidates_.push_back(1));
      if (loaded) {
        ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 1));
        ASSERT_EQ(OB_SUCCESS, r.ddl_runtime_->prepare_source_routes(r));
      }
      ASSERT_EQ(OB_SUCCESS, alloc_runtime_object(allocator, r.active_plan_, allocator));
      auto &plan = *r.active_plan_;
      plan.plan_id_ = 1;
      plan.stage_ = ObIVFMergeStage::SOAR;
      plan.step_ = ObIVFMergeStep::SOAR_ASSIGN;
      ObIVFMergeWorkItem item;
      item.vid_begin_ = 0; item.vid_end_ = 1; item.cid_begin_ = 0; item.source_input_count_ = 1;
      ASSERT_EQ(OB_SUCCESS, plan.work_items_.push_back(item));
      ASSERT_EQ(OB_SUCCESS, share::vector_array_resize(plan.result_slots_, 1));
      const int ret = processor.execute_post_hierarchy_task(plan.stage_, plan.step_, 1, 0);
      EXPECT_EQ(OB_SUCCESS, ret);
      if (ret == OB_SUCCESS) {
        EXPECT_TRUE(plan.result_slots_.at(0).installed_);
        if (loaded) {
          ASSERT_EQ(2, output.routed_.count());
          EXPECT_EQ(children, output.routed_.at(0));
          EXPECT_EQ(children - 1, output.routed_.at(1));
        } else {
          ASSERT_NE(nullptr, plan.result_slots_.at(0).center_idxs_);
          EXPECT_EQ(children - 1, plan.result_slots_.at(0).center_idxs_->at(0));
        }
      }
    }
  }
}

TEST(TestIVFInsertionMatrix, malformed_center_layout_is_rejected_before_distance)
{
  const int64_t dim = 3000, count = 88;
  SoarArrayStore store(1, dim);
  ObIVFVectorReader reader;
  ObIVFHcaAlgorithmAdapter algorithm;
  share::ObVectorMatrix malformed; // Deliberately retain the default dimension of one.
  ObArray<ObIVFVectorVID> vids;
  ObArray<int64_t> offsets, candidates, assigned;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
  ASSERT_EQ(OB_SUCCESS, malformed.resize(count * dim));
  ASSERT_LT(malformed.contiguous_count(87 * dim), dim);
  ASSERT_EQ(OB_SUCCESS, vids.push_back(17));
  ASSERT_EQ(OB_SUCCESS, offsets.push_back(0));
  ASSERT_EQ(OB_SUCCESS, offsets.push_back(0));
  EXPECT_EQ(OB_INVALID_DATA, algorithm.insertion_assign_vectors(
      reader, vids, malformed, offsets, candidates, 0, 1, assigned));
  ASSERT_EQ(OB_SUCCESS, candidates.push_back(87));
  offsets.at(1) = 1;
  EXPECT_EQ(OB_INVALID_DATA, algorithm.insertion_assign_vectors(
      reader, vids, malformed, offsets, candidates, 0, 1, assigned));
}

TEST(TestIVFDDLRuntime, insertion_preserves_leaf_geometry_relations_codes_and_empty_leaves)
{
  for (const bool loaded : {false, true}) {
  for (const int64_t delta_count : {0L, 8L}) {
    SCOPED_TRACE(loaded);
    SCOPED_TRACE(delta_count);
    ObArenaAllocator allocator;
    const int64_t old_count = 503;
    DDLCountingStore store(old_count + delta_count);
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_loaded_incremental_prior(prior, store, old_count, true));
    // An empty inherited leaf must keep its ordinal and remain routable.
    ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(6.0F));
    ASSERT_EQ(OB_SUCCESS, prior.centroids_.push_back(16.0F));
    ASSERT_EQ(OB_SUCCESS, prior.old_cids_.push_back(109));
    ASSERT_EQ(OB_SUCCESS, prior.posting_mass_.push_back(0));
    ++prior.segments_.at(0).centroid_count_;
    if (delta_count > 0) { ++prior.segments_.at(1).centroid_begin_; }
    ASSERT_EQ(OB_SUCCESS, prior.validate());
    PriorCodeSource source(prior);
    prior.posting_source_ = &source;
    class Output final : public LoadedIncrementalOutput
    {
    public:
      int64_t layout_calls_ = 0;
      int set_centroid_layout(const blocksstable::ObIvfCentroidLayout &layout) override
      {
        ++layout_calls_;
        if (layout.leaf_count() != 9 || layout.dimension() != 2) { return OB_INVALID_DATA; }

        return OB_SUCCESS;
      }
    } output;
    IncrementalOutput legacy_output;
    ObIVFMergeQuantizer &sink = loaded ? static_cast<ObIVFMergeQuantizer &>(output)
        : static_cast<ObIVFMergeQuantizer &>(legacy_output);
    ReuseCountingOutput &rows = loaded ? static_cast<ReuseCountingOutput &>(output)
        : static_cast<ReuseCountingOutput &>(legacy_output);
    ObIVFMergeInput input;
    ObIVFDDLHcaAdapter algorithm;
    ObIVFMergeProcessor processor;
    ObIVFInsertionPlanner planner;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    // Every populated old leaf exceeds this limit. Frozen insertion must not split.
    ASSERT_EQ(OB_SUCCESS, input.init(store, store, 32, false, true, true, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, sink, ObIVFProcessType::INSERTION, &prior));
    ASSERT_NE(nullptr, processor.runtime_->insertion_prior_);
    if (loaded) { ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 4)); }
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    for (int guard = 0; plan != nullptr && guard < 100; ++guard) {
      const auto step = planner.get_step();
      const auto stage = planner.get_stage();
      const int64_t before = store.source_rows_.load() + store.vid_rows_.load();
      EXPECT_NE(ObIVFMergeStage::CENTROID_INDEX, stage);
      EXPECT_NE(ObIVFMergeStep::HIERARCHY_CLUSTER, step);
      EXPECT_NE(ObIVFMergeStep::REFINE_TRAIN, step);
      for (int64_t t = plan->get_task_count(); t-- > 0;) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t)) << ivf_merge_step_name(step);
      }
      const int64_t reads = store.source_rows_.load() + store.vid_rows_.load() - before;
      if (step == ObIVFMergeStep::HIERARCHY_ASSIGN) { EXPECT_EQ(delta_count, reads); }
      if (step == ObIVFMergeStep::HIERARCHY_UPDATE) { EXPECT_EQ(0, reads); }
      if (step == ObIVFMergeStep::SOAR_ASSIGN) { EXPECT_EQ(delta_count, reads); }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next)) << ivf_merge_step_name(step);
      plan = next;
    }
    ASSERT_EQ(nullptr, plan);
    EXPECT_EQ(9, rows.clusters_);
    EXPECT_EQ(prior.postings_.count(), loaded ? output.reused_ : legacy_output.reused_);
    EXPECT_EQ(delta_count + rows.secondary_count_, loaded ? output.encoded_ : legacy_output.encoded_);
    ASSERT_EQ(prior.centroids_.count(), rows.centers_.count());
    for (int64_t i = 0; i < rows.centers_.count(); ++i) {
      EXPECT_EQ(0, MEMCMP(&prior.centroids_.at(i), &rows.centers_.at(i), sizeof(float)));
    }
    std::set<std::pair<uint64_t, int64_t>> actual;
    std::vector<int64_t> counts(9, 0);
    for (const auto &p : rows.primary_postings_) {
      EXPECT_TRUE(actual.insert({p.vid_, p.centroid_idx_}).second);
      ++counts.at(p.centroid_idx_);
    }
    for (const auto &p : rows.secondary_postings_) {
      EXPECT_TRUE(actual.insert({p.vid_, p.centroid_idx_}).second);
      EXPECT_GE(p.vid_, store.vid(old_count));
      ++counts.at(p.centroid_idx_);
    }
    for (int64_t i = 0; i < prior.postings_.count(); ++i) {
      const auto &p = prior.postings_.at(i);
      EXPECT_EQ(1, actual.count({p.vid_, p.centroid_idx_}));
    }
    // Independent small-K fixed-center reference, including the exact-hit
    // residual guard. SOAR chooses its loss minimum, not distance runner-up.
    for (int64_t i = old_count; i < store.n_; ++i) {
      const double x = store.value(i, 0), y = store.value(i, 1);
      double best = DBL_MAX;
      int64_t primary = -1, secondary = -1;
      for (int64_t c = 0; c < 9; ++c) {
        const double dx = x - prior.centroids_.at(c * 2), dy = y - prior.centroids_.at(c * 2 + 1);
        const double distance = dx * dx + dy * dy;
        if (distance < best) { best = distance; primary = c; }
      }
      ASSERT_GE(primary, 0);
      if (best > 1e-16) {
        const double r1x = x - prior.centroids_.at(primary * 2);
        const double r1y = y - prior.centroids_.at(primary * 2 + 1);
        double best_loss = DBL_MAX;
        for (int64_t c = 0; c < 9; ++c) {
          if (c == primary) { continue; }
          const double dx = x - prior.centroids_.at(c * 2), dy = y - prior.centroids_.at(c * 2 + 1);
          const double dot = r1x * dx + r1y * dy;
          const double loss = dx * dx + dy * dy + dot * dot / best;
          if (loss < best_loss) { best_loss = loss; secondary = c; }
        }
      }
      int64_t primary_seen = 0, secondary_seen = 0;
      for (const auto &p : rows.primary_postings_) {
        if (p.vid_ == store.vid(i)) { ++primary_seen; EXPECT_EQ(primary, p.centroid_idx_); }
      }
      for (const auto &p : rows.secondary_postings_) {
        if (p.vid_ == store.vid(i)) { ++secondary_seen; EXPECT_EQ(secondary, p.centroid_idx_); }
      }
      EXPECT_EQ(1, primary_seen);
      EXPECT_EQ(secondary < 0 ? 0 : 1, secondary_seen);
    }

    if (delta_count == 0) { EXPECT_EQ(0, counts.at(8)); }
    else { EXPECT_GT(counts.at(8), 0); } // The formerly empty leaf accepted delta.
    if (loaded) {
      EXPECT_EQ(1, output.layout_calls_);
      ASSERT_EQ(9, output.leaf_posting_counts_.count());
      for (int64_t c = 0; c < 9; ++c) { EXPECT_EQ(counts.at(c), output.leaf_posting_counts_.at(c)); }
    }
    EXPECT_EQ(store.n_, rows.raw_count_);
  }
  }
}







TEST(TestIVFDDLRuntime, insertion_loaded_output_failure_and_cancel_do_not_append_twice)
{
  for (const bool copy_task : {false, true}) {
  SCOPED_TRACE(copy_task);
  for (const bool cancel : {false, true}) {
    SCOPED_TRACE(cancel);
    ObArenaAllocator allocator;
    DDLCountingStore store(511);
    ObIVFMergePrior prior(allocator);
    ASSERT_NO_FATAL_FAILURE(make_loaded_incremental_prior(prior, store, 503));
    PriorCodeSource source(prior);
    prior.posting_source_ = &source;
    ObIVFMergeInput input;
    ObIVFDDLHcaAdapter algorithm;
    LoadedIncrementalOutput output;
    ObIVFMergeProcessor processor;
    ObIVFInsertionPlanner planner;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
    ASSERT_EQ(OB_SUCCESS, input.init(store, store, 128, false, true, true, VIDA_L2));
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator, input, algorithm, output, ObIVFProcessType::INSERTION, &prior));
    ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 4));
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator, processor));
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    for (int guard = 0; plan != nullptr && planner.get_step() != ObIVFMergeStep::SOAR_ASSIGN && guard < 200; ++guard) {
      for (int64_t t = 0; t < plan->get_task_count(); ++t) { ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t)); }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
      plan = next;
    }
    ASSERT_NE(nullptr, plan);
    ASSERT_EQ(ObIVFMergeStep::SOAR_ASSIGN, planner.get_step());
    output.output_error_ = cancel ? OB_SUCCESS : OB_IO_ERROR;
    output.cancel_after_output_ = cancel ? &processor : nullptr;
    ASSERT_GT(plan->get_task_count(), 1);
    int64_t failed_task = -1;
    for (int64_t t = 0; t < plan->get_task_count() && failed_task < 0; ++t) {
      const auto &item = processor.runtime_->active_plan_->work_items_.at(t);
      if (copy_task ? item.vid_begin_ < 0 : item.source_input_count_ > 0) { failed_task = t; }
    }
    ASSERT_GE(failed_task, 0);
    ObIVFTaskResult result;
    ASSERT_EQ(OB_SUCCESS, result.init(*plan, failed_task));
    EXPECT_EQ(cancel ? OB_CANCELED : OB_IO_ERROR,
        processor.execute_task(planner.get_stage(), planner.get_step(), planner.get_depth(), failed_task, result));
    EXPECT_GT(output.primary_count_, 0);
    EXPECT_FALSE(processor.runtime_->active_plan_->result_slots_.at(failed_task).installed_);
    EXPECT_FALSE(processor.runtime_->soar_finished_);
    EXPECT_FALSE(processor.runtime_->quant_finished_);
    EXPECT_EQ(0, output.drain_mask_);
    EXPECT_EQ(0, output.raw_count_);
    const int64_t appended = output.primary_count_ + output.secondary_count_;
    const int64_t reads = store.source_rows_.load();
    output.output_error_ = OB_SUCCESS;
    output.cancel_after_output_ = nullptr;
    EXPECT_EQ(cancel ? OB_STATE_NOT_MATCH : OB_IO_ERROR,
        processor.execute_task(planner.get_stage(), planner.get_step(), planner.get_depth(), failed_task, result));
    EXPECT_EQ(appended, output.primary_count_ + output.secondary_count_);
    EXPECT_EQ(reads, store.source_rows_.load());
    planner.cancel(OB_CANCELED);
  }
  }
}

namespace
{
class NeighborhoodTestAllocator final : public vsag::Allocator
{
public:
  std::string Name() override { return "IVFNeighborhoodTest"; }
  void *Allocate(size_t size) override
  {
    if (size == 0 || size > SIZE_MAX - sizeof(size_t)) { return nullptr; }
    auto *raw = static_cast<size_t *>(std::malloc(size + sizeof(size_t)));
    if (raw == nullptr) { return nullptr; }
    *raw = size;
    bytes_.fetch_add(size);
    return raw + 1;
  }
  void Deallocate(void *p) override
  {
    if (p != nullptr) {
      auto *raw = static_cast<size_t *>(p) - 1;
      bytes_.fetch_sub(*raw);
      std::free(raw);
    }
  }
  void *Reallocate(void *p, size_t size) override
  {
    if (size == 0) { Deallocate(p); return nullptr; }
    if (p == nullptr) { return Allocate(size); }
    const size_t old_size = *(static_cast<size_t *>(p) - 1);
    if (old_size >= size) { return p; }
    void *next = Allocate(size);
    if (next != nullptr) { MEMCPY(next, p, old_size); Deallocate(p); }
    return next;
  }
  std::atomic<int64_t> bytes_{0};
};

void fill_neighborhood_centers(share::ObVectorMatrix &centers, int64_t count, int64_t dim)
{
  ASSERT_EQ(OB_SUCCESS, centers.set_dimension(dim));
  ASSERT_EQ(OB_SUCCESS, centers.resize(count * dim));
  for (int64_t cid = 0; cid < count; ++cid) {
    for (int64_t d = 0; d < dim; ++d) {
      centers.at(cid * dim + d) = static_cast<float>((cid * 37 + d * 17) % 101)
          + static_cast<float>(cid) * 0.001F;
    }
  }
}

// Protect a page outside the requested rows. Copying the whole matrix would
// touch it, so this guards the public adapter path without production hooks.
class UnreadCenterPageGuard
{
public:
  ~UnreadCenterPageGuard()
  {
    if (page_ != nullptr) { EXPECT_EQ(0, mprotect(page_, page_size_, PROT_READ | PROT_WRITE)); }
  }
  int protect(share::ObVectorMatrix &centers, int64_t dim, int64_t begin, int64_t end)
  {
    page_size_ = sysconf(_SC_PAGESIZE);
    if (page_size_ <= 0) { return OB_ERR_UNEXPECTED; }
    const uintptr_t data = reinterpret_cast<uintptr_t>(centers.data_at(0, centers.count()));
    const uintptr_t limit = data + centers.count() * sizeof(float);
    const uintptr_t read_begin = data + begin * dim * sizeof(float);
    const uintptr_t read_end = data + end * dim * sizeof(float);
    for (uintptr_t p = (data + page_size_ - 1) / page_size_ * page_size_;
         p + page_size_ <= limit; p += page_size_) {
      if (p + page_size_ <= read_begin || p >= read_end) {
        if (mprotect(reinterpret_cast<void *>(p), page_size_, PROT_NONE) != 0) { return OB_ERR_UNEXPECTED; }
        page_ = reinterpret_cast<void *>(p);
        return OB_SUCCESS;
      }
    }
    return OB_SIZE_OVERFLOW;
  }
private:
  void *page_ = nullptr;
  int64_t page_size_ = 0;
};

void compare_neighborhood_range(ObIVFDDLHcaAdapter &adapter,
    share::ObVectorMatrix &centers, ObIVFNeighborhoodMode mode,
    int64_t count, int64_t dim, int64_t begin, int64_t end)
{
  ObArray<int64_t> offsets, expected_offsets;
  ObArray<int64_t> expected_ids;
  ObArray<int64_t> ids;
  ObArray<float> distances, expected_distances;

  {
    UnreadCenterPageGuard guard;
    if (mode == ObIVFNeighborhoodMode::HGRAPH) {
      ASSERT_EQ(OB_SUCCESS, guard.protect(centers, dim, begin, end));
    }
    ASSERT_EQ(OB_SUCCESS, adapter.neighborhood_find_neighbors(
        mode, centers, count, dim, begin, end, offsets, ids, distances));
  }
  ASSERT_EQ(OB_SUCCESS, adapter.ObIVFHcaAlgorithmAdapter::neighborhood_find_neighbors(
      mode, centers, count, dim, begin, end, expected_offsets, expected_ids, expected_distances));
  ASSERT_EQ(end - begin + 1, offsets.count());
  ASSERT_EQ(end - begin, distances.count());
  ASSERT_EQ(expected_ids.count(), ids.count());
  for (int64_t i = 0; i < offsets.count(); ++i) { EXPECT_EQ(expected_offsets.at(i), offsets.at(i)); }
  for (int64_t i = 0; i < ids.count(); ++i) { EXPECT_EQ(expected_ids.at(i), ids.at(i)); }
  for (int64_t i = 0; i < distances.count(); ++i) { EXPECT_FLOAT_EQ(expected_distances.at(i), distances.at(i)); }
  for (int64_t row = 0; row < end - begin; ++row) {
    std::set<int64_t> unique;
    EXPECT_EQ(OB_HCA_NEIGHBOR_COUNT, offsets.at(row + 1) - offsets.at(row));
    for (int64_t i = offsets.at(row); i < offsets.at(row + 1); ++i) {
      EXPECT_NE(begin + row, ids.at(i));
      EXPECT_TRUE(unique.insert(ids.at(i)).second);
    }
  }
}
} // namespace

TEST(TestIVFDDLNeighborhood, concurrent_hgraph_add_borrows_centers_and_lookup_matches_merge)
{
  ASSERT_TRUE(common::obvsag::is_init());
  const int64_t count = 512;
  const int64_t dim = MAX(8L, sysconf(_SC_PAGESIZE) / 512);
  share::ObVectorMatrix centers;
  fill_neighborhood_centers(centers, count, dim);
  std::vector<float> original(centers.count());
  centers.copy_to(0, centers.count(), original.data());
  NeighborhoodTestAllocator allocator;
  {
    ObIVFDDLHcaAdapter adapter;
    ObIVFHcaAlgorithmParam parameters;
    ASSERT_EQ(OB_SUCCESS, adapter.init(parameters, &allocator));
    ASSERT_EQ(OB_SUCCESS, adapter.neighborhood_begin(ObIVFNeighborhoodMode::HGRAPH, centers, count, dim));

    {
      UnreadCenterPageGuard guard;
      ASSERT_EQ(OB_SUCCESS, guard.protect(centers, dim, 0, 128));
      ASSERT_EQ(OB_SUCCESS, adapter.neighborhood_add_centers(centers, count, dim, 0, 128));
    }
    std::atomic<int> errors{0};
    std::vector<std::thread> workers;
    for (int64_t t = 1; t < 4; ++t) {
      workers.emplace_back([&, t] {
        if (adapter.neighborhood_add_centers(centers, count, dim, t * 128, (t + 1) * 128) != OB_SUCCESS) { ++errors; }
      });
    }
    for (auto &worker : workers) { worker.join(); }
    ASSERT_EQ(0, errors.load());
    ASSERT_EQ(OB_SUCCESS, adapter.neighborhood_finish_add());
    compare_neighborhood_range(adapter, centers, ObIVFNeighborhoodMode::HGRAPH, count, dim, 37, 41);
    compare_neighborhood_range(adapter, centers, ObIVFNeighborhoodMode::HGRAPH, count, dim, count - 3, count);
    for (int64_t i = 0; i < centers.count(); ++i) {
      EXPECT_FLOAT_EQ(original[i], centers.at(i));
    }
    adapter.cancel(OB_TIMEOUT);
    EXPECT_EQ(OB_TIMEOUT, adapter.neighborhood_add_centers(centers, count, dim, 0, 1));
    ObArray<int64_t> offsets;
    ObArray<int64_t> ids;
    ObArray<float> distances;
    EXPECT_EQ(OB_TIMEOUT, adapter.neighborhood_find_neighbors(
        ObIVFNeighborhoodMode::HGRAPH, centers, count, dim, 0, 1, offsets, ids, distances));
    EXPECT_EQ(0, offsets.count());
  }
  EXPECT_EQ(0, allocator.bytes_.load());
}

TEST(TestIVFDDLNeighborhood, brute_force_range_and_failure_preserve_centers)
{
  const int64_t count = 256;
  const int64_t dim = 8;
  share::ObVectorMatrix centers;
  fill_neighborhood_centers(centers, count, dim);
  std::vector<float> original(centers.count());
  centers.copy_to(0, centers.count(), original.data());
  ObIVFDDLHcaAdapter adapter;
  ObIVFHcaAlgorithmParam parameters;
  ASSERT_EQ(OB_SUCCESS, adapter.init(parameters));
  ASSERT_EQ(OB_SUCCESS, adapter.neighborhood_begin(ObIVFNeighborhoodMode::BRUTE_FORCE, centers, count, dim));
  compare_neighborhood_range(adapter, centers, ObIVFNeighborhoodMode::BRUTE_FORCE, count, dim, 17, 21);
  ObArray<int64_t> offsets;
  ObArray<int64_t> ids;
  ObArray<float> distances;
  EXPECT_EQ(OB_INVALID_ARGUMENT, adapter.neighborhood_find_neighbors(
      ObIVFNeighborhoodMode::BRUTE_FORCE, centers, count, dim, -1, 3, offsets, ids, distances));
  EXPECT_EQ(OB_INVALID_ARGUMENT, adapter.neighborhood_find_neighbors(
      ObIVFNeighborhoodMode::BRUTE_FORCE, centers, count, dim, 0, count + 1, offsets, ids, distances));
  EXPECT_EQ(OB_STATE_NOT_MATCH, adapter.neighborhood_find_neighbors(
      ObIVFNeighborhoodMode::HGRAPH, centers, count, dim, 0, 2, offsets, ids, distances));
  EXPECT_EQ(0, offsets.count()); EXPECT_EQ(0, ids.count()); EXPECT_EQ(0, distances.count());
  for (int64_t i = 0; i < centers.count(); ++i) {
    EXPECT_FLOAT_EQ(original[i], centers.at(i));
  }
  compare_neighborhood_range(adapter, centers, ObIVFNeighborhoodMode::BRUTE_FORCE, count, dim, count - 2, count);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  // Resident RADatumStore/external-sort preparation needs a tenant identity
  // and a macro-block size, but these unit cases do not perform file I/O.
  oceanbase::share::ObTenantBase tenant(oceanbase::common::OB_SYS_TENANT_ID);
  oceanbase::unittest::ResidentTmpFileManager resident_files;
  oceanbase::tmp_file::ObTenantTmpFileManager *file_manager = &resident_files;
  tenant.set(file_manager);
  oceanbase::common::ObTimerService timer_service(oceanbase::common::OB_SYS_TENANT_ID);
  oceanbase::common::ObTimerService *timer_ptr = &timer_service;
  tenant.set(timer_ptr);
  oceanbase::share::ObTenantEnv::set_tenant(&tenant);
  // Monitor cleanup consults the tenant registry for its memory limit. No
  // observer is started: an empty registry keeps the default monitor budget.
  oceanbase::omt::ObMultiTenant registry;
  auto *previous_registry = GCTX.omt_;
  GCTX.omt_ = &registry;
  if (oceanbase::common::OB_SUCCESS != timer_service.start()) { return 1; }
  OB_STORAGE_OBJECT_MGR.super_block_.body_.macro_block_size_ = 2L << 20;
  OB_LOGGER.set_file_name("test_ivf_merge_dag.log", true);
  OB_LOGGER.set_log_level("WARN");
  const int ret = RUN_ALL_TESTS();
  // MemoryDump's thread-group helpers can start the global timer service.
  // Stop it explicitly before static destruction and before destroying tenant.
  auto &global_timer = oceanbase::common::ObTimerService::get_instance();
  global_timer.stop();
  global_timer.wait();
  global_timer.destroy();
  timer_service.stop();
  timer_service.wait();
  timer_service.destroy();
  GCTX.omt_ = previous_registry;
  oceanbase::share::ObTenantEnv::set_tenant(nullptr);
  return ret;
}
