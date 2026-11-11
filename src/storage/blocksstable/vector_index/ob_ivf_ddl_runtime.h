/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_RUNTIME_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_RUNTIME_H_

#include "storage/blocksstable/vector_index/ob_ivf_build_state.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace storage
{
using common::ObArenaAllocator;
using common::ObMemAttr;
using common::Ob2DArray;
using common::ObSpinLock;

struct ObIVFSourceRoute
{
  ObIVFSourceRoute() : vector_idx_(0), node_idx_(-1) {}
  uint32_t vector_idx_;
  int32_t node_idx_;
  TO_STRING_KV(K_(vector_idx), K_(node_idx));
};

// Each worker owns a disjoint interval of the source-sorted sample requests.
struct ObIVFClusterRead
{
  uint32_t source_;
  uint32_t vector_idx_;
  uint32_t node_idx_;
  uint32_t ordinal_;
  bool operator<(const ObIVFClusterRead &other) const { return source_ < other.source_; }
  TO_STRING_KV(K_(source), K_(vector_idx), K_(node_idx), K_(ordinal));
};

struct ObIVFClusterSampleRange
{
  ObIVFClusterSampleRange() : begin_(0), end_(0) {}
  int64_t begin_;
  int64_t end_;
  TO_STRING_KV(K_(begin), K_(end));
};

struct ObIVFClusterSampleShard
{
  ObIVFClusterSampleShard()
    : allocator_(ObMemAttr(MTL_ID(), "IVFClusterStore", common::ObCtxIds::VECTOR_CTX_ID)),
      samples_(allocator_)
  {
    ranges_.set_attr(ObMemAttr(MTL_ID(), "IVFClusterRoute", common::ObCtxIds::VECTOR_CTX_ID));
  }
  void reset()
  {
    samples_.~ObIVFReplayInput();
    allocator_.reset();
    new (&samples_) ObIVFReplayInput(allocator_);
    ranges_.reuse();
  }
  ObArenaAllocator allocator_;
  ObIVFReplayInput samples_;
  ObArray<ObIVFClusterSampleRange> ranges_;
  TO_STRING_KV(K_(ranges));
};

struct ObIVFUpdateLock
{
  ObIVFUpdateLock() : lock_(common::ObLatchIds::OB_IVF_BUILD_HELPER_LOCK) {}
  ObSpinLock lock_;
};

struct ObIVFSourceUpdate
{
  explicit ObIVFSourceUpdate(ObIAllocator &allocator)
    : allocator_(ObMemAttr(MTL_ID(), "IVFUpdateStats", common::ObCtxIds::VECTOR_CTX_ID)),
      items_(), sums_(ObWrapperAllocator(allocator_)), counts_(ObWrapperAllocator(allocator_))
  {
    items_.set_block_allocator(ObWrapperAllocator(allocator));
    results_.set_allocator(&allocator);
  }
  ObArenaAllocator allocator_;
  // Original center order is retained for the existing hierarchy commit.
  ObArray<ObIVFMergeWorkItem, ObWrapperAllocator> items_;
  ObFixedArray<ObIVFMergeResultSlot, ObIAllocator> results_;
  Ob2DArray<float *, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator> sums_;
  Ob2DArray<int64_t, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator> counts_;
  // Protect only in-memory SIMD addition; no read, allocation or yield under
  // these locks. A single sum per leaf avoids workers * centers * dimension.
  static const int64_t LOCK_COUNT = 1024;
  ObIVFUpdateLock locks_[LOCK_COUNT];
};

struct ObIVFDDLPlan
{
  ObIVFDDLPlan()
    : source_allocator_(ObMemAttr(MTL_ID(), "IVFSourcePlan", common::ObCtxIds::VECTOR_CTX_ID)),
      source_routes_(ObWrapperAllocator(source_allocator_)),
      source_cids_(ObWrapperAllocator(source_allocator_))
  {
    cluster_requests_.set_attr(ObMemAttr(MTL_ID(), "IVFClusterRoute", common::ObCtxIds::VECTOR_CTX_ID));
    cluster_shards_.set_attr(ObMemAttr(MTL_ID(), "IVFClusterRoute", common::ObCtxIds::VECTOR_CTX_ID));
  }
  ~ObIVFDDLPlan()
  {
    for (int64_t i = 0; i < cluster_shards_.count(); ++i) {
      if (nullptr != cluster_shards_.at(i)) {
        cluster_shards_.at(i)->~ObIVFClusterSampleShard();
        source_allocator_.free(cluster_shards_.at(i));
      }
    }
  }
  ObArray<ObIVFClusterRead> cluster_requests_;
  ObArray<ObIVFClusterSampleShard *> cluster_shards_;
  ObArenaAllocator source_allocator_;
  // No growing arrays during execution. Workers own disjoint source ranges
  // and global CID entries; finish reads them only after all slots install.
  Ob2DArray<ObIVFSourceRoute, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator> source_routes_;
  Ob2DArray<int32_t, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator> source_cids_;
};

bool needs_cluster_samples(const ObIVFMergeRuntime &runtime,
    const ObIVFMergeAlgorithm &algorithm, const int64_t depth);
int prepare_cluster_samples(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &plan, ObIAllocator &allocator);
int gather_cluster_samples(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan,
    const int64_t task_idx);
int train_cluster_samples(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &samples, const ObIVFMergeWorkItem &item,
    int64_t &local_k, ObIArray<float> &centers);
int prepare_source_assign(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan, ObIAllocator &allocator);
int prepare_source_update(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan,
    ObIAllocator &allocator);
int execute_source_update(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &plan, const int64_t task_idx);
int finish_source_update(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &plan, ObIAllocator &allocator);
int execute_source_assign(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &plan, const int64_t task_idx);
template <typename Visitor>
int visit_source_assignments(const ObIVFMergeRuntime &runtime,
    const ObIVFMergeActivePlan &plan, Visitor visitor)
{
  int ret = OB_SUCCESS;
  // Restore the exact preexisting parent/member order, independent of source
  // order and worker completion order. This also preserves later sampling.
  for (int64_t n = 0; OB_SUCC(ret) && n < runtime.current_nodes_.count(); ++n) {
    const ObIVFHierarchyNodeState *node = runtime.current_nodes_.at(n);
    for (int64_t i = 0; OB_SUCC(ret)
        && i < get_vector_ref_count(*node->vid_list_, runtime.all_vids_); ++i) {
      ObIVFVectorVID vid = 0;
      int64_t idx = -1;
      if (i % 4096 == 0 && is_runtime_canceled(runtime)) {
        ret = OB_CANCELED;
      } else if (i % 4096 == 0 && OB_FAIL(share::dag_yield())) {
        LOG_WARN("IVF source result collection interrupted", K(ret));
      } else if (OB_FAIL(get_vector_ref(*node->vid_list_, runtime.all_vids_, i, vid, idx))) {
        LOG_WARN("invalid IVF source result member", K(ret), K(n), K(i));
      } else if (plan.ddl_->source_cids_.at(idx) < 0 || plan.ddl_->source_cids_.at(idx) >= node->local_k_) {
        ret = OB_INVALID_DATA;
      } else {
        ret = visitor(n, plan.ddl_->source_cids_.at(idx), vid, idx);
      }
    }
  }
  return ret;
}

class ObIVFDDLHcaAdapter;

// Source-ordered execution shared by INITIAL_BUILD and merge. Formal state stays in
// ObIVFMergeRuntime; this object owns only source routing and transient stats.
class ObIVFDDLRuntime
{
public:
  ObIVFDDLRuntime(ObIAllocator &allocator, ObIVFDDLHcaAdapter &algorithm, int64_t parallelism);
  ~ObIVFDDLRuntime();
  int64_t parallelism() const { return parallelism_; }
  static int64_t batch_rows(int64_t dimension);
  static bool quantize_during_soar(const ObIVFMergeRuntime &runtime);
  ObIVFRefinePhase refine_train_phase() const;
  int prepare_refine_phase(int64_t &count);
  int execute_refine_phase(int64_t begin, int64_t end);
  int finish_refine_phase(ObIArray<float> &centers, bool &finished);
  bool handles(const ObIVFMergeRuntime &runtime, ObIVFMergeStep step,
      const ObIVFMergeQuantizer &output) const;
  int prepare(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan,
      ObIVFMergeQuantizer &output);
  int execute(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan,
      int64_t task_idx, ObIVFMergeQuantizer &output);
  int finish_samples();
  int assign_loaded_secondary(const ObIArray<float> &vectors, int64_t dimension,
      const ObIArray<float> &centers, const ObIArray<int64_t> &primary,
      const ObIVFNeighborGraphView *graph, ObIArray<int64_t> &secondary);
  int posting_error() const { return ATOMIC_LOAD(&soar_quant_error_); }
  void fail_postings(int ret) { if (ret != OB_SUCCESS) { (void)ATOMIC_BCAS(&soar_quant_error_, OB_SUCCESS, ret); } }
  int64_t vector_index(int64_t source) const;
  int prepare_source_routes(const ObIVFMergeRuntime &runtime) { return prepare_routes(runtime); }
  void release_stats();
  void release_routes();
private:
  int prepare_routes(const ObIVFMergeRuntime &runtime);
  int prepare_refine_stats(const ObIVFMergeRuntime &runtime);
  int read_batch(const ObIVFMergeRuntime &runtime, int64_t begin, int64_t end,
      ObIArray<int64_t> &indices, ObIArray<float> &vectors, bool load_payload);
  ObIAllocator &allocator_;
  ObIVFDDLHcaAdapter &algorithm_;
  const int64_t parallelism_;
  ObArenaAllocator route_allocator_;
  Ob2DArray<uint32_t, OB_MALLOC_BIG_BLOCK_SIZE, ObWrapperAllocator> source_to_vector_;
  bool routes_ready_;
  ObIVFSourceUpdate *refine_stats_;
  int soar_quant_error_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFDDLRuntime);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_DDL_RUNTIME_H_
