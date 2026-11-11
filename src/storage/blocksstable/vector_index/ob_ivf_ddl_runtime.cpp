/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_ddl_runtime.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_hca_adapter.h"
#include "storage/blocksstable/vector_index/ob_ivf_sorter.h"
#include "storage/ob_parallel_external_sort.h"
#include "lib/utility/ob_sort.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
static const int64_t IVF_DDL_VECTORS_PER_TASK = 4096;
static int64_t ddl_chunk_task_count(int64_t n, int64_t chunk)
{ return n <= 0 || chunk <= 0 ? 0 : (n - 1) / chunk + 1; }

bool ObIVFDDLRuntime::quantize_during_soar(const ObIVFMergeRuntime &runtime)
{
  // Primary postings are encoded here even when secondary assignment is
  // disabled. QUANT must only drain completed shards, never race encoders.
  return runtime.insertion_prior_ == nullptr && runtime.effective_type_ != ObIVFProcessType::CONCATENATION;
}

bool needs_cluster_samples(const ObIVFMergeRuntime &runtime,
    const ObIVFMergeAlgorithm &algorithm, const int64_t depth)
{
  return depth > 0 && runtime.vector_store_->supports_source_order() && !runtime.vector_store_->is_fully_memory_resident()
      && algorithm.supports_prepared_cluster()
      && runtime.all_vids_.count() <= UINT32_MAX && runtime.current_nodes_.count() <= UINT32_MAX;
}

int prepare_cluster_samples(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &plan, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_runtime_object(allocator, plan.ddl_))) {
    LOG_WARN("failed to allocate IVF cluster sample plan", K(ret));
    return ret;
  }
  const int64_t started = ObTimeUtility::current_time();
  ObArray<ObIVFClusterRead> &requests = plan.ddl_->cluster_requests_;
  // Unlike an arena, this allocator releases large ObArray::reuse() buffers.
  ObArray<int64_t> ordinals;
  ordinals.set_attr(ObMemAttr(MTL_ID(), "IVFClusterPrep", ObCtxIds::VECTOR_CTX_ID));
  int64_t sample_count = 0;
  for (int pass = 0; OB_SUCC(ret) && pass < 2; ++pass) {
    if (pass == 1 && OB_FAIL(requests.reserve(sample_count))) {
      LOG_WARN("failed to reserve IVF cluster sample requests", K(ret), K(sample_count));
      break;
    }
    for (int64_t n = 0; OB_SUCC(ret) && n < runtime.current_nodes_.count(); ++n) {
      const ObIVFHierarchyNodeState *node = runtime.current_nodes_.at(n);
      if (is_runtime_canceled(runtime)) {
        ret = OB_CANCELED;
      } else if (OB_FAIL(share::dag_yield())) {
      } else if (OB_ISNULL(node) || OB_ISNULL(node->vid_list_)) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("invalid IVF cluster sample node", K(ret), K(n), KP(node));
      } else if (OB_FAIL(algorithm.hca_collect_training_rows(
          get_vector_ref_count(*node->vid_list_, runtime.all_vids_), runtime.cluster_size_, ordinals))) {
        LOG_WARN("failed to plan IVF cluster samples", K(ret), K(n));
      } else if (pass == 0) {
        if (ordinals.empty() || ordinals.count() > runtime.all_vids_.count() - sample_count) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid IVF cluster sample count", K(ret), K(n), K(sample_count), "ordinals", ordinals.count());
        } else {
          plan.work_items_.at(n).source_input_count_ = ordinals.count();
          sample_count += ordinals.count();
        }
      } else {
        if (ordinals.count() != plan.work_items_.at(n).source_input_count_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("IVF cluster sample plan changed", K(ret), K(n), "ordinals", ordinals.count());
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < ordinals.count(); ++j) {
          ObIVFVectorVID vid = 0;
          int64_t idx = -1, source = -1;
          if (ordinals.at(j) < 0 || ordinals.at(j) > UINT32_MAX
              || (j > 0 && ordinals.at(j - 1) >= ordinals.at(j))) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid IVF cluster sample ordinal", K(ret), K(n), K(j), "ordinal", ordinals.at(j));
          } else if (OB_FAIL(get_vector_ref(*node->vid_list_, runtime.all_vids_, ordinals.at(j), vid, idx))) {
            LOG_WARN("failed to resolve IVF cluster sample member", K(ret), K(n), K(j));
          } else if (OB_FAIL(runtime.vector_store_->get_source_row_idx(idx, source))) {
            LOG_WARN("failed to locate IVF cluster sample source", K(ret), K(n), K(idx));
          } else if (idx < 0 || idx >= runtime.all_vids_.count()
              || source < 0 || source >= runtime.all_vids_.count()) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid IVF cluster sample source", K(ret), K(n), K(idx), K(source));
          } else {
            const ObIVFClusterRead request = {static_cast<uint32_t>(source),
                static_cast<uint32_t>(idx), static_cast<uint32_t>(n),
                static_cast<uint32_t>(ordinals.at(j))};
            if (OB_FAIL(requests.push_back(request))) {
              LOG_WARN("failed to append IVF cluster sample request", K(ret), K(n), K(j), K(source));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) { lib::ob_sort(requests.begin(), requests.end()); }
  // DDL workers or merge capture parallelism bound the shards. Cap concurrent sort
  // and read buffers to 1.5 GiB; a shard has a 32 MiB sorter plus 16 MiB input.
  const int64_t workers = runtime.ddl_runtime_ == nullptr
      ? MAX(1L, runtime.vector_store_->get_source_read_parallelism()) : runtime.ddl_runtime_->parallelism();
  const int64_t buffer_bound = (1536L * 1024 * 1024) / (48L * 1024 * 1024);
  const int64_t shard_count = MIN(MIN(workers, buffer_bound), ddl_chunk_task_count(sample_count,
      MAX(1L, (64L * 1024 * 1024 / sizeof(float)) / runtime.dimension_)));
  if (OB_SUCC(ret) && shard_count <= 0) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid IVF cluster sample shard count", K(ret), K(shard_count), K(sample_count), K(workers));
  }
  if (OB_SUCC(ret)) { plan.work_items_.reuse(); }
  for (int64_t i = 0; OB_SUCC(ret) && i < shard_count; ++i) {
    ObIVFClusterSampleShard *shard = nullptr;
    ObIVFMergeWorkItem item;
    item.vid_begin_ = sample_count * i / shard_count;
    item.vid_end_ = sample_count * (i + 1) / shard_count;
    item.source_input_count_ = item.vid_end_ - item.vid_begin_;
    if (OB_FAIL(alloc_runtime_object(plan.ddl_->source_allocator_, shard))) {
      LOG_WARN("failed to allocate IVF cluster sample shard", K(ret), K(i), K(shard_count));
    } else if (OB_FAIL(plan.ddl_->cluster_shards_.push_back(shard))) {
      LOG_WARN("failed to retain IVF cluster sample shard", K(ret), K(i), K(shard_count));
      free_runtime_object(plan.ddl_->source_allocator_, shard);
    } else if (OB_FAIL(plan.work_items_.push_back(item))) {
      LOG_WARN("failed to retain IVF sample shard", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("planned parallel IVF cluster samples", K(plan.depth_), K(sample_count),
        K(shard_count), "node_count", runtime.current_nodes_.count(),
        "plan_us", ObTimeUtility::current_time() - started,
        "sample_payload_bytes", sample_count * runtime.dimension_ * sizeof(float));
  }
  return ret;
}

int gather_cluster_samples(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  const int64_t started = ObTimeUtility::current_time();
  const int64_t dim = runtime.dimension_;
  const int64_t rows_per_batch = MAX(1L, MIN(IVF_DDL_VECTORS_PER_TASK,
      (16L * 1024 * 1024 / sizeof(float)) / dim));
  const ObIVFMergeWorkItem &work = plan.work_items_.at(task_idx);
  ObIVFClusterSampleShard &shard = *plan.ddl_->cluster_shards_.at(task_idx);
  const ObArray<ObIVFClusterRead> &requests = plan.ddl_->cluster_requests_;
  // Keep capacity across caller/callee reuse(). If a large request exceeds
  // this block size, its allocation is still freed by ObArray's heap allocator.
  ObArray<int64_t> idxs(IVF_DDL_VECTORS_PER_TASK * sizeof(int64_t));
  ObArray<float> values(16L * 1024 * 1024);
  idxs.set_attr(ObMemAttr(MTL_ID(), "IVFClusterPrep", ObCtxIds::VECTOR_CTX_ID));
  values.set_attr(ObMemAttr(MTL_ID(), "IVFClusterPrep", ObCtxIds::VECTOR_CTX_ID));
  ObIVFClusterSampleCompare compare;
  ObExternalSort<ObIVFSortItem, ObIVFClusterSampleCompare> sorter;
  // Borrow only unused bytes in this build's already-reserved quota. The
  // sealed original Replay cannot grow; shards partition this budget once.
  const int64_t sample_memory_limit = MAX(ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT,
      runtime.vector_store_->get_available_replay_memory() / plan.ddl_->cluster_shards_.count());
  shard.reset();
  if (OB_FAIL(shard.ranges_.prepare_allocate(runtime.current_nodes_.count()))) {
    LOG_WARN("failed to allocate IVF sample shard ranges", K(ret), K(task_idx), "nodes",
             runtime.current_nodes_.count());
  } else if (OB_FAIL(sorter.init(32L * 1024 * 1024,
      OB_STORAGE_OBJECT_MGR.get_macro_block_size(), 0, MTL_ID(), &compare))) {
    LOG_WARN("failed to initialize IVF sample shard sort", K(ret), K(task_idx));
  } else if (OB_FAIL(shard.samples_.init(MTL_ID(), dim, sample_memory_limit))) {
    LOG_WARN("failed to initialize IVF sample shard", K(ret), K(task_idx));
  }
  for (int64_t begin = work.vid_begin_; OB_SUCC(ret) && begin < work.vid_end_; begin += rows_per_batch) {
    const int64_t end = MIN(work.vid_end_, begin + rows_per_batch);
    idxs.reuse();
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      if (i > 0 && requests.at(i - 1).source_ >= requests.at(i).source_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("unordered IVF cluster sample source", K(ret), K(task_idx), K(i), "source", requests.at(i).source_);
      } else {
        if (OB_FAIL(idxs.push_back(requests.at(i).vector_idx_))) {
          LOG_WARN("failed to append IVF cluster sample index", K(ret), K(task_idx), K(i));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(share::dag_yield())) {
    } else if (OB_FAIL(runtime.vector_store_->batch_get_by_source(idxs, values))) {
      LOG_WARN("failed to gather IVF sample shard", K(ret), K(task_idx), K(begin));
    } else if (values.count() != idxs.count() * dim) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF sample shard vector count", K(ret), K(task_idx), K(dim), "indices", idxs.count(), "values",
               values.count());
    }
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      ObIVFSortItem row;
      row.row_type_ = 1;
      row.cid_ = requests.at(i).node_idx_;
      row.vid_ = requests.at(i).ordinal_;
      row.payload_.assign_ptr(reinterpret_cast<char *>(&values.at((i - begin) * dim)), dim * sizeof(float));
      if (OB_FAIL(sorter.add_item(row))) {
        LOG_WARN("failed to sort IVF cluster sample row", K(ret), K(task_idx), K(i));
      }
    }
  }
  const int64_t gathered = ObTimeUtility::current_time();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sorter.do_sort(true))) {
      LOG_WARN("failed to finish IVF cluster sample sort", K(ret), K(task_idx));
    }
  }
  const ObIVFSortItem *item = nullptr;
  int64_t previous_node = -1;
  uint64_t previous_ordinal = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sorter.get_next_item(item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to read sorted IVF cluster sample", K(ret), K(task_idx));
      }
    } else if (OB_ISNULL(item) || item->cid_ < 0 || item->cid_ < previous_node
        || item->cid_ >= shard.ranges_.count() || item->vid_ > UINT32_MAX
        || (item->cid_ == previous_node && item->vid_ <= previous_ordinal)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid sorted IVF cluster sample", K(ret), K(task_idx), KP(item), K(previous_node),
               K(previous_ordinal));
    } else {
      const int64_t row = shard.samples_.get_row_count();
      ObIVFClusterSampleRange &range = shard.ranges_.at(item->cid_);
      if (item->cid_ != previous_node) { range.begin_ = row; }
      if (row % rows_per_batch == 0 && is_runtime_canceled(runtime)) {
        ret = OB_CANCELED;
      } else if (row % rows_per_batch == 0 && OB_FAIL(share::dag_yield())) {
      } else if (OB_FAIL(shard.samples_.add_row(item->vid_, item->payload_))) {
        LOG_WARN("failed to append compact IVF cluster sample", K(ret), K(task_idx), K(row));
      } else {
        range.end_ = row + 1;
        previous_node = item->cid_;
        previous_ordinal = item->vid_;
      }
    }
  }
  if (OB_SUCC(ret) && shard.samples_.get_row_count() != work.source_input_count_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("IVF compact sample count mismatch", K(ret), K(task_idx), "actual", shard.samples_.get_row_count(),
             "expected", work.source_input_count_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(shard.samples_.seal())) {
      LOG_WARN("failed to seal IVF compact cluster samples", K(ret), K(task_idx));
    }
  }
  if (OB_SUCC(ret) && is_runtime_canceled(runtime)) { ret = OB_CANCELED; }
  if (OB_SUCC(ret)) {
    plan.result_slots_.at(task_idx).installed_ = true;
    LOG_INFO("prepared IVF compact cluster sample shard", K(plan.depth_), K(task_idx),
        "sample_count", work.source_input_count_, "gather_us", gathered - started,
        "compact_us", ObTimeUtility::current_time() - gathered,
        "sample_mem_hold", shard.samples_.get_mem_hold(),
        "sample_file_bytes", shard.samples_.get_file_size());
  } else {
    // A retry owns the same shard only after this attempt drains.
    shard.reset();
  }
  // ExternalSort requires explicit cleanup even on success; its destructor
  // does not release fragment files. Keep every exit on this path.
  sorter.clean_up();
  return ret;
}

int train_cluster_samples(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &samples, const ObIVFMergeWorkItem &item,
    int64_t &local_k, ObIArray<float> &centers)
{
  int ret = OB_SUCCESS;
  const int64_t dim = runtime.dimension_;
  const int64_t vector_count = get_vector_ref_count(
      *runtime.current_nodes_.at(item.node_idx_)->vid_list_, runtime.all_vids_);
  ObArray<int64_t> ordinals, shard_ordinals;
  ObArray<float> values, shard_values;
  ObArray<uint8_t> seen;
  const ObMemAttr attr(MTL_ID(), "IVFClusterRead", ObCtxIds::VECTOR_CTX_ID);
  ordinals.set_attr(attr); shard_ordinals.set_attr(attr);
  values.set_attr(attr); shard_values.set_attr(attr); seen.set_attr(attr);
  if (OB_FAIL(algorithm.hca_collect_training_rows(vector_count, runtime.cluster_size_, ordinals))) {
    LOG_WARN("failed to collect IVF training sample ordinals", K(ret), K(vector_count), K(item.node_idx_));
  } else if (ordinals.empty() || ordinals.count() > INT64_MAX / dim) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid IVF training sample count", K(ret), K(dim), "ordinals", ordinals.count());
  } else if (OB_FAIL(values.prepare_allocate(ordinals.count() * dim))) {
    LOG_WARN("failed to allocate IVF training sample vectors", K(ret), K(dim), "ordinals", ordinals.count());
  } else if (OB_FAIL(append_filled_byte_array(ordinals.count(), 0, seen))) {
    LOG_WARN("failed to allocate IVF training sample presence flags", K(ret), "ordinals", ordinals.count());
  }
  int64_t copied = 0;
  for (int64_t s = 0; OB_SUCC(ret) && s < samples.ddl_->cluster_shards_.count(); ++s) {
    ObIVFClusterSampleShard &shard = *samples.ddl_->cluster_shards_.at(s);
    const ObIVFClusterSampleRange &range = shard.ranges_.at(item.node_idx_);
    if (is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(share::dag_yield())) {
    } else if (range.begin_ == range.end_) {
    } else if (OB_FAIL(shard.samples_.copy_row_range(range.begin_, range.end_, shard_ordinals, shard_values))) {
      LOG_WARN("failed to read IVF sample shard", K(ret), K(s), K(item.node_idx_));
    } else if (shard_values.count() != shard_ordinals.count() * dim) {
      ret = OB_INVALID_DATA;
      LOG_WARN("IVF training shard payload count mismatch", K(ret), K(s), K(dim), "ordinals", shard_ordinals.count(),
               "values", shard_values.count());
    } else {
      // Shards are disjoint in source order. Restore original node ordinals
      // before calling the unchanged deterministic training kernel.
      for (int64_t i = 0; OB_SUCC(ret) && i < shard_ordinals.count(); ++i) {
        const int64_t *pos = std::lower_bound(ordinals.begin(), ordinals.end(), shard_ordinals.at(i));
        const int64_t idx = pos - ordinals.begin();
        if (idx >= ordinals.count() || ordinals.at(idx) != shard_ordinals.at(i) || seen.at(idx)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("unexpected or duplicate IVF training sample ordinal", K(ret), K(s), K(i), K(idx), "ordinal",
                   shard_ordinals.at(i));
        } else {
          MEMCPY(&values.at(idx * dim), &shard_values.at(i * dim), dim * sizeof(float));
          seen.at(idx) = 1;
          ++copied;
        }
      }
    }
  }
  if (OB_SUCC(ret) && copied != ordinals.count()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("incomplete IVF training samples", K(ret), K(copied), "expected", ordinals.count(), K(item.node_idx_));
  }
  if (OB_SUCC(ret)) {
    // These buffers have been copied/validated. Only values and ordinals are
    // inputs to numerical training, whose own workspace can be substantial.
    shard_values.reset();
    shard_ordinals.reset();
    seen.reset();
    ret = algorithm.hca_train_loaded_samples(vector_count, runtime.cluster_size_, dim,
        ordinals, values, local_k, centers);
  }
  return ret;
}

int prepare_source_assign(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_runtime_object(allocator, plan.ddl_))) {
    LOG_WARN("failed to allocate IVF source assignment plan", K(ret));
    return ret;
  }
  const int64_t count = runtime.all_vids_.count();
  // Bound payload to 16 MiB (or one vector) independently of dimension.
  const int64_t rows_per_task = MAX(1L, MIN(IVF_DDL_VECTORS_PER_TASK,
      (16L * 1024 * 1024 / sizeof(float)) / runtime.dimension_));
  if (OB_FAIL(plan.ddl_->source_routes_.prepare_allocate(count))) {
    LOG_WARN("failed to allocate IVF source routes", K(ret), K(count));
  } else if (OB_FAIL(plan.ddl_->source_cids_.prepare_allocate(count))) {
    LOG_WARN("failed to allocate IVF source assignments", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    plan.ddl_->source_routes_.at(i) = ObIVFSourceRoute();
    plan.ddl_->source_cids_.at(i) = -1;
    if (i % IVF_DDL_VECTORS_PER_TASK == 0) {
      ret = is_runtime_canceled(runtime) ? OB_CANCELED : share::dag_yield();
    }
  }
  for (int64_t n = 0; OB_SUCC(ret) && n < runtime.current_nodes_.count(); ++n) {
    const ObIVFHierarchyNodeState *node = runtime.current_nodes_.at(n);
    if (OB_ISNULL(node) || OB_ISNULL(node->vid_list_) || node->local_k_ <= 0
        || node->local_k_ > 128
        || node->local_centers_.count() != node->local_k_ * runtime.dimension_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid IVF source assignment node", K(ret), K(n), KP(node));
    }
    for (int64_t j = 0; OB_SUCC(ret)
        && j < get_vector_ref_count(*node->vid_list_, runtime.all_vids_); ++j) {
      ObIVFVectorVID vid = 0;
      int64_t idx = -1;
      int64_t source = -1;
      if (j % IVF_DDL_VECTORS_PER_TASK == 0 && is_runtime_canceled(runtime)) {
        ret = OB_CANCELED;
      } else if (j % IVF_DDL_VECTORS_PER_TASK == 0 && OB_FAIL(share::dag_yield())) {
        LOG_WARN("IVF source routing interrupted", K(ret));
      } else if (OB_FAIL(get_vector_ref(*node->vid_list_, runtime.all_vids_, j, vid, idx))) {
        LOG_WARN("invalid IVF source member", K(ret), K(n), K(j));
      } else if (OB_FAIL(runtime.vector_store_->get_source_row_idx(idx, source))) {
        LOG_WARN("failed to locate IVF source member", K(ret), K(idx));
      } else if (source < 0 || source >= count || plan.ddl_->source_routes_.at(source).node_idx_ >= 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid or duplicate IVF source membership", K(ret), K(source), K(idx));
      } else {
        plan.ddl_->source_routes_.at(source).vector_idx_ = static_cast<uint32_t>(idx);
        plan.ddl_->source_routes_.at(source).node_idx_ = static_cast<int32_t>(n);
      }
    }
  }
  for (int64_t begin = 0; OB_SUCC(ret) && begin < count; begin += rows_per_task) {
    ObIVFMergeWorkItem item;
    item.vid_begin_ = begin;
    item.vid_end_ = MIN(count, begin + rows_per_task);
    for (int64_t i = begin; i < item.vid_end_; ++i) {
      item.source_input_count_ += plan.ddl_->source_routes_.at(i).node_idx_ >= 0 ? 1 : 0;
    }
    if (is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(share::dag_yield())) {
      LOG_WARN("IVF source planning interrupted", K(ret));
    } else if (item.source_input_count_ > 0 && OB_FAIL(plan.work_items_.push_back(item))) {
      LOG_WARN("failed to retain IVF source work item", K(ret), K(begin));
    }
  }
  if (OB_SUCC(ret)) {
    plan.source_assign_ = true;
    LOG_INFO("prepared IVF source-order assign", K(plan.depth_), K(count),
        K(rows_per_task), "task_count", plan.work_items_.count());
  }
  return ret;
}

struct ObIVFSourcePositionLess
{
  explicit ObIVFSourcePositionLess(const ObIArray<int64_t> &nodes) : nodes_(nodes) {}
  bool operator()(int64_t a, int64_t b) const
  { return nodes_.at(a) == nodes_.at(b) ? a < b : nodes_.at(a) < nodes_.at(b); }
  const ObIArray<int64_t> &nodes_;
};

enum class ObIVFSourceUpdateMode { SKIP, COPY, STATS };

static int classify_source_update(const ObIVFMergeRuntime &runtime, const ObIVFMergeActivePlan &plan,
    const ObIVFMergeWorkItem &item, ObIVFSourceUpdateMode &mode)
{
  int ret = OB_SUCCESS;
  const bool inherited_root = runtime.insertion_prior_ != nullptr && plan.depth_ == 0;
  mode = ObIVFSourceUpdateMode::STATS;
  if (item.node_idx_ < 0 || item.node_idx_ >= runtime.current_nodes_.count()
      || runtime.current_nodes_.at(item.node_idx_) == nullptr) {
    ret = OB_INVALID_DATA;
  } else {
    const auto &node = *runtime.current_nodes_.at(item.node_idx_);
    if (node.member_lists_ == nullptr || node.vid_list_ == nullptr || item.local_center_idx_ < 0
        || item.local_center_idx_ >= node.member_lists_->lists_.count()
        || node.member_lists_->lists_.at(item.local_center_idx_) == nullptr) {
      ret = OB_INVALID_DATA;
    } else {
      const auto &members = *node.member_lists_->lists_.at(item.local_center_idx_);
      const int64_t count = members.vids_.count();
      const int64_t parent = get_vector_ref_count(*node.vid_list_, runtime.all_vids_);
      if (count < 0 || (count == 0 && !inherited_root) || count > parent) {
        ret = OB_INVALID_DATA;
      } else if (inherited_root) {
        mode = ObIVFSourceUpdateMode::COPY;
      } else if (can_skip_hierarchy_center_stats(count, parent, runtime.cluster_size_)) {
        mode = ObIVFSourceUpdateMode::SKIP;
      }
    }
  }
  if (OB_FAIL(ret)) { LOG_WARN("invalid IVF source update classification", K(ret), K(item), K(inherited_root)); }
  return ret;
}

int prepare_source_update(ObIVFMergeRuntime &runtime, ObIVFMergeActivePlan &plan,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_runtime_object(allocator, plan.ddl_))) {
    LOG_WARN("failed to allocate IVF source update plan", K(ret));
    return ret;
  }
  int64_t input_count = 0;
  int64_t leaf_count = 0;
  const int64_t count = runtime.all_vids_.count();
  const int64_t dim = runtime.dimension_;
  const bool inherited_root = runtime.insertion_prior_ != nullptr && plan.depth_ == 0;
  ObArray<ObIVFSourceUpdateMode> modes;
  modes.set_attr(ObMemAttr(MTL_ID(), "IVFUpdatePlan", ObCtxIds::VECTOR_CTX_ID));
  if (OB_FAIL(modes.prepare_allocate(plan.work_items_.count()))) {
    LOG_WARN("failed to allocate IVF source update classifications", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < plan.work_items_.count(); ++i) {
    const auto &item = plan.work_items_.at(i);
    if (i % 64 == 0 && is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else if (i % 64 == 0 && OB_FAIL(share::dag_yield())) {
    } else if (OB_FAIL(classify_source_update(runtime, plan, item, modes.at(i)))) {
    } else if (modes.at(i) == ObIVFSourceUpdateMode::STATS) {
      const auto &node = *runtime.current_nodes_.at(item.node_idx_);
      const int64_t members = node.member_lists_->lists_.at(item.local_center_idx_)->vids_.count();
      if (members > count - input_count) {
        ret = OB_INVALID_DATA;
        LOG_WARN("IVF source update member count overflow", K(ret), K(i), K(members), K(count), K(input_count));
      } else {
        input_count += members;
        ++leaf_count;
      }
    }
  }
  // Keep tiny updates and a single no-progress leaf on the existing path.
  // In particular, early hierarchy levels need no O(N) routing allocation.
  if (OB_FAIL(ret) || (!inherited_root && (input_count < IVF_DDL_VECTORS_PER_TASK || leaf_count < 2))) {
    return ret;
  }
  const int64_t rows_per_task = MAX(1L, MIN(IVF_DDL_VECTORS_PER_TASK,
      (16L * 1024 * 1024 / sizeof(float)) / dim));
  if (OB_FAIL(alloc_runtime_object(allocator, plan.source_update_, allocator))) {
    LOG_WARN("failed to allocate IVF source update", K(ret));
  } else if (OB_FAIL(plan.source_update_->items_.assign(plan.work_items_))) {
    LOG_WARN("failed to copy IVF source update work items", K(ret));
  } else if (OB_FAIL(plan.source_update_->results_.prepare_allocate(plan.work_items_.count()))) {
    LOG_WARN("failed to allocate IVF source update results", K(ret), "count", plan.work_items_.count());
  } else if (OB_FAIL(plan.source_update_->sums_.prepare_allocate(plan.work_items_.count()))) {
    LOG_WARN("failed to allocate IVF source update sum pointers", K(ret), "count", plan.work_items_.count());
  } else if (OB_FAIL(plan.source_update_->counts_.prepare_allocate(plan.work_items_.count()))) {
    LOG_WARN("failed to allocate IVF source update counts", K(ret), "count", plan.work_items_.count());
  } else if (input_count > 0 && OB_FAIL(plan.ddl_->source_routes_.prepare_allocate(count))) {
    LOG_WARN("failed to allocate IVF update routes", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < plan.ddl_->source_routes_.count(); ++i) {
    plan.ddl_->source_routes_.at(i) = ObIVFSourceRoute();
    if (i % IVF_DDL_VECTORS_PER_TASK == 0) {
      ret = is_runtime_canceled(runtime) ? OB_CANCELED : share::dag_yield();
    }
  }
  for (int64_t cid = 0; OB_SUCC(ret) && cid < plan.work_items_.count(); ++cid) {
    auto &update = *plan.source_update_;
    const auto &item = update.items_.at(cid);
    const auto &node = *runtime.current_nodes_.at(item.node_idx_);
    const auto &members = *node.member_lists_->lists_.at(item.local_center_idx_);
    update.sums_.at(cid) = nullptr;
    update.counts_.at(cid) = 0;
    if (cid % 64 == 0 && is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else if (cid % 64 == 0 && OB_FAIL(share::dag_yield())) {
    } else if (modes.at(cid) == ObIVFSourceUpdateMode::SKIP) {
      update.results_.at(cid).installed_ = true;
    } else if (modes.at(cid) == ObIVFSourceUpdateMode::COPY) {
      auto &slot = update.results_.at(cid);
      if (dim <= 0 || node.local_centers_.count() % dim != 0
          || item.local_center_idx_ >= node.local_centers_.count() / dim) {
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(alloc_runtime_array(allocator, slot.center_values_))) {
        LOG_WARN("failed to allocate inherited source update center", K(ret), K(cid));
      } else if (OB_FAIL(slot.center_values_->prepare_allocate(dim))) {
        LOG_WARN("failed to allocate inherited source center values", K(ret), K(cid));
      } else {
        MEMCPY(&slot.center_values_->at(0), &node.local_centers_.at(item.local_center_idx_ * dim),
            dim * sizeof(float));
        if (!are_finite_center_values(*slot.center_values_)) {
          ret = OB_INVALID_DATA;
        } else { slot.installed_ = true; }
      }
    } else if (nullptr == (update.sums_.at(cid) =
        static_cast<float *>(update.allocator_.alloc(dim * sizeof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF source update sums", K(ret), K(cid), K(dim));
    } else {
      MEMSET(update.sums_.at(cid), 0, dim * sizeof(float));
      for (int64_t j = 0; OB_SUCC(ret) && j < members.vids_.count(); ++j) {
        ObIVFVectorVID vid = 0;
        int64_t idx = -1, source = -1;
        if (j % IVF_DDL_VECTORS_PER_TASK == 0 && is_runtime_canceled(runtime)) {
          ret = OB_CANCELED;
        } else if (j % IVF_DDL_VECTORS_PER_TASK == 0 && OB_FAIL(share::dag_yield())) {
        } else if (OB_FAIL(get_vector_ref(members, runtime.all_vids_, j, vid, idx))) {
          LOG_WARN("failed to resolve IVF source update member", K(ret), K(cid), K(j));
        } else if (OB_FAIL(runtime.vector_store_->get_source_row_idx(idx, source))) {
          LOG_WARN("failed to locate IVF source update member", K(ret), K(cid), K(idx));
        } else if (source < 0 || source >= count || plan.ddl_->source_routes_.at(source).node_idx_ >= 0) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid or duplicate IVF source update route", K(ret), K(cid), K(idx), K(source), K(count));
        } else {
          plan.ddl_->source_routes_.at(source).vector_idx_ = static_cast<uint32_t>(idx);
          plan.ddl_->source_routes_.at(source).node_idx_ = static_cast<int32_t>(cid);
        }
      }
    }
  }
  if (OB_SUCC(ret)) { plan.work_items_.reuse(); }
  for (int64_t begin = 0; OB_SUCC(ret) && input_count > 0 && begin < count; begin += rows_per_task) {
    ObIVFMergeWorkItem item;
    item.vid_begin_ = begin;
    item.vid_end_ = MIN(count, begin + rows_per_task);
    for (int64_t i = begin; i < item.vid_end_; ++i) {
      item.source_input_count_ += plan.ddl_->source_routes_.at(i).node_idx_ >= 0 ? 1 : 0;
    }
    if (is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(share::dag_yield())) {
    } else if (item.source_input_count_ > 0) {
      if (OB_FAIL(plan.work_items_.push_back(item))) {
        LOG_WARN("failed to append IVF DDL work item", K(ret), K(item.vid_begin_), K(item.vid_end_));
      }
    }
  }
  if (OB_SUCC(ret) && plan.work_items_.empty() && inherited_root && input_count == 0) {
    // Thousands of unchanged/empty root centers need only one DAG barrier;
    // neither source routing nor payload readers are needed in this case.
    ObIVFMergeWorkItem item;
    item.vid_begin_ = item.vid_end_ = 0;
    item.source_input_count_ = 0;
    ret = plan.work_items_.push_back(item);
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("prepared IVF source-order update", K(plan.depth_), K(input_count),
        K(leaf_count), K(rows_per_task), "task_count", plan.work_items_.count(),
        "sum_payload_bytes", leaf_count * dim * sizeof(float),
        "stats_mem_hold", plan.source_update_->allocator_.total());
  }
  return ret;
}

int execute_source_update(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &plan, const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  auto &update = *plan.source_update_;
  const auto &item = plan.work_items_.at(task_idx);
  const int64_t dim = runtime.dimension_;
  if (item.source_input_count_ == 0) {
    if (runtime.insertion_prior_ == nullptr || plan.depth_ != 0 || plan.ddl_ == nullptr
        || !plan.ddl_->source_routes_.empty() || plan.work_items_.count() != 1
        || item.vid_begin_ != 0 || item.vid_end_ != 0 || plan.result_slots_.at(task_idx).installed_) {
      ret = OB_INVALID_DATA;
    } else if (is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else { plan.result_slots_.at(task_idx).installed_ = true; }
    return ret;
  }
  const ObMemAttr attr(MTL_ID(), "IVFUpdateRead", ObCtxIds::VECTOR_CTX_ID);
  ObArray<int64_t> idxs, cids, positions;
  ObArray<float> vectors;
  idxs.set_attr(attr); cids.set_attr(attr); positions.set_attr(attr); vectors.set_attr(attr);
  for (int64_t i = item.vid_begin_; OB_SUCC(ret) && i < item.vid_end_; ++i) {
    const auto &route = plan.ddl_->source_routes_.at(i);
    if (route.node_idx_ < 0) {
    } else if (route.node_idx_ >= update.sums_.count()
        || nullptr == update.sums_.at(route.node_idx_)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF source update sum route", K(ret), K(task_idx), K(i), K(route.node_idx_));
    } else if (OB_FAIL(idxs.push_back(route.vector_idx_))) {
      LOG_WARN("failed to retain IVF source update vector index", K(ret), K(task_idx), K(i));
    } else if (OB_FAIL(cids.push_back(route.node_idx_))) {
      LOG_WARN("failed to retain IVF source update center index", K(ret), K(task_idx), K(i));
    } else {
      if (OB_FAIL(positions.push_back(positions.count()))) {
        LOG_WARN("failed to retain IVF source batch position", K(ret), K(task_idx), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (idxs.empty() || idxs.count() != item.source_input_count_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("IVF source update batch count mismatch", K(ret), K(task_idx), "actual", idxs.count(), "expected",
             item.source_input_count_);
  } else if (is_runtime_canceled(runtime)) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(share::dag_yield())) {
  } else if (OB_FAIL(runtime.vector_store_->batch_get_by_source(idxs, vectors))) {
    LOG_WARN("failed to read IVF source update", K(ret), K(task_idx));
  } else if (vectors.count() != idxs.count() * dim || !are_finite_center_values(vectors)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid IVF source update vectors", K(ret), K(task_idx), K(dim), "indices", idxs.count(), "values",
             vectors.count());
  }
  if (OB_SUCC(ret)) {
    ObIVFSourcePositionLess less(cids);
    lib::ob_sort(positions.begin(), positions.end(), less);
    if (is_runtime_canceled(runtime)) { ret = OB_CANCELED; }
  }
  if (OB_SUCC(ret)) {
    // All fallible work precedes publication. Do not allocate, yield, or check
    // cancellation inside this bounded commit: retries must never double-add.
    // Unlike the old per-CID loop, floating-point addition follows batch
    // completion order. Membership, SIMD kernel and normalization are unchanged.
    for (int64_t begin = 0; begin < positions.count();) {
      const int64_t cid = cids.at(positions.at(begin));
      int64_t end = begin + 1;
      while (end < positions.count() && cids.at(positions.at(end)) == cid) { ++end; }
      {
        ObSpinLockGuard guard(update.locks_[cid % ObIVFSourceUpdate::LOCK_COUNT].lock_);
        for (int64_t i = begin; i < end; ++i) {
          algorithm.hca_accumulate_center_stats(update.sums_.at(cid),
              &vectors.at(positions.at(i) * dim), dim);
        }
        update.counts_.at(cid) += end - begin;
      }
      begin = end;
    }
    plan.result_slots_.at(task_idx).installed_ = true;
  }
  return ret;
}

int finish_source_update(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &plan, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  auto &update = *plan.source_update_;
  for (int64_t cid = 0; OB_SUCC(ret) && cid < update.items_.count(); ++cid) {
    auto &slot = update.results_.at(cid);
    const auto &item = update.items_.at(cid);
    const auto &node = *runtime.current_nodes_.at(item.node_idx_);
    const int64_t expected = node.member_lists_->lists_.at(item.local_center_idx_)->vids_.count();
    ObArray<float, ObWrapperAllocator> *center = nullptr;
    ObIVFSourceUpdateMode mode;
    if (cid % 64 == 0 && is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else if (cid % 64 == 0 && OB_FAIL(share::dag_yield())) {
    } else if (OB_FAIL(classify_source_update(runtime, plan, item, mode))) {
    } else if (mode == ObIVFSourceUpdateMode::SKIP) {
      if (!slot.installed_ || slot.center_values_ != nullptr || update.sums_.at(cid) != nullptr
          || update.counts_.at(cid) != 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid skipped IVF source update result", K(ret), K(cid));
      }
    } else if (mode == ObIVFSourceUpdateMode::COPY) {
      if (!slot.installed_ || slot.center_values_ == nullptr || update.sums_.at(cid) != nullptr
          || update.counts_.at(cid) != 0 || runtime.dimension_ <= 0
          || slot.center_values_->count() != runtime.dimension_
          || !are_finite_center_values(*slot.center_values_)
          || node.local_centers_.count() % runtime.dimension_ != 0
          || item.local_center_idx_ >= node.local_centers_.count() / runtime.dimension_
          || MEMCMP(&slot.center_values_->at(0),
              &node.local_centers_.at(item.local_center_idx_ * runtime.dimension_),
              runtime.dimension_ * sizeof(float)) != 0) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid copied IVF source update center", K(ret), K(cid));
      }
    } else if (expected <= 0 || nullptr == update.sums_.at(cid) || update.counts_.at(cid) != expected) {
      ret = OB_INVALID_DATA;
      LOG_WARN("IVF source update statistics mismatch", K(ret), K(cid), K(expected), "actual", update.counts_.at(cid));
    } else if (slot.installed_) {
      if (slot.center_values_ == nullptr || slot.center_values_->count() != runtime.dimension_
          || !are_finite_center_values(*slot.center_values_)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid completed IVF source update center", K(ret), K(cid));
      }
    } else if (slot.center_values_ != nullptr) {
      ret = OB_INVALID_DATA;
      LOG_WARN("uninstalled IVF source update retains a center", K(ret), K(cid));
    } else if (OB_FAIL(alloc_runtime_array(allocator, center))) {
      LOG_WARN("failed to allocate IVF source update center", K(ret), K(cid));
    } else if (OB_FAIL(center->reserve(runtime.dimension_))) {
      LOG_WARN("failed to reserve IVF source update center values", K(ret), K(cid), K(runtime.dimension_));
    } else if (OB_FAIL(algorithm.hca_finalize_center_stats(update.sums_.at(cid), expected,
        runtime.dimension_, *center))) {
      LOG_WARN("failed to finalize IVF source update", K(ret), K(cid));
    } else if (center->count() != runtime.dimension_ || !are_finite_center_values(*center)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF finalized source center", K(ret), K(cid), K(runtime.dimension_), "values", center->count());
    } else {
      slot.center_values_ = center;
      slot.installed_ = true;
      center = nullptr;
    }
    free_runtime_object(allocator, center);
  }
  return ret;
}

int execute_source_assign(ObIVFMergeRuntime &runtime, ObIVFMergeAlgorithm &algorithm,
    ObIVFMergeActivePlan &plan, const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  const ObIVFMergeWorkItem &item = plan.work_items_.at(task_idx);
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFSourceAssign", ObCtxIds::VECTOR_CTX_ID));
  ObArray<int64_t, ObWrapperAllocator> idxs;
  ObArray<int64_t, ObWrapperAllocator> nodes;
  ObArray<int64_t, ObWrapperAllocator> positions;
  ObArray<int64_t, ObWrapperAllocator> cids;
  ObArray<float, ObWrapperAllocator> vectors;
  idxs.set_block_allocator(ObWrapperAllocator(allocator));
  nodes.set_block_allocator(ObWrapperAllocator(allocator));
  positions.set_block_allocator(ObWrapperAllocator(allocator));
  cids.set_block_allocator(ObWrapperAllocator(allocator));
  vectors.set_block_allocator(ObWrapperAllocator(allocator));
  for (int64_t i = item.vid_begin_; OB_SUCC(ret) && i < item.vid_end_; ++i) {
    const ObIVFSourceRoute &route = plan.ddl_->source_routes_.at(i);
    if (route.node_idx_ < 0) {
      // This row belongs to a leaf completed at an earlier depth.
    } else if (OB_FAIL(idxs.push_back(route.vector_idx_))) {
    } else if (OB_FAIL(nodes.push_back(route.node_idx_))) {
    } else if (OB_FAIL(positions.push_back(positions.count()))) {
    } else {
      if (OB_FAIL(cids.push_back(-1))) {
        LOG_WARN("failed to initialize IVF source assignment CID", K(ret), K(task_idx), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to prepare IVF source batch", K(ret));
  } else if (idxs.count() != item.source_input_count_ || idxs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF source assignment batch count mismatch", K(ret), K(task_idx), "actual", idxs.count(), "expected",
             item.source_input_count_);
  } else if (OB_FAIL(runtime.vector_store_->batch_get_by_source(idxs, vectors))) {
    LOG_WARN("failed to read IVF source batch", K(ret), K(task_idx));
  } else if (vectors.count() != idxs.count() * runtime.dimension_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid IVF source assignment vector count", K(ret), K(task_idx), "indices", idxs.count(), "values",
             vectors.count());
  } else {
    ObIVFSourcePositionLess less(nodes);
    lib::ob_sort(positions.begin(), positions.end(), less);
  }
  for (int64_t begin = 0; OB_SUCC(ret) && begin < positions.count();) {
    const int64_t node_idx = nodes.at(positions.at(begin));
    int64_t end = begin + 1;
    while (end < positions.count() && nodes.at(positions.at(end)) == node_idx) { ++end; }
    const ObIVFHierarchyNodeState *node = runtime.current_nodes_.at(node_idx);
    if (is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(algorithm.hca_assign_loaded_vectors(vectors, positions, begin, end,
        runtime.dimension_, node->local_centers_, node->local_k_, cids))) {
      LOG_WARN("failed to assign IVF source group", K(ret), K(task_idx), K(node_idx));
    }
    begin = end;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < idxs.count(); ++i) {
    if (cids.at(i) < 0 || cids.at(i) >= runtime.current_nodes_.at(nodes.at(i))->local_k_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF source assignment CID", K(ret), K(task_idx), K(i), "cid", cids.at(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_runtime_canceled(runtime)) {
      ret = OB_CANCELED;
    } else {
      // Each global index has one source owner. A failed attempt publishes no
      // slot, and the barrier cannot consume partial assignments.
      for (int64_t i = 0; i < idxs.count(); ++i) {
        plan.ddl_->source_cids_.at(idxs.at(i)) = static_cast<int32_t>(cids.at(i));
      }
      plan.result_slots_.at(task_idx).installed_ = true;
    }
  }
  return ret;
}


ObIVFDDLRuntime::ObIVFDDLRuntime(ObIAllocator &allocator,
    ObIVFDDLHcaAdapter &algorithm, int64_t parallelism)
  : allocator_(allocator), algorithm_(algorithm), parallelism_(parallelism),
    route_allocator_(ObMemAttr(MTL_ID(), "IVFDDLRoute", ObCtxIds::VECTOR_CTX_ID)),
    source_to_vector_(ObWrapperAllocator(route_allocator_)), routes_ready_(false), refine_stats_(nullptr),
    soar_quant_error_(OB_SUCCESS)
{}
ObIVFDDLRuntime::~ObIVFDDLRuntime() { release_stats(); }
int64_t ObIVFDDLRuntime::batch_rows(int64_t dim)
{ return dim <= 0 ? 0 : MAX(1L, MIN(4096L, (16L * 1024 * 1024 / sizeof(float)) / dim)); }
void ObIVFDDLRuntime::release_stats() { free_runtime_object(allocator_, refine_stats_); }
void ObIVFDDLRuntime::release_routes()
{
  source_to_vector_.reset();
  route_allocator_.reset();
  routes_ready_ = false;
}
int64_t ObIVFDDLRuntime::vector_index(int64_t source) const
{
  return routes_ready_ && source >= 0 && source < source_to_vector_.count()
      ? source_to_vector_.at(source) : -1;
}
bool ObIVFDDLRuntime::handles(const ObIVFMergeRuntime &runtime, ObIVFMergeStep step,
    const ObIVFMergeQuantizer &output) const
{
  if (runtime.effective_type_ == ObIVFProcessType::CONCATENATION) { return false; }
  // Incremental posting inheritance has its own ownership and CID routing.
  if (runtime.insertion_prior_ != nullptr
      && (step == ObIVFMergeStep::SOAR_ASSIGN || step == ObIVFMergeStep::QUANT)) { return false; }
  return step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE
      || step == ObIVFMergeStep::REFINE_ASSIGN || step == ObIVFMergeStep::REFINE_UPDATE
      || step == ObIVFMergeStep::SOAR_ASSIGN
      || (step == ObIVFMergeStep::QUANT && output.supports_loaded_quantization());
}
int ObIVFDDLRuntime::prepare_routes(const ObIVFMergeRuntime &r)
{
  int ret = OB_SUCCESS;
  const int64_t n = r.all_vids_.count();
  if (routes_ready_) {
    if (source_to_vector_.count() != n) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF source route count changed", K(ret), K(n), "routes", source_to_vector_.count());
    }
  } else if (n <= 0 || n > UINT32_MAX || !r.vector_store_->supports_source_order()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported IVF source routing input", K(ret), K(n));
  } else if (OB_FAIL(source_to_vector_.prepare_allocate(n))) {
    LOG_WARN("failed to allocate IVF source routes", K(ret), K(n));
  } else {
    for (int64_t i = 0; i < n; ++i) { source_to_vector_.at(i) = UINT32_MAX; }
    for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
      int64_t source = -1;
      if (i % 4096 == 0 && is_runtime_canceled(r)) { ret = OB_CANCELED; }
      else if (i % 4096 == 0 && OB_FAIL(share::dag_yield())) {
      } else if (OB_FAIL(r.vector_store_->get_source_row_idx(i, source))) {
        LOG_WARN("failed to locate IVF vector source", K(ret), K(i));
      } else if (source < 0 || source >= n || source_to_vector_.at(source) != UINT32_MAX) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid or duplicate IVF vector source", K(ret), K(i), K(source), K(n));
      } else { source_to_vector_.at(source) = static_cast<uint32_t>(i); }
    }
    if (OB_SUCC(ret)) { routes_ready_ = true; }
    else { release_routes(); }
  }
  return ret;
}
int ObIVFDDLRuntime::prepare_refine_stats(const ObIVFMergeRuntime &r)
{
  int ret = OB_SUCCESS;
  const int64_t k = r.hierarchy_k_, dim = r.dimension_;
  const int64_t per_block = batch_rows(dim);
  if (refine_stats_ != nullptr || k <= 0 || dim <= 0 || k > INT64_MAX / sizeof(float) / dim) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF refinement statistics state", K(ret), KP_(refine_stats), K(k), K(dim));
  } else if (OB_FAIL(alloc_runtime_object(allocator_, refine_stats_, allocator_))) {
    LOG_WARN("failed to allocate IVF refinement statistics", K(ret), K(k), K(dim));
  } else if (OB_FAIL(refine_stats_->sums_.prepare_allocate(k))) {
    LOG_WARN("failed to allocate IVF refinement sum pointers", K(ret), K(k));
  } else if (OB_FAIL(refine_stats_->counts_.prepare_allocate(k))) {
    LOG_WARN("failed to allocate IVF refinement counts", K(ret), K(k));
  } else {
    for (int64_t begin = 0; OB_SUCC(ret) && begin < k; begin += per_block) {
      const int64_t count = MIN(per_block, k - begin);
      float *sums = static_cast<float *>(refine_stats_->allocator_.alloc(count * dim * sizeof(float)));
      if (is_runtime_canceled(r)) {
        ret = OB_CANCELED;
      } else if (sums == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate IVF refinement sums", K(ret), K(begin), K(count), K(dim));
      } else {
        MEMSET(sums, 0, count * dim * sizeof(float));
        for (int64_t i = 0; i < count; ++i) {
          refine_stats_->sums_.at(begin + i) = sums + i * dim;
          refine_stats_->counts_.at(begin + i) = 0;
        }
      }
      if (OB_SUCC(ret)) { ret = share::dag_yield(); }
    }
  }
  if (OB_FAIL(ret)) { release_stats(); }
  else { LOG_INFO("allocated IVF DDL refinement stats", K(k), K(dim),
      "sum_bytes", k * dim * sizeof(float), "memory_hold", refine_stats_->allocator_.total()); }
  return ret;
}
int ObIVFDDLRuntime::prepare(ObIVFMergeRuntime &r, ObIVFMergeActivePlan &plan,
    ObIVFMergeQuantizer &output)
{
  int ret = OB_SUCCESS;
  const auto step = plan.step_;
  const int64_t n = r.all_vids_.count(), dim = r.dimension_, k = r.hierarchy_k_;
  const bool neighborhood_ready = r.neighborhood_mode_ == ObIVFNeighborhoodMode::SKIP || r.neighborhood_finished_;
  if (dim <= 0 || n <= 0
      || !handles(r, step, output) || !plan.work_items_.empty() || plan.ddl_ != nullptr) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF DDL source stage input", K(ret), K(step), K(n), K(k), K(dim));
  } else if (OB_FAIL(alloc_runtime_object(allocator_, plan.ddl_))) {
    LOG_WARN("failed to allocate IVF DDL source stage plan", K(ret), K(step));
  } else if (step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE) {
    ObIVFNeighborGraphView graph;
    if (r.g0_offsets_ != nullptr && r.g0_cids_ != nullptr && r.g0_max_distances_ != nullptr) {
      graph = ObIVFNeighborGraphView(*r.g0_offsets_, *r.g0_cids_, *r.g0_max_distances_, k);
    }
    if (!neighborhood_ready || r.refine_skipped_ || r.a0_ == nullptr || r.working_centers_ != nullptr
        || r.cluster_sample_plan_ != nullptr) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid IVF refinement sample preparation state", K(ret), K(neighborhood_ready), K(r.refine_skipped_),
               KP(r.a0_), KP(r.working_centers_), KP(r.cluster_sample_plan_));
    } else {
      // Hierarchy tasks have drained. Refinement uses A0/C0 and constructs new
      // member lists after assignment; the old per-leaf arrays are no longer
      // read. Retire them before reserving the canonical training sample cache.
      // Skipped refinement keeps its lists for REFINE_COMPACT on the old path.
      const int64_t retired_lists = r.initial_member_lists_.count();
      for (int64_t i = 0; i < retired_lists; ++i) {
        free_runtime_object(allocator_, r.initial_member_lists_.at(i));
      }
      r.initial_member_lists_.reset();
      LOG_INFO("retired IVF DDL hierarchy members before refinement", K(retired_lists));
      ret = algorithm_.begin_refine_samples(n, dim, r.c0_, *r.a0_,
          graph.is_valid() ? &graph : nullptr,
          MAX(0L, r.vector_store_->get_available_replay_memory()), parallelism_,
          r.vector_store_, &r.all_vids_);
    }
    if (OB_SUCC(ret) && algorithm_.uses_resident_refine_source()) {
      // Keep the initialization barrier but do not gather or materialize samples.
      if (OB_FAIL(plan.work_items_.push_back(ObIVFMergeWorkItem()))) {
        LOG_WARN("failed to append resident IVF refinement initialization task", K(ret));
      }
    } else if (OB_SUCC(ret)) {
      const auto &indices = *algorithm_.refine_sample_indices();
      auto &requests = plan.ddl_->cluster_requests_;
      if (OB_FAIL(requests.reserve(indices.count()))) {
        LOG_WARN("failed to reserve IVF refinement sample requests", K(ret), "count", indices.count());
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < indices.count(); ++i) {
        int64_t source = -1;
        if (i % 4096 == 0 && is_runtime_canceled(r)) { ret = OB_CANCELED; }
        else if (i % 4096 == 0 && OB_FAIL(share::dag_yield())) {
        } else if (OB_FAIL(r.vector_store_->get_source_row_idx(indices.at(i), source))) {
          LOG_WARN("failed to locate IVF refinement sample source", K(ret), K(i), "index", indices.at(i));
        } else if (source < 0 || source >= n || n > UINT32_MAX) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid IVF refinement sample source", K(ret), K(i), K(source), K(n));
        } else {
          if (OB_FAIL(requests.push_back({static_cast<uint32_t>(source), static_cast<uint32_t>(indices.at(i)), 0,
                                          static_cast<uint32_t>(i)}))) {
            LOG_WARN("failed to append IVF refinement sample request", K(ret), K(i), K(source));
          }
        }
      }
      if (OB_SUCC(ret)) { lib::ob_sort(requests.begin(), requests.end()); }
      // Each task owns a store and scans one source-ordered range in bounded
      // batches. The previous hierarchy sample stores have already retired;
      // refinement borrows the same unused Replay reservation, not a new quota.
      const int64_t shards = algorithm_.refine_sample_shards();
      for (int64_t shard = 0; OB_SUCC(ret) && shard < shards; ++shard) {
        ObIVFMergeWorkItem item;
        item.vid_begin_ = requests.count() * shard / shards;
        item.vid_end_ = requests.count() * (shard + 1) / shards;
        item.source_input_count_ = item.vid_end_ - item.vid_begin_;
        if (OB_FAIL(plan.work_items_.push_back(item))) {
          LOG_WARN("failed to append IVF DDL work item", K(ret), K(step), K(item.vid_begin_), K(item.vid_end_),
              K(item.cid_begin_), K(item.cid_end_), K(item.local_center_idx_));
        }
      }
    }
  } else if (step == ObIVFMergeStep::REFINE_UPDATE) {
    if (refine_stats_ == nullptr || !r.refine_has_changes_ || r.changed_cids_ == nullptr
        || r.refined_member_lists_ == nullptr || r.working_centers_ == nullptr
        || r.changed_cids_->count() != k || refine_stats_->counts_.count() != k) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid IVF refinement update state", K(ret), K(k), KP_(refine_stats));
    }
    for (int64_t cid = 0; OB_SUCC(ret) && cid < k; ++cid) {
      if (refine_stats_->counts_.at(cid) != r.refined_member_lists_->lists_.at(cid)->vids_.count()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("IVF refinement member count mismatch", K(ret), K(cid), "actual", refine_stats_->counts_.at(cid),
                 "expected", r.refined_member_lists_->lists_.at(cid)->vids_.count());
      } else if (r.changed_cids_->at(cid) != 0 && refine_stats_->counts_.at(cid) > 0) {
        ObIVFMergeWorkItem item;
        item.local_center_idx_ = cid;
        if (OB_FAIL(plan.work_items_.push_back(item))) {
          LOG_WARN("failed to append IVF DDL work item", K(ret), K(step), K(item.vid_begin_), K(item.vid_end_),
              K(item.cid_begin_), K(item.cid_end_), K(item.local_center_idx_));
        }
      }
    }
  } else {
    const bool postings_ready = step == ObIVFMergeStep::QUANT && quantize_during_soar(r);
    if (step == ObIVFMergeStep::REFINE_ASSIGN) {
      if (!neighborhood_ready || r.refine_skipped_ || r.a0_ == nullptr || r.working_centers_ == nullptr ||
          r.refined_assignments_ != nullptr || r.working_centers_->count() != k * dim) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("invalid IVF refinement assignment state", K(ret), K(k), K(dim), K(neighborhood_ready));
      } else {
        ret = prepare_refine_stats(r);
      }
    } else if (step == ObIVFMergeStep::SOAR_ASSIGN) {
      if (!r.refine_finished_ || r.old_to_new_ == nullptr || r.c1_ == nullptr || r.final_member_lists_ == nullptr ||
          r.a1_ != nullptr) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("invalid IVF SOAR assignment state", K(ret), K(r.refine_finished_), KP(r.old_to_new_), KP(r.c1_),
                 KP(r.a1_));
      }
    } else if (!r.soar_finished_ || r.quant_finished_ || r.c1_ == nullptr
        || (!postings_ready && r.a1_ == nullptr)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid IVF quantization state", K(ret), K(r.soar_finished_), K(r.quant_finished_), KP(r.c1_),
               KP(r.a1_));
    }
    if (OB_SUCC(ret) && step == ObIVFMergeStep::SOAR_ASSIGN && quantize_during_soar(r)
        && OB_FAIL(output.prepare_loaded_output(r.final_cluster_count_, parallelism_))) {
      LOG_WARN("failed to prepare IVF posting output", K(ret), K(r.final_cluster_count_), K_(parallelism));
    }
    if (OB_SUCC(ret) && !postings_ready && OB_FAIL(prepare_routes(r))) {
      LOG_WARN("failed to prepare IVF DDL source routes", K(ret), K(step));
    }
    // A fused SOAR task has already emitted both postings from its resident
    // batch. QUANT retains centroid tasks and the existing output barrier.
    for (int64_t begin = 0; OB_SUCC(ret) && !postings_ready && begin < n; begin += batch_rows(dim)) {
      ObIVFMergeWorkItem item;
      item.vid_begin_ = begin; item.vid_end_ = MIN(n, begin + batch_rows(dim));
      item.source_input_count_ = item.vid_end_ - item.vid_begin_;
      if (OB_FAIL(plan.work_items_.push_back(item))) {
        LOG_WARN("failed to append IVF DDL work item", K(ret), K(item.vid_begin_), K(item.vid_end_));
      }
    }
    // Centroid rows are emitted once by bounded DAG tasks, without payload reads.
    if (step == ObIVFMergeStep::QUANT) {
      const int64_t center_chunk = output.posting_write_task_count() > 0 ? r.final_cluster_count_ : 128;
      for (int64_t begin = 0; OB_SUCC(ret) && begin < r.final_cluster_count_; begin += center_chunk) {
        ObIVFMergeWorkItem item;
        item.cid_begin_ = begin; item.cid_end_ = MIN(r.final_cluster_count_, begin + center_chunk);
        if (OB_FAIL(plan.work_items_.push_back(item))) {
          LOG_WARN("failed to append IVF DDL work item", K(ret), K(step), K(item.vid_begin_), K(item.vid_end_),
              K(item.cid_begin_), K(item.cid_end_), K(item.local_center_idx_));
        }
      }
    }
  }
  if (step == ObIVFMergeStep::QUANT) {
    for (int64_t i = 0; OB_SUCC(ret) && i < output.posting_write_task_count(); ++i) {
      ObIVFMergeWorkItem item;
      item.local_center_idx_ = i;
      item.source_input_count_ = 0;
      if (OB_FAIL(plan.work_items_.push_back(item))) {
        LOG_WARN("failed to append IVF posting writer task", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("prepared IVF DDL source stage", K(step), "tasks", plan.work_items_.count(),
        "rows_per_batch", batch_rows(dim), K_(parallelism),
        "quantize_in_soar", quantize_during_soar(r));
  }
  return ret;
}
int ObIVFDDLRuntime::read_batch(const ObIVFMergeRuntime &r, int64_t begin, int64_t end,
    ObIArray<int64_t> &indices, ObIArray<float> &vectors, bool load_payload)
{
  int ret = OB_SUCCESS;
  indices.reuse(); vectors.reuse();
  if (!routes_ready_ || begin < 0 || end <= begin || end > source_to_vector_.count() ||
      end - begin > batch_rows(r.dimension_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL batch range", K(ret), K_(routes_ready), K(begin), K(end), "routes",
             source_to_vector_.count());
  } else if (OB_FAIL(indices.reserve(end - begin))) {
    LOG_WARN("failed to reserve IVF DDL batch indices", K(ret), K(begin), K(end));
  }
  for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
    if (OB_FAIL(indices.push_back(vector_index(i)))) {
      LOG_WARN("failed to append IVF DDL batch vector index", K(ret), K(i), K(begin), K(end));
    }
  }
  if (OB_SUCC(ret) && load_payload && OB_FAIL(r.vector_store_->batch_get_by_source(indices, vectors))) {
    LOG_WARN("failed to read IVF DDL source batch", K(ret), K(begin), K(end));
  } else if (OB_SUCC(ret) && load_payload &&
             (vectors.count() != indices.count() * r.dimension_ || !are_finite_center_values(vectors))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid IVF DDL batch vectors", K(ret), K(begin), K(end), K(r.dimension_), "values", vectors.count());
  }
  return ret;
}
int ObIVFDDLRuntime::execute(ObIVFMergeRuntime &r, ObIVFMergeActivePlan &plan,
    int64_t task_idx, ObIVFMergeQuantizer &output)
{
  int ret = OB_SUCCESS;
  auto &slot = plan.result_slots_.at(task_idx);
  const auto &item = plan.work_items_.at(task_idx);
  const int64_t dim = r.dimension_;
  const auto step = plan.step_;
  const ObMemAttr attr(MTL_ID(), "IVFDDLBatch", ObCtxIds::VECTOR_CTX_ID);
  ObArray<int64_t> indices, positions;
  ObArray<float> vectors;
  indices.set_attr(attr); positions.set_attr(attr); vectors.set_attr(attr);
  ObArray<int64_t, ObWrapperAllocator> *primary = nullptr, *secondary = nullptr;
  ObArray<float, ObWrapperAllocator> *center = nullptr;
  ObIVFNeighborGraphView graph;
  if (slot.installed_ || plan.ddl_ == nullptr) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF DDL task state", K(ret), K(step), K(task_idx), K(slot.installed_), KP(plan.ddl_));
  } else if (is_runtime_canceled(r)) {
    ret = OB_CANCELED;
  } else if (step == ObIVFMergeStep::SOAR_ASSIGN
      && OB_SUCCESS != (ret = ATOMIC_LOAD(&soar_quant_error_))) {
  } else if (step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE && algorithm_.uses_resident_refine_source()) {
    // Session initialization already ran in prepare(); the barrier makes it readable.
    ret = share::dag_yield();
  } else if (step == ObIVFMergeStep::REFINE_SAMPLE_PREPARE) {
    // Reuse one gather buffer across this shard's batches, sized for the actual
    // dimension. Other stages retain their existing task-local allocation policy.
    const int64_t rows = MAX(1L, MIN(batch_rows(dim), item.vid_end_ - item.vid_begin_));
    indices.set_block_size(rows * sizeof(int64_t));
    positions.set_block_size(rows * sizeof(int64_t));
    vectors.set_block_size(rows * dim * sizeof(float));
    if (OB_FAIL(algorithm_.begin_refine_sample_shard(task_idx))) {
      LOG_WARN("failed to start IVF refinement sample shard", K(ret), K(task_idx));
    }
    for (int64_t begin = item.vid_begin_; OB_SUCC(ret) && begin < item.vid_end_; begin += batch_rows(dim)) {
      const int64_t end = MIN(item.vid_end_, begin + batch_rows(dim));
      indices.reuse();
      positions.reuse();
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        const auto &request = plan.ddl_->cluster_requests_.at(i);
        if (OB_FAIL(indices.push_back(request.vector_idx_))) {
          LOG_WARN("failed to append IVF refinement sample index", K(ret), K(i));
        } else if (OB_FAIL(positions.push_back(request.ordinal_))) {
          LOG_WARN("failed to append IVF refinement sample position", K(ret), K(i));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_runtime_canceled(r)) { ret = OB_CANCELED;
      } else if (OB_FAIL(share::dag_yield())) {
      } else if (OB_FAIL(r.vector_store_->batch_get_by_source(indices, vectors))) {
        LOG_WARN("failed to read IVF refinement sample batch", K(ret), K(task_idx), K(begin), K(end));
      } else if (!are_finite_center_values(vectors)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid IVF refinement sample values", K(ret), K(task_idx), K(begin), K(end));
      } else if (OB_FAIL(algorithm_.load_refine_samples(positions, vectors, task_idx))) {
        LOG_WARN("failed to store IVF refinement sample batch", K(ret), K(task_idx), K(begin), K(end));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(algorithm_.finish_refine_sample_shard(task_idx))) {
      LOG_WARN("failed to finish IVF refinement sample shard", K(ret), K(task_idx));
    }
  } else if (step == ObIVFMergeStep::REFINE_UPDATE) {
    const int64_t cid = item.local_center_idx_;
    if (refine_stats_ == nullptr || cid < 0 || cid >= refine_stats_->counts_.count()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("invalid IVF refinement center update state", K(ret), K(task_idx), K(cid), KP_(refine_stats));
    } else if (OB_FAIL(alloc_runtime_array(allocator_, center))) {
      LOG_WARN("failed to allocate IVF refinement center", K(ret), K(task_idx), K(cid));
    } else { ret = algorithm_.hca_finalize_center_stats(refine_stats_->sums_.at(cid),
        refine_stats_->counts_.at(cid), dim, *center); }
  } else if (step == ObIVFMergeStep::QUANT && item.local_center_idx_ >= 0) {
    ret = output.write_posting_range(item.local_center_idx_);
  } else if (step == ObIVFMergeStep::QUANT && item.cid_begin_ >= 0) {
    ret = output.write_loaded_centers(*r.c1_, dim, item.cid_begin_, item.cid_end_);
  } else if (OB_FAIL(
                 read_batch(r, item.vid_begin_, item.vid_end_, indices, vectors,
                            step != ObIVFMergeStep::SOAR_ASSIGN || quantize_during_soar(r)))) {
  } else if (step == ObIVFMergeStep::REFINE_ASSIGN) {
    if (r.g0_offsets_ != nullptr) {
      graph = ObIVFNeighborGraphView(*r.g0_offsets_, *r.g0_cids_, *r.g0_max_distances_, r.hierarchy_k_);
    }
    if (OB_FAIL(alloc_runtime_array(allocator_, primary))) {
      LOG_WARN("failed to allocate IVF refinement assignments", K(ret), K(task_idx));
    } else if (OB_FAIL(algorithm_.refine_assign_loaded(vectors, indices, dim,
        *r.working_centers_, *r.a0_, graph.is_valid() ? &graph : nullptr, *primary))) {
      LOG_WARN("failed to assign IVF DDL refinement batch", K(ret), K(task_idx), K(dim));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < primary->count(); ++i) {
      if (primary->at(i) < 0 || primary->at(i) >= r.hierarchy_k_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid IVF refinement primary CID", K(ret), K(task_idx), K(i), "cid", primary->at(i),
                 K(r.hierarchy_k_));
      } else {
        if (OB_FAIL(positions.push_back(i))) {
          LOG_WARN("failed to append IVF refinement assignment position", K(ret), K(task_idx), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && primary->count() != indices.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("IVF refinement assignment count mismatch", K(ret), K(task_idx), "actual", primary->count(), "expected",
               indices.count());
    }
    if (OB_SUCC(ret)) { lib::ob_sort(positions.begin(), positions.end(), ObIVFSourcePositionLess(*primary)); }
  } else if (step == ObIVFMergeStep::SOAR_ASSIGN) {
    const auto *old = r.refine_skipped_ ? r.a0_ : r.refined_assignments_;
    if (old == nullptr || r.old_to_new_ == nullptr) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("missing IVF SOAR source assignments or CID remap", K(ret), K(task_idx), KP(old), KP(r.old_to_new_));
    } else if (OB_FAIL(alloc_runtime_array(allocator_, primary))) {
      LOG_WARN("failed to allocate IVF SOAR primary assignments", K(ret), K(task_idx));
    } else if (OB_FAIL(primary->reserve(indices.count()))) {
      LOG_WARN("failed to reserve IVF SOAR primary assignments", K(ret), K(task_idx), "count", indices.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < indices.count(); ++i) {
      const int64_t old_cid = old->at(indices.at(i));
      if (old_cid < 0 || old_cid >= r.old_to_new_->count()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid IVF SOAR old CID", K(ret), K(task_idx), K(i), K(old_cid), "count", r.old_to_new_->count());
      } else if (r.old_to_new_->at(old_cid) < 0 || r.old_to_new_->at(old_cid) >= r.final_cluster_count_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid IVF SOAR remapped CID", K(ret), K(task_idx), K(old_cid), "new_cid",
                 r.old_to_new_->at(old_cid), K(r.final_cluster_count_));
      } else {
        if (OB_FAIL(primary->push_back(r.old_to_new_->at(old_cid)))) {
          LOG_WARN("failed to append remapped IVF SOAR CID", K(ret), K(task_idx), K(i), K(old_cid));
        }
      }
    }
    if (OB_SUCC(ret) && r.enable_soar_ && r.final_cluster_count_ > 1) {
      if (r.g1_offsets_ != nullptr) {
        graph = ObIVFNeighborGraphView(*r.g1_offsets_, *r.g1_cids_, *r.g1_max_distances_, r.final_cluster_count_);
      }
      if (OB_FAIL(alloc_runtime_array(allocator_, secondary))) {
        LOG_WARN("failed to allocate IVF SOAR secondary assignments", K(ret), K(task_idx));
      } else { ret = algorithm_.soar_assign_loaded(vectors, dim, *r.c1_, *primary,
          graph.is_valid() ? &graph : nullptr, *secondary); }
    }
    // A single center skips secondary assignment, but still needs the loaded
    // vectors to encode its primary postings in this batch.
    if (OB_SUCC(ret) && quantize_during_soar(r)) {
      ObArray<ObIVFVectorVID> vids;
      vids.set_attr(attr);
      for (int64_t i = 0; OB_SUCC(ret) && i < indices.count(); ++i) {
        if (OB_FAIL(vids.push_back(r.all_vids_.at(indices.at(i))))) {
          LOG_WARN("failed to retain IVF SOAR quantization VID", K(ret), K(task_idx), K(i));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_runtime_canceled(r)) {
        ret = OB_CANCELED;
      } else if (OB_FAIL(output.quantize_loaded_vectors(*r.c1_, dim, vids, vectors, *primary, secondary))) {
        // A sink may have appended part of this batch. Prevent an explicit
        // task retry from duplicating that prefix, including for custom sinks.
        LOG_WARN("failed to quantize IVF SOAR batch", K(ret), K(task_idx), "rows", indices.count());
        (void)ATOMIC_BCAS(&soar_quant_error_, OB_SUCCESS, ret);
      }
    }
  } else if (step == ObIVFMergeStep::QUANT) {
    ObArray<ObIVFVectorVID> vids;
    ObArray<int64_t> a1, a2;
    vids.set_attr(attr); a1.set_attr(attr); a2.set_attr(attr);
    for (int64_t i = 0; OB_SUCC(ret) && i < indices.count(); ++i) {
      const int64_t idx = indices.at(i);
      if (OB_FAIL(vids.push_back(r.all_vids_.at(idx)))) {
        LOG_WARN("failed to retain IVF quantization VID", K(ret), K(task_idx), K(idx));
      } else if (OB_FAIL(a1.push_back(r.a1_->at(idx)))) {
        LOG_WARN("failed to retain IVF quantization primary CID", K(ret), K(task_idx), K(idx));
      } else if (r.a2_ != nullptr) {
        if (OB_FAIL(a2.push_back(r.a2_->at(idx)))) {
          LOG_WARN("failed to retain IVF quantization secondary CID", K(ret), K(task_idx), K(idx));
        }
      }
    }
    if (OB_SUCC(ret)) { ret = output.quantize_loaded_vectors(*r.c1_, dim, vids, vectors,
        a1, r.a2_ == nullptr ? nullptr : &a2); }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported IVF DDL task step", K(ret), K(step), K(task_idx));
  }
  if (OB_SUCC(ret) && is_runtime_canceled(r)) { ret = OB_CANCELED; }
  if (OB_SUCC(ret)) {
    // No fallible operations after this point. Commit all members, including
    // unchanged ones, exactly once per task before publishing its result slot.
    if (step == ObIVFMergeStep::REFINE_ASSIGN) {
      for (int64_t begin = 0; begin < positions.count();) {
        const int64_t cid = primary->at(positions.at(begin));
        int64_t end = begin + 1;
        while (end < positions.count() && primary->at(positions.at(end)) == cid) { ++end; }
        {
          ObSpinLockGuard guard(refine_stats_->locks_[cid % ObIVFSourceUpdate::LOCK_COUNT].lock_);
          for (int64_t i = begin; i < end; ++i) {
            algorithm_.hca_accumulate_center_stats(refine_stats_->sums_.at(cid),
                &vectors.at(positions.at(i) * dim), dim);
          }
          refine_stats_->counts_.at(cid) += end - begin;
        }
        begin = end;
      }
    }
    slot.center_idxs_ = primary; primary = nullptr;
    slot.secondary_center_idxs_ = secondary; secondary = nullptr;
    slot.center_values_ = center; center = nullptr;
    slot.installed_ = true;
  }
  free_runtime_object(allocator_, primary);
  free_runtime_object(allocator_, secondary);
  free_runtime_object(allocator_, center);
  return ret;
}
ObIVFRefinePhase ObIVFDDLRuntime::refine_train_phase() const
{ return algorithm_.refine_train_phase(); }
int ObIVFDDLRuntime::prepare_refine_phase(int64_t &count)
{ return algorithm_.prepare_refine_phase(count); }
int ObIVFDDLRuntime::execute_refine_phase(int64_t begin, int64_t end)
{ return algorithm_.execute_refine_phase(begin, end); }
int ObIVFDDLRuntime::finish_refine_phase(ObIArray<float> &centers, bool &finished)
{ return algorithm_.finish_refine_phase(centers, finished); }
int ObIVFDDLRuntime::finish_samples() { return algorithm_.finish_refine_samples(); }

int ObIVFDDLRuntime::assign_loaded_secondary(const ObIArray<float> &vectors, int64_t dimension,
    const ObIArray<float> &centers, const ObIArray<int64_t> &primary,
    const ObIVFNeighborGraphView *graph, ObIArray<int64_t> &secondary)
{
  return algorithm_.soar_assign_loaded(vectors, dimension, centers, primary, graph, secondary);
}

} // namespace storage
} // namespace oceanbase
