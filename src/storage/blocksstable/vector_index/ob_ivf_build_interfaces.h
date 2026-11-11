/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_INTERFACES_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_INTERFACES_H_

#include "lib/container/ob_se_array.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "storage/blocksstable/vector_index/ob_ivf_dag_planner.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_strategy.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_layout.h"

namespace oceanbase
{
namespace storage
{
static const int64_t IVF_MERGE_INVALID_DEPTH = -1;
typedef uint64_t ObIVFVectorVID;
struct ObIVFHcaAlgorithmParam;

enum class ObIVFMergeStage : uint8_t
{
  INVALID = 0,
  PRIOR,                                // reusable root-center initialization
  HIERARCHY,                            // 层次化聚类
  NEIGHBORHOOD,                         // 质心邻接结构构造
  REFINE,                               // 质心数量平衡微调
  SOAR,                                 // 向量冗余分配
  CENTROID_INDEX,                      // 对聚类中心做二次聚类生产父聚类中心
  QUANT,                                // 1-bit 量化及按 cid 有序写入
  RAW_WRITE,                            // 按 VID 顺序写入全精度向量
  FINISH,
  MAX
};

// Operations understood by the shared executor, not a strategy state machine.
enum class ObIVFMergeStep : uint8_t
{
  INVALID = 0,
  PRIOR_WORK,           // prior initializer phase; may repeat across barriers
  HIERARCHY_WARM_START, // legacy insertion step, retained but no longer dispatched
  HIERARCHY_CLUSTER,    // 当前 depth 聚类并生成质心
  HIERARCHY_ASSIGN,     // 并行将向量分配到当前质心
  HIERARCHY_UPDATE,     // 更新质心、删除空质心并判断下一 depth
  NEIGHBORHOOD_ADD,     // 划分质心并向 HGraph Add
  NEIGHBORHOOD_BUILD,   // 查询、精排并生成质心邻接结构
  REFINE_TRAIN,         // 训练并冻结 WorkingCenters
  REFINE_ASSIGN,        // 并行生成 refined assignment/member fragments
  REFINE_UPDATE,        // 并行更新 membership 发生变化的质心
  REFINE_COMPACT,       // 删除空质心并重映射为连续 cid
  SOAR_ASSIGN,          // 生成 primary/secondary assignment；DDL 可在同批完成量化
  QUANT,                // 量化输出屏障；融合 DDL 路径仅生成中心行并完成排序写出
  RAW_WRITE,            // QUANT 完成后独立写入 RAW_VECTOR
  HIERARCHY_SAMPLE_PREPARE, // INITIAL_BUILD 非根层并行读取并整理训练样本
  REFINE_SAMPLE_PREPARE, // INITIAL_BUILD source-order sample loading
  MAX
};

enum class ObIVFRefinePhase : uint8_t { DISTANCE, ASSIGN, UPDATE };

// Value-only snapshots: the observer must not retain runtime/work-item pointers.
struct ObIVFMergeStepPerf
{
  ObIVFMergeStage stage_ = ObIVFMergeStage::INVALID;
  ObIVFMergeStep step_ = ObIVFMergeStep::INVALID;
  // Capture the executed variant before prepare; completion can release the
  // runtime state used to select it. Scheduling still uses the shared enums.
  bool quantize_in_soar_ = false;
  ObIVFRefinePhase refine_phase_ = ObIVFRefinePhase::DISTANCE;
  int64_t depth_ = -1;
  int64_t plan_id_ = -1;
  int64_t task_count_ = 0;
  int64_t start_us_ = 0;
  int64_t prepared_us_ = 0;
  int64_t finish_begin_us_ = 0;
  int64_t end_us_ = 0;
  int ret_code_ = 0;
};

struct ObIVFMergeWorkPerf
{
  int64_t node_idx_ = -1;
  int64_t center_idx_ = -1;
  int64_t input_vectors_ = -1;
  int64_t secondary_vectors_ = 0;
  // Actual output relations after the task commits; copied codes are included.
  int64_t posting_rows_ = 0;
  int64_t reused_posting_rows_ = 0;
  int64_t centers_ = -1;
  int64_t first_level_samples_ = -1;
};

class ObIVFMergePerfObserver
{
public:
  virtual ~ObIVFMergePerfObserver() = default;
  virtual void on_step_perf(const ObIVFMergeStepPerf &perf) = 0;
};

const char *ivf_merge_stage_name(ObIVFMergeStage stage);
const char *ivf_merge_step_name(ObIVFMergeStep step);
const char *ivf_merge_step_name(const ObIVFMergeStepPerf &perf);

enum class ObIVFNeighborhoodMode : uint8_t
{
  INVALID = 0,
  SKIP,
  BRUTE_FORCE,
  HGRAPH,
  MAX
};

inline bool is_valid_ivf_neighborhood_mode(const ObIVFNeighborhoodMode mode)
{
  return mode > ObIVFNeighborhoodMode::INVALID
      && mode < ObIVFNeighborhoodMode::MAX;
}

class ObIVFVectorStore
{
public:
  virtual ~ObIVFVectorStore() = default;
  // Optional sealed-source order, independent of VID order. Replay-backed
  // stores let applicable algorithm stages read active rows in physical source order.
  virtual bool supports_source_order() const { return false; }
  virtual bool is_fully_memory_resident() const { return false; }
  virtual int64_t get_source_read_parallelism() const { return 1; }
  // Unused bytes in this build's reservation after sealing Replay. Successive
  // sample phases may borrow these bytes, but must not overlap their lifetimes.
  virtual int64_t get_available_replay_memory() const { return 0; }
  virtual int get_source_row_idx(const int64_t vector_idx, int64_t &source_idx) const
  { UNUSED(vector_idx); source_idx = -1; return OB_NOT_SUPPORTED; }
  // vector_idxs are ranks in get_all_vids(), strictly ordered by source row.
  // Read only the requested live rows, retaining task-private reader state.
  virtual int batch_get_by_source(const common::ObIArray<int64_t> &vector_idxs,
      common::ObIArray<float> &vectors) const
  { UNUSEDx(vector_idxs, vectors); return OB_NOT_SUPPORTED; }
  // Production adapters expose sealed replay stores captured from DDL input
  // or live IVF RAW rows. A valid store is immutable and guarantees globally unique
  // VIDs.
  virtual bool is_valid() const = 0;
  virtual int64_t get_vector_count() const = 0;
  virtual int64_t get_dimension() const = 0;
  // Returns all unique business VIDs in strictly ascending order. VID is
  // uint64_t and is independent of ObRADatumStore's dense row_id locator.
  virtual int get_all_vids(
      common::ObIArray<ObIVFVectorVID> &vids) const = 0;
  // Implementations resolve VID through the immutable replay store and must
  // use task-local reader state. Output is copied and flattened as
  // (vid_end - vid_begin) * dimension floats.
  virtual int batch_get_by_vid(
      const common::ObIArray<ObIVFVectorVID> &vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<float> &vectors) const = 0;
};

// First-level samples are preselected locators over captured replay stores.
// Adapters load sample payloads and verify their VIDs against the locators.
// Root training consumes this view; node cardinality comes from VectorStore.
class ObIVFFirstLevelSampleSet
{
public:
  virtual ~ObIVFFirstLevelSampleSet() = default;
  // A valid set is the already merged global bottom-K sample locator set;
  // each sampled VID appears exactly once.
  virtual bool is_valid() const = 0;
  virtual int64_t get_sample_count() const = 0;
  virtual int64_t get_dimension() const = 0;
  virtual int batch_get_samples(
      const int64_t sample_begin,
      const int64_t sample_end,
      common::ObIArray<ObIVFVectorVID> &sample_vids,
      common::ObIArray<float> &sample_vectors) const = 0;
};

class ObIVFVectorReader
{
public:
  ObIVFVectorReader();
  ~ObIVFVectorReader() = default;
  int init(const ObIVFVectorStore &vector_store);
  int batch_get_by_vid(
      const common::ObIArray<ObIVFVectorVID> &vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<float> &vectors) const;
  int64_t get_dimension() const;
private:
  const ObIVFVectorStore *vector_store_;
};

// Merge/DDL capture supplies vectors in the persisted FHT coordinate space.
struct ObIVFMergeInput
{
  ObIVFMergeInput();
  ~ObIVFMergeInput() = default;
  int init(
      const ObIVFVectorStore &vector_store,
      const ObIVFFirstLevelSampleSet &first_level_samples,
      const int64_t cluster_size,
      const bool vectors_normalized,
      const bool vectors_rotated,
      const bool enable_soar = true,
      const share::ObVectorIndexDistAlgorithm dist_type = share::VIDA_COS);
  bool is_valid() const;

  bool is_inited_;
  const ObIVFVectorStore *vector_store_;
  const ObIVFFirstLevelSampleSet *first_level_samples_;
  int64_t cluster_size_;
  bool enable_soar_;
  bool vectors_normalized_;
  bool vectors_rotated_;
  share::ObVectorIndexDistAlgorithm dist_type_;
  TO_STRING_KV(K_(is_inited), KP_(vector_store), KP_(first_level_samples),
      K_(cluster_size), K_(enable_soar), K_(dist_type),
      K_(vectors_normalized), K_(vectors_rotated));
};

// A compact immutable adjacency view. offsets_[cid]..offsets_[cid + 1]
// addresses one center's neighbor row in cids_. A null view means callers
// must scan every center.
struct ObIVFNeighborGraphView
{
  ObIVFNeighborGraphView();
  ObIVFNeighborGraphView(
      const common::ObIArray<int64_t> &offsets,
      const common::ObIArray<int64_t> &cids,
      const common::ObIArray<float> &max_intra_distances,
      const int64_t center_count);
  bool is_valid() const;

  const common::ObIArray<int64_t> *offsets_;
  const common::ObIArray<int64_t> *cids_;
  const common::ObIArray<float> *max_intra_distances_;
  int64_t center_count_;
  TO_STRING_KV(KP_(offsets), KP_(cids), KP_(max_intra_distances),
      K_(center_count));
};

// Physical output is deliberately a separate seam from HCA. Production uses
// one cluster as the one-bit task granularity. Once quantization has finished,
// disjoint VID ranges may write RAW_VECTOR concurrently; finish_raw_write()
// is the barrier that publishes completion of the physical output.
class ObIVFEncodedPostingConsumer
{
public:
  virtual ~ObIVFEncodedPostingConsumer() = default;
  // The bytes are borrowed only until this synchronous call returns.
  virtual int append(const ObIVFVectorVID vid, const common::ObString &payload) = 0;
};

// Searches immutable source leaves and returns zero-based source leaf indices.
// Implementations own task-local query state and may use either cache or storage.
class ObIVFCentroidSource
{
public:
  virtual ~ObIVFCentroidSource() = default;
  virtual int search_centroids(int64_t source_segment_idx, const float *query,
      int64_t dimension, int32_t *candidates, int64_t capacity, int64_t &count) const = 0;
};

class ObIVFEncodedPostingSource
{
public:
  virtual ~ObIVFEncodedPostingSource() = default;
  // source_center_idx is an index in the captured prior, not a final CID.
  virtual int scan_postings(const int64_t source_center_idx,
      ObIVFEncodedPostingConsumer &consumer) const = 0;
};

class ObIVFMergeQuantizer
{
public:
  virtual ~ObIVFMergeQuantizer() = default;
  virtual int set_centroid_layout(const blocksstable::ObIvfCentroidLayout &layout)
  { UNUSED(layout); return OB_SUCCESS; }
  // Shared loaded output used by DDL and merge. Merge also supports the
  // CID-range protocol below for strategies that reuse encoded postings.
  // Loaded sinks accept SOAR postings before centroid rows and retain them
  // until finish_quantization() validates the complete build. Output failures
  // terminate the attempt because a partially appended batch cannot roll back.
  virtual bool supports_loaded_quantization() const { return false; }
  virtual bool supports_loaded_posting_reuse() const { return false; }
  virtual int copy_loaded_postings(const ObIVFEncodedPostingSource &source,
      int64_t source_cid, int64_t target_cid, const common::ObIArray<ObIVFVectorVID> &expected_vids)
  { UNUSEDx(source, source_cid, target_cid, expected_vids); return OB_NOT_SUPPORTED; }
  // Called at the SOAR barrier before center output, including when postings
  // are encoded later in QUANT. Counts include primary and secondary postings.
  virtual int set_leaf_posting_counts(const common::ObIArray<uint32_t> &counts)
  { UNUSED(counts); return OB_SUCCESS; }
  virtual int prepare_loaded_output(int64_t center_count, int64_t parallelism)
  { UNUSEDx(center_count, parallelism); return OB_SUCCESS; }
  virtual int64_t posting_write_task_count() const { return 0; }
  virtual int write_posting_range(int64_t task_idx)
  { UNUSED(task_idx); return OB_NOT_SUPPORTED; }
  virtual int write_loaded_centers(const common::ObIArray<float> &centers,
      int64_t dimension, int64_t begin, int64_t end)
  { UNUSEDx(centers, dimension, begin, end); return OB_NOT_SUPPORTED; }
  virtual int quantize_loaded_vectors(const common::ObIArray<float> &centers, int64_t dimension,
      const common::ObIArray<ObIVFVectorVID> &vids, const common::ObIArray<float> &vectors,
      const common::ObIArray<int64_t> &primary, const common::ObIArray<int64_t> *secondary)
  { UNUSEDx(centers, dimension, vids, vectors, primary, secondary); return OB_NOT_SUPPORTED; }

  // Prepare is serial. Each task owns a contiguous CID range and closes its
  // output before the QUANT barrier. Legacy sinks keep one task per center.
  virtual int prepare_quantization(const common::ObIArray<float> &centers,
      const int64_t center_count, int64_t &task_count)
  { UNUSED(centers); task_count = center_count; return OB_SUCCESS; }
  virtual int open_quantization_range(const int64_t cid_begin, const int64_t cid_end,
      const int64_t task_idx, common::ObIAllocator &allocator, ObIVFMergeQuantizer *&output)
  { UNUSEDx(cid_begin, cid_end, task_idx, allocator); output = this; return OB_SUCCESS; }
  // Always called for an opened range, including failures/cancellation. Closing
  // releases only task-local state; the owning sink outlives all active tasks.
  virtual int close_quantization_range(ObIVFMergeQuantizer *&output, const bool success)
  { UNUSED(success); output = nullptr; return OB_SUCCESS; }
  virtual bool supports_encoded_posting_reuse() const { return false; }
  virtual int quantize_cluster_with_reuse(
      ObIVFVectorReader &reader, const common::ObIArray<float> &final_centers,
      const int64_t center_count, const int64_t cid,
      const common::ObIArray<ObIVFVectorVID> &member_vids,
      const common::ObIArray<ObIVFVectorVID> *secondary_member_vids,
      const ObIVFEncodedPostingSource &source, const int64_t source_center_idx,
      const int64_t reused_prefix_count)
  {
    UNUSEDx(reader, final_centers, center_count, cid, member_vids,
        secondary_member_vids, source, source_center_idx, reused_prefix_count);
    return OB_NOT_SUPPORTED;
  }
  virtual int quantize_cluster(
      ObIVFVectorReader &reader,
      const common::ObIArray<float> &final_centers,
      const int64_t center_count,
      const int64_t cid,
      const common::ObIArray<ObIVFVectorVID> &primary_member_vids,
      const common::ObIArray<ObIVFVectorVID> *secondary_member_vids) = 0;
  virtual int finish_quantization(
      const int64_t vector_count,
      const int64_t center_count,
      const int64_t posting_count) = 0;
  virtual int write_raw_vectors(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      const int64_t task_idx) = 0;
  virtual int finish_raw_write(const int64_t vector_count) = 0;
};

// Processor owns every durable VIDList, center buffer, assignment and graph
// installed by the task/barrier protocol. The algorithm adapter may retain
// temporary numerical state only between its paired begin/finish callbacks.
class ObIVFMergeAlgorithm
{
public:
  virtual ~ObIVFMergeAlgorithm() = default;
  // The parent build creates its own adapter and reuses these numerical settings.
  virtual int get_parent_cluster_params(ObIVFHcaAlgorithmParam &param) const
  { UNUSED(param); return OB_SUCCESS; }
  virtual int check_cluster_quality(const common::ObIArray<int64_t> &counts, bool &degenerate)
  { UNUSEDx(counts, degenerate); return OB_NOT_SUPPORTED; }
  virtual int insertion_train_centers(ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const ObIVFFirstLevelSampleSet &first_level_samples,
      const common::ObIArray<float> &initial_centers, const int64_t center_count,
      common::ObIArray<float> &trained_centers)
  { UNUSEDx(reader, all_vids, first_level_samples, initial_centers, center_count, trained_centers); return OB_NOT_SUPPORTED; }

  virtual int insertion_assign_vectors(ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &vids,
      const common::ObIArray<float> &centers,
      const common::ObIArray<int64_t> &offsets,
      const common::ObIArray<int64_t> &candidates,
      int64_t begin, int64_t end, common::ObIArray<int64_t> &assignments)
  { UNUSEDx(reader, vids, centers, offsets, candidates, begin, end, assignments); return OB_NOT_SUPPORTED; }

  virtual bool supports_prepared_cluster() const { return false; }
  virtual int hca_collect_training_rows(const int64_t vector_count,
      const int64_t cluster_size, common::ObIArray<int64_t> &ordinals)
  { UNUSEDx(vector_count, cluster_size, ordinals); return OB_NOT_SUPPORTED; }
  virtual int hca_train_loaded_samples(const int64_t vector_count,
      const int64_t cluster_size, const int64_t dimension,
      const common::ObIArray<int64_t> &ordinals,
      const common::ObIArray<float> &vectors, int64_t &local_k,
      common::ObIArray<float> &centers)
  { UNUSEDx(vector_count, cluster_size, dimension, ordinals, vectors, local_k, centers); return OB_NOT_SUPPORTED; }

  virtual int hca_train_local_centers(
      const int64_t depth,
      const int64_t node_idx,
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &node_vids,
      const ObIVFFirstLevelSampleSet *first_level_samples,
      const int64_t cluster_size,
      int64_t &local_k,
      common::ObIArray<float> &local_centers) = 0;
  virtual int hca_assign_vectors(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &node_vids,
      const common::ObIArray<float> &local_centers,
      const int64_t local_k,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<int64_t> &local_center_idxs) = 0;
  virtual bool supports_loaded_assign() const { return false; }
  virtual int hca_assign_loaded_vectors(
      const common::ObIArray<float> &vectors,
      const common::ObIArray<int64_t> &positions,
      const int64_t begin, const int64_t end, const int64_t dimension,
      const common::ObIArray<float> &centers, const int64_t local_k,
      common::ObIArray<int64_t> &assignments)
  {
    UNUSEDx(vectors, positions, begin, end, dimension, centers, local_k, assignments);
    return OB_NOT_SUPPORTED;
  }
  // Optional source-ordered UPDATE. Accumulation is allocation-free and cannot
  // fail, so a task publishes its entire validated batch without partial retry.
  virtual bool supports_source_update() const { return false; }
  virtual void hca_accumulate_center_stats(float *sum, const float *vector,
      const int64_t dimension) { UNUSEDx(sum, vector, dimension); }
  virtual int hca_finalize_center_stats(const float *sum, const int64_t count,
      const int64_t dimension, common::ObIArray<float> &center)
  { UNUSEDx(sum, count, dimension, center); return OB_NOT_SUPPORTED; }
  virtual int hca_compute_local_center_stats_range(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &member_vids,
      const common::ObIArray<float> &local_centers,
      const int64_t local_center_idx,
      common::ObIArray<float> &updated_center) = 0;

  // The methods below are the numerical seam for the post-Hierarchy stages.
  // Processor owns all durable buffers; implementations may retain only
  // temporary algorithm state between begin/finish calls (for example HGraph).
  // Concurrent task callbacks must be thread-safe.
  virtual int neighborhood_begin(
      const ObIVFNeighborhoodMode mode,
      const common::ObIArray<float> &centers,
      const int64_t center_count,
      const int64_t dimension)
  {
    UNUSEDx(mode, centers, center_count, dimension);
    return OB_NOT_SUPPORTED;
  }
  virtual int neighborhood_add_centers(
      const common::ObIArray<float> &centers,
      const int64_t center_count,
      const int64_t dimension,
      const int64_t cid_begin,
      const int64_t cid_end)
  {
    UNUSEDx(centers, center_count, dimension, cid_begin, cid_end);
    return OB_NOT_SUPPORTED;
  }
  virtual int neighborhood_finish_add()
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int neighborhood_find_neighbors(
      const ObIVFNeighborhoodMode mode,
      const common::ObIArray<float> &centers,
      const int64_t center_count,
      const int64_t dimension,
      const int64_t cid_begin,
      const int64_t cid_end,
      common::ObIArray<int64_t> &neighbor_offsets,
      common::ObIArray<int64_t> &neighbor_cids,
      common::ObIArray<float> &max_intra_distances)
  {
    UNUSEDx(mode, centers, center_count, dimension, cid_begin, cid_end,
        neighbor_offsets, neighbor_cids, max_intra_distances);
    return OB_NOT_SUPPORTED;
  }
  virtual int neighborhood_finish()
  {
    return OB_NOT_SUPPORTED;
  }
  // Assignment arrays are dense in all_vids replay order; they are never
  // indexed by the numeric uint64_t VID value.
  virtual bool supports_batched_refine_train() const { return false; }
  virtual int refine_train_prepare(ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const common::ObIArray<float> &centers, const common::ObIArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *graph, int64_t center_count, int64_t &batch_size)
  { UNUSEDx(reader, all_vids, centers, assignments, graph, center_count, batch_size); return OB_NOT_SUPPORTED; }
  virtual int refine_train_distances(int64_t begin, int64_t end)
  { UNUSEDx(begin, end); return OB_NOT_SUPPORTED; }
  virtual int refine_train(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const common::ObIArray<float> &centers,
      const common::ObIArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *neighbor_graph,
      const int64_t center_count,
      common::ObIArray<float> &working_centers)
  {
    UNUSEDx(reader, all_vids, centers, assignments, neighbor_graph,
        center_count, working_centers);
    return OB_NOT_SUPPORTED;
  }
  // Begin/finish run at serial plan boundaries; only distance tasks are parallel.
  virtual bool supports_parallel_refine() const { return false; }
  virtual int refine_train_begin(ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &vids,
      const common::ObIArray<float> &centers,
      const common::ObIArray<int64_t> &assignments,
      const ObIVFNeighborGraphView *graph, int64_t center_count,
      common::ObIArray<float> &working_centers)
  { return refine_train(reader, vids, centers, assignments, graph, center_count, working_centers); }
  virtual int64_t refine_train_task_count() const { return 0; }
  virtual int refine_train_distance_task(int64_t task_idx)
  { UNUSED(task_idx); return OB_NOT_SUPPORTED; }
  virtual int refine_train_finish_batch(common::ObIArray<float> &centers, bool &finished)
  { UNUSEDx(centers, finished); return OB_NOT_SUPPORTED; }
  virtual int refine_assign_vectors(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const common::ObIArray<float> &working_centers,
      const common::ObIArray<int64_t> &old_assignments,
      const ObIVFNeighborGraphView *neighbor_graph,
      const int64_t center_count,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<int64_t> &new_cids)
  {
    UNUSEDx(reader, all_vids, working_centers, old_assignments,
        neighbor_graph, center_count, vid_begin, vid_end, new_cids);
    return OB_NOT_SUPPORTED;
  }
  virtual int refine_update_center(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &member_vids,
      const common::ObIArray<float> &working_centers,
      const int64_t center_count,
      const int64_t cid,
      common::ObIArray<float> &updated_center)
  {
    UNUSEDx(reader, member_vids, working_centers, center_count, cid,
        updated_center);
    return OB_NOT_SUPPORTED;
  }
  virtual int soar_assign_secondary(
      ObIVFVectorReader &reader,
      const common::ObIArray<ObIVFVectorVID> &all_vids,
      const common::ObIArray<float> &final_centers,
      const common::ObIArray<int64_t> &primary_cids,
      const ObIVFNeighborGraphView *neighbor_graph,
      const int64_t center_count,
      const int64_t vid_begin,
      const int64_t vid_end,
      common::ObIArray<int64_t> &secondary_cids)
  {
    UNUSEDx(reader, all_vids, final_centers, primary_cids, neighbor_graph,
        center_count, vid_begin, vid_end, secondary_cids);
    return OB_NOT_SUPPORTED;
  }
  // Called only after all insertion root assignment tasks have joined.
  virtual void finish_insertion_assign() {}
  virtual void cancel(const int ret_code) { UNUSED(ret_code); }
};

struct ObIVFMergeStepResult
{
  ObIVFMergeStepResult();
  bool is_valid() const;
  int64_t plan_id_;
  ObIVFMergeStage stage_;
  ObIVFMergeStep step_;
  int64_t depth_;
  bool committed_;
  bool prior_finished_;
  bool quality_fallback_;
  bool hierarchy_finished_;
  // UPDATE requests sample preparation before training the next depth only
  // when the input supports source-ordered replay and the algorithm supports prepared samples.
  bool next_cluster_needs_samples_;
  bool next_refine_needs_samples_; // INITIAL_BUILD optional preparation barrier.
  int64_t k_;
  ObIVFNeighborhoodMode neighborhood_mode_;
  bool refine_skipped_;
  bool refine_has_changes_;
  // A REFINE_TRAIN plan covers one distance batch. False keeps the planner
  // at this step until the serial batch barrier has consumed every epoch.
  bool refine_train_finished_;
  bool centroid_index_finished_; // false schedules the next concrete centroid-index step.
  bool merge_finished_;
  TO_STRING_KV(K_(plan_id), K_(stage), K_(step), K_(depth),
      K_(committed), K_(hierarchy_finished), K_(next_cluster_needs_samples), K_(next_refine_needs_samples), K_(k),
      K_(neighborhood_mode), K_(refine_skipped),
      K_(refine_has_changes), K_(refine_train_finished), K_(centroid_index_finished), K_(merge_finished));
};

// Shared merge completion metadata. finalized_ is published only after every
// cluster has been quantized and all one-bit/RAW physical rows have been
// written through the supplied physical-output adapter; the owning DAG can
// then create and install the readable IVF SSTable.
struct ObIVFMergeArtifact : public ObIVFDagArtifact
{
  ObIVFMergeArtifact();
  virtual ~ObIVFMergeArtifact() = default;
  virtual bool is_valid() const override
  {
    return finalized_ && vector_count_ > 0 && cluster_count_ > 0;
  }
  bool finalized_;
  ObIVFProcessType selected_type_;
  ObIVFProcessType effective_type_;
  ObIVFQualityFallback fallback_reason_;
  int64_t stage_elapsed_us_[static_cast<int>(ObIVFMergeStage::MAX)];
  int64_t vector_count_;
  int64_t cluster_count_;
  int64_t secondary_assignment_count_;
  TO_STRING_KV(K_(finalized), K_(selected_type), K_(effective_type), K_(fallback_reason), K_(vector_count), K_(cluster_count),
      K_(secondary_assignment_count));
};


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_INTERFACES_H_
