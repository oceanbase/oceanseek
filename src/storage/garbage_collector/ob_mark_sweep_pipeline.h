/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_MARK_SWEEP_PIPELINE_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_MARK_SWEEP_PIPELINE_H_

#include "storage/ob_parallel_external_sort.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/utility.h"
#include "storage/garbage_collector/ob_mark_sweep_tmp_file_mgr.h"

namespace oceanbase
{
namespace storage
{

/** External merge sort over temporary files (with dedup + sorted set difference).
 *
 * Four classes:
 *   ObSortedRunWriter   -- Stage 1: streaming external sort writer (sort + dedup -> temp file)
 *   ObMultiPassMerger   -- Stage 2: multi-pass K-way merge (with dedup)
 *   ObSortedSetDiff     -- Stage 3: sorted set difference (lazy iterator)
 *   ObMarkSweepPipeline -- Top-level orchestrator (chains the three stages)
 *
 * The pipeline operates on ObGCMacroBlockItem (defined below) compared by
 * ObGCMacroBlockCompare.
 */

// Group of items processed by mark-sweep pipeline. DEL items are candidates
// for garbage collection; KEPT items are still referenced. The final diff
// (DEL - KEPT) yields reclaimable items.
enum class ObMarkSweepGroupType : uint8_t
{
  DEL = 0,
  KEPT = 1
};


// ====================================================================================
// The Item wraps a MacroBlockId and is the unit handled by the pipeline.
// ====================================================================================
struct ObGCMacroBlockItem
{
public:
  ObGCMacroBlockItem() : macro_id_() {}
  explicit ObGCMacroBlockItem(const blocksstable::MacroBlockId &id) : macro_id_(id) {}
  virtual ~ObGCMacroBlockItem() = default;

  bool is_valid() const { return macro_id_.is_valid(); }
  bool operator==(const ObGCMacroBlockItem &other) const { return macro_id_ == other.macro_id_; }
  bool operator!=(const ObGCMacroBlockItem &other) const { return !(macro_id_ == other.macro_id_); }
  bool operator<(const ObGCMacroBlockItem &other) const { return macro_id_ < other.macro_id_; }
  ObGCMacroBlockItem &operator=(const ObGCMacroBlockItem &other)
  {
    if (this != &other) {
      macro_id_ = other.macro_id_;
    }
    return *this;
  }
  void reset() { macro_id_.reset(); }

  int64_t get_serialize_size() const { return macro_id_.get_serialize_size(); }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  { return macro_id_.serialize(buf, buf_len, pos); }
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  { return macro_id_.deserialize(buf, data_len, pos); }

  int64_t get_deep_copy_size() const { return 0; }
  int deep_copy(const ObGCMacroBlockItem &src, char *buf, int64_t len, int64_t &pos)
  {
    UNUSEDx(buf, len, pos);
    macro_id_ = src.macro_id_;
    return common::OB_SUCCESS;
  }

  TO_STRING_KV(K_(macro_id));

public:
  blocksstable::MacroBlockId macro_id_;
};


struct ObGCMacroBlockCompare
{
public:
  ObGCMacroBlockCompare() : result_code_(common::OB_SUCCESS) {}
  virtual ~ObGCMacroBlockCompare() = default;
  bool operator()(const ObGCMacroBlockItem *left, const ObGCMacroBlockItem *right)
  {
    bool bret = false;
    if (OB_ISNULL(left) || OB_ISNULL(right) || !left->is_valid() || !right->is_valid()) {
      result_code_ = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG_RET(ERROR, result_code_, "invalid argument", KP(left), KP(right));
    } else {
      bret = (*left < *right);
    }
    return bret;
  }

public:
  int result_code_;
};


// ==================== Info describing a sorted file ====================
struct ObSortedFileInfo
{
public:
  ObSortedFileInfo() : file_handle_(), sample_item_(), item_count_(0) {}
  virtual ~ObSortedFileInfo() { reset(); }
  bool is_valid() const { return file_handle_.is_valid(); }
  int assign(const ObSortedFileInfo &other);
  void reset()
  {
    file_handle_.reset();
    sample_item_.reset();
    item_count_ = 0;
  }
  int64_t get_fd() const { return file_handle_.get_fd(); }
  int64_t get_dir_id() const { return file_handle_.get_dir_id(); }
  TO_STRING_KV(K_(file_handle), K_(item_count));

public:
  ObMarkSweepTmpFileHandle file_handle_;
  ObGCMacroBlockItem sample_item_;
  int64_t item_count_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSortedFileInfo);
};


// ====================================================================================
// ObSortedRunWriter
//   接收无序输入，在内存中排序并去重后，刷出为有序 run。
//   所有 run 都会作为独立的有序去重文件输出（每个 run 对应一个文件）。
//   跨 run 的去重延迟到全局流水线归并（ObMultiPassMerger）中处理。
// ====================================================================================
class ObSortedRunWriter
{
public:
  ObSortedRunWriter();
  virtual ~ObSortedRunWriter();

  int init(const uint64_t tenant_id,
           const int64_t memory_limit,
           const int64_t file_buf_size,
           const int64_t expire_timestamp,
           ObGCMacroBlockCompare *compare,
           ObMarkSweepTmpFileMgr *tmp_file_mgr);
  int add_item(const ObGCMacroBlockItem &item);
  // Accumulated add-triggered flush cost; excludes finish and is cleared by reset.
  int64_t get_flush_sorted_run_cost_us() const { return flush_sorted_run_cost_us_; }
  // 将剩余的内存数据刷出后，把所有有序 run 作为独立文件返回（不做最终归并）。
  // 小输入对应 1 个 run、1 个文件；大输入对应 N 个 run、N 个文件。
  // 跨 run 的去重延迟到全局流水线归并中处理。
  int finish(common::ObIArray<ObSortedFileInfo> &results);
  void reset();

private:
  int flush_run_();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t expire_timestamp_;
  ObGCMacroBlockCompare *compare_;
  ObMarkSweepTmpFileMgr *tmp_file_mgr_;
  int64_t dir_id_;

  common::ObArenaAllocator mem_allocator_;
  common::ObVector<ObGCMacroBlockItem *> item_list_;

  common::ObSEArray<ObSortedFileInfo, 16> runs_;
  bool has_data_;
  int64_t flush_sorted_run_cost_us_;
  DISALLOW_COPY_AND_ASSIGN(ObSortedRunWriter);
};


// ====================================================================================
// ObMultiPassMerger
//   Multiple sorted files -> multi-pass K-way merge (with dedup)
//   -> single globally sorted, deduplicated file
// ====================================================================================
class ObMultiPassMerger
{
public:
  ObMultiPassMerger();
  virtual ~ObMultiPassMerger();

  int init(const uint64_t tenant_id,
           const int64_t file_buf_size,
           const int64_t expire_timestamp,
           ObGCMacroBlockCompare *compare,
           ObMarkSweepTmpFileMgr *tmp_file_mgr);
  int add_input(const common::ObIArray<ObSortedFileInfo> &file_infos);
  void cleanup();

  // Incremental multi-pass merge API, driven by parallel batch tasks.
  // Per-pass loop pattern:
  //   while (get_input_count() > 1) {
  //     for (i in [0, get_batch_count())) {
  //       int64_t s, e; get_batch_range(i, s, e);
  //       batch_merge(s, e);                  // appends one merged file to next_inputs_
  //     }
  //     prepare_next_pass();                  // swap cur_inputs_ <-> next_inputs_
  //   }
  //   take_final_input(result);              // when count <= 1
  int64_t get_input_count() const;
  int64_t get_batch_count() const;            // ceil(cur_inputs_.count() / MAX_FAN_IN)
  int get_batch_range(const int64_t batch_idx, int64_t &start, int64_t &end) const;
  int batch_merge(const int64_t start, const int64_t end);
  int prepare_next_pass();                      // move next_inputs_ -> cur_inputs_; clear next_inputs_
  int take_final_input(ObSortedFileInfo &result);  // input_count must be <= 1

private:
  int single_pass_merge_(const common::ObIArray<ObSortedFileInfo> &inputs,
                         const int64_t start_idx,
                         const int64_t end_idx,
                         ObSortedFileInfo &output);

public:
  static const int64_t MAX_FAN_IN =
      ObFragmentMerge<ObGCMacroBlockItem, ObGCMacroBlockCompare>::DEFAULT_ITERATOR_NUM;

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t file_buf_size_;
  int64_t expire_timestamp_;
  ObGCMacroBlockCompare *compare_;
  ObMarkSweepTmpFileMgr *tmp_file_mgr_;
  int64_t dir_id_;

  common::ObSEArray<ObSortedFileInfo, 64> cur_inputs_;
  // Per-pass output container. Filled by parallel batch_merge() calls (lock-protected),
  // then promoted to cur_inputs_ by prepare_next_pass().
  common::ObSEArray<ObSortedFileInfo, 64> next_inputs_;
  // Guards cur_inputs_ and next_inputs_.
  mutable common::ObSpinLock input_lock_;
  DISALLOW_COPY_AND_ASSIGN(ObMultiPassMerger);
};


// ====================================================================================
// ObSortedSetDiff
//   Compute the set difference {left} - {right}.
//   left and right are two sorted, deduplicated files.
//   using a lazy-iterator model.
// ====================================================================================
class ObSortedSetDiff
{
public:
  ObSortedSetDiff();
  virtual ~ObSortedSetDiff();

  int init(const ObSortedFileInfo &left,
           const ObSortedFileInfo &right,
           const uint64_t tenant_id,
           const int64_t file_buf_size,
           const int64_t expire_timestamp);
  int get_next_item(const ObGCMacroBlockItem *&item);
  void cleanup();

private:
  static int prepare_(const uint64_t tenant_id,
                      const int64_t file_buf_size,
                      const int64_t expire_timestamp,
                      const ObSortedFileInfo &info,
                      ObFragmentReaderV2<ObGCMacroBlockItem> &reader,
                      const ObGCMacroBlockItem *&item,
                      int &iter_ret);

private:
  bool is_inited_;
  ObSortedFileInfo left_file_;
  ObSortedFileInfo right_file_;
  ObFragmentReaderV2<ObGCMacroBlockItem> left_reader_;
  ObFragmentReaderV2<ObGCMacroBlockItem> right_reader_;
  const ObGCMacroBlockItem *left_item_;
  const ObGCMacroBlockItem *right_item_;
  int left_ret_;
  int right_ret_;
  ObGCMacroBlockItem output_item_; // keep a copy of the output value to avoid pointer aliasing
  DISALLOW_COPY_AND_ASSIGN(ObSortedSetDiff);
};


class ObMarkSweepSorter;


// ====================================================================================
// ObMarkSweepPipeline
//   Top-level orchestrator that chains the three pipeline stages together.
// ====================================================================================
class ObMarkSweepPipeline
{
public:
  ObMarkSweepPipeline();
  virtual ~ObMarkSweepPipeline();

  int init(const uint64_t tenant_id,
           const int64_t memory_limit,
           const int64_t file_buf_size,
           const int64_t expire_timestamp,
           ObGCMacroBlockCompare *compare);

  // ===== Stage 1: file submission (thread-safe; called by ObMarkSweepSorter) =====
  int add_file(const ObMarkSweepGroupType group, const ObSortedFileInfo &file_info);

  // ===== Stage 2: global merge =====
  // Precondition: all files have been submitted (no active workers remain).
  //
  // Driven by parallel dag tasks: prepare_merge() seeds the mergers, then
  // multiple batch tasks call batch_merge(group, ...) in parallel,
  // a pass ctrl task calls prepare_next_pass(group) between rounds and
  // complete_group(group) at the terminal round, finally complete_merge()
  // seals the result.
  int prepare_merge();
  int get_input_count(const ObMarkSweepGroupType group, int64_t &count) const;
  int get_batch_count(const ObMarkSweepGroupType group, int64_t &count) const;
  int get_batch_range(const ObMarkSweepGroupType group,
                      const int64_t batch_idx,
                      int64_t &start, int64_t &end) const;
  int batch_merge(const ObMarkSweepGroupType group,
                  const int64_t start, const int64_t end);
  int prepare_next_pass(const ObMarkSweepGroupType group);
  // Pull merged_*_ from the underlying merger; only valid when
  // get_input_count(group, ...) returns <= 1 (caller's responsibility).
  int complete_group(const ObMarkSweepGroupType group);
  // Final stage of merge: init diff iterator, release submitted file lists.
  int complete_merge();

  // ===== Stage 3: set-difference iteration =====
  int init_diff_iter();
  int get_next_diff_item(const ObGCMacroBlockItem *&item);

  // Deduplicated item count of the final merged file for a group (merged_del_/merged_kept_).
  int get_merged_item_count(const ObMarkSweepGroupType group, int64_t &count) const;
  // size of the final merged tmp file for a group (merged_del_/merged_kept_).
  // Returns 0 when the group has no merged file (empty group).
  int get_merged_file_size(const ObMarkSweepGroupType group, int64_t &size) const;

  void cleanup();

  // Parameter accessors (used by ObMarkSweepSorter to fetch init parameters)
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_memory_limit() const { return memory_limit_; }
  int64_t get_file_buf_size() const { return file_buf_size_; }
  int64_t get_expire_timestamp() const { return expire_timestamp_; }
  ObGCMacroBlockCompare *get_compare() const { return compare_; }

private:
  friend class ObMarkSweepSorter;
  ObMultiPassMerger &get_merger_(const ObMarkSweepGroupType group);
  const ObMultiPassMerger &get_merger_(const ObMarkSweepGroupType group) const;

  bool is_inited_;
  uint64_t tenant_id_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t expire_timestamp_;
  ObGCMacroBlockCompare *compare_;
  ObMarkSweepTmpFileMgr tmp_file_mgr_;

  // Guards mutable file state: del_files_, kept_files_, merged_del_, merged_kept_.
  mutable common::ObSpinLock file_lock_;
  common::ObSEArray<ObSortedFileInfo, 64> del_files_;
  common::ObSEArray<ObSortedFileInfo, 64> kept_files_;

  // Long-lived per-group multi-pass mergers, driven incrementally by parallel
  // batch tasks (see prepare_merge / complete_group / batch_merge / prepare_next_pass).
  // Initialized in init(); state cleared by cleanup(). Each merger
  // owns its own input_lock_ that serializes cur_inputs_ / next_inputs_ access.
  ObMultiPassMerger del_merger_;
  ObMultiPassMerger kept_merger_;
  ObSortedFileInfo merged_del_;
  ObSortedFileInfo merged_kept_;

  ObSortedSetDiff diff_;
  int64_t active_sorter_count_;  // reference count of active workers (atomic access)
  DISALLOW_COPY_AND_ASSIGN(ObMarkSweepPipeline);
};


// ====================================================================================
// ObMarkSweepSorter
//   Each thread owns one Sorter and independently processes one group
//   (lock-free). The Sorter is group-type agnostic: it only sorts items.
//   The caller submits the result to the Pipeline with the appropriate group type.
//
// Usage pattern:
//   ObMarkSweepPipeline pipeline;
//   pipeline.init(...);
//
//   // on each thread, for one group:
//   ObMarkSweepSorter sorter;
//   sorter.init(&pipeline);
//   sorter.add_item(id);
//   ObSEArray<ObSortedFileInfo, 16> results;
//   sorter.finish(results);                        // one file per spilled run
//   for (i in [0, results.count()))
//     pipeline.add_file(group_type, results.at(i));  // caller routes to the right group
//
//   // after all threads finish, drive the merge stage incrementally:
//   //   pipeline.prepare_merge();
//   //   for each group g in {DEL, KEPT}:
//   //     while pipeline.get_input_count(g, ...) > 1:
//   //       for each batch b: pipeline.batch_merge(g, start, end)
//   //       pipeline.prepare_next_pass(g)
//   //     pipeline.complete_group(g)
//   //   pipeline.complete_merge();
//   pipeline.get_next_diff_item(item);
// ====================================================================================
class ObMarkSweepSorter
{
public:
  ObMarkSweepSorter();
  virtual ~ObMarkSweepSorter();

  int init(ObMarkSweepPipeline *pipeline);
  int add_item(const ObGCMacroBlockItem &item);
  int64_t get_flush_sorted_run_cost_us() const { return writer_.get_flush_sorted_run_cost_us(); }
  // Finish and hand out all sorted runs as separate files (see ObSortedRunWriter::finish).
  int finish(common::ObIArray<ObSortedFileInfo> &results);

private:
  bool is_inited_;
  ObMarkSweepPipeline *pipeline_;
  ObSortedRunWriter writer_;
  DISALLOW_COPY_AND_ASSIGN(ObMarkSweepSorter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_MARK_SWEEP_PIPELINE_H_
