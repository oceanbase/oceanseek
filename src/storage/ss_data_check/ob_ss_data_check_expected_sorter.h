/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_SORTER_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_SORTER_H_

#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_bit_set.h"
#include "lib/utility/utility.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/ss_data_check/ob_ss_data_check_define.h"

namespace oceanbase
{
namespace storage
{

// 仅在内存排序和归并期间使用的视图。relative_path_ 不参与序列化，
// 其存储由 run arena 或单路归并 iterator 持有。
class ObSSDataCheckExpectedSortItem final
{
public:
  ObSSDataCheckExpectedSortItem();
  ~ObSSDataCheckExpectedSortItem() = default;

  int init(
      const ObSSDataCheckExpectedItem &item,
      const common::ObString &relative_path);
  void reset();
  bool is_valid() const;
  bool is_same_key(const ObSSDataCheckExpectedSortItem &other) const;
  bool is_same_object(const ObSSDataCheckExpectedSortItem &other) const;

  const ObSSDataCheckExpectedItem &get_item() const { return item_; }
  const common::ObString &get_relative_path() const { return relative_path_; }

  TO_STRING_KV(K_(item), K_(relative_path));

private:
  ObSSDataCheckExpectedItem item_;
  common::ObString relative_path_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedSortItem);
};

struct ObSSDataCheckExpectedSortItemCompare final
{
public:
  explicit ObSSDataCheckExpectedSortItemCompare(int &result_code)
    : result_code_(result_code)
  {}
  ~ObSSDataCheckExpectedSortItemCompare() = default;

  bool operator()(
      const ObSSDataCheckExpectedSortItem *left,
      const ObSSDataCheckExpectedSortItem *right) const;

public:
  int &result_code_;
};

// 每个输入文件对应一个 iterator，只缓存该 reader 当前记录的相对路径。
class ObSSDataCheckExpectedPathIterator final
    : public ObFragmentIterator<ObSSDataCheckExpectedSortItem>
{
  typedef ObFragmentReaderV2<ObSSDataCheckExpectedItem> ExpectedReader;
  typedef ObFragmentIterator<ObSSDataCheckExpectedItem> ExpectedItemIterator;
public:
  explicit ObSSDataCheckExpectedPathIterator(const int64_t page_size);
  virtual ~ObSSDataCheckExpectedPathIterator();

  int init_for_pread(
      const ObSSDataCheckSortedFileInfo &input,
      const int64_t io_expire_timestamp,
      const uint64_t tenant_id,
      const int64_t file_buf_size);
  virtual int get_next_item(const ObSSDataCheckExpectedSortItem *&item) override;
  virtual int prefetch() override;
  virtual int clean_up() override;
  void reset();

private:
  bool is_inited_;
  ExpectedReader reader_;
  ExpectedItemIterator *reader_iter_;
  char relative_path_buf_[common::OB_MAX_URI_LENGTH];
  ObSSDataCheckExpectedSortItem current_item_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedPathIterator);
};

struct ObSSDataCheckExpectedMergeParam final
{
  ObSSDataCheckExpectedMergeParam(
      const common::ObIArray<ObSSDataCheckSortedFileInfo> &inputs,
      const int64_t start,
      const int64_t end,
      const uint64_t tenant_id,
      const int64_t file_buf_size,
      const int64_t io_expire_timestamp,
      const int64_t dir_id,
      ObMarkSweepTmpFileMgr &tmp_file_mgr);
  ~ObSSDataCheckExpectedMergeParam() = default;
  bool is_valid() const;
  TO_STRING_KV("input_count", inputs_.count(), K_(start), K_(end), K_(tenant_id),
      K_(file_buf_size), K_(io_expire_timestamp), K_(dir_id));

  const common::ObIArray<ObSSDataCheckSortedFileInfo> &inputs_;
  int64_t start_;
  int64_t end_;
  uint64_t tenant_id_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;
  int64_t dir_id_;
  ObMarkSweepTmpFileMgr &tmp_file_mgr_;
};

class ObSSDataCheckExpectedSortUtil final
{
public:
  static bool is_valid_file_buffer_size(const int64_t file_buf_size);
  static int build_relative_path(
      const blocksstable::MacroBlockId &macro_id,
      char *buf,
      const int64_t buf_len,
      common::ObString &relative_path);
  static int merge_expected_files(
      const ObSSDataCheckExpectedMergeParam &param,
      ObSSDataCheckSortedFileInfo &output);

private:
  typedef ObSSDataCheckExpectedPathIterator ExpectedReader;
  typedef ObFragmentIterator<ObSSDataCheckExpectedSortItem> ExpectedIterator;
  typedef ObFragmentMerge<
      ObSSDataCheckExpectedSortItem,
      ObSSDataCheckExpectedSortItemCompare> ExpectedMerge;
  typedef ObFragmentWriterV2<ObSSDataCheckExpectedItem> ExpectedWriter;

  static int init_merge_readers_(
      const ObSSDataCheckExpectedMergeParam &param,
      common::ObArenaAllocator &reader_allocator,
      common::ObSEArray<ExpectedReader *, 64> &reader_holders,
      common::ObSEArray<ExpectedIterator *, 64> &readers);
  static int merge_and_write_expected_files_(
      const ObSSDataCheckExpectedMergeParam &param,
      const common::ObIArray<ExpectedIterator *> &readers,
      ObSSDataCheckSortedFileInfo &output);
  static int write_merged_items_(
      ExpectedMerge &merge,
      ExpectedWriter &writer,
      int64_t &written_count);
  static void destroy_merge_readers_(
      common::ObSEArray<ExpectedReader *, 64> &reader_holders);
};

// 内存受限的有序段写入器。每个刷盘段按规范化对象键排序并去重，
// 再通过临时文件 Handle 发布。
class ObSSDataCheckExpectedRunWriter final
{
public:
  ObSSDataCheckExpectedRunWriter();
  ~ObSSDataCheckExpectedRunWriter();

  int init(
      const uint64_t tenant_id,
      const int64_t memory_limit,
      const int64_t file_buf_size,
      const int64_t io_expire_timestamp,
      ObMarkSweepTmpFileMgr *tmp_file_mgr);
  int add_item(const ObSSDataCheckExpectedItem &item);
  int finish(common::ObIArray<ObSSDataCheckSortedFileInfo> &runs);
  void reset();

private:
  int flush_run_();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;
  ObMarkSweepTmpFileMgr *tmp_file_mgr_;
  int64_t dir_id_;
  common::ObArenaAllocator mem_allocator_;
  common::ObVector<ObSSDataCheckExpectedSortItem *> item_list_;
  common::ObSEArray<ObSSDataCheckSortedFileInfo, 16> runs_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedRunWriter);
};

// 单分组增量多趟归并器。不同批次可以并行执行，
// prepare_next_pass() 必须由 DAG 屏障保护。
class ObSSDataCheckExpectedMerger final
{
public:
  static const int64_t MAX_FAN_IN =
      ObFragmentMerge<
          ObSSDataCheckExpectedSortItem,
          ObSSDataCheckExpectedSortItemCompare>::DEFAULT_ITERATOR_NUM;

  ObSSDataCheckExpectedMerger();
  ~ObSSDataCheckExpectedMerger();

  int init(
      const uint64_t tenant_id,
      const int64_t memory_limit,
      const int64_t file_buf_size,
      const int64_t io_expire_timestamp,
      ObMarkSweepTmpFileMgr *tmp_file_mgr);
  int add_inputs(const common::ObIArray<ObSSDataCheckSortedFileInfo> &inputs);
  int start_merge();
  int get_current_pass_idx(int64_t &pass_idx) const;
  int get_input_count(const int64_t pass_idx, int64_t &count) const;
  int get_batch_count(const int64_t pass_idx, int64_t &count) const;
  int get_batch_range(
      const int64_t pass_idx,
      const int64_t batch_idx,
      int64_t &start,
      int64_t &end) const;
  int batch_merge(const int64_t pass_idx, const int64_t batch_idx);
  int prepare_next_pass(const int64_t pass_idx);
  int take_final_input(
      const int64_t pass_idx,
      ObSSDataCheckSortedFileInfo &result);
  int cleanup();

private:
  typedef common::ObSEArray<ObSSDataCheckSortedFileInfo, 64> FileArray;

  int single_pass_merge_(
      const common::ObIArray<ObSSDataCheckSortedFileInfo> &inputs,
      const int64_t start,
      const int64_t end,
      ObSSDataCheckSortedFileInfo &output);
  int get_batch_count_locked_(int64_t &count) const;
  int get_batch_range_locked_(
      const int64_t batch_idx,
      int64_t &start,
      int64_t &end) const;
  FileArray &get_current_inputs_();
  const FileArray &get_current_inputs_() const;
  FileArray &get_next_inputs_();
  const FileArray &get_next_inputs_() const;

private:
  bool is_inited_;
  bool is_transitioning_;
  uint64_t tenant_id_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;
  ObMarkSweepTmpFileMgr *tmp_file_mgr_;
  int64_t dir_id_;
  int64_t fan_in_;
  int64_t current_pass_idx_;
  int64_t current_input_set_idx_;
  int64_t inflight_batch_count_;
  FileArray input_sets_[2];
  common::ObBitSet<> completed_batches_;
  mutable common::ObSpinLock input_lock_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedMerger);
};

class ObSSDataCheckExpectedSorter;

// 协调各生产者持有的排序器和唯一的 EXPECTED 归并组。
// 临时文件管理器借用自本轮 Context，其生命周期必须长于流水线，
// 以及 get_sorted_file() 返回的所有文件 Handle。
class ObSSDataCheckExpectedSortPipeline final
{
public:
  ObSSDataCheckExpectedSortPipeline();
  ~ObSSDataCheckExpectedSortPipeline();

  int init(
      const uint64_t tenant_id,
      const int64_t memory_limit,
      const int64_t file_buf_size,
      const int64_t io_expire_timestamp,
      ObMarkSweepTmpFileMgr *tmp_file_mgr);
  int add_files(const common::ObIArray<ObSSDataCheckSortedFileInfo> &files);

  int prepare_merge();
  int get_current_pass_idx(int64_t &pass_idx) const;
  int get_input_count(const int64_t pass_idx, int64_t &count) const;
  int get_batch_count(const int64_t pass_idx, int64_t &count) const;
  int get_batch_range(
      const int64_t pass_idx,
      const int64_t batch_idx,
      int64_t &start,
      int64_t &end) const;
  int batch_merge(const int64_t pass_idx, const int64_t batch_idx);
  int prepare_next_pass(const int64_t pass_idx);
  int complete_merge(const int64_t pass_idx);

  // Empty expected input returns success with an invalid/default file info.
  int get_sorted_file(ObSSDataCheckSortedFileInfo &result) const;
  int get_sorted_item_count(int64_t &count) const;
  int get_sorted_file_size(int64_t &size) const;
  int cleanup();

  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_memory_limit() const { return memory_limit_; }
  int64_t get_file_buf_size() const { return file_buf_size_; }
  int64_t get_io_expire_timestamp() const { return io_expire_timestamp_; }

private:
  enum class State : uint8_t
  {
    NOT_INIT = 0,
    COLLECTING,
    PREPARING,
    MERGING,
    COMPLETING,
    CLEANING,
    COMPLETE,
    FAILED
  };

  friend class ObSSDataCheckExpectedSorter;
  int register_sorter_();
  void unregister_sorter_();
  int register_merge_op_() const;
  void unregister_merge_op_() const;

private:
  State state_;
  uint64_t tenant_id_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;
  ObMarkSweepTmpFileMgr *tmp_file_mgr_;
  mutable common::ObSpinLock file_lock_;
  common::ObSEArray<ObSSDataCheckSortedFileInfo, 64> files_;
  ObSSDataCheckExpectedMerger merger_;
  ObSSDataCheckSortedFileInfo sorted_file_;
  int64_t active_sorter_count_;
  mutable int64_t active_merge_op_count_;
  int64_t completed_pass_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedSortPipeline);
};

// 生产者本地排序器。finish() 将所有生成的有序段原子地移交给共享流水线，
// 无论成功还是失败，都会注销该排序器。
class ObSSDataCheckExpectedSorter final
{
public:
  ObSSDataCheckExpectedSorter();
  ~ObSSDataCheckExpectedSorter();

  int init(ObSSDataCheckExpectedSortPipeline *pipeline);
  int add_item(const ObSSDataCheckExpectedItem &item);
  int finish();
  void reset();

private:
  bool is_inited_;
  ObSSDataCheckExpectedSortPipeline *pipeline_;
  ObSSDataCheckExpectedRunWriter writer_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedSorter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_SORTER_H_
