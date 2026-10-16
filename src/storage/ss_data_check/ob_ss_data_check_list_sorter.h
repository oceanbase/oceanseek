/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_SORTER_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_SORTER_H_

#include "common/storage/ob_device_common.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_check_stop_provider.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/ss_data_check/ob_ss_data_check_define.h"

namespace oceanbase
{
namespace storage
{

class ObSSDataCheckCtx;

// LIST 后端回调中的 dirent 只在当前调用内有效，因此 Actual item 必须深拷贝名称。
class ObSSDataCheckListedObjectItem final
{
  static const int32_t DATA_CHECK_LISTED_OBJECT_ITEM_VERSION = 1;
  OB_UNIS_VERSION(DATA_CHECK_LISTED_OBJECT_ITEM_VERSION);
public:
  ObSSDataCheckListedObjectItem();
  ~ObSSDataCheckListedObjectItem() = default;

  int init(const common::ObString &name);
  void reset();
  bool is_valid() const;
  int64_t get_deep_copy_size() const;
  int deep_copy(
      const ObSSDataCheckListedObjectItem &src,
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  const common::ObString &get_name() const { return name_; }

  TO_STRING_KV(K_(name));

private:
  common::ObString name_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckListedObjectItem);
};

struct ObSSDataCheckListedObjectItemCompare final
{
public:
  explicit ObSSDataCheckListedObjectItemCompare(int &result_code)
    : result_code_(result_code)
  {}
  ~ObSSDataCheckListedObjectItemCompare() = default;

  bool operator()(
      const ObSSDataCheckListedObjectItem *left,
      const ObSSDataCheckListedObjectItem *right) const;

  int &result_code_;
};

// 每个父 prefix 独立排序。小集合直接在内存中排序，超过内存上限后使用
// Context 的临时文件管理器生成有序 run，并在回放前完成多趟归并。
class ObSSDataCheckListedObjectSorter final
    : public common::ObBaseDirEntryOperator
{
  typedef ObFragmentReaderV2<ObSSDataCheckListedObjectItem> ListedObjectReader;
  typedef ObFragmentIterator<ObSSDataCheckListedObjectItem> ListedObjectIterator;
  typedef ObFragmentMerge<
      ObSSDataCheckListedObjectItem,
      ObSSDataCheckListedObjectItemCompare> ListedObjectMerge;
  typedef ObFragmentWriterV2<ObSSDataCheckListedObjectItem> ListedObjectWriter;
  typedef common::ObSEArray<ObSSDataCheckSortedFileInfo, 16> RunSet;
public:
  static const int64_t CHECK_STOP_INTERVAL = 1024;
  static const int64_t MAX_FAN_IN =
      ListedObjectMerge::DEFAULT_ITERATOR_NUM;
  static const int64_t MAX_PENDING_RUN_COUNT = MAX_FAN_IN;
  // 64 进制 11 层可覆盖 int64_t 能表达的全部非负 run 数量。
  static const int64_t MAX_COMPACT_LEVEL_COUNT = 11;

  ObSSDataCheckListedObjectSorter();
  virtual ~ObSSDataCheckListedObjectSorter();

  int init(
      const uint64_t tenant_id,
      const int64_t memory_limit,
      const int64_t file_buf_size,
      const int64_t io_expire_timestamp,
      const share::ObCheckStopProvider &stop_checker,
      ObSSDataCheckCtx &ctx);
  virtual int func(const dirent *entry) override;
  int sort_and_replay(common::ObBaseDirEntryOperator &op);
  void reset();

private:
  int add_name_(const common::ObString &name);
  int sort_memory_items_();
  int replay_memory_items_(common::ObBaseDirEntryOperator &op);
  int replay_file_(
      const ObSSDataCheckSortedFileInfo &file,
      common::ObBaseDirEntryOperator &op);
  int replay_item_(
      const ObSSDataCheckListedObjectItem &item,
      common::ObBaseDirEntryOperator &op);
  int flush_run_();
  int compact_runs_if_needed_(const int64_t start_level_idx);
  int collect_runs_for_final_merge_();
  int merge_all_runs_(ObSSDataCheckSortedFileInfo &output);
  int merge_run_set_(
      RunSet &inputs,
      ObSSDataCheckSortedFileInfo &output);
  int merge_batch_(
      const RunSet &inputs,
      const int64_t start,
      const int64_t end,
      ObSSDataCheckSortedFileInfo &output);
  int init_merge_readers_(
      const RunSet &inputs,
      const int64_t start,
      const int64_t end,
      common::ObArenaAllocator &reader_allocator,
      common::ObSEArray<ListedObjectReader *, 64> &reader_holders,
      common::ObSEArray<ListedObjectIterator *, 64> &readers);
  int merge_and_write_(
      const common::ObIArray<ListedObjectIterator *> &readers,
      ObSSDataCheckSortedFileInfo &output);
  int write_merged_items_(
      ListedObjectMerge &merge,
      ListedObjectWriter &writer,
      int64_t &written_count);
  int ensure_dir_();
  int check_stop_(const int64_t processed_count) const;
  bool has_file_runs_() const;
  void clear_memory_items_();
  void destroy_merge_readers_(
      common::ObSEArray<ListedObjectReader *, 64> &reader_holders);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;
  int64_t dir_id_;
  int64_t fan_in_;
  int64_t in_memory_size_;
  int64_t collected_count_;
  const share::ObCheckStopProvider *stop_checker_;
  ObSSDataCheckCtx *ctx_;
  common::ObArenaAllocator mem_allocator_;
  common::ObArray<ObSSDataCheckListedObjectItem *> item_list_;
  RunSet run_levels_[MAX_COMPACT_LEVEL_COUNT];
  RunSet final_runs_;
  RunSet merge_work_runs_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckListedObjectSorter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_SORTER_H_
