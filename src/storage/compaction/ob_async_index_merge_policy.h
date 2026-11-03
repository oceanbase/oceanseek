/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_ASYNC_INDEX_MERGE_POLICY_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_ASYNC_INDEX_MERGE_POLICY_H_

#include "lib/container/ob_array.h"
#include "common/ob_range.h"
#include "common/ob_tablet_id.h"
#include "storage/compaction/ob_partition_merge_policy.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObAsyncIndexParams;
} // namespace schema
} // namespace share
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{
class ObLS;
class ObTablet;
class ObTabletHandle;
class ObSSTableArray;
class ObITable;
}
namespace compaction
{

struct ObAsyncIndexSSTableMergeInfo
{
public:
  ObAsyncIndexSSTableMergeInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(start_snapshot_version),
               K_(end_snapshot_version),
               K_(seg_size),
               K_(level));

public:
  int64_t start_snapshot_version_;
  int64_t end_snapshot_version_;
  int64_t seg_size_;
  float level_;
};

class ObAsyncIndexMergePolicyHelper
{
public:
  // Retain crossing IncMajor ranges for boundary alignment.
  static int collect_merge_infos(
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      const int64_t schedule_snapshot_version,
      common::ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos);
  static int get_inc_major_sstables(
      storage::ObLS &ls,
      const storage::ObTablet &tablet,
      const int64_t snapshot,
      const int64_t last_major_snapshot,
      storage::ObGetMergeTablesResult &result);
  static int select_inc_major_sstables(
      storage::ObLS &ls,
      const storage::ObSSTableArray &inc_major_sstables,
      const storage::ObStorageMetaHandle &meta_handle,
      const int64_t snapshot,
      const int64_t last_major_snapshot,
      storage::ObGetMergeTablesResult &result);
  static int calc_schedule_snapshot_version(
      storage::ObLS &ls,
      const common::ObIArray<common::ObTabletID> &index_tablet_ids,
      int64_t &schedule_snapshot_version);
  // Lower the schedule snapshot if it cuts an aligned window, and retain complete windows.
  static int align_merge_infos(
      const common::ObIArray<common::ObSEArray<ObAsyncIndexSSTableMergeInfo, 8>> &all_merge_infos,
      int64_t &schedule_snapshot_version,
      common::ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos);
  static int calc_delete_percentage(
      storage::ObLS &ls,
      const storage::ObTablet &data_tablet,
      const common::ObTabletID &delete_index_tablet_id,
      int64_t &delete_percentage);

private:
  // Check selected IncMajor ranges for overlap with the base Major or the
  // previous range. Logical gaps and empty input are allowed; the selector
  // determines merge boundaries.
  static int check_inc_major_sstables_(
      const storage::ObTablesHandleArray &tables_handle,
      const int64_t start_idx,
      const int64_t last_major_snapshot);
  static int calc_tablet_visible_row_count_(
      const storage::ObTablet &tablet,
      int64_t &row_count);
  static int get_all_tables_(
      const storage::ObSSTableArray &sstable_array,
      common::ObIArray<storage::ObITable *> &tables);
  static int push_sstable_merge_info_(
      blocksstable::ObSSTable *table,
      common::ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos);
};

class ObAsyncIndexMergePolicyBase
{
public:
  ObAsyncIndexMergePolicyBase();
  virtual ~ObAsyncIndexMergePolicyBase();

  virtual int find_merges(
      common::ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      common::ObNewVersionRange &merge_range) = 0;

protected:
  int validate_merge_infos_(const common::ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos) const;

  bool is_inited_;
};

// Async index merge policy aligned with Elasticsearch Lucene LogMergePolicy.
//
// Input: merge_infos —  SSTables ordered by snapshot version (each segment's
//   [start_snapshot_version, end_snapshot_version) must chain without overlap).
// Output: on success, merge_range covers (base_version_, snapshot_version] spanning the
//   chosen segment range; returns OB_NO_NEED_MERGE when no merge is warranted.
//
// Algorithm (two-level, same structure as LogMergePolicy.findMerges):
//   1. Level assignment — level = log(seg_size) / log(merge_factor) for each segment.
//   2. Outer loop — scan from index 0; for each band [start, n):
//        a. max_level = max level in the suffix;
//        b. level_bottom = max_level - LEVEL_LOG_SPAN (or 2×span when max_level <= level_floor);
//        c. shrink upto from the tail while level < level_bottom;
//        d. call pick_merge_at_level on [start, upto]; advance start = upto + 1 if not found.
//   3. Inner sliding window (pick_merge_at_level) — step windows of merge_factor segments:
//        a. cap window by max_merge_size (truncate or skip singleton-too-large);
//        b. if merge_factor segments packed but total < min_merge_size_, extend window forward;
//        c. accept the first window with > 1 segment; singleton merges are never emitted.
//
class ObAsyncIndexMergePolicy : public ObAsyncIndexMergePolicyBase
{
public:
  static constexpr int64_t DEFAULT_MERGE_FACTOR = 10;
  static constexpr float LEVEL_LOG_SPAN = 0.75f;
  static constexpr int64_t DEFAULT_MIN_MERGE_SIZE = 15000;
  static constexpr int64_t DEFAULT_MAX_MERGE_SIZE = 15000000L;

  static int choose_async_index_merge_range(
    storage::ObLS &ls,
    const storage::ObTablet &data_tablet,
    const common::ObIArray<common::ObTabletID> &index_tablet_ids,
    const common::ObTabletID &delete_index_tablet_id,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason,
    const share::schema::ObAsyncIndexParams &async_index_params,
    common::ObNewVersionRange &merge_version_range);
  static double calc_level(const int64_t seg_size, const int64_t merge_factor);

public:
  ObAsyncIndexMergePolicy();
  int init(const int64_t merge_segment_threshold);

  virtual int find_merges(
      common::ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      common::ObNewVersionRange &merge_range) override;

private:
  int calc_and_fill_levels_(common::ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos) const;
  double calc_level_floor_() const;
  int pick_merge_at_level_(
      const common::ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      const int32_t level_start,
      const int32_t upto,
      int32_t &merge_start_idx,
      int32_t &merge_end_idx) const;

  int64_t merge_factor_;
  int64_t min_merge_size_;
  int64_t max_merge_size_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_COMPACTION_OB_ASYNC_INDEX_MERGE_POLICY_H_
