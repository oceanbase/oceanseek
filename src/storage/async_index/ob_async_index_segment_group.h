/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_ASYNC_INDEX_SEGMENT_GROUP_H_
#define OCEANBASE_STORAGE_OB_ASYNC_INDEX_SEGMENT_GROUP_H_

#include "common/ob_range.h"
#include "lib/container/ob_array_array.h"
#include "lib/container/ob_se_array.h"
#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace storage
{

/**
 * @brief A single segment group: SSTable version ranges each tablet must scan within the same
 *        logical snapshot interval.
 *
 * @note tablet_version_ranges_[i] corresponds one-to-one by index with
 *       ObAsyncIndexSegmentGroupCtx::index_tablet_handles_[i].
 */
class ObAsyncIndexSegmentGroup
{
public:
  ObAsyncIndexSegmentGroup();
  void reset();
  int init(const int64_t tablet_cnt);
  int add_version_range(const int64_t tablet_idx, const common::ObNewVersionRange &version_range);
  int64_t get_tablet_cnt() const { return tablet_version_ranges_.count(); }
  const common::ObIArray<common::ObNewVersionRange> &get_tablet_version_ranges(
      const int64_t tablet_idx) const
  { return tablet_version_ranges_.at(tablet_idx); }

  TO_STRING_KV(K_(tablet_version_ranges));

private:
  common::ObArrayArray<common::ObNewVersionRange, 4> tablet_version_ranges_;
};

/**
 * @brief Query context for async index segment groups.
 *
 * @note Holds the effective read snapshot, segment group list, ObTabletHandle for each index
 *       tablet, and their SSTable version range lists (index_version_ranges[i] corresponds
 *       one-to-one by index with index_tablet_handles[i]); retains tablet references until
 *       async_table_scan completes to prevent SSTable GC.
 */
class ObAsyncIndexSegmentGroupCtx
{
public:
  ObAsyncIndexSegmentGroupCtx();
  ~ObAsyncIndexSegmentGroupCtx();
  void reset();
  int add_index_tablet_version_ranges(
      const ObTabletHandle &tablet_handle,
      common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges);
  int add_index_tablet_version_ranges(
      const ObTabletHandle &tablet_handle,
      common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges,
      const int64_t published_version);
  int set_delete_table_published_version(const int64_t published_version);
  int calc_query_version_ranges(const share::SCN &sql_read_snapshot);

  share::SCN get_async_index_read_snapshot() const { return async_index_read_snapshot_; }
  bool is_strict_aligned() const { return is_strict_aligned_; }
  bool is_one_segment() const { return is_strict_aligned_ && groups_.count() == 1; }
  int64_t get_group_count() const { return groups_.count(); }
  const ObAsyncIndexSegmentGroup *get_group(const int64_t idx) const { return groups_.at(idx); }
  int64_t get_index_tablet_count() const { return index_tablet_handles_.count(); }
  const ObTabletHandle *get_index_tablet_handle(const int64_t idx) const
  {
    return idx >= 0 && idx < index_tablet_handles_.count()
        ? &index_tablet_handles_.at(idx) : nullptr;
  }

  TO_STRING_KV(K_(async_index_read_snapshot),
               K_(delete_table_published_version),
               K_(index_physical_published_versions),
               K_(index_published_versions),
               K_(is_strict_aligned),
               K_(groups),
               K_(index_tablet_handles),
               K_(index_version_ranges));

private:
  struct ObNewVersionRangeCompare
  {
    bool operator()(const common::ObNewVersionRange &left,
                    const common::ObNewVersionRange &right) const;
  };

  static int find_version_chain_(
      const common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges,
      int64_t &pos,
      const int64_t merge_start,
      const int64_t merge_end,
      common::ObIArray<common::ObNewVersionRange> &out_chain);
  static bool is_version_chain_complete_(
      const common::ObIArray<common::ObNewVersionRange> &version_chain,
      const int64_t merge_start,
      const int64_t merge_end,
      const int64_t physical_published_version,
      const int64_t query_published_version);
  static int build_query_ranges_(
      const common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges,
      const int64_t read_ver,
      common::ObIArray<common::ObNewVersionRange> &out_ranges);
  static int find_base_tablet_idx_(
      const common::ObArrayArray<common::ObNewVersionRange, 8> &query_lists,
      const common::ObIArray<int64_t> &physical_published_versions,
      int64_t &base_idx,
      bool &is_strict_aligned);
  int build_strict_aligned_segment_groups_(
      const common::ObArrayArray<common::ObNewVersionRange, 8> &query_lists,
      const int64_t base_idx,
      const int64_t tablet_cnt);

  int calc_async_index_read_snapshot_(const share::SCN &sql_read_snapshot);

private:
  ObArenaAllocator allocator_;
  share::SCN async_index_read_snapshot_;
  int64_t delete_table_published_version_;
  bool is_strict_aligned_;  // each tablet has exactly one version range in every segment group
  common::ObSEArray<ObAsyncIndexSegmentGroup*, 8> groups_;
  common::ObSEArray<ObTabletHandle, 8> index_tablet_handles_;
  // 每个索引 Tablet 的真实物理水位。query published version 仅允许在
  // 随表创建已证明为空的尾区间上高于该值。
  common::ObSEArray<int64_t, 8> index_physical_published_versions_;
  common::ObSEArray<int64_t, 8> index_published_versions_;
  common::ObArrayArray<common::ObNewVersionRange, 8> index_version_ranges_;

  DISALLOW_COPY_AND_ASSIGN(ObAsyncIndexSegmentGroupCtx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_ASYNC_INDEX_SEGMENT_GROUP_H_
