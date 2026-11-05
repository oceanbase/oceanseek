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
  int set_delete_table_version_ranges(
      const common::ObIArray<common::ObNewVersionRange> &version_ranges,
      const int64_t published_version);
  // INT64_MAX means the data tablet has no active async-index retention checkpoint.
  int calc_query_version_ranges(const share::SCN &sql_read_snapshot,
      const int64_t min_async_index_snapshot = INT64_MAX);

  share::SCN get_async_index_read_snapshot() const { return async_index_read_snapshot_; }
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
               K_(delete_version_ranges),
               K_(index_published_versions),
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
      const int64_t merge_start,
      const int64_t merge_end,
      common::ObIArray<common::ObNewVersionRange> &out_chain);
  static int build_query_ranges_(
      const common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges,
      const int64_t read_ver,
      common::ObIArray<common::ObNewVersionRange> &out_ranges);
  static bool is_group_boundary_(
      const common::ObIArray<common::ObNewVersionRange> &ranges,
      const int64_t boundary);
  static int64_t next_group_end_(
      const common::ObArrayArray<common::ObNewVersionRange, 8> &query_lists,
      const int64_t base_idx,
      const int64_t previous_end,
      int64_t &base_pos);
  int append_segment_group_(
      const common::ObArrayArray<common::ObNewVersionRange, 8> &chains);

  int calc_async_index_read_snapshot_(
      const share::SCN &sql_read_snapshot, share::SCN &upper_bound,
      bool &sql_limited, int64_t &min_published_version) const;
  int calc_real_published_read_version_(const int64_t upper_bound, int64_t &read_version) const;
  int select_query_ranges_(const int64_t read_ver);
  void reset_query_result_();

private:
  ObArenaAllocator allocator_;
  share::SCN async_index_read_snapshot_;
  int64_t delete_table_published_version_;
  // Deletion endpoints contribute to the SQL-limited starting version, but do not
  // constrain subsequent index alignment: bitmap rows are filtered by rowscn <= R.
  common::ObSEArray<common::ObNewVersionRange, 8> delete_version_ranges_;
  common::ObSEArray<ObAsyncIndexSegmentGroup*, 8> groups_;
  common::ObSEArray<ObTabletHandle, 8> index_tablet_handles_;
  // Committed logical refresh watermarks certify gaps without materializing empty tables.
  common::ObSEArray<int64_t, 8> index_published_versions_;
  common::ObArrayArray<common::ObNewVersionRange, 8> index_version_ranges_;

  DISALLOW_COPY_AND_ASSIGN(ObAsyncIndexSegmentGroupCtx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_ASYNC_INDEX_SEGMENT_GROUP_H_
