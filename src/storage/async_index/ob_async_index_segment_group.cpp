/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_async_index_segment_group.h"
#include "lib/utility/ob_sort.h"

namespace oceanbase
{
namespace storage
{

ObAsyncIndexSegmentGroup::ObAsyncIndexSegmentGroup()
  : tablet_version_ranges_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("AIndexSG", MTL_ID()))
{
}

ObAsyncIndexSegmentGroupCtx::~ObAsyncIndexSegmentGroupCtx()
{
  reset();
}

void ObAsyncIndexSegmentGroup::reset()
{
  tablet_version_ranges_.reset();
}

int ObAsyncIndexSegmentGroup::init(const int64_t tablet_cnt)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(tablet_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet cnt", K(ret), K(tablet_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
    common::ObSEArray<common::ObNewVersionRange, 4> empty_ranges;
    if (OB_FAIL(tablet_version_ranges_.push_back(empty_ranges))) {
      LOG_WARN("failed to init tablet version ranges", K(ret), K(i));
    }
  }
  return ret;
}

int ObAsyncIndexSegmentGroup::add_version_range(
    const int64_t tablet_idx,
    const common::ObNewVersionRange &version_range)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_idx < 0 || tablet_idx >= tablet_version_ranges_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet idx", K(ret), K(tablet_idx), K(tablet_version_ranges_.count()));
  } else if (OB_FAIL(tablet_version_ranges_.at(tablet_idx).push_back(version_range))) {
    LOG_WARN("failed to push version range", K(ret), K(tablet_idx), K(version_range));
  }
  return ret;
}

ObAsyncIndexSegmentGroupCtx::ObAsyncIndexSegmentGroupCtx()
  : allocator_("AIndexSGC", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    async_index_read_snapshot_(),
    delete_table_published_version_(INT64_MAX),
    is_strict_aligned_(false),
    groups_(),
    index_tablet_handles_(),
    index_physical_published_versions_(),
    index_published_versions_(),
    index_version_ranges_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("AIndexSGCV", MTL_ID()))
{
  groups_.set_attr(ObMemAttr(MTL_ID(), "AIndexSGCA"));
  index_tablet_handles_.set_attr(ObMemAttr(MTL_ID(), "AIndexSGCH"));
}

void ObAsyncIndexSegmentGroupCtx::reset()
{
  async_index_read_snapshot_.reset();
  delete_table_published_version_ = INT64_MAX;
  is_strict_aligned_ = false;
  for (int64_t i = 0; i < groups_.count(); ++i) {
    ObAsyncIndexSegmentGroup *segment_group = groups_.at(i);
    if (OB_NOT_NULL(segment_group)) {
      segment_group->reset();
      OB_DELETEx(ObAsyncIndexSegmentGroup, &allocator_, segment_group);
    }
  }
  groups_.reset();
  for (int64_t i = 0; i < index_version_ranges_.count(); ++i) {
    index_version_ranges_.at(i).reset();
  }
  index_version_ranges_.reset();
  index_tablet_handles_.reset();
  index_physical_published_versions_.reset();
  index_published_versions_.reset();
  allocator_.reset();
}

bool ObAsyncIndexSegmentGroupCtx::ObNewVersionRangeCompare::operator()(
    const common::ObNewVersionRange &left,
    const common::ObNewVersionRange &right) const
{
  int cmp_ret = 0;
  if (left.base_version_ != right.base_version_) {
    cmp_ret = left.base_version_ < right.base_version_ ? -1 : 1;
  } else if (left.snapshot_version_ != right.snapshot_version_) {
    cmp_ret = left.snapshot_version_ > right.snapshot_version_ ? -1 : 1;
  }
  return cmp_ret < 0;
}

int ObAsyncIndexSegmentGroupCtx::add_index_tablet_version_ranges(
    const ObTabletHandle &tablet_handle,
    common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges)
{
  int64_t published_version = 0;
  for (int64_t i = 0; i < tablet_version_ranges.count(); ++i) {
    published_version = OB_MAX(
        published_version, tablet_version_ranges.at(i).snapshot_version_);
  }
  return add_index_tablet_version_ranges(
      tablet_handle, tablet_version_ranges, published_version);
}

int ObAsyncIndexSegmentGroupCtx::add_index_tablet_version_ranges(
    const ObTabletHandle &tablet_handle,
    common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges,
    const int64_t published_version)
{
  int ret = OB_SUCCESS;
  int64_t physical_published_version = 0;
  for (int64_t i = 0; i < tablet_version_ranges.count(); ++i) {
    physical_published_version = OB_MAX(
        physical_published_version,
        tablet_version_ranges.at(i).snapshot_version_);
  }
  if (OB_UNLIKELY(published_version < physical_published_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("async-index query published version is behind physical ranges",
             K(ret), K(physical_published_version), K(published_version));
  } else if (OB_FAIL(index_tablet_handles_.push_back(tablet_handle))) {
    LOG_WARN("failed to push tablet handle", K(ret), K(tablet_handle));
  } else {
    lib::ob_sort(tablet_version_ranges.get_data(), tablet_version_ranges.get_data() + tablet_version_ranges.count(), ObNewVersionRangeCompare());
    if (OB_FAIL(index_version_ranges_.push_back(tablet_version_ranges))) {
      LOG_WARN("failed to push tablet version ranges", K(ret));
    } else if (OB_FAIL(index_physical_published_versions_.push_back(
                   physical_published_version))) {
      LOG_WARN("failed to push physical async-index published version", K(ret),
               K(physical_published_version));
    } else if (OB_FAIL(index_published_versions_.push_back(published_version))) {
      LOG_WARN("failed to push async-index published version", K(ret),
               K(published_version));
    }
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::set_delete_table_published_version(
    const int64_t published_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(published_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid published version", K(ret), K(published_version));
  } else {
    delete_table_published_version_ = published_version;
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::build_query_ranges_(
    const common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges,
    const int64_t read_ver,
    common::ObIArray<common::ObNewVersionRange> &out_ranges)
{
  int ret = OB_SUCCESS;
  out_ranges.reset();
  for (int64_t j = 0; OB_SUCC(ret) && j < tablet_version_ranges.count();) {
    const common::ObNewVersionRange &version_range = tablet_version_ranges.at(j);
    if (version_range.snapshot_version_ > read_ver) {
      // skip ranges beyond read snapshot
      LOG_INFO("skip range beyond read snapshot", K(j), K(version_range), K(read_ver));
      ++j;
    } else if (OB_FAIL(out_ranges.push_back(version_range))) {
      LOG_WARN("failed to push query range", K(ret), K(j), K(version_range));
    } else {
      while (j < tablet_version_ranges.count()
        && tablet_version_ranges.at(j).base_version_ >= version_range.base_version_
        && tablet_version_ranges.at(j).snapshot_version_ <= version_range.snapshot_version_) {
        ++j;
      }
    }
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::find_base_tablet_idx_(
    const common::ObArrayArray<common::ObNewVersionRange, 8> &query_lists,
    const common::ObIArray<int64_t> &physical_published_versions,
    int64_t &base_idx,
    bool &is_strict_aligned)
{
  int ret = OB_SUCCESS;
  base_idx = 0;
  is_strict_aligned = true;
  const common::ObIArray<common::ObNewVersionRange> &first_list = query_lists.at(0);
  const int64_t first_cnt = first_list.count();
  int64_t max_cnt = first_cnt;
  int64_t max_physical_published_version =
      physical_published_versions.at(0);
  for (int64_t i = 1; OB_SUCC(ret) && i < query_lists.count(); ++i) {
    const common::ObIArray<common::ObNewVersionRange> &current_list = query_lists.at(i);
    const int64_t cnt = query_lists.at(i).count();
    const int64_t physical_published_version =
        physical_published_versions.at(i);
    // 随表创建可能使多个索引处于不同物理水位。基准 Tablet 必须先覆盖
    // 最远的真实物理 range，避免用另一个索引的逻辑空后缀截断扫描范围；
    // 物理水位相同时再选择 range 更多的 Tablet，保留原有细粒度对齐能力。
    if (physical_published_version > max_physical_published_version
        || (physical_published_version == max_physical_published_version
            && cnt > max_cnt)) {
      max_physical_published_version = physical_published_version;
      max_cnt = cnt;
      base_idx = i;
    }
    if (cnt != first_cnt) {
      is_strict_aligned = false;
    } else if (is_strict_aligned) {
      for (int64_t j = 0; is_strict_aligned && j < cnt; ++j) {
        is_strict_aligned =
            first_list.at(j).base_version_ == current_list.at(j).base_version_
            && first_list.at(j).snapshot_version_ == current_list.at(j).snapshot_version_;
      }
    }
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::build_strict_aligned_segment_groups_(
    const common::ObArrayArray<common::ObNewVersionRange, 8> &query_lists,
    const int64_t base_idx,
    const int64_t tablet_cnt)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObNewVersionRange> &base_list = query_lists.at(base_idx);
  for (int64_t k = 0; OB_SUCC(ret) && k < base_list.count(); ++k) {
    ObAsyncIndexSegmentGroup *segment_group = nullptr;
    if (OB_ISNULL(segment_group = OB_NEWx(ObAsyncIndexSegmentGroup, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate segment group", K(ret), "size", sizeof(ObAsyncIndexSegmentGroup));
    } else if (OB_FAIL(segment_group->init(tablet_cnt))) {
      LOG_WARN("failed to init segment group", K(ret), K(tablet_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
      if (OB_FAIL(segment_group->add_version_range(i, query_lists.at(i).at(k)))) {
        LOG_WARN("failed to add version range to segment group", K(ret), K(i), K(k), "version_range", query_lists.at(i).at(k));
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(segment_group)) {
        segment_group->reset();
        OB_DELETEx(ObAsyncIndexSegmentGroup, &allocator_, segment_group);
      }
    } else if (OB_FAIL(groups_.push_back(segment_group))) {
      LOG_WARN("failed to push segment group", K(ret), K(k));
      segment_group->reset();
      OB_DELETEx(ObAsyncIndexSegmentGroup, &allocator_, segment_group);
    }
  }
  return ret;
}


int ObAsyncIndexSegmentGroupCtx::calc_async_index_read_snapshot_(const share::SCN &sql_read_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t min_published_version = INT64_MAX;
  share::SCN min_published_scn;
  const int64_t tablet_cnt = index_version_ranges_.count();
  if (OB_UNLIKELY(!sql_read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql read snapshot", K(ret), K(sql_read_snapshot));
  } else if (OB_UNLIKELY(0 == tablet_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_cnt));
  } else if (OB_UNLIKELY(index_physical_published_versions_.count() != tablet_cnt
                         || index_published_versions_.count() != tablet_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async-index range and published-version count mismatch", K(ret), K(tablet_cnt),
             "physical_published_version_count",
             index_physical_published_versions_.count(), "published_version_count",
             index_published_versions_.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
    min_published_version = OB_MIN(
        min_published_version, index_published_versions_.at(i));
  }
  min_published_version =
      OB_MIN(min_published_version, delete_table_published_version_);
  if (OB_SUCC(ret) && OB_FAIL(min_published_scn.convert_for_tx(min_published_version))) {
    LOG_WARN("failed to convert min published version to scn", K(ret), K(min_published_version));
  } else if (OB_SUCC(ret)) {
    async_index_read_snapshot_ = share::SCN::min(sql_read_snapshot, min_published_scn);
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::find_version_chain_(
    const common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges,
    int64_t &pos,
    const int64_t merge_start,
    const int64_t merge_end,
    common::ObIArray<common::ObNewVersionRange> &out_chain)
{
  int ret = OB_SUCCESS;
  out_chain.reuse();
  int64_t covered_start = merge_start;
  int64_t j = pos;
  for (; OB_SUCC(ret) && j < tablet_version_ranges.count();) {
    const common::ObNewVersionRange &version_range = tablet_version_ranges.at(j);
    const int64_t covered_end = version_range.snapshot_version_;
    if (version_range.base_version_ < covered_start) {
      LOG_WARN("version range is before covered start", K(pos), K(j), K(version_range), K(covered_start));
      ++j;
    } else if (version_range.base_version_ != covered_start) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected version range", K(ret), K(pos), K(j), K(version_range), K(covered_start));
    } else if (version_range.snapshot_version_ > merge_end) {
      LOG_TRACE("this range is after merge_end", K(pos), K(j), K(version_range), K(merge_end));
      ++j;
      if (j < tablet_version_ranges.count()) {
        if (tablet_version_ranges.at(j).base_version_ > merge_end) {
          LOG_TRACE("covered start is greater than merge end", K(pos), K(j), K(covered_start), K(merge_end));
          break;
        } else if (tablet_version_ranges.at(j).snapshot_version_ > merge_end) {
          LOG_TRACE("this range is after merge_end", K(pos), K(j), K(tablet_version_ranges.at(j)), K(merge_end));
          break;
        }
      }
    } else {
      if (OB_FAIL(out_chain.push_back(version_range))) {
        LOG_WARN("failed to push chain range", K(ret), K(version_range));
      } else {
        // Advance j until (base_version_, snapshot_version_] is not within (covered_start, covered_end]
        ++j;
        while (j < tablet_version_ranges.count()
            && tablet_version_ranges.at(j).base_version_ >= covered_start
            && tablet_version_ranges.at(j).snapshot_version_ <= covered_end) {
          LOG_TRACE("skip this range", K(pos), K(j), K(tablet_version_ranges.at(j)), K(covered_start), K(covered_end));
          ++j;
        }
        if (j < tablet_version_ranges.count()) {
          covered_start = tablet_version_ranges.at(j).base_version_;
          LOG_TRACE("update covered start", K(pos), K(j), K(covered_start));
          if (covered_start > merge_end) {
            LOG_TRACE("covered start is greater than merge end", K(pos), K(j), K(covered_start), K(merge_end));
            break;
          } else if (tablet_version_ranges.at(j).snapshot_version_ > merge_end) {
            LOG_TRACE("this range is after merge_end", K(pos), K(j), K(tablet_version_ranges.at(j)), K(merge_end));
            break;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("find version chain v2", K(pos), K(j), K(merge_start), K(merge_end));
    pos = j;
  }
  return ret;
}

bool ObAsyncIndexSegmentGroupCtx::is_version_chain_complete_(
    const common::ObIArray<common::ObNewVersionRange> &version_chain,
    const int64_t merge_start,
    const int64_t merge_end,
    const int64_t physical_published_version,
    const int64_t query_published_version)
{
  // query_published_version 只有在随表创建的 (P, C] 区间已证明
  // 没有用户数据时才会高于物理水位 P。物理 chain 必须连续覆盖到 P；
  // 这里只允许用逻辑空后缀补齐尾部，任何 P 之前的物理 gap 仍然失败。
  const int64_t physical_cover_end = OB_MIN(merge_end, physical_published_version);
  bool is_complete = merge_start < merge_end
      && !version_chain.empty()
      && physical_published_version >= 0
      && query_published_version >= physical_published_version
      && merge_end <= query_published_version;
  int64_t covered_end = merge_start;
  for (int64_t i = 0; is_complete && i < version_chain.count(); ++i) {
    const common::ObNewVersionRange &version_range = version_chain.at(i);
    if (version_range.base_version_ != covered_end
        || version_range.snapshot_version_ <= version_range.base_version_
        || version_range.snapshot_version_ > physical_cover_end) {
      is_complete = false;
    } else {
      covered_end = version_range.snapshot_version_;
    }
  }
  return is_complete && covered_end == physical_cover_end;
}

static bool has_version_range_starting_at(
    const common::ObIArray<common::ObNewVersionRange> &version_ranges,
    const int64_t start_pos,
    const int64_t base_version)
{
  bool found = false;
  for (int64_t i = start_pos; !found && i < version_ranges.count(); ++i) {
    if (version_ranges.at(i).base_version_ < base_version) {
      // Skip obsolete ranges that overlap a segment already selected for the current group.
    } else {
      found = version_ranges.at(i).base_version_ == base_version;
      break;
    }
  }
  return found;
}

int ObAsyncIndexSegmentGroupCtx::calc_query_version_ranges(const share::SCN &sql_read_snapshot)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_cnt = index_version_ranges_.count();
  common::ObSEArray<int64_t, 8> pos_array;
  common::ObArrayArray<common::ObNewVersionRange, 8> query_lists;
  if (OB_FAIL(calc_async_index_read_snapshot_(sql_read_snapshot))) {
    LOG_WARN("failed to calc async index read snapshot", K(ret), K(sql_read_snapshot));
  } else if (OB_UNLIKELY(0 == tablet_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_cnt));
  } else {
    int64_t base_idx = -1;
    const int64_t read_ver = async_index_read_snapshot_.get_val_for_tx();
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
      common::ObSEArray<common::ObNewVersionRange, 8> query_ranges;
      if (OB_FAIL(pos_array.push_back(0))) {
        LOG_WARN("failed to push merge cursor", K(ret), K(i));
      } else if (OB_FAIL(build_query_ranges_(index_version_ranges_.at(i), read_ver, query_ranges))) {
        LOG_WARN("failed to build query ranges", K(ret), K(i), K(read_ver));
      } else if (OB_FAIL(query_lists.push_back(query_ranges))) {
        LOG_WARN("failed to push query ranges", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(find_base_tablet_idx_(
                   query_lists, index_physical_published_versions_,
                   base_idx, is_strict_aligned_))) {
      LOG_WARN("failed to find base tablet", K(ret));
    } else if (is_strict_aligned_) {
      if (OB_FAIL(build_strict_aligned_segment_groups_(query_lists, base_idx, tablet_cnt))) {
        LOG_WARN("failed to build strict aligned segment groups", K(ret));
      }
    } else {
      const common::ObIArray<common::ObNewVersionRange> &base_list = query_lists.at(base_idx);
      common::ObSEArray<common::ObNewVersionRange, 4> chain;
      common::ObSEArray<int64_t, 8> candidate_pos_array;
      int64_t start_pos = 0;
      int64_t end_pos = 0;
      while (OB_SUCC(ret) && end_pos < base_list.count()) {
        const int64_t merge_start = base_list.at(start_pos).base_version_;
        const int64_t merge_end = base_list.at(end_pos).snapshot_version_;

        int64_t match_cnt = 0;
        candidate_pos_array.reuse();
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
          chain.reuse();
          // notice: not ref
          int64_t pos = pos_array.at(i);
          if (OB_FAIL(find_version_chain_(index_version_ranges_.at(i), pos, merge_start, merge_end, chain))) {
            LOG_WARN("failed to pick ranges for logical segment", K(ret), K(i), K(merge_start), K(merge_end));
          } else if (OB_FAIL(candidate_pos_array.push_back(pos))) {
            LOG_WARN("failed to save candidate merge cursor", K(ret), K(i), K(pos));
          } else if (is_version_chain_complete_(
                         chain, merge_start, merge_end,
                         index_physical_published_versions_.at(i),
                         index_published_versions_.at(i))) {
            ++match_cnt;
          }
        }
        bool can_commit_group = match_cnt == tablet_cnt;
        if (OB_SUCC(ret) && can_commit_group && end_pos + 1 < base_list.count()) {
          const int64_t next_merge_start = base_list.at(end_pos + 1).base_version_;
          for (int64_t i = 0; can_commit_group && i < tablet_cnt; ++i) {
            can_commit_group = has_version_range_starting_at(
                index_version_ranges_.at(i), candidate_pos_array.at(i), next_merge_start);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (can_commit_group) {
          ObAsyncIndexSegmentGroup *segment_group = nullptr;
          if (OB_ISNULL(segment_group = OB_NEWx(ObAsyncIndexSegmentGroup, &allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate segment group", K(ret), "size", sizeof(ObAsyncIndexSegmentGroup));
          } else if (OB_FAIL(segment_group->init(tablet_cnt))) {
            LOG_WARN("failed to init segment group", K(ret), K(tablet_cnt));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
            chain.reuse();
            // notice: is ref
            int64_t &pos = pos_array.at(i);
            if (OB_FAIL(find_version_chain_(index_version_ranges_.at(i), pos, merge_start, merge_end, chain))) {
              LOG_WARN("failed to pick ranges for logical segment", K(ret), K(i), K(merge_start), K(merge_end));
            } else if (!is_version_chain_complete_(
                           chain, merge_start, merge_end,
                           index_physical_published_versions_.at(i),
                           index_published_versions_.at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("version chain does not cover logical segment", K(ret), K(i),
                  K(merge_start), K(merge_end), K(chain));
            } else {
              for (int64_t j = 0; OB_SUCC(ret) && j < chain.count(); ++j) {
                if (OB_FAIL(segment_group->add_version_range(i, chain.at(j)))) {
                  LOG_WARN("failed to add version range to segment group", K(ret), K(i), K(chain.at(j)));
                }
              }
            }
          }
          if (OB_FAIL(ret)) {
            if (OB_NOT_NULL(segment_group)) {
              segment_group->reset();
              OB_DELETEx(ObAsyncIndexSegmentGroup, &allocator_, segment_group);
            }
          } else if (OB_FAIL(groups_.push_back(segment_group))) {
            LOG_WARN("failed to push segment group", K(ret), K(merge_start), K(merge_end));
            segment_group->reset();
            OB_DELETEx(ObAsyncIndexSegmentGroup, &allocator_, segment_group);
          } else {
            start_pos = end_pos + 1;
            end_pos = start_pos;
          }
        } else {
          ++end_pos;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (end_pos != start_pos) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(end_pos), K(start_pos));
      } else if (end_pos < base_list.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(end_pos), K(base_list.count()));
      } else if (groups_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no segment group after merge, may be input is invalid", K(ret));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
