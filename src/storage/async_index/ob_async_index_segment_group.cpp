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
    delete_version_ranges_(),
    groups_(),
    index_tablet_handles_(),
    index_published_versions_(),
    index_version_ranges_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("AIndexSGCV", MTL_ID()))
{
  groups_.set_attr(ObMemAttr(MTL_ID(), "AIndexSGCA"));
  delete_version_ranges_.set_attr(ObMemAttr(MTL_ID(), "AIndexSGCV"));
  index_tablet_handles_.set_attr(ObMemAttr(MTL_ID(), "AIndexSGCH"));
}

void ObAsyncIndexSegmentGroupCtx::reset()
{
  reset_query_result_();
  delete_table_published_version_ = INT64_MAX;
  delete_version_ranges_.reset();
  for (int64_t i = 0; i < index_version_ranges_.count(); ++i) {
    index_version_ranges_.at(i).reset();
  }
  index_version_ranges_.reset();
  index_tablet_handles_.reset();
  index_published_versions_.reset();
  allocator_.reset();
}

void ObAsyncIndexSegmentGroupCtx::reset_query_result_()
{
  async_index_read_snapshot_.reset();
  for (int64_t i = 0; i < groups_.count(); ++i) {
    ObAsyncIndexSegmentGroup *segment_group = groups_.at(i);
    if (OB_NOT_NULL(segment_group)) {
      segment_group->reset();
      OB_DELETEx(ObAsyncIndexSegmentGroup, &allocator_, segment_group);
    }
  }
  groups_.reset();
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
  if (OB_UNLIKELY(published_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index published version", K(ret), K(published_version));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_version_ranges.count(); ++i) {
    const common::ObNewVersionRange &range = tablet_version_ranges.at(i);
    if (OB_UNLIKELY(range.base_version_ < 0
        || range.snapshot_version_ <= range.base_version_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async index version range", K(ret), K(range));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(index_tablet_handles_.push_back(tablet_handle))) {
    LOG_WARN("failed to push tablet handle", K(ret), K(tablet_handle));
  } else {
    // The tablet may have advanced after reading the committed logical watermark.
    // Keep newer tables here; query selection checks the common read boundary.
    if (!tablet_version_ranges.empty()) {
      lib::ob_sort(tablet_version_ranges.get_data(),
          tablet_version_ranges.get_data() + tablet_version_ranges.count(),
          ObNewVersionRangeCompare());
    }
    if (OB_FAIL(index_version_ranges_.push_back(tablet_version_ranges))) {
      LOG_WARN("failed to push tablet version ranges", K(ret));
    } else if (OB_FAIL(index_published_versions_.push_back(published_version))) {
      LOG_WARN("failed to push async index published version", K(ret), K(published_version));
    }
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::set_delete_table_version_ranges(
    const common::ObIArray<common::ObNewVersionRange> &version_ranges,
    const int64_t published_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(published_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid published version", K(ret), K(published_version));
  } else if (OB_FAIL(delete_version_ranges_.assign(version_ranges))) {
    LOG_WARN("failed to copy deletion version ranges", K(ret), K(version_ranges));
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
  out_ranges.reuse();
  for (int64_t j = 0; OB_SUCC(ret) && j < tablet_version_ranges.count(); ++j) {
    const common::ObNewVersionRange &range = tablet_version_ranges.at(j);
    if (range.snapshot_version_ <= read_ver && OB_FAIL(out_ranges.push_back(range))) {
      LOG_WARN("failed to push query range", K(ret), K(j), K(range));
    }
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::calc_async_index_read_snapshot_(
    const share::SCN &sql_read_snapshot, share::SCN &upper_bound) const
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
  } else if (OB_UNLIKELY(index_published_versions_.count() != tablet_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async-index range and published-version count mismatch", K(ret), K(tablet_cnt),
             "published_version_count",
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
    upper_bound = share::SCN::min(sql_read_snapshot, min_published_scn);
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::find_version_chain_(
    const common::ObIArray<common::ObNewVersionRange> &tablet_version_ranges,
    const int64_t merge_start,
    const int64_t merge_end,
    common::ObIArray<common::ObNewVersionRange> &out_chain)
{
  int ret = OB_SUCCESS;
  out_chain.reuse();
  // select_query_ranges_ has checked that both group boundaries do not cut an SSTable.
  for (int64_t pos = 0; OB_SUCC(ret) && pos < tablet_version_ranges.count(); ++pos) {
    const common::ObNewVersionRange &range = tablet_version_ranges.at(pos);
    if (range.base_version_ >= merge_end) {
      break;
    } else if (range.snapshot_version_ <= merge_start) {
      // Already consumed by an earlier group.
    } else if (OB_FAIL(out_chain.push_back(range))) {
      LOG_WARN("failed to push chain range", K(ret), K(range));
    }
  }
  return ret;
}

bool ObAsyncIndexSegmentGroupCtx::is_group_boundary_(
    const common::ObIArray<common::ObNewVersionRange> &ranges,
    const int64_t boundary)
{
  bool is_safe = true;
  for (int64_t i = 0; is_safe && i < ranges.count(); ++i) {
    is_safe = ranges.at(i).base_version_ >= boundary
        || ranges.at(i).snapshot_version_ <= boundary;
  }
  return is_safe;
}

int64_t ObAsyncIndexSegmentGroupCtx::next_group_end_(
    const common::ObArrayArray<common::ObNewVersionRange, 8> &query_lists,
    const int64_t base_idx,
    const int64_t previous_end,
    int64_t &base_pos)
{
  const common::ObIArray<common::ObNewVersionRange> &base = query_lists.at(base_idx);
  while (base_pos < base.count() && base.at(base_pos).snapshot_version_ <= previous_end) {
    ++base_pos;
  }
  int64_t next_end = base_pos < base.count()
      ? base.at(base_pos).snapshot_version_ : INT64_MAX;
  const int64_t gap_end = base_pos < base.count()
      ? base.at(base_pos).base_version_ : INT64_MAX;
  // Probe other indexes' endpoints through the base tablet's head, middle and tail gaps.
  for (int64_t i = 0; i < query_lists.count(); ++i) {
    const common::ObIArray<common::ObNewVersionRange> &ranges = query_lists.at(i);
    for (int64_t j = 0; j < ranges.count(); ++j) {
      const int64_t end = ranges.at(j).snapshot_version_;
      if (end > previous_end && (end <= gap_end || INT64_MAX == next_end)) {
        next_end = OB_MIN(next_end, end);
      }
    }
  }
  return next_end;
}

int ObAsyncIndexSegmentGroupCtx::append_segment_group_(
    const common::ObArrayArray<common::ObNewVersionRange, 8> &chains)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexSegmentGroup *group = nullptr;
  if (OB_ISNULL(group = OB_NEWx(ObAsyncIndexSegmentGroup, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate segment group", K(ret));
  } else if (OB_FAIL(group->init(chains.count()))) {
    LOG_WARN("failed to initialize segment group", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < chains.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < chains.at(i).count(); ++j) {
      if (OB_FAIL(group->add_version_range(i, chains.at(i).at(j)))) {
        LOG_WARN("failed to add group range", K(ret), K(i), K(j));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(groups_.push_back(group))) {
    LOG_WARN("failed to push segment group", K(ret));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(group)) {
    OB_DELETEx(ObAsyncIndexSegmentGroup, &allocator_, group);
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::calc_query_version_ranges(const share::SCN &sql_read_snapshot)
{
  int ret = OB_SUCCESS;
  share::SCN upper_bound;
  const bool already_calculated = !groups_.empty();
  if (already_calculated) {
    ret = OB_INIT_TWICE;
    LOG_WARN("segment groups already calculated", K(ret));
  } else if (OB_FAIL(calc_async_index_read_snapshot_(sql_read_snapshot, upper_bound))) {
    LOG_WARN("failed to calculate async index upper bound", K(ret), K(sql_read_snapshot));
  }
  int64_t read_ver = OB_SUCC(ret) ? upper_bound.get_val_for_tx() : 0;
  bool changed = true;
  // No historical replacement: move R to the start of each crossing SSTable.
  // Revisit indexes and deletion batches because lowering R can cut another SSTable.
  while (OB_SUCC(ret) && changed) {
    changed = false;
    for (int64_t i = 0; i < index_version_ranges_.count(); ++i) {
      const common::ObIArray<common::ObNewVersionRange> &ranges = index_version_ranges_.at(i);
      for (int64_t j = 0; j < ranges.count(); ++j) {
        if (ranges.at(j).base_version_ < read_ver
            && read_ver < ranges.at(j).snapshot_version_) {
          LOG_DEBUG("rollback async index read snapshot across sstable",
              "is_delete_table", false, K(read_ver), "new_read_ver", ranges.at(j).base_version_,
              "version_range", ranges.at(j), K(upper_bound));
          read_ver = ranges.at(j).base_version_;
          changed = true;
        }
      }
    }
    for (int64_t i = 0; i < delete_version_ranges_.count(); ++i) {
      const common::ObNewVersionRange &range = delete_version_ranges_.at(i);
      if (range.base_version_ < read_ver && read_ver < range.snapshot_version_) {
        LOG_DEBUG("rollback async index read snapshot across sstable",
            "is_delete_table", true, K(read_ver), "new_read_ver", range.base_version_,
            "version_range", range, K(upper_bound));
        read_ver = range.base_version_;
        changed = true;
      }
    }
    if (0 == read_ver && 0 < upper_bound.get_val_for_tx()) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("latest async index baseline exceeds read boundary", K(ret), K(upper_bound));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_query_ranges_(read_ver))) {
      LOG_WARN("failed to build async index groups", K(ret), K(read_ver));
    } else if (OB_FAIL(async_index_read_snapshot_.convert_for_tx(read_ver))) {
      LOG_WARN("failed to publish async index snapshot", K(ret), K(read_ver));
    }
  }
  if (OB_FAIL(ret) && !already_calculated) {
    reset_query_result_();
  }
  return ret;
}

int ObAsyncIndexSegmentGroupCtx::select_query_ranges_(const int64_t read_ver)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_cnt = index_version_ranges_.count();
  int64_t base_idx = 0;
  common::ObArrayArray<common::ObNewVersionRange, 8> query_lists(
      OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("AIndexSGCQ", MTL_ID()));
  common::ObArrayArray<common::ObNewVersionRange, 8> chains(
      OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator("AIndexSGCC", MTL_ID()));
  common::ObSEArray<common::ObNewVersionRange, 8> chain;
  chain.set_attr(ObMemAttr(MTL_ID(), "AIndexSGCT"));
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
    if (OB_FAIL(query_lists.push_back(chain)) || OB_FAIL(chains.push_back(chain))) {
      LOG_WARN("failed to initialize query scratch buffers", K(ret));
    } else if (OB_FAIL(build_query_ranges_(
        index_version_ranges_.at(i), read_ver, query_lists.at(i)))) {
      LOG_WARN("failed to build query ranges", K(ret), K(i), K(read_ver));
    } else if (query_lists.at(i).count() > query_lists.at(base_idx).count()) {
      base_idx = i;
    }
  }
  int64_t base_pos = 0;
  int64_t group_start = 0;
  int64_t candidate_end = 0;
  while (OB_SUCC(ret)) {
    candidate_end = next_group_end_(query_lists, base_idx, candidate_end, base_pos);
    if (INT64_MAX == candidate_end) {
      break;
    }
    bool can_commit = true;
    for (int64_t i = 0; can_commit && i < tablet_cnt; ++i) {
      can_commit = is_group_boundary_(query_lists.at(i), candidate_end);
    }
    if (can_commit) {
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
        if (OB_FAIL(find_version_chain_(
            query_lists.at(i), group_start, candidate_end, chains.at(i)))) {
          LOG_WARN("failed to select group ranges", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(append_segment_group_(chains))) {
        LOG_WARN("failed to append segment group", K(ret));
      } else if (OB_SUCC(ret)) {
        group_start = candidate_end;
      }
    }
  }
  // The largest selected endpoint cannot cross any selected SSTable, so every
  // nonempty input is consumed. Any remaining interval up to R is an empty gap.
  if (OB_SUCC(ret) && groups_.empty() && OB_FAIL(append_segment_group_(chains))) {
    LOG_WARN("failed to append all-empty segment group", K(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
