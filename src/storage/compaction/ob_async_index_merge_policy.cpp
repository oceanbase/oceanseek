/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/compaction/ob_async_index_merge_policy.h"
#ifdef ERRSIM
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#endif
#include "share/schema/ob_async_index_params.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ls/ob_ls.h"
#include "storage/ob_storage_struct.h"
#include "lib/utility/ob_sort.h"
#include <cmath>

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
namespace compaction
{

struct IncMajorTableCmp
{
  bool operator()(ObITable *left, ObITable *right) const
  {
    const ObSSTable *lhs = static_cast<const ObSSTable *>(left);
    const ObSSTable *rhs = static_cast<const ObSSTable *>(right);
    const int64_t left_base = lhs->get_async_index_base_version();
    const int64_t right_base = rhs->get_async_index_base_version();
    if (left_base != right_base) {
      return left_base < right_base;
    }
    const int64_t left_snapshot = lhs->get_async_index_snapshot_version();
    const int64_t right_snapshot = rhs->get_async_index_snapshot_version();
    if (left_snapshot != right_snapshot) {
      return left_snapshot > right_snapshot;
    }
    return false;
  }
};

ObAsyncIndexSSTableMergeInfo::ObAsyncIndexSSTableMergeInfo()
  : start_snapshot_version_(0),
    end_snapshot_version_(0),
    seg_size_(0),
    level_(0.0f)
{
}

void ObAsyncIndexSSTableMergeInfo::reset()
{
  start_snapshot_version_ = 0;
  end_snapshot_version_ = 0;
  seg_size_ = 0;
  level_ = 0.0f;
}

bool ObAsyncIndexSSTableMergeInfo::is_valid() const
{
  return start_snapshot_version_ >= 0
      && end_snapshot_version_ > start_snapshot_version_
      && seg_size_ >= 0;
}

int ObAsyncIndexMergePolicyHelper::push_sstable_merge_info_(
    ObSSTable *table,
    ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sstable", KR(ret));
  } else {
    ObNewVersionRange version_range;
    ObAsyncIndexSSTableMergeInfo merge_info;
    if (OB_FAIL(table->get_async_index_version_range(version_range))) {
      LOG_WARN("failed to get async index sstable version range", KR(ret), KPC(table));
    } else {
      merge_info.start_snapshot_version_ = version_range.base_version_;
      merge_info.end_snapshot_version_ = version_range.snapshot_version_;
      merge_info.seg_size_ = table->get_occupy_size();
      if (OB_FAIL(merge_infos.push_back(merge_info))) {
        LOG_WARN("failed to push back merge info", KR(ret), K(merge_info));
      }
    }
  }
  return ret;
}

int ObAsyncIndexMergePolicyHelper::get_all_tables_(
    const ObSSTableArray &sstable_array,
    ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!sstable_array.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable array not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      ObSSTable *sstable = sstable_array.at(i);
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sstable", KR(ret), K(i));
      } else if (OB_FAIL(tables.push_back(sstable))) {
        LOG_WARN("failed to push sstable", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObAsyncIndexMergePolicyHelper::collect_merge_infos(
    ObLS &ls,
    const ObTablet &tablet,
    const int64_t schedule_snapshot_version,
    ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> all_merge_infos;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  const int64_t last_major_snapshot = tablet.get_last_major_snapshot_version();
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret), K(tablet));
  } else if (OB_ISNULL(table_store = table_store_wrapper.get_member())
      || OB_UNLIKELY(!table_store->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store", KR(ret), K(tablet));
  } else {
    const ObSSTableArray &major_sstables = table_store->get_major_sstables();
    const ObSSTableArray &inc_major_sstables = table_store->get_inc_major_sstables();
    ObSSTable *last_major = static_cast<ObSSTable *>(major_sstables.get_boundary_table(true/*last*/));
    if (OB_NOT_NULL(last_major)) {
      if (OB_FAIL(push_sstable_merge_info_(last_major, all_merge_infos))) {
        LOG_WARN("failed to push back major merge info", KR(ret), KPC(last_major));
      }
    }
    ObSEArray<ObITable *, 16> inc_major_tables;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_all_tables_(inc_major_sstables, inc_major_tables))) {
      LOG_WARN("failed to get inc major tables", KR(ret), K(tablet));
    } else if (inc_major_tables.count() > 0) {
      lib::ob_sort(inc_major_tables.begin(), inc_major_tables.end(), IncMajorTableCmp());
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_tables.count();) {
        ObSSTable *sstable = static_cast<ObSSTable *>(inc_major_tables.at(i));
        ObNewVersionRange version_range;
        int64_t tx_state = ObTxData::UNKOWN;
        int64_t commit_version = -1;
        if (OB_ISNULL(sstable) || !sstable->is_inc_major_type_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected inc major sstable", KR(ret), K(i), KP(sstable));
        } else if (OB_FAIL(ObIncMajorTxHelper::get_inc_major_commit_version(
            ls, *sstable, SCN::max_scn(), tx_state, commit_version))) {
          LOG_WARN("failed to get inc major commit version", KR(ret), K(i), KPC(sstable));
        } else if (ObTxData::COMMIT != tx_state) {
          LOG_INFO("skip uncommitted async index inc major sstable", K(i), KPC(sstable), K(tx_state));
          ++i;
        } else if (OB_FAIL(sstable->get_async_index_version_range(version_range))) {
          LOG_WARN("failed to get async index sstable version range", KR(ret), K(i), KPC(sstable));
        } else if (version_range.snapshot_version_ <= last_major_snapshot) {
          LOG_DEBUG("skip inc major covered by last major", K(i), K(version_range), K(last_major_snapshot));
          ++i;
        } else if (version_range.base_version_ >= schedule_snapshot_version) {
          LOG_DEBUG("skip inc major beyond schedule snapshot", K(i), K(version_range), K(schedule_snapshot_version));
          ++i;
        } else if (OB_FAIL(push_sstable_merge_info_(sstable, all_merge_infos))) {
          LOG_WARN("failed to push back inc major merge info", KR(ret), K(i), K(version_range));
        } else {
          while (OB_SUCC(ret) && i < inc_major_tables.count()) {
            ObNewVersionRange next_range;
            ObSSTable *next_sstable = static_cast<ObSSTable *>(inc_major_tables.at(i));
            if (OB_FAIL(next_sstable->get_async_index_version_range(next_range))) {
              LOG_WARN("failed to get next async index sstable version range", KR(ret),
                  K(i), KPC(next_sstable));
            } else if (next_range.base_version_ >= version_range.base_version_
                && next_range.snapshot_version_ <= version_range.snapshot_version_) {
              ++i;
            } else {
              break;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      struct MergeInfoCmp {
        bool operator()(const ObAsyncIndexSSTableMergeInfo &left,
                        const ObAsyncIndexSSTableMergeInfo &right) const
        {
          return left.start_snapshot_version_ != right.start_snapshot_version_
              ? left.start_snapshot_version_ < right.start_snapshot_version_
              : left.end_snapshot_version_ > right.end_snapshot_version_;
        }
      };
      lib::ob_sort(all_merge_infos.begin(), all_merge_infos.end(), MergeInfoCmp());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_merge_infos.count();) {
      const ObAsyncIndexSSTableMergeInfo &candidate = all_merge_infos.at(i);
      if (candidate.end_snapshot_version_ > schedule_snapshot_version) {
        LOG_INFO("skip range beyond read snapshot", K(i), K(candidate), K(schedule_snapshot_version), K(lbt()));
        ++i;
      } else if (OB_FAIL(merge_infos.push_back(candidate))) {
        LOG_WARN("failed to push queryable range", KR(ret), K(i), K(candidate));
      } else {
        while (i < all_merge_infos.count()
          && all_merge_infos.at(i).start_snapshot_version_ >= candidate.start_snapshot_version_
          && all_merge_infos.at(i).end_snapshot_version_ <= candidate.end_snapshot_version_) {
          ++i;
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexMergePolicyHelper::get_inc_major_sstables(
    ObLS &ls,
    const ObTablet &tablet,
    const int64_t snapshot,
    const int64_t last_major_snapshot,
    ObGetMergeTablesResult &result)
{
  int ret = OB_SUCCESS;
  const bool is_inverted_index = tablet.is_async_inverted_storage_index();
  const int64_t start_idx = result.handle_.get_count();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  ObSEArray<ObITable *, 16> inc_major_tables;
  bool has_aggregated_table_key_range = false;
  bool has_aggregated_rec_scn = !result.handle_.empty();
  if (OB_UNLIKELY(snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid snapshot", KR(ret), K(snapshot));
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret), K(tablet));
  } else if (OB_ISNULL(table_store = table_store_wrapper.get_member())
      || OB_UNLIKELY(!table_store->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store", KR(ret), K(tablet));
  } else if (OB_FAIL(get_all_tables_(table_store->get_inc_major_sstables(), inc_major_tables))) {
    LOG_WARN("failed to get inc major tables", KR(ret), K(tablet));
  } else if (inc_major_tables.count() > 0) {
    lib::ob_sort(inc_major_tables.begin(), inc_major_tables.end(), IncMajorTableCmp());
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_tables.count();) {
      ObSSTable *sstable = static_cast<ObSSTable *>(inc_major_tables.at(i));
      ObNewVersionRange version_range;
      int64_t tx_state = ObTxData::UNKOWN;
      int64_t commit_version = -1;
      if (OB_ISNULL(sstable) || !sstable->is_inc_major_type_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected inc major sstable", KR(ret), K(i), KP(sstable));
      } else if (OB_FAIL(ObIncMajorTxHelper::get_inc_major_commit_version(ls, *sstable, SCN::max_scn(), tx_state, commit_version))) {
        LOG_WARN("failed to get inc major commit version", KR(ret), K(i), KPC(sstable));
      } else if (ObTxData::COMMIT != tx_state) {
        LOG_INFO("skip uncommitted async index inc major sstable", K(i), KPC(sstable), K(tx_state));
        ++i;
      } else if (OB_FAIL(sstable->get_async_index_version_range(version_range))) {
        LOG_WARN("failed to get async index sstable version range", KR(ret), K(i), KPC(sstable));
      } else if (version_range.snapshot_version_ <= last_major_snapshot) {
        ++i;
      } else if (version_range.base_version_ >= snapshot) {
        ++i;
      } else if (OB_FAIL(result.handle_.add_sstable(sstable, table_store_wrapper.get_meta_handle()))) {
        LOG_WARN("failed to add async index inc major sstable", KR(ret), K(i), K(version_range));
      } else {
        if (!has_aggregated_table_key_range) {
          result.scn_range_.start_scn_ = sstable->get_start_scn();
          result.scn_range_.end_scn_ = sstable->get_end_scn();
          has_aggregated_table_key_range = true;
        } else {
          result.scn_range_.start_scn_ = SCN::min(
              result.scn_range_.start_scn_, sstable->get_start_scn());
          result.scn_range_.end_scn_ = SCN::max(
              result.scn_range_.end_scn_, sstable->get_end_scn());
        }
        if (!has_aggregated_rec_scn) {
          result.rec_scn_ = sstable->get_rec_scn();
          has_aggregated_rec_scn = true;
        } else {
          result.rec_scn_ = SCN::min(result.rec_scn_, sstable->get_rec_scn());
        }
        result.version_range_.snapshot_version_ = MAX(
            result.version_range_.snapshot_version_, version_range.snapshot_version_);

        while (OB_SUCC(ret) && i < inc_major_tables.count()) {
          ObNewVersionRange next_range;
          ObSSTable *next_sstable = static_cast<ObSSTable *>(inc_major_tables.at(i));
          if (OB_FAIL(next_sstable->get_async_index_version_range(next_range))) {
            LOG_WARN("failed to get next async index sstable version range", KR(ret),
                K(i), KPC(next_sstable));
          } else if (next_range.base_version_ >= version_range.base_version_
              && next_range.snapshot_version_ <= version_range.snapshot_version_) {
            ++i;
          } else {
            break;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && is_inverted_index) {
    ret = check_inc_major_sstables_(result.handle_, start_idx, last_major_snapshot, snapshot);
  }
  return ret;
}

int ObAsyncIndexMergePolicyHelper::check_inc_major_sstables_(
    const ObTablesHandleArray &tables_handle,
    const int64_t start_idx,
    const int64_t last_major_snapshot,
    const int64_t merge_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t previous_snapshot = last_major_snapshot;
  const ObITable *table = nullptr;
  ObNewVersionRange version_range;
  // The old TableKey range represented continuous version coverage, so the old
  // policy required adjacent IncMajor ranges to cover [last_major_snapshot,
  // merge_snapshot] exactly. After TableKey changes to describe the range that
  // actually contains data, a missing range may simply mean that the interval
  // has no data. Therefore range continuity is no longer a validity condition;
  // prefix, middle, and suffix gaps, as well as no selected IncMajor, are legal.
  //
  // IncMajorTableCmp orders selected inputs by logical base version. For the
  // resulting sequence, the current check enforces only two invariants:
  //   1. base_version >= previous_snapshot, so an IncMajor does not overlap the
  //      base Major or the previously selected IncMajor;
  //   2. snapshot_version <= merge_snapshot, so no selected input crosses this
  //      round's merge target.
  // merge_snapshot is only the current merge target, not an MDS publish
  // version. This helper does not prove that all data up to the target has been
  // published; that will be determined from the SSTable data range and MDS
  // publish version after the latter is implemented.
  for (int64_t i = start_idx; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
    version_range.reset();
    if (OB_ISNULL(table = tables_handle.get_table(i))
        || OB_UNLIKELY(!table->is_inc_major_type_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected async index input", KR(ret), K(i), KPC(table));
    } else if (OB_FAIL(static_cast<const ObSSTable *>(table)->get_async_index_version_range(
        version_range))) {
      LOG_WARN("failed to get async index sstable version range", KR(ret), K(i), KPC(table));
    } else if (version_range.base_version_ < previous_snapshot
        || version_range.snapshot_version_ > merge_snapshot) {
      ret = OB_NO_NEED_MERGE;
    } else {
      previous_snapshot = version_range.snapshot_version_;
    }
  }
  if (OB_NO_NEED_MERGE == ret) {
    LOG_INFO("async index input range overlaps or exceeds merge snapshot, skip this round",
        KR(ret), K(last_major_snapshot), K(merge_snapshot), K(previous_snapshot),
        K(version_range), KPC(table));
  }
  return ret;
}

int ObAsyncIndexMergePolicyHelper::calc_tablet_visible_row_count_(const ObTablet &tablet, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  int64_t base_row_count = 0;
  const int64_t inc_row_count = tablet.get_inc_row_cnt();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  row_count = 0;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret), K(tablet));
  } else if (OB_ISNULL(table_store = table_store_wrapper.get_member())
      || OB_UNLIKELY(!table_store->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table store", KR(ret), K(tablet));
  } else {
    ObSSTable *last_major = static_cast<ObSSTable *>(table_store->get_major_sstables().get_boundary_table(true));
    base_row_count = OB_ISNULL(last_major) ? 0 : last_major->get_row_count();
    row_count = inc_row_count + base_row_count;
  }
  LOG_DEBUG("calc tablet visible row count", KR(ret), K(tablet), K(inc_row_count), K(base_row_count), K(row_count));
  return ret;
}

int ObAsyncIndexMergePolicyHelper::calc_delete_percentage(
    ObLS &ls,
    const ObTablet &data_tablet,
    const ObTabletID &delete_index_tablet_id,
    int64_t &delete_percentage)
{
  int ret = OB_SUCCESS;
  ObTabletHandle delete_log_handle;
  int64_t data_row_count = 0;
  int64_t delete_row_count = 0;
  delete_percentage = 0;
  if (!delete_index_tablet_id.is_valid()) {
    LOG_INFO("delete log tablet id is invalid, skip calc delete percentage", K(data_tablet));
  } else if (OB_FAIL(calc_tablet_visible_row_count_(data_tablet, data_row_count))) {
    LOG_WARN("failed to calc data tablet row count", KR(ret), K(data_tablet));
  } else if (OB_FAIL(ls.get_tablet(delete_index_tablet_id, delete_log_handle))) {
    LOG_WARN("failed to get delete log tablet", KR(ret), K(delete_index_tablet_id));
  } else if (OB_UNLIKELY(!delete_log_handle.is_valid() || OB_ISNULL(delete_log_handle.get_obj()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid delete log tablet handle", KR(ret), K(delete_index_tablet_id));
  } else if (OB_FAIL(calc_tablet_visible_row_count_(*delete_log_handle.get_obj(), delete_row_count))) {
    LOG_WARN("failed to calc delete log row count", KR(ret), K(delete_index_tablet_id));
  } else if (data_row_count == 0) {
    delete_percentage = delete_row_count > 0 ? 100 : 0;
  } else {
    delete_percentage = delete_row_count * 100 / data_row_count;
  }
  LOG_INFO("calc async index delete percentage", KR(ret), K(delete_row_count), K(data_row_count), K(delete_percentage),
      K(data_tablet), K(delete_index_tablet_id));
  return ret;
}

int ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
    ObLS &ls,
    const ObIArray<ObTabletID> &index_tablet_ids,
    int64_t &schedule_snapshot_version)
{
  int ret = OB_SUCCESS;
  schedule_snapshot_version = INT64_MAX;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_tablet_ids.count(); ++i) {
    const ObTabletID &index_tablet_id = index_tablet_ids.at(i);
    ObTabletHandle index_handle;
    int64_t max_published_version = 0;
    if (OB_FAIL(ls.get_tablet(index_tablet_id, index_handle))) {
      LOG_WARN("failed to get index tablet", KR(ret), K(i), K(index_tablet_id));
    } else if (OB_UNLIKELY(!index_handle.is_valid() || OB_ISNULL(index_handle.get_obj()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index tablet handle", KR(ret), K(i), K(index_tablet_id));
    } else if (OB_FAIL(index_handle.get_obj()->get_async_index_max_published_version(max_published_version))) {
      LOG_WARN("failed to calc eligible max scn", KR(ret), K(i), K(index_tablet_id));
    } else if (max_published_version <= 0) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("tablet max published version is 0, so skip", KR(ret), K(i), K(index_tablet_id), K(max_published_version));
    } else {
      schedule_snapshot_version = MIN(schedule_snapshot_version, max_published_version);
      LOG_DEBUG("async index merge published version", K(i), K(index_tablet_id),
          K(max_published_version), K(schedule_snapshot_version));
    }
  }
  return ret;
}

ObAsyncIndexMergePolicyBase::ObAsyncIndexMergePolicyBase()
  : is_inited_(false)
{
}

ObAsyncIndexMergePolicyBase::~ObAsyncIndexMergePolicyBase()
{
}

int ObAsyncIndexMergePolicyBase::validate_merge_infos_(
    const ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos) const
{
  int ret = OB_SUCCESS;
  const int64_t count = merge_infos.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    const ObAsyncIndexSSTableMergeInfo &info = merge_infos.at(i);
    if (!info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid merge info", K(ret), K(i), K(info));
    } else if (i > 0) {
      const ObAsyncIndexSSTableMergeInfo &prev = merge_infos.at(i - 1);
      if (info.start_snapshot_version_ < prev.start_snapshot_version_
          || info.end_snapshot_version_ < prev.end_snapshot_version_
          || info.start_snapshot_version_ < prev.end_snapshot_version_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("snapshot chain broken", K(ret), K(i), K(prev.start_snapshot_version_), K(prev.end_snapshot_version_),
          K(info.start_snapshot_version_), K(info.end_snapshot_version_));
      }
    }
  }
  return ret;
}

double ObAsyncIndexMergePolicy::calc_level(const int64_t seg_size, const int64_t merge_factor)
{
  const double size = static_cast<double>(MAX(1, seg_size));
  const double merge_factor_d = static_cast<double>(MAX(2, merge_factor));
  return std::log(size) / std::log(merge_factor_d);
}

ObAsyncIndexMergePolicy::ObAsyncIndexMergePolicy()
  : merge_factor_(DEFAULT_MERGE_FACTOR),
    min_merge_size_(DEFAULT_MIN_MERGE_SIZE),
    max_merge_size_(DEFAULT_MAX_MERGE_SIZE)
{
}

int ObAsyncIndexMergePolicy::init(const int64_t merge_segment_threshold)
{
  merge_factor_ = merge_segment_threshold;
  min_merge_size_ = DEFAULT_MIN_MERGE_SIZE;
  max_merge_size_ = DEFAULT_MAX_MERGE_SIZE;
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObAsyncIndexMergePolicy::calc_and_fill_levels_(
    ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < merge_infos.count(); ++i) {
    ObAsyncIndexSSTableMergeInfo &info = merge_infos.at(i);
    info.level_ = static_cast<float>(calc_level(info.seg_size_, merge_factor_));
  }
  return ret;
}

double ObAsyncIndexMergePolicy::calc_level_floor_() const
{
  return calc_level(min_merge_size_, merge_factor_);
}

int ObAsyncIndexMergePolicy::pick_merge_at_level_(
    const ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
    const int32_t level_start,
    const int32_t upto,
    int32_t &merge_start_idx,
    int32_t &merge_end_idx) const
{
  int ret = OB_SUCCESS;
  merge_start_idx = -1;
  merge_end_idx = -1;
  if (level_start < 0 || upto < level_start || upto >= merge_infos.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid level start or upto", K(ret), K(level_start), K(upto), K(merge_infos.count()));
  } else {
    const int32_t merge_factor = static_cast<int32_t>(merge_factor_);
    int32_t start = level_start;
    int32_t end = start + merge_factor;
    while (OB_SUCC(ret) && merge_start_idx < 0 && end <= upto + 1) {
      int64_t merge_size = 0;
      for (int32_t i = start; i < end; ++i) {
        const int64_t segment_size = merge_infos.at(i).seg_size_;
        // too big, so end at i.
        if (merge_size + segment_size > max_merge_size_) {
          if (i == start) {
            end = i + 1;
          } else {
            end = i;
          }
          break;
        }
        merge_size += segment_size;
      }
      // too small, keep adding trailing segments until min_merge_size is reached.
      if (end - start >= merge_factor && merge_size < min_merge_size_) {
        while (end <= upto) {
          const int64_t segment_size = merge_infos.at(end).seg_size_;
          if (merge_size + segment_size > min_merge_size_) {
            break;
          }
          merge_size += segment_size;
          ++end;
        }
      }
      if (end - start > 1) {
        merge_start_idx = start;
        merge_end_idx = end - 1;
      }
      start = end;
      end = start + merge_factor;
    }
    if (OB_SUCC(ret) && merge_start_idx < 0) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("no need merge", K(ret), K(level_start), K(upto));
    }
  }
  return ret;
}

int ObAsyncIndexMergePolicy::find_merges(
    ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
    ObNewVersionRange &merge_range)
{
  int ret = OB_SUCCESS;
  merge_range.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (merge_infos.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no merge infos", K(ret), K(merge_infos.count()));
  } else if (OB_FAIL(validate_merge_infos_(merge_infos))) {
    LOG_WARN("validate merge infos failed", K(ret));
  } else if (OB_FAIL(calc_and_fill_levels_(merge_infos))) {
    LOG_WARN("calc and fill levels failed", K(ret));
  } else {
    const int32_t count = merge_infos.count();
    const double level_floor = calc_level_floor_();
    bool found = false;
    int32_t merge_start_idx = -1;
    int32_t merge_end_idx = -1;
    for (int32_t start = 0; OB_SUCC(ret) && !found && start < count; ) {
      float max_level = merge_infos.at(start).level_;
      for (int32_t idx = start + 1; idx < count; ++idx) {
        max_level = MAX(max_level, merge_infos.at(idx).level_);
      }
      // ES LogMergePolicy: merge band on [start..n) is [level_bottom, max_level].
      // Normally bottom = max - LEVEL_LOG_SPAN; when max_level <= level_floor, use 2×span
      // so segments near minMergeSize are not excluded by an overly narrow band.
      const float level_bottom = (max_level > level_floor)
          ? (max_level - LEVEL_LOG_SPAN)
          : (max_level - 2 * LEVEL_LOG_SPAN);
      int32_t upto = count - 1;
      // shrink upto from the tail until level >= level_bottom,
      // so pick_merge_at_level_ only searches [start, upto] within the merge band.
      while (upto >= start && merge_infos.at(upto).level_ < level_bottom) {
        --upto;
      }
      if (upto >= start) {
        const int tmp_ret = pick_merge_at_level_(merge_infos,
                                                 start,
                                                 upto,
                                                 merge_start_idx,
                                                 merge_end_idx);
        if (OB_SUCCESS == tmp_ret) {
          found = true;
        } else if (OB_NO_NEED_MERGE != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("pick merge at level failed", K(ret), K(start), K(upto));
        }
      }
      if (upto < start) {
        start = start + 1;
      } else {
        start = upto + 1;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (found) {
      if (merge_start_idx >= 0 && merge_end_idx >= merge_start_idx && merge_end_idx < merge_infos.count()) {
        merge_range.base_version_ = merge_infos.at(merge_start_idx).start_snapshot_version_;
        merge_range.snapshot_version_ = merge_infos.at(merge_end_idx).end_snapshot_version_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected start and end idx", K(ret), K(merge_start_idx), K(merge_end_idx), K(merge_infos.count()));
      }
    } else {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("no need merge", K(merge_infos));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DISABLE_ASYNC_INDEX_INC_MERGE, "When set, async index inc merge is disabled.");
ERRSIM_POINT_DEF(ERRSIM_FORCE_ASYNC_INDEX_INC_MERGE, "When set, async index inc merge is forced for adaptive merge");
int ObAsyncIndexMergePolicy::choose_async_index_merge_range(
    ObLS &ls,
    const ObTablet &data_tablet,
    const ObIArray<ObTabletID> &index_tablet_ids,
    const ObTabletID &delete_index_tablet_id,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason,
    const share::schema::ObAsyncIndexParams &async_index_params,
    ObNewVersionRange &merge_version_range)
{
  int ret = OB_SUCCESS;
  int64_t schedule_snapshot_version = INT64_MAX;
  const bool force_medium_major = 
      ObAdaptiveMergePolicy::TENANT_MAJOR == merge_reason
      || ObAdaptiveMergePolicy::WINDOW_COMPACTION == merge_reason
      || ObAdaptiveMergePolicy::USER_REQUEST == merge_reason
      || ObAdaptiveMergePolicy::ASYNC_INDEX_FORCE_MERGE == merge_reason;
  ObSEArray<ObSEArray<ObAsyncIndexSSTableMergeInfo, 8>, 4> all_merge_infos;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> final_merge_infos;
  ObSEArray<ObTabletID, 4> merge_tablet_ids;
  int64_t delete_percentage = 0;
  if (OB_FAIL(merge_tablet_ids.assign(index_tablet_ids))) {
    LOG_WARN("failed to copy async index tablet ids", KR(ret), K(index_tablet_ids));
  } else if (delete_index_tablet_id.is_valid()
             && OB_FAIL(merge_tablet_ids.push_back(delete_index_tablet_id))) {
    LOG_WARN("failed to add async delete index tablet", KR(ret), K(delete_index_tablet_id));
  } else if (OB_FAIL(ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
      ls, merge_tablet_ids, schedule_snapshot_version))) {
    LOG_WARN("failed to calc schedule snapshot version", KR(ret), K(merge_tablet_ids));
  } else if (INT64_MAX == schedule_snapshot_version) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("schedule snapshot version is INT64_MAX, so skip", KR(ret));
  } else if (force_medium_major) {
    merge_version_range.base_version_ = 0;
    merge_version_range.snapshot_version_ = schedule_snapshot_version;
    LOG_INFO("force async index medium major merge", KR(ret), K(merge_version_range), K(schedule_snapshot_version), K(merge_reason));
  } else if (OB_FAIL(ObAsyncIndexMergePolicyHelper::calc_delete_percentage(
      ls, data_tablet, delete_index_tablet_id, delete_percentage))) {
    LOG_WARN("failed to calc delete percentage", KR(ret), K(data_tablet));
  } else if (delete_percentage >= async_index_params.delete_purge_threshold_) {
    merge_version_range.base_version_ = 0;
    merge_version_range.snapshot_version_ = schedule_snapshot_version;
    LOG_INFO("choose async index medium merge range by delete percentage", KR(ret), K(merge_version_range), K(schedule_snapshot_version), K(merge_reason), K(delete_percentage), K(async_index_params));
  } else {
    ObAsyncIndexMergePolicy merge_policy;
    int64_t final_merge_info_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_tablet_ids.count(); ++i) {
      const ObTabletID &index_tablet_id = index_tablet_ids.at(i);
      ObTabletHandle index_handle;
      ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> merge_infos;
      if (OB_FAIL(ls.get_tablet(index_tablet_id, index_handle))) {
        LOG_WARN("failed to get index tablet", KR(ret), K(i), K(index_tablet_id));
      } else if (OB_UNLIKELY(!index_handle.is_valid() || OB_ISNULL(index_handle.get_obj()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index tablet handle", KR(ret), K(i), K(index_tablet_id));
      } else if (OB_FAIL(ObAsyncIndexMergePolicyHelper::collect_merge_infos(
          ls, *index_handle.get_obj(), schedule_snapshot_version, merge_infos))) {
        LOG_WARN("failed to collect async index inc major merge infos", KR(ret), K(i), K(index_tablet_id), K(schedule_snapshot_version));
      } else if (OB_FAIL(all_merge_infos.push_back(merge_infos))) {
        LOG_WARN("failed to push back merge infos", KR(ret), K(i), K(index_tablet_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (all_merge_infos.count() <= 0) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("no merge info, skip merge", KR(ret));
    } else if (OB_FAIL(final_merge_infos.assign(all_merge_infos.at(0)))) {
      LOG_WARN("failed to copy merge infos", KR(ret), K(all_merge_infos.at(0)));
    } else {
      final_merge_info_cnt = final_merge_infos.count();
      for (int64_t i = 1; OB_SUCC(ret) && i < all_merge_infos.count(); ++i) {
        const int64_t curr_cnt = all_merge_infos.at(i).count();
        if (curr_cnt != final_merge_info_cnt) {
          ret = OB_NO_NEED_MERGE;
          LOG_INFO("index merge info count is not aligned, skip merge", KR(ret), K(i), K(final_merge_info_cnt), K(curr_cnt), K(index_tablet_ids.at(i)));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < curr_cnt; ++j) {
            const ObAsyncIndexSSTableMergeInfo &curr_merge_info = all_merge_infos.at(i).at(j);
            ObAsyncIndexSSTableMergeInfo &final_merge_info = final_merge_infos.at(j);
            if (curr_merge_info.start_snapshot_version_ != final_merge_info.start_snapshot_version_
                || curr_merge_info.end_snapshot_version_ != final_merge_info.end_snapshot_version_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("merge info mismatch", KR(ret), K(i), K(j), K(curr_merge_info), K(final_merge_info));
            } else {
              final_merge_info.seg_size_ += curr_merge_info.seg_size_;
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (final_merge_info_cnt > 2
#ifdef ERRSIM
        && storage::ObSSGCUtil::is_errsim_target(
            ls.get_tenant_id(), data_tablet.get_tablet_meta().tablet_id_.id())
#endif
        && ERRSIM_FORCE_ASYNC_INDEX_INC_MERGE != OB_SUCCESS) {
      if (final_merge_infos.at(0).start_snapshot_version_ == 0) {
        merge_version_range.base_version_ = final_merge_infos.at(1).start_snapshot_version_;
      } else {
        merge_version_range.base_version_ = final_merge_infos.at(0).start_snapshot_version_;
      }
      merge_version_range.snapshot_version_ = schedule_snapshot_version;
      LOG_INFO("async index inc merge by errsim", K(merge_version_range), K(schedule_snapshot_version), K(merge_reason));
    } else if (final_merge_info_cnt < async_index_params.merge_segment_threshold_) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("sstable info count is less than merge segment threshold, skip merge", KR(ret), K(final_merge_info_cnt), K(async_index_params));
    } else if (ERRSIM_DISABLE_ASYNC_INDEX_INC_MERGE != OB_SUCCESS) {
      merge_version_range.base_version_ = 0;
      merge_version_range.snapshot_version_ = schedule_snapshot_version;
      LOG_INFO("async index medium merge by errsim", KR(ret), K(merge_version_range), K(schedule_snapshot_version), K(merge_reason));
    } else if (final_merge_info_cnt > MAX_INC_MAJOR_SSTABLE_CNT / 2) {
      // inc sstable 太多了, 不需要通过策略选表, 直接合并所有表
      // 但是为了避免基线过多, 导致合并代价过大, 需要检查基线占比, 小于50%才合并基线, 以保证基线一定是最大的sstable
      int64_t total_size = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < final_merge_info_cnt; ++i) {
        total_size += final_merge_infos.at(i).seg_size_;
      }
      int64_t base_size = 0;
      int64_t first_merge_idx = 0;
      if (final_merge_infos.at(0).start_snapshot_version_ == 0) {
        base_size = final_merge_infos.at(0).seg_size_;
        if (base_size > 0 && total_size - base_size < base_size) {
          first_merge_idx = 1;
        }
      }
      merge_version_range.base_version_ = final_merge_infos.at(first_merge_idx).start_snapshot_version_;
      merge_version_range.snapshot_version_ = schedule_snapshot_version;
      LOG_INFO("async index medium merge by too many inc sstable", KR(ret), K(merge_version_range), K(schedule_snapshot_version), K(merge_reason));
    } else if (OB_FAIL(merge_policy.init(async_index_params.merge_segment_threshold_))) {
      LOG_WARN("failed to init async index merge policy", KR(ret));
    } else if (OB_FAIL(merge_policy.find_merges(final_merge_infos, merge_version_range))) {
      LOG_WARN("failed to find async index inc major merge range", KR(ret), K(final_merge_infos));
    } else {
      LOG_INFO("choose async index inc major merge range", KR(ret), K(merge_version_range), K(final_merge_infos));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("finish to choose async index merge range",
        "data_tablet_id", data_tablet.get_tablet_id(), K(index_tablet_ids),
        K(delete_index_tablet_id), K(merge_reason), K(schedule_snapshot_version),
        K(merge_version_range), K(delete_percentage), K(final_merge_infos));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
