/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/tablet/ob_async_index_refresh_info.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

OB_DEF_SERIALIZE(ObTabletAsyncIndexRefreshInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              refresh_snapshot_versions_,
              has_async_index_,
              published_version_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletAsyncIndexRefreshInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              refresh_snapshot_versions_,
              has_async_index_);
  if (OB_SUCC(ret)) {
    if (pos < data_len) {
      OB_UNIS_DECODE(published_version_);
    } else {
      published_version_ = INVALID_PUBLISHED_VERSION;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTabletAsyncIndexRefreshInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              refresh_snapshot_versions_,
              has_async_index_,
              published_version_);
  return len;
}

void ObTabletAsyncIndexRefreshInfo::reset()
{
  refresh_snapshot_versions_.reset();
  has_async_index_ = false;
  published_version_ = INVALID_PUBLISHED_VERSION;
}

bool ObTabletAsyncIndexRefreshInfo::is_valid() const
{
  bool valid = published_version_ >= USE_COMMIT_REFRESH_SNAPSHOT;
  if (valid && !refresh_snapshot_versions_.empty()) {
    const int64_t first_snapshot = refresh_snapshot_versions_.at(0);
    if (is_pending_refresh_snapshot(first_snapshot)) {
      valid = need_fill_refresh_snapshot_on_commit();
    } else {
      valid = is_committed_refresh_snapshot(first_snapshot);
    }
  }
  for (int64_t i = 1; valid && i < refresh_snapshot_versions_.count(); ++i) {
    if (refresh_snapshot_versions_.at(i) <= refresh_snapshot_versions_.at(i - 1)) {
      valid = false;
    }
  }
  return valid;
}

bool ObTabletAsyncIndexRefreshInfo::is_valid_for_refresh() const
{
  bool valid = true;
  if (1 == refresh_snapshot_versions_.count()) {
    valid = is_committed_refresh_snapshot(refresh_snapshot_versions_.at(0));
  }
  return valid;
}

int ObTabletAsyncIndexRefreshInfo::assign(const ObTabletAsyncIndexRefreshInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_snapshot_versions_.assign(other.refresh_snapshot_versions_))) {
    LOG_WARN("fail to assign refresh snapshot versions", K(ret));
  } else {
    has_async_index_ = other.has_async_index_;
    published_version_ = other.published_version_;
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfo::init_pending_refresh_snapshot()
{
  int ret = OB_SUCCESS;
  if (!need_fill_refresh_snapshot_on_commit()) {
    // do nothing
  } else if (OB_UNLIKELY(!refresh_snapshot_versions_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected refresh snapshots for pending init", K(ret), K(refresh_snapshot_versions_), K(published_version_));
  } else if (OB_FAIL(refresh_snapshot_versions_.push_back(PENDING_REFRESH_SNAPSHOT))) {
    LOG_WARN("fail to init pending refresh snapshot", K(ret));
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfo::merge_floor_pin(const ObTabletAsyncIndexRefreshInfo &pin)
{
  int ret = OB_SUCCESS;
  const ObIArray<int64_t> &snapshots = pin.get_refresh_snapshots();
  if (OB_UNLIKELY(snapshots.count() != 1 || snapshots.at(0) <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(!is_valid() || !is_valid_for_refresh())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (!need_apply_floor_pin(pin)) {
    // An earlier checkpoint already protects this floor.
  } else if (refresh_snapshot_versions_.count() >= MAX_INC_MAJOR_SSTABLE_CNT) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    const int64_t count = refresh_snapshot_versions_.count();
    if (OB_FAIL(refresh_snapshot_versions_.push_back(snapshots.at(0)))) {
      LOG_WARN("failed to extend refresh snapshots for floor pin", K(ret));
    } else {
      for (int64_t i = count; i > 0; --i) {
        refresh_snapshot_versions_.at(i) = refresh_snapshot_versions_.at(i - 1);
      }
      refresh_snapshot_versions_.at(0) = snapshots.at(0);
    }
  }
  return ret;
}

bool ObTabletAsyncIndexRefreshInfo::need_apply_floor_pin(
    const ObTabletAsyncIndexRefreshInfo &new_info) const
{
  const ObIArray<int64_t> &new_snapshots = new_info.get_refresh_snapshots();
  const int64_t new_floor_pin = new_snapshots.empty() ? 0 : new_snapshots.at(0);
  return refresh_snapshot_versions_.empty() || refresh_snapshot_versions_.at(0) > new_floor_pin;
}

void ObTabletAsyncIndexRefreshInfo::on_commit(
    const share::SCN &commit_version,
    const share::SCN &commit_scn)
{
  const int64_t commit_val = commit_version.get_val_for_tx();
  UNUSED(commit_scn);
  // 仅随表创建的索引 Tablet 会写入占位值；普通 refresh 写明确版本。
  if (need_fill_commit_version()) {
    published_version_ = commit_val;
  } else if (need_fill_refresh_snapshot_on_commit()) {
    if (refresh_snapshot_versions_.count() >= 1
        && is_pending_refresh_snapshot(refresh_snapshot_versions_.at(0))
        && commit_val > 0) {
      refresh_snapshot_versions_.at(0) = commit_val;
    }
    published_version_ = INVALID_PUBLISHED_VERSION;
  }
}

// Append a new refresh snapshot version (called by the scheduling task).
// Requires: new_snapshot > the last element in the array.
// Returns OB_SIZE_OVERFLOW if the array has reached MAX_INC_MAJOR_SSTABLE_CNT.
int ObTabletAsyncIndexRefreshInfo::append_refresh_snapshot(const int64_t new_snapshot)
{
  int ret = OB_SUCCESS;
  const int64_t cnt = refresh_snapshot_versions_.count();
  const int64_t last_snapshot = cnt > 0 ? refresh_snapshot_versions_.at(cnt - 1) : 0;
  if (OB_UNLIKELY(new_snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid snapshot", K(ret), K(new_snapshot));
  } else if (cnt >= MAX_INC_MAJOR_SSTABLE_CNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("refresh snapshot versions array is full", K(ret), K(cnt), K(MAX_INC_MAJOR_SSTABLE_CNT));
  } else if (cnt > 0 && new_snapshot <= last_snapshot) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new snapshot must be strictly greater than the last entry", K(ret), K(new_snapshot), K(last_snapshot));
  } else if (OB_FAIL(refresh_snapshot_versions_.push_back(new_snapshot))) {
    LOG_WARN("fail to push back snapshot", K(ret), K(new_snapshot));
  }
  return ret;
}

// Remove all refresh snapshot versions < min_publish_snapshot from the head of a non-empty array,
// and ensure min_publish_snapshot is retained as the first element.
int ObTabletAsyncIndexRefreshInfo::cleanup_retired_snapshots(const int64_t min_publish_snapshot)
{
  int ret = OB_SUCCESS;
  const bool has_existing_snapshot = !refresh_snapshot_versions_.empty();
  if (OB_UNLIKELY(min_publish_snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid min_publish_snapshot", K(ret), K(min_publish_snapshot));
  } else {
    int64_t remove_cnt = 0;
    for (int64_t i = 0; i < refresh_snapshot_versions_.count(); ++i) {
      if (refresh_snapshot_versions_.at(i) < min_publish_snapshot) {
        ++remove_cnt;
      } else {
        break;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < remove_cnt; ++i) {
      if (OB_FAIL(refresh_snapshot_versions_.remove(0))) {
        LOG_WARN("fail to remove snapshot", K(ret));
      }
    }
    // 并保证当前最小发布版本位于数组首位。
    if (OB_SUCC(ret) && has_existing_snapshot
        && (refresh_snapshot_versions_.empty()|| refresh_snapshot_versions_.at(0) > min_publish_snapshot)) {
      const int64_t cnt = refresh_snapshot_versions_.count();
      if (OB_UNLIKELY(cnt >= MAX_INC_MAJOR_SSTABLE_CNT)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("refresh snapshot versions array is full", K(ret), K(cnt),
            K(MAX_INC_MAJOR_SSTABLE_CNT), K(min_publish_snapshot));
      } else if (OB_FAIL(refresh_snapshot_versions_.push_back(min_publish_snapshot))) {
        LOG_WARN("fail to extend refresh snapshot versions", K(ret), K(min_publish_snapshot));
      } else {
        for (int64_t i = cnt; i > 0; --i) {
          refresh_snapshot_versions_.at(i) = refresh_snapshot_versions_.at(i - 1);
        }
        refresh_snapshot_versions_.at(0) = min_publish_snapshot;
      }
    }
  }
  return ret;
}

// Find the next refresh target for an index: the first entry strictly greater than
// last_publish_snapshot.  Returns OB_ITER_END if the index has already caught up.
int ObTabletAsyncIndexRefreshInfo::get_next_refresh_snapshot(const int64_t last_publish_snapshot, int64_t &next_refresh_snapshot) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  int64_t lo = 0;
  int64_t hi = refresh_snapshot_versions_.count() - 1;
  if (OB_UNLIKELY(!is_valid_for_refresh())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("refresh snapshots not ready for refresh", K(ret), K(last_publish_snapshot), KPC(this));
  }
  while (OB_SUCC(ret) && lo <= hi) { // binary search
    const int64_t mid = lo + (hi - lo) / 2;
    if (refresh_snapshot_versions_.at(mid) <= last_publish_snapshot) {
      lo = mid + 1;
    } else {
      next_refresh_snapshot = refresh_snapshot_versions_.at(mid);
      found = true;
      hi = mid - 1;
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ITER_END;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
