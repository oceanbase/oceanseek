/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_H
#define OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_H

#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace share
{
class SCN;
}

namespace storage
{

class ObTabletAsyncIndexRefreshInfo
{
  OB_UNIS_VERSION(1);
public:
  static constexpr int64_t INVALID_PUBLISHED_VERSION = -1;
  // 随表创建异步索引时，建表事务先写入 USE_COMMIT_VERSION，on_commit()
  // 再将其替换成事务提交版本。旧 MDS 不含 published_version_ 时，继续
  // 从物理 TableStore 读取，直到后续 refresh 写入明确发布版本。
  static constexpr int64_t USE_COMMIT_VERSION = -2;
  static constexpr int64_t USE_COMMIT_REFRESH_SNAPSHOT = -3;
  static constexpr int64_t PENDING_REFRESH_SNAPSHOT = -4;

  ObTabletAsyncIndexRefreshInfo()
      : refresh_snapshot_versions_(),
        has_async_index_(false),
        published_version_(INVALID_PUBLISHED_VERSION)
  {}
  ~ObTabletAsyncIndexRefreshInfo() = default;

  void reset();
  bool is_valid() const;
  bool is_valid_for_refresh() const;
  int assign(const ObTabletAsyncIndexRefreshInfo &other);
  int append_refresh_snapshot(const int64_t new_snapshot);
  int cleanup_retired_snapshots(const int64_t min_publish_snapshot);
  int get_next_refresh_snapshot(const int64_t last_publish_snapshot, int64_t &next_refresh_snapshot) const;
  int init_pending_refresh_snapshot();
  bool need_apply_floor_pin(const ObTabletAsyncIndexRefreshInfo &new_info) const;
  int merge_floor_pin(const ObTabletAsyncIndexRefreshInfo &pin);
  const ObIArray<int64_t> &get_refresh_snapshots() const { return refresh_snapshot_versions_; }
  bool has_async_index() const { return has_async_index_; }
  void set_has_async_index(const bool has_async_index) { has_async_index_ = has_async_index; }
  int64_t get_published_version() const { return published_version_; }
  void set_published_version(const int64_t published_version) { published_version_ = published_version; }
  bool has_published_version() const { return published_version_ >= 0; }
  bool need_fill_commit_version() const { return USE_COMMIT_VERSION == published_version_; }
  bool need_fill_refresh_snapshot_on_commit() const { return USE_COMMIT_REFRESH_SNAPSHOT == published_version_; }
  static bool is_pending_refresh_snapshot(const int64_t snapshot) { return PENDING_REFRESH_SNAPSHOT == snapshot; }
  static bool is_committed_refresh_snapshot(const int64_t snapshot) { return snapshot > 0; }
  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn);
  TO_STRING_KV(K_(refresh_snapshot_versions), K_(has_async_index), K_(published_version));

private:
  // ordered, strictly monotonically increasing, capped at MAX_INC_MAJOR_SSTABLE_CNT
  ObSEArray<int64_t, common::MAX_INC_MAJOR_SSTABLE_CNT> refresh_snapshot_versions_;
  bool has_async_index_;
  int64_t published_version_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_H
