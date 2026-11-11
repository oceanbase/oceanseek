/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/tablet/ob_tablet_mds_data_cache.h"

#include "storage/tablet/ob_async_index_refresh_info.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"

using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{
ObTabletStatusCache::ObTabletStatusCache()
  : tablet_status_(ObTabletStatus::MAX),
    create_commit_version_(ObTransVersion::INVALID_TRANS_VERSION),
    delete_commit_version_(ObTransVersion::INVALID_TRANS_VERSION)
{
}

void ObTabletStatusCache::set_value(
    const ObTabletStatus &tablet_status,
    const int64_t create_commit_version,
    const int64_t delete_commit_version)
{
  tablet_status_ = tablet_status;
  create_commit_version_ = create_commit_version;
  delete_commit_version_ = delete_commit_version;
}

void ObTabletStatusCache::set_value(const ObTabletCreateDeleteMdsUserData &user_data)
{
  tablet_status_ = user_data.tablet_status_;
  create_commit_version_ = user_data.create_commit_version_;
  delete_commit_version_ = user_data.delete_commit_version_;
}

void ObTabletStatusCache::reset()
{
  tablet_status_ = ObTabletStatus::MAX;
  create_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  delete_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
}


ObDDLInfoCache::ObDDLInfoCache()
  : redefined_(false),
    invalidation_count_(0),
    schema_version_(INT64_MAX),
    snapshot_version_(INT64_MAX)
{
}

void ObDDLInfoCache::set_value(
    const bool redefined,
    const int64_t schema_version,
    const int64_t snapshot_version)
{
  redefined_ = redefined;
  schema_version_ = schema_version;
  snapshot_version_ = snapshot_version;
}

void ObDDLInfoCache::set_value(const ObTabletBindingMdsUserData &user_data)
{
  redefined_ = user_data.redefined_;
  schema_version_ = user_data.schema_version_;
  snapshot_version_ = user_data.snapshot_version_;
}

void ObDDLInfoCache::reset()
{
  // Even an invalid cache may have a reader preparing to populate it.
  ++invalidation_count_;
  redefined_ = false;
  schema_version_ = INT64_MAX;
  snapshot_version_ = INT64_MAX;
}

ObAsyncIndexMembershipCache::ObAsyncIndexMembershipCache()
  : published_version_(ObTabletAsyncIndexRefreshInfo::INVALID_PUBLISHED_VERSION),
    invalidation_count_(0),
    is_valid_(false),
    has_async_index_(false)
{
}

void ObAsyncIndexMembershipCache::set_value(
    const bool has_async_index,
    const int64_t published_version)
{
  published_version_ = published_version;
  has_async_index_ = has_async_index;
  is_valid_ = true;
}

void ObAsyncIndexMembershipCache::reset()
{
  // Even an invalid cache may have a reader preparing to populate it.
  ++invalidation_count_;
  published_version_ = ObTabletAsyncIndexRefreshInfo::INVALID_PUBLISHED_VERSION;
  is_valid_ = false;
  has_async_index_ = false;
}

} // namespace storage
} // namespace oceanbase
