/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_RESOURCE_MANAGER_OB_DATABASE_ISOLATION_UTILS_H_
#define OCEANBASE_SHARE_RESOURCE_MANAGER_OB_DATABASE_ISOLATION_UTILS_H_

#include "common/ob_version_def.h"

namespace oceanbase
{
namespace share
{
// Feature activation thresholds; these do not advance the release versions.
constexpr uint64_t DATABASE_ISOLATION_MIN_CLUSTER_VERSION = common::cal_version(4, 6, 3, 1);
constexpr uint64_t DATABASE_ISOLATION_MIN_DATA_VERSION = common::cal_version(4, 6, 3, 1);

inline bool is_database_isolation_version_supported(const uint64_t cluster_version,
                                                    const uint64_t data_version)
{
  return cluster_version >= DATABASE_ISOLATION_MIN_CLUSTER_VERSION
      && data_version >= DATABASE_ISOLATION_MIN_DATA_VERSION;
}
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_RESOURCE_MANAGER_OB_DATABASE_ISOLATION_UTILS_H_
