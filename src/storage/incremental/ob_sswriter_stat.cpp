/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ob_sswriter_stat.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
constexpr const char SSWriterModID::OB_SSWRITER_GROUP_STAT[];
constexpr const char SSWriterModID::OB_SSWRITER_MGR_STAT[];

void ObSSWriterGroupStat::reset()
{
  key_.reset();
  group_id_ = -1;
  grant_server_.reset();
  current_ts_ = -1;
  remaining_lease_us_ = -1;
  lease_expire_ts_ = -1;
  create_ts_ = -1;
  epoch_ = -1;
  last_request_lease_ts_ = -1;
  is_inited_ = false;
}

int ObSSWriterGroupStat::init(
    const ObLsSSWriterKey &key,
    const int64_t group_id,
    const ObAddr &grant_server,
    const int64_t current_ts,
    const int64_t remaining_lease_us,
    const int64_t lease_expire_ts,
    const int64_t create_ts,
    const int64_t epoch,
    const int64_t last_request_lease_ts)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterGroupStat inited twice", K(ret));
  } else {
    key_ = key;
    group_id_ = group_id;
    grant_server_ = grant_server;
    current_ts_ = current_ts;
    remaining_lease_us_ = remaining_lease_us;
    lease_expire_ts_ = lease_expire_ts;
    create_ts_ = create_ts;
    epoch_ = epoch;
    last_request_lease_ts_ = last_request_lease_ts;
    is_inited_ = true;
  }
  return ret;
}

void ObSSWriterMgrStat::reset()
{
  group_id_ = -1;
  target_server_.reset();
  is_inited_ = false;
}

int ObSSWriterMgrStat::init(
    const share::ObLSID ls_id,
    const ObSSWriterType sswriter_type,
    const int64_t group_id,
    const ObRegion &region,
    const ObSSWriterGroupState state,
    const ObAddr &target_server,
    const int64_t current_ts,
    const int64_t last_update_state_ts,
    const int64_t lease_expire_ts,
    const int64_t epoch)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSWriterMgrStat inited twice", K(ret));
  } else {
    ls_id_ = ls_id;
    sswriter_type_ = sswriter_type;
    group_id_ = group_id;
    region_ = region;
    state_ = state;
    target_server_ = target_server;
    current_ts_ = current_ts;
    last_update_state_ts_ = last_update_state_ts;
    lease_expire_ts_ = lease_expire_ts;
    epoch_ = epoch;
    is_inited_ = true;
  }
  return ret;
}

} // storage
} // oceanbase

