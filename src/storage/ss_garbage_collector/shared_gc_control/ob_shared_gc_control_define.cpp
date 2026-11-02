/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_gc_control_define.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;

static const char *const SHARED_GC_CONTROL_STATUS_STR[] = {
  "IDLE",
  "DOING",
};
STATIC_ASSERT(ARRAYSIZEOF(SHARED_GC_CONTROL_STATUS_STR)
              == static_cast<int64_t>(ObSharedGCControlStatus::MAX),
              "shared gc control status string array size mismatch");

bool is_valid_shared_gc_control_status(const ObSharedGCControlStatus status)
{
  return (status >= ObSharedGCControlStatus::IDLE)
      && (status < ObSharedGCControlStatus::MAX);
}

const char *shared_gc_control_status_to_str(const ObSharedGCControlStatus status)
{
  const char *str = "UNKNOWN";
  if (is_valid_shared_gc_control_status(status)) {
    str = SHARED_GC_CONTROL_STATUS_STR[static_cast<int64_t>(status)];
  }
  return str;
}

int shared_gc_control_status_from_str(const ObString &str, ObSharedGCControlStatus &status)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; !found && (i < ARRAYSIZEOF(SHARED_GC_CONTROL_STATUS_STR)); ++i) {
    if (0 == str.case_compare(SHARED_GC_CONTROL_STATUS_STR[i])) {
      status = static_cast<ObSharedGCControlStatus>(i);
      found = true;
    }
  }
  if (!found) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unknown shared gc control status string", KR(ret), K(str));
  }
  return ret;
}

ObSharedGCControlInfo::ObSharedGCControlInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    executor_server_id_(0),
    status_(ObSharedGCControlStatus::MAX),
    gc_round_id_(0),
    allow_gc_round_id_(0),
    gc_meta_scn_(),
    last_succ_gc_meta_scn_(),
    start_time_(0)
{
}

ObSharedGCControlInfo::ObSharedGCControlInfo(
    const uint64_t tenant_id,
    const int64_t executor_server_id,
    const ObSharedGCControlStatus status,
    const int64_t gc_round_id,
    const int64_t allow_gc_round_id,
    const share::SCN gc_meta_scn,
    const share::SCN last_succ_gc_meta_scn,
    const int64_t start_time)
  : tenant_id_(tenant_id), executor_server_id_(executor_server_id), status_(status),
    gc_round_id_(gc_round_id),
    allow_gc_round_id_(allow_gc_round_id),
    gc_meta_scn_(gc_meta_scn),
    last_succ_gc_meta_scn_(last_succ_gc_meta_scn),
    start_time_(start_time)
{
}

void ObSharedGCControlInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  executor_server_id_ = 0;
  status_ = ObSharedGCControlStatus::MAX;
  gc_round_id_ = 0;
  allow_gc_round_id_ = 0;
  gc_meta_scn_.reset();
  last_succ_gc_meta_scn_.reset();
  start_time_ = 0;
}

bool ObSharedGCControlInfo::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
      && is_valid_server_id(static_cast<uint64_t>(executor_server_id_))
      && is_valid_shared_gc_control_status(status_)
      && (gc_round_id_ >= 0)
      && (allow_gc_round_id_ >= 0)
      && (start_time_ >= 0);
}

int ObSharedGCControlInfo::assign(const ObSharedGCControlInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (this != &other) {
    tenant_id_ = other.tenant_id_;
    executor_server_id_ = other.executor_server_id_;
    status_ = other.status_;
    gc_round_id_ = other.gc_round_id_;
    allow_gc_round_id_ = other.allow_gc_round_id_;
    gc_meta_scn_ = other.gc_meta_scn_;
    last_succ_gc_meta_scn_ = other.last_succ_gc_meta_scn_;
    start_time_ = other.start_time_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
