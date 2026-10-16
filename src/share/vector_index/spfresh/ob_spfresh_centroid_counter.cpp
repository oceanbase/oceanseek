/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_centroid_counter.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace share
{

ObSpfreshCentroidCounter::ObSpfreshCentroidCounter()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    lock_(common::ObLatchIds::OB_SPFRESH_CENTROID_COUNTER_LOCK),
    counter_map_()
{
}

ObSpfreshCentroidCounter::~ObSpfreshCentroidCounter()
{
  destroy();
}

int ObSpfreshCentroidCounter::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSpfreshCentroidCounter init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    if (OB_FAIL(counter_map_.create(64, lib::ObMemAttr(tenant_id, "SpfreshCentCnt")))) {
      LOG_WARN("failed to create counter map", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObSpfreshCentroidCounter::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_ID;
    counter_map_.destroy();
  }
}

int ObSpfreshCentroidCounter::inc_center_count(uint64_t center_id, int64_t delta,
                                                 int64_t &new_count)
{
  int ret = OB_SUCCESS;
  new_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("counter not inited", K(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    int64_t old_count = 0;
    if (OB_FAIL(counter_map_.get_refactored(center_id, old_count))) {
      if (OB_HASH_NOT_EXIST == ret) {
        old_count = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get center count", K(ret), K(center_id));
      }
    }
    if (OB_SUCC(ret)) {
      new_count = old_count + delta;
      if (OB_FAIL(counter_map_.set_refactored(center_id, new_count, 1))) {
        LOG_WARN("failed to set center count", K(ret), K(center_id), K(new_count));
      }
    }
  }
  return ret;
}

int ObSpfreshCentroidCounter::dec_center_count(uint64_t center_id, int64_t delta,
                                                 int64_t &new_count)
{
  int ret = OB_SUCCESS;
  new_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("counter not inited", K(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    int64_t old_count = 0;
    if (OB_FAIL(counter_map_.get_refactored(center_id, old_count))) {
      if (OB_HASH_NOT_EXIST == ret) {
        old_count = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get center count", K(ret), K(center_id));
      }
    }
    if (OB_SUCC(ret)) {
      new_count = old_count - delta;
      if (new_count < 0) {
        new_count = 0;
      }
      if (OB_FAIL(counter_map_.set_refactored(center_id, new_count, 1))) {
        LOG_WARN("failed to set center count", K(ret), K(center_id), K(new_count));
      }
    }
  }
  return ret;
}

int ObSpfreshCentroidCounter::get_center_count(uint64_t center_id, int64_t &count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("counter not inited", K(ret));
  } else {
    ObSpinLockGuard guard(const_cast<ObSpfreshCentroidCounter*>(this)->lock_);
    int tmp_ret = counter_map_.get_refactored(center_id, count);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      count = 0;
    } else if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to get center count", K(ret), K(center_id));
    }
  }
  return ret;
}

int ObSpfreshCentroidCounter::set_center_count(uint64_t center_id, int64_t count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("counter not inited", K(ret));
  } else if (count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid count", K(ret), K(count));
  } else {
    ObSpinLockGuard guard(lock_);
    if (OB_FAIL(counter_map_.set_refactored(center_id, count, 1))) {
      LOG_WARN("failed to set center count", K(ret), K(center_id), K(count));
    }
  }
  return ret;
}

void ObSpfreshCentroidCounter::reset()
{
  ObSpinLockGuard guard(lock_);
  counter_map_.clear();
}

} // namespace share
} // namespace oceanbase