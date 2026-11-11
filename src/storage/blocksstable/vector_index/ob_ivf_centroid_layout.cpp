/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/vector_index/ob_ivf_centroid_layout.h"
#include "storage/blocksstable/vector_index/ob_ivf_parent_centroid_build.h"
#include "storage/blocksstable/vector_index/ob_ivf_hca_algorithm_adapter.h"
#include "lib/allocator/page_arena.h"
#include "lib/time/ob_time_utility.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/vector_type/ob_vector_common_util.h"
#include <cmath>

namespace oceanbase
{
using namespace common;
using namespace share;
namespace blocksstable
{
ObIvfCentroidLayout::ObIvfCentroidLayout(ObIAllocator &allocator)
  : allocator_(allocator), parents_(), primary_(), secondary_(), offsets_(), members_(),
    dim_(0), leaf_count_(0), ready_(false)
{
  parents_.set_block_allocator(ObWrapperAllocator(allocator));
  primary_.set_block_allocator(ObWrapperAllocator(allocator));
  secondary_.set_block_allocator(ObWrapperAllocator(allocator));
  offsets_.set_block_allocator(ObWrapperAllocator(allocator));
  members_.set_block_allocator(ObWrapperAllocator(allocator));
}

void ObIvfCentroidLayout::reset()
{
  parents_.reset(); primary_.reset(); secondary_.reset(); offsets_.reset(); members_.reset();
  dim_ = leaf_count_ = 0;
  ready_ = false;
}

int64_t ObIvfCentroidLayout::get_hierarchy_threshold()
{
  int64_t threshold = IVF_CENTROID_HIERARCHY_THRESHOLD;
  omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
  if (config.is_valid()) { threshold = config->_disk_bbq_centroid_hierarchy_threshold; }
  return threshold;
}

int ObIvfCentroidLayout::begin_grouped(const ObIArray<float> &parents,
    int64_t leaf_count, int64_t dimension)
{
  int ret = OB_SUCCESS;
  reset();
  if (dimension <= 0 || leaf_count <= 0 || leaf_count > INT32_MAX || parents.empty()
      || parents.count() % dimension != 0 || parents.count() / dimension > leaf_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid grouped centroid layout input", K(ret), K(dimension),
        K(leaf_count), "parent_values", parents.count());
  } else if (OB_FAIL(parents_.assign(parents))) {
    LOG_WARN("[DISK_BBQ] failed to copy parent centroid values", K(ret), "value_count", parents.count());
  } else if (OB_FAIL(primary_.prepare_allocate(leaf_count))) {
    LOG_WARN("[DISK_BBQ] failed to allocate primary centroid memberships", K(ret), K(leaf_count));
  } else if (OB_FAIL(secondary_.prepare_allocate(leaf_count))) {
    LOG_WARN("[DISK_BBQ] failed to allocate secondary centroid memberships", K(ret), K(leaf_count));
  } else {
    dim_ = dimension;
    leaf_count_ = leaf_count;
    MEMSET(primary_.get_data(), -1, primary_.count() * sizeof(int32_t));
    MEMSET(secondary_.get_data(), -1, secondary_.count() * sizeof(int32_t));
    for (int64_t i = 0; OB_SUCC(ret) && i < parents_.count(); ++i) {
      if (!std::isfinite(parents_.at(i))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("[DISK_BBQ] non-finite parent centroid coordinate", K(ret), K(i), K_(dim));
      }
    }
  }
  if (OB_FAIL(ret)) { reset(); }
  return ret;
}

int ObIvfCentroidLayout::set_parent(int64_t leaf_idx, int64_t parent_idx, bool secondary)
{
  int ret = OB_SUCCESS;
  if (ready_ || leaf_idx < 0 || leaf_idx >= leaf_count_ || parent_idx < 0 || parent_idx >= parent_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid centroid membership", K(ret), K_(ready), K(leaf_idx),
        K(parent_idx), K(secondary), K_(leaf_count), "parent_count", parent_count());
  } else {
    int32_t &slot = secondary ? secondary_.at(leaf_idx) : primary_.at(leaf_idx);
    if (slot != -1 || (secondary && primary_.at(leaf_idx) == parent_idx)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] duplicate centroid membership", K(ret), K(leaf_idx), K(parent_idx), K(secondary), K(slot));
    }
    else { slot = static_cast<int32_t>(parent_idx); }
  }
  return ret;
}

int ObIvfCentroidLayout::finish_grouped()
{
  int ret = OB_SUCCESS;
  if (ready_ || leaf_count_ <= 0 || parent_count() <= 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] invalid centroid layout completion state", K(ret), K_(ready),
        K_(leaf_count), "parent_count", parent_count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < leaf_count_; ++i) {
    if (primary_.at(i) < 0 || primary_.at(i) >= parent_count()
        || secondary_.at(i) < -1 || secondary_.at(i) >= parent_count()
        || primary_.at(i) == secondary_.at(i)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] invalid completed centroid membership", K(ret), K(i),
          "primary", primary_.at(i), "secondary", secondary_.at(i), "parent_count", parent_count());
    }
  }
  if (OB_SUCC(ret)) { ret = build_groups(); }
  if (OB_FAIL(ret)) { reset(); }
  else { ready_ = true; }
  return ret;
}

int ObIvfCentroidLayout::build_groups()
{
  int ret = OB_SUCCESS;
  const int64_t p = parent_count();
  ObArray<uint32_t, ObWrapperAllocator> cursors;
  cursors.set_block_allocator(ObWrapperAllocator(allocator_));
  if (OB_FAIL(offsets_.prepare_allocate(p + 1))) {
    LOG_WARN("[DISK_BBQ] failed to allocate centroid group offsets", K(ret), K(p));
  } else if (OB_FAIL(cursors.prepare_allocate(p))) {
    LOG_WARN("[DISK_BBQ] failed to allocate centroid group cursors", K(ret), K(p));
  } else {
    MEMSET(offsets_.get_data(), 0, (p + 1) * sizeof(int64_t));
    MEMSET(cursors.get_data(), 0, p * sizeof(uint32_t));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < leaf_count_; ++j) {
    const int32_t a = primary_.at(j), b = secondary_.at(j);
    if (a < 0 || a >= p || b < -1 || b >= p || a == b) {
      ret = OB_INVALID_DATA;
      LOG_WARN("[DISK_BBQ] invalid parent IDs while building centroid groups", K(ret), K(j), K(a), K(b), K(p));
    } else {
      ret = ObIvfCentroidPayload::add_child_count(cursors.at(a), 1);
      if (OB_SUCC(ret) && b >= 0) {
        ret = ObIvfCentroidPayload::add_child_count(cursors.at(b), 1);
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < p; ++i) {
      offsets_.at(i + 1) = offsets_.at(i) + cursors.at(i);
      // K <= INT32_MAX and each leaf has at most two memberships, so the
      // complete member array also fits uint32_t cursors.
      if (offsets_.at(i + 1) > UINT32_MAX) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("[DISK_BBQ] centroid group offset overflow", K(ret), K(i), "offset", offsets_.at(i + 1));
      } else {
        cursors.at(i) = static_cast<uint32_t>(offsets_.at(i));
      }
    }
    if (OB_SUCC(ret)) {
      ret = members_.prepare_allocate(offsets_.at(p));
      if (OB_FAIL(ret)) {
        LOG_WARN("[DISK_BBQ] failed to allocate centroid group members", K(ret), "member_count", offsets_.at(p));
      }
    }
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < leaf_count_; ++j) {
    members_.at(cursors.at(primary_.at(j))++) = j;
    if (secondary_.at(j) >= 0) { members_.at(cursors.at(secondary_.at(j))++) = j; }
  }
  return ret;
}

int ObIvfCentroidLayout::build(const ObIArrayWrap<float> &leaves, const int64_t dimension,
    const ObHcaTrainParam &train_param, const ObHcaSoarParam &soar_param,
    const ObVectorNormalizeInfo *norm_info, const int64_t threshold, const int64_t cluster_size)
{
  int ret = OB_SUCCESS;
  const int64_t started_at_us = ObTimeUtility::current_time();
  reset();
  if (dimension <= 0 || leaves.empty() || leaves.get_data() == nullptr || leaves.count() % dimension != 0
      || leaves.count() / dimension > INT32_MAX || threshold <= 0 || cluster_size <= 0
      || !train_param.is_valid() || !soar_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid centroid layout build input", K(ret), K(dimension), K(threshold),
        K(cluster_size), "leaf_values", leaves.count(), "has_leaf_data", leaves.get_data() != nullptr,
        "train_params_valid", train_param.is_valid(), "soar_params_valid", soar_param.is_valid());
  } else {
    const int64_t leaf_count = leaves.count() / dimension;
    if (leaf_count < threshold) {
      if (OB_FAIL(init_flat(leaf_count, dimension))) {
        LOG_WARN("[DISK_BBQ] failed to initialize flat centroid layout", K(ret), K(leaf_count), K(dimension));
      }
    } else {
      storage::ObIVFHcaAlgorithmParam param;
      param.train_param_ = train_param;
      param.soar_param_ = soar_param;
      param.center_norm_info_ = norm_info;
      storage::ObIVFParentCentroidBuilder builder(allocator_);
      bool finished = false;
      if (OB_FAIL(builder.init(leaves, dimension, param, cluster_size))) {
        LOG_WARN("[DISK_BBQ] failed to initialize centroid layout builder", K(ret),
            K(leaf_count), K(dimension), K(cluster_size));
      } else {
        // Compatibility callers without a DAG drive the same task/barrier
        // executor synchronously. DDL/Merge schedule these tasks in their DAG.
        for (int64_t plan_id = 0; OB_SUCC(ret) && !finished; ++plan_id) {
          int64_t task_count = 0;
          if (OB_FAIL(builder.prepare(plan_id, task_count))) {
            LOG_WARN("[DISK_BBQ] failed to prepare centroid layout plan", K(ret), K(plan_id), K(task_count));
          } else {
            for (int64_t task = 0; OB_SUCC(ret) && task < task_count; ++task) {
              if (OB_FAIL(share::dag_yield())) {
                LOG_WARN("[DISK_BBQ] failed to yield during centroid layout build", K(ret), K(plan_id), K(task));
              } else if (OB_FAIL(builder.execute(task))) {
                LOG_WARN("[DISK_BBQ] failed to execute centroid layout task", K(ret), K(plan_id), K(task));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(builder.finish(plan_id, *this, finished))) {
              LOG_WARN("[DISK_BBQ] failed to finish centroid layout plan", K(ret), K(plan_id), K(task_count));
            }
          }
        }
      }
    }
    ready_ = OB_SUCC(ret);
    if (ready_) {
      const int64_t layout_elapsed_us = ObTimeUtility::current_time() - started_at_us;
      LOG_INFO("built IVF centroid layout", K(threshold), K(cluster_size), K_(leaf_count),
          "parent_count", parent_count(), "center_row_count", center_row_count(), K(layout_elapsed_us));
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
