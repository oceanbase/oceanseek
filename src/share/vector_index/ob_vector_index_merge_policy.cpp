/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SHARE
#include "ob_vector_index_merge_policy.h"
#include "share/scn.h"
#include "lib/time/ob_time_utility.h"
#include <cmath>

namespace oceanbase
{
namespace share
{
int ObVecMergePlan::get_vector_count(const ObVecMergePolicyInput &input, int64_t &vector_count) const
{
  int ret = OB_SUCCESS;
  vector_count = 0;
  int64_t total = 0;
  if (begin_ < 0 || end_ <= begin_ || end_ > input.segments_.count()) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = begin_; OB_SUCC(ret) && i < end_; ++i) {
    const ObVecSegmentMergeInfo &info = input.segments_.at(i);
    if (info.vector_count_ < 0 || info.deleted_vector_count_ < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!info.readable_) {
      ret = OB_EAGAIN;
    } else if (info.live_vector_count() > INT64_MAX - total) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      total += info.live_vector_count();
    }
  }
  if (OB_SUCC(ret)) { vector_count = total; }
  return ret;
}

bool ObVectorIndexMergePolicy::reaches_threshold_(
    const uint64_t deleted, const uint64_t count, const uint64_t percentage)
{
  // Compare against ceil(count * percentage / 100) without overflowing the product.
  return count > 0 && deleted >= (count / 100) * percentage
      + ((count % 100) * percentage + 99) / 100;
}

int ObVectorIndexMergePolicy::validate_(const ObVecMergePolicyInput &input) const
{
  int ret = OB_SUCCESS;
  if (config_.max_vectors_ <= 0 || config_.merge_factor_ < 2
      || config_.delete_percentage_ < 0 || config_.delete_percentage_ > 100
      || config_.idle_us_ < 0 || input.base_count_ < 0
      || input.base_count_ > input.segments_.count() || input.deleted_vid_count_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid merge policy configuration or input", K(ret), K(config_.max_vectors_),
        K(config_.merge_factor_), K(config_.delete_percentage_), K(config_.idle_us_),
        K(input.base_count_), K(input.segments_.count()), K(input.deleted_vid_count_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input.segments_.count(); ++i) {
    if (input.segments_.at(i).vector_count_ < 0
        || input.segments_.at(i).deleted_vector_count_ < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid merge segment statistics", K(ret), K(i), K(input.segments_.at(i)));
    }
  }
  return ret;
}

bool ObVectorIndexMergePolicy::can_reclaim_insert_(const ObVecMergePolicyInput &input,
    const int64_t begin, const int64_t end, const bool for_freeze_rebuild) const
{
  bool can_reclaim = true;
  for (int64_t i = begin; input.has_async_index_ && can_reclaim && i < end; ++i) {
    const ObVecSegmentMergeInfo &info = input.segments_.at(i);
    if (for_freeze_rebuild && info.seg_type_ == ObVectorIndexSegmentType::FREEZE_PERSIST) {
      // Probe only: the actual plan rebuilds one FREEZE and retains its insert bitmap.
      continue;
    }
    // BASE sources already have query-safe visibility. Only INCR_MERGE needs
    // publication to cover its build snapshot before dropping the insert whitelist.
    can_reclaim = info.scn_ > 0 && input.async_published_scn_ > 0
        && (info.seg_type_ == ObVectorIndexSegmentType::BASE
            || info.seg_type_ == ObVectorIndexSegmentType::INCR_BASE
            || (info.seg_type_ == ObVectorIndexSegmentType::INCR_MERGE
                && info.scn_ <= input.async_published_scn_));
  }
  return can_reclaim;
}

bool ObVectorIndexMergePolicy::can_merge_window_(const ObVecMergePolicyInput &input,
    const int64_t begin, const int64_t end) const
{
  bool has_pruned_source = false;
  for (int64_t i = begin; input.has_async_index_ && !has_pruned_source && i < end; ++i) {
    has_pruned_source = i < input.base_count_
        || input.segments_.at(i).seg_type_ == ObVectorIndexSegmentType::INCR_BASE;
  }
  // A pruned source cannot supply an insert whitelist for an unsafe output.
  return !has_pruned_source || can_reclaim_insert_(input, begin, end);
}

bool ObVectorIndexMergePolicy::pick_window_(const ObVecMergePolicyInput &input,
    int64_t start, const int64_t stop,
    const int64_t required, ObVecMergePlan &plan, const bool for_freeze_rebuild) const
{
  bool found = false;
  const int64_t capacity = config_.merge_capacity();
  while (!found && stop - start >= required) {
    int64_t end = start;
    int64_t total = 0;
    bool has_pruned_source = false;
    bool can_reclaim = true;
    // required gates candidate positions, not the number of selected segments.
    // Extend within this tier/readable range until the next segment cannot fit.
    while (end < stop) {
      const ObVecSegmentMergeInfo &info = input.segments_.at(end);
      const int64_t size = info.live_vector_count();
      has_pruned_source |= end < input.base_count_ || info.seg_type_ == ObVectorIndexSegmentType::INCR_BASE;
      can_reclaim = can_reclaim && can_reclaim_insert_(input, end, end + 1, for_freeze_rebuild);
      if (size > capacity - total || (input.has_async_index_ && has_pruned_source && !can_reclaim)) {
        // Extending this window cannot restore capacity or reclamation eligibility.
        break;
      }
      total += size;
      ++end;
    }
    if (end - start >= 2) {
      plan.begin_ = start;
      plan.end_ = end;
      found = true;
    } else {
      start = MAX(end, start + 1);
    }
  }
  return found;
}

// Sizes in 10k vectors, M=100 and capacity=120 (all sources readable):
// [80,20,20] -> select all three: continue extending after reaching M.
// [80,20,20.1] -> select the first two: adding the third exceeds capacity.
// [80,20,80,20] -> select the first two, not the later window totaling 120:
// prefer the earliest valid start, then its longest valid contiguous window.
// [99,1] -> no TARGET: the additional input is below minimum growth.
// Unreadable sources stop this window, but do not block later readable ranges.
int ObVectorIndexMergePolicy::find_target_merge_(const ObVecMergePolicyInput &input,
    ObVecMergePlan &plan, const bool for_freeze_rebuild) const
{
  int ret = OB_NO_NEED_MERGE;
  bool pending = false;
  const int64_t capacity = config_.merge_capacity();
  for (int64_t start = 0; ret == OB_NO_NEED_MERGE && start < input.segments_.count(); ++start) {
    if (!input.segments_.at(start).readable_) {
      pending = true;
      // Skip this start, but keep searching later readable windows.
      continue;
    }
    int64_t total = 0;
    int64_t largest = 0;
    int64_t selected_end = -1;
    bool has_pruned_source = false;
    bool can_reclaim = true;
    const int64_t width = input.segments_.count() - start;
    for (int64_t offset = 0; offset < width; ++offset) {
      const ObVecSegmentMergeInfo &info = input.segments_.at(start + offset);
      if (!info.readable_) {
        // A contiguous merge window cannot cross an unreadable source.
        break;
      }
      const int64_t size = info.live_vector_count();
      has_pruned_source |= start + offset < input.base_count_
          || info.seg_type_ == ObVectorIndexSegmentType::INCR_BASE;
      can_reclaim = can_reclaim && can_reclaim_insert_(input, start + offset, start + offset + 1,
          for_freeze_rebuild);
      if (size >= config_.max_vectors_ || size > capacity - total
          || (input.has_async_index_ && has_pruned_source && !can_reclaim)) {
        // Retain the last valid prefix; extending this window cannot make it eligible.
        break;
      }
      total += size;
      largest = MAX(largest, size);
      // Round the minimum growth up to a whole vector; largest is below M.
      const int64_t min_growth = (largest * ObVecMergeConfig::TARGET_MIN_GROWTH_PERCENT + 99) / 100;
      if (offset > 0 && total >= config_.max_vectors_ && total - largest >= min_growth) {
        // Keep extending at this start; reaching the target is not a stop condition.
        selected_end = start + offset + 1;
      }
    }
    if (selected_end > start) {
      plan.begin_ = start;
      plan.end_ = selected_end;
      plan.strategy_ = ObVecMergePlan::TARGET_MERGE;
      ret = OB_SUCCESS;
    }
  }
  return ret == OB_NO_NEED_MERGE && pending ? OB_EAGAIN : ret;
}

static double get_merge_segment_level(const ObVecSegmentMergeInfo &info, const double log_factor)
{
  const int64_t size = info.live_vector_count();
  return std::log(static_cast<double>(MAX(size, 1))) / log_factor;
}

int ObVectorIndexMergePolicy::find_tiered_merge_(const ObVecMergePolicyInput &input,
    ObVecMergePlan &plan, const bool for_freeze_rebuild) const
{
  // Trim trailing segments below top - span in log_F(live_count) space.
  // The corresponding size boundary is max_size / F^span.
  constexpr double TIER_LEVEL_SPAN = 0.75;
  int ret = OB_NO_NEED_MERGE;
  bool pending = false;
  const double log_factor = std::log(static_cast<double>(config_.merge_factor_));
  for (int64_t block = 0; ret == OB_NO_NEED_MERGE && block < input.segments_.count();) {
    if (!input.segments_.at(block).readable_) {
      pending = true;
      ++block;
      continue;
    }
    int64_t block_end = block;
    while (block_end < input.segments_.count() && input.segments_.at(block_end).readable_) {
      ++block_end;
    }
    for (int64_t start = block; ret == OB_NO_NEED_MERGE && start < block_end;) {
      int64_t stop = block_end;
      double top = get_merge_segment_level(input.segments_.at(start), log_factor);
      for (int64_t i = start + 1; i < block_end; ++i) {
        top = MAX(top, get_merge_segment_level(input.segments_.at(i), log_factor));
      }
      while (stop > start
          && get_merge_segment_level(input.segments_.at(stop - 1), log_factor) < top - TIER_LEVEL_SPAN) {
        --stop;
      }
      if (pick_window_(input, start, stop, config_.merge_factor_, plan, for_freeze_rebuild)) {
        plan.strategy_ = ObVecMergePlan::TIERED_MERGE;
        ret = OB_SUCCESS;
      }
      start = stop;
    }
    block = block_end;
  }
  return ret == OB_NO_NEED_MERGE && pending ? OB_EAGAIN : ret;
}

int ObVectorIndexMergePolicy::find_delete_merge_(
    const ObVecMergePolicyInput &input, ObVecMergePlan &plan) const
{
  int ret = OB_NO_NEED_MERGE;
  for (int64_t i = 0; ret == OB_NO_NEED_MERGE && i < input.segments_.count(); ++i) {
    const ObVecSegmentMergeInfo &info = input.segments_.at(i);
    if (info.readable_ && can_merge_window_(input, i, i + 1)
        && reaches_threshold_(info.deleted_vector_count_, info.vector_count_, config_.delete_percentage_)) {
      plan.begin_ = i;
      plan.end_ = i + 1;
      plan.strategy_ = ObVecMergePlan::DELETE_MERGE;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObVectorIndexMergePolicy::find_residual_merge_(const ObVecMergePolicyInput &input,
    const int64_t now_us, ObVecMergePlan &plan, const bool for_freeze_rebuild) const
{
  int ret = OB_NO_NEED_MERGE;
  bool pending = false;
  int64_t latest = 0;
  for (int64_t i = 0; i < input.segments_.count(); ++i) {
    latest = MAX(latest, input.segments_.at(i).scn_);
    pending |= !input.segments_.at(i).readable_;
  }
  SCN latest_scn;
  if (latest <= 0
      || OB_SUCCESS != latest_scn.convert_for_inner_table_field(latest)
      || !latest_scn.is_valid()
      || now_us < latest_scn.convert_to_ts()
      || now_us - latest_scn.convert_to_ts() < config_.idle_us_) {
    // Wait until the latest published segment reaches the waiting interval.
  } else {
    // Ignore tiers, but retain readability and capacity bounds; require at least two segments.
    for (int64_t block = 0; ret == OB_NO_NEED_MERGE && block < input.segments_.count();) {
      if (!input.segments_.at(block).readable_) { ++block; continue; }
      int64_t block_end = block;
      while (block_end < input.segments_.count() && input.segments_.at(block_end).readable_) {
        ++block_end;
      }
      if (pick_window_(input, block, block_end, 2, plan, for_freeze_rebuild)) {
        plan.strategy_ = ObVecMergePlan::RESIDUAL_MERGE;
        ret = OB_SUCCESS;
      }
      block = block_end;
    }
  }
  return ret == OB_NO_NEED_MERGE && pending ? OB_EAGAIN : ret;
}

// 正常策略没有找到合并计划，且不存在源段不可读或删除统计未就绪等
// pending 状态时，尝试通过单段重建解除 FREEZE 对后续合并的阻塞。
//
// 例如：[INCR_BASE A, FREEZE B] 的大小满足合并条件，A 已被异步发布
// 水位覆盖，但 A 可能已回收 insert 白名单，而 B 的冻结 SCN 不能保证
// 图内事务均已提交。因此两段既不能直接合成为 INCR_BASE，也不能退回
// 依赖完整 insert 白名单的 INCR_MERGE。
//
// 先临时跳过 FREEZE 的回收资格检查，复用 TARGET/TIERED/RESIDUAL
// 搜索候选窗口。容量、层级、空闲时间和可读性条件仍然有效；候选窗口
// 包含 BASE/INCR_BASE 时，其他非 FREEZE 源段仍须满足安全回收条件。
//
// candidate 用于确认存在可推进的合并窗口；实际 plan 只选择其中第一个
// FREEZE，将其重建为保留 insert 白名单的 INCR_MERGE。等发布水位覆盖
// 新段 SCN 后，再由正常策略合并为 INCR_BASE；若窗口包含 BASE，则输出
// BASE。孤立 FREEZE 没有阻挡其他合并窗口时，不触发此兜底。
int ObVectorIndexMergePolicy::find_freeze_rebuild_(
    const ObVecMergePolicyInput &input, ObVecMergePlan &plan) const
{
  int ret = OB_NO_NEED_MERGE;
  ObVecMergePlan candidate;
  if (input.has_async_index_ && input.async_published_scn_ > 0) {
    // 最后一个参数 true 仅用于探查窗口，不表示允许回收 FREEZE 的 bitmap。
    ret = find_target_merge_(input, candidate, true);
    if (ret == OB_NO_NEED_MERGE) {
      ret = find_tiered_merge_(input, candidate, true);
    }
    if (ret == OB_NO_NEED_MERGE) {
      ret = find_residual_merge_(input, ObTimeUtility::current_time(), candidate, true);
    }
    if (OB_SUCC(ret)) {
      bool has_pruned_source = false;
      int64_t freeze = -1;
      for (int64_t i = candidate.begin_; i < candidate.end_; ++i) {
        const ObVectorIndexSegmentType type = input.segments_.at(i).seg_type_;
        has_pruned_source |= i < input.base_count_ || type == ObVectorIndexSegmentType::INCR_BASE;
        if (freeze < 0 && type == ObVectorIndexSegmentType::FREEZE_PERSIST) { freeze = i; }
      }
      if (has_pruned_source && freeze >= 0) {
        // 实际只重建选中的 FREEZE，不执行整个 candidate 窗口的合并。
        plan.begin_ = freeze;
        plan.end_ = freeze + 1;
        plan.strategy_ = ObVecMergePlan::FREEZE_REBUILD;
      } else {
        ret = OB_NO_NEED_MERGE;
      }
    }
  }
  return ret;
}

int ObVectorIndexMergePolicy::find_manual_merge_(
    const ObVecMergePolicyInput &input, ObVecMergePlan &plan) const
{
  int ret = OB_NO_NEED_MERGE;
  if (!input.segments_.empty()) {
    // Used only when preceding strategies found no plan and no segment is pending.
    plan.begin_ = MAX(input.base_count_ - 1, 0);
    plan.end_ = input.segments_.count();
    while (plan.begin_ < plan.end_ && !can_merge_window_(input, plan.begin_, plan.end_)) {
      ++plan.begin_;
    }
    // Manual fallback must combine segments; single-segment rebuilds use other strategies.
    if (plan.end_ - plan.begin_ >= 2) {
      plan.strategy_ = ObVecMergePlan::MANUAL_MERGE;
      ret = OB_SUCCESS;
    } else {
      plan = ObVecMergePlan();
    }
  }
  return ret;
}

void ObVectorIndexMergePolicy::determine_output_role_(
    const ObVecMergePolicyInput &input, ObVecMergePlan &plan) const
{
  bool has_incr_base = false;
  for (int64_t i = plan.begin_; !has_incr_base && i < plan.end_; ++i) {
    has_incr_base = input.segments_.at(i).seg_type_ == ObVectorIndexSegmentType::INCR_BASE;
  }
  if (input.has_async_index_ || has_incr_base) {
    int64_t vector_count = 0;
    for (int64_t i = plan.begin_; i < plan.end_; ++i) {
      const int64_t count = input.segments_.at(i).live_vector_count();
      vector_count = count > INT64_MAX - vector_count ? INT64_MAX : vector_count + count;
    }
    const bool can_reclaim = can_reclaim_insert_(input, plan.begin_, plan.end_);
    // BASE multi-output cannot retain an async insert whitelist. Rebuild unsafe
    // oversized windows as INCR_MERGE until publication covers their output SCN.
    if (plan.strategy_ == ObVecMergePlan::MANUAL_MERGE
        && vector_count > config_.max_vectors_ && can_reclaim) {
      plan.output_type_ = ObVectorIndexSegmentType::BASE;
    } else if (plan.begin_ < input.base_count_) {
      // Window selection already checked visibility; consuming a BASE preserves its role.
      plan.output_type_ = ObVectorIndexSegmentType::BASE;
    } else {
      plan.output_type_ = can_reclaim ? ObVectorIndexSegmentType::INCR_BASE : ObVectorIndexSegmentType::INCR_MERGE;
    }
    plan.output_base_ = plan.output_type_ == ObVectorIndexSegmentType::BASE;
  } else {
    if (plan.begin_ < input.base_count_) {
      // A window containing Base segments remains Base.
      plan.output_base_ = true;
    } else if (plan.strategy_ == ObVecMergePlan::DELETE_MERGE) {
      // Rebuilding a single Incr segment does not promote it to Base.
      plan.output_base_ = false;
    } else if (plan.begin_ != input.base_count_) {
      // Only a window starting at the original I0 can be promoted.
      plan.output_base_ = false;
    } else if (plan.strategy_ == ObVecMergePlan::MANUAL_MERGE || input.base_count_ == 0) {
      plan.output_base_ = true;
    } else {
      const ObVecSegmentMergeInfo &last_base = input.segments_.at(input.base_count_ - 1);
      plan.output_base_ = last_base.readable_ && last_base.live_vector_count() >= config_.max_vectors_;
    }
    plan.output_type_ = plan.output_base_ ? ObVectorIndexSegmentType::BASE : ObVectorIndexSegmentType::INCR_MERGE;
  }
}

int ObVectorIndexMergePolicy::check_need_merge_task(const ObVecMergePolicyInput &input,
    bool &need_task, ObVecMergePlan &plan) const
{
  int ret = find_merge_segments(input, OB_VEC_TRIGGER_AUTO, plan);
  need_task = false;
  uint64_t total = 0;
  // Keep the existing coarse total-count overflow check, even if a window was found.
  for (int64_t i = 0; (ret == OB_SUCCESS || ret == OB_NO_NEED_MERGE || ret == OB_EAGAIN)
      && i < input.segments_.count(); ++i) {
    const uint64_t count = input.segments_.at(i).vector_count_;
    if (count > UINT64_MAX - total) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      total += count;
    }
  }
  if (ret == OB_SUCCESS) {
    need_task = true;
  } else {
    plan = ObVecMergePlan();
    if (ret == OB_NO_NEED_MERGE) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObVectorIndexMergePolicy::find_merge_segments(const ObVecMergePolicyInput &input,
    const ObVecIndexAsyncTaskTriggerType trigger, ObVecMergePlan &plan) const
{
  int ret = validate_(input);
  plan = ObVecMergePlan();
  if (OB_SUCC(ret) && trigger == OB_VEC_TRIGGER_ALTER_INDEX_PARAM) {
    if (input.segments_.empty()) {
      ret = OB_NO_NEED_MERGE;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < input.segments_.count(); ++i) {
      if (!input.segments_.at(i).readable_) {
        ret = OB_EAGAIN;
        LOG_TRACE("full merge source is not ready", K(ret), K(i), K(input.segments_.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      plan.begin_ = 0;
      plan.end_ = input.segments_.count();
      plan.output_base_ = true;
      plan.strategy_ = ObVecMergePlan::FULL_MERGE;
      plan.output_type_ = ObVectorIndexSegmentType::BASE;
    }
  } else if (OB_SUCC(ret)) {
    // Share window selection; exact deletes precede residual/manual merging.
    ObVecMergePlan target_plan;
    ObVecMergePlan tiered_plan;
    const int target_ret = find_target_merge_(input, target_plan);
    const int tiered_ret = find_tiered_merge_(input, tiered_plan);
    bool pending = target_ret == OB_EAGAIN || tiered_ret == OB_EAGAIN;
    if (target_ret != OB_SUCCESS && target_ret != OB_NO_NEED_MERGE && target_ret != OB_EAGAIN) {
      ret = target_ret;
    } else if (tiered_ret != OB_SUCCESS && tiered_ret != OB_NO_NEED_MERGE && tiered_ret != OB_EAGAIN) {
      ret = tiered_ret;
    } else if (target_ret == OB_SUCCESS
        && (tiered_ret != OB_SUCCESS || target_plan.begin_ <= tiered_plan.begin_)) {
      // Both strategies protect earlier windows; TARGET wins only at the same start.
      plan = target_plan;
      ret = OB_SUCCESS;
    } else if (tiered_ret == OB_SUCCESS) {
      plan = tiered_plan;
      ret = OB_SUCCESS;
    } else {
      ret = pending ? OB_EAGAIN : OB_NO_NEED_MERGE;
    }
    if (ret == OB_NO_NEED_MERGE || ret == OB_EAGAIN) {
      ret = find_delete_merge_(input, plan);
    }
    if (ret == OB_NO_NEED_MERGE) {
      ret = find_residual_merge_(input, ObTimeUtility::current_time(), plan);
    }
    // An unavailable segment must not trigger manual merge.
    if (ret == OB_NO_NEED_MERGE && pending) {
      ret = OB_EAGAIN;
    }
    if (ret == OB_NO_NEED_MERGE) {
      ret = find_freeze_rebuild_(input, plan);
    }
    if (ret == OB_NO_NEED_MERGE && trigger == OB_VEC_TRIGGER_MANUAL) {
      ret = find_manual_merge_(input, plan);
    }
    if (OB_SUCC(ret)) {
      determine_output_role_(input, plan);
    }
  }
  return ret;
}
} // namespace share
} // namespace oceanbase
