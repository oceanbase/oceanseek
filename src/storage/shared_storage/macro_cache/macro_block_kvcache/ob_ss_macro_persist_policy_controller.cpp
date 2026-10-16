/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_policy_controller.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

// 刷盘策略：持续低速刷盘，压力增大时逐步加速。
//
// 压力怎么算：
//   Ring 使用率从 60% 开始加速，达到 90% 全速；租户内存对应的水位为 75% 和 90%。
//   将这两条水位乘以各自容量，得到起始用量和全速用量。
//   起始用量以下记 0 分，达到全速用量记 100 分，中间按下面的公式计算：
//     压力分数 = (当前用量 - 起始用量) × 100 / (全速用量 - 起始用量)
//   Ring 和内存分别计算，取较高的分数作为刷盘压力。
//
// 刷盘参数怎么算（压力分数为 0～100）：
//   每批最多取出的 entry 数 = 2 + 压力分数 × (64 - 2) / 100
//     最低 2 条，最高 64 条。
//   写 IO 并发上限 = 低速并发数 + 压力分数 × (全速并发数 - 低速并发数) / 100
//     最低 min(2, 写任务容量)，最高 min(64, 写任务容量)。
//   批次最小间隔 = 100ms - 压力分数 × (100ms - 10ms) / 100
//     最低 10ms（全速），最高 100ms（低速）。
//   其中，低速并发数取 min(2, 写任务容量)，全速并发数取 min(64, 写任务容量)。
//
// 水位和 entry 数的计算结果向下取整，批次间隔在代码中以微秒计算。
// 满分时仍保留 10ms 批次间隔，避免频繁入队唤醒造成连续取批；每批数量和 IO 并发也有上限。

namespace oceanbase
{
namespace storage
{

ObSSMacroPersistPressureSnapshot::ObSSMacroPersistPressureSnapshot()
{
  reset();
}

void ObSSMacroPersistPressureSnapshot::reset()
{
  ring_size_ = 0;
  ring_capacity_ = 0;
  tenant_memory_hold_ = 0;
  tenant_memory_limit_ = 0;
}

ObSSMacroPersistDrainPolicy::ObSSMacroPersistDrainPolicy()
{
  reset();
}

void ObSSMacroPersistDrainPolicy::reset()
{
  scan_batch_size_ = 0;
  target_inflight_count_ = 0;
  min_batch_interval_us_ = 0;
}

bool ObSSMacroPersistDrainPolicy::is_enabled() const
{
  return scan_batch_size_ > 0 && target_inflight_count_ > 0;
}

bool ObSSMacroPersistDrainPolicy::is_valid(
    const int64_t write_task_capacity) const
{
  return is_enabled()
      && scan_batch_size_ <= MAX_SCAN_BATCH_SIZE
      && target_inflight_count_ <= write_task_capacity
      && min_batch_interval_us_ >= MIN_BATCH_INTERVAL_US;
}

// 以下函数只用于本文件：先计算两种资源的压力分数，再将较高分数换算成刷盘参数。
static bool is_sample_valid(const int64_t usage, const int64_t capacity)
{
  return usage >= 0 && capacity > 0;
}

static int64_t calculate_score(
    const int64_t resource_usage,
    const int64_t resource_capacity,
    const int64_t start_percentage,
    const int64_t full_speed_percentage)
{
  // 将百分比换算成队列条数或内存字节数，小数部分向下取整。
  // 调用方已保证使用量非负、容量大于 0。
  const int64_t start_watermark = resource_capacity * start_percentage / 100;
  const int64_t full_speed_watermark = resource_capacity * full_speed_percentage / 100;
  int64_t score = 0;
  if (resource_usage >= full_speed_watermark) {
    score = 100;
  } else if (resource_usage > start_watermark) {
    score = (resource_usage - start_watermark) * 100
        / (full_speed_watermark - start_watermark);
  }
  return score;
}

static int64_t calculate_ring_score(const ObSSMacroPersistPressureSnapshot &snapshot)
{
  const int64_t START_PERCENTAGE = 60;
  const int64_t FULL_SPEED_PERCENTAGE = 90;
  return calculate_score(snapshot.ring_size_, snapshot.ring_capacity_,
      START_PERCENTAGE, FULL_SPEED_PERCENTAGE);
}

static int64_t calculate_memory_score(const ObSSMacroPersistPressureSnapshot &snapshot)
{
  const int64_t START_PERCENTAGE = 75;
  const int64_t FULL_SPEED_PERCENTAGE = 90;
  return calculate_score(snapshot.tenant_memory_hold_, snapshot.tenant_memory_limit_,
      START_PERCENTAGE, FULL_SPEED_PERCENTAGE);
}

static void build_drain_policy(
    const int64_t pressure_score,
    const int64_t write_task_capacity,
    ObSSMacroPersistDrainPolicy &policy)
{
  using DrainPolicy = ObSSMacroPersistDrainPolicy;
  const int64_t min_inflight = MIN(DrainPolicy::MIN_TARGET_INFLIGHT_COUNT, write_task_capacity);
  const int64_t max_inflight = MIN(DrainPolicy::MAX_TARGET_INFLIGHT_COUNT, write_task_capacity);

  // 分数越高，每批允许处理的 entry 和同时进行的写 IO 越多，两批之间等待越短。
  // 这些参数按比例变化；实际写入带宽还取决于 IO 耗时和有多少 entry 需要写盘。
  policy.scan_batch_size_ = DrainPolicy::MIN_SCAN_BATCH_SIZE
      + pressure_score * (DrainPolicy::MAX_SCAN_BATCH_SIZE - DrainPolicy::MIN_SCAN_BATCH_SIZE) / 100;
  policy.target_inflight_count_ = min_inflight
      + pressure_score * (max_inflight - min_inflight) / 100;
  policy.min_batch_interval_us_ = DrainPolicy::MAX_BATCH_INTERVAL_US
      - pressure_score * (DrainPolicy::MAX_BATCH_INTERVAL_US - DrainPolicy::MIN_BATCH_INTERVAL_US) / 100;
}

int ObSSMacroPersistPolicyController::calculate_drain_policy(
    const ObSSMacroPersistPressureSnapshot &snapshot,
    const int64_t write_task_capacity,
    ObSSMacroPersistDrainPolicy &policy)
{
  int ret = OB_SUCCESS;
  const bool ring_sample_valid = is_sample_valid(
      snapshot.ring_size_, snapshot.ring_capacity_);
  const bool memory_sample_valid = is_sample_valid(
      snapshot.tenant_memory_hold_, snapshot.tenant_memory_limit_);
  policy.reset();
  if (OB_UNLIKELY(write_task_capacity <= 0 || (!ring_sample_valid && !memory_sample_valid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for macro persist policy", KR(ret), K(snapshot), K(write_task_capacity));
  } else {
    // 只有一种资源采样有效时，只使用该采样；没有压力时也生成有效的低速策略。
    const int64_t ring_score = ring_sample_valid
        ? calculate_ring_score(snapshot) : 0;
    const int64_t memory_score = memory_sample_valid
        ? calculate_memory_score(snapshot) : 0;
    build_drain_policy(
        MAX(ring_score, memory_score), write_task_capacity, policy);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
