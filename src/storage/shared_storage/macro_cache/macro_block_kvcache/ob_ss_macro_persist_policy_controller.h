/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_POLICY_CONTROLLER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_POLICY_CONTROLLER_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

// 刷盘参数的计算输入，由 Worker 每轮采集一次。
// Ring 使用率 = 队列条数 / 队列容量；内存使用率 = 租户总内存用量 / 租户内存上限。
struct ObSSMacroPersistPressureSnapshot
{
  ObSSMacroPersistPressureSnapshot();
  void reset();
  TO_STRING_KV(K_(ring_size), K_(ring_capacity),
      K_(tenant_memory_hold), K_(tenant_memory_limit));

  int64_t ring_size_;            // Ring 中等待处理的 entry 数。
  int64_t ring_capacity_;        // Ring 最多容纳的 entry 数。
  int64_t tenant_memory_hold_;   // 租户总内存用量，单位字节，包含其他模块的内存。
  int64_t tenant_memory_limit_;  // 租户内存上限，单位字节。
};

// 刷盘参数的计算结果：一批处理多少条、最多并发多少个写 IO、两批之间等多久。
// Worker 按这三个限制执行，策略计算本身不发起 IO。
struct ObSSMacroPersistDrainPolicy
{
  // 刷盘策略的计算范围，IO 并发还受实际写任务容量限制。
  static constexpr int64_t MIN_SCAN_BATCH_SIZE = 2;
  static constexpr int64_t MAX_SCAN_BATCH_SIZE = 64;

  static constexpr int64_t MIN_TARGET_INFLIGHT_COUNT = 2;
  static constexpr int64_t MAX_TARGET_INFLIGHT_COUNT = 64;

  // 缓存下沉允许丢弃待刷盘记录，全速仍间隔 10ms，减少连续取批对前台 IO 的竞争。
  static constexpr int64_t MIN_BATCH_INTERVAL_US = 10 * 1000L;
  static constexpr int64_t MAX_BATCH_INTERVAL_US = 100 * 1000L;

  ObSSMacroPersistDrainPolicy();
  void reset();
  bool is_enabled() const;
  bool is_valid(const int64_t write_task_capacity) const;

  TO_STRING_KV(K_(scan_batch_size), K_(target_inflight_count), K_(min_batch_interval_us));

  // 每批最多从 Ring 取出的 entry 数；无需写盘的 entry 也计入这个数量。
  int64_t scan_batch_size_;
  // 已提交但尚未回收的写 IO 总数上限，包含之前批次的 IO；达到上限后暂停取 entry。
  int64_t target_inflight_count_;
  // 相邻两批开始时间的最小间隔，单位微秒；有效策略至少间隔 10ms。
  // 本轮没有取出 entry 时，不算新的一批，也不重新计时。
  int64_t min_batch_interval_us_;
};

// 按 Ring 和租户内存中较高的压力计算刷盘参数。
// 开始加速水位以下使用低速参数，两条水位之间按比例加速，达到全速水位后使用全速参数。
class ObSSMacroPersistPolicyController final
{
public:
  static int calculate_drain_policy(
      const ObSSMacroPersistPressureSnapshot &snapshot,
      const int64_t write_task_capacity,
      ObSSMacroPersistDrainPolicy &policy);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_POLICY_CONTROLLER_H_
