/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_WORKER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_WORKER_H_

#include "lib/literals/ob_literals.h"
#include "lib/restore/ob_object_storage_base.h"
#include "lib/thread/thread_mgr_interface.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_policy_controller.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_stat.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_ring_buffer.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_write_kv_2_lcache_task.h"

namespace oceanbase
{
namespace common
{
class ObKVCacheHandle;
}
namespace storage
{
class ObSSMacroBlockCacheValue;

// 每租户单线程 Worker：持有 Ring Buffer，筛选 entry 后交给 WriteTask 提交 L2 IO。
// 低水位下持续低速刷盘；水位升高后，每批处理更多 entry、允许更多写 IO 并发，并缩短批次间隔。
class ObSSMacroPersistWorker final : public lib::TGRunnable
{
public:
  ObSSMacroPersistWorker();
  virtual ~ObSSMacroPersistWorker();
  int init(const uint64_t tenant_id,
           const int64_t ring_buffer_capacity,
           const int64_t io_depth);
  int enqueue(const blocksstable::MacroBlockId &macro_id);
  int discard_pending_entries();
  int start();
  void stop();
  int wait();
  void destroy();
  virtual void run1() override;

  bool is_cache_enabled() const
  {
    return ATOMIC_LOAD(&cache_enabled_);
  }
  void set_cache_enabled(const bool enabled)
  {
    ATOMIC_STORE(&cache_enabled_, enabled);
  }
  void get_persist_stat_snapshot(ObSSMacroPersistStat &stat) const;

private:
  // 读取本轮 Ring 条数与容量、租户内存使用量与上限，供策略计算使用。
  void collect_pressure_snapshot_(ObSSMacroPersistPressureSnapshot &snapshot) const;
  // 检查距离上次批次的时间，满足当前策略间隔后才处理下一批。
  // 两个时间参数均为系统时间的微秒值；本批取出过 entry 才更新 last_drain_ts_us。
  int drain_one_batch_(
      const ObSSMacroPersistPressureSnapshot &snapshot,
      const ObSSMacroPersistDrainPolicy &policy,
      const int64_t current_ts_us,
      int64_t &last_drain_ts_us);
  // 按批次数量和 IO 并发上限取出 entry；降速时暂停补充 IO，已提交的 IO 继续完成。
  // popped_entry_count 为本批从 Ring 取出的 entry 数，包含无需写盘和处理失败的 entry。
  int execute_drain_policy_(const ObSSMacroPersistDrainPolicy &policy, int64_t &popped_entry_count);
  // 根据 IO 回收和下一批处理的检查时机，计算 worker 本轮等待时间，单位为微秒。
  int64_t calc_wait_us_(
      const ObSSMacroPersistDrainPolicy &policy,
      const int64_t current_ts_us,
      const int64_t last_drain_ts_us) const;

  int tenant_ctx_check_();
  // Checks L2 admission, including the queue age of temporary-file entries.
  int check_can_persist_(
      const ObSSMacroPersistEntry &entry,
      const ObSSMacroBlockCacheValue &value,
      bool &can_persist) const;
  int prepare_persist_entry_(
      const ObSSMacroPersistEntry &entry,
      const ObSSMacroBlockCacheValue *&value,
      common::ObKVCacheHandle &kv_handle,
      bool &need_submit);
  int process_entry_(const ObSSMacroPersistEntry &entry);

private:
  // Skip L2 persistence for temporary-file entries queued longer than two minutes.
  static constexpr int64_t TMP_FILE_PERSIST_MAX_AGE_US = 2_min;
  // 在途异步写达到并发上限时的完成轮询间隔。
  static constexpr int64_t INFLIGHT_POLL_INTERVAL_US = common::MS_US;
  // 有在途 IO 或压力批次尚未完成时的常规等待间隔。
  static constexpr int64_t NORMAL_WAIT_INTERVAL_US = 10 * common::MS_US;
  // 空闲时检查新任务和水位的最长等待间隔。
  static constexpr int64_t IDLE_WAIT_INTERVAL_US = 200 * common::MS_US;
  // 持久化统计打印间隔，沿用原 mem macro cache 的 60 秒周期。
  static constexpr int64_t STAT_LOG_INTERVAL_US = 60 * common::S_US;

  // Worker 只记录内部资源事实；对外服务状态由 facade 持有。
  bool is_inited_;
  // TG_START 成功后置 true，完成 TG_WAIT 后清零。
  bool tg_started_;
  bool cache_enabled_;
  uint64_t tenant_id_;
  int tg_id_;
  // Ring Buffer 初始化容量，用于计算开始加速和全速水位。
  int64_t ring_buffer_capacity_;
  int64_t peek_miss_cnt_;
  int64_t crc_fail_cnt_;
  ObSSMacroRingBuffer ring_buffer_;
  ObSSWriteKV2LCacheTask write_task_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroPersistWorker);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_WORKER_H_
