/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_worker.h"
#include "lib/alloc/alloc_func.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/utility.h"
#include "share/cache/ob_kvcache_inst_map.h"
#include "share/config/ob_server_config.h"
#include "share/io/ob_io_struct.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_stat.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_global_macro_block_kvcache.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"

// 将 L1 KVCACHE 中的宏块异步写入 L2 本地缓存：
// 1. Ring 只记录宏块 ID，处理任务时再从 L1 获取数据。
// 2. 每轮先回收已完成的写 IO，再根据队列和内存水位计算刷盘参数。
// 3. 按每批数量、IO 并发上限和批次间隔处理任务。
// 4. 数据已淘汰、无需写盘或处理失败时，放弃当前缓存任务，不重新入队。

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

ObSSMacroPersistWorker::ObSSMacroPersistWorker()
  : is_inited_(false),
    tg_started_(false),
    cache_enabled_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    tg_id_(-1),
    ring_buffer_capacity_(0),
    peek_miss_cnt_(0),
    crc_fail_cnt_(0),
    ring_buffer_(),
    write_task_()
{
}

ObSSMacroPersistWorker::~ObSSMacroPersistWorker()
{
  destroy();
}

int ObSSMacroPersistWorker::init(
    const uint64_t tenant_id,
    const int64_t ring_buffer_capacity,
    const int64_t io_depth)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ATOMIC_LOAD(&is_inited_))) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro persist worker is already inited",
        KR(ret), K_(tenant_id), K_(tg_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id)
      || tenant_id != MTL_ID()
      || ring_buffer_capacity <= 0
      || io_depth <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persist worker argument", KR(ret), K(tenant_id),
        "mtl_id", MTL_ID(), K(ring_buffer_capacity), K(io_depth));
  } else if (OB_UNLIKELY(!OB_GLOBAL_MACRO_BLOCK_KVCACHE.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("global macro block KVCACHE is not inited",
        KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(ring_buffer_.init(tenant_id, ring_buffer_capacity))) {
      LOG_WARN("fail to init macro ring buffer",
          KR(ret), K(tenant_id), K(ring_buffer_capacity));
    } else if (OB_FAIL(write_task_.init(tenant_id, io_depth))) {
      LOG_WARN("fail to init write KVCACHE to LCache task",
          KR(ret), K(tenant_id), K(io_depth));
    } else if (OB_FAIL(TG_CREATE_TENANT(
        lib::TGDefIDs::SSMacroPersist, tg_id_))) {
      LOG_WARN("fail to create tenant persist tg", KR(ret), K(tenant_id));
    } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
      LOG_WARN("fail to set tenant persist runnable",
          KR(ret), K(tenant_id), K_(tg_id));
    } else {
      tenant_id_ = tenant_id;
      ring_buffer_capacity_ = ring_buffer_capacity;
      ATOMIC_STORE(&cache_enabled_, true);
      ATOMIC_STORE(&tg_started_, false);
      ATOMIC_STORE(&is_inited_, true);
    }
  }
  if (OB_FAIL(ret) && !ATOMIC_LOAD(&is_inited_)) {
    if (-1 != tg_id_) {
      TG_DESTROY(tg_id_);
      tg_id_ = -1;
    }
    write_task_.destroy();
    ring_buffer_.destroy();
  }
  return ret;
}

int ObSSMacroPersistWorker::enqueue(
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ATOMIC_LOAD(&is_inited_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro persist worker is not inited", KR(ret));
  } else if (OB_UNLIKELY(
      !ATOMIC_LOAD(&tg_started_) || !ring_buffer_.is_accepting())) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro persist worker is stopped",
        KR(ret), K_(tenant_id), K(macro_id));
  } else if (OB_FAIL(ring_buffer_.push_overwrite(macro_id))) {
    if (OB_EAGAIN == ret || OB_SERVICE_STOPPED == ret) {
      LOG_WARN("macro persist entry is not enqueued",
          KR(ret), K_(tenant_id), K(macro_id));
    } else {
      LOG_WARN("fail to enqueue macro persist entry",
          KR(ret), K_(tenant_id), K(macro_id));
    }
  }
  return ret;
}

int ObSSMacroPersistWorker::discard_pending_entries()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ring_buffer_.discard_all())) {
    LOG_WARN("fail to discard pending macro persist entries",
        KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSMacroPersistWorker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ATOMIC_LOAD(&is_inited_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro persist worker is not inited", KR(ret));
  } else if (OB_UNLIKELY(!ring_buffer_.is_accepting())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("macro persist worker cannot be restarted",
        KR(ret), K_(tenant_id), K_(tg_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&tg_started_))) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro persist worker is already started",
        KR(ret), K_(tenant_id), K_(tg_id));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start tenant persist tg",
        KR(ret), K_(tenant_id), K_(tg_id));
  } else {
    ATOMIC_STORE(&tg_started_, true);
  }
  return ret;
}

void ObSSMacroPersistWorker::stop()
{
  if (ATOMIC_LOAD(&is_inited_)) {
    ring_buffer_.stop_accepting();
    if (ATOMIC_LOAD(&tg_started_)) {
      TG_STOP(tg_id_);
    }
  }
}

int ObSSMacroPersistWorker::wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ATOMIC_LOAD(&is_inited_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro persist worker is not inited", KR(ret));
  } else if (OB_UNLIKELY(ring_buffer_.is_accepting())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("stop macro persist worker before wait",
        KR(ret), K_(tenant_id), K_(tg_id));
  } else {
    if (ATOMIC_LOAD(&tg_started_)) {
      TG_WAIT(tg_id_);
      ATOMIC_STORE(&tg_started_, false);
    }
    // Worker 已退出，丢弃尚未处理的 entry。L2 持久化是 best-effort，
    // 允许这类 entry 在 destroy() 时丢弃。资源释放前由 MTL 保证
    // 所有并发 put() 已结束。
    if (OB_FAIL(ring_buffer_.discard_all())) {
      LOG_WARN("fail to discard pending macro persist entries",
          KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

void ObSSMacroPersistWorker::destroy()
{
  // Owner 已依次完成 stop() 和 wait()，这里只释放资源。
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  write_task_.destroy();
  ring_buffer_.destroy();
  peek_miss_cnt_ = 0;
  crc_fail_cnt_ = 0;
  ATOMIC_STORE(&is_inited_, false);
  ATOMIC_STORE(&tg_started_, false);
  ATOMIC_STORE(&cache_enabled_, false);
  tenant_id_ = OB_INVALID_TENANT_ID;
  tg_id_ = -1;
  ring_buffer_capacity_ = 0;
}

void ObSSMacroPersistWorker::collect_pressure_snapshot_(
    ObSSMacroPersistPressureSnapshot &snapshot) const
{
  snapshot.reset();
  snapshot.ring_size_ = ring_buffer_.size();
  snapshot.ring_capacity_ = ring_buffer_capacity_;
  // Meta tenant cache entries are washed using the user tenant's memory pressure.
  uint64_t memory_tenant_id = to_mb_list_tenant_id(tenant_id_);
  int64_t memory_limit = lib::get_tenant_memory_limit(memory_tenant_id);
  // Fall back to this tenant if the paired user tenant has no valid limit yet.
  if (memory_tenant_id != tenant_id_ && (memory_limit <= 0 || INT64_MAX == memory_limit)) {
    memory_tenant_id = tenant_id_;
    memory_limit = lib::get_tenant_memory_limit(memory_tenant_id);
  }
  // INT64_MAX means the limit is unset. Keep only Ring pressure if no limit is usable.
  if (memory_limit > 0 && INT64_MAX != memory_limit) {
    snapshot.tenant_memory_hold_ = lib::get_tenant_memory_hold(memory_tenant_id);
    snapshot.tenant_memory_limit_ = memory_limit;
  }
}

int ObSSMacroPersistWorker::drain_one_batch_(
    const ObSSMacroPersistPressureSnapshot &snapshot,
    const ObSSMacroPersistDrainPolicy &policy,
    const int64_t current_ts_us,
    int64_t &last_drain_ts_us)
{
  int ret = OB_SUCCESS;
  int64_t popped_entry_count = 0;
  if (OB_UNLIKELY(!policy.is_valid(write_task_.capacity()) || current_ts_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro persist batch argument", KR(ret), K(policy), K(current_ts_us));
  } else if (last_drain_ts_us > 0
      && (current_ts_us - last_drain_ts_us < policy.min_batch_interval_us_)) {
    // 距上批开始的时间还不够，本轮暂不取任务；
  } else {
    if (OB_FAIL(execute_drain_policy_(policy, popped_entry_count))) {
      LOG_WARN("fail to drain macro persist entries", KR(ret), K_(tenant_id),
          K(snapshot), K(policy), K(popped_entry_count));
    }
    // 本轮取过 entry 才更新批次时间戳，即使后续无需写盘或处理失败，也要遵守批次间隔。
    // 未取出 entry 时保留原时间戳，避免下轮额外等待一个完整间隔。
    if (popped_entry_count > 0) {
      last_drain_ts_us = current_ts_us;
      if (TC_REACH_TIME_INTERVAL(10 * common::S_US)) {
        LOG_INFO("drain macro block KVCACHE entries", KR(ret), K_(tenant_id),
            K(snapshot), K(policy), K(popped_entry_count),
            "ring_size", ring_buffer_.size(), "inflight_count", write_task_.inflight_count());
      }
    }
  }
  return ret;
}

int ObSSMacroPersistWorker::execute_drain_policy_(
    const ObSSMacroPersistDrainPolicy &policy,
    int64_t &popped_entry_count)
{
  int ret = OB_SUCCESS;
  ObSSMacroPersistEntry entry;
  bool is_ring_empty = false;
  popped_entry_count = 0;
  if (OB_UNLIKELY(!policy.is_valid(write_task_.capacity()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block KVCACHE drain policy",
        KR(ret), K_(tenant_id), K(policy),
        "write_task_capacity", write_task_.capacity());
  } else {
    while (OB_SUCC(ret) && !has_set_stop()
        && !is_ring_empty
        && popped_entry_count < policy.scan_batch_size_
        && write_task_.inflight_count() < policy.target_inflight_count_) {
      if (OB_FAIL(ring_buffer_.try_pop(entry))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // Ring 已取空，标记本批结束；空队列不作为错误返回。
          ret = OB_SUCCESS;
          is_ring_empty = true;
        } else {
          LOG_WARN("fail to pop macro block KVCACHE persist entry",
              KR(ret), K_(tenant_id), K(policy), K(popped_entry_count));
        }
      } else {
        ++popped_entry_count;
        if (OB_FAIL(process_entry_(entry))) {
          // 当前 entry 已经出队，处理失败后不会重新入队。
          LOG_WARN("fail to process macro block KVCACHE persist entry",
              KR(ret), K_(tenant_id), K(policy), K(popped_entry_count), K(entry));
        }
      }
    }
  }
  return ret;
}

int64_t ObSSMacroPersistWorker::calc_wait_us_(
    const ObSSMacroPersistDrainPolicy &policy,
    const int64_t current_ts_us,
    const int64_t last_drain_ts_us) const
{
  // worker 醒来后要回收已完成的 IO，并检查是否可以处理下一批，按较早需要检查的时间等待。
  const int64_t ring_size = ring_buffer_.size();
  const int64_t inflight_count = write_task_.inflight_count();
  int64_t worker_wait_us = IDLE_WAIT_INTERVAL_US;

  // 没有 IO 待回收时默认等 200ms；有 IO 时每 10ms 检查，积压且槽位已满时缩短到 1ms。
  if (inflight_count > 0) {
    if (ring_size > 0 && inflight_count >= write_task_.capacity()) {
      worker_wait_us = INFLIGHT_POLL_INTERVAL_US;
    } else {
      worker_wait_us = NORMAL_WAIT_INTERVAL_US;
    }
  }

  if (ring_size > 0 && policy.is_enabled()) {
    // 首批无需等待批次间隔；后续批次按上次取批时间计算剩余间隔。
    const int64_t remaining_batch_interval_us = (last_drain_ts_us > 0)
        ? policy.min_batch_interval_us_ - (current_ts_us - last_drain_ts_us)
        : 0;
    // 批次间隔未到时等待剩余时间；首批或间隔已到时，默认 10ms 后再次检查取批条件。
    const int64_t next_batch_check_wait_us = (remaining_batch_interval_us > 0)
        ? remaining_batch_interval_us
        : NORMAL_WAIT_INTERVAL_US;
    worker_wait_us = MIN(worker_wait_us, next_batch_check_wait_us);
  }

  // 等待不低于 1ms，避免过于频繁地检查。
  return MAX(worker_wait_us, INFLIGHT_POLL_INTERVAL_US);
}

int ObSSMacroPersistWorker::tenant_ctx_check_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MTL_ID() != tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("persist worker runs in wrong tenant context",
        KR(ret), K_(tenant_id), "mtl_id", MTL_ID());
  }
  return ret;
}

// Skips L2 persistence for TMP_FILE entries queued longer than two minutes, without erasing L1.
int ObSSMacroPersistWorker::check_can_persist_(
    const ObSSMacroPersistEntry &entry,
    const ObSSMacroBlockCacheValue &value,
    bool &can_persist) const
{
  int ret = OB_SUCCESS;
  bool is_cold_tablet = false;
  can_persist = false;
  if (OB_UNLIKELY(has_set_stop()
      || !is_cache_enabled()
      || is_macro_read_cache_disabled())) {
    // 缓存关闭时保留 L1 Value，只跳过本次 L2 下沉。
  } else if (OB_UNLIKELY(
      !is_io_aligned(value.data_size_, DIO_ALIGN_SIZE))) {
    // L2 local cache uses direct IO and cannot persist unaligned data.
  } else if (SSObjUtil::is_tmp_file(entry.macro_id_)
      && entry.enqueue_ts_ > 0
      && ObTimeUtility::current_time_us() - entry.enqueue_ts_ > TMP_FILE_PERSIST_MAX_AGE_US) {
    // Temporary-file data in the macro block KVCache comes from prefetching.
    // Temporary files are usually read sequentially. If persistence is delayed beyond
    // TMP_FILE_PERSIST_MAX_AGE_US, the data has likely already been consumed,
    // so skip writing it to L2.
    // Leave L1 eviction to KVCACHE.
  } else if (OB_FAIL(ObSSObjectAccessUtil::check_if_cold_tablet(
      value.effective_tablet_id_, is_cold_tablet))) {
    LOG_WARN("fail to check persist entry tablet cache policy",
        KR(ret), K(entry), K(value.effective_tablet_id_));
  } else if (is_cold_tablet) {
    // COLD tablet 的读缓存不写入 LCache。
  } else {
    can_persist = true;
  }
  return ret;
}

int ObSSMacroPersistWorker::prepare_persist_entry_(
    const ObSSMacroPersistEntry &entry,
    const ObSSMacroBlockCacheValue *&value,
    ObKVCacheHandle &kv_handle,
    bool &need_submit)
{
  int ret = OB_SUCCESS;
  bool l2_exist = false;
  bool can_persist = false;
  ObSSMacroCacheMgr *l2_mgr = MTL(ObSSMacroCacheMgr *);
  const ObSSMacroBlockCacheKey key(tenant_id_, entry.macro_id_);
  value = nullptr;
  kv_handle.reset();
  need_submit = false;

  if (OB_FAIL(tenant_ctx_check_())) {
    LOG_WARN("fail to check macro persist worker tenant context",
        KR(ret), K_(tenant_id), K(entry));
  } else if (OB_ISNULL(l2_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("l2 macro cache mgr is null",
        KR(ret), K_(tenant_id), K(entry.macro_id_));
  } else if (OB_FAIL(l2_mgr->exist(entry.macro_id_, l2_exist))) {
    LOG_WARN("fail to check l2 macro existence",
        KR(ret), K_(tenant_id), K(entry.macro_id_));
  } else if (l2_exist) {
    // 已经下沉到 L2，无需重复提交。
  } else if (OB_FAIL(
      OB_GLOBAL_MACRO_BLOCK_KVCACHE.peek(key, value, kv_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ATOMIC_INC(&peek_miss_cnt_);
      LOG_TRACE("peek macro block from KVCACHE miss",
          K_(tenant_id), K(entry.macro_id_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to peek macro block from KVCACHE",
          KR(ret), K_(tenant_id), K(entry.macro_id_));
    }
  } else if (OB_ISNULL(value)
      || OB_UNLIKELY(!value->is_valid() || !kv_handle.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid macro block KVCACHE value or handle",
        KR(ret), K_(tenant_id), K(entry.macro_id_),
        KP(value), K(kv_handle));
  } else if (OB_FAIL(check_can_persist_(entry, *value, can_persist))) {
    LOG_WARN("fail to check macro persist policy",
        KR(ret), K_(tenant_id), K(entry.macro_id_));
  } else if (!can_persist) {
    // Admission skips, including stale TMP_FILE requests, do not retry or erase L1.
  } else if (OB_FAIL(value->check_crc())) {
    ATOMIC_INC(&crc_fail_cnt_);
    LOG_WARN("fail to check macro block KVCACHE value crc",
        KR(ret), K_(tenant_id), K(entry.macro_id_));
  } else {
    need_submit = true;
  }
  return ret;
}

int ObSSMacroPersistWorker::process_entry_(
    const ObSSMacroPersistEntry &entry)
{
  int ret = OB_SUCCESS;
  bool need_submit = false;
  const ObSSMacroBlockCacheValue *value = nullptr;
  ObKVCacheHandle kv_handle;

  if (OB_FAIL(prepare_persist_entry_(
      entry, value, kv_handle, need_submit))) {
    LOG_WARN("fail to prepare macro persist entry",
        KR(ret), K_(tenant_id), K(entry));
  } else if (need_submit
      && OB_FAIL(write_task_.async_persist_macro_block(entry, *value))) {
    // entry 已出队，提交失败后不主动重试，等待后续业务自然回源。
    if (OB_EAGAIN == ret) {
      LOG_TRACE("macro persist IO reaches concurrency limit",
          KR(ret), K_(tenant_id), K(entry));
    } else {
      LOG_WARN("fail to submit macro persist io",
          KR(ret), K_(tenant_id), K(entry.macro_id_));
    }
  }

  // async_persist_macro_block 返回时 IOManager 已复制 buffer，可以立即释放 L1 pin。
  kv_handle.reset();
  // 以下错误只影响当前 entry；L2 持久化为 best-effort，
  // 丢弃当前 entry 后继续处理后续 entry。
  if (OB_EAGAIN == ret || OB_CHECKSUM_ERROR == ret
      || OB_INVALID_DATA == ret || OB_ALLOCATE_MEMORY_FAILED == ret
      || OB_SIZE_OVERFLOW == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

void ObSSMacroPersistWorker::run1()
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *l2_mgr = nullptr;
  ObSSMacroPersistPressureSnapshot snapshot;
  ObSSMacroPersistDrainPolicy policy;
  // 记录最近一次取出过任务的批次开始时间；0 表示第一批可以立即执行。
  // 批次间隔和剩余等待时间统一使用系统时间，单位为微秒。
  int64_t last_drain_ts_us = 0;
  lib::set_thread_name("SSMacroPersist");
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_FAIL(tenant_ctx_check_())) {
    LOG_WARN("fail to check macro persist worker tenant context",
        KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(l2_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("l2 macro cache mgr is null", KR(ret), K_(tenant_id));
  } else {
    while (!has_set_stop()) {
      int tmp_ret = OB_SUCCESS;
      ret = OB_SUCCESS;

      // 先回收已完成的写 IO，为后续批次释放并发额度。
      if (OB_TMP_FAIL(write_task_.handle_completed_entry())) {
        LOG_WARN_RET(tmp_ret, "fail to reclaim persist io",
            K_(tenant_id));
      }

      const int64_t current_ts_us = ObTimeUtility::current_time_us();
      collect_pressure_snapshot_(snapshot);
      if (OB_FAIL(ObSSMacroPersistPolicyController::calculate_drain_policy(
          snapshot, write_task_.capacity(), policy))) {
        LOG_WARN("fail to calculate macro persist policy", KR(ret), K_(tenant_id),
            K(snapshot), "write_task_capacity", write_task_.capacity());
      } else if (l2_mgr->is_ckpt_replayed()) {
        // 等 L2 checkpoint 回放完成后再处理 Ring，确保判断本地是否已有数据时状态完整。
        if (OB_FAIL(drain_one_batch_(snapshot, policy, current_ts_us, last_drain_ts_us))) {
          LOG_WARN("fail to execute macro persist batch", KR(ret), K_(tenant_id));
        }
      }

      // 每分钟打印完整统计及本轮刷盘策略，无覆盖时也能观察积压和刷盘进度。
      if (TC_REACH_TIME_INTERVAL(STAT_LOG_INTERVAL_US)) {
        ObSSMacroPersistStat persist_stat;
        get_persist_stat_snapshot(persist_stat);
        LOG_INFO("macro persist worker stat", K_(tenant_id), K(snapshot), K(policy), K(persist_stat));
      }

      if (!has_set_stop()) {
        const int64_t worker_wait_us = calc_wait_us_(
            policy, ObTimeUtility::current_time_us(), last_drain_ts_us);
        // 定时检查新任务和 IO 完成情况；停止时允许等本轮休眠结束后再退出。
        ob_usleep(worker_wait_us);
      }
    }
  }
}

void ObSSMacroPersistWorker::get_persist_stat_snapshot(
    ObSSMacroPersistStat &stat) const
{
  ObSSMacroRingBufferStat ring_buffer_stat;
  ObSSWriteKV2LCacheStat write_task_stat;
  ring_buffer_.get_stat_snapshot(ring_buffer_stat);
  write_task_.get_stat_snapshot(write_task_stat);
  stat.reset();
  stat.persist_enqueue_cnt_ = ring_buffer_stat.enqueue_cnt_;
  stat.persist_overwrite_oldest_cnt_ =
      ring_buffer_stat.overwrite_oldest_cnt_;
  stat.persist_queue_current_ = ring_buffer_stat.queue_current_;
  stat.persist_peek_miss_cnt_ = ATOMIC_LOAD(&peek_miss_cnt_);
  stat.persist_crc_fail_cnt_ = ATOMIC_LOAD(&crc_fail_cnt_);
  stat.persist_submit_fail_cnt_ = write_task_stat.submit_fail_cnt_;
  stat.persist_success_cnt_ = write_task_stat.success_cnt_;
  stat.persist_success_bytes_ = write_task_stat.success_bytes_;
  stat.persist_io_fail_cnt_ = write_task_stat.io_fail_cnt_;
  stat.persist_inflight_cnt_ = write_task_stat.inflight_cnt_;
}

} // namespace storage
} // namespace oceanbase
