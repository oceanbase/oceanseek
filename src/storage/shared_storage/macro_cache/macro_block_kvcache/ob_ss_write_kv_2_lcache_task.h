/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_WRITE_KV_2_LCACHE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_WRITE_KV_2_LCACHE_TASK_H_

#include "lib/allocator/page_arena.h"
#include "lib/queue/ob_fixed_queue.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_ring_buffer.h"

namespace oceanbase
{
namespace storage
{
class ObSSMacroBlockCacheValue;

struct ObSSWriteKV2LCacheStat
{
  ObSSWriteKV2LCacheStat()
    : submit_fail_cnt_(0),
      success_cnt_(0),
      success_bytes_(0),
      io_fail_cnt_(0),
      inflight_cnt_(0)
  {
  }

  int64_t submit_fail_cnt_;
  int64_t success_cnt_;
  int64_t success_bytes_;
  int64_t io_fail_cnt_;
  int64_t inflight_cnt_;
};

// 只跟踪已经提交的 IO。IOManager 在异步提交成功时已复制 buffer，
// 因此 Entry 无需继续持有 KV Handle。
struct ObSSWriteKV2LCacheEntry
{
  ObSSWriteKV2LCacheEntry();
  void reset();
  TO_STRING_KV(K_(macro_id), K_(effective_tablet_id), K_(data_size),
      K_(enqueue_ts), K_(submit_ts), K_(write_handle));

  blocksstable::MacroBlockId macro_id_;
  uint64_t effective_tablet_id_;
  int64_t data_size_;
  blocksstable::ObStorageObjectHandle write_handle_;
  // 对应 entry 成功加入 Ring Buffer 的时间，单位为微秒。
  int64_t enqueue_ts_;
  // 异步 L2 写成功提交给 IOManager 的时间，单位为微秒。
  int64_t submit_ts_;
};

// 负责提交 KVCACHE Value 到 L2 Cache，并跟踪、回收异步 IO。
// 本类没有独立线程或 Timer，由 ObSSMacroPersistWorker 驱动。
class ObSSWriteKV2LCacheTask
{
public:
  ObSSWriteKV2LCacheTask();
  ~ObSSWriteKV2LCacheTask();

  int init(const uint64_t tenant_id, const int64_t capacity);
  void destroy();

  int async_persist_macro_block(const ObSSMacroPersistEntry &persist_entry,
                                const ObSSMacroBlockCacheValue &value);
  int handle_completed_entry();

  void get_stat_snapshot(ObSSWriteKV2LCacheStat &stat) const;
  int64_t inflight_count() const { return write_entry_list_.get_curr_total(); }
  int64_t capacity() const { return capacity_; }

private:
  int acquire_entry_(ObSSWriteKV2LCacheEntry *&entry);
  int do_write_into_macro_cache_(const ObSSMacroPersistEntry &persist_entry,
                                 const ObSSMacroBlockCacheValue &value,
                                 ObSSWriteKV2LCacheEntry &entry);
  int recycle_entry_(ObSSWriteKV2LCacheEntry &entry);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t capacity_;
  // Entry 在 free/write 两个固定队列间流转。
  ObSSWriteKV2LCacheEntry *total_entry_arr_;
  common::ObFixedQueue<ObSSWriteKV2LCacheEntry> free_entry_list_;
  common::ObFixedQueue<ObSSWriteKV2LCacheEntry> write_entry_list_;
  int64_t submit_fail_cnt_;
  int64_t success_cnt_;
  int64_t success_bytes_;
  int64_t io_fail_cnt_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSWriteKV2LCacheTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_WRITE_KV_2_LCACHE_TASK_H_
