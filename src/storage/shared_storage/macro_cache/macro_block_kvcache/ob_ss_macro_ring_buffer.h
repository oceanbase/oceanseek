/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_RING_BUFFER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_RING_BUFFER_H_

#include "lib/allocator/page_arena.h"
#include "lib/queue/ob_fixed_queue.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{
struct ObSSMacroRingBufferStat
{
  ObSSMacroRingBufferStat() { reset(); }

  void reset()
  {
    capacity_ = 0;
    enqueue_cnt_ = 0;
    overwrite_oldest_cnt_ = 0;
    queue_current_ = 0;
  }

  // Ring Buffer 初始化时配置的 entry 总数。
  int64_t capacity_;
  // 成功写入 Ring Buffer 的累计次数，包括覆盖最旧 entry 后的写入。
  int64_t enqueue_cnt_;
  // Ring Buffer 满时，覆盖最旧 entry 的累计次数。
  int64_t overwrite_oldest_cnt_;
  // 当前在 Ring Buffer 中等待 Worker 处理的 entry 数。
  int64_t queue_current_;
};

// Ring Buffer 中只保存轻量 Key 和入队时间，不保存 Value 或 KV Handle。
struct ObSSMacroPersistEntry
{
  ObSSMacroPersistEntry() { reset(); }
  void reset();
  TO_STRING_KV(K_(macro_id), K_(enqueue_ts));

  blocksstable::MacroBlockId macro_id_;
  // entry 成功加入 Ring Buffer 的时间，单位为微秒。
  int64_t enqueue_ts_;
};

// 每租户固定容量 Ring Buffer。满时复用最旧 Entry，避免 put 路径阻塞或扩容。
class ObSSMacroRingBuffer
{
public:
  ObSSMacroRingBuffer();
  ~ObSSMacroRingBuffer();

  int init(const uint64_t tenant_id, const int64_t capacity);
  void stop_accepting();
  void destroy();

  int push_overwrite(const blocksstable::MacroBlockId &macro_id);
  int try_pop(ObSSMacroPersistEntry &entry);
  // 尽力丢弃当前仍在等待处理的 entry，可与 push/try_pop 并发调用。
  int discard_all();

  void get_stat_snapshot(ObSSMacroRingBufferStat &stat) const;
  int64_t size() const { return ready_queue_.get_curr_total(); }
  int64_t free_size() const { return free_queue_.get_curr_total(); }
  bool is_accepting() const { return ATOMIC_LOAD(&accepting_); }

private:
  int recycle_entry_(ObSSMacroPersistEntry *entry);

private:
  bool is_inited_;
  bool accepting_;
  uint64_t tenant_id_;
  ObSSMacroPersistEntry *entries_;
  common::ObFixedQueue<ObSSMacroPersistEntry> free_queue_;
  common::ObFixedQueue<ObSSMacroPersistEntry> ready_queue_;
  ObSSMacroRingBufferStat stat_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroRingBuffer);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_RING_BUFFER_H_
