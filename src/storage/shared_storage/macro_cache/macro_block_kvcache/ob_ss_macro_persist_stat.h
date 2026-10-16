/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_STAT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_STAT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

// Worker 汇总 Ring Buffer、WriteTask 和自身统计后生成的持久化统计快照。
struct ObSSMacroPersistStat
{
public:
  ObSSMacroPersistStat() { reset(); }
  void reset()
  {
    persist_enqueue_cnt_ = 0;
    persist_overwrite_oldest_cnt_ = 0;
    persist_queue_current_ = 0;
    persist_peek_miss_cnt_ = 0;
    persist_crc_fail_cnt_ = 0;
    persist_submit_fail_cnt_ = 0;
    persist_success_cnt_ = 0;
    persist_success_bytes_ = 0;
    persist_io_fail_cnt_ = 0;
    persist_inflight_cnt_ = 0;
  }

  TO_STRING_KV(K_(persist_enqueue_cnt), K_(persist_overwrite_oldest_cnt),
      K_(persist_queue_current), K_(persist_peek_miss_cnt), K_(persist_crc_fail_cnt),
      K_(persist_submit_fail_cnt), K_(persist_success_cnt),
      K_(persist_success_bytes), K_(persist_io_fail_cnt),
      K_(persist_inflight_cnt));

public:
  // 成功写入 Ring Buffer 的 entry 数，包括覆盖最旧 entry 后写入的新 entry。
  int64_t persist_enqueue_cnt_;
  // Ring Buffer 满时，覆盖最旧 entry 的次数。
  int64_t persist_overwrite_oldest_cnt_;
  // 当前 Ring Buffer 中的 entry 数。
  int64_t persist_queue_current_;
  // Worker 未在全局 KVCACHE 中找到 entry 对应 Value 的次数。
  int64_t persist_peek_miss_cnt_;
  // Worker 读取的 Value 未通过 CRC 校验的次数。
  int64_t persist_crc_fail_cnt_;
  // 因没有空闲 IO entry 或异步 L2 写提交失败而未能提交的次数。
  int64_t persist_submit_fail_cnt_;
  // 异步 L2 写成功的宏块数。
  int64_t persist_success_cnt_;
  // 异步 L2 写成功的宏块总字节数。
  int64_t persist_success_bytes_;
  // 异步 L2 写已提交但执行失败的宏块数。
  int64_t persist_io_fail_cnt_;
  // 当前正在执行的异步 L2 写数量。
  int64_t persist_inflight_cnt_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_MACRO_PERSIST_STAT_H_
