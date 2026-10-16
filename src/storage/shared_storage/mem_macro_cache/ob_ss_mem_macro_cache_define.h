/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_DEFINE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_DEFINE_H_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase 
{
namespace storage 
{
static constexpr int64_t SS_DEF_MEM_MACRO_CACHE_LIMIT_PCT = 5;
static constexpr int32_t SS_DEF_MEM_MACRO_CACHE_BLK_SIZE = 2 * 1024 * 1024;
static constexpr int64_t SS_DEF_MEM_MACRO_CACHE_BUCKET_CNT = 64;
static constexpr int32_t SS_SMALL_MACRO_BLOCK_SIZE = 128 * 1024;
static constexpr int32_t SS_MACRO_META_ALLOCATOR_BLK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;
static constexpr int64_t SS_DEF_MACRO_META_MAP_BUCKET_CNT = 10000;
static constexpr int64_t SS_MEM_MACRO_CACHE_EVICT_TASK_INTERVAL_US = 5 * 1000;
static constexpr int64_t SS_DEF_TRIGGER_EVICT_USAGE_PCT = 80;
static constexpr int64_t SS_DEF_EVICT_MEM_BLOCK_CNT = 50;
static constexpr int64_t SS_PRINT_MEM_MACRO_CACHE_STAT_INTERVAL_US = 60 * 1000 * 1000L;
static constexpr int64_t SS_MEM_LOOP_SLEEP_INTERVAL_US = 1000;
static constexpr int64_t SS_MEM_LOOP_PRINT_LOG_INTERVAL_US = 5 * 1000 * 1000L;
static constexpr int64_t SS_DEF_MACRO_BLOCK_WRITE_BACK_SIZE_MAX_PCT = 90;
static constexpr int64_t SS_DEF_WRITE_BACK_DISK_UTIL_MAX_PCT = 80;
static constexpr int64_t SS_DEF_MACRO_BLOCK_TTL_TIME_US = 5 * 60 * 1000 * 1000L; // 5min
static constexpr int64_t SS_DEF_FAST_PERSIST_TRIGGER_TIME_MAX_US = 1 * 60 * 1000 * 1000LL; // trigger persist macro block within 1 minute
static constexpr int64_t SS_DEF_ASYNC_EVICT_TRIGGER_TIME_MAX_US = 3 * 1000 * 1000LL; // trigger async evict macro block within 3 seconds
static constexpr int64_t SS_DEF_CHECK_MACRO_TTL_INTERVAL_US = 10 * 1000 * 1000L; // 10s
static constexpr int64_t SS_CLEAR_MEM_MACRO_CACHE_TIMEOUT_US = 60 * 1000 * 1000L; // 1min
static constexpr int64_t SS_MEM_MACRO_PRINT_WARN_INTERVAL_US = 500 * 1000;

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MEM_MACRO_CACHE_DEFINE_H_ */
