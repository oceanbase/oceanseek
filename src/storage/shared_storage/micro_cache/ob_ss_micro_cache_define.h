/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_DEFINE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_DEFINE_H_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase 
{
namespace storage 
{
// NOTICE: if you update this enum, please check 'is_prewarm_access_type()' and 'ObSSMicroCachePrewarmStat'
enum class ObSSMicroCacheAccessType : uint8_t
{
  INVALID_TYPE = 0,
  COMMON_IO_TYPE = 1,
  MAJOR_COMPACTION_PREWARM_TYPE = 2,
  MIGRATION_PREWARM_TYPE = 3,
  REPLICA_PREWARM_TYPE = 4,
  DDL_PREWARM_TYPE = 5,
  STORAGE_CACHE_POLICY_PREWARM_TYPE = 6,
  MINOR_COMPACTION_PREWARM_TYPE = 7,
  PREREAD_TASK_PREWARM_TYPE = 8,
  SPLIT_PART_PREWARM_TYPE = 9,
  TRY_WRITE_LCACHE_PREWARM_TYPE = 10,
  MAX_TYPE
};

enum class ObSSMicroBlockCacheKeyMode : uint8
{
  PHYSICAL_KEY_MODE = 0,
  LOGICAL_KEY_MODE = 1,
  MAX_MODE
};

enum class ObSSMicroCacheGetType : uint8_t
{
  // when cache hit, get data from ss_micro_cache
  // when cache miss, do not get data.
  // for replica_prewarm
  GET_CACHE_HIT_DATA = 0,
  // when cache hit, do not get data.
  // when cache miss, get data from object storage and add it into ss_micro_cache.
  // for major_compaction_prewarm
  GET_CACHE_MISS_DATA = 1,
  // when cache hit, get data from micro_cache
  // when cache miss, get data from object storage and add it into ss_micro_cache.
  // for common io
  FORCE_GET_DATA = 2,
};

enum class ObSSMicroCacheHitType : uint8
{
  SS_CACHE_MISS = 0,
  SS_CACHE_HIT_MEM = 1,
  SS_CACHE_HIT_DISK = 2,
};

enum class ObSSMicroCacheCkptType : uint8
{
  SS_CACHE_MICRO_CKPT = 0,
  SS_CACHE_BLK_CKPT = 1,
};

// NOTICE: add new op type before SS_MAX_OP, and add corresponding string in SS_MICRO_CACHE_TASK_OP_TYPE_STR_ARRAY
enum class ObSSMicroCacheTaskOpType : uint8
{
  SS_INVALID_OP = 0,
  SS_PERSIST_MICRO_DATA_OP = 1,
  SS_PERSIST_MICRO_META_OP = 2,
  SS_DO_BLK_CKPT_OP = 3,
  SS_EVICT_MICRO_OP = 4,
  SS_REORGANIZE_MICRO_OP = 5,
  SS_MONITOR_OP = 6,
  SS_CALIBRATION_OP = 7,
  SS_MAX_OP
};
static constexpr int64_t SS_MICRO_CACHE_TASK_OP_TYPE_CNT = static_cast<int64_t>(ObSSMicroCacheTaskOpType::SS_MAX_OP);

// String names for task op types, index must match ObSSMicroCacheTaskOpType value
static constexpr const char *SS_MICRO_CACHE_TASK_OP_NAME_ARRAY[] = {
  "invalid_op",         // SS_INVALID_OP = 0
  "persist_micro_data", // SS_PERSIST_MICRO_DATA_OP = 1
  "persist_micro_meta", // SS_PERSIST_MICRO_META_OP = 2
  "do_blk_ckpt",        // SS_DO_BLK_CKPT_OP = 3
  "evict_micro",        // SS_EVICT_MICRO_OP = 4
  "reorganize_micro",   // SS_REORGANIZE_MICRO_OP = 5
  "monitor",            // SS_MONITOR_OP = 6
  "calibration",        // SS_CALIBRATION_OP = 7
};
static_assert(sizeof(SS_MICRO_CACHE_TASK_OP_NAME_ARRAY) / sizeof(SS_MICRO_CACHE_TASK_OP_NAME_ARRAY[0])
              == SS_MICRO_CACHE_TASK_OP_TYPE_CNT,
              "task op name array size must match task op type count");

// [Cache file size settings]
static constexpr int64_t SS_MICRO_CACHE_FIXED_FILE_SIZE = 10250 * 1024 * 1024L; // 10250MB

// [Phy block settings]
static constexpr int32_t SS_DEFAULT_PHY_BLK_SIZE = 2 * 1024 * 1024;
static constexpr int32_t SS_SUPER_BLK_COUNT = 2;
static constexpr double SS_PHY_BLK_REORGAN_USAGE_PCT = 0.72;
static constexpr int64_t SS_MIN_REORGAN_BLK_CNT = 7;
static constexpr int64_t SS_MIN_RESERVE_REORGAN_BLK_CNT = SS_MIN_REORGAN_BLK_CNT * 3;
static constexpr int64_t SS_MAX_RESERVE_REORGAN_BLK_CNT = SS_MIN_REORGAN_BLK_CNT * 6;
static constexpr int64_t SS_MIN_MICRO_DATA_BLK_CNT_PCT = 80;
static constexpr int64_t SS_MIN_MICRO_META_BLK_CNT_PCT = 4;
static constexpr int64_t SS_MAX_SCAN_SPARSE_BLK_CNT = 1000;
static constexpr int64_t SS_FREE_BLK_MIN_REUSE_TIME_S = 600;
static constexpr int64_t SS_INVALID_PHY_BLK_ID = -1;
static constexpr uint64_t SS_INVALID_BLK_REUSE_VERSION = 0;

// [Mem block settings]
static constexpr int64_t SS_MAX_BG_MEM_BLK_CNT = SS_MIN_REORGAN_BLK_CNT * 4;
static constexpr int64_t SS_MAX_FG_MEM_BLK_CNT = SS_MIN_REORGAN_BLK_CNT * 4;

// [Micro block meta settings]
static constexpr int32_t SS_MAX_DATA_LOC_BIT = 48;
static constexpr int32_t SS_AVG_MICRO_META_PERSIST_SIZE = 78;
static constexpr int32_t SS_META_ALLOCATOR_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;
static constexpr int32_t SS_SUB_RNG_ALLOCATOR_BLOCK_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;
static constexpr uint64_t SS_MICRO_KEY_MAX_HASH_VAL = 0xFFFFFFFF;
static constexpr uint64_t SS_MICRO_KEY_MIN_HASH_VAL = 0;
static constexpr uint32_t SS_SUB_RNG_PERSIST_META_MAX_CNT = 300;
static constexpr uint32_t SS_INIT_RNG_DEF_SUB_RNG_CNT = 6;
static constexpr uint32_t SS_INIT_RNG_DEF_META_CNT = SS_INIT_RNG_DEF_SUB_RNG_CNT * SS_SUB_RNG_PERSIST_META_MAX_CNT;
static constexpr uint32_t SS_INIT_RNG_MIN_CNT = 3000;
static constexpr uint32_t SS_INIT_RNG_MAX_CNT = 100000;
static constexpr int64_t SS_DEF_CACHE_EXPIRATION_TIME_S = 2 * 24 * 3600L; // 2day
static constexpr int64_t SS_MICRO_CACHE_MAX_BLOCK_SIZE = 128 * 1024; // 128KB

// [ARC cache algo settings] 
static constexpr int64_t SS_ARC_SEG_COUNT = 4;
static constexpr int64_t SS_ARC_T1 = 0;
static constexpr int64_t SS_ARC_B1 = 1;
static constexpr int64_t SS_ARC_T2 = 2;
static constexpr int64_t SS_ARC_B2 = 3;
static constexpr int64_t SS_ARC_HANDLE_OP_MAX_COUNT = 500;
static constexpr int64_t SS_ARC_FETCH_MULTIPLE = 8;
static constexpr int64_t SS_ARC_FETCH_COUNT = SS_ARC_HANDLE_OP_MAX_COUNT * SS_ARC_FETCH_MULTIPLE;
static constexpr int64_t SS_LIMIT_PREWARM_SHRINK_PCT = 90; // work_limit = limit * 90%, to reserve space for prewarm
static constexpr int64_t SS_MIN_ARC_LIMIT_PCT = 10;
static constexpr int64_t SS_ARC_LIMIT_PCT = 70;

// [Background task settings]
static constexpr int64_t SS_SLOW_TASK_COST_US = 5 * 1000 * 1000; // 5s
// task schedule interval
static constexpr int64_t SS_DO_BLK_CKPT_TASK_INTERVAL_US = 10 * 1000; // 10ms
static constexpr int64_t SS_PERSIST_DATA_TASK_INTERVAL_US = 2 * 1000; // 2ms
static constexpr int64_t SS_PERSIST_META_TASK_INTERVAL_US = 200 * 1000; // 200ms
static constexpr int64_t SS_PERSIST_DATA_TASK_FAST_INTERVAL_US = 10; // 10us
static constexpr int64_t SS_RELEASE_CACHE_TASK_INTERVAL_US = 10 * 1000; // 10ms
static constexpr int64_t SS_MONITOR_TASK_INTERVAL_US = 1 * 1000 * 1000; // 1s
static constexpr int64_t SS_CALIBRATION_TASK_INTERVAL_US = 1 * 1000 * 1000; // 1s
static constexpr int64_t SS_CALIBRATION_WAIT_TIMEOUT_US = 60 * 1000 * 1000; // 60s
// really execute checkpoint interval
static constexpr int64_t SS_DO_BLK_CKPT_INTERVAL_US = 5L * 60 * 1000 * 1000; // 5min
static constexpr int64_t SS_PERSIST_META_INTERVAL_US = 10L * 60 * 1000 * 1000; // 10min
static constexpr int64_t SS_PERSIST_PARTIAL_META_INTERVAL_US = 1000 * 1000; // 1s
static constexpr int64_t SS_SCAN_SPARSE_BLK_INTERVAL_US = 30 * 1000 * 1000; // 30s
static constexpr int64_t SS_SCAN_REUSABLE_BLK_INTERVAL_US = 5L * 60 * 1000 * 1000; // 5min
static constexpr int64_t SS_PRINT_CACHE_STAT_INTERVAL_US = 60 * 1000 * 1000; // 1min
static constexpr int64_t SS_UPDATE_CACHE_LOAD_INTERVAL_US = 5 * 1000 * 1000; // 5s
static constexpr int64_t SS_CHECK_CONFIG_INTERVAL_US = 2 * 1000 * 1000; // 2s
static constexpr int64_t SS_UPDATE_EXPIRATION_TIME_INTERVAL_US = 2 * 1000 * 1000; // 2s
// if avg_micro_size is less than 4KB, reorganize_task reserves at most SS_MIN_REORGAN_BLK_CNT blocks
static constexpr int32_t SS_REORGAN_MIN_MICRO_SIZE = (1L << 12);
// avg_micro_size increases 2KB, one more block is reserved for reorganize_task, upper limit is MAX_REORGAN_BLK_CNT
static constexpr int64_t SS_REORGAN_BLK_SCALING_FACTOR = (1L << 11);
static constexpr int64_t SS_CLOSE_EVICTION_DIFF_SIZE = 100L * 1024 * 1024; // if valid_size >= work_limit - 100MB, we think it may evict soon
static constexpr int64_t SS_HANDLE_EXPIRATION_MICRO_MIN_INTERVAL_US = 30 * 1000 * 1000L; // 30s
static constexpr uint32_t SS_PERSIST_MICRO_CKPT_SPLIT_CNT = 100;
static constexpr uint32_t SS_PERSIST_MICRO_CKPT_SPLIT_SMALL_CNT = 8;
static constexpr int64_t SS_LOAD_PERSISTED_MICRO_META_MAX_TIME_US = 5 * 60 * 1000 * 1000L; // 5min
static constexpr int64_t SS_PERSIST_META_MAX_RETRY_DURATION_US = 10 * 60 * 1000 * 1000L; // 10min
static constexpr int64_t SS_MEM_LIMIT_TRIGGER_CKPT_PCT = 90; // Trigger micro_ckpt when memory reaches 90% of limit

// [Common settings]
static constexpr int64_t SS_MEM_BUF_ALIGNMENT = 4096;
static constexpr int64_t SS_MIN_CACHE_FILE_SIZE = 180 * 1024 * 1024L; // 180MB
static constexpr int64_t SS_STARTUP_CACHE_FILE_SIZE = 810 * 1024 * 1024L; // 810MB
static constexpr int64_t SS_MINI_MODE_CACHE_FILE_SIZE = 10250 * 1024 * 1024L;
static constexpr int64_t SS_SERIALIZE_EXTRA_BUF_LEN = (sizeof(int64_t) + NS_::OB_SERIALIZE_SIZE_NEED_BYTES);
static constexpr int64_t SS_MICRO_META_MAP_ITEM_SIZE = 32;
static constexpr int64_t SS_LOOP_SLEEP_INTERVAL_US = 1000;
static constexpr int64_t SS_LOOP_PRINT_LOG_INTERVAL_US = 5 * 1000 * 1000L;
static constexpr uint64_t SS_MAX_CKPT_EXPIRATION_TIME_US = SS_DEF_CACHE_EXPIRATION_TIME_S * 1000 * 1000L;
static constexpr int64_t SS_DEF_ARRAY_CNT = 32;
static constexpr int64_t SS_DEF_SMALL_ARRAY_CNT = 4;
static constexpr bool SS_ENABLE_SAVE_META_MEMORY = true;
static constexpr int64_t SS_OPERATE_MICRO_CACHE_TIMEOUT_US = 60 * 1000 * 1000L; // 60s
static constexpr int64_t SS_DEF_MICRO_META_CNT_PER_8KB = 49;
static constexpr int64_t SS_MICRO_CACHE_PRINT_WARN_INTERVAL_US = 500 * 1000;
static constexpr int64_t SS_MONITOR_ABNORMAL_LOG_INTERVAL_US = 60 * 1000 * 1000; // 1 minute

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_DEFINE_H_ */
