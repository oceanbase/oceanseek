/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_ss_common_info.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"

namespace oceanbase
{
namespace storage
{

static int set_add_stat_value(common::ObDiagnoseTenantInfo &diag_info, const uint32_t stat_id, const int64_t value)
{
  int ret = OB_SUCCESS;
  ObStatEventAddStat *stat = nullptr;
  if (OB_UNLIKELY(stat_id >= ObStatEventIds::STAT_EVENT_ADD_END)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "stat id invalid", KR(ret), K(stat_id));
  } else if (OB_ISNULL(stat = diag_info.get_add_stat_stats().get(stat_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "stat id invalid", KR(ret), K(stat_id), KP(stat));
  } else {
    stat->stat_value_ = value;
  }
  return ret;
}


static int set_ss_macro_cache_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info)
{
  enum StatColumn
  {
    COL_MAX_CAPACITY = 0,
    COL_USED_SIZE = 1,
    COL_HIT_CNT = 2,
    COL_MISS_CNT = 3,
    COL_HIT_BYTES = 4,
    COL_MISS_BYTES = 5,
    COLUMN_COUNT
  };
  static uint32_t MACRO_CACHE_TYPE_TO_STAT_ID_MAP[SS_MACRO_CACHE_MAX_TYPE_VAL][COLUMN_COUNT] = {
    {
      ObStatEventIds::MACRO_CACHE_META_MAX_CAPACITY,
      ObStatEventIds::MACRO_CACHE_META_USED_SIZE,
      ObStatEventIds::MACRO_CACHE_META_HIT_CNT,
      ObStatEventIds::MACRO_CACHE_META_MISS_CNT,
      ObStatEventIds::MACRO_CACHE_META_HIT_BYTES,
      ObStatEventIds::MACRO_CACHE_META_MISS_BYTES,
    },
    {
      ObStatEventIds::MACRO_CACHE_TMP_MAX_CAPACITY,
      ObStatEventIds::MACRO_CACHE_TMP_USED_SIZE,
      ObStatEventIds::MACRO_CACHE_TMP_HIT_CNT,
      ObStatEventIds::MACRO_CACHE_TMP_MISS_CNT,
      ObStatEventIds::MACRO_CACHE_TMP_HIT_BYTES,
      ObStatEventIds::MACRO_CACHE_TMP_MISS_BYTES,
    },
    {
      ObStatEventIds::MACRO_CACHE_MACRO_MAX_CAPACITY,
      ObStatEventIds::MACRO_CACHE_MACRO_USED_SIZE,
      ObStatEventIds::MACRO_CACHE_MACRO_HIT_CNT,
      ObStatEventIds::MACRO_CACHE_MACRO_MISS_CNT,
      ObStatEventIds::MACRO_CACHE_MACRO_HIT_BYTES,
      ObStatEventIds::MACRO_CACHE_MACRO_MISS_BYTES,
    },
    {
      ObStatEventIds::MACRO_CACHE_HOT_MACRO_MAX_CAPACITY,
      ObStatEventIds::MACRO_CACHE_HOT_MACRO_USED_SIZE,
      ObStatEventIds::MACRO_CACHE_HOT_MACRO_HIT_CNT,
      ObStatEventIds::MACRO_CACHE_HOT_MACRO_MISS_CNT,
      ObStatEventIds::MACRO_CACHE_HOT_MACRO_HIT_BYTES,
      ObStatEventIds::MACRO_CACHE_HOT_MACRO_MISS_BYTES,
    },
  };
  static_assert(
      sizeof(MACRO_CACHE_TYPE_TO_STAT_ID_MAP) ==
          sizeof(uint32_t) * COLUMN_COUNT * SS_MACRO_CACHE_MAX_TYPE_VAL,
      "element num mismatch");

  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(macro_cache_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "macro cache manager is null", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tnt_disk_space_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "disk space manager is null", KR(ret), K(tenant_id));
  }

  ObSSMacroCacheStat cache_stat;
  ObStorageCacheHitStat hit_stat;
  for (int64_t i = 0; OB_SUCC(ret) && i < SS_MACRO_CACHE_MAX_TYPE_VAL; i++) {
    const ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(i);
    cache_stat.reset();
    hit_stat.reset();
    if (OB_FAIL(tnt_disk_space_mgr->get_macro_cache_stat(cache_type, cache_stat))) {
      OB_LOG(WARN, "fail to get macro cache stat", KR(ret), K(cache_type));
    } else if (OB_FAIL(macro_cache_mgr->get_macro_cache_hit_stat_by_cache_type(cache_type, hit_stat))) {
      OB_LOG(WARN, "fail to get macro cache hit stat", KR(ret), K(cache_type));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_MAX_CAPACITY],
        cache_stat.get_max() * tnt_disk_space_mgr->get_unit_config_shared_macro_cache_size() / 100))) {
      OB_LOG(WARN, "fail to set macro cache max capacity", KR(ret), K(cache_type),
          K(cache_stat), K(hit_stat), K(MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_MAX_CAPACITY]));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_USED_SIZE], cache_stat.used_))) {
      OB_LOG(WARN, "fail to set macro cache used", KR(ret), K(cache_type),
          K(cache_stat), K(hit_stat), K(MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_USED_SIZE]));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_HIT_CNT], hit_stat.cache_hit_cnt_))) {
      OB_LOG(WARN, "fail to set macro cache hit count", KR(ret), K(cache_type),
          K(cache_stat), K(hit_stat), K(MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_HIT_CNT]));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_MISS_CNT], hit_stat.cache_miss_cnt_))) {
      OB_LOG(WARN, "fail to set macro cache miss count", KR(ret), K(cache_type),
          K(cache_stat), K(hit_stat), K(MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_MISS_CNT]));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_HIT_BYTES], hit_stat.cache_hit_bytes_))) {
      OB_LOG(WARN, "fail to set macro cache miss count", KR(ret), K(cache_type),
          K(cache_stat), K(hit_stat), K(MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_HIT_BYTES]));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_MISS_BYTES], hit_stat.cache_miss_bytes_))) {
      OB_LOG(WARN, "fail to set macro cache miss count", KR(ret), K(cache_type),
          K(cache_stat), K(hit_stat), K(MACRO_CACHE_TYPE_TO_STAT_ID_MAP[i][COL_MISS_BYTES]));
    } 
  }

  if (FAILEDx(diag_info.set_stat(ObStatEventIds::MACRO_CACHE_ALLOC_DISK_SIZE,
                                 tnt_disk_space_mgr->get_unit_config_shared_macro_cache_size()))) {
    OB_LOG(WARN, "fail to set ss macro cache alloc disk size", KR(ret));
  } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MACRO_CACHE_WRITE_CACHE_SIZE,
                                        macro_cache_mgr->get_write_cache_size()))) {
    OB_LOG(WARN, "fail to set ss macro cache write cache size", KR(ret));
  }

  return ret;
}

static int set_ss_macro_cache_block_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info)
{
  enum StatColumn
  {
    COL_HIT_CNT = 0,
    COL_MISS_CNT = 1,
    COL_HIT_BYTES = 2,
    COL_MISS_BYTES = 3,
    COLUMN_COUNT
  };
  static uint32_t MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[][COLUMN_COUNT] = {
    {
      ObStatEventIds::MACRO_CACHE_PRIVATE_MACRO_HIT_CNT,
      ObStatEventIds::MACRO_CACHE_PRIVATE_MACRO_MISS_CNT,
      ObStatEventIds::MACRO_CACHE_PRIVATE_MACRO_HIT_BYTES,
      ObStatEventIds::MACRO_CACHE_PRIVATE_MACRO_MISS_BYTES,
    },
    {
      ObStatEventIds::MACRO_CACHE_SHARED_MACRO_HIT_CNT,
      ObStatEventIds::MACRO_CACHE_SHARED_MACRO_MISS_CNT,
      ObStatEventIds::MACRO_CACHE_SHARED_MACRO_HIT_BYTES,
      ObStatEventIds::MACRO_CACHE_SHARED_MACRO_MISS_BYTES,
    },
    {
      ObStatEventIds::EXTERNAL_TABLE_DISK_CACHE_HIT_CNT,
      ObStatEventIds::EXTERNAL_TABLE_DISK_CACHE_MISS_CNT,
      ObStatEventIds::EXTERNAL_TABLE_DISK_CACHE_HIT_BYTES,
      ObStatEventIds::EXTERNAL_TABLE_DISK_CACHE_MISS_BYTES,
    },
    {
      ObStatEventIds::EXTERNAL_LOB_DISK_CACHE_HIT_CNT,
      ObStatEventIds::EXTERNAL_LOB_DISK_CACHE_MISS_CNT,
      ObStatEventIds::EXTERNAL_LOB_DISK_CACHE_HIT_BYTES,
      ObStatEventIds::EXTERNAL_LOB_DISK_CACHE_MISS_BYTES,
    },
  };
  static_assert(
      sizeof(MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP) ==
          sizeof(uint32_t) * COLUMN_COUNT * SS_MACRO_BLOCK_MAX_TYPE_VAL,
      "element num mismatch");

  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(macro_cache_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "macro cache manager is null", KR(ret), K(tenant_id));
  }

  ObStorageCacheHitStat hit_stat;
  for (int64_t i = 0; OB_SUCC(ret) && i < SS_MACRO_BLOCK_MAX_TYPE_VAL; i++) {
    const ObSSMacroBlockType block_type = static_cast<ObSSMacroBlockType>(i);
    hit_stat.reset();
    if (OB_FAIL(macro_cache_mgr->get_macro_cache_hit_stat_by_block_type(block_type, hit_stat))) {
      OB_LOG(WARN, "fail to get macro cache hit stat", KR(ret), K(block_type));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[i][COL_HIT_CNT], hit_stat.cache_hit_cnt_))) {
      OB_LOG(WARN, "fail to set macro cache hit count", KR(ret), K(block_type),
          K(hit_stat), K(MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[i][COL_HIT_CNT]));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[i][COL_MISS_CNT], hit_stat.cache_miss_cnt_))) {
      OB_LOG(WARN, "fail to set macro cache miss count", KR(ret), K(block_type),
          K(hit_stat), K(MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[i][COL_MISS_CNT]));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[i][COL_HIT_BYTES], hit_stat.cache_hit_bytes_))) {
      OB_LOG(WARN, "fail to set macro cache miss count", KR(ret), K(block_type),
          K(hit_stat), K(MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[i][COL_HIT_BYTES]));
    } else if (OB_FAIL(diag_info.set_stat(
        MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[i][COL_MISS_BYTES], hit_stat.cache_miss_bytes_))) {
      OB_LOG(WARN, "fail to set macro cache miss count", KR(ret), K(block_type),
          K(hit_stat), K(MACRO_CACHE_BLOCK_TYPE_TO_STAT_ID_MAP[i][COL_MISS_BYTES]));
    } 
  }

  return ret;
}

static int set_ss_macro_block_kvcache_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_kvcache_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "tenant macro block KVCACHE mgr is null", KR(ret), K(tenant_id));
  } else {
    ObSSTenantMacroBlockKVCacheMgrStat cache_stat;
    const int64_t total_capacity = 0; // 新 KVCACHE 没有租户级宏块缓存硬上限。
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(tenant_kvcache_mgr->get_cache_stat(cache_stat))) {
      // 诊断统计采用 best-effort：保留访问统计，用量查询失败时按零值展示。
      OB_LOG(WARN, "fail to get macro block KVCACHE stat", KR(tmp_ret), K(tenant_id));
    }
    const ObSSMacroPersistStat &persist_stat = cache_stat.persist_stat_;
    const int64_t failed_write_back_cnt =
        persist_stat.persist_submit_fail_cnt_
        + persist_stat.persist_io_fail_cnt_;
    // 以下映射只用于兼容旧 ObSSMemMacroCache 诊断项：
    // 1. memory_size 包含 Key/Value 和节点元数据，不能映射为纯宏块数据大小；
    // 2. 通用 KVCACHE 没有 small/evict/expire 分类；
    // 3. 新链路没有 retry prewarm、fast persist 和 async evict 请求；
    // 4. 通用 KVCACHE 没有旧固定租户 MemBlock 池，也不提供同口径淘汰原因统计。
    // TODO(ss-kvcache): 旧 MEM_MACRO_CACHE_* 诊断项下线后删除该兼容映射。
    if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_HIT_CNT,
                                   cache_stat.hit_cnt_))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_HIT_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_HIT_BYTES,
                                          cache_stat.hit_size_))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_HIT_BYTES", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_MISS_CNT,
                                          cache_stat.miss_cnt_))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_MISS_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_MISS_BYTES,
                                          cache_stat.miss_size_))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_MISS_BYTES", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_TOTAL_CAPACITY,
                                          total_capacity))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_TOTAL_CAPACITY", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_USED_MEM_SIZE,
                                          cache_stat.memory_size_))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_USED_MEM_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_VALID_MACRO_CNT,
                                          cache_stat.valid_macro_cnt_))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_VALID_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_VALID_MACRO_SIZE,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_VALID_MACRO_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_SMALL_MACRO_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_SMALL_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_SMALL_MACRO_SIZE,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_SMALL_MACRO_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_WRITE_BACK_MACRO_CNT,
                                          persist_stat.persist_success_cnt_))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_WRITE_BACK_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_WRITE_BACK_MACRO_SIZE,
                                          persist_stat.persist_success_bytes_))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_WRITE_BACK_MACRO_SIZE", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_FAIL_WRITE_BACK_MACRO_CNT,
                                          failed_write_back_cnt))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_FAIL_WRITE_BACK_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_RETRY_PREWARM_MACRO_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_RETRY_PREWARM_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_EVICT_MACRO_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_EVICT_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_EXPIRE_MACRO_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_EXPIRE_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_FAST_PERSIST_MACRO_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_FAST_PERSIST_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_ASYNC_EVICT_MACRO_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_ASYNC_EVICT_MACRO_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_EVICT_BLK_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_EVICT_BLK_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_EXPIRE_BLK_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_EXPIRE_BLK_CNT", KR(ret), K(tenant_id));
    } else if (OB_FAIL(diag_info.set_stat(ObStatEventIds::MEM_MACRO_CACHE_EXTRA_EVICT_BLK_CNT,
                                          0))) {
      OB_LOG(WARN, "fail to set MEM_MACRO_CACHE_EXTRA_EVICT_BLK_CNT", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int set_ss_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(set_ss_macro_cache_stats(tenant_id, diag_info))) {
    OB_LOG(WARN, "fail to set ss macro cache stats", KR(ret), K(tenant_id));
  } else if (OB_FAIL(set_ss_macro_cache_block_stats(tenant_id, diag_info))) {
    OB_LOG(WARN, "fail to set ss macro cache block stats", KR(ret), K(tenant_id));
  } else if (OB_FAIL(set_ss_macro_block_kvcache_stats(tenant_id, diag_info))) {
    OB_LOG(WARN, "fail to set ss macro block KVCACHE stats", KR(ret), K(tenant_id));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
