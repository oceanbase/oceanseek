/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include <stdint.h>
#include "lib/alloc/alloc_func.h"
#include "lib/checksum/ob_crc64.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_global_macro_block_kvcache.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_persist_stat.h"
#include "storage/shared_storage/ob_ss_local_cache_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObSSTenantMacroBlockKVCacheMgr::ObSSTenantMacroBlockKVCacheMgr()
  : is_inited_(false),
    is_stopped_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    persist_worker_(),
    hit_cnt_(0),
    hit_size_(0),
    miss_cnt_(0),
    miss_size_(0)
{
}

int ObSSTenantMacroBlockKVCacheMgr::calc_persist_ring_capacity_(
    const int64_t tenant_memory_limit,
    int64_t &ring_capacity)
{
  int ret = OB_SUCCESS;
  const int64_t MEMORY_PERCENTAGE = 80;
  const int64_t MIN_RING_CAPACITY = 16384;
  const int64_t MAX_RING_CAPACITY = 131072;
  ring_capacity = 0;
  if (OB_UNLIKELY(tenant_memory_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant memory limit for macro persist ring capacity", KR(ret), K(tenant_memory_limit));
  } else {
    // Ring 容量 = 租户内存上限 × 80% / 2MiB，向下取整并限制在 [16384, 131072] 条。
    const int64_t estimated_entry_count =
        tenant_memory_limit * MEMORY_PERCENTAGE / 100 / OB_DEFAULT_MACRO_BLOCK_SIZE;
    ring_capacity = MAX(MIN_RING_CAPACITY, MIN(MAX_RING_CAPACITY, estimated_entry_count));
  }
  return ret;
}

void ObSSTenantMacroBlockKVCacheMgr::reset_hit_stat_()
{
  ATOMIC_STORE(&hit_cnt_, 0);
  ATOMIC_STORE(&hit_size_, 0);
  ATOMIC_STORE(&miss_cnt_, 0);
  ATOMIC_STORE(&miss_size_, 0);
}

void ObSSTenantMacroBlockKVCacheMgr::record_hit_(
    const int64_t count,
    const int64_t size)
{
  ATOMIC_AAF(&hit_cnt_, count);
  ATOMIC_AAF(&hit_size_, size);
}

void ObSSTenantMacroBlockKVCacheMgr::record_miss_(
    const int64_t count,
    const int64_t size)
{
  ATOMIC_AAF(&miss_cnt_, count);
  ATOMIC_AAF(&miss_size_, size);
}

int ObSSTenantMacroBlockKVCacheMgr::mtl_init(
    ObSSTenantMacroBlockKVCacheMgr *&tenant_kvcache_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_kvcache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro block KVCACHE is null", KR(ret));
  } else {
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(tenant_kvcache_mgr->init(tenant_id))) {
      LOG_WARN("fail to init macro block KVCACHE", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObSSTenantMacroBlockKVCacheMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro block KVCACHE is already inited", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    const int64_t tenant_memory_limit = lib::get_tenant_memory_limit(tenant_id);
    int64_t persist_ring_capacity = 0;
    reset_hit_stat_();
    if (OB_UNLIKELY(!OB_GLOBAL_MACRO_BLOCK_KVCACHE.is_inited())) {
      ret = OB_NOT_INIT;
      LOG_WARN("global macro block KVCACHE is not inited",
          KR(ret), K(tenant_id));
    } else if (OB_FAIL(calc_persist_ring_capacity_(tenant_memory_limit, persist_ring_capacity))) {
      LOG_WARN("fail to calculate macro persist ring capacity", KR(ret), K(tenant_id), K(tenant_memory_limit));
    } else if (OB_FAIL(persist_worker_.init(
        tenant_id, persist_ring_capacity, PERSIST_IO_DEPTH))) {
      LOG_WARN("fail to init macro persist worker",
          KR(ret), K(tenant_id));
    } else {
      tenant_id_ = tenant_id;
      ATOMIC_STORE(&is_stopped_, true);
      is_inited_ = true;
      LOG_INFO("init tenant macro block KVCACHE", K(tenant_id), K(tenant_memory_limit), K(persist_ring_capacity));
    }
  }
  return ret;
}

int ObSSTenantMacroBlockKVCacheMgr::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block KVCACHE is not inited", KR(ret));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro block KVCACHE is already started", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(persist_worker_.start())) {
    LOG_WARN("fail to start macro persist worker", KR(ret), K_(tenant_id));
  } else {
    ATOMIC_STORE(&is_stopped_, false);
    LOG_INFO("succ to start macro block KVCACHE", K_(tenant_id));
  }
  return ret;
}

void ObSSTenantMacroBlockKVCacheMgr::stop()
{
  LOG_INFO("start to stop macro block KVCACHE");
  if (IS_INIT) {
    // 先关闭 facade 前台入口，再通知 Worker 停止接收 entry 并退出。
    ATOMIC_STORE(&is_stopped_, true);
    persist_worker_.stop();
  }
  LOG_INFO("finish to stop macro block KVCACHE");
}

int ObSSTenantMacroBlockKVCacheMgr::wait()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to wait macro block KVCACHE");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block KVCACHE is not inited", KR(ret));
  } else {
    // Worker 检查是否已 stop，并负责汇合线程、丢弃未提交 entry。
    // 已提交的 best-effort L2 IO 不在 wait() 中等待，后续在 destroy()
    // 中释放 IO Handle，并取消未完成的 IO 操作。
    if (OB_FAIL(persist_worker_.wait())) {
      LOG_WARN("fail to wait macro persist worker",
          KR(ret), K_(tenant_id));
    }
  }
  LOG_INFO("finish to wait macro block KVCACHE", KR(ret));
  return ret;
}

void ObSSTenantMacroBlockKVCacheMgr::destroy()
{
  LOG_INFO("start to destroy macro block KVCACHE");
  // MTL 已依次完成 stop() 和 wait()，这里只释放资源。
  persist_worker_.destroy();
  reset_hit_stat_();
  ATOMIC_STORE(&is_stopped_, true);
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  LOG_INFO("finish to destroy macro block KVCACHE");
}

bool ObSSTenantMacroBlockKVCacheMgr::is_cache_enabled() const
{
  return persist_worker_.is_cache_enabled();
}

void ObSSTenantMacroBlockKVCacheMgr::enable_cache()
{
  persist_worker_.set_cache_enabled(true);
  LOG_INFO("enable macro block KVCACHE", K_(tenant_id));
}

void ObSSTenantMacroBlockKVCacheMgr::disable_cache()
{
  // disable 后，put/get 停止服务，check_exist 按不存在处理，Worker 不再提交新的 L2 IO。
  // 已有 KVCACHE 数据和已提交的 L2 IO 保留；erase 接口仍可清理已有数据。
  persist_worker_.set_cache_enabled(false);
  LOG_INFO("disable macro block KVCACHE", K_(tenant_id));
}

int ObSSTenantMacroBlockKVCacheMgr::erase_tenant_entries()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block KVCACHE is not inited", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro block KVCACHE is stopped", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(persist_worker_.discard_pending_entries())) {
    LOG_WARN("fail to discard pending macro persist entries",
        KR(ret), K_(tenant_id));
  } else if (OB_FAIL(OB_GLOBAL_MACRO_BLOCK_KVCACHE.erase_tenant(tenant_id_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase tenant macro block KVCACHE",
          KR(ret), K_(tenant_id));
    }
  }
  return ret;
}

// TODO @yuchen: 添加周期性打印 stat 的逻辑。
int ObSSTenantMacroBlockKVCacheMgr::get_cache_stat(
    ObSSTenantMacroBlockKVCacheMgrStat &cache_stat) const
{
  int ret = OB_SUCCESS;
  cache_stat.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block KVCACHE is not inited", KR(ret));
  } else {
    cache_stat.hit_cnt_ = ATOMIC_LOAD(&hit_cnt_);
    cache_stat.hit_size_ = ATOMIC_LOAD(&hit_size_);
    cache_stat.miss_cnt_ = ATOMIC_LOAD(&miss_cnt_);
    cache_stat.miss_size_ = ATOMIC_LOAD(&miss_size_);
    persist_worker_.get_persist_stat_snapshot(cache_stat.persist_stat_);

    cache_stat.memory_size_ = OB_GLOBAL_MACRO_BLOCK_KVCACHE.size(tenant_id_);
    cache_stat.valid_macro_cnt_ = OB_GLOBAL_MACRO_BLOCK_KVCACHE.count(tenant_id_);
  }
  return ret;
}

int ObSSTenantMacroBlockKVCacheMgr::put(
    const MacroBlockId &macro_id,
    const uint64_t effective_tablet_id,
    const char *buf,
    const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block KVCACHE is not inited", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!macro_id.is_valid()
      || buf_size <= 0 || buf_size > INT32_MAX)
      || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(buf_size), KP(buf));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro block KVCACHE is stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_cache_enabled())) {
    ret = OB_SERVICE_STOPPED;
    LOG_TRACE("macro block KVCACHE is disabled", KR(ret));
  } else {
    const ObSSMacroBlockCacheKey key(tenant_id_, macro_id);
    const uint32_t crc = static_cast<uint32_t>(ob_crc64(buf, buf_size));
    const ObSSMacroBlockCacheValue value(
        effective_tablet_id, buf, buf_size, crc);
    if (OB_FAIL(OB_GLOBAL_MACRO_BLOCK_KVCACHE.put(key, value, false/*overwrite*/))) {
      if (OB_ENTRY_EXIST == ret) {
      // 重复 put 保留旧值，按幂等成功处理，也不重复加入 Ring Buffer。
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to put macro block into KVCACHE", KR(ret), K(macro_id),
            K(effective_tablet_id), K(buf_size));
      }
    } else {
      if (OB_FAIL(persist_worker_.enqueue(macro_id))) {
        if (OB_SERVICE_STOPPED == ret || OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to add macro block persist entry", KR(ret), K(macro_id));
        }
      }
    }
  }
  return ret;
}

int ObSSTenantMacroBlockKVCacheMgr::get(
    const MacroBlockId &macro_id,
    ObIOInfo &io_info,
    ObStorageObjectHandle &obj_handle,
    bool &is_hit_cache)
{
  int ret = OB_SUCCESS;
  is_hit_cache = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block KVCACHE is not inited", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid()
      || io_info.offset_ < 0 || io_info.size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id), K(io_info));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro block KVCACHE is stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_cache_enabled())) {
    ret = OB_SERVICE_STOPPED;
    LOG_TRACE("macro block KVCACHE is disabled", KR(ret));
  } else {
    const ObSSMacroBlockCacheValue *value = nullptr;
    ObKVCacheHandle handle;
    int64_t macro_size = 0;
    int64_t available_size = 0;
    const ObSSMacroBlockCacheKey key(tenant_id_, macro_id);
    if (OB_FAIL(OB_GLOBAL_MACRO_BLOCK_KVCACHE.get(key, value, handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        record_miss_(1, io_info.size_);
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get macro block from KVCACHE", KR(ret), K(macro_id));
      }
    } else if (OB_ISNULL(value) || OB_UNLIKELY(!value->is_valid() || !handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("KVCACHE value or handle should be valid", KR(ret), K(macro_id),
          KP(value), K(handle));
    } else if (FALSE_IT(macro_size = value->data_size_)) {
    } else if (OB_FAIL(value->check_crc())) {
      LOG_WARN("fail to check crc", KR(ret), K(macro_id), K(macro_size));
    } else if (OB_UNLIKELY(io_info.offset_ >= macro_size)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro block read range", KR(ret), K(macro_id), K(macro_size), K(io_info));
    } else if (FALSE_IT(available_size = macro_size - io_info.offset_)) {
      // value.data_ 指向完整宏块起始地址，命中后需要叠加本次读取的 offset。
    } else if (OB_FAIL(handle_hit_macro_block_(
        macro_id, value->data_ + io_info.offset_,
        available_size, io_info, obj_handle))) {
      LOG_WARN("fail to handle hit macro block",
          KR(ret), K(macro_id), K(macro_size), K(available_size));
    } else {
      // hit/miss bytes 都统计本次业务实际读取的字节。
      record_hit_(
          1, MIN(available_size, static_cast<int64_t>(io_info.size_)));
      is_hit_cache = true;
    }
  }
  return ret;
}

int ObSSTenantMacroBlockKVCacheMgr::handle_hit_macro_block_(
    const MacroBlockId &macro_id,
    const char *io_buf,
    const int64_t io_buf_size,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  const int64_t data_size =
      MIN(io_buf_size, static_cast<int64_t>(io_info.size_));
  if (OB_ISNULL(io_buf) || OB_UNLIKELY(io_buf_size <= 0 || data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block data",
        KR(ret), KP(io_buf), K(io_buf_size), K(data_size), K(io_info));
  } else if (OB_FAIL(ObSSLocalCacheUtil::handle_hit_cache_in_memory(
      io_buf, data_size, io_info, object_handle))) {
    LOG_WARN("fail to handle hit cache in memory",
        KR(ret), K(data_size), K(io_info));
  } else if (OB_FAIL(object_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
  }
  return ret;
}

int ObSSTenantMacroBlockKVCacheMgr::check_exist(
    const MacroBlockId &macro_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block KVCACHE is not inited", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro block KVCACHE is stopped", KR(ret));
  } else if (OB_UNLIKELY(!is_cache_enabled())) {
    // 缓存禁用时按不存在处理。
    if (TC_REACH_TIME_INTERVAL(CACHE_DISABLED_WARN_INTERVAL_US)) {
      LOG_WARN("macro block KVCACHE is disabled", K_(tenant_id));
    }
  } else {
    const ObSSMacroBlockCacheValue *value = nullptr;
    ObKVCacheHandle handle;
    const ObSSMacroBlockCacheKey key(tenant_id_, macro_id);
    if (OB_FAIL(OB_GLOBAL_MACRO_BLOCK_KVCACHE.peek(key, value, handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to peek macro block KVCACHE for exist", KR(ret), K(macro_id));
      }
    } else if (OB_ISNULL(value) || OB_UNLIKELY(!value->is_valid() || !handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("KVCACHE peek value or handle should be valid", KR(ret), K(macro_id),
          KP(value), K(handle));
    } else {
      is_exist = true;
    }
  }
  return ret;
}

int ObSSTenantMacroBlockKVCacheMgr::erase_macro_block(
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro block KVCACHE is not inited", KR(ret));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(ATOMIC_LOAD(&is_stopped_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro block KVCACHE is stopped",
        KR(ret), K(macro_id), K_(tenant_id));
  } else {
    const ObSSMacroBlockCacheKey key(tenant_id_, macro_id);
    if (OB_FAIL(OB_GLOBAL_MACRO_BLOCK_KVCACHE.erase(key))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to erase macro block from KVCACHE", KR(ret), K(macro_id));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
