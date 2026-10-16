/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_write_kv_2_lcache_task.h"
#include "share/config/ob_server_config.h"
#include "share/io/ob_io_define.h"
#include "share/rc/ob_tenant_base.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_global_macro_block_kvcache.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

ObSSWriteKV2LCacheEntry::ObSSWriteKV2LCacheEntry()
  : macro_id_(),
    effective_tablet_id_(OB_INVALID_ID),
    data_size_(0),
    write_handle_(),
    enqueue_ts_(0),
    submit_ts_(0)
{
}

void ObSSWriteKV2LCacheEntry::reset()
{
  macro_id_.reset();
  effective_tablet_id_ = OB_INVALID_ID;
  data_size_ = 0;
  write_handle_.reset();
  enqueue_ts_ = 0;
  submit_ts_ = 0;
}

ObSSWriteKV2LCacheTask::ObSSWriteKV2LCacheTask()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    capacity_(0),
    total_entry_arr_(nullptr),
    free_entry_list_(),
    write_entry_list_(),
    submit_fail_cnt_(0),
    success_cnt_(0),
    success_bytes_(0),
    io_fail_cnt_(0),
    allocator_()
{
}

ObSSWriteKV2LCacheTask::~ObSSWriteKV2LCacheTask()
{
  destroy();
}

int ObSSWriteKV2LCacheTask::init(
    const uint64_t tenant_id,
    const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("write KVCACHE to LCache task is already inited",
        KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id)
      || tenant_id != MTL_ID()
      || capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write KVCACHE to LCache task argument", KR(ret),
        K(tenant_id), "mtl_id", MTL_ID(), K(capacity));
  } else {
    tenant_id_ = tenant_id;
    capacity_ = capacity;
    allocator_.set_attr(ObMemAttr(tenant_id, "SSMacroPIO"));
    if (OB_ISNULL(total_entry_arr_ = static_cast<ObSSWriteKV2LCacheEntry *>(
        allocator_.alloc(sizeof(ObSSWriteKV2LCacheEntry) * capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate write KVCACHE to LCache entries",
          KR(ret), K(tenant_id), K(capacity));
    } else {
      for (int64_t i = 0; i < capacity; ++i) {
        new (total_entry_arr_ + i) ObSSWriteKV2LCacheEntry();
      }
      if (OB_FAIL(free_entry_list_.init(capacity, &allocator_,
          ObMemAttr(tenant_id, "SSMacroPIOFree")))) {
        LOG_WARN("fail to init free write KVCACHE to LCache queue",
            KR(ret), K(tenant_id), K(capacity));
      } else if (OB_FAIL(write_entry_list_.init(capacity, &allocator_,
          ObMemAttr(tenant_id, "SSMacroPIOUse")))) {
        LOG_WARN("fail to init inflight write KVCACHE to LCache queue",
            KR(ret), K(tenant_id), K(capacity));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < capacity; ++i) {
          if (OB_FAIL(free_entry_list_.push(total_entry_arr_ + i))) {
            LOG_WARN("fail to seed write KVCACHE to LCache entry pool",
                KR(ret), K(i), K(capacity));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    } else {
      destroy();
    }
  }
  return ret;
}

int ObSSWriteKV2LCacheTask::acquire_entry_(
    ObSSWriteKV2LCacheEntry *&entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_entry_list_.pop(entry))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
      LOG_TRACE("no free write KVCACHE to LCache entry",
          KR(ret), K_(tenant_id),
          "free_count", free_entry_list_.get_curr_total());
    } else {
      LOG_WARN("fail to acquire write KVCACHE to LCache entry",
          KR(ret), "free_count", free_entry_list_.get_curr_total());
    }
    ATOMIC_INC(&submit_fail_cnt_);
  } else if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write KVCACHE to LCache entry is null",
        KR(ret), "free_count", free_entry_list_.get_curr_total());
    ATOMIC_INC(&submit_fail_cnt_);
  }
  return ret;
}

int ObSSWriteKV2LCacheTask::recycle_entry_(
    ObSSWriteKV2LCacheEntry &entry)
{
  int ret = OB_SUCCESS;
  entry.reset();
  if (OB_FAIL(free_entry_list_.push(&entry))) {
    LOG_WARN("fail to recycle write KVCACHE to LCache entry",
        KR(ret), K(entry));
  }
  return ret;
}

int ObSSWriteKV2LCacheTask::do_write_into_macro_cache_(
    const ObSSMacroPersistEntry &persist_entry,
    const ObSSMacroBlockCacheValue &value,
    ObSSWriteKV2LCacheEntry &write_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!persist_entry.macro_id_.is_valid()
      || persist_entry.enqueue_ts_ <= 0
      || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write KVCACHE to LCache argument",
        KR(ret), K(persist_entry), K(value));
  } else if (OB_UNLIKELY(write_entry.write_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write KVCACHE to LCache entry is already in use",
        KR(ret), K(write_entry));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);
    ObStorageObjectWriteInfo write_info;
    write_entry.macro_id_ = persist_entry.macro_id_;
    write_entry.effective_tablet_id_ = value.effective_tablet_id_;
    write_entry.data_size_ = value.data_size_;
    write_entry.enqueue_ts_ = persist_entry.enqueue_ts_;
    write_info.mtl_tenant_id_ = tenant_id_;
    write_info.buffer_ = value.data_;
    write_info.offset_ = 0;
    write_info.size_ = value.data_size_;
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.io_desc_.set_wait_event(
        ObWaitEventIds::SS_MACRO_KVCACHE_PERSIST_WRITE);
    write_info.io_desc_.set_unsealed();
    write_info.set_effective_tablet_id(value.effective_tablet_id_);
    write_info.set_is_write_cache(false);
    // async_write 成功后 IOManager 已复制 buffer，调用方即可释放 KV Handle。
    if (OB_FAIL(ObSSObjectAccessUtil::async_write_file_to_macro_cache(
        SSObjUtil::has_effective_tablet_id(persist_entry.macro_id_),
        persist_entry.macro_id_, write_info, write_entry.write_handle_,
        0 /* hot_tablet_alloc_timeout_us */))) {
      LOG_WARN("fail to asynchronously write macro block into LCache",
          KR(ret), K_(tenant_id), K(persist_entry.macro_id_),
          K(value.effective_tablet_id_), K(value.data_size_));
    } else if (OB_UNLIKELY(!write_entry.write_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid write KVCACHE to LCache IO handle",
          KR(ret), K_(tenant_id), K(persist_entry.macro_id_));
    } else {
      write_entry.submit_ts_ = ObTimeUtility::current_time();
    }
  }
  if (OB_FAIL(ret)) {
    ATOMIC_INC(&submit_fail_cnt_);
  }
  return ret;
}

int ObSSWriteKV2LCacheTask::async_persist_macro_block(
    const ObSSMacroPersistEntry &persist_entry,
    const ObSSMacroBlockCacheValue &value)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSWriteKV2LCacheEntry *entry = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("write KVCACHE to LCache task is not inited", KR(ret));
  } else if (OB_UNLIKELY(MTL_ID() != tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write KVCACHE to LCache task runs in wrong tenant context",
        KR(ret), K_(tenant_id), "mtl_id", MTL_ID());
  } else if (OB_UNLIKELY(!persist_entry.macro_id_.is_valid()
      || persist_entry.enqueue_ts_ <= 0
      || !value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write KVCACHE to LCache argument",
        KR(ret), K(persist_entry), K(value));
  } else if (OB_FAIL(acquire_entry_(entry))) {
    if (OB_EAGAIN == ret) {
      LOG_TRACE("write KVCACHE to LCache reaches concurrency limit",
          KR(ret), K_(tenant_id), K(persist_entry));
    } else {
      LOG_WARN("fail to acquire write KVCACHE to LCache entry",
          KR(ret), K_(tenant_id), K(persist_entry));
    }
  } else if (OB_FAIL(do_write_into_macro_cache_(persist_entry, value, *entry))) {
    LOG_WARN("fail to write KVCACHE entry into LCache",
        KR(ret), K_(tenant_id), K(persist_entry), K(value));
  } else if (OB_FAIL(write_entry_list_.push(entry))) {
    LOG_WARN("fail to track write KVCACHE to LCache IO",
        KR(ret), KPC(entry));
    // 跟踪失败后放弃本次 best-effort L2 写。
  } else {
    // Entry 已转交给 write_entry_list_ 管理。
    entry = nullptr;
  }

  if (OB_NOT_NULL(entry)) {
    if (OB_TMP_FAIL(recycle_entry_(*entry))) {
      LOG_WARN_RET(tmp_ret,
          "fail to recycle write KVCACHE to LCache entry",
          KPC(entry));
    }
  }
  return ret;
}

int ObSSWriteKV2LCacheTask::handle_completed_entry()
{
  int ret = OB_SUCCESS;
  const int64_t count = write_entry_list_.get_curr_total();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObSSWriteKV2LCacheEntry *entry = nullptr;
    bool is_finished = false;
    bool need_recycle = true;
    if (OB_FAIL(write_entry_list_.pop(entry))) {
      LOG_WARN("fail to pop inflight write KVCACHE to LCache IO",
          KR(ret), K(i), K(count));
    } else if (OB_ISNULL(entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inflight write KVCACHE to LCache entry is null", KR(ret), K(i), K(count));
    } else {
      if (OB_FAIL(entry->write_handle_.check_is_finished(is_finished))) {
        LOG_WARN("fail to check write KVCACHE to LCache IO", KR(ret), KPC(entry));
        // 无法继续跟踪时放弃本次 best-effort L2 写。
      } else if (is_finished) {
        if (OB_FAIL(entry->write_handle_.wait())) {
          ATOMIC_INC(&io_fail_cnt_);
          LOG_WARN("write KVCACHE to LCache IO failed", KR(ret),
              K_(tenant_id), K(entry->macro_id_));
        } else {
          ATOMIC_INC(&success_cnt_);
          ATOMIC_AAF(&success_bytes_, entry->data_size_);
        }
      } else if (OB_FAIL(write_entry_list_.push(entry))) {
        LOG_WARN("fail to restore inflight write KVCACHE to LCache entry", KR(ret), KPC(entry));
        // 重新入队失败后放弃本次 best-effort L2 写。
      } else {
        need_recycle = false;
      }
    }

    if (need_recycle && OB_NOT_NULL(entry)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(recycle_entry_(*entry))) {
        LOG_WARN_RET(tmp_ret,
            "fail to recycle write KVCACHE to LCache IO entry",
            KPC(entry));
      }
    }
    ret = OB_SUCCESS;
  }
  return ret;
}

void ObSSWriteKV2LCacheTask::get_stat_snapshot(
    ObSSWriteKV2LCacheStat &stat) const
{
  stat.submit_fail_cnt_ = ATOMIC_LOAD(&submit_fail_cnt_);
  stat.success_cnt_ = ATOMIC_LOAD(&success_cnt_);
  stat.success_bytes_ = ATOMIC_LOAD(&success_bytes_);
  stat.io_fail_cnt_ = ATOMIC_LOAD(&io_fail_cnt_);
  stat.inflight_cnt_ = write_entry_list_.get_curr_total();
}

void ObSSWriteKV2LCacheTask::destroy()
{
  // 不等待在途 IO；释放 handle 后 IO 仍可能继续执行。
  write_entry_list_.destroy();
  free_entry_list_.destroy();
  if (OB_NOT_NULL(total_entry_arr_)) {
    for (int64_t i = 0; i < capacity_; ++i) {
      total_entry_arr_[i].~ObSSWriteKV2LCacheEntry();
    }
  }
  allocator_.reset();
  total_entry_arr_ = nullptr;
  capacity_ = 0;
  submit_fail_cnt_ = 0;
  success_cnt_ = 0;
  success_bytes_ = 0;
  io_fail_cnt_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase
