/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_macro_ring_buffer.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

void ObSSMacroPersistEntry::reset()
{
  macro_id_.reset();
  enqueue_ts_ = 0;
}

ObSSMacroRingBuffer::ObSSMacroRingBuffer()
  : is_inited_(false),
    accepting_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    entries_(nullptr),
    free_queue_(),
    ready_queue_(),
    stat_(),
    allocator_()
{
}

ObSSMacroRingBuffer::~ObSSMacroRingBuffer()
{
  destroy();
}

int ObSSMacroRingBuffer::init(
    const uint64_t tenant_id,
    const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("macro ring buffer is already inited", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || is_virtual_tenant_id(tenant_id)
      || capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(capacity));
  } else {
    allocator_.set_attr(ObMemAttr(tenant_id, "SSMacroRingBuf"));
    if (OB_ISNULL(entries_ = static_cast<ObSSMacroPersistEntry *>(
        allocator_.alloc(sizeof(ObSSMacroPersistEntry) * capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate macro ring buffer entries",
          KR(ret), K(tenant_id), K(capacity));
    } else {
      for (int64_t i = 0; i < capacity; ++i) {
        new (entries_ + i) ObSSMacroPersistEntry();
      }
      stat_.capacity_ = capacity;
      if (OB_FAIL(free_queue_.init(capacity, &allocator_,
          ObMemAttr(tenant_id, "SSMacroRBFree")))) {
        LOG_WARN("fail to init free queue", KR(ret), K(tenant_id), K(capacity));
      } else if (OB_FAIL(ready_queue_.init(capacity, &allocator_,
          ObMemAttr(tenant_id, "SSMacroRBReady")))) {
        LOG_WARN("fail to init ready queue", KR(ret), K(tenant_id), K(capacity));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < capacity; ++i) {
        if (OB_FAIL(free_queue_.push(entries_ + i))) {
          LOG_WARN("fail to seed macro ring buffer free entries",
              KR(ret), K(i), K(capacity));
        }
      }
    }
    if (OB_SUCC(ret)) {
      tenant_id_ = tenant_id;
      ATOMIC_STORE(&accepting_, true);
      is_inited_ = true;
    } else {
      destroy();
    }
  }
  return ret;
}

void ObSSMacroRingBuffer::stop_accepting()
{
  ATOMIC_STORE(&accepting_, false);
}

int ObSSMacroRingBuffer::recycle_entry_(ObSSMacroPersistEntry *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro ring buffer entry", KR(ret), KP(entry));
  } else {
    entry->reset();
    if (OB_FAIL(free_queue_.push(entry))) {
      LOG_WARN("fail to recycle macro ring buffer entry", KR(ret), KP(entry),
          "free", free_queue_.get_curr_total(),
          "ready", ready_queue_.get_curr_total());
    }
  }
  return ret;
}

int ObSSMacroRingBuffer::push_overwrite(
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObSSMacroPersistEntry *entry = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro ring buffer is not inited", KR(ret));
  } else if (OB_UNLIKELY(tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro ring buffer used in wrong tenant context",
        KR(ret), K_(tenant_id), "mtl_id", MTL_ID());
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", KR(ret), K(macro_id));
  } else if (OB_UNLIKELY(!ATOMIC_LOAD(&accepting_))) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("macro ring buffer has stopped accepting entries",
        KR(ret), K_(tenant_id), K(macro_id));
  } else if (OB_FAIL(free_queue_.pop(entry))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(ready_queue_.pop(entry))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_EAGAIN;
          LOG_WARN("no macro ring buffer entry is available",
              KR(ret), K_(tenant_id),
              "free", free_queue_.get_curr_total(),
              "ready", ready_queue_.get_curr_total());
        } else {
          LOG_WARN("fail to pop oldest macro ring buffer entry", KR(ret),
              "free", free_queue_.get_curr_total(),
              "ready", ready_queue_.get_curr_total());
        }
      } else {
        ATOMIC_INC(&stat_.overwrite_oldest_cnt_);
      }
    } else {
      LOG_WARN("fail to pop free macro ring buffer entry", KR(ret),
          "free", free_queue_.get_curr_total(),
          "ready", ready_queue_.get_curr_total());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro ring buffer entry is null", KR(ret), K_(tenant_id),
          "free", free_queue_.get_curr_total(),
          "ready", ready_queue_.get_curr_total());
    } else {
      entry->macro_id_ = macro_id;
      entry->enqueue_ts_ = ObTimeUtility::current_time();
      if (OB_FAIL(ready_queue_.push(entry))) {
        LOG_WARN("macro ring buffer invariant broken", KR(ret), KP(entry),
            "free", free_queue_.get_curr_total(),
            "ready", ready_queue_.get_curr_total());
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(recycle_entry_(entry))) {
          LOG_WARN("fail to recover macro ring buffer entry", KR(ret), K(tmp_ret), KP(entry));
        }
      } else {
        ATOMIC_INC(&stat_.enqueue_cnt_);
        // 入队不唤醒 worker，由定时检查发现新任务和水位变化，避免打断批次等待。
      }
    }
  }

  return ret;
}

int ObSSMacroRingBuffer::try_pop(ObSSMacroPersistEntry &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroPersistEntry *queue_entry = nullptr;
  entry.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro ring buffer is not inited", KR(ret));
  } else if (OB_UNLIKELY(tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro ring buffer used in wrong tenant context",
        KR(ret), K_(tenant_id), "mtl_id", MTL_ID());
  } else if (OB_FAIL(ready_queue_.pop(queue_entry))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to pop ready macro ring buffer entry", KR(ret),
          "free", free_queue_.get_curr_total(),
          "ready", ready_queue_.get_curr_total());
    }
  } else if (OB_ISNULL(queue_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ready macro ring buffer entry is null", KR(ret),
        "free", free_queue_.get_curr_total(),
        "ready", ready_queue_.get_curr_total());
  } else {
    entry = *queue_entry;
    if (OB_FAIL(recycle_entry_(queue_entry))) {
      LOG_WARN("fail to recycle popped macro ring buffer entry",
          KR(ret), K_(tenant_id), K(entry));
    }
  }
  return ret;
}

int ObSSMacroRingBuffer::discard_all()
{
  int ret = OB_SUCCESS;
  ObSSMacroPersistEntry entry;
  while (OB_SUCC(try_pop(entry))) {
    // empty
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("fail to discard macro ring buffer entries", KR(ret),
        K_(tenant_id), "ready", ready_queue_.get_curr_total());
  }
  return ret;
}

void ObSSMacroRingBuffer::get_stat_snapshot(
    ObSSMacroRingBufferStat &stat) const
{
  stat.capacity_ = stat_.capacity_;
  stat.enqueue_cnt_ = ATOMIC_LOAD(&stat_.enqueue_cnt_);
  stat.overwrite_oldest_cnt_ = ATOMIC_LOAD(&stat_.overwrite_oldest_cnt_);
  stat.queue_current_ = ready_queue_.get_curr_total();
}

void ObSSMacroRingBuffer::destroy()
{
  ATOMIC_STORE(&accepting_, false);
  if (OB_UNLIKELY(is_inited_ && ready_queue_.get_curr_total() != 0)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED,
        "destroy nonempty macro ring buffer", K_(tenant_id),
        "ready", ready_queue_.get_curr_total(),
        "free", free_queue_.get_curr_total());
  }
  ready_queue_.destroy();
  free_queue_.destroy();
  if (OB_NOT_NULL(entries_)) {
    for (int64_t i = 0; i < stat_.capacity_; ++i) {
      entries_[i].~ObSSMacroPersistEntry();
    }
  }
  allocator_.reset();
  entries_ = nullptr;
  stat_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

} // namespace storage
} // namespace oceanbase
