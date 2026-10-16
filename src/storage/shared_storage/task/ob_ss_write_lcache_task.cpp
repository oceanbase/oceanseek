/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_ss_write_lcache_task.h"
#include "lib/thread/thread_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

ObSSWriteLCacheTask::ObSSWriteLCacheTask() : is_inited_(false), write_lcache_queue_()
{
}

int ObSSWriteLCacheTask::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("write lcache task is already inited", KR(ret));
  } else {
    is_inited_ = true;
  }
  LOG_INFO("finish to init write lcache task", KR(ret));
  return ret;
}

int ObSSWriteLCacheTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write lcache task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule write lcache task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("succ to start write lcache task", K(tg_id));
  }
  return ret;
}

void ObSSWriteLCacheTask::destroy()
{
  is_inited_ = false;
}

void ObSSWriteLCacheTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSSLocalCacheService *local_cache_service = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache service is nullptr", KR(ret));
  } else {
    const int64_t queue_size = write_lcache_queue_.size();
    for (int64_t i = 0; OB_SUCC(ret) && (i < queue_size); i++) {
      ObLink *ptr = nullptr;
      ObSSWriteLCacheEntry *write_lcache_entry = nullptr;
      if (OB_FAIL(write_lcache_queue_.pop(ptr))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to pop", KR(ret));
        }
      } else if (OB_ISNULL(ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ptr should not be null", KR(ret));
      } else {
        write_lcache_entry = static_cast<ObSSWriteLCacheEntry *>(ptr);
        bool is_finished = false;
        bool need_delete = true;
        if (OB_FAIL(write_lcache_entry->handle_.check_is_finished(is_finished))) {
          LOG_WARN("fail to check is finished", KR(ret));
        } else if (is_finished) {
          if (OB_FAIL(write_lcache_entry->handle_.wait())) {
            LOG_WARN("fail to wait", KR(ret));
          }
        } else { // !is_finished
          // push back to queue again, and do not delete ObSSWriteLCacheEntry
          if (OB_FAIL(write_lcache_queue_.push(write_lcache_entry))) {
            LOG_WARN("fail to push back to queue", KR(ret));
          } else {
            need_delete = false;
          }
        }
        if (need_delete) {
          OB_DELETE(ObSSWriteLCacheEntry, "WrLCacheEntry", write_lcache_entry);
        }
      }
      ret = OB_SUCCESS; // ignore ret, and go on process next entry
    }
  }
}

int ObSSWriteLCacheTask::push_write_lcache_queue(ObSSWriteLCacheEntry &entry)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("write lcache task is not init", KR(ret));
  } else if (OB_UNLIKELY(!entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid handle", KR(ret), K(entry));
  } else if (OB_FAIL(write_lcache_queue_.push(&entry))) {
    LOG_WARN("fail to push write lcache entry to queue", KR(ret), K(entry));
  }
  return ret;
}

void ObSSWriteLCacheTask::clear_write_lcache_queue()
{
  int ret = OB_SUCCESS;
  ObLink *ptr = nullptr;
  ObSSWriteLCacheEntry *write_lcache_entry = nullptr;
  bool is_completed = false;
  while (OB_SUCC(ret) && !is_completed) {
    if (OB_FAIL(write_lcache_queue_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        is_completed = true;
      } else {
        LOG_ERROR("fail to pop write lcache entry from queue", KR(ret));
      }
    } else {
      write_lcache_entry = static_cast<ObSSWriteLCacheEntry *>(ptr);
      OB_DELETE(ObSSWriteLCacheEntry, "WrLCacheEntry", write_lcache_entry);
    }
  }
}

} // storage
} // oceanbase
