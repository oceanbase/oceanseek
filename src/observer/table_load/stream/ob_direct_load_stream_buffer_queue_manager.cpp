/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue_manager.h"

namespace oceanbase
{
namespace observer
{

ObDirectLoadStreamBufferQueueManager::ObDirectLoadStreamBufferQueueManager(
    const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    mutex_(common::ObLatchIds::DIRECT_LOAD_STREAM_QUEUE_MGR_LOCK),
    queue_map_(),
    cleaning_queue_set_(),
    state_(ObDirectLoadStreamBufferQueueManagerState::UNINIT),
    mock_store_factory_(nullptr)
{
}

ObDirectLoadStreamBufferQueueManager::~ObDirectLoadStreamBufferQueueManager()
{
  destroy();
}

int ObDirectLoadStreamBufferQueueManager::init()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueManagerState::UNINIT != state_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "stream buffer queue manager init twice", KR(ret));
  } else if (OB_UNLIKELY(!common::is_user_tenant(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream buffer queue manager tenant", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(queue_map_.create(1024, "DLSBQMap", "DLSBQMap", tenant_id_))) {
    OB_LOG(WARN, "fail to create stream buffer queue map", KR(ret));
  } else if (OB_FAIL(cleaning_queue_set_.create(
                 1024, "DLSBQCln", "DLSBQCln", tenant_id_))) {
    queue_map_.destroy();
    OB_LOG(WARN, "fail to create cleaning stream buffer queue set", KR(ret));
  } else {
    state_ = ObDirectLoadStreamBufferQueueManagerState::RUNNING;
  }
  return ret;
}

void ObDirectLoadStreamBufferQueueManager::destroy()
{
  {
    lib::ObMutexGuard guard(mutex_);
    if (ObDirectLoadStreamBufferQueueManagerState::UNINIT == state_
        || ObDirectLoadStreamBufferQueueManagerState::STOPPING == state_) {
      return;
    }
    state_ = ObDirectLoadStreamBufferQueueManagerState::STOPPING;
  }
  // 调用方必须先 stop/wait 所有生产线程，并释放所有 queue/append handle。
  // 满足该前置条件后，destroy 独占 map/set，一次性完成最终资源释放。
  for (QueueMap::iterator iter = queue_map_.begin(); queue_map_.end() != iter; ++iter) {
    ObDirectLoadStreamBufferQueue *queue = iter->second;
    if (OB_ISNULL(queue)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED,
                   "unexpected null stream buffer queue during manager destroy",
                   K(iter->first));
    } else {
      const int cleanup_ret = queue->cleanup();
      if (OB_SUCCESS != cleanup_ret) {
        // 最终销毁已完成静默，cleanup 错误不能再交给不存在的上层重试路径。
        LOG_WARN_RET(cleanup_ret,
                     "fail to cleanup stream buffer queue during final manager destroy",
                     KR(cleanup_ret), K(iter->first), KP(queue));
      }
      OB_DELETE(ObDirectLoadStreamBufferQueue,
                lib::ObMemAttr(tenant_id_, "DLSBQ"), queue);
    }
  }
  {
    lib::ObMutexGuard guard(mutex_);
    queue_map_.destroy();
    // cleanup set 不持有 queue 所有权。
    cleaning_queue_set_.destroy();
    state_ = ObDirectLoadStreamBufferQueueManagerState::UNINIT;
  }
}

bool ObDirectLoadStreamBufferQueueManager::is_inited() const
{
  lib::ObMutexGuard guard(mutex_);
  return ObDirectLoadStreamBufferQueueManagerState::UNINIT != state_;
}

int ObDirectLoadStreamBufferQueueManager::set_mock_queue_store_factory_for_test(
    ObIDirectLoadStreamQueueStoreFactory &mock_store_factory)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueManagerState::UNINIT != state_)) {
    ret = OB_STATE_NOT_MATCH;
    OB_LOG(WARN, "cannot set mock stream queue store factory after manager init", KR(ret));
  } else if (OB_NOT_NULL(mock_store_factory_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "mock stream queue store factory is already set", KR(ret));
  } else {
    mock_store_factory_ = &mock_store_factory;
  }
  return ret;
}

int ObDirectLoadStreamBufferQueueManager::create_queue(const ObDirectLoadStreamBufferQueueKey &key,
                                                       ObDirectLoadStreamBufferQueueHandle &handle)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  handle.reset();
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueManagerState::UNINIT == state_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueManagerState::STOPPING == state_)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream buffer queue key", KR(ret), K(key));
  } else if (OB_SUCCESS == (ret = queue_map_.get_refactored(key, queue))) {
    if (OB_ISNULL(queue)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected null stream buffer queue", KR(ret), K(key));
    } else {
      // 重复 create 明确报错，不返回已有 handle，避免隐式增加 ref。
      queue = nullptr;
      ret = OB_HASH_EXIST;
      OB_LOG(WARN, "stream buffer queue already exists", KR(ret), K(key));
    }
  } else if (OB_HASH_NOT_EXIST != ret) {
    OB_LOG(WARN, "fail to get stream buffer queue before create", KR(ret), K(key));
  } else {
    ret = OB_SUCCESS;
    if (OB_ISNULL(queue = OB_NEW(ObDirectLoadStreamBufferQueue,
                                 lib::ObMemAttr(tenant_id_, "DLSBQ")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc stream buffer queue", KR(ret), K(key));
    } else {
      if (OB_ISNULL(mock_store_factory_)) {
        if (OB_FAIL(queue->init(tenant_id_, key))) {
          OB_LOG(WARN, "fail to init stream buffer queue", KR(ret), K(key));
        }
      } else {
        ObIDirectLoadStreamQueueStore *mock_store = nullptr;
        if (OB_FAIL(mock_store_factory_->create_store(mock_store))) {
          OB_LOG(WARN, "fail to create mock stream queue store", KR(ret), K(key));
        } else if (OB_ISNULL(mock_store)) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "mock stream queue store factory returned null", KR(ret), K(key));
        } else if (OB_FAIL(queue->init_with_mock_store_for_test(tenant_id_, key, *mock_store))) {
          OB_LOG(WARN, "fail to init stream buffer queue with mock store", KR(ret), K(key));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(handle.set_queue(queue))) {
        OB_LOG(WARN, "fail to set stream buffer queue handle", KR(ret), K(key));
      } else if (OB_SUCC(ret) && OB_FAIL(queue_map_.set_refactored(key, queue))) {
        OB_LOG(WARN, "fail to set stream buffer queue", KR(ret), K(key));
      }
    }
  }
  if (OB_FAIL(ret) && nullptr != queue) {
    handle.reset();
    OB_DELETE(ObDirectLoadStreamBufferQueue, lib::ObMemAttr(tenant_id_, "DLSBQ"), queue);
  }
  return ret;
}

int ObDirectLoadStreamBufferQueueManager::get_queue(const ObDirectLoadStreamBufferQueueKey &key,
                                                    ObDirectLoadStreamBufferQueueHandle &handle)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  handle.reset();
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueManagerState::UNINIT == state_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueManagerState::STOPPING == state_)) {
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid stream buffer queue key", KR(ret), K(key));
  } else if (OB_HASH_EXIST == (ret = cleaning_queue_set_.exist_refactored(key))) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_HASH_NOT_EXIST != ret) {
    OB_LOG(WARN, "fail to check cleaning stream buffer queue set", KR(ret), K(key));
  } else if (OB_FAIL(queue_map_.get_refactored(key, queue))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      OB_LOG(WARN, "fail to get stream buffer queue", KR(ret), K(key));
    }
  } else if (OB_ISNULL(queue)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unexpected null stream buffer queue", KR(ret), K(key));
  } else if (OB_FAIL(handle.set_queue(queue))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      OB_LOG(WARN, "fail to set stream buffer queue handle", KR(ret), K(key));
    }
  }
  return ret;
}

int ObDirectLoadStreamBufferQueueManager::seal_queue(const ObDirectLoadStreamBufferQueueKey &key)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamBufferQueueHandle handle;
  if (OB_FAIL(get_queue(key, handle))) {
    if (OB_INVALID_ARGUMENT != ret) {
      OB_LOG(WARN, "fail to get stream buffer queue for seal", KR(ret), K(key));
    }
  } else if (OB_FAIL(handle.get_queue()->request_close())) {
    OB_LOG(WARN, "fail to seal stream buffer queue", KR(ret), K(key));
  }
  return ret;
}

int ObDirectLoadStreamBufferQueueManager::cleanup_queue(const ObDirectLoadStreamBufferQueueKey &key)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  ObDirectLoadStreamBufferQueue *erased_queue = nullptr;
  {
    lib::ObMutexGuard guard(mutex_);
    if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueManagerState::UNINIT == state_)) {
      ret = OB_NOT_INIT;
    } else if (OB_UNLIKELY(ObDirectLoadStreamBufferQueueManagerState::STOPPING == state_)) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_UNLIKELY(!key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid stream buffer queue key", KR(ret), K(key));
    } else if (OB_HASH_EXIST == (ret = cleaning_queue_set_.exist_refactored(key))) {
      ret = OB_EAGAIN;
      OB_LOG(INFO, "stream buffer queue cleanup is already in progress", KR(ret), K(key));
    } else if (OB_HASH_NOT_EXIST != ret) {
      OB_LOG(WARN, "fail to check cleaning stream buffer queue set", KR(ret), K(key));
    } else if (OB_FAIL(queue_map_.get_refactored(key, queue))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // cleanup RPC/GC 可能重试，queue 不存在视为已清理。
        ret = OB_SUCCESS;
      } else {
        OB_LOG(WARN, "fail to get stream buffer queue for cleanup", KR(ret), K(key));
      }
    } else if (OB_ISNULL(queue)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected null stream buffer queue for cleanup", KR(ret), K(key));
    } else if (OB_FAIL(cleaning_queue_set_.set_refactored(key, 0))) {
      OB_LOG(WARN, "fail to mark cleaning stream buffer queue", KR(ret), K(key));
    }
  }
  if (OB_SUCC(ret) && nullptr != queue) {
    const int cleanup_ret = queue->cleanup();
    {
      lib::ObMutexGuard guard(mutex_);
      const int erase_marker_ret = cleaning_queue_set_.erase_refactored(key);
      if (OB_SUCCESS != erase_marker_ret && OB_HASH_NOT_EXIST != erase_marker_ret) {
        ret = erase_marker_ret;
        OB_LOG(WARN, "fail to erase cleaning stream buffer queue marker", KR(ret), K(key));
      }
      if (OB_SUCCESS != cleanup_ret) {
        ret = cleanup_ret;
        // cleanup 失败时保留 map 项，不能让未释放对象从 manager 视角消失。
        OB_LOG(WARN, "fail to cleanup stream buffer queue", KR(ret), K(key), KP(queue));
      } else if (OB_SUCC(ret) && OB_FAIL(queue_map_.erase_refactored(key, &erased_queue))) {
        OB_LOG(WARN, "fail to erase stream buffer queue after cleanup", KR(ret), K(key));
      } else if (OB_SUCC(ret) && OB_UNLIKELY(erased_queue != queue)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "unexpected erased stream buffer queue", KR(ret), K(key), KP(queue),
               KP(erased_queue));
      }
    }
  }
  if (OB_SUCC(ret) && nullptr != queue) {
    OB_DELETE(ObDirectLoadStreamBufferQueue, lib::ObMemAttr(tenant_id_, "DLSBQ"), queue);
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
