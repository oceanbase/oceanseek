/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_mutex.h"
#include "lib/utility/ob_print_utils.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"

namespace oceanbase
{
namespace observer
{

enum class ObDirectLoadStreamBufferQueueManagerState
{
  UNINIT = 0,
  RUNNING,
  STOPPING,
};

class ObDirectLoadStreamBufferQueueManager
{
public:
  explicit ObDirectLoadStreamBufferQueueManager(const uint64_t tenant_id);
  ~ObDirectLoadStreamBufferQueueManager();
  int init();
  // 仅在 tenant ObDirectLoadStreamService 最终销毁阶段调用。
  // 调用前 worker 和模块线程已完成 stop/wait，且所有 queue/append handle 已释放。
  // manager 已静默，生产路径不应并发调用；残留 cleanup marker 由 destroy 接管。
  // queue cleanup 失败仅记录错误，destroy 仍完成最终资源释放。
  void destroy();
  bool is_inited() const;
  // focused 单测必须在 init 前设置；生产路径禁止调用本接口。
  int set_mock_queue_store_factory_for_test(
      ObIDirectLoadStreamQueueStoreFactory &mock_store_factory);
  int create_queue(const ObDirectLoadStreamBufferQueueKey &key,
                   ObDirectLoadStreamBufferQueueHandle &handle);
  int get_queue(const ObDirectLoadStreamBufferQueueKey &key,
                ObDirectLoadStreamBufferQueueHandle &handle);
  int seal_queue(const ObDirectLoadStreamBufferQueueKey &key);
  int cleanup_queue(const ObDirectLoadStreamBufferQueueKey &key);
  TO_STRING_KV(K_(tenant_id));
private:
  typedef common::hash::ObHashMap<ObDirectLoadStreamBufferQueueKey,
                                  ObDirectLoadStreamBufferQueue *> QueueMap;
  typedef common::hash::ObHashSet<ObDirectLoadStreamBufferQueueKey> QueueSet;
private:
  const uint64_t tenant_id_;
  mutable lib::ObMutex mutex_; // 保护 state_ 转换和 RUNNING 阶段的 map/set。
  // RUNNING 时由 mutex_ 保护，STOPPING 后由 destroy 独占；manager 持有 queue 所有权。
  QueueMap queue_map_;
  // 非 owning 的 cleanup key marker；同步规则同 queue_map_。
  QueueSet cleaning_queue_set_;
  // 由 mutex_ 保护；只有 RUNNING 才允许公开 queue 操作。
  ObDirectLoadStreamBufferQueueManagerState state_;
  // 仅供测试且不持有所有权；factory 生命周期必须覆盖 manager 及其全部 queue。
  ObIDirectLoadStreamQueueStoreFactory *mock_store_factory_;
};

}  // namespace observer
}  // namespace oceanbase
