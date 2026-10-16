/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SS_WRITE_LCACHE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SS_WRITE_LCACHE_TASK_H_

#include "lib/task/ob_timer.h"
#include "lib/queue/ob_link_queue.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
namespace oceanbase
{
namespace storage
{

class ObSSWriteLCacheEntry final : public common::ObLink
{
public:
  ObSSWriteLCacheEntry() : handle_() {}
  ~ObSSWriteLCacheEntry() {}
  bool is_valid() const { return handle_.is_valid(); }
  TO_STRING_KV(K_(handle));

public:
  blocksstable::ObStorageObjectHandle handle_;
};

class ObSSWriteLCacheTask : public common::ObTimerTask
{
public:
  ObSSWriteLCacheTask();
  virtual ~ObSSWriteLCacheTask() { destroy(); }
  int init();
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;
  int push_write_lcache_queue(ObSSWriteLCacheEntry &entry);
  void clear_write_lcache_queue();

private:
  static const int64_t SCHEDULE_INTERVAL_US = 10 * 1000L; // 10ms
  static const int INVALID_TG_ID = -1;
  bool is_inited_;
  common::ObLinkQueue write_lcache_queue_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SS_WRITE_LCACHE_TASK_H_ */
