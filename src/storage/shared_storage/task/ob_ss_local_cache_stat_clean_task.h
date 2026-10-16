/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_LOCAL_CACHE_STAT_CLEAN_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_LOCAL_CACHE_STAT_CLEAN_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{
class ObSSLocalCacheService;
class ObSSLocalCacheStatCleanTask : public common::ObTimerTask
{
public:
  explicit ObSSLocalCacheStatCleanTask();
  virtual ~ObSSLocalCacheStatCleanTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int INVALID_TG_ID = -1;
  static const int64_t SCHEDULE_INTERVAL_US = 24LL * 3600LL * 1000LL * 1000LL; // 24h
  bool is_inited_;
  uint64_t tenant_id_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_LOCAL_CACHE_STAT_CLEAN_TASK_H_ */
