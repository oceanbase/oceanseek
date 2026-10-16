/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SS_LOCAL_CACHE_PARAM_REFRESH_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SS_LOCAL_CACHE_PARAM_REFRESH_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

class ObSSLocalCacheParamRefreshTask : public common::ObTimerTask
{
public:
  ObSSLocalCacheParamRefreshTask();
  virtual ~ObSSLocalCacheParamRefreshTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int64_t SCHEDULE_INTERVAL_US = 1 * 1000 * 1000L; // 1s
  static const int INVALID_TG_ID = -1;
  bool is_inited_;
  uint64_t tenant_id_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SS_LOCAL_CACHE_PARAM_REFRESH_TASK_H_ */