/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MC_PREWARM_LEVEL_REFRESH_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MC_PREWARM_LEVEL_REFRESH_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

class ObMCPrewarmLevelRefreshTask : public common::ObTimerTask
{
public:
  ObMCPrewarmLevelRefreshTask();
  virtual ~ObMCPrewarmLevelRefreshTask();
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int64_t SCHEDULE_INTERVAL_US = 1 * 60 * 1000 * 1000L; // 1min
  bool is_inited_;
  uint64_t tenant_id_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MC_PREWARM_LEVEL_REFRESH_TASK_H_ */