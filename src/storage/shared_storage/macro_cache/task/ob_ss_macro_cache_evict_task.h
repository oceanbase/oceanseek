/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_EVICT_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_EVICT_TASK_H_

#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"

namespace oceanbase
{
namespace blocksstable
{
  class MacroBlockId;
}
namespace storage
{
class ObSSMacroCacheMgr;

class ObSSMacroCacheEvictTask : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSMacroCacheEvictTask(volatile bool &is_stopped);
  virtual ~ObSSMacroCacheEvictTask();
  int init(ObSSMacroCacheMgr *macro_cache_mgr);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

  static int64_t get_schedule_interval_us() { return SCHEDULE_INTERVAL_US; }

private:
  static const int INVALID_TG_ID = -1;
  static const int64_t SCHEDULE_INTERVAL_US = 10 * 1000L; // 10ms
  ObSSMacroCacheMgr *macro_cache_mgr_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_EVICT_TASK_H_ */
