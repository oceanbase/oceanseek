/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_PREWARM_MAP_CLEAN_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_PREWARM_MAP_CLEAN_TASK_H_

#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
}
namespace storage
{
class ObSSLocalCacheService;

class ObSSPrewarmMapCleanTask : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSPrewarmMapCleanTask(volatile bool &is_stopped);
  virtual ~ObSSPrewarmMapCleanTask();
  int init(ObSSLocalCacheService *local_cache_service);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int INVALID_TG_ID = -1;
  static const int64_t SCHEDULE_INTERVAL_US = 600 * 1000L * 1000L; // 10min
  ObSSLocalCacheService *local_cache_service_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_PREWARM_MAP_CLEAN_TASK_H_ */
