/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_REPLAY_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_REPLAY_TASK_H_

#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"

namespace oceanbase
{
namespace storage
{
class ObSSMacroCacheMgr;
class ObSSMacroCache;

class ObSSMacroCacheReplayTask : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSMacroCacheReplayTask(volatile bool &is_stopped);
  virtual ~ObSSMacroCacheReplayTask();
  int init(ObSSMacroCacheMgr *macro_cache_mgr);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  ObSSMacroCacheMgr *macro_cache_mgr_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_REPLAY_TASK_H_ */
