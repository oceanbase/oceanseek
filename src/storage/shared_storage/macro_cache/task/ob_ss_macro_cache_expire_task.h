/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_EXPIRE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_EXPIRE_TASK_H_

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

class ObSSMacroCacheExpireTask : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSMacroCacheExpireTask(volatile bool &is_stopped);
  virtual ~ObSSMacroCacheExpireTask();
  int init(ObSSMacroCacheMgr *macro_cache_mgr);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int check_if_need_do_expire_work(bool &is_need);

private:
  static const int INVALID_TG_ID = -1;
  static const int64_t SCHEDULE_INTERVAL_US = 30 * 1000L * 1000L; // 30s
  static const int64_t MAX_EXEC_INTERVAL_US = 600 * 1000L * 1000L; // 10min
  ObSSMacroCacheMgr *macro_cache_mgr_;
  int64_t last_exec_timestamp_us_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_EXPIRE_TASK_H_ */
