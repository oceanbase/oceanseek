/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_TMP_SEQ_FILE_CLEAN_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_TMP_SEQ_FILE_CLEAN_TASK_H_
 
#include "lib/task/ob_timer.h"
 
namespace oceanbase
{
namespace storage
{
class ObTenantFileManager;
class ObTmpSeqFileCleanTask : public common::ObTimerTask
{
public:
  ObTmpSeqFileCleanTask();
  virtual ~ObTmpSeqFileCleanTask();
  int init();
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;
 
private:
  static const int INVALID_TG_ID = -1;
  static const int64_t SCHEDULE_INTERVAL_US = 10LL * 1000LL * 1000LL; // 10s
  bool is_inited_;
};
 
} /* namespace storage */
} /* namespace oceanbase */
 
#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_TMP_SEQ_FILE_CLEAN_TASK_H_ */

