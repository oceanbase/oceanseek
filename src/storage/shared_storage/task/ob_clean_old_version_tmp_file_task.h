/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CLEAN_OLD_VERSION_TMP_FILE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CLEAN_OLD_VERSION_TMP_FILE_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

class ObSegmentFileManager;

/*
 * ObCleanOldVersionTmpFileTask is used for cleaning old version tmp files once after reboot.
 * It will retry on failure until success, then stop automatically.
 */
class ObCleanOldVersionTmpFileTask : public common::ObTimerTask
{
public:
  ObCleanOldVersionTmpFileTask();
  virtual ~ObCleanOldVersionTmpFileTask() { destroy(); }

  int init(ObSegmentFileManager *segment_file_mgr);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int64_t INTERVAL_US = 10L * 1000L * 1000L; // 10s

private:
  bool is_inited_;
  int64_t retry_count_;  // retry times for cleaning old version tmp files
  ObSegmentFileManager *segment_file_mgr_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CLEAN_OLD_VERSION_TMP_FILE_TASK_H_ */

