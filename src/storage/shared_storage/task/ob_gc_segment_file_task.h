/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_SEGMENT_FILE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_SEGMENT_FILE_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

class ObSegmentFileManager;

/*
 * ObGCSegmentFileTask is used for async deleting unsealed segment file
 */
class ObGCSegmentFileTask : public common::ObTimerTask
{
public:
  ObGCSegmentFileTask();
  virtual ~ObGCSegmentFileTask() { destroy(); }

  int init(ObSegmentFileManager *segment_file_mgr);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask() override;

private:
  bool is_inited_;
  ObSegmentFileManager *segment_file_mgr_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_SEGMENT_FILE_TASK_H_ */