/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_DATA_DISK_AVAIL_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_DATA_DISK_AVAIL_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

/*
 * Unlike shared-nothing, which occupies disk space with large blockfile,
 * shared-storage occupies disk space with small files of 2MB,
 * so need check data disk available periodically
 */
class ObCheckDataDiskAvailTask : public common::ObTimerTask
{
public:
  ObCheckDataDiskAvailTask();
  virtual ~ObCheckDataDiskAvailTask() { destroy(); }
  int init(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int check_data_disk_avail();

private:
  bool is_inited_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_DATA_DISK_AVAIL_TASK_H_ */