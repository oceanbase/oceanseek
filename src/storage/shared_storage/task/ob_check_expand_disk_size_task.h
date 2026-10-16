/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_NEED_EXPAND_DISK_SIZE_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_NEED_EXPAND_DISK_SIZE_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{
/*
 * ObCheckExpandDiskTask is to check whether the tenant needs to expand data disk size
 */
class ObCheckExpandDiskTask : public common::ObTimerTask
{
public:
  ObCheckExpandDiskTask();
  virtual ~ObCheckExpandDiskTask() { destroy(); }
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int64_t INVALID_TG_ID = -1;
  int check_expand_disk_size_() const;
  int check_shrink_disk_size_() const;
  int sync_tenant_data_disk_size_() const;
  int sync_sys_tenant_data_disk_size_() const;


private:
  bool is_inited_;
  uint64_t tenant_id_;
  int tg_id_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_CHECK_NEED_EXPAND_DISK_SIZE_TASK_H_ */