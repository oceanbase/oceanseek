/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_IO_DETECT_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_IO_DETECT_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

class ObTenantIODetectTask : public common::ObTimerTask
{
public:
  ObTenantIODetectTask();
  virtual ~ObTenantIODetectTask() { destroy(); }
  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int64_t INVALID_TG_ID = -1;
  bool is_inited_;
  uint64_t tenant_id_;
  int tg_id_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_IO_DETECT_TASK_H_ */
