/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_LOCAL_MAJOR_DATA_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_LOCAL_MAJOR_DATA_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{
/*
 * ObGCLocalMajorDataTask is used for deleting major_macro file when observer restart
 */
class ObGCLocalMajorDataTask : public common::ObTimerTask
{
public:
  ObGCLocalMajorDataTask();
  virtual ~ObGCLocalMajorDataTask() { destroy(); }

  int init(const uint64_t tenant_id);
  int start(const int tg_id);
  void destroy();

  virtual void runTimerTask() override;

private:
  static const int64_t INVALID_TG_ID = -1;
  static const int64_t DELAY_US = 100L * 1000L; // 100ms
  int gc_local_major_data();

private:
  bool is_inited_;
  uint64_t tenant_id_;
  int tg_id_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_GC_LOCAL_MAJOR_DATA_TASK_H_ */