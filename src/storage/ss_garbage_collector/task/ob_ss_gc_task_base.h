/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_TASK_BASE_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_TASK_BASE_H_

#include "lib/ob_define.h"
#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace storage
{

class ObSSGCTaskBase : public common::ObTimerTask
{
public:
  explicit ObSSGCTaskBase(volatile bool &is_stopped);
  virtual ~ObSSGCTaskBase();
  virtual void stop();

protected:
  static const int INVALID_TG_ID = -1;
  static const int64_t DEFAULT_GC_EXEC_INTERVAL_US = 24LL * 3600LL * 1000LL * 1000LL; // 1d

protected:
  int check_init_and_stop_() const;
  int start_task_(const int tg_id, const int64_t interval_us);
  int schedule_task_(common::ObTimerTask &task,
                     const int tg_id,
                     const int64_t interval_us,
                     const bool repeat);
  void reset_task_state_();
  int check_enable_gc_service_(bool &enable_gc_service) const;
  int check_is_gc_sswriter_(bool &is_gc_sswriter) const;
  virtual int check_if_need_do_gc_work(bool &is_need);
  virtual void get_gc_exec_interval_us_(int64_t &exec_interval_us) const;

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  volatile bool &is_stopped_;
  uint64_t run_cnt_;
  int64_t last_exec_time_us_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_TASK_BASE_H_
