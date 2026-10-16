/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_DAG_SCHEDULE_CONTROLLER_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_DAG_SCHEDULE_CONTROLLER_H_

#include "lib/ob_errno.h"

namespace oceanbase
{
namespace storage
{

class ObSSGCDagScheduleController final
{
public:
  enum class State
  {
    WAIT_FOR_NEXT_SCHEDULE,
    WAIT_FOR_DAG_FINISH
  };

  ObSSGCDagScheduleController();
  void reset();
  State state() const;
  int switch_to_wait_for_next_schedule();
  int switch_to_wait_for_dag_finish();

private:
  State state_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_TASK_OB_SS_GC_DAG_SCHEDULE_CONTROLLER_H_
