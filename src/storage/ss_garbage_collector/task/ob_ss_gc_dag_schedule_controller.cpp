/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/ss_garbage_collector/task/ob_ss_gc_dag_schedule_controller.h"

namespace oceanbase
{
namespace storage
{

ObSSGCDagScheduleController::ObSSGCDagScheduleController()
  : state_(State::WAIT_FOR_NEXT_SCHEDULE)
{
}

void ObSSGCDagScheduleController::reset()
{
  state_ = State::WAIT_FOR_NEXT_SCHEDULE;
}

ObSSGCDagScheduleController::State ObSSGCDagScheduleController::state() const
{
  return state_;
}

int ObSSGCDagScheduleController::switch_to_wait_for_next_schedule()
{
  int ret = common::OB_SUCCESS;
  if (State::WAIT_FOR_DAG_FINISH != state_) {
    ret = common::OB_STATE_NOT_MATCH;
  } else {
    state_ = State::WAIT_FOR_NEXT_SCHEDULE;
  }
  return ret;
}

int ObSSGCDagScheduleController::switch_to_wait_for_dag_finish()
{
  int ret = common::OB_SUCCESS;
  if (State::WAIT_FOR_NEXT_SCHEDULE != state_) {
    ret = common::OB_STATE_NOT_MATCH;
  } else {
    state_ = State::WAIT_FOR_DAG_FINISH;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
