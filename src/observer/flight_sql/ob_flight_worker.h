/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_WORKER_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_WORKER_H_

#include "lib/worker.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "sql/executor/ob_memory_tracker.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace observer
{

// gRPC threads do not run under the tenant scheduler. They still execute SQL
// and wait for remote-SQL RPCs, whose wait loop polls THIS_WORKER.check_status().
// Give Flight requests the session/timeout checks that those waits require.
class ObFlightWorker final : public lib::Worker
{
public:
  int check_status() override
  {
    int ret = common::OB_SUCCESS;
    // Preserve the session interruption error before considering timeouts:
    // the remote RPC wait uses that error to terminate its pending request.
    if (session_ != nullptr) { session_->is_terminate(ret); }
    if (OB_SUCC(ret) && common::OB_SUCCESS != (ret = CHECK_MEM_STATUS())) {
    } else if (OB_SUCC(ret) && is_timeout()) {
      ret = common::OB_TIMEOUT;
    } else if (OB_SUCC(ret) && common::IS_INTERRUPTED()) {
      ret = common::GET_INTERRUPT_CODE().code_;
    } else if (OB_SUCC(ret) && WS_OUT_OF_THROTTLE == check_wait()) {
      ret = common::OB_KILLED_BY_THROTTLING;
    }
    return ret;
  }
};

// Install at the DoPut handler entry, BEFORE timeout/session/cgroup guards.
// Start fresh request state rather than copying a partly configured Worker.
// Declaring this guard first keeps it alive until all SQL and request cleanup
// has finished. Nested engine calls use this same worker and its timeout.
class ObFlightWorkerGuard final
{
public:
  ObFlightWorkerGuard()
      : previous_(lib::Worker::self_),
        installed_(previous_ == nullptr || !previous_->is_th_worker())
  {
    if (installed_) { lib::Worker::set_worker_to_thread_local(&worker_); }
  }
  ~ObFlightWorkerGuard()
  {
    // Restore TLS before worker_'s destructor, which otherwise clears it.
    if (installed_) { lib::Worker::set_worker_to_thread_local(previous_); }
  }
private:
  lib::Worker *previous_;
  ObFlightWorker worker_;
  bool installed_;
  DISALLOW_COPY_AND_ASSIGN(ObFlightWorkerGuard);
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHT_WORKER_H_
