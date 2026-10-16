/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_HANDLER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_HANDLER_H_

#include "lib/atomic/ob_atomic.h"
#include "lib/thread/thread_mgr_interface.h"
#include "share/vector_index/spfresh/ob_spfresh_task_queue.h"
#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace share
{

class ObSpfreshTaskExecutor;

// Asynchronous handler for SPFresh SPLIT/MERGE/REASSIGN tasks.
// Tasks are pushed by ObSpfreshTaskExecutor::process_queue() (timer thread)
// and executed in a dedicated tenant thread pool (an independent instance
// created from lib::TGDefIDs::VectorTaskPool, same pattern as ObEmbeddingTaskHandler),
// so the timer thread is never blocked by long-running split/merge/reassign.
// The thread pool is created lazily on the first real task dispatch.
class ObSpfreshTaskHandler : public lib::TGTaskHandler
{
public:
  ObSpfreshTaskHandler();
  virtual ~ObSpfreshTaskHandler();

  // Bind tenant/executor. Does not create threads.
  int init(const uint64_t tenant_id, ObSpfreshTaskExecutor *executor);

  // Create thread group, set adaptive thread count and start the pool.
  int start();

  bool is_started() const { return started_; }

  // Stop accepting/handling new tasks. Idempotent.
  void stop();

  // Block until all in-flight handle() calls finish.
  void wait();

  // Release thread group resources. Must be called after wait().
  void destroy();

  // Atomically reserve and submit a task; the thread pool releases its copy.
  int push_task(const ObSpfreshTask &task, int64_t max_in_flight);
  // Retry publication once per timer round; retain ownership on failure.
  void retry_pending_tasks();

  // Return the number of reserved tasks that have not completed.
  int64_t get_in_flight_count() const { return ATOMIC_LOAD(&in_flight_count_); }

  virtual void handle(void *task) override;

  // Called by TG for tasks still queued when the thread pool stops: release
  // the task copy and in-flight bookkeeping WITHOUT executing it (executing
  // would touch service/storage components that are already stopping).
  virtual void handle_drop(void *task) override;

private:
  class PendingTask : public ObSpfreshTask
  {
  public:
    explicit PendingTask(const ObSpfreshTask &task) : ObSpfreshTask(task), next_(nullptr) {}
    PendingTask *next_;
  };
  PendingTask *detach_pending_tasks_();
  // Serialize retry ownership and executor_ lifetime with destroy. Workers
  // only take pending_lock_, never this mutex.
  lib::ObMutex pending_retry_lock_;
  common::ObSpinLock pending_lock_;
  PendingTask *pending_tasks_ = nullptr;

  // Reserve an in-flight slot with CAS when the limit has not been reached.
  int reserve_in_flight_slot_(int64_t max_in_flight);

  class InFlightGuard
  {
  public:
    explicit InFlightGuard(volatile int64_t *counter);
    virtual ~InFlightGuard();
  private:
    volatile int64_t *counter_;
    DISALLOW_COPY_AND_ASSIGN(InFlightGuard);
  };

  static constexpr int64_t INVALID_TG_ID = -1;
  static constexpr int64_t MIN_THREAD_COUNT = 2;
  static constexpr int64_t MAX_THREAD_COUNT = 12;

  bool is_inited_;
  int tg_id_;
  uint64_t tenant_id_;
  ObSpfreshTaskExecutor *executor_;
  volatile int64_t in_flight_count_;
  bool stopped_;
  bool started_;

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshTaskHandler);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TASK_HANDLER_H_
