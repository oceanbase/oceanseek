/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef _OCEANBASE_OBSERVER_OMT_OB_PX_POOL_H_
#define _OCEANBASE_OBSERVER_OMT_OB_PX_POOL_H_

#include <functional>
#include "lib/allocator/ob_malloc.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/ob_define.h"
#include "lib/queue/ob_priority_queue.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "lib/worker.h"
#include "share/ob_thread_pool.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace omt
{
class ObPxPool
    : public share::ObThreadPool
{
  using RunFuncT = std::function<void (bool)>;
  void run(int64_t idx) final;
  void run1() final;
  static const int64_t QUEUE_WAIT_TIME = 100 * 1000;

public:
	class Task;
  ObPxPool() :
      tenant_id_(common::OB_INVALID_ID),
      group_id_(0),
      is_inited_(false),
      concurrency_(0),
      active_threads_(0),
      enable_database_isolation_mode_(false),
      is_expanding_(false),
      recycle_lock_(common::ObLatchIds::OB_PX_POOL_RECYCLE_LOCK)
  {}
  virtual void stop();
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_group_id(uint64_t group_id)
  {
    GET_DIAGNOSTIC_INFO->get_ash_stat().group_id_ = THIS_WORKER.get_group_id();
    group_id_ = group_id;
  }
  int64_t get_pool_size() const { return get_thread_count(); }
  int expand_thread_count(int64_t task_count);
  int submit(const RunFuncT &func);
  void set_px_thread_name();
  int64_t get_queue_size() const { return queue_.size(); }
  void set_enable_database_isolation_mode(bool mode) { enable_database_isolation_mode_ = mode; }
private:
  void apply_px_worker_thread_sched_priority();
  void handle(common::ObLink *task);
  void try_recycle(int64_t idle_time);
  void disable_recycle()
  {
    recycle_lock_.lock();
  }
  void enable_recycle()
  {
    IGNORE_RETURN recycle_lock_.unlock();
  }
private:
  uint64_t tenant_id_;
  uint64_t group_id_;
	common::ObPriorityQueue2<0, 1> queue_;
  bool is_inited_;
  int64_t concurrency_;
  int64_t active_threads_;
  bool enable_database_isolation_mode_;
  bool is_expanding_;
  mutable common::ObSpinLock recycle_lock_;
};

class ObPxPool::Task : public common::ObLink
{
public:
  Task(const RunFuncT &func)
      : func_(func)
  {}
public:
  RunFuncT func_;
};


class ObPxPools
{
public:
  class StopPoolFunc
  {
  public:
    StopPoolFunc() {}
    virtual ~StopPoolFunc() = default;
    int operator()(common::hash::HashMapPair<int64_t, ObPxPool*> &kv);
  };
  class DeletePoolFunc
  {
  public:
    DeletePoolFunc() {}
    virtual ~DeletePoolFunc() = default;
    int operator()(common::hash::HashMapPair<int64_t, ObPxPool*> &kv);
  };
  class ThreadRecyclePoolFunc
  {
  public:
    ThreadRecyclePoolFunc() {}
    virtual ~ThreadRecyclePoolFunc() = default;
    int operator()(common::hash::HashMapPair<int64_t, ObPxPool*> &kv);
  };
public:
  static int mtl_init(ObPxPools *&pools)
  {
    int ret = common::OB_SUCCESS;
    uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(pools->init(tenant_id))) {
    }
    return ret;
  }
  static void mtl_stop(ObPxPools *&pools);
  static void mtl_destroy(ObPxPools *&pools)
  {
    common::ob_delete(pools);
    pools = nullptr;
  }
public:
  ObPxPools() : tenant_id_(common::OB_INVALID_ID),
                enable_database_isolation_mode_(false),
                lock_(common::ObLatchIds::OB_PX_POOLS_LOCK)
  {}
  ~ObPxPools()
  {
    destroy();
  }
  int init(uint64_t tenant_id);
  int get_or_create(int64_t group_id, ObPxPool *&pool);
  int thread_recycle();
private:
  void destroy();
  int create_pool(int64_t group_id, ObPxPool *&pool);
private:
  uint64_t tenant_id_;
  bool enable_database_isolation_mode_;
  common::SpinRWLock lock_;
  common::hash::ObHashMap<int64_t, ObPxPool *> pool_map_;
};

} // namespace omt
} // namespace oceanbase

#endif // _OCEANBASE_OBSERVER_OMT_OB_PX_POOL_H_
