/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_BLOCK_GC_THREAD_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_BLOCK_GC_THREAD_H_

#include "lib/thread/thread_mgr_interface.h"
#include "lib/allocator/page_arena.h"
#include "src/storage/shared_storage/ob_block_gc_handler.h"
#include "share/ob_thread_define.h"           // TGDefIDs
#include "share/ob_thread_mgr.h"              // TG_START

namespace oceanbase
{
namespace storage
{

template<class CTX>
class ObBlockGCThread : public lib::TGTaskHandler
{
public:
  ObBlockGCThread()
    : tg_id_(-1),
      is_inited_(false),
      unfinished_task_cnt_(0),
      gc_task_ctx_(NULL)
  {}
  ~ObBlockGCThread()
  {
    destroy();
  }


public:
  int init(const int tg_id);
  int start();
  int stop();
  int wait();
  void destroy();
  int get_tg_id() const;

  void init_gc_task_ctx(CTX &gc_task_ctx)
  { gc_task_ctx_ = &gc_task_ctx; }

  bool is_valid_ctx()
  { return NULL != gc_task_ctx_ && gc_task_ctx_->is_valid(); }

  void clear_task_ctx()
  { gc_task_ctx_ = NULL; }

  CTX* get_gc_task_ctx()
  { return gc_task_ctx_; }

  void wait_gc_task_finished();

  virtual void handle(void *task) = 0;
  virtual void handle_drop(void *task) 
  {
    int ret = OB_SUCCESS;
    handle_end(task, true /* need_free */);
    STORAGE_LOG(INFO, "gc thread pool is stop", K(ret), KP(task), KPC(this));
  }
  void handle_end(void *task, const bool need_free)
  {
    if (need_free) {
      CTX *ctx = get_gc_task_ctx();
      if (OB_ISNULL(ctx)) {
        STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "gc task ctx is NULL", KP(task), KPC(this));
      } else {
        ctx->get_allocator().free(task);
      }
    }
    ATOMIC_DEC(&unfinished_task_cnt_); 
    STORAGE_LOG(TRACE, "tablet_gc_task end", KP(task), KPC(this));
  }

  template<class T>
  int add_gc_task(T &task);
  // Enqueue the original object. Its owner keeps it alive until all tasks finish;
  // the worker completes it with handle_end(task, false).
  template<class T>
  int add_gc_task(T *task);
public:
  VIRTUAL_TO_STRING_KV(K_(tg_id), K_(is_inited), K_(unfinished_task_cnt));
private:
  static bool reach_time_interval(
      int64_t interval,
      int64_t &last_time)
  {
    bool bret = false;
    int64_t cur_time = common::ObTimeUtility::fast_current_time();
    if (OB_UNLIKELY((interval + last_time < cur_time))) {
      last_time = cur_time;
      bret = true;
    }
    return bret;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObBlockGCThread);

private:
  int tg_id_;
  bool is_inited_;
  int64_t unfinished_task_cnt_;
  CTX *gc_task_ctx_;
};

template<class CTX>
template<class T>
int ObBlockGCThread<CTX>::add_gc_task(T &task)
{
  int ret = OB_SUCCESS;
  void *handler_ptr = NULL;
  T *handler;
  if (!is_valid_ctx()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "gc ctx is invalid", K(ret), K(task), KPC(this));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "task is invalid", K(ret), K(task), KPC(this));
  } else if (OB_ISNULL(handler_ptr = gc_task_ctx_->get_allocator().alloc(sizeof(T)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to alloc", K(ret), K(task), KPC(this));
  } else {
    handler = new (handler_ptr)T(task);
    int64_t last_time = 0;
    const int64_t RETRY_INTERVAL = 1000 * 1000;
    int64_t retry_times = 10;
    while (OB_FAIL(TG_PUSH_TASK(tg_id_, handler))) {
      if (OB_IN_STOP_STATE == ret) {
        STORAGE_LOG(WARN, "thread_pool has been stopped, skip task", K(ret), KPC(this));
        break;
      } else if (retry_times-- > 0) {
        if (reach_time_interval(60 * 1000 * 1000, last_time)) {
          STORAGE_LOG(WARN, "push gc task error", K(ret), K(handler), KPC(this));
        }
        ob_usleep(1000 * 1000);
      } else {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_INC(&unfinished_task_cnt_);
      STORAGE_LOG(TRACE, "tablet_gc_task add", K(ret), KP(handler), K(task), KPC(this));
    }
  }
  return ret;
}

template<class CTX>
template<class T>
int ObBlockGCThread<CTX>::add_gc_task(T *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "gc thread is not initialized", KR(ret), KPC(this));
  } else if (!is_valid_ctx()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "gc ctx is invalid", KR(ret), KP(task), KPC(this));
  } else if (OB_ISNULL(task) || !task->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "task is invalid", KR(ret), KP(task), KPC(this));
  } else {
    // The task may finish immediately after being published to the queue.
    ATOMIC_INC(&unfinished_task_cnt_);
    int64_t last_time_us = 0;
    int64_t retry_times = 10;
    bool need_retry = true;
    while (need_retry && OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
      if (OB_EAGAIN != ret) {
        STORAGE_LOG(WARN, "push gc task failed, stop retrying", KR(ret), KP(task), KPC(this));
        need_retry = false;
      } else if (retry_times-- > 0) {
        if (reach_time_interval(60 * 1000 * 1000, last_time_us)) {
          STORAGE_LOG(WARN, "push gc task error", KR(ret), KP(task), KPC(this));
        }
        ob_usleep(1000 * 1000);
      } else {
        need_retry = false;
      }
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "tablet_gc_task add by pointer", KR(ret), KP(task), KPC(this));
    } else {
      // Failed submission does not transfer ownership or leave an unfinished task.
      ATOMIC_DEC(&unfinished_task_cnt_);
    }
  }
  return ret;
}

template<class CTX>
int ObBlockGCThread<CTX>::init(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "ObBlockGCThread<CTX> has inited!!!", K(ret), KPC(this));
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_))) {
    STORAGE_LOG(WARN, "ObBlockGCThread<CTX> TG_CREATE failed", K(ret), KPC(this));
  } else {
    is_inited_ = true;
    STORAGE_LOG(INFO, "ObBlockGCThread<CTX> init success", K(ret), KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

template<class CTX>
int ObBlockGCThread<CTX>::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObBlockGCThread<CTX> not inited!!!", K(ret), KPC(this));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    STORAGE_LOG(ERROR, "start ObBlockGCThread<CTX> failed", K(ret));
  } else {
    STORAGE_LOG(INFO, "start ObBlockGCThread<CTX> success", K(ret), KPC(this));
  }
  return ret;
}

template<class CTX>
int ObBlockGCThread<CTX>::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBlockGCThread<CTX> not inited!!!", K(ret), KPC(this));
  } else {
    TG_STOP(tg_id_);
    STORAGE_LOG(INFO, "stop ObBlockGCThread<CTX> success", KPC(this));
  }
  return ret;
}

template<class CTX>
int ObBlockGCThread<CTX>::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBlockGCThread<CTX> not inited!!!", K(ret), KPC(this));
  } else {
    TG_WAIT(tg_id_);
    STORAGE_LOG(INFO, "wait ObBlockGCThread<CTX> success", KPC(this));
  }
  return ret;
}

template<class CTX>
void ObBlockGCThread<CTX>::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = -1;
  STORAGE_LOG(INFO, "destroy ObBlockGCThread<CTX> success", K(tg_id_), KPC(this));
}

template<class CTX>
void ObBlockGCThread<CTX>::wait_gc_task_finished()
{
  int ret = OB_SUCCESS;
  int64_t unfinished_task_cnt = 0;
  int64_t last_time = 0;
  while (true) {
    unfinished_task_cnt = ATOMIC_LOAD(&unfinished_task_cnt_);
    if (unfinished_task_cnt < 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "task_cnt is invalid", K(ret), KPC(this));
      break;
    } else if (unfinished_task_cnt > 0) {
      if (reach_time_interval(60 * 1000 * 1000, last_time)) {
        STORAGE_LOG(INFO, "wait gc threads end", K(ret), KPC(this));
      }
      ob_usleep(100 * 1000);
    } else {
      break;
    }
  }
}

template<class CTX>
int ObBlockGCThread<CTX>::get_tg_id() const
{
  return tg_id_;
}

template<class CTX>
class ObBlockGCThreadGuard
{
public:
  ObBlockGCThreadGuard(
      ObBlockGCThread<CTX> &gc_thread,
      CTX &ctx)
    : gc_thread_(gc_thread)
  { gc_thread_.init_gc_task_ctx(ctx); }

  ~ObBlockGCThreadGuard()
  { 
    gc_thread_.clear_task_ctx(); 
  }

  void wait_gc_task_finished()
  { gc_thread_.wait_gc_task_finished(); }

  template<class T>
  int add_gc_task(T &task)
  { return gc_thread_.template add_gc_task(task); }

  template<class T>
  int add_gc_task(T *task)
  { return gc_thread_.template add_gc_task<T>(task); }

  TO_STRING_KV(K_(gc_thread));
private:
  DISALLOW_COPY_AND_ASSIGN(ObBlockGCThreadGuard);
private: 
  ObBlockGCThread<CTX> &gc_thread_;
};

}
} // end namespace oceanbase

#endif
