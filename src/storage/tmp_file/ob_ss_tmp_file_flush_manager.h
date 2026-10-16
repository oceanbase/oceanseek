/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_MANAGER_H_

#include "lib/list/ob_dlist.h"
#include "lib/task/ob_timer.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/tmp_file/ob_ss_tmp_file_flush_priority_manager.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace tmp_file
{
class ObSSTmpFileAsyncFlushWaitTaskHandle;
class ObSSTmpFileFlushContext;
class ObSSTmpFileFlushManager;
class ObSSTenantTmpFileManager;
class ObSharedStorageTmpFile;
class ObSSTmpWriteBufferPool;

struct ObSSTmpFileAsyncFlushWaitTask
{
public:
  friend class ObSSTmpFileAsyncFlushWaitTaskHandle;

public:
  ObSSTmpFileAsyncFlushWaitTask();
  ~ObSSTmpFileAsyncFlushWaitTask();
  int init(const int64_t fd, const int64_t length, const uint32_t begin_page_id,
           const int64_t current_begin_page_virtual_id,
           const int64_t expected_flushed_page_num,
           const ObTmpFileGlobal::FlushCtxState current_flush_state,
           ObSSTmpFileFlushManager *flush_mgr);
  int push_back_io_task(const ObSSTmpFileFlushContext &ctx, const int64_t flushed_page_num);
  int exec_wait();
  int cond_broadcast(int32_t ret_code);
  int cond_wait(const int64_t timeout_ms);
  OB_INLINE bool have_io_task() const { return !io_tasks_.empty() && !io_page_num_.empty(); }
  OB_INLINE int32_t get_ret_code() const { return ret_code_; }
  OB_INLINE int32_t get_fd() const { return fd_; }

  TO_STRING_KV(K(fd_), K(current_length_), K(current_begin_page_id_),
               K(current_begin_page_virtual_id_),
               K(flushed_offset_), K(expected_flushed_page_num_),
               K(succeed_wait_page_nums_), K(current_flush_state_),
               K(ref_cnt_), K(wait_has_finished_),
               K(is_inited_), K(ret_code_),
               K(io_page_num_.count()),
               K(io_tasks_.count()));

private:
  int release_io_tasks_();
  OB_INLINE void inc_ref_cnt_()
  {
    ATOMIC_INC(&ref_cnt_);
  }
  OB_INLINE void dec_ref_cnt_(int32_t *new_ref_cnt = nullptr)
  {
    int32_t ref_cnt = -1;
    ref_cnt = ATOMIC_AAF(&ref_cnt_, -1);
    if (OB_NOT_NULL(new_ref_cnt)) {
      *new_ref_cnt = ref_cnt;
    }
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTmpFileAsyncFlushWaitTask);

public:
  int64_t fd_;
  int64_t current_length_;                  // when generate a flush task, record current file length
  uint32_t current_begin_page_id_;          // when generate a flush task, record current page id
  int64_t current_begin_page_virtual_id_;   // when generate a flush task, record current virtual page id
  int64_t flushed_offset_;                  // when flush task are generated over, record current flushed offset
                                            // (it is target flushing offset of this task, which is aligned with page size [8KB])
  ObArray<std::pair<blocksstable::ObStorageObjectHandle *, char *>> io_tasks_;
  ObArray<int64_t> io_page_num_;
  int64_t expected_flushed_page_num_;      // flushing page nums in async thread
  int64_t succeed_wait_page_nums_;
  ObTmpFileGlobal::FlushCtxState current_flush_state_;

private:
  ObSSTmpFileFlushManager *flush_mgr_;
  ObThreadCond cond_;
  int32_t ref_cnt_;
  bool wait_has_finished_;
  bool is_inited_;
  int32_t ret_code_;
};

struct ObSSTmpFileAsyncFlushWaitTaskHandle : public common::ObDLink
{
public:
  ObSSTmpFileAsyncFlushWaitTaskHandle();
  ObSSTmpFileAsyncFlushWaitTaskHandle(ObSSTmpFileAsyncFlushWaitTask *wait_task);
  ~ObSSTmpFileAsyncFlushWaitTaskHandle();
  void set_wait_task(ObSSTmpFileAsyncFlushWaitTask *wait_task);
  void reset();
  OB_INLINE ObSSTmpFileAsyncFlushWaitTask * get() { return wait_task_; }
  int wait(const int64_t timeout_ms);
  TO_STRING_KV(KPC(wait_task_));

public:
  ObSSTmpFileAsyncFlushWaitTask *wait_task_;
};

class ObTmpFileAsyncWaitTaskQueue
{
public:
  ObTmpFileAsyncWaitTaskQueue();
  ~ObTmpFileAsyncWaitTaskQueue();
  int push(ObSSTmpFileAsyncFlushWaitTaskHandle *task_handle);
  int pop(ObSSTmpFileAsyncFlushWaitTaskHandle *&task_handle);
  OB_INLINE bool is_empty() const { return queue_.is_empty(); }
  OB_INLINE int64_t get_queue_length() const { return ATOMIC_LOAD(&queue_length_); }

  TO_STRING_KV(K(queue_length_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileAsyncWaitTaskQueue);

private:
  common::ObSpLinkQueue queue_;
  int64_t queue_length_;
};

// background flush task, invoke flush when buffer pool reaches high watermark
class ObSSTmpFileFlushTimerTask : public common::ObTimerTask
{
public:
  ObSSTmpFileFlushTimerTask() : is_inited_(false),
                                wbp_(nullptr),
                                flush_mgr_(nullptr) {}
  virtual ~ObSSTmpFileFlushTimerTask() { destroy(); }
  int init(ObSSTmpWriteBufferPool *wbp, ObSSTmpFileFlushManager *flush_mgr);
  void destroy();
  virtual void runTimerTask() override;
  TO_STRING_KV(K(is_inited_), KP(wbp_), KP(flush_mgr_));
private:
  bool is_inited_;
  ObSSTmpWriteBufferPool *wbp_;
  ObSSTmpFileFlushManager *flush_mgr_;
};

class ObSSTmpFileWaitTimerTask : public common::ObTimerTask
{
public:
  ObSSTmpFileWaitTimerTask() : is_inited_(false), last_print_ts_(ObTimeUtility::current_time()), flush_mgr_(nullptr) {}
  virtual ~ObSSTmpFileWaitTimerTask() {}
  int init(ObSSTmpFileFlushManager *flush_mgr);
  virtual void runTimerTask() override;
  TO_STRING_KV(K(is_inited_), K(last_print_ts_), KP(flush_mgr_));

private:
  bool is_inited_;
  int64_t last_print_ts_;
  ObSSTmpFileFlushManager *flush_mgr_;
};

class ObSSTmpFileFlushManager
{
public:
  ObSSTmpFileFlushManager() : is_inited_(false),
                              is_processing_wait_task_(false),
                              wait_tg_id_(OB_INVALID_INDEX),
                              flush_tg_id_(OB_INVALID_INDEX),
                              flush_prio_mgr_(),
                              wait_task_allocator_(), wait_task_queue_(),
                              wait_timer_task_(),
                              flush_timer_task_(),
                              file_mgr_(nullptr),
                              wbp_(nullptr),
                              stat_lock_(common::ObLatchIds::OB_SS_TMP_FILE_FLUSH_STATE_LOCK),
                              total_flushing_page_num_(0),
                              f1_cnt_(0),
                              f2_cnt_(0),
                              f3_cnt_(0)
                              {}
  virtual ~ObSSTmpFileFlushManager() {}
  int init(const uint64_t tenant_id, const ObSSTenantTmpFileManager *file_mgr,
           ObSSTmpWriteBufferPool *wbp);
  int start();
  void stop();
  int wait();
  void destroy();
  int exec_wait_task_once();
  void print_stat_info();

public:
  typedef ObTmpFileGlobal::FlushCtxState FlushCtxState;
  int wash(const int64_t expect_wash_size, ObSSTmpFileAsyncFlushWaitTaskHandle &wait_task_handle, int64_t &actual_wash_size);
  int get_tmp_file(const int64_t fd, ObSSTmpFileHandle &tmp_file_handle) const;
  int update_meta_after_flush(ObSSTmpFileHandle &tmp_file_handle, const ObSSTmpFileAsyncFlushWaitTask &wait_task) const;
  int notify_flush_breakdown(const int64_t fd, const int discard_reason);
  int wait_task_enqueue(ObSSTmpFileAsyncFlushWaitTask *task);

  ObSSTmpFileFlushPriorityManager &get_flush_prio_mgr() { return flush_prio_mgr_; }
  common::ObFIFOAllocator& get_wait_task_allocator() { return wait_task_allocator_; }
  ObTmpFileAsyncWaitTaskQueue& get_wait_task_queue() { return wait_task_queue_; }
  OB_INLINE int64_t get_task_num() const { return wait_task_queue_.get_queue_length(); }
  OB_INLINE bool have_task() const { return wait_task_queue_.get_queue_length() > 0; }
  int64_t get_flushing_page_num();

  TO_STRING_KV(K(is_inited_), K(wait_tg_id_), K(flush_tg_id_), KP(&flush_prio_mgr_), KP(&wait_task_allocator_), K(wait_task_queue_),
               K(total_flushing_page_num_), K(f1_cnt_), K(f2_cnt_), K(f3_cnt_));
private:
  int record_stat_info_(const FlushCtxState &stage, const int64_t flushing_page_num);
  int modify_stat_info_(const FlushCtxState &stage, const int64_t flushed_page_num);
private:
  bool is_inited_;
  bool is_processing_wait_task_; // used in ut to check flushing status
  int wait_tg_id_;
  int flush_tg_id_;
  ObSSTmpFileFlushPriorityManager flush_prio_mgr_;
  common::ObFIFOAllocator wait_task_allocator_;
  ObTmpFileAsyncWaitTaskQueue wait_task_queue_;
  ObSSTmpFileWaitTimerTask wait_timer_task_;
  ObSSTmpFileFlushTimerTask flush_timer_task_;
  const ObSSTenantTmpFileManager *file_mgr_;
  ObSSTmpWriteBufferPool *wbp_;

  // statistics
  ObSpinLock stat_lock_;
  int64_t total_flushing_page_num_;
  int64_t f1_cnt_;
  int64_t f2_cnt_;
  int64_t f3_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTmpFileFlushManager);
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_MANAGER_H_