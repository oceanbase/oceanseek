/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_TASK_H_

#include "lib/task/ob_timer.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread/thread_mgr_interface.h"
#include "common/ob_tablet_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
namespace oceanbase
{
namespace storage
{

class ObPrereadCacheManager;

class ObPreReadFileMeta final : public common::ObLink
{
public:
  ObPreReadFileMeta()
    : file_id_(), effective_tablet_id_(common::ObTabletID::INVALID_TABLET_ID), func_type_(0),
      trace_id_()
  {}
  ObPreReadFileMeta(const blocksstable::MacroBlockId &file_id,
                    const common::ObTabletID &effective_tablet_id,
                    const uint8_t func_type)
    : file_id_(file_id), effective_tablet_id_(effective_tablet_id), func_type_(func_type)
  {
    trace_id_ = *ObCurTraceId::get_trace_id();
  }
  ObPreReadFileMeta &operator=(const ObPreReadFileMeta &other);
  ~ObPreReadFileMeta() { reset(); }
  void reset();
  bool is_valid() const { return file_id_.is_valid(); }
  TO_STRING_KV(K_(file_id), K_(effective_tablet_id), K_(func_type), K_(trace_id));

public:
  blocksstable::MacroBlockId file_id_;
  common::ObTabletID effective_tablet_id_;
  uint8_t func_type_;
  common::ObCurTraceId::TraceId trace_id_;
};

class ObSSPreReadEntry final
{
public:
  ObSSPreReadEntry(common::ObIAllocator &allocator)
      : buf_(nullptr), file_meta_(), read_handle_(), allocator_(allocator), kvcache_put_ret_(OB_SUCCESS) {}
  ~ObSSPreReadEntry() { reset(); }
  int init(const ObPreReadFileMeta &file_meta);
  bool is_valid() const { return OB_NOT_NULL(buf_) && file_meta_.is_valid(); }
  bool is_read_handle_valid() const { return read_handle_.is_valid(); }
  void reset();
  share::ObFunctionType get_function_type() const
  {
    return static_cast<share::ObFunctionType>(file_meta_.func_type_);
  }
  TO_STRING_KV(KP_(buf), K_(file_meta), K_(read_handle));

public:
  char *buf_;
  ObPreReadFileMeta file_meta_;
  blocksstable::ObStorageObjectHandle read_handle_;
  common::ObIAllocator &allocator_;
  // 后台 KVCACHE put 的返回码，仅在 preread 从完成队列取回 entry 后有效。
  int kvcache_put_ret_;
};

// 每租户一个 put worker 线程；执行 put 并返回结果，节点更新及资源回收由 preread 处理。
class ObSSPreReadPutWorker final : public lib::TGTaskHandler
{
public:
  explicit ObSSPreReadPutWorker(common::ObFixedQueue<ObSSPreReadEntry> &completed_put_entries)
    : tg_id_(-1), is_stopped_(true), submit_lock_(common::ObLatchIds::THREAD_POOL_LOCK),
      completed_put_entries_(completed_put_entries) {}
  virtual ~ObSSPreReadPutWorker() { destroy(); }
  int init();
  int start();
  // 停止接收任务，通知 worker 退出。
  void stop();
  // 等待 worker 退出，调用前需先 stop()。
  void wait();
  void destroy();
  bool is_stopped() const { return ATOMIC_LOAD(&is_stopped_); }
  int submit(ObSSPreReadEntry &preread_entry);
  virtual void handle(void *task) override;
  virtual void handle_drop(void *task) override;

private:
  void enqueue_completed_entry_(ObSSPreReadEntry &preread_entry, const int kvcache_put_ret);

private:
  int tg_id_;
  bool is_stopped_;
  // 提交和停止共用这把锁，确保正在提交的任务先入队，再停止 worker。
  common::ObSpinLock submit_lock_;
  // 引用 preread 持有的完成队列，仅投递结果，不管理队列的生命周期。
  common::ObFixedQueue<ObSSPreReadEntry> &completed_put_entries_;
  DISALLOW_COPY_AND_ASSIGN(ObSSPreReadPutWorker);
};

class ObSSPreReadTask : public common::ObTimerTask, public ObSSBasePrewarmer
{
public:
  // entry 池最大条数与异步 put 队列容量共用此上限，避免调整预读并发时漏改队列容量。
  static const int64_t MAX_PRE_READ_PARALLELISM = 128;

  ObSSPreReadTask();
  virtual ~ObSSPreReadTask() { destroy(); }
  int init(ObPrereadCacheManager *preread_cache_mgr);
  int start(const int tg_id);
  // 请求停止后台 put，不等待预读定时任务或 worker 退出。
  void stop();
  // 调用前须先 stop() 并等待预读定时任务退出；等待 worker 退出后统一回收完成的 entry。
  void wait();
  void destroy();

  virtual void runTimerTask() override;
  int schedule_preread_task();

private:
  void destroy_preread_entry_arr();
  int pre_alloc_preread_entry();
  int preread_files();
  int batch_get_preread_entry_ids();
  int do_async_read_entry(ObSSPreReadEntry &preread_entry);
  // 成功返回表示 entry 已交给 worker，须等完成结果返回后才能回收。
  int try_submit_kvcache_put_(ObSSPreReadEntry &preread_entry);
  int recycle_completed_put_entries_();
  int async_read_entries();
  int write_preread_entries();
  int complete_entry(const blocksstable::MacroBlockId &macro_id, const int64_t buf_size, const ObPreReadFileMeta &file_meta);
  int recycle_preread_entry(ObSSPreReadEntry &preread_entry);
  void clear_for_next_round();
  void set_read_cache_parallel_cnt();
  void adjust_pre_read_task_interval();
  void set_max_preread_parallelism();

private:
  static const int64_t SLOW_SCHEDULE_INTERVAL_US = 10 * 1000L; // 10ms
  static const int64_t MODERATE_SCHEDULE_INTERVAL_US = 100L; // 100us, wait for in flight object storage io
  static const int64_t FAST_SCHEDULE_INTERVAL_US = 10L; // 10us, wait for in flight local disk io
  static const int64_t UPDATE_PARALLEL_INTERVAL_US = 60 * 1000 * 1000L; // 1min
  static const int64_t DEFAULT_PRE_READ_PARALLELISM = 5;
  static const int64_t GB = 1024L * 1024L * 1024L;

private:
  bool is_inited_;
  int tg_id_;
  uint64_t tenant_id_;
  int64_t preread_parallel_cnt_;
  int64_t interval_us_;
  int64_t max_pre_read_parallelism_;
  ObPrereadCacheManager *preread_cache_mgr_;
  ObSSPreReadEntry *preread_entry_arr_;
  // entry 从空闲池取出后，先用于远端读，再交给 worker 执行 KVCACHE put，最后由 preread 回收。
  // 读队列保存正在读取或已读完待提交的 entry；提交给 worker 的 entry 在取回前计入 pending。
  // 每轮检查容量时，空闲数 + 读队列中的数量 + pending_put_count_ 等于 entry 池总数。
  common::ObFixedQueue<ObSSPreReadEntry> free_list_;
  common::ObFixedQueue<ObSSPreReadEntry> async_read_list_;
  // worker 投递完成的 entry，preread 取出后处理结果并回收；队列须保留到 worker 退出。
  common::ObFixedQueue<ObSSPreReadEntry> completed_put_entries_;
  ObSSPreReadPutWorker put_worker_;
  // 已提交给 worker、尚未由 preread 取回的 entry 数。
  int64_t pending_put_count_;
  common::ObArray<ObPreReadFileMeta> segment_files_;
  common::ObArray<ObPreReadFileMeta> preread_files_;
  common::ObFIFOAllocator allocator_;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_PREREAD_TASK_H_ */
