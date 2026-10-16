/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_ss_preread_task.h"
#include "share/ob_thread_mgr.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "share/io/ob_io_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObPreReadFileMeta-----------------------------------------*/
void ObPreReadFileMeta::reset()
{
  file_id_.reset();
  effective_tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  func_type_ = 0;
  trace_id_.reset();
}

ObPreReadFileMeta &ObPreReadFileMeta::operator=(const ObPreReadFileMeta &other)
{
  if (this != &other) {
    file_id_ = other.file_id_;
    effective_tablet_id_ = other.effective_tablet_id_;
    func_type_ = other.func_type_;
    trace_id_ = other.trace_id_;
  }
  return *this;
}

/*-----------------------------------------ObSSPreReadEntry-----------------------------------------*/
int ObSSPreReadEntry::init(const ObPreReadFileMeta &file_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr != buf_) || read_handle_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(*this));
  } else if (OB_UNLIKELY(!file_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_meta));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(OB_DEFAULT_MACRO_BLOCK_SIZE));
  } else {
    read_handle_.reset();
    file_meta_ = file_meta;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObSSPreReadEntry::reset()
{
  read_handle_.reset();
  if (OB_NOT_NULL(buf_)) {
    allocator_.free(buf_);
    buf_ = nullptr;
  }
  file_meta_.reset();
  kvcache_put_ret_ = OB_SUCCESS;
}

/*-----------------------------------------ObSSPreReadPutWorker-----------------------------------------*/
int ObSSPreReadPutWorker::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(-1 != tg_id_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("preread put worker is already inited", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSPrereadPut, tg_id_))) {
    LOG_WARN("fail to create preread put worker", KR(ret));
  }
  return ret;
}

int ObSSPreReadPutWorker::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(-1 == tg_id_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread put worker is not inited", KR(ret), K_(tg_id));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("fail to start preread put worker", KR(ret));
  } else {
    ATOMIC_STORE(&is_stopped_, false);
  }
  return ret;
}

void ObSSPreReadPutWorker::stop()
{
  // 等正在入队的提交结束后再停止线程组；put 执行和完成结果投递不持有此锁。
  ObSpinLockGuard guard(submit_lock_);
  ATOMIC_STORE(&is_stopped_, true);
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
  }
}

void ObSSPreReadPutWorker::wait()
{
  if (-1 != tg_id_) {
    TG_WAIT(tg_id_);
  }
}

void ObSSPreReadPutWorker::destroy()
{
  stop();
  wait();
  if (-1 != tg_id_) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
}

int ObSSPreReadPutWorker::submit(ObSSPreReadEntry &preread_entry)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(submit_lock_);
  if (OB_UNLIKELY(is_stopped())) {
    ret = OB_SERVICE_STOPPED;
    LOG_WARN("preread put worker is stopped, reject submission", KR(ret), K_(tg_id), K(preread_entry));
  } else if (OB_UNLIKELY(!preread_entry.is_valid() || !preread_entry.is_read_handle_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid preread entry for KVCACHE put", KR(ret), K(preread_entry));
  } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, &preread_entry))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to submit preread put", KR(ret), K(preread_entry));
    }
  }
  return ret;
}

// put 线程回调：将预读数据写入 Macro Block KVCache，再向 preread 的完成队列投递结果。
void ObSSPreReadPutWorker::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObSSPreReadEntry *preread_entry = static_cast<ObSSPreReadEntry *>(task);
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr = nullptr;
  if (OB_ISNULL(preread_entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("preread put entry is null", KR(ret));
  } else {
    ObTraceIDGuard trace_guard(preread_entry->file_meta_.trace_id_);
    CONSUMER_GROUP_FUNC_GUARD(preread_entry->get_function_type());
    const uint64_t effective_tablet_id = preread_entry->file_meta_.effective_tablet_id_.id();
    if (OB_UNLIKELY(is_stopped())) {
      ret = OB_SERVICE_STOPPED;
      LOG_WARN("preread put worker is stopped, cancel put", KR(ret), K_(tg_id), KPC(preread_entry));
    } else if (OB_ISNULL(tenant_kvcache_mgr = MTL(ObSSTenantMacroBlockKVCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant macro block KVCACHE mgr is null", KR(ret));
    } else if (OB_FAIL(tenant_kvcache_mgr->put(preread_entry->file_meta_.file_id_, effective_tablet_id,
        preread_entry->read_handle_.get_buffer(), preread_entry->read_handle_.get_data_size()))) {
      // 在 put 线程记录失败现场；EAGAIN 交回 preread 重试，避免重复告警。
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail to put preread macro block into KVCACHE", KR(ret),
            "macro_id", preread_entry->file_meta_.file_id_, K(effective_tablet_id),
            "data_size", preread_entry->read_handle_.get_data_size());
      }
    }
    // 无论成败都交回 entry；buffer 释放和节点更新由 preread 主流程处理。
    enqueue_completed_entry_(*preread_entry, ret);
  }
}

// 停止时不再执行排队的 put，交回 entry 和 OB_SERVICE_STOPPED，由 preread 回收资源。
void ObSSPreReadPutWorker::handle_drop(void *task)
{
  int ret = OB_SUCCESS;
  ObSSPreReadEntry *preread_entry = static_cast<ObSSPreReadEntry *>(task);
  if (OB_ISNULL(preread_entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dropped preread put entry is null", KR(ret));
  } else {
    enqueue_completed_entry_(*preread_entry, OB_SERVICE_STOPPED);
  }
}

void ObSSPreReadPutWorker::enqueue_completed_entry_(ObSSPreReadEntry &preread_entry, const int kvcache_put_ret)
{
  int ret = OB_SUCCESS;
  preread_entry.kvcache_put_ret_ = kvcache_put_ret;
  // 完成队列入队成功后，worker 不再访问 entry；preread 可能立即取回、回收并复用它。
  if (OB_FAIL(completed_put_entries_.push(&preread_entry))) {
    LOG_ERROR("fail to return preread put entry", KR(ret), KP(&preread_entry));
  }
}

/*-----------------------------------------ObSSPreReadTask-----------------------------------------*/
ObSSPreReadTask::ObSSPreReadTask()
  : is_inited_(false), tg_id_(-1), tenant_id_(OB_INVALID_TENANT_ID), preread_parallel_cnt_(0), interval_us_(-1),
    max_pre_read_parallelism_(0), preread_cache_mgr_(nullptr), preread_entry_arr_(nullptr), free_list_(), async_read_list_(),
    completed_put_entries_(), put_worker_(completed_put_entries_), pending_put_count_(0),
    segment_files_(), preread_files_(), allocator_()
{}

int ObSSPreReadTask::init(ObPrereadCacheManager *preread_cache_mgr)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), "SSPreRead");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSPreReadTask init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(preread_cache_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(preread_cache_mgr));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE, attr))) {
    LOG_WARN("fail to init allocatro", KR(ret));
  } else if (FALSE_IT(set_max_preread_parallelism())) {
    LOG_WARN("fail to set max_pre_read_parallelism_", KR(ret));
  } else if (OB_FAIL(free_list_.init(max_pre_read_parallelism_))) {
    LOG_WARN("fail to init free_list", KR(ret), K_(max_pre_read_parallelism));
  } else if (OB_FAIL(async_read_list_.init(max_pre_read_parallelism_))) {
    LOG_WARN("fail to init async_read_list", KR(ret), K_(max_pre_read_parallelism));
  } else if (OB_FAIL(completed_put_entries_.init(max_pre_read_parallelism_))) {
    LOG_WARN("fail to init preread put completion queue", KR(ret), K_(max_pre_read_parallelism));
  } else if (OB_FAIL(pre_alloc_preread_entry())) {
    LOG_WARN("fail to pre alloc preread_entry", KR(ret));
  } else if (OB_FAIL(put_worker_.init())) {
    LOG_WARN("fail to init preread put worker", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    preread_cache_mgr_ = preread_cache_mgr;
    interval_us_ = SLOW_SCHEDULE_INTERVAL_US;
    is_inited_ = true;
    set_read_cache_parallel_cnt();
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  LOG_INFO("finish to init preread task", KR(ret), K_(preread_parallel_cnt), K(MTL_MEM_SIZE()));
  return ret;
}

int ObSSPreReadTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("preread is not init", KR(ret));
  } else if (OB_UNLIKELY(-1 == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(put_worker_.start())) {
    LOG_WARN("fail to start preread put worker", KR(ret));
  } else if (OB_FAIL(schedule_preread_task())) {
    LOG_WARN("fail to schedule preread task", KR(ret));
  } else {
    LOG_INFO("succ to start preread task", K(tg_id));
  }
  return ret;
}

void ObSSPreReadTask::stop()
{
  put_worker_.stop();
}

void ObSSPreReadTask::wait()
{
  int ret = OB_SUCCESS;
  put_worker_.wait();
  if (OB_FAIL(recycle_completed_put_entries_())) {
    LOG_WARN("fail to recycle stopped preread puts", KR(ret));
  }
}

void ObSSPreReadTask::destroy()
{
  stop();
  wait();
  put_worker_.destroy();
  destroy_preread_entry_arr();
  pending_put_count_ = 0;
  tg_id_ = -1;
  tenant_id_ = OB_INVALID_TENANT_ID;
  preread_parallel_cnt_ = 0;
  max_pre_read_parallelism_ = 0;
  interval_us_ = -1;
  preread_cache_mgr_ = nullptr;
  free_list_.destroy();
  async_read_list_.destroy();
  completed_put_entries_.destroy();
  segment_files_.destroy();
  preread_files_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

void ObSSPreReadTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(preread_files())) {
    LOG_WARN("fail to preread file to local cache", KR(ret), K_(tenant_id));
  }

  // adjust preread_parallel_cnt if memory size changes.
  if (TC_REACH_TIME_INTERVAL(UPDATE_PARALLEL_INTERVAL_US)) {
    set_read_cache_parallel_cnt();
  }

  adjust_pre_read_task_interval();
  if (OB_TMP_FAIL(schedule_preread_task())) {
    if (OB_CANCELED == tmp_ret) {
      LOG_INFO("preread task has stopped");
    } else {
      LOG_ERROR("fail to reschedule preread task", KR(ret), K(tmp_ret), K_(interval_us));
    }
  }
}

int ObSSPreReadTask::schedule_preread_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, interval_us_, false/*repeat*/))) {
    LOG_WARN("fail to schedule ss preread task", KR(ret), K_(tg_id), K_(interval_us));
  } else {
    LOG_TRACE("succ to schedule ss preread task", K_(tg_id), K_(interval_us));
  }
  return ret;
}

void ObSSPreReadTask::destroy_preread_entry_arr()
{
  if (nullptr != preread_entry_arr_) {
    for (int64_t i = 0; i < max_pre_read_parallelism_; ++i) {
      preread_entry_arr_[i].~ObSSPreReadEntry();
    }
    allocator_.free(preread_entry_arr_);
    preread_entry_arr_ = nullptr;
  }
}

void ObSSPreReadTask::set_max_preread_parallelism()
{
  if (MTL_MEM_SIZE() / GB < 32) {
    max_pre_read_parallelism_ = 32;
  } else if (MTL_MEM_SIZE() / GB < 64) {
    max_pre_read_parallelism_ = 64;
  } else {
    max_pre_read_parallelism_ = MAX_PRE_READ_PARALLELISM;
  }
}

int ObSSPreReadTask::pre_alloc_preread_entry()
{
  int ret = OB_SUCCESS;
  const int64_t mem_size = sizeof(ObSSPreReadEntry) * max_pre_read_parallelism_;
  if (OB_ISNULL(preread_entry_arr_ = static_cast<ObSSPreReadEntry *>(allocator_.alloc(mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(mem_size));
  } else {
    for (int64_t i = 0; i < max_pre_read_parallelism_; ++i) {
      new (preread_entry_arr_ + i) ObSSPreReadEntry(allocator_);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < max_pre_read_parallelism_; ++i) {
      ObSSPreReadEntry *preread_entry = preread_entry_arr_ + i;
      if (OB_FAIL(free_list_.push(preread_entry))) {
        LOG_WARN("fail to push preread_entry into free_list", K(ret), K(i), K(free_list_.get_curr_total()),
          K_(max_pre_read_parallelism));
      }
    }
  }

  if (OB_FAIL(ret)) {
    destroy_preread_entry_arr();
  }
  return ret;
}

int ObSSPreReadTask::preread_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *)) || OB_ISNULL(preread_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_disk_space_mgr or read_cache_mgr is null", KR(ret), KP(tnt_disk_space_mgr), KP_(preread_cache_mgr));
  } else {
    const int64_t start_us = ObTimeUtility::current_time_us();
    clear_for_next_round();
    // 每轮预读按以下顺序执行：
    // 1. 处理 worker 返回的 put 结果：需要重试的 entry 留待重试，其余归还空闲列表。
    // 2. 将远端读已完成、且仍需写入缓存的 entry 提交给 worker，异步执行 KVCACHE put。
    // 3. 获取下一批预读任务，用空闲 entry 发起远端读，无需等待上述 put 完成。
    if (OB_FAIL(recycle_completed_put_entries_())) {
      LOG_WARN("fail to recycle completed preread puts", KR(ret));
    } else if (OB_FAIL(write_preread_entries())) {
      LOG_WARN("fail to submit preread puts", KR(ret));
    } else if (OB_FAIL(batch_get_preread_entry_ids())) {
      LOG_WARN("fail to get preread entry ids", KR(ret), K_(segment_files));
    } else if (OB_FAIL(async_read_entries())) {
      LOG_WARN("fail to async read entries", KR(ret));
    }

    if (preread_files_.count() > 0) {
      LOG_INFO("finish to read file to local cache", KR(ret), K(preread_files_.count()),
               K_(preread_files), K(async_read_list_.get_curr_total()),
               "preread_queue_size", (preread_cache_mgr_->preread_queue_.size()),
               "preread_map_size", (preread_cache_mgr_->get_preread_map_size()),
               "cost_time_us", ObTimeUtility::current_time_us() - start_us);
    }
  }
  return ret;
}

int ObSSPreReadTask::batch_get_preread_entry_ids()
{
  int ret = OB_SUCCESS;
  const int64_t total_entry_cnt = free_list_.get_curr_total() + async_read_list_.get_curr_total()
      + pending_put_count_;
  const int64_t remaining_parallel_cnt = preread_parallel_cnt_ - async_read_list_.get_curr_total();
  if (OB_ISNULL(preread_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("preread_cache_mr is null", KR(ret), KP_(preread_cache_mgr));
  } else if (OB_UNLIKELY(total_entry_cnt != max_pre_read_parallelism_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("preread entry leak!!!", KR(ret), K(total_entry_cnt), K_(max_pre_read_parallelism));
  } else if (remaining_parallel_cnt < 0) {
    LOG_INFO("tenant memory may be reduced", KR(ret), K(remaining_parallel_cnt), K_(preread_parallel_cnt),
        K(async_read_list_.get_curr_total()));
  }

  const int64_t max_cnt = MIN(remaining_parallel_cnt, free_list_.get_curr_total());
  for (int64_t i = 0; OB_SUCC(ret) && (i < max_cnt); ++i) {
    ObLink *ptr = nullptr;
    ObPreReadFileMeta *preread_file_meta = nullptr;
    if (OB_FAIL(preread_cache_mgr_->preread_queue_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to pop preread file meta", KR(ret));
      }
    } else if (OB_ISNULL(ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread file meta is null", KR(ret));
    } else {
      preread_file_meta = static_cast<ObPreReadFileMeta *>(ptr);
      if (OB_FAIL(segment_files_.push_back(*preread_file_meta))) {
        LOG_WARN("fail to push back file_meta", KR(ret), KPC(preread_file_meta));
      }
    }

    // free memory
    OB_DELETE(ObPreReadFileMeta, attr, preread_file_meta);
  }
  return ret;
}

int ObSSPreReadTask::do_async_read_entry(ObSSPreReadEntry &preread_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!preread_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("preread_entry should be valid", KR(ret), K(preread_entry));
  } else if (OB_UNLIKELY(preread_entry.is_read_handle_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_handle should not be valid", KR(ret), K(preread_entry.read_handle_));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(preread_entry.get_function_type());

    const MacroBlockId &file_id = preread_entry.file_meta_.file_id_;
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = file_id;
    read_info.offset_ = 0;
    read_info.size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.io_desc_.set_sealed(); // preread tmp_file must be sealed
    read_info.buf_ = preread_entry.buf_;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(read_info.mtl_tenant_id_);
    ObSSObjectStorageReader object_storage_reader;
    if (OB_FAIL(object_storage_reader.aio_read(read_info, preread_entry.read_handle_))) {
      if (OB_OBJECT_NOT_EXIST == ret) {
        // because segment file has been gc, fail to read from remote object storage
        LOG_INFO("entry does not exist, may be already gc", K(file_id));
      } else {
        LOG_WARN("fail to aio read", KR(ret), K(read_info), K(preread_entry));
      }
    }
  }
  return ret;
}

int ObSSPreReadTask::async_read_entries()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t segment_file_cnt = segment_files_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < segment_file_cnt; ++i) {
    const ObPreReadFileMeta &file_meta = segment_files_.at(i);
    ObTraceIDGuard trace_guard(file_meta.trace_id_);
    ObSSPreReadEntry *preread_entry = nullptr;
    bool need_free = true;
    bool is_need_preread = true;
    if (OB_FAIL(free_list_.pop(preread_entry))) {
      LOG_WARN("fail to pop preread_entry from free_list", KR(ret), K(free_list_.get_curr_total()));
    } else if (OB_ISNULL(preread_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread_entry should not be null", KR(ret), KP(preread_entry));
    } else if (OB_FAIL(preread_entry->init(file_meta))) {
      LOG_WARN("fail to init preread_entry", KR(ret), K(file_meta), KPC(preread_entry));
    } else if (OB_FAIL(preread_cache_mgr_->is_file_id_need_preread(file_meta.file_id_, is_need_preread))) {
      LOG_WARN("fail to check need preread", KR(ret), K_(file_meta.file_id));
    } else if (!is_need_preread) {
      // do nothing, 1.NORMAL node do not need preread, 2.FAKE node has been read whole do not need preread, 3.preread node has been remove(gc) do not need preread
    } else if (OB_FAIL(do_async_read_entry(*preread_entry))) {
      if (OB_OBJECT_NOT_EXIST != ret) {
        LOG_WARN("fail to do async read entry", KR(ret), KPC(preread_entry));
      }
    } else if (OB_FAIL(async_read_list_.push(preread_entry))) {
      LOG_ERROR("fail to push preread_entry into read_list", KR(ret), K(async_read_list_.get_curr_total()));
    } else {
      need_free = false;
    }

    if (OB_FAIL(ret) && OB_TMP_FAIL(preread_cache_mgr_->remove_preread_node(file_meta.file_id_))) {
      LOG_WARN("fail to remove preread node", KR(tmp_ret), K_(file_meta.file_id));
    }

    if (need_free && (nullptr != preread_entry)) {
      if (OB_TMP_FAIL(recycle_preread_entry(*preread_entry))) {
        LOG_WARN("fail to recycle preread_entry", KR(tmp_ret), KPC(preread_entry), K(free_list_.get_curr_total()));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next segment_file.
  }
  return ret;
}

int ObSSPreReadTask::try_submit_kvcache_put_(ObSSPreReadEntry &preread_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!preread_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("preread_entry should be valid", KR(ret), K(preread_entry));
  } else if (OB_UNLIKELY(!preread_entry.is_read_handle_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_handle should be valid", KR(ret), K(preread_entry));
  } else {
    ObTraceIDGuard trace_guard(preread_entry.file_meta_.trace_id_);
    CONSUMER_GROUP_FUNC_GUARD(preread_entry.get_function_type());
    // 已读回的数据直接写入 L1，避免按冷热或大小跳过后再次回源；L2 刷盘仍按原策略处理。
    if (OB_FAIL(put_worker_.submit(preread_entry))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail to submit macro block KVCACHE put", KR(ret), K(preread_entry));
      }
    } else {
      ++pending_put_count_;
    }
  }
  return ret;
}

int ObSSPreReadTask::recycle_completed_put_entries_()
{
  int ret = OB_SUCCESS;
  ObSSPreReadEntry *preread_entry = nullptr;
  // 成功时更新节点和统计后回收 entry；可重试时保留数据；其他失败则清理节点并回收。
  // 完成队列为空时正常结束本轮处理，让调用方继续提交后续预读任务。
  // 初始化中途失败或重复销毁时，完成队列可能尚未初始化，无需回收。
  while (completed_put_entries_.is_inited() && OB_SUCC(completed_put_entries_.pop(preread_entry))) {
    int tmp_ret = OB_SUCCESS;
    bool need_recycle = true;
    --pending_put_count_;
    const MacroBlockId &macro_id = preread_entry->file_meta_.file_id_;
    const int64_t data_size = preread_entry->read_handle_.get_data_size();
    ObTraceIDGuard trace_guard(preread_entry->file_meta_.trace_id_);
    ret = preread_entry->kvcache_put_ret_;
    if (OB_UNLIKELY(put_worker_.is_stopped())) {
      // 正在停止时统一清理预读节点并回收 entry，不再重试或记录成功统计。
      ret = OB_SERVICE_STOPPED;
      LOG_WARN("preread put worker is stopped, recycle completed entry", KR(ret), K(macro_id));
    } else if (OB_EAGAIN == ret) {
      // put 返回可重试错误时留待下轮处理，worker 本身不循环重试。
      // 放回 async_read_list_ 并保留 buffer/预读节点，不回收 entry，也无需重新远端读。
      LOG_TRACE("retry preread KVCACHE put", KR(ret), K(macro_id));
      if (OB_TMP_FAIL(async_read_list_.push(preread_entry))) {
        LOG_WARN("fail to retry preread put entry", KR(tmp_ret), K(macro_id));
      } else {
        need_recycle = false;
      }
    } else if (OB_FAIL(ret)) {
      // 失败现场已由 put worker 记录，这里只跟踪异步回收，避免重复告警。
      LOG_TRACE("recycle failed preread KVCACHE put", KR(ret), K(macro_id), K(data_size),
          "effective_tablet_id", preread_entry->file_meta_.effective_tablet_id_);
    } else if (OB_FAIL(complete_entry(macro_id, data_size, preread_entry->file_meta_))) {
      LOG_WARN("fail to complete preread entry", KR(ret), K(macro_id), K(data_size));
    } else {
      inc_local_cache_prewarm_stat(ObSSLocalCachePrewarmType::PREREAD_TASK, 1/*delta_cnt*/, data_size);
    }

    // 只有成功放回读队列的 entry 留待重试；其他 entry 在这里统一回收。
    if (need_recycle) {
      if (OB_FAIL(ret) && OB_TMP_FAIL(preread_cache_mgr_->remove_preread_node(macro_id))) {
        LOG_WARN("fail to remove failed preread node", KR(tmp_ret), K(macro_id));
      }
      if (OB_TMP_FAIL(recycle_preread_entry(*preread_entry))) {
        LOG_ERROR("fail to recycle completed preread put", KR(tmp_ret));
      }
    }
    // 当前 entry 已安排重试或完成回收，清除本地错误码，让循环和后续预读调度继续执行。
    ret = OB_SUCCESS;
  }
  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ret)) {
    LOG_WARN("fail to pop completed preread put entry", KR(ret), K_(pending_put_count));
  }
  return ret;
}

int ObSSPreReadTask::write_preread_entries()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t io_cnt = async_read_list_.get_curr_total();
  for (int64_t i = 0; OB_SUCC(ret) && (i < io_cnt); ++i) {
    ObSSPreReadEntry *preread_entry = nullptr;
    bool need_free = true;
    bool is_finished = false;
    if (OB_FAIL(async_read_list_.pop(preread_entry))) {
      LOG_WARN("fail to pop preread_entry from async_read_list", KR(ret), K(i), K(io_cnt));
    } else if (OB_ISNULL(preread_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread_entry should not be null", KR(ret), KP(preread_entry));
    } else if (OB_UNLIKELY(!preread_entry->is_valid() || !preread_entry->is_read_handle_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("preread_entry and read_handle should be valid", KR(ret), KPC(preread_entry));
    } else if (OB_FAIL(preread_entry->read_handle_.check_is_finished(is_finished))) {
      LOG_WARN("fail to check read_handle is_finished", KR(ret), K_(preread_entry->read_handle));
    } else {
      const MacroBlockId &file_id = preread_entry->file_meta_.file_id_;
      if (is_finished) {
        bool is_need_preread = true;
        if (OB_FAIL(preread_entry->read_handle_.wait())) {
          LOG_WARN("fail to wait read_handle", KR(ret), K_(preread_entry->read_handle));
        } else if (OB_FAIL(preread_cache_mgr_->is_file_id_need_preread(file_id, is_need_preread))) {
          LOG_WARN("fail to check need preread", KR(ret), K(file_id));
        } else if (!is_need_preread) {
          // do nothing, 1.NORMAL node do not need preread, 2.FAKE node has been read whole do not need preread, 3.preread node has been remove(gc) do not need preread
        } else if (OB_FAIL(try_submit_kvcache_put_(*preread_entry))) {
          if (OB_EAGAIN == ret) {
            LOG_TRACE("preread put queue is full", KR(ret), K(i), KPC(preread_entry));
            if (OB_TMP_FAIL(async_read_list_.push(preread_entry))) {
              LOG_ERROR("fail to push preread_entry", KR(tmp_ret), K(async_read_list_.get_curr_total()));
              tmp_ret = OB_SUCCESS;
            } else {
              need_free = false;
            }
          } else {
            LOG_WARN("fail to submit preread KVCACHE put", KR(ret), K(i), KPC(preread_entry));
          }
        } else {
          // put 已提交，须等完成结果返回后才能回收 entry。
          need_free = false;
        }
      } else {
        if (OB_FAIL(async_read_list_.push(preread_entry))) {
          LOG_ERROR("fail to push preread_entry into async_read_list", KR(ret), K(async_read_list_.get_curr_total()));
        } else {
          need_free = false;
        }
      }
    }

    if (need_free && OB_NOT_NULL(preread_entry)) {
      // 读、检查或提交失败时只回收 entry，保留预读节点，不新增删除语义。
      // worker 已接收的 entry 由完成结果分支回收。
      if (OB_TMP_FAIL(recycle_preread_entry(*preread_entry))) {
        LOG_WARN("fail to recycle preread_entry", KR(tmp_ret), KPC(preread_entry), K(free_list_.get_curr_total()));
        tmp_ret = OB_SUCCESS;
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next preread_entry
  }
  return ret;
}

int ObSSPreReadTask::complete_entry(
    const MacroBlockId &macro_id,
    const int64_t buf_size,
    const ObPreReadFileMeta &file_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_cache_mgr_->update_to_normal_status(macro_id, buf_size))) {
    LOG_WARN("fail to update to normal status", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_files_.push_back(file_meta))) {
    LOG_WARN("fail to push file_meta into preread_files", KR(ret), K(macro_id), K(file_meta));
  }
  return ret;
}

int ObSSPreReadTask::recycle_preread_entry(ObSSPreReadEntry &preread_entry)
{
  int ret = OB_SUCCESS;
  preread_entry.reset();
  if (OB_FAIL(free_list_.push(&preread_entry))) {
    LOG_ERROR("fail to push preread_entry into free_list", KR(ret), K(preread_entry), K(free_list_.get_curr_total()));
  }
  return ret;
}

void ObSSPreReadTask::clear_for_next_round()
{
  segment_files_.reuse();
  preread_files_.reuse();
}

void ObSSPreReadTask::set_read_cache_parallel_cnt()
{
  // default parallel is 5, max parallel is 30, each 1GB of tenant memory increase one parallel
  preread_parallel_cnt_ = MIN(DEFAULT_PRE_READ_PARALLELISM + MTL_MEM_SIZE() / GB, max_pre_read_parallelism_);
}

void ObSSPreReadTask::adjust_pre_read_task_interval()
{
  // exist unprocessed segment_file or unfinished async write io on loacl disk.
  if (OB_NOT_NULL(preread_cache_mgr_) && (preread_cache_mgr_->preread_queue_.size() > 0) &&
      (free_list_.get_curr_total() > 0)) {
    interval_us_ = FAST_SCHEDULE_INTERVAL_US;
  } else if (async_read_list_.get_curr_total() > 0 || pending_put_count_ > 0) {
    // 还有 put 结果未取回时，保持较短的调度间隔，及时回收 entry 供后续预读使用。
    interval_us_ = MODERATE_SCHEDULE_INTERVAL_US;
  } else {
    interval_us_ = SLOW_SCHEDULE_INTERVAL_US;
  }
}
} // storage
} // oceanbase
