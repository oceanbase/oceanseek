/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX CLOG
#include "ob_remote_log_writer.h"
#include "storage/tx_storage/ob_ls_service.h"           // ObLSService
#include "ob_log_restore_service.h"                     // ObLogRestoreService

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::palf;
using namespace oceanbase::storage;
using namespace share;

static bool need_retain_buffer_and_yield(const ObRestoreWriteRetryReason reason)
{
  return ObRestoreWriteRetryReason::RATE_LIMITED == reason
      || ObRestoreWriteRetryReason::DISK_FULL == reason;
}

#define GET_RESTORE_HANDLER_CTX(id)       \
  ObLS *ls = NULL;      \
  ObLSHandle ls_handle;         \
  ObLogRestoreHandler *restore_handler = NULL;       \
  if (OB_FAIL(ls_svr_->get_ls(id, ls_handle, ObLSGetMod::LOG_MOD))) {   \
    LOG_WARN("get ls failed", K(ret), K(id));     \
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {      \
    ret = OB_ERR_UNEXPECTED;       \
    LOG_INFO("get ls is NULL", K(ret), K(id));      \
  } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {     \
    ret = OB_ERR_UNEXPECTED;      \
    LOG_INFO("restore_handler is NULL", K(ret), K(id));   \
  }    \
  if (OB_SUCC(ret))

ObRemoteLogWriter::ObRemoteLogWriter() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  ls_svr_(NULL),
  restore_service_(NULL),
  worker_(NULL),
  task_cond_(ObCond::SPIN_WAIT_NUM, common::ObWaitEventIds::REMOTE_LOG_WRITER_COND_WAIT)
{}

ObRemoteLogWriter::~ObRemoteLogWriter()
{
  destroy();
}

int ObRemoteLogWriter::init(const uint64_t tenant_id,
    ObLSService *ls_svr,
    ObLogRestoreService *restore_service,
    ObRemoteFetchWorker *worker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObRemoteLogWriter init twice", K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls_svr)
      || OB_ISNULL(restore_service)
      || OB_ISNULL(worker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ls_svr), K(restore_service), K(worker));
  } else {
    tenant_id_ = tenant_id;
    ls_svr_ = ls_svr;
    restore_service_ = restore_service;
    worker_ = worker;
    inited_ = true;
  }
  return ret;
}

void ObRemoteLogWriter::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_svr_ = NULL;
  restore_service_ = NULL;
  worker_ = NULL;
  share::ObThreadPool::destroy();
  LOG_INFO("ObRemoteLogWriter destroy succ");
}

int ObRemoteLogWriter::start()
{
  int ret = OB_SUCCESS;
  share::ObThreadPool::set_run_wrapper(MTL_CTX());
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObRemoteLogWriter not init", K(ret));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    LOG_WARN("ObRemoteLogWriter start failed", K(ret));
  } else {
    LOG_INFO("ObRemoteLogWriter start succ");
  }
  return ret;
}

void ObRemoteLogWriter::stop()
{
  LOG_INFO("ObRemoteLogWriter thread stop", K_(tenant_id));
  share::ObThreadPool::stop();
}

void ObRemoteLogWriter::wait()
{
  LOG_INFO("ObRemoteLogWriter thread wait", K_(tenant_id));
  share::ObThreadPool::wait();
}

void ObRemoteLogWriter::run1()
{
  LOG_INFO("ObRemoteLogWriter thread start");
  ObDIActionGuard ag("LogService", "LogRestoreService", "RemoteLogWriter");
  // Keep distinct from fetch worker "RFLWorker" for thread-level diagnostics.
  lib::set_thread_name("RFLWriter");

  const int64_t THREAD_RUN_INTERVAL_US = 100 * 1000L;
  // The limiter keepalive uses elapsed > 50 ms, so retry after 60 ms.
  // TODO: schedule only tasks whose next_retry_ts has arrived instead of scanning every LS.
  const int64_t RATE_LIMITED_RUN_INTERVAL_US = 60 * 1000L;
  if (OB_UNLIKELY(! inited_)) {
    LOG_INFO("ObRemoteLogWriter not init");
  } else {
    while (! has_set_stop()) {
      const int64_t begin_tstamp = ObTimeUtility::current_time();
      const bool need_fast_retry = do_thread_task_();
      const int64_t end_tstamp = ObTimeUtility::current_time();
      const int64_t run_interval = need_fast_retry
          ? RATE_LIMITED_RUN_INTERVAL_US : THREAD_RUN_INTERVAL_US;
      const int64_t wait_interval = run_interval - (end_tstamp - begin_tstamp);
      if (wait_interval > 0) {
        task_cond_.timedwait(wait_interval);
      }
    }
  }
}

void ObRemoteLogWriter::notify_task()
{
  if (OB_UNLIKELY(! inited_)) {
    LOG_INFO("ObRemoteLogWriter not init");
  } else {
    task_cond_.signal();
  }
}

bool ObRemoteLogWriter::do_thread_task_()
{
  bool need_fast_retry = false;
  int ret = OB_SUCCESS;
  ObLS *ls = NULL;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  if (OB_FAIL(ls_svr_->get_ls_iter(guard, ObLSGetMod::LOG_MOD))) {
    LOG_WARN("get ls iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("iter is NULL", K(ret), K(iter));
  } else {
    while (OB_SUCC(ret) && ! has_set_stop()) {
     ls = NULL;
     if (OB_FAIL(iter->get_next(ls))) {
       if (OB_ITER_END != ret) {
         LOG_WARN("iter ls get next failed", K(ret));
       } else {
         LOG_TRACE("iter to end", K(ret));
       }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls is NULL", K(ret), K(ls));
      } else {
        const bool ls_need_fast_retry = submit_ls_tasks_(ls->get_ls_id());
        need_fast_retry = need_fast_retry || ls_need_fast_retry;
      }
    }
  }
  return need_fast_retry;
}

bool ObRemoteLogWriter::submit_ls_tasks_(const ObLSID &id)
{
  bool need_fast_retry = false;
  int ret = OB_SUCCESS;
  const int64_t MAX_TASK_COUNT_PER_LS_ROUND = 6;
  ObFetchLogTask *task = NULL;
  for (int64_t i = 0; i < MAX_TASK_COUNT_PER_LS_ROUND && OB_SUCC(ret); i++) {
    ObRestoreWriteRetryReason retry_reason = ObRestoreWriteRetryReason::NONE;
    GET_RESTORE_HANDLER_CTX(id) {
      task = NULL;
      // get task only if it is in turn
      if (OB_FAIL(restore_handler->get_next_sorted_task(task))) {
        if (OB_NOT_MASTER == ret) {
          // do nothing
          LOG_TRACE("ls not master, just skip", K(ret), K(id));
        } else {
          LOG_WARN("get sorted task failed", K(ret), K(id));
        }
      } else if (NULL == task) {
        LOG_TRACE("task is null", K(id));
        break;
      } else if (OB_FAIL(submit_entries_(*task, retry_reason))) {
        if (need_retain_buffer_and_yield(retry_reason)) {
          int tmp_ret = OB_SUCCESS;
          // the task is owned by submit_array_ once submit_sorted_task succeeds, and it may be
          // freed at any time by a concurrent ObRemoteFetchContext reset(role change or restore
          // source change), so record what to print before resubmitting it, and never touch the
          // task after the ownership is handed over
          const LSN cur_lsn = task->cur_lsn_;
          const LSN end_lsn = task->end_lsn_;
          if (ObRestoreWriteRetryReason::RATE_LIMITED == retry_reason) {
            task->mark_rate_limited();
          }
          if (OB_SUCCESS != (tmp_ret = restore_handler->submit_sorted_task(*task))) {
            // The task may be refetched or freed below; close the marker before leaving
            // the retained-buffer path.
            finish_rate_limit_wait_(*task, true /*record_elapsed*/);
            LOG_WARN("resubmit deferred task failed", K(tmp_ret), K(id), K(retry_reason), KPC(task));
          } else {
            task = NULL;
            if (ObRestoreWriteRetryReason::RATE_LIMITED == retry_reason) {
              restore_handler->record_rate_limit_yield();
              need_fast_retry = true;
            }
            LOG_TRACE("deferred task resubmitted, yield current ls", K(ret), K(id),
                K(retry_reason), K(cur_lsn), K(end_lsn));
          }
        } else if (OB_RESTORE_LOG_TO_END != ret) {
          LOG_WARN("submit_entries_ failed", K(ret), KPC(task));
        }
      }

      // try retire task, if task is consumed done or stale, free it,
      // otherwise push_back to task_queue_
      if (NULL != task) {
        ObTimeGuard time_guard("retire_task", 5 * 1000 * 1000);
        int tmp_ret = OB_SUCCESS;
        task->iter_.update_source_cb();
        if (OB_SUCCESS != (tmp_ret = try_retire_(task))) {
          LOG_WARN("retire task failed", K(tmp_ret), KP(task));
        }
      }
    }
  }
  return need_fast_retry;
}

int ObRemoteLogWriter::submit_entries_(ObFetchLogTask &task, ObRestoreWriteRetryReason &retry_reason)
{
  int ret = OB_SUCCESS;
  ipalf::IGroupEntry entry;
  const char *buf = NULL;
  LSN lsn;
  const ObLSID id = task.id_;
  const int64_t proposal_id = task.proposal_id_;
  int64_t entry_size = 0;
  LSN max_submit_lsn;
  SCN max_submit_scn;
  const uint64_t io_tenant_id = MTL_ID();
  ObObjectStorageTenantGuard object_storage_tenant_guard(
    io_tenant_id, OB_IO_MANAGER.get_object_storage_io_timeout_ms(io_tenant_id) * 1000LL);
  retry_reason = ObRestoreWriteRetryReason::NONE;
  const int64_t submit_round_start_ts = ObTimeUtility::current_time();
  const bool is_first_submit_round = OB_INVALID_TIMESTAMP == task.task_stat_.start_submit_ts_;
  if (is_first_submit_round) {
    task.task_stat_.start_submit_ts_ = submit_round_start_ts;
  }
  while (OB_SUCC(ret) && ! has_set_stop()) {
    if (OB_FAIL(task.iter_.next(entry, lsn, buf, entry_size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("ObRemoteLogIterator next failed", K(task));
      } else {
        LOG_TRACE("ObRemoteLogIterator to end", K(task.iter_));
      }
    } else if (OB_UNLIKELY(! entry.check_integrity(lsn))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("entry is invalid", K(entry), K(lsn), K(task));
    } else if (! entry.check_compatibility()) {
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        LOG_ERROR("data version is not new enough to recover clog", KR(ret));
      }
    } else if (task.cur_lsn_ > lsn) {
      LOG_INFO("repeated log, just skip", K(lsn), K(entry), K(task));
    } else if (OB_FAIL(submit_log_(id, proposal_id, lsn,
            entry.get_scn(), buf, entry_size, retry_reason))) {
      if (need_retain_buffer_and_yield(retry_reason)) {
        int tmp_ret = OB_SUCCESS;
        if (ObRestoreWriteRetryReason::DISK_FULL == retry_reason) {
          // raw_write reached PALF, so a preceding rate-limit wait has already ended even
          // though the entry could not be persisted because the disk is full.
          finish_rate_limit_wait_(task, true /*record_elapsed*/);
        }
        if (OB_SUCCESS != (tmp_ret = task.iter_.rewind(task.cur_lsn_))) {
          const int rewind_ret = tmp_ret;
          // The fetched buffer cannot be reused any more, fall back to re-fetching
          // the remaining range of this task, which is done by try_retire_ later.
          // NB: task.cur_lsn_ has not been advanced for the deferred entry, so
          // re-fetching starts just from it and no log hole is introduced.
          if (ObRestoreWriteRetryReason::RATE_LIMITED == retry_reason) {
            finish_rate_limit_wait_(task, true /*record_elapsed*/);
          }
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
            LOG_WARN("rewind deferred entry failed, fallback to refetch remaining logs",
                K(rewind_ret), K(retry_reason), K(lsn), K(task));
          }
          ret = OB_SUCCESS;
          retry_reason = ObRestoreWriteRetryReason::NONE;
          break;
        }
      } else {
        LOG_WARN("submit log failed", K(buf), K(entry), K(lsn), K(task));
      }
    } else {
      task.cur_lsn_ = lsn + entry_size;
      max_submit_lsn = lsn + entry_size;
      max_submit_scn = entry.get_scn();
      finish_rate_limit_wait_(task, true /*record_elapsed*/);
    }
  } // while

  if (OB_FAIL(ret)
      && ObRestoreWriteRetryReason::RATE_LIMITED != retry_reason
      && ObRestoreWriteRetryReason::DISK_FULL != retry_reason) {
    // PALF_BUSY means the limiter has passed. Other failures only cancel the marker so
    // role-change or refetch time is not charged to rate limiting.
    finish_rate_limit_wait_(task,
        ObRestoreWriteRetryReason::PALF_BUSY == retry_reason /*record_elapsed*/);
  }

  const int64_t submit_round_finish_ts = ObTimeUtility::current_time();
  task.task_stat_.finish_submit_ts_ = submit_round_finish_ts;
  const int64_t fetch_to_submit_time_us = is_first_submit_round
      && OB_INVALID_TIMESTAMP != task.task_stat_.finish_fetch_ts_
      && submit_round_start_ts >= task.task_stat_.finish_fetch_ts_
      ? submit_round_start_ts - task.task_stat_.finish_fetch_ts_
      : 0;
  const int64_t submit_log_time_us = submit_round_finish_ts >= submit_round_start_ts
      ? submit_round_finish_ts - submit_round_start_ts
      : 0;
  task.task_stat_.submit_log_time_us_ += submit_log_time_us;

  if (OB_ITER_END == ret) {
    if (lsn.is_valid()) {
      LOG_INFO("submit_entries_ succ", K(id), K(lsn), K(entry.get_scn()), K(task));
    }
    ret = OB_SUCCESS;
  }

  if (max_submit_lsn.is_valid() && max_submit_scn.is_valid()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(update_max_fetch_info_(id, proposal_id, max_submit_lsn, max_submit_scn))) {
      LOG_WARN("update max fetch info failed", K(id), K(proposal_id), K(max_submit_lsn), K(max_submit_scn));
    } else {
      LOG_INFO("update max fetch context succ", K(id), K(proposal_id), K(max_submit_lsn), K(max_submit_scn));
    }
  }

  // Record every writer round, including one rejected before its first entry.
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(record_submit_stat_(id, proposal_id, max_submit_scn, task.task_stat_,
      is_first_submit_round, fetch_to_submit_time_us, submit_log_time_us))) {
    if (OB_NOT_INIT == tmp_ret || OB_IN_STOP_STATE == tmp_ret || OB_NOT_MASTER == tmp_ret) {
      LOG_TRACE("skip submit stat for inactive proposal", K(tmp_ret), K(id), K(proposal_id));
    } else {
      LOG_WARN("record submit stat failed", K(tmp_ret), K(id), K(proposal_id), K(max_submit_scn),
          K(fetch_to_submit_time_us), K(submit_log_time_us));
    }
  }
  return ret;
}

int ObRemoteLogWriter::submit_log_(const ObLSID &id,
    const int64_t proposal_id,
    const LSN &lsn,
    const SCN &scn,
    const char *buf,
    const int64_t buf_size,
    ObRestoreWriteRetryReason &retry_reason)
{
  int ret = OB_SUCCESS;
  retry_reason = ObRestoreWriteRetryReason::NONE;
  GET_RESTORE_HANDLER_CTX(id) {
    if (OB_FAIL(restore_handler->raw_write(proposal_id, lsn, scn, buf, buf_size,
        ObRestoreWriteMode::YIELD_WHEN_RATE_LIMITED, retry_reason))) {
      if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        ret = OB_SUCCESS;
      } else if (OB_RESTORE_LOG_TO_END == ret) {
        // do nothing
      } else if (need_retain_buffer_and_yield(retry_reason)) {
        // Let the caller rewind and requeue the task before switching to another LS.
        if (ObRestoreWriteRetryReason::DISK_FULL == retry_reason
            && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
          LOG_WARN("disk is full, yield remote log writer", K(ret), K(id), K(lsn));
        }
      } else {
        LOG_WARN("raw write failed", K(ret), K(id), K(lsn), K(buf), K(buf_size));
      }
    }
  }
  if (OB_ERR_UNEXPECTED == ret) {
    report_error_(id, ret, lsn, ObLogRestoreErrorContext::ErrorType::SUBMIT_LOG);
  }
  return ret;
}

void ObRemoteLogWriter::finish_rate_limit_wait_(ObFetchLogTask &task, const bool record_elapsed)
{
  const int64_t wait_time_us = task.finish_rate_limit_wait();
  if (record_elapsed && wait_time_us > 0) {
    int ret = OB_SUCCESS;
    const ObLSID id = task.id_;
    GET_RESTORE_HANDLER_CTX(id) {
      restore_handler->record_rate_limit_resume(wait_time_us);
    }
    if (OB_SUCCESS != ret) {
      LOG_WARN("record rate limit resume failed", K(ret), K(id), K(wait_time_us));
    }
  }
}

int ObRemoteLogWriter::update_max_fetch_info_(const ObLSID &id,
    const int64_t proposal_id,
    const palf::LSN &lsn,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    if (OB_FAIL(restore_handler->update_max_fetch_info(proposal_id, lsn, scn))) {
      LOG_WARN("update max fetch info failed", K(id), K(proposal_id), K(lsn), K(scn));
    }
  }
  return ret;
}

int ObRemoteLogWriter::record_submit_stat_(const ObLSID &id,
    const int64_t proposal_id,
    const share::SCN &scn,
    const ObRemoteFetchTaskStat &stat,
    const bool is_first_submit_round,
    const int64_t fetch_to_submit_time_us,
    const int64_t submit_log_time_us)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    ret = restore_handler->record_submit_stat(proposal_id, scn, stat,
        is_first_submit_round, fetch_to_submit_time_us, submit_log_time_us);
  }
  return ret;
}

int ObRemoteLogWriter::try_retire_(ObFetchLogTask *&task)
{
  int ret = OB_SUCCESS;
  bool done = false;
  GET_RESTORE_HANDLER_CTX(task->id_) {
    if (OB_FAIL(restore_handler->try_retire_task(*task, done))) {
      LOG_WARN("try retire task failed", KPC(task), KPC(restore_handler));
    } else if (done) {
      inner_free_task_(*task);
      task = NULL;
      restore_service_->signal();
    } else {
      if (OB_FAIL(worker_->submit_fetch_log_task(task))) {
        LOG_ERROR("submit fetch log task failed", K(ret), KPC(task));
        inner_free_task_(*task);
        task = NULL;
      } else {
        task = NULL;
      }
    }
  } else {
    // ls not exist, just free task
    inner_free_task_(*task);
    task = NULL;
  }
  return ret;
}

void ObRemoteLogWriter::inner_free_task_(ObFetchLogTask &task)
{
  task.~ObFetchLogTask();
  mtl_free(&task);
}

void ObRemoteLogWriter::report_error_(const ObLSID &id,
                                        const int ret_code,
                                        const palf::LSN &lsn,
                                        const ObLogRestoreErrorContext::ErrorType &error_type)
{
  int ret = OB_SUCCESS;
  GET_RESTORE_HANDLER_CTX(id) {
    restore_handler->mark_error(*ObCurTraceId::get_trace_id(), ret_code, lsn, error_type);
  }
}
} // namespace logservice
} // namespace oceanbase
