/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/profile/ob_trace_id.h"
#include "ob_tablet_gc_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/ls/ob_ls.h"
#include "logservice/ob_log_service.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "src/storage/incremental/ob_shared_meta_service.h"
#endif

namespace oceanbase
{
using namespace share;
namespace storage
{
namespace checkpoint
{

// The time interval for waiting persist tablet successful after freezing is 5s
const int64_t ObTabletGCHandler::FLUSH_CHECK_INTERVAL = 5 * 1000 * 1000L;

// Periodically nudge tx-data freeze/flush while waiting for rec_scn to pass decided_scn (avoid starving other LS).
static const int64_t TX_DATA_FREEZE_NUDGE_INTERVAL_US = 10LL * 1000 * 1000L;

// The time interval for waiting persist tablet successful after freezing in total is 72 * 5s = 6m
const int64_t ObTabletGCHandler::FLUSH_CHECK_MAX_TIMES = 72;

// The time interval for checking tablet_persist_trigger_ is 5s
const int64_t ObTabletGCService::GC_CHECK_INTERVAL = 5 * 1000 * 1000L;

// The time interval for gc tablet and persist tablet whether the tablet_persist_trigger_ is 24 * 720 * 5s = 1d
const int64_t ObTabletGCService::GLOBAL_GC_CHECK_INTERVAL_TIMES = 24 * 720;

int ObTabletGCService::mtl_init(ObTabletGCService* &m)
{
  return m->init();
}

int ObTabletGCService::init()
{
  int ret = OB_SUCCESS;
  const bool is_shared_storage = GCTX.is_shared_storage_mode();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTabletGCService init twice.", KR(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage && OB_FAIL(private_block_gc_thread_.init())) {
    LOG_WARN("failed to init private_block_gc_thread", K_(private_block_gc_thread));
#endif
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTabletGCService::reload_config()
{
  int ret = OB_SUCCESS;
  const bool is_shared_storage = GCTX.is_shared_storage_mode();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTabletGCService is not initialized", KR(ret));
  } else if (!is_shared_storage) {
  } else if (OB_FAIL(private_block_gc_thread_.reload_config())) {
    STORAGE_LOG(WARN, "failed to reload private block gc thread config", KR(ret));
  }
  return ret;
}

int ObTabletGCService::start()
{
  int ret = OB_SUCCESS;
  bool is_shared_storage = GCTX.is_shared_storage_mode();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(timer_for_tablet_change_.set_run_wrapper_with_ret(MTL_CTX()))) {
    STORAGE_LOG(ERROR, "fail to set timer's run wrapper", KR(ret));
  } else if (OB_FAIL(timer_for_tablet_change_.init("TabletChange", ObMemAttr(MTL_ID(), "TabletChange")))) {
    STORAGE_LOG(ERROR, "fail to init timer", KR(ret));
  } else if (OB_FAIL(timer_for_tablet_shell_.set_run_wrapper_with_ret(MTL_CTX()))) {
    STORAGE_LOG(ERROR, "fail to set timer's run wrapper", KR(ret));
  } else if (OB_FAIL(timer_for_tablet_shell_.init("TabletShell", ObMemAttr(MTL_ID(), "TabletShell")))) {
    STORAGE_LOG(ERROR, "fail to init timer", KR(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage && OB_FAIL(timer_for_private_block_gc_.set_run_wrapper_with_ret(MTL_CTX()))) {
    STORAGE_LOG(ERROR, "fail to set timer's run wrapper", KR(ret));
  } else if (is_shared_storage && OB_FAIL(timer_for_private_block_gc_.init("PvtBlkGCTimer", ObMemAttr(MTL_ID(), "PvtBlkGCTimer")))) {
    STORAGE_LOG(ERROR, "fail to init timer", KR(ret));
#endif
  } else if (OB_FAIL(timer_for_tablet_change_.schedule(tablet_change_task_, GC_CHECK_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule task", KR(ret));
  } else if (OB_FAIL(timer_for_tablet_shell_.schedule(tablet_shell_task_, ObEmptyShellTask::GC_EMPTY_TABLET_SHELL_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule task", KR(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (is_shared_storage && FALSE_IT(private_block_gc_task_.clear_is_stopped())) {
  } else if (is_shared_storage && OB_FAIL(timer_for_private_block_gc_.schedule(private_block_gc_task_, GC_CHECK_INTERVAL, true))) {
    STORAGE_LOG(ERROR, "fail to schedule task", KR(ret));
  } else if (is_shared_storage && OB_FAIL(private_block_gc_thread_.start())) {
    STORAGE_LOG(ERROR, "fail to start private block gc thread", KR(ret), K_(private_block_gc_task));
#endif
  }
  return ret;
}

int ObTabletGCService::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTabletGCService is not initialized", KR(ret));
  } else {
    // Do NOT block here waiting for in-flight timer tasks to finish.
    // The tabletchange task may be parked inside wait_unpersist_tablet_ids_flushed
    // (waiting for mds tablet/tx data rec_scn to advance), and that loop only breaks
    // when ObTabletGCHandler::check_stop() returns true. The signal that flips it is
    // ObLSService::stop() -> ls.offline() -> tablet_gc_handler_.offline() -> set_stop(),
    // which sits at a smaller mtl_id and is reached later in the reverse stop_mtl_module
    // traversal. Inlining wait() here would deadlock the traversal. The companion wait()
    // below is invoked from wait_mtl_module after every module's stop has run.
    timer_for_tablet_change_.stop(false /*wait_running*/);
    timer_for_tablet_shell_.stop(false /*wait_running*/);
#ifdef OB_BUILD_SHARED_STORAGE
    if (GCTX.is_shared_storage_mode()) {
      if (OB_FAIL(private_block_gc_thread_.stop())) {
        STORAGE_LOG(WARN, "failed to stop private block gc thread", K(ret), K_(private_block_gc_thread));
      }
      private_block_gc_task_.set_is_stopped();
      timer_for_private_block_gc_.stop(false /*wait_running*/);
    }
#endif
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "ObTabletGCService stoped", KR(ret));
  }
  return ret;
}

void ObTabletGCService::wait()
{
  timer_for_tablet_change_.wait();
  timer_for_tablet_shell_.wait();
#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    timer_for_private_block_gc_.wait();
    private_block_gc_thread_.wait();
  }
#endif
}

void ObTabletGCService::destroy()
{
  is_inited_ = false;
  timer_for_tablet_change_.destroy();
  timer_for_tablet_shell_.destroy();
#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    timer_for_private_block_gc_.destroy();
    private_block_gc_thread_.destroy();
  }
#endif
}

void ObTabletGCService::ObTabletChangeTask::runTimerTask()
{
  STORAGE_LOG(INFO, "====== [tabletchange] timer task ======", K(GC_CHECK_INTERVAL));
  ObCurTraceId::init(GCONF.self_addr_);
  RLOCAL_STATIC(int64_t, times) = 0;
  times = (times + 1) % GLOBAL_GC_CHECK_INTERVAL_TIMES;
  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  bool skip_gc_task = false;

  skip_gc_task = (OB_SUCCESS != (OB_E(EventTable::EN_TABLET_GC_TASK_FAILED) OB_SUCCESS));

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "mtl ObLSService should not be null", KR(ret));
  } else if (OB_UNLIKELY(skip_gc_task)) {
    // do nothing
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObTabletGCHandler *tablet_gc_handler = NULL;
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "ls is NULL", KR(ret));
      } else if (ObReplicaTypeCheck::is_log_replica(ls->get_replica_type())) {
        STORAGE_LOG(TRACE, "skip logonly replica", KPC(ls));
      } else if (FALSE_IT(tablet_gc_handler = ls->get_tablet_gc_handler())) {
      } else if (tablet_gc_handler->check_stop()) {
        STORAGE_LOG(INFO, "[tabletchange] tablet_gc_handler is offline", K(ls->get_ls_id()));
      } else {
        uint8_t tablet_persist_trigger = tablet_gc_handler->get_tablet_persist_trigger_and_reset();
        STORAGE_LOG(INFO, "[tabletchange] task check ls", K(ls->get_ls_id()), K(tablet_persist_trigger));
        if (times == 0
            || ObTabletGCHandler::is_set_tablet_persist_trigger(tablet_persist_trigger)
            || ObTabletGCHandler::is_tablet_gc_trigger(tablet_persist_trigger)) {
          const bool only_persist = 0 != times && !ObTabletGCHandler::is_tablet_gc_trigger(tablet_persist_trigger);
          obsys::ObRLockGuard<> lock(tablet_gc_handler->wait_lock_);
          bool need_retry = false;
          // add temporary flag for not support persist uncommited mds data.
          bool no_need_wait_persist = false;
          SCN decided_scn;
          ObFreezer *freezer = ls->get_freezer();
          common::ObTabletIDArray unpersist_tablet_ids;
          common::ObSEArray<ObTabletHandle, 16> deleted_tablets;
          common::ObSEArray<ObTabletHandle, 16> immediately_deleted_tablets;
          const bool is_deleted = true;

          if (OB_ISNULL(freezer)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "freezer should not null", K(ls->get_ls_id()), KR(ret));
          }
          // 1. get minor merge point
          else if (OB_FAIL(ls->get_max_decided_scn(decided_scn))) {
            need_retry = true;
            STORAGE_LOG(WARN, "decide_max_decided_scn failed", KR(ret), K(freezer->get_ls_id()));
          } else if (!decided_scn.is_valid()
                     || SCN::min_scn() == decided_scn
                     || decided_scn < ls->get_tablet_change_checkpoint_scn()) {
            need_retry = true;
            STORAGE_LOG(INFO, "no any log callback and no need to update clog checkpoint",
              K(freezer->get_ls_id()), K(decided_scn), KPC(ls), K(ls->get_ls_meta()));
          }
          // 2. get gc tablet and get unpersist_tablet_ids
          else if (OB_FAIL(tablet_gc_handler->get_unpersist_tablet_ids(deleted_tablets,
                  immediately_deleted_tablets, unpersist_tablet_ids, decided_scn,
                  only_persist, need_retry, no_need_wait_persist))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to get_unpersist_tablet_ids", KPC(ls), KR(ret));
          }
          // 3. check and gc immediately_deleted_tablets
          else if (!only_persist && !immediately_deleted_tablets.empty()
                   && OB_FAIL(tablet_gc_handler->gc_tablets(
                       immediately_deleted_tablets, true /*is_immediate_gc*/))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to gc tablet", KR(ret));
          }
          // 4. freeze unpersit_tablet_ids
          else if (OB_FAIL(tablet_gc_handler->freeze_unpersist_tablet_ids(unpersist_tablet_ids, decided_scn))) {
            need_retry = true;
            STORAGE_LOG(WARN, "fail to freeze unpersist tablet", KR(ret), KPC(tablet_gc_handler->ls_), K(unpersist_tablet_ids));
          }
          // 5. wait unpersit_tablet_ids
          else if (no_need_wait_persist) {
            // temporariily skip for not support persist uncommited mds data.
            ob_usleep(ObTabletGCHandler::FLUSH_CHECK_INTERVAL, true);
          } else if(OB_FAIL(tablet_gc_handler->wait_unpersist_tablet_ids_flushed(unpersist_tablet_ids, decided_scn))) {
            need_retry = true;
            STORAGE_LOG(WARN, "fail to wait unpersist tablet ids flushed", KR(ret), KPC(tablet_gc_handler->ls_), K(unpersist_tablet_ids));
          }
	  // 6. set ls transfer scn
          else if (!only_persist && OB_FAIL(tablet_gc_handler->set_ls_transfer_scn(deleted_tablets))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to set ls transfer scn", KPC(ls), KR(ret), K(decided_scn));
          }
          // 7. update tablet_change_checkpoint in log meta
          else if (decided_scn > ls->get_tablet_change_checkpoint_scn()
                  && OB_FAIL(tablet_gc_handler->set_tablet_change_checkpoint_scn(decided_scn))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to set_tablet_change_checkpoint_scn", KPC(ls), KR(ret), K(decided_scn));
          }
          // 8. check and gc deleted_tablets
          else if (!only_persist && !deleted_tablets.empty()
                   && OB_FAIL(tablet_gc_handler->gc_tablets(
                       deleted_tablets, false /*is_immediate_gc*/))) {
            need_retry = true;
            STORAGE_LOG(WARN, "failed to gc tablet", KR(ret));
          }
          STORAGE_LOG(INFO, "[tabletchange] tablet in a ls persist and gc process end", KR(ret), KPC(ls), K(decided_scn), K(unpersist_tablet_ids));
          if (need_retry) {
            STORAGE_LOG(INFO, "[tabletchange] persist or gc error, need try", KR(ret), KPC(ls), K(decided_scn), K(tablet_persist_trigger));
            if (ObTabletGCHandler::is_set_tablet_persist_trigger(tablet_persist_trigger)) {
              tablet_gc_handler->set_tablet_persist_trigger();
            }
            if (ObTabletGCHandler::is_tablet_gc_trigger(tablet_persist_trigger)) {
              tablet_gc_handler->set_tablet_gc_trigger();
            }
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        STORAGE_LOG(INFO, "[tabletchange] succeed to gc_tablet", KR(ret), K(ls_cnt), K(times));
      } else {
        STORAGE_LOG(INFO, "[tabletchange] no logstream", KR(ret), K(ls_cnt), K(times));
      }
    }
  }
}

int ObTabletGCHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTabletGCHandler init twice", KR(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret));
  } else {
    ls_id_ = ls->get_ls_id();
    ls_ = ls;
    set_stop();
    is_inited_ = true;
  }
  return ret;
}

void ObTabletGCHandler::set_tablet_persist_trigger()
{
  uint8_t old_v = 0;
  uint8_t new_v = 0;
  do {
    old_v = ATOMIC_LOAD(&tablet_persist_trigger_);
    new_v = old_v | 1;
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&tablet_persist_trigger_, old_v, new_v) != old_v);
  STORAGE_LOG(INFO, "set_tablet_persist_trigger", KPC(this));
}

void ObTabletGCHandler::set_tablet_gc_trigger()
{
  uint8_t old_v = 0;
  uint8_t new_v = 0;
  do {
    old_v = ATOMIC_LOAD(&tablet_persist_trigger_);
    new_v = old_v | 2;
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&tablet_persist_trigger_, old_v, new_v) != old_v);
  STORAGE_LOG(INFO, "set_tablet_gc_trigger", KPC(this));
}

uint8_t ObTabletGCHandler::get_tablet_persist_trigger_and_reset()
{
  uint8_t old_v = 0;
  uint8_t new_v = 0;
  do {
    old_v = ATOMIC_LOAD(&tablet_persist_trigger_);
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&tablet_persist_trigger_, old_v, new_v) != old_v);
  return old_v;
}

int ObTabletGCHandler::disable_gc()
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = get_gc_lock_abs_timeout();
  if (OB_FAIL(gc_rwlock_.rdlock(abs_timeout_us))) {
    ret = OB_TABLET_GC_LOCK_CONFLICT;
    LOG_WARN("try lock failed, please retry later", KPC(ls_), K(ret));
  } else if (check_stop()) {
    gc_rwlock_.rdunlock();
    ret = OB_NOT_RUNNING;
    LOG_WARN("gc handler has already been offline", K(ret));
  } else {
    FLOG_INFO("disable tablet gc success", KPC(ls_), K(lbt()));
  }

  return ret;
}

void ObTabletGCHandler::enable_gc()
{
  gc_rwlock_.rdunlock();
  FLOG_INFO("enable tablet gc success", KPC(ls_), K(lbt()));
}

int ObTabletGCHandler::set_tablet_change_checkpoint_scn(const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = get_gc_lock_abs_timeout();
  if (OB_FAIL(gc_rwlock_.wrlock(abs_timeout_us))) {
    ret = OB_TABLET_GC_LOCK_CONFLICT;
    LOG_WARN("try lock failed, please retry later", K(ls_id_), K(ret));
  } else {
    if (OB_FAIL(ls_->set_tablet_change_checkpoint_scn(scn))) {
      LOG_WARN("fail to set tablet_change_checkpoint_scn", K(ret), K(ls_id_), K(scn));
    } else {
      // do nothing
    }
    gc_rwlock_.wrunlock();
  }
  return ret;
}

bool ObTabletGCHandler::is_tablet_gc_trigger_and_reset()
{
  uint8_t old_v = 0;
  uint8_t new_v = 0;
  do {
    old_v = ATOMIC_LOAD(&tablet_persist_trigger_);
    new_v = old_v & (~2);
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&tablet_persist_trigger_, old_v, new_v) != old_v);
  return 0 != (old_v & 2);
}

int ObTabletGCHandler::check_tablet_need_persist_(
    ObTabletHandle &tablet_handle,
    const SCN &decided_scn,
    bool &need_persist,
    bool &need_retry,
    bool &no_need_wait_persist)
{
  int ret = OB_SUCCESS;
  need_persist = false;
  ObTablet *tablet = NULL;
  bool is_locked = false;
  SCN rec_scn;
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet is NULL", KR(ret));
  } else if (OB_FAIL(tablet->is_locked_by_others<ObTabletCreateDeleteMdsUserData>(is_locked))) {
    LOG_WARN("failed to get is locked", KR(ret), KPC(tablet));
  } else if (is_locked) {
    LOG_INFO("tablet_status is changing", KR(ret), KPC(tablet));
    no_need_wait_persist = true;
    need_retry = true;
  }

  if (OB_FAIL(ret)) {
  } else if (tablet->is_empty_shell()) {
  } else if (FALSE_IT(rec_scn = tablet->get_persistence_rec_scn())) {
  } else if (rec_scn > decided_scn) {
    // todo check tablet_status and binding_info scn to mds_checkpoint_scn
  } else {
    need_persist = true;
    STORAGE_LOG(INFO, "[tabletgc] get tablet for persist", K(ret), K(ls_id_), K(rec_scn), KPC(tablet));
  }

  return ret;
}

int ObTabletGCHandler::check_tablet_need_gc_(
    ObTabletHandle &tablet_handle,
    TabletGCStatus &need_gc,
    bool &need_retry,
    const SCN &decided_scn)
{
  int ret = OB_SUCCESS;
  need_gc = TabletGCStatus::NOT_NEED_GC;
  ObTablet *tablet = tablet_handle.get_obj();
  const common::ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet is NULL", KR(ret));
  // for tablet shell
  } else if (tablet->is_empty_shell()) {
    share::SCN delete_commit_scn = tablet->get_tablet_meta().empty_shell_tablet_status_.delete_commit_scn_;
    if (!delete_commit_scn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "deleted_commit_scn is unvalid", KR(ret), KPC(tablet));
    } else if (delete_commit_scn > decided_scn) {
      need_retry = true;
      LOG_INFO("tablet cannot be gc, as delete_commit_scn > decided_scn, retry", KR(ret), K_(ls_id),KPC(tablet), K(decided_scn));
    } else {
      need_gc = TabletGCStatus::NEED_GC_AFTER_MDS_PERSIST;
      LOG_TRACE("tablet is shell, need gc", K_(ls_id), K(tablet_id), K(need_gc),
          K(delete_commit_scn), K(decided_scn),
          "transfer_scn", tablet->get_reorganization_scn(),
          "transfer_seq", tablet->get_transfer_seq(),
          "tablet_addr", tablet->get_tablet_addr());
    }
  } else {
    // for create tablet abort
    ObTabletCreateDeleteMdsUserData data;
    mds::MdsWriter writer;
    mds::TwoPhaseCommitState trans_stat;
    share::SCN trans_version;
    bool status_is_written = false;
    if (OB_FAIL(tablet->check_tablet_status_written(status_is_written))) {
      STORAGE_LOG(WARN, "failed to check mds written", KR(ret), KPC(tablet));
    } else if (OB_FAIL(tablet->get_latest_tablet_status(data, writer, trans_stat, trans_version))) {
      if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
        if (!status_is_written) {
          share::SCN mds_start_scn = tablet->get_tablet_meta().mds_start_scn_;
          if (mds_start_scn.is_valid_and_not_min()) {
            // the tablet already has ON_REDO callback, settings must completed
            status_is_written = mds_start_scn <= decided_scn;
            STORAGE_LOG(INFO, "tablet has ON_REDO callback", K(ret), K_(ls_id), K(tablet_id), K(mds_start_scn), K(decided_scn), K(status_is_written));
          } else {
            STORAGE_LOG(INFO, "tablet status has not been written", K(ret), K_(ls_id), K(tablet_id), KP(tablet), K(need_gc));
          }
        }
        if (!status_is_written) {
          LOG_INFO("tablet status has not been written, decide later", K(ret), K_(ls_id), K(tablet_id));
        } else if (OB_FAIL(check_tablet_from_aborted_tx(*tablet, need_gc))) {
          STORAGE_LOG(WARN, "failed to check tablet from aborted tx", K(ret), K_(ls_id), K(tablet_id));
        }
      } else {
        STORAGE_LOG(WARN, "failed to get CreateDeleteMdsUserData", KR(ret), KPC(tablet));
      }
    } else if (trans_stat != mds::TwoPhaseCommitState::ON_COMMIT) {
      need_retry = true;
    }
  }
  return ret;
}

int ObTabletGCHandler::check_tablet_from_aborted_tx(const ObTablet &tablet, TabletGCStatus &gc_status)
{
  int ret = OB_SUCCESS;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  share::SCN rec_scn;
  if (tablet.get_tablet_meta().has_transfer_table()) {
    gc_status = TabletGCStatus::NEED_GC_IMMEDIATELY;
    STORAGE_LOG(INFO, "tablet has transfer table, should delete tablet instantly",
        K(ret), K_(ls_id), K(tablet_id), "transfer_info", tablet.get_tablet_meta().transfer_info_);
  } else if (FALSE_IT(rec_scn = tablet.get_tablet_meta().mds_start_scn_)) {
  } else if (rec_scn.is_min()) {
    gc_status = TabletGCStatus::NEED_GC_IMMEDIATELY;
    STORAGE_LOG(INFO, "tablet persistence rec scn is MIN, redo log has NOT been written, should delete tablet instantly",
        K(ret), K_(ls_id), K(tablet_id), K(gc_status), K(rec_scn));
  } else {
    gc_status = TabletGCStatus::NOT_NEED_GC;
    STORAGE_LOG(INFO, "tablet persistence rec scn is NOT MAX, redo log has been written, try to convert to empty shell",
        K(ret), K_(ls_id), K(tablet_id), K(rec_scn), K(gc_status));
  }

  return ret;
}

int ObTabletGCHandler::get_unpersist_tablet_ids(common::ObIArray<ObTabletHandle> &deleted_tablets,
                                                common::ObIArray<ObTabletHandle> &immediately_deleted_tablets,
                                                common::ObTabletIDArray &unpersist_tablet_ids,
                                                const SCN &decided_scn,
                                                const bool only_persist,
                                                bool &need_retry,
                                                bool &no_need_wait_persist)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  int64_t scanned_tablet_count = 0;
  int64_t empty_shell_count = 0;
  int64_t gc_after_persist_candidate_count = 0;
  int64_t immediate_gc_candidate_count = 0;
  int64_t persist_candidate_count = 0;
  int64_t scan_retry_count = 0;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handle is not inited", KR(ret));
  } else if (OB_FAIL(ls_->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    STORAGE_LOG(WARN, "failed to build ls tablet iter", KR(ret), KPC(this));
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = NULL;
    TabletGCStatus need_gc = TabletGCStatus::NOT_NEED_GC;
    bool need_persist = false;
    while (OB_SUCC(ret)) {
      bool tablet_need_retry = false;
      if (check_stop()) {
        ret = OB_EAGAIN;
        STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), K(tablet_handle), KPC(ls_), K(ls_->get_ls_meta()));
      } else if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "failed to get tablet", KR(ret), KPC(this), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid tablet handle", KR(ret), KPC(this), K(tablet_handle));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet is NULL", KR(ret));
      } else if (tablet->is_ls_inner_tablet()) {
        // skip ls inner tablet
        continue;
      } else {
        ++scanned_tablet_count;
        if (tablet->is_empty_shell()) {
          ++empty_shell_count;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (only_persist) {
      } else if (OB_FAIL(check_tablet_need_gc_(tablet_handle, need_gc, tablet_need_retry, decided_scn))) {
        STORAGE_LOG(WARN, "failed to check_tablet_need_gc_", KR(ret), KPC(tablet));
      } else if (TabletGCStatus::NOT_NEED_GC == need_gc) {
      } else if (TabletGCStatus::NEED_GC_AFTER_MDS_PERSIST == need_gc
          && OB_FAIL(deleted_tablets.push_back(tablet_handle))) {
        STORAGE_LOG(WARN, "failed to push_back", KR(ret));
      } else if (TabletGCStatus::NEED_GC_IMMEDIATELY == need_gc
          && OB_FAIL(immediately_deleted_tablets.push_back(tablet_handle))) {
        STORAGE_LOG(WARN, "failed to push_back", KR(ret));
      } else {
        if (TabletGCStatus::NEED_GC_AFTER_MDS_PERSIST == need_gc) {
          ++gc_after_persist_candidate_count;
        } else if (TabletGCStatus::NEED_GC_IMMEDIATELY == need_gc) {
          ++immediate_gc_candidate_count;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!only_persist && TabletGCStatus::NEED_GC_IMMEDIATELY == need_gc) {
        // 该 tablet 会在持久化前删除，避免遗留 ID 命中同 ID 的新实例。
      } else if (OB_FAIL(check_tablet_need_persist_(tablet_handle, decided_scn, need_persist,
              tablet_need_retry, no_need_wait_persist))) {
        STORAGE_LOG(WARN, "failed to check_tablet_need_persist_", KR(ret), KPC(tablet));
      } else if (!need_persist) {
      } else if (OB_FAIL(unpersist_tablet_ids.push_back(tablet->get_tablet_meta().tablet_id_))) {
        STORAGE_LOG(WARN, "failed to push_back", KR(ret));
      } else {
        ++persist_candidate_count;
        STORAGE_LOG(INFO, "[tabletgc] get tablet for persist", KPC(tablet), K(decided_scn));
      }

      if (tablet_need_retry) {
        need_retry = true;
        ++scan_retry_count;
      }
    }
  }
  const int64_t scan_cost_us = ObTimeUtility::fast_current_time() - start_ts;
  STORAGE_LOG(INFO, "[tabletgc] scan tablet finished", KR(ret), K_(ls_id),
      K(scanned_tablet_count), K(empty_shell_count), K(gc_after_persist_candidate_count),
      K(immediate_gc_candidate_count), K(persist_candidate_count), K(scan_retry_count),
      K(decided_scn), K(scan_cost_us));
  return ret;
}

int ObTabletGCHandler::flush_unpersist_tablet_ids(const common::ObTabletIDArray &unpersist_tablet_ids,
                                                  const SCN &decided_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handle is not inited", KR(ret));
  } else if (OB_FAIL(freeze_unpersist_tablet_ids(unpersist_tablet_ids, decided_scn))) {
    STORAGE_LOG(WARN, "fail to freeze unpersist tablet", KR(ret), K_(ls_id), K(unpersist_tablet_ids));
  } else if (OB_FAIL(wait_unpersist_tablet_ids_flushed(unpersist_tablet_ids, decided_scn))) {
    STORAGE_LOG(WARN, "fail to wait unpersist tablet ids flushed", KR(ret), K_(ls_id), K(unpersist_tablet_ids));
  }
  return ret;
}

int ObTabletGCHandler::freeze_unpersist_tablet_ids(const common::ObTabletIDArray &unpersist_tablet_ids,
                                                   const SCN &decided_scn)
{
  // 1. flush all created and in memory tablets
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handle is not inited", KR(ret));
  } else {
    DEBUG_SYNC(BEFORE_TABLET_MDS_FLUSH);
    // freeze all tablets
    for (int64_t i = 0; i < unpersist_tablet_ids.count() && OB_SUCC(ret); i++) {
      if (!unpersist_tablet_ids.at(i).is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid tablet_id", KR(ret), KPC(this->ls_), K(unpersist_tablet_ids));
      } else {
        ObTabletHandle persist_tablet_handle;
        const common::ObTabletID &tablet_id = unpersist_tablet_ids.at(i);
        if (OB_FAIL(ls_->get_tablet_svr()->persist_if_in_memory(tablet_id, persist_tablet_handle))) {
          STORAGE_LOG(WARN, "failed to persist tablet", KR(ret), KPC(this->ls_), K(tablet_id));
        }
      }
    }
  }
  // 2. flush all mds data to decided_scn
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ls_->get_mds_table()->flush(decided_scn, 1 /*trace_id*/, true /*is_sync*/))) {
    STORAGE_LOG(WARN, "failed to flush mds table", KR(ret), K_(ls_id), K(decided_scn));
  }
  const int64_t end_ts = ObTimeUtility::fast_current_time();
  const int64_t cost = end_ts - start_ts;
  STORAGE_LOG(INFO, "[tabletgc] freeze unpersist_tablet_ids", KR(ret), K_(ls_id), K(unpersist_tablet_ids), K(unpersist_tablet_ids.count()), K(cost));
  return ret;
}

ERRSIM_POINT_DEF(EN_GC_WAIT_FLUSH_RETRY_TIMES);
int ObTabletGCHandler::wait_unpersist_tablet_ids_flushed(const common::ObTabletIDArray &unpersist_tablet_ids,
                                                         const SCN &decided_scn)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  int64_t retry_times = FLUSH_CHECK_MAX_TIMES;

#ifdef ERRSIM
  retry_times = EN_GC_WAIT_FLUSH_RETRY_TIMES ? 10 : FLUSH_CHECK_MAX_TIMES;
#endif

  int64_t i = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handle is not inited", KR(ret));
  } else if (check_stop()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  }

  // wait all tablet flushed
  while (unpersist_tablet_ids.count() > i && retry_times > 0 && OB_SUCC(ret)) {
    ob_usleep(FLUSH_CHECK_INTERVAL, true);
    while (unpersist_tablet_ids.count() > i && OB_SUCC(ret)) {
      ObTabletHandle handle;
      ObTablet *tablet = nullptr;
      SCN rec_scn = SCN::min_scn();
      ObTabletID tablet_id = unpersist_tablet_ids.at(i);
      if (check_stop()) {
        ret = OB_EAGAIN;
        STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this));
      } else if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_id, handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        STORAGE_LOG(WARN, "fail to get tablet", KR(ret), K(i), K(tablet_id));
      } else if (OB_ISNULL(tablet = handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet is NULL", KR(ret), K(i), K(tablet_id));
      } else {
        rec_scn = tablet->get_persistence_rec_scn();
      }

      if (OB_FAIL(ret)) {
      } else if (rec_scn > decided_scn) {
        STORAGE_LOG(INFO, "[tabletgc] finish tablet flush", K(rec_scn), K(decided_scn),
                    K(retry_times), K(i), K_(ls_id), K(tablet_id));
        i++;
      } else {
        STORAGE_LOG(INFO, "[tabletgc] wait tablet flush", K(rec_scn), K(decided_scn),
                    K(retry_times), K(i), K_(ls_id), K(tablet_id));
        break;
      }
    }
    retry_times--;
  }

  if (OB_SUCC(ret) && i != unpersist_tablet_ids.count()) {
    ret = OB_TIMEOUT;
    STORAGE_LOG(WARN, "flush tablet timeout", KR(ret), K(retry_times), K(i), K_(ls_id), K(unpersist_tablet_ids));
  }

  bool mds_tablet_flush_finished = false;
  while (!mds_tablet_flush_finished && OB_SUCC(ret)) {
    share::SCN mds_tablet_rec_scn = ls_->get_mds_table()->get_rec_scn();
    if (check_stop()) {
      ret = OB_EAGAIN;
      STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
    } else if (mds_tablet_rec_scn > decided_scn) {
      STORAGE_LOG(INFO, "[tabletgc] finish mds tablet flush", K(mds_tablet_rec_scn), K(decided_scn),
                  K(retry_times), K(i), K(ls_->get_ls_id()));
      mds_tablet_flush_finished = true;
    } else {
      STORAGE_LOG(INFO, "[tabletgc] wait mds tablet flush", K(mds_tablet_rec_scn), K(decided_scn),
                  K(retry_times), K(i), K(ls_->get_ls_id()));      
      ob_usleep(FLUSH_CHECK_INTERVAL, true);
    }
  }
  bool tx_data_flush_finished = false;
  int64_t last_tx_data_freeze_nudge_ts = 0;
  while (!tx_data_flush_finished && OB_SUCC(ret)) {
    share::SCN tx_data_rec_scn = ls_->get_tx_table()->get_tx_data_table()->get_rec_scn();
    if (check_stop()) {
      ret = OB_EAGAIN;
      STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
    } else if (tx_data_rec_scn > decided_scn) {
      STORAGE_LOG(INFO, "[tabletgc] finish tx data flush", K(tx_data_rec_scn), K(decided_scn), K(retry_times), K(i), K_(ls_id));
      tx_data_flush_finished = true;
    } else {
      const int64_t now_ts = ObTimeUtility::fast_current_time();
      if (0 == last_tx_data_freeze_nudge_ts
          || now_ts - last_tx_data_freeze_nudge_ts >= TX_DATA_FREEZE_NUDGE_INTERVAL_US) {
        last_tx_data_freeze_nudge_ts = now_ts;
        ObLSTxService *tx_svr = ls_->get_tx_svr();
        if (OB_NOT_NULL(tx_svr)) {
          int tmp_ret = tx_svr->flush_ls_inner_tablet(common::LS_TX_DATA_TABLET, decided_scn, *ObCurTraceId::get());
          if (OB_SUCCESS != tmp_ret && OB_EAGAIN != tmp_ret && OB_SIZE_OVERFLOW != tmp_ret) {
            STORAGE_LOG(WARN, "[tabletgc] nudge tx data freeze/flush failed", K(tmp_ret), K_(ls_id));
          }
        }
      }
      STORAGE_LOG(INFO, "[tabletgc] wait tx data flush", K(tx_data_rec_scn), K(decided_scn), K(retry_times), K(i), K_(ls_id));      
      ob_usleep(FLUSH_CHECK_INTERVAL, true);
    }
  }

  const int64_t end_ts = ObTimeUtility::fast_current_time();
  const int64_t cost = end_ts - start_ts;
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "[tabletgc] flush tablet finish", KR(ret), K_(ls_id), K(unpersist_tablet_ids), K(unpersist_tablet_ids.count()), K(cost));
  }
  return ret;
}

int ObTabletGCHandler::gc_tablets(
    const common::ObIArray<ObTabletHandle> &deleted_tablets,
    const bool is_immediate_gc)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_->get_ls_id();
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  int64_t gc_success_count = 0;
  int64_t gc_retry_count = 0;
  int64_t gc_failure_count = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handler is not inited", KR(ret));
  } else {
    DEBUG_SYNC(BEFORE_TABLET_GC);

    bool need_retry = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_tablets.count(); ++i) {
      const ObTabletHandle &tablet_handle = deleted_tablets.at(i);
      const ObTabletID tablet_id = tablet_handle.get_obj()->get_tablet_meta().tablet_id_;
      if (check_stop()) {
        ret = OB_EAGAIN;
        ++gc_retry_count;
        STORAGE_LOG(INFO, "tablet gc handler stop", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
#ifdef OB_BUILD_SHARED_STORAGE
      } else if (GCTX.is_shared_storage_mode()) {
        ObTablet *tablet = tablet_handle.get_obj();
        if (tablet->is_empty_shell()) {
          const share::SCN transfer_scn = tablet->get_reorganization_scn();
          const share::SCN deleted_scn = tablet->get_tablet_meta().empty_shell_tablet_status_.delete_commit_scn_;
          if (OB_FAIL(MTL(ObSSMetaService *)->mark_tablet_deleted(ls_id, tablet_id, transfer_scn, deleted_scn))) {
            if (OB_OBJECT_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              STORAGE_LOG(INFO, "tablet_meta has been gc, or create tablet abort", KR(ret), KPC(this), KPC(ls_),
                          K(tablet_id), K(transfer_scn));
#ifdef ERRSIM
              SERVER_EVENT_SYNC_ADD("tablet_gc", "create_tablet_abort",
                                    "tenant_id", MTL_ID(),
                                    "ls_id", ls_id.id(),
                                    "tablet_id", tablet_id.id());
#endif
            } else {
              ++gc_failure_count;
              STORAGE_LOG(WARN, "failed to mark_tablet_deleted", KR(ret), KPC(this), KPC(ls_), K(tablet_id), K(transfer_scn));
            }
          }
        } else {
          STORAGE_LOG(INFO, "tablet is not empty shell, skip SS mark delete", KR(ret), K(ls_id), K(tablet_id), KPC(tablet));
        }
      }
      if (OB_FAIL(ret)) {
#endif
      } else if (OB_FAIL(ls_->get_tablet_svr()->remove_tablet(tablet_handle))) {
        if (OB_EAGAIN == ret) {
          need_retry = true;
          ++gc_retry_count;
          ret = OB_SUCCESS;
        } else {
          ++gc_failure_count;
          STORAGE_LOG(WARN, "failed to remove tablet", K(ret), K(tablet_handle));
        }
      } else {
        ++gc_success_count;
#ifdef ERRSIM
        SERVER_EVENT_SYNC_ADD("tablet_gc", "gc_tablet_finish",
                              "ls_id", ls_id.id(),
                              "tablet_id", tablet_id.id(),
                              "transfer_seq", tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_);
#endif
        FLOG_INFO("[TabletGC] gc tablet finish", K(ret), K(ls_id), K(tablet_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (need_retry) {
      ret = OB_EAGAIN;
    }
  }
  const int64_t gc_cost_us = ObTimeUtility::fast_current_time() - start_ts;
  const int64_t unprocessed_tablet_count = deleted_tablets.count()
      - gc_success_count - gc_retry_count - gc_failure_count;
  STORAGE_LOG(INFO, "[tabletgc] gc tablets finished", KR(ret), K(ls_id), K(is_immediate_gc),
      "input_tablet_count", deleted_tablets.count(), K(gc_success_count), K(gc_retry_count),
      K(gc_failure_count), K(unprocessed_tablet_count), K(gc_cost_us));
  return ret;
}

int ObTabletGCHandler::get_max_tablet_transfer_scn(
    const common::ObIArray<ObTabletHandle> &deleted_tablets,
    share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  const bool need_initial_state = false;
  const bool need_sorted_tablet_id = false;
  ObHALSTabletIDIterator iter(ls_->get_ls_id(), need_initial_state, need_sorted_tablet_id);
  share::SCN max_transfer_scn = share::SCN::min_scn();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tablet gc handler is not inited", KR(ret));
  } else {
    ObTabletCreateDeleteMdsUserData mds_data;
    ObTablet *tablet = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < deleted_tablets.count(); i++) {
      mds_data.reset();
      tablet = deleted_tablets.at(i).get_obj();
      if (OB_ISNULL(tablet)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is null", K(ret), K(i), "tablet_handle", deleted_tablets.at(i));
      } else if (OB_FAIL(tablet->get_latest_committed_tablet_status(mds_data))) {
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("create tablet abort, need gc", K(ret),
              "ls_id", tablet->get_tablet_meta().ls_id_,
              "tablet_id", tablet->get_tablet_meta().tablet_id_);
          continue;
        } else {
          LOG_WARN("failed to get mds table", KR(ret),
              "ls_id", tablet->get_tablet_meta().ls_id_,
              "tablet_id", tablet->get_tablet_meta().tablet_id_);
        }
      } else if (share::SCN::invalid_scn() == mds_data.transfer_scn_) {
        // do nothing
      } else {
        transfer_scn = mds_data.transfer_scn_;
        max_transfer_scn = MAX(transfer_scn, max_transfer_scn);
      }
    }
    if (OB_SUCC(ret)) {
      transfer_scn = max_transfer_scn;
    }
  }
  return ret;
}

int ObTabletGCHandler::set_ls_transfer_scn(const common::ObIArray<ObTabletHandle> &deleted_tablets)
{
  int ret = OB_SUCCESS;
  share::SCN tablet_max_transfer_scn;
  if (0 == deleted_tablets.count()) {
    // do nothing
  } else if (OB_FAIL(get_max_tablet_transfer_scn(deleted_tablets, tablet_max_transfer_scn))) {
    STORAGE_LOG(WARN, "failed to get max tablet transfer scn", K(ret));
  } else if (OB_FAIL(ls_->inc_update_transfer_scn(tablet_max_transfer_scn))) {
    LOG_WARN("failed to set transfer scn", K(ret));
  }
  return ret;
}

int ObTabletGCHandler::offline()
{
  int ret = OB_SUCCESS;
  const int64_t abs_timeout_us = get_gc_lock_abs_timeout();
  set_stop();
  if (!is_finish()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "tablet gc handler not finish, retry", KR(ret), KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  } else if (OB_FAIL(gc_rwlock_.wrlock(abs_timeout_us))) {
    // make sure 'gc_rwlock_' is not using.
    ret = OB_TABLET_GC_LOCK_CONFLICT;
    LOG_WARN("tablet gc handler not finish, retry", KPC(ls_), K(ret));
  } else {
    gc_rwlock_.wrunlock();
    STORAGE_LOG(INFO, "tablet gc handler offline", KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
  }
  return ret;
}

void ObTabletGCHandler::online()
{
  set_tablet_persist_trigger();
  set_tablet_gc_trigger();
  set_start();
  STORAGE_LOG(INFO, "tablet gc handler online", KPC(this), KPC(ls_), K(ls_->get_ls_meta()));
}

int64_t ObTabletGCHandler::get_gc_lock_abs_timeout() const
{
  const int64_t abs_timeout_us = GC_LOCK_TIMEOUT + ObTimeUtility::current_time();
  return abs_timeout_us;
}
} // checkpoint
} // storage
} // oceanbase
