/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/ob_running_mode.h"
#include "ob_private_block_gc_task.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "storage/meta_store/ob_storage_meta_io_util.h"
#include "src/storage/shared_storage/ob_dir_manager.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/ob_debug_sync.h" // DEBUG_SYNC

namespace oceanbase
{
namespace storage
{
// The time interval for checking block gc trigger is 5s
const int64_t ObLSPrivateBlockGCHandler::PRIVATE_BLOCK_GC_INTERVAL = 5 * 1000 * 1000L;

// The time interval for block gc check is 24 * 720 * 5s = 1d
const int64_t ObPrivateBlockGCTask::GLOBAL_PRIVATE_BLOCK_GC_INTERVAL = 1 * 24 * 60 * 60 * 1000 * 1000L;

// The time interval for tablet gc is 0 (safe time discard temporarily)
const int64_t ObLSPrivateBlockGCHandler::DEFAULT_PRIVATE_TABLET_GC_SAFE_TIME = 0;

int ObPrivateBlockGCThread::start()
{
  int ret = OB_SUCCESS;
  int64_t thread_count = 0;
  if (OB_FAIL(ObBlockGCThread<ObPrivateBlockGCThreadGuard>::start())) {
    STORAGE_LOG(WARN, "failed to start private block gc thread", KR(ret));
  } else if (OB_FAIL(get_target_thread_count_(thread_count))) {
    STORAGE_LOG(WARN, "failed to get target private block gc thread count", KR(ret));
  } else if (OB_FAIL(TG_SET_THREAD_CNT(get_tg_id(), thread_count))) {
    STORAGE_LOG(WARN, "failed to set private block gc thread count", KR(ret), K(thread_count));
  }
  return ret;
}

int ObPrivateBlockGCThread::reload_config()
{
  int ret = OB_SUCCESS;
  int64_t thread_count = 0;
  if (OB_FAIL(get_target_thread_count_(thread_count))) {
    STORAGE_LOG(WARN, "failed to get target private block gc thread count", KR(ret));
  } else {
    const int64_t old_thread_count = TG_GET_THREAD_CNT(get_tg_id());
    if (old_thread_count != thread_count) {
      if (OB_FAIL(TG_SET_THREAD_CNT(get_tg_id(), thread_count))) {
        STORAGE_LOG(WARN, "failed to set private block gc thread count",
            KR(ret), K(old_thread_count), K(thread_count));
      } else {
        STORAGE_LOG(INFO, "private block gc thread count updated",
            K(old_thread_count), K(thread_count));
      }
    }
  }
  return ret;
}

int ObPrivateBlockGCThread::get_target_thread_count_(int64_t &thread_count)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tenant config is invalid", KR(ret));
  } else {
    const int64_t config_thread_count = tenant_config->_private_gc_thread_count;
    const int64_t default_thread_count = lib::is_mini_mode()
        ? MINI_MODE_DEFAULT_THREAD_NUM
        : DEFAULT_THREAD_NUM;
    thread_count = (0 == config_thread_count) ? default_thread_count : config_thread_count;
  }
  return ret;
}

void ObPrivateBlockGCTask::runTimerTask()
{
  LOG_INFO("====== private block gc ======", K(ObLSPrivateBlockGCHandler::PRIVATE_BLOCK_GC_INTERVAL), KPC(this));
  loop_process_tablet_infos();
  loop_process_failed_macro_blocks(); 
  loop_check_tablet_gc();
  loop_check_tablet_version_gc();
  observer_start_macro_block_check();
  loop_check_ls_gc(); 
}

void ObPrivateBlockGCTask::observer_start_macro_block_check()
{
  int ret = OB_SUCCESS;
  const bool observer_start_macro_block_trigger = ATOMIC_LOAD(&observer_start_macro_block_trigger_);
  if (observer_start_macro_block_trigger) {
    LOG_INFO("====== observer start macro block check ======", K(ObLSPrivateBlockGCHandler::PRIVATE_BLOCK_GC_INTERVAL), KPC(this));

    ObLSIterator *iter = NULL;
    common::ObSharedGuard<ObLSIterator> guard;
    ObLSService *ls_svr = MTL(ObLSService*); 
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
      LOG_WARN("get log stream iter failed", KR(ret));
    } else if (OB_ISNULL(iter = guard.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is NULL", KR(ret));
    } else {
      ObLS *ls = NULL;
      int ls_cnt = 0;
      for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
        if (OB_ISNULL(ls)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls is NULL", KR(ret));
        } else {
          LOG_INFO("private block loop check", K(ls->get_ls_id()));
          ObLSPrivateBlockGCHandler &handler = ls->get_ls_private_block_gc_handler();
          obsys::ObRLockGuard<> lock(handler.wait_lock_);
          if (!handler.check_stop()) {
            handler.ls_macro_block_check();
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        if (ls_cnt > 0) {
          LOG_INFO("succeed to gc block", KR(ret), K(ls_cnt));
        } else {
          LOG_INFO("no logstream", KR(ret), K(ls_cnt));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ATOMIC_STORE(&observer_start_macro_block_trigger_, false);
    }
  }
}

int ObPrivateBlockGCTask::ls_gc(
    const ObLSItem &ls_item,
    const bool no_delete_tablet)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = ls_item.ls_id_;
  const int64_t ls_epoch = ls_item.epoch_;
  ObArray<ObPendingFreeTabletItem> gc_items;
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  int gc_macro_block_cnt = 0;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  ObPrivateBlockGCThread *gc_thread = NULL;
  bool all_tablet_deleted = false;
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_id), K(ls_epoch));
  } else if (FALSE_IT(gc_thread = gc_service->get_private_block_gc_thread())) {
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is not valid", K(ret), K(ls_id), K(ls_epoch));
  } else if (no_delete_tablet) {
  } else if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.get_wait_gc_tablet_items(ls_id, ls_epoch, gc_items))) {
    LOG_WARN("failed to get wait gc tablet items", K(ret), K(ls_id), K(ls_epoch));
  } else if (gc_items.count() > 0) {
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "PriBlkGCThrd"));
    ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::GCTablet, allocator);
    for (int64_t i = 0; !is_stopped() && i < gc_items.count(); i++) {
      int tmp_ret = OB_SUCCESS;
      const ObPendingFreeTabletItem &gc_item = gc_items.at(i);
      ObPrivateBlockGCHandler handler(ls_id, ls_epoch, gc_item.tablet_id_, gc_item.tablet_meta_version_, gc_item.last_gc_version_,
          gc_item.reorganization_scn_, gc_item.gc_type_,
          ls_item.min_macro_seq_, ls_item.max_macro_seq_, gc_items.at(i), is_stopped_);
      if (OB_TMP_FAIL(gc_thread_guard.add_gc_task(handler))) {
        LOG_WARN("failed to add_gc_task", K(handler), K(gc_thread_guard));
      }
    }
    gc_thread_guard.wait_gc_task_finished();
    gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();
    if (gc_macro_block_cnt > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "private_dir_ls_gc", "tenant_id", MTL_ID(), "ls_id", ls_id.id(), "gc_macro_block_cnt", gc_macro_block_cnt);
    }

    ObArray<ObPendingFreeTabletItem> &thread_succ_gc_items = gc_thread_guard.get_succ_gc_items();
    if (is_stopped()) {
      LOG_INFO("private dir gc service stop, skip delete pending array", K(ls_id), K(ls_epoch), K(is_stopped()));
    } else if (thread_succ_gc_items.count() > gc_items.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("succ items is more than gc_items", K(ret), K(ls_id), K(gc_items.count()), K(thread_succ_gc_items.count()), K(thread_succ_gc_items));
    } else if (thread_succ_gc_items.count() < gc_items.count()) {
      if (thread_succ_gc_items.count() > 0
          && OB_FAIL(TENANT_STORAGE_META_SERVICE.delete_wait_gc_tablet_items(ls_id, ls_epoch, thread_succ_gc_items))) {
        LOG_WARN("failed to delete wait gc tablet items", K(ret), K(ls_id), K(thread_succ_gc_items.count()), K(thread_succ_gc_items));
      }
    } else {
      all_tablet_deleted = true;
    }
  } else {
    all_tablet_deleted = true;
  }

  if (OB_FAIL(ret)) {
  } else if (is_stopped()) {
    LOG_INFO("private dir gc service stop, skip delete ls dir", K(ls_id), K(ls_epoch), K(is_stopped()));
  } else if (no_delete_tablet || all_tablet_deleted) {
    if (OB_FAIL(tfm->delete_ls_dir(ls_id.id(), ls_epoch))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("file has been delete", K(ls_id), K(ls_epoch));
      } else {
        LOG_WARN("failed to delete_ls_dir", K(ret), K(ls_id), K(ls_epoch));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.delete_ls_item(ls_id, ls_epoch))) {
      LOG_WARN("failed to delete ls item", K(ret), K(ls_id), K(ls_epoch));
    }
  } else {
    LOG_INFO("skip delete ls dir", K(ls_id), K(ls_epoch), K(no_delete_tablet), K(all_tablet_deleted));
  }

  LOG_INFO("finish ls_gc", K(ret), K(ls_id), K(ls_epoch), K(gc_items.count()), K(gc_macro_block_cnt), K(gc_items));
  return ret;
}

void ObPrivateBlockGCTask::loop_check_ls_gc() 
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== ls gc ======", KPC(this));
  ObTenantStorageMetaService *tsms = MTL(ObTenantStorageMetaService*);
  ObArray<ObLSItem> deleted_ls_items;
  ObArray<ObLSItem> abort_ls_items;
  if (OB_ISNULL(tsms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(tsms->get_ls_items_by_status(ObLSItemStatus::DELETED, deleted_ls_items))) {
    LOG_WARN("failed to get_ls_items_by_status", K(ret), K(deleted_ls_items.count()));
  } else if (OB_FAIL(tsms->get_ls_items_by_status(ObLSItemStatus::CREATE_ABORT, abort_ls_items))) {
    LOG_WARN("failed to get_ls_items_by_status", K(ret), K(abort_ls_items.count()));
  } else {
    for (int64_t i = 0; i < deleted_ls_items.count(); i++) {
      ret = OB_SUCCESS; // reset ret, continue gc ls
      const ObLSItem &item = deleted_ls_items.at(i);
      if (OB_FAIL(ls_gc(item))) {
        LOG_WARN("failed to gc deleted ls", K(ret), K(item));
      }
    }
    for (int64_t i = 0; i < abort_ls_items.count(); i++) {
      ret = OB_SUCCESS; // reset ret, continue gc ls
      const ObLSItem &item = abort_ls_items.at(i);
      if (OB_FAIL(ls_gc(item, true /* no_delete_tablet */))) {
        LOG_WARN("failed to gc abort ls", K(ret), K(item));
      }
    }
  }
}

void ObPrivateBlockGCTask::loop_check_tablet_gc()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== tablet gc ======", KPC(this));
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is NULL", KR(ret));
      } else {
        ObLSPrivateBlockGCHandler &handler = ls->get_ls_private_block_gc_handler();
        obsys::ObRLockGuard<> lock(handler.wait_lock_);
        if (!handler.check_stop()) {
          handler.gc_tablets();
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        LOG_INFO("succeed to gc block", KR(ret), K(ls_cnt));
      } else {
        LOG_INFO("no logstream", KR(ret), K(ls_cnt));
      }
    }
  }
}

void ObPrivateBlockGCTask::loop_process_tablet_infos()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== process tablet info ======", KPC(this));
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is NULL", KR(ret));
      } else {
        ObLSPrivateBlockGCHandler &handler = ls->get_ls_private_block_gc_handler();
        obsys::ObRLockGuard<> lock(handler.wait_lock_);
        if (!handler.check_stop()) {
          handler.process_tablet_infos_for_tablet_meta_gc();
          handler.process_tablet_infos_for_macro_block_check();
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {
        LOG_INFO("succeed to gc block", KR(ret), K(ls_cnt));
      } else {
        LOG_INFO("no logstream", KR(ret), K(ls_cnt));
      }
    }
  }
}

void ObPrivateBlockGCTask::loop_process_failed_macro_blocks()
{
  ObIArray<blocksstable::MacroBlockId> &block_ids = drain_failed_macro_ids_();
  const int64_t count = block_ids.count();
  FLOG_INFO("====== process process failed blocks ======", K(count), KPC(this));
  if (count > 0) {
    delete_macro_blocks(block_ids);
  }
}

void ObPrivateBlockGCTask::loop_check_tablet_version_gc()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== tablet meta version gc ======", KPC(this));
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*); 
  const int64_t next_loop_tablet_meta_version_gc_ts = ATOMIC_LOAD(&next_loop_tablet_meta_version_gc_ts_);
  const int64_t curr_ts = ObTimeUtility::fast_current_time();
  if (next_loop_tablet_meta_version_gc_ts > curr_ts) {
  } else if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mtl ObLSService should not be null", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("get log stream iter failed", KR(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is NULL", KR(ret));
  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is NULL", KR(ret));
      } else {
        LOG_INFO("private block loop check", K(ls->get_ls_id()));
        ObLSPrivateBlockGCHandler &handler = ls->get_ls_private_block_gc_handler();
        obsys::ObRLockGuard<> lock(handler.wait_lock_);
        if (!handler.check_stop()) {
          handler.gc_tablets_meta_versions();
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      ATOMIC_STORE(&next_loop_tablet_meta_version_gc_ts_, curr_ts + GLOBAL_PRIVATE_BLOCK_GC_INTERVAL);
      if (ls_cnt > 0) {
        LOG_INFO("succeed to gc block", KR(ret), K(ls_cnt), K(next_loop_tablet_meta_version_gc_ts));
      } else {
        LOG_INFO("no logstream", KR(ret), K(ls_cnt), K(next_loop_tablet_meta_version_gc_ts));
      }
    }
  }
}

int ObLSPrivateBlockGCHandler::get_ls_min_macro_block_id_(
    const share::ObLSID &ls_id,
    uint64_t &min_macro_block_id)
{
  int ret = OB_SUCCESS;
  ObTenantStorageMetaService *tsms = MTL(ObTenantStorageMetaService*);
  ObArray<ObLSItem> ls_items;
  min_macro_block_id = UINT64_MAX;
  uint64_t max_macro_block_id = 0;
  if (OB_ISNULL(tsms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(tsms->get_ls_items_by_status(ObLSItemStatus::CREATED, ls_items))) {
    LOG_WARN("failed to get_ls_items_by_status", K(ret), K(ls_items.count()));
  } else if (0 == ls_items.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_items count is 0", K(ret), K(ls_id), KPC(this));
  } else {
    int i = 0;
    for (; OB_SUCC(ret) && i < ls_items.count(); i++) {
      ObLSItem &ls_item = ls_items.at(i);
      if (!ls_item.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls_item is invalid", K(ret), K(ls_id), K(ls_item), KPC(this), K(ls_items.count()), K(ls_items));
      } else if (ls_item.ls_id_ == ls_id) {
        min_macro_block_id = ls_item.min_macro_seq_;
        max_macro_block_id = ls_item.max_macro_seq_;
        break;
      }
    }
    if (i == ls_items.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is not in ls_itmes", K(ret), K(ls_id), K(ls_items));
    } else if (UINT64_MAX != max_macro_block_id
               || UINT64_MAX == min_macro_block_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls min/max_macro_block_id is unexpected", K(ret), K(min_macro_block_id), K(max_macro_block_id), K(ls_id), K(ls_items));
    }
  }
  return ret;
}

int ObLSPrivateBlockGCHandler::gc_tablet_meta_versions_(
    const ObTabletID &tablet_id,
    ObPrivateBlockGCThreadGuard &gc_thread_guard)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = ls_.get_ls_id();
  const int64_t ls_epoch = ls_.get_ls_epoch();
  int64_t current_tablet_version = -1;
  int64_t tablet_last_gc_version = -1;
  int64_t unused_transfer_seq = -1;
  int64_t reorganization_scn = -1;
  uintptr_t tablet_fingerprint = 0;
  bool allow_tablet_version_gc = false;
  bool is_transfer_out_deleted = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (!gc_thread_guard.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gc_thread_guard is invalid", K(ret), K(gc_thread_guard));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", K(ret));
  } else if (OB_FAIL(t3m->get_current_version_for_tablet(ls_id, tablet_id, current_tablet_version, tablet_last_gc_version,
                                                         unused_transfer_seq, tablet_fingerprint,
                                                         allow_tablet_version_gc, is_transfer_out_deleted,
                                                         reorganization_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tablet is not exist", KR(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get_current_version_for_tablet", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (!allow_tablet_version_gc) {
    ret = OB_EAGAIN;
    LOG_INFO("old version is using", KR(ret), K(ls_id), K(tablet_id), K(current_tablet_version));
  } else if (current_tablet_version == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current_tablet_version is invalid", KR(ret), K(ls_id), K(tablet_id));
  } else {
    ObPrivateBlockGCHandler handler(ls_id, ls_epoch, tablet_id, current_tablet_version,
        tablet_last_gc_version, reorganization_scn, tablet_fingerprint,
        is_set_stop_, is_transfer_out_deleted);
    if (OB_FAIL(gc_thread_guard.add_gc_task(handler))) {
      LOG_WARN("failed to add_gc_task", K(handler), K(gc_thread_guard));
    }
  }

  return ret;
}

int ObLSPrivateBlockGCHandler::macro_block_check_(
    const ObTabletID &tablet_id,
    const uint64_t min_macro_block_id,
    const uint64_t max_macro_block_id,
    ObPrivateBlockGCThreadGuard &gc_thread_guard)
{
  int ret = OB_SUCCESS;
  const share::ObLSID ls_id = ls_.get_ls_id();
  const int64_t ls_epoch = ls_.get_ls_epoch();
  int64_t current_tablet_version = -1;
  int64_t tablet_last_gc_version = -1;
  int64_t unused_transfer_seq = -1;
  int64_t reorganization_scn = -1;
  uintptr_t tablet_fingerprint = 0;
  bool unuse_flag = false;
  bool is_transfer_out_deleted = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (tablet_id.is_ls_inner_tablet()) {
  } else if (!gc_thread_guard.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gc_thread_guard is invalid", K(ret), K(gc_thread_guard));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", K(ret));
  } else if (UINT64_MAX == min_macro_block_id
             || UINT64_MAX == max_macro_block_id
             || min_macro_block_id >= max_macro_block_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min/max_macro_block_id is invalid", KR(ret), K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else if (OB_FAIL(t3m->get_current_version_for_tablet(ls_id, tablet_id, current_tablet_version, tablet_last_gc_version,
                                                         unused_transfer_seq, tablet_fingerprint, unuse_flag,
                                                         is_transfer_out_deleted, reorganization_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tablet is not exist", KR(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get_current_version_for_tablet", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (current_tablet_version == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current_tablet_version is invalid", KR(ret), K(ls_id), K(tablet_id));
  } else {
    ObPrivateBlockGCHandler handler(ls_id, ls_epoch, tablet_id, current_tablet_version,
        tablet_last_gc_version, reorganization_scn,
        min_macro_block_id, max_macro_block_id,
        tablet_fingerprint, is_set_stop_);
    if (OB_FAIL(gc_thread_guard.add_gc_task(handler))) {
      LOG_WARN("failed to add_gc_task", K(handler), K(gc_thread_guard));
    }
  }

  return ret;
}

void ObLSPrivateBlockGCHandler::gc_tablets_meta_versions()
{
  int ret = OB_SUCCESS;
  common::ObTabletIDArray tablet_ids;
  int64_t gc_macro_block_cnt = 0;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  ObPrivateBlockGCThread *gc_thread = NULL;
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else if (FALSE_IT(gc_thread = gc_service->get_private_block_gc_thread())) {
  } else if (OB_FAIL(ls_.get_tablet_svr()->get_all_tablet_ids(true, tablet_ids))) { 
    LOG_WARN("failed to get_all_tablet_ids", KR(ret), K(ls_));
  } else {
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "PriBlkGCThrd"));
    ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::GCTabletMetaVersion, allocator);
    for (int i = 0; !check_stop() && i < tablet_ids.count(); i++) {
      if (OB_FAIL(gc_tablet_meta_versions_(tablet_ids.at(i), gc_thread_guard))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), K(ls_), K(tablet_ids.at(i)));
        }
        const ObTabletGCInfo tablet_info(tablet_ids.at(i));
        if (OB_FAIL(report_tablet_id_for_gc_service(tablet_info))) {
          LOG_WARN("failed to report tablet id for gc", K(ret), K(tablet_info));
        }
      }
    }
    gc_thread_guard.wait_gc_task_finished();
    gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();

    if (gc_macro_block_cnt > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "private_dir_loop_check_tablet_meta_gc", "tenant_id", MTL_ID(), 
          "ls_id", ls_.get_ls_id().id(), "gc_macro_block_cnt", gc_macro_block_cnt);
    }
  }
  LOG_INFO("finish loop check gc_tablets_meta_versions", K(ret), KPC(this), K(gc_macro_block_cnt));
}

void ObLSPrivateBlockGCHandler::ls_macro_block_check()
{
  int ret = OB_SUCCESS;
  common::ObTabletIDArray tablet_ids;
  int64_t gc_macro_block_cnt = 0;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  uint64_t min_macro_block_id = UINT64_MAX;
  uint64_t max_macro_block_id = 0;
  const share::ObLSID ls_id = ls_.get_ls_id();
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else if (OB_FAIL(get_ls_min_macro_block_id_(ls_id, min_macro_block_id))) {
    LOG_WARN("failed to get_ls_min_macro_block_id_", KR(ret), K(ls_id));
  } else if (FALSE_IT(max_macro_block_id = gc_service->get_mtl_start_max_block_id())) {
  } else if (UINT64_MAX == min_macro_block_id
             || UINT64_MAX == max_macro_block_id
             || min_macro_block_id == max_macro_block_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min/max_macro_block_id is invalid", KR(ret), K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else if (min_macro_block_id > max_macro_block_id) {
    LOG_INFO("max_macro_block_id is less min_macro_block_id, ls maybe create later", K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else if (OB_FAIL(ls_.get_tablet_svr()->get_all_tablet_ids(true, tablet_ids))) { 
    LOG_WARN("failed to get_all_tablet_ids", KR(ret), K(ls_));
  } else {
    ObPrivateBlockGCThread *gc_thread = gc_service->get_private_block_gc_thread();
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "PriBlkGCThrd"));
    ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::MacroCheck, allocator);
    for (int i = 0; !check_stop() && i < tablet_ids.count(); i++) {
      if (OB_FAIL(macro_block_check_(tablet_ids.at(i), min_macro_block_id, max_macro_block_id, gc_thread_guard))) {
        LOG_WARN("failed to macro_block_check_", KR(ret), K(ls_), K(tablet_ids.at(i)));
        const ObTabletGCInfo tablet_info(tablet_ids.at(i), ProcessType::MacroCheck);
        if (OB_FAIL(report_tablet_id_for_gc_service(tablet_info))) {
          LOG_WARN("failed to report tablet id for gc", K(ret), K(tablet_info));
        }
      }
    }
    gc_thread_guard.wait_gc_task_finished();
    gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();

    if (gc_macro_block_cnt > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "macro_block_check_delete", "tenant_id", MTL_ID(), 
          "ls_id", ls_.get_ls_id().id(), "gc_macro_block_cnt", gc_macro_block_cnt);
    }
  }
  LOG_INFO("finish loop check macro_block_check_", K(ret), KPC(this), K(tablet_ids.count()), K(gc_macro_block_cnt));
}

void ObLSPrivateBlockGCHandler::process_tablet_infos_for_macro_block_check()
{
  int ret = OB_SUCCESS;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  uint64_t min_macro_block_id = UINT64_MAX;
  uint64_t max_macro_block_id = 0;
  const share::ObLSID ls_id = ls_.get_ls_id();
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else if (OB_FAIL(get_ls_min_macro_block_id_(ls_id, min_macro_block_id))) {
    LOG_WARN("failed to get_ls_min_macro_block_id_", KR(ret), K(ls_id));
  } else if (FALSE_IT(max_macro_block_id = gc_service->get_mtl_start_max_block_id())) {
  } else if (UINT64_MAX == min_macro_block_id
             || UINT64_MAX == max_macro_block_id
             || min_macro_block_id == max_macro_block_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min/max_macro_block_id is invalid", KR(ret), K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else if (min_macro_block_id > max_macro_block_id) {
    LOG_INFO("max_macro_block_id is less min_macro_block_id, ls maybe create later", K(min_macro_block_id), K(max_macro_block_id), K(ls_id));
  } else {
    ObPrivateBlockGCThread *gc_thread = gc_service->get_private_block_gc_thread();
    const ObIArray<ObTabletGCInfo> &tablet_infos = drain_tablet_infos_for_macro_block_check_();
    const int64_t count = tablet_infos.count();
    if (count > 0) {
      LOG_INFO("process_tablet_infos_for_macro_block_check start", K(count), KPC(this));
      int64_t gc_macro_block_cnt = 0;
      ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "PriBlkGCThrd"));
      ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::MacroCheck, allocator);
      for (int i = 0; !check_stop() && i < tablet_infos.count(); i++) {
        const ObTabletGCInfo &tablet_info = tablet_infos.at(i);
        if (ProcessType::MacroCheck != tablet_info.gc_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet_info.type and process_type is invalid", K(ret), K(tablet_info), K(ls_));
        } else if (OB_FAIL(macro_block_check_(tablet_info.tablet_id_, min_macro_block_id, max_macro_block_id, gc_thread_guard))) {
          LOG_WARN("failed to macro_block_check_", KR(ret), K(ls_), K(tablet_info));
          if (OB_FAIL(report_tablet_id_for_gc_service(tablet_info))) {
            LOG_WARN("failed to report_tablet_id_for_gc_service", K(ret), K(tablet_info));
          }
        }
      }
      gc_thread_guard.wait_gc_task_finished();
      gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();

      if (gc_macro_block_cnt > 0) {
        SERVER_EVENT_ADD("ss_macro_block_gc", "macro_block_check_delete", 
            "tenant_id", MTL_ID(), "ls_id", ls_.get_ls_id().id(), "gc_macro_block_cnt", gc_macro_block_cnt);
      }
      LOG_INFO("finish process_tablet_infos_for_macro_block_check", K(ret), KPC(this), K(gc_macro_block_cnt));
    }
  }
}

int ObLSPrivateBlockGCHandler::deduplicate_tablet_info(
    const ObIArray<ObTabletGCInfo> &tablet_infos,
    TabletInfoSet &tablet_info_set)
{
  int ret = OB_SUCCESS;
  if (!tablet_info_set.created() || !tablet_info_set.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_info_set is invalid", K(ret), K(tablet_infos));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_infos.count(); i++) {
    const ObTabletGCInfo &tablet_info = tablet_infos.at(i);
    if (OB_FAIL(tablet_info_set.set_refactored(tablet_info))) {
      LOG_WARN("failed to insert tablet_info_set", K(ret), K(tablet_info));
    }
  }
  return ret;
}

void ObLSPrivateBlockGCHandler::process_tablet_infos_for_tablet_meta_gc()
{
  int ret = OB_SUCCESS;
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else {
    ObPrivateBlockGCThread *gc_thread = gc_service->get_private_block_gc_thread();
    const ObIArray<ObTabletGCInfo> &tablet_infos = drain_tablet_infos_for_gc_();
    const int64_t count = tablet_infos.count();
    TabletInfoSet tablet_info_set;
    if (count > 0) {
      int64_t gc_macro_block_cnt = 0;
      ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "PriBlkGCThrd"));
      ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::GCTabletMetaVersion, allocator);
      if (OB_FAIL(tablet_info_set.create(tablet_infos.count(), "TblIfoSetBkt", "TblIfoSetNode",
                 OB_SERVER_TENANT_ID))) {
        LOG_WARN("failed to create tablet info set", K(ret), KPC(this));
      } else if (OB_FAIL(deduplicate_tablet_info(tablet_infos,tablet_info_set))) {
        LOG_WARN("failed to deduplicate_tablet_info", K(ret), KPC(this), K(tablet_infos), K(tablet_info_set));
      } else {
        LOG_INFO("process_tablet_infos_for_tablet_meta_gc start", K(tablet_infos.count()), K(tablet_info_set.size()), KPC(this), K(tablet_info_set), K(tablet_infos));
        for (typename TabletInfoSet::const_iterator iter = tablet_info_set.begin(); 
            !check_stop() && iter != tablet_info_set.end(); ++iter) {
          const ObTabletGCInfo tablet_info = iter->first;
          if (ProcessType::GCTabletMetaVersion != tablet_info.gc_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet_info.type and process_type is invalid", K(ret), K(tablet_info), K(ls_));
          } else if (OB_FAIL(gc_tablet_meta_versions_(tablet_info.tablet_id_, gc_thread_guard))) {
            if (OB_EAGAIN != ret) {
              LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), K(ls_), K(tablet_info));
            }
            if (OB_FAIL(report_tablet_id_for_gc_service(tablet_info))) {
              LOG_WARN("failed to report_tablet_id_for_gc_service", K(ret), K(tablet_info));
            }
          }
        }
        gc_thread_guard.wait_gc_task_finished();
        gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();

        if (gc_macro_block_cnt > 0) {
          SERVER_EVENT_ADD("ss_macro_block_gc", "private_dir_tablet_meta_gc", 
              "tenant_id", MTL_ID(), "ls_id", ls_.get_ls_id().id(), "gc_macro_block_cnt", gc_macro_block_cnt);
        }
        LOG_INFO("finish process_tablet_infos_for_tablet_meta_gc", K(ret), KPC(this), K(gc_macro_block_cnt));
      }
    }
    tablet_info_set.destroy();
  }
}

void ObLSPrivateBlockGCHandler::gc_tablets()
{
  int ret = OB_SUCCESS;
  ObArray<ObPendingFreeTabletItem> gc_items;
  const int64_t curr_ts = ObTimeUtility::fast_current_time();
  const ObLSID ls_id = ls_.get_ls_id();
  const int64_t ls_epoch = ls_.get_ls_epoch();
  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  int gc_macro_block_cnt = 0;
  ObPrivateBlockGCThread *gc_thread = NULL;
  uint64_t min_macro_block_id = 0;
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTabletGCService is NULL", K(ret), K(ls_));
  } else if (next_tablet_gc_ts_ > curr_ts) {
  } else if (OB_FAIL(get_ls_min_macro_block_id_(ls_id, min_macro_block_id))) {
    LOG_WARN("failed to get_ls_min_macro_block_id_", KR(ret), K(ls_id));
  } else if (UINT64_MAX == min_macro_block_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("min_macro_block_id is invalid", KR(ret), K(ls_id));
  } else if (FALSE_IT(gc_thread = gc_service->get_private_block_gc_thread())) {
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.get_wait_gc_tablet_items(ls_id, ls_epoch, gc_items))) {
    LOG_WARN("failed to get wait gc tablet items", K(ret), K(ls_id), K(ls_epoch), K(ls_));
  } else if (gc_items.count() > 0) {
    /// NOTE: this log is required for obtest@transfer_retry_err_gc.
    LOG_INFO("tablet gc start", K(ret), K(gc_items.count()), K(ls_), K(gc_items));
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "PriBlkGCThrd"));
    ObPrivateBlockGCThreadGuard gc_thread_guard(*gc_thread, ProcessType::GCTablet, allocator);
    for (int64_t i = 0; !check_stop() && i < gc_items.count(); i++) {
      const ObPendingFreeTabletItem &gc_item = gc_items.at(i);
      int64_t safe_ts = gc_item.free_time_ + gc_service->get_private_tablet_gc_safe_time();
      if (GCTabletType::DropLS == gc_item.gc_type_) {
        // ls maybe has not been dropped, drop inner tablet will cause some errors when restart observer
      } 
      // safe time discard temporarily : private_tablet_gc_safe_time_ is 0
      else if (0 == gc_service->get_private_tablet_gc_safe_time()
          || curr_ts >= safe_ts) {
        ObPrivateBlockGCHandler handler(ls_id, ls_epoch, gc_item.tablet_id_,
            gc_item.tablet_meta_version_, gc_item.last_gc_version_,
            gc_item.reorganization_scn_,
            gc_item.gc_type_, min_macro_block_id, UINT64_MAX /* not ls replica delete */,
            gc_item, is_set_stop_);
        if (true) {
          if (OB_FAIL(gc_thread_guard.add_gc_task(handler))) {
            LOG_WARN("failed to add_gc_task", K(handler), K(gc_thread_guard));
          }
        } 
      } else {
        next_tablet_gc_ts_ = safe_ts;
        LOG_INFO("tablet gc need wait", K(ret), K(gc_item), K(ls_), K(safe_ts), K(i), K(gc_items.count()), K(gc_service->get_private_tablet_gc_safe_time()));
        break;
      }
    }
    gc_thread_guard.wait_gc_task_finished();
    gc_macro_block_cnt = gc_thread_guard.get_gc_macro_block_cnt();
    ObArray<ObPendingFreeTabletItem> &thread_succ_gc_items = gc_thread_guard.get_succ_gc_items();

    if (thread_succ_gc_items.count() > gc_items.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("succ items is more than gc_items", K(ret), K(ls_), K(gc_items.count()), K(thread_succ_gc_items.count()), K(thread_succ_gc_items));
    } else if (check_stop()) {
      LOG_INFO("ls is offline, skip delete pending_free_tablet_array", K(ls_), K(gc_items.count()), K(gc_macro_block_cnt), K(thread_succ_gc_items.count()));
    } else if (thread_succ_gc_items.count() > 0
        && OB_FAIL(TENANT_STORAGE_META_SERVICE.delete_wait_gc_tablet_items(ls_id, ls_epoch, thread_succ_gc_items))) {
      LOG_WARN("failed to delete wait gc tablet items", K(ret), K(ls_), K(thread_succ_gc_items.count()), K(thread_succ_gc_items));
    }


    if (gc_macro_block_cnt > 0) {
      SERVER_EVENT_ADD("ss_macro_block_gc", "private_dir_tablet_gc", "tenant_id", MTL_ID(), "ls_id", ls_id.id(), "gc_macro_block_cnt", gc_macro_block_cnt);
    }
    LOG_INFO("finish gc_tablets", K(ret), K(ls_), K(gc_items.count()), K(gc_macro_block_cnt), K(thread_succ_gc_items.count()), K(gc_items), K(thread_succ_gc_items));
  }
}

int ObLSPrivateBlockGCHandler::report_tablet_id_for_gc_service(
    const ObTabletGCInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  switch (tablet_info.gc_type_) {
  case ProcessType::GCTabletMetaVersion:
    if (OB_FAIL(gc_tablet_info_queue_.add_item(tablet_info))) {
      LOG_WARN("failed to add_item", K(ret), K(tablet_info));
    }
    break;
  case ProcessType::MacroCheck:
    if (OB_FAIL(macro_block_check_tablet_info_queue_.add_item(tablet_info))) {
      LOG_WARN("failed to add_item", K(ret), K(tablet_info));
    }
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("process type is invalid", KR(ret), K(tablet_info));
  }
  return ret;
}

int ObPrivateBlockGCHandler::get_blocks_for_tablet(
  const int64_t tablet_meta_version,
  const bool is_shared,
  ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  block_ids.reset();
  ObTenantStorageMetaService *tsms = MTL(ObTenantStorageMetaService*);
  if (OB_ISNULL(tsms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", K(ret));
  } else if (OB_FAIL(tsms->get_blocks_from_private_tablet_meta(ls_id_, ls_epoch_, tablet_id_,
                     tablet_meta_version, reorganization_scn_, is_shared, block_ids))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("block is deleted", K(ret), K(tablet_meta_version), K(is_shared), KPC(this));
    } else {
      LOG_WARN("failed to get_blocks_from_private_tablet_meta", K(ret), K(tablet_meta_version),
               K(is_shared), KPC(this));
    }
  }
  return ret;
}

int ObPrivateBlockGCHandler::list_tablet_meta_version(
  ObIArray<int64_t> &tablet_versions,
  const bool is_macro_check,
  int64_t &gc_end_version)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObStorageMetaIOUtil::collect_private_tablet_versions(ls_id_,
                                                                   ls_epoch_,
                                                                   tablet_id_,
                                                                   reorganization_scn_,
                                                                   tablet_last_gc_version_ + 1,
                                                                   max_tablet_meta_version_, 
                                                                   true /*allow_stale_versions*/,
                                                                   tablet_versions))) {
    LOG_WARN("failed to collect tablet versions", K(ret), K_(tablet_last_gc_version), K_(max_tablet_meta_version));
  } else if (tablet_versions.empty()) {
    // do nothing
    gc_end_version = 0;
  } else if (is_macro_check) {
    /// find minimum version in @c tablet_versions
    int64_t min_ver = INT64_MAX;
    for (int64_t i = 0; i < tablet_versions.count(); ++i) {
      min_ver = min(min_ver, tablet_versions.at(i));
    }
    gc_end_version = min_ver - 1;
  } else {
    // gc old tablet version
    gc_end_version = max_tablet_meta_version_ - 1;
  }
  return ret;
}

int ObPrivateBlockGCHandler::list_tablet_meta_version(
    ObIArray<int64_t> &tablet_versions)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObStorageMetaIOUtil::collect_private_tablet_versions(ls_id_,
                                                                   ls_epoch_,
                                                                   tablet_id_,
                                                                   reorganization_scn_,
                                                                   tablet_last_gc_version_ + 1,
                                                                   max_tablet_meta_version_, 
                                                                   true /*allow_stale_versions*/,
                                                                   tablet_versions))) {
    LOG_WARN("failed to collect tablet versions", K(ret), K_(tablet_last_gc_version), K_(max_tablet_meta_version));
  }
  return ret;
}

int ObPrivateBlockGCHandler::delete_tablet_meta_version(
  int64_t tablet_meta_version)
{
  FLOG_INFO("gc tablet meta version", KPC(this), K(tablet_meta_version));
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId file_id;
  ObStorageObjectOpt opt;
  opt.set_ss_private_tablet_meta_object_opt(ls_id_.id(), tablet_id_.id(), tablet_meta_version,
      tablet_id_.is_ls_inner_tablet(), reorganization_scn_);
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, file_id))) {
    LOG_WARN("failed to get_object_id", K(ret));
  } else if (OB_FAIL(tfm->delete_file_with_ls_context(file_id, ls_epoch_, ls_id_.id()))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), K(file_id), KPC(this));
    } else {
      LOG_WARN("failed to delete_file", K(ret), K(file_id), KPC(this));
    }
  }
  return ret;
}

int ObPrivateBlockGCHandler::try_delete_tablet_meta_dir()
{
  int ret = OB_SUCCESS;
  const int64_t private_macro_third_id = tablet_id_.is_ls_inner_tablet()
      ? ls_id_.id()
      : reorganization_scn_;
  // 1. delete tenant_epoch/ls/ls_id_ls_epoch/tablet_meta/scatter/tablet/private_macro_third_id
  if (OB_FAIL(OB_DIR_MGR.delete_tablet_meta_tablet_id_private_macro_third_id_dir(
      MTL_ID(), MTL_EPOCH_ID(), ls_id_.id(), ls_epoch_,
      tablet_id_.id(), private_macro_third_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete tablet meta reorganization scn dir", K(ret), KPC(this));
    }
  }
  // 2. delete tenant_epoch/ls/ls_id_ls_epoch/tablet_meta/scatter/tablet
  if (FAILEDx(OB_DIR_MGR.delete_tablet_meta_tablet_id_dir(
      MTL_ID(), MTL_EPOCH_ID(), ls_id_.id(), ls_epoch_, tablet_id_.id()))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete tablet meta tablet id dir", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObPrivateBlockGCHandler::try_delete_tablet_data_dir()
{
  int ret = OB_SUCCESS;
  const int64_t private_macro_third_id = tablet_id_.is_ls_inner_tablet()
      ? ls_id_.id()
      : reorganization_scn_;
  if (OB_FAIL(OB_DIR_MGR.delete_tablet_data_tablet_id_private_macro_third_id_dir(
      MTL_ID(), MTL_EPOCH_ID(), tablet_id_.id(), private_macro_third_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), KPC(this));
    } else if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      LOG_INFO("there are some file in dir, ls migration maybe in process", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete private macro namespace dir", K(ret), KPC(this));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.delete_tablet_data_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(),
          tablet_id_.id()))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("file has been delete", K(ret), KPC(this));
    } else if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      LOG_INFO("there are some file in dir, another private macro namespace may exist", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete_tablet_data_tablet_id_dir", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObPrivateBlockGCHandler::get_block_ids_from_dir(
  ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  // Fires after gc_blocks_ and before cleanup of residual private macro blocks and data/meta directories.
  if (GCONF.errsim_migration_tablet_id == tablet_id_.id()) {
    SERVER_EVENT_SYNC_ADD("private_block_gc", "before_gc_tablet",
                          "tenant_id", MTL_ID(),
                          "ls_id", ls_id_.id(),
                          "tablet_id", tablet_id_.id(),
                          "gc_type", static_cast<int64_t>(gc_type_));
    DEBUG_SYNC(BEFORE_PRIVATE_TABLET_GC);
  }
#endif

  const int64_t private_macro_third_id = tablet_id_.is_ls_inner_tablet()
      ? ls_id_.id()
      : reorganization_scn_;
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(tfm->list_private_macro_file(
      tablet_id_.id(), private_macro_third_id, block_ids))) {
    LOG_WARN("failed to list private macro file", K(ret), KPC(this));
  }
  return ret;
}

int ObPrivateBlockGCHandler::gc_tablet()
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> tablet_versions;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  bool allow = false;
  if (GCTabletType::InvalidType == gc_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc_type is invalid", K(ret), KPC(this));
  } else if ((GCTabletType::DropLS == gc_type_ && UINT64_MAX == ls_max_block_id_)
             || ls_min_block_id_ >= ls_max_block_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_max_block_id_ is invalid", K(ret), KPC(this));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", K(ret), KPC(this));
  } else if (OB_FAIL(t3m->check_allow_tablet_gc(tablet_id_, reorganization_scn_, allow))) {
    LOG_WARN("failed to check_allow_tablet_gc", K(ret), KPC(this));
  } else if (!allow) {
    ret = OB_EAGAIN;
    // expected retry: flying/external refs still exist; rate-limit to avoid log flood
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000LL)) { // 10s
      LOG_INFO("tablet not allow be gc", K(ret), KPC(this));
    }
  } else if (OB_FAIL(list_tablet_meta_version(tablet_versions))) {
    LOG_WARN("failed to list_tablet_meta_version", K(ret), KPC(this));
  } else if (tablet_versions.count() == 0) {
    LOG_INFO("there is not tablet_versions, maybe have been gc", KR(ret), KPC(this));
  } else if (OB_FAIL(ObBlockGCHandler::gc_tablet(tablet_versions, gc_type_, ls_min_block_id_, ls_max_block_id_))) {
    LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), KPC(this), K(tablet_versions.count()),
             K(tablet_versions), K_(gc_type), K_(ls_min_block_id), K_(ls_max_block_id));
  }
  /// NOTE: this log is required for obtest@transfer_retry_err_gc / ss_gc (ret=0 path must keep INFO).
  /// OB_EAGAIN is expected retry, rate-limit to avoid flooding with wait_gc backlog.
  if (OB_SUCC(ret) || OB_EAGAIN != ret || REACH_TIME_INTERVAL(10 * 1000 * 1000LL)) { // 10s
    LOG_INFO("finish private tablet gc", K(ret), KPC(this), K(tablet_versions));
  }
  return ret;
}

int ObPrivateBlockGCHandler::gc_tablet_meta_versions()
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> tablet_versions;
  int64_t gc_end_version = 0;
  if (GCTabletType::InvalidType != gc_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta gc, gc_type is valid", K(ret), KPC(this));
  } else if (OB_FAIL(list_tablet_meta_version(tablet_versions, /*is_macro_check*/false, gc_end_version))) {
    LOG_WARN("failed to list_tablet_meta_version", K(ret), KPC(this));
  } else if (tablet_versions.count() == 0) {
    LOG_INFO("there is not tablet_versions, maybe have been gc", KR(ret), KPC(this));
  } else if (tablet_versions.count() == 1) {
    // only one version not be gc
  } else if (OB_FAIL(ObBlockGCHandler::gc_tablet_meta_versions(tablet_versions,
                     max_tablet_meta_version_, is_transfer_out_deleted_))) {
    LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), KPC(this), K(tablet_versions.count()),
             K(tablet_versions), K_(max_tablet_meta_version), K_(is_transfer_out_deleted));
  } else if (OB_FAIL(update_tablet_last_gc_version_(gc_end_version))) {
    LOG_WARN("failed to update tablet last gc version", K(ret), K(gc_end_version));
  }
  return ret;
}

int ObPrivateBlockGCHandler::macro_block_check()
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> tablet_versions;
  BlockCollectOP op;
  int64_t gc_end_version = 0;
  if (GCTabletType::InvalidType != gc_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta gc, gc_type is valid", K(ret), KPC(this));
  } else if (OB_FAIL(list_tablet_meta_version(tablet_versions, /*is_macro_check*/true, gc_end_version))) {
    LOG_WARN("failed to list_tablet_meta_version", K(ret), KPC(this));
  } else if (tablet_versions.count() == 0) {
    LOG_INFO("there is not tablet_versions, maybe have been gc", KR(ret), KPC(this));
  } else if (OB_FAIL(build_macro_block(tablet_versions, -1, op))) {
    LOG_WARN("failed to list_macro_block", K(ret), KPC(this), K(tablet_versions));
  } else if (OB_FAIL(ObBlockGCHandler::macro_block_check(op.get_result_block_id_set(), ls_min_block_id_, ls_max_block_id_))) {
    LOG_WARN("failed to macro_block_check", K(ret), KPC(this));
  } else if (OB_FAIL(update_tablet_last_gc_version_(gc_end_version))) {
    LOG_WARN("failed to update tablet last gc version", K(ret), K(gc_end_version));
  }
  return ret;
}

int ObPrivateBlockGCHandler::update_tablet_last_gc_version_(const int64_t gc_end_version)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  
  if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null t3m", K(ret), KP(t3m));
  } else if (OB_FAIL(t3m->update_tablet_last_gc_version(ls_id_, tablet_id_, tablet_fingerprint_, gc_end_version))) {
    if (OB_NOT_THE_OBJECT == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("tablet pointer has been changed, skip update", KPC(this));
    } else {
      LOG_WARN("failed to update tablet last gc version", K(ret), K(gc_end_version), KPC(this));
    }
  }
  return ret;
}

/**
 * @brief GC protection mechanism for transfer in tablet persistent macro blocks
 * 
 * This mechanism prevents accidental garbage collection of macro blocks during
 * tablet transfer operations by implementing a three-step verification process.
 * 
 * Timeline and Process of @c macro_check():
 * 1. step1: Collect macro blocks that need to be deleted
 * 2. step2: Check T3M to see if macro blocks can be safely recycled
 * 3. step3: Delete macro blocks collected in step1
 * 
 * Protection Scenarios for "transfer in tablet" persistent macro blocks:
 * 
 * Scenario 1(before step1):
 * timeline: ---------------------------------------------------->  
 *                        ^                   ^       ^      ^
 *              create_transfer_in_tablet   step1   step2   step3
 * - Transfer in tablet persistent macro blocks might be collected by step1,
 *   but will definitely be detected by step2
 * - If detected, step3 will be skipped, preventing accidental GC
 * 
 * Scenario 2 (between step1 and step2):
 * timeline: ---------------------------------------------------->  
 *               ^               ^                   ^       ^ 
 *             step1   create_transfer_in_tablet   step2   step3
 * - Transfer in tablet persistent macro blocks will not be collected by step1,
 *   and will definitely be detected by step2
 * - This prevents accidental GC
 * 
 * Scenario 3 (between step2 and step3):
 * timeline: ---------------------------------------------------->  
 *               ^       ^                   ^              ^ 
 *             step1   step2   create_transfer_in_tablet   step3
 * - Transfer in tablet persistent macro blocks will not be collected by step1,
 *   and although they will not be detected by step2, they will also not be
 *   accidentally GC.
 * 
 * More Details:
 * - private objects use reorganization_scn as the User Tablet namespace
 * - retries of the same transfer keep the same reorganization_scn
 * - a new transfer uses a different reorganization_scn
 * 
 * T3M Check Logic:
 * Query T3M using (ls_id, tablet_id), then compare reorganization_scn. If a
 * tablet with the same reorganization_scn exists,
 * skip the current macro block cleanup (because remaining macro blocks might
 * be generated by tablet dump after transfer in tablet been created).
 */
int ObPrivateBlockGCHandler::safe_delete_blocks_for_macro_check_(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObLSTabletService *ls_tablet_svr = nullptr;
  bool allow = true;

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null t3m", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id_, 
                                    ls_handle, 
                                    ObLSGetMod::TXSTORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_), K(ls_handle));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ls", K(ret), K(ls_id_), K(ls_handle));
  } else if (OB_ISNULL(ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ls_tablet_svr", K(ret), K(ls_id_), K(ls_handle));
  } else if (OB_FAIL(ls_tablet_svr->check_allow_tablet_macro_check(
      tablet_id_, reorganization_scn_, allow))) {
    LOG_WARN("failed to check allow tablet macro check", K(ret), K(ls_id_), K(tablet_id_),
        K(reorganization_scn_));
  } else if (!allow) {
    // do nothing
    /// NOTE: this log is required for obtest@transfer_retry_err_gc.
    LOG_INFO("macro check is not allowed, skip it", K(ret), K(allow), KPC(this));
  } else if (OB_FAIL(ObBlockGCHandler::delete_macro_blocks(block_ids))) {
    LOG_WARN("failed to delete macro blocks", K(ret), K(allow), K(block_ids));
  }
  return ret;
}

void ObPrivateBlockGCThread::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObPrivateBlockGCHandler *handler = NULL;
  const ObPendingFreeTabletItem *extra_info = NULL;

  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is NULL", KR(ret), KPC(this));
  } else if (!is_valid_ctx()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is invalid", KR(ret), KPC(this));
  } else if (FALSE_IT(handler = static_cast<ObPrivateBlockGCHandler*>(task))) {
  } else if (handler->check_stop()) {
    LOG_INFO("private block gc handler is in stop status, skip gc", KPC(handler));
  } else {
    switch (get_gc_task_ctx()->process_type_) {
    case ProcessType::GCTabletMetaVersion:
      if(OB_FAIL(handler->gc_tablet_meta_versions())) {
        LOG_WARN("failed to gc_tablet_meta_versions", KR(ret), KPC(this), KPC(handler));
      }
      break;
    case ProcessType::GCTablet:
      if (OB_FAIL(handler->gc_tablet())) {
        LOG_WARN("failed to gc_tablet", KR(ret), KPC(this), KPC(handler));
      } else if (OB_ISNULL(extra_info = handler->get_extra_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("extra_info is NULL", KR(ret), KPC(this), KPC(handler));
      } else if (OB_FAIL(get_gc_task_ctx()->update_succ_gc_items(*extra_info))) {
        LOG_WARN("failed to update_succ_gc_items", KR(ret), KPC(this), KPC(handler));
      }
      break;
    case ProcessType::MacroCheck:
      if(OB_FAIL(handler->macro_block_check())) {
        LOG_WARN("failed to macro_block_check", KR(ret), KPC(this), KPC(handler));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("process type is invalid", KR(ret), KPC(this), KPC(handler));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (handler->gc_macro_block_cnt_ > 0) {
    get_gc_task_ctx()->update_gc_macro_block_cnt(handler->gc_macro_block_cnt_);
  }
  handle_end(task);
}

} /* namespace storage */
} /* namespace oceanbase */
