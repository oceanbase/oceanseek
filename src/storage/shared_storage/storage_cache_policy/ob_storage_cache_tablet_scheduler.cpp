/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "lib/ob_errno.h"
#include "lib/literals/ob_literals.h"
#include "lib/utility/ob_macro_utils.h"
#define USING_LOG_PREFIX STORAGE
#include "share/ob_define.h"
#include "share/ob_thread_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
  
int ObStorageCacheTabletScheduler::get_tablet_handle(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  
  tablet_handle.reset();
  
  if (OB_UNLIKELY(!tablet_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_CACHE_POLICY_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_handle is in_valid", KR(ret), K(ls_handle), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K(ls_handle), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet svr is null", KR(ret), K(ls_handle), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 
                     0 /*timeout*/, storage::ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id), K(ls_id), K(tablet_handle));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", KR(ret), K(tablet_handle), K(tablet_id), K(ls_id));
  }
  return ret;
}

int get_macro_blocks(const ObLSID &ls_id,
                     const ObTabletID &tablet_id,
                     ObIArray<MacroBlockId> &data_block_ids, 
                     ObIArray<MacroBlockId> &meta_block_ids)
{
  int ret = OB_SUCCESS;
  data_block_ids.reset();
  meta_block_ids.reset();
  ObLSService *ls_service = nullptr;
  ObTablet *tablet = nullptr;
  ObTabletMacroInfo *macro_info = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  storage::ObMacroInfoIterator macro_iter;
  storage::ObTabletBlockInfo block_info;
  int64_t ls_epoch = -1;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "SCPBLocks"));
  LOG_TRACE("[SCP]start to get macro blocks", K(tablet_id), K(ls_id));
  if (OB_UNLIKELY(!tablet_id.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantStorageMetaService is NULL", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_CACHE_POLICY_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_handle is in_valid", KR(ret), K(ls_handle), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K(ls_handle), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet svr is null", KR(ret), K(ls_handle), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 0 /*timeout*/, 
      storage::ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id), K(ls_id), K(tablet_handle));
  } else if (!tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table handle is in_valid", KR(ret), K(tablet_handle), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be null", KR(ret), K(tablet_handle), K(tablet_id), K(ls_id));
  } else if (FALSE_IT(ls_epoch = ls_handle.get_ls()->get_ls_epoch())) {
  } else if (OB_FAIL(tablet->load_macro_info(ls_epoch, allocator, macro_info))) {
    LOG_WARN("failed to load macro info", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(macro_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro info is null", KR(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, *macro_info))) {
    LOG_WARN("fail to init macro iterator", KR(ret), KPC(macro_info), K(tablet_id), K(ls_id));
  } else {
    while (OB_SUCC(ret)) {
      block_info.reset();
      if (OB_FAIL(macro_iter.get_next(block_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next block info", KR(ret), K(block_info), K(tablet_id), K(ls_id));
        }
      } else if (SSObjUtil::is_macro_data(block_info.macro_id()) && OB_FAIL(data_block_ids.push_back(block_info.macro_id()))) {
        LOG_WARN("fail to push data macro id", KR(ret), K(block_info), K(ls_id), K(tablet_id));
      } else if (SSObjUtil::is_macro_meta(block_info.macro_id()) && OB_FAIL(meta_block_ids.push_back(block_info.macro_id()))) {
        LOG_WARN("fail to push meta macro id", KR(ret), K(block_info), K(ls_id), K(tablet_id));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_NOT_NULL(macro_info)) {
    macro_info->reset();
  }
  return ret;
}

int get_tablet_ls_id(const int64_t tenant_id, const uint64_t tablet_id, int64_t &ls_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ls_id = ObLSID::INVALID_LS_ID;
  common::ObSEArray<share::ObLSID, DEFAULT_LS_SIZE> ls_ids;
  ls_ids.set_attr(ObMemAttr(tenant_id, "SCP_get_ls_id"));

  if (OB_UNLIKELY(tablet_id == common::ObTabletID::INVALID_TABLET_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    if (OB_ISNULL(MTL(ObLSService *))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), K(tablet_id), K(ls_id));
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls_ids(ls_ids))) {
      LOG_WARN("fail to get ls ids", KR(ret));
    } else {
      ObLSTabletService *tablet_svr = nullptr;
      ObLSHandle ls_handle;
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count() && !is_exist; ++i) {
        tablet_svr = nullptr;
        ls_handle.reset();
        if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_ids[i], ls_handle, ObLSGetMod::STORAGE_CACHE_POLICY_MOD))) {
          LOG_WARN("failed to get ls", KR(ret), K(ls_ids[i].id()));
        } else if (OB_ISNULL(ls_handle.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls is null", KR(ret), K(ls_handle));
        } else if (OB_ISNULL(tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get ls", KR(ret), K(ls_ids[i].id()));
        } else if (OB_FAIL(tablet_svr->is_tablet_exist(ObTabletID(tablet_id), is_exist))) {
          LOG_WARN("fail to judge is tablet exist", KR(ret), K(tablet_id), K(ls_ids[i].id()));
        } else if (is_exist) {
          ls_id = ls_ids[i].id();
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObLSID::INVALID_LS_ID == ls_id) {
    ret = OB_EAGAIN;
    LOG_WARN("ls id is invalid, need retry", KR(ret), K(ls_id), K(ls_ids));
  }
  return ret;
}

/******************ObStorageCacheTabletScheduler*******************/
ObStorageCacheTabletScheduler::ObStorageCacheTabletScheduler()
  : tablet_task_map_(),
    active_tablet_ids_(),
    exist_skipped_tablet_(false),
    is_inited_(false),   
    tg_id_(OB_INVALID_TG_ID),
    tenant_id_(OB_INVALID_TENANT_ID),
    has_retry_waiting_task_(false)
{
}

int ObStorageCacheTabletScheduler::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(tablet_task_map_.create(DEFAULT_TABLET_TASK_MAP_SIZE, ObMemAttr(MTL_ID(),"TabletTaskMap")))) {
    LOG_WARN("fail to create tablet task map", KR(ret));
  } else if (OB_FAIL(active_tablet_ids_.create(DEFAULT_TABLET_TASK_MAP_SIZE, ObMemAttr(MTL_ID(),"SCPActiveTablet")))) {
    LOG_WARN("fail to create active tablet ids map", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::StorageCacheTabletScheduler, tg_id_))) {
    LOG_WARN("fail to create tenant", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObStorageCacheTabletScheduler::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler is not init", KR(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("fail to start tenant", KR(ret));
  }   
  return ret;
}

void ObStorageCacheTabletScheduler::stop()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_STOP(tg_id_);
  }
}

void ObStorageCacheTabletScheduler::wait()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_WAIT(tg_id_);
  }
}

int ObStorageCacheTabletScheduler::check_is_active_tablet(const int64_t tablet_id, bool &is_active) const
{
  int ret = OB_SUCCESS;
  is_active = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant tablet storage cache tablet scheduler not init", KR(ret));
  } else if (common::ObTabletID::INVALID_TABLET_ID == tablet_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_FAIL(active_tablet_ids_.exist_refactored(tablet_id))) {
    if (OB_HASH_EXIST == ret) {
      is_active = true;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check exist in active tablet ids", KR(ret), K(tablet_id), K(tenant_id_));
    }
  }
  return ret;
}

int ObStorageCacheTabletScheduler::generate_block_task(const uint64_t tenant_id,
                                                       const int64_t tablet_id, 
                                                       const ObStorageCachePolicy &block_storage_cache_policy)
{
  int ret = OB_SUCCESS;
  ObStorageCacheTabletTaskHandle ori_task_handle;
  ObStorageCacheTabletTaskHandle task_handle;
  ObStorageCacheTabletTask *task = nullptr;
  bool is_active = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant tablet storage cache tablet scheduler not init", KR(ret));
  } else if (OB_UNLIKELY(!block_storage_cache_policy.is_block_time_policy())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid block storage cache policy", KR(ret), K(block_storage_cache_policy));
  } else if (OB_INVALID_TENANT_ID == tenant_id || common::ObTabletID::INVALID_TABLET_ID == tablet_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(check_is_active_tablet(tablet_id, is_active))) {
    LOG_WARN("fail to check is active tablet", KR(ret), K(tablet_id), K(block_storage_cache_policy));
  } else if (is_active) {
    if (OB_ISNULL(task = OB_NEW(ObStorageCacheTabletTask, ObMemAttr(tenant_id_, "ObSCPBlock")))) { // Task will be freed in destructor function
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for tablet task", KR(ret));
    } else if (FALSE_IT(task_handle.set_ptr(task))) {
    } else if (OB_FAIL(task->init_block_task(tenant_id, tablet_id, block_storage_cache_policy))) {
      LOG_WARN("fail to init block task", KR(ret), KPC(task), K(tenant_id), K(tablet_id), K(block_storage_cache_policy));
    } else if (OB_UNLIKELY(!task->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is not valid", KR(ret), KPC(task));
    } else if (OB_FAIL(push_task(task_handle))) {
      LOG_WARN("fail to push task", KR(ret), K(tenant_id), K(tablet_id), K(block_storage_cache_policy), KPC(task));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObStorageCacheTabletScheduler::generate_partition_task(
    const uint64_t tenant_id,
    const int64_t tablet_id,
    const PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  ObStorageCacheTabletTaskHandle ori_task_handle;
  ObStorageCacheTabletTaskHandle task_handle;
  ObStorageCacheTabletTask *task = nullptr;
  bool is_active = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant tablet storage cache tablet scheduler not init", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || common::ObTabletID::INVALID_TABLET_ID == tablet_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(check_is_active_tablet(tablet_id, is_active))) {
    LOG_WARN("fail to check is active tablet", KR(ret), K(tablet_id));
  } else if (is_active) {
    if (OB_ISNULL(task = OB_NEW(ObStorageCacheTabletTask, ObMemAttr(tenant_id_, "ObSCPTask")))) { // Task will be freed in destructor function
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for tablet task", KR(ret));
    } else if (FALSE_IT(task_handle.set_ptr(task))) {
    } else if (OB_FAIL(task->init_partition_task(tenant_id, tablet_id, policy_status))) {
      LOG_WARN("fail to init task", KR(ret), KPC(task), K(tenant_id), K(tablet_id), K(policy_status));
    } else if (OB_UNLIKELY(!task->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is not valid", KR(ret), KPC(task));
    } else if (OB_FAIL(push_task(task_handle))) {
      LOG_WARN("fail to push task", KR(ret), K(tenant_id), K(tablet_id), KPC(task));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObStorageCacheTabletScheduler::push_task(ObStorageCacheTabletTaskHandle &task_handle)
{
  int ret = OB_SUCCESS;
  ObStorageCacheTabletTaskHandle ori_task_handle;
  int64_t tablet_id = common::ObTabletID::INVALID_TABLET_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant tablet storage cache tablet scheduler not init", KR(ret));
  } else if (!task_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task handle is invalid", KR(ret), K(task_handle), K(tenant_id_));
  } else if (OB_UNLIKELY(!task_handle()->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task is invalid", KR(ret), K(task_handle));
  } else if (FALSE_IT(tablet_id = task_handle()->get_tablet_id())) {
  } else if (OB_FAIL(tablet_task_map_.get_refactored(tablet_id, ori_task_handle))) {
    // If the task is not in the map, just insert it into the map
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get task from map", KR(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(ori_task_handle())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ori task handle is null", KR(ret), K(ori_task_handle));
  } else if (ori_task_handle()->need_retry_push_task()) {
    // TG has not taken ownership of a RETRY_WAITING task. The new task takes over only after
    // overwriting the map successfully; otherwise, the old task remains available for the next retry.
  } else if (OB_FAIL(ori_task_handle()->set_status(
      ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED))) {
    LOG_WARN("fail to cancel origin task", KR(ret), KPC(ori_task_handle.get_ptr()));
  } else {
    LOG_INFO("[SCP] set origin task canceled", KR(ret), KPC(ori_task_handle.get_ptr()));
  }

  if (OB_FAIL(ret)) {
  } else {
    // Do not wait for the queue thread to remove the task, 
    // overwrite the task here directly to insert the new task
    if (OB_FAIL(tablet_task_map_.set_refactored(tablet_id, task_handle, true/*overwrite*/))) {
      LOG_WARN("fail to set task", KR(ret), K(task_handle));
    } else {
      ObStorageCacheTabletTaskHandle *queue_task_handle = nullptr;
      if (OB_ISNULL(queue_task_handle = OB_NEW(ObStorageCacheTabletTaskHandle, ObMemAttr(tenant_id_, "SCPTaskHandle")))) {
        // task handle will be freed in handle or handle_drop if it push to queue successfully,
        // or will be freed below if push operation failed
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for queue task handle", KR(ret));
      } else if (OB_FAIL(queue_task_handle->assign(task_handle))) {
        LOG_WARN("fail to assign task handle", KR(ret), K(task_handle), K(queue_task_handle));
      } else if (OB_FAIL(OB_E(common::EventTable::EN_PUSH_TASK_FAILED) OB_SUCCESS)) {
        LOG_WARN("fail to push task by errsim", KR(ret), K(tablet_id), K(tg_id_));
      } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, queue_task_handle))) {
        if (OB_EAGAIN == ret) {
          if (REACH_TIME_INTERVAL(1_s)) {
            LOG_WARN("fail to push task", KR(ret), K(queue_task_handle), K(tablet_id), K(tg_id_));
          }
        } else {
          LOG_WARN("fail to push task", KR(ret), K(queue_task_handle), K(tablet_id), K(tg_id_));
        }
      }

      // Release the handle not owned by TG before recording a scheduling failure.
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_NOT_NULL(queue_task_handle)) {
          OB_DELETE(ObStorageCacheTabletTaskHandle, ObMemAttr(tenant_id_, "SCPTaskHandle"), queue_task_handle);
        }
        ObStorageCacheTabletTask *task = task_handle.get_ptr();
        if (OB_NOT_NULL(task)) {
          if (OB_TMP_FAIL(task->mark_schedule_failure(ret))) {
            LOG_WARN("fail to mark storage cache task scheduling failure",
                KR(ret), KR(tmp_ret), K(tablet_id), K(tenant_id_), KPC(task));
          } else if (OB_EAGAIN == ret) {
            ATOMIC_STORE(&has_retry_waiting_task_, true);
            ret = OB_SUCCESS;
            LOG_TRACE("storage cache task waits for scheduling retry",
                K(tablet_id), K(tenant_id_), KPC(task));
          }
        }
      }
    }
  }
  LOG_TRACE("[SCP]storage cache policy push task into map and queue", KR(ret), K_(tenant_id), K(task_handle));
  return ret;
}

int ObStorageCacheTabletScheduler::regenerate_task(const ObStorageCacheTabletTaskHandle &task_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant tablet storage cache tablet scheduler not init", KR(ret));
  } else if (!task_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task handle", KR(ret), K(task_handle));
  } else if (task_handle()->is_canceled()) {
    // resume 线程可能已复制 SUSPENDED task handle；schema scan 强制取消后在此复查，
    // 避免快照中的不可服务对象任务继续 regenerate。
    LOG_INFO("skip regenerating task canceled by schema", KPC(task_handle.get_ptr()));
  } else if (task_handle()->is_partition_task()) {
    if (OB_FAIL(generate_partition_task(task_handle()->get_tenant_id(), task_handle()->get_tablet_id(),
        task_handle()->get_policy_status()))) {
      LOG_WARN("fail to regenerate partition task", KR(ret), KPC(task_handle.get_ptr()));
    }
  } else if (task_handle()->is_block_task()) {
    if (OB_FAIL(generate_block_task(task_handle()->get_tenant_id(), task_handle()->get_tablet_id(), task_handle()->get_block_storage_cache_policy()))) {
      LOG_WARN("fail to regenerate block task", KR(ret), KPC(task_handle.get_ptr()));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task type", KR(ret), KPC(task_handle.get_ptr()));
  }
  return ret;
}

void ObStorageCacheTabletScheduler::handle(void *task_handle)
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);
  ObSCPTraceIdGuard scp_trace_id_guard;
  ObStorageCacheTabletTaskHandle *tablet_task_handle = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else if (OB_ISNULL(task_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task_handle));
  } else {
    tablet_task_handle = static_cast<ObStorageCacheTabletTaskHandle *>(task_handle);
    if (OB_ISNULL(tablet_task_handle) || OB_ISNULL((*tablet_task_handle)())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet task handle is null", KR(ret), K(tablet_task_handle), K((*tablet_task_handle)()));
    } else if (OB_FAIL((*tablet_task_handle)()->process())) {
      LOG_WARN("fail to process task", KR(ret), K(tablet_task_handle));
    } else if (((*tablet_task_handle)()->is_uncompleted())) {
      LOG_WARN("invalid task status", KR(ret), K(tablet_task_handle), KPC((*tablet_task_handle)()));
    } else if ((*tablet_task_handle)()->need_retry_skipped_task()) {
      exist_skipped_tablet_ = true;
    }
    LOG_TRACE("[SCP]storage cache policy handle task process", KR(ret), KPC(tablet_task_handle->get_ptr()), K(tablet_task_map_.size()));
    // After the task is completed, the task record is not deleted actively, waiting for the scheduled task to be cleaned up.
    // Todo(baonian.wcx): Users can filter the virtual table to see the task records in the completed state
  }
  if (OB_NOT_NULL(tablet_task_handle)) {
    // Always delete the old task handle after processing
    OB_DELETE(ObStorageCacheTabletTaskHandle, "SCPTaskHandle", tablet_task_handle);
  }
  LOG_TRACE("storage cache policy handle task end", KR(ret), K(tablet_task_map_.size()));
}

void ObStorageCacheTabletScheduler::handle_drop(void *task_handle)
{
  int ret = OB_SUCCESS;
  ObStorageCacheTabletTaskHandle *tablet_task_handle = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else if (OB_ISNULL(task_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task_handle));
  } else {
    // if the thread has set stop, just try to free memory
    tablet_task_handle = static_cast<ObStorageCacheTabletTaskHandle *>(task_handle);
    if (OB_ISNULL(tablet_task_handle) || OB_ISNULL((*tablet_task_handle)())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tablet task handle is null", KR(ret), K(tablet_task_handle), K((*tablet_task_handle)()));
    } else if (OB_FAIL(tablet_task_map_.erase_refactored((*tablet_task_handle)()->get_tablet_id()))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to erase task", KR(ret), K((*tablet_task_handle)()->get_tablet_id()));
      }
    } 

    if (OB_NOT_NULL(tablet_task_handle)) {
      OB_DELETE(ObStorageCacheTabletTaskHandle, "SCPTaskHandle", tablet_task_handle);
    }
  }
}

int ObStorageCacheTabletScheduler::convert_hot_to_auto(const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  const uint64_t tenant_id = MTL_ID();
  int64_t ls_id = ObLSID::INVALID_LS_ID;
  ObStorageCacheTabletTaskHandle ori_task_handle;
  bool skip_non_active_tablet = false;

  data_block_ids.set_attr(ObMemAttr(tenant_id, "AutoDataMacros"));
  meta_block_ids.set_attr(ObMemAttr(tenant_id, "AutoMetaMacros"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not init", KR(ret));
  } else if (common::ObTabletID::INVALID_TABLET_ID == tablet_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), KPC(this));
  } else if (OB_FAIL(active_tablet_ids_.exist_refactored(tablet_id))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      skip_non_active_tablet = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check exist in active tablet ids", KR(ret), K(tablet_id), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!skip_non_active_tablet) {
    if (OB_FAIL(tablet_task_map_.get_refactored(tablet_id, ori_task_handle))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get task from map", KR(ret), K(tablet_id));
      }
    } else if (OB_UNLIKELY(!ori_task_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ori task handle is invalid", KR(ret), K(ori_task_handle));
    } else if (OB_FAIL(ori_task_handle()->set_status(
        ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED))) {
      LOG_WARN("fail to cancel origin task", KR(ret), KPC(ori_task_handle.get_ptr()));
    } else if (OB_FAIL(get_tablet_ls_id(tenant_id_, tablet_id, ls_id))) {
      LOG_WARN("fail to get tablet ls id", KR(ret), K(tenant_id), K(tablet_id), KPC(this));
    } else if (OB_FAIL(get_macro_blocks(ObLSID(ls_id), ObTabletID(tablet_id), data_block_ids, meta_block_ids))) {
      LOG_WARN("fail to get macro blocks", KR(ret), K(tenant_id), K(tablet_id), KPC(this));
    } else if (OB_FAIL(convert_macro_blks_to_auto(data_block_ids))) {
      LOG_WARN("modify data macro blocks fail", KR(ret), K(tenant_id), K(tablet_id), K(data_block_ids), KPC(this));
    } else if (OB_FAIL(convert_macro_blks_to_auto(meta_block_ids))) {
      LOG_WARN("modify meta macro blocks fail", KR(ret), K(tenant_id), K(tablet_id), K(meta_block_ids), KPC(this));
    }
  }
  return ret;
}


int ObStorageCacheTabletScheduler::convert_macro_blks_to_auto(const ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  bool is_macro_cache_hit = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not init", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), KPC(this));
  } else {
    for (int i = 0; i < block_ids.count() && OB_SUCC(ret); i++) {
      const MacroBlockId &cur_block_id = block_ids.at(i);
      is_macro_cache_hit = false;
      if (OB_FAIL(macro_cache_mgr->exist(cur_block_id, is_macro_cache_hit))) {
        LOG_WARN("fail to check if exist", KR(ret), K(cur_block_id), KPC(this));
      } else if (is_macro_cache_hit) { // macro cache hit
        if (OB_FAIL(macro_cache_mgr->modify_macro_cache_type(cur_block_id, 
            ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, ObSSMacroCacheType::MACRO_BLOCK))) {
          LOG_WARN("fail to modify cache type for auto task", KR(ret), KPC(this), K(cur_block_id));
        }
      } else { // macro cache miss
        // do nothing
      }
    }
  }
  return ret;
}

class ObStorageCacheGetUnfinishedTabletFunc
{
public:
  ObStorageCacheGetUnfinishedTabletFunc(ObIArray<uint64_t> &unfinished_tablet_ids)
      : unfinished_tablet_ids_(unfinished_tablet_ids)
  {}

  int operator()(const hash::HashMapPair<int64_t, ObStorageCacheTabletTaskHandle> &entry)
  {
    int ret = OB_SUCCESS;
    if (!entry.second.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("task handle is invalid", KR(ret), K(entry.first));
    } else if (entry.second()->need_retry_skipped_task()) {
      if (OB_FAIL(unfinished_tablet_ids_.push_back(entry.first))) {
        LOG_WARN("fail to push tablet id to tablet_ids_in_tablet_map", KR(ret), K(unfinished_tablet_ids_.count()));
      }
    }
    return ret;
  }
private:
  ObIArray<uint64_t> &unfinished_tablet_ids_;
};

int ObStorageCacheTabletScheduler::get_skipped_macro_cache_tasks(ObIArray<uint64_t> &unfinished_tablet_ids)
{
  int ret = OB_SUCCESS;
  unfinished_tablet_ids.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else {
    ObStorageCacheGetUnfinishedTabletFunc get_unfinished_task_func(unfinished_tablet_ids);
    if (OB_FAIL(unfinished_tablet_ids.reserve(tablet_task_map_.size()))) {
      LOG_WARN("fail to reserve", KR(ret), "size", tablet_task_map_.size());
    } else if ((tablet_task_map_.size() > 0) &&
               OB_FAIL(tablet_task_map_.foreach_refactored(get_unfinished_task_func))) {
      LOG_WARN("fail to get unfinished tablet tasks", KR(ret), K(tablet_task_map_.size()));
    }
  }
  return ret;
}

int ObStorageCacheTabletScheduler::trigger_skipped_macro_cache_tablets()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 128> unfinished_tablet_ids;
  unfinished_tablet_ids.set_attr(ObMemAttr(tenant_id_, "SkippedTablet"));
  ObStorageCacheTabletTaskHandle tmp_current_task_handle;
  uint64_t tablet_id = common::ObTabletID::INVALID_TABLET_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else if (OB_FAIL(get_skipped_macro_cache_tasks(unfinished_tablet_ids))) {
    LOG_WARN("fail to get unfinished tasks", KR(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unfinished_tablet_ids.count(); i++) {
      tmp_current_task_handle.reset();
      tablet_id = unfinished_tablet_ids.at(i);
      if (OB_FAIL(tablet_task_map_.get_refactored(tablet_id, tmp_current_task_handle))) {
        LOG_WARN("fail to get current task handle in tablet_task_map", KR(ret), K(tablet_id), KPC(this));
      } else if (OB_UNLIKELY(!tmp_current_task_handle.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("task handle is invalid", KR(ret), K(tmp_current_task_handle), K(i), KPC(this));
      } else if (tmp_current_task_handle()->need_retry_skipped_task()) {
        // If the current task still meets the conditions of being completed or failed, 
        // and the macro block cache warm-up is skipped at the same time.
        // We trigger it as a new task again.
        if (OB_FAIL(regenerate_task(tmp_current_task_handle))) {
          LOG_WARN("fail to regenerate task", KR(ret), K(tablet_id), KPC(this));
        }
      }
    }
  }
  return ret;
}

class ObCollectRetryWaitingTabletIdFunc
{
public:
  explicit ObCollectRetryWaitingTabletIdFunc(ObIArray<uint64_t> &retry_tablet_ids)
      : retry_tablet_ids_(retry_tablet_ids)
  {}

  int operator()(const hash::HashMapPair<int64_t, ObStorageCacheTabletTaskHandle> &entry)
  {
    int ret = OB_SUCCESS;
    const int64_t tablet_id = entry.first;
    if (!entry.second.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task handle is invalid", KR(ret), K(tablet_id));
    } else if (entry.second()->need_retry_push_task()
        && OB_FAIL(retry_tablet_ids_.push_back(tablet_id))) {
      const int64_t retry_tablet_count = retry_tablet_ids_.count();
      LOG_WARN("fail to push retry waiting tablet id",
          KR(ret), K(tablet_id), K(retry_tablet_count));
    }
    return ret;
  }

private:
  ObIArray<uint64_t> &retry_tablet_ids_;
};

int ObStorageCacheTabletScheduler::get_retry_waiting_tablet_ids(
    ObIArray<uint64_t> &retry_tablet_ids) const
{
  int ret = OB_SUCCESS;
  retry_tablet_ids.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else {
    const int64_t task_count = tablet_task_map_.size();
    ObCollectRetryWaitingTabletIdFunc collect_retry_waiting_tablet_id_func(retry_tablet_ids);
    if (OB_LIKELY(task_count > 0)
        && OB_FAIL(tablet_task_map_.foreach_refactored(collect_retry_waiting_tablet_id_func))) {
      LOG_WARN("fail to get retry waiting storage cache tasks", KR(ret), K(task_count));
    }
  }
  return ret;
}

int ObStorageCacheTabletScheduler::trigger_retry_waiting_tasks()
{
  int ret = OB_SUCCESS;
  int64_t queue_task_count = 0;
  ObSEArray<uint64_t, 128> retry_tablet_ids;
  retry_tablet_ids.set_attr(ObMemAttr(tenant_id_, "RetryTablet"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret));
  } else if (OB_FAIL(TG_GET_QUEUE_NUM(tg_id_, queue_task_count))) {
    LOG_WARN("fail to get storage cache task queue count", KR(ret), K(tg_id_));
  } else if (queue_task_count > 0) {
    if (REACH_TIME_INTERVAL(10_s)) {
      LOG_INFO("skip enqueue retry while storage cache task queue is not empty",
          K(queue_task_count), K(tg_id_));
    }
  } else if (OB_FAIL(get_retry_waiting_tablet_ids(retry_tablet_ids))) {
    LOG_WARN("fail to get retry waiting storage cache tasks", KR(ret), KPC(this));
  } else {
    ATOMIC_STORE(&has_retry_waiting_task_, false);
    const int64_t retry_tablet_count = retry_tablet_ids.count();
    int64_t canceled_inactive_task_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < retry_tablet_count; ++i) {
      const uint64_t tablet_id = retry_tablet_ids.at(i);
      ObStorageCacheTabletTaskHandle current_task_handle;
      bool is_active = false;
      if (OB_FAIL(tablet_task_map_.get_refactored(tablet_id, current_task_handle))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get retry waiting task from map", KR(ret), K(tablet_id), KPC(this));
        }
      } else if (OB_UNLIKELY(!current_task_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("retry waiting task handle is invalid",
            KR(ret), K(tablet_id), K(current_task_handle), KPC(this));
      } else if (!current_task_handle()->need_retry_push_task()) {
        // The task state changed after the snapshot was taken; no action is required.
      } else if (OB_FAIL(check_is_active_tablet(tablet_id, is_active))) {
        LOG_WARN("fail to check retry waiting tablet is active",
            KR(ret), K(i), K(tablet_id), K(retry_tablet_count), KPC(this));
      } else if (!is_active) {
        if (OB_FAIL(current_task_handle()->set_canceled())) {
          LOG_WARN("fail to cancel retry waiting task for inactive tablet",
              KR(ret), K(i), K(tablet_id), K(retry_tablet_count), KPC(this));
        } else {
          ++canceled_inactive_task_count;
        }
      } else if (OB_FAIL(regenerate_task(current_task_handle))) {
        LOG_WARN("fail to regenerate retry waiting task",
            KR(ret), K(i), K(tablet_id), K(retry_tablet_count), KPC(this));
      }
    }
    if (canceled_inactive_task_count > 0) {
      LOG_INFO("cancel retry waiting tasks for inactive tablets",
          K(canceled_inactive_task_count), K(retry_tablet_count), K(tenant_id_));
    }

    // Rescan to preserve retry signals after failed map overwrites, failed regeneration,
    // or concurrent insertion of tasks waiting for an enqueue retry.
    int tmp_ret = OB_SUCCESS;
    retry_tablet_ids.reuse();
    if (OB_TMP_FAIL(get_retry_waiting_tablet_ids(retry_tablet_ids))) {
      ATOMIC_STORE(&has_retry_waiting_task_, true);
      LOG_WARN("fail to check remaining retry waiting tasks", KR(ret), KR(tmp_ret), KPC(this));
      ret = COVER_SUCC(tmp_ret);
    } else if (retry_tablet_ids.count() > 0) {
      ATOMIC_STORE(&has_retry_waiting_task_, true);
    }
  }
  return ret;
}

int ObStorageCacheTabletScheduler::cancel_task_by_tablet_id(const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  ObStorageCacheTabletTaskHandle ori_task_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not init", KR(ret));
  } else if (OB_UNLIKELY(common::ObTabletID::INVALID_TABLET_ID == tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), KPC(this));
  } else if (OB_FAIL(tablet_task_map_.get_refactored(tablet_id, ori_task_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get task from map", KR(ret), K(tablet_id));
    }
  } else if (OB_UNLIKELY(!ori_task_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ori task handle is invalid", KR(ret), K(ori_task_handle));
  } else if (OB_FAIL(ori_task_handle()->set_status(
      ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED))) {
    LOG_WARN("fail to cancel origin task", KR(ret), KPC(ori_task_handle.get_ptr()));
  }
  return ret;
}

int ObStorageCacheTabletScheduler::invalidate_task_by_tablet_id(const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  ObStorageCacheTabletTaskHandle task_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet scheduler not init", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(common::ObTabletID::INVALID_TABLET_ID == tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_task_map_.get_refactored(tablet_id, task_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get task from map", KR(ret), K(tablet_id));
    }
  } else if (OB_UNLIKELY(!task_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task handle is invalid", KR(ret), K(tablet_id), K(task_handle));
  } else if (OB_FAIL(task_handle()->set_canceled())) {
    LOG_WARN("fail to cancel task by schema", KR(ret), K(tablet_id), KPC(task_handle.get_ptr()));
  }
  return ret;
}

// micro cache is mark deleted, and clear in async thread; read macro cache is evicted directly, while write macro cache will first write to object storage, then convert to read cache.
int ObStorageCacheTabletScheduler::evict_local_cache(const int64_t tablet_id, int64_t &macro_write_cache_cnt)
{
  int ret = OB_SUCCESS;
  bool skip_non_active_tablet = false;
  ObStorageCacheTabletTaskHandle ori_task_handle;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  int64_t macro_read_cache_cnt = 0;
  const uint64_t tenant_id = MTL_ID();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not init", KR(ret));
  } else if (common::ObTabletID::INVALID_TABLET_ID == tablet_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), KPC(this));
  } else if (OB_FAIL(active_tablet_ids_.exist_refactored(tablet_id))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      skip_non_active_tablet = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check exist in active tablet ids", KR(ret), K(tablet_id), K(tenant_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (skip_non_active_tablet) {
    // do nothing
  } else if (OB_FAIL(cancel_task_by_tablet_id(tablet_id))) {
    LOG_WARN("fail to cancel task by tablet id", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMacroCacheMgr is null", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(macro_cache_mgr->evict_by_tablet_id(ObTabletID(tablet_id), macro_read_cache_cnt, macro_write_cache_cnt))) {
    LOG_WARN("fail to delete tablet macro cache", KR(ret), K(tablet_id));
  }
  return ret;
}

void ObStorageCacheTabletScheduler::destroy()
{
  if (IS_INIT && OB_LIKELY(OB_INVALID_TG_ID != tg_id_)) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = OB_INVALID_TG_ID;
  tablet_task_map_.destroy();
  exist_skipped_tablet_ = false;
  ATOMIC_STORE(&has_retry_waiting_task_, false);
  is_inited_ = false;
}

/******************ObStorageCacheTabletTaskStatus*******************/
static const char *task_status_strs[] = {"WAITING", "DOING", "FINISHED", "FAILED", "SUSPENDED", "CANCELED"};
const char *ObStorageCacheTaskStatus::get_str(const TaskStatus &type)
{
  const char *str = nullptr;
  if (type < 0 || type >= TaskStatus::OB_STORAGE_CACHE_TASK_STATUS_MAX) {
    str = "UNKNOWN";
  } else {
    str = task_status_strs[type];
  }
  return str;
}



/******************ObStorageCacheTabletTask*******************/
ObStorageCacheTabletTask::ObStorageCacheTabletTask()
  : is_inited_(false),
    is_stop_(false),
    is_skipped_macro_cache_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(ObLSID::INVALID_LS_ID),
    tablet_id_(common::ObTabletID::INVALID_TABLET_ID),
    ref_count_(0),
    block_storage_cache_policy_(),
    task_type_(ObStorageCacheTaskType::OB_STORAGE_CACHE_TASK_TYPE_MAX),
    task_lock_(common::ObLatchIds::STORAGE_CACHE_POLICY_TASK_LOCK),
    status_(ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX),
    policy_status_(PolicyStatus::MAX_STATUS),
    start_time_us_(0),
    end_time_us_(0),
    comment_(),
    comment_allocator_("SCPTaskComment"),
    result_(OB_SUCCESS),
    prewarm_stat_()
{
}

int ObStorageCacheTabletTask::init_partition_task(
                              const uint64_t tenant_id, 
                              const int64_t tablet_id,
                              const PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || common::ObTabletID::INVALID_TABLET_ID == tablet_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(tablet_id), K(policy_status_));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = tablet_id;
    status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT;
    policy_status_ = policy_status;
    task_type_ = ObStorageCacheTaskType::OB_STORAGE_CACHE_PARTITION_TASK;
    is_inited_ = true;
    comment_allocator_.set_tenant_id(tenant_id);
  }
  return ret;
}

int ObStorageCacheTabletTask::init_block_task(
                              const uint64_t tenant_id, 
                              const int64_t tablet_id, 
                              const ObStorageCachePolicy &block_storage_cache_policy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || common::ObTabletID::INVALID_TABLET_ID == tablet_id
      || !block_storage_cache_policy.is_block_time_policy())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(tablet_id), K(block_storage_cache_policy));
  } else {
    tenant_id_ = tenant_id;
    tablet_id_ = tablet_id;
    status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT;
    task_type_ = ObStorageCacheTaskType::OB_STORAGE_CACHE_BLOCK_TASK;
    block_storage_cache_policy_ = block_storage_cache_policy;
    policy_status_ = PolicyStatus::MACRO_HOT;
    is_inited_ = true;
    comment_allocator_.set_tenant_id(tenant_id);
  }
  return ret;
}

// Only used for test!!!!
int ObStorageCacheTabletTask::init_for_test(
                              const uint64_t tenant_id, 
                              const int64_t ls_id, 
                              const int64_t tablet_id, 
                              const PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || ObLSID::INVALID_LS_ID == ls_id
      || common::ObTabletID::INVALID_TABLET_ID == tablet_id
      || !ObStorageCachePolicyStatus::is_valid(policy_status)
      || PolicyStatus::NONE == policy_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(ls_id), K(tablet_id), K(policy_status));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT;
    task_type_ = ObStorageCacheTaskType::OB_STORAGE_CACHE_PARTITION_TASK;
    policy_status_ = policy_status;
    is_inited_ = true;
    comment_allocator_.set_tenant_id(tenant_id);
  }
  return ret;
}
void ObStorageCacheTabletTask::destroy()
{
  comment_.reset();
  comment_allocator_.reset();
  is_inited_ = false;
  task_type_ = ObStorageCacheTaskType::OB_STORAGE_CACHE_TASK_TYPE_MAX;
  policy_status_ = PolicyStatus::MAX_STATUS;
  status_ = ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX;
  block_storage_cache_policy_.reset();
  tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_ = ObLSID::INVALID_LS_ID;
  ref_count_ = 0;
  task_lock_.destroy();
  start_time_us_ = 0;
  end_time_us_ = 0;
  result_ = OB_SUCCESS;
  prewarm_stat_.reset();
  is_skipped_macro_cache_ = false;
  is_stop_ = false;
}

ERRSIM_POINT_DEF(ERRSIM_SLEEP_STORAGE_CACHE_TASK_ERROR);

int ObStorageCacheTabletTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not init", KR(ret));
  } else {
    bool succ = false;
    ObStorageCacheTaskStatusType origin_status =
        ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX;
    set_start_time(ObTimeUtility::fast_current_time());

    switch (get_status()) {
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED: 
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED: {
        LOG_WARN("task is canceled or suspended", KR(ret), KPC(this));
        break;
      }
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT: {
        if (OB_FAIL(set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING,
            origin_status, succ))) {
          LOG_WARN("fail to update task to doing status", KR(ret), KPC(this));
        } else if (!succ) {
          // do nothing
        } else if (OB_FAIL(do_prewarm())) {
          // if ret is OB_CANCELED, means task is canceled by new task or suspended
          if (OB_CANCELED == ret) {
            const ObStorageCacheTaskStatusType cur_task_status = get_status();
            if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED != cur_task_status
                && ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED != cur_task_status) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to do prewarm, task is in unexpected status", KR(ret), KPC(this));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            LOG_WARN("fail to do prewarm", KR(ret), KPC(this));
          }
        } else if (OB_UNLIKELY(ERRSIM_SLEEP_STORAGE_CACHE_TASK_ERROR)) {
          LOG_WARN("set doing sleep 5s");
          ob_usleep(5 * 1000 * 1000);
          ret = OB_SUCCESS;
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(set_status(
            ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED))) {
          LOG_WARN("fail to update task status", KR(ret), KPC(this));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid task status", KR(ret), KPC(this));
        break;
      }
    }

    if (OB_FAIL(ret) && OB_TMP_FAIL(set_status(
        ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED))) {
      LOG_WARN("fail to update task status", KR(ret), KR(tmp_ret), KPC(this));
    }

    set_end_time(ObTimeUtility::fast_current_time());
    LOG_INFO("[SCP] finish processing storage cache tablet task", KR(ret), KPC(this));
  }
  return ret;
}

int ObStorageCacheTabletTask::do_prewarm()
{
  int ret = OB_SUCCESS;
  const int64_t tenant_mem = MTL_MEM_SIZE();
  // < 8GB: 8
  // < 16GB: 16
  // < 24GB: 24
  // ...
  // max: 64
  const int64_t parallelism = MIN(64, (tenant_mem / 8 / 1024 / 1024 / 1024) * 8 + 8);
  ObStorageCachePolicyPrewarmer prewarmer(parallelism);

  const ObTabletID tablet_id(tablet_id_);
  // LS inner tablet task 以 tablet_id 表示 group，不绑定单一 ls_id。
  if (!tablet_id.is_ls_inner_tablet()) {
    if (OB_FAIL(get_tablet_ls_id(tenant_id_, tablet_id_, ls_id_))) {
      LOG_WARN("fail to get tablet ls id", KR(ret), KPC(this));
    }
  }
  if (FAILEDx(prewarmer.prewarm_tablet(this))) {
    LOG_WARN("fail to prewarm hot tablet", KR(ret), KPC(this));
  }
  {
    SpinWLockGuard guard(task_lock_);
    result_ = ret;
  }
  set_prewarm_stat(prewarmer.get_prewarm_stat());
  return ret;
}

int ObStorageCacheTabletTask::generate_comment()
{
  int ret = OB_SUCCESS;
  static constexpr int64_t COMMENT_BUF_SIZE = 512;
  char comment_buf[COMMENT_BUF_SIZE] = {0};
  int64_t pos = 0;
  int result = OB_SUCCESS;
  ObSSPrewarmStat prewarm_stat;
  {
    SpinRLockGuard guard(task_lock_);
    result = result_;
    prewarm_stat = prewarm_stat_;
  }
  const bool macro_cache_not_enough =
      (prewarm_stat.get_macro_block_bytes() >= prewarm_stat.get_macro_cache_max_available_size());
  if (OB_FAIL(databuff_printf(
      comment_buf, sizeof(comment_buf), pos,
      "size=%ld, total_macro_count=%ld, macro_block_hit_count=%ld, macro_block_add_count=%ld.",
      prewarm_stat.get_macro_block_bytes(), prewarm_stat.get_total_macro_count(),
      prewarm_stat.get_macro_block_hit_cnt(), prewarm_stat.get_macro_block_add_cnt()))) {
    LOG_WARN("fail to construct comment", KR(ret), KPC(this));
  }
  if (OB_SUCC(ret) && macro_cache_not_enough) {
    if (OB_FAIL(databuff_printf(
        comment_buf, sizeof(comment_buf), pos, " current macro cache available size(%ld) is not enough.",
        prewarm_stat.get_macro_cache_max_available_size()))) {
      LOG_WARN("fail to construct comment", KR(ret), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS == result || OB_CANCELED == result) {
  } else if (OB_FAIL(databuff_printf(
      comment_buf, sizeof(comment_buf), pos, " %s, %s.",
      common::ob_error_name(result), common::ObCurTraceId::get_trace_id_str()))) {
    LOG_WARN("fail to construct comment", KR(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(pos <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("comment is empty", KR(ret), K(pos), KPC(this));
  } else {
    SpinWLockGuard guard(task_lock_);
    comment_.reset();
    comment_allocator_.reuse();
    if (OB_FAIL(ob_write_string(comment_allocator_, comment_buf, comment_, true/*c_style*/))) {
      LOG_WARN("fail to update task comment", KR(ret), K(pos), KPC(this));
    }
  }
  return ret;
}



/**
 * @brief Attempts to transition the task to the target status with concurrency control
 * 
 * @param target_status Target state to transition to
 * @param origin_status [out] Returns the original state before transition attempt
 * @param succ          [out] Indicates whether the state transition succeeded
 * 
 * @return int - Operation status codes:
 *   OB_FAIL: Indicates invalid task state or illegal transition path
 *   OB_SUCCESS with succ=false: Valid state but transition failed due to concurrent modifications
 *   OB_SUCCESS with succ=true: Successful state transition to target_status
 */
int ObStorageCacheTabletTask::set_status(
    const ObStorageCacheTaskStatusType &target_status,
    ObStorageCacheTaskStatusType &origin_status,
    bool &succ)
{
  int ret = OB_SUCCESS;
  origin_status = ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX;
  succ = false;
  SpinWLockGuard guard(task_lock_);
  origin_status = status_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageCacheTabletTask not init", KR(ret));
  } else if (OB_UNLIKELY(!ObStorageCacheTaskStatus::is_valid(target_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", KR(ret), K(target_status), KPC(this));
  } else if (target_status == origin_status) {
    succ = true;
  } else {
    // There are two concurrent scenarios:
    // 1. Canceled by new tasks (concurrent cancellation) or 
    // 2. Suspended by background threads (concurrent suspension).
    switch (status_) {
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT: {
        if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED == target_status) {
          succ = false;
          ret = OB_INVALID_ARGUMENT;
        } else {
          succ = true;
        }
        break;
      }
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING: {
        if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT == target_status) {
          succ = false;
          ret = OB_INVALID_ARGUMENT;
        } else {
          succ = true;
        }
        break;
      }
      // resume a suspended task will create an identical duplicate task
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED:
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED: {
        if (ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT == target_status
            || ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING == target_status) {
          ret = OB_INVALID_ARGUMENT;
        }
        succ = false;
        break;
      }
      default: {
        succ = false;
        ret = OB_INVALID_ARGUMENT;
        break;
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("invalid status", KR(ret), K(target_status), KPC(this));
    } else if (succ) {
      status_ = target_status;
    }
  }
  return ret;
}

int ObStorageCacheTabletTask::set_status(const ObStorageCacheTaskStatusType &target_status)
{
  bool succ = false;
  ObStorageCacheTaskStatusType origin_status =
      ObStorageCacheTaskStatusType::OB_STORAGE_CACHE_TASK_STATUS_MAX;
  return set_status(target_status, origin_status, succ);
}

int ObStorageCacheTabletTask::mark_schedule_failure(const int schedule_ret)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(task_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet task not init", KR(ret), K(schedule_ret));
  } else if (OB_UNLIKELY(OB_SUCCESS == schedule_ret
      || ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT != status_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache task scheduling result",
        KR(ret), K(schedule_ret), K(status_), KPC(this));
  } else {
    result_ = schedule_ret;
    if (OB_EAGAIN != schedule_ret) {
      status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED;
      end_time_us_ = ObTimeUtility::fast_current_time_us();
    }
  }
  return ret;
}

ObStorageCacheTaskStatusType ObStorageCacheTabletTask::get_status() const
{
  SpinRLockGuard guard(task_lock_);
  return status_;
}

PolicyStatus ObStorageCacheTabletTask::get_policy_status() const
{
  SpinRLockGuard guard(task_lock_);
  return policy_status_;
}

bool ObStorageCacheTabletTask::is_hot() const
{
  SpinRLockGuard guard(task_lock_);
  return (PolicyStatus::HOT == policy_status_);
}

bool ObStorageCacheTabletTask::is_auto() const
{
  SpinRLockGuard guard(task_lock_);
  return (PolicyStatus::AUTO == policy_status_);
}

bool ObStorageCacheTabletTask::is_cold() const
{
  SpinRLockGuard guard(task_lock_);
  return (PolicyStatus::COLD == policy_status_);
}

bool ObStorageCacheTabletTask::is_macro_hot() const
{
  SpinRLockGuard guard(task_lock_);
  return (PolicyStatus::MACRO_HOT == policy_status_);
}

int ObStorageCacheTabletTask::set_canceled()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage cache tablet task not init", KR(ret));
  } else {
    SpinWLockGuard guard(task_lock_);
    status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED;
  }
  return ret;
}

bool ObStorageCacheTabletTask::is_canceled() const
{
  SpinRLockGuard guard(task_lock_);
  return ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED == status_;
}


bool ObStorageCacheTabletTask::is_completed() const
{
  SpinRLockGuard guard(task_lock_);
  return ObStorageCacheTaskStatus::is_completed_status(status_);
}

bool ObStorageCacheTabletTask::is_suspended() const
{
  SpinRLockGuard guard(task_lock_);
  return ObStorageCacheTaskStatus::is_suspended_status(status_);
}

bool ObStorageCacheTabletTask::is_uncompleted() const
{
  SpinRLockGuard guard(task_lock_);
  return !(ObStorageCacheTaskStatus::is_suspended_status(status_) || ObStorageCacheTaskStatus::is_completed_status(status_));
}

bool ObStorageCacheTabletTask::is_valid() const
{
  SpinRLockGuard guard(task_lock_);
  bool is_valid = (OB_INVALID_TENANT_ID != tenant_id_) &&
         (common::ObTabletID::INVALID_TABLET_ID != tablet_id_);
  if (is_block_task()) {
    is_valid = is_valid && 
              (block_storage_cache_policy_.is_block_time_policy()) && 
              (policy_status_ == PolicyStatus::MACRO_HOT);
  } else if (is_partition_task()) {
    is_valid = is_valid &&  
               (policy_status_ == PolicyStatus::HOT || policy_status_ == PolicyStatus::AUTO);
  }
  return is_valid;
}

bool ObStorageCacheTabletTask::is_skipped_macro_cache() const
{
  SpinRLockGuard guard(task_lock_);
  return (is_skipped_macro_cache_ && 
          (ObStorageCacheTaskStatus::is_finished_or_failed(status_)));
}

bool ObStorageCacheTabletTask::need_retry_skipped_task() const
{
  SpinRLockGuard guard(task_lock_);
  return is_skipped_macro_cache_
      && PolicyStatus::HOT == policy_status_
      && ObStorageCacheTaskStatus::is_finished_or_failed(status_);
}

bool ObStorageCacheTabletTask::need_retry_push_task() const
{
  SpinRLockGuard guard(task_lock_);
  return ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_INIT == status_
      && OB_EAGAIN == result_;
}

void ObStorageCacheTabletTask::set_tablet_id(const int64_t tablet_id)
{
  SpinWLockGuard guard(task_lock_);  
  tablet_id_ = tablet_id;
}

void ObStorageCacheTabletTask::set_ls_id(const int64_t ls_id)
{
  SpinWLockGuard guard(task_lock_);
  ls_id_ = ls_id;
}

int64_t ObStorageCacheTabletTask::get_tablet_id() const
{
  SpinRLockGuard guard(task_lock_);
  return tablet_id_;
}

int64_t ObStorageCacheTabletTask::get_ls_id() const
{
  SpinRLockGuard guard(task_lock_);
  return ls_id_;
}

uint64_t ObStorageCacheTabletTask::get_tenant_id() const
{
  SpinRLockGuard guard(task_lock_);
  return tenant_id_;
}

ObStorageCachePolicy ObStorageCacheTabletTask::get_block_storage_cache_policy() const
{
  SpinRLockGuard guard(task_lock_);
  return block_storage_cache_policy_;
}

ObStorageCacheTaskType ObStorageCacheTabletTask::get_task_type() const
{
  SpinRLockGuard guard(task_lock_);
  return task_type_;
}

int64_t ObStorageCacheTabletTask::get_ref_count() const
{
  SpinRLockGuard guard(task_lock_);
  return ref_count_;
}

void ObStorageCacheTabletTask::inc_ref_count()
{
  SpinWLockGuard guard(task_lock_);
  ref_count_++;
}

void ObStorageCacheTabletTask::dec_ref_count()
{
  int ref_count = 0;
  int ret = OB_SUCCESS;
  {
    SpinWLockGuard guard(task_lock_);
    ref_count_--;
    ref_count = ref_count_;
  }
  if (ref_count == 0) {
    ObStorageCacheTabletTask *tmp_task = this;
    OB_DELETE(ObStorageCacheTabletTask, "SCPTask", tmp_task);
  } else if (OB_UNLIKELY(ref_count < 0)) {
    LOG_ERROR("invalid ref count", KR(ret), K(ref_count));
  }
}

ObSSPrewarmStat ObStorageCacheTabletTask::get_prewarm_stat() const
{
  SpinRLockGuard guard(task_lock_);
  return prewarm_stat_;
}

void ObStorageCacheTabletTask::set_prewarm_stat(const ObSSPrewarmStat &stat)
{
  int ret = OB_SUCCESS;
  {
    SpinWLockGuard guard(task_lock_);
    prewarm_stat_ = stat;
    is_skipped_macro_cache_ =
        stat.get_macro_block_bytes() >= stat.get_macro_cache_max_available_size();
  }
  if (OB_FAIL(generate_comment())) {
    LOG_WARN("fail to update task comment", KR(ret), K(stat), KPC(this));
  }
}

double ObStorageCacheTabletTask::get_speed() const
{
  double speed = -1.0;  // B/s
  SpinRLockGuard guard(task_lock_);
  if (IS_INIT) {
    if (start_time_us_ > 0) {  // <= 0 means it hasn't started yet
      const int64_t used_time_us = 
          (end_time_us_ > 0 ? end_time_us_ : ObTimeUtility::fast_current_time_us()) - start_time_us_;
      if (used_time_us > 0) {
        speed = static_cast<double>(prewarm_stat_.get_macro_block_bytes())
              / (static_cast<double>(used_time_us) / 1000.0 / 1000.0);
      } else {
        LOG_WARN_RET(OB_SUCCESS, "task stat invalid", KPC(this), K(used_time_us));
      }
    } else {
      speed = 0.0;
    }
  }
  return speed;
}

int64_t ObStorageCacheTabletTask::get_start_time() const
{
  SpinRLockGuard guard(task_lock_);
  return start_time_us_;
}

int64_t ObStorageCacheTabletTask::get_end_time() const
{
  SpinRLockGuard guard(task_lock_);
  return end_time_us_;
}

int ObStorageCacheTabletTask::get_result() const
{
  SpinRLockGuard guard(task_lock_);
  return result_;
}

int ObStorageCacheTabletTask::get_comment(common::ObIAllocator &allocator, ObString &comment) const
{
  int ret = OB_SUCCESS;
  comment.reset();
  SpinRLockGuard guard(task_lock_);
  if (!comment_.empty() && OB_FAIL(ob_write_string(allocator, comment_, comment, true/*c_style*/))) {
    LOG_WARN("fail to copy task comment", KR(ret), KPC(this));
  }
  return ret;
}

void ObStorageCacheTabletTask::set_start_time(const int64_t start_time)
{
  SpinWLockGuard guard(task_lock_);
  start_time_us_ = start_time;
}

void ObStorageCacheTabletTask::set_end_time(const int64_t end_time)
{
  SpinWLockGuard guard(task_lock_);
  end_time_us_ = end_time;
}

void ObStorageCacheTabletTask::set_block_storage_cache_policy(const ObStorageCachePolicy &block_storage_cache_policy)
{
  SpinWLockGuard guard(task_lock_);
  block_storage_cache_policy_ = block_storage_cache_policy;
}

void ObStorageCacheTabletTask::set_policy_status(const PolicyStatus &policy_status)
{
  SpinWLockGuard guard(task_lock_);
  policy_status_ = policy_status;
}

void ObStorageCacheTabletTask::set_task_type(const ObStorageCacheTaskType task_type)
{
  SpinWLockGuard guard(task_lock_);
  task_type_ = task_type;
}

}
}
