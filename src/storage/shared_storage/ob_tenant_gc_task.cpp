/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_tenant_gc_task.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "src/storage/shared_storage/ob_file_manager.h"
// TODO(remove-old-gc-code): #include "src/storage/incremental/garbage_collector/ob_ss_garbage_collector.h"
#include "share/config/ob_server_config.h"
#include "rootserver/ob_server_zone_op_service.h"
#include "rootserver/ob_root_utils.h"
#include "lib/container/ob_se_array.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "src/storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_util.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_iterator.h"
#include "src/storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "src/storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"

namespace oceanbase
{
namespace storage
{

const int64_t ObTenantGCTask::GC_INTERVAL = 30L * 1000L * 1000L; //30s
// sizeof(version) + sizeof(len) + sizeof(delete_ts) < 64;
const int64_t ObIsDeletedObj::MAX_IS_DELETED_OBJ_SIZE = 64;

ObTenantGCTask::ObTenantGCTask(volatile bool &is_stopped)
  : is_inited_(false),
    is_stopped_(is_stopped)
{}

int ObTenantGCTask::init(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantGCTask has a already been inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, GC_INTERVAL, true))) {
    LOG_WARN("fail to schedule task ObTenantGCTask", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTenantGCTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantGCTask has not been inited", K(ret));
  } else {
    loop_check_tenant_private_dir_gc_();

    // DELETE_TENANT / DELETE_SERVER 任务持久化在 SYS 租户的
    // __all_shared_gc_task 中，并且只由当前 RootServer 消费。
    if (rootserver::ObRootServiceRoleChecker::is_rootserver()) {
      loop_check_tenant_shared_dir_gc_();
      loop_check_server_dir_gc_();
    }
  }
}

int ObTenantGCTask::get_gc_safe_time_val_(
    int64_t &gc_safe_time_val_us) const
{
  gc_safe_time_val_us = GCONF._shared_gc_file_expiration_time;
  return OB_SUCCESS;
}

void ObTenantGCTask::loop_check_tenant_shared_dir_gc_() const
{
  int ret = OB_SUCCESS;
  int64_t task_count = 0;
  FLOG_INFO("====== delete tenant task gc ======", KPC(this));
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObDeleteTenantTaskIterator iter;
    ObArenaAllocator allocator(ObMemAttr(OB_SYS_TENANT_ID, "DelTenantGC"));
    if (OB_FAIL(iter.init(*GCTX.sql_proxy_))) {
      LOG_WARN("failed to init delete tenant task iterator", KR(ret));
    } else {
      bool is_iter_end = false;
      ObSharedGCTask task;
      ObDeleteTenantTaskValue value;
      while (OB_SUCC(ret) && !is_stopped_ && !is_iter_end) {
        int tmp_ret = OB_SUCCESS;
        task.reset();
        value.reset();
        allocator.reuse();
        if (OB_FAIL(iter.get_next(allocator, task, value))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_iter_end = true;
          } else {
            LOG_WARN("failed to get next delete tenant task", KR(ret));
          }
        } else {
          ++task_count;
          if (OB_SUCCESS != (tmp_ret = process_delete_tenant_shared_data_(task, value))) {
            if (OB_ENTRY_EXIST == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
              LOG_INFO("skip delete tenant task claimed or finished by peer",
                  KR(tmp_ret), K(task.key_), K(value));
            } else {
              LOG_WARN("failed to process delete tenant task",
                  KR(tmp_ret), K(task.key_), K(value));
            }
          }
        }
      }
    }
  }
  LOG_INFO("finish loop_check_tenant_shared_dir_gc_", KR(ret), K(task_count), K(is_stopped_));
}

void ObTenantGCTask::loop_check_server_dir_gc_() const
{
  int ret = OB_SUCCESS;
  int64_t task_count = 0;
  FLOG_INFO("====== delete server task gc ======", KPC(this));
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else {
    ObDeleteServerTaskIterator iter;
    ObArenaAllocator allocator(ObMemAttr(OB_SYS_TENANT_ID, "DelServerGC"));
    if (OB_FAIL(iter.init(*GCTX.sql_proxy_))) {
      LOG_WARN("failed to init delete server task iterator", KR(ret));
    } else {
      bool is_iter_end = false;
      ObSharedGCTask task;
      ObDeleteServerTaskValue value;
      while (OB_SUCC(ret) && !is_stopped_ && !is_iter_end) {
        int tmp_ret = OB_SUCCESS;
        task.reset();
        value.reset();
        allocator.reuse();
        if (OB_FAIL(iter.get_next(allocator, task, value))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_iter_end = true;
          } else {
            LOG_WARN("failed to get next delete server task", KR(ret));
          }
        } else {
          ++task_count;
          if (OB_SUCCESS != (tmp_ret = process_delete_server_task_(task, value))) {
            if (OB_ENTRY_EXIST == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
              LOG_INFO("skip delete server task claimed or finished by peer",
                  KR(tmp_ret), K(task.key_), K(value));
            } else {
              LOG_WARN("failed to process delete server task",
                  KR(tmp_ret), K(task.key_), K(value));
            }
          }
        }
      }
    }
  }
  LOG_INFO("finish loop_check_server_dir_gc_", KR(ret), K(task_count), K(is_stopped_));
}

int ObTenantGCTask::process_delete_tenant_shared_data_(
    ObSharedGCTask &task,
    const ObDeleteTenantTaskValue &value) const
{
  int ret = OB_SUCCESS;
  int64_t gc_safe_time_val_us = 0;
  const int64_t start_us = ObTimeUtility::current_time_us();
  LOG_INFO("start process delete tenant task", K(task.key_), K(value));

  if (OB_UNLIKELY(!task.is_valid()
      || !is_delete_tenant_type(task.key_.type_)
      || !value.is_valid()
      || static_cast<uint64_t>(task.key_.task_id_) != value.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delete tenant task", KR(ret), K(task.key_), K(value));
  } else if (OB_FAIL(get_gc_safe_time_val_(gc_safe_time_val_us))) {
    LOG_WARN("failed to get gc safe time", KR(ret));
  } else if (value.delete_ts_ + gc_safe_time_val_us > ObTimeUtility::fast_current_time_us()) {
    LOG_TRACE("delete tenant task has not reached gc safe time", K(task.key_), K(value),
        K(gc_safe_time_val_us));
  } else if (OB_FAIL(claim_task_(task))) {
    if (OB_ENTRY_EXIST != ret && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to claim delete tenant task", KR(ret), K(task.key_), K(value));
    }
  } else {
    SERVER_EVENT_ADD("SHARED_GC", "START_TENANT_GC",
        "tenant_id", value.tenant_id_, "delete_ts", value.delete_ts_,
        "trace_id", ObCurTraceId::get_trace_id_str());

    if (is_meta_tenant(value.tenant_id_)) {
      //TODO(muwei.ym) delete meta tenant data which in sys tenant
      //delete __all_shared_meta
      //delete __all_shared_meta_control
      //delete __all_ss_tire_meta_table
      //delete __all_ss_gc_task
      //delete __all_ss_gc_ctrl_info
      if (OB_FAIL(delete_inner_table_rows_(value.tenant_id_))) {
        LOG_WARN("failed to delete inner table rows", KR(ret), K(task.key_), K(value));
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(OB_SERVER_FILE_MGR.delete_shared_tenant_dir(value.tenant_id_))) {
      LOG_WARN("failed to delete shared tenant dir", KR(ret), K(task.key_), K(value));
    } else if (OB_FAIL(finish_task_(task))) {
      LOG_WARN("failed to finish delete tenant task", KR(ret), K(task.key_), K(value));
    }

    const int64_t cost_us = ObTimeUtility::current_time_us() - start_us;
    SERVER_EVENT_ADD("SHARED_GC", "FINISH_TENANT_GC",
        "tenant_id", value.tenant_id_, "ret", ret,
        "cost_us", cost_us, "trace_id", ObCurTraceId::get_trace_id_str());
  }

  LOG_INFO("finish process_delete_tenant_shared_data_", KR(ret), K(task.key_), K(value),
      "cost_us", ObTimeUtility::current_time_us() - start_us);
  return ret;
}

int ObTenantGCTask::process_delete_server_task_(
    ObSharedGCTask &task,
    const ObDeleteServerTaskValue &value) const
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  LOG_INFO("start process delete server task", K(task.key_), K(value));
  if (OB_UNLIKELY(!task.is_valid()
      || !is_delete_server_type(task.key_.type_)
      || !value.is_valid()
      || static_cast<uint64_t>(task.key_.task_id_) != value.server_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delete server task", KR(ret), K(task.key_), K(value));
  } else if (OB_FAIL(claim_task_(task))) {
    if (OB_ENTRY_EXIST != ret && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to claim delete server task", KR(ret), K(task.key_), K(value));
    }
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.delete_remote_server_dir(value.server_id_))) {
    LOG_WARN("failed to delete remote server dir", KR(ret), K(task.key_), K(value));
  } else if (OB_FAIL(rootserver::ObServerZoneOpService::delete_server_id_in_palf_kv(
      value.server_id_))) {
    LOG_WARN("failed to delete server id in palf kv", KR(ret), K(task.key_), K(value));
  } else if (OB_FAIL(finish_task_(task))) {
    LOG_WARN("failed to finish delete server task", KR(ret), K(task.key_), K(value));
  }
  LOG_INFO("finish process delete server task", KR(ret), K(task.key_), K(value),
      "cost_us", ObTimeUtility::current_time_us() - start_us);
  return ret;
}

int ObTenantGCTask::claim_task_(ObSharedGCTask &task) const
{
  int ret = OB_SUCCESS;
  const uint64_t server_id = GCTX.get_server_id();
  const char *trace_id_str = ObCurTraceId::get_trace_id_str();
  ObString trace_id;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(task.key_));
  } else if (OB_UNLIKELY(!is_valid_server_id(server_id) || OB_ISNULL(trace_id_str))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid claim owner", KR(ret), K(task.key_), K(server_id), KP(trace_id_str));
  } else {
    trace_id.assign_ptr(trace_id_str, static_cast<int32_t>(strlen(trace_id_str)));
    if (OB_FAIL(ObSharedGCTaskTableOperator::update_server_id_and_trace_id(
        task.key_, static_cast<int64_t>(server_id), trace_id, *GCTX.sql_proxy_))) {
      if (OB_ENTRY_EXIST != ret && OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to claim shared gc task", KR(ret), K(task.key_), K(server_id), K(trace_id));
      }
    } else {
      task.server_id_ = static_cast<int64_t>(server_id);
      task.trace_id_ = trace_id;
    }
  }
  return ret;
}

int ObTenantGCTask::finish_task_(const ObSharedGCTask &task) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(task.key_));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::delete_task_and_insert_history(
      task, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to move finished shared gc task to history", KR(ret), K(task));
  }
  return ret;
}

void ObTenantGCTask::loop_check_tenant_private_dir_gc_() const
{
  int ret = OB_SUCCESS;
  FLOG_INFO("====== tenant private dir gc ======", KPC(this));
  ObArray<storage::ObTenantItem> deleted_tenant_items;
  ObArray<storage::ObTenantItem> abort_tenant_items;
  if (OB_FAIL(SERVER_STORAGE_META_SERVICE.get_tenant_items_by_status(ObTenantCreateStatus::DELETED, deleted_tenant_items))) {
    LOG_WARN("failed to get_tenant_items_by_status", K(ret), K(deleted_tenant_items.count()));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.get_tenant_items_by_status(ObTenantCreateStatus::CREATE_ABORT, abort_tenant_items))) {
    LOG_WARN("failed to get_tenant_items_by_status", K(ret), K(abort_tenant_items.count()));
  } else {
    delete_tenants_(deleted_tenant_items);
    delete_tenants_(abort_tenant_items);
  }
}

int ObTenantGCTask::delete_tenant_(
    const storage::ObTenantItem &item) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_SERVER_FILE_MGR.delete_local_tenant_dir(item.tenant_id_, item.epoch_))) {
    LOG_WARN("failed to delete local tenant dir files", KR(ret), K(item));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.delete_remote_tenant_dir(item.tenant_id_, item.epoch_))) {
    LOG_WARN("failed to delete remote tenant dir files", KR(ret), K(item));
  }
  LOG_INFO("finish private tenant dir gc", K(ret), K(item));
  return ret;
}

void ObTenantGCTask::delete_tenants_(
  ObIArray<storage::ObTenantItem> &items) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < items.count(); i++) {
    const storage::ObTenantItem &item = items.at(i);
    if (!item.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ObTenantItem is invalid", KR(ret), K(item));
    } else if (OB_FAIL(delete_tenant_(item))) {
      LOG_WARN("failed to delete_tenant", KR(ret), K(item));
    } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.delete_super_block_tenant_item(item.tenant_id_, item.epoch_))) {
      LOG_WARN("failed to delete_super_block_tenant_item", KR(ret), K(item));
    }
  }
}

int ObTenantGCTask::delete_inner_table_rows_(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_meta_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(delete_shared_meta_ctrl_(tenant_id))) {
    LOG_WARN("failed to delete shared meta control rows", KR(ret), K(tenant_id));
  } else if (OB_FAIL(delete_shared_gc_ctrl_info_(tenant_id))) {
    LOG_WARN("failed to delete shared gc control info rows", KR(ret), K(tenant_id));
  }

  LOG_INFO("finish delete_inner_table_rows_", KR(ret), K(tenant_id));
  return ret;
}

int ObTenantGCTask::delete_shared_meta_ctrl_(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_meta_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSMetaCtrlUtil::delete_rows(tenant_id, is_stopped_))) {
    LOG_WARN("failed to delete shared meta control rows", KR(ret), K(tenant_id));
  }

  LOG_INFO("finish delete_shared_meta_ctrl_", KR(ret), K(tenant_id), K(is_stopped_), "cost_us",
      ObTimeUtility::current_time_us() - start_us);
  return ret;
}

int ObTenantGCTask::delete_shared_gc_ctrl_info_(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSharedGCControlOperator::delete_row(
      *GCTX.sql_proxy_, tenant_id, affected_rows))) {
    LOG_WARN("failed to delete shared gc control info row", KR(ret), K(tenant_id));
  }

  LOG_INFO("finish delete_shared_gc_ctrl_info_", KR(ret), K(tenant_id), K(affected_rows));
  return ret;
}



} /* namespace storage */
} /* namespace oceanbase */
