/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_gc_task_util.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_server_struct.h"
#include "share/ob_max_id_fetcher.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{

int ObSharedGCTaskUtil::generate_mark_sweep_gc_task(
    const uint64_t tenant_id,
    const int64_t round_id,
    const ObSharedGCMacroIdList &macro_id_list,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObSharedGCTask task;
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "SharedGCTask"));
  ObString value;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || round_id <= 0
      || !macro_id_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(round_id), K(macro_id_list));
  } else if (OB_FAIL(macro_id_list.to_value(allocator, value))) {
    LOG_WARN("fail to serialize macro id list", KR(ret), K(macro_id_list));
  } else if (OB_UNLIKELY(value.length() > ObSharedGCTask::MAX_VALUE_LENGTH)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialized macro id list value size overflow", KR(ret),
        "value_length", value.length(), K(macro_id_list));
  } else if (OB_FAIL(fetch_task_id_(tenant_id, ObSharedGCTaskType::MARK_SWEEP_GC, task_id))) {
    LOG_WARN("fail to fetch mark sweep gc task id", KR(ret), K(tenant_id), K(round_id));
  } else {
    task.key_ = ObSharedGCTaskKey(tenant_id,
                                  ObSharedGCTaskType::MARK_SWEEP_GC,
                                  round_id,
                                  task_id);
    task.value_ = value;
    if (OB_FAIL(ObSharedGCTaskTableOperator::insert_task(task, client))) {
      LOG_WARN("fail to insert mark sweep gc task", KR(ret), K(task));
    } else {
      LOG_TRACE("succ to insert mark sweep gc task", K(task));
    }
  }
  return ret;
}

int ObSharedGCTaskUtil::generate_mark_sweep_gc_diag_task(
    const uint64_t tenant_id,
    const int64_t round_id,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ObSharedGCTask task;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || round_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid diagnose round", KR(ret), K(tenant_id), K(round_id));
  } else {
    task.key_ = ObSharedGCTaskKey(
        tenant_id, ObSharedGCTaskType::MARK_SWEEP_GC_DIAG, round_id, 0/*task_id*/);
    task.value_ = ObString::make_string("");
    if (OB_FAIL(ObSharedGCTaskTableOperator::insert_task(task, client))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to insert diagnose task", KR(ret), K(task.key_));
      }
    }
  }
  return ret;
}

int ObSharedGCTaskUtil::generate_fail_gc_task(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &ctrl_key,
    const ObSSMetaCtrlValue &ctrl_value,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObSharedGCTask task;
  const int64_t round_id = 0;
  ObSharedGCFailInfo fail_info;
  ObString value;
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "SharedGCTask"));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !ctrl_key.is_valid()
      || !ctrl_value.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(ctrl_key), K(ctrl_value));
  } else {
    fail_info.meta_type_ = ctrl_key.type_;
    fail_info.op_id_ = ctrl_value.ctrl_info_.op_id_;
    fail_info.ls_id_ = ls_id;
    fail_info.tablet_id_ = ctrl_key.tablet_key_.tablet_id_;
    fail_info.transfer_scn_ = ctrl_key.tablet_key_.reorg_scn_;
    fail_info.ctrl_object_info_ = ctrl_value.object_info_;
    if (OB_FAIL(fail_info.to_value(allocator, value))) {
      LOG_WARN("fail to serialize fail info", KR(ret), K(fail_info));
    } else if (OB_UNLIKELY(value.length() > ObSharedGCTask::MAX_VALUE_LENGTH)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("serialized fail info value size overflow", KR(ret),
          "value_length", value.length(), K(fail_info));
    } else if (OB_FAIL(fetch_task_id_(tenant_id, ObSharedGCTaskType::FAIL_GC, task_id))) {
      LOG_WARN("fail to fetch fail gc task id", KR(ret), K(tenant_id), K(ctrl_key), K(ctrl_value));
    } else {
      task.key_ = ObSharedGCTaskKey(tenant_id,
                                    ObSharedGCTaskType::FAIL_GC,
                                    round_id,
                                    task_id);
      task.value_ = value;
      if (OB_FAIL(ObSharedGCTaskTableOperator::insert_task(task, client))) {
        LOG_WARN("fail to insert fail gc task", KR(ret), K(task));
      } else {
        LOG_TRACE("succ to insert fail gc task", K(task));
      }
    }
  }
  return ret;
}

int ObSharedGCTaskUtil::generate_fail_gc_macro_id_history(
    const uint64_t tenant_id,
    const int64_t fail_gc_task_id,
    const ObSharedGCMacroIdList &macro_id_list,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObSharedGCTask task;
  const int64_t round_id = 0;
  ObString value;
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "SharedGCTask"));
  char extra_info_buf[ObSharedGCTask::MAX_EXTRA_INFO_LENGTH] = {0};
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || (fail_gc_task_id < 0)
      || !macro_id_list.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(fail_gc_task_id), K(macro_id_list));
  } else if (OB_FAIL(macro_id_list.to_value(allocator, value))) {
    LOG_WARN("fail to serialize macro id list", KR(ret), K(macro_id_list));
  } else if (OB_UNLIKELY(value.length() > ObSharedGCTask::MAX_VALUE_LENGTH)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("serialized macro id list value size overflow", KR(ret),
        "value_length", value.length(), K(macro_id_list));
  } else if (OB_FAIL(databuff_printf(extra_info_buf, sizeof(extra_info_buf), pos,
      "fail_gc_task_id=%ld", fail_gc_task_id))) {
    LOG_WARN("fail to build extra info", KR(ret), K(fail_gc_task_id));
  } else if (OB_FAIL(fetch_task_id_(tenant_id, ObSharedGCTaskType::FAIL_GC_MACRO_ID, task_id))) {
    LOG_WARN("fail to fetch fail gc macro id task id", KR(ret), K(tenant_id), K(fail_gc_task_id));
  } else {
    task.key_ = ObSharedGCTaskKey(tenant_id,
                                  ObSharedGCTaskType::FAIL_GC_MACRO_ID,
                                  round_id,
                                  task_id);
    task.value_ = value;
    task.extra_info_.assign_ptr(extra_info_buf, static_cast<int32_t>(pos));
    if (OB_FAIL(ObSharedGCTaskTableOperator::insert_history(task, client))) {
      LOG_WARN("fail to insert fail gc macro id history", KR(ret), K(task));
    } else {
      LOG_TRACE("succ to insert fail gc macro id history", K(task), K(fail_gc_task_id));
    }
  }
  return ret;
}

int ObSharedGCTaskUtil::generate_ls_meta_gc_task(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &meta_scn,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObSharedGCTask task;
  const int64_t round_id = 0;
  ObSharedGCLSMetaInfo ls_meta_info;
  ObString value;
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "SharedGCTask"));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(meta_scn));
  } else {
    ls_meta_info.ls_id_ = ls_id;
    ls_meta_info.meta_scn_ = meta_scn;
    if (OB_FAIL(ls_meta_info.to_value(allocator, value))) {
      LOG_WARN("fail to serialize ls meta info", KR(ret), K(ls_meta_info));
    } else if (OB_UNLIKELY(value.length() > ObSharedGCTask::MAX_VALUE_LENGTH)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("serialized ls meta info value size overflow", KR(ret),
          "value_length", value.length(), K(ls_meta_info));
    } else if (OB_FAIL(fetch_task_id_(tenant_id, ObSharedGCTaskType::LS_META_GC, task_id))) {
      LOG_WARN("fail to fetch ls meta gc task id", KR(ret), K(tenant_id), K(ls_id), K(meta_scn));
    } else {
      task.key_ = ObSharedGCTaskKey(tenant_id,
                                    ObSharedGCTaskType::LS_META_GC,
                                    round_id,
                                    task_id);
      task.value_ = value;
      if (OB_FAIL(ObSharedGCTaskTableOperator::insert_task(task, client))) {
        LOG_WARN("fail to insert ls meta gc task", KR(ret), K(task));
      } else {
        LOG_TRACE("succ to insert ls meta gc task", K(task));
      }
    }
  }
  return ret;
}

int ObSharedGCTaskUtil::generate_ls_ckpt_gc_task(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const share::SCN &meta_scn,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObSharedGCTask task;
  ObSharedGCLSCheckpointInfo ckpt_info;
  ObString value;
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "SharedGCTask"));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), K(meta_scn));
  } else {
    ckpt_info.ls_id_ = ls_id;
    ckpt_info.meta_scn_ = meta_scn;
    if (OB_FAIL(ckpt_info.to_value(allocator, value))) {
      LOG_WARN("fail to serialize ls checkpoint info", KR(ret), K(ckpt_info));
    } else if (OB_UNLIKELY(value.length() > ObSharedGCTask::MAX_VALUE_LENGTH)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("serialized ls checkpoint info value size overflow", KR(ret),
          "value_length", value.length(), K(ckpt_info));
    } else if (OB_FAIL(fetch_task_id_(tenant_id, ObSharedGCTaskType::LS_CKPT_GC, task_id))) {
      LOG_WARN("fail to fetch ls checkpoint gc task id", KR(ret), K(tenant_id), K(ls_id), K(meta_scn));
    } else {
      task.key_ = ObSharedGCTaskKey(tenant_id, ObSharedGCTaskType::LS_CKPT_GC, 0, task_id);
      task.value_ = value;
      if (OB_FAIL(ObSharedGCTaskTableOperator::insert_task(task, client))) {
        LOG_WARN("fail to insert ls checkpoint gc task", KR(ret), K(task));
      } else {
        LOG_TRACE("succ to insert ls checkpoint gc task", K(task));
      }
    }
  }
  return ret;
}

int ObSharedGCTaskUtil::generate_delete_tenant_task(
    const uint64_t target_tenant_id,
    const int64_t delete_ts,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ObSharedGCTask task;
  ObDeleteTenantTaskValue task_value;
  ObString value;
  ObArenaAllocator allocator(ObMemAttr(OB_SYS_TENANT_ID, "DelTenantGC"));
  task_value.tenant_id_ = target_tenant_id;
  task_value.delete_ts_ = delete_ts;
  if (OB_UNLIKELY(!task_value.is_valid() || target_tenant_id > INT64_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_tenant_id), K(delete_ts));
  } else if (OB_FAIL(task_value.to_value(allocator, value))) {
    LOG_WARN("fail to serialize delete tenant task value", KR(ret), K(task_value));
  } else {
    task.key_ = ObSharedGCTaskKey(
        OB_SYS_TENANT_ID,
        ObSharedGCTaskType::DELETE_TENANT,
        0 /* round_id */,
        static_cast<int64_t>(target_tenant_id));
    task.value_ = value;
    if (OB_FAIL(ObSharedGCTaskTableOperator::insert_task(task, client))) {
      LOG_WARN("fail to insert delete tenant task", KR(ret), K(task), K(task_value));
    } else {
      LOG_INFO("succ to insert delete tenant task", K(task), K(task_value));
    }
  }
  return ret;
}

int ObSharedGCTaskUtil::generate_delete_server_task(
    const uint64_t target_server_id,
    const ObAddr &server_addr,
    const int64_t delete_ts,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ObSharedGCTask task;
  ObDeleteServerTaskValue task_value;
  ObString value;
  ObArenaAllocator allocator(ObMemAttr(OB_SYS_TENANT_ID, "DelServerGC"));
  task_value.server_id_ = target_server_id;
  task_value.server_addr_ = server_addr;
  task_value.delete_ts_ = delete_ts;
  if (OB_UNLIKELY(!is_valid_server_id(target_server_id)
      || target_server_id > INT64_MAX
      || !server_addr.is_valid()
      || delete_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_server_id), K(server_addr), K(delete_ts));
  } else if (OB_FAIL(task_value.to_value(allocator, value))) {
    LOG_WARN("fail to serialize delete server task value", KR(ret), K(task_value));
  } else {
    task.key_ = ObSharedGCTaskKey(
        OB_SYS_TENANT_ID,
        ObSharedGCTaskType::DELETE_SERVER,
        0 /* round_id */,
        static_cast<int64_t>(target_server_id));
    task.value_ = value;
    if (OB_FAIL(ObSharedGCTaskTableOperator::insert_task(task, client))) {
      LOG_WARN("fail to insert delete server task", KR(ret), K(task), K(task_value));
    } else {
      LOG_INFO("succ to insert delete server task", K(task), K(task_value));
    }
  }
  return ret;
}

int ObSharedGCTaskUtil::fetch_task_id_(
    const uint64_t tenant_id,
    const ObSharedGCTaskType type,
    int64_t &task_id)
{
  int ret = OB_SUCCESS;
  uint64_t new_id = OB_INVALID_ID;
  ObMaxIdType id_type = OB_MAX_USED_SHARED_MARK_SWEEP_GC_TASK_ID_TYPE;
  ObMySQLProxy *sql_proxy = nullptr;
  task_id = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || (!is_shared_data_gc_task_type(type)
          && !is_ls_meta_gc_type(type)
          && !is_ls_ckpt_gc_type(type)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(type));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), K(tenant_id), K(type));
  } else {
    if (is_mark_sweep_gc_type(type)) {
      id_type = OB_MAX_USED_SHARED_MARK_SWEEP_GC_TASK_ID_TYPE;
    } else if (is_fail_gc_type(type)) {
      id_type = OB_MAX_USED_SHARED_FAIL_GC_TASK_ID_TYPE;
    } else if (is_fail_gc_macro_id_type(type)) {
      id_type = OB_MAX_USED_SHARED_FAIL_GC_MACRO_ID_TASK_ID_TYPE;
    } else if (is_ls_meta_gc_type(type)) {
      id_type = OB_MAX_USED_SHARED_LS_META_GC_TASK_ID_TYPE;
    } else if (is_ls_ckpt_gc_type(type)) {
      id_type = OB_MAX_USED_SHARED_LS_CKPT_GC_TASK_ID_TYPE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected gc task type", KR(ret), K(type));
    }
    if (OB_SUCC(ret)) {
      ObMaxIdFetcher id_fetcher(*sql_proxy);
      if (OB_FAIL(id_fetcher.fetch_new_max_id(tenant_id, id_type, new_id, 1 /*initial*/))) {
        LOG_WARN("fail to fetch new max id", KR(ret), K(tenant_id), K(type), K(id_type));
      } else {
        task_id = static_cast<int64_t>(new_id);
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
