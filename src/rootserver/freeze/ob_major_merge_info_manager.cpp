/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "rootserver/freeze/ob_major_merge_info_manager.h"

#include "rootserver/ob_ddl_service.h"
#include "rootserver/freeze/window/ob_window_compaction_helper.h"
#include "share/ob_global_stat_proxy.h"
#include "storage/tx/ob_ts_mgr.h"
#include "rootserver/ob_tenant_balance_service.h"
namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace palf;

namespace rootserver
{

/****************************** ObMajorMergeInfoManager ******************************/
int ObMajorMergeInfoManager::init(
    uint64_t tenant_id,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.init(tenant_id, sql_proxy))) {
    LOG_WARN("fail to init zone_merge_mgr", KR(ret));
  } else if (OB_FAIL(freeze_info_mgr_.init(tenant_id, sql_proxy))) {
    LOG_WARN("fail to init freeze_info_mgr", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }

  return ret;
}

int ObMajorMergeInfoManager::try_reload()
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  if (freeze_info_mgr_.is_valid()) {
    // do nothing
  } else if (OB_FAIL(reload())) {
    LOG_WARN("fail to reload", KR(ret));
  }
  return ret;
}

int ObMajorMergeInfoManager::reload(const bool reload_zone_merge_info)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  SCN global_broadcast_scn;
  if (reload_zone_merge_info && OB_FAIL(zone_merge_mgr_.reload())) {
    LOG_WARN("fail to reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (!reload_zone_merge_info && OB_FAIL(zone_merge_mgr_.try_reload())) {
    LOG_WARN("fail to try reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_.get_global_broadcast_scn(global_broadcast_scn))) {
    LOG_WARN("fail to get broadcast version", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_.reload(global_broadcast_scn))) {
    LOG_WARN("fail to update freeze info", KR(ret), K_(tenant_id), K(global_broadcast_scn));
  } else {
    LOG_INFO("succ to reload merge info manager", K_(tenant_id), K(global_broadcast_scn));
  }
  return ret;
}

// add freeze info to inner_table
int ObMajorMergeInfoManager::set_freeze_info(const ObMajorFreezeReason freeze_reason)
{
  int ret = OB_SUCCESS;
  SCN new_frozen_scn;
  ObRecursiveMutexGuard guard(lock_);

  const int64_t fake_schema_version = 1000;
  SCN remote_snapshot_gc_scn;
  ObFreezeInfo freeze_info;

  if (OB_FAIL(try_reload())) {
    LOG_WARN("inner stat error", KR(ret));
  } else {
    ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
    // freeze get_schema_version need interactive with ddl trans but don't use gen_new_schema_version so no need check_in_rs
    // freeze disable check_newest_schema so not to use schema_guard
    ObDDLSQLTransaction trans(GCTX.schema_service_, false/*need_end_signal*/, false/*stash*/, false/*parallel*/, false/*check_in_rs*/, false/*check_newest_schema*/);

    // In 'ddl_sql_transaction.start()', it implements the semantics of 'lock_all_ddl_operation'.
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_, fake_schema_version))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(fake_schema_version));
    // 1. lock snapshot_gc_ts in __all_global_stat
    } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(
              trans, tenant_id_, remote_snapshot_gc_scn))) {
      LOG_WARN("fail to select for update", KR(ret));
    } else {
      int64_t schema_version_in_frozen_ts = 0;
      uint64_t data_version = 0;

      // 2. generate new frozen_scn
      if (OB_FAIL(generate_frozen_scn(remote_snapshot_gc_scn, new_frozen_scn))) {
        LOG_WARN("fail to generate frozen timestamp", KR(ret));
      // 3. get schema_version at frozen_scn
      } else if (OB_FAIL(get_schema_version(new_frozen_scn, schema_version_in_frozen_ts))) {
        LOG_WARN("fail to get schema version", KR(ret), K(new_frozen_scn));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
      } else {
        freeze_info.frozen_scn_ = new_frozen_scn;
        freeze_info.schema_version_ = schema_version_in_frozen_ts;
        freeze_info.data_version_ = data_version;
        // 4. insert freeze info
        if (OB_FAIL(freeze_info_proxy.set_freeze_info(trans, freeze_info))) {
          LOG_WARN("fail to set freeze info", KR(ret), K(freeze_info), K_(tenant_id));
        }
      }
    }

    ret = trans.handle_trans_in_the_end(ret);
  }

  if (FAILEDx(freeze_info_mgr_.add_freeze_info(freeze_info))) {
    LOG_WARN("fail to push back", KR(ret), K(freeze_info));
  }

  if (OB_FAIL(ret)) {
    freeze_info_mgr_.reset_freeze_info(); // reload freeze info on the next fetch
  }

  LOG_INFO("finish set freeze info", KR(ret), K(freeze_info), K_(tenant_id));
  ROOTSERVICE_EVENT_ADD("major_merge", "root_major_freeze", K_(tenant_id),
                        K(ret), "new_frozen_scn", new_frozen_scn.get_val_for_inner_table_field(),
                        "freeze_reason", major_freeze_reason_to_str(freeze_reason));
  return ret;
}

int ObMajorMergeInfoManager::set_window_compaction_info(
    const ObWindowCompactionParam &param,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("fail to get lock", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.set_window_compaction_info(param, expected_epoch))) {
    LOG_WARN("fail to set window compaction info", KR(ret), K_(tenant_id), K(param), K(expected_epoch));
  }
  return ret;
}

int ObMajorMergeInfoManager::check_can_set_ss_window_freeze_info_(const ObGlobalMergeInfo &global_merge_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!global_merge_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(global_merge_info));
  } else if (ObZoneMergeInfo::MERGE_STATUS_IDLE != global_merge_info.merge_status_.get_value()) {
    ret = OB_MAJOR_FREEZE_NOT_FINISHED;
    LOG_WARN("cannot do ss window compaction now, need wait current major_freeze finish",
        KR(ret), K_(tenant_id), K(global_merge_info));
  } else if (!global_merge_info.is_last_merge_complete()
      || global_merge_info.frozen_scn() != global_merge_info.global_broadcast_scn()) {
    ret = OB_MAJOR_FREEZE_NOT_FINISHED;
    LOG_WARN("cannot do ss window compaction now, previous merge round is not complete",
        KR(ret), K_(tenant_id), K(global_merge_info));
  } else if (global_merge_info.suspend_merging_.get_value()) {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("cannot do ss window compaction while merge is suspended", KR(ret), K_(tenant_id), K(global_merge_info));
  } else if (OB_UNLIKELY(!GCONF.enable_major_freeze)) {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("major freeze is disabled, no need to set ss window freeze info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObMajorMergeInfoManager::inner_set_ss_window_freeze_info_(
    const int64_t window_start_time_us,
    const int64_t window_duration_us,
    const ObGlobalMergeInfo &global_merge_info,
    ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  const int64_t fake_schema_version = 1000;
  SCN remote_snapshot_gc_scn;
  SCN existing_target_scn;
  int64_t existing_window_start_time_us = 0;
  bool task_exist = false;

  if (OB_UNLIKELY(window_start_time_us <= 0
      || window_duration_us <= 0
      || !global_merge_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(window_start_time_us), K(window_duration_us),
        K(global_merge_info));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", KR(ret), K_(tenant_id), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_));
  } else {
    ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
    ObDDLSQLTransaction trans(GCTX.schema_service_, false/*need_end_signal*/, false/*stash*/, false/*parallel*/,
        false/*check_in_rs*/, false/*check_newest_schema*/);
    ObGlobalStatProxy global_stat_proxy(trans, tenant_id_);
    // STEP1：锁定 global_stat，并在锁内检查待激活任务和同窗口重复触发。
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_, fake_schema_version))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(fake_schema_version));
    } else if (OB_FAIL(global_stat_proxy.get_window_compaction_context_for_update(
        remote_snapshot_gc_scn, existing_target_scn, existing_window_start_time_us, task_exist))) {
      LOG_WARN("fail to get ss window compaction context for update", KR(ret), K_(tenant_id));
    } else if (task_exist
        && (existing_target_scn > global_merge_info.frozen_scn()
            || window_start_time_us <= existing_window_start_time_us
            || window_start_time_us - existing_window_start_time_us < window_duration_us)) {
      ret = OB_MAJOR_FREEZE_NOT_FINISHED;
      LOG_WARN("ss window compaction task already exists", KR(ret), K_(tenant_id), K(existing_target_scn),
          K(existing_window_start_time_us), K(window_start_time_us), K(window_duration_us), K(global_merge_info));
    } else {
      // STEP2：生成 target_scn，并在同一事务中持久化 freeze_info 与 SS 窗口任务。
      int64_t schema_version_in_frozen_ts = 0;
      uint64_t data_version = 0;
      if (OB_FAIL(generate_frozen_scn(remote_snapshot_gc_scn, freeze_info.frozen_scn_))) {
        LOG_WARN("fail to generate frozen timestamp", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(get_schema_version(freeze_info.frozen_scn_, schema_version_in_frozen_ts))) {
        LOG_WARN("fail to get schema version", KR(ret), K_(tenant_id), K(freeze_info));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
      } else {
        freeze_info.schema_version_ = schema_version_in_frozen_ts;
        freeze_info.data_version_ = data_version;
        if (OB_FAIL(freeze_info_proxy.set_freeze_info(trans, freeze_info))) {
          LOG_WARN("fail to set freeze info", KR(ret), K_(tenant_id), K(freeze_info));
        } else if (OB_FAIL(global_stat_proxy.set_window_compaction_task(
            freeze_info.frozen_scn_, window_start_time_us))) {
          LOG_WARN("fail to set ss window compaction task",
              KR(ret), K_(tenant_id), K(freeze_info), K(window_start_time_us));
        }
      }
    }
    ret = trans.handle_trans_in_the_end(ret);
  }
  return ret;
}

int ObMajorMergeInfoManager::set_ss_window_freeze_info(const ObWindowCompactionParam &param)
{
  int ret = OB_SUCCESS;
  ObFreezeInfo freeze_info;
  ObGlobalMergeInfo global_merge_info;
  ObWindowParameters window_params;
  ObRecursiveMutexGuard guard(lock_);

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(param));
  } else if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("fail to get lock", KR(ret));
  } else if (OB_FAIL(try_reload())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.try_reload())) {
    LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_.get_snapshot(global_merge_info))) {
    LOG_WARN("fail to get ss window global merge info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(window_params.init(tenant_id_, global_merge_info))) {
    LOG_WARN("fail to init window parameters", KR(ret), K_(tenant_id), K(global_merge_info));
  } else if (window_params.is_window_compaction_stopped()) {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("ss window compaction is stopped", KR(ret), K_(tenant_id), K(window_params));
  } else if (OB_FAIL(check_can_set_ss_window_freeze_info_(global_merge_info))) {
    LOG_WARN("cannot set ss window freeze info", KR(ret), K_(tenant_id), K(param), K(global_merge_info));
  } else {
    const int64_t window_start_time_us = param.with_start_ts_
        ? param.window_start_time_us_
        : ObTimeUtility::current_time();
    if (OB_FAIL(inner_set_ss_window_freeze_info_(
        window_start_time_us, window_params.window_duration_us_, global_merge_info, freeze_info))) {
      LOG_WARN("fail to set ss window freeze info", KR(ret), K_(tenant_id), K(param), K(global_merge_info));
      freeze_info_mgr_.reset_freeze_info();
    }
  }

  if (OB_SUCC(ret)) {
    const int tmp_ret = freeze_info_mgr_.add_freeze_info(freeze_info);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to push back freeze info", KR(tmp_ret), K_(tenant_id), K(freeze_info));
      freeze_info_mgr_.reset_freeze_info();
    }
  }

  LOG_INFO("finish set ss window freeze info", KR(ret), K_(tenant_id), K(param), K(freeze_info));
  return ret;
}

int ObMajorMergeInfoManager::check_window_compaction_task(
    const SCN &target_scn,
    const int64_t task_start_time_us,
    const int64_t request_start_time_us,
    const ObWindowParameters &window_params,
    const ObGlobalMergeInfo &global_info,
    bool &need_update_window_start_time) const
{
  int ret = OB_SUCCESS;
  const int64_t window_duration_us = window_params.window_duration_us_;
  need_update_window_start_time = false;
  if (OB_UNLIKELY(!target_scn.is_valid()
      || task_start_time_us <= 0
      || request_start_time_us <= 0
      || !window_params.is_valid()
      || window_duration_us < 0
      || !global_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(target_scn), K(task_start_time_us),
        K(request_start_time_us), K(window_params), K(global_info));
  } else if (window_params.is_window_compaction_stopped()
      || global_info.suspend_merging_.get_value()
      || !GCONF.enable_major_freeze) {
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("ss window compaction is stopped, suspended or disabled",
        KR(ret), K_(tenant_id), K(global_info), K(window_params));
  } else if (target_scn > global_info.frozen_scn()
      || request_start_time_us - task_start_time_us < window_duration_us) {
    ret = OB_MAJOR_FREEZE_NOT_FINISHED;
    LOG_WARN("ss window task is pending activation or has already been triggered in current window",
        KR(ret), K_(tenant_id), K(target_scn), K(task_start_time_us), K(request_start_time_us),
        K(window_duration_us), K(global_info));
  } else if (target_scn <= global_info.last_merged_scn()) {
    LOG_INFO("previous ss window task is complete, allow generating a new target",
        K_(tenant_id), K(target_scn), K(task_start_time_us), K(request_start_time_us), K(global_info));
  } else {
    need_update_window_start_time = true;
  }
  return ret;
}

int ObMajorMergeInfoManager::try_continue_window_compaction_task(
    const ObWindowCompactionParam &param,
    bool &need_create_new_task)
{
  int ret = OB_SUCCESS;
  SCN snapshot_gc_scn;
  SCN target_scn;
  int64_t task_start_time_us = 0;
  bool task_exist = false;
  bool need_update_window_start_time = false;
  ObGlobalMergeInfo global_info;
  ObWindowParameters window_params;
  int64_t request_start_time_us = 0;
  ObRecursiveMutexGuard guard(lock_);
  need_create_new_task = false;

  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(param));
  } else if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("fail to get lock", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else {
    request_start_time_us = param.with_start_ts_
        ? param.window_start_time_us_
        : ObTimeUtility::current_time();
    ObMySQLTransaction trans;
    ObGlobalStatProxy global_stat_proxy(trans, tenant_id_);
    // 与新任务创建共用 snapshot_gc_scn 哨兵锁，读取、检查和更新任务在同一事务内完成。
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(global_stat_proxy.get_window_compaction_context_for_update(
        snapshot_gc_scn, target_scn, task_start_time_us, task_exist))) {
      LOG_WARN("fail to get ss window compaction context for update", KR(ret), K_(tenant_id));
    } else if (!task_exist) {
      // 无旧任务，由调用方继续创建新任务。
    } else if (OB_FAIL(zone_merge_mgr_.try_reload())) {
      LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(zone_merge_mgr_.get_snapshot(global_info))) {
      LOG_WARN("fail to get ss window global merge info", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(window_params.init(tenant_id_, global_info))) {
      LOG_WARN("fail to init window parameters", KR(ret), K_(tenant_id), K(global_info));
    } else if (OB_FAIL(check_window_compaction_task(target_scn, task_start_time_us, request_start_time_us,
        window_params, global_info, need_update_window_start_time))) {
      LOG_WARN("fail to check ss window task", KR(ret), K_(tenant_id), K(target_scn),
          K(task_start_time_us), K(request_start_time_us), K(window_params));
    } else if (!need_update_window_start_time) {
      // 旧任务已完成，由调用方继续创建新任务。
    } else if (OB_FAIL(global_stat_proxy.set_window_compaction_task(target_scn, request_start_time_us))) {
      LOG_WARN("fail to update ss window start time", KR(ret), K_(tenant_id), K(target_scn), K(request_start_time_us));
    }

    ret = trans.handle_trans_in_the_end(ret);
    if (OB_SUCC(ret)) {
      need_create_new_task = !need_update_window_start_time;
      if (need_update_window_start_time) {
        ROOTSERVICE_EVENT_ADD("window_compaction", "update_ss_window_start_time", K_(tenant_id),
            K(target_scn), K(task_start_time_us), K(request_start_time_us));
      }
    }
  }

  LOG_INFO("finish try continue ss window task", KR(ret), K_(tenant_id), K(need_create_new_task), K(task_exist),
      K(target_scn), K(task_start_time_us), K(request_start_time_us));
  return ret;
}

int ObMajorMergeInfoManager::finish_window_compaction(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("fail to get lock", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.finish_window_compaction(expected_epoch))) {
    LOG_WARN("fail to finish window compaction", KR(ret), K_(tenant_id), K(expected_epoch));
  }
  return ret;
}

int ObMajorMergeInfoManager::finish_ss_window_compaction(
    const int64_t expected_epoch,
    const SCN &target_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expected_epoch < 0 || !target_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(expected_epoch), K(target_scn));
  } else {
    ObRecursiveMutexGuard guard(lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("fail to get lock", KR(ret));
    } else if (OB_FAIL(zone_merge_mgr_.finish_ss_window_compaction(expected_epoch, target_scn))) {
      LOG_WARN("fail to finish ss window compaction", KR(ret), K_(tenant_id), K(expected_epoch), K(target_scn));
    }
  }
  return ret;
}

// lock guarded by caller
int ObMajorMergeInfoManager::generate_frozen_scn(
    const SCN &snapshot_gc_scn,
    SCN &new_frozen_scn)
{
  int ret = OB_SUCCESS;
  ObSnapshotTableProxy snapshot_proxy;
  ObSnapshotInfo snapshot_info;

  // build index or backup will acquire snapshot,
  // so should make sure frozen_scn will be greater max snapshot_ts.
  if (OB_FAIL(snapshot_proxy.get_max_snapshot_info(*GCTX.sql_proxy_, tenant_id_, snapshot_info))) {
   if (OB_ENTRY_NOT_EXIST == ret) {
     // no acquired snapshot
     ret = OB_SUCCESS;
   } else {
     LOG_WARN("fail to get max snapshot info", KR(ret));
   }
  }

  SCN tmp_frozen_scn;
  share::ObFreezeInfo latest_frozen_status;
  SCN local_max_frozen_scn;
  uint64_t cur_min_data_version = 0;

  ObFreezeInfo max_frozen_status;
  ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
  if (FAILEDx(freeze_info_proxy.get_max_freeze_info(*GCTX.sql_proxy_, max_frozen_status))) {
    LOG_WARN("fail to get freeze info with max frozen_scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_.get_latest_freeze_info(latest_frozen_status))) {
    LOG_WARN("fail to get latest frozen_scn", KR(ret), K(latest_frozen_status));
  } else if (FALSE_IT(local_max_frozen_scn = latest_frozen_status.frozen_scn_)) {
  } else if (max_frozen_status.frozen_scn_ != local_max_frozen_scn) {
    // after new leader updates epoch and reloads freeze_info, old leader generates one new
    // frozen_scn and can add it into __all_freeze_info (cuz not checking epoch)
    // 
    if (local_max_frozen_scn < max_frozen_status.frozen_scn_) {
      ret = OB_EAGAIN;
      LOG_WARN("max frozen_scn in cache is smaller than max frozen_scn in table, will try again",
               KR(ret), K(local_max_frozen_scn), K(max_frozen_status));
    } else { // local_max_frozen_scn > max_frozen_status.frozen_scn_
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max frozen_scn in cache is larger than max frozen_scn in table", KR(ret),
               K(local_max_frozen_scn), K(max_frozen_status));
    }
    if (max_frozen_status.is_modified_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(compaction::ADD_SUSPECT_LS_INFO(
                  compaction::MAJOR_MERGE,
                  ObDiagnoseTabletType::TYPE_RS_MAJOR_MERGE,
                  ObLSID(1)/*ls_id*/,
                  ObSuspectInfoType::SUSPECT_RS_FROZEN_SCN_ERROR,
                  static_cast<int64_t>(max_frozen_status.frozen_scn_.get_val_for_inner_table_field()),
                  static_cast<int64_t>(local_max_frozen_scn.get_val_for_inner_table_field())))) {
        LOG_WARN("failed to add suspect info", KR(tmp_ret));
      }
    }
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, cur_min_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
  } else if (cur_min_data_version < max_frozen_status.data_version_) {
    // do not allow data_version of freeze_info rollback
    ret = OB_MAJOR_FREEZE_NOT_ALLOW;
    LOG_WARN("major freeze is not allowed now, please check and upgrade observer", KR(ret),
             K_(tenant_id), K(cur_min_data_version), "last_min_data_version",
             max_frozen_status.data_version_);
  } else if (OB_FAIL(get_gts(tmp_frozen_scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  } else if ((tmp_frozen_scn <= snapshot_gc_scn)
             || (tmp_frozen_scn <= local_max_frozen_scn)
             || (tmp_frozen_scn <= snapshot_info.snapshot_scn_)) {
    // current time from gts must be greater than old ts
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get invalid frozen_timestmap", KR(ret), K(snapshot_gc_scn),
              K(tmp_frozen_scn), K(local_max_frozen_scn), K(snapshot_info));
  } else {
    new_frozen_scn = tmp_frozen_scn;
    DEL_SUSPECT_INFO(compaction::MAJOR_MERGE, ObLSID(1)/*ls_id*/, UNKNOW_TABLET_ID, share::ObDiagnoseTabletType::TYPE_RS_MAJOR_MERGE);
  }

  return ret;
}

int ObMajorMergeInfoManager::get_schema_version(
    const SCN &frozen_scn,
    int64_t &schema_version) const
{
  int ret = OB_SUCCESS;
  ObSchemaService *server_schema_service = nullptr;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_ISNULL(server_schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_schema_service is null", KR(ret));
  } else {
    ObRefreshSchemaStatus status;
    status.tenant_id_ = tenant_id_;
    // TODO snapshot_timestamp_ should be SCN
    status.snapshot_timestamp_ = frozen_scn.get_val_for_inner_table_field();

    if (OB_FAIL(server_schema_service->fetch_schema_version(status, *GCTX.sql_proxy_, schema_version))) {
      LOG_WARN("fail to fetch schema version", KR(ret), K(status));
    }
  }

  return ret;
}

int ObMajorMergeInfoManager::get_freeze_info(
    const SCN &frozen_scn,
    share::ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  if (SCN::base_scn() == frozen_scn) {
    freeze_info.frozen_scn_ = SCN::base_scn();
  } else {
    ObRecursiveMutexGuard guard(lock_);
    if (OB_FAIL(try_reload())) {
      LOG_WARN("fail to check inner stat", KR(ret));
    } else if (OB_FAIL(freeze_info_mgr_.get_freeze_info(frozen_scn, freeze_info))) {
      LOG_WARN("fail to get frozen status", KR(ret), K(frozen_scn), K(freeze_info));
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::get_local_latest_freeze_info(share::ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);
  share::ObFreezeInfo latest_freeze_info;

  if (OB_FAIL(try_reload())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(freeze_info_mgr_.get_latest_freeze_info(latest_freeze_info))) {
    LOG_WARN("inner stat error", KR(ret), K(latest_freeze_info));
  } else {
    freeze_info.assign(latest_freeze_info);
  }
  return ret;
}

int ObMajorMergeInfoManager::renew_snapshot_gc_scn()
{
  int ret = OB_SUCCESS;

  SCN cur_snapshot_gc_scn;
  SCN latest_snapshot_gc_scn;
  SCN new_snapshot_gc_scn;
  int64_t affected_rows = 0;
  ObMySQLTransaction trans;
  ObRecursiveMutexGuard guard(lock_);

  if (OB_FAIL(try_reload())) {
    LOG_WARN("inner error", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(trans, tenant_id_,
      cur_snapshot_gc_scn))) {
    LOG_WARN("fail to select snapshot_gc_scn for update", KR(ret), K_(tenant_id));
  }
  // no need to minus max_stale_time_for_weak_consistency since 4.1, because the collection of
  // multi-version data no longer depends on snapshot_gc_scn since 4.1
  else if (OB_FAIL(get_gts(new_snapshot_gc_scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  } else if (FALSE_IT(latest_snapshot_gc_scn = freeze_info_mgr_.get_snapshot_gc_scn())) {
  } else if ((new_snapshot_gc_scn <= latest_snapshot_gc_scn)
             || (cur_snapshot_gc_scn >= new_snapshot_gc_scn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid snaptshot gc time", KR(ret), K(cur_snapshot_gc_scn), K(new_snapshot_gc_scn),
      K(latest_snapshot_gc_scn));
  } else if (OB_FAIL(ObGlobalStatProxy::update_snapshot_gc_scn(trans, tenant_id_, new_snapshot_gc_scn,
      affected_rows))) {
    LOG_WARN("fail to update snapshot_gc_scn", KR(ret), K_(tenant_id), K(new_snapshot_gc_scn));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be one", KR(ret), K(affected_rows));
  } else if (OB_FAIL(freeze_info_mgr_.update_snapshot_gc_scn(new_snapshot_gc_scn))) {
    LOG_WARN("fail to update snapshot_gc_scn", KR(ret), K_(tenant_id), K(new_snapshot_gc_scn));
  }

  ret = trans.handle_trans_in_the_end(ret);

  if (OB_FAIL(ret)) {
    freeze_info_mgr_.reset_freeze_info();
  }
  LOG_INFO("renew snapshot_gc_scn", K(ret), K(new_snapshot_gc_scn), K_(tenant_id));

  return ret;
}

int ObMajorMergeInfoManager::try_gc_freeze_info()
{
  ObRecursiveMutexGuard guard(lock_);
  int ret = OB_SUCCESS;

  const int64_t MAX_KEEP_INTERVAL_NS =  300 * 24 * 60 * 60 * 1000L * 1000L * 1000L; // 300 day
  const int64_t MIN_REMAINED_VERSION_COUNT = 300;
  SCN cur_gts_scn;
  SCN min_frozen_scn;
  if (OB_FAIL(get_gts(cur_gts_scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  } else {
    min_frozen_scn = SCN::minus(cur_gts_scn, MAX_KEEP_INTERVAL_NS);
  }

  ObFreezeInfoProxy freeze_info_proxy(tenant_id_);
  ObMySQLTransaction trans;
  ObArray<ObFreezeInfo> all_freeze_info;
  SCN cur_snapshot_gc_scn;

  if (FAILEDx(try_reload())) {
    LOG_WARN("fail to try reload", K(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
    LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(trans, tenant_id_, cur_snapshot_gc_scn))) {
    LOG_WARN("fail to select snapshot_gc_scn for update", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_proxy.get_all_freeze_info(trans, all_freeze_info))) {
    LOG_WARN("fail to get all freeze info", KR(ret), K_(tenant_id));
  } else {
    const int64_t freeze_info_cnt = all_freeze_info.count();
    if (freeze_info_cnt > MIN_REMAINED_VERSION_COUNT) {
      int64_t reserved_idx = freeze_info_cnt - MIN_REMAINED_VERSION_COUNT - 1;
      const SCN &tmp_frozen_scn = all_freeze_info.at(reserved_idx).frozen_scn_;

      min_frozen_scn = MIN(min_frozen_scn, tmp_frozen_scn);
      if (OB_FAIL(freeze_info_proxy.batch_delete(trans, min_frozen_scn))) {
        LOG_WARN("fail to batch delete freeze info", KR(ret), K(min_frozen_scn));
      } else {
        // reload will later
        freeze_info_mgr_.reset_freeze_info();
        LOG_INFO("succ to batch delete freeze info", K_(tenant_id), K(min_frozen_scn));
      }
    }
  }

  ret = trans.handle_trans_in_the_end(ret);
  return ret;
}

int ObMajorMergeInfoManager::try_update_zone_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  if (OB_FAIL(try_reload())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.try_update_zone_merge_info(expected_epoch))) {
    LOG_WARN("fail to try update zone_merge_info", KR(ret), K_(tenant_id), K(expected_epoch));
  }
  return ret;
}

int ObMajorMergeInfoManager::check_snapshot_gc_scn()
{
  int ret = OB_SUCCESS;
  SCN cur_gts_scn;
  int64_t delay = 0;
  int64_t start_service_time = -1;
  int64_t total_service_time = -1;
  SCN snapshot_gc_scn;

  ObRecursiveMutexGuard guard(lock_);
  if (OB_FAIL(try_reload())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_gts(cur_gts_scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  } else if (FALSE_IT(snapshot_gc_scn = freeze_info_mgr_.get_snapshot_gc_scn())) {
  } else {
    if (snapshot_gc_scn > cur_gts_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to check snapshot_gc_scn, snapshot_gc_scn is larger than cur_gts_scn",
               KR(ret), K(snapshot_gc_scn), K(cur_gts_scn), K_(tenant_id));
    } else {
      const int64_t delay = (!snapshot_gc_scn.is_valid_and_not_min()) ? 0 :
          (ObTimeUtility::current_time() - snapshot_gc_scn.convert_to_ts());

      if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        if (delay > SNAPSHOT_GC_TS_ERROR) {
          // In order to avoid LOG_ERROR when the tenant reloads old snapshot_gc_scn due to the
          // cluster restarted. LOG_ERROR should satisfy two additional conditions:
          // 1. start_service_time > 0. start_service_time is initialized to 0 when observer starts.
          // Then it will be updated to the time when observer starts through heartbeat, which is
          // scheduled every 2 seconds.
          // 2. total_service_time > SNAPSHOT_GC_TS_ERROR.
          ObServerTableOperator st_operator;
          if (OB_FAIL(st_operator.init(GCTX.sql_proxy_))) {
            LOG_WARN("fail to init server table operator", K(ret), K_(tenant_id));
          } else if (OB_FAIL(st_operator.get_start_service_time(GCONF.self_addr_, start_service_time))) {
            LOG_WARN("fail to get start service time", KR(ret), K_(tenant_id));
          } else if (FALSE_IT(total_service_time = ObTimeUtility::current_time() - start_service_time)) {
          } else if ((start_service_time > 0) && (total_service_time > SNAPSHOT_GC_TS_ERROR)) {
            LOG_ERROR("rs_monitor_check : snapshot_gc_ts delay for a long time",
                      K(snapshot_gc_scn), K(delay), K_(tenant_id), K(start_service_time),
                      K(total_service_time));
          }
        } else if (delay > SNAPSHOT_GC_TS_WARN) {
          LOG_WARN("rs_monitor_check : snapshot_gc_ts delay for a long time",
                  K(snapshot_gc_scn), K(delay), K_(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::inner_get_min_freeze_info(ObFreezeInfo &freeze_info)
{
  int ret = OB_SUCCESS;
  SCN global_last_merged_scn;

  if (OB_FAIL(try_reload())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.try_reload())) {
    LOG_WARN("fail to try_reload zone_merge_info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_.get_global_last_merged_scn(global_last_merged_scn))) {
    LOG_WARN("fail to get global_last_merged_scn", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(freeze_info_mgr_.get_min_freeze_info_greater_than(
             global_last_merged_scn, freeze_info))) {
    LOG_WARN("fail to get freeze info", KR(ret), K(global_last_merged_scn));
  }
  return ret;
}

int ObMajorMergeInfoManager::check_need_broadcast(bool &need_broadcast)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  ObFreezeInfo freeze_info;
  if (OB_FAIL(inner_get_min_freeze_info(freeze_info))) {
    LOG_WARN("fail to get min freeze info", KR(ret), K_(tenant_id));
  } else if (freeze_info.is_valid()) {
    if (OB_FAIL(zone_merge_mgr_.check_need_broadcast(freeze_info.frozen_scn_, need_broadcast))) {
      LOG_WARN("fail to check need broadcast", KR(ret), K_(tenant_id), K(freeze_info));
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::broadcast_freeze_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  ObRecursiveMutexGuard guard(lock_);

  ObFreezeInfo freeze_info;
  ObGlobalMergeInfo global_merge_info;
  int64_t window_start_time_us = 0;
  bool is_ss_window_task = false;
  if (OB_FAIL(inner_get_min_freeze_info(freeze_info))) {
    LOG_WARN("fail to get min freeze info", KR(ret), K_(tenant_id));
  } else if (freeze_info.is_valid()) {
    if (OB_FAIL(zone_merge_mgr_.get_snapshot(global_merge_info))) {
      LOG_WARN("fail to get global merge info", KR(ret), K_(tenant_id), K(freeze_info));
    } else if (OB_FAIL(get_pending_window_task_(
            freeze_info, global_merge_info, is_ss_window_task, window_start_time_us))) {
      LOG_WARN("fail to get pending ss window task", KR(ret), K_(tenant_id), K(freeze_info), K(global_merge_info));
    } else if (is_ss_window_task) {
      if (OB_FAIL(zone_merge_mgr_.set_ss_window_compaction_info(
          freeze_info.frozen_scn_, window_start_time_us, expected_epoch))) {
        LOG_WARN("fail to set ss window compaction info", KR(ret), K_(tenant_id),
            K(freeze_info), K(window_start_time_us), K(expected_epoch));
      }
    } else if (OB_FAIL(zone_merge_mgr_.set_global_freeze_info(freeze_info.frozen_scn_, expected_epoch))) {
      LOG_WARN("fail to set global freeze info", KR(ret), K_(tenant_id), K(freeze_info), K(expected_epoch));
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::sync_window_compaction_start_time(
    const int64_t expected_epoch,
    bool &updated)
{
  int ret = OB_SUCCESS;
  SCN target_scn;
  int64_t window_start_time_us = 0;
  bool task_exist = false;
  ObGlobalMergeInfo global_info;
  ObRecursiveMutexGuard guard(lock_);
  updated = false;

  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("fail to get lock", KR(ret), K_(tenant_id));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(expected_epoch < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(expected_epoch));
  } else if (OB_FAIL(zone_merge_mgr_.try_reload())) {
    LOG_WARN("fail to try reload zone merge info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(zone_merge_mgr_.get_snapshot(global_info))) {
    LOG_WARN("fail to get global merge info", KR(ret), K_(tenant_id));
  } else if (!global_info.is_ss_window_compaction_active(global_info.frozen_scn())) {
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else {
    ObGlobalStatProxy global_stat_proxy(*GCTX.sql_proxy_, tenant_id_);
    if (OB_FAIL(global_stat_proxy.get_window_compaction_task(target_scn, window_start_time_us, task_exist))) {
      LOG_WARN("fail to get ss window compaction task", KR(ret), K_(tenant_id));
    } else if (!task_exist
        || target_scn != global_info.frozen_scn()
        || window_start_time_us <= global_info.merge_start_time()) {
      // 任务不存在、目标不匹配或开始时间未推进，无需同步。
    } else if (OB_FAIL(zone_merge_mgr_.update_window_compaction_start_time(
        target_scn, window_start_time_us, expected_epoch, updated))) {
      LOG_WARN("fail to update ss window start time",
          KR(ret), K_(tenant_id), K(target_scn), K(window_start_time_us), K(expected_epoch));
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::get_pending_window_task_(
    const ObFreezeInfo &freeze_info,
    const ObGlobalMergeInfo &global_merge_info,
    bool &is_ss_window_task,
    int64_t &window_start_time_us) const
{
  int ret = OB_SUCCESS;
  SCN target_scn;
  bool task_exist = false;
  is_ss_window_task = false;
  window_start_time_us = 0;
  if (OB_UNLIKELY(!freeze_info.is_valid() || !global_merge_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(tenant_id), K(freeze_info), K(global_merge_info));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K_(tenant_id));
  } else {
    ObGlobalStatProxy global_stat_proxy(*GCTX.sql_proxy_, tenant_id_);
    if (OB_FAIL(global_stat_proxy.get_window_compaction_task(
        target_scn, window_start_time_us, task_exist))) {
      LOG_WARN("fail to get ss window compaction task", KR(ret), K_(tenant_id), K(freeze_info));
    } else if (task_exist
        && target_scn == freeze_info.frozen_scn_
        && target_scn > global_merge_info.frozen_scn()) {
      is_ss_window_task = true;
    }
  }
  return ret;
}

int ObMajorMergeInfoManager::adjust_global_merge_info(const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge info mgr not inited", KR(ret));
  } else if (OB_FAIL(zone_merge_mgr_.adjust_global_merge_info(expected_epoch))) {
    LOG_WARN("fail to adjust global merge info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObMajorMergeInfoManager::get_gts(SCN &gts_scn) const
{
  int ret = OB_SUCCESS;
  bool is_external_consistent = true;
  const int64_t timeout_us = 10 * 1000 * 1000;

  if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id_, timeout_us, gts_scn, is_external_consistent))) {
    LOG_WARN("fail to get ts sync", K(ret), K_(tenant_id), K(timeout_us));
  } else if (!is_external_consistent) { // only suppport gts in 4.0
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("cannot freeze without gts", KR(ret), K_(tenant_id));
  }
  return ret;
}


} // rootserver
} // oceanbase
