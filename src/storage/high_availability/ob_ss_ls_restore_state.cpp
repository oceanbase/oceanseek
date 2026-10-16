/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 
#define USING_LOG_PREFIX STORAGE
#include "ob_ss_ls_restore_state.h"
#include "storage/high_availability/ob_ss_ls_migration.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "share/restore/ob_physical_restore_table_operator.h"
#include "observer/ob_server_event_history_table_operator.h"

using namespace oceanbase;

//================================ObSSLSRestoreStartState=======================================
ObSSLSRestoreStartState::ObSSLSRestoreStartState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::SS_RESTORE_START)
{
}

ObSSLSRestoreStartState::~ObSSLSRestoreStartState()
{
}

int ObSSLSRestoreStartState::do_restore()
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::SS_RESTORE_LS);
  logservice::ObLogRestoreHandler *log_restore_handle = nullptr;
  bool is_finish = false;
  bool is_exist = true;
  LOG_INFO("ready to start restore ls", K(ls_restore_status_), KPC(ls_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(log_restore_handle = ls_->get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log restore handle can't nullptr", K(ret), KP(log_restore_handle));
  } else if (OB_FAIL(check_ss_restore_data_finish_(is_finish))) {
    LOG_WARN("fail to check can advance status", K(ret), KPC(ls_));
  } else if (!is_finish) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      LOG_INFO("restore data has not finished, wait later", KPC(ls_));
    }
  } else if (OB_FAIL(insert_initial_ls_restore_progress_())) {
    LOG_WARN("fail to insert initial ls restore progress", K(ret));
  } else if (OB_FAIL(check_ls_meta_exist_(is_exist))) {
    LOG_WARN("fail to check ls meta exist", K(ret));
  } else if (!is_exist) {
    if (OB_FAIL(do_with_no_ls_meta_())) {
      LOG_WARN("fail to do with no meta ls", K(ret), KPC(ls_));
    }
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  }
  return ret;  
}

int ObSSLSRestoreStartState::check_ls_meta_exist_(bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObBackupDataStore store;
  const ObArray<share::ObBackupSetBriefInfo> &backup_set_array = ls_restore_arg_->get_backup_set_list();
  int idx = backup_set_array.count() - 1;
  ObBackupSSLSMetaInfosDesc meta_info;
  is_exist = false;
  
  if (idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup_set_array can't empty", K(ret), K(ls_restore_arg_));
  } else if (OB_FAIL(store.init(backup_set_array.at(idx).backup_set_path_.ptr()))) {
    LOG_WARN("fail to init backup data store", K(ret));
  } else if (OB_FAIL(store.read_ss_ls_meta_infos(allocator, meta_info))) {
    LOG_WARN("fail to read ss ls meta infos", K(ret), K(store));
  } else {
    ARRAY_FOREACH_X(meta_info.ls_metas_, i, cnt, OB_SUCC(ret)) {
      const ObSSRawMetaRow &ls_meta_row = meta_info.ls_metas_.at(i).raw_ls_meta_row_;
      share::ObLSID parsed_ls_id;
      share::SCN meta_scn;
      if (OB_FAIL(storage::ObSSMetaInfoUtil::parse_ls_meta_key(ls_meta_row.meta_key_, parsed_ls_id, meta_scn))) {
        LOG_WARN("fail to parse ls meta key", K(ret), K(ls_meta_row));
      } else if (parsed_ls_id == ls_->get_ls_id()) {
        is_exist = true;
        break;
      }
    }
  }
  return ret;
}

int ObSSLSRestoreStartState::do_with_no_ls_meta_()
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::SS_RESTORE_WAIT_LS);
  if (OB_FAIL(online_())) {
    LOG_WARN("fail to online ls", K(ret), KPC_(ls));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), K(*ls_), K(next_status));
  }
  return ret;
}

int ObSSLSRestoreStartState::check_ss_restore_data_finish_(bool &is_finish) const
{
  int ret = OB_SUCCESS;
  is_finish = false;
  share::ObPhysicalRestoreTableOperator restore_table_operator;
  const uint64_t tenant_id = ls_->get_tenant_id();
  if (OB_FAIL(restore_table_operator.init(proxy_, tenant_id, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table operator", K(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObPhysicalRestoreJob, job_info) {
      if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id, job_info))) {
        LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
      } else {
        is_finish = is_ss_restore_data_finish_(job_info.get_status());
      }
    }  
  }
  return ret;
}

bool ObSSLSRestoreStartState::is_ss_restore_data_finish_(const share::PhysicalRestoreStatus &status) const
{
  bool b_ret = true;
  switch (status) {
    case share::PhysicalRestoreStatus::PHYSICAL_RESTORE_PRE:
    case share::PhysicalRestoreStatus::PHYSICAL_RESTORE_CREATE_INIT_LS:
    case share::PhysicalRestoreStatus::PHYSICAL_SS_RESTORE_GEN_TASK:
    case share::PhysicalRestoreStatus::PHYSICAL_SS_RESTORE_DATA:
      b_ret = false;
      break;
    default:
      b_ret = true;
      break;
  }
  return b_ret;
}

//================================ObSSLSRestoreLSState=======================================
ObSSLSRestoreLSState::ObSSLSRestoreLSState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::SS_RESTORE_LS),
    retry_flag_(false)
{
}

ObSSLSRestoreLSState::~ObSSLSRestoreLSState()
{
}

int ObSSLSRestoreLSState::do_restore()
{
  int ret = OB_SUCCESS;  
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::SS_RESTORE_WAIT_LS);
  ObArray<common::ObTabletID> no_use_tablet_ids;
  LOG_INFO("ready to start restore ls", K(ls_restore_status_), KPC(ls_));
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tablet_mgr_.has_no_tablets_restoring()) {
    if (OB_FAIL(do_restore_ls_())) {
      LOG_WARN("fail to do restore ls", K(ret), KPC(ls_));
    }
  } else if (OB_FAIL(tablet_mgr_.remove_restored_tablets(no_use_tablet_ids))) {
    LOG_WARN("fail to pop restored tablets", K(ret));
  } else if (!tablet_mgr_.has_no_tablets_restoring()) {
  } else if (is_need_retry_()) {
    // next term to retry
  } else if (OB_FAIL(online_())) {
    LOG_WARN("fail to load ls inner tablet", K(ret));
  } else if (OB_FAIL(ls_->get_ls_restore_handler()->update_rebuild_seq())) {
    LOG_WARN("failed to update rebuild seq", K(ret), KPC(ls_));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  } else {
    LOG_INFO("succ to restore ls", KPC(ls_));
  }
  
  if (OB_FAIL(ret)) {
    set_retry_flag();
  }
  return ret;  
}


bool ObSSLSRestoreLSState::is_need_retry_()
{
  bool bret = retry_flag_;
  retry_flag_ = false;
  return bret;
}

int ObSSLSRestoreLSState::do_restore_ls_()
{
  int ret = OB_SUCCESS;
  ObMigrationOpArg arg;
  ObTaskId task_id;
  task_id.init(GCONF.self_addr_);
  
  if (OB_FAIL(fill_ss_ls_restore_arg_(arg))) {
    LOG_WARN("fail to fill ss ls restore arg", K(ret));
  } else if (OB_FAIL(tablet_mgr_.schedule_ls_restore(task_id))) {
    LOG_WARN("fail to schedule ls restore", K(ret), K(task_id));
  } else if (OB_FAIL(schedule_ls_restore_(arg, task_id))) {
    LOG_WARN("fail to schedule ls restore", K(ret), K(arg), K(task_id));
  }
  return ret;
}

int ObSSLSRestoreLSState::schedule_ls_restore_(const ObMigrationOpArg &arg, const ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid() || !task_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(arg), K(task_id));
  } else {
    ObSSMigrationDagNetInitParam param;
    ObTenantDagScheduler *scheduler = nullptr;
    param.arg_ = arg;
    param.task_id_ = task_id;
    param.cost_static_ = &cost_static_;
    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
    } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObSSMigrationDagNet>(&param))) {
      LOG_WARN("failed to create and add ObSSMigrationDagNet dag net", K(ret), K(arg), K(task_id));
    } else {
      SERVER_EVENT_ADD("storage_ha", "ls_restore",
        "tenant_id", MTL_ID(),
        "ls_id", arg.ls_id_.id(),
        "status", ls_restore_status_.get_status(),
        "task_id", task_id);
      LOG_INFO("success to create ss ls restore dag net", K(ret), K(param), K(task_id));
    }
  }
  return ret;
}

int ObSSLSRestoreLSState::fill_ss_ls_restore_arg_(ObMigrationOpArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();
  ObReplicaMember src(GCONF.self_addr_, 0/*timstamp*/, REPLICA_TYPE_FULL/*dummy_replica_type*/);
  ObReplicaMember dest(GCONF.self_addr_, 0/*timstamp*/, REPLICA_TYPE_FULL/*dummy_replica_type*/);
  arg.ls_id_ = ls_->get_ls_id();
  arg.type_ = ObMigrationOpType::RESTORE_LS_OP;
  arg.cluster_id_ = GCONF.cluster_id;
  arg.src_ = src;
  arg.dst_ = dest;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObMigrationOpArg is invalid", K(arg));
  } 
  return ret;
}

//================================ObSSWaitLSRestoreState=======================================
ObSSWaitLSRestoreState::ObSSWaitLSRestoreState()
  : ObILSRestoreState(ObLSRestoreStatus::SS_RESTORE_WAIT_LS)
{
}

ObSSWaitLSRestoreState::~ObSSWaitLSRestoreState()
{
}

int ObSSWaitLSRestoreState::do_restore()
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::SS_RESTORE_CLOG);
  if (OB_FAIL(check_ss_restore_ls_finish_(is_finish))) {
    LOG_WARN("fail to check ss restore ls finish", K(ret), KPC(ls_));
  } else if (!is_finish) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      LOG_INFO("not all ls finish restore, wait later", KPC(ls_));
    }
  } else if (OB_FAIL(report_start_replay_clog_lsn_())) {
    LOG_WARN("fail to report start replay clog lsn", K(ret));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  }
  return ret;  
}

int ObSSWaitLSRestoreState::check_ss_restore_ls_finish_(bool &is_finish) const
{
  int ret = OB_SUCCESS;
  is_finish = false;
  share::ObPhysicalRestoreTableOperator restore_table_operator;
  const uint64_t tenant_id = ls_->get_tenant_id();
  if (OB_FAIL(restore_table_operator.init(proxy_, tenant_id, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table operator", K(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObPhysicalRestoreJob, job_info) {
      if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id, job_info))) {
        LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
      } else {
        is_finish = is_ss_restore_ls_finish_(job_info.get_status());
      }
    }  
  }
  return ret;
}

bool ObSSWaitLSRestoreState::is_ss_restore_ls_finish_(const share::PhysicalRestoreStatus &status) const
{
  bool b_ret = true;
  switch (status) {
    case share::PhysicalRestoreStatus::PHYSICAL_RESTORE_PRE:
    case share::PhysicalRestoreStatus::PHYSICAL_RESTORE_CREATE_INIT_LS:
    case share::PhysicalRestoreStatus::PHYSICAL_SS_RESTORE_GEN_TASK:
    case share::PhysicalRestoreStatus::PHYSICAL_SS_RESTORE_DATA:
    case share::PhysicalRestoreStatus::PHYSICAL_SS_RESTORE_LS:
      b_ret = false;
      break;
    default:
      b_ret = true;
      break;
  }
  return b_ret;
}

//================================ObSSLSRestoreCLogState=======================================
ObSSLSRestoreCLogState::ObSSLSRestoreCLogState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::SS_RESTORE_CLOG)
{
}

ObSSLSRestoreCLogState::~ObSSLSRestoreCLogState()
{
}

int ObSSLSRestoreCLogState::do_restore()
{
  int ret = OB_SUCCESS;
  bool is_finish = false;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::NONE);
  if (OB_FAIL(check_clog_replay_finish_(is_finish))) {
    LOG_WARN("fail to check clog replay finish", K(ret), KPC(ls_));
  } else if (!is_finish) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      LOG_INFO("clog replay not finish, wait later", KPC(ls_));
    }
  } else if (OB_FAIL(report_finish_replay_clog_lsn_())) {
    LOG_WARN("fail to report finish replay clog lsn", K(ret));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  }
  return ret;  
}
