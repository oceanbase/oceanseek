/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS_RESTORE
#include "ob_ss_restore_scheduler.h"
#include "rootserver/backup/ob_backup_service.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_tenant_ddl_service.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_unit_manager.h"//convert_pool_name_lis
#include "rootserver/ob_ls_service_helper.h"//create_new_ls_in_trans
#include "rootserver/ob_common_ls_service.h"//do_create_user_ls
#include "share/ob_schema_status_proxy.h"
#include "rootserver/standby/ob_standby_service.h"
#include "rootserver/restore/ob_restore_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "share/backup/ob_backup_connectivity.h"
#include "share/ob_global_stat_proxy.h" // ObGlobalStatProxy
#include "rootserver/mview/ob_collect_mv_merge_info_task.h"
#include "rootserver/mview/ob_mview_push_refresh_scn_task.h"
#include "rootserver/restore/ob_restore_common_util.h"
#include "storage/backup/ob_ss_backup_restore_util.h"
#include "share/restore/ob_restore_persist_helper.h"
#include "share/backup/ob_ss_ha_macro_block_table_operator.h"
namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace palf;

ObSSRestoreScheduler::ObSSRestoreScheduler()
  : ObRestoreScheduler(),
    task_scheduler_(nullptr),
    macro_task_mgr_()
{
}

ObSSRestoreScheduler::~ObSSRestoreScheduler()
{
}

int ObSSRestoreScheduler::init(ObRestoreService &restore_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task_scheduler_ = MTL(ObBackupTaskScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task scheduler is null", K(ret));
  } else if (OB_FAIL(ObRestoreScheduler::init(restore_service))) {
    LOG_WARN("fail to init restore scheduler", K(ret));
  }
  return ret;
}

int ObSSRestoreScheduler::process_restore_job(const share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else {
    switch (job.get_status()) {
      case PHYSICAL_RESTORE_PRE:
        ret = restore_pre(job);
        break;
      case PHYSICAL_RESTORE_CREATE_INIT_LS:
        ret = restore_init_ls(job);
        break;
      case PHYSICAL_SS_RESTORE_GEN_TASK:
        ret = generate_restore_task(job);
        break;
      case PHYSICAL_SS_RESTORE_DATA:
        ret = restore_data(job);
        break;
      case PHYSICAL_SS_RESTORE_LS:
        ret = restore_wait_ls(job);
        break;
      case PHYSICAL_SS_RESTORE_CLOG:
        ret = restore_wait_clog(job);
        break;
      case PHYSICAL_RESTORE_POST_CHECK:
        ret = post_check(job);
        break;
      case PHYSICAL_RESTORE_UPGRADE:
        ret = restore_upgrade(job);
        break;
      case PHYSICAL_RESTORE_SUCCESS:
        ret = restore_finish(job);
        break;
      case PHYSICAL_RESTORE_FAIL:
        ret = restore_finish(job);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(job));
        break;
    }
  }
  return ret;
}

PhysicalRestoreStatus ObSSRestoreScheduler::get_next_status(
  int return_ret,
  PhysicalRestoreStatus current_status)
{
  PhysicalRestoreStatus next_status = PHYSICAL_RESTORE_MAX_STATUS;
  if (OB_SUCCESS != return_ret) {
    next_status = PHYSICAL_RESTORE_FAIL;
  } else if (is_sys_tenant(MTL_ID())) {
    next_status = get_sys_next_status(current_status);
  } else {
    switch (current_status) {
      case PHYSICAL_RESTORE_PRE : {
        next_status = PHYSICAL_RESTORE_CREATE_INIT_LS;
        break;
      }
      case PHYSICAL_RESTORE_CREATE_INIT_LS : {
        next_status = PHYSICAL_SS_RESTORE_GEN_TASK;
        break;
      }
      case PHYSICAL_SS_RESTORE_GEN_TASK: {
        next_status = PHYSICAL_SS_RESTORE_DATA;
        break;
      }
      case PHYSICAL_SS_RESTORE_DATA : {
        next_status = PHYSICAL_SS_RESTORE_LS;
        break;
      }
      case PHYSICAL_SS_RESTORE_LS : {
        next_status = PHYSICAL_SS_RESTORE_CLOG;
        break;
      }
      case PHYSICAL_SS_RESTORE_CLOG : {
        next_status = PHYSICAL_RESTORE_POST_CHECK;
        break;
      }
      case PHYSICAL_RESTORE_POST_CHECK : {
        next_status = PHYSICAL_RESTORE_UPGRADE;
        break;
      }
      case PHYSICAL_RESTORE_UPGRADE : {
        next_status = PHYSICAL_RESTORE_SUCCESS;
        break;
      }
      default : {
        // do nothing
      }
    }
  }
  return next_status;
}

int ObSSRestoreScheduler::try_update_job_status(
    common::ObISQLClient &sql_client,
    int return_ret,
    const ObPhysicalRestoreJob &job,
    share::PhysicalRestoreMod mod)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(restore_op.init(&sql_client, tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("fail init", K(ret), K(tenant_id_));
  } else {
    PhysicalRestoreStatus next_status = get_next_status(return_ret, job.get_status());
    const common::ObCurTraceId::TraceId trace_id = *ObCurTraceId::get_trace_id();

    if (PHYSICAL_RESTORE_FAIL == next_status && OB_LS_RESTORE_FAILED != return_ret
        && OB_FAIL(restore_op.update_job_error_info(job.get_job_id(), return_ret, mod, trace_id, self_addr_))) {
      LOG_WARN("fail to update job error info", K(ret), K(job), K(return_ret), K(mod), K(tenant_id_));
    } else if (OB_FAIL(restore_op.update_job_status(job.get_job_id(), next_status))) {
      LOG_WARN("fail update job status", K(ret), K(job), K(next_status), K(tenant_id_));
    } else {
      restore_service_->wakeup();
      LOG_INFO("[RESTORE] switch job status", K(ret), K(job), K(next_status));
      
      (void)record_rs_event(job, next_status);
    }
  }
  return ret;
}

//TODO(mingqiao): fill restore statistics
int ObSSRestoreScheduler::fill_restore_statistics_(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObSSRestoreScheduler::generate_restore_task(const share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;

  const int total_task_count = 0; /* total_task_count will be updated later */
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    ObSSHAMacroTaskType task_type; 
    task_type.type_ = ObSSHAMacroTaskType::RESTORE;
    if (OB_FAIL(ObSSHAMacroBlockTaskOperator::insert_mgr_state_init(
                  trans, tenant_id_, job.get_job_id(), task_type, total_task_count))) {
      LOG_WARN("fail to inser mgr state init");
    } else if (OB_FAIL(try_update_job_status(trans, ret, job))) {
      LOG_WARN("fail to update job status", K(ret), K(job)); 
    }
    
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback trans", K(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObSSRestoreScheduler::restore_data(const share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  bool is_finished = false;
  bool is_failed = false;  // is the macro block task failed and should not retry
  DEBUG_SYNC(BEFORE_SS_RESTORE_DATA);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(init_macro_task_mgr_if_need_(job))) {
    LOG_WARN("fail to init macro task mgr", K(ret), K(job));
  } else if (OB_FAIL(macro_task_mgr_.process(is_finished, is_failed))) {
    LOG_WARN("fail to do macro task mgr process", K(ret), K(job));
  } else if (OB_FAIL(stat_restore_progress_(job, is_finished))) {
    LOG_WARN("fail to stat restore progress", K(ret), K(job), K(is_finished));
  }
  
  int tmp_ret = OB_SUCCESS;
  if (OB_FAIL(ret) && can_retry_(ret) && !is_failed) {
    LOG_INFO("fail to restore data, can retry", K(ret), K(job));
  } else if (!is_finished) {    
  } else if (OB_TMP_FAIL(try_update_job_status(*sql_proxy_, ret, job))) {
    LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job)); 
  }
  LOG_INFO("[RESTORE] restore data finish", K(ret), K(is_finished), K(job));
  return ret;
}

int ObSSRestoreScheduler::init_macro_task_mgr_if_need_(const share::ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  ObSSHAMacroTaskType task_type;
  task_type.type_ = ObSSHAMacroTaskType::RESTORE;  
      
  if (macro_task_mgr_.is_inited()) {
    // do nothing
  } else {
    ObBackupJobAttr job_attr;
    ObBackupSetTaskAttr set_task_attr;
    ObArray<ObLSID> ls_id_array;    
    if (OB_FAIL(fill_macro_task_mgr_init_args_(job, job_attr, set_task_attr, ls_id_array))) {
      LOG_WARN("fail to fill macro task mgr init args", K(ret), K(job));
    } else if (OB_FAIL(macro_task_mgr_.init(task_type, 
                                            job_attr, 
                                            set_task_attr, 
                                            *task_scheduler_, 
                                            *sql_proxy_, 
                                            *restore_service_, 
                                            ls_id_array))) {
      LOG_WARN("fail to init macro restore task mgr", K(ret));
    } else {
      LOG_INFO("init macro task mgr successfully");
    }
  }
  return ret;
}

int ObSSRestoreScheduler::fill_macro_task_mgr_init_args_(
      const share::ObPhysicalRestoreJob &job,
      ObBackupJobAttr &job_attr,
      ObBackupSetTaskAttr &set_task_attr,
      ObIArray<ObLSID> &ls_id_array)
{
  //TODO(yanfeng.yyy): refactor init param, remove unnecessary fields
  int ret = OB_SUCCESS;
  job_attr.tenant_id_ = tenant_id_;
  job_attr.initiator_tenant_id_ = OB_SYS_TENANT_ID;
  job_attr.backup_type_.type_ = ObBackupType::FULL_BACKUP; //fake
  job_attr.job_id_ = job.get_job_id();
  job_attr.backup_set_id_ = 1; //fake
  job_attr.encryption_mode_ = ObBackupEncryptionMode::EncryptionMode::NONE; //fake
  job_attr.incarnation_id_ = 1;
  job_attr.start_ts_ = job.get_restore_start_ts();
  job_attr.status_ = ObBackupStatus::Status::DOING; //fake
  set_task_attr.tenant_id_ = tenant_id_;
  set_task_attr.task_id_ = job.get_job_id();
  set_task_attr.encryption_mode_ = ObBackupEncryptionMode::EncryptionMode::NONE; //fake
  set_task_attr.job_id_ = job.get_job_id();
  set_task_attr.backup_set_id_ = 1; //fake
  set_task_attr.status_ = ObBackupStatus::Status::DOING; //fake
  set_task_attr.start_ts_ = job.get_restore_start_ts();
    
  const common::ObSArray<share::ObBackupSetPath> &backup_set_path_array =
    job.get_multi_restore_path_list().get_backup_set_path_list();  
  const share::ObBackupSetPath &backup_set_path = backup_set_path_array.at(backup_set_path_array.count() - 1);
  if (OB_FAIL(set_task_attr.start_scn_.convert_from_ts(job.get_restore_start_ts()))) {
    LOG_WARN("fail to convert start scn", K(ret), K(job));
  }  else {       
    ObBackupDest dest;
    ObString root_path;
    char backup_path[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    int64_t pos = 0;
    if (OB_FAIL(dest.set(backup_set_path))) {
      LOG_WARN("fail to set backup dest", K(ret));
    } else if (OB_FAIL(databuff_printf(backup_path, sizeof(backup_path), pos, "%s?%s",
                                       dest.get_root_path().ptr(), dest.get_storage_info()->endpoint_))) {
      LOG_WARN("fail to databuff printf", K(ret), K(dest));
    } else if (OB_FAIL(set_task_attr.backup_path_.assign(backup_path))) {
      LOG_WARN("fail to assign backup path", K(ret), K(backup_path));
    } else if (OB_FAIL(job_attr.backup_path_.assign(backup_path))) {
      LOG_WARN("fail to assign backup path", K(ret), K(backup_path));
    }
    
    //get ls id array
    storage::ObBackupDataLSAttrDesc backup_ls_attr;
    ObBackupDataStore store;
    if (FAILEDx(store.init(dest))) {
      LOG_WARN("fail to init backup data store", K(ret), K(dest));
    } else if (OB_FAIL(store.read_ls_attr_info(backup_ls_attr))) {
      LOG_WARN("fail to read ls attr info", K(ret), K(dest));
    } else {
      for (int64_t i = 0; i < backup_ls_attr.ls_attr_array_.count(); OB_SUCC(ret) && i++) {
        if (OB_FAIL(ls_id_array.push_back(backup_ls_attr.ls_attr_array_.at(i).get_ls_id()))) {
          LOG_WARN("fail to push back ls id", K(ret), K(backup_ls_attr.ls_attr_array_.at(i).get_ls_id()));
        }
      } 
    }
  }
  
  if (OB_SUCC(ret)) {
    LOG_INFO("get macro_task_mgr_ init args", K(job_attr), K(set_task_attr), K(job));
  }
  return ret;
}

int ObSSRestoreScheduler::restore_wait_ls(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  TenantRestoreStatus tenant_restore_status;
  const uint64_t tenant_id = job_info.get_tenant_id();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_tenant_can_restore_(tenant_id))) {
    LOG_WARN("failed to check tenant can restore", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(check_all_ls_finish_restore_ls_(tenant_id, tenant_restore_status))) {
    LOG_WARN("fail to check all ls restore finish", K(ret));
  } else if (tenant_restore_status.is_finish()) {
    LOG_INFO("[RESTORE] restore wait all ls finish done", K(tenant_id), K(tenant_restore_status));
    int tmp_ret = OB_SUCCESS;
    int tenant_restore_result = OB_LS_RESTORE_FAILED;
    if (tenant_restore_status.is_success()) {
      tenant_restore_result = OB_SUCCESS;
    }
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, tenant_restore_result, job_info))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), KR(tenant_restore_result), K(job_info));
    }
  }
  return ret;
}

int ObSSRestoreScheduler::check_all_ls_finish_restore_ls_(
  const uint64_t tenant_id,
  TenantRestoreStatus &tenant_restore_status)
{
int ret = OB_SUCCESS;
bool is_finished = false;
bool is_success = false;
ObPhysicalRestoreTableOperator restore_op;
const ObLSRestoreStatus target_status(ObLSRestoreStatus::Status::SS_RESTORE_WAIT_LS);
if (OB_FAIL(restore_op.init(sql_proxy_, tenant_id, share::OBCG_STORAGE/*group_id*/))) {
  LOG_WARN("fail init", K(ret), K(tenant_id_));
} else if (OB_FAIL(restore_op.check_finish_restore_to_target_status(target_status, target_status, is_finished, is_success))) {
  LOG_WARN("fail to check finish restore finish", K(ret), K(tenant_id));
} else if (!is_finished) {
  tenant_restore_status = TenantRestoreStatus::IN_PROGRESS;
} else if (is_success) {
  tenant_restore_status = TenantRestoreStatus::SUCCESS;
} else {
  tenant_restore_status = TenantRestoreStatus::FAILED;
}

if (OB_FAIL(ret)) {
} else if (!tenant_restore_status.is_finish()) {
  if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
    LOG_INFO("check all ls restore ls not finish, just wait", KR(ret),
        K(tenant_id), K(tenant_restore_status));    
  }
}

return ret;
}

int ObSSRestoreScheduler::restore_wait_clog(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  TenantRestoreStatus tenant_restore_status;
  bool is_replay_finish = false;
  const uint64_t tenant_id = job_info.get_tenant_id();

  DEBUG_SYNC(BEFORE_SS_RESTORE_CLOG);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_tenant_can_restore_(tenant_id))) {
    LOG_WARN("failed to check tenant can restore", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(check_all_ls_restore_finish_(tenant_id, tenant_restore_status))) {
    LOG_WARN("failed to check all ls restore finish", KR(ret), K(job_info));
  } else if (tenant_restore_status.is_success()
      && OB_FAIL(check_tenant_replay_to_restore_scn_(job_info, is_replay_finish))) {
    LOG_WARN("failed to check tenant replay to restore scn", K(ret), K(tenant_id), K(job_info));
  } else if (tenant_restore_status.is_success() && !is_replay_finish) {
    // Migration can clear the per-LS restore status before log replay finishes.
  } else if (tenant_restore_status.is_finish()) {
    LOG_INFO("[RESTORE] restore wait all ls finish done", K(tenant_id), K(tenant_restore_status));
    int tmp_ret = OB_SUCCESS;
    int tenant_restore_result = OB_LS_RESTORE_FAILED;
    if (tenant_restore_status.is_success()) {
      tenant_restore_result = OB_SUCCESS;
    }
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, tenant_restore_result, job_info))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), KR(tenant_restore_result), K(job_info));
    }
  }
  return ret;
}

int ObSSRestoreScheduler::check_tenant_replay_to_restore_scn_(
    const share::ObPhysicalRestoreJob &job_info, bool &is_replay_finish)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  const uint64_t tenant_id = job_info.get_tenant_id();
  const SCN &restore_scn = job_info.get_restore_scn();
  is_replay_finish = false;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                 tenant_id, sql_proxy_, false/*for_update*/, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (tenant_info.get_recovery_until_scn() != restore_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected recovery until scn", KR(ret), K(tenant_info), K(restore_scn));
  } else {
    is_replay_finish = restore_scn <= tenant_info.get_readable_scn();
    if (is_replay_finish || REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      LOG_INFO("[RESTORE] tenant replay to restore scn", K(tenant_id), K(restore_scn),
          "readable_scn", tenant_info.get_readable_scn(), K(is_replay_finish));
    }
  }
  return ret;
}

int ObSSRestoreScheduler::restore_upgrade(const share::ObPhysicalRestoreJob &job) 
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_TMP_FAIL(try_update_job_status(*sql_proxy_, ret, job))) {
    LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job)); 
  }  
  return ret;
}

int ObSSRestoreScheduler::reload_task(ObBackupTaskSchedulerQueue &queue)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreJob job;
  ObAllTenantInfo all_tenant_info;  
  ObPhysicalRestoreTableOperator restore_op;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", K(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
              tenant_id_, sql_proxy_, false/*for_update*/, all_tenant_info))) {
    LOG_WARN("fail to load tenant info", K(ret), K_(tenant_id));
  } else if (!all_tenant_info.is_restore()) {
    LOG_INFO("tenant not in restore, no need to reload", K_(tenant_id));    
  } else if (OB_FAIL(restore_op.init(sql_proxy_, tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("fail to init restore op", K(ret), K_(tenant_id)); 
  } else if (OB_FAIL(restore_op.get_job_by_tenant_id(tenant_id_, job))) {
    LOG_WARN("fail to get job by tenant id", K(ret), K_(tenant_id));
  } else {
    ObBackupJobAttr job_attr;
    ObBackupSetTaskAttr set_task_attr;
    ObArray<ObLSID> ls_ids; //unused
    if (OB_FAIL(fill_macro_task_mgr_init_args_(job, job_attr, set_task_attr, ls_ids))) {
      LOG_WARN("fail to fill macro task mgr init args", K(ret), K(job));      
    } else {
      ObSSHAMacroTaskType task_type(ObSSHAMacroTaskType::RESTORE);
      if (OB_FAIL(backup::ObSSBackupRestoreUtil::reload_macro_block_tasks(*sql_proxy_,
                                                                          task_type,
                                                                          job_attr,
                                                                          set_task_attr,
                                                                          queue))) {
        LOG_WARN("fail to reload macro block tasks", K(ret), K(task_type), K(job_attr), K(set_task_attr));
      } else {
        LOG_INFO("succeed to reload macro block tasks", K_(tenant_id), K(job_attr), K(set_task_attr));
      }
    }
  }
  return ret;
}

int ObSSRestoreScheduler::stat_restore_progress_(const share::ObPhysicalRestoreJob &job, const bool is_finish)
{
  int ret = OB_SUCCESS;
  int64_t total_bytes = 0;
  int64_t finish_bytes = 0;
  ObSSHAMacroTaskType task_type;
  task_type.type_ = ObSSHAMacroTaskType::RESTORE;
  ObSSHAMacroTaskMgrState mgr_state;
  ObRestoreJobPersistKey job_key;
  job_key.tenant_id_ = tenant_id_;
  job_key.job_id_ = job.get_job_id();
  ObRestorePersistHelper helper;
  
  if (OB_FAIL(helper.init(tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore persist helper", K(ret), K_(tenant_id));
  } else if (OB_FAIL(ObSSHAMacroBlockTaskOperator::load_mgr_state(
      *sql_proxy_, tenant_id_, job.get_job_id(), task_type, mgr_state))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (is_finish) { // task succeeded
        if OB_FAIL(helper.get_total_bytes(*sql_proxy_, job_key, total_bytes)) {
          LOG_WARN("fail to get total bytes", K(ret), K(job_key));
        } else {
          finish_bytes = total_bytes;
        }
      }
    } else {
      LOG_WARN("fail to load mgr state for progress sync", K(ret));
    }
  } else {
    total_bytes = mgr_state.total_bytes_;
    finish_bytes = mgr_state.finish_bytes_;
  }
  
  if (OB_SUCC(ret) && total_bytes > 0) {
    if (OB_FAIL(helper.update_restore_progress_by_bytes(
        *sql_proxy_, job_key, total_bytes, finish_bytes))) {
      LOG_WARN("fail to update restore progress by bytes", K(ret), K(job_key),
                K(total_bytes), K(finish_bytes));
    } else {
      LOG_INFO("[RESTORE] stat restore progress", K(job_key), K(total_bytes), K(finish_bytes));
    }
  }
  return ret;
}

} //rootserver
} //oceanbase
