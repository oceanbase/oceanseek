
/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_SS_RESTORE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_SS_RESTORE_SCHEDULER_H_

#include "rootserver/restore/ob_restore_util.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_upgrade_utils.h"
#include "rootserver/restore/ob_restore_common_util.h"
#include "storage/high_availability/ob_ss_ha_macro_copy_task_generator.h"
#include "rootserver/backup/ob_ss_ha_macro_block_task_mgr.h"
#include "rootserver/restore/ob_restore_scheduler.h"
namespace oceanbase
{
namespace rootserver
{
  class ObRestoreService;
  class ObBackupDataService;
// Running in a single thread.
// schedule restore job, register to sys ls of meta tenant
class ObSSRestoreScheduler : public ObRestoreScheduler
{
public:
  static const int64_t MAX_RESTORE_TASK_CNT = 10000;
public:
  ObSSRestoreScheduler();
  virtual ~ObSSRestoreScheduler();
  int init(ObRestoreService &restore_service);
  int reload_task(ObBackupTaskSchedulerQueue &queue);
private:
  int process_restore_job(const share::ObPhysicalRestoreJob &job) override;
  int generate_restore_task(const share::ObPhysicalRestoreJob &job);
  int restore_data(const share::ObPhysicalRestoreJob &job);
  int restore_wait_ls(const share::ObPhysicalRestoreJob &job_info);
  int restore_wait_clog(const share::ObPhysicalRestoreJob &job_info);
  int check_tenant_replay_to_restore_scn_(
      const share::ObPhysicalRestoreJob &job_info, bool &is_replay_finish);
  int restore_upgrade(const share::ObPhysicalRestoreJob &job) override;
  PhysicalRestoreStatus get_next_status(int return_ret, PhysicalRestoreStatus current_status);
  int try_update_job_status(
      common::ObISQLClient &sql_client,
      int return_ret,
      const ObPhysicalRestoreJob &job,
      share::PhysicalRestoreMod mod=share::PHYSICAL_RESTORE_MOD_RS) override;   
  int init_macro_task_mgr_if_need_(const share::ObPhysicalRestoreJob &job);
  int fill_macro_task_mgr_init_args_(
      const share::ObPhysicalRestoreJob &job,
      ObBackupJobAttr &job_attr,
      ObBackupSetTaskAttr &set_task_attr,
      ObIArray<ObLSID> &ls_id_array);
  int fill_restore_statistics_(const share::ObPhysicalRestoreJob &job_info);
  int stat_restore_progress_(const share::ObPhysicalRestoreJob &job, const bool is_finish);
  int check_all_ls_finish_restore_ls_(
    const uint64_t tenant_id,
    TenantRestoreStatus &tenant_restore_status);
private:
  ObBackupTaskScheduler *task_scheduler_;
  ObSSHAMacroBlockRestoreTaskMgr macro_task_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObSSRestoreScheduler);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_SS_RESTORE_SCHEDULER_H_
