/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_SS_LS_RESTORE_STATE_H
#define OCEABASE_STORAGE_SS_LS_RESTORE_STATE_H

#include "storage/restore/ob_i_ls_restore_state.h"
#include "storage/high_availability/ob_storage_ha_struct.h"

namespace oceanbase
{
namespace storage
{
class ObSSLSRestoreStartState final : public ObILSRestoreState 
{
public:
  ObSSLSRestoreStartState();
  virtual ~ObSSLSRestoreStartState();
  virtual int do_restore() override;  
private:
  int check_ss_restore_data_finish_(bool &is_finish) const;
  bool is_ss_restore_data_finish_(const share::PhysicalRestoreStatus &status) const;
  int check_ls_meta_exist_(bool &is_exist);
  int do_with_no_ls_meta_();
  DISALLOW_COPY_AND_ASSIGN(ObSSLSRestoreStartState);
};

class ObSSLSRestoreLSState final : public ObILSRestoreState 
{
public:
  ObSSLSRestoreLSState();
  virtual ~ObSSLSRestoreLSState();
  virtual int do_restore() override;  
  virtual void set_retry_flag() { retry_flag_ = true; }
private:
  int do_restore_ls_();
  int fill_ss_ls_restore_arg_(ObMigrationOpArg &arg);
  int schedule_ls_restore_(const ObMigrationOpArg &arg, const ObTaskId &task_id);
  bool is_need_retry_();
private:  
  bool retry_flag_;
  ObLSMigrationCostStatic cost_static_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLSRestoreLSState);
};

class ObSSWaitLSRestoreState final : public ObILSRestoreState
{
public:
  ObSSWaitLSRestoreState();
  virtual ~ObSSWaitLSRestoreState();
  virtual int do_restore() override;  
private:
  int check_ss_restore_ls_finish_(bool &is_finish) const;
  bool is_ss_restore_ls_finish_(const share::PhysicalRestoreStatus &status) const;
  DISALLOW_COPY_AND_ASSIGN(ObSSWaitLSRestoreState);
};

class ObSSLSRestoreCLogState final : public ObILSRestoreState 
{
public:
ObSSLSRestoreCLogState();
  virtual ~ObSSLSRestoreCLogState();
  virtual int do_restore() override;  
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSLSRestoreCLogState);
}; 

} //oceanbase
} //storage
#endif