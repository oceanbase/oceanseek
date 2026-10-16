/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_HA_DAG
#define OCEABASE_STORAGE_HA_DAG

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "ob_storage_restore_struct.h"

namespace oceanbase
{
namespace storage
{

struct ObStorageHAResultMgr final
{
public:
  ObStorageHAResultMgr();
  ~ObStorageHAResultMgr();
  int get_result(int32_t &result);
  int set_result(const int32_t result, const bool allow_retry,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  int check_allow_retry(bool &allow_retry);
  void reuse();
  void reset();
  void reset_result();
  int get_retry_count(int32_t &retry_count);
  int get_first_failed_task_id(share::ObTaskId &task_id);
  TO_STRING_KV(K_(result), K_(retry_count), K_(allow_retry), K_(failed_task_id_list));

private:
  static const int64_t MAX_RETRY_CNT = 3;
  common::SpinRWLock lock_;
  int32_t result_;
  int32_t retry_count_;
  bool allow_retry_;
  common::ObSEArray<share::ObTaskId, MAX_RETRY_CNT> failed_task_id_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageHAResultMgr);
};

struct ObIHADagNetCtx
{
public:
  enum DagNetCtxType {
    LS_COMPLETE_MIGRATION = 0,
    BACKFILL_TX = 1,
    TRANSFER_BACKFILL_TX = 2,
    SS_LS_MIGRATION = 3,
    SS_TRANSFER_BACKFILL_TX = 4,
    SS_MACRO_COPY = 5,
    SS_MACRO_DELETE = 6,
    SS_LS_RESTORE = 7,
    INC_UPLOAD = 8,
    MAX
  };

  ObIHADagNetCtx();
  virtual ~ObIHADagNetCtx();
  virtual int fill_comment(char *buf, const int64_t buf_len) const = 0;
  virtual DagNetCtxType get_dag_net_ctx_type() = 0;
  virtual bool is_valid() const = 0;
  int set_result(const int32_t result, const bool need_retry,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  virtual int check_allow_retry(bool &allow_retry);
  int get_result(int32_t &result);
  void reuse();
  void reset();
  void reset_result();
  int check_is_in_retry(bool &is_in_retry);
  int get_retry_count(int32_t &retry_count);
  int get_first_failed_task_id(share::ObTaskId &task_id);

  VIRTUAL_TO_STRING_KV(K("ObIHADagNetCtx"), K_(result_mgr));
private:
  ObStorageHAResultMgr result_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIHADagNetCtx);
};

class ObStorageHADag : public share::ObIDag
{
public:
  explicit ObStorageHADag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObStorageHADag();
  virtual int inner_reset_status_for_retry();
  virtual bool inner_check_can_retry();
  int check_is_in_retry(bool &is_in_retry);

  int set_result(const int32_t result, const bool allow_retry = true,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  virtual int report_result();
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override 
  { return consumer_group_id_; }  
  ObIHADagNetCtx *get_ha_dag_net_ctx() const { return ha_dag_net_ctx_; }
  virtual bool is_ha_dag() const override { return true; }
  bool is_failed() const { return result_mgr_.is_failed(); }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, KPC_(ha_dag_net_ctx), K_(result_mgr));
protected:
  ObIHADagNetCtx *ha_dag_net_ctx_;
  ObStorageHAResultMgr result_mgr_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHADag);
};

class ObStorageHADagUtils
{
public:
  static int deal_with_fo(
      const int err,
      share::ObIDag *dag,
      const bool allow_retry = true);
  static int get_ls(
      const share::ObLSID &ls_id,
      ObLSHandle &ls_handle);
  static int check_self_is_valid_member(
      const share::ObLSID &ls_id,
      bool &is_valid_member);
  static int check_self_is_valid_member_after_inc_config_version(
      const share::ObLSID &ls_id,
      const bool with_leader,
      bool &is_valid_member);
  static int inc_member_list_config_version(
      const share::ObLSID &ls_id,
      const bool with_leader);
  static int get_migration_src_info(
      const ObMigrationOpArg &arg,
      const uint64_t tenant_id,
      const share::SCN &local_clog_checkpoint_scn,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHASrcInfo &src_info);
#ifdef OB_BUILD_SHARED_STORAGE
  static int check_self_is_valid_member_with_log_service(
      const share::ObLSID &ls_id,
      bool &is_valid_member);
  static int inc_config_version_with_log_service(
      const share::ObLSID &ls_id);
#endif

private:
  static int inner_check_self_is_valid_member_(
      const share::ObLSID &ls_id,
      const common::ObMemberList &member_list,
      const common::GlobalLearnerList &learner_list,
      bool &is_valid_member);
};



class ObStorageHACancelDagNetUtils
{
public:
  static int cancel_task(const share::ObLSID &ls_id, const share::ObTaskId &task_id);
private:
  static int cancel_migration_task_(const share::ObTaskId &task_id, const ObLSHandle &ls_handle, bool &is_exist);
};
}
}
#endif
