/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BACKUP_SS_BACKUP_CTX_H_
#define OCEANBASE_STORAGE_BACKUP_SS_BACKUP_CTX_H_

#include "lib/oblog/ob_log_module.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "common/storage/ob_device_common.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/backup/ob_ss_backup_tablet_pair.h"

namespace oceanbase
{
namespace backup
{

struct ObSSBackupDagNetInitParam : public share::ObIDagInitParam
{
  ObSSBackupDagNetInitParam();
  virtual ~ObSSBackupDagNetInitParam();
  virtual bool is_valid() const override;
  int assign(const ObSSBackupDagNetInitParam &param);
  bool operator==(const ObSSBackupDagNetInitParam &other) const;
  VIRTUAL_TO_STRING_KV(K_(job_desc), K_(task_id), K_(backup_dest), K_(tenant_id), K_(dest_id),
      K_(backup_set_desc), K_(ls_id), K_(retry_id), K_(snapshot_scn), K_(is_backup_data));
  backup::ObBackupJobDesc job_desc_;
  share::ObTaskId task_id_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t retry_id_;
  ObBackupReportCtx report_ctx_;
  share::SCN snapshot_scn_;
  bool is_backup_data_;
};

struct ObSSBackupTabletCtx final
{
  ObSSBackupTabletCtx();
  ~ObSSBackupTabletCtx();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tablet));
  ObSSBackupTabletPair tablet_;
};

class ObBackupTabletGroupCtx final
{
public:
  ObBackupTabletGroupCtx();
  ~ObBackupTabletGroupCtx();
  int init(
      const common::ObIArray<ObSSBackupTabletPair> &tablet_array);
  int get_next_tablet(ObSSBackupTabletPair &tablet);
  int get_all_tablets(common::ObIArray<ObSSBackupTabletPair> &tablet_id);
  void reuse();
public:
  TO_STRING_KV(K_(tablet_array), K_(idx));
protected:
  bool is_inited_;
  common::SpinRWLock lock_;
  int64_t idx_;
  common::ObArray<ObSSBackupTabletPair> tablet_array_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletGroupCtx);
};

class ObBackupTabletGroupMgr final
{
public:
  ObBackupTabletGroupMgr();
  ~ObBackupTabletGroupMgr();
  int init();
  int get_next_tablet_group_ctx(ObBackupTabletGroupCtx *&tablet_group_ctx);
  int build_tablet_group_ctx(
      const common::ObIArray<ObSSBackupTabletPair> &tablet_array);
  int alloc_and_new_tablet_group_ctx(ObBackupTabletGroupCtx *&tablet_group_ctx);
  void reuse();
  int64_t get_tablet_group_ctx_count() const;
  int get_tablet_group_ctx(const int64_t idx, ObBackupTabletGroupCtx *&tablet_group_ctx);

  TO_STRING_KV(K_(tablet_group_ctx_array), K_(idx));
private:
  bool is_inited_;
  common::SpinRWLock lock_;
  ObArenaAllocator allocator_;
  ObArray<ObBackupTabletGroupCtx *> tablet_group_ctx_array_;
  int64_t idx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletGroupMgr);
};

struct ObBackupSSTableCtx final
{
  ObBackupSSTableCtx() : tenant_id_(), ls_id_(), tablet_id_(), table_key_() {}
  ~ObBackupSSTableCtx() = default;

  TO_STRING_KV(K_(tenant_id), 
               K_(ls_id), 
               K_(tablet_id),
               K_(table_key));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  storage::ObITable::TableKey table_key_;
};

struct ObSSBackupCtx final
{
public:
  ObSSBackupCtx()
    : job_desc_(),
      tenant_id_(OB_INVALID_TENANT_ID),
      start_ts_(0),
      finish_ts_(0),
      mod_(),
      dest_id_(0),
      backup_dest_(),
      backup_set_desc_(),
      task_id_(),
      ls_id_(),
      retry_id_(0),
      tablet_id_array_(),
      tablet_group_mgr_(),
      result_mgr_(),
      report_ctx_(),
      bandwidth_throttle_(nullptr),
      snapshot_scn_(),
      is_inited_(false),
      lock_(common::ObLatchIds::BACKUP_LOCK),
      has_set_need_backup_(false),
      need_backup_tablet_count_(0),
      finish_tablet_count_(0),
      finished_tablet_set_()
  {}
  backup::ObBackupJobDesc job_desc_;
  uint64_t tenant_id_;
  int64_t start_ts_;
  int64_t finish_ts_;
  common::ObStorageIdMod mod_;
  int64_t dest_id_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObTaskId task_id_;
  share::ObLSID ls_id_;
  int64_t retry_id_;
  common::ObArray<ObTabletID> tablet_id_array_;
  ObBackupTabletGroupMgr tablet_group_mgr_;
  storage::ObStorageHAResultMgr result_mgr_;
  ObBackupReportCtx report_ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  share::SCN snapshot_scn_;

  int init();
  bool is_valid() const;
  void set_need_backup_tablet_count(const int64_t tablet_count);
  int mark_tablet_finished(const ObSSBackupTabletPair &tablet);
  int get_all_finished_tablets(common::ObIArray<ObSSBackupTabletPair> &tablets);

  int set_result(const int32_t result, const bool need_retry,
      const enum share::ObDagType::ObDagTypeEnum type = share::ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  int get_result(int32_t &result);
  bool is_finished() const; 

  TO_STRING_KV(K_(tenant_id), K_(backup_set_desc), K_(task_id), K_(ls_id), K_(retry_id), K_(start_ts), K_(finish_ts));

private:
  bool is_inited_;
  common::SpinRWLock lock_;
  bool has_set_need_backup_;
  int64_t need_backup_tablet_count_;
  int64_t finish_tablet_count_;
  common::hash::ObHashSet<ObSSBackupTabletPair> finished_tablet_set_;
};

}
}

#endif
