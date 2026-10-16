/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_LS_MIGRATION_H_
#define OCEANBASE_SHARED_STORAGE_LS_MIGRATION_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_reader.h"
#include "storage/high_availability/ob_ss_physical_create_tablets_task.h"

namespace oceanbase
{
namespace storage
{
// ObSSMigrationCtx
struct ObSSMigrationCtx :  public ObIHADagNetCtx
{
public:
  ObSSMigrationCtx();
  virtual ~ObSSMigrationCtx();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::SS_LS_MIGRATION; }
  virtual bool is_valid() const;
  bool is_ls_restore() const { return arg_.type_ == ObMigrationOpType::RESTORE_LS_OP; }

  void reset();
  void reuse();
  INHERIT_TO_STRING_KV(
      "ObIHADagNetCtx", ObIHADagNetCtx,
      K_(tenant_id),
      K_(arg),
      K_(start_ts),
      K_(finish_ts),
      K_(task_id),
      KP_(cost_static));

public:
  uint64_t tenant_id_;
  ObMigrationOpArg arg_;
  int64_t start_ts_;
  int64_t finish_ts_;
  share::ObTaskId task_id_;
  ObLSMigrationCostStatic *cost_static_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMigrationCtx);
};


// ObSSMigrationDagNetInitParam
struct ObSSMigrationDagNetInitParam: public share::ObIDagInitParam
{
  ObSSMigrationDagNetInitParam();
  virtual ~ObSSMigrationDagNetInitParam() {}
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(arg), K_(task_id), KP_(cost_static));

  ObMigrationOpArg arg_;
  share::ObTaskId task_id_;
  ObLSMigrationCostStatic *cost_static_;
};


// ObSSMigrationDagNet
class ObSSMigrationDagNet: public share::ObIDagNet
{
public:
  ObSSMigrationDagNet();
  virtual ~ObSSMigrationDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  virtual int deal_with_cancel() override;

  ObSSMigrationCtx *get_ss_migration_ctx() const { return ctx_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(ctx));

private:
  int start_running_for_migration_();
  int alloc_migration_ctx_();
  void free_migration_ctx_();
  int generate_migration_init_dag_();
  int report_restore_result_(ObLS *ls);

private:
  bool is_inited_;
  ObSSMigrationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMigrationDagNet);
};


// ObSSMigrationDag
class ObSSMigrationDag : public ObStorageHADag
{
public:
  explicit ObSSMigrationDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObSSMigrationDag();
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  ObSSMigrationCtx *get_ss_migration_ctx() const { return static_cast<ObSSMigrationCtx *>(ha_dag_net_ctx_); }

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
  DISALLOW_COPY_AND_ASSIGN(ObSSMigrationDag);
};


// ObSSInitialMigrationDag
class ObSSInitialMigrationDag : public ObSSMigrationDag
{
public:
  ObSSInitialMigrationDag();
  virtual ~ObSSInitialMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSMigrationDag", ObSSMigrationDag, KP(this));

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSInitialMigrationDag);
};


// ObSSInitialMigrationTask
class ObSSInitialMigrationTask : public share::ObITask
{
public:
  ObSSInitialMigrationTask();
  virtual ~ObSSInitialMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSInitialMigrationTask"), KP(this), KPC_(ctx));

private:
  int generate_migration_dags_();
  int record_server_event_();

private:
  bool is_inited_;
  ObSSMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObSSInitialMigrationTask);
};


// ObSSStartMigrationDag
class ObSSStartMigrationTask;
class ObSSStartMigrationFinishTask;
class ObSSStartMigrationDag final : public ObSSMigrationDag
{
  friend class ObSSStartMigrationTask;
  friend class ObSSStartMigrationFinishTask;
public:
  ObSSStartMigrationDag();
  virtual ~ObSSStartMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual bool inner_check_can_retry() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSMigrationDag", ObSSMigrationDag, KP(this));

private:
  bool is_inited_;
  ObLSHandle ls_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObSSStartMigrationDag);
};


// ObSSStartMigrationTask
class ObSSStartMigrationTask : public share::ObITask
{
public:
  ObSSStartMigrationTask();
  virtual ~ObSSStartMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSStartMigrationTask"), KP(this), KPC(ctx_), K_(snapshot));

private:
  int choose_snapshot_();
  int build_ls_();
  int init_create_tablets_task_param_(ObPhysicalCreateTabletsTaskParam &param);
  int ls_online_();
  int generate_create_tablets_task_();
  int record_server_event_();
  int get_tx_data_table_filled_tx_scn_(share::SCN &filled_tx_scn);
  int join_learner_list_();
  int try_update_warmup_src_info_();
  int check_can_do_migrate_();
  int get_tenant_restore_status_(
      bool &is_in_restore, 
      share::PhysicalRestoreStatus &status);
  int check_restore_status_(const share::PhysicalRestoreStatus &status);
  int report_ls_meta_table_();
  int wait_meta_ls_online_();
  int load_ls_ckpt_if_needed_();
  // meta service 尚未拉起时，从入口固定的 ls_ckpt 读取 LS_TX_DATA tablet。
  int get_tx_data_tablet_from_ls_ckpt_(common::ObArenaAllocator &allocator,
                                       ObTabletHandle &tablet_handle);

private:
  bool is_inited_;
  ObLS *ls_;
  ObSSMigrationCtx *ctx_;
  ObSSMetaService *meta_srv_;
  ObCopySSTabletsMetaServiceReader *sstablets_reader_;
  share::SCN snapshot_;
  share::SCN ss_checkpoint_scn_;
  share::SCN filled_tx_scn_;
  palf::LSN ss_checkpoint_lsn_;
  ObSSLsCkptReader ls_ckpt_reader_;
  bool ls_ckpt_loaded_;

  DISALLOW_COPY_AND_ASSIGN(ObSSStartMigrationTask);
};


// ObSSStartMigrationFinishTask
class ObSSStartMigrationFinishTask : public share::ObITask
{
public:
  ObSSStartMigrationFinishTask();
  virtual ~ObSSStartMigrationFinishTask();
  int init(const bool should_write_ckp_before_online);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSStartMigrationFinishTask"), 
                       KP(this), 
                       K_(should_write_ckp_before_online),
                       KPC_(ls), 
                       KPC_(ctx));

private:
  int ls_online_();
  int write_checkpoint_();

private:
  bool is_inited_;
  bool should_write_ckp_before_online_;
  ObLS *ls_;
  ObSSMigrationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSStartMigrationFinishTask);
};


// ObSSMigrationFinishDag
class ObSSMigrationFinishDag : public ObSSMigrationDag
{
public:
  ObSSMigrationFinishDag();
  virtual ~ObSSMigrationFinishDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSMigrationDag", ObSSMigrationDag, KP(this));

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMigrationFinishDag);
};


// ObSSMigrationFinishTask
class ObSSMigrationFinishTask : public share::ObITask
{
public:
  ObSSMigrationFinishTask();
  virtual ~ObSSMigrationFinishTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSMigrationFinishTask"), KP(this), KPC(ctx_));

private:
  int generate_migration_init_dag_();
private:
  bool is_inited_;
  ObSSMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMigrationFinishTask);
};
}
}

#endif
