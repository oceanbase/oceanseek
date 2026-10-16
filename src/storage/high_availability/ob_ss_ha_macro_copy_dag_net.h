/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SHARED_STORAGE_HA_MACRO_COPY_DAG_NET_H
#define OB_SHARED_STORAGE_HA_MACRO_COPY_DAG_NET_H

#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "storage/high_availability/ob_ss_storage_ha_reader.h"
#include "storage/high_availability/ob_ss_storage_ha_writer.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace storage
{
//===================ObSSHAMacroCopyArg=================================
struct ObSSHAMacroCopyArg final
{
public:
  ObSSHAMacroCopyArg();
  ~ObSSHAMacroCopyArg();
  int assign(const ObSSHAMacroCopyArg &other);
  bool is_valid() const;
  void reset();
  bool operator==(const ObSSHAMacroCopyArg &other) const;    
  bool operator!=(const ObSSHAMacroCopyArg &other) const {return !(*this == other); }
  TO_STRING_KV(K_(job_id), K_(task_attr), K_(backup_set_dest), K_(backup_set_desc));
public:
  int64_t job_id_;
  ObSSHAMacroBlockTaskAttr task_attr_;
  share::ObBackupDest backup_set_dest_;
  ObBackupSetDesc backup_set_desc_;
};
  
//============ObSSHAMacroCopyDagNetInitParam============================
struct ObSSHAMacroCopyDagNetInitParam : public ObIDagInitParam
{
public:
  ObSSHAMacroCopyDagNetInitParam();
  virtual ~ObSSHAMacroCopyDagNetInitParam();
  int assign(const ObSSHAMacroCopyDagNetInitParam &param);
  bool operator==(const ObSSHAMacroCopyDagNetInitParam &other) const;  
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(arg), K_(task_id));
public:
  uint64_t tenant_id_;
  ObSSHAMacroCopyArg arg_;
  ObTaskId task_id_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  
};

//============ObSSHAMacroCopyDagNetCtx============================
struct ObSSHAMacroCopyDagNetCtx : public ObIHADagNetCtx
{
public:
  ObSSHAMacroCopyDagNetCtx();
  virtual ~ObSSHAMacroCopyDagNetCtx();
  int init(const ObSSHAMacroCopyArg &arg, uint64_t tenant_id, const ObTaskId &task_id);
  void reset();
  void reuse();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() override { return ObIHADagNetCtx::SS_MACRO_COPY; }
  virtual bool is_valid() const override;
  int mark_finished_macro_ids(const ObSSHAMacroBlockBatch &macro_ids);
  int get_unfinished_macro_ids(ObSSHAMacroBlockBatch &macro_ids);
  int64_t get_unfinished_macro_id_count();
  INHERIT_TO_STRING_KV("ObIHADagNetCtx", ObIHADagNetCtx, K_(tenant_id),
    K_(arg), K_(task_id), K_(start_time), K_(finish_time));

public:
  uint64_t tenant_id_;
  ObSSHAMacroCopyArg arg_;
  ObTaskId task_id_;
  int64_t start_time_;
  int64_t finish_time_;
private:
  int mark_initial_unfinished_macro_ids_();
  hash::ObHashSet<ObSSHAMacroBlockInfo> unfinished_macro_ids_; //unfinished macro ids will be retried inside of the dag_net
  SpinRWLock lock_; // to protect unfinished_macro_ids_
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroCopyDagNetCtx);
};

//============ObSSHAMacroCopyDagNet============================
class ObSSHAMacroCopyDagNet : public ObIDagNet
{
public:
  ObSSHAMacroCopyDagNet();
  virtual ~ObSSHAMacroCopyDagNet();
  virtual int init_by_param(const ObIDagInitParam *param) override;
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  virtual int deal_with_cancel() override;

  ObSSHAMacroCopyDagNetCtx *get_ctx() { return ctx_; }
  common::ObInOutBandwidthThrottle *get_bandwidth_throttle() { return bandwidth_throttle_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(ctx));

private:
  int report_result_();
  int alloc_ctx_();  
  void free_ctx_();
  int start_running_for_macro_copy_();  
private:
  bool is_inited_;
  ObSSHAMacroCopyDagNetCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;  
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroCopyDagNet);
  
};

//============ObSSHAMacroCopyDag============================
class ObSSHAMacroCopyDag : public ObStorageHADag
{
public:
  explicit ObSSHAMacroCopyDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObSSHAMacroCopyDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  ObSSHAMacroCopyDagNetCtx *get_ctx() const { return static_cast<ObSSHAMacroCopyDagNetCtx *>(ha_dag_net_ctx_); }
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
protected:
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroCopyDag);
};

//============ObSSHAInitialMacroCopyDag============================
class ObSSHAInitialMacroCopyDag : public ObSSHAMacroCopyDag
{
public:
  ObSSHAInitialMacroCopyDag();
  virtual ~ObSSHAInitialMacroCopyDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSHAMacroCopyDag", ObSSHAMacroCopyDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAInitialMacroCopyDag);
};

//============ObSSHAInitialMacroCopyTask============================
class ObSSHAInitialMacroCopyTask : public ObITask
{
public:
  ObSSHAInitialMacroCopyTask();
  virtual ~ObSSHAInitialMacroCopyTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSHAInitialMacroCopyDag"), KP(this), KPC(ctx_));
private:
  int generate_copy_dags_();
  int record_server_event_();
  bool is_inited_;
  ObSSHAMacroCopyDagNetCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAInitialMacroCopyTask);
  
};

//============ObSSHACopyMacroRangeInfoMgr============================
class ObSSHACopyMacroRangeInfoMgr final
{
public:
  ObSSHACopyMacroRangeInfoMgr();
  ~ObSSHACopyMacroRangeInfoMgr();
  int init(const ObSSHAMacroBlockBatch &whole_batch);
  int reuse(const ObSSHAMacroBlockBatch &unfinished_batch);
  int get_next_sub_batch(ObSSHAMacroBlockBatch &sub_batch);
  bool is_iter_end() const
  {
    common::ObSpinLockGuard guard(lock_);
    return next_idx_ >= batch_.count();
  }
private:
  static const int64_t MAX_COPY_MACRO_BLOCK_CNT_PER_TASK = 8;
  bool is_inited_;
  int64_t next_idx_;  
  ObSSHAMacroBlockBatch batch_;
  mutable common::ObSpinLock lock_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHACopyMacroRangeInfoMgr);
};

//============ObSSHAStartMacroCopyDag============================
class ObSSHAStartMacroCopyDag : public ObSSHAMacroCopyDag
{
public:
  ObSSHAStartMacroCopyDag();
  virtual ~ObSSHAStartMacroCopyDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int inner_reset_status_for_retry() override;
  int init(share::ObIDagNet *dag_net);
  int get_next_sub_batch(ObSSHAMacroBlockBatch &sub_batch);
  bool is_iter_end() const { return mgr_.is_iter_end(); }
  INHERIT_TO_STRING_KV("ObSSHAMacroCopyDag", ObSSHAMacroCopyDag, KP(this));
protected:
  bool is_inited_;
  ObSSHACopyMacroRangeInfoMgr mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAStartMacroCopyDag);
};

//============ObSSHAStartMacroCopyTask============================
class ObSSHAStartMacroCopyTask : public ObITask
{
public:
  ObSSHAStartMacroCopyTask();
  virtual ~ObSSHAStartMacroCopyTask();
  int init();
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
  VIRTUAL_TO_STRING_KV(K("ObSSHAStartMacroCopyDag"), KP(this), KPC(ctx_));
  
private:
  int alloc_and_init_reader_(ObICopySSMacroBlockReader *&reader);
  int alloc_and_init_restore_reader_(ObICopySSMacroBlockReader *&reader);
  int alloc_and_init_backup_reader_(ObICopySSMacroBlockReader *&reader);
  int alloc_and_init_writer_(ObICopySSMacroBlockReader *reader, ObICopySSMacroBlockWriter *&writer);
  int alloc_and_init_restore_writer_(ObICopySSMacroBlockReader *reader, ObICopySSMacroBlockWriter *&writer);
  int alloc_and_init_backup_writer_(ObICopySSMacroBlockReader *reader, ObICopySSMacroBlockWriter *&writer);
  bool is_inited_;
  ObSSHAMacroCopyDagNetCtx *ctx_;
  ObSSHAMacroBlockBatch batch_;  
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAStartMacroCopyTask);
};

//============ObSSHAFinishMacroCopyDag============================
class ObSSHAFinishMacroCopyDag : public ObSSHAMacroCopyDag
{
public:
  ObSSHAFinishMacroCopyDag();
  virtual ~ObSSHAFinishMacroCopyDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSHAMacroCopyDag", ObSSHAMacroCopyDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAFinishMacroCopyDag);
};

//============ObSSHAFinishMacroCopyTask============================
class ObSSHAFinishMacroCopyTask : public ObITask
{
public:
  ObSSHAFinishMacroCopyTask();
  virtual ~ObSSHAFinishMacroCopyTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSHAFinishMacroCopyDag"), KP(this), KPC(ctx_));
private:
  int generate_copy_init_dag_();
  int record_server_event_();
  bool is_inited_;
  ObSSHAMacroCopyDagNetCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAFinishMacroCopyTask);
};


  
} // namespace storage
} // namespace oceanbase
#endif
