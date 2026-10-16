/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SHARED_STORAGE_HA_MACRO_DELETE_DAG_NET_H
#define OB_SHARED_STORAGE_HA_MACRO_DELETE_DAG_NET_H

#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "storage/high_availability/ob_storage_ha_dag.h"

namespace oceanbase
{
namespace storage
{
//===================ObSSHAMacroDeleteArg=================================
struct ObSSHAMacroDeleteArg 
{
public:
  ObSSHAMacroDeleteArg();
  virtual ~ObSSHAMacroDeleteArg();
  int assign(const ObSSHAMacroDeleteArg &other);
  bool is_valid() const;
  void reset();
  bool operator==(const ObSSHAMacroDeleteArg &other) const;    
  bool operator!=(const ObSSHAMacroDeleteArg &other) const {return !(*this == other); }
  TO_STRING_KV(K_(task_attr), K_(backup_set_dest), K_(backup_set_desc));
public:
  ObSSHAMacroBlockTaskAttr task_attr_;
  share::ObBackupDest backup_set_dest_;
  share::ObBackupSetDesc backup_set_desc_;
};
  
//============ObSSHAMacroDeleteDagNetInitParam============================
struct ObSSHAMacroDeleteDagNetInitParam : public ObIDagInitParam
{
public:
  ObSSHAMacroDeleteDagNetInitParam();
  virtual ~ObSSHAMacroDeleteDagNetInitParam();
  int assign(const ObSSHAMacroDeleteDagNetInitParam &param);
  bool operator==(const ObSSHAMacroDeleteDagNetInitParam &other) const;  
  bool operator!=(const ObSSHAMacroDeleteDagNetInitParam &other) const;
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(job_id), K_(arg), K_(task_id));
public:
  uint64_t tenant_id_;
  int64_t job_id_;
  ObSSHAMacroDeleteArg arg_;
  ObTaskId task_id_;
};

//============ObSSHAMacroDeleteDagNetCtx============================
struct ObSSHAMacroDeleteDagNetCtx : public ObIHADagNetCtx
{
public:
  ObSSHAMacroDeleteDagNetCtx();
  virtual ~ObSSHAMacroDeleteDagNetCtx();
  void reset();
  void reuse();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::SS_MACRO_DELETE; }
  virtual bool is_valid() const;
  INHERIT_TO_STRING_KV("ObIHADagNetCtx", ObIHADagNetCtx, K_(tenant_id), K_(job_id),
    K_(arg), K_(task_id), K_(start_time), K_(finish_time));

public:
  uint64_t tenant_id_;
  int64_t job_id_;
  ObSSHAMacroDeleteArg arg_;
  ObTaskId task_id_;
  int64_t start_time_;
  int64_t finish_time_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroDeleteDagNetCtx);
};

//============ObSSHAMacroDeleteDagNet============================
class ObSSHAMacroDeleteDagNet final : public ObIDagNet
{
public:
  ObSSHAMacroDeleteDagNet();
  virtual ~ObSSHAMacroDeleteDagNet();
  virtual int init_by_param(const ObIDagInitParam *param) override;
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  virtual int deal_with_cancel() override;

  ObSSHAMacroDeleteDagNetCtx *get_ctx() { return ctx_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(ctx));

private:
  int report_result_();
  int alloc_ctx_();  
  void free_ctx_();
  int start_running_for_macro_delete_();  
private:
  bool is_inited_;
  ObSSHAMacroDeleteDagNetCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroDeleteDagNet);
  
};

//============ObSSHAMacroDeleteDag============================
class ObSSHAMacroDeleteDag final : public ObStorageHADag
{
public:
  ObSSHAMacroDeleteDag();
  virtual ~ObSSHAMacroDeleteDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  ObSSHAMacroDeleteDagNetCtx *get_ctx() const { return static_cast<ObSSHAMacroDeleteDagNetCtx *>(ha_dag_net_ctx_); }
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroDeleteDag);
};

//============ObSSHAMacroDeleteTask============================
class ObSSHAMacroDeleteTask final : public ObITask
{
public:
  ObSSHAMacroDeleteTask();
  virtual ~ObSSHAMacroDeleteTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSHAMacroDeleteTask"), KP(this), KPC(ctx_));
  
private:
  int check_can_do_task_(bool &can);
  int handle_macro_block_delete_task_();
  int get_macro_block_ids_from_block_file_();
  int get_next_sub_batch(common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids);
  int deduplicate_before_batch_delete_(common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids);
  bool is_iter_end() const { return next_idx_ >= batch_.count(); }
  int read_macro_block_items_(
    const share::ObBackupPath &file_list_dir,
    const share::ObLSID &ls_id,
    const backup::ObBackupBlockFileAddr &block_addr,
    const common::ObStorageIdMod &storage_id_mod);
  int delete_backup_macro_blocks_(const common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids);
  int generate_file_paths_(
      const common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids,
      const share::ObBackupPath &backup_set_dir_path,
      common::ObIArray<common::ObString> &file_path_array,
      common::ObIAllocator &allocator);
  int batch_delete_files_with_fallback_(
      const common::ObIArray<common::ObString> &file_path_array,
      const share::ObBackupStorageInfo *storage_info);
  int single_delete_failed_files_(
      const common::ObIArray<common::ObString> &file_path_array,
      const common::ObIArray<int64_t> &failed_file_idx,
      const share::ObBackupStorageInfo *storage_info);
  bool is_inited_;
  ObSSHAMacroDeleteDagNetCtx *ctx_;
  ObSSHAMacroBlockBatch batch_;
  int64_t next_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroDeleteTask);
};

  
} // namespace storage
} // namespace oceanbase
#endif