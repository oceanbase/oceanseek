/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BACKUP_SS_BACKUP_TASK_H_
#define OCEANBASE_STORAGE_BACKUP_SS_BACKUP_TASK_H_

#include "storage/backup/ob_ss_backup_ctx.h"
#include "storage/backup/ob_ss_backup_utils.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"

namespace oceanbase
{
namespace backup
{

class ObLSBackupSSMetaTask : public share::ObITask
{
public:
  ObLSBackupSSMetaTask();
  virtual ~ObLSBackupSSMetaTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObLSBackupSSMetaTask"), KP(this), KPC_(ctx), K_(snapshot_scn));

private:
  int backup_ls_meta_and_tablet_metas_(const share::SCN &snapshot);
  int backup_ss_ls_meta_(const share::SCN &snapshot);
  int backup_ss_tablet_metas_(const share::SCN &snapshot);
  int get_tablet_id_from_meta_row_(const storage::ObSSRawMetaRow &meta_row,
      common::ObTabletID &tablet_id, share::SCN &reorgan_scn);
  int backup_single_ss_tablet_meta_(const storage::ObSSRawMetaRow &meta_row, const common::ObTabletID &tablet_id,
      ObSSExternTabletMetaWriter &writer);
  int backup_ss_tablet_meta_(
      const share::SCN &snapshot,
      ObSSExternTabletMetaWriter &writer,
      int64_t &ss_meta_count,
      int64_t &tablet_count,
      share::SCN &max_tablet_replayed_scn);
  int report_meta_stats_(const int64_t tablet_count);
  int backup_ls_meta_ctrl_(
      const share::SCN &snapshot,
      ObSSExternTabletMetaWriter &writer,
      int64_t &ss_meta_count);
  int backup_single_ss_meta_ctrl_(
      const share::SCN &snapshot,
      const storage::ObSSMetaCtrlKey &ctrl_key,
      ObSSExternTabletMetaWriter &writer);
  int get_tablet_max_replayed_scn_(
      const common::ObTabletID &tablet_id,
      const share::SCN &reorgan_scn,
      share::SCN &max_replayed_scn);
  int report_task_result_();
  int record_server_event_();

private:
  bool is_inited_;
  ObSSBackupCtx *ctx_;
  share::ObIDagNet *dag_net_;
  share::SCN snapshot_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupSSMetaTask);
};

class ObInitialLSBackupTask : public share::ObITask
{
public:
  ObInitialLSBackupTask();
  virtual ~ObInitialLSBackupTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialLSBackupTask"), KP(this), KPC_(ctx));
private:
  int generate_ls_backup_dags_();
  int record_server_event_();
private:
  bool is_inited_;
  ObSSBackupCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialLSBackupTask);
};

class ObStartLSBackupTask : public share::ObITask
{
public:
  ObStartLSBackupTask();
  virtual ~ObStartLSBackupTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartLSBackupTask"), KP(this), KPC(ctx_));
private:
  int generate_tablets_backup_dag_();
  int record_server_event_();
private:
  bool is_inited_;
  ObSSBackupCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObStartLSBackupTask);
};

class ObBackupSysTabletsTask : public share::ObITask
{
public:
  ObBackupSysTabletsTask();
  virtual ~ObBackupSysTabletsTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObBackupSysTabletsTask"), KP(this), K_(sys_tablet_id_array));
private:
  int build_sys_tablet_id_list_();
  int generate_sys_tablet_backup_dag_();
  int record_server_event_();

private:
  bool is_inited_;
  ObSSBackupCtx *ctx_;
  common::ObArray<common::ObTabletID> sys_tablet_id_array_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupSysTabletsTask);
};

class ObBackupDataTabletsTask : public share::ObITask
{
public:
  ObBackupDataTabletsTask();
  virtual ~ObBackupDataTabletsTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObDataTabletsBackupTask"), KP(this), KPC(ctx_));
private:
  int get_need_backup_tablet_list_();
  int get_all_tablet_list_(
      const share::ObLSID &ls_id,
      common::ObIArray<ObSSBackupTabletPair> &all_tablet_list);
  int get_all_checkpoint_tablet_list_(
      common::ObIArray<ObSSBackupTabletPair> &checkpoint_tablet_list);
  int recover_last_retry_ctx_(
      const common::ObIArray<ObSSBackupTabletPair> &all_tablet_list,
      const common::ObIArray<ObSSBackupTabletPair> &checkpoint_tablet_list);
  int build_tablet_group_info_();
  int generate_tablet_group_dag_();
  int record_server_event_();
private:
  static const int64_t MAX_TABLET_GROUP_SIZE = 1024;
private:
  bool is_inited_;
  ObSSBackupCtx *ctx_;
  share::ObIDag *finish_dag_;
  common::ObArray<ObSSBackupTabletPair> tablet_array_;
  ObBackupDataStore backup_data_store_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataTabletsTask);
};

class ObTabletGroupBackupTask : public share::ObITask
{
public:
  ObTabletGroupBackupTask();
  virtual ~ObTabletGroupBackupTask();
  int init(
      const common::ObIArray<ObSSBackupTabletPair> &tablet_id_array,
      share::ObIDag *finish_dag,
      ObBackupTabletGroupCtx *tablet_group_ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletGroupBackupTask"), KP(this), KPC(ctx_));
private:
  int generate_tablet_backup_dag_();
  int record_server_event_();
private:
  bool is_inited_;
  ObSSBackupCtx *ctx_;
  ObBackupTabletGroupCtx *tablet_group_ctx_;
  common::ObArray<ObSSBackupTabletPair> tablet_array_;
  share::ObIDag *finish_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupBackupTask);
};

class ObTabletBackupFinishTask;
class ObTabletBackupTask : public share::ObITask
{
public:
  ObTabletBackupTask();
  virtual ~ObTabletBackupTask();
  int init(
      ObSSBackupCtx &backup_ctx,
      ObSSBackupTabletCtx &backup_tablet_ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletBackupTask"), KP(this));
private:
  int check_tablet_replica_validity_(
      const common::ObTabletID &tablet_id);
  int check_need_skip_tablet_(
      const common::ObTabletID &tablet_id,
      bool &need_skip);
  int build_backup_table_key_infos_(
      const bool need_skip,
      ObTablet *tablet);
  int generate_backup_tasks_(
      ObTabletBackupFinishTask *tablet_finish_task);
  int generate_tablet_backup_finish_task_(
      ObTabletBackupFinishTask *&tablet_finish_task);
  int generate_sstables_backup_task_(
      ObTabletBackupFinishTask *tablet_finish_task,
      share::ObITask *&parent_task);
  int generate_sstable_backup_task_(
      const ObITable::TableKey &table_key,
      ObTabletBackupFinishTask *tablet_finish_task,
      ObITask *parent_task,
      ObITask *child_task);
  int record_server_event_();
private:
  bool is_inited_;
  ObSSBackupCtx *backup_ctx_;
  ObSSBackupTabletCtx *backup_tablet_ctx_;
  common::ObArray<storage::ObITable::TableKey> table_key_array_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletBackupTask);
};

class ObTabletBackupFinishTask : public share::ObITask
{
public:
  ObTabletBackupFinishTask();
  virtual ~ObTabletBackupFinishTask();
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      ObSSBackupCtx &backup_ctx);
  virtual int process() override;
  ObTablet *get_tablet() const;
  int record_macro_block_id(const blocksstable::MacroBlockId &macro_id);
  VIRTUAL_TO_STRING_KV(K("ObTabletBackupFinishTask"), KP(this), K_(ls_id), K_(tablet_id),
      K_(transfer_scn), KPC_(tablet));

private:
  int get_tablet_handle_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn);
  int get_tablet_other_macro_id_list_(
      const common::ObTabletID &tablet_id);
  int backup_macro_block_(
      const blocksstable::MacroBlockId &block_id);
  int backup_macro_blocks_();
  int build_tablet_meta_file_info_(
      common::ObIAllocator &allocator,
      ObSSBackupTabletMetaFileInfoDesc &desc);
  int write_tablet_meta_file_info_(
      const ObSSBackupTabletMetaFileInfoDesc &desc);
  int build_tablet_macro_block_file_info_(
      ObSSBackupTabletMacroBlockFileInfoDesc &desc);
  int write_tablet_macro_block_file_info_(
      const ObSSBackupTabletMacroBlockFileInfoDesc &desc);
  int mark_tablet_finished_();
  int record_server_event_();
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  common::ObArenaAllocator allocator_;
  ObTabletHandle tablet_handle_;
  ObTablet *tablet_;
  ObSSBackupCtx *backup_ctx_;
  common::hash::ObHashSet<blocksstable::MacroBlockId> macro_id_set_;
  share::SCN snapshot_scn_;
  common::ObArray<blocksstable::MacroBlockId> tablet_meta_macro_id_array_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletBackupFinishTask);
};

class ObSSTableBackupFinishTask;
class ObSSTableBackupTask : public share::ObITask
{
public:
  ObSSTableBackupTask();
  virtual ~ObSSTableBackupTask();
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const storage::ObITable::TableKey &table_key,
      ObSSBackupCtx *backup_ctx,
      ObTabletBackupFinishTask *tablet_finish_task,
      ObSSTableBackupFinishTask *sstable_finish_task);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
  VIRTUAL_TO_STRING_KV(K("ObSSTableBackupTask"), KP(this), K_(ls_id), K_(tablet_id), K_(table_key));
private:
  int build_backup_macro_block_info_(
      ObSSTableBackupFinishTask *finish_task);
  int backup_macro_block_(
      const blocksstable::MacroBlockId &block_id);
  int backup_macro_blocks_();
  int mark_macro_block_finished_(
      const blocksstable::MacroBlockId &block_id);
  int record_server_event_();
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  storage::ObITable::TableKey table_key_;
  common::ObArray<blocksstable::MacroBlockId> macro_id_array_;
  ObSSBackupCtx *backup_ctx_;
  ObTabletBackupFinishTask *tablet_finish_task_;
  ObSSTableBackupFinishTask *sstable_finish_task_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableBackupTask);
};

class ObSSTableBackupFinishTask : public share::ObITask
{
public:
  ObSSTableBackupFinishTask();
  virtual ~ObSSTableBackupFinishTask();
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const storage::ObITable::TableKey &table_key,
      ObTabletBackupFinishTask *tablet_finish_task,
      ObSSBackupCtx *backup_ctx);
  int get_next_batch_macro_block_id(common::ObIArray<MacroBlockId> &macro_id_array);
  int check_is_iter_end(bool &is_end);
  virtual int process() override;

  VIRTUAL_TO_STRING_KV(K("ObSSTableBackupFinishTask"), KP(this));
private:
  int record_server_event_();
private:
  bool is_inited_;
  common::SpinRWLock lock_;
  ObSSBackupCtx *backup_ctx_;
  blocksstable::ObSSTable *sstable_;
  ObTableHandleV2 table_handle_;
  ObSSTableMetaHandle sstable_meta_hdl_;
  ObBackupSSTableCtx sstable_ctx_;
  bool is_iter_end_;
  ObBackupSSTableAllMacroIdIterator macro_id_iterator_;
  ObTabletBackupFinishTask *tablet_finish_task_;
  int64_t macro_block_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableBackupFinishTask);
};

class ObFinishLSBackupTask : public share::ObITask
{
public:
  ObFinishLSBackupTask();
  virtual ~ObFinishLSBackupTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObFinishLSBackupTask"), KP(this));
private:
  int build_tablet_group_checkpoint_desc_(
      ObSSBackupTabletGroupCheckpointDesc &desc);
  int record_success_backup_tablets_(
      const int64_t retry_id,
      const ObSSBackupTabletGroupCheckpointDesc &desc);
  int report_task_result_();
  int record_server_event_();
private:
  bool is_inited_;
  ObSSBackupCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishLSBackupTask);
};

}
}

#endif
