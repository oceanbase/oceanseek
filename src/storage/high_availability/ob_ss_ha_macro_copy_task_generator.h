/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_HA_MACRO_COPY_TASK_PRODUCER_H_
#define OCEANBASE_SHARED_STORAGE_HA_MACRO_COPY_TASK_PRODUCER_H_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/backup/ob_ss_backup_utils.h"
#include "share/backup/ob_ss_ha_macro_block_table_operator.h"
#include "src/storage/high_availability/ob_ss_storage_ha_reader.h"

namespace oceanbase
{
namespace storage
{


struct ObSSHAMacroCopyTaskGeneratorInitParam final
{
public:
  ObSSHAMacroCopyTaskGeneratorInitParam()
  : tenant_id_(),
    parent_task_id_(),
    task_type_(),
    backup_set_dest_(),
    ls_id_array_(),
    snapshot_()
  {
  }
  explicit ObSSHAMacroCopyTaskGeneratorInitParam(const share::ObSSHAMacroTaskType &task_type)
  : tenant_id_(),
    parent_task_id_(),
    task_type_(task_type),
    backup_set_dest_(),
    ls_id_array_()
  {
  }
  ~ObSSHAMacroCopyTaskGeneratorInitParam() {}
  bool is_valid() const;
  int assign(const ObSSHAMacroCopyTaskGeneratorInitParam &other);
  //common fields
  uint64_t tenant_id_;
  int64_t parent_task_id_; // parent task id, for backup clean, it is the task id of the backup clean task
  share::ObSSHAMacroTaskType task_type_;
  share::ObBackupDest backup_set_dest_;
  common::ObSArray<share::ObLSID> ls_id_array_;
  share::SCN snapshot_;  // Snapshot SCN for backup
  TO_STRING_KV(K_(tenant_id), K_(parent_task_id), K_(task_type), K_(backup_set_dest), K_(ls_id_array), K_(snapshot));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroCopyTaskGeneratorInitParam);
};


/** ==================== ObSSHAMacroBlockBatchPersister ====================
 * Macro block ID batch persister,
 * responsible for collecting MACRO_BLOCK_BATCH_SIZE macro block IDs and their occupy_size
*  and inserting into internal tables.
 */
class ObSSHAMacroBlockBatchPersister final
{
public:
  ObSSHAMacroBlockBatchPersister();
  ~ObSSHAMacroBlockBatchPersister();

  int init(const uint64_t tenant_id, const int64_t job_id,
           const share::ObSSHAMacroTaskType &task_type,
           share::ObHATransEndHelper *trans_end_helper);
  int add_macro_block(const ObSSHAMacroBlockInfo &macro_info);
  int add_task_attr(const ObSSHAMacroBlockTaskAttr &task_attr);
  int start();
  int close();
  int64_t get_total_task_count() const { return total_task_count_; }
  int64_t get_total_macro_cnt() const { return total_macro_cnt_; }
  int64_t get_total_bytes() const { return total_bytes_; }

private:
  static const int64_t MACRO_BLOCK_BATCH_SIZE = 1024;
  int persist_current_batch_();
  int switch_ls_id_if_need_(const ObSSHAMacroBlockInfo &macro_info);
  int build_task_attr_(
    const ObSSHAMacroBlockBatch &batch,
    share::ObSSHAMacroBlockTaskAttr &task_attr);
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t job_id_;
  int64_t parent_task_id_;
  ObSSHAMacroBlockBatch current_batch_;
  share::ObLSID curr_ls_id_;
  int64_t curr_task_id_;
  int64_t total_task_count_;
  int64_t total_macro_cnt_;
  int64_t total_bytes_;
  share::ObSSHAMacroTaskType task_type_;
  share::ObHATransEndHelper *trans_end_helper_;

  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroBlockBatchPersister);
};

// ==================== ObSSHAMacroCopyTaskGenStrategy ====================
// Strategy pattern for generating macro copy tasks
class ObSSHAMacroCopyTaskGenStrategy
{
public:
  ObSSHAMacroCopyTaskGenStrategy() {}
  virtual ~ObSSHAMacroCopyTaskGenStrategy() = default;
  virtual int alloc_and_init_ls_iter(
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              ObIHASSLSMetaIter *&ls_iter) = 0;
  virtual int alloc_and_init_tablet_iter(
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              const ObLSID &ls_id,
              ObIHASSTabletMetaIter *&tablet_iter) = 0;
  virtual void free_ls_iter(ObIHASSLSMetaIter *&ls_iter) = 0;
  virtual void free_tablet_iter(ObIHASSTabletMetaIter *&tablet_iter) = 0;
  virtual int generate_tasks_by_ls(
                const ObLSID &ls_id,
                const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
                ObSSHAMacroBlockBatchPersister *persister,
                ObSSRawMetaRow &ls_meta_row) { return OB_SUCCESS; }  
  virtual int generate_tasks_by_tablet(
                const uint64_t tenant_id,
                const ObLSID &ls_id,
                ObArenaAllocator &allocator,
                ObSSHAMacroBlockBatchPersister *persister,
                ObSSRawMetaRow &ss_tablet_row) { return OB_SUCCESS; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroCopyTaskGenStrategy);
};

/** ==================== ObSSHAMacroCopyTaskGenerator ====================
 * Main task producer class
 */
class ObSSHAMacroCopyTaskGenerator
{
public:
  ObSSHAMacroCopyTaskGenerator();
  virtual ~ObSSHAMacroCopyTaskGenerator();
  int init(const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
    share::ObHATransEndHelper *trans_end_helper);

  int generate(int64_t &total_task_cnt, int64_t &total_macro_cnt, int64_t &total_bytes);
  TO_STRING_KV(K_(is_inited), K_(init_param));
private:
  int build_generation_strategy_(const share::ObSSHAMacroTaskType &task_type);
  void free_generation_strategy_();
  int scan_all_ls_();
  int scan_ls_for_backup_clean_();
  int scan_ls_with_iterator_();
  int scan_tablets_in_ls_(const ObLSID &ls_id);
  int get_ls_id_from_meta_row_(
      const ObSSRawMetaRow &ls_meta_row, 
      share::ObLSID &ls_id);
  int get_tablet_id_from_meta_row_(
      const ObSSRawMetaRow &tablet_meta_row, 
      common::ObTabletID &tablet_id);

private:
  bool is_inited_;
  ObSSHAMacroCopyTaskGeneratorInitParam init_param_;
  ObSSHAMacroCopyTaskGenStrategy *strategy_;
  ObSSHAMacroBlockBatchPersister persister_;

  DISALLOW_COPY_AND_ASSIGN(ObSSHAMacroCopyTaskGenerator);
};

class ObBackupMacroCopyTaskGenStrategy : public ObSSHAMacroCopyTaskGenStrategy
{
public:
  ObBackupMacroCopyTaskGenStrategy() {}
  ~ObBackupMacroCopyTaskGenStrategy() {}
private:
  virtual int alloc_and_init_ls_iter(
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              ObIHASSLSMetaIter *&ls_iter) override;
  virtual int alloc_and_init_tablet_iter(
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              const ObLSID &ls_id,
              ObIHASSTabletMetaIter *&tablet_iter) override;
  virtual void free_ls_iter(ObIHASSLSMetaIter *&ls_iter) override;
  virtual void free_tablet_iter(ObIHASSTabletMetaIter *&tablet_iter) override;
  virtual int generate_tasks_by_tablet(
              const uint64_t tenant_id,
              const ObLSID &ls_id,
              ObArenaAllocator &allocator,
              ObSSHAMacroBlockBatchPersister *persister,
              ObSSRawMetaRow &ss_tablet_row) override;
  // Backup tablet_iter rows are grouped by tablet and processed as a stream.
  // Reset this set on tablet boundaries to deduplicate macro ids across
  // versions, since persister does not deduplicate them.
  common::ObTabletID last_tablet_id_;
  common::hash::ObHashSet<blocksstable::MacroBlockId, common::hash::NoPthreadDefendMode> dedup_set_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMacroCopyTaskGenStrategy);
};
class ObRestoreMacroCopyTaskGenStrategy : public ObSSHAMacroCopyTaskGenStrategy
{
public:
  ObRestoreMacroCopyTaskGenStrategy() {}
  ~ObRestoreMacroCopyTaskGenStrategy() {}
  virtual int alloc_and_init_ls_iter(
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              ObIHASSLSMetaIter *&ls_iter) override;
  virtual int alloc_and_init_tablet_iter(
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              const ObLSID &ls_id,
              ObIHASSTabletMetaIter *&tablet_iter) override;
  virtual void free_ls_iter(ObIHASSLSMetaIter *&ls_iter) override;
  virtual void free_tablet_iter(ObIHASSTabletMetaIter *&tablet_iter) override;
  virtual int generate_tasks_by_ls(
              const ObLSID &ls_id,
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              ObSSHAMacroBlockBatchPersister *persister,
              ObSSRawMetaRow &ls_meta_row) override;
  virtual int generate_tasks_by_tablet(
              const uint64_t tenant_id,
              const ObLSID &ls_id,
              ObArenaAllocator &allocator,
              ObSSHAMacroBlockBatchPersister *persister,
              ObSSRawMetaRow &ss_tablet_row) override;
private:
  int insert_tablet_meta_into_ss_meta_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      ObSSRawMetaRow &meta_row);
  int insert_ss_meta_ctrl_row_(const uint64_t tenant_id, const share::ObLSID &ls_id, ObSSRawMetaRow &meta_row);
  int update_ls_meta_in_ss_meta_(const uint64_t tenant_id, const ObLSID &ls_id,
      ObSSRawMetaRow &ls_meta_row);
  DISALLOW_COPY_AND_ASSIGN(ObRestoreMacroCopyTaskGenStrategy);
};

class ObRestoreLSMetaFunctor final : public ObIMetaFunction
{
  OB_UNIS_VERSION(1);
public:
  ObRestoreLSMetaFunctor(const ObIMetaFunction::MetaFuncType func_type, const ObLSID &ls_id)
      : ObIMetaFunction(func_type), ls_id_(ls_id), ls_meta_row_() {}
  ~ObRestoreLSMetaFunctor() {}

  virtual int operator()(ObSSLSMeta &ss_ls_meta) override;
public:
  ObLSID ls_id_;
  ObSSRawMetaRow ls_meta_row_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRestoreLSMetaFunctor);
};

class ObBackupCleanMacroCopyTaskGenStrategy : public ObSSHAMacroCopyTaskGenStrategy
{
public:
  ObBackupCleanMacroCopyTaskGenStrategy() {}
  ~ObBackupCleanMacroCopyTaskGenStrategy() {}
  virtual int alloc_and_init_ls_iter(
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              ObIHASSLSMetaIter *&ls_iter) override;
  virtual int alloc_and_init_tablet_iter(
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              const ObLSID &ls_id,
              ObIHASSTabletMetaIter *&tablet_iter) override;
  virtual void free_ls_iter(ObIHASSLSMetaIter *&ls_iter) override;
  virtual void free_tablet_iter(ObIHASSTabletMetaIter *&tablet_iter) override;

  virtual int generate_tasks_by_ls(
              const ObLSID &ls_id,
              const ObSSHAMacroCopyTaskGeneratorInitParam &init_param,
              ObSSHAMacroBlockBatchPersister *persister,
              ObSSRawMetaRow &ls_meta_row) override;
  virtual int generate_tasks_by_tablet(
              const uint64_t tenant_id,
              const ObLSID &ls_id,
              ObArenaAllocator &allocator,
              ObSSHAMacroBlockBatchPersister *persister,
              ObSSRawMetaRow &ss_tablet_row) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanMacroCopyTaskGenStrategy);
};

} // end namespace storage
} // end namespace oceanbase
#endif // OCEANBASE_SHARED_STORAGE_HA_MACRO_COPY_TASK_PRODUCER_H_
