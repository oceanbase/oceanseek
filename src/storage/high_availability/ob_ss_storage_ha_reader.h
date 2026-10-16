/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 
#ifndef OCEANBASE_SHARED_STORAGE_HA_READER_H_
#define OCEANBASE_SHARED_STORAGE_HA_READER_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_reader.h"
#include "share/backup/ob_ss_ha_macro_block_struct.h"
#include "storage/backup/ob_ss_backup_utils.h"

namespace oceanbase
{
namespace storage
{
// ObCopySSTabletMeta
struct ObCopySSTabletMeta final
{
  OB_UNIS_VERSION(1);
public:
  ObCopySSTabletMeta();
  virtual ~ObCopySSTabletMeta() {}
  void reset();
  int assign(const ObCopySSTabletMeta &info);
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(meta_version), K_(update_log));

  common::ObTabletID tablet_id_;
  share::SCN meta_version_;
  ObSSUpdateTabletLog update_log_;
};


// ObICopySSTabletsReader
class ObICopySSTabletsReader
{
public:
  enum Type {
    META_SERVICE_SSTABLETS_READER = 0,
    LS_CKPT_SSTABLETS_READER = 1, //FARM COMPAT WHITELIST
    MAX_SSTABLETS_READER_TYPE
  };

  virtual ~ObICopySSTabletsReader() {}
  virtual int get_next_tablet(
      common::ObArenaAllocator &allocator, 
      ObTabletHandle &tablet_handle, 
      ObCopySSTabletMeta &sstablet_meta) = 0;
  virtual int get_next_tablet(
      common::ObArenaAllocator &allocator,
      char *&tablet_raw_buf,
      int64_t &tablet_raw_buf_len,
      ObCopySSTabletMeta &sstablet_meta) = 0;
  virtual int check_is_iter_end(bool &is_iter_end) const = 0;
  virtual int64_t get_iterated_tablet_cnt() const = 0;
  virtual Type get_type() const = 0;
  virtual int64_t get_iterated_tablet_cost_ts() const = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};


// ObCopySSTabletsMetaServiceReader
class ObCopySSTabletsMetaServiceReader final : public ObICopySSTabletsReader
{
public:
  ObCopySSTabletsMetaServiceReader();
  ~ObCopySSTabletsMetaServiceReader() {}
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const share::SCN &snapshot);

  virtual int get_next_tablet(
      common::ObArenaAllocator &allocator, 
      ObTabletHandle &tablet_handle, 
      ObCopySSTabletMeta &sstablet_meta) override;

  virtual int get_next_tablet(
      common::ObArenaAllocator &allocator,
      char *&tablet_raw_buf,
      int64_t &tablet_raw_buf_len,
      ObCopySSTabletMeta &sstablet_meta) override;
  virtual int check_is_iter_end(bool &is_iter_end) const override;
  virtual int64_t get_iterated_tablet_cnt() const override { return total_count_; }
  virtual Type get_type() const override { return META_SERVICE_SSTABLETS_READER; }
  virtual int64_t get_iterated_tablet_cost_ts() const override { return total_cost_; }

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(snapshot), K_(is_iter_end), K_(total_count), K_(total_cost));

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::SCN snapshot_;

  bool is_iter_end_;
  int64_t total_count_;
  ObSSMetaIterGuard<ObSSMetaTabletIter> iter_guard_;
  ObSSMetaTabletIter *iter_;
  int64_t total_cost_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTabletsMetaServiceReader);
};

// 从恢复入口已经固定的 ls_ckpt 读取 SYS 租户 SYS_LS 的 tablet meta。
class ObCopySSTabletsLsCkptReader final : public ObICopySSTabletsReader
{
public:
  ObCopySSTabletsLsCkptReader();
  ~ObCopySSTabletsLsCkptReader() {}
  int init(const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const blocksstable::MacroBlockId &ls_ckpt_macro_id);

  virtual int get_next_tablet(
      common::ObArenaAllocator &allocator,
      ObTabletHandle &tablet_handle,
      ObCopySSTabletMeta &sstablet_meta) override;
  // 当前 SYS replace 不走 fast migration，ls_ckpt reader 不提供 raw buffer。
  virtual int get_next_tablet(
      common::ObArenaAllocator &allocator,
      char *&tablet_raw_buf,
      int64_t &tablet_raw_buf_len,
      ObCopySSTabletMeta &sstablet_meta) override;
  virtual int check_is_iter_end(bool &is_iter_end) const override;
  virtual int64_t get_iterated_tablet_cnt() const override { return total_count_; }
  virtual Type get_type() const override { return LS_CKPT_SSTABLETS_READER; }
  virtual int64_t get_iterated_tablet_cost_ts() const override { return total_cost_; }

  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(ls_id), K_(cur_idx), K_(total_count), K_(total_cost));

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSSLsCkptReader ckpt_reader_;
  int64_t cur_idx_;
  int64_t total_count_;
  int64_t total_cost_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSTabletsLsCkptReader);
};

class ObICopySSMacroBlockReader
{
public:
  enum Type {
    MACRO_BLOCK_BACKUP_READER = 0,
    MACRO_BLOCK_RESTORE_READER = 1,
    MAX_READER_TYPE
  };
  ObICopySSMacroBlockReader() {}
  virtual ~ObICopySSMacroBlockReader() {}
  virtual int get_next_macro_block(
              common::ObIAllocator &allocator,
              blocksstable::ObBufferReader &macro_data, 
              blocksstable::MacroBlockId &macro_block_id) = 0;
  virtual Type get_type() const = 0;
  virtual int64_t get_data_size() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICopySSMacroBlockReader);
};

class ObCopySSMacroBlockRestoreReader : public ObICopySSMacroBlockReader
{
public:
  ObCopySSMacroBlockRestoreReader();
  ~ObCopySSMacroBlockRestoreReader() {}
  int init(const uint64_t tenant_id,
         const ObBackupDest &backup_set_dest,
         const ObSSHAMacroBlockBatch &macro_batch);
  virtual int get_next_macro_block(
              common::ObIAllocator &allocator,
              blocksstable::ObBufferReader &macro_data,
              blocksstable::MacroBlockId &macro_block_id) override;
  virtual Type get_type() const override { return MACRO_BLOCK_RESTORE_READER; }
  int64_t get_data_size() const override { return data_size_; }
private:
  bool is_iter_end() const { return curr_idx_ == macro_batch_.count(); }
  bool is_inited_;
  ObStorageIdMod mod_;
  share::ObBackupDest backup_set_dest_;
  ObSSHAMacroBlockBatch macro_batch_;
  int64_t curr_idx_;
  int64_t data_size_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSMacroBlockRestoreReader);
};

class ObCopySSMacroBlockBackupReader : public ObICopySSMacroBlockReader
{
public:
  ObCopySSMacroBlockBackupReader();
  ~ObCopySSMacroBlockBackupReader();
  int init(
      const share::ObBackupDest &backup_dest,
      const share::ObBackupSetDesc &backup_set_desc,
      const common::ObStorageIdMod &mod,
      const ObSSHAMacroBlockBatch &macro_batch);
  virtual int get_next_macro_block(
              common::ObIAllocator &allocator,
              blocksstable::ObBufferReader &macro_data, 
              blocksstable::MacroBlockId &macro_block_id) override;
  virtual Type get_type() const override { return MACRO_BLOCK_BACKUP_READER; }
  int64_t get_data_size() const override { return data_size_; }
private:
  int read_macro_block_(
      const blocksstable::MacroBlockId &block_id,
      common::ObIAllocator &allocator,
      blocksstable::ObStorageObjectHandle &object_handle);
  int check_macro_block_(
      const blocksstable::MacroBlockId &block_id,
      blocksstable::ObStorageObjectHandle &object_handle);
  bool is_iter_end() const { return curr_idx_ == macro_batch_.count(); }
private:
  bool is_inited_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  common::ObStorageIdMod mod_;
  ObSSHAMacroBlockBatch macro_batch_;
  int64_t curr_idx_;
  int64_t data_size_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSMacroBlockBackupReader);
};

// ==================== ObIHASSLSMetaIter ========================
class ObIHASSLSMetaIter
{
public:
  ObIHASSLSMetaIter() {}
  virtual ~ObIHASSLSMetaIter() {}
  virtual int get_next(ObIAllocator &allocator, ObSSRawMetaRow &ls_meta_row) = 0;
  virtual int64_t get_iterated_ls_cnt() const = 0;
  virtual share::ObSSHAMacroTaskType::Type get_task_type() const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIHASSLSMetaIter);
};

// ==================== ObIHASSTabletMetaIter ========================
class ObIHASSTabletMetaIter
{
public:
  ObIHASSTabletMetaIter() {}
  virtual ~ObIHASSTabletMetaIter() {}
  virtual int get_next(ObIAllocator &allocator, ObSSRawMetaRow &sstablet_meta_row) = 0;
  virtual int64_t get_iterated_tablet_cnt() const = 0;
  virtual share::ObSSHAMacroTaskType::Type get_task_type() const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
    
private:
  DISALLOW_COPY_AND_ASSIGN(ObIHASSTabletMetaIter);
};

// ==================== ObRestoreSSLSMetaIterator ====================
class ObRestoreSSLSMetaIterator final : public ObIHASSLSMetaIter
{
public:
  ObRestoreSSLSMetaIterator();
  ~ObRestoreSSLSMetaIterator() {}

  int init(const uint64_t tenant_id, const ObBackupDest &backup_set_dest);
  virtual int get_next(ObIAllocator &allocator, ObSSRawMetaRow &ls_meta_row) override;
  virtual int64_t get_iterated_ls_cnt() const override { return iterated_ls_cnt_; }
  virtual share::ObSSHAMacroTaskType::Type get_task_type() const override { return share::ObSSHAMacroTaskType::Type::RESTORE; }
  TO_STRING_KV(K_(is_inited), K_(iterated_ls_cnt), K_(tenant_id), 
               K_(dest_id), K_(backup_set_dest), K_(ls_id_array));

private:
  bool is_inited_;
  int64_t iterated_ls_cnt_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  ObBackupDest backup_set_dest_;
  ObArray<share::ObLSID> ls_id_array_;
};

// ==================== ObBackupSSLSMetaIterator ====================
class ObBackupSSLSMetaIterator final : public ObIHASSLSMetaIter
{
public:
  ObBackupSSLSMetaIterator();
  ~ObBackupSSLSMetaIterator() {}

  int init(const uint64_t tenant_id, 
           const common::ObSArray<share::ObLSID> &ls_id_array, 
           const share::SCN &snapshot);
  virtual int get_next(ObIAllocator &allocator, ObSSRawMetaRow &ls_meta_row) override;
  virtual int64_t get_iterated_ls_cnt() const override { return iterated_ls_cnt_; }
  virtual share::ObSSHAMacroTaskType::Type get_task_type() const override { return share::ObSSHAMacroTaskType::Type::BACKUP; }
  TO_STRING_KV( K_(is_inited), K_(iterated_ls_cnt), K_(tenant_id), 
                K_(backup_set_dest), K_(current_ls_id));

private:
  bool is_inited_;
  int64_t iterated_ls_cnt_;
  uint64_t tenant_id_;
  ObBackupDest backup_set_dest_;
  common::ObSArray<share::ObLSID> ls_id_array_;
  share::ObLSID current_ls_id_;
  share::SCN snapshot_;  // Snapshot SCN for reading metadata
};

// ==================== ObRestoreSSTabletMetatIterator ====================
class ObRestoreSSTabletMetatIterator final : public ObIHASSTabletMetaIter
{
public:
  ObRestoreSSTabletMetatIterator();
  ~ObRestoreSSTabletMetatIterator() {}
  int init(const share::ObLSID &ls_id, const uint64_t tenant_id, const ObBackupDest &backup_set_dest);
  virtual int get_next(ObIAllocator &allocator, ObSSRawMetaRow &tablet_meta_row) override;
  virtual int64_t get_iterated_tablet_cnt() const override { return iterated_tablet_cnt_; }
  virtual share::ObSSHAMacroTaskType::Type get_task_type() const override { return share::ObSSHAMacroTaskType::Type::RESTORE; }
  TO_STRING_KV(K_(is_inited), K_(iterated_tablet_cnt), K_(tenant_id), K_(ls_id));

private:
  bool is_inited_;
  int64_t iterated_tablet_cnt_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  backup::ObSSExternTabletMetaReader reader_;
};

// ==================== ObBackupSSTableMetatIterator ====================
class ObBackupSSTableMetatIterator final : public ObIHASSTabletMetaIter
{
public:
  ObBackupSSTableMetatIterator();
  ~ObBackupSSTableMetatIterator() {}
  int init(const share::ObLSID &ls_id, const uint64_t tenant_id, const share::SCN &snapshot);
  virtual int get_next(ObIAllocator &allocator, ObSSRawMetaRow &tablet_meta_row) override;
  virtual int64_t get_iterated_tablet_cnt() const override { return iterated_tablet_cnt_; }
  virtual share::ObSSHAMacroTaskType::Type get_task_type() const override { return share::ObSSHAMacroTaskType::Type::BACKUP; }
  TO_STRING_KV(K_(is_inited), K_(iterated_tablet_cnt), K_(tenant_id), K_(ls_id));

private:
  bool is_inited_;
  int64_t iterated_tablet_cnt_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  // TODO @donglou.zl check it
  // row_scn_/extra_info_ no longer exposed via V2 iterator
  ObSSMetaService *meta_svr_;
  ObSSMetaReadParamV2 param_;
  ObSSMetaRawRowIter *iter_;
  ObSSMetaIterGuard<ObSSMetaRawRowIter> iter_guard_;
  share::SCN snapshot_;  // Snapshot SCN for reading metadata
};

}
}

#endif
