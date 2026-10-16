/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_HA_WRITER_H_
#define OCEANBASE_SHARED_STORAGE_HA_WRITER_H_

#include "storage/high_availability/ob_ss_storage_ha_reader.h"
namespace oceanbase
{
namespace storage
{
class ObICopySSMacroBlockWriter
{
public:
  enum Type {
    BACKUP_WRITER = 0,
    RESTORE_WRITER = 1,
    MAX_WRITER_TYPE
  };
  ObICopySSMacroBlockWriter() {}
  virtual ~ObICopySSMacroBlockWriter() {}
  virtual int process() = 0;
  virtual Type get_type() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObICopySSMacroBlockWriter);
};

class ObCopySSMacroBlockBackupWriter : public ObICopySSMacroBlockWriter
{
public:
  ObCopySSMacroBlockBackupWriter();
  virtual ~ObCopySSMacroBlockBackupWriter();
  int init(
      const uint64_t tenant_id,
      const ObDagId &dag_id,
      const share::ObBackupDest &backup_dest,
      const share::ObBackupSetDesc &backup_set_desc,
      const common::ObStorageIdMod &mod,
      ObCopySSMacroBlockBackupReader *reader);
  int process() override;
  virtual Type get_type() const override { return BACKUP_WRITER; }
private:
  int write_macro_block_(
      const blocksstable::MacroBlockId &block_id,
      const blocksstable::ObBufferReader &macro_data);
  int get_macro_block_backup_path_(
      const blocksstable::MacroBlockId &block_id,
      const share::ObBackupDest &backup_dest,
      const share::ObBackupSetDesc &desc,
      share::ObBackupPath &backup_path);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObDagId dag_id_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  common::ObStorageIdMod mod_;
  ObCopySSMacroBlockBackupReader *reader_;
  DISALLOW_COPY_AND_ASSIGN(ObCopySSMacroBlockBackupWriter);
};

class ObCopySSMacroBlockRestoreWriter : public ObICopySSMacroBlockWriter
{
public:
  ObCopySSMacroBlockRestoreWriter();
  virtual ~ObCopySSMacroBlockRestoreWriter();
  int init(const uint64_t tenant_id, 
          const share::ObLSID &ls_id, 
          const ObDagId &dag_id,
          ObCopySSMacroBlockRestoreReader *reader);
  int process() override;
  virtual Type get_type() const override { return RESTORE_WRITER; }
private:
  int set_macro_write_info_(
      const MacroBlockId &macro_block_id,
      blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectOpt &opt);
  int check_macro_block_(
      const blocksstable::ObBufferReader &data);
  int write_macro_block_(
      const ObStorageObjectOpt &opt,
      blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectHandle &write_handle,
      blocksstable::ObBufferReader &data);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObDagId dag_id_;
  ObCopySSMacroBlockRestoreReader *reader_;     
  blocksstable::ObSSTableMacroBlockChecker macro_checker_; 
  DISALLOW_COPY_AND_ASSIGN(ObCopySSMacroBlockRestoreWriter);  
};

}
}
#endif
