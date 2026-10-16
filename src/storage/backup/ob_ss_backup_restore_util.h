/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef STORAGE_SS_BACKUP_RESTORE_UTIL_H_
#define STORAGE_SS_BACKUP_RESTORE_UTIL_H_

#include "lib/function/ob_function.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/incremental/share/ob_shared_meta_common.h"

namespace oceanbase
{
namespace share
{
  struct ObSSHAMacroTaskType;
  struct ObSSHAMacroBlockTaskAttr;
}
namespace rootserver
{
  class ObBackupTaskSchedulerQueue;
  class ObBackupScheduleTask;
}
namespace backup
{

typedef common::ObFunction<int(const storage::ObSSRawMetaRow&)> HandleSSTabletMetaFunc;

class ObSSBackupRestoreUtil
{
public:
  static int read_ss_ls_meta(const share::ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const int64_t dest_id, common::ObIAllocator &allocator,
    storage::ObBackupSSLSMetaInfo &ls_meta);
  static int read_ss_tablet_metas(const share::ObBackupDest &backup_set_dest,
    const common::ObStorageIdMod &mod, const share::ObLSID &ls_id,
    common::ObIAllocator &allocator, const HandleSSTabletMetaFunc &func);
  static int read_ss_macro_block_list(const share::ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
    common::ObIArray<blocksstable::MacroBlockId> &macro_id_list);
  static int read_ss_macro_block(const share::ObBackupDest &backup_set_dest,
    const blocksstable::MacroBlockId &macro_id, const common::ObStorageIdMod &mod, 
    common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer);
  static int reload_macro_block_tasks(
      common::ObISQLClient &sql_proxy,
      const share::ObSSHAMacroTaskType &task_type,
      const share::ObBackupJobAttr &job,
      const share::ObBackupSetTaskAttr &set_task_attr,
      rootserver::ObBackupTaskSchedulerQueue &queue);

private:
  static int read_ss_macro_block_(const ObString &backup_path,
    const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
    const int64_t buf_size, common::ObIAllocator &allocator,
    blocksstable::ObBufferReader &buffer);
  static int read_file_(const ObString &path, const share::ObBackupStorageInfo *storage_info,
    const ObStorageIdMod &mod, char *buf, const int64_t buf_size, int64_t &real_read_size);
  static int do_reload_macro_block_tasks_(
      const share::ObBackupJobAttr &job,
      const share::ObBackupSetTaskAttr &set_task_attr,
      const ObIArray<share::ObSSHAMacroBlockTaskAttr> &macro_tasks,
      rootserver::ObBackupTaskSchedulerQueue &queue);
};

}  // namespace backup
}  // namespace oceanbase

#endif
