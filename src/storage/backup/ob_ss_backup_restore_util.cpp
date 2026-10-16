/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/backup/ob_ss_backup_restore_util.h"
#include "storage/backup/ob_ss_backup_utils.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/backup/ob_ss_ha_macro_block_table_operator.h"
#include "rootserver/backup/ob_backup_task_scheduler.h"
#include "rootserver/backup/ob_backup_schedule_task.h"

namespace oceanbase
{
namespace backup
{

int ObSSBackupRestoreUtil::read_ss_ls_meta(const share::ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const int64_t dest_id,
    common::ObIAllocator &allocator, ObBackupSSLSMetaInfo &ls_meta)
{
  int ret = OB_SUCCESS;
  ObSSExternLSMetaMgr mgr;
  int64_t turn_id = 1;
  int64_t retry_id = 0;
  share::ObBackupPath backup_path;

  // get max turn_id and retry_id by listing meta_info directories
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_ls_backup_dir_path(backup_set_dest, ls_id, backup_path))) {
    LOG_WARN("failed to get ls backup dir path", K(ret), K(backup_set_dest), K(ls_id));
  } else {
    ObSSMetaInfoDirFilter filter;
    ObBackupIoAdapter util;
    if (OB_FAIL(util.list_directories(backup_path.get_obstr(), backup_set_dest.get_storage_info(), filter))) {
      LOG_WARN("failed to list directories", K(ret), K(backup_path), K(backup_set_dest));
    } else if (filter.get_turn_id() <= 0 || filter.get_retry_id() < 0) {
      ret = OB_OBJECT_NOT_EXIST;
      LOG_WARN("invalid turn id and retry id, may not have meta info dir", K(ret), K(filter), K(backup_path));
    } else {
      turn_id = filter.get_turn_id();
      retry_id = filter.get_retry_id();
      LOG_INFO("get max turn_id and retry_id", K(turn_id), K(retry_id), K(backup_path));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mgr.init(backup_set_dest, ls_id, turn_id, retry_id, dest_id))) {
    LOG_WARN("failed to init ss extern ls meta mgr", K(ret), K(turn_id), K(retry_id));
  } else if (OB_FAIL(mgr.read_ls_meta_info(allocator, ls_meta))) {
    LOG_WARN("failed to read ls meta info", K(ret));
  } else {
    LOG_INFO("read ss ls meta", K(ls_id), K(ls_meta));
  }
  return ret;
}

int ObSSBackupRestoreUtil::read_ss_tablet_metas(const share::ObBackupDest &backup_set_dest,
    const common::ObStorageIdMod &mod, const share::ObLSID &ls_id,
    common::ObIAllocator &allocator, const HandleSSTabletMetaFunc &func)
{
  int ret = OB_SUCCESS;
  ObSSExternTabletMetaReader reader;
  if (OB_FAIL(reader.init(backup_set_dest, mod, ls_id))) {
    LOG_WARN("failed to init reader", K(ret), K(backup_set_dest), K(mod), K(ls_id));
  } else {
    storage::ObSSRawMetaRow meta_row;
    while (OB_SUCC(ret)) {
      meta_row.reset();
      allocator.reset();
      if (OB_FAIL(reader.get_next(allocator, meta_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next", K(ret));
        }
      } else if (OB_FAIL(func(meta_row))) {
        LOG_WARN("failed to do func", K(ret));
      }
    }
  }
  return ret;
}

int ObSSBackupRestoreUtil::read_ss_macro_block_list(const share::ObBackupDest &backup_set_dest,
    const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
    common::ObIArray<blocksstable::MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  macro_id_list.reset();
  ObBackupDataStore store;
  ObSSBackupTabletMacroBlockFileInfoDesc desc;
  if (OB_FAIL(store.init(backup_set_dest))) {
    LOG_WARN("failed to init backup data store", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(store.read_tablet_macro_block_file_info(ls_id, tablet_id, desc))) {
    LOG_WARN("failed to raed tablet macro block file info", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(macro_id_list.assign(desc.macro_id_list_))) {
    LOG_WARN("failed to assign macro id list", K(ret));
  } else {
    LOG_INFO("read ss macro block list", K(ls_id), K(tablet_id), K(macro_id_list));
  }
  return ret;
}

int ObSSBackupRestoreUtil::read_ss_macro_block(const share::ObBackupDest &backup_set_dest,
    const blocksstable::MacroBlockId &macro_id, const common::ObStorageIdMod &mod, 
    common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  const int64_t length = 1024;
  char path[length] = {0};
  int64_t buf_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
  ObBackupIoAdapter util;
  const bool can_retry_with_actual_size = macro_id.is_shared_tablet_sub_meta_in_table();
  buffer.assign(nullptr, 0, 0);
  // 恢复路径只组装一次backup_path；先按默认大小读取，避免每次读取前都先get_file_length，减少get_file_length调用次数。
  // 仅在buffer不足（OB_BUF_NOT_ENOUGH）时，才认为是tiered表数据行太大导致的，获取实际长度后再重试一次；
  // 其他错误（如对象不存在、权限错误）原样向上返回，不做无谓重试。
  if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(ObSSBackupUtils::get_macro_block_relative_path(macro_id, path, length))) {
    LOG_WARN("failed to get macro block relative path", K(ret), K(macro_id));
  } else if (OB_FAIL(backup_path.join(path))) {
    LOG_WARN("failed to join path", K(ret), K(path));
  } else if (OB_FAIL(read_ss_macro_block_(backup_path.get_obstr(),
                 backup_set_dest.get_storage_info(), mod, buf_size,
                 allocator, buffer))) {
    if (OB_BUF_NOT_ENOUGH == ret && can_retry_with_actual_size) {
      // tiered表的子元数据可能超过一个宏块大小，buffer不足时按实际文件长度重试一次。
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(util.get_file_length(backup_path.get_obstr(), backup_set_dest.get_storage_info(), buf_size))) {
        LOG_WARN("failed to get macro block file length", K(tmp_ret), K(backup_path));
      } else if (OB_UNLIKELY(buf_size <= 0)) {
        tmp_ret = OB_INVALID_DATA;
        LOG_WARN("invalid macro block file length", K(tmp_ret), K(backup_path), K(buf_size));
      } else if (OB_TMP_FAIL(read_ss_macro_block_(
                     backup_path.get_obstr(), backup_set_dest.get_storage_info(),
                     mod, buf_size, allocator, buffer))) {
        LOG_WARN("failed to read ss macro block with actual file length", K(tmp_ret), K(macro_id), K(buf_size));
      }
      ret = tmp_ret;
    } else if (OB_BUF_NOT_ENOUGH == ret) {
      LOG_WARN("failed to read ss macro block", K(ret), K(macro_id), K(buf_size));
    }
  }
  return ret;
}

int ObSSBackupRestoreUtil::read_ss_macro_block_(const ObString &backup_path,
    const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
    const int64_t buf_size, common::ObIAllocator &allocator,
    blocksstable::ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t real_read_size = 0;
  if (OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read buffer size", K(ret), K(buf_size));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read buf", K(ret), K(buf_size));
  } else if (OB_FAIL(read_file_(backup_path, storage_info, mod, buf, buf_size, real_read_size))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      LOG_DEBUG("failed to read file", K(ret), K(backup_path), K(buf_size));
    } else {
      LOG_WARN("failed to read file", K(ret), K(backup_path), K(buf_size));
    }
  } else {
    buffer = ObBufferReader(buf, real_read_size, real_read_size);
    LOG_DEBUG("read ss macro block", K(real_read_size), K(backup_path));
  }
  // 读取失败时不向调用方暴露临时缓冲区，并释放本次分配。
  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator.free(buf);
  }
  return ret;
}

int ObSSBackupRestoreUtil::read_file_(const ObString &path,
    const share::ObBackupStorageInfo *storage_info, const ObStorageIdMod &mod,
    char *buf, const int64_t buf_size, int64_t &real_read_size)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  real_read_size = 0;
  if (OB_UNLIKELY(0 == path.length() || !mod.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "path is invalid", K(path), K(mod), K(ret));
  } else if (OB_FAIL(util.read_single_file(path,
                                           storage_info,
                                           buf,
                                           buf_size,
                                           real_read_size,
                                           mod))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      STORAGE_LOG(DEBUG, "fail to pread file", K(ret), K(path));
    } else {
      STORAGE_LOG(WARN, "fail to pread file", K(ret), K(path));
    }
  }
  return ret;
}

int ObSSBackupRestoreUtil::reload_macro_block_tasks(
      common::ObISQLClient &sql_proxy,
      const share::ObSSHAMacroTaskType &task_type,
      const share::ObBackupJobAttr &job,
      const share::ObBackupSetTaskAttr &set_task_attr,
      rootserver::ObBackupTaskSchedulerQueue &queue)
{
  int ret = OB_SUCCESS;
  if (!task_type.is_valid() || !job.is_valid() || !set_task_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(task_type), K(job), K(set_task_attr));
  } else {
    HEAP_VAR(ObArray<share::ObSSHAMacroBlockTaskAttr>, macro_tasks) {
      if (OB_FAIL(share::ObSSHAMacroBlockTaskOperator::get_pending_or_doing_tasks(
          sql_proxy, job.tenant_id_, job.job_id_, task_type, macro_tasks))) {
        LOG_WARN("failed to get macro block tasks", K(ret), K(job));
      } else if (macro_tasks.empty()) { // no macro task, no need to reload
      } else if (OB_FAIL(do_reload_macro_block_tasks_(job, set_task_attr, macro_tasks, queue))) {
        LOG_WARN("failed to reload macro block task to scheduler", K(ret), K(job), K(macro_tasks));
      }
    }
  }
  return ret;
}

int ObSSBackupRestoreUtil::do_reload_macro_block_tasks_(
      const share::ObBackupJobAttr &job,
      const share::ObBackupSetTaskAttr &set_task_attr,
      const ObIArray<share::ObSSHAMacroBlockTaskAttr> &macro_tasks,
      rootserver::ObBackupTaskSchedulerQueue &queue)
{
  int ret = OB_SUCCESS;
  bool queue_is_full = false;
  HEAP_VAR(rootserver::ObSSHAMacroBlockScheduleTask, task) {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_tasks.count(); ++i) {
      const share::ObSSHAMacroBlockTaskAttr &macro_task_attr = macro_tasks.at(i);
      queue_is_full = false;
      task.reset();
      if (OB_FAIL(task.build(job, set_task_attr, macro_task_attr))) {
        LOG_WARN("failed to build macro block schedule task", K(ret), K(macro_task_attr));
      } else if (OB_FAIL(queue.check_queue_is_full(queue_is_full))) {
        LOG_WARN("failed to check queue is full", K(ret));
      } else if (queue_is_full) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("queue is full, stop reload macro block tasks", K(task));
      } else if (OB_FAIL(queue.push_task(task))) {
        if (OB_ENTRY_EXIST == ret) {
          LOG_DEBUG("task already exist in queue, skip", K(task));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to push task to queue", K(ret), K(task));
        }
      } else {
        LOG_DEBUG("successfully reload macro block task to queue", K(task));
      }
    }    
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
