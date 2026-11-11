/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_storage_ha_writer.h"
#include "storage/backup/ob_ss_backup_utils.h"
#include "storage/lob/store/ob_lob_outtable_io.h"

namespace oceanbase
{
namespace storage
{
ObCopySSMacroBlockRestoreWriter::ObCopySSMacroBlockRestoreWriter()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    dag_id_(),
    reader_(nullptr),
    macro_checker_()
{  
}

ObCopySSMacroBlockRestoreWriter::~ObCopySSMacroBlockRestoreWriter()
{
}

int ObCopySSMacroBlockRestoreWriter::init(const uint64_t tenant_id, 
                                          const share::ObLSID &ls_id, 
                                          const ObDagId &dag_id,
                                          ObCopySSMacroBlockRestoreReader *reader)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
            || !ls_id.is_valid()
            || dag_id.is_invalid()
            || OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;    
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(dag_id), KP(reader));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    dag_id_.set(dag_id);
    reader_ = reader;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSMacroBlockRestoreWriter::process()
{
  static const int64_t PIPE_DEPTH = 2;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_cancel = false;
  ObArenaAllocator allocators[PIPE_DEPTH]; // two allocators, one for read, one for write
  blocksstable::ObBufferReader macro_data;
  blocksstable::MacroBlockId macro_block_id;
  blocksstable::ObStorageObjectHandle write_handles[PIPE_DEPTH];
  blocksstable::ObStorageObjectWriteInfo write_info;
  ObStorageObjectOpt opt;
  int64_t write_count = 0;
  int64_t write_size = 0;
  int64_t data_size = 0;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t allocator_idx = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss macro block reader is not inited", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      blocksstable::ObStorageObjectHandle &cur_handle = write_handles[allocator_idx];
      ObArenaAllocator &cur_allocator = allocators[allocator_idx];

      if (!GCTX.omt_->has_tenant(tenant_id_)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exists, stop copy", K(ret), K_(tenant_id));
        break;
      } else if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(dag_id_, is_cancel))) {
        LOG_WARN("failed to check is task canceled", K(ret), K_(dag_id));
      } else if (is_cancel) {
        ret = OB_CANCELED;
        LOG_WARN("copy task has been canceled, skip remaining macro blocks", K(ret), K_(dag_id));
        break;
      } else if (!cur_handle.is_empty() && OB_FAIL(cur_handle.wait())) {
        LOG_WARN("fail to wait write handle", K(ret), K(write_info));
      } else {
        cur_handle.reset();
        cur_allocator.reuse();
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(reader_->get_next_macro_block(cur_allocator, macro_data, macro_block_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("get this batch of macro blocks end", K_(tenant_id), K_(ls_id));
          break;
        } else {
          LOG_WARN("fail to get next macro block", K(ret));
        }
      } else if (ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO == macro_block_id.storage_object_type()) {
        // Preserve the original object ID and exact byte length; LOB data has no SSTable header.
        if (OB_FAIL(ObLobOutTableIO::write(macro_block_id, ObString(macro_data.length(), macro_data.data())))) {
          LOG_WARN("failed to restore lob object", K(ret), K(macro_block_id));
        }
      } else if (!macro_block_id.is_shared_tablet_sub_meta_in_table()
                 && OB_FAIL(check_macro_block_(macro_data))) {
        LOG_WARN("fail to check macro block, fatal error", K(ret), K(macro_data));
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(set_macro_write_info_(macro_block_id, write_info, opt))) {
        LOG_WARN("fail to set macro write info", K(ret), K(macro_block_id));
      } else if (OB_FAIL(write_macro_block_(opt, write_info, cur_handle, macro_data))) {
        LOG_WARN("fail to write macro data", K(ret), K(opt), K(macro_block_id));
      }
      if (OB_SUCC(ret)) {
        ObTaskController::get().allow_next_syslog();
        ++write_count;
        write_size += macro_data.capacity();
        LOG_DEBUG("success copy macro block", K(write_count));
      }
      allocator_idx = (allocator_idx + 1) % PIPE_DEPTH;
    }
  }
  
  for (int64_t i = 0; i < PIPE_DEPTH; ++i) {
    blocksstable::ObStorageObjectHandle &handle = write_handles[i];
    if (!handle.is_empty()) {
      int tmp_ret = handle.wait();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to wait write handle", K(ret), K(tmp_ret), K(write_info));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }
  }
  
  data_size = reader_->get_data_size();
  
  int64_t cost_time_ms = (ObTimeUtility::current_time() - start_time) / 1000;
  int64_t data_size_KB = data_size / 1024;
  int64_t write_size_KB = write_size / 1024;
  int64_t total_speed_KB = 0;
  int64_t write_speed_KB = 0;
  if (cost_time_ms > 0) {
    total_speed_KB = data_size_KB * 1000 / cost_time_ms;
    write_speed_KB = write_size_KB * 1000 / cost_time_ms;
  }
  STORAGE_LOG(INFO, "finish copy macro block data", K(ret), K(write_count), K(cost_time_ms), 
                    "read_size_B", data_size, K(write_size), K(total_speed_KB), K(write_speed_KB));  
  return ret;  
}

int ObCopySSMacroBlockRestoreWriter::set_macro_write_info_(
    const MacroBlockId &macro_block_id,
    blocksstable::ObStorageObjectWriteInfo &write_info,
    blocksstable::ObStorageObjectOpt &opt)
{
  int ret = OB_SUCCESS;
  if (!macro_block_id.is_valid() || !macro_block_id.is_id_mode_share()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare shared macro write info get invalid argument", K(ret), K(macro_block_id));
  } else {
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_MIGRATE_WRITE);
    write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    write_info.io_desc_.set_unsealed();
    write_info.write_strategy_ = common::ObStorageObjectWriteStrategy::WRITE_THROUGH;
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.offset_ = 0;
    write_info.set_ls_epoch_id(0);
    
    if (ObStorageObjectType::SHARED_TABLET_SUB_META == macro_block_id.storage_object_type()) {
      const bool is_object_storage = macro_block_id.is_in_object_storage();
      opt.set_ss_tablet_sub_meta_opt(ls_id_.id(),
                                    macro_block_id.second_id(),
                                    macro_block_id.op_id(),
                                    macro_block_id.seq_id(),
                                    macro_block_id.meta_is_inner_tablet(),
                                    macro_block_id.reorganization_scn(),
                                    is_object_storage);
    } else {
      const ObStorageObjectType object_type = macro_block_id.storage_object_type();
      opt.set_ss_share_object_opt(object_type,
                                  macro_block_id.meta_is_inner_tablet(),
                                  ls_id_.id(),
                                  macro_block_id.second_id(),
                                  macro_block_id.fourth_id(),
                                  macro_block_id.seq_id(),
                                  macro_block_id.column_group_id(),
                                  macro_block_id.reorganization_scn());
      }    
    }
  return ret;
}

int ObCopySSMacroBlockRestoreWriter::write_macro_block_(
    const ObStorageObjectOpt &opt,
    blocksstable::ObStorageObjectWriteInfo &write_info,
    blocksstable::ObStorageObjectHandle &write_handle,
    blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    write_info.buffer_ = data.data();
    write_info.size_ = (ObStorageObjectType::SHARED_TABLET_SUB_META == opt.object_type_
                        && opt.ss_tablet_sub_meta_opt_.is_in_table_)
                        ? data.length()
                        : data.upper_align_length();
    write_handle.reset();
    if (OB_FAIL(ObObjectManager::async_write_object(opt, write_info, write_handle))) {
      LOG_WARN("fail to async write block", K(ret), K(data), K(write_info), K(write_handle));
    }
  }
  return ret;
}

int ObCopySSMacroBlockRestoreWriter::check_macro_block_(
    const blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (NULL == data.data() || data.length() < 0 || data.length() > data.capacity()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data", K(ret), K(data));
  } else if (OB_FAIL(macro_checker_.check(data.data(), data.length()))) {
    LOG_ERROR("failed to check macro block", K(ret), K(data));
  }
  return ret;
}

// ObCopySSMacroBlockBackupWriter
ObCopySSMacroBlockBackupWriter::ObCopySSMacroBlockBackupWriter()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    dag_id_(),
    backup_dest_(),
    backup_set_desc_(),
    mod_(),
    reader_(nullptr)
{
}

ObCopySSMacroBlockBackupWriter::~ObCopySSMacroBlockBackupWriter()
{
}

int ObCopySSMacroBlockBackupWriter::init(
    const uint64_t tenant_id,
    const ObDagId &dag_id,
    const share::ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &backup_set_desc,
    const common::ObStorageIdMod &mod,
    ObCopySSMacroBlockBackupReader *reader)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup macro block writer init twice", K(ret));
  } else if (!backup_dest.is_valid() || !backup_set_desc.is_valid() || !mod.is_valid() || OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_dest), K(backup_set_desc), K(mod), KP(reader));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
  } else {
    tenant_id_ = tenant_id;
    dag_id_.set(dag_id);
    backup_set_desc_ = backup_set_desc;
    mod_ = mod;
    reader_ = reader;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSMacroBlockBackupWriter::process()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  blocksstable::ObBufferReader macro_data;
  blocksstable::MacroBlockId macro_block_id;
  int64_t write_count = 0;
  bool is_cancel = false;
  int64_t write_size = 0;
  int64_t data_size = 0;  
  int64_t start_time = ObTimeUtility::current_time();
  int64_t allocator_idx = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup macro block writer not inited", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      allocator.reuse();
      if (!GCTX.omt_->has_tenant(tenant_id_)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exists, stop copy", K(ret), K_(tenant_id));
        break;
      } else if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(dag_id_, is_cancel))) {
        LOG_WARN("failed to check is task canceled", K(ret), K_(dag_id));
      } else if (is_cancel) {
        ret = OB_CANCELED;
        LOG_WARN("copy task has been canceled, skip remaining macro blocks", K(ret), K_(dag_id));
        break;
      } else if (OB_FAIL(reader_->get_next_macro_block(allocator, macro_data, macro_block_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("backup macro block writer process all macro blocks success", K(write_count));
          break;
        } else {
          LOG_WARN("fail to get next macro block", K(ret), K(macro_block_id));
        }
      } else {
        blocksstable::ObBufferReader write_data = macro_data;
        if (backup::ObSSBackupUtils::need_backup_common_header(macro_block_id)) {
          if (OB_FAIL(backup::ObSSBackupUtils::wrap_with_backup_common_header(
                  macro_block_id, macro_data, allocator, write_data))) {
            LOG_WARN("failed to wrap macro block with backup common header", K(ret), K(macro_block_id));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(write_macro_block_(macro_block_id, write_data))) {
          LOG_WARN("failed to write macro block", K(ret), K(macro_block_id));
        }
      }
      if (OB_SUCC(ret)) {
        ObTaskController::get().allow_next_syslog();
        ++write_count;
        LOG_DEBUG("success backup macro block", K(write_count), K(macro_block_id));
      }
    }
  }
  
  data_size = reader_->get_data_size();
  
  int64_t cost_time_ms = (ObTimeUtility::current_time() - start_time) / 1000;
  int64_t data_size_KB = data_size / 1024;
  int64_t write_size_KB = write_size / 1024;
  int64_t total_speed_KB = 0;
  int64_t write_speed_KB = 0;
  if (cost_time_ms > 0) {
    total_speed_KB = data_size_KB * 1000 / cost_time_ms;
    write_speed_KB = write_size_KB * 1000 / cost_time_ms;
  }
  STORAGE_LOG(INFO, "finish copy macro block data", K(ret), K(write_count), K(cost_time_ms), 
                    "read_size_B", data_size, K(write_size), K(total_speed_KB), K(write_speed_KB));   
  return ret;
}

int ObCopySSMacroBlockBackupWriter::write_macro_block_(
    const blocksstable::MacroBlockId &block_id,
    const blocksstable::ObBufferReader &macro_data)
{
  int ret = OB_SUCCESS;
  share::ObBackupPath backup_path;
  common::ObBackupIoAdapter util;
  
  if (OB_FAIL(get_macro_block_backup_path_(block_id, backup_dest_, backup_set_desc_, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret));
  } else if (OB_FAIL(util.mk_parent_dir(backup_path.get_obstr(), backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to mk parent dir", K(ret), K(backup_path));
  } else if (OB_FAIL(util.write_single_file(backup_path.get_obstr(),
                                            backup_dest_.get_storage_info(),
                                            macro_data.data(),
                                            macro_data.length(),
                                            mod_))) {
    LOG_WARN("failed to write single file", K(ret));
  } else {
    LOG_DEBUG("write single file success", K(backup_path), K(block_id));
  }
  return ret;
}

int ObCopySSMacroBlockBackupWriter::get_macro_block_backup_path_(
    const blocksstable::MacroBlockId &block_id,
    const share::ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &desc,
    share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  share::ObBackupDest backup_set_dest;
  const int64_t length = OB_MAX_BACKUP_PATH_LENGTH;
  char path[length] = {0};
  if (OB_FAIL(share::ObBackupPathUtil::construct_backup_set_dest(backup_dest, desc, backup_set_dest))) {
    LOG_WARN("failed to construct backup set dest", K(ret), K(backup_dest), K(desc));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_backup_set_dir_path(backup_set_dest, backup_path))) {
    LOG_WARN("failed to get backup set dir path", K(ret), K(backup_set_dest));
  } else if (OB_FAIL(backup::ObSSBackupUtils::get_macro_block_relative_path(block_id, path, length))) {
    LOG_WARN("failed to get macro block relative path", K(ret), K(block_id));
  } else if (OB_FAIL(backup_path.join(path))) {
    LOG_WARN("failed to join path", K(ret), K(path));
  } else {
    LOG_DEBUG("get macro block path success", K(block_id), K(backup_path));
  }
  return ret;
}

}
}
