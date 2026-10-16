/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_file_manager.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "deps/oblib/src/lib/file/file_directory_utils.h"
#include "share/ob_ss_file_util.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_file_op.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_remote_io_adapter.h"
#include "share/io/ob_io_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "share/ob_server_struct.h"
#include "share/ob_debug_sync.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_store.h"
#include "lib/literals/ob_literals.h"
#include "lib/utility/utility.h"
namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;
/**
 * --------------------------------ObSegmentFileInfo------------------------------------
 */
void ObSegmentFileInfo::reset()
{
  file_id_.reset();
  func_type_ = 0;
}

// if total_dir_size_ >= initial_dir_size, return 0, because total_file_size_ already includes the dir size;
// if total_dir_size_ < initial_dir_size, return initial_dir_size - total_dir_size_, we need to calibrate the dir size to the initial size.
// initial_dir_size is the dir number * default dir size
// 1. when rename tmp.seq, it will modify dir m_time, this dir can not be list when scanning disk.
// 2. when write new file, it will modify dir m_time and may expand the dir size, this dir can not be list when scanning disk.
// so we need dir_num in mem to check.
int ObCalibrateDiskSpaceResult::get_dir_calibrate_size(const int64_t initial_dir_num, int64_t &calibrate_size) const
{
  int ret = OB_SUCCESS;
  // when one dir has many files, its size will be larger than the 4KB. 
  // For example, tmp files in tmp_data dir are frequently created and deleted, its size will be larger than the 4KB.
  // so we need to set a max tolerance size to check whether the dir size is too large.
  const int64_t initial_dir_size = initial_dir_num * ObDirManager::DEFAULT_DIR_SIZE;
  int64_t deviation_threshold = 0;
  calibrate_size = 0;
  if (total_dir_size_ > initial_dir_size) {
    const uint64_t diff_size = total_dir_size_ - initial_dir_size;
    if (OB_FAIL(ObCalibrateDiskSpaceUtil::get_deviation_threshold(diff_size, deviation_threshold))) {
      LOG_WARN("fail to get deviation threshold", KR(ret), K(diff_size), K(initial_dir_size), K(total_dir_size_), K(total_dir_num_));
    } else if (OB_UNLIKELY(diff_size > (deviation_threshold / 2))) {
      // deviation_threshold is the threshold of files and dirs, so we need to divide by 2 to get the threshold of dirs.
      LOG_ERROR("total dir size is larger than initial dir size", K(diff_size), K(deviation_threshold), K(total_dir_size_), K(initial_dir_size), K(total_dir_num_));
    } else {
      LOG_WARN("total dir size is larger than initial dir size", K(diff_size), K(deviation_threshold), K(total_dir_size_), K(initial_dir_size), K(total_dir_num_));
    }
  } else {
    calibrate_size = initial_dir_size - total_dir_size_;
  }
  FLOG_INFO("finish to get dir calibrate size", KR(ret), K(calibrate_size), K(deviation_threshold), K(total_dir_size_),
            K(total_dir_num_), K(initial_dir_size), K(initial_dir_num));
  return ret;
}

int ObCalibrateDiskSpaceUtil::get_deviation_threshold(const int64_t deviation_alloc_size,
                                                      int64_t &deviation_threshold)
{
  int ret = OB_SUCCESS;
  int64_t deviation_percentage_threshold = 0;
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (deviation_alloc_size <= 0) {
    deviation_percentage_threshold = ACTUAL_LESS_DEVIATION_PERCENTAGE_THRESHOLD;
  } else if (deviation_alloc_size > 0) {
    deviation_percentage_threshold = ACTUAL_MORE_DEVIATION_PERCENTAGE_THRESHOLD;
  }
  if (OB_FAIL(ret)) {
  } else {
    const int64_t deviation_threshold_from_usage_ratio = tnt_disk_space_mgr->get_allocated_shared_macro_cache_size() *
                                                         deviation_percentage_threshold / 100;
    deviation_threshold = MAX(MIN(deviation_threshold_from_usage_ratio, MAX_BYTES_THRESHOLD), MIN_BYTES_THRESHOLD);
  }
  return ret;
}
/**
 * --------------------------------ObBaseFileManager------------------------------------
 */
ObBaseFileManager::ObBaseFileManager()
  : tenant_id_(OB_INVALID_TENANT_ID),
    io_callback_allocator_(),
    is_inited_(false),
    is_started_(false),
    start_server_time_s_(0),
    flushed_slog_seq_(0),
    io_detect_fd_(OB_INVALID_FD),
    fd_cache_node_pool_()
{
}

ObBaseFileManager::~ObBaseFileManager() {}

int ObBaseFileManager::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t mem_limit = 1 * 1024 * 1024 * 1024LL; // 1GB
  const int64_t FIXED_COUNT = 10000; // 1w
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("share storage disk space manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(io_callback_allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, "SSIOCallback",
                                                 tenant_id, mem_limit))) {
    LOG_WARN("fail to init io callback allocator", KR(ret), K(tenant_id), K(mem_limit));
  } else if (OB_FAIL(fd_cache_node_pool_.init(FIXED_COUNT, "UniqueFdPool", tenant_id))) {
    LOG_WARN("fail to init fd cache node pool", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    start_server_time_s_ = ObTimeUtility::current_time_s(); // second level
    is_inited_ = true;
    LOG_INFO("succ to init file manager", K_(tenant_id));
  }
  return ret;
}

int ObBaseFileManager::start()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObBaseFileManager::destroy()
{
  const int64_t start_us = ObTimeUtility::fast_current_time();
  LOG_INFO("start to destroy base file manager");
  // to destroy fd_cache_node_pool safely, wait all fd_cache_node memory are free
  while (fd_cache_node_pool_.get_alloc_count() != fd_cache_node_pool_.get_free_count()) {
    if (TC_REACH_TIME_INTERVAL(10 * 1000L * 1000L)) { // 10s
      LOG_INFO("wait fd cache node free", "alloc_count", fd_cache_node_pool_.get_alloc_count(),
               "free_count", fd_cache_node_pool_.get_free_count(), K_(tenant_id));
    }
    ob_usleep(500 * 1000L); // 500ms
  }
  fd_cache_node_pool_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  io_callback_allocator_.destroy();
  is_inited_ = false;
  flushed_slog_seq_ = 0;
  start_server_time_s_ = 0;
  is_started_ = false;
  const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
  LOG_INFO("finish to destroy base file manager", K(cost_us));
}

int ObBaseFileManager::get_storage_dest(ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDeviceConfig, device_config) {
    if (OB_FAIL(ObDeviceConfigMgr::get_instance().get_device_config(
        ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config))) {
      LOG_WARN("fail to get device config", KR(ret));
    } else if (OB_UNLIKELY(!device_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid device config", KR(ret), K(device_config));
    } else if (OB_FAIL(storage_dest.set(device_config.path_, device_config.endpoint_,
        device_config.access_info_, device_config.extension_))) {
      LOG_WARN("fail to set ObBackupDest", KR(ret), K(device_config));
    } else if (OB_UNLIKELY(!storage_dest.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage dest", KR(ret), K(storage_dest));
    }
  }
  return ret;
}

int ObBaseFileManager::do_aio_write(const ObStorageObjectWriteInfo &write_info,
                                    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!write_info.is_valid() || !object_handle.get_macro_id().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(write_info), K(object_handle));
  } else {
    if (object_handle.get_macro_id().storage_object_type() == ObStorageObjectType::PRIVATE_SLOG_FILE) {
      // try flush previous slog files, if previous slog files has not been flushed
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(try_flush_prev_slog_files(object_handle.get_macro_id()))) {
        LOG_WARN("fail to try flush prev slog files", KR(tmp_ret), "macro_id", object_handle.get_macro_id());
      }
    }
    if (OB_FAIL(SSObjUtil::aio_write(write_info, object_handle))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
      }
    }
  }
  return ret;
}

int ObBaseFileManager::try_flush_prev_slog_files(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  int64_t cur_slog_seq = macro_id.fifth_id();
  if (flushed_slog_seq_ < (cur_slog_seq - 1)) { // previous slogs has not been flushed
    for (int64_t i = (flushed_slog_seq_ + 1); (i < cur_slog_seq) && OB_SUCC(ret); ++i) {
      MacroBlockId flush_macro_id = macro_id;
      flush_macro_id.set_fifth_id(i);
      if (OB_SERVER_TENANT_ID == tenant_id_) { // 500 tenant
        if (OB_FAIL(OB_SERVER_FILE_MGR.push_to_flush_map(flush_macro_id))) {
          LOG_WARN("fail to push to flush map", KR(ret), K(flush_macro_id));
        }
      } else { // !500 tenant
        ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
        if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("macro cache mgr is null", KR(ret));
        } else if (OB_FAIL(macro_cache_mgr->push_to_flush_map(flush_macro_id, 0/*ls_epoch_id*/))) {
          LOG_WARN("fail to push to flush map", KR(ret), K(flush_macro_id));
        }
      }
    }
  }
  return ret;
}

int ObBaseFileManager::pread_file(const ObStorageObjectReadInfo &read_info,
                                  ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(async_pread_file(read_info, object_handle))) {
    LOG_WARN("fail to sync read file", KR(ret), K(read_info));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait io finish", KR(ret), K(read_info));
  }
  return ret;
}

int ObBaseFileManager::async_pread_file(const ObStorageObjectReadInfo &read_info,
                                        ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info), K(object_handle));
  } else if (OB_FAIL(do_aio_read(read_info, object_handle))) {
    LOG_WARN("fail to do aio read", KR(ret), K(read_info), K(object_handle));
  }
  return ret;
}

int ObBaseFileManager::delete_remote_file(const MacroBlockId &file_id)
{
  return delete_remote_file_with_ls_context(file_id, 0/*ls_epoch_id*/, 0/*ls_id*/);
}

int ObBaseFileManager::delete_remote_file_with_ls_context(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_FAIL(ObRemoteIoAdapter::del_file(tenant_id_, file_id, ls_epoch_id, GCONF.observer_id, ls_id))) {
    LOG_WARN("fail to delete remote file", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    FLOG_INFO("succ to delete remote file", K(file_id), K(ls_epoch_id), K_(tenant_id));
  }
  return ret;
}

int ObBaseFileManager::delete_file(const MacroBlockId &file_id)
{
  return delete_file_with_ls_context(file_id, 0/*ls_epoch_id*/, 0/*ls_id*/);
}

int ObBaseFileManager::delete_file_with_ls_context(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    const ObStorageObjectType object_type = file_id.storage_object_type();
    // step 1: delete remote file
    if (SSObjUtil::is_pin_local(object_type)) {
      // do nothing, this object type only store in local cache, do not delete remote file
    } else if (OB_FAIL(delete_remote_file_with_ls_context(file_id, ls_epoch_id, ls_id))) {
      LOG_WARN("fail to delete remote file", KR(ret), K(file_id), K(ls_epoch_id));
    }
    // step 2: delete local cache file
    if (OB_FAIL(ret)) {
    } else if (SSObjUtil::is_direct_read(object_type)) {
      // do nothing, this object type only store in remote object storage, do not delete local cache file
    } else if (OB_FAIL(ObSSObjectAccessUtil::delete_server_tenant_local_file(file_id, ls_epoch_id, ls_id))) {
      LOG_WARN("fail to delete local file", KR(ret), K(file_id), K(ls_epoch_id));
    }
  }
  return ret;
}

int ObBaseFileManager::do_aio_read(const ObStorageObjectReadInfo &read_info,
                                   ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info), K(object_handle));
  } else {
    if (OB_FAIL(SSObjUtil::aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    }
  }
  return ret;
}
int ObBaseFileManager::list_local_files(const char *dir_path, ObBaseDirEntryOperator &list_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(dir_path, list_op))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to list dirs", KR(ret), K(dir_path));
    }
  }
  return ret;
}

int ObBaseFileManager::list_local_scatter_files(
    const char *dir_path,
    const int64_t scatter_num,
    const char *scatter_format,
    ObBaseDirEntryOperator &list_op)
{
  int ret = OB_SUCCESS;
  char scatter_dir_format[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path) || OB_UNLIKELY(scatter_num <= 0) || OB_ISNULL(scatter_format)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dir_path), K(scatter_num), KP(scatter_format));
  } else if (OB_FAIL(databuff_printf(scatter_dir_format, sizeof(scatter_dir_format),
                                     "%%s/%s", scatter_format))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(scatter_format));
  } else {
    char scatter_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    for (int64_t i = 0; OB_SUCC(ret) && (i < scatter_num); ++i) {
      scatter_dir_path[0] = '\0';
      if (OB_FAIL(databuff_printf(scatter_dir_path, sizeof(scatter_dir_path),
                                  scatter_dir_format, dir_path, i))) {
        LOG_WARN("fail to databuff printf", KR(ret), K(dir_path), K(i));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(scatter_dir_path, list_op))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to list dirs", KR(ret), K(dir_path));
        }
      }
    }
  }
  return ret;
}

int ObBaseFileManager::list_local_files_rec(const char *dir_path,
                                            ObScanDirOp &list_file_op,
                                            ObScanDirOp &list_dir_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir_rec(dir_path, list_file_op, list_dir_op))) {
    LOG_WARN("fail to list dirs", KR(ret), K(dir_path));
  }
  return ret;
}

int ObBaseFileManager::list_remote_files(const char *dir_path, ObBaseDirEntryOperator &list_op)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  ObBackupDest storage_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(get_storage_dest(storage_dest))) {
    LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
  } else if (OB_FAIL(adapter.list_files(dir_path, storage_dest.get_storage_info(), list_op))) {
    LOG_WARN("fail to list files", KR(ret), K(dir_path));
  }
  return ret;
}

int ObBaseFileManager::list_remote_macro_files(
    const bool is_ls_inner_tablet, const int64_t ls_id,
    const int64_t tablet_id, const int64_t reorganization_scn,
    ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> table_macro_ids;
  table_macro_ids.set_attr(ObMemAttr(tenant_id_, "MacroFiles"));
  char macro_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY((ls_id < 0) || (tablet_id <= 0) || (reorganization_scn < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(reorganization_scn));
  } else if (is_user_tenant(MTL_ID())) {
    SMART_VAR(ObTieredMetadataStore, tm_store) {
      const uint64_t real_tenant_id = gen_meta_tenant_id(tenant_id_);
      if (OB_FAIL(tm_store.init(real_tenant_id, GCTX.sql_proxy_))) {
        LOG_WARN("fail to init tm_store", KR(ret), K(real_tenant_id));
      } else if (OB_FAIL(tm_store.list_meta_macro_files(is_ls_inner_tablet, ls_id, tablet_id, reorganization_scn, table_macro_ids))) {
        LOG_WARN("fail to list table macro files v2", KR(ret), K(tablet_id), K(reorganization_scn), K(is_ls_inner_tablet), K(ls_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_macro_dir(
                     macro_path, sizeof(macro_path), tenant_id_, is_ls_inner_tablet,
                     ls_id, tablet_id, reorganization_scn))) {
    LOG_WARN("fail to get remote macro dir", KR(ret), K(tablet_id), K(reorganization_scn),
             K_(tenant_id), K(is_ls_inner_tablet), K(ls_id), K(tablet_id));
  } else {
    ObMacroIdListOp remote_macro_op(macro_path);
    if (OB_FAIL(list_remote_files(macro_path, remote_macro_op))) {
      LOG_WARN("fail to list remote files", KR(ret), K(macro_path));
    } else if (OB_FAIL(remote_macro_op.get_macro_id_list(macro_ids))) {
      LOG_WARN("fail to get macro id list", KR(ret), K(macro_path));
    } else if (OB_FAIL(common::append(macro_ids, table_macro_ids))) {
      LOG_WARN("fail to append table macro ids", KR(ret), K(table_macro_ids));
    }
  }
  return ret;
}

int ObBaseFileManager::list_remote_macro_files_by_op(const common::ObTabletID &tablet_id, const int64_t ls_id,
                                                     const int64_t reorganization_scn, const char *sstable_type_str,
                                                     const int64_t op_id, common::ObIArray<blocksstable::MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  char macro_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY((ls_id < 0) || (!tablet_id.is_valid()) || (reorganization_scn < 0) || (op_id < 0) ||
                         (sstable_type_str == nullptr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(reorganization_scn), K(op_id), K(sstable_type_str));
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_macro_dir_by_op(
                     macro_path, sizeof(macro_path), tenant_id_, tablet_id, ls_id,
                     reorganization_scn, sstable_type_str, op_id))) {
    LOG_WARN("fail to get remote macro dir", KR(ret), K(sstable_type_str), K(tablet_id), K(ls_id), K(reorganization_scn),
             K_(tenant_id), K(op_id));
  } else {
    ObMacroIdListOp remote_macro_op(macro_path);
    if (OB_FAIL(list_remote_files(macro_path, remote_macro_op))) {
      LOG_WARN("fail to list remote files", KR(ret), K(macro_path));
    } else if (OB_FAIL(remote_macro_op.get_macro_id_list(macro_ids))) {
      LOG_WARN("fail to get macro id list", KR(ret), K(macro_path));
    }
  }
  return ret;
}

int ObBaseFileManager::delete_local_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(dir_path))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to delete local dir", KR(ret), K(dir_path));
    }
  } else {
    FLOG_INFO("succ to delete local dir", K(dir_path), "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
  }
  return ret;
}

int ObBaseFileManager::delete_remote_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest storage_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_ISNULL(dir_path) || strlen(dir_path) >= common::MAX_PATH_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(get_storage_dest(storage_dest))) {
    LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObBackupIoAdapter adapter;
    if (OB_FAIL(adapter.del_dir(dir_path, storage_dest.get_storage_info(), true/*recursive*/))) {
      LOG_WARN("fail to delete dir", KR(ret), K(dir_path));
    } else {
      FLOG_INFO("succ to delete remote dir", K(dir_path), "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObBaseFileManager::fsync_file(const MacroBlockId &file_id, const int64_t ls_epoch_id, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  ObPathContext ctx;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_cache*/, GCONF.observer_id, ls_id))) {
    LOG_WARN("fail to convert file path", KR(ret), K(ctx));
  } else if (OB_FAIL(ObSSFileUtil::fsync_file(ctx.get_path()))) {
    LOG_WARN("fail to fsync file", KR(ret), K(ctx));
  }
  return ret;
}

int ObBaseFileManager::get_file_length(const MacroBlockId &file_id, const int64_t ls_epoch_id, int64_t &file_length, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObStorageObjectType object_type = file_id.storage_object_type();
    // step 1: get file length from local cache
    if (SSObjUtil::is_direct_read(object_type)) {
      // this object type only store in remote object storage, do not get file length from local cache
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    } else if (OB_FAIL(get_local_file_length(file_id, ls_epoch_id, file_length, ls_id))) {
      LOG_WARN("fail to get local file length", KR(ret), K(file_id), K(ls_epoch_id), K(file_length));
    }
    // step 2: get file length from remote object storage
    if (SSObjUtil::is_pin_local(object_type)) {
      // do nothing, this object type only store in local cache, do not get file length from remote object storage
    } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(get_remote_file_length(file_id, ls_epoch_id, file_length, ls_id))) {
        LOG_WARN("fail to get remote file length", KR(ret), K(file_id), K(ls_epoch_id), K(file_length));
      }
    }
  }
  return ret;
}

int ObBaseFileManager::get_local_file_length(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    int64_t &file_length,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (SSObjUtil::is_store_in_table(file_id)) {
    // STORE_IN_TABLE which store in table or micro cache
    // thus, these is no need to get file length from local cache
    file_length = 0;
  } else {
    ObIODFileStat statbuf;
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_cache*/, GCONF.observer_id, ls_id))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf))) {
      LOG_WARN("fail to stat", KR(ret), K(ctx));
    } else {
      file_length = statbuf.size_;
    }
  }
  return ret;
}

int ObBaseFileManager::get_remote_file_length(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    int64_t &file_length,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObBackupIoAdapter adapter;
    ObBackupDest storage_dest;
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, false/*is_local_cache*/, GCONF.observer_id, ls_id))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
    } else if (OB_FAIL(adapter.get_file_length(ctx.get_path(), storage_dest.get_storage_info(), file_length))) {
      LOG_WARN("fail to get file length", KR(ret), K(ctx));
    }
  }
  return ret;
}

int ObBaseFileManager::is_exist_file(const MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObStorageObjectType object_type = file_id.storage_object_type();
    // step 1: is exist in local cache
    if (SSObjUtil::is_direct_read(object_type)) {
      // do nothing, this object type only store in remote object storage, do not do not judge is exist in local cache
    } else if (OB_FAIL(is_exist_local_file(file_id, ls_epoch_id, is_exist, ls_id))) {
      LOG_WARN("fail to judge local file exist", KR(ret), K(file_id), K(ls_epoch_id));
    }
    // step 2: is exist in remote object storage
    if (OB_FAIL(ret)) {
    } else if (SSObjUtil::is_pin_local(object_type)) {
      // do nothing, this object type only store in local cache, do not judge is exist in remote object storage
    } else if (!is_exist) {
      if (OB_FAIL(is_exist_remote_file(file_id, ls_epoch_id, is_exist, ls_id))) {
        LOG_WARN("fail to judge remote file exist", KR(ret), K(file_id), K(ls_epoch_id));
      }
    }
  }
  return ret;
}

int ObBaseFileManager::is_exist_local_file(const MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (SSObjUtil::is_store_in_table(file_id)) {
    is_exist = false; // STORE_IN_TABLE which store in table or micro cache, do not judge is exist in local cache
  } else {
    ObPathContext ctx;
    if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_cache*/, GCONF.observer_id, ls_id))) {
      LOG_WARN("fail to convert file path", KR(ret), K(ctx));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::exist(ctx.get_path(), is_exist))) {
      LOG_WARN("fail to exist", KR(ret), K(ctx));
    }
  }
  return ret;
}

int ObBaseFileManager::is_exist_remote_file(const MacroBlockId &file_id, const int64_t ls_epoch_id, bool &is_exist, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_FAIL(ObRemoteIoAdapter::is_exist(
      tenant_id_, file_id, ls_id, ls_epoch_id, GCONF.observer_id, is_exist))) {
    LOG_WARN("fail to check remote file exist", KR(ret), K(file_id));
  }
  return ret;
}

int ObBaseFileManager::get_slog_id_range(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                         int64_t &min_slog_id, int64_t &max_slog_id)
{
  int ret = OB_SUCCESS;
  char slog_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObSSGetSlogIdRangeFunctor functor;
  // step 1: list slog dir in local cache
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_tenant_slog_dir(slog_dir, sizeof(slog_dir), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant slog dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(list_local_files(slog_dir, functor))) {
    LOG_WARN("fail to list local files", KR(ret), K(slog_dir));
  }
  slog_dir[0] = '\0';
  // step 2: list slog dir in remote object storage
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tenant_slog_dir(slog_dir,
                     sizeof(slog_dir), tenant_id, tenant_epoch_id))) {
    // in case of observer start first time and before bootstrap, object_storage_root_dir_ is not exist.
    if (OB_DIR_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("log dir is empty", KR(ret), K(tenant_id), K(tenant_epoch_id), K(min_slog_id), K(max_slog_id));
    } else {
      LOG_WARN("fail to get remote tenant slog dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
    }
  } else if (OB_FAIL(list_remote_files(slog_dir, functor))) { 
    LOG_WARN("fail to list remote files", KR(ret), K(slog_dir));
  } else {
    min_slog_id = functor.get_min_file_id();
    max_slog_id = functor.get_max_file_id();
    if ((OB_INVALID_FILE_ID == min_slog_id) || (OB_INVALID_FILE_ID == max_slog_id) || (max_slog_id < min_slog_id)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("log dir is empty", KR(ret), K(tenant_id), K(tenant_epoch_id), K(min_slog_id), K(max_slog_id));
    }
  }
  return ret;
}

// remote path: cluster_id/server_id/tenant_id_epoch_id/ckpt/ or cluster_id/server_id/server_ckpt/
int ObBaseFileManager::list_private_ckpt_file(const uint64_t tenant_id, const int64_t tenant_epoch_id, ObIArray<int64_t> &object_ids)
{
  int ret = OB_SUCCESS;
  char ckpt_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (tenant_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_private_ckpt_file_dir(
                     ckpt_path, sizeof(ckpt_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get remote private ckpt file dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else {
    ObCkptFileListOp remote_ckpt_op;
    if (OB_FAIL(list_remote_files(ckpt_path, remote_ckpt_op))) {
      LOG_WARN("fail to list remote files", KR(ret), K(ckpt_path));
    } else if (OB_FAIL(remote_ckpt_op.get_object_id_list(object_ids))) {
      LOG_WARN("fail to get ckpt file id list", KR(ret), K(ckpt_path));
    }
  }
  return ret;
}

int ObBaseFileManager::update_flushed_slog_seq(const int64_t flushed_slog_seq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(flushed_slog_seq < flushed_slog_seq_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flushed slog seq should not rollback", KR(ret), K(flushed_slog_seq), K(flushed_slog_seq_));
  } else {
    flushed_slog_seq_ = flushed_slog_seq;
  }
  FLOG_INFO("finish to update flushed slog seq", KR(ret), K(flushed_slog_seq_), K(flushed_slog_seq));
  return ret;
}

int ObBaseFileManager::restore_flushed_slog_seq()
{
  int ret = OB_SUCCESS;
  int64_t min_slog_seq = -1;
  int64_t max_slog_seq = -1;
  const int64_t tenant_epoch_id = (OB_SERVER_TENANT_ID == tenant_id_) ? 0 : MTL_EPOCH_ID();
  if (OB_FAIL(get_slog_id_range(tenant_id_, tenant_epoch_id, min_slog_seq, max_slog_seq))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
      flushed_slog_seq_ = 0; // Note: no slog has been written
    } else {
      LOG_WARN("fail to get slog id range", KR(ret), K_(tenant_id), K(tenant_epoch_id));
    }
  } else {
    // Note: maybe min_slog_seq is already flushed to object storage. however, try flush again on
    // observer restart has no side-effect.
    flushed_slog_seq_ = min_slog_seq - 1;
  }
  return ret;
}

int ObBaseFileManager::create_io_detect_file()
{
  int ret = OB_SUCCESS;
  bool is_io_detect_file_exist = false;
  char io_detect_file_path[OB_MAX_FILE_PATH_LENGTH] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager not init", KR(ret));
  } else if (OB_FAIL(get_io_detect_file_path(io_detect_file_path, sizeof(io_detect_file_path)))) {
    LOG_WARN("fail to get io detect file path", KR(ret));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::exist(io_detect_file_path, is_io_detect_file_exist))) {
    LOG_WARN("fail to check if file exist", KR(ret), K(io_detect_file_path));
  } else {
    const int open_flag = (is_io_detect_file_exist
        ? (O_DIRECT | O_RDWR)
        : (O_CREAT | O_EXCL | O_DIRECT | O_RDWR));
    const mode_t open_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    if (OB_FAIL(ObSSFileUtil::open(io_detect_file_path, open_flag, open_mode, io_detect_fd_))) {
      LOG_ERROR("fail to open io detect file", KR(ret), K(io_detect_file_path));
    } else if (OB_FAIL(fill_io_detect_file())) {
      LOG_WARN("fail to fill io detect file", KR(ret), K(io_detect_file_path));
    }
  }
  return ret;
}

int ObBaseFileManager::fill_io_detect_file()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager not init", KR(ret));
  } else {
    const int64_t IO_DETECT_FILE_SIZE = 4096; // 4KB
    char write_buf[IO_DETECT_FILE_SIZE] = { 0 };
    ObIOInfo io_info;
    io_info.tenant_id_ = tenant_id_;
    io_info.offset_ = 0;
    io_info.size_ = IO_DETECT_FILE_SIZE;
    io_info.buf_ = write_buf;
    io_info.flag_.set_mode(ObIOMode::WRITE);
    io_info.flag_.set_sys_module_id(ObIOModule::DETECT_IO);
    io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    io_info.fd_.first_id_ = ObIOFd::NORMAL_FILE_ID;
    io_info.fd_.second_id_ = io_detect_fd_;
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    io_info.timeout_us_ = GCONF._data_storage_io_timeout;
    if (OB_FAIL(OB_IO_MANAGER.write(io_info))) {
      LOG_WARN("fail to write io detect file", KR(ret), K(io_info));
    }
  }
  return ret;
}

void ObBaseFileManager::close_io_detect_file()
{
  int ret = OB_SUCCESS;
  if (io_detect_fd_ != OB_INVALID_FD) {
    if (OB_FAIL(ObSSFileUtil::close(io_detect_fd_))) {
      LOG_ERROR("fail to close fd", KR(ret), K(io_detect_fd_));
    }
    io_detect_fd_ = OB_INVALID_FD;
  }
}

/**
 * --------------------------------ObTenantFileManager------------------------------------
 */
ObTenantFileManager::ObTenantFileManager()
  : ObBaseFileManager(),
    flush_queue_(),
    preread_cache_mgr_(),
    segment_file_mgr_(),
    tg_id_(INVALID_TG_ID),
    calibrate_disk_space_task_(),
    check_expand_disk_task_(),
    tmp_seq_file_clean_task_(),
    fd_cache_mgr_(),
    is_pause_gc_(false),
    is_tmp_file_cache_pause_gc_(false),
    tmp_logical_delete_epoch_(0),
    tmp_logical_delete_inflight_(0),
    is_stop_(false)
{
}

ObTenantFileManager::~ObTenantFileManager()
{
  destroy();
}

int ObTenantFileManager::mtl_init(ObTenantFileManager *&tenant_file_manager)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_file_manager->init(MTL_ID()))) {
    LOG_WARN("fail to init", KR(ret));
  } else {
    LOG_INFO("succ to init tenant file manager", K(MTL_ID()));
  }
  return ret;
}

int ObTenantFileManager::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t BUCKET_NUM = 10000;
  ObMemAttr attr1(MTL_ID(), "AtomicMap");
  ObMemAttr attr2(MTL_ID(), "AtomicCleanMap");
  if (OB_FAIL(ObBaseFileManager::init(tenant_id))) {
    LOG_WARN("fail to init ObBaseFileManager", KR(ret), K(tenant_id));
  } else if (OB_FAIL(preread_cache_mgr_.init(this))) {
    LOG_WARN("fail to init preread cache manager", KR(ret));
  } else if (OB_FAIL(segment_file_mgr_.init(this))) {
    LOG_WARN("fail to init segment file manager", KR(ret));
  } else if (OB_FAIL(fd_cache_mgr_.init())) {
    LOG_WARN("fail to init fd cache mgr", KR(ret), K(tenant_id));
  } else if (OB_FAIL(calibrate_disk_space_task_.init(tenant_id))) {
    LOG_WARN("fail to init calibrate disk space task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_expand_disk_task_.init(tenant_id_))) {
    LOG_WARN("fail to init check expand disk task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_seq_file_clean_task_.init())) {
    LOG_WARN("fail to init tmp seq file clean task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(io_detect_task_.init(tenant_id))) {
    LOG_WARN("fail to init io detect task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(atomic_write_file_map_.create(BUCKET_NUM, attr1))) {
    LOG_WARN("fail to create atomic write file map", KR(ret));
  } else if (OB_FAIL(atomic_write_file_clean_map_.create(BUCKET_NUM, attr2))) {
    LOG_WARN("fail to create atomic write file clean map", KR(ret));
  } else {
    ATOMIC_STORE(&tmp_logical_delete_epoch_, 0);
    ATOMIC_STORE(&tmp_logical_delete_inflight_, 0);
  }

  return ret;
}

int ObTenantFileManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(OB_DIR_MGR.create_tenant_dir(tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to create tenant dir", KR(ret), K_(tenant_id), K(MTL_EPOCH_ID()));
  } else if (OB_FAIL(create_io_detect_file())) {
    LOG_WARN("fail to create io detect file", KR(ret), K(tenant_id_));
  } else {
    if (OB_FAIL(restore_flushed_slog_seq())) {
      LOG_WARN("fail to restore flushed slog seq", KR(ret));
    }
    // read tenant disk space meta file
    ObTenantDiskSpaceMeta disk_space_meta;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(read_tenant_disk_space_meta(disk_space_meta))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to read tenant disk space meta", KR(ret), K(disk_space_meta));
      }
    } else if (OB_UNLIKELY(disk_space_meta.body_.tenant_id_ != tenant_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected error", KR(ret), K_(tenant_id), K(disk_space_meta));
    } else {
      const uint8_t macro_cache_stat_num = disk_space_meta.body_.macro_cache_stats_.count();
      for (uint8_t i = 0; (i < macro_cache_stat_num) && OB_SUCC(ret); ++i) {
        ObSSMacroCacheType cur_macro_cache_type = static_cast<ObSSMacroCacheType>(i);
        ObSSMacroCacheStat &cur_macro_cache_stat = disk_space_meta.body_.macro_cache_stats_.at(i);
        const int64_t dir_num = disk_space_meta.body_.dir_nums_.at(i);
        const int64_t dir_size = disk_space_meta.body_.dir_sizes_.at(i);
        if (OB_FAIL(MTL(ObTenantDiskSpaceManager *)->update_macro_cache_stat_info(
              cur_macro_cache_type, cur_macro_cache_stat, dir_num, dir_size))) {
          LOG_WARN("fail to update macro cache stat", KR(ret),
                   K_SS_MACRO_CACHE_TYPE(cur_macro_cache_type), K(cur_macro_cache_stat));
        }
      }
    }
    // start tenant timer task
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::TmpFileFlushTimer, tg_id_))) {
      LOG_WARN("fail to create timer thread", KR(ret));
    } else if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("fail to start timer thread", KR(ret), K_(tg_id));
    } else if (OB_FAIL(start_timer_task())) {
      LOG_WARN("fail to start tenant timer task", KR(ret));
    } else if (OB_FAIL(preread_cache_mgr_.start())) {
      LOG_WARN("fail to start preread cache manager", KR(ret));
    } else if (OB_FAIL(segment_file_mgr_.start())) {
      LOG_WARN("fail to start segment file manager", KR(ret));
    } else if (OB_FAIL(fd_cache_mgr_.start(tg_id_))) {
      LOG_WARN("fail to start fd cache mgr", KR(ret), K_(tg_id));
    }
    if (OB_SUCC(ret)) {
      is_started_ = true;
      LOG_INFO("succeed to start tenant file manager");
    }
  }
  return ret;
}

int ObTenantFileManager::start_timer_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(calibrate_disk_space_task_.start())) {
    LOG_WARN("fail to start calibrate disk space task", KR(ret));
  } else if (OB_FAIL(check_expand_disk_task_.start(tg_id_))) {
    LOG_WARN("fail to start check expand disk task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(tmp_seq_file_clean_task_.start(tg_id_))) {
    LOG_WARN("fail to start tmp seq file clean task", KR(ret), K_(tg_id));
  } else if (OB_FAIL(io_detect_task_.start(tg_id_))) {
    LOG_WARN("fail to start io detect task", KR(ret), K(tg_id_));
  }
  return ret;
}

int ObTenantFileManager::get_io_detect_file_path(char *path, const int64_t length)
{
  int ret = OB_SUCCESS;
  char tenant_dir[OB_MAX_FILE_PATH_LENGTH] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(path) || OB_UNLIKELY(length <= 0 || length > OB_MAX_FILE_PATH_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(path), K(length));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(
      tenant_dir, sizeof(tenant_dir), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get local tenant dir", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s", tenant_dir, IO_DETECT_FILE_NAME))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_dir));
  }
  return ret;
}

void ObTenantFileManager::destroy()
{
  int ret = OB_SUCCESS;
  is_stop_ = true;
  is_pause_gc_ = false;
  is_tmp_file_cache_pause_gc_ = false;
  if (INVALID_TG_ID != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  atomic_write_file_clean_map_.destroy();
  atomic_write_file_map_.destroy();
  io_detect_task_.destroy();
  tmp_seq_file_clean_task_.destroy();
  check_expand_disk_task_.destroy();
  calibrate_disk_space_task_.destroy();
  close_io_detect_file();
  fd_cache_mgr_.destroy();
  segment_file_mgr_.destroy();
  preread_cache_mgr_.destroy();
  const int64_t queue_size = flush_queue_.size();
  ObLink *ptr = nullptr;
  ObSegmentFileInfo *seg_file_info = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && (i < queue_size); i++) {
    if (OB_FAIL(flush_queue_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to pop", KR(ret));
      }
    } else {
      seg_file_info = static_cast<ObSegmentFileInfo *>(ptr);
      OB_DELETE(ObSegmentFileInfo, attr, seg_file_info);
    }
  }
  // must wait all io finish, before destroy io_callback_allocator_ in ObBaseFileManager::destroy()
  if (IS_INIT) {
    const int64_t start_us = ObTimeUtility::current_time_us();
    ObTenantIOManager *tenant_io_mgr = nullptr;
    if (OB_ISNULL(tenant_io_mgr = MTL(ObTenantIOManager *))) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tenant io manager should not be null", "tenant_id", MTL_ID());
    } else {
      while (1 != tenant_io_mgr->get_ref_cnt()) {
        if (REACH_TIME_INTERVAL(1000L * 1000L)) { // 1s
          LOG_INFO("wait tenant io manager quit", "tenant_id", MTL_ID(), K(start_us), "ref_cnt",
                   tenant_io_mgr->get_ref_cnt());
        }
        ob_usleep((useconds_t)10L * 1000L); //10ms
      }
    }
  }
  ObBaseFileManager::destroy();
  // All file-manager tasks and I/O callbacks have quiesced; reset mutation state for re-init.
  ATOMIC_STORE(&tmp_logical_delete_epoch_, 0);
  ATOMIC_STORE(&tmp_logical_delete_inflight_, 0);
}

void ObTenantFileManager::stop()
{
  is_stop_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  fd_cache_mgr_.stop();
  segment_file_mgr_.stop();
  preread_cache_mgr_.stop();
  calibrate_disk_space_task_.stop();
}

void ObTenantFileManager::wait()
{
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  fd_cache_mgr_.wait();
  segment_file_mgr_.wait();
  preread_cache_mgr_.wait();
  calibrate_disk_space_task_.wait();
}

int ObTenantFileManager::read_tenant_disk_space_meta(ObTenantDiskSpaceMeta &disk_space_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    DefaultPageAllocator allocator("DiskSpaceMeta", tenant_id_);
    MacroBlockId file_id;
    file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TENANT_DISK_SPACE_META);
    file_id.set_second_id(tenant_id_);
    file_id.set_third_id(0);
    file_id.set_fourth_id(MTL_EPOCH_ID());
    int64_t ls_epoch = 0;
    int64_t file_size = 0;
    char *buf = nullptr;
    if (OB_FAIL(get_file_length(file_id, ls_epoch, file_size))) {
      LOG_WARN("fail to get file size", KR(ret), K(file_id), K(ls_epoch), K(file_size));
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(file_size));
    } else {
      ObStorageObjectHandle object_handle;
      ObStorageObjectReadInfo read_info;
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      read_info.buf_ = buf;
      read_info.macro_block_id_ = file_id;
      read_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
      read_info.offset_ = 0;
      read_info.size_ = file_size;
      read_info.set_ls_epoch_id(ls_epoch);
      read_info.mtl_tenant_id_ = tenant_id_;
      int64_t pos = 0;
      if (OB_FAIL(ObSSObjectAccessUtil::pread_file(read_info, object_handle))) {
        LOG_WARN("fail to pread file", KR(ret), K(read_info));
      } else if (OB_FAIL(disk_space_meta.deserialize(
          object_handle.get_buffer(), object_handle.get_data_size(), pos))) {
        LOG_WARN("fail to deserialize", KR(ret), K(file_id), K(read_info), K(object_handle));
      } else if (OB_UNLIKELY(!disk_space_meta.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid disk space meta", KR(ret), K(disk_space_meta));
      } else {
        LOG_INFO("succeed read tenant disk space meta", KR(ret), K(disk_space_meta), K(pos));
      }
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObTenantFileManager::write_tenant_disk_space_meta(ObTenantDiskSpaceMeta &disk_space_meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!disk_space_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk space meta", KR(ret), K(disk_space_meta));
  } else {
    DefaultPageAllocator allocator("DiskSpaceMeta", tenant_id_);
    MacroBlockId file_id;
    file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TENANT_DISK_SPACE_META);
    file_id.set_second_id(tenant_id_);
    file_id.set_third_id(0);
    file_id.set_fourth_id(MTL_EPOCH_ID());
    const int64_t serialize_size = disk_space_meta.get_serialize_size();
    const int64_t buf_len = upper_align(serialize_size, DIO_ALIGN_SIZE);
    int64_t pos = 0;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(buf_len));
    } else if (OB_FAIL(disk_space_meta.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize disk space meta", KR(ret), K(disk_space_meta));
    } else if (OB_UNLIKELY(pos != serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pos", KR(ret), K(pos), K(serialize_size));
    } else {
      memset(buf + pos, 0, buf_len - pos);
      int64_t ls_epoch = 0;
      ObStorageObjectWriteInfo write_info;
      ObStorageObjectHandle object_handle;
      write_info.buffer_ = buf;
      write_info.size_ = buf_len;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      write_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
      write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      write_info.set_ls_epoch_id(ls_epoch);
      write_info.mtl_tenant_id_ = tenant_id_;
      if (OB_FAIL(object_handle.set_macro_block_id(file_id))) {
        LOG_WARN("fail to set file id", KR(ret), K(file_id));
      } else if (OB_FAIL(ObSSObjectAccessUtil::write_file(write_info, object_handle))) {
        LOG_WARN("fail to write tenant disk space meta", KR(ret), K(disk_space_meta));
      }
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int ObTenantFileManager::calc_disk_space(
    const char *dir_path,
    const ObSSMacroCacheType cache_type,
    const int64_t start_calc_time_s,
    ObCalibrateDiskSpaceResult &calibrate_res)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0) || OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s), KP(dir_path));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObDirCalcSizeOp dir_calc_size_op(is_stop_);
    ObDirCalcSizeOp file_calc_size_op(is_stop_);
    dir_calc_size_op.set_macro_cache_type(cache_type);
    file_calc_size_op.set_macro_cache_type(cache_type);
    if (OB_FAIL(dir_calc_size_op.set_start_calc_size_time(start_calc_time_s))) {
      LOG_WARN("fail to set start calculate size time", KR(ret), K(start_calc_time_s));
    } else if (OB_FAIL(file_calc_size_op.set_start_calc_size_time(start_calc_time_s))) {
      LOG_WARN("fail to set start calculate size time", KR(ret), K(start_calc_time_s));
    } else if (OB_FAIL(list_local_files_rec(dir_path, file_calc_size_op, dir_calc_size_op))) {
      LOG_WARN("fail to list local files recursion", KR(ret), K(dir_path));
    } else {
      calibrate_res.total_file_size_ += file_calc_size_op.get_total_file_size();
      calibrate_res.total_file_size_ += dir_calc_size_op.get_total_file_size();
      calibrate_res.total_dir_size_ += dir_calc_size_op.get_total_file_size();
      calibrate_res.total_dir_num_ += dir_calc_size_op.get_total_dir_num();
      calibrate_res.hot_tablet_total_used_size_ += file_calc_size_op.get_hot_tablet_total_file_size();
      calibrate_res.del_tmp_seq_file_size_ += file_calc_size_op.get_del_tmp_seq_file_size();
      calibrate_res.tmp_seq_file_size_ += file_calc_size_op.get_tmp_seq_file_size();
      calibrate_res.tmp_seq_file_cnt_ += file_calc_size_op.get_tmp_seq_file_num();
      FLOG_INFO("succ to calc disk space", K(dir_path), K(calibrate_res), "not_in_meta_map_file_cnt",
                file_calc_size_op.get_not_in_meta_map_file_cnt(),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts,
                "file_size", file_calc_size_op.get_total_file_size(),
                "dir_size", dir_calc_size_op.get_total_file_size(),
                K(dir_calc_size_op));
    }
  }
  return ret;
}

int ObTenantFileManager::calc_macro_cache_disk_space(
    const ObSSMacroCacheType cache_type,
    const int64_t start_calc_time_s,
    ObCalibrateDiskSpaceResult &calibrate_res)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(start_calc_time_s));
  } else {
    switch (cache_type) {
      case ObSSMacroCacheType::META_FILE: {
        if (OB_FAIL(calc_meta_file_disk_space(start_calc_time_s, calibrate_res))) {
          LOG_WARN("fail to calc meta file disk space", KR(ret), K(start_calc_time_s));
        }
        break;
      }
      case ObSSMacroCacheType::TMP_FILE: {
        if (OB_FAIL(calc_tmp_file_disk_space(start_calc_time_s, calibrate_res))) {
          LOG_WARN("fail to calc tmp file disk space", KR(ret), K(start_calc_time_s));
        }
        break;
      }
      case ObSSMacroCacheType::MACRO_BLOCK: {
        if (OB_FAIL(calc_macro_block_disk_space(start_calc_time_s, calibrate_res))) {
          LOG_WARN("fail to calc macro block disk space", KR(ret), K(start_calc_time_s));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected macro cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
        break;
      }
    }
  }
  return ret;
}

// calc disk space of PRIVATE_TABLET_META
int ObTenantFileManager::calc_meta_file_disk_space(
    const int64_t start_calc_time_s,
    ObCalibrateDiskSpaceResult &calibrate_res)
{
  int ret = OB_SUCCESS;
  char ls_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s));
  } else if (OB_FAIL(OB_DIR_MGR.get_ls_dir(ls_dir, sizeof(ls_dir), MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get ls dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(ls_dir, ObSSMacroCacheType::META_FILE, start_calc_time_s, calibrate_res))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(ls_dir));
  } else {
    LOG_INFO("succ to calc meta file disk space", K(calibrate_res));
  }
  return ret;
}

int ObTenantFileManager::calc_tmp_file_disk_space(
    const int64_t start_calc_time_s,
    ObCalibrateDiskSpaceResult &calibrate_res)
{
  int ret = OB_SUCCESS;
  ObTimePiece time_piece;
  int64_t scan_attempt = 0;
  constexpr int64_t MAX_SCAN_RETRY_COUNT = 3;
  // A delete batch can keep short renames continuously in flight; allow the batch to make progress.
  constexpr int64_t SCAN_RETRY_INTERVAL_US = 100_ms;
  calibrate_res.reset();
  char tmp_file_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tmp_data_dir(tmp_file_dir, sizeof(tmp_file_dir), MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get tmp data dir", KR(ret));
  } else {
    bool is_scan_stable = false;
    while (OB_SUCC(ret) && !is_scan_stable && scan_attempt < MAX_SCAN_RETRY_COUNT) {
      ObCalibrateDiskSpaceResult scan_result;
      scan_attempt++;
      const int64_t epoch_before = get_tmp_logical_delete_epoch();
      const int64_t inflight_before = get_tmp_logical_delete_inflight();
      DEBUG_SYNC(BEFORE_TMP_FILE_CALIBRATE_SCAN);
      if (inflight_before > 0) {
        ret = OB_EAGAIN;
      } else if (OB_FAIL(calc_disk_space(
          tmp_file_dir, ObSSMacroCacheType::TMP_FILE, start_calc_time_s, scan_result))) {
        LOG_WARN("fail to calc disk space", KR(ret), K(tmp_file_dir), K(scan_attempt));
      } else {
        DEBUG_SYNC(AFTER_TMP_FILE_CALIBRATE_SCAN);
        const int64_t inflight_after = get_tmp_logical_delete_inflight();
        const int64_t epoch_after = get_tmp_logical_delete_epoch();
        if (epoch_before != epoch_after || 0 != inflight_after) {
          ret = OB_EAGAIN;
          LOG_WARN("tmp file directory changed during disk space scan, retry",
              KR(ret), K(tenant_id_), K(scan_attempt), K(epoch_before), K(epoch_after),
              K(inflight_before), K(inflight_after), K(scan_result));
        } else {
          calibrate_res = scan_result;
          is_scan_stable = true;
        }
      }

      if (OB_EAGAIN == ret && scan_attempt < MAX_SCAN_RETRY_COUNT) {
        ret = OB_SUCCESS;
        ob_usleep(SCAN_RETRY_INTERVAL_US);
      }
    }

    if (OB_EAGAIN == ret) {
      LOG_WARN("fail to get stable tmp file disk space scan",
          KR(ret), K(tenant_id_), K(scan_attempt));
    }
  }
  LOG_INFO("finish to calc tmp file disk space",
      KR(ret), K(tenant_id_), K(start_calc_time_s),
      K(scan_attempt), K(calibrate_res), K(time_piece));
  return ret;
}

// calc disk space of PRIVATE_[DATA|META]_MACRO, SHARED_[MAJOR|MINOR|MINI]_[DATA|META]_MACRO,
// SHARED_TABLET_SUB_META, EXTERNAL_TABLE_FILE
int ObTenantFileManager::calc_macro_block_disk_space(
    const int64_t start_calc_time_s,
    ObCalibrateDiskSpaceResult &calibrate_res)
{
  int ret = OB_SUCCESS;
  char private_macro_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  char shared_major_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  char shared_minor_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  char shared_mini_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  char shared_inc_major_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  char shared_tablet_sub_meta_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  char external_table_file_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(start_calc_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_time_s));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tablet_data_dir(private_macro_dir, sizeof(private_macro_dir),
                                                          MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get tablet data dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(private_macro_dir, ObSSMacroCacheType::MACRO_BLOCK, start_calc_time_s, calibrate_res))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(private_macro_dir));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_macro_cache_dir(shared_major_dir, sizeof(shared_major_dir),
                                                           MTL_ID(), MTL_EPOCH_ID(),
                                                           ObSharedMacroType::MAJOR))) {
    LOG_WARN("fail to get major data dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(shared_major_dir, ObSSMacroCacheType::MACRO_BLOCK, start_calc_time_s, calibrate_res))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(shared_major_dir));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_macro_cache_dir(shared_minor_dir, sizeof(shared_minor_dir),
                                                           MTL_ID(), MTL_EPOCH_ID(),
                                                           ObSharedMacroType::MINOR))) {
    LOG_WARN("fail to get major data dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(shared_minor_dir, ObSSMacroCacheType::MACRO_BLOCK, start_calc_time_s, calibrate_res))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(shared_minor_dir));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_macro_cache_dir(shared_mini_dir, sizeof(shared_mini_dir),
                                                           MTL_ID(), MTL_EPOCH_ID(),
                                                           ObSharedMacroType::MINI))) {
    LOG_WARN("fail to get major data dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(shared_mini_dir, ObSSMacroCacheType::MACRO_BLOCK, start_calc_time_s, calibrate_res))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(shared_mini_dir));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_macro_cache_dir(shared_inc_major_dir, sizeof(shared_inc_major_dir),
                                                           MTL_ID(), MTL_EPOCH_ID(),
                                                           ObSharedMacroType::INC_MAJOR))) {
    LOG_WARN("fail to get inc major data dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(shared_inc_major_dir, ObSSMacroCacheType::MACRO_BLOCK, start_calc_time_s, calibrate_res))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(shared_inc_major_dir));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_sub_meta_cache_dir(shared_tablet_sub_meta_dir,
                                                                      sizeof(shared_tablet_sub_meta_dir),
                                                                      MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get shared tablet sub meta cache dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(shared_tablet_sub_meta_dir, ObSSMacroCacheType::MACRO_BLOCK, start_calc_time_s, calibrate_res))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(shared_tablet_sub_meta_dir));
  } else if (OB_FAIL(OB_DIR_MGR.get_external_table_file_dir(external_table_file_dir,
                                                            sizeof(external_table_file_dir),
                                                            MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get external table file dir", KR(ret));
  } else if (OB_FAIL(calc_disk_space(external_table_file_dir, ObSSMacroCacheType::MACRO_BLOCK, start_calc_time_s, calibrate_res))) {
    LOG_WARN("fail to calc disk space", KR(ret), K(external_table_file_dir));
  } else {
    LOG_INFO("succ to calc macro block disk space", K(calibrate_res));
  }
  return ret;
}

int ObTenantFileManager::async_write_file(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle ori_object_handle(object_handle);  // when aio write failed, object_handle can reset
  const MacroBlockId &file_id = object_handle.get_macro_id();
  ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!SSObjUtil::need_fsync(file_id) || !write_info.is_valid() || !file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "storage object type", STI(object_type), K(write_info), K(object_handle));
  } else if (OB_FAIL(try_fsync_private_macro_parent_dir(object_type, file_id))) {
    LOG_WARN("fail to try fsync private macro dir", KR(ret), "macro_id",
              file_id, "ls_epoch_id", write_info.get_ls_epoch_id());
  } else {
    ObStorageObjectWriteInfo modifiable_write_info = write_info;
    if (OB_FAIL(aio_write_with_create_parent_dir(modifiable_write_info, ori_object_handle, object_handle))) {
      LOG_WARN("fail to aio write with create parent dir", KR(ret), K(write_info), K(object_handle));
    }
  }
  return ret;
}

int ObTenantFileManager::async_pread_file(const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_info), K(object_handle));
  } else if (OB_FAIL(do_aio_read(read_info, object_handle))) {
    LOG_WARN("fail to do aio read", KR(ret), K(read_info), K(object_handle));
  } else if (OB_TMP_FAIL(preread_cache_mgr_.refresh_preread_node_if_need(read_info))) { // do not affect async_read
    LOG_WARN("fail to check preread lru if need refresh", KR(tmp_ret), K(read_info));
  }
  return ret;
}

int ObTenantFileManager::delete_file_with_ls_context(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else {
    ObStorageObjectType object_type = file_id.storage_object_type();
    bool is_write_cache_exist = false;
    if (OB_FAIL(check_if_write_cache_exist(file_id, ls_epoch_id, is_write_cache_exist, ls_id))) {
      LOG_WARN("fail to check if write cache exist", KR(ret), K(file_id), K(ls_epoch_id));
    } else {
      // step 1: delete remote file
      // Note: is_write_cache_exist == true, means this file is in local write cache and is not
      // on object storage in most scenarios. we leverage this characteristic to avoid 'null-delete'
      // operations here. howerver, we do not guarantee that the file is must not on object
      // storage (e.g., concurrent eviction and deletion scenarios), which should depend on
      // 'check-delete' mechanism to delete.
      if (SSObjUtil::is_pin_local(object_type) || is_write_cache_exist) {
        // do nothing, this object type only store in local cache, do not delete remote file
      } else if (OB_FAIL(delete_remote_file_with_ls_context(file_id, ls_epoch_id, ls_id))) {
        LOG_WARN("fail to delete remote file", KR(ret), K(file_id), K(ls_epoch_id));
      }
      // step 2: delete local cache file
      if (OB_FAIL(ret)) {
      } else if (SSObjUtil::is_direct_read(object_type)) {
        // do nothing, this object type only store in remote object storage, do not delete local cache file
      } else if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(file_id, ls_epoch_id, true, true, false, ls_id))) {
        LOG_WARN("fail to delete local file", KR(ret), K(file_id), K(ls_epoch_id));
      }
    }
  }
  return ret;
}

int ObTenantFileManager::check_if_write_cache_exist(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    bool &is_exist,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObStorageObjectType object_type = file_id.storage_object_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || (ls_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (file_id.is_private_macro()) {
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache mgr is null", KR(ret));
    } else if (OB_FAIL(macro_cache_mgr->check_if_write_cache_exist(file_id, is_exist))) {
      LOG_WARN("fail to check if write cache exist", KR(ret), K(file_id));
    }
  } else if (SSObjUtil::is_private(object_type) && SSObjUtil::is_tablet_meta(object_type)) {
    if (OB_FAIL(is_exist_local_file(file_id, ls_epoch_id, is_exist, ls_id))) {
      LOG_WARN("fail to check if local file exist", KR(ret), K(file_id), K(ls_epoch_id));
    }
  } else {
    // do not check other than private_macro and private_tablet_meta
  }
  return ret;
}
int ObTenantFileManager::update_object_type_remote_delete_stat(const blocksstable::ObStorageObjectType &object_type,
                                                                const int result, const int64_t delta_cnt)
{
  int ret = OB_SUCCESS;
  ObSSLocalCacheService *local_cache_service = nullptr;
  if (!is_meta_tenant(MTL_ID()) && !is_sys_tenant(MTL_ID()) && !is_user_tenant(MTL_ID())) {
  } else if (OB_ISNULL(local_cache_service = MTL(ObSSLocalCacheService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache service is null", KR(ret));
  } else if (OB_FAIL(local_cache_service->update_object_type_stat(object_type, ObSSObjectTypeStatType::DELETE,
      true/*is_remote*/, result, delta_cnt, 0/*delta_size*/, 0/*delta_time_us*/))) {
    LOG_WARN("fail to update object type remote delete stat", KR(ret), K(object_type), K(result), K(delta_cnt));
  }
  return ret;
}

int ObTenantFileManager::delete_remote_file_with_ls_context(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_FAIL(ObBaseFileManager::delete_remote_file_with_ls_context(file_id, ls_epoch_id, ls_id))) {
    LOG_WARN("fail to delete remote file", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (SSObjUtil::is_tmp_file(file_id)) {
    // For tmp files, delete both meta and conflict info
    const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
    // Note: segment_file_mgr_.delete_meta expect not to fail
    if (OB_FAIL(segment_file_mgr_.delete_meta(seg_id))) {
      LOG_ERROR("fail to delete segment file meta and conflict info", KR(ret), K(file_id));
    } 
  }
  // update object type remote delete stat
  IGNORE_RETURN update_object_type_remote_delete_stat(file_id.storage_object_type(), ret, 1/*delta_cnt*/);
  return ret;
}

int ObTenantFileManager::delete_tmp_file(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  const int64_t tmp_file_id = file_id.second_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || !SSObjUtil::is_tmp_file(file_id) || tmp_file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), "storage object type", STI(file_id.storage_object_type()), K(tmp_file_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    char tmp_file_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    // step 1: delete remote tmp_file dir's segment files
    if (OB_FAIL(OB_DIR_MGR.get_remote_tmp_file_dir(tmp_file_path, sizeof(tmp_file_path),
                                                      tenant_id, tenant_epoch_id, tmp_file_id))) {
      LOG_WARN("fail to get remote tmp file dir",KR(ret), K(tmp_file_path));
    } else if (OB_FAIL(delete_remote_dir(tmp_file_path))) {
      LOG_WARN("fail to delete remote dir", KR(ret), K(tmp_file_path));
    }
    // step 2: delete local tmp_file dir's segment files
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(delete_local_tmp_file(file_id))) {
      LOG_WARN("fail to delete local tmp file", KR(ret), K(file_id));
    } else {
      FLOG_INFO("succ to delete tmp file", K(file_id), K(tenant_id), K(tenant_epoch_id),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::is_delete_read_cache_tmp_file(const blocksstable::MacroBlockId &file_id, bool &is_need_delete)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id));
  } else if (OB_FAIL(preread_cache_mgr_.is_exist_in_preread_map(file_id, is_exist))) {
    LOG_WARN("fail to check file id exist in preread map", KR(ret), K(file_id));
  } else if (is_exist) {
    // if exist in preread map, file_id is read cache, need delete
    is_need_delete = true;
  } else {
    // if not exist in preread map, file_id is write cache, cannot need delete
    is_need_delete = false;
  }
  return ret;
}

int ObTenantFileManager::delete_local_tmp_file(const MacroBlockId &file_id, const bool is_only_delete_read_cache)
{
  int ret = OB_SUCCESS;
  int64_t tmp_file_id = file_id.second_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || !SSObjUtil::is_tmp_file(file_id) || tmp_file_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), "storage object type", STI(file_id.storage_object_type()), K(tmp_file_id));
  } else {
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    char tmp_file_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    ObTmpFileSegListOp tmp_file_seg_op;
    if (OB_FAIL(OB_DIR_MGR.get_local_tmp_file_dir(tmp_file_path,
                       sizeof(tmp_file_path), tenant_id, tenant_epoch_id, tmp_file_id))) {
      LOG_WARN("fail to get tmp file dir", KR(ret), K(tmp_file_path));
    } else if (OB_FAIL(list_local_files(tmp_file_path, tmp_file_seg_op))) {
      LOG_WARN("fail to list local files", KR(ret), K(tmp_file_path));
    } else {
      ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> segment_file_ids;
      segment_file_ids.set_attr(ObMemAttr(tenant_id_, "DelFiles"));
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_file_seg_op.seg_ids_.count(); i++) {
        MacroBlockId segment_file_id;
        segment_file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        segment_file_id.set_second_id(tmp_file_id);
        segment_file_id.set_third_id(0);
        segment_file_id.set_fourth_id(tmp_file_seg_op.seg_ids_.at(i));
        bool is_need_delete = true;
        // if only delete tmp_file read cache, need check if exist in lru map
        if (is_only_delete_read_cache) {
          if (OB_FAIL(is_delete_read_cache_tmp_file(file_id, is_need_delete))) {
            LOG_WARN("fail to check tmp file read cache need delete", KR(ret), K(file_id));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (is_need_delete) {
          if (OB_FAIL(segment_file_ids.push_back(segment_file_id))) {
            LOG_WARN("fail to push back", KR(ret), K(segment_file_id));
          }
        }
      }
      // delete local tmp_file dir's segment files
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(delete_local_files(segment_file_ids))) {
        LOG_WARN("fail to delete local files", KR(ret), K(segment_file_ids));
      }
    }
    // delete local tmp_file directory
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_DIR_MGR.delete_tmp_file_dir(tenant_id, tenant_epoch_id, tmp_file_id))) {
      LOG_WARN("fail to delete tmp file dir", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
    } else {
      FLOG_INFO("succ to delete local tmp file", K(file_id), K(tenant_id), K(tenant_epoch_id));
    }
  }
  return ret;
}

int ObTenantFileManager::delete_tmp_file(const MacroBlockId &file_id, const int64_t length)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = file_id.storage_object_type();
  const int64_t tmp_file_id = file_id.second_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || !SSObjUtil::is_tmp_file(file_id) || tmp_file_id < 0 || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), "storage object type", STI(file_id.storage_object_type()),
             K(tmp_file_id), K(length));
  } else {
    const int64_t start_us = ObTimeUtility::current_time_us();
    ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> segment_file_ids;
    segment_file_ids.set_attr(ObMemAttr(tenant_id_, "DelFiles"));
    // step 1: delete tmp_file dir's segment file
    const int64_t max_segment_id = std::ceil((double)length / OB_DEFAULT_MACRO_BLOCK_SIZE);
    const int64_t last_segment_id = MAX(0, max_segment_id - 1);
    for (int64_t i = 0; OB_SUCC(ret) && i < max_segment_id; i++) {
      MacroBlockId segment_file_id;
      segment_file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
      segment_file_id.set_second_id(tmp_file_id);
      segment_file_id.set_third_id(0);
      segment_file_id.set_fourth_id(i);
      if (OB_FAIL(segment_file_ids.push_back(segment_file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(segment_file_id));
      }
    }
    TmpFileSegId last_seg_id(tmp_file_id, last_segment_id);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_delete_last_unsealed_segment(last_seg_id))) {
      LOG_WARN("fail to delete last unsealed seg meta", KR(ret), K(last_seg_id));
    } else if (OB_FAIL(delete_files(segment_file_ids))) {
      LOG_WARN("fail to delete files", KR(ret), K(segment_file_ids));
    }
    // step 2: delete local tmp_file directory
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_DIR_MGR.delete_tmp_file_dir(tenant_id_, MTL_EPOCH_ID(), tmp_file_id))) {
      LOG_WARN("fail to delete tmp file dir", KR(ret), K(tmp_file_id), K_(tenant_id));
    } else {
      FLOG_INFO("succ to delete tmp file", K(file_id), K(length), K_(tenant_id),
                "cost_time_us", ObTimeUtility::current_time_us() - start_us);
    }
  }
  return ret;
}

int ObTenantFileManager::delete_tmp_seq_file(const blocksstable::MacroBlockId &file_id, const ObAtomicWriteFileInfo &atomic_write_file)
{
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  ObSSMacroCacheType cache_type = atomic_write_file.cache_type_;
  ObPathContext ctx;
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid() || atomic_write_file.ls_epoch_id_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(atomic_write_file));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *)) ||
             OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr or file manager or disk space mgr is null", KR(ret),
             KP(macro_cache_mgr), KP(disk_space_mgr));
  } else if (!is_object_type_need_stat(file_id)) {
  } else if (is_pause_gc()) {
    // if pause gc, do not delete .tmp.seq file to avoid effect the disk space calibration 
  } else if (OB_FAIL(ctx.set_atomic_write_ctx(
      file_id, atomic_write_file.ls_epoch_id_, atomic_write_file.seq_,
      GCONF.observer_id, atomic_write_file.ls_id_))) {
    LOG_WARN("fail to convert file path", KR(ret), K(ctx));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(ctx.get_path()))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to del file", KR(ret), K(ctx));
    }
  } else if (ObSSMacroCacheType::MAX_TYPE == cache_type
      // 兜底：map中未记录cache_type（如旧版本写入的条目），回退到meta_map反查
      && OB_FAIL(macro_cache_mgr->get_macro_cache_type(file_id, cache_type))) {
    LOG_WARN("fail to get cache type", KR(ret), K(file_id));
  } else if (OB_FAIL(disk_space_mgr->free_file_size(atomic_write_file.size_, cache_type, ObDiskSpaceType::FILE))) {
    LOG_WARN("fail to free file size", KR(ret), K(atomic_write_file), K(cache_type), K(ctx));
  } else if (OB_FAIL(remove_preread_node(file_id))) {
    LOG_WARN("fail to remove lru node", KR(ret), K(file_id), K(ctx));
  }
  return ret;
}

int ObTenantFileManager::delete_tmp_files(const ObIArray<MacroBlockId> &file_ids, const ObIArray<int64_t> &lengths)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(file_ids.count() != lengths.count() || file_ids.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_ids), K(lengths));
  } else {
    const int64_t start_us = ObTimeUtility::current_time_us();
    ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> all_segment_file_ids;
    all_segment_file_ids.set_attr(ObMemAttr(tenant_id_, "DelFiles"));
    ObSEArray<TmpFileSegId, OB_DEFAULT_ARRAY_CAPACITY> last_seg_ids;
    last_seg_ids.set_attr(ObMemAttr(tenant_id_, "DelFiles"));
    
    // step 1: validate and collect all segment files
    for (int64_t idx = 0; OB_SUCC(ret) && idx < file_ids.count(); idx++) {
      const MacroBlockId &file_id = file_ids.at(idx);
      const int64_t length = lengths.at(idx);
      const int64_t tmp_file_id = file_id.second_id();
      
      if (OB_UNLIKELY(!file_id.is_valid() || !SSObjUtil::is_tmp_file(file_id) || tmp_file_id < 0 || length <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(idx), K(file_id), "storage object type", 
                 STI(file_id.storage_object_type()), K(tmp_file_id), K(length));
      } else {
        const int64_t max_segment_id = std::ceil((double)length / OB_DEFAULT_MACRO_BLOCK_SIZE);
        const int64_t last_segment_id = MAX(0, max_segment_id - 1);
        
        // collect segment file ids
        for (int64_t i = 0; OB_SUCC(ret) && i < max_segment_id; i++) {
          MacroBlockId segment_file_id;
          segment_file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
          segment_file_id.set_second_id(tmp_file_id);
          segment_file_id.set_third_id(0);
          segment_file_id.set_fourth_id(i);
          if (OB_FAIL(all_segment_file_ids.push_back(segment_file_id))) {
            LOG_WARN("fail to push back segment file id", KR(ret), K(segment_file_id));
          }
        }
        
        // collect last segment id for each tmp file
        if (OB_SUCC(ret)) {
          TmpFileSegId last_seg_id(tmp_file_id, last_segment_id);
          if (OB_FAIL(last_seg_ids.push_back(last_seg_id))) {
            LOG_WARN("fail to push back last seg id", KR(ret), K(last_seg_id));
          }
        }
      }
    }
    
    // step 2: delete last unsealed segments
    for (int64_t idx = 0; OB_SUCC(ret) && idx < last_seg_ids.count(); idx++) {
      const TmpFileSegId &last_seg_id = last_seg_ids.at(idx);
      if (OB_FAIL(try_delete_last_unsealed_segment(last_seg_id))) {
        LOG_WARN("fail to delete last unsealed seg meta", KR(ret), K(idx), K(last_seg_id));
      }
    }
    
    // step 3: batch delete all segment files
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(delete_files(all_segment_file_ids))) {
      LOG_WARN("fail to delete files", KR(ret), K(all_segment_file_ids));
    }
    
    // step 4: delete local tmp_file directories
    for (int64_t idx = 0; OB_SUCC(ret) && idx < file_ids.count(); idx++) {
      const int64_t tmp_file_id = file_ids.at(idx).second_id();
      if (OB_FAIL(OB_DIR_MGR.delete_tmp_file_dir(tenant_id_, MTL_EPOCH_ID(), tmp_file_id))) {
        LOG_WARN("fail to delete tmp file dir", KR(ret), K(idx), K(tmp_file_id), K_(tenant_id));
      }
    }
    
    if (OB_SUCC(ret)) {
      FLOG_INFO("succ to delete tmp files", K(file_ids), K(lengths), K_(tenant_id),
                "cost_time_us", ObTimeUtility::current_time_us() - start_us);
    }
  }
  return ret;
}

int ObTenantFileManager::delete_ls_dir(const int64_t ls_id,
                                       const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  char ls_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  int64_t file_size = 0;
  int64_t dir_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret));
  } else if (is_pause_gc()) {
    ret = OB_FILE_DELETE_FAILED;
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_WARN("the tenant is calibrating data disk alloc size and pausing gc, so do not allow to delete ls dir", KR(ret), K_(tenant_id));
    }
  } else if (OB_FAIL(OB_DIR_MGR.get_ls_id_dir(ls_dir_path, sizeof(ls_dir_path),
                     tenant_id_, MTL_EPOCH_ID(), ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls id dir", KR(ret), K(ls_id), K(ls_epoch_id), K_(tenant_id));
  } else if (OB_FAIL(ObDirManager::delete_dir_rec(ls_dir_path, file_size, dir_num))) {
    LOG_WARN("fail to delete dir file", KR(ret), K(ls_dir_path), K(file_size), K(dir_num));
  } else {
    FLOG_INFO("succ to delete ls dir", K(ls_id), K(ls_epoch_id), K(file_size), K(dir_num),
              K_(tenant_id), "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
  }

  // free size of deleted dirs and files, no matter OB_SUCC or OB_FAIL
  int tmp_ret = OB_SUCCESS;
  if (OB_LIKELY(file_size > 0) &&
      OB_TMP_FAIL(disk_space_mgr->free_file_size(file_size, ObSSMacroCacheType::META_FILE, ObDiskSpaceType::FILE))) { // expect won't fail
    LOG_WARN("fail to free file size", KR(tmp_ret), K(file_size));
  }
  if (OB_LIKELY(dir_num > 0)) {
    const int64_t dir_size = dir_num * ObDirManager::DEFAULT_DIR_SIZE;
    if (OB_TMP_FAIL(disk_space_mgr->free_file_size(dir_size, ObSSMacroCacheType::META_FILE, ObDiskSpaceType::DIR))) { // expect won't fail
      LOG_WARN("fail to free file size", KR(tmp_ret), K(dir_num), K(dir_size));
    }
  }
  return ret;
}

int ObTenantFileManager::delete_files(const ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    if (OB_FAIL(delete_remote_files(block_ids))) {
      LOG_WARN("fail to delete remote files", KR(ret));
    } else if (OB_FAIL(delete_local_files(block_ids))) {
      LOG_WARN("fail to delete local files", KR(ret));
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("succ to delete files", "block_ids_cnt", block_ids.count(),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::delete_local_files(const ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> deleted_block_ids;
    deleted_block_ids.set_attr(ObMemAttr(tenant_id_, "DelFiles"));
    for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); i++) {
      const MacroBlockId file_id = block_ids.at(i);
      ObStorageObjectType object_type = file_id.storage_object_type();
      if (SSObjUtil::is_direct_read(object_type)) {
        // do nothing, this object type only store in remote object storage, do not delete local cache file
      } else if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(file_id, 0/*ls_epoch_id*/, false/*is_print_log*/))) {
        LOG_WARN("fail to delete local file", KR(ret),  K(file_id));
      } else if (OB_FAIL(deleted_block_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
    if (OB_SUCC(ret)) {
      batch_print_deleted_file_ids(deleted_block_ids, true/*is_local*/);
      FLOG_INFO("succ to delete local files", "block_ids_cnt", block_ids.count(), "deleted_ids_cnt", deleted_block_ids.count(),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::delete_remote_files(const ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    if (OB_FAIL(ObRemoteIoAdapter::batch_del_files(tenant_id_, block_ids, GCONF.observer_id))) {
      LOG_WARN("fail to delete remote files", KR(ret), K(block_ids.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
        const MacroBlockId &file_id = block_ids.at(i);
        if (SSObjUtil::is_tmp_file(file_id)) {
          const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
          if (OB_FAIL(segment_file_mgr_.delete_meta(seg_id))) {
            LOG_ERROR("fail to delete segment file meta and conflict info", KR(ret), K(seg_id), K(file_id));
          }
        }
      }
      if (OB_SUCC(ret)) {
        batch_print_deleted_file_ids(block_ids, false /*is_local*/);
        FLOG_INFO("succ to delete remote files", "block_ids_cnt", block_ids.count(),
                  "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
      }
    }
  }
  // Batch deletion interacts with object storage once, counting as one I/O operation, so the object type is determined by the first one.
  if (block_ids.count() > 0) {
    ObStorageObjectType first_object_type = block_ids.at(0).storage_object_type();
    IGNORE_RETURN update_object_type_remote_delete_stat(first_object_type, ret, 1/*delta_cnt*/);
  }

  return ret;
}

void ObTenantFileManager::batch_print_deleted_file_ids(const ObIArray<MacroBlockId> &block_ids,
                                                       const bool is_local)
{
  int ret = OB_SUCCESS;
  ObSEArray<MacroBlockId, BATCH_PRINT_FILE_PATH_COUNT> batch_file_id_array;
  batch_file_id_array.set_max_print_count(BATCH_PRINT_FILE_PATH_COUNT);
  for (int64_t i = 0; i < block_ids.count(); i++) {
    ret = OB_SUCCESS; // ignore error code
    if (!is_local && SSObjUtil::is_pin_local(block_ids.at(i).storage_object_type())) {
    } else if (OB_FAIL(batch_file_id_array.push_back(block_ids.at(i)))) {
      LOG_WARN("fail to push back", KR(ret), K(block_ids.at(i)));
    } else if ((batch_file_id_array.count() >= BATCH_PRINT_FILE_PATH_COUNT) ||
               (i + 1 == block_ids.count())) {
      FLOG_INFO("succ to delete batch files", K(is_local), "array_cnt", batch_file_id_array.count(), K(batch_file_id_array));
      batch_file_id_array.reuse();
    }
  }
  // because array maybe fail to push back, lead to array's elements to not be printed
  if (batch_file_id_array.count() > 0) {
    FLOG_INFO("succ to delete batch files", K(is_local), "array_cnt", batch_file_id_array.count(), K(batch_file_id_array));
  }
}

int ObTenantFileManager::delete_shared_tablet_data_dir(
    const int64_t tablet_id,
    const int64_t reorganization_scn)
{
  int ret = OB_SUCCESS;
  char tablet_data_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(tablet_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_data_dir(tablet_data_dir_path,
                     sizeof(tablet_data_dir_path), tablet_id, reorganization_scn))) {
    LOG_WARN("fail to get shared tablet data dir", KR(ret), K(tablet_data_dir_path),
             K(tablet_id), K(reorganization_scn));
  } else if (OB_FAIL(delete_remote_dir(tablet_data_dir_path))) {
    LOG_WARN("fail to delete remote dir", KR(ret), K(tablet_data_dir_path));
  } else {
    FLOG_INFO("succ to delete shared tablet data dir", K(tablet_id));
  }
  return ret;
}

int ObTenantFileManager::delete_local_major_data_dir(const int64_t delete_time_stamp_s)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(delete_time_stamp_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(delete_time_stamp_s));
  } else if (is_pause_gc()) {
    ret = OB_FILE_DELETE_FAILED;
    if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_WARN("the tenant is calibrating data disk alloc size and pausing gc, so do not allow to delete files", KR(ret), K_(tenant_id));
    }
  } else {
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    const uint64_t tenant_id = MTL_ID();
    const int64_t tenant_epoch_id = MTL_EPOCH_ID();
    ObMajorDataDeleteOp delete_op;
    char major_data_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(delete_op.set_time_stamp(delete_time_stamp_s))) {
      LOG_WARN("fail to set time stamp", KR(ret), K_(start_server_time_s));
    } else if (OB_FAIL(OB_DIR_MGR.get_shared_macro_cache_dir(major_data_path, sizeof(major_data_path),
                                                             tenant_id, tenant_epoch_id,
                                                             ObSharedMacroType::MAJOR))) {
      LOG_WARN("fail to get major data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
    } else if (OB_FAIL(list_local_scatter_files(major_data_path,
                                                ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
                                                "%02lX", delete_op))) {
      LOG_WARN("fail to list local scatter files", KR(ret), K(major_data_path));
    } else {
      FLOG_INFO("succ to delete major data dir", KR(ret), K(delete_op), K(tenant_id), K(tenant_epoch_id),
                "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
    }
  }
  return ret;
}

int ObTenantFileManager::list_tmp_file(ObIArray<MacroBlockId> &tmp_file_ids)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> tmp_file_id_set;
  lib::ObMemAttr attr(tenant_id_, "ListTmpFile");
  char tmp_data_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObSingleNumFileListOp tmp_file_dir_op;
  tmp_file_dir_op.set_dir_flag();
  // step 1: list tmp file in local cache (only new version with scatter dir)
  // Note: old version tmp files are already deleted by clean_old_version_tmp_files_() at reboot
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(tmp_file_id_set.create(OB_DEFAULT_BUCKET_NUM, attr))){
    LOG_WARN("create block id set failed", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tmp_data_dir(tmp_data_path,
                     sizeof(tmp_data_path), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get tmp data dir", KR(ret), K(tmp_data_path), K_(tenant_id));
  } else if (OB_FAIL(list_local_scatter_files(tmp_data_path,
                                              ObDirManager::TMP_FILE_SCATTER_DIR_NUM,
                                              "%03ld", tmp_file_dir_op))) {
    LOG_WARN("fail to list local scatter files", KR(ret), K(tmp_data_path));
  } else {
    // successfully scanned scatter dir, collect all tmp_file_ids
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_file_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tmp_file_id_set.set_refactored(tmp_file_dir_op.file_list_.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(tmp_file_dir_op.file_list_.at(i)));
      }
    }
  }
  ObSingleNumFileListOp remote_tmp_data_dir_op;
  remote_tmp_data_dir_op.set_dir_flag();
  tmp_data_path[0] = '\0';
  // step 2: list tmp file in remote object storage
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tmp_data_dir(tmp_data_path,
                     sizeof(tmp_data_path), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get remote tmp data dir", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(list_remote_files(tmp_data_path, remote_tmp_data_dir_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(tmp_data_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_tmp_data_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tmp_file_id_set.set_refactored(remote_tmp_data_dir_op.file_list_.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(remote_tmp_data_dir_op.file_list_.at(i)));
      }
    }
  }
  hash::ObHashSet<int64_t>::const_iterator iter;
  for (iter = tmp_file_id_set.begin(); OB_SUCC(ret) && iter != tmp_file_id_set.end(); ++iter) {
    MacroBlockId file_id;
    file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    file_id.set_second_id(iter->first);
    if (OB_FAIL(tmp_file_ids.push_back(file_id))) {
      LOG_WARN("fail to push back", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObTenantFileManager::list_private_tablet_meta(
    const int64_t ls_id,
    const int64_t ls_epoch_id,
    const int64_t tablet_id,
    const int64_t private_macro_third_id,
    ObIArray<int64_t> &tablet_meta_vers)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> tablet_meta_id_set;
  lib::ObMemAttr attr(MTL_ID(), "ListTabMeta");
  char tablet_meta_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObTabletMetaListOp tablet_meta_dir_op;
  // step 1: list private tablet meta in local cache
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_meta_id_set.create(OB_DEFAULT_BUCKET_NUM, attr))){
    LOG_WARN("create block id set failed", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_tablet_meta_tablet_id_private_macro_third_id_dir(
                     tablet_meta_path, sizeof(tablet_meta_path), MTL_ID(), MTL_EPOCH_ID(),
                     ls_id, ls_epoch_id, tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to get private tablet meta dir", KR(ret), K(tablet_meta_path));
  } else if (OB_FAIL(list_local_files(tablet_meta_path, tablet_meta_dir_op))) {
    LOG_WARN("fail to list private tablet meta dir", KR(ret), K(tablet_meta_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_meta_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tablet_meta_id_set.set_refactored(tablet_meta_dir_op.file_list_.at(i)))) {
        LOG_WARN("add tablet meta id to set failed", KR(ret), K(tablet_meta_dir_op.file_list_.at(i)));
      }
    }
  }

  ObTabletMetaListOp remote_tablet_meta_dir_op;
  char remote_tablet_meta_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  remote_tablet_meta_path[0] = '\0';
  // step 2: list private tablet meta in object storage
  if (OB_FAIL(ret)) {
    // do nothing, already logged error in step 1
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tablet_meta_tablet_id_private_macro_third_id_dir(
                     remote_tablet_meta_path, sizeof(remote_tablet_meta_path), MTL_ID(), MTL_EPOCH_ID(),
                     ls_id, ls_epoch_id, tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to get remote private tablet meta dir", KR(ret));
  } else if (OB_FAIL(list_remote_files(remote_tablet_meta_path, remote_tablet_meta_dir_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(remote_tablet_meta_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_tablet_meta_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tablet_meta_id_set.set_refactored(remote_tablet_meta_dir_op.file_list_.at(i)))) {
        LOG_WARN("add private tablet meta id to set failed", KR(ret), K(remote_tablet_meta_dir_op.file_list_.at(i)));
      }
    }
  }

  hash::ObHashSet<int64_t>::const_iterator iter;
  for (iter = tablet_meta_id_set.begin(); OB_SUCC(ret) && iter != tablet_meta_id_set.end(); ++iter) {
    const int64_t ver_id = iter->first;
    if (OB_FAIL(tablet_meta_vers.push_back(ver_id))) {
      LOG_WARN("fail to push back", KR(ret), K(ver_id));
    }
  }
  return ret;
}

int ObTenantFileManager::list_tablet_data_dir(ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  // TODO @xiaotao.ht 把所有tablet都list出来，比较占用内存，之后实现流式接口
  hash::ObHashSet<int64_t> tablet_id_set;
  lib::ObMemAttr attr(tenant_id_, "ListTabletDa");
  char tablet_data_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObSingleNumFileListOp tablet_data_dir_op;
  tablet_data_dir_op.set_dir_flag();
  // step 1: list tablet id in local dir
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(tablet_id_set.create(OB_DEFAULT_BUCKET_NUM, attr))) {
    LOG_WARN("create tablet id set failed", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tablet_data_dir(tablet_data_dir_path,
                     sizeof(tablet_data_dir_path), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get local tablet data dir", KR(ret), K(tablet_data_dir_path), K_(tenant_id),
             "epoch_id", MTL_EPOCH_ID());
  } else if (OB_FAIL(list_local_scatter_files(tablet_data_dir_path,
                                              ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM,
                                              "%02ld", tablet_data_dir_op))) {
    LOG_WARN("fail to list local scatter files", KR(ret), K(tablet_data_dir_path));
  } else {
    const int64_t file_cnt = tablet_data_dir_op.file_list_.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < file_cnt); i++) {
      if (OB_FAIL(tablet_id_set.set_refactored(tablet_data_dir_op.file_list_.at(i)))) {
        LOG_WARN("fail to set refactored", KR(ret), "file_id", tablet_data_dir_op.file_list_.at(i));
      }
    }
  }

  ObSingleNumFileListOp remote_tablet_data_dir_op;
  remote_tablet_data_dir_op.set_dir_flag();
  tablet_data_dir_path[0] = '\0';
  // step 2: list tablet id in remote dir
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tablet_data_dir(tablet_data_dir_path,
                     sizeof(tablet_data_dir_path), tenant_id_, MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get shared tablet meta dir", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(list_remote_files(tablet_data_dir_path, remote_tablet_data_dir_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(tablet_data_dir_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_tablet_data_dir_op.file_list_.count(); i++) {
      if (OB_FAIL(tablet_id_set.set_refactored(remote_tablet_data_dir_op.file_list_.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(remote_tablet_data_dir_op.file_list_.at(i)));
      }
    }
  }
  hash::ObHashSet<int64_t>::const_iterator iter;
  for (iter = tablet_id_set.begin(); OB_SUCC(ret) && iter != tablet_id_set.end(); ++iter) {
    if (OB_FAIL(tablet_ids.push_back(iter->first))) {
      LOG_WARN("fail to push back", KR(ret), K(iter->first));
    }
  }
  return ret;
}

int ObTenantFileManager::list_shared_tablet_ids(ObIArray<int64_t> &tablet_ids)
{
  return OB_SUCCESS; // FIXME: remove this interface
}

int ObTenantFileManager::list_private_macro_file(const int64_t tablet_id,
                                                 const int64_t private_macro_third_id,
                                                 ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<MacroBlockId> block_id_set;
  lib::ObMemAttr attr(tenant_id_, "ListPriMacro");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(tablet_id <= 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(private_macro_third_id));
  } else if (OB_FAIL(block_id_set.create(OB_DEFAULT_BUCKET_NUM, attr))){
    LOG_WARN("create block id set failed", KR(ret));
  }
  ObSEArray<MacroBlockId, OB_DEFAULT_ARRAY_CAPACITY> listed_block_ids;
  listed_block_ids.set_attr(ObMemAttr(tenant_id_, "ListPriMacro"));
  // step 1: list private data macro file in local dir
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(list_local_private_macro_file(tablet_id, private_macro_third_id, ObMacroType::DATA_MACRO, listed_block_ids))) {
    LOG_WARN("fail to list local private data macro dir", KR(ret), K(tablet_id), K(private_macro_third_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < listed_block_ids.count(); i++) {
      if (OB_FAIL(block_id_set.set_refactored(listed_block_ids.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(listed_block_ids.at(i)));
      }
    }
  }
  // step 2: list private meta macro file in local dir
  listed_block_ids.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(list_local_private_macro_file(tablet_id, private_macro_third_id, ObMacroType::META_MACRO, listed_block_ids))) {
    LOG_WARN("fail to list local private meta macro dir", KR(ret), K(tablet_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < listed_block_ids.count(); i++) {
      if (OB_FAIL(block_id_set.set_refactored(listed_block_ids.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(listed_block_ids.at(i)));
      }
    }
  }
  // step 3: list private data macro file in remote dir
  listed_block_ids.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(list_remote_private_macro_file(tablet_id, private_macro_third_id, ObMacroType::DATA_MACRO, listed_block_ids))) {
    LOG_WARN("fail to list remote private data macro dir", KR(ret), K(tablet_id), K(private_macro_third_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < listed_block_ids.count(); i++) {
      if (OB_FAIL(block_id_set.set_refactored(listed_block_ids.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(listed_block_ids.at(i)));
      }
    }
  }
  // step 4: list private meta macro file in remote dir
  listed_block_ids.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(list_remote_private_macro_file(tablet_id, private_macro_third_id, ObMacroType::META_MACRO, listed_block_ids))) {
    LOG_WARN("fail to list remote private meta macro dir", KR(ret), K(tablet_id), K(private_macro_third_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < listed_block_ids.count(); i++) {
      if (OB_FAIL(block_id_set.set_refactored(listed_block_ids.at(i)))) {
        LOG_WARN("add tmp file id to set failed", KR(ret), K(listed_block_ids.at(i)));
      }
    }
  }
  hash::ObHashSet<MacroBlockId>::const_iterator iter;
  for (iter = block_id_set.begin(); OB_SUCC(ret) && iter != block_id_set.end(); ++iter) {;
    if (OB_FAIL(block_ids.push_back(iter->first))) {
      LOG_WARN("fail to push back", KR(ret), K(iter->first));
    }
  }
  return ret;
}

int ObTenantFileManager::list_local_private_macro_file(const int64_t tablet_id, const int64_t private_macro_third_id,
                                                       const ObMacroType macro_type,
                                                       ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  char private_macro_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  ObPrivateMacroListOp local_private_macro_op(macro_type);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(tablet_id <= 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(private_macro_third_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tablet_id_macro_dir(private_macro_path,
                     sizeof(private_macro_path), tenant_id_, MTL_EPOCH_ID(), tablet_id, private_macro_third_id, macro_type))) {
    LOG_WARN("fail to get tablet macro dir", KR(ret), K_(tenant_id), K(tablet_id), K(private_macro_third_id), K(macro_type));
  } else if (OB_FAIL(list_local_files(private_macro_path, local_private_macro_op))) {
    LOG_WARN("fail to list local files", KR(ret), K(private_macro_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < local_private_macro_op.file_list_.count(); i++) {
      MacroBlockId file_id;
      if (ObMacroType::DATA_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
      } else if (ObMacroType::META_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_META_MACRO);
      }
      file_id.set_second_id(tablet_id);
      file_id.set_third_id(private_macro_third_id);
      file_id.set_op_id(local_private_macro_op.file_list_.at(i).first);
      file_id.set_tenant_seq(local_private_macro_op.file_list_.at(i).second);
      if (OB_FAIL(block_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
  }
  return ret;
}

int ObTenantFileManager::list_remote_private_macro_file(const int64_t tablet_id, const int64_t private_macro_third_id,
                                                        const ObMacroType macro_type, ObIArray<MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObPrivateMacroListOp remote_private_macro_op(macro_type);
  char private_macro_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(tablet_id <= 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(private_macro_third_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tablet_id_macro_dir(
             private_macro_path, sizeof(private_macro_path), tenant_id_, MTL_EPOCH_ID(), tablet_id, private_macro_third_id, macro_type))) {
    LOG_WARN("fail to get remote tablet macro dir", KR(ret), K(tablet_id), K(private_macro_third_id), K_(tenant_id), K(macro_type));
  } else if (OB_FAIL(list_remote_files(private_macro_path, remote_private_macro_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(private_macro_path));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < remote_private_macro_op.file_list_.count(); i++) {
      MacroBlockId file_id;
      if (ObMacroType::DATA_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
      } else if (ObMacroType::META_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_META_MACRO);
      }
      file_id.set_second_id(tablet_id);
      file_id.set_third_id(private_macro_third_id);
      file_id.set_op_id(remote_private_macro_op.file_list_.at(i).first);
      file_id.set_tenant_seq(remote_private_macro_op.file_list_.at(i).second);
      if (OB_FAIL(block_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
  }
  return ret;
}

int ObTenantFileManager::try_delete_last_unsealed_segment(const TmpFileSegId &seg_id)
{
  int ret = OB_SUCCESS;
  bool is_meta_exist = false;
  TmpFileMetaHandle meta_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(segment_file_mgr_.try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to get seg meta", KR(ret), K(seg_id));
  } else if (!is_meta_exist) {
    // do nothing
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, meta handle is invalid", KR(ret), K(meta_handle));
  } else if (!meta_handle.is_in_local()) {
    // the last segment file is unsealed and it is in remote, so need delete unsealed segment file
    if (OB_FAIL(segment_file_mgr_.push_seg_file_to_remove_queue(seg_id, meta_handle.get_valid_length()))) {
      LOG_WARN("fail to push seg file to remove queue", KR(ret), K(seg_id), K(meta_handle));
    }
  }
  return ret;
}

int ObTenantFileManager::schedule_calibrate_disk_space_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_FAIL(calibrate_disk_space_task_.schedule_calibrate_disk_space())) {
    LOG_WARN("fail to schedule calibrate disk space", KR(ret));
  }
  return ret;
}

int ObTenantFileManager::push_to_preread_queue(
    const MacroBlockId &macro_id,
    const ObTabletID &effective_tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_cache_mgr_.push_to_preread_queue(macro_id, effective_tablet_id))) {
    LOG_WARN("fail to push to preread queue", KR(ret), K(macro_id), K(effective_tablet_id));
  }
  return ret;
}

int ObTenantFileManager::remove_preread_node(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(preread_cache_mgr_.remove_preread_node(macro_id))) {
    LOG_WARN("fail to remove preread node", KR(ret), K(macro_id));
  }
  return ret;
}

int ObTenantFileManager::aio_write_with_create_parent_dir(
    ObStorageObjectWriteInfo &write_info,
    const ObStorageObjectHandle &ori_object_handle,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  MacroBlockId file_id = object_handle.get_macro_id();
  ObStorageObjectType object_type = file_id.storage_object_type();
  if (OB_FAIL(do_aio_write(write_info, object_handle))) {
    // No such file or directory, and !write_through, need create file parent dir
    if ((OB_NO_SUCH_FILE_OR_DIRECTORY == ret) && !write_info.io_desc_.is_write_through()) {
      ret = OB_SUCCESS;
      object_handle = ori_object_handle;
      if (OB_FAIL(ObFileHelper::create_file_parent_dir(
          file_id, write_info.get_ls_id(), write_info.get_ls_epoch_id()))) {
        LOG_WARN("fail to create file parent dir", KR(ret), "macro_id", file_id,
                "ls_epoch_id", write_info.get_ls_epoch_id());
      } else if (OB_FAIL(do_aio_write(write_info, object_handle))) {
        LOG_WARN("fail to do aio write", KR(ret), K(write_info), K(object_handle));
      }
    } else {
      LOG_WARN("fail to do aio write", KR(ret), K(write_info), K(object_handle));
    }
  }
  return ret;
}

int ObTenantFileManager::try_fsync_private_macro_parent_dir(
    const ObStorageObjectType object_type,
    const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  if (ObStorageObjectType::PRIVATE_TABLET_META == object_type) {
    if (OB_FAIL(fsync_private_tablet_dir(ObStorageObjectType::PRIVATE_DATA_MACRO,
                                         file_id.second_id(), file_id.third_id()))) {
      LOG_WARN("fail to fsync priate tablet dir", KR(ret), K(file_id));
    } else if (OB_FAIL(fsync_private_tablet_dir(ObStorageObjectType::PRIVATE_META_MACRO,
                                                file_id.second_id(), file_id.third_id()))) {
      LOG_WARN("fail to fsync priate tablet dir", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObTenantFileManager::fsync_private_tablet_dir(
    const ObStorageObjectType object_type,
    const int64_t tablet_id,
    const int64_t private_macro_third_id)
{
  int ret = OB_SUCCESS;
  // PRIVATE_DATA_MACRO and PRIVATE_META_MACRO are in the same dir, so need fsync both.
  if (OB_UNLIKELY(!(SSObjUtil::is_private(object_type) && SSObjUtil::is_macro(object_type)) || (tablet_id <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "storage object type", STI(object_type), K(tablet_id));
  } else {
    MacroBlockId mock_private_macro_id;
    mock_private_macro_id.set_storage_object_type(static_cast<uint64_t>(object_type));
    mock_private_macro_id.set_second_id(tablet_id);
    mock_private_macro_id.set_third_id(private_macro_third_id);
    char private_macro_dir[OB_MAX_FILE_PATH_LENGTH];
    bool is_private_macro_dir_exist = false;
    if (OB_FAIL(ObFileHelper::get_file_parent_dir(private_macro_dir, OB_MAX_FILE_PATH_LENGTH, mock_private_macro_id, 0/*ls_epoch_id*/))) {
      LOG_WARN("fail to get private macro parent dir", KR(ret), K(private_macro_dir));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::exist(private_macro_dir, is_private_macro_dir_exist))) {
      LOG_WARN("fail to exist", KR(ret), K(private_macro_dir));
    } else if (is_private_macro_dir_exist) {
      if (OB_FAIL(ObDirManager::fsync_dir(private_macro_dir))) {
        LOG_WARN("fail to fsync private macro parent dir", KR(ret), K(private_macro_dir));
      }
    }
  }
  return ret;
}

int ObTenantFileManager::rename_local_tmp_file_to_deleted_(
    const char *old_path,
    const char *new_path)
{
  int ret = OB_SUCCESS;
  // Publish the active mutation and its epoch without holding a lock across filesystem I/O.
  ATOMIC_INC(&tmp_logical_delete_inflight_);
  ret = ObIODeviceLocalFileOp::rename(old_path, new_path);
  if (OB_SUCC(ret)) {
    ATOMIC_INC(&tmp_logical_delete_epoch_);
  }
  ATOMIC_DEC(&tmp_logical_delete_inflight_);
  return ret;
}

// logical delete local tmp file is renamed file to .deleted file
int ObTenantFileManager::logical_delete_local_tmp_file(
    const MacroBlockId &file_id,
    const bool is_del_seg_meta)
{
  int ret = OB_SUCCESS;
  ObPathContext old_path_ctx;
  ObPathContext new_path_ctx;
  TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_UNLIKELY(!file_id.is_valid() || (!SSObjUtil::is_tmp_file(file_id)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), K(file_id), K(is_del_seg_meta));
  } else if (OB_FAIL(old_path_ctx.set_file_ctx(
      file_id, 0/*ls_epoch_id*/, true/*is_local_cache*/, GCONF.observer_id))) {
    LOG_WARN("fail to convert file path", KR(ret), K(old_path_ctx));
  } else if (OB_FAIL(new_path_ctx.set_logical_delete_ctx(
      file_id, 0/*ls_epoch_id*/, GCONF.observer_id))) {
    LOG_WARN("fail to convert file path", KR(ret), K(new_path_ctx));
  } else if (OB_FAIL(rename_local_tmp_file_to_deleted_(
      old_path_ctx.get_path(), new_path_ctx.get_path()))) {
    // if report OB_NO_SUCH_FILE_OR_DIRECTORY, maybe the file has been renamed
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to rename", KR(ret), K(old_path_ctx), K(new_path_ctx));
    }
  // Note: logical delete local tmp file should erase macro cache meta, and physical delete
  // .deleted tmp file should not erase macro cache meta. please see the details in
  // ObSSObjectAccessUtil::inner_delete_local_file
  } else if (OB_FAIL(macro_cache_mgr->erase(file_id))) {
    LOG_WARN("fail to erase from macro cache mgr", KR(ret), K(file_id));
  } else {
    FLOG_INFO("succ to logical delete local tmp file", K(file_id), K(is_del_seg_meta));
    if (is_del_seg_meta) {
      // Note: segment_file_mgr_.delete_meta expect not to fail
      if (OB_FAIL(segment_file_mgr_.delete_meta(seg_id))) {
        LOG_ERROR("fail to delete seg meta and conflict info", KR(ret), K(file_id));
      } 
    }
  }
  return ret;
}

bool ObTenantFileManager::is_object_type_need_stat(const MacroBlockId &macro_id)
{
  return SSObjUtil::is_need_stat(macro_id);
}

int ObTenantFileManager::push_to_atomic_write_file_map(const bool is_clean, const blocksstable::MacroBlockId &file_id, ObAtomicWriteFileInfo &atomic_write_file)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id));
  } else {
    ObAtomicWriteFileInfoMap &target_map = get_atomic_write_file_map(is_clean);
    atomic_write_file.set_time_s(ObTimeUtility::current_time_s());
    if (OB_FAIL(target_map.set_refactored(file_id, atomic_write_file))) {
      LOG_WARN("fail to set to atomic write file map", KR(ret), K(file_id), K(atomic_write_file));
    }
  }
  return ret;
}

int ObTenantFileManager::erase_from_atomic_write_file_map(const bool is_clean, const blocksstable::MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id));
  } else {
    ObAtomicWriteFileInfoMap &target_map = get_atomic_write_file_map(is_clean);
    if (OB_FAIL(target_map.erase_refactored(file_id))) {
      LOG_WARN("fail to erase from atomic write file map", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObTenantFileManager::get_from_atomic_write_file_map(const bool is_clean, const blocksstable::MacroBlockId &file_id, ObAtomicWriteFileInfo &atomic_write_file)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id));
  } else {
    ObAtomicWriteFileInfoMap &target_map = get_atomic_write_file_map(is_clean);
    if (OB_FAIL(target_map.get_refactored(file_id, atomic_write_file))) {
      // no log because it is normal to not find the file in atomic write file map
    }
  }
  return ret;
}

ObAtomicWriteFileInfoMap &ObTenantFileManager::get_atomic_write_file_map(const bool is_clean)
{
  return is_clean ? atomic_write_file_clean_map_ : atomic_write_file_map_;
}

int ObTenantFileManager::do_tmp_seq_file_clean_work(const bool is_clean, const int64_t safe_del_time_s)
{
  int ret = OB_SUCCESS;
  ObSEArray<blocksstable::MacroBlockId, 8> deleted_file_ids;
  deleted_file_ids.set_attr(ObMemAttr(tenant_id_, "DeletedFileIds"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else {
    ObAtomicWriteFileInfoMap &target_map = get_atomic_write_file_map(is_clean);
    if (target_map.size() == 0) {
    } else if (OB_FAIL(deleted_file_ids.reserve(target_map.size()))) {
      LOG_WARN("fail to reserve", KR(ret), "size", target_map.size());
    } else {
      ObTmpSeqFileCleanFunc map_func(is_clean, safe_del_time_s, deleted_file_ids);
      if (OB_FAIL(target_map.foreach_refactored(map_func))) {
        LOG_WARN("fail to foreach atomic write file map", KR(ret));
      } else {
        int tmp_ret = OB_SUCCESS;
        for (int64_t j = 0; j < deleted_file_ids.count(); j++) {
          // erase from map, if failed, return the last error
          if (OB_TMP_FAIL(target_map.erase_refactored(deleted_file_ids.at(j)))) {
            LOG_WARN("fail to erase from atomic write file map", KR(tmp_ret), K(deleted_file_ids.at(j)));
            ret = tmp_ret;
          }
        }
      }
    }
  }
  return ret;
}

int ObTmpSeqFileCleanFunc::operator()(const ObAtomicWriteFileInfoMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tenant_file_manager = nullptr;
  if (OB_ISNULL(tenant_file_manager = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret));
  } else if (entry.second.time_s_ > (ObTimeUtility::current_time_s() - tmp_seq_file_del_secy_time_s_)) {
  } else if (OB_FAIL(tenant_file_manager->delete_tmp_seq_file(entry.first, entry.second))) {
    LOG_WARN("fail to delete tmp seq file", KR(ret), K(entry.first), K(entry.second));
  } else if (OB_FAIL(deleted_file_ids_.push_back(entry.first))) {
    LOG_WARN("fail to push back deleted file id", KR(ret), K(entry.first));
  }
  return ret;
}

/**
 * --------------------------------ObServerFileManager------------------------------------
 */
ObServerFileManager::ObServerFileManager()
  : ObBaseFileManager(),
    resize_device_size_lock_(ObLatchIds::FILE_MANAGER_LOCK),
    is_stopped_(false),
    io_calibration_fd_(OB_INVALID_FD),
    tg_id_(INVALID_TG_ID),
    flush_map_(false/*use_promise*/),
    server_slog_flush_task_(is_stopped_),
    check_data_disk_avail_task_()
{
}

ObServerFileManager::~ObServerFileManager()
{
  destroy();
}

ObServerFileManager &ObServerFileManager::get_instance()
{
  static ObServerFileManager instance_;
  return instance_;
}

int ObServerFileManager::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "SlogFlushMap");
  if (OB_FAIL(ObBaseFileManager::init(tenant_id))) {
    LOG_WARN("fail to init ObBaseFileManager", KR(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::ServerSlogFlushTimer, tg_id_))) {
    LOG_WARN("create thread group id failed", KR(ret));
  } else if (OB_FAIL(flush_map_.create(100/*bucket_num*/, attr))) {
    LOG_WARN("fail to create flush map", KR(ret));
  } else {
    LOG_INFO("succ to init server file manager", K_(tg_id));
  }
  return ret;
}

int ObServerFileManager::start(const int64_t reserved_size)
{
  int ret = OB_SUCCESS;
  int64_t total_disk_size = 0;
  ObIODOpts io_d_opts;
  ObIODOpt opt;
  io_d_opts.opt_cnt_ = 1;
  io_d_opts.opts_ = &(opt);
  opt.set("reserved size", reserved_size);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.start(io_d_opts))) {
    LOG_WARN("start local cache device fail", KR(ret));
  } else if (FALSE_IT(total_disk_size = LOCAL_DEVICE_INSTANCE.get_total_block_size())) {
  } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.init(total_disk_size))) {
    LOG_WARN("fail to init server disk space manager", KR(ret), K(total_disk_size));
  } else if (OB_FAIL(check_data_disk_avail_task_.init(lib::TGDefIDs::ServerGTimer))) {
    LOG_WARN("fail to init check data disk available task", KR(ret));
  } else if (OB_FAIL(restore_flushed_slog_seq())) {
    LOG_WARN("fail to restore flushed slog seq", KR(ret));
  } else if (OB_FAIL(create_io_detect_file())) {
    LOG_WARN("fail to create io detect file", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("init server slog flush timer fail", KR(ret));
  } else if (OB_FAIL(server_slog_flush_task_.start(tg_id_))) {
    LOG_WARN("fail to start server_slog_flush_task", KR(ret));
  }
  if (OB_SUCC(ret)) {
    is_started_ = true;
    LOG_INFO("start server file manager", K_(is_started), K(total_disk_size));
  }
  return ret;
}

void ObServerFileManager::stop()
{
  FLOG_INFO("start to stop server file manager");
  is_stopped_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  FLOG_INFO("finish to stop server file manager");
}

void ObServerFileManager::wait()
{
  FLOG_INFO("start to wait server file manager");
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  FLOG_INFO("finish to wait server file manager");
}

void ObServerFileManager::destroy()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start to destroy server file manager");
  is_stopped_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  flush_map_.destroy();
  if (io_calibration_fd_ != OB_INVALID_FD) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObSSFileUtil::close(io_calibration_fd_))) {
      LOG_ERROR("fail to close fd", KR(tmp_ret), K_(io_calibration_fd));
    }
    io_calibration_fd_ = OB_INVALID_FD;
  }
  close_io_detect_file();
  ObBaseFileManager::destroy();
  FLOG_INFO("finish to destroy server file manager");
}

int ObServerFileManager::resize_device_size(const int64_t new_device_size,
                                            const int64_t new_device_disk_percentage,
                                            const int64_t reserved_size,
                                            storage::ObServerSuperBlock &super_block)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(resize_device_size_lock_);
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(reserved_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(reserved_size));
  } else {
    const int64_t old_data_file_size = LOCAL_DEVICE_INSTANCE.get_total_block_size();
    ObIODOpts io_d_opts;
    ObIODOpt opts[3];
    opts[0].set("datafile_size", new_device_size);
    opts[1].set("datafile_disk_percentage", new_device_disk_percentage);
    opts[2].set("reserved_size", reserved_size);
    io_d_opts.opts_ = opts;
    io_d_opts.opt_cnt_ = 3;
    if (OB_FAIL(LOCAL_DEVICE_INSTANCE.reconfig(io_d_opts))) {
      LOG_WARN("fail to resize file", KR(ret), K(new_device_size));
    } else {
      const int64_t new_actual_file_size = LOCAL_DEVICE_INSTANCE.get_total_block_size();
      const int64_t new_macro_block_cnt = new_actual_file_size / OB_STORAGE_OBJECT_MGR.get_macro_block_size();
      if (old_data_file_size < new_actual_file_size) {
        super_block.body_.total_file_size_ = new_actual_file_size;
        super_block.body_.total_macro_block_count_ = new_macro_block_cnt;
        super_block.body_.modify_timestamp_ = ObTimeUtility::current_time();
        if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.resize(new_actual_file_size))) {
          LOG_WARN("fail to resize total disk size", KR(ret));
        } else {
          FLOG_INFO("succeed to resize data file", K(new_actual_file_size),
                    K(new_device_size), K(new_device_disk_percentage));
        }
      } else if (old_data_file_size == new_actual_file_size) {
        LOG_INFO("the data file size is not changed", K(old_data_file_size), K(new_actual_file_size));
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("new data file size is less than old data file size, not supported", KR(ret),
                  K(new_actual_file_size), K(old_data_file_size));
      }

      // super block may have format upgrade. Whatever body changed, reconstruct header for safe
      if (FAILEDx(super_block.construct_header())) {
        LOG_WARN("fail to construct header", K(ret));
      }
    }
  }
  return ret;
}

int ObServerFileManager::async_write_file(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle ori_object_handle(object_handle);  // when aio write failed, object_handle can reset
  ObStorageObjectWriteInfo new_write_info(write_info);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!new_write_info.is_valid() || !object_handle.get_macro_id().is_valid())
                         || new_write_info.io_desc_.is_write_through()) {
    // disable user layer to set write_through/back, only io layer can determine write_through/back
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_write_info), K(object_handle));
  } else {
    // defence for 500 tenant write
    if (SSObjUtil::server_tenant_can_have(object_handle.get_macro_id())) {
      if (OB_FAIL(aio_write_with_create_parent_dir(new_write_info, ori_object_handle, object_handle))) {
        LOG_WARN("fail to aio write with create parent dir", KR(ret), K(new_write_info), K(object_handle));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected server tenant io", KR(ret), "storage object type", STI(object_handle.get_macro_id().storage_object_type()),
                K(write_info), K(object_handle));
    }
  }
  return ret;
}

int ObServerFileManager::delete_local_tenant_dir(const uint64_t tenant_id,
                                                 const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  char tenant_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tenant_dir(tenant_dir_path, sizeof(tenant_dir_path),
                                                        tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(delete_local_dir(tenant_dir_path))) {
    LOG_WARN("fail to delete dir file", KR(ret), K(tenant_dir_path));
  } else {
    FLOG_INFO("succ to delete local tenant dir", K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

int ObServerFileManager::delete_remote_tenant_dir(const uint64_t tenant_id,
                                                  const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  char tenant_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tenant_dir(tenant_dir_path,
                     sizeof(tenant_dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get remote tenant dir", KR(ret), K(tenant_dir_path),
             K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(delete_remote_dir(tenant_dir_path))) {
    LOG_WARN("fail to delete remote dir", KR(ret), K(tenant_dir_path));
  } else {
    FLOG_INFO("succ to delete remote tenant dir", K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

int ObServerFileManager::delete_remote_server_dir(const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  char server_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(server_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_server_dir(server_dir_path,
                     sizeof(server_dir_path), server_id))) {
    LOG_WARN("fail to get remote server dir", KR(ret), K(server_dir_path), K(server_id));
  } else if (OB_FAIL(delete_remote_dir(server_dir_path))) {
    LOG_WARN("fail to delete remote dir", KR(ret), K(server_dir_path));
  } else {
    FLOG_INFO("succ to delete remote server dir", K(server_id));
  }
  return ret;
}

int ObServerFileManager::delete_shared_tenant_dir(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_tenant_dir(tenant_dir_path,
                     sizeof(tenant_dir_path), tenant_id))) {
    LOG_WARN("fail to get shared tenant dir", KR(ret), K(tenant_dir_path), K(tenant_id));
  } else if (OB_FAIL(delete_remote_dir(tenant_dir_path))) {
    LOG_WARN("fail to delete remote dir", KR(ret), K(tenant_dir_path));
  } else {
    FLOG_INFO("succ to delete shared tenant dir", K(tenant_id));
  }
  return ret;
}

int ObServerFileManager::delete_shared_ls_dir(
    const uint64_t tenant_id,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  char ls_dir_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(ls_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_shared_ls_dir(ls_dir_path,
                     sizeof(ls_dir_path), tenant_id, ls_id))) {
    LOG_WARN("fail to get shared ls dir", KR(ret), K(ls_dir_path), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(delete_remote_dir(ls_dir_path))) {
    LOG_WARN("fail to delete remote dir", KR(ret), K(ls_dir_path));
  } else {
    FLOG_INFO("succ to delete shared ls dir", K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObServerFileManager::list_shared_tenant_ids(const uint64_t tenant_id, ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret), K_(is_inited), K_(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObBackupIoAdapter adapter;
    ObBackupDest storage_dest;
    ObSharedTenantDirListOp shared_tenant_id_op(tenant_id);
    char cluster_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_cluster_dir(cluster_path, sizeof(cluster_path)))) {
      LOG_WARN("fail to get shared cluster dir", KR(ret), K(cluster_path));
    } else if (OB_FAIL(get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage info", KR(ret), K(storage_dest));
    } else if (OB_FAIL(adapter.list_directories(cluster_path, storage_dest.get_storage_info(), shared_tenant_id_op))) {
      LOG_WARN("fail to list files", KR(ret), K(cluster_path));
    } else if (OB_FAIL(shared_tenant_id_op.get_file_list(tenant_ids))) {
      LOG_WARN("fail to get file list", KR(ret));
    }
  }
  return ret;
}

int ObServerFileManager::list_server_ids(ObIArray<uint64_t> &server_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("file manager has not been inited", KR(ret));
  } else {
    ObBackupIoAdapter adapter;
    ObBackupDest storage_dest;
    ObServerDirListOp server_id_op;
    char cluster_path[OB_MAX_FILE_PATH_LENGTH] = {0};
    if (OB_FAIL(OB_DIR_MGR.get_cluster_dir(cluster_path, sizeof(cluster_path)))) {
      LOG_WARN("fail to get cluster dir", KR(ret), K(cluster_path));
    } else if (OB_FAIL(get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage dest", KR(ret), K(storage_dest));
    } else if (OB_FAIL(adapter.list_directories(cluster_path, storage_dest.get_storage_info(), server_id_op))) {
      LOG_WARN("fail to list files", KR(ret), K(cluster_path));
    } else if (OB_FAIL(server_id_op.get_server_ids(server_ids))) {
      LOG_WARN("fail to get server ids", KR(ret), K(server_id_op));
    }
  }
  return ret;
}

int ObServerFileManager::get_slog_used_size(const uint64_t tenant_id, const int64_t tenant_epoch_id, int64_t &used_size)
{
  int ret = OB_SUCCESS;
  used_size = 0;
  char slog_dir[OB_MAX_FILE_PATH_LENGTH] = {0};
  // step 1: get slog dir's file size in local cache
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_tenant_slog_dir(slog_dir, sizeof(slog_dir), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant slog dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else {
    ObSSGetSlogSizeFunctor functor(slog_dir);
    if (OB_FAIL(list_local_files(slog_dir, functor))) {
      LOG_WARN("fail to list local files", KR(ret), K(slog_dir));
    } else {
      used_size += functor.get_total_size();
    }
  }
  slog_dir[0] = '\0';
  ObObjectDirCalcSizeOp object_dir_calc_op;
  // step 2: get slog dir's file size in remote object storage
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(OB_DIR_MGR.get_remote_tenant_slog_dir(slog_dir,
                     sizeof(slog_dir), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get remote tenant slog dir", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(list_remote_files(slog_dir, object_dir_calc_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(slog_dir));
  } else {
    used_size += object_dir_calc_op.get_total_size();
  }
  return ret;
}

int ObServerFileManager::check_disk_space_available_(const bool need_report_user_error) const
{
  int ret = OB_SUCCESS;
  int64_t used_disk_size = 0;
  const double DIFF_RATIO = 0.01; // 1%
  const int64_t MAX_DEVIATION_DISK_SIZE = 5L * 1024L * 1024L * 1024L; // 5GB
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_FAIL(ObDiskSpaceManager::get_used_disk_size(used_disk_size))) {
    LOG_WARN("fail to get used disk size", KR(ret), K(used_disk_size));
  } else {
    const int64_t total_disk_size = OB_SERVER_DISK_SPACE_MGR.get_total_disk_size();
    // Adjusted used disk size with tolerance for concurrent write operations
    // that may cause inaccuracies in disk usage statistics
    const int64_t adjusted_used_disk_size = 
        used_disk_size + MIN(static_cast<double>(used_disk_size) * DIFF_RATIO, MAX_DEVIATION_DISK_SIZE);
    // In SN mode, reserved_disk_size is not part of data_file_size,
    // but in SS mode, reserved_disk_size is included in total_disk_size
    // so pass 0 as reserved_disk_size
    if (OB_FAIL(ObIODeviceLocalFileOp::check_disk_space_available(
        OB_DIR_MGR.get_local_cache_root_dir(), total_disk_size, 0/*reserved_disk_size*/,
        adjusted_used_disk_size, need_report_user_error))) {
      LOG_WARN("fail to check disk space available", KR(ret), K(total_disk_size),
          K(used_disk_size), K(adjusted_used_disk_size), K(need_report_user_error));
    }
  }
  return ret;
}

// Two-phase disk space check to avoid false positives caused by concurrent write operations.
// The first check uses adjusted disk usage with tolerance, and if it fails, performs a second
// check to confirm the actual disk space availability. This is especially important for IO
// calibration files which can consume up to 20GB and cause significant errors in used size
// statistics during concurrent operations.
int ObServerFileManager::check_disk_space_available()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_FAIL(check_disk_space_available_(false/*report user error*/))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(check_disk_space_available_(true/*report user error*/))) {
        LOG_WARN("fail to check disk space available", KR(ret));
      }
    } else {
      LOG_WARN("fail to check disk space available", KR(ret));
    }
  }
  return ret;
}

int ObServerFileManager::aio_write_with_create_parent_dir(
    ObStorageObjectWriteInfo &write_info,
    const ObStorageObjectHandle &ori_object_handle,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = object_handle.get_macro_id().storage_object_type();
  if (OB_FAIL(do_aio_write(write_info, object_handle))) {
    // No such file or directory, need create file parent dir
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      object_handle = ori_object_handle;
      if (OB_FAIL(ObFileHelper::create_file_parent_dir(
          object_handle.get_macro_id(), write_info.get_ls_id(), write_info.get_ls_epoch_id()))) {
        LOG_WARN("fail to create file parent dir", KR(ret), "macro_id", object_handle.get_macro_id(),
                "ls_epoch_id", write_info.get_ls_epoch_id());
      } else if (OB_FAIL(do_aio_write(write_info, object_handle))) {
        LOG_WARN("fail to do aio write", KR(ret), K(write_info), K(object_handle));
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to do aio write", KR(ret), K(object_handle), K(write_info));
    }
  }
  return ret;
}

int ObServerFileManager::create_io_calibration_file(const int64_t block_count)
{
  int ret = OB_SUCCESS;
  char io_calibration_file_path[OB_MAX_FILE_PATH_LENGTH] = { 0 };
  int open_flag = O_CREAT | O_EXCL | O_DIRECT | O_RDWR | O_LARGEFILE;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(block_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(block_count));
  } else {
    const int64_t alloc_file_size = block_count * DEFAULT_MACRO_BLOCK_SIZE;
    if (OB_UNLIKELY(io_calibration_fd_ >= 0)) {
      ret = OB_FILE_ALREADY_EXIST;
      LOG_WARN("io calibration file already exist", KR(ret), K_(io_calibration_fd));
    // must delete io calibration file before create io calibration file,
    // because the observer crushes after creating io_calibration_file, and has not deleted io_calibration_file
    } else if (OB_FAIL(delete_io_calibration_file())) {
      LOG_WARN("fail to delete io calibration file", KR(ret));
    } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.alloc(alloc_file_size))) {
      LOG_WARN("fail to alloc io_calibration_file size", KR(ret), K(alloc_file_size));
    } else {
      // The io_calibration_blk_cnt_ is used to represent the size of
      // the cloud disk cache occupied by io_calibration_file.
      // Therefore, space must be preallocated for io_calibration_file before its creation.
      // Once the OB_SERVER_DISK_SPACE_MGR successfully allocates space,
      // it becomes occupied by io_calibration_file,
      // and remains so until io_calibration_file is deleted.
      if (OB_FAIL(databuff_printf(io_calibration_file_path, sizeof(io_calibration_file_path),
                                  "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(), IO_CALIBRATION_FILE))) {
        LOG_WARN("fail to construct io_calibration_file_path", KR(ret));
      } else if (OB_FAIL(ObSSFileUtil::open(io_calibration_file_path, open_flag,
                         S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH, io_calibration_fd_))) {
        LOG_WARN("fail to open io_calibration_file", KR(ret), K(io_calibration_file_path));
      } else if (0 != ::fallocate(io_calibration_fd_, 0/*mode*/, 0/*offset*/, alloc_file_size)) {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        LOG_WARN("fail to fallocate io_calibration_file",
            KR(ret), K(io_calibration_file_path), K(alloc_file_size), KERRMSG);
      }
    }

    if (OB_FAIL(ret) && io_calibration_fd_ >= 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSFileUtil::close(io_calibration_fd_))) {
        LOG_ERROR("fail to close fd", KR(tmp_ret), K_(io_calibration_fd));
      } else {
        io_calibration_fd_ = OB_INVALID_FD;
      }
    }
  }
  return ret;
}

int ObServerFileManager::delete_io_calibration_file()
{
  int ret = OB_SUCCESS;
  char io_calibration_file_path[OB_MAX_FILE_PATH_LENGTH] = {0};
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else {
    if (io_calibration_fd_ >= 0) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSFileUtil::close(io_calibration_fd_))) {
        LOG_ERROR("fail to close fd", KR(tmp_ret), K_(io_calibration_fd));
      } else {
        io_calibration_fd_ = OB_INVALID_FD;
      }
    }
    ObIODFileStat statbuf;
    if (FAILEDx(databuff_printf(io_calibration_file_path, sizeof(io_calibration_file_path),
                                "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(), IO_CALIBRATION_FILE))) {
      LOG_WARN("fail to construct io_calibration_file_path", KR(ret));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(io_calibration_file_path, statbuf))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get file length", KR(ret), K(io_calibration_file_path));
      }
    } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(io_calibration_file_path))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("fail to del io_calibration_file", KR(ret), K(io_calibration_file_path));
      }
    } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.free(statbuf.size_))) {
      LOG_ERROR("fail to free io_calibration_file size", KR(ret), K(statbuf.size_));
    }
  }
  return ret;
}

int ObServerFileManager::get_io_detect_file_path(char *path, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("server file manager not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > OB_MAX_FILE_PATH_LENGTH))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(path), K(length));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s", OB_DIR_MGR.get_local_cache_root_dir(),
                                     IO_DETECT_FILE_NAME))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

int ObServerFileManager::push_to_flush_map(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheFlushStatus flush_status(GET_FUNC_TYPE(), false/*is_flushing*/);
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file id", KR(ret), K(file_id));
  } else if (OB_FAIL(flush_map_.set_refactored(file_id, flush_status))) {
    LOG_WARN("fail to set refactored flush map", KR(ret), K(file_id), K(flush_status));
  }
  return ret;
}

int ObServerFileManager::erase_from_flush_map(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file id", KR(ret), K(file_id));
  } else if (OB_FAIL(flush_map_.erase_refactored(file_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to erase refactored flush map", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObServerFileManager::get_next_flush_entry(ObSSMacroCacheFlushInfo &flush_info, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (IS_NOT_INIT || OB_UNLIKELY(!is_started_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited), K_(is_started));
  } else if (OB_FAIL(flush_map_.get_next_flush_entry(flush_info))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;  // ignore ret
    } else {
      LOG_WARN("fail to get next flush entry", KR(ret));
    }
  } else {
    is_exist = true;
  }
  return ret;
}


ObMandatoryCalibrateGuard::ObMandatoryCalibrateGuard(volatile bool &is_mandatory_calibrate)
  : need_reset_flag_(is_mandatory_calibrate), is_mandatory_calibrate_(is_mandatory_calibrate) {}

ObMandatoryCalibrateGuard::~ObMandatoryCalibrateGuard()
{
  if (need_reset_flag_) {
    is_mandatory_calibrate_ = false;
  }
}

bool ObMandatoryCalibrateGuard::is_cur_calibrate_mandatory() const
{
  // need_reset_flag_ == true means is_mandatory_calibrate == true. see construction function
  return need_reset_flag_;
}

} // namespace storage
} // namespace oceanbase
