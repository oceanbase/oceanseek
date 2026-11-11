/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <string.h>
#include "storage/shared_storage/ob_dir_manager.h"
#include "common/ob_smart_call.h"
#include "common/ob_tablet_id.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "share/ob_ss_file_util.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

const char *get_ls_inner_tablet_name_(const int64_t tablet_id)
{
  ObTabletID id(tablet_id);
  if (id.is_ls_tx_ctx_tablet()) {
    return "TX_CTX";
  } else if (id.is_ls_tx_data_tablet()) {
    return "TX_DATA";
  } else if (id.is_ls_lock_tablet()) {
    return "TX_LOCK";
  } else if (id.is_ls_reorg_info_tablet()) {
    return "REORG_INFO";
  } else {
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "unexpected tablet_id", K(tablet_id));
    return "UNEXPECTED";
  }
}

ObDirManager::ObDirManager()
{
  object_storage_root_dir_[0] = '\0';
}

ObDirManager::~ObDirManager()
{
  object_storage_root_dir_[0] = '\0';
}

ObDirManager &ObDirManager::get_instance()
{
  static ObDirManager instance_;
  return instance_;
}

int ObDirManager::set_object_storage_root_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(databuff_printf(object_storage_root_dir_, sizeof(object_storage_root_dir_), "%s", dir_path))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(dir_path));
  } else {
    LOG_INFO("succ to set object storage root dir", K_(object_storage_root_dir));
  }
  return ret;
}

int ObDirManager::fsync_dir(const char *dir_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dir path is null", KR(ret));
  } else {
    // 1. open
    int fd = INVALID_FD;
    RETRY_ON_EINTR(fd, ::open(dir_path, O_DIRECTORY | O_RDONLY));

    // 2. fsync
    if (OB_UNLIKELY(fd < 0)) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_WARN("::open failed", KR(ret), K(dir_path), K(errno), KERRMSG);
    } else if (OB_UNLIKELY(0 != ::fsync(fd))) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_WARN("::fsync failed", KR(ret), K(dir_path), K(errno), KERRMSG);
    }

    // 3. close
    if (OB_LIKELY(fd >= 0)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ObSSFileUtil::close(fd))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_ERROR("fail to close fd", KR(ret), KR(tmp_ret), K(fd));
      }
    }
  }
  return ret;
}

// 1. to avoid successful create dir, but failed alloc file size (this may lead to disk space leak).
//    we should alloc size firstly, and create dir secondly.
//    we suppose ::mkdir and ::fsync_dir rarely fail.
// 2. dir size is varient in different file systems (even in the same file system). thus, we
//    alloc 4KB for dir size firstly, and free dir size (which is alloc more) after create dir.
//    we suppose ::stat and free_file_size rarely fail.

int ObDirManager::create_dir(const char *dir_path, const ObStorageObjectType object_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  MacroBlockId dir_macro_id;
  dir_macro_id.set_storage_object_type(static_cast<uint64_t>(object_type));
  dir_macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  // Note: 500 tenant write PRIVATE_SLOG_FILE, has no ObTenantFileManager and ObSSMacroCacheMgr
  const bool need_alloc_file_size = (OB_NOT_NULL(file_mgr) && OB_NOT_NULL(macro_cache_mgr) &&
                                     file_mgr->is_object_type_need_stat(dir_macro_id));
  const mode_t mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;
  // step 1: alloc size for dir with 4KB
  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (need_alloc_file_size) {
    if (OB_FAIL(macro_cache_mgr->alloc_file_size(
                                 object_type,
                                 ObTabletID::INVALID_TABLET_ID/*effective_tablet_id*/,
                                 false/*use_effective_tablet_id*/,
                                 ObDirManager::DEFAULT_DIR_SIZE,
                                 ObDiskSpaceType::DIR))) {
      LOG_WARN("fail to alloc file size", KR(ret), "storage object type", STI(object_type),
               "alloc_size", ObDirManager::DEFAULT_DIR_SIZE);
    }
  }
  // step 2: create directory
  if (OB_SUCC(ret)) {
    bool need_free_file_size = false;
    // 仅本次创建的目录允许失败回滚，非本次创建或未创建目录时，只释放本次预留统计。已存在目录后续fsync失败时不能删除。
    bool need_rmdir = false;
    if (OB_FAIL(ObIODeviceLocalFileOp::mkdir_exclusively(dir_path, mode))) {
      if (OB_FILE_OR_DIRECTORY_EXIST == ret) { // already created by others
        ret = OB_SUCCESS; // ignore ret
        need_free_file_size = need_alloc_file_size; // free file size if alloc_file_size above
      } else {
        LOG_WARN("fail to mkdir exclusively", KR(ret), K(dir_path));
      }
    } else {
      need_rmdir = true;
    }
    if (FAILEDx(ObDirManager::fsync_dir(dir_path))) {
      LOG_WARN("fail to fsync dir", KR(ret), K(dir_path));
    }

    // if mkdir or fsync_dir failed, then try rmdir
    if (OB_FAIL(ret)) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        // has not create dir, and thus do not rmdir for OB_NO_SUCH_FILE_OR_DIRECTORY.
        // otherwise may wrongly delete dir, which is created by other thread.
        // but still need to free_file_size if alloc_file_size above.
        need_free_file_size = need_alloc_file_size;
      } else if (need_rmdir) { // OB_NO_SUCH_FILE_OR_DIRECTORY != ret
        // 仅回滚本次创建的目录；确认目录已删除后才释放统计。
        tmp_ret = ObIODeviceLocalFileOp::rmdir(dir_path);
        if (OB_SUCCESS == tmp_ret || OB_NO_SUCH_FILE_OR_DIRECTORY == tmp_ret) {
          need_free_file_size = need_alloc_file_size;
        } else {
          LOG_WARN("fail to rmdir, dir may persist on disk, keep alloc", KR(tmp_ret), K(dir_path));
        }
      } else {
        // 非本次创建的路径不能删除，只释放本次预留统计。
        need_free_file_size = need_alloc_file_size;
      }
    }

    // try free_file_size
    if (need_free_file_size) {
      if (OB_TMP_FAIL(macro_cache_mgr->free_file_size(
                                       object_type,
                                       ObTabletID::INVALID_TABLET_ID/*effective_tablet_id*/,
                                       false/*use_effective_tablet_id*/,
                                       ObDirManager::DEFAULT_DIR_SIZE,
                                       ObDiskSpaceType::DIR))) {
        LOG_WARN("fail to free file size", KR(tmp_ret), "storage object type", STI(object_type),
              "free_size", ObDirManager::DEFAULT_DIR_SIZE);
      }
    }
  }
  return ret;
}

int ObDirManager::create_dir_and_fsync_parent_dir(
    const char *dir_path,
    const ObStorageObjectType object_type)
{
  int ret = OB_SUCCESS;
  // 1. create dir
  if (OB_ISNULL(dir_path) || OB_UNLIKELY(STRLEN(dir_path) <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_FAIL(create_dir(dir_path, object_type))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path), "storage object type", STI(object_type));
  } else {
    // 2. get parent dir, and fsync parent dir
    const char *last_level_path = nullptr;
    if (dir_path[STRLEN(dir_path) - 1] == '/') { // case 1: dir_path ends with '/', e.g., "/a/b/c/"
      last_level_path = ObString(dir_path).reverse_find('/', 2);
    } else { // case 2: dir_path does not end with '/', e.g., "/a/b/c"
      last_level_path = ObString(dir_path).reverse_find('/', 1);
    }
    if (OB_ISNULL(last_level_path)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get last level path", KR(ret), K(dir_path));
    } else {
      const int64_t parent_dir_len = STRLEN(dir_path) - STRLEN(last_level_path);
      if (OB_UNLIKELY((parent_dir_len <= 0) || (parent_dir_len >= common::MAX_PATH_SIZE))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected parent dir length", KR(ret), K(parent_dir_len), K(dir_path));
      } else {
        char parent_dir_path[common::MAX_PATH_SIZE] = {0};
        MEMCPY(parent_dir_path, dir_path, parent_dir_len);
        parent_dir_path[parent_dir_len] = '\0';
        if (OB_FAIL(fsync_dir(parent_dir_path))) {
          LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
        }
      }
    }
  }
  return ret;
}

int ObDirManager::create_scatter_dir(
    const char *dir_path,
    const int64_t scatter_num,
    const char *scatter_format,
    const ObStorageObjectType object_type)
{
  int ret = OB_SUCCESS;
  char scatter_dir_format[common::MAX_PATH_SIZE] = {0};
  if (OB_ISNULL(dir_path) || OB_UNLIKELY(scatter_num <= 0) || OB_ISNULL(scatter_format)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(dir_path), K(scatter_num), KP(scatter_format));
  } else if (OB_FAIL(databuff_printf(scatter_dir_format, sizeof(scatter_dir_format),
                                     "%%s/%s", scatter_format))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(scatter_format));
  } else {
    // 1. create scatter dirs
    char scatter_dir_path[common::MAX_PATH_SIZE] = {0};
    for (int64_t i = 0; OB_SUCC(ret) && (i < scatter_num); ++i) {
      scatter_dir_path[0] = '\0';
      if (OB_FAIL(databuff_printf(scatter_dir_path, sizeof(scatter_dir_path),
                                  scatter_dir_format, dir_path, i))) {
        LOG_WARN("fail to databuff printf", KR(ret), K(scatter_dir_format), K(dir_path), K(i));
      } else if (OB_FAIL(ObDirManager::create_dir(scatter_dir_path, object_type))) {
        LOG_WARN("fail to create dir", KR(ret), K(scatter_dir_path), "storage object type", STI(object_type));
      }
    }
    // 2. fsync parent dir
    if (FAILEDx(ObDirManager::fsync_dir(dir_path))) {
      LOG_WARN("fail to fsync dir", KR(ret), K(dir_path));
    }
  }
  return ret;
}

int ObDirManager::delete_dir(const char *dir_path, const ObStorageObjectType object_type)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_mgr = nullptr;
  ObIODFileStat statbuf;
  ObStorageObjectType type = object_type;
  MacroBlockId dir_macro_id;
  dir_macro_id.set_storage_object_type(static_cast<uint64_t>(object_type));
  dir_macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dir_path));
  } else if (OB_ISNULL(file_mgr = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if ((SSObjUtil::is_tmp_file(object_type) && file_mgr->is_tmp_file_cache_pause_gc()) ||
             (!SSObjUtil::is_tmp_file(object_type) && file_mgr->is_pause_gc())) {
    // for tmp_file use is_tmp_file_write_cache_pause_gc(), for other object use is_pause_gc()
    ret = OB_FILE_DELETE_FAILED;
    if (TC_REACH_TIME_INTERVAL(ObBaseFileManager::PRINT_LOG_INTERVAL)) {
      LOG_WARN("the tenant is calibrating data disk alloc size and pausing gc, so do not allow to delete directory",
               KR(ret), K(dir_path), "storage object type", STI(object_type));
    }
  } else if (file_mgr->is_object_type_need_stat(dir_macro_id)) { // 1. is_object_type_need_stat = true
    if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
      LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
    } else if (OB_FAIL(ObSSMacroCacheMgr::free_file_size(
                                          object_type,
                                          ObTabletID::INVALID_TABLET_ID/*effective_tablet_id*/,
                                          false/*use_effective_tablet_id*/,
                                          ObDirManager::DEFAULT_DIR_SIZE,
                                          ObDiskSpaceType::DIR))) {
      LOG_WARN("fail to free file size", KR(ret), "storage object type", STI(object_type), "size", ObDirManager::DEFAULT_DIR_SIZE);
    }
  } else { // 2. is_object_type_need_stat = false
    if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
      LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
    }
  }
  return ret;
}

int ObDirManager::delete_dir_rec(const char *path, int64_t &file_size, int64_t &dir_num)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is null", KR(ret));
  } else if (OB_ISNULL(dir = opendir(path))) {
    if (ENOENT == errno) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_WARN("dir does not exist", KR(ret), K(path), K(errno), KERRMSG);
    } else {
      ret = OB_FILE_NOT_OPENED;
      LOG_WARN("fail to open dir", KR(ret), K(path), K(errno), KERRMSG);
    }
  } else {
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    ObIODFileStat statbuf;
    while (((entry = readdir(dir)) != NULL) && OB_SUCC(ret)) {
      bool is_dir = false;
      MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if ((0 == strcmp(entry->d_name, ".")) || (0 == strcmp(entry->d_name, ".."))) {
        // do nothing
      } else if (OB_FAIL(databuff_printf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", path, entry->d_name))) {
        LOG_WARN("fail to databuff printf", KR(ret), K(current_file_path), K(path), K(entry->d_name));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(current_file_path, statbuf))) {
        LOG_WARN("fail to stat file", KR(ret), K(current_file_path), K(statbuf));
      } else if (S_ISDIR(statbuf.mode_)) { // 1. delete dir recursively
        if (OB_FAIL(SMART_CALL(delete_dir_rec(current_file_path, file_size, dir_num)))) {
          LOG_WARN("delete_dir_rec failed", KR(ret), K(entry->d_name), K(path));
        }
      } else if (S_ISREG(statbuf.mode_)) { // 2. delete regular file
        if (OB_FAIL(ObIODeviceLocalFileOp::unlink(current_file_path))) {
          LOG_WARN("unlink failed", KR(ret), K(current_file_path));
        } else {
          file_size += statbuf.size_;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpcted file in current dir", KR(ret), K(current_file_path), K(statbuf.mode_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("delete directory rec failed", KR(ret), K(path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(path))) {
    LOG_WARN("rmdir failed", KR(ret), K(path));
  } else {
    dir_num++;
  }

  if (NULL != dir) {
    closedir(dir);
    dir = nullptr;
  }
  return ret;
}

int ObDirManager::create_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char scatter_dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1: create tenant_id_epoch_id/ directory
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_local_tenant_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  // step 2: create tenant_id_epoch_id/ls/ directory
  // Note: ls/ is the entry dir for META_FILE calc_disk_space, it should not be counted in memory
  if (FAILEDx(get_ls_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get ls dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  // step 3.1: create tenant_id_epoch_id/tablet_data/ directory
  // Note: tablet_data/ is the entry dir for MACRO_BLOCK calc_disk_space, it should not be counted in memory
  if (FAILEDx(get_local_tablet_data_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 3.2: create tenant_id_epoch_id/tablet_data/scatter_id/ directory,
  // and fsync tenant_id_epoch_id/tablet_data/ directory
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM,
                                               "%02ld", ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to create scatter dir", KR(ret), K(dir_path));
  }

  // step 4: create tenant_id_epoch_id/tmp_data/ directory
  // Note: tmp_data/ is the entry dir for TMP_FILE calc_disk_space, it should not be counted in memory
  dir_path[0] = '\0';
  if (FAILEDx(get_local_tmp_data_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tmp data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  // step 5: create tenant_id_epoch_id/slog/ directory
  if (FAILEDx(get_tenant_slog_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant slog dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  // step 6.1: create tenant_id_epoch_id/shared_mini_macro_cache/ directory
  // Note: shared_mini_macro_cache/ is the entry dir for MACRO_BLOCK calc_disk_space, it should not be counted in memory
  dir_path[0] = '\0';
  if (FAILEDx(get_shared_macro_cache_dir(dir_path, sizeof(dir_path), tenant_id,
                                                tenant_epoch_id, ObSharedMacroType::MINI))) {
    LOG_WARN("fail to get major data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 6.2: create tenant_id_epoch_id/shared_mini_macro_cache/scatter_id/ directory
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
                                               "%02lX", ObStorageObjectType::SHARED_MINI_DATA_MACRO))) {
    LOG_WARN("fail to create scatter dir", KR(ret), K(dir_path));
  }
  // step 6.3: create tenant_id_epoch_id/shared_mini_macro_cache/ls directory
  // Note: shared_mini_macro_cache/ls/ is a subdir of entry dir, it will be counted in calc_disk_space
  if (FAILEDx(databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "%s", "/ls"))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::SHARED_MINI_DATA_MACRO))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 6.4: fsync tenant_id_epoch_id/shared_mini_macro_cache/
  if (FAILEDx(ObDirManager::fsync_dir(dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(dir_path));
  }

  // step 7.1: create tenant_id_epoch_id/shared_minor_macro_cache/ directory
  // Note: shared_minor_macro_cache/ is the entry dir for MACRO_BLOCK calc_disk_space, it should not be counted in memory
  dir_path[0] = '\0';
  if (FAILEDx(get_shared_macro_cache_dir(dir_path, sizeof(dir_path), tenant_id,
                                                tenant_epoch_id, ObSharedMacroType::MINOR))) {
    LOG_WARN("fail to get major data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 7.2: create tenant_id_epoch_id/shared_minor_macro_cache/scatter_id/ directory
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
                                               "%02lX", ObStorageObjectType::SHARED_MINOR_DATA_MACRO))) {
    LOG_WARN("fail to create scatter dir", KR(ret), K(dir_path));
  }
  // step 7.3: create tenant_id_epoch_id/shared_minor_macro_cache/ls directory
  // Note: shared_minor_macro_cache/ls/ is a subdir of entry dir, it will be counted in calc_disk_space
  if (FAILEDx(databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "%s", "/ls"))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::SHARED_MINOR_DATA_MACRO))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 7.4: fsync tenant_id_epoch_id/shared_minor_macro_cache/
  if (FAILEDx(ObDirManager::fsync_dir(dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(dir_path));
  }

  // step 8.1: create tenant_id_epoch_id/shared_major_macro_cache/ directory
  // Note: shared_major_macro_cache/ is the entry dir for MACRO_BLOCK calc_disk_space, it should not be counted in memory
  dir_path[0] = '\0';
  if (FAILEDx(get_shared_macro_cache_dir(dir_path, sizeof(dir_path), tenant_id,
                                                tenant_epoch_id, ObSharedMacroType::MAJOR))) {
    LOG_WARN("fail to get major data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 8.2: create tenant_id_epoch_id/shared_major_macro_cache/scatter_id/ directory,
  // and fsync tenant_id_epoch_id/shared_major_macro_cache/ directory
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
                                               "%02lX", ObStorageObjectType::SHARED_MAJOR_DATA_MACRO))) {
    LOG_WARN("fail to create scatter dir", KR(ret), K(dir_path));
  }

  // step 9.1: create tenant_id_epoch_id/shared_tablet_sub_meta_cache/ directory
  dir_path[0] = '\0';
  if (FAILEDx(get_shared_tablet_sub_meta_cache_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get shared tablet sub meta cache dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 9.2: create tenant_id_epoch_id/shared_tablet_sub_meta_cache/scatter_id/ directory
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
                                               "%02lX", ObStorageObjectType::SHARED_TABLET_SUB_META))) {
    LOG_WARN("fail to create scatter dir", KR(ret), K(dir_path));
  }
  // step 9.3: create tenant_id_epoch_id/shared_tablet_sub_meta_cache/ls directory
  if (FAILEDx(databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "%s", "/ls"))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 9.4: fsync tenant_id_epoch_id/shared_tablet_sub_meta_cache/
  if (FAILEDx(ObDirManager::fsync_dir(dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(dir_path));
  }

  // step 10.1: create tenant_id_epoch_id/external_table_file/ directory
  // Note: external_table_file/ is the entry dir for MACRO_BLOCK calc_disk_space, it should not be counted in memory
  dir_path[0] = '\0';
  if (FAILEDx(get_external_table_file_dir(
      dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get external table file dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 10.2: create tenant_id_epoch_id/external_table_file/scatter_id/ directory,
  // and fsync tenant_id_epoch_id/external_table_file/ directory
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, ObDirManager::EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM,
                                               "%02lX", ObStorageObjectType::EXTERNAL_TABLE_FILE))) {
    LOG_WARN("fail to create scatter dir", KR(ret), K(dir_path));
  }
  // External LOBs create only evictable local cache directories, not remote objects.
  dir_path[0] = '\0';
  if (FAILEDx(get_external_lob_cache_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("failed to get external lob cache directory", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("failed to create external lob cache directory", KR(ret), K(dir_path));
  }
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, EXTERNAL_LOB_SCATTER_DIR_NUM, "%02lX",
                                               ObStorageObjectType::SHARED_EXTERNAL_LOB_DATA_MACRO))) {
    LOG_WARN("failed to create external lob cache scatter directories", KR(ret), K(dir_path));
  }
  // step 11.1: create tenant_id_epoch_id/shared_inc_major_macro_cache/ directory
  // Note: shared_inc_major_macro_cache/ is the entry dir for MACRO_BLOCK calc_disk_space, it should not be counted in memory
  dir_path[0] = '\0';
  if (FAILEDx(get_shared_macro_cache_dir(dir_path, sizeof(dir_path), tenant_id,
                                                tenant_epoch_id, ObSharedMacroType::INC_MAJOR))) {
    LOG_WARN("fail to get inc major data dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }
  // step 11.2: create tenant_id_epoch_id/shared_inc_major_macro_cache/scatter_id/ directory,
  // and fsync tenant_id_epoch_id/shared_inc_major_macro_cache/ directory
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM,
                                               "%02lX", ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO))) {
    LOG_WARN("fail to create scatter dir", KR(ret), K(dir_path));
  }
  // setp 12: fsync parent dirs
  // fsync tenant_id_epoch_id/
  // fsync local_cache_root_dir
  parent_dir_path[0] = '\0';
  if (FAILEDx(get_local_tenant_dir(parent_dir_path, sizeof(parent_dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  } else if (FALSE_IT(parent_dir_path[0] = '\0')) {
  } else if (OB_FAIL(databuff_printf(parent_dir_path, sizeof(parent_dir_path), "%s", get_local_cache_root_dir()))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

// tenant_id_epoch_id/ls_id_epoch_id/
int ObDirManager::create_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                   const int64_t ls_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char scatter_dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1: create tenant_id_epoch_id/ls/ls_id_epoch_id/ directory
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_ls_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                   tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls id dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  // step 2: create tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/ directory
  if (FAILEDx(get_tablet_meta_dir(dir_path, sizeof(dir_path), tenant_id,
                                  tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get tablet meta dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  // step 3: create tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/ directory,
  // and fsync tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/ directory
  if (FAILEDx(ObDirManager::create_scatter_dir(dir_path, ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM,
                                               "%02ld", ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to create scatter dir", KR(ret), K(dir_path));
  }

  // step 4: fsync parent dirs
  // fsync tenant_id_epoch_id/ls/ls_id_epoch_id/ directory
  // fsync tenant_id_epoch_id/ls/
  parent_dir_path[0] = '\0';
  if (FAILEDx(get_ls_id_dir(parent_dir_path, sizeof(parent_dir_path), tenant_id,
                            tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls id dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  } else if (FALSE_IT(parent_dir_path[0] = '\0')) {
  } else if (OB_FAIL(get_ls_dir(parent_dir_path, sizeof(parent_dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get ls dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/
int ObDirManager::create_tablet_meta_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                   const int64_t ls_id, const int64_t ls_epoch_id,
                                                   const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || tablet_id <= 0 ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id), K(ls_id), K(ls_epoch_id), K(tablet_id));
  } else if (OB_FAIL(get_tablet_meta_tablet_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                       tenant_epoch_id, ls_id, ls_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet meta tablet id dir", KR(ret), K(ls_id),
             K(ls_epoch_id), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      LOG_WARN("fail to create dir", KR(ret), K(dir_path));
    } else if (OB_FAIL(create_ls_id_dir(tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
      LOG_WARN("fail to create ls id dir", K(ret));
    } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
      LOG_WARN("fail to create dir", KR(ret), K(dir_path));
    }
  }

  if (FAILEDx(get_tablet_meta_scatter_dir(parent_dir_path, sizeof(parent_dir_path), tenant_id,
                                          tenant_epoch_id, ls_id, ls_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet meta scatter dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id), K(tablet_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

int ObDirManager::create_tablet_meta_tablet_id_private_macro_third_id_dir(
    const uint64_t tenant_id, const int64_t tenant_epoch_id,
    const int64_t ls_id, const int64_t ls_epoch_id, const int64_t tablet_id,
    const int64_t private_macro_third_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY((ls_id < 0) || (ls_epoch_id < 0) || (tablet_id <= 0) ||
                  !is_valid_tenant_id(tenant_id) || (tenant_epoch_id < 0) ||
                  (private_macro_third_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id), K(ls_id),
             K(ls_epoch_id), K(tablet_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_tablet_meta_tablet_id_private_macro_third_id_dir(dir_path, sizeof(dir_path),
                     tenant_id, tenant_epoch_id, ls_id, ls_epoch_id, tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to get tablet meta tablet id dir", KR(ret), K(tenant_id), K(tenant_epoch_id),
             K(ls_id), K(ls_epoch_id), K(tablet_id), K(private_macro_third_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      LOG_WARN("fail to create dir", KR(ret), K(dir_path));
    } else if (OB_FAIL(create_tablet_meta_tablet_id_dir(tenant_id, tenant_epoch_id, ls_id, ls_epoch_id, tablet_id))) {
      LOG_WARN("fail to create tablet meta tablet id dir", K(ret), K(tenant_id), K(tenant_epoch_id),
               K(ls_id), K(ls_epoch_id), K(tablet_id));
    } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
      LOG_WARN("fail to create dir", KR(ret), K(dir_path));
    }
  }

  if (FAILEDx(get_tablet_meta_tablet_id_dir(parent_dir_path, sizeof(parent_dir_path), tenant_id,
                                            tenant_epoch_id, ls_id, ls_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet meta dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(ls_id),
             K(ls_epoch_id), K(tablet_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/
int ObDirManager::create_tablet_data_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                   const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tablet_id <= 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                   tenant_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet data tablet id dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  } else if (OB_FAIL(get_local_tablet_data_scatter_dir(parent_dir_path, sizeof(parent_dir_path),
                                                       tenant_id, tenant_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(tablet_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

int ObDirManager::create_tablet_data_tablet_id_private_macro_third_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                                const int64_t tablet_id, const int64_t private_macro_third_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1: create tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id/ directory
  if (OB_UNLIKELY(tablet_id <= 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_tablet_data_tablet_id_private_macro_third_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                                tenant_epoch_id, tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to get tablet data tablet id private macro third id dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    // PRIVATE_DATA_MACRO means cache type is macro cache
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      // if report error OB_NO_SUCH_FILE_OR_DIRECTORY, create tenant_id_epoch_id/tablet_data/tablet_id/ directory first
      if (OB_FAIL(create_tablet_data_tablet_id_dir(tenant_id, tenant_epoch_id, tablet_id))) {
        LOG_WARN("fail to create tablet data tablet id dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(tablet_id));
      } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
        LOG_WARN("fail to create dir", KR(ret), K(dir_path));
      }
    } else {
      LOG_WARN("fail to create dir", KR(ret), K(dir_path));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_data_tablet_id_dir(parent_dir_path, sizeof(parent_dir_path),
                                                   tenant_id, tenant_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet data tablet id dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }

  dir_path[0] = '\0';
  // step 2: create tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id/data directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_id_macro_dir(
                     dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id, tablet_id, private_macro_third_id, ObMacroType::DATA_MACRO))) {
    LOG_WARN("fail to get tablet id data macro dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  // step 3: create tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id/meta directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_id_macro_dir(
             dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id, tablet_id, private_macro_third_id, ObMacroType::META_MACRO))) {
    LOG_WARN("fail to get tablet id meta macro dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(ObDirManager::create_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to create dir", KR(ret), K(dir_path));
  }

  // step 4: fsync data/ and meta/ parent dir
  parent_dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_data_tablet_id_private_macro_third_id_dir(parent_dir_path,
                     sizeof(parent_dir_path), tenant_id, tenant_epoch_id, tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to get tablet data tablet id dir", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_WARN("fail to fsync parent dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}


// tenant_id_epoch_id/tmp_data/scatter_id/
// Why is_exist check is necessary:
// Without is_exist check, create_dir would:
//    - alloc_file_size(4KB) first
//    - mkdir fails if directory already exists
//    - rmdir may fail if directory is not empty
//    - Result: 4KB space leak
int ObDirManager::create_tmp_data_scatter_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                               const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  bool is_exist = false;
  if (OB_UNLIKELY(tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_local_tmp_data_scatter_dir(dir_path, sizeof(dir_path), tenant_id,
                                                     tenant_epoch_id, tmp_file_id))) {
    LOG_WARN("fail to get local tmp data scatter dir", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::exist(dir_path, is_exist))) {
    LOG_WARN("fail to check dir exist", KR(ret), K(dir_path));
  } else if (!is_exist) {
    // Directory doesn't exist, create it
    if (OB_FAIL(ObDirManager::create_dir_and_fsync_parent_dir(dir_path, ObStorageObjectType::TMP_FILE))) {
      LOG_WARN("fail to create dir and fsync parent dir", KR(ret), K(dir_path));
    }
  } else {
    // Directory already exists, do nothing
  }
  return ret;
}

// tenant_id_epoch_id/tmp_data/scatter_id/tmp_file_id/
int ObDirManager::create_tmp_data_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_local_tmp_file_dir(dir_path, sizeof(dir_path), tenant_id,
                                            tenant_epoch_id, tmp_file_id))) {
    LOG_WARN("fail to get local tmp file dir", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir_and_fsync_parent_dir(dir_path, ObStorageObjectType::TMP_FILE))) {
    LOG_WARN("fail to create dir and fsync parent dir", KR(ret), K(dir_path));
  }
  return ret;
}

// Create tmp file directory with scatter mechanism
// Step 1: Try to create tmp_file_id/ under scatter_id/
// Step 2: If OB_NO_SUCH_FILE_OR_DIRECTORY (scatter_id/ not exists), create scatter_id/ then retry
// Note: Under normal circumstances, scatter_id/ is already created, so we avoid unnecessary creation attempts
int ObDirManager::create_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                      const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(create_tmp_data_tmp_file_dir(tenant_id, tenant_epoch_id, tmp_file_id))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      // Parent directory (scatter_id/) does not exist, create it first
      if (OB_FAIL(create_tmp_data_scatter_dir(tenant_id, tenant_epoch_id, tmp_file_id))) {
        LOG_WARN("fail to create tmp data scatter dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(tmp_file_id));
      } else if (OB_FAIL(create_tmp_data_tmp_file_dir(tenant_id, tenant_epoch_id, tmp_file_id))) {
        LOG_WARN("fail to create tmp data tmp file dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(tmp_file_id));
      }
    } else {
      LOG_WARN("fail to create tmp data tmp file dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(tmp_file_id));
    }
  }
  return ret;
}

int ObDirManager::create_slog_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char parent_dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_tenant_slog_dir(dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get tenant slog dir", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir_and_fsync_parent_dir(dir_path, ObStorageObjectType::PRIVATE_SLOG_FILE))) {
    LOG_WARN("fail to create dir and fsync parent dir", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::create_shared_mini_ls_id_dir(
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id,
    const int64_t ls_id)
{
  return create_shared_macro_ls_id_dir(tenant_id, tenant_epoch_id, ls_id, ObSharedMacroType::MINI);
}

int ObDirManager::create_shared_minor_ls_id_dir(
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id,
    const int64_t ls_id)
{
  return create_shared_macro_ls_id_dir(tenant_id, tenant_epoch_id, ls_id, ObSharedMacroType::MINOR);
}

int ObDirManager::get_shared_tablet_sub_meta_cache_dir(char *path, const int64_t length,
    const uint64_t tenant_id, const int64_t tenant_epoch_id) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // tenant_id_epoch_id/shared_tablet_sub_meta_cache
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  (!is_valid_tenant_id(tenant_id)) || (tenant_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s",
              get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_TABLET_SUB_META_CACHE_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

int ObDirManager::get_shared_tablet_sub_meta_ls_id_dir(char *path, const int64_t length,
    const uint64_t tenant_id, const int64_t tenant_epoch_id, const int64_t ls_id) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // tenant_id_epoch_id/shared_tablet_sub_meta_cache/ls/ls_id
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  (!is_valid_tenant_id(tenant_id)) || (tenant_epoch_id < 0) || (ls_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id), K(ls_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%s/%ld",
              get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_TABLET_SUB_META_CACHE_DIR_STR, LS_DIR_STR, ls_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id), K(ls_id));
  }
  return ret;
}

int ObDirManager::create_shared_tablet_sub_meta_ls_id_dir(const uint64_t tenant_id,
    const int64_t tenant_epoch_id, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  // tenant_id_epoch_id/shared_tablet_sub_meta_cache/ls/ls_id
  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id)) || (tenant_epoch_id < 0) || (ls_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id), K(ls_id));
  } else if (OB_FAIL(get_shared_tablet_sub_meta_ls_id_dir(dir_path, sizeof(dir_path),
                                                          tenant_id, tenant_epoch_id, ls_id))) {
    LOG_WARN("fail to get shared tablet sub meta ls id dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(ls_id));
  } else if (OB_FAIL(ObDirManager::create_dir_and_fsync_parent_dir(dir_path))) {
    LOG_WARN("fail to create dir and fsync parent dir", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::get_shared_tablet_sub_meta_scatter_dir(char *path, const int64_t length,
    const uint64_t tenant_id, const int64_t tenant_epoch_id, const int64_t scatter_id) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // tenant_id_epoch_id/shared_tablet_sub_meta_cache/scatter_id
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  (!is_valid_tenant_id(tenant_id)) || (tenant_epoch_id < 0) || (scatter_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id), K(scatter_id));
  } else if (OB_FAIL(databuff_printf(path, length, pos, "%s/%lu_%ld/%s/%02lX",
              get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
              SHARED_TABLET_SUB_META_CACHE_DIR_STR, scatter_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id), K(scatter_id));
  }
  return ret;
}

int ObDirManager::create_shared_tablet_sub_meta_scatter_dir(const uint64_t tenant_id,
    const int64_t tenant_epoch_id, const int64_t scatter_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  // tenant_id_epoch_id/shared_tablet_sub_meta_cache/scatter_id
  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id)) || (tenant_epoch_id < 0) || (scatter_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id), K(scatter_id));
  } else if (OB_FAIL(get_shared_tablet_sub_meta_scatter_dir(dir_path, sizeof(dir_path),
              tenant_id, tenant_epoch_id, scatter_id))) {
    LOG_WARN("fail to get shared tablet sub meta scatter dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(scatter_id));
  } else if (OB_FAIL(ObDirManager::create_dir_and_fsync_parent_dir(dir_path))) {
    LOG_WARN("fail to create dir and fsync parent dir", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::create_shared_macro_ls_id_dir(
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id,
    const int64_t ls_id,
    const ObSharedMacroType type)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  // default to shared mini data macro, since the cache type is macro cache
  const blocksstable::ObStorageObjectType object_type = ObStorageObjectType::SHARED_MINI_DATA_MACRO;
  // tenant_id_epoch_id/shared_[mini|minor]_macro_cache/ls/ls_id
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (tenant_epoch_id < 0) || (ls_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_epoch_id), K(ls_id));
  } else if (OB_FAIL(get_shared_macro_cache_dir(dir_path, sizeof(dir_path), tenant_id,
                                                tenant_epoch_id, type))) {
    LOG_WARN("fail to get major data dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(type));
  } else if (OB_FAIL(databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "/ls/%ld", ls_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::create_dir_and_fsync_parent_dir(dir_path, object_type))) {
    LOG_WARN("fail to create dir and fsync parent dir", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                   const int64_t ls_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  // step 1:delete tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/ directory
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_tablet_meta_dir(dir_path, sizeof(dir_path),
                                   tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get tablet meta dir", KR(ret), K(ls_id), K(ls_epoch_id),
             K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  }
  // step 2:delete tenant_id_epoch_id/ls/ls_id_epoch_id/ directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_ls_id_dir(dir_path, sizeof(dir_path),
                                   tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get ls dir", KR(ret), K(ls_id), K(ls_epoch_id),
             K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(dir_path))) {
    LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tablet_meta_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                   const int64_t ls_id, const int64_t ls_epoch_id,
                                                   const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  // delete tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/ directory
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_tablet_meta_tablet_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                       tenant_epoch_id, ls_id, ls_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet meta tablet id dir", KR(ret), K(ls_id), K(ls_epoch_id), K(tablet_id),
             K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tablet_meta_tablet_id_private_macro_third_id_dir(
    const uint64_t tenant_id, const int64_t tenant_epoch_id,
    const int64_t ls_id, const int64_t ls_epoch_id,
    const int64_t tablet_id, const int64_t private_macro_third_id)
{
  int ret = OB_SUCCESS;
  // delete tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/private_macro_third_id directory
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(ls_id < 0 || ls_epoch_id < 0 || tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(ls_epoch_id), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_tablet_meta_tablet_id_private_macro_third_id_dir(dir_path, sizeof(dir_path),
                     tenant_id, tenant_epoch_id, ls_id, ls_epoch_id, tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to get tablet meta tablet id private macro third id dir", KR(ret), K(tenant_id),
             K(tenant_epoch_id), K(ls_id), K(ls_epoch_id), K(tablet_id), K(private_macro_third_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tablet_data_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                   const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  // delete tenant_id_epoch_id/tablet_data/scatter_id/tablet_id directory
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                                 tenant_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tablet_data_tablet_id_private_macro_third_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                                const int64_t tablet_id, const int64_t private_macro_third_id)
{
  int ret = OB_SUCCESS;
  // step 1: delete tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id/meta directory
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_local_tablet_id_macro_dir(
               dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id, tablet_id, private_macro_third_id, ObMacroType::META_MACRO))) {
    LOG_WARN("fail to get tablet meta macro dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }

  // step 2: delete tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id/data directory
  dir_path[0] = '\0';
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_local_tablet_id_macro_dir(
               dir_path, sizeof(dir_path), tenant_id, tenant_epoch_id, tablet_id, private_macro_third_id, ObMacroType::DATA_MACRO))) {
    LOG_WARN("fail to get tablet data macro dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }

  dir_path[0] = '\0';
  // step 3: delete tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id directory
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_data_tablet_id_private_macro_third_id_dir(dir_path, sizeof(dir_path), tenant_id,
                                                                tenant_epoch_id, tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to get tablet data dir", KR(ret), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::PRIVATE_DATA_MACRO))) {
    LOG_WARN("fail to delete directory", KR(ret), K(dir_path));
  }
  return ret;
}

int ObDirManager::delete_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                      const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  if (OB_UNLIKELY(tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(ObDirManager::get_local_tmp_file_dir(dir_path, sizeof(dir_path),
                                           tenant_id, tenant_epoch_id, tmp_file_id))) {
    LOG_WARN("fail to get local tmp file dir", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(delete_dir(dir_path, ObStorageObjectType::TMP_FILE))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
    } else if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      // due to the concurrency of tmp file preread and tmp file delete, maybe .tmp files exist in tmp_file_dir when delete tmp_file_dir
      // .tmp files will be gc by timertask
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to rmdir", KR(ret), K(dir_path));
    }
  }
  return ret;
}

// cluster_id/
int ObDirManager::get_cluster_dir(char *path, const int64_t length)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld",
                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(cluster_id));
  }
  return ret;
}

// tenant_id_epoch_id/
int ObDirManager::get_local_tenant_dir(char *path, const int64_t length,
                                       const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/
int ObDirManager::get_remote_tenant_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                        const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/
int ObDirManager::get_remote_server_dir(char *path, const int64_t length, const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || server_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(server_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, SERVER_DIR_STR, server_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(server_id));
  }
  return ret;
}

// cluster_id/tenant_id/
int ObDirManager::get_shared_tenant_dir(char *path, const int64_t length, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu",
                     object_storage_root_dir, CLUSTER_DIR_STR,
                     cluster_id, TENANT_DIR_STR, tenant_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(cluster_id));
  }
  return ret;
}

// cluster_id/tenant_id/ls/
int ObDirManager::get_shared_ls_dir(
    char *path, 
    const int64_t length, 
    const uint64_t tenant_id,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld",
                     object_storage_root_dir, CLUSTER_DIR_STR,
                     cluster_id, TENANT_DIR_STR, tenant_id, LS_DIR_STR, ls_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(cluster_id), K(tenant_id));
  }
  return ret;
}

// tenant_id_epoch_id/slog/
int ObDirManager::get_tenant_slog_dir(
    char *path,
    const int64_t length,
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_SERVER_TENANT_ID == tenant_id) {
    if (OB_FAIL(databuff_printf(path, length, "%s/server_%s",
                                get_local_cache_root_dir(), SLOG_STR))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, SLOG_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/slog/ or cluster_id/server_id/server_slog/
int ObDirManager::get_remote_tenant_slog_dir(
    char *path,
    const int64_t length,
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_SERVER_TENANT_ID == tenant_id) {
    if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s_%s",
                                object_storage_root_dir, CLUSTER_DIR_STR,
                                cluster_id, SERVER_DIR_STR, server_id, SERVER_DIR_STR, SLOG_STR))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
    }
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                                     SLOG_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/
int ObDirManager::get_ls_dir(char *path, const int64_t length,
                             const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, LS_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/
int ObDirManager::get_ls_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                const int64_t tenant_epoch_id, const int64_t ls_id,
                                const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || ls_id < 0 ||
                  ls_epoch_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id), K(ls_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld_%ld",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, LS_DIR_STR, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(ls_id), K(ls_epoch_id), K(tenant_id),
             K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/
int ObDirManager::get_local_tablet_data_dir(char *path, const int64_t length,
                                            const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id, TABLET_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/tablet_data/
int ObDirManager::get_remote_tablet_data_dir(char *path, const int64_t length,
                                             const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                                     TABLET_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/scatter_id
int ObDirManager::get_local_tablet_data_scatter_dir(
    char *path, const int64_t length, const uint64_t tenant_id,
    const int64_t tenant_epoch_id, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  !is_valid_tenant_id(tenant_id) || (tenant_epoch_id < 0) || (tablet_id <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id), K(tablet_id));
  } else if (OB_FAIL(get_local_tablet_data_dir(path, length, tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get local tablet data dir", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%02ld",
                                     (tablet_id % ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM)))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/
int ObDirManager::get_tablet_data_tablet_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                const int64_t tenant_epoch_id, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tablet_id <= 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id));
  } else if (OB_FAIL(get_local_tablet_data_scatter_dir(path, length, tenant_id, tenant_epoch_id, tablet_id))) {
    LOG_WARN("fail to get local tablet data scatter dir", KR(ret), K(path), K(length), K(tenant_id),
             K(tenant_epoch_id), K(tablet_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%ld", tablet_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id/
int ObDirManager::get_tablet_data_tablet_id_private_macro_third_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                             const int64_t tenant_epoch_id, const int64_t tablet_id,
                                                             const int64_t private_macro_third_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_tablet_data_tablet_id_dir(path, length, tenant_id, tenant_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet data tablet id dir", KR(ret), K(path), K(length), K(tenant_id),
             K(tenant_epoch_id), K(tablet_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%ld", private_macro_third_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(private_macro_third_id));
  }
  return ret;
}

// tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/private_macro_third_id/data/ or meta/
int ObDirManager::get_local_tablet_id_macro_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                const int64_t tenant_epoch_id, const int64_t tablet_id,
                                                const int64_t private_macro_third_id, const ObMacroType macro_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length),
             K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_tablet_data_tablet_id_private_macro_third_id_dir(path, length, tenant_id, tenant_epoch_id,
                                                                tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to get tablet data tablet id private macro third id dir", KR(ret), K(path), K(length),
             K(tenant_id), K(tenant_epoch_id), K(tablet_id), K(private_macro_third_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%s",
                                     get_macro_type_str(macro_type)))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/private_macro_third_id/data/ or meta/
int ObDirManager::get_remote_tablet_id_macro_dir(char *path, const int64_t length,
                                                 const uint64_t tenant_id,
                                                 const int64_t tenant_epoch_id,
                                                 const int64_t tablet_id,
                                                 const int64_t private_macro_third_id,
                                                 const ObMacroType macro_type)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0 || private_macro_third_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id), K(tenant_id),
             K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld/%ld/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id,
                                     TABLET_DATA_DIR_STR, tablet_id, private_macro_third_id, get_macro_type_str(macro_type)))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// inner_tablet: cluster_id/tenant_id/ls/ls_id/tablet_name
// user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn
int ObDirManager::get_remote_macro_dir(
    char *path, const int64_t length, const uint64_t tenant_id, const bool is_ls_inner_tablet,
    const int64_t ls_id, const int64_t tablet_id, const int64_t reorganization_scn)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  !is_valid_tenant_id(tenant_id) || (ls_id < 0) || (tablet_id <= 0) ||
                  (reorganization_scn < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id), K(tenant_id),
             K(is_ls_inner_tablet), K(ls_id), K(reorganization_scn));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (is_ls_inner_tablet) { // 1. inner tablet
    if (OB_FAIL(databuff_printf(path, length, "%s/%s_%lu/%s_%lu/%s/%ld/%s",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                TENANT_DIR_STR, tenant_id, LS_DIR_STR, ls_id, get_ls_inner_tablet_name_(tablet_id)))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id));
    }
  } else { // 2. user tablet
    if (OB_FAIL(databuff_printf(path, length, "%s/%s_%lu/%s_%lu/%s/%ld/%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, tablet_id, reorganization_scn))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id), K(reorganization_scn));
    }
  }
  return ret;
}

// inner_tablet: cluster_id/tenant_id/ls/ls_id/TABLET_NAME/sstable_type_str/sstable/op_id
// user_tablet: cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/sstable_type_str/sstable/op_id
int ObDirManager::get_remote_macro_dir_by_op(char *path, const int64_t length, const uint64_t tenant_id,
                                             const common::ObTabletID &tablet_id, const int64_t ls_id,
                                             const int64_t reorganization_scn, const char *sstable_type_str,
                                             const int64_t op_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  !is_valid_tenant_id(tenant_id) || (ls_id < 0) || (!tablet_id.is_valid()) ||
                  (reorganization_scn < 0) || (op_id < 0) || (sstable_type_str == nullptr))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id), K(tenant_id),
             K(ls_id), K(reorganization_scn), K(op_id), K(sstable_type_str));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (tablet_id.is_ls_inner_tablet()) { // 1. inner tablet
    if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/%s/op_%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                TENANT_DIR_STR, tenant_id, LS_DIR_STR, ls_id, ObFileHelper::get_ls_inner_tablet_name(tablet_id.id()),
                sstable_type_str, SHARED_TABLET_SSTABLE_DIR_STR, op_id))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id), K(ls_id), K(ObFileHelper::get_ls_inner_tablet_name(tablet_id.id())), K(sstable_type_str), K(SHARED_TABLET_SSTABLE_DIR_STR), K(op_id));
    }
  } else { // 2. user tablet
    if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s/op_%ld",
                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, TENANT_DIR_STR, tenant_id, TABLET_DIR_STR, tablet_id.id(),
                reorganization_scn, sstable_type_str, SHARED_TABLET_SSTABLE_DIR_STR, op_id))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(tenant_id), K(reorganization_scn), K(sstable_type_str), K(op_id));
    }
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/
int ObDirManager::get_tablet_meta_dir(char *path, const int64_t length,
                                      const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                      const int64_t ls_id, const int64_t ls_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  ls_id < 0 || ls_epoch_id < 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id),
             K(ls_epoch_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s/%ld_%ld/%s",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id,
                     LS_DIR_STR, ls_id, ls_epoch_id, TABLET_META_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(ls_id), K(ls_epoch_id),
             K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/
int ObDirManager::get_tablet_meta_scatter_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                              const int64_t tenant_epoch_id, const int64_t ls_id,
                                              const int64_t ls_epoch_id, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  (ls_id < 0) || (ls_epoch_id < 0) || (tablet_id <= 0) ||
                  !is_valid_tenant_id(tenant_id) || (tenant_epoch_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id), K(ls_epoch_id),
             K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_tablet_meta_dir(path, length, tenant_id, tenant_epoch_id, ls_id, ls_epoch_id))) {
    LOG_WARN("fail to get tablet meta dir", KR(ret), K(path), K(length), K(tenant_id),
             K(tenant_epoch_id), K(ls_id), K(ls_epoch_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%02ld",
                                     (tablet_id % ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM)))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/
int ObDirManager::get_tablet_meta_tablet_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                const int64_t tenant_epoch_id, const int64_t ls_id,
                                                const int64_t ls_epoch_id, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  ls_id < 0 || ls_epoch_id < 0 || tablet_id <= 0 || !is_valid_tenant_id(tenant_id) ||
                  tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id), K(ls_epoch_id),
             K(tablet_id), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_tablet_meta_scatter_dir(path, length, tenant_id, tenant_epoch_id, ls_id,
                                                 ls_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet meta scatter dir", KR(ret), K(path), K(length), K(tenant_id),
             K(tenant_epoch_id), K(ls_id), K(ls_epoch_id), K(tablet_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%ld", tablet_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id));
  }
  return ret;
}

// tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/private_macro_third_id
int ObDirManager::get_tablet_meta_tablet_id_private_macro_third_id_dir(
    char *path, const int64_t length, const uint64_t tenant_id,
    const int64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id,
    const int64_t tablet_id, const int64_t private_macro_third_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  (ls_id < 0) || (ls_epoch_id < 0) || (tablet_id <= 0) || !is_valid_tenant_id(tenant_id) ||
                  (tenant_epoch_id < 0) || (private_macro_third_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id), K(ls_epoch_id),
             K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_tablet_meta_tablet_id_dir(path, length, tenant_id, tenant_epoch_id,
                                                   ls_id, ls_epoch_id, tablet_id))) {
    LOG_WARN("fail to get tablet meta dir", KR(ret), K(tenant_id), K(tenant_epoch_id), K(ls_id),
             K(ls_epoch_id), K(tablet_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%ld", private_macro_third_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(private_macro_third_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/private_macro_third_id
int ObDirManager::get_remote_tablet_meta_tablet_id_private_macro_third_id_dir(
    char *path, const int64_t length, const uint64_t tenant_id,
    const int64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id,
    const int64_t tablet_id, const int64_t private_macro_third_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  (ls_id < 0) || (ls_epoch_id < 0) || (tablet_id <= 0) || !is_valid_tenant_id(tenant_id) ||
                  (tenant_epoch_id < 0) || (private_macro_third_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(ls_id),
             K(tablet_id), K(tenant_id), K(tenant_epoch_id), K(private_macro_third_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%ld_%ld/%s/%ld_%ld/%s/%ld/%ld",
                                     object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, 
                                     server_id, tenant_id, tenant_epoch_id, LS_DIR_STR, ls_id, ls_epoch_id, TABLET_META_DIR_STR, tablet_id, private_macro_third_id))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  }
  return ret;
}

// cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/meta/
int ObDirManager::get_shared_tablet_meta_dir(
    char *path,
    const int64_t length,
    const int64_t tablet_id,
    const int64_t reorganization_scn)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || tablet_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(), TABLET_DIR_STR, tablet_id,
                                     reorganization_scn, MAJOR_DIR_STR, META_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(MTL_ID()));
  }
  return ret;
}

// inner_tablet: ls/ls_id/tablet_name/meta
// user_tablet: tablet/tablet_id/reorganization_scn/meta
int ObDirManager::get_shared_tablet_meta_relative_dir(
    char *path, 
    const int64_t length,
    const bool is_ls_inner_tablet, 
    const int64_t ls_id,
    const int64_t tablet_id, 
    const int64_t reorganization_scn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  (ls_id < 0) || (tablet_id <= 0) || (reorganization_scn < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(path), K(length), K(is_ls_inner_tablet), 
             K(ls_id), K(tablet_id), K(reorganization_scn));
  } else if (is_ls_inner_tablet) { // 1. inner tablet
    if (OB_FAIL(databuff_printf(path, length, "%s/%ld/%s/%s",
                LS_DIR_STR, ls_id, ObFileHelper::get_ls_inner_tablet_name(tablet_id), META_DIR_STR))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(is_ls_inner_tablet), K(ls_id), K(tablet_id));
    }
  } else { // 2. user tablet
    if (OB_FAIL(databuff_printf(path, length, "%s/%ld/%ld/%s",
                TABLET_DIR_STR, tablet_id, reorganization_scn, META_DIR_STR))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(reorganization_scn));
    }
  }
  return ret;
}

// cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/sstable/
int ObDirManager::get_shared_tablet_data_dir(
    char *path,
    const int64_t length,
    const int64_t tablet_id,
    const int64_t reorganization_scn)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || tablet_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld/%ld/%s/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(), TABLET_DIR_STR, tablet_id,
                                     reorganization_scn, MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(MTL_ID()));
  }
  return ret;
}

// cluster_id/tenant_id/tablet_ids/
int ObDirManager::get_shared_tablet_ids_dir(char *path, const int64_t length)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(), TABLET_IDS_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(MTL_ID()));
  }
  return ret;
}

// cluster_id/tenant_id/tablet/tablet_id
int ObDirManager::get_shared_tablet_dir(char *path, const int64_t length, const int64_t tablet_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE || tablet_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tablet_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s/%ld",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(),
                                     TABLET_DIR_STR, tablet_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(MTL_ID()));
  }
  return ret;
}

// tenant_id_epoch_id/tmp_data/
int ObDirManager::get_local_tmp_data_dir(char *path, const int64_t length,
                                         const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
                     get_local_cache_root_dir(), tenant_id,
                     tenant_epoch_id, TMP_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/tmp_data/
int ObDirManager::get_remote_tmp_data_dir(char *path, const int64_t length,
                                          const uint64_t tenant_id, const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s",
                     object_storage_root_dir,
                     CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
                     tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/tmp_data/scatter_id
int ObDirManager::get_local_tmp_data_scatter_dir(
    char *path, const int64_t length, const uint64_t tenant_id,
    const int64_t tenant_epoch_id, const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr == path) || (length <= 0) || (length > common::MAX_PATH_SIZE) ||
                  !is_valid_tenant_id(tenant_id) || (tenant_epoch_id < 0) || (tmp_file_id < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id), K(tmp_file_id));
  } else if (OB_FAIL(get_local_tmp_data_dir(path, length, tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to get local tmp data dir", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%03ld",
                                     ((tmp_file_id / ObDirManager::TMP_FILE_SCATTER_DIR_NUM) % ObDirManager::TMP_FILE_SCATTER_DIR_NUM)))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tmp_file_id));
  }
  return ret;
}

// tenant_id_epoch_id/tmp_data/scatter_id/tmp_file_id/
int ObDirManager::get_local_tmp_file_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                         const int64_t tenant_epoch_id, const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tmp_file_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_local_tmp_data_scatter_dir(path, length, tenant_id, tenant_epoch_id, tmp_file_id))) {
    LOG_WARN("fail to get local tmp data scatter dir", KR(ret), K(path), K(length), K(tenant_id),
             K(tenant_epoch_id), K(tmp_file_id));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), length - STRLEN(path), "/%ld", tmp_file_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tmp_file_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/
int ObDirManager::get_remote_tmp_file_dir(char *path, const int64_t length,
                                          const uint64_t tenant_id,
                                          const int64_t tenant_epoch_id,
                                          const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  tmp_file_id < 0 || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tmp_file_id), K(tenant_id),
             K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%ld",
                     object_storage_root_dir, CLUSTER_DIR_STR,
                     cluster_id, SERVER_DIR_STR, server_id,
                     tenant_id, tenant_epoch_id, TMP_DATA_DIR_STR, tmp_file_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tmp_file_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// tenant_id_epoch_id/shared_mini_macro_cache/
// tenant_id_epoch_id/shared_minor_macro_cache/
// tenant_id_epoch_id/shared_major_macro_cache/
// tenant_id_epoch_id/shared_inc_major_macro_cache/
int ObDirManager::get_shared_macro_cache_dir(char *path, const int64_t length,
                                             const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                             const ObSharedMacroType type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/",
                     get_local_cache_root_dir(), tenant_id, tenant_epoch_id))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
  } else {
    const int64_t str_len = strlen(path);
    switch (type) {
      case ObSharedMacroType::MINI: {
        if (OB_FAIL(databuff_printf(path + str_len, length - str_len, "%s", SHARED_MINI_MACRO_CACHE_DIR_STR))) {
          LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
        }
        break;
      }
      case ObSharedMacroType::MINOR: {
        if (OB_FAIL(databuff_printf(path + str_len, length - str_len, "%s", SHARED_MINOR_MACRO_CACHE_DIR_STR))) {
          LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
        }
        break;
      }
      case ObSharedMacroType::MAJOR: {
        if (OB_FAIL(databuff_printf(path + str_len, length - str_len, "%s", SHARED_MAJOR_MACRO_CACHE_DIR_STR))) {
          LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
        }
        break;
      }
      case ObSharedMacroType::INC_MAJOR: {
        if (OB_FAIL(databuff_printf(path + str_len, length - str_len, "%s", INC_MAJOR_DATA_DIR_STR))) {
          LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(tenant_epoch_id));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected shared macro type", KR(ret), K(type));
        break;
      }
    }
  }
  return ret;
}

int ObDirManager::get_object_storage_root_dir(char *&path)
{
  int ret = OB_SUCCESS;
  if (0 == STRLEN(object_storage_root_dir_)) {
    ret = OB_DIR_NOT_EXIST;
    LOG_WARN("object storage root dir has not been set", KR(ret), K_(object_storage_root_dir));
  } else {
    path = object_storage_root_dir_;
  }
  return ret;
}

// tenant_id_epoch_id/external_table_file
int ObDirManager::get_external_table_file_dir(
    char *path,
    const int64_t length,
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)
      || OB_UNLIKELY(length <= 0 || length > common::MAX_PATH_SIZE
          || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
      get_local_cache_root_dir(), tenant_id, tenant_epoch_id, EXTERNAL_TABLE_FILE_DIR_STR))) {
    LOG_WARN("failed to get external table file dir", KR(ret),
        K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

int ObDirManager::get_external_lob_cache_dir(
    char *path,
    const int64_t length,
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)
      || OB_UNLIKELY(length <= 0 || length > common::MAX_PATH_SIZE
          || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%lu_%ld/%s",
      get_local_cache_root_dir(), tenant_id, tenant_epoch_id, EXTERNAL_LOB_CACHE_DIR_STR))) {
    LOG_WARN("failed to format external lob cache directory", KR(ret),
        K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/meta
int ObDirManager::get_macro_cache_ckpt_meta_dir(
    char *path,
    const int64_t length,
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_ISNULL(path)
      || OB_UNLIKELY(length <= 0 || length > common::MAX_PATH_SIZE
          || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret),
        K(cluster_id), K(server_id), K(tenant_id), K(tenant_epoch_id));
  }  else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
      tenant_id, tenant_epoch_id, MACRO_CACHE_CKPT_DIR_STR, META_DIR_STR))) {
    LOG_WARN("failed to get macro cache ckpt meta dir", KR(ret),
        K(path), K(length), K(cluster_id), K(server_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/data
int ObDirManager::get_macro_cache_ckpt_data_dir(
    char *path,
    const int64_t length,
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_ISNULL(path)
      || OB_UNLIKELY(length <= 0 || length > common::MAX_PATH_SIZE
          || !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret),
        K(cluster_id), K(server_id), K(tenant_id), K(tenant_epoch_id));
  }  else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s/%s",
      object_storage_root_dir, CLUSTER_DIR_STR, cluster_id, SERVER_DIR_STR, server_id,
      tenant_id, tenant_epoch_id, MACRO_CACHE_CKPT_DIR_STR, DATA_MACRO_DIR_STR))) {
    LOG_WARN("failed to get macro cache ckpt meta dir", KR(ret),
        K(path), K(length), K(cluster_id), K(server_id), K(tenant_id), K(tenant_epoch_id));
  }
  return ret;
}

// cluster_id/server_id/tenant_id_epoch_id/ckpt/ or cluster_id/server_id/server_ckpt/
int ObDirManager::get_remote_private_ckpt_file_dir(
    char *path,
    const int64_t length,
    const uint64_t tenant_id,
    const int64_t tenant_epoch_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  char *object_storage_root_dir = nullptr;
  if (OB_UNLIKELY(nullptr == path || length <= 0 || length > common::MAX_PATH_SIZE ||
                  !is_valid_tenant_id(tenant_id) || tenant_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(path), K(length), K(tenant_id), K(tenant_epoch_id));
  } else if (OB_FAIL(get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  } else if (OB_SERVER_TENANT_ID == tenant_id) {
    // server tenant: cluster_id/server_id/server_ckpt/
    if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%s_%s",
                                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                                SERVER_DIR_STR, server_id, SERVER_DIR_STR, CKPT_STR))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(cluster_id), K(server_id));
    }
  } else {
    // normal tenant: cluster_id/server_id/tenant_id_epoch_id/ckpt/
    if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s_%lu/%lu_%ld/%s",
                                object_storage_root_dir, CLUSTER_DIR_STR, cluster_id,
                                SERVER_DIR_STR, server_id, tenant_id, tenant_epoch_id, CKPT_STR))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(tenant_id), K(cluster_id), K(server_id), K(tenant_epoch_id));
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
