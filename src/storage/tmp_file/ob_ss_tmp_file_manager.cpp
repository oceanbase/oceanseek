/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/container/ob_array.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
namespace tmp_file
{
/* -------------------------- ObTmpFileDiskUsageCalculator --------------------------- */

bool ObSSTenantTmpFileManager::ObTmpFileDiskUsageCalculator::operator()(
    const ObTmpFileKey &key, ObITmpFileHandle &tmp_file_handle)
{
  if (OB_NOT_NULL(tmp_file_handle.get())) {
    int64_t tmp_file_flushed_size = static_cast<ObSharedStorageTmpFile*>(tmp_file_handle.get())->cal_wbp_begin_offset();
    disk_data_size_ += tmp_file_flushed_size;
    occupied_disk_size_ += common::upper_align(
        tmp_file_flushed_size, ObTmpFileGlobal::PAGE_SIZE);
  } else {
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tmp file handle is null", KR(ret), K(key));
  }
  return true;  // never ends prematurely.
}

/* -------------------------- ObSSTenantTmpFileManager --------------------------- */

ObSSTenantTmpFileManager::ObSSTenantTmpFileManager()
  : wbp_(),
    flush_mgr_(),
    remove_mgr_(),
    shrink_mgr_(),
    wbp_index_cache_allocator_(),
    wbp_index_cache_bucket_allocator_()
{
}

ObSSTenantTmpFileManager::~ObSSTenantTmpFileManager()
{
  destroy();
}

int ObSSTenantTmpFileManager::init_sub_module_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_mgr_.init(tenant_id_, this, &wbp_))) {
    LOG_WARN("fail to init flush mgr", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(remove_mgr_.init(tenant_id_, &files_))) {
    LOG_WARN("fail to init remove mgr", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(shrink_mgr_.init(&wbp_, &flush_mgr_))) {
    LOG_WARN("fail to init shrink mgr", KR(ret));
  } else if (OB_FAIL(wbp_.init())) {
    LOG_WARN("fail to init ObSSTmpWriteBufferPool", KR(ret));
  } else if (OB_FAIL(wbp_index_cache_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                     OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                     ObMemAttr(tenant_id_, "TmpFileIndCache",
                                                     ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init wbp index cache allocator", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(wbp_index_cache_bucket_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                                            OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                                            ObMemAttr(tenant_id_, "TmpFileIndCBkt",
                                                            ObCtxIds::DEFAULT_CTX_ID)))) {
    LOG_WARN("fail to init wbp index cache bucket allocator", KR(ret), K(tenant_id_));
  }  else {
    LOG_INFO("ObSSTenantTmpFileManager init successful", K(tenant_id_), KP(this));
  }

  return ret;
}

int ObSSTenantTmpFileManager::start_sub_module_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(flush_mgr_.start())) {
    LOG_WARN("fail to wait thread", KR(ret));
  } else if (OB_FAIL(remove_mgr_.start())) {
    LOG_WARN("fail to remove mgr", KR(ret));
  } else if (OB_FAIL(shrink_mgr_.start())) {
    LOG_WARN("fail to shrink mgr", KR(ret));
  } else {
    LOG_INFO("ObSSTenantTmpFileManager start successful", K(tenant_id_), KP(this));
  }
  return ret;
}

int ObSSTenantTmpFileManager::stop_sub_module_()
{
  int ret = OB_SUCCESS;
  shrink_mgr_.stop();
  flush_mgr_.stop();
  remove_mgr_.stop();

  LOG_INFO("ObSSTenantTmpFileManager stop finished", K(tenant_id_), KP(this));
  return ret;
}

int ObSSTenantTmpFileManager::wait_sub_module_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_TMP_FAIL(shrink_mgr_.wait())) {
    LOG_WARN("fail to wait wait thread", KR(tmp_ret));
  }
  if (OB_SUCC(ret)) {
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  if (OB_TMP_FAIL(flush_mgr_.wait())) {
    LOG_WARN("fail to wait wait thread", KR(tmp_ret));
  }
  if (OB_SUCC(ret)) {
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  if (OB_TMP_FAIL(remove_mgr_.wait())) {
    LOG_WARN("fail to wait remove thread", KR(tmp_ret));
  }
  if (OB_SUCC(ret)) {
    ret = tmp_ret == OB_SUCCESS ? ret : tmp_ret;
  }

  LOG_INFO("ObSSTenantTmpFileManager wait finished", KR(ret), K(tenant_id_), KP(this));
  return ret;
}

int ObSSTenantTmpFileManager::destroy_sub_module_()
{
  int ret = OB_SUCCESS;
  shrink_mgr_.destroy();
  wbp_.destroy();
  flush_mgr_.destroy();
  wbp_index_cache_allocator_.reset();
  wbp_index_cache_bucket_allocator_.reset();
  if (OB_FAIL(remove_mgr_.destroy())) {
    LOG_WARN("fail to destroy remove mgr", KR(ret), K(remove_mgr_));
  }

  LOG_INFO("ObSSTenantTmpFileManager destroy", KR(ret),
           K(tenant_id_), KP(this),
           K(flush_mgr_), K(remove_mgr_));
  return ret;
}

int ObSSTenantTmpFileManager::alloc_dir(int64_t &dir_id)
{
  int ret = OB_SUCCESS;
  dir_id = ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_running())) {
    if (!GCTX.omt_->is_available_tenant(tenant_id_)) {
      // tenant has been unit-migrated out of this server (e.g. UnitGC stopped it),
      // report OB_TENANT_NOT_IN_SERVER so that the outer caller can re-select a server
      ret = OB_TENANT_NOT_IN_SERVER;
      LOG_WARN("fail to alloc dir, tenant not in server", KR(ret), K(tenant_id_), K(is_running_));
    } else {
      ret = OB_NOT_RUNNING;
      LOG_WARN("fail to alloc dir, not running", KR(ret), K(is_running_));
    }
  } else {
    dir_id = ObTmpFileGlobal::SHARE_STORAGE_DIR_ID;
  }

  LOG_DEBUG("alloc dir over", KR(ret), K(dir_id), K(lbt()));
  return ret;
}

int ObSSTenantTmpFileManager::open(int64_t &fd, const int64_t &dir_id, const char* const label)
{
  int ret = OB_SUCCESS;
  fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  void *buf = nullptr;
  ObSSTmpFileHandle handle;
  ObSharedStorageTmpFile *tmp_file = nullptr;
  blocksstable::MacroBlockId tmp_file_id;
  tmp_file_id.set_storage_object_type(static_cast<uint64_t>(blocksstable::ObStorageObjectType::TMP_FILE));
  blocksstable::ObStorageObjectOpt opt;
  opt.set_ss_tmp_file_object_opt();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_running())) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("fail to open, not running", KR(ret), K(is_running_));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, tmp_file_id))) {
    LOG_WARN("fail to allocate temporary file id from object manager", KR(ret), K(opt));
  } else if (OB_ISNULL(buf = tmp_file_allocator_.alloc(sizeof(ObSharedStorageTmpFile),
                                                       lib::ObMemAttr(tenant_id_, "SSTmpFile")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for share storage temporary file",
             KR(ret), K(sizeof(ObSharedStorageTmpFile)));
  } else if (FALSE_IT(tmp_file = new (buf) ObSharedStorageTmpFile())) {
  } else if (OB_FAIL(tmp_file->init(tenant_id_, tmp_file_id, &wbp_, &flush_mgr_, &remove_mgr_,
                                    &callback_allocator_,
                                    &wbp_index_cache_allocator_, &wbp_index_cache_bucket_allocator_,
                                    label))) {
    LOG_WARN("fail to init share storage temporary file", KR(ret), K(tenant_id_), K(tmp_file_id),
             KPC(tmp_file), KP(label));
  } else if (OB_FAIL(handle.init(tmp_file))) {
    LOG_WARN("fail to init tmp file handle", KR(ret), KPC(tmp_file));
  } else if (OB_FAIL(files_.insert_or_update(ObTmpFileKey(tmp_file_id.second_id()), handle))) {
    LOG_WARN("fail to set refactored to temporary file map", KR(ret),
             K(tmp_file_id.second_id()), KPC(tmp_file));
  } else {
    fd = tmp_file_id.second_id();
    LOG_INFO("succeed to set a share storage temporary file",
             K(tmp_file_id.second_id()), KPC(tmp_file), K(files_.count()), K(lbt()));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_file)) {
    tmp_file->~ObSharedStorageTmpFile();
    tmp_file_allocator_.free(tmp_file);
    tmp_file = nullptr;
  }

  FLOG_INFO("open a tmp file over", KR(ret), K(fd), K(dir_id), KP(tmp_file), K(lbt()));

  return ret;
}

int ObSSTenantTmpFileManager::seal(const int64_t fd, ObITmpFileHandle* file_handle_hint)
{
  int ret = OB_SUCCESS;
  ObITmpFileHandle tmp_file_handle;
  ObITmpFileHandle* tmp_file_handle_ptr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObITenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(get_tmp_file_(fd, file_handle_hint, tmp_file_handle_ptr, tmp_file_handle))) {
    LOG_WARN("fail to get tmp file handle", KR(ret), K(fd));
  } else if (OB_FAIL(static_cast<ObSSTmpFileHandle*>(tmp_file_handle_ptr)->get()->seal())) {
    LOG_WARN("fail to seal", KR(ret), K(fd), KP(tmp_file_handle_ptr));
  } else {
    LOG_INFO("seal a tmp file over", KR(ret), K(fd));
  }
  return ret;
}

int ObSSTenantTmpFileManager::get_tmp_file(const int64_t fd, ObSSTmpFileHandle &handle) const
{
  int ret = OB_SUCCESS;

  // Get temporary file and increase refcnt, return through handle.
  if (OB_FAIL(files_.get(ObTmpFileKey(fd), handle)) && ret != OB_ENTRY_NOT_EXIST) {
    LOG_WARN("fail to get tmp file", KR(ret), K(fd));
  } else if (ret == OB_ENTRY_NOT_EXIST) {
    LOG_WARN("fail to get tmp file, not exist", KR(ret), K(fd));
  } else if (OB_ISNULL(handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid tmp file pointer", KR(ret), K(fd), KP(handle.get()));
  }

  return ret;
}

int ObSSTenantTmpFileManager::get_tmp_file_disk_usage(int64_t &disk_data_size, int64_t &occupied_disk_size)
{
  int ret = OB_SUCCESS;
  ObTmpFileDiskUsageCalculator op;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
// XXX This function must still be available after the tenant is stopped and before it is destroyed.
//  } else if (OB_UNLIKELY(!is_running())) {
//    ret = OB_NOT_RUNNING;
//    LOG_WARN("fail to remove, not running", KR(ret), K(is_running_));
  } else if (OB_FAIL(files_.for_each(op))) {
    LOG_WARN("fail to for each in tmp file map", KR(ret));
  } else {
    disk_data_size = op.get_disk_data_size();
    occupied_disk_size = op.get_occupied_disk_size();
  }
  return ret;
}

int ObSSTenantTmpFileManager::get_suggested_max_tmp_file_num(int64_t& suggested_max_tmp_file_num,
                const int64_t write_cache_size_expected_reside_in_memory)
{
  int ret = OB_SUCCESS;
  suggested_max_tmp_file_num = 0;

  int64_t memory_limit = 0;
  int64_t local_disk_cache_size = 0;

  int64_t suggested_max_tmp_file_num_by_memory = 0;
  int64_t suggested_max_tmp_file_num_by_local_cache = 0;
  memory_limit = wbp_.get_memory_limit();

  if (write_cache_size_expected_reside_in_memory <= 0) {
    int64_t& write_cache_size_expected_reside_in_memory_mutable_ref =
                  const_cast<int64_t&>(write_cache_size_expected_reside_in_memory);
    write_cache_size_expected_reside_in_memory_mutable_ref = 16 * 1024; // 16KB
  }

  if (memory_limit <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected memory limit", KR(ret), K(memory_limit));
  } else {
    suggested_max_tmp_file_num_by_memory = memory_limit / (write_cache_size_expected_reside_in_memory);
    LOG_INFO("suggested max tmp file num by tmp file write buffer pool memory limit",
        KR(ret), K(memory_limit), K(suggested_max_tmp_file_num_by_memory));
  }

  if (OB_SUCC(ret)) {
    // int64_t preread_cache_reserved_size = MTL(ObTenantDiskSpaceManager*)->get_preread_cache_reserved_size();
    // int64_t write_cache_reserved_size = MTL(ObTenantDiskSpaceManager*)->get_tmp_file_write_cache_reserved_size();

    // local_disk_cache_size = preread_cache_reserved_size + write_cache_reserved_size;
    local_disk_cache_size = MTL(ObTenantDiskSpaceManager*)->get_unit_config_tnt_macro_cache_size_by_usage_ratio(); // TODO: check if this is suitable

    if (OB_STORAGE_OBJECT_MGR.get_macro_object_size() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected macro_object_size", KR(ret), K(OB_STORAGE_OBJECT_MGR.get_macro_object_size()));
    } else {
      suggested_max_tmp_file_num_by_local_cache =
          local_disk_cache_size / OB_STORAGE_OBJECT_MGR.get_macro_object_size();
      LOG_INFO("suggested max tmp file num by tmp file local disk cache size",
          KR(ret), K(local_disk_cache_size), K(suggested_max_tmp_file_num_by_local_cache));
    }
  }

  if (OB_SUCC(ret)) {
    suggested_max_tmp_file_num = MIN(suggested_max_tmp_file_num_by_memory,
                                     suggested_max_tmp_file_num_by_local_cache);
  }

  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase