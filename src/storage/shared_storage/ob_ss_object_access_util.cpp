/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "share/ob_io_device_helper.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::lib;
using namespace oceanbase::share;

int ObSSObjectAccessUtil::pread_file(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSObjectAccessUtil::async_pread_file(read_info, object_handle))) {
    LOG_WARN("fail to async pread file", KR(ret), K(read_info));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait", KR(ret), K(read_info));
  }
  return ret;
}

int ObSSObjectAccessUtil::async_pread_file(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_manager = nullptr;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io argument", K(ret), K(read_info), KCSTRING(lbt()));
  } else if (OB_FAIL(get_file_manager(read_info.mtl_tenant_id_, file_manager))) {
    LOG_WARN("fail to get file manager", KR(ret), "tenant_id", read_info.mtl_tenant_id_, K(read_info));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (SSObjUtil::is_tmp_file(read_info.macro_block_id_.storage_object_type())) {
    // For temporary files, use ObSegmentFileManager
    ObTenantFileManager *tenant_file_manager = static_cast<ObTenantFileManager*>(file_manager);
    if (OB_FAIL(tenant_file_manager->get_segment_file_mgr().async_pread_file(read_info, object_handle))) {
      LOG_WARN("fail to async pread tmp file", KR(ret), K(read_info), K(object_handle));
    }
  } else if (OB_FAIL(file_manager->async_pread_file(read_info, object_handle))) {
    LOG_WARN("fail to async pread file", KR(ret), K(read_info), K(object_handle));
  }
  return ret;
}

int ObSSObjectAccessUtil::append_file(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSObjectAccessUtil::async_append_file(write_info, object_handle))) {
    LOG_WARN("fail to async append file", KR(ret), K(write_info), K(object_handle));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait", KR(ret), K(write_info), K(object_handle));
  }
  return ret;
}

int ObSSObjectAccessUtil::async_append_file(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_manager = nullptr;
  const MacroBlockId &file_id = object_handle.get_macro_id();
  if (OB_UNLIKELY((!SSObjUtil::is_tmp_file(file_id)) || !write_info.is_valid() ||
                  (OB_SERVER_TENANT_ID == write_info.mtl_tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(write_info), "storage object type", STI(file_id.storage_object_type()));
  } else if (OB_FAIL(get_file_manager(write_info.mtl_tenant_id_, file_manager))) {
    LOG_WARN("fail to get file manager", KR(ret), "tenant_id", write_info.mtl_tenant_id_, K(write_info));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else {
    ObTenantFileManager *tenant_file_manager = static_cast<ObTenantFileManager *>(file_manager);
    ObSegmentFileManager &segment_file_manager = tenant_file_manager->get_segment_file_mgr();
    if (OB_FAIL(segment_file_manager.async_append_file(write_info, object_handle))) {
      LOG_WARN("fail to async append file", KR(ret), K(write_info), K(object_handle));
    }
  }
  return ret;
}

int ObSSObjectAccessUtil::write_file(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSObjectAccessUtil::async_write_file(write_info, object_handle))) {
    LOG_WARN("fail to async write file", KR(ret), K(write_info), K(object_handle));
  } else if (OB_FAIL(object_handle.wait())) {
    LOG_WARN("fail to wait", KR(ret), K(write_info), K(object_handle));
  }
  return ret;
}

int ObSSObjectAccessUtil::async_write_file(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle ori_object_handle(object_handle); // when aio write failed, object_handle will be reset
  const MacroBlockId &file_id = object_handle.get_macro_id();
  if (OB_UNLIKELY((SSObjUtil::is_tmp_file(file_id)) ||
                  !write_info.is_valid() || !file_id.is_valid() ||
                  write_info.io_desc_.is_write_through())) {
    // disable user layer to set write_through/back, only io layer can determine write_through/back
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "storage object type", STI(file_id.storage_object_type()), K(write_info), K(object_handle));
  } else if (OB_FAIL(inner_async_write_file(write_info, object_handle))) {
    if ((OB_SERVER_OUTOF_DISK_SPACE == ret) && file_id.is_macro_write_cache_ctrl_obj_type()) {
      // ignore ret, and retry write through
      ret = OB_SUCCESS;
      object_handle = ori_object_handle;
      ObStorageObjectWriteInfo new_write_info(write_info);
      new_write_info.io_desc_.set_write_through();
      if (OB_FAIL(inner_async_write_file(new_write_info, object_handle))) {
        LOG_WARN("fail to inner async write file", KR(ret), K(new_write_info), K(file_id), K(object_handle));
      }
    } else {
      LOG_WARN("fail to inner async write file", KR(ret), K(write_info), K(file_id));
    }
  }
  return ret;
}

int ObSSObjectAccessUtil::inner_async_write_file(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObBaseFileManager *file_manager = nullptr;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObStorageObjectWriteInfo new_write_info(write_info);
  const MacroBlockId &file_id = object_handle.get_macro_id();
  if (OB_UNLIKELY(SSObjUtil::is_tmp_file(file_id) || !write_info.is_valid() || !file_id.is_valid() ||
                  (write_info.io_desc_.is_write_through() && !file_id.is_macro_write_cache_ctrl_obj_type()) ||
                  ((ObStorageObjectWriteStrategy::WRITE_BACK == new_write_info.write_strategy_) &&
                   !STI(file_id.storage_object_type()).has_write_back_strategy()) ||
                  ((ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE == new_write_info.write_strategy_) &&
                   !STI(file_id.storage_object_type()).has_write_through_and_try_write_lcache_strategy()))) {
    // disable user layer to set write_through/back, only io layer can determine write_through/back
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "storage object type", STI(file_id.storage_object_type()), K(write_info), K(object_handle));
  } else if (OB_FAIL(try_set_default_write_strategy(file_id, new_write_info))) {
    LOG_WARN("fail to try set default write strategy", KR(ret), K(file_id), K(new_write_info));
  } else if (OB_FAIL(get_file_manager(write_info.mtl_tenant_id_, file_manager))) {
    LOG_WARN("fail to get file manager", KR(ret), K(write_info), K(object_handle));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(write_info), K(object_handle));
  } else if (OB_SERVER_TENANT_ID == write_info.mtl_tenant_id_) { // case1: 500 tenant
    // 500 tenant only writes SERVER_META and IS_SHARED_TENANT_DELETED,
    // which occupies small disk space that can be ignored.
    // therefore, no need to alloc file size, directly write file.
    if (OB_FAIL(file_manager->async_write_file(write_info, object_handle))) {
      LOG_WARN("fail to async write file", KR(ret), K(write_info), K(object_handle));
    }
  } else { // case2: !500 tenant
    // alloc file size, and then write file
    uint64_t tmp_effective_tablet_id = ObTabletID::INVALID_TABLET_ID;
    if (SSObjUtil::has_effective_tablet_id(file_id)) {
      tmp_effective_tablet_id = ObSSMacroCacheMgr::get_effective_tablet_id(write_info.get_is_write_cache(),
                                                                           write_info.get_effective_tablet_id(),
                                                                           file_id);
    }
    const uint64_t effective_tablet_id = tmp_effective_tablet_id;
    const bool need_alloc_file_size = SSObjUtil::is_need_alloc_file_size(file_id);
    bool need_free_file_size = need_alloc_file_size;
    bool is_write_cache_exceed_threshold = false;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache mgr is null", KR(ret), K(write_info), K(object_handle));
    } else if (OB_FAIL(macro_cache_mgr->check_write_cache_threshold(is_write_cache_exceed_threshold))) {
      LOG_WARN("fail to check write cache threshold", KR(ret));
    } else if ((SSObjUtil::is_macro(file_id) && is_write_cache_exceed_threshold) ||
               (ObStorageObjectWriteStrategy::WRITE_BACK != new_write_info.write_strategy_)) {
      // 1. write through PRIVATE_DATA_MACRO/PRIVATE_META_MACRO/SHARED_MINI_DATA_MACRO/SHARED_MINI_META_MACRO,
      // when write cache exceeds threshold or not adopt WRITE_BACK strategy
      // 2. write through object type that has no write back strategy (e.g., SHARED_TABLET_SUB_META)
      new_write_info.io_desc_.set_write_through(true);
      need_free_file_size = false;
      if (is_write_cache_exceed_threshold && (ObStorageObjectWriteStrategy::WRITE_BACK == new_write_info.write_strategy_)) {
        if (TC_REACH_TIME_INTERVAL(10_s)) {
          LOG_INFO("fallback to write through due to write cache exceeds threshold", K(file_id));
        }
      }
    } else if (is_macro_write_cache_disabled() && file_id.is_macro_write_cache_ctrl_obj_type()) {
      // write through PRIVATE_DATA_MACRO/PRIVATE_META_MACRO/PRIVATE_TABLET_META/SHARED_MINI_DATA_MACRO/SHARED_MINI_META_MACRO,
      // when write cache is disabled
      new_write_info.io_desc_.set_write_through(true);
      need_free_file_size = false;
    } else if (new_write_info.io_desc_.is_write_through() && file_id.is_macro_write_cache_ctrl_obj_type()) {
      // in case of create_dir outof disk space, retry write through
      need_free_file_size = false;
      if (TC_REACH_TIME_INTERVAL(10_s)) {
        LOG_INFO("fallback to write through due to disk space is not enough", K(file_id));
      }
    } else if (!need_alloc_file_size) {
      // no need to alloc file size, skip
    } else if (OB_FAIL(macro_cache_mgr->alloc_file_size(file_id.storage_object_type(),
                                                        effective_tablet_id,
                                                        SSObjUtil::has_effective_tablet_id(file_id)/*use_effective_tablet_id*/,
                                                        new_write_info.size_,
                                                        ObDiskSpaceType::FILE))) {
      // for PRIVATE_DATA_MACRO/PRIVATE_META_MACRO/PRIVATE_TABLET_META/SHARED_MINI_META_MACRO/SHARED_MINI_META_MACRO,
      // if disk space is not enough, then write through object storage
      if ((OB_SERVER_OUTOF_DISK_SPACE == ret) && file_id.is_macro_write_cache_ctrl_obj_type()) {
        ret = OB_SUCCESS; // ignore ret, and then write through
        new_write_info.io_desc_.set_write_through(true);
        need_free_file_size = false; // write through no need to alloc and free file size
        if (TC_REACH_TIME_INTERVAL(10_s)) {
          LOG_INFO("fallback to write through due to disk space is not enough", K(file_id));
        }
      } else {
        LOG_WARN("fail to alloc file size", KR(ret), "storage object type", STI(file_id.storage_object_type()), K(effective_tablet_id), "size",
                 new_write_info.size_);
      }
    }
    if (FAILEDx(file_manager->async_write_file(new_write_info, object_handle))) {
      LOG_WARN("fail to async write file", KR(ret), K(write_info), K(object_handle));
      // free file size on fail
      if (need_free_file_size) {
        int tmp_ret = OB_SUCCESS;
        if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("macro cache mgr is null", KR(tmp_ret));
        } else if (OB_TMP_FAIL(macro_cache_mgr->free_file_size(file_id.storage_object_type(),
                                                               effective_tablet_id,
                                                               true/*use_effective_tablet_id*/,
                                                               new_write_info.size_,
                                                               ObDiskSpaceType::FILE))) {
          LOG_WARN("fail to free file size", KR(tmp_ret), "storage object type",
                   STI(file_id.storage_object_type()), K(effective_tablet_id), "size", new_write_info.size_);
        }
      }
    }
  }
  return ret;
}

// set default write strategy for macros that has not set write strategy.
// e.g., PrivateMetaMacro (contains sub meta) written by ObTabletPersister.
int ObSSObjectAccessUtil::try_set_default_write_strategy(
    const MacroBlockId &file_id,
    ObStorageObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_id.is_valid() || !write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(write_info));
  } else if (OB_UNLIKELY((write_info.write_strategy_ < ObStorageObjectWriteStrategy::WRITE_THROUGH)
             || (write_info.write_strategy_ > ObStorageObjectWriteStrategy::INVALID_WRITE_STRATEGY))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected write strategy", KR(ret), K(file_id), K(write_info));
  } else if (ObStorageObjectWriteStrategy::INVALID_WRITE_STRATEGY == write_info.write_strategy_) {
    if (STI(file_id.storage_object_type()).has_write_back_strategy()) {
      write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
    } else {
      write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
    }
  }
  return ret;
}

int ObSSObjectAccessUtil::check_if_hot_tablet(const uint64_t tablet_id, bool &is_hot)
{
  int ret = OB_SUCCESS;
  is_hot = false;
  if (is_valid_tenant_id(MTL_ID())) {
    ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
    if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage cache policy service is null", KR(ret));
    } else if (OB_FAIL(storage_cache_policy_service->is_hot_tablet(tablet_id, is_hot))) {
      LOG_WARN("fail to check if hot tablet", KR(ret), K(tablet_id));
    }
  } else {
    is_hot = false;
  }
  return ret;
}

int ObSSObjectAccessUtil::check_if_cold_tablet(const uint64_t tablet_id, bool &is_cold)
{
  int ret = OB_SUCCESS;
  is_cold = false;
  if (is_valid_tenant_id(MTL_ID())) {
    ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
    if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage cache policy service is null", KR(ret));
    } else if (OB_FAIL(storage_cache_policy_service->is_cold_tablet(tablet_id, is_cold))) {
      LOG_WARN("fail to check if cold tablet", KR(ret), K(tablet_id));
    }
  } else {
    is_cold = false;
  }
  return ret;
}

int ObSSObjectAccessUtil::check_if_hot_or_macro_hot_tablet(const uint64_t tablet_id, bool &is_hot_or_macro_hot)
{
  int ret = OB_SUCCESS;
  is_hot_or_macro_hot = false;
  if (is_valid_tenant_id(MTL_ID()) && is_not_virtual_tenant_id(MTL_ID())) {
    ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
    if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage cache policy service is null", KR(ret));
    } else if (OB_FAIL(storage_cache_policy_service->is_hot_or_macro_hot_tablet(tablet_id, is_hot_or_macro_hot))) {
      LOG_WARN("fail to check if hot or macro hot tablet", KR(ret), K(tablet_id));
    }
  } else {
    is_hot_or_macro_hot = false;
  }
  return ret;
}

int ObSSObjectAccessUtil::get_tablet_policy_status(const uint64_t tablet_id, PolicyStatus &policy_status)
{
  int ret = OB_SUCCESS;
  policy_status = PolicyStatus::MAX_STATUS;
  if (is_valid_tenant_id(MTL_ID())) {
    ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
    if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage cache policy service is null", KR(ret));
    } else if (OB_FAIL(storage_cache_policy_service->get_tablet_policy_status(tablet_id, policy_status))) {
      LOG_WARN("fail to get tablet policy status", KR(ret), K(tablet_id));
    }
  }
  return ret;
}
int ObSSObjectAccessUtil::async_write_file_to_macro_cache(
    const bool use_effective_tablet_id,
    const blocksstable::MacroBlockId &macro_id,
    const blocksstable::ObStorageObjectWriteInfo &write_info,
    blocksstable::ObStorageObjectHandle &object_handle,
    const int64_t hot_tablet_alloc_timeout_us)
{
  int ret = OB_SUCCESS;
  bool is_hot_policy_tablet = false;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  const ObStorageObjectType object_type = macro_id.storage_object_type();
  if (OB_UNLIKELY(!macro_id.is_valid() || !write_info.is_valid() || hot_tablet_alloc_timeout_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(macro_id), K(write_info), K(object_handle),
        K(hot_tablet_alloc_timeout_us));
  } else if (OB_FAIL(object_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set macro block id", KR(ret), K(macro_id), K(write_info), K(object_handle),
        K(hot_tablet_alloc_timeout_us));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), K(macro_id), K(write_info), K(object_handle),
        K(hot_tablet_alloc_timeout_us));
  } else {
    if (use_effective_tablet_id) {
      if (OB_FAIL(check_if_hot_tablet(write_info.get_effective_tablet_id(), is_hot_policy_tablet))) {
        LOG_WARN("fail to check if hot tablet", KR(ret), K(write_info));
      }
    }    
    if (OB_FAIL(ret)) {
    } else if (is_hot_policy_tablet) {
      if (OB_FAIL(macro_cache_mgr->alloc_file_size_for_hot_tablet_with_retry(
                  object_type,
                  write_info.get_effective_tablet_id(), use_effective_tablet_id,
                  write_info.size_,
                  hot_tablet_alloc_timeout_us))) {
        LOG_WARN("fail to alloc_file_size_for_hot_tablet_with_retry", KR(ret),
            K(is_hot_policy_tablet), K(use_effective_tablet_id), K(macro_id), K(write_info), K(object_handle),
            K(hot_tablet_alloc_timeout_us));
      }
    } else {
      if (OB_FAIL(macro_cache_mgr->alloc_file_size(
                  object_type, write_info.get_effective_tablet_id(), use_effective_tablet_id,
                  write_info.size_, ObDiskSpaceType::FILE))) {
        LOG_WARN("fail to alloc_file_size", KR(ret),
            K(is_hot_policy_tablet), K(macro_id), K(write_info), K(object_handle));
      }
    }

    ObSSLocalCacheWriter local_cache_writer;
    if (FAILEDx(local_cache_writer.aio_write_with_create_parent_dir(write_info, object_handle))) {
      LOG_WARN("fail to aio write with create parent dir", KR(ret),
          K(is_hot_policy_tablet), K(use_effective_tablet_id), K(macro_id), K(write_info), K(object_handle),
          K(hot_tablet_alloc_timeout_us));
      
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(macro_cache_mgr->free_file_size(
          object_type,
          write_info.get_effective_tablet_id(), use_effective_tablet_id,
          write_info.size_,
          ObDiskSpaceType::FILE))) {
        LOG_WARN("fail to free file size", KR(ret), KR(tmp_ret),
            K(is_hot_policy_tablet), K(use_effective_tablet_id), K(macro_id), K(write_info), K(object_handle),
            K(hot_tablet_alloc_timeout_us));
      }
    }
  }
  return ret;
}

int ObSSObjectAccessUtil::delete_local_file(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    const bool is_print_log,
    const bool is_del_seg_meta,
    const bool is_logical_delete,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (SSObjUtil::use_reserved_disk_space(file_id)) { // 1. PRIVATE_SLOG_FILE
    // PRIVATE_SLOG_FILE use reserved disk space, which does not participate in calibration.
    // thus, these is need for PRIVATE_SLOG_FILE to consider pause_gc.
    if (OB_FAIL(inner_delete_local_file(file_id, ls_epoch_id, is_print_log,
                                        is_del_seg_meta, is_logical_delete, ls_id))) {
      LOG_WARN("fail to inner delete local file", KR(ret), K(file_id), K(ls_epoch_id),
               K(is_print_log), K(is_del_seg_meta), K(is_logical_delete));
    }
  } else if (SSObjUtil::is_tmp_file(file_id)) { // 2. TMP_FILE
    // 2.1 pause gc
    if (file_manager->is_tmp_file_cache_pause_gc()) {
      // Problem: tmp file exists overwrite. e.g., write one segment [0, 1MB) in local, and then
      // overwrite the same segment [0, 2MB). the overwrite io will delete local file, and write
      // through to object storage. if delete local file return errno, then the whole overwrite io
      // will fail.
      // Solution: if it delete tmp file when calibrating disk space, rename the file to .deleted file.
      // After calibrating disk space, list all files and delete the .deleted file.
      if (OB_FAIL(file_manager->logical_delete_local_tmp_file(file_id, is_del_seg_meta))) {
        LOG_WARN("fail to logical delete local file", KR(ret), K(file_id), K(is_del_seg_meta));
      }
    // 2.2 non pause gc
    } else if (OB_FAIL(inner_delete_local_file(file_id, ls_epoch_id, is_print_log,
                                               is_del_seg_meta, is_logical_delete, ls_id))) {
      LOG_WARN("fail to inner delete local file", KR(ret), K(file_id), K(ls_epoch_id),
               K(is_print_log), K(is_del_seg_meta), K(is_logical_delete));
    }
  } else if (SSObjUtil::is_store_in_table(file_id)) { // 3. STORE_IN_TABLE
    ret = OB_SUCCESS; // STORE_IN_TABLE which store in table or micro cache, do not delete local file
  } else { // 3. non-PRIVATE_SLOG_FILE and non-TMP_FILE
    // 3.1 pause gc
    if (file_manager->is_pause_gc()) {
      // if it delete non-TMP_FILE when calibrating disk space, return OB_FILE_DELETE_FAILED.
      ret = OB_FILE_DELETE_FAILED;
      if (TC_REACH_TIME_INTERVAL(ObTenantFileManager::PRINT_LOG_INTERVAL)) {
        LOG_WARN("the tenant is calibrating data disk alloc size and pausing gc, so do not allow "
                 "to delete files", KR(ret));
      }
    // 3.2 non pause gc
    } else if (OB_FAIL(inner_delete_local_file(file_id, ls_epoch_id, is_print_log,
                                               is_del_seg_meta, is_logical_delete, ls_id))) {
      LOG_WARN("fail to inner delete local file", KR(ret), K(file_id), K(ls_epoch_id),
               K(is_print_log), K(is_del_seg_meta), K(is_logical_delete));
    }
  }
  return ret;
}

// Different from the delete_local_file interface which is used by user tenant and contains many MTL operations,
// the delete_server_tenant_local_file interface only performs unlink operations for 500 tenant.
int ObSSObjectAccessUtil::delete_server_tenant_local_file(
    const blocksstable::MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  ObPathContext ctx;
  if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (SSObjUtil::is_store_in_table(file_id)) {
    ret = OB_SUCCESS; // STORE_IN_TABLE which store in table or micro cache, do not delete local file
    FLOG_INFO("succ to delete local file", K(file_id), K(ls_epoch_id));
  } else if (OB_FAIL(ctx.set_file_ctx(
      file_id, ls_epoch_id, true/*is_local_file*/, GCONF.observer_id, ls_id))) {
    LOG_WARN("fail to convert file path", KR(ret), K(ctx));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(ctx.get_path()))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to del file", KR(ret), K(ctx));
    }
  } else {
    FLOG_INFO("succ to delete local file", K(file_id), K(ls_epoch_id), K(ctx.get_path()));
  }
  return ret;
}

int ObSSObjectAccessUtil::get_file_manager(
    const uint64_t tenant_id,
    ObBaseFileManager *&file_manager)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_SERVER_TENANT_ID == tenant_id) {
    file_manager = &OB_SERVER_FILE_MGR;
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSObjectAccessUtil::inner_delete_local_file(
    const MacroBlockId &file_id,
    const int64_t ls_epoch_id,
    const bool is_print_log,
    const bool is_del_seg_meta,
    const bool is_logical_delete,
    const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObTenantFileManager *file_manager = nullptr;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  const ObStorageObjectType object_type = file_id.storage_object_type();
  ObIODFileStat statbuf;
  bool is_del_file_succ = true;
  ObPathContext ctx;

  // Step 1: Validate parameters and get MTL components
  if (OB_UNLIKELY(!file_id.is_valid() || ls_epoch_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_id), K(ls_epoch_id));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *)) ||
             OB_ISNULL(file_manager = MTL(ObTenantFileManager *)) ||
             OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr or file manager or disk space mgr is null", KR(ret),
             KP(macro_cache_mgr), KP(file_manager), KP(disk_space_mgr));
  }

  // Step 2: Set file context (logical delete or normal)
  if (OB_SUCC(ret)) {
    if (is_logical_delete) {
      if (OB_FAIL(ctx.set_logical_delete_ctx(file_id, ls_epoch_id, GCONF.observer_id, ls_id))) {
        LOG_WARN("fail to convert file path for logical delete", KR(ret), K(ctx));
      }
    } else { // is_logical_delete == false
      if (OB_FAIL(ctx.set_file_ctx(file_id, ls_epoch_id, true/*is_local_file*/, GCONF.observer_id, ls_id))) {
        LOG_WARN("fail to convert file path", KR(ret), K(ctx));
      }
    }
  }

  // Step 3: Get file size if needed (stat before delete)
  if (OB_SUCC(ret)) {
    if (file_manager->is_object_type_need_stat(file_id)) {
      if (OB_FAIL(ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
          is_del_file_succ = false;
        } else {
          LOG_WARN("fail to get file length", KR(ret), K(ctx));
        }
      }
    }
  }

  // Step 4: Delete file under lock protection
  if (OB_SUCC(ret)) {
    // use mutex to avoid concurrent put/put and put/erase of one same macro_id
    ObSSMacroCacheLockGuard guard(file_id, *macro_cache_mgr);

    // 4.1: Get cache type
    if (OB_FAIL(macro_cache_mgr->get_macro_cache_type(file_id, cache_type))) {
      LOG_WARN("fail to get cache type", KR(ret), K(file_id));
    }

    // 4.2: Erase macro cache meta (skip for logical delete)
    // rm .deleted file cannot erase macro cache meta, which may lead to later macro cache meta
    // erased, and then cannot read this tmp file. issue-id: 2025072500110129488
    if (OB_SUCC(ret)) {
      if (!is_logical_delete) {
        if (OB_FAIL(macro_cache_mgr->erase(file_id))) { // Note: erase macro cache meta before unlink
          LOG_WARN("fail to erase from macro cache mgr", KR(ret), K(file_id));
        }
      }
    }

    // 4.3: Delete segment file meta if needed
    // Note: erase segment file meta before unlink.
    // in case of delete local old version seg file when seg file write to object storage, cannot delete seg_meta.
    if (OB_SUCC(ret)) {
      if (SSObjUtil::is_tmp_file(file_id) && is_del_seg_meta) {
        const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
        if (OB_FAIL(file_manager->get_segment_file_mgr().delete_meta(seg_id))) {
          LOG_WARN("fail to delete segment file meta and conflict info", KR(ret), K(file_id));
        }
      }
    }

    // 4.4: Unlink file
    if (FAILEDx(ObIODeviceLocalFileOp::unlink(ctx.get_path()))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
        is_del_file_succ = false;
      } else {
        LOG_WARN("fail to del file", KR(ret), K(ctx));
      }
    } else {
      is_del_file_succ = true;
    }

    // 4.5: Free disk space if file was deleted successfully
    if (OB_SUCC(ret)) {
      if (is_del_file_succ && file_manager->is_object_type_need_stat(file_id) && statbuf.size_ > 0) {
        if (OB_FAIL(disk_space_mgr->free_file_size(statbuf.size_, cache_type, ObDiskSpaceType::FILE))) {
          LOG_WARN("fail to free file size", KR(ret), K(statbuf.size_), K(cache_type));
        // TODO(binifei.bnf): remove below code, after integrating preread cache into macro cache
        } else if (OB_FAIL(file_manager->remove_preread_node(file_id))) {
          LOG_WARN("fail to remove lru node", KR(ret), K(file_id));
        } else if (is_print_log) {
          FLOG_INFO("succ to delete local file", K(file_id), K(ls_epoch_id), "file_path",
                    ctx.get_path(), "size", statbuf.size_);
        }
      }
    }
  }

  // Step 5: Erase fd cache (use TMP_FAIL to not affect main flow)
  if (OB_SUCC(ret)) {
    if (is_del_file_succ && ObSSIOCommonOp::is_supported_fd_cache_obj_type(object_type)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(file_manager->get_fd_cache_mgr().erase_fd_cache_when_del_file(file_id))) {
        LOG_WARN("fail to erase fd cache when del file", KR(tmp_ret), K(file_id));
      }
    }
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase
