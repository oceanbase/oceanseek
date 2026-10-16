/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_handler.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/shared_storage/ob_ss_local_cache_util.h"
#include "share/ob_io_device_helper.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

/**********************************HandleHitDiskParam*****************************/
HandleHitDiskParam::HandleHitDiskParam(
    const ObSSMicroBlockCacheKey &micro_key,
    const blocksstable::MacroBlockId &macro_id,
    const int64_t data_dest,
    const uint32_t crc,
    const ObSSMicroCacheGetType get_type,
    const ObSSMicroCacheAccessType access_type)
  : micro_key_(micro_key), macro_id_(macro_id), data_dest_(data_dest),
    crc_(crc), get_type_(get_type), access_type_(access_type)
{
}

HandleHitDiskParam::~HandleHitDiskParam()
{
}

/**********************************ObSSMicroCacheHandler*****************************/
int ObSSMicroCacheHandler::handle_not_hit(
    const ObSSMicroBlockCacheKey &micro_key,
    const MacroBlockId &macro_id,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(load_micro_data(macro_id, io_info))) {
    LOG_WARN("fail to load micro data", KR(ret), K(macro_id), K(io_info));
  } else if (OB_ISNULL(io_info.fd_.device_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device handle is null", KR(ret));
  } else if (io_info.fd_.device_handle_->is_object_device()) {
    if (OB_FAIL(set_cache_load_from_remote_io_callback(micro_key, io_info, access_type))) {
      LOG_WARN("fail to set cache load io callback", KR(ret), K(micro_key), K(io_info), K(access_type));
    }
  }

  if (FAILEDx(ObIOManager::get_instance().aio_read(io_info, object_handle.get_io_handle()))) {
    LOG_WARN("fail to aio_read", KR(ret), K(io_info));
    free_micro_cache_io_callback(io_info.callback_);
  }

  if (OB_FAIL(ret)) {
    // close_device_and_fd on failed IOManager::aio_read
    if (OB_NOT_NULL(io_info.fd_.device_handle_)) {
      int tmp_ret = OB_SUCCESS;
      if (io_info.fd_.device_handle_->is_object_device()) {
        if (OB_TMP_FAIL(ObBackupIoAdapter::close_device_and_fd(io_info.fd_.device_handle_, io_info.fd_))) {
          LOG_WARN("fail to close device and fd", KR(ret), KR(tmp_ret), K(io_info), K(macro_id),
                   K(micro_key), K(access_type));
        }
      }
    }
  }

  if (FAILEDx(object_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
  }
  return ret;
}

int ObSSMicroCacheHandler::handle_hit_memory(
    const ObSSMicroBlockCacheKey &micro_key,
    const ObSSMicroCacheGetType get_type,
    const MacroBlockId &macro_id,
    const char *io_buf,
    const uint32_t crc,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(io_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), KP(io_buf));
  } else if (OB_FAIL(ObSSMicroCacheUtil::check_data_crc(io_buf, io_info.size_, crc))) {
    LOG_WARN("fail to check data crc", KR(ret), K(crc), K(io_info), KP(io_buf));
    ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
    if (nullptr != micro_cache) {
      if (OB_TMP_FAIL(micro_cache->delete_micro_block_cache(micro_key))) {
        LOG_WARN("fail to delete micro_block in ss_micro_cache", KR(tmp_ret), K(micro_key));
        tmp_ret = OB_SUCCESS;
      }
    }
  } else if (OB_FAIL(ObSSLocalCacheUtil::handle_hit_cache_in_memory(io_buf, io_info.size_, io_info, object_handle))) {
    LOG_WARN("fail to handle hit cache in memory", KR(ret), K(io_info));
  } else if (OB_FAIL(set_macro_block_id(get_type, macro_id, object_handle))) {
    LOG_WARN("fail to set macro block id", KR(ret), K(get_type), K(macro_id));
  }
  return ret;
}

int ObSSMicroCacheHandler::handle_hit_disk(
    const HandleHitDiskParam &param,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tenant_file_manager = nullptr;
  if (OB_UNLIKELY((param.data_dest_ <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), "data_dest", param.data_dest_);
  } else if (OB_ISNULL(tenant_file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is null", KR(ret), "tenant_id", MTL_ID());
  } else {
    io_info.fd_.first_id_ = param.macro_id_.first_id(); // first_id is not used in shared storage mode
    // io_info.fd_.second_id_ = tenant_file_manager->get_micro_cache_file_fd(); // read micro cache fd
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    io_info.offset_ = param.data_dest_; // offset inside micro cache file
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_cache_load_from_local_io_callback(param.micro_key_, param.crc_, io_info, param.access_type_))) {
      LOG_WARN("fail to set cache load io callback", KR(ret), K(param), K(io_info));
    } else if (OB_FAIL(ObIOManager::get_instance().aio_read(io_info, object_handle.get_io_handle()))) {
      LOG_WARN("fail to aio_read", KR(ret), K(io_info));
      free_micro_cache_io_callback(io_info.callback_);
    } else if (OB_FAIL(set_macro_block_id(param.get_type_, param.macro_id_, object_handle))) {
      LOG_WARN("fail to set macro block id", KR(ret), "get_type", param.get_type_, "macro_id", param.macro_id_);
    }
  }
  return ret;
}

int ObSSMicroCacheHandler::load_micro_data(
    const MacroBlockId &macro_id,
    ObIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(get_object_device_and_fd(ObStorageAccessType::OB_STORAGE_ACCESS_NOHEAD_READER,
                                              macro_id, 0/*ls_epoch_id*/, io_info))) {  // micro data path does not need ls_epoch_id
    LOG_WARN("fail to get object device and fd for micro cache", KR(ret), K(macro_id), K(io_info));
  }
  return ret;
}

int ObSSMicroCacheHandler::set_cache_load_from_remote_io_callback(
    const ObSSMicroBlockCacheKey &micro_key,
    ObIOInfo &io_info,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  // cache miss and read data from object storage, use io_callback to load data into micro cache
  ObIAllocator *callback_allocator = nullptr;
  ObSSCacheLoadFromRemoteIOCallback *cache_load_io_callback = nullptr;
  if (OB_FAIL(get_io_callback_allocator(io_info.tenant_id_, callback_allocator))) {
    LOG_WARN("fail to get io callback allocator", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(callback_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("callback allocator is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(cache_load_io_callback = static_cast<ObSSCacheLoadFromRemoteIOCallback *>(
                          callback_allocator->alloc(sizeof(ObSSCacheLoadFromRemoteIOCallback))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cache load io callback memory", KR(ret), "size",
             sizeof(ObSSCacheLoadFromRemoteIOCallback));
  } else {
    cache_load_io_callback = new (cache_load_io_callback) ObSSCacheLoadFromRemoteIOCallback(
                                  callback_allocator, io_info.callback_, micro_key,
                                  io_info.user_data_buf_, io_info.effective_tablet_id_, access_type);
    io_info.callback_ = cache_load_io_callback;
  }
  return ret;
}

int ObSSMicroCacheHandler::set_cache_load_from_local_io_callback(
    const ObSSMicroBlockCacheKey &micro_key,
    const uint32_t crc,
    ObIOInfo &io_info,
    const ObSSMicroCacheAccessType access_type)
{
  int ret = OB_SUCCESS;
  // hit disk and read data from micro cache file, use io_callback to load data into micro cache
  ObIAllocator *callback_allocator = nullptr;
  ObSSCacheLoadFromLocalIOCallback *cache_load_io_callback = nullptr;
  if (OB_FAIL(get_io_callback_allocator(io_info.tenant_id_, callback_allocator))) {
    LOG_WARN("fail to get io callback allocator", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(callback_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("callback allocator is null", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(cache_load_io_callback = static_cast<ObSSCacheLoadFromLocalIOCallback *>(
                          callback_allocator->alloc(sizeof(ObSSCacheLoadFromLocalIOCallback))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cache load io callback memory", KR(ret), "size",
             sizeof(ObSSCacheLoadFromLocalIOCallback));
  } else {
    cache_load_io_callback = new (cache_load_io_callback) ObSSCacheLoadFromLocalIOCallback(
                                  callback_allocator, io_info.callback_, micro_key,
                                  io_info.user_data_buf_, io_info.effective_tablet_id_,
                                  access_type, crc);
    io_info.callback_ = cache_load_io_callback;
  }
  return ret;
}

int ObSSMicroCacheHandler::set_macro_block_id(
    const ObSSMicroCacheGetType get_type,
    const MacroBlockId &macro_id,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (ObSSMicroCacheGetType::GET_CACHE_HIT_DATA == get_type) {
    // do nothing. there is no valid MacroBlockId when GET_CACHE_HIT_DATA (e.g., replica prewarm)
  } else if (OB_FAIL(object_handle.set_macro_block_id(macro_id))) {
    LOG_WARN("fail to set macro block id", KR(ret), K(macro_id));
  }
  return ret;
}

void ObSSMicroCacheHandler::free_micro_cache_io_callback(ObIOCallback *&io_callback)
{
  if (OB_NOT_NULL(io_callback)) {
    if (ObIOCallbackType::SS_CACHE_LOAD_FROM_REMOTE_CALLBACK == io_callback->get_type()) {
      // clear original io callback to avoid double free original io callback, because upper module is
      // responsible for free original io callback
      ObIOCallback *original_callback = clear_original_io_callback2<ObSSCacheLoadFromRemoteIOCallback>(io_callback);
      free_io_callback<ObSSCacheLoadFromRemoteIOCallback>(io_callback);
      io_callback = original_callback;
    } else if (ObIOCallbackType::SS_CACHE_LOAD_FROM_LOCAL_CALLBACK == io_callback->get_type()) {
      // clear original io callback to avoid double free original io callback, because upper module is
      // responsible for free original io callback
      ObIOCallback *original_callback = clear_original_io_callback2<ObSSCacheLoadFromLocalIOCallback>(io_callback);
      free_io_callback<ObSSCacheLoadFromLocalIOCallback>(io_callback);
      io_callback = original_callback;
    }
  }
}

/**********************************ObSSCacheLoadFromRemoteIOCallback*****************************/
ObSSCacheLoadFromRemoteIOCallback::~ObSSCacheLoadFromRemoteIOCallback()
{
  // free original callback
  if (OB_NOT_NULL(callback_) && OB_NOT_NULL(callback_->get_allocator())) {
    ObIAllocator *tmp_allocator = callback_->get_allocator();
    callback_->~ObIOCallback();
    tmp_allocator->free(callback_);
    callback_ = nullptr;
  }
}

int ObSSCacheLoadFromRemoteIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t original_callback_us = 0;
  int64_t cache_load_callback_us = 0;
  if (OB_UNLIKELY((nullptr == data_buffer) || (size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", KR(ret), KP(data_buffer), K(size));
  } else if (nullptr == callback_) {
    if (OB_ISNULL(user_data_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("both callback and user data buf are null", KR(ret));
    } else {
      MEMCPY(user_data_buf_, data_buffer, size);
    }
  } else { // nullptr != callback_
    const int64_t original_callback_start_us = ObTimeUtility::current_time_us();
    if (OB_FAIL(callback_->inner_process(data_buffer, size))) {
      LOG_WARN("fail to inner process", KR(ret));
    }
    original_callback_us = ObTimeUtility::current_time_us() - original_callback_start_us;
  }

  if (OB_SUCC(ret)) {
    ObSSMicroCache *micro_cache = nullptr;
    if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro cache manager is null", KR(ret), "tenant_id", MTL_ID());
    } else {
      const int64_t cache_load_callback_start_us = ObTimeUtility::current_time_us();
      // Note: failing to add_micro_block_cache does not affect io_callback inner_process
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(micro_cache->add_micro_block_cache(micro_key_, data_buffer, size,
                                                         effective_tablet_id_, access_type_))) {
        LOG_WARN("fail to add micro block cache", KR(tmp_ret), K_(micro_key), K(size),
                 K_(effective_tablet_id), K_(access_type));
        ATOMIC_STORE(&is_add_cache_failed_, true);
      }
      cache_load_callback_us = ObTimeUtility::current_time_us() - cache_load_callback_start_us;
    }
  }

  if (OB_UNLIKELY((original_callback_us + cache_load_callback_us) > (2 * 1000 * 1000LL))) { // 2s
    LOG_INFO("callback cost too much time", K(original_callback_us), K(cache_load_callback_us), K(size));
  }
  return ret;
}

/**********************************ObSSCacheLoadFromLocalIOCallback*****************************/
ObSSCacheLoadFromLocalIOCallback::~ObSSCacheLoadFromLocalIOCallback()
{
  // free original callback
  if (OB_NOT_NULL(callback_) && OB_NOT_NULL(callback_->get_allocator())) {
    ObIAllocator *tmp_allocator = callback_->get_allocator();
    callback_->~ObIOCallback();
    tmp_allocator->free(callback_);
    callback_ = nullptr;
  }
}

int ObSSCacheLoadFromLocalIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t original_callback_us = 0;
  if (OB_UNLIKELY((nullptr == data_buffer) || (size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", KR(ret), KP(data_buffer), K(size));
  } else if (OB_FAIL(ObSSMicroCacheUtil::check_data_crc(data_buffer, size, crc_))) {
    LOG_WARN("fail to check data crc", KR(ret), K_(crc), K(size), KP(data_buffer));
    ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
    if (nullptr != micro_cache) {
      if (OB_TMP_FAIL(micro_cache->delete_micro_block_cache(micro_key_))) {
        LOG_WARN("fail to delete micro_block in ss_micro_cache", KR(tmp_ret), K_(micro_key));
        tmp_ret = OB_SUCCESS;
      }
    }
  } else if (nullptr == callback_) {
    if (OB_ISNULL(user_data_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("both callback and user data buf are null", KR(ret));
    } else {
      MEMCPY(user_data_buf_, data_buffer, size);
    }
  } else { // nullptr != callback_
    const int64_t original_callback_start_us = ObTimeUtility::current_time_us();
    if (OB_FAIL(callback_->inner_process(data_buffer, size))) {
      LOG_WARN("fail to inner process", KR(ret));
    }
    original_callback_us = ObTimeUtility::current_time_us() - original_callback_start_us;
  }

  if (OB_UNLIKELY((original_callback_us) > (2 * 1000 * 1000LL))) { // 2s
    FLOG_INFO("callback cost too much time", K(original_callback_us), K(size));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
