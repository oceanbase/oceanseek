/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_tmp_file_io_callback.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

/**
 * --------------------------------ObSSTmpFileReadCallback------------------------------------
 */
ObSSTmpFileReadCallback::~ObSSTmpFileReadCallback()
{
  int ret = OB_SUCCESS;

  // free original callback (only if it exists)
  if (OB_NOT_NULL(callback_)) {
    if (OB_ISNULL(callback_->get_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("callback allocator is null in read callback destructor", K_(seg_id), KR(ret));
    } else {
      ObIAllocator *tmp_allocator = callback_->get_allocator();
      callback_->~ObIOCallback();
      tmp_allocator->free(callback_);
      callback_ = nullptr;
    }
  }

  // Check if read access has already been released in inner_process()
  if (!read_access_released_) {
    // This is a backup: if inner_process() didn't run or failed, release read access here
    if (OB_LIKELY(conflict_info_handle_.is_valid())) {
      if (OB_FAIL(conflict_info_handle_.release_read_access())) {
        LOG_WARN("failed to release read access in callback destructor", KR(ret), K_(seg_id));
      } else {
        LOG_INFO("read access released in destructor as fallback", K_(seg_id), KP(this));
      }
    } else {
      // This happens when callback object was created but set_ss_tmpfile_read_callback() failed,
      // so conflict_info_handle_ was never properly initialized
      LOG_TRACE("conflict info handle is invalid in read callback destructor - no read access to release", K_(seg_id));
    }
  } else {
    // Normal case: read access was already released in inner_process()
    LOG_TRACE("read access already released in inner_process, skip release in destructor", K_(seg_id), KP(this));
  }
}

ObSSTmpFileReadCallback::ObSSTmpFileReadCallback(common::ObIAllocator *allocator, common::ObIOCallback *callback, char *user_data_buf)
  : common::ObIOCallback(common::ObIOCallbackType::SS_TMP_FILE_READ_CALLBACK), seg_id_(), seg_file_mgr_(nullptr), allocator_(allocator), conflict_info_handle_(), read_access_released_(false), callback_(callback), user_data_buf_(user_data_buf)
{
}

int ObSSTmpFileReadCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  
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
    if (OB_FAIL(callback_->inner_process(data_buffer, size))) {
      LOG_WARN("fail to inner process", KR(ret));
    }
  }
  
  // Release read access immediately after I/O processing completes
  // This minimizes lock holding time and prevents lock contention
  if (OB_LIKELY(conflict_info_handle_.is_valid())) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(conflict_info_handle_.release_read_access())) {
      LOG_WARN("failed to release read access in inner_process", K(tmp_ret), K_(seg_id), KP(this));
      // Don't override ret if processing succeeded but read access release failed
      // The read access will be released again in destructor as fallback
    } else {
      read_access_released_ = true;  // Mark that read access has been released
      LOG_TRACE("read access released in inner_process", K_(seg_id), KP(this));
    }
  }
  
  return ret;
}

int ObSSTmpFileReadCallback::set_ss_tmpfile_read_callback(const TmpFileSegId &seg_id, 
                                                          ObSegmentFileManager *seg_file_mgr, 
                                                          common::ObIAllocator *allocator,
                                                          const TmpFileConflictInfoHandle &conflict_info_handle)
{
  int ret = OB_SUCCESS;
  
  if (OB_UNLIKELY(!seg_id.is_valid()) || OB_ISNULL(seg_file_mgr) || OB_ISNULL(allocator) || !conflict_info_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(seg_id), KP(seg_file_mgr), KP(allocator), KP(conflict_info_handle.get_conflict_info()));
  } else if (OB_FAIL(conflict_info_handle_.assign(conflict_info_handle))) {
    LOG_WARN("fail to assign conflict info handle", KR(ret), K(conflict_info_handle));
  } else {
    seg_id_ = seg_id;
    seg_file_mgr_ = seg_file_mgr;
    allocator_ = allocator;
  }
  
  return ret;
}

common::ObIOCallback *ObSSTmpFileReadCallback::clear_original_io_callback()
{
  common::ObIOCallback *original_io_callback = callback_;
  callback_ = nullptr;
  return original_io_callback;
}

void ObSSTmpFileReadCallback::free_io_callback_and_detach_original(common::ObIOCallback *&io_callback)
{
  if (OB_NOT_NULL(io_callback)) {
    if (ObIOCallbackType::SS_TMP_FILE_READ_CALLBACK == io_callback->get_type()) {
      ObSSTmpFileReadCallback *load_callback = static_cast<ObSSTmpFileReadCallback *>(io_callback);
      ObIOCallback *original_callback = load_callback->clear_original_io_callback();
      free_io_callback<ObSSTmpFileReadCallback>(io_callback);
      io_callback = original_callback;
    } else {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected io callback type", KPC(io_callback));
    }
  }
}

/**
 * --------------------------------ObSSTmpFileWriteCallback------------------------------------
 */
ObSSTmpFileWriteCallback::ObSSTmpFileWriteCallback()
  : ObIOCallback(ObIOCallbackType::SS_TMP_FILE_CALLBACK), seg_id_(), meta_handle_(), conflict_info_handle_(),
    seg_meta_op_type_(ObSSTmpFileSegMetaOpType::INSERT), seg_del_type_(ObSSTmpFileSegDeleteType::NONE),
    need_put_or_update_macro_cache_(false), del_seg_valid_len_(0), allocator_(nullptr), write_access_released_(false)
{
}

ObSSTmpFileWriteCallback::~ObSSTmpFileWriteCallback()
{
  int ret = OB_SUCCESS;
  
  // Check if write access has already been released in inner_process_()
  if (!write_access_released_) {
    // This is a backup: if inner_process_() didn't run or failed, release write access here
    if (OB_LIKELY(conflict_info_handle_.is_valid())) {
      if (OB_FAIL(conflict_info_handle_.release_write_access())) {
        LOG_WARN("failed to release write access in callback destructor", KR(ret), K_(seg_id));
      } else {
        LOG_INFO("write access released in destructor as fallback", K_(seg_id), KP(this));
      }
    } else {
      // This happens when callback object was created but set_ss_tmpfile_write_callback() failed,
      // so conflict_info_handle_ was never properly initialized
      LOG_TRACE("conflict info handle is invalid in write callback destructor", K_(seg_id));
    }
  } else {
    // Normal case: write access was already released in inner_process_()
    LOG_TRACE("write access already released in inner_process, skip release in destructor", K_(seg_id), KP(this));
  }
}

int ObSSTmpFileWriteCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  UNUSED(data_buffer);
  UNUSED(size);
  // Note:: although do not support concurrent read and write of one same segment, try lock here. (support concurrent reads with_conflict_detection)
  TmpFileMetaHandle meta_handle;
  bool is_meta_exist = false;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id_, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to try get seg meta", KR(ret), K_(seg_id));
  } else if (is_meta_exist) {
    if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle is in valid", KR(ret), K(meta_handle));
    } else {
      SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
      if (OB_FAIL(inner_process_())) {
        LOG_WARN("fail to inner process", KR(ret), K(meta_handle));
      }
    }
  } else { // !is_meta_exist
    if (OB_FAIL(inner_process_())) {
      LOG_WARN("fail to inner process", KR(ret), K(meta_handle));
    }
  }
  return ret;
}

int ObSSTmpFileWriteCallback::inner_process_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(process_seg_meta())) {
    LOG_WARN("fail to process seg meta", KR(ret), KPC(this));
  } else if (OB_FAIL(process_seg_deletion())) {
    LOG_WARN("fail to process seg deletion", KR(ret), KPC(this));
  } else if (OB_FAIL(process_macro_cache())) {
    LOG_WARN("fail to process macro cache", KR(ret), KPC(this));
  }
  
  // Release write access immediately after I/O processing completes
  // This minimizes lock holding time and prevents lock contention
  if (OB_LIKELY(conflict_info_handle_.is_valid())) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(conflict_info_handle_.release_write_access())) {
      LOG_WARN("failed to release write access in inner_process", K(tmp_ret), K_(seg_id), KP(this));
      // Don't override ret if processing succeeded but write access release failed
      // The write access will be released again in destructor as fallback
    } else {
      write_access_released_ = true;  // Mark that write access has been released
      LOG_TRACE("write access released in inner_process", K_(seg_id), KP(this));
    }
  }
  
  return ret;
}

int ObSSTmpFileWriteCallback::set_ss_tmpfile_write_callback(
    const TmpFileSegId &seg_id,
    const TmpFileMetaHandle &meta_handle,
    const TmpFileConflictInfoHandle &conflict_info_handle,
    const ObSSTmpFileSegMetaOpType seg_meta_op_type,
    const ObSSTmpFileSegDeleteType seg_del_type,
    const bool need_put_or_update_macro_cache,
    const int64_t del_seg_valid_len,
    common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_handle.is_valid() ||
                  !conflict_info_handle.is_valid() ||
                  !seg_id.is_valid() ||
                  (((ObSSTmpFileSegDeleteType::REMOTE == seg_del_type) ||
                    (ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE == seg_del_type)) &&
                   (del_seg_valid_len <= 0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle), K(seg_id), K(seg_del_type), K(del_seg_valid_len));
  } else if (OB_FAIL(meta_handle_.assign(meta_handle))) {
    LOG_WARN("fail to assign meta handle", KR(ret), K(meta_handle));
  } else if (OB_FAIL(conflict_info_handle_.assign(conflict_info_handle))) {
    LOG_WARN("fail to assign conflict info handle", KR(ret), K(conflict_info_handle));
  } else {
    type_ = ObIOCallbackType::SS_TMP_FILE_CALLBACK;
    seg_id_ = seg_id;
    seg_meta_op_type_ = seg_meta_op_type;
    seg_del_type_ = seg_del_type;
    need_put_or_update_macro_cache_ = need_put_or_update_macro_cache;
    del_seg_valid_len_ = del_seg_valid_len;
    allocator_ = allocator;
  }
  return ret;
}

int ObSSTmpFileWriteCallback::process_seg_meta()
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else {
    switch (seg_meta_op_type_) {
      case ObSSTmpFileSegMetaOpType::INSERT: {
        ObSegmentFileManager &segment_file_manager = file_manager->get_segment_file_mgr();
        if (OB_FAIL(segment_file_manager.insert_meta(seg_id_, meta_handle_))) {
          LOG_WARN("fail to insert meta", KR(ret), K_(seg_id), K_(meta_handle));
        }
        break;
      }
      case ObSSTmpFileSegMetaOpType::DELETE: {
        ObSegmentFileManager &segment_file_manager = file_manager->get_segment_file_mgr();
        // Note: although there is no concurrent read and write, set is_in_local_ = false here.
        bool is_meta_exist = false;
        TmpFileMetaHandle meta_handle;
        if (OB_FAIL(segment_file_manager.try_get_seg_meta(seg_id_, meta_handle, is_meta_exist))) {
          LOG_WARN("fail to try get seg meta", KR(ret), K_(seg_id));
        } else if (!is_meta_exist) {
          // do nothing. may be gc concurrently
        } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("meta handle should not be invalid", KR(ret), K_(seg_id), K(meta_handle));
        } else {
          // Note: already wlock outside, cannot wlock again here
          meta_handle.get_tmpfile_meta()->is_in_local_ = false;
          // Note: segment_file_mgr_.delete_meta expect not to fail
          if (OB_FAIL(segment_file_manager.delete_meta(seg_id_))) {
            LOG_ERROR("fail to delete meta and conflict info", KR(ret), K_(seg_id));
          }
        }
        break;
      }
      case ObSSTmpFileSegMetaOpType::UPDATE: {
        ObSegmentFileManager &segment_file_manager = file_manager->get_segment_file_mgr();
        if (OB_FAIL(segment_file_manager.update_meta(seg_id_, meta_handle_))) {
          LOG_WARN("fail to update meta", KR(ret), K_(seg_id), K_(meta_handle));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected segment meta operation type", KR(ret), K_(seg_meta_op_type));
        break;
      }
    }
  }
  return ret;
}

int ObSSTmpFileWriteCallback::process_seg_deletion()
{
  int ret = OB_SUCCESS;
  switch (seg_del_type_) {
    case ObSSTmpFileSegDeleteType::NONE: {
      // do nothing
      break;
    }
    case ObSSTmpFileSegDeleteType::LOCAL: {
      // use OB_FAIL. if delete seg file failed, then make this whole write io failed.
      // so as to make local cache tmp file is always newest, and we can read local file in any time.
      if (OB_FAIL(delete_local_seg_file(seg_id_))) {
        LOG_WARN("fail to delete local seg file", KR(ret), K_(seg_id));
      }
      break;
    }
    case ObSSTmpFileSegDeleteType::REMOTE: {
      int tmp_ret = OB_SUCCESS;
      // TMP_FAIL, if push seg file to remove task failed, the seg_id will be deleted by SSTmpFileARemove thread finally
      if (OB_TMP_FAIL(push_del_seg_file_to_remove_queue(seg_id_, del_seg_valid_len_))) {
        LOG_WARN("fail to push seg file to remove queue", KR(tmp_ret), K_(seg_id), K_(del_seg_valid_len));
      }
      break;
    }
    case ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE: {
      // use OB_FAIL. if delete seg file failed, then make this whole write io failed.
      // so as to make local cache tmp file is always newest, and we can read local file in any time.
      if (OB_FAIL(delete_local_seg_file(seg_id_))) {
        LOG_WARN("fail to delete local seg file", KR(ret), K_(seg_id));
      }
      int tmp_ret = OB_SUCCESS;
      // TMP_FAIL, if push seg file to remove task failed, the seg_id will be deleted by SSTmpFileARemove thread finally
      if (OB_TMP_FAIL(push_del_seg_file_to_remove_queue(seg_id_, del_seg_valid_len_))) {
        LOG_WARN("fail to push seg file to remove queue", KR(tmp_ret), K_(seg_id), K_(del_seg_valid_len));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected segment delete operation type", KR(ret), K_(seg_del_type));
      break;
    }
  }
  return ret;
}


int ObSSTmpFileWriteCallback::process_macro_cache()
{
  int ret = OB_SUCCESS;
  if (need_put_or_update_macro_cache_) {
    MacroBlockId macro_id;
    macro_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
    macro_id.set_second_id(seg_id_.tmp_file_id_);
    macro_id.set_third_id(0);
    macro_id.set_fourth_id(seg_id_.segment_id_);
    ObSSMacroCacheInfo macro_cache_info;
    macro_cache_info.size_ = upper_align(meta_handle_.get_valid_length(), tmp_file::ObTmpFileGlobal::PAGE_SIZE);
    macro_cache_info.cache_type_ = ObSSMacroCacheType::TMP_FILE;
    macro_cache_info.is_write_cache_ = true;
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache mgr is null", KR(ret));
    } else if (OB_FAIL(macro_cache_mgr->put_or_update(macro_id, macro_cache_info))) {
      LOG_WARN("fail to put or update macro cache mgr", KR(ret), K(macro_id), K(macro_cache_info));
    }
  }
  return ret;
}

int ObSSTmpFileWriteCallback::delete_local_seg_file(const TmpFileSegId &del_seg_id)
{
  int ret = OB_SUCCESS;
  MacroBlockId segment_file_id;
  segment_file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  segment_file_id.set_second_id(del_seg_id.tmp_file_id_);
  segment_file_id.set_third_id(0);
  segment_file_id.set_fourth_id(del_seg_id.segment_id_);
  if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(segment_file_id, 0/*ls_epoch_id*/,
              true/*is_print_log*/, false/*is_del_seg_meta*/))) { // delete old version seg_file, cannot delete seg_meta
    LOG_WARN("fail to delete local file", KR(ret), K(segment_file_id));
  }
  return ret;
}

int ObSSTmpFileWriteCallback::push_del_seg_file_to_remove_queue(const TmpFileSegId &del_seg_id,
                                                                const int64_t &del_seg_valid_len)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager->get_segment_file_mgr().push_seg_file_to_remove_queue(del_seg_id, del_seg_valid_len))) {
    LOG_WARN("fail to push seg file to remove queue", KR(ret), K(del_seg_id), K(del_seg_valid_len));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
