/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_segment_file_manager.h"
#include "share/ob_io_device_helper.h"
#include "share/io/ob_io_manager.h"
#include "share/io/ob_io_define.h"
#include "storage/shared_storage/ob_file_op.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_tmp_file_io_callback.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/shared_storage/ob_ss_tmp_file_append_callback.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
/**
 * --------------------------------TmpFileConflictInfo------------------------------------
 */
TmpFileConflictInfo::TmpFileConflictInfo()
  : concurrent_access_lock_(common::ObLatchIds::TMP_FILE_CONFLICT_INFO_LOCK), is_writing_(false), active_readers_count_(0), ref_cnt_(0), segment_file_manager_(nullptr)
{
}

TmpFileConflictInfo::TmpFileConflictInfo(const bool is_writing, const int64_t active_readers_count)
  : concurrent_access_lock_(common::ObLatchIds::TMP_FILE_CONFLICT_INFO_LOCK), is_writing_(is_writing), active_readers_count_(active_readers_count), ref_cnt_(0), segment_file_manager_(nullptr)
{
}

void TmpFileConflictInfo::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void TmpFileConflictInfo::dec_ref_count()
{
  const int64_t ref_cnt = ATOMIC_SAF(&ref_cnt_, 1);
  if (0 == ref_cnt) {
    if (OB_ISNULL(segment_file_manager_)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED,
          "segment_file_manager_ is null, TmpFileConflictInfo must be allocated via ObSegmentFileManager",
          KPC(this));
    } else {
      // Use ObSegmentFileManager's conflict_info_allocator_ to free
      segment_file_manager_->free_conflict_info(this);
    }
  } else if (ref_cnt < 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tmp file conflict info's ref cnt should not be less than 0", KPC(this));
  }
}

void TmpFileConflictInfo::reset()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(concurrent_access_lock_);
  if (ref_cnt_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ref count > 0 when reset", KR(ret), KPC(this));
  }
  is_writing_ = false;
  active_readers_count_ = 0;
  ref_cnt_ = 0;
}

int TmpFileConflictInfo::try_acquire_write_access()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(concurrent_access_lock_);
  
  if (is_writing_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write-write conflict detected", KR(ret), KPC(this));
  } else if (active_readers_count_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write-read conflict: cannot acquire write access while readers are active", 
             KR(ret), KPC(this));
  } else {
    is_writing_ = true;
  }
  
  return ret;
}

int TmpFileConflictInfo::release_write_access()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(concurrent_access_lock_);

  if (is_writing_) {
    is_writing_ = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("write access has been released", KR(ret), KPC(this));
  }
  return ret;
}

int TmpFileConflictInfo::try_acquire_read_access()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(concurrent_access_lock_);
  
  if (is_writing_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read-write conflict: cannot acquire read access while write is in progress", 
             KR(ret), KPC(this));
  } else {
    active_readers_count_++;
  }
  
  return ret;
}

int TmpFileConflictInfo::release_read_access()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(concurrent_access_lock_);

  if (active_readers_count_ > 0) {
    active_readers_count_--;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("active readers count <= 0", KR(ret), KPC(this));
  }
  return ret;
}

/**
 * --------------------------------TmpFileMeta------------------------------------
 */
TmpFileMeta::TmpFileMeta()
  : lock_(common::ObLatchIds::TMP_FILE_META_LOCK), is_in_local_(true), is_sealed_(false), is_flushed_seg_sealed_(false),
    flushed_valid_length_(0), valid_length_(0), target_valid_length_(0), ref_cnt_(0),
    append_timestamp_us_(OB_INVALID_TIMESTAMP)
{
}

TmpFileMeta::TmpFileMeta(const bool is_in_local, const int32_t valid_length, const int32_t target_valid_length)
    : lock_(common::ObLatchIds::TMP_FILE_META_LOCK), is_in_local_(is_in_local), is_sealed_(false), is_flushed_seg_sealed_(false),
      flushed_valid_length_(0), valid_length_(valid_length), target_valid_length_(target_valid_length), ref_cnt_(0),
      append_timestamp_us_(ObTimeUtility::fast_current_time())
{
}

void TmpFileMeta::inc_ref_count()
{
  ATOMIC_INC(&ref_cnt_);
}

void TmpFileMeta::dec_ref_count()
{
  const int64_t ref_cnt = ATOMIC_SAF(&ref_cnt_, 1);
  if (0 == ref_cnt) {
    TmpFileMeta *meta_ptr = this;
    OB_DELETE(TmpFileMeta, attr, meta_ptr);
  } else if (ref_cnt < 0) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tmp file meta's ref cnt should not be less than 0", KPC(this));
  }
}

int TmpFileMeta::set_target_valid_length(const int32_t target_valid_length, const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  // defence for target_valid_length, which should not rollback
  if (OB_UNLIKELY(target_valid_length < target_valid_length_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected target_valid_length",  KR(ret), K(target_valid_length),
                  K(target_valid_length_), K(macro_id));
  } else if (OB_UNLIKELY(target_valid_length == target_valid_length_)) {
    // Retry scenarios may hit this equality; do not treat as error.
    LOG_WARN("target_valid_length equal to target_valid_length_",
                  K(target_valid_length), K(target_valid_length_), K(macro_id));
  } else {
    target_valid_length_ = target_valid_length;
  }
  return ret;
}

// update flushed_valid_length and is_flushed_seg_sealed atomically
void TmpFileMeta::set_flush_info(const int32_t flushed_valid_length, const bool is_flushed_seg_sealed)
{
  SpinWLockGuard guard(lock_);
  // defence for flushed_valid_length and is_flushed_seg_sealed:
  // 1. flushed_valid_length should not rollback.
  // 2. is_flushed_seg_sealed is passing from is_sealed_. hence, if is_sealed_ is false,
  //    is_flushed_seg_sealed should not be true.
  if (OB_UNLIKELY((flushed_valid_length < flushed_valid_length_) ||
                  (is_flushed_seg_sealed && !is_sealed_))) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected flushed_valid_length or is_flushed_seg_sealed",
                  K(flushed_valid_length), K(flushed_valid_length_), K(is_flushed_seg_sealed), K(is_sealed_));
  } else {
    flushed_valid_length_ = flushed_valid_length;
    is_flushed_seg_sealed_ = is_flushed_seg_sealed;
  }
}

void TmpFileMeta::set_sealed()
{
  is_sealed_ = true;
}

int TmpFileMeta::check_tmp_file_appending(ObSSTmpFileAppendCallback &callback)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  // Note: get flushed_valid_length_ and is_flushed_seg_sealed in lock to guarantee atomicicy
  if (is_tmp_file_appending_nolock(flushed_valid_length_)) {
    if (OB_FAIL(callback.process_on_appending(*this))) {
      LOG_WARN("fail to process on appending", KR(ret), KPC(this), K(callback));
    }
  } else {
    if (OB_FAIL(callback.process_on_non_appending(*this))) {
      LOG_WARN("fail to process on non appending", KR(ret), KPC(this), K(callback));
    }
  }
  return ret;
}

bool TmpFileMeta::is_tmp_file_appending(const int64_t flushed_valid_len)
{
  SpinWLockGuard guard(lock_);
  return is_tmp_file_appending_nolock(flushed_valid_len);
}

bool TmpFileMeta::is_tmp_file_appending_nolock(const int64_t flushed_valid_len)
{
  // flushed_valid_len is not equal to target_valid_length, means seg file has been appended or seg file is appending
  return (flushed_valid_len != target_valid_length_);
}

void TmpFileMeta::reset()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (ref_cnt_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ref count > 0 when reset", KR(ret), KPC(this));
  }
  is_in_local_ = true;
  is_sealed_ = false;
  is_flushed_seg_sealed_ = false;
  flushed_valid_length_ = 0;
  valid_length_ = 0;
  target_valid_length_ = 0;
  ref_cnt_ = 0;
  append_timestamp_us_ = OB_INVALID_TIMESTAMP;
}

bool TmpFileMeta::is_valid() const
{
  return ((valid_length_ > 0) && (OB_INVALID_TIMESTAMP != append_timestamp_us_));
}

/**
 * --------------------------------TmpFileConflictInfoHandle------------------------------------
 */
TmpFileConflictInfoHandle::TmpFileConflictInfoHandle() : ObPtrHandle<TmpFileConflictInfo>()
{
}

TmpFileConflictInfoHandle::~TmpFileConflictInfoHandle()
{
  reset();
}

void TmpFileConflictInfoHandle::reset()
{
  ObPtrHandle<TmpFileConflictInfo>::reset();
}

int TmpFileConflictInfoHandle::assign(const TmpFileConflictInfoHandle &other)
{
  return ObPtrHandle<TmpFileConflictInfo>::assign(other);
}

int TmpFileConflictInfoHandle::try_acquire_write_access()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict info is null", KR(ret));
  } else if (OB_FAIL(ptr_->try_acquire_write_access())) {
    LOG_WARN("fail to acquire write access", KR(ret), KPC_(ptr));
  }
  return ret;
}

int TmpFileConflictInfoHandle::release_write_access()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict info is null when releasing write access", KR(ret));
  } else if (OB_FAIL(ptr_->release_write_access())) {
    LOG_WARN("failed to release write access", KR(ret), KPC_(ptr));
  }
  return ret;
}

int TmpFileConflictInfoHandle::try_acquire_read_access()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict info is null", KR(ret));
  } else if (OB_FAIL(ptr_->try_acquire_read_access())) {
    LOG_WARN("fail to acquire read access", KR(ret), KPC_(ptr));
  }
  return ret;
}

int TmpFileConflictInfoHandle::release_read_access()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict info is null when releasing read access", KR(ret));
  } else if (OB_FAIL(ptr_->release_read_access())) {
    LOG_WARN("failed to release read access", KR(ret), KPC_(ptr));
  }
  return ret;
}

/**
 * --------------------------------TmpFileMetaHandle------------------------------------
 */
TmpFileMetaHandle::TmpFileMetaHandle() : ObPtrHandle<TmpFileMeta>()
{
}

TmpFileMetaHandle::~TmpFileMetaHandle()
{
  reset();
}

void TmpFileMetaHandle::reset()
{
  ObPtrHandle<TmpFileMeta>::reset();
}

int TmpFileMetaHandle::set_tmpfile_meta(
    const bool is_in_local,
    const int32_t valid_length,
    const int32_t target_valid_length)
{
  int ret = OB_SUCCESS;
  reset();
  ObMemAttr attr(MTL_ID(), "TmpFileMeta");
  // because deleting tmp file is async, when the sql is completed, tmp file has not been deleted, and the farm will report memory leak
  SET_IGNORE_MEM_VERSION(attr);
  TmpFileMeta *meta_ptr = nullptr;
  if (OB_UNLIKELY((valid_length < 0) || (target_valid_length < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(valid_length), K(target_valid_length));
  } else if (OB_ISNULL(meta_ptr = OB_NEW(TmpFileMeta, attr, is_in_local, valid_length, target_valid_length))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret));
  } else {
    set_ptr(meta_ptr);
  }
  return ret;
}

int TmpFileMetaHandle::assign(const TmpFileMetaHandle &other)
{
  return ObPtrHandle<TmpFileMeta>::assign(other);
}

// Note: already wlock by caller, cannot wlock again here
void TmpFileMetaHandle::update_tmpfile_meta(const bool is_in_local, const int64_t valid_length)
{
  if (OB_NOT_NULL(ptr_)) {
    ptr_->is_in_local_ = is_in_local;
    ptr_->valid_length_ = valid_length;
    ptr_->append_timestamp_us_ = ObTimeUtility::fast_current_time();
  }
}

int TmpFileMetaHandle::set_target_valid_length(const int32_t target_valid_length, const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta is null", KR(ret));
  } else if (OB_FAIL(ptr_->set_target_valid_length(target_valid_length, macro_id))) {
    LOG_WARN("fail to set target_valid_length", KR(ret), K(target_valid_length), K(macro_id));
  }
  return ret;
}

void TmpFileMetaHandle::set_sealed()
{
  if (OB_NOT_NULL(ptr_)) {
    ptr_->set_sealed();
  }
}

int TmpFileMetaHandle::check_tmp_file_appending(ObSSTmpFileAppendCallback &callback)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta is null", KR(ret));
  } else if (OB_FAIL(ptr_->check_tmp_file_appending(callback))) {
    LOG_WARN("fail to check tmp file appending", K(callback));
  }
  return ret;
}


ObSegmentFileManager::GetWildTmpFileSegIdFunc::GetWildTmpFileSegIdFunc(const int64_t tmp_file_id)
  : tmp_file_id_(tmp_file_id), wild_seg_ids_()
{
  wild_seg_ids_.set_attr(ObMemAttr(MTL_ID(), "GetWildTmpF"));
}

int ObSegmentFileManager::GetWildTmpFileSegIdFunc::operator()(
    const HashMapPair<TmpFileSegId, TmpFileMetaHandle> &entry)
{
  int ret = OB_SUCCESS;
  const TmpFileSegId &cur_seg_id = entry.first;
  if (cur_seg_id.tmp_file_id_ == tmp_file_id_) {
    if (OB_FAIL(wild_seg_ids_.push_back(cur_seg_id))) {
      LOG_WARN("fail to push back", KR(ret), K(cur_seg_id));
    }
  }
  return ret;
}

ObSegmentFileManager::GetWildTmpFileSegIdFuncForConflict::GetWildTmpFileSegIdFuncForConflict(const int64_t tmp_file_id)
  : tmp_file_id_(tmp_file_id), wild_seg_ids_()
{
  wild_seg_ids_.set_attr(ObMemAttr(MTL_ID(), "GetWildConf"));
}

int ObSegmentFileManager::GetWildTmpFileSegIdFuncForConflict::operator()(
    const HashMapPair<TmpFileSegId, TmpFileConflictInfoHandle> &entry)
{
  int ret = OB_SUCCESS;
  const TmpFileSegId &cur_seg_id = entry.first;
  if (cur_seg_id.tmp_file_id_ == tmp_file_id_) {
    if (OB_FAIL(wild_seg_ids_.push_back(cur_seg_id))) {
      LOG_WARN("fail to push back", KR(ret), K(cur_seg_id));
    }
  }
  return ret;
}

/**
 * --------------------------------ObSSTmpFileAppendParam------------------------------------
 */
ObSSTmpFileAppendParam::ObSSTmpFileAppendParam()
  : arena_allocator_(), need_write_io_(true), need_free_file_size_(true), free_file_size_(0)
{
  arena_allocator_.set_attr(ObMemAttr(MTL_ID(), "TmpFileWrite"));
}

ObSSTmpFileAppendParam::ObSSTmpFileAppendParam(
    const bool need_write_io,
    const bool need_free_file_size,
    const int64_t free_file_size)
  : arena_allocator_(), need_write_io_(need_write_io), need_free_file_size_(need_free_file_size),
    free_file_size_(free_file_size)
{
  arena_allocator_.set_attr(ObMemAttr(MTL_ID(), "TmpFileWrite"));
}


/**
 * --------------------------------ObSegmentFileManager------------------------------------
 */
ObSegmentFileManager::ObSegmentFileManager()
  : is_inited_(false), is_stop_(false), file_manager_(nullptr), allocator_(),
    seg_meta_map_(), seg_conflict_info_map_(), gc_segment_file_task_(), clean_old_version_tmp_file_task_(),
    unsealed_seg_file_remove_queue_(), conflict_info_allocator_(), conflict_info_live_count_(0)
{
}

ObSegmentFileManager::~ObSegmentFileManager() { destroy(); }

int ObSegmentFileManager::init(ObTenantFileManager *file_manager)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr attr(MTL_ID(), "SegMetaMap");
  const int64_t MEM_LIMIT = 1 * 1024 * 1024 * 1024LL; // 1GB
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("segment file manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_manager));
  } else if (OB_FAIL(seg_meta_map_.create(ObBaseFileManager::OB_DEFAULT_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create seg meta map", KR(ret));
  } else if (OB_FAIL(seg_conflict_info_map_.create(ObBaseFileManager::OB_DEFAULT_BUCKET_NUM, attr))) {
    LOG_WARN("fail to create seg conflict info map", KR(ret));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE, "SegFileMGR", MTL_ID(), MEM_LIMIT))) {
    LOG_WARN("fail to init allocator", KR(ret), K(MEM_LIMIT));
  } else if (OB_FAIL(conflict_info_allocator_.init(sizeof(TmpFileConflictInfo), "TmpConfInfo", MTL_ID()))) {
    LOG_WARN("fail to init conflict_info_allocator", KR(ret), "obj_size", sizeof(TmpFileConflictInfo));
  } else if (OB_FAIL(gc_segment_file_task_.init(this))) {
    LOG_WARN("fail to init gc unsealed tmpfile task", KR(ret));
  } else if (OB_FAIL(clean_old_version_tmp_file_task_.init(this))) {
    LOG_WARN("fail to init clean old version tmp file task", KR(ret));
  } else {
    file_manager_ = file_manager;
    conflict_info_live_count_ = 0;
    is_inited_ = true;
    LOG_INFO("succ to init segment file manager");
  }
  return ret;
}

int ObSegmentFileManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(gc_segment_file_task_.start())) {
    LOG_WARN("fail to start gc unsealed tmpfile task", KR(ret));
  } else if (OB_FAIL(clean_old_version_tmp_file_task_.start())) {
    LOG_WARN("fail to start clean old version tmp file task", KR(ret));
  } else {
    LOG_INFO("succ to start segment file manager");
  }
  return ret;
}

void ObSegmentFileManager::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  file_manager_ = nullptr;
  clean_old_version_tmp_file_task_.destroy();
  gc_segment_file_task_.destroy();

  // Phase 1: drain unsealed seg file remove queue
  const int64_t queue_size = unsealed_seg_file_remove_queue_.size();
  for (int64_t i = 0; OB_SUCC(ret) && (i < queue_size); ++i) {
    ObLink *ptr = nullptr;
    ObUnsealedSegFile *seg_file = nullptr;
    if (OB_FAIL(unsealed_seg_file_remove_queue_.pop(ptr))) {
      LOG_WARN("fail to pop remove task", KR(ret), K(i), K(queue_size));
    } else {
      seg_file = static_cast<ObUnsealedSegFile *>(ptr);
      OB_DELETEx(ObUnsealedSegFile, &allocator_, seg_file);
    }
  }

  // Phase 2: destroy map so all handles in map release their refs (dec_ref_count)
  seg_conflict_info_map_.destroy();

  // Phase 3: wait destruction of all TmpFileConflictInfo (handles in IO callbacks may still hold refs)
  while (ATOMIC_LOAD(&conflict_info_live_count_) > 0) {
    if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {  // 10s
      LOG_WARN("there still exists TmpFileConflictInfo which has not been destructed",
               "conflict_info_live_count", ATOMIC_LOAD(&conflict_info_live_count_));
    }
    ob_usleep(100 * 1000L);  // 100ms
  }
  conflict_info_allocator_.destroy();

  allocator_.destroy();
  seg_meta_map_.destroy();
}

void ObSegmentFileManager::stop()
{
  is_stop_ = true;
  clean_old_version_tmp_file_task_.stop();
  gc_segment_file_task_.stop();
}

void ObSegmentFileManager::wait()
{
  clean_old_version_tmp_file_task_.wait();
  gc_segment_file_task_.wait();
}



int ObSegmentFileManager::async_pread_file(
    const ObStorageObjectReadInfo &read_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectReadInfo new_read_info(read_info);
  const MacroBlockId &file_id = read_info.macro_block_id_;
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(!new_read_info.is_valid() || !file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_read_info), K(file_id));
  } else {
    // OPTIMIZATION: Check if this is a sealed file
    // Sealed files (both local and remote) don't need conflict detection because:
    // 1. They are read-only (no append writes allowed)
    // 2. Skipping conflict detection saves memory (no conflict_info needed)
    bool is_meta_exist = false;
    TmpFileMetaHandle meta_handle;
    const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());

    if (OB_FAIL(try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
      LOG_WARN("fail to try get seg meta", KR(ret), K(seg_id));
    } else if (!is_meta_exist || (meta_handle.is_valid() && meta_handle.is_sealed())) {
      // Skip conflict detection for sealed files or files without meta
      if (OB_FAIL(file_manager_->async_pread_file(new_read_info, object_handle))) {
        LOG_WARN("fail to async pread file without conflict detection", KR(ret), K(new_read_info), K(object_handle));
      }
    } else if (!meta_handle.is_valid()) {
      // meta exist but meta_handle is invalid
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle is invalid", KR(ret), K(seg_id), K(meta_handle));
    } else {
      // For unsealed files, use conflict detection
      TmpFileConflictInfoHandle conflict_info_handle;
      
      if (OB_FAIL(get_or_create_conflict_info_handle(seg_id, conflict_info_handle, ObTmpFileAccessType::READ_ACCESS))) {
        LOG_WARN("fail to get or create conflict info handle", KR(ret), K(seg_id));
      } else { 
        if (OB_FAIL(construct_tmp_file_read_callback(seg_id, conflict_info_handle, new_read_info))) {
          LOG_WARN("fail to construct tmp file conflict callback", KR(ret), K(seg_id));
        } else if (OB_FAIL(file_manager_->async_pread_file(new_read_info, object_handle))) {
          LOG_WARN("fail to async pread file with conflict detection", KR(ret), K(new_read_info), K(object_handle));
          // Cleanup wrapped callback only when async_pread_file fails after successful wrapping
          if (OB_NOT_NULL(new_read_info.io_callback_)) {
            if (ObIOCallbackType::SS_TMP_FILE_READ_CALLBACK == new_read_info.io_callback_->get_type()) {
              ObSSTmpFileReadCallback::free_io_callback_and_detach_original(new_read_info.io_callback_);
            } else {
              LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected io callback", "callback_type", new_read_info.io_callback_->get_type());
            }
          }
        }
      }
    }
  }
  
  return ret;
}

int ObSegmentFileManager::async_append_file(
  const ObStorageObjectWriteInfo &write_info,
  ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle ori_object_handle(object_handle);  // when aio write failed, object_handle will be reset
  ObStorageObjectWriteInfo new_write_info(write_info);
  const MacroBlockId &file_id = object_handle.get_macro_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager is not inited", KR(ret));
  } else if (OB_UNLIKELY((!SSObjUtil::is_tmp_file(file_id)) ||
                         !new_write_info.is_valid() || !file_id.is_valid() ||
                         new_write_info.io_desc_.is_write_through() ||
                         ((new_write_info.size_ % tmp_file::ObTmpFileGlobal::PAGE_SIZE) != 0) || // tmp_file is 8KB aligned
                         (new_write_info.size_ > OB_DEFAULT_MACRO_BLOCK_SIZE))) { // tmp_file segment <= 2MB
    // disable user layer to set write_through/back, only io layer can determine write_through/back
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "storage object type", STI(file_id.storage_object_type()), K(new_write_info), K(object_handle));
  } else {
    ObSSTmpFileAppendParam append_param(true/*need_write_io*/, true/*need_free_file_size*/, new_write_info.size_/*free_file_size*/);
    if (OB_FAIL(get_or_create_conflict_info_handle(file_id, append_param, ObTmpFileAccessType::WRITE_ACCESS))) {
      LOG_WARN("fail to get or create conflict info handle", KR(ret), K(file_id));
    }
    
    if (OB_FAIL(ret)) {
      // do nothing, already logged before
    } else if ((SSObjUtil::is_tmp_file(file_id)) &&
        (OB_DEFAULT_MACRO_BLOCK_SIZE == new_write_info.size_) &&
        (new_write_info.io_desc_.is_sealed())) { // case 1: 2MB sealed tmp file
      // Dual write mode: write to local cache or object storage based on space availability
      if (OB_FAIL(async_write_dual(new_write_info, ori_object_handle, object_handle, append_param))) {
        LOG_WARN("fail to async write dual", KR(ret), K(new_write_info), K(object_handle));
      }
    } else { // case 2: is_write_through = false
      bool is_meta_exist = false;
      TmpFileMetaHandle meta_handle;
      const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
      if (OB_FAIL(try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
        LOG_WARN("fail to try get seg meta", KR(ret), K(seg_id));
      } else if (is_meta_exist) { // case 2.1: is_meta_exist = true
        if (OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("meta handle is invalid", KR(ret), K(seg_id), K(meta_handle));
        } else {
          SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
          if (OB_FAIL(handle_write_on_meta_exist(meta_handle, new_write_info, object_handle, append_param))) {
            LOG_WARN("fail to handle write on meta exist", KR(ret), K(meta_handle), K(new_write_info), K(object_handle));
          } else if (append_param.need_write_io_) {
            if (OB_FAIL(aio_write_with_create_parent_dir(append_param, new_write_info, ori_object_handle, object_handle))) {
              LOG_WARN("fail to aio write with create parent dir", KR(ret), K(new_write_info), K(object_handle), K(append_param));
            }
          }
        }
      } else { // case 2.2: is_meta_exist = false
        if (OB_FAIL(handle_write_on_meta_not_exist(new_write_info, object_handle, append_param))) {
          LOG_WARN("fail to handle write on meta not exist", KR(ret), K(new_write_info), K(object_handle));
        } else if (append_param.need_write_io_) {
          if (OB_FAIL(aio_write_with_create_parent_dir(append_param, new_write_info, ori_object_handle, object_handle))) {
            LOG_WARN("fail to aio write with create parent dir", KR(ret), K(new_write_info), K(object_handle), K(append_param));
          }
        }
      }
    }
    // IOManger::aio_write failed or failed before IOManger::aio_write, free memory of ObSSTmpFileIOCallback
    if (OB_FAIL(ret) && OB_NOT_NULL(new_write_info.io_callback_)) {
      if (ObIOCallbackType::SS_TMP_FILE_CALLBACK == new_write_info.io_callback_->get_type()) {
        free_io_callback<ObSSTmpFileWriteCallback>(new_write_info.io_callback_);
      } else {
        LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected io callback", "callback_type", new_write_info.io_callback_->get_type());
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::insert_meta(const TmpFileSegId &seg_id, const TmpFileMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle));
  } else if (OB_FAIL(seg_meta_map_.set_refactored(seg_id, meta_handle, false/*overwrite*/))) {
    LOG_WARN("fail to set refactored seg meta", KR(ret), K(seg_id), K(meta_handle));
  }
  return ret;
}


int ObSegmentFileManager::delete_meta(const TmpFileSegId &seg_id)
{
  int ret = OB_SUCCESS;
  // first delete meta
  if (OB_FAIL(seg_meta_map_.erase_refactored(seg_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret, maybe meta is not exist or meta has already been deleted
    } else {
      LOG_WARN("fail to erase refactored seg meta", KR(ret), K(seg_id));
    }
    // then delete conflict info
  } else if (OB_FAIL(seg_conflict_info_map_.erase_refactored(seg_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret, maybe conflict info is not exist or conflict info has already been deleted
    } else {
      LOG_WARN("fail to erase refactored seg conflict info", KR(ret), K(seg_id));
    }
  }
  
  return ret;
}


// This function is specially designed for ObSSTmpFileRemoveManager::delete_reboot_gc_files_ Scan GC.
// It deletes metas and conflict_infos of the tmp file that has already been GC.
// Note: why need to delete wild meta?
// When concurrent file writes and deletions occur on temporary files, a flush I/O may be initiated first.  
// If the file is deleted during the I/O operation, metadata and conflict info residue from the flush might persist.  
// The `delete_wild_meta` serves as a safeguard to clean up such orphaned metadata and conflict info.  
int ObSegmentFileManager::delete_wild_meta(const int64_t tmp_file_id)
{
  int ret = OB_SUCCESS;
  
  // Delete wild metas 
  GetWildTmpFileSegIdFunc meta_func(tmp_file_id);
  if (OB_FAIL(seg_meta_map_.foreach_refactored(meta_func))) {
    LOG_WARN("fail to foreach refactored meta map", KR(ret), K(meta_func));
  } else {
    const int64_t wild_meta_cnt = meta_func.wild_seg_ids_.count();
    for (int64_t i = 0; (i < wild_meta_cnt) && OB_SUCC(ret); ++i) {
      const TmpFileSegId &cur_seg_id = meta_func.wild_seg_ids_.at(i);
      if (OB_FAIL(seg_meta_map_.erase_refactored(cur_seg_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // ignore ret
        } else {
          LOG_WARN("fail to erase refactored seg meta", KR(ret), K(cur_seg_id));
        }
      }
    }
    if (wild_meta_cnt > 0) {
      LOG_INFO("finish to delete wild seg metas", KR(ret), K(wild_meta_cnt), "wild_seg_ids",
               meta_func.wild_seg_ids_);
    }
  } 

  // Delete wild conflict infos
  GetWildTmpFileSegIdFuncForConflict conflict_func(tmp_file_id);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(seg_conflict_info_map_.foreach_refactored(conflict_func))) {
    LOG_WARN("fail to foreach refactored conflict info map", KR(ret), K(conflict_func));
  } else {
    const int64_t wild_conflict_cnt = conflict_func.wild_seg_ids_.count();
    for (int64_t i = 0; (i < wild_conflict_cnt) && OB_SUCC(ret); ++i) {
      const TmpFileSegId &cur_seg_id = conflict_func.wild_seg_ids_.at(i);
      if (OB_FAIL(seg_conflict_info_map_.erase_refactored(cur_seg_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // ignore ret
        } else {
          LOG_WARN("fail to erase refactored seg conflict info", KR(ret), K(cur_seg_id));
        }
      }
    }
    if (wild_conflict_cnt > 0) {
      LOG_INFO("finish to delete wild conflict infos", KR(ret), K(wild_conflict_cnt), "wild_seg_ids",
               conflict_func.wild_seg_ids_);
    }
  }
  
  return ret;
}

int ObSegmentFileManager::update_meta(const TmpFileSegId &seg_id, const TmpFileMetaHandle &meta_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle));
  } else {
    ObSegMetaUpdateOp update_callback(meta_handle);
    if (OB_FAIL(seg_meta_map_.atomic_refactored(seg_id, update_callback))) {
      LOG_WARN("fail to update seg meta map", KR(ret));
    }
  }
  return ret;
}

int ObSegmentFileManager::set_sealed(const TmpFileSegId &seg_id)
{
  int ret = OB_SUCCESS;
  ObSetSealedCallback set_sealed_callback;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!seg_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid seg id", KR(ret), K(seg_id));
  } else if (OB_FAIL(seg_meta_map_.atomic_refactored(seg_id, set_sealed_callback))) {
    LOG_WARN("fail to atomic refactored seg meta", KR(ret), K(seg_id));
  }
  return ret;
}

int ObSegmentFileManager::try_get_seg_meta(
    const TmpFileSegId &seg_id,
    TmpFileMetaHandle &meta_handle,
    bool &is_meta_exist)
{
  int ret = OB_SUCCESS;
  is_meta_exist = false;
  meta_handle.reset();
  if (OB_FAIL(seg_meta_map_.get_refactored(seg_id, meta_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get refactored", KR(ret), K(seg_id));
    }
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta handle is invalid", KR(ret), K(meta_handle));
  } else {
    is_meta_exist = true;
  }
  return ret;
}

int ObSegmentFileManager::try_get_seg_conflict_info(
  const TmpFileSegId &seg_id,
  TmpFileConflictInfoHandle &conflict_info_handle,
  bool &is_conflict_info_exist)
{
  int ret = OB_SUCCESS;

  is_conflict_info_exist = false;
  conflict_info_handle.reset();
  if (OB_FAIL(seg_conflict_info_map_.get_refactored(seg_id, conflict_info_handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get refactored", KR(ret), K(seg_id));
    }
  } else if (OB_UNLIKELY(!conflict_info_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conflict info handle is invalid", KR(ret), K(conflict_info_handle));
  } else {
    is_conflict_info_exist = true;
  }

  return ret;
}

int ObSegmentFileManager::construct_tmp_file_read_callback(const TmpFileSegId &seg_id, TmpFileConflictInfoHandle &conflict_info_handle, ObStorageObjectReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  ObSSTmpFileReadCallback *tmp_file_read_callback = nullptr;
  bool read_access_acquired = false;
  bool callback_take_over_read_access = false;
  
  if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_UNLIKELY(!conflict_info_handle.is_valid() || !seg_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(seg_id), K(conflict_info_handle));
  } else {
    // First acquire read access before creating callback
    if (OB_FAIL(conflict_info_handle.try_acquire_read_access())) {
      LOG_WARN("failed to acquire read access for seg", KR(ret), K(seg_id));
    } else {
      read_access_acquired = true;
    }
    
    if (OB_SUCC(ret)) {
      ObIAllocator &io_callback_allocator = file_manager_->get_io_callback_allocator();
      if (OB_ISNULL(tmp_file_read_callback = static_cast<ObSSTmpFileReadCallback *>(
                           io_callback_allocator.alloc(sizeof(ObSSTmpFileReadCallback))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc read callback memory", KR(ret));
      } else if (FALSE_IT(tmp_file_read_callback = new (tmp_file_read_callback) ObSSTmpFileReadCallback(&io_callback_allocator, read_info.io_callback_, read_info.buf_))) {
      } else if (OB_FAIL(tmp_file_read_callback->set_ss_tmpfile_read_callback(seg_id, this, &io_callback_allocator, conflict_info_handle))) {
        LOG_WARN("fail to set read callback", KR(ret), K(seg_id));
      } else {
        read_info.io_callback_ = static_cast<common::ObIOCallback *>(tmp_file_read_callback);
        callback_take_over_read_access = true;
      }
    }
    
    // Clean up on error
    if (OB_FAIL(ret)) {
      if (read_access_acquired && !callback_take_over_read_access) {
        int tmp_ret = OB_SUCCESS;
        if (OB_UNLIKELY(!conflict_info_handle.is_valid())) {
          LOG_ERROR("conflict info handle is invalid when releasing read access in error cleanup", K(seg_id));
        } else if (OB_TMP_FAIL(conflict_info_handle.release_read_access())) {
          LOG_WARN("failed to release read access in error cleanup", K(tmp_ret), K(seg_id));
        }
      }

      if (OB_NOT_NULL(tmp_file_read_callback)) {
        if (OB_UNLIKELY(callback_take_over_read_access)) {
          // Note: callback_take_over_read_access expected to be false when tmp_file_read_callback is not null
          // this is unexpected error
          int tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("callback has been set to read_info.io_callback_, need to detach original", KR(tmp_ret), K(seg_id), K(callback_take_over_read_access));
        } else {
          // Callback was allocated but not set to read_info.io_callback_, just free it
          common::ObIOCallback *callback_ptr = static_cast<common::ObIOCallback *>(tmp_file_read_callback);
          free_io_callback<ObSSTmpFileReadCallback>(callback_ptr);
        }
      }
    }
  }
  
  return ret;
}

int ObSegmentFileManager::allocate_conflict_info_handle(
    const bool is_writing,
    const int64_t active_readers_count,
    TmpFileConflictInfoHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  
  void *buf = conflict_info_allocator_.alloc();
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory from conflict_info_allocator", KR(ret), 
             "obj_size", sizeof(TmpFileConflictInfo));
  } else {
    ATOMIC_INC(&conflict_info_live_count_);
    TmpFileConflictInfo *ptr = new (buf) TmpFileConflictInfo(is_writing, active_readers_count);
    ptr->set_segment_file_manager(this);
    handle.set_ptr(ptr);
  }
  
  return ret;
}

void ObSegmentFileManager::free_conflict_info(TmpFileConflictInfo *ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ptr is null when freeing conflict info", KR(ret));
  } else {
    ptr->~TmpFileConflictInfo();
    conflict_info_allocator_.free(ptr);
    ATOMIC_DEC(&conflict_info_live_count_);
  }
}

int ObSegmentFileManager::get_or_create_conflict_info_handle(
    const TmpFileSegId &seg_id,
    TmpFileConflictInfoHandle &conflict_info_handle,
    const ObTmpFileAccessType &access_type)
{
  int ret = OB_SUCCESS;
  bool is_conflict_info_exist = false;
  
  conflict_info_handle.reset();
  
  if (OB_UNLIKELY(!seg_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid seg_id", KR(ret), K(seg_id));
  } else if (OB_FAIL(try_get_seg_conflict_info(seg_id, conflict_info_handle, is_conflict_info_exist))) {
    LOG_WARN("fail to try get seg conflict info", KR(ret), K(seg_id));
  } else if (!is_conflict_info_exist) {
    if (ObTmpFileAccessType::READ_ACCESS == access_type) {
      // Note: read must after write, so conflict info must be exist, if conflict info not exist 
      // it could be that the file has been removed by reboot GC
      ret = OB_OBJECT_NOT_EXIST;
      LOG_WARN("read conflict info is not exist, file may have been removed by GC", KR(ret), K(seg_id));
    } else if (ObTmpFileAccessType::WRITE_ACCESS == access_type) { 
      // Create new conflict info
      TmpFileConflictInfoHandle new_handle;
      if (OB_FAIL(allocate_conflict_info_handle(false/*is_writing*/, 0/*active_readers_count*/, new_handle/*conflict_info_handle*/))) {
        LOG_WARN("fail to allocate conflict info", KR(ret), K(seg_id));
      } else if (OB_UNLIKELY(!new_handle.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid conflict info handle", KR(ret), K(seg_id), K(new_handle));
      } else if (OB_FAIL(seg_conflict_info_map_.set_refactored(seg_id, new_handle, false/*overwrite*/))) {
        if (OB_HASH_EXIST == ret) {
          // Concurrent race condition: Between try_get_seg_conflict_info() and set_refactored(),
          // another write thread has already inserted the conflict info for the same seg_id in seg_conflict_info_map_.
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
          LOG_WARN("conflict info already created by concurrent write thread", KR(ret), K(seg_id));
        } else {
          LOG_WARN("fail to set refactored conflict info map", KR(ret), K(seg_id));
        }
      } else {
        conflict_info_handle = new_handle;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("access_type is not support", KR(ret), K(seg_id));
    }
  } else { // is_conflict_info_exist == true
    // Another thread already created the conflict info, use the existing handle
    // The conflict_info_handle has already been set by try_get_seg_conflict_info()
    // No additional work needed here - just proceed with conflict detection
  }
  
  return ret;
}

int ObSegmentFileManager::get_or_create_conflict_info_handle(
    const MacroBlockId &macro_id,
    ObSSTmpFileAppendParam &append_param,
    const ObTmpFileAccessType &access_type)
{
  int ret = OB_SUCCESS;
  const TmpFileSegId seg_id(macro_id.second_id(), macro_id.fourth_id());
  if (OB_FAIL(get_or_create_conflict_info_handle(seg_id, append_param.conflict_info_handle_, ObTmpFileAccessType::WRITE_ACCESS))) {
    LOG_WARN("fail to get or create conflict info handle for write", KR(ret), K(seg_id));
  }
  
  return ret;
}

int ObSegmentFileManager::push_seg_file_to_remove_queue(const TmpFileSegId &seg_id, const int64_t valid_length)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager has not been inited", KR(ret), K_(is_inited));
  } else {
    ObUnsealedSegFile *seg_file = nullptr;
    if (OB_ISNULL(seg_file = OB_NEWx(ObUnsealedSegFile, &allocator_, seg_id, valid_length))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate ObUnsealedSegFile", KR(ret));
    } else if (OB_FAIL(unsealed_seg_file_remove_queue_.push(seg_file))) {
      LOG_WARN("fail to push remove task", KR(ret), K(seg_id), K(valid_length));
    }
    // if push seg file to remove queue failed, the seg_file will be deleted by SSTmpFileARemove thread finally
    if (OB_FAIL(ret)) {
      OB_DELETEx(ObUnsealedSegFile, &allocator_, seg_file);
    }
  }
  return ret;
}

int ObSegmentFileManager::exec_remove_task_once()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager has not been inited", KR(ret), K_(is_inited));
  }
  const int64_t start_ts = ObTimeUtility::fast_current_time();
  ObSEArray<MacroBlockId, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> file_ids;
  const int64_t queue_size = unsealed_seg_file_remove_queue_.size();
  for (int64_t i = 0; OB_SUCC(ret) && !is_stop_ && (i < queue_size); ++i) {
    ObUnsealedSegFile *seg_file = nullptr;
    ObLink *ptr = nullptr;
    if (OB_FAIL(unsealed_seg_file_remove_queue_.pop(ptr))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;  // ignore ret
        break;
      } else {
        LOG_WARN("fail to pop seg file remove task", KR(ret));
      }
    } else if (OB_ISNULL(ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr is null", KR(ret), KP(ptr));
    } else {
      seg_file = static_cast<ObUnsealedSegFile *>(ptr);
      MacroBlockId file_id;
      file_id.set_storage_object_type((uint64_t)ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE);
      file_id.set_second_id(seg_file->seg_id_.tmp_file_id_);
      file_id.set_third_id(0);
      file_id.set_fourth_id(seg_file->seg_id_.segment_id_);
      file_id.set_fifth_id(seg_file->valid_length_);
      if (OB_FAIL(file_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
    // if failed, push seg_file to remove_queue again
    if (OB_FAIL(ret) && OB_NOT_NULL(seg_file)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(push_seg_file_to_remove_queue(seg_file->seg_id_, seg_file->valid_length_))) {
        LOG_WARN("fail to push seg file to remove queue", KR(tmp_ret), KPC(seg_file));
      }
    }
    // free memory
    OB_DELETEx(ObUnsealedSegFile, &allocator_, seg_file);
  }
  if (OB_FAIL(ret)) {
  } else if (file_ids.empty()) {
    // do nothing
  } else if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager_->delete_remote_files(file_ids))) {
    LOG_WARN("fail to delete remote files", KR(ret));
  } else {
    LOG_INFO("succ to execute remove task once", K(file_ids), "cost_time_us", ObTimeUtility::fast_current_time() - start_ts);
  }
  return ret;
}

bool ObSegmentFileManager::check_tmp_file_disk_size_enough()
{
  int ret = OB_SUCCESS;
  bool has_enough_space = false;
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  
  if (OB_ISNULL(disk_space_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space mgr is null", KR(ret));
  } else {
    // Check if tmp file cache has enough free space with 80% threshold
    // To avoid exhausting cache, we only allow using up to 80% of total space
    // This reserves 20% of free space as buffer
    const int64_t free_size = disk_space_mgr->get_unit_config_shared_macro_cache_free_size();
    const int64_t total_size = disk_space_mgr->get_unit_config_shared_macro_cache_size();
    
    // Check if free_size is above the 20% of total
    has_enough_space = (free_size >= total_size * 20 / 100);
  }

  return has_enough_space;
}

int ObSegmentFileManager::handle_write_on_meta_exist(
    TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle));
  } else if (write_info.get_tmp_file_valid_length() <= meta_handle.get_valid_length()) { // case 1: no need write io
    append_param.need_write_io_ = false;
    if (OB_FAIL(simulate_io_result(write_info, object_handle))) {
      LOG_WARN("fail to simulate io result", KR(ret), K(write_info), K(object_handle));
    } else {
      LOG_INFO("current io's valid length is smaller than or equal to seg meta's valid length, "
               "no need write io", K(write_info), K(meta_handle));
    }
  } else { // case 2: write_info.tmp_file_valid_length_ > seg_meta.valid_length_, need write io
    if (meta_handle.is_in_local()) { // case 2.1: current segment is in local
      if (OB_FAIL(handle_write_with_local_seg(meta_handle, write_info, object_handle, append_param))) {
        LOG_WARN("fail to handle write with local seg", KR(ret), K(meta_handle), K(write_info), K(object_handle));
      }
    } else { // case 2.2: current segment is in remote
      if (OB_FAIL(handle_write_with_remote_seg(meta_handle, write_info, object_handle, append_param))) {
        LOG_WARN("fail to handle write with remote seg", KR(ret), K(meta_handle), K(write_info), K(object_handle));
      }
    }
  }
  LOG_INFO("finish to handle write on meta exsit", KR(ret));
  return ret;
}

int ObSegmentFileManager::handle_write_on_meta_not_exist(
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
  const int64_t need_alloc_size = write_info.offset_ + write_info.size_;
  append_param.need_free_file_size_ = true;
  append_param.free_file_size_ = need_alloc_size;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *)) ||
      OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr or disk space mgr is null", KR(ret), KP(macro_cache_mgr), KP(disk_space_mgr));
  } else if (is_macro_write_cache_disabled()) {
    if (OB_FAIL(write_through_on_meta_not_exist(seg_id, write_info, append_param))) {
      LOG_WARN("fail to write through on meta not exist", KR(ret), K(seg_id), K(write_info), K(append_param));
    }
  // Notes for file size:
  // 1. fail before IOManager::aio_write, should free file size.
  // 2. fail on IOManager::aio_write, should free file size.
  // 3. fail after IOManager::aio_write (e.g., io timeout or callback failed), this tmp_file will
  // be marked as bad tmp_file. then, the file size will be freed by stat, when gc this tmp_file.
  // in case of partial write (alloc more, free less), disk space calibration task will calibrate
  // the space usage.
  } else if (OB_FAIL(macro_cache_mgr->alloc_file_size(ObSSMacroCacheType::TMP_FILE,
                                                      need_alloc_size,
                                                      ObDiskSpaceType::FILE))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) { // case 1: local disk space is not enough, write through
      ret = OB_SUCCESS; // ignore ret
      if (OB_FAIL(write_through_on_meta_not_exist(seg_id, write_info, append_param))) {
        LOG_WARN("fail to write through on meta not exist", KR(ret), K(seg_id), K(write_info), K(append_param));
      }
    } else {
      LOG_WARN("fail to alloc file size", KR(ret), "storage object type", STI(file_id.storage_object_type()), K(need_alloc_size));
    }
  } else { // case 2: local disk space is enough, write local
    // insert meta for sealed && unsealed
    TmpFileMetaHandle meta_handle;
    const int64_t tmp_file_valid_length = write_info.get_tmp_file_valid_length();
    if (OB_FAIL(meta_handle.set_tmpfile_meta(true/*is_in_local*/, tmp_file_valid_length/*valid_length*/,
                                             tmp_file_valid_length/*target_valid_length*/))) {
      LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
    } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id, meta_handle, append_param.conflict_info_handle_, ObSSTmpFileSegMetaOpType::INSERT,
               ObSSTmpFileSegDeleteType::NONE, true/*need_put_or_update_macro_cache*/, write_info.io_callback_, 0/*del_seg_valid_len*/))) {
      LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
    }
    if (OB_FAIL(ret)) { // free file size on fail
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(disk_space_mgr->free_file_size(need_alloc_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE))) {
        LOG_WARN("fail to free file size", KR(tmp_ret), K(need_alloc_size));
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::handle_write_with_local_seg(
    TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
  // tmp_file is 8KB aligned
  const int64_t current_seg_file_size = common::upper_align(meta_handle.get_valid_length(), tmp_file::ObTmpFileGlobal::PAGE_SIZE);
  // because tmp_file maybe overwrite, it need calculate need_alloc_size according to current_seg_file_size and offset
  const int64_t need_alloc_size = MAX(0, write_info.offset_ + write_info.size_ - current_seg_file_size);
  const int64_t valid_length = write_info.get_tmp_file_valid_length();
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_FAIL(meta_handle.set_target_valid_length(valid_length, file_id))) {
    LOG_WARN("fail to set target_valid_length", KR(ret), K(valid_length), K(file_id), K(meta_handle));
  } else if (FALSE_IT(append_param.free_file_size_ = need_alloc_size)) {
  } else if (FALSE_IT(append_param.need_free_file_size_ = (0 == need_alloc_size) ? false : true)) {
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *)) ||
      OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr or disk space mgr is null", KR(ret), KP(macro_cache_mgr), KP(disk_space_mgr));
  } else if ((need_alloc_size > 0) && OB_FAIL(macro_cache_mgr->alloc_file_size(ObSSMacroCacheType::TMP_FILE,
                                                                               need_alloc_size,
                                                                               ObDiskSpaceType::FILE))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) { // case 1: local disk space is not enough, write through
      ret = OB_SUCCESS; // ignore ret
      if (OB_FAIL(read_existing_data_and_append_buf(file_id, meta_handle, write_info, append_param))) {
        LOG_WARN("fail to read existing data and append buf", KR(ret), K(file_id), K(meta_handle), K(write_info));
      } else {
        write_info.io_desc_.set_write_through(true);
        append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
        if (write_info.io_desc_.is_sealed()) { // case 1.1: sealed
          TmpFileMetaHandle fake_meta_handle;
          if (OB_FAIL(fake_meta_handle.set_tmpfile_meta(false/*is_in_local*/, valid_length, valid_length/*target_valid_length*/))) {
            LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
          } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id, fake_meta_handle, append_param.conflict_info_handle_, ObSSTmpFileSegMetaOpType::DELETE,
                      ObSSTmpFileSegDeleteType::LOCAL, false/*need_put_or_update_macro_cache*/, write_info.io_callback_, 0/*del_seg_valid_len*/))) {
            LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
          }
        } else { // case 1.2: unsealed
          TmpFileMetaHandle cur_meta_handle;
          if (OB_FAIL(cur_meta_handle.set_tmpfile_meta(false/*is_in_local*/, valid_length, valid_length/*target_valid_length*/))) {
            LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
          } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id, cur_meta_handle, append_param.conflict_info_handle_, ObSSTmpFileSegMetaOpType::UPDATE,
                      ObSSTmpFileSegDeleteType::LOCAL, false/*need_put_or_update_macro_cache*/, write_info.io_callback_, 0/*del_seg_valid_len*/))) {
            LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(cur_meta_handle));
          }
        }
      }
    } else {
      LOG_WARN("fail to alloc file size", KR(ret), "storage object type", STI(file_id.storage_object_type()), K(write_info), "size", need_alloc_size);
    }
  } else { // case 2: local disk space is enough, write local
    // update meta for sealed && unsealed
    TmpFileMetaHandle cur_meta_handle;
    if (OB_FAIL(cur_meta_handle.set_tmpfile_meta(true/*is_in_local*/, valid_length, valid_length/*target_valid_length*/))) {
      LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
    } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id, cur_meta_handle, append_param.conflict_info_handle_, ObSSTmpFileSegMetaOpType::UPDATE,
                ObSSTmpFileSegDeleteType::NONE, true/*need_put_or_update_macro_cache*/, write_info.io_callback_, 0/*del_seg_valid_len*/))) {
      LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(cur_meta_handle));
    }
    if (OB_FAIL(ret)) { // free file size on fail
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(disk_space_mgr->free_file_size(need_alloc_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE))) {
        LOG_WARN("fail to free file size", KR(tmp_ret), K(need_alloc_size));
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::handle_write_with_remote_seg(
    TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
  const int64_t need_alloc_size = write_info.offset_ + write_info.size_;
  const int64_t valid_length = write_info.get_tmp_file_valid_length();
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_FAIL(meta_handle.set_target_valid_length(valid_length, file_id))) {
    LOG_WARN("fail to set target_valid_length", KR(ret), K(valid_length), K(file_id), K(meta_handle));
  } else if (FALSE_IT(append_param.free_file_size_ = need_alloc_size)) {
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *)) ||
      OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr or disk space mgr is null", KR(ret), KP(macro_cache_mgr), KP(disk_space_mgr));
  } else if (OB_FAIL(read_existing_data_and_append_buf(file_id, meta_handle, write_info, append_param))) {
    LOG_WARN("fail to read remote data and append buf", KR(ret), K(file_id), K(meta_handle), K(write_info));
  } else if (is_macro_write_cache_disabled()) {
    if (OB_FAIL(write_through_with_remote_seg(seg_id, meta_handle, write_info, append_param))) {
      LOG_WARN("fail to write through with remote seg", KR(ret), K(seg_id), K(meta_handle),
                K(write_info), K(append_param));
    }
  } else if (OB_FAIL(macro_cache_mgr->alloc_file_size(ObSSMacroCacheType::TMP_FILE,
                                                      need_alloc_size,
                                                      ObDiskSpaceType::FILE))) {
    if (OB_SERVER_OUTOF_DISK_SPACE == ret) { // case 1: local disk space is not enough, write through
      ret = OB_SUCCESS; // ignore ret
      if (OB_FAIL(write_through_with_remote_seg(seg_id, meta_handle, write_info, append_param))) {
        LOG_WARN("fail to write through with remote seg", KR(ret), K(seg_id), K(meta_handle),
                 K(write_info), K(append_param));
      }
    } else {
      LOG_WARN("fail to alloc file size", KR(ret), "storage object type", STI(file_id.storage_object_type()), K(need_alloc_size));
    }
  } else { // case 2: local disk space is enough, write local for unsealed and write remote for sealed
    if (write_info.io_desc_.is_sealed()) { // case 2.1: sealed
      // free file size alloc above, and write through
      if (OB_FAIL(disk_space_mgr->free_file_size(need_alloc_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE))) {
        LOG_WARN("fail to free file size", KR(ret), K(file_id), K(write_info));
      } else {
        write_info.io_desc_.set_write_through(true);
        append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
        TmpFileMetaHandle fake_meta_handle;
        if (OB_FAIL(fake_meta_handle.set_tmpfile_meta(false/*is_in_local*/, valid_length, valid_length/*target_valid_length*/))) {
          LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
        } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id, fake_meta_handle, append_param.conflict_info_handle_, ObSSTmpFileSegMetaOpType::DELETE,
            ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE, false/*need_put_or_update_macro_cache*/, write_info.io_callback_, meta_handle.get_valid_length()))) {
          LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
        }
      }
    } else { // case 2.2: unsealed
      TmpFileMetaHandle cur_meta_handle;
      if (OB_FAIL(cur_meta_handle.set_tmpfile_meta(true/*is_in_local*/, valid_length, valid_length/*target_valid_length*/))) {
        LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
      } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id, cur_meta_handle, append_param.conflict_info_handle_, ObSSTmpFileSegMetaOpType::UPDATE,
          ObSSTmpFileSegDeleteType::REMOTE, true/*need_put_or_update_macro_cache*/, write_info.io_callback_, meta_handle.get_valid_length()))) {
        LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(cur_meta_handle));
      }
      if (OB_FAIL(ret)) { // free file size on fail
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(disk_space_mgr->free_file_size(need_alloc_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE))) {
          LOG_WARN("fail to free file size", KR(tmp_ret), K(file_id), K(need_alloc_size));
        }
      }
    }
  }
  return ret;
}

int ObSegmentFileManager::read_existing_data_and_append_buf(
    const MacroBlockId &file_id,
    const TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::fast_current_time();
  ObMemAttr attr(write_info.mtl_tenant_id_, "TmpFileWrite");
  ObArenaAllocator allocator(attr);
  char *write_buf = nullptr;
  int64_t write_size = 0;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = file_id;
  read_info.offset_ = 0;
  read_info.size_ = meta_handle.get_valid_length();
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  read_info.mtl_tenant_id_ = write_info.mtl_tenant_id_;
  read_info.io_desc_.set_unsealed(); // existing data must be unsealed
  ObStorageObjectHandle read_object_handle;
  ObSSLocalCacheReader local_cache_reader; // for seg_meta.is_in_local_ == true
  ObSSObjectStorageReader object_storage_reader; // for seg_meta.is_in_local_ == false
  if (OB_ISNULL(read_info.buf_ = static_cast<char *>(allocator.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for read buf", KR(ret), "size", read_info.size_);
  } else if (meta_handle.is_in_local()) {
    // meta_handle.is_in_local() == true, read from local cache
    if (OB_FAIL(local_cache_reader.aio_read(read_info, read_object_handle))) {
      LOG_WARN("fail to aio read from local cache", KR(ret), K(read_info), K(read_object_handle));
    }
  } else {
    // meta_handle.is_in_local() == false, read from object storage
    if (OB_FAIL(object_storage_reader.aio_read(read_info, read_object_handle))) {
      LOG_WARN("fail to aio read from object storage", KR(ret), K(read_info), K(read_object_handle));
    }
  }
  
  if (FAILEDx(read_object_handle.wait())) {
    LOG_WARN("fail to wait", KR(ret), K(read_info), K(read_object_handle));
  } else if (FALSE_IT(write_size = (write_info.offset_ + write_info.size_))) {
  // Note: this write_buf will be free after IOManager::aio_write, which copies write_buf into IOManager
  } else if (OB_ISNULL(write_buf = static_cast<char *>(append_param.arena_allocator_.alloc(write_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for write buf", KR(ret), K(write_size));
  } else if (OB_ISNULL(write_info.buffer_) || OB_UNLIKELY(write_size < read_info.size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null or write buffer size is too small", KR(ret), K(write_info), K(read_info));
  } else {
    MEMCPY(write_buf, read_info.buf_, read_info.size_);
    MEMCPY(write_buf + write_info.offset_, write_info.buffer_, write_info.size_);
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = write_size;
  }
  const int64_t cost_us = ObTimeUtility::fast_current_time() - start_us;
  if (OB_UNLIKELY(cost_us > 200 * 1000L)) { // 200ms
    LOG_WARN("append tmp file cost too much time", KR(ret), K(cost_us), K(file_id), K(write_info));
  }
  return ret;
}

int ObSegmentFileManager::write_through_on_meta_not_exist(
    const TmpFileSegId &seg_id,
    ObStorageObjectWriteInfo &write_info,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  write_info.io_desc_.set_write_through(true);
  append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
  if (!write_info.io_desc_.is_sealed()) {
    TmpFileMetaHandle meta_handle;
    const int64_t valid_length = write_info.get_tmp_file_valid_length();
    if (OB_FAIL(meta_handle.set_tmpfile_meta(false/*is_in_local*/,
                                             valid_length,
                                             valid_length/*target_valid_length*/))) {
      LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
    } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id,
                                                        meta_handle,
                                                        append_param.conflict_info_handle_,
                                                        ObSSTmpFileSegMetaOpType::INSERT,
                                                        ObSSTmpFileSegDeleteType::NONE,
                                                        false/*need_put_or_update_macro_cache*/,
                                                        write_info.io_callback_,
                                                        0/*del_seg_valid_len*/))) {
      LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
    }
  }
  return ret;
}

int ObSegmentFileManager::write_through_with_remote_seg(
    const TmpFileSegId &seg_id,
    const TmpFileMetaHandle &meta_handle,
    ObStorageObjectWriteInfo &write_info,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  write_info.io_desc_.set_write_through(true);
  append_param.need_free_file_size_ = false; // write through no need to alloc and free file size
  const int64_t valid_length = write_info.get_tmp_file_valid_length();
  if (write_info.io_desc_.is_sealed()) { // case 1.1: sealed
    TmpFileMetaHandle fake_meta_handle;
    if (OB_FAIL(fake_meta_handle.set_tmpfile_meta(false/*is_in_local*/,
                                                  valid_length,
                                                  valid_length/*target_valid_length*/))) {
      LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
    } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id,
                                                        fake_meta_handle,
                                                        append_param.conflict_info_handle_,
                                                        ObSSTmpFileSegMetaOpType::DELETE,
                                                        ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE,
                                                        false/*need_put_or_update_macro_cache*/,
                                                        write_info.io_callback_,
                                                        meta_handle.get_valid_length()))) {
      LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(meta_handle));
    }
  } else { // case 1.2: unsealed
    TmpFileMetaHandle cur_meta_handle;
    if (OB_FAIL(cur_meta_handle.set_tmpfile_meta(false/*is_in_local*/,
                                                 valid_length,
                                                 valid_length/*target_valid_length*/))) {
      LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
    } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id,
                                                        cur_meta_handle,
                                                        append_param.conflict_info_handle_,
                                                        ObSSTmpFileSegMetaOpType::UPDATE,
                                                        ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE,
                                                        false/*need_put_or_update_macro_cache*/,
                                                        write_info.io_callback_,
                                                        meta_handle.get_valid_length()))) {
      LOG_WARN("fail to construct tmp file io callback", KR(ret), K(seg_id), K(cur_meta_handle));
    }
  }
  return ret;
}

int ObSegmentFileManager::simulate_io_result(
    const ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle) const
{
  int ret = OB_SUCCESS;
  ObIOInfo io_info;
  io_info.tenant_id_ = write_info.mtl_tenant_id_;
  io_info.offset_ = write_info.offset_;
  io_info.size_ = write_info.size_;
  io_info.buf_ = write_info.buffer_;
  io_info.flag_ = write_info.io_desc_;
  io_info.callback_ = write_info.io_callback_;
  io_info.tmp_file_valid_length_ = write_info.get_tmp_file_valid_length();
  io_info.flag_.set_sys_module_id(write_info.io_desc_.get_sys_module_id());
  io_info.flag_.set_write();
  io_info.timeout_us_ = write_info.io_timeout_ms_;
  io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;

  ObIOResult *io_result = nullptr;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(io_info.tenant_id_, tenant_holder))) {
    LOG_WARN("fail to get tenant io manager", KR(ret), "tenant_id", io_info.tenant_id_);
  } else if (OB_ISNULL(tenant_holder.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant holder ptr is null", KR(ret));
  } else if (OB_FAIL(tenant_holder.get_ptr()->alloc_and_init_result(io_info, io_result))) {
    LOG_WARN("fail to alloc and init result", KR(ret), K(io_info));
  } else if (OB_ISNULL(io_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io result is null", KR(ret));
  } else if (OB_FAIL(object_handle.get_io_handle().set_result(*io_result))) {
    LOG_WARN("fail to set result", KR(ret));
  } else {
    io_result->finish_without_accumulate(ret);
  }
  return ret;
}

int ObSegmentFileManager::construct_tmp_file_write_callback(
    const TmpFileSegId &seg_id,
    const TmpFileMetaHandle &meta_handle,
    TmpFileConflictInfoHandle &conflict_info_handle,
    const ObSSTmpFileSegMetaOpType seg_meta_op_type,
    const ObSSTmpFileSegDeleteType seg_del_type,
    const bool need_put_or_update_macro_cache,
    ObIOCallback *&io_callback,
    const int64_t del_seg_valid_len)
{
  int ret = OB_SUCCESS;
  ObSSTmpFileWriteCallback *tmp_file_write_callback = nullptr;
  bool write_access_acquired = false;
  bool callback_take_over_write_access = false;

  if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_UNLIKELY(!meta_handle.is_valid() || !conflict_info_handle.is_valid() || !seg_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(meta_handle), K(conflict_info_handle));
  } else {
    // First acquire write access before creating callback
    if (OB_FAIL(conflict_info_handle.try_acquire_write_access())) {
      LOG_WARN("failed to acquire write access for seg", KR(ret), K(seg_id));
    } else {
      write_access_acquired = true;
    }

    if (OB_SUCC(ret)) {
      ObIAllocator &io_callback_allocator = file_manager_->get_io_callback_allocator();
      if (OB_ISNULL(tmp_file_write_callback = static_cast<ObSSTmpFileWriteCallback *>(
                    io_callback_allocator.alloc(sizeof(ObSSTmpFileWriteCallback))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ss tmp file io callback memory", KR(ret));
      } else if (FALSE_IT(tmp_file_write_callback = new (tmp_file_write_callback) ObSSTmpFileWriteCallback())) {
      } else if (OB_FAIL(tmp_file_write_callback->set_ss_tmpfile_write_callback(seg_id, meta_handle, conflict_info_handle,
                                                                               seg_meta_op_type, seg_del_type, need_put_or_update_macro_cache,
                                                                               del_seg_valid_len, &io_callback_allocator))) {
        LOG_WARN("fail to set write callbcak", KR(ret), K(seg_id));
      } else {
        io_callback = static_cast<common::ObIOCallback *>(tmp_file_write_callback);
        callback_take_over_write_access = true;
      }
    }

    // Cleanup on failure
    if (OB_FAIL(ret)) {
      if (write_access_acquired && !callback_take_over_write_access) {
        int tmp_ret = OB_SUCCESS;
        if (OB_UNLIKELY(!conflict_info_handle.is_valid())) {
          LOG_ERROR("conflict info handle is invalid when releasing write access in error cleanup", K(seg_id));
        } else if (OB_TMP_FAIL(conflict_info_handle.release_write_access())) {
          LOG_WARN("failed to release write access in error cleanup", K(tmp_ret), K(seg_id));
        }
      }
      
      if (OB_NOT_NULL(tmp_file_write_callback)) {
        ObIOCallback *callback_ptr = static_cast<ObIOCallback*>(tmp_file_write_callback);
        free_io_callback<ObSSTmpFileWriteCallback>(callback_ptr);
      }
    }
  }

  return ret;
}

int ObSegmentFileManager::aio_write_with_create_parent_dir(
    ObSSTmpFileAppendParam &append_param,
    ObStorageObjectWriteInfo &write_info,
    const ObStorageObjectHandle &ori_object_handle,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager_->aio_write_with_create_parent_dir(write_info, ori_object_handle, object_handle))) {
    if (should_fallback_to_object_storage(ret, write_info)) {
      ret = OB_SUCCESS;
      if (OB_FAIL(handle_local_write_failure_and_fallback(ori_object_handle, append_param, write_info, object_handle))) {
        LOG_WARN("fail to handle local write failure and fallback", KR(ret), K(write_info), K(object_handle));
      }
    } else {
      LOG_WARN("fail to aio write with create parent dir", KR(ret), K(write_info), K(object_handle));
    }
  }
  
  // free file size on fail (but skip if already freed during fallback)
  if (OB_FAIL(ret) && append_param.need_free_file_size_) {
    int tmp_ret = OB_SUCCESS;
    ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
    if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("disk space manager is null", KR(tmp_ret));
    } else if (OB_TMP_FAIL(disk_space_mgr->free_file_size(append_param.free_file_size_,
                                                          ObSSMacroCacheType::TMP_FILE,
                                                          ObDiskSpaceType::FILE))) {
      LOG_WARN("fail to free file size", KR(tmp_ret), "size", append_param.free_file_size_);
    }
  }
  return ret;
}

int ObSegmentFileManager::async_write_dual(
    ObStorageObjectWriteInfo &write_info,
    const ObStorageObjectHandle &ori_object_handle,
    ObStorageObjectHandle &object_handle,
    ObSSTmpFileAppendParam &append_param)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());

  // because tmp file may be overwritten, local file need to be deleted before write through
  bool is_exist = false;
  if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager_->is_exist_local_file(file_id, 0/*ls_epoch_id*/, is_exist))) {
    LOG_WARN("fail to judge local file exist", KR(ret), K(file_id));
  } else if (is_exist) {
    if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(file_id))) {
      LOG_WARN("fail to delete file", KR(ret), K(file_id));
    }
  }

  // write through sealed 2MB segment, should delete previous seg meta. otherwise, the next read
  // io would read the previous seg. e.g., write 8KB unsealed seg (with valid_len suffix) to
  // object storage, which introduces one seg meta. then, write 2MB sealed seg to object storage.
  // if the previous seg meta is not deleted, then next read io will read the 8KB unsealed seg
  // (with valid_len suffix) on object storage.
  if (FAILEDx(delete_meta(seg_id))) {
    LOG_WARN("fail to delete meta", KR(ret), K(file_id));
  }

  // Check if local space is available
  const bool local_space_available = check_tmp_file_disk_size_enough();
  if (OB_FAIL(ret)) {
    // do nothing, already logged before
  } else if (!local_space_available) {
    // Local space insufficient, write directly to object storage
    // Skip local file check/delete to reduce overhead - they will be handled by normal cleanup
    // Write through to object storage directly
    write_info.io_desc_.set_write_through(true);
    append_param.need_free_file_size_ = false;
    if (OB_FAIL(file_manager_->aio_write_with_create_parent_dir(write_info, ori_object_handle, object_handle))) {
      LOG_WARN("dual write: write_through failed", KR(ret), K(file_id));
    }
  } else {
    // Local space available, allocate file size before write
    const int64_t need_alloc_size = write_info.offset_ + write_info.size_;
    ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
    ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
    
    if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *)) ||
        OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache mgr is null", KR(ret), KP(macro_cache_mgr), KP(disk_space_mgr));
    } else if (OB_FAIL(macro_cache_mgr->alloc_file_size(ObSSMacroCacheType::TMP_FILE,
                                                        need_alloc_size,
                                                        ObDiskSpaceType::FILE))) {
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        // Disk space allocation failed, fallback to object storage
        ret = OB_SUCCESS;
        append_param.need_free_file_size_ = false; // alloc failed, no need to free
        if (OB_FAIL(handle_local_write_failure_and_fallback(ori_object_handle, append_param, write_info, object_handle))) {
          LOG_WARN("fail to handle local write failure and fallback", KR(ret), K(file_id));
        }
      } else {
        LOG_WARN("fail to alloc file size", KR(ret), "storage object type", STI(file_id.storage_object_type()), K(need_alloc_size));
      }
    } else {
      // File size allocated successfully, set flags
      append_param.free_file_size_ = need_alloc_size;
      append_param.need_free_file_size_ = true;
      
      // Create callback to put data to macro cache (but don't INSERT meta for sealed segment)
      // Sealed segment doesn't need meta because it always reads the full 2MB
      TmpFileMetaHandle meta_handle;
      const int64_t tmp_file_valid_length = write_info.get_tmp_file_valid_length();
      if (OB_FAIL(meta_handle.set_tmpfile_meta(true/*is_in_local*/, tmp_file_valid_length/*valid_length*/,
                                               tmp_file_valid_length/*target_valid_length*/))) {
        LOG_WARN("fail to set meta handle", KR(ret), K(write_info));
      } else if (OB_FAIL(construct_tmp_file_write_callback(seg_id, meta_handle, append_param.conflict_info_handle_, ObSSTmpFileSegMetaOpType::INSERT,
                 ObSSTmpFileSegDeleteType::NONE, true/*need_put_or_update_macro_cache*/, write_info.io_callback_, 0/*del_seg_valid_len*/))) {
        LOG_WARN("fail to construct tmp file io callback for write_dual", KR(ret), K(seg_id), K(meta_handle));
      }
      
      // Free file size on callback construction failure
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(disk_space_mgr->free_file_size(need_alloc_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE))) {
          LOG_WARN("fail to free file size", KR(tmp_ret), K(need_alloc_size));
        } else {
          append_param.need_free_file_size_ = false; // already freed here
        }
      } else if (OB_FAIL(file_manager_->aio_write_with_create_parent_dir(write_info, ori_object_handle, object_handle))) {
        LOG_WARN("dual write: local write failed", KR(ret), K(file_id));
        
        ret = OB_SUCCESS;
        // fallback to object storage (will free file size inside)
        if (OB_FAIL(handle_local_write_failure_and_fallback(ori_object_handle, append_param, write_info, object_handle))) {
          LOG_WARN("fail to handle local write failure and fallback", KR(ret), K(file_id));
        }
      }
    }
  }

  return ret;
}

bool ObSegmentFileManager::should_fallback_to_object_storage(const int ret, const blocksstable::ObStorageObjectWriteInfo &write_info) const {
  return OB_SERVER_OUTOF_DISK_SPACE == ret && !write_info.io_desc_.is_write_through();
}

int ObSegmentFileManager::handle_local_write_failure_and_fallback(
    const ObStorageObjectHandle &ori_object_handle,
    ObSSTmpFileAppendParam &append_param,
    ObStorageObjectWriteInfo &write_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  
  // Free allocated disk space since we're falling back to object storage
  if (append_param.need_free_file_size_) {
    int tmp_ret = OB_SUCCESS;
    ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
    if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("disk space manager is null", KR(tmp_ret));
    } else if (OB_TMP_FAIL(disk_space_mgr->free_file_size(append_param.free_file_size_,
                                                          ObSSMacroCacheType::TMP_FILE,
                                                          ObDiskSpaceType::FILE))) {
      LOG_WARN("fail to free file size during object storage fallback", KR(tmp_ret), "size", append_param.free_file_size_);
    } else {
      // Mark that file size has been freed to prevent double free
      append_param.need_free_file_size_ = false;
    }
  }
  
  // Clear existing callback first
  if (OB_NOT_NULL(write_info.io_callback_)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(ObIOCallbackType::SS_TMP_FILE_CALLBACK != write_info.io_callback_->get_type())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected io callback type during object storage fallback",
                "callback_type", write_info.io_callback_->get_type(),
                "expected_type", ObIOCallbackType::SS_TMP_FILE_CALLBACK,
                KR(tmp_ret), K(write_info));
      
      // Always free the callback regardless of type check result to prevent resource leaks
      if (OB_NOT_NULL(write_info.io_callback_)) {
        free_io_callback<ObIOCallback>(write_info.io_callback_);
      }
    } else {
      if (OB_NOT_NULL(write_info.io_callback_)) {
        free_io_callback<ObSSTmpFileWriteCallback>(write_info.io_callback_);
      }
    }
  }
  
  const MacroBlockId &file_id = object_handle.get_macro_id();
  const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
  TmpFileMetaHandle meta_handle;
  bool is_meta_exist = false;
  if (OB_FAIL(try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to try get seg meta", KR(ret), K(seg_id));
  } else if (is_meta_exist) { // case 1: is_meta_exist = true
    if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle is invalid", KR(ret), K(seg_id), K(meta_handle));
    } else {
      // Note: If segment meta exists, it MUST reside in remote storage. 
      // Local existence of segment metadata implies its parent directory 
      // would have been initialized during prior operations.
      if (OB_UNLIKELY(meta_handle.is_in_local())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("meta handle is in local", KR(ret), K(seg_id), K(meta_handle));
      } else {
        if (OB_FAIL(write_through_with_remote_seg(seg_id, meta_handle, write_info, append_param))) {
          LOG_WARN("fail to fallback write through with remote seg", KR(ret), K(seg_id), K(meta_handle));
        }
      }
    }
  } else { // case 2: is_meta_exist = false
    // Note: If segment meta not exists, it MUST write through because local disk not enough
    if (OB_FAIL(write_through_on_meta_not_exist(seg_id, write_info, append_param))) {
      LOG_WARN("fail to fallback write through on meta not exsit", KR(ret), K(seg_id), K(meta_handle));
    }
  }
  
  if (OB_FAIL(ret)) {
    // If fallback preparation failed, keep the original error
    LOG_WARN("fail to fallback to object storage", KR(ret), K(write_info), K(object_handle));
  } else if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null during fallback", KR(ret));
  } else if (OB_FAIL(file_manager_->aio_write_with_create_parent_dir(write_info, ori_object_handle, object_handle))) {
    LOG_WARN("fail to write to object storage after parent dir creation failure", KR(ret), K(write_info), K(object_handle));
  }
  
  return ret;
}

// Case 1: file_id >= 1000 or file_id in [0,99], must be old version tmp file, delete directly
int ObSegmentFileManager::delete_old_version_files_in_old_dir(const char *tmp_data_path,
                                                              const int64_t file_num,
                                                              int64_t &delete_success_count)
{
  int ret = OB_SUCCESS;
  char old_tmp_file_dir[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(tmp_data_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp_data_path is null", KR(ret));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space mgr is null", KR(ret));
  } else if (OB_FAIL(databuff_printf(old_tmp_file_dir, sizeof(old_tmp_file_dir), "%s/%ld", tmp_data_path, file_num))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tmp_data_path), K(file_num));
  } else {
    int64_t file_size = 0;
    int64_t dir_num = 0;
    if (OB_FAIL(OB_DIR_MGR.delete_dir_rec(old_tmp_file_dir, file_size, dir_num))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        // Already deleted, ignore
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to delete old version tmp file dir", KR(ret), K(old_tmp_file_dir));
      }
    } else {
      delete_success_count++;
      if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        FLOG_INFO("successfully deleted old version tmp file directory", K(file_num), K(old_tmp_file_dir), K(delete_success_count));
      }
    }
    
    // free size of deleted dirs and files, no matter OB_SUCC or OB_FAIL
    int tmp_ret = OB_SUCCESS;
    if (OB_LIKELY(file_size > 0)) {
      if (OB_TMP_FAIL(disk_space_mgr->free_file_size(file_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE))) {
        LOG_WARN("fail to free file size", KR(tmp_ret), K(file_size), K(old_tmp_file_dir));
      }
    }
    if (OB_LIKELY(dir_num > 0)) {
      const int64_t dir_size = dir_num * ObDirManager::DEFAULT_DIR_SIZE;
      if (OB_TMP_FAIL(disk_space_mgr->free_file_size(dir_size, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::DIR))) {
        LOG_WARN("fail to free dir size", KR(tmp_ret), K(dir_num), K(dir_size), K(old_tmp_file_dir));
      }
    }
  }
  return ret;
}

// Case 2: file_id < 1000, this is a scatter directory (100-999)
// Check if there are any old version tmp files under this scatter directory
// Old version structure: tmp_data/100/seg1.T8 (seg files directly under scatter_dir)
// New version structure: tmp_data/100/100000/seg1.T8 (tmp_file_id directories under scatter_dir)
int ObSegmentFileManager::delete_old_version_files_in_scatter_dir(const char *tmp_data_path,
                                                                  const int64_t file_num,
                                                                  int64_t &delete_success_count)
{
  int ret = OB_SUCCESS;
  char scatter_dir[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  if (OB_ISNULL(tmp_data_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tmp_data_path is null", KR(ret));
  } else if (OB_FAIL(databuff_printf(scatter_dir, sizeof(scatter_dir), "%s/%03ld", tmp_data_path, file_num))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tmp_data_path), K(file_num));
  } else {
    // Use scan_dir with operator to process directory entries
    ObDeleteOldVersionTmpFileOp delete_op(scatter_dir, delete_success_count);
    if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(scatter_dir, delete_op))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        // Directory doesn't exist, nothing to clean, this is OK
        LOG_DEBUG("scatter dir not exist, skip cleaning", KR(ret), K(scatter_dir));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to scan dir", KR(ret), K(scatter_dir));
      }
    }
  }
  
  return ret;
}

int ObSegmentFileManager::clean_old_version_tmp_files()
{
  int ret = OB_SUCCESS;
  char tmp_data_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  int64_t delete_success_count = 0;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("segment file manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space mgr is null", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_local_tmp_data_dir(tmp_data_path, sizeof(tmp_data_path), MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get local tmp data dir", KR(ret));
  } else {
    ObSingleNumFileListOp file_list_op;
    file_list_op.set_dir_flag();
    
    // List all first-level directories under tmp_data/
    if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(tmp_data_path, file_list_op))) {
      LOG_WARN("fail to scan dir", KR(ret), K(tmp_data_path));
    } else {
      const int64_t total_count = file_list_op.file_list_.count();
      FLOG_INFO("start to clean old version tmp files", K(total_count), K(tmp_data_path), K(file_list_op.file_list_));
      for (int64_t i = 0; OB_SUCC(ret) && i < total_count; ++i) {
        const int64_t file_num = file_list_op.file_list_.at(i);
        
        if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
          FLOG_INFO("cleaning old version tmp files in progress", K(i), K(total_count), K(delete_success_count));
        }
        
        if ((file_num >= ObDirManager::TMP_FILE_SCATTER_DIR_NUM) || ((file_num >= 0) && (file_num <= 99))) {
          // file_num >= 1000 or file_num in [0,99]: old version directories, delete directly
          if (OB_FAIL(delete_old_version_files_in_old_dir(tmp_data_path, file_num, delete_success_count))) {
            LOG_WARN("fail to delete old version direct dir", KR(ret), K(file_num));
          }
        } else {
          // file_num in [100,999]: scatter directories, process files inside
          if (OB_FAIL(delete_old_version_files_in_scatter_dir(tmp_data_path, file_num, delete_success_count))) {
            LOG_WARN("fail to delete old version files in scatter dir", KR(ret), K(file_num));
          }
        }
      }
      FLOG_INFO("finish cleaning old version tmp files", K(total_count), K(delete_success_count));
    }
  }
  
  return ret;
}

} // namespace storage
} // namespace oceanbase
