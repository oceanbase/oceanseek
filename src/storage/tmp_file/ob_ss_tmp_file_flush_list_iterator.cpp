/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tmp_file/ob_ss_tmp_file_flush_list_iterator.h"
#include "storage/tmp_file/ob_shared_storage_tmp_file.h"

namespace oceanbase
{
namespace tmp_file
{
ObSSTmpFileFlushListIterator::ObSSTmpFileFlushListIterator() :
  is_inited_(false), prio_mgr_(nullptr), files_(),
  cur_caching_list_idx_(FileList::L1),
  cur_iter_file_idx_(-1),
  cached_file_num_(0)
{}

ObSSTmpFileFlushListIterator::~ObSSTmpFileFlushListIterator()
{
  destroy();
}

int ObSSTmpFileFlushListIterator::init(ObSSTmpFileFlushPriorityManager *prio_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(prio_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(prio_mgr));
  } else if (FALSE_IT(files_.set_attr(ObMemAttr(MTL_ID(), "TFFlushIterFile")))) {
  } else if (OB_FAIL(files_.prepare_allocate(MAX_CACHE_NUM))) {
    LOG_WARN("fail to prepare allocate", KR(ret));
  } else {
    is_inited_ = true;
    prio_mgr_ = prio_mgr;
  }
  return ret;
}

int ObSSTmpFileFlushListIterator::clear()
{
  int ret = OB_SUCCESS;

  // reinsert unused cached file into flush list
  FlushCtxState cur_stage = cal_current_flush_stage_();
  if (cur_stage < FlushCtxState::FSM_F1 || cur_stage >= FlushCtxState::FSM_FINISHED) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected flush stage", KR(ret), K(cur_stage));
  } else if (cached_file_num_ > 0 && cur_iter_file_idx_ != cached_file_num_ &&
             OB_FAIL(reinsert_files_into_flush_list_(cur_iter_file_idx_, cached_file_num_ - 1))){
    // cur_iter_file_idx_ == cached_file_num_ means all files have been reset, no need to reinsert.
    LOG_ERROR("fail to reinsert files into flush list", KR(ret), K(cur_iter_file_idx_), K(cached_file_num_));
  } else {
    cur_caching_list_idx_ = FileList::L1;
    cur_iter_file_idx_ = -1;
    cached_file_num_ = 0;
    for (int64_t i = 0; i < files_.count(); ++i) {
      files_[i].reset();
    }
  }

  return ret;
}

int ObSSTmpFileFlushListIterator::reset()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    if (OB_FAIL(clear())) {
      LOG_WARN("fail to clear", KR(ret));
    }
    is_inited_ = false;
  }
  return ret;
}

void ObSSTmpFileFlushListIterator::destroy()
{
  reset();
}

int ObSSTmpFileFlushListIterator::reinsert_files_into_flush_list_(const int64_t start_file_idx,
                                                                  const int64_t end_file_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_file_idx < 0 || start_file_idx > end_file_idx ||
                  end_file_idx >= cached_file_num_ ||
                  cached_file_num_ > files_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file idx", KR(ret), K(start_file_idx), K(end_file_idx),
             K(cached_file_num_), K(files_.count()));
  } else {
    for (int64_t i = start_file_idx; OB_SUCC(ret) && i <= end_file_idx; i++) {
      if (OB_UNLIKELY(!files_[i].is_inited_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("uninitialized file is unexpected", KR(ret), K(i), K(files_[i]));
      } else {
        ObSSTmpFileHandle &file_handle = files_[i].file_handle_;
        if (OB_ISNULL(file_handle.get())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", KR(ret));
        } else if (OB_FAIL(file_handle.get()->reinsert_data_flush_node())) {
          LOG_WARN("fail to reinsert flush node", KR(ret), K(files_[i]));
        }
      }
    }
  }
  return ret;
}

int ObSSTmpFileFlushListIterator::next(const FlushCtxState iter_stage, ObSSTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  FlushCtxState cur_stage = FlushCtxState::FSM_FINISHED;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(files_.count() != MAX_CACHE_NUM ||
                         cached_file_num_ > MAX_CACHE_NUM ||
                         cached_file_num_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(files_.count()), K(cached_file_num_));
  } else if (OB_UNLIKELY(FlushCtxState::FSM_FINISHED <= iter_stage || iter_stage < FlushCtxState::FSM_F1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid iter_stage", KR(ret), K(iter_stage));
  } else if (FALSE_IT(cur_stage = cal_current_flush_stage_())) {
  } else if (OB_UNLIKELY(FlushCtxState::FSM_FINISHED <= cur_stage || cur_stage < FlushCtxState::FSM_F1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_stage));
  } else if (OB_UNLIKELY(cur_stage > iter_stage)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid iter_stage", KR(ret), K(iter_stage), K(cur_stage));
  } else if (cur_stage < iter_stage || cur_iter_file_idx_ == cached_file_num_) {
    // code will run here when:
    // 1. expected iterating flush stage is over than current flush stage;
    // 2. all cached file has been iterated
    if (OB_FAIL(clear())) {
      LOG_WARN("fail to clear", KR(ret), K(cur_stage), K(iter_stage), K(cur_iter_file_idx_), K(cached_file_num_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == cached_file_num_ && OB_FAIL(cache_files_(iter_stage))) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("fail to cache files", KR(ret));
    } else {
      LOG_WARN("fail to cache files", KR(ret));
    }
  } else if (OB_FAIL(check_cur_idx_status_())) {
    LOG_WARN("fail to check cur idx status", KR(ret));
  } else if (FALSE_IT(file_handle = files_[cur_iter_file_idx_].file_handle_)) {
  } else if (OB_FAIL(advance_file_idx_())) {
    LOG_WARN("fail to advance big file idx", KR(ret));
  }
  LOG_DEBUG("try to get next file", KR(ret), K(iter_stage), K(cur_stage),
            K(cur_iter_file_idx_), K(cached_file_num_), K(file_handle));
  return ret;
}

int ObSSTmpFileFlushListIterator::cache_files_(const FlushCtxState iter_stage)
{
  int ret = OB_SUCCESS;
  FileList end_list_idx = FileList::MAX;
  const int64_t target_cache_file_num = iter_stage == FlushCtxState::FSM_F1 ?
                                        BIG_FILE_CACHE_NUM : MAX_CACHE_NUM;
  ObArray<ObSSTmpFileHandle> file_handles;

  if (OB_UNLIKELY(0 != cached_file_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cached num", KR(ret), K(cached_file_num_));
  } else if (OB_FAIL(init_caching_list_with_flush_stage_(iter_stage))) {
    LOG_WARN("fail to init caching list", KR(ret), K(iter_stage));
  } else if (OB_FAIL(acquire_final_list_of_flush_stage_(iter_stage, end_list_idx))) {
    LOG_WARN("fail to acquire final list of flush stage", KR(ret), K(iter_stage));
  } else if (OB_FAIL(file_handles.prepare_allocate_and_keep_count(target_cache_file_num))) {
    LOG_WARN("fail to prepare allocate", KR(ret), K(target_cache_file_num));
  } else { // pop enough files from priority manager for caching
    int64_t remain_cache_file_num = target_cache_file_num;
    bool cache_over = false;
    while (OB_SUCC(ret) && !cache_over && remain_cache_file_num > 0) {
      int64_t actual_cache_file_num = 0;
      if (OB_FAIL(prio_mgr_->popN_from_file_list(cur_caching_list_idx_,
                                                 remain_cache_file_num, actual_cache_file_num,
                                                 file_handles))) {
        LOG_WARN("fail to pop N from file list", KR(ret), K(cur_caching_list_idx_));
      } else if (FALSE_IT(remain_cache_file_num -= actual_cache_file_num)) {
      } else if (OB_UNLIKELY(remain_cache_file_num < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cache file num", KR(ret), K(target_cache_file_num),
                 K(remain_cache_file_num), K(actual_cache_file_num));
      } else if (0 == remain_cache_file_num) { // cache enough files
        cache_over = true;
      } else { // remain_cache_file_num > 0
        if (cur_caching_list_idx_ == end_list_idx) { // has reached the last list of this flush stage
          if (file_handles.empty()) {
            ret = OB_ITER_END;
            LOG_DEBUG("iter end in current flush stage", KR(ret), K(iter_stage),
                      K(cur_caching_list_idx_), K(remain_cache_file_num));
          } else { // cache files successful, but the num is not enough
            cache_over = true;
          }
        } else if (OB_FAIL(advance_caching_list_idx_())) { // pop files of the next list of this flush stage
          LOG_WARN("fail to advance caching list idx", KR(ret));
        }
      }
    } // end while
  }

  if (FAILEDx(build_file_wrappers_(file_handles))) {
    LOG_WARN("fail to cache files", KR(ret));
  }

  if (OB_FAIL(ret)) {
    cached_file_num_ = 0; // to guarantee that the iterator must not reinsert cached part files into priority list
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_LIKELY(OB_SUCCESS == tmp_ret) && i < file_handles.count(); ++i) {
      ObSharedStorageTmpFile *tmp_file = file_handles[i].get();
      if (OB_ISNULL(tmp_file)) {
        // could not happen, just skip
      } else if (OB_TMP_FAIL(tmp_file->reinsert_data_flush_node())) {
        LOG_ERROR("fail to reinsert flush node", KR(tmp_ret), K(i), K(file_handles[i]));
      }
    }
  }
  return ret;
}

int ObSSTmpFileFlushListIterator::build_file_wrappers_(const ObArray<ObSSTmpFileHandle> &file_handles)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(file_handles.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(file_handles.empty()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_handles.count(); ++i) {
      if (i >= files_.count()) {
        ret = OB_ERROR_OUT_OF_RANGE;
        LOG_ERROR("index is out of range", KR(ret), K(i), K(files_));
      } else if (OB_FAIL(files_[i].init(file_handles[i]))) {
        LOG_WARN("fail to init tmp file handle wrapper", KR(ret), K(i), K(file_handles[i]), K(files_[i]));

        for (int64_t j = 0; j < i; ++j) {
          files_[j].reset();
        }
      }
    }

    if (OB_SUCC(ret)) {
      lib::ob_sort(files_.begin(), files_.begin() + file_handles.count());
      cached_file_num_ = file_handles.count();
      cur_iter_file_idx_ = 0;
    }
  }
  return ret;
}

int ObSSTmpFileFlushListIterator::advance_caching_list_idx_()
{
  int ret = OB_SUCCESS;

  switch(cur_caching_list_idx_) {
    case FileList::L1:
      cur_caching_list_idx_ = FileList::L2;
      break;
    case FileList::L2:
      cur_caching_list_idx_ = FileList::L3;
      break;
    case FileList::L3:
      cur_caching_list_idx_ = FileList::L4;
      break;
    case FileList::L4:
      cur_caching_list_idx_ = FileList::L5;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", KR(ret), K(cur_caching_list_idx_));
      break;
  }

  return ret;
}

int ObSSTmpFileFlushListIterator::check_cur_idx_status_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_iter_file_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_file_idx_));
  } else if (OB_UNLIKELY(cur_iter_file_idx_ >= cached_file_num_ || cached_file_num_ > files_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_file_idx_), K(cached_file_num_));
  } else if (OB_UNLIKELY(!files_[cur_iter_file_idx_].is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(cur_iter_file_idx_), K(files_[cur_iter_file_idx_]));
  }

  return ret;
}

int ObSSTmpFileFlushListIterator::advance_file_idx_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_iter_file_idx_ < 0 || cur_iter_file_idx_ >= cached_file_num_ ||
                  cached_file_num_ > files_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid file idx", KR(ret), K(cur_iter_file_idx_), K(cached_file_num_), K(files_));
  } else {
    files_[cur_iter_file_idx_].reset();
    cur_iter_file_idx_++;
  }
  return ret;
}

FlushCtxState ObSSTmpFileFlushListIterator::cal_current_flush_stage_()
{
  FlushCtxState stage = FlushCtxState::FSM_FINISHED;
  if (OB_UNLIKELY(cur_caching_list_idx_ < FileList::L1 ||
                  cur_caching_list_idx_ > FileList::L5)) {
    // stage = FlushCtxState::FSM_FINISHED;
  } else if (cur_caching_list_idx_ == FileList::L1) {
    stage = FlushCtxState::FSM_F1;
  } else if (cur_caching_list_idx_ <= FileList::L4) {
    stage = FlushCtxState::FSM_F2;
  } else if (cur_caching_list_idx_ == FileList::L5) {
    stage = FlushCtxState::FSM_F3;
  }

  return stage;
}

int ObSSTmpFileFlushListIterator::init_caching_list_with_flush_stage_(const FlushCtxState iter_stage)
{
  int ret = OB_SUCCESS;
  if (cal_current_flush_stage_() == iter_stage) {
    // no need to change caching list
  } else if (iter_stage == FlushCtxState::FSM_F1) {
    cur_caching_list_idx_ = FileList::L1;
  } else if (iter_stage == FlushCtxState::FSM_F2) {
    cur_caching_list_idx_ = FileList::L2;
  } else if (iter_stage == FlushCtxState::FSM_F3) {
    cur_caching_list_idx_ = FileList::L5;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stage", KR(ret), K(iter_stage));
  }

  return ret;
}

int ObSSTmpFileFlushListIterator::acquire_final_list_of_flush_stage_(const FlushCtxState iter_stage,
                                                                   FileList &list_idx)
{
  int ret = OB_SUCCESS;
  switch(iter_stage) {
    case FlushCtxState::FSM_F1:
      list_idx = FileList::L1;
      break;
    case FlushCtxState::FSM_F2:
      list_idx = FileList::L4;
      break;
    case FlushCtxState::FSM_F3:
      list_idx = FileList::L5;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid stage", KR(ret), K(iter_stage));
  }
  return ret;
}

int ObSSTmpFileFlushListIterator::ObFlushingTmpFileWrapper::init(const ObSSTmpFileHandle &file_handle)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(file_handle.get())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(file_handle.get()));
  } else {
    file_handle_ = file_handle;
    is_inited_ = true;
  }
  return ret;
}

void ObSSTmpFileFlushListIterator::ObFlushingTmpFileWrapper::reset()
{
  if (IS_INIT) {
    is_inited_ = false;
    file_handle_.reset();
  }
}

bool ObSSTmpFileFlushListIterator::ObFlushingTmpFileWrapper::operator <(const ObFlushingTmpFileWrapper &other)
{
  int ret = OB_SUCCESS;
  bool b_ret = false;
  if (OB_UNLIKELY(!other.is_inited_ || !is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", KR(ret), K(other), KPC(this));
  } else if (OB_ISNULL(other.file_handle_.get()) || OB_ISNULL(file_handle_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attempt to compare file handle with nullptr", KR(ret), K(file_handle_), K(other.file_handle_));
  } else {
    b_ret = file_handle_.get()->get_data_page_flush_level() < other.file_handle_.get()->get_data_page_flush_level();
  }
  return b_ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
