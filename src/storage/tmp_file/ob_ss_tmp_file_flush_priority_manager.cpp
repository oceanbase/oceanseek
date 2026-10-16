/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/tmp_file/ob_ss_tmp_file_flush_priority_manager.h"
#include "storage/tmp_file/ob_shared_storage_tmp_file.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace tmp_file
{
//----------------------- ObSSTmpFileFlushPriorityManager -------------------//
common::ObSpinLock& ObSSTmpFileFlushPriorityManager::get_data_list_lock(int64_t index)
{
  return data_list_locks_[index].get_lock();
}

int ObSSTmpFileFlushPriorityManager::init()
{
  int ret = OB_SUCCESS;
  STATIC_ASSERT(ARRAYSIZEOF(data_flush_lists_) == (int64_t)FileList::MAX,
                "data_flush_lists_ size mismatch enum FileList count");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTmpFileFlushPriorityManager inited twice", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("ObSSTmpFileFlushPriorityManager init succ", K(is_inited_));
  }
  return ret;
}

void ObSSTmpFileFlushPriorityManager::destroy()
{
  is_inited_ = false;
  for (int64_t i = 0; i < FileList::MAX; i++) {
    ObSpinLockGuard guard(get_data_list_lock(i));
    data_flush_lists_[i].reset();
  }
  LOG_INFO("ObSSTmpFileFlushPriorityManager destroy succ");
}

int64_t ObSSTmpFileFlushPriorityManager::get_file_size()
{
  int64_t size = 0;
  is_inited_ = false;
  for (int64_t i = 0; i < FileList::MAX; i++) {
    ObSpinLockGuard guard(get_data_list_lock(i));
    size += data_flush_lists_[i].get_size();
  }
  return size;
}

// attention:
// call this function with protection of ObSharedStorageTmpFile's meta_lock
int ObSSTmpFileFlushPriorityManager::insert_data_flush_list(ObSharedStorageTmpFile &file, const int64_t dirty_page_size)
{
  int ret = OB_SUCCESS;
  FileList flush_idx = FileList::MAX;
  ObTmpFileNode *flush_node = file.get_data_flush_node();

  if (OB_ISNULL(flush_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush node is null", KR(ret), K(file));
  } else if (OB_UNLIKELY(flush_node->get_next() != nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file already in flush list", KR(ret), K(flush_node));
  } else if (OB_FAIL(get_data_list_idx_(dirty_page_size, flush_idx))) {
    LOG_WARN("fail to get data list idx", KR(ret), K(dirty_page_size));
  } else if (OB_UNLIKELY(flush_idx < FileList::L1 || flush_idx >= FileList::MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush list idx", KR(ret), K(flush_idx));
  } else {
    ObSpinLockGuard guard(get_data_list_lock(flush_idx));
    file.inc_ref_cnt();
    if (OB_UNLIKELY(!data_flush_lists_[flush_idx].add_last(flush_node))) {
      file.dec_ref_cnt();
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add node to list", KR(ret), K(flush_idx), K(flush_node));
    } else {
      file.set_data_page_flush_level(flush_idx);
    }
  }

  LOG_DEBUG("insert tmp file into flush list", KR(ret), K(file), K(flush_idx));
  return ret;
}

// attention:
// call this function with protection of ObSharedStorageTmpFile's meta_lock
int ObSSTmpFileFlushPriorityManager::update_data_flush_list(ObSharedStorageTmpFile &file, const int64_t dirty_page_size)
{
  int ret = OB_SUCCESS;
  FileList new_flush_idx = FileList::MAX;
  ObTmpFileNode *flush_node = file.get_data_flush_node();
  FileList cur_flush_idx = static_cast<FileList>(file.get_data_page_flush_level());

  if (OB_ISNULL(flush_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush node is null", KR(ret), K(file));
  } else if (OB_FAIL(get_data_list_idx_(dirty_page_size, new_flush_idx))) {
    LOG_WARN("fail to get data list idx", KR(ret), K(dirty_page_size));
  } else if (cur_flush_idx < FileList::L1 || cur_flush_idx >= FileList::MAX ||
             new_flush_idx < FileList::L1 || new_flush_idx >= FileList::MAX){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid flush list idx", KR(ret), K(new_flush_idx));
  } else if (new_flush_idx == cur_flush_idx) {
    // no need to update
  } else { // need to move file into a new flush list
    bool is_in_flushing = false;
    {
      ObSpinLockGuard guard(get_data_list_lock(cur_flush_idx));
      if (OB_ISNULL(flush_node->get_next())) {
        // before we lock the list, flush task mgr has popped the node from list and is operating it, do nothing
        is_in_flushing = true;
      } else if (OB_UNLIKELY(!data_flush_lists_[cur_flush_idx].remove(flush_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to remove node from old list", KR(ret), K(cur_flush_idx));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_in_flushing) {
      // the node will be added into flush list again by flush task mgr
      // do nothing
    } else {
      ObSpinLockGuard guard(get_data_list_lock(new_flush_idx));
      if (OB_UNLIKELY(!data_flush_lists_[new_flush_idx].add_last(flush_node))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add node to new list", KR(ret), K(new_flush_idx));
      } else {
        file.set_data_page_flush_level(new_flush_idx);
      }
    }
  }

  LOG_DEBUG("update tmp file flush list", KR(ret), K(file), K(new_flush_idx));
  return ret;
}

// attention:
// call this function with protection of ObSharedStorageTmpFile's meta_lock
int ObSSTmpFileFlushPriorityManager::remove_file(ObSharedStorageTmpFile &file)
{
  int ret = OB_SUCCESS;
  FileList flush_idx = static_cast<FileList>(file.get_data_page_flush_level());
  ObTmpFileNode *flush_node = file.get_data_flush_node();

  if (FileList::INVALID == flush_idx) {
    // file doesn't exist in the flushing list
    // do nothing
  } else if (flush_idx < FileList::L1 || flush_idx >= FileList::MAX){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid flush list idx", KR(ret), K(flush_idx));
  } else if (OB_ISNULL(flush_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("flush node is null", KR(ret), K(file));
  } else {
    ObSpinLockGuard guard(get_data_list_lock(flush_idx));
    if (OB_ISNULL(flush_node->get_next())) {
      // node has not been inserted, do nothing
    } else if (OB_ISNULL(data_flush_lists_[flush_idx].remove(flush_node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove node from list", KR(ret), K(flush_idx));
    } else {
      file.dec_ref_cnt();
      file.set_data_page_flush_level(FileList::INVALID);
      LOG_DEBUG("remove file succ", K(file));
    }
  }
  return ret;
}

int ObSSTmpFileFlushPriorityManager::popN_from_file_list(const int64_t list_idx,
                                                         const int64_t expected_count, int64_t &actual_count,
                                                         ObArray<ObSSTmpFileHandle> &file_handles)
{
  int ret = OB_SUCCESS;
  actual_count = 0;

  if (OB_UNLIKELY(list_idx < FileList::L1 || list_idx >= FileList::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(list_idx));
  } else {
    ObSpinLockGuard guard(get_data_list_lock(list_idx));
    while (OB_SUCC(ret) && !data_flush_lists_[list_idx].is_empty() && actual_count < expected_count) {
      ObSharedStorageTmpFile *file = nullptr;
      if (OB_ISNULL(file = &data_flush_lists_[list_idx].remove_first()->file_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file is null", KR(ret));
      } else if (OB_FAIL(file_handles.push_back(file))) {
        LOG_WARN("fail to push back", KR(ret), KP(file));
        int tmp_ret = OB_SUCCESS;
        ObTmpFileNode *node = file->get_data_flush_node();
        if (OB_ISNULL(node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("flush node is null", KR(ret), KPC(file));
        } else if (OB_UNLIKELY(!data_flush_lists_[list_idx].add_last(node))) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to add node to list", KR(tmp_ret), K(list_idx), KP(file));
        }
      } else {
        LOG_DEBUG("pop file succ", KPC(file));
        file->dec_ref_cnt(); // ref_cnt of flush list
        actual_count++;
      }
    } // end while
  }

  return ret;
}

int ObSSTmpFileFlushPriorityManager::get_data_list_idx_(const int64_t dirty_page_size, FileList &idx)
{
  int ret = OB_SUCCESS;
  idx = FileList::MAX;
  if (dirty_page_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(dirty_page_size));
  } else if (dirty_page_size < (1 << 13)) {   // page_size 8KB, (0, 8KB)
    idx = FileList::L5;
  } else if (dirty_page_size < (1 << 17)) {   // [8KB, 128KB)
    idx = FileList::L4;
  } else if (dirty_page_size < (1 << 20)) {   // [128KB, 1MB)
    idx = FileList::L3;
  } else if (dirty_page_size < (1 << 21)) {   // [1MB, 2MB)
    idx = FileList::L2;
  } else {                                    // [2MB, INFINITE)
    idx = FileList::L1;
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
