/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_

#include "storage/tmp_file/ob_shared_storage_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace tmp_file
{
class ObSSTmpFileHandle;
class ObSharedStorageTmpFile;
class ObTmpFileNode;

class ObSSTmpFileFlushPriorityManager
{
private:
  class SpinLockWrapper
  {
  public:
    SpinLockWrapper()
      : lock_(common::ObLatchIds::OB_SS_TMP_FILE_FLUSH_PRIORITY_MANAGER_LOCK)
    {}
    ~SpinLockWrapper() = default;
    common::ObSpinLock &get_lock() { return lock_; }
  private:
    common::ObSpinLock lock_;
    DISALLOW_COPY_AND_ASSIGN(SpinLockWrapper);
  };
public:
  ObSSTmpFileFlushPriorityManager()
    : is_inited_(false),
      data_flush_lists_()
  {}
  ~ObSSTmpFileFlushPriorityManager()
  {
  }
  int init();
  void destroy();

private:
  typedef common::ObDList<ObTmpFileNode> ObTmpFileFlushList;
  friend class ObSSTmpFileFlushListIterator;

public:
  int insert_data_flush_list(ObSharedStorageTmpFile &file, const int64_t dirty_page_size);
  int update_data_flush_list(ObSharedStorageTmpFile &file, const int64_t dirty_page_size);
  int remove_file(ObSharedStorageTmpFile &file);
  int popN_from_file_list(const int64_t list_idx,
                          const int64_t expected_count, int64_t &actual_count,
                          ObArray<ObSSTmpFileHandle> &file_handles);
  int64_t get_file_size();
private:
  typedef ObTmpFileGlobal::FileList FileList;
  int get_data_list_idx_(const int64_t dirty_page_size, FileList &idx);
  common::ObSpinLock& get_data_list_lock(int64_t index);

private:
  bool is_inited_;
  ObTmpFileFlushList data_flush_lists_[FileList::MAX];
  SpinLockWrapper data_list_locks_[FileList::MAX];
  DISALLOW_COPY_AND_ASSIGN(ObSSTmpFileFlushPriorityManager);
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_FLUSH_PRIORITY_MANAGER_H_
