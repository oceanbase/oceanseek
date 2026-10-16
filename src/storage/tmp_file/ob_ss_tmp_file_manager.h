/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_MANAGER_H_

#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "storage/tmp_file/ob_ss_tmp_file_write_buffer_pool.h"
#include "storage/tmp_file/ob_shared_storage_tmp_file.h"
#include "storage/tmp_file/ob_i_tmp_file_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_flush_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_remove_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_shrink_manager.h"

namespace oceanbase
{
namespace tmp_file
{

class ObSSTenantTmpFileManager : public ObITenantTmpFileManager
{
public:
  ObSSTenantTmpFileManager();
  virtual ~ObSSTenantTmpFileManager();

  virtual int alloc_dir(int64_t &dir_id) override;
  virtual int open(int64_t &fd, const int64_t &dir_id, const char* const label) override;
  int seal(const int64_t fd,
           ObITmpFileHandle* file_handle_hint = nullptr);
  int get_tmp_file(const int64_t fd, ObSSTmpFileHandle &handle) const;
  virtual int get_tmp_file_disk_usage(int64_t &disk_data_size, int64_t &occupied_disk_size) override;
  virtual int get_suggested_max_tmp_file_num(int64_t& suggested_max_tmp_file_num,
                const int64_t write_cache_size_expected_reside_in_memory) override;

  // TODO: wanyue.wy
  // remove this function when ObSSTmpFileAsyncFlushWaitTaskHandle has been removed
  OB_INLINE ObIAllocator * get_wait_task_allocator() { return &flush_mgr_.get_wait_task_allocator(); }

private:
  virtual int init_sub_module_() override;
  virtual int start_sub_module_() override;
  virtual int stop_sub_module_() override;
  virtual int wait_sub_module_() override;
  virtual int destroy_sub_module_() override;

private:
  class ObTmpFileDiskUsageCalculator final
  {
  public:
    ObTmpFileDiskUsageCalculator() : disk_data_size_(0), occupied_disk_size_(0) {}
    bool operator()(const ObTmpFileKey &key,
                    ObITmpFileHandle &tmp_file_handle);
    OB_INLINE int64_t get_disk_data_size() const { return disk_data_size_; }
    OB_INLINE int64_t get_occupied_disk_size() const { return occupied_disk_size_; }

  private:
    int64_t disk_data_size_;
    int64_t occupied_disk_size_;
  };

private:
  ObSSTmpWriteBufferPool wbp_;
  ObSSTmpFileFlushManager flush_mgr_;
  ObSSTmpFileRemoveManager remove_mgr_;
  ObSSTmpFileShrinkManager shrink_mgr_;
  common::ObFIFOAllocator wbp_index_cache_allocator_;
  common::ObFIFOAllocator wbp_index_cache_bucket_allocator_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_MANAGER_H_