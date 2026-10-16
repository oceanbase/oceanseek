/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_REMOVE_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_REMOVE_MANAGER_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/task/ob_timer.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_i_tmp_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{
class ObSSTmpFileRemoveManager;

class DeletedTmpFileInfo
{
public:
  DeletedTmpFileInfo() : tmp_file_id_(), length_(INT64_MAX) {}
  explicit DeletedTmpFileInfo(const blocksstable::MacroBlockId &tmp_file_id, const int64_t length);
  ~DeletedTmpFileInfo() = default;
  DeletedTmpFileInfo &operator=(const DeletedTmpFileInfo &other)
  {
    if (this != &other) {
      tmp_file_id_ = other.tmp_file_id_;
      length_ = other.length_;
    }
    return *this;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;
  OB_INLINE const blocksstable::MacroBlockId &get_tmp_file_id() const { return tmp_file_id_; }
  OB_INLINE int64_t get_length() const { return length_; }
private:
  blocksstable::MacroBlockId tmp_file_id_;
  int64_t length_;
};

class ObSSTmpFileRemoveTimerTask : public common::ObTimerTask
{
public:
  ObSSTmpFileRemoveTimerTask()
    : is_inited_(false),
      index_(-1),
      remove_mgr_(nullptr) {}
  virtual ~ObSSTmpFileRemoveTimerTask() { destroy(); }
  int init(ObSSTmpFileRemoveManager *remove_mgr, const int index);
  void destroy();
  virtual void runTimerTask() override;
  TO_STRING_KV(K(is_inited_), K(index_), KP(remove_mgr_));
private:
  bool is_inited_;
  int index_;
  ObSSTmpFileRemoveManager *remove_mgr_;
};

// Remove tmp file segments in oss and local disk in async style after ObTenantTmpFileManager::remove is called
class ObSSTmpFileRemoveManager
{
private:
  typedef ObITenantTmpFileManager::TmpFileMap TmpFileMap;
  typedef ObLinearHashMap<blocksstable::MacroBlockId, int64_t> DeletedFileMap;
  static const int64_t SCAN_INVALID_FILES_INTERVAL = 12 * 60L * 60 * 1000 * 1000; // 12 hours
  static const int64_t ADD_THREAD_THRESHOLD_TIME = 30 * 1000 * 1000; // 30s
  static const int64_t ADD_THREAD_THRESHOLD_FILE_SIZE = 100000;
  static const int64_t EXEC_REMOVE_INTERVAL = 1 * 1000 * 1000; // 1s
  static const int64_t REMOVE_BATCH_SIZE = 1000;
  static const int64_t MAX_THREAD_NUM = 8;
public:
  ObSSTmpFileRemoveManager() : is_inited_(false),
                               is_running_(false),
                               last_print_ts_(ObTimeUtility::current_time()),
                               scan_invalid_files_ts_(ObTimeUtility::current_time()),
                               first_tmp_file_id_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
                               schedule_info_(),
                               tg_ids_(),
                               remove_timer_tasks_(),
                               deleted_file_map_() {
    for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
      tg_ids_[i] = OB_INVALID_INDEX;
      remove_timer_tasks_[i].init(this, i);
    }
  }
  virtual ~ObSSTmpFileRemoveManager() {}
  int init(const uint64_t tenant_id, const TmpFileMap *files);
  int start();
  void stop();
  int wait();
  int destroy();
  int exec_remove_task_once(const int index);
  int add_deleted_file(const blocksstable::MacroBlockId &tmp_file_id, const int64_t file_size);

  OB_INLINE int64_t get_task_num() const { return deleted_file_map_.count(); }
  OB_INLINE bool have_task() const { return deleted_file_map_.count() > 0; }
  OB_INLINE int64_t &last_print_ts() { return last_print_ts_; }
  TO_STRING_KV(K(is_inited_), K(is_running_), KP(files_), K(first_tmp_file_id_),
               K(deleted_file_map_.count()), K(schedule_info_));
private:
  bool need_more_threads_();
  int add_immediate_task_(const int index);
  int add_one_thread_();
  int remove_one_thread_(const int index);
  int scan_invalid_files_();
  int batch_delete_tmp_files_();
  int exec_reboot_gc_once_();
  int get_first_tmp_file_id_();
  int reinsert_files_(ObIArray<DeletedTmpFileInfo> &files);
  int delete_files_with_valid_length_(ObIArray<DeletedTmpFileInfo> &deleted_files);
  int delete_reboot_gc_files_(ObIArray<DeletedTmpFileInfo> &reboot_gc_files);
  struct GetDeletedFileOp
  {
    GetDeletedFileOp(ObIArray<DeletedTmpFileInfo> &files,
                     ObIArray<DeletedTmpFileInfo> &reboot_gc_files)
      : count_(0), files_(files), reboot_gc_files_(reboot_gc_files) {}
    bool operator()(const blocksstable::MacroBlockId &tmp_file_id, const int64_t &file_size);
    int64_t count_;
    ObIArray<DeletedTmpFileInfo> &files_;
    ObIArray<DeletedTmpFileInfo> &reboot_gc_files_; // file size is INVALID(INT64_MAX)
  };
  struct ThreadScheduleInfo {
    ThreadScheduleInfo() :
      thread_count_(0),
      exceed_threshold_ts_(INT64_MAX),
      latch_(),
      reboot_latch_() {}
    TO_STRING_KV(K(thread_count_), K(exceed_threshold_ts_));
    int64_t thread_count_;
    int64_t exceed_threshold_ts_;
    common::ObLatch latch_;
    common::ObLatch reboot_latch_;
  };

private:
  bool is_inited_;
  bool is_running_;
  int64_t last_print_ts_;
  const TmpFileMap *files_;
  int64_t scan_invalid_files_ts_;
  // record newest valid tmp file id after reboot,
  // file ids smaller than this id are invalid files and need to be gc.
  int64_t first_tmp_file_id_;
  ThreadScheduleInfo schedule_info_;
  int tg_ids_[MAX_THREAD_NUM];
  ObSSTmpFileRemoveTimerTask remove_timer_tasks_[MAX_THREAD_NUM];
  // <tmp file id, file size>
  // record file size for faster deletion, otherwise io manager has to
  // list all segments of a tmp file to delete the file, which is slow.
  DeletedFileMap deleted_file_map_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SS_TMP_FILE_REMOVE_MANAGER_H_