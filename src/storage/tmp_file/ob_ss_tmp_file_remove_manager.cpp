/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_remove_manager.h"
#include "storage/tmp_file/ob_ss_tmp_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace tmp_file
{

/* -------------------------- DeletedTmpFileInfo --------------------------- */

DeletedTmpFileInfo::DeletedTmpFileInfo(
    const blocksstable::MacroBlockId &tmp_file_id, const int64_t length)
    : tmp_file_id_(tmp_file_id), length_(length)
{
}

int64_t DeletedTmpFileInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t fd = tmp_file_id_.second_id();
  J_OBJ_START();
  J_KV(K(fd), K(length_), K(tmp_file_id_));
  J_OBJ_END();
  return pos;
}

/* -------------------------- ObSSTmpFileRemoveTimerTask --------------------------- */
int ObSSTmpFileRemoveTimerTask::init(ObSSTmpFileRemoveManager *remove_mgr, const int index)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(remove_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(remove_mgr));
  } else {
    remove_mgr_ = remove_mgr;
    index_ = index;
    is_inited_ = true;
  }
  return ret;
}

void ObSSTmpFileRemoveTimerTask::destroy()
{
  remove_mgr_ = nullptr;
  index_ = -1;
  is_inited_ = false;
}

void ObSSTmpFileRemoveTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(remove_mgr_->exec_remove_task_once(index_))) {
    LOG_WARN("fail to exec remove task once", KR(ret));
  }

  if (REACH_TIME_INTERVAL_WITH_TS(&remove_mgr_->last_print_ts(), ObTmpFileGlobal::TMP_FILE_STAT_FREQUENCY)) {
    STORAGE_LOG(INFO, "tmp file remove statistics information", KPC(remove_mgr_));
  }
}

/* -------------------------- ObSSTmpFileRemoveManager --------------------------- */
int ObSSTmpFileRemoveManager::init(const uint64_t tenant_id, const TmpFileMap *files)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_ISNULL(files))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(files));
  } else if (OB_FAIL(deleted_file_map_.init(ObMemAttr(tenant_id, "TmpFileRmMap")))) {
    LOG_WARN("fail to init deleted file map", KR(ret), K(tenant_id));
  } else {
    files_ = files;
    is_inited_ = true;
  }

  return ret;
}

int ObSSTmpFileRemoveManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(add_one_thread_())) {
    LOG_WARN("fail to add one thread", KR(ret));
  } else if (OB_FAIL(get_first_tmp_file_id_())) {
    LOG_WARN("fail to get first tmp file id", KR(ret), KPC(this));
  } else {
    ATOMIC_STORE(&is_running_, true);
  }
  return ret;
}

void ObSSTmpFileRemoveManager::stop()
{
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    if (OB_INVALID_INDEX != tg_ids_[i]) {
      TG_STOP(tg_ids_[i]);
    }
  }
  ATOMIC_STORE(&is_running_, false);
}

int ObSSTmpFileRemoveManager::wait()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
    if (OB_INVALID_INDEX != tg_ids_[i]) {
      TG_WAIT(tg_ids_[i]);
    }
  }
  return ret;
}

int ObSSTmpFileRemoveManager::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    for (int64_t i = 0; i < MAX_THREAD_NUM; ++i) {
      if (OB_INVALID_INDEX != tg_ids_[i]) {
        TG_DESTROY(tg_ids_[i]);
      }
      tg_ids_[i] = OB_INVALID_INDEX;
    }
    deleted_file_map_.destroy();
    first_tmp_file_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    is_inited_ = false;
  }
  LOG_INFO("destroy remove manager end", K(ret));
  return ret;
}

int ObSSTmpFileRemoveManager::add_one_thread_()
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(schedule_info_.latch_, ObLatchIds::TMP_FILE_MGR_LOCK);
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_NUM; ++i) {
    if (tg_ids_[i] == OB_INVALID_INDEX) {
      if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSTmpFileARemove, tg_ids_[i]))) {
        LOG_WARN("fail to create thread", KR(ret), K(tg_ids_[i]));
      } else if (OB_FAIL(TG_START(tg_ids_[i]))) {
        LOG_WARN("fail to start thread", KR(ret), K(tg_ids_[i]));
      } else if (OB_FAIL(TG_SCHEDULE(tg_ids_[i], remove_timer_tasks_[i], EXEC_REMOVE_INTERVAL, true/* repeat */))) {
        LOG_WARN("fail to schedule thread", KR(ret), K(tg_ids_[i]));
      } else {
        LOG_INFO("add one thread success", K(tg_ids_[i]));
        ATOMIC_INC(&schedule_info_.thread_count_);
      }
      break;
    }
  }
  return ret;
}

int ObSSTmpFileRemoveManager::remove_one_thread_(const int index)
{
  int ret = OB_SUCCESS;
  int tg_id_to_remove = OB_INVALID_INDEX;
  {
    ObLatchWGuard guard(schedule_info_.latch_, ObLatchIds::TMP_FILE_MGR_LOCK);
    for (int64_t i = MAX_THREAD_NUM - 1; OB_SUCC(ret) && i > index && i > 0; --i) {
      // Threads remove in ascending order to avoid deadlock
      if (tg_ids_[i] != OB_INVALID_INDEX && i != index) {
        tg_id_to_remove = tg_ids_[i];
        tg_ids_[i] = OB_INVALID_INDEX;
        LOG_INFO("remove one thread get index", K(i), K(tg_id_to_remove));
        ATOMIC_DEC(&schedule_info_.thread_count_);
        break;
      }
    }
  }
  // Stop and destroy the thread outside of the lock
  if (OB_INVALID_INDEX != tg_id_to_remove) {
    TG_STOP(tg_id_to_remove);
    TG_WAIT(tg_id_to_remove);
    TG_DESTROY(tg_id_to_remove);
  }
  LOG_INFO("remove one thread", K(ret), K(index), K(tg_id_to_remove));
  return ret;
}

int ObSSTmpFileRemoveManager::add_deleted_file(
    const blocksstable::MacroBlockId &tmp_file_id,
    const int64_t file_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tmp_file_id.is_valid() || file_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tmp_file_id), K(file_size));
  } else if (OB_FAIL(deleted_file_map_.insert(tmp_file_id, file_size))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("fail to insert deleted file", KR(ret), K(tmp_file_id), K(file_size));
    } else {
      LOG_INFO("file already exists", K(tmp_file_id), K(file_size));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSSTmpFileRemoveManager::get_first_tmp_file_id_()
{
  int ret = OB_SUCCESS;

  blocksstable::MacroBlockId max_tmp_file_id;
  max_tmp_file_id.set_storage_object_type(static_cast<uint64_t>(blocksstable::ObStorageObjectType::TMP_FILE));
  blocksstable::ObStorageObjectOpt opt;
  opt.set_ss_tmp_file_object_opt();
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(opt, max_tmp_file_id))) {
    LOG_WARN("fail to get current max tmp file id", KR(ret), K(opt));
  } else {
    first_tmp_file_id_ = max_tmp_file_id.second_id();
  }

  return ret;
}

int ObSSTmpFileRemoveManager::reinsert_files_(ObIArray<DeletedTmpFileInfo> &files)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
    DeletedTmpFileInfo * remove_task = &files.at(i);
    if (OB_FAIL(add_deleted_file(remove_task->get_tmp_file_id(), remove_task->get_length()))) {
      LOG_WARN("fail to add deleted file", KR(ret), KPC(remove_task));
    }
  }
  return ret;
}

bool ObSSTmpFileRemoveManager::GetDeletedFileOp::operator()(
    const blocksstable::MacroBlockId &tmp_file_id,
    const int64_t &file_size)
{
  int ret = OB_SUCCESS;
  if (ATOMIC_AAF(&count_, 1) <= REMOVE_BATCH_SIZE) {
    // We use INT64_MAX to represent files collected in Reboot GC or Scan GC
    DeletedTmpFileInfo remove_task(tmp_file_id, file_size);
    if (INT64_MAX != file_size && OB_FAIL(files_.push_back(remove_task))) {
      LOG_ERROR("fail to push back tmp file id", KR(ret), K(tmp_file_id), K(file_size));
    } else if (INT64_MAX == file_size && OB_FAIL(reboot_gc_files_.push_back(remove_task))) {
      LOG_ERROR("fail to push back tmp file id", KR(ret), K(tmp_file_id), K(file_size));
    }
  } else {
    ret = OB_ITER_END;
  }
  return OB_SUCCESS == ret;
}

int ObSSTmpFileRemoveManager::delete_files_with_valid_length_(ObIArray<DeletedTmpFileInfo> &deleted_files)
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::MacroBlockId> file_ids;
  ObArray<int64_t> lengths;
  if (OB_UNLIKELY(REMOVE_BATCH_SIZE < deleted_files.count() ||
      deleted_files.count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid deleted files count", KR(ret), K(deleted_files.count()));
  } else if (OB_FAIL(file_ids.reserve(REMOVE_BATCH_SIZE))) {
    LOG_WARN("fail to prepare allocate file ids", KR(ret));
  } else if (OB_FAIL(lengths.reserve(REMOVE_BATCH_SIZE))) {
    LOG_WARN("fail to prepare allocate lengths", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_files.count(); ++i) {
      const DeletedTmpFileInfo &deleted_file = deleted_files.at(i);
      if (INT64_MAX == deleted_file.get_length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid length", KR(ret), K(deleted_file));
      } else if (OB_FAIL(file_ids.push_back(deleted_file.get_tmp_file_id()))) {
        LOG_WARN("fail to push back file id", KR(ret), K(deleted_file));
      } else if (OB_FAIL(lengths.push_back(deleted_file.get_length()))) {
        LOG_WARN("fail to push back length", KR(ret), K(deleted_file));
      }
    }
    if (FAILEDx(MTL(ObTenantFileManager *)->delete_tmp_files(file_ids, lengths))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to delete tmp file", KR(ret), K(deleted_files));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

// do scan gc or reboot gc, and scan gc need to delete wild meta
int ObSSTmpFileRemoveManager::delete_reboot_gc_files_(ObIArray<DeletedTmpFileInfo> &reboot_gc_files)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < reboot_gc_files.count(); ++i) {
    const DeletedTmpFileInfo &file = reboot_gc_files.at(i);
    int64_t tmp_file_id = file.get_tmp_file_id().second_id();
    if (OB_FAIL(MTL(ObTenantFileManager *)->delete_tmp_file(file.get_tmp_file_id()))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to delete tmp file", KR(ret), K(file));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(MTL(ObTenantFileManager *)->get_segment_file_mgr().delete_wild_meta(tmp_file_id))) {
      LOG_WARN("fail to delete wild meta", KR(ret), K(file));
    }
  }
  return ret;
}

// remove REMOVE_BATCH_SIZE files at a time
int ObSSTmpFileRemoveManager::batch_delete_tmp_files_()
{
  int ret = OB_SUCCESS;
  ObArray<DeletedTmpFileInfo> deleted_files;
  ObArray<DeletedTmpFileInfo> reboot_gc_files;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(deleted_files.reserve(REMOVE_BATCH_SIZE))) {
    LOG_WARN("fail to prepare allocate deleted files", KR(ret));
  } else if (OB_FAIL(reboot_gc_files.reserve(REMOVE_BATCH_SIZE))) {
    LOG_WARN("fail to prepare allocate reboot gc files", KR(ret));
  } else {
    GetDeletedFileOp get_remove_file_op(deleted_files, reboot_gc_files);
    if (OB_FAIL(deleted_file_map_.remove_if(get_remove_file_op))) {
      // remove_if may throw OB_EGAIN after fetching REMOVE_BATCH_SIZE elements
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail to foreach deleted file map", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (deleted_files.count() > 0 &&
               OB_FAIL(delete_files_with_valid_length_(deleted_files))) {
      LOG_WARN("fail to delete tmp files", KR(ret), K(deleted_files.count()));
    } else if (reboot_gc_files.count() > 0 &&
            OB_FAIL(delete_reboot_gc_files_(reboot_gc_files))) {
      LOG_WARN("fail to delete tmp files", KR(ret), K(reboot_gc_files.count()));
    } else {
      LOG_INFO("batch delete tmp files", KR(ret), K(deleted_files.count()), K(reboot_gc_files.count()));
      deleted_files.reset();
      reboot_gc_files.reset();
    }

    int tmp_ret = OB_SUCCESS;
    if (deleted_files.count() > 0 && OB_TMP_FAIL(reinsert_files_(deleted_files))) {
      LOG_WARN("fail to reinsert files", KR(ret), K(deleted_files.count()));
    }
    if (reboot_gc_files.count() > 0 && OB_TMP_FAIL(reinsert_files_(reboot_gc_files))) {
      LOG_WARN("fail to reinsert files", KR(ret), K(reboot_gc_files.count()));
    }
  }
  return ret;
}

int ObSSTmpFileRemoveManager::exec_remove_task_once(const int index)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    // Execute scan gc.
    if (REACH_TIME_INTERVAL_WITH_TS(&scan_invalid_files_ts_, SCAN_INVALID_FILES_INTERVAL)) {
      if (OB_TMP_FAIL(scan_invalid_files_())) {
        LOG_ERROR("fail to scan invalid files", KR(tmp_ret));
      }
    }
    // Execute reboot gc.
    if (OB_TMP_FAIL(exec_reboot_gc_once_())) {
      LOG_WARN("fail to exec reboot gc once", KR(tmp_ret), K(first_tmp_file_id_));
    }

    // delete local files and remote files
    if (deleted_file_map_.count() > 0 &&
        OB_FAIL(batch_delete_tmp_files_())) {
      LOG_WARN("fail to batch remove tmp files", KR(ret));
    }
    // add immediate task to current thread if file number larger than REMOVE_BATCH_SIZE
    if (deleted_file_map_.count() > REMOVE_BATCH_SIZE * 2) {
      if (OB_TMP_FAIL(add_immediate_task_(index))) {
        LOG_WARN("fail to add immediate task", KR(tmp_ret));
      }
    }
    // add more thread if unhandled file number larger than 100k
    if (need_more_threads_()) {
      if (OB_TMP_FAIL(add_one_thread_())) {
        LOG_WARN("fail to add one thread", KR(tmp_ret));
      }
    } else if (deleted_file_map_.count() < ADD_THREAD_THRESHOLD_FILE_SIZE &&
               ATOMIC_LOAD(&schedule_info_.thread_count_) > 1) {
      if (OB_TMP_FAIL(remove_one_thread_(index))) {
        LOG_WARN("fail to remove one thread", KR(tmp_ret));
      }
    }
  }
  return ret;
}

bool ObSSTmpFileRemoveManager::need_more_threads_()
{
  int ret = OB_SUCCESS;
  bool need_more_threads = false;
  ObLatchWGuard guard(schedule_info_.latch_, ObLatchIds::TMP_FILE_MGR_LOCK);
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("fail to get ret", KR(guard.get_ret()));
  } else {
    int64_t &exceed_ts = schedule_info_.exceed_threshold_ts_;
    int64_t exceed_thread_duration = ObTimeUtility::current_time() - exceed_ts;

    // 超过阈值后开始计时；1min数量没降低到阈值下则增加线程，重新计时
    if (deleted_file_map_.count() < ADD_THREAD_THRESHOLD_FILE_SIZE) {
      ATOMIC_SET(&exceed_ts, INT64_MAX);
    } else if (INT64_MAX == exceed_ts) {
      ATOMIC_SET(&exceed_ts, ObTimeUtility::current_time());
    } else if (exceed_thread_duration > ADD_THREAD_THRESHOLD_TIME &&
              ATOMIC_LOAD(&schedule_info_.thread_count_) < MAX_THREAD_NUM) {
      need_more_threads = true;
      ATOMIC_SET(&exceed_ts, ObTimeUtility::current_time());
    }
    LOG_DEBUG("need more threads",
        KR(ret), K(need_more_threads), K(ATOMIC_LOAD(&schedule_info_.thread_count_)), K(exceed_thread_duration));
  }
  return need_more_threads;
}

int ObSSTmpFileRemoveManager::add_immediate_task_(const int index)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (index < 0 || index >= MAX_THREAD_NUM) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(index));
  } else if (tg_ids_[index] == OB_INVALID_INDEX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("thread is not running", KR(ret), K(index), K(tg_ids_[index]));
  } else if (OB_FAIL(TG_SCHEDULE(tg_ids_[index], remove_timer_tasks_[index], 0, false/* once */))) {
    LOG_WARN("fail to schedule thread", KR(ret), K(index));
  } else {
    LOG_INFO("add immediate task", K(index));
  }
  return ret;
}

// Reboot GC
int ObSSTmpFileRemoveManager::exec_reboot_gc_once_()
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(schedule_info_.reboot_latch_, ObLatchIds::TMP_FILE_MGR_LOCK);

  int64_t min_id = INT64_MAX;
  int64_t max_id = -1;
  ObArray<blocksstable::MacroBlockId> list_tmp_file_res;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_FAIL(guard.get_ret())) {
    LOG_WARN("fail to get ret", KR(guard.get_ret()));
  } else if (ObTmpFileGlobal::INVALID_TMP_FILE_FD == first_tmp_file_id_) {
    // we have rescanned after reboot if first_tmp_file_id_ is invalid
    ret = OB_SUCCESS;
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager->list_tmp_file(list_tmp_file_res))) {
    LOG_WARN("fail to list tmp file", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < list_tmp_file_res.count(); ++i) {
      const blocksstable::MacroBlockId &curr_tmp_file_id = list_tmp_file_res.at(i);
      if (curr_tmp_file_id.second_id() < first_tmp_file_id_) {
        if (OB_FAIL(add_deleted_file(curr_tmp_file_id, INT64_MAX))) {
          LOG_WARN("fail to add files into deleted_file_map_ in reboot gc",
            KR(ret), K(first_tmp_file_id_), K(curr_tmp_file_id));
        } else {
          LOG_INFO("previous temporary file add into deleted_file_map_", K(curr_tmp_file_id));
        }
      } else if (curr_tmp_file_id.second_id() == first_tmp_file_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected tmp file id", KR(ret), K(first_tmp_file_id_), K(curr_tmp_file_id));
      } else {
        // new temporary file after this reboot, do nothing.
        min_id = min(min_id, curr_tmp_file_id.second_id());
        max_id = max(max_id, curr_tmp_file_id.second_id());
      }
    }
    first_tmp_file_id_ = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
    FLOG_INFO("finish reboot gc",
        KR(ret), K(first_tmp_file_id_), K(min_id), K(max_id), K(list_tmp_file_res.count()));
  }
  return ret;
}

// Scan GC
int ObSSTmpFileRemoveManager::scan_invalid_files_()
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::MacroBlockId> list_tmp_file_res;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(MTL(ObTenantFileManager *)->list_tmp_file(list_tmp_file_res))) {
    LOG_WARN("fail to list tmp file", KR(ret));
  } else {
    int64_t min_id = INT64_MAX;
    int64_t max_id = -1;
    for (int64_t i = 0; i < list_tmp_file_res.count() && OB_SUCC(ret); ++i) {
      const blocksstable::MacroBlockId &file_id = list_tmp_file_res.at(i);
      ObSSTmpFileHandle tmp_file_handle;
      if (OB_FAIL(files_->get(ObTmpFileKey(file_id.second_id()), tmp_file_handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get tmp file handle", KR(ret), K(file_id));
        } else if (OB_FAIL(add_deleted_file(file_id, INT64_MAX))) { // find a invalid remain file
          LOG_WARN("fail to add invalid file into deleted_file_map_ in scan gc", KR(ret), K(file_id));
        } else {
          LOG_INFO("invalid temporary file add into deleted_file_map_ in scan gc", K(file_id));
        }
        ret = OB_SUCCESS;
      }
      min_id = min(min_id, file_id.second_id());
      max_id = max(max_id, file_id.second_id());
    }
    FLOG_INFO("scan invalid files id range", KR(ret), K(min_id), K(max_id), K(list_tmp_file_res.count()));
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase