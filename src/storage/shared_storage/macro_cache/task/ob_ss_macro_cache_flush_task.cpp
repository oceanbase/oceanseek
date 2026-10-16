/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_flush_task.h"
#include "share/config/ob_server_config.h"
#include "share/io/ob_io_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_flush_map.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/tmp_file/ob_tmp_file_global.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

/*--------------------------------- ObSSMacroCacheFlushMeta ---------------------------------*/
void ObSSMacroCacheFlushMeta::reset()
{
  file_id_.reset();
  file_len_ = 0;
  func_type_ = 0;
  is_sealed_ = true;
  valid_length_ = 0;
}

ObSSMacroCacheFlushMeta &ObSSMacroCacheFlushMeta::operator=(const ObSSMacroCacheFlushMeta &other)
{
  if (this != &other) {
    file_id_ = other.file_id_;
    file_len_ = other.file_len_;
    func_type_ = other.func_type_;
    is_sealed_ = other.is_sealed_;
    valid_length_ = other.valid_length_;
  }
  return *this;
}

/*---------------------------------- ObSSMacroCacheFlushEntry ----------------------------------*/
int ObSSMacroCacheFlushEntry::init(const ObSSMacroCacheFlushMeta &flush_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((nullptr != buf_) || read_handle_.is_valid() || write_handle_.is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(*this));
  } else if (OB_UNLIKELY(!flush_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(flush_meta));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(flush_meta.file_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K_(flush_meta.file_len));
  } else {
    read_handle_.reset();
    write_handle_.reset();
    flush_meta_ = flush_meta;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObSSMacroCacheFlushEntry::reset()
{
  flush_meta_.reset();
  read_handle_.reset();
  write_handle_.reset();
  if (nullptr != buf_) {
    // Need to pay attention!!!
    // The allocator is used to allocate io data buffer, and its memory lifecycle needs to be longer than the object handle.
    allocator_.free(buf_);
    buf_ = nullptr;
  }
}

/*----------------------------------- ObSSMacroCacheFlushTaskBase -----------------------------------*/
ObSSMacroCacheFlushTaskBase::ObSSMacroCacheFlushTaskBase(
    ObSSMacroCacheMgr &macro_cache_mgr,
    volatile bool &is_stopped)
  : ObSSMacroCacheTaskBase(is_stopped), macro_cache_mgr_(macro_cache_mgr), tg_id_(-1),
    flush_parallel_cnt_(0), interval_us_(-1), file_manager_(nullptr), flush_entry_arr_(nullptr),
    free_list_(), async_read_list_(), async_write_list_(), files_to_flush_(), flushed_files_(),
    allocator_()
{}

int ObSSMacroCacheFlushTaskBase::init(ObTenantFileManager *file_manager)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "SSFlushTask");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss_tmp_file flush task init twice", KR(ret));
  } else if (OB_ISNULL(file_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(file_manager));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(), OB_MALLOC_BIG_BLOCK_SIZE, attr))) {
    LOG_WARN("fail to init allocatro", KR(ret));
  } else if (OB_FAIL(free_list_.init(MAX_FLUSH_PARALLELISM))) {
    LOG_WARN("fail to init free_list", KR(ret));
  } else if (OB_FAIL(async_write_list_.init(MAX_FLUSH_PARALLELISM))) {
    LOG_WARN("fail to init async_write_list", KR(ret));
  } else if (OB_FAIL(async_read_list_.init(MAX_FLUSH_PARALLELISM))) {
    LOG_WARN("fail to init async_read_list", KR(ret));
  } else if (OB_FAIL(pre_alloc_flush_entry())) {
    LOG_WARN("fail to pre alloc flush_entry", KR(ret));
  } else {
    file_manager_ = file_manager;
    interval_us_ = SLOW_SCHEDULE_INTERVAL_US;
    is_inited_ = true;
    set_flush_parallel_cnt();
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  LOG_INFO("finish to init ss macro cache flush task", KR(ret), K_(flush_parallel_cnt), K(MTL_MEM_SIZE()));
  return ret;
}

int ObSSMacroCacheFlushTaskBase::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tmp file flush task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (FALSE_IT(tg_id_ = tg_id)) {
  } else if (OB_FAIL(schedule_flush_task())) {
    LOG_WARN("fail to schedule flush task", KR(ret));
  } else {
    LOG_INFO("succ to start flush task", K(tg_id));
  }
  return ret;
}

void ObSSMacroCacheFlushTaskBase::destroy()
{
  destroy_flush_entry_arr();
  tg_id_ = -1;
  flush_parallel_cnt_ = 0;
  interval_us_ = -1;
  file_manager_ = nullptr;
  free_list_.destroy();
  async_read_list_.destroy();
  async_write_list_.destroy();
  files_to_flush_.destroy();
  flushed_files_.destroy();
  is_inited_ = false;
}

int ObSSMacroCacheFlushTaskBase::schedule_flush_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(schedule_task_(*this, tg_id_, interval_us_, false/*repeat*/))) {
    LOG_WARN("fail to schedule macro cache flush task", KR(ret), K_(tg_id), K_(interval_us));
  } else {
    LOG_TRACE("succ to schedule macro cache flush task", K_(tg_id), K_(interval_us));
  }
  return ret;
}

void ObSSMacroCacheFlushTaskBase::runTimerTask()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tmp file flush task is not inited", KR(ret));
  } else if (!macro_cache_mgr_.is_ckpt_replayed()) {
    LOG_INFO("macro cache ckpt has not been replayed, cannot do macro cache flush task now");
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_FAIL(flush_files())) {
    LOG_WARN("fail to flush files", KR(ret));
  }

  // adjust flush_task_parallel_cnt if memory size changes.
  if (TC_REACH_TIME_INTERVAL(UPDATE_PARALLEL_INTERVAL_US)) {
    set_flush_parallel_cnt();
  }

  adjust_flush_task_interval();
  if (OB_TMP_FAIL(schedule_flush_task())) {
    if (OB_CANCELED == tmp_ret) {
      LOG_INFO("flush task has been stopped");
    } else {
      LOG_ERROR("fail to reschedule flush task", KR(ret), K(tmp_ret), K_(interval_us));
    }
  }
}

void ObSSMacroCacheFlushTaskBase::destroy_flush_entry_arr()
{
  if (nullptr != flush_entry_arr_) {
    for (int64_t i = 0; i < MAX_FLUSH_PARALLELISM; ++i) {
      flush_entry_arr_[i].~ObSSMacroCacheFlushEntry();
    }
    allocator_.free(flush_entry_arr_);
    flush_entry_arr_ = nullptr;
  }
}

int ObSSMacroCacheFlushTaskBase::pre_alloc_flush_entry()
{
  int ret = OB_SUCCESS;
  const int64_t mem_size = sizeof(ObSSMacroCacheFlushEntry) * MAX_FLUSH_PARALLELISM;
  if (OB_ISNULL(flush_entry_arr_ = static_cast<ObSSMacroCacheFlushEntry *>(allocator_.alloc(mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(mem_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < MAX_FLUSH_PARALLELISM); ++i) {
      ObSSMacroCacheFlushEntry *flush_entry = new (flush_entry_arr_ + i) ObSSMacroCacheFlushEntry(allocator_);
      if (OB_FAIL(free_list_.push(flush_entry))) {
        LOG_WARN("fail to push flush_entry into free_list", K(ret), K(i), K(free_list_.get_curr_total()));
      }
    }
  }

  if (OB_FAIL(ret)) {
    destroy_flush_entry_arr();
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::flush_files()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  clear_for_next_round();
  const int64_t start_us = ObTimeUtility::current_time();
  if (OB_FAIL(batch_get_files())) {
    LOG_WARN("fail to batch get files", KR(ret));
  } else if (OB_FAIL(async_read_files())) {
    LOG_WARN("fail to async read files", KR(ret));
  } else if (OB_FAIL(async_write_files())) {
    LOG_WARN("fail to async write files", KR(ret));
  } else if (OB_FAIL(complete_flush_files())) {
    LOG_WARN("fail to complete flush files", KR(ret));
  }
  if (flushed_files_.count() > 0) {
    const int64_t cost_us = ObTimeUtility::current_time() - start_us;
    LOG_INFO("finish flush task", KR(ret), K(cost_us), "flushed_files_cnt", flushed_files_.count(),
             K_(flushed_files), "async_read_list_total", async_read_list_.get_curr_total(),
             "async_write_list_total", async_write_list_.get_curr_total());
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::batch_get_files()
{
  int ret = OB_SUCCESS;
  const int64_t total_entry_cnt = free_list_.get_curr_total() +
                                  async_read_list_.get_curr_total() +
                                  async_write_list_.get_curr_total();
  if (OB_UNLIKELY(total_entry_cnt != MAX_FLUSH_PARALLELISM)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("flush entry leak!!!", KR(ret), K(total_entry_cnt));
  }
  const int64_t max_cnt = MIN(flush_parallel_cnt_, free_list_.get_curr_total());
  ObSSMacroCacheFlushMap &flush_map = get_flush_map();
  for (int64_t i = 0; (i < max_cnt) && OB_SUCC(ret); ++i) {
    ObSSMacroCacheFlushInfo flush_info;
    if (OB_FAIL(flush_map.get_next_flush_entry(flush_info))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;  // ignore ret
        break;
      } else {
        LOG_WARN("fail to get next flush entry", KR(ret));
      }
    } else {
      if (SSObjUtil::is_tmp_file(flush_info.get_macro_id())) { // 1. write cache of tmp file
        if (OB_FAIL(construct_tmp_file_flush_meta(flush_info))) {
          LOG_WARN("fail to construct tmp file flush meta", KR(ret), K(flush_info));
        }
      } else { // 2. write cache other than tmp file
        if (OB_FAIL(construct_other_file_flush_meta(flush_info))) {
          LOG_WARN("fail to construct other file flush meta", KR(ret), K(flush_info));
        }
      }
    }

    if (OB_FAIL(ret) && flush_info.is_valid()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(callback_and_erase_from_flush_map(flush_info.get_macro_id(), ret))) {
        LOG_WARN("fail to execute callback and erase from flush map", KR(tmp_ret), K(flush_info), K(ret));
      }
    }

    // ignore ret, go on process previous flush metas that were successfully constructed.
    // otherwise, flush_entry's is_flushing is true, but will not be flushed.
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObSSMacroCacheFlushTaskBase::construct_tmp_file_flush_meta(
    const ObSSMacroCacheFlushInfo &flush_info)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = flush_info.get_macro_id();
  TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
  bool is_meta_exist = false;
  TmpFileMetaHandle meta_handle;
  ObSSMacroCacheFlushMeta flush_meta;
  bool is_sealed = false;
  int64_t valid_length = 0;
  int64_t file_len = 0;
  bool need_erase = false;
  if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_manager is null", KR(ret), KP_(file_manager));
  } else if (OB_FAIL(file_manager_->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to get seg meta", KR(ret), K(seg_id), K(meta_handle));
  } else if (!is_meta_exist) {
    // do nothing, seg_meta is not existed, maybe segment file has been gc, cannot flush to object storage
    need_erase = true;
    LOG_INFO("the segment file meta does not exist", K(flush_meta));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta handle should not be invalid", KR(ret), K(meta_handle));
  } else if (FALSE_IT(is_sealed = meta_handle.is_sealed())) { // must check is_sealed before get file_len
  } else if (FALSE_IT(valid_length = meta_handle.get_valid_length())) {
  // Note: TMP_FILE segment is 8KB aligned, thus we can upper_align file_len with 8KB
  } else if (FALSE_IT(file_len = upper_align(valid_length, tmp_file::ObTmpFileGlobal::PAGE_SIZE))) {
  } else if (FALSE_IT(flush_meta = ObSSMacroCacheFlushMeta(file_id,
                                                           file_len,
                                                           flush_info.get_flush_status().get_func_type(),
                                                           is_sealed,
                                                           valid_length))) {
  } else if (!is_sealed) {
    // Note: does not ensure no appending during flush. need check appending again after flush
    if (meta_handle.is_tmp_file_appending(valid_length)) {
      // do nothing, seg file is appending, cannot flush to object storage
      need_erase = true;
    } else if (OB_FAIL(files_to_flush_.push_back(flush_meta))) {
      LOG_WARN("fail to push back", KR(ret), K(flush_meta));
    }
  } else if (OB_FAIL(files_to_flush_.push_back(flush_meta))) {
    LOG_WARN("fail to push back", KR(ret), K(flush_meta));
  }

  if (need_erase) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(erase_from_flush_map(file_id))) { // expect won't fail
      LOG_ERROR("fail to erase from flush map", KR(tmp_ret), K(file_id));
    }
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::construct_other_file_flush_meta(
    const ObSSMacroCacheFlushInfo &flush_info)
{
  int ret = OB_SUCCESS;
  int64_t file_len = 0;
  ObSSMacroCacheFlushMeta flush_meta;
  bool is_file_exist = false;
  const MacroBlockId &file_id = flush_info.get_macro_id();
  if (OB_FAIL(try_get_file_length(file_id, is_file_exist, file_len))) {
    LOG_WARN("fail to try get file length", KR(ret), K(file_id));
  } else if (!is_file_exist) {
    // do nothing
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(callback_and_erase_from_flush_map(file_id, OB_SUCCESS/*ret_code*/))) {
      LOG_WARN("fail to execute callback and erase from flush map", KR(tmp_ret), K(file_id));
    }
  } else if (FALSE_IT(flush_meta = ObSSMacroCacheFlushMeta(file_id,
                                                           file_len,
                                                           flush_info.get_flush_status().get_func_type(),
                                                           true/*is_sealed*/,
                                                           0/*valid_length*/))) {
  } else if (OB_FAIL(files_to_flush_.push_back(flush_meta))) {
    LOG_WARN("fail to push back", KR(ret), K(flush_meta));
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::do_async_read_file(ObSSMacroCacheFlushEntry &flush_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!flush_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("flush_entry should be valid", KR(ret), K(flush_entry));
  } else if (OB_UNLIKELY(flush_entry.is_read_handle_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_handle should not be valid", KR(ret), K_(flush_entry.read_handle));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(get_function_type(flush_entry));
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = flush_entry.flush_meta_.file_id_;
    read_info.offset_ = 0;
    read_info.size_ = flush_entry.flush_meta_.file_len_;
    read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.buf_ = flush_entry.buf_;
    read_info.mtl_tenant_id_ = MTL_ID();
    ObSSLocalCacheReader local_cache_reader;
    if (OB_FAIL(local_cache_reader.aio_read(read_info, flush_entry.read_handle_))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        // has already been gc
        LOG_INFO("seg file does not exist, may be already gc", K(flush_entry));
      } else {
        LOG_WARN("fail to aio read", KR(ret), K(read_info), K(flush_entry));
      }
    }
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::async_read_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t seg_file_cnt = files_to_flush_.count();
  for (int64_t i = 0; (i < seg_file_cnt) && OB_SUCC(ret); ++i) {
    ObSSMacroCacheFlushEntry *flush_entry = nullptr;
    const ObSSMacroCacheFlushMeta &flush_meta = files_to_flush_.at(i);
    bool need_recycle = true;
    if (OB_FAIL(free_list_.pop(flush_entry))) {
      LOG_WARN("fail to pop flush_entry from free_list", KR(ret), K(free_list_.get_curr_total()));
    } else if (OB_ISNULL(flush_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry should not be null", KR(ret), KP(flush_entry));
    } else if (OB_FAIL(flush_entry->init(flush_meta))) {
      LOG_WARN("fail to init flush_entry", KR(ret), K(flush_meta), KPC(flush_entry));
    } else if (OB_FAIL(do_async_read_file(*flush_entry))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) { // file has been gc, ignore ret.
        ret = OB_SUCCESS;
        if (OB_TMP_FAIL(callback_and_erase_from_flush_map(flush_entry->flush_meta_.file_id_, OB_SUCCESS/*ret_code*/))) {
          LOG_WARN("fail to execute callback and erase from flush map", KR(tmp_ret), "file_id",
                   flush_entry->flush_meta_.file_id_);
        }
      } else {
        LOG_WARN("fail to do async read seg_file", KR(ret), KPC(flush_entry));
      }
    } else if (OB_FAIL(async_read_list_.push(flush_entry))) {
      LOG_ERROR("fail to push flush_entry into read_list", KR(ret), K(async_read_list_.get_curr_total()));
    } else {
      need_recycle = false;
    }

    if (OB_FAIL(ret)) {
      if (OB_TMP_FAIL(callback_and_erase_from_flush_map(flush_meta.file_id_, ret))) {
        LOG_WARN("fail to execute callback and erase from flush map", KR(tmp_ret), "file_id",
                 flush_meta.file_id_, K(ret));
      }
    }
    if (need_recycle && (nullptr != flush_entry)) {
      if (OB_TMP_FAIL(recycle_flush_entry(*flush_entry))) {
        LOG_WARN("fail to recycle flush_entry", KR(tmp_ret), KPC(flush_entry), K(free_list_.get_curr_total()));
      }
    }

    ret = OB_SUCCESS; // ignore ret, process next seg_file.
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::do_async_write_file(ObSSMacroCacheFlushEntry &flush_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!flush_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("flush_entry should be valid", KR(ret), K(flush_entry));
  } else if (OB_UNLIKELY(!flush_entry.is_read_handle_valid() || flush_entry.is_write_handle_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read_handle should be valid and write_handle should be invalid", KR(ret), K(flush_entry));
  } else {
    CONSUMER_GROUP_FUNC_GUARD(get_function_type(flush_entry));
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = flush_entry.read_handle_.get_buffer();
    write_info.offset_ = 0;
    write_info.size_ = flush_entry.read_handle_.get_data_size();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(write_info.mtl_tenant_id_);
    if (SSObjUtil::is_tmp_file(flush_entry.flush_meta_.file_id_) && !flush_entry.flush_meta_.is_sealed_) {
      write_info.io_desc_.set_unsealed();
      // unsealed tmp file write must set tmp_file_valid_length used for concat file path
      write_info.set_tmp_file_valid_length(flush_entry.flush_meta_.valid_length_);
    }
    ObSSObjectStorageWriter object_storage_writer;
    if (OB_FAIL(flush_entry.write_handle_.set_macro_block_id(flush_entry.flush_meta_.file_id_))) {
      LOG_WARN("fail to set macro block id", KR(ret), K(flush_entry));
    } else if (OB_FAIL(object_storage_writer.aio_write(write_info, flush_entry.write_handle_))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K_(flush_entry.write_handle));
    }
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::async_write_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t io_cnt = async_read_list_.get_curr_total();
  for (int64_t i = 0; (i < io_cnt) && OB_SUCC(ret); ++i) {
    ObSSMacroCacheFlushEntry *flush_entry = nullptr;
    bool need_recycle = true;
    bool is_finished = false;
    if (OB_FAIL(async_read_list_.pop(flush_entry))) {
      LOG_WARN("fail to pop flush_entry from async_read_list", KR(ret), K(i), K(io_cnt));
    } else if (OB_ISNULL(flush_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry should not be null", KR(ret), KP(flush_entry));
    } else if (OB_UNLIKELY(!flush_entry->is_valid() || !flush_entry->is_read_handle_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry and read_handle should be valid", KR(ret), KPC(flush_entry));
    } else if (OB_FAIL(flush_entry->read_handle_.check_is_finished(is_finished))) {
      LOG_WARN("fail to check read_handle is_finished", KR(ret), K_(flush_entry->read_handle));
    } else {
      if (is_finished) {
        if (OB_FAIL(flush_entry->read_handle_.wait())) {
          LOG_WARN("fail to wait read_handle", KR(ret), K_(flush_entry->read_handle));
        } else if (OB_FAIL(do_async_write_file(*flush_entry))) {
          LOG_WARN("fail do async write seg_file", KR(ret), KPC(flush_entry));
        } else if (OB_FAIL(async_write_list_.push(flush_entry))) {
          LOG_ERROR("fail to push flush_entry into write_list", KR(ret), K(async_write_list_.get_curr_total()));
        } else {
          need_recycle = false;
        }
      } else {
        if (OB_FAIL(async_read_list_.push(flush_entry))) {
          LOG_ERROR("fail to push flush_entry into async_read_list", KR(ret), K(async_read_list_.get_curr_total()));
        } else {
          need_recycle = false;
        }
      }
    }

    if (OB_FAIL(ret) && (nullptr != flush_entry) && flush_entry->is_valid()) {
      const ObSSMacroCacheFlushMeta &flush_meta = flush_entry->flush_meta_;
      if (OB_TMP_FAIL(callback_and_erase_from_flush_map(flush_meta.file_id_, ret))) {
        LOG_WARN("fail to execute callback and erase from flush map", KR(tmp_ret), K(flush_meta), K(ret));
      }
    }
    if (need_recycle && (nullptr != flush_entry)) {
      if (OB_TMP_FAIL(recycle_flush_entry(*flush_entry))) {
        LOG_WARN("fail to recycle flush_entry", KR(tmp_ret), KPC(flush_entry), K(free_list_.get_curr_total()));
      }
    }

    ret = OB_SUCCESS; // ignore ret, process next flush_entry
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::complete_flush_files()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t io_cnt = async_write_list_.get_curr_total();
  for (int64_t i = 0; (i < io_cnt) && OB_SUCC(ret); ++i) {
    ObSSMacroCacheFlushEntry *flush_entry = nullptr;
    bool need_recycle = true;
    bool is_finished = false;
    if (OB_FAIL(async_write_list_.pop(flush_entry))) {
      LOG_WARN("fail to pop flush_entry from async_write_list", KR(ret), K(i), K(io_cnt));
    } else if (OB_ISNULL(flush_entry)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry should not be null", KR(ret), KP(flush_entry));
    } else if (OB_UNLIKELY(!flush_entry->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("flush_entry should be valid", KR(ret), KPC(flush_entry));
    } else if (OB_UNLIKELY(!flush_entry->is_read_handle_valid() || !flush_entry->is_write_handle_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read_handle and write_handle should be valid", KR(ret), KPC(flush_entry));
    } else if (OB_FAIL(flush_entry->write_handle_.check_is_finished(is_finished))) {
      LOG_WARN("fail to check io_handle is_finished", KR(ret), K_(flush_entry->write_handle));
    } else {
      if (is_finished) {
        const ObStorageObjectType object_type = flush_entry->flush_meta_.file_id_.storage_object_type();
        if (OB_FAIL(flush_entry->write_handle_.wait())) {
          LOG_WARN("fail to wait write_handle", KR(ret), K(flush_entry->write_handle_));
        } else if (SSObjUtil::is_tmp_file(flush_entry->flush_meta_.file_id_)) { // 1. TMP_FILE
          // update flushed_valid_length and is_flushed_seg_sealed of TmpFileMeta
          if (OB_FAIL(update_flush_info_of_tmp_file_meta(*flush_entry))) {
            LOG_WARN("fail to update flush info of tmp file meta", KR(ret), KPC(flush_entry));
          }
        } else if (ObStorageObjectType::PRIVATE_SLOG_FILE == object_type) { // 2. PRIVATE_SLOG_FILE
          if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(flush_entry->flush_meta_.file_id_, 0/*ls_epoch_id*/))) {
            LOG_WARN("fail to delete local file", KR(ret), KPC(flush_entry));
          } else if (OB_ISNULL(file_manager_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("file_manager is null", KR(ret), KP_(file_manager));
          } else if (OB_FAIL(file_manager_->update_flushed_slog_seq(flush_entry->flush_meta_.file_id_.fifth_id()))) {
            LOG_WARN("fail to update flushed slog seq", KR(ret), KPC(flush_entry));
          }
        } else { // 3. non-TMP_FILE and non-PRIVATE_SLOG_FILE
          // modify ObSSMacroCacheMeta from write_cache to read_cache, which can be evicted next time
          if (OB_FAIL(modify_from_write_cache_to_read_cache(flush_entry->flush_meta_.file_id_))) {
            LOG_WARN("fail to modify from write cache to read cache", KR(ret), KPC(flush_entry));
          }
        }
        if (OB_SUCC(ret)) {
          // used to count which tmp_files have completed flushing in a round.
          if (OB_TMP_FAIL(flushed_files_.push_back(flush_entry->flush_meta_))) {
            LOG_WARN("fail to push file_meta into flushed_file_arr", KR(tmp_ret), KPC(flush_entry));
          }
          if (OB_TMP_FAIL(callback_and_erase_from_flush_map(flush_entry->flush_meta_.file_id_, OB_SUCCESS/*ret_code*/))) {
            LOG_WARN("fail to execute callback and erase from flush map", KR(tmp_ret), "file_id",
                     flush_entry->flush_meta_.file_id_);
          }
        }
      } else {
        if (OB_FAIL(async_write_list_.push(flush_entry))) {
          LOG_ERROR("fail to push flush_entry into async_write_list", KR(ret), K(async_write_list_.get_curr_total()));
        } else {
          need_recycle = false;
        }
      }
    }

    if (OB_FAIL(ret) && (nullptr != flush_entry) && flush_entry->is_valid()) {
      const ObSSMacroCacheFlushMeta &flush_meta = flush_entry->flush_meta_;
      if (OB_TMP_FAIL(callback_and_erase_from_flush_map(flush_meta.file_id_, ret))) {
        LOG_WARN("fail to execute callback and erase from flush map", KR(tmp_ret), K(flush_meta), K(ret));
      }
    }
    if (need_recycle && (nullptr != flush_entry)) {
      if (OB_TMP_FAIL(recycle_flush_entry(*flush_entry))) {
        LOG_WARN("fail to recycle flush_entry", KR(tmp_ret), KPC(flush_entry), K(free_list_.get_curr_total()));
      }
    }
    ret = OB_SUCCESS; // ignore ret, process next flush entry
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::try_get_file_length(
    const MacroBlockId &file_id,
    bool &is_file_exist,
    int64_t &file_len)
{
  int ret = OB_SUCCESS;
  is_file_exist = true;
  if (OB_UNLIKELY(!file_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file id", KR(ret), K(file_id));
  } else if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_manager is null", KR(ret), KP_(file_manager));
  } else if (OB_FAIL(file_manager_->get_local_file_length(file_id, 0 /*ls_epoch_id*/, file_len))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      // has already been gc, ignore ret
      is_file_exist = false;
      ret = OB_SUCCESS;
      LOG_INFO("seg file does not exist, may be already gc", K(file_id));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(callback_and_erase_from_flush_map(file_id, OB_SUCCESS/*ret_code*/))) {
        LOG_WARN("fail to execute callback and erase from flush map", KR(tmp_ret), K(file_id));
      }
    } else {
      LOG_WARN("fail to get file length", KR(ret), K(file_id));
    }
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::update_flush_info_of_tmp_file_meta(
    const ObSSMacroCacheFlushEntry &flush_entry)
{
  int ret = OB_SUCCESS;
  const MacroBlockId &file_id = flush_entry.flush_meta_.file_id_;
  const TmpFileSegId seg_id(file_id.second_id(), file_id.fourth_id());
  TmpFileMetaHandle meta_handle;
  bool is_meta_exist = false;
  if (OB_UNLIKELY(!flush_entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid flush entry", KR(ret), K(flush_entry));
  } else if (OB_ISNULL(file_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file_manager is null", KR(ret), KP_(file_manager));
  } else if (OB_FAIL(file_manager_->get_segment_file_mgr().try_get_seg_meta(seg_id, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to get seg meta", KR(ret), K(seg_id), K(meta_handle));
  } else if (!is_meta_exist) {
    // do nothing, seg_meta is not existed, maybe segment file has been gc
    LOG_INFO("the segment file meta does not exist", K(seg_id));
  } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta handle should not be invalid", KR(ret), K(meta_handle));
  } else {
    // update flushed_valid_length and is_flushed_seg_sealed atomically, which will be read atomically when evict
    meta_handle.set_flush_info(flush_entry.flush_meta_.valid_length_, flush_entry.flush_meta_.is_sealed_);
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::modify_from_write_cache_to_read_cache(
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->modify_from_write_cache_to_read_cache(macro_id))) {
    LOG_WARN("fail to modify from write cache to read cache", KR(ret), K(macro_id));
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::erase_from_flush_map(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheFlushMap &flush_map = get_flush_map();
  if (OB_FAIL(flush_map.erase_refactored(macro_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to erase refactored flush map", KR(ret), K(macro_id));
    }
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::callback_and_erase_from_flush_map(
    const MacroBlockId &macro_id,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // private inner function, and no need to check whether macro_id is valid.
  if (OB_TMP_FAIL(callback(macro_id, ret_code))) {
    LOG_WARN("fail to execute callback", KR(tmp_ret), K(macro_id), K(ret_code));
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  if (OB_TMP_FAIL(erase_from_flush_map(macro_id))) { // expect won't fail
    LOG_ERROR("fail to erase from flush map", KR(tmp_ret), K(macro_id));
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  return ret;
}

int ObSSMacroCacheFlushTaskBase::recycle_flush_entry(ObSSMacroCacheFlushEntry &flush_entry)
{
  int ret = OB_SUCCESS;
  flush_entry.reset();
  if (OB_FAIL(free_list_.push(&flush_entry))) {
    LOG_ERROR("fail to push flush_entry into free_list", KR(ret), K(flush_entry), K(free_list_.get_curr_total()));
  }
  return ret;
}

void ObSSMacroCacheFlushTaskBase::clear_for_next_round()
{
  files_to_flush_.reuse();
  flushed_files_.reuse();
}

void ObSSMacroCacheFlushTaskBase::set_flush_parallel_cnt()
{
  // default parallel is 5, max parallel is 30, each 1GB of tenant memory increase one parallel
  flush_parallel_cnt_ = MIN(DEFAULT_FLUSH_PARALLELISM + MTL_MEM_SIZE() / GB, MAX_FLUSH_PARALLELISM);
}

void ObSSMacroCacheFlushTaskBase::adjust_flush_task_interval()
{
  // exist unprocessed seg_file or unfinished async read io on local disk.
  if ((OB_NOT_NULL(file_manager_) && (file_manager_->flush_queue_.size() > 0) && (free_list_.get_curr_total() > 0)) ||
      (async_read_list_.get_curr_total() > 0)) {
    interval_us_ = FAST_SCHEDULE_INTERVAL_US;
  } else if (async_write_list_.get_curr_total() > 0) {  // exist unfinished async write io on object storage.
    interval_us_ = MODERATE_SCHEDULE_INTERVAL_US;
  } else {
    interval_us_ = SLOW_SCHEDULE_INTERVAL_US;
  }
}


/*----------------------------------- ObSSMacroCacheFlushTask -----------------------------------*/
ObSSMacroCacheFlushMap &ObSSMacroCacheFlushTask::get_flush_map()
{
  return macro_cache_mgr_.get_flush_map();
}


/*----------------------------------- ObSSWriteCacheFlushTask -----------------------------------*/
ObSSMacroCacheFlushMap &ObSSWriteCacheFlushTask::get_flush_map()
{
  return macro_cache_mgr_.get_write_cache_flush_map();
}

int ObSSWriteCacheFlushTask::callback(const MacroBlockId &macro_id, const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else {
    ObSSMacroCacheFlushMap &flush_map = get_flush_map();
    if (OB_FAIL(flush_map.set_promise(macro_id, ret_code))) {
      LOG_WARN("fail to set promise", KR(ret), K(macro_id), K(ret_code));
    }
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase
