/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/task/ob_server_slog_flush_task.h"
#include "lib/thread/thread_mgr.h"
#include "lib/allocator/page_arena.h"
#include "share/io/ob_io_manager.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;

ObServerSlogFlushTask::ObServerSlogFlushTask(volatile bool &is_stopped)
  : is_inited_(false), is_stopped_(is_stopped)
{
}

int ObServerSlogFlushTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerSlogFlushTask has already been inited", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tg_id", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL_US, true))) {
    LOG_WARN("fail to schedule ObServerSlogFlushTask", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("succ to schedule server slog flush task", K(tg_id));
  }
  return ret;
}

void ObServerSlogFlushTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);
  ObSSMacroCacheFlushInfo flush_info;
  bool is_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerSlogFlushTask has not been inited", KR(ret));
  } else {
    // flush all slog files in flush_map
    do {
      if (OB_FAIL(OB_SERVER_FILE_MGR.get_next_flush_entry(flush_info, is_exist))) {
        LOG_WARN("fail to get next flush entry", KR(ret));
      } else if (is_exist && OB_FAIL(flush_slog(flush_info))) {
        LOG_WARN("fail to flush slog", KR(ret), K(flush_info));
      }
    } while (OB_SUCC(ret) && !is_stopped_ && is_exist);
  }
}

int ObServerSlogFlushTask::flush_slog(const ObSSMacroCacheFlushInfo &flush_info)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  int64_t file_len = 0;
  char *buf = nullptr;
  DefaultPageAllocator allocator("SvrSlogFlush", OB_SERVER_TENANT_ID);
  const MacroBlockId &macro_id = flush_info.get_macro_id();
  if (OB_UNLIKELY(!flush_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(flush_info));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.get_local_file_length(macro_id, 0/*ls_epoch_id*/, file_len))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS; // ignore ret
      LOG_INFO("file does not exist, may be already gc", K(flush_info));
    } else {
      LOG_WARN("fail to get file length", KR(ret), K(flush_info));
    }
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", KR(ret), K(file_len));
  } else {
    ObStorageObjectHandle read_handle;
    ObStorageObjectHandle write_handle;
    if (OB_FAIL(read_file(flush_info, file_len, buf, read_handle))) {
      LOG_WARN("fail to read file", KR(ret), K(flush_info), K(file_len), KP(buf), K(read_handle));
    } else if (OB_FAIL(write_file(flush_info, read_handle, write_handle))) {
      LOG_WARN("fail to write file", KR(ret), K(flush_info), K(read_handle), K(write_handle));
    } else if (OB_FAIL(ObSSObjectAccessUtil::delete_server_tenant_local_file(macro_id, 0/*ls_epoch_id*/))) {
      LOG_WARN("fail to delete server tenant local file", KR(ret), K(macro_id));
    } else if (OB_FAIL(OB_SERVER_FILE_MGR.update_flushed_slog_seq(macro_id.fifth_id()))) {
      LOG_WARN("fail to update flushed slog seq", KR(ret), "slog_seq", macro_id.fifth_id());
    }
  }

  // free memory of buf
  if (OB_NOT_NULL(buf)) {
    allocator.free(buf);
    buf = nullptr;
  }

  // erase flush entry from flush map
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(erase_from_flush_map(flush_info.get_macro_id()))) { // expect won't fail
    LOG_ERROR("fail to erase from flush map", KR(tmp_ret), K(flush_info));
  }

  const int64_t cost_us =  ObTimeUtility::current_time_us() - start_us;
  LOG_INFO("finish to flush slog", KR(ret), KR(tmp_ret), K(flush_info), K(cost_us));
  return ret;
}

int ObServerSlogFlushTask::read_file(
    const ObSSMacroCacheFlushInfo &flush_info,
    const int64_t file_len,
    char *buf,
    ObStorageObjectHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!flush_info.is_valid() || (file_len <= 0) || OB_ISNULL(buf) || read_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(flush_info), K(file_len), KP(buf), K(read_handle));
  } else {
    // step 1: read from local cache
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = flush_info.get_macro_id();
    read_info.offset_ = 0;
    read_info.size_ = file_len;
    read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.buf_ = buf;
    read_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
    ObSSLocalCacheReader local_cache_reader;
    if (OB_FAIL(local_cache_reader.aio_read(read_info, read_handle))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS; // ignore ret
        LOG_INFO("file does not exist, may be already gc", K(flush_info));
      } else {
        LOG_WARN("fail to aio read", KR(ret), K(read_info), K(flush_info));
      }
      LOG_WARN("fail to aio read", KR(ret), K(read_info));
    } else if (OB_FAIL(read_handle.wait())) {
      LOG_WARN("fail to wait read_handle", KR(ret), K(read_handle));
    }
  }
  return ret;
}

int ObServerSlogFlushTask::write_file(
    const ObSSMacroCacheFlushInfo &flush_info,
    ObStorageObjectHandle &read_handle,
    ObStorageObjectHandle &write_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!flush_info.is_valid() || !read_handle.is_valid() || write_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(flush_info), K(read_handle));
  } else {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = read_handle.get_buffer();
    write_info.offset_ = 0;
    write_info.size_ = read_handle.get_data_size();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
    write_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(write_info.mtl_tenant_id_);
    ObSSObjectStorageWriter object_storage_writer;
    if (OB_FAIL(write_handle.set_macro_block_id(flush_info.get_macro_id()))) {
      LOG_WARN("fail to set macro block id", KR(ret));
    } else if (OB_FAIL(object_storage_writer.aio_write(write_info, write_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(write_handle));
    } else if (OB_FAIL(write_handle.wait())) {
      LOG_WARN("fail to wait write handle", KR(ret), K(flush_info), K(write_handle));
    }
  }
  return ret;
}

int ObServerSlogFlushTask::erase_from_flush_map(const MacroBlockId &file_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_SERVER_FILE_MGR.erase_from_flush_map(file_id))) {
    LOG_WARN("fail to erase refactored flush map", KR(ret), K(file_id));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
