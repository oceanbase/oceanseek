/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "ob_log_external_storage_utils.h"
#include "share/backup/ob_backup_io_adapter.h"                // ObBackupIOAdapter
namespace oceanbase
{
using namespace common;
using namespace share;
namespace logservice
{
static int convert_to_storage_access_type(const OPEN_FLAG &open_flag,
                                          ObStorageAccessType &storage_access_type)
{
  int ret = OB_SUCCESS;
  if (OPEN_FLAG::READ_FLAG == open_flag) {
    storage_access_type = ObStorageAccessType::OB_STORAGE_ACCESS_ADAPTIVE_READER;     
  } else if (OPEN_FLAG::MULTI_UPLOAD_FLAG == open_flag) {
    storage_access_type = ObStorageAccessType::OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER;
  } else {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "not supported flag", KR(ret), K(open_flag));
  }
  return ret;
}

ObLogExternalStorageCtxItem::ObLogExternalStorageCtxItem(
  const ObIOFd &io_fd,
  ObIODevice *io_device)
: io_handle_(), io_fd_(io_fd), io_device_(io_device) {}

ObLogExternalStorageCtxItem::ObLogExternalStorageCtxItem() {reset();}

ObLogExternalStorageCtxItem::~ObLogExternalStorageCtxItem()
{
  reset();
}

void ObLogExternalStorageCtxItem::reset()
{
  io_handle_.reset();
  io_fd_.reset();
  io_device_ = NULL;
}

bool ObLogExternalStorageCtxItem::is_valid() const
{
  // io_handle_ will init by async_pread or multi upload.
  return io_fd_.is_valid() && NULL != io_device_;
}

ObLogExternalStorageCtx::ObLogExternalStorageCtx()
: items_(NULL), count_(0), capacity_(0), is_inited_(false) {}

ObLogExternalStorageCtx::~ObLogExternalStorageCtx() { destroy(); }

int ObLogExternalStorageCtx::init(const ObString &uri,
                                  const ObString &storage_info,
                                  const uint64_t storage_id,
                                  const int64_t concurrency,
                                  const OPEN_FLAG &flag)
{
  int ret = OB_SUCCESS;
  ObLogExternalStorageCtxItem *tmp_ptr = nullptr;
  ObIODevice *io_device = nullptr;
  ObIOFd io_fd;
  ObBackupStorageInfo parsed_storage_info;
  const ObStorageIdMod storage_id_mod(storage_id, ObStorageUsedMod::STORAGE_USED_CLOG);
  ObStorageAccessType access_type;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx has inited", KR(ret));
  } else if (uri.empty()
      || (!uri.prefix_match(OB_FILE_PREFIX) && storage_info.empty())
      || 0 >= concurrency) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(uri), K(concurrency), K(flag));
  } else if (OB_FAIL(convert_to_storage_access_type(flag, access_type))) {
    CLOG_LOG(WARN, "failed to convert access type", KR(ret), K(flag));
  } else if (OB_FAIL(parsed_storage_info.set(uri.ptr(), storage_info.ptr()))) {
    CLOG_LOG(WARN, "failed to set log storage info", KR(ret), K(uri));
  } else if (OB_ISNULL(tmp_ptr = reinterpret_cast<ObLogExternalStorageCtxItem*>(mtl_malloc(
      concurrency * sizeof(ObLogExternalStorageCtxItem), "ObLogEXTHandler")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "allocate memory failed", KR(ret));
  } else if (OB_FAIL(ObBackupIoAdapter::open_with_access_type(io_device, io_fd,
      &parsed_storage_info, uri, access_type, storage_id_mod, false/*is_batch_write*/))) {
    CLOG_LOG(WARN, "failed to open log storage file", KR(ret), K(uri));
  } else {
    // 各 Item 共用设备和 fd，分别持有异步 IO handle；设备和 fd 由 Ctx 统一关闭。
    for (int64_t i = 0; i < concurrency; i++) {
      new(tmp_ptr+i)ObLogExternalStorageCtxItem(io_fd, io_device);
    }
    count_ = 0;
    capacity_ = concurrency;
    items_ = tmp_ptr;
    io_fd_ = io_fd;
    io_device_ = io_device;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(io_device)
        && OB_TMP_FAIL(ObBackupIoAdapter::close_device_and_fd(io_device, io_fd))) {
      CLOG_LOG(WARN, "failed to close log storage file after init failure", KR(ret), KR(tmp_ret));
    }
    if (OB_NOT_NULL(tmp_ptr)) {
      mtl_free(tmp_ptr);
      tmp_ptr = nullptr;
    }
  }
  return ret;
}

int ObLogExternalStorageCtx::wait(int64_t &out_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else {
    ObLogExternalStorageCtxItem *item = NULL;
    int tmp_ret = OB_SUCCESS;
    // NB: wait until all item finished, we can not check OB_SUCC(ret) in for loop.
    //     record first errno in for loop.
    for (int64_t i = 0; i < count_; i++) {
      if (OB_TMP_FAIL(get_item(i, item))) {
        CLOG_LOG(WARN, "get_item failed", KR(ret), K(i), KPC(this));
      } else if (OB_UNLIKELY(!item->is_valid())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error, item is invalid", KR(ret), K(i), KPC(this), K(item));
      } else if (OB_TMP_FAIL(item->io_handle_.wait())) {
        CLOG_LOG(WARN, "wait io_handle failed", KR(ret), K(i), KPC(this));
      } else {
        out_size += item->io_handle_.get_data_size();
        CLOG_LOG(TRACE, "wait success", K(i), K(out_size), KPC(item));
      }
      if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
    count_ = 0;
  }
  return ret;
}

int ObLogExternalStorageCtx::get_item(const int64_t index,
                                      ObLogExternalStorageCtxItem *&item) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else if (index >= capacity_ || 0 > index) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument inited", KR(ret), K(index), KPC(this));
  } else {
    item = &items_[index];
  }
  return ret;
}

int ObLogExternalStorageCtx::get_io_fd(ObIOFd &io_fd) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else {
    io_fd = io_fd_;
  }
  return ret;
}

int ObLogExternalStorageCtx::get_io_device(ObIODevice *&io_device) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else {
    io_device = io_device_;
  }
  return ret;
}

void ObLogExternalStorageCtx::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  if (NULL != items_) {
    if (OB_FAIL(ObBackupIoAdapter::close_device_and_fd(io_device_, io_fd_))) {
      CLOG_LOG(WARN, "failed to close log storage file", KR(ret));
    }
    for (int64_t i = 0; i < capacity_; i++) {
      ObLogExternalStorageCtxItem &item = items_[i];
      item.~ObLogExternalStorageCtxItem();
    }
    mtl_free(items_);
  }
  count_ = 0;
  capacity_ = 0;
  items_ = NULL;
}

int ObLogExternalStorageCtx::inc_count()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else if (count_ >= capacity_) {
    ret = OB_SIZE_OVERFLOW;
    CLOG_LOG(ERROR, "size overflow", KR(ret), K_(count), K_(capacity));
  } else {
    count_++;
  }
  return ret;
}

int64_t ObLogExternalStorageCtx::get_count() const
{
  return count_;
}

bool ObLogExternalStorageCtx::is_valid() const
{
  return true == is_inited_ && NULL != items_ && count_ <= capacity_ && capacity_ > 0 && io_fd_.is_valid() && NULL != io_device_;
}

ObLogExternalStorageHandleAdapter::ObLogExternalStorageHandleAdapter() {}

ObLogExternalStorageHandleAdapter::~ObLogExternalStorageHandleAdapter() {}

int ObLogExternalStorageHandleAdapter::exist(const ObString &uri,
                                             const ObString &storage_info,
                                             bool &exist)
{
  int ret = OB_SUCCESS;
  ObBackupStorageInfo parsed_storage_info;
  exist = false;
  if (OB_FAIL(parsed_storage_info.set(uri.ptr(), storage_info.ptr()))) {
    CLOG_LOG(WARN, "failed to set log storage info", KR(ret), K(uri));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_is_exist(
      uri, &parsed_storage_info, exist))) {
    CLOG_LOG(WARN, "failed to check log file existence", KR(ret), K(uri));
  }
  return ret;
}

int ObLogExternalStorageHandleAdapter::get_file_size(const ObString &uri,
                                                     const ObString &storage_info,
                                                     int64_t &file_size)
{
  int ret = OB_SUCCESS;
  ObBackupStorageInfo parsed_storage_info;
  file_size = 0;
  if (OB_FAIL(parsed_storage_info.set(uri.ptr(), storage_info.ptr()))) {
    CLOG_LOG(WARN, "failed to set log storage info", KR(ret), K(uri));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_get_file_length(
      uri, &parsed_storage_info, file_size))) {
    CLOG_LOG(WARN, "failed to get log file size", KR(ret), K(uri));
  }
  return ret;
}

int ObLogExternalStorageHandleAdapter::async_pread(const int64_t offset,
                                                   char *buf,
                                                   const int64_t read_buf_size,
                                                   ObLogExternalStorageCtxItem &io_ctx)
{
  ObTimeGuard time_guard("storage pread", 100 * 1000);
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  if (0 > offset || NULL == buf || 0 >= read_buf_size || !io_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(offset), KP(buf), K(read_buf_size), K(io_ctx));
  } else if (OB_FAIL(io_adapter.async_pread(*io_ctx.io_device_, io_ctx.io_fd_, buf,
                                            offset, read_buf_size, io_ctx.io_handle_, common::ObIOModule::CLOG_READ_IO))) {
    CLOG_LOG(WARN, "pread failed", KR(ret), K(io_ctx), K(offset), K(read_buf_size),
             KP(buf), K(time_guard));
  } else if (FALSE_IT(time_guard.click("after async_pread"))) {
  } else {
    CLOG_LOG(TRACE, "pread success", K(time_guard), K(io_ctx), K(offset), K(read_buf_size), KP(buf));
  }
  return ret;
}

int ObLogExternalStorageHandleAdapter::async_pwrite(const int64_t offset,
                                                    const char *buf,
                                                    const int64_t write_buf_size,
                                                    ObLogExternalStorageCtxItem &io_ctx)
{
  ObTimeGuard time_guard("storage pread", 100 * 1000);
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  if (0 > offset || NULL == buf || 0 >= write_buf_size || !io_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(offset), KP(buf), K(write_buf_size), K(io_ctx));
  } else if (OB_FAIL(io_adapter.async_upload_data(*io_ctx.io_device_, io_ctx.io_fd_, buf,
                                                   offset, write_buf_size, io_ctx.io_handle_, common::ObIOModule::CLOG_WRITE_IO))) {
    CLOG_LOG(WARN, "async_upload_data failed", KR(ret), K(io_ctx), K(offset), K(write_buf_size),
             KP(buf), K(time_guard));
  } else if (FALSE_IT(time_guard.click("after async_upload_data"))) {
  } else {
    CLOG_LOG(TRACE, "async_upload_data success", K(time_guard), K(io_ctx), K(offset), K(write_buf_size), KP(buf));
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
