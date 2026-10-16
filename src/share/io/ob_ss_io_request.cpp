/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "share/io/ob_ss_io_request.h"
#include "src/storage/shared_storage/ob_file_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_io_device_helper.h"

namespace oceanbase
{
namespace common
{
using namespace oceanbase::share;

ObSSIORequest::ObSSIORequest() : fd_cache_handle_()
{
}

ObSSIORequest::~ObSSIORequest()
{
  destroy();
}

void ObSSIORequest::reset() //only for test, not dec resut_ref
{
  try_close_device_and_fd();
  fd_cache_handle_.reset();
  ObIORequest::reset();
}

void ObSSIORequest::destroy()
{
  // The control_block_ is allocated during ObIORequest::prepare and is managed by ObIODevice.
  // It is used to execute asynchronous requests in ObAsyncIOChannel and should be tied to the
  // lifecycle of ObIORequest. Therefore, ObIODevice must release it subsequently.
  // However, SSIORequest::destroy attempts to prematurely release the device and fd, 
  // posing a potential memory leak risk. Thus, control_block_ should be freed first, 
  // followed by the device and fd. Nonetheless, ObIORequest::destroy also attempts to 
  // release control_block_; this part is retained for compatibility with SN logic.
  int ret = OB_SUCCESS;
  if (control_block_ != nullptr) {
    if (OB_ISNULL(fd_.device_handle_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("device handle is null, memory leak", KR(ret), K(*this));
    } else {
      fd_.device_handle_->free_iocb(control_block_);
      control_block_ = nullptr;
    }
  }
  try_close_device_and_fd();
  fd_cache_handle_.reset();
  ObIORequest::destroy();
}

// TODO(renju.rj) try_close_device_and_fd的逻辑最好封装在ObIOFd中
void ObSSIORequest::try_close_device_and_fd()
{
  int ret = OB_SUCCESS;
  if ((nullptr != io_result_) && io_result_->flag_.is_need_close_dev_and_fd()) {
    ObIODevice *device_handle = nullptr;
    if (OB_ISNULL(device_handle = fd_.device_handle_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("device handle is null", KR(ret), K(*this));
    } else if (device_handle->is_object_device()) {
      ObBackupIoAdapter io_adapter;
      if (OB_FAIL(io_adapter.close_device_and_fd(device_handle, fd_))) {
        LOG_WARN("fail to close device and fd", KR(ret), K(*this));
      }
    }
  }
}

int ObSSIORequest::set_fd_cache_handle(const ObIOInfo &info)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode() && info.fd_cache_handle_.is_valid()) {
    if (OB_FAIL(fd_cache_handle_.assign(info.fd_cache_handle_))) {
      LOG_WARN("fail to assign fd cache handle", KR(ret), "fd_cache_handle", info.fd_cache_handle_);
    }
  }
  return ret;
}

} /* namespace common */
} /* namespace oceanbase */
