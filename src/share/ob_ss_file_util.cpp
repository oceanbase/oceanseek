/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "share/ob_ss_file_util.h"
#include "share/ob_io_device_helper.h"

namespace oceanbase
{
namespace share
{

int ObSSFileUtil::open(const char *file_path, int open_flag, mode_t open_mode, int &fd)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file path is null", KR(ret));
  } else {
    int tmp_fd = OB_INVALID_FD;
    RETRY_ON_EINTR(tmp_fd, ::open(file_path, open_flag, open_mode));
    if (OB_UNLIKELY(tmp_fd < 0)) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_WARN("fail to open", KR(ret), K(file_path), K(errno), KERRMSG);
    } else {
      fd = tmp_fd;
    }
  }
  return ret;
}

int ObSSFileUtil::close(const int fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fd < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fd", KR(ret), K(fd));
  } else {
    if (OB_UNLIKELY(0 != ::close(fd))) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_ERROR("fail to close fd, may lead to fd leak", KR(ret), K(fd), K(errno), KERRMSG);
    }
  }
  return ret;
}

int ObSSFileUtil::fsync_file(const char *file_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("file path is null", KR(ret));
  } else {
    // step 1: open file
    int fd = INVALID_FD;
    RETRY_ON_EINTR(fd, ::open(file_path, O_RDONLY));

    // step 2: fsync
    if (OB_UNLIKELY(fd < 0)) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_WARN("::open failed", KR(ret), K(file_path), K(errno), KERRMSG);
    } else if (OB_UNLIKELY(0 != ::fsync(fd))) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_WARN("::fsync failed", KR(ret), K(file_path), K(errno), KERRMSG);
    }

    // step 3: close file
    if (OB_LIKELY(fd >= 0)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(close(fd))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("fail to close fd", KR(ret), KR(tmp_ret), K(fd));
      }
    }
  }
  return ret;
}

} /* namespace share */
} /* namespace oceanbase */
