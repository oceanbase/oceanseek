/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "palf_ffi.h"
#include "libpalf_logger.h"
#include "libpalf_common_define.h"
#include "share/ob_define.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace libpalf
{

int LibPalfLogger::cal_libpalf_shared_syslog_capacity(
  const int64_t max_syslog_file_count,
  const int64_t syslog_disk_size,
  const int64_t libpalf_shared_percentage,
  const int64_t default_max_syslog_file_size,
  int64_t &libpalf_syslog_max_file_count,
  int64_t &observer_syslog_max_file_count,
  int64_t &observer_syslog_disk_size)
{
  int ret = OB_SUCCESS;
  if (0 > max_syslog_file_count || 0 > syslog_disk_size || 0 >= libpalf_shared_percentage
    || 100 <= libpalf_shared_percentage || 0 >= default_max_syslog_file_size
    || 0 != libpalf_syslog_max_file_count || 0 != observer_syslog_max_file_count
    || 0 != observer_syslog_disk_size) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(libpalf_shared_percentage), K(default_max_syslog_file_size),
      K(libpalf_syslog_max_file_count), K(observer_syslog_max_file_count), K(observer_syslog_disk_size));
  } else {
    libpalf_syslog_max_file_count = 0;
    observer_syslog_max_file_count = 0;
    observer_syslog_disk_size = 0;
    if (0 == max_syslog_file_count && 0 == syslog_disk_size) {
      // do nothing
    } else if (0 == max_syslog_file_count) {
      libpalf_syslog_max_file_count = (syslog_disk_size / default_max_syslog_file_size) * libpalf_shared_percentage / 100;
    } else if (0 == syslog_disk_size) {
      libpalf_syslog_max_file_count = max_syslog_file_count * libpalf_shared_percentage / 100;
    } else {
      libpalf_syslog_max_file_count = min(syslog_disk_size / default_max_syslog_file_size, max_syslog_file_count)
        * libpalf_shared_percentage / 100;
    }

    if (0 < max_syslog_file_count) {
      observer_syslog_max_file_count = max_syslog_file_count - libpalf_syslog_max_file_count;
    }
    if (0 < syslog_disk_size) {
      observer_syslog_disk_size = syslog_disk_size - libpalf_syslog_max_file_count * default_max_syslog_file_size;
    }
  }
  return ret;
}

int LibPalfLogger::get_max_syslog_file_count(int64_t &max_syslog_file_count)
{
  int ret = OB_SUCCESS;
  if (0 != max_syslog_file_count) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "output param max_syslog_file_count should be 0", K(ret), K(max_syslog_file_count));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_max_syslog_file_count(reinterpret_cast<uint64_t*>(&max_syslog_file_count))))) {
    PALF_LOG(WARN, "fail to get max syslog file count", K(ret));
  }
  return ret;
}

int LibPalfLogger::set_max_syslog_file_count(const int64_t max_syslog_file_count)
{
  int ret = OB_SUCCESS;
  if (0 > max_syslog_file_count) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(max_syslog_file_count));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_set_max_syslog_file_count(max_syslog_file_count)))) {
    PALF_LOG(WARN, "fail to set max syslog file count", K(ret), K(max_syslog_file_count));
  }
  return ret;
}

} // end namespace libpalf
} // end namespace oceanbase