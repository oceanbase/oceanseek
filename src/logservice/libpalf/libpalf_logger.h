/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_LOGGER_
#define OCEANBASE_LOGSERVICE_LIBPALF_LOGGER_

#include "stdint.h"

namespace oceanbase
{
namespace libpalf
{

class LibPalfLogger
{
public:
  static constexpr int64_t LIBPALF_SHARED_MAX_SYSLOG_CAPACITY_PERCENTAGE = 20;
public:
  // pure function, calculate the max syslog file count of libpalf and observer
  // by dividing the capacity of syslog disk into two parts.
  // libpalf will get the part from libpalf_shared_percentage.
  // the default value of libpalf_shared_percentage is 20%.
  // so if the max_syslog_file_count is 5, than libpalf will get 1, observer will get 4.
  // buf if the max_syslog_file_count is 4, than libpalf will get 0, observer will get 4.
  // cause the floor operation of interger division.
  // 
  // libpalf_syslog_max_file_count is the min value of division result from libpalf_syslog_max_file_count and syslog_disk_size.
  // for example, if the max_syslog_file_count is 1024, syslog_disk_size is 50G and default_max_syslog_file_size is 256MB,
  // then libpalf_syslog_max_file_count will be min(1024 / 5, 50G / 256MB / 5) = 40.
  static int cal_libpalf_shared_syslog_capacity(
    const int64_t max_syslog_file_count,
    const int64_t syslog_disk_size,
    const int64_t libpalf_shared_percentage,
    const int64_t default_max_syslog_file_size,
    int64_t &libpalf_syslog_max_file_count,
    int64_t &observer_syslog_max_file_count,
    int64_t &observer_syslog_disk_size);

  // Total syslog file count, including the current working file; 0 means unlimited.
  // A value of 0 disables quantity recycling.
  static int get_max_syslog_file_count(int64_t &max_syslog_file_count);

  // Total syslog file count, including the current working file; 0 means unlimited.
  // A value of 0 disables quantity recycling.
  static int set_max_syslog_file_count(const int64_t max_syslog_file_count);
};

}; // end namespace libpalf
}; // end namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LIBPALF_LOGGER_