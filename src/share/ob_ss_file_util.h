/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_SHARE_OB_FILE_UTIL_H_
#define OCEANBASE_SHARED_STORAGE_SHARE_OB_FILE_UTIL_H_

#include <sys/stat.h>

namespace oceanbase
{
namespace share
{

// Retry on EINTR for functions like open() that return -1 on error.
#define RETRY_ON_EINTR(err, expr) do { \
  static_assert(std::is_signed<decltype(err)>::value, \
                #err " must be a signed integer"); \
  (err) = (expr); \
  if (((err) == -1) && (errno == EINTR)) { \
    OB_LOG(WARN, "retry on EINTR"); \
  } \
} while (((err) == -1) && (errno == EINTR))

class ObSSFileUtil
{
public:
  static int open(const char *file_path, int open_flag, mode_t open_mode, int &fd);
  static int close(const int fd);
  static int fsync_file(const char *file_path);
};

} /* namespace share */
} /* namespace oceanbase */

#endif /* OCEANBASE_SHARED_STORAGE_SHARE_OB_FILE_UTIL_H_ */
