/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_UTILITY_OB_LOG_BRIEF_H_
#define OCEANBASE_LIB_UTILITY_OB_LOG_BRIEF_H_

#include <stdint.h>
#include "lib/oblog/ob_log_print_kv.h"

namespace oceanbase
{
namespace common
{

// The referenced object must outlive this view.
template <typename T>
struct ObLogBrief final
{
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (nullptr != buf && buf_len > 0) {
      if (nullptr == obj_) {
        (void)logdata_printf(buf, buf_len, pos, "NULL");
      } else {
        pos = obj_->to_string_brief(buf, buf_len);
      }
    }
    return pos;
  }
  const T *obj_;
};

template <typename T>
ObLogBrief<T> brief(const T &obj) { return {&obj}; }

template <typename T>
ObLogBrief<T> brief(T *obj) { return {obj}; }

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_UTILITY_OB_LOG_BRIEF_H_
