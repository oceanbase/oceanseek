/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_define.h"
#include "share/ob_server_struct.h"

namespace oceanbase {
namespace common {

bool is_hardcode_schema_table(const uint64_t tid)
{
  return share::OB_ALL_CORE_TABLE_TID == tid;
}

}
} // namespace oceanbase
