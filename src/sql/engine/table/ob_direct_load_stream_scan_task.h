/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/container/ob_iarray.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{
struct ObDASScanCtDef;
struct ObIExtTblScanTask;

int validate_direct_load_stream_target_schema(const uint64_t tenant_id,
                                              const int64_t stream_id,
                                              const int64_t target_schema_version);

int prepare_direct_load_stream_scan_task(const uint64_t tenant_id,
                                         const ObDASScanCtDef &das_ctdef,
                                         common::ObIAllocator &allocator,
                                         common::ObIArray<ObIExtTblScanTask *> &scan_tasks);

}  // namespace sql
}  // namespace oceanbase
