/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/ob_define.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
}
namespace common
{
class ObIAllocator;
}
namespace sql
{
class ObExecContext;
class ObPhysicalPlan;
class ObOptimizerContext;
class ObDMLStmt;

class ObTableDirectInsertService
{
public:
  // all insert-tasks within an insert into select clause are wrapped by a single direct insert instance
  static int check_direct_load_stream_source_plan(ObPhysicalPlan &plan,
                                                  common::ObIAllocator &allocator,
                                                  int64_t &stream_id);
  static int start_direct_insert(ObExecContext &ctx, ObPhysicalPlan &plan,
                                 const int64_t stream_id);
  static int commit_direct_insert(ObExecContext &ctx, ObPhysicalPlan &plan);
  static int finish_direct_insert(ObExecContext &ctx, ObPhysicalPlan &plan, const bool commit,
                                  int64_t &claimed_direct_load_stream_id);
  // each insert-task is processed in a single thread and is wrapped by a table load trans
  static int open_task(const uint64_t table_id,
                       const int64_t px_task_id,
                       const int64_t ddl_task_id,
                       observer::ObTableLoadTableCtx *&table_ctx);
  static int close_task(const uint64_t table_id,
                        const int64_t px_task_id,
                        const int64_t ddl_task_id,
                        observer::ObTableLoadTableCtx *table_ctx,
                        const int error_code = OB_SUCCESS);
};
} // namespace sql
} // namespace oceanbase
