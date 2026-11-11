/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_SYNC_CATALOG_EXECUTOR_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_SYNC_CATALOG_EXECUTOR_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace sql
{

class ObExecContext;
class ObSyncCatalogStmt;

class ObSyncCatalogExecutor
{
public:
  ObSyncCatalogExecutor() {}
  virtual ~ObSyncCatalogExecutor() {}
  int execute(ObExecContext &ctx, ObSyncCatalogStmt &stmt);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSyncCatalogExecutor);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_SYNC_CATALOG_EXECUTOR_H_
