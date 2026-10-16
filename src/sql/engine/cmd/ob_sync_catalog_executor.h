/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
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
