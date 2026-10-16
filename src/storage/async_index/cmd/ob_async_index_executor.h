/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#pragma once

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace pl { class ObPLExecCtx; }
namespace sql
{
class ObExecContext;
class ObSQLSessionInfo;
}

namespace storage
{

struct ObAsyncIndexForceMergeCmdArg
{
  ObString table_name_;
  bool is_valid() const { return !table_name_.empty(); }
  TO_STRING_KV(K_(table_name));
};

struct ObAsyncIndexRefreshCmdArg
{
  ObString table_name_;
  bool is_valid() const { return !table_name_.empty(); }
  TO_STRING_KV(K_(table_name));
};

class ObAsyncIndexExecutor
{
public:
  ObAsyncIndexExecutor() :
    tenant_id_(OB_INVALID_TENANT_ID),
    db_name_(),
    table_name_(),
    ctx_(nullptr),
    session_info_(nullptr)
  {}
  ~ObAsyncIndexExecutor() = default;

  int execute_forcemerge(pl::ObPLExecCtx &pl_ctx, const ObAsyncIndexForceMergeCmdArg &arg);
  int execute_refresh(pl::ObPLExecCtx &pl_ctx, const ObAsyncIndexRefreshCmdArg &arg);

private:
  int resolve_table_name(const ObString &raw_name);

  uint64_t tenant_id_;
  ObString db_name_;
  ObString table_name_;
  sql::ObExecContext *ctx_;
  sql::ObSQLSessionInfo *session_info_;
};

} // namespace storage
} // namespace oceanbase
