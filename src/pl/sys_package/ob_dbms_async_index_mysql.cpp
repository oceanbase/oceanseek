/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_async_index_mysql.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/async_index/cmd/ob_async_index_executor.h"

namespace oceanbase
{
namespace pl
{

using namespace storage;

// Convert before scanning syntax: a GBK character can contain a backtick byte.
// Never trim or replace a character, which could address a different table.
int ObDBMSAsyncIndexMySql::normalize_table_name_(
    common::ObIAllocator &allocator, const common::ObObj &param, common::ObString &name)
{
  int ret = OB_SUCCESS;
  const ObString input = param.get_varchar();
  const ObCollationType collation = param.get_collation_type();
  if (input.empty() || ObCharset::charset_type_by_coll(collation) == CHARSET_UTF8MB4) {
    name = input;
  } else {
    const int64_t capacity = static_cast<int64_t>(input.length()) * 4;
    char *buffer = static_cast<char *>(allocator.alloc(capacity));
    uint32_t length = 0;
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate table name conversion buffer", KR(ret), K(capacity));
    } else if (OB_FAIL(ObCharset::charset_convert(collation, input.ptr(), input.length(),
        CS_TYPE_UTF8MB4_BIN, buffer, capacity, length, false /* trim_incomplete_tail */))) {
      LOG_WARN("fail to convert table name", KR(ret), K(collation));
    } else {
      name.assign_ptr(buffer, length);
    }
  }
  return ret;
}

/*
PROCEDURE DBMS_ASYNC_INDEX_MYSQL_REFRESH.REFRESH(
  IN    TABLE_NAME    VARCHAR(65535)
);
*/
int ObDBMSAsyncIndexMySql::refresh(ObPLExecCtx &ctx,
                                   sql::ParamStore &params,
                                   common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count", KR(ret), K(params.count()));
  } else if (!params.at(0).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name must be varchar", KR(ret));
  } else {
    ObAsyncIndexRefreshCmdArg arg;
    ObAsyncIndexExecutor executor;
    if (OB_ISNULL(ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execution context is null for async index refresh", KR(ret));
    } else if (OB_FAIL(normalize_table_name_(ctx.exec_ctx_->get_allocator(), params.at(0), arg.table_name_))) {
      LOG_WARN("fail to normalize table name", KR(ret));
    } else if (OB_FAIL(executor.execute_refresh(ctx, arg))) {
      LOG_WARN("fail to execute refresh", KR(ret), K(arg));
    }
  }
  return ret;
}

/*
PROCEDURE DBMS_ASYNC_INDEX_MYSQL_FORCEMERGE.FORCEMERGE(
  IN    TABLE_NAME    VARCHAR(65535)
);
*/
int ObDBMSAsyncIndexMySql::forcemerge(ObPLExecCtx &ctx,
                                      sql::ParamStore &params,
                                      common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != params.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count", KR(ret), K(params.count()));
  } else if (!params.at(0).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name must be varchar", KR(ret));
  } else {
    ObAsyncIndexForceMergeCmdArg arg;
    ObAsyncIndexExecutor executor;
    if (OB_ISNULL(ctx.exec_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execution context is null for async index forcemerge", KR(ret));
    } else if (OB_FAIL(normalize_table_name_(ctx.exec_ctx_->get_allocator(), params.at(0), arg.table_name_))) {
      LOG_WARN("fail to normalize table name", KR(ret));
    } else if (OB_FAIL(executor.execute_forcemerge(ctx, arg))) {
      LOG_WARN("fail to execute force_merge", KR(ret), K(arg));
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
