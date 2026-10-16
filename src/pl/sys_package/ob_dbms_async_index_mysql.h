/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#pragma once

#include "pl/ob_pl.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSAsyncIndexMySql
{
public:
  ObDBMSAsyncIndexMySql() {}
  virtual ~ObDBMSAsyncIndexMySql() {}

#define DECLARE_FUNC(func) \
  static int func(ObPLExecCtx &ctx, sql::ParamStore &params, common::ObObj &result);

  DECLARE_FUNC(refresh)
  DECLARE_FUNC(forcemerge)

#undef DECLARE_FUNC

private:
  static int normalize_table_name_(common::ObIAllocator &allocator,
                                   const common::ObObj &param,
                                   common::ObString &name);
};

} // namespace pl
} // namespace oceanbase
