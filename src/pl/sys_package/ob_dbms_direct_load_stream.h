/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_DIRECT_LOAD_STREAM_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_DIRECT_LOAD_STREAM_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{

class ObDBMSDirectLoadStream
{
public:
  ObDBMSDirectLoadStream() {}
  virtual ~ObDBMSDirectLoadStream() {}

  static int create(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
  static int close(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
};

} // namespace pl
} // namespace oceanbase

#endif /* OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_DIRECT_LOAD_STREAM_H_ */
