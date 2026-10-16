/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_TEST_H_
#define OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_TEST_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace pl
{
class ObDBMSTest
{
public:
  ObDBMSTest() {}
  virtual ~ObDBMSTest() {}

  static int register_mds(sql::ObExecContext &ctx, sql::ParamStore &params, common::ObObj &result);
};
} // end of pl
} // end of oceanbase

#endif // OCEANBASE_SRC_PL_SYS_PACKAGE_OB_DBMS_TEST_H_
