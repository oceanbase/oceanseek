/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_RLS_POLICY_EXECUTOR_H_
#define OCEANBASE_SQL_OB_RLS_POLICY_EXECUTOR_H_

#include "share/ob_define.h"
#include "sql/engine/cmd/ob_simple_executor_macro.h"

namespace oceanbase
{
namespace sql
{

class ObExecContext;
class ObCreateRlsPolicyStmt;
class ObDropRlsPolicyStmt;
class ObAlterRlsPolicyStmt;

DEF_SIMPLE_EXECUTOR(ObCreateRlsPolicy);
DEF_SIMPLE_EXECUTOR(ObDropRlsPolicy);
DEF_SIMPLE_EXECUTOR(ObAlterRlsPolicy);

}  // namespace sql
}  // namespace oceanbase

#endif // OCEANBASE_SQL_OB_RLS_POLICY_EXECUTOR_H_
