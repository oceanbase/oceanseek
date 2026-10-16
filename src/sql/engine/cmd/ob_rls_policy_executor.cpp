/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_rls_policy_executor.h"
#include "sql/resolver/ddl/ob_rls_policy_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

DEF_SIMPLE_EXECUTOR_IMPL(ObCreateRlsPolicy, ObRlsPolicyDDLArg, handle_rls_policy_ddl);
DEF_SIMPLE_EXECUTOR_IMPL(ObDropRlsPolicy, ObRlsPolicyDDLArg, handle_rls_policy_ddl);
DEF_SIMPLE_EXECUTOR_IMPL(ObAlterRlsPolicy, ObRlsPolicyDDLArg, handle_rls_policy_ddl);

}  // namespace sql
}  // namespace oceanbase
