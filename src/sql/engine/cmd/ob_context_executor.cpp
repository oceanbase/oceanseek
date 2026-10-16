/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_context_executor.h"
#include "sql/resolver/ddl/ob_context_stmt.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

DEF_SIMPLE_EXECUTOR_IMPL(ObCreateContext, ObContextDDLArg, do_context_ddl);
DEF_SIMPLE_EXECUTOR_IMPL(ObDropContext, ObContextDDLArg, do_context_ddl);
}  // namespace sql
}  // namespace oceanbase
