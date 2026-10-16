/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_sequence_executor.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

DEF_SIMPLE_EXECUTOR_IMPL(ObCreateSequence, ObSequenceDDLArg, do_sequence_ddl);
DEF_SIMPLE_EXECUTOR_IMPL(ObAlterSequence,  ObSequenceDDLArg, do_sequence_ddl);
DEF_SIMPLE_EXECUTOR_IMPL(ObDropSequence,   ObSequenceDDLArg, do_sequence_ddl);
}  // namespace sql
}  // namespace oceanbase
