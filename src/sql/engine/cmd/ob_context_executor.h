/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OB_SQL_CONTEXT_EXECUTOR_H__
#define __OB_SQL_CONTEXT_EXECUTOR_H__
#include "share/ob_define.h"
#include "sql/engine/cmd/ob_simple_executor_macro.h"
namespace oceanbase
{
namespace sql
{

class ObExecContext;
class ObCreateContextStmt;
class ObDropContextStmt;

DEF_SIMPLE_EXECUTOR(ObCreateContext);
DEF_SIMPLE_EXECUTOR(ObDropContext);

}
}
#endif /* __OB_SQL_CONTEXT_EXECUTOR_H__ */
//// end of header file
