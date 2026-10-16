/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OB_SQL_SEQUENCE_EXECUTOR_H__
#define __OB_SQL_SEQUENCE_EXECUTOR_H__
#include "share/ob_define.h"
#include "sql/engine/cmd/ob_simple_executor_macro.h"
namespace oceanbase
{
namespace sql
{

class ObExecContext;
class ObCreateSequenceStmt;
class ObDropSequenceStmt;
class ObAlterSequenceStmt;

DEF_SIMPLE_EXECUTOR(ObCreateSequence);
DEF_SIMPLE_EXECUTOR(ObDropSequence);
DEF_SIMPLE_EXECUTOR(ObAlterSequence);

}
}
#endif /* __OB_SQL_SEQUENCE_EXECUTOR_H__ */
//// end of header file
