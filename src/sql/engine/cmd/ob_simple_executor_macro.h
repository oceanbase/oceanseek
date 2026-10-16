/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_SIMPLE_EXECUTOR_MACRO_H_
#define OCEANBASE_SQL_OB_SIMPLE_EXECUTOR_MACRO_H_

#define DEF_SIMPLE_EXECUTOR(name)                          \
  class name##Executor                                     \
  {                                                        \
  public:                                                  \
    name##Executor() {}                                    \
    virtual ~name##Executor() {}                           \
    int execute(ObExecContext &ctx, name##Stmt &stmt);     \
  private:                                                 \
    DISALLOW_COPY_AND_ASSIGN(name##Executor);              \
  }

#define DEF_SIMPLE_EXECUTOR_IMPL(name, argtype, func) \
int name##Executor::execute(ObExecContext &ctx, name##Stmt &stmt) \
{ \
  int ret = OB_SUCCESS; \
  ObTaskExecutorCtx *task_exec_ctx = NULL; \
  obrpc::ObCommonRpcProxy *common_rpc_proxy = NULL; \
  const obrpc::argtype &ddl_arg = stmt.get_arg(); \
  ObString first_stmt; \
  if (OB_FAIL(stmt.get_first_stmt(first_stmt))) { \
    LOG_WARN("fail to get first stmt" , K(ret)); \
  } else { \
    const_cast<obrpc::argtype&>(ddl_arg).ddl_stmt_str_ = first_stmt; \
  } \
  if (OB_FAIL(ret)) { \
  } else if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(ctx))) { \
    ret = OB_NOT_INIT; \
    LOG_WARN("get task executor context failed"); \
  } else if (OB_ISNULL(common_rpc_proxy = task_exec_ctx->get_common_rpc())) { \
    ret = OB_NOT_INIT; \
    LOG_WARN("get common rpc proxy failed"); \
  } else if (OB_FAIL(common_rpc_proxy->func(ddl_arg))) { \
    LOG_WARN("rpc proxy failed", K(ddl_arg), K(ret)); \
  } \
  return ret; \
}

#endif // OCEANBASE_SQL_OB_SIMPLE_EXECUTOR_MACRO_H_
