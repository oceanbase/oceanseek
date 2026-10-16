/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_EXPR_OB_EXPR_LOB_APPENDWRITE_H_
#define OCEANBASE_EXPR_OB_EXPR_LOB_APPENDWRITE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

// LOB_WRITEAPPEND 构造供 UPDATE 存储路径消费的 APPEND_DELTA_LOB。
// 表达式本身不会对存储产生副作用；目标 LOB 由常规 DML 执行路径
// 完成更新。
class ObExprLobAppendWrite : public ObFuncExprOperator
{
public:
  explicit ObExprLobAppendWrite(common::ObIAllocator &alloc);
  virtual ~ObExprLobAppendWrite() {}

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &lob,
                                ObExprResType &amount,
                                ObExprResType &buffer,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_lob_appendwrite(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLobAppendWrite);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_EXPR_OB_EXPR_LOB_APPENDWRITE_H_
