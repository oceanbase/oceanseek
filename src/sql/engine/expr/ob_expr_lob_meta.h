/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_EXPR_OB_EXPR_LOB_META_H_
#define OCEANBASE_EXPR_OB_EXPR_LOB_META_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

// Return LOB metadata as JSON without reading external objects or modifying the input locator.
class ObExprLobMeta : public ObFuncExprOperator
{
public:
  explicit ObExprLobMeta(common::ObIAllocator &alloc);
  virtual ~ObExprLobMeta() {}

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &arg,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types, int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_lob_meta(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLobMeta);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_EXPR_OB_EXPR_LOB_META_H_
