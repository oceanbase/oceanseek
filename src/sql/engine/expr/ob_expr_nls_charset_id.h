/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_EXPR_NLS_CHARSET_ID_H
#define OB_EXPR_NLS_CHARSET_ID_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprNLSCharsetId : public ObFuncExprOperator
{
public:
  explicit ObExprNLSCharsetId(common::ObIAllocator &alloc);
  virtual ~ObExprNLSCharsetId() {}
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_nls_charset_id(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 ObDatum &expr_datum);
  static int eval_nls_charset_id_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound);

private:
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprTypeCtx &type_ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprNLSCharsetId);
};

}
}
#endif // OB_EXPR_NLS_CHARSET_ID_H