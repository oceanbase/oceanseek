/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_DSL_MULTI_VALUE_RANGE_H_
#define OCEANBASE_SQL_OB_EXPR_DSL_MULTI_VALUE_RANGE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

// Internal DSL expression; not callable from user SQL.
// DSL_MULTI_VALUE_RANGE(target, lower, upper, include_lower, include_upper).
// SQL NULL bounds are unbounded. Each inclusion flag must be 0 or 1.
// Bounds use the standard SQL-to-JSON scalar conversion, including binary and
// temporal values. Only values of the same encoding type are compared. Arrays
// in target are traversed recursively; objects and their members are skipped.
class ObExprDslMultiValueRange : public ObFuncExprOperator
{
public:
  explicit ObExprDslMultiValueRange(common::ObIAllocator &alloc);
  virtual ~ObExprDslMultiValueRange() = default;
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_dsl_multi_value_range(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDslMultiValueRange);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_EXPR_DSL_MULTI_VALUE_RANGE_H_
