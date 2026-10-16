/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_OVERLAPS_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_OVERLAPS_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_wrapper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObExprJsonOverlaps : public ObFuncExprOperator
{
public:
  explicit ObExprJsonOverlaps(common::ObIAllocator &alloc);
  virtual ~ObExprJsonOverlaps();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx) const override;
                                
  static int eval_json_overlaps(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonOverlaps);
  static int json_overlaps(const ObJsonWrapper &wrapper_a, const ObJsonWrapper &wrapper_b, bool &result);
  static int json_overlaps_array(const ObJsonWrapper &wrapper_a, const ObJsonWrapper &wrapper_b, bool &result);
  static int json_overlaps_object(const ObJsonWrapper &wrapper_a, const ObJsonWrapper &wrapper_b, bool &result);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_OVERLAPS_H_