/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_LENGTH_
#define OCEANBASE_SQL_ENGINE_EXPR_LENGTH_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprLength : public ObFuncExprOperator
{
public:
  explicit  ObExprLength(common::ObIAllocator &alloc);
  virtual ~ObExprLength();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_oracle_mode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_mysql_mode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_mysql_length_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);
  static int calc_oracle_length_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);

private:
  // Query the byte length of an outrow (or mem-loc) lob from the lob storage.
  // The byte length of an outrow lob cannot be read from the locator header
  // alone, so a query param is built from the result-row locator and the lob
  // manager scans the lob meta table to compute it.
  static int get_outrow_lob_byte_length(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const common::ObString &lob_str,
                                        int64_t &byte_length);
  template <typename ArgVec, typename ResVec>
  static int calc_mysql_length_vector_dispatch(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);
  template <typename ArgVec, typename ResVec>
  static int calc_oracle_length_vector_dispatch(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);
  DISALLOW_COPY_AND_ASSIGN(ObExprLength);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_LENGTH_ */
