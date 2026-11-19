/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SQL_OB_EXPR_DSL_PARAM_VALUE_H_
#define OCEANBASE_SQL_OB_EXPR_DSL_PARAM_VALUE_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace common
{
class ObIJsonBase;
}
namespace sql
{
// Internal expression for DSL conversions whose semantics differ from SQL CAST.
// Its input remains an ordinary SQL parameter; the conversion is part of the plan.
class ObExprDslParamValue : public ObFuncExprOperator
{
public:
  enum Mode
  {
    BOOLEAN = 1,
    ARRAY_FLOAT = 2,
    // Modes 3-5 are reserved; keep the remaining mode identifiers stable.
    ARRAY_FLOAT_NARROW = 6,
    ARRAY_FLOAT_IN_RANGE = 7,
    INTEGER_TERMS_BASE = 128, // Add the integer column/array element ObObjType.
    // Guard the choice between a typed integer, a fractional candidate and SQL fallback.
    INTEGER_CANDIDATE_BASE = 256, // Add the integer column/array element ObObjType.
  };
  explicit ObExprDslParamValue(common::ObIAllocator &alloc);
  virtual ~ObExprDslParamValue() = default;
  int calc_result_type2(ObExprResType &type, ObExprResType &input,
      ObExprResType &mode, ObExprTypeCtx &ctx) const override;
  int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw, ObExpr &expr) const override;
  // A NULL result denotes a valid fractional candidate that cannot match an integer field.
  static int convert_integer_terms(common::ObIAllocator &allocator, common::ObIJsonBase &node,
      common::ObObjType target_type, common::ObObj &result);
  enum IntegerCandidateKind { SQL_FALLBACK = 0, INTEGER_VALUE = 1, FRACTIONAL_VALUE = 2 };
  static IntegerCandidateKind classify_integer_candidate(const common::ObObj &value,
      common::ObObjType target_type);
  static int eval_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDslParamValue);
};
} // namespace sql
} // namespace oceanbase
#endif
