/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_LOB_AUX_TO_STRING_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_LOB_AUX_TO_STRING_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprLobIdToString : public ObFuncExprOperator
{
public:
  explicit ObExprLobIdToString(common::ObIAllocator &alloc);
  virtual ~ObExprLobIdToString() = default;
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &input_type,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_lob_id_to_string(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLobIdToString);
};

class ObExprLobSeqIdToString : public ObFuncExprOperator
{
public:
  explicit ObExprLobSeqIdToString(common::ObIAllocator &alloc);
  virtual ~ObExprLobSeqIdToString() = default;
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &input_type,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_lob_seq_id_to_string(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLobSeqIdToString);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_LOB_AUX_TO_STRING_H_
