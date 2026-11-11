/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

enum class ObVecIVFExprRole : int8_t
{
  TYPE,
  CID,
  DATA
};

class ObExprVecIVFBase : public ObFuncExprOperator
{
public:
  ObExprVecIVFBase(common::ObIAllocator &allocator,
                   const ObItemType item_type,
                   const char *name,
                   const ObVecIVFExprRole role);
  virtual ~ObExprVecIVFBase() = default;
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs_array,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_NULL_ON_WARN; }
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_vec_ivf_type(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      ObDatum &res);
  static int eval_vec_ivf_cid(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      ObDatum &res);
  static int eval_vec_ivf_data(
      const ObExpr &expr,
      ObEvalCtx &ctx,
      ObDatum &res);

private:
  ObVecIVFExprRole role_;
  DISALLOW_COPY_AND_ASSIGN(ObExprVecIVFBase);
};

class ObExprVecIVFType final : public ObExprVecIVFBase
{
public:
  explicit ObExprVecIVFType(common::ObIAllocator &allocator);
};

class ObExprVecIVFCid final : public ObExprVecIVFBase
{
public:
  explicit ObExprVecIVFCid(common::ObIAllocator &allocator);
};

class ObExprVecIVFData final : public ObExprVecIVFBase
{
public:
  explicit ObExprVecIVFData(common::ObIAllocator &allocator);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_H_
