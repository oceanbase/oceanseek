/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_MERGE_TABLE_H_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_MERGE_TABLE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
// Value equality for MERGE TABLE's generated SQL. Keep argument representations
// intact instead of applying relational comparison casts or collation rules.
class ObExprMergeValueEqual : public ObFuncExprOperator
{
public:
  explicit ObExprMergeValueEqual(common::ObIAllocator &alloc);
  virtual ~ObExprMergeValueEqual() = default;
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &left,
                                ObExprResType &right,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_merge_value_equal(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMergeValueEqual);
};

// Conflict guard for MERGE TABLE's FAIL strategy, placed in the NOT EXISTS
// subquery of the generated INSERT ... SELECT. Returns true when the argument (a
// conflict predicate) is false or NULL, and aborts the statement when it is true,
// so conflict detection and the insert run inside a single INSERT statement.
class ObExprMergeConflictCheck : public ObFuncExprOperator
{
public:
  explicit ObExprMergeConflictCheck(common::ObIAllocator &alloc);
  virtual ~ObExprMergeConflictCheck() = default;
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_merge_conflict_check(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMergeConflictCheck);
};

// Key guard for MERGE TABLE's THEIRS strategy, placed in the ON DUPLICATE KEY
// UPDATE assignments. ON DUPLICATE KEY UPDATE fires on any unique key while
// MERGE TABLE matches rows by primary key only, so the first argument tells
// whether the conflicting row has a different primary key. Returns the second
// argument when the first one is false or NULL, and aborts the statement when
// it is true instead of overwriting a row with a different primary key.
class ObExprMergeUniqueCheck : public ObFuncExprOperator
{
public:
  explicit ObExprMergeUniqueCheck(common::ObIAllocator &alloc);
  virtual ~ObExprMergeUniqueCheck() = default;
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_merge_unique_check(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprMergeUniqueCheck);
};
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_MERGE_TABLE_H_
