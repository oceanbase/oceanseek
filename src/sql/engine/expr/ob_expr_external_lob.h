/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_EXPR_OB_EXPR_EXTERNAL_LOB_H_
#define OCEANBASE_EXPR_OB_EXPR_EXTERNAL_LOB_H_

#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

// Build a temporary memory locator for EXTERNAL_LOB(url, offset, length, kind [, etag]).
// Return EXTBLOB; later paths handle persistence and remote reads.
class ObExprExternalLob : public ObFuncExprOperator
{
public:
  explicit ObExprExternalLob(common::ObIAllocator &alloc);
  virtual ~ObExprExternalLob() {}

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_external_lob(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               ObDatum &res_datum);

private:
  static int get_uint64_argument_(const common::ObDatum &datum,
                                  uint64_t &value);
  static int parse_kind_(const common::ObString &kind_str,
                         common::ObExternalBlobKind &kind);

  DISALLOW_COPY_AND_ASSIGN(ObExprExternalLob);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_EXPR_OB_EXPR_EXTERNAL_LOB_H_
