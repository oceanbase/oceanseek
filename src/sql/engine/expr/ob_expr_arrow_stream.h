/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARROW_STREAM_H_
#define OCEANBASE_SQL_OB_EXPR_ARROW_STREAM_H_

#include <cstdint>
#include "sql/engine/expr/ob_expr_operator.h"
#include "common/ob_accuracy.h"
#include "common/object/ob_object.h"
#include "common/object/ob_obj_type.h"
#include "lib/container/ob_array.h"
#include "lib/charset/ob_mysql_global.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{

class ObRawExpr;

// ---------------------------------------------------------------------------
// Per-column descriptor returned by get_arrow_stream_col_descs().
// Used by the resolver and ObFunctionTableOp to set up column metadata.
// ---------------------------------------------------------------------------
struct ObArrowStreamColDesc
{
  common::ObString   col_name;
  common::ObObjType  ob_type;
  common::ObAccuracy accuracy;
  common::ObCollationType cs_type;       // for string columns
  // For ObCollectionSQLType: subschema_id resolved by the catalog via
  // exec_ctx->get_subschema_id_by_type_string.  UINT_MAX16 = not set.
  uint16_t           subschema_id{UINT_MAX16};

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos,
        "{col_name:%.*s, ob_type:%d}",
        col_name.length(), col_name.ptr(), static_cast<int>(ob_type));
    return pos;
  }
};

// ---------------------------------------------------------------------------
// ObExprArrowStream — marker expression for TABLE(arrow_stream(stream_id))
//
// ObFunctionTableOp detects T_FUN_SYS_ARROW_STREAM and handles row iteration
// by reading directly from the session-owned ObArrowStreamRegistry.
// eval_func is a sentinel that should never be invoked in normal operation.
// ---------------------------------------------------------------------------
class ObExprArrowStream : public ObFuncExprOperator
{
public:
  explicit ObExprArrowStream(common::ObIAllocator &alloc);
  virtual ~ObExprArrowStream();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &stream_id,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  // Extract the literal stream id from an arrow_stream(stream_id) raw expression.
  static int extract_stream_id(const ObRawExpr *expr, uint64_t &stream_id);

  static int eval_arrow_stream(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrowStream);
};

// V2 multi-stream: TABLE(arrow_streams('ingest_id'))
// Takes a VARCHAR ingest_id; VecOp iterates all registered slots sequentially.
class ObExprArrowStreams : public ObFuncExprOperator
{
public:
  explicit ObExprArrowStreams(common::ObIAllocator &alloc);
  virtual ~ObExprArrowStreams();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &ingest_id,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_arrow_streams(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrowStreams);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_EXPR_ARROW_STREAM_H_
