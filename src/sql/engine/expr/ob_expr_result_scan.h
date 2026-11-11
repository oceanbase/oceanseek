/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RESULT_SCAN_H_
#define OCEANBASE_SQL_OB_EXPR_RESULT_SCAN_H_

#include "common/ob_accuracy.h"
#include "common/object/ob_obj_type.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObRawExpr;

struct ObResultScanColDesc
{
  common::ObString col_name;
  uint64_t column_id{common::OB_INVALID_ID};
  common::ObObjType ob_type;
  common::ObAccuracy accuracy;
  common::ObCollationType cs_type;
  uint16_t subschema_id{UINT_MAX16};

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos,
        "{col_name:%.*s, column_id:%lu, ob_type:%d}",
        col_name.length(), col_name.ptr(), static_cast<unsigned long>(column_id),
        static_cast<int>(ob_type));
    return pos;
  }
};

// Marker expression for TABLE(RESULT_SCAN('<query_id>')). The resolver uses
// the public scheduler query handle to obtain the published result schema. A
// later pre-process transform replaces the logical function table before
// execution.
class ObExprResultScan : public ObFuncExprOperator
{
public:
  explicit ObExprResultScan(common::ObIAllocator &alloc);
  virtual ~ObExprResultScan();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  // query_id is deliberately literal-only. It identifies both the result
  // schema and the authorization target during SQL resolution.
  static int extract_query_id(const ObRawExpr *expr, common::ObString &query_id);
  // The optional chunk id is a zero-based global Parquet row-group ordinal.
  // OB_INVALID_INDEX_INT64 means TABLE(RESULT_SCAN(query_id)) scans all chunks.
  static int extract_query_id_and_chunk_no(const ObRawExpr *expr,
                                           common::ObString &query_id,
                                           int64_t &chunk_no);
  static int eval_result_scan(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprResultScan);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_EXPR_RESULT_SCAN_H_
