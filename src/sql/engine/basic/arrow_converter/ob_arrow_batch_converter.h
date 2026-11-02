/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_BATCH_CONVERTER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_BATCH_CONVERTER_H_

#include "sql/engine/ob_batch_rows.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_operator.h"
#include <arrow/api.h>
#include <memory>

namespace oceanbase
{
namespace sql
{

// OB 执行期数据到 Arrow 列的转换器。
class ObArrowBatchConverter
{
public:
  // Type-level preflight for Flight SQL result columns.  Values and collection
  // payloads can still fail conversion while rows are fetched.
  static bool supports_output_type(common::ObObjType type);

  // 向量化路径：优先按 (OB type, arrow type) 到二维表中查找静态转换函数，
  // 未注册时回退到逐行 datum 路径。lob_arena 供 LOB/Json 临时解码复用。
  static int append_brs_to_builders(
      const ExprFixedArray &output_exprs,
      ObEvalCtx &eval_ctx,
      const ObBatchRows &brs,
      std::vector<std::unique_ptr<arrow::ArrayBuilder>> &builders,
      common::ObIAllocator &lob_arena);

  // 逐行路径：向调用方持有的 builder 追加一个已求值 ObDatum，仅供非向量化
  // reader 使用。lob_arena 由调用方按批次复用，避免每行单独创建临时分配器。
  static int append_datum_to_builder(const ObExpr &expr,
                                     const ObDatum &datum,
                                     arrow::ArrayBuilder &builder,
                                     common::ObIAllocator &lob_arena,
                                     ObEvalCtx *eval_ctx = nullptr);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_ARROW_BATCH_CONVERTER_H_
