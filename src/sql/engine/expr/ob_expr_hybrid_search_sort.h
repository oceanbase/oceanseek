/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SQL_OB_EXPR_HYBRID_SEARCH_SORT_H_
#define OCEANBASE_SQL_OB_EXPR_HYBRID_SEARCH_SORT_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
/**
 * @brief Build __shard_doc from the main-table tablet ID and HEAP __pk_increment.
 * Its 32-character hex string gives rows with equal sort values a fixed order for search_after.
 */
class ObExprHybridUniqueShardDoc : public ObFuncExprOperator
{
public:
  /** @brief Register the internal __shard_doc function; disable automatic charset conversion. */
  explicit ObExprHybridUniqueShardDoc(common::ObIAllocator &alloc);
  /** @brief Require UINT64 inputs and return VARCHAR(32) with binary collation. */
  virtual int calc_result_type2(ObExprResType &type, ObExprResType &tablet,
                                ObExprResType &increment, common::ObExprTypeCtx &ctx) const override;
  /** @brief Set the __shard_doc evaluator and check that both inputs are present. */
  virtual int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw, ObExpr &expr) const override;
  /**
   * @brief Encode __shard_doc into memory owned by the expression result.
   * @return OB_SUCCESS on success, or an error for missing inputs, an invalid tablet ID, or allocation failure.
   */
  static int eval_unique_shard_doc(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
  /** @brief Validate a 32-digit hexadecimal __shard_doc and copy it as lowercase text. */
  static int normalize_shard_doc(common::ObIAllocator &allocator, const common::ObString &value,
                                 common::ObString &normalized);
  /** @brief Write tablet ID and heap increment as 32 hexadecimal bytes without a trailing NUL. */
  static int encode_shard_doc(uint64_t tablet_id, uint64_t increment, char *out);
};

/**
 * @brief Return each row's sort values as the __sort JSON array for the next search_after request.
 * Values follow the sort item order, including missing-value replacements and __shard_doc when present.
 */
class ObExprHybridSearchSortValues : public ObFuncExprOperator
{
public:
  /** @brief Register the internal __sort function. */
  explicit ObExprHybridSearchSortValues(common::ObIAllocator &alloc);
  /** @brief Set the JSON result and argument conversions using the public JSON value rules. */
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                int64_t count, common::ObExprTypeCtx &ctx) const override;
  /** @brief Set the evaluator that builds the __sort JSON array. */
  virtual int cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw, ObExpr &expr) const override;
  /**
   * @brief Build __sort with public JSON conversion; encode JSON sort keys as JSON text strings.
   * Temporary JSON nodes use the evaluation allocator; the result uses expression result memory.
   */
  static int eval_sort_values(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
};
} // namespace sql
} // namespace oceanbase
#endif
