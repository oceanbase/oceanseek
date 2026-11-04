/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_AI_DECIDE_H_
#define OCEANBASE_SQL_OB_EXPR_AI_DECIDE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_multi_mode_func_helper.h"
#include "lib/json_type/ob_json_tree.h"

namespace oceanbase
{
namespace common
{
struct ObAIFuncBatchState;
class ObAIFuncClient;
}
namespace share
{
struct ObAIModelConfigInfo;
class ObAiGatewayRouteSession;
}
namespace sql
{

class ObExprAIDecide : public ObFuncExprOperator
{
public:
  explicit ObExprAIDecide(common::ObIAllocator &allocator);
  virtual ~ObExprAIDecide() = default;
  virtual int calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                int64_t param_num, common::ObExprTypeCtx &type_ctx) const override;
  static int eval_ai_decide(const ObExpr &expr, ObEvalCtx &ctx, common::ObDatum &result);
  static int eval_ai_decide_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
private:
  static int check_decision_config(const share::ObAIModelConfigInfo &config);
  static int build_body(common::ObIAllocator &allocator, const share::ObAIModelConfigInfo &config,
                        common::ObJsonNode *state, common::ObJsonNode *questions,
                        common::ObJsonObject *&body);
  static int send_request(common::ObIAllocator &allocator, const share::ObAIModelConfigInfo &config,
                          common::ObJsonNode *state, common::ObJsonNode *questions,
                          common::ObJsonObject *&output);
  static int init_decision_pending(common::ObIAllocator &allocator, const common::ObString &model,
                                   common::ObAIFuncBatchState &pending);
  static int pack_batch_response(const ObExpr &expr, ObEvalCtx &ctx,
                                 common::ObIAllocator &allocator,
                                 common::ObJsonObject *response,
                                 const common::ObArray<int64_t> &rows,
                                 const share::ObAIModelConfigInfo &config, int64_t user_dim,
                                 common::ObIVector *result);
  static int read_request_args(const ObExpr &expr, ObEvalCtx &ctx, MultimodeAlloctor &allocator,
                               const common::ObDatum &state_datum,
                               const common::ObDatum &questions_datum,
                               common::ObJsonNode *&state, common::ObJsonNode *&questions);
  static constexpr int64_t MODEL_IDX = 0;
  static constexpr int64_t STATE_IDX = 1;
  static constexpr int64_t QUESTIONS_IDX = 2;
  static constexpr int64_t PARAM_COUNT = 3;
  DISALLOW_COPY_AND_ASSIGN(ObExprAIDecide);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_AI_DECIDE_H_
