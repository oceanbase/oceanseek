/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_CENTER_ID_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_CENTER_ID_H_
#include "sql/engine/expr/ob_expr_operator.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "share/logical_table/ob_logical_table_define.h"

namespace oceanbase
{
namespace sql
{

enum class ObIvfCentroidResolveMode : uint8_t
{
  STATIC = 0,
  SPFRESH = 1,
  LOGIC_NOT_SUPPORTED = 0xFF,
};

class ObExprVecIVFCenterID : public ObFuncExprOperator
{
public:
  // Logical-table SPFresh VEC_IVF_CENTER_ID has exactly six arguments. Codegen
  // converts them to CtDef metadata; the leader never receives this ObExpr.
  static constexpr int64_t LOGICAL_SPFRESH_ARG_COUNT = 6;
  static constexpr int64_t VECTOR_ARG_IDX = 0;
  static constexpr int64_t DISTANCE_ALGORITHM_ARG_IDX = 3;
  static constexpr int64_t NAMESPACE_ID_ARG_IDX = 4;
  static constexpr int64_t LOGICAL_TABLE_ID_ARG_IDX = 5;
  static constexpr int64_t CENTER_ID_BYTE_LENGTH = common::OB_DOC_ID_COLUMN_BYTE_LENGTH;
  explicit ObExprVecIVFCenterID(common::ObIAllocator &alloc);
  virtual ~ObExprVecIVFCenterID() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs_array,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_NULL_ON_WARN;}
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx,
      const ObRawExpr &raw_expr,
      ObExpr &rt_expr) const override;
  static int calc_center_id(
      const ObExpr &expr,
      ObEvalCtx &eval_ctx,
      ObDatum &expr_datum);
  // Fixed-width placeholder protocol shared with leader materialization.
  static bool is_deferred_placeholder(const common::ObString &value);
  static int restore_deferred_placeholder(common::ObDatum &datum);
  static bool is_deferred_enabled(const ObExpr &expr);
  static bool is_deferred_protocol_available(const uint64_t min_data_version);
  virtual bool need_rt_ctx() const override { return true; }
private :
  static constexpr uint64_t RESOLVE_MODE_MASK = 0xFF;
  static constexpr int64_t DEFERRED_FLAG_SHIFT = 16;
  static constexpr uint64_t DEFERRED_FLAG_MASK = 1ULL << DEFERRED_FLAG_SHIFT;
  static const char DEFERRED_PLACEHOLDER[CENTER_ID_BYTE_LENGTH];
  static uint64_t pack_extra(
      const ObIvfCentroidResolveMode resolve_mode,
      const bool enable_deferred);
  static ObIvfCentroidResolveMode get_resolve_mode(const ObExpr &expr);
  static int build_deferred_placeholder(
      const ObExpr &expr,
      ObEvalCtx &eval_ctx,
      ObDatum &expr_datum);
  static int calc_materialized_center_id(
      const ObExpr &expr,
      ObEvalCtx &eval_ctx,
      ObDatum &expr_datum);
  static int resolve_centroid_by_mode(
      const ObIvfCentroidResolveMode resolve_mode,
      common::ObIArrayType *arr,
      const ObTableID table_id,
      const ObTabletID tablet_id,
      const int64_t ns_id,
      const uint64_t ltable_id,
      const ObVectorIndexDistAlgorithm dis_algo,
      common::ObArenaAllocator &allocator,
      ObEvalCtx &eval_ctx,
      int64_t &center_idx);
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprVecIVFCenterID);
};
}
}
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_VEC_IVF_CENTER_ID_H_ */
