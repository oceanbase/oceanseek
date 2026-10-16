/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "sql/engine/expr/ob_expr_vec_ivf_center_id.h"
#include "sql/das/ob_ivf_cid_util.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "share/ob_cluster_version.h"
#include "sql/optimizer/ob_log_plan.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

const char ObExprVecIVFCenterID::DEFERRED_PLACEHOLDER[CENTER_ID_BYTE_LENGTH] = {
    'I', 'V', 'F', 'C', 'T', 'D', 'E', 'F',
    'D', 'E', 'F', 'E', 'R', 'R', 'E', 'D'};
STATIC_ASSERT(ObExprVecIVFCenterID::CENTER_ID_BYTE_LENGTH
                  == sizeof(common::ObCenterId),
              "deferred IVF placeholder must have the same width as ObCenterId");
int ObExprVecIVFCenterID::resolve_centroid_by_mode(
    const ObIvfCentroidResolveMode resolve_mode,
    ObIArrayType *arr,
    const ObTableID table_id,
    const ObTabletID tablet_id,
    const int64_t ns_id,
    const uint64_t ltable_id,
    const ObVectorIndexDistAlgorithm dis_algo,
    common::ObArenaAllocator &allocator,
    ObEvalCtx &eval_ctx,
    int64_t &center_idx)
{
  int ret = OB_SUCCESS;
  switch (resolve_mode) {
    case ObIvfCentroidResolveMode::STATIC:
      ret = ObIVFCidUtil::resolve_centroid_static(arr, table_id, tablet_id, dis_algo,
                                    allocator, eval_ctx, center_idx);
      break;
    case ObIvfCentroidResolveMode::SPFRESH:
      ret = ObIVFCidUtil::resolve_centroid_spfresh(arr, table_id, tablet_id, table_id,
                                     ns_id, ltable_id, dis_algo, allocator,
                                     eval_ctx, center_idx);
      break;
    case ObIvfCentroidResolveMode::LOGIC_NOT_SUPPORTED:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("non-SPFresh IVF index on logical table is not supported", K(ret), K(table_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-SPFresh IVF index on logical table is");
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ivf centroid resolve mode", K(ret), K(resolve_mode), K(table_id));
      break;
  }
  return ret;
}

ObExprVecIVFCenterID::ObExprVecIVFCenterID(common::ObIAllocator &allocator)
  : ObFuncExprOperator(allocator, T_FUN_SYS_VEC_IVF_CENTER_ID, N_VEC_IVF_CENTER_ID, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  need_charset_convert_ = false;
}

int ObExprVecIVFCenterID::calc_result_typeN(ObExprResType &type,
                                       ObExprResType *types,
                                       int64_t param_num,
                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSEDx(param_num, types);
  type.set_varchar();
  type.set_collation_type(CS_TYPE_BINARY);
  return ret;
}

int ObExprVecIVFCenterID::calc_resultN(common::ObObj &result,
                                  const common::ObObj *objs_array,
                                  int64_t param_num,
                                  ObExprCtx &expr_ctx) const
{
  // TODO by query ivf index
  return OB_NOT_SUPPORTED;
}

int ObExprVecIVFCenterID::cg_expr(
    ObExprCGCtx &expr_cg_ctx,
    const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIvfCentroidResolveMode resolve_mode = ObIvfCentroidResolveMode::STATIC;
  bool enable_deferred = false;
  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 4
                  && rt_expr.arg_cnt_ != LOGICAL_SPFRESH_ARG_COUNT
                  && rt_expr.arg_cnt_ != 1 && rt_expr.arg_cnt_ != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param count", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else if (OB_UNLIKELY((rt_expr.arg_cnt_ == 4
                          || rt_expr.arg_cnt_ == LOGICAL_SPFRESH_ARG_COUNT)
                         && OB_ISNULL(rt_expr.args_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rt_expr.args_ is nullptr", K(rt_expr.arg_cnt_), K(rt_expr.args_));
  } else {
    if (4 == rt_expr.arg_cnt_ || LOGICAL_SPFRESH_ARG_COUNT == rt_expr.arg_cnt_) {
      const ObTableID ref_table_id = static_cast<ObTableID>(raw_expr.get_ref_table_id());
      const share::schema::ObTableSchema *index_schema = nullptr;
      if (OB_INVALID_ID == ref_table_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref_table_id not resolved", K(ret), K(ref_table_id));
      } else if (OB_ISNULL(expr_cg_ctx.schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null in cg_expr", K(ret), K(ref_table_id));
      } else if (OB_FAIL(expr_cg_ctx.schema_guard_->get_table_schema(
              MTL_ID(), ref_table_id, index_schema))) {
        LOG_WARN("fail to get index table schema", K(ret), K(ref_table_id));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema not exist", K(ret), K(ref_table_id));
      } else {
        if (!index_schema->is_logic_table()) {
          resolve_mode = ObIvfCentroidResolveMode::STATIC;
        } else {
          bool is_spfresh = false;
          if (OB_FAIL(ObVectorIndexUtil::is_spfresh_mode(*index_schema, is_spfresh))) {
            LOG_WARN("failed to check SPFresh mode", K(ret), K(ref_table_id));
          } else if (is_spfresh) {
            resolve_mode = ObIvfCentroidResolveMode::SPFRESH;
          } else {
            resolve_mode = ObIvfCentroidResolveMode::LOGIC_NOT_SUPPORTED;
          }
        }
        // Deferred output is a DML transport protocol, not a global semantic
        // change to VEC_IVF_CENTER_ID.  SELECT/query evaluation, DDL and normal
        // physical IVF paths must continue to return a materialized 16B CID.
        enable_deferred = index_schema->is_logic_table()
            && ObIvfCentroidResolveMode::SPFRESH == resolve_mode
            && OB_NOT_NULL(expr_cg_ctx.log_plan_)
            && OB_NOT_NULL(expr_cg_ctx.log_plan_->get_stmt())
            && expr_cg_ctx.log_plan_->get_stmt()->is_dml_write_stmt();
        if (OB_SUCC(ret) && enable_deferred) {
          uint64_t min_data_version = 0;
          if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), min_data_version))) {
            LOG_WARN("get min data version for logical IVF DML failed", K(ret));
          } else if (!is_deferred_protocol_available(min_data_version)) {
            // Old leaders cannot decode v2 CtDef/placeholder during rolling
            // upgrade. Enable deferred codegen only after min data version
            // crosses the gate.
            enable_deferred = false;
            LOG_INFO("logical IVF deferred protocol is gated by min data version",
                     K(min_data_version), "required_version", DATA_VERSION_4_6_3_0);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      rt_expr.extra_ = pack_extra(resolve_mode, enable_deferred);
      rt_expr.eval_func_ = calc_center_id;
    }
  }
  return ret;
}

uint64_t ObExprVecIVFCenterID::pack_extra(
    const ObIvfCentroidResolveMode resolve_mode,
    const bool enable_deferred)
{
  return static_cast<uint64_t>(static_cast<uint8_t>(resolve_mode))
      | (enable_deferred ? DEFERRED_FLAG_MASK : 0);
}

ObIvfCentroidResolveMode ObExprVecIVFCenterID::get_resolve_mode(const ObExpr &expr)
{
  return static_cast<ObIvfCentroidResolveMode>(
      static_cast<uint8_t>(expr.extra_ & RESOLVE_MODE_MASK));
}

bool ObExprVecIVFCenterID::is_deferred_enabled(const ObExpr &expr)
{
  return 0 != (expr.extra_ & DEFERRED_FLAG_MASK);
}

int ObExprVecIVFCenterID::build_deferred_placeholder(
    const ObExpr &expr,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *vector_datum = nullptr;
  if (OB_UNLIKELY(LOGICAL_SPFRESH_ARG_COUNT != expr.arg_cnt_
      || OB_ISNULL(expr.args_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical ivf deferred expression", K(ret), K(expr));
  } else if (OB_ISNULL(expr.args_[VECTOR_ARG_IDX])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("logical ivf deferred expression has null vector argument", K(ret), K(expr));
  } else if (OB_FAIL(expr.args_[VECTOR_ARG_IDX]->eval(eval_ctx, vector_datum))) {
    LOG_WARN("evaluate deferred ivf vector failed", K(ret));
  } else if (OB_ISNULL(vector_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deferred ivf vector datum is null", K(ret));
  } else {
    // The legacy expression maps a NULL vector to a 16-byte center-0 CID. Keep
    // the fixed-width placeholder so the leader can preserve that behavior.
    char *buf = expr.get_str_res_mem(eval_ctx, CENTER_ID_BYTE_LENGTH);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate deferred ivf placeholder failed", K(ret));
    } else {
      MEMCPY(buf, DEFERRED_PLACEHOLDER, sizeof(DEFERRED_PLACEHOLDER));
      expr_datum.set_string(ObString(sizeof(DEFERRED_PLACEHOLDER), buf));
    }
  }
  return ret;
}

bool ObExprVecIVFCenterID::is_deferred_placeholder(const ObString &value)
{
  return value.length() == sizeof(DEFERRED_PLACEHOLDER)
      && 0 == MEMCMP(value.ptr(), DEFERRED_PLACEHOLDER,
                     sizeof(DEFERRED_PLACEHOLDER));
}

int ObExprVecIVFCenterID::restore_deferred_placeholder(ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum.ptr_)
      || datum.len_ != static_cast<uint32_t>(sizeof(DEFERRED_PLACEHOLDER))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid CID slot for deferred placeholder restore", K(ret), K(datum));
  } else {
    MEMCPY(const_cast<char *>(datum.ptr_), DEFERRED_PLACEHOLDER,
           sizeof(DEFERRED_PLACEHOLDER));
  }
  return ret;
}

bool ObExprVecIVFCenterID::is_deferred_protocol_available(
    const uint64_t min_data_version)
{
  return min_data_version >= DATA_VERSION_4_6_3_0;
}

int ObExprVecIVFCenterID::calc_center_id(
    const ObExpr &expr,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  const ObIvfCentroidResolveMode resolve_mode = get_resolve_mode(expr);
  bool allow_deferred = true;
  if (OB_UNLIKELY(ObIvfCentroidResolveMode::STATIC != resolve_mode
      && ObIvfCentroidResolveMode::SPFRESH != resolve_mode
      && ObIvfCentroidResolveMode::LOGIC_NOT_SUPPORTED != resolve_mode)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF centroid resolve mode", K(ret), K(expr.extra_),
             "resolve_mode", static_cast<uint8_t>(resolve_mode));
  } else if (OB_NOT_NULL(eval_ctx.exec_ctx_.get_sql_ctx())
      && OB_NOT_NULL(eval_ctx.exec_ctx_.get_sql_ctx()->session_info_)) {
    ObSQLSessionInfo *session =
        eval_ctx.exec_ctx_.get_sql_ctx()->session_info_;
    allow_deferred = !session->get_ddl_info().is_ddl() && !session->is_inner();
  }
  if (OB_FAIL(ret)) {
  } else if (allow_deferred
      && is_deferred_enabled(expr)
      && ObIvfCentroidResolveMode::SPFRESH == resolve_mode
      && LOGICAL_SPFRESH_ARG_COUNT == expr.arg_cnt_) {
    // Logical-table SPFresh assignment is stateful. The coordinator only
    // writes a fixed-width transport placeholder. The write leader reads the
    // vector Datum directly from StoredRow and calls the assignment algorithm;
    // it never evaluates this IVF expression.
    ret = build_deferred_placeholder(expr, eval_ctx, expr_datum);
  } else {
    ret = calc_materialized_center_id(expr, eval_ctx, expr_datum);
  }
  return ret;
}

int ObExprVecIVFCenterID::calc_materialized_center_id(
    const ObExpr &expr,
    ObEvalCtx &eval_ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (expr.arg_cnt_ == 1) {
    expr_datum.set_null();
    LOG_DEBUG("[vec index debug]succeed to genearte empty center id", KP(&expr), K(expr), K(expr_datum), K(eval_ctx));
  } else if (expr.arg_cnt_ == 2) {
    int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
    char *buf = expr.get_str_res_mem(eval_ctx, buf_len);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate center id result buffer failed", K(ret), K(buf_len));
    } else {
      ObString str(buf_len, 0, buf);
      ObCenterId center_id(1, 0);
      if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(center_id, str))) {
        LOG_WARN("failed to set center_id to string", K(ret), K(center_id), K(str));
      } else {
        expr_datum.set_string(str);
      }
    }
  } else if (OB_UNLIKELY((4 != expr.arg_cnt_
                          && LOGICAL_SPFRESH_ARG_COUNT != expr.arg_cnt_)
                         || OB_ISNULL(expr.args_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(expr), KP(expr.args_));
  } else {
    common::ObArenaAllocator tmp_allocator("IVFExprCID", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTableID table_id;
    ObTabletID tablet_id;
    ObVectorIndexDistAlgorithm dis_algo = VIDA_MAX;
    bool contain_null = false;
    ObIArrayType *arr = NULL;
    int64_t center_idx = 0;
    int64_t ns_id = share::ObIvfCacheMgrKey::INVALID_NS_ID;
    uint64_t ltable_id = share::OB_INVALID_LOGICAL_TABLE_ID;
    ObDatum *ns_datum = nullptr;
    ObDatum *lt_datum = nullptr;

    if (LOGICAL_SPFRESH_ARG_COUNT == expr.arg_cnt_) {
      ObExpr *ns_expr = expr.args_[NAMESPACE_ID_ARG_IDX];
      ObExpr *lt_expr = expr.args_[LOGICAL_TABLE_ID_ARG_IDX];
      if (OB_ISNULL(ns_expr) || OB_ISNULL(lt_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("namespace_id/ltable_id expr is null", K(ret), KP(ns_expr), KP(lt_expr));
      } else if (OB_FAIL(ns_expr->eval(eval_ctx, ns_datum))) {
        LOG_WARN("failed to eval namespace_id expr", K(ret));
      } else if (OB_ISNULL(ns_datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("namespace_id datum is null after eval", K(ret));
      } else if (ns_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("namespace_id is null", K(ret));
      } else if (OB_FAIL(lt_expr->eval(eval_ctx, lt_datum))) {
        LOG_WARN("failed to eval ltable_id expr", K(ret));
      } else if (OB_ISNULL(lt_datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ltable_id datum is null after eval", K(ret));
      } else if (lt_datum->is_null()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ltable_id is null", K(ret));
      } else {
        ns_id = static_cast<int64_t>(ns_datum->get_uint64());
        ltable_id = lt_datum->get_uint64();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObVectorIndexUtil::eval_ivf_centers_common(
        tmp_allocator, expr, eval_ctx, table_id, tablet_id, dis_algo, contain_null, arr))) {
      LOG_WARN("failed to eval ivf centers", K(ret), K(expr), K(eval_ctx));
    } else if (contain_null) {
      // do nothing, center_idx remains 0
    } else {
      ObIvfCentroidResolveMode mode = get_resolve_mode(expr);
      if (OB_FAIL(resolve_centroid_by_mode(
          mode, arr, table_id, tablet_id, ns_id, ltable_id,
          dis_algo, tmp_allocator, eval_ctx, center_idx))) {
        LOG_WARN("failed to resolve centroid by mode", K(ret), K(mode));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
      char *buf = expr.get_str_res_mem(eval_ctx, buf_len);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate center id result buffer failed", K(ret), K(buf_len));
      } else {
        ObString str(buf_len, 0, buf);
        ObCenterId center_id(tablet_id.id(), center_idx);
        if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(center_id, str))) {
          LOG_WARN("failed to set center_id to string", K(ret), K(center_id), K(str));
        } else {
          expr_datum.set_string(str);
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
