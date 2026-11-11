/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/file_prune/ob_ext_predicate_json.h"

#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/rewrite/ob_query_range_define.h"
#include "plugin/v2/include/ob_external_table_plugin.h"  // OB_EXT_K_* + grammar
#include "common/object/ob_object.h"  // OB_APP_MIN_COLUMN_ID, ObObj
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace sql
{
namespace ext_predicate
{
namespace
{

// Per-call context carried through the recursive emission.
struct EmitCtx
{
  common::ObIAllocator &alloc;
  ObExecContext *exec_ctx;
  const common::ObIArray<uint64_t> &partition_col_ids;
};

// Strip lossless cast and return the underlying column-ref expr, or null if
// `e` is not a column ref. Mirrors legacy `get_column_ref_without_lossless_cast`.
int get_column_ref(const ObRawExpr *e, const ObColumnRefRawExpr *&col)
{
  int ret = OB_SUCCESS;
  col = nullptr;
  const ObRawExpr *real = e;
  if (OB_ISNULL(e)) {
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_without_lossless_cast(real, real))) {
    LOG_WARN("strip lossless cast failed", K(ret), KPC(e));
  } else if (real->is_column_ref_expr()) {
    col = static_cast<const ObColumnRefRawExpr *>(real);
  }
  return ret;
}

bool is_partition_col(const EmitCtx &ctx, const ObColumnRefRawExpr &col)
{
  const uint64_t col_id = col.get_column_id();
  for (int64_t i = 0; i < ctx.partition_col_ids.count(); ++i) {
    if (ctx.partition_col_ids.at(i) == col_id) { return true; }
  }
  return false;
}

// Fold a const expr to ObObj. Returns OB_SUCCESS with is_valid=false for
// non-foldable (dynamic param / non-const) — caller skips the predicate.
int fold_const(const EmitCtx &ctx, const ObRawExpr *e, common::ObObj &obj, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(e) || OB_ISNULL(ctx.exec_ctx)) {
  } else if (e->has_flag(CNT_DYNAMIC_PARAM)) {
    // exec params evaluate at runtime — not safe for pushdown.
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.exec_ctx, e, obj, is_valid,
                                                                ctx.alloc))) {
    LOG_WARN("calc const expr failed", K(ret), KPC(e));
  } else if (!is_valid || obj.is_unknown() || obj.is_nop_value()) {
    is_valid = false;
  }
  return ret;
}

// JSON-escape a string into `out`.
int json_escape(common::ObSqlString &out, const char *p, int64_t len)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
    char c = p[i];
    switch (c) {
      case '"':  ret = out.append("\\\""); break;
      case '\\': ret = out.append("\\\\"); break;
      case '\n': ret = out.append("\\n");  break;
      case '\r': ret = out.append("\\r");  break;
      case '\t': ret = out.append("\\t");  break;
      case '\b': ret = out.append("\\b");  break;
      case '\f': ret = out.append("\\f");  break;
      default:
        // Control chars -> \u00XX; otherwise literal.
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
          ret = out.append(buf);
        } else {
          ret = out.append(&c, 1);
        }
    }
  }
  return ret;
}

int json_escape(common::ObSqlString &out, const common::ObString &s)
{
  return json_escape(out, s.ptr(), s.length());
}

// Render an ObObj's value as the lit node's `value` string (already JSON-escaped).
int obj_to_lit_value(common::ObSqlString &out, const common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (obj.is_null()) {
    // NULL literal: emit as null JSON literal value? The contract lit node is a
    // string; partition maps cannot carry NULL, and equality against
    // NULL is not meaningful. Skip (treat as non-convertible) by returning err.
    ret = OB_ERR_UNEXPECTED;
  } else if (obj.is_string_type() || obj.is_lob()) {
    const common::ObString s = obj.get_string();
    ret = json_escape(out, s);
  } else {
    char buf[OB_MAX_ROW_KEY_LENGTH];
    int64_t pos = 0;
    if (OB_FAIL(obj.print_plain_str_literal(buf, sizeof(buf), pos))) {
      LOG_WARN("print obj plain str failed", K(ret), K(obj));
    } else {
      ret = json_escape(out, ObString(pos, buf));
    }
  }
  return ret;
}

// Emit a `col` node. `partition_eligible` is AND-ed with whether this col is a
// partition column (a non-partition col disqualifies the subtree from
// partition_filter).
int emit_col(const EmitCtx &ctx, const ObColumnRefRawExpr &col, bool &partition_eligible,
             common::ObSqlString &out, const ObCollationType cmp_collation = CS_TYPE_INVALID)
{
  int ret = OB_SUCCESS;
  if (!is_partition_col(ctx, col)) { partition_eligible = false; }
  const int64_t field_id =
      static_cast<int64_t>(col.get_column_id() - OB_APP_MIN_COLUMN_ID);
  if (OB_FAIL(out.append_fmt("{\"%s\":\"col\",\"%s\":%ld,\"%s\":\"",
                             OB_EXT_K_KIND, OB_EXT_K_COL_IDX,
                             static_cast<long>(field_id), OB_EXT_K_NAME))) {
  } else if (OB_FAIL(json_escape(out, col.get_column_name()))) {
  } else if (col.get_result_type().is_string_type() && cmp_collation != CS_TYPE_INVALID) {
    ret = out.append_fmt("\",\"%s\":%d,\"%s\":\"%s\"}", OB_EXT_K_RESULT_TYPE_ID,
                         static_cast<int>(col.get_data_type()), OB_EXT_K_COLLATION,
                         ObCharset::collation_name(cmp_collation));
  } else {
    ret = out.append_fmt("\",\"%s\":%d}", OB_EXT_K_RESULT_TYPE_ID,
                         static_cast<int>(col.get_data_type()));
  }
  return ret;
}

// Emit a `lit` node from a const expr (folded). Returns OB_ERR_UNEXPECTED if the
// const is not foldable / null (caller skips the whole predicate).
int emit_lit(const EmitCtx &ctx, const ObRawExpr *const_expr, common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  common::ObObj obj;
  bool is_valid = false;
  if (OB_FAIL(fold_const(ctx, const_expr, obj, is_valid))) {
  } else if (!is_valid) {
    ret = OB_ERR_UNEXPECTED;  // non-foldable -> skip predicate
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"lit\",\"%s\":\"", OB_EXT_K_KIND, OB_EXT_K_VALUE))) {
  } else if (OB_FAIL(obj_to_lit_value(out, obj))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = out.append_fmt("\",\"%s\":%d}", OB_EXT_K_RESULT_TYPE_ID,
                         static_cast<int>(const_expr->get_data_type()));
  }
  return ret;
}

// A raw comparison does not expose its calculation collation directly. For
// string operands in the same charset, the stronger coercibility determines
// the comparison collation; equal-strength disagreements remain unproven.
ObCollationType common_string_collation(const ObRawExpr *left, const ObRawExpr *right)
{
  ObCollationType collation = CS_TYPE_INVALID;
  if (OB_NOT_NULL(left) && OB_NOT_NULL(right)
      && left->get_result_type().is_string_type()
      && right->get_result_type().is_string_type()) {
    const ObRawExprResType &lhs = left->get_result_type();
    const ObRawExprResType &rhs = right->get_result_type();
    if (lhs.get_collation_type() == rhs.get_collation_type()) {
      collation = lhs.get_collation_type();
    } else if (ObCharset::charset_type_by_coll(lhs.get_collation_type())
               == ObCharset::charset_type_by_coll(rhs.get_collation_type())
               && lhs.get_collation_level() != CS_LEVEL_INVALID
               && rhs.get_collation_level() != CS_LEVEL_INVALID) {
      if (lhs.get_collation_level() < rhs.get_collation_level()) {
        collation = lhs.get_collation_type();
      } else if (rhs.get_collation_level() < lhs.get_collation_level()) {
        collation = rhs.get_collation_type();
      }
    }
  }
  return collation;
}

// Normalize a comparison op when the col/const sides are reversed (e.g. `5 > col`
// -> `col < 5`). Mirrors legacy `normalize_cmp_op_type`.
ObItemType normalize_cmp_op(ObItemType op, bool reversed)
{
  if (!reversed) { return op; }
  switch (op) {
    case T_OP_LT: return T_OP_GT;
    case T_OP_GT: return T_OP_LT;
    case T_OP_LE: return T_OP_GE;
    case T_OP_GE: return T_OP_LE;
    default:      return op;  // eq/ne symmetric
  }
}

const char *cmp_op_name(ObItemType op)
{
  switch (op) {
    case T_OP_EQ:   return "eq";
    case T_OP_NE:   return "ne";
    case T_OP_LT:   return "lt";
    case T_OP_LE:   return "le";
    case T_OP_GT:   return "gt";
    case T_OP_GE:   return "ge";
    default:        return nullptr;
  }
}

// Recursive emit. OB_SUCCESS + `ok=true` => `out` holds the node JSON and
// `partition_eligible` is set. OB_SUCCESS + `ok=false` => not convertible
// (skip). Error => abort.
int emit_node(const EmitCtx &ctx, const ObRawExpr *e, bool &partition_eligible,
              bool &ok, common::ObSqlString &out, const int64_t depth = 0)
{
  int ret = OB_SUCCESS;
  ok = false;
  partition_eligible = true;
  if (OB_ISNULL(e)) {
  } else if (depth >= 64) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    const ObItemType type = e->get_expr_type();
    switch (type) {
      case T_OP_EQ:
      case T_OP_LT:
      case T_OP_LE:
      case T_OP_GT:
      case T_OP_GE:
      case T_OP_NE: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() != 2) {
        } else {
          const ObColumnRefRawExpr *col = nullptr;
          const ObRawExpr *left = op_expr->get_param_expr(0);
          const ObRawExpr *right = op_expr->get_param_expr(1);
          const ObRawExpr *const_expr = nullptr;
          bool reversed = false;
          if (OB_FAIL(get_column_ref(left, col))) {
            LOG_WARN("get col ref failed", K(ret), KPC(left));
          } else if (col != nullptr) {
            const_expr = right;
          } else if (OB_FAIL(get_column_ref(right, col))) {
            LOG_WARN("get col ref failed", K(ret), KPC(right));
          } else if (col != nullptr) {
            const_expr = left;
            reversed = true;
          }
          if (OB_SUCC(ret) && col != nullptr && const_expr != nullptr) {
            const ObItemType op = normalize_cmp_op(type, reversed);
            const char *opn = cmp_op_name(op);
            const ObCollationType cmp_collation = common_string_collation(left, right);
            if (opn != nullptr
                && OB_FAIL(out.append_fmt("{\"%s\":\"cmp\",\"%s\":\"%s\",\"%s\":[",
                                          OB_EXT_K_KIND, OB_EXT_K_OP, opn,
                                          OB_EXT_K_CHILDREN))) {
            } else if (OB_FAIL(emit_col(ctx, *col, partition_eligible, out,
                                        cmp_collation))) {
            } else if (OB_FAIL(out.append(","))) {
            } else if (OB_FAIL(emit_lit(ctx, const_expr, out))) {
              ret = OB_SUCCESS;  // non-foldable const -> skip this cmp
            } else if (OB_FAIL(out.append("]}"))) {
            } else {
              ok = true;
            }
          }
        }
        break;
      }
      case T_OP_BTW: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        const ObColumnRefRawExpr *col = nullptr;
        ObCollationType cmp_collation = CS_TYPE_INVALID;
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() != 3) {
        } else if (OB_FAIL(get_column_ref(op_expr->get_param_expr(0), col))) {
          LOG_WARN("get between col ref failed", K(ret), KPC(e));
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(col)) {
          cmp_collation = common_string_collation(
              op_expr->get_param_expr(0), op_expr->get_param_expr(1));
          if (cmp_collation != common_string_collation(
                  op_expr->get_param_expr(0), op_expr->get_param_expr(2))) {
            cmp_collation = CS_TYPE_INVALID;
          }
        }
        if (OB_FAIL(ret) || OB_ISNULL(col)) {
        } else if (OB_FAIL(out.append_fmt(
                       "{\"%s\":\"and\",\"%s\":[{\"%s\":\"cmp\",\"%s\":\"ge\",\"%s\":[",
                       OB_EXT_K_KIND, OB_EXT_K_CHILDREN, OB_EXT_K_KIND, OB_EXT_K_OP,
                       OB_EXT_K_CHILDREN))) {
        } else if (OB_FAIL(emit_col(ctx, *col, partition_eligible, out,
                                    cmp_collation))) {
        } else if (OB_FAIL(out.append(","))) {
        } else if (OB_FAIL(emit_lit(ctx, op_expr->get_param_expr(1), out))) {
          // The generic fallback below resets this incomplete compact JSON.
          ret = OB_SUCCESS;
        } else if (OB_FAIL(out.append_fmt(
                       "]},{\"%s\":\"cmp\",\"%s\":\"le\",\"%s\":[",
                       OB_EXT_K_KIND, OB_EXT_K_OP, OB_EXT_K_CHILDREN))) {
        } else if (OB_FAIL(emit_col(ctx, *col, partition_eligible, out,
                                    cmp_collation))) {
        } else if (OB_FAIL(out.append(","))) {
        } else if (OB_FAIL(emit_lit(ctx, op_expr->get_param_expr(2), out))) {
          // The generic fallback below resets this incomplete compact JSON.
          ret = OB_SUCCESS;
        } else if (OB_FAIL(out.append("]}]}"))) {
        } else {
          ok = true;
        }
        break;
      }
      case T_OP_IN:
      case T_OP_NOT_IN: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() != 2) {
        } else {
          const ObRawExpr *left = op_expr->get_param_expr(0);
          const ObRawExpr *right = op_expr->get_param_expr(1);
          const ObColumnRefRawExpr *col = nullptr;
          if (OB_FAIL(get_column_ref(left, col))) {
            LOG_WARN("get col ref failed", K(ret), KPC(left));
          } else if (col == nullptr || OB_ISNULL(right)
                     || T_OP_ROW != right->get_expr_type()) {
          } else {
            const ObOpRawExpr *row_expr = static_cast<const ObOpRawExpr *>(right);
            const char *kind = (T_OP_IN == type) ? "in" : "not_in";
            ObCollationType cmp_collation = CS_TYPE_INVALID;
            if (row_expr->get_param_count() > 0) {
              cmp_collation = common_string_collation(left, row_expr->get_param_expr(0));
              for (int64_t i = 1; i < row_expr->get_param_count(); ++i) {
                if (cmp_collation != common_string_collation(left, row_expr->get_param_expr(i))) {
                  cmp_collation = CS_TYPE_INVALID;
                  break;
                }
              }
            }
            if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[", OB_EXT_K_KIND, kind,
                                       OB_EXT_K_CHILDREN))) {
            } else if (OB_FAIL(emit_col(ctx, *col, partition_eligible, out,
                                        cmp_collation))) {
            } else {
              bool all_lit_ok = true;
              for (int64_t i = 0; OB_SUCC(ret) && all_lit_ok
                                  && i < row_expr->get_param_count(); ++i) {
                if (OB_FAIL(out.append(","))) {
                } else if (OB_FAIL(emit_lit(ctx, row_expr->get_param_expr(i), out))) {
                  ret = OB_SUCCESS;  // non-foldable item -> whole IN not convertible
                  all_lit_ok = false;
                }
              }
              if (OB_SUCC(ret) && all_lit_ok && OB_FAIL(out.append("]}"))) {
              } else if (OB_SUCC(ret) && all_lit_ok) {
                ok = true;
              }
            }
          }
        }
        break;
      }
      case T_OP_IS:
      case T_OP_IS_NOT: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() < 2) {
        } else {
          const ObRawExpr *left = op_expr->get_param_expr(0);
          const ObRawExpr *right = op_expr->get_param_expr(1);
          const ObColumnRefRawExpr *col = nullptr;
          common::ObObj const_obj;
          bool is_valid = false;
          if (OB_FAIL(get_column_ref(left, col))) {
            LOG_WARN("get col ref failed", K(ret), KPC(left));
          } else if (col == nullptr) {
          } else if (OB_FAIL(fold_const(ctx, right, const_obj, is_valid))) {
          } else if (!is_valid || !const_obj.is_null()) {
            // IS <not-null> is not the IS NULL pattern; skip.
          } else {
            const char *kind = (T_OP_IS == type) ? "is_null" : "is_not_null";
            if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[", OB_EXT_K_KIND, kind,
                                       OB_EXT_K_CHILDREN))) {
            } else if (OB_FAIL(emit_col(ctx, *col, partition_eligible, out))) {
            } else if (OB_FAIL(out.append("]}"))) {
            } else {
              ok = true;
            }
          }
        }
        break;
      }
      case T_OP_AND:
      case T_OP_OR: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        const char *logical_op = type == T_OP_AND ? "and" : "or";
        bool all_ok = true;
        bool all_part_eligible = true;
        int64_t emitted_count = 0;
        if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[", OB_EXT_K_KIND,
                                   logical_op, OB_EXT_K_CHILDREN))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && all_ok
                              && i < op_expr->get_param_count(); ++i) {
            bool child_ok = false;
            bool child_part = true;
            common::ObSqlString child_json;
            if (OB_FAIL(emit_node(ctx, op_expr->get_param_expr(i), child_part, child_ok,
                                 child_json, depth + 1))) {
              LOG_WARN("emit compound-child failed", K(ret), K(i));
            } else if (!child_ok) {
              // Preserve the complete tree. A plugin, not OB, decides whether
              // a child may be omitted from a pushed predicate.
              all_ok = false;
            } else {
              if (emitted_count > 0 && OB_FAIL(out.append(","))) {}
              if (OB_SUCC(ret) && OB_FAIL(out.append(child_json.string()))) {
              } else {
                ++emitted_count;
                if (!child_part) { all_part_eligible = false; }
              }
            }
          }
          if (OB_SUCC(ret) && all_ok && emitted_count > 0) {
            if (OB_FAIL(out.append("]}"))) {
            } else {
              ok = true;
              partition_eligible = all_part_eligible;
            }
          }
        }
        break;
      }
      case T_OP_NOT: {
        const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr *>(e);
        if (OB_ISNULL(op_expr) || op_expr->get_param_count() != 1) {
        } else {
          bool child_ok = false;
          bool child_part = true;
          common::ObSqlString child_json;
          if (OB_FAIL(emit_node(ctx, op_expr->get_param_expr(0), child_part, child_ok,
                               child_json, depth + 1))) {
          } else if (!child_ok) {
          } else if (OB_FAIL(out.append_fmt("{\"%s\":\"not\",\"%s\":[", OB_EXT_K_KIND,
                                              OB_EXT_K_CHILDREN))) {
          } else if (OB_FAIL(out.append(child_json.string()))) {
          } else if (OB_FAIL(out.append("]}"))) {
          } else {
            partition_eligible = child_part;
            ok = true;
          }
        }
        break;
      }
      default:
        // Generic expression encoding below preserves operators which do not
        // match the compact col/literal predicate grammar.
        break;
    }
    if (OB_SUCC(ret) && !ok) {
      out.reset();
      if (e->is_column_ref_expr()) {
        ret = emit_col(ctx, *static_cast<const ObColumnRefRawExpr *>(e),
                       partition_eligible, out);
        ok = OB_SUCC(ret);
      } else if (e->is_const_raw_expr()) {
        common::ObObj value;
        bool valid = false;
        if (OB_FAIL(fold_const(ctx, e, value, valid))) {
          LOG_WARN("fold generic literal failed", K(ret));
        } else if (valid && !value.is_null()) {
          if (OB_FAIL(emit_lit(ctx, e, out))) {
            LOG_WARN("emit generic literal failed", K(ret));
          } else {
            ok = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !ok) {
      out.reset();
      partition_eligible = true;
      // A dynamic parameter has no value at planning time. Keep its position
      // in the tree so plugins cannot mistake the surrounding expression for
      // a complete supported predicate. TODO: add bound-parameter transport
      // before plugins may evaluate such nodes themselves.
      const char *op_name = get_type_name(type);
      if (OB_FAIL(out.append_fmt("{\"%s\":\"expr\",\"%s\":\"", OB_EXT_K_KIND,
                                 OB_EXT_K_OP))) {
      } else if (OB_FAIL(json_escape(out, op_name, STRLEN(op_name)))) {
      } else if (OB_FAIL(out.append_fmt(
                     "\",\"%s\":%d,\"%s\":%d,\"%s\":%d,\"%s\":%d,\"%s\":%s",
                     OB_EXT_K_OP_ID, static_cast<int>(type),
                     OB_EXT_K_RESULT_TYPE_ID, static_cast<int>(e->get_data_type()),
                     OB_EXT_K_PRECISION, static_cast<int>(e->get_accuracy().get_precision()),
                     OB_EXT_K_SCALE, static_cast<int>(e->get_accuracy().get_scale()),
                     OB_EXT_K_NULLABLE, e->is_not_null_for_read() ? "false" : "true"))) {
      } else {
        if (e->is_sys_func_expr()) {
          const common::ObString &function_name =
              static_cast<const ObSysFunRawExpr *>(e)->get_func_name();
          if (OB_FAIL(out.append_fmt(",\"%s\":\"", OB_EXT_K_NAME))) {
          } else if (OB_FAIL(json_escape(out, function_name))) {
          } else if (OB_FAIL(out.append("\""))) {
          }
        }
        const ObCollationType comparison_collation = e->get_param_count() == 2
            ? common_string_collation(e->get_param_expr(0), e->get_param_expr(1))
            : CS_TYPE_INVALID;
        if (OB_FAIL(ret)) {
        } else if (comparison_collation != CS_TYPE_INVALID
            && OB_FAIL(out.append_fmt(",\"%s\":\"%s\"",
                                      OB_EXT_K_COMPARE_COLLATION,
                                      ObCharset::collation_name(comparison_collation)))) {
        } else if (OB_FAIL(out.append_fmt(",\"%s\":[", OB_EXT_K_CHILDREN))) {
        } else {
          bool children_complete = true;
          for (int64_t i = 0; OB_SUCC(ret) && children_complete
                              && i < e->get_param_count(); ++i) {
            bool child_ok = false;
            bool child_part = true;
            common::ObSqlString child_json;
            if (OB_FAIL(emit_node(ctx, e->get_param_expr(i), child_part, child_ok,
                                  child_json, depth + 1))) {
              LOG_WARN("emit generic child failed", K(ret), K(i));
            } else if (!child_ok) {
              children_complete = false;
            } else if (i > 0 && OB_FAIL(out.append(","))) {
            } else if (OB_FAIL(out.append(child_json.string()))) {
            } else if (!child_part) {
              partition_eligible = false;
            }
          }
          if (OB_SUCC(ret) && children_complete && OB_FAIL(out.append("]}"))) {
          } else if (OB_SUCC(ret) && children_complete) {
            ok = true;
          }
        }
      }
    }
  }
  return ret;
}

// Wrap a list of node JSON strings into an AND root (or return the single node
// as-is). `parts` are already-emitted node JSON strings.
int wrap_and(const common::ObIArray<common::ObSqlString> &parts, common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  if (parts.count() == 1) {
    ret = out.append(parts.at(0).string());
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"and\",\"%s\":[", OB_EXT_K_KIND,
                                     OB_EXT_K_CHILDREN))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parts.count(); ++i) {
      if (i > 0 && OB_FAIL(out.append(","))) {}
      if (OB_SUCC(ret) && OB_FAIL(out.append(parts.at(i).string()))) {}
    }
    if (OB_SUCC(ret)) { ret = out.append("]}"); }
  }
  return ret;
}

struct RuntimeEmitCtx
{
  const common::ObIArray<uint64_t> &column_ids;
  const common::ObIArray<common::ObString> &column_names;
};

bool find_runtime_column(const RuntimeEmitCtx &ctx,
                         const uint64_t column_id,
                         common::ObString &column_name)
{
  bool found = false;
  for (int64_t i = 0; !found && i < ctx.column_ids.count(); ++i) {
    if (ctx.column_ids.at(i) == column_id) {
      column_name = ctx.column_names.at(i);
      found = true;
    }
  }
  return found;
}

int emit_runtime_col(const RuntimeEmitCtx &ctx,
                     const uint64_t column_id,
                     bool &ok,
                     common::ObSqlString &out,
                     const ObObjMeta *cmp_meta = nullptr)
{
  int ret = OB_SUCCESS;
  ok = false;
  common::ObString column_name;
  if (column_id < OB_APP_MIN_COLUMN_ID
      || !find_runtime_column(ctx, column_id, column_name)) {
    // The executor can reference a pseudo/derived column with no plugin field.
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"col\",\"%s\":%ld,\"%s\":\"",
                                    OB_EXT_K_KIND, OB_EXT_K_COL_IDX,
                                    static_cast<long>(column_id - OB_APP_MIN_COLUMN_ID),
                                    OB_EXT_K_NAME))) {
  } else if (OB_FAIL(json_escape(out, column_name))) {
  } else {
    if (OB_NOT_NULL(cmp_meta) && cmp_meta->is_string_type()
        && cmp_meta->get_collation_type() != CS_TYPE_INVALID) {
      ret = out.append_fmt("\",\"%s\":\"%s\"}", OB_EXT_K_COLLATION,
                           ObCharset::collation_name(cmp_meta->get_collation_type()));
    } else {
      ret = out.append("\"}");
    }
    if (OB_SUCC(ret)) {
      ok = true;
    }
  }
  return ret;
}

int emit_runtime_lit(const ObWhiteFilterExecutor &filter,
                     const int64_t datum_idx,
                     bool &ok,
                     common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  common::ObObjMeta obj_meta;
  common::ObObj obj;
  if (datum_idx < 0 || datum_idx >= datums.count()) {
  } else if (WHITE_OP_IN == filter.get_op_type()
             && OB_FAIL(filter.get_filter_node().get_filter_in_val_meta(datum_idx, obj_meta))) {
    LOG_WARN("get runtime IN literal meta failed", K(ret), K(datum_idx));
  } else {
    if (WHITE_OP_IN != filter.get_op_type()) {
      obj_meta = filter.get_param_obj_meta();
    }
    if (datums.at(datum_idx).is_null()) {
      // NULL comparisons are not represented by a lit node.
    } else if (OB_FAIL(datums.at(datum_idx).to_obj(obj, obj_meta))) {
      LOG_WARN("convert runtime predicate datum failed", K(ret), K(datum_idx), K(obj_meta));
    } else if (OB_FAIL(out.append_fmt("{\"%s\":\"lit\",\"%s\":\"",
                                      OB_EXT_K_KIND, OB_EXT_K_VALUE))) {
    } else if (OB_FAIL(obj_to_lit_value(out, obj))) {
    } else if (OB_FAIL(out.append("\"}"))) {
    } else {
      ok = true;
    }
  }
  return ret;
}

const char *white_cmp_op_name(const ObWhiteFilterOperatorType op)
{
  switch (op) {
    case WHITE_OP_EQ: return "eq";
    case WHITE_OP_NE: return "ne";
    case WHITE_OP_LT: return "lt";
    case WHITE_OP_LE: return "le";
    case WHITE_OP_GT: return "gt";
    case WHITE_OP_GE: return "ge";
    default:          return nullptr;
  }
}

int emit_runtime_cmp(const RuntimeEmitCtx &ctx,
                     const uint64_t column_id,
                     const ObWhiteFilterExecutor &filter,
                     const ObWhiteFilterOperatorType op,
                     const int64_t datum_idx,
                     bool &ok,
                     common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  const char *op_name = white_cmp_op_name(op);
  bool col_ok = false;
  bool lit_ok = false;
  common::ObSqlString col_json;
  common::ObSqlString lit_json;
  if (OB_ISNULL(op_name)) {
  } else if (OB_FAIL(emit_runtime_col(ctx, column_id, col_ok, col_json,
                                      &filter.get_col_obj_meta()))) {
  } else if (!col_ok) {
  } else if (OB_FAIL(emit_runtime_lit(filter, datum_idx, lit_ok, lit_json))) {
  } else if (!lit_ok) {
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"cmp\",\"%s\":\"%s\",\"%s\":[",
                                    OB_EXT_K_KIND, OB_EXT_K_OP, op_name,
                                    OB_EXT_K_CHILDREN))) {
  } else if (OB_FAIL(out.append(col_json.string()))) {
  } else if (OB_FAIL(out.append(","))) {
  } else if (OB_FAIL(out.append(lit_json.string()))) {
  } else if (OB_FAIL(out.append("]}"))) {
  } else {
    ok = true;
  }
  return ret;
}

int wrap_runtime_children(const char *kind,
                          const common::ObIArray<common::ObSqlString> &children,
                          common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  if (children.count() == 1) {
    ret = out.append(children.at(0).string());
  } else if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[",
                                    OB_EXT_K_KIND, kind, OB_EXT_K_CHILDREN))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children.count(); ++i) {
      if (i > 0 && OB_FAIL(out.append(","))) {}
      if (OB_SUCC(ret) && OB_FAIL(out.append(children.at(i).string()))) {}
    }
    if (OB_SUCC(ret)) {
      ret = out.append("]}");
    }
  }
  return ret;
}

int emit_runtime_white_filter(const RuntimeEmitCtx &ctx,
                              ObWhiteFilterExecutor &filter,
                              bool &ok,
                              common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  const common::ObIArray<uint64_t> &col_ids = filter.get_col_ids();
  const common::ObIArray<common::ObDatum> &datums = filter.get_datums();
  if (filter.is_filter_dynamic_node() || filter.is_semistruct_filter_node()
      || col_ids.count() != 1) {
    // Runtime/semistruct filters cannot be frozen into a reader created once.
  } else {
    const uint64_t column_id = col_ids.at(0);
    const ObWhiteFilterOperatorType op = filter.get_op_type();
    if (WHITE_OP_NU == op || WHITE_OP_NN == op) {
      bool col_ok = false;
      common::ObSqlString col_json;
      const char *kind = WHITE_OP_NU == op ? "is_null" : "is_not_null";
      if (OB_FAIL(emit_runtime_col(ctx, column_id, col_ok, col_json))) {
      } else if (!col_ok) {
      } else if (OB_FAIL(out.append_fmt("{\"%s\":\"%s\",\"%s\":[",
                                        OB_EXT_K_KIND, kind, OB_EXT_K_CHILDREN))) {
      } else if (OB_FAIL(out.append(col_json.string()))) {
      } else if (OB_FAIL(out.append("]}"))) {
      } else {
        ok = true;
      }
    } else if (WHITE_OP_IN == op) {
      bool col_ok = false;
      common::ObSqlString col_json;
      common::ObSEArray<common::ObSqlString, 8> lit_jsons;
      if (datums.empty()) {
        // Do not rely on plugins accepting an empty IN list.
      } else if (OB_FAIL(emit_runtime_col(ctx, column_id, col_ok, col_json,
                                          &filter.get_col_obj_meta()))) {
      } else if (!col_ok) {
      } else if (OB_FAIL(lit_jsons.reserve(datums.count()))) {
      } else {
        bool all_lits_ok = true;
        for (int64_t i = 0; OB_SUCC(ret) && all_lits_ok && i < datums.count(); ++i) {
          bool lit_ok = false;
          common::ObSqlString lit_json;
          if (OB_FAIL(emit_runtime_lit(filter, i, lit_ok, lit_json))) {
          } else if (!lit_ok) {
            all_lits_ok = false;
          } else if (OB_FAIL(lit_jsons.push_back(lit_json))) {
          }
        }
        if (OB_SUCC(ret) && all_lits_ok
            && OB_FAIL(out.append_fmt("{\"%s\":\"in\",\"%s\":[",
                                      OB_EXT_K_KIND, OB_EXT_K_CHILDREN))) {
        } else if (OB_SUCC(ret) && all_lits_ok
                   && OB_FAIL(out.append(col_json.string()))) {
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && all_lits_ok && i < lit_jsons.count(); ++i) {
            if (OB_FAIL(out.append(","))) {
            } else if (OB_FAIL(out.append(lit_jsons.at(i).string()))) {
            }
          }
          if (OB_SUCC(ret) && all_lits_ok && OB_FAIL(out.append("]}"))) {
          } else if (OB_SUCC(ret) && all_lits_ok) {
            ok = true;
          }
        }
      }
    } else if (WHITE_OP_BT == op && datums.count() == 2) {
      common::ObSEArray<common::ObSqlString, 2> children;
      bool lower_ok = false;
      bool upper_ok = false;
      common::ObSqlString lower;
      common::ObSqlString upper;
      if (OB_FAIL(emit_runtime_cmp(ctx, column_id, filter, WHITE_OP_GE, 0,
                                   lower_ok, lower))) {
      } else if (OB_FAIL(emit_runtime_cmp(ctx, column_id, filter, WHITE_OP_LE, 1,
                                          upper_ok, upper))) {
      } else if (!lower_ok || !upper_ok) {
      } else if (OB_FAIL(children.push_back(lower))) {
      } else if (OB_FAIL(children.push_back(upper))) {
      } else if (OB_FAIL(wrap_runtime_children("and", children, out))) {
      } else {
        ok = true;
      }
    } else if (datums.count() == 1) {
      ret = emit_runtime_cmp(ctx, column_id, filter, op, 0, ok, out);
    }
  }
  return ret;
}

int emit_runtime_node(const RuntimeEmitCtx &ctx,
                      ObPushdownFilterExecutor *filter,
                      bool &ok,
                      common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  ok = false;
  if (OB_ISNULL(filter)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (filter->is_logic_and_node() || filter->is_logic_or_node()) {
    const bool is_and = filter->is_logic_and_node();
    bool all_ok = true;
    common::ObSEArray<common::ObSqlString, 4> children;
    ObPushdownFilterExecutor **child_filters = filter->get_childs();
    for (uint32_t i = 0; OB_SUCC(ret) && (is_and || all_ok)
                         && i < filter->get_child_count(); ++i) {
      bool child_ok = false;
      common::ObSqlString child_json;
      if (OB_ISNULL(child_filters) || OB_ISNULL(child_filters[i])) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(emit_runtime_node(ctx, child_filters[i], child_ok, child_json))) {
        LOG_WARN("emit runtime predicate child failed", K(ret), K(i));
      } else if (!child_ok) {
        if (!is_and) {
          all_ok = false;  // Partial OR could incorrectly discard rows.
        }
      } else if (OB_FAIL(children.push_back(child_json))) {
      }
    }
    if (OB_SUCC(ret) && all_ok && !children.empty()) {
      if (OB_FAIL(wrap_runtime_children(is_and ? "and" : "or", children, out))) {
      } else {
        ok = true;
      }
    }
  } else if (filter->is_filter_white_node()) {
    ret = emit_runtime_white_filter(
        ctx, *static_cast<ObWhiteFilterExecutor *>(filter), ok, out);
  }
  // Black/sample/other nodes remain as OB-side residual filters.
  return ret;
}

int copy_out(common::ObIAllocator &alloc, const common::ObSqlString &s, common::ObString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (s.empty()) {
  } else {
    // The plugin ABI takes predicate JSON as `const char *` without a length.
    // Keep the ObString length exact, but make the backing buffer C-string safe.
    char *dst = static_cast<char *>(alloc.alloc(s.length() + 1));
    if (OB_ISNULL(dst)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc predicate json failed", K(ret), K(s.length()));
    } else {
      MEMCPY(dst, s.ptr(), s.length());
      dst[s.length()] = '\0';
      out.assign(dst, static_cast<int32_t>(s.length()));
    }
  }
  return ret;
}

const char *partition_range_type_name(const common::ObObjType type)
{
  switch (type) {
    case common::ObTinyIntType: return "TINYINT";
    case common::ObSmallIntType: return "SMALLINT";
    case common::ObInt32Type: return "INT";
    case common::ObIntType: return "BIGINT";
    case common::ObDateType: return "DATE";
    default: return nullptr;
  }
}

// Every emitted interval has at least one finite endpoint. NULL-containing and
// whole/false ranges stay with OB until the protocol has explicit representations
// for them. On unsupported input, `usable` becomes false and the whole column
// candidate is discarded; it must never become a partial planning proof.
int append_partition_interval(const common::ObNewRange &range,
                              const common::ObObjType column_type,
                              common::ObSqlString &out, bool &usable)
{
  int ret = OB_SUCCESS;
  usable = false;
  if (range.is_false_range() || range.is_whole_range()
      || range.get_start_key().get_obj_cnt() != 1
      || range.get_end_key().get_obj_cnt() != 1) {
  } else {
    const common::ObObj &lo = range.get_start_key().get_obj_ptr()[0];
    const common::ObObj &hi = range.get_end_key().get_obj_ptr()[0];
    const bool has_lo = !lo.is_min_value();
    const bool has_hi = !hi.is_max_value();
    if ((!has_lo && !has_hi) || lo.is_null() || hi.is_null()
        || (has_lo && lo.get_type() != column_type)
        || (has_hi && hi.get_type() != column_type)) {
    } else if (OB_FAIL(out.append("{"))) {
    } else if (has_lo && OB_FAIL(out.append_fmt("\"%s\":\"", OB_EXT_K_LOWER))) {
    } else if (has_lo && OB_FAIL(obj_to_lit_value(out, lo))) {
    } else if (has_lo && OB_FAIL(out.append_fmt("\",\"%s\":%s",
                                                 OB_EXT_K_LOWER_INCLUSIVE,
                                                 range.border_flag_.inclusive_start() ? "true" : "false"))) {
    } else if (has_hi && has_lo && OB_FAIL(out.append(","))) {
    } else if (has_hi && OB_FAIL(out.append_fmt("\"%s\":\"", OB_EXT_K_UPPER))) {
    } else if (has_hi && OB_FAIL(obj_to_lit_value(out, hi))) {
    } else if (has_hi && OB_FAIL(out.append_fmt("\",\"%s\":%s",
                                                 OB_EXT_K_UPPER_INCLUSIVE,
                                                 range.border_flag_.inclusive_end() ? "true" : "false"))) {
    } else if (OB_FAIL(out.append("}"))) {
    } else {
      usable = true;
    }
  }
  return ret;
}

} // namespace

int build_predicate_json_from_raw_expr(common::ObIAllocator &alloc,
                                       ObExecContext *exec_ctx,
                                       const common::ObIArray<ObRawExpr *> &filters,
                                       const common::ObIArray<uint64_t> &partition_col_ids,
                                       common::ObString &out_predicate_json,
                                       common::ObString &out_partition_filter_json,
                                       common::ObIArray<ObRawExpr *> &out_partition_filter_exprs)
{
  int ret = OB_SUCCESS;
  out_predicate_json.reset();
  out_partition_filter_json.reset();
  out_partition_filter_exprs.reset();
  EmitCtx ctx = {alloc, exec_ctx, partition_col_ids};
  common::ObSEArray<common::ObSqlString, 4> parts;
  common::ObSEArray<common::ObSqlString, 4> partition_parts;
  if (OB_FAIL(parts.reserve(filters.count()))) {
  } else if (OB_FAIL(partition_parts.reserve(filters.count()))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
      ObRawExpr *f = filters.at(i);
      bool ok = false;
      bool part_eligible = true;
      common::ObSqlString node;
      if (OB_FAIL(emit_node(ctx, f, part_eligible, ok, node))) {
        LOG_WARN("emit predicate node failed", K(ret), K(i), KPC(f));
      } else if (!ok) {
        // non-convertible — OB's own pipeline filters it.
      } else if (OB_FAIL(parts.push_back(node))) {
        LOG_WARN("push back predicate part failed", K(ret));
      } else if (part_eligible && !f->has_flag(CNT_DYNAMIC_PARAM)
                 && OB_FAIL(partition_parts.push_back(node))) {
        LOG_WARN("push back partition predicate part failed", K(ret));
      } else if (part_eligible && !f->has_flag(CNT_DYNAMIC_PARAM)
                 && OB_FAIL(out_partition_filter_exprs.push_back(f))) {
        LOG_WARN("push back partition predicate expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !parts.empty()) {
    common::ObSqlString assembled;
    if (OB_FAIL(wrap_and(parts, assembled))) {
      LOG_WARN("assemble predicate json failed", K(ret));
    } else if (OB_FAIL(copy_out(alloc, assembled, out_predicate_json))) {
      LOG_WARN("copy predicate json failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !partition_parts.empty()) {
    common::ObSqlString assembled;
    if (OB_FAIL(wrap_and(partition_parts, assembled))) {
      LOG_WARN("assemble partition predicate json failed", K(ret));
    } else if (OB_FAIL(copy_out(alloc, assembled, out_partition_filter_json))) {
      LOG_WARN("copy partition predicate json failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    out_predicate_json.reset();
    out_partition_filter_json.reset();
    out_partition_filter_exprs.reset();
  }
  return ret;
}

int build_partition_range_json(common::ObIAllocator &alloc,
                               const ObDMLStmt &stmt,
                               ObExecContext *exec_ctx,
                               uint64_t table_id,
                               const common::ObIArray<ObRawExpr *> &filters,
                               const common::ObIArray<uint64_t> &partition_col_ids,
                               common::ObString &out_json,
                               common::ObIArray<ObRawExpr *> &out_precise_exprs)
{
  int ret = OB_SUCCESS;
  out_json.reset();
  out_precise_exprs.reset();
  common::ObSqlString columns_json;
  int64_t emitted_columns = 0;
  if (exec_ctx == nullptr || exec_ctx->get_my_session() == nullptr) {
    ret = OB_INVALID_ARGUMENT;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_ids.count(); ++i) {
    const uint64_t col_id = partition_col_ids.at(i);
    ColumnItem *item = stmt.get_column_item_by_id(table_id, col_id);
    if (item == nullptr || item->expr_ == nullptr) { continue; }
    const common::ObObjType col_type = item->expr_->get_data_type();
    const char *type_name = partition_range_type_name(col_type);
    if (type_name == nullptr) { continue; }

    common::ObArenaAllocator tmp_alloc("ExtPreRange");
    common::ObSEArray<ColumnItem, 1> range_columns;
    common::ObSEArray<ObRawExpr *, 4> precise_exprs;
    common::ObSEArray<uint64_t, 1> one_col_id;
    if (OB_FAIL(range_columns.push_back(*item)) || OB_FAIL(one_col_id.push_back(col_id))) {
      LOG_WARN("failed to prepare partition range column", K(ret));
    } else {
      ObPreRangeGraph graph(tmp_alloc);
      if (OB_FAIL(graph.preliminary_extract_query_range(range_columns, filters, exec_ctx,
                                                       nullptr, nullptr, false, true))) {
        LOG_WARN("failed to extract plugin partition range", K(ret), K(col_id));
      } else {
        EmitCtx emit_ctx = {alloc, exec_ctx, one_col_id};
        const common::ObIArray<ObRawExpr *> &graph_exprs = graph.get_range_exprs();
        for (int64_t j = 0; OB_SUCC(ret) && j < graph_exprs.count(); ++j) {
          ObRawExpr *expr = graph_exprs.at(j);
          bool partition_eligible = true;
          bool ok = false;
          common::ObSqlString ignored;
          // Retain only original filter pointers. Graph-created expressions
          // live in tmp_alloc and cannot be used as optimizer proof handles.
          if (expr == nullptr || !ObOptimizerUtil::find_item(filters, expr)
              || expr->has_flag(CNT_DYNAMIC_PARAM)) {
          } else if (OB_FAIL(emit_node(emit_ctx, expr, partition_eligible, ok, ignored))) {
            LOG_WARN("failed to inspect exact partition expr", K(ret));
          } else if (ok && partition_eligible && OB_FAIL(precise_exprs.push_back(expr))) {
            LOG_WARN("failed to collect exact partition expr", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret) || precise_exprs.empty()) { continue; }

    ObPreRangeGraph exact_graph(tmp_alloc);
    ObQueryRangeArray ranges;
    bool all_single = false;
    const common::ObDataTypeCastParams dtc_params =
        ObBasicSessionInfo::create_dtc_params(exec_ctx->get_my_session());
    if (OB_FAIL(exact_graph.preliminary_extract_query_range(range_columns, precise_exprs,
                                                            exec_ctx, nullptr, nullptr,
                                                            false, true))) {
      LOG_WARN("failed to extract exact plugin partition ranges", K(ret));
    } else if (OB_FAIL(exact_graph.get_tablet_ranges(tmp_alloc, *exec_ctx, ranges,
                                                   all_single, dtc_params))) {
      LOG_WARN("failed to materialize exact plugin partition ranges", K(ret));
    }
    if (OB_FAIL(ret) || ranges.empty() || ranges.count() > 256) { continue; }
    // Only an exact range representation may prove a predicate redundant.
    // Recheck after rebuilding the graph: a later range conversion must not
    // silently turn one of the selected filters into an approximation.
    bool exact = exact_graph.get_unprecise_range_exprs().empty();
    const common::ObIArray<ObRawExpr *> &materialized_exprs = exact_graph.get_range_exprs();
    for (int64_t j = 0; exact && j < precise_exprs.count(); ++j) {
      exact = ObOptimizerUtil::find_item(materialized_exprs, precise_exprs.at(j));
    }
    if (!exact) { continue; }

    common::ObSqlString range_json;
    bool usable = true;
    if (OB_FAIL(range_json.append_fmt("{\"%s\":\"", OB_EXT_K_NAME))) {
    } else if (OB_FAIL(json_escape(range_json, item->expr_->get_column_name()))) {
    } else if (OB_FAIL(range_json.append_fmt("\",\"%s\":\"%s\",\"%s\":[",
                                              OB_EXT_K_EXT_TYPE, type_name, OB_EXT_K_RANGES))) {
    }
    for (int64_t j = 0; OB_SUCC(ret) && usable && j < ranges.count(); ++j) {
      bool interval_usable = false;
      if (j > 0 && OB_FAIL(range_json.append(","))) {
      } else if (ranges.at(j) == nullptr) {
        usable = false;
      } else if (OB_FAIL(append_partition_interval(*ranges.at(j), col_type,
                                                   range_json, interval_usable))) {
        LOG_WARN("failed to encode plugin partition interval", K(ret));
      } else if (!interval_usable) {
        usable = false;
      }
    }
    if (OB_SUCC(ret) && usable && OB_FAIL(range_json.append("]}"))) {
    }
    if (OB_SUCC(ret) && usable) {
      if (emitted_columns > 0 && OB_FAIL(columns_json.append(","))) {
      } else if (OB_FAIL(columns_json.append(range_json.ptr(), range_json.length()))) {
      } else {
        ++emitted_columns;
        for (int64_t j = 0; OB_SUCC(ret) && j < precise_exprs.count(); ++j) {
          ObRawExpr *expr = precise_exprs.at(j);
          if (!ObOptimizerUtil::find_item(out_precise_exprs, expr)
              && OB_FAIL(out_precise_exprs.push_back(expr))) {
            LOG_WARN("failed to retain exact partition expr", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && emitted_columns > 0) {
    common::ObSqlString doc;
    if (OB_FAIL(doc.append_fmt("{\"%s\":\"%s\",\"%s\":[",
                               OB_EXT_K_KIND, OB_EXT_KIND_RANGE_SET, OB_EXT_K_COLUMNS))) {
    } else if (OB_FAIL(doc.append(columns_json.ptr(), columns_json.length()))) {
    } else if (OB_FAIL(doc.append("]}"))) {
    } else if (OB_FAIL(copy_out(alloc, doc, out_json))) {
    }
  }
  if (OB_FAIL(ret)) {
    out_json.reset();
    out_precise_exprs.reset();
  }
  return ret;
}

int build_predicate_json_from_pushdown_filter(
    common::ObIAllocator &alloc,
    ObPushdownFilterExecutor *filter,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray<common::ObString> &column_names,
    common::ObString &out_predicate_json)
{
  int ret = OB_SUCCESS;
  out_predicate_json.reset();
  if (column_ids.count() != column_names.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("runtime predicate column mapping count mismatch",
             K(ret), K(column_ids.count()), K(column_names.count()));
  } else if (OB_NOT_NULL(filter)) {
    RuntimeEmitCtx ctx = {column_ids, column_names};
    bool ok = false;
    common::ObSqlString node;
    if (OB_FAIL(emit_runtime_node(ctx, filter, ok, node))) {
      LOG_WARN("emit runtime predicate failed", K(ret));
    } else if (ok && OB_FAIL(copy_out(alloc, node, out_predicate_json))) {
      LOG_WARN("copy runtime predicate json failed", K(ret));
    }
  }
  return ret;
}

} // namespace ext_predicate
} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX
