/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_search_index_query_range_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/ob_sql_utils.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "lib/json_type/ob_json_path.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_json_param_type.h"
#include "share/search_index/ob_search_index_config_filter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObSearchIndexQueryRangeUtils::build_column_idx_expr(ObQueryRangeCtx &ctx,
                                                        const ObColumnRefRawExpr &column_expr,
                                                        ObRawExpr *&column_idx_expr)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_search_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a search index", K(ret));
  } else if (OB_UNLIKELY(column_expr.get_column_id() != ctx.search_index_range_ctx_->column_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column id is not match", K(ret), K(column_expr.get_column_id()),
                                       K(*ctx.search_index_range_ctx_));
  } else {
    ObConstRawExpr *const_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx.expr_factory_,
                                                     ObIntType,
                                                     ctx.search_index_range_ctx_->column_idx_,
                                                     const_expr))) {
      LOG_WARN("failed to build const int expr for column idx", K(ret));
    } else if (OB_ISNULL(const_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column idx expr is null", K(ret));
    } else {
      column_idx_expr = const_expr;
    }
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::build_array_or_scalar_path_expr(ObQueryRangeCtx &ctx, ObRawExpr *&path_expr)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_search_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a search index", K(ret));
  } else {
    const ObString &array_type_path = ctx.search_index_range_ctx_->array_type_path_;
    if (!array_type_path.empty()) {
      // Array column: path in index is type string e.g. "ARRAY(INT)"
      ObConstRawExpr *path_const_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*ctx.expr_factory_, ObVarcharType,
                                                           array_type_path, CS_TYPE_BINARY,
                                                           path_const_expr))) {
        LOG_WARN("failed to build const string expr for array type path", K(ret));
      } else {
        path_expr = path_const_expr;
      }
    } else {
      ObRawExpr *null_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx.expr_factory_, null_expr))) {
        LOG_WARN("failed to build null expr for path", K(ret));
      } else {
        path_expr = null_expr;
      }
    }
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::build_json_single_path_expr(ObQueryRangeCtx &ctx,
                                                              const ObRawExpr &const_expr,
                                                              ObRawExpr *&path)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *path_prefix_expr = NULL;
  ObSysFunRawExpr *path_expr = NULL;
  if (!ctx.is_search_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a search index", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*ctx.expr_factory_,
                                                             ObVarcharType,
                                                             ctx.search_index_range_ctx_->path_prefix_,
                                                             CS_TYPE_BINARY,
                                                             path_prefix_expr))) {
    LOG_WARN("failed to build const string expr for path prefix", K(ret));
  } else if (OB_FAIL(ctx.expr_factory_->create_raw_expr(T_FUN_SYS_SEARCH_INDEX_INNER_PATH, path_expr))) {
    LOG_WARN("failed to create search index type expr", K(ret));
  } else if (OB_ISNULL(path_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path expr is null", K(ret));
  } else if (OB_FAIL(path_expr->set_param_exprs(path_prefix_expr, const_cast<ObRawExpr*>(&const_expr)))) {
    LOG_WARN("failed to set param expr for search index type", K(ret));
  } else if (OB_FAIL(path_expr->formalize(ctx.session_info_))) {
    LOG_WARN("failed to formalize search index type expr", K(ret));
  } else {
    path_expr->set_func_name(ObString::make_string("SEARCH_INDEX_INNER_PATH"));
    path_expr->set_pick(ctx.search_index_range_ctx_->pick_type_);
    path = path_expr;
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::build_json_range_path_expr(ObIAllocator &allocator,
                                                             ObQueryRangeCtx &ctx,
                                                             const ObRawExpr &const_expr,
                                                             ObItemType cmp_type,
                                                             ObRawExpr *&start_path,
                                                             ObRawExpr *&end_path)
{
  int ret = OB_SUCCESS;
  if (!ctx.is_search_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a search index", K(ret));
  } else if (cmp_type == T_OP_LE || cmp_type == T_OP_LT) {
    // "col < const" / "col <= const"
    // Build range: [static_lower, dynamic_upper(UPPER_BOUND)]
    //   - start_path (static): a fixed path that sits before the relevant type entries.
    //   - end_path   (dynamic): search_index_inner_path(const_expr) with UPPER_BOUND,
    //     computed at runtime by calc_pick_inner_path to determine the precise upper
    //     boundary based on const type vs pick type.
    ObString lower_path;
    ObConstRawExpr *lower_path_expr = nullptr;
    ObRawExpr *path_expr = nullptr;
    const ObString &path_prefix = ctx.search_index_range_ctx_->path_prefix_;
    if (ctx.search_index_range_ctx_->has_pick()) {
      // With pick: generate the pick-type path then subtract 1 from the last byte
      // to land in the gap just before pick-type entries.
      const ObItemType pick_type = ctx.search_index_range_ctx_->pick_type_;
      if (OB_FAIL(ObSearchIndexPathEncoder::generate_pick_path(allocator, path_prefix, pick_type,
                                                               lower_path))) {
        LOG_WARN("failed to generate lower path", K(ret));
      } else {
        lower_path.ptr()[lower_path.length() - 1] -= 1;
      }
    } else {
      // Without pick: generate a path before all type entries for this path prefix.
      if (OB_FAIL(ObSearchIndexPathEncoder::generate_lower_path(allocator, path_prefix, lower_path))) {
        LOG_WARN("failed to generate lower path", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*ctx.expr_factory_, ObVarcharType,
        lower_path, CS_TYPE_BINARY, lower_path_expr))) {
      LOG_WARN("failed to build const string expr for lower path", K(ret));
    } else if (OB_FAIL(build_json_single_path_expr(ctx, const_expr, path_expr))) {
      LOG_WARN("failed to build json single path expr", K(ret));
    } else {
      start_path = lower_path_expr;
      path_expr->set_bound_enc_type(ObSearchIndexPathEncoder::UPPER_BOUND);
      end_path = path_expr;
    }
  } else if (cmp_type == T_OP_GE || cmp_type == T_OP_GT) {
    // "col > const" / "col >= const"
    // Build range: [dynamic_lower(LOWER_BOUND), static_upper]
    //   - start_path (dynamic): search_index_inner_path(const_expr) with LOWER_BOUND,
    //     computed at runtime by calc_pick_inner_path to determine the precise lower
    //     boundary based on const type vs pick type.
    //   - end_path   (static): a fixed path that sits after the relevant type entries.
    ObString upper_path;
    ObConstRawExpr *upper_path_expr = nullptr;
    ObRawExpr *path_expr = nullptr;
    const ObString &path_prefix = ctx.search_index_range_ctx_->path_prefix_;
    if (ctx.search_index_range_ctx_->has_pick()) {
      // With pick: generate the pick-type path then add 1 to the last byte
      // to land in the gap just after pick-type entries.
      const ObItemType pick_type = ctx.search_index_range_ctx_->pick_type_;
      if (OB_FAIL(ObSearchIndexPathEncoder::generate_pick_path(allocator, path_prefix, pick_type,
                                                               upper_path))) {
        LOG_WARN("failed to generate upper path", K(ret));
      } else {
        upper_path.ptr()[upper_path.length() - 1] += 1;
      }
    } else {
      // Without pick: generate a path after all type entries for this path prefix.
      if (OB_FAIL(ObSearchIndexPathEncoder::generate_upper_path(allocator, path_prefix, upper_path))) {
        LOG_WARN("failed to generate upper path", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*ctx.expr_factory_, ObVarcharType,
        upper_path, CS_TYPE_BINARY, upper_path_expr))) {
      LOG_WARN("failed to build const string expr for upper path", K(ret));
    } else if (OB_FAIL(build_json_single_path_expr(ctx, const_expr, path_expr))) {
      LOG_WARN("failed to build json single path expr", K(ret));
    } else {
      path_expr->set_bound_enc_type(ObSearchIndexPathEncoder::LOWER_BOUND);
      start_path = path_expr;
      end_path = upper_path_expr;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid cmp type", K(ret), K(cmp_type));
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::build_value_expr(ObQueryRangeCtx &ctx,
                                                   const ObRawExpr &const_expr,
                                                   const ObObjType *cmp_type,
                                                   ObRawExpr *&value_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *value_func_expr = NULL;
  ObRawExpr *type_expr = NULL;
  if (!ctx.is_search_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a search index", K(ret));
  } else {
    // Create type expression: null if cmp_type is null, otherwise create a const int expr
    if (OB_ISNULL(cmp_type)) {
      if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx.expr_factory_, type_expr))) {
        LOG_WARN("failed to build null expr for type", K(ret));
      }
    } else {
      ObConstRawExpr *const_type_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx.expr_factory_,
                                                       ObIntType,
                                                       static_cast<int64_t>(*cmp_type),
                                                       const_type_expr))) {
        LOG_WARN("failed to build const int expr for type", K(ret), K(*cmp_type));
      } else {
        type_expr = const_type_expr;
      }
    }
    // Create T_FUN_SYS_SEARCH_INDEX_INNER_VALUE function expression
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ctx.expr_factory_->create_raw_expr(T_FUN_SYS_SEARCH_INDEX_INNER_VALUE, value_func_expr))) {
      LOG_WARN("failed to create search index inner value expr", K(ret));
    } else if (OB_ISNULL(value_func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value func expr is null", K(ret));
    } else if (OB_FAIL(value_func_expr->set_param_exprs(const_cast<ObRawExpr*>(&const_expr), type_expr))) {
      LOG_WARN("failed to set param expr for search index inner value", K(ret));
    } else if (OB_FAIL(value_func_expr->formalize(ctx.session_info_))) {
      LOG_WARN("failed to formalize search index inner value expr", K(ret));
    } else {
      value_func_expr->set_func_name(ObString::make_string("SEARCH_INDEX_INNER_VALUE"));
      value_expr = value_func_expr;
    }
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::json_value_can_extract_range(const ObRawExpr *origin_expr,
                                                               const ObRawExpr *json_expr,
                                                               bool &can_extract)
{
  int ret = OB_SUCCESS;
  can_extract = false;
  if (OB_ISNULL(json_expr) || OB_ISNULL(origin_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), K(json_expr), K(origin_expr));
  } else if (json_expr->get_expr_type() != T_FUN_SYS_JSON_VALUE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json expr is not json value", K(ret), K(json_expr->get_expr_type()));
  } else if (json_expr->get_pick() != T_NULL) {
    // For json value with pick, can extract range only when
    // returning type and other options (truncate, ascii, empty, error) are all default.
    // 1. Check returning type matches the resolver override for pick:
    //    varchar + CS_TYPE_UTF8MB4_BIN + length >= OB_MAX_USER_ROW_KEY_LENGTH.
    //    A shorter length (e.g. user-specified RETURNING VARCHAR(100)) could
    //    truncate key values, producing incorrect range boundaries.
    const ObExprResType &res_type = json_expr->get_result_type();
    if (res_type.get_type() != ObVarcharType
        || res_type.get_collation_type() != CS_TYPE_UTF8MB4_BIN
        || res_type.get_length() < OB_MAX_USER_ROW_KEY_LENGTH) {
      can_extract = false;
    } else if (json_expr->get_param_count() <= JSN_VAL_MISMATCH) {
      can_extract = false;
    } else {
      // 2. Check other options are default:
      //    truncate = 0, ascii = 0, empty_type = 3 (NULL ON EMPTY), error_type = 3 (NULL ON ERROR)
      bool is_all_default = true;
      const int64_t default_checks[][2] = {
        {JSN_VAL_TRUNC, 0},   // truncate: default 0
        {JSN_VAL_ASCII, 0},   // ascii: default 0
        {JSN_VAL_EMPTY, 3},   // empty type: default 3 (NULL ON EMPTY)
        {JSN_VAL_ERROR, 3},   // error type: default 3 (NULL ON ERROR)
      };
      for (int64_t i = 0; is_all_default && i < ARRAYSIZEOF(default_checks); ++i) {
        const ObRawExpr *param = json_expr->get_param_expr(default_checks[i][0]);
        if (OB_ISNULL(param) || !param->is_const_raw_expr()) {
          is_all_default = false;
        } else {
          const ObObj &val = static_cast<const ObConstRawExpr*>(param)->get_value();
          if (!val.is_int() || val.get_int() != default_checks[i][1]) {
            is_all_default = false;
          }
        }
      }
      can_extract = is_all_default;
    }
    if (OB_SUCC(ret) && can_extract) {
      if (json_expr->get_pick() == T_JSON_NUMBER) {
        can_extract = ob_is_number_or_decimal_int_tc(origin_expr->get_result_type().get_type());
      } else if (json_expr->get_pick() == T_JSON_STRING) {
        // no additional cast exprs in origin expr, can extract range
        can_extract = origin_expr == json_expr;
      }
    }
  }
  return ret;
}

namespace {
// Returns matched=true when the sequence of OBJECT member keys currently accumulated in
// path_items (array markers ignored) exactly equals the segments of one of the query's
// enclosing nested() clause paths. Each enclosing path is an internal "$." path such as
// "$.comments.replies". This drives ARRAY-marker placement from the QUERY's explicit nesting
// rather than the index NESTED_PATHS declarations.
int cumulative_path_is_enclosing(
    ObIAllocator &allocator,
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    const ObIArray<ObString> &enclosing_nested_paths,
    bool &matched)
{
  int ret = OB_SUCCESS;
  matched = false;
  ObSEArray<ObString, 20> members;
  for (int64_t i = 0; OB_SUCC(ret) && i < path_items.count(); ++i) {
    if (!path_items.at(i).is_array_path()
        && OB_FAIL(members.push_back(path_items.at(i).path))) {
      LOG_WARN("failed to collect member key", K(ret), K(i));
    }
  }
  for (int64_t e = 0; OB_SUCC(ret) && !matched && e < enclosing_nested_paths.count(); ++e) {
    const ObString &enc = enclosing_nested_paths.at(e);
    // Parse the enclosing "$." path with ObJsonPath and compare each member node's raw key
    // against the query-side keys in path_items (which are also raw object names, matching the
    // keys stored in the index and in the config-filter trie). Splitting on raw '.' would
    // mis-segment keys that embed a dot such as $.a."b.c"; parsing handles the escaping.
    ObJsonPath enc_path(enc, &allocator);
    int64_t seg_i = 0;
    bool ok = true;
    if (OB_FAIL(enc_path.parse_path())) {
      // Enclosing paths are internally generated and already validated, so they must be legal
      // json paths; a parse failure is an unexpected internal error and should propagate.
      LOG_WARN("failed to parse enclosing nested path", K(ret), K(enc));
    }
    for (JsonPathIterator it = enc_path.begin();
         OB_SUCC(ret) && ok && it != enc_path.end(); ++it) {
      ObJsonPathNode *node = *it;
      if (OB_ISNULL(node)) {
        ok = false;
      } else if (node->get_node_type() == JPN_ROOT) {
        continue;
      } else if (node->get_node_type() == JPN_MEMBER) {
        ObJsonPathBasicNode *basic_node = static_cast<ObJsonPathBasicNode *>(node);
        ObString seg(basic_node->get_object().len_, basic_node->get_object().object_name_);
        if (seg.empty() || seg_i >= members.count() || 0 != seg.compare(members.at(seg_i))) {
          ok = false;
        } else {
          ++seg_i;
        }
      } else {
        // A non-member node cannot appear in an enclosing member path.
        ok = false;
      }
    }
    if (OB_SUCC(ret) && ok && seg_i > 0 && seg_i == members.count()) {
      matched = true;
    }
  }
  return ret;
}
}  // anonymous namespace

int ObSearchIndexQueryRangeUtils::json_prefix_path_encode(ObIAllocator &allocator,
                                                          ObQueryRangeCtx &ctx,
                                                          const ObRawExpr &path_expr,
                                                          ObString &path_prefix,
                                                          const bool index_sub_path,
                                                          bool &can_extract_range)
{
  int ret = OB_SUCCESS;
  can_extract_range = false;
  const share::ObSearchIndexConfigFilter *json_filter = nullptr;
  ObItemType pick_type = T_NULL;
  const ObIArray<ObString> *enclosing_nested_paths = nullptr;
  if (ctx.is_search_index() && OB_NOT_NULL(ctx.search_index_range_ctx_)) {
    json_filter = ctx.search_index_range_ctx_->get_json_filter();
    pick_type = ctx.search_index_range_ctx_->get_pick_type();
    enclosing_nested_paths = &ctx.search_index_range_ctx_->get_enclosing_nested_paths();
  }
  if (!path_expr.is_static_const_expr()) {
    can_extract_range = false;
  } else {
    ObObj path_val;
    bool is_valid = false;
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx.exec_ctx_,
                                                          &path_expr,
                                                          path_val,
                                                          is_valid,
                                                          allocator,
                                                          ctx.ignore_calc_failure_,
                                                          ctx.expr_constraints_))) {
      LOG_WARN("failed to calc const or calculable expr", K(ret));
    } else if (!is_valid) {
      can_extract_range = false;
    } else if (path_val.is_null() || !path_val.is_string_type()) {
      can_extract_range = false;
    } else {
      ObArenaAllocator tmp_allocator(ObModIds::JSON_PARSER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObString path_str = path_val.get_string();
      // Validate JSON path using ObJsonPath
      ObJsonPath json_path(path_str, &tmp_allocator);
      if (OB_FAIL(json_path.parse_path())) {
        LOG_WARN("invalid JSON path expression", K(ret), K(path_str));
        ret = OB_SUCCESS;
        can_extract_range = false;
      } else {
        // Check that all path nodes are JPN_MEMBER (only member access allowed, no array access)
        // Only paths like $.a.b.c are allowed, reject array access like $.a[0] or $.a[*]
        bool only_member_nodes = true;
        // Set when this candidate index does not declare a NESTED_PATH covering a nested()
        // boundary the query crosses; such an index cannot answer the nested query correctly.
        bool nested_index_ineligible = false;
        ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 16> path_items;
        for (JsonPathIterator it = json_path.begin(); it != json_path.end() && only_member_nodes && OB_SUCC(ret); ++it) {
          ObJsonPathNode *node = *it;
          if (OB_ISNULL(node)) {
            only_member_nodes = false;
          } else {
            ObJsonPathNodeType node_type = node->get_node_type();
            // Only allow JPN_ROOT and JPN_MEMBER, reject all array-related types
            // JPN_ROOT is the first node ($), JPN_MEMBER is member access (.key)
            // Reject: JPN_ARRAY_CELL, JPN_ARRAY_RANGE, JPN_ARRAY_CELL_WILDCARD, JPN_MULTIPLE_ARRAY
            if (node_type == JPN_ROOT) {
              continue;
            } else if (node_type == JPN_MEMBER) {
              // Use the raw object name (not node_to_string, which double-quotes keys that are not
              // ECMAScript identifiers). The index stores raw JSON member names and the config
              // filter's NESTED_PATHS trie is keyed by raw names too, so a quoted key like
              // "comments.v2" would never match the stored raw key comments.v2.
              if (OB_SUCC(ret)) {
                ObJsonPathBasicNode *basic_node = static_cast<ObJsonPathBasicNode *>(node);
                ObString member_key(basic_node->get_object().len_, basic_node->get_object().object_name_);
                if (OB_FAIL(path_items.push_back(ObSearchIndexPathEncoder::make_object_path(member_key)))) {
                  LOG_WARN("failed to push path item", K(ret));
                } else if (OB_NOT_NULL(enclosing_nested_paths) && !enclosing_nested_paths->empty()) {
                  // Insert an ARRAY marker only when the cumulative member path equals one of the
                  // query's explicitly-opened nested() clause paths. Array-vs-object shape thus
                  // follows the QUERY's nesting, not the index NESTED_PATHS declarations: a single
                  // nested(path="$.a.b") treats `a` as an object and only `b` as the nested array,
                  // whereas nested(a) > nested(a.b) marks both layers. A plain (non-nested)
                  // predicate has no enclosing paths and never inserts a marker, so it keeps pure
                  // object-path MySQL json_extract semantics.
                  bool is_boundary = false;
                  if (OB_FAIL(cumulative_path_is_enclosing(tmp_allocator, path_items,
                                                           *enclosing_nested_paths, is_boundary))) {
                    LOG_WARN("failed to check enclosing nested boundary", K(ret));
                  } else if (is_boundary) {
                    // The candidate index must declare this boundary as a NESTED_PATH.  An index
                    // that only lists the path under INCLUDE_PATHS stores array-descendant scalars
                    // without nested-key correlation and would answer the nested() query
                    // incorrectly.  Mark such an index ineligible so the optimizer picks an index
                    // whose NESTED_PATHS cover the query's nesting.
                    bool declared_nested = false;
                    if (OB_NOT_NULL(json_filter)
                        && OB_FAIL(json_filter->is_nested_path(path_items, declared_nested))) {
                      LOG_WARN("failed to check declared nested path", K(ret), K(path_items));
                    } else if (OB_NOT_NULL(json_filter) && !declared_nested) {
                      nested_index_ineligible = true;
                      break;
                    } else if (OB_FAIL(path_items.push_back(ObSearchIndexPathEncoder::make_array_path()))) {
                      LOG_WARN("failed to push array path item", K(ret));
                    } else {
                      LOG_TRACE("[NESTED-DBG] search index nested query path items", K(path_items));
                    }
                  }
                }
              }
            } else {
              only_member_nodes = false;
              LOG_DEBUG("JSON path contains non-member node, cannot extract range",
                       K(path_str), K(node_type));
            }
          }
        }
        // Check path filter if json_filter is configured
        bool filter_passed = true;
        if (OB_SUCC(ret) && only_member_nodes && !nested_index_ineligible && OB_NOT_NULL(json_filter)) {
          if (OB_FAIL(json_filter->is_indexed(path_items, pick_type, filter_passed, index_sub_path))) {
            LOG_WARN("failed to check path filter", K(ret));
          }
        }
        can_extract_range = only_member_nodes && filter_passed && !nested_index_ineligible;
        if (OB_SUCC(ret) && can_extract_range) {
          if (path_items.empty()) {
            // empty path, such as $
            path_prefix.reset();
          } else if (OB_FAIL(ObSearchIndexPathEncoder::encode_path_prefix(allocator, path_items,
                                                                          path_prefix))) {
            LOG_WARN("failed to encode path prefix", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::is_json_scalar_match_index(ObExecContext &exec_ctx,
                                                             const ObObj &json_value,
                                                             const uint8_t cons_encode_type,
                                                             bool &is_match)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::JSON_PARSER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObIJsonBase *j_base = nullptr;
  is_match = false;
  if (OB_FAIL(ObJsonExprHelper::refine_range_json_value_const(json_value,
                                                              &exec_ctx,
                                                              false,
                                                              &allocator,
                                                              j_base))) {
    LOG_WARN("failed to refine range json value const", K(ret));
  } else if (OB_FAIL(ObSearchIndexConstraint::is_json_scalar_match(j_base, cons_encode_type, is_match))) {
    LOG_WARN("failed to check json scalar match", K(ret));
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::is_json_scalar_or_array_match_index(ObExecContext &exec_ctx,
                                                                      const ObObj &json_value,
                                                                      const uint8_t cons_encode_type,
                                                                      bool &is_match)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::JSON_PARSER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObIJsonBase *j_base = nullptr;
  is_match = false;
  if (OB_FAIL(ObJsonExprHelper::refine_range_json_value_const(json_value,
                                                              &exec_ctx,
                                                              false,
                                                              &allocator,
                                                              j_base))) {
    LOG_WARN("failed to refine range json value const", K(ret));
  } else if (OB_FAIL(ObSearchIndexConstraint::is_json_scalar_or_array_match(j_base, cons_encode_type, is_match))) {
    LOG_WARN("failed to check json array match", K(ret));
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::add_json_scalar_constraint(ObQueryRangeCtx &ctx,
                                                             const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  PreCalcExprExpectResult expect_result = PRE_CALC_SEARCH_INDEX_CONSTRAINT;
  const uint8_t cons_encode_type =
    ctx.search_index_range_ctx_ ? ctx.search_index_range_ctx_->get_cons_encode_type() : 0;
  int64_t extra = ObSearchIndexConstraint::make_extra(ObSearchIndexConstraint::JSON_TYPE_SCALAR,
                                                      cons_encode_type, 0);
  ObConstraintExtra cons_extra;
  cons_extra.extra_ = extra;
  ObExprConstraint cons(const_cast<ObRawExpr*>(expr), expect_result, cons_extra);
  if (NULL == ctx.expr_constraints_) {
    // do nothing
  } else if (OB_FAIL(add_var_to_array_no_dup(*ctx.expr_constraints_, cons))) {
    LOG_WARN("failed to add precise constraint", K(ret));
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::add_json_scalar_or_array_constraint(ObQueryRangeCtx &ctx,
                                                                      const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  PreCalcExprExpectResult expect_result = PRE_CALC_SEARCH_INDEX_CONSTRAINT;
  const uint8_t cons_encode_type =
    ctx.search_index_range_ctx_ ? ctx.search_index_range_ctx_->get_cons_encode_type() : 0;
  int64_t extra = ObSearchIndexConstraint::make_extra(
    ObSearchIndexConstraint::JSON_TYPE_SCALAR_OR_ARRAY, cons_encode_type, 0);
  ObConstraintExtra cons_extra;
  cons_extra.extra_ = extra;
  ObExprConstraint cons(const_cast<ObRawExpr*>(expr), expect_result, cons_extra);
  if (NULL == ctx.expr_constraints_) {
    // do nothing
  } else if (OB_FAIL(add_var_to_array_no_dup(*ctx.expr_constraints_, cons))) {
    LOG_WARN("failed to add precise constraint", K(ret));
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::add_array_string_length_constraint(ObQueryRangeCtx &ctx,
                                                                     const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  PreCalcExprExpectResult expect_result = PRE_CALC_SEARCH_INDEX_CONSTRAINT;
  int64_t extra = ObSearchIndexConstraint::make_extra(ObSearchIndexConstraint::ARRAY_STRING_LENGTH, 0, 0);
  ObConstraintExtra cons_extra;
  cons_extra.extra_ = extra;
  ObExprConstraint cons(const_cast<ObRawExpr*>(expr), expect_result, cons_extra);
  if (NULL == ctx.expr_constraints_) {
    // do nothing
  } else if (OB_ISNULL(expr) || !ob_is_collection_sql_type(expr->get_result_type().get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null or not collection sql type", K(ret), KP(expr));
  } else if (OB_FAIL(add_var_to_array_no_dup(*ctx.expr_constraints_, cons))) {
    LOG_WARN("failed to add array string length constraint", K(ret));
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::add_string_type_length_constraint(ObQueryRangeCtx &ctx,
                                                                   const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  PreCalcExprExpectResult expect_result = PRE_CALC_SEARCH_INDEX_CONSTRAINT;
  int64_t extra = ObSearchIndexConstraint::make_extra(ObSearchIndexConstraint::STRING_TYPE_LENGTH, 0, 0);
  ObConstraintExtra cons_extra;
  cons_extra.extra_ = extra;
  ObExprConstraint cons(const_cast<ObRawExpr*>(expr), expect_result, cons_extra);
  if (NULL == ctx.expr_constraints_) {
    // do nothing
  } else if (OB_ISNULL(expr) || !ob_is_string_type(expr->get_result_type().get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null or not string type", K(ret), KP(expr));
  } else if (OB_FAIL(add_var_to_array_no_dup(*ctx.expr_constraints_, cons))) {
    LOG_WARN("failed to add string type length constraint", K(ret));
  }
  return ret;
}

int ObSearchIndexQueryRangeUtils::is_string_length_match_index(const ObObj &str_value,
                                                              bool &is_match)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR_CALC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (!str_value.is_string_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not string type", K(ret));
  } else if (OB_FAIL(ObSearchIndexConstraint::is_string_length_match(allocator, str_value, is_match))) {
    LOG_WARN("fail to check string length match", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
