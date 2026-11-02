/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_search.h"
#include "share/ob_json_access_utils.h"
#include "ob_expr_json_func_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/json_type/ob_json_wrapper.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

// cmp for ObJsonBaseSortedVector
struct ObJsonBufferPathCmp {
  bool operator()(const ObJsonBuffer* a, const ObJsonBuffer* b) {
    return (a->string().compare(b->string()) < 0);
  }
};

// Keep wildcard patterns intact. Otherwise prepare the literal for direct comparison.
static int prepare_search_pattern(ObIAllocator &allocator, int32_t escape_wc,
                                 ObString &target, bool &need_wildcmp)
{
  int ret = OB_SUCCESS;
  need_wildcmp = false;
  const ObCharsetInfo *cs = ObCharset::get_charset(CS_TYPE_UTF8MB4_BIN);
  const char *data = target.ptr();
  const int64_t length = target.length();
  int64_t first_escape = -1;
  for (int64_t pos = 0; !need_wildcmp && pos < length;) {
    // Match ob_wildcmp_mb_bin: check the escape byte before wildcard handling,
    // then consume a complete character. A trailing escape is a literal itself.
    if (data[pos] == escape_wc) {
      if (pos + 1 < length) {
        if (first_escape < 0) {
          first_escape = pos;
        }
        ++pos;
      }
    } else if (data[pos] == '%' || data[pos] == '_') {
      need_wildcmp = true;
    }
    pos += static_cast<unsigned char>(data[pos]) < 0x80
        ? 1 : max(1U, ob_ismbchar(cs, data + pos, data + length));
  }
  if (!need_wildcmp && first_escape >= 0) {
    char *literal = static_cast<char *>(allocator.alloc(length));
    if (OB_ISNULL(literal)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate literal search pattern failed", K(ret), K(length));
    } else {
      MEMCPY(literal, data, first_escape);
      int64_t literal_length = first_escape;
      for (int64_t pos = first_escape; pos < length;) {
        if (data[pos] == escape_wc && pos + 1 < length) {
          ++pos;
        }
        const int64_t char_length = static_cast<unsigned char>(data[pos]) < 0x80
            ? 1 : max(1U, ob_ismbchar(cs, data + pos, data + length));
        MEMCPY(literal + literal_length, data + pos, char_length);
        literal_length += char_length;
        pos += char_length;
      }
      target.assign_ptr(literal, literal_length);
    }
  }
  return ret;
}

struct CacheSearchArgs
{
  CacheSearchArgs(int32_t escape_wc, const ObString &target, bool need_wildcmp, bool one_flag)
      : escape_wc_(escape_wc), target_(target), need_wildcmp_(need_wildcmp), one_flag_(one_flag)
  {}

  const int32_t escape_wc_;
  const ObString target_;
  const bool need_wildcmp_;
  const bool one_flag_;
};

class ObJsonSearchCtx : public ObExprOperatorCtx
{
public:
  explicit ObJsonSearchCtx(ObIAllocator &allocator)
      : search_args_(NULL), path_cache_(&allocator)
  {}

  CacheSearchArgs *search_args_;
  ObJsonPathCache path_cache_;
};

// cmp for ObJsonBaseSortedVector
struct ObJsonBufferPathUnique {
  bool operator()(const ObJsonBuffer* a, const ObJsonBuffer* b){
    return (a->string().compare(b->string()) == 0);
  }
};

int ObExprJsonSearch::add_path_unique(const ObJsonBuffer* path,
                                      ObJsonBufferSortedVector &duplicates,
                                      ObJsonBufferVector &hits)
{
  INIT_SUCC(ret);
  ObJsonBufferPathCmp path_cmp;
  ObJsonBufferPathUnique path_unique;
  ObJsonBufferSortedVector::iterator insert_pos = duplicates.end();

  if ((OB_SUCC(duplicates.insert_unique(path, insert_pos, path_cmp, path_unique)))) {
    if (OB_FAIL(hits.push_back(path))) {
      LOG_WARN("fail to push_back path into result", K(ret));
    }
  } else if (ret == OB_CONFLICT_VALUE) { // value is already insert, set ret to success
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObExprJsonSearch::find_matches(common::ObIAllocator *allocator,
                                   const ObJsonWrapper &wrapper,
                                   ObJsonBuffer &path,
                                   ObJsonBufferVector &hits,
                                   ObJsonBufferSortedVector &duplicates,
                                   const ObString &target,
                                   bool one_match,
                                   const int32_t &escape_wc,
                                   bool need_wildcmp)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null", K(ret));
  } else if (one_match && hits.size() > 0) {
    // do nothing
  } else {
    switch (wrapper.json_type()) {
      case ObJsonNodeType::J_STRING: {
        ObString curr_str = wrapper.get_string();
        bool b = false;
        if (need_wildcmp) {
          b = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, curr_str, target, escape_wc,
                                static_cast<int32_t>('_'), static_cast<int32_t>('%'));
        } else {
          b = curr_str == target;
        }
        if (b) {
          void *buf = allocator->alloc(sizeof(ObJsonBuffer));
          if (OB_ISNULL(buf)){
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("falied to alloc new path buffer.", K(ret));
          } else {
            ObJsonBuffer *temp = new (buf) ObJsonBuffer(allocator);
            if (OB_FAIL(temp->append(path.ptr(), path.length()))) {
              LOG_WARN("failed to append curr path.", K(ret));
            } else if (OB_FAIL(add_path_unique(temp, duplicates, hits))) {
              LOG_WARN("failed to add path to duplicates.", K(ret));
            }
          }
        }
        break;
      }

      case ObJsonNodeType::J_OBJECT: {
        uint64_t pos = path.length();
        uint64_t count = wrapper.element_count();
        bool is_finish = false;
        for (uint64_t i = 0; OB_SUCC(ret) && i < count && !is_finish; i++) {
          if (one_match && hits.size() > 0) {
            is_finish = true;
          } else {
            ObString key;
            ObJsonWrapper child;
            if (OB_FAIL(wrapper.get_key_value(i, key, child))) {
              LOG_WARN("fail to get key_value", K(ret), K(i));
            } else if (OB_FAIL(ObJsonPathUtil::append_member_key(path, key))) {
              LOG_WARN("fail to append key to path",K(ret), K(i));
            } else if (OB_FAIL(find_matches(allocator, child, path, hits, duplicates,
                                            target, one_match, escape_wc, need_wildcmp))) {
              LOG_WARN("fail to seek recursively",K(ret), K(i));
            }
            if (OB_SUCC(ret) && OB_FAIL(path.set_length(pos))) { // reset path pos
              LOG_WARN("failed to reset path length.", K(ret), K(path.capacity()), K(pos));
            }
          }
        }
        break;
      }

      case ObJsonNodeType::J_ARRAY: {
        uint64_t pos = path.length();
        uint64_t size = wrapper.element_count();
        bool is_finish = false;
        for (uint64_t i = 0; OB_SUCC(ret) && i < size && !is_finish; i++) {
          if (one_match && hits.size() > 0) {
            is_finish = true;
          } else {
            ObJsonWrapper child;
            if (OB_FAIL(wrapper.element(i, child))) {
              LOG_WARN("fail to get element", K(ret), K(i));
            } else if (OB_FAIL(ObJsonPathUtil::append_bracketed_index(path, i))) {
              LOG_WARN("fail to append array index to path", K(ret), K(i));
            } else if (OB_FAIL(find_matches(allocator, child, path, hits, duplicates,
                target, one_match, escape_wc, need_wildcmp))) {
              LOG_WARN("fail to seek recursively",K(ret), K(i));
            }
            if (OB_SUCC(ret) && OB_FAIL(path.set_length(pos))) { // reset path pos
              LOG_WARN("failed to reset path length.", K(ret), K(path.capacity()), K(pos));
            }
          }
        }
        break; 
      }

      default: {
        // do nothing
        break;
      }
    }
  }

  return ret;
}

ObExprJsonSearch::ObExprJsonSearch(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_SEARCH, N_JSON_SEARCH, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonSearch::~ObExprJsonSearch()
{
}

int ObExprJsonSearch::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx); 
  INIT_SUCC(ret);

  if (OB_UNLIKELY(param_num < 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else {
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    
    // json doc
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, "json_search"))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
    }

    // [one_or_all][target_str][escape][path_string...]
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
      if (types_stack[i].get_type() == ObNullType) {
      } else if (ob_is_string_type(types_stack[i].get_type())) {
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else {
        types_stack[i].set_calc_type(ObLongTextType);
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
  }

  return ret;
}

int ObExprJsonSearch::eval_json_search(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[0];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObJsonWrapper wrapper;
  bool is_null = false;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, ret, ctx, "json_search");
  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc_wrapper(expr, ctx, temp_allocator,
                                                            0, wrapper, is_null, false, false))) {
    LOG_WARN("failed to get json doc wrapper.", K(ret));
  }

  // Only static constants are stable across rescans; dynamic parameters may change.
  bool can_cache_search_args = expr.args_[1]->is_static_const_expr()
      && expr.args_[2]->is_static_const_expr()
      && (expr.arg_cnt_ < 4 || expr.args_[3]->is_static_const_expr());
  const CacheSearchArgs *search_args = NULL;
  bool one_flag = false;
  int32_t escape_wc = static_cast<int32_t>('\\');
  ObString target;
  bool need_wildcmp = true;
  ObJsonSearchCtx *search_ctx = NULL;
  if (OB_SUCC(ret) && !is_null && expr.expr_ctx_id_ != ObExpr::INVALID_EXP_CTX_ID) {
    search_ctx = static_cast<ObJsonSearchCtx *>(ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_));
    if (can_cache_search_args || expr.arg_cnt_ > 4) {
      if (OB_ISNULL(search_ctx)) {
        void *buf = NULL;
        if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr.expr_ctx_id_, sizeof(ObJsonSearchCtx), buf))) {
          LOG_WARN("create json search context failed", K(ret));
        } else {
          search_ctx = new (buf) ObJsonSearchCtx(ctx.exec_ctx_.get_allocator());
        }
      } else {
        search_args = search_ctx->search_args_;
        if (OB_NOT_NULL(search_args)) {
          one_flag = search_args != NULL ? search_args->one_flag_ : false;
          escape_wc = search_args != NULL ? search_args->escape_wc_ : static_cast<int32_t>('\\');
          target = search_args != NULL ? search_args->target_ : ObString();
          need_wildcmp = search_args != NULL ? search_args->need_wildcmp_ : true;
        }
      }
    }
  }

  // check one_or_all flag
  if (OB_SUCC(ret) && !is_null && OB_ISNULL(search_args)) {
    json_arg = expr.args_[1];
    val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(temp_allocator.eval_arg(json_arg, ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
      is_null = true;
    } else if (!ob_is_string_type(val_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type));
    } else {
      ObString target_str = json_datum->get_string();
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, target_str, is_null))) {
        LOG_WARN("fail to get real data.", K(ret), K(target_str));
      } else if (json_arg->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN && 
        OB_FAIL(ObJsonExprHelper::convert_string_collation_type(json_arg->datum_meta_.cs_type_,
                                                                CS_TYPE_UTF8MB4_BIN, 
                                                                &temp_allocator,
                                                                target_str,
                                                                target_str))) {
        LOG_WARN("fail to convert string", K(ret));
      } else if (0 == target_str.case_compare("one")) {
        one_flag = true;
      } else if (0 == target_str.case_compare("all")) {
        one_flag = false;
      } else {
        ret = OB_ERR_JSON_BAD_ONE_OR_ALL_ARG;
        LOG_USER_ERROR(OB_ERR_JSON_BAD_ONE_OR_ALL_ARG);
      }
    }
  }

  // check escape if exist
  if (OB_SUCC(ret) && expr.arg_cnt_ >= 4 && !is_null && OB_ISNULL(search_args)) {
    json_arg = expr.args_[3];
    val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(temp_allocator.eval_arg(json_arg, ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
      // NULL uses the default escape.
    } else if (!ob_is_string_type(val_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type));
    } else {
      ObString escape = json_datum->get_string();
      bool is_null_str = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, escape, is_null_str))) {
        LOG_WARN("fail to get real data.", K(ret), K(escape));
      } else if (json_arg->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN && 
        OB_FAIL(ObJsonExprHelper::convert_string_collation_type(json_arg->datum_meta_.cs_type_,
                                                                CS_TYPE_UTF8MB4_BIN, 
                                                                &temp_allocator,
                                                                escape,
                                                                escape))) {
        LOG_WARN("fail to convert string", K(ret));
      } else if (escape.length() > 0) {
        const ObCollationType escape_coll = json_arg->datum_meta_.cs_type_;
        size_t length = ObCharset::strlen_char(escape_coll, escape.ptr(), escape.length());
        if (length != 1) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument to ESCAPE", K(escape), K(length), K(ret));
        } else if (OB_FAIL(ObCharset::mb_wc(escape_coll, escape, escape_wc))) {
          LOG_WARN("failed to convert escape to wc", K(ret), K(escape),
                  K(escape_coll), K(escape_wc));
          ret = OB_INVALID_ARGUMENT;
        }
      }
    }
  }

  // get target string
  if (OB_SUCC(ret) && !is_null && OB_ISNULL(search_args)) {
    json_arg = expr.args_[2];
    val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(temp_allocator.eval_arg(json_arg, ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
      is_null = true;
    } else if (!ob_is_string_type(val_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input type error", K(val_type));
    } else if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, target, is_null))) {
      LOG_WARN("fail to get real data.", K(ret), K(target));
    } else if (json_arg->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN && 
      OB_FAIL(ObJsonExprHelper::convert_string_collation_type(json_arg->datum_meta_.cs_type_,
                                                              CS_TYPE_UTF8MB4_BIN, 
                                                              &temp_allocator,
                                                              target,
                                                              target))) {
      LOG_WARN("fail to convert string", K(ret));
    }
  }

  if (OB_SUCC(ret) && !is_null && OB_ISNULL(search_args) && can_cache_search_args) {
    if (OB_FAIL(prepare_search_pattern(temp_allocator, escape_wc, target, need_wildcmp))) {
      LOG_WARN("prepare search pattern failed", K(ret));
    } else if (OB_NOT_NULL(search_ctx)) {
      ObIAllocator &allocator = ctx.exec_ctx_.get_allocator();
      void *buf = allocator.alloc(sizeof(CacheSearchArgs));
      ObString cached_target;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate cached search arguments failed", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator, target, cached_target))) {
        LOG_WARN("copy constant search target failed", K(ret));
      } else {
        // Publish only after every argument is ready and target has execution-owned storage.
        search_ctx->search_args_ = new (buf) CacheSearchArgs(escape_wc, cached_target, need_wildcmp, one_flag);
      }
    }
  }

  // Materialize the legacy binary once so every search path shares the same DOM.
  if (OB_SUCC(ret) && !is_null && !wrapper.is_bin() && wrapper.get_dom()->is_bin()) {
    ObIJsonBase *json_tree = NULL;
    if (OB_FAIL(ObJsonBaseFactory::transform(&temp_allocator, wrapper.get_dom(),
                                            ObJsonInType::JSON_TREE, json_tree))) {
      LOG_WARN("convert legacy binary to tree failed", K(ret));
    } else {
      wrapper = ObJsonWrapper(json_tree);
    }
  }

  ObJsonBuffer path_str(&temp_allocator, 128);
  ObJsonBufferVector hits;
  ObJsonBufferSortedVector duplicates;
  if (OB_SUCC(ret) && !is_null) {
    if (expr.arg_cnt_ < 5) {
      if (OB_FAIL(path_str.append("$"))) {
        LOG_WARN("faild to append '$' to path str.", K(ret));
      } else if (OB_FAIL(find_matches(&temp_allocator, wrapper, path_str, hits, duplicates,
          target, one_flag, escape_wc, need_wildcmp))) {
        LOG_WARN("failed to find matches for path.", K(ret), K(one_flag), K(escape_wc));
      }
    } else {
      // read all JsonPath from arg
      ObVector<ObJsonPath*> json_paths;
      ObJsonPathCache ctx_cache(&temp_allocator);
      ObJsonPathCache *path_cache = search_ctx != NULL ? &search_ctx->path_cache_ : &ctx_cache;

      for (uint64_t i = 4; OB_SUCC(ret) && !is_null && i < expr.arg_cnt_; i++) {        
        json_arg = expr.args_[i];
        bool is_const = json_arg->is_static_const_expr();
        val_type = json_arg->datum_meta_.type_;
        if (OB_FAIL(temp_allocator.eval_arg(json_arg, ctx, json_datum))) {
          LOG_WARN("eval json arg failed", K(ret));
        } else if (val_type == ObNullType || json_datum->is_null()) {
          is_null = true;
        } else if (!ob_is_string_type(val_type)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("input type error", K(val_type));
        } else {
          ObString j_path_text = json_datum->get_string();
          ObJsonPath *j_path = NULL;
          if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, j_path_text, is_null))) {
            LOG_WARN("fail to get real data.", K(ret), K(j_path_text));
          } else if (j_path_text.length() == 0) {
            is_null = true;
          } else if (json_arg->datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN && 
            OB_FAIL(ObJsonExprHelper::convert_string_collation_type(json_arg->datum_meta_.cs_type_,
                                                                    CS_TYPE_UTF8MB4_BIN, 
                                                                    &temp_allocator,
                                                                    j_path_text,
                                                                    j_path_text))) {
            LOG_WARN("fail to convert string", K(ret));
          } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(temp_allocator, path_cache, j_path,
              j_path_text, i, true, is_const))) {
            LOG_WARN("parse text to path failed", K(j_path_text), K(ret));
          } else if (OB_FAIL(json_paths.push_back(j_path))) {
            LOG_WARN("push new path to vector failed", K(i), K(ret));
          }
        }
      }

      bool is_finish = false;
      for (uint64_t i = 4; OB_SUCC(ret) && !is_null && i < expr.arg_cnt_ && !is_finish; i++) {
        ObJsonPath *j_path = json_paths[i - 4];
        const bool can_match_many = j_path->can_match_many();
        if (one_flag && hits.size() > 0) {
          is_finish = true;
        }
        if (OB_SUCC(ret) && !is_finish) {
          ObJsonSeekIterator iter(temp_allocator);
          if (OB_FAIL(iter.init(wrapper, *j_path))) {
            LOG_WARN("initialize path iterator failed", K(ret), K(i));
          }
          while (OB_SUCC(ret) && !(one_flag && hits.size() > 0)) {
            ObJsonWrapper candidate;
            ObJsonBuffer *candidate_path = NULL;
            path_str.reuse();
            if (OB_ITER_END == (ret = iter.next(candidate, candidate_path))) {
              ret = OB_SUCCESS;
              break;
            } else if (OB_FAIL(ret)) {
              LOG_WARN("get next path candidate failed", K(ret), K(i));
            } else if (can_match_many) {
              if (OB_FAIL(find_matches(&temp_allocator, candidate, *candidate_path,
                                       hits, duplicates, target, one_flag, escape_wc, need_wildcmp))) {
                LOG_WARN("failed to find matches", K(ret), K(i), K(one_flag), K(escape_wc));
              }
            } else {
              if (OB_FAIL(j_path->to_string(path_str))) {
                LOG_WARN("serialize search path failed", K(ret));
              } else if (OB_FAIL(find_matches(&temp_allocator, candidate, path_str,
                                              hits, duplicates, target, one_flag, escape_wc, need_wildcmp))) {
                LOG_WARN("failed to find matches", K(ret), K(i), K(one_flag), K(escape_wc));
              }
            }
          }
        }
      }
    }
  }

  // packed string to return
  ObIJsonBase *j_res = NULL;
  if (OB_UNLIKELY(OB_FAIL(ret))) {
    LOG_WARN("json_search failed", K(ret));
  } else if (hits.size() == 0) {
    is_null = true;
  } else if (hits.size() == 1) {
    void *buf = temp_allocator.alloc(sizeof(ObJsonString));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("json_search alloc ObJsonString failed", K(ret));
    } else {
      ObJsonString *j_str = new (buf) ObJsonString(hits[0]->ptr(), hits[0]->length());
      j_res = j_str;
    }
  } else {
    void *buf = temp_allocator.alloc(sizeof(ObJsonArray));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("json_search alloc ObJsonArray failed", K(ret));
    } else {
      ObJsonArray *j_arr = new (buf) ObJsonArray(&temp_allocator);
      for (int32_t i = 0; OB_SUCC(ret) && i < hits.size(); i++) {
        buf = temp_allocator.alloc(sizeof(ObJsonString));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("json_search alloc jsonString failed", K(ret));
        } else {
          ObJsonString *j_str = new (buf) ObJsonString(hits[i]->ptr(), hits[i]->length());
          if (OB_FAIL(j_arr->append(j_str))) {
            LOG_WARN("failed to append path to array.", K(ret), K(i), K(*j_str));
          }
        }
      }
      if (OB_SUCC(ret)) {
        j_res = j_arr;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null) {
      res.set_null();
    } else {
      ObString raw_bin;
      if (OB_FAIL(share::ObJsonAccessUtils::get_raw_binary(j_res, raw_bin, &temp_allocator))) {
        LOG_WARN("json_keys get result binary failed", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
        LOG_WARN("fail to pack json result", K(ret));
      }
    }
  }

  return ret;
}

int ObExprJsonSearch::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_search;
  return OB_SUCCESS;
}


}
}
