/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_PC
#include "ob_hybrid_search_plan_cache.h"
#include "ob_hybrid_search_dsl_parameterizer.h"
#include "sql/parser/ob_fast_parser.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

void ObHybridSearchTokenHandler::reject_carrier(const ObHybridDslFallbackReason reason)
{
  state_ = HYBRID_UNSUPPORTED;
  source_.supported_ = false;
  if (DSL_FALLBACK_NONE == source_.fallback_reason_) {
    source_.fallback_reason_ = reason;
  }
}

int ObHybridSearchTokenHandler::process_token(const ObString &raw_token,
    const int64_t offset, const int64_t key_offset, const bool is_normal, const bool is_parameter,
    const bool is_ignored, const ParseNode *node, const int64_t raw_param_idx)
{
  int ret = OB_SUCCESS;
  const ObString token = raw_token.trim();
  static const int64_t HYBRID_SEARCH_NAME_LENGTH = sizeof("hybrid_search") - 1;
  if (HYBRID_UNSUPPORTED == state_) {
    // An unsupported carrier stays on the full parsing path.
  } else if (token.length() >= 3 && 0 == MEMCMP(token.ptr(), "/*!", 3)) {
    // Executable comments may hide another carrier and have no inner lexer tokens.
    has_executable_comment_ = true;
    for (int64_t i = 3; !source_.present_
         && i + HYBRID_SEARCH_NAME_LENGTH <= token.length(); ++i) {
      if (('h' == token.ptr()[i] || 'H' == token.ptr()[i])
          && 0 == STRNCASECMP(token.ptr() + i, "hybrid_search", HYBRID_SEARCH_NAME_LENGTH)) {
        source_.present_ = true;
      }
    }
    if (source_.present_) {
      reject_carrier(DSL_FALLBACK_EXECUTABLE_COMMENT);
    }
  } else if (is_ignored || token.empty()) {
    // Executable comments must be handled before other ignored tokens.
  } else if ((FIND_HYBRID == state_ || HYBRID_COMPLETE == state_)
             && is_normal && 0 == token.case_compare("hybrid_search")) {
    // HYBRID_SEARCH is non-reserved and may be an ordinary identifier. Only
    // the complete call prefix below establishes a DSL carrier.
    state_ = EXPECT_LPAREN;
  } else {
    switch (state_) {
      case FIND_HYBRID:
      case HYBRID_COMPLETE:
        break;
      case EXPECT_LPAREN:
        if (token == "(") {
          state_ = EXPECT_TABLE;
        } else if (source_.present_) {
          state_ = HYBRID_COMPLETE;
        } else {
          state_ = FIND_HYBRID;
        }
        break;
      case EXPECT_TABLE:
        if (0 == token.case_compare("table")) {
          // Multiple actual carriers cannot share one DSL parameter layout.
          if (source_.present_) {
            reject_carrier(DSL_FALLBACK_MULTIPLE_CARRIERS);
          } else {
            state_ = EXPECT_TABLE_NAME;
          }
          source_.present_ = true;
          source_.supported_ = false;
        } else if (source_.present_) {
          state_ = HYBRID_COMPLETE;
        } else {
          state_ = FIND_HYBRID;
        }
        break;
      case EXPECT_TABLE_NAME:
      case IN_TABLE_NAME:
        if (IN_TABLE_NAME == state_ && token == ",") {
          state_ = EXPECT_DSL;
        } else if (is_normal && token != "("
                   && token != ")" && token != "," && token != ";") {
          // Qualified names may be a single token or separate quoted identifiers.
          state_ = IN_TABLE_NAME;
        } else {
          reject_carrier(DSL_FALLBACK_TABLE_SYNTAX);
        }
        break;
      case EXPECT_DSL: {
        source_.sql_offset_ = offset;
        source_.key_offset_ = key_offset;
        source_.raw_param_idx_ = raw_param_idx;
        source_.sql_length_ = raw_token.length();
        if (is_parameter && OB_NOT_NULL(node) && T_VARCHAR == node->type_) {
          source_.kind_ = SOURCE_LITERAL;
          source_.literal_.assign_ptr(node->str_value_, node->str_len_);
          state_ = EXPECT_RPAREN;
        } else if (is_normal && token.length() > 1
                   && token.ptr()[0] == '@' && token.ptr()[1] != '@'
                   && token.ptr()[1] != '\'' && token.ptr()[1] != '"'
                   && token.ptr()[1] != '`') {
          source_.kind_ = SOURCE_USER_VARIABLE;
          source_.user_var_name_.assign_ptr(token.ptr() + 1, token.length() - 1);
          state_ = EXPECT_RPAREN;
        } else {
          reject_carrier(DSL_FALLBACK_CARRIER_SYNTAX);
        }
        break;
      }
      case EXPECT_RPAREN:
        if (token == ")") {
          state_ = HYBRID_COMPLETE;
          source_.supported_ = true;
        } else {
          reject_carrier(DSL_FALLBACK_CARRIER_SYNTAX);
        }
        break;
      default:
        break;
    }
  }
  if (HYBRID_UNSUPPORTED == state_) {
    source_.supported_ = false;
  }
  return ret;
}

int ObHybridSearchTokenHandler::finish(ObIAllocator &allocator,
                                       ObFastParserResult &result) const
{
  int ret = OB_SUCCESS;
  result.hybrid_dsl_request_ = nullptr;
  if (source_.present_) {
    void *buf = allocator.alloc(sizeof(ObHybridDslRequestInfo));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObHybridDslRequestInfo *request = new (buf) ObHybridDslRequestInfo(allocator);
      request->get_source() = source_;
      request->set_cacheable(!has_executable_comment_);
      if (DSL_FALLBACK_NONE == request->get_source().fallback_reason_) {
        if (has_executable_comment_) {
          request->get_source().fallback_reason_ = DSL_FALLBACK_EXECUTABLE_COMMENT;
        } else if (!source_.supported_) {
          request->get_source().fallback_reason_ = DSL_FALLBACK_INCOMPLETE_CALL;
        }
      }
      result.hybrid_dsl_request_ = request;
    }
  }
  return ret;
}

int ObHybridSearchTokenHandler::finish_parse_impl(ObIAllocator &allocator,
    const FPContext &context, ObFastParserResult &result)
{
  int ret = OB_SUCCESS;
  OZ(finish(allocator, result));
  OZ(ObHybridSearchPlanCache::finish_fast_parse(allocator, context, result, session_));
  return ret;
}

int ObHybridSearchPlanCache::init_fast_parser(ObHybridSearchTokenHandler &handler,
    ObSQLSessionInfo *session, const bool is_enabled, FPContext &fp_ctx)
{
  int ret = OB_SUCCESS;
  fp_ctx.hybrid_search_token_handler_ = nullptr;
  // DSL parameterization requires the fast lexer's token stream. Without a
  // DSL request, need_add_plan() keeps hybrid plans out of the cache.
  if (is_enabled && GCONF._ob_enable_fast_parser && !lib::is_oracle_mode()
      && !fp_ctx.is_format_ && !fp_ctx.is_udr_mode_) {
    handler.set_session(session);
    fp_ctx.hybrid_search_token_handler_ = &handler;
  }
  return ret;
}

int ObHybridSearchPlanCache::init_fast_parser(ObHybridSearchTokenHandler &handler, const ObPlanCacheCtx &ctx,
    FPContext &fp_ctx)
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  if (ctx.mode_ == PC_TEXT_MODE && !ctx.is_rewrite_sql_
      && OB_NOT_NULL(ctx.sql_ctx_.session_info_)) {
    omt::ObTenantConfigGuard tenant_config(
        TENANT_CONF(ctx.sql_ctx_.session_info_->get_effective_tenant_id()));
    is_enabled = tenant_config.is_valid() && tenant_config->_enable_hybrid_search_plan_cache;
  }
  // Gate before attaching the token handler or constructing the DSL template key.
  // When disabled, the ordinary SQL key cannot select a cached DSL template, and no DSL request
  // is created; need_add_plan() therefore also rejects newly compiled hybrid plans.
  OZ(init_fast_parser(handler, ctx.sql_ctx_.session_info_,
      is_enabled, fp_ctx));
  return ret;
}

// Keep the first lexer pass's SQL nodes. The carrier itself is fixed text in
// the key, while the extracted DSL values follow the SQL raw-parameter prefix.
int ObHybridSearchPlanCache::prepare_dsl_key(ObIAllocator &allocator, const FPContext &fp_ctx,
    ObHybridDslRequestInfo &request, ObFastParserResult &result)
{
  int ret = OB_SUCCESS;
  ObSqlString literal;
  ObSqlString key;
  bool no_backslash_escapes = false;
  IS_NO_BACKSLASH_ESCAPES(fp_ctx.sql_mode_, no_backslash_escapes);
  OZ(literal.append("'"));
  for (int64_t i = 0; OB_SUCC(ret) && i < request.get_dsl_template().length(); ++i) {
    const char ch = request.get_dsl_template().ptr()[i];
    if (ch == '\\' && !no_backslash_escapes) {
      OZ(literal.append("\\\\"));
    } else if (ch == '\'') {
      OZ(literal.append("''"));
    } else {
      OZ(literal.append(&ch, 1));
    }
  }
  OZ(literal.append("'"));
  const ObHybridDslSource &source = request.get_source();
  const int64_t carrier = source.raw_param_idx_;
  const bool is_variable = SOURCE_USER_VARIABLE == source.kind_;
  const int64_t position = source.key_offset_;
  const int64_t replaced_length = is_variable ? source.sql_length_ : 1;
  if (OB_FAIL(ret)) {
  } else if (carrier < 0 || carrier > result.raw_params_.count()
             || (!is_variable && carrier == result.raw_params_.count())
             || position < 0 || position + replaced_length > result.pc_key_.name_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DSL carrier layout", K(ret), K(source), K(result.pc_key_.name_));
  } else {
    OZ(key.append(result.pc_key_.name_.ptr(), position));
    OZ(key.append(literal.string()));
    OZ(key.append(result.pc_key_.name_.ptr() + position + replaced_length,
        result.pc_key_.name_.length() - position - replaced_length));
    if (OB_SUCC(ret) && is_variable) {
      // Normalize @dsl to the same fixed carrier slot as a string literal.
      // The full-parser hook performs the same normalization on its SQL tree.
      // This keeps literal/variable requests on one key and one raw layout.
      void *param_buf = allocator.alloc(sizeof(ObPCParam));
      void *node_buf = allocator.alloc(sizeof(ParseNode));
      if (OB_ISNULL(param_buf) || OB_ISNULL(node_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObPCParam *param = new (param_buf) ObPCParam();
        param->node_ = static_cast<ParseNode *>(node_buf);
        MEMSET(param->node_, 0, sizeof(ParseNode));
        OZ(result.raw_params_.push_back(param));
        for (int64_t i = result.raw_params_.count() - 1; OB_SUCC(ret) && i > carrier; --i) {
          result.raw_params_.at(i) = result.raw_params_.at(i - 1);
        }
        if (OB_SUCC(ret)) {
          result.raw_params_.at(carrier) = param;
        }
      }
    }
    OZ(ob_write_string(allocator, key.string(), result.pc_key_.name_));
    if (OB_SUCC(ret)) {
      ParseNode *node = result.raw_params_.at(carrier)->node_;
      node->type_ = T_VARCHAR;
      node->str_value_ = request.get_dsl_template().ptr();
      node->str_len_ = request.get_dsl_template().length();
      node->raw_text_ = result.pc_key_.name_.ptr() + position;
      node->text_len_ = literal.length();
      node->raw_sql_offset_ = source.sql_offset_;
      node->pos_ = position;
      for (int64_t i = carrier + 1; i < result.raw_params_.count(); ++i) {
        result.raw_params_.at(i)->node_->pos_ += literal.length() - replaced_length;
      }
    }
  }
  return ret;
}

int ObHybridSearchPlanCache::finish_fast_parse(ObIAllocator &allocator,
    const FPContext &fp_ctx, ObFastParserResult &fp_result,
    ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  int fallback_error = OB_SUCCESS;
  if (fp_ctx.hybrid_search_token_handler_ != nullptr && !lib::is_oracle_mode()) {
    ObHybridDslRequestInfo *context = fp_result.hybrid_dsl_request_;
    if (context != nullptr && context->can_cache()) {
      const ObHybridDslSource source = context->get_source();
      context->set_cacheable(false);
      if (!source.supported_) {
        // Keep the carrier's fallback reason from token recognition.
      } else if (fp_ctx.enable_batched_multi_stmt_) {
        context->get_source().fallback_reason_ = DSL_FALLBACK_BATCHED_MULTI_STMT;
      } else {
        ObHybridDslRequestInfo *request = nullptr;
        ObString text = source.literal_;
        ObObj variable;
        ObSEArray<ObPCParam *, 8> dsl_params;
        int extract_ret = OB_SUCCESS;
        if (SOURCE_USER_VARIABLE == source.kind_) {
          if (OB_ISNULL(session)) {
            extract_ret = OB_NOT_SUPPORTED;
          } else if (OB_SUCCESS != (extract_ret = session->get_user_variable_value(
                                     source.user_var_name_, variable))) {
          } else {
            extract_ret = variable.get_string(text);
          }
        }
        if (OB_SUCCESS == extract_ret) {
          context->get_source().fallback_reason_ = DSL_FALLBACK_DSL_EXTRACTION;
          extract_ret = ObHybridSearchDslParameterizer::extract(allocator, text, source,
                                                                request, dsl_params);
        } else {
          context->get_source().fallback_reason_ = DSL_FALLBACK_USER_VARIABLE;
        }
        if (OB_ALLOCATE_MEMORY_FAILED == extract_ret) {
          ret = extract_ret;
        } else if (OB_SUCCESS != extract_ret) {
          // Full parsing preserves errors for invalid/unsupported DSL; skip cache lookup.
          fallback_error = extract_ret;
        } else if (OB_FAIL(prepare_dsl_key(allocator, fp_ctx, *request, fp_result))) {
          LOG_WARN("failed to prepare hybrid DSL SQL", K(ret));
        } else {
          request->set_sql_raw_param_count(fp_result.raw_params_.count());
          for (int64_t i = 0; OB_SUCC(ret) && i < dsl_params.count(); ++i) {
            OZ(fp_result.raw_params_.push_back(dsl_params.at(i)));
          }
          if (OB_SUCC(ret)) {
            fp_result.hybrid_dsl_request_ = request;
          }
        }
      }
    }
    const ObHybridDslRequestInfo *request = fp_result.hybrid_dsl_request_;
    if (OB_SUCC(ret) && request != nullptr && !request->has_parameters()) {
      log_fallback(*request, fallback_error);
    }
  }
  return ret;
}

const char *ObHybridSearchPlanCache::fallback_reason_name(const ObHybridDslFallbackReason reason)
{
  const char *name = "unknown";
  switch (reason) {
    case DSL_FALLBACK_NONE:
      name = "none";
      break;
    case DSL_FALLBACK_EXECUTABLE_COMMENT:
      name = "executable_comment";
      break;
    case DSL_FALLBACK_MULTIPLE_CARRIERS:
      name = "multiple_carriers";
      break;
    case DSL_FALLBACK_TABLE_SYNTAX:
      name = "table_syntax";
      break;
    case DSL_FALLBACK_CARRIER_SYNTAX:
      name = "carrier_syntax";
      break;
    case DSL_FALLBACK_INCOMPLETE_CALL:
      name = "incomplete_call";
      break;
    case DSL_FALLBACK_FAST_PARSER_DISABLED:
      name = "fast_parser_disabled";
      break;
    case DSL_FALLBACK_BATCHED_MULTI_STMT:
      name = "batched_multi_stmt";
      break;
    case DSL_FALLBACK_USER_VARIABLE:
      name = "user_variable";
      break;
    case DSL_FALLBACK_DSL_EXTRACTION:
      name = "dsl_extraction";
      break;
    case DSL_FALLBACK_SQL_REWRITE:
      name = "sql_rewrite";
      break;
    default:
      break;
  }
  return name;
}

void ObHybridSearchPlanCache::log_fallback(const ObHybridDslRequestInfo &request, const int error)
{
  // Expected bypasses are reported once at the request boundary, not per token.
  LOG_TRACE("hybrid search plan cache bypass", K(error),
      "reason", fallback_reason_name(request.get_source().fallback_reason_),
      "source", request.get_source(), "cacheable", request.can_cache(),
      "sql_raw_count", request.get_sql_raw_param_count(),
      "dsl_count", request.get_dsl_param_count());
}

ObHybridDslRequestInfo *ObHybridSearchPlanCache::get_request(ObHybridDslRequestInfo *request)
{
  return request != nullptr && request->has_parameters() ? request : nullptr;
}

ObHybridDslRequestInfo *ObHybridSearchPlanCache::get_request(const ObFastParserResult &result)
{
  return get_request(result.hybrid_dsl_request_);
}

bool ObHybridSearchPlanCache::can_add_plan(const ObFastParserResult &result)
{
  const ObHybridDslRequestInfo *request = result.hybrid_dsl_request_;
  if (request == nullptr && !GCONF._ob_enable_fast_parser) {
    // The caller has resolved HYBRID_SEARCH, so this diagnostic needs no SQL probe.
    LOG_TRACE("hybrid search plan cache bypass",
        "reason", fallback_reason_name(DSL_FALLBACK_FAST_PARSER_DISABLED));
  }
  return request != nullptr && request->params_bound();
}

int ObHybridDslRequestInfo::check_dsl_param_idx(const int64_t dsl_idx) const
{
  int ret = OB_SUCCESS;
  if (dsl_idx < 0 || dsl_idx >= get_dsl_param_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DSL parameter index", K(ret), K(dsl_idx), "dsl_count", get_dsl_param_count());
  }
  return ret;
}

int ObHybridDslRequestInfo::append_param_node(const ObIJsonBase &node)
{
  int ret = OB_SUCCESS;
  if (sql_raw_param_count_ >= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot append DSL node after layout preparation", K(ret), K_(sql_raw_param_count));
  } else {
    OZ(param_nodes_.push_back(&node));
  }
  return ret;
}

int ObHybridDslRequestInfo::find_dsl_param_idx(const ObIJsonBase &node, int64_t &dsl_idx) const
{
  int ret = OB_SUCCESS;
  dsl_idx = -1;
  for (int64_t i = 0; dsl_idx < 0 && i < get_dsl_param_count(); ++i) {
    if (param_nodes_.at(i) == &node) {
      dsl_idx = i;
    }
  }
  // A fixed DSL option is not a parameter and is not an error.
  return ret;
}

int ObHybridDslRequestInfo::check_param_layout(const ObIArray<ObPCParam *> &raw) const
{
  int ret = OB_SUCCESS;
  if (sql_raw_param_count_ < 0 || sql_raw_param_count_ > raw.count()
      || get_dsl_param_count() != raw.count() - sql_raw_param_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inconsistent DSL raw parameter layout", K(ret), K_(sql_raw_param_count),
        "raw_count", raw.count(), "dsl_count", get_dsl_param_count());
  }
  return ret;
}

int ObHybridDslRequestInfo::get_absolute_param_idx(const int64_t dsl_idx,
    const int64_t offset, int64_t &param_idx) const
{
  int ret = OB_SUCCESS;
  param_idx = -1;
  if (OB_FAIL(check_dsl_param_idx(dsl_idx))) {
  } else if (offset < 0 || dsl_idx > INT64_MAX - offset) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DSL parameter offset", K(ret), K(offset), K(dsl_idx));
  } else {
    param_idx = offset + dsl_idx;
  }
  return ret;
}

int ObHybridDslRequestInfo::get_dsl_raw_param_idx(const int64_t dsl_idx,
    const ObIArray<ObPCParam *> &raw, int64_t &raw_idx) const
{
  int ret = OB_SUCCESS;
  raw_idx = -1;
  OZ(check_param_layout(raw));
  OZ(get_absolute_param_idx(dsl_idx, sql_raw_param_count_, raw_idx));
  return ret;
}

int ObHybridDslRequestInfo::get_dsl_exec_param_idx(const int64_t dsl_idx,
    const int64_t sql_param_count, int64_t &param_idx) const
{
  int ret = OB_SUCCESS;
  ret = get_absolute_param_idx(dsl_idx, sql_param_count, param_idx);
  return ret;
}

int ObHybridDslRequestInfo::mark_param_resolved(const int64_t dsl_idx)
{
  int ret = OB_SUCCESS;
  OZ(check_dsl_param_idx(dsl_idx));
  OZ(resolved_param_idxs_.add_member(dsl_idx));
  return ret;
}

void ObHybridSearchPlanCache::prepare_parse(ObPlanCacheCtx &ctx, const ObString &sql,
    bool &add_plan, bool &transform_tree)
{
  ObHybridDslRequestInfo *request = get_request(ctx.fp_result_);
  if (request != nullptr) {
    request->reset_compile();
    if (ctx.is_rewrite_sql_ || sql != ctx.raw_sql_) {
      request->set_cacheable(false);
      request->get_source().fallback_reason_ = DSL_FALLBACK_SQL_REWRITE;
      ctx.should_add_plan_ = false;
      add_plan = false;
      transform_tree = false;
      log_fallback(*request, OB_SUCCESS);
    }
  }
}

int ObHybridSearchPlanCache::normalize_carrier(ParseNode *tree,
    const ObHybridDslRequestInfo &request, int64_t &carrier_count, const ParseNode *&carrier_node)
{
  int ret = OB_SUCCESS;
  if (tree == nullptr) {
  } else if (T_DSL_QUERY == tree->type_) {
    ParseNode *carrier = nullptr;
    const ObHybridDslSource &source = request.get_source();
    if (tree->num_child_ != 1 || OB_ISNULL(tree->children_)
        || OB_ISNULL(carrier = tree->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
    } else if (SOURCE_LITERAL == source.kind_) {
      if (T_VARCHAR != carrier->type_
          || ObString(carrier->str_len_, carrier->str_value_) != source.literal_) {
        ret = OB_NOT_SUPPORTED;
      }
    } else if (SOURCE_USER_VARIABLE == source.kind_) {
      if (T_USER_VARIABLE_IDENTIFIER != carrier->type_
          || ObString(carrier->str_len_, carrier->str_value_) != source.user_var_name_) {
        ret = OB_NOT_SUPPORTED;
      }
    }
    if (OB_NOT_SUPPORTED == ret) {
      LOG_WARN("DSL carrier differs between fast and full parsing", K(ret), K(source));
    } else if (OB_SUCC(ret)) {
      // Only normalize after the original SQL has passed the full parser.
      // The resolver consumes the request snapshot, not this structural string.
      carrier->type_ = T_VARCHAR;
      carrier->num_child_ = 0;
      carrier->children_ = nullptr;
      carrier->str_value_ = request.get_dsl_template().ptr();
      carrier->str_len_ = request.get_dsl_template().length();
      ++carrier_count;
      carrier_node = tree;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tree->num_child_; ++i) {
      OZ(SMART_CALL(normalize_carrier(tree->children_[i], request, carrier_count, carrier_node)));
    }
  }
  return ret;
}

int ObHybridSearchPlanCache::transform_sql_params(ObIAllocator &allocator, ObPlanCacheCtx &ctx,
    ParseNode *tree, SqlInfo &sql_info, ParamStore &params,
    share::schema::ObMaxConcurrentParam::FixParamStore &fixed_params,
    const bool is_transform_outline)
{
  int ret = OB_SUCCESS;
  ObHybridDslRequestInfo *request = get_request(ctx.fp_result_);
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(request) || OB_ISNULL(session)
      || TEXT_MODE != ObSqlParameterization::get_sql_execution_mode(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DSL parameterization input", K(ret), KP(request), KP(session), K(ctx.mode_));
  } else {
    request->reset_compile();
    int64_t carrier_count = 0;
    const ParseNode *carrier_node = nullptr;
    OZ(request->check_param_layout(ctx.fp_result_.raw_params_));
    OZ(normalize_carrier(tree, *request, carrier_count, carrier_node));
    if (OB_SUCC(ret) && carrier_count != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unexpected number of DSL carriers", K(ret), K(carrier_count));
    }
    if (OB_SUCC(ret)) {
      // Only the SQL prefix has full-parser nodes. DSL inputs are appended later.
      const int64_t count = request->get_sql_raw_param_count();
      ObArrayHelper<ObPCParam *> sql_params(count, ctx.fp_result_.raw_params_.get_data(), count);
      OZ(ObSqlParameterization::transform_syntax_tree(allocator, *session, &sql_params,
          tree, sql_info, params, &ctx.select_item_param_infos_, fixed_params,
          is_transform_outline, TEXT_MODE, &ctx.fixed_param_info_list_, false));
      if (OB_SUCC(ret)) {
        request->set_carrier_node(carrier_node);
      }
    }
  }
  return ret;
}

int ObHybridSearchPlanCache::check_sql_param_info(ObPlanCacheCtx &ctx, const SqlInfo &sql_info,
    ObIArray<ObPCParam *> &special_params)
{
  int ret = OB_SUCCESS;
  const ObHybridDslRequestInfo *request = get_request(ctx.fp_result_);
  if (OB_ISNULL(request)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("missing DSL parameterization input", K(ret));
  } else if (OB_FAIL(request->check_param_layout(ctx.fp_result_.raw_params_))) {
  } else {
    const int64_t count = request->get_sql_raw_param_count();
    ObArrayHelper<ObPCParam *> sql_params(count, ctx.fp_result_.raw_params_.get_data(), count);
    OZ(ObSqlParameterization::check_and_generate_param_info(sql_params, sql_info, special_params));
  }
  return ret;
}

int ObHybridSearchPlanCache::append_dsl_params(ObPlanCacheCtx &ctx, SqlInfo &sql_info, ParamStore &params,
    ObIArray<ObPCParam *> &special_params)
{
  int ret = OB_SUCCESS;
  ObHybridDslRequestInfo *request = get_request(ctx.fp_result_);
  if (request != nullptr) {
    const int64_t sql_count = request->get_sql_raw_param_count();
    const int64_t dsl_count = request->get_dsl_param_count();
    const int64_t original_count = params.count();
    ObArrayHelper<ObPCParam *> dsl_raw;
    ObArrayHelper<ObCharsetType> dsl_charsets;
    ObBitSet<> empty_flags;
    OZ(request->check_param_layout(ctx.fp_result_.raw_params_));
    if (OB_SUCC(ret) && sql_info.param_charset_type_.count() != sql_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inconsistent SQL parameter charsets", K(ret), K(sql_count),
          "charset_count", sql_info.param_charset_type_.count());
    }
    // Extend the growable SqlInfo array before gen_special_param_info() copies
    // it into the fixed-size request metadata used by PCV initialization.
    for (int64_t i = 0; OB_SUCC(ret) && i < dsl_count; ++i) {
      // JSON input has no SQL charset introducer. The ordinary resolver uses
      // the connection collation, exactly as for an unadorned SQL literal.
      OZ(sql_info.param_charset_type_.push_back(CHARSET_INVALID));
    }
    if (OB_SUCC(ret)) {
      dsl_raw.init(dsl_count, ctx.fp_result_.raw_params_.get_data() + sql_count, dsl_count);
      dsl_charsets.init(dsl_count, sql_info.param_charset_type_.get_data() + sql_count, dsl_count);
      // SQL parameters already exist; restore only the suffix on cold compilation.
      // Cache hits restore the complete array through the unchanged resolver_params().
      OZ(ObPlanCacheValue::resolver_params(ctx, stmt::T_SELECT, dsl_charsets,
          empty_flags, empty_flags, empty_flags, empty_flags, dsl_raw, &params));
    }
    OZ(ObHybridSearchDslParameterizer::remove_carrier_param(
        *request, ctx.fp_result_.raw_params_, special_params));
    if (OB_SUCC(ret)) {
      for (int64_t i = original_count; i < params.count(); ++i) {
        params.at(i).set_need_to_check_type(true);
      }
      request->set_compile_params(ctx.fp_result_.raw_params_, params, original_count);
    } else {
      while (params.count() > original_count) {
        params.pop_back();
      }
      request->reset_compile();
    }
  }
  return ret;
}

int ObHybridSearchPlanCache::construct_sql(ObIAllocator &allocator, ObPlanCacheCtx &ctx,
    const SqlInfo &sql_info, const FPContext &fp_ctx, ObIArray<ObPCParam *> &special_params)
{
  int ret = OB_SUCCESS;
  const ObHybridDslRequestInfo *request = get_request(ctx.fp_result_);
  if (OB_ISNULL(request)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("missing DSL parameterization input", K(ret));
  } else {
    // A variable carrier can expand beyond the submitted SQL length. Keep this
    // allocation and carrier normalization out of ordinary SQL construction.
    const int64_t length = ctx.raw_sql_.length() + ctx.fp_result_.pc_key_.name_.length();
    char *buffer = static_cast<char *>(allocator.alloc(length));
    int32_t pos = 0;
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate hybrid SQL buffer", K(ret), K(length));
    } else if (OB_FAIL(ObSqlParameterization::construct_sql(
        ctx.fp_result_.pc_key_.name_, special_params, buffer, length, pos))) {
      LOG_WARN("failed to construct hybrid SQL template", K(ret));
    } else {
      const ObHybridDslSource &source = request->get_source();
      const ParseNode *carrier = ctx.fp_result_.raw_params_.at(source.raw_param_idx_)->node_;
      const int64_t end = source.sql_offset_ + source.sql_length_;
      ObSqlString sql;
      // SPM formatting uses the normalized carrier; full parsing uses the original SQL.
      OZ(sql.append(ctx.raw_sql_.ptr(), source.sql_offset_));
      OZ(sql.append(carrier->raw_text_, carrier->text_len_));
      OZ(sql.append(ctx.raw_sql_.ptr() + end, ctx.raw_sql_.length() - end));
      OZ(ObSqlParameterization::formalize_sql_text(allocator, sql.string(),
          ctx.sql_ctx_.spm_ctx_.bl_key_.format_sql_, sql_info, fp_ctx));
      if (OB_SUCC(ret)) {
        ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_.assign_ptr(buffer, pos);
      }
    }
  }
  return ret;
}

int ObHybridSearchPlanCache::check_raw_param_count(const ObFastParserResult &result,
    const int64_t normal_parse_count)
{
  int ret = OB_SUCCESS;
  const ObHybridDslRequestInfo *request = get_request(result);
  if (OB_ISNULL(request)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("missing DSL parameter layout", K(ret));
  } else if (OB_FAIL(request->check_param_layout(result.raw_params_))) {
  } else if (normal_parse_count != request->get_sql_raw_param_count()) {
    // SQL parsing counts R; raw_params_ also contains M extracted DSL inputs.
    ret = OB_ERR_UNEXPECTED;
    LOG_TRACE("DSL SQL constant counts differ between fast and full parsing", K(ret),
        K(normal_parse_count), "sql_raw_count", request->get_sql_raw_param_count(),
        "dsl_count", request->get_dsl_param_count(), "raw_count", result.raw_params_.count());
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
