/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_ai_decide.h"
#include "ob_ai_decision_utils.h"
#include "ob_ai_func_client.h"
#include "ob_ai_func_utils.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/random/ob_random.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_cluster_version.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

ObExprAIDecide::ObExprAIDecide(ObIAllocator &allocator)
    : ObFuncExprOperator(allocator, T_FUN_SYS_AI_DECIDE, N_AI_DECIDE, MORE_THAN_ZERO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprAIDecide::check_decision_config(const share::ObAIModelConfigInfo &config)
{
  int ret = OB_SUCCESS;
  const bool typesafe = 0 == config.get_provider().case_compare(ObAIFuncProviderUtils::TYPESAFE);
  if (config.is_gateway_route() || (!typesafe
      && !ObAIDecisionUtils::is_openai_provider(config.get_provider()))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI_DECIDE endpoint protocol");
  } else if (!ObAIDecisionUtils::is_valid_url(config.get_url())
      || (typesafe && OB_NOT_NULL(config.get_message_parameters())
          && 0 != config.get_message_parameters()->element_count())
      || config.get_min_concurrency() <= 0
      || config.get_max_concurrency() < config.get_min_concurrency()) {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObExprAIDecide::build_body(ObIAllocator &allocator, const share::ObAIModelConfigInfo &config,
                               ObJsonNode *state, ObJsonNode *questions, ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(config.get_provider().case_compare(ObAIFuncProviderUtils::TYPESAFE) == 0
      ? ObTypeSafeDecisionUtils::get_body(
          allocator, config.get_request_model_name(), state, questions, body)
      : ObOpenAIDecisionUtils::get_body(
          allocator, config.get_request_model_name(), state, questions, body,
          config.get_message_parameters()))) {
    LOG_WARN("failed to construct decision request body", K(ret));
  }
  return ret;
}

int ObExprAIDecide::send_request(ObIAllocator &allocator, const share::ObAIModelConfigInfo &config,
                                 ObJsonNode *state, ObJsonNode *questions, ObJsonObject *&output)
{
  int ret = OB_SUCCESS;
  output = nullptr;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const int64_t max_retries = tenant_config.is_valid() ? tenant_config->model_max_retries : 2;
  const int64_t timeout_us = tenant_config.is_valid()
      ? tenant_config->model_request_timeout : 60L * 1000000;
  ObJsonObject *body = nullptr;
  ObJsonObject *response = nullptr;
  if (!config.is_gateway_route()) {
    ObArray<ObString> headers;
    ObAIFuncClient client;
    client.set_timeout_sec(timeout_us / 1000000);
    client.set_max_retry_times(max_retries);
    if (OB_FAIL(build_body(allocator, config, state, questions, body))) {
      LOG_WARN("failed to construct decision request", K(ret));
    } else if (OB_FAIL(ObAIDecisionUtils::get_header(allocator, config.get_api_key(), headers))) {
      LOG_WARN("failed to construct decision headers", K(ret));
    } else if (OB_FAIL(client.send_post(allocator, config.get_url(), headers, body, response))) {
      LOG_WARN("decision request failed", K(ret));
    } else if (OB_FAIL(ObAIDecisionUtils::parse_output(allocator, response, output))) {
      LOG_WARN("invalid decision response", K(ret));
    }
  } else {
    share::ObAiGatewayRouteSession session;
    share::ObAiGatewayEndpoint endpoint;
    share::ObAIModelConfigInfo endpoint_config;
    if (OB_FAIL(session.init(config.get_gw_state()))) {
      LOG_WARN("failed to initialize decision gateway", K(ret));
    }
    DEFER(session.cancel_pending());
    for (int64_t attempt = 0; OB_SUCC(ret); ++attempt) {
      ObArray<ObString> headers;
      ObArray<ObJsonObject *> bodies;
      ObArray<ObAIBatchItemResult> results;
      ObAIFuncClient client;
      // Reissue a complete request per selected endpoint, using the existing one-shot batch API.
      // This rebuilds the endpoint's model/body without modifying the shared client's retry hook.
      if (THIS_WORKER.is_timeout_ts_valid() && THIS_WORKER.get_timeout_remain() <= 0) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(ObAIFuncUtils::gateway_next_config(allocator, session,
                     share::EndpointType::DECISION, endpoint, endpoint_config))) {
        LOG_WARN("failed to select decision gateway endpoint", K(ret));
      } else if (OB_FAIL(check_decision_config(endpoint_config))) {
        LOG_WARN("invalid decision gateway configuration", K(ret));
      } else if (OB_FAIL(build_body(allocator, endpoint_config, state, questions, body))) {
        LOG_WARN("failed to construct decision gateway request", K(ret));
      } else if (OB_FAIL(ObAIDecisionUtils::get_header(
                     allocator, endpoint_config.get_api_key(), headers))) {
        LOG_WARN("failed to construct decision gateway headers", K(ret));
      } else if (OB_FAIL(bodies.push_back(body))) {
        LOG_WARN("failed to retain decision gateway body", K(ret));
      } else if (OB_FAIL(client.send_post_batch_with_result(
                     allocator, endpoint_config.get_url(), headers, bodies, results))) {
        LOG_WARN("decision gateway request failed", K(ret));
      } else if (1 != results.count()) {
        // The one-shot client must return one item for the single submitted body.
        ret = OB_INVALID_DATA;
        LOG_WARN("decision client result count violates the single-request contract",
                 K(ret), K(results.count()));
      } else {
        const ObAIBatchItemResult &item = results.at(0);
        const bool http_success = CURLE_OK == item.curl_code_
            && item.http_code_ >= 200 && item.http_code_ < 300;
        const int report_ret = CURLE_URL_MALFORMAT == item.curl_code_ ? OB_SUCCESS
            : (http_success ? session.on_success() : session.on_failure(item.http_code_));
        if (OB_SUCCESS != report_ret) {
          LOG_WARN("failed to record decision gateway outcome", K(report_ret));
        }
        if (CURLE_URL_MALFORMAT == item.curl_code_) {
          ret = OB_INVALID_ARGUMENT;
        } else if (http_success) {
          if (OB_ISNULL(item.response_)) {
            ret = OB_ERR_INVALID_JSON_TEXT;
          } else if (OB_FAIL(ObAIDecisionUtils::parse_output(allocator, item.response_, output))) {
            LOG_WARN("invalid decision gateway answer", K(ret));
          }
          break;
        } else if ((!ObAIFuncClient::is_retryable_status_code(item.http_code_)
                    && !ObAIFuncClient::is_retryable_curl_code(item.curl_code_))
                   || (max_retries > 0 && attempt >= max_retries)) {
          ret = OB_CURL_ERROR;
        } else {
          const int64_t delay_us = MIN(10000L, 1000L * (1L << MIN(attempt, 4L))
              + ObRandom::rand(0, 1000)) * 1000;
          const int64_t remaining = THIS_WORKER.is_timeout_ts_valid()
              ? MAX(0L, THIS_WORKER.get_timeout_remain()) : delay_us;
          ob_usleep(MIN(delay_us, remaining));
        }
      }
    }
  }
  return ret;
}

int ObExprAIDecide::calc_result_typeN(ObExprResType &type, ObExprResType *types,
                                      int64_t param_num, ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("decision argument types are null", K(ret));
  } else if (PARAM_COUNT != param_num) {
    const ObString name(get_name());
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else {
    types[MODEL_IDX].set_calc_type(ObVarcharType);
    types[MODEL_IDX].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    for (int64_t i = STATE_IDX; OB_SUCC(ret) && i < param_num; ++i) {
      if (ObNullType == types[i].get_type() || ObJsonType == types[i].get_type()) {
      } else if (ob_is_string_type(types[i].get_type())
                 && !ob_is_varbinary_or_binary(types[i].get_type(), types[i].get_collation_type())
                 && !ob_is_blob(types[i].get_type(), types[i].get_collation_type())) {
        types[i].set_calc_type(ObVarcharType);
        types[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
      }
    }
    if (OB_SUCC(ret)) {
      type.set_json();
      type.set_length(ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType].get_length());
    }
  }
  return ret;
}

int ObExprAIDecide::read_request_args(const ObExpr &expr, ObEvalCtx &ctx,
                                      MultimodeAlloctor &allocator,
                                      const ObDatum &state_datum, const ObDatum &questions_datum,
                                      ObJsonNode *&state, ObJsonNode *&questions)
{
  int ret = OB_SUCCESS;
  state = nullptr;
  questions = nullptr;
  for (int64_t index = STATE_IDX; OB_SUCC(ret) && index <= QUESTIONS_IDX; ++index) {
    const ObDatum &datum = STATE_IDX == index ? state_datum : questions_datum;
    ObJsonNode *&node = STATE_IDX == index ? state : questions;
    ObString input;
    if (index >= expr.arg_cnt_ || OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[index])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid decision argument slot", K(ret), K(index));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, datum,
                   expr.args_[index]->datum_meta_, input, &ctx.exec_ctx_))) {
      LOG_WARN("failed to read decision argument", K(ret), K(index));
    } else {
      allocator.add_baseline_size(input.length());
      if (STATE_IDX == index && ObJsonType != expr.args_[index]->datum_meta_.type_) {
        ObJsonString *value = nullptr;
        if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, input, value))) {
          LOG_WARN("failed to construct decision text state", K(ret));
        } else {
          node = value;
        }
      } else {
        ObIJsonBase *value = nullptr;
        const ObJsonInType input_type = ObJsonExprHelper::get_json_internal_type(
            expr.args_[index]->datum_meta_.type_);
        if (ObJsonInType::JSON_TREE == input_type && input.length() > 0
            && nullptr != memchr(input.ptr(), '\0', input.length())) {
          ret = OB_ERR_INVALID_JSON_TEXT;
        } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, input, input_type,
                       ObJsonInType::JSON_TREE, value, ObJsonParser::JSN_UNIQUE_FLAG,
                       ObJsonExprHelper::get_json_max_depth_config(ctx)))) {
          LOG_WARN("failed to parse decision argument", K(ret), K(index));
        } else {
          node = static_cast<ObJsonNode *>(value);
        }
      }
    }
  }
  return ret;
}

int ObExprAIDecide::eval_ai_decide(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *model = nullptr;
  ObDatum *state = nullptr;
  ObDatum *questions = nullptr;
  if (OB_UNLIKELY(PARAM_COUNT != expr.arg_cnt_)) {
    const ObString name(N_AI_DECIDE);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else if (OB_FAIL(expr.eval_param_value(ctx, model, state, questions))) {
    LOG_WARN("failed to evaluate decision arguments", K(ret));
  } else if (model->is_null() || state->is_null() || questions->is_null()) {
    result.set_null();
  } else {
    const uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    ObEvalCtx::TempAllocGuard allocation_guard(ctx);
    MultimodeAlloctor allocator(allocation_guard.get_allocator(), expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_DECIDE));
    const ObString model_key = model->get_string();
    share::ObAIModelConfigInfo config;
    uint64_t data_version = 0;
    ObJsonNode *state_json = nullptr;
    ObJsonNode *questions_json = nullptr;
    ObJsonObject *output = nullptr;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("failed to get decision data version", K(ret));
    } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_1
               || data_version < DATA_VERSION_4_6_3_1) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI decisions require version 4.6.3.1");
    } else if (model_key.empty()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(ObAIFuncUtils::get_model_config_info(allocator, model_key,
                 share::EndpointType::DECISION, config))) {
      LOG_WARN("failed to resolve decision model", K(ret));
    } else if (!config.is_gateway_route() && OB_FAIL(check_decision_config(config))) {
      LOG_WARN("invalid direct decision configuration", K(ret));
    } else if (OB_FAIL(read_request_args(expr, ctx, allocator, *state, *questions,
                                         state_json, questions_json))) {
      LOG_WARN("failed to read decision request arguments", K(ret));
    } else if (OB_FAIL(send_request(allocator, config, state_json, questions_json, output))) {
      LOG_WARN("decision request failed", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_res(expr, ctx, allocator, output, result))) {
      LOG_WARN("failed to pack decision JSON result", K(ret));
    }
  }
  if (OB_INVALID_ARGUMENT == ret) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_DECIDE arguments or decision response");
  }
  return ret;
}

int ObExprAIDecide::init_decision_pending(ObIAllocator &allocator, const ObString &model,
                                          ObAIFuncBatchState &pending)
{
  int ret = OB_SUCCESS;
  // The source model may live in the per-row arena; retain it for the pending group.
  if (model.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ob_write_string(allocator, model, pending.model_id_))) {
    LOG_WARN("failed to retain decision model identifier", K(ret));
  } else if (OB_FAIL(ObAIFuncBatchUtils::init_pending_state(allocator, pending.model_id_,
                 share::EndpointType::DECISION, pending, check_decision_config))) {
    LOG_WARN("failed to initialize vector decision model", K(ret));
  }
  return ret;
}

int ObExprAIDecide::pack_batch_response(const ObExpr &expr, ObEvalCtx &ctx, ObIAllocator &allocator,
                                        ObJsonObject *response, const ObArray<int64_t> &rows,
                                        const share::ObAIModelConfigInfo &config, int64_t user_dim,
                                        ObIVector *result)
{
  int ret = OB_SUCCESS;
  UNUSED(config);
  UNUSED(user_dim);
  ObJsonObject *output = nullptr;
  ObString binary;
  if (1 != rows.count()) {
    // Each decision body is queued with exactly one original row index.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("decision batch row mapping violates the one-row-per-request contract",
             K(ret), K(rows.count()));
  } else if (OB_FAIL(ObAIDecisionUtils::parse_output(allocator, response, output))) {
    LOG_WARN("invalid vector decision response", K(ret));
  } else if (OB_ISNULL(output) || OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("vector decision result is null", K(ret));
  } else if (OB_FAIL(output->get_raw_binary(binary, &allocator))) {
    LOG_WARN("failed to encode vector decision JSON", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::inner_pack_raw_str_to_res(
                 binary, expr, ctx, result, rows.at(0)))) {
    LOG_WARN("failed to pack vector decision JSON result", K(ret));
  }
  return ret;
}

int ObExprAIDecide::eval_ai_decide_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (PARAM_COUNT != expr.arg_cnt_) {
    const ObString name(N_AI_DECIDE);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else if (OB_ISNULL(expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("vector decision arguments are null", K(ret));
  }
  // Evaluate every parameter before NULL handling, preserving subexpression errors.
  for (int64_t i = MODEL_IDX; OB_SUCC(ret) && i < PARAM_COUNT; ++i) {
    if (OB_ISNULL(expr.args_[i])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("vector decision argument expression is null", K(ret));
    } else if (OB_FAIL(expr.args_[i]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("failed to evaluate vector decision argument", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    ObIVector *model_vector = expr.args_[MODEL_IDX]->get_vector(ctx);
    ObIVector *state_vector = expr.args_[STATE_IDX]->get_vector(ctx);
    ObIVector *questions_vector = expr.args_[QUESTIONS_IDX]->get_vector(ctx);
    ObIVector *result = expr.get_vector(ctx);
    ObBitVector &evaluated = expr.get_evaluated_flags(ctx);
    const uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
    ObArenaAllocator config_arena(lib::ObMemAttr(tenant_id, "AIDecideCfg"));
    ObArenaAllocator body_arena(lib::ObMemAttr(tenant_id, "AIDecideBody"));
    ObArenaAllocator row_arena(lib::ObMemAttr(tenant_id, "AIDecideRow"));
    MultimodeAlloctor allocator(body_arena, expr.type_, tenant_id, ret);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, N_AI_DECIDE));
    ObAIFuncBatchState pending;
    DEFER({
      if (pending.is_gateway_) {
        pending.route_session_.cancel_pending();
      }
    });
    if (OB_ISNULL(model_vector) || OB_ISNULL(state_vector)
        || OB_ISNULL(questions_vector) || OB_ISNULL(result)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("decision argument or result vector is null", K(ret));
    }
    for (int64_t row = bound.start(); OB_SUCC(ret) && row < bound.end(); ++row) {
      if (skip.at(row) || evaluated.at(row)) {
        continue;
      } else if (model_vector->is_null(row) || state_vector->is_null(row)
                 || questions_vector->is_null(row)) {
        result->set_null(row);
        evaluated.set(row);
        continue;
      }
      row_arena.reuse();
      ObString model;
      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
        LOG_WARN("failed to get vector decision data version", K(ret));
      } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_1
                 || data_version < DATA_VERSION_4_6_3_1) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI decisions require version 4.6.3.1");
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                     row_arena, model_vector, expr.args_[MODEL_IDX]->datum_meta_,
                     model, row, &ctx.exec_ctx_))) {
        LOG_WARN("failed to read vector decision model identifier", K(ret));
      } else if (pending.initialized_ && pending.model_id_ != model) {
        if (OB_FAIL(ObAIFuncBatchUtils::flush_pending_batch(allocator, expr, ctx, pending,
                       result, evaluated, pack_batch_response))) {
          LOG_WARN("failed to flush decisions before model switch", K(ret));
        } else {
          pending.reset();
          body_arena.reuse();
          config_arena.reuse();
        }
      }
      if (OB_SUCC(ret) && !pending.initialized_
          && OB_FAIL(init_decision_pending(config_arena, model, pending))) {
        LOG_WARN("failed to initialize vector decision model", K(ret));
      }
      ObJsonNode *state = nullptr;
      ObJsonNode *questions = nullptr;
      ObJsonObject *body = nullptr;
      ObDatum state_datum;
      ObDatum questions_datum;
      if (OB_SUCC(ret)) {
        state_datum.set_string(state_vector->get_string(row));
        questions_datum.set_string(questions_vector->get_string(row));
        if (OB_FAIL(read_request_args(expr, ctx, allocator, state_datum, questions_datum,
                                      state, questions))) {
          LOG_WARN("failed to read vector decision request arguments", K(ret));
        } else if (OB_FAIL(build_body(allocator, pending.config_, state, questions, body))) {
          LOG_WARN("failed to construct vector decision request", K(ret));
        } else if (OB_FAIL(pending.bodies_.push_back(body))) {
          LOG_WARN("failed to retain vector decision body", K(ret));
        } else if (OB_FAIL(pending.row_starts_.push_back(pending.row_indices_flat_.count()))) {
          LOG_WARN("failed to retain vector decision row start", K(ret));
        } else if (OB_FAIL(pending.row_lens_.push_back(1))) {
          LOG_WARN("failed to retain vector decision row count", K(ret));
        } else if (OB_FAIL(pending.row_indices_flat_.push_back(row))) {
          LOG_WARN("failed to retain vector decision row index", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObAIFuncBatchUtils::flush_pending_batch(allocator, expr, ctx,
                   pending, result, evaluated, pack_batch_response))) {
      LOG_WARN("failed to flush final vector decisions", K(ret));
    }
  }
  if (OB_INVALID_ARGUMENT == ret) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "AI_DECIDE arguments or decision response");
  }
  return ret;
}

int ObExprAIDecide::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_ISNULL(expr_cg_ctx.session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("decision code generation session is not initialized", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(
                 expr_cg_ctx.session_->get_effective_tenant_id(), data_version))) {
    LOG_WARN("failed to get decision data version", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_1
             || data_version < DATA_VERSION_4_6_3_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "AI decisions require version 4.6.3.1");
  } else {
    rt_expr.eval_func_ = eval_ai_decide;
    rt_expr.eval_vector_func_ = eval_ai_decide_vector;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
