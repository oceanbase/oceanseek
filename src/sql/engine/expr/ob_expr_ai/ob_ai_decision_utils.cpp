/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_ai_decision_utils.h"
#include "ob_ai_func_utils.h"

namespace oceanbase
{
namespace common
{

int ObAIDecisionUtils::get_header(ObIAllocator &allocator, const ObString &api_key,
                                  ObArray<ObString> &headers)
{
  ObString key = api_key;
  return ObOpenAIUtils::get_header(allocator, key, headers);
}

bool ObAIDecisionUtils::is_valid_url(const ObString &url)
{
  bool valid = url.prefix_match_ci("http://") || url.prefix_match_ci("https://");
  // curl consumes a C string. Reject literal controls before it can truncate the URL;
  // percent-encoded bytes remain literal URL syntax and retain existing curl validation.
  for (int64_t i = 0; valid && i < url.length(); ++i) {
    const uint8_t byte = static_cast<uint8_t>(url[i]);
    valid = byte > 0x20 && byte != 0x7f;
  }
  return valid;
}

bool ObAIDecisionUtils::is_openai_provider(const ObString &provider)
{
  return provider.case_compare(ObAIFuncProviderUtils::OPENAI) == 0;
}

int ObAIDecisionUtils::parse_output(ObIAllocator &allocator, ObJsonObject *response,
                                    ObJsonObject *&result)
{
  int ret = OB_SUCCESS;
  result = nullptr;
  ObJsonNode *answers = nullptr;
  if (OB_ISNULL(response) || ObJsonNodeType::J_OBJECT != response->json_type()
      || OB_ISNULL(answers = response->get_value("answers"))
      || (ObJsonNodeType::J_OBJECT != answers->json_type()
          && ObJsonNodeType::J_ARRAY != answers->json_type())
      || answers->element_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("decision response must contain nonempty answers", K(ret));
  } else {
    ObJsonObject *output = nullptr;
    if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, output))) {
      LOG_WARN("failed to allocate decision result object", K(ret));
    } else if (OB_FAIL(output->add("answers", answers))) {
      LOG_WARN("failed to retain decision answers", K(ret));
    } else {
      result = output;
    }
  }
  return ret;
}

static int build_decision_body(ObIAllocator &allocator, const ObString &model,
                               const char *input_key, ObJsonNode *input,
                               ObJsonNode *questions, ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  ObJsonObject *request = nullptr;
  ObJsonString *model_json = nullptr;
  ObString model_ref = model;
  body = nullptr;
  if (OB_FAIL(ObAIFuncJsonUtils::get_json_object(allocator, request))) {
    LOG_WARN("failed to allocate decision request object", K(ret));
  } else if (OB_FAIL(ObAIFuncJsonUtils::get_json_string(allocator, model_ref, model_json))) {
    LOG_WARN("failed to construct decision model string", K(ret));
  } else if (OB_FAIL(request->add("model", model_json))) {
    LOG_WARN("failed to add decision model", K(ret));
  } else if (OB_FAIL(request->add(input_key, input))) {
    LOG_WARN("failed to add decision input", K(ret));
  } else if (OB_FAIL(request->add("questions", questions))) {
    LOG_WARN("failed to add decision questions", K(ret));
  } else {
    body = request;
  }
  return ret;
}

int ObTypeSafeDecisionUtils::get_body(ObIAllocator &allocator, const ObString &model,
                                      ObJsonNode *state, ObJsonNode *questions, ObJsonObject *&body)
{
  int ret = OB_SUCCESS;
  body = nullptr;
  if (model.empty() || OB_ISNULL(state) || OB_ISNULL(questions)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("decision request argument is empty", K(ret));
  } else if (ObJsonNodeType::J_STRING != state->json_type()
             && ObJsonNodeType::J_OBJECT != state->json_type()
             && ObJsonNodeType::J_ARRAY != state->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("decision state must be text, object or array", K(ret));
  } else if (ObJsonNodeType::J_OBJECT != questions->json_type()
             || questions->element_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("decision questions must be a nonempty object", K(ret));
  } else if (OB_FAIL(build_decision_body(allocator, model, "state", state, questions, body))) {
    LOG_WARN("failed to build TypeSafe decision body", K(ret));
  }
  return ret;
}

int ObOpenAIDecisionUtils::get_body(ObIAllocator &allocator, const ObString &model,
                                    ObJsonNode *input, ObJsonNode *questions, ObJsonObject *&body,
                                    const ObJsonObject *parameters)
{
  int ret = OB_SUCCESS;
  body = nullptr;
  if (model.empty() || OB_ISNULL(input) || OB_ISNULL(questions)
      || ObJsonNodeType::J_ARRAY != questions->json_type() || questions->element_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("OpenAI decision request structure is invalid", K(ret));
  } else if (OB_NOT_NULL(parameters)
             && (OB_NOT_NULL(parameters->get_value("model"))
                 || OB_NOT_NULL(parameters->get_value("input"))
                 || OB_NOT_NULL(parameters->get_value("questions")))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("decision parameters cannot override generated request fields", K(ret));
  } else if (ObJsonNodeType::J_STRING != input->json_type()
             && ObJsonNodeType::J_OBJECT != input->json_type()
             && ObJsonNodeType::J_ARRAY != input->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("OpenAI decision input must be text, object or array", K(ret));
  } else {
    ObJsonObject *request = nullptr;
    if (OB_FAIL(build_decision_body(allocator, model, "input", input, questions, request))) {
      LOG_WARN("failed to build OpenAI decision body", K(ret));
    } else {
      for (uint64_t i = 0;
           OB_SUCC(ret) && OB_NOT_NULL(parameters) && i < parameters->element_count();
           ++i) {
        ObString key;
        if (OB_FAIL(parameters->get_key(i, key))) {
          LOG_WARN("failed to read decision parameter name", K(ret));
        } else if (OB_FAIL(request->add(key, parameters->get_value(i)))) {
          LOG_WARN("failed to add native decision parameter", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        body = request;
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
