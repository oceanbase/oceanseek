/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ai_service/ob_ai_service_struct.h"
#include "lib/container/ob_array.h"
#include "lib/json/ob_json.h"
#include "share/ai_service/ob_ai_gateway_circuit_state.h"
#include "share/ob_encryption_util.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_ai_model_mgr.h"
#include "share/schema/ob_ai_provider_mgr.h"

#define USING_LOG_PREFIX SHARE

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::json;

namespace oceanbase
{
namespace share
{

const int64_t ObAIExtraHeadersUtils::MAX_SERIALIZED_LENGTH;

namespace
{

int append_json_string(const ObString &value, ObJsonBuffer &buf)
{
  int ret = OB_SUCCESS;
  static const char HEX[] = "0123456789abcdef";
  if (OB_FAIL(buf.append("\""))) {
    LOG_WARN("failed to append json quote", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    const unsigned char c = static_cast<unsigned char>(value.ptr()[i]);
    const char *escaped = nullptr;
    switch (c) {
      case '"': escaped = "\\\""; break;
      case '\\': escaped = "\\\\"; break;
      case '\b': escaped = "\\b"; break;
      case '\f': escaped = "\\f"; break;
      case '\n': escaped = "\\n"; break;
      case '\r': escaped = "\\r"; break;
      case '\t': escaped = "\\t"; break;
      default: break;
    }
    if (OB_NOT_NULL(escaped)) {
      if (OB_FAIL(buf.append(escaped))) {
        LOG_WARN("failed to append escaped json character", K(ret), K(c));
      }
    } else if (c <= 0x1f || c == 0x7f) {
      char unicode_escape[6] = {'\\', 'u', '0', '0', HEX[c >> 4], HEX[c & 0x0f]};
      if (OB_FAIL(buf.append(unicode_escape, sizeof(unicode_escape)))) {
        LOG_WARN("failed to append json unicode escape", K(ret), K(c));
      }
    } else if (OB_FAIL(buf.append(value.ptr() + i, 1))) {
      LOG_WARN("failed to append json character", K(ret), K(c));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(buf.append("\""))) {
    LOG_WARN("failed to append json quote", K(ret));
  }
  return ret;
}

bool is_http_token_char(const unsigned char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
         (c >= '0' && c <= '9') ||
         c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\'' ||
         c == '*' || c == '+' || c == '-' || c == '.' || c == '^' || c == '_' ||
         c == '`' || c == '|' || c == '~';
}

bool is_invalid_http_field_value_char(const unsigned char c)
{
  return (c <= 0x1f && c != '\t') || c == 0x7f;
}

bool matches_nonempty_header_prefix(const ObString &name, const ObString &prefix)
{
  return name.length() > prefix.length() && name.prefix_match_ci(prefix);
}

bool is_allowed_header_name(const ObString &name)
{
  bool allowed = matches_nonempty_header_prefix(name, "X-DashScope-") ||
                 matches_nonempty_header_prefix(name, "OpenAI-") ||
                 matches_nonempty_header_prefix(name, "X-TC-") ||
                 matches_nonempty_header_prefix(name, "X-Bce-") ||
                 0 == name.case_compare("X-Client-Request-Id") ||
                 0 == name.case_compare("appid");
  for (int64_t i = 0; allowed && i < name.length(); ++i) {
    allowed = is_http_token_char(static_cast<unsigned char>(name.ptr()[i]));
  }
  return allowed && !name.empty();
}

int normalize_header_string(const ObIJsonBase &value, ObString &normalized)
{
  int ret = OB_SUCCESS;
  if (value.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
    LOG_WARN("header value should be json string", K(ret), K(value.json_type()));
  } else {
    const char *data = value.get_data();
    int64_t begin = 0;
    int64_t end = value.get_data_length();
    while (begin < end && (data[begin] == ' ' || data[begin] == '\t')) {
      ++begin;
    }
    while (end > begin && (data[end - 1] == ' ' || data[end - 1] == '\t')) {
      --end;
    }
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      const unsigned char c = static_cast<unsigned char>(data[i]);
      if (is_invalid_http_field_value_char(c)) {
        ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
        LOG_WARN("header string contains invalid character", K(ret), K(c));
      }
    }
    if (OB_SUCC(ret)) {
      normalized.assign_ptr(end > begin ? data + begin : nullptr,
                            static_cast<int32_t>(end - begin));
    }
  }
  return ret;
}

} // namespace

int ObAIExtraHeadersUtils::serialize_compact(const ObIJsonBase &value, ObJsonBuffer &buf)
{
  int ret = OB_SUCCESS;
  switch (value.json_type()) {
    case ObJsonNodeType::J_OBJECT: {
      if (OB_FAIL(buf.append("{"))) {
        LOG_WARN("failed to append json object start", K(ret));
      }
      JsonObjectIterator iter = value.object_iterator();
      bool first = true;
      while (OB_SUCC(ret) && !iter.end()) {
        ObJsonObjPair elem;
        if (OB_FAIL(iter.get_elem(elem))) {
          LOG_WARN("failed to get json object element", K(ret));
        } else if (!first && OB_FAIL(buf.append(","))) {
          LOG_WARN("failed to append json object separator", K(ret));
        } else if (OB_FAIL(append_json_string(elem.first, buf))) {
          LOG_WARN("failed to append json object key", K(ret));
        } else if (OB_FAIL(buf.append(":"))) {
          LOG_WARN("failed to append json object colon", K(ret));
        } else if (OB_ISNULL(elem.second)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("json object value is null", K(ret));
        } else if (OB_FAIL(serialize_compact(*elem.second, buf))) {
          LOG_WARN("failed to serialize json object value", K(ret));
        } else {
          first = false;
          iter.next();
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(buf.append("}"))) {
        LOG_WARN("failed to append json object end", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_ARRAY: {
      if (OB_FAIL(buf.append("["))) {
        LOG_WARN("failed to append json array start", K(ret));
      }
      const uint64_t count = value.element_count();
      for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        ObIJsonBase *elem = nullptr;
        if (i > 0 && OB_FAIL(buf.append(","))) {
          LOG_WARN("failed to append json array separator", K(ret));
        } else if (OB_FAIL(value.get_array_element(i, elem))) {
          LOG_WARN("failed to get json array element", K(ret), K(i));
        } else if (OB_ISNULL(elem)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("json array value is null", K(ret), K(i));
        } else if (OB_FAIL(serialize_compact(*elem, buf))) {
          LOG_WARN("failed to serialize json array value", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(buf.append("]"))) {
        LOG_WARN("failed to append json array end", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_STRING: {
      ObString string_value(value.get_data_length(), value.get_data());
      if (OB_FAIL(append_json_string(string_value, buf))) {
        LOG_WARN("failed to append json string", K(ret));
      }
      break;
    }
    case ObJsonNodeType::J_INT:
    case ObJsonNodeType::J_UINT:
    case ObJsonNodeType::J_DECIMAL:
    case ObJsonNodeType::J_DOUBLE:
    case ObJsonNodeType::J_BOOLEAN:
      if (OB_FAIL(value.print(buf, true))) {
        LOG_WARN("failed to serialize json scalar", K(ret), K(value.json_type()));
      }
      break;
    case ObJsonNodeType::J_NULL:
      if (!value.is_real_json_null(&value)) {
        ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
        LOG_WARN("json null is not real null", K(ret));
      } else if (OB_FAIL(buf.append("null"))) {
        LOG_WARN("failed to append json null", K(ret));
      }
      break;
    default:
      ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
      LOG_WARN("unsupported json type", K(ret), K(value.json_type()));
      break;
  }
  return ret;
}

int ObAIExtraHeadersUtils::json_value_to_header_value(const ObIJsonBase &value,
                                                      ObJsonBuffer &buf)
{
  int ret = OB_SUCCESS;
  if (value.json_type() == ObJsonNodeType::J_STRING) {
    ObString normalized;
    if (OB_FAIL(normalize_header_string(value, normalized))) {
      LOG_WARN("failed to normalize header string", K(ret));
    } else if (!normalized.empty() && OB_FAIL(buf.append(normalized))) {
      LOG_WARN("failed to append header string", K(ret));
    }
  } else if (OB_FAIL(serialize_compact(value, buf))) {
    LOG_WARN("failed to serialize header value", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < buf.length(); ++i) {
    const unsigned char c = static_cast<unsigned char>(buf.ptr()[i]);
    if (is_invalid_http_field_value_char(c)) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_WARN("header value contains invalid character", K(ret), K(c));
    }
  }
  return ret;
}

int ObAIExtraHeadersUtils::validate_and_serialize(const ObIJsonBase &headers,
                                                  ObJsonBuffer &buf)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> names;
  if (headers.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_AI_FUNC_PARAM_TYPE_INVALID;
    LOG_WARN("extra headers should be json object", K(ret), K(headers.json_type()));
  } else if (OB_FAIL(buf.append("{"))) {
    LOG_WARN("failed to append headers object start", K(ret));
  } else {
    JsonObjectIterator iter = headers.object_iterator();
    bool first = true;
    while (OB_SUCC(ret) && !iter.end()) {
      ObJsonObjPair elem;
      if (OB_FAIL(iter.get_elem(elem))) {
        LOG_WARN("failed to get header element", K(ret));
      } else if (!is_allowed_header_name(elem.first)) {
        ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
        LOG_WARN("header name is not allowed", K(ret), K(elem.first));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < names.count(); ++i) {
        if (0 == names.at(i).case_compare(elem.first)) {
          ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
          LOG_WARN("duplicate header name", K(ret), K(elem.first));
        }
      }
      ObJsonBuffer wire_value(buf.get_allocator());
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(elem.second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("header value is null", K(ret));
      } else if (OB_FAIL(json_value_to_header_value(*elem.second, wire_value))) {
        LOG_WARN("failed to convert header value", K(ret), K(elem.first));
      } else if (!first && OB_FAIL(buf.append(","))) {
        LOG_WARN("failed to append header separator", K(ret));
      } else if (OB_FAIL(append_json_string(elem.first, buf))) {
        LOG_WARN("failed to append header name", K(ret));
      } else if (OB_FAIL(buf.append(":"))) {
        LOG_WARN("failed to append header colon", K(ret));
      } else if (elem.second->json_type() == ObJsonNodeType::J_STRING) {
        const ObString normalized(wire_value.length(), wire_value.ptr());
        if (OB_FAIL(append_json_string(normalized, buf))) {
          LOG_WARN("failed to append normalized header string", K(ret), K(elem.first));
        }
      } else if (OB_FAIL(buf.append(wire_value.string()))) {
        LOG_WARN("failed to append structured header value", K(ret), K(elem.first));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(names.push_back(elem.first))) {
          LOG_WARN("failed to record header name", K(ret));
        } else {
          first = false;
          iter.next();
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(buf.append("}"))) {
      LOG_WARN("failed to append headers object end", K(ret));
    }
  }
  if (OB_SUCC(ret) && buf.length() > MAX_SERIALIZED_LENGTH) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_WARN("extra headers exceed max serialized length", K(ret), K(buf.length()),
             K(MAX_SERIALIZED_LENGTH));
  }
  return ret;
}

const ObString ObAiModelEndpointInfo::DEFAULT_SCOPE = "ALL";

const char *VALID_PROVIDERS[] = {
  "ALIYUN-OPENAI",
  "ALIYUN-DASHSCOPE",
  "DEEPSEEK",
  "SILICONFLOW",
  "COHERE",
  "HUNYUAN-OPENAI",
  "OPENAI",
  "VLLM",
  "TYPESAFE"
};

#define EXTRACT_JSON_ELEM_STR(json_key, member) \
  EXTRACT_JSON_ELEM_STR_WITH_PROCESS(json_key, member, "void")

#define EXTRACT_JSON_ELEM_STR_WITH_PROCESS(json_key, member, post_process) \
      if (elem.first.case_compare(json_key) == 0) { \
        if (elem.second->json_type() != ObJsonNodeType::J_STRING) { \
          ret = OB_AI_FUNC_PARAM_VALUE_INVALID; \
          LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, elem.first.length(), elem.first.ptr()); \
          LOG_WARN("invalid json type", K(ret), K(elem.first), K(elem.second->json_type())); \
        } else { \
          member = ObString(elem.second->get_data_length(), elem.second->get_data()); \
          post_process; \
        } \
      } else

#define EXTRACT_JSON_ELEM_INT(json_key, int_val) \
if (elem.first.case_compare(json_key) == 0) { \
  if (elem.second->json_type() != ObJsonNodeType::J_INT && elem.second->json_type() != ObJsonNodeType::J_UINT) { \
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID; \
    LOG_WARN("invalid json type", K(ret), K(elem.first), K(elem.second->json_type())); \
    FORWARD_USER_ERROR(ret, "invalid parameters type for parameters." #json_key ", " #json_key " must be an integer"); \
  } else { \
    int_val = elem.second->get_int(); \
  } \
} else

#define EXTRACT_JSON_ELEM_DOUBLE(json_key, double_val) \
if (elem.first.case_compare(json_key) == 0) { \
  if (elem.second->json_type() != ObJsonNodeType::J_DOUBLE && \
      elem.second->json_type() != ObJsonNodeType::J_INT && \
      elem.second->json_type() != ObJsonNodeType::J_UINT) { \
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID; \
    LOG_WARN("invalid json type", K(ret), K(elem.first), K(elem.second->json_type())); \
    FORWARD_USER_ERROR(ret, "invalid parameters type for parameters." #json_key ", " #json_key " must be a number"); \
  } else { \
    if (elem.second->json_type() == ObJsonNodeType::J_DOUBLE) { \
      double_val = elem.second->get_double(); \
    } else { \
      double_val = static_cast<double>(elem.second->get_int()); \
    } \
  } \
} else

#define EXTRACT_JSON_ELEM_END() \
  { \
    ret = OB_AI_FUNC_PARAM_INVALID; \
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, elem.first.length(), elem.first.ptr()); \
    LOG_WARN("unknown json key param", K(ret), K(elem.first)); \
  }

#define EXTRACT_JSON_ELEM_NO_CHECK_END() \
  { \
  }

static int check_endpoint_parameters_valid(const ObString &parameters)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AIEpParamChk");
  ObIJsonBase *params_base = nullptr;
  int64_t batch_size = 0;
  int64_t max_image_size = 0;
  int64_t max_tokens = 0;
  int64_t min_concurrency = 0;
  int64_t max_concurrency = 0;
  double temperature = 0.0;
  double top_p = 0.0;
  if (parameters.empty()) {
    // do nothing
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                      parameters,
                                                      ObJsonInType::JSON_TREE,
                                                      ObJsonInType::JSON_TREE,
                                                      params_base))) {
    LOG_WARN("failed to parse endpoint parameters", K(ret), K(parameters));
  } else if (OB_ISNULL(params_base) || params_base->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("parameters"), "parameters");
    LOG_WARN("endpoint parameters should be json object", K(ret), K(parameters));
  } else {
    JsonObjectIterator iter = params_base->object_iterator();
    while (!iter.end() && OB_SUCC(ret)) {
      ObJsonObjPair elem;
      if (OB_FAIL(iter.get_elem(elem))) {
        LOG_WARN("failed to get elem", K(ret));
      } else {
        EXTRACT_JSON_ELEM_INT("batch_size", batch_size)
        EXTRACT_JSON_ELEM_INT("max_image_size", max_image_size)
        EXTRACT_JSON_ELEM_INT("max_tokens", max_tokens)
        EXTRACT_JSON_ELEM_INT("min_concurrency", min_concurrency)
        EXTRACT_JSON_ELEM_INT("max_concurrency", max_concurrency)
        EXTRACT_JSON_ELEM_DOUBLE("temperature", temperature)
        EXTRACT_JSON_ELEM_DOUBLE("top_p", top_p)
        EXTRACT_JSON_ELEM_NO_CHECK_END()
      }
      iter.next();
    }
  }
  if (OB_SUCC(ret)) {
    if (batch_size < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("batch_size"), "batch_size");
      LOG_WARN("batch size is invalid", K(ret), K(batch_size));
    } else if (max_image_size < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("max_image_size"), "max_image_size");
      LOG_WARN("max image size is invalid", K(ret), K(max_image_size));
    } else if (max_tokens < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("max_tokens"), "max_tokens");
      LOG_WARN("max_tokens is invalid", K(ret), K(max_tokens));
    } else if (temperature < 0.0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("temperature"), "temperature");
      LOG_WARN("temperature is invalid, should be in range [0.0, 2.0]", K(ret), K(temperature));
    } else if (top_p < 0.0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("top_p"), "top_p");
      LOG_WARN("top_p is invalid, should be in range [0.0, 1.0]", K(ret), K(top_p));
    } else if (min_concurrency < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("min_concurrency"), "min_concurrency");
      LOG_WARN("min_concurrency is invalid, must be a positive integer", K(ret), K(min_concurrency));
    } else if (max_concurrency < 0) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("max_concurrency"), "max_concurrency");
      LOG_WARN("max_concurrency is invalid, must be a positive integer", K(ret), K(max_concurrency));
    } else if (min_concurrency > 0 && max_concurrency > 0 && min_concurrency > max_concurrency) {
      ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("min_concurrency"), "min_concurrency");
      LOG_WARN("min_concurrency must not exceed max_concurrency",
               K(ret), K(min_concurrency), K(max_concurrency));
    }
  }
  return ret;
}

int ObAiModelEndpointInfo::parse_from_json_base(common::ObArenaAllocator &allocator, const ObString &name, const ObIJsonBase &params_jbase)
{
  int ret = OB_SUCCESS;
  reset();
  name_ = name;
  if (OB_FAIL(merge_delta_endpoint(allocator, params_jbase))) {
    LOG_WARN("failed to merge delta endpoint", K(ret), K(params_jbase));
  }
  LOG_INFO("parse from json base", K(ret), K(params_jbase), K(params_jbase.json_type()), K(params_jbase.element_count()));
  return ret;
}

int ObAiModelEndpointInfo::check_valid() const
{
  int ret = OB_SUCCESS;
  if (name_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("name"), "name");
    LOG_WARN("name is empty", K(ret), K(*this));
  } else if (scope_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("scope"), "scope");
    LOG_WARN("scope is empty", K(ret), K(*this));
  } else if (scope_.case_compare(DEFAULT_SCOPE) != 0) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("scope"), "scope");
    LOG_WARN("scope value is invalid", K(ret), K(*this));
  } else if (ai_model_name_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("ai_model_name"), "ai_model_name");
    LOG_WARN("ai_model_name is empty", K(ret), K(*this));
  } else if (url_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("url"), "url");
    LOG_WARN("url is empty", K(ret), K(*this));
  } else if (access_key_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("access_key"), "access_key");
    LOG_WARN("access_key is empty", K(ret), K(*this));
  } else if (provider_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("provider"), "provider");
    LOG_WARN("provider is empty", K(ret), K(*this));
  } else if (!is_valid_provider(provider_)) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("provider"), "provider");
    LOG_WARN("provider is invalid", K(ret), K(*this));
  } else if (OB_FAIL(check_endpoint_parameters_valid(parameters_))) {
    LOG_WARN("parameters is invalid", K(ret), K(*this), K(parameters_));
  } else if (!request_transform_fn_.empty()) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("request_transform_fn"), "request_transform_fn");
    LOG_WARN("request_transform_fn is not empty", K(ret), K(*this));
  } else if (!response_transform_fn_.empty()) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("response_transform_fn"), "response_transform_fn");
    LOG_WARN("response_transform_fn is not empty", K(ret), K(*this));
  }
  return ret;
}

bool ObAiModelEndpointInfo::is_valid_provider(const ObString &provider)
{
  bool is_valid = false;
  for (int i = 0; i < ARRAYSIZEOF(VALID_PROVIDERS); i++) {
    if (provider.case_compare(VALID_PROVIDERS[i]) == 0) {
      is_valid = true;
      break;
    }
  }
  return is_valid;
}

ObString ObAiModelEndpointInfo::get_batch_file_url() const
{
  static const char *KNOWN_SUFFIXES[] = {"/embeddings", "/rerank", "/completions"};
  for (int64_t i = 0; i < ARRAYSIZEOF(KNOWN_SUFFIXES); i++) {
    int32_t suf_len = static_cast<int32_t>(strlen(KNOWN_SUFFIXES[i]));
    if (url_.length() > suf_len) {
      ObString tail(suf_len, url_.ptr() + url_.length() - suf_len);
      if (tail.case_compare(KNOWN_SUFFIXES[i]) == 0) {
        return ObString(url_.length() - suf_len, url_.ptr());
      }
    }
  }
  return url_;
}

int ObAiModelEndpointInfo::merge_delta_endpoint(common::ObArenaAllocator &allocator, const ObIJsonBase &delta_jbase)
{
  int ret = OB_SUCCESS;
  JsonObjectIterator iter = delta_jbase.object_iterator();
  bool has_api_key = false;
  while (!iter.end() && OB_SUCC(ret)) {
    ObJsonObjPair elem;
    if (OB_FAIL(iter.get_elem(elem))) {
      LOG_WARN("failed to get elem", K(ret));
    } else {
      EXTRACT_JSON_ELEM_STR("scope", scope_)
      EXTRACT_JSON_ELEM_STR("ai_model_name", ai_model_name_)
      EXTRACT_JSON_ELEM_STR("url", url_)
      EXTRACT_JSON_ELEM_STR_WITH_PROCESS("access_key", access_key_, has_api_key = true)
      EXTRACT_JSON_ELEM_STR("provider", provider_)
      EXTRACT_JSON_ELEM_STR("request_model_name", request_model_name_)
      EXTRACT_JSON_ELEM_STR("parameters", parameters_)
      EXTRACT_JSON_ELEM_STR("request_transform_fn", request_transform_fn_)
      EXTRACT_JSON_ELEM_STR("response_transform_fn", response_transform_fn_)
      EXTRACT_JSON_ELEM_END()
    }
    iter.next();
  }

  if (OB_SUCC(ret)) {
    if (has_api_key && !access_key_.empty() && OB_FAIL(encrypt_access_key_(allocator, access_key_, access_key_))) {
      LOG_WARN("failed to encrypt access key", K(ret));
    } else if (OB_FAIL(check_valid())) {
      LOG_WARN("invalid endpoint", K(ret), K(delta_jbase));
    }
  }

  LOG_INFO("merge delta endpoint", K(ret), K(delta_jbase), K(delta_jbase.json_type()), K(delta_jbase.element_count()));
  return ret;
}

int ObAiModelEndpointInfo::encrypt_access_key_to_storage_format(ObIAllocator &allocator,
                                                                const ObString &plain_access_key,
                                                                ObString &storage_access_key)
{
  ObAiModelEndpointInfo holder;
  return holder.encrypt_access_key_(allocator, plain_access_key, storage_access_key);
}

int ObAiModelEndpointInfo::encrypt_access_key_(ObIAllocator &allocator, const ObString &access_key, ObString &encrypted_access_key)
{
#ifdef OB_BUILD_TDE_SECURITY
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t out_len = 0;
  int64_t encrypted_buf_length = ObEncryptionUtil::sys_encrypted_length(access_key.length());
  char *encrypted_key_buf = NULL;
  char *encrypted_hex_key_buf = NULL;
  int64_t hex_buf_length = encrypted_buf_length * 2 + 1;
  int64_t hex_buf_pos = 0;

  if (OB_ISNULL(encrypted_key_buf = static_cast<char *>(allocator.alloc(encrypted_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc encrypted key buf", K(ret), K(encrypted_buf_length));
  } else if (OB_FAIL(ObEncryptionUtil::encrypt_sys_data(tenant_id, access_key.ptr(), access_key.length(),
                                                        encrypted_key_buf, encrypted_buf_length, out_len))) {
    LOG_WARN("failed to encrypt access key", K(ret));
  } else if (0 >= out_len || out_len > encrypted_buf_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected encrypted out length", K(ret), K(out_len), K(encrypted_buf_length));
  } else if (OB_ISNULL(encrypted_hex_key_buf = static_cast<char *>(allocator.alloc(hex_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc encrypted hex key buf", K(ret), K(hex_buf_length));
  } else if (OB_FAIL(hex_print(encrypted_key_buf, out_len, encrypted_hex_key_buf, hex_buf_length, hex_buf_pos))) {
    LOG_WARN("failed to convert encrypted key to hex", K(ret), K(encrypted_key_buf), K(hex_buf_length));
  } else {
    encrypted_access_key.assign_ptr(encrypted_hex_key_buf, hex_buf_pos);
  }
  return ret;
#else
  return encrypt_access_key_no_tde_(allocator, access_key, encrypted_access_key);
#endif
}

int ObAiModelEndpointInfo::encrypt_access_key_no_tde_(ObIAllocator &allocator, const ObString &access_key, ObString &encrypted_access_key)
{
  int ret = OB_SUCCESS;
  int64_t hex_buf_length = access_key.length() * 2 + 1;
  int64_t hex_buf_pos = 0;
  char *hex_buf = NULL;
  if (OB_ISNULL(hex_buf = static_cast<char *>(allocator.alloc(hex_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc hex key buf", K(ret), K(hex_buf_length));
  } else if (OB_FAIL(hex_print(access_key.ptr(), access_key.length(), hex_buf, hex_buf_length, hex_buf_pos))) {
    LOG_WARN("failed to convert access key to hex", K(ret), K(access_key), K(hex_buf_length));
  } else {  
    encrypted_access_key.assign_ptr(hex_buf, hex_buf_pos);
  }
  return ret;
}

int ObAiModelEndpointInfo::decrypt_access_key_(ObIAllocator &allocator, const ObString &encrypted_access_key, ObString &unencrypted_access_key) const
{
#ifdef OB_BUILD_TDE_SECURITY
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t deser_buf_length = encrypted_access_key.length() / 2 + 1;
  char *deser_buf = NULL;
  int64_t deser_buf_pos = 0;
  int64_t access_key_buf_length = encrypted_access_key.length() / 2;
  char *access_key_buf = NULL;
  int64_t decrypted_length = 0;
  if (encrypted_access_key.length() % 2 != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid encrypted access key", K(ret), K(encrypted_access_key.length()));
  } else if (OB_ISNULL(deser_buf = static_cast<char *>(allocator.alloc(deser_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc deserialized key buf", K(ret), K(deser_buf_length));
  } else if (OB_FAIL(hex_to_cstr(encrypted_access_key.ptr(), encrypted_access_key.length(),
                                 deser_buf, deser_buf_length, deser_buf_pos))) {
    LOG_WARN("failed to deserialize encrypted key", K(ret), K(encrypted_access_key), K(deser_buf_length));
  } else if (deser_buf_pos != deser_buf_length-1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid encrypted access key", K(ret), K(encrypted_access_key.length()));
  } else if (OB_ISNULL(access_key_buf = static_cast<char *>(allocator.alloc(access_key_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc decrypted key buf", K(ret), K(access_key_buf_length));
  } else if (OB_FAIL(ObEncryptionUtil::decrypt_sys_data(tenant_id, deser_buf, deser_buf_length - 1,
                                                        access_key_buf, access_key_buf_length, decrypted_length))) {
    LOG_WARN("failed to decrypt access key", K(ret));
  } else {
    unencrypted_access_key.assign_ptr(access_key_buf, decrypted_length);
  }
  return ret;
#else
  return decrypt_access_key_no_tde_(allocator, encrypted_access_key, unencrypted_access_key);
#endif
}

int ObAiModelEndpointInfo::decrypt_access_key_no_tde_(ObIAllocator &allocator, const ObString &encrypted_access_key, ObString &unencrypted_access_key) const
{
  int ret = OB_SUCCESS;
  int64_t unencrypted_buf_length = encrypted_access_key.length() / 2 + 1;
  int64_t unencrypted_buf_pos = 0;
  char *unencrypted_buf = NULL;
  if (OB_ISNULL(unencrypted_buf = static_cast<char *>(allocator.alloc(unencrypted_buf_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc unencrypted key buf", K(ret), K(unencrypted_buf_length));
  } else if (OB_FAIL(hex_to_cstr(encrypted_access_key.ptr(), encrypted_access_key.length(),
                                 unencrypted_buf, unencrypted_buf_length, unencrypted_buf_pos))) {
    LOG_WARN("failed to convert encrypted key to unencrypted key", K(ret), K(encrypted_access_key), K(unencrypted_buf_length));
  } else {
    unencrypted_access_key.assign_ptr(unencrypted_buf, unencrypted_buf_pos);
  }
  return ret;
}

const char *EndpointType::ENDPOINT_TYPE_STR[] = {
  "DENSE_EMBEDDING",
  "SPARSE_EMBEDDING",
  "COMPLETION",
  "RERANK",
  "DECISION",
};

EndpointType::TYPE EndpointType::str_to_endpoint_type(const ObString &type_str)
{
  STATIC_ASSERT(static_cast<int64_t>(EndpointType::MAX_TYPE) == ARRAYSIZEOF(ENDPOINT_TYPE_STR) + 1, "endpoint type str len is mismatch");
  EndpointType::TYPE endpoint_type = EndpointType::INVALID_TYPE;
  bool is_found = false;
  for (int i = 1; i < EndpointType::MAX_TYPE && !is_found; i++) {
    if (type_str.case_compare(ENDPOINT_TYPE_STR[i-1]) == 0) {
      endpoint_type = static_cast<EndpointType::TYPE>(i);
      is_found = true;
    }
  }
  return endpoint_type;
}

int ObAiModelEndpointInfo::get_unencrypted_access_key(common::ObIAllocator &allocator, ObString &unencrypted_access_key) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(decrypt_access_key_(allocator, access_key_, unencrypted_access_key))) {
    LOG_WARN("failed to decrypt access key", K(ret));
  }
  return ret;
}

int ObAiModelEndpointInfo::assign_storage_access_key_only(common::ObIAllocator &allocator,
                                                        const ObString &encrypted_access_key_from_table)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ob_write_string(allocator, encrypted_access_key_from_table, access_key_, true))) {
    LOG_WARN("failed to copy encrypted access key", K(ret));
  }
  return ret;
}

int ObAiModelEndpointInfo::deep_copy(common::ObIAllocator &allocator, const ObAiModelEndpointInfo &other)
{
  int ret = OB_SUCCESS;
  reset();
  endpoint_id_ = other.endpoint_id_;
  if (OB_FAIL(ob_write_string(allocator, other.name_, name_))) {
    LOG_WARN("failed to copy name", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.scope_, scope_))) {
    LOG_WARN("failed to copy scope", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.ai_model_name_, ai_model_name_))) {
    LOG_WARN("failed to copy ai_model_name", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.url_, url_))) {
    LOG_WARN("failed to copy url", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.access_key_, access_key_))) {
    LOG_WARN("failed to copy access_key", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.provider_, provider_))) {
    LOG_WARN("failed to copy provider", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.request_model_name_, request_model_name_))) {
    LOG_WARN("failed to copy request_model_name", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.parameters_, parameters_))) {
    LOG_WARN("failed to copy parameters", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.request_transform_fn_, request_transform_fn_))) {
    LOG_WARN("failed to copy request_transform_fn", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.response_transform_fn_, response_transform_fn_))) {
    LOG_WARN("failed to copy response_transform_fn", K(ret));
  }
  return ret;
}

int ObAiServiceModelInfo::parse_from_json_base(const ObString &name, const common::ObIJsonBase &params_jbase)
{
  int ret = OB_SUCCESS;
  reset();
  name_ = name;
  JsonObjectIterator iter = params_jbase.object_iterator();
  ObString type_str;
  while (!iter.end() && OB_SUCC(ret)) {
    ObJsonObjPair elem;
    if (OB_FAIL(iter.get_elem(elem))) {
      LOG_WARN("failed to get elem", K(ret));
    } else {
      EXTRACT_JSON_ELEM_STR("model_name", model_name_)
      EXTRACT_JSON_ELEM_STR_WITH_PROCESS("type", type_str, type_ = EndpointType::str_to_endpoint_type(type_str))
      EXTRACT_JSON_ELEM_END()
    }
    iter.next();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_valid())) {
      LOG_WARN("invalid model", K(ret), K(params_jbase));
    }
  }

  LOG_TRACE("parse from json base", K(ret), K(params_jbase), K(params_jbase.json_type()), K(params_jbase.element_count()));
  return ret;
}

int ObAiServiceModelInfo::check_valid() const
{
  int ret = OB_SUCCESS;
  if (name_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("name"), "name");
    LOG_WARN("name is empty", K(ret), K(*this));
  } else if (model_name_.empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("model_name"), "model_name");
    LOG_WARN("model_name is empty", K(ret), K(*this));
  } else if (type_ == EndpointType::MAX_TYPE) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, strlen("type"), "type");
    LOG_WARN("model type is empty", K(ret), K(*this), K(type_));
  } else if (type_ == EndpointType::INVALID_TYPE) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID, strlen("type"), "type");
    LOG_WARN("model type is invalid", K(ret), K(*this), K(type_));
  }
  return ret;
}

void ObAIModelConfigInfo::release_gw_state_()
{
  ObAiGatewayCircuitState::dec_ref_and_release(gw_state_);
  gw_state_ = nullptr;
}

int ObAIModelConfigInfo::init(ObIAllocator &allocator,
                              const schema::ObAiModelSchema &ai_model_schema,
                              const ObAiModelEndpointInfo &endpoint_info)
{
  int ret = OB_SUCCESS;
  reset();
  const ObString request_model_name = endpoint_info.get_request_model_name().empty()
                                      ? ai_model_schema.get_model_name()
                                      : endpoint_info.get_request_model_name();
  if (OB_FAIL(ob_write_string(allocator, ai_model_schema.get_name(), model_key_, true))) {
    LOG_WARN("failed to deep copy model key", K(ret), K(ai_model_schema));
  } else if (OB_FAIL(ob_write_string(allocator, ai_model_schema.get_model_name(), model_name_, true))) {
    LOG_WARN("failed to deep copy model name", K(ret), K(ai_model_schema));
  } else if (OB_FAIL(ob_write_string(allocator, endpoint_info.get_provider(), provider_, true))) {
    LOG_WARN("failed to deep copy provider", K(ret), K(endpoint_info));
  } else if (OB_FAIL(ob_write_string(allocator, endpoint_info.get_url(), url_, true))) {
    LOG_WARN("failed to deep copy endpoint url", K(ret), K(endpoint_info));
  } else if (OB_FAIL(ob_write_string(allocator, request_model_name, request_model_name_, true))) {
    LOG_WARN("failed to deep copy request model name", K(ret), K(request_model_name));
  } else if (OB_FAIL(endpoint_info.get_unencrypted_access_key(allocator, api_key_))) {
    LOG_WARN("failed to get unencrypted access key", K(ret), K(endpoint_info));
  } else if (OB_FALSE_IT(model_type_ = ai_model_schema.get_type())) {
  } else {
    const ObString &parameters = endpoint_info.get_parameters();
    if (!parameters.empty()) {
      ObIJsonBase *params_base = nullptr;
      if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                    parameters,
                                                    ObJsonInType::JSON_TREE,
                                                    ObJsonInType::JSON_TREE,
                                                    params_base))) {
        LOG_WARN("failed to parse endpoint parameters", K(ret), K(parameters));
      } else if (OB_ISNULL(params_base) || params_base->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("endpoint parameters should be json object", K(ret), K(parameters));
      } else {
        // get endpoint parameters
        JsonObjectIterator iter = params_base->object_iterator();
        while (!iter.end() && OB_SUCC(ret)) {
          ObJsonObjPair elem;
          if (OB_FAIL(iter.get_elem(elem))) {
            LOG_WARN("failed to get elem", K(ret));
          } else {
            EXTRACT_JSON_ELEM_INT("batch_size", batch_size_)
            EXTRACT_JSON_ELEM_INT("max_image_size", max_image_size_)
            EXTRACT_JSON_ELEM_INT("min_concurrency", min_concurrency_)
            EXTRACT_JSON_ELEM_INT("max_concurrency", max_concurrency_)
            EXTRACT_JSON_ELEM_NO_CHECK_END()
          }
          iter.next();
        }
        // get message parameters
        ObJsonObject *message_parameters = static_cast<ObJsonObject *>(params_base);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(message_parameters->remove("batch_size"))) {
            LOG_WARN("failed to remove batch size", K(ret));
          } else if (OB_FAIL(message_parameters->remove("max_image_size"))) {
            LOG_WARN("failed to remove max image size", K(ret));
          } else if (OB_FAIL(message_parameters->remove("min_concurrency"))) {
            LOG_WARN("failed to remove min concurrency", K(ret));
          } else if (OB_FAIL(message_parameters->remove("max_concurrency"))) {
            LOG_WARN("failed to remove max concurrency", K(ret));
          } else {
            message_parameters_ = message_parameters;
          }
        }
      }
    }
  }
  return ret;
}

int ObAIModelConfigInfo::init_from_inline_provider_model(common::ObIAllocator &allocator,
                                                         const common::ObString &inline_model_key,
                                                         const schema::ObAIProviderSchema &provider_schema,
                                                         const common::ObString &request_model_name,
                                                         const EndpointType::TYPE model_type,
                                                         const common::ObString &dispatch_provider_tag,
                                                         const common::ObString &full_service_url)
{
  int ret = OB_SUCCESS;
  reset();
  ObAiModelEndpointInfo key_holder;
  if (provider_schema.get_access_key().empty()) {
    ret = OB_AI_FUNC_PARAM_EMPTY;
    const ObString var_name("access_key");
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_EMPTY, var_name.length(), var_name.ptr());
    LOG_WARN("provider access_key is empty, register provider first", K(ret), K(inline_model_key));
  } else if (OB_FAIL(ob_write_string(allocator, inline_model_key, model_key_, true))) {
    LOG_WARN("failed to deep copy model key", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, request_model_name, model_name_, true))) {
    LOG_WARN("failed to deep copy model name", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, request_model_name, request_model_name_, true))) {
    LOG_WARN("failed to deep copy request model name", K(ret));
  } else if (OB_FALSE_IT(model_type_ = model_type)) {
  } else if (OB_FAIL(ob_write_string(allocator, dispatch_provider_tag, provider_, true))) {
    LOG_WARN("failed to deep copy dispatch provider", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, full_service_url, url_, true))) {
    LOG_WARN("failed to deep copy url", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, provider_schema.get_base_url(), provider_base_url_, true))) {
    LOG_WARN("failed to deep copy provider base url", K(ret));
  } else if (OB_FAIL(key_holder.assign_storage_access_key_only(allocator, provider_schema.get_access_key()))) {
    LOG_WARN("failed to assign encrypted access key", K(ret));
  } else if (OB_FAIL(key_holder.get_unencrypted_access_key(allocator, api_key_))) {
    LOG_WARN("failed to decrypt access key", K(ret));
  }
  return ret;
}

int ObAIModelConfigInfo::apply_profile_params(ObIAllocator &allocator,
                                              const ObString &model_config,
                                              const ObString &run_config)
{
  int ret = OB_SUCCESS;
  extra_headers_ = nullptr;
  if (!run_config.empty()) {
    // JSON string nodes reference the input buffer, so keep the storage with extra_headers_.
    ObString options_copy;
    ObIJsonBase *options_base = nullptr;
    if (OB_FAIL(ob_write_string(allocator, run_config, options_copy))) {
      LOG_WARN("failed to copy run_config", K(ret));
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                        options_copy,
                                                        ObJsonInType::JSON_TREE,
                                                        ObJsonInType::JSON_TREE,
                                                        options_base))) {
      LOG_WARN("failed to parse run_config", K(ret), K(run_config.length()));
    } else if (OB_ISNULL(options_base) || options_base->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("run_config should be json object", K(ret), K(run_config.length()));
    } else {
      int64_t batch_size = 0;
      int64_t max_image_size = 0;
      int64_t min_concurrency = 0;
      int64_t max_concurrency = 0;
      JsonObjectIterator iter = options_base->object_iterator();
      while (!iter.end() && OB_SUCC(ret)) {
        ObJsonObjPair elem;
        if (OB_FAIL(iter.get_elem(elem))) {
          LOG_WARN("failed to get elem", K(ret));
        } else {
          EXTRACT_JSON_ELEM_INT("batch_size", batch_size)
          EXTRACT_JSON_ELEM_INT("max_image_size", max_image_size)
          EXTRACT_JSON_ELEM_INT("min_concurrency", min_concurrency)
          EXTRACT_JSON_ELEM_INT("max_concurrency", max_concurrency)
          if (0 == elem.first.case_compare("extra_headers")) {
            if (elem.second->json_type() != ObJsonNodeType::J_OBJECT) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("extra_headers should be json object", K(ret));
            } else {
              extra_headers_ = static_cast<const ObJsonObject *>(elem.second);
            }
          } else
          EXTRACT_JSON_ELEM_NO_CHECK_END()
        }
        iter.next();
      }
      if (OB_SUCC(ret)) {
        if (batch_size > 0) { batch_size_ = batch_size; }
        if (max_image_size > 0) { max_image_size_ = max_image_size; }
        if (min_concurrency > 0) { min_concurrency_ = min_concurrency; }
        if (max_concurrency > 0) { max_concurrency_ = max_concurrency; }
      }
    }
  }
  if (OB_SUCC(ret) && !model_config.empty()) {
    // deep-copy model_config into allocator so the JSON tree's ObString nodes
    // reference allocator-owned memory and do not dangle after the caller's
    // temporary profile_allocator is destroyed
    ObString config_copy;
    ObIJsonBase *params_base = nullptr;
    if (OB_FAIL(ob_write_string(allocator, model_config, config_copy))) {
      LOG_WARN("failed to copy model_config", K(ret));
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                         config_copy,
                                                         ObJsonInType::JSON_TREE,
                                                         ObJsonInType::JSON_TREE,
                                                         params_base))) {
      LOG_WARN("failed to parse model_config", K(ret), K(config_copy));
    } else if (OB_ISNULL(params_base) || params_base->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("model_config should be json object", K(ret), K(config_copy));
    } else {
      message_parameters_ = static_cast<ObJsonObject *>(params_base);
    }
  }
  return ret;
}

int ObAIModelConfigInfo::merge_default_config(ObIAllocator &allocator, const share::ObAIModelConfigItem &default_config)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  batch_size_ = batch_size_ == 0 ? default_config.batch_size_ : batch_size_;
  max_image_size_ = max_image_size_ == 0 ? default_config.max_image_size_ : max_image_size_;
  min_concurrency_ = min_concurrency_ == 0 ? default_config.min_concurrency_ : min_concurrency_;
  max_concurrency_ = max_concurrency_ == 0 ? default_config.max_concurrency_ : max_concurrency_;
  return ret;
}

OB_SERIALIZE_MEMBER(ObAiServiceModelInfo, name_, type_, model_name_);

int parse_gateway_endpoints_json(common::ObIAllocator &allocator,
                                 const common::ObString &json_str,
                                 common::ObIArray<ObAiGatewayEndpoint> &endpoints)
{
  int ret = OB_SUCCESS;
  endpoints.reset();
  ObIJsonBase *j_base = nullptr;
  if (json_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("endpoints json string is empty", K(ret));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                       json_str,
                                                       ObJsonInType::JSON_TREE,
                                                       ObJsonInType::JSON_TREE,
                                                       j_base))) {
    LOG_WARN("failed to parse endpoints json", K(ret), K(json_str));
  } else if (OB_ISNULL(j_base) || j_base->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("endpoints json should be array", K(ret), K(json_str));
  } else {
    uint64_t ep_count = j_base->element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < ep_count; ++i) {
      ObIJsonBase *ep = nullptr;
      if (OB_FAIL(j_base->get_array_element(i, ep))) {
        LOG_WARN("failed to get array element", K(ret), K(i));
      } else if (OB_ISNULL(ep) || ep->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("endpoint element should be json object", K(ret), K(i));
      } else {
        ObAiGatewayEndpoint endpoint;
        ObString raw_name;
        ObString raw_model;
        JsonObjectIterator iter = ep->object_iterator();
        while (OB_SUCC(ret) && !iter.end()) {
          ObJsonObjPair elem;
          if (OB_FAIL(iter.get_elem(elem))) {
            LOG_WARN("failed to get endpoint element", K(ret));
          } else if (0 == elem.first.case_compare("name")) {
            if (elem.second->json_type() != ObJsonNodeType::J_STRING) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("endpoint name should be string", K(ret));
            } else {
              raw_name = ObString(elem.second->get_data_length(), elem.second->get_data());
            }
          } else if (0 == elem.first.case_compare("model")) {
            if (elem.second->json_type() != ObJsonNodeType::J_STRING) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("endpoint model should be string", K(ret));
            } else {
              raw_model = ObString(elem.second->get_data_length(), elem.second->get_data());
            }
          } else if (0 == elem.first.case_compare("weight")) {
            if (elem.second->json_type() != ObJsonNodeType::J_INT
                && elem.second->json_type() != ObJsonNodeType::J_UINT) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("endpoint weight should be integer", K(ret));
            } else {
              endpoint.weight_ = elem.second->get_int();
            }
          } else {
            // ignore unknown keys
          }
          if (OB_SUCC(ret)) { iter.next(); }
        }
        // validate required fields
        if (OB_FAIL(ret)) {
        } else if (raw_name.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("endpoint name is empty", K(ret), K(i));
        } else if (raw_model.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("endpoint model is empty", K(ret), K(i));
        } else {
          // split model by '/' to get provider and model_name
          const char *slash_ptr = static_cast<const char *>(MEMCHR(raw_model.ptr(), '/', raw_model.length()));
          if (OB_ISNULL(slash_ptr)
              || slash_ptr == raw_model.ptr()
              || slash_ptr == raw_model.ptr() + raw_model.length() - 1) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("endpoint model format invalid, expected provider/model_name", K(ret), K(raw_model));
          } else {
            int32_t provider_len = static_cast<int32_t>(slash_ptr - raw_model.ptr());
            int32_t model_name_len = static_cast<int32_t>(raw_model.length() - provider_len - 1);
            ObString raw_provider(provider_len, raw_model.ptr());
            ObString raw_model_name(model_name_len, slash_ptr + 1);
            // deep copy all strings
            if (OB_FAIL(ob_write_string(allocator, raw_name, endpoint.endpoint_name_, true))) {
              LOG_WARN("failed to deep copy endpoint name", K(ret), K(raw_name));
            } else if (OB_FAIL(ob_write_string(allocator, raw_model, endpoint.model_, true))) {
              LOG_WARN("failed to deep copy endpoint model", K(ret), K(raw_model));
            } else if (OB_FAIL(ob_write_string(allocator, raw_provider, endpoint.provider_, true))) {
              LOG_WARN("failed to deep copy endpoint provider", K(ret), K(raw_provider));
            } else if (OB_FAIL(ob_write_string(allocator, raw_model_name, endpoint.model_name_, true))) {
              LOG_WARN("failed to deep copy endpoint model_name", K(ret), K(raw_model_name));
            } else if (OB_FAIL(endpoints.push_back(endpoint))) {
              LOG_WARN("failed to push endpoint", K(ret), K(endpoint));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAiCircuitBreakerParams::validate_ranges(int64_t failure_rate_threshold,
                                              int64_t window_size_seconds,
                                              int64_t minimum_requests,
                                              int64_t break_duration_seconds,
                                              int64_t probe_requests)
{
  int ret = OB_SUCCESS;
  if (failure_rate_threshold < MIN_FAILURE_RATE_THRESHOLD
      || failure_rate_threshold > MAX_FAILURE_RATE_THRESHOLD) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID,
                   (int)strlen("failure_rate_threshold"), "failure_rate_threshold");
  } else if (window_size_seconds < MIN_WINDOW_SIZE_SECONDS
             || window_size_seconds > MAX_WINDOW_SIZE_SECONDS) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID,
                   (int)strlen("window_size_seconds"), "window_size_seconds");
  } else if (minimum_requests < MIN_MINIMUM_REQUESTS
             || minimum_requests > MAX_MINIMUM_REQUESTS) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID,
                   (int)strlen("minimum_requests"), "minimum_requests");
  } else if (break_duration_seconds < MIN_BREAK_DURATION_SECONDS
             || break_duration_seconds > MAX_BREAK_DURATION_SECONDS) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID,
                   (int)strlen("break_duration_seconds"), "break_duration_seconds");
  } else if (probe_requests < MIN_PROBE_REQUESTS
             || probe_requests > MAX_PROBE_REQUESTS) {
    ret = OB_AI_FUNC_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_AI_FUNC_PARAM_VALUE_INVALID,
                   (int)strlen("probe_requests"), "probe_requests");
  }
  return ret;
}

int parse_gateway_circuit_breaker_json(common::ObIAllocator &allocator,
                                       const common::ObString &json_str,
                                       ObAiCircuitBreakerParams &params)
{
  int ret = OB_SUCCESS;
  params.set_defaults();
  if (json_str.empty()) {
    // empty string means use all defaults
  } else {
    ObIJsonBase *j_base = nullptr;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                                                  json_str,
                                                  ObJsonInType::JSON_TREE,
                                                  ObJsonInType::JSON_TREE,
                                                  j_base))) {
      LOG_WARN("failed to parse circuit_breaker json", K(ret), K(json_str));
    } else if (OB_ISNULL(j_base) || j_base->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("circuit_breaker json should be object", K(ret), K(json_str));
    } else {
      JsonObjectIterator iter = j_base->object_iterator();
      while (OB_SUCC(ret) && !iter.end()) {
        ObJsonObjPair elem;
        if (OB_FAIL(iter.get_elem(elem))) {
          LOG_WARN("failed to get circuit_breaker element", K(ret));
        } else if (0 == elem.first.case_compare("failure_rate_threshold")) {
          if (elem.second->json_type() != ObJsonNodeType::J_INT
              && elem.second->json_type() != ObJsonNodeType::J_UINT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("failure_rate_threshold should be integer", K(ret));
          } else {
            params.failure_rate_threshold_ = elem.second->get_int();
          }
        } else if (0 == elem.first.case_compare("window_size_seconds")) {
          if (elem.second->json_type() != ObJsonNodeType::J_INT
              && elem.second->json_type() != ObJsonNodeType::J_UINT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("window_size_seconds should be integer", K(ret));
          } else {
            params.window_size_seconds_ = elem.second->get_int();
          }
        } else if (0 == elem.first.case_compare("minimum_requests")) {
          if (elem.second->json_type() != ObJsonNodeType::J_INT
              && elem.second->json_type() != ObJsonNodeType::J_UINT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("minimum_requests should be integer", K(ret));
          } else {
            params.minimum_requests_ = elem.second->get_int();
          }
        } else if (0 == elem.first.case_compare("break_duration_seconds")) {
          if (elem.second->json_type() != ObJsonNodeType::J_INT
              && elem.second->json_type() != ObJsonNodeType::J_UINT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("break_duration_seconds should be integer", K(ret));
          } else {
            params.break_duration_seconds_ = elem.second->get_int();
          }
        } else if (0 == elem.first.case_compare("probe_requests")) {
          if (elem.second->json_type() != ObJsonNodeType::J_INT
              && elem.second->json_type() != ObJsonNodeType::J_UINT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("probe_requests should be integer", K(ret));
          } else {
            params.probe_requests_ = elem.second->get_int();
          }
        } else {
          ret = OB_AI_FUNC_PARAM_INVALID;
          LOG_USER_ERROR(OB_AI_FUNC_PARAM_INVALID, elem.first.length(), elem.first.ptr());
          LOG_WARN("unknown circuit_breaker param", K(ret), K(elem.first));
        }
        if (OB_SUCC(ret)) { iter.next(); }
      }
      // Defense-in-depth: validate ranges (DDL layer should have caught these,
      // but guard against internal callers or corrupted stored data).
      if (OB_SUCC(ret)
          && OB_FAIL(ObAiCircuitBreakerParams::validate_ranges(
                 params.failure_rate_threshold_,
                 params.window_size_seconds_,
                 params.minimum_requests_,
                 params.break_duration_seconds_,
                 params.probe_requests_))) {
        LOG_WARN("circuit breaker param out of range", K(ret), K(params));
      }
    }
  }
  return ret;
}

int merge_gateway_circuit_breaker_json(common::ObIAllocator &allocator,
                                       const common::ObString &old_json,
                                       const common::ObString &new_json,
                                       common::ObString &merged_json)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *old_base = nullptr;
  ObIJsonBase *new_base = nullptr;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, old_json,
          ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, old_base))) {
    LOG_WARN("failed to parse old circuit_breaker json", K(ret), K(old_json));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, new_json,
          ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, new_base))) {
    LOG_WARN("failed to parse new circuit_breaker json", K(ret), K(new_json));
  } else if (OB_ISNULL(old_base) || OB_ISNULL(new_base)
             || old_base->json_type() != ObJsonNodeType::J_OBJECT
             || new_base->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("circuit_breaker json should be object", K(ret), K(old_json), K(new_json));
  } else {
    // RFC 7396 merge: old_obj as base, new_obj overlays; unspecified fields kept.
    ObJsonObject *merged_obj = static_cast<ObJsonObject *>(old_base);
    ObJsonBuffer buf(&allocator);
    ObAiCircuitBreakerParams params;
    if (OB_FAIL(merged_obj->merge_patch(&allocator, static_cast<ObJsonObject *>(new_base)))) {
      LOG_WARN("failed to merge circuit_breaker json", K(ret));
    } else if (OB_FAIL(merged_obj->print(buf, false))) {
      LOG_WARN("failed to serialize merged circuit_breaker json", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, ObString(buf.length(), buf.ptr()), merged_json))) {
      LOG_WARN("failed to copy merged circuit_breaker", K(ret));
    } else if (OB_FAIL(parse_gateway_circuit_breaker_json(allocator, merged_json, params))) {
      // Re-validate ranges so M1 bounds cannot be bypassed via a preserved old field.
      LOG_WARN("merged circuit_breaker is invalid", K(ret), K(merged_json));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
