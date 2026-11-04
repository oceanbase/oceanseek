/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <string>
#include <gtest/gtest.h>
#define private public
#include "share/ai_service/ob_ai_service_struct.h"
#undef private
#include "lib/json/ob_json.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace unittest
{

class ToggleAllocator : public ObIAllocator
{
public:
  ToggleAllocator() : allocator_("TestAIFail"), fail_(false) {}
  virtual ~ToggleAllocator() {}
  void *alloc(const int64_t size) override
  {
    return fail_ ? nullptr : allocator_.alloc(size);
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    return fail_ ? nullptr : allocator_.alloc(size, attr);
  }
  void free(void *ptr) override
  {
    allocator_.free(ptr);
  }
  void set_fail(const bool fail)
  {
    fail_ = fail;
  }

private:
  ObArenaAllocator allocator_;
  bool fail_;
};

class TestAiServiceStruct : public ::testing::Test
{
public:
  void init_valid_endpoint(ObAiModelEndpointInfo &endpoint, const char *parameters = "")
  {
    endpoint.reset();
    endpoint.name_ = ObString::make_string("ep1");
    endpoint.scope_ = ObAiModelEndpointInfo::DEFAULT_SCOPE;
    endpoint.ai_model_name_ = ObString::make_string("model1");
    endpoint.url_ = ObString::make_string("https://example.com/v1/chat/completions");
    endpoint.access_key_ = ObString::make_string("mock_access_key");
    endpoint.provider_ = ObString::make_string("OPENAI");
    endpoint.parameters_ = ObString::make_string(parameters);
    endpoint.request_transform_fn_.reset();
    endpoint.response_transform_fn_.reset();
  }

  int build_json_base(ObArenaAllocator &allocator, const char *json_text, ObIJsonBase *&jbase)
  {
    ObString json_str = ObString::make_string(json_text);
    return ObJsonBaseFactory::get_json_base(&allocator,
                                            json_str,
                                            ObJsonInType::JSON_TREE,
                                            ObJsonInType::JSON_TREE,
                                            jbase);
  }

  int build_json_base(ObArenaAllocator &allocator,
                      const ObString &json_text,
                      const ObJsonInType output_type,
                      ObIJsonBase *&jbase)
  {
    return ObJsonBaseFactory::get_json_base(&allocator,
                                            json_text,
                                            ObJsonInType::JSON_TREE,
                                            output_type,
                                            jbase);
  }

  int validate_headers(ObArenaAllocator &allocator,
                       const ObString &json_text,
                       ObJsonBuffer &buf,
                       const ObJsonInType output_type = ObJsonInType::JSON_TREE)
  {
    int ret = OB_SUCCESS;
    ObIJsonBase *jbase = nullptr;
    if (OB_FAIL(build_json_base(allocator, json_text, output_type, jbase))) {
    } else if (nullptr == jbase) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = ObAIExtraHeadersUtils::validate_and_serialize(*jbase, buf);
    }
    return ret;
  }

  int alter_endpoint_parameters(const char *delta_json, ObAiModelEndpointInfo &endpoint)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator("TestAIEP");
    ObIJsonBase *delta_jbase = nullptr;
    if (OB_FAIL(build_json_base(allocator, delta_json, delta_jbase))) {
      return ret;
    }
    if (nullptr == delta_jbase) {
      return OB_ERR_UNEXPECTED;
    }
    ret = endpoint.merge_delta_endpoint(allocator, *delta_jbase);
    return ret;
  }
};

TEST_F(TestAiServiceStruct, supported_provider_protocols_include_vllm)
{
  ASSERT_TRUE(is_supported_ai_provider_protocol(ObString::make_string("vllm")));
  ASSERT_TRUE(is_supported_ai_provider_protocol(ObString::make_string("VLLM")));
  ASSERT_FALSE(is_supported_ai_provider_protocol(ObString::make_string("unknown")));
}

TEST_F(TestAiServiceStruct, supports_typesafe_decision_metadata)
{
  ASSERT_TRUE(is_supported_ai_provider_protocol(ObString::make_string("typesafe")));
  ASSERT_TRUE(is_supported_ai_provider_protocol(ObString::make_string("TypeSafe")));
  ASSERT_EQ(5, static_cast<int>(EndpointType::str_to_endpoint_type(
                   ObString::make_string("decision"))));
  ASSERT_EQ(3, static_cast<int>(EndpointType::str_to_endpoint_type(
                   ObString::make_string("completion"))));
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint);
  endpoint.provider_ = ObString::make_string("typesafe");
  endpoint.url_ = ObString::make_string("http://127.0.0.1:28880/v1/systemone");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
}

TEST_F(TestAiServiceStruct, legacy_model_endpoint_accepts_vllm_provider)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint);
  endpoint.provider_ = ObString::make_string("vllm");
  endpoint.url_ = ObString::make_string("https://example.com/v1/embeddings");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
}

// check_valid(): create path
TEST_F(TestAiServiceStruct, check_valid_positive_cases)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint, "{\"batch_size\":8,\"max_image_size\":2048}");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
  init_valid_endpoint(endpoint, "{\"batch_size\":1}");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
  init_valid_endpoint(endpoint, "{\"max_image_size\":4096}");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
  init_valid_endpoint(endpoint, "");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
  // temperature: current design does not validate, should pass
  init_valid_endpoint(endpoint, "{\"temperature\":1}");
  ASSERT_EQ(OB_SUCCESS, endpoint.check_valid());
}

TEST_F(TestAiServiceStruct, check_valid_reject_type_or_format_errors)
{
  ObAiModelEndpointInfo endpoint;
  // value must be integer, string is rejected
  init_valid_endpoint(endpoint, "{\"batch_size\":\"8\"}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // value must be integer, float is rejected
  init_valid_endpoint(endpoint, "{\"max_image_size\":3.14}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // value must be integer, bool is rejected
  init_valid_endpoint(endpoint, "{\"batch_size\":true}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // parameters must be a JSON object
  init_valid_endpoint(endpoint, "[1,2,3]");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // malformed JSON text in parameters string
  init_valid_endpoint(endpoint, "{\"batch_size\":16");
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT, endpoint.check_valid());
}

TEST_F(TestAiServiceStruct, check_valid_reject_value_or_key_errors)
{
  ObAiModelEndpointInfo endpoint;
  // batch_size must be non-negative
  init_valid_endpoint(endpoint, "{\"batch_size\":-1}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
  // max_image_size must be non-negative
  init_valid_endpoint(endpoint, "{\"max_image_size\":-1024}");
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID, endpoint.check_valid());
}

// merge_delta_endpoint(): alter path
TEST_F(TestAiServiceStruct, merge_delta_endpoint_positive_cases)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint, "");
  ASSERT_EQ(OB_SUCCESS, alter_endpoint_parameters("{\"parameters\":\"{\\\"batch_size\\\":16}\"}", endpoint));
  ASSERT_EQ(0, endpoint.get_parameters().compare("{\"batch_size\":16}"));
  ASSERT_EQ(OB_SUCCESS,
            alter_endpoint_parameters("{\"parameters\":\"{\\\"batch_size\\\":8,\\\"max_image_size\\\":1024}\"}", endpoint));
  ASSERT_EQ(0, endpoint.get_parameters().compare("{\"batch_size\":8,\"max_image_size\":1024}"));
}

TEST_F(TestAiServiceStruct, merge_delta_endpoint_reject_type_or_format_errors)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint, "");
  // inner parameters JSON has string type for integer field
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters(R"({"parameters":"{\"batch_size\":\"16\"}"})", endpoint));
  // inner parameters must be object, not array
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters(R"({"parameters":"[1,2]"})", endpoint));
  // inner parameters JSON text is malformed
  ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT,
            alter_endpoint_parameters("{\"parameters\":\"{\\\"batch_size\\\":16\"}", endpoint));
  // outer parameters field must be string in alter payload
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters("{\"parameters\":{\"batch_size\":16}}", endpoint));
}

TEST_F(TestAiServiceStruct, merge_delta_endpoint_reject_value_or_key_errors)
{
  ObAiModelEndpointInfo endpoint;
  init_valid_endpoint(endpoint, "");
  // temperature: current design does not validate, should pass
  ASSERT_EQ(OB_SUCCESS,
            alter_endpoint_parameters(R"({"parameters":"{\"temperature\":1}"})", endpoint));
  // negative batch_size is invalid
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters(R"({"parameters":"{\"batch_size\":-1}"})", endpoint));
  // negative max_image_size is invalid
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            alter_endpoint_parameters(R"({"parameters":"{\"max_image_size\":-1024}"})", endpoint));
}

TEST_F(TestAiServiceStruct, extra_headers_normalize_and_serialize)
{
  ObArenaAllocator allocator("TestAIHeader");
  ObJsonBuffer buf(&allocator);
  const char *headers =
      R"({"X-DashScope-Wait-Timeout":300,"OpenAI-Project":" \tp1\t ",)"
      R"("appid":true,"x-tc-empty":"","X-TC-Data":{"b":[null,1.25,"\u007f"]}})";
  ASSERT_EQ(OB_SUCCESS, validate_headers(allocator, ObString::make_string(headers), buf));
  const std::string normalized(buf.ptr(), buf.length());
  ASSERT_NE(std::string::npos,
            normalized.find(R"("X-DashScope-Wait-Timeout":300)"));
  ASSERT_NE(std::string::npos, normalized.find(R"("OpenAI-Project":"p1")"));
  ASSERT_NE(std::string::npos, normalized.find(R"("appid":true)"));
  ASSERT_NE(std::string::npos, normalized.find(R"("x-tc-empty":"")"));
  ASSERT_NE(std::string::npos,
            normalized.find(R"("X-TC-Data":{"b":[null,1.25,"\u007f"]})"));

  ObJsonBuffer bin_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS,
            validate_headers(allocator,
                             ObString::make_string(headers),
                             bin_buf,
                             ObJsonInType::JSON_BIN));
  ASSERT_EQ(0, bin_buf.string().compare(buf.string()));
}

TEST_F(TestAiServiceStruct, extra_headers_json_representation_parity)
{
  ObArenaAllocator allocator("TestAIHeader");
  const ObString valid = ObString::make_string(
      R"({"X-TC-Empty":"","X-TC-Data":{"":{},"k":"","\u007f":"\u007f",)"
      R"("arr":[{},[],"\u007f",null,true,1]}})");
  ObJsonBuffer tree_buf(&allocator);
  ObJsonBuffer bin_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, validate_headers(allocator, valid, tree_buf));
  ASSERT_EQ(OB_SUCCESS,
            validate_headers(allocator, valid, bin_buf, ObJsonInType::JSON_BIN));
  ASSERT_EQ(0, tree_buf.string().compare(bin_buf.string()));
  const char *expected =
      R"({"X-TC-Data":{"":{},"k":"","\u007f":"\u007f",)"
      R"("arr":[{},[],"\u007f",null,true,1]},"X-TC-Empty":""})";
  ASSERT_EQ(std::string(expected),
            std::string(tree_buf.ptr(), tree_buf.length()));

  const ObString invalid = ObString::make_string(R"({"X-TC-A":"a\rb"})");
  ObJsonBuffer invalid_tree_buf(&allocator);
  ObJsonBuffer invalid_bin_buf(&allocator);
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            validate_headers(allocator, invalid, invalid_tree_buf));
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            validate_headers(
                allocator, invalid, invalid_bin_buf, ObJsonInType::JSON_BIN));
}

TEST_F(TestAiServiceStruct, extra_headers_json_scalar_types)
{
  ObArenaAllocator allocator("TestAIHeader");
  number::ObNumber number;
  ASSERT_EQ(OB_SUCCESS, number.from("1.25", allocator));
  ObJsonInt json_int(-7);
  ObJsonUint json_uint(8);
  ObJsonDecimal json_decimal(number);
  ObJsonDouble json_double(2.5);
  ObJsonBoolean json_true(true);
  ObJsonBoolean json_false(false);
  ObJsonNull json_null;
  ObJsonObject headers(&allocator);
  ASSERT_EQ(OB_SUCCESS, headers.add("X-TC-Int", &json_int));
  ASSERT_EQ(OB_SUCCESS, headers.add("X-TC-Uint", &json_uint));
  ASSERT_EQ(OB_SUCCESS, headers.add("X-TC-Decimal", &json_decimal));
  ASSERT_EQ(OB_SUCCESS, headers.add("X-TC-Double", &json_double));
  ASSERT_EQ(OB_SUCCESS, headers.add("X-TC-True", &json_true));
  ASSERT_EQ(OB_SUCCESS, headers.add("X-TC-False", &json_false));
  ASSERT_EQ(OB_SUCCESS, headers.add("X-TC-Null", &json_null));
  ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS, ObAIExtraHeadersUtils::validate_and_serialize(headers, buf));
  const char *expected =
      R"({"X-TC-Int":-7,"X-TC-Null":null,"X-TC-True":true,"X-TC-Uint":8,)"
      R"("X-TC-False":false,"X-TC-Double":2.5,"X-TC-Decimal":1.25})";
  ASSERT_EQ(std::string(expected), std::string(buf.ptr(), buf.length()));
}

TEST_F(TestAiServiceStruct, extra_headers_json_rejected_types)
{
  ObArenaAllocator allocator("TestAIHeader");
  ObTime time;
  ObJsonDatetime json_date(time, ObDateType);
  ObJsonDatetime json_time(time, ObTimeType);
  ObJsonDatetime json_datetime(time, ObDateTimeType);
  ObJsonDatetime json_timestamp(time, ObTimestampType);
  ObJsonOpaque json_opaque(ObString::make_string("opaque"), ObVarcharType);
  ObJsonNull json_non_real_null(true);
  ObJsonNode *invalid_values[] = {
      &json_date,
      &json_time,
      &json_datetime,
      &json_timestamp,
      &json_opaque,
      &json_non_real_null
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_values); ++i) {
    ObJsonObject headers(&allocator);
    ASSERT_EQ(OB_SUCCESS, headers.add("X-TC-Invalid", invalid_values[i]));
    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_AI_FUNC_PARAM_TYPE_INVALID,
              ObAIExtraHeadersUtils::validate_and_serialize(headers, buf));
  }
}

TEST_F(TestAiServiceStruct, extra_headers_allocation_failure)
{
  ObArenaAllocator json_allocator("TestAIHeader");
  ObIJsonBase *headers = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            build_json_base(
                json_allocator,
                R"({"X-TC-A":"abcdefghijklmnopqrstuvwxyz"})",
                headers));
  ASSERT_NE(nullptr, headers);

  ToggleAllocator buffer_allocator;
  ObJsonBuffer buf(&buffer_allocator);
  buffer_allocator.set_fail(true);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            ObAIExtraHeadersUtils::validate_and_serialize(*headers, buf));
}

TEST_F(TestAiServiceStruct, extra_headers_name_validation)
{
  const char *valid[] = {
      R"({"X-DashScope-A":"v"})",
      R"({"X-DashScope-Test":"v"})",
      R"({"x-dashscope-test":"v"})",
      R"({"X-DashScope-LoggingConsent":"v"})",
      R"({"X-DashScope-Wait-Timeout":"v"})",
      R"({"X-DashScope-DataInspection":"v"})",
      R"({"X-DashScope-SSE":"v"})",
      R"({"X-DashScope-WorkSpace":"v"})",
      R"({"OpenAI-A":"v"})",
      R"({"OPENAI-ORGANIZATION":"v"})",
      R"({"OpenAI-Project":"v"})",
      R"({"OpenAI-Custom":"v"})",
      R"({"X-CLIENT-REQUEST-ID":"v"})",
      R"({"X-TC-A":"v"})",
      R"({"x-tc-action":"v"})",
      R"({"X-TC-Version":"v"})",
      R"({"X-TC-Timestamp":"v"})",
      R"({"X-TC-Region":"v"})",
      R"({"X-TC-Token":"v"})",
      R"({"X-TC-Language":"v"})",
      R"({"X-Bce-A":"v"})",
      R"({"X-Bce-Request-Id":"v"})",
      R"({"x-bce-date":"v"})",
      R"({"AppId":"v"})"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(valid); ++i) {
    ObArenaAllocator allocator("TestAIHeader");
    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_SUCCESS,
              validate_headers(allocator, ObString::make_string(valid[i]), buf));
  }

  const ObString all_token =
      ObString::make_string(R"({"X-TC-AZaz09!#$%&'*+-.^_`|~":"token"})");
  ObArenaAllocator token_allocator("TestAIHeader");
  ObJsonBuffer token_buf(&token_allocator);
  ASSERT_EQ(OB_SUCCESS, validate_headers(token_allocator, all_token, token_buf));
  ASSERT_EQ(0, token_buf.string().compare(all_token));

  const char *invalid[] = {
      R"({"":"v"})",
      R"({"Authorization":"v"})",
      R"({"Content-Type":"v"})",
      R"({"X-A":"v"})",
      R"({"X-Request-Id":"v"})",
      R"({"X-DashScope-":"v"})",
      R"({"OpenAI-":"v"})",
      R"({"X-TC-":"v"})",
      R"({"x-tc-":"v"})",
      R"({"X-Bce-":"v"})",
      R"({"DashScope-Wait-Timeout":"v"})",
      R"({"X-DashScopeX":"v"})",
      R"({"OpenAI":"v"})",
      R"({"X-TC":"v"})",
      R"({"X-Bce":"v"})",
      R"({"X-Client-Request-Id-Suffix":"v"})",
      R"({"appid-extra":"v"})",
      R"({"X-Goog-Test":"v"})",
      R"({"X-DashScope-A:B":"v"})",
      R"({"X-DashScope-A B":"v"})",
      R"({"X-TC-A\"B":"v"})",
      R"({"X-TC-A(B":"v"})",
      R"({"X-TC-A)B":"v"})",
      R"({"X-TC-A,B":"v"})",
      R"({"X-TC-A/B":"v"})",
      R"({"X-TC-A;B":"v"})",
      R"({"X-TC-A<B":"v"})",
      R"({"X-TC-A=B":"v"})",
      R"({"X-TC-A>B":"v"})",
      R"({"X-TC-A?B":"v"})",
      R"({"X-TC-A@B":"v"})",
      R"({"X-TC-A[B":"v"})",
      R"({"X-TC-A\\B":"v"})",
      R"({"X-TC-A]B":"v"})",
      R"({"X-TC-A{B":"v"})",
      R"({"X-TC-A}B":"v"})",
      R"({"X-TC-A\tB":"v"})",
      R"({"X-TC-A\rB":"v"})",
      R"({"X-TC-A\nB":"v"})",
      R"({"X-TC-A\u0000B":"v"})",
      R"({"X-TC-A\u007fB":"v"})",
      R"({"X-DashScope-中文":"v"})",
      R"({"X-TC-A":"v","x-tc-a":"v2"})"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid); ++i) {
    ObArenaAllocator allocator("TestAIHeader");
    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
              validate_headers(allocator, ObString::make_string(invalid[i]), buf));
  }
}

TEST_F(TestAiServiceStruct, extra_headers_string_wire_validation)
{
  const char *valid[] = {
      R"({"X-TC-A":"  value with space\t "})",
      R"({"X-TC-A":"a\tb"})",
      R"({"X-TC-A":"\t \t"})",
      R"({"X-TC-A":""})"
  };
  const char *expected[] = {
      R"({"X-TC-A":"value with space"})",
      R"({"X-TC-A":"a\tb"})",
      R"({"X-TC-A":""})",
      R"({"X-TC-A":""})"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(valid); ++i) {
    ObArenaAllocator allocator("TestAIHeader");
    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_SUCCESS,
              validate_headers(allocator, ObString::make_string(valid[i]), buf));
    ASSERT_EQ(0, buf.string().compare(expected[i]));
  }

  const char *invalid[] = {
      R"({"X-TC-A":"a\nb"})",
      R"({"X-TC-A":"a\rb"})",
      R"({"X-TC-A":"a\u0000b"})",
      R"({"X-TC-A":"a\u007fb"})"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid); ++i) {
    ObArenaAllocator allocator("TestAIHeader");
    ObJsonBuffer buf(&allocator);
    ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
              validate_headers(allocator, ObString::make_string(invalid[i]), buf));
  }
}

TEST_F(TestAiServiceStruct, extra_headers_type_and_capacity)
{
  ObArenaAllocator allocator("TestAIHeader");
  ObJsonBuffer invalid_type_buf(&allocator);
  ASSERT_EQ(OB_AI_FUNC_PARAM_TYPE_INVALID,
            validate_headers(allocator, ObString::make_string("[]"), invalid_type_buf));

  std::string exact = "{\"X-TC-A\":\"" + std::string(1011, 'a') + "\"}";
  ObJsonBuffer exact_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS,
            validate_headers(allocator,
                             ObString(exact.length(), exact.data()),
                             exact_buf));
  ASSERT_EQ(ObAIExtraHeadersUtils::MAX_SERIALIZED_LENGTH, exact_buf.length());

  std::string oversized = "{\"X-TC-A\":\"" + std::string(1012, 'a') + "\"}";
  ObJsonBuffer oversized_buf(&allocator);
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            validate_headers(allocator,
                             ObString(oversized.length(), oversized.data()),
                             oversized_buf));

  std::string trimmed =
      "{\"X-TC-A\":\" \\t" + std::string(1011, 'a') + "\\t \"}";
  ObJsonBuffer trimmed_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS,
            validate_headers(allocator,
                             ObString(trimmed.length(), trimmed.data()),
                             trimmed_buf));
  ASSERT_EQ(ObAIExtraHeadersUtils::MAX_SERIALIZED_LENGTH, trimmed_buf.length());

  std::string escaped_value;
  for (int64_t i = 0; i < 505; ++i) {
    escaped_value.append("\\\"");
  }
  std::string escaped_exact = "{\"X-TC-A\":\"" + escaped_value + "a\"}";
  ObJsonBuffer escaped_exact_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS,
            validate_headers(allocator,
                             ObString(escaped_exact.length(), escaped_exact.data()),
                             escaped_exact_buf));
  ASSERT_EQ(ObAIExtraHeadersUtils::MAX_SERIALIZED_LENGTH,
            escaped_exact_buf.length());

  escaped_value.append("\\\"");
  std::string escaped_oversized = "{\"X-TC-A\":\"" + escaped_value + "\"}";
  ObJsonBuffer escaped_oversized_buf(&allocator);
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            validate_headers(
                allocator,
                ObString(escaped_oversized.length(), escaped_oversized.data()),
                escaped_oversized_buf));

  std::string utf8_exact_value;
  for (int64_t i = 0; i < 337; ++i) {
    utf8_exact_value.append("中");
  }
  std::string utf8_exact = "{\"X-TC-A\":\"" + utf8_exact_value + "\"}";
  ObJsonBuffer utf8_exact_buf(&allocator);
  ASSERT_EQ(OB_SUCCESS,
            validate_headers(allocator,
                             ObString(utf8_exact.length(), utf8_exact.data()),
                             utf8_exact_buf));
  ASSERT_EQ(ObAIExtraHeadersUtils::MAX_SERIALIZED_LENGTH, utf8_exact_buf.length());

  utf8_exact_value.append("中");
  std::string utf8_oversized = "{\"X-TC-A\":\"" + utf8_exact_value + "\"}";
  ObJsonBuffer utf8_oversized_buf(&allocator);
  ASSERT_EQ(OB_AI_FUNC_PARAM_VALUE_INVALID,
            validate_headers(
                allocator,
                ObString(utf8_oversized.length(), utf8_oversized.data()),
                utf8_oversized_buf));
}

TEST_F(TestAiServiceStruct, extra_headers_profile_lifecycle)
{
  ObArenaAllocator allocator("TestAIHeader");
  ObAIModelConfigInfo config;
  ASSERT_EQ(OB_SUCCESS,
            config.apply_profile_params(
                allocator,
                ObString(),
                ObString::make_string(R"({"extra_headers":{"X-TC-A":"v"}})")));
  ASSERT_NE(nullptr, config.get_extra_headers());
  ObJsonBuffer buf(&allocator);
  ASSERT_EQ(OB_SUCCESS,
            ObAIExtraHeadersUtils::validate_and_serialize(*config.get_extra_headers(), buf));
  ASSERT_EQ(0, buf.string().compare(R"({"X-TC-A":"v"})"));

  config.reset();
  ASSERT_EQ(nullptr, config.get_extra_headers());
  ASSERT_EQ(OB_SUCCESS,
            config.apply_profile_params(
                allocator, ObString(), ObString::make_string(R"({"batch_size":8})")));
  ASSERT_EQ(nullptr, config.get_extra_headers());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ai_service_struct.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
