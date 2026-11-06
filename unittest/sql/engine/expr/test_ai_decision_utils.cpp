/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include "lib/json_type/ob_json_parse.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_decision_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;

class TestAIDecisionUtils : public ::testing::Test
{
protected:
  static constexpr uint32_t FLAGS = ObJsonParser::JSN_UNIQUE_FLAG;
  static int parse(ObIAllocator &allocator, const ObString &text, ObJsonNode *&node)
  {
    return ObJsonParser::get_tree(&allocator, text, node, FLAGS);
  }
};

TEST_F(TestAIDecisionUtils, openai_decisions_require_the_openai_provider)
{
  EXPECT_TRUE(ObAIDecisionUtils::is_openai_provider(ObString::make_string("OPENAI")));
  EXPECT_TRUE(ObAIDecisionUtils::is_openai_provider(ObString::make_string("openai")));
  const char *unsupported[] = {"ALIYUN-OPENAI", "DEEPSEEK", "SILICONFLOW", "HUNYUAN-OPENAI"};
  for (int64_t i = 0; i < ARRAYSIZEOF(unsupported); ++i) {
    EXPECT_FALSE(ObAIDecisionUtils::is_openai_provider(ObString::make_string(unsupported[i])));
  }
}

TEST_F(TestAIDecisionUtils, rejects_unsupported_header_model_types)
{
  const share::EndpointType::TYPE unsupported[] = {
    share::EndpointType::INVALID_TYPE,
    share::EndpointType::SPARSE_EMBEDDING,
    share::EndpointType::MAX_TYPE
  };
  ObArenaAllocator allocator;
  for (int64_t i = 0; i < ARRAYSIZEOF(unsupported); ++i) {
    ObArray<ObString> headers;
    EXPECT_EQ(OB_NOT_SUPPORTED, ObAIFuncUtils::get_header(allocator, unsupported[i],
        ObString::make_string("OPENAI"), ObString::make_string("fictional-key"),
        ObString::make_string("model"), nullptr, headers));
    EXPECT_EQ(0, headers.count());
  }
}

TEST_F(TestAIDecisionUtils, forwards_openai_options_without_overriding_request_fields)
{
  ObArenaAllocator allocator;
  ObJsonNode *input = nullptr;
  ObJsonNode *questions = nullptr;
  ObJsonNode *parameters = nullptr;
  ObJsonNode *expected = nullptr;
  ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string("\"state\""), input));
  ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(
      R"([{"type":"predicate","instructions":"Check the state."}])"), questions));
  ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(
      R"({"fixture_option":{"nested":[1,true]},"nullable_option":null})"), parameters));
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObOpenAIDecisionUtils::get_body(allocator, ObString::make_string("model"),
      input, questions, body, static_cast<ObJsonObject *>(parameters)));
  ASSERT_NE(nullptr, body);
  ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(
      R"({"model":"model","input":"state","questions":[{"type":"predicate",)"
      R"("instructions":"Check the state."}],"fixture_option":{"nested":[1,true]},)"
      R"("nullable_option":null})"), expected));
  int comparison = 1;
  ASSERT_EQ(OB_SUCCESS, body->compare(*expected, comparison));
  ASSERT_EQ(0, comparison);

  const char *reserved_parameters[] = {
    R"({"model":null})",
    R"({"input":"override"})",
    R"({"questions":[]})"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(reserved_parameters); ++i) {
    ASSERT_EQ(OB_SUCCESS,
              parse(allocator, ObString::make_string(reserved_parameters[i]), parameters));
    body = nullptr;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObOpenAIDecisionUtils::get_body(allocator, ObString::make_string("model"),
                  input, questions, body, static_cast<ObJsonObject *>(parameters)));
    EXPECT_EQ(nullptr, body);
  }
}

TEST_F(TestAIDecisionUtils, accepts_escaped_nul_and_binary_json)
{
  ObArenaAllocator allocator;
  ObJsonNode *node = nullptr;
  ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string("{\"body\":\"a\\u0000b\"}"), node));
  ASSERT_NE(nullptr, node);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, node->json_type());
  const ObJsonNode *body = static_cast<ObJsonObject *>(node)->get_value("body");
  ASSERT_NE(nullptr, body);
  ASSERT_EQ(3, body->get_data_length());
  ASSERT_EQ(0, memcmp("a\0b", body->get_data(), 3));
  ObString binary;
  ASSERT_EQ(OB_SUCCESS, node->get_raw_binary(binary, &allocator));
  ASSERT_NE(nullptr, memchr(binary.ptr(), '\0', binary.length()));
  ObIJsonBase *roundtrip = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, binary,
      ObJsonInType::JSON_BIN, ObJsonInType::JSON_TREE, roundtrip, FLAGS));
  ASSERT_NE(nullptr, roundtrip);
  ObStringBuffer text(&allocator);
  ASSERT_EQ(OB_SUCCESS, roundtrip->print(text, false));
  ObJsonNode *reparsed = nullptr;
  ASSERT_EQ(OB_SUCCESS, parse(allocator, text.string(), reparsed));
  ASSERT_NE(nullptr, reparsed);
}

TEST_F(TestAIDecisionUtils, typesafe_checks_only_request_outer_containers)
{
  struct RequestCase
  {
    const char *state_;
    const char *questions_;
    int expected_ret_;
  };
  const RequestCase cases[] = {
    {"null", R"({"q":{}})", OB_INVALID_ARGUMENT},
    {"true", R"({"q":{}})", OB_INVALID_ARGUMENT},
    {"1", R"({"q":{}})", OB_INVALID_ARGUMENT},
    {R"("state")", "null", OB_INVALID_ARGUMENT},
    {R"("state")", "[]", OB_INVALID_ARGUMENT},
    {R"("state")", "{}", OB_INVALID_ARGUMENT},
    {R"("state")", R"("questions")", OB_INVALID_ARGUMENT},
    {R"("state")", R"({"q":null})", OB_SUCCESS},
    {R"("state")",
        R"({"q":{"type":"future","instructions":1,"criteria":[]},"other":17})",
        OB_SUCCESS},
    {"{}", R"({"q":{}})", OB_SUCCESS},
    {"[]", R"({"q":{"type":"choice"}})", OB_SUCCESS},
    {R"([{"native":[true,null]}])",
        R"({"q":{"type":null},"s":{"type":"score","criteria":{}}})",
        OB_SUCCESS}
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    const RequestCase &item = cases[i];
    SCOPED_TRACE(item.state_);
    SCOPED_TRACE(item.questions_);
    ObArenaAllocator allocator;
    ObJsonNode *state = nullptr;
    ObJsonNode *questions = nullptr;
    ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(item.state_), state));
    ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(item.questions_), questions));
    ObJsonObject *body = nullptr;
    ASSERT_EQ(item.expected_ret_, ObTypeSafeDecisionUtils::get_body(allocator,
        ObString::make_string("model"), state, questions, body));
    if (OB_SUCCESS == item.expected_ret_) {
      ASSERT_NE(nullptr, body);
      const std::string expected_text = std::string("{\"model\":\"model\",\"state\":")
          + item.state_ + ",\"questions\":" + item.questions_ + "}";
      ObJsonNode *expected = nullptr;
      ASSERT_EQ(OB_SUCCESS, parse(allocator,
          ObString(expected_text.length(), expected_text.data()), expected));
      int comparison = 1;
      ASSERT_EQ(OB_SUCCESS, body->compare(*expected, comparison));
      EXPECT_EQ(0, comparison);
    } else {
      EXPECT_EQ(nullptr, body);
    }
  }
}

TEST_F(TestAIDecisionUtils, openai_checks_only_request_outer_containers)
{
  struct RequestCase
  {
    const char *input_;
    const char *questions_;
    int expected_ret_;
  };
  const RequestCase cases[] = {
    {"null", "[{}]", OB_INVALID_ARGUMENT},
    {"true", "[{}]", OB_INVALID_ARGUMENT},
    {"1", "[{}]", OB_INVALID_ARGUMENT},
    {R"("state")", "null", OB_INVALID_ARGUMENT},
    {R"("state")", "{}", OB_INVALID_ARGUMENT},
    {R"("state")", "[]", OB_INVALID_ARGUMENT},
    {R"("state")", R"("questions")", OB_INVALID_ARGUMENT},
    {R"("state")", "[null,17,true]", OB_SUCCESS},
    {"{}", "[{}]", OB_SUCCESS},
    {"[]", R"([{"type":"future","name":null,"instructions":false}])", OB_SUCCESS},
    {R"({"role":false,"content":{"nested":[1,true,null]}})",
        R"([{"type":"choice","choices":{}},{"type":"score","levels":[]}])", OB_SUCCESS},
    {R"([{"type":"custom_tool_call","input":17},42,null])",
        R"([{"name":"same"},{"name":"same"}])", OB_SUCCESS}
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    const RequestCase &item = cases[i];
    SCOPED_TRACE(item.input_);
    SCOPED_TRACE(item.questions_);
    ObArenaAllocator allocator;
    ObJsonNode *input = nullptr;
    ObJsonNode *questions = nullptr;
    ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(item.input_), input));
    ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(item.questions_), questions));
    ObJsonObject *body = nullptr;
    ASSERT_EQ(item.expected_ret_, ObOpenAIDecisionUtils::get_body(allocator,
        ObString::make_string("model"), input, questions, body));
    if (OB_SUCCESS == item.expected_ret_) {
      ASSERT_NE(nullptr, body);
      const std::string expected_text = std::string("{\"model\":\"model\",\"input\":")
          + item.input_ + ",\"questions\":" + item.questions_ + "}";
      ObJsonNode *expected = nullptr;
      ASSERT_EQ(OB_SUCCESS, parse(allocator,
          ObString(expected_text.length(), expected_text.data()), expected));
      int comparison = 1;
      ASSERT_EQ(OB_SUCCESS, body->compare(*expected, comparison));
      EXPECT_EQ(0, comparison);
    } else {
      EXPECT_EQ(nullptr, body);
    }
  }
}

TEST_F(TestAIDecisionUtils, rejects_missing_empty_or_scalar_answers)
{
  const char *invalid_responses[] = {
    R"({})",
    R"({"answers":null})",
    R"({"answers":{}})",
    R"({"answers":[]})",
    R"({"answers":false})",
    R"({"answers":17})",
    R"({"answers":""})",
    R"({"answers":"nonempty scalar"})"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_responses); ++i) {
    SCOPED_TRACE(invalid_responses[i]);
    ObArenaAllocator allocator;
    ObJsonNode *response = nullptr;
    ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(invalid_responses[i]), response));
    ASSERT_NE(nullptr, response);
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, response->json_type());
    ObJsonObject *output = nullptr;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObAIDecisionUtils::parse_output(allocator,
        static_cast<ObJsonObject *>(response), output));
    EXPECT_EQ(nullptr, output);
  }
  ObArenaAllocator allocator;
  ObJsonObject *output = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObAIDecisionUtils::parse_output(allocator, nullptr, output));
  EXPECT_EQ(nullptr, output);
}

TEST_F(TestAIDecisionUtils, retains_nonempty_answers_verbatim_and_omits_metadata)
{
  struct ResponseCase
  {
    const char *response_;
    const char *expected_;
  };
  const ResponseCase cases[] = {
    {R"({"answers":{"other":{"type":null,"noul":"model-value"},)"
        R"("extra":null},"usage":{"tokens":1}})",
        R"({"answers":{"other":{"type":null,"noul":"model-value"},"extra":null}})"},
    {R"({"answers":[{"type":"future","name":"other"},null,true,17],"model":"ignored"})",
        R"({"answers":[{"type":"future","name":"other"},null,true,17]})"},
    {R"({"answers":[{"type":"predicate","name":"second",)"
        R"("probability":"model-value"},{"type":"refusal","name":"first"}]})",
        R"({"answers":[{"type":"predicate","name":"second",)"
        R"("probability":"model-value"},{"type":"refusal","name":"first"}]})"},
    {R"({"answers":{"q":false},"metadata":"ignored"})", R"({"answers":{"q":false}})"},
    {R"({"answers":[null]})", R"({"answers":[null]})"}
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    const ResponseCase &item = cases[i];
    SCOPED_TRACE(item.response_);
    ObArenaAllocator allocator;
    ObJsonNode *response = nullptr;
    ObJsonNode *expected = nullptr;
    ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(item.response_), response));
    ASSERT_EQ(OB_SUCCESS, parse(allocator, ObString::make_string(item.expected_), expected));
    ASSERT_NE(nullptr, response);
    ASSERT_EQ(ObJsonNodeType::J_OBJECT, response->json_type());
    ObJsonObject *output = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObAIDecisionUtils::parse_output(allocator,
        static_cast<ObJsonObject *>(response), output));
    ASSERT_NE(nullptr, output);
    ASSERT_EQ(1, output->element_count());
    int comparison = 1;
    ASSERT_EQ(OB_SUCCESS, output->compare(*expected, comparison));
    EXPECT_EQ(0, comparison);
  }
}

TEST_F(TestAIDecisionUtils, decision_url_preserves_escapes_and_rejects_literal_controls)
{
  for (int byte = 0; byte <= 32; ++byte) {
    const std::string url = std::string("http://127.0.0.1/path") + static_cast<char>(byte) + "tail";
    EXPECT_FALSE(ObAIDecisionUtils::is_valid_url(ObString(url.length(), url.data())));
  }
  const char del_url[] = "http://127.0.0.1/path\x7f" "tail";
  EXPECT_FALSE(ObAIDecisionUtils::is_valid_url(ObString::make_string(del_url)));
  EXPECT_FALSE(ObAIDecisionUtils::is_valid_url(
      ObString::make_string("ftp://example.invalid/path")));
  EXPECT_FALSE(ObAIDecisionUtils::is_valid_url(ObString()));
  EXPECT_TRUE(ObAIDecisionUtils::is_valid_url(
      ObString::make_string("http://127.0.0.1/path%00?trace=%0D%0A")));
  EXPECT_TRUE(ObAIDecisionUtils::is_valid_url(ObString::make_string("HTTPS://[::1]:46340/native")));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ai_decision_utils.log", true, false);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
