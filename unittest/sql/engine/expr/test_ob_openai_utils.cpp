/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "ob_expr_test_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObOpenAIUtilsTest: public ::testing::Test
{
public:
    ObOpenAIUtilsTest();
    virtual ~ObOpenAIUtilsTest();
    virtual void SetUp();
    virtual void TearDown();
private:
    // disallow copy
    ObOpenAIUtilsTest(const ObOpenAIUtilsTest &other);
    ObOpenAIUtilsTest& operator=(const ObOpenAIUtilsTest &other);
protected:
    // data members
};

ObOpenAIUtilsTest::ObOpenAIUtilsTest()
{
}

ObOpenAIUtilsTest::~ObOpenAIUtilsTest()
{
}

void ObOpenAIUtilsTest::SetUp()
{
}

void ObOpenAIUtilsTest::TearDown()
{
}

static ObString get_json_string_value(ObJsonNode *node)
{
    if (nullptr == node) {
        ADD_FAILURE() << "JSON node is null";
        return ObString();
    } else if (ObJsonNodeType::J_STRING != node->json_type()) {
        ADD_FAILURE() << "JSON node is not a string";
        return ObString();
    }
    return static_cast<ObJsonString *>(node)->get_str();
}

TEST_F(ObOpenAIUtilsTest, test_complete_get_header)
{
    ObArray<ObString> headers;
    ObString api_key("sk-1234567890");
    ObString authorization("Authorization: Bearer sk-1234567890");
    ObString content_type("Content-Type: application/json");
    ObArenaAllocator allocator(ObModIds::TEST);
    ObOpenAIUtils::ObOpenAIComplete completion;
    ASSERT_EQ(OB_SUCCESS, completion.get_header(allocator, api_key, headers));
    ASSERT_EQ(2, headers.count());
    ASSERT_EQ(authorization, headers[0]);
    ASSERT_EQ(content_type, headers[1]);
}

TEST_F(ObOpenAIUtilsTest, test_complete_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString model("gpt-4o-mini");
    ObString prompt("You are a helpful assistant.");
    ObString content("oceanbase is a distributed database");
    ObString messages("[{\"role\": \"system\", \"content\": \"You are a helpful assistant.\"}, {\"role\": \"user\", \"content\": \"what is oceanbase?\"}]");
    ObOpenAIUtils::ObOpenAIComplete completion;
    ObJsonObject *body = nullptr;
    ObJsonObject *config = nullptr;
    ASSERT_EQ(OB_SUCCESS, completion.get_body(allocator, model, prompt, content, config, body));
    ObJsonNode *model_node = body->get_value("model");
    ObStringBuffer model_buf(&allocator);
    model_node->print(model_buf, 0);
    ASSERT_EQ(model, model_buf.string());

    ObJsonNode *messages_node = body->get_value("messages");
    ObJsonArray *messages_array = static_cast<ObJsonArray *>(messages_node);
    ASSERT_EQ(2, messages_array->element_count());

    ObJsonNode *message_node1 = messages_array->get_value(0);
    ObJsonObject *message_obj1 = static_cast<ObJsonObject *>(message_node1);
    ObJsonNode *prompt_node = message_obj1->get_value("content");
    ObStringBuffer prompt_buf(&allocator);
    prompt_node->print(prompt_buf, 0);
    ASSERT_EQ(prompt, prompt_buf.string());

    ObJsonNode *message_node2 = messages_array->get_value(1);
    ObJsonObject *message_obj2 = static_cast<ObJsonObject *>(message_node2);
    ObJsonNode *content_node = message_obj2->get_value("content");
    ObStringBuffer content_buf(&allocator);
    content_node->print(content_buf, 0);
    ASSERT_EQ(content, content_buf.string());
}

TEST_F(ObOpenAIUtilsTest, test_complete_parse_output)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString content("天气真好。");
    ObString response(
        "{"
        "\"choices\": [ "
            "{"
                "\"message\": {"
                    "\"role\": \"assistant\","
                    "\"content\": \"天气真好。\""
                "},"
                "\"finish_reason\": \"stop\","
                "\"index\": 0,"
                "\"logprobs\": null"
            "}"
        "],"
        "\"object\": \"chat.completion\","
        "\"usage\": {"
            "\"prompt_tokens\": 3019,"
            "\"completion_tokens\": 104,"
            "\"total_tokens\": 3123,"
            "\"prompt_tokens_details\": {"
                "\"cached_tokens\": 2048"
            "}"
        "},"
        "\"created\": 1735120033,"
        "\"system_fingerprint\": null,"
        "\"model\": \"qwen-plus\","
        "\"id\": \"chatcmpl-6ada9ed2-7f33-9de2-8bb0-78bd4035025a\""
    "}"
    );
    ObOpenAIUtils::ObOpenAIComplete completion;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, completion.parse_output(allocator, http_response, result));
    ObStringBuffer content_buf(&allocator);
    result->print(content_buf, 0);
    ASSERT_EQ(content, content_buf.string());
}

TEST_F(ObOpenAIUtilsTest, test_complete_parse_output_empty)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString content("天气真好。");
    ObString response(
        "{"
        "\"choices\": [ "
            "{"
                "\"message\": {"
                    "\"role\": \"assistant\""

                "},"
                "\"finish_reason\": \"stop\","
                "\"index\": 0,"
                "\"logprobs\": null"
            "}"
        "],"
        "\"object\": \"chat.completion\","
        "\"usage\": {"
            "\"prompt_tokens\": 3019,"
            "\"completion_tokens\": 104,"
            "\"total_tokens\": 3123,"
            "\"prompt_tokens_details\": {"
                "\"cached_tokens\": 2048"
            "}"
        "},"
        "\"created\": 1735120033,"
        "\"system_fingerprint\": null,"
        "\"model\": \"qwen-plus\","
        "\"id\": \"chatcmpl-6ada9ed2-7f33-9de2-8bb0-78bd4035025a\""
    "}"
    );
    ObOpenAIUtils::ObOpenAIComplete completion;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_INVALID_DATA, completion.parse_output(allocator, http_response, result));
}

TEST_F(ObOpenAIUtilsTest, test_embedding_get_header)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString api_key("sk-1234567890");
    ObString authorization("Authorization: Bearer sk-1234567890");
    ObString content_type("Content-Type: application/json");
    ObOpenAIUtils::ObOpenAIEmbed embedding;
    ObArray<ObString> headers;
    ASSERT_EQ(OB_SUCCESS, embedding.get_header(allocator, api_key, headers));
    ASSERT_EQ(2, headers.count());
    ASSERT_EQ(authorization, headers[0]);
    ASSERT_EQ(content_type, headers[1]);
}

TEST_F(ObOpenAIUtilsTest, test_embedding_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString model("text-embedding-3-small");
    ObString input("oceanbase is a distributed database");
    ObArray<ObString> input_array;
    input_array.push_back(input);
    ObOpenAIUtils::ObOpenAIEmbed embedding;
    ObJsonObject *body = nullptr;
    ObJsonObject *config = nullptr;
    ObString input_type("text");
    ASSERT_EQ(OB_SUCCESS, embedding.get_body(allocator, model, input_array, config, input_type, body));

    ObJsonNode *model_node = body->get_value("model");
    ObStringBuffer model_buf(&allocator);
    model_node->print(model_buf, 0);
    ASSERT_EQ(model, model_buf.string());

    ObJsonNode *input_node = body->get_value("input");
    ObJsonArray *input_array_node = static_cast<ObJsonArray *>(input_node);
    ASSERT_EQ(1, input_array_node->element_count());
    ObJsonNode *input_node2 = input_array_node->get_value(0);
    ObStringBuffer input_buf(&allocator);
    input_node2->print(input_buf, 0);
    ASSERT_EQ(input, input_buf.string());
}

TEST_F(ObOpenAIUtilsTest, test_embedding_parse_output)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString content("[0.0023064255, -0.009327292, -0.0028842222]");
    ObString response(
        "{"
            "\"data\": ["
                "{"
                    "\"embedding\": [0.0023064255, -0.009327292, -0.0028842222],"
                    "\"index\": 0,"
                    "\"object\": \"embedding\""
                "}"
            "],"
            "\"model\":\"text-embedding-v4\","
            "\"object\":\"list\","
            "\"usage\":{\"prompt_tokens\":23,\"total_tokens\":23},"
            "\"id\":\"f62c2ae7-0906-9758-ab34-47c5764f07e2\""
        "}"
    );
    ObOpenAIUtils::ObOpenAIEmbed embedding;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding.parse_output(allocator, http_response, result));

    ObJsonArray *embeddings_array = static_cast<ObJsonArray *>(result);
    ASSERT_EQ(1, embeddings_array->element_count());

    ObJsonNode *embedding_node = embeddings_array->get_value(0);
    ObJsonArray *embedding_array = static_cast<ObJsonArray *>(embedding_node);
    ObStringBuffer embedding_buf(&allocator);
    embedding_array->print(embedding_buf, 0);
    ASSERT_EQ(content, embedding_buf.string());
}

TEST_F(ObOpenAIUtilsTest, test_embedding_parse_output_empty)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString content("[0.0023064255, -0.009327292, -0.0028842222]");
    ObString response(
        "{"
            "\"data\": ["
                "{"
                    "\"index\": 0,"
                    "\"object\": \"embedding\""
                "}"
            "],"
            "\"model\":\"text-embedding-v4\","
            "\"object\":\"list\","
            "\"usage\":{\"prompt_tokens\":23,\"total_tokens\":23},"
            "\"id\":\"f62c2ae7-0906-9758-ab34-47c5764f07e2\""
        "}"
    );
    ObOpenAIUtils::ObOpenAIEmbed embedding;
    ObIJsonBase *j_base = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, response, ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_base));
    ObJsonObject *http_response = static_cast<ObJsonObject *>(j_base);
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_INVALID_DATA, embedding.parse_output(allocator, http_response, result));
}

TEST_F(ObOpenAIUtilsTest, test_vllm_text_embedding_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObVllmUtils::ObVllmEmbed embedding;
    ObString model("text-embedding-model");
    ObString input_type("text");
    ObJsonObject *body = nullptr;

    ObArray<ObString> single_input;
    ASSERT_EQ(OB_SUCCESS, single_input.push_back(ObString("first")));
    ASSERT_EQ(OB_SUCCESS, embedding.get_body(allocator, model, single_input, nullptr, input_type, body));
    ASSERT_NE(nullptr, body);
    ASSERT_EQ(ObString("first"), get_json_string_value(body->get_value("input")));
    ASSERT_EQ(ObString("float"), get_json_string_value(body->get_value("encoding_format")));
    ASSERT_EQ(nullptr, body->get_value("messages"));
    ASSERT_EQ(nullptr, body->get_value("dimensions"));

    ObJsonObject *config = nullptr;
    ObJsonInt *dimensions = nullptr;
    ObJsonString *requested_encoding_format = nullptr;
    ObString requested_encoding_format_value("base64");
    ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object(allocator, config));
    ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_int(allocator, 1024, dimensions));
    ASSERT_EQ(OB_SUCCESS, config->add("dimensions", dimensions));
    ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_string(allocator,
                                                            requested_encoding_format_value,
                                                            requested_encoding_format));
    ASSERT_EQ(OB_SUCCESS, config->add("encoding_format", requested_encoding_format));

    ObArray<ObString> batch_input;
    ASSERT_EQ(OB_SUCCESS, batch_input.push_back(ObString("first")));
    ASSERT_EQ(OB_SUCCESS, batch_input.push_back(ObString("second")));
    body = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding.get_body(allocator, model, batch_input, config, input_type, body));
    ObJsonArray *input_array = static_cast<ObJsonArray *>(body->get_value("input"));
    ASSERT_NE(nullptr, input_array);
    ASSERT_EQ(2, input_array->element_count());
    ASSERT_EQ(ObString("first"), get_json_string_value(input_array->get_value(0)));
    ASSERT_EQ(ObString("second"), get_json_string_value(input_array->get_value(1)));
    ASSERT_EQ(1024, body->get_value("dimensions")->get_int());
    ASSERT_EQ(ObString("float"), get_json_string_value(body->get_value("encoding_format")));
    ASSERT_STREQ("/embeddings", embedding.get_embed_path());
}

TEST_F(ObOpenAIUtilsTest, test_vllm_qwen3_vl_embedding_get_body)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObVllmUtils::ObVllmEmbed embedding;
    ObString model("qWeN3-vL-eMbEdDiNg-8b");
    ObArray<ObString> contents;
    ASSERT_EQ(OB_SUCCESS, contents.push_back(ObString("describe this image")));
    ObString input_type("text");
    ObJsonObject *body = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding.get_body(allocator, model, contents, nullptr, input_type, body));

    ASSERT_EQ(nullptr, body->get_value("input"));
    ASSERT_EQ(ObString("float"), get_json_string_value(body->get_value("encoding_format")));
    ASSERT_TRUE(body->get_value("add_generation_prompt")->get_boolean());
    ASSERT_FALSE(body->get_value("continue_final_message")->get_boolean());
    ASSERT_TRUE(body->get_value("add_special_tokens")->get_boolean());

    ObJsonArray *messages = static_cast<ObJsonArray *>(body->get_value("messages"));
    ASSERT_NE(nullptr, messages);
    ASSERT_EQ(1, messages->element_count());
    ObJsonObject *message = static_cast<ObJsonObject *>(messages->get_value(0));
    ASSERT_EQ(ObString("user"), get_json_string_value(message->get_value("role")));
    ObJsonArray *content = static_cast<ObJsonArray *>(message->get_value("content"));
    ASSERT_NE(nullptr, content);
    ASSERT_EQ(1, content->element_count());
    ObJsonObject *text_item = static_cast<ObJsonObject *>(content->get_value(0));
    ASSERT_EQ(ObString("text"), get_json_string_value(text_item->get_value("type")));
    ASSERT_EQ(ObString("describe this image"), get_json_string_value(text_item->get_value("text")));

    contents.reset();
    ASSERT_EQ(OB_SUCCESS, contents.push_back(ObString("data:image/png;base64,iVBORw0KGgo=")));
    input_type = ObString("image");
    body = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding.get_body(allocator, model, contents, nullptr, input_type, body));
    messages = static_cast<ObJsonArray *>(body->get_value("messages"));
    message = static_cast<ObJsonObject *>(messages->get_value(0));
    content = static_cast<ObJsonArray *>(message->get_value("content"));
    ObJsonObject *image_item = static_cast<ObJsonObject *>(content->get_value(0));
    ASSERT_EQ(ObString("image_url"), get_json_string_value(image_item->get_value("type")));
    ObJsonObject *image_url = static_cast<ObJsonObject *>(image_item->get_value("image_url"));
    ASSERT_EQ(ObString("data:image/png;base64,iVBORw0KGgo="),
              get_json_string_value(image_url->get_value("url")));
}

TEST_F(ObOpenAIUtilsTest, test_vllm_other_vl_model_and_batch_validation)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObVllmUtils::ObVllmEmbed embedding;
    ObString model("Qwen3-VL-Embedding-8B-Alias");
    ObString input_type("text");
    ObArray<ObString> contents;
    ASSERT_EQ(OB_SUCCESS, contents.push_back(ObString("first")));
    ObJsonObject *body = nullptr;
    ASSERT_EQ(OB_SUCCESS, embedding.get_body(allocator, model, contents, nullptr, input_type, body));
    ASSERT_NE(nullptr, body->get_value("messages"));
    ASSERT_EQ(nullptr, body->get_value("add_generation_prompt"));
    ASSERT_EQ(nullptr, body->get_value("continue_final_message"));
    ASSERT_EQ(nullptr, body->get_value("add_special_tokens"));

    ASSERT_EQ(OB_SUCCESS, contents.push_back(ObString("second")));
    body = nullptr;
    ASSERT_EQ(OB_NOT_SUPPORTED, embedding.get_body(allocator, model, contents, nullptr, input_type, body));
}

TEST_F(ObOpenAIUtilsTest, test_vllm_embedding_parse_output_keeps_response_order)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObVllmUtils::ObVllmEmbed embedding;
    ObString response(
        "{"
          "\"data\":["
            "{\"embedding\":[1.0,1.5],\"index\":1},"
            "{\"embedding\":[0.0,0.5],\"index\":0}"
          "]"
        "}");
    ObIJsonBase *json_base = nullptr;
    ASSERT_EQ(OB_SUCCESS,
              ObJsonBaseFactory::get_json_base(&allocator,
                                               response,
                                               ObJsonInType::JSON_TREE,
                                               ObJsonInType::JSON_TREE,
                                               json_base));
    ObIJsonBase *result = nullptr;
    ASSERT_EQ(OB_SUCCESS,
              embedding.parse_output(allocator, static_cast<ObJsonObject *>(json_base), result));
    ObJsonArray *embeddings = static_cast<ObJsonArray *>(result);
    ASSERT_EQ(2, embeddings->element_count());
    ASSERT_DOUBLE_EQ(1.0,
                     static_cast<ObJsonArray *>(embeddings->get_value(0))->get_value(0)->get_double());
    ASSERT_DOUBLE_EQ(0.0,
                     static_cast<ObJsonArray *>(embeddings->get_value(1))->get_value(0)->get_double());
}

TEST_F(ObOpenAIUtilsTest, test_vllm_provider_capability_dispatch)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString provider(ObAIFuncProviderUtils::VLLM);
    ObAIFuncIEmbed *embed_provider = nullptr;
    ObAIFuncIComplete *complete_provider = nullptr;
    ObAIFuncIVLComplete *vl_complete_provider = nullptr;
    ObAIFuncIRerank *rerank_provider = nullptr;

    ASSERT_EQ(OB_SUCCESS, ObAIFuncUtils::get_embed_provider(allocator, provider, embed_provider, false));
    ASSERT_NE(nullptr, embed_provider);
    ASSERT_EQ(OB_SUCCESS, ObAIFuncUtils::get_complete_provider(allocator, provider, complete_provider));
    ASSERT_NE(nullptr, complete_provider);
    ASSERT_EQ(OB_SUCCESS, ObAIFuncUtils::get_vl_complete_provider(allocator, provider, vl_complete_provider));
    ASSERT_NE(nullptr, vl_complete_provider);
    ASSERT_EQ(OB_NOT_SUPPORTED, ObAIFuncUtils::get_rerank_provider(allocator, provider, rerank_provider));
    ASSERT_EQ(nullptr, rerank_provider);
}

TEST_F(ObOpenAIUtilsTest, test_vllm_batch_api_not_supported)
{
    ObArenaAllocator allocator(ObModIds::TEST);
    ObVllmUtils::ObVllmEmbed embedding;
    ObAiBatchFileLine line;
    ObString endpoint;
    ObString completion_window;
    ASSERT_EQ(OB_NOT_SUPPORTED,
              embedding.encode_batch_line(allocator,
                                          ObString("Qwen3-VL-Embedding-8B"),
                                          0,
                                          ObString("text"),
                                          line));
    ASSERT_EQ(OB_NOT_SUPPORTED,
              embedding.get_batch_submit_spec(endpoint, completion_window));
}

TEST_F(ObOpenAIUtilsTest, test_model_config_manager_singleton_and_init)
{
    ObAIModelConfigInfoManager &manager = ObAIModelConfigInfoManager::get_instance();
    // singleton instance should be stable
    ASSERT_EQ(&manager, &ObAIModelConfigInfoManager::get_instance());

    // reset and re-init should both succeed
    manager.reset();
    ASSERT_EQ(OB_SUCCESS, manager.init());
    ASSERT_GT(manager.size(), 0);
    // repeated init should be no-op and still success
    ASSERT_EQ(OB_SUCCESS, manager.init());
}

TEST_F(ObOpenAIUtilsTest, test_model_config_manager_get_model_config_positive)
{
    ObAIModelConfigInfoManager &manager = ObAIModelConfigInfoManager::get_instance();
    manager.reset();

    // positive: known model config should be found (lazy init path)
    oceanbase::share::ObAIModelConfigItem item;
    ASSERT_EQ(OB_SUCCESS, manager.get_model_config(ObAIFuncProviderUtils::DASHSCOPE,
                                                   ObString("qwen3-vl-embedding"),
                                                   item));
    ASSERT_EQ(5, item.batch_size_);
    ASSERT_EQ(5 * 1024 * 1024, item.max_image_size_);

    ASSERT_EQ(OB_SUCCESS, manager.get_model_config(ObAIFuncProviderUtils::DASHSCOPE,
                                                   ObString("qwen2.5-vl-embedding"),
                                                   item));
    ASSERT_EQ(1, item.batch_size_);
    ASSERT_EQ(5 * 1024 * 1024, item.max_image_size_);

    ASSERT_EQ(OB_SUCCESS, manager.get_model_config(ObAIFuncProviderUtils::DASHSCOPE,
                                                   ObString("multimodal-embedding-v1"),
                                                   item));
    ASSERT_EQ(1, item.batch_size_);
    ASSERT_EQ(3 * 1024 * 1024, item.max_image_size_);
}

TEST_F(ObOpenAIUtilsTest, test_model_config_manager_get_model_config_negative)
{
    ObAIModelConfigInfoManager &manager = ObAIModelConfigInfoManager::get_instance();
    manager.reset();
    oceanbase::share::ObAIModelConfigItem item;

    // unknown model: get_model_config returns OB_SUCCESS with default item (design: fallback to default)
    ASSERT_EQ(OB_SUCCESS, manager.get_model_config(ObAIFuncProviderUtils::DASHSCOPE,
                                                  ObString("model-not-exists"),
                                                  item));
    ASSERT_EQ(0, item.batch_size_);
    ASSERT_EQ(0, item.max_image_size_);
    // unknown provider: same behavior
    ASSERT_EQ(OB_SUCCESS, manager.get_model_config(ObString("UNKNOWN-PROVIDER"),
                                                   ObString("qwen3-vl-embedding"),
                                                   item));
    ASSERT_EQ(0, item.batch_size_);
    ASSERT_EQ(0, item.max_image_size_);
    // invalid input should fail fast
    ASSERT_EQ(OB_INVALID_ARGUMENT, manager.get_model_config(ObString(),
                                                           ObString("qwen3-vl-embedding"),
                                                           item));
    ASSERT_EQ(OB_INVALID_ARGUMENT, manager.get_model_config(ObAIFuncProviderUtils::DASHSCOPE,
                                                           ObString(),
                                                           item));
}

TEST_F(ObOpenAIUtilsTest, test_model_config_manager_get_config_negative)
{
    ObAIModelConfigInfoManager &manager = ObAIModelConfigInfoManager::get_instance();
    manager.reset();
    oceanbase::share::ObAIModelConfigItem item;

    // get_config has the same argument checks
    ASSERT_EQ(OB_INVALID_ARGUMENT, manager.get_config(ObString(),
                                                      ObString("qwen3-vl-embedding"),
                                                      item));
    ASSERT_EQ(OB_INVALID_ARGUMENT, manager.get_config(ObAIFuncProviderUtils::DASHSCOPE,
                                                      ObString(),
                                                      item));
}


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
