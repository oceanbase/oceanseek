/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>

#define private public
#include "storage/async_index/ob_async_index_define.h"
#include "lib/alloc/ob_malloc_allocator.h"
#undef private
#include "common/ob_clock_generator.h"
#include "common/ob_tenant_data_version_mgr.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_vec_index_builder_util.h"
#include "share/schema/ob_schema_utils.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/parser/parse_node.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

class TestAsyncIndexDefine : public ::testing::Test
{
public:
  TestAsyncIndexDefine() {}
  virtual ~TestAsyncIndexDefine() {}
  virtual void SetUp() override { ODV_MGR.set_mock_data_version(DATA_CURRENT_VERSION); }
  virtual void TearDown() override { ODV_MGR.set_mock_data_version(0); }
};

TEST_F(TestAsyncIndexDefine, test_refresh_type_enum)
{
  ASSERT_TRUE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_PERIOD));
  ASSERT_TRUE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_CREATE_INDEX));
  ASSERT_FALSE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_MAX));
  ASSERT_FALSE(is_valid_async_refresh_type(static_cast<ObAsyncRefreshType>(-1)));

  ASSERT_STREQ("PERIOD", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_PERIOD));
  ASSERT_STREQ("CREATE_INDEX", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_CREATE_INDEX));
  ASSERT_STREQ("UNKNOWN", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_MAX));
}

TEST_F(TestAsyncIndexDefine, test_per_index_refresh_classification)
{
  ObAsyncIndexInfo fts_info;
  fts_info.index_type_ = share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL;
  ASSERT_TRUE(fts_info.is_async_fts_index());
  ASSERT_FALSE(fts_info.is_async_ivf_index());

  ObAsyncIndexInfo delete_info;
  delete_info.index_type_ = share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
  ASSERT_TRUE(delete_info.is_async_delete_index());
  ASSERT_FALSE(delete_info.is_async_ivf_index());

  ObAsyncIndexInfo ivf_info;
  ivf_info.index_type_ = share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  ASSERT_FALSE(ivf_info.is_async_fts_index());
  ASSERT_FALSE(ivf_info.is_async_delete_index());
  ASSERT_TRUE(ivf_info.is_async_ivf_index());

  ASSERT_TRUE(share::schema::is_async_vec_index(
      share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL));
  ASSERT_FALSE(share::schema::is_async_vec_index(
      share::schema::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL));
  ASSERT_TRUE(share::schema::is_vec_or_async_vec_index(
      share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL));
  ASSERT_TRUE(share::schema::is_vec_or_async_vec_index(
      share::schema::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL));
  ASSERT_FALSE(share::schema::is_vec_or_async_vec_index(
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL));
  ASSERT_FALSE(share::schema::is_vec_or_async_vec_index(
      share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));

  ASSERT_TRUE(share::schema::is_user_defined_async_index(
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL));
  ASSERT_TRUE(share::schema::is_user_defined_async_index(
      share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL));
  ASSERT_FALSE(share::schema::is_user_defined_async_index(
      share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));
  ASSERT_TRUE(share::schema::is_async_index_aux(
      share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));
  ASSERT_FALSE(share::schema::is_async_index_aux(
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL));
  ASSERT_FALSE(share::schema::is_async_index_aux(
      share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL));
  ASSERT_TRUE(share::schema::is_async_index(
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL));
  ASSERT_TRUE(share::schema::is_async_index(
      share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL));
  ASSERT_TRUE(share::schema::is_async_index(
      share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL));
}

TEST_F(TestAsyncIndexDefine, test_disk_bbq_algorithm_name_contract)
{
  using namespace share::schema;
  ObVectorIndexParam param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2"), VIT_IVF_INDEX, param, false));
  ASSERT_EQ(VIAT_DISK_BBQ, param.type_);
  ASSERT_STREQ("DISK_BBQ", ObVectorIndexUtil::get_type_str(param.type_));

  ObVectorIndexParam legacy_param;
  ASSERT_NE(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=IVF_RABITQ,DISTANCE=L2"), VIT_IVF_INDEX,
      legacy_param, false));
}

TEST_F(TestAsyncIndexDefine, test_async_ivf_index_name_is_preserved_and_copied)
{
  using namespace share::schema;
  ObArenaAllocator allocator("AsyncVecName");
  const ObString index_name("idx_async_disk_bbq");
  ObString generated_name;

  ASSERT_EQ(OB_SUCCESS, ObVecIndexBuilderUtil::generate_vec_index_name(
      &allocator, INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, index_name, generated_name));
  ASSERT_EQ(0, index_name.compare(generated_name));
  ASSERT_NE(index_name.ptr(), generated_name.ptr());

  ASSERT_EQ(OB_ERR_KEY_NAME_DUPLICATE,
      ObVecIndexBuilderUtil::generate_vec_index_name(
          &allocator,
          INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL,
          ObString::make_string("AsYnC_DeLeTe"),
          generated_name));
}

TEST_F(TestAsyncIndexDefine, test_async_delete_index_name_contract)
{
  using namespace share::schema;
  ObArenaAllocator allocator("AsyncDeleteName");
  obrpc::ObCreateIndexArg arg;
  const ObString original_name("user_index_name");
  arg.index_type_ = INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
  arg.index_name_ = original_name;

  ASSERT_EQ(OB_SUCCESS, ObIndexBuilderUtil::generate_async_delete_index_name(
      arg, &allocator));
  ASSERT_EQ(0, arg.index_name_.compare(
      ObString::make_string(ObIndexBuilderUtil::ASYNC_DELETE_INDEX_NAME)));
  ASSERT_NE(original_name.ptr(), arg.index_name_.ptr());

  obrpc::ObCreateIndexArg invalid_arg;
  invalid_arg.index_type_ = INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObIndexBuilderUtil::generate_async_delete_index_name(invalid_arg, &allocator));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObIndexBuilderUtil::generate_async_delete_index_name(arg, nullptr));
}

TEST_F(TestAsyncIndexDefine, test_async_delete_index_arg_normalization)
{
  using namespace share::schema;
  ObArenaAllocator allocator("AsyncDeleteArg");
  obrpc::ObCreateIndexArg arg;
  const ObString original_name("idx_user_async");
  arg.index_type_ = INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  arg.index_name_ = original_name;
  arg.index_option_.parser_name_ = ObString::make_string("unused_parser");
  obrpc::ObColumnSortItem source_item;
  source_item.column_name_ = ObString::make_string("vec");
  ASSERT_EQ(OB_SUCCESS, arg.index_columns_.push_back(source_item));
  ASSERT_EQ(OB_SUCCESS,
            arg.store_columns_.push_back(ObString::make_string("stored")));
  ASSERT_EQ(OB_SUCCESS,
            arg.hidden_store_columns_.push_back(
                ObString::make_string("hidden_stored")));

  ASSERT_EQ(OB_SUCCESS, ObIndexBuilderUtil::normalize_async_delete_index_arg(
      arg, &allocator));
  ASSERT_EQ(INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, arg.index_type_);
  ASSERT_EQ(1, arg.index_columns_.count());
  ASSERT_EQ(0, arg.store_columns_.count());
  ASSERT_EQ(0, arg.hidden_store_columns_.count());
  ASSERT_TRUE(arg.index_option_.parser_name_.empty());
  ASSERT_EQ(0, arg.index_columns_.at(0).column_name_.compare(
      ObString::make_string("__pk_increment")));
  ASSERT_EQ(0, arg.index_name_.compare(
      ObString::make_string(ObIndexBuilderUtil::ASYNC_DELETE_INDEX_NAME)));
  ASSERT_NE(original_name.ptr(), arg.index_name_.ptr());

  obrpc::ObCreateIndexArg invalid_arg;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObIndexBuilderUtil::normalize_async_delete_index_arg(
                invalid_arg, nullptr));
  invalid_arg.index_type_ = INDEX_TYPE_NORMAL_LOCAL;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObIndexBuilderUtil::normalize_async_delete_index_arg(
                invalid_arg, &allocator));
}

TEST_F(TestAsyncIndexDefine, test_disk_bbq_nbits_default_contract)
{
  using namespace share::schema;
  ObVectorIndexParam disk_bbq_param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2"), VIT_IVF_INDEX,
      disk_bbq_param, true));
  ASSERT_EQ(static_cast<int64_t>(share::DISK_BBQ_RABITQ_BITS),
      disk_bbq_param.nbits_);
  ASSERT_EQ(ObVectorIndexParam::DEFAULT_DISK_BBQ_CLUSTER_SIZE,
      disk_bbq_param.cluster_size_);

  ObVectorIndexParam min_cluster_param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2,CLUSTER_SIZE=64"),
      VIT_IVF_INDEX, min_cluster_param, true));
  ASSERT_EQ(ObVectorIndexParam::MIN_DISK_BBQ_CLUSTER_SIZE,
      min_cluster_param.cluster_size_);

  ObVectorIndexParam max_cluster_param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2,CLUSTER_SIZE=65536"),
      VIT_IVF_INDEX, max_cluster_param, true));
  ASSERT_EQ(ObVectorIndexParam::MAX_DISK_BBQ_CLUSTER_SIZE,
      max_cluster_param.cluster_size_);

  ObVectorIndexParam invalid_param;
  ASSERT_NE(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2,CLUSTER_SIZE=63"),
      VIT_IVF_INDEX, invalid_param, true));
  ASSERT_NE(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2,CLUSTER_SIZE=65537"),
      VIT_IVF_INDEX, invalid_param, true));
  ASSERT_NE(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=IVF_FLAT,DISTANCE=L2,CLUSTER_SIZE=384"),
      VIT_IVF_INDEX, invalid_param, true));

  ObVectorIndexParam legacy_disk_bbq_param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2,NLIST=128,SAMPLE_PER_NLIST=256"),
      VIT_IVF_INDEX, legacy_disk_bbq_param, true));
  ASSERT_EQ(ObVectorIndexParam::DEFAULT_DISK_BBQ_CLUSTER_SIZE,
      legacy_disk_bbq_param.cluster_size_);

  char print_buf[512] = {};
  int64_t print_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      disk_bbq_param.print_to_string(print_buf, sizeof(print_buf), print_pos));
  const std::string printed_params(print_buf, print_pos);
  ASSERT_NE(std::string::npos, printed_params.find("cluster_size=384"));
  ASSERT_EQ(std::string::npos, printed_params.find("nlist="));
  ASSERT_EQ(std::string::npos, printed_params.find("sample_per_nlist="));

  ObVectorIndexParam ivf_pq_param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=IVF_PQ,DISTANCE=L2"), VIT_IVF_INDEX,
      ivf_pq_param, true));
  ASSERT_EQ(8, ivf_pq_param.nbits_);
}

TEST_F(TestAsyncIndexDefine, test_disk_bbq_refine_k_parameter_roundtrip)
{
  ObVectorIndexParam index_param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2"), VIT_IVF_INDEX, index_param, true));
  ASSERT_FLOAT_EQ(4.0f, index_param.refine_k_);
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      ObString("TYPE=DISK_BBQ,DISTANCE=L2,REFINE_K=7.5"),
      VIT_IVF_INDEX, index_param, true));
  ASSERT_FLOAT_EQ(7.5f, index_param.refine_k_);

  char text_buf[512] = {};
  int64_t text_pos = 0;
  ASSERT_EQ(OB_SUCCESS, index_param.print_to_string(text_buf, sizeof(text_buf), text_pos));
  ASSERT_NE(std::string::npos, std::string(text_buf, text_pos).find("refine_k=7.500000"));

  char wire_buf[8192] = {};
  int64_t wire_pos = 0;
  ASSERT_EQ(OB_SUCCESS, index_param.serialize(wire_buf, sizeof(wire_buf), wire_pos));
  ObVectorIndexParam decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(wire_buf, wire_pos, read_pos));
  ASSERT_EQ(wire_pos, read_pos);
  ASSERT_EQ(VIAT_DISK_BBQ, decoded.type_);
  ASSERT_FLOAT_EQ(7.5f, decoded.refine_k_);
}

TEST_F(TestAsyncIndexDefine, test_disk_bbq_print_parameter_separators)
{
  for (const bool custom_refine : {false, true}) {
    SCOPED_TRACE(custom_refine);
    ObVectorIndexParam param;
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
        ObString("TYPE=DISK_BBQ,DISTANCE=L2,CLUSTER_SIZE=512"),
        VIT_IVF_INDEX, param, true));
    std::string expected_suffix = ",cluster_size=512";
    if (custom_refine) {
      param.refine_k_ = 7.5f;
      expected_suffix += ",refine_k=7.500000";
    }
    char buf[512] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, param.print_to_string(buf, sizeof(buf), pos));
    const std::string printed(buf, pos);
    ASSERT_GE(printed.size(), expected_suffix.size());
    EXPECT_EQ(expected_suffix, printed.substr(printed.size() - expected_suffix.size()));
  }
}

TEST_F(TestAsyncIndexDefine, test_disk_bbq_refine_k_ddl_validation)
{
  ObArenaAllocator allocator("BBQParamDDL");
  std::unique_ptr<sql::ObSQLSessionInfo> session(new sql::ObSQLSessionInfo());
  share::schema::ObTableSchema schema;
  schema.set_tenant_id(1001);
  schema.set_table_id(50001);
  schema.set_table_type(share::schema::USER_TABLE);
  share::schema::ObColumnSchemaV2 column;
  column.set_tenant_id(1001);
  column.set_table_id(50001);
  column.set_column_id(OB_APP_MIN_COLUMN_ID);
  column.set_column_name("vec");
  column.set_data_type(ObCollectionSQLType);
  column.set_collation_type(CS_TYPE_BINARY);
  ObSEArray<ObString, 1> type_info;
  ASSERT_EQ(OB_SUCCESS, type_info.push_back(ObString("VECTOR(64)")));
  ASSERT_EQ(OB_SUCCESS, column.set_extended_type_info(type_info));
  ASSERT_EQ(OB_SUCCESS, schema.add_column(column));
  ObString column_name("vec");
  const char *values[] = {"TYPE", "DISK_BBQ", "DISTANCE", "L2", "REFINE_K", "7.5"};
  ParseNode nodes[6] = {};
  ParseNode *children[6] = {};
  for (int i = 0; i < 6; ++i) {
    nodes[i].type_ = i == 5 ? T_NUMBER : T_IDENT;
    nodes[i].str_value_ = values[i];
    nodes[i].str_len_ = STRLEN(values[i]);
    children[i] = &nodes[i];
  }
  ParseNode options = {};
  options.type_ = T_VEC_INDEX_PARAMS;
  options.num_child_ = 6;
  options.children_ = children;
  ObString params;
  options.str_value_ = "TYPE=DISK_BBQ,DISTANCE=L2,REFINE_K=7.5";
  options.str_len_ = STRLEN(options.str_value_);
  share::schema::ObIndexType type = share::schema::INDEX_TYPE_MAX;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::check_vec_index_param(
      1001, &options, allocator, schema, params, column_name, type, session.get()));
  ASSERT_EQ(share::schema::INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL, type);
  ObVectorIndexParam parsed;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(params, VIT_IVF_INDEX, parsed, true));
  ASSERT_FLOAT_EQ(7.5F, parsed.refine_k_);

  nodes[4].str_value_ = "REFINE_TYPE";
  nodes[4].str_len_ = STRLEN("REFINE_TYPE");
  nodes[5].str_value_ = "1";
  nodes[5].str_len_ = 1;
  nodes[5].value_ = 1;
  options.str_value_ = "TYPE=DISK_BBQ,DISTANCE=L2,REFINE_TYPE=1";
  options.str_len_ = STRLEN(options.str_value_);
  params.reset();
  ASSERT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::check_vec_index_param(
      1001, &options, allocator, schema, params, column_name, type, session.get()));
}

TEST_F(TestAsyncIndexDefine, test_disk_bbq_refine_k_query_override)
{
  ObVectorIndexParam index_param;
  index_param.type_ = VIAT_DISK_BBQ;
  index_param.refine_k_ = 7.5f;
  ObVectorIndexQueryParam query_param;
  ObVectorIndexParam search_param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexParam::build_search_param(index_param, query_param, search_param));
  ASSERT_FLOAT_EQ(7.5f, search_param.refine_k_);
  query_param.is_set_refine_k_ = 1;
  query_param.refine_k_ = 2.5f;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexParam::build_search_param(index_param, query_param, search_param));
  ASSERT_FLOAT_EQ(2.5f, search_param.refine_k_);
  ASSERT_FLOAT_EQ(7.5f, index_param.refine_k_);

  index_param.type_ = VIAT_IVF_FLAT;
  query_param.refine_k_ = 4.0f;
  ASSERT_EQ(OB_NOT_SUPPORTED,
      ObVectorIndexParam::build_search_param(index_param, query_param, search_param));
}

TEST_F(TestAsyncIndexDefine, test_disk_bbq_zero_nprobes_query_mode)
{
  ParseNode key = {};
  key.type_ = T_IDENT;
  key.str_value_ = "IVF_NPROBES";
  key.str_len_ = STRLEN(key.str_value_);
  ParseNode value = {};
  value.type_ = T_INT;
  value.value_ = 0;
  ParseNode *children[] = {&key, &value};
  ParseNode params = {};
  params.num_child_ = 2;
  params.children_ = children;
  ObVectorIndexQueryParam query_param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::resolve_query_param(&params, query_param));
  ASSERT_TRUE(query_param.is_set_ivf_nprobes_);
  ASSERT_EQ(0, query_param.ivf_nprobes_);
  ObVectorIndexParam index_param;
  ObVectorIndexParam search_param;
  index_param.type_ = VIAT_DISK_BBQ;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexParam::build_search_param(index_param, query_param, search_param));
  index_param.type_ = VIAT_IVF_FLAT;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObVectorIndexParam::build_search_param(index_param, query_param, search_param));

  key.str_value_ = "NPROBES";
  key.str_len_ = STRLEN(key.str_value_);
  query_param = ObVectorIndexQueryParam();
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::resolve_query_param(&params, query_param));
  for (const int64_t invalid : {-1L, 65537L}) {
    value.value_ = invalid;
    query_param = ObVectorIndexQueryParam();
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObVectorIndexUtil::resolve_query_param(&params, query_param));
  }
  value.value_ = 0;
  value.type_ = T_NUMBER;
  for (const char *zero : {"0.0", "00.000", "-0.0", ".0"}) {
    value.str_value_ = zero;
    value.str_len_ = STRLEN(zero);
    query_param = ObVectorIndexQueryParam();
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::resolve_query_param(&params, query_param));
    ASSERT_TRUE(query_param.is_set_ivf_nprobes_);
    ASSERT_EQ(0, query_param.ivf_nprobes_);
    index_param.type_ = VIAT_DISK_BBQ;
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexParam::build_search_param(index_param, query_param, search_param));
    index_param.type_ = VIAT_IVF_FLAT;
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObVectorIndexParam::build_search_param(index_param, query_param, search_param));
  }
  // A decimal's unused integer slot must not turn nonzero values into auto.
  const std::string tiny = "0." + std::string(400, '0') + "1";
  for (const char *invalid : {"0.5", "1.5", "65537.0", tiny.c_str()}) {
    value.str_value_ = invalid;
    value.str_len_ = STRLEN(invalid);
    query_param = ObVectorIndexQueryParam();
    ASSERT_EQ(OB_INVALID_ARGUMENT, ObVectorIndexUtil::resolve_query_param(&params, query_param));
  }
  value.type_ = T_BOOL;
  query_param = ObVectorIndexQueryParam();
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObVectorIndexUtil::resolve_query_param(&params, query_param));
}

TEST_F(TestAsyncIndexDefine, test_disk_bbq_refine_k_rejects_invalid_values)
{
  const char *invalid_values[] = {"0.5", "1001", "2x"};
  for (const char *value : invalid_values) {
    const std::string params = std::string("TYPE=DISK_BBQ,DISTANCE=L2,REFINE_K=") + value;
    ObVectorIndexParam index_param;
    ASSERT_NE(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
        ObString(static_cast<int32_t>(params.size()), params.data()), VIT_IVF_INDEX, index_param, true));
  }
}

TEST_F(TestAsyncIndexDefine, test_existing_query_parameter_boundaries)
{
  struct ParamCase {
    const char *name_;
    ObItemType type_;
    int64_t integer_;
    const char *literal_;
    int expected_;
  } cases[] = {
    {"IVF_NPROBES", T_INT, 1, "1", OB_SUCCESS},
    {"nPrObEs", T_INT, 65536, "65536", OB_SUCCESS},
    {"IVF_NPROBES", T_INT, 65537, "65537", OB_INVALID_ARGUMENT},
    {"IVF_NPROBES", T_NUMBER, 0, "1.0", OB_INVALID_ARGUMENT},
    {"IVF_NPROBES", T_NUMBER, 0, "18446744073709551616", OB_INVALID_ARGUMENT},
    {"EF_SEARCH", T_INT, 1, "1", OB_SUCCESS},
    {"EF_SEARCH", T_INT, 64, "64", OB_SUCCESS},
    {"EF_SEARCH", T_INT, 0, "0", OB_INVALID_ARGUMENT},
    {"EF_SEARCH", T_NUMBER, 0, "0.0", OB_INVALID_ARGUMENT},
    {"REFINE_K", T_NUMBER, 0, "1.0", OB_SUCCESS},
    {"REFINE_K", T_NUMBER, 0, "1000.0", OB_SUCCESS},
    {"REFINE_K", T_INT, 0, "0", OB_INVALID_ARGUMENT},
    {"REFINE_K", T_NUMBER, 0, "0.0", OB_INVALID_ARGUMENT},
    {"REFINE_K", T_NUMBER, 0, "0.5", OB_INVALID_ARGUMENT},
    {"REFINE_K", T_INT, 1001, "1001", OB_INVALID_ARGUMENT},
  };
  for (const auto &item : cases) {
    SCOPED_TRACE(item.name_);
    SCOPED_TRACE(item.literal_);
    ParseNode key = {}, value = {}, params = {};
    key.type_ = T_IDENT;
    key.str_value_ = item.name_;
    key.str_len_ = STRLEN(item.name_);
    value.type_ = item.type_;
    value.value_ = item.integer_;
    value.str_value_ = item.literal_;
    value.str_len_ = STRLEN(item.literal_);
    ParseNode *children[] = {&key, &value};
    params.num_child_ = 2;
    params.children_ = children;
    ObVectorIndexQueryParam query;
    ASSERT_EQ(item.expected_, ObVectorIndexUtil::resolve_query_param(&params, query));
  }
}

TEST_F(TestAsyncIndexDefine, test_existing_index_query_parameters_remain_compatible)
{
  for (const auto type : {VIAT_IVF_FLAT, VIAT_IVF_PQ, VIAT_HNSW, VIAT_HNSW_SQ,
                         VIAT_HNSW_BQ, VIAT_IPIVF, VIAT_IPIVF_SQ, VIAT_DISK_BBQ}) {
    SCOPED_TRACE(static_cast<int>(type));
    ObVectorIndexParam index, search;
    index.type_ = type;
    index.refine_k_ = 7.5F;
    index.ef_search_ = 55;
    ObVectorIndexQueryParam query;
    query.is_set_ivf_nprobes_ = 1;
    query.ivf_nprobes_ = 10;
    query.is_set_ef_search_ = 1;
    query.ef_search_ = 71;
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexParam::build_search_param(index, query, search));
    ASSERT_EQ(71, search.ef_search_);
    ASSERT_FLOAT_EQ(7.5F, search.refine_k_);
    query.is_set_refine_k_ = 1;
    query.refine_k_ = 2.5F;
    const bool supports_refine = type == VIAT_HNSW_BQ || type == VIAT_IPIVF
        || type == VIAT_IPIVF_SQ || type == VIAT_DISK_BBQ;
    ASSERT_EQ(supports_refine ? OB_SUCCESS : OB_NOT_SUPPORTED,
        ObVectorIndexParam::build_search_param(index, query, search));
    if (supports_refine) { ASSERT_FLOAT_EQ(2.5F, search.refine_k_); }
    ASSERT_EQ(55, index.ef_search_);
    ASSERT_FLOAT_EQ(7.5F, index.refine_k_);
    query.is_set_refine_k_ = 0;
    query.ivf_nprobes_ = 0;
    ASSERT_EQ(type == VIAT_DISK_BBQ ? OB_SUCCESS : OB_INVALID_ARGUMENT,
        ObVectorIndexParam::build_search_param(index, query, search));
  }
}

TEST_F(TestAsyncIndexDefine, test_zero_nprobes_mixed_options_and_duplicate_aliases)
{
  ParseNode keys[3] = {}, values[3] = {}, params = {};
  const char *names[] = {"nPrObEs", "REFINE_K", "EF_SEARCH"};
  const char *literals[] = {"0.0", "7.5", "64"};
  for (int64_t i = 0; i < 3; ++i) {
    keys[i].type_ = T_IDENT;
    keys[i].str_value_ = names[i];
    keys[i].str_len_ = STRLEN(names[i]);
    values[i].type_ = i == 2 ? T_INT : T_NUMBER;
    values[i].str_value_ = literals[i];
    values[i].str_len_ = STRLEN(literals[i]);
    values[i].value_ = i == 2 ? 64 : 0;
  }
  for (const bool reverse : {false, true}) {
    ParseNode *children[6];
    for (int64_t i = 0; i < 3; ++i) {
      const int64_t slot = reverse ? 2 - i : i;
      children[2 * i] = &keys[slot];
      children[2 * i + 1] = &values[slot];
    }
    params.num_child_ = 6;
    params.children_ = children;
    ObVectorIndexQueryParam query;
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::resolve_query_param(&params, query));
    ASSERT_TRUE(query.is_set_ivf_nprobes_ && query.is_set_refine_k_ && query.is_set_ef_search_);
    ASSERT_EQ(0, query.ivf_nprobes_);
    ASSERT_FLOAT_EQ(7.5F, query.refine_k_);
    ASSERT_EQ(64, query.ef_search_);
  }
  keys[2].str_value_ = "IVF_NPROBES";
  keys[2].str_len_ = STRLEN(keys[2].str_value_);
  for (const bool reverse : {false, true}) {
    ParseNode *children[] = {&keys[reverse ? 2 : 0], &values[reverse ? 2 : 0],
                            &keys[reverse ? 0 : 2], &values[reverse ? 0 : 2]};
    params.num_child_ = 4;
    params.children_ = children;
    ObVectorIndexQueryParam query;
    ASSERT_EQ(OB_ERR_PARAM_DUPLICATE, ObVectorIndexUtil::resolve_query_param(&params, query));
  }
}

TEST_F(TestAsyncIndexDefine, test_async_ivf_generated_column_contract)
{
  using namespace share::schema;
  const uint64_t tenant_id = 1001;
  const uint64_t data_table_id = 50001;
  ObTableSchema data_schema;
  data_schema.set_tenant_id(tenant_id);
  data_schema.set_table_id(data_table_id);
  data_schema.set_database_id(1);
  data_schema.set_table_type(USER_TABLE);
  data_schema.set_table_name("t_async_ivf");
  data_schema.set_table_pk_mode(TPKM_TABLET_SEQ_PK);
  data_schema.set_table_pk_exists_mode(TOM_TABLE_WITHOUT_PK);

  ObColumnSchemaV2 source_column;
  source_column.set_tenant_id(tenant_id);
  source_column.set_table_id(data_table_id);
  source_column.set_column_id(OB_APP_MIN_COLUMN_ID);
  source_column.set_column_name("vec`source");
  source_column.set_data_type(ObCollectionSQLType);
  source_column.set_collation_type(CS_TYPE_BINARY);
  ObSEArray<ObString, 1> extended_type_info;
  ASSERT_EQ(OB_SUCCESS, extended_type_info.push_back(ObString("VECTOR(3)")));
  ASSERT_EQ(OB_SUCCESS, source_column.set_extended_type_info(extended_type_info));
  ASSERT_EQ(OB_SUCCESS, data_schema.add_column(source_column));

  ObColumnSchemaV2 hidden_pk;
  hidden_pk.set_tenant_id(tenant_id);
  hidden_pk.set_table_id(data_table_id);
  hidden_pk.set_column_id(OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  hidden_pk.set_column_name(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME);
  hidden_pk.set_data_type(ObUInt64Type);
  hidden_pk.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type]);
  hidden_pk.set_is_hidden(true);
  hidden_pk.set_nullable(false);
  hidden_pk.set_rowkey_position(1);
  hidden_pk.set_order_in_rowkey(ASC);
  ASSERT_EQ(OB_SUCCESS, data_schema.add_column(hidden_pk));

  // Async IVF follows the local IVF allowance for partitioned heap tables.
  data_schema.set_part_level(PARTITION_LEVEL_ONE);

  obrpc::ObCreateIndexArg arg;
  arg.index_type_ = INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
  obrpc::ObColumnSortItem source_item;
  source_item.column_name_ = ObString("vec`source");
  ASSERT_EQ(OB_SUCCESS, arg.index_columns_.push_back(source_item));
  ObSEArray<ObColumnSchemaV2 *, 3> generated_columns;
  ASSERT_EQ(OB_SUCCESS, share::ObIndexBuilderUtil::adjust_expr_index_args(
      arg, data_schema, arg.allocator_, generated_columns));
  ASSERT_EQ(3, generated_columns.count());
  ASSERT_EQ(3, arg.index_columns_.count());
  ASSERT_EQ(1, arg.store_columns_.count());

  int64_t type_count = 0;
  int64_t cid_count = 0;
  int64_t data_count = 0;
  for (int64_t i = 0; i < generated_columns.count(); ++i) {
    const ObColumnSchemaV2 *column = generated_columns.at(i);
    ASSERT_NE(nullptr, column);
    ASSERT_TRUE(column->is_hidden());
    ASSERT_TRUE(column->is_virtual_generated_column());
    ASSERT_TRUE(column->is_vec_index_column());
    ObSEArray<uint64_t, 1> cascaded_ids;
    ASSERT_EQ(OB_SUCCESS, column->get_cascaded_column_ids(cascaded_ids));
    ASSERT_EQ(1, cascaded_ids.count());
    ASSERT_EQ(source_column.get_column_id(), cascaded_ids.at(0));
    type_count += column->is_vec_ivf_type_column();
    cid_count += column->is_vec_ivf_cid_column();
    data_count += column->is_vec_ivf_data_column();
    const char *expected_expr = column->is_vec_ivf_type_column()
        ? "VEC_IVF_TYPE(`vec``source`)"
        : column->is_vec_ivf_cid_column()
            ? "VEC_IVF_CID(`vec``source`)" : "VEC_IVF_DATA(`vec``source`)";
    ASSERT_TRUE(column->get_cur_default_value().get_string() == ObString(expected_expr));
    ASSERT_TRUE(column->get_orig_default_value().get_string() == ObString(expected_expr));
    const int64_t role_flag = column->is_vec_ivf_type_column()
        ? GENERATED_VEC_IVF_TYPE_COLUMN_FLAG
        : column->is_vec_ivf_cid_column()
            ? GENERATED_VEC_IVF_CID_COLUMN_FLAG : GENERATED_VEC_IVF_DATA_COLUMN_FLAG;
    ObColumnSchemaV2 reloaded_column;
    ASSERT_EQ(OB_SUCCESS, reloaded_column.assign(*column));
    reloaded_column.del_column_flag(role_flag);
    ASSERT_FALSE(ObSchemaUtils::is_vec_ivf_type_column(reloaded_column.get_column_flags())
                 || ObSchemaUtils::is_vec_ivf_cid_column(reloaded_column.get_column_flags())
                 || ObSchemaUtils::is_vec_ivf_data_column(reloaded_column.get_column_flags()));
    ASSERT_EQ(OB_SUCCESS, ObSchemaUtils::cascaded_generated_column(
        data_schema, reloaded_column, false));
    ASSERT_EQ(column->is_vec_ivf_type_column(), reloaded_column.is_vec_ivf_type_column());
    ASSERT_EQ(column->is_vec_ivf_cid_column(), reloaded_column.is_vec_ivf_cid_column());
    ASSERT_EQ(column->is_vec_ivf_data_column(), reloaded_column.is_vec_ivf_data_column());
    if (column->is_vec_ivf_data_column()) {
      ASSERT_NE(nullptr, strstr(column->get_column_name(), OB_VEC_IVF_DATA_COLUMN_NAME_PREFIX));
      ASSERT_EQ(ObVarcharType, column->get_data_type());
      ASSERT_EQ(OB_MAX_VARCHAR_LENGTH, column->get_data_length());
      ASSERT_EQ(CS_TYPE_BINARY, column->get_collation_type());
      ASSERT_EQ(ObCharset::charset_type_by_coll(CS_TYPE_BINARY), column->get_charset_type());
    } else {
      const char *expected_prefix = column->is_vec_ivf_type_column()
          ? OB_VEC_IVF_TYPE_COLUMN_NAME_PREFIX : OB_VEC_IVF_CID_COLUMN_NAME_PREFIX;
      ASSERT_NE(nullptr, strstr(column->get_column_name(), expected_prefix));
      ASSERT_EQ(ObIntType, column->get_data_type());
    }
  }
  ASSERT_EQ(1, type_count);
  ASSERT_EQ(1, cid_count);
  ASSERT_EQ(1, data_count);

  ObTableSchema index_schema;
  index_schema.set_tenant_id(tenant_id);
  index_schema.set_table_id(60001);
  index_schema.set_data_table_id(data_table_id);
  index_schema.set_table_type(USER_INDEX);
  index_schema.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  index_schema.set_table_name("idx_async_ivf");
  ASSERT_EQ(OB_SUCCESS,
      index_schema.set_index_params(ObString("TYPE=DISK_BBQ,DISTANCE=L2")));
  ASSERT_EQ(OB_SUCCESS, share::ObIndexBuilderUtil::set_index_table_columns(
      arg, data_schema, index_schema, false));
  ASSERT_EQ(3, index_schema.get_rowkey_column_num());
  for (int64_t i = 0; i < generated_columns.count(); ++i) {
    const ObColumnSchemaV2 *generated = generated_columns.at(i);
    const ObColumnSchemaV2 *physical = index_schema.get_column_schema(generated->get_column_id());
    ASSERT_NE(nullptr, physical);
    ASSERT_EQ(generated->get_column_name_str(), physical->get_column_name_str());
    ASSERT_FALSE(physical->is_virtual_generated_column());
    ASSERT_TRUE(physical->is_vec_index_column());
    ASSERT_EQ(generated->is_nullable(), physical->is_nullable());
  }
  const ObColumnSchemaV2 *physical_pk =
      index_schema.get_column_schema(OB_HIDDEN_PK_INCREMENT_COLUMN_ID);
  ASSERT_NE(nullptr, physical_pk);
  ASSERT_EQ(ObUInt64Type, physical_pk->get_data_type());

  // Index-table role flags use high bits that are not persisted in
  // __all_column. Source-column resolution must rely on the generated column
  // in the data table, just as async FTS resolves dependencies from its data
  // table generated column.
  for (int64_t i = 0; i < generated_columns.count(); ++i) {
    const ObColumnSchemaV2 *generated = generated_columns.at(i);
    ObColumnSchemaV2 *physical = index_schema.get_column_schema(generated->get_column_id());
    ASSERT_NE(nullptr, physical);
    if (generated->is_vec_ivf_type_column()) {
      physical->del_column_flag(GENERATED_VEC_IVF_TYPE_COLUMN_FLAG);
    } else if (generated->is_vec_ivf_cid_column()) {
      physical->del_column_flag(GENERATED_VEC_IVF_CID_COLUMN_FLAG);
    } else if (generated->is_vec_ivf_data_column()) {
      physical->del_column_flag(GENERATED_VEC_IVF_DATA_COLUMN_FLAG);
    }
  }
  const ObColumnSchemaV2 *resolved_source = nullptr;
  ASSERT_EQ(OB_SUCCESS, share::ObIndexBuilderUtil::get_async_ivf_source_column(
      data_schema, index_schema, resolved_source));
  ASSERT_EQ(source_column.get_column_id(), resolved_source->get_column_id());

  bool is_match = false;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::check_vector_index_column_id_match(
      data_schema, index_schema, source_column.get_column_id(), is_match));
  ASSERT_TRUE(is_match);
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::check_vector_index_column_id_match(
      data_schema, index_schema, hidden_pk.get_column_id(), is_match));
  ASSERT_FALSE(is_match);

  ObSEArray<uint64_t, 1> vector_column_ids;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::get_vector_index_column_id(
      data_schema, index_schema, vector_column_ids));
  ASSERT_EQ(1, vector_column_ids.count());
  ASSERT_EQ(source_column.get_column_id(), vector_column_ids.at(0));

  ObSEArray<ObString, 1> vector_column_names;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::get_vector_index_column_name(
      data_schema, index_schema, vector_column_names));
  ASSERT_EQ(1, vector_column_names.count());
  ASSERT_EQ(source_column.get_column_name_str(), vector_column_names.at(0));

  ObVecIdxExtraInfo extra_info;
  ASSERT_EQ(OB_SUCCESS, extra_info.set_vec_param_info(&index_schema));
  ASSERT_TRUE(extra_info.is_async_index_);
  ASSERT_TRUE(extra_info.is_async_ivf_scan());
  ASSERT_FALSE(extra_info.is_ivf_vec_scan());
  ASSERT_EQ(VIAT_DISK_BBQ, extra_info.get_vector_index_param().type_);
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_reset)
{
  ObRefreshTaskStat stat;
  ASSERT_EQ(OB_INVALID_TENANT_ID, stat.tenant_id_);
  ASSERT_FALSE(stat.data_tablet_id_.is_valid());
  ASSERT_FALSE(stat.tablet_id_.is_valid());
  ASSERT_EQ(0, stat.refresh_id_);
  ASSERT_EQ(0, stat.retry_cnt_);
  ASSERT_EQ(ASYNC_REFRESH_TYPE_PERIOD, stat.refresh_type_);
  ASSERT_EQ(0, stat.queue_time_);
  ASSERT_EQ(0, stat.start_time_);
  ASSERT_EQ(0, stat.end_time_);
  ASSERT_EQ(0, stat.affected_rows_);
  ASSERT_FALSE(stat.start_refresh_scn_.is_valid());
  ASSERT_FALSE(stat.target_refresh_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, stat.result_);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, stat.refresh_parallelism_);
  ASSERT_EQ(0, stat.schema_version_);
  ASSERT_EQ(0, stat.leader_epoch_);
  ASSERT_FALSE(stat.is_search_index_);
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_is_valid)
{
  ObRefreshTaskStat stat;
  // after reset, stat should be invalid (missing required fields)
  ASSERT_FALSE(stat.is_valid());

  // set required fields
  stat.tenant_id_ = 1001;
  stat.data_tablet_id_ = ObTabletID(200001);
  stat.tablet_id_ = ObTabletID(200002);
  stat.target_refresh_scn_ = SCN::min_scn();
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
  ASSERT_FALSE(stat.is_valid());

  // invalid refresh type
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_MAX;
  ASSERT_FALSE(stat.is_valid());

  // restore and invalidate tenant_id
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
  stat.tenant_id_ = OB_INVALID_TENANT_ID;
  ASSERT_FALSE(stat.is_valid());
}

TEST_F(TestAsyncIndexDefine, test_retry_delay_calculation)
{
  // first 3 retries: immediate (0 delay)
  ASSERT_EQ(0, calc_async_index_retry_delay_us(0));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(1));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(2));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(3));

  // 4th retry: 5s
  ASSERT_EQ(5L * 1000L * 1000L, calc_async_index_retry_delay_us(4));
  // 5th retry: 10s
  ASSERT_EQ(10L * 1000L * 1000L, calc_async_index_retry_delay_us(5));
  // 6th retry: 20s
  ASSERT_EQ(20L * 1000L * 1000L, calc_async_index_retry_delay_us(6));
  // 7th retry: 40s
  ASSERT_EQ(40L * 1000L * 1000L, calc_async_index_retry_delay_us(7));
  // 8th retry: 80s
  ASSERT_EQ(80L * 1000L * 1000L, calc_async_index_retry_delay_us(8));
  // 9th retry: 160s
  ASSERT_EQ(160L * 1000L * 1000L, calc_async_index_retry_delay_us(9));
  // 10th retry: capped at 5 min = 300s
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US, calc_async_index_retry_delay_us(10));
  // large retry count: still capped
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US, calc_async_index_retry_delay_us(100));
}

TEST_F(TestAsyncIndexDefine, test_retryable_error_codes)
{
  // A new transaction can recover these failures while reusing the original SQL.
  ASSERT_TRUE(is_async_index_retryable_error(OB_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_RPC_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_CONNECT_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_SEND_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_POST_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_ROLLBACKED));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_NEED_ROLLBACK));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_STMT_NEED_RETRY));

  ASSERT_FALSE(is_async_index_retryable_error(OB_SUCCESS));
  ASSERT_FALSE(is_async_index_retryable_error(OB_SCHEMA_ERROR));
  ASSERT_FALSE(is_async_index_retryable_error(OB_TABLE_NOT_EXIST));
  ASSERT_FALSE(is_async_index_retryable_error(OB_TENANT_NOT_EXIST));
  // stale ob_ddl_schema_version hint: SQL fails at resolve, regenerated next round
  ASSERT_FALSE(is_async_index_retryable_error(OB_DDL_SCHEMA_VERSION_NOT_MATCH));
  ASSERT_FALSE(is_async_index_retryable_error(OB_ERR_UNEXPECTED));

  // index tablet lock conflict: timer path skips retry, next round reschedules
  ASSERT_FALSE(is_async_index_retryable_error(OB_EAGAIN));

  // Errors outside the retry whitelist release the batch for the next timer round.
  ASSERT_FALSE(is_async_index_retryable_error(OB_NOT_MASTER));
  ASSERT_FALSE(is_async_index_retryable_error(OB_SIZE_OVERFLOW));
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_to_string)
{
  ObRefreshTaskStat stat;
  stat.tenant_id_ = 1001;
  stat.data_tablet_id_ = ObTabletID(200001);
  stat.tablet_id_ = ObTabletID(200002);
  stat.refresh_id_ = 42;
  stat.target_refresh_scn_ = SCN::min_scn();
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_CREATE_INDEX;

  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  pos = stat.to_string(buf, buf_len);
  ASSERT_GT(pos, 0);
  // verify key fields appear in output
  ASSERT_NE(nullptr, strstr(buf, "tenant_id"));
  ASSERT_NE(nullptr, strstr(buf, "CREATE_INDEX"));
}

class TestAsyncIndexDepsMemory : public ::testing::Test
{
protected:
  static constexpr uint64_t TENANT_ID = 1002;
  static constexpr uint64_t OTHER_TENANT_ID = 1004;
  static void SetUpTestCase()
  {
    lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
    for (uint64_t tenant_id : {TENANT_ID, OTHER_TENANT_ID}) {
      if (nullptr == allocator->get_tenant_ctx_allocator(tenant_id, ObCtxIds::DEFAULT_CTX_ID)) {
        ASSERT_EQ(OB_SUCCESS, allocator->create_and_add_tenant_allocator(tenant_id));
      }
      ASSERT_EQ(OB_SUCCESS, allocator->set_tenant_limit(tenant_id, 1L << 30));
    }
  }
  static int64_t used(uint64_t tenant_id, const char *label = "AsyncIdxDeps")
  {
    // Read live objects under the allocator's locks. The public label-usage
    // API reads a periodic memory-dump snapshot and cannot detect a leak in
    // a single assignment/reset without waiting for that background task.
    auto allocator = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
        tenant_id, ObCtxIds::DEFAULT_CTX_ID);
    EXPECT_NE(nullptr, allocator);
    int64_t bytes = 0;
    if (nullptr != allocator) {
      allocator->do_cleanup();
      auto &mgr = allocator->obj_mgr_;
      for (int64_t i = 0; i < mgr.parallel_; ++i) {
        for (auto &size_class : mgr.obj_sets_[i].scs) {
          size_class.lock_.lock();
          for (lib::ABlock *head : {size_class.avail_blist_, size_class.full_blist_}) {
            lib::ABlock *block = head;
            if (nullptr != block) {
              do {
                lib::AObject *object = reinterpret_cast<lib::AObject *>(block->data());
                const int64_t cells = lib::AllocHelper::cells_per_block(block->ablock_size_);
                while (true) {
                  if (object->in_use_ && 0 == STRCMP(label, object->label_)) {
                    bytes += object->alloc_bytes_;
                  }
                  if (object->is_large_ || object->is_last(cells)) {
                    break;
                  }
                  object = object->phy_next(object->nobjs_);
                }
                block = block->next_;
              } while (block != head);
            }
          }
          size_class.lock_.unlock();
        }
      }
    }
    return bytes;
  }
  static void fill_index(ObAsyncIndexInfo &info)
  {
    info.index_table_id_ = 200001;
    info.def_index_table_id_ = 200002;
    info.index_type_ = schema::INDEX_TYPE_NORMAL_LOCAL;
    info.index_table_name_ = ObString::make_string("async_index");
    info.enable_newsort_ = true;
    info.is_delete_index_ = true;
    info.is_refresh_on_error_skip_ = true;
    // Overflow every local array, including the 16-entry partition arrays.
    for (int64_t i = 0; i < 17; ++i) {
      ASSERT_EQ(OB_SUCCESS, info.select_columns_.push_back(ObString::make_string("selected")));
      ASSERT_EQ(OB_SUCCESS, info.source_columns_.push_back(ObString::make_string("source")));
      ASSERT_EQ(OB_SUCCESS, info.partition_key_columns_.push_back(ObString::make_string("partition_key")));
      ASSERT_EQ(OB_SUCCESS, info.rowkey_columns_.push_back(ObString::make_string("rowkey")));
      ASSERT_EQ(OB_SUCCESS, info.order_columns_.push_back(ObString::make_string("order_key")));
      ASSERT_EQ(OB_SUCCESS, info.tablet_ids_.push_back(ObTabletID(300000 + i)));
      ASSERT_EQ(OB_SUCCESS, info.partition_names_.push_back(ObString::make_string("p0")));
    }
  }
  static void fill_table(ObAsyncIndexDataTableInfo &info)
  {
    info.data_table_id_ = 100001;
    info.database_name_ = ObString::make_string("db");
    info.data_table_name_ = ObString::make_string("data_table");
    info.schema_version_ = 12345;
    info.refresh_interval_us_ = 3000000;
    for (int64_t i = 0; i < 17; ++i) {
      ObAsyncIndexDataTabletInfo tablet;
      tablet.data_tablet_id_ = ObTabletID(400000 + i);
      tablet.partition_name_ = ObString::make_string("p0");
      tablet.data_ls_id_ = ObLSID(1001);
      ASSERT_EQ(OB_SUCCESS, info.data_tablet_infos_.push_back(tablet));
    }
    ObAsyncIndexInfo index(TENANT_ID);
    fill_index(index);
    // Overflow index_infos_ as well, exercising copies during expansion.
    for (int64_t i = 0; i < 5; ++i) {
      index.index_table_id_ = 200001 + i;
      ASSERT_EQ(OB_SUCCESS, info.index_infos_.push_back(index));
    }
  }
  template <typename T, int64_t N>
  static void check_array(const ObSEArray<T, N> &actual, const ObSEArray<T, N> &expected)
  {
    EXPECT_EQ(TENANT_ID, actual.block_allocator_.get_attr().tenant_id_);
    EXPECT_EQ(lib::ObLabel("AsyncIdxDeps"), actual.block_allocator_.get_attr().label_);
    ASSERT_EQ(expected.count(), actual.count());
    ASSERT_NE(expected.get_data(), actual.get_data());
    for (int64_t i = 0; i < actual.count(); ++i) {
      EXPECT_EQ(expected.at(i), actual.at(i));
    }
  }
  static void check_table(const ObAsyncIndexDataTableInfo &actual, const ObAsyncIndexDataTableInfo &expected)
  {
    EXPECT_EQ(expected.data_table_id_, actual.data_table_id_);
    EXPECT_EQ(expected.database_name_, actual.database_name_);
    EXPECT_EQ(expected.data_table_name_, actual.data_table_name_);
    EXPECT_EQ(expected.schema_version_, actual.schema_version_);
    EXPECT_EQ(expected.refresh_interval_us_, actual.refresh_interval_us_);
    EXPECT_EQ(TENANT_ID, actual.data_tablet_infos_.block_allocator_.get_attr().tenant_id_);
    EXPECT_EQ(TENANT_ID, actual.index_infos_.block_allocator_.get_attr().tenant_id_);
    EXPECT_EQ(lib::ObLabel("AsyncIdxDeps"), actual.data_tablet_infos_.block_allocator_.get_attr().label_);
    EXPECT_EQ(lib::ObLabel("AsyncIdxDeps"), actual.index_infos_.block_allocator_.get_attr().label_);
    ASSERT_EQ(expected.data_tablet_infos_.count(), actual.data_tablet_infos_.count());
    for (int64_t i = 0; i < actual.data_tablet_infos_.count(); ++i) {
      EXPECT_EQ(expected.data_tablet_infos_.at(i).data_tablet_id_, actual.data_tablet_infos_.at(i).data_tablet_id_);
      EXPECT_EQ(expected.data_tablet_infos_.at(i).partition_name_, actual.data_tablet_infos_.at(i).partition_name_);
      EXPECT_EQ(expected.data_tablet_infos_.at(i).data_ls_id_, actual.data_tablet_infos_.at(i).data_ls_id_);
    }
    ASSERT_EQ(expected.index_infos_.count(), actual.index_infos_.count());
    for (int64_t i = 0; i < actual.index_infos_.count(); ++i) {
      const ObAsyncIndexInfo &a = actual.index_infos_.at(i);
      const ObAsyncIndexInfo &e = expected.index_infos_.at(i);
      EXPECT_EQ(e.index_table_id_, a.index_table_id_);
      EXPECT_EQ(e.def_index_table_id_, a.def_index_table_id_);
      EXPECT_EQ(e.index_type_, a.index_type_);
      EXPECT_EQ(e.index_table_name_, a.index_table_name_);
      EXPECT_EQ(e.enable_newsort_, a.enable_newsort_);
      EXPECT_EQ(e.is_delete_index_, a.is_delete_index_);
      EXPECT_EQ(e.is_refresh_on_error_skip_, a.is_refresh_on_error_skip_);
      check_array(a.select_columns_, e.select_columns_);
      check_array(a.source_columns_, e.source_columns_);
      check_array(a.partition_key_columns_, e.partition_key_columns_);
      check_array(a.rowkey_columns_, e.rowkey_columns_);
      check_array(a.order_columns_, e.order_columns_);
      check_array(a.tablet_ids_, e.tablet_ids_);
      check_array(a.partition_names_, e.partition_names_);
    }
  }
};

TEST_F(TestAsyncIndexDepsMemory, reserve_round_capacity_and_rebind_tenant)
{
  RecordProperty("data_table_info_bytes", static_cast<int>(sizeof(ObAsyncIndexDataTableInfo)));
  const int64_t initial_used = used(TENANT_ID);
  const int64_t server_used = used(OB_SERVER_TENANT_ID);
  const int64_t server_array_used = used(OB_SERVER_TENANT_ID, "SeArray");
  ObAsyncIndexRefreshDeps deps(TENANT_ID);
  for (int64_t count : {0, 8, 9, 4096, 4097, 4144}) {
    ASSERT_EQ(OB_SUCCESS, deps.prepare(TENANT_ID, count));
    const int64_t capacity = deps.data_table_infos_.get_capacity();
    EXPECT_EQ(std::max<int64_t>(8, count), capacity);
    if (4144 == count) {
      RecordProperty("reserved_4144_bytes", static_cast<int>(capacity * sizeof(ObAsyncIndexDataTableInfo)));
    }
    for (int64_t i = 0; i < count; ++i) {
      ObAsyncIndexDataTableInfo info(TENANT_ID);
      info.data_table_id_ = i + 1;
      ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.push_back(info));
    }
    EXPECT_EQ(capacity, deps.data_table_infos_.get_capacity());
    EXPECT_EQ(count, deps.data_table_infos_.count());
    ASSERT_NE(nullptr, deps.allocator_.alloc(1024));
    EXPECT_GT(used(TENANT_ID), initial_used);
    EXPECT_EQ(server_used, used(OB_SERVER_TENANT_ID));
    EXPECT_EQ(server_array_used, used(OB_SERVER_TENANT_ID, "SeArray"));
    deps.reset();
    EXPECT_EQ(initial_used, used(TENANT_ID));
  }
  const int64_t other_used = used(OTHER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, deps.prepare(OTHER_TENANT_ID, 9));
  ASSERT_NE(nullptr, deps.allocator_.alloc(1024));
  EXPECT_GT(used(OTHER_TENANT_ID), other_used);
  EXPECT_EQ(initial_used, used(TENANT_ID));
  EXPECT_EQ(OB_INVALID_ARGUMENT, deps.prepare(OB_INVALID_TENANT_ID, 9));
  EXPECT_TRUE(deps.empty());
  EXPECT_EQ(other_used, used(OTHER_TENANT_ID));
  EXPECT_EQ(OB_INVALID_ARGUMENT, deps.prepare(TENANT_ID, -1));
}

TEST_F(TestAsyncIndexDepsMemory, nested_copy_expansion_and_destruction)
{
  const int64_t initial_used = used(TENANT_ID);
  const int64_t server_used = used(OB_SERVER_TENANT_ID);
  const int64_t server_array_used = used(OB_SERVER_TENANT_ID, "SeArray");
  const int64_t server_page_used = used(OB_SERVER_TENANT_ID, "ModulePageAlloc");
  {
    ObAsyncIndexDataTableInfo source(TENANT_ID);
    fill_table(source);
    ObAsyncIndexRefreshDeps deps(TENANT_ID);
    for (int64_t i = 0; i < 9; ++i) {
      ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.push_back(source));
    }
    ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.reserve(20));
    ASSERT_EQ(OB_SUCCESS, source.assign(source));
    ASSERT_EQ(OB_SUCCESS, source.index_infos_.at(0).assign(source.index_infos_.at(0)));
    for (int64_t i = 0; i < deps.data_table_infos_.count(); ++i) {
      check_table(deps.data_table_infos_.at(i), source);
    }
    EXPECT_EQ(server_used, used(OB_SERVER_TENANT_ID));
    EXPECT_EQ(server_array_used, used(OB_SERVER_TENANT_ID, "SeArray"));
    EXPECT_EQ(server_page_used, used(OB_SERVER_TENANT_ID, "ModulePageAlloc"));
    source.reset();
    EXPECT_EQ(ObString::make_string("selected"), deps.data_table_infos_.at(0).index_infos_.at(0).select_columns_.at(16));
  }
  EXPECT_EQ(initial_used, used(TENANT_ID));
}

TEST_F(TestAsyncIndexDepsMemory, reserved_round_nested_arrays_charge_business_tenant)
{
  const int64_t initial_used = used(TENANT_ID);
  const int64_t server_used = used(OB_SERVER_TENANT_ID);
  const int64_t server_array_used = used(OB_SERVER_TENANT_ID, "SeArray");
  const int64_t server_page_used = used(OB_SERVER_TENANT_ID, "ModulePageAlloc");
  // Cover the inner inline-capacity boundary and a representative large round
  // from issues 2026102800120127223 / 2026102800120130573. Eleven indexes fall
  // in the observed 9..12-entry allocation bucket; the exact DDL is unknown.
  for (int64_t index_count : {4, 5, 11}) {
    {
      const int64_t table_count = 11 == index_count ? 4144 : 9;
      ObAsyncIndexDataTableInfo source(TENANT_ID);
      source.database_name_ = ObString::make_string("db");
      source.data_table_name_ = ObString::make_string("data_table");
      ObAsyncIndexDataTabletInfo tablet;
      tablet.data_tablet_id_ = ObTabletID(100001);
      ASSERT_EQ(OB_SUCCESS, source.data_tablet_infos_.push_back(tablet));
      ObAsyncIndexInfo index(TENANT_ID);
      index.index_table_name_ = ObString::make_string("async_index");
      ASSERT_EQ(OB_SUCCESS, index.select_columns_.push_back(ObString::make_string("c1")));
      ASSERT_EQ(OB_SUCCESS, index.tablet_ids_.push_back(ObTabletID(300001)));
      ASSERT_EQ(OB_SUCCESS, index.partition_names_.push_back(ObString()));
      for (int64_t i = 0; i < index_count; ++i) {
        index.index_table_id_ = 200001 + i;
        ASSERT_EQ(OB_SUCCESS, source.index_infos_.push_back(index));
      }
      ObAsyncIndexRefreshDeps deps(TENANT_ID);
      ASSERT_EQ(OB_SUCCESS, deps.prepare(TENANT_ID, table_count));
      for (int64_t i = 0; i < table_count; ++i) {
        source.data_table_id_ = 100001 + i;
        ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.push_back(source));
      }
      source.reset();
      EXPECT_EQ(table_count, deps.data_table_infos_.get_capacity());
      ASSERT_EQ(table_count, deps.data_table_infos_.count());
      for (int64_t i = 0; i < table_count; ++i) {
        const ObAsyncIndexDataTableInfo &copy = deps.data_table_infos_.at(i);
        EXPECT_EQ(100001 + i, copy.data_table_id_);
        ASSERT_EQ(index_count, copy.index_infos_.count());
        EXPECT_EQ(TENANT_ID, copy.index_infos_.block_allocator_.get_attr().tenant_id_);
        EXPECT_EQ(lib::ObLabel("AsyncIdxDeps"), copy.index_infos_.block_allocator_.get_attr().label_);
        EXPECT_EQ(ObString::make_string("c1"), copy.index_infos_.at(index_count - 1).select_columns_.at(0));
      }
      EXPECT_GT(used(TENANT_ID), initial_used);
      EXPECT_EQ(server_used, used(OB_SERVER_TENANT_ID));
      EXPECT_EQ(server_array_used, used(OB_SERVER_TENANT_ID, "SeArray"));
      EXPECT_EQ(server_page_used, used(OB_SERVER_TENANT_ID, "ModulePageAlloc"));
      if (4144 == table_count) {
        RecordProperty("tables_4144_business_dependency_bytes", used(TENANT_ID) - initial_used);
        RecordProperty("tables_4144_server_module_page_alloc_delta",
            used(OB_SERVER_TENANT_ID, "ModulePageAlloc") - server_page_used);
        RecordProperty("copied_index_buffer_bytes", deps.data_table_infos_.at(0).index_infos_.get_data_size());
      }
    }
    EXPECT_EQ(initial_used, used(TENANT_ID));
  }
}

// EN_4 applies to the real allocator, including arrays constructed inside a
// container copy. Always restore the event even when an assertion aborts.
class ScopedAsyncDepsAllocFailure
{
public:
  explicit ScopedAsyncDepsAllocFailure(int64_t frequency)
    : saved_(EventTable::EN_4.item_), saved_signal_handler_(signal(49, SIG_IGN))
  { TP_SET_EVENT(EventTable::EN_4, OB_ALLOCATE_MEMORY_FAILED, 0, frequency); }
  ~ScopedAsyncDepsAllocFailure()
  {
    EventTable::EN_4.item_.set_event(saved_);
    signal(49, saved_signal_handler_);
  }
private:
  EventItem saved_;
  // Allocation-failure diagnostics send observer's memory-dump signal.
  // There is no observer signal handler in this standalone unit test.
  void (*saved_signal_handler_)(int);
};

TEST_F(TestAsyncIndexDepsMemory, allocation_failure_keeps_source_and_releases_partial_copies)
{
  ObAsyncIndexDataTableInfo source(TENANT_ID);
  fill_table(source);
  const int64_t source_used = used(TENANT_ID);
  TP_SWITCH_GUARD(false);
  ObAsyncIndexRefreshDeps deps(TENANT_ID);
  {
    ScopedAsyncDepsAllocFailure failure(1);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, deps.prepare(TENANT_ID, 4144));
    EXPECT_TRUE(deps.empty());
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, deps.data_table_infos_.push_back(source));
    EXPECT_TRUE(deps.empty());
  }
  // Exercise failures at different depths, including after earlier indexes
  // have been fully copied. Every failed assignment must release its prefix.
  for (int64_t i = 0; i < 128; ++i) {
    ObAsyncIndexDataTableInfo copy(OTHER_TENANT_ID);
    int ret = OB_SUCCESS;
    {
      ScopedAsyncDepsAllocFailure failure(8);
      ret = copy.assign(source);
    }
    if (OB_SUCCESS == ret) {
      check_table(copy, source);
      copy.reset();
    } else {
      EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);
      EXPECT_FALSE(copy.is_valid());
      EXPECT_TRUE(copy.index_infos_.empty());
      EXPECT_TRUE(copy.data_tablet_infos_.empty());
    }
    ASSERT_EQ(source_used, used(TENANT_ID));
  }
  ASSERT_TRUE(source.is_valid());
  ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.push_back(source));
  check_table(deps.data_table_infos_.at(0), source);
  deps.reset();
  EXPECT_EQ(source_used, used(TENANT_ID));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::sql::ObExprOperatorFactory::register_expr_operators();
  if (OB_SUCCESS != oceanbase::common::ObClockGenerator::init()) {
    return 1;
  }
  testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  oceanbase::common::ObClockGenerator::destroy();
  return ret;
}
