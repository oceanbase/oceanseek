/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "../test_sql_utils.h"
#include "sql/plan_cache/ob_sql_parameterization.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace test
{
namespace
{
ParseNode *find_parse_node(ParseNode *node, const ObItemType type)
{
  ParseNode *found = NULL;
  if (NULL != node) {
    if (type == node->type_) {
      found = node;
    } else if (NULL != node->children_) {
      for (int64_t i = 0; NULL == found && i < node->num_child_; ++i) {
        found = find_parse_node(node->children_[i], type);
      }
    }
  }
  return found;
}
}

class TestParser: public TestSqlUtils, public ::testing::Test
{
public:
  TestParser();
  virtual ~TestParser();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestParser);
protected:
  // function members
  void do_parse(const char *query_str, std::ofstream &of_result, int64_t expect_error = OB_SUCCESS);
  void print_parse_tree(const char *query_str, std::ofstream &of_result, int64_t expect_error = OB_SUCCESS);
  void print_parse_outline(const char *query_str, std::ofstream &of_result, int64_t expect_error = OB_SUCCESS);
  void do_filter_hint(const char *query_str, std::ofstream &of_result, int64_t expect_error = OB_SUCCESS);
  bool pretreat_cmd(std::string line, int64_t &expect_error);
  void parse_keyword(std::ifstream &if_tests, std::ofstream &of_result, bool is_sql_keyword);
  void do_parse_keyword(const char *keyword, std::ofstream &of_result, bool is_reserved, bool is_sql_keyword);
  bool non_reserved_keyword_can_not_be_name(const char *keyword, bool is_sql_keyword);
protected:
  // data members
  ObArenaAllocator allocator_;
};

TestParser::TestParser() : allocator_(ObModIds::TEST)
{
}

TestParser::~TestParser()
{
}

void TestParser::SetUp()
{
  init();
}

void TestParser::TearDown()
{
}

TEST_F(TestParser, direct_load_stream_insert_target)
{
  // Stream 目标只接受正整数；参数化后 ID 保留字面量，VALUES 仍产生普通参数。
  test::clp.sql_mode = DEFAULT_MYSQL_MODE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);

  ObParser parser(allocator_, test::clp.sql_mode);
  ParseResult parse_result;
  ObString query = ObString::make_string(
      "insert into direct_load_stream 10086 (c1, c2) values (1, 2)");
  ASSERT_EQ(OB_SUCCESS, parser.parse(query, parse_result));
  ASSERT_NE(nullptr, parse_result.result_tree_);

  ParseNode *target_node = find_parse_node(parse_result.result_tree_, T_DIRECT_LOAD_STREAM_TARGET);
  ASSERT_NE(nullptr, target_node);
  ASSERT_EQ(1, target_node->num_child_);
  ASSERT_NE(nullptr, target_node->children_[0]);
  EXPECT_EQ(T_INT, target_node->children_[0]->type_);
  EXPECT_EQ(10086, target_node->children_[0]->value_);

  ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
  ASSERT_NE(nullptr, pctx);
  ParamStore &param_store = pctx->get_param_store_for_update();
  ObMaxConcurrentParam::FixParamStore fixed_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                        ObWrapperAllocator(&allocator_));
  SqlInfo not_param_info;
  bool is_transform_outline = false;
  param_store.reset();
  ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::transform_syntax_tree(allocator_,
                                                                     session_info_,
                                                                     NULL,
                                                                     parse_result.result_tree_,
                                                                     not_param_info,
                                                                     param_store,
                                                                     NULL,
                                                                     fixed_param_store,
                                                                     is_transform_outline));
  EXPECT_EQ(2, param_store.count());
  EXPECT_EQ(3, not_param_info.total_);
  EXPECT_TRUE(not_param_info.not_param_index_.has_member(0));
  target_node = find_parse_node(parse_result.result_tree_, T_DIRECT_LOAD_STREAM_TARGET);
  ASSERT_NE(nullptr, target_node);
  ASSERT_EQ(1, target_node->num_child_);
  ASSERT_NE(nullptr, target_node->children_[0]);
  EXPECT_EQ(T_INT, target_node->children_[0]->type_);
  EXPECT_EQ(10086, target_node->children_[0]->value_);
  parser.free_result(parse_result);

  query = ObString::make_string(
      "insert into direct_load_stream 10086 (c1, c2) values (?, ?)");
  ASSERT_EQ(OB_SUCCESS, parser.parse(query, parse_result));
  target_node = find_parse_node(parse_result.result_tree_, T_DIRECT_LOAD_STREAM_TARGET);
  ASSERT_NE(nullptr, target_node);
  ASSERT_NE(nullptr, target_node->children_[0]);
  EXPECT_EQ(10086, target_node->children_[0]->value_);
  parser.free_result(parse_result);

  query = ObString::make_string("insert into direct_load_stream 0 values (1)");
  ASSERT_NE(OB_SUCCESS, parser.parse(query, parse_result));
  parser.free_result(parse_result);

  query = ObString::make_string("insert into direct_load_stream -1 values (1)");
  ASSERT_NE(OB_SUCCESS, parser.parse(query, parse_result));
  parser.free_result(parse_result);

  query = ObString::make_string("insert into direct_load_stream '10086' values (1)");
  ASSERT_NE(OB_SUCCESS, parser.parse(query, parse_result));
  parser.free_result(parse_result);

  query = ObString::make_string("insert into direct_load_stream 1.2 values (1)");
  ASSERT_NE(OB_SUCCESS, parser.parse(query, parse_result));
  parser.free_result(parse_result);

  query = ObString::make_string("insert into direct_load_stream 1 + 1 values (1)");
  ASSERT_NE(OB_SUCCESS, parser.parse(query, parse_result));
  parser.free_result(parse_result);

  query = ObString::make_string("insert into direct_load_stream ? values (1)");
  ASSERT_NE(OB_SUCCESS, parser.parse(query, parse_result));
  parser.free_result(parse_result);
}

TEST_F(TestParser, direct_load_stream_text_parameterization)
{
  // 使用真实 fast parser 结果检查两种 Stream 入口的常量计数、顺序和 SQL 重建。
  test::clp.sql_mode = DEFAULT_MYSQL_MODE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  struct ParameterizationCase
  {
    const char *sql_;
    const char *expected_sql_;
    ObItemType node_type_;
    int64_t stream_id_;
    int64_t raw_count_;
    int64_t id_index_;
  };
  const ParameterizationCase cases[] = {
    // 覆盖原始 EXPLAIN 复现和普通文本 APPEND。
    {"EXPLAIN INSERT INTO DIRECT_LOAD_STREAM 999999 (id, payload) VALUES (1, 1)",
     "EXPLAIN INSERT INTO DIRECT_LOAD_STREAM 999999 (id, payload) VALUES (?, ?)",
     T_DIRECT_LOAD_STREAM_TARGET, 999999, 3, 0},
    {"insert into direct_load_stream 10086 (c1, c2) values (1, 2)",
     "insert into direct_load_stream 10086 (c1, c2) values (?, ?)",
     T_DIRECT_LOAD_STREAM_TARGET, 10086, 3, 0},
    // ID 与 VALUES 数值相同、多行数据时，只保留目标 ID。
    {"insert into direct_load_stream 1 values (1, 1), (1, 1)",
     "insert into direct_load_stream 1 values (?, ?), (?, ?)",
     T_DIRECT_LOAD_STREAM_TARGET, 1, 5, 0},
    // INT64 上界仍是有效的 Stream ID 字面量。
    {"insert into direct_load_stream 9223372036854775807 values (1)",
     "insert into direct_load_stream 9223372036854775807 values (?)",
     T_DIRECT_LOAD_STREAM_TARGET, INT64_MAX, 2, 0},
    // SOURCE 属性交换顺序后，NOT_PARAM 下标跟随 ID 的实际文本位置。
    {"insert into t select * from source(type='DIRECT_LOAD_STREAM', stream_id=999999)",
     "insert into t select * from source(type=?, stream_id=999999)",
     T_DIRECT_LOAD_STREAM_ID, 999999, 2, 1},
    {"insert into t select * from source(stream_id=10086, type='DIRECT_LOAD_STREAM')",
     "insert into t select * from source(stream_id=10086, type=?)",
     T_DIRECT_LOAD_STREAM_ID, 10086, 2, 0},
    // EXPLAIN 包装不改变 SOURCE ID 的计数和保留规则。
    {"explain insert into t select * from source(type='DIRECT_LOAD_STREAM', stream_id=1)",
     "explain insert into t select * from source(type=?, stream_id=1)",
     T_DIRECT_LOAD_STREAM_ID, 1, 2, 1}
  };
  ObPhysicalPlanCtx *pctx = exec_ctx_.get_physical_plan_ctx();
  ASSERT_NE(nullptr, pctx);
  ParamStore &params = pctx->get_param_store_for_update();
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    SCOPED_TRACE(cases[i].sql_);
    ObString query = ObString::make_string(cases[i].sql_);
    ObString no_param_sql;
    ObSEArray<ObPCParam *, 8> raw_params;
    ObSEArray<ObPCParam *, 8> special_params;
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::raw_fast_parameterize_sql(
        allocator_, session_info_, query, no_param_sql, raw_params));
    ASSERT_EQ(cases[i].raw_count_, raw_params.count());

    ObParser parser(allocator_, test::clp.sql_mode);
    ParseResult parse_result;
    ASSERT_EQ(OB_SUCCESS, parser.parse(query, parse_result));
    SqlInfo sql_info;
    sql_info.need_check_fp_ = true;
    SelectItemParamInfoArray select_item_infos;
    ObMaxConcurrentParam::FixParamStore fixed_params(
        OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(&allocator_));
    params.reset();
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::transform_syntax_tree(
        allocator_, session_info_, &raw_params, parse_result.result_tree_, sql_info,
        params, &select_item_infos, fixed_params, false, TEXT_MODE));
    EXPECT_EQ(raw_params.count(), sql_info.total_);
    EXPECT_EQ(cases[i].raw_count_ - 1, params.count());
    EXPECT_TRUE(sql_info.not_param_index_.has_member(cases[i].id_index_));

    ParseNode *id_parent = find_parse_node(parse_result.result_tree_, cases[i].node_type_);
    ASSERT_NE(nullptr, id_parent);
    ASSERT_EQ(1, id_parent->num_child_);
    ASSERT_NE(nullptr, id_parent->children_[0]);
    EXPECT_EQ(T_INT, id_parent->children_[0]->type_);
    EXPECT_EQ(cases[i].stream_id_, id_parent->children_[0]->value_);
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::check_and_generate_param_info(
        raw_params, sql_info, special_params));
    ASSERT_EQ(1, special_params.count());
    EXPECT_EQ(raw_params.at(cases[i].id_index_), special_params.at(0));
    EXPECT_EQ(NOT_PARAM, special_params.at(0)->flag_);
    ASSERT_EQ(raw_params.count(), sql_info.parse_infos_.count());
    for (int64_t j = 0; j < raw_params.count(); ++j) {
      EXPECT_EQ(j, sql_info.parse_infos_.at(j).param_idx_);
      EXPECT_EQ(j == cases[i].id_index_ ? NOT_PARAM : NORMAL_PARAM,
                sql_info.parse_infos_.at(j).flag_);
    }
    EXPECT_EQ(raw_params.at(cases[i].id_index_)->node_->raw_sql_offset_,
              sql_info.parse_infos_.at(cases[i].id_index_).raw_text_pos_);

    char sql_buf[1024];
    int32_t sql_len = 0;
    ASSERT_EQ(OB_SUCCESS, ObSqlParameterization::construct_sql(
        no_param_sql, special_params, sql_buf, sizeof(sql_buf), sql_len));
    EXPECT_EQ(ObString::make_string(cases[i].expected_sql_), ObString(sql_len, sql_buf));
    parser.free_result(parse_result);
  }
}

TEST_F(TestParser, direct_load_stream_non_reserved_keyword)
{
  test::clp.sql_mode = DEFAULT_MYSQL_MODE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);

  ObParser parser(allocator_, test::clp.sql_mode);
  ParseResult parse_result;
  const char *queries[] = {
      "create table direct_load_stream (c1 int)",
      "select direct_load_stream from t",
      "select * from direct_load_stream",
      "select * from t as direct_load_stream",
      "insert into direct_load_stream values (1)"};
  for (int64_t i = 0; i < ARRAYSIZEOF(queries); ++i) {
    ObString query = ObString::make_string(queries[i]);
    ASSERT_EQ(OB_SUCCESS, parser.parse(query, parse_result)) << queries[i];
    ASSERT_NE(nullptr, parse_result.result_tree_);
    EXPECT_EQ(nullptr, find_parse_node(parse_result.result_tree_, T_DIRECT_LOAD_STREAM_TARGET))
        << queries[i];
    parser.free_result(parse_result);
  }
}

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, basic_test)
{
  const char* test_file = "./test_parser.test";
  const char* result_file = "./test_parser.result";
  const char* tmp_file = "./test_parser.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  while (std::getline(if_tests, line)) {
    if (pretreat_cmd(line, expect_error)) {
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    ASSERT_NO_FATAL_FAILURE(do_parse(line.c_str(), of_result, expect_error));
    if (expect_error != 0) {
      expect_error = 0;
    }
  }
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, filter_hint)
{
  const char* test_file = "./test_parser_outline.test";
  const char* result_file = "./test_filter_hint.result";
  const char* tmp_file = "./test_filter_hint.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  while (std::getline(if_tests, line)) {
    if (pretreat_cmd(line, expect_error)) {
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    do_filter_hint(line.c_str(), of_result, expect_error);
    if (expect_error != 0) {
      expect_error = 0;
    }
  }
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, print_parser_tree)
{
  const char* test_file = "./test_parser.test";
  const char* result_file = "./print_parser_tree.result";
  const char* tmp_file = "./print_parser_tree.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  int64_t line_number = 0;
  while (std::getline(if_tests, line)) {
    ++line_number;
    if (pretreat_cmd(line, expect_error)) {
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    _OB_LOG(INFO, "line content:%ld, %s", line_number, line.c_str());
    ASSERT_NO_FATAL_FAILURE(print_parse_tree(line.c_str(), of_result, expect_error));
    if (expect_error != 0) {
      expect_error = 0;
    }
  }
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, pre_parse)
{
  constexpr int STR_NUM = 4;
  std::string str[] = {
    "\t/*\ttrace_id=ABC\t*/rpc_id=xxx",
    "\n/*\ntrace_id=ABC\nrpc_id=xxx*/",
    "\f/*\ftrace_id=ABC\frpc_id=xxx*/",
    "\r/*\rtrace_id=ABC\rrpc_id=xxx*/"
  };
  const char* test_file = "./test_pre_parse.test";
  const char* result_file = "./test_pre_parse.result";
  const char* tmp_file = "./test_pre_parse.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  char *w;
  char *p;
  for (int i = 0; i < STR_NUM; i++) {
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << str[i] << std::endl;
    PreParseResult res;
    ASSERT_EQ(OB_SUCCESS, ObParser::pre_parse(ObString(str[i].length(), str[i].c_str()), res));
    of_result << "trace_id: " << std::string(res.trace_id_.ptr(), res.trace_id_.length()) << std::endl;
  }
  while (std::getline(if_tests, line)) {
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;
    if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
      p = const_cast<char*>(line.c_str());
      w = strsep(&p, " ");
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    PreParseResult res;
    ASSERT_EQ(OB_SUCCESS, ObParser::pre_parse(ObString(line.length(), line.c_str()), res));
    of_result << "trace_id: " << std::string(res.trace_id_.ptr(), res.trace_id_.length()) << std::endl;
  }
  UNUSED(w);
  of_result.close();
   //verify results
  is_equal_content(tmp_file,result_file);
}
#endif

void TestParser::print_parse_tree(const char *query_str, std::ofstream &of_result, int64_t expect_error) {
  ObSQLMode mode = test::clp.sql_mode;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  ret = parser.parse(query, parse_result);
  if(expect_error != -ret) {
    fprintf(stderr, "QUERY:%s failed\n", query_str);
  }
  ASSERT_EQ(expect_error, -ret);
  if (NULL != parse_result.result_tree_) {
    _OB_LOG(INFO, "%s", CSJ(ObParserResultTreePrintWrapper(*parse_result.result_tree_)));
    of_result << "question_mask_size: " << parse_result.result_tree_->children_[0]->value_ << std::endl;
    of_result << CSJ(ObParserResultTreePrintWrapper(*parse_result.result_tree_)) << std::endl;
  }
  parser.free_result(parse_result);
}

void TestParser::print_parse_outline(const char *query_str, std::ofstream &of_result, int64_t expect_error) {
  ObSQLMode mode = test::clp.sql_mode;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  ret = parser.parse(query, parse_result, FP_PARAMERIZE_AND_FILTER_HINT_MODE);
  if(expect_error != -ret) {
    fprintf(stderr, "QUERY:%s failed\n", query_str);
  }
  ASSERT_EQ(expect_error, -ret);
  of_result << parse_result.no_param_sql_<<std::endl<<std::endl;
  parser.free_result(parse_result);
}

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_parser_outline)
{
  const char* test_file = "./test_parser_outline.test";
  const char* result_file = "./test_parser_outline.result";
  const char* tmp_file = "./test_parser_outline.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  while (std::getline(if_tests, line)) {
    if (pretreat_cmd(line, expect_error)) {
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    ASSERT_NO_FATAL_FAILURE(print_parse_outline(line.c_str(), of_result, expect_error));
    if (expect_error != 0) {
      expect_error = 0;
    }
  }
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_mysql_sql_keyword)
{
  const char* test_file = "../../../../src/sql/parser/sql_parser_mysql_mode.y";
  const char* result_file = "./test_mysql_sql_keyword.result";
  const char* tmp_file = "./test_mysql_sql_keyword.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  test::clp.sql_mode = DEFAULT_MYSQL_MODE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  ASSERT_NO_FATAL_FAILURE(parse_keyword(if_tests, of_result, true));
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_mysql_pl_keyword)
{
  const char* test_file = "../../../../src/pl/parser/pl_parser_mysql_mode.y";
  const char* result_file = "./test_mysql_pl_keyword.result";
  const char* tmp_file = "./test_mysql_pl_keyword.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  test::clp.sql_mode = DEFAULT_MYSQL_MODE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  ASSERT_NO_FATAL_FAILURE(parse_keyword(if_tests, of_result, false));
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_oracle_sql_keyword)
{
  const char* test_file = "../../../../close_modules/oracle_parser/sql/parser/sql_parser_oracle_mode.y";
  const char* result_file = "./test_oracle_sql_keyword.result";
  const char* tmp_file = "./test_oracle_sql_keyword.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  test::clp.sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
  ASSERT_NO_FATAL_FAILURE(parse_keyword(if_tests, of_result, true));
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_oracle_pl_keyword)
{
  const char* test_file = "../../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.y";
  const char* result_file = "./test_oracle_pl_keyword.result";
  const char* tmp_file = "./test_oracle_pl_keyword.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  test::clp.sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
  ASSERT_NO_FATAL_FAILURE(parse_keyword(if_tests, of_result, false));
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

void TestParser::parse_keyword(std::ifstream &if_tests, std::ofstream &of_result, bool is_sql_keyword)
{
  std::string line;
  const char* reserved_keyword_begin = "//-----------------------------reserved keyword begin-----------------------------------------------";
  const char* reserved_keyword_end = "//-----------------------------reserved keyword end-------------------------------------------------";
  const char* non_reserved_keyword_begin = "//-----------------------------non_reserved keyword begin-------------------------------------------";
  const char* non_keyword_end = "//-----------------------------non_reserved keyword end---------------------------------------------";
  bool test_reserved_keyword = false;
  bool test_non_reserved_keyword = false;
  int64_t num_reserved_keyword = 0;
  while (std::getline(if_tests, line)) {
    if (strncmp(line.c_str(), reserved_keyword_begin, strlen(reserved_keyword_begin)) == 0) {
      of_result << "**************   Begin Test Reserved Keyword   ***************" << std::endl;
      test_reserved_keyword = true;
      continue;
    } else if (strncmp(line.c_str(), reserved_keyword_end, strlen(reserved_keyword_end)) == 0) {
      of_result << "************** Total Count of Reserved Keyword:" << num_reserved_keyword <<"   ***************" << std::endl;
      of_result << "**************   End Test Reserved Keyword   ***************" << std::endl;
      test_reserved_keyword = false;
      continue;
    } else if (strncmp(line.c_str(), non_reserved_keyword_begin, strlen(non_reserved_keyword_begin)) == 0) {
      of_result << "**************   Begin Test Non-Reserved Keyword  ***************" << std::endl;
      test_non_reserved_keyword = true;
      continue;
    } else if (strncmp(line.c_str(), non_keyword_end, strlen(non_keyword_end)) == 0) {
      of_result << "**************   End Test Non-Reserved Keyword  ***************" << std::endl;
      break;
    } else if (!test_reserved_keyword && !test_non_reserved_keyword) {
      continue;
    }
    int64_t line_size = strlen(line.c_str());
    char *tmp_buf = NULL;
    if (NULL == (tmp_buf = static_cast<char*>(allocator_.alloc(line_size + 1)))) {
      fprintf(stderr, "failed to alloc memory for: %s\n", line.c_str());
      break;
    } else {
      MEMSET(tmp_buf, '\0', line_size + 1);
      int64_t valid_buf_len = 0;
      for (int64_t i = 0; i < line_size; ++i) {
        if (isspace(line.c_str()[i])) {
          if (valid_buf_len > 0 && valid_buf_len <= line_size) {
            tmp_buf[valid_buf_len] = '\0';
            if ((is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "FILE_KEY") == 0) ||
                (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "BEGIN_KEY") == 0) ||
                (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "END_KEY") == 0)) {
              valid_buf_len -= 4;//FILE_KEY ==> FILE、BEGIN_KEY==> DEGIN、END_KEY==> END
              tmp_buf[valid_buf_len] = '\0';
            } else if ((is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "INITIAL_") == 0) ||
                       (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "NULLX") == 0)) {
              valid_buf_len -= 1;//INITIAL_ ==> INITIAL、NULLX==>NULL
              tmp_buf[valid_buf_len] = '\0';
            }
            ASSERT_NO_FATAL_FAILURE(do_parse_keyword(tmp_buf, of_result, test_reserved_keyword, is_sql_keyword));
            valid_buf_len = 0;
            if (test_reserved_keyword) {
              ++ num_reserved_keyword;
            }
          }
        } else if (line.c_str()[i] == '/' && i < line_size - 1 && line.c_str()[i + 1] == '*') {
          i = i + 2;
          while (i < line_size) {
            if (line.c_str()[i] == '*' && i < line_size - 1 && line.c_str()[i + 1] == '/') {
              i = i + 2;
              break;
            }
            ++ i;
          }
        } else if (valid_buf_len < line_size) {
          tmp_buf[valid_buf_len++] = line.c_str()[i];
        }
      }
      if (valid_buf_len > 0 && valid_buf_len <= line_size) {
        tmp_buf[valid_buf_len] = '\0';
        if ((is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "FILE_KEY") == 0) ||
            (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "BEGIN_KEY") == 0) ||
            (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "END_KEY") == 0)) {
          valid_buf_len -= 4;//FILE_KEY ==> FILE、BEGIN_KEY==> DEGIN、END_KEY==> END
          tmp_buf[valid_buf_len] = '\0';
        } else if ((is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "INITIAL_") == 0) ||
                    (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "NULLX") == 0)) {
          valid_buf_len -= 1;//INITIAL_ ==> INITIAL、NULLX==>NULL
          tmp_buf[valid_buf_len] = '\0';
        }
        ASSERT_NO_FATAL_FAILURE(do_parse_keyword(tmp_buf, of_result, test_reserved_keyword, is_sql_keyword));
        valid_buf_len = 0;
        if (test_reserved_keyword) {
          ++ num_reserved_keyword;
        }
      }
    }
  }
}
//reserved keyword test: create table table_name(column_name int);
//non_reserved keyword test: create table table_name(column_name int)
//                           select 1 alias_name from t1 alias_name;
void TestParser::do_parse_keyword(const char *keyword, std::ofstream &of_result, bool is_reserved, bool is_sql_keyword)
{
  char *buf = NULL;
  int64_t buf_len = strlen(keyword) * 2 + 1024;
  if (NULL == (buf = static_cast<char*>(allocator_.alloc(buf_len)))) {
    fprintf(stderr, "failed to alloc memory for: %s\n", keyword);
  } else if (is_sql_keyword) {
    sprintf(buf, "create table %s(%s int);", keyword, keyword);
    if (is_reserved) {
      of_result << "Reserved Keyword: "<< keyword << std::endl;
      of_result << "Test Table Name and Column Name Sql:" << buf << std::endl;
    }
  } else if (test::clp.sql_mode & SMO_ORACLE) {
    sprintf(buf, "create type %s as table of varchar2(1);", keyword);
    if (is_reserved) {
      of_result << "Reserved Keyword: "<< keyword << std::endl;
      of_result << "Test PL type name Sql:" << buf << std::endl;
    }
  } else {
    sprintf(buf, "create procedure %s () select 1;", keyword);
    if (is_reserved) {
      of_result << "Reserved Keyword: "<< keyword << std::endl;
      of_result << "Test PL procedure name Sql:" << buf << std::endl;
    }
  }
  ObString query = ObString::make_string(buf);
  int ret = OB_SUCCESS;
  _OB_LOG(INFO, "test keyword to be name: %s", buf);
  ObSQLMode mode = test::clp.sql_mode;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  ret = parser.parse(query, parse_result);
  if ((is_reserved && ret != 0) ||
      (!is_reserved && ret == 0)) {
    ret = OB_SUCCESS;
  } else if (!is_reserved && is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(keyword, "CONSTRAINT") == 0) {
    ret = OB_SUCCESS;
  } else if ((is_reserved && !is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(keyword, "EXISTS") == 0) ||
              (is_reserved && !is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(keyword, "SET") == 0)) {
    ret = OB_SUCCESS;
  } else if (!is_reserved && !is_sql_keyword && non_reserved_keyword_can_not_be_name(keyword, is_sql_keyword)) {
    ret = OB_SUCCESS;
  } else {
    fprintf(stderr, "test keyword to be name failed:%s\n", buf);
  }
  ASSERT_EQ(OB_SUCCESS, -ret);
  if (!is_reserved && is_sql_keyword) {
    sprintf(buf, "select 1 %s from t1 %s;", keyword, keyword);
    query = ObString::make_string(buf);
    _OB_LOG(INFO, "test keyword to alias name: %s", buf);
    ret = parser.parse(query, parse_result);
    if (ret != 0) {
      if (non_reserved_keyword_can_not_be_name(keyword, is_sql_keyword)) {
        ret = 0;
      } else {
        fprintf(stderr, "test keyword to alias name failed:%s\n", buf);
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, -ret);
  parser.free_result(parse_result);
}

bool TestParser::non_reserved_keyword_can_not_be_name(const char *keyword, bool is_sql_keyword)
{
  bool ret = false;
  if (is_sql_keyword) {
    if (test::clp.sql_mode & SMO_ORACLE) {
      if (strcmp(keyword, "USING") == 0 ||
          strcmp(keyword, "BULK") == 0 ||
          strcmp(keyword, "COLLATE") == 0 ||
          strcmp(keyword, "CROSS") == 0 ||
          strcmp(keyword, "FULL") == 0 ||
          strcmp(keyword, "INNER") == 0 ||
          strcmp(keyword, "JOIN") == 0 ||
          strcmp(keyword, "LOG") == 0 ||
          strcmp(keyword, "LEFT") == 0 ||
          strcmp(keyword, "NATURAL") == 0 ||
          strcmp(keyword, "RETURN") == 0 ||
          strcmp(keyword, "RIGHT") == 0 ||
          strcmp(keyword, "RETURNING") == 0) {
        ret = true;
      }
    } else if (strcmp(keyword, "EXCEPT") == 0 ||
               strcmp(keyword, "INTERSECT") == 0 ||
               strcmp(keyword, "MINUS") == 0 ||
               strcmp(keyword, "MEMBER") == 0 ||
               strcmp(keyword, "SOUNDS") == 0 ||
               strcmp(keyword, "WINDOW") == 0) {
      ret = true;
    }
  } else {
    if (test::clp.sql_mode & SMO_ORACLE) {
      if (strcmp(keyword, "CHARACTER") == 0 ||
          strcmp(keyword, "CONSTANT") == 0 ||
          strcmp(keyword, "DATE") == 0 ||
          strcmp(keyword, "FLOAT") == 0 ||
          strcmp(keyword, "LOOP") == 0 ||
          strcmp(keyword, "NUMBER") == 0 ||
          strcmp(keyword, "RAW") == 0 ||
          strcmp(keyword, "REAL") == 0 ||
          strcmp(keyword, "VARCHAR") == 0 ||
          strcmp(keyword, "VARCHAR2") == 0) {
        ret = true;
      }
    }
  }
  return ret;
}

void TestParser::do_parse(const char *query_str, std::ofstream &of_result, int64_t expect_error) {
  ObSQLMode mode = test::clp.sql_mode;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  _OB_LOG(INFO, "QUERY: %s", query_str);
  ret = parser.parse(query, parse_result);
  if(expect_error != -ret) {
    fprintf(stderr, "QUERY:%s failed\n", query_str);
  }
  ASSERT_EQ(expect_error, -ret);
  if (NULL != parse_result.result_tree_){
    _OB_LOG(INFO, "%s", CSJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    of_result << "question_mask_size: " << parse_result.result_tree_->children_[0]->value_ << std::endl;
    of_result << CSJ(ObParserResultPrintWrapper(*parse_result.result_tree_)) << std::endl;
  }
  parser.free_result(parse_result);
}

void TestParser::do_filter_hint(const char *query_str, std::ofstream &of_result, int64_t expect_error)
{
  ObString real_query = ObString::make_string(query_str).trim();
  if (real_query.length() > 0 && *real_query.ptr() != '#') {
    //ignore empty query and comment
    _OB_LOG(INFO, "query_str: %s", query_str);
    int ret = OB_SUCCESS;
    ObString result_query;
    if (OB_FAIL(ObSQLUtils::filter_hint_in_query_sql(allocator_,
                                                     session_info_,
                                                     real_query,
                                                     result_query))) {
      _OB_LOG(INFO, "query_str: %s", query_str);
      if(expect_error != -ret) {
        fprintf(stderr, "QUERY:%s failed\n", query_str);
      }
      ASSERT_EQ(expect_error, -ret);
    }
    if (expect_error == 0) {
      char buffer[OB_MAX_SQL_LENGTH];
      memset(buffer, '\0', OB_MAX_SQL_LENGTH);
      memcpy(buffer, result_query.ptr(), result_query.length());
      of_result << buffer << std::endl;
    }
  }
}

bool TestParser::pretreat_cmd(std::string line, int64_t &expect_error)
{
  bool skip_cmd = false;
  char *w = NULL;
  char *p = NULL;
  if (line.size() <= 0) skip_cmd = true;
  else if (line.at(0) == '#') skip_cmd = true;
  else if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
    p = const_cast<char*>(line.c_str());
    w = strsep(&p, " ");
    expect_error = atol(p);
    skip_cmd = true;
  } else if (strncmp(line.c_str(), "--sql_mode", strlen("--sql_mode")) == 0) {
    p = const_cast<char*>(line.c_str());
    w = strsep(&p, " ");
    if (strncmp(p, "oracle", strlen("oracle")) == 0) {
      OB_LOG(INFO, "switch parser sql_mode to oracle");
      test::clp.sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
      set_compat_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
    } else if (strncmp(p, "mysql", strlen("mysql")) == 0) {
      OB_LOG(INFO, "switch parser sql_mode to mysql");
      test::clp.sql_mode = DEFAULT_MYSQL_MODE;
      set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
    } else if (strncmp(p, "PIPES_AS_CONCAT", strlen("PIPES_AS_CONCAT")) == 0) {
      OB_LOG(INFO, "switch parser sql_mode to PIPES_AS_CONCAT");
      test::clp.sql_mode = DEFAULT_MYSQL_MODE | SMO_PIPES_AS_CONCAT;
      set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
    }
    skip_cmd = true;
    UNUSED(w);
  }
  return skip_cmd;
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_parser.log", true);
  test::parse_cmd_line_param(argc, argv, test::clp);
  return RUN_ALL_TESTS();
}
