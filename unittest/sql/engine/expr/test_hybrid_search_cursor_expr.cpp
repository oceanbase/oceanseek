/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Unit tests for __shard_doc and __sort cursor expressions.

#include <gtest/gtest.h>
#include <cstring>
#include <limits>
#include <string>
#include <vector>
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/expr/ob_expr_hybrid_search_sort.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/engine/expr/ob_expr_json_array.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "share/object/ob_obj_cast.h"
#include "share/system_variable/ob_system_variable.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

class RejectAllocation : public ObIAllocator
{
public:
  void *alloc(const int64_t) override { return nullptr; }
  void *alloc(const int64_t, const ObMemAttr &) override { return nullptr; }
  void free(void *) override {}
};

class TestHybridSearchCursorExpr : public ::testing::Test
{
protected:
  static const int64_t INPUT_COUNT = 8;
  static const int64_t SLOT_SIZE = 256;
  static const int64_t FRAME_SIZE = 256 * 1024;

  TestHybridSearchCursorExpr()
      : allocator_("TestSortExpr"), exec_ctx_(allocator_), eval_ctx_(exec_ctx_), frames_{nullptr}, args_{}
  {}

  void SetUp() override
  {
    frames_[0] = static_cast<char *>(allocator_.alloc(FRAME_SIZE));
    ASSERT_NE(nullptr, frames_[0]);
    MEMSET(frames_[0], 0, FRAME_SIZE);
    exec_ctx_.set_frames(frames_);
    exec_ctx_.set_frame_cnt(1);
    // set_my_session() binds expression allocators to the session's effective tenant.
    // A default session still has OB_INVALID_ID and cannot allocate JSON temporaries.
    ASSERT_EQ(OB_SUCCESS, session_.init_tenant(
        ObString::make_string("server"), OB_SERVER_TENANT_ID));
    exec_ctx_.set_my_session(&session_);
    eval_ctx_.frames_ = frames_;
    session_.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
    ASSERT_EQ(OB_SUCCESS, session_.set_time_zone(ObString::make_string("+00:00"), false, true));
    for (int64_t i = 0; i < INPUT_COUNT; ++i) {
      configure_expr(inputs_[i], i, ObNullType);
      args_[i] = &inputs_[i];
      inputs_[i].locate_datum_for_write(eval_ctx_).set_null();
    }
    configure_expr(output_, INPUT_COUNT, ObJsonType);
    output_.type_ = T_FUN_SYS_HYBRID_SEARCH_SORT_VALUES;
    output_.args_ = args_;
    output_.res_buf_len_ = 128 * 1024;
  }

  void configure_expr(ObExpr &expr, const int64_t slot, ObObjType type,
                      ObCollationType collation = CS_TYPE_UTF8MB4_BIN, ObScale scale = 0)
  {
    expr.obj_meta_.set_type(type);
    expr.obj_meta_.set_collation_type(collation);
    expr.obj_meta_.set_scale(scale);
    expr.datum_meta_ = ObDatumMeta(type, collation, scale);
    expr.obj_datum_map_ = ObDatum::get_obj_datum_map_type(type);
    expr.frame_idx_ = 0;
    expr.datum_off_ = slot * SLOT_SIZE;
    expr.eval_info_off_ = expr.datum_off_ + sizeof(ObDatum);
    expr.res_buf_off_ = expr.eval_info_off_ + sizeof(ObEvalInfo);
    expr.res_buf_len_ = SLOT_SIZE - sizeof(ObDatum) - sizeof(ObEvalInfo);
  }

  ObDatum &input(const int64_t idx, ObObjType type,
                 ObCollationType collation = CS_TYPE_UTF8MB4_BIN, ObScale scale = 0)
  {
    configure_expr(inputs_[idx], idx, type, collation, scale);
    return inputs_[idx].locate_datum_for_write(eval_ctx_);
  }

  void set_json_input(const int64_t idx, const ObString &text)
  {
    ObJsonNode *node = nullptr;
    ObString binary;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&allocator_, text, node));
    ASSERT_EQ(OB_SUCCESS, node->get_raw_binary(binary, &allocator_));
    ObDatum &datum = input(idx, ObJsonType);
    ObTextStringDatumResult json_result(ObJsonType, &datum);
    ASSERT_EQ(OB_SUCCESS, json_result.init(binary.length(), &allocator_));
    ASSERT_EQ(OB_SUCCESS, json_result.append(binary.ptr(), binary.length()));
    json_result.set_result();
  }

  void read_client_cursor(const ObDatum &result, ObJsonNode *&client_value)
  {
    ASSERT_FALSE(result.is_null());
    ObString binary = result.get_string();
    ASSERT_EQ(OB_SUCCESS, ObTextStringHelper::read_real_string_data(
        &allocator_, ObJsonType, CS_TYPE_UTF8MB4_BIN, binary));
    ObJsonBin actual(binary.ptr(), binary.length(), &allocator_);
    ASSERT_EQ(OB_SUCCESS, actual.reset_iter());
    EXPECT_EQ(ObJsonNodeType::J_ARRAY, actual.json_type());
    // Compare the JSON text clients receive and return as search_after.
    ObJsonBuffer printed(&allocator_);
    ASSERT_EQ(OB_SUCCESS, actual.print(printed, true));
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&allocator_, ObString(printed.length(), printed.ptr()), client_value));
    ASSERT_NE(nullptr, client_value);
  }

  void expect_json(const ObDatum &result, const ObString &expected)
  {
    ObJsonNode *client_value = nullptr;
    ASSERT_NO_FATAL_FAILURE(read_client_cursor(result, client_value));
    ObJsonNode *expected_tree = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&allocator_, expected, expected_tree));
    int cmp = 0;
    ASSERT_EQ(OB_SUCCESS, client_value->compare(*expected_tree, cmp));
    ObJsonBuffer printed(&allocator_);
    ASSERT_EQ(OB_SUCCESS, client_value->print(printed, true));
    EXPECT_EQ(0, cmp) << "actual JSON: " << std::string(printed.ptr(), printed.length())
                      << "; expected JSON: " << std::string(expected.ptr(), expected.length());
  }

  void expect_json_argument(ObIJsonBase &cursor, const int64_t index, const ObString &expected)
  {
    ObIJsonBase *actual = nullptr;
    ASSERT_EQ(OB_SUCCESS, cursor.get_array_element(index, actual));
    ASSERT_NE(nullptr, actual);
    // JSON sort keys retain their native types in the client cursor.
    ObJsonNode *expected_tree = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&allocator_, expected, expected_tree));
    ASSERT_NE(nullptr, expected_tree);
    ASSERT_EQ(expected_tree->json_type(), actual->json_type());
    int cmp = 0;
    ASSERT_EQ(OB_SUCCESS, actual->compare(*expected_tree, cmp));
    EXPECT_EQ(0, cmp);
  }

  void set_cast_input(const int64_t idx, const ObObj &source, ObObjType target_type,
                      ObCollationType target_collation)
  {
    ObDataTypeCastParams params = ObBasicSessionInfo::create_dtc_params(&session_);
    ObCastCtx cast_ctx(&allocator_, &params, CM_NONE, target_collation);
    ObObj converted;
    ASSERT_EQ(OB_SUCCESS, ObObjCaster::to_type(target_type, target_collation, cast_ctx, source, converted));
    ASSERT_EQ(OB_SUCCESS, input(idx, converted.get_type(), converted.get_collation_type(),
                               converted.get_scale()).from_obj(converted));
  }

  void expect_native_json_array()
  {
    ObDatum actual;
    ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, actual));
    ObString actual_copy;
    ASSERT_EQ(OB_SUCCESS, ob_write_string(allocator_, actual.get_string(), actual_copy));
    ObDatum expected;
    ASSERT_EQ(OB_SUCCESS, ObExprJsonArray::eval_json_array(output_, eval_ctx_, expected));
    ObString expected_binary = expected.get_string();
    ASSERT_EQ(OB_SUCCESS, ObTextStringHelper::read_real_string_data(
        &allocator_, ObJsonType, CS_TYPE_UTF8MB4_BIN, actual_copy));
    ASSERT_EQ(OB_SUCCESS, ObTextStringHelper::read_real_string_data(
        &allocator_, ObJsonType, CS_TYPE_UTF8MB4_BIN, expected_binary));
    ObJsonBin actual_array(actual_copy.ptr(), actual_copy.length(), &allocator_);
    ObJsonBin expected_array(expected_binary.ptr(), expected_binary.length(), &allocator_);
    ASSERT_EQ(OB_SUCCESS, actual_array.reset_iter());
    ASSERT_EQ(OB_SUCCESS, expected_array.reset_iter());
    int cmp = 0;
    ASSERT_EQ(OB_SUCCESS, actual_array.compare(expected_array, cmp));
    EXPECT_EQ(0, cmp);
  }

  static int fail_argument(const ObExpr &, ObEvalCtx &, ObDatum &)
  {
    return OB_INVALID_ARGUMENT;
  }

  ObArenaAllocator allocator_;
  ObSQLSessionInfo session_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  char *frames_[1];
  ObExpr inputs_[INPUT_COUNT];
  ObExpr *args_[INPUT_COUNT];
  ObExpr output_;
};

TEST_F(TestHybridSearchCursorExpr, shard_doc_requires_unsigned_inputs_and_binary_result)
{
  ObExprHybridUniqueShardDoc op(allocator_);
  ObExprResType result;
  ObExprResType tablet;
  ObExprResType increment;
  ObExprTypeCtx ctx;
  tablet.set_uint64();
  increment.set_uint64();
  ASSERT_EQ(OB_SUCCESS, op.calc_result_type2(result, tablet, increment, ctx));
  EXPECT_EQ(ObVarcharType, result.get_type());
  EXPECT_EQ(32, result.get_length());
  EXPECT_EQ(CS_TYPE_BINARY, result.get_collation_type());
  tablet.set_int();
  EXPECT_EQ(OB_INVALID_ARGUMENT, op.calc_result_type2(result, tablet, increment, ctx));
  tablet.set_uint64();
  increment.set_varchar();
  EXPECT_EQ(OB_INVALID_ARGUMENT, op.calc_result_type2(result, tablet, increment, ctx));
}

TEST_F(TestHybridSearchCursorExpr, sort_values_use_public_json_argument_types)
{
  ObExprHybridSearchSortValues op(allocator_);
  ObExprJsonArray json_array(allocator_);
  ObExprTypeCtx ctx;
  ObExprResType result;
  ObExprResType arg;
  EXPECT_EQ(OB_INVALID_ARGUMENT, op.calc_result_typeN(result, nullptr, 0, ctx));
  const ObObjType types[] = {ObNullType, ObIntType, ObUInt64Type, ObFloatType,
      ObDoubleType, ObNumberType, ObDecimalIntType, ObVarcharType, ObCharType,
      ObDateType, ObDateTimeType, ObTimestampType, ObYearType,
      ObMySQLDateType, ObMySQLDateTimeType, ObJsonType, ObBitType, ObRawType,
      ObTextType, ObTimestampTZType, ObGeometryType};
  for (ObObjType type : types) {
    SCOPED_TRACE(static_cast<int>(type));
    arg.set_type(type);
    arg.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObExprResType expected_arg = arg;
    ObExprResType expected_result;
    ASSERT_EQ(OB_SUCCESS, json_array.calc_result_typeN(expected_result, &expected_arg, 1, ctx));
    ASSERT_EQ(OB_SUCCESS, op.calc_result_typeN(result, &arg, 1, ctx));
    EXPECT_EQ(ObJsonType, result.get_type());
    EXPECT_EQ(CS_TYPE_UTF8MB4_BIN, result.get_collation_type());
    EXPECT_EQ(expected_arg.get_calc_type(), arg.get_calc_type());
    EXPECT_EQ(expected_arg.get_calc_collation_type(), arg.get_calc_collation_type());
  }
  arg.set_type(ObTimeType);
  arg.set_scale(6);
  ASSERT_EQ(OB_SUCCESS, op.calc_result_typeN(result, &arg, 1, ctx));
  EXPECT_EQ(ObVarcharType, arg.get_calc_type());
  EXPECT_EQ(CS_TYPE_UTF8MB4_BIN, arg.get_calc_collation_type());
  EXPECT_GE(arg.get_calc_length(), 17);
}

TEST_F(TestHybridSearchCursorExpr, sort_values_apply_public_charset_rules_except_shard_doc)
{
  ObExprHybridSearchSortValues op(allocator_);
  ObExprTypeCtx ctx;
  ObExprResType result;
  ObExprResType arg;
  arg.set_varchar();
  arg.set_collation_type(CS_TYPE_LATIN1_BIN);
  ASSERT_EQ(OB_SUCCESS, op.calc_result_typeN(result, &arg, 1, ctx));
  EXPECT_EQ(CS_TYPE_UTF8MB4_BIN, arg.get_calc_collation_type());
  arg.set_collation_type(CS_TYPE_BINARY);
  ASSERT_EQ(OB_SUCCESS, op.calc_result_typeN(result, &arg, 1, ctx));
  EXPECT_EQ(ObHexStringType, arg.get_calc_type());
  // The native HEX_STRING result is binary, even though JSON_ARRAY initially requests UTF8MB4.
  EXPECT_EQ(CS_TYPE_BINARY, arg.get_calc_collation_type());

  ObSysFunRawExpr raw(allocator_);
  ObSysFunRawExpr shard_doc(allocator_);
  raw.set_expr_type(T_FUN_SYS_HYBRID_SEARCH_SORT_VALUES);
  shard_doc.set_expr_type(T_FUN_SYS_HYBRID_UNIQUE_SHARD_DOC);
  ASSERT_EQ(OB_SUCCESS, raw.init_param_exprs(1));
  ASSERT_EQ(OB_SUCCESS, raw.add_param_expr(&shard_doc));
  ctx.set_raw_expr(&raw);
  arg.set_calc_type(ObVarcharType);
  arg.set_calc_collation_type(CS_TYPE_BINARY);
  ASSERT_EQ(OB_SUCCESS, op.calc_result_typeN(result, &arg, 1, ctx));
  EXPECT_EQ(ObVarcharType, arg.get_calc_type());
  EXPECT_EQ(CS_TYPE_BINARY, arg.get_calc_collation_type());
}

TEST_F(TestHybridSearchCursorExpr, binary_sort_value_formalize_twice_keeps_one_cast)
{
  ObExprOperatorFactory::register_expr_operators();
  ASSERT_EQ(OB_SUCCESS, share::ObPreProcessSysVars::init_sys_var());
  ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, &allocator_));
  ASSERT_EQ(OB_SUCCESS, session_.load_default_sys_variable(false, true));
  session_.set_cur_exec_ctx(&exec_ctx_);

  ObRawExprFactory factory(allocator_);
  ObColumnRefRawExpr *binary_column = nullptr;
  ObSysFunRawExpr *sort_values = nullptr;
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_REF_COLUMN, binary_column));
  ObRawExprResType column_type;
  column_type.set_varchar();
  column_type.set_collation_type(CS_TYPE_BINARY);
  column_type.set_collation_level(CS_LEVEL_IMPLICIT);
  column_type.set_length(8);
  binary_column->set_result_type(column_type);
  binary_column->set_ref_id(1, 16);
  ASSERT_EQ(OB_SUCCESS, binary_column->add_relation_id(1));
  ASSERT_EQ(OB_SUCCESS, factory.create_raw_expr(T_FUN_SYS_HYBRID_SEARCH_SORT_VALUES, sort_values));
  sort_values->set_func_name(ObString::make_string("hybrid_search_sort_values"));
  ASSERT_EQ(OB_SUCCESS, sort_values->init_param_exprs(1));
  ASSERT_EQ(OB_SUCCESS, sort_values->add_param_expr(binary_column));

  ASSERT_EQ(OB_SUCCESS, sort_values->formalize(&session_));
  ObRawExpr *first_cast = sort_values->get_param_expr(0);
  ASSERT_NE(nullptr, first_cast);
  ASSERT_EQ(T_FUN_SYS_CAST, first_cast->get_expr_type());
  EXPECT_TRUE(first_cast->has_flag(IS_OP_OPERAND_IMPLICIT_CAST));
  EXPECT_EQ(ObHexStringType, first_cast->get_data_type());
  EXPECT_EQ(CS_TYPE_BINARY, first_cast->get_collation_type());
  ASSERT_EQ(2, first_cast->get_param_count());
  EXPECT_EQ(binary_column, first_cast->get_param_expr(0));

  ASSERT_EQ(OB_SUCCESS, sort_values->formalize(&session_));
  EXPECT_EQ(first_cast, sort_values->get_param_expr(0));
  EXPECT_EQ(binary_column, first_cast->get_param_expr(0));
  EXPECT_EQ(ObHexStringType, first_cast->get_data_type());
  EXPECT_EQ(CS_TYPE_BINARY, first_cast->get_collation_type());
  EXPECT_EQ(ObJsonType, sort_values->get_data_type());
}

TEST_F(TestHybridSearchCursorExpr, shard_doc_evaluator_uses_both_physical_identifiers)
{
  input(0, ObUInt64Type).set_uint(7001);
  input(1, ObUInt64Type).set_uint(103);
  output_.arg_cnt_ = 2;
  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::eval_unique_shard_doc(output_, eval_ctx_, result));
  EXPECT_EQ(ObString::make_string("0000000000001b590000000000000067"), result.get_string());
  input(0, ObUInt64Type).set_uint(UINT64_MAX);
  input(1, ObUInt64Type).set_uint(UINT64_MAX);
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::eval_unique_shard_doc(output_, eval_ctx_, result));
  EXPECT_EQ(ObString::make_string("ffffffffffffffffffffffffffffffff"), result.get_string());
  input(1, ObUInt64Type).set_uint(0);
  EXPECT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::eval_unique_shard_doc(output_, eval_ctx_, result));
}

TEST_F(TestHybridSearchCursorExpr, shard_doc_evaluator_rejects_null_and_zero_tablet)
{
  output_.arg_cnt_ = 2;
  input(0, ObUInt64Type).set_uint(0);
  input(1, ObUInt64Type).set_uint(1);
  ObDatum result;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObExprHybridUniqueShardDoc::eval_unique_shard_doc(output_, eval_ctx_, result));
  input(0, ObUInt64Type).set_null();
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObExprHybridUniqueShardDoc::eval_unique_shard_doc(output_, eval_ctx_, result));
  input(0, ObUInt64Type).set_uint(1);
  input(1, ObUInt64Type).set_null();
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObExprHybridUniqueShardDoc::eval_unique_shard_doc(output_, eval_ctx_, result));
  inputs_[1].eval_func_ = fail_argument;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObExprHybridUniqueShardDoc::eval_unique_shard_doc(output_, eval_ctx_, result));
}

TEST_F(TestHybridSearchCursorExpr, sort_values_preserve_argument_order_types_and_null)
{
  input(0, ObIntType).set_int(-42);
  input(1, ObUInt64Type).set_uint(UINT64_MAX);
  input(2, ObDoubleType).set_double(1.25);
  input(3, ObVarcharType).set_string(ObString::make_string("line\n\"中文"));
  input(4, ObIntType).set_null();
  input(5, ObVarcharType, CS_TYPE_BINARY).set_string(
      ObString::make_string("0000000000001B590000000000000067"));
  inputs_[5].type_ = T_FUN_SYS_HYBRID_UNIQUE_SHARD_DOC;
  output_.arg_cnt_ = 6;
  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
  const ObString expected = ObString::make_string(
      "[-42,18446744073709551615,1.25,\"line\\n\\\"中文\",null,\"0000000000001b590000000000000067\"]");
  ASSERT_NO_FATAL_FAILURE(expect_json(result, expected));
  // The returned cursor must survive destruction/reuse of temporary JSON nodes.
  eval_ctx_.tmp_alloc_.reuse();
  ASSERT_NO_FATAL_FAILURE(expect_json(result, expected));
}

TEST_F(TestHybridSearchCursorExpr, sort_values_preserve_large_json_value_and_exact_decimal)
{
  const std::string json = "{\"payload\":\"" + std::string(64 * 1024, 'p')
      + "\",\"values\":[null,true,9007199254740993,0.1000000000000000000000000001]}";
  ASSERT_NO_FATAL_FAILURE(set_json_input(0, ObString(json.size(), json.data())));
  number::ObNumber decimal;
  const char *text = "12345678901234567890.123456789";
  ASSERT_EQ(OB_SUCCESS, decimal.from(text, strlen(text), allocator_));
  input(1, ObNumberType, CS_TYPE_UTF8MB4_BIN, 9).set_number(decimal);
  output_.arg_cnt_ = 2;
  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
  ObJsonNode *cursor = nullptr;
  ASSERT_NO_FATAL_FAILURE(read_client_cursor(result, cursor));
  ASSERT_EQ(2, cursor->element_count());
  ASSERT_NO_FATAL_FAILURE(expect_json_argument(*cursor, 0, ObString(json.size(), json.data())));
  // Public text parsing uses ordinary JSON numbers; verify DECIMAL directly in the native binary result.
  ObString binary = result.get_string();
  ASSERT_EQ(OB_SUCCESS, ObTextStringHelper::read_real_string_data(
      &allocator_, ObJsonType, CS_TYPE_UTF8MB4_BIN, binary));
  ObJsonBin array(binary.ptr(), binary.length(), &allocator_);
  ASSERT_EQ(OB_SUCCESS, array.reset_iter());
  ObIJsonBase *decimal_value = nullptr;
  ASSERT_EQ(OB_SUCCESS, array.get_array_element(1, decimal_value));
  ASSERT_EQ(ObJsonNodeType::J_DECIMAL, decimal_value->json_type());
  number::ObNumber decoded_decimal;
  ASSERT_EQ(OB_SUCCESS, decimal_value->to_number(&allocator_, decoded_decimal));
  EXPECT_EQ(0, decimal.compare(decoded_decimal));
  ObJsonBuffer printed(&allocator_);
  ASSERT_EQ(OB_SUCCESS, array.print(printed, true));
  EXPECT_NE(std::string::npos, std::string(printed.ptr(), printed.length()).find(text));
}

TEST_F(TestHybridSearchCursorExpr, sort_values_unify_nulls_and_preserve_json_types)
{
  input(0, ObJsonType).set_null();
  ASSERT_NO_FATAL_FAILURE(set_json_input(1, ObString::make_string("null")));
  ASSERT_NO_FATAL_FAILURE(set_json_input(2, ObString::make_string("\"null\"")));
  ASSERT_NO_FATAL_FAILURE(set_json_input(3, ObString::make_string("1")));
  ASSERT_NO_FATAL_FAILURE(set_json_input(4, ObString::make_string("\"1\"")));
  input(5, ObIntType).set_int(1);
  input(6, ObVarcharType).set_string(ObString::make_string("1"));
  output_.arg_cnt_ = 7;
  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
  // SQL NULL and JSON null share the bare null output; strings and numbers keep their types.
  const ObString expected = ObString::make_string(
      R"json([null,null,"null",1,"1",1,"1"])json");
  ASSERT_NO_FATAL_FAILURE(expect_json(result, expected));
  eval_ctx_.tmp_alloc_.reuse();
  ASSERT_NO_FATAL_FAILURE(expect_json(result, expected));
}

TEST_F(TestHybridSearchCursorExpr, json_sort_values_preserve_public_json_types_and_escaped_content)
{
  const char *values[] = {
      "null", "\"null\"", "1", "\"1\"", "true", "false", "[]", "{}", "\"\"",
      "18446744073709551615", "0.1000000000000000000000000001",
      R"json("quote\"\n\u0000尾")json",
      R"json([null,"null",1,"1",true,0.1000000000000000000000000001])json",
      R"json({"n":null,"s":"null","v":9007199254740993,"nested":[{},[]]})json"};
  output_.arg_cnt_ = 1;
  for (const char *text : values) {
    SCOPED_TRACE(text);
    ASSERT_NO_FATAL_FAILURE(set_json_input(0, ObString::make_string(text)));
    ObDatum result;
    ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
    ObJsonNode *cursor = nullptr;
    ASSERT_NO_FATAL_FAILURE(read_client_cursor(result, cursor));
    ASSERT_EQ(1, cursor->element_count());
    ASSERT_NO_FATAL_FAILURE(expect_json_argument(*cursor, 0, ObString::make_string(text)));
  }
}

TEST_F(TestHybridSearchCursorExpr, sort_values_support_more_than_uint16_arguments)
{
  input(0, ObIntType).set_int(1);
  input(1, ObIntType).set_int(-17);
  // Large arrays can exceed the reserved result buffer. Keep its dynamic-buffer header
  // in unused frame space instead of allocating a frame slot for every repeated argument.
  output_.dyn_buf_header_offset_ = FRAME_SIZE - sizeof(ObDynReserveBuf);
  const uint32_t argument_counts[] = {65536, 65537};
  for (uint32_t argument_count : argument_counts) {
    SCOPED_TRACE(argument_count);
    std::vector<ObExpr *> arguments(argument_count, &inputs_[0]);
    arguments.back() = &inputs_[1];
    output_.args_ = arguments.data();
    output_.arg_cnt_ = argument_count;
    ObDatum result;
    const int ret = ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result);
    output_.args_ = args_;
    output_.arg_cnt_ = 0;
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_FALSE(result.is_null());

    ObString binary = result.get_string();
    ASSERT_EQ(OB_SUCCESS, ObTextStringHelper::read_real_string_data(
        &allocator_, ObJsonType, CS_TYPE_UTF8MB4_BIN, binary));
    ObJsonBin array(binary.ptr(), binary.length(), &allocator_);
    ASSERT_EQ(OB_SUCCESS, array.reset_iter());
    ASSERT_EQ(ObJsonNodeType::J_ARRAY, array.json_type());
    ASSERT_EQ(argument_count, array.element_count());
    const uint32_t indexes[] = {0, argument_count - 2, argument_count - 1};
    for (uint32_t index : indexes) {
      SCOPED_TRACE(index);
      ObIJsonBase *value = nullptr;
      ASSERT_EQ(OB_SUCCESS, array.get_array_element(index, value));
      ASSERT_NE(nullptr, value);
      ASSERT_EQ(ObJsonNodeType::J_INT, value->json_type());
      // Index 65536 must reach the distinct last argument, not truncate to argument zero.
      EXPECT_EQ(index == argument_count - 1 ? -17 : 1, value->get_int());
    }
  }
}

TEST_F(TestHybridSearchCursorExpr, sort_values_use_public_charset_cast_and_binary_encoding)
{
  const char latin1[] = {'c', 'a', 'f', static_cast<char>(0xe9), '\0', 'x'};
  ObObj text;
  text.set_varchar(ObString(sizeof(latin1), latin1));
  text.set_collation_type(CS_TYPE_LATIN1_BIN);
  ASSERT_NO_FATAL_FAILURE(set_cast_input(0, text, ObVarcharType, CS_TYPE_UTF8MB4_BIN));
  text.set_varchar(ObString::make_string(""));
  ASSERT_NO_FATAL_FAILURE(set_cast_input(1, text, ObVarcharType, CS_TYPE_UTF8MB4_BIN));
  output_.arg_cnt_ = 2;
  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
  ASSERT_NO_FATAL_FAILURE(expect_json(result, ObString::make_string("[\"café\\u0000x\",\"\"]")));

  const char binary[] = {'a', '\0', static_cast<char>(0xff)};
  input(0, ObHexStringType).set_string(ObString(sizeof(binary), binary));
  input(1, ObBitType, CS_TYPE_BINARY, 8).set_uint(65);
  inputs_[1].datum_meta_.length_semantics_ = static_cast<ObLengthSemantics>(8);
  ASSERT_NO_FATAL_FAILURE(expect_native_json_array());
  ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
  ASSERT_NO_FATAL_FAILURE(expect_json(result, ObString::make_string(
      "[\"base64:type15:YQD/\",\"base64:type16:QQ==\"]")));
}

TEST_F(TestHybridSearchCursorExpr, sort_values_serialize_temporal_values_for_cursor_reuse)
{
  input(0, ObDateType).set_date(1);
  input(1, ObDateTimeType, CS_TYPE_UTF8MB4_BIN, 6).set_datetime(123456);
  input(2, ObTimestampType, CS_TYPE_UTF8MB4_BIN, 6).set_timestamp(123456);
  ObObj time;
  time.set_time(-1234567);
  time.set_scale(6);
  ASSERT_NO_FATAL_FAILURE(set_cast_input(3, time, ObVarcharType, CS_TYPE_UTF8MB4_BIN));
  input(4, ObYearType).set_year(124);
  output_.arg_cnt_ = 5;
  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
  ASSERT_NO_FATAL_FAILURE(expect_json(result, ObString::make_string(
      "[\"1970-01-02\",\"1970-01-01 00:00:00.123456\","
      "\"1970-01-01 00:00:00.123456\",\"-00:00:01.234567\",2024]")));
}

TEST_F(TestHybridSearchCursorExpr, serialized_time_cursor_roundtrips_sign_and_microseconds)
{
  const int64_t times[] = {
      -TIME_MAX_VAL, -90000123456L, -1234567, -1, 0, 1, 1234567, 90000123456L, TIME_MAX_VAL};
  output_.arg_cnt_ = 1;
  for (int64_t time : times) {
    SCOPED_TRACE(time);
    ObObj source;
    source.set_time(time);
    source.set_scale(6);
    ASSERT_NO_FATAL_FAILURE(set_cast_input(0, source, ObVarcharType, CS_TYPE_UTF8MB4_BIN));
    ObDatum result;
    ASSERT_EQ(OB_SUCCESS, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
    ObString binary = result.get_string();
    ASSERT_EQ(OB_SUCCESS, ObTextStringHelper::read_real_string_data(
        &allocator_, ObJsonType, CS_TYPE_UTF8MB4_BIN, binary));
    ObJsonBin array(binary.ptr(), binary.length(), &allocator_);
    ASSERT_EQ(OB_SUCCESS, array.reset_iter());
    ObJsonBuffer printed(&allocator_);
    ASSERT_EQ(OB_SUCCESS, array.print(printed, true));
    ObJsonNode *client_value = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&allocator_, ObString(printed.length(), printed.ptr()), client_value));
    ObIJsonBase *boundary = nullptr;
    ASSERT_EQ(OB_SUCCESS, client_value->get_array_element(0, boundary));
    ASSERT_EQ(ObJsonNodeType::J_STRING, boundary->json_type());
    ObObj text;
    text.set_varchar(ObString(boundary->get_data_length(), boundary->get_data()));
    text.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    ObDataTypeCastParams params = ObBasicSessionInfo::create_dtc_params(&session_);
    ObCastCtx cast_ctx(&allocator_, &params, CM_NONE, CS_TYPE_UTF8MB4_BIN);
    ObObj decoded;
    ASSERT_EQ(OB_SUCCESS, ObObjCaster::to_type(ObTimeType, cast_ctx, text, decoded));
    EXPECT_EQ(time, decoded.get_time());
  }
}

TEST_F(TestHybridSearchCursorExpr, sort_values_follow_public_nonfinite_number_conversion)
{
  output_.arg_cnt_ = 1;
  const double values[] = {std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()};
  for (double value : values) {
    input(0, ObDoubleType).set_double(value);
    ASSERT_NO_FATAL_FAILURE(expect_native_json_array());
  }
  input(0, ObFloatType).set_float(std::numeric_limits<float>::infinity());
  ASSERT_NO_FATAL_FAILURE(expect_native_json_array());
  input(0, ObFloatType).set_float(std::numeric_limits<float>::quiet_NaN());
  ASSERT_NO_FATAL_FAILURE(expect_native_json_array());
}

TEST_F(TestHybridSearchCursorExpr, sort_values_reject_bad_shard_doc_and_propagate_child_error)
{
  output_.arg_cnt_ = 1;
  input(0, ObVarcharType, CS_TYPE_BINARY).set_string(ObString::make_string("bad"));
  inputs_[0].type_ = T_FUN_SYS_HYBRID_UNIQUE_SHARD_DOC;
  ObDatum result;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
  input(0, ObVarcharType, CS_TYPE_BINARY).set_null();
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
  inputs_[0].eval_func_ = fail_argument;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObExprHybridSearchSortValues::eval_sort_values(output_, eval_ctx_, result));
}

TEST_F(TestHybridSearchCursorExpr, shard_doc_encodes_uint64_tuple_without_overwriting_guard)
{
  char output[34];
  MEMSET(output, '#', sizeof(output));
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::encode_shard_doc(7001, 103, output + 1));
  EXPECT_EQ('#', output[0]);
  EXPECT_EQ('#', output[33]);
  EXPECT_EQ(ObString::make_string("0000000000001b590000000000000067"), ObString(32, output + 1));
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::encode_shard_doc(UINT64_MAX, UINT64_MAX, output + 1));
  EXPECT_EQ(ObString::make_string("ffffffffffffffffffffffffffffffff"), ObString(32, output + 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObExprHybridUniqueShardDoc::encode_shard_doc(1, 1, nullptr));

  char lower[32];
  char higher[32];
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::encode_shard_doc(1, UINT64_MAX, lower));
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::encode_shard_doc(2, 0, higher));
  EXPECT_LT(memcmp(lower, higher, sizeof(lower)), 0);
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::encode_shard_doc(2, 15, lower));
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::encode_shard_doc(2, 16, higher));
  EXPECT_LT(memcmp(lower, higher, sizeof(lower)), 0);
}

TEST_F(TestHybridSearchCursorExpr, shard_doc_normalizes_case_and_rejects_invalid_tokens)
{
  ObString normalized;
  ASSERT_EQ(OB_SUCCESS, ObExprHybridUniqueShardDoc::normalize_shard_doc(
      allocator_, ObString::make_string("0000000000001B5900000000000000AF"), normalized));
  EXPECT_EQ(ObString::make_string("0000000000001b5900000000000000af"), normalized);
  const char *invalid[] = {"", "0", "0000000000000000000000000000000",
      "000000000000000000000000000000000", "0000000000000000000000000000000g",
      "-0000000000000000000000000000000"};
  for (const char *text : invalid) {
    SCOPED_TRACE(text);
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObExprHybridUniqueShardDoc::normalize_shard_doc(
        allocator_, ObString::make_string(text), normalized));
  }
}

TEST_F(TestHybridSearchCursorExpr, shard_doc_normalization_propagates_allocation_failure)
{
  RejectAllocation allocator;
  ObString output;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObExprHybridUniqueShardDoc::normalize_shard_doc(
      allocator, ObString::make_string("00000000000000000000000000000001"), output));
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_hybrid_search_cursor_expr.log", true);
  // LOG_WARN emits WDIAG; the string "WARN" selects the separate DBA warning level.
  OB_LOGGER.set_log_level("WDIAG");
  return RUN_ALL_TESTS();
}
