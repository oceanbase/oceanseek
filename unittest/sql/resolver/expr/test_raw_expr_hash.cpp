/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */



#include <gtest/gtest.h>
#define private public
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "observer/ob_server.h"
#undef private
#include "lib/string/ob_sql_string.h"
#include "lib/worker.h"
#include "share/ob_cluster_version.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"
#define USING_LOG_PREFIX SQL_OPTIMIZER
using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace test
{

#define MAKE_RAW_EXPR_FROM_STR(str, expr)                                       \
  ({                                                                            \
	  ret = ObRawExprUtils::make_raw_expr_from_str(str,                           \
                                                 strlen(str),                   \
                                                 ctx,                           \
                                                 expr,                          \
                                                 columns,                       \
                                                 sys_vars,                      \
                                                 &sub_query_info,               \
                                                 aggr_exprs,                    \
                                                 win_exprs,                     \
                                                 udf_info);                     \
    ret;                                                                        \
  })

class TestRawExprToStr: public ::testing::Test
{
public:
  TestRawExprToStr() {}
  virtual ~TestRawExprToStr() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  // disallow copy and assign
  TestRawExprToStr(const TestRawExprToStr &other);
  TestRawExprToStr& operator=(const TestRawExprToStr &ohter);
};
#define T(expr1)                                         \
  do                                                     \
  {                                                      \
  MAKE_RAW_EXPR_FROM_STR(expr1, expr);                   \
  EXPECT_TRUE(OB_SUCC(ret));                        \
  uint64_t hash = expr->hash(0);                         \
  _OB_LOG(INFO, "hash(%s) = %lu", expr1, hash);        \
  } while(0)
TEST_F(TestRawExprToStr, basic)
{
  int ret = OB_SUCCESS;
  //ObExprOperatorGFactory::get_instance()->init();
  // mock params
  // stmts
  ObArenaAllocator allocator(ObModIds::TEST);
  ObRawExprFactory expr_factory(allocator);
  ObArray<ObQualifiedName> columns;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObUDFInfo> udf_info;
  ObTimeZoneInfo tz_info;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObExprResolveContext ctx(expr_factory, &tz_info, case_mode);
  ctx.connection_charset_ = ObCharset::get_default_charset();
  ctx.dest_collation_ = ObCharset::get_default_collation(ctx.connection_charset_);
  ctx.is_extract_param_type_ = false;
  ObSQLSessionInfo session;
  session.effective_tenant_id_ = 1;
  ctx.session_info_ = &session;
  OBSERVER.init_version();

  EXPECT_TRUE(OB_SUCCESS == oceanbase::ObPreProcessSysVars::init_sys_var());
  EXPECT_TRUE(OB_SUCCESS == session.test_init(0, 0, 0, NULL));
  EXPECT_TRUE(OB_SUCCESS == session.load_default_sys_variable(false, true));

  // const int64_t buf_len = 1024;
  //int64_t pos = 0;
  //char buf[buf_len];

  ObRawExpr *expr = NULL;
  // const char* inner_offset = "1+c1 > ? and 'abc' || c2 = 'def'";
  //const char* expr1 = "1+c1 > ? and SUM(1) OR 2 >= 1";
  //const char* expr2 = "CASE WHEN 10>=2 THEN 1+2 ELSE 0 END";
  //const char* expr3 = "CASE WHEN 10>=2 THEN 10-2 ELSE SUM(10-2) END";



  T("1+c1 > ? and SUM(1) OR 2 >= 1");
  T("CASE WHEN 10>=2 THEN 1+2 ELSE 0 END");
  T("1");
  T("100");
  T("sum(1)");
  T("c1");
}

class TestJsonPickSameAs : public ::testing::Test
{
protected:
  TestJsonPickSameAs() : allocator_(ObModIds::TEST), expr_factory_(allocator_) {}

  void SetUp() override
  {
    saved_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    ObClusterVersion::get_instance().update_cluster_version(CLUSTER_CURRENT_VERSION);
    ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());
    ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, NULL));
    ASSERT_EQ(OB_SUCCESS, session_.set_tenant(ObString::make_string("sys"), OB_SYS_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, session_.switch_tenant(OB_SYS_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, session_.load_default_sys_variable(false, true));
  }

  void TearDown() override
  {
    ObClusterVersion::get_instance().update_cluster_version(saved_cluster_version_);
  }

  int resolve(const char *sql, ObRawExpr *&expr)
  {
    ObArray<ObQualifiedName> columns;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObSubQueryInfo> subqueries;
    ObArray<ObAggFunRawExpr *> aggr_exprs;
    ObArray<ObWinFunRawExpr *> win_exprs;
    ObArray<ObUDFInfo> udf_info;
    ObTimeZoneInfo tz_info;
    ObExprResolveContext ctx(expr_factory_, &tz_info, OB_NAME_CASE_INVALID);
    ctx.connection_charset_ = CHARSET_UTF8MB4;
    ctx.dest_collation_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    ctx.is_extract_param_type_ = false;
    ctx.session_info_ = &session_;
    return ObRawExprUtils::make_raw_expr_from_str(sql, strlen(sql), ctx, expr,
        columns, sys_vars, &subqueries, aggr_exprs, win_exprs, udf_info);
  }

  void check_pick_equality(const char *func)
  {
    lib::CompatModeGuard guard(Worker::CompatMode::MYSQL);
    const char *picks[] = {"", " pick json_number", " pick json_string"};
    const ObItemType pick_types[] = {T_NULL, T_JSON_NUMBER, T_JSON_STRING};
    ObRawExpr *exprs[3] = {};
    for (int64_t i = 0; i < 3; ++i) {
      ObSqlString sql;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("%s('{\"a\":1}', '$.a'%s)", func, picks[i]));
      ASSERT_EQ(OB_SUCCESS, resolve(sql.ptr(), exprs[i]));
      ASSERT_NE(nullptr, exprs[i]);
      EXPECT_EQ(pick_types[i], exprs[i]->get_pick());

      // Separate expression objects with matching PICKs must compare equal.
      ObRawExpr *copy = NULL;
      ASSERT_EQ(OB_SUCCESS, resolve(sql.ptr(), copy));
      ASSERT_NE(nullptr, copy);
      EXPECT_TRUE(exprs[i]->same_as(*copy));
      EXPECT_TRUE(copy->same_as(*exprs[i]));

      // Matching PICKs must not override a mismatch in ordinary arguments.
      ObRawExpr *different_path = NULL;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("%s('{\"a\":1}', '$.b'%s)", func, picks[i]));
      ASSERT_EQ(OB_SUCCESS, resolve(sql.ptr(), different_path));
      ASSERT_NE(nullptr, different_path);
      EXPECT_FALSE(exprs[i]->same_as(*different_path));
      EXPECT_FALSE(different_path->same_as(*exprs[i]));
    }
    // Exercise both directions, including absent PICK versus explicit PICK.
    for (int64_t i = 0; i < 3; ++i) {
      for (int64_t j = i + 1; j < 3; ++j) {
        SCOPED_TRACE(::testing::Message() << func << ": " << picks[i] << " vs " << picks[j]);
        EXPECT_FALSE(exprs[i]->same_as(*exprs[j]));
        EXPECT_FALSE(exprs[j]->same_as(*exprs[i]));
      }
    }
  }

  ObArenaAllocator allocator_;
  ObRawExprFactory expr_factory_;
  ObSQLSessionInfo session_;
  uint64_t saved_cluster_version_ = 0;
};

TEST_F(TestJsonPickSameAs, json_extract)
{
  check_pick_equality("json_extract");
}

TEST_F(TestJsonPickSameAs, json_value)
{
  check_pick_equality("json_value");
}

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  ObExprOperatorFactory::register_expr_operators();
  return RUN_ALL_TESTS();
}
