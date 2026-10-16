/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include "share/vector_index/ob_ltable_bg_dag.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "lib/oblog/ob_log.h"
#define private public
#include "pl/sys_package/ob_dbms_logic_table_mysql.h"
#undef private

namespace oceanbase
{
namespace share
{

class TestLTableBGDropLTable : public ::testing::Test
{
public:
  static ObLTableBGTaskCtx make_ctx(
      const int64_t namespace_id = 1001,
      const int64_t ltable_id = 2001,
      const uint64_t tablet_id = 3001)
  {
    ObLTableBGTaskCtx ctx;
    ctx.type_ = LTABLE_BG_LTABLE_DELETE;
    ctx.tenant_id_ = 1004;
    ctx.database_id_ = 500001;
    ctx.namespace_id_ = namespace_id;
    ctx.ltable_id_ = ltable_id;
    ctx.tablet_id_ = common::ObTabletID(tablet_id);
    MEMCPY(ctx.collection_id_,
           "0123456789abcdef0123456789abcdef",
           sizeof("0123456789abcdef0123456789abcdef"));
    return ctx;
  }
};

TEST_F(TestLTableBGDropLTable, collection_id_validation)
{
  EXPECT_TRUE(ObLTableBGSqlUtil::is_valid_collection_id(
      "0123456789abcdef0123456789abcdef"));
  EXPECT_TRUE(ObLTableBGSqlUtil::is_valid_collection_id(
      "ABCDEF0123456789ABCDEF0123456789"));
  EXPECT_FALSE(ObLTableBGSqlUtil::is_valid_collection_id(nullptr));
  EXPECT_FALSE(ObLTableBGSqlUtil::is_valid_collection_id(""));
  EXPECT_FALSE(ObLTableBGSqlUtil::is_valid_collection_id(
      "0123456789abcdef0123456789abcde"));
  EXPECT_FALSE(ObLTableBGSqlUtil::is_valid_collection_id(
      "0123456789abcdef0123456789abcdef0"));
  EXPECT_FALSE(ObLTableBGSqlUtil::is_valid_collection_id(
      "0123456789abcdef0123456789abcdeg"));
}

TEST_F(TestLTableBGDropLTable, missing_physical_table_is_idempotent_success)
{
  EXPECT_TRUE(ObLTableBGSqlUtil::is_missing_physical_table_error(
      OB_TABLE_NOT_EXIST));
  EXPECT_TRUE(ObLTableBGSqlUtil::is_missing_physical_table_error(
      OB_ENTRY_NOT_EXIST));
  EXPECT_FALSE(ObLTableBGSqlUtil::is_missing_physical_table_error(
      OB_TIMEOUT));
  EXPECT_FALSE(ObLTableBGSqlUtil::is_missing_physical_table_error(
      OB_ERR_UNEXPECTED));
}

TEST_F(TestLTableBGDropLTable, quoted_identifier_validation)
{
  EXPECT_EQ(OB_SUCCESS,
            ObLTableBGSqlUtil::check_no_backtick("test_db", "database"));
  EXPECT_EQ(OB_SUCCESS,
            ObLTableBGSqlUtil::check_no_backtick("", "database"));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObLTableBGSqlUtil::check_no_backtick("bad`db", "database"));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObLTableBGSqlUtil::check_no_backtick(nullptr, "database"));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObLTableBGSqlUtil::check_no_backtick("test_db", nullptr));
}

TEST_F(TestLTableBGDropLTable, dag_context_round_trip_and_init_guard)
{
  ObLTableBGDeleteDag dag;
  ObLTableBGTaskCtx out;
  EXPECT_EQ(OB_NOT_INIT, dag.get_ctx(out));

  const ObLTableBGTaskCtx ctx = make_ctx();
  ASSERT_EQ(OB_SUCCESS, dag.init(ctx));
  EXPECT_EQ(OB_INIT_TWICE, dag.init(ctx));
  ASSERT_EQ(OB_SUCCESS, dag.get_ctx(out));
  EXPECT_EQ(ctx.type_, out.type_);
  EXPECT_EQ(ctx.tenant_id_, out.tenant_id_);
  EXPECT_EQ(ctx.database_id_, out.database_id_);
  EXPECT_STREQ(ctx.collection_id_, out.collection_id_);
  EXPECT_EQ(ctx.namespace_id_, out.namespace_id_);
  EXPECT_EQ(ctx.ltable_id_, out.ltable_id_);
  EXPECT_EQ(ctx.tablet_id_, out.tablet_id_);
}

TEST_F(TestLTableBGDropLTable, dag_identity_isolated_by_ltable)
{
  ObLTableBGDeleteDag dag1;
  ObLTableBGDeleteDag dag2;
  ObLTableBGDeleteDag dag3;
  ASSERT_EQ(OB_SUCCESS, dag1.init(make_ctx(1001, 2001, 3001)));
  ASSERT_EQ(OB_SUCCESS, dag2.init(make_ctx(1001, 2001, 3001)));
  ASSERT_EQ(OB_SUCCESS, dag3.init(make_ctx(1001, 2002, 3001)));

  EXPECT_TRUE(dag1 == dag2);
  EXPECT_EQ(dag1.hash(), dag2.hash());
  EXPECT_FALSE(dag1 == dag3);
}

TEST_F(TestLTableBGDropLTable, dag_key_contains_drop_scope)
{
  ObLTableBGDeleteDag dag;
  char key[256] = {0};
  EXPECT_EQ(OB_NOT_INIT, dag.fill_dag_key(key, sizeof(key)));

  ASSERT_EQ(OB_SUCCESS, dag.init(make_ctx()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.fill_dag_key(nullptr, sizeof(key)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, dag.fill_dag_key(key, 0));
  ASSERT_EQ(OB_SUCCESS, dag.fill_dag_key(key, sizeof(key)));
  EXPECT_NE(nullptr, STRSTR(key, "1004_"));
  EXPECT_NE(nullptr, STRSTR(key, "_1001_2001_"));
  EXPECT_NE(nullptr, STRSTR(key, "_3001"));
}

TEST_F(TestLTableBGDropLTable, ivf_cache_key_isolated_by_ltable)
{
  const common::ObTabletID tablet_id(3001);
  const ObIvfCacheMgrKey users_key(tablet_id, 1001, 2001);
  const ObIvfCacheMgrKey users_key_copy(tablet_id, 1001, 2001);
  const ObIvfCacheMgrKey orders_key(tablet_id, 1001, 2002);
  const ObIvfCacheMgrKey other_namespace_key(tablet_id, 1002, 2001);

  EXPECT_TRUE(users_key == users_key_copy);
  EXPECT_EQ(users_key.hash(), users_key_copy.hash());
  EXPECT_FALSE(users_key == orders_key);
  EXPECT_FALSE(users_key == other_namespace_key);
}

TEST_F(TestLTableBGDropLTable, drop_ltable_sql_escaping)
{
  common::ObSqlString escaped;
  ASSERT_EQ(OB_SUCCESS,
            pl::ObDBMSLogicTableMySql::escape_sql_identifier_(
                common::ObString::make_string("db`name"), escaped));
  EXPECT_STREQ("db``name", escaped.ptr());

  escaped.reset();
  ASSERT_EQ(OB_SUCCESS,
            pl::ObDBMSLogicTableMySql::escape_sql_literal_(
                common::ObString::make_string("ltable'\\name"), escaped));
  EXPECT_STREQ("ltable''\\\\name", escaped.ptr());

  escaped.reset();
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            pl::ObDBMSLogicTableMySql::escape_sql_identifier_(
                common::ObString(), escaped));
}

TEST_F(TestLTableBGDropLTable, stale_exec_errcode_allowlist)
{
  EXPECT_TRUE(pl::ObDBMSLogicTableMySql::is_benign_stale_exec_errcode_(
      OB_NOT_MASTER));
  EXPECT_TRUE(pl::ObDBMSLogicTableMySql::is_benign_stale_exec_errcode_(
      OB_NEED_RETRY));
  EXPECT_FALSE(pl::ObDBMSLogicTableMySql::is_benign_stale_exec_errcode_(
      OB_ERR_UNEXPECTED));
  EXPECT_FALSE(pl::ObDBMSLogicTableMySql::is_benign_stale_exec_errcode_(
      OB_INVALID_ARGUMENT));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
