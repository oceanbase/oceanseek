/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#define protected public
#include "storage/tx/ob_trans_define_v4.h"
#undef protected
#include <string>
#include "sql/ob_sql_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace test 
{
class ObSQLUtilsTest: public ::testing::Test
{
  public:
	  ObSQLUtilsTest(){}
    virtual ~ObSQLUtilsTest(){}
    virtual void SetUp(){}
    virtual void TearDown(){}
  private:
    // disallow copy
    ObSQLUtilsTest(const ObSQLUtilsTest &other);
    ObSQLUtilsTest& operator=(const ObSQLUtilsTest &other);
  private:
    // data members
};
TEST_F(ObSQLUtilsTest, make_field_name)
{
  ObString name;
  ObArenaAllocator allocator(common::ObModIds::TEST, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSQLUtils::make_field_name(NULL, -1, CS_TYPE_UTF8MB4_GENERAL_CI, NULL, name));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSQLUtils::make_field_name(NULL, 0, CS_TYPE_UTF8MB4_GENERAL_CI, NULL, name));
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(NULL, 0, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_TRUE(name.empty());
  const char *buf1 = "adf";
  int64_t len1 = 3;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf1, len1, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string("adf"));
  ASSERT_EQ(name.length(), 3);
  
  buf1 = "  adf";
  len1 = 5;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf1, len1, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string("adf"));
  ASSERT_EQ(name.length(), 3);
  
  buf1 = "\t\r\naaa";
  len1 = 6;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf1, len1, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string("aaa"));
  ASSERT_EQ(name.length(), 3);

  buf1 = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111";
  len1 = 255;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf1, len1, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string(buf1));
	printf("prt:%s, len:%d\n", name.ptr(), name.length());
  ASSERT_EQ(name.length(), 255);
  
  const char *buf2 = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111122222";
  int64_t len2 = 261;
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf2, len2, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string(buf1));
  ASSERT_EQ(name.length(), 255);

  buf2 = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111222222";
  len2 = 272;
  const char *buf3 = "111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111222";
  ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(buf2, len2, CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
  ASSERT_EQ(name, ObString::make_string(buf3));
  ASSERT_EQ(name.length(), 255);
	printf("prt:%s, len:%d\n", name.ptr(), name.length());

}

TEST_F(ObSQLUtilsTest, outtable_lob_control_nested_session)
{
  ObArenaAllocator allocator(common::ObModIds::TEST, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObExecContext parent_ctx(allocator);
  ObExecContext child_ctx(allocator);
  ObExecContext no_session_ctx(allocator);
  ObSQLSessionInfo session;

  ASSERT_FALSE(ObSQLUtils::is_nested_sql(nullptr));
  child_ctx.set_my_session(&session);
  ASSERT_FALSE(ObSQLUtils::is_nested_sql(&child_ctx));
  session.set_nested_count(1);
  ASSERT_FALSE(ObSQLUtils::is_nested_sql(&child_ctx));
  session.set_cur_exec_ctx(&parent_ctx);
  {
    LinkExecCtxGuard guard(session, child_ctx);
    ASSERT_FALSE(ObSQLUtils::is_nested_sql(&child_ctx));
  }
  parent_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = true;
  {
    LinkExecCtxGuard guard(session, child_ctx);
    ASSERT_TRUE(ObSQLUtils::is_nested_sql(&child_ctx));
  }
  session.set_nested_count(0);
  {
    LinkExecCtxGuard guard(session, child_ctx);
    ASSERT_FALSE(ObSQLUtils::is_nested_sql(&child_ctx));
  }
  session.set_nested_count(1);
  {
    LinkExecCtxGuard guard(session, no_session_ctx);
    ASSERT_FALSE(ObSQLUtils::is_nested_sql(&no_session_ctx));
  }
  parent_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = false;
  {
    LinkExecCtxGuard guard(session, child_ctx);
    ASSERT_FALSE(ObSQLUtils::is_nested_sql(&child_ctx));
  }
}

TEST_F(ObSQLUtilsTest, existing_nested_sql_branches_without_outtable_flag)
{
  ObArenaAllocator allocator(common::ObModIds::TEST, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObExecContext parent_ctx(allocator);
  ObExecContext child_ctx(allocator);
  ObSQLSessionInfo session;
  child_ctx.set_my_session(&session);
  session.set_cur_exec_ctx(&parent_ctx);
  session.set_nested_count(1);

  parent_ctx.get_das_ctx().is_fk_cascading_ = true;
  {
    LinkExecCtxGuard guard(session, child_ctx);
    ASSERT_TRUE(ObSQLUtils::is_nested_sql(&child_ctx));
  }
  parent_ctx.get_das_ctx().is_fk_cascading_ = false;
  parent_ctx.set_is_online_stats_gathering(true);
  {
    LinkExecCtxGuard guard(session, child_ctx);
    ASSERT_TRUE(ObSQLUtils::is_nested_sql(&child_ctx));
  }
  parent_ctx.set_is_online_stats_gathering(false);
  parent_ctx.get_das_ctx().is_direct_load_stream_finishing_ = true;
  {
    LinkExecCtxGuard guard(session, child_ctx);
    ASSERT_TRUE(ObSQLUtils::is_nested_sql(&child_ctx));
  }
}

TEST_F(ObSQLUtilsTest, outtable_lob_control_owner_requires_scope_and_transaction_identity)
{
  ObArenaAllocator allocator(common::ObModIds::TEST, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObExecContext parent_ctx(allocator);
  ObExecContext child_ctx(allocator);
  transaction::ObTxDesc tx;
  transaction::ObTxDesc another_tx;
  transaction::ObTxDesc shadow_tx;
  ObSQLSessionInfo session;
  ObSQLSessionInfo another_session;
  ASSERT_TRUE(tx.addr_.set_ip_addr("127.0.0.1", 2882));
  tx.tx_id_ = transaction::ObTransID(101);
  another_tx.addr_ = tx.addr_;
  another_tx.tx_id_ = tx.tx_id_;
  shadow_tx.addr_ = tx.addr_;
  shadow_tx.tx_id_ = tx.tx_id_;
  shadow_tx.flags_.SHADOW_ = true;
  parent_ctx.set_my_session(&session);
  child_ctx.set_my_session(&session);
  session.get_tx_desc() = &tx;
  session.set_is_remote(true);
  session.set_nested_count(1);
  another_session.get_tx_desc() = &tx;
  another_session.set_is_remote(true);
  another_session.set_nested_count(1);

  EXPECT_EQ(nullptr, parent_ctx.get_outtable_lob_control_owned_tx());
  EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(nullptr));
  EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
  bool allow_control_complete = true;
  EXPECT_EQ(OB_SUCCESS,
      ObSQLUtils::check_outtable_lob_control_complete(child_ctx, 1, allow_control_complete));
  EXPECT_FALSE(allow_control_complete);
  parent_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = true;
  session.set_cur_exec_ctx(&parent_ctx);
  {
    LinkExecCtxGuard guard(session, child_ctx);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    parent_ctx.set_outtable_lob_control_owned_tx(&tx);
    EXPECT_TRUE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));

    allow_control_complete = true;
    EXPECT_EQ(OB_ERR_UNEXPECTED,
        ObSQLUtils::check_outtable_lob_control_complete(child_ctx, 1, allow_control_complete));
    EXPECT_FALSE(allow_control_complete);
    ObSqlCtx sql_ctx;
    child_ctx.set_sql_ctx(&sql_ctx);
    allow_control_complete = true;
    EXPECT_EQ(OB_ERR_UNEXPECTED,
        ObSQLUtils::check_outtable_lob_control_complete(child_ctx, 1, allow_control_complete));
    EXPECT_FALSE(allow_control_complete);
    child_ctx.set_sql_ctx(nullptr);

    parent_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = false;
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    parent_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = true;
    session.set_nested_count(0);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    session.set_nested_count(1);

    parent_ctx.set_my_session(nullptr);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    parent_ctx.set_my_session(&session);
    child_ctx.set_my_session(nullptr);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    child_ctx.set_my_session(&another_session);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    child_ctx.set_my_session(&session);

    session.get_tx_desc() = nullptr;
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    // A copied descriptor is not the owned transaction, even with the same ID and address.
    session.get_tx_desc() = &another_tx;
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    session.get_tx_desc() = &tx;
    tx.tx_id_.reset();
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    tx.tx_id_ = transaction::ObTransID(101);

    // A same-server shadow must stay rejected even if it is incorrectly marked as owned.
    session.get_tx_desc() = &shadow_tx;
    parent_ctx.set_outtable_lob_control_owned_tx(&shadow_tx);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    session.get_tx_desc() = &tx;
    parent_ctx.set_outtable_lob_control_owned_tx(&tx);
    EXPECT_TRUE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
  }
  // These stack descriptors do not belong to the session's transaction allocator.
  session.get_tx_desc() = nullptr;
  another_session.get_tx_desc() = nullptr;
}

TEST_F(ObSQLUtilsTest, outtable_lob_control_owner_rejects_branches_and_reset_context)
{
  ObArenaAllocator allocator(common::ObModIds::TEST, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObExecContext parent_ctx(allocator);
  ObExecContext child_ctx(allocator);
  transaction::ObTxDesc tx;
  ObSQLSessionInfo session;
  tx.tx_id_ = transaction::ObTransID(102);
  parent_ctx.set_my_session(&session);
  child_ctx.set_my_session(&session);
  parent_ctx.set_outtable_lob_control_owned_tx(&tx);
  parent_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = true;
  session.get_tx_desc() = &tx;
  session.set_is_remote(true);
  session.set_nested_count(1);
  session.set_cur_exec_ctx(&parent_ctx);
  {
    LinkExecCtxGuard guard(session, child_ctx);
    EXPECT_TRUE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    parent_ctx.get_das_ctx().set_write_branch_id(1);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    parent_ctx.get_das_ctx().set_write_branch_id(0);
    child_ctx.get_das_ctx().set_write_branch_id(1);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    child_ctx.get_das_ctx().set_write_branch_id(0);

    parent_ctx.get_das_ctx().set_savepoint(transaction::ObTxSEQ(1, 1));
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    parent_ctx.get_das_ctx().set_savepoint(transaction::ObTxSEQ(1, 0));
    child_ctx.get_das_ctx().set_savepoint(transaction::ObTxSEQ(2, 1));
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    child_ctx.get_das_ctx().set_savepoint(transaction::ObTxSEQ(2, 0));
    EXPECT_TRUE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));

    parent_ctx.reset_op_env();
    EXPECT_EQ(nullptr, parent_ctx.get_outtable_lob_control_owned_tx());
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    parent_ctx.set_outtable_lob_control_owned_tx(&tx);
    EXPECT_TRUE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
    parent_ctx.set_outtable_lob_control_owned_tx(nullptr);
    EXPECT_FALSE(ObSQLUtils::is_outtable_lob_control_owner_sql(&child_ctx));
  }
  session.get_tx_desc() = nullptr;
}

TEST_F(ObSQLUtilsTest, make_field_name_utf8_boundary)
{
  ObArenaAllocator allocator(common::ObModIds::TEST, common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  struct TestCase {
    const char *leading_;
    int64_t prefix_len_;
    const char *tail_;
    int64_t expected_len_;
  };
  const TestCase cases[] = {
    // Every possible split inside a 2/3/4-byte UTF-8 character.
    {"", 254, "\xc3\xa9suffix", 254},
    {"", 254, "\xe5\x8f\xb7suffix", 254},
    {"", 253, "\xe5\x8f\xb7suffix", 253},
    {"", 254, "\xf0\x9f\x98\x80suffix", 254},
    {"", 253, "\xf0\x9f\x98\x80suffix", 253},
    {"", 252, "\xf0\x9f\x98\x80suffix", 252},
    // A complete character ending exactly at the byte limit must be kept.
    {"", 253, "\xc3\xa9suffix", 255},
    {"", 252, "\xe5\x8f\xb7suffix", 255},
    {"", 251, "\xf0\x9f\x98\x80suffix", 255},
    // Exercise the copy branch after stripping leading whitespace.
    {" \t\r\n", 253, "\xe5\x8f\xb7suffix", 253},
    // Inputs below or exactly at the limit must remain intact.
    {"", 251, "\xe5\x8f\xb7", 254},
    {"", 252, "\xe5\x8f\xb7", 255},
  };
  for (const TestCase &test_case : cases) {
    const std::string leading(test_case.leading_);
    const std::string value = std::string(test_case.prefix_len_, 'a') + test_case.tail_;
    const std::string input = leading + value;
    SCOPED_TRACE(::testing::Message() << "leading bytes=" << leading.length()
                                    << ", prefix bytes=" << test_case.prefix_len_
                                    << ", tail=" << test_case.tail_);
    ObString name;
    ASSERT_EQ(OB_SUCCESS, ObSQLUtils::make_field_name(input.data(), input.length(),
              CS_TYPE_UTF8MB4_GENERAL_CI, &allocator, name));
    ASSERT_EQ(value.substr(0, test_case.expected_len_), std::string(name.ptr(), name.length()));
  }
}

}
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
