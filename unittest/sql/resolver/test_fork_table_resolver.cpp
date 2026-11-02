/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/worker.h"
#include "sql/parser/parse_node.h"
// Only set the session's tenant identity: these early rejection paths need no tenant runtime.
#define private public
#include "sql/session/ob_sql_session_info.h"
#undef private
#include "sql/resolver/ddl/ob_fork_table_resolver.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

class TestForkTableResolver : public ::testing::Test
{
protected:
  void SetUp() override
  {
    session_.effective_tenant_id_ = 1002;
    params_.allocator_ = &allocator_;
    params_.session_info_ = &session_;
    MEMSET(&node_, 0, sizeof(node_));
    MEMSET(&src_node_, 0, sizeof(src_node_));
    MEMSET(&dst_node_, 0, sizeof(dst_node_));
    MEMSET(children_, 0, sizeof(children_));
    src_node_.type_ = T_IDENT;
    src_node_.str_value_ = "fork_src";
    src_node_.str_len_ = 8;
    dst_node_.type_ = T_IDENT;
    dst_node_.str_value_ = "fork_dst";
    dst_node_.str_len_ = 8;
    children_[ObForkDatabaseResolver::DST_DATABASE_NODE] = &dst_node_;
    children_[ObForkDatabaseResolver::SRC_DATABASE_NODE] = &src_node_;
    node_.type_ = T_FORK_DATABASE;
    // Oracle's grammar has no COPY GRANTS child, even after the MySQL tree gains one.
    node_.num_child_ = 2;
    node_.children_ = children_;
  }

  int resolve_database(const lib::Worker::CompatMode mode)
  {
    lib::CompatModeGuard mode_guard(mode);
    ObForkDatabaseResolver resolver(params_);
    return resolver.resolve(node_);
  }

  common::ObArenaAllocator allocator_;
  ObSQLSessionInfo session_;
  ObResolverParams params_;
  ParseNode node_;
  ParseNode src_node_;
  ParseNode dst_node_;
  ParseNode *children_[ObForkTableResolver::MAX_NODE];
};

TEST_F(TestForkTableResolver, oracle_database_legacy_tree_is_explicitly_unsupported)
{
  ASSERT_NE(ObForkDatabaseResolver::MAX_NODE, node_.num_child_);
  EXPECT_EQ(OB_NOT_SUPPORTED, resolve_database(lib::Worker::CompatMode::ORACLE));
}

TEST_F(TestForkTableResolver, oracle_table_legacy_tree_remains_explicitly_unsupported)
{
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::ORACLE);
  node_.type_ = T_FORK_TABLE;
  node_.num_child_ = 3;
  ObForkTableResolver resolver(params_);
  EXPECT_EQ(OB_NOT_SUPPORTED, resolver.resolve(node_));
}

TEST_F(TestForkTableResolver, mysql_database_missing_copy_grants_child_is_invalid)
{
  EXPECT_EQ(OB_ERR_UNEXPECTED, resolve_database(lib::Worker::CompatMode::MYSQL));
}

TEST_F(TestForkTableResolver, mysql_database_extra_child_is_invalid)
{
  node_.num_child_ = ObForkDatabaseResolver::MAX_NODE + 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, resolve_database(lib::Worker::CompatMode::MYSQL));
}

TEST_F(TestForkTableResolver, oracle_database_null_children_is_invalid)
{
  node_.children_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, resolve_database(lib::Worker::CompatMode::ORACLE));
}

TEST_F(TestForkTableResolver, oracle_database_wrong_node_type_is_invalid)
{
  node_.type_ = T_FORK_TABLE;
  EXPECT_EQ(OB_ERR_UNEXPECTED, resolve_database(lib::Worker::CompatMode::ORACLE));
}

TEST_F(TestForkTableResolver, oracle_database_missing_session_is_invalid)
{
  params_.session_info_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, resolve_database(lib::Worker::CompatMode::ORACLE));
}

TEST_F(TestForkTableResolver, oracle_database_missing_allocator_is_invalid)
{
  params_.allocator_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, resolve_database(lib::Worker::CompatMode::ORACLE));
}

TEST_F(TestForkTableResolver, sys_tenant_database_is_explicitly_unsupported)
{
  session_.effective_tenant_id_ = OB_SYS_TENANT_ID;
  node_.num_child_ = ObForkDatabaseResolver::MAX_NODE;
  EXPECT_EQ(OB_NOT_SUPPORTED, resolve_database(lib::Worker::CompatMode::MYSQL));
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
