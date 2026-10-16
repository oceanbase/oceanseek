/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_partition_exchange.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;
using namespace share::schema;

class TestablePartitionExchange : public ObPartitionExchange
{
public:
  using ObPartitionExchange::ObPartitionExchange;
  using ObPartitionExchange::check_search_index_definition_compatibility_;
};

TEST(TestPartitionExchange, reject_sync_async_search_definition_types)
{
  ObDDLService ddl_service;
  TestablePartitionExchange exchange(ddl_service, DATA_VERSION_4_6_2_1);
  ObTableSchema sync_schema;
  ObTableSchema async_schema;
  sync_schema.set_table_type(USER_INDEX);
  sync_schema.set_index_type(INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL);
  async_schema.set_table_type(USER_INDEX);
  async_schema.set_index_type(INDEX_TYPE_ASYNC_SEARCH_DEF_INDEX_LOCAL);
  ASSERT_TRUE(sync_schema.is_search_def_index());
  ASSERT_TRUE(async_schema.is_search_def_index());
  ASSERT_NE(sync_schema.get_index_type(), async_schema.get_index_type());
  ASSERT_EQ(sync_schema.get_table_flags(), async_schema.get_table_flags());

  // Invoke the helper directly so parent-table checks cannot mask this branch.
  for (const bool is_oracle_mode : {false, true}) {
    SCOPED_TRACE(is_oracle_mode ? "Oracle" : "MySQL");
    const int expected_ret = is_oracle_mode
        ? OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION
        : OB_TABLES_DIFFERENT_DEFINITIONS;
    EXPECT_EQ(OB_SUCCESS, exchange.check_search_index_definition_compatibility_(
        sync_schema, sync_schema, is_oracle_mode));
    EXPECT_EQ(OB_SUCCESS, exchange.check_search_index_definition_compatibility_(
        async_schema, async_schema, is_oracle_mode));
    EXPECT_EQ(expected_ret, exchange.check_search_index_definition_compatibility_(
        sync_schema, async_schema, is_oracle_mode));
    EXPECT_EQ(expected_ret, exchange.check_search_index_definition_compatibility_(
        async_schema, sync_schema, is_oracle_mode));
  }
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
