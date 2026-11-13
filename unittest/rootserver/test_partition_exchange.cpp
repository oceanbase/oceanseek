/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_partition_exchange.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_table_schema.h"
#include "share/search_index/ob_search_index_config.h"

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

TEST(TestPartitionExchange, compare_legacy_and_v1_search_definition_comments)
{
  ObDDLService ddl_service;
  TestablePartitionExchange exchange(ddl_service, DATA_VERSION_4_6_2_1);
  const struct {
    const char *legacy;
    const char *current_filter;
    int mysql_ret;
  } cases[] = {
    {"", "", OB_SUCCESS},
    {"业务数据", "", OB_SUCCESS},
    {"业务数据", "INCLUDE_PATHS=3$.a", OB_TABLES_DIFFERENT_DEFINITIONS},
    {"INCLUDE_TYPES=11json_string11json_number",
      "INCLUDE_TYPES=11json_string11json_number", OB_SUCCESS},
    {"INCLUDE_TYPES=11json_number11json_string",
      "INCLUDE_TYPES=11json_string11json_number", OB_SUCCESS},
    {"INCLUDE_TYPES=11json_number11json_string11json_number",
      "INCLUDE_TYPES=11json_string11json_number", OB_SUCCESS},
    {"INCLUDE_PATHS=3$.a3$.bNESTED_PATHS=3$.aINCLUDE_TYPES=11json_number11json_string11json_string",
      "INCLUDE_PATHS=3$.a3$.bNESTED_PATHS=3$.aINCLUDE_TYPES=11json_string11json_number", OB_SUCCESS},
    {"INCLUDE_TYPES=11json_string", "INCLUDE_TYPES=11json_number", OB_TABLES_DIFFERENT_DEFINITIONS},
    {"INCLUDE_PATHS=3$.a", "INCLUDE_PATHS=3$.b", OB_TABLES_DIFFERENT_DEFINITIONS},
    {"INCLUDE_PATHS=3$.a", "EXCLUDE_PATHS=3$.a", OB_TABLES_DIFFERENT_DEFINITIONS},
    {"INCLUDE_PATHS=3$.a3$.b", "INCLUDE_PATHS=3$.b3$.a", OB_TABLES_DIFFERENT_DEFINITIONS},
    {"INCLUDE_TYPES=7unknown", "", OB_SUCCESS},
    {"INCLUDE_TYPES=", "", OB_SUCCESS},
    {"INCLUDE_PATHS=", "", OB_SUCCESS},
    {"EXCLUDE_PATHS=", "", OB_SUCCESS},
    {"NESTED_PATHS=", "", OB_SUCCESS},
    {"INCLUDE_PATHS=NESTED_PATHS=3$.b", "NESTED_PATHS=3$.b", OB_SUCCESS},
    {"INCLUDE_PATHS=", "INCLUDE_PATHS=3$.a", OB_TABLES_DIFFERENT_DEFINITIONS},
    {"INCLUDE_TYPES=7unknown11json_stringtrailing", "INCLUDE_TYPES=11json_string", OB_SUCCESS},
    {"INCLUDE_PATHS=3$.atrailing", "INCLUDE_PATHS=3$.a", OB_SUCCESS},
    {"INCLUDE_PATHS=3$.atrailing", "INCLUDE_PATHS=3$.b", OB_TABLES_DIFFERENT_DEFINITIONS},
    {"INCLUDE_TYPES=100unknown", "", OB_INVALID_ARGUMENT}
  };
  for (const auto &item : cases) {
    SCOPED_TRACE(item.legacy);
    ObSearchIndexConfig current;
    ASSERT_EQ(OB_SUCCESS, current.decode(ObString(item.current_filter)));
    ObTableSchema schemas[2];
    for (int64_t i = 0; i < 2; ++i) {
      ObTableSchema &schema = schemas[i];
      schema.set_tenant_id(1001);
      schema.set_table_id(500001 + i);
      schema.set_table_type(USER_INDEX);
      schema.set_index_type(INDEX_TYPE_SEARCH_DEF_INDEX_LOCAL);
      schema.set_index_status(INDEX_STATUS_AVAILABLE);
      ObColumnSchemaV2 column;
      column.set_tenant_id(schema.get_tenant_id());
      column.set_table_id(schema.get_table_id());
      column.set_column_id(OB_APP_MIN_COLUMN_ID);
      column.set_data_type(ObJsonType);
      column.set_data_length(ObAccuracy::MAX_ACCURACY[ObJsonType].get_length());
      column.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      column.set_index_position(1);
      ASSERT_EQ(OB_SUCCESS, column.set_column_name("payload"));
      ASSERT_EQ(OB_SUCCESS, column.set_comment(i == 0 ? ObString(item.legacy) : current.get_comment()));
      ASSERT_EQ(OB_SUCCESS, schema.add_column(column));
    }
    for (const bool is_oracle_mode : {false, true}) {
      SCOPED_TRACE(is_oracle_mode ? "Oracle" : "MySQL");
      const int expected_ret = is_oracle_mode && item.mysql_ret == OB_TABLES_DIFFERENT_DEFINITIONS
          ? OB_ERR_INDEX_MISMATCH_ALTER_TABLE_EXCHANGE_PARTITION : item.mysql_ret;
      EXPECT_EQ(expected_ret, exchange.check_search_index_definition_compatibility_(
          schemas[0], schemas[1], is_oracle_mode));
      EXPECT_EQ(expected_ret, exchange.check_search_index_definition_compatibility_(
          schemas[1], schemas[0], is_oracle_mode));
    }
    EXPECT_EQ(ObString(item.legacy), schemas[0].get_column_schema(OB_APP_MIN_COLUMN_ID)->get_comment_str());
  }
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::share::ObCompatModeGetter::instance().init(nullptr);
  oceanbase::lib::Worker::CompatMode mode = oceanbase::lib::Worker::CompatMode::MYSQL;
  oceanbase::share::ObCompatModeGetter::instance().set_tenant_compat_mode(1001, mode);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
