/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#define private public
#include "common/object/ob_obj_type.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_rls_ddl_helper.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/mock_schema_service.h"

using namespace oceanbase;
using namespace common;
using namespace share::schema;
using namespace rootserver;

// =====================================================================
// ObDDLService::check_*_column_with_rls_policy — fast paths (no RLS ids)
// =====================================================================

// MockSchemaService::init() registers OB_SYS_TENANT_ID (= 1) with
// OB_ORIGIN_AND_INSENSITIVE, so get_tenant_name_case_mode(1, ...) succeeds.

TEST(TestObDDLRlsColumnCheck, drop_column_empty_rls_policy_ids_ok)
{
  MockSchemaService schema_service;
  ASSERT_EQ(OB_SUCCESS, schema_service.init());

  // User-space table id so ObTableSchema::add_column passes row/meta checks
  // (same id band as storage unittest schema_utils).
  const uint64_t test_table_id = 219039915101ULL;

  ObTableSchema table_schema;
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_database_id(1);
  table_schema.set_table_id(test_table_id);
  table_schema.set_rowkey_column_num(1);

  // check_columns_referenced_by_rls_policy resolves column_id → name before
  // checking RLS policies, so the column must exist in the table even when the
  // rls_policy_ids list is empty.
  ObColumnSchemaV2 col_schema;
  col_schema.set_tenant_id(OB_SYS_TENANT_ID);
  col_schema.set_table_id(test_table_id);
  col_schema.set_column_id(1);
  ASSERT_EQ(OB_SUCCESS, col_schema.set_column_name("col1"));
  col_schema.set_data_type(ObIntType);
  col_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  col_schema.set_data_length(10);
  col_schema.set_rowkey_position(1);
  ASSERT_EQ(OB_SUCCESS, table_schema.add_column(col_schema));

  ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, schema_service.get_schema_guard(guard));

  ObSEArray<uint64_t, 4> drop_col_ids;
  ASSERT_EQ(OB_SUCCESS, drop_col_ids.push_back(1));
  ASSERT_EQ(OB_SUCCESS,
            ObRlsDDLHelper::check_columns_referenced_by_rls_policy(guard, table_schema, drop_col_ids));
}

TEST(TestObDDLRlsColumnCheck, rename_column_empty_rls_policy_ids_ok)
{
  MockSchemaService schema_service;
  ASSERT_EQ(OB_SUCCESS, schema_service.init());

  ObTableSchema table_schema;
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);

  ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, schema_service.get_schema_guard(guard));

  ASSERT_EQ(OB_SUCCESS,
            ObRlsDDLHelper::check_column_referenced_by_rls_policy(guard, table_schema, ObString("any_col")));
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
