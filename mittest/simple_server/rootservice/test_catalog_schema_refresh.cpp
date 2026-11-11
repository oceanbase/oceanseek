// owner: cjl476581

/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gmock/gmock.h>

#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "share/schema/ob_schema_service_sql_impl.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace share::schema;
using namespace common;

static uint64_t g_tenant_id = OB_INVALID_TENANT_ID;

class TestCatalogSchemaRefresh : public unittest::ObSimpleClusterTestBase
{
public:
  TestCatalogSchemaRefresh() : unittest::ObSimpleClusterTestBase("test_catalog_schema_refresh") {}
};

TEST_F(TestCatalogSchemaRefresh, prepare_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(TestCatalogSchemaRefresh, test_get_increment_catalog_keys_reversely)
{
  const uint64_t user_id = OB_SYS_USER_ID;
  const ObString catalog_name = ObString::make_string("catalog_for_refresh");
  const ObString database_name = ObString::make_string("database_for_refresh");
  const ObString table_name = ObString::make_string("table_for_refresh");
  ObSchemaGetterGuard schema_guard;
  const ObSchemaMgr *schema_mgr = nullptr;
  int64_t curr_version = OB_INVALID_VERSION;
  int64_t version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;

  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(OB_NOT_NULL(GCTX.schema_service_));
  ObSchemaService *schema_service = GCTX.schema_service_->get_schema_service();
  ASSERT_TRUE(OB_NOT_NULL(schema_service));
  ASSERT_EQ(OB_SUCCESS,
            get_curr_observer().get_schema_service().get_tenant_schema_guard(
                g_tenant_id, schema_guard));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_version(g_tenant_id, curr_version));
  ASSERT_NE(OB_INVALID_VERSION, curr_version);
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_status(g_tenant_id, schema_status));
  ASSERT_TRUE(OB_NOT_NULL(schema_guard.schema_service_));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_schema_mgr(g_tenant_id, schema_mgr));
  ASSERT_TRUE(OB_NOT_NULL(schema_mgr));

  ObSchemaMgr &mutable_schema_mgr = const_cast<ObSchemaMgr&>(*schema_mgr);
  uint64_t catalog_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, schema_service->fetch_new_catalog_id(g_tenant_id, catalog_id));
  ASSERT_NE(OB_INVALID_ID, catalog_id);
  ASSERT_EQ(OB_SUCCESS,
            schema_service->gen_new_schema_version(g_tenant_id, curr_version, version));
  ASSERT_NE(OB_INVALID_VERSION, version);

  ObCatalogSchema catalog_schema;
  catalog_schema.set_tenant_id(g_tenant_id);
  catalog_schema.set_catalog_id(catalog_id);
  catalog_schema.set_schema_version(version);
  ASSERT_EQ(OB_SUCCESS, catalog_schema.set_catalog_name(catalog_name));
  ASSERT_EQ(OB_SUCCESS,
            mutable_schema_mgr.catalog_mgr_.add_catalog(
                catalog_schema, ObNameCaseMode::OB_ORIGIN_AND_SENSITIVE));

  ObCatalogPriv catalog_priv;
  catalog_priv.set_tenant_id(g_tenant_id);
  catalog_priv.set_user_id(user_id);
  catalog_priv.set_schema_version(version);
  catalog_priv.set_priv_set(OB_PRIV_SELECT);
  ASSERT_EQ(OB_SUCCESS, catalog_priv.set_catalog_name(catalog_name));
  ASSERT_EQ(OB_SUCCESS, mutable_schema_mgr.priv_mgr_.add_catalog_priv(catalog_priv));

  ObCatalogDatabasePriv database_priv;
  database_priv.set_tenant_id(g_tenant_id);
  database_priv.set_user_id(user_id);
  database_priv.set_schema_version(version);
  database_priv.set_priv_set(OB_PRIV_SELECT);
  ASSERT_EQ(OB_SUCCESS, database_priv.set_catalog_name(catalog_name));
  ASSERT_EQ(OB_SUCCESS, database_priv.set_database_name(database_name));
  ASSERT_EQ(OB_SUCCESS,
            mutable_schema_mgr.priv_mgr_.add_catalog_database_priv(database_priv));

  ObCatalogTablePriv table_priv;
  table_priv.set_tenant_id(g_tenant_id);
  table_priv.set_user_id(user_id);
  table_priv.set_schema_version(version);
  table_priv.set_priv_set(OB_PRIV_SELECT);
  ASSERT_EQ(OB_SUCCESS, table_priv.set_catalog_name(catalog_name));
  ASSERT_EQ(OB_SUCCESS, table_priv.set_database_name(database_name));
  ASSERT_EQ(OB_SUCCESS, table_priv.set_table_name(table_name));
  ASSERT_EQ(OB_SUCCESS, mutable_schema_mgr.priv_mgr_.add_catalog_table_priv(table_priv));

  ObServerSchemaService::AllSchemaKeys keys;
  ASSERT_EQ(OB_SUCCESS, keys.create(8));

  ObSchemaOperation catalog_op;
  catalog_op.op_type_ = OB_DDL_CREATE_CATALOG;
  catalog_op.tenant_id_ = g_tenant_id;
  catalog_op.catalog_id_ = catalog_id;
  ASSERT_EQ(OB_SUCCESS,
            schema_guard.schema_service_->get_increment_catalog_keys_reversely(
                *schema_mgr, catalog_op, keys));

  ObSchemaOperation catalog_priv_op;
  catalog_priv_op.op_type_ = OB_DDL_GRANT_REVOKE_CATALOG;
  catalog_priv_op.tenant_id_ = g_tenant_id;
  catalog_priv_op.user_id_ = user_id;
  catalog_priv_op.legacy_catalog_name_ = catalog_name;
  ASSERT_EQ(OB_SUCCESS,
            schema_guard.schema_service_->get_increment_catalog_priv_keys_reversely(
                *schema_mgr, catalog_priv_op, keys));

  ObSchemaOperation database_priv_op;
  database_priv_op.op_type_ = OB_DDL_GRANT_REVOKE_CATALOG_DATABASE_PRIV;
  database_priv_op.tenant_id_ = g_tenant_id;
  database_priv_op.user_id_ = user_id;
  database_priv_op.catalog_name_ = catalog_name;
  database_priv_op.database_name_ = database_name;
  ASSERT_EQ(OB_SUCCESS,
            schema_guard.schema_service_->get_increment_catalog_database_priv_keys_reversely(
                *schema_mgr, database_priv_op, keys));

  ObSchemaOperation table_priv_op;
  table_priv_op.op_type_ = OB_DDL_GRANT_REVOKE_CATALOG_TABLE_PRIV;
  table_priv_op.tenant_id_ = g_tenant_id;
  table_priv_op.user_id_ = user_id;
  table_priv_op.catalog_name_ = catalog_name;
  table_priv_op.database_name_ = database_name;
  table_priv_op.table_name_ = table_name;
  ASSERT_EQ(OB_SUCCESS,
            schema_guard.schema_service_->get_increment_catalog_table_priv_keys_reversely(
                *schema_mgr, table_priv_op, keys));

  ASSERT_EQ(0, keys.new_catalog_keys_.size());
  ASSERT_EQ(1, keys.del_catalog_keys_.size());
  ASSERT_EQ(0, keys.new_catalog_priv_keys_.size());
  ASSERT_EQ(1, keys.del_catalog_priv_keys_.size());
  ASSERT_EQ(0, keys.new_catalog_database_priv_keys_.size());
  ASSERT_EQ(1, keys.del_catalog_database_priv_keys_.size());
  ASSERT_EQ(0, keys.new_catalog_table_priv_keys_.size());
  ASSERT_EQ(1, keys.del_catalog_table_priv_keys_.size());
  ASSERT_EQ(OB_SUCCESS,
            schema_guard.schema_service_->update_schema_mgr(
                inner_sql_proxy, schema_status, mutable_schema_mgr, curr_version, keys));

  const ObCatalogSchema *actual_catalog = nullptr;
  const ObCatalogPriv *actual_catalog_priv = nullptr;
  const ObCatalogDatabasePriv *actual_database_priv = nullptr;
  const ObCatalogTablePriv *actual_table_priv = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            mutable_schema_mgr.catalog_mgr_.get_schema_by_id(catalog_id, actual_catalog));
  ASSERT_TRUE(OB_ISNULL(actual_catalog));
  ASSERT_EQ(OB_SUCCESS,
            mutable_schema_mgr.priv_mgr_.get_catalog_priv(
                ObCatalogPrivSortKey(g_tenant_id, user_id, catalog_name), actual_catalog_priv));
  ASSERT_TRUE(OB_ISNULL(actual_catalog_priv));
  ASSERT_EQ(OB_SUCCESS,
            mutable_schema_mgr.priv_mgr_.get_catalog_database_priv(
                ObCatalogDatabasePrivSortKey(
                    g_tenant_id, user_id, catalog_name, database_name),
                actual_database_priv));
  ASSERT_TRUE(OB_ISNULL(actual_database_priv));
  ASSERT_EQ(OB_SUCCESS,
            mutable_schema_mgr.priv_mgr_.get_catalog_table_priv(
                ObCatalogTablePrivSortKey(
                    g_tenant_id, user_id, catalog_name, database_name, table_name),
                actual_table_priv));
  ASSERT_TRUE(OB_ISNULL(actual_table_priv));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
