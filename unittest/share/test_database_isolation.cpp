/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include "share/resource_manager/ob_database_isolation_utils.h"
#include "share/resource_manager/ob_resource_mapping_rule_manager.h"
#include "share/ob_dml_sql_splicer.h"
#include "common/object/ob_object.h"
#include "observer/virtual_table/ob_all_virtual_tenant_worker_group.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace share
{
using namespace common;

TEST(TestDatabaseIsolation, upgrade_boundaries)
{
  const uint64_t cluster = common::cal_version(4, 6, 3, 1);
  const uint64_t data = common::cal_version(4, 6, 3, 1);
  EXPECT_EQ(cluster, DATABASE_ISOLATION_MIN_CLUSTER_VERSION);
  EXPECT_EQ(data, DATABASE_ISOLATION_MIN_DATA_VERSION);
  EXPECT_FALSE(is_database_isolation_version_supported(CLUSTER_VERSION_4_6_3_0, DATA_VERSION_4_6_3_0));
  EXPECT_FALSE(is_database_isolation_version_supported(CLUSTER_VERSION_4_6_3_0, data));
  EXPECT_FALSE(is_database_isolation_version_supported(cluster, DATA_VERSION_4_6_3_0));
  EXPECT_FALSE(is_database_isolation_version_supported(cluster - 1, data - 1));
  EXPECT_FALSE(is_database_isolation_version_supported(cluster - 1, data));
  EXPECT_FALSE(is_database_isolation_version_supported(cluster, data - 1));
  EXPECT_TRUE(is_database_isolation_version_supported(cluster, data));
  EXPECT_TRUE(is_database_isolation_version_supported(cluster + 1, data + 1));
}

class TestDatabaseIsolationView : public ::testing::Test
{
protected:
  void SetUp() override
  {
    saved_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_3_0);
    ObClusterVersion::get_instance().update_data_version(DATA_VERSION_4_6_3_0);
  }
  void TearDown() override
  {
    ObClusterVersion::get_instance().update_data_version(0);
    ObClusterVersion::get_instance().update_cluster_version(saved_cluster_version_);
  }
  uint64_t saved_cluster_version_ = 0;
};

TEST_F(TestDatabaseIsolationView, empty_before_activation)
{
  ObArenaAllocator allocator;
  observer::ObAllVirtualTenantWorkerGroup view;
  const uint64_t tenant_ids[] = {OB_SYS_TENANT_ID, 1001};
  for (const uint64_t tenant_id : tenant_ids) {
    view.reset();
    view.set_allocator(&allocator);
    view.set_effective_tenant_id(tenant_id);
    ASSERT_EQ(OB_SUCCESS, view.inner_open());
    ObNewRow *row = nullptr;
    EXPECT_EQ(OB_ITER_END, view.inner_get_next_row(row));
    EXPECT_EQ(nullptr, row);
    EXPECT_EQ(OB_ITER_END, view.inner_get_next_row(row));
  }
}

TEST(TestDatabaseIsolation, database_identifier_case)
{
  const ObString lower = ObString::make_string("sales");
  const ObString upper = ObString::make_string("SALES");
  const ObNameCaseMode modes[] = {OB_ORIGIN_AND_SENSITIVE,
      OB_LOWERCASE_AND_INSENSITIVE, OB_ORIGIN_AND_INSENSITIVE};
  for (int64_t i = 0; i < ARRAYSIZEOF(modes); ++i) {
    const ObTenantDatabaseKey key(1001, lower, modes[i]);
    const ObTenantDatabaseKey alias(1001, upper, modes[i]);
    EXPECT_EQ(OB_ORIGIN_AND_SENSITIVE != modes[i], key == alias);
    if (OB_ORIGIN_AND_SENSITIVE != modes[i]) {
      EXPECT_EQ(key.hash(), alias.hash());
    }
    EXPECT_FALSE(key == ObTenantDatabaseKey(1002, lower, modes[i]));
  }
}

TEST(TestDatabaseIsolation, database_cache_case_lookup)
{
  hash::ObHashMap<ObTenantDatabaseKey, uint64_t> map;
  ASSERT_EQ(OB_SUCCESS, map.create(16, "DbCaseTest", "DbCaseNode"));
  {
    const ObString name = ObString::make_string("sales");
    ASSERT_EQ(OB_SUCCESS, map.set_refactored(ObTenantDatabaseKey(1001, name, OB_ORIGIN_AND_INSENSITIVE), 101));
    ASSERT_EQ(OB_SUCCESS, map.set_refactored(ObTenantDatabaseKey(1002, name, OB_ORIGIN_AND_SENSITIVE), 102));
  }
  uint64_t group_id = 0;
  const ObString alias = ObString::make_string("SALES");
  ASSERT_EQ(OB_SUCCESS, map.get_refactored(ObTenantDatabaseKey(1001, alias, OB_ORIGIN_AND_INSENSITIVE), group_id));
  EXPECT_EQ(101, group_id);
  EXPECT_EQ(OB_HASH_NOT_EXIST, map.get_refactored(ObTenantDatabaseKey(1002, alias, OB_ORIGIN_AND_SENSITIVE), group_id));
  EXPECT_EQ(OB_SUCCESS, map.erase_refactored(ObTenantDatabaseKey(1001, alias, OB_ORIGIN_AND_INSENSITIVE)));
  EXPECT_EQ(1, map.size());
}

TEST(TestDatabaseIsolation, database_key_owns_name)
{
  ObTenantDatabaseKey copy;
  {
    char source[] = "sales";
    ObTenantDatabaseKey key(1001, ObString::make_string(source), OB_ORIGIN_AND_INSENSITIVE);
    ASSERT_EQ(OB_SUCCESS, copy.assign(key));
    source[0] = 'x';
  }
  EXPECT_TRUE(copy == ObTenantDatabaseKey(1001, ObString::make_string("SALES"), OB_ORIGIN_AND_INSENSITIVE));
}

TEST(TestDatabaseIsolation, quoted_database_predicate)
{
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("tenant_id", 1001));
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("attribute", ObHexEscapeSqlStr(ObString::make_string("DATABASE"))));
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("value", ObHexEscapeSqlStr(ObString::make_string("db'quoted"))));
  ASSERT_EQ(OB_SUCCESS, dml.splice_delete_sql("__all_res_mgr_mapping_rule", sql));
  EXPECT_EQ(nullptr, strstr(sql.ptr(), "db'quoted"));
  EXPECT_NE(nullptr, strstr(sql.ptr(), "64622771756F746564"));
}
} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
