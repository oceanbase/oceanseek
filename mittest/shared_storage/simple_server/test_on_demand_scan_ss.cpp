/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * SS-only coverage of OnDemand get_table_schemas_with_async_index_in_tenant.
 * Async FTS DDL is rejected on SN (OB_NOT_SUPPORTED); this binary bootstraps
 * shared_storage so the scan path is not vacuously empty.
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SHARE_SCHEMA
#define protected public
#define private public
#define UNITTEST
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "share/schema/ob_on_demand_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
char *shared_storage_info = NULL;
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;

namespace
{

struct RunCtx
{
  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
  uint64_t async_fts_id_ = OB_INVALID_ID;
  uint64_t async_rb_id_ = OB_INVALID_ID;
};
RunCtx R;

bool contains_id(const ObIArray<uint64_t> &ids, const uint64_t id)
{
  for (int64_t i = 0; i < ids.count(); ++i) {
    if (ids.at(i) == id) {
      return true;
    }
  }
  return false;
}

bool same_id_set(const ObIArray<uint64_t> &lhs, const ObIArray<uint64_t> &rhs)
{
  if (lhs.count() != rhs.count()) {
    return false;
  }
  for (int64_t i = 0; i < lhs.count(); ++i) {
    if (!contains_id(rhs, lhs.at(i))) {
      return false;
    }
  }
  return true;
}

int exec_sql(ObMySQLProxy &proxy, const char *sql)
{
  int64_t affected = 0;
  return proxy.write(sql, affected);
}

int exec_sql_retry(ObMySQLProxy &proxy, const char *sql)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < 30; ++i) {
    if (OB_SUCC(exec_sql(proxy, sql))) {
      break;
    } else if (OB_NOT_MASTER != ret && OB_LS_NOT_LEADER != ret
               && OB_EAGAIN != ret && OB_TIMEOUT != ret
               && OB_TENANT_NOT_IN_SERVER != ret) {
      break;
    }
    LOG_WARN("exec_sql need retry", K(ret), K(i), K(sql));
    ob_usleep(2 * 1000 * 1000);
  }
  return ret;
}

// CREATE may finish schema write then return NOT_MASTER; retry gets -1050.
int exec_create_table_retry(ObMySQLProxy &proxy, const char *sql)
{
  int ret = exec_sql_retry(proxy, sql);
  if (OB_ERR_TABLE_EXIST == ret || -ER_TABLE_EXISTS_ERROR == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int query_uint(ObMySQLProxy &proxy, const char *sql, const char *col, uint64_t &value)
{
  int ret = OB_SUCCESS;
  value = OB_INVALID_ID;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(proxy.read(res, sql))) {
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result->next())) {
    } else if (OB_FAIL(result->get_uint(col, value))) {
    }
  }
  return ret;
}

int get_table_id_by_name(ObMySQLProxy &proxy, const uint64_t tenant_id,
                         const char *table_name, uint64_t &table_id)
{
  ObSqlString sql;
  int ret = sql.assign_fmt(
      "SELECT table_id FROM oceanbase.__all_virtual_table "
      "WHERE tenant_id = %lu AND table_name = '%s' LIMIT 1",
      tenant_id, table_name);
  if (OB_SUCC(ret)) {
    ret = query_uint(proxy, sql.ptr(), "table_id", table_id);
  }
  return ret;
}

int collect_ids(const ObIArray<const ObSimpleTableSchemaV2 *> &schemas,
                ObIArray<uint64_t> &ids)
{
  int ret = OB_SUCCESS;
  ids.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
    if (OB_ISNULL(schemas.at(i))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ids.push_back(schemas.at(i)->get_table_id()))) {
    }
  }
  return ret;
}

int get_latest_guard(const uint64_t tenant_id, ObSchemaGetterGuard &guard)
{
  return GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard);
}

const ObOnDemandSchemaGetterGuard &od()
{
  return ObOnDemandSchemaGetterGuard::instance();
}

} // namespace

class TestOnDemandScanSS : public ObSimpleClusterTestBase
{
public:
  TestOnDemandScanSS()
      : ObSimpleClusterTestBase("test_on_demand_scan_ss_",
                                "50G", "50G", "50G")
  {}

  void wait_tenant_to_leader(const uint64_t tenant_id)
  {
    share::ObTenantSwitchGuard tenant_guard;
    int ret = OB_ERR_UNEXPECTED;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
    ObLS *ls = nullptr;
    ObLSID ls_id(ObLSID::SYS_LS_ID);
    ObLSHandle handle;
    ObLSService *ls_svr = MTL(ObLSService *);
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    for (int i = 0; i < 300; i++) {
      ObRole role;
      int64_t proposal_id = 0;
      ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
      if (role == ObRole::LEADER) {
        ret = OB_SUCCESS;
        break;
      }
      ob_usleep(100 * 1000);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  void wait_sys_to_leader()
  {
    wait_tenant_to_leader(OB_SYS_TENANT_ID);
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }
};

TEST_F(TestOnDemandScanSS, observer_start)
{
  LOG_INFO("observer_start succ");
}

TEST_F(TestOnDemandScanSS, prepare_async_index_tables)
{
  wait_sys_to_leader();
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  do {
    if (OB_FAIL(create_tenant("tt1", "3G", "3G", false /*oracle_mode*/, 2))) {
      LOG_WARN("create_tenant fail, need retry", K(ret), K(retry_cnt));
      ob_usleep(15 * 1000 * 1000);
    }
    retry_cnt++;
  } while (OB_FAIL(ret) && retry_cnt < 10);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_));
  ASSERT_NE(OB_INVALID_TENANT_ID, R.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  wait_tenant_to_leader(R.tenant_id_);

  ObMySQLProxy &sys_proxy = get_curr_simple_server().get_sql_proxy();
  ObMySQLProxy &proxy2 = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, exec_sql_retry(sys_proxy,
      "alter system set _enable_async_index = true tenant = 'all'"));
  ASSERT_EQ(OB_SUCCESS, exec_sql_retry(sys_proxy,
      "alter system set _enable_async_index_refresh = true tenant = 'all'"));
  usleep(2 * 1000 * 1000);

  ASSERT_EQ(OB_SUCCESS, exec_sql_retry(proxy2, "set recyclebin = on"));
  ASSERT_EQ(OB_SUCCESS, exec_create_table_retry(proxy2,
      "create table od_async_fts(c1 int, c2 varchar(128), "
      "async fulltext index od_async_idx(c2)) organization heap"));
  ASSERT_EQ(OB_SUCCESS, exec_create_table_retry(proxy2,
      "create table od_async_rb(c1 int, c2 varchar(128), "
      "async fulltext index od_async_rb_idx(c2)) organization heap"));
  usleep(2 * 1000 * 1000);
  ASSERT_EQ(OB_SUCCESS,
            get_table_id_by_name(sys_proxy, R.tenant_id_, "od_async_fts", R.async_fts_id_));
  ASSERT_EQ(OB_SUCCESS,
            get_table_id_by_name(sys_proxy, R.tenant_id_, "od_async_rb", R.async_rb_id_));
  ASSERT_NE(OB_INVALID_ID, R.async_fts_id_);
  ASSERT_NE(OB_INVALID_ID, R.async_rb_id_);
  ASSERT_EQ(OB_SUCCESS, exec_sql_retry(proxy2, "drop table od_async_rb"));
  usleep(2 * 1000 * 1000);
}

TEST_F(TestOnDemandScanSS, scan_async_index_data_tables)
{
  ASSERT_TRUE(GCTX.is_shared_storage_mode());
  ASSERT_NE(OB_INVALID_ID, R.async_fts_id_);
  ASSERT_NE(OB_INVALID_ID, R.async_rb_id_);

  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(R.tenant_id_));

  ObSchemaGetterGuard guard;
  ObArray<const ObSimpleTableSchemaV2 *> od_schemas;
  ObArray<uint64_t> od_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, guard));
  ASSERT_EQ(OB_SUCCESS,
            od().get_table_schemas_with_async_index_in_tenant(
                guard, R.tenant_id_, od_schemas));
  ASSERT_EQ(OB_SUCCESS, collect_ids(od_schemas, od_ids));
  LOG_INFO("async index data tables", "od", od_ids.count(),
           K(R.async_fts_id_), K(R.async_rb_id_));
  ASSERT_FALSE(od_ids.empty());
  ASSERT_TRUE(contains_id(od_ids, R.async_fts_id_));
  // Recyclebin tables retain async indexes and must remain discoverable for refresh.
  ASSERT_TRUE(contains_id(od_ids, R.async_rb_id_));

  ObSchemaGetterGuard strong_guard;
  ObArray<const ObSimpleTableSchemaV2 *> strong_schemas;
  ObArray<uint64_t> strong_ids;
  ASSERT_EQ(OB_SUCCESS, get_latest_guard(R.tenant_id_, strong_guard));
  strong_guard.set_schema_scan_consistency_mode(ObSchemaScanConsistencyMode::STRONG);
  ASSERT_EQ(OB_SUCCESS,
            od().get_table_schemas_with_async_index_in_tenant(
                strong_guard, R.tenant_id_, strong_schemas));
  ASSERT_EQ(OB_SUCCESS, collect_ids(strong_schemas, strong_ids));
  ASSERT_TRUE(contains_id(strong_ids, R.async_fts_id_));
  ASSERT_TRUE(contains_id(strong_ids, R.async_rb_id_));
  ASSERT_TRUE(same_id_set(strong_ids, od_ids));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  oceanbase::common::databuff_printf(
      buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns,
      oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK,
      oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
