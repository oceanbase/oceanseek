/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cstdlib>
#include <gtest/gtest.h>
#define USING_LOG_PREFIX RS
#define protected public
#define private public
#include "env/ob_simple_cluster_test_base.h"
#include "rootserver/ddl_task/ob_fts_index_build_task.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "share/ob_fts_index_builder_util.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
const char *shared_storage_info = nullptr;

namespace unittest
{
using namespace common;
using namespace rootserver;
using namespace share;
using namespace share::schema;
using namespace storage;

class TestFtsDependentTaskTimeout : public ObSimpleClusterTestBase
{
public:
  TestFtsDependentTaskTimeout()
      : ObSimpleClusterTestBase("fts_dependent_timeout_env_", "50G", "50G", "50G")
  {}

  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(GCTX.is_shared_storage_mode());
    ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry("tt1", "5G", "10G", false, 4));
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    ObTenantSwitchGuard guard;
    ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id_));
    ObTenantDiskSpaceManager *disk_mgr = MTL(ObTenantDiskSpaceManager *);
    ASSERT_NE(nullptr, disk_mgr);
    OB_SERVER_DISK_SPACE_MGR.hidden_sys_data_disk_config_size_ = 1024;
    int64_t configured_size = 0;
    int64_t actual_size = 0;
    bool resized = false;
    ASSERT_EQ(OB_SUCCESS, disk_mgr->get_tenant_unit_data_disk_size(
        tenant_id_, configured_size, actual_size));
    ASSERT_EQ(OB_SUCCESS, disk_mgr->resize_total_disk_size_(configured_size, resized));
  }

  int exec_sql(const char *sql)
  {
    int64_t affected_rows = 0;
    return get_curr_simple_server().get_sql_proxy2().write(sql, affected_rows);
  }

  int read_count(ObMySQLProxy &proxy, const char *sql, int64_t &count)
  {
    int ret = OB_SUCCESS;
    ObMySQLProxy::MySQLResult result;
    if (OB_FAIL(proxy.read(result, sql))) {
    } else if (OB_FAIL(result.get_result()->next())) {
    } else if (OB_FAIL(result.get_result()->get_int("cnt", count))) {
    }
    return ret;
  }

  uint64_t tenant_id_ = OB_INVALID_TENANT_ID;
};

// Execute the original parent/child path through SQL with a 216-tablet synchronous FTS index.
TEST_F(TestFtsDependentTaskTimeout, partitioned_fts_direct_load_completes)
{
  ObMySQLProxy &tenant_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, exec_sql("alter system set direct_load_allow_fallback=False"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(
      "create table test.fts_load_source(id int primary key, content varchar(100))"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(
      "create table test.fts_load_target(id int, content varchar(100), "
      "fulltext index idx_content(content) with parser space) organization heap "
      "partition by hash(id) partitions 216"));
  ASSERT_EQ(OB_SUCCESS, exec_sql(
      "insert into test.fts_load_target values(-1, 'baseline token')"));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, sql.append("insert into test.fts_load_source values"));
  const int64_t row_count = 2160;
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, sql.append_fmt("%s(%ld, 'directtoken common')", i == 0 ? "" : ",", i));
  }
  ASSERT_EQ(OB_SUCCESS, exec_sql(sql.ptr()));
  {
    ObSchemaGetterGuard guard;
    const ObTableSchema *schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, guard));
    ASSERT_EQ(OB_SUCCESS, guard.get_table_schema(tenant_id_, ObString::make_string("test"),
        ObString::make_string("fts_load_target"), false, schema));
    ASSERT_NE(nullptr, schema);
    ObSEArray<ObTabletID, 216> tablets;
    ASSERT_EQ(OB_SUCCESS, ObDDLUtil::get_tablets(tenant_id_, schema->get_table_id(), tablets));
    ASSERT_EQ(216, tablets.count());
    bool need_doc_id = true;
    ASSERT_EQ(OB_SUCCESS, ObFtsIndexBuilderUtil::check_need_doc_id(*schema, need_doc_id));
    ASSERT_FALSE(need_doc_id);
  }
  const int64_t begin_time = ObTimeUtility::current_time();
  fprintf(stdout, "FTS_DIRECT_LOAD_BEGIN tenant=%lu tablets=216 rows=%ld time=%ld\n",
      tenant_id_, row_count, begin_time);
  fflush(stdout);
  ASSERT_EQ(OB_SUCCESS, exec_sql(
      "insert /*+ direct(true, 0, 'full') enable_parallel_dml parallel(4) "
      "query_timeout(900000000) */ into test.fts_load_target select * from test.fts_load_source"));
  fprintf(stdout, "FTS_DIRECT_LOAD_END elapsed_us=%ld\n",
      ObTimeUtility::current_time() - begin_time);
  fflush(stdout);
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, read_count(tenant_proxy,
      "select count(*) as cnt from test.fts_load_target", count));
  ASSERT_EQ(row_count + 1, count);
  ASSERT_EQ(OB_SUCCESS, read_count(tenant_proxy,
      "select count(*) as cnt from test.fts_load_target "
      "where match(content) against('directtoken')", count));
  ASSERT_EQ(row_count, count);
  ASSERT_EQ(OB_SUCCESS, read_count(tenant_proxy,
      "select count(*) as cnt from test.fts_load_target "
      "where match(content) against('baseline')", count));
  ASSERT_EQ(1, count);

  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.append_fmt(
      "select count(*) as cnt from oceanbase.__all_virtual_ddl_task_status "
      "where tenant_id=%lu and ddl_type in (%d, %d)",
      tenant_id_, DDL_DIRECT_LOAD_INSERT, DDL_CREATE_FTS_INDEX));
  for (int64_t i = 0; i < 300; ++i) {
    ASSERT_EQ(OB_SUCCESS, read_count(*GCTX.sql_proxy_, sql.ptr(), count));
    if (0 == count) {
      break;
    }
    ob_usleep(100 * 1000);
  }
  ASSERT_EQ(0, count);
  ASSERT_EQ(OB_SUCCESS, exec_sql("drop table test.fts_load_target, test.fts_load_source"));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase::common;
  using namespace oceanbase::unittest;
  const char *endpoint = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_minio = nullptr != endpoint && '\0' != endpoint[0];
  // Use the standard SS test store unless an isolated local MinIO is supplied.
  const char *bucket = use_minio ? "s3://fts-prepare-tests" : S3_BUCKET;
  const char *host = use_minio ? endpoint : S3_ENDPOINT;
  const char *access_id = use_minio ? "minioadmin" : S3_AK;
  const char *access_key = use_minio ? "minioadmin" : S3_SK;
  const char *region = use_minio ? "us-east-1" : S3_REGION;
  char storage_info[1000] = {0};
  const int ret = databuff_printf(storage_info, sizeof(storage_info),
      "%s/%ld?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      bucket, ObTimeUtility::current_time_ns(), host, access_id, access_key, region);
  if (OB_SUCCESS != ret) {
    return ret;
  }
  oceanbase::shared_storage_info = storage_info;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
