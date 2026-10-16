// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gmock/gmock.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "observer/ob_server_struct.h"
#include "share/scn.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_define.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_iterator.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;
using namespace unittest;

// Validation-path tests for ObSharedMetaReservedIterator. Real SELECT paths
// require a created user tenant; here we cover init / not-init / argument
// contracts and verify the lazy-open + reset lifecycle.
class TestSharedMetaReservedIterator : public unittest::ObSimpleClusterTestBase
{
public:
  TestSharedMetaReservedIterator()
    : unittest::ObSimpleClusterTestBase("test_shared_meta_reserved_iterator") {}

  static constexpr uint64_t FAKE_USER_TENANT_ID = 1002;
  static constexpr uint64_t SQL_TEST_META_TENANT_ID = 3001;
  static constexpr int64_t TEST_SQL_TIMEOUT_US = 10 * 1000 * 1000; // 10s

  ObMySQLProxy &sql_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  static SCN scn(uint64_t v)
  {
    SCN s;
    (void)s.convert_for_tx(v);
    return s;
  }
};

TEST_F(TestSharedMetaReservedIterator, init_invalid_tenant)
{
  ObSharedMetaReservedIterator iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(OB_INVALID_TENANT_ID, TEST_SQL_TIMEOUT_US));
  ASSERT_FALSE(iter.is_inited_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(FAKE_USER_TENANT_ID, 0));
}

TEST_F(TestSharedMetaReservedIterator, sys_and_meta_tenant)
{
  ObSharedMetaReservedIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(OB_SYS_TENANT_ID, TEST_SQL_TIMEOUT_US));
  ASSERT_EQ(OB_SYS_TENANT_ID, iter.real_tenant_id_);
  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(gen_meta_tenant_id(FAKE_USER_TENANT_ID),
                                  TEST_SQL_TIMEOUT_US));
  ASSERT_EQ(OB_SYS_TENANT_ID, iter.real_tenant_id_);
  iter.reset();
}

TEST_F(TestSharedMetaReservedIterator, init_twice_rejected)
{
  ObSharedMetaReservedIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(FAKE_USER_TENANT_ID, TEST_SQL_TIMEOUT_US));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(FAKE_USER_TENANT_ID, TEST_SQL_TIMEOUT_US));
}

TEST_F(TestSharedMetaReservedIterator, init_caches_real_tenant_id_and_proxy)
{
  ObSharedMetaReservedIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(FAKE_USER_TENANT_ID, TEST_SQL_TIMEOUT_US));
  ASSERT_TRUE(iter.is_inited_);
  ASSERT_FALSE(iter.opened_);
  ASSERT_EQ(FAKE_USER_TENANT_ID, iter.tenant_id_);
  ASSERT_EQ(gen_meta_tenant_id(FAKE_USER_TENANT_ID), iter.real_tenant_id_);
  ASSERT_NE(nullptr, iter.sql_proxy_);
  ASSERT_EQ(TEST_SQL_TIMEOUT_US, iter.sql_timeout_us_);
  ASSERT_EQ(0, iter.returned_row_count_);
}

TEST_F(TestSharedMetaReservedIterator, get_next_not_init)
{
  ObSharedMetaReservedIterator iter;
  ObSharedMetaReservedInfo info;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(info));
}

TEST_F(TestSharedMetaReservedIterator, reset_clears_state_and_allows_reinit)
{
  ObSharedMetaReservedIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(FAKE_USER_TENANT_ID, TEST_SQL_TIMEOUT_US));
  iter.reset();
  ASSERT_FALSE(iter.is_inited_);
  ASSERT_FALSE(iter.opened_);
  ASSERT_EQ(OB_INVALID_TENANT_ID, iter.tenant_id_);
  ASSERT_EQ(OB_INVALID_TENANT_ID, iter.real_tenant_id_);
  ASSERT_EQ(nullptr, iter.sql_proxy_);
  ASSERT_EQ(0, iter.sql_timeout_us_);
  ASSERT_EQ(nullptr, iter.result_);
  // re-init after reset must succeed (user tenant ids are even; +2 keeps it a user tenant).
  ASSERT_EQ(OB_SUCCESS, iter.init(FAKE_USER_TENANT_ID + 2, TEST_SQL_TIMEOUT_US));
  ASSERT_EQ(FAKE_USER_TENANT_ID + 2, iter.tenant_id_);
}

// build_select_sql_ excludes control rows by name. Pin the SQL shape so a
// future rename of these enum values surfaces here, not at runtime.
TEST_F(TestSharedMetaReservedIterator, build_select_sql_excludes_control_rows)
{
  ObSharedMetaReservedIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(FAKE_USER_TENANT_ID, TEST_SQL_TIMEOUT_US));
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, iter.build_select_sql_(sql));
  ObString s = sql.string();
  ASSERT_TRUE(nullptr != strstr(s.ptr(), "QUERY_TIMEOUT"));
  // Must mention table name, tenant_id, and both control row literals in a
  // NOT IN predicate. Substring assertions stay readable while a full string
  // comparison would be brittle.
  ASSERT_TRUE(nullptr != strstr(s.ptr(), OB_ALL_SHARED_META_RESERVED_SCN_TNAME));
  ASSERT_TRUE(nullptr != strstr(s.ptr(), "type != 'MAX_RESERVED'"));
  ASSERT_TRUE(nullptr != strstr(s.ptr(), "tenant_id = "));
  ASSERT_TRUE(nullptr != strstr(s.ptr(), "owner"));
}

// Iterator must parse the owner column for both NULL (SCN-keyed) and non-null
// (owner-keyed) reservation rows.
TEST_F(TestSharedMetaReservedIterator, iterator_read_owner_column)
{
  const ObSharedMetaReservedType max_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  const ObSharedMetaReservedType fork_table_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
  const ObSharedMetaReservedType ddl_type(ObSharedMetaReservedTypeEnum::DDL);
  const ObSharedMetaReservedType backup_type(ObSharedMetaReservedTypeEnum::BACKUP);
  const ObSharedMetaReservedType snapshot_type(ObSharedMetaReservedTypeEnum::SNAPSHOT);
  const ObString fork_owner = ObString::make_string("iter:fork_table:1");
  const ObString ddl_owner = ObString::make_string("iter:ddl:'quoted'");
  const ObString backup_owner = ObString::make_string("iter:backup:1");
  int64_t affected_rows = 0;

  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), TEST_SQL_TIMEOUT_US));
  const ObSharedMetaReservedInfo max_info(
      SQL_TEST_META_TENANT_ID, max_type, SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, op.insert_row(max_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedInfo fork_info(
      SQL_TEST_META_TENANT_ID, fork_table_type, scn(920001));
  ASSERT_EQ(OB_SUCCESS, fork_info.set_owner(fork_owner));
  ASSERT_EQ(OB_SUCCESS, op.insert_row(fork_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedInfo ddl_info(
      SQL_TEST_META_TENANT_ID, ddl_type, scn(920002));
  ASSERT_EQ(OB_SUCCESS, ddl_info.set_owner(ddl_owner));
  ASSERT_EQ(OB_SUCCESS, op.insert_row(ddl_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedInfo backup_info(
      SQL_TEST_META_TENANT_ID, backup_type, scn(920003));
  ASSERT_EQ(OB_SUCCESS, backup_info.set_owner(backup_owner));
  ASSERT_EQ(OB_SUCCESS, op.insert_row(backup_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  const ObSharedMetaReservedInfo snapshot_info(
      SQL_TEST_META_TENANT_ID, snapshot_type, scn(920004));
  ASSERT_EQ(OB_SUCCESS, op.insert_row(snapshot_info, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSharedMetaReservedIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init(SQL_TEST_META_TENANT_ID, TEST_SQL_TIMEOUT_US));

  ObArray<ObSharedMetaReservedInfo> rows;
  ObSharedMetaReservedInfo info;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret = iter.get_next(info))) {
    ASSERT_EQ(OB_SUCCESS, rows.push_back(info));
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(4, rows.count());

  bool found_fork = false;
  bool found_ddl = false;
  bool found_backup = false;
  bool found_snapshot = false;
  for (int64_t i = 0; i < rows.count(); ++i) {
    const ObSharedMetaReservedInfo &row = rows.at(i);
    ASSERT_EQ(SQL_TEST_META_TENANT_ID, row.tenant_id_);
    if (row.type_.is_fork_table()) {
      found_fork = true;
      ASSERT_EQ(fork_owner, row.get_owner());
      ASSERT_EQ(scn(920001), row.meta_scn_);
    } else if (row.type_.is_ddl()) {
      found_ddl = true;
      ASSERT_EQ(ddl_owner, row.get_owner());
      ASSERT_EQ(scn(920002), row.meta_scn_);
    } else if (row.type_.is_backup()) {
      found_backup = true;
      ASSERT_EQ(backup_owner, row.get_owner());
      ASSERT_EQ(scn(920003), row.meta_scn_);
    } else if (row.type_.is_snapshot()) {
      found_snapshot = true;
      ASSERT_FALSE(row.has_owner());
      ASSERT_EQ(scn(920004), row.meta_scn_);
    }
  }
  ASSERT_TRUE(found_fork);
  ASSERT_TRUE(found_ddl);
  ASSERT_TRUE(found_backup);
  ASSERT_TRUE(found_snapshot);

  ASSERT_EQ(OB_SUCCESS, op.delete_row(fork_info, affected_rows));
  ASSERT_EQ(OB_SUCCESS, op.delete_row(ddl_info, affected_rows));
  ASSERT_EQ(OB_SUCCESS, op.delete_row(backup_info, affected_rows));
  ASSERT_EQ(OB_SUCCESS, op.delete_row(snapshot_info, affected_rows));
  ObArray<ObSharedMetaReservedInfo> max_to_delete;
  ASSERT_EQ(OB_SUCCESS, op.scan_rows(
      SQL_TEST_META_TENANT_ID, max_type, 0 /*limit_count*/, max_to_delete));
  ASSERT_EQ(1, max_to_delete.count());
  ASSERT_EQ(OB_SUCCESS, op.delete_row(max_to_delete.at(0), affected_rows));
  ASSERT_EQ(1, affected_rows);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
