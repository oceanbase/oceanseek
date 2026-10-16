/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <cstdio>
#include <cstring>
#include <unistd.h>

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/time/ob_time_utility.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/ss_data_check/ob_ss_data_check_ctx.h"
#include "storage/ss_data_check/ob_ss_data_check_dag.h"
#include "storage/ss_data_check/ob_ss_data_check_list_task.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{

struct SSDataCheckEventRow final
{
  SSDataCheckEventRow()
    : module_(),
      event_(),
      name1_(),
      value1_(),
      name2_(),
      value2_(),
      name3_(),
      value3_(),
      name4_(),
      value4_(),
      name5_(),
      value5_(),
      name6_(),
      value6_(),
      extra_info_()
  {}

  char module_[MAX_ROOTSERVICE_EVENT_DESC_LENGTH];
  char event_[MAX_ROOTSERVICE_EVENT_DESC_LENGTH];
  char name1_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value1_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name2_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value2_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name3_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value3_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name4_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value4_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name5_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value5_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char name6_[MAX_ROOTSERVICE_EVENT_NAME_LENGTH];
  char value6_[MAX_ROOTSERVICE_EVENT_VALUE_LENGTH];
  char extra_info_[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH];
};

class TestSSDataCheckServerEvent
    : public unittest::ObSimpleClusterTestBase
{
public:
  static const int64_t MEMORY_LIMIT = 8 * 1024 * 1024;
  static const int64_t FILE_BUF_SIZE = 32 * 1024;

  TestSSDataCheckServerEvent()
    : unittest::ObSimpleClusterTestBase(
          "test_ss_data_check_server_event")
  {}
  virtual ~TestSSDataCheckServerEvent() = default;

  void publish_result(
      const SCN &snapshot_meta_scn,
      const ObSSDataCheckMode check_mode,
      const ObSSDataCheckExpectedItem *missing_item);
  void wait_and_get_event(
      const int64_t begin_ts,
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const char *check_mode,
      const char *status,
      SSDataCheckEventRow &row);
  void wait_and_get_not_health_event(
      const int64_t begin_ts,
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const ObSSDataCheckExpectedItem &missing_item,
      SSDataCheckEventRow &row);
  void assert_event_row(
      const SSDataCheckEventRow &row,
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const char *check_mode,
      const char *status);
  void assert_not_health_event_row(
      const SSDataCheckEventRow &row,
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const char *check_mode,
      const ObSSDataCheckExpectedItem &missing_item);
};

void TestSSDataCheckServerEvent::publish_result(
    const SCN &snapshot_meta_scn,
    const ObSSDataCheckMode check_mode,
    const ObSSDataCheckExpectedItem *missing_item)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init(
      OB_SYS_TENANT_ID, snapshot_meta_scn, check_mode,
      MEMORY_LIMIT, FILE_BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, ctx.finish_expected_collect());
  if (OB_NOT_NULL(missing_item)) {
    ASSERT_EQ(OB_SUCCESS, ctx.record_referenced_missing(*missing_item));
  }
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());

  ObArenaAllocator dag_allocator("DCEventDag");
  ObSSDataCheckDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING);
  ObSSDataCheckFinalizeTask finalize_task;
  finalize_task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, finalize_task.init(&ctx));
  ASSERT_EQ(OB_SUCCESS, finalize_task.process());
  ASSERT_TRUE(ctx.has_published_status());
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

void TestSSDataCheckServerEvent::wait_and_get_event(
    const int64_t begin_ts,
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const char *check_mode,
    const char *status,
    SSDataCheckEventRow &row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const int64_t deadline_us = ObTimeUtility::current_time()
      + 10 * 1000 * 1000LL;
  ObSqlString sql;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT module, event, name1, value1, name2, value2, "
        "name3, value3, name4, value4 "
        "FROM oceanbase.%s WHERE time_to_usec(gmt_create) >= %ld "
        "AND module = 'SHARED_GC' "
        "AND event = 'DATA_CHECK_RESULT' "
        "AND name1 = 'tenant_id' AND value1 = '%lu' "
        "AND name2 = 'snapshot_meta_scn' AND value2 = '%lu' "
        "AND name3 = 'check_mode' AND value3 = '%s' "
        "AND name4 = 'status' AND value4 = '%s' "
        "ORDER BY gmt_create DESC LIMIT 1",
        OB_ALL_SERVER_EVENT_HISTORY_TNAME,
        begin_ts,
        tenant_id,
        snapshot_meta_scn.get_val_for_inner_table_field(),
        check_mode,
        status));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(
          res, sql.ptr()))) {
        LOG_WARN("failed to query data check server event", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data check server event result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        int64_t real_str_len = 0;
        found = true;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "module", row.module_,
            MAX_ROOTSERVICE_EVENT_DESC_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "event", row.event_,
            MAX_ROOTSERVICE_EVENT_DESC_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name1", row.name1_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value1", row.value1_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name2", row.name2_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value2", row.value2_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name3", row.name3_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value3", row.value3_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name4", row.name4_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value4", row.value4_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    if (!found) {
      usleep(100 * 1000);
    }
  } while (!found && ObTimeUtility::current_time() < deadline_us);
  ASSERT_TRUE(found) << "data check server event not found, sql=" << sql.ptr();
}

void TestSSDataCheckServerEvent::wait_and_get_not_health_event(
    const int64_t begin_ts,
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObSSDataCheckExpectedItem &missing_item,
    SSDataCheckEventRow &row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const int64_t deadline_us = ObTimeUtility::current_time()
      + 10 * 1000 * 1000LL;
  ObSqlString sql;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "SELECT module, event, name1, value1, name2, value2, "
        "name3, value3, name4, value4, name5, value5, "
        "name6, value6, extra_info "
        "FROM oceanbase.%s WHERE time_to_usec(gmt_create) >= %ld "
        "AND module = 'SHARED_GC' "
        "AND event = 'DATA_CHECK_RESULT' "
        "AND name1 = 'tenant_id' AND value1 = '%lu' "
        "AND name2 = 'snapshot_meta_scn' AND value2 = '%lu' "
        "AND name3 = 'check_mode' AND value3 = 'FULL' "
        "AND name4 = 'status' AND value4 = 'NOT_HEALTH' "
        "AND name5 = 'ls_id' AND value5 = '%ld' "
        "AND name6 = 'tablet_id' AND value6 = '%lu' "
        "ORDER BY gmt_create DESC LIMIT 1",
        OB_ALL_SERVER_EVENT_HISTORY_TNAME,
        begin_ts,
        tenant_id,
        snapshot_meta_scn.get_val_for_inner_table_field(),
        missing_item.get_ls_id().id(),
        missing_item.get_tablet_id().id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(
          res, sql.ptr()))) {
        LOG_WARN("failed to query unhealthy data check server event",
            KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unhealthy data check server event result is null",
            KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        int64_t real_str_len = 0;
        found = true;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "module", row.module_,
            MAX_ROOTSERVICE_EVENT_DESC_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "event", row.event_,
            MAX_ROOTSERVICE_EVENT_DESC_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name1", row.name1_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value1", row.value1_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name2", row.name2_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value2", row.value2_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name3", row.name3_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value3", row.value3_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name4", row.name4_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value4", row.value4_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name5", row.name5_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value5", row.value5_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "name6", row.name6_,
            MAX_ROOTSERVICE_EVENT_NAME_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "value6", row.value6_,
            MAX_ROOTSERVICE_EVENT_VALUE_LENGTH, real_str_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "extra_info", row.extra_info_,
            MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH, real_str_len);
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    if (!found) {
      usleep(100 * 1000);
    }
  } while (!found && ObTimeUtility::current_time() < deadline_us);
  ASSERT_TRUE(found)
      << "unhealthy data check server event not found, sql=" << sql.ptr();
}

void TestSSDataCheckServerEvent::assert_event_row(
    const SSDataCheckEventRow &row,
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const char *check_mode,
    const char *status)
{
  char tenant_id_buf[32] = {0};
  char snapshot_meta_scn_buf[32] = {0};
  ASSERT_GT(snprintf(
      tenant_id_buf, sizeof(tenant_id_buf), "%lu", tenant_id), 0);
  ASSERT_GT(snprintf(
      snapshot_meta_scn_buf, sizeof(snapshot_meta_scn_buf), "%lu",
      snapshot_meta_scn.get_val_for_inner_table_field()), 0);

  ASSERT_STREQ("SHARED_GC", row.module_);
  ASSERT_STREQ("DATA_CHECK_RESULT", row.event_);
  ASSERT_STREQ("tenant_id", row.name1_);
  ASSERT_STREQ(tenant_id_buf, row.value1_);
  ASSERT_STREQ("snapshot_meta_scn", row.name2_);
  ASSERT_STREQ(snapshot_meta_scn_buf, row.value2_);
  ASSERT_STREQ("check_mode", row.name3_);
  ASSERT_STREQ(check_mode, row.value3_);
  ASSERT_STREQ("status", row.name4_);
  ASSERT_STREQ(status, row.value4_);
}

void TestSSDataCheckServerEvent::assert_not_health_event_row(
    const SSDataCheckEventRow &row,
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const char *check_mode,
    const ObSSDataCheckExpectedItem &missing_item)
{
  char tenant_id_buf[32] = {0};
  char snapshot_meta_scn_buf[32] = {0};
  char ls_id_buf[32] = {0};
  char tablet_id_buf[32] = {0};
  char item_buf[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
  ASSERT_GT(snprintf(
      tenant_id_buf, sizeof(tenant_id_buf), "%lu", tenant_id), 0);
  ASSERT_GT(snprintf(
      snapshot_meta_scn_buf, sizeof(snapshot_meta_scn_buf), "%lu",
      snapshot_meta_scn.get_val_for_inner_table_field()), 0);
  ASSERT_GT(snprintf(
      ls_id_buf, sizeof(ls_id_buf), "%ld", missing_item.get_ls_id().id()), 0);
  ASSERT_GT(snprintf(
      tablet_id_buf, sizeof(tablet_id_buf), "%lu",
      missing_item.get_tablet_id().id()), 0);
  ASSERT_GT(missing_item.to_string(item_buf, sizeof(item_buf)), 0);

  ASSERT_STREQ("SHARED_GC", row.module_);
  ASSERT_STREQ("DATA_CHECK_RESULT", row.event_);
  ASSERT_STREQ("tenant_id", row.name1_);
  ASSERT_STREQ(tenant_id_buf, row.value1_);
  ASSERT_STREQ("snapshot_meta_scn", row.name2_);
  ASSERT_STREQ(snapshot_meta_scn_buf, row.value2_);
  ASSERT_STREQ("check_mode", row.name3_);
  ASSERT_STREQ(check_mode, row.value3_);
  ASSERT_STREQ("status", row.name4_);
  ASSERT_STREQ("NOT_HEALTH", row.value4_);
  ASSERT_STREQ("ls_id", row.name5_);
  ASSERT_STREQ(ls_id_buf, row.value5_);
  ASSERT_STREQ("tablet_id", row.name6_);
  ASSERT_STREQ(tablet_id_buf, row.value6_);
  ASSERT_STREQ(item_buf, row.extra_info_);
  ASSERT_NE(nullptr, strstr(row.extra_info_, "macro_id"));
  ASSERT_NE(nullptr, strstr(row.extra_info_, "reorganize_scn"));
  ASSERT_NE(nullptr, strstr(row.extra_info_, "meta_scn"));
}

TEST_F(TestSSDataCheckServerEvent, publish_health_result)
{
  SCN snapshot_meta_scn;
  ASSERT_EQ(OB_SUCCESS, snapshot_meta_scn.convert_for_gts(1000001));
  const int64_t begin_ts = ObTimeUtility::current_time();
  publish_result(
      snapshot_meta_scn, ObSSDataCheckMode::SINGLE, nullptr);

  SSDataCheckEventRow row;
  wait_and_get_event(
      begin_ts, OB_SYS_TENANT_ID, snapshot_meta_scn,
      "SINGLE", "HEALTH", row);
  assert_event_row(
      row, OB_SYS_TENANT_ID, snapshot_meta_scn,
      "SINGLE", "HEALTH");
}

TEST_F(TestSSDataCheckServerEvent, publish_not_health_result)
{
  SCN snapshot_meta_scn;
  SCN reorganize_scn;
  SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, snapshot_meta_scn.convert_for_gts(3000002));
  ASSERT_EQ(OB_SUCCESS, reorganize_scn.convert_for_gts(1000001));
  ASSERT_EQ(OB_SUCCESS, meta_scn.convert_for_gts(2000001));
  blocksstable::MacroBlockId macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
      200001, 100, 200, 300001);
  macro_id.set_storage_object_type(static_cast<uint64_t>(
      blocksstable::ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  ObSSDataCheckExpectedItem missing_item;
  ASSERT_EQ(OB_SUCCESS, missing_item.init(
      macro_id, ObLSID(1001), ObTabletID(2001),
      reorganize_scn, meta_scn));

  const int64_t begin_ts = ObTimeUtility::current_time();
  publish_result(
      snapshot_meta_scn, ObSSDataCheckMode::FULL, &missing_item);

  SSDataCheckEventRow row;
  wait_and_get_not_health_event(
      begin_ts, OB_SYS_TENANT_ID, snapshot_meta_scn,
      missing_item, row);
  assert_not_health_event_row(
      row, OB_SYS_TENANT_ID, snapshot_meta_scn, "FULL", missing_item);
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
