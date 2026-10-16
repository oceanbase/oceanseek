/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include <atomic>
#include <thread>

#include <gtest/gtest.h>

#define protected public
#define private public
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "rootserver/freeze/ob_major_freeze_service.h"
#include "share/compaction/ob_compaction_resource_manager.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_global_merge_table_operator.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#undef private
#undef protected

#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
char *shared_storage_info = nullptr;

namespace rootserver
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;

class TestSSWindowCompactionFinish : public oceanbase::unittest::ObSimpleClusterTestBase
{
public:
  TestSSWindowCompactionFinish()
    : ObSimpleClusterTestBase("test_ss_window_compaction_finish")
  {}

  int set_active_window_merge_info(
      ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const uint64_t last_merged_scn,
      const uint64_t target_scn,
      const int64_t merge_start_time);
  int insert_tablet_meta_rows(
      ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const uint64_t first_tablet_id,
      const int64_t row_count,
      const uint64_t compaction_scn,
      const uint64_t report_scn);
  int get_report_scn_count(
      ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const uint64_t first_tablet_id,
      const int64_t row_count,
      const uint64_t report_scn,
      int64_t &count);
  int update_tablet_meta_rows(
      ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const uint64_t first_tablet_id,
      const int64_t row_count,
      const uint64_t compaction_scn,
      const uint64_t report_scn);
  int wait_first_batch(
      ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const uint64_t first_tablet_id,
      const uint64_t report_scn,
      const std::atomic<bool> &update_done);
  int batch_update_report_scn(
      const uint64_t tenant_id,
      const uint64_t report_scn,
      volatile bool &stop,
      const int64_t expected_epoch);
  int execute_tenant_stmt(const char *stmt);
};

int TestSSWindowCompactionFinish::set_active_window_merge_info(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t last_merged_scn,
    const uint64_t target_scn,
    const int64_t merge_start_time)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt(
      "UPDATE %s SET frozen_scn = %lu, global_broadcast_scn = %lu, "
      "last_merged_scn = %lu, merge_status = %ld, merge_mode = %ld, "
      "is_merge_error = 0, suspend_merging = 0, merge_start_time = %ld "
      "WHERE tenant_id = %lu",
      OB_ALL_MERGE_INFO_TNAME,
      target_scn,
      target_scn,
      last_merged_scn,
      static_cast<int64_t>(ObZoneMergeInfo::MERGE_STATUS_MERGING),
      static_cast<int64_t>(ObGlobalMergeInfo::MERGE_MODE_WINDOW),
      merge_start_time,
      tenant_id))) {
    LOG_WARN("fail to build window merge info sql", KR(ret), K(tenant_id), K(target_scn));
  } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to set window merge info", KR(ret), K(tenant_id), K(sql));
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(tenant_id), K(affected_rows));
  }
  return ret;
}

int TestSSWindowCompactionFinish::insert_tablet_meta_rows(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t first_tablet_id,
    const int64_t row_count,
    const uint64_t compaction_scn,
    const uint64_t report_scn)
{
  int ret = OB_SUCCESS;
  const int64_t batch_count = 1000;
  if (OB_UNLIKELY(row_count <= 0
      || !ObTabletID(first_tablet_id).is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet argument", KR(ret), K(tenant_id), K(first_tablet_id), K(row_count));
  } else {
    for (int64_t start_idx = 0; OB_SUCC(ret) && start_idx < row_count; start_idx += batch_count) {
      const int64_t end_idx = MIN(start_idx + batch_count, row_count);
      int64_t affected_rows = 0;
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt(
          "INSERT INTO %s (tenant_id, tablet_id, ls_id, svr_ip, svr_port, compaction_scn, "
          "data_size, report_scn, status) VALUES ",
          OB_ALL_TABLET_META_TABLE_TNAME))) {
        LOG_WARN("fail to build tablet meta sql", KR(ret), K(tenant_id));
      }
      for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
        if (OB_FAIL(sql.append_fmt(
            "%s(%lu, %lu, 1, '127.0.0.1', 2882, %lu, 1, %lu, %ld)",
            start_idx == i ? "" : ",",
            tenant_id,
            first_tablet_id + i,
            compaction_scn,
            report_scn,
            static_cast<int64_t>(ObTabletReplica::SCN_STATUS_IDLE)))) {
          LOG_WARN("fail to append tablet meta sql", KR(ret), K(tenant_id), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
          LOG_WARN("fail to insert tablet meta rows", KR(ret), K(tenant_id), K(start_idx), K(end_idx));
        } else if (end_idx - start_idx != affected_rows) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected affected rows", KR(ret), K(tenant_id), K(start_idx), K(end_idx), K(affected_rows));
        }
      }
    }
  }
  return ret;
}

int TestSSWindowCompactionFinish::get_report_scn_count(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t first_tablet_id,
    const int64_t row_count,
    const uint64_t report_scn,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  count = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM %s WHERE tenant_id = %lu "
        "AND tablet_id >= %lu AND tablet_id < %lu AND report_scn = %lu",
        OB_ALL_TABLET_META_TABLE_TNAME,
        tenant_id,
        first_tablet_id,
        first_tablet_id + row_count,
        report_scn))) {
      LOG_WARN("fail to build report_scn query sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(sql_proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("fail to query report_scn", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null result", KR(ret), K(tenant_id));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next result", KR(ret), K(tenant_id));
    } else if (OB_FAIL(result->get_int("cnt", count))) {
      LOG_WARN("fail to get report_scn count", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int TestSSWindowCompactionFinish::update_tablet_meta_rows(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t first_tablet_id,
    const int64_t row_count,
    const uint64_t compaction_scn,
    const uint64_t report_scn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_UNLIKELY(row_count <= 0
      || !ObTabletID(first_tablet_id).is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet argument", KR(ret), K(tenant_id), K(first_tablet_id), K(row_count));
  } else if (OB_FAIL(sql.assign_fmt(
      "UPDATE %s SET compaction_scn = %lu, report_scn = %lu, status = %ld "
      "WHERE tenant_id = %lu AND tablet_id >= %lu AND tablet_id < %lu",
      OB_ALL_TABLET_META_TABLE_TNAME,
      compaction_scn,
      report_scn,
      static_cast<int64_t>(ObTabletReplica::SCN_STATUS_IDLE),
      tenant_id,
      first_tablet_id,
      first_tablet_id + row_count))) {
    LOG_WARN("fail to build tablet meta update sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("fail to update tablet meta rows", KR(ret), K(tenant_id), K(sql));
  } else if (row_count != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(tenant_id), K(row_count), K(affected_rows));
  }
  return ret;
}

int TestSSWindowCompactionFinish::wait_first_batch(
    ObMySQLProxy &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t first_tablet_id,
    const uint64_t report_scn,
    const std::atomic<bool> &update_done)
{
  int ret = OB_SUCCESS;
  int64_t marker_count = 0;
  const int64_t deadline = ObTimeUtility::current_time() + 30 * 1000 * 1000L;
  while (OB_SUCC(ret)
      && 0 == marker_count
      && !update_done.load()
      && ObTimeUtility::current_time() < deadline) {
    if (OB_FAIL(get_report_scn_count(
        sql_proxy, tenant_id, first_tablet_id, 1, report_scn, marker_count))) {
      LOG_WARN("fail to query first batch marker", KR(ret), K(tenant_id), K(report_scn));
    } else if (0 == marker_count) {
      ob_usleep(1000);
    }
  }
  if (OB_SUCC(ret) && 0 == marker_count) {
    ret = OB_TIMEOUT;
  }
  return ret;
}

int TestSSWindowCompactionFinish::batch_update_report_scn(
    const uint64_t tenant_id,
    const uint64_t report_scn,
    volatile bool &stop,
    const int64_t expected_epoch)
{
  return ObTabletMetaTableCompactionOperator::batch_update_report_scn(
      tenant_id,
      report_scn,
      ObTabletReplica::SCN_STATUS_ERROR,
      stop,
      expected_epoch);
}

int TestSSWindowCompactionFinish::execute_tenant_stmt(const char *stmt)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().write(stmt, affected_rows))) {
    LOG_WARN("fail to execute tenant sql", KR(ret), K(stmt));
  }
  return ret;
}

// SSW-C2-T008～T011：验证真实收口入口、资源清理、批间中断、最终事务失败恢复和幂等重试。
TEST_F(TestSSWindowCompactionFinish, finish_and_failure_recovery)
{
  const int64_t batch_tablet_count = 50001;
  const uint64_t progress_tablet_id = 100000000000;
  const uint64_t first_batch_tablet_id = 100001000000;
  const char *tenant_name = "ss_window_finish";
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant(tenant_name, "2G", "3G", false, 1, "2G"));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, tenant_name));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(tenant_name));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_stmt(
      "call DBMS_RESOURCE_MANAGER.CREATE_PLAN('INNER_DAILY_WINDOW_PLAN')"));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_stmt(
      "call DBMS_RESOURCE_MANAGER.CREATE_CONSUMER_GROUP('INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP')"));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_stmt(
      "call DBMS_RESOURCE_MANAGER.CREATE_PLAN_DIRECTIVE(PLAN => 'INNER_DAILY_WINDOW_PLAN', "
      "GROUP_OR_SUBPLAN => 'INNER_DAILY_WINDOW_COMPACTION_LOW_GROUP', MAX_IOPS => 100, "
      "MIN_IOPS => 30, WEIGHT_IOPS => 50, UTILIZATION_LIMIT => 100, MGMT_P1 => 70)"));

  MTL_SWITCH(tenant_id) {
    ObPrimaryMajorFreezeService *primary_service = MTL(ObPrimaryMajorFreezeService *);
    ObRestoreMajorFreezeService *restore_service = MTL(ObRestoreMajorFreezeService *);
    ASSERT_NE(nullptr, primary_service);
    ASSERT_NE(nullptr, restore_service);
    ASSERT_EQ(OB_SUCCESS, primary_service->switch_to_leader());
    primary_service->stop();
    primary_service->wait();
    restore_service->stop();
    restore_service->wait();
    ASSERT_NE(nullptr, primary_service->tenant_major_freeze_);

    ObTenantMajorFreeze &tenant_major_freeze = *primary_service->tenant_major_freeze_;
    ObMajorMergeInfoManager &merge_info_mgr = tenant_major_freeze.major_merge_info_mgr_;
    ObMajorMergeScheduler &merge_scheduler = tenant_major_freeze.merge_scheduler_;
    ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
    ObGlobalMergeInfo global_info;
    ASSERT_EQ(OB_SUCCESS, merge_info_mgr.get_zone_merge_mgr().reload());
    ASSERT_EQ(OB_SUCCESS, merge_info_mgr.get_zone_merge_mgr().get_snapshot(global_info));
    uint64_t first_target_scn_val = global_info.frozen_scn().get_val_for_inner_table_field();
    first_target_scn_val = MAX(first_target_scn_val,
        global_info.global_broadcast_scn().get_val_for_inner_table_field());
    first_target_scn_val = MAX(first_target_scn_val,
        global_info.last_merged_scn().get_val_for_inner_table_field()) + 100;
    const uint64_t initial_last_merged_scn_val =
        global_info.last_merged_scn().get_val_for_inner_table_field();
    const uint64_t stop_target_scn_val = first_target_scn_val + 100;
    const uint64_t epoch_target_scn_val = stop_target_scn_val + 100;
    SCN first_target_scn;
    SCN epoch_target_scn;
    ASSERT_EQ(OB_SUCCESS, first_target_scn.convert_for_inner_table_field(first_target_scn_val));
    ASSERT_EQ(OB_SUCCESS, epoch_target_scn.convert_for_inner_table_field(epoch_target_scn_val));

    int64_t current_epoch = -1;
    ASSERT_EQ(OB_SUCCESS, ObServiceEpochProxy::get_service_epoch(
        sql_proxy,
        tenant_id,
        ObServiceEpochProxy::FREEZE_SERVICE_EPOCH,
        current_epoch));
    ASSERT_GE(current_epoch, 0);
    const int64_t next_epoch = current_epoch + 1;

    int64_t schema_version = OB_INVALID_VERSION;
    uint64_t min_data_version = 0;
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_refreshed_schema_version(
        tenant_id, schema_version));
    ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(tenant_id, min_data_version));
    const ObFreezeInfo first_freeze_info(first_target_scn, schema_version, min_data_version);
    ObFreezeInfoProxy freeze_info_proxy(tenant_id);
    ASSERT_EQ(OB_SUCCESS, freeze_info_proxy.set_freeze_info(sql_proxy, first_freeze_info));

    const int64_t first_merge_start_time = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, set_active_window_merge_info(
        sql_proxy,
        tenant_id,
        initial_last_merged_scn_val,
        first_target_scn_val,
        first_merge_start_time));
    ASSERT_EQ(OB_SUCCESS, merge_info_mgr.get_zone_merge_mgr().reload());
    merge_info_mgr.get_freeze_info_mgr().reset_freeze_info();
    ObFreezeInfo loaded_freeze_info;
    ASSERT_EQ(OB_SUCCESS, merge_info_mgr.get_freeze_info(first_target_scn, loaded_freeze_info));
    ASSERT_EQ(OB_SUCCESS, insert_tablet_meta_rows(
        sql_proxy,
        tenant_id,
        progress_tablet_id,
        1,
        first_target_scn_val,
        initial_last_merged_scn_val));

    ObCompactionResourceManager resource_mgr;
    bool is_switched = false;
    ObSqlString current_plan;
    ASSERT_EQ(OB_SUCCESS, resource_mgr.get_current_resource_manager_plan_(current_plan));
    ASSERT_EQ(OB_SUCCESS, resource_mgr.check_window_plan_and_consumer_group_exist_());
    // 当前测试版本尚未开放窗口合并准入，仅绕过版本校验，资源切换仍执行真实事务逻辑。
    ASSERT_EQ(OB_SUCCESS, resource_mgr.switch_compaction_plan_(
        current_plan.string(), true, first_merge_start_time,
        merge_scheduler.window_resource_cache_, is_switched));
    ASSERT_TRUE(is_switched);
    current_plan.reset();
    ASSERT_EQ(OB_SUCCESS, resource_mgr.get_current_resource_manager_plan_(current_plan));
    EXPECT_TRUE(ObCompactionResourceManager::is_window_plan_(current_plan.string()));

    merge_scheduler.stop_ = false;
    ASSERT_EQ(OB_SUCCESS, merge_scheduler.progress_checker_.set_basic_info(
        loaded_freeze_info, current_epoch));
    merge_scheduler.progress_checker_.first_loop_in_cur_round_ = false;
    merge_scheduler.progress_checker_.progress_.total_table_cnt_ = 1;
    merge_scheduler.progress_checker_.progress_.table_cnt_[
        compaction::ObTableCompactionInfo::VERIFIED] = 1;
    merge_scheduler.progress_checker_.progress_.merge_finish_ = true;
    bool finished = false;
    ASSERT_EQ(OB_SUCCESS, merge_info_mgr.get_zone_merge_mgr().get_snapshot(global_info));
    ASSERT_EQ(OB_SUCCESS, merge_scheduler.check_and_finish_ss_window_compaction(
        current_epoch, global_info, finished));
    EXPECT_TRUE(finished);
    current_plan.reset();
    ASSERT_EQ(OB_SUCCESS, resource_mgr.get_current_resource_manager_plan_(current_plan));
    EXPECT_TRUE(ObCompactionResourceManager::is_window_plan_(current_plan.string()));

    ASSERT_EQ(OB_SUCCESS, ObGlobalMergeTableOperator::load_global_merge_info(
        sql_proxy, tenant_id, global_info));
    ASSERT_EQ(OB_SUCCESS, resource_mgr.switch_to_normal_before_major_merge(
        first_merge_start_time, merge_scheduler.window_resource_cache_));
    current_plan.reset();
    ASSERT_EQ(OB_SUCCESS, resource_mgr.get_current_resource_manager_plan_(current_plan));
    EXPECT_FALSE(ObCompactionResourceManager::is_window_plan_(current_plan.string()));

    int64_t target_report_scn_count = 0;
    ASSERT_EQ(OB_SUCCESS, get_report_scn_count(
        sql_proxy,
        tenant_id,
        progress_tablet_id,
        1,
        first_target_scn_val,
        target_report_scn_count));
    EXPECT_EQ(1, target_report_scn_count);
    ASSERT_EQ(OB_SUCCESS, ObGlobalMergeTableOperator::load_global_merge_info(
        sql_proxy, tenant_id, global_info));
    EXPECT_EQ(ObZoneMergeInfo::MERGE_STATUS_IDLE, global_info.merge_status_.get_value());
    EXPECT_EQ(first_target_scn, global_info.last_merged_scn());
    EXPECT_EQ(ObGlobalMergeInfo::MERGE_MODE_WINDOW, global_info.merge_mode_.get_value());
    EXPECT_FALSE(merge_scheduler.progress_checker_.freeze_info_.is_valid());
    EXPECT_EQ(0, merge_scheduler.progress_checker_.tablet_status_map_.size());

    const int64_t stop_merge_start_time = first_merge_start_time + 1;
    ASSERT_EQ(OB_SUCCESS, set_active_window_merge_info(
        sql_proxy,
        tenant_id,
        first_target_scn_val,
        stop_target_scn_val,
        stop_merge_start_time));
    ASSERT_EQ(OB_SUCCESS, merge_info_mgr.get_zone_merge_mgr().reload());
    ASSERT_EQ(OB_SUCCESS, insert_tablet_meta_rows(
        sql_proxy,
        tenant_id,
        first_batch_tablet_id,
        batch_tablet_count,
        stop_target_scn_val,
        first_target_scn_val));

    volatile bool stop = false;
    std::atomic<bool> update_done(false);
    int interrupt_ret = OB_SUCCESS;
    std::thread stop_thread([&]() {
      lib::set_thread_name_inner("SS_STOP_BATCH");
      interrupt_ret = wait_first_batch(
          sql_proxy, tenant_id, first_batch_tablet_id, stop_target_scn_val, update_done);
      if (OB_SUCCESS == interrupt_ret) {
        ATOMIC_STORE(&stop, true);
      }
    });
    const int stop_ret = batch_update_report_scn(
        tenant_id, stop_target_scn_val, stop, current_epoch);
    update_done.store(true);
    stop_thread.join();
    ASSERT_EQ(OB_SUCCESS, interrupt_ret);
    EXPECT_EQ(OB_CANCELED, stop_ret);
    ASSERT_EQ(OB_SUCCESS, get_report_scn_count(
        sql_proxy,
        tenant_id,
        first_batch_tablet_id,
        batch_tablet_count,
        stop_target_scn_val,
        target_report_scn_count));
    EXPECT_GT(target_report_scn_count, 0);
    EXPECT_LT(target_report_scn_count, batch_tablet_count);

    volatile bool retry_stop = false;
    ASSERT_EQ(OB_SUCCESS, batch_update_report_scn(
        tenant_id, stop_target_scn_val, retry_stop, current_epoch));
    ASSERT_EQ(OB_SUCCESS, get_report_scn_count(
        sql_proxy,
        tenant_id,
        first_batch_tablet_id,
        batch_tablet_count,
        stop_target_scn_val,
        target_report_scn_count));
    EXPECT_EQ(batch_tablet_count, target_report_scn_count);

    const int64_t epoch_merge_start_time = stop_merge_start_time + 1;
    ASSERT_EQ(OB_SUCCESS, set_active_window_merge_info(
        sql_proxy,
        tenant_id,
        first_target_scn_val,
        epoch_target_scn_val,
        epoch_merge_start_time));
    ASSERT_EQ(OB_SUCCESS, merge_info_mgr.get_zone_merge_mgr().reload());
    ASSERT_EQ(OB_SUCCESS, update_tablet_meta_rows(
        sql_proxy,
        tenant_id,
        first_batch_tablet_id,
        batch_tablet_count,
        epoch_target_scn_val,
        first_target_scn_val));

    volatile bool epoch_stop = false;
    update_done.store(false);
    interrupt_ret = OB_SUCCESS;
    int64_t epoch_affected_rows = 0;
    std::thread epoch_thread([&]() {
      lib::set_thread_name_inner("SS_EPOCH_BATCH");
      interrupt_ret = wait_first_batch(
          sql_proxy, tenant_id, first_batch_tablet_id, epoch_target_scn_val, update_done);
      if (OB_SUCCESS == interrupt_ret) {
        interrupt_ret = ObServiceEpochProxy::update_service_epoch(
            sql_proxy,
            tenant_id,
            ObServiceEpochProxy::FREEZE_SERVICE_EPOCH,
            next_epoch,
            epoch_affected_rows);
      }
    });
    const int epoch_ret = batch_update_report_scn(
        tenant_id, epoch_target_scn_val, epoch_stop, current_epoch);
    update_done.store(true);
    epoch_thread.join();
    ASSERT_EQ(OB_SUCCESS, interrupt_ret);
    EXPECT_EQ(1, epoch_affected_rows);
    EXPECT_EQ(OB_FREEZE_SERVICE_EPOCH_MISMATCH, epoch_ret);
    ASSERT_EQ(OB_SUCCESS, get_report_scn_count(
        sql_proxy,
        tenant_id,
        first_batch_tablet_id,
        batch_tablet_count,
        epoch_target_scn_val,
        target_report_scn_count));
    EXPECT_GT(target_report_scn_count, 0);
    EXPECT_LT(target_report_scn_count, batch_tablet_count);

    ASSERT_EQ(OB_SUCCESS, batch_update_report_scn(
        tenant_id, epoch_target_scn_val, epoch_stop, next_epoch));
    ASSERT_EQ(OB_SUCCESS, get_report_scn_count(
        sql_proxy,
        tenant_id,
        first_batch_tablet_id,
        batch_tablet_count,
        epoch_target_scn_val,
        target_report_scn_count));
    EXPECT_EQ(batch_tablet_count, target_report_scn_count);

    ASSERT_EQ(OB_SUCCESS, merge_scheduler.progress_checker_.clear_cached_info());
    merge_scheduler.progress_checker_.freeze_info_ =
        ObFreezeInfo(epoch_target_scn, schema_version, min_data_version);
    merge_scheduler.progress_checker_.expected_epoch_ = next_epoch;
    ASSERT_EQ(OB_SUCCESS, merge_scheduler.progress_checker_.tablet_status_map_.set_refactored(
        ObTabletID(first_batch_tablet_id), compaction::ObTabletCompactionStatusEnum::COMPACTED));
    EXPECT_EQ(OB_FREEZE_SERVICE_EPOCH_MISMATCH,
        merge_info_mgr.finish_ss_window_compaction(current_epoch, epoch_target_scn));
    ASSERT_EQ(OB_SUCCESS, ObGlobalMergeTableOperator::load_global_merge_info(
        sql_proxy, tenant_id, global_info));
    EXPECT_TRUE(global_info.is_ss_window_compaction_active(epoch_target_scn));
    EXPECT_TRUE(merge_scheduler.progress_checker_.freeze_info_.is_valid());
    EXPECT_EQ(1, merge_scheduler.progress_checker_.tablet_status_map_.size());
    ASSERT_EQ(OB_SUCCESS, merge_scheduler.finish_ss_window_compaction(next_epoch, epoch_target_scn));
    ASSERT_EQ(OB_SUCCESS, ObGlobalMergeTableOperator::load_global_merge_info(
        sql_proxy, tenant_id, global_info));
    EXPECT_EQ(ObZoneMergeInfo::MERGE_STATUS_IDLE, global_info.merge_status_.get_value());
    EXPECT_EQ(epoch_target_scn, global_info.last_merged_scn());
    EXPECT_FALSE(merge_scheduler.progress_checker_.freeze_info_.is_valid());
    EXPECT_EQ(0, merge_scheduler.progress_checker_.tablet_status_map_.size());

    merge_scheduler.stop_ = true;
  }
}

} // namespace unittest
} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  const char *s3_bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (OB_ISNULL(s3_bucket) || '\0' == s3_bucket[0]) {
    s3_bucket = oceanbase::unittest::S3_BUCKET;
  }
  const char *s3_endpoint_override = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_test_endpoint = OB_NOT_NULL(s3_endpoint_override)
      && '\0' != s3_endpoint_override[0];
  const char *s3_endpoint = use_test_endpoint
      ? s3_endpoint_override : oceanbase::unittest::S3_ENDPOINT;
  const char *s3_ak = use_test_endpoint
      ? "minioadmin" : oceanbase::unittest::S3_AK;
  const char *s3_sk = use_test_endpoint
      ? "minioadmin" : oceanbase::unittest::S3_SK;
  const char *s3_region = std::getenv("OB_MITTEST_S3_REGION");
  if (OB_ISNULL(s3_region) || '\0' == s3_region[0]) {
    s3_region = oceanbase::unittest::S3_REGION;
  }
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  const int format_ret = databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      s3_bucket,
      cur_time_ns,
      s3_endpoint,
      s3_ak,
      s3_sk,
      s3_region);
  if (OB_SUCCESS != format_ret) {
    return format_ret;
  }
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
