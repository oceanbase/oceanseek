// owner: muwei.ym
// owner group: storage_ha

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "test_transfer_common_fun.h"
#include "env/ob_simple_server_restart_helper.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "rootserver/ob_tenant_transfer_service.h" // ObTenantTransferService
#include "share/transfer/ob_transfer_task_operator.h" // ObTransferTaskOperator
#include "storage/high_availability/ob_transfer_handler.h"  //ObTransferHandler
#include "lib/utility/utility.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_service.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{
using namespace share::schema;
using namespace common;
using namespace share;
using namespace transaction::tablelock;
using namespace rootserver;

static uint64_t g_tenant_id = OB_INVALID_TENANT_ID;

static const char *TEST_FILE_NAME = "reorg_info_table_basic";
static ObTransferTask g_task;

class TestMemberTable : public unittest::ObSimpleClusterTestBase
{
public:
  TestMemberTable() : unittest::ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

TEST_F(TestMemberTable, prepare_valid_data)
{
  g_tenant_id = OB_INVALID_TENANT_ID;

  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
}

TEST_F(TestMemberTable, insert_with_trans)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));

  //prepare member table data
  ObArray<ObTabletReorgInfoData> reorg_info_data;
  ObTabletID tablet_id(3000000); // Keep this DML-only row unrelated to existing internal tablets.
  SCN reorganization_scn(SCN::min_scn());
  SCN transfer_scn(SCN::min_scn());
  ObLSID relative_ls_id(1001);
  const ObTabletStatus tablet_status(ObTabletStatus::TRANSFER_OUT);
  int64_t transfer_seq = 0;
  const SCN src_reorgainzation_scn(SCN::min_scn());
  ret = ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(tablet_id, reorganization_scn, tablet_status,
      relative_ls_id, transfer_seq, transfer_scn, src_reorgainzation_scn, reorg_info_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  //insert
  ObLSID ls_id(1);
  ObMySQLTransaction trans1;
  ObTabletReorgInfoTableWriteOperator write_op1;
  ASSERT_EQ(OB_SUCCESS, trans1.start(&inner_sql_proxy, g_tenant_id));
  ret = write_op1.init(trans1, g_tenant_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_op1.insert_rows(reorg_info_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = trans1.end(OB_SUCC(ret));
  ASSERT_EQ(OB_SUCCESS, ret);

  //conflict rowkey

  ObMySQLTransaction trans2;
  ObTabletReorgInfoTableWriteOperator write_op2;
  ASSERT_EQ(OB_SUCCESS, trans2.start(&inner_sql_proxy, g_tenant_id));
  ret = write_op2.init(trans2, g_tenant_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_op2.insert_rows(reorg_info_data);
  ASSERT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, ret);
  ret = trans2.end(OB_SUCC(ret));
  ASSERT_EQ(OB_SUCCESS, ret);

  //single get
  LOG_WARN("victor start do read");
  const ObTabletReorgInfoData &old_data = reorg_info_data.at(0);
  ObTabletReorgInfoData get_data;
  ObTabletReorgInfoTableReadOperator read_op;
  ret = read_op.init(g_tenant_id, ls_id, old_data.key_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = read_op.get_next(get_data);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_WARN("member table data", K(old_data), K(get_data));
  ret = read_op.get_next(get_data);
  ASSERT_EQ(OB_ITER_END, ret);
  //ASSERT_EQ(old_data, get_data);
  LOG_WARN("victor end do read");
}

TEST_F(TestMemberTable, insert_without_trans)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &inner_sql_proxy = get_curr_observer().get_mysql_proxy();
  ASSERT_TRUE(is_valid_tenant_id(g_tenant_id));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));

  //prepare member table data
  ObArray<ObTabletReorgInfoData> reorg_info_data;
  ObTabletID tablet_id(112);
  SCN reorganization_scn(SCN::min_scn());
  SCN transfer_scn(SCN::min_scn());
  ObLSID relative_ls_id(1001);
  ObTabletStatus tablet_status(ObTabletStatus::TRANSFER_OUT);
  int64_t transfer_seq = 0;
  const SCN src_reorgainzation_scn(SCN::min_scn());
  ret = ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(tablet_id, reorganization_scn, tablet_status,
      relative_ls_id, transfer_seq, transfer_scn, src_reorgainzation_scn, reorg_info_data);
  ASSERT_EQ(OB_SUCCESS, ret);

  //insert
  ObLSID ls_id(1);
  ObSingleConnectionProxy single_connection;
  ret = single_connection.connect(g_tenant_id, 0, &inner_sql_proxy);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletReorgInfoTableWriteOperator write_op;
  ret = write_op.init(single_connection, g_tenant_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = write_op.insert_rows(reorg_info_data);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}


TEST_F(TestMemberTable, transfer_recycle_after_local_replay)
{
  if (OB_INVALID_TENANT_ID == g_tenant_id) {
    ASSERT_EQ(OB_SUCCESS, create_tenant());
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  }
  const uint64_t tenant_id = g_tenant_id;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  auto *service = MTL(ObTabletReorgInfoTableService *);
  auto *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, service);
  ASSERT_NE(nullptr, ls_service);
  // Drive GC synchronously for the suite's tenant.
  service->stop();
  service->wait();

  const ObLSID ls_id(1);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ObTabletReorgInfoTable &table = *ls->get_reorg_info_table();
  ASSERT_EQ(OB_SUCCESS, table.update_can_recycle_scn(SCN::base_scn()));

  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  const ObTabletStatus::Status statuses[] = {ObTabletStatus::TRANSFER_IN, ObTabletStatus::TRANSFER_OUT};
  uint64_t tablet_id = 1000000; // Absent tablets are recyclable once their rows are safe.
  for (const auto status : statuses) {
    SCOPED_TRACE(ObTabletStatus::get_str(ObTabletStatus(status)));
    const SCN transfer_scn = SCN::base_scn();
    SCN commit_scns[2];
    // Use separate commits to check that the scan publishes the newest row.
    for (auto &commit_scn : commit_scns) {
      ObArray<ObTabletReorgInfoData> rows;
      ASSERT_EQ(OB_SUCCESS, ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(
          ObTabletID(tablet_id++), transfer_scn, ObTabletStatus(status), ObLSID(1001),
          0 /* transfer_seq */, transfer_scn, SCN::min_scn(), rows));

      ObMySQLTransaction trans;
      ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id));
      ObTabletReorgInfoTableWriteOperator write_op;
      int ret = write_op.init(trans, tenant_id, ls_id);
      if (OB_SUCCESS == ret) {
        ret = write_op.insert_rows(rows);
      }
      const int end_ret = trans.end(OB_SUCCESS == ret);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, end_ret);

      ObTabletReorgInfoTableReadOperator read_op;
      ObTabletReorgInfoData data;
      ASSERT_EQ(OB_SUCCESS, read_op.init(tenant_id, ls_id, rows.at(0).key_));
      ASSERT_EQ(OB_SUCCESS, read_op.get_next(data, commit_scn));
    }

    SCN old_watermark;
    ASSERT_EQ(OB_SUCCESS, table.get_can_recycle_scn(old_watermark));
    ASSERT_LT(old_watermark, commit_scns[0]);
    ASSERT_LT(commit_scns[0], commit_scns[1]);

    // Local progress is an exclusive recycle boundary, so wait until it passes
    // the newest commit before expecting that row to be recyclable.
    const int64_t deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000L;
    SCN max_decided_scn;
    do {
      ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(max_decided_scn));
      if (max_decided_scn > commit_scns[1]) {
        break;
      }
      usleep(10 * 1000);
    } while (ObTimeUtility::current_time() < deadline);
    ASSERT_GT(max_decided_scn, commit_scns[1]);
    ASSERT_EQ(OB_SUCCESS, service->calc_ls_max_recycle_scn_(ls_id));
    SCN watermark;
    ASSERT_EQ(OB_SUCCESS, table.get_can_recycle_scn(watermark));
    EXPECT_EQ(commit_scns[1], watermark);
  }
}


#ifdef ERRSIM
TEST_F(TestMemberTable, reorg_recycle_local_progress_boundaries)
{
  if (OB_INVALID_TENANT_ID == g_tenant_id) {
    ASSERT_EQ(OB_SUCCESS, create_tenant());
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  }
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(g_tenant_id));
  auto *service = MTL(ObTabletReorgInfoTableService *);
  auto *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, service);
  ASSERT_NE(nullptr, ls_service);
  service->stop();
  service->wait();

  const ObLSID ls_id(1);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ObTabletReorgInfoTable &table = *ls->get_reorg_info_table();

  struct LocalProgressGuard
  {
    ~LocalProgressGuard() { EventTable::set_event("EN_REORG_GC_TEST_SCN", EventItem()); }
    int set(const uint64_t tenant_id, const SCN &scn)
    {
      EventItem event;
      event.cond_ = tenant_id;
      event.trigger_freq_ = 1;
      event.error_code_ = scn.get_val_for_inner_table_field();
      return EventTable::set_event("EN_REORG_GC_TEST_SCN", event);
    }
  } progress;

  ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  const ObTabletStatus::Status statuses[] = {
      ObTabletStatus::TRANSFER_IN, ObTabletStatus::TRANSFER_OUT, ObTabletStatus::FORK_DST};
  uint64_t tablet_base = 4000000;
  for (const auto status : statuses) {
    SCOPED_TRACE(ObTabletStatus::get_str(ObTabletStatus(status)));
    // Commit the larger rowkey first. The later commit is first in scan order.
    const ObTabletID tablet_ids[] = {ObTabletID(tablet_base + 2), ObTabletID(tablet_base)};
    SCN commits[2];
    for (int i = 0; i < 2; ++i) {
      ObArray<ObTabletReorgInfoData> rows;
      if (ObTabletStatus::FORK_DST == status) {
        ASSERT_EQ(OB_SUCCESS, ObTabletReorgInfoTableDataGenerator::gen_fork_reorg_info_data(
            tablet_ids[i], SCN::base_scn(), 1 /* ddl_task_id */,
            1001 /* src_table_id */, 1002 /* dst_table_id */,
            1 /* src_schema_version */, 1 /* dst_schema_version */,
            DATA_CURRENT_VERSION, rows));
      } else {
        ASSERT_EQ(OB_SUCCESS, ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(
            tablet_ids[i], SCN::base_scn(), ObTabletStatus(status), ObLSID(1001),
            0 /* transfer_seq */, SCN::base_scn(), SCN::min_scn(), rows));
      }
      ObMySQLTransaction trans;
      ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, g_tenant_id));
      ObTabletReorgInfoTableWriteOperator write_op;
      int ret = write_op.init(trans, g_tenant_id, ls_id);
      if (OB_SUCCESS == ret) {
        ret = write_op.insert_rows(rows);
      }
      const int end_ret = trans.end(OB_SUCCESS == ret);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, end_ret);
      ObTabletReorgInfoTableReadOperator read_op;
      ObTabletReorgInfoData data;
      ASSERT_EQ(OB_SUCCESS, read_op.init(g_tenant_id, ls_id, rows.at(0).key_));
      ASSERT_EQ(OB_SUCCESS, read_op.get_next(data, commits[i]));
    }

    const SCN before_commits = SCN::scn_dec(commits[0]);
    const SCN between_commits = SCN::scn_inc(commits[0]);
    ASSERT_LT(between_commits, commits[1]);
    SCN old_watermark;
    ASSERT_EQ(OB_SUCCESS, table.get_can_recycle_scn(old_watermark));
    ASSERT_LT(old_watermark, SCN::scn_dec(before_commits));

    // A normal full scan really encounters the ahead row before the old row.
    {
      ObTabletReorgInfoTableReadOperator read_op;
      ASSERT_EQ(OB_SUCCESS, read_op.init(g_tenant_id, ls_id));
      int matched = 0;
      while (true) {
        ObTabletReorgInfoData data;
        SCN commit_scn;
        const int ret = read_op.get_next(data, commit_scn);
        if (OB_ITER_END == ret) {
          break;
        }
        ASSERT_EQ(OB_SUCCESS, ret);
        if (data.key_.tablet_id_ == tablet_ids[0] || data.key_.tablet_id_ == tablet_ids[1]) {
          ASSERT_LT(matched, 2);
          EXPECT_EQ(tablet_ids[1 - matched], data.key_.tablet_id_);
          EXPECT_EQ(commits[1 - matched], commit_scn);
          ++matched;
        } else {
          ASSERT_LE(commit_scn, old_watermark);
        }
      }
      ASSERT_EQ(2, matched);
    }

    SCN watermark;
    ASSERT_EQ(OB_SUCCESS, progress.set(g_tenant_id, before_commits));
    ASSERT_EQ(OB_SUCCESS, service->calc_ls_max_recycle_scn_(ls_id));
    ASSERT_EQ(OB_SUCCESS, table.get_can_recycle_scn(watermark));
    EXPECT_EQ(old_watermark, watermark);

    ASSERT_EQ(OB_SUCCESS, progress.set(g_tenant_id, between_commits));
    ASSERT_EQ(OB_SUCCESS, service->calc_ls_max_recycle_scn_(ls_id));
    ASSERT_EQ(OB_SUCCESS, table.get_can_recycle_scn(watermark));
    EXPECT_EQ(commits[0], watermark);

    // At equality the row remains beyond the exclusive recycle boundary.
    ASSERT_EQ(OB_SUCCESS, progress.set(g_tenant_id, commits[1]));
    ASSERT_EQ(OB_SUCCESS, service->calc_ls_max_recycle_scn_(ls_id));
    ASSERT_EQ(OB_SUCCESS, table.get_can_recycle_scn(watermark));
    EXPECT_EQ(SCN::scn_dec(commits[1]), watermark);

    ASSERT_EQ(OB_SUCCESS, progress.set(g_tenant_id, SCN::scn_inc(commits[1])));
    ASSERT_EQ(OB_SUCCESS, service->calc_ls_max_recycle_scn_(ls_id));
    ASSERT_EQ(OB_SUCCESS, table.get_can_recycle_scn(watermark));
    // The newest eligible row must contribute, without a peer or old cache masking it.
    EXPECT_EQ(commits[1], watermark);
    tablet_base += 10;
  }
}
#endif

TEST_F(TestMemberTable, mixed_transfer_recycle_watermark)
{
  const ObTabletStatus::Status statuses[] = {ObTabletStatus::TRANSFER_IN, ObTabletStatus::TRANSFER_OUT};
  if (OB_INVALID_TENANT_ID == g_tenant_id) {
    ASSERT_EQ(OB_SUCCESS, create_tenant());
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  }
  const uint64_t tenant_id = g_tenant_id;
  {
    share::ObTenantSwitchGuard tenant_guard;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

    auto *service = MTL(ObTabletReorgInfoTableService *);
    auto *ls_service = MTL(ObLSService *);
    ASSERT_NE(nullptr, service);
    ASSERT_NE(nullptr, ls_service);
    service->stop();
    service->wait();

    const ObLSID ls_id(1);
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
    ObLS *ls = ls_handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ObTabletReorgInfoTable &table = *ls->get_reorg_info_table();

    // An existing tablet with an older transfer sequence keeps row B alive.
    // Absent tablets make A, C and D recyclable, using the real type-specific checks.
    const ObTabletID tablet_ids[] = {
        ObTabletID(2000002), ObTabletID(ObTabletID::LS_REORG_INFO_TABLET_ID),
        ObTabletID(2000000), ObTabletID(2000001)};
    ObTabletHandle active_tablet;
    ASSERT_EQ(OB_SUCCESS, ls->ha_get_tablet(tablet_ids[1], active_tablet));
    ASSERT_NE(nullptr, active_tablet.get_obj());
    const auto &transfer_info = active_tablet.get_obj()->get_tablet_meta().transfer_info_;
    const int64_t transfer_seq = transfer_info.transfer_seq_ + 1;
    ASSERT_GE(transfer_seq, 0);
    const SCN reorganization_scn = SCN::max(SCN::base_scn(), transfer_info.transfer_start_scn_);

    ObArray<ObTabletReorgInfoData> rows;
    for (const auto &tablet_id : tablet_ids) {
      for (const auto status : statuses) {
        ASSERT_EQ(OB_SUCCESS, ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(
            tablet_id, reorganization_scn, ObTabletStatus(status), ObLSID(1001),
            transfer_seq, reorganization_scn, SCN::min_scn(), rows));
      }
    }
    ASSERT_EQ(8, rows.count());

    ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
    SCN commit_scns[4];
    // Commit both types of A first, then B+C together, and finally D.
    for (int stage = 0; stage < 3; ++stage) {
      SCOPED_TRACE(stage);
      const int first = 0 == stage ? 0 : (1 == stage ? 1 : 3);
      const int count = 1 == stage ? 2 : 1;
      ObMySQLTransaction trans;
      ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, tenant_id));
      ObTabletReorgInfoTableWriteOperator write_op;
      int ret = write_op.init(trans, tenant_id, ls_id);
      for (int i = first * 2; OB_SUCCESS == ret && i < (first + count) * 2; ++i) {
        ret = write_op.insert_row(rows.at(i));
      }
      const int end_ret = trans.end(OB_SUCCESS == ret);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(OB_SUCCESS, end_ret);

      for (int i = first; i < first + count; ++i) {
        for (int status_index = 0; status_index < 2; ++status_index) {
          SCOPED_TRACE(status_index);
          ObTabletReorgInfoTableReadOperator read_op;
          ObTabletReorgInfoData data;
          SCN commit_scn;
          ASSERT_EQ(OB_SUCCESS, read_op.init(tenant_id, ls_id, rows.at(i * 2 + status_index).key_));
          ASSERT_EQ(OB_SUCCESS, read_op.get_next(data, commit_scn));
          if (0 == status_index) {
            commit_scns[i] = commit_scn;
          } else {
            ASSERT_EQ(commit_scns[i], commit_scn);
          }
          bool can_recycle = false;
          ASSERT_EQ(OB_SUCCESS, service->check_can_recycle_(*ls, data, can_recycle));
          ASSERT_EQ(1 != i, can_recycle);
        }
      }
      if (0 == stage) {
        continue;
      }

      ASSERT_LT(commit_scns[0], commit_scns[1]);
      ASSERT_EQ(commit_scns[1], commit_scns[2]);
      if (1 == stage) {
        // The actual scan sees both types of B, C, A, not commit-SCN order.
        ObTabletReorgInfoTableReadOperator scan;
        ASSERT_EQ(OB_SUCCESS, scan.init(tenant_id, ls_id));
        const int expected_order[] = {1, 1, 2, 2, 0, 0};
        int matched_rows = 0;
        while (true) {
          ObTabletReorgInfoData data;
          SCN commit_scn;
          const int ret = scan.get_next(data, commit_scn);
          if (OB_ITER_END == ret) {
            break;
          }
          ASSERT_EQ(OB_SUCCESS, ret);
          // The shared fixture may also contain rows from earlier tests.
          if (data.key_.tablet_id_ == tablet_ids[0]
              || data.key_.tablet_id_ == tablet_ids[1]
              || data.key_.tablet_id_ == tablet_ids[2]) {
            ASSERT_LT(matched_rows, 6);
            const int index = expected_order[matched_rows++];
            EXPECT_EQ(tablet_ids[index], data.key_.tablet_id_);
            EXPECT_EQ(commit_scns[index], commit_scn);
          }
        }
        ASSERT_EQ(6, matched_rows);
      } else {
        ASSERT_LT(commit_scns[1], commit_scns[3]);
      }

      const SCN &newest_commit = commit_scns[first + count - 1];
      const int64_t deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000L;
      SCN max_decided_scn;
      do {
        ASSERT_EQ(OB_SUCCESS, ls->get_max_decided_scn(max_decided_scn));
        if (max_decided_scn >= newest_commit) {
          break;
        }
        usleep(10 * 1000);
      } while (ObTimeUtility::current_time() < deadline);
      ASSERT_GE(max_decided_scn, newest_commit);

      ASSERT_EQ(OB_SUCCESS, service->calc_ls_max_recycle_scn_(ls_id));
      SCN watermark;
      ASSERT_EQ(OB_SUCCESS, table.get_can_recycle_scn(watermark));
      // Equal-SCN B+C and newer recyclable D must preserve B, while A can be reclaimed.
      EXPECT_EQ(SCN::scn_dec(commit_scns[1]), watermark);
      EXPECT_GE(watermark, commit_scns[0]);
      EXPECT_LT(watermark, commit_scns[1]);
    }
  }
}


} // namespace rootserver
} // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
