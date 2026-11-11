/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gmock/gmock.h>

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/utility.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/lob/store/ob_outtable_lob_control_manager.h"
#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#define private public
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_delete_task.h"
#undef private

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace unittest;

class TestOuttableLobControlManager : public ObSimpleClusterTestBase
{
public:
  TestOuttableLobControlManager()
    : ObSimpleClusterTestBase("test_outtable_lob_control_manager")
  {}

  ObMySQLProxy &sql_proxy()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  int append_lob_ctrl_pk_condition_(
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      ObSqlString &sql);
  int get_or_create_tenant_(const char *tenant_name, uint64_t &tenant_id);
  int read_gc_candidate_(const ObSSOuttableLobCtrlCandidate &candidate,
      ObOuttableLobControlRow &row, int64_t &row_scn);
  int write_gc_status_(const ObSSOuttableLobCtrlCandidate &candidate, const int64_t status);
  void check_gc_delete_batch_(ObSSLobCtrlGCDeleteTask &task, ObSSMarkSweepCtx &ctx);
  void check_complete_reads_t2_row_in_earlier_t1_(const char *isolation, const bool fixed_snapshot);

  void check_tablet_ls_(const uint64_t tenant_id, const bool expect_sys_ls)
  {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
        tenant_id,
        ObString::make_string("oceanbase"),
        ObString::make_string(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME),
        false /*is_index*/,
        table_schema));
    ASSERT_NE(nullptr, table_schema);

    ObArray<ObTabletID> table_tablet_ids;
    ObArray<ObLSID> table_ls_ids;
    ASSERT_EQ(OB_SUCCESS, table_schema->get_tablet_ids(table_tablet_ids));
    ASSERT_FALSE(table_tablet_ids.empty());
    ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_ls(
        sql_proxy(), tenant_id, table_tablet_ids, table_ls_ids));
    ASSERT_EQ(table_tablet_ids.count(), table_ls_ids.count());
    for (int64_t i = 0; i < table_ls_ids.count(); ++i) {
      EXPECT_EQ(expect_sys_ls, table_ls_ids.at(i).is_sys_ls());
    }

    ObArray<const ObSimpleTableSchemaV2 *> index_schemas;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_index_schemas_with_data_table_id(
        tenant_id, table_schema->get_table_id(), index_schemas));
    int64_t checked_index_count = 0;
    for (int64_t i = 0; i < index_schemas.count(); ++i) {
      const ObTableSchema *index_schema = nullptr;
      ObString index_name;
      ASSERT_NE(nullptr, index_schemas.at(i));
      ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
          tenant_id, index_schemas.at(i)->get_table_id(), index_schema));
      ASSERT_NE(nullptr, index_schema);
      ASSERT_EQ(OB_SUCCESS, index_schema->get_index_name(index_name));
      if (0 == index_name.case_compare("idx_lob_ctrl_hash_status")
          || 0 == index_name.case_compare("idx_lob_ctrl_status_time")) {
        ObArray<ObTabletID> index_tablet_ids;
        ObArray<ObLSID> index_ls_ids;
        ASSERT_EQ(OB_SUCCESS, index_schema->get_tablet_ids(index_tablet_ids));
        ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_ls(
            sql_proxy(), tenant_id, index_tablet_ids, index_ls_ids));
        ASSERT_EQ(table_ls_ids.count(), index_ls_ids.count());
        for (int64_t j = 0; j < index_ls_ids.count(); ++j) {
          EXPECT_EQ(table_ls_ids.at(j), index_ls_ids.at(j));
          EXPECT_EQ(expect_sys_ls, index_ls_ids.at(j).is_sys_ls());
        }
        ++checked_index_count;
      }
    }
    EXPECT_EQ(2, checked_index_count);
  }
};

int TestOuttableLobControlManager::append_lob_ctrl_pk_condition_(
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  char macro_id_buf[128];
  int64_t pos = 0;
  if (OB_UNLIKELY(!macro_id.is_valid()
      || macro_id.get_serialize_size() > sizeof(macro_id_buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(macro_id.serialize(macro_id_buf, sizeof(macro_id_buf), pos))) {
  } else if (OB_FAIL(sql.append("macro_id = "))) {
  } else if (OB_FAIL(sql_append_hex_escape_str(ObString(pos, macro_id_buf), sql))) {
  } else if (OB_FAIL(sql.append_fmt(" AND hash = %lu", hash))) {
  }
  return ret;
}

int TestOuttableLobControlManager::get_or_create_tenant_(
    const char *tenant_name,
    uint64_t &tenant_id)
{
  int ret = get_tenant_id(tenant_id, tenant_name);
  if (OB_ERR_UNEXPECTED == ret) {
    if (OB_FAIL(create_tenant_with_retry(tenant_name))) {
    } else if (OB_FAIL(get_tenant_id(tenant_id, tenant_name))) {
    }
  }
  return ret;
}

TEST_F(TestOuttableLobControlManager, sys_tenant_sql_round_trip)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const char info_buf[] = {'l', 'o', 'b', '\0', 'i', 'n', 'f', 'o'};
  const blocksstable::MacroBlockId macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 20260909, 0, 0, 0);
  const uint64_t hash = 0x1122334455667788ULL;

  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
      tenant_id,
      ObString::make_string("oceanbase"),
      ObString::make_string(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME),
      false /*is_index*/,
      table_schema));
  ASSERT_NE(nullptr, table_schema);
  EXPECT_TRUE(table_schema->is_aux_outtable_lob_control_table());
  EXPECT_EQ(PARTITION_LEVEL_ONE, table_schema->get_part_level());
  EXPECT_EQ(1, table_schema->get_part_option().get_part_num());

  ObArray<const ObSimpleTableSchemaV2 *> index_schemas;
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_index_schemas_with_data_table_id(
      tenant_id, table_schema->get_table_id(), index_schemas));
  bool found_hash_index = false;
  bool found_status_time_index = false;
  for (int64_t i = 0; i < index_schemas.count(); ++i) {
    const ObTableSchema *index_schema = nullptr;
    ObString index_name;
    ASSERT_NE(nullptr, index_schemas.at(i));
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
        tenant_id, index_schemas.at(i)->get_table_id(), index_schema));
    ASSERT_NE(nullptr, index_schema);
    ASSERT_EQ(OB_SUCCESS, index_schema->get_index_name(index_name));
    if (0 == index_name.case_compare("idx_lob_ctrl_hash_status")
        || 0 == index_name.case_compare("idx_lob_ctrl_status_time")) {
      EXPECT_EQ(table_schema->get_table_id(), index_schema->get_data_table_id());
      EXPECT_EQ(INDEX_TYPE_NORMAL_LOCAL, index_schema->get_index_type());
      EXPECT_EQ(INDEX_STATUS_AVAILABLE, index_schema->get_index_status());
      if (0 == index_name.case_compare("idx_lob_ctrl_hash_status")) {
        found_hash_index = true;
      } else {
        found_status_time_index = true;
        const char *expected_columns[] = {"status", "last_modify_time", "macro_id"};
        const ObIndexInfo &index_info = index_schema->get_index_info();
        ASSERT_EQ(3, index_info.get_size());
        for (int64_t j = 0; j < index_info.get_size(); ++j) {
          const ObRowkeyColumn *index_column = index_info.get_column(j);
          ASSERT_NE(nullptr, index_column);
          const ObColumnSchemaV2 *column_schema =
              index_schema->get_column_schema(index_column->column_id_);
          ASSERT_NE(nullptr, column_schema);
          EXPECT_EQ(0, column_schema->get_column_name_str().case_compare(expected_columns[j]));
        }
        const ObColumnSchemaV2 *info_column = index_schema->get_column_schema("info");
        ASSERT_NE(nullptr, info_column);
        EXPECT_TRUE(info_column->is_user_specified_storing_column());
      }
    }
  }
  ASSERT_TRUE(found_hash_index);
  ASSERT_TRUE(found_status_time_index);
  check_tablet_ls_(tenant_id, true /*expect_sys_ls*/);

  ObOuttableLobControlManager manager;
  ASSERT_EQ(OB_SUCCESS, manager.init(tenant_id));

  ObOuttableLobControlRow invalid_insert_row;
  invalid_insert_row.key_.macro_id_ = macro_id;
  invalid_insert_row.key_.hash_ = hash;
  invalid_insert_row.status_ = OUTTABLE_LOB_CONTROL_COMPLETE;
  ASSERT_EQ(OB_INVALID_ARGUMENT, manager.insert(invalid_insert_row));

  ObOuttableLobControlRow insert_row;
  insert_row.key_.macro_id_ = macro_id;
  insert_row.key_.hash_ = hash;
  insert_row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  insert_row.info_.assign_ptr(info_buf, sizeof(info_buf));
  ASSERT_EQ(OB_SUCCESS, manager.insert(insert_row));

  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), tenant_id));
  DEFER({
    if (trans.is_started()) {
      EXPECT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));
    }
  });

  ObArenaAllocator allocator("LobCtrlSQL");
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
      trans, tenant_id, macro_id, hash, true /*for_update*/,
      allocator, row_scn, selected_row));
  ASSERT_EQ(OUTTABLE_LOB_CONTROL_DOING, selected_row.status_);
  ASSERT_EQ(sizeof(info_buf), selected_row.info_.length());
  EXPECT_EQ(0, MEMCMP(info_buf, selected_row.info_.ptr(), sizeof(info_buf)));

  const char empty_info[] = "";
  ObOuttableLobControlRow update_row;
  update_row.key_.macro_id_ = macro_id;
  update_row.key_.hash_ = hash;
  update_row.status_ = OUTTABLE_LOB_CONTROL_COMPLETE;
  update_row.info_.assign_ptr(empty_info, 0);
  ASSERT_EQ(OB_SUCCESS, manager.update_complete(trans, update_row));

  ASSERT_EQ(OB_STATE_NOT_MATCH, manager.delete_fail_row(trans, macro_id, hash));

  ASSERT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));

  ObSqlString heartbeat_sql;
  int64_t heartbeat_affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, heartbeat_sql.assign_fmt(
      "UPDATE %s SET last_modify_time = usec_to_time(%ld) WHERE ",
      OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, selected_row.last_modify_time_ + 1));
  ASSERT_EQ(OB_SUCCESS, append_lob_ctrl_pk_condition_(macro_id, hash, heartbeat_sql));
  ASSERT_EQ(OB_SUCCESS,
      sql_proxy().write(tenant_id, heartbeat_sql.ptr(), heartbeat_affected_rows));
  ASSERT_EQ(1, heartbeat_affected_rows);
  selected_row.status_ = OUTTABLE_LOB_CONTROL_FAIL;
  ASSERT_EQ(OB_STATE_NOT_MATCH, manager.update_fail(selected_row));

  allocator.reuse();
  selected_row.reset();
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
      sql_proxy(), tenant_id, macro_id, hash, false /*for_update*/,
      allocator, row_scn, selected_row));
  selected_row.status_ = OUTTABLE_LOB_CONTROL_FAIL;
  ASSERT_EQ(OB_SUCCESS, manager.update_fail(selected_row));
  ASSERT_EQ(OB_STATE_NOT_MATCH, manager.update_fail(selected_row));

  ObMySQLTransaction delete_trans;
  ASSERT_EQ(OB_SUCCESS, delete_trans.start(&sql_proxy(), tenant_id));
  ASSERT_EQ(OB_SUCCESS, manager.delete_fail_row(delete_trans, macro_id, hash));
  ASSERT_EQ(OB_SUCCESS, delete_trans.end(true /*commit*/));
}

TEST_F(TestOuttableLobControlManager, delete_complete_row_checks_row_scn)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const blocksstable::MacroBlockId macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 20260930, 0, 0, 0);
  const uint64_t hash = 0x2233445566778899ULL;
  ObOuttableLobControlManager manager;
  ASSERT_EQ(OB_SUCCESS, manager.init(tenant_id));

  ObOuttableLobControlRow insert_row;
  insert_row.key_.macro_id_ = macro_id;
  insert_row.key_.hash_ = hash;
  insert_row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  ASSERT_EQ(OB_SUCCESS, manager.insert(insert_row));

  ObOuttableLobControlRow complete_row;
  complete_row.key_.macro_id_ = macro_id;
  complete_row.key_.hash_ = hash;
  complete_row.status_ = OUTTABLE_LOB_CONTROL_COMPLETE;
  ObMySQLTransaction complete_trans;
  ASSERT_EQ(OB_SUCCESS, complete_trans.start(&sql_proxy(), tenant_id));
  ASSERT_EQ(OB_SUCCESS, manager.update_complete(complete_trans, complete_row));
  ASSERT_EQ(OB_SUCCESS, complete_trans.end(true /*commit*/));

  ObArenaAllocator allocator("LobCtrlRowSCN");
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
      sql_proxy(), tenant_id, macro_id, hash, false /*for_update*/,
      allocator, row_scn, selected_row));
  ASSERT_GT(row_scn, 0);
  ASSERT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, selected_row.status_);

  ObMySQLTransaction stale_delete_trans;
  ASSERT_EQ(OB_SUCCESS, stale_delete_trans.start(&sql_proxy(), tenant_id));
  ASSERT_EQ(OB_STATE_NOT_MATCH,
      manager.delete_complete_row(stale_delete_trans, macro_id, hash, row_scn + 1));
  ASSERT_EQ(OB_SUCCESS, stale_delete_trans.end(false /*rollback*/));

  ObMySQLTransaction delete_trans;
  ASSERT_EQ(OB_SUCCESS, delete_trans.start(&sql_proxy(), tenant_id));
  ASSERT_EQ(OB_SUCCESS, manager.delete_complete_row(delete_trans, macro_id, hash, row_scn));
  ASSERT_EQ(OB_SUCCESS, delete_trans.end(true /*commit*/));

  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObOuttableLobControlOperator::select(
      sql_proxy(), tenant_id, macro_id, hash, false /*for_update*/,
      allocator, row_scn, selected_row));
}

int TestOuttableLobControlManager::read_gc_candidate_(
    const ObSSOuttableLobCtrlCandidate &candidate,
    ObOuttableLobControlRow &row,
    int64_t &row_scn)
{
  ObArenaAllocator allocator("LobGCTest");
  const int ret = ObOuttableLobControlOperator::select(sql_proxy(), OB_SYS_TENANT_ID,
      candidate.macro_id_, candidate.hash_, false /*for_update*/, allocator, row_scn, row);
  // Only the fixed-size row fields escape this helper's allocator.
  row.info_.reset();
  return ret;
}

int TestOuttableLobControlManager::write_gc_status_(
    const ObSSOuttableLobCtrlCandidate &candidate,
    const int64_t status)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("UPDATE oceanbase.__all_outtable_lob_control SET status=%ld,"
      "info='updated after scan', last_modify_time=now(6) WHERE ", status))) {
  } else if (OB_FAIL(append_lob_ctrl_pk_condition_(candidate.macro_id_, candidate.hash_, sql))) {
  } else if (OB_FAIL(sql_proxy().write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows))) {
  } else if (1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

void TestOuttableLobControlManager::check_gc_delete_batch_(
    ObSSLobCtrlGCDeleteTask &task,
    ObSSMarkSweepCtx &ctx)
{
  constexpr int64_t ROW_COUNT = 6;
  ObSSOuttableLobCtrlCandidate candidates[ROW_COUNT];
  ObSqlString cleanup_sql;
  ASSERT_EQ(OB_SUCCESS,
      cleanup_sql.assign("DELETE FROM oceanbase.__all_outtable_lob_control WHERE "));
  for (int64_t i = 0; i < ROW_COUNT; ++i) {
    candidates[i].macro_id_ = blocksstable::MacroBlockId(
        blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 202610270100 + i, 0, 0, 0);
    candidates[i].hash_ = 0x1020304050607080ULL;
    candidates[i].mark_row_scn_ = 1;
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append((0 == i) ? "(" : " OR ("));
    ASSERT_EQ(OB_SUCCESS, append_lob_ctrl_pk_condition_(
        candidates[i].macro_id_, candidates[i].hash_, cleanup_sql));
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(")"));
  }
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(ctx.tenant_id_, cleanup_sql.ptr(), affected_rows));
  DEFER({
    int64_t cleanup_affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS, sql_proxy().write(
        ctx.tenant_id_, cleanup_sql.ptr(), cleanup_affected_rows));
  });
  ObOuttableLobControlRow row;
  for (int64_t i = 0; i < ROW_COUNT; ++i) {
    if (4 != i) {
      row.reset();
      row.key_.macro_id_ = candidates[i].macro_id_;
      row.key_.hash_ = candidates[i].hash_;
      row.status_ = (1 == i) ? OUTTABLE_LOB_CONTROL_DOING
          : (2 == i) ? OUTTABLE_LOB_CONTROL_FAIL : OUTTABLE_LOB_CONTROL_COMPLETE;
      ASSERT_EQ(OB_SUCCESS,
          ObOuttableLobControlOperator::insert(sql_proxy(), ctx.tenant_id_, row, affected_rows));
      ASSERT_EQ(OB_SUCCESS, read_gc_candidate_(candidates[i], row, candidates[i].mark_row_scn_));
    }
  }
  // Change an existing COMPLETE version after the scan; the old candidate must be skipped.
  ASSERT_EQ(OB_SUCCESS, write_gc_status_(candidates[3], OUTTABLE_LOB_CONTROL_COMPLETE));
  int64_t current_row_scn = 0;
  ASSERT_EQ(OB_SUCCESS, read_gc_candidate_(candidates[3], row, current_row_scn));
  ASSERT_GT(current_row_scn, candidates[3].mark_row_scn_);
  ObSEArray<ObSSOuttableLobCtrlCandidate, ROW_COUNT> batch;
  for (int64_t i = 0; i < ROW_COUNT - 1; ++i) {
    ASSERT_EQ(OB_SUCCESS, batch.push_back(candidates[i]));
  }
  ASSERT_EQ(OB_SUCCESS, task.delete_batch_(ctx, batch));
  ASSERT_EQ(OB_SUCCESS, task.delete_batch_(ctx, batch));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, read_gc_candidate_(candidates[0], row, current_row_scn));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, read_gc_candidate_(candidates[4], row, current_row_scn));
  for (int64_t i = 1; i <= 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, read_gc_candidate_(candidates[i], row, current_row_scn));
    ASSERT_EQ((1 == i) ? OUTTABLE_LOB_CONTROL_DOING
        : (2 == i) ? OUTTABLE_LOB_CONTROL_FAIL : OUTTABLE_LOB_CONTROL_COMPLETE, row.status_);
  }
  // The last row is malformed in SQL, so the earlier successful DELETE must roll back.
  row.reset();
  row.key_.macro_id_ = candidates[0].macro_id_;
  row.key_.hash_ = candidates[0].hash_;
  row.status_ = OUTTABLE_LOB_CONTROL_COMPLETE;
  ASSERT_EQ(OB_SUCCESS,
      ObOuttableLobControlOperator::insert(sql_proxy(), ctx.tenant_id_, row, affected_rows));
  ASSERT_EQ(OB_SUCCESS, read_gc_candidate_(candidates[0], row, candidates[0].mark_row_scn_));
  ASSERT_EQ(OB_SUCCESS, write_gc_status_(candidates[5], 123 /*invalid_status*/));
  batch.reuse();
  ASSERT_EQ(OB_SUCCESS, batch.push_back(candidates[0]));
  ASSERT_EQ(OB_SUCCESS, batch.push_back(candidates[5]));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.delete_batch_(ctx, batch));
  ASSERT_EQ(OB_SUCCESS, read_gc_candidate_(candidates[0], row, current_row_scn));
  ASSERT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, row.status_);
  ASSERT_EQ(candidates[0].mark_row_scn_, current_row_scn);
  ASSERT_EQ(OB_SUCCESS, write_gc_status_(candidates[5], OUTTABLE_LOB_CONTROL_COMPLETE));
  ASSERT_EQ(OB_SUCCESS, read_gc_candidate_(candidates[5], row, candidates[5].mark_row_scn_));
  batch.at(1) = candidates[5];
  ASSERT_EQ(OB_SUCCESS, task.delete_batch_(ctx, batch));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, read_gc_candidate_(candidates[0], row, current_row_scn));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, read_gc_candidate_(candidates[5], row, current_row_scn));
}

TEST_F(TestOuttableLobControlManager, gc_delete_batch_rechecks_current_rows_and_rolls_back)
{
  ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObSSGCService *gc_service = MTL(ObSSGCService *);
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  const bool restart_gc = nullptr != gc_service && !gc_service->is_stopped();
  DEFER(if (restart_gc) { EXPECT_EQ(OB_SUCCESS, gc_service->start()); });
  // Isolate the SYS control round from background GC without stopping the DAG scheduler.
  if (nullptr != gc_service) {
    gc_service->stop();
    gc_service->wait();
  }
  ObSSMarkSweepDag dag;
  dag.set_tenant_id(OB_SYS_TENANT_ID);
  bool dag_exists = false;
  bool is_emergency = false;
  ASSERT_EQ(OB_SUCCESS, scheduler->check_dag_exist(&dag, dag_exists, is_emergency));
  if (dag_exists) {
    ASSERT_EQ(OB_SUCCESS, scheduler->cancel_dag(&dag, true /*force_cancel*/));
    const int64_t deadline = ObTimeUtility::current_time_us() + 30 * 1000 * 1000;
    while (dag_exists && ObTimeUtility::current_time_us() < deadline) {
      ob_usleep(10 * 1000);
      ASSERT_EQ(OB_SUCCESS, scheduler->check_dag_exist(&dag, dag_exists, is_emergency));
    }
    ASSERT_FALSE(dag_exists);
  }
  ObSharedGCControlInfo saved_control;
  const int read_ret =
      ObSharedGCControlOperator::read_row(sql_proxy(), OB_SYS_TENANT_ID, saved_control);
  ObSqlString restore_sql;
  if (OB_ENTRY_NOT_EXIST == read_ret) {
    // NORMAL-mode fixtures have no GC service to initialize this control row.
    ASSERT_EQ(OB_SUCCESS, restore_sql.assign_fmt(
        "DELETE FROM oceanbase.__all_shared_gc_control WHERE tenant_id=%lu", OB_SYS_TENANT_ID));
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_default_row(
        sql_proxy(), OB_SYS_TENANT_ID, affected_rows));
  } else {
    ASSERT_EQ(OB_SUCCESS, read_ret);
    ASSERT_EQ(OB_SUCCESS, restore_sql.assign_fmt("UPDATE oceanbase.__all_shared_gc_control SET "
        "executor_server_id=%ld,status='%s',gc_round_id=%ld,start_time=%ld WHERE tenant_id=%lu",
        saved_control.executor_server_id_, shared_gc_control_status_to_str(saved_control.status_),
        saved_control.gc_round_id_, saved_control.start_time_, OB_SYS_TENANT_ID));
  }
  DEFER({
    int64_t affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS, sql_proxy().write(OB_SYS_TENANT_ID, restore_sql.ptr(), affected_rows));
  });
  const int64_t round_id = (OB_ENTRY_NOT_EXIST == read_ret) ? 1 : saved_control.gc_round_id_ + 1;
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), OB_SYS_TENANT_ID));
  DEFER(if (trans.is_started()) { EXPECT_EQ(OB_SUCCESS, trans.end(false /*rollback*/)); });
  ObSharedGCControlInfo current_control;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::select_tenant_id_for_update(
      trans, OB_SYS_TENANT_ID, current_control));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_executor_state(trans, OB_SYS_TENANT_ID,
      GCTX.get_server_id(), ObSharedGCControlStatus::DOING, round_id, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, trans.end(true /*commit*/));
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID, round_id,
      ObSSMarkSweepDagParam::DEFAULT_MEMORY_LIMIT, ObSSMarkSweepDagParam::DEFAULT_FILE_BUF_SIZE,
      ObSSMarkSweepDagParam::DEFAULT_BATCH_SIZE));
  param.snapshot_meta_scn_ = SCN::base_scn();
  param.safe_meta_scn_ = SCN::base_scn();
  ASSERT_EQ(OB_SUCCESS, param.tablet_with_meta_scn_arr_.push_back(ObSSGCTabletWithMetaScn(
      ObSSGCTablet(ObLSID(1), ObTabletID(400001), SCN::base_scn()), SCN::base_scn())));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ctx.min_outtable_data_scn_ = SCN::minus(SCN::max_scn(), 1);
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_NO_FATAL_FAILURE(check_gc_delete_batch_(task, ctx));
}

TEST_F(TestOuttableLobControlManager, batch_insert_commits_independently_and_rolls_back_duplicates)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const int64_t batch_count = 9;
  ObArray<ObOuttableLobControlKey> all_keys;
  ObArray<ObOuttableLobControlKey> keys;
  ObSqlString cleanup_sql;
  ASSERT_EQ(OB_SUCCESS, cleanup_sql.assign_fmt(
      "DELETE FROM %s WHERE ", OB_ALL_OUTTABLE_LOB_CONTROL_TNAME));
  for (int64_t i = 0; i < batch_count + 3; ++i) {
    ObOuttableLobControlKey key;
    key.macro_id_ = blocksstable::MacroBlockId(
        blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 2026101700 + i, 0, 0, 0);
    key.hash_ = (0 == i % 2) ? UINT64_MAX : 0;
    ASSERT_EQ(OB_SUCCESS, all_keys.push_back(key));
    if (i < batch_count) {
      ASSERT_EQ(OB_SUCCESS, keys.push_back(key));
    }
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append((0 == i) ? "(" : " OR ("));
    ASSERT_EQ(OB_SUCCESS,
        append_lob_ctrl_pk_condition_(key.macro_id_, key.hash_, cleanup_sql));
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(")"));
  }
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id, cleanup_sql.ptr(), affected_rows));
  DEFER({
    int64_t cleanup_affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS,
        sql_proxy().write(tenant_id, cleanup_sql.ptr(), cleanup_affected_rows));
  });

  ObOuttableLobControlManager manager;
  ASSERT_EQ(OB_SUCCESS, manager.init(tenant_id));
  ObMySQLTransaction outer_trans;
  ASSERT_EQ(OB_SUCCESS, outer_trans.start(&sql_proxy(), tenant_id));
  DEFER({
    if (outer_trans.is_started()) {
      EXPECT_EQ(OB_SUCCESS, outer_trans.end(false /*rollback*/));
    }
  });
  ObOuttableLobControlRow marker_row;
  marker_row.key_ = all_keys.at(batch_count);
  marker_row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(
      outer_trans, tenant_id, marker_row, affected_rows));
  ASSERT_EQ(1, affected_rows);

  ASSERT_EQ(OB_SUCCESS, manager.insert(keys));
  ObArenaAllocator allocator("LobCtrlInsert");
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  // T2 is already visible to another connection while T1 is still uncommitted.
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
      sql_proxy(), tenant_id, keys.at(0).macro_id_, keys.at(0).hash_,
      false /*for_update*/, allocator, row_scn, selected_row));
  EXPECT_EQ(OUTTABLE_LOB_CONTROL_DOING, selected_row.status_);
  ASSERT_EQ(OB_SUCCESS, outer_trans.end(false /*rollback*/));

  allocator.reuse();
  selected_row.reset();
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObOuttableLobControlOperator::select(
      sql_proxy(), tenant_id, marker_row.key_.macro_id_, marker_row.key_.hash_,
      false /*for_update*/, allocator, row_scn, selected_row));
  for (int64_t i = 0; i < keys.count(); ++i) {
    allocator.reuse();
    selected_row.reset();
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
        sql_proxy(), tenant_id, keys.at(i).macro_id_, keys.at(i).hash_,
        false /*for_update*/, allocator, row_scn, selected_row));
    EXPECT_EQ(OUTTABLE_LOB_CONTROL_DOING, selected_row.status_);
    EXPECT_EQ(nullptr, selected_row.info_.ptr());
    EXPECT_EQ(0, selected_row.info_.length());
    EXPECT_GT(selected_row.last_modify_time_, 0);
  }

  // A duplicate at the end must fail the entire batch, including the preceding rows.
  keys.reset();
  ASSERT_EQ(OB_SUCCESS, keys.push_back(all_keys.at(batch_count + 1)));
  ASSERT_EQ(OB_SUCCESS, keys.push_back(all_keys.at(batch_count + 2)));
  ASSERT_EQ(OB_SUCCESS, keys.push_back(all_keys.at(batch_count + 1)));
  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, manager.insert(keys));
  for (int64_t i = 0; i < 2; ++i) {
    allocator.reuse();
    selected_row.reset();
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObOuttableLobControlOperator::select(
        sql_proxy(), tenant_id, keys.at(i).macro_id_, keys.at(i).hash_,
        false /*for_update*/, allocator, row_scn, selected_row));
  }
}

TEST_F(TestOuttableLobControlManager, batch_complete_deduplicates_preserves_info_and_rolls_back)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const int64_t requested_count = 65; // Exercise the 64-key batch boundary.
  const int64_t stored_count = requested_count + 1;
  const char binary_info[] = {'l', '\0', '\'', static_cast<char>(0xFF)};
  const char empty_info[] = "";
  ObArray<ObOuttableLobControlKey> stored_keys;
  ObSqlString cleanup_sql;
  ASSERT_EQ(OB_SUCCESS, cleanup_sql.assign_fmt(
      "DELETE FROM %s WHERE ", OB_ALL_OUTTABLE_LOB_CONTROL_TNAME));
  for (int64_t i = 0; i < stored_count; ++i) {
    ObOuttableLobControlKey key;
    key.macro_id_ = blocksstable::MacroBlockId(
        blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
        2026101600 + ((i >= 63 && i < requested_count) ? 1 : 0), 0, 0, 0);
    key.hash_ = (i < 63) ? i : ((63 == i) ? 0 : UINT64_MAX);
    ASSERT_EQ(OB_SUCCESS, stored_keys.push_back(key));
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append((0 == i) ? "(" : " OR ("));
    ASSERT_EQ(OB_SUCCESS,
        append_lob_ctrl_pk_condition_(key.macro_id_, key.hash_, cleanup_sql));
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(")"));
  }
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id, cleanup_sql.ptr(), affected_rows));
  DEFER({
    int64_t cleanup_affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS,
        sql_proxy().write(tenant_id, cleanup_sql.ptr(), cleanup_affected_rows));
  });

  ObOuttableLobControlManager manager;
  ASSERT_EQ(OB_SUCCESS, manager.init(tenant_id));
  for (int64_t i = 0; i < stored_count; ++i) {
    ObOuttableLobControlRow row;
    row.key_ = stored_keys.at(i);
    row.status_ = OUTTABLE_LOB_CONTROL_DOING;
    if (1 == i % 3) {
      row.info_.assign_ptr(empty_info, 0);
    } else if (2 == i % 3) {
      row.info_.assign_ptr(binary_info, sizeof(binary_info));
    }
    ASSERT_EQ(OB_SUCCESS, manager.insert(row));
  }

  ObArenaAllocator allocator("LobCtrlBatch");
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  ObArray<ObOuttableLobControlKey> keys;
  for (int64_t i = 0; i < requested_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, keys.push_back(stored_keys.at(i)));
  }

  // A missing key in the second batch must not leave the first batch committed.
  keys.at(requested_count - 1).hash_ = 999;
  {
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), tenant_id));
    DEFER({
      if (trans.is_started()) {
        EXPECT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));
      }
    });
    ASSERT_NE(nullptr, trans.get_connection());
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, manager.update_complete(*trans.get_connection(), keys));
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
        *trans.get_connection(), tenant_id, keys.at(0).macro_id_, keys.at(0).hash_,
        false /*for_update*/, allocator, row_scn, selected_row));
    EXPECT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, selected_row.status_);
    ASSERT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));
  }
  keys.at(requested_count - 1) = stored_keys.at(requested_count - 1);

  for (const int64_t invalid_status : {OUTTABLE_LOB_CONTROL_FAIL, OUTTABLE_LOB_CONTROL_COMPLETE}) {
    ObSqlString status_sql;
    ASSERT_EQ(OB_SUCCESS, status_sql.assign_fmt(
        "UPDATE %s SET status = %ld WHERE ", OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, invalid_status));
    ASSERT_EQ(OB_SUCCESS, append_lob_ctrl_pk_condition_(
        keys.at(requested_count - 1).macro_id_, keys.at(requested_count - 1).hash_, status_sql));
    ASSERT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id, status_sql.ptr(), affected_rows));
    ASSERT_EQ(1, affected_rows);

    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), tenant_id));
    DEFER({
      if (trans.is_started()) {
        EXPECT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));
      }
    });
    ASSERT_NE(nullptr, trans.get_connection());
    EXPECT_EQ(OB_STATE_NOT_MATCH, manager.update_complete(*trans.get_connection(), keys));
    ASSERT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));

    allocator.reuse();
    selected_row.reset();
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
        sql_proxy(), tenant_id, keys.at(0).macro_id_, keys.at(0).hash_,
        false /*for_update*/, allocator, row_scn, selected_row));
    EXPECT_EQ(OUTTABLE_LOB_CONTROL_DOING, selected_row.status_);
  }

  ObSqlString restore_sql;
  ASSERT_EQ(OB_SUCCESS, restore_sql.assign_fmt(
      "UPDATE %s SET status = %ld WHERE ",
      OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, static_cast<int64_t>(OUTTABLE_LOB_CONTROL_DOING)));
  ASSERT_EQ(OB_SUCCESS, append_lob_ctrl_pk_condition_(
      keys.at(requested_count - 1).macro_id_, keys.at(requested_count - 1).hash_, restore_sql));
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id, restore_sql.ptr(), affected_rows));
  ASSERT_EQ(1, affected_rows);

  // Reverse input and duplicate keys across the original batch boundary.
  keys.reset();
  ASSERT_EQ(OB_SUCCESS, keys.push_back(stored_keys.at(0)));
  for (int64_t i = requested_count - 1; i >= 0; --i) {
    ASSERT_EQ(OB_SUCCESS, keys.push_back(stored_keys.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, keys.push_back(stored_keys.at(requested_count - 1)));
  {
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), tenant_id));
    DEFER({
      if (trans.is_started()) {
        EXPECT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));
      }
    });
    ASSERT_NE(nullptr, trans.get_connection());
    ASSERT_EQ(OB_SUCCESS, manager.update_complete(*trans.get_connection(), keys));
    ASSERT_EQ(OB_SUCCESS, trans.end(true /*commit*/));
  }

  for (int64_t i = 0; i < stored_count; ++i) {
    allocator.reuse();
    selected_row.reset();
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
        sql_proxy(), tenant_id, stored_keys.at(i).macro_id_, stored_keys.at(i).hash_,
        false /*for_update*/, allocator, row_scn, selected_row));
    // The final row combines a requested macro_id and hash but was not requested.
    EXPECT_EQ((i < requested_count) ? OUTTABLE_LOB_CONTROL_COMPLETE : OUTTABLE_LOB_CONTROL_DOING,
        selected_row.status_);
    if (0 == i % 3) {
      EXPECT_EQ(nullptr, selected_row.info_.ptr());
    } else if (1 == i % 3) {
      EXPECT_NE(nullptr, selected_row.info_.ptr());
      EXPECT_EQ(0, selected_row.info_.length());
    } else {
      ASSERT_EQ(sizeof(binary_info), selected_row.info_.length());
      EXPECT_EQ(0, MEMCMP(binary_info, selected_row.info_.ptr(), sizeof(binary_info)));
    }
  }
}

TEST_F(TestOuttableLobControlManager, complete_reads_t2_row_in_earlier_rr_t1)
{
  check_complete_reads_t2_row_in_earlier_t1_("REPEATABLE READ", true /*fixed_snapshot*/);
}

TEST_F(TestOuttableLobControlManager, complete_reads_t2_row_in_earlier_rc_t1)
{
  check_complete_reads_t2_row_in_earlier_t1_("READ COMMITTED", false /*fixed_snapshot*/);
}

TEST_F(TestOuttableLobControlManager, complete_reads_t2_row_in_earlier_serial_t1)
{
  check_complete_reads_t2_row_in_earlier_t1_("SERIALIZABLE", true /*fixed_snapshot*/);
}

void TestOuttableLobControlManager::check_complete_reads_t2_row_in_earlier_t1_(
    const char *isolation, const bool fixed_snapshot)
{
  const char *tenant_name = "lob_ctrl_tenant";
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_or_create_tenant_(tenant_name, tenant_id));
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS, GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
      tenant_id,
      ObString::make_string("oceanbase"),
      ObString::make_string(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME),
      false /*is_index*/,
      table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(PARTITION_LEVEL_ONE, table_schema->get_part_level());
  ASSERT_EQ(128, table_schema->get_part_option().get_part_num());

  const blocksstable::MacroBlockId macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 20261014, 0, 0, 0);
  const uint64_t hash = 0x3344556677889900ULL;
  const char batch_info[] = {'r', 'r', '\0', '\''};
  ObArray<ObOuttableLobControlKey> batch_keys;
  ObSqlString cleanup_sql;
  ASSERT_EQ(OB_SUCCESS, cleanup_sql.assign_fmt(
      "DELETE FROM %s WHERE (", OB_ALL_OUTTABLE_LOB_CONTROL_TNAME));
  ASSERT_EQ(OB_SUCCESS, append_lob_ctrl_pk_condition_(macro_id, hash, cleanup_sql));
  ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(")"));
  for (int64_t i = 1; i <= 2; ++i) {
    ObOuttableLobControlKey key;
    key.macro_id_ = macro_id;
    key.hash_ = hash + i;
    ASSERT_EQ(OB_SUCCESS, batch_keys.push_back(key));
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(" OR ("));
    ASSERT_EQ(OB_SUCCESS, append_lob_ctrl_pk_condition_(macro_id, key.hash_, cleanup_sql));
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(")"));
  }
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id, cleanup_sql.ptr(), affected_rows));
  DEFER({
    int64_t cleanup_affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS,
        sql_proxy().write(tenant_id, cleanup_sql.ptr(), cleanup_affected_rows));
  });

  ObOuttableLobControlManager manager;
  ASSERT_EQ(OB_SUCCESS, manager.init(tenant_id));
  common::sqlclient::ObISQLConnectionPool *connection_pool = sql_proxy().get_pool();
  ASSERT_NE(nullptr, connection_pool);
  ASSERT_EQ(common::sqlclient::INNER_POOL, connection_pool->get_type());
  common::sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, connection_pool->acquire(
      tenant_id, connection, &sql_proxy(), 0 /*group_id*/));
  ASSERT_NE(nullptr, connection);
  observer::ObInnerSQLConnection *inner_connection =
      static_cast<observer::ObInnerSQLConnection *>(connection);
  ObArenaAllocator exec_ctx_allocator("LobCtrlRRCtx");
  sql::ObExecContext parent_exec_ctx(exec_ctx_allocator);
  sql::ObExecContext *saved_exec_ctx = nullptr;
  bool publish_trans_started = false;
  bool parent_exec_ctx_linked = false;
  DEFER({
    if (parent_exec_ctx_linked) {
      inner_connection->get_session().set_cur_exec_ctx(saved_exec_ctx);
    }
    int cleanup_ret = OB_SUCCESS;
    if (publish_trans_started) {
      cleanup_ret = connection->rollback();
      EXPECT_EQ(OB_SUCCESS, cleanup_ret);
    }
    EXPECT_EQ(OB_SUCCESS, connection_pool->release(connection, OB_SUCCESS == cleanup_ret));
  });

  ObSqlString isolation_sql;
  ASSERT_EQ(OB_SUCCESS, isolation_sql.assign_fmt(
      "SET SESSION TRANSACTION ISOLATION LEVEL %s", isolation));
  ASSERT_EQ(OB_SUCCESS,
      connection->execute_write(tenant_id, isolation_sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, connection->start_transaction(tenant_id, true /*with_snapshot*/));
  publish_trans_started = true;

  ObArenaAllocator allocator("LobCtrlCurrent");
  ObOuttableLobControlRow selected_row;
  int64_t row_scn = 0;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObOuttableLobControlOperator::select(
      *connection, tenant_id, macro_id, hash, false /*for_update*/,
      allocator, row_scn, selected_row));
  transaction::ObTxDesc *tx_desc = inner_connection->get_session().get_tx_desc();
  ASSERT_NE(nullptr, tx_desc);
  ASSERT_TRUE(tx_desc->is_in_tx());
  ASSERT_EQ(fixed_snapshot, tx_desc->is_RR_or_SERIAL_isolevel());
  const SCN t1_snapshot_scn = tx_desc->get_tx_snapshot_version();
  if (fixed_snapshot) {
    ASSERT_TRUE(t1_snapshot_scn.is_valid());
  }

  ObOuttableLobControlRow doing_row;
  doing_row.key_.macro_id_ = macro_id;
  doing_row.key_.hash_ = hash;
  doing_row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  ASSERT_EQ(OB_SUCCESS, manager.insert(doing_row));
  for (int64_t i = 0; i < batch_keys.count(); ++i) {
    ObOuttableLobControlRow batch_row;
    batch_row.key_ = batch_keys.at(i);
    batch_row.status_ = OUTTABLE_LOB_CONTROL_DOING;
    batch_row.info_.assign_ptr(batch_info, sizeof(batch_info));
    ASSERT_EQ(OB_SUCCESS, manager.insert(batch_row));
  }

  allocator.reuse();
  selected_row.reset();
  ASSERT_EQ(fixed_snapshot ? OB_ENTRY_NOT_EXIST : OB_SUCCESS, ObOuttableLobControlOperator::select(
      *connection, tenant_id, macro_id, hash, false /*for_update*/,
      allocator, row_scn, selected_row));

  // An active transaction keeps its isolation even if the session setting changes.
  inner_connection->get_session().set_tx_isolation(fixed_snapshot
      ? transaction::ObTxIsolationLevel::RC : transaction::ObTxIsolationLevel::RR);
  parent_exec_ctx.set_my_session(&inner_connection->get_session());
  parent_exec_ctx.get_das_ctx().use_outtable_lob_control_current_snapshot_ = true;
  saved_exec_ctx = inner_connection->get_session().get_cur_exec_ctx();
  inner_connection->get_session().set_cur_exec_ctx(&parent_exec_ctx);
  parent_exec_ctx_linked = true;
  ASSERT_EQ(OB_SUCCESS, manager.update_complete(*connection, doing_row.key_));
  ASSERT_EQ(OB_SUCCESS, manager.update_complete(*connection, batch_keys));
  inner_connection->get_session().set_cur_exec_ctx(saved_exec_ctx);
  parent_exec_ctx_linked = false;
  EXPECT_EQ(t1_snapshot_scn, tx_desc->get_tx_snapshot_version());
  ASSERT_EQ(OB_SUCCESS, connection->commit());
  publish_trans_started = false;

  allocator.reuse();
  selected_row.reset();
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
      sql_proxy(), tenant_id, macro_id, hash, false /*for_update*/,
      allocator, row_scn, selected_row));
  EXPECT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, selected_row.status_);
  for (int64_t i = 0; i < batch_keys.count(); ++i) {
    allocator.reuse();
    selected_row.reset();
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
        sql_proxy(), tenant_id, batch_keys.at(i).macro_id_, batch_keys.at(i).hash_,
        false /*for_update*/, allocator, row_scn, selected_row));
    EXPECT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, selected_row.status_);
    ASSERT_EQ(sizeof(batch_info), selected_row.info_.length());
    EXPECT_EQ(0, MEMCMP(batch_info, selected_row.info_.ptr(), sizeof(batch_info)));
  }
}

TEST_F(TestOuttableLobControlManager, user_and_meta_tenant_schema)
{
  const char *tenant_name = "lob_ctrl_tenant";
  uint64_t user_tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_or_create_tenant_(tenant_name, user_tenant_id));
  const uint64_t tenant_ids[] = {
      user_tenant_id,
      gen_meta_tenant_id(user_tenant_id),
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_ids); ++i) {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS,
        GCTX.schema_service_->get_tenant_schema_guard(tenant_ids[i], schema_guard));
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
        tenant_ids[i],
        ObString::make_string("oceanbase"),
        ObString::make_string(OB_ALL_OUTTABLE_LOB_CONTROL_TNAME),
        false /*is_index*/,
        table_schema));
    ASSERT_NE(nullptr, table_schema);
    EXPECT_TRUE(table_schema->is_aux_outtable_lob_control_table());
    EXPECT_EQ(PARTITION_LEVEL_ONE, table_schema->get_part_level());
    EXPECT_EQ(128, table_schema->get_part_option().get_part_num());

    ObArray<const ObSimpleTableSchemaV2 *> index_schemas;
    ASSERT_EQ(OB_SUCCESS, schema_guard.get_index_schemas_with_data_table_id(
        tenant_ids[i], table_schema->get_table_id(), index_schemas));
    bool found_hash_index = false;
    bool found_status_time_index = false;
    for (int64_t j = 0; j < index_schemas.count(); ++j) {
      const ObTableSchema *index_schema = nullptr;
      ObString index_name;
      ASSERT_NE(nullptr, index_schemas.at(j));
      ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(
          tenant_ids[i], index_schemas.at(j)->get_table_id(), index_schema));
      ASSERT_NE(nullptr, index_schema);
      ASSERT_EQ(OB_SUCCESS, index_schema->get_index_name(index_name));
      found_hash_index = found_hash_index
          || 0 == index_name.case_compare("idx_lob_ctrl_hash_status");
      found_status_time_index = found_status_time_index
          || 0 == index_name.case_compare("idx_lob_ctrl_status_time");
    }
    EXPECT_TRUE(found_hash_index);
    EXPECT_TRUE(found_status_time_index);
    check_tablet_ls_(tenant_ids[i], false /*expect_sys_ls*/);
  }
}

TEST_F(TestOuttableLobControlManager, page_iterator_uses_primary_key_cursor)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const uint64_t first_hash = 90000001;
  const blocksstable::MacroBlockId shared_macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 20260920, 0, 0, 0);
  const blocksstable::MacroBlockId fail_macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 20260921, 0, 0, 0);
  const blocksstable::MacroBlockId complete_macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 20260922, 0, 0, 0);
  const blocksstable::MacroBlockId after_upper_bound_macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID, 20260923, 0, 0, 0);
  const blocksstable::MacroBlockId cleanup_macro_ids[] = {
      shared_macro_id,
      shared_macro_id,
      fail_macro_id,
      complete_macro_id,
      after_upper_bound_macro_id,
  };
  ObSqlString cleanup_sql;
  ASSERT_EQ(OB_SUCCESS, cleanup_sql.assign_fmt(
      "DELETE FROM %s WHERE ", OB_ALL_OUTTABLE_LOB_CONTROL_TNAME));
  for (int64_t i = 0; i < ARRAYSIZEOF(cleanup_macro_ids); ++i) {
    if (i > 0) {
      ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(" OR "));
    }
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append("("));
    ASSERT_EQ(OB_SUCCESS,
        append_lob_ctrl_pk_condition_(cleanup_macro_ids[i], first_hash + i, cleanup_sql));
    ASSERT_EQ(OB_SUCCESS, cleanup_sql.append(")"));
  }
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id, cleanup_sql.ptr(), affected_rows));
  DEFER({
    int64_t cleanup_affected_rows = 0;
    EXPECT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id, cleanup_sql.ptr(), cleanup_affected_rows));
  });

  const char empty_info[] = "";
  const char binary_info[] = {'p', '\0', static_cast<char>(0xFF)};

  ObOuttableLobControlRow row;
  row.key_.macro_id_ = shared_macro_id;
  row.key_.hash_ = first_hash;
  row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(
      sql_proxy(), tenant_id, row, affected_rows));

  row.reset();
  row.key_.macro_id_ = shared_macro_id;
  row.key_.hash_ = first_hash + 1;
  row.status_ = OUTTABLE_LOB_CONTROL_FAIL;
  row.info_.assign_ptr(empty_info, 0);
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(
      sql_proxy(), tenant_id, row, affected_rows));

  row.reset();
  row.key_.macro_id_ = fail_macro_id;
  row.key_.hash_ = first_hash + 2;
  row.status_ = OUTTABLE_LOB_CONTROL_FAIL;
  row.info_.assign_ptr(binary_info, sizeof(binary_info));
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(
      sql_proxy(), tenant_id, row, affected_rows));

  row.reset();
  row.key_.macro_id_ = complete_macro_id;
  row.key_.hash_ = first_hash + 3;
  row.status_ = OUTTABLE_LOB_CONTROL_COMPLETE;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(
      sql_proxy(), tenant_id, row, affected_rows));

  ObOuttableLobControlPageIterator iterator;
  ASSERT_EQ(OB_SUCCESS, iterator.init(sql_proxy(), 10 * 1000 * 1000, tenant_id, 2));
  ASSERT_EQ(OB_INIT_TWICE, iterator.init(sql_proxy(), 10 * 1000 * 1000, tenant_id, 2));

  row.reset();
  row.key_.macro_id_ = after_upper_bound_macro_id;
  row.key_.hash_ = first_hash + 4;
  row.status_ = OUTTABLE_LOB_CONTROL_DOING;
  ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::insert(
      sql_proxy(), tenant_id, row, affected_rows));

  ObArenaAllocator allocator("LobCtrlPage");
  bool seen[4] = {false, false, false, false};
  int64_t returned_rows = 0;
  while (true) {
    allocator.reuse();
    row.reset();
    const int ret = iterator.get_next(allocator, row);
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(is_valid_outtable_lob_control_status(row.status_));
    ASSERT_GE(row.key_.hash_, first_hash);
    ASSERT_LE(row.key_.hash_, first_hash + 3);
    const int64_t index = static_cast<int64_t>(row.key_.hash_ - first_hash);
    ASSERT_FALSE(seen[index]);
    seen[index] = true;
    ++returned_rows;
    if (0 == index) {
      EXPECT_EQ(shared_macro_id, row.key_.macro_id_);
      EXPECT_EQ(nullptr, row.info_.ptr());
    } else if (1 == index) {
      EXPECT_EQ(shared_macro_id, row.key_.macro_id_);
      EXPECT_NE(nullptr, row.info_.ptr());
      EXPECT_EQ(0, row.info_.length());
    } else if (2 == index) {
      EXPECT_EQ(fail_macro_id, row.key_.macro_id_);
      ASSERT_EQ(sizeof(binary_info), row.info_.length());
      EXPECT_EQ(0, MEMCMP(binary_info, row.info_.ptr(), sizeof(binary_info)));
    } else {
      EXPECT_EQ(complete_macro_id, row.key_.macro_id_);
      EXPECT_EQ(OUTTABLE_LOB_CONTROL_COMPLETE, row.status_);
      EXPECT_EQ(nullptr, row.info_.ptr());
    }

    if (1 == returned_rows) {
      ObArenaAllocator select_allocator("LobCtrlSelect");
      ObOuttableLobControlRow selected_row;
      int64_t row_scn = 0;
      ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlOperator::select(
          sql_proxy(), tenant_id, row.key_.macro_id_, row.key_.hash_, false /*for_update*/,
          select_allocator, row_scn, selected_row));
      EXPECT_EQ(row.key_.macro_id_, selected_row.key_.macro_id_);
      EXPECT_EQ(row.key_.hash_, selected_row.key_.hash_);
      EXPECT_EQ(row.status_, selected_row.status_);

      ObSqlString update_sql;
      int64_t update_affected_rows = 0;
      ASSERT_EQ(OB_SUCCESS, update_sql.assign_fmt(
          "UPDATE %s SET last_modify_time = usec_to_time(%ld) WHERE ",
          OB_ALL_OUTTABLE_LOB_CONTROL_TNAME, selected_row.last_modify_time_ + 1));
      ASSERT_EQ(OB_SUCCESS,
          append_lob_ctrl_pk_condition_(row.key_.macro_id_, row.key_.hash_, update_sql));
      ASSERT_EQ(OB_SUCCESS, sql_proxy().write(tenant_id, update_sql.ptr(), update_affected_rows));
      ASSERT_EQ(1, update_affected_rows);
    }
  }
  EXPECT_EQ(4, returned_rows);
  EXPECT_TRUE(seen[0]);
  EXPECT_TRUE(seen[1]);
  EXPECT_TRUE(seen[2]);
  EXPECT_TRUE(seen[3]);
  EXPECT_EQ(OB_ITER_END, iterator.get_next(allocator, row));
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
