/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <thread>
#include "lib/oblog/ob_log.h"
#define private public
#include "share/vector_index/ob_vector_index_util.h"
#undef private
#include "sql/engine/expr/ob_expr_vector.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "../schema/mock_schema_service.h"
#undef private
#undef protected

namespace oceanbase
{
namespace share
{

class TestVecIndexSchemaChecker : public ::testing::Test
{
public:
  enum : uint64_t { TENANT_ID = 1, DATABASE_ID = 500003, DATA_TABLE_ID = 500004, INDEX_TABLE_ID = 500007 };
  static const int64_t SCHEMA_VERSION = 128;

  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, schema_service_.init());
    ASSERT_EQ(OB_SUCCESS, add_database(false));
    ASSERT_EQ(OB_SUCCESS, add_table(DATA_TABLE_ID, "vector_data", OB_INVALID_ID));
    ASSERT_EQ(OB_SUCCESS, add_table(INDEX_TABLE_ID, "__idx_500004_vector", DATA_TABLE_ID));
  }

  int add_database(const bool recycled)
  {
    schema::ObDatabaseSchema database;
    database.set_tenant_id(TENANT_ID);
    database.set_database_id(DATABASE_ID);
    database.set_schema_version(SCHEMA_VERSION);
    database.set_name_case_mode(common::OB_ORIGIN_AND_INSENSITIVE);
    database.set_in_recyclebin(recycled);
    int ret = database.set_database_name("vector_database");
    if (OB_SUCC(ret)) {
      ret = schema_service_.add_database_schema(database, SCHEMA_VERSION);
    }
    return ret;
  }

  int add_table(const uint64_t table_id, const char *name, const uint64_t data_table_id,
                const uint64_t database_id = DATABASE_ID)
  {
    schema::ObTableSchema table;
    table.set_tenant_id(TENANT_ID);
    table.set_database_id(database_id);
    table.set_table_id(table_id);
    table.set_schema_version(SCHEMA_VERSION);
    table.set_name_case_mode(common::OB_ORIGIN_AND_INSENSITIVE);
    table.set_table_type(OB_INVALID_ID == data_table_id ? schema::USER_TABLE : schema::USER_INDEX);
    if (OB_INVALID_ID != data_table_id) {
      table.set_data_table_id(data_table_id);
      table.set_index_type(schema::INDEX_TYPE_NORMAL_LOCAL);
    }
    int ret = table.set_table_name(name);
    if (OB_SUCC(ret)) {
      ret = schema_service_.add_table_schema(table, SCHEMA_VERSION);
    }
    return ret;
  }

  void expect_active(const uint64_t table_id, const bool expected)
  {
    schema::ObSchemaGetterGuard guard;
    ASSERT_EQ(OB_SUCCESS, schema_service_.get_tenant_schema_guard(TENANT_ID, guard, SCHEMA_VERSION));
    ObVecIndexSchemaChecker checker(TENANT_ID, guard);
    bool active = !expected;
    ASSERT_EQ(OB_SUCCESS, checker.check_table(table_id, active));
    EXPECT_EQ(expected, active);
  }

  void set_tenant_state(const bool recycled,
                        const schema::ObTenantStatus status = schema::TENANT_STATUS_NORMAL)
  {
    const schema::ObSimpleTenantSchema *tenant_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, schema_service_.mgr_.get_tenant_schema(TENANT_ID, tenant_schema));
    ASSERT_NE(nullptr, tenant_schema);
    schema::ObSimpleTenantSchema updated_tenant(*tenant_schema);
    ASSERT_EQ(OB_SUCCESS, updated_tenant.get_err_ret());
    updated_tenant.set_in_recyclebin(recycled);
    updated_tenant.set_status(status);
    ASSERT_EQ(OB_SUCCESS, schema_service_.mgr_.add_tenant(updated_tenant));
  }

  schema::MockSchemaService schema_service_;
};

TEST_F(TestVecIndexSchemaChecker, tenant_recycle_and_flashback)
{
  for (const bool recycled : {false, true, false}) {
    set_tenant_state(recycled);
    expect_active(INDEX_TABLE_ID, !recycled);
    expect_active(DATA_TABLE_ID, !recycled);
    schema::ObSchemaGetterGuard guard;
    ASSERT_EQ(OB_SUCCESS, schema_service_.get_tenant_schema_guard(TENANT_ID, guard, SCHEMA_VERSION));
    ObVecIndexSchemaChecker checker(TENANT_ID, guard);
    bool active = recycled;
    ASSERT_EQ(OB_SUCCESS, checker.check_database(DATABASE_ID, active));
    EXPECT_EQ(!recycled, active);
  }
}

TEST_F(TestVecIndexSchemaChecker, tenant_purge_keeps_schema_inactive)
{
  set_tenant_state(true);
  expect_active(INDEX_TABLE_ID, false);
  expect_active(DATA_TABLE_ID, false);

  // PURGE clears the recyclebin flag before the tenant's database and table schemas disappear.
  set_tenant_state(false, schema::TENANT_STATUS_DROPPING);
  expect_active(INDEX_TABLE_ID, false);
  expect_active(DATA_TABLE_ID, false);
  schema::ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, schema_service_.get_tenant_schema_guard(TENANT_ID, guard, SCHEMA_VERSION));
  ObVecIndexSchemaChecker checker(TENANT_ID, guard);
  bool active = true;
  ASSERT_EQ(OB_SUCCESS, checker.check_database(DATABASE_ID, active));
  EXPECT_FALSE(active);
}

TEST_F(TestVecIndexSchemaChecker, missing_tenant)
{
  schema::ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, schema_service_.get_tenant_schema_guard(TENANT_ID, guard, SCHEMA_VERSION));
  ObVecIndexSchemaChecker checker(TENANT_ID, guard);
  bool active = false;
  ASSERT_EQ(OB_SUCCESS, checker.check_database(DATABASE_ID, active));
  ASSERT_TRUE(active);
  ASSERT_EQ(OB_SUCCESS, schema_service_.mgr_.del_tenant(TENANT_ID));
  EXPECT_EQ(OB_SUCCESS, checker.check_database(DATABASE_ID, active));
  EXPECT_FALSE(active);
}

TEST_F(TestVecIndexSchemaChecker, database_recycle_and_flashback)
{
  expect_active(INDEX_TABLE_ID, true);
  ASSERT_EQ(OB_SUCCESS, add_database(true));
  expect_active(INDEX_TABLE_ID, false);
  expect_active(DATA_TABLE_ID, false);
  ASSERT_EQ(OB_SUCCESS, add_database(false));
  expect_active(INDEX_TABLE_ID, true);
}

TEST_F(TestVecIndexSchemaChecker, missing_index_and_data_table)
{
  expect_active(600000, false);
  ASSERT_EQ(OB_SUCCESS, schema_service_.drop_table_schema(TENANT_ID, DATA_TABLE_ID));
  expect_active(INDEX_TABLE_ID, false);
}

TEST_F(TestVecIndexSchemaChecker, missing_database)
{
  ASSERT_EQ(OB_SUCCESS, schema_service_.mgr_.del_database(schema::ObTenantDatabaseId(TENANT_ID, DATABASE_ID)));
  expect_active(INDEX_TABLE_ID, false);
}

TEST_F(TestVecIndexSchemaChecker, recycled_data_table)
{
  ASSERT_EQ(OB_SUCCESS, add_table(DATA_TABLE_ID, "vector_data", OB_INVALID_ID, common::OB_RECYCLEBIN_SCHEMA_ID));
  expect_active(INDEX_TABLE_ID, false);
}

TEST_F(TestVecIndexSchemaChecker, recycled_index_table)
{
  ASSERT_EQ(OB_SUCCESS,
      add_table(INDEX_TABLE_ID, "__idx_500004_vector", DATA_TABLE_ID, common::OB_RECYCLEBIN_SCHEMA_ID));
  expect_active(INDEX_TABLE_ID, false);
}

TEST_F(TestVecIndexSchemaChecker, schema_errors_are_retryable)
{
  schema::ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, schema_service_.get_tenant_schema_guard(TENANT_ID, guard, SCHEMA_VERSION + 1));
  ObVecIndexSchemaChecker checker(TENANT_ID, guard);
  bool active = true;
  EXPECT_EQ(OB_EAGAIN, checker.check_table(INDEX_TABLE_ID, active));
  EXPECT_FALSE(active);
  ASSERT_EQ(OB_SUCCESS, schema_service_.get_tenant_schema_guard(TENANT_ID, guard, SCHEMA_VERSION));
  EXPECT_EQ(OB_SUCCESS, checker.check_table(INDEX_TABLE_ID, active));
  EXPECT_TRUE(active);
}

TEST_F(TestVecIndexSchemaChecker, checks_preserve_independent_results)
{
  const uint64_t missing_table_id = INDEX_TABLE_ID + 1;
  schema::ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, schema_service_.get_tenant_schema_guard(TENANT_ID, guard, SCHEMA_VERSION));
  ObVecIndexSchemaChecker checker(TENANT_ID, guard);
  for (int64_t iteration = 0; iteration < 3; ++iteration) {
    bool active = false;
    ASSERT_EQ(OB_SUCCESS, checker.check_table(INDEX_TABLE_ID, active));
    ASSERT_TRUE(active);
    ASSERT_EQ(OB_SUCCESS, checker.check_table(missing_table_id, active));
    ASSERT_FALSE(active);
    ASSERT_EQ(OB_SUCCESS, checker.check_database(DATABASE_ID, active));
    ASSERT_TRUE(active);
    ASSERT_EQ(OB_SUCCESS, checker.check_database(DATABASE_ID + 1, active));
    ASSERT_FALSE(active);
  }
}

TEST_F(TestVecIndexSchemaChecker, invalid_arguments_are_not_missing_schemas)
{
  schema::ObSchemaGetterGuard guard;
  ASSERT_EQ(OB_SUCCESS, schema_service_.get_tenant_schema_guard(TENANT_ID, guard, SCHEMA_VERSION));
  ObVecIndexSchemaChecker checker(TENANT_ID, guard);
  bool active = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, checker.check_table(OB_INVALID_ID, active));
  EXPECT_FALSE(active);
  active = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, checker.check_database(OB_INVALID_ID, active));
  EXPECT_FALSE(active);
  EXPECT_EQ(OB_SUCCESS, checker.check_table(INDEX_TABLE_ID, active));
  EXPECT_TRUE(active);
}

class TestVecDropDDLConflict : public ::testing::Test
{
public:
  enum : uint64_t { INDEX_ID = 500007, BITMAP_ID = 500010, INC_TABLET = 200001, BITMAP_TABLET = 200002 };

  TestVecDropDDLConflict() : manager_(memory_context_, 1), adapter_(&allocator_, memory_context_, 1) {}

  void SetUp() override
  {
    adapter_.inc_ref();
    ASSERT_EQ(OB_SUCCESS, table_conflicts_.create(16));
    ASSERT_EQ(OB_SUCCESS, index_conflicts_.create(16));
    ASSERT_EQ(OB_SUCCESS, dropping_indexes_.create(16));
    ASSERT_EQ(OB_SUCCESS, manager_.get_vec_adaptor_map().create(16, "VecDDLTest", "VecDDLTest"));
    ASSERT_EQ(OB_SUCCESS, adapter_.set_table_id(VIRT_INC, INDEX_ID));
    ASSERT_EQ(OB_SUCCESS, adapter_.set_table_id(VIRT_BITMAP, BITMAP_ID));
    ASSERT_EQ(OB_SUCCESS, adapter_.set_tablet_id(VIRT_INC, ObTabletID(INC_TABLET)));
    ASSERT_EQ(OB_SUCCESS, adapter_.set_tablet_id(VIRT_BITMAP, ObTabletID(BITMAP_TABLET)));
    ASSERT_EQ(OB_SUCCESS, manager_.get_vec_adaptor_map().set_refactored(ObTabletID(INC_TABLET), &adapter_));
  }

  void TearDown() override
  {
    manager_.get_vec_adaptor_map().destroy();
    EXPECT_TRUE(adapter_.dec_ref_and_check_release());
  }

  void expect_conflict(const ObVecIndexAsyncTaskType type,
                       const ObVecIndexAsyncTaskTriggerType trigger, const bool expected,
                       const uint64_t inc_tablet_id = OB_INVALID_ID)
  {
    ObVecIndexAsyncTaskCtx task;
    task.task_status_.task_type_ = type;
    task.task_status_.trigger_type_ = trigger;
    task.task_status_.table_id_ = OB_VECTOR_ASYNC_INDEX_MERGE == type ? BITMAP_ID : INDEX_ID;
    task.task_status_.tablet_id_ = ObTabletID(OB_VECTOR_ASYNC_INDEX_MERGE == type ? BITMAP_TABLET : INC_TABLET);
    task.task_status_.inc_tablet_id_ = ObTabletID(inc_tablet_id);
    bool conflict = !expected;
    ASSERT_EQ(OB_SUCCESS, ObVecITaskExecutor::check_task_ddl_conflict(
        &task, &manager_, table_conflicts_, index_conflicts_, dropping_indexes_, conflict));
    EXPECT_EQ(expected, conflict);
  }

  common::ObArenaAllocator allocator_;
  lib::MemoryContext memory_context_;
  ObPluginVectorIndexMgr manager_;
  ObPluginVectorIndexAdaptor adapter_;
  common::hash::ObHashSet<uint64_t> table_conflicts_;
  common::hash::ObHashSet<uint64_t> index_conflicts_;
  common::hash::ObHashSet<uint64_t> dropping_indexes_;
};

TEST_F(TestVecDropDDLConflict, freeze_only_matching_drop)
{
  ASSERT_EQ(OB_SUCCESS, dropping_indexes_.set_refactored(INDEX_ID, 0));
  expect_conflict(OB_VECTOR_ASYNC_INDEX_FREEZE, OB_VEC_TRIGGER_AUTO, true);
  expect_conflict(OB_VECTOR_ASYNC_INDEX_FREEZE, OB_VEC_TRIGGER_MANUAL, true);
  ASSERT_EQ(OB_SUCCESS, dropping_indexes_.erase_refactored(INDEX_ID));
  ASSERT_EQ(OB_SUCCESS, dropping_indexes_.set_refactored(INDEX_ID + 1, 0));
  expect_conflict(OB_VECTOR_ASYNC_INDEX_FREEZE, OB_VEC_TRIGGER_AUTO, false);
  expect_conflict(OB_VECTOR_ASYNC_INDEX_FREEZE, OB_VEC_TRIGGER_MANUAL, false);
}

TEST_F(TestVecDropDDLConflict, merge_resolves_delta_index)
{
  ASSERT_EQ(OB_SUCCESS, dropping_indexes_.set_refactored(INDEX_ID, 0));
  expect_conflict(OB_VECTOR_ASYNC_INDEX_MERGE, OB_VEC_TRIGGER_AUTO, true);
  expect_conflict(OB_VECTOR_ASYNC_INDEX_MERGE, OB_VEC_TRIGGER_MANUAL, true);
  expect_conflict(OB_VECTOR_ASYNC_INDEX_MERGE, OB_VEC_TRIGGER_MANUAL, true, INC_TABLET);
  expect_conflict(OB_VECTOR_ASYNC_INDEX_MERGE, OB_VEC_TRIGGER_MANUAL, true, 0);
}

TEST_F(TestVecDropDDLConflict, unrelated_drop_does_not_cancel_merge)
{
  ASSERT_EQ(OB_SUCCESS, dropping_indexes_.set_refactored(BITMAP_ID, 0));
  expect_conflict(OB_VECTOR_ASYNC_INDEX_MERGE, OB_VEC_TRIGGER_AUTO, false);
  expect_conflict(OB_VECTOR_ASYNC_INDEX_MERGE, OB_VEC_TRIGGER_MANUAL, false);
}

TEST_F(TestVecDropDDLConflict, other_ddl_does_not_block_ordinary_tasks)
{
  ASSERT_EQ(OB_SUCCESS, index_conflicts_.set_refactored(INDEX_ID, 0));
  for (const auto trigger : {OB_VEC_TRIGGER_AUTO, OB_VEC_TRIGGER_MANUAL}) {
    expect_conflict(OB_VECTOR_ASYNC_INDEX_FREEZE, trigger, false);
    expect_conflict(OB_VECTOR_ASYNC_INDEX_MERGE, trigger, false);
  }
}

TEST_F(TestVecDropDDLConflict, other_task_policies_are_unchanged)
{
  ASSERT_EQ(OB_SUCCESS, index_conflicts_.set_refactored(INDEX_ID, 0));
  ASSERT_EQ(OB_SUCCESS, dropping_indexes_.set_refactored(INDEX_ID, 0));
  expect_conflict(OB_VECTOR_ASYNC_INDEX_OPTINAL, OB_VEC_TRIGGER_AUTO, true);
  expect_conflict(OB_VECTOR_ASYNC_INDEX_FREEZE, OB_VEC_TRIGGER_ALTER_INDEX_PARAM, false);
  expect_conflict(OB_VECTOR_ASYNC_MEM_SYNC_TASK, OB_VEC_TRIGGER_AUTO, false);
}

TEST_F(TestVecDropDDLConflict, missing_adapter_is_not_a_matching_drop)
{
  ASSERT_EQ(OB_SUCCESS, dropping_indexes_.set_refactored(INDEX_ID + 1, 0));
  ASSERT_EQ(OB_SUCCESS, manager_.get_vec_adaptor_map().erase_refactored(ObTabletID(INC_TABLET)));
  ObVecIndexAsyncTaskCtx task;
  task.task_status_.task_type_ = OB_VECTOR_ASYNC_INDEX_MERGE;
  task.task_status_.trigger_type_ = OB_VEC_TRIGGER_MANUAL;
  task.task_status_.table_id_ = BITMAP_ID;
  task.task_status_.tablet_id_ = ObTabletID(BITMAP_TABLET);
  bool conflict = true;
  EXPECT_EQ(OB_HASH_NOT_EXIST, ObVecITaskExecutor::check_task_ddl_conflict(
      &task, &manager_, table_conflicts_, index_conflicts_, dropping_indexes_, conflict));
  EXPECT_FALSE(conflict);
}

TEST_F(TestVecDropDDLConflict, invalid_arguments)
{
  bool conflict = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObVecITaskExecutor::check_task_ddl_conflict(
      nullptr, &manager_, table_conflicts_, index_conflicts_, dropping_indexes_, conflict));
  EXPECT_FALSE(conflict);
}

TEST_F(TestVecDropDDLConflict, snapshot_tablet_resolves_incremental_adapter_key)
{
  const uint64_t snapshot_table_id = 500011;
  const ObTabletID snapshot_tablet_id(200003);
  ASSERT_EQ(OB_SUCCESS, adapter_.set_table_id(VIRT_SNAP, snapshot_table_id));
  ASSERT_EQ(OB_SUCCESS, adapter_.set_tablet_id(VIRT_SNAP, snapshot_tablet_id));
  ObPluginVectorIndexAdapterGuard guard;
  ASSERT_EQ(OB_SUCCESS, manager_.get_adapter_inst_guard_for_embedding(
      snapshot_table_id, snapshot_tablet_id, guard));
  EXPECT_EQ(&adapter_, guard.get_adatper());
  EXPECT_EQ(ObTabletID(INC_TABLET), guard.get_adatper()->get_inc_tablet_id());
}

TEST_F(TestVecDropDDLConflict, snapshot_lookup_rejects_other_index_and_tablet)
{
  const uint64_t snapshot_table_id = 500011;
  const ObTabletID snapshot_tablet_id(200003);
  ASSERT_EQ(OB_SUCCESS, adapter_.set_table_id(VIRT_SNAP, snapshot_table_id));
  ASSERT_EQ(OB_SUCCESS, adapter_.set_tablet_id(VIRT_SNAP, snapshot_tablet_id));
  ObPluginVectorIndexAdapterGuard guard;
  EXPECT_EQ(OB_HASH_NOT_EXIST, manager_.get_adapter_inst_guard_for_embedding(
      snapshot_table_id + 1, snapshot_tablet_id, guard));
  EXPECT_FALSE(guard.is_valid());
  EXPECT_EQ(OB_HASH_NOT_EXIST, manager_.get_adapter_inst_guard_for_embedding(
      snapshot_table_id, ObTabletID(INC_TABLET), guard));
  EXPECT_FALSE(guard.is_valid());
}

TEST_F(TestVecDropDDLConflict, removed_snapshot_adapter_is_not_resolved)
{
  const uint64_t snapshot_table_id = 500011;
  const ObTabletID snapshot_tablet_id(200003);
  ASSERT_EQ(OB_SUCCESS, adapter_.set_table_id(VIRT_SNAP, snapshot_table_id));
  ASSERT_EQ(OB_SUCCESS, adapter_.set_tablet_id(VIRT_SNAP, snapshot_tablet_id));
  ASSERT_EQ(OB_SUCCESS, manager_.get_vec_adaptor_map().erase_refactored(ObTabletID(INC_TABLET)));
  ObPluginVectorIndexAdapterGuard guard;
  EXPECT_EQ(OB_HASH_NOT_EXIST, manager_.get_adapter_inst_guard_for_embedding(
      snapshot_table_id, snapshot_tablet_id, guard));
  EXPECT_FALSE(guard.is_valid());
}

TEST_F(TestVecDropDDLConflict, manual_embedding_resolves_incremental_task_identity)
{
  ObPluginVectorIndexAdapterGuard guard;
  ASSERT_EQ(OB_SUCCESS, manager_.get_adapter_inst_guard_for_embedding(
      INDEX_ID, ObTabletID(INC_TABLET), guard));
  EXPECT_EQ(ObTabletID(INC_TABLET), guard.get_adatper()->get_inc_tablet_id());
  guard.reset();
  EXPECT_EQ(OB_HASH_NOT_EXIST, manager_.get_adapter_inst_guard_for_embedding(
      INDEX_ID, ObTabletID(BITMAP_TABLET), guard));
  EXPECT_FALSE(guard.is_valid());
}

class TestVecEmbeddingTaskLoad : public TestVecDropDDLConflict
{
public:
  enum : uint64_t { SNAPSHOT_ID = 500011, SNAPSHOT_TABLET = 200003 };

  void SetUp() override
  {
    TestVecDropDDLConflict::SetUp();
    ObLSID ls_id(1);
    ASSERT_EQ(OB_SUCCESS, manager_.get_async_task_opt().init(16, 1, ls_id));
    ASSERT_EQ(OB_SUCCESS, adapter_.set_table_id(VIRT_SNAP, SNAPSHOT_ID));
    ASSERT_EQ(OB_SUCCESS, adapter_.set_tablet_id(VIRT_SNAP, ObTabletID(SNAPSHOT_TABLET)));
    row_.tenant_id_ = 1;
    row_.table_id_ = SNAPSHOT_ID;
    row_.tablet_id_ = ObTabletID(SNAPSHOT_TABLET);
    row_.task_id_ = 1;
    row_.task_type_ = OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING;
    row_.trigger_type_ = OB_VEC_TRIGGER_MANUAL;
    row_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
    ObAddr addr;
    ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
    row_.trace_id_.init(addr);
  }

  void TearDown() override
  {
    manager_.get_async_task_opt().destroy();
    TestVecDropDDLConflict::TearDown();
  }

  int load_task()
  {
    storage::ObLSHandle ls_handle;
    return ObVecIndexAsyncTaskUtil::try_add_task_ctx_from_inner_row(
        1, row_, nullptr, ls_handle, manager_, false /*verify_tablet_on_ls*/);
  }

  ObVecIndexTaskStatus row_;
};

TEST_F(TestVecEmbeddingTaskLoad, snapshot_task_publishes_incremental_identity)
{
  ASSERT_EQ(OB_SUCCESS, load_task());
  ObVecIndexAsyncTaskCtx *ctx = nullptr;
  const ObVecIndexAsyncTaskKey key(ObTabletID(SNAPSHOT_TABLET), OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING);
  ASSERT_EQ(OB_SUCCESS, manager_.get_async_task_opt().get_async_task_map().get_refactored(key, ctx));
  ASSERT_NE(nullptr, ctx);
  EXPECT_EQ(ObTabletID(INC_TABLET), ctx->task_status_.inc_tablet_id_);
  EXPECT_EQ(ObTabletID(SNAPSHOT_TABLET), ctx->task_status_.tablet_id_);
  EXPECT_EQ(SNAPSHOT_ID, ctx->task_status_.table_id_);
  EXPECT_EQ(OB_INVALID_ID, row_.inc_tablet_id_.id());
}

TEST_F(TestVecEmbeddingTaskLoad, legacy_incremental_identity_loads)
{
  row_.table_id_ = INDEX_ID;
  row_.tablet_id_ = ObTabletID(INC_TABLET);
  ASSERT_EQ(OB_SUCCESS, load_task());
  ObVecIndexAsyncTaskCtx *ctx = nullptr;
  const ObVecIndexAsyncTaskKey key(ObTabletID(INC_TABLET), OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING);
  ASSERT_EQ(OB_SUCCESS, manager_.get_async_task_opt().get_async_task_map().get_refactored(key, ctx));
  ASSERT_NE(nullptr, ctx);
  EXPECT_EQ(ObTabletID(INC_TABLET), ctx->task_status_.inc_tablet_id_);
}

TEST_F(TestVecEmbeddingTaskLoad, missing_adapter_fails_without_publishing_task)
{
  ASSERT_EQ(OB_SUCCESS, manager_.get_vec_adaptor_map().erase_refactored(ObTabletID(INC_TABLET)));
  EXPECT_EQ(OB_HASH_NOT_EXIST, load_task());
  EXPECT_EQ(0, manager_.get_async_task_opt().get_async_task_map().size());
}

TEST_F(TestVecEmbeddingTaskLoad, cancelled_adapter_fails_without_publishing_task)
{
  adapter_.set_need_cancel_task();
  EXPECT_EQ(OB_CANCELED, load_task());
  EXPECT_EQ(0, manager_.get_async_task_opt().get_async_task_map().size());
}

class TestVecTaskAdapterLifetime : public TestVecDropDDLConflict
{
public:
  void SetUp() override
  {
    TestVecDropDDLConflict::SetUp();
    ASSERT_EQ(OB_SUCCESS,
        manager_.get_async_task_opt().get_async_task_map().create(16, "VecLoadTest", "VecLoadTest"));
    task_.tenant_id_ = 1;
    task_.truncate_version_ = 0;
    task_.task_status_.task_type_ = OB_VECTOR_ASYNC_INDEX_FREEZE;
    task_.task_status_.tablet_id_ = ObTabletID(INC_TABLET);
    task_.task_status_.table_id_ = INDEX_ID;
  }

  void TearDown() override
  {
    manager_.get_async_task_opt().get_async_task_map().destroy();
    TestVecDropDDLConflict::TearDown();
  }

  int register_task(bool &added)
  {
    return ObVecITaskExecutor::try_add_task_ctx_for_adapter(
        manager_, ObTabletID(INC_TABLET), &adapter_, &task_, added);
  }

  ObVecIndexAsyncTaskCtx task_;
};

TEST_F(TestVecTaskAdapterLifetime, current_adapter_registers_once)
{
  bool added = false;
  ASSERT_EQ(OB_SUCCESS, register_task(added));
  EXPECT_TRUE(added);
  EXPECT_EQ(1, manager_.get_async_task_opt().get_async_task_map().size());
  ASSERT_EQ(OB_SUCCESS, register_task(added));
  EXPECT_FALSE(added);
}

TEST_F(TestVecTaskAdapterLifetime, replaced_adapter_does_not_register)
{
  ObPluginVectorIndexAdaptor replacement(&allocator_, memory_context_, 1);
  ASSERT_EQ(OB_SUCCESS,
      manager_.get_vec_adaptor_map().set_refactored(ObTabletID(INC_TABLET), &replacement, 1));
  bool added = true;
  ASSERT_EQ(OB_SUCCESS, register_task(added));
  EXPECT_FALSE(added);
  EXPECT_EQ(0, manager_.get_async_task_opt().get_async_task_map().size());
}

TEST_F(TestVecTaskAdapterLifetime, removed_adapter_does_not_register)
{
  ASSERT_EQ(OB_SUCCESS, manager_.get_vec_adaptor_map().erase_refactored(ObTabletID(INC_TABLET)));
  bool added = true;
  ASSERT_EQ(OB_SUCCESS, register_task(added));
  EXPECT_FALSE(added);
}

TEST_F(TestVecTaskAdapterLifetime, cancelled_adapter_does_not_register)
{
  adapter_.set_need_cancel_task();
  bool added = true;
  ASSERT_EQ(OB_SUCCESS, register_task(added));
  EXPECT_FALSE(added);
}

TEST_F(TestVecTaskAdapterLifetime, stopping_ls_does_not_register)
{
  manager_.get_async_task_opt().set_stop();
  bool added = true;
  ASSERT_EQ(OB_SUCCESS, register_task(added));
  EXPECT_FALSE(added);
}

TEST_F(TestVecTaskAdapterLifetime, destroying_ls_does_not_register)
{
  manager_.set_ls_destroying();
  bool added = true;
  ASSERT_EQ(OB_SUCCESS, register_task(added));
  EXPECT_FALSE(added);
}

TEST_F(TestVecTaskAdapterLifetime, invalid_arguments_do_not_register)
{
  bool added = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObVecITaskExecutor::try_add_task_ctx_for_adapter(
      manager_, ObTabletID(INC_TABLET), nullptr, &task_, added));
  EXPECT_FALSE(added);
}

TEST_F(TestVecTaskAdapterLifetime, registration_can_skip_schema_fetch)
{
  task_.truncate_version_ = OB_INVALID_VERSION;
  task_.tenant_id_ = OB_INVALID_TENANT_ID;
  bool added = false;
  ASSERT_EQ(OB_SUCCESS, manager_.get_async_task_opt().add_task_ctx(
      task_.task_status_.tablet_id_, &task_, added, false));
  EXPECT_TRUE(added);
  EXPECT_EQ(OB_INVALID_VERSION, task_.truncate_version_);
}

class VecAdapterReleaseAllocator : public common::ObIAllocator
{
public:
  VecAdapterReleaseAllocator() : tracked_(nullptr), released_(0) {}
  void *alloc(const int64_t size) override { return allocator_.alloc(size); }
  void *alloc(const int64_t size, const common::ObMemAttr &attr) override
  {
    return allocator_.alloc(size, attr);
  }
  void free(void *pointer) override
  {
    if (pointer == tracked_) {
      ++released_;
    }
    allocator_.free(pointer);
  }
  common::ObArenaAllocator allocator_;
  void *tracked_;
  int64_t released_;
};

TEST_F(TestVecTaskAdapterLifetime, guard_keeps_removed_adapter_alive_without_holding_map_lock)
{
  VecAdapterReleaseAllocator tracked_allocator;
  tracked_allocator.tracked_ = tracked_allocator.alloc(sizeof(ObPluginVectorIndexAdaptor));
  ASSERT_NE(nullptr, tracked_allocator.tracked_);
  ObPluginVectorIndexAdaptor *held_adapter = new(tracked_allocator.tracked_)
      ObPluginVectorIndexAdaptor(&tracked_allocator, memory_context_, 1);
  ASSERT_EQ(OB_SUCCESS, held_adapter->set_table_id(VIRT_INC, INDEX_ID));
  ASSERT_EQ(OB_SUCCESS, held_adapter->set_tablet_id(VIRT_INC, ObTabletID(INC_TABLET)));
  held_adapter->inc_ref();
  held_adapter->inc_ls_ref();
  ASSERT_EQ(OB_SUCCESS,
      manager_.get_vec_adaptor_map().set_refactored(ObTabletID(INC_TABLET), held_adapter, 1));
  {
    ObSEArray<ObTabletID, 4> tablet_ids;
    ASSERT_EQ(OB_SUCCESS, manager_.collect_adaptor_tablet_ids(tablet_ids));
    ASSERT_EQ(1, tablet_ids.count());
    ObPluginVectorIndexAdapterGuard guard;
    ASSERT_EQ(OB_SUCCESS, manager_.get_adapter_inst_guard(tablet_ids.at(0), guard));
    bool write_locked = false;
    bool detached = false;
    int detach_ret = OB_ERR_UNEXPECTED;
    std::thread writer([&]() {
      write_locked = manager_.get_adapter_map_lock().try_wrlock();
      if (write_locked) {
        manager_.get_adapter_map_lock().wrunlock();
        detach_ret = manager_.detach_adapter(ObTabletID(INC_TABLET), detached);
      }
    });
    writer.join();
    ASSERT_TRUE(write_locked);
    ASSERT_EQ(OB_SUCCESS, detach_ret);
    ASSERT_TRUE(detached);
    EXPECT_EQ(0, tracked_allocator.released_);
    EXPECT_EQ(INDEX_ID, guard.get_adatper()->get_inc_table_id());
    bool added = true;
    ASSERT_EQ(OB_SUCCESS, ObVecITaskExecutor::try_add_task_ctx_for_adapter(
        manager_, ObTabletID(INC_TABLET), guard.get_adatper(), &task_, added));
    EXPECT_FALSE(added);
  }
  EXPECT_EQ(1, tracked_allocator.released_);
}

TEST(TestVectorIndexUtil, hnsw_build_parameter_physical_degree_boundaries)
{
  const ObVectorIndexAlgorithmType types[] = {VIAT_HNSW, VIAT_HNSW_SQ, VIAT_HNSW_BQ, VIAT_HGRAPH};
  const int64_t ef_values[] = {10, 11, 15, 19, 20, 21};
  for (const auto type : types) {
    for (const bool support_duplicate : {false, true}) {
      for (const int64_t ef : ef_values) {
        SCOPED_TRACE(::testing::Message() << "type=" << type
            << ", duplicate=" << support_duplicate << ", ef=" << ef);
        ObVectorIndexParam param;
        param.type_ = type;
        param.m_ = 10;
        param.ef_construction_ = ef;
        param.support_duplicate_ = support_duplicate;
        const bool valid = VIAT_HNSW == type && !support_duplicate ? ef > 10 : ef >= 20;
        EXPECT_EQ(valid ? OB_SUCCESS : OB_NOT_SUPPORTED,
                  ObVectorIndexUtil::validate_hnsw_build_params(param));
      }
    }
  }
}

TEST(TestVectorIndexUtil, alter_validates_physical_degree)
{
  const char *invalid_params[] = {
      "TYPE=HNSW,M=10,EF_CONSTRUCTION=15,DUPLICATE_GROUP=TRUE",
      "TYPE=HNSW_SQ,M=10,EF_CONSTRUCTION=15,DUPLICATE_GROUP=FALSE",
      "TYPE=HNSW_BQ,M=10,EF_CONSTRUCTION=15,DUPLICATE_GROUP=FALSE"};
  for (const char *params : invalid_params) {
    SCOPED_TRACE(params);
    EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::validate_alter_hnsw_index_params(
        common::ObString::make_string(params), false));
  }
  EXPECT_EQ(OB_SUCCESS, ObVectorIndexUtil::validate_alter_hnsw_index_params(
      common::ObString::make_string("TYPE=HNSW,M=10,EF_CONSTRUCTION=15,DUPLICATE_GROUP=FALSE"),
      false));
  EXPECT_EQ(OB_SUCCESS, ObVectorIndexUtil::validate_alter_hnsw_index_params(
      common::ObString::make_string("TYPE=HNSW,M=10,EF_CONSTRUCTION=20,DUPLICATE_GROUP=TRUE"),
      false));
}

TEST(TestVectorIndexUtil, extra_info_hnsw_validates_physical_degree)
{
  ObVectorIndexParam param;
  param.type_ = VIAT_HNSW;
  param.m_ = 10;
  param.extra_info_actual_size_ = 8;
  param.support_duplicate_ = false;
  for (const int64_t ef : {19, 20, 21}) {
    SCOPED_TRACE(ef);
    param.ef_construction_ = ef;
    EXPECT_EQ(ef >= 20 ? OB_SUCCESS : OB_NOT_SUPPORTED,
              ObVectorIndexUtil::validate_hnsw_build_params(param));
  }
  EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::validate_alter_hnsw_index_params(
      common::ObString::make_string(
          "TYPE=HNSW,M=10,EF_CONSTRUCTION=19,EXTRA_INFO_ACTUAL_SIZE=8"), false));
  EXPECT_EQ(OB_SUCCESS, ObVectorIndexUtil::validate_alter_hnsw_index_params(
      common::ObString::make_string(
          "TYPE=HNSW,M=10,EF_CONSTRUCTION=20,EXTRA_INFO_ACTUAL_SIZE=8"), false));
}

TEST(TestVectorIndexUtil, rebuild_validates_inherited_build_parameters)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const common::ObString old_params = common::ObString::make_string(
      "DISTANCE=L2,TYPE=HNSW,LIB=VSAG,M=10,EF_CONSTRUCTION=15,EF_SEARCH=64");
  for (const char *patch : {"DUPLICATE_GROUP=TRUE", "TYPE=HNSW_SQ", "TYPE=HNSW_BQ"}) {
    SCOPED_TRACE(patch);
    common::ObString new_params = common::ObString::make_string(patch);
    EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::construct_rebuild_index_param(
        table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator));
  }
  common::ObString new_params = common::ObString::make_string(
      "DUPLICATE_GROUP=TRUE,EF_CONSTRUCTION=20");
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::construct_rebuild_index_param(
      table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator));
  ObVectorIndexParam param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      new_params, VIT_HNSW_INDEX, param, false));
  EXPECT_EQ(10, param.m_);
  EXPECT_EQ(20, param.ef_construction_);
  EXPECT_TRUE(param.support_duplicate_);
}

TEST(TestVectorIndexUtil, rebuild_validates_inherited_duplicate_mode)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const common::ObString old_params = common::ObString::make_string(
      "DISTANCE=L2,TYPE=HNSW,LIB=VSAG,M=10,EF_CONSTRUCTION=20,EF_SEARCH=64,DUPLICATE_GROUP=TRUE");
  common::ObString new_params = common::ObString::make_string("EF_CONSTRUCTION=15");
  EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::construct_rebuild_index_param(
      table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator));
  new_params = common::ObString::make_string("DUPLICATE_GROUP=FALSE,EF_CONSTRUCTION=15");
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::construct_rebuild_index_param(
      table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator));
  ObVectorIndexParam param;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
      new_params, VIT_HNSW_INDEX, param, false));
  EXPECT_FALSE(param.support_duplicate_);
  EXPECT_EQ(15, param.ef_construction_);
}

TEST(TestVectorIndexUtil, l1_parameter_and_expression_mapping)
{
  ObVectorIndexParam param;
  const common::ObString params =
      common::ObString::make_string("DISTANCE=L1, TYPE=HNSW, LIB=VSAG");
  ASSERT_EQ(OB_SUCCESS,
      ObVectorIndexUtil::parser_params_from_string(
          params, ObVectorIndexType::VIT_HNSW_INDEX, param, false));
  EXPECT_EQ(ObVectorIndexDistAlgorithm::VIDA_L1, param.dist_algorithm_);
  EXPECT_EQ(ObVectorIndexAlgorithmType::VIAT_HNSW, param.type_);
  EXPECT_STREQ("l1",
      VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_L1]);
  EXPECT_STREQ("l1",
      ObVectorIndexUtil::get_dist_algorithm_str(
          ObVectorIndexDistAlgorithm::VIDA_L1));

  EXPECT_TRUE(ObVectorIndexUtil::is_expr_type_and_distance_algorithm_match(
      T_FUN_SYS_L1_DISTANCE, ObVectorIndexDistAlgorithm::VIDA_L1));
  EXPECT_FALSE(ObVectorIndexUtil::is_expr_type_and_distance_algorithm_match(
      T_FUN_SYS_L2_DISTANCE, ObVectorIndexDistAlgorithm::VIDA_L1));

  int64_t dis_type = sql::ObExprVectorDistance::ObVecDisType::MAX_TYPE;
  ASSERT_EQ(OB_SUCCESS,
      ObVectorIndexUtil::get_vec_dis_type_from_dis_algorithm(
          ObVectorIndexDistAlgorithm::VIDA_L1, dis_type));
  EXPECT_EQ(sql::ObExprVectorDistance::ObVecDisType::MANHATTAN, dis_type);
}

TEST(TestVectorIndexUtil, rebuild_rejects_l1_quantized_hnsw)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const common::ObString old_params = common::ObString::make_string(
      "DISTANCE=L1,TYPE=HNSW,LIB=VSAG,M=16,EF_CONSTRUCTION=200,EF_SEARCH=64");
  const char *unsupported_params[] = {
      "DISTANCE=L1,TYPE=HNSW_SQ", "DISTANCE=L1,TYPE=HNSW_BQ"};
  for (const char *params : unsupported_params) {
    SCOPED_TRACE(params);
    common::ObString new_params = common::ObString::make_string(params);
    EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::construct_rebuild_index_param(
        table_schema, old_params, new_params, VIT_HNSW_INDEX, &allocator, 128));
  }
}

TEST(TestVectorIndexUtil, rebuild_preserves_supported_metric_type_combinations)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const char *old_params[] = {
      "DISTANCE=L1,TYPE=HNSW,LIB=VSAG,M=16,EF_CONSTRUCTION=200,EF_SEARCH=64",
      "DISTANCE=L2,TYPE=HNSW,LIB=VSAG,M=16,EF_CONSTRUCTION=200,EF_SEARCH=64",
      "DISTANCE=L1,TYPE=IVF_FLAT,LIB=OB,NLIST=16,SAMPLE_PER_NLIST=256"};
  const char *requested_params[] = {
      "DISTANCE=L1,TYPE=HNSW", "DISTANCE=L2,TYPE=HNSW_SQ", "DISTANCE=L1,TYPE=IVF_FLAT"};
  const ObVectorIndexType index_types[] = {VIT_HNSW_INDEX, VIT_HNSW_INDEX, VIT_IVF_INDEX};
  const ObVectorIndexAlgorithmType expected_types[] = {VIAT_HNSW, VIAT_HNSW_SQ, VIAT_IVF_FLAT};
  const ObVectorIndexDistAlgorithm expected_metrics[] = {VIDA_L1, VIDA_L2, VIDA_L1};
  for (int64_t i = 0; i < 3; ++i) {
    SCOPED_TRACE(requested_params[i]);
    common::ObString new_params = common::ObString::make_string(requested_params[i]);
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::construct_rebuild_index_param(
        table_schema, common::ObString::make_string(old_params[i]), new_params,
        index_types[i], &allocator, 128));
    ObVectorIndexParam parsed;
    ASSERT_EQ(OB_SUCCESS, ObVectorIndexUtil::parser_params_from_string(
        new_params, index_types[i], parsed, false));
    EXPECT_EQ(expected_types[i], parsed.type_);
    EXPECT_EQ(expected_metrics[i], parsed.dist_algorithm_);
  }
}

TEST(TestVectorIndexUtil, rebuild_rejects_l1_spfresh)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  schema::ObTableSchema table_schema;
  const common::ObString old_params = common::ObString::make_string(
      "DISTANCE=L1,TYPE=IVF_FLAT,LIB=OB,NLIST=16,SAMPLE_PER_NLIST=256");
  common::ObString new_params = common::ObString::make_string(
      "DISTANCE=L1,TYPE=IVF_FLAT,CENTROIDS_FRESH_MODE=SPFRESH");
  EXPECT_EQ(OB_NOT_SUPPORTED, ObVectorIndexUtil::construct_rebuild_index_param(
      table_schema, old_params, new_params, VIT_IVF_INDEX, &allocator, 128));
}

TEST(TestVectorIndexUtil, existing_metric_values_are_stable)
{
  EXPECT_EQ(0, static_cast<int>(ObVectorIndexDistAlgorithm::VIDA_L2));
  EXPECT_EQ(1, static_cast<int>(ObVectorIndexDistAlgorithm::VIDA_IP));
  EXPECT_EQ(2, static_cast<int>(ObVectorIndexDistAlgorithm::VIDA_COS));
  EXPECT_STREQ("l2", VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_L2]);
  EXPECT_STREQ("ip", VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_IP]);
  EXPECT_STREQ("cosine", VEC_INDEX_ALGTH[ObVectorIndexDistAlgorithm::VIDA_COS]);
}

TEST(TestVectorIndexUtil, unknown_metric_is_still_rejected)
{
  ObVectorIndexParam param;
  const common::ObString params =
      common::ObString::make_string("DISTANCE=UNKNOWN, TYPE=HNSW");
  EXPECT_EQ(OB_NOT_SUPPORTED,
      ObVectorIndexUtil::parser_params_from_string(
          params, ObVectorIndexType::VIT_HNSW_INDEX, param, false));
}

TEST(TestVectorIndexUtil, l1_vsag_distance_converts_to_ob_distance)
{
  constexpr double vsag_distance = 12.5;
  double ob_distance = 0;
  ASSERT_EQ(OB_SUCCESS,
      ObVectorIndexUtil::convert_vsag_to_ob_distance(
          vsag_distance, T_FUN_SYS_L1_DISTANCE, ob_distance));
  EXPECT_DOUBLE_EQ(vsag_distance, ob_distance);
}

TEST(TestVectorIndexUtil, l1_ob_distance_converts_to_vsag_distance)
{
  constexpr double ob_distance = 12.5;
  double vsag_distance = 0;
  ASSERT_EQ(OB_SUCCESS,
      ObVectorIndexUtil::convert_ob_to_vsag_distance(
          ob_distance, T_FUN_SYS_L1_DISTANCE, vsag_distance));
  EXPECT_DOUBLE_EQ(ob_distance, vsag_distance);
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
